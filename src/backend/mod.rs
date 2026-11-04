//! 測速伺服器來源（後端）。
//!
//! 後端只負責「把位元組搬進搬出」：開一條串流，把傳輸的應用層位元組數累加到
//! `counter`。取樣、暖機、統計全部由 [`crate::engine`] 統一處理，
//! 所以不同後端量出來的數字是用同一套方法算的，可以直接互相比較。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use serde::Serialize;
use tokio::time::Instant;

pub mod cloudflare;
pub mod ndt7;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Download,
    Upload,
}

impl Direction {
    pub fn label(self) -> &'static str {
        match self {
            Direction::Download => "下載",
            Direction::Upload => "上傳",
        }
    }
}

/// 給使用者看的伺服器資訊。
#[derive(Debug, Clone, Serialize)]
pub struct ServerInfo {
    /// 主機名稱或機器 ID
    pub name: String,
    /// 地點（城市、國家、機房代碼等），拿不到就是 `None`
    pub location: Option<String>,
}

/// 伺服器端回報的量測（例如 ndt7 的 TCPInfo），用來交叉驗證客戶端的結果。
#[derive(Debug, Clone)]
pub struct ServerMeasurement {
    /// 客戶端收到這筆量測的時間
    pub at: Instant,
    /// 伺服器端從連線開始算起的時間（微秒）
    pub elapsed_us: Option<u64>,
    /// 伺服器端確認傳輸成功的累計位元組數
    /// （下載：對方已確認收到的 BytesAcked；上傳：伺服器收到的 BytesReceived）
    pub bytes: Option<u64>,
    /// 伺服器端核心量到的平滑 RTT（毫秒）
    pub rtt_ms: Option<f64>,
}

pub trait Backend: Send + Sync {
    /// 後端名稱，用於輸出。
    fn name(&self) -> &'static str;

    /// 測試開始前的準備工作，例如向 locate API 查詢最近的伺服器。
    fn prepare(&mut self) -> BoxFuture<'_, anyhow::Result<ServerInfo>>;

    /// 是否支援這個方向的測試。
    fn supports(&self, dir: Direction) -> bool;

    /// 傳輸一條串流，直到伺服器結束或發生錯誤。
    ///
    /// 實作時要遵守：
    /// - 下載：收到資料就把長度加到 `counter`，然後直接丟掉，不要寫入硬碟或累積在記憶體。
    /// - 上傳：資料交給網路堆疊時就把長度加到 `counter`。剛開始會先塞滿 socket 緩衝區
    ///   而偏高，這段由暖機排除；穩定後「交出去的速度」就等於「實際送出的速度」。
    /// - 量的是應用層的 payload（goodput），不含 TCP/IP/TLS 標頭。
    fn transfer(
        &self,
        dir: Direction,
        counter: Arc<AtomicU64>,
    ) -> BoxFuture<'_, anyhow::Result<()>>;

    /// 協定或伺服器政策允許的最大平行連線數。`None` 表示不限制。
    fn max_connections(&self) -> Option<usize> {
        None
    }

    /// 一條串流結束後，是否要馬上再開一條新的。
    ///
    /// - HTTP：`true`，傳完就再請求一次，讓連線一直保持忙碌。
    /// - ndt7：`false`，測試長度由伺服器決定，結束了就是結束了。
    fn restart_streams(&self) -> bool {
        true
    }

    /// 延遲量測的目標（主機, TCP port），應該是實際提供測速資料的那台伺服器。
    /// 必須在 [`Backend::prepare`] 之後呼叫。`None` 表示不支援延遲量測。
    fn latency_target(&self) -> Option<(String, u16)> {
        None
    }

    /// 取出（並清空）上一次測試期間收集到的伺服器端量測。
    fn take_server_measurements(&self) -> Vec<ServerMeasurement> {
        Vec::new()
    }
}

/// 建立共用設定的 HTTP client。
///
/// TLS 使用 ring 加密後端（比 aws-lc-rs 小，編譯也不需要 cmake），
/// 第一次呼叫時註冊為全程式預設值；WebSocket（tokio-tungstenite）也會用到它。
pub fn http_client() -> anyhow::Result<reqwest::Client> {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        // 已經有人註冊過也沒關係
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
    Ok(reqwest::Client::builder()
        .user_agent(concat!("trueput/", env!("CARGO_PKG_VERSION")))
        .build()?)
}

/// 上傳用的不可壓縮資料。產生一次後重複使用（`Bytes` 複製只是增加參考計數）。
pub fn random_payload(len: usize) -> Bytes {
    // xorshift64*：不需要密碼學強度，只要不可壓縮
    let mut x = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0x9E37_79B9_7F4A_7C15, |d| d.as_nanos() as u64)
        | 1;
    let mut buf = Vec::with_capacity(len + 8);
    while buf.len() < len {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        buf.extend_from_slice(&x.wrapping_mul(0x2545_F491_4F6C_DD1D).to_le_bytes());
    }
    buf.truncate(len);
    Bytes::from(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_is_not_trivially_compressible() {
        let p = random_payload(1 << 16);
        assert_eq!(p.len(), 1 << 16);
        // 簡單檢查：位元組值分布要大致平均
        let mut counts = [0usize; 256];
        for b in p.iter() {
            counts[*b as usize] += 1;
        }
        let expected = p.len() / 256;
        assert!(counts.iter().all(|&c| c > expected / 2 && c < expected * 2));
    }
}
