//! 測速伺服器來源（後端）。
//!
//! 後端只負責「把位元組搬進來」：開一條下載串流，把收到的應用層位元組數累加到
//! `counter`。取樣、暖機、統計全部由 [`crate::engine`] 統一處理，
//! 所以不同後端量出來的數字是用同一套方法算的，可以直接互相比較。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use serde::Serialize;

pub mod cloudflare;
pub mod ndt7;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// 給使用者看的伺服器資訊。
#[derive(Debug, Clone, Serialize)]
pub struct ServerInfo {
    /// 主機名稱或機器 ID
    pub name: String,
    /// 地點（城市、國家、機房代碼等），拿不到就是 `None`
    pub location: Option<String>,
}

pub trait Backend: Send + Sync {
    /// 後端名稱，用於輸出。
    fn name(&self) -> &'static str;

    /// 測試開始前的準備工作，例如向 locate API 查詢最近的伺服器。
    fn prepare(&mut self) -> BoxFuture<'_, anyhow::Result<ServerInfo>>;

    /// 下載一條串流，直到伺服器結束或發生錯誤。
    ///
    /// 實作時要遵守：
    /// - 收到資料就把長度加到 `counter`，然後直接丟掉，不要寫入硬碟或累積在記憶體。
    /// - 量的是應用層的 payload（goodput），不含 TCP/IP/TLS 標頭。
    fn download(&self, counter: Arc<AtomicU64>) -> BoxFuture<'_, anyhow::Result<()>>;

    /// 協定或伺服器政策允許的最大平行連線數。`None` 表示不限制。
    fn max_connections(&self) -> Option<usize> {
        None
    }

    /// 一條串流結束後，是否要馬上再開一條新的。
    ///
    /// - HTTP 下載：`true`，檔案下載完就再請求一次，讓連線一直保持忙碌。
    /// - ndt7：`false`，測試長度由伺服器決定，結束了就是結束了。
    fn restart_streams(&self) -> bool {
        true
    }
}
