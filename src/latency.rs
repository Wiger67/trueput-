//! 延遲量測：閒置延遲與負載下延遲（bufferbloat）。
//!
//! 量法：對測速伺服器建立 TCP 連線，量三向交握（SYN → SYN-ACK）花的時間，
//! 拿到後立刻關閉連線。
//!
//! - 只量網路來回時間，不含 TLS 交握與伺服器處理時間。
//! - DNS 事先解析一次，不算在延遲裡。
//! - 下載把瓶頸的佇列塞滿時，SYN 封包也要排同一個佇列，
//!   所以「下載中延遲 − 閒置延遲」就是 bufferbloat 造成的額外延遲。

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use serde::Serialize;
use tokio::net::TcpStream;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct Config {
    /// 下載前量幾次閒置延遲
    pub idle_probes: usize,
    /// 閒置量測的間隔
    pub idle_interval: Duration,
    /// 下載期間量測的間隔
    pub interval: Duration,
    /// 超過這個時間沒回應就算遺失
    pub timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// 下載開始前
    Idle,
    /// 下載中、暖機期
    Warmup,
    /// 下載中、暖機後
    Loaded,
}

#[derive(Debug, Clone, Serialize)]
pub struct Probe {
    /// 量測開始的時間點，以下載開始為 0（閒置量測為負值）
    pub t_secs: f64,
    /// 來回時間；`None` 表示逾時或連線失敗
    pub rtt_ms: Option<f64>,
    pub phase: Phase,
}

pub struct Prober {
    addr: SocketAddr,
    timeout: Duration,
}

impl Prober {
    /// 先解析 DNS，之後每次量測都直接連 IP。
    pub async fn resolve(host: &str, port: u16, timeout: Duration) -> anyhow::Result<Self> {
        let addr = tokio::net::lookup_host((host, port))
            .await
            .with_context(|| format!("無法解析 {host}"))?
            .next()
            .with_context(|| format!("{host} 沒有任何 IP 位址"))?;
        Ok(Self { addr, timeout })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// 量一次 TCP 交握時間。逾時或失敗回傳 `None`。
    pub async fn probe(&self) -> Option<Duration> {
        let start = Instant::now();
        match tokio::time::timeout(self.timeout, TcpStream::connect(self.addr)).await {
            Ok(Ok(stream)) => {
                let rtt = start.elapsed();
                drop(stream);
                Some(rtt)
            }
            _ => None,
        }
    }
}

pub fn to_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn listener() -> (TcpListener, u16) {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        (l, port)
    }

    #[tokio::test]
    async fn probe_succeeds_on_open_port() {
        let (_l, port) = listener().await;
        let p = Prober::resolve("127.0.0.1", port, Duration::from_secs(1))
            .await
            .unwrap();
        let rtt = p.probe().await.expect("應該連得上");
        assert!(rtt < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn probe_fails_on_closed_port() {
        // 綁定後立刻關閉，取得一個沒有人在聽的 port
        let port = listener().await.1;
        let p = Prober::resolve("127.0.0.1", port, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(p.probe().await.is_none());
    }
}
