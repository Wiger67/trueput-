//! M-Lab ndt7 測速後端。
//!
//! 協定規格：<https://github.com/m-lab/ndt-server/blob/main/spec/ndt7-protocol.md>
//!
//! 流程：
//! 1. 向 locate API 查詢最近的 ndt7 伺服器，取得帶 access token 的 WebSocket URL。
//! 2. 用子協定 `net.measurementlab.ndt.v7` 連上 `/ndt/v7/download`。
//! 3. 伺服器會持續送二進位訊息（測速資料），偶爾夾帶文字訊息（伺服器端量測，JSON）。
//!    測試長度由伺服器決定（約 10 秒），結束時伺服器關閉連線。
//!
//! 依照 M-Lab 的規範，ndt7 每次測試只用一條連線。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, anyhow};
use futures_util::StreamExt;
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use super::{Backend, BoxFuture, ServerInfo};

const LOCATE_URL: &str = "https://locate.measurementlab.net/v2/nearest/ndt/ndt7";
const SUBPROTOCOL: &str = "net.measurementlab.ndt.v7";
const DOWNLOAD_KEY: &str = "wss:///ndt/v7/download";

pub struct Ndt7 {
    client: reqwest::Client,
    download_url: Option<String>,
}

#[derive(Deserialize)]
struct LocateResponse {
    results: Vec<LocateResult>,
}

#[derive(Deserialize)]
struct LocateResult {
    machine: String,
    location: Option<LocateLocation>,
    urls: std::collections::HashMap<String, String>,
}

#[derive(Deserialize)]
struct LocateLocation {
    city: Option<String>,
    country: Option<String>,
}

impl Ndt7 {
    pub fn new() -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("trueput/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            client,
            download_url: None,
        })
    }
}

impl Backend for Ndt7 {
    fn name(&self) -> &'static str {
        "ndt7"
    }

    fn prepare(&mut self) -> BoxFuture<'_, anyhow::Result<ServerInfo>> {
        Box::pin(async move {
            let resp: LocateResponse = self
                .client
                .get(LOCATE_URL)
                .send()
                .await
                .context("無法連線到 M-Lab locate API")?
                .error_for_status()?
                .json()
                .await
                .context("locate API 回應格式不符")?;

            // locate 已依距離排序，取第一台有提供 wss 下載網址的伺服器
            let server = resp
                .results
                .into_iter()
                .find(|r| r.urls.contains_key(DOWNLOAD_KEY))
                .ok_or_else(|| anyhow!("locate API 沒有回傳可用的 ndt7 伺服器"))?;

            self.download_url = server.urls.get(DOWNLOAD_KEY).cloned();

            let location = server.location.and_then(|l| {
                let parts: Vec<String> = [l.city, l.country].into_iter().flatten().collect();
                (!parts.is_empty()).then(|| parts.join(", "))
            });

            Ok(ServerInfo {
                name: server.machine,
                location,
            })
        })
    }

    fn download(&self, counter: Arc<AtomicU64>) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let url = self
                .download_url
                .clone()
                .ok_or_else(|| anyhow!("尚未呼叫 prepare()"))?;

            let mut request = url.into_client_request()?;
            request.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                HeaderValue::from_static(SUBPROTOCOL),
            );

            let (mut ws, _) = tokio_tungstenite::connect_async(request)
                .await
                .context("無法連線到 ndt7 伺服器")?;

            while let Some(msg) = ws.next().await {
                match msg? {
                    // 規格要求客戶端把收到的所有應用層訊息都算進去
                    Message::Binary(data) => {
                        counter.fetch_add(data.len() as u64, Ordering::Relaxed);
                    }
                    Message::Text(text) => {
                        counter.fetch_add(text.len() as u64, Ordering::Relaxed);
                        // TODO: 解析伺服器端量測（TCPInfo、BBRInfo），可用來交叉驗證客戶端結果
                    }
                    Message::Close(_) => break,
                    // Ping/Pong 由 tungstenite 自動處理
                    _ => {}
                }
            }
            Ok(())
        })
    }

    fn max_connections(&self) -> Option<usize> {
        Some(1)
    }

    fn restart_streams(&self) -> bool {
        false
    }
}
