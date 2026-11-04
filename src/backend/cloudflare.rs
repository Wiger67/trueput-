//! Cloudflare 測速後端（speed.cloudflare.com）。
//!
//! 注意：`__down` 與 `meta` 不是正式公開的 API，Cloudflare 隨時可能修改或限流。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context;
use serde::Deserialize;

use super::{Backend, BoxFuture, ServerInfo};

const HOST: &str = "speed.cloudflare.com";
const BASE: &str = "https://speed.cloudflare.com";

pub struct Cloudflare {
    client: reqwest::Client,
    /// 每次請求下載的位元組數。下載完會再請求一次，所以不需要設得很大。
    bytes_per_request: u64,
}

#[derive(Deserialize)]
struct Meta {
    colo: Option<String>,
    city: Option<String>,
    country: Option<String>,
}

impl Cloudflare {
    pub fn new(bytes_per_request: u64) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("trueput/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            client,
            bytes_per_request,
        })
    }
}

impl Backend for Cloudflare {
    fn name(&self) -> &'static str {
        "cloudflare"
    }

    fn prepare(&mut self) -> BoxFuture<'_, anyhow::Result<ServerInfo>> {
        Box::pin(async move {
            // meta 只用來顯示連到哪個機房，失敗也不影響測速。
            let meta: Option<Meta> = async {
                self.client
                    .get(format!("{BASE}/meta"))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await
            }
            .await
            .ok();

            let location = meta.and_then(|m| {
                let parts: Vec<String> =
                    [m.colo, m.city, m.country].into_iter().flatten().collect();
                (!parts.is_empty()).then(|| parts.join(", "))
            });

            Ok(ServerInfo {
                name: HOST.into(),
                location,
            })
        })
    }

    fn download(&self, counter: Arc<AtomicU64>) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let url = format!("{BASE}/__down?bytes={}", self.bytes_per_request);
            let mut resp = self
                .client
                .get(url)
                // 明確要求不壓縮：壓縮過的資料會讓速度看起來比實際快
                .header(reqwest::header::ACCEPT_ENCODING, "identity")
                .send()
                .await
                .context("連線到 Cloudflare 失敗")?
                .error_for_status()?;

            while let Some(chunk) = resp.chunk().await? {
                counter.fetch_add(chunk.len() as u64, Ordering::Relaxed);
            }
            Ok(())
        })
    }

    fn latency_target(&self) -> Option<(String, u16)> {
        Some((HOST.into(), 443))
    }
}
