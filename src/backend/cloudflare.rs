//! Cloudflare 測速後端（speed.cloudflare.com）。
//!
//! - 下載：`GET /__down?bytes=N`
//! - 上傳：`POST /__up`，body 為不可壓縮的隨機資料
//!
//! 注意：這些端點不是正式公開的 API，Cloudflare 隨時可能修改或限流。

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context as TaskContext, Poll};

use anyhow::Context;
use bytes::Bytes;
use http_body::{Frame, SizeHint};
use serde::Deserialize;

use super::{Backend, BoxFuture, Direction, ServerInfo, http_client, random_payload};

const HOST: &str = "speed.cloudflare.com";
const BASE: &str = "https://speed.cloudflare.com";
/// 每次上傳請求的大小。傳完會再請求一次；設太大可能被伺服器拒絕。
const UPLOAD_BYTES_PER_REQUEST: u64 = 10_000_000;
/// 每次交給 HTTP 堆疊的區塊大小
const UPLOAD_CHUNK: usize = 64 * 1024;

pub struct Cloudflare {
    client: reqwest::Client,
    /// 每次請求下載的位元組數。下載完會再請求一次，所以不需要設得很大。
    bytes_per_request: u64,
    payload: Bytes,
}

/// 上傳用的 body：hyper 每取走一個區塊才計數，所以有背壓——
/// 網路送不出去時 hyper 不會再來拿，計數也就不會虛增。
struct CountingBody {
    payload: Bytes,
    remaining: u64,
    counter: Arc<AtomicU64>,
}

impl http_body::Body for CountingBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        if this.remaining == 0 {
            return Poll::Ready(None);
        }
        let n = this.remaining.min(this.payload.len() as u64) as usize;
        this.remaining -= n as u64;
        this.counter.fetch_add(n as u64, Ordering::Relaxed);
        Poll::Ready(Some(Ok(Frame::data(this.payload.slice(..n)))))
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }
}

#[derive(Deserialize)]
struct Meta {
    colo: Option<String>,
    city: Option<String>,
    country: Option<String>,
}

impl Cloudflare {
    pub fn new(bytes_per_request: u64) -> anyhow::Result<Self> {
        Ok(Self {
            client: http_client()?,
            bytes_per_request,
            payload: random_payload(UPLOAD_CHUNK),
        })
    }

    async fn download(&self, counter: Arc<AtomicU64>) -> anyhow::Result<()> {
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
    }

    async fn upload(&self, counter: Arc<AtomicU64>) -> anyhow::Result<()> {
        let body = CountingBody {
            payload: self.payload.clone(),
            remaining: UPLOAD_BYTES_PER_REQUEST,
            counter,
        };
        let mut resp = self
            .client
            .post(format!("{BASE}/__up"))
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(reqwest::Body::wrap(body))
            .send()
            .await
            .context("連線到 Cloudflare 失敗")?
            .error_for_status()?;
        while resp.chunk().await?.is_some() {}
        Ok(())
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

    fn supports(&self, _dir: Direction) -> bool {
        true
    }

    fn transfer(
        &self,
        dir: Direction,
        counter: Arc<AtomicU64>,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        match dir {
            Direction::Download => Box::pin(self.download(counter)),
            Direction::Upload => Box::pin(self.upload(counter)),
        }
    }

    fn latency_target(&self) -> Option<(String, u16)> {
        Some((HOST.into(), 443))
    }
}
