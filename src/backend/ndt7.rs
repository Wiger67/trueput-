//! M-Lab ndt7 測速後端。
//!
//! 協定規格：<https://github.com/m-lab/ndt-server/blob/main/spec/ndt7-protocol.md>
//!
//! 流程：
//! 1. 向 locate API 查詢最近的 ndt7 伺服器，取得帶 access token 的 WebSocket URL。
//! 2. 用子協定 `net.measurementlab.ndt.v7` 連上 `/ndt/v7/download` 或 `/ndt/v7/upload`。
//! 3. 下載：伺服器持續送二進位訊息；上傳：客戶端持續送二進位訊息。
//!    兩種情況下伺服器都會定期送文字訊息（JSON），內含伺服器核心的 TCPInfo，
//!    我們拿來交叉驗證客戶端的結果（RTT、實際確認的位元組數）。
//!
//! 依照 M-Lab 的規範，ndt7 每次測試只用一條連線，長度約 10 秒。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, anyhow};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderValue, Uri};
use tokio_tungstenite::tungstenite::{Bytes, Message};

use super::{
    Backend, BoxFuture, Direction, ServerInfo, ServerMeasurement, http_client, random_payload,
};

const LOCATE_URL: &str = "https://locate.measurementlab.net/v2/nearest/ndt/ndt7";
const SUBPROTOCOL: &str = "net.measurementlab.ndt.v7";
const DOWNLOAD_KEY: &str = "wss:///ndt/v7/download";
const UPLOAD_KEY: &str = "wss:///ndt/v7/upload";

/// 規格建議：上傳訊息從 8 KiB 開始，已送出量達到目前訊息大小的 16 倍時加倍，上限 16 MiB。
const UPLOAD_MIN_MSG: usize = 1 << 13;
const UPLOAD_MAX_MSG: usize = 1 << 24;
/// 規格建議的上傳測試長度
const UPLOAD_DURATION: Duration = Duration::from_secs(10);

pub struct Ndt7 {
    client: reqwest::Client,
    download_url: Option<String>,
    upload_url: Option<String>,
    measurements: Mutex<Vec<ServerMeasurement>>,
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

/// 伺服器送來的量測訊息（只取需要的欄位）
#[derive(Deserialize)]
struct Measurement {
    #[serde(rename = "TCPInfo")]
    tcp_info: Option<TcpInfo>,
}

#[derive(Deserialize)]
struct TcpInfo {
    /// 微秒
    #[serde(rename = "ElapsedTime")]
    elapsed_time: Option<u64>,
    /// 平滑 RTT，微秒
    #[serde(rename = "RTT")]
    rtt: Option<u64>,
    #[serde(rename = "BytesAcked")]
    bytes_acked: Option<u64>,
    #[serde(rename = "BytesReceived")]
    bytes_received: Option<u64>,
}

fn parse_measurement(text: &str, dir: Direction) -> Option<ServerMeasurement> {
    let tcp = serde_json::from_str::<Measurement>(text).ok()?.tcp_info?;
    Some(ServerMeasurement {
        at: Instant::now(),
        elapsed_us: tcp.elapsed_time,
        bytes: match dir {
            Direction::Download => tcp.bytes_acked,
            Direction::Upload => tcp.bytes_received,
        },
        rtt_ms: tcp.rtt.map(|us| us as f64 / 1000.0),
    })
}

impl Ndt7 {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            client: http_client()?,
            download_url: None,
            upload_url: None,
            measurements: Mutex::new(Vec::new()),
        })
    }

    fn record(&self, text: &str, dir: Direction) {
        if let Some(m) = parse_measurement(text, dir) {
            self.measurements.lock().unwrap().push(m);
        }
    }

    async fn connect(
        &self,
        dir: Direction,
    ) -> anyhow::Result<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    > {
        let url = match dir {
            Direction::Download => &self.download_url,
            Direction::Upload => &self.upload_url,
        }
        .clone()
        .ok_or_else(|| anyhow!("尚未呼叫 prepare()，或伺服器不支援{}", dir.label()))?;

        let mut request = url.into_client_request()?;
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static(SUBPROTOCOL),
        );
        let (ws, _) = tokio_tungstenite::connect_async(request)
            .await
            .context("無法連線到 ndt7 伺服器")?;
        Ok(ws)
    }

    async fn download(&self, counter: Arc<AtomicU64>) -> anyhow::Result<()> {
        let mut ws = self.connect(Direction::Download).await?;
        while let Some(msg) = ws.next().await {
            match msg? {
                // 規格要求客戶端把收到的所有應用層訊息都算進去
                Message::Binary(data) => {
                    counter.fetch_add(data.len() as u64, Ordering::Relaxed);
                }
                Message::Text(text) => {
                    counter.fetch_add(text.len() as u64, Ordering::Relaxed);
                    self.record(&text, Direction::Download);
                }
                Message::Close(_) => break,
                // Ping/Pong 由 tungstenite 自動處理
                _ => {}
            }
        }
        Ok(())
    }

    async fn upload(&self, counter: Arc<AtomicU64>) -> anyhow::Result<()> {
        let ws = self.connect(Direction::Upload).await?;
        let (mut sink, mut stream) = ws.split();
        let payload: Bytes = random_payload(UPLOAD_MAX_MSG);

        let sender = async {
            let start = Instant::now();
            let (mut size, mut sent) = (UPLOAD_MIN_MSG, 0u64);
            while start.elapsed() < UPLOAD_DURATION {
                // send() 會等到訊息寫進 socket 才完成，所以有背壓
                sink.send(Message::Binary(payload.slice(..size))).await?;
                counter.fetch_add(size as u64, Ordering::Relaxed);
                sent += size as u64;
                if size < UPLOAD_MAX_MSG && sent >= 16 * size as u64 {
                    size *= 2;
                }
            }
            let _ = sink.send(Message::Close(None)).await;
            anyhow::Ok(())
        };

        let reader = async {
            while let Some(msg) = stream.next().await {
                match msg? {
                    Message::Text(text) => self.record(&text, Direction::Upload),
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            anyhow::Ok(())
        };

        // 任一邊結束（送滿 10 秒，或伺服器關閉連線）就結束
        tokio::select! {
            r = sender => r,
            r = reader => r,
        }
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
            let mut server = resp
                .results
                .into_iter()
                .find(|r| r.urls.contains_key(DOWNLOAD_KEY))
                .ok_or_else(|| anyhow!("locate API 沒有回傳可用的 ndt7 伺服器"))?;

            self.download_url = server.urls.remove(DOWNLOAD_KEY);
            self.upload_url = server.urls.remove(UPLOAD_KEY);

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

    fn supports(&self, dir: Direction) -> bool {
        match dir {
            Direction::Download => self.download_url.is_some(),
            Direction::Upload => self.upload_url.is_some(),
        }
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

    fn max_connections(&self) -> Option<usize> {
        Some(1)
    }

    fn restart_streams(&self) -> bool {
        false
    }

    fn latency_target(&self) -> Option<(String, u16)> {
        let uri: Uri = self.download_url.as_deref()?.parse().ok()?;
        Some((uri.host()?.to_owned(), uri.port_u16().unwrap_or(443)))
    }

    fn take_server_measurements(&self) -> Vec<ServerMeasurement> {
        std::mem::take(&mut *self.measurements.lock().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
        "AppInfo": {"ElapsedTime": 1000000, "NumBytes": 12500000},
        "Origin": "server",
        "Test": "download",
        "TCPInfo": {
            "ElapsedTime": 1000123, "RTT": 23456, "MinRTT": 12000,
            "BytesAcked": 12000000, "BytesReceived": 345
        }
    }"#;

    #[test]
    fn parses_server_tcpinfo() {
        let m = parse_measurement(SAMPLE, Direction::Download).unwrap();
        assert_eq!(m.elapsed_us, Some(1_000_123));
        assert_eq!(m.bytes, Some(12_000_000));
        assert_eq!(m.rtt_ms, Some(23.456));

        let m = parse_measurement(SAMPLE, Direction::Upload).unwrap();
        assert_eq!(m.bytes, Some(345));
    }

    #[test]
    fn ignores_messages_without_tcpinfo() {
        assert!(
            parse_measurement(r#"{"AppInfo": {"NumBytes": 1}}"#, Direction::Download).is_none()
        );
        assert!(parse_measurement("not json", Direction::Download).is_none());
    }

    // ── 用本機 WebSocket 伺服器走完整協定 ──

    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    /// 接受一條 WebSocket 連線，回應 ndt7 子協定（客戶端會檢查伺服器有沒有回）。
    // 回呼的錯誤型別由 tungstenite 決定，不是我們能改的
    #[allow(clippy::result_large_err)]
    async fn accept(
        listener: &TcpListener,
    ) -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
        let (tcp, _) = listener.accept().await.unwrap();
        tokio_tungstenite::accept_hdr_async(tcp, |req: &Request, mut resp: Response| {
            assert_eq!(
                req.headers().get("Sec-WebSocket-Protocol").unwrap(),
                SUBPROTOCOL
            );
            resp.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                HeaderValue::from_static(SUBPROTOCOL),
            );
            Ok(resp)
        })
        .await
        .unwrap()
    }

    fn tcpinfo(elapsed_us: u64, bytes: u64, dir: Direction) -> String {
        let (acked, received) = match dir {
            Direction::Download => (bytes, 0),
            Direction::Upload => (0, bytes),
        };
        format!(
            r#"{{"TCPInfo":{{"ElapsedTime":{elapsed_us},"RTT":15000,"BytesAcked":{acked},"BytesReceived":{received}}}}}"#
        )
    }

    async fn local_backend(path: &str) -> (Ndt7, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/ndt/v7/{path}", listener.local_addr().unwrap());
        let mut b = Ndt7::new().unwrap();
        match path {
            "download" => b.download_url = Some(url),
            _ => b.upload_url = Some(url),
        }
        (b, listener)
    }

    #[tokio::test]
    async fn download_over_websocket() {
        let (backend, listener) = local_backend("download").await;
        let server = tokio::spawn(async move {
            let mut ws = accept(&listener).await;
            let chunk = Bytes::from(vec![0u8; 1 << 16]);
            for i in 1..=20u64 {
                ws.send(Message::Binary(chunk.clone())).await.unwrap();
                if i % 10 == 0 {
                    let text = tcpinfo(i * 1000, i << 16, Direction::Download);
                    ws.send(Message::text(text)).await.unwrap();
                }
            }
            ws.close(None).await.unwrap();
        });

        let counter = Arc::new(AtomicU64::new(0));
        backend
            .transfer(Direction::Download, counter.clone())
            .await
            .unwrap();
        server.await.unwrap();

        // 20 個 64 KiB 二進位訊息 + 2 個文字訊息都要算進去
        assert!(counter.load(Ordering::Relaxed) > 20 << 16);
        let m = backend.take_server_measurements();
        assert_eq!(m.len(), 2);
        assert_eq!(m[1].bytes, Some(20 << 16));
        assert_eq!(m[1].rtt_ms, Some(15.0));
        assert!(
            backend.take_server_measurements().is_empty(),
            "取出後要清空"
        );
    }

    #[tokio::test]
    async fn upload_over_websocket() {
        let (backend, listener) = local_backend("upload").await;
        let server = tokio::spawn(async move {
            let mut ws = accept(&listener).await;
            let mut received = 0u64;
            // 收一段資料後回報 TCPInfo，然後關閉（不用等滿 10 秒）
            while received < 4 << 20 {
                match ws.next().await.unwrap().unwrap() {
                    Message::Binary(b) => received += b.len() as u64,
                    other => panic!("unexpected {other:?}"),
                }
            }
            let text = tcpinfo(500_000, received, Direction::Upload);
            ws.send(Message::text(text)).await.unwrap();
            ws.close(None).await.unwrap();
            // 把剩下的訊息讀完，讓關閉流程完成
            while let Some(Ok(_)) = ws.next().await {}
            received
        });

        let counter = Arc::new(AtomicU64::new(0));
        backend
            .transfer(Direction::Upload, counter.clone())
            .await
            .unwrap();
        let received = server.await.unwrap();

        let sent = counter.load(Ordering::Relaxed);
        assert!(sent >= received, "sent {sent} < received {received}");
        let m = backend.take_server_measurements();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].bytes, Some(received));
    }
}
