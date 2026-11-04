//! 測速引擎：開平行連線、定時取樣、區分暖機期、收集結果。
//!
//! 測量方法：
//! - 開 N 條平行串流（受後端的 `max_connections` 限制），全部累加到同一個計數器。
//! - 每隔 `interval` 讀一次計數器，用「這段期間的位元組數 ÷ 實際經過時間」算出一個樣本。
//!   用實際時間而不是假設的 interval，排程延遲才不會影響結果。
//! - 前 `warmup` 秒的樣本標記為暖機（TCP 慢啟動），不列入統計。
//! - 時間到或所有串流都結束就停止。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Serialize;
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};

use crate::backend::Backend;
use crate::stats::mbps;

#[derive(Debug, Clone)]
pub struct Config {
    /// 想要的平行連線數（實際值可能被後端限制）
    pub connections: usize,
    /// 測試總長度上限（含暖機）
    pub duration: Duration,
    /// 暖機長度，這段期間的樣本不列入統計
    pub warmup: Duration,
    /// 取樣間隔
    pub interval: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct Sample {
    /// 從測試開始算起的秒數（這個樣本的結束時間點）
    pub t_secs: f64,
    pub mbps: f64,
    pub warmup: bool,
}

#[derive(Debug)]
pub struct Outcome {
    pub samples: Vec<Sample>,
    /// 實際使用的連線數
    pub connections: usize,
    pub total_bytes: u64,
    pub elapsed_secs: f64,
    /// 暖機後收到的位元組數與經過時間
    pub steady_bytes: u64,
    pub steady_secs: f64,
    /// 個別串流的錯誤。只要還有資料進來，測試就會繼續，但結果可能偏低。
    pub stream_errors: Vec<String>,
}

impl Outcome {
    /// 暖機後的樣本速度（Mbps）
    pub fn steady_samples(&self) -> Vec<f64> {
        self.samples
            .iter()
            .filter(|s| !s.warmup)
            .map(|s| s.mbps)
            .collect()
    }

    pub fn steady_avg_mbps(&self) -> f64 {
        mbps(self.steady_bytes, self.steady_secs)
    }
}

pub async fn run(
    backend: Arc<dyn Backend>,
    cfg: &Config,
    mut on_sample: impl FnMut(&Sample),
) -> anyhow::Result<Outcome> {
    let connections = backend
        .max_connections()
        .map_or(cfg.connections, |max| cfg.connections.min(max))
        .max(1);

    let counter = Arc::new(AtomicU64::new(0));
    let mut workers = JoinSet::new();
    let start = Instant::now();

    for _ in 0..connections {
        let (backend, counter) = (backend.clone(), counter.clone());
        workers.spawn(async move {
            loop {
                backend.download(counter.clone()).await?;
                if !backend.restart_streams() {
                    return anyhow::Ok(());
                }
            }
        });
    }

    let mut tick = tokio::time::interval(cfg.interval);
    // 落後時不要連續補 tick，否則會產生時間極短、數值很亂的樣本
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick.tick().await; // 第一次 tick 會立刻完成

    let mut samples = Vec::new();
    let mut stream_errors = Vec::new();
    let (mut last_bytes, mut last_t) = (0u64, start);
    let mut steady_start: Option<(u64, Instant)> = None;

    loop {
        tick.tick().await;
        let now = Instant::now();
        let bytes = counter.load(Ordering::Relaxed);
        let elapsed = now - start;

        let sample = Sample {
            t_secs: elapsed.as_secs_f64(),
            mbps: mbps(bytes - last_bytes, (now - last_t).as_secs_f64()),
            warmup: elapsed <= cfg.warmup,
        };
        if !sample.warmup && steady_start.is_none() {
            steady_start = Some((last_bytes, last_t));
        }
        on_sample(&sample);
        samples.push(sample);
        (last_bytes, last_t) = (bytes, now);

        while let Some(res) = workers.try_join_next() {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => stream_errors.push(format!("{e:#}")),
                Err(e) => stream_errors.push(e.to_string()),
            }
        }

        if workers.is_empty() || elapsed >= cfg.duration {
            break;
        }
    }
    workers.abort_all();

    let total_bytes = last_bytes;
    if total_bytes == 0 {
        anyhow::bail!(
            "沒有收到任何資料：{}",
            stream_errors.first().map_or("未知原因", String::as_str)
        );
    }

    let (steady_bytes, steady_secs) = match steady_start {
        Some((b0, t0)) => (last_bytes - b0, (last_t - t0).as_secs_f64()),
        None => (0, 0.0),
    };

    Ok(Outcome {
        samples,
        connections,
        total_bytes,
        elapsed_secs: (last_t - start).as_secs_f64(),
        steady_bytes,
        steady_secs,
        stream_errors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{BoxFuture, ServerInfo};

    /// 假後端：每 10ms 送出固定位元組數，可設定串流長度與是否失敗。
    struct Fake {
        bytes_per_10ms: u64,
        stream_len: Option<Duration>,
        fail: bool,
        max_conn: Option<usize>,
    }

    impl Backend for Fake {
        fn name(&self) -> &'static str {
            "fake"
        }
        fn prepare(&mut self) -> BoxFuture<'_, anyhow::Result<ServerInfo>> {
            Box::pin(async {
                Ok(ServerInfo {
                    name: "fake".into(),
                    location: None,
                })
            })
        }
        fn download(&self, counter: Arc<AtomicU64>) -> BoxFuture<'_, anyhow::Result<()>> {
            Box::pin(async move {
                if self.fail {
                    anyhow::bail!("boom");
                }
                let start = Instant::now();
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    counter.fetch_add(self.bytes_per_10ms, Ordering::Relaxed);
                    if self.stream_len.is_some_and(|len| start.elapsed() >= len) {
                        return Ok(());
                    }
                }
            })
        }
        fn max_connections(&self) -> Option<usize> {
            self.max_conn
        }
        fn restart_streams(&self) -> bool {
            self.stream_len.is_none()
        }
    }

    fn cfg(connections: usize) -> Config {
        Config {
            connections,
            duration: Duration::from_secs(5),
            warmup: Duration::from_secs(1),
            interval: Duration::from_millis(200),
        }
    }

    fn fake() -> Fake {
        // 125,000 bytes / 10ms = 100 Mbps
        Fake {
            bytes_per_10ms: 125_000,
            stream_len: None,
            fail: false,
            max_conn: None,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn measures_aggregate_throughput() {
        let out = run(Arc::new(fake()), &cfg(4), |_| {}).await.unwrap();
        assert_eq!(out.connections, 4);
        let avg = out.steady_avg_mbps();
        assert!((avg - 400.0).abs() < 400.0 * 0.05, "avg = {avg}");
        assert!(out.samples.iter().any(|s| s.warmup));
        assert!(out.samples.iter().any(|s| !s.warmup));
    }

    #[tokio::test(start_paused = true)]
    async fn respects_backend_connection_limit() {
        let backend = Fake {
            max_conn: Some(1),
            ..fake()
        };
        let out = run(Arc::new(backend), &cfg(8), |_| {}).await.unwrap();
        assert_eq!(out.connections, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn stops_when_streams_finish() {
        let backend = Fake {
            stream_len: Some(Duration::from_secs(2)),
            ..fake()
        };
        let out = run(Arc::new(backend), &cfg(1), |_| {}).await.unwrap();
        assert!(out.elapsed_secs < 3.0, "elapsed = {}", out.elapsed_secs);
    }

    #[tokio::test(start_paused = true)]
    async fn errors_when_no_data() {
        let backend = Fake {
            fail: true,
            ..fake()
        };
        let err = run(Arc::new(backend), &cfg(2), |_| {}).await.unwrap_err();
        assert!(err.to_string().contains("boom"));
    }
}
