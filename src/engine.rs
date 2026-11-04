//! 測速引擎：開平行連線、定時取樣、區分暖機期、收集結果。
//!
//! 測量方法：
//! - （可選）下載前先量閒置延遲。
//! - 開平行串流（受後端的 `max_connections` 限制），全部累加到同一個計數器。
//!   連線數可以固定，或由 [`crate::autoscale`] 動態調整。
//! - 每隔 `interval` 讀一次計數器，用「這段期間的位元組數 ÷ 實際經過時間」算出一個樣本。
//!   用實際時間而不是假設的 interval，排程延遲才不會影響結果。
//! - 下載期間另外有一個 task 持續量延遲（負載下延遲），細節見 [`crate::latency`]。
//! - 暖機（TCP 慢啟動、連線數調整中）的樣本不列入統計。暖機要同時滿足兩個條件才結束：
//!   經過至少 `warmup` 秒，而且連線數已經鎖定。
//! - 時間到或所有串流都結束就停止。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};

use crate::autoscale::{AutoConfig, Scaler};
use crate::backend::Backend;
use crate::latency::{self, Phase, Probe, Prober, to_ms};
use crate::stats::mbps;

#[derive(Debug, Clone)]
pub enum Connections {
    /// 固定連線數
    Fixed(usize),
    /// 動態調整
    Auto(AutoConfig),
}

#[derive(Debug, Clone)]
pub struct Config {
    /// 平行連線數（實際值會受後端限制）
    pub connections: Connections,
    /// 測試總長度上限（含暖機）
    pub duration: Duration,
    /// 最短暖機長度；自動模式下，連線數鎖定前也算暖機
    pub warmup: Duration,
    /// 取樣間隔
    pub interval: Duration,
    /// 延遲量測設定；`None` 表示不量
    pub latency: Option<latency::Config>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Sample {
    /// 從測試開始算起的秒數（這個樣本的結束時間點）
    pub t_secs: f64,
    pub mbps: f64,
    pub warmup: bool,
    /// 這個樣本期間的連線數
    pub connections: usize,
    /// 取樣當下最近一次的延遲量測，只用於即時顯示
    #[serde(skip)]
    pub latest_rtt_ms: Option<f64>,
}

/// 連線數的變化紀錄
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionStep {
    pub t_secs: f64,
    pub connections: usize,
}

#[derive(Debug, Default)]
pub struct LatencyOutcome {
    /// 實際量測的 IP 與 port
    pub addr: Option<String>,
    pub probes: Vec<Probe>,
    /// 無法量測延遲的原因（不影響下載測速）
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct Outcome {
    pub samples: Vec<Sample>,
    /// 最終使用的連線數
    pub connections: usize,
    /// 連線數的變化（第一筆為初始值）
    pub connection_steps: Vec<ConnectionStep>,
    /// 暖機實際結束的時間點；`None` 表示整場測試都在暖機
    pub steady_start_secs: Option<f64>,
    pub total_bytes: u64,
    pub elapsed_secs: f64,
    /// 暖機後收到的位元組數與經過時間
    pub steady_bytes: u64,
    pub steady_secs: f64,
    /// 個別串流的錯誤。只要還有資料進來，測試就會繼續，但結果可能偏低。
    pub stream_errors: Vec<String>,
    /// 延遲量測結果；`None` 表示沒有啟用或後端不支援
    pub latency: Option<LatencyOutcome>,
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

/// 量閒置延遲。成功的話回傳 prober 供下載期間繼續使用。
async fn measure_idle(
    backend: &dyn Backend,
    cfg: &latency::Config,
) -> (
    LatencyOutcome,
    Vec<(Instant, Option<Duration>)>,
    Option<Arc<Prober>>,
) {
    let mut out = LatencyOutcome::default();
    let Some((host, port)) = backend.latency_target() else {
        out.error = Some("後端沒有提供延遲量測目標".into());
        return (out, Vec::new(), None);
    };
    let prober = match Prober::resolve(&host, port, cfg.timeout).await {
        Ok(p) => p,
        Err(e) => {
            out.error = Some(format!("{e:#}"));
            return (out, Vec::new(), None);
        }
    };
    out.addr = Some(prober.addr().to_string());

    let mut idle = Vec::with_capacity(cfg.idle_probes);
    for i in 0..cfg.idle_probes {
        if i > 0 {
            tokio::time::sleep(cfg.idle_interval).await;
        }
        let at = Instant::now();
        idle.push((at, prober.probe().await));
    }

    if idle.iter().all(|(_, rtt)| rtt.is_none()) {
        out.error = Some(format!(
            "閒置延遲量測全部失敗（{} 可能擋掉了額外的 TCP 連線）",
            prober.addr()
        ));
        return (out, idle, None);
    }
    (out, idle, Some(Arc::new(prober)))
}

pub async fn run(
    backend: Arc<dyn Backend>,
    cfg: &Config,
    mut on_sample: impl FnMut(&Sample),
) -> anyhow::Result<Outcome> {
    let limit = backend.max_connections().unwrap_or(usize::MAX).max(1);
    let (mut connections, mut scaler, window) = match &cfg.connections {
        Connections::Fixed(n) => ((*n).clamp(1, limit), Scaler::fixed(), Duration::MAX),
        Connections::Auto(a) => {
            let max = a.max.clamp(1, limit);
            let start = a.start.clamp(1, max);
            (
                start,
                Scaler::auto(start, max, a.growth_threshold),
                a.window,
            )
        }
    };

    // ── 閒置延遲 ───────────────────────────────────────────
    let (mut latency_out, idle_raw, prober) = match &cfg.latency {
        Some(lc) => {
            let (out, idle, prober) = measure_idle(backend.as_ref(), lc).await;
            (Some(out), idle, prober)
        }
        None => (None, Vec::new(), None),
    };

    // ── 下載 ───────────────────────────────────────────────
    let counter = Arc::new(AtomicU64::new(0));
    let mut workers = JoinSet::new();
    let start = Instant::now();

    let spawn_worker = |workers: &mut JoinSet<anyhow::Result<()>>| {
        let (backend, counter) = (backend.clone(), counter.clone());
        workers.spawn(async move {
            loop {
                backend.download(counter.clone()).await?;
                if !backend.restart_streams() {
                    return Ok(());
                }
            }
        });
    };
    for _ in 0..connections {
        spawn_worker(&mut workers);
    }
    let mut connection_steps = vec![ConnectionStep {
        t_secs: 0.0,
        connections,
    }];
    // 延遲量測 task 用這個旗標判斷目前是暖機還是正式量測
    let steady = Arc::new(AtomicBool::new(false));

    // ── 負載下延遲：與下載同時進行 ─────────────────────────
    // 一次只有一個量測在進行；量測慢的時候不會越積越多。
    let loaded_probes: Arc<Mutex<Vec<Probe>>> = Arc::default();
    let probe_task = match (&cfg.latency, prober) {
        (Some(lc), Some(prober)) => {
            let (probes, interval, steady) = (loaded_probes.clone(), lc.interval, steady.clone());
            Some(tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    let at = Instant::now();
                    let phase = if steady.load(Ordering::Relaxed) {
                        Phase::Loaded
                    } else {
                        Phase::Warmup
                    };
                    let rtt = prober.probe().await;
                    probes.lock().unwrap().push(Probe {
                        t_secs: (at - start).as_secs_f64(),
                        rtt_ms: rtt.map(to_ms),
                        phase,
                    });
                }
            }))
        }
        _ => None,
    };

    let mut tick = tokio::time::interval(cfg.interval);
    // 落後時不要連續補 tick，否則會產生時間極短、數值很亂的樣本
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick.tick().await; // 第一次 tick 會立刻完成

    let mut samples = Vec::new();
    let mut stream_errors = Vec::new();
    let (mut last_bytes, mut last_t) = (0u64, start);
    let mut steady_start: Option<(u64, Instant)> = None;
    let (mut window_bytes, mut window_t) = (0u64, start);

    loop {
        tick.tick().await;
        let now = Instant::now();
        let bytes = counter.load(Ordering::Relaxed);
        let elapsed = now - start;

        // 這個樣本期間的連線數（調整前）
        let sample_connections = connections;

        // 測試過半還沒穩定就強制鎖定，確保有足夠的正式樣本。
        // 要在調整之前檢查，避免在正式量測開始的瞬間又加開新連線。
        if elapsed >= cfg.duration / 2 {
            scaler.lock();
        }
        // ── 連線數調整：每個窗格評估一次 ──
        if !scaler.locked() && now - window_t >= window {
            let rate = mbps(bytes - window_bytes, (now - window_t).as_secs_f64());
            if let Some(target) = scaler.observe(rate, connections) {
                for _ in connections..target {
                    spawn_worker(&mut workers);
                }
                connections = target;
                connection_steps.push(ConnectionStep {
                    t_secs: elapsed.as_secs_f64(),
                    connections,
                });
            }
            (window_bytes, window_t) = (bytes, now);
        }
        let sample = Sample {
            t_secs: elapsed.as_secs_f64(),
            connections: sample_connections,
            mbps: mbps(bytes - last_bytes, (now - last_t).as_secs_f64()),
            // 鎖定是在這次取樣時才決定的，所以這一段仍然算暖機
            warmup: steady_start.is_none(),
            latest_rtt_ms: loaded_probes.lock().unwrap().last().and_then(|p| p.rtt_ms),
        };
        on_sample(&sample);
        samples.push(sample);
        (last_bytes, last_t) = (bytes, now);

        if steady_start.is_none() && elapsed >= cfg.warmup && scaler.locked() {
            steady_start = Some((bytes, now));
            steady.store(true, Ordering::Relaxed);
        }

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
    // 先停延遲量測，避免下載結束後量到的「閒置」數值混進負載下延遲
    if let Some(task) = probe_task {
        task.abort();
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

    if let Some(out) = latency_out.as_mut() {
        out.probes = idle_raw
            .into_iter()
            .map(|(at, rtt)| Probe {
                t_secs: -(start - at).as_secs_f64(),
                rtt_ms: rtt.map(to_ms),
                phase: Phase::Idle,
            })
            .collect();
        out.probes
            .extend(std::mem::take(&mut *loaded_probes.lock().unwrap()));
        // 最後一次取樣之後才開始的量測不算
        let end = (last_t - start).as_secs_f64();
        out.probes.retain(|p| p.t_secs <= end);
    }

    Ok(Outcome {
        samples,
        connections,
        connection_steps,
        steady_start_secs: steady_start.map(|(_, t)| (t - start).as_secs_f64()),
        total_bytes,
        elapsed_secs: (last_t - start).as_secs_f64(),
        steady_bytes,
        steady_secs,
        stream_errors,
        latency: latency_out,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{BoxFuture, ServerInfo};

    /// 假後端：每條連線每 10ms 送出固定位元組數，可設定總頻寬上限、串流長度與是否失敗。
    struct Fake {
        bytes_per_10ms: u64,
        /// 所有連線共用的頻寬上限（每 10ms 的位元組數），模擬瓶頸
        link_cap_per_10ms: Option<u64>,
        active: AtomicU64,
        stream_len: Option<Duration>,
        fail: bool,
        max_conn: Option<usize>,
        latency_target: Option<(String, u16)>,
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
                struct Active<'a>(&'a AtomicU64);
                impl Drop for Active<'_> {
                    fn drop(&mut self) {
                        self.0.fetch_sub(1, Ordering::Relaxed);
                    }
                }
                self.active.fetch_add(1, Ordering::Relaxed);
                let _guard = Active(&self.active);

                let start = Instant::now();
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let share = self.link_cap_per_10ms.map_or(u64::MAX, |cap| {
                        cap / self.active.load(Ordering::Relaxed).max(1)
                    });
                    counter.fetch_add(self.bytes_per_10ms.min(share), Ordering::Relaxed);
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
        fn latency_target(&self) -> Option<(String, u16)> {
            self.latency_target.clone()
        }
    }

    fn cfg(connections: usize) -> Config {
        Config {
            connections: Connections::Fixed(connections),
            duration: Duration::from_secs(5),
            warmup: Duration::from_secs(1),
            interval: Duration::from_millis(200),
            latency: None,
        }
    }

    fn fake() -> Fake {
        // 125,000 bytes / 10ms = 100 Mbps
        Fake {
            bytes_per_10ms: 125_000,
            link_cap_per_10ms: None,
            active: AtomicU64::new(0),
            stream_len: None,
            fail: false,
            max_conn: None,
            latency_target: None,
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

    // 以下測試用真實的 TCP 連線量延遲，不能用暫停的時鐘
    // （時鐘會在等待 I/O 時自動快轉，讓逾時立刻觸發）。

    fn latency_cfg() -> Config {
        Config {
            connections: Connections::Fixed(2),
            duration: Duration::from_millis(1000),
            warmup: Duration::from_millis(300),
            interval: Duration::from_millis(100),
            latency: Some(latency::Config {
                idle_probes: 3,
                idle_interval: Duration::from_millis(20),
                interval: Duration::from_millis(50),
                timeout: Duration::from_millis(500),
            }),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn measures_idle_and_loaded_latency() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let backend = Fake {
            latency_target: Some(("127.0.0.1".into(), port)),
            ..fake()
        };

        let out = run(Arc::new(backend), &latency_cfg(), |_| {})
            .await
            .unwrap();
        let lat = out.latency.expect("應該有延遲結果");
        assert!(lat.error.is_none(), "{:?}", lat.error);

        let count = |phase| lat.probes.iter().filter(|p| p.phase == phase).count();
        assert_eq!(count(Phase::Idle), 3);
        assert!(count(Phase::Warmup) > 0);
        assert!(count(Phase::Loaded) > 0);
        assert!(lat.probes.iter().all(|p| p.rtt_ms.is_some()));
        assert!(
            lat.probes
                .iter()
                .filter(|p| p.phase == Phase::Idle)
                .all(|p| p.t_secs < 0.0)
        );
        drop(listener);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn latency_failure_does_not_break_download() {
        // 取得一個沒有人在聽的 port
        let port = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let backend = Fake {
            latency_target: Some(("127.0.0.1".into(), port)),
            ..fake()
        };

        let out = run(Arc::new(backend), &latency_cfg(), |_| {})
            .await
            .unwrap();
        assert!(out.total_bytes > 0);
        let lat = out.latency.unwrap();
        assert!(lat.error.is_some());
        // 閒置量測全部失敗後，就不會再做負載下量測
        assert!(lat.probes.iter().all(|p| p.phase == Phase::Idle));
    }

    #[tokio::test(start_paused = true)]
    async fn latency_unsupported_backend() {
        let mut c = cfg(1);
        c.latency = latency_cfg().latency;
        let out = run(Arc::new(fake()), &c, |_| {}).await.unwrap();
        assert!(out.latency.unwrap().error.is_some());
    }

    // ── 動態連線數 ──

    fn auto_cfg(max: usize) -> Config {
        Config {
            connections: Connections::Auto(AutoConfig {
                start: 2,
                max,
                window: Duration::from_secs(1),
                growth_threshold: 0.1,
            }),
            duration: Duration::from_secs(10),
            warmup: Duration::from_secs(1),
            interval: Duration::from_millis(200),
            latency: None,
        }
    }

    fn steps(out: &Outcome) -> Vec<usize> {
        out.connection_steps.iter().map(|s| s.connections).collect()
    }

    /// 每條連線 100 Mbps、瓶頸 450 Mbps：要一路加到跑滿瓶頸才鎖定。
    #[tokio::test(start_paused = true)]
    async fn auto_scales_up_to_bottleneck() {
        let backend = Fake {
            link_cap_per_10ms: Some(562_500), // 450 Mbps
            ..fake()
        };
        let out = run(Arc::new(backend), &auto_cfg(16), |_| {}).await.unwrap();
        // 2→4 (+100%) →8 (+12.5%) →16 (+0%，鎖定)
        assert_eq!(steps(&out), [2, 4, 8, 16]);
        let steady = out.steady_start_secs.unwrap();
        assert!((3.9..=4.3).contains(&steady), "steady = {steady}");
        let avg = out.steady_avg_mbps();
        assert!((avg - 450.0).abs() < 450.0 * 0.05, "avg = {avg}");
        // 調整期間的樣本都算暖機
        assert!(
            out.samples
                .iter()
                .filter(|s| s.t_secs <= 4.0)
                .all(|s| s.warmup)
        );
    }

    /// 單一連線就能跑滿：試一次加倍沒幫助就停。
    #[tokio::test(start_paused = true)]
    async fn auto_stops_when_more_connections_do_not_help() {
        let backend = Fake {
            bytes_per_10ms: 10_000_000,
            link_cap_per_10ms: Some(375_000), // 300 Mbps
            ..fake()
        };
        let out = run(Arc::new(backend), &auto_cfg(16), |_| {}).await.unwrap();
        assert_eq!(steps(&out), [2, 4]);
        assert_eq!(out.connections, 4);
    }

    #[tokio::test(start_paused = true)]
    async fn auto_respects_max_and_backend_limit() {
        let out = run(Arc::new(fake()), &auto_cfg(6), |_| {}).await.unwrap();
        assert_eq!(steps(&out), [2, 4, 6]);

        let backend = Fake {
            max_conn: Some(1),
            ..fake()
        };
        let out = run(Arc::new(backend), &auto_cfg(16), |_| {}).await.unwrap();
        assert_eq!(steps(&out), [1]);
        // 一開始就鎖定，暖機只看最短時間
        assert_eq!(out.steady_start_secs, Some(1.0));
    }

    /// 一直沒有穩定時，過了一半的測試時間就強制鎖定。
    #[tokio::test(start_paused = true)]
    async fn auto_force_locks_at_half_duration() {
        let mut c = auto_cfg(1024);
        c.duration = Duration::from_secs(6);
        let out = run(Arc::new(fake()), &c, |_| {}).await.unwrap();
        assert_eq!(out.steady_start_secs, Some(3.0));
        assert_eq!(steps(&out), [2, 4, 8]);
    }
}
