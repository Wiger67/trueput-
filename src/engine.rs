//! 測速引擎：開平行連線、定時取樣、區分暖機期、收集結果。下載與上傳共用同一套流程。
//!
//! 測量方法：
//! - （可選）測試前先量閒置延遲（[`measure_idle`]），同一台伺服器的下載與上傳共用。
//! - 開平行串流（受後端的 `max_connections` 限制），全部累加到同一個計數器。
//!   連線數可以固定，或由 [`crate::autoscale`] 動態調整；評估窗格依閒置延遲拉長。
//! - 主要結果是「暖機後的總位元組數 ÷ 暖機後經過的時間」（見 [`crate::stats`]）。
//! - 每隔 `interval` 取一個樣本：最近 `sample_window`（1 秒）的位元組數 ÷ 實際經過時間。
//!   用實際時間而不是假設的 interval，排程延遲才不會影響結果；用 1 秒而不是 200ms，
//!   是因為 socket 緩衝區會讓資料一陣一陣地送（上傳特別明顯），太短的窗格會忽高忽低。
//!   正式樣本的窗格不會跨進暖機期。
//! - 傳輸期間另外有一個 task 持續量延遲（負載下延遲），細節見 [`crate::latency`]。
//! - 暖機（TCP 慢啟動、連線數調整中）的樣本不列入統計。暖機要同時滿足兩個條件才結束：
//!   經過至少 `warmup` 秒，而且連線數已經鎖定。
//! - 結束條件：時間到、所有串流都結束，或累積平均已經穩定（提前結束，見 [`Stability`]）。
//! - 後端若有伺服器端量測（ndt7 的 TCPInfo），會另外算出伺服器端的速度與 RTT 供對照。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};

use crate::autoscale::{AutoConfig, Scaler, window_for_rtt};
use crate::backend::{Backend, Direction, ServerMeasurement};
use crate::latency::{self, Phase, Probe, Prober, to_ms};
use crate::stats::{Stability, mbps, percentile};

#[derive(Debug, Clone)]
pub enum Connections {
    /// 固定連線數
    Fixed(usize),
    /// 動態調整
    Auto(AutoConfig),
}

/// 上傳時自動調整連線數的最短評估窗格
const UPLOAD_MIN_SCALE_WINDOW: Duration = Duration::from_secs(2);

/// 上傳提前結束的最低容許變化幅度。
///
/// 上傳計的是「交給 socket 的位元組」，會隨緩衝區一批一批進來，累積平均會呈鋸齒狀，
/// 振幅約為「一批的大小 ÷ 已傳總量」，要比下載多傳一段時間才會縮到 2% 以下。
const UPLOAD_MIN_TOLERANCE: f64 = 0.03;

/// 提前結束的條件，見 [`Stability`]。
#[derive(Debug, Clone)]
pub struct EarlyStop {
    /// 正式量測至少要進行多久
    pub min_steady: Duration,
    /// 觀察累積平均變化的時間範圍
    pub window: Duration,
    /// 變化幅度小於這個比例就算穩定，0.02 = 2%（上傳至少 3%）
    pub tolerance: f64,
}

impl Default for EarlyStop {
    fn default() -> Self {
        Self {
            min_steady: Duration::from_secs(4),
            window: Duration::from_secs(2),
            tolerance: 0.02,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    /// 平行連線數（實際值會受後端限制）
    pub connections: Connections,
    /// 每個方向的測試長度上限（含暖機）
    pub duration: Duration,
    /// 最短暖機長度；自動模式下，連線數鎖定前也算暖機
    pub warmup: Duration,
    /// 取樣間隔
    pub interval: Duration,
    /// 每個樣本涵蓋的時間（最近多久的平均速度）
    pub sample_window: Duration,
    /// 延遲量測設定；`None` 表示不量
    pub latency: Option<latency::Config>,
    /// 提前結束；`None` 表示一定測滿 `duration`
    pub early_stop: Option<EarlyStop>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Sample {
    /// 從測試開始算起的秒數（這個樣本的結束時間點）
    pub t_secs: f64,
    /// 最近 `sample_window` 的平均速度
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

/// 閒置延遲量測結果
#[derive(Debug, Default)]
pub struct Idle {
    /// 實際量測的 IP 與 port
    pub addr: Option<String>,
    /// 閒置量測（`t_secs` 從閒置量測開始算）
    pub probes: Vec<Probe>,
    /// 無法量測延遲的原因（不影響測速）
    pub error: Option<String>,
    /// 閒置量測成功時保留，供傳輸期間繼續使用
    prober: Option<Arc<Prober>>,
}

impl Idle {
    pub fn median_rtt(&self) -> Option<Duration> {
        let mut ok: Vec<f64> = self.probes.iter().filter_map(|p| p.rtt_ms).collect();
        if ok.is_empty() {
            return None;
        }
        ok.sort_by(f64::total_cmp);
        Some(Duration::from_secs_f64(percentile(&ok, 50.0) / 1000.0))
    }
}

/// 伺服器端量測整理後的結果
#[derive(Debug, Clone, Serialize)]
pub struct ServerSide {
    /// 伺服器端的 RTT（`phase` 依取得時間標記暖機或正式）
    pub rtt_probes: Vec<Probe>,
    /// 正式量測期間，伺服器端確認的位元組數換算的速度
    pub steady_mbps: Option<f64>,
}

#[derive(Debug)]
pub struct Outcome {
    pub direction: Direction,
    pub samples: Vec<Sample>,
    /// 最終使用的連線數
    pub connections: usize,
    /// 後端（協定或伺服器政策）允許的最大連線數
    pub connection_limit: Option<usize>,
    /// 連線數的變化（第一筆為初始值）
    pub connection_steps: Vec<ConnectionStep>,
    /// 自動模式的評估窗格（秒）
    pub scale_window_secs: Option<f64>,
    /// 暖機實際結束的時間點；`None` 表示整場測試都在暖機
    pub steady_start_secs: Option<f64>,
    pub total_bytes: u64,
    pub elapsed_secs: f64,
    /// 暖機後傳輸的位元組數與經過時間
    pub steady_bytes: u64,
    pub steady_secs: f64,
    /// 是否因為結果已穩定而提前結束
    pub early_stopped: bool,
    /// 個別串流的錯誤。只要還有資料在傳，測試就會繼續，但結果可能偏低。
    pub stream_errors: Vec<String>,
    /// 傳輸期間的延遲量測；`None` 表示沒有量
    pub loaded_probes: Option<Vec<Probe>>,
    /// 伺服器端量測；後端不支援時為 `None`
    pub server_side: Option<ServerSide>,
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

/// 量閒置延遲。
pub async fn measure_idle(backend: &dyn Backend, cfg: &latency::Config) -> Idle {
    let mut out = Idle::default();
    let Some((host, port)) = backend.latency_target() else {
        out.error = Some("後端沒有提供延遲量測目標".into());
        return out;
    };
    let prober = match Prober::resolve(&host, port, cfg.timeout).await {
        Ok(p) => p,
        Err(e) => {
            out.error = Some(format!("{e:#}"));
            return out;
        }
    };
    out.addr = Some(prober.addr().to_string());

    let start = Instant::now();
    for i in 0..cfg.idle_probes {
        if i > 0 {
            tokio::time::sleep(cfg.idle_interval).await;
        }
        let at = Instant::now();
        let rtt = prober.probe().await;
        out.probes.push(Probe {
            t_secs: (at - start).as_secs_f64(),
            rtt_ms: rtt.map(to_ms),
            phase: Phase::Idle,
        });
    }

    if out.probes.iter().all(|p| p.rtt_ms.is_none()) {
        out.error = Some(format!(
            "閒置延遲量測全部失敗（{} 可能擋掉了額外的 TCP 連線）",
            prober.addr()
        ));
    } else {
        out.prober = Some(Arc::new(prober));
    }
    out
}

/// 把伺服器端量測整理成 RTT 序列與正式量測期間的速度。
fn summarize_server(
    measurements: Vec<ServerMeasurement>,
    start: Instant,
    steady_start: Option<Instant>,
) -> Option<ServerSide> {
    if measurements.is_empty() {
        return None;
    }
    let is_steady = |at: Instant| steady_start.is_some_and(|s| at >= s);

    let rtt_probes = measurements
        .iter()
        .filter(|m| m.rtt_ms.is_some())
        .map(|m| Probe {
            t_secs: m.at.saturating_duration_since(start).as_secs_f64(),
            rtt_ms: m.rtt_ms,
            phase: if is_steady(m.at) {
                Phase::Loaded
            } else {
                Phase::Warmup
            },
        })
        .collect();

    // 用正式量測期間第一筆與最後一筆的差值；時間優先用伺服器端的時鐘
    let steady: Vec<&ServerMeasurement> = measurements
        .iter()
        .filter(|m| is_steady(m.at) && m.bytes.is_some())
        .collect();
    let steady_mbps = match (steady.first(), steady.last()) {
        (Some(a), Some(b)) if steady.len() >= 2 => {
            let bytes = b.bytes?.saturating_sub(a.bytes?);
            let secs = match (a.elapsed_us, b.elapsed_us) {
                (Some(ea), Some(eb)) => eb.saturating_sub(ea) as f64 / 1e6,
                _ => (b.at - a.at).as_secs_f64(),
            };
            (secs > 0.0).then(|| mbps(bytes, secs))
        }
        _ => None,
    };

    Some(ServerSide {
        rtt_probes,
        steady_mbps,
    })
}

pub async fn run(
    backend: Arc<dyn Backend>,
    dir: Direction,
    cfg: &Config,
    idle: Option<&Idle>,
    mut on_sample: impl FnMut(&Sample),
) -> anyhow::Result<Outcome> {
    let limit = backend.max_connections().unwrap_or(usize::MAX).max(1);
    let (mut connections, mut scaler, window) = match &cfg.connections {
        Connections::Fixed(n) => ((*n).clamp(1, limit), Scaler::fixed(), None),
        Connections::Auto(a) => {
            let max = a.max.clamp(1, limit);
            let start = a.start.clamp(1, max);
            // 上傳計的是「交給 socket 的位元組」，會隨緩衝區一陣一陣地進來，
            // 1 秒的窗格容易誤判「加連線沒幫助」，所以至少用 2 秒。
            let base = match dir {
                Direction::Download => a.window,
                Direction::Upload => a.window.max(UPLOAD_MIN_SCALE_WINDOW),
            };
            let window = window_for_rtt(base, idle.and_then(Idle::median_rtt));
            (
                start,
                Scaler::auto(start, max, a.growth_threshold),
                Some(window),
            )
        }
    };
    // 丟掉之前殘留的伺服器端量測
    backend.take_server_measurements();

    let counter = Arc::new(AtomicU64::new(0));
    let mut workers = JoinSet::new();
    let start = Instant::now();

    let spawn_worker = |workers: &mut JoinSet<anyhow::Result<()>>| {
        let (backend, counter) = (backend.clone(), counter.clone());
        workers.spawn(async move {
            loop {
                backend.transfer(dir, counter.clone()).await?;
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

    // ── 負載下延遲：與傳輸同時進行 ─────────────────────────
    // 一次只有一個量測在進行；量測慢的時候不會越積越多。
    let loaded_probes: Arc<Mutex<Vec<Probe>>> = Arc::default();
    let probe_task = match (&cfg.latency, idle.and_then(|i| i.prober.clone())) {
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
    // 落後時直接跳過錯過的 tick：不會連續補 tick（產生時間極短、數值很亂的樣本），
    // 也不會像 Delay 那樣每次延遲都往後累積，讓取樣時間點越飄越遠。
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tick.tick().await; // 第一次 tick 會立刻完成
    // 測試剛好在上限時間結束：最後一次取樣對齊 deadline，不會超出
    let deadline = start + cfg.duration;

    let mut samples = Vec::new();
    let mut stream_errors = Vec::new();
    // 最後一次取樣的累計位元組數與時間（迴圈至少跑一次，一定會被賦值）
    let (mut last_bytes, mut last_t): (u64, Instant);
    // 每次取樣時的 (時間, 累計位元組數)，用來算最近 sample_window 的速度
    let mut history = std::collections::VecDeque::from([(start, 0u64)]);
    let mut steady_start: Option<(u64, Instant)> = None;
    let (mut window_bytes, mut window_t) = (0u64, start);
    let mut stability = cfg.early_stop.as_ref().map(|e| {
        let tolerance = match dir {
            Direction::Download => e.tolerance,
            Direction::Upload => e.tolerance.max(UPLOAD_MIN_TOLERANCE),
        };
        Stability::new(
            e.min_steady.as_secs_f64(),
            e.window.as_secs_f64(),
            tolerance,
        )
    });
    let mut early_stopped = false;

    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = tokio::time::sleep_until(deadline) => {}
        }
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
        if let Some(window) = window
            && !scaler.locked()
            && now - window_t >= window
        {
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

        // 樣本起點：最近 sample_window 內最早的一次取樣；正式樣本不跨進暖機期
        let mut boundary = now.checked_sub(cfg.sample_window).unwrap_or(start);
        if let Some((_, t0)) = steady_start {
            boundary = boundary.max(t0);
        }
        while history.len() > 1 && history[0].0 < boundary {
            history.pop_front();
        }
        let (ref_t, ref_bytes) = history[0];

        let sample = Sample {
            t_secs: elapsed.as_secs_f64(),
            connections: sample_connections,
            mbps: mbps(bytes - ref_bytes, (now - ref_t).as_secs_f64()),
            // 鎖定是在這次取樣時才決定的，所以這一段仍然算暖機
            warmup: steady_start.is_none(),
            latest_rtt_ms: loaded_probes.lock().unwrap().last().and_then(|p| p.rtt_ms),
        };
        on_sample(&sample);
        samples.push(sample);
        (last_bytes, last_t) = (bytes, now);
        history.push_back((now, bytes));

        // ── 提前結束：累積平均已經穩定 ──
        if let (Some((b0, t0)), Some(stab)) = (steady_start, stability.as_mut()) {
            let steady_secs = (now - t0).as_secs_f64();
            if stab.observe(steady_secs, mbps(bytes - b0, steady_secs)) {
                early_stopped = true;
            }
        }

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

        if early_stopped || workers.is_empty() || elapsed >= cfg.duration {
            break;
        }
    }
    // 先停延遲量測，避免傳輸結束後量到的「閒置」數值混進負載下延遲
    if let Some(task) = probe_task {
        task.abort();
    }
    workers.abort_all();
    // 等串流真的停下來，下一個測試（例如上傳）才不會被殘留的流量干擾
    while workers.join_next().await.is_some() {}

    let total_bytes = last_bytes;
    if total_bytes == 0 {
        anyhow::bail!(
            "沒有傳輸任何資料：{}",
            stream_errors.first().map_or("未知原因", String::as_str)
        );
    }

    let (steady_bytes, steady_secs) = match steady_start {
        Some((b0, t0)) => (last_bytes - b0, (last_t - t0).as_secs_f64()),
        None => (0, 0.0),
    };

    let loaded_probes = cfg.latency.as_ref().map(|_| {
        let end = (last_t - start).as_secs_f64();
        let mut probes = std::mem::take(&mut *loaded_probes.lock().unwrap());
        // 最後一次取樣之後才開始的量測不算
        probes.retain(|p| p.t_secs <= end);
        probes
    });

    let server_side = summarize_server(
        backend.take_server_measurements(),
        start,
        steady_start.map(|(_, t)| t),
    );

    Ok(Outcome {
        direction: dir,
        samples,
        connections,
        connection_limit: backend.max_connections(),
        connection_steps,
        scale_window_secs: window.map(|w| w.as_secs_f64()),
        steady_start_secs: steady_start.map(|(_, t)| (t - start).as_secs_f64()),
        total_bytes,
        elapsed_secs: (last_t - start).as_secs_f64(),
        steady_bytes,
        steady_secs,
        early_stopped,
        stream_errors,
        loaded_probes,
        server_side,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{BoxFuture, ServerInfo};

    /// 假後端：每條連線每 10ms 傳固定位元組數，可設定總頻寬上限、串流長度、
    /// 是否失敗，以及是否模擬伺服器端量測。下載與上傳行為相同。
    struct Fake {
        bytes_per_10ms: u64,
        /// 所有連線共用的頻寬上限（每 10ms 的位元組數），模擬瓶頸
        link_cap_per_10ms: Option<u64>,
        active: AtomicU64,
        stream_len: Option<Duration>,
        fail: bool,
        max_conn: Option<usize>,
        latency_target: Option<(String, u16)>,
        /// 每 250ms 產生一筆伺服器端量測（RTT 固定 20ms）
        server_reports: bool,
        measurements: Mutex<Vec<ServerMeasurement>>,
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
        fn supports(&self, _dir: Direction) -> bool {
            true
        }
        fn transfer(
            &self,
            _dir: Direction,
            counter: Arc<AtomicU64>,
        ) -> BoxFuture<'_, anyhow::Result<()>> {
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
                let mut own_bytes = 0u64;
                for i in 1u64.. {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let share = self.link_cap_per_10ms.map_or(u64::MAX, |cap| {
                        cap / self.active.load(Ordering::Relaxed).max(1)
                    });
                    let n = self.bytes_per_10ms.min(share);
                    counter.fetch_add(n, Ordering::Relaxed);
                    own_bytes += n;
                    if self.server_reports && i % 25 == 0 {
                        self.measurements.lock().unwrap().push(ServerMeasurement {
                            at: Instant::now(),
                            elapsed_us: Some(start.elapsed().as_micros() as u64),
                            bytes: Some(own_bytes),
                            rtt_ms: Some(20.0),
                        });
                    }
                    if self.stream_len.is_some_and(|len| start.elapsed() >= len) {
                        return Ok(());
                    }
                }
                unreachable!()
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
        fn take_server_measurements(&self) -> Vec<ServerMeasurement> {
            std::mem::take(&mut *self.measurements.lock().unwrap())
        }
    }

    fn cfg(connections: usize) -> Config {
        Config {
            connections: Connections::Fixed(connections),
            duration: Duration::from_secs(5),
            warmup: Duration::from_secs(1),
            interval: Duration::from_millis(200),
            sample_window: Duration::from_secs(1),
            latency: None,
            early_stop: None,
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
            server_reports: false,
            measurements: Mutex::new(Vec::new()),
        }
    }

    async fn run_dl(backend: Fake, cfg: &Config) -> anyhow::Result<Outcome> {
        run(Arc::new(backend), Direction::Download, cfg, None, |_| {}).await
    }

    #[tokio::test(start_paused = true)]
    async fn measures_aggregate_throughput() {
        let out = run_dl(fake(), &cfg(4)).await.unwrap();
        assert_eq!(out.connections, 4);
        let avg = out.steady_avg_mbps();
        assert!((avg - 400.0).abs() < 400.0 * 0.05, "avg = {avg}");
        assert!(out.samples.iter().any(|s| s.warmup));
        assert!(out.samples.iter().any(|s| !s.warmup));
        assert!(!out.early_stopped);
        assert!(out.server_side.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn upload_uses_same_pipeline() {
        let out = run(Arc::new(fake()), Direction::Upload, &cfg(2), None, |_| {})
            .await
            .unwrap();
        assert_eq!(out.direction, Direction::Upload);
        assert!((out.steady_avg_mbps() - 200.0).abs() < 10.0);
    }

    #[tokio::test(start_paused = true)]
    async fn respects_backend_connection_limit() {
        let backend = Fake {
            max_conn: Some(1),
            ..fake()
        };
        let out = run_dl(backend, &cfg(8)).await.unwrap();
        assert_eq!(out.connections, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn stops_when_streams_finish() {
        let backend = Fake {
            stream_len: Some(Duration::from_secs(2)),
            ..fake()
        };
        let out = run_dl(backend, &cfg(1)).await.unwrap();
        assert!(out.elapsed_secs < 3.0, "elapsed = {}", out.elapsed_secs);
    }

    #[tokio::test(start_paused = true)]
    async fn errors_when_no_data() {
        let backend = Fake {
            fail: true,
            ..fake()
        };
        let err = run_dl(backend, &cfg(2)).await.unwrap_err();
        assert!(err.to_string().contains("boom"));
    }

    // ── 提前結束 ──

    #[tokio::test(start_paused = true)]
    async fn stops_early_when_stable() {
        let mut c = cfg(2);
        c.duration = Duration::from_secs(15);
        c.early_stop = Some(EarlyStop::default()); // 至少 4 秒、2 秒內變化 < 2%
        let out = run_dl(fake(), &c).await.unwrap();
        assert!(out.early_stopped);
        // 暖機 1 秒 + 正式 4 秒
        assert!(
            (4.9..=5.5).contains(&out.elapsed_secs),
            "elapsed = {}",
            out.elapsed_secs
        );
        assert!((out.steady_avg_mbps() - 200.0).abs() < 10.0);
    }

    // ── 伺服器端量測 ──

    #[tokio::test(start_paused = true)]
    async fn summarizes_server_measurements() {
        let backend = Fake {
            server_reports: true,
            ..fake()
        };
        let out = run_dl(backend, &cfg(1)).await.unwrap();
        let server = out.server_side.expect("應該有伺服器端量測");
        let mbps = server.steady_mbps.unwrap();
        assert!((mbps - 100.0).abs() < 5.0, "server mbps = {mbps}");
        assert!(server.rtt_probes.iter().any(|p| p.phase == Phase::Warmup));
        assert!(server.rtt_probes.iter().any(|p| p.phase == Phase::Loaded));
        assert!(server.rtt_probes.iter().all(|p| p.rtt_ms == Some(20.0)));
    }

    // ── 延遲 ──
    // 以下測試用真實的 TCP 連線量延遲，不能用暫停的時鐘
    // （時鐘會在等待 I/O 時自動快轉，讓逾時立刻觸發）。

    fn latency_cfg() -> Config {
        Config {
            connections: Connections::Fixed(2),
            duration: Duration::from_millis(1000),
            warmup: Duration::from_millis(300),
            interval: Duration::from_millis(100),
            sample_window: Duration::from_millis(300),
            latency: Some(latency::Config {
                idle_probes: 3,
                idle_interval: Duration::from_millis(20),
                interval: Duration::from_millis(50),
                timeout: Duration::from_millis(500),
            }),
            early_stop: None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn measures_idle_and_loaded_latency_for_both_directions() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let backend: Arc<dyn Backend> = Arc::new(Fake {
            latency_target: Some(("127.0.0.1".into(), port)),
            ..fake()
        });
        let c = latency_cfg();

        let idle = measure_idle(backend.as_ref(), c.latency.as_ref().unwrap()).await;
        assert!(idle.error.is_none(), "{:?}", idle.error);
        assert_eq!(idle.probes.len(), 3);
        assert!(idle.median_rtt().is_some());

        // 同一份閒置結果供下載與上傳共用
        for dir in [Direction::Download, Direction::Upload] {
            let out = run(backend.clone(), dir, &c, Some(&idle), |_| {})
                .await
                .unwrap();
            let probes = out.loaded_probes.expect("應該有負載下延遲");
            assert!(probes.iter().any(|p| p.phase == Phase::Warmup));
            assert!(probes.iter().any(|p| p.phase == Phase::Loaded));
            assert!(probes.iter().all(|p| p.rtt_ms.is_some()));
        }
        drop(listener);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn latency_failure_does_not_break_transfer() {
        // 取得一個沒有人在聽的 port
        let port = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let backend: Arc<dyn Backend> = Arc::new(Fake {
            latency_target: Some(("127.0.0.1".into(), port)),
            ..fake()
        });
        let c = latency_cfg();

        let idle = measure_idle(backend.as_ref(), c.latency.as_ref().unwrap()).await;
        assert!(idle.error.is_some());
        let out = run(backend, Direction::Download, &c, Some(&idle), |_| {})
            .await
            .unwrap();
        assert!(out.total_bytes > 0);
        // 閒置量測全部失敗後，就不會再做負載下量測
        assert!(out.loaded_probes.unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn latency_unsupported_backend() {
        let idle = measure_idle(&fake(), latency_cfg().latency.as_ref().unwrap()).await;
        assert!(idle.error.is_some());
        assert!(idle.probes.is_empty());
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
            sample_window: Duration::from_secs(1),
            latency: None,
            early_stop: None,
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
        let out = run_dl(backend, &auto_cfg(16)).await.unwrap();
        // 2→4 (+100%) →8 (+12.5%) →16 (+0%，鎖定)
        assert_eq!(steps(&out), [2, 4, 8, 16]);
        assert_eq!(out.scale_window_secs, Some(1.0));
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
        let out = run_dl(backend, &auto_cfg(16)).await.unwrap();
        assert_eq!(steps(&out), [2, 4]);
        assert_eq!(out.connections, 4);
    }

    #[tokio::test(start_paused = true)]
    async fn auto_respects_max_and_backend_limit() {
        let out = run_dl(fake(), &auto_cfg(6)).await.unwrap();
        assert_eq!(steps(&out), [2, 4, 6]);

        let backend = Fake {
            max_conn: Some(1),
            ..fake()
        };
        let out = run_dl(backend, &auto_cfg(16)).await.unwrap();
        assert_eq!(steps(&out), [1]);
        // 一開始就鎖定，暖機只看最短時間
        assert_eq!(out.steady_start_secs, Some(1.0));
    }

    /// 一直沒有穩定時，過了一半的測試時間就強制鎖定。
    #[tokio::test(start_paused = true)]
    async fn auto_force_locks_at_half_duration() {
        let mut c = auto_cfg(1024);
        c.duration = Duration::from_secs(6);
        let out = run_dl(fake(), &c).await.unwrap();
        assert_eq!(out.steady_start_secs, Some(3.0));
        assert_eq!(steps(&out), [2, 4, 8]);
    }

    #[test]
    fn idle_median_rtt() {
        let probe = |rtt| Probe {
            t_secs: 0.0,
            rtt_ms: rtt,
            phase: Phase::Idle,
        };
        let idle = Idle {
            probes: vec![
                probe(Some(10.0)),
                probe(None),
                probe(Some(30.0)),
                probe(Some(20.0)),
            ],
            ..Default::default()
        };
        assert_eq!(idle.median_rtt(), Some(Duration::from_millis(20)));
        assert_eq!(Idle::default().median_rtt(), None);
    }

    /// 模擬 socket 緩衝區造成的「一陣一陣」：每隔 `period` 一次送出 `bytes`。
    struct Bursty {
        bytes: u64,
        period: Duration,
    }

    impl Backend for Bursty {
        fn name(&self) -> &'static str {
            "bursty"
        }
        fn prepare(&mut self) -> BoxFuture<'_, anyhow::Result<ServerInfo>> {
            unreachable!()
        }
        fn supports(&self, _dir: Direction) -> bool {
            true
        }
        fn transfer(
            &self,
            _dir: Direction,
            counter: Arc<AtomicU64>,
        ) -> BoxFuture<'_, anyhow::Result<()>> {
            Box::pin(async move {
                loop {
                    tokio::time::sleep(self.period).await;
                    counter.fetch_add(self.bytes, Ordering::Relaxed);
                }
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn bursty_transfer_is_measured_correctly() {
        let mut c = cfg(1);
        c.duration = Duration::from_secs(10);
        // 每 500ms 1 MB，平均 16 Mbps
        let bursty = Bursty {
            bytes: 1_000_000,
            period: Duration::from_millis(500),
        };
        let out = run(Arc::new(bursty), Direction::Upload, &c, None, |_| {})
            .await
            .unwrap();
        // 主要結果：總量 ÷ 時間
        let avg = out.steady_avg_mbps();
        assert!((avg - 16.0).abs() < 16.0 * 0.06, "avg = {avg}");
        // 1 秒窗格的樣本中位數也要接近真實速度（200ms 窗格的中位數會是 0）
        let mut s = out.steady_samples();
        s.sort_by(f64::total_cmp);
        let median = percentile(&s, 50.0);
        assert!((median - 16.0).abs() < 16.0 * 0.15, "median = {median}");
    }

    /// 正式樣本的窗格不能跨進暖機期。
    #[tokio::test(start_paused = true)]
    async fn steady_samples_do_not_include_warmup_data() {
        let out = run_dl(fake(), &cfg(1)).await.unwrap();
        let first_steady = out.samples.iter().find(|s| !s.warmup).unwrap();
        // 只涵蓋暖機結束後的 200ms，速度仍是 100 Mbps
        assert!(
            (first_steady.mbps - 100.0).abs() < 5.0,
            "{}",
            first_steady.mbps
        );
    }

    #[tokio::test(start_paused = true)]
    async fn upload_uses_longer_scale_window() {
        let out = run(
            Arc::new(fake()),
            Direction::Upload,
            &auto_cfg(16),
            None,
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(out.scale_window_secs, Some(2.0));
    }

    /// 取樣間隔無法整除測試長度時，也要剛好在上限結束，不能超出。
    #[tokio::test(start_paused = true)]
    async fn ends_exactly_at_duration() {
        let mut c = cfg(1);
        c.interval = Duration::from_millis(300);
        c.duration = Duration::from_secs(1);
        c.warmup = Duration::from_millis(300);
        let out = run_dl(fake(), &c).await.unwrap();
        assert_eq!(out.elapsed_secs, 1.0);
        assert_eq!(out.samples.last().unwrap().t_secs, 1.0);
    }

    /// 上傳的鋸齒狀累積平均：容許 3% 時比下載（2%）更早判定穩定。
    #[tokio::test(start_paused = true)]
    async fn upload_early_stop_tolerates_buffer_sawtooth() {
        let mut c = cfg(1);
        c.duration = Duration::from_secs(15);
        c.early_stop = Some(EarlyStop::default());
        // 每 125ms 250 KB（16 Mbps）：鋸齒振幅在 2%～3% 之間的時間會持續好幾秒
        let bursty = || {
            Arc::new(Bursty {
                bytes: 250_000,
                period: Duration::from_millis(125),
            })
        };
        let dl = run(bursty(), Direction::Download, &c, None, |_| {})
            .await
            .unwrap();
        let ul = run(bursty(), Direction::Upload, &c, None, |_| {})
            .await
            .unwrap();
        assert!(dl.early_stopped && ul.early_stopped);
        assert!(
            ul.elapsed_secs < dl.elapsed_secs,
            "upload {} vs download {}",
            ul.elapsed_secs,
            dl.elapsed_secs
        );
        // 提前結束也不能犧牲準確度
        assert!((ul.steady_avg_mbps() - 16.0).abs() < 16.0 * 0.03);
    }

    #[tokio::test(start_paused = true)]
    async fn reports_backend_connection_limit() {
        let backend = Fake {
            max_conn: Some(1),
            ..fake()
        };
        let out = run_dl(backend, &auto_cfg(16)).await.unwrap();
        assert_eq!(out.connection_limit, Some(1));
        assert_eq!(
            run_dl(fake(), &cfg(2)).await.unwrap().connection_limit,
            None
        );
    }
}
