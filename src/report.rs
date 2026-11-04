//! 輸出測試結果（人類可讀 / JSON）。

use serde::Serialize;

use crate::backend::ServerInfo;
use crate::engine::{Config, ConnectionStep, Connections, Outcome, Sample};
use crate::latency::Probe;
use crate::stats::{LatencySummary, RttStats, Summary};

#[derive(Serialize)]
pub struct Report {
    pub backend: &'static str,
    pub server: ServerInfo,
    /// "auto" 或 "fixed"
    pub connection_mode: &'static str,
    /// 最終連線數
    pub connections: usize,
    pub connection_steps: Vec<ConnectionStep>,
    /// 暖機實際結束的時間點（秒）
    pub warmup_secs: Option<f64>,
    pub elapsed_secs: f64,
    pub total_bytes: u64,
    /// 暖機後沒有任何樣本時為 `None`（例如測試提早結束）
    pub summary: Option<Summary>,
    pub samples: Vec<Sample>,
    pub stream_errors: Vec<String>,
    /// 延遲量測；`None` 表示沒有啟用
    pub latency: Option<LatencyReport>,
}

#[derive(Serialize)]
pub struct LatencyReport {
    /// 量測方式
    pub method: &'static str,
    pub addr: Option<String>,
    pub summary: LatencySummary,
    pub probes: Vec<Probe>,
    pub error: Option<String>,
}

impl Report {
    pub fn new(backend: &'static str, server: ServerInfo, cfg: &Config, out: &Outcome) -> Self {
        Self {
            backend,
            server,
            connection_mode: match cfg.connections {
                Connections::Auto(_) => "auto",
                Connections::Fixed(_) => "fixed",
            },
            connections: out.connections,
            connection_steps: out.connection_steps.clone(),
            warmup_secs: out.steady_start_secs,
            elapsed_secs: out.elapsed_secs,
            total_bytes: out.total_bytes,
            summary: Summary::from_samples(&out.steady_samples(), out.steady_avg_mbps()),
            samples: out.samples.clone(),
            stream_errors: out.stream_errors.clone(),
            latency: out.latency.as_ref().map(|l| LatencyReport {
                method: "tcp_connect",
                addr: l.addr.clone(),
                summary: LatencySummary::from_probes(&l.probes),
                probes: l.probes.clone(),
                error: l.error.clone(),
            }),
        }
    }

    pub fn print_human(&self) {
        match &self.summary {
            Some(s) => {
                println!("下載速度：{:.1} Mbps（中位數）", s.median_mbps);
                println!(
                    "  範圍 P10–P90：{:.1} – {:.1} Mbps，最高 {:.1} Mbps",
                    s.p10_mbps, s.p90_mbps, s.max_mbps
                );
                println!(
                    "  暖機後平均：{:.1} Mbps（{} 個樣本）",
                    s.steady_avg_mbps, s.samples
                );
            }
            None => println!("測試太短，暖機結束後沒有任何樣本，請加長測試時間或縮短暖機。"),
        }

        let conn = if self.connection_mode == "auto" {
            let path: Vec<String> = self
                .connection_steps
                .iter()
                .map(|s| s.connections.to_string())
                .collect();
            format!("{}（自動：{}）", self.connections, path.join(" → "))
        } else {
            self.connections.to_string()
        };
        let warmup = self
            .warmup_secs
            .map_or("整場".into(), |w| format!("{w:.1}s"));
        println!(
            "  連線數 {conn}，總時長 {:.1}s（暖機 {warmup}），共收到 {:.1} MB",
            self.elapsed_secs,
            self.total_bytes as f64 / 1_000_000.0
        );
        println!("  註：數值為應用層 goodput，不含 TCP/IP/TLS 標頭，約比線路速度低 3–6%。");

        if let Some(lat) = &self.latency {
            print_latency(lat);
        }

        if !self.stream_errors.is_empty() {
            println!(
                "警告：有 {} 條串流發生錯誤，結果可能偏低。第一個錯誤：{}",
                self.stream_errors.len(),
                self.stream_errors[0]
            );
        }
    }
}

fn print_latency(lat: &LatencyReport) {
    println!();
    match &lat.addr {
        Some(addr) => println!("延遲（TCP 交握，{addr}）："),
        None => println!("延遲："),
    }

    let line = |label: &str, s: &Option<RttStats>| match s {
        Some(s) => println!(
            "  {label}  {:6.1} ms（最低 {:.1}，P90 {:.1}，抖動 {:.1} ms）{}",
            s.median_ms,
            s.min_ms,
            s.p90_ms,
            s.jitter_ms,
            if s.lost > 0 {
                format!("，遺失 {} 次", s.lost)
            } else {
                String::new()
            }
        ),
        None => println!("  {label}  無資料"),
    };
    line("閒置  ", &lat.summary.idle);
    line("下載中", &lat.summary.loaded);

    if let (Some(inc), Some(grade)) = (lat.summary.increase_ms, lat.summary.bufferbloat_grade) {
        println!("  增加   +{inc:.1} ms → Bufferbloat 等級 {grade}");
    }
    if let Some(err) = &lat.error {
        println!("  無法量測延遲：{err}");
    }
}
