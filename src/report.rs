//! 整理與輸出測試結果（人類可讀 / JSON）。

use serde::Serialize;

use crate::backend::{Direction, ServerInfo};
use crate::engine::{Config, ConnectionStep, Connections, Idle, Outcome, Sample};
use crate::latency::{Phase, Probe};
use crate::stats::{LatencySummary, RttStats, Summary};

/// 不同伺服器結果差距超過這個比例時，提醒使用者瓶頸可能在路徑上
pub const SPREAD_WARN_PCT: f64 = 10.0;

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub results: Vec<BackendReport>,
    /// 測了兩台以上伺服器時的比較
    pub comparison: Option<Comparison>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackendReport {
    pub backend: &'static str,
    pub server: Option<ServerInfo>,
    /// 延遲量測方式與目標
    pub latency_method: &'static str,
    pub latency_addr: Option<String>,
    pub idle_latency: Option<RttStats>,
    pub idle_probes: Vec<Probe>,
    pub latency_error: Option<String>,
    pub download: Option<DirectionReport>,
    pub upload: Option<DirectionReport>,
    /// 準備失敗、某個方向失敗等
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DirectionReport {
    /// 後端名稱（只用於顯示）
    #[serde(skip)]
    backend: &'static str,
    pub direction: Direction,
    /// "auto" 或 "fixed"
    pub connection_mode: &'static str,
    /// 最終連線數
    pub connections: usize,
    /// 後端（協定或伺服器政策）允許的最大連線數
    pub connection_limit: Option<usize>,
    pub connection_steps: Vec<ConnectionStep>,
    /// 自動模式的評估窗格（秒）
    pub scale_window_secs: Option<f64>,
    /// 暖機實際結束的時間點（秒）
    pub warmup_secs: Option<f64>,
    pub elapsed_secs: f64,
    pub early_stopped: bool,
    pub total_bytes: u64,
    /// 暖機後沒有任何樣本時為 `None`
    pub summary: Option<Summary>,
    /// 閒置 vs 這個方向傳輸中的延遲
    pub latency: Option<LatencySummary>,
    pub server_side: Option<ServerSideReport>,
    pub samples: Vec<Sample>,
    pub loaded_probes: Vec<Probe>,
    pub stream_errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerSideReport {
    /// 伺服器端確認的位元組數換算的速度
    pub steady_mbps: Option<f64>,
    /// 客戶端結果相對伺服器端的差距（%）
    pub client_diff_pct: Option<f64>,
    /// 伺服器核心量到的 RTT（正式量測期間）
    pub loaded_rtt: Option<RttStats>,
    pub rtt_probes: Vec<Probe>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Comparison {
    /// (最大 − 最小) / 最大，以主要結果比較
    pub download_spread_pct: Option<f64>,
    pub upload_spread_pct: Option<f64>,
}

impl BackendReport {
    pub fn new(backend: &'static str) -> Self {
        Self {
            backend,
            server: None,
            latency_method: "tcp_connect",
            latency_addr: None,
            idle_latency: None,
            idle_probes: Vec::new(),
            latency_error: None,
            download: None,
            upload: None,
            errors: Vec::new(),
        }
    }

    pub fn set_idle(&mut self, idle: &Idle) {
        self.latency_addr = idle.addr.clone();
        self.idle_probes = idle.probes.clone();
        self.latency_error = idle.error.clone();
        let rtts: Vec<Option<f64>> = idle.probes.iter().map(|p| p.rtt_ms).collect();
        self.idle_latency = RttStats::from_rtts(&rtts);
    }

    /// 加入一個方向的結果，回傳它的報告（方便立刻印出）。
    pub fn set_direction(&mut self, cfg: &Config, out: &Outcome) -> &DirectionReport {
        let report = DirectionReport::new(self.backend, cfg, out, &self.idle_probes);
        let slot = match out.direction {
            Direction::Download => &mut self.download,
            Direction::Upload => &mut self.upload,
        };
        slot.insert(report)
    }

    /// 某個方向的結果（還沒測或失敗時為 `None`）
    pub fn direction(&self, dir: Direction) -> Option<&DirectionReport> {
        match dir {
            Direction::Download => self.download.as_ref(),
            Direction::Upload => self.upload.as_ref(),
        }
    }

    /// 某個方向的主要結果（Mbps）
    pub fn mbps(&self, dir: Direction) -> Option<f64> {
        self.direction(dir)?.summary.as_ref().map(|s| s.mbps)
    }

    /// 伺服器標題，例如「speed.cloudflare.com — TPE, Taipei, TW」
    pub fn server_text(&self) -> String {
        let server = self.server.as_ref();
        format!(
            "{}{}",
            server.map_or("（未連線）", |s| s.name.as_str()),
            server
                .and_then(|s| s.location.as_deref())
                .map(|l| format!(" — {l}"))
                .unwrap_or_default()
        )
    }

    /// 閒置延遲的說明；沒有量時為 `None`
    pub fn idle_text(&self) -> Option<String> {
        match (&self.idle_latency, &self.latency_error) {
            (Some(s), _) => Some(format!(
                "閒置延遲  {:.1} ms（最低 {:.1}，抖動 {:.1} ms）  [TCP 交握，{}]",
                s.median_ms,
                s.min_ms,
                s.jitter_ms,
                self.latency_addr.as_deref().unwrap_or("?")
            )),
            (None, Some(err)) => Some(format!("閒置延遲  無法量測：{err}")),
            (None, None) => None,
        }
    }

    pub fn print_header(&self) {
        println!();
        println!("━━ {}：{} ━━", self.backend, self.server_text());
    }

    pub fn print_idle(&self) {
        if let Some(t) = self.idle_text() {
            println!("{t}");
        }
    }
}

impl DirectionReport {
    fn new(backend: &'static str, cfg: &Config, out: &Outcome, idle_probes: &[Probe]) -> Self {
        let summary = Summary::from_samples(&out.steady_samples(), out.steady_avg_mbps());
        let loaded_probes = out.loaded_probes.clone().unwrap_or_default();
        let latency = out.loaded_probes.as_ref().map(|loaded| {
            let all: Vec<Probe> = idle_probes.iter().chain(loaded).cloned().collect();
            LatencySummary::from_probes(&all)
        });

        let server_side = out.server_side.as_ref().map(|s| {
            let loaded: Vec<Option<f64>> = s
                .rtt_probes
                .iter()
                .filter(|p| p.phase == Phase::Loaded)
                .map(|p| p.rtt_ms)
                .collect();
            let client = out.steady_avg_mbps();
            ServerSideReport {
                steady_mbps: s.steady_mbps,
                client_diff_pct: s
                    .steady_mbps
                    .filter(|&m| m > 0.0 && client > 0.0)
                    .map(|m| (client - m) / m * 100.0),
                loaded_rtt: RttStats::from_rtts(&loaded),
                rtt_probes: s.rtt_probes.clone(),
            }
        });

        Self {
            backend,
            direction: out.direction,
            connection_mode: match cfg.connections {
                Connections::Auto(_) => "auto",
                Connections::Fixed(_) => "fixed",
            },
            connections: out.connections,
            connection_limit: out.connection_limit,
            connection_steps: out.connection_steps.clone(),
            scale_window_secs: out.scale_window_secs,
            warmup_secs: out.steady_start_secs,
            elapsed_secs: out.elapsed_secs,
            early_stopped: out.early_stopped,
            total_bytes: out.total_bytes,
            summary,
            latency,
            server_side,
            samples: out.samples.clone(),
            loaded_probes,
            stream_errors: out.stream_errors.clone(),
        }
    }

    /// 主要結果，例如「下載      91.8 Mbps」
    pub fn headline(&self) -> String {
        let label = self.direction.label();
        match &self.summary {
            Some(s) => format!("{label}      {:.1} Mbps", s.mbps),
            None => {
                format!("{label}      測試太短，暖機結束後沒有任何樣本，請加長測試時間或縮短暖機。")
            }
        }
    }

    /// 細節說明（每秒起伏、連線數、延遲、伺服器端對照、警告），每個元素一行
    pub fn detail_lines(&self) -> Vec<String> {
        let label = self.direction.label();
        let mut lines = Vec::new();
        if let Some(s) = &self.summary {
            lines.push(format!(
                "每秒起伏 P10–P90：{:.1} – {:.1} Mbps，中位數 {:.1}，最高 {:.1}（{} 個樣本）",
                s.p10_mbps, s.p90_mbps, s.median_mbps, s.max_mbps, s.samples
            ));
        }

        let warmup = self
            .warmup_secs
            .map_or("整場".into(), |w| format!("{w:.1}s"));
        let stop = if self.early_stopped {
            "，結果已穩定、提前結束"
        } else {
            ""
        };
        lines.push(format!(
            "連線數 {}；測試 {:.1}s（暖機 {warmup}{stop}），共 {:.1} MB",
            self.connection_text(),
            self.elapsed_secs,
            self.total_bytes as f64 / 1_000_000.0
        ));

        if let Some(lat) = &self.latency {
            match (&lat.loaded, lat.increase_ms, lat.bufferbloat_grade) {
                (Some(l), Some(inc), Some(grade)) => lines.push(format!(
                    "{label}中延遲 {:.1} ms（P90 {:.1}，抖動 {:.1}{}）→ 比閒置 +{inc:.1} ms，Bufferbloat 等級 {grade}",
                    l.median_ms,
                    l.p90_ms,
                    l.jitter_ms,
                    if l.lost > 0 {
                        format!("，遺失 {} 次", l.lost)
                    } else {
                        String::new()
                    }
                )),
                (Some(l), _, _) => lines.push(format!("{label}中延遲 {:.1} ms", l.median_ms)),
                (None, _, _) => {}
            }
        }

        if let Some(s) = &self.server_side {
            let speed = match (s.steady_mbps, s.client_diff_pct) {
                (Some(m), Some(d)) => format!("{m:.1} Mbps（客戶端差 {d:+.1}%）"),
                (Some(m), None) => format!("{m:.1} Mbps"),
                _ => "無速度資料".into(),
            };
            let rtt = s
                .loaded_rtt
                .as_ref()
                .map(|r| format!("，核心 RTT {:.1} ms", r.median_ms))
                .unwrap_or_default();
            lines.push(format!("伺服器端（TCPInfo）：{speed}{rtt}"));
        }

        if !self.stream_errors.is_empty() {
            lines.push(format!(
                "警告：有 {} 條串流發生錯誤，結果可能偏低。第一個錯誤：{}",
                self.stream_errors.len(),
                self.stream_errors[0]
            ));
        }
        lines
    }

    pub fn print_human(&self) {
        println!("{}", self.headline());
        for line in self.detail_lines() {
            println!("  {line}");
        }
    }
}

impl DirectionReport {
    /// 連線數的說明文字。
    pub fn connection_text(&self) -> String {
        // 協定只允許 1 條（例如 ndt7）：不論設定為何都只能 1 條，「自動：1」沒有意義
        if self.connection_limit == Some(1) {
            return format!("1（{} 規範固定 1 條）", self.backend);
        }
        if self.connection_mode != "auto" {
            return self.connections.to_string();
        }
        let path: Vec<String> = self
            .connection_steps
            .iter()
            .map(|s| s.connections.to_string())
            .collect();
        let window = self
            .scale_window_secs
            .filter(|&w| w > 1.0)
            .map(|w| format!("，評估窗格 {w:.1}s"))
            .unwrap_or_default();
        format!("{}（自動：{}{window}）", self.connections, path.join(" → "))
    }
}

fn spread_pct(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let hi = values.iter().copied().fold(f64::MIN, f64::max);
    let lo = values.iter().copied().fold(f64::MAX, f64::min);
    (hi > 0.0).then(|| (hi - lo) / hi * 100.0)
}

impl Report {
    pub fn new(results: Vec<BackendReport>) -> Self {
        let comparison = (results.len() >= 2).then(|| {
            let collect =
                |dir| -> Vec<f64> { results.iter().filter_map(|r| r.mbps(dir)).collect() };
            Comparison {
                download_spread_pct: spread_pct(&collect(Direction::Download)),
                upload_spread_pct: spread_pct(&collect(Direction::Upload)),
            }
        });
        Self {
            results,
            comparison,
        }
    }

    /// 各伺服器的結果已在測試過程中印出，這裡只印比較與註解。
    pub fn print_footer(&self) {
        if let Some(cmp) = &self.comparison {
            println!();
            println!("━━ 比較 ━━");
            let fmt = |v: Option<f64>| v.map_or("—".into(), |v| format!("{v:.1}"));
            let loaded = |d: &Option<DirectionReport>| {
                d.as_ref()
                    .and_then(|d| d.latency.as_ref())
                    .and_then(|l| l.loaded.as_ref())
                    .map(|l| l.median_ms)
            };
            for r in &self.results {
                println!(
                    "  {:<12} 下載 {:>8} Mbps   上傳 {:>8} Mbps   延遲（閒置/下載中/上傳中）{} / {} / {} ms",
                    r.backend,
                    fmt(r.mbps(Direction::Download)),
                    fmt(r.mbps(Direction::Upload)),
                    fmt(r.idle_latency.as_ref().map(|s| s.median_ms)),
                    fmt(loaded(&r.download)),
                    fmt(loaded(&r.upload)),
                );
            }
            for (label, spread) in [
                ("下載", cmp.download_spread_pct),
                ("上傳", cmp.upload_spread_pct),
            ] {
                if let Some(p) = spread {
                    println!("  {label}差異 {p:.1}%");
                    if p > SPREAD_WARN_PCT {
                        println!(
                            "    → 超過 {SPREAD_WARN_PCT:.0}%：瓶頸可能在你到特定伺服器的路徑上，而不是你自己的網路。"
                        );
                    }
                }
            }
        }
        println!();
        println!("註：速度為應用層 goodput，不含 TCP/IP/TLS 標頭，約比線路速度低 3–6%。");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spread() {
        assert_eq!(spread_pct(&[100.0]), None);
        assert_eq!(spread_pct(&[100.0, 90.0]), Some(10.0));
        assert_eq!(spread_pct(&[0.0, 0.0]), None);
    }

    fn dir_report(mode: &'static str, limit: Option<usize>, steps: &[usize]) -> DirectionReport {
        DirectionReport {
            backend: "ndt7",
            direction: Direction::Download,
            connection_mode: mode,
            connections: *steps.last().unwrap(),
            connection_limit: limit,
            connection_steps: steps
                .iter()
                .map(|&c| ConnectionStep {
                    t_secs: 0.0,
                    connections: c,
                })
                .collect(),
            scale_window_secs: Some(2.0),
            warmup_secs: None,
            elapsed_secs: 0.0,
            early_stopped: false,
            total_bytes: 0,
            summary: None,
            latency: None,
            server_side: None,
            samples: vec![],
            loaded_probes: vec![],
            stream_errors: vec![],
        }
    }

    #[test]
    fn connection_text() {
        // 協定固定 1 條：自動或固定模式都顯示規範
        assert_eq!(
            dir_report("auto", Some(1), &[1]).connection_text(),
            "1（ndt7 規範固定 1 條）"
        );
        assert_eq!(
            dir_report("fixed", Some(1), &[1]).connection_text(),
            "1（ndt7 規範固定 1 條）"
        );
        assert_eq!(
            dir_report("auto", None, &[2, 4]).connection_text(),
            "4（自動：2 → 4，評估窗格 2.0s）"
        );
        assert_eq!(dir_report("fixed", None, &[8]).connection_text(), "8");
    }
}
