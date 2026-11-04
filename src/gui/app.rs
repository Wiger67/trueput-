//! 主畫面：左邊設定、中間每台伺服器的結果卡片與即時圖表、最後是比較表。
//!
//! 測試在背景的 tokio runtime 執行，透過 channel 把進度送回 UI 執行緒；
//! 按「停止」會中止背景 task，傳輸與延遲量測都會跟著停。

use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::time::{Duration, Instant};

use eframe::egui::{self, Color32, RichText};
use egui_plot::{ColorConflictHandling, Legend, Line, Plot, PlotPoints};

use trueput::autoscale::AutoConfig;
use trueput::backend::Direction;
use trueput::engine::{self, Connections, Sample};
use trueput::latency;
use trueput::report::{BackendReport, Report, SPREAD_WARN_PCT};
use trueput::runner::{self, BackendKind, Progress};

const DOWNLOAD_COLOR: Color32 = Color32::from_rgb(59, 130, 246);
const UPLOAD_COLOR: Color32 = Color32::from_rgb(168, 85, 247);
const ERROR_COLOR: Color32 = Color32::from_rgb(220, 38, 38);
const WARN_COLOR: Color32 = Color32::from_rgb(217, 119, 6);

const DIRECTIONS: [Direction; 2] = [Direction::Download, Direction::Upload];

fn dir_color(dir: Direction) -> Color32 {
    match dir {
        Direction::Download => DOWNLOAD_COLOR,
        Direction::Upload => UPLOAD_COLOR,
    }
}

fn dir_index(dir: Direction) -> usize {
    match dir {
        Direction::Download => 0,
        Direction::Upload => 1,
    }
}

fn dir_arrow(dir: Direction) -> &'static str {
    match dir {
        Direction::Download => "↓",
        Direction::Upload => "↑",
    }
}

/// Bufferbloat 等級的顏色：A+/A 綠、B 黃綠、C 橘、D/F 紅
pub fn grade_color(grade: &str) -> Color32 {
    match grade {
        "A+" | "A" => Color32::from_rgb(22, 163, 74),
        "B" => Color32::from_rgb(132, 204, 22),
        "C" => Color32::from_rgb(234, 88, 12),
        _ => ERROR_COLOR,
    }
}

// ── 設定 ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    pub cloudflare: bool,
    pub ndt7: bool,
    pub download: bool,
    pub upload: bool,
    pub auto_connections: bool,
    pub max_connections: usize,
    pub fixed_connections: usize,
    pub duration_secs: f64,
    pub latency: bool,
    pub early_stop: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            cloudflare: true,
            ndt7: true,
            download: true,
            upload: true,
            auto_connections: true,
            max_connections: AutoConfig::default().max,
            fixed_connections: 4,
            duration_secs: engine::Config::default().duration.as_secs_f64(),
            latency: true,
            early_stop: true,
        }
    }
}

impl Settings {
    pub fn backends(&self) -> Vec<BackendKind> {
        BackendKind::ALL
            .into_iter()
            .filter(|k| match k {
                BackendKind::Cloudflare => self.cloudflare,
                BackendKind::Ndt7 => self.ndt7,
            })
            .collect()
    }

    pub fn directions(&self) -> Vec<Direction> {
        DIRECTIONS
            .into_iter()
            .filter(|d| match d {
                Direction::Download => self.download,
                Direction::Upload => self.upload,
            })
            .collect()
    }

    pub fn can_start(&self) -> bool {
        !self.backends().is_empty() && !self.directions().is_empty()
    }

    /// 轉成引擎設定；沒有在畫面上出現的項目用與命令列版相同的預設值。
    pub fn config(&self) -> engine::Config {
        engine::Config {
            connections: if self.auto_connections {
                Connections::Auto(AutoConfig {
                    max: self.max_connections,
                    ..Default::default()
                })
            } else {
                Connections::Fixed(self.fixed_connections)
            },
            duration: Duration::from_secs_f64(self.duration_secs),
            latency: self.latency.then(latency::Config::default),
            early_stop: self.early_stop.then(engine::EarlyStop::default),
            ..Default::default()
        }
    }
}

// ── 背景測試與 UI 之間的訊息 ────────────────────────────────

enum Msg {
    Status(String),
    /// 第幾台伺服器的最新報告
    Report(usize, Box<BackendReport>),
    Sample(usize, Direction, Sample),
    Finished(Report),
}

struct Job {
    rx: Receiver<Msg>,
    handle: tokio::task::JoinHandle<()>,
    started: Instant,
}

/// 一台伺服器在畫面上的狀態
struct BackendView {
    kind: BackendKind,
    planned: Vec<Direction>,
    report: Option<BackendReport>,
    /// 即時樣本（下載、上傳）
    live: [Vec<Sample>; 2],
    active: Option<Direction>,
}

impl BackendView {
    fn new(kind: BackendKind, planned: Vec<Direction>) -> Self {
        Self {
            kind,
            planned,
            report: None,
            live: [Vec::new(), Vec::new()],
            active: None,
        }
    }
}

/// 把樣本分成暖機與正式兩段折線。暖機段多帶第一個正式點，讓兩段線接起來。
pub fn split_series(samples: &[Sample]) -> (Vec<[f64; 2]>, Vec<[f64; 2]>) {
    let mut warm = Vec::new();
    let mut steady = Vec::new();
    for s in samples {
        let p = [s.t_secs, s.mbps];
        if s.warmup {
            warm.push(p);
        } else {
            if steady.is_empty() && !warm.is_empty() {
                warm.push(p);
            }
            steady.push(p);
        }
    }
    (warm, steady)
}

// ── App ─────────────────────────────────────────────────────

pub struct App {
    rt: tokio::runtime::Runtime,
    ctx: egui::Context,
    settings: Settings,
    job: Option<Job>,
    views: Vec<BackendView>,
    status: String,
    report: Option<Report>,
    copied_at: Option<Instant>,
}

impl App {
    pub fn new(ctx: egui::Context) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        Ok(Self {
            rt,
            ctx,
            settings: Settings::default(),
            job: None,
            views: Vec::new(),
            status: "選擇伺服器與測試項目，按「開始測試」。".into(),
            report: None,
            copied_at: None,
        })
    }

    fn running(&self) -> bool {
        self.job.is_some()
    }

    fn start(&mut self) {
        let backends = self.settings.backends();
        let dirs = self.settings.directions();
        let cfg = self.settings.config();
        self.views = backends
            .iter()
            .map(|&k| BackendView::new(k, dirs.clone()))
            .collect();
        self.report = None;
        self.status = "準備中…".into();

        let (tx, rx) = mpsc::channel();
        let ctx = self.ctx.clone();
        let handle = self.rt.spawn(async move {
            let send = |m: Msg| {
                let _ = tx.send(m);
                ctx.request_repaint();
            };
            let opts = runner::Options::default();
            let mut reports = Vec::new();
            for (i, kind) in backends.into_iter().enumerate() {
                let rep = runner::run_backend(kind, &cfg, &dirs, &opts, |p| match p {
                    Progress::Status(s) => send(Msg::Status(format!("{}：{s}", kind.name()))),
                    Progress::Ready(r) | Progress::DirectionDone(r, _) => {
                        send(Msg::Report(i, Box::new(r.clone())))
                    }
                    Progress::Sample(d, s) => send(Msg::Sample(i, d, s.clone())),
                })
                .await;
                reports.push(rep);
            }
            send(Msg::Finished(Report::new(reports)));
        });
        self.job = Some(Job {
            rx,
            handle,
            started: Instant::now(),
        });
    }

    fn stop(&mut self) {
        if let Some(job) = self.job.take() {
            job.handle.abort();
            for v in &mut self.views {
                v.active = None;
            }
            self.status = "已停止。".into();
        }
    }

    /// 處理背景送來的訊息
    fn poll(&mut self) {
        let Some(job) = &self.job else { return };
        loop {
            match job.rx.try_recv() {
                Ok(Msg::Status(s)) => self.status = s,
                Ok(Msg::Report(i, r)) => {
                    let v = &mut self.views[i];
                    v.report = Some(*r);
                    v.active = None;
                }
                Ok(Msg::Sample(i, d, s)) => {
                    let v = &mut self.views[i];
                    v.active = Some(d);
                    v.live[dir_index(d)].push(s);
                }
                Ok(Msg::Finished(rep)) => {
                    let secs = job.started.elapsed().as_secs_f64();
                    self.status = format!("完成，共 {secs:.0} 秒。");
                    self.report = Some(rep);
                    self.job = None;
                    return;
                }
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) => {
                    self.status = "測試意外中止。".into();
                    self.job = None;
                    return;
                }
            }
        }
    }

    // ── 畫面 ──

    fn top_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading("trueput 網路速度測試");
            ui.add_space(16.0);
            if self.running() {
                if ui
                    .add_sized(
                        [110.0, 30.0],
                        egui::Button::new(RichText::new("停止").size(16.0)),
                    )
                    .clicked()
                {
                    self.stop();
                }
            } else {
                let start = ui
                    .add_enabled(
                        self.settings.can_start(),
                        egui::Button::new(RichText::new("開始測試").size(16.0).strong())
                            .fill(DOWNLOAD_COLOR.gamma_multiply(0.85))
                            .min_size(egui::vec2(110.0, 30.0)),
                    )
                    .on_disabled_hover_text("至少選一台伺服器與一個測試項目");
                if start.clicked() {
                    self.start();
                }
            }
            ui.add_space(8.0);
            // 不用會持續轉動的 spinner：它讓畫面每秒重畫數十次，會跟測速搶 CPU。
            // 畫面只在收到新資料、或每 0.2 秒更新計時時才重畫。
            if let Some(job) = &self.job {
                ui.label(
                    RichText::new(format!(
                        "測試中 {:.0}s",
                        job.started.elapsed().as_secs_f64()
                    ))
                    .strong(),
                );
            }
            ui.label(&self.status);
        });
    }

    fn settings_panel(&mut self, ui: &mut egui::Ui) {
        let enabled = !self.running();
        ui.add_enabled_ui(enabled, |ui| {
            let s = &mut self.settings;
            ui.add_space(4.0);
            ui.strong("伺服器");
            ui.checkbox(&mut s.cloudflare, "Cloudflare")
                .on_hover_text(BackendKind::Cloudflare.description());
            ui.checkbox(&mut s.ndt7, "M-Lab ndt7")
                .on_hover_text(BackendKind::Ndt7.description());
            ui.weak("勾選多台會在最後比較結果");

            ui.separator();
            ui.strong("測試項目");
            ui.horizontal(|ui| {
                ui.checkbox(&mut s.download, "下載");
                ui.checkbox(&mut s.upload, "上傳");
            });

            ui.separator();
            ui.strong("連線數");
            ui.radio_value(&mut s.auto_connections, true, "自動調整");
            ui.add_enabled_ui(s.auto_connections, |ui| {
                ui.horizontal(|ui| {
                    ui.add_space(18.0);
                    ui.label("上限");
                    ui.add(egui::DragValue::new(&mut s.max_connections).range(1..=64));
                    ui.label("條");
                });
            });
            ui.radio_value(&mut s.auto_connections, false, "固定");
            ui.add_enabled_ui(!s.auto_connections, |ui| {
                ui.horizontal(|ui| {
                    ui.add_space(18.0);
                    ui.add(egui::DragValue::new(&mut s.fixed_connections).range(1..=64));
                    ui.label("條");
                });
            });
            ui.weak("ndt7 依規範固定 1 條");

            ui.separator();
            ui.strong("每個方向最長");
            ui.add(
                egui::Slider::new(&mut s.duration_secs, 5.0..=60.0)
                    .step_by(1.0)
                    .suffix(" 秒"),
            );
            ui.checkbox(&mut s.early_stop, "結果穩定時提前結束");
            ui.checkbox(&mut s.latency, "量測延遲（bufferbloat）");

            ui.separator();
            if ui.button("恢復預設值").clicked() {
                *s = Settings::default();
            }
        });
    }

    fn results(&mut self, ui: &mut egui::Ui) {
        if self.views.is_empty() {
            ui.centered_and_justified(|ui| {
                ui.label(
                    RichText::new("選擇左邊的伺服器與測試項目，按「開始測試」。")
                        .size(16.0)
                        .weak(),
                );
            });
            return;
        }
        egui::ScrollArea::vertical().show(ui, |ui| {
            let running = self.running();
            for view in &self.views {
                backend_card(ui, view, running);
                ui.add_space(10.0);
            }
            if let Some(rep) = &self.report {
                comparison(ui, rep);
            }
        });
    }

    fn bottom_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.weak("速度為應用層 goodput，不含 TCP/IP/TLS 標頭，約比線路速度低 3–6%。");
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                let copied = self
                    .copied_at
                    .is_some_and(|t| t.elapsed() < Duration::from_secs(2));
                let label = if copied {
                    "已複製！"
                } else {
                    "複製 JSON 結果"
                };
                let btn = ui
                    .add_enabled(self.report.is_some(), egui::Button::new(label))
                    .on_hover_text("完整結果（含所有樣本），可貼到檔案或回報問題時使用");
                if btn.clicked()
                    && let Some(rep) = &self.report
                    && let Ok(json) = serde_json::to_string_pretty(rep)
                {
                    ui.ctx().copy_text(json);
                    self.copied_at = Some(Instant::now());
                }
            });
        });
    }
}

impl eframe::App for App {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll();
        // 執行中每 0.2 秒更新一次計時；「已複製」的提示也要按時消失
        if self.running() || self.copied_at.is_some() {
            ctx.request_repaint_after(Duration::from_millis(200));
        }
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        egui::Panel::top("top")
            .frame(egui::Frame::side_top_panel(ui.style()).inner_margin(10.0))
            .show(ui, |ui| self.top_bar(ui));
        egui::Panel::bottom("bottom")
            .frame(egui::Frame::side_top_panel(ui.style()).inner_margin(8.0))
            .show(ui, |ui| self.bottom_bar(ui));
        egui::Panel::left("settings")
            .resizable(false)
            .exact_size(230.0)
            .frame(egui::Frame::side_top_panel(ui.style()).inner_margin(12.0))
            .show(ui, |ui| self.settings_panel(ui));
        egui::CentralPanel::default().show(ui, |ui| self.results(ui));
    }
}

// ── 卡片元件 ────────────────────────────────────────────────

fn backend_card(ui: &mut egui::Ui, view: &BackendView, running: bool) {
    egui::Frame::group(ui.style())
        .inner_margin(14.0)
        .corner_radius(8.0)
        .show(ui, |ui| {
            ui.set_width(ui.available_width());
            let rep = view.report.as_ref();

            ui.horizontal(|ui| {
                ui.label(RichText::new(view.kind.name()).size(18.0).strong());
                match rep {
                    Some(r) => ui.label(RichText::new(r.server_text()).weak()),
                    None => ui.label(RichText::new("等待中").weak()),
                };
            });
            if let Some(t) = rep.and_then(BackendReport::idle_text) {
                ui.label(t);
            }
            if let Some(r) = rep {
                for e in &r.errors {
                    ui.label(RichText::new(e).color(ERROR_COLOR));
                }
            }
            ui.add_space(6.0);

            ui.columns(2, |cols| {
                for (col, dir) in cols.iter_mut().zip(DIRECTIONS) {
                    direction_tile(col, view, dir, running);
                }
            });

            if view.live.iter().any(|l| !l.is_empty()) {
                ui.add_space(6.0);
                speed_chart(ui, view);
            }

            if let Some(r) = rep
                && (r.download.is_some() || r.upload.is_some())
            {
                egui::CollapsingHeader::new("詳細資料")
                    .id_salt(("details", view.kind.name()))
                    .show(ui, |ui| {
                        for dir in DIRECTIONS {
                            if let Some(d) = r.direction(dir) {
                                ui.label(RichText::new(dir.label()).strong().color(dir_color(dir)));
                                for line in d.detail_lines() {
                                    ui.label(line);
                                }
                            }
                        }
                    });
            }
        });
}

/// 一個方向沒有結果、也不在進行中時的狀態文字
pub fn pending_text(planned: bool, connected: Option<bool>, running: bool) -> &'static str {
    match (planned, connected, running) {
        (false, _, _) => "未選擇",
        // 連不上伺服器，這個方向不會執行
        (true, Some(false), _) => "未執行",
        (true, _, true) => "等待中",
        // 測試已結束（完成、停止或失敗），這個方向沒有結果
        (true, _, false) => "未完成",
    }
}

fn direction_tile(ui: &mut egui::Ui, view: &BackendView, dir: Direction, running: bool) {
    let color = dir_color(dir);
    ui.label(
        RichText::new(format!("{} {}", dir_arrow(dir), dir.label()))
            .color(color)
            .strong(),
    );

    let done = view.report.as_ref().and_then(|r| r.direction(dir));
    let big = |ui: &mut egui::Ui, value: String, strong: bool| {
        ui.horizontal(|ui| {
            let text = RichText::new(value).size(34.0);
            ui.label(if strong { text.strong() } else { text.weak() });
            ui.label(RichText::new("Mbps").weak());
        });
    };

    if let Some(d) = done {
        match &d.summary {
            Some(s) => {
                big(ui, format!("{:.1}", s.mbps), true);
                ui.weak(format!(
                    "每秒 P10–P90：{:.0} – {:.0}",
                    s.p10_mbps, s.p90_mbps
                ));
            }
            None => {
                ui.label(RichText::new("測試太短").color(WARN_COLOR));
            }
        }
        if let Some(lat) = &d.latency
            && let (Some(l), Some(inc), Some(grade)) =
                (&lat.loaded, lat.increase_ms, lat.bufferbloat_grade)
        {
            ui.horizontal(|ui| {
                ui.label(
                    RichText::new(format!(" {grade} "))
                        .strong()
                        .color(Color32::WHITE)
                        .background_color(grade_color(grade)),
                )
                .on_hover_text("Bufferbloat 等級：傳輸中延遲比閒置增加越少越好");
                ui.label(format!(
                    "{}中延遲 {:.1} ms（+{inc:.1}）",
                    dir.label(),
                    l.median_ms
                ));
            });
        }
        if d.early_stopped {
            ui.weak(format!("{:.1} 秒，結果穩定提前結束", d.elapsed_secs));
        }
    } else if view.active == Some(dir) {
        let last = view.live[dir_index(dir)].last();
        big(
            ui,
            last.map_or("…".into(), |s| format!("{:.1}", s.mbps)),
            false,
        );
        if let Some(s) = last {
            let phase = if s.warmup { "暖機中" } else { "量測中" };
            ui.weak(format!(
                "{phase} {:.1}s · {} 條連線",
                s.t_secs, s.connections
            ));
        }
    } else {
        let connected = view.report.as_ref().map(|r| r.server.is_some());
        big(ui, "—".into(), false);
        ui.weak(pending_text(
            view.planned.contains(&dir),
            connected,
            running,
        ));
    }
}

fn speed_chart(ui: &mut egui::Ui, view: &BackendView) {
    Plot::new(("speed", view.kind.name()))
        .height(170.0)
        // 同一方向的暖機段與正式段同名、顏色不同：圖例用先畫的正式段顏色
        .legend(Legend::default().color_conflict_handling(ColorConflictHandling::PickFirst))
        .allow_drag(false)
        .allow_zoom(false)
        .allow_scroll(false)
        .allow_boxed_zoom(false)
        .include_x(0.0)
        .include_y(0.0)
        .x_axis_label("秒")
        .y_axis_label("Mbps")
        .show(ui, |plot| {
            for dir in DIRECTIONS {
                let (warm, steady) = split_series(&view.live[dir_index(dir)]);
                let color = dir_color(dir);
                // 先畫正式段（實色），再畫暖機段（淡色）
                if !steady.is_empty() {
                    plot.line(
                        Line::new(dir.label(), PlotPoints::from(steady))
                            .color(color)
                            .width(2.0),
                    );
                }
                if !warm.is_empty() {
                    plot.line(
                        Line::new(dir.label(), PlotPoints::from(warm))
                            .color(color.gamma_multiply(0.35))
                            .width(1.5),
                    );
                }
            }
        });
    ui.weak("淡色：暖機（慢啟動、調整連線數），不列入結果");
}

fn comparison(ui: &mut egui::Ui, rep: &Report) {
    let Some(cmp) = &rep.comparison else { return };
    egui::Frame::group(ui.style())
        .inner_margin(14.0)
        .corner_radius(8.0)
        .show(ui, |ui| {
            ui.set_width(ui.available_width());
            ui.label(RichText::new("比較").size(18.0).strong());
            let fmt = |v: Option<f64>| v.map_or("—".into(), |v| format!("{v:.1}"));
            egui::Grid::new("comparison")
                .striped(true)
                .spacing([24.0, 6.0])
                .show(ui, |ui| {
                    for h in ["伺服器", "下載 Mbps", "上傳 Mbps", "閒置延遲 ms", "下載中 ms", "上傳中 ms"] {
                        ui.strong(h);
                    }
                    ui.end_row();
                    for r in &rep.results {
                        let loaded = |dir| {
                            r.direction(dir)
                                .and_then(|d| d.latency.as_ref())
                                .and_then(|l| l.loaded.as_ref())
                                .map(|l| l.median_ms)
                        };
                        ui.label(r.backend);
                        ui.label(fmt(r.mbps(Direction::Download)));
                        ui.label(fmt(r.mbps(Direction::Upload)));
                        ui.label(fmt(r.idle_latency.as_ref().map(|s| s.median_ms)));
                        ui.label(fmt(loaded(Direction::Download)));
                        ui.label(fmt(loaded(Direction::Upload)));
                        ui.end_row();
                    }
                });
            ui.add_space(6.0);
            for (label, spread) in [
                ("下載", cmp.download_spread_pct),
                ("上傳", cmp.upload_spread_pct),
            ] {
                let Some(p) = spread else { continue };
                if p > SPREAD_WARN_PCT {
                    ui.label(
                        RichText::new(format!(
                            "{label}差異 {p:.1}%：超過 {SPREAD_WARN_PCT:.0}%，瓶頸可能在你到特定伺服器的路徑上，而不是你自己的網路。"
                        ))
                        .color(WARN_COLOR),
                    );
                } else {
                    ui.label(format!("{label}差異 {p:.1}%：各伺服器結果一致。"));
                }
            }
        });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(t: f64, mbps: f64, warmup: bool) -> Sample {
        Sample {
            t_secs: t,
            mbps,
            warmup,
            connections: 1,
            latest_rtt_ms: None,
        }
    }

    #[test]
    fn default_settings_match_cli_defaults() {
        let s = Settings::default();
        let cfg = s.config();
        let cli = engine::Config::default();
        assert_eq!(cfg.duration, cli.duration);
        assert_eq!(cfg.warmup, cli.warmup);
        assert_eq!(cfg.interval, cli.interval);
        assert_eq!(cfg.sample_window, cli.sample_window);
        assert!(cfg.latency.is_some() && cfg.early_stop.is_some());
        assert!(matches!(cfg.connections, Connections::Auto(a) if a.max == 16));
        assert_eq!(s.backends(), BackendKind::ALL);
        assert_eq!(s.directions(), DIRECTIONS);
    }

    #[test]
    fn settings_map_to_config() {
        let s = Settings {
            ndt7: false,
            upload: false,
            auto_connections: false,
            fixed_connections: 6,
            duration_secs: 20.0,
            latency: false,
            early_stop: false,
            ..Default::default()
        };
        let cfg = s.config();
        assert!(matches!(cfg.connections, Connections::Fixed(6)));
        assert_eq!(cfg.duration, Duration::from_secs(20));
        assert!(cfg.latency.is_none() && cfg.early_stop.is_none());
        assert_eq!(s.backends(), [BackendKind::Cloudflare]);
        assert_eq!(s.directions(), [Direction::Download]);
        assert!(s.can_start());
        assert!(
            !Settings {
                download: false,
                ..s
            }
            .can_start()
        );
    }

    #[test]
    fn split_series_connects_warmup_to_steady() {
        let samples = [
            sample(0.2, 10.0, true),
            sample(0.4, 20.0, true),
            sample(0.6, 30.0, false),
            sample(0.8, 31.0, false),
        ];
        let (warm, steady) = split_series(&samples);
        assert_eq!(warm, [[0.2, 10.0], [0.4, 20.0], [0.6, 30.0]]);
        assert_eq!(steady, [[0.6, 30.0], [0.8, 31.0]]);

        let (warm, steady) = split_series(&samples[2..]);
        assert!(warm.is_empty());
        assert_eq!(steady.len(), 2);
    }

    #[test]
    fn grade_colors() {
        assert_eq!(grade_color("A+"), grade_color("A"));
        assert_ne!(grade_color("A"), grade_color("C"));
        assert_eq!(grade_color("F"), ERROR_COLOR);
    }

    #[test]
    fn pending_states() {
        assert_eq!(pending_text(false, None, true), "未選擇");
        assert_eq!(pending_text(true, None, true), "等待中");
        assert_eq!(pending_text(true, Some(true), true), "等待中");
        assert_eq!(pending_text(true, Some(false), true), "未執行");
        assert_eq!(pending_text(true, Some(false), false), "未執行");
        assert_eq!(pending_text(true, Some(true), false), "未完成");
        assert_eq!(pending_text(true, None, false), "未完成");
    }
}
