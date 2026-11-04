//! 主畫面，盡量簡單：
//!
//! - 上方三個大數字（下載、上傳、延遲）和一顆「開始測試」按鈕；
//! - 測試進行中才出現即時圖表；
//! - 每台伺服器一列的精簡表格，加一句結論；
//! - 其餘技術細節收在預設摺疊的「詳細資料」，設定按「設定」才展開。
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
use trueput::stats::bufferbloat_grade;

const DOWNLOAD_COLOR: Color32 = Color32::from_rgb(59, 130, 246);
const UPLOAD_COLOR: Color32 = Color32::from_rgb(168, 85, 247);
const ERROR_COLOR: Color32 = Color32::from_rgb(220, 38, 38);
const WARN_COLOR: Color32 = Color32::from_rgb(217, 119, 6);

/// 內容欄的最大寬度：視窗再寬也置中，不要拉得太散
const CONTENT_WIDTH: f32 = 620.0;

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

fn grade_badge(ui: &mut egui::Ui, grade: &str) -> egui::Response {
    ui.label(
        RichText::new(format!(" {grade} "))
            .strong()
            .color(Color32::WHITE)
            .background_color(grade_color(grade)),
    )
    .on_hover_text("Bufferbloat 等級：傳輸中延遲比閒置時增加越少越好（A+ 最好，F 最差）")
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
    report: Option<BackendReport>,
    /// 即時樣本（下載、上傳）
    live: [Vec<Sample>; 2],
    active: Option<Direction>,
}

impl BackendView {
    fn new(kind: BackendKind) -> Self {
        Self {
            kind,
            report: None,
            live: [Vec::new(), Vec::new()],
            active: None,
        }
    }

    /// 正在測這個方向時的即時速度
    fn live_mbps(&self, dir: Direction) -> Option<f64> {
        (self.active == Some(dir))
            .then(|| self.live[dir_index(dir)].last().map(|s| s.mbps))
            .flatten()
    }

    fn idle_ms(&self) -> Option<f64> {
        self.report
            .as_ref()?
            .idle_latency
            .as_ref()
            .map(|s| s.median_ms)
    }

    /// 各方向中最大的「傳輸中延遲增加量」（等級取最差的那個方向）
    fn worst_increase_ms(&self) -> Option<f64> {
        let r = self.report.as_ref()?;
        DIRECTIONS
            .into_iter()
            .filter_map(|d| r.direction(d)?.latency.as_ref()?.increase_ms)
            .reduce(f64::max)
    }

    /// 連不上伺服器（準備失敗）時的錯誤
    fn connect_error(&self) -> Option<&str> {
        let r = self.report.as_ref()?;
        r.server
            .is_none()
            .then(|| r.errors.first().map_or("無法連線", String::as_str))
    }
}

/// 上方大數字要顯示的內容
#[derive(Debug, Default, PartialEq)]
pub struct Hero {
    /// 下載、上傳：(Mbps, 是否為測試中的即時值)
    pub speed: [Option<(f64, bool)>; 2],
    pub idle_ms: Option<f64>,
    pub increase_ms: Option<f64>,
    pub grade: Option<&'static str>,
}

/// 整理上方大數字：
/// - 速度：正在測的方向顯示即時值；測完的取各伺服器中最高的（最接近你的線路上限，
///   較低的通常是到該伺服器的路徑造成的）。
/// - 延遲：取閒置延遲最低（最近）的那台伺服器，等級看它最差的方向。
fn hero(views: &[BackendView]) -> Hero {
    let mut h = Hero::default();
    for dir in DIRECTIONS {
        h.speed[dir_index(dir)] = views
            .iter()
            .find_map(|v| v.live_mbps(dir))
            .map(|m| (m, true))
            .or_else(|| {
                views
                    .iter()
                    .filter_map(|v| v.report.as_ref()?.mbps(dir))
                    .reduce(f64::max)
                    .map(|m| (m, false))
            });
    }
    if let Some(nearest) = views
        .iter()
        .filter(|v| v.idle_ms().is_some())
        .min_by(|a, b| a.idle_ms().unwrap().total_cmp(&b.idle_ms().unwrap()))
    {
        h.idle_ms = nearest.idle_ms();
        h.increase_ms = nearest.worst_increase_ms();
        h.grade = h.increase_ms.map(bufferbloat_grade);
    }
    h
}

/// 延遲數字：10 ms 以下多一位小數（避免 0.3 ms 顯示成 0），以上取整數
pub fn fmt_ms(ms: f64) -> String {
    if ms < 10.0 {
        format!("{ms:.1}")
    } else {
        format!("{ms:.0}")
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

/// 一句話的比較結論；不到兩台伺服器有結果時為 `None`
fn verdict(report: &Report) -> Option<(String, bool)> {
    let cmp = report.comparison.as_ref()?;
    let worst = [cmp.download_spread_pct, cmp.upload_spread_pct]
        .into_iter()
        .flatten()
        .reduce(f64::max)?;
    Some(if worst > SPREAD_WARN_PCT {
        (
            format!(
                "各伺服器差異 {worst:.0}%：瓶頸可能在到某些伺服器的路徑上，而不是你自己的網路。"
            ),
            true,
        )
    } else {
        (
            format!("各伺服器結果一致（差異 {worst:.1}%），數字可信。"),
            false,
        )
    })
}

// ── App ─────────────────────────────────────────────────────

pub struct App {
    rt: tokio::runtime::Runtime,
    ctx: egui::Context,
    settings: Settings,
    show_settings: bool,
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
            show_settings: false,
            job: None,
            views: Vec::new(),
            status: String::new(),
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
        self.views = backends.iter().map(|&k| BackendView::new(k)).collect();
        self.report = None;
        self.show_settings = false;
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
            self.status = "已停止".into();
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
                    self.status = format!("完成，共 {secs:.0} 秒");
                    self.report = Some(rep);
                    self.job = None;
                    return;
                }
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) => {
                    self.status = "測試意外中止".into();
                    self.job = None;
                    return;
                }
            }
        }
    }

    // ── 畫面 ──

    fn header(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label(RichText::new("trueput").size(22.0).strong());
            ui.label(RichText::new("網路速度測試").size(16.0).weak());
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                let label = if self.show_settings {
                    "收起設定"
                } else {
                    "設定"
                };
                let btn = ui.add_enabled(!self.running(), egui::Button::new(label));
                if btn.clicked() {
                    self.show_settings = !self.show_settings;
                }
            });
        });
    }

    fn hero(&mut self, ui: &mut egui::Ui) {
        let h = hero(&self.views);
        ui.columns(3, |cols| {
            for (col, dir) in cols.iter_mut().zip(DIRECTIONS) {
                col.vertical_centered(|ui| {
                    ui.label(
                        RichText::new(format!("{} {}", dir_arrow(dir), dir.label()))
                            .color(dir_color(dir))
                            .strong(),
                    );
                    let (text, live) = match h.speed[dir_index(dir)] {
                        Some((m, live)) => (format!("{m:.1}"), live),
                        None => ("—".into(), false),
                    };
                    let big = RichText::new(text).size(46.0);
                    ui.label(if live { big.weak() } else { big.strong() })
                        .on_hover_text("測了多台伺服器時，顯示其中最高的結果");
                    ui.weak("Mbps");
                });
            }
            cols[2].vertical_centered(|ui| {
                ui.label(RichText::new("延遲").strong());
                let text = h.idle_ms.map_or("—".into(), fmt_ms);
                ui.label(RichText::new(text).size(46.0).strong())
                    .on_hover_text("閒置時的延遲（取最近的伺服器）");
                ui.weak("ms");
                if let (Some(inc), Some(grade)) = (h.increase_ms, h.grade) {
                    ui.horizontal(|ui| {
                        // 手動置中：badge + 文字
                        let w = 150.0;
                        ui.add_space(((ui.available_width() - w) / 2.0).max(0.0));
                        grade_badge(ui, grade);
                        ui.weak(format!("負載下 +{} ms", fmt_ms(inc)));
                    });
                }
            });
        });
    }

    fn start_button(&mut self, ui: &mut egui::Ui) {
        ui.vertical_centered(|ui| {
            let size = egui::vec2(200.0, 44.0);
            if self.running() {
                let stop = egui::Button::new(RichText::new("停止").size(18.0)).min_size(size);
                if ui.add(stop).clicked() {
                    self.stop();
                }
            } else {
                let label = if self.views.is_empty() {
                    "開始測試"
                } else {
                    "再測一次"
                };
                let start = egui::Button::new(
                    RichText::new(label)
                        .size(18.0)
                        .strong()
                        .color(Color32::WHITE),
                )
                .fill(DOWNLOAD_COLOR)
                .min_size(size);
                let resp = ui
                    .add_enabled(self.settings.can_start(), start)
                    .on_disabled_hover_text("請在「設定」中至少選一台伺服器與一個測試項目");
                if resp.clicked() {
                    self.start();
                }
            }
            let status = match &self.job {
                Some(job) => format!(
                    "{}  ·  {:.0}s",
                    self.status,
                    job.started.elapsed().as_secs_f64()
                ),
                None => self.status.clone(),
            };
            if !status.is_empty() {
                ui.weak(status);
            }
        });
    }

    /// 測試進行中才顯示：目前這台伺服器、這個方向的即時速度
    fn live_chart(&self, ui: &mut egui::Ui) {
        let Some((view, dir)) = self.views.iter().find_map(|v| v.active.map(|d| (v, d))) else {
            return;
        };
        speed_plot(ui, ("live", view.kind.name()), &view.live, Some(dir), 130.0);
    }

    fn server_table(&self, ui: &mut egui::Ui) {
        if self.views.is_empty() {
            return;
        }
        egui::Grid::new("servers")
            .num_columns(5)
            .spacing([28.0, 8.0])
            .striped(true)
            .show(ui, |ui| {
                for h in ["伺服器", "下載", "上傳", "延遲", "等級"] {
                    ui.weak(h);
                }
                ui.end_row();
                for v in &self.views {
                    let name = ui.strong(v.kind.name());
                    if let Some(r) = &v.report {
                        name.on_hover_text(r.server_text());
                    }
                    if let Some(err) = v.connect_error() {
                        ui.label(RichText::new("無法連線").color(ERROR_COLOR))
                            .on_hover_text(err);
                        ui.end_row();
                        continue;
                    }
                    for dir in DIRECTIONS {
                        let done = v.report.as_ref().and_then(|r| r.mbps(dir));
                        match (done, v.live_mbps(dir)) {
                            (Some(m), _) => ui.label(format!("{m:.1}")),
                            (None, Some(m)) => ui.weak(format!("{m:.1}…")),
                            (None, None) => ui.weak("—"),
                        };
                    }
                    ui.label(
                        v.idle_ms()
                            .map_or("—".into(), |ms| format!("{} ms", fmt_ms(ms))),
                    );
                    match v.worst_increase_ms() {
                        Some(inc) => {
                            grade_badge(ui, bufferbloat_grade(inc));
                        }
                        None => {
                            ui.weak("—");
                        }
                    }
                    ui.end_row();
                }
            });
        if let Some((text, warn)) = self.report.as_ref().and_then(verdict) {
            ui.add_space(4.0);
            let t = RichText::new(text);
            ui.label(if warn { t.color(WARN_COLOR) } else { t.weak() });
        }
    }

    fn details(&mut self, ui: &mut egui::Ui) {
        if self.views.iter().all(|v| v.report.is_none()) {
            return;
        }
        egui::CollapsingHeader::new("詳細資料")
            .default_open(false)
            .show(ui, |ui| {
                for v in &self.views {
                    let Some(r) = &v.report else { continue };
                    ui.add_space(6.0);
                    ui.label(
                        RichText::new(format!("{}：{}", r.backend, r.server_text())).strong(),
                    );
                    if let Some(t) = r.idle_text() {
                        ui.label(t);
                    }
                    for e in &r.errors {
                        ui.label(RichText::new(e).color(ERROR_COLOR));
                    }
                    for dir in DIRECTIONS {
                        if let Some(d) = r.direction(dir) {
                            ui.label(RichText::new(d.headline()).color(dir_color(dir)));
                            for line in d.detail_lines() {
                                ui.label(format!("    {line}"));
                            }
                        }
                    }
                    if v.live.iter().any(|l| !l.is_empty()) {
                        speed_plot(ui, ("detail", v.kind.name()), &v.live, None, 120.0);
                    }
                }
                ui.add_space(8.0);
                ui.weak("速度為應用層 goodput，不含 TCP/IP/TLS 標頭，約比線路速度低 3–6%。圖表淡色段為暖機，不列入結果。");
                let copied = self
                    .copied_at
                    .is_some_and(|t| t.elapsed() < Duration::from_secs(2));
                let label = if copied { "已複製！" } else { "複製 JSON 結果" };
                let btn = ui
                    .add_enabled(self.report.is_some(), egui::Button::new(label))
                    .on_hover_text("完整結果（含所有樣本），回報問題時可以附上");
                if btn.clicked()
                    && let Some(rep) = &self.report
                    && let Ok(json) = serde_json::to_string_pretty(rep)
                {
                    ui.ctx().copy_text(json);
                    self.copied_at = Some(Instant::now());
                }
            });
    }

    /// 設定：在標題下方展開，不會蓋住其他內容
    fn settings_panel(&mut self, ui: &mut egui::Ui) {
        egui::Frame::group(ui.style())
            .inner_margin(12.0)
            .corner_radius(8.0)
            .show(ui, |ui| {
                ui.set_width(ui.available_width());
                let s = &mut self.settings;
                ui.horizontal(|ui| {
                    ui.strong("伺服器");
                    ui.checkbox(&mut s.cloudflare, "Cloudflare")
                        .on_hover_text(BackendKind::Cloudflare.description());
                    ui.checkbox(&mut s.ndt7, "M-Lab ndt7")
                        .on_hover_text(BackendKind::Ndt7.description());
                });
                ui.horizontal(|ui| {
                    ui.strong("測試項目");
                    ui.checkbox(&mut s.download, "下載");
                    ui.checkbox(&mut s.upload, "上傳");
                });
                egui::CollapsingHeader::new("進階").show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label("連線數");
                        ui.radio_value(&mut s.auto_connections, true, "自動，最多");
                        ui.radio_value(&mut s.auto_connections, false, "固定");
                        let n = if s.auto_connections {
                            &mut s.max_connections
                        } else {
                            &mut s.fixed_connections
                        };
                        ui.add(egui::DragValue::new(n).range(1..=64).suffix(" 條"));
                    });
                    ui.horizontal(|ui| {
                        ui.label("每個方向最長");
                        ui.add(
                            egui::Slider::new(&mut s.duration_secs, 5.0..=60.0)
                                .step_by(1.0)
                                .suffix(" 秒"),
                        );
                    });
                    ui.checkbox(&mut s.early_stop, "結果穩定時提前結束");
                    ui.checkbox(&mut s.latency, "量測延遲（bufferbloat）");
                    if ui.button("恢復預設值").clicked() {
                        *s = Settings::default();
                    }
                });
            });
    }
}

impl eframe::App for App {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll();
        // 執行中每 0.2 秒更新一次計時；「已複製」的提示也要按時消失。
        // 不用持續的動畫：畫面只在有新資料時重畫，才不會跟測速搶 CPU。
        if self.running() || self.copied_at.is_some() {
            ctx.request_repaint_after(Duration::from_millis(200));
        }
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ui, |ui| {
            egui::ScrollArea::vertical().show(ui, |ui| {
                // 內容置中，最寬 CONTENT_WIDTH
                let width = ui.available_width().min(CONTENT_WIDTH);
                ui.horizontal(|ui| {
                    ui.add_space(((ui.available_width() - width) / 2.0).max(0.0));
                    ui.vertical(|ui| {
                        ui.set_width(width);
                        ui.add_space(8.0);
                        self.header(ui);
                        if self.show_settings && !self.running() {
                            ui.add_space(8.0);
                            self.settings_panel(ui);
                        }
                        ui.add_space(28.0);
                        self.hero(ui);
                        ui.add_space(24.0);
                        self.start_button(ui);
                        ui.add_space(16.0);
                        self.live_chart(ui);
                        ui.add_space(12.0);
                        self.server_table(ui);
                        ui.add_space(12.0);
                        self.details(ui);
                        ui.add_space(16.0);
                    });
                });
            });
        });
    }
}

// ── 圖表 ────────────────────────────────────────────────────

/// 速度折線圖；`only` 指定時只畫那個方向（即時圖），否則下載、上傳都畫。
fn speed_plot(
    ui: &mut egui::Ui,
    id: impl std::hash::Hash + std::fmt::Debug,
    live: &[Vec<Sample>; 2],
    only: Option<Direction>,
    height: f32,
) {
    Plot::new(id)
        .height(height)
        // 同一方向的暖機段與正式段同名、顏色不同：圖例用先畫的正式段顏色
        .legend(Legend::default().color_conflict_handling(ColorConflictHandling::PickFirst))
        .allow_drag(false)
        .allow_zoom(false)
        .allow_scroll(false)
        .allow_boxed_zoom(false)
        .include_x(0.0)
        .include_y(0.0)
        .y_axis_label("Mbps")
        .show(ui, |plot| {
            for dir in DIRECTIONS {
                if only.is_some_and(|d| d != dir) {
                    continue;
                }
                let (warm, steady) = split_series(&live[dir_index(dir)]);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use trueput::report::{DirectionReport, Report};
    use trueput::stats::{LatencySummary, RttStats, Summary};

    fn sample(t: f64, mbps: f64, warmup: bool) -> Sample {
        Sample {
            t_secs: t,
            mbps,
            warmup,
            connections: 1,
            latest_rtt_ms: None,
        }
    }

    fn rtt(ms: f64) -> RttStats {
        RttStats {
            median_ms: ms,
            min_ms: ms,
            p90_ms: ms,
            jitter_ms: 0.0,
            count: 10,
            lost: 0,
        }
    }

    /// 組出一份測完的報告
    fn finished(name: &'static str, down: f64, up: f64, idle: f64, inc: [f64; 2]) -> BackendView {
        let dir = |direction: Direction, mbps: f64, inc: f64| DirectionReport {
            backend: name,
            direction,
            connection_mode: "auto",
            connections: 4,
            connection_limit: None,
            connection_steps: vec![],
            scale_window_secs: Some(1.0),
            warmup_secs: Some(3.0),
            elapsed_secs: 8.0,
            early_stopped: true,
            total_bytes: 1,
            summary: Some(Summary {
                mbps,
                median_mbps: mbps,
                p10_mbps: mbps,
                p90_mbps: mbps,
                max_mbps: mbps,
                samples: 10,
            }),
            latency: Some(LatencySummary {
                idle: Some(rtt(idle)),
                loaded: Some(rtt(idle + inc)),
                increase_ms: Some(inc),
                bufferbloat_grade: Some(bufferbloat_grade(inc)),
            }),
            server_side: None,
            samples: vec![],
            loaded_probes: vec![],
            stream_errors: vec![],
        };
        let mut r = BackendReport::new(name);
        r.server = Some(trueput::backend::ServerInfo {
            name: format!("{name}.example"),
            location: None,
        });
        r.idle_latency = Some(rtt(idle));
        r.download = Some(dir(Direction::Download, down, inc[0]));
        r.upload = Some(dir(Direction::Upload, up, inc[1]));
        BackendView {
            kind: if name == "ndt7" {
                BackendKind::Ndt7
            } else {
                BackendKind::Cloudflare
            },
            report: Some(r),
            live: [Vec::new(), Vec::new()],
            active: None,
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
    fn hero_takes_best_speed_and_nearest_server_latency() {
        let views = [
            // Cloudflare 比較近（6.7 ms），上傳時延遲增加較多
            finished("cloudflare", 91.8, 94.7, 6.7, [24.0, 35.0]),
            finished("ndt7", 92.5, 93.2, 15.6, [24.0, 24.0]),
        ];
        let h = hero(&views);
        assert_eq!(h.speed, [Some((92.5, false)), Some((94.7, false))]);
        assert_eq!(h.idle_ms, Some(6.7));
        assert_eq!(h.increase_ms, Some(35.0), "取最近伺服器最差的方向");
        assert_eq!(h.grade, Some("B"));
    }

    #[test]
    fn hero_shows_live_value_while_testing() {
        let mut testing = BackendView::new(BackendKind::Ndt7);
        testing.active = Some(Direction::Upload);
        testing.live[1].push(sample(1.0, 50.0, true));
        let views = [finished("cloudflare", 91.8, 94.7, 6.7, [5.0, 5.0]), testing];
        let h = hero(&views);
        assert_eq!(h.speed[0], Some((91.8, false)));
        assert_eq!(h.speed[1], Some((50.0, true)), "正在測的方向顯示即時值");
        assert_eq!(hero(&[]), Hero::default());
    }

    #[test]
    fn connect_error_only_when_prepare_failed() {
        let mut failed = BackendView::new(BackendKind::Ndt7);
        let mut r = BackendReport::new("ndt7");
        r.errors.push("準備測試失敗：timeout".into());
        failed.report = Some(r);
        assert_eq!(failed.connect_error(), Some("準備測試失敗：timeout"));
        assert_eq!(
            finished("cloudflare", 1.0, 1.0, 1.0, [0.0, 0.0]).connect_error(),
            None
        );
        assert_eq!(BackendView::new(BackendKind::Ndt7).connect_error(), None);
    }

    #[test]
    fn verdict_text() {
        let views = [
            finished("cloudflare", 91.8, 94.7, 6.7, [5.0, 5.0]),
            finished("ndt7", 90.9, 93.2, 15.6, [5.0, 5.0]),
        ];
        let rep = Report::new(views.into_iter().map(|v| v.report.unwrap()).collect());
        let (text, warn) = verdict(&rep).unwrap();
        assert!(!warn && text.contains("一致"), "{text}");

        let views = [
            finished("cloudflare", 100.0, 50.0, 6.7, [5.0, 5.0]),
            finished("ndt7", 70.0, 50.0, 15.6, [5.0, 5.0]),
        ];
        let rep = Report::new(views.into_iter().map(|v| v.report.unwrap()).collect());
        let (text, warn) = verdict(&rep).unwrap();
        assert!(warn && text.contains("30%"), "{text}");

        let single = Report::new(vec![
            finished("cloudflare", 1.0, 1.0, 1.0, [0.0, 0.0])
                .report
                .unwrap(),
        ]);
        assert!(verdict(&single).is_none());
    }

    #[test]
    fn ms_formatting() {
        assert_eq!(fmt_ms(0.34), "0.3");
        assert_eq!(fmt_ms(6.7), "6.7");
        assert_eq!(fmt_ms(15.6), "16");
        assert_eq!(fmt_ms(123.4), "123");
    }
}
