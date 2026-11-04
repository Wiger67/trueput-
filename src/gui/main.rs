//! 視窗版。命令列版見 `src/main.rs`；兩者共用同一套測速引擎。
//!
//! Windows 上 `cargo build --release` 會產生獨立的 `target\release\trueput-gui.exe`。

// Windows release 版不要跳出黑色主控台視窗（debug 版保留，方便看錯誤訊息）
#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

mod app;
mod fonts;
mod icon;

use eframe::egui;

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("trueput 網路速度測試")
            .with_inner_size([1040.0, 760.0])
            .with_min_inner_size([760.0, 540.0])
            .with_icon(icon::app_icon()),
        ..Default::default()
    };
    eframe::run_native(
        "trueput",
        options,
        Box::new(|cc| {
            fonts::install(&cc.egui_ctx);
            Ok(Box::new(app::App::new(cc.egui_ctx.clone())?))
        }),
    )
}
