//! trueput：輕量、方法透明的網路速度測試。
//!
//! 命令列版（`src/main.rs`）與視窗版（`src/gui/`）共用這個函式庫：
//! 測速引擎、後端、統計與報告都在這裡，兩邊的結果完全一致。

pub mod autoscale;
pub mod backend;
pub mod engine;
pub mod latency;
pub mod report;
pub mod runner;
pub mod stats;
