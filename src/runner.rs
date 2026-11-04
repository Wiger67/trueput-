//! 對一台伺服器跑完整測試：準備 → 閒置延遲 → 各方向傳輸。
//!
//! 命令列版與視窗版共用；進度透過 [`Progress`] 回呼通知，由呼叫端決定怎麼顯示。

use std::sync::Arc;

use anyhow::Context;

use crate::backend::cloudflare::Cloudflare;
use crate::backend::ndt7::Ndt7;
use crate::backend::{Backend, Direction};
use crate::engine::{self, Config, Sample};
use crate::report::BackendReport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    Cloudflare,
    Ndt7,
}

impl BackendKind {
    pub const ALL: [BackendKind; 2] = [BackendKind::Cloudflare, BackendKind::Ndt7];

    pub fn name(self) -> &'static str {
        match self {
            BackendKind::Cloudflare => "cloudflare",
            BackendKind::Ndt7 => "ndt7",
        }
    }

    /// 給使用者看的說明
    pub fn description(self) -> &'static str {
        match self {
            BackendKind::Cloudflare => "speed.cloudflare.com（HTTP，可多連線）",
            BackendKind::Ndt7 => "M-Lab ndt7（單連線，附伺服器端 TCPInfo）",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Options {
    /// Cloudflare 每次下載請求的位元組數
    pub cf_request_bytes: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            cf_request_bytes: 25_000_000,
        }
    }
}

/// 測試進度。參照的報告是「到目前為止」的狀態。
pub enum Progress<'a> {
    /// 狀態文字（連線中、量測閒置延遲…）
    Status(&'a str),
    /// 伺服器準備完成、閒置延遲量好了；準備失敗時 `server` 為 `None` 且 `errors` 有原因
    Ready(&'a BackendReport),
    /// 傳輸中的即時樣本
    Sample(Direction, &'a Sample),
    /// 一個方向結束（成功時報告裡有該方向的結果，失敗時 `errors` 最後一筆是原因）
    DirectionDone(&'a BackendReport, Direction),
}

fn make_backend(kind: BackendKind, opts: &Options) -> anyhow::Result<Box<dyn Backend>> {
    Ok(match kind {
        BackendKind::Cloudflare => Box::new(Cloudflare::new(opts.cf_request_bytes)?),
        BackendKind::Ndt7 => Box::new(Ndt7::new()?),
    })
}

/// 對一台伺服器跑 `dirs` 指定的測試。錯誤不會中斷，而是記在報告的 `errors`。
pub async fn run_backend(
    kind: BackendKind,
    cfg: &Config,
    dirs: &[Direction],
    opts: &Options,
    mut on: impl FnMut(Progress<'_>) + Send,
) -> BackendReport {
    let mut rep = BackendReport::new(kind.name());

    on(Progress::Status(&format!("連線到 {}…", kind.name())));
    let prepared = async {
        let mut backend = make_backend(kind, opts)?;
        let server = backend.prepare().await.context("準備測試失敗")?;
        anyhow::Ok((backend, server))
    }
    .await;
    let backend: Arc<dyn Backend> = match prepared {
        Ok((backend, server)) => {
            rep.server = Some(server);
            Arc::from(backend)
        }
        Err(e) => {
            rep.errors.push(format!("{e:#}"));
            on(Progress::Ready(&rep));
            return rep;
        }
    };

    // 閒置延遲只量一次，下載與上傳共用
    let idle = match &cfg.latency {
        Some(lc) => {
            on(Progress::Status("量測閒置延遲…"));
            let idle = engine::measure_idle(backend.as_ref(), lc).await;
            rep.set_idle(&idle);
            Some(idle)
        }
        None => None,
    };
    on(Progress::Ready(&rep));

    for &dir in dirs {
        if !backend.supports(dir) {
            rep.errors
                .push(format!("{} 不支援{}測試", kind.name(), dir.label()));
            on(Progress::DirectionDone(&rep, dir));
            continue;
        }
        on(Progress::Status(&format!("{}測試中…", dir.label())));
        let res = engine::run(backend.clone(), dir, cfg, idle.as_ref(), |s| {
            on(Progress::Sample(dir, s));
        })
        .await;
        match res {
            Ok(out) => {
                rep.set_direction(cfg, &out);
            }
            Err(e) => rep.errors.push(format!("{}失敗：{e:#}", dir.label())),
        }
        on(Progress::DirectionDone(&rep, dir));
    }
    rep
}
