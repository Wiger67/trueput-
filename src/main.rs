mod autoscale;
mod backend;
mod engine;
mod latency;
mod report;
mod stats;

use std::io::{IsTerminal, Write};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, ValueEnum};

use backend::cloudflare::Cloudflare;
use backend::ndt7::Ndt7;
use backend::{Backend, Direction};

/// 輕量、方法透明的網路速度測試工具
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// 測速伺服器來源，可指定多個（以逗號分隔），`all` 表示全部；多個時會輸出比較
    #[arg(
        short,
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "cloudflare"
    )]
    backend: Vec<BackendKind>,

    /// 測試項目
    #[arg(short, long, value_enum, default_value_t = TestKind::Both)]
    test: TestKind,

    /// 平行連線數：`auto` 自動調整，或指定 1～64 的固定值（ndt7 固定為 1）
    #[arg(short, long, default_value = "auto", value_parser = parse_connections)]
    connections: ConnArg,

    /// 自動模式的連線數上限
    #[arg(long, default_value_t = 16, value_parser = clap::value_parser!(u16).range(1..=64))]
    max_connections: u16,

    /// 每個方向的測試長度上限（秒，含暖機）；結果穩定時會提前結束
    #[arg(short, long, default_value_t = 15.0, value_parser = parse_secs)]
    duration: f64,

    /// 最短暖機長度（秒），這段期間的樣本不列入統計；自動模式下，連線數穩定前也算暖機
    #[arg(short, long, default_value_t = 3.0, value_parser = parse_secs)]
    warmup: f64,

    /// 一定測滿 --duration，不因結果穩定而提前結束
    #[arg(long)]
    no_early_stop: bool,

    /// 取樣間隔（毫秒）
    #[arg(long, default_value_t = 200, value_parser = clap::value_parser!(u64).range(50..=5000))]
    interval_ms: u64,

    /// Cloudflare 每次請求的下載大小（MB）
    #[arg(long, default_value_t = 25)]
    cf_request_mb: u64,

    /// 不量延遲（閒置與負載下）
    #[arg(long)]
    no_latency: bool,

    /// 傳輸期間延遲量測的間隔（毫秒）
    #[arg(long, default_value_t = 250, value_parser = clap::value_parser!(u64).range(50..=5000))]
    latency_interval_ms: u64,

    /// 以 JSON 輸出結果（含所有樣本）
    #[arg(long)]
    json: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum BackendKind {
    /// speed.cloudflare.com（HTTP，可多連線）
    Cloudflare,
    /// M-Lab ndt7（WebSocket，單連線，附伺服器端 TCPInfo）
    Ndt7,
    /// 全部後端依序測試並比較
    All,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum TestKind {
    Download,
    Upload,
    Both,
}

impl TestKind {
    fn directions(self) -> &'static [Direction] {
        match self {
            TestKind::Download => &[Direction::Download],
            TestKind::Upload => &[Direction::Upload],
            TestKind::Both => &[Direction::Download, Direction::Upload],
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ConnArg {
    Auto,
    Fixed(usize),
}

fn parse_connections(s: &str) -> Result<ConnArg, String> {
    if s.eq_ignore_ascii_case("auto") {
        return Ok(ConnArg::Auto);
    }
    match s.parse::<usize>() {
        Ok(n) if (1..=64).contains(&n) => Ok(ConnArg::Fixed(n)),
        _ => Err("必須是 auto 或 1～64 的整數".into()),
    }
}

fn parse_secs(s: &str) -> Result<f64, String> {
    let v: f64 = s.parse().map_err(|_| format!("不是有效的秒數：{s}"))?;
    if v.is_finite() && (0.0..=300.0).contains(&v) {
        Ok(v)
    } else {
        Err("秒數必須介於 0 到 300 之間".into())
    }
}

/// 展開 `all`、去除重複，保留使用者指定的順序。
fn backend_list(kinds: &[BackendKind]) -> Vec<BackendKind> {
    let mut out = Vec::new();
    for &k in kinds {
        let expanded = match k {
            BackendKind::All => vec![BackendKind::Cloudflare, BackendKind::Ndt7],
            other => vec![other],
        };
        for e in expanded {
            if !out.contains(&e) {
                out.push(e);
            }
        }
    }
    out
}

fn make_backend(kind: BackendKind, args: &Args) -> anyhow::Result<Box<dyn Backend>> {
    Ok(match kind {
        BackendKind::Cloudflare => Box::new(Cloudflare::new(args.cf_request_mb * 1_000_000)?),
        BackendKind::Ndt7 => Box::new(Ndt7::new()?),
        BackendKind::All => unreachable!("all 已在 backend_list 展開"),
    })
}

fn progress_line(dir: Direction, s: &engine::Sample) {
    let tag = if s.warmup { " (暖機)" } else { "" };
    let rtt = s
        .latest_rtt_ms
        .map(|r| format!("  {r:7.1} ms"))
        .unwrap_or_default();
    eprint!(
        "\r\x1b[2K{} {:5.1}s  {:9.1} Mbps  {:2} 條連線{rtt}{tag}",
        dir.label(),
        s.t_secs,
        s.mbps,
        s.connections
    );
    let _ = std::io::stderr().flush();
}

fn status(progress: bool, msg: &str) {
    if progress {
        eprint!("\r\x1b[2K{msg}");
        let _ = std::io::stderr().flush();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if args.warmup >= args.duration {
        anyhow::bail!("暖機時間必須小於測試總長度");
    }

    let cfg = engine::Config {
        connections: match args.connections {
            ConnArg::Fixed(n) => engine::Connections::Fixed(n),
            ConnArg::Auto => engine::Connections::Auto(autoscale::AutoConfig {
                max: args.max_connections.into(),
                ..Default::default()
            }),
        },
        duration: Duration::from_secs_f64(args.duration),
        warmup: Duration::from_secs_f64(args.warmup),
        interval: Duration::from_millis(args.interval_ms),
        sample_window: Duration::from_secs(1),
        latency: (!args.no_latency).then(|| latency::Config {
            idle_probes: 10,
            idle_interval: Duration::from_millis(100),
            interval: Duration::from_millis(args.latency_interval_ms),
            timeout: Duration::from_secs(3),
        }),
        early_stop: (!args.no_early_stop).then(engine::EarlyStop::default),
    };

    let human = !args.json;
    let progress = human && std::io::stderr().is_terminal();
    let mut results = Vec::new();

    for kind in backend_list(&args.backend) {
        let mut backend = make_backend(kind, &args)?;
        let mut rep = report::BackendReport::new(backend.name());

        status(progress, &format!("連線到 {}…", backend.name()));
        match backend.prepare().await.context("準備測試失敗") {
            Ok(server) => rep.server = Some(server),
            Err(e) => {
                status(progress, "");
                rep.errors.push(format!("{e:#}"));
                if human {
                    rep.print_header();
                    println!("錯誤：{e:#}");
                }
                results.push(rep);
                continue;
            }
        }
        let backend: Arc<dyn Backend> = Arc::from(backend);

        // 閒置延遲只量一次，下載與上傳共用
        let idle = match &cfg.latency {
            Some(lc) => {
                status(progress, "量測閒置延遲…");
                let idle = engine::measure_idle(backend.as_ref(), lc).await;
                rep.set_idle(&idle);
                Some(idle)
            }
            None => None,
        };
        status(progress, "");
        if human {
            rep.print_header();
            rep.print_idle();
        }

        for &dir in args.test.directions() {
            if !backend.supports(dir) {
                let msg = format!("{} 不支援{}測試", backend.name(), dir.label());
                if human {
                    println!("{msg}");
                }
                rep.errors.push(msg);
                continue;
            }
            let res = engine::run(backend.clone(), dir, &cfg, idle.as_ref(), |s| {
                if progress {
                    progress_line(dir, s);
                }
            })
            .await;
            status(progress, "");
            match res {
                Ok(out) => {
                    let d = rep.set_direction(&cfg, &out);
                    if human {
                        d.print_human();
                    }
                }
                Err(e) => {
                    let msg = format!("{}失敗：{e:#}", dir.label());
                    if human {
                        println!("{msg}");
                    }
                    rep.errors.push(msg);
                }
            }
        }
        results.push(rep);
    }

    let all_failed = results
        .iter()
        .all(|r| r.download.is_none() && r.upload.is_none());
    let report = report::Report::new(results);
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        report.print_footer();
    }
    if all_failed {
        anyhow::bail!("所有測試都失敗了");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use BackendKind::*;

    #[test]
    fn expands_and_dedups_backends() {
        assert_eq!(backend_list(&[Cloudflare]), [Cloudflare]);
        assert_eq!(backend_list(&[All]), [Cloudflare, Ndt7]);
        assert_eq!(backend_list(&[Ndt7, All, Cloudflare]), [Ndt7, Cloudflare]);
    }
}
