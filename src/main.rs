//! 命令列版。視窗版見 `src/gui/`。

use std::io::{IsTerminal, Write};
use std::time::Duration;

use clap::{Parser, ValueEnum};

use trueput::backend::Direction;
use trueput::runner::{self, Progress};
use trueput::{autoscale, engine, latency, report};

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

impl BackendKind {
    fn to_runner(self) -> Option<runner::BackendKind> {
        match self {
            BackendKind::Cloudflare => Some(runner::BackendKind::Cloudflare),
            BackendKind::Ndt7 => Some(runner::BackendKind::Ndt7),
            BackendKind::All => None,
        }
    }
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
        latency: (!args.no_latency).then(|| latency::Config {
            interval: Duration::from_millis(args.latency_interval_ms),
            ..Default::default()
        }),
        early_stop: (!args.no_early_stop).then(engine::EarlyStop::default),
        ..Default::default()
    };

    let human = !args.json;
    let progress = human && std::io::stderr().is_terminal();
    let opts = runner::Options {
        cf_request_bytes: args.cf_request_mb * 1_000_000,
    };
    let mut results = Vec::new();

    for kind in backend_list(&args.backend) {
        let kind = kind.to_runner().expect("all 已在 backend_list 展開");
        let rep = runner::run_backend(kind, &cfg, args.test.directions(), &opts, |p| match p {
            Progress::Status(msg) => status(progress, msg),
            Progress::Sample(dir, s) => {
                if progress {
                    progress_line(dir, s);
                }
            }
            Progress::Ready(rep) => {
                status(progress, "");
                if human {
                    rep.print_header();
                    match rep.server {
                        Some(_) => rep.print_idle(),
                        None => println!("錯誤：{}", rep.errors.last().map_or("", String::as_str)),
                    }
                }
            }
            Progress::DirectionDone(rep, dir) => {
                status(progress, "");
                if !human {
                    return;
                }
                let done = match dir {
                    Direction::Download => &rep.download,
                    Direction::Upload => &rep.upload,
                };
                match done {
                    Some(d) => d.print_human(),
                    None => println!("{}", rep.errors.last().map_or("", String::as_str)),
                }
            }
        })
        .await;
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
