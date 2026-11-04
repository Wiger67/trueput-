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

use backend::Backend;
use backend::cloudflare::Cloudflare;
use backend::ndt7::Ndt7;

/// 輕量、方法透明的網路下載速度測試工具
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// 測速伺服器來源
    #[arg(short, long, value_enum, default_value_t = BackendKind::Cloudflare)]
    backend: BackendKind,

    /// 平行連線數：`auto` 自動調整，或指定 1～64 的固定值（ndt7 固定為 1）
    #[arg(short, long, default_value = "auto", value_parser = parse_connections)]
    connections: ConnArg,

    /// 自動模式的連線數上限
    #[arg(long, default_value_t = 16, value_parser = clap::value_parser!(u16).range(1..=64))]
    max_connections: u16,

    /// 測試總長度上限（秒，含暖機）
    #[arg(short, long, default_value_t = 15.0, value_parser = parse_secs)]
    duration: f64,

    /// 最短暖機長度（秒），這段期間的樣本不列入統計；自動模式下，連線數穩定前也算暖機
    #[arg(short, long, default_value_t = 3.0, value_parser = parse_secs)]
    warmup: f64,

    /// 取樣間隔（毫秒）
    #[arg(long, default_value_t = 200, value_parser = clap::value_parser!(u64).range(50..=5000))]
    interval_ms: u64,

    /// Cloudflare 每次請求的下載大小（MB）
    #[arg(long, default_value_t = 25)]
    cf_request_mb: u64,

    /// 不量延遲（閒置與負載下）
    #[arg(long)]
    no_latency: bool,

    /// 下載期間延遲量測的間隔（毫秒）
    #[arg(long, default_value_t = 250, value_parser = clap::value_parser!(u64).range(50..=5000))]
    latency_interval_ms: u64,

    /// 以 JSON 輸出結果（含所有樣本）
    #[arg(long)]
    json: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum BackendKind {
    /// speed.cloudflare.com（HTTP，可多連線）
    Cloudflare,
    /// M-Lab ndt7（WebSocket，單連線，測試長度由伺服器決定）
    Ndt7,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("無法設定 TLS 加密後端"))?;

    if args.warmup >= args.duration {
        anyhow::bail!("暖機時間必須小於測試總長度");
    }

    let mut backend: Box<dyn Backend> = match args.backend {
        BackendKind::Cloudflare => Box::new(Cloudflare::new(args.cf_request_mb * 1_000_000)?),
        BackendKind::Ndt7 => Box::new(Ndt7::new()?),
    };

    let progress = !args.json && std::io::stderr().is_terminal();

    let server = backend.prepare().await.context("準備測試失敗")?;
    if !args.json {
        eprintln!(
            "伺服器：{} ({}){}",
            server.name,
            backend.name(),
            server
                .location
                .as_deref()
                .map(|l| format!(" — {l}"))
                .unwrap_or_default()
        );
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
            idle_probes: 10,
            idle_interval: Duration::from_millis(100),
            interval: Duration::from_millis(args.latency_interval_ms),
            timeout: Duration::from_secs(3),
        }),
    };

    if progress && cfg.latency.is_some() {
        eprint!("量測閒置延遲…");
        let _ = std::io::stderr().flush();
    }

    let backend: Arc<dyn Backend> = Arc::from(backend);
    let outcome = engine::run(backend.clone(), &cfg, |s| {
        if progress {
            let tag = if s.warmup { " (暖機)" } else { "" };
            let rtt = s
                .latest_rtt_ms
                .map(|r| format!("  {r:7.1} ms"))
                .unwrap_or_default();
            eprint!(
                "\r\x1b[2K{:5.1}s  {:9.1} Mbps  {:2} 條連線{rtt}{tag}",
                s.t_secs, s.mbps, s.connections
            );
            let _ = std::io::stderr().flush();
        }
    })
    .await?;
    if progress {
        eprintln!();
    }

    let report = report::Report::new(backend.name(), server, &cfg, &outcome);
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        report.print_human();
    }
    Ok(())
}
