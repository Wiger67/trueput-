//! 統計工具。用中位數與百分位數，而不是平均值，以降低突波的影響。

use serde::Serialize;

use crate::latency::{Phase, Probe};

#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    /// 暖機後的樣本中位數（主要結果）
    pub median_mbps: f64,
    pub p10_mbps: f64,
    pub p90_mbps: f64,
    pub max_mbps: f64,
    /// 暖機後的總位元組數 ÷ 暖機後經過的時間
    pub steady_avg_mbps: f64,
    pub samples: usize,
}

impl Summary {
    /// `samples` 為暖機後的每段速度（Mbps）。沒有樣本時回傳 `None`。
    pub fn from_samples(samples: &[f64], steady_avg_mbps: f64) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        let mut sorted = samples.to_vec();
        sorted.sort_by(f64::total_cmp);
        Some(Self {
            median_mbps: percentile(&sorted, 50.0),
            p10_mbps: percentile(&sorted, 10.0),
            p90_mbps: percentile(&sorted, 90.0),
            max_mbps: *sorted.last().unwrap(),
            steady_avg_mbps,
            samples: sorted.len(),
        })
    }
}

/// 對已排序的資料取百分位數（線性內插）。`p` 介於 0～100。
pub fn percentile(sorted: &[f64], p: f64) -> f64 {
    assert!(!sorted.is_empty());
    let rank = (p / 100.0).clamp(0.0, 1.0) * (sorted.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    let frac = rank - lo as f64;
    sorted[lo] + (sorted[hi] - sorted[lo]) * frac
}

/// 位元組數與秒數換算成 Mbps（十進位，1 Mbps = 1,000,000 bit/s）。
pub fn mbps(bytes: u64, secs: f64) -> f64 {
    if secs <= 0.0 {
        return 0.0;
    }
    bytes as f64 * 8.0 / secs / 1_000_000.0
}

/// 一組延遲量測的摘要。
#[derive(Debug, Clone, Serialize)]
pub struct RttStats {
    pub median_ms: f64,
    pub min_ms: f64,
    pub p90_ms: f64,
    /// 相鄰兩次量測差值的平均（簡化版的 RFC 3550 抖動）
    pub jitter_ms: f64,
    /// 成功的量測次數
    pub count: usize,
    /// 逾時或失敗的次數
    pub lost: usize,
}

impl RttStats {
    /// `rtts` 依時間順序排列；`None` 代表遺失。全部遺失時回傳 `None`。
    pub fn from_rtts(rtts: &[Option<f64>]) -> Option<Self> {
        let ok: Vec<f64> = rtts.iter().flatten().copied().collect();
        if ok.is_empty() {
            return None;
        }
        let jitter_ms = if ok.len() < 2 {
            0.0
        } else {
            ok.windows(2).map(|w| (w[1] - w[0]).abs()).sum::<f64>() / (ok.len() - 1) as f64
        };
        let mut sorted = ok.clone();
        sorted.sort_by(f64::total_cmp);
        Some(Self {
            median_ms: percentile(&sorted, 50.0),
            min_ms: sorted[0],
            p90_ms: percentile(&sorted, 90.0),
            jitter_ms,
            count: ok.len(),
            lost: rtts.len() - ok.len(),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencySummary {
    pub idle: Option<RttStats>,
    /// 下載中、暖機後的延遲
    pub loaded: Option<RttStats>,
    /// 下載中延遲中位數 − 閒置延遲中位數
    pub increase_ms: Option<f64>,
    pub bufferbloat_grade: Option<&'static str>,
}

impl LatencySummary {
    pub fn from_probes(probes: &[Probe]) -> Self {
        let rtts = |phase| -> Vec<Option<f64>> {
            probes
                .iter()
                .filter(|p| p.phase == phase)
                .map(|p| p.rtt_ms)
                .collect()
        };
        let idle = RttStats::from_rtts(&rtts(Phase::Idle));
        let loaded = RttStats::from_rtts(&rtts(Phase::Loaded));
        let increase_ms = match (&idle, &loaded) {
            (Some(i), Some(l)) => Some((l.median_ms - i.median_ms).max(0.0)),
            _ => None,
        };
        Self {
            idle,
            loaded,
            increase_ms,
            bufferbloat_grade: increase_ms.map(bufferbloat_grade),
        }
    }
}

/// 依「負載下延遲增加量」給等級。
///
/// 門檻參考常見的 bufferbloat 分級：增加 5 ms 以內幾乎無感，
/// 超過 200 ms 時視訊通話與遊戲會明顯受影響。
pub fn bufferbloat_grade(increase_ms: f64) -> &'static str {
    match increase_ms {
        x if x < 5.0 => "A+",
        x if x < 30.0 => "A",
        x if x < 60.0 => "B",
        x if x < 200.0 => "C",
        x if x < 400.0 => "D",
        _ => "F",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_interpolates() {
        let data = [10.0, 20.0, 30.0, 40.0];
        assert_eq!(percentile(&data, 0.0), 10.0);
        assert_eq!(percentile(&data, 100.0), 40.0);
        assert_eq!(percentile(&data, 50.0), 25.0);
    }

    #[test]
    fn percentile_single_value() {
        assert_eq!(percentile(&[42.0], 90.0), 42.0);
    }

    #[test]
    fn mbps_conversion() {
        // 12.5 MB 在 1 秒內 = 100 Mbps
        assert_eq!(mbps(12_500_000, 1.0), 100.0);
        assert_eq!(mbps(100, 0.0), 0.0);
    }

    #[test]
    fn summary_ignores_order() {
        let s = Summary::from_samples(&[300.0, 100.0, 200.0], 200.0).unwrap();
        assert_eq!(s.median_mbps, 200.0);
        assert_eq!(s.max_mbps, 300.0);
        assert_eq!(s.samples, 3);
        assert!(Summary::from_samples(&[], 0.0).is_none());
    }

    #[test]
    fn rtt_stats_with_loss_and_jitter() {
        let s = RttStats::from_rtts(&[Some(10.0), None, Some(14.0), Some(12.0)]).unwrap();
        assert_eq!(s.count, 3);
        assert_eq!(s.lost, 1);
        assert_eq!(s.median_ms, 12.0);
        assert_eq!(s.min_ms, 10.0);
        // |14-10| + |12-14| = 6，除以 2
        assert_eq!(s.jitter_ms, 3.0);
        assert!(RttStats::from_rtts(&[None, None]).is_none());
    }

    #[test]
    fn latency_summary_grades_increase() {
        let probe = |rtt, phase| Probe {
            t_secs: 0.0,
            rtt_ms: Some(rtt),
            phase,
        };
        let probes = [
            probe(10.0, Phase::Idle),
            probe(10.0, Phase::Idle),
            probe(500.0, Phase::Warmup), // 暖機期不列入
            probe(50.0, Phase::Loaded),
            probe(50.0, Phase::Loaded),
        ];
        let s = LatencySummary::from_probes(&probes);
        assert_eq!(s.increase_ms, Some(40.0));
        assert_eq!(s.bufferbloat_grade, Some("B"));
    }

    #[test]
    fn grade_thresholds() {
        assert_eq!(bufferbloat_grade(0.0), "A+");
        assert_eq!(bufferbloat_grade(29.9), "A");
        assert_eq!(bufferbloat_grade(199.0), "C");
        assert_eq!(bufferbloat_grade(1000.0), "F");
    }
}
