//! 統計工具。
//!
//! 主要結果是「暖機後的總位元組數 ÷ 暖機後經過的時間」：這就是那段期間實際的傳輸量，
//! 不受單一樣本突波影響，也不受 socket 緩衝區「一陣一陣」送資料的節奏影響
//! （緩衝區在暖機時已經填滿，開始與結束時的緩衝量互相抵銷）。
//! 樣本（每 200ms 取一次的最近 1 秒速度）只用來描述起伏：P10–P90 與中位數。
//! 延遲則用中位數與百分位數，以降低偶發突波的影響。

use serde::Serialize;

use crate::latency::{Phase, Probe};

#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    /// 主要結果：暖機後的總位元組數 ÷ 暖機後經過的時間
    pub mbps: f64,
    /// 以下描述樣本（最近 1 秒速度）的起伏
    pub median_mbps: f64,
    pub p10_mbps: f64,
    pub p90_mbps: f64,
    pub max_mbps: f64,
    pub samples: usize,
}

impl Summary {
    /// `samples` 為暖機後的樣本速度（Mbps），`steady_mbps` 為主要結果。
    /// 沒有樣本時回傳 `None`。
    pub fn from_samples(samples: &[f64], steady_mbps: f64) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        let mut sorted = samples.to_vec();
        sorted.sort_by(f64::total_cmp);
        Some(Self {
            mbps: steady_mbps,
            median_mbps: percentile(&sorted, 50.0),
            p10_mbps: percentile(&sorted, 10.0),
            p90_mbps: percentile(&sorted, 90.0),
            max_mbps: *sorted.last().unwrap(),
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

/// 提前結束判斷：暖機後的累積平均速度已經收斂，就不必再測下去。
///
/// 判斷方式：
/// - 正式量測至少進行 `min_steady`；
/// - 最近 `window` 內每次取樣時的累積平均，最大值與最小值的差距小於 `tolerance`
///   （例如 2%）。
///
/// 用累積平均而不是單一樣本，是因為單一樣本本來就會有起伏（Wi-Fi、排程），
/// 我們要的是「再測下去，最終結果也不會改變多少」。
#[derive(Debug)]
pub struct Stability {
    min_steady: f64,
    window: f64,
    tolerance: f64,
    /// (正式量測開始後的秒數, 當時的累積平均)
    history: std::collections::VecDeque<(f64, f64)>,
}

impl Stability {
    pub fn new(min_steady_secs: f64, window_secs: f64, tolerance: f64) -> Self {
        Self {
            min_steady: min_steady_secs,
            window: window_secs,
            tolerance,
            history: Default::default(),
        }
    }

    /// 每次取樣後呼叫，傳入正式量測經過的秒數與目前的累積平均。回傳是否已穩定。
    pub fn observe(&mut self, steady_secs: f64, running_avg: f64) -> bool {
        self.history.push_back((steady_secs, running_avg));
        while self
            .history
            .front()
            .is_some_and(|&(t, _)| t < steady_secs - self.window)
        {
            self.history.pop_front();
        }
        // 窗格要填滿才判斷
        let covered = self.history.front().map_or(0.0, |&(t, _)| steady_secs - t);
        if steady_secs < self.min_steady || covered < self.window * 0.99 {
            return false;
        }
        let (lo, hi) = self
            .history
            .iter()
            .fold((f64::MAX, f64::MIN), |(lo, hi), &(_, v)| {
                (lo.min(v), hi.max(v))
            });
        hi > 0.0 && (hi - lo) / hi < self.tolerance
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
        let s = Summary::from_samples(&[300.0, 100.0, 200.0], 210.0).unwrap();
        assert_eq!(s.mbps, 210.0);
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

    #[test]
    fn stability_requires_min_time_and_flat_average() {
        let mut s = Stability::new(4.0, 2.0, 0.02);
        // 平均一直固定，但還沒到最短時間
        for i in 1..=19 {
            assert!(!s.observe(i as f64 * 0.2, 100.0), "t = {}", i as f64 * 0.2);
        }
        assert!(s.observe(4.0, 100.0));
    }

    #[test]
    fn stability_rejects_drifting_average() {
        let mut s = Stability::new(1.0, 2.0, 0.02);
        // 每 0.2 秒成長 1%：2 秒內變化約 10%，不算穩定
        let mut v = 100.0;
        for i in 1..=40 {
            v *= 1.01;
            assert!(!s.observe(i as f64 * 0.2, v));
        }
    }
}
