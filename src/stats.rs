//! 統計工具。用中位數與百分位數，而不是平均值，以降低突波的影響。

use serde::Serialize;

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
}
