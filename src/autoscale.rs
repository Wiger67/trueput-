//! 動態調整平行連線數。
//!
//! 單一條 TCP 連線常被視窗大小、伺服器端限速或路徑上的設備卡住，跑不滿頻寬；
//! 但連線開太多又會增加開銷，在慢速網路上也沒必要。所以採用「試探」的方式：
//!
//! 1. 從 `start` 條連線開始。
//! 2. 每經過一個量測窗格（預設 1 秒）看一次速度：
//!    - 第一個窗格：還沒有比較基準，直接把連線數加倍試試看。
//!    - 之後：如果速度比上一個窗格高出 `growth_threshold`（預設 10%）以上，
//!      代表加連線有幫助，繼續加倍；否則鎖定目前的連線數。
//! 3. 到達 `max` 也會鎖定。
//!
//! 鎖定之後才開始收正式的統計樣本（見 [`crate::engine`]），
//! 這樣新連線慢啟動造成的低估不會混進結果。
//!
//! 評估窗格的長度會依閒置延遲調整（[`window_for_rtt`]）：TCP 慢啟動要花
//! 好幾個 RTT 才能把速度拉起來，高延遲路徑上 1 秒可能還看不出加連線的效果。

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct AutoConfig {
    /// 初始連線數
    pub start: usize,
    /// 連線數上限
    pub max: usize,
    /// 每隔多久評估一次
    pub window: Duration,
    /// 速度要成長多少比例才算「加連線有幫助」，0.1 = 10%
    pub growth_threshold: f64,
}

impl Default for AutoConfig {
    fn default() -> Self {
        Self {
            start: 2,
            max: 16,
            window: Duration::from_secs(1),
            growth_threshold: 0.10,
        }
    }
}

/// 評估窗格至少要涵蓋幾個 RTT
const WINDOW_RTTS: u32 = 10;
/// 評估窗格的上限，避免測試時間都花在調整上
const MAX_WINDOW: Duration = Duration::from_secs(3);

/// 依閒置 RTT 決定評估窗格：`max(base, 10 × RTT)`，上限 3 秒。
///
/// 例如 1 Gbps、RTT 200ms 的路徑，BDP 約 25 MB；慢啟動從 ~15 KB 加倍到 25 MB
/// 約需 11 個 RTT（2.2 秒），固定 1 秒的窗格會在新連線還沒跑起來前就判定「沒幫助」。
pub fn window_for_rtt(base: Duration, idle_rtt: Option<Duration>) -> Duration {
    match idle_rtt {
        Some(rtt) => (rtt * WINDOW_RTTS).clamp(base, MAX_WINDOW.max(base)),
        None => base,
    }
}

#[derive(Debug)]
pub struct Scaler {
    max: usize,
    growth_threshold: f64,
    prev_rate: Option<f64>,
    locked: bool,
}

impl Scaler {
    /// 固定連線數：一開始就鎖定。
    pub fn fixed() -> Self {
        Self {
            max: 0,
            growth_threshold: 0.0,
            prev_rate: None,
            locked: true,
        }
    }

    /// 自動模式。`max` 應已依後端限制調整過；`start >= max` 時直接鎖定。
    pub fn auto(start: usize, max: usize, growth_threshold: f64) -> Self {
        Self {
            max,
            growth_threshold,
            prev_rate: None,
            locked: start >= max,
        }
    }

    pub fn locked(&self) -> bool {
        self.locked
    }

    /// 強制鎖定（例如測試時間已經過半）。
    pub fn lock(&mut self) {
        self.locked = true;
    }

    /// 每個窗格結束時呼叫一次，傳入該窗格的速度與目前連線數。
    /// 回傳 `Some(n)` 表示應該把連線數增加到 `n`。
    pub fn observe(&mut self, rate_mbps: f64, current: usize) -> Option<usize> {
        if self.locked {
            return None;
        }
        if let Some(prev) = self.prev_rate
            && rate_mbps < prev * (1.0 + self.growth_threshold)
        {
            self.locked = true;
            return None;
        }
        self.prev_rate = Some(rate_mbps);
        if current >= self.max {
            self.locked = true;
            return None;
        }
        Some((current * 2).min(self.max))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_is_locked() {
        let mut s = Scaler::fixed();
        assert!(s.locked());
        assert_eq!(s.observe(100.0, 4), None);
    }

    #[test]
    fn doubles_while_growing_then_locks() {
        let mut s = Scaler::auto(2, 16, 0.1);
        assert_eq!(s.observe(200.0, 2), Some(4)); // 第一個窗格：直接試
        assert_eq!(s.observe(400.0, 4), Some(8)); // +100%
        assert_eq!(s.observe(450.0, 8), Some(16)); // +12.5%
        assert_eq!(s.observe(455.0, 16), None); // +1%：鎖定
        assert!(s.locked());
        assert_eq!(s.observe(900.0, 16), None); // 鎖定後不再變動
    }

    #[test]
    fn locks_when_adding_does_not_help() {
        let mut s = Scaler::auto(2, 16, 0.1);
        assert_eq!(s.observe(300.0, 2), Some(4));
        assert_eq!(s.observe(305.0, 4), None);
        assert!(s.locked());
    }

    #[test]
    fn caps_at_max() {
        let mut s = Scaler::auto(2, 6, 0.1);
        assert_eq!(s.observe(100.0, 2), Some(4));
        assert_eq!(s.observe(200.0, 4), Some(6));
        assert_eq!(s.observe(300.0, 6), None);
        assert!(s.locked());
    }

    #[test]
    fn window_scales_with_rtt() {
        let base = Duration::from_secs(1);
        assert_eq!(window_for_rtt(base, None), base);
        // 低延遲：維持基本值
        assert_eq!(window_for_rtt(base, Some(Duration::from_millis(15))), base);
        // 200ms → 2 秒
        assert_eq!(
            window_for_rtt(base, Some(Duration::from_millis(200))),
            Duration::from_secs(2)
        );
        // 很高的延遲：上限 3 秒
        assert_eq!(
            window_for_rtt(base, Some(Duration::from_millis(800))),
            Duration::from_secs(3)
        );
    }

    #[test]
    fn start_at_or_above_max_is_locked() {
        assert!(Scaler::auto(2, 1, 0.1).locked());
        assert!(Scaler::auto(4, 4, 0.1).locked());
    }
}
