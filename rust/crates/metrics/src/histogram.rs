use std::time::Duration;

const WINDOWS: usize = 3;
const PERCENTILE_ROTATION_MS: i64 = 20_000;
const MAX_ROTATION_MS: i64 = 60_000;
const QUANTILES: [f64; 6] = [75.0, 85.0, 95.0, 98.0, 99.0, 99.9];

// HdrHistogram's precision-one double layout: https://github.com/HdrHistogram/HdrHistogram/blob/HdrHistogram-2.2.2/src/main/java/org/HdrHistogram/DoubleHistogram.java
struct DoubleHistogram<const BINS: usize, const MIN: i32> { bins: Box<[i64; BINS]>, count: i64, low_exponent: i32, range_exponent: i32, minimum: Option<usize>, maximum: usize }
impl<const BINS: usize, const MIN: i32> DoubleHistogram<BINS, MIN> {
    fn new() -> Self {
        let bins = vec![0; BINS].into_boxed_slice().try_into().expect("the allocation has exactly BINS counters");
        Self { bins, count: 0, low_exponent: 800, range_exponent: 3, minimum: None, maximum: 0 }
    }
    fn empty_like(&self) -> Self { Self { range_exponent: self.range_exponent, ..Self::new() } }
    fn reset(&mut self) { self.bins.fill(0); self.count = 0; self.low_exponent = 800; self.minimum = None; self.maximum = 0; }
    fn nonzero_bounds(&self) -> Option<(i32, i32)> {
        let first = self.minimum?;
        let last = self.maximum;
        Some((MIN + ((first - 1) / 16) as i32, MIN + ((last - 1) / 16) as i32))
    }
    fn cover(&mut self, exponent: i32) -> bool {
        let Some((minimum, maximum)) = self.nonzero_bounds() else { self.low_exponent = exponent; return true; };
        while exponent < self.low_exponent {
            let shift = (self.low_exponent - exponent).min(self.range_exponent - 2).min(50);
            if maximum >= self.low_exponent - shift + self.range_exponent {
                if self.range_exponent + shift > 59 { return false; }
                self.range_exponent += shift;
            }
            self.low_exponent -= shift;
        }
        while exponent >= self.low_exponent + self.range_exponent {
            let shift = (exponent - self.low_exponent - self.range_exponent + 1).min(self.range_exponent - 2).min(50);
            if minimum >= self.low_exponent + shift { self.low_exponent += shift; }
            else {
                if self.range_exponent + shift > 59 { return false; }
                self.range_exponent += shift;
            }
        }
        true
    }
    fn record(&mut self, value: f64, count: i64) -> bool {
        let index = if value == 0.0 { 0 } else {
            let bits = value.to_bits();
            let exponent = ((bits >> 52) & 0x7ff) as i32 - 1023;
            if exponent < MIN || exponent >= MIN + ((BINS - 1) / 16) as i32 || !self.cover(exponent) { return false; }
            1 + (exponent - MIN) as usize * 16 + ((bits >> 48) & 15) as usize
        };
        self.bins[index] = self.bins[index].wrapping_add(count);
        self.count = self.count.wrapping_add(count);
        if index != 0 { self.minimum = Some(self.minimum.map_or(index, |minimum| minimum.min(index))); self.maximum = self.maximum.max(index); }
        true
    }
    fn add(&mut self, other: &Self) {
        for (index, count) in other.bins.iter().copied().enumerate() {
            if count != 0 && !self.record(lower_value(index, MIN), count) { break; }
        }
    }
    fn percentile(&self, percentile: f64) -> f64 {
        let below = f64::from_bits(percentile.to_bits() - 1);
        let rank = ((below * self.count as f64 / 100.0).ceil() as i64).max(1);
        let mut count = 0i64;
        for (index, entries) in self.bins.iter().copied().enumerate() {
            count = count.wrapping_add(entries);
            if count >= rank {
                if index == 0 { return 0.0; }
                let exponent = MIN + ((index - 1) / 16) as i32;
                return lower_value(index, MIN) + 2f64.powi(exponent - 4) - 2f64.powi(self.low_exponent - 4);
            }
        }
        0.0
    }
}
fn lower_value(index: usize, minimum: i32) -> f64 {
    if index == 0 { 0.0 } else { (16 + (index - 1) % 16) as f64 * 2f64.powi(minimum + ((index - 1) / 16) as i32 - 4) }
}

struct Recorder<const BINS: usize, const MIN: i32> { active: DoubleHistogram<BINS, MIN>, inactive: Option<DoubleHistogram<BINS, MIN>> }
impl<const BINS: usize, const MIN: i32> Recorder<BINS, MIN> {
    fn new() -> Self { Self { active: DoubleHistogram::new(), inactive: None } }
    fn sample(&mut self) -> &DoubleHistogram<BINS, MIN> {
        let inactive = self.inactive.get_or_insert_with(|| self.active.empty_like());
        inactive.reset();
        std::mem::swap(&mut self.active, inactive);
        inactive
    }
    fn reset(&mut self) { self.sample(); self.sample(); }
    fn drain_into(&mut self, target: &mut DoubleHistogram<BINS, MIN>) { target.reset(); target.add(self.sample()); }
}

struct Percentiles<const BINS: usize, const MIN: i32> {
    recorders: [Recorder<BINS, MIN>; WINDOWS], current: usize, last_rotation: i64,
    interval: DoubleHistogram<BINS, MIN>, accumulated: DoubleHistogram<BINS, MIN>, stale: bool,
}
impl<const BINS: usize, const MIN: i32> Percentiles<BINS, MIN> {
    const BIN_BYTES: usize = (2 * WINDOWS + 2) * std::mem::size_of::<[i64; BINS]>();

    fn new(now_ms: i64) -> Self { Self { recorders: std::array::from_fn(|_| Recorder::new()), current: 0, last_rotation: now_ms,
        interval: DoubleHistogram::new(), accumulated: DoubleHistogram::new(), stale: false } }
    fn rotate(&mut self, now_ms: i64) {
        let elapsed = now_ms.saturating_sub(self.last_rotation);
        if elapsed < PERCENTILE_ROTATION_MS { return; }
        let rotations = (elapsed / PERCENTILE_ROTATION_MS).min(WINDOWS as i64);
        for _ in 0..rotations {
            self.recorders[self.current].reset();
            self.current = (self.current + 1) % WINDOWS;
            self.last_rotation += PERCENTILE_ROTATION_MS;
        }
        self.accumulated.reset(); self.stale = true;
    }
    fn record(&mut self, value: f64, now_ms: i64) {
        self.rotate(now_ms);
        for recorder in &mut self.recorders { if !recorder.active.record(value, 1) { break; } }
        self.stale = true;
    }
    fn snapshot<const N: usize>(&mut self, now_ms: i64, quantiles: [f64; N]) -> [f64; N] {
        self.rotate(now_ms);
        if self.stale {
            self.recorders[self.current].drain_into(&mut self.interval);
            self.accumulated.add(&self.interval);
            self.stale = false;
        }
        quantiles.map(|percentile| self.accumulated.percentile(percentile))
    }
}

struct Maximum { values: [i64; WINDOWS], current: usize, last_rotation: i64 }
impl Maximum {
    fn new(now_ms: i64) -> Self { Self { values: [0; WINDOWS], current: 0, last_rotation: now_ms } }
    fn rotate(&mut self, now_ms: i64) {
        let elapsed = now_ms.saturating_sub(self.last_rotation);
        if elapsed < MAX_ROTATION_MS { return; }
        if elapsed >= MAX_ROTATION_MS * WINDOWS as i64 {
            self.values.fill(0); self.current = 0;
            self.last_rotation = now_ms - elapsed % MAX_ROTATION_MS;
            return;
        }
        for _ in 0..elapsed / MAX_ROTATION_MS {
            self.values[self.current] = 0;
            self.current = (self.current + 1) % WINDOWS;
            self.last_rotation += MAX_ROTATION_MS;
        }
    }
    fn record(&mut self, nanos: i64, now_ms: i64) { self.rotate(now_ms); for maximum in &mut self.values { *maximum = (*maximum).max(nanos); } }
    fn snapshot(&mut self, now_ms: i64) -> i64 { self.rotate(now_ms); self.values[self.current] }
}

pub(crate) struct Timer { count: i64, total: i64, percentiles: Percentiles<1025, 0>, maximum: Maximum, step: crate::step::Pair<i64> }
pub(crate) struct Snapshot { pub count: i64, pub total: f64, pub mean: i64, pub maximum: i64, pub percentiles: [f64; 6] }
impl Timer {
    pub const RETAINED_BYTES: usize = std::mem::size_of::<Self>() + Percentiles::<1025, 0>::BIN_BYTES;

    pub fn new(now_ms: i64) -> Self { Self { count: 0, total: 0, percentiles: Percentiles::new(now_ms), maximum: Maximum::new(now_ms), step: crate::step::Pair::new(now_ms) } }
    pub fn record(&mut self, elapsed: Duration, now_ms: i64) {
        let nanos = elapsed.as_nanos().min(i64::MAX as u128) as i64;
        self.percentiles.record(nanos as f64, now_ms);
        self.count = self.count.wrapping_add(1); self.total = self.total.wrapping_add(nanos);
        self.step.add(nanos);
        self.maximum.record((nanos as f64) as i64, now_ms);
    }
    pub fn snapshot(&mut self, now_ms: i64) -> Snapshot {
        Snapshot { count: self.count, total: self.total as f64, mean: if self.count == 0 { 0 } else { (self.total as f64 / self.count as f64) as i64 },
            maximum: self.maximum.snapshot(now_ms), percentiles: self.percentiles.snapshot(now_ms, QUANTILES) }
    }
    pub fn roll_step(&mut self, now_ms: i64, closing: bool) { self.step.poll(now_ms, closing); }
    pub fn step_snapshot(&mut self, now_ms: i64) -> Snapshot {
        let mut result = self.snapshot(now_ms); let (count, total) = self.step.value(now_ms);
        result.count = count; result.total = total as f64; result.mean = if count == 0 { 0 } else { (total as f64 / count as f64) as i64 }; result
    }
}

pub(crate) struct CommitLag { count: i64, total: f64, percentiles: Percentiles<1345, -20>, maximum: Maximum, step: crate::step::Pair<f64> }
pub(crate) struct SummarySnapshot { pub count: i64, pub total: f64, pub maximum: f64, pub percentiles: [f64; 3] }
impl CommitLag {
    pub const RETAINED_BYTES: usize = std::mem::size_of::<Self>() + Percentiles::<1345, -20>::BIN_BYTES;

    pub fn new(now_ms: i64) -> Self { Self { count: 0, total: 0.0, percentiles: Percentiles::new(now_ms), maximum: Maximum::new(now_ms), step: crate::step::Pair::new(now_ms) } }
    pub fn record(&mut self, seconds: f64, now_ms: i64) {
        if !(seconds >= 0.0) { return; }
        self.percentiles.record(seconds, now_ms);
        self.count = self.count.wrapping_add(1); self.total += seconds;
        self.step.add(seconds);
        self.maximum.record(seconds.to_bits() as i64, now_ms);
    }
    pub fn snapshot(&mut self, now_ms: i64) -> SummarySnapshot {
        SummarySnapshot { count: self.count, total: self.total, maximum: f64::from_bits(self.maximum.snapshot(now_ms) as u64), percentiles: self.percentiles.snapshot(now_ms, [50.0, 95.0, 99.0]) }
    }
    pub fn roll_step(&mut self, now_ms: i64, closing: bool) { self.step.poll(now_ms, closing); }
    pub fn step_snapshot(&mut self, now_ms: i64) -> SummarySnapshot {
        let mut result = self.snapshot(now_ms); let (count, total) = self.step.value(now_ms); result.count = count; result.total = total; result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hdr_precision_one_retains_fractional_upper_value_and_does_not_clamp_to_observation() {
        let mut timer = Timer::new(0);
        timer.record(Duration::from_nanos(1_000), 0);
        assert_eq!(timer.snapshot(0).percentiles, [992.0; 6]);
        timer.record(Duration::from_nanos(1), 0);
        let snapshot = timer.snapshot(0);
        assert_eq!(snapshot.percentiles, [1_023.9375; 6]);
        assert_eq!(snapshot.maximum, 1_000);
        assert_eq!(snapshot.total, 1_001.0);
        assert_eq!(snapshot.mean, 500);
    }
    #[test]
    fn percentile_expiry_and_maximum_expiry_have_distinct_rotations() {
        let mut timer = Timer::new(0);
        timer.record(Duration::from_nanos(25), 19_999);
        assert_eq!(timer.snapshot(39_999).percentiles, [25.0; 6]);
        assert_eq!(timer.snapshot(59_999).percentiles, [25.0; 6]);
        let expired = timer.snapshot(60_000);
        assert_eq!(expired.percentiles, [0.0; 6]); assert_eq!(expired.maximum, 25);
        assert_eq!(timer.snapshot(179_999).maximum, 25);
        let expired = timer.snapshot(180_000);
        assert_eq!(expired.maximum, 0); assert_eq!(expired.count, 1); assert_eq!(expired.total, 25.0);
    }
    #[test]
    fn repeated_snapshots_do_not_duplicate_samples_and_percentile_rank_rounding_matches_hdr() {
        let mut timer = Timer::new(0);
        for nanos in 1..=100 { timer.record(Duration::from_nanos(nanos), 0); }
        let first = timer.snapshot(0);
        assert_eq!(first.percentiles, [75.9375, 87.9375, 95.9375, 99.9375, 99.9375, 103.9375]);
        assert_eq!(timer.snapshot(0).percentiles, first.percentiles);
        assert_eq!(first.count, 100); assert_eq!(first.total, 5_050.0);
    }
    #[test]
    fn long_idle_histogram_catches_up_three_slots_per_call_while_maximum_aligns_to_clock() {
        let mut timer = Timer::new(0);
        timer.record(Duration::from_nanos(25), 600_000);
        let snapshot = timer.snapshot(600_000);
        assert_eq!(snapshot.percentiles, [0.0; 6]); assert_eq!(snapshot.maximum, 25);
        assert_eq!(snapshot.count, 1);
    }
    #[test]
    fn backwards_wall_clock_keeps_current_windows_and_zero_duration_counts() {
        let mut timer = Timer::new(20_000);
        timer.record(Duration::ZERO, 10_000);
        timer.record(Duration::from_nanos(1), 10_000);
        let snapshot = timer.snapshot(0);
        assert_eq!(snapshot.percentiles, [1.0; 6]); assert_eq!(snapshot.count, 2);
        assert_eq!(snapshot.mean, 0); assert_eq!(snapshot.total, 1.0);
    }
    #[test]
    fn histogram_range_rejection_preserves_timer_count_sum_and_maximum() {
        let mut timer = Timer::new(0);
        timer.record(Duration::from_nanos(1), 0);
        timer.record(Duration::from_nanos(1 << 62), 0);
        let snapshot = timer.snapshot(0);
        assert_eq!(snapshot.count, 2); assert_eq!(snapshot.total, ((1i64 << 62) + 1) as f64);
        assert_eq!(snapshot.maximum, 1 << 62); assert_eq!(snapshot.percentiles, [1.0; 6]);
    }
    #[test]
    fn timer_total_uses_signed_long_accumulation_and_maximum_uses_double_conversion() {
        let mut timer = Timer::new(0);
        timer.record(Duration::from_nanos((1 << 53) + 1), 0);
        assert_eq!(timer.snapshot(0).maximum, 1 << 53);
        let mut timer = Timer::new(0);
        timer.record(Duration::from_nanos(i64::MAX as u64), 0);
        timer.record(Duration::from_nanos(1), 0);
        assert_eq!(timer.snapshot(0).total, i64::MIN as f64);
    }
    #[test]
    fn commit_lag_summary_counts_every_observation_and_rejects_negative_lag() {
        let mut summary = CommitLag::new(0);
        for value in [0.0, 0.5, 1.0, 2.0, -1.0] { summary.record(value, 0); }
        let snapshot = summary.snapshot(0);
        assert_eq!(snapshot.count, 4); assert_eq!(snapshot.total, 3.5); assert_eq!(snapshot.maximum, 2.0);
        assert_eq!(snapshot.percentiles, [0.5, 2.09375, 2.09375]);
        let expired = summary.snapshot(180_000);
        assert_eq!(expired.percentiles, [0.0; 3]); assert_eq!(expired.maximum, 0.0);
        assert_eq!(expired.count, 4); assert_eq!(expired.total, 3.5);
    }
}
