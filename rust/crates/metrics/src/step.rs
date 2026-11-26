pub(crate) const MILLIS: i64 = 60_000;

#[derive(Clone, Copy)]
pub(crate) struct Counter { total: f64, current: f64, previous: f64, position: i64 }
impl Counter {
    pub fn new(now: i64) -> Self { Self { total: 0.0, current: 0.0, previous: 0.0, position: now / MILLIS } }
    pub fn add(&mut self, amount: u64) { self.total += amount as f64; self.current += amount as f64; }
    pub fn poll(&mut self, now: i64, closing: bool) {
        if self.position == i64::MAX { return; }
        let step = now / MILLIS;
        if closing || step > self.position {
            let current = std::mem::take(&mut self.current);
            self.previous = if closing || self.position == step - 1 { current } else { 0.0 };
            self.position = if closing { i64::MAX } else { step };
        }
    }
    pub fn value(&mut self, now: i64, step: bool) -> f64 { if step { self.poll(now, false); self.previous } else { self.total } }
}

/// A step counter over a total its owner accumulates elsewhere: each observation adds what the total grew by since the last.
#[derive(Clone, Copy)]
pub(crate) struct ObservedCounter { observed: u64, counter: Counter }
impl ObservedCounter {
    pub fn new(now: i64) -> Self { Self { observed: 0, counter: Counter::new(now) } }
    pub fn observe(&mut self, total: u64) {
        self.counter.add(total.saturating_sub(self.observed));
        self.observed = self.observed.max(total);
    }
    pub fn poll(&mut self, now: i64, closing: bool) { self.counter.poll(now, closing); }
    pub fn value(&mut self, now: i64, step: bool) -> f64 { self.counter.value(now, step) }
}

pub(crate) trait Number: Copy + Default { fn add(self, other: Self) -> Self; }
impl Number for i64 { fn add(self, other: Self) -> Self { self.wrapping_add(other) } }
impl Number for f64 { fn add(self, other: Self) -> Self { self + other } }
pub(crate) struct Pair<T> { current: (i64, T), previous: (i64, T), position: i64 }
impl<T: Number> Pair<T> {
    pub fn new(now: i64) -> Self { Self { current: (0, T::default()), previous: (0, T::default()), position: now / MILLIS } }
    pub fn add(&mut self, value: T) { self.current.0 = self.current.0.wrapping_add(1); self.current.1 = self.current.1.add(value); }
    pub fn poll(&mut self, now: i64, closing: bool) {
        if self.position == i64::MAX { return; }
        let step = now / MILLIS;
        if closing || step > self.position {
            self.previous = if closing || self.position == step - 1 { std::mem::replace(&mut self.current, (0, T::default())) } else { (0, T::default()) };
            self.position = if closing { i64::MAX } else { step };
        }
    }
    pub fn value(&mut self, now: i64) -> (i64, T) { self.poll(now, false); self.previous }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn step_value_resets_on_missed_poll_while_step_tuple_retains_its_supplier_values() {
        let mut counter = Counter::new(0); let mut pair = Pair::<i64>::new(0);
        counter.add(3); pair.add(7);
        assert_eq!(counter.value(1, true), 0.0); assert_eq!(pair.value(1), (0, 0));
        assert_eq!(counter.value(120_001, true), 0.0); assert_eq!(pair.value(120_001), (0, 0));
        counter.add(2); pair.add(11);
        assert_eq!(counter.value(180_001, true), 2.0); assert_eq!(pair.value(180_001), (2, 18));
        assert_eq!(counter.value(180_002, false), 5.0);
    }
    #[test]
    fn an_observed_counter_steps_by_what_its_total_grew() {
        let mut counter = ObservedCounter::new(0);
        counter.observe(4);
        assert_eq!(counter.value(1, false), 4.0);
        counter.poll(60_001, false);
        counter.observe(4);
        counter.observe(7);
        assert_eq!(counter.value(120_001, true), 3.0);
        assert_eq!(counter.value(120_002, false), 7.0);
    }
    #[test]
    fn closing_rollover_exports_current_partial_step_and_backward_time_does_not_roll() {
        let mut counter = Counter::new(60_000); let mut pair = Pair::<f64>::new(60_000);
        counter.add(2); pair.add(0.25);
        assert_eq!(counter.value(0, true), 0.0); assert_eq!(pair.value(0), (0, 0.0));
        counter.poll(61_000, true); pair.poll(61_000, true);
        assert_eq!(counter.value(999_999, true), 2.0); assert_eq!(pair.value(999_999), (1, 0.25));
    }
}
