use std::{collections::BTreeMap, sync::Arc};
use parking_lot::Mutex;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows};
use rtdb_types::Result;
use crate::{MetricRegistration, Registry, overflow, step::ObservedCounter, wall_time_millis};

/// Reads the current process-wide totals, one per series, in registration order.
pub type ObservedTotals = Arc<dyn Fn() -> Vec<u64> + Send + Sync>;

/// One counter series published from a total its owner accumulates elsewhere.
#[derive(Clone, Debug)]
pub struct ObservedSeries { pub name: &'static str, pub tags: BTreeMap<String, String> }

struct ObservedMetrics { series: Vec<ObservedSeries>, totals: ObservedTotals, counters: Mutex<Vec<ObservedCounter>>, memory: Arc<MemoryBudget>, _reservation: Arc<MemoryReservation> }
impl Registry {
    /// Publishes counters over totals that `totals` reads, such as process-wide atomics owned by another crate.
    /// `totals` must return one value per series, in the order of `series`, and must not block.
    pub fn register_observed_counters(&self, series: Vec<ObservedSeries>, totals: ObservedTotals) -> Result<MetricRegistration> {
        let bytes = series.iter().flat_map(|series| series.tags.iter().map(|(key, value)| key.len().saturating_add(value.len())).chain([series.name.len().saturating_add(64)]))
            .try_fold(512usize, |bytes, more| bytes.checked_add(more)).ok_or_else(overflow)?;
        let reservation = self.memory.reserve(bytes)?;
        let now = wall_time_millis();
        let counters = Mutex::new(vec![ObservedCounter::new(now); series.len()]);
        self.register_provider(Arc::new(ObservedMetrics { series, totals, counters, memory: Arc::clone(&self.memory), _reservation: reservation }))
    }
}
impl ObservedMetrics {
    fn observe(&self, counters: &mut [ObservedCounter]) {
        for (counter, total) in counters.iter_mut().zip((self.totals)()) { counter.observe(total); }
    }
    fn rows(&self, step: bool) -> Result<CatalogRows<CatalogMetric>> {
        let reservation = self.memory.reserve(self.series.len().checked_mul(512).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?)?;
        let now = wall_time_millis();
        let mut counters = self.counters.lock();
        self.observe(&mut counters);
        let rows = self.series.iter().zip(counters.iter_mut())
            .map(|(series, counter)| CatalogMetric::Counter { name: series.name.into(), tags: series.tags.clone(), count: counter.value(now, step) })
            .collect();
        Ok(CatalogRows::new(rows, reservation))
    }
}
#[async_trait::async_trait]
impl CatalogMetrics for ObservedMetrics {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { self.rows(false) }
    fn roll_step(&self, now_ms: i64, closing: bool) {
        let mut counters = self.counters.lock();
        self.observe(&mut counters);
        for counter in counters.iter_mut() { counter.poll(now_ms, closing); }
    }
    async fn sample_step(&self) -> Result<CatalogRows<CatalogMetric>> { self.rows(true) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[tokio::test]
    async fn observed_counters_publish_the_owner_totals_and_unregister_on_drop() {
        let memory = MemoryBudget::new(1 << 22);
        let registry = Registry::new("node", memory).unwrap();
        let total = Arc::new(AtomicU64::new(2));
        let read = Arc::clone(&total);
        let registration = registry.register_observed_counters(
            vec![ObservedSeries { name: "rtdb.test.observed", tags: BTreeMap::from([("code".into(), "a".into())]) }],
            Arc::new(move || vec![read.load(Ordering::Relaxed)]),
        ).unwrap();
        total.store(5, Ordering::Relaxed);
        let rows = registry.sample().await.unwrap();
        assert!(rows.rows().iter().any(|metric| matches!(metric, CatalogMetric::Counter { name, tags, count } if name == "rtdb.test.observed" && tags["code"] == "a" && *count == 5.0)));
        drop(registration);
        assert!(!registry.metrics().unwrap().rows().iter().any(|metric| metric.name() == "rtdb.test.observed"));
    }
}
