use std::{collections::BTreeMap, sync::{Arc, Weak}};
use parking_lot::Mutex;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows};
use rtdb_storage::{cache::CacheBudget, disk_cache::{DiskCache, DiskCacheStats, DiskCacheTraffic}};
use rtdb_types::Result;
use crate::{MetricRegistration, Registry, step::ObservedCounter, wall_time_millis};

/// Upstream's `RemoteBufferPool` counters, in the order `traffic_totals` lists them.
const TRAFFIC: [&str; 3] = ["disk-cache-misses", "buffer-pool.network.read", "buffer-pool.network.write"];
fn traffic_totals(traffic: DiskCacheTraffic) -> [u64; 3] { [traffic.misses, traffic.network_read_bytes, traffic.network_write_bytes] }

struct DiskCacheMetrics { cache: Weak<DiskCache>, memory: Arc<MemoryBudget>, traffic: Mutex<[ObservedCounter; 3]>, _reservation: Arc<MemoryReservation> }
/// Upstream's `memory-cache-misses`, counted across every pool sharing the node's cache budget.
struct MemoryCacheMetrics { cache: Weak<CacheBudget>, memory: Arc<MemoryBudget>, misses: Mutex<ObservedCounter>, _reservation: Arc<MemoryReservation> }
impl Registry {
    /// The registration observes the cache without extending its lifetime.
    /// Retain the returned guard for as long as the cache's gauges should be published.
    pub fn register_disk_cache(&self, cache: Arc<DiskCache>) -> Result<MetricRegistration> {
        let reservation = self.memory.reserve(512)?;
        let now = wall_time_millis();
        self.register_provider(Arc::new(DiskCacheMetrics { cache: Arc::downgrade(&cache), memory: Arc::clone(&self.memory), traffic: Mutex::new([ObservedCounter::new(now); 3]), _reservation: reservation }))
    }
    /// Publishes `memory-cache-misses` for the pools drawing on `cache`, without extending its lifetime.
    pub fn register_memory_cache(&self, cache: &Arc<CacheBudget>) -> Result<MetricRegistration> {
        let reservation = self.memory.reserve(512)?;
        self.register_provider(Arc::new(MemoryCacheMetrics { cache: Arc::downgrade(cache), memory: Arc::clone(&self.memory), misses: Mutex::new(ObservedCounter::new(wall_time_millis())), _reservation: reservation }))
    }
}
impl DiskCacheMetrics {
    fn rows(&self, step: bool) -> Result<CatalogRows<CatalogMetric>> {
        let reservation = self.memory.reserve(4096)?;
        let Some(cache) = self.cache.upgrade() else { return Ok(CatalogRows::new(Vec::new(), reservation)); };
        let mut rows = gauges(cache.stats());
        let now = wall_time_millis();
        let mut traffic = self.traffic.lock();
        for ((name, counter), total) in TRAFFIC.into_iter().zip(traffic.iter_mut()).zip(traffic_totals(cache.traffic())) {
            counter.observe(total);
            rows.push(CatalogMetric::Counter { name: name.into(), tags: BTreeMap::new(), count: counter.value(now, step) });
        }
        Ok(CatalogRows::new(rows, reservation))
    }
}
#[async_trait::async_trait]
impl CatalogMetrics for DiskCacheMetrics {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { self.rows(false) }
    fn roll_step(&self, now_ms: i64, closing: bool) {
        let totals = self.cache.upgrade().map(|cache| traffic_totals(cache.traffic()));
        for (index, counter) in self.traffic.lock().iter_mut().enumerate() {
            if let Some(totals) = totals { counter.observe(totals[index]); }
            counter.poll(now_ms, closing);
        }
    }
    async fn sample_step(&self) -> Result<CatalogRows<CatalogMetric>> { self.rows(true) }
}
impl MemoryCacheMetrics {
    fn rows(&self, step: bool) -> Result<CatalogRows<CatalogMetric>> {
        let reservation = self.memory.reserve(1024)?;
        let Some(cache) = self.cache.upgrade() else { return Ok(CatalogRows::new(Vec::new(), reservation)); };
        let mut misses = self.misses.lock();
        misses.observe(cache.misses());
        let count = misses.value(wall_time_millis(), step);
        Ok(CatalogRows::new(vec![CatalogMetric::Counter { name: "memory-cache-misses".into(), tags: BTreeMap::new(), count }], reservation))
    }
}
#[async_trait::async_trait]
impl CatalogMetrics for MemoryCacheMetrics {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { self.rows(false) }
    fn roll_step(&self, now_ms: i64, closing: bool) {
        let mut misses = self.misses.lock();
        if let Some(cache) = self.cache.upgrade() { misses.observe(cache.misses()); }
        misses.poll(now_ms, closing);
    }
    async fn sample_step(&self) -> Result<CatalogRows<CatalogMetric>> { self.rows(true) }
}
fn gauges(stats: DiskCacheStats) -> Vec<CatalogMetric> {
    let free = i128::from(stats.maximum_bytes) - i128::from(stats.pinned_bytes) - i128::from(stats.evictable_bytes);
    [("disk-cache.pinnedBytes", stats.pinned_bytes as f64), ("disk-cache.evictableBytes", stats.evictable_bytes as f64),
        ("disk-cache.freeBytes", free as f64), ("disk-cache.maxSizeBytes", stats.maximum_bytes as f64)]
        .into_iter().map(|(name, value)| CatalogMetric::Gauge { name: name.into(), tags: BTreeMap::new(), unit: Some(rtdb_plan::CatalogMetricUnit::Bytes), value }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disk_cache_free_space_is_negative_while_pinned_bytes_exceed_capacity() {
        let metrics = gauges(DiskCacheStats { maximum_bytes: 10, pinned_bytes: 11, evictable_bytes: 2 });
        assert!(metrics.iter().any(|metric| matches!(metric, CatalogMetric::Gauge { name, value, .. } if name == "disk-cache.freeBytes" && *value == -3.0)));
    }
    #[test]
    fn disk_cache_registration_does_not_keep_cache_alive_and_snapshots_retain_values() {
        let memory = MemoryBudget::new(1 << 22);
        let registry = Registry::new("node", Arc::clone(&memory)).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let cache = DiskCache::open(directory.path(), 128, memory).unwrap();
        let registration = registry.register_disk_cache(Arc::clone(&cache)).unwrap();
        let snapshot = registry.metrics().unwrap();
        drop(cache);
        assert!(!registry.metrics().unwrap().rows().iter().any(|row| row.name().starts_with("disk-cache.")));
        assert_eq!(snapshot.rows().iter().filter(|row| row.name().starts_with("disk-cache.")).count(), 4);
        assert!(snapshot.rows().iter().any(|row| matches!(row, CatalogMetric::Counter { name, count, .. } if name == "disk-cache-misses" && *count == 0.0)));
        assert!(snapshot.rows().iter().any(|row| row.name() == "buffer-pool.network.read"));
        drop(registration);
    }
    #[tokio::test]
    async fn memory_cache_misses_count_reads_sent_to_the_store_and_step_with_the_registry() {
        let memory = MemoryBudget::new(1 << 22);
        let registry = Registry::new("node", Arc::clone(&memory)).unwrap();
        let budget = CacheBudget::new(1 << 20);
        let registration = registry.register_memory_cache(&budget).unwrap();
        let store = Arc::new(rtdb_storage::MemoryStore::default());
        let key = rtdb_storage::ObjectKey::new("object").unwrap();
        rtdb_storage::ObjectStore::put(store.as_ref(), &key, bytes::Bytes::from_static(b"bytes")).await.unwrap();
        let pool = rtdb_storage::cache::BufferPool::with_budget(store, Arc::clone(&budget));
        drop(pool.get(&key).await.unwrap());
        drop(pool.get(&key).await.unwrap());
        let misses = |rows: &CatalogRows<CatalogMetric>| rows.rows().iter().find_map(|row| match row { CatalogMetric::Counter { name, count, .. } if name == "memory-cache-misses" => Some(*count), _ => None });
        assert_eq!(misses(&registry.metrics().unwrap()), Some(1.0));
        registry.roll_step(wall_time_millis(), true);
        assert_eq!(misses(&registry.sample_step().await.unwrap()), Some(1.0), "a provider with a counter steps rather than failing the exporter");
        drop(registration);
    }
}
