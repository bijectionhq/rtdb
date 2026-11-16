use std::{collections::BTreeMap, sync::{Arc, Weak}};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows};
use rtdb_storage::disk_cache::{DiskCache, DiskCacheStats};
use rtdb_types::Result;
use crate::{MetricRegistration, Registry};

struct DiskCacheMetrics { cache: Weak<DiskCache>, memory: Arc<MemoryBudget>, _reservation: Arc<MemoryReservation> }
impl Registry {
    /// The registration observes the cache without extending its lifetime.
    /// Retain the returned guard for as long as the cache's gauges should be published.
    pub fn register_disk_cache(&self, cache: Arc<DiskCache>) -> Result<MetricRegistration> {
        let reservation = self.memory.reserve(512)?;
        self.register_provider(Arc::new(DiskCacheMetrics { cache: Arc::downgrade(&cache), memory: Arc::clone(&self.memory), _reservation: reservation }))
    }
}
impl CatalogMetrics for DiskCacheMetrics {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> {
        let reservation = self.memory.reserve(4096)?;
        let rows = match self.cache.upgrade() { Some(cache) => rows(cache.stats()), None => Vec::new() };
        Ok(CatalogRows::new(rows, reservation))
    }
}
fn rows(stats: DiskCacheStats) -> Vec<CatalogMetric> {
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
        let metrics = rows(DiskCacheStats { maximum_bytes: 10, pinned_bytes: 11, evictable_bytes: 2 });
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
        drop(registration);
    }
}
