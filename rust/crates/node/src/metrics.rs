use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows};
use rtdb_query::SharedPlans;
use rtdb_types::{Anomaly, Result};
use crate::{DatabaseCatalog, database::DatabaseFactory};

pub(crate) struct NodeMetrics {
    catalog: Weak<DatabaseCatalog>,
    factory: Weak<DatabaseFactory>,
    shared_plans: Weak<SharedPlans>,
    memory: Arc<MemoryBudget>,
    _reservation: Arc<MemoryReservation>,
}
impl NodeMetrics {
    pub fn new(catalog: Weak<DatabaseCatalog>, factory: Weak<DatabaseFactory>, shared_plans: Weak<SharedPlans>, memory: Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(512)?;
        Ok(Self { catalog, factory, shared_plans, memory, _reservation: reservation })
    }
}
impl CatalogMetrics for NodeMetrics {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> {
        let (Some(catalog), Some(factory)) = (self.catalog.upgrade(), self.factory.upgrade()) else { return Ok(CatalogRows::new(Vec::new(), self.memory.reserve(0)?)); };
        let databases = catalog.open_database_snapshot(&self.memory)?;
        let bound = databases.rows().iter().try_fold(8192usize, |bytes, database| database.name().len().checked_add(1024).and_then(|size| size.checked_mul(9)).and_then(|size| bytes.checked_add(size)).ok_or_else(overflow))?;
        let reservation = self.memory.reserve(bound)?;
        let mut rows = Vec::with_capacity(15 + databases.rows().len() * 9);
        let push_allocator = |rows: &mut Vec<CatalogMetric>, allocator: &str, database: &str, allocated: usize, limit: usize| {
            let tags = BTreeMap::from([("allocator".into(), allocator.into()), ("database".into(), database.into())]);
            rows.push(CatalogMetric::Gauge { name: "xtdb.allocator.memory.allocated".into(), tags: tags.clone(), unit: Some(rtdb_plan::CatalogMetricUnit::Bytes), value: allocated as f64 });
            if limit < usize::MAX {
                rows.push(CatalogMetric::Gauge { name: "xtdb.allocator.memory.limit".into(), tags, unit: Some(rtdb_plan::CatalogMetricUnit::Bytes), value: limit as f64 });
            }
        };
        for database in databases.rows() {
            let completed = database.partition.value().live().latest_completed_tx().map_or(-1.0, |key| key.tx_id.0 as f64);
            let processed = database.progress().current().source_position.0;
            let submitted = database.latest_submitted_position();
            let lag = submitted.as_ref().map_or(f64::NAN, |position| position.0.wrapping_sub(processed).max(0) as f64);
            let status = database.status();
            let leader = if matches!(status.role, rtdb_replication::Role::Leading(_)) { 1.0 } else { 0.0 };
            for (name, value) in [("node.tx.latestCompletedTxId", completed), ("node.tx.latestSubmittedMsgId", submitted.map_or(f64::NAN, |position| position.0 as f64)),
                ("node.tx.latestProcessedMsgId", processed as f64), ("node.tx.lag.MsgId", lag), ("xtdb.log.leader", leader), ("xtdb.block.last_upload_time", status.last_block_upload_epoch_seconds as f64)] {
                let unit = (name == "xtdb.block.last_upload_time").then_some(rtdb_plan::CatalogMetricUnit::Seconds);
                rows.push(CatalogMetric::Gauge { name: name.into(), tags: BTreeMap::from([("db".into(), database.name().into())]), unit, value });
            }
            let available = rtdb_maintenance::available_jobs(&database.trie_catalog()).map_or(f64::NAN, |jobs| jobs.len() as f64);
            rows.push(CatalogMetric::Gauge { name: "compactor.jobs.available".into(), tags: BTreeMap::from([("db".into(), database.name().into())]), unit: None, value: available });
            push_allocator(&mut rows, "buffer-pool", database.name(), database.storage.pool_retained_bytes(), database.storage.scan_byte_limit());
        }
        let (retained, limit) = (factory.cache.retained_bytes(), factory.cache.limit_bytes());
        push_allocator(&mut rows, "memory-cache", "", retained, limit);
        push_allocator(&mut rows, "query", "", factory.query_memory.used_bytes(), factory.query_memory.limit_bytes());
        // Upstream's memory cache holds nothing evictable: every cached buffer is pinned by its allocator.
        for (name, value) in [("memory-cache.pinnedBytes", retained as f64), ("memory-cache.evictableBytes", 0.0), ("memory-cache.freeBytes", limit as f64 - retained as f64)] {
            rows.push(CatalogMetric::Gauge { name: name.into(), tags: BTreeMap::new(), unit: Some(rtdb_plan::CatalogMetricUnit::Bytes), value });
        }
        rows.push(CatalogMetric::Gauge { name: "rtdb.cache.retained.bytes".into(), tags: BTreeMap::new(), unit: Some(rtdb_plan::CatalogMetricUnit::Bytes), value: factory.cache.retained_bytes() as f64 });
        rows.push(CatalogMetric::Gauge { name: "rtdb.cache.limit.bytes".into(), tags: BTreeMap::new(), unit: Some(rtdb_plan::CatalogMetricUnit::Bytes), value: factory.cache.limit_bytes() as f64 });
        if let Some(plans) = self.shared_plans.upgrade() {
            let stats = plans.stats();
            for (name, count) in [
                ("rtdb.query.cache.read.preparations", stats.read_preparations),
                ("rtdb.query.cache.plan.misses", stats.plan_misses),
                ("rtdb.query.cache.syntax.hits", stats.syntax_hits),
                ("rtdb.query.cache.syntax.misses", stats.syntax_misses),
            ] {
                rows.push(CatalogMetric::Counter { name: name.into(), tags: BTreeMap::new(), count: count as f64 });
            }
            for (name, bytes) in [
                ("rtdb.query.cache.plan.charged.bytes", stats.plan_charged_bytes),
                ("rtdb.query.cache.syntax.retained.bytes", stats.syntax_retained_bytes),
            ] {
                rows.push(CatalogMetric::Gauge { name: name.into(), tags: BTreeMap::new(), unit: Some(rtdb_plan::CatalogMetricUnit::Bytes), value: bytes as f64 });
            }
        }
        Ok(CatalogRows::new(rows, reservation))
    }
}
fn overflow() -> Anomaly { Anomaly::incorrect("rtdb.node/metric-allocation", "Node metric allocation exceeds addressable memory") }
