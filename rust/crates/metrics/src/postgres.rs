use std::{collections::BTreeMap, sync::{Arc, Weak}};
use parking_lot::Mutex;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_pg_source::{SourceAssignmentMetrics, SourceMetrics, SourceMetricsFactory, SourceObserver, SourcePhase};
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows};
use rtdb_types::{Anomaly, Result};
use crate::{MetricRegistration, Registry, histogram::CommitLag, overflow, wall_time_millis};

struct State { events: crate::step::Counter, commits: crate::step::Counter, lag: Box<CommitLag>, generation: u64, assigned: Option<SourceObserver> }
struct PostgresMetrics { database: String, slot: String, memory: Arc<MemoryBudget>, state: Mutex<State>, _reservation: Arc<MemoryReservation> }
struct Registration { metrics: Arc<PostgresMetrics>, _registration: MetricRegistration }
struct Assignment { metrics: Weak<PostgresMetrics>, generation: u64, _reservation: Arc<MemoryReservation> }
impl SourceAssignmentMetrics for Assignment {}
impl Drop for Assignment {
    fn drop(&mut self) {
        if let Some(metrics) = self.metrics.upgrade() {
            let mut state = metrics.state.lock();
            if state.generation == self.generation { state.assigned = None; }
        }
    }
}
impl SourceMetricsFactory for Registry {
    fn register_source(&self, database: &str, slot: &str) -> Result<Arc<dyn SourceMetrics>> {
        let bound = database.len().checked_add(slot.len()).and_then(|bytes| bytes.checked_add(CommitLag::RETAINED_BYTES)).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
        let reservation = self.memory.reserve(bound)?;
        let metrics = Arc::new(PostgresMetrics { database: database.into(), slot: slot.into(), memory: Arc::clone(&self.memory),
            state: Mutex::new(State { events: crate::step::Counter::new(wall_time_millis()), commits: crate::step::Counter::new(wall_time_millis()), lag: Box::new(CommitLag::new(wall_time_millis())), generation: 0, assigned: None }), _reservation: reservation });
        let registration = self.register_provider(metrics.clone())?;
        Ok(Arc::new(Registration { metrics, _registration: registration }))
    }
}
impl SourceMetrics for Registration {
    fn assigned(&self, observer: SourceObserver) -> Result<Box<dyn SourceAssignmentMetrics>> {
        let reservation = self.metrics.memory.reserve(512)?;
        let mut state = self.metrics.state.lock();
        if state.assigned.is_some() { return Err(Anomaly::fault("rtdb.metrics/source-assigned", "Previous PostgreSQL source assignment has not ended")); }
        state.generation = state.generation.checked_add(1).ok_or_else(overflow)?;
        state.assigned = Some(observer);
        Ok(Box::new(Assignment { metrics: Arc::downgrade(&self.metrics), generation: state.generation, _reservation: reservation }))
    }
    fn applied(&self, events: usize, lag_seconds: Option<f64>) {
        let mut state = self.metrics.state.lock();
        state.events.add(events as u64); state.commits.add(1);
        if let Some(lag) = lag_seconds { state.lag.record(lag, wall_time_millis()); }
    }
}
impl PostgresMetrics {
    fn rows(&self, wal: Option<(u64, f64)>, step: bool) -> Result<CatalogRows<CatalogMetric>> {
        let per_row = self.database.len().checked_add(self.slot.len()).and_then(|bytes| bytes.checked_add(2048)).ok_or_else(overflow)?;
        let reservation = self.memory.reserve(per_row.checked_mul(6).ok_or_else(overflow)?)?;
        let mut state = self.state.lock();
        let status = state.assigned.as_ref().map(SourceObserver::status);
        let tags = BTreeMap::from([("db".into(), self.database.clone()), ("source".into(), self.slot.clone()), ("source_type".into(), "postgres".into())]);
        let now = wall_time_millis();
        let lag = if step { state.lag.step_snapshot(now) } else { state.lag.snapshot(now) };
        let mut rows = Vec::with_capacity(6);
        rows.push(CatalogMetric::Counter { name: "xtdb.postgres_source.events.total".into(), tags: tags.clone(), count: state.events.value(now, step) });
        rows.push(CatalogMetric::Counter { name: "xtdb.postgres_source.commits.total".into(), tags: tags.clone(), count: state.commits.value(now, step) });
        rows.push(CatalogMetric::DistributionSummary { name: "xtdb.postgres_source.commit_lag_seconds".into(), tags: tags.clone(), unit: Some(rtdb_plan::CatalogMetricUnit::Seconds), count: lag.count, total: lag.total, maximum: lag.maximum, percentiles: lag.percentiles });
        for (name, value, unit) in [
            ("xtdb.postgres_source.last_event_time", status.as_ref().and_then(|status| status.last_commit_time).map_or(0.0, |time| time.0.div_euclid(1_000_000) as f64), Some(rtdb_plan::CatalogMetricUnit::Seconds)),
            ("xtdb.postgres_source.connection_state", if status.as_ref().is_some_and(|status| status.phase == SourcePhase::Streaming) { 1.0 } else { 0.0 }, None),
            ("xtdb.postgres_source.wal_lag_bytes", wal.filter(|(generation, _)| *generation == state.generation && state.assigned.is_some()).map_or(f64::NAN, |(_, value)| value), Some(rtdb_plan::CatalogMetricUnit::Bytes)),
        ] { rows.push(CatalogMetric::Gauge { name: name.into(), tags: tags.clone(), unit, value }); }
        Ok(CatalogRows::new(rows, reservation))
    }
    async fn sampled(&self, step: bool) -> Result<CatalogRows<CatalogMetric>> {
        let assigned = { let state = self.state.lock(); state.assigned.clone().map(|observer| (state.generation, observer)) };
        let lag = match assigned {
            Some((generation, observer)) => Some((generation, observer.wal_lag_bytes().await.ok().flatten().map_or(f64::NAN, |bytes| bytes as f64))),
            None => None,
        };
        self.rows(lag, step)
    }
}
#[async_trait::async_trait]
impl CatalogMetrics for PostgresMetrics {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { self.rows(None, false) }
    async fn sample(&self) -> Result<CatalogRows<CatalogMetric>> { self.sampled(false).await }
    async fn sample_step(&self) -> Result<CatalogRows<CatalogMetric>> { self.sampled(true).await }
    fn roll_step(&self, now: i64, closing: bool) { let mut state = self.state.lock(); state.events.poll(now, closing); state.commits.poll(now, closing); state.lag.roll_step(now, closing); }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_pg_source::{Remote, SourceConfig, SourceDriver, SslMode};
    fn observer(memory: Arc<MemoryBudget>) -> SourceObserver {
        let mut remote = Remote::new("unused.invalid".into(), "db".into(), "user".into(), "password".into()); remote.ssl = SslMode::Disable;
        SourceDriver::new(remote, SourceConfig { remote: "remote".into(), slot_name: "slot".into(), publication_name: "publication".into(), indexer: None, ignore_tables: Default::default() }, memory, None).unwrap().observer().unwrap()
    }
    #[tokio::test]
    async fn postgres_source_counters_survive_assignments_while_unassigned_gauges_reset() {
        let memory = MemoryBudget::new(1 << 22);
        let registry = Registry::new("node", Arc::clone(&memory)).unwrap();
        let registration = registry.register_source("db", "slot").unwrap();
        let first = registration.assigned(observer(Arc::clone(&memory))).unwrap();
        registration.applied(3, Some(0.5));
        drop(first);
        let second = registration.assigned(observer(memory)).unwrap();
        registration.applied(2, Some(1.5));
        drop(second);
        let rows = registry.sample().await.unwrap();
        assert!(rows.rows().iter().any(|metric| matches!(metric, CatalogMetric::Counter { name, count, .. } if name == "xtdb.postgres_source.events.total" && *count == 5.0)));
        assert!(rows.rows().iter().any(|metric| matches!(metric, CatalogMetric::DistributionSummary { name, count: 2, total: 2.0, .. } if name == "xtdb.postgres_source.commit_lag_seconds")));
        assert!(rows.rows().iter().any(|metric| matches!(metric, CatalogMetric::Gauge { name, value, .. } if name == "xtdb.postgres_source.wal_lag_bytes" && value.is_nan())));
        assert!(rows.rows().iter().any(|metric| matches!(metric, CatalogMetric::Gauge { name, value, .. } if name == "xtdb.postgres_source.last_event_time" && *value == 0.0)));
        drop(registration);
        assert!(!registry.metrics().unwrap().rows().iter().any(|metric| metric.name().starts_with("xtdb.postgres_source")));
        assert!(rows.rows().iter().any(|metric| metric.name() == "xtdb.postgres_source.commits.total"));
    }
}
