use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::Mutex;
use rtdb_columnar::MemoryReservation;
use rtdb_kafka::{KafkaObserver, KafkaWarning, connect::{SourceIdentity, SourceObserver, SourceRegistration, SourceStatus}};
use rtdb_plan::CatalogMetric;
use rtdb_types::Result;
use tokio::sync::watch;
use crate::{Registry, overflow, wall_time_millis, histogram::CommitLag};

struct Source { identity: SourceIdentity, status: watch::Receiver<SourceStatus>, records: crate::step::Counter, commit_lag: Box<CommitLag>, _reservation: Arc<MemoryReservation> }
#[derive(Default)]
pub(crate) struct Sources { next: u64, entries: BTreeMap<u64, Source> }
impl Sources {
    pub fn len(&self) -> usize { self.entries.len() }
    pub fn label_bytes(&self) -> Result<usize> {
        self.entries.values().try_fold(0usize, |bytes, source| bytes.checked_add(source.identity.database.len()).and_then(|bytes| bytes.checked_add(source.identity.topic.len())).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow))?.checked_mul(6).ok_or_else(overflow)
    }
    pub fn append_rows(&mut self, rows: &mut Vec<CatalogMetric>, node_id: &str, step: bool) {
        let now_ms = wall_time_millis();
        for source in self.entries.values_mut() {
            let status = source.status.borrow();
            let tags = BTreeMap::from([("node-id".into(), node_id.into()), ("db".into(), source.identity.database.clone()),
                ("source".into(), source.identity.topic.clone()), ("source_type".into(), "kafka-connect".into()), ("partition".into(), source.identity.partition.to_string())]);
            rows.push(CatalogMetric::Counter { name: "xtdb.kafka_connect_source.records.total".into(), tags: tags.clone(), count: source.records.value(now_ms, step) });
            for (name, value, unit) in [
                ("xtdb.kafka_connect_source.connection_state", if status.connected { 1.0 } else { 0.0 }, None),
                ("xtdb.kafka_connect_source.current_offset", status.current_offset.unwrap_or(-1) as f64, None),
                ("xtdb.kafka_connect_source.last_event_time", status.last_event_epoch_seconds.unwrap_or(0) as f64, Some(rtdb_plan::CatalogMetricUnit::Seconds)),
                ("xtdb.kafka_connect_source.records_lag", status.records_lag.map_or(f64::NAN, |lag| lag as f64), None),
            ] { rows.push(CatalogMetric::Gauge { name: name.into(), tags: tags.clone(), unit, value }); }
            let lag = if step { source.commit_lag.step_snapshot(now_ms) } else { source.commit_lag.snapshot(now_ms) };
            rows.push(CatalogMetric::DistributionSummary { name: "xtdb.kafka_connect_source.commit_lag_seconds".into(), tags,
                unit: Some(rtdb_plan::CatalogMetricUnit::Seconds), count: lag.count, total: lag.total, maximum: lag.maximum, percentiles: lag.percentiles });
        }
    }
    pub fn roll_step(&mut self, now: i64, closing: bool) { for source in self.entries.values_mut() { source.records.poll(now, closing); source.commit_lag.roll_step(now, closing); } }
}
struct Registration { sources: Arc<Mutex<Sources>>, id: u64 }
impl Drop for Registration { fn drop(&mut self) { self.sources.lock().entries.remove(&self.id); } }
impl SourceRegistration for Registration {
    fn recorded(&self, count: u64) { if let Some(source) = self.sources.lock().entries.get_mut(&self.id) { source.records.add(count); } }
    fn committed(&self, lag_seconds: f64) {
        if let Some(source) = self.sources.lock().entries.get_mut(&self.id) { source.commit_lag.record(lag_seconds, wall_time_millis()); }
    }
}
impl SourceObserver for Registry {
    fn register(&self, identity: &SourceIdentity, status: watch::Receiver<SourceStatus>) -> Result<Box<dyn SourceRegistration>> {
        let bound = identity.database.len().checked_add(identity.topic.len()).and_then(|bytes| bytes.checked_add(std::mem::size_of::<Source>())).and_then(|bytes| bytes.checked_add(CommitLag::RETAINED_BYTES)).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
        let reservation = self.memory.reserve(bound)?;
        let mut sources = self.sources.lock();
        let id = sources.next;
        sources.next = id.checked_add(1).ok_or_else(overflow)?;
        sources.entries.insert(id, Source { identity: identity.clone(), status, records: crate::step::Counter::new(wall_time_millis()), commit_lag: Box::new(CommitLag::new(wall_time_millis())), _reservation: reservation });
        Ok(Box::new(Registration { sources: Arc::clone(&self.sources), id }))
    }
}
impl KafkaObserver for Registry {
    fn warning(&self, warning: KafkaWarning) {
        let index = match &warning { KafkaWarning::AcknowledgementsOverridden { .. } => 0, KafkaWarning::IdempotenceDisabled => 1,
            KafkaWarning::InsufficientReplicas { .. } => 2, KafkaWarning::UncleanElection { .. } => 3, KafkaWarning::ConfigurationUnavailable { .. } => 4 };
        let mut state = self.state.lock(); state.counters.kafka_warnings[index].add(1); drop(state);
        tracing::warn!("Kafka configuration warning: {warning:?}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_columnar::MemoryBudget;
    use rtdb_plan::CatalogMetrics;
    #[test]
    fn source_registration_observes_published_status_and_unregisters_on_drop() {
        let memory = MemoryBudget::new(1 << 22);
        let registry = Registry::new("n", memory).unwrap();
        let (status, receiver) = watch::channel(SourceStatus::default());
        let registration = registry.register(&SourceIdentity { database: "d".into(), topic: "t".into(), partition: 0 }, receiver).unwrap();
        status.send_modify(|status| { status.connected = true; status.records = 17; status.current_offset = Some(42); });
        registration.recorded(17);
        registration.committed(0.5); registration.committed(1.5); registration.committed(-1.0);
        let rows = registry.metrics().unwrap();
        assert!(rows.rows().iter().any(|metric| matches!(metric, CatalogMetric::Counter { name, count, tags } if name == "xtdb.kafka_connect_source.records.total" && *count == 17.0 && tags.get("source").is_some_and(|source| source == "t"))));
        assert!(rows.rows().iter().any(|metric| matches!(metric, CatalogMetric::DistributionSummary { name, count: 2, total: 2.0, maximum: 1.5, .. } if name == "xtdb.kafka_connect_source.commit_lag_seconds")));
        drop(registration);
        assert!(!registry.metrics().unwrap().rows().iter().any(|metric| metric.name().starts_with("xtdb.kafka_connect_source")));
        assert!(rows.rows().iter().any(|metric| metric.name() == "xtdb.kafka_connect_source.current_offset"));
    }
}
