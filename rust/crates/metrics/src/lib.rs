mod histogram;
mod disk;
mod trace;
mod step;
mod processor;
mod observed;
pub use observed::{ObservedSeries, ObservedTotals};
pub mod logging;
pub use trace::{
    OTLP_PROTOBUF_CONTENT_TYPE, OtlpAnyValue, OtlpExportTracePartialSuccess, OtlpExportTraceRequest,
    OtlpExportTraceResponse, OtlpInstrumentationScope, OtlpKeyValue, OtlpResource, OtlpResourceSpans,
    OtlpScopeSpans, OtlpSpan, OtlpValue, TracePayload, TraceReceiver, TraceRecord, TraceRecorder,
    TraceStats, decode_trace_request, decode_trace_response, encode_trace_response,
};
#[cfg(feature = "pg-source")]
mod postgres;
#[cfg(feature = "kafka")]
mod kafka;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use parking_lot::Mutex;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_maintenance::CollectionObserver;
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows, ConnectionEvent, ExecutionError, ExecutionObserver, ExecutionTimer, ReplicaRecordKind};
use rtdb_types::{Anomaly, Result, TableRef};

const TIMERS: [ExecutionTimer; 14] = [ExecutionTimer::Query, ExecutionTimer::TxAwait, ExecutionTimer::TxSubmit, ExecutionTimer::TxExecute, ExecutionTimer::PgwireTransaction, ExecutionTimer::TxOp, ExecutionTimer::CompactorJob, ExecutionTimer::DurableCommitLag, ExecutionTimer::StagedCommitLag, ExecutionTimer::BlockUpload, ExecutionTimer::TrieDelete, ExecutionTimer::BlockFileDelete, ExecutionTimer::TableBlockFileDelete, ExecutionTimer::OffloadedPayloadDelete];
struct Counters { query_errors: step::Counter, query_warnings: step::Counter, transaction_errors: step::Counter, collection_errors: step::Counter, durable_commit_clock_invalid: step::Counter, staged_commit_clock_invalid: step::Counter, kafka_warnings: [step::Counter; 5], connections: [step::Counter; 3] }
impl Counters {
    fn roll(&mut self, now: i64, closing: bool) {
        for counter in [&mut self.query_errors, &mut self.query_warnings, &mut self.transaction_errors, &mut self.collection_errors, &mut self.durable_commit_clock_invalid, &mut self.staged_commit_clock_invalid].into_iter().chain(self.kafka_warnings.iter_mut()).chain(self.connections.iter_mut()) { counter.poll(now, closing); }
    }
}
const SAMPLE_SHARDS: usize = 16;
const SHARD_SAMPLES: usize = 256;
#[derive(Clone, Copy)]
struct Sample { timer: usize, elapsed: Duration, now_ms: i64 }
/// Durations recorded on the threads sharing this shard and not yet applied to the timers.
#[repr(align(128))]
struct Shard(Mutex<Vec<Sample>>);
static NEXT_SHARD: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
fn shard_index() -> usize {
    thread_local! { static SHARD: usize = NEXT_SHARD.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % SAMPLE_SHARDS; }
    SHARD.with(|shard| *shard)
}
struct State { timers: Vec<histogram::Timer>, counters: Counters, pending: Vec<Sample> }
impl State {
    /// Applies every shard's samples in the order of their clock readings, as a lock taken per sample would have.
    fn absorb(&mut self, shards: &[Shard]) {
        for shard in shards { self.pending.extend(shard.0.lock().drain(..)); }
        self.pending.sort_unstable_by_key(|sample| sample.now_ms);
        for sample in self.pending.drain(..) { self.timers[sample.timer].record(sample.elapsed, sample.now_ms); }
    }
}
/// One database's follower meters, the reference's `FollowerLogProcessor` ones, registered on its first observation.
struct ReplicaMeters { process: [histogram::Timer; 5], buffer: histogram::Timer, buffered: histogram::CommitLag, _reservation: Arc<MemoryReservation> }
impl ReplicaMeters {
    const RETAINED_BYTES: usize = std::mem::size_of::<Self>() + 6 * histogram::Timer::RETAINED_BYTES + histogram::CommitLag::RETAINED_BYTES;
    fn roll_step(&mut self, now: i64, closing: bool) {
        for timer in self.process.iter_mut().chain(std::iter::once(&mut self.buffer)) { timer.roll_step(now, closing); }
        self.buffered.roll_step(now, closing);
    }
}
fn replica_index(kind: ReplicaRecordKind) -> usize { ReplicaRecordKind::ALL.iter().position(|candidate| *candidate == kind).unwrap_or(0) }

struct Provider { value: Arc<dyn CatalogMetrics>, _reservation: Arc<MemoryReservation> }
#[derive(Default)]
struct Providers { next: u64, entries: BTreeMap<u64, Provider> }
pub struct MetricRegistration { providers: Arc<Mutex<Providers>>, id: u64 }
impl Drop for MetricRegistration { fn drop(&mut self) { self.providers.lock().entries.remove(&self.id); } }

pub struct Registry {
    node_id: String,
    memory: Arc<MemoryBudget>,
    state: Mutex<State>,
    shards: Box<[Shard]>,
    processor: Mutex<processor::CpuSampler>,
    replica: Mutex<BTreeMap<String, ReplicaMeters>>,
    _reservation: Arc<MemoryReservation>,
    providers: Arc<Mutex<Providers>>,
    #[cfg(feature = "kafka")]
    sources: Arc<Mutex<kafka::Sources>>,
}
impl Registry {
    /// Registry allocations and returned catalog snapshots share the supplied execution budget.
    /// Callbacks allocate no metric names or labels; source registrations retain their own charges until removed.
    pub fn new(node_id: &str, memory: Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let samples = (2 * SAMPLE_SHARDS * SHARD_SAMPLES * std::mem::size_of::<Sample>()).checked_add(SAMPLE_SHARDS * std::mem::size_of::<Shard>()).ok_or_else(overflow)?;
        let reservation = memory.reserve(std::mem::size_of::<Self>().checked_add(TIMERS.len() * histogram::Timer::RETAINED_BYTES).and_then(|bytes| bytes.checked_add(node_id.len())).and_then(|bytes| bytes.checked_add(samples)).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?)?;
        let now = wall_time_millis();
        Ok(Arc::new(Self { node_id: node_id.to_owned(), memory,
            state: Mutex::new(State { timers: TIMERS.iter().map(|_| histogram::Timer::new(now)).collect(), counters: Counters { query_errors: step::Counter::new(now), query_warnings: step::Counter::new(now), transaction_errors: step::Counter::new(now), collection_errors: step::Counter::new(now), durable_commit_clock_invalid: step::Counter::new(now), staged_commit_clock_invalid: step::Counter::new(now), kafka_warnings: [step::Counter::new(now); 5], connections: [step::Counter::new(now); 3] }, pending: Vec::with_capacity(SAMPLE_SHARDS * SHARD_SAMPLES) }),
            shards: (0..SAMPLE_SHARDS).map(|_| Shard(Mutex::new(Vec::with_capacity(SHARD_SAMPLES)))).collect(), processor: Mutex::new(processor::CpuSampler::new()), replica: Mutex::new(BTreeMap::new()), _reservation: reservation, providers: Arc::new(Mutex::new(Providers::default())),
            #[cfg(feature = "kafka")]
            sources: Arc::new(Mutex::new(kafka::Sources::default())),
        }))
    }
    fn tags(&self) -> BTreeMap<String, String> { BTreeMap::from([("node-id".into(), self.node_id.clone())]) }
    /// Runs `record` on `database`'s follower meters, registering them first; a database whose meters cannot be charged goes unmeasured.
    fn replica_meters(&self, database: &str, record: impl FnOnce(&mut ReplicaMeters, i64)) {
        let now = wall_time_millis();
        let mut replica = self.replica.lock();
        if !replica.contains_key(database) {
            let Ok(reservation) = self.memory.reserve(ReplicaMeters::RETAINED_BYTES.saturating_add(database.len())) else { return; };
            replica.insert(database.to_owned(), ReplicaMeters {
                process: std::array::from_fn(|_| histogram::Timer::new(now)), buffer: histogram::Timer::new(now),
                buffered: histogram::CommitLag::new(now), _reservation: reservation,
            });
        }
        if let Some(meters) = replica.get_mut(database) { record(meters, now); }
    }
    fn counter(&self, name: &str, count: f64) -> CatalogMetric { CatalogMetric::Counter { name: name.into(), tags: self.tags(), count } }
    fn gauge(&self, name: &str, value: f64) -> CatalogMetric { CatalogMetric::Gauge { name: name.into(), tags: self.tags(), unit: Some(rtdb_plan::CatalogMetricUnit::Bytes), value } }
    /// The provider must not sample or roll this registry recursively, mutate its registrations during rollover, or retain its node through a strong ownership cycle.
    /// Keep the returned guard for the provider's region; dropping it unregisters future observations.
    pub fn register_provider(&self, provider: Arc<dyn CatalogMetrics>) -> Result<MetricRegistration> {
        let reservation = self.memory.reserve(512)?;
        let mut providers = self.providers.lock();
        let id = providers.next; providers.next = id.checked_add(1).ok_or_else(overflow)?;
        providers.entries.insert(id, Provider { value: provider, _reservation: reservation });
        Ok(MetricRegistration { providers: Arc::clone(&self.providers), id })
    }
}
impl ExecutionObserver for Registry {
    fn record_duration(&self, timer: ExecutionTimer, elapsed: Duration) {
        let index = match timer { ExecutionTimer::Query => 0, ExecutionTimer::TxAwait => 1, ExecutionTimer::TxSubmit => 2, ExecutionTimer::TxExecute => 3, ExecutionTimer::PgwireTransaction => 4, ExecutionTimer::TxOp => 5, ExecutionTimer::CompactorJob => 6, ExecutionTimer::DurableCommitLag => 7, ExecutionTimer::StagedCommitLag => 8, ExecutionTimer::BlockUpload => 9, ExecutionTimer::TrieDelete => 10, ExecutionTimer::BlockFileDelete => 11, ExecutionTimer::TableBlockFileDelete => 12, ExecutionTimer::OffloadedPayloadDelete => 13 };
        let sample = Sample { timer: index, elapsed, now_ms: wall_time_millis() };
        let shard = &self.shards[shard_index()];
        loop {
            let mut pending = shard.0.lock();
            if pending.len() < SHARD_SAMPLES { pending.push(sample); return; }
            drop(pending);
            self.state.lock().absorb(&self.shards);
        }
    }
    fn record_error(&self, error: ExecutionError) {
        let mut state = self.state.lock();
        let count = match error { ExecutionError::Query => &mut state.counters.query_errors, ExecutionError::Transaction => &mut state.counters.transaction_errors };
        count.add(1);
    }
    fn record_query_warning(&self) { self.state.lock().counters.query_warnings.add(1); }
    fn connection_event(&self, event: ConnectionEvent) {
        let index = match event { ConnectionEvent::Opened => 0, ConnectionEvent::Cancelled => 1, ConnectionEvent::Refused => 2 };
        let mut state = self.state.lock(); state.counters.connections[index].add(1);
    }
    fn record_durable_commit_lag(&self, lag: Option<Duration>) {
        match lag {
            Some(lag) => self.record_duration(ExecutionTimer::DurableCommitLag, lag),
            None => self.state.lock().counters.durable_commit_clock_invalid.add(1),
        }
    }
    fn record_staged_commit_lag(&self, lag: Option<Duration>) {
        match lag {
            Some(lag) => self.record_duration(ExecutionTimer::StagedCommitLag, lag),
            None => self.state.lock().counters.staged_commit_clock_invalid.add(1),
        }
    }
    fn record_replica_process(&self, database: &str, kind: ReplicaRecordKind, elapsed: Duration) {
        self.replica_meters(database, |meters, now| meters.process[replica_index(kind)].record(elapsed, now));
    }
    fn record_replica_block_buffer(&self, database: &str, elapsed: Duration, records: usize) {
        self.replica_meters(database, |meters, now| { meters.buffer.record(elapsed, now); meters.buffered.record(records as f64, now); });
    }
}
impl CollectionObserver for Registry {
    fn table_failed(&self, table: Option<&TableRef>, error: &Anomaly) {
        let mut state = self.state.lock(); state.counters.collection_errors.add(1); drop(state);
        match table {
            Some(table) => tracing::error!(target: "rtdb_maintenance::gc", table = %table.schema_and_table(), "Garbage collection failed: {error}"),
            None => tracing::error!(target: "rtdb_maintenance::gc", "Garbage collection failed: {error}"),
        }
    }
    fn trie_deleted(&self, elapsed: Duration) { self.record_duration(ExecutionTimer::TrieDelete, elapsed); }
    fn block_file_deleted(&self, elapsed: Duration) { self.record_duration(ExecutionTimer::BlockFileDelete, elapsed); }
    fn table_block_file_deleted(&self, elapsed: Duration) { self.record_duration(ExecutionTimer::TableBlockFileDelete, elapsed); }
    fn offloaded_payload_deleted(&self, elapsed: Duration) { self.record_duration(ExecutionTimer::OffloadedPayloadDelete, elapsed); }
}
impl Registry {
    fn provider_snapshot(&self) -> Result<(Vec<Arc<dyn CatalogMetrics>>, Arc<MemoryReservation>)> {
        let providers = self.providers.lock();
        let reservation = self.memory.reserve(providers.entries.len().checked_mul(256).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?)?;
        Ok((providers.entries.values().map(|provider| Arc::clone(&provider.value)).collect(), reservation))
    }
    fn metric_rows(&self, provided: &[CatalogRows<CatalogMetric>], step: bool) -> Result<CatalogRows<CatalogMetric>> {
        let provided_count = provided.iter().try_fold(0usize, |count, rows| count.checked_add(rows.rows().len()).ok_or_else(overflow))?;
        let provided_bytes = provided.iter().flat_map(|rows| rows.rows()).try_fold(0usize, |bytes, row| bytes.checked_add(metric_bytes(row)?).ok_or_else(overflow))?;
        #[cfg(feature = "kafka")]
        let mut sources = self.sources.lock();
        // Six execution counters, three connections, five Kafka warnings and six gauges.
        let mut count = (TIMERS.len() + 20).checked_add(provided_count).ok_or_else(overflow)?;
        #[cfg(feature = "kafka")]
        let labels = { count = count.checked_add(sources.len().checked_mul(6).ok_or_else(overflow)?).ok_or_else(overflow)?; sources.label_bytes()? };
        #[cfg(not(feature = "kafka"))]
        let labels = 0usize;
        let mut replica = self.replica.lock();
        let replica_labels = replica.keys().try_fold(0usize, |bytes, database| bytes.checked_add(database.len().checked_mul(7)?)).ok_or_else(overflow)?;
        count = count.checked_add(replica.len().checked_mul(7).ok_or_else(overflow)?).ok_or_else(overflow)?;
        let labels = labels.checked_add(replica_labels).ok_or_else(overflow)?;
        let per_metric = self.node_id.len().checked_add(2048).ok_or_else(overflow)?;
        let bound = count.checked_mul(per_metric).and_then(|bytes| bytes.checked_add(labels)).and_then(|bytes| bytes.checked_add(provided_bytes)).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
        let reservation = self.memory.reserve(bound)?;
        let allocated = self.memory.used_bytes();
        let mut rows = Vec::with_capacity(count);
        let mut state = self.state.lock();
        state.absorb(&self.shards);
        let now = wall_time_millis();
        for (timer, value) in TIMERS.into_iter().zip(&mut state.timers) {
            let snapshot = if step { value.step_snapshot(now) } else { value.snapshot(now) };
            rows.push(CatalogMetric::Timer { name: timer.name().into(), tags: self.tags(), count: snapshot.count, total_ns: snapshot.total, percentiles_ns: snapshot.percentiles,
                mean_ns: Some(snapshot.mean), max_ns: Some(snapshot.maximum), p75_ns: Some(snapshot.percentiles[0] as i64), p95_ns: Some(snapshot.percentiles[2] as i64),
                p99_ns: Some(snapshot.percentiles[4] as i64), p999_ns: Some(snapshot.percentiles[5] as i64) });
        }
        rows.push(self.counter("query.error", state.counters.query_errors.value(now, step)));
        rows.push(self.counter("query.warning", state.counters.query_warnings.value(now, step)));
        rows.push(self.counter("tx.error", state.counters.transaction_errors.value(now, step)));
        rows.push(self.counter("rtdb.gc.error", state.counters.collection_errors.value(now, step)));
        rows.push(self.counter("rtdb.replica.commit.clock_invalid", state.counters.durable_commit_clock_invalid.value(now, step)));
        rows.push(self.counter("rtdb.replica.commit.staged_clock_invalid", state.counters.staged_commit_clock_invalid.value(now, step)));
        for (name, count) in ["pgwire.total_connections", "pgwire.cancelled_connections", "pgwire.connections_refused"].into_iter().zip(&mut state.counters.connections) { rows.push(self.counter(name, count.value(now, step))); }
        for (reason, count) in ["acknowledgements_overridden", "idempotence_disabled", "insufficient_replicas", "unclean_election", "configuration_unavailable"].into_iter().zip(&mut state.counters.kafka_warnings) {
            let mut tags = self.tags(); tags.insert("reason".into(), reason.into());
            rows.push(CatalogMetric::Counter { name: "rtdb.kafka.warning".into(), tags, count: count.value(now, step) });
        }
        drop(state);
        for (database, meters) in replica.iter_mut() {
            let timer = |name: &str, tags: BTreeMap<String, String>, value: &mut histogram::Timer| {
                let snapshot = if step { value.step_snapshot(now) } else { value.snapshot(now) };
                CatalogMetric::Timer { name: name.into(), tags, count: snapshot.count, total_ns: snapshot.total, percentiles_ns: snapshot.percentiles,
                    mean_ns: Some(snapshot.mean), max_ns: Some(snapshot.maximum), p75_ns: Some(snapshot.percentiles[0] as i64), p95_ns: Some(snapshot.percentiles[2] as i64),
                    p99_ns: Some(snapshot.percentiles[4] as i64), p999_ns: Some(snapshot.percentiles[5] as i64) }
            };
            let mut tags = self.tags();
            tags.insert("db".into(), database.clone());
            for (kind, value) in ReplicaRecordKind::ALL.into_iter().zip(&mut meters.process) {
                let mut tags = tags.clone();
                tags.insert("msg.type".into(), kind.name().into());
                rows.push(timer("xtdb.replica.process.timer", tags, value));
            }
            rows.push(timer("xtdb.replica.block.buffer.timer", tags.clone(), &mut meters.buffer));
            let summary = if step { meters.buffered.step_snapshot(now) } else { meters.buffered.snapshot(now) };
            rows.push(CatalogMetric::DistributionSummary { name: "xtdb.replica.block.buffered.records".into(), tags, unit: None,
                count: summary.count, total: summary.total, maximum: summary.maximum, percentiles: summary.percentiles });
        }
        drop(replica);
        rows.push(self.gauge("xtdb.allocator.root.memory.allocated", allocated as f64));
        rows.push(self.gauge("xtdb.allocator.root.memory.limit", self.memory.limit_bytes() as f64));
        let processors = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        rows.push(CatalogMetric::Gauge { name: "system.cpu.count".into(), tags: self.tags(), unit: None, value: processors as f64 });
        let reading = self.processor.lock().read(processors);
        for (name, value) in [("system.cpu.usage", reading.system_cpu_usage), ("process.cpu.usage", reading.process_cpu_usage), ("system.load.average.1m", reading.load_average)] {
            rows.push(CatalogMetric::Gauge { name: name.into(), tags: self.tags(), unit: None, value: value.unwrap_or(f64::NAN) });
        }
        #[cfg(feature = "kafka")]
        sources.append_rows(&mut rows, &self.node_id, step);
        for metric in provided.iter().flat_map(|rows| rows.rows()) {
            let mut metric = metric.clone();
            let tags = match &mut metric { CatalogMetric::Timer { tags, .. } | CatalogMetric::DistributionSummary { tags, .. } | CatalogMetric::Gauge { tags, .. } | CatalogMetric::Counter { tags, .. } => tags };
            tags.insert("node-id".into(), self.node_id.clone());
            if !rows.iter().any(|existing| existing.name() == metric.name() && existing.tags() == metric.tags()) { rows.push(metric); }
        }
        Ok(CatalogRows::new(rows, reservation))
    }
}
#[async_trait::async_trait]
impl CatalogMetrics for Registry {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> {
        let (providers, _provider_charge) = self.provider_snapshot()?;
        let provided = providers.iter().map(|provider| provider.metrics()).collect::<Result<Vec<_>>>()?;
        self.metric_rows(&provided, false)
    }
    async fn sample(&self) -> Result<CatalogRows<CatalogMetric>> {
        let (providers, _provider_charge) = self.provider_snapshot()?;
        let mut provided = Vec::with_capacity(providers.len());
        for provider in providers { provided.push(provider.sample().await?); }
        self.metric_rows(&provided, false)
    }
    fn roll_step(&self, now_ms: i64, closing: bool) {
        { let mut state = self.state.lock(); state.absorb(&self.shards); for timer in &mut state.timers { timer.roll_step(now_ms, closing); } state.counters.roll(now_ms, closing); }
        for meters in self.replica.lock().values_mut() { meters.roll_step(now_ms, closing); }
        #[cfg(feature = "kafka")]
        self.sources.lock().roll_step(now_ms, closing);
        for provider in self.providers.lock().entries.values() { provider.value.roll_step(now_ms, closing); }
    }
    async fn sample_step(&self) -> Result<CatalogRows<CatalogMetric>> {
        let (providers, _provider_charge) = self.provider_snapshot()?;
        let mut provided = Vec::with_capacity(providers.len());
        for provider in providers { provided.push(provider.sample_step().await?); }
        self.metric_rows(&provided, true)
    }
}
fn metric_bytes(metric: &CatalogMetric) -> Result<usize> {
    metric.tags().iter().try_fold(metric.name().len().checked_add(512).ok_or_else(overflow)?, |bytes, (key, value)| bytes.checked_add(key.len()).and_then(|bytes| bytes.checked_add(value.len())).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow))
}
fn overflow() -> Anomaly { Anomaly::incorrect("rtdb.metrics/allocation-overflow", "Metric allocation exceeds addressable memory").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded) }
fn wall_time_millis() -> i64 { match SystemTime::now().duration_since(UNIX_EPOCH) { Ok(elapsed) => elapsed.as_millis().min(i64::MAX as u128) as i64, Err(error) => -(error.duration().as_millis().min(i64::MAX as u128) as i64) } }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registry_initialization_and_sampling_fit_a_small_worker_stack() {
        std::thread::Builder::new().stack_size(128 << 10).spawn(|| {
            let memory = MemoryBudget::new(1 << 22);
            let registry = Registry::new("embedded", Arc::clone(&memory)).unwrap();
            registry.record_duration(ExecutionTimer::Query, Duration::from_millis(1));
            assert!(registry.metrics().unwrap().rows().iter().any(|row| matches!(row, CatalogMetric::Timer { count: 1, .. })));
            drop(registry);
            assert_eq!(memory.used_bytes(), 0);
        }).unwrap().join().unwrap();
    }
    #[test]
    fn durations_recorded_across_threads_report_as_one_serial_timer() {
        let registry = Registry::new("node-1", MemoryBudget::new(1 << 24)).unwrap();
        let now = wall_time_millis();
        let mut serial = histogram::Timer::new(now);
        let durations: Vec<_> = (0..4 * SAMPLE_SHARDS * SHARD_SAMPLES).map(|index| Duration::from_nanos(1024 + (index as u64 * 37) % 1024)).collect();
        for elapsed in &durations { serial.record(*elapsed, now); }
        std::thread::scope(|scope| {
            for chunk in durations.chunks(durations.len() / 24 + 1) {
                let registry = &registry;
                scope.spawn(move || for elapsed in chunk { registry.record_duration(ExecutionTimer::Query, *elapsed); });
            }
        });
        let expected = serial.snapshot(wall_time_millis());
        let rows = registry.metrics().unwrap();
        let Some(CatalogMetric::Timer { count, total_ns, percentiles_ns, max_ns, mean_ns, .. }) = rows.rows().iter().find(|row| row.name() == "query.timer") else { panic!("query timer"); };
        assert_eq!((*count, *total_ns, *max_ns, *mean_ns), (expected.count, expected.total, Some(expected.maximum), Some(expected.mean)));
        assert_eq!(percentiles_ns.as_slice(), expected.percentiles.as_slice());
        assert!(registry.shards.iter().all(|shard| { let pending = shard.0.lock(); pending.is_empty() && pending.capacity() == SHARD_SAMPLES }));
    }
    #[test]
    fn query_warnings_are_counted_from_zero() {
        let registry = Registry::new("node-1", MemoryBudget::new(1 << 22)).unwrap();
        let warnings = |registry: &Registry| registry.metrics().unwrap().rows().iter().find_map(|row| match row { CatalogMetric::Counter { name, count, .. } if name == "query.warning" => Some(*count), _ => None });
        assert_eq!(warnings(&registry), Some(0.0));
        registry.record_query_warning();
        registry.record_query_warning();
        assert_eq!(warnings(&registry), Some(2.0));
    }
    #[test]
    fn follower_records_are_timed_per_database_and_message_type_under_the_reference_names() {
        let memory = MemoryBudget::new(1 << 24);
        let registry = Registry::new("node-1", Arc::clone(&memory)).unwrap();
        assert!(!registry.metrics().unwrap().rows().iter().any(|row| row.name().starts_with("xtdb.replica.")), "a database is listed from its first record, as the reference registers on its follower's start");
        registry.record_replica_process("xtdb", ReplicaRecordKind::TriesAdded, Duration::from_micros(5));
        registry.record_replica_block_buffer("xtdb", Duration::from_millis(3), 12);
        let rows = registry.metrics().unwrap();
        let tagged = |row: &CatalogMetric, kind: Option<&str>| row.tags().get("db").map(String::as_str) == Some("xtdb") && row.tags().get("msg.type").map(String::as_str) == kind;
        assert!(rows.rows().iter().any(|row| matches!(row, CatalogMetric::Timer { name, count: 1, .. } if name == "xtdb.replica.process.timer") && tagged(row, Some("TriesAdded"))));
        assert!(rows.rows().iter().any(|row| matches!(row, CatalogMetric::Timer { name, count: 0, .. } if name == "xtdb.replica.process.timer") && tagged(row, Some("ResolvedTx"))));
        assert!(rows.rows().iter().any(|row| matches!(row, CatalogMetric::Timer { name, count: 1, .. } if name == "xtdb.replica.block.buffer.timer") && tagged(row, None)));
        assert!(rows.rows().iter().any(|row| matches!(row, CatalogMetric::DistributionSummary { name, count: 1, total, .. } if name == "xtdb.replica.block.buffered.records" && *total == 12.0)));
        drop(rows);
        drop(registry);
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn the_processor_count_is_a_gauge() {
        let registry = Registry::new("node-1", MemoryBudget::new(1 << 22)).unwrap();
        assert!(registry.metrics().unwrap().rows().iter().any(|row| matches!(row, CatalogMetric::Gauge { name, value, .. } if name == "system.cpu.count" && *value >= 1.0)));
    }
    #[test]
    fn processor_gauges_are_listed_with_micrometer_names() {
        let registry = Registry::new("node-1", MemoryBudget::new(1 << 22)).unwrap();
        let rows = registry.metrics().unwrap();
        for expected in ["system.cpu.usage", "process.cpu.usage", "system.load.average.1m"] {
            assert!(rows.rows().iter().any(|row| matches!(row, CatalogMetric::Gauge { name, unit: None, .. } if name == expected)), "{expected}");
        }
    }
    #[test]
    fn reference_timers_are_listed_before_their_first_observation() {
        let registry = Registry::new("node-1", MemoryBudget::new(1 << 22)).unwrap();
        let rows = registry.metrics().unwrap();
        for expected in ["query.timer", "tx.op.timer", "compactor.job.timer", "block.upload.timer"] {
            assert!(rows.rows().iter().any(|row| matches!(row, CatalogMetric::Timer { name, count: 0, .. } if name == expected)), "{expected}");
        }
        drop(rows);
        registry.record_duration(ExecutionTimer::CompactorJob, Duration::from_nanos(10));
        assert!(registry.metrics().unwrap().rows().iter().any(|row| matches!(row, CatalogMetric::Timer { name, count: 1, .. } if name == "compactor.job.timer")));
    }
    #[test]
    fn the_collector_reports_trie_deletes_under_the_upstream_timer() {
        let registry = Registry::new("node-1", MemoryBudget::new(1 << 22)).unwrap();
        for expected in ["xtdb.gc.tries.delete.timer", "xtdb.gc.block_files.delete.timer", "xtdb.gc.table_block_files.delete.timer", "xtdb.gc.offloaded_payloads.delete.timer"] {
            assert!(registry.metrics().unwrap().rows().iter().any(|row| matches!(row, CatalogMetric::Timer { name, count: 0, .. } if name == expected)), "{expected}");
        }
        CollectionObserver::trie_deleted(registry.as_ref(), Duration::from_nanos(10));
        CollectionObserver::block_file_deleted(registry.as_ref(), Duration::from_nanos(20));
        CollectionObserver::table_block_file_deleted(registry.as_ref(), Duration::from_nanos(30));
        CollectionObserver::offloaded_payload_deleted(registry.as_ref(), Duration::from_nanos(40));
        let rows = registry.metrics().unwrap();
        for expected in ["xtdb.gc.tries.delete.timer", "xtdb.gc.block_files.delete.timer", "xtdb.gc.table_block_files.delete.timer", "xtdb.gc.offloaded_payloads.delete.timer"] {
            assert!(rows.rows().iter().any(|row| matches!(row, CatalogMetric::Timer { name, count: 1, .. } if name == expected)), "{expected}");
        }
    }
    #[tokio::test]
    async fn closing_step_preserves_cumulative_catalog_totals_and_timer_percentiles() {
        let memory = MemoryBudget::new(1 << 22);
        let registry = Registry::new("fixture", memory).unwrap();
        registry.record_error(ExecutionError::Transaction);
        registry.record_duration(ExecutionTimer::Query, Duration::from_nanos(500));
        registry.roll_step(wall_time_millis(), true);
        let step = registry.sample_step().await.unwrap();
        assert!(step.rows().iter().any(|row| matches!(row, CatalogMetric::Counter { name, count, .. } if name == "tx.error" && *count == 1.0)));
        assert!(step.rows().iter().any(|row| matches!(row, CatalogMetric::Timer { name, count: 1, total_ns: 500.0, percentiles_ns, .. } if name == "query.timer" && percentiles_ns[0] > 0.0)));
        registry.record_error(ExecutionError::Transaction);
        assert!(registry.metrics().unwrap().rows().iter().any(|row| matches!(row, CatalogMetric::Counter { name, count, .. } if name == "tx.error" && *count == 2.0)));
        assert!(registry.sample_step().await.unwrap().rows().iter().any(|row| matches!(row, CatalogMetric::Counter { name, count, .. } if name == "tx.error" && *count == 1.0)));
    }
    #[test]
    fn connection_metrics_share_counters_and_snapshot_retains_charge() {
        let memory = MemoryBudget::new(1 << 22);
        let registry = Registry::new("node-1", Arc::clone(&memory)).unwrap();
        registry.record_duration(ExecutionTimer::Query, Duration::from_nanos(25));
        registry.record_error(ExecutionError::Transaction); registry.record_error(ExecutionError::Transaction);
        let baseline = memory.used_bytes();
        let rows = registry.metrics().unwrap();
        assert!(rows.rows().iter().any(|metric| matches!(metric, CatalogMetric::Counter { name, count, .. } if name == "tx.error" && *count == 2.0)));
        assert!(rows.rows().iter().any(|metric| matches!(metric, CatalogMetric::Timer { name, count: 1, mean_ns: Some(25), .. } if name == "query.timer")));
        let clone = rows.clone(); drop(rows); assert!(memory.used_bytes() > baseline); drop(clone); assert_eq!(memory.used_bytes(), baseline);
        drop(registry); assert_eq!(memory.used_bytes(), 0);
    }
    struct Fixed { memory: Arc<MemoryBudget> }
    impl CatalogMetrics for Fixed {
        fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> {
            let reservation = self.memory.reserve(1024)?;
            Ok(CatalogRows::new(vec![CatalogMetric::Gauge { name: "fixture.live".into(), tags: BTreeMap::new(), unit: None, value: 7.0 }], reservation))
        }
    }
    #[test]
    fn provider_registration_is_scoped_and_snapshots_keep_common_tags() {
        let memory = MemoryBudget::new(1 << 22);
        let registry = Registry::new("node", Arc::clone(&memory)).unwrap();
        let guard = registry.register_provider(Arc::new(Fixed { memory })).unwrap();
        let duplicate = registry.register_provider(Arc::new(Fixed { memory: Arc::clone(&registry.memory) })).unwrap();
        let snapshot = registry.metrics().unwrap();
        assert_eq!(snapshot.rows().iter().filter(|metric| metric.name() == "fixture.live").count(), 1);
        assert!(snapshot.rows().iter().all(|metric| metric.tags().get("node-id").is_some_and(|value| value == "node")));
        drop(guard); drop(duplicate);
        assert!(!registry.metrics().unwrap().rows().iter().any(|metric| metric.name() == "fixture.live"));
        assert!(snapshot.rows().iter().any(|metric| metric.name() == "fixture.live"));
        registry.connection_event(ConnectionEvent::Opened); registry.connection_event(ConnectionEvent::Cancelled); registry.connection_event(ConnectionEvent::Refused);
        assert_eq!(registry.metrics().unwrap().rows().iter().filter(|metric| matches!(metric, CatalogMetric::Counter { name, count, .. } if name.starts_with("pgwire.") && *count == 1.0)).count(), 3);
    }
    struct PendingProvider { memory: Arc<MemoryBudget>, dropped: Arc<std::sync::atomic::AtomicUsize> }
    struct SampleGuard(Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for SampleGuard { fn drop(&mut self) { self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst); } }
    #[async_trait::async_trait]
    impl CatalogMetrics for PendingProvider {
        fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { Ok(CatalogRows::new(Vec::new(), self.memory.reserve(0)?)) }
        async fn sample(&self) -> Result<CatalogRows<CatalogMetric>> {
            let _guard = SampleGuard(Arc::clone(&self.dropped));
            std::future::pending().await
        }
    }
    #[tokio::test]
    async fn cancelled_registry_sample_drops_provider_io_and_all_temporary_charges() {
        let memory = MemoryBudget::new(1 << 22);
        let registry = Registry::new("node", Arc::clone(&memory)).unwrap();
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let registration = registry.register_provider(Arc::new(PendingProvider { memory: Arc::clone(&memory), dropped: Arc::clone(&dropped) })).unwrap();
        let baseline = memory.used_bytes();
        let mut sample = Box::pin(registry.sample());
        tokio::select! { biased; _ = &mut sample => panic!("pending provider completed"), _ = std::future::ready(()) => {} }
        assert!(memory.used_bytes() > baseline);
        drop(sample);
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(memory.used_bytes(), baseline);
        drop(registration);
    }
}
