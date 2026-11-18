use std::{fmt, ops::Deref, sync::Arc, time::Duration};
use rdkafka::{Message, Offset, TopicPartitionList, client::ClientContext, consumer::{BaseConsumer, Consumer, ConsumerContext}, error::KafkaError, message::{Headers, Timestamp as KafkaTimestamp}, statistics::Statistics};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_replication::{DriverStatus, TermHandle};
use rtdb_types::{AllocationScope, Anomaly, Result};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use super::{ConnectConfig, ConversionContext, Converter, DateEnvironment, PluginRegistry, RecordIndexer, ResumeToken, SinkRecord, SourceConfig, SourceTransactions, error};
use crate::KafkaClusterConfig;
mod native_account;

struct ConsumerConfiguration {
    client: rdkafka::ClientConfig,
    _providers: Option<AllocationScope>,
}

impl Deref for ConsumerConfiguration {
    type Target = rdkafka::ClientConfig;

    fn deref(&self) -> &Self::Target { &self.client }
}

impl fmt::Debug for ConsumerConfiguration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ConsumerConfiguration").finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct SourceOptions {
    pub partition: i32,
    pub maximum_record_bytes: usize,
    pub maximum_batch_bytes: usize,
    pub maximum_batch_records: usize,
    pub maximum_decoded_bytes: usize,
    pub poll_interval: Duration,
    pub request_timeout: Duration,
}
impl Default for SourceOptions {
    fn default() -> Self { Self { partition: 0, maximum_record_bytes: 1024 * 1024, maximum_batch_bytes: 8 * 1024 * 1024, maximum_batch_records: 500,
        maximum_decoded_bytes: 128 * 1024 * 1024, poll_interval: Duration::from_millis(10), request_timeout: Duration::from_secs(30) } }
}
impl SourceOptions {
    pub fn validate(&self) -> Result<()> {
        if self.partition < 0 || self.maximum_record_bytes == 0 || self.maximum_batch_bytes < self.maximum_record_bytes || self.maximum_batch_records == 0
            || self.maximum_decoded_bytes == 0 || self.poll_interval.is_zero() || self.request_timeout.is_zero() {
            return Err(error("invalid-config", "Kafka Connect source limits must be positive and fit a record"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct SourceStatus {
    pub connected: bool,
    pub records: u64,
    pub current_offset: Option<i64>,
    pub last_event_epoch_seconds: Option<i64>,
    pub last_commit_lag_seconds: Option<f64>,
    pub records_lag: Option<i64>,
}
impl Default for SourceStatus {
    fn default() -> Self { Self { connected: false, records: 0, current_offset: None, last_event_epoch_seconds: None, last_commit_lag_seconds: None, records_lag: None } }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceIdentity { pub database: String, pub topic: String, pub partition: i32 }
pub trait SourceRegistration: Send {
    /// Records the retained batch's row count after successful indexing, including rows without timestamps.
    fn recorded(&self, count: u64);
    /// Records a retained record's timestamp lag after its indexer call succeeds.
    /// The value is a signed millisecond timestamp difference divided by 1000; callbacks must not panic or retain record payloads.
    fn committed(&self, lag_seconds: f64);
}
pub trait SourceObserver: Send + Sync {
    /// Registers a caller-readable status monitor; dropping the returned guard removes the registration.
    /// Registration must not spawn tasks that outlive the guard.
    fn register(&self, identity: &SourceIdentity, status: watch::Receiver<SourceStatus>) -> Result<Box<dyn SourceRegistration>>;
}
struct SourceProgress(watch::Sender<SourceStatus>);
struct SourceContext { progress: Arc<SourceProgress>, topic: String, partition: i32 }
impl ClientContext for SourceContext {
    fn stats(&self, statistics: Statistics) {
        let lag = statistics.topics.get(&self.topic).and_then(|topic| topic.partitions.get(&self.partition))
            .filter(|partition| partition.app_offset >= 0 && partition.hi_offset >= partition.app_offset).map(|partition| partition.hi_offset - partition.app_offset);
        self.progress.0.send_modify(|state| state.records_lag = lag);
    }
}
impl ConsumerContext for SourceContext {}
struct ConnectionGuard(Arc<SourceProgress>);
impl Drop for ConnectionGuard { fn drop(&mut self) { self.0.0.send_modify(|state| state.connected = false); } }
pub struct SourceDriver {
    cluster: KafkaClusterConfig,
    source: SourceConfig,
    config: ConnectConfig,
    options: SourceOptions,
    plugins: PluginRegistry,
    indexer: Box<dyn RecordIndexer>,
    memory: Arc<MemoryBudget>,
    environment: Arc<DateEnvironment>,
    progress: Arc<SourceProgress>,
    metrics: Option<(SourceIdentity, Arc<dyn SourceObserver>)>,
}
impl SourceDriver {
    /// The assignment owner must await `run`; all converter, consumer, and transform state ends with that future.
    pub fn new(cluster: KafkaClusterConfig, source: SourceConfig, options: SourceOptions, plugins: PluginRegistry, indexer: Box<dyn RecordIndexer>, memory: Arc<MemoryBudget>, environment: Arc<DateEnvironment>) -> Result<Self> {
        options.validate()?;
        let config = source.validate()?;
        Ok(Self { cluster, source, config, options, plugins, indexer, memory, environment, progress: Arc::new(SourceProgress(watch::channel(SourceStatus::default()).0)), metrics: None })
    }
    pub fn with_observer(mut self, database: String, observer: Arc<dyn SourceObserver>) -> Self {
        self.metrics = Some((SourceIdentity { database, topic: self.source.topic.clone(), partition: self.options.partition }, observer)); self
    }
    pub fn subscribe(&self) -> watch::Receiver<SourceStatus> { self.progress.0.subscribe() }
    pub async fn run(mut self, term: TermHandle, after: Option<Arc<[u8]>>, _blocks: watch::Receiver<DriverStatus>, cancellation: CancellationToken) -> Result<()> {
        if cancellation.is_cancelled() { return Ok(()); }
        let token = after.as_deref().map(ResumeToken::decode).transpose()?;
        let configuration_bytes = self.source.connect_config.iter().try_fold(0usize, |size, (key, value)| size.checked_add(key.len()).and_then(|size| size.checked_add(value.len())).ok_or_else(overflow))?;
        let plugin_charge = self.memory.reserve(configuration_bytes.checked_mul(512).and_then(|size| size.checked_add(65_536)).ok_or_else(overflow)?)?;
        let mut converters = Converters::open(&self.plugins, &self.config).await?;
        let result = {
            let work = self.ingest(&term, token, &mut converters, &plugin_charge);
            tokio::pin!(work);
            tokio::select! { result = &mut work => result, _ = cancellation.cancelled() => Ok(()) }
        };
        after_close(result, converters.close().await)
    }
    fn consumer_configuration(&mut self) -> Result<ConsumerConfiguration> {
        use crate::config::{providers, signed_integer};

        let mut captured = self.cluster.capture_client_properties(&self.memory)?;
        let properties = &mut captured.values;
        for key in ["key.deserializer", "value.deserializer", "enable.auto.commit"] { properties.remove(key); }
        if let Some(scope) = captured.allocation.as_ref() {
            if let std::borrow::Cow::Owned(resolved) = providers::resolve(properties, &self.memory, &mut |bytes| scope.reserve(bytes))? {
                *properties = resolved;
            }
        }
        if let Some(maximum) = properties.remove("max.poll.records") {
            let maximum = signed_integer(&maximum).and_then(|value| i32::try_from(value).ok()).filter(|value| *value > 0)
                .ok_or_else(|| error("invalid-config", "max.poll.records must be a positive signed integer"))?;
            self.options.maximum_batch_records = (maximum as usize).min(self.options.maximum_batch_records);
        }
        properties.retain(|name, _| !providers::control_property(name));
        let reset = properties.entry("auto.offset.reset".into()).or_insert_with(|| "error".into());
        if reset == "none" { *reset = "error".into(); }
        properties.insert("enable.auto.commit".into(), "false".into());
        properties.insert("enable.auto.offset.store".into(), "false".into());
        properties.insert("enable.partition.eof".into(), "true".into());
        properties.entry("statistics.interval.ms".into()).or_insert_with(|| "1000".into());
        properties.entry("group.id".into()).or_insert_with(|| "rtdb-kafka-connect-explicit-assignment".into());
        properties.insert("queued.max.messages.kbytes".into(), self.options.maximum_batch_bytes.div_ceil(1024).to_string());
        properties.insert("fetch.message.max.bytes".into(), self.options.maximum_record_bytes.to_string());
        if let Some(scope) = captured.allocation.as_ref() {
            let bound = properties.iter().try_fold(4096usize, |bytes, (name, value)| {
                bytes.checked_add(name.len())?.checked_add(value.len())?.checked_add(256)
            }).and_then(|bytes| bytes.checked_mul(8)).ok_or_else(overflow)?;
            scope.reserve(bound)?;
        }
        let mut client = crate::client::prepare(properties, &self.memory)?;
        native_account::install(&mut client, &self.memory)?;
        Ok(ConsumerConfiguration { client, _providers: captured.allocation })
    }
    async fn ingest(&mut self, term: &TermHandle, after: Option<ResumeToken>, converters: &mut Converters, plugin_charge: &Arc<MemoryReservation>) -> Result<()> {
        let registration = self.metrics.as_ref().map(|(identity, observer)| observer.register(identity, self.subscribe())).transpose()?;
        let configuration = self.consumer_configuration()?;
        let consumer: BaseConsumer<SourceContext> = configuration.create_with_context(SourceContext { progress: Arc::clone(&self.progress), topic: self.source.topic.clone(), partition: self.options.partition }).map_err(transport)?;
        let (low, high) = consumer.fetch_watermarks(&self.source.topic, self.options.partition, self.options.request_timeout).map_err(transport)?;
        let next = after.map(|token| token.0.checked_add(1).ok_or_else(|| error("invalid-token", "Kafka offset exhausted"))).transpose()?.unwrap_or(low);
        if next < low || next > high { return Err(error("offset-out-of-range", format!("Resume offset {next} is outside retained range [{low}, {high}]"))); }
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition_offset(&self.source.topic, self.options.partition, Offset::Offset(next)).map_err(transport)?;
        consumer.assign(&assignment).map_err(transport)?;
        let mut chain = self.plugins.chain_with_environment(&self.config, Arc::clone(&self.environment))?;
        self.progress.0.send_modify(|state| state.connected = true);
        let _connected = ConnectionGuard(Arc::clone(&self.progress));
        let mut transactions = SourceTransactions { term };
        let raw_bound = self.options.maximum_batch_bytes.checked_add(self.options.maximum_batch_records.checked_mul(1024).ok_or_else(overflow)?).ok_or_else(overflow)?;
        loop {
            let raw_charge = self.memory.reserve(raw_bound)?;
            let mut raw = Vec::new(); let mut bytes = 0usize;
            while raw.len() < self.options.maximum_batch_records && bytes.saturating_add(self.options.maximum_record_bytes) <= self.options.maximum_batch_bytes {
                match consumer.poll(Duration::ZERO) {
                    Some(Ok(message)) => {
                        if message.topic() != self.source.topic || message.partition() != self.options.partition { return Err(error("wrong-partition", "Consumer returned an unassigned partition")); }
                        let headers = message.headers();
                        let size = message.key().map_or(0, <[u8]>::len).saturating_add(message.payload().map_or(0, <[u8]>::len))
                            .saturating_add(headers.map_or(0, |headers| headers.iter().map(|header| header.key.len().saturating_add(header.value.map_or(0, <[u8]>::len)).saturating_add(128)).sum::<usize>()));
                        if size > self.options.maximum_record_bytes { return Err(error("record-too-large", "Kafka Connect record exceeds its allocation limit")); }
                        bytes += size;
                        raw.push(RawRecord { offset: message.offset(), timestamp: Some(kafka_timestamp(message.timestamp())),
                            key: message.key().map(<[u8]>::to_vec), value: message.payload().map(<[u8]>::to_vec), headers: headers.map_or(Vec::new(), |headers| headers.iter().map(|header| (header.key.into(), header.value.map(<[u8]>::to_vec))).collect()) });
                    }
                    Some(Err(KafkaError::PartitionEOF(_))) | None => break,
                    Some(Err(cause)) => return Err(transport(cause)),
                }
            }
            if raw.is_empty() { drop(raw_charge); tokio::time::sleep(self.options.poll_interval).await; continue; }
            let decoded_bound = bytes.checked_mul(512).and_then(|bytes| bytes.checked_add(raw.len().checked_mul(65_536)?)).ok_or_else(overflow)?;
            if decoded_bound > self.options.maximum_decoded_bytes { return Err(error("batch-too-large", "Decoded Kafka Connect batch exceeds its allocation limit")); }
            let last_offset = raw.last().map(|record| record.offset);
            let mut records = Vec::new();
            for raw in raw {
                let mut context = ConversionContext::with_limit(Arc::clone(&self.memory), self.options.maximum_decoded_bytes);
                context.retain(Arc::clone(plugin_charge))?;
                let mut record = SinkRecord::new(self.source.topic.clone(), self.options.partition, raw.offset,
                    converters.key.decode(&self.source.topic, raw.key.as_deref(), &mut context).await?, converters.value.decode(&self.source.topic, raw.value.as_deref(), &mut context).await?);
                record.timestamp_millis = raw.timestamp;
                record.headers = raw.headers.into_iter().map(|(name, bytes)| {
                    context.reserve(bytes.as_ref().map_or(0, Vec::len).checked_mul(512).and_then(|size| size.checked_add(4096)).ok_or_else(overflow)?)?;
                    Ok((name, super::header::decode_with_environment(bytes.as_deref(), &self.environment)?))
                }).collect::<Result<_>>()?;
                record.reservations = context.into_charges();
                if let Some(record) = chain.apply(record)? { records.push(record); }
            }
            let retained = records.iter().try_fold(0usize, |size, record| size.checked_add(record.retained_bytes()?).ok_or_else(overflow))?;
            if retained > self.options.maximum_decoded_bytes { return Err(error("batch-too-large", "Converted records exceed their retained allocation limit")); }
            drop(raw_charge);
            let count = records.len() as u64;
            let last_timestamp = records.last().and_then(|record| record.timestamp_millis).filter(|timestamp| *timestamp > 0);
            let timestamp_charge = self.memory.reserve(records.len().checked_mul(std::mem::size_of::<i64>()).and_then(|bytes| bytes.checked_add(64)).ok_or_else(overflow)?)?;
            let mut timestamps = Vec::with_capacity(records.len());
            timestamps.extend(records.iter().filter_map(|record| record.timestamp_millis).filter(|timestamp| *timestamp > 0));
            if !records.is_empty() { self.indexer.index_records(records, &mut transactions).await?; }
            let now_ms = chrono::Utc::now().timestamp_millis();
            if let Some(registration) = &registration {
                registration.recorded(count);
                for timestamp in &timestamps { registration.committed(now_ms.wrapping_sub(*timestamp) as f64 / 1000.0); }
            }
            self.progress.0.send_modify(|state| {
                state.records = state.records.saturating_add(count); state.current_offset = last_offset;
                if let Some(timestamp) = last_timestamp { state.last_event_epoch_seconds = Some(timestamp / 1000); state.last_commit_lag_seconds = Some(now_ms.wrapping_sub(timestamp) as f64 / 1000.0); }
            });
            drop(timestamp_charge);
        }
    }
}
struct Converters { key: Box<dyn Converter>, value: Box<dyn Converter> }
impl Converters {
    async fn open(plugins: &PluginRegistry, config: &ConnectConfig) -> Result<Self> {
        let mut key = plugins.converter(&config.key, true)?;
        match plugins.converter(&config.value, false) {
            Ok(value) => Ok(Self { key, value }),
            Err(error) => after_close(Err(error), key.close().await),
        }
    }
    async fn close(mut self) -> Result<()> {
        let value = self.value.close().await;
        after_close(value, self.key.close().await)
    }
}
fn after_close<T>(result: Result<T>, close: Result<()>) -> Result<T> {
    match (result, close) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(close)) => Err(error.with_context("converter-close-error", close.to_string().into())),
    }
}
struct RawRecord { offset: i64, timestamp: Option<i64>, key: Option<Vec<u8>>, value: Option<Vec<u8>>, headers: Vec<(String, Option<Vec<u8>>)> }
fn kafka_timestamp(timestamp: KafkaTimestamp) -> i64 {
    match timestamp { KafkaTimestamp::NotAvailable => -1, KafkaTimestamp::CreateTime(value) | KafkaTimestamp::LogAppendTime(value) => value }
}
fn transport(cause: impl std::fmt::Display) -> Anomaly { Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "xtdb.kafka-connect-source/kafka", cause.to_string()) }
fn overflow() -> Anomaly { error("allocation-overflow", "Kafka Connect allocation size overflow") }

#[cfg(test)]
mod client_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unavailable_kafka_timestamp_remains_minus_one_millisecond_for_system_time_smoothing() {
        assert_eq!(kafka_timestamp(KafkaTimestamp::NotAvailable), -1);
        assert_eq!(kafka_timestamp(KafkaTimestamp::CreateTime(123)), 123);
        assert_eq!(kafka_timestamp(KafkaTimestamp::LogAppendTime(456)), 456);
    }
}

#[cfg(test)]
mod converter_lifetime_tests {
    use super::*;
    use std::{collections::BTreeMap, sync::Mutex};
    use super::super::{ConnectData, ConverterFactory};

    struct Factory { events: Arc<Mutex<Vec<String>>>, fail_value_open: bool, fail_value_close: bool }
    struct Opened { events: Arc<Mutex<Vec<String>>>, role: &'static str, fail_close: bool }
    impl ConverterFactory for Factory {
        fn open(&self, _: &BTreeMap<String, String>, is_key: bool) -> Result<Box<dyn Converter>> {
            if !is_key && self.fail_value_open { return Err(error("value-open", "value open failed")); }
            Ok(Box::new(Opened { events: Arc::clone(&self.events), role: if is_key { "key" } else { "value" }, fail_close: !is_key && self.fail_value_close }))
        }
    }
    #[async_trait::async_trait]
    impl Converter for Opened {
        async fn decode(&mut self, _: &str, _: Option<&[u8]>, _: &mut ConversionContext) -> Result<ConnectData> { Ok(ConnectData::null()) }
        async fn close(&mut self) -> Result<()> {
            tokio::task::yield_now().await;
            self.events.lock().unwrap().push(self.role.into());
            if self.fail_close { Err(error("value-close", "value close failed")) } else { Ok(()) }
        }
    }
    fn config() -> ConnectConfig {
        ConnectConfig::parse(&BTreeMap::from([("key.converter".into(), "owned".into()), ("value.converter".into(), "owned".into())])).unwrap()
    }
    #[tokio::test]
    async fn failed_value_open_awaits_the_already_opened_key_close() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let plugins = PluginRegistry::default().with_converter("owned".into(), Arc::new(Factory { events: Arc::clone(&events), fail_value_open: true, fail_value_close: false })).unwrap();
        let result = Converters::open(&plugins, &config()).await;
        assert!(matches!(result, Err(error) if error.code.ends_with("value-open")));
        assert_eq!(*events.lock().unwrap(), ["key"]);
    }
    #[tokio::test]
    async fn both_converters_close_after_an_error_and_the_decode_error_remains_primary() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let plugins = PluginRegistry::default().with_converter("owned".into(), Arc::new(Factory { events: Arc::clone(&events), fail_value_open: false, fail_value_close: true })).unwrap();
        let converters = Converters::open(&plugins, &config()).await.unwrap();
        let result: Result<()> = after_close(Err(error("decode", "decode failed")), converters.close().await);
        assert!(result.unwrap_err().code.ends_with("decode"));
        assert_eq!(*events.lock().unwrap(), ["value", "key"]);
    }
}
