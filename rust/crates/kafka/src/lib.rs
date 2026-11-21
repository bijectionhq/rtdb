mod config;
mod client;
mod driver;
mod log_configuration;
mod tail;
mod tls;
pub mod connect;

pub use config::{KafkaClusterConfig, KafkaLogConfig, KafkaLogOptions, native_properties, parse_properties, producer_properties};
pub use driver::KafkaLogDriver;

use std::collections::BTreeMap;
use std::sync::{Arc, atomic::{AtomicBool, AtomicI64, Ordering}};
use async_trait::async_trait;
use bytes::Bytes;
use rtdb_columnar::MemoryBudget;
use rdkafka::{ClientConfig, Message, admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication}, client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer}, error::{KafkaError, RDKafkaErrorCode}, message::BorrowedMessage};
use rtdb_storage::{Error, Result as StorageResult, log::{AppendReceipt, Log, LogMetadata, LogOffset, LogRecord, LogTail}};
use rtdb_types::{Anomaly, Result, Timestamp};
use tokio::sync::{Semaphore, mpsc, oneshot};
use log_configuration::{ConfiguredLogClient, LogConfiguration, PreparedLogClient, Role};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KafkaWarning {
    AcknowledgementsOverridden { configured: String },
    IdempotenceDisabled,
    InsufficientReplicas { topic: String, replicas: usize, minimum_in_sync: i32 },
    UncleanElection { topic: String },
    ConfigurationUnavailable { topic: String },
}

pub trait KafkaObserver: Send + Sync { fn warning(&self, warning: KafkaWarning); }

struct Progress { observed_tip: AtomicI64, closed: AtomicBool }
impl Progress {
    fn new(tip: i64) -> Self { Self { observed_tip: AtomicI64::new(tip), closed: AtomicBool::new(false) } }
    fn check(&self) -> StorageResult<()> { if self.closed.load(Ordering::Acquire) { Err(Error::Closed("Kafka driver ended".into())) } else { Ok(()) } }
    fn advance(&self, offset: i64) { self.observed_tip.fetch_max(offset, Ordering::AcqRel); }
}

enum Access {
    ReadOnly,
    Writable { queue: mpsc::Sender<driver::Queued>, bytes: Arc<Semaphore>, records: Arc<Semaphore> },
}

pub struct KafkaLog { configuration: LogConfiguration, options: KafkaLogOptions, progress: Arc<Progress>, access: Access }
pub struct OpenedKafkaLog { pub log: Arc<KafkaLog>, pub driver: Option<KafkaLogDriver> }

impl KafkaLog {
    /// The owner must run the returned driver, then cancel and join it before releasing a writable log.
    /// TLS material is admitted against `memory` and remains charged until the last derived native client is destroyed.
    pub async fn open(cluster: &KafkaClusterConfig, options: KafkaLogOptions, observer: Arc<dyn KafkaObserver>, memory: Arc<MemoryBudget>) -> Result<OpenedKafkaLog> {
        options.validate()?;
        let configuration = LogConfiguration::from_cluster(cluster, memory)?;
        let admin_config = configuration.prepare(Role::Admin)?;
        if admin_config.configuration.get("transactional.id").is_some() { return Err(Anomaly::incorrect("xtdb.kafka/invalid-config", "XTDB logs do not open Kafka producer transactions")); }
        ensure_topic(admin_config, &options, observer.as_ref()).await?;
        let consumer = consumer(&configuration, &options).map_err(Anomaly::from)?;
        let (_, end) = consumer.fetch_watermarks(&options.topic, 0, options.request_timeout).map_err(|error| Anomaly::from(transport_error(&options.topic, error)))?;
        let progress = Arc::new(Progress::new(end - 1));
        let (access, driver) = if options.read_only { (Access::ReadOnly, None) } else {
            if let Some(configured) = configuration.overridden_acks() { observer.warning(KafkaWarning::AcknowledgementsOverridden { configured: configured.into() }); }
            let producer = configuration.prepare(Role::Producer)?.open(|configuration| {
                producer_config(&configuration, &options, observer.as_ref()).create_with_context(driver::DeliveryContext { progress: Arc::clone(&progress), topic: options.topic.clone() })
            })
                .map_err(|error| Anomaly::from(transport_error(&options.topic, error)))?;
            let (queue, receiver) = mpsc::channel(options.queue_capacity);
            let bytes = Arc::new(Semaphore::new(options.queued_bytes));
            let records = Arc::new(Semaphore::new(options.queue_capacity));
            let access = Access::Writable { queue, bytes: Arc::clone(&bytes), records: Arc::clone(&records) };
            (access, Some(KafkaLogDriver::new(producer, receiver, Arc::clone(&progress), options.clone(), bytes, records)))
        };
        Ok(OpenedKafkaLog { log: Arc::new(Self { configuration, options, progress, access }), driver })
    }
}

fn client_config(properties: &BTreeMap<String, String>) -> ClientConfig {
    let mut config = ClientConfig::new();
    for (key, value) in properties { config.set(key, value); }
    config
}

fn producer_config(configuration: &ClientConfig, options: &KafkaLogOptions, observer: &dyn KafkaObserver) -> ClientConfig {
    if let Some(value) = configuration.get("acks").filter(|value| *value != "all" && *value != "-1") {
        observer.warning(KafkaWarning::AcknowledgementsOverridden { configured: value.into() });
    }
    let properties = configuration.config_map().iter().map(|(name, value)| (name.clone(), value.clone())).collect();
    let mut properties = producer_properties(&properties, options.pipelined);
    if properties.get("enable.idempotence").is_some_and(|value| value == "false") { observer.warning(KafkaWarning::IdempotenceDisabled); }
    properties.insert("queue.buffering.max.messages".into(), options.queue_capacity.to_string());
    properties.insert("queue.buffering.max.kbytes".into(), options.queued_bytes.div_ceil(1024).to_string());
    properties.entry("message.max.bytes".into()).or_insert_with(|| options.maximum_message_bytes.saturating_add(1024).to_string());
    let mut result = configuration.clone();
    result.extend(properties);
    result
}

fn consumer(configuration: &LogConfiguration, options: &KafkaLogOptions) -> StorageResult<ConfiguredLogClient<Arc<BaseConsumer>>> {
    let prepared = configuration.prepare(Role::Consumer).map_err(|error| Error::Remote { key: options.topic.clone(), message: error.to_string() })?;
    prepared.open(|mut config| {
        config.set("group.id", "rtdb-explicit-assignment").set("enable.auto.commit", "false").set("enable.auto.offset.store", "false")
            .set("allow.auto.create.topics", "false")
            .set("isolation.level", "read_committed").set("auto.offset.reset", "error").set("enable.partition.eof", "true")
            .set("queued.max.messages.kbytes", options.maximum_poll_bytes.div_ceil(1024).to_string())
            .set("fetch.message.max.bytes", options.maximum_message_bytes.saturating_add(1024).to_string())
            .set("receive.message.max.bytes", options.maximum_poll_bytes.saturating_add(1024 * 1024).to_string());
        config.create::<BaseConsumer>().map(Arc::new)
    }).map_err(|error| transport_error(&options.topic, error))
}

fn transport_error(topic: &str, error: KafkaError) -> Error {
    let message = match error {
        KafkaError::ClientConfig(_, _, key, _) => format!("Invalid or unsupported Kafka property '{key}'"),
        error => error.to_string(),
    };
    Error::Remote { key: topic.into(), message }
}

fn metadata(message: &BorrowedMessage<'_>, topic: &str) -> StorageResult<LogMetadata> {
    if message.partition() != 0 || message.offset() < 0 { return Err(Error::Remote { key: topic.into(), message: "Kafka record has an invalid partition or offset".into() }); }
    let millis = message.timestamp().to_millis().ok_or_else(|| Error::Remote { key: topic.into(), message: "Kafka record has no timestamp".into() })?;
    Ok(LogMetadata { offset: LogOffset(message.offset()), timestamp: Timestamp(millis.checked_mul(1000).ok_or_else(|| Error::Remote { key: topic.into(), message: "Kafka timestamp exceeds microsecond range".into() })?) })
}

async fn ensure_topic(configuration: PreparedLogClient, options: &KafkaLogOptions, observer: &dyn KafkaObserver) -> Result<()> {
    let admin = configuration.open(|mut config| {
        config.set("allow.auto.create.topics", "false");
        config.create::<AdminClient<DefaultClientContext>>()
    }).map_err(|error| Anomaly::from(transport_error(&options.topic, error)))?;
    let mut metadata = admin.inner().fetch_metadata(Some(&options.topic), options.request_timeout).map_err(|error| Anomaly::from(transport_error(&options.topic, error)))?;
    let missing = metadata.topics().iter().find(|topic| topic.name() == options.topic).is_none_or(|topic| topic.error().is_some_and(|error| RDKafkaErrorCode::from(error) == RDKafkaErrorCode::UnknownTopicOrPartition));
    let admin_options = AdminOptions::new().request_timeout(Some(options.request_timeout)).operation_timeout(Some(options.request_timeout));
    if missing {
        if !options.auto_create_topic { return Err(Anomaly::incorrect("xtdb.kafka/missing-topic", format!("Kafka topic '{}' does not exist", options.topic))); }
        let topic = NewTopic::new(&options.topic, 1, TopicReplication::Fixed(1)).set("message.timestamp.type", "LogAppendTime");
        let results = admin.create_topics([&topic], &admin_options).await.map_err(|error| Anomaly::from(transport_error(&options.topic, error)))?;
        for result in results {
            match result { Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => (), Err((_, error)) => return Err(Anomaly::fault("xtdb.kafka/create-topic", error.to_string())) }
        }
        metadata = admin.inner().fetch_metadata(Some(&options.topic), options.request_timeout).map_err(|error| Anomaly::from(transport_error(&options.topic, error)))?;
    }
    let topic = metadata.topics().iter().find(|topic| topic.name() == options.topic).ok_or_else(|| Anomaly::fault("xtdb.kafka/missing-topic", "Kafka topic is absent after creation"))?;
    if let Some(error) = topic.error() { return Err(Anomaly::fault("xtdb.kafka/topic-metadata", format!("Kafka metadata error: {error:?}"))); }
    if topic.partitions().len() != 1 || topic.partitions()[0].id() != 0 { return Err(Anomaly::incorrect("xtdb.kafka/invalid-topic", "An XTDB Kafka log requires exactly one partition")); }
    let replicas = topic.partitions()[0].replicas().len();
    if replicas < 2 { return Ok(()); }
    match admin.describe_configs([&ResourceSpecifier::Topic(&options.topic)], &admin_options).await {
        Ok(resources) => for resource in resources {
            match resource {
                Ok(resource) => {
                    if let Some(minimum) = resource.get("min.insync.replicas").and_then(|entry| entry.value.as_deref()).and_then(|value| value.parse::<i32>().ok()).filter(|minimum| *minimum < 2) {
                        observer.warning(KafkaWarning::InsufficientReplicas { topic: options.topic.clone(), replicas, minimum_in_sync: minimum });
                    }
                    if resource.get("unclean.leader.election.enable").and_then(|entry| entry.value.as_deref()) == Some("true") { observer.warning(KafkaWarning::UncleanElection { topic: options.topic.clone() }); }
                }
                Err(_) => observer.warning(KafkaWarning::ConfigurationUnavailable { topic: options.topic.clone() }),
            }
        },
        Err(_) => observer.warning(KafkaWarning::ConfigurationUnavailable { topic: options.topic.clone() }),
    }
    Ok(())
}

#[async_trait]
impl Log for KafkaLog {
    async fn range(&self, from: LogOffset, to: LogOffset) -> StorageResult<Box<dyn rtdb_storage::log::LogRange>> {
        if from.0 < 0 || to < from { return Err(Error::UnavailableLogRange { requested: from.0, first_retained: 0 }); }
        let mut options = self.options.clone();
        options.maximum_poll_records = 1;
        let consumer = consumer(&self.configuration, &options)?;
        let (low, high) = consumer.fetch_watermarks(&options.topic, 0, options.request_timeout).map_err(|error| transport_error(&options.topic, error))?;
        if from.0 < low { return Err(Error::UnavailableLogRange { requested: from.0, first_retained: low }); }
        let tail = tail::KafkaTail::new(consumer, options, Arc::clone(&self.progress), LogOffset(from.0.min(high) - 1))?;
        let end = high.min(to.0);
        Ok(Box::new(tail::KafkaRange::new(tail, end, self.options.maximum_message_bytes, self.options.request_timeout)))
    }
    async fn enqueue(&self, payload: Bytes, _: Timestamp) -> StorageResult<AppendReceipt> {
        self.progress.check()?;
        let Access::Writable { queue, bytes, records } = &self.access else { return Err(Error::ReadOnlyLog(self.options.topic.clone().into())); };
        if payload.len() > self.options.maximum_message_bytes { return Err(Error::MessageTooLarge { size: payload.len(), maximum: self.options.maximum_message_bytes }); }
        let count = payload.len().max(1).checked_mul(2).and_then(|count| u32::try_from(count).ok()).ok_or_else(|| Error::MessageTooLarge { size: payload.len(), maximum: self.options.maximum_message_bytes })?;
        let byte_permit = Arc::clone(bytes).acquire_many_owned(count).await.map_err(|_| Error::Closed("Kafka allocation budget ended".into()))?;
        let record_permit = Arc::clone(records).acquire_owned().await.map_err(|_| Error::Closed("Kafka append capacity ended".into()))?;
        self.progress.check()?;
        let (reply, durable) = oneshot::channel();
        let (accepted, acceptance) = oneshot::channel();
        queue.send(driver::Queued { payload, accepted, completion: Box::new(driver::Delivery { reply, _bytes: byte_permit, _record: record_permit }) }).await.map_err(|_| Error::Closed("Kafka append driver ended".into()))?;
        acceptance.await.map_err(|_| Error::Closed("Kafka driver ended before fixing append order".into()))??;
        Ok(AppendReceipt::new(durable))
    }

    fn latest(&self) -> StorageResult<LogOffset> {
        self.progress.check()?;
        let consumer = consumer(&self.configuration, &self.options)?;
        let (_, end) = consumer.fetch_watermarks(&self.options.topic, 0, self.options.request_timeout).map_err(|error| transport_error(&self.options.topic, error))?;
        if end - 1 < self.progress.observed_tip.load(Ordering::Acquire) { return Err(Error::Remote { key: self.options.topic.clone(), message: "Kafka withdrew an observed log position".into() }); }
        self.progress.advance(end - 1);
        Ok(LogOffset(end - 1))
    }

    async fn retained_floor(&self) -> StorageResult<Option<LogOffset>> {
        self.progress.check()?;
        let consumer = consumer(&self.configuration, &self.options)?;
        let (low, _) = consumer.fetch_watermarks(&self.options.topic, 0, self.options.request_timeout)
            .map_err(|error| transport_error(&self.options.topic, error))?;
        Ok(Some(LogOffset(low)))
    }

    async fn recovery_tail(&self, after: LogOffset) -> StorageResult<Option<Box<dyn LogTail>>> {
        self.tail(after).await.map(Some)
    }

    async fn tail(&self, after: LogOffset) -> StorageResult<Box<dyn LogTail>> {
        Ok(Box::new(tail::KafkaTail::new(consumer(&self.configuration, &self.options)?, self.options.clone(), Arc::clone(&self.progress), after)?))
    }

    async fn read_range(&self, from: LogOffset, to: LogOffset) -> StorageResult<Vec<LogRecord>> {
        if from.0 < 0 || to < from { return Err(Error::UnavailableLogRange { requested: from.0, first_retained: 0 }); }
        let after = LogOffset(from.0 - 1);
        let mut tail = tail::KafkaTail::new(consumer(&self.configuration, &self.options)?, self.options.clone(), Arc::clone(&self.progress), after)?;
        let end = tail.end()?.min(to.0);
        let mut result = Vec::new();
        let mut bytes = 0usize;
        while tail.next_offset() < end {
            let records = tail.poll(self.options.request_timeout).await?;
            if records.is_empty() { return Err(Error::Remote { key: self.options.topic.clone(), message: "Kafka range ended before its captured tip".into() }); }
            for record in records {
                if record.metadata.offset.0 >= end { return Ok(result); }
                bytes = bytes.checked_add(record.payload.len()).ok_or_else(|| Error::LogReadLimit { maximum_records: self.options.maximum_poll_records, maximum_payload_bytes: self.options.maximum_poll_bytes })?;
                if result.len() == self.options.maximum_poll_records || bytes > self.options.maximum_poll_bytes { return Err(Error::LogReadLimit { maximum_records: self.options.maximum_poll_records, maximum_payload_bytes: self.options.maximum_poll_bytes }); }
                result.push(record);
            }
        }
        Ok(result)
    }
}
