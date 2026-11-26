use std::{collections::BTreeMap, mem::size_of, sync::{Arc, Mutex}, time::{Duration, Instant}};
use rdkafka::{ClientContext, error::{KafkaError, RDKafkaErrorCode}, message::{Header, OwnedHeaders}, native_byte_account::NativeByteAccount, producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext}};
use rtdb_columnar::MemoryReservation;
use rtdb_types::Result;
use super::{ConversionContext, Original, error, rules::DlqRule};
mod config;
mod tls;

pub(super) struct Record {
    topic: String,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: [(&'static str, Option<Vec<u8>>); 5],
    charge: Arc<MemoryReservation>,
}
impl Record {
    pub fn new(rule: &DlqRule, destination: &str, topic: &str, is_key: bool, original: Original, framed: &[u8], context: &mut ConversionContext) -> Result<Self> {
        if destination.is_empty() || destination == "." || destination == ".." || destination.len() > 249 || !destination.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')) {
            return Err(error("dlq", "Invalid dead-letter topic name"));
        }
        let payload_bound = match &original { Original::Framed => framed.len(), Original::Json(json) => json.encoded_bound()? };
        let key_bytes = if is_key { 0 } else { context.registry_key().map_or(0, <[u8]>::len) };
        let bound = [destination.len(), topic.len(), rule.name.as_ref().map_or(0, String::len), rule.executor.len(), rule.subject.len(), payload_bound, key_bytes]
            .into_iter().try_fold(4096usize, |bytes, next| bytes.checked_add(next).ok_or_else(overflow))?.checked_mul(4).ok_or_else(overflow)?;
        let charge = context.reserve(bound)?;
        let payload = match original {
            Original::Framed => framed.to_vec(),
            Original::Json(json) => {
                let mut bytes = Vec::with_capacity(payload_bound);
                json.write(&mut bytes)?;
                bytes
            }
        };
        let (key, value) = if is_key { (Some(payload), None) } else { (context.registry_key().map(<[u8]>::to_vec), Some(payload)) };
        let headers = [
            ("__rule.name", rule.name.as_ref().map(|name| name.as_bytes().to_vec())),
            ("__rule.mode", Some(rule.mode.as_str().as_bytes().to_vec())),
            ("__rule.subject", Some(rule.subject.as_bytes().to_vec())),
            ("__rule.topic", Some(topic.as_bytes().to_vec())),
            ("__rule.exception", Some(format!("Could not find rule executor of type {}", rule.executor).into_bytes())),
        ];
        Ok(Self { topic: destination.into(), key, value, headers, charge })
    }
    fn kafka_batch_upper_bound(&self) -> Result<usize> {
        const BATCH_HEADER_BYTES: usize = 61;
        const RECORD_ENVELOPE_BYTES: usize = 21;
        let length = |value: Option<&[u8]>| -> Result<usize> {
            value.map_or(Ok(1), |value| value.len().checked_add(varint_length(value.len())?).ok_or_else(overflow))
        };
        let bytes = BATCH_HEADER_BYTES.checked_add(RECORD_ENVELOPE_BYTES).and_then(|bytes| bytes.checked_add(varint_length(self.headers.len()).ok()?)).ok_or_else(overflow)?;
        let bytes = bytes.checked_add(length(self.key.as_deref())?).and_then(|bytes| bytes.checked_add(length(self.value.as_deref()).ok()?)).ok_or_else(overflow)?;
        self.headers.iter().try_fold(bytes, |bytes, (name, value)| {
            bytes.checked_add(length(Some(name.as_bytes()))?).and_then(|bytes| bytes.checked_add(length(value.as_deref()).ok()?)).ok_or_else(overflow)
        })
    }

}
struct Delivery { _charge: Arc<MemoryReservation>, _receipt_charge: Arc<MemoryReservation>, state: DeliveryState }
#[derive(Clone, Copy)]
enum DeliveryState { Pending, Reported { next: Option<usize> } }
#[derive(Default)]
struct Deliveries { receipts: BTreeMap<usize, Box<Delivery>>, completed: Option<usize> }
struct DeliveryContext { deliveries: Mutex<Deliveries>, _charge: Arc<MemoryReservation> }
struct PendingReceipt<'a> { context: &'a DeliveryContext, identity: usize }
impl PendingReceipt<'_> {
    fn native_enqueue_accepted(self) { std::mem::forget(self); }
}
impl Drop for PendingReceipt<'_> {
    fn drop(&mut self) {
        let mut deliveries = self.context.deliveries.lock().unwrap_or_else(|cause| cause.into_inner());
        if matches!(deliveries.receipts.get(&self.identity).map(|receipt| receipt.state), Some(DeliveryState::Pending)) {
            if let Some(receipt) = deliveries.receipts.remove(&self.identity) { drop(DeliveryContext::deallocate_receipt(receipt)); }
        }
    }
}
impl DeliveryContext {
    fn new(charge: Arc<MemoryReservation>) -> Self { Self { deliveries: Mutex::new(Deliveries::default()), _charge: charge } }
    fn register(&self, charge: Arc<MemoryReservation>, receipt_charge: Arc<MemoryReservation>) -> PendingReceipt<'_> {
        let receipt = Box::new(Delivery { _charge: charge, _receipt_charge: receipt_charge, state: DeliveryState::Pending });
        let identity = receipt.as_ref() as *const Delivery as usize;
        self.deliveries.lock().unwrap_or_else(|cause| cause.into_inner()).receipts.insert(identity, receipt);
        PendingReceipt { context: self, identity }
    }
    fn completed(&self, identity: usize) {
        let mut deliveries = self.deliveries.lock().unwrap_or_else(|cause| cause.into_inner());
        let next = deliveries.completed;
        if let Some(receipt) = deliveries.receipts.get_mut(&identity) {
            if matches!(receipt.state, DeliveryState::Pending) {
                receipt.state = DeliveryState::Reported { next };
                deliveries.completed = Some(identity);
            }
        }
    }
    /// The producer poll or flush that delivered these reports must have returned.
    fn release_completed(&self) {
        loop {
            let receipt = {
                let mut deliveries = self.deliveries.lock().unwrap_or_else(|cause| cause.into_inner());
                let Some(identity) = deliveries.completed else { break; };
                let receipt = deliveries.receipts.remove(&identity).expect("completed receipt remains owned");
                let DeliveryState::Reported { next } = receipt.state else { unreachable!("completion links contain reported receipts"); };
                deliveries.completed = next;
                receipt
            };
            drop(Self::deallocate_receipt(receipt));
        }
    }
    fn deallocate_receipt(delivery: Box<Delivery>) -> Delivery { *delivery }
}
impl Drop for DeliveryContext {
    fn drop(&mut self) {
        let deliveries = self.deliveries.get_mut().unwrap_or_else(|cause| cause.into_inner());
        while let Some((_, receipt)) = deliveries.receipts.pop_first() { drop(Self::deallocate_receipt(receipt)); }
        deliveries.completed = None;
    }
}
impl ClientContext for DeliveryContext {}
impl ProducerContext for DeliveryContext {
    type DeliveryOpaque = usize;
    fn delivery(&self, result: &DeliveryResult<'_>, delivery: Self::DeliveryOpaque) {
        self.completed(delivery);
        if let Err((cause, _)) = result { tracing::warn!("Kafka Registry dead-letter delivery failed: {cause}"); }
    }
}
pub(super) struct DlqProducer {
    producer: BaseProducer<DeliveryContext>,
    settings: config::Settings,
    charges: Vec<Arc<MemoryReservation>>,
}
impl DlqProducer {
    pub fn open(options: &BTreeMap<String, String>, context: &mut ConversionContext) -> Result<Self> {
        use crate::config::providers;
        let resolved = if providers::configured(options) {
            let memory = context.scratch_budget();
            let mut provider_context = ConversionContext::new(Arc::clone(&memory));
            let resolved = providers::resolve(options, &memory, &mut |bytes| provider_context.reserve(bytes).map(drop))?;
            for charge in provider_context.into_charges() { context.retain(charge)?; }
            resolved
        } else { std::borrow::Cow::Borrowed(options) };
        let options = resolved.as_ref();
        let configuration = options.iter().try_fold(65_536usize, |bytes, (name, value)| bytes.checked_add(name.len()).and_then(|bytes| bytes.checked_add(value.len())).ok_or_else(overflow))?;
        let mut charges = vec![context.reserve(configuration.checked_mul(8).ok_or_else(overflow)?)?];
        let mut settings = config::Settings::parse(options)?;
        let native_capacity = settings.native_byte_capacity()?;
        let native_charge = context.reserve(native_capacity)?;
        charges.push(Arc::clone(&native_charge));
        let tls = tls::configure(options, &mut settings.properties, context)?;
        let mut client = crate::client_config(&settings.properties);
        if let Some(tls) = tls {
            let tls = crate::client::material::Configuration::capture(tls, &settings.properties, &mut |bytes| context.reserve(bytes))?;
            charges.extend(tls.reservations().cloned());
            client.set_ssl_context_configurer(tls);
        }
        let account = NativeByteAccount::new(native_capacity, native_charge).map_err(|cause| error("dlq", cause))?;
        client.set_native_byte_account(account);
        let delivery_charge = context.reserve(size_of::<DeliveryContext>().checked_mul(16).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?)?;
        charges.push(Arc::clone(&delivery_charge));
        let producer = client.create_with_context(DeliveryContext::new(delivery_charge)).map_err(|cause| error("dlq", cause))?;
        Ok(Self { producer, settings, charges })
    }
    pub async fn send(&mut self, record: Record, context: &mut ConversionContext) -> Result<()> {
        let timestamp = rdkafka::util::current_time_millis();
        self.send_at(record, context, timestamp).await
    }
    async fn send_at(&mut self, record: Record, context: &mut ConversionContext, timestamp: i64) -> Result<()> {
        for charge in &self.charges { context.retain(Arc::clone(charge))?; }
        self.settings.validate_bytes(record.key.as_deref(), record.value.as_deref())?;
        if record.kafka_batch_upper_bound()? > self.settings.maximum_record_bytes { return Err(error("dlq", "Dead-letter record exceeds producer allocation or request size")); }
        let receipt_charge = context.reserve(size_of::<Delivery>().checked_add(size_of::<*const ()>()).and_then(|bytes| bytes.checked_mul(16)).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?)?;
        let receipt = self.producer.context().register(Arc::clone(&record.charge), receipt_charge);
        let headers = record.headers.iter().fold(OwnedHeaders::new_with_capacity(5), |headers, (name, value)| headers.insert(Header { key: name, value: value.as_deref() }));
        let mut pending = BaseRecord::<[u8], [u8], usize>::with_opaque_to(&record.topic, receipt.identity).headers(headers).timestamp(timestamp);
        pending.key = record.key.as_deref(); pending.payload = record.value.as_deref();
        let start = Instant::now();
        loop {
            self.poll();
            match self.producer.send(pending) {
                Ok(()) => { receipt.native_enqueue_accepted(); break; }
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), returned)) => {
                    pending = returned;
                    if start.elapsed() >= self.settings.maximum_block { return Err(error("dlq", "Dead-letter producer queue admission timed out")); }
                    tokio::time::sleep(Duration::from_millis(10).min(self.settings.maximum_block.saturating_sub(start.elapsed()))).await;
                }
                Err((cause, _)) => return Err(error("dlq", cause)),
            }
        }
        if self.settings.auto_flush { self.drain().await; }
        Ok(())
    }
    fn poll(&self) {
        self.producer.poll(Duration::ZERO);
        self.producer.context().release_completed();
    }
    pub async fn drain(&mut self) {
        while self.producer.in_flight_count() != 0 {
            let _ = self.producer.flush(Duration::ZERO);
            self.poll();
            if self.producer.in_flight_count() != 0 { tokio::time::sleep(Duration::from_millis(10)).await; }
        }
        self.producer.context().release_completed();
    }
}
fn varint_length(value: usize) -> Result<usize> {
    let value = i32::try_from(value).map_err(|_| overflow())? as u32;
    Ok((32 - (value << 1).leading_zeros()).max(1).div_ceil(7) as usize)
}
fn overflow() -> rtdb_types::Anomaly { error("allocation-overflow", "Dead-letter allocation exceeds addressable memory") }

#[cfg(test)]
mod native_account_tests;
#[cfg(test)]
mod delivery_release_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::rules::{DlqAction, Mode};
    use rtdb_columnar::MemoryBudget;
    use serde_json::{json, Value as Json};

    fn rule(mode: Mode) -> DlqRule {
        DlqRule { name: Some("guard".into()), executor: "missing".into(), mode, subject: "events-value".into(), topic: Some("failures".into()), action: Arc::new(DlqAction { name: "_default_".into(), options: BTreeMap::new() }) }
    }

    #[test]
    #[cfg(unix)]
    fn producer_opens_with_file_supplied_bootstrap_and_settings() {
        let cluster = rdkafka::mocking::MockCluster::new(1).unwrap();
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("producer.properties");
        std::fs::write(&file, format!("brokers={}\ntimeout=19\n", cluster.bootstrap_servers())).unwrap();
        let options = BTreeMap::from([
            ("config.providers".into(), "file".into()),
            ("config.providers.file.class".into(), "org.apache.kafka.common.config.provider.FileConfigProvider".into()),
            ("config.providers.file.param.allowed.paths".into(), root.path().to_str().unwrap().into()),
            ("bootstrap.servers".into(), format!("${{file:{}:brokers}}", file.display())),
            ("max.block.ms".into(), format!("${{file:{}:timeout}}", file.display())),
            ("buffer.memory".into(), "1048576".into()),
        ]);
        let memory = MemoryBudget::new(1 << 27);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let producer = DlqProducer::open(&options, &mut context).unwrap();
        assert_eq!(producer.settings.maximum_block, Duration::from_millis(19));
        assert_eq!(producer.settings.properties["bootstrap.servers"], cluster.bootstrap_servers());
        drop(context);
        assert!(memory.used_bytes() > 0);
        drop(producer);
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn read_and_migration_actions_retain_original_key_value_and_rule_headers() {
        let memory = MemoryBudget::new(1 << 20);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        context.set_registry_key(Some(&[0, 0, 0, 0, 4, 14])).unwrap();
        let framed = [0, 0, 0, 0, 7, 2];
        let read = Record::new(&rule(Mode::Read), "failures", "events", false, Original::Framed, &framed, &mut context).unwrap();
        assert_eq!(read.key.as_deref(), Some([0, 0, 0, 0, 4, 14].as_slice()));
        assert_eq!(read.value.as_deref(), Some(framed.as_slice()));
        assert_eq!(read.headers[0], ("__rule.name", Some(b"guard".to_vec())));
        assert_eq!(read.headers[1], ("__rule.mode", Some(b"READ".to_vec())));
        assert_eq!(read.headers[2], ("__rule.subject", Some(b"events-value".to_vec())));
        assert_eq!(read.headers[3], ("__rule.topic", Some(b"events".to_vec())));
        assert_eq!(read.headers[4], ("__rule.exception", Some(b"Could not find rule executor of type missing".to_vec())));
        let migrated = Record::new(&rule(Mode::Downgrade), "failures", "events", true, Original::Json(super::super::super::json_value::JsonValue::from_serde(json!({"id":7,"quoted":"\u{0}\n\""}))), &framed, &mut context).unwrap();
        assert_eq!(serde_json::from_slice::<Json>(migrated.key.as_ref().unwrap()).unwrap(), json!({"id":7,"quoted":"\u{0}\n\""}));
        assert!(migrated.value.is_none());
        assert_eq!(migrated.headers[1].1.as_deref(), Some(b"DOWNGRADE".as_slice()));
        drop(context); assert!(memory.used_bytes() > 0);
        drop(read); drop(migrated); assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn record_admission_fails_before_copying_original_bytes() {
        let memory = MemoryBudget::new(1 << 20);
        let mut context = ConversionContext::with_limit(Arc::clone(&memory), 16);
        assert!(Record::new(&rule(Mode::Read), "failures", "events", true, Original::Framed, &[0; 1024], &mut context).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn avro_migration_payload_keeps_big_integer_digits_and_jackson_double_spelling() {
        use super::super::super::json_value::JsonValue;
        let memory = MemoryBudget::new(1 << 20);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let original = JsonValue::from_str(r#"{"integer":18446744073709551617,"double":1e20,"zero":-0.0}"#).unwrap();
        let record = Record::new(&rule(Mode::Upgrade), "failures", "events", false, Original::Json(original), &[], &mut context).unwrap();
        assert_eq!(record.value.as_deref(), Some(br#"{"integer":18446744073709551617,"double":1.0E20,"zero":-0.0}"#.as_slice()));
        assert!(record.key.is_none());
        drop(context); assert!(memory.used_bytes() > 0);
        drop(record); assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn registry_migration_payload_keeps_big_decimal_precision_and_scale() {
        use super::super::super::json_value::JsonValue;
        let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
        let original = JsonValue::from_registry_str(r#"{"decimal":18446744073709551617.99,"scale":1.00,"exponent":1e20,"zero":-0.0}"#).unwrap();
        let record = Record::new(&rule(Mode::Upgrade), "failures", "events", false, Original::Json(original), &[], &mut context).unwrap();
        assert_eq!(record.value.as_deref(), Some(br#"{"decimal":18446744073709551617.99,"scale":1.00,"exponent":1E+20,"zero":0.0}"#.as_slice()));
    }

}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use super::super::rules::{DlqAction, Mode};
    use rdkafka::mocking::MockCluster;
    use rtdb_columnar::MemoryBudget;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dlq_append_passes_the_frozen_epoch_timestamp_to_the_native_message() {
        use rdkafka::{Message, Offset, Timestamp, TopicPartitionList, consumer::{BaseConsumer, Consumer}};

        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("timestamp-dlq", 1, 1).unwrap();
        let bootstrap = cluster.bootstrap_servers();
        let options = BTreeMap::from([
            ("bootstrap.servers".into(), bootstrap.clone()),
            ("buffer.memory".into(), (1 << 20).to_string()),
            ("dlq.auto.flush".into(), "true".into()),
        ]);
        let mut context = ConversionContext::new(MemoryBudget::new(64 << 20));
        let mut producer = DlqProducer::open(&options, &mut context).unwrap();
        let rule = DlqRule { name: None, executor: "missing".into(), mode: Mode::Read, subject: "events-value".into(), topic: None, action: Arc::new(DlqAction { name: "_default_".into(), options }) };
        let record = Record::new(&rule, "timestamp-dlq", "events", false, Original::Framed, b"record", &mut context).unwrap();
        tokio::time::timeout(Duration::from_secs(10), producer.send_at(record, &mut context, 0)).await.unwrap().unwrap();
        let consumer: BaseConsumer = rdkafka::ClientConfig::new().set("bootstrap.servers", &bootstrap)
            .set("group.id", "dlq-timestamp").set("enable.auto.commit", "false").create().unwrap();
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition_offset("timestamp-dlq", 0, Offset::Beginning).unwrap();
        consumer.assign(&assignment).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let message = loop {
            if let Some(result) = consumer.poll(Duration::from_millis(20)) { break result.unwrap(); }
            assert!(Instant::now() < deadline);
        };
        assert_eq!(message.payload(), Some(b"record".as_slice()));
        assert_eq!(message.timestamp(), Timestamp::CreateTime(0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_auto_flush_retains_delivery_until_the_owner_drains() {
        let cluster = MockCluster::new(1).unwrap(); cluster.create_topic("failures", 1, 1).unwrap();
        let options = BTreeMap::from([
            ("bootstrap.servers".into(), cluster.bootstrap_servers()),
            ("buffer.memory".into(), (1 << 20).to_string()),
            ("dlq.auto.flush".into(), "true".into()),
        ]);
        let rule = DlqRule { name: Some("guard".into()), executor: "missing".into(), mode: Mode::Read, subject: "events-value".into(), topic: Some("failures".into()), action: Arc::new(DlqAction { name: "_default_".into(), options: options.clone() }) };
        let memory = MemoryBudget::new(1 << 26);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let mut producer = DlqProducer::open(&options, &mut context).unwrap();
        cluster.broker_down(1).unwrap();
        let record = Record::new(&rule, "failures", "events", false, Original::Framed, &[0, 0, 0, 0, 7, 14], &mut context).unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(20), producer.send(record, &mut context)).await.is_err());
        assert!(producer.producer.in_flight_count() > 0);
        drop(context); assert!(memory.used_bytes() > 0);
        cluster.broker_up(1).unwrap();
        tokio::time::timeout(Duration::from_secs(10), producer.drain()).await.unwrap();
        assert_eq!(producer.producer.in_flight_count(), 0);
        drop(producer); assert_eq!(memory.used_bytes(), 0);
    }
}

#[cfg(test)]
mod record_size_tests {
    use super::*;
    #[test]
    fn producer_record_upper_bound_includes_kafka_headers_and_varint_length_transitions() {
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let record = Record { topic: "failures".into(), key: None, value: Some(vec![0; 64]), headers: [("a", None), ("b", None), ("c", None), ("d", None), ("e", None)], charge: memory.reserve(1 << 16).unwrap() };
        assert_eq!(record.kafka_batch_upper_bound().unwrap(), 165);
        assert_eq!(varint_length(63).unwrap(), 1);
        assert_eq!(varint_length(64).unwrap(), 2);
        assert_eq!(varint_length(8191).unwrap(), 2);
        assert_eq!(varint_length(8192).unwrap(), 3);
    }
}
