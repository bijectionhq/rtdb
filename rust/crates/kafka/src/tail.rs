use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;
use async_trait::async_trait;
use bytes::Bytes;
use rdkafka::{Message, Offset, TopicPartitionList, consumer::{BaseConsumer, Consumer}, error::KafkaError, message::RetainedConsumerMessage};
use rtdb_storage::{Error, Result, log::{LogOffset, LogRecord, LogReplayTail, LogTail, ReplayHead}, object_store::ReadReservation, sealing::{KafkaFrameKeys, KAFKA_LOG_SEAL_OVERHEAD}};
use crate::{ConfiguredLogClient, KafkaLogOptions, Progress, metadata, transport_error};

const DELIVERY_OVERHEAD_BYTES: usize = 256;

pub(crate) struct KafkaTail {
    pending: Option<RetainedConsumerMessage>,
    consumer: ConfiguredLogClient<Arc<BaseConsumer>>,
    options: KafkaLogOptions,
    progress: Arc<Progress>,
    next: i64,
    keys: Option<Arc<KafkaFrameKeys>>,
}

pub(crate) struct KafkaRange { tail: KafkaTail, end: i64, maximum: usize, timeout: Duration }
impl KafkaRange {
    pub fn new(tail: KafkaTail, end: i64, maximum: usize, timeout: Duration) -> Self { Self { tail, end, maximum, timeout } }
}
#[async_trait]
impl rtdb_storage::log::LogRange for KafkaRange {
    fn allocation_bound(&self) -> usize { self.tail.allocation_bound(self.maximum) }
    async fn next(&mut self, reservation: ReadReservation) -> Result<Option<LogRecord>> {
        if reservation.length() < self.allocation_bound() { return Err(Error::Task("log read reservation is too small".into())); }
        if self.tail.next_offset() >= self.end { return Ok(None); }
        let head = self.tail.poll_head(self.timeout).await?.ok_or_else(|| Error::Remote { key: self.tail.options.topic.clone(), message: "Kafka range ended before its captured tip".into() })?;
        self.tail.read_head(head, reservation).await.map(Some)
    }
}

impl KafkaTail {
    pub fn new(consumer: ConfiguredLogClient<Arc<BaseConsumer>>, options: KafkaLogOptions, progress: Arc<Progress>, after: LogOffset, keys: Option<Arc<KafkaFrameKeys>>) -> Result<Self> {
        progress.check()?;
        let next = after.0.checked_add(1).filter(|next| *next >= 0).ok_or_else(|| Error::UnavailableLogRange { requested: after.0, first_retained: 0 })?;
        let (low, high) = consumer.fetch_watermarks(&options.topic, 0, options.request_timeout).map_err(|error| transport_error(&options.topic, error))?;
        let low = low.max(keys.as_ref().map_or(Ok(0), |keys| keys.kafka_erasure_floor())?);
        if next < low || next > high { return Err(Error::UnavailableLogRange { requested: next, first_retained: low }); }
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition_offset(&options.topic, 0, Offset::Offset(next)).map_err(|error| transport_error(&options.topic, error))?;
        consumer.assign(&assignment).map_err(|error| transport_error(&options.topic, error))?;
        progress.advance(high - 1);
        Ok(Self { consumer, pending: None, options, progress, next, keys })
    }

    pub fn next_offset(&self) -> i64 { self.next }

    pub fn end(&self) -> Result<i64> {
        let (low, high) = self.consumer.fetch_watermarks(&self.options.topic, 0, self.options.request_timeout).map_err(|error| transport_error(&self.options.topic, error))?;
        let low = low.max(self.keys.as_ref().map_or(Ok(0), |keys| keys.kafka_erasure_floor())?);
        if self.next < low { return Err(Error::UnavailableLogRange { requested: self.next, first_retained: low }); }
        if high - 1 < self.progress.observed_tip.load(Ordering::Acquire) { return Err(Error::Remote { key: self.options.topic.clone(), message: "Kafka withdrew an observed log position".into() }); }
        self.progress.advance(high - 1);
        Ok(high)
    }

    fn available_head(&mut self) -> Result<Option<ReplayHead>> {
        if self.pending.is_none() {
            match RetainedConsumerMessage::poll_from(&*self.consumer, Duration::ZERO) {
                Some(Ok(message)) => self.pending = Some(message),
                Some(Err(KafkaError::PartitionEOF(_))) | None => return Ok(None),
                Some(Err(error)) => return Err(transport_error(&self.options.topic, error)),
            }
        }
        self.pending_head().map(Some)
    }

    fn pending_message(&self) -> Result<&RetainedConsumerMessage> {
        self.pending.as_ref().ok_or_else(|| Error::Task("Kafka replay has no prepared head".into()))
    }

    fn pending_head(&self) -> Result<ReplayHead> {
        let message = self.pending_message()?.as_borrowed();
        let header = metadata(message, &self.options.topic)?;
        if header.offset.0 != self.next {
            return Err(Error::UnavailableLogRange { requested: self.next, first_retained: header.offset.0 });
        }
        let payload_len = message.payload().ok_or_else(|| self.tombstone())?.len();
        if payload_len > self.options.maximum_message_bytes {
            return Err(Error::MessageTooLarge { size: payload_len, maximum: self.options.maximum_message_bytes });
        }
        if header.offset.0 == i64::MAX {
            return Err(Error::Remote { key: self.options.topic.clone(), message: "Kafka offset space exhausted".into() });
        }
        let allocation_bound = self.allocation_bound(payload_len);
        let overhead = if self.keys.is_some() { KAFKA_LOG_SEAL_OVERHEAD } else { 0 };
        let payload_len = payload_len.checked_sub(overhead)
            .ok_or_else(|| Error::Task("Truncated sealed Kafka record".into()))?;
        Ok(ReplayHead { metadata: header, payload_len, allocation_bound })
    }

    fn allocation_bound(&self, physical: usize) -> usize {
        let copies = if self.keys.is_some() { 2 } else { 1 };
        physical.saturating_mul(copies).saturating_add(DELIVERY_OVERHEAD_BYTES)
    }

    fn tombstone(&self) -> Error {
        Error::Remote { key: self.options.topic.clone(), message: "Kafka log contains a tombstone".into() }
    }

    async fn deliver_head(&mut self, head: ReplayHead, reservation: Option<ReadReservation>) -> Result<LogRecord> {
        if reservation.as_ref().is_some_and(|reservation| reservation.length() < head.allocation_bound) {
            return Err(Error::Task("log read reservation is too small".into()));
        }
        let payload = Bytes::copy_from_slice(self.pending_message()?.as_borrowed().payload().ok_or_else(|| self.tombstone())?);
        let payload = match &self.keys {
            Some(keys) => {
                let open = keys.open_kafka_publication(head.metadata.offset.0, payload);
                match &reservation {
                    Some(reservation) => reservation.retaining_io(open).await?,
                    None => open.await?,
                }
            }
            None => payload,
        };
        let record = LogRecord::new(head.metadata, payload);
        let record = match reservation { Some(reservation) => record.retain(reservation)?, None => record };
        self.pending = None;
        self.next = record.metadata.offset.0 + 1;
        self.progress.advance(record.metadata.offset.0);
        Ok(record)
    }

    fn confirm_tip(&self) -> Result<()> {
        let end = self.end()?;
        if self.next == end { return Ok(()); }
        Err(Error::LogFetchPending { requested: self.next, tip: end - 1 })
    }
}

#[async_trait]
impl LogTail for KafkaTail {
    async fn poll(&mut self, timeout: Duration) -> Result<Vec<LogRecord>> {
        let deadline = tokio::time::Instant::now().checked_add(timeout).ok_or_else(|| Error::Task("Kafka poll deadline overflow".into()))?;
        let mut records = Vec::new();
        let mut bytes = 0usize;
        loop {
            self.progress.check()?;
            if records.len() == self.options.maximum_poll_records || bytes.checked_add(self.options.maximum_message_bytes).is_none_or(|bytes| bytes > self.options.maximum_poll_bytes) { return Ok(records); }
            if let Some(head) = self.available_head()? {
                records.push(self.deliver_head(head, None).await?);
                bytes += head.payload_len;
                continue;
            }
            if !records.is_empty() { return Ok(records); }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                self.confirm_tip()?;
                return Ok(records);
            }
            tokio::time::sleep(remaining.min(self.options.poll_interval)).await;
        }
    }

    fn replay(&mut self) -> Option<&mut dyn LogReplayTail> { Some(self) }
}

#[async_trait]
impl LogReplayTail for KafkaTail {
    async fn poll_head(&mut self, timeout: Duration) -> Result<Option<ReplayHead>> {
        let deadline = tokio::time::Instant::now().checked_add(timeout).ok_or_else(|| Error::Task("Kafka poll deadline overflow".into()))?;
        loop {
            self.progress.check()?;
            if let Some(head) = self.available_head()? { return Ok(Some(head)); }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                self.confirm_tip()?;
                return Ok(None);
            }
            tokio::time::sleep(remaining.min(self.options.poll_interval)).await;
        }
    }

    async fn read_head(&mut self, head: ReplayHead, reservation: ReadReservation) -> Result<LogRecord> {
        self.progress.check()?;
        if self.pending_head()? != head {
            return Err(Error::Task("Kafka replay head does not match the pending record".into()));
        }
        self.deliver_head(head, Some(reservation)).await
    }
}
