use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;
use async_trait::async_trait;
use bytes::Bytes;
use rdkafka::{Message, Offset, TopicPartitionList, consumer::{BaseConsumer, Consumer}, error::KafkaError};
use rtdb_storage::{Error, Result, log::{LogOffset, LogRecord, LogTail}};
use crate::{KafkaLogOptions, Progress, metadata, transport_error};

pub(crate) struct KafkaTail { consumer: BaseConsumer, options: KafkaLogOptions, progress: Arc<Progress>, next: i64 }

impl KafkaTail {
    pub fn new(consumer: BaseConsumer, options: KafkaLogOptions, progress: Arc<Progress>, after: LogOffset) -> Result<Self> {
        progress.check()?;
        let next = after.0.checked_add(1).filter(|next| *next >= 0).ok_or_else(|| Error::UnavailableLogRange { requested: after.0, first_retained: 0 })?;
        let (low, high) = consumer.fetch_watermarks(&options.topic, 0, options.request_timeout).map_err(|error| transport_error(&options.topic, error))?;
        if next < low || next > high { return Err(Error::UnavailableLogRange { requested: next, first_retained: low }); }
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition_offset(&options.topic, 0, Offset::Offset(next)).map_err(|error| transport_error(&options.topic, error))?;
        consumer.assign(&assignment).map_err(|error| transport_error(&options.topic, error))?;
        progress.advance(high - 1);
        Ok(Self { consumer, options, progress, next })
    }

    pub fn next_offset(&self) -> i64 { self.next }

    pub fn end(&self) -> Result<i64> {
        let (low, high) = self.consumer.fetch_watermarks(&self.options.topic, 0, self.options.request_timeout).map_err(|error| transport_error(&self.options.topic, error))?;
        if self.next < low { return Err(Error::UnavailableLogRange { requested: self.next, first_retained: low }); }
        if high - 1 < self.progress.observed_tip.load(Ordering::Acquire) { return Err(Error::Remote { key: self.options.topic.clone(), message: "Kafka withdrew an observed log position".into() }); }
        self.progress.advance(high - 1);
        Ok(high)
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
            match self.consumer.poll(Duration::ZERO) {
                Some(Ok(message)) => {
                    let header = metadata(&message, &self.options.topic)?;
                    if header.offset.0 != self.next { return Err(Error::UnavailableLogRange { requested: self.next, first_retained: header.offset.0 }); }
                    let payload = message.payload().ok_or_else(|| Error::Remote { key: self.options.topic.clone(), message: "Kafka log contains a tombstone".into() })?;
                    if payload.len() > self.options.maximum_message_bytes { return Err(Error::MessageTooLarge { size: payload.len(), maximum: self.options.maximum_message_bytes }); }
                    bytes += payload.len();
                    self.next = header.offset.0.checked_add(1).ok_or_else(|| Error::Remote { key: self.options.topic.clone(), message: "Kafka offset space exhausted".into() })?;
                    self.progress.advance(header.offset.0);
                    records.push(LogRecord { metadata: header, payload: Bytes::copy_from_slice(payload) });
                    continue;
                }
                Some(Err(KafkaError::PartitionEOF(_))) | None => (),
                Some(Err(error)) => return Err(transport_error(&self.options.topic, error)),
            }
            if !records.is_empty() { return Ok(records); }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                let end = self.end()?;
                if self.next == end { return Ok(records); }
                return Err(Error::Remote { key: self.options.topic.clone(), message: "Kafka fetch timed out before the observed tip".into() });
            }
            tokio::time::sleep(remaining.min(self.options.poll_interval)).await;
        }
    }
}
