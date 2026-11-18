use rdkafka::{message::ToBytes, producer::BaseRecord, util::IntoOpaque};
use rtdb_types::{Anomaly, Result};

use super::{MetadataMonitor, WaitCompletion};
use super::super::{
    Failure, IdentityWitness, MetadataTopicError, MetadataWait, Outcome, PreparedSend, ProducerMetadata,
    SelectedPartition, Snapshot, WaitFailure,
};

/// One producer call's topic, requested partition and original wait budget.
pub struct TopicWaitRequest<'a> {
    /// The original non-null Java topic name in the represented UTF8 domain.
    pub topic: &'a str,
    /// The validated nonnegative explicit partition, if supplied by the record.
    pub partition: Option<i32>,
    /// The signed millisecond start sample taken by the producer call.
    pub started_ms: i64,
    /// The original nonnegative maximum wait in milliseconds.
    pub maximum_wait_ms: i64,
}

/// The serialized source time observations for one producer call.
pub trait MetadataClock {
    /// Obtains the next signed Java millisecond sample.
    fn milliseconds(&mut self) -> Result<i64>;
    /// Obtains the next signed Java monotonic nanosecond sample.
    fn nanoseconds(&mut self) -> Result<i64>;
}

/// The retained generation and source timing of a resolved topic.
pub struct TopicMetadata<'call> {
    monitor: &'call MetadataMonitor,
    request: TopicWaitRequest<'call>,
    snapshot: Snapshot,
    elapsed_ms: i64,
    elapsed_ns: Option<i64>,
}

impl TopicMetadata<'_> {
    /// Borrows the retained generation whose partition count satisfied the producer call.
    pub fn snapshot(&self) -> &Snapshot { &self.snapshot }

    /// Returns the source elapsed milliseconds charged to the producer call.
    pub fn elapsed_ms(&self) -> i64 { self.elapsed_ms }

    /// Returns the measured source nanoseconds for metrics, absent for cached success.
    pub fn elapsed_ns(&self) -> Option<i64> { self.elapsed_ns }

    /// Returns the signed source wall time used for the accumulator append after waiting.
    /// Addition uses Java long wrapping arithmetic.
    pub fn append_time_ms(&self) -> i64 { self.request.started_ms.wrapping_add(self.elapsed_ms) }

    /// Returns the source milliseconds left for queue admission after metadata.
    /// The Java long subtraction wraps, then the result is clamped at zero.
    pub fn queue_wait_ms(&self) -> i64 { self.request.maximum_wait_ms.wrapping_sub(self.elapsed_ms).max(0) }

    /// Fixes the native record's partition using the generation and producer from this wait.
    /// The caller supplies the already admitted, serialized record with the original topic and explicit partition, if any.
    /// Mismatched records and zero bytes in the topic fail before a counter draw.
    /// A source explicit partition bypasses the counter.
    /// RoundRobin consumes its counter before rejecting a negative result.
    /// An admission failure ends the mutable capability and wakes waiters, as in [`ProducerMetadata::select_send`] and [`MetadataMonitor::wait_for_topic`].
    /// When enqueue returns the record for retry, reuse it with the selected partition and existing payload owner.
    pub fn prepare_round_robin<'record, K: ToBytes + ?Sized, P: ToBytes + ?Sized, D: IntoOpaque>(
        self, record: BaseRecord<'record, K, P, D>,
    ) -> Outcome<PreparedSend<BaseRecord<'record, K, P, D>>> {
        let Self { monitor, request, snapshot, .. } = self;
        if record.topic != request.topic || record.partition != request.partition {
            return Err(Failure::RecordMetadataMismatch);
        }
        if record.topic.as_bytes().contains(&0) {
            return Err(Failure::InvalidNativeRecordTopic);
        }
        let mut prepared = if let Some(id) = request.partition {
            monitor.lock()?.usable()?;
            let partition = SelectedPartition::explicit(id).ok_or(Failure::InvalidPartition(id))?;
            PreparedSend { record, partition, snapshot }
        } else {
            monitor.apply_topic_change(|metadata| metadata.select_send_in(snapshot, request.topic, record))?
        };
        let partition = prepared.partition_id();
        if partition < 0 {
            return Err(Failure::InvalidPartition(partition));
        }
        prepared.record = prepared.record.partition(partition);
        Ok(prepared)
    }
}

/// The first failure in the producer's topic metadata wait.
#[derive(Debug)]
pub enum TopicWaitFailure {
    /// The metadata state, its admission or the synchronized wait clock failed.
    Metadata(Failure),
    /// The source metadata monitor is closed.
    Closed,
    /// The source Timer's initial clock sample or timeout validation failed.
    Timer(WaitFailure),
    /// A clock sample outside the Timer, sender wakeup or partition count failed.
    Observation(Anomaly),
    /// A source fatal, invalid-topic or authorization error.
    Topic(MetadataTopicError),
    /// The original timeout with the previous iteration's count and a cause from one synchronized lookup.
    /// Timer expiry retains any topic error; elapsed-budget expiry after an update retains only retriable codes.
    TimedOut {
        known_partitions: Option<i32>,
        maximum_wait_ms: i64,
        cause: Option<i16>,
    },
}

impl From<Failure> for TopicWaitFailure {
    fn from(error: Failure) -> Self { Self::Metadata(error) }
}

fn partition_count(snapshot: &Snapshot, topic: &str) -> Result<Option<i32>> {
    let count = i32::try_from(snapshot.partitions(topic).len()).map_err(|_| Anomaly::unsupported(
        "rtdb/kafka-partition-list-size", "Java partition lists require a signed int length",
    ))?;
    Ok((count != 0).then_some(count))
}

// Exception ancestry of Kafka 4.4 Errors.forCode, pinned in the state README.
fn retriable(code: i16) -> bool {
    matches!(code, 2 | 3 | 5 | 6 | 7 | 9 | 13 | 14 | 15 | 16 | 19 | 20 | 41 | 51 | 56
        | 70 | 71 | 72 | 74 | 75 | 78 | 80 | 83 | 84 | 88 | 89 | 100 | 103 | 106 | 122 | 123 | 133)
}

impl MetadataMonitor {
    fn apply_topic_change<T>(&self, change: impl FnOnce(&mut ProducerMetadata) -> Outcome<T>) -> Outcome<T> {
        let result = {
            let mut metadata = self.lock()?;
            change(&mut metadata)
        };
        if matches!(&result, Err(Failure::Native(_))) {
            self.state.changed.notify_waiters();
        }
        result
    }

    /// Waits for a topic and any requested partition using the original producer call's time budget.
    /// The supplied wake operation asks the existing sender to fetch metadata, outside the metadata lock.
    /// Cached success adds the topic without requesting clock samples or waking the sender.
    /// A cached invalid topic fails before addition, and cached success skips the fatal and closed wait checks.
    /// Fatal consumption, source clocks and the Tokio time runtime follow [`Self::await_update`].
    /// After an update within budget, the topic check can clear both recoverable sets as in [`ProducerMetadata::take_error_for_topic`].
    /// The clock and identity callbacks must not reenter the monitor.
    /// Cancellation leaves completed topic and request mutations and issued sender wakeups in effect.
    pub async fn wait_for_topic<'call>(
        &'call self,
        request: TopicWaitRequest<'call>,
        clock: &mut impl MetadataClock,
        identity: &mut impl IdentityWitness,
        mut wake_sender: impl FnMut() -> Result<()>,
    ) -> std::result::Result<TopicMetadata<'call>, TopicWaitFailure> {
        let TopicWaitRequest { topic, partition, started_ms, maximum_wait_ms } = request;
        let mut snapshot = self.snapshot()?;
        if snapshot.invalid_topics().any(|name| name == Some(topic)) {
            return Err(TopicWaitFailure::Topic(MetadataTopicError::Recoverable(17)));
        }
        self.apply_topic_change(|metadata| metadata.add_topic(topic, started_ms, identity))?;
        let mut known_partitions = partition_count(&snapshot, topic).map_err(TopicWaitFailure::Observation)?;
        let has_partition = |count: Option<i32>| count.is_some_and(|count| partition.is_none_or(|id| id < count));
        if has_partition(known_partitions) {
            return Ok(TopicMetadata { monitor: self, request, snapshot, elapsed_ms: 0, elapsed_ns: None });
        }

        let started_ns = clock.nanoseconds().map_err(TopicWaitFailure::Observation)?;
        let mut elapsed_ms = 0;
        let mut remaining_ms = maximum_wait_ms;
        loop {
            self.apply_topic_change(|metadata| metadata.add_topic(topic, started_ms.wrapping_add(elapsed_ms), identity))?;
            let version = self.apply_topic_change(|metadata| metadata.request_for_topic(topic, identity))?;
            wake_sender().map_err(TopicWaitFailure::Observation)?;
            let mut wait = MetadataWait::new(version, remaining_ms, || clock.milliseconds())
                .map_err(TopicWaitFailure::Timer)?;
            match self.await_update(&mut wait, || clock.milliseconds()).await? {
                WaitCompletion::Updated => (),
                WaitCompletion::Closed => return Err(TopicWaitFailure::Closed),
                WaitCompletion::Fatal(error) => return Err(TopicWaitFailure::Topic(MetadataTopicError::Fatal(error))),
                WaitCompletion::TimedOut(_) => {
                    let cause = self.lock()?.topic_error(topic);
                    return Err(TopicWaitFailure::TimedOut { known_partitions, maximum_wait_ms, cause });
                }
            }

            snapshot = self.snapshot()?;
            elapsed_ms = clock.milliseconds().map_err(TopicWaitFailure::Observation)?.wrapping_sub(started_ms);
            if elapsed_ms >= maximum_wait_ms {
                let cause = self.lock()?.topic_error(topic).filter(|code| retriable(*code));
                return Err(TopicWaitFailure::TimedOut { known_partitions, maximum_wait_ms, cause });
            }
            if let Some(error) = self.apply_topic_change(|metadata| metadata.take_error_for_topic(topic))? {
                return Err(TopicWaitFailure::Topic(error));
            }
            remaining_ms = maximum_wait_ms.wrapping_sub(elapsed_ms);
            known_partitions = partition_count(&snapshot, topic).map_err(TopicWaitFailure::Observation)?;
            if has_partition(known_partitions) {
                let elapsed_ns = clock.nanoseconds().map_err(TopicWaitFailure::Observation)?.wrapping_sub(started_ns);
                return Ok(TopicMetadata { monitor: self, request, snapshot, elapsed_ms, elapsed_ns: Some(elapsed_ns) });
            }
        }
    }
}
