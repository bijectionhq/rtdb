use super::{AppendReceipt, AppendTimestampSource, GuardedAppend, LOG_RECORD_OVERHEAD_BYTES, Log, LogMetadata, LogOffset, LogRecord, LogReplayTail, LogTail, Progress, ReplayHead, SuppliedTimestamp, await_tip};
use crate::{Error, Result, object_store::ReadReservation};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use rtdb_types::Timestamp;
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

const MAXIMUM_POLL_RECORDS: usize = 1024;
const DEFAULT_MAXIMUM_POLL_PAYLOAD_BYTES: usize = 128 * 1024 * 1024;

struct Records {
    first: i64,
    next: i64,
    values: VecDeque<LogRecord>,
}

impl Records {
    fn range(&self, from: i64, to: i64) -> Result<impl Iterator<Item = &LogRecord> + '_> {
        if from < self.first || from > self.next {
            return Err(Error::UnavailableLogRange { requested: from, first_retained: self.first });
        }
        Ok(self.values.iter().skip((from - self.first) as usize)
            .take_while(move |record| record.metadata.offset.0 < to))
    }

    fn replay_record(&self, offset: i64) -> Result<&LogRecord> {
        self.range(offset, i64::MAX)?.next().ok_or(Error::UnavailableLogRange { requested: offset, first_retained: self.first })
    }
}

struct Shared {
    records: Mutex<Records>,
    progress: Progress,
    maximum_message_bytes: usize,
    timestamps: Arc<dyn AppendTimestampSource>,
    retained: Option<Arc<Semaphore>>,
}

pub struct MemoryLog { shared: Arc<Shared> }

impl MemoryLog {
    pub fn new(maximum_message_bytes: usize) -> Self {
        Self::with_timestamp_source(maximum_message_bytes, Arc::new(SuppliedTimestamp))
    }

    pub fn with_timestamp_source(maximum_message_bytes: usize, timestamps: Arc<dyn AppendTimestampSource>) -> Self {
        Self::empty(maximum_message_bytes, LogOffset(0), timestamps, None)
    }

    /// An empty log whose first record takes offset `next`, for a log that continues positions a previous owner assigned.
    pub fn starting_at(maximum_message_bytes: usize, next: LogOffset) -> Self {
        Self::empty(maximum_message_bytes, next, Arc::new(SuppliedTimestamp), None)
    }

    pub fn bounded_starting_at(maximum_message_bytes: usize, next: LogOffset, maximum_retained_bytes: usize) -> Self {
        let retained = Arc::new(Semaphore::new(maximum_retained_bytes.min(Semaphore::MAX_PERMITS)));
        Self::empty(maximum_message_bytes, next, Arc::new(SuppliedTimestamp), Some(retained))
    }

    fn empty(maximum_message_bytes: usize, next: LogOffset, timestamps: Arc<dyn AppendTimestampSource>, retained: Option<Arc<Semaphore>>) -> Self {
        let next = next.0.max(0);
        Self { shared: Arc::new(Shared {
            records: Mutex::new(Records { first: next, next, values: VecDeque::new() }),
            progress: Progress::new(LogOffset(next - 1)), maximum_message_bytes, timestamps, retained,
        }) }
    }

    /// The offset of the oldest record still retained.
    pub fn first_retained(&self) -> LogOffset { LogOffset(self.shared.records.lock().first) }

    /// The caller must retain every record at and above the last recoverable block boundary.
    pub fn truncate_before(&self, boundary: LogOffset) -> Result<()> {
        let mut records = self.shared.records.lock();
        if boundary.0 < records.first || boundary.0 > records.next {
            return Err(Error::UnavailableLogRange { requested: boundary.0, first_retained: records.first });
        }
        while records.first < boundary.0 { records.values.pop_front(); records.first += 1; }
        Ok(())
    }

    pub fn close(&self) {
        let _records = self.shared.records.lock();
        self.shared.progress.close();
    }

    fn enqueue_at(&self, payload: Bytes, sample: impl FnOnce(&[u8]) -> Result<Timestamp>) -> Result<AppendReceipt> {
        if payload.len() > self.shared.maximum_message_bytes {
            return Err(Error::MessageTooLarge { size: payload.len(), maximum: self.shared.maximum_message_bytes });
        }
        let mut records = self.shared.records.lock();
        self.shared.progress.latest()?;
        let next = records.next.checked_add(1).ok_or_else(|| Error::Closed("log positions exhausted".into()))?;
        // Release the producer's allocation charge once this log owns the payload.
        let payload = if let Some(retained) = &self.shared.retained {
            let required = payload.len().saturating_add(LOG_RECORD_OVERHEAD_BYTES);
            let unavailable = || Error::AppendCapacity { required_bytes: required as u64 };
            let charge = u32::try_from(required).map_err(|_| unavailable())?;
            let credit = Arc::clone(retained).try_acquire_many_owned(charge).map_err(|_| unavailable())?;
            Bytes::from_owner(RetainedPayload { bytes: payload.to_vec(), _credit: credit })
        } else {
            Bytes::copy_from_slice(&payload)
        };
        let timestamp = sample(&payload)?;
        let metadata = LogMetadata { offset: LogOffset(records.next), timestamp };
        records.next = next;
        records.values.push_back(LogRecord::new(metadata, payload));
        self.shared.progress.advance(metadata.offset);
        let (sender, durable) = oneshot::channel();
        let _ = sender.send(Ok(metadata));
        Ok(AppendReceipt::new(durable))
    }
}

struct RetainedPayload {
    bytes: Vec<u8>,
    _credit: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for RetainedPayload {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}

fn read(shared: &Shared, from: i64, to: i64) -> Result<Vec<LogRecord>> {
    if to < from { return Err(Error::Task("log range end precedes its start".into())); }
    let records = shared.records.lock();
    let result = records.range(from, to)?.cloned().collect();
    Ok(result)
}

impl MemoryLog {
    fn append_record(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        self.enqueue_at(payload, |payload| self.shared.timestamps.timestamp(payload, timestamp))
    }

    fn open_tail(&self, after: LogOffset) -> Result<MemoryTail> {
        let next = after.0.checked_add(1).ok_or_else(|| Error::Closed("log positions exhausted".into()))?;
        read(&self.shared, next, next)?;
        Ok(MemoryTail { shared: self.shared.clone(), after })
    }
}

#[async_trait]
impl super::SharedLog for MemoryLog {
    async fn append(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        self.append_record(payload, timestamp)
    }

    fn latest(&self) -> Result<LogOffset> { self.shared.progress.latest() }

    async fn read_after(&self, after: LogOffset) -> Result<Box<dyn LogReplayTail>> {
        Ok(Box::new(self.open_tail(after)?))
    }
}

#[async_trait]
impl Log for MemoryLog {
    fn maximum_append_bytes(&self) -> Option<usize> { Some(self.shared.maximum_message_bytes) }
    fn maximum_guarded_append_bytes(&self) -> Option<usize> { self.maximum_append_bytes() }

    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        self.append_record(payload, timestamp)
    }

    async fn enqueue_guarded(&self, payload: Bytes, guard: GuardedAppend<'_>) -> Result<AppendReceipt> {
        if guard.journal_before().is_some() {
            return Err(Error::GuardedAppendUnsupported);
        }
        self.enqueue_at(payload, |_| guard.sample())
    }

    fn latest(&self) -> Result<LogOffset> { super::SharedLog::latest(self) }

    async fn retained_floor(&self) -> Result<Option<LogOffset>> {
        Ok(Some(LogOffset(self.shared.records.lock().first)))
    }

    async fn recovery_tail(&self, after: LogOffset) -> Result<Option<Box<dyn LogTail>>> {
        self.tail(after).await.map(Some)
    }

    async fn tail(&self, after: LogOffset) -> Result<Box<dyn LogTail>> {
        Ok(Box::new(self.open_tail(after)?))
    }

    async fn read_range(&self, from: LogOffset, to: LogOffset) -> Result<Vec<LogRecord>> {
        read(&self.shared, from.0, to.0)
    }

    async fn range(&self, from: LogOffset, to: LogOffset) -> Result<Box<dyn super::LogRange>> {
        if to < from { return Err(Error::Task("log range end precedes its start".into())); }
        let records = self.shared.records.lock();
        if from.0 < records.first { return Err(Error::UnavailableLogRange { requested: from.0, first_retained: records.first }); }
        let end = records.next.min(to.0);
        drop(records);
        Ok(Box::new(MemoryRange { shared: Arc::clone(&self.shared), next: from.0, end }))
    }
}

struct MemoryRange { shared: Arc<Shared>, next: i64, end: i64 }
#[async_trait]
impl super::LogRange for MemoryRange {
    fn allocation_bound(&self) -> usize { self.shared.maximum_message_bytes.saturating_add(256) }
    async fn next(&mut self, reservation: crate::object_store::ReadReservation) -> Result<Option<LogRecord>> {
        if reservation.length() < self.allocation_bound() { return Err(Error::Task("log read reservation is too small".into())); }
        if self.next >= self.end { return Ok(None); }
        let record = read(&self.shared, self.next, self.next + 1)?.pop().ok_or_else(|| Error::UnavailableLogRange { requested: self.next, first_retained: self.end })?;
        self.next += 1;
        record.retain(reservation).map(Some)
    }
}

struct MemoryTail { shared: Arc<Shared>, after: LogOffset }

impl MemoryTail {
    async fn poll_with_payload_limit(&mut self, timeout: Duration, payload_limit: usize) -> Result<Vec<LogRecord>> {
        let tip = await_tip(&self.shared.progress, self.after, timeout).await?;
        if tip <= self.after { return Ok(Vec::new()); }
        let payload_limit = payload_limit.max(self.shared.maximum_message_bytes);
        let from = self.after.0 + 1;
        let records = self.shared.records.lock();
        let mut batch = Vec::new();
        let mut payload_bytes = 0usize;
        for record in records.range(from, tip.0 + 1)?.take(MAXIMUM_POLL_RECORDS) {
            let Some(next_bytes) = payload_bytes.checked_add(record.payload.len()).filter(|bytes| *bytes <= payload_limit) else { break; };
            batch.push(record.clone());
            payload_bytes = next_bytes;
        }
        let last = batch.last().ok_or(Error::UnavailableLogRange { requested: from, first_retained: tip.0 })?;
        self.after = last.metadata.offset;
        Ok(batch)
    }
}

#[async_trait]
impl LogTail for MemoryTail {
    async fn poll(&mut self, timeout: Duration) -> Result<Vec<LogRecord>> {
        self.poll_with_payload_limit(timeout, DEFAULT_MAXIMUM_POLL_PAYLOAD_BYTES).await
    }

    fn replay(&mut self) -> Option<&mut dyn LogReplayTail> { Some(self) }
}

fn replay_head(record: &LogRecord) -> ReplayHead {
    ReplayHead { metadata: record.metadata, payload_len: record.payload.len(), allocation_bound: LOG_RECORD_OVERHEAD_BYTES }
}

#[async_trait]
impl LogReplayTail for MemoryTail {
    async fn poll_head(&mut self, timeout: Duration) -> Result<Option<ReplayHead>> {
        let tip = await_tip(&self.shared.progress, self.after, timeout).await?;
        if tip <= self.after { return Ok(None); }
        let records = self.shared.records.lock();
        self.shared.progress.latest()?;
        Ok(Some(replay_head(records.replay_record(self.after.0 + 1)?)))
    }

    async fn read_head(&mut self, head: ReplayHead, reservation: ReadReservation) -> Result<LogRecord> {
        let records = self.shared.records.lock();
        self.shared.progress.latest()?;
        let record = records.replay_record(self.after.0 + 1)?;
        if replay_head(record) != head {
            return Err(Error::Task("log replay head does not match the next record".into()));
        }
        if reservation.length() < head.allocation_bound {
            return Err(Error::Task("log read reservation is too small".into()));
        }
        let record = record.clone().with_read_reservation(reservation);
        self.after = record.metadata.offset;
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_staging_clock_cannot_certify_an_original_durable_quorum() {
        let log = MemoryLog::new(1024);
        let clock = || Timestamp(10);
        let cutoff = rtdb_types::TimedJournalCutoff::new(Timestamp(25), [7; 32]).unwrap();
        let timed = GuardedAppend::new(&clock, Timestamp(10), Timestamp(20), Some(cutoff));
        assert!(log.maximum_guarded_append_bytes().is_some());
        assert!(log.maximum_timed_original_append_bytes().is_none());
        assert!(matches!(
            log.enqueue_guarded(Bytes::from_static(b"timed"), timed).await,
            Err(Error::GuardedAppendUnsupported)
        ));
        assert_eq!(log.latest().unwrap(), LogOffset(-1));
        let staged = GuardedAppend::new(&clock, Timestamp(10), Timestamp(20), None);
        let record = log.enqueue_guarded(Bytes::from_static(b"staged"), staged).await.unwrap();
        assert_eq!(record.durable().await.unwrap().offset, LogOffset(0));
    }

    #[tokio::test]
    async fn an_appended_payload_releases_its_producers_charge() {
        let memory = rtdb_types::MemoryBudget::new(1 << 20);
        struct Charged(Vec<u8>, std::sync::Arc<rtdb_types::MemoryReservation>);
        impl AsRef<[u8]> for Charged { fn as_ref(&self) -> &[u8] { &self.0 } }
        let log = MemoryLog::new(1 << 20);
        let payload = Bytes::from_owner(Charged(vec![7; 4096], memory.reserve(8192).unwrap()));
        log.enqueue(payload, Timestamp(1)).await.unwrap().durable().await.unwrap();
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(log.read_range(LogOffset(0), LogOffset(1)).await.unwrap()[0].payload.as_ref(), &[7; 4096][..]);
    }

    #[tokio::test]
    async fn tail_payload_limit_preserves_the_prefix_and_admits_a_maximum_size_record() {
        let log = MemoryLog::new(6);
        let payloads: [&'static [u8]; 5] = [b"abc", b"defg", b"x", b"uvwxyz", b"y"];
        for (offset, &payload) in payloads.iter().enumerate() {
            log.enqueue(Bytes::from_static(payload), Timestamp(offset as i64)).await.unwrap().durable().await.unwrap();
        }
        let mut tail = MemoryTail { shared: Arc::clone(&log.shared), after: LogOffset(-1) };
        for (limit, from, to) in [(7, 0, 2), (5, 2, 3), (5, 3, 4), (5, 4, 5)] {
            let batch = tail.poll_with_payload_limit(Duration::ZERO, limit).await.unwrap();
            let observed: Vec<_> = batch.iter().map(|record| record.payload.as_ref()).collect();
            assert_eq!(observed.as_slice(), &payloads[from..to]);
            assert!(batch.iter().zip(from..to).all(|(record, offset)| record.metadata.offset == LogOffset(offset as i64)));
        }
        assert!(tail.poll_with_payload_limit(Duration::ZERO, 5).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_log_started_at_an_offset_continues_from_it() {
        let log = MemoryLog::starting_at(1024, LogOffset(42));
        assert_eq!(log.latest().unwrap(), LogOffset(41));
        let appended = log.enqueue(Bytes::from_static(b"a"), Timestamp(1)).await.unwrap().durable().await.unwrap();
        assert_eq!(appended.offset, LogOffset(42));
        let mut tail = log.tail(LogOffset(41)).await.unwrap();
        assert_eq!(tail.poll(Duration::from_millis(10)).await.unwrap()[0].metadata.offset, LogOffset(42));
    }
}


#[cfg(test)]
mod retained_capacity_tests {
    use super::*;

    #[tokio::test]
    async fn a_read_copy_holds_intake_credit_after_trim() {
        let log = MemoryLog::bounded_starting_at(1024, LogOffset(0), LOG_RECORD_OVERHEAD_BYTES + 1);
        log.enqueue(Bytes::from_static(b"a"), Timestamp(0)).await.unwrap().durable().await.unwrap();
        let records = log.read_range(LogOffset(0), LogOffset(1)).await.unwrap();
        log.truncate_before(LogOffset(1)).unwrap();
        assert!(matches!(
            log.enqueue(Bytes::from_static(b"b"), Timestamp(0)).await,
            Err(Error::AppendCapacity { .. }),
        ));
        assert_eq!(log.latest().unwrap(), LogOffset(0));
        drop(records);
        let receipt = log.enqueue(Bytes::from_static(b"b"), Timestamp(0)).await.unwrap();
        assert_eq!(receipt.durable().await.unwrap().offset, LogOffset(1));
    }
}
