use super::{AppendReceipt, Log, LogMetadata, LogOffset, LogRecord, LogTail, Progress, await_tip};
use crate::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use rtdb_types::Timestamp;
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::oneshot;

struct Records {
    first: i64,
    next: i64,
    values: VecDeque<LogRecord>,
}

struct Shared { records: Mutex<Records>, progress: Progress, maximum_message_bytes: usize }

pub struct MemoryLog { shared: Arc<Shared> }

impl MemoryLog {
    pub fn new(maximum_message_bytes: usize) -> Self {
        Self { shared: Arc::new(Shared {
            records: Mutex::new(Records { first: 0, next: 0, values: VecDeque::new() }),
            progress: Progress::new(LogOffset(-1)), maximum_message_bytes,
        }) }
    }

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
}

fn read(shared: &Shared, from: i64, to: i64) -> Result<Vec<LogRecord>> {
    if to < from { return Err(Error::Task("log range end precedes its start".into())); }
    let records = shared.records.lock();
    if from < records.first || from > records.next {
        return Err(Error::UnavailableLogRange { requested: from, first_retained: records.first });
    }
    Ok(records.values.iter().skip((from - records.first) as usize)
        .take_while(|record| record.metadata.offset.0 < to).cloned().collect())
}

#[async_trait]
impl Log for MemoryLog {
    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        if payload.len() > self.shared.maximum_message_bytes {
            return Err(Error::MessageTooLarge { size: payload.len(), maximum: self.shared.maximum_message_bytes });
        }
        let mut records = self.shared.records.lock();
        self.shared.progress.latest()?;
        let metadata = LogMetadata { offset: LogOffset(records.next), timestamp };
        records.next = records.next.checked_add(1).ok_or_else(|| Error::Closed("log positions exhausted".into()))?;
        records.values.push_back(LogRecord { metadata, payload });
        self.shared.progress.advance(metadata.offset);
        let (sender, durable) = oneshot::channel();
        let _ = sender.send(Ok(metadata));
        Ok(AppendReceipt::new(durable))
    }

    fn latest(&self) -> Result<LogOffset> { self.shared.progress.latest() }

    async fn tail(&self, after: LogOffset) -> Result<Box<dyn LogTail>> {
        let next = after.0.checked_add(1).ok_or_else(|| Error::Closed("log positions exhausted".into()))?;
        read(&self.shared, next, next)?;
        Ok(Box::new(MemoryTail { shared: self.shared.clone(), after }))
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

#[async_trait]
impl LogTail for MemoryTail {
    async fn poll(&mut self, timeout: Duration) -> Result<Vec<LogRecord>> {
        let tip = await_tip(&self.shared.progress, self.after, timeout).await?;
        if tip <= self.after { return Ok(Vec::new()); }
        let records = read(&self.shared, self.after.0 + 1, tip.0 + 1)?;
        if let Some(last) = records.last() { self.after = last.metadata.offset; }
        else { return Err(Error::UnavailableLogRange { requested: self.after.0 + 1, first_retained: tip.0 }); }
        Ok(records)
    }
}
