use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use rtdb_types::Timestamp;

use super::{AppendReceipt, LocalLogAttachment, Log, LogOffset, LogRange, LogRecord, LogTail, MemoryLog};
use crate::Result;

/// The holder's in-process source log keeps pending bytes until their replica outcomes are installed.
pub struct IntakeLog {
    log: MemoryLog,
    consumed: AtomicI64,
    start: i64,
    reported: Option<AtomicI64>,
}

impl IntakeLog {
    pub fn new(maximum_message_bytes: usize, start: LogOffset) -> Self {
        let start = LogOffset(start.0.max(0));
        let capacity = maximum_message_bytes.saturating_mul(2).max(16 * 1024 * 1024);
        Self {
            log: MemoryLog::bounded_starting_at(maximum_message_bytes, start, capacity),
            consumed: AtomicI64::new(start.0 - 1),
            start: start.0,
            reported: None,
        }
    }

    pub fn for_local_replica(maximum_message_bytes: usize, start: LogOffset) -> Self {
        Self { reported: Some(AtomicI64::new(-1)), ..Self::new(maximum_message_bytes, start) }
    }

    fn report(&self, position: LogOffset) {
        if let Some(reported) = &self.reported {
            reported.fetch_max(position.0, Ordering::AcqRel);
        }
    }

    pub fn trim(&self) {
        let boundary = self.consumed.load(Ordering::Acquire).saturating_add(1);
        if boundary > self.log.first_retained().0
            && let Err(error) = self.log.truncate_before(LogOffset(boundary))
        {
            tracing::warn!(boundary, error = %error, "Failed to trim intake log");
        }
    }
}

#[async_trait]
impl Log for IntakeLog {
    fn is_volatile_intake(&self) -> bool { true }

    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        self.trim();
        let appended = self.log.append_owned(payload, None, timestamp)?;
        if self.reported.is_some() {
            self.report(self.log.latest()?);
        }
        Ok(appended)
    }
    async fn enqueue_local_attachment(&self, marker: Bytes, attachment: LocalLogAttachment, timestamp: Timestamp) -> Result<AppendReceipt> {
        if self.reported.is_none() { return Err(crate::Error::LocalAttachmentUnsupported); }
        self.trim();
        let appended = self.log.append_owned(marker, Some(attachment), timestamp)?;
        self.report(self.log.latest()?);
        Ok(appended)
    }
    fn latest(&self) -> Result<LogOffset> {
        match &self.reported {
            Some(reported) => Ok(LogOffset(reported.load(Ordering::Acquire))),
            None => self.log.latest(),
        }
    }
    fn establish_source_checkpoint(&self, through: LogOffset) {
        if through.0 < self.start {
            self.report(through);
        }
    }
    fn record_applied(&self, through: LogOffset) {
        let previous = self.consumed.fetch_max(through.0, Ordering::AcqRel);
        if self.reported.is_some() && previous < through.0 {
            self.log.release_local_attachments(LogOffset(previous.saturating_add(1)), through);
        }
        self.report(through);
    }
    async fn tail(&self, after: LogOffset) -> Result<Box<dyn LogTail>> {
        // Positions below the intake's start were never assigned to it, so a reader asking for them starts at the start; trimmed positions above it still fail as unavailable.
        let after = LogOffset(after.0.max(self.start - 1));
        self.log.tail(after).await
    }
    async fn range(&self, from: LogOffset, to: LogOffset) -> Result<Box<dyn LogRange>> { self.log.range(from, to).await }
    async fn read_range(&self, from: LogOffset, to: LogOffset) -> Result<Vec<LogRecord>> { self.log.read_range(from, to).await }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn the_intake_releases_applied_records_and_keeps_pending_records() {
        let intake = IntakeLog::new(1 << 20, LogOffset(10));
        for payload in [b"applied".as_slice(), b"pending".as_slice()] {
            intake.enqueue(Bytes::from_static(payload), Timestamp(0)).await.unwrap().durable().await.unwrap();
        }
        let mut tail = intake.tail(LogOffset(-1)).await.unwrap();
        assert_eq!(tail.poll(Duration::from_millis(10)).await.unwrap().len(), 2);
        intake.trim();
        assert_eq!(intake.log.first_retained(), LogOffset(10));
        intake.record_applied(LogOffset(10));
        intake.trim();
        assert_eq!(intake.log.first_retained(), LogOffset(11));
        assert!(intake.tail(LogOffset(9)).await.is_err());
        let pending = intake.read_range(LogOffset(11), LogOffset(12)).await.unwrap();
        assert_eq!(pending[0].payload.as_ref(), b"pending");
    }

    #[tokio::test]
    async fn local_intake_reports_source_progress_independently_of_its_allocation_floor() {
        let intake = IntakeLog::for_local_replica(4096, LogOffset(100));
        assert_eq!(intake.latest().unwrap(), LogOffset(-1));
        intake.establish_source_checkpoint(LogOffset(101));
        assert_eq!(intake.latest().unwrap(), LogOffset(-1));
        intake.establish_source_checkpoint(LogOffset(42));
        assert_eq!(intake.latest().unwrap(), LogOffset(42));
        intake.record_applied(LogOffset(49));
        assert_eq!(intake.latest().unwrap(), LogOffset(49));
        let appended = intake.enqueue(Bytes::from_static(b"next"), Timestamp(0)).await.unwrap();
        assert_eq!(appended.durable().await.unwrap().offset, LogOffset(100));
        assert_eq!(intake.latest().unwrap(), LogOffset(100));
        intake.record_applied(LogOffset(50));
        assert_eq!(intake.latest().unwrap(), LogOffset(100));
    }

    #[tokio::test]
    async fn volatile_local_attachment_uses_its_retained_size_and_drops_at_application() {
        use rtdb_columnar::{MemoryBudget, MemoryReservation};
        use std::sync::Arc;

        let memory = MemoryBudget::new(512);
        let intake = IntakeLog::for_local_replica(4096, LogOffset(0));
        let too_large = LocalLogAttachment::new((), 4096);
        assert!(matches!(
            intake.enqueue_local_attachment(Bytes::from_static(b"marker"), too_large, Timestamp(1)).await,
            Err(crate::Error::MessageTooLarge { .. })
        ));
        assert_eq!(intake.latest().unwrap(), LogOffset(-1));

        let attachment = LocalLogAttachment::new(memory.reserve(512).unwrap(), 512);
        intake.enqueue_local_attachment(Bytes::from_static(b"marker"), attachment, Timestamp(2)).await.unwrap();
        let mut tail = intake.tail(LogOffset(-1)).await.unwrap();
        let replay = tail.replay().unwrap();
        let head = replay.poll_head(Duration::from_millis(100)).await.unwrap().unwrap();
        let record = replay.read_local_attachment(head).unwrap().unwrap();
        assert_eq!(record.payload, Bytes::from_static(b"marker"));
        assert_eq!(record.local_attachment.as_ref().unwrap().get::<Arc<MemoryReservation>>().unwrap().bytes(), 512);
        assert!(replay.poll_head(Duration::from_millis(1)).await.unwrap().is_none());
        drop(record);
        assert_eq!(memory.used_bytes(), 512);
        intake.record_applied(head.metadata.offset);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[tokio::test]
    async fn intake_preserves_the_producers_memory_charge_until_application() {
        use rtdb_columnar::MemoryBudget;

        let memory = MemoryBudget::new(4096);
        let buffer = super::super::AppendBuffer::new(
            bytes::BytesMut::from(&b"pending"[..]), 0, memory.reserve(128).unwrap(),
        );

        let intake = IntakeLog::new(4096, LogOffset(0));
        intake.enqueue(buffer.into_bytes(), Timestamp(0)).await.unwrap().durable().await.unwrap();
        assert_eq!(memory.used_bytes(), 128);
        intake.record_applied(LogOffset(0));
        intake.trim();
        assert_eq!(memory.used_bytes(), 0);
    }
}
