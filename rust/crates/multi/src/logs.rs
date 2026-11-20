//! The logs a node opens a deployment's database with.
//!
//! The replica stream lives on the deployment's logkeeper ensemble: a holder opens it as the writer at its epoch, which seals it, so no earlier holder's append is accepted; a subscriber opens it as a reader.
//! The source log is the holder's in-process intake: only the holder submits, the holder alone resolves, and a resolved transaction is durable once its replica record commits, so a transaction costs one quorum append.
//! A submission the holder had not yet resolved when it stopped was never acknowledged, and its client retries it.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use prost_types::Any;
use rtdb_format::envelope::{ReplicaBody, ReplicaEnvelope};
use rtdb_logkeeper::StreamConfig;
use rtdb_node::{LogAccess, LogFactory, OpenedLogs};
use rtdb_storage::log::{AppendReceipt, Log, LogOffset, LogRange, LogRecord, LogReplayTail, LogTail, MemoryLog, ReplayHead};
use rtdb_storage::object_store::ReadReservation;
use rtdb_types::{Anomaly, Result, SourcePosition, StorageEpoch, Timestamp};
use tokio_util::sync::CancellationToken;

use crate::cipher::{Cipher, SEALED_OVERHEAD};
use crate::deployment::LogkeeperLogConfig;
use crate::keys::KeyService;

/// How many already-resolved intake records stay retained behind the holder's read position.
const INTAKE_RETAINED_BEHIND: i64 = 1024;
const SCAN_WINDOW: i64 = 256;

/// The writers a node has opened, by deployment, for confirming leadership before a linearizable read.
pub type Writers = Arc<parking_lot::Mutex<std::collections::HashMap<String, std::sync::Weak<rtdb_logkeeper::QuorumLog>>>>;

/// Records a completed ensemble move, as the holder that made it at `epoch`, before anything is appended on the new ensemble.
#[async_trait]
pub trait EnsembleRecorder: Send + Sync {
    async fn record(&self, deployment: &str, epoch: u64, ensemble: Vec<rtdb_directory::MemberId>) -> Result<()>;
}

pub struct LogkeeperLogFactory { pub maximum_message_bytes: usize, pub keys: Arc<dyn KeyService>, pub writers: Writers, pub recorder: Arc<dyn EnsembleRecorder> }

impl LogkeeperLogFactory {
    pub fn new(maximum_message_bytes: usize, keys: Arc<dyn KeyService>, writers: Writers, recorder: Arc<dyn EnsembleRecorder>) -> Arc<Self> { Arc::new(Self { maximum_message_bytes, keys, writers, recorder }) }
}

pub struct DirectoryRecorder { pub directory: Arc<dyn rtdb_directory::Directory> }

#[async_trait]
impl EnsembleRecorder for DirectoryRecorder {
    async fn record(&self, deployment: &str, epoch: u64, ensemble: Vec<rtdb_directory::MemberId>) -> Result<()> {
        for _ in 0..8 {
            let current = self.directory.deployment(deployment).await?.ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-deployment", format!("No deployment {deployment}")))?;
            if current.epoch != epoch { return Err(Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "rtdb.multi/deposed", "The deployment moved on before its new ensemble was recorded")); }
            let mut next = current.clone();
            next.ensemble = ensemble.clone();
            if let rtdb_directory::Cas::Applied(_) = self.directory.replace_deployment(next, current.version).await? { return Ok(()); }
        }
        Err(Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "rtdb.multi/record-contended", "The deployment kept changing while its new ensemble was recorded"))
    }
}

#[async_trait]
impl LogFactory for LogkeeperLogFactory {
    async fn open(&self, configuration: &Any, access: LogAccess, cancellation: CancellationToken) -> Result<OpenedLogs> {
        let config: LogkeeperLogConfig = serde_json::from_slice(&configuration.value)
            .map_err(|error| Anomaly::incorrect("rtdb.multi/log-config", format!("Logkeeper log configuration is malformed: {error}")))?;
        let stream = StreamConfig { servers: config.servers.clone(), stream: config.replica_stream.clone(), write_quorum: config.write_quorum, maximum_message_bytes: self.maximum_message_bytes };
        let quorum: Arc<dyn Log> = match access {
            LogAccess::ReadWrite => {
                let writer = match &config.previous_servers {
                    Some(previous) => {
                        let moved = rtdb_logkeeper::open_writer_on(vec![rtdb_logkeeper::Loglet { servers: previous.clone(), start_lsn: 0 }], stream, config.epoch, rtdb_logkeeper::CopyFrom::Retained, cancellation.clone()).await?;
                        // Recorded before the writer appends: until then the old ensemble, sealed and complete, still holds everything.
                        self.recorder.record(&config.deployment, config.epoch, config.ensemble.iter().map(|id| rtdb_directory::MemberId(id.clone())).collect()).await?;
                        moved
                    }
                    None => rtdb_logkeeper::open_writer(stream, config.epoch, cancellation.clone()).await?,
                };
                self.writers.lock().insert(config.deployment.clone(), Arc::downgrade(&writer));
                writer
            }
            LogAccess::ReadOnly => rtdb_logkeeper::open_reader(stream, cancellation.clone()).await?,
            LogAccess::SourceWriter => return Err(Anomaly::unsupported("rtdb.multi/source-writer",
                "A deployment has one writer, its holder; submissions reach it through the gateway")),
        };
        let replica: Arc<dyn Log> = Arc::new(SealedLog { inner: quorum, cipher: Arc::new(Cipher::new(&self.keys.key(&config.deployment)?)?), stream: Arc::from(config.replica_stream.as_str()) });
        let epoch = StorageEpoch(config.log_epoch);
        let next = next_source_offset(replica.as_ref(), epoch).await?;
        let intake = Arc::new(IntakeLog { log: MemoryLog::starting_at(self.maximum_message_bytes, LogOffset(next)), consumed: Arc::new(AtomicI64::new(next - 1)) });
        let trimming = {
            let intake = Arc::clone(&intake);
            async move {
                loop {
                    tokio::select! {
                        _ = cancellation.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(Duration::from_secs(1)) => intake.trim(),
                    }
                }
            }.boxed()
        };
        Ok(OpenedLogs { source: intake, replica, source_epoch: epoch, replica_epoch: epoch, drivers: vec![trimming] })
    }
}

/// The first source offset no replica record can already have consumed.
/// Every replica record consumes at most one source position, so the newest one named, plus one per later record, bounds every position assigned before; oversized records, whose position is out of line, are covered by that count.
pub async fn next_source_offset(replica: &dyn Log, epoch: StorageEpoch) -> Result<i64> {
    let latest = replica.latest()?.0;
    let mut end = latest + 1;
    let mut after_newest = 0i64;
    while end > 0 {
        let start = (end - SCAN_WINDOW).max(0);
        let records = replica.read_range(LogOffset(start), LogOffset(end)).await?;
        for record in records.iter().rev() {
            if let Some(position) = source_position(&record.payload, epoch)? { return Ok(position + 1 + after_newest); }
            after_newest += 1;
        }
        end = start;
    }
    Ok(after_newest)
}

fn source_position(payload: &[u8], epoch: StorageEpoch) -> Result<Option<i64>> {
    let Some(envelope) = ReplicaEnvelope::decode(payload)? else { return Ok(None); };
    let position = match &envelope.message {
        ReplicaBody::ResolvedTx(tx) => tx.src_msg_id,
        ReplicaBody::NoOp(noop) => noop.src_msg_id,
        ReplicaBody::BlockBoundary(boundary) => boundary.latest_processed_msg_id,
        ReplicaBody::BlockUploaded(upload) => upload.latest_processed_msg_id,
        _ => None,
    };
    Ok(position.filter(|position| *position >= 0).map(SourcePosition).filter(|position| position.epoch() == epoch).map(SourcePosition::offset))
}

/// A log whose payloads are sealed under the deployment's key, so a logkeeper never holds a row in the clear.
pub struct SealedLog { inner: Arc<dyn Log>, cipher: Arc<Cipher>, stream: Arc<str> }

fn opened(cipher: &Cipher, stream: &str, record: LogRecord) -> rtdb_storage::Result<LogRecord> {
    Ok(LogRecord { metadata: record.metadata, payload: cipher.open(stream, &record.payload)? })
}

#[async_trait]
impl Log for SealedLog {
    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> rtdb_storage::Result<AppendReceipt> {
        self.inner.enqueue(self.cipher.seal(&self.stream, &payload)?, timestamp).await
    }
    fn latest(&self) -> rtdb_storage::Result<LogOffset> { self.inner.latest() }
    async fn tail(&self, after: LogOffset) -> rtdb_storage::Result<Box<dyn LogTail>> {
        Ok(Box::new(SealedTail { inner: self.inner.tail(after).await?, cipher: Arc::clone(&self.cipher), stream: Arc::clone(&self.stream) }))
    }
    async fn range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Box<dyn LogRange>> {
        Ok(Box::new(SealedRange { inner: self.inner.range(from, to).await?, cipher: Arc::clone(&self.cipher), stream: Arc::clone(&self.stream) }))
    }
    async fn read_range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Vec<LogRecord>> {
        self.inner.read_range(from, to).await?.into_iter().map(|record| opened(&self.cipher, &self.stream, record)).collect()
    }
}

struct SealedTail { inner: Box<dyn LogTail>, cipher: Arc<Cipher>, stream: Arc<str> }

#[async_trait]
impl LogTail for SealedTail {
    async fn poll(&mut self, timeout: Duration) -> rtdb_storage::Result<Vec<LogRecord>> {
        self.inner.poll(timeout).await?.into_iter().map(|record| opened(&self.cipher, &self.stream, record)).collect()
    }
    fn replay(&mut self) -> Option<&mut dyn LogReplayTail> { if self.inner.replay().is_some() { Some(self) } else { None } }
}

#[async_trait]
impl LogReplayTail for SealedTail {
    async fn poll_head(&mut self, timeout: Duration) -> rtdb_storage::Result<Option<ReplayHead>> {
        let head = self.inner.replay().expect("checked when replay was offered").poll_head(timeout).await?;
        Ok(head.map(|head| ReplayHead { payload_len: head.payload_len.saturating_sub(SEALED_OVERHEAD), ..head }))
    }
    async fn read_head(&mut self, head: ReplayHead, reservation: ReadReservation) -> rtdb_storage::Result<LogRecord> {
        let sealed = ReplayHead { payload_len: head.payload_len + SEALED_OVERHEAD, ..head };
        let record = self.inner.replay().expect("checked when replay was offered").read_head(sealed, reservation).await?;
        opened(&self.cipher, &self.stream, record)
    }
}

struct SealedRange { inner: Box<dyn LogRange>, cipher: Arc<Cipher>, stream: Arc<str> }

#[async_trait]
impl LogRange for SealedRange {
    fn allocation_bound(&self) -> usize { self.inner.allocation_bound() }
    async fn next(&mut self, reservation: ReadReservation) -> rtdb_storage::Result<Option<LogRecord>> {
        self.inner.next(reservation).await?.map(|record| opened(&self.cipher, &self.stream, record)).transpose()
    }
}

/// The holder's in-process source log: it remembers how far its reader has got, and drops what lies well behind it.
pub struct IntakeLog { log: MemoryLog, consumed: Arc<AtomicI64> }

impl IntakeLog {
    fn trim(&self) {
        let boundary = self.consumed.load(Ordering::Acquire) + 1 - INTAKE_RETAINED_BEHIND;
        if boundary > self.log.first_retained().0 { let _ = self.log.truncate_before(LogOffset(boundary)); }
    }
}

#[async_trait]
impl Log for IntakeLog {
    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> rtdb_storage::Result<AppendReceipt> { self.log.enqueue(payload, timestamp).await }
    fn latest(&self) -> rtdb_storage::Result<LogOffset> { self.log.latest() }
    async fn tail(&self, after: LogOffset) -> rtdb_storage::Result<Box<dyn LogTail>> {
        Ok(Box::new(IntakeTail { tail: self.log.tail(after).await?, consumed: Arc::clone(&self.consumed) }))
    }
    async fn range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Box<dyn LogRange>> { self.log.range(from, to).await }
    async fn read_range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Vec<LogRecord>> { self.log.read_range(from, to).await }
}

struct IntakeTail { tail: Box<dyn LogTail>, consumed: Arc<AtomicI64> }

impl IntakeTail {
    fn delivered(&self, offset: LogOffset) { self.consumed.fetch_max(offset.0, Ordering::AcqRel); }
}

#[async_trait]
impl LogTail for IntakeTail {
    async fn poll(&mut self, timeout: Duration) -> rtdb_storage::Result<Vec<LogRecord>> {
        let records = self.tail.poll(timeout).await?;
        if let Some(last) = records.last() { self.delivered(last.metadata.offset); }
        Ok(records)
    }
    fn replay(&mut self) -> Option<&mut dyn LogReplayTail> { if self.tail.replay().is_some() { Some(self) } else { None } }
}

#[async_trait]
impl LogReplayTail for IntakeTail {
    async fn poll_head(&mut self, timeout: Duration) -> rtdb_storage::Result<Option<ReplayHead>> {
        self.tail.replay().expect("checked when replay was offered").poll_head(timeout).await
    }
    async fn read_head(&mut self, head: ReplayHead, reservation: ReadReservation) -> rtdb_storage::Result<LogRecord> {
        let record = self.tail.replay().expect("checked when replay was offered").read_head(head, reservation).await?;
        self.delivered(record.metadata.offset);
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::log::proto;
    use rtdb_types::Term;

    async fn append(log: &MemoryLog, message: ReplicaBody) {
        let envelope = ReplicaEnvelope { term: Term(1), term_sequence: None, message };
        log.enqueue(Bytes::from(envelope.encode()), Timestamp(0)).await.unwrap().durable().await.unwrap();
    }

    #[tokio::test]
    async fn the_intake_starts_beyond_every_source_position_the_replica_stream_may_hold() {
        let replica = MemoryLog::new(1 << 20);
        assert_eq!(next_source_offset(&replica, StorageEpoch(0)).await.unwrap(), 0, "a fresh deployment starts at 0");
        append(&replica, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None })).await;
        append(&replica, ReplicaBody::ResolvedTx(proto::ResolvedTx { src_msg_id: Some(41), ..Default::default() })).await;
        assert_eq!(next_source_offset(&replica, StorageEpoch(0)).await.unwrap(), 42);
        append(&replica, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None })).await;
        append(&replica, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None })).await;
        assert_eq!(next_source_offset(&replica, StorageEpoch(0)).await.unwrap(), 44, "each later record may have consumed one position");
    }

    #[tokio::test]
    async fn the_intake_drops_what_lies_well_behind_its_reader() {
        let intake = IntakeLog { log: MemoryLog::starting_at(1 << 20, LogOffset(10)), consumed: Arc::new(AtomicI64::new(9)) };
        for _ in 0..(INTAKE_RETAINED_BEHIND + 100) { intake.enqueue(Bytes::from_static(b"x"), Timestamp(0)).await.unwrap().durable().await.unwrap(); }
        let mut tail = intake.tail(LogOffset(9)).await.unwrap();
        let mut read = 0;
        while read < INTAKE_RETAINED_BEHIND + 100 { read += tail.poll(Duration::from_millis(10)).await.unwrap().len() as i64; }
        intake.trim();
        assert_eq!(intake.log.first_retained(), LogOffset(10 + 100), "everything but the retained window behind the reader is dropped");
    }
}
