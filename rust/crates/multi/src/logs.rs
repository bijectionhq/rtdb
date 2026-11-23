//! The logs a node opens a deployment's database with.
//!
//! The replica stream lives on the deployment's logkeeper ensemble: a holder opens it as the writer at its epoch, which seals it, so no earlier holder's append is accepted; a subscriber opens it as a reader.
//! The source log is the holder's in-process intake: only the holder submits, the holder alone resolves, and a resolved transaction is durable once its replica record commits, so a transaction costs one quorum append.
//! A submission the holder had not yet resolved when it stopped was never acknowledged, and its client retries it.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures::FutureExt;
use prost_types::Any;
use rtdb_format::envelope::{ReplicaBody, ReplicaEnvelope};
use rtdb_logkeeper::{QuorumLog, StreamConfig};
use rtdb_node::{AwaitAuthority, ClosedEpochRecovery, LogAccess, LogFactory, OpenedLogs, PlacedReceiptCodec, ReplicaReadIndex};
use rtdb_storage::log::{AppendReceipt, Log, LogOffset, LogRange, LogRecord, LogReplayTail, LogTail, MemoryLog, ReplayHead};
use rtdb_storage::object_store::ReadReservation;
use rtdb_types::{Anomaly, BlockIndex, JournalEndpoint, PLACED_ATTEMPT_OUTCOME_TABLE, PlacedAttemptScope, ReplicaPosition, Result, SourcePosition, StorageEpoch, Timestamp};
use tokio_util::sync::CancellationToken;

use crate::cipher::{Cipher, SEALED_OVERHEAD};
use crate::deployment::LogkeeperLogConfig;
use crate::recovery_holds::{RecoveryAdmission, RecoveryHolds, ReplayHold};

/// How many already-resolved intake records stay retained behind the holder's read position.
const INTAKE_RETAINED_BEHIND: i64 = 1024;
const SCAN_WINDOW: i64 = 256;
const INTAKE_TIP_WINDOW: i64 = 8;
const READ_INDEX_WITHIN: Duration = Duration::from_secs(2);

struct LogletReadIndex { log: Arc<QuorumLog>, epoch: u64 }

#[async_trait]
impl ReplicaReadIndex for LogletReadIndex {
    async fn confirm(&self) -> Result<LogOffset> {
        tokio::time::timeout(READ_INDEX_WITHIN, self.log.read_index(self.epoch)).await
            .map_err(|_| Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.multi/read-index-timeout", "Logkeepers did not confirm a read index in time"))?
            .map_err(Into::into)
    }
}

struct DeploymentReceipts {
    scope: PlacedAttemptScope,
    cipher: Cipher,
}

const RECEIPT_ASSOCIATED: &str = "rtdb.multi/placed-await/v1";

impl PlacedReceiptCodec for DeploymentReceipts {
    fn scope(&self) -> PlacedAttemptScope { self.scope }
    fn seal(&self, value: &[u8]) -> Result<String> {
        Ok(URL_SAFE_NO_PAD.encode(self.cipher.seal(RECEIPT_ASSOCIATED, value)?))
    }
    fn open(&self, token: &str) -> Result<Vec<u8>> {
        let invalid = || Anomaly::incorrect("rtdb/invalid-await-token", "Await token is invalid for this database");
        if token.len() > 192 { return Err(invalid()); }
        let sealed = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?;
        self.cipher.open(RECEIPT_ASSOCIATED, &sealed).map(|bytes| bytes.to_vec()).map_err(|_| invalid())
    }
}

/// The writers a node has opened, by deployment, for confirming leadership before a linearizable read.
pub type Writers = Arc<parking_lot::Mutex<std::collections::HashMap<String, std::sync::Weak<rtdb_logkeeper::QuorumLog>>>>;

/// Records a completed ensemble move, as the holder that made it at `epoch`, before anything is appended on the new ensemble.
#[async_trait]
pub trait EnsembleRecorder: Send + Sync {
    /// Records every endpoint a copy can touch before any bytes move.
    async fn reserve_copy(&self, deployment: &str, epoch: u64, servers: &[JournalEndpoint]) -> Result<()>;
    async fn record(&self, deployment: &str, epoch: u64, ensemble: Vec<rtdb_directory::MemberId>) -> Result<()>;
}

pub(crate) struct LogkeeperLogFactory {
    maximum_message_bytes: usize,
    writers: Writers,
    recorder: DirectoryRecorder,
    holds: Arc<RecoveryHolds>,
}

impl LogkeeperLogFactory {
    pub fn new(maximum_message_bytes: usize, writers: Writers, holds: Arc<RecoveryHolds>) -> Arc<Self> {
        let recorder = DirectoryRecorder { directory: Arc::clone(&holds.directory) };
        Arc::new(Self { maximum_message_bytes, writers, recorder, holds })
    }
}

pub struct DirectoryRecorder { pub directory: Arc<dyn rtdb_directory::Directory> }

#[async_trait]
impl EnsembleRecorder for DirectoryRecorder {
    async fn reserve_copy(&self, deployment: &str, epoch: u64, servers: &[JournalEndpoint]) -> Result<()> {
        for _ in 0..8 {
            let mut record = self.directory.deployment(deployment).await?.ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-deployment", format!("No deployment {deployment}")))?;
            if record.epoch != epoch || record.holder.is_none()
                || !matches!(record.lifecycle, rtdb_directory::Lifecycle::Live | rtdb_directory::Lifecycle::Quarantined) {
                return Err(Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "rtdb.multi/deposed", "The deployment moved on before its journal inventory was reserved"));
            }
            let mut endpoints = record.replica_journals.clone();
            endpoints.extend(record.log_copy_servers.iter().cloned());
            endpoints.extend(servers.iter().cloned());
            if endpoints == record.replica_journals { return Ok(()); }
            record.replica_journals = endpoints;
            let version = record.version;
            if let rtdb_directory::Cas::Applied(_) = self.directory.replace_deployment(record, version).await? { return Ok(()); }
        }
        Err(Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "rtdb.multi/record-contended", "The deployment kept changing while its log copy was reserved"))
    }

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

pub(crate) fn frozen_checkpoint(
    checkpoint: rtdb_directory::Checkpoint,
    epoch: StorageEpoch,
    maximum_message_bytes: usize,
) -> Result<OpenedLogs> {
    let end = |position: i64| position.checked_add(1).filter(|next| *next >= 0)
        .ok_or_else(|| Anomaly::incorrect("rtdb.multi/checkpoint-boundary", "Invalid frozen checkpoint boundary"));
    let replica = Arc::new(MemoryLog::starting_at(maximum_message_bytes, LogOffset(end(checkpoint.boundary)?)));
    let start = end(checkpoint.source)?;
    let intake = Arc::new(IntakeLog {
        log: MemoryLog::starting_at(maximum_message_bytes, LogOffset(start)),
        consumed: Arc::new(AtomicI64::new(start - 1)),
        start,
    });
    Ok(OpenedLogs { source: intake, replica, source_epoch: epoch, replica_epoch: epoch, replica_read_index: None,
        await_authority: AwaitAuthority::Frozen, drivers: Vec::new() })
}

#[async_trait]
impl LogFactory for LogkeeperLogFactory {
    async fn open(&self, configuration: &Any, access: LogAccess, cancellation: CancellationToken) -> Result<OpenedLogs> {
        let config: LogkeeperLogConfig = serde_json::from_slice(&configuration.value)
            .map_err(|error| Anomaly::incorrect("rtdb.multi/log-config", format!("Logkeeper log configuration is malformed: {error}")))?;
        let epoch = StorageEpoch(config.log_epoch);
        if let Some(checkpoint) = config.frozen_at {
            let mut logs = frozen_checkpoint(checkpoint, epoch, self.maximum_message_bytes)?;
            if let Some(root) = checkpoint.root_sha256 {
                let key = self.holds.keys.existing(&config.key_id).await?;
                let scope = PlacedAttemptScope::new(&config.replica_stream, config.epoch)?;
                logs.await_authority = AwaitAuthority::Checkpoint {
                    codec: Box::new(DeploymentReceipts { scope, cipher: Cipher::new(&key)? }),
                    block: BlockIndex(checkpoint.block),
                    through: ReplicaPosition::from_epoch_offset(epoch, checkpoint.boundary)?,
                    root,
                };
            }
            return Ok(logs);
        }
        let stream = StreamConfig { servers: config.servers.clone(), stream: config.replica_stream.clone(), write_quorum: config.write_quorum, maximum_message_bytes: self.maximum_message_bytes };
        let scope = (access == LogAccess::ReadWrite || config.epoch != 0).then(|| PlacedAttemptScope::new(&stream.stream, config.epoch)).transpose()?;
        let (quorum, key) = match access {
            LogAccess::ReadWrite => {
                let key = self.holds.keys.existing(&config.key_id).await?;
                // An interrupted copy can leave records even if the new
                // ensemble never becomes the reported serving ensemble.
                let ensemble = member_ids(&stream.servers)?;
                self.recorder.reserve_copy(&config.deployment, config.epoch, &stream.servers).await?;
                let writer = match &config.previous_servers {
                    Some(previous) => {
                        member_ids(previous)?;
                        self.recorder.reserve_copy(&config.deployment, config.epoch, previous).await?;
                        let moved = rtdb_logkeeper::open_writer_on(vec![rtdb_logkeeper::Loglet { servers: previous.clone(), start_lsn: 0 }], stream, config.epoch, rtdb_logkeeper::CopyFrom::Retained, cancellation.clone()).await?;
                        // Recorded before the writer appends: until then the old ensemble, sealed and complete, still holds everything.
                        self.recorder.record(&config.deployment, config.epoch, ensemble).await?;
                        moved
                    }
                    None => rtdb_logkeeper::open_writer(stream, config.epoch, cancellation.clone()).await?,
                };
                self.writers.lock().insert(config.deployment.clone(), Arc::downgrade(&writer));
                (writer, key)
            }
            LogAccess::ReadOnly => {
                let key = self.holds.keys.existing(&config.key_id).await?;
                member_ids(&stream.servers)?;
                (rtdb_logkeeper::open_reader(stream, cancellation.clone()).await?, key)
            }
            LogAccess::SourceWriter => return Err(Anomaly::unsupported("rtdb.multi/source-writer",
                "A deployment has one writer, its holder; submissions reach it through the gateway")),
        };
        let replica_read_index = config.require_read_index.then(|| Box::new(LogletReadIndex {
            log: Arc::clone(&quorum), epoch: config.epoch,
        }) as Box<dyn ReplicaReadIndex>);
        let await_authority = if let Some(scope) = scope {
            let recovery = if access == LogAccess::ReadWrite {
                ClosedEpochRecovery::Opened(quorum.latest()?)
            } else {
                ClosedEpochRecovery::ReadIndex(Box::new(LogletReadIndex { log: Arc::clone(&quorum), epoch: config.epoch }))
            };
            AwaitAuthority::Live { codec: Box::new(DeploymentReceipts { scope, cipher: Cipher::new(&key)? }), recovery }
        } else { AwaitAuthority::Frozen };
        let replica: Arc<dyn Log> = Arc::new(SealedLog::for_serving(quorum, &key, &config, Arc::clone(&self.holds))?);
        let mut next = next_source_offset(replica.as_ref(), epoch).await?;
        if let Some(through) = config.transferred_source_through {
            let floor = through.checked_add(1).filter(|floor| *floor >= 0)
                .ok_or_else(|| Anomaly::fault("rtdb.multi/intake-floor", "Invalid transferred source offset"))?;
            SourcePosition::from_epoch_offset(epoch, floor)?;
            next = next.max(floor);
        }
        let intake = Arc::new(IntakeLog { log: MemoryLog::starting_at(self.maximum_message_bytes, LogOffset(next)), consumed: Arc::new(AtomicI64::new(next - 1)), start: next });
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
        Ok(OpenedLogs { source: intake, replica, source_epoch: epoch, replica_epoch: epoch,
            replica_read_index, await_authority, drivers: vec![trimming] })
    }
}

fn member_ids(servers: &[JournalEndpoint]) -> Result<Vec<rtdb_directory::MemberId>> {
    servers.iter().map(|endpoint| {
        endpoint.journal().filter(|id| rtdb_types::valid_journal_identity(id))
            .map(|id| rtdb_directory::MemberId(id.to_owned())).ok_or_else(||
                Anomaly::incorrect("rtdb.multi/journal-identity-missing", format!("No valid journal identity for {}", endpoint.address())))
    }).collect()
}

/// The first source offset no replica record can already have consumed.
/// Every replica record consumes at most one source position, so the newest one named, plus one per later record, bounds every position assigned before; oversized records, whose position is out of line, are covered by that count.
pub async fn next_source_offset(replica: &dyn Log, epoch: StorageEpoch) -> Result<i64> {
    let latest = replica.latest()?.0;
    let mut end = latest + 1;
    let mut after_newest = 0i64;
    let mut floor = 0;
    let mut window = INTAKE_TIP_WINDOW;
    while end > floor {
        let start = (end - window).max(floor);
        let records = match replica.read_range(LogOffset(start), LogOffset(end)).await {
            Ok(records) => records,
            // Trimmed below here: the scan continues from the first retained record.
            Err(rtdb_storage::Error::UnavailableLogRange { first_retained, .. }) if first_retained > start && first_retained < end => { floor = first_retained; continue; }
            Err(rtdb_storage::Error::UnavailableLogRange { .. }) => break,
            Err(error) => return Err(error.into()),
        };
        for record in records.iter().rev() {
            match source_position(&record.payload, epoch)? {
                Scanned::Position(position) => return Ok(position + 1 + after_newest),
                Scanned::MayConsume => after_newest += 1,
                Scanned::Consumes => {}
            }
        }
        end = start;
        window = SCAN_WINDOW;
    }
    Ok(after_newest)
}

enum Scanned { Position(i64), MayConsume, Consumes }

/// What one replica record says about the source positions consumed up to it.
/// A claim or heartbeat (a no-op naming no source position) and a leader's own deletion notice consume none; anything else whose position it does not name may have consumed one.
fn source_position(payload: &[u8], epoch: StorageEpoch) -> Result<Scanned> {
    let Some(envelope) = ReplicaEnvelope::decode(payload)? else { return Ok(Scanned::MayConsume); };
    let position = match &envelope.message {
        ReplicaBody::ResolvedTx(tx) => tx.src_msg_id,
        ReplicaBody::NoOp(noop) => match noop.src_msg_id { None => return Ok(Scanned::Consumes), position => position },
        ReplicaBody::BlockBoundary(boundary) => boundary.latest_processed_msg_id,
        ReplicaBody::BlockUploaded(upload) => upload.latest_processed_msg_id,
        ReplicaBody::TriesDeleted(_) => return Ok(Scanned::Consumes),
        _ => None,
    };
    Ok(match position.filter(|position| *position >= 0).map(SourcePosition).filter(|position| position.epoch() == epoch) {
        Some(position) => Scanned::Position(position.offset()),
        None => Scanned::MayConsume,
    })
}

/// The transactions after replica offset `boundary` up to `system_time`, renumbered as one fresh term above every term the log has used, for a fork's stream.
/// Their ids are still the parent's: `renumber_into` gives them the fork's.
/// A tail holding an oversized message is refused: its payload object is the parent's, written after the fork's block.
pub async fn transactions_until(replica: &dyn Log, boundary: i64, system_time: rtdb_types::Timestamp) -> Result<Vec<ReplicaEnvelope>> {
    let latest = replica.latest()?.0;
    let mut kept = Vec::new();
    let mut highest = rtdb_types::Term(0);
    let mut next = boundary;
    let mut past = false;
    while next < latest {
        let end = (next + 1 + SCAN_WINDOW).min(latest + 1);
        for record in replica.read_range(LogOffset(next + 1), LogOffset(end)).await? {
            let Some(envelope) = ReplicaEnvelope::decode(&record.payload)? else { continue };
            highest = highest.max(envelope.term);
            match envelope.message {
                ReplicaBody::OversizedMessage(_) => return Err(Anomaly::unsupported("rtdb.multi/fork-oversized", "The tail holds an oversized transaction: flush the parent and fork again")),
                ReplicaBody::ResolvedTx(tx) if !past && tx.system_time_micros.is_none_or(|micros| micros <= system_time.0) => kept.push(ReplicaBody::ResolvedTx(tx)),
                ReplicaBody::ResolvedTx(_) => past = true,
                _ => {}
            }
        }
        next = end - 1;
    }
    let term = rtdb_types::Term(highest.0 + 1);
    Ok(kept.into_iter().enumerate().map(|(sequence, message)| ReplicaEnvelope { term, term_sequence: Some(rtdb_types::TermSequence(sequence as i64)), message }).collect())
}

/// Gives a fork's copied transactions ids, and source positions, in the fork's log epoch, so they come after its block's and before its own.
pub fn renumber_into(tail: &mut [ReplicaEnvelope], epoch: StorageEpoch) -> Result<()> {
    let attempt_table = format!("xt/{PLACED_ATTEMPT_OUTCOME_TABLE}");
    for (offset, envelope) in tail.iter_mut().enumerate() {
        let ReplicaBody::ResolvedTx(tx) = &mut envelope.message else { continue };
        let position = rtdb_types::SourcePosition::from_epoch_offset(epoch, offset as i64)?.0;
        tx.tx_id = Some(position);
        tx.src_msg_id = Some(position);
        tx.placed_attempt = None;
        tx.table_data.remove(&attempt_table);
    }
    Ok(())
}

/// A log whose payloads are sealed under the deployment's key, so a logkeeper never holds a row in the clear.
pub struct SealedLog {
    inner: Arc<dyn Log>,
    cipher: Arc<Cipher>,
    stream: Arc<str>,
    recovery: Option<RecoveryAdmission>,
}

impl SealedLog {
    pub fn new(inner: Arc<dyn Log>, key: &[u8], stream: &str) -> Result<Self> {
        Ok(Self { inner, cipher: Arc::new(Cipher::new(key)?), stream: Arc::from(stream), recovery: None })
    }

    fn for_serving(inner: Arc<dyn Log>, key: &[u8], config: &LogkeeperLogConfig, holds: Arc<RecoveryHolds>) -> Result<Self> {
        let mut log = Self::new(inner, key, &config.replica_seal_scope)?;
        log.recovery = Some(RecoveryAdmission::new(holds, config));
        Ok(log)
    }
}

fn opened(cipher: &Cipher, stream: &str, record: LogRecord) -> rtdb_storage::Result<LogRecord> {
    Ok(LogRecord { metadata: record.metadata, payload: cipher.open(stream, &record.payload)? })
}

#[async_trait]
impl Log for SealedLog {
    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> rtdb_storage::Result<AppendReceipt> {
        self.inner.enqueue(self.cipher.seal(&self.stream, &payload)?, timestamp).await
    }
    fn latest(&self) -> rtdb_storage::Result<LogOffset> { self.inner.latest() }
    async fn retained_floor(&self) -> rtdb_storage::Result<Option<LogOffset>> { self.inner.retained_floor().await }
    async fn recovery_tail(&self, after: LogOffset) -> rtdb_storage::Result<Option<Box<dyn LogTail>>> {
        let hold = match &self.recovery {
            Some(recovery) => recovery.hold(after, self.inner.latest()?).await?,
            None => None,
        };
        Ok(self.inner.recovery_tail(after).await?.map(|inner| {
            Box::new(SealedTail { inner, cipher: Arc::clone(&self.cipher), stream: Arc::clone(&self.stream), hold: hold.map(Arc::new), admission: self.recovery.clone() }) as Box<dyn LogTail>
        }))
    }
    async fn tail(&self, after: LogOffset) -> rtdb_storage::Result<Box<dyn LogTail>> {
        if let Some(admission) = &self.recovery { admission.check_start(after).await?; }
        Ok(Box::new(SealedTail { inner: self.inner.tail(after).await?, cipher: Arc::clone(&self.cipher), stream: Arc::clone(&self.stream), hold: None, admission: self.recovery.clone() }))
    }
    async fn range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Box<dyn LogRange>> {
        Ok(Box::new(SealedRange { inner: self.inner.range(from, to).await?, cipher: Arc::clone(&self.cipher), stream: Arc::clone(&self.stream), admission: self.recovery.clone() }))
    }
    async fn read_range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Vec<LogRecord>> {
        let mut records = self.inner.read_range(from, to).await?.into_iter()
            .map(|record| opened(&self.cipher, &self.stream, record)).collect::<rtdb_storage::Result<Vec<_>>>()?;
        if let Some(admission) = &self.recovery { admission.retain_offloaded(&mut records).await?; }
        Ok(records)
    }
}

struct SealedTail {
    inner: Box<dyn LogTail>,
    cipher: Arc<Cipher>,
    stream: Arc<str>,
    hold: Option<Arc<ReplayHold>>,
    admission: Option<RecoveryAdmission>,
}

impl SealedTail {
    async fn retain_deliveries(&mut self, records: &mut [LogRecord]) -> rtdb_storage::Result<()> {
        let mut covered = 0;
        if let Some(hold) = &self.hold {
            covered = records.partition_point(|record| record.metadata.offset <= hold.through);
            for record in &mut records[..covered] {
                record.payload = hold.retain_delivery(std::mem::take(&mut record.payload));
            }
            if records.last().is_some_and(|record| record.metadata.offset >= hold.through) { self.hold = None; }
        }
        if let Some(admission) = &self.admission { admission.retain_offloaded(&mut records[covered..]).await?; }
        Ok(())
    }
}

#[async_trait]
impl LogTail for SealedTail {
    async fn poll(&mut self, timeout: Duration) -> rtdb_storage::Result<Vec<LogRecord>> {
        let mut records = self.inner.poll(timeout).await?.into_iter()
            .map(|record| opened(&self.cipher, &self.stream, record)).collect::<rtdb_storage::Result<Vec<_>>>()?;
        self.retain_deliveries(&mut records).await?;
        Ok(records)
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
        let mut record = opened(&self.cipher, &self.stream, record)?;
        self.retain_deliveries(std::slice::from_mut(&mut record)).await?;
        Ok(record)
    }
}

struct SealedRange { inner: Box<dyn LogRange>, cipher: Arc<Cipher>, stream: Arc<str>, admission: Option<RecoveryAdmission> }

#[async_trait]
impl LogRange for SealedRange {
    fn allocation_bound(&self) -> usize { self.inner.allocation_bound() }
    async fn next(&mut self, reservation: ReadReservation) -> rtdb_storage::Result<Option<LogRecord>> {
        let Some(record) = self.inner.next(reservation).await? else { return Ok(None); };
        let mut record = opened(&self.cipher, &self.stream, record)?;
        if let Some(admission) = &self.admission { admission.retain_offloaded(std::slice::from_mut(&mut record)).await?; }
        Ok(Some(record))
    }
}

/// The holder's in-process source log: it remembers how far its reader has got, and drops what lies well behind it.
pub struct IntakeLog { log: MemoryLog, consumed: Arc<AtomicI64>, start: i64 }

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
        // Positions below the intake's start were never assigned to it, so a reader asking for them starts at the start; trimmed positions above it still fail as unavailable.
        let after = LogOffset(after.0.max(self.start - 1));
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
    use crate::keys::KeyService;
    use rtdb_format::log::proto;
    use rtdb_types::Term;

    #[tokio::test]
    async fn a_log_open_without_a_serving_key_never_reserves_journal_endpoints() {
        use rtdb_directory::{DeploymentRecord, Directory, MemberId, MemoryDirectory};

        let files = tempfile::tempdir().unwrap();
        let keys = Arc::new(crate::keys::LocalKeyService::new(files.path()).unwrap());
        let directory = MemoryDirectory::new();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.holder = Some(MemberId("holder".to_owned()));
        record.epoch = 1;
        let record = directory.create_deployment(record).await.unwrap();
        let configuration = LogkeeperLogConfig {
            servers: vec!["127.0.0.1:1".into()],
            deployment: record.id.clone(),
            key_id: record.key_id(),
            replica_stream: record.stream("replica"),
            replica_seal_scope: record.replica_seal_scope(),
            write_quorum: 1,
            epoch: record.epoch,
            require_read_index: false,
            log_epoch: record.log_epoch,
            transferred_source_through: None,
            frozen_at: None,
            previous_servers: None,
        };
        let configuration = Any { type_url: String::new(), value: serde_json::to_vec(&configuration).unwrap() };
        let holds = Arc::new(RecoveryHolds { directory: directory.clone(), keys: keys.clone(), custody: rtdb_directory::ReplayCustody::Local, tasks: Default::default() });
        let factory = LogkeeperLogFactory::new(1024, Default::default(), holds);

        let refused = factory.open(&configuration, LogAccess::ReadWrite, CancellationToken::new()).await.err().unwrap();
        assert_eq!(refused.code, "rtdb.multi/key-missing");
        assert!(directory.deployment(&record.id).await.unwrap().unwrap().replica_journals.is_empty());
        assert!(keys.existing_key(&record.key_id()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn recovery_refuses_a_pending_trim_before_the_logkeeper_applies_it() {
        use rtdb_directory::{Cas, Checkpoint, DeploymentRecord, Directory, JournalAnchor, MemoryDirectory, ReplayCustody};
        use rtdb_storage::Error;

        let directory = MemoryDirectory::new();
        let mut record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let config = LogkeeperLogConfig {
            servers: vec![],
            deployment: record.id.clone(),
            key_id: record.key_id(),
            replica_stream: record.stream("replica"),
            replica_seal_scope: record.replica_seal_scope(),
            previous_servers: None,
            epoch: record.epoch,
            require_read_index: true,
            frozen_at: None,
            log_epoch: record.log_epoch,
            transferred_source_through: None,
            write_quorum: 1,
        };
        let provider = Arc::new(MemoryLog::new(1024));
        let files = tempfile::tempdir().unwrap();
        let holds = Arc::new(RecoveryHolds {
            directory: directory.clone(), keys: Arc::new(crate::keys::LocalKeyService::new(files.path()).unwrap()),
            custody: ReplayCustody::Local, tasks: Default::default(),
        });
        let replica = SealedLog::for_serving(provider.clone(), &[7; 32], &config, holds.clone()).unwrap();
        for _ in 0..7 {
            replica.enqueue(Bytes::from_static(b"entry"), Timestamp(0)).await.unwrap().durable().await.unwrap();
        }
        assert!(replica.recovery_tail(LogOffset(3)).await.unwrap().is_some());

        holds.tasks.close();
        holds.tasks.wait().await;
        record = directory.deployment(&record.id).await.unwrap().unwrap();
        let version = record.version;
        record.checkpoint = Some(Checkpoint { block: 1, boundary: 5, source: 1, system_time: 0, root_sha256: None });
        record.replica_anchor = Some(Box::new(JournalAnchor { root_incarnation: record.incarnation, interval: [0, 4], digest: [0; 32] }));
        let Cas::Applied(mut record) = directory.replace_deployment(record, version).await.unwrap() else { panic!("failed to authorize trim"); };
        assert_eq!(provider.first_retained(), LogOffset(0));
        assert!(provider.recovery_tail(LogOffset(3)).await.unwrap().is_some());
        assert!(matches!(replica.tail(LogOffset(3)).await,
            Err(Error::UnavailableLogRange { requested: 4, first_retained: 5 })));
        assert!(matches!(replica.recovery_tail(LogOffset(3)).await,
            Err(Error::UnavailableLogRange { requested: 4, first_retained: 5 })), "authorized trim must exclude the required record before it is removed");
        assert!(replica.recovery_tail(LogOffset(4)).await.unwrap().is_some());

        holds.tasks.wait().await;
        record = directory.deployment(&record.id).await.unwrap().unwrap();
        let version = record.version;
        record.epoch += 1;
        let Cas::Applied(mut record) = directory.replace_deployment(record, version).await.unwrap() else { panic!("failed to change writer epoch"); };
        assert!(matches!(replica.recovery_tail(LogOffset(3)).await,
            Err(Error::UnavailableLogRange { requested: 4, first_retained: 5 })));
        assert!(replica.recovery_tail(LogOffset(5)).await.unwrap().is_some());

        holds.tasks.wait().await;
        record = directory.deployment(&record.id).await.unwrap().unwrap();
        let version = record.version;
        record.log_epoch += 1;
        assert!(matches!(directory.replace_deployment(record, version).await.unwrap(), Cas::Applied(_)));
        assert!(matches!(replica.recovery_tail(LogOffset(5)).await, Err(Error::Closed(_))));
    }

    #[tokio::test]
    async fn a_live_offload_after_the_startup_tip_pins_its_replica_position() {
        use rtdb_directory::{DeploymentRecord, Directory, MemoryDirectory, ReplayCustody};

        let directory = MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let config = LogkeeperLogConfig {
            servers: vec![], deployment: record.id.clone(), key_id: record.key_id(),
            replica_stream: record.stream("replica"), replica_seal_scope: record.replica_seal_scope(),
            write_quorum: 1, epoch: record.epoch, require_read_index: false, log_epoch: record.log_epoch,
            transferred_source_through: None, frozen_at: None, previous_servers: None,
        };
        let directory_files = tempfile::tempdir().unwrap();
        let holds = Arc::new(RecoveryHolds {
            directory: directory.clone(), keys: Arc::new(crate::keys::LocalKeyService::new(directory_files.path()).unwrap()),
            custody: ReplayCustody::Local, tasks: Default::default(),
        });
        let replica = SealedLog::for_serving(Arc::new(MemoryLog::new(1024)), &[7; 32], &config, holds.clone()).unwrap();
        let claim = ReplicaEnvelope { term: Term(1), term_sequence: None, message: ReplicaBody::NoOp(Default::default()) };
        replica.enqueue(Bytes::from(claim.encode()), Timestamp(0)).await.unwrap().durable().await.unwrap();
        let mut tail = replica.recovery_tail(LogOffset(-1)).await.unwrap().unwrap();
        let proxy = claim.offloaded(rtdb_format::CURRENT_STORAGE_VERSION, StorageEpoch(0), BlockIndex(1), "paused".into(), &claim.encode());
        replica.enqueue(Bytes::from(proxy.encode()), Timestamp(1)).await.unwrap().durable().await.unwrap();
        let mut records = tail.poll(Duration::from_secs(1)).await.unwrap();
        assert_eq!(records.len(), 2);
        let delivery = records.pop().unwrap().payload;
        drop((tail, records));

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let current = directory.deployment(&record.id).await.unwrap().unwrap();
                if current.required_replica_offsets(0).collect::<Vec<_>>() == [1] { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        let current = directory.deployment(&record.id).await.unwrap().unwrap();
        let first_required = current.required_replica_offsets(0).min().unwrap();
        assert_eq!(crate::replica_retention::replica_trim_through(3, first_required), Some(0));

        drop(delivery);
        holds.tasks.close();
        tokio::time::timeout(Duration::from_secs(5), holds.tasks.wait()).await.unwrap();
        assert!(directory.deployment(&record.id).await.unwrap().unwrap().storage_regions.is_empty());
    }

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
        for _ in 0..INTAKE_TIP_WINDOW {
            append(&replica, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None })).await;
        }
        assert_eq!(next_source_offset(&replica, StorageEpoch(0)).await.unwrap(), 42, "a claim consumes no source position");
        append(&replica, ReplicaBody::TriesAdded(Default::default())).await;
        assert_eq!(next_source_offset(&replica, StorageEpoch(0)).await.unwrap(), 43, "a record that names no position may have consumed one");
    }

    #[tokio::test]
    async fn the_intake_drops_what_lies_well_behind_its_reader() {
        let intake = IntakeLog { log: MemoryLog::starting_at(1 << 20, LogOffset(10)), consumed: Arc::new(AtomicI64::new(9)), start: 10 };
        for _ in 0..(INTAKE_RETAINED_BEHIND + 100) { intake.enqueue(Bytes::from_static(b"x"), Timestamp(0)).await.unwrap().durable().await.unwrap(); }
        let mut tail = intake.tail(LogOffset(9)).await.unwrap();
        let mut read = 0;
        while read < INTAKE_RETAINED_BEHIND + 100 { read += tail.poll(Duration::from_millis(10)).await.unwrap().len() as i64; }
        assert!(intake.tail(LogOffset(-1)).await.is_ok(), "a reader asking for positions from before the intake started begins at its start");
        intake.trim();
        assert_eq!(intake.log.first_retained(), LogOffset(10 + 100), "everything but the retained window behind the reader is dropped");
        assert!(intake.tail(LogOffset(50)).await.is_err(), "a trimmed position still fails");
    }
}
