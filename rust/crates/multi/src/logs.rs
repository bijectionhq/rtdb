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
use rtdb_format::{catalog::BlockCatalog, envelope::{ReplicaBody, ReplicaEnvelope, ReplicaFrame}, keys::CURRENT_STORAGE_VERSION};
use rtdb_logkeeper::{QuorumLog, StreamConfig};
use rtdb_node::{AwaitAuthority, ClosedEpochRecovery, LogAccess, LogFactory, OpenedLogs, PlacedReceiptCodec, ReplicaReadIndex};
use rtdb_replication::{TermFence, Verdict};
use rtdb_storage::log::{AppendReceipt, GuardedAppend, Log, LogOffset, LogRange, LogRecord, LogReplayTail, LogTail, MemoryLog, ReplayHead};
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore, object_store::ReadReservation};
use rtdb_types::{Anomaly, BlockIndex, JournalEndpoint, PLACED_ATTEMPT_OUTCOME_TABLE, PlacedAttemptScope, ReplicaPosition, Result, SourcePosition, StorageEpoch, Timestamp};
use tokio_util::sync::CancellationToken;

use crate::cipher::Cipher;
use crate::deployment::LogkeeperLogConfig;
use crate::keys::KeyService;
use crate::recovery_holds::{RecoveryAdmission, RecoveryHolds, ReplayHold};

mod units;
pub(crate) use units::JournalUnits;
pub(crate) use units::commits_payload_reference;
use units::OVERHEAD as UNIT_OVERHEAD;

const SCAN_WINDOW: i64 = 256;
const INTAKE_TIP_WINDOW: i64 = 8;
const READ_INDEX_WITHIN: Duration = Duration::from_secs(2);

struct LogletReadIndex {
    log: Arc<QuorumLog>,
    epoch: u64,
    custody: RecoveryAdmission,
    deployment: Option<uuid::Uuid>,
}

impl LogletReadIndex {
    async fn sample(&self) -> Result<rtdb_logkeeper::ActiveLogletReadIndex> {
        let index = tokio::time::timeout(READ_INDEX_WITHIN, self.log.read_index(self.epoch)).await
            .map_err(|_| Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.multi/read-index-timeout", "Logkeepers did not confirm a read index in time"))??;
        crate::residence_trace::record("read-index", || serde_json::json!({
            "stream": index.stream(), "epoch": index.epoch(), "ensemble": index.ensemble(), "through": index.through().0,
        }));
        Ok(index)
    }
}

#[async_trait]
impl ReplicaReadIndex for LogletReadIndex {
    fn deployment(&self) -> Option<uuid::Uuid> { self.deployment }
    async fn confirm(&self) -> Result<LogOffset> { Ok(self.sample().await?.through()) }

    async fn protect(&self, applied: LogOffset) -> Result<rtdb_node::ProtectedReadIndex> {
        let index = self.sample().await?;
        let offset = index.through();
        let replay = if applied < offset {
            self.custody.hold(applied, offset).await?
        } else {
            None
        };
        let certified_read = if rtdb_types::residence_trace::enabled() {
            index.decision().map(|proof| {
                let observation = Arc::new(rtdb_types::residence_trace::CertifiedRead {
                    invocation: index.invocation(), stream: index.stream().to_owned(), epoch: index.epoch(),
                    ensemble: index.ensemble().try_into().expect("the certified ensemble fingerprint"),
                    through: offset.0, decision: proof.certificate.statement.fingerprint(),
                });
                observation.observe("certified-read-index");
                observation
            })
        } else { None };
        Ok(rtdb_node::ProtectedReadIndex {
            offset,
            wait: READ_INDEX_WITHIN,
            retention: Some(Arc::new((index, replay))),
            certified_read,
        })
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
    let intake = Arc::new(IntakeLog::new(maximum_message_bytes, start));
    Ok(OpenedLogs { source: intake, replica, source_epoch: epoch, replica_epoch: epoch, replica_read_index: None,
        placed_transfer: None, await_authority: AwaitAuthority::Frozen, drivers: Vec::new() })
}

pub(crate) async fn ordered_opened(
    ordered: Arc<crate::journal::JournalLog>,
    key: &[u8],
    record: &rtdb_directory::DeploymentRecord,
    admission: crate::journal::WriterAdmission,
    checkpoint: Option<rtdb_directory::Checkpoint>,
    maximum_message_bytes: usize,
    cancellation: CancellationToken,
) -> Result<OpenedLogs> {
    let epoch = StorageEpoch(record.log_epoch);
    let opened = ordered.latest()?;
    let scope = PlacedAttemptScope::new(&record.stream("replica"), admission.grant.epoch)?;
    let replica: Arc<dyn Log> = ordered.clone();
    let terminal = record.checkpoint_transfer_activation.as_ref()
        .map(|activation| &activation.replica.bootstrap.terminal);
    let intake_cut = terminal.map(|terminal| {
        crate::deployment::TransferIntakeCut {
            replica_through: terminal.replica_through, source_through: terminal.source_through,
        }
    }).or_else(|| checkpoint.map(|block| crate::deployment::TransferIntakeCut {
        replica_through: block.boundary, source_through: block.source,
    }));
    let placed_transfer = terminal.map(rtdb_directory::CheckpointTransferTerminal::placed_transfer);
    let next = next_source_offset_from_cut(replica.as_ref(), epoch, include_placed_debt(intake_cut, placed_transfer.as_ref())?).await?;
    let intake = Arc::new(IntakeLog::new(maximum_message_bytes, next));
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
    Ok(OpenedLogs {
        source: intake, replica, source_epoch: epoch, replica_epoch: epoch,
        placed_transfer,
        replica_read_index: Some(Box::new(OrderedReadIndex { journal: ordered, deployment: uuid::Uuid::parse_str(&record.id).ok() })),
        await_authority: AwaitAuthority::Live {
            codec: Box::new(DeploymentReceipts { scope, cipher: Cipher::new(key)? }),
            recovery: ClosedEpochRecovery::Opened(opened),
            placed_receipt_retirement: record.placed_receipt_retirement,
        },
        drivers: vec![trimming],
    })
}

struct OrderedReadIndex { journal: Arc<crate::journal::JournalLog>, deployment: Option<uuid::Uuid> }

#[async_trait]
impl ReplicaReadIndex for OrderedReadIndex {
    fn deployment(&self) -> Option<uuid::Uuid> { self.deployment }
    async fn confirm(&self) -> Result<LogOffset> { self.journal.confirm().await }
    async fn protect(&self, applied: LogOffset) -> Result<rtdb_node::ProtectedReadIndex> { self.journal.protect(applied).await }
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
                        let moved = rtdb_logkeeper::open_writer_on(vec![rtdb_logkeeper::Loglet { servers: previous.clone(), start_lsn: 0, write_quorum: crate::deployment::write_quorum(previous.len()) }], stream, config.epoch, rtdb_logkeeper::CopyFrom::Retained, cancellation.clone()).await?;
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
        let read_index = || -> Box<dyn ReplicaReadIndex> {
            Box::new(LogletReadIndex {
                log: Arc::clone(&quorum), epoch: config.epoch,
                custody: RecoveryAdmission::new(Arc::clone(&self.holds), &config),
                deployment: uuid::Uuid::parse_str(&config.deployment).ok(),
            })
        };
        let replica_read_index = config.require_read_index.then(&read_index);
        let await_authority = if let Some(scope) = scope {
            let recovery = if access == LogAccess::ReadWrite {
                ClosedEpochRecovery::Opened(quorum.latest()?)
            } else {
                ClosedEpochRecovery::ReadIndex(read_index())
            };
            AwaitAuthority::Live { codec: Box::new(DeploymentReceipts { scope, cipher: Cipher::new(&key)? }), recovery,
                placed_receipt_retirement: config.placed_receipt_retirement }
        } else { AwaitAuthority::Frozen };
        let replica: Arc<dyn Log> = Arc::new(SealedLog::for_serving(quorum, &config, Arc::clone(&self.holds)).await?);
        let intake_cut = include_placed_debt(config.transfer_intake_cut, config.placed_transfer.as_ref())?;
        let mut next = next_source_offset_from_cut(replica.as_ref(), epoch, intake_cut).await?;
        if let Some(through) = config.transferred_source_through {
            let floor = through.checked_add(1).filter(|floor| *floor >= 0)
                .ok_or_else(|| Anomaly::fault("rtdb.multi/intake-floor", "Invalid transferred source offset"))?;
            SourcePosition::from_epoch_offset(epoch, floor)?;
            next = next.max(floor);
        }
        if config.transfer_intake_cut.is_some() { SourcePosition::from_epoch_offset(epoch, next)?; }
        let intake = Arc::new(IntakeLog::new(self.maximum_message_bytes, next));
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
            replica_read_index, placed_transfer: config.placed_transfer.clone(), await_authority, drivers: vec![trimming] })
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
    next_source_offset_from_cut(replica, epoch, None).await
}

fn include_placed_debt(
    cut: Option<crate::deployment::TransferIntakeCut>,
    transfer: Option<&rtdb_types::PlacedTransfer>,
) -> Result<Option<crate::deployment::TransferIntakeCut>> {
    let Some(transfer) = transfer else { return Ok(cut); };
    let Some(debt) = &transfer.debt else { return Ok(cut); };
    let mut cut = cut.ok_or_else(|| Anomaly::fault(
        "rtdb.multi/intake-floor", "Transferred receiver debt has no terminal intake cut",
    ))?;
    if transfer.through != cut.replica_through {
        return Err(Anomaly::fault("rtdb.multi/intake-floor", "Receiver debt belongs to another terminal cut"));
    }
    let reserved = rtdb_types::PlacedReservation::decode(&debt.reservation)?;
    cut.source_through = cut.source_through.max(reserved.attempt.position().offset());
    Ok(Some(cut))
}

async fn next_source_offset_from_cut(
    replica: &dyn Log, epoch: StorageEpoch, cut: Option<crate::deployment::TransferIntakeCut>,
) -> Result<i64> {
    let invalid_floor = || Anomaly::fault("rtdb.multi/intake-floor", "Invalid transferred source or replica offset");
    let latest = replica.latest()?.0;
    let mut end = latest.checked_add(1).ok_or_else(invalid_floor)?;
    let (mut floor, source_floor) = if let Some(cut) = cut {
        if cut.replica_through < -1 || cut.source_through < -1 || cut.replica_through > latest { return Err(invalid_floor()); }
        ReplicaPosition::from_epoch_offset(epoch, cut.replica_through)?;
        SourcePosition::from_epoch_offset(epoch, cut.source_through)?;
        (cut.replica_through.checked_add(1).ok_or_else(invalid_floor)?,
         cut.source_through.checked_add(1).ok_or_else(invalid_floor)?)
    } else { (0, 0) };
    let mut after_newest = 0i64;
    let mut missing = 0i64;
    let mut observed_next = 0i64;
    let mut window = INTAKE_TIP_WINDOW;
    while end > floor {
        let start = end.saturating_sub(window).max(floor);
        let records = match replica.read_range(LogOffset(start), LogOffset(end)).await {
            Ok(records) => records,
            Err(rtdb_storage::Error::UnavailableLogRange { first_retained, .. }) if first_retained > start => {
                missing += first_retained.min(end) - floor;
                floor = first_retained;
                continue;
            }
            Err(rtdb_storage::Error::UnavailableLogRange { .. }) if cut.is_none() => break,
            Err(error) => return Err(error.into()),
        };
        for record in records.iter().rev() {
            match source_position(&record.payload, epoch)? {
                Scanned::Position(position) => {
                    let next = position.checked_add(1).and_then(|next| next.checked_add(after_newest)).ok_or_else(invalid_floor)?;
                    if cut.is_none() { return Ok(next); }
                    observed_next = observed_next.max(next);
                    after_newest += 1;
                }
                Scanned::MayConsume => after_newest += 1,
                Scanned::Consumes => {}
            }
        }
        end = start;
        window = SCAN_WINDOW;
    }
    let anchored = source_floor.checked_add(after_newest)
        .and_then(|next| next.checked_add(if cut.is_some() { missing } else { 0 }))
        .ok_or_else(invalid_floor)?;
    Ok(observed_next.max(anchored))
}

enum Scanned { Position(i64), MayConsume, Consumes }

/// What one replica record says about source positions allocated up to it.
/// A claim or heartbeat (a no-op naming no source position) and a leader's own deletion notice consume none; anything else whose position it does not name may have consumed one.
fn source_position(payload: &[u8], epoch: StorageEpoch) -> Result<Scanned> {
    let Some(envelope) = ReplicaEnvelope::decode(payload)? else { return Ok(Scanned::MayConsume); };
    let position = match &envelope.message {
        ReplicaBody::ResolvedTx(tx) => tx.src_msg_id,
        ReplicaBody::NoOp(noop) => match noop.placed_slot_reservation.as_deref() {
            Some(wire) => Some(rtdb_types::PlacedReservation::decode(wire)?.attempt.position().0),
            None => match noop.src_msg_id { None => return Ok(Scanned::Consumes), position => position },
        },
        ReplicaBody::BlockBoundary(boundary) => boundary.latest_processed_msg_id,
        ReplicaBody::BlockUploaded(upload) => upload.latest_processed_msg_id,
        ReplicaBody::TriesDeleted(_) | ReplicaBody::TablesRetired(_) => return Ok(Scanned::Consumes),
        _ => None,
    };
    Ok(match position.filter(|position| *position >= 0).map(SourcePosition).filter(|position| position.epoch() == epoch) {
        Some(position) => Scanned::Position(position.offset()),
        None => Scanned::MayConsume,
    })
}

/// Materializes admitted transactions up to the requested time in the confirmed replica prefix.
pub(crate) async fn transactions_until(
    replica: &dyn Log, store: &dyn ObjectStore, catalog: &BlockCatalog, boundary: i64, through: LogOffset, system_time: Timestamp,
) -> Result<Vec<ReplicaEnvelope>> {
    let mut kept = Vec::new();
    let mut fence = TermFence::from_catalog(catalog)?;
    let mut next = boundary;
    let gap = || Anomaly::fault("rtdb.multi/fork-replay-gap", "The retained journal did not deliver the complete fork interval");
    if next > through.0 { return Err(gap()); }
    if next == through.0 { return Ok(Vec::new()); }
    let mut tail = replica.tail(LogOffset(boundary)).await?;
    'capture: while next < through.0 {
        for record in tail.poll(Duration::from_millis(250)).await? {
            if next >= through.0 { break 'capture; }
            if record.metadata.offset.0 != next + 1 { return Err(gap()); }
            next = record.metadata.offset.0;
            let frame = ReplicaFrame::decode(&record.payload)?;
            if fence.admit(frame.term, frame.term_sequence)?.verdict != Verdict::Apply { continue; }
            let envelope = frame.into_envelope().ok_or_else(gap)?;
            let envelope = if let Some(key) = envelope.oversized_payload(CURRENT_STORAGE_VERSION, StorageEpoch(0))? {
                let key = ObjectKey::new(key)?;
                IoRegion::retaining(Arc::new(record), async {
                    let bytes = store.get_fresh(&key).await?;
                    Ok::<_, Anomaly>(envelope.unwrap_payload(&bytes)?)
                }).await?
            } else { envelope };
            if let ReplicaBody::ResolvedTx(tx) = envelope.message {
                tx.checked_acceptance_stage()?;
                let time = tx.system_time_micros.ok_or_else(gap)?;
                if time > system_time.0 { break 'capture; }
                kept.push(ReplicaBody::ResolvedTx(tx));
            }
        }
    }
    let term = fence.claim_term()?;
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
        tx.placed_slot_terminal = None;
        tx.acceptance_stage = None;
        tx.table_data.remove(&attempt_table);
    }
    Ok(())
}

/// Directory-managed payloads enter the journal as independently sealed publications.
pub struct SealedLog {
    inner: Arc<dyn Log>,
    units: Arc<JournalUnits>,
    recovery: Option<RecoveryAdmission>,
}

impl SealedLog {
    pub async fn new(inner: Arc<dyn Log>, keys: &dyn KeyService, residence: &str, scope: &str, io: Arc<IoRegion>) -> Result<Self> {
        let units = Arc::new(JournalUnits::open(keys, residence, scope, io).await?);
        Ok(Self { inner, units, recovery: None })
    }

    async fn for_serving(inner: Arc<dyn Log>, config: &LogkeeperLogConfig, holds: Arc<RecoveryHolds>) -> Result<Self> {
        let mut log = Self::new(inner, holds.keys.as_ref(), &config.key_id, &config.replica_seal_scope, IoRegion::new()).await?;
        log.recovery = Some(RecoveryAdmission::new(holds, config));
        Ok(log)
    }
}

#[async_trait]
impl Log for SealedLog {
    fn requires_replica_payload_permit(&self) -> bool { self.recovery.is_some() }
    fn requires_checkpoint_digest(&self) -> bool { self.inner.requires_checkpoint_digest() }
    fn needs_published_recovery_checkpoint(&self) -> bool {
        self.recovery.is_some() || self.inner.needs_published_recovery_checkpoint()
    }

    async fn confirms_recovery_checkpoint(&self, block: i64, through: LogOffset, root: [u8; 32]) -> rtdb_storage::Result<bool> {
        match &self.recovery {
            Some(owner) => owner.confirms_checkpoint(block, through, root).await,
            None => self.inner.confirms_recovery_checkpoint(block, through, root).await,
        }
    }
    fn fences_writer_terms(&self) -> bool { self.inner.fences_writer_terms() }
    async fn holds_writer_grant(&self) -> rtdb_storage::Result<bool> { self.inner.holds_writer_grant().await }
    fn maximum_append_bytes(&self) -> Option<usize> {
        self.inner.maximum_append_bytes()?.checked_sub(UNIT_OVERHEAD)
    }
    fn maximum_guarded_append_bytes(&self) -> Option<usize> {
        self.inner.maximum_guarded_append_bytes()?.checked_sub(UNIT_OVERHEAD)
    }
    fn maximum_timed_original_append_bytes(&self) -> Option<usize> {
        self.inner.maximum_timed_original_append_bytes()?.checked_sub(UNIT_OVERHEAD)
    }

    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> rtdb_storage::Result<AppendReceipt> {
        let outcome = crate::residence_trace::AttemptOutcome::from_replica(&payload);
        let receipt = self.inner.enqueue(self.units.seal(payload).await?, timestamp).await?;
        Ok(observe_attempt(receipt, outcome))
    }
    async fn enqueue_maintenance(&self, payload: Bytes, timestamp: Timestamp) -> rtdb_storage::Result<AppendReceipt> {
        self.inner.enqueue_maintenance(self.units.seal(payload).await?, timestamp).await
    }
    async fn available_append_capacity(&self) -> rtdb_storage::Result<Option<rtdb_storage::log::AppendCapacity>> {
        self.inner.available_append_capacity().await
    }
    async fn enqueue_guarded(&self, payload: Bytes, guard: GuardedAppend<'_>) -> rtdb_storage::Result<AppendReceipt> {
        let outcome = crate::residence_trace::AttemptOutcome::from_replica(&payload);
        let receipt = self.inner.enqueue_guarded(self.units.seal(payload).await?, guard).await?;
        Ok(observe_attempt(receipt, outcome))
    }
    fn latest(&self) -> rtdb_storage::Result<LogOffset> { self.inner.latest() }
    async fn retained_floor(&self) -> rtdb_storage::Result<Option<LogOffset>> { self.inner.retained_floor().await }
    async fn recovery_tail(&self, after: LogOffset) -> rtdb_storage::Result<Option<Box<dyn LogTail>>> {
        let hold = match &self.recovery {
            Some(recovery) => recovery.hold(after, self.inner.latest()?).await?,
            None => None,
        };
        Ok(self.inner.recovery_tail(after).await?.map(|inner| {
            Box::new(SealedTail { inner, units: Arc::clone(&self.units), hold: hold.map(Arc::new), admission: self.recovery.clone() }) as Box<dyn LogTail>
        }))
    }
    async fn tail(&self, after: LogOffset) -> rtdb_storage::Result<Box<dyn LogTail>> {
        if let Some(admission) = &self.recovery { admission.check_start(after).await?; }
        Ok(Box::new(SealedTail { inner: self.inner.tail(after).await?, units: Arc::clone(&self.units), hold: None, admission: self.recovery.clone() }))
    }
    async fn range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Box<dyn LogRange>> {
        Ok(Box::new(SealedRange { inner: self.inner.range(from, to).await?, units: Arc::clone(&self.units), admission: self.recovery.clone() }))
    }
    async fn read_range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Vec<LogRecord>> {
        let mut records = open_records(&self.units, self.inner.read_range(from, to).await?).await?;
        if let Some(admission) = &self.recovery { admission.retain_offloaded(&mut records).await?; }
        Ok(records)
    }
}

fn observe_attempt(receipt: AppendReceipt, outcome: Option<crate::residence_trace::AttemptOutcome>) -> AppendReceipt {
    let Some(outcome) = outcome else { return receipt; };
    AppendReceipt::observed(async move {
        let metadata = receipt.durable().await?;
        outcome.observe(metadata);
        Ok(metadata)
    })
}

struct SealedTail {
    inner: Box<dyn LogTail>,
    units: Arc<JournalUnits>,
    hold: Option<Arc<ReplayHold>>,
    admission: Option<RecoveryAdmission>,
}

impl SealedTail {
    async fn retain_deliveries(&mut self, records: &mut [LogRecord]) -> rtdb_storage::Result<()> {
        let mut covered = 0;
        if let Some(hold) = &self.hold {
            covered = records.partition_point(|record| record.metadata.offset <= hold.through);
            for record in &mut records[..covered] {
                hold.retain_record(record)?;
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
        let mut records = open_records(&self.units, self.inner.poll(timeout).await?).await?;
        self.retain_deliveries(&mut records).await?;
        Ok(records)
    }
    fn replay(&mut self) -> Option<&mut dyn LogReplayTail> { if self.inner.replay().is_some() { Some(self) } else { None } }
}

#[async_trait]
impl LogReplayTail for SealedTail {
    async fn poll_head(&mut self, timeout: Duration) -> rtdb_storage::Result<Option<ReplayHead>> {
        let head = self.inner.replay().expect("checked when replay was offered").poll_head(timeout).await?;
        head.map(|head| {
            let payload_len = head.payload_len.checked_sub(UNIT_OVERHEAD)
                .ok_or_else(|| rtdb_storage::Error::Task("Truncated sealed journal record".into()))?;
            let allocation_bound = head.allocation_bound.checked_mul(2)
                .ok_or_else(|| rtdb_storage::Error::Task("Journal replay allocation overflow".into()))?;
            Ok(ReplayHead { payload_len, allocation_bound, ..head })
        }).transpose()
    }
    async fn read_head(&mut self, head: ReplayHead, reservation: ReadReservation) -> rtdb_storage::Result<LogRecord> {
        let payload_len = head.payload_len.checked_add(UNIT_OVERHEAD)
            .ok_or_else(|| rtdb_storage::Error::Task("Journal record length overflow".into()))?;
        let sealed = ReplayHead { payload_len, allocation_bound: head.allocation_bound / 2, ..head };
        if reservation.length() < head.allocation_bound {
            return Err(rtdb_storage::Error::Task("Journal replay reservation is too small".into()));
        }
        let record = self.inner.replay().expect("checked when replay was offered").read_head(sealed, reservation).await?;
        let mut record = self.units.open_record(record).await?;
        self.retain_deliveries(std::slice::from_mut(&mut record)).await?;
        Ok(record)
    }
}

async fn open_records(units: &JournalUnits, records: Vec<LogRecord>) -> rtdb_storage::Result<Vec<LogRecord>> {
    let mut opened = Vec::with_capacity(records.len());
    for record in records {
        opened.push(units.open_record(record).await?);
    }
    Ok(opened)
}

struct SealedRange { inner: Box<dyn LogRange>, units: Arc<JournalUnits>, admission: Option<RecoveryAdmission> }

#[async_trait]
impl LogRange for SealedRange {
    fn allocation_bound(&self) -> usize { self.inner.allocation_bound().saturating_mul(2) }
    async fn next(&mut self, reservation: ReadReservation) -> rtdb_storage::Result<Option<LogRecord>> {
        if reservation.length() < self.allocation_bound() {
            return Err(rtdb_storage::Error::Task("Journal range reservation is too small".into()));
        }
        let Some(record) = self.inner.next(reservation).await? else { return Ok(None); };
        let mut record = self.units.open_record(record).await?;
        if let Some(admission) = &self.admission { admission.retain_offloaded(std::slice::from_mut(&mut record)).await?; }
        Ok(Some(record))
    }
}

/// The holder's in-process source log keeps pending bytes until their replica outcomes are installed.
pub struct IntakeLog { log: MemoryLog, consumed: AtomicI64, start: i64 }

impl IntakeLog {
    fn new(maximum_message_bytes: usize, start: i64) -> Self {
        let capacity = maximum_message_bytes.saturating_mul(2).max(16 * 1024 * 1024);
        Self {
            log: MemoryLog::bounded_starting_at(maximum_message_bytes, LogOffset(start), capacity),
            consumed: AtomicI64::new(start - 1),
            start,
        }
    }

    fn trim(&self) {
        let boundary = self.consumed.load(Ordering::Acquire).saturating_add(1);
        if boundary > self.log.first_retained().0 { let _ = self.log.truncate_before(LogOffset(boundary)); }
    }
}

#[async_trait]
impl Log for IntakeLog {
    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> rtdb_storage::Result<AppendReceipt> {
        self.trim();
        self.log.enqueue(payload, timestamp).await
    }
    fn latest(&self) -> rtdb_storage::Result<LogOffset> { self.log.latest() }
    fn record_applied(&self, through: LogOffset) {
        self.consumed.fetch_max(through.0, Ordering::AcqRel);
    }
    async fn tail(&self, after: LogOffset) -> rtdb_storage::Result<Box<dyn LogTail>> {
        // Positions below the intake's start were never assigned to it, so a reader asking for them starts at the start; trimmed positions above it still fail as unavailable.
        let after = LogOffset(after.0.max(self.start - 1));
        self.log.tail(after).await
    }
    async fn range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Box<dyn LogRange>> { self.log.range(from, to).await }
    async fn read_range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Vec<LogRecord>> { self.log.read_range(from, to).await }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::KeyService;
    use rtdb_format::log::proto;
    use rtdb_types::Term;

    #[tokio::test]
    async fn inherited_receiver_debt_keeps_its_source_position_across_fresh_intake() {
        use rtdb_types::{PlacedReservation, PlacedTransfer, PlacedTransferDebt};

        let epoch = StorageEpoch(2);
        let position = SourcePosition::from_epoch_offset(epoch, 94).unwrap();
        let attempt = PlacedAttemptScope::new("original", 3).unwrap().issue()
            .identify(position, Some("sender")).unwrap();
        let reserved = PlacedReservation::new(attempt, Timestamp(1), b"original");
        let transfer = PlacedTransfer {
            after: 2,
            through: 8,
            debt: Some(PlacedTransferDebt { term: 3, reserved_at: 7, reservation: reserved.encode() }),
        };
        let cut = crate::deployment::TransferIntakeCut { replica_through: 8, source_through: 10 };
        let journal = MemoryLog::starting_at(1024, LogOffset(9));
        let sealed = include_placed_debt(Some(cut), Some(&transfer)).unwrap();
        assert_eq!(next_source_offset_from_cut(&journal, epoch, sealed).await.unwrap(), 95);

        let stale = crate::deployment::TransferIntakeCut { replica_through: 7, source_through: 10 };
        assert!(include_placed_debt(Some(stale), Some(&transfer)).is_err());
    }

    #[tokio::test]
    async fn a_fork_materializes_only_admitted_offloaded_frames() {
        use rtdb_format::log::proto;
        use rtdb_types::Term;

        let journal = MemoryLog::new(1024);
        let store = rtdb_storage::MemoryStore::default();
        let catalog = BlockCatalog::from_proto(rtdb_format::block::proto::Block {
            block_index: Some(0), term_id: Some(2), boundary_term_seq: Some(0), ..Default::default()
        }).unwrap();
        let transaction = ReplicaEnvelope {
            term: Term(2), term_sequence: Some(rtdb_types::TermSequence(1)),
            message: ReplicaBody::ResolvedTx(proto::ResolvedTx {
                tx_id: Some(9), system_time_micros: Some(10), committed: Some(true), ..Default::default()
            }),
        };
        let bytes: Bytes = transaction.encode().into();
        let reference = transaction.offloaded(CURRENT_STORAGE_VERSION, StorageEpoch(0), BlockIndex(1), "good".into(), &bytes);
        let path = ObjectKey::new(reference.oversized_payload(CURRENT_STORAGE_VERSION, StorageEpoch(0)).unwrap().unwrap()).unwrap();
        let mut stale = reference.clone();
        stale.term = Term(1);
        let ReplicaBody::OversizedMessage(pointer) = &mut stale.message else { unreachable!() };
        pointer.payload_id = Some("missing".into());
        journal.enqueue(stale.encode().into(), Timestamp(9)).await.unwrap().durable().await.unwrap();
        journal.enqueue(reference.encode().into(), Timestamp(10)).await.unwrap().durable().await.unwrap();
        store.put(&path, bytes).await.unwrap();

        let tail = transactions_until(&journal, &store, &catalog, -1, LogOffset(1), Timestamp(10)).await.unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].message, transaction.message);

        store.delete(&path).await.unwrap();
        store.put(&path, Bytes::from_static(b"replacement")).await.unwrap();
        let error = transactions_until(&journal, &store, &catalog, -1, LogOffset(1), Timestamp(10)).await.unwrap_err();
        assert_eq!(error.code, "rtdb.format/oversized-payload-mismatch");
    }

    #[tokio::test]
    async fn a_time_fork_waits_for_a_confirmed_but_unpublished_record() {
        use rtdb_format::log::proto;
        use rtdb_types::Term;

        let journal = MemoryLog::new(1024);
        let store = rtdb_storage::MemoryStore::default();
        let catalog = BlockCatalog::from_proto(rtdb_format::block::proto::Block {
            block_index: Some(0), term_id: Some(2), boundary_term_seq: Some(0), ..Default::default()
        }).unwrap();
        let transaction = ReplicaEnvelope {
            term: Term(2), term_sequence: Some(rtdb_types::TermSequence(1)),
            message: ReplicaBody::ResolvedTx(proto::ResolvedTx {
                tx_id: Some(9), system_time_micros: Some(10), committed: Some(true), ..Default::default()
            }),
        };

        let copy = transactions_until(&journal, &store, &catalog, -1, LogOffset(0), Timestamp(10));
        tokio::pin!(copy);
        assert!(futures::poll!(copy.as_mut()).is_pending());
        journal.enqueue(transaction.encode().into(), Timestamp(10)).await.unwrap().durable().await.unwrap();
        let copied = tokio::time::timeout(Duration::from_secs(1), copy).await.unwrap().unwrap();
        assert_eq!(copied.len(), 1);
        assert_eq!(copied[0].message, transaction.message);
    }

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
            placed_receipt_retirement: false,
            log_epoch: record.log_epoch,
            transferred_source_through: None,
            transfer_intake_cut: None, placed_transfer: None,
            frozen_at: None,
            previous_servers: None,
        };
        let configuration = Any { type_url: String::new(), value: serde_json::to_vec(&configuration).unwrap() };
        let holds = Arc::new(RecoveryHolds { directory: directory.clone(), keys: keys.clone(),
            storage: crate::deployment::CellStorage::Local { root: files.path().to_owned() }, custody: rtdb_directory::ReplayCustody::Local, tasks: Default::default() });
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
            placed_receipt_retirement: false,
            frozen_at: None,
            log_epoch: record.log_epoch,
            transferred_source_through: None,
            transfer_intake_cut: None, placed_transfer: None,
            write_quorum: 1,
        };
        let provider = Arc::new(MemoryLog::new(1024));
        let files = tempfile::tempdir().unwrap();
        let holds = Arc::new(RecoveryHolds {
            directory: directory.clone(), keys: Arc::new(crate::keys::LocalKeyService::new(files.path()).unwrap()),
            storage: crate::deployment::CellStorage::Local { root: files.path().to_owned() },
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
            transferred_source_through: None, transfer_intake_cut: None, placed_transfer: None, frozen_at: None, previous_servers: None,
            placed_receipt_retirement: false,
        };
        let directory_files = tempfile::tempdir().unwrap();
        let holds = Arc::new(RecoveryHolds {
            directory: directory.clone(), keys: Arc::new(crate::keys::LocalKeyService::new(directory_files.path()).unwrap()),
            storage: crate::deployment::CellStorage::Local { root: directory_files.path().to_owned() },
            custody: ReplayCustody::Local, tasks: Default::default(),
        });
        let replica = SealedLog::for_serving(Arc::new(MemoryLog::new(1024)), &[7; 32], &config, holds.clone()).unwrap();
        let claim = ReplicaEnvelope { term: Term(1), term_sequence: None, message: ReplicaBody::NoOp(Default::default()) };
        replica.enqueue(Bytes::from(claim.encode()), Timestamp(0)).await.unwrap().durable().await.unwrap();
        let mut tail = replica.recovery_tail(LogOffset(-1)).await.unwrap().unwrap();
        let proxy = claim.offloaded(rtdb_format::keys::CURRENT_STORAGE_VERSION, StorageEpoch(0), BlockIndex(1), "paused".into(), &claim.encode());
        replica.enqueue(Bytes::from(proxy.encode()), Timestamp(1)).await.unwrap().durable().await.unwrap();
        let mut records = tail.poll(Duration::from_secs(1)).await.unwrap();
        assert_eq!(records.len(), 2);
        let delivery = records.pop().unwrap();
        let permit = delivery.replica_objects.as_ref().unwrap();
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

        let ticket = *current.storage_regions.keys().next().unwrap();
        crate::store::release_region(directory.as_ref(), &record.id, ticket, false).await.unwrap();
        let store = rtdb_storage::MemoryStore::default();
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        assert!(matches!(permit.redeem(&store, &memory).await, Err(rtdb_storage::Error::Closed(_))));
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
        append(&replica, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() })).await;
        append(&replica, ReplicaBody::ResolvedTx(proto::ResolvedTx { src_msg_id: Some(41), ..Default::default() })).await;
        assert_eq!(next_source_offset(&replica, StorageEpoch(0)).await.unwrap(), 42);
        for _ in 0..INTAKE_TIP_WINDOW {
            append(&replica, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() })).await;
        }
        assert_eq!(next_source_offset(&replica, StorageEpoch(0)).await.unwrap(), 42, "a claim consumes no source position");
        append(&replica, ReplicaBody::TriesAdded(Default::default())).await;
        assert_eq!(next_source_offset(&replica, StorageEpoch(0)).await.unwrap(), 43, "a record that names no position may have consumed one");
    }

    #[tokio::test]
    async fn transferred_intake_counts_only_after_the_ordered_replica_cut() {
        use crate::deployment::TransferIntakeCut;

        let replica = MemoryLog::starting_at(1 << 20, LogOffset(502));
        let cut = TransferIntakeCut { replica_through: 501, source_through: 1000 };
        assert_eq!(next_source_offset_from_cut(&replica, StorageEpoch(0), Some(cut)).await.unwrap(), 1001);
        append(&replica, ReplicaBody::OversizedMessage(proto::OversizedMessage::default())).await;
        append(&replica, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() })).await;
        assert_eq!(next_source_offset_from_cut(&replica, StorageEpoch(0), Some(cut)).await.unwrap(), 1002);
        let earlier = TransferIntakeCut { replica_through: 499, ..cut };
        assert_eq!(next_source_offset_from_cut(&replica, StorageEpoch(0), Some(earlier)).await.unwrap(), 1004);
    }

    #[tokio::test]
    async fn the_intake_releases_applied_records_and_keeps_pending_records() {
        let intake = IntakeLog::new(1 << 20, 10);
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
}
