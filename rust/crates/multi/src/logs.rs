//! The logs a node opens a deployment's database with.
//!
//! The replica stream lives on the deployment's logkeeper ensemble: a holder opens it as the writer at its epoch, which seals it, so no earlier holder's append is accepted; a subscriber opens it as a reader.
//! The source log is the holder's in-process intake: only the holder submits, the holder alone resolves, and a resolved transaction is durable once its replica record commits, so a transaction costs one quorum append.
//! A submission the holder had not yet resolved when it stopped was never acknowledged, and its client retries it.

use std::{ops::AsyncFn, sync::Arc};
use std::time::Duration;

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures::FutureExt;
use prost_types::Any;
use rtdb_format::{catalog::BlockCatalog, envelope::{ReplicaBody, ReplicaEnvelope, ReplicaFrame, ReplicaPeek, ReplicaPeekBody}, keys::CURRENT_STORAGE_VERSION};
use rtdb_logkeeper::{QuorumLog, StreamConfig};
use rtdb_node::{AwaitAuthority, ClosedEpochRecovery, LogAccess, LogFactory, OpenedLogs, PlacedReceiptCodec, ReplicaReadIndex};
use rtdb_replication::{TermFence, Verdict};
use rtdb_storage::log::{AppendReceipt, GuardedAppend, IntakeLog, Log, LogOffset, LogRange, LogRecord, LogReplayTail, LogTail, MemoryLog, ReplayHead};
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore, object_store::ReadReservation};
use rtdb_types::{Anomaly, BlockIndex, JournalEndpoint, PLACED_ATTEMPT_OUTCOME_TABLE, PlacedAttemptScope, ReplicaPosition, Result, SourcePosition, StorageEpoch, Timestamp};
use tokio_util::sync::CancellationToken;

use crate::cipher::Cipher;
use crate::deployment::LogkeeperLogConfig;
use crate::keys::KeyService;
use crate::recovery_holds::{RecoveryAdmission, RecoveryHolds, ReplayHold};

mod units;
pub(crate) use units::JournalUnits;
pub(crate) use units::{commits_payload_reference, payload_reference_digest};
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
const SQL_SNAPSHOT_ASSOCIATED: &str = "rtdb.multi/sql-snapshot/v2";

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

    fn seal_snapshot(&self, value: &[u8]) -> Result<String> {
        Ok(URL_SAFE_NO_PAD.encode(self.cipher.seal(SQL_SNAPSHOT_ASSOCIATED, value)?))
    }

    fn open_snapshot(&self, token: &str) -> Result<Vec<u8>> {
        let invalid = || Anomaly::incorrect("rtdb/invalid-snapshot-token", "Snapshot token is invalid for this database");
        let sealed = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?;
        self.cipher.open(SQL_SNAPSHOT_ASSOCIATED, &sealed).map(|bytes| bytes.to_vec()).map_err(|_| invalid())
    }

    fn seal_range(&self, value: &[u8]) -> Result<String> {
        Ok(URL_SAFE_NO_PAD.encode(self.cipher.seal("rtdb.multi/owned-range/v1", value)?))
    }

    fn open_range(&self, token: &str) -> Result<Vec<u8>> {
        let invalid = || Anomaly::incorrect("rtdb.owned/durable-range", "Invalid placed range witness");
        if token.len() > 2 * rtdb_node::MAX_DURABLE_RANGE_BYTES { return Err(invalid()); }
        let sealed = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?;
        let bytes = self.cipher.open("rtdb.multi/owned-range/v1", &sealed).map_err(|_| invalid())?;
        if bytes.len() > rtdb_node::MAX_DURABLE_RANGE_BYTES { return Err(invalid()); }
        Ok(bytes.to_vec())
    }
}

/// The writers a node has opened, by deployment, for confirming leadership before a linearizable read.
pub type Writers = Arc<parking_lot::Mutex<std::collections::HashMap<String, std::sync::Weak<rtdb_logkeeper::QuorumLog>>>>;

/// Records a completed ensemble move, as the holder that made it at `epoch`, before anything is appended on the new ensemble.
#[async_trait]
pub trait EnsembleRecorder: Send + Sync {
    /// Records every endpoint a copy can touch before any bytes move.
    async fn reserve_copy(&self, deployment: &str, epoch: u64, servers: &[JournalEndpoint]) -> Result<()>;
    async fn record(
        &self, deployment: &str, epoch: u64, ensemble: Vec<rtdb_directory::MemberId>,
        handoff: &rtdb_logkeeper::HandoffCertificate,
    ) -> Result<()>;
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
            if self.directory.replace_deployment(record, version).await.into_applied()?.is_some() { return Ok(()); }
        }
        Err(Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "rtdb.multi/record-contended", "The deployment kept changing while its log copy was reserved"))
    }

    async fn record(
        &self, deployment: &str, epoch: u64, ensemble: Vec<rtdb_directory::MemberId>,
        proof: &rtdb_logkeeper::HandoffCertificate,
    ) -> Result<()> {
        for _ in 0..8 {
            let current = self.directory.deployment(deployment).await?.ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-deployment", format!("No deployment {deployment}")))?;
            if current.epoch != epoch { return Err(Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "rtdb.multi/deposed", "The deployment moved on before its new ensemble was recorded")); }
            let ordered_ids = |members: &[rtdb_directory::MemberId]| {
                let mut ids: Vec<_> = members.iter().map(|member| member.0.clone()).collect();
                ids.sort_unstable();
                ids
            };
            let handoff = &proof.handoff;
            let checkpoint = handoff_checkpoint(&current);
            if !proof.authentic() || handoff.source.stream != current.stream("replica")
                || handoff.destination.epoch != epoch || handoff.source.journals != ordered_ids(&current.ensemble)
                || handoff.destination.journals != ordered_ids(&ensemble)
                || handoff.source.quorum as usize != crate::deployment::write_quorum(current.ensemble.len())
                || handoff.destination.quorum as usize != crate::deployment::write_quorum(ensemble.len())
                || handoff.checkpoint != checkpoint || (handoff.first > 0 && checkpoint.is_none())
            {
                return Err(Anomaly::incorrect("rtdb.multi/ensemble-handoff", "The installed predecessor and checkpoint do not attest this successor"));
            }
            let mut next = current.clone();
            next.ensemble = ensemble.clone();
            let identities = |journals: &[String]| journals.iter().cloned().map(rtdb_directory::MemberId).collect();
            next.ensemble_handoff = Some(rtdb_directory::EnsembleHandoff {
                epoch, source: identities(&handoff.source.journals), destination: identities(&handoff.destination.journals),
                first: handoff.first, through: handoff.through, checkpoint_boundary: current.checkpoint.map(|checkpoint| checkpoint.boundary),
            });
            if next.journal_placement.as_ref().is_some_and(|placement| placement.reached_by(&ensemble)) {
                next.journal_placement = None;
            }
            if self.directory.replace_deployment(next, current.version).await.into_applied()?.is_some() { return Ok(()); }
        }
        Err(Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "rtdb.multi/record-contended", "The deployment kept changing while its new ensemble was recorded"))
    }
}

fn handoff_checkpoint(record: &rtdb_directory::DeploymentRecord) -> Option<rtdb_logkeeper::HandoffCheckpoint> {
    let checkpoint = record.checkpoint?;
    Some(rtdb_logkeeper::HandoffCheckpoint {
        through: checkpoint.boundary,
        catalog: Bytes::copy_from_slice(&checkpoint.root_sha256?),
        inventory: Bytes::copy_from_slice(&record.recovery_manifest_sha256?),
    })
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
    let intake = Arc::new(IntakeLog::new(maximum_message_bytes, LogOffset(start)));
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
    direct_sql_commits: bool,
    cancellation: CancellationToken,
) -> Result<OpenedLogs> {
    let epoch = StorageEpoch(record.log_epoch);
    let opened = ordered.latest()?;
    let chosen = admission.recovery.checkpoint;
    if !crate::journal::replay_cursor::recovery_coordinates_match(
        opened.0,
        admission.recovery.through.next_lsn,
        chosen.cut.next_lsn,
        crate::journal::hosted::genesis(chosen),
        checkpoint.map(|block| block.boundary),
        checkpoint.is_some_and(|block| block.root_sha256 == Some(chosen.root)),
    ) {
        return Err(Anomaly::fault("rtdb.multi/recovery-coordinates", "The opened logs differ from the certified checkpoint or suffix"));
    }
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
    let intake = Arc::new(IntakeLog::new(maximum_message_bytes, LogOffset(next)));
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
            direct_sql_commits,
        },
        drivers: vec![trimming],
    })
}

pub(crate) fn ordered_reader_opened(
    journal: Arc<crate::journal::JournalLog>, key: &[u8], record: &rtdb_directory::DeploymentRecord,
    writer_epoch: u64, checkpoint: Option<rtdb_directory::Checkpoint>, maximum: usize,
) -> Result<OpenedLogs> {
    let epoch = StorageEpoch(record.log_epoch);
    let deployment = uuid::Uuid::parse_str(&record.id).ok();
    let index = || Box::new(OrderedReadIndex { journal: Arc::clone(&journal), deployment }) as Box<dyn ReplicaReadIndex>;
    let await_authority = if writer_epoch == 0 { AwaitAuthority::Frozen } else {
        let scope = PlacedAttemptScope::new(&record.stream("replica"), writer_epoch)?;
        AwaitAuthority::Live {
            codec: Box::new(DeploymentReceipts { scope, cipher: Cipher::new(key)? }),
            recovery: ClosedEpochRecovery::ReadIndex(index()), placed_receipt_retirement: record.placed_receipt_retirement,
            direct_sql_commits: false,
        }
    };
    let next = checkpoint.map_or(0, |checkpoint| checkpoint.source.saturating_add(1));
    let replica_read_index = Some(index());
    let replica: Arc<dyn Log> = journal;
    Ok(OpenedLogs { source: Arc::new(IntakeLog::new(maximum, LogOffset(next))), replica, source_epoch: epoch,
        replica_epoch: epoch, placed_transfer: None, replica_read_index, await_authority, drivers: Vec::new() })
}

struct OrderedReadIndex { journal: Arc<crate::journal::JournalLog>, deployment: Option<uuid::Uuid> }

#[async_trait]
impl ReplicaReadIndex for OrderedReadIndex {
    fn deployment(&self) -> Option<uuid::Uuid> { self.deployment }
    fn requires_anchored_catalog(&self) -> bool { true }
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
                        let original = self.holds.directory.deployment(&config.deployment).await?
                            .filter(|record| record.epoch == config.epoch && record.stream("replica") == stream.stream)
                            .ok_or_else(|| Anomaly::incorrect("rtdb.multi/ensemble-handoff", "The directory has no current predecessor placement"))?;
                        let checkpoint = handoff_checkpoint(&original);
                        let moved = rtdb_logkeeper::open_writer_on_checkpoint(
                            vec![rtdb_logkeeper::Loglet { servers: previous.clone(), start_lsn: 0,
                                write_quorum: crate::deployment::write_quorum(previous.len()) }],
                            stream, config.epoch, rtdb_logkeeper::CopyFrom::Retained, checkpoint, cancellation.clone(),
                        ).await?;
                        let proof = moved.activation_handoff().ok_or_else(|| {
                            Anomaly::incorrect("rtdb.multi/ensemble-handoff", "The moved writer has no certified predecessor")
                        })?;
                        self.recorder.record(&config.deployment, config.epoch, ensemble, proof).await?;
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
                placed_receipt_retirement: config.placed_receipt_retirement, direct_sql_commits: false }
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
        let intake = Arc::new(IntakeLog::new(self.maximum_message_bytes, LogOffset(next)));
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
/// A source advancement plus one per later unclassified record bounds the assigned source positions.
/// A checkpoint confirmation can name an older source position than transactions committed while its files were uploading.
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
    let mut checkpoint_floor = 0i64;
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
                    if cut.is_none() { return Ok(next.max(checkpoint_floor)); }
                    observed_next = observed_next.max(next);
                    after_newest += 1;
                }
                Scanned::Checkpoint(position) => {
                    checkpoint_floor = checkpoint_floor.max(position.checked_add(1).ok_or_else(invalid_floor)?);
                }
                Scanned::MayConsume => after_newest += 1,
                Scanned::Consumes => {}
            }
        }
        end = start;
        window = SCAN_WINDOW;
    }
    let anchor = if cut.is_some() { source_floor } else { checkpoint_floor };
    let anchored = anchor.checked_add(after_newest)
        .and_then(|next| next.checked_add(if cut.is_some() { missing } else { 0 }))
        .ok_or_else(invalid_floor)?;
    Ok(observed_next.max(anchored).max(checkpoint_floor))
}

enum Scanned { Position(i64), Checkpoint(i64), MayConsume, Consumes }

/// What one replica record says about source positions allocated up to it.
/// A claim or heartbeat (a no-op naming no source position) and a leader's own deletion notice consume none; anything else whose position it does not name may have consumed one.
fn source_position(payload: &[u8], epoch: StorageEpoch) -> Result<Scanned> {
    let position = match ReplicaPeek::read(payload).map(|frame| frame.body) {
        Some(ReplicaPeekBody::Resolved { source, .. } | ReplicaPeekBody::NoOp(source @ Some(_))) => source,
        Some(ReplicaPeekBody::NoOp(None)) => return Ok(Scanned::Consumes),
        None => {
            let Some(envelope) = ReplicaEnvelope::decode(payload)? else { return Ok(Scanned::MayConsume); };
            match &envelope.message {
                ReplicaBody::ResolvedTx(tx) => tx.src_msg_id,
                ReplicaBody::NoOp(noop) => match noop.placed_slot_reservation.as_deref() {
                    Some(wire) => Some(rtdb_types::PlacedReservation::decode(wire)?.attempt.position().0),
                    None => match noop.src_msg_id { None => return Ok(Scanned::Consumes), position => position },
                },
                ReplicaBody::BlockBoundary(boundary) => boundary.latest_processed_msg_id,
                ReplicaBody::BlockUploaded(upload) => return Ok(upload.latest_processed_msg_id.map(SourcePosition)
                    .filter(|position| position.0 >= 0 && position.epoch() == epoch)
                    .map_or(Scanned::Consumes, |position| Scanned::Checkpoint(position.offset()))),
                ReplicaBody::TriesDeleted(_) | ReplicaBody::TablesRetired(_) => return Ok(Scanned::Consumes),
                _ => None,
            }
        }
    };
    Ok(match position.filter(|position| *position >= 0).map(SourcePosition).filter(|position| position.epoch() == epoch) {
        Some(position) => Scanned::Position(position.offset()),
        None => Scanned::MayConsume,
    })
}

/// Materializes admitted transactions up to the requested time in the confirmed replica prefix.
pub(crate) async fn transactions_until(
    replica: &dyn Log, catalog: &BlockCatalog, boundary: i64, through: LogOffset, system_time: Timestamp,
    read_payload: impl AsyncFn(i64, &[u8]) -> Result<rtdb_storage::log::ReplicaPayload>,
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
            let frame = if let Some(peek) = ReplicaPeek::read(&record.payload) {
                if fence.admit(peek.term, peek.term_sequence)?.verdict != Verdict::Apply { continue; }
                let ReplicaPeekBody::Resolved { time, .. } = peek.body else { continue; };
                if time.ok_or_else(gap)? > system_time.0 { break 'capture; }
                ReplicaFrame::decode_bytes(record.payload.clone())?
            } else {
                let frame = ReplicaFrame::decode_bytes(record.payload.clone())?;
                if fence.admit(frame.term, frame.term_sequence)?.verdict != Verdict::Apply { continue; }
                frame
            };
            let envelope = frame.into_envelope().ok_or_else(gap)?;
            let envelope = if matches!(envelope.message, ReplicaBody::OversizedMessage(_)) {
                let body = read_payload(record.metadata.offset.0, &record.payload).await?;
                envelope.unwrap_payload(body.as_ref())?
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
        tx.placed_direct_admission = None;
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

    async fn admit_detached_append(&self, payload: &Bytes) -> rtdb_storage::Result<Option<(RecoveryAdmission, Bytes)>> {
        let Some(admission) = &self.recovery else { return Ok(None); };
        if !ReplicaFrame::has_external_payload(payload).map_err(|error| rtdb_storage::Error::Task(error.to_string()))? { return Ok(None); }
        admission.admit_detached_append(payload).await?;
        Ok(Some((admission.clone(), payload.clone())))
    }

    fn admit_size(&self, payload: &Bytes) -> rtdb_storage::Result<()> {
        if let Some(maximum) = self.maximum_append_bytes()
            && payload.len() > maximum
        {
            return Err(rtdb_storage::Error::MessageTooLarge { size: payload.len(), maximum });
        }
        Ok(())
    }
}

#[async_trait]
impl Log for SealedLog {
    fn requires_replica_payload_permit(&self) -> bool { self.recovery.is_some() }
    async fn publish_replica_payload(
        &self, _store: &dyn ObjectStore, key: &ObjectKey, body: Bytes,
    ) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectVersion>> {
        let admission = self.recovery.as_ref().ok_or_else(|| rtdb_storage::Error::Closed("The detached replay writer is unavailable".into()))?;
        admission.publish(key, body).await
    }
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
        self.admit_size(&payload)?;
        let staged = self.admit_detached_append(&payload).await?;
        let outcome = crate::residence_trace::AttemptOutcome::from_replica(&payload);
        let receipt = self.inner.enqueue(self.units.seal(payload).await?, timestamp).await?;
        Ok(observe_detached_commit(observe_attempt(receipt, outcome), staged))
    }
    async fn enqueue_maintenance(&self, payload: Bytes, timestamp: Timestamp) -> rtdb_storage::Result<AppendReceipt> {
        self.admit_size(&payload)?;
        let staged = self.admit_detached_append(&payload).await?;
        let receipt = self.inner.enqueue_maintenance(self.units.seal(payload).await?, timestamp).await?;
        Ok(observe_detached_commit(receipt, staged))
    }
    async fn available_append_capacity(&self) -> rtdb_storage::Result<Option<rtdb_storage::log::AppendCapacity>> {
        self.inner.available_append_capacity().await
    }
    async fn enqueue_guarded(&self, payload: Bytes, guard: GuardedAppend<'_>) -> rtdb_storage::Result<AppendReceipt> {
        let staged = self.admit_detached_append(&payload).await?;
        let outcome = crate::residence_trace::AttemptOutcome::from_replica(&payload);
        let payload = self.units.seal(payload).await?;
        if let Some((admission, cutoff)) = self.recovery.as_ref().zip(guard.journal_before()) {
            admission.check_handoff_dispatch(cutoff).await?;
        }
        let receipt = self.inner.enqueue_guarded(payload, guard).await?;
        Ok(observe_detached_commit(observe_attempt(receipt, outcome), staged))
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
        Ok(Box::new(SealedRange { inner: self.inner.range(from, to).await?, units: Arc::clone(&self.units), admission: self.recovery.clone(), protected: None }))
    }
    async fn protect_range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<rtdb_storage::log::ProtectedLogRange> {
        let absent = || rtdb_storage::Error::Closed("The replica interval has no durable custody hold".into());
        let admission = self.recovery.as_ref().ok_or_else(absent)?;
        let after = from.0.checked_sub(1).map(LogOffset).ok_or_else(absent)?;
        let through = to.0.checked_sub(1).map(LogOffset).ok_or_else(absent)?;
        let hold = Arc::new(admission.hold(after, through).await?.ok_or_else(absent)?);
        let inner = self.inner.range(from, to).await?;
        let range = Box::new(SealedRange { inner, units: Arc::clone(&self.units), admission: None, protected: Some(Arc::clone(&hold)) });
        Ok(rtdb_storage::log::ProtectedLogRange { authenticated_head: None, range, hold })
    }

    async fn read_range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Vec<LogRecord>> {
        let mut records = open_records(&self.units, self.inner.read_range(from, to).await?).await?;
        if let Some(admission) = &self.recovery { admission.retain_offloaded(&mut records).await?; }
        Ok(records)
    }
}

fn observe_detached_commit(receipt: AppendReceipt, stage: Option<(RecoveryAdmission, Bytes)>) -> AppendReceipt {
    let Some((admission, frame)) = stage else { return receipt; };
    AppendReceipt::observed(async move {
        let metadata = receipt.durable().await?;
        admission.observe_detached_commit(metadata.offset, frame);
        Ok(metadata)
    })
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

struct SealedRange {
    inner: Box<dyn LogRange>,
    units: Arc<JournalUnits>,
    admission: Option<RecoveryAdmission>,
    protected: Option<Arc<ReplayHold>>,
}

#[async_trait]
impl LogRange for SealedRange {
    fn allocation_bound(&self) -> usize { self.inner.allocation_bound().saturating_mul(2) }
    async fn next(&mut self, reservation: ReadReservation) -> rtdb_storage::Result<Option<LogRecord>> {
        if reservation.length() < self.allocation_bound() {
            return Err(rtdb_storage::Error::Task("Journal range reservation is too small".into()));
        }
        let Some(record) = self.inner.next(reservation).await? else { return Ok(None); };
        let mut record = self.units.open_record(record).await?;
        if let Some(hold) = &self.protected {
            hold.retain_record(&mut record)?;
        } else if let Some(admission) = &self.admission {
            admission.retain_offloaded(std::slice::from_mut(&mut record)).await?;
        }
        Ok(Some(record))
    }
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

        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let read = async |position, frame: &[u8]| {
            assert_eq!(position, 1);
            let reference = rtdb_storage::log::ReplicaPayloadReference::from_frame(frame, CURRENT_STORAGE_VERSION, StorageEpoch(0))?.unwrap();
            Ok::<_, Anomaly>(reference.read_at(&store, &memory).await?)
        };
        let tail = transactions_until(&journal, &catalog, -1, LogOffset(1), Timestamp(10), &read).await.unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].message, transaction.message);

        store.delete(&path).await.unwrap();
        store.put(&path, Bytes::from_static(b"replacement")).await.unwrap();
        let error = transactions_until(&journal, &catalog, -1, LogOffset(1), Timestamp(10), &read).await.unwrap_err();
        assert_eq!(error.code, "rtdb.storage/immutable-object-conflict");
    }

    #[tokio::test]
    async fn arrow_byte_path_a_time_fork_waits_for_a_confirmed_but_unpublished_record() {
        use rtdb_format::log::proto;
        use rtdb_types::Term;

        let journal = MemoryLog::new(1024);
        let catalog = BlockCatalog::from_proto(rtdb_format::block::proto::Block {
            block_index: Some(0), term_id: Some(2), boundary_term_seq: Some(0), ..Default::default()
        }).unwrap();
        let transaction = ReplicaEnvelope {
            term: Term(2), term_sequence: Some(rtdb_types::TermSequence(1)),
            message: ReplicaBody::ResolvedTx(proto::ResolvedTx {
                tx_id: Some(9), system_time_micros: Some(10), committed: Some(true), ..Default::default()
            }),
        };

        let copy = transactions_until(&journal, &catalog, -1, LogOffset(0), Timestamp(10), async |_, _| unreachable!());
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
            broker: None, custody: rtdb_directory::ReplayCustody::Local, tasks: Default::default(), stop: CancellationToken::new() });
        let factory = LogkeeperLogFactory::new(1024, Default::default(), holds);

        let refused = factory.open(&configuration, LogAccess::ReadWrite, CancellationToken::new()).await.err().unwrap();
        assert_eq!(refused.code, "rtdb.multi/key-missing");
        assert!(directory.deployment(&record.id).await.unwrap().unwrap().replica_journals.is_empty());
        assert!(keys.existing_key(&record.key_id()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn recovery_refuses_a_pending_trim_before_the_logkeeper_applies_it() {
        use rtdb_directory::{Checkpoint, DeploymentRecord, Directory, JournalAnchor, MemoryDirectory, ReplaceOutcome, ReplayCustody};
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
            broker: None,
            custody: ReplayCustody::Local, tasks: Default::default(), stop: CancellationToken::new(),
        });
        holds.keys.key(&config.key_id).await.unwrap();
        let replica = SealedLog::for_serving(provider.clone(), &config, holds.clone()).await.unwrap();
        let entry = ReplicaEnvelope { term: Term(1), term_sequence: None, message: ReplicaBody::NoOp(Default::default()) };
        for _ in 0..7 {
            replica.enqueue(Bytes::from(entry.encode()), Timestamp(0)).await.unwrap().durable().await.unwrap();
        }
        assert!(replica.recovery_tail(LogOffset(3)).await.unwrap().is_some());

        holds.tasks.close();
        holds.tasks.wait().await;
        record = directory.deployment(&record.id).await.unwrap().unwrap();
        let version = record.version;
        record.checkpoint = Some(Checkpoint { block: 1, boundary: 5, source: 1, system_time: 0, root_sha256: None });
        record.replica_anchor = Some(Box::new(JournalAnchor { root_incarnation: record.incarnation, interval: [0, 4], digest: [0; 32] }));
        let ReplaceOutcome::Applied(mut record) = directory.replace_deployment(record, version).await else { panic!("failed to authorize trim"); };
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
        let ReplaceOutcome::Applied(mut record) = directory.replace_deployment(record, version).await else { panic!("failed to change writer epoch"); };
        assert!(matches!(replica.recovery_tail(LogOffset(3)).await,
            Err(Error::UnavailableLogRange { requested: 4, first_retained: 5 })));
        assert!(replica.recovery_tail(LogOffset(5)).await.unwrap().is_some());

        holds.tasks.wait().await;
        record = directory.deployment(&record.id).await.unwrap().unwrap();
        let version = record.version;
        record.log_epoch += 1;
        assert!(matches!(directory.replace_deployment(record, version).await, ReplaceOutcome::Applied(_)));
        assert!(matches!(replica.recovery_tail(LogOffset(5)).await, Err(Error::Closed(_))));
    }

    #[tokio::test]
    async fn a_live_offload_after_the_startup_tip_pins_its_replica_position() {
        use rtdb_directory::{DeploymentRecord, Directory, MemoryDirectory, ReplayCustody};
        use tokio_util::task::TaskTracker;
        use crate::{deployment::CellStorage, keys::LocalKeyService, replay_broker::Client};

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
        let tasks = TaskTracker::new();
        let broker = Client::standalone(directory.clone(), CellStorage::Local { root: directory_files.path().join("detached") },
            &CellStorage::Local { root: directory_files.path().join("serving") },
            Arc::new(LocalKeyService::new(directory_files.path().join("detached-keys")).unwrap()), tasks.clone(), CancellationToken::new()).unwrap();
        let holds = Arc::new(RecoveryHolds {
            directory: directory.clone(), keys: Arc::new(LocalKeyService::new(directory_files.path().join("keys")).unwrap()),
            broker: Some(broker),
            custody: ReplayCustody::Local, tasks, stop: CancellationToken::new(),
        });
        holds.keys.key(&config.key_id).await.unwrap();
        let replica = SealedLog::for_serving(Arc::new(MemoryLog::new(1024)), &config, holds.clone()).await.unwrap();
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
    async fn arrow_byte_path_the_intake_starts_beyond_every_source_position_the_replica_stream_may_hold() {
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
    async fn a_deferred_upload_preserves_the_successor_intake_floor() {
        let epoch = StorageEpoch(0);
        let upload = || ReplicaBody::BlockUploaded(proto::BlockUploaded { latest_processed_msg_id: Some(41), ..Default::default() });
        let replica = MemoryLog::new(1 << 20);
        append(&replica, ReplicaBody::BlockBoundary(proto::BlockBoundary {
            latest_processed_msg_id: Some(41), deferred_checkpoint: Some(true), ..Default::default()
        })).await;
        append(&replica, ReplicaBody::ResolvedTx(proto::ResolvedTx { src_msg_id: Some(45), ..Default::default() })).await;
        append(&replica, upload()).await;
        assert_eq!(next_source_offset(&replica, epoch).await.unwrap(), 46);

        let retained = MemoryLog::starting_at(1 << 20, LogOffset(1));
        append(&retained, ReplicaBody::OversizedMessage(proto::OversizedMessage::default())).await;
        append(&retained, upload()).await;
        assert_eq!(next_source_offset(&retained, epoch).await.unwrap(), 43);
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
}
