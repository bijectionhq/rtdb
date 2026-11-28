use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{catalog::{BlockCatalog, RelationOidOrder, legacy_table_ref}, database::proto::DatabaseConfig, envelope::{ReplicaBody, ReplicaEnvelope, SourceBody, SourceMessage}, keys::TrieKey, log::proto::{self, resolved_tx::DbOp}};
use rtdb_index::{CatalogCut, CatalogTrie, PartitionState, PreparedSearchDelta, SearchDelta, ServingSearch, Snapshot, StagedWrite, TrieCatalog};
use rtdb_storage::{Error as StorageError, IoRegion, ObjectKey, ObjectStore, log::{AppendCapacity, LogOffset, LogRecord, ReplayHead}, object_store::ReadReservation};
use rtdb_tx::{ResolveContext, ResolvedTx, SourceTransaction, TransactionResult, TransactionWriter, TxResolver, WriterPublication};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, EventOp, PlacedReservation, PlacedTerminal, ReplicaPosition, Result, SourcePosition, TableRef, Term, TermSequence, Timestamp, TransactionKey, TxId};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

use crate::promotion::{initial_term_sequence, replica_read_is_empty, should_start_claim};
use crate::{BlockCycle, BlockStore, FlushTimer, PartitionLogs, PendingBlock, ProgressMonitor, PublishedCheckpoint, PublishedPrimaryReset, RecoveredPartition, ReplicaMetadata, ReplicaRecord, TermFence, TermPosition, Verdict};
use crate::admission::AdmittedReplica;
use crate::appender::{Applied, AppenderHandle, CommitLagBasis, PendingWrite, PreparedInteractivePublication, ReplicaAppender, WriteBody, complete_record, join_error, now};
use crate::blocks::{RecoveredBlockConfirmation, checkpoint_digest, matches_catalog_boundary, matches_confirmation, validate_uploaded_root};
use crate::logs::{APPEND_CAPACITY, DecodedReplica, PartitionReplay, STRICT_DISPATCH_CUTOFF, STRICT_DISPATCH_UNAVAILABLE, replica_decode_bound};
use crate::progress::{AppliedTransaction, DriverRestart, stopped, term_ended};
use crate::readback_identity::{TransactionEvent, matching_present_integer, own_readback_term, readback_matches_transaction, transaction_completion};
use crate::applied_prefix::AppliedPrefix;
use crate::recovery_promotion::{WriterActivation, publish_installed_prefix, writer_activation};
use crate::state::fail_pending;
use crate::serving::{self, NativeRequest, NativeServing, NativeServingAuthority, NativeServingHandle, ServingCheckpointSeal, SourceRequest};
use crate::checkpoint_admin::{PinControl, ReleaseControl};
use rtdb_tx::transact::{Erasure, Outcome as ServingOutcome, Receipt as ServingReceipt};
use crate::default_erasure::{self, AppliedErasure, DefaultErasure, ErasureCut};

mod default_erasure_runtime;

#[async_trait]
pub trait DatabaseEffects: Send + Sync {
    fn check_db_op(&self, operation: &DbOp) -> Result<()>;
    async fn apply_db_op(&self, operation: &DbOp) -> Result<()>;
    fn secondary_databases(&self) -> Result<BTreeMap<String, DatabaseConfig>>;
}

/// Supplies UTC timestamps in Unix microseconds for replication.
/// The driver, term appender and cloned handles can sample it concurrently.
pub type ReplicationClock = Arc<dyn Fn() -> Timestamp + Send + Sync>;

// The deployed clocks use the same UTC uncertainty budget as Multi.
const SERVING_CLOCK_UNCERTAINTY_MICROS: i64 = 250_000;

#[async_trait]
pub trait ServingSearchBuilder: Send + Sync {
    async fn prepare(&self, snapshot: Snapshot, cancellation: &CancellationToken) -> Result<Option<ServingSearch>>;

    fn journals_deltas(&self) -> bool { false }

    async fn prepare_delta(&self, _snapshot: Snapshot, _write: Arc<StagedWrite>, _cancellation: &CancellationToken) -> Result<Option<PreparedSearchDelta>> { Ok(None) }

    async fn install_delta(&self, snapshot: Snapshot, _write: &StagedWrite, _delta: Option<SearchDelta>,
        cancellation: &CancellationToken) -> Result<Option<ServingSearch>> {
        self.prepare(snapshot, cancellation).await
    }

    async fn seal(&self, value: &Arc<rtdb_index::PartitionValue>, table: &TableRef, trie: &TrieKey,
        file: &std::path::Path, size: u64, cancellation: &CancellationToken) -> Result<Vec<proto::ArtefactRecord>>;
}

#[derive(Clone, Debug)]
pub struct ReplicationConfig {
    pub enabled: bool,
    pub may_lead: bool,
    pub external_source: bool,
    pub skipped_transactions: BTreeSet<SourcePosition>,
    pub rows_per_block: u64,
    pub deferred_checkpoints: bool,
    pub hosted_deferred_checkpoints: bool,
    pub activate_classic_utf16_relation_oids: bool,
    pub assertion_interval: Duration,
    pub flush_timeout: Duration,
    pub pending_block_records: usize,
    pub pending_block_bytes: usize,
    pub election_seed: u64,
    /// Whether an idle leader appends a no-op every `assertion_interval` so that followers can tell a quiet log from a leaderless one.
    /// A placed holder needs none: only it may claim, and its liveness is its member lease.
    pub idle_heartbeat: bool,
    /// How long a node that may lead waits on an empty replica read before claiming, in place of the randomized election wait.
    pub claim_wait: Option<Duration>,
    /// Names this node's directory under `crashes/` when a leader's document write fails.
    pub node_id: String,
}
impl ReplicationConfig {
    /// The reference's `IndexerConfig` defaults, which the server configuration also falls back to.
    pub const DEFAULT_ROWS_PER_BLOCK: u64 = 102_400;
    pub const DEFAULT_FLUSH_TIMEOUT: Duration = Duration::from_secs(15 * 60);

    /// The reference's node identity default without `XTDB_NODE_ID`, which the server configuration reads: the first group of a random UUID.
    pub fn default_node_id() -> String {
        uuid::Uuid::new_v4().to_string().split('-').next().unwrap_or_default().to_owned()
    }
}
impl Default for ReplicationConfig {
    fn default() -> Self {
        Self { enabled: true, may_lead: true, external_source: false, skipped_transactions: BTreeSet::new(), rows_per_block: Self::DEFAULT_ROWS_PER_BLOCK, deferred_checkpoints: false, hosted_deferred_checkpoints: false, activate_classic_utf16_relation_oids: false, assertion_interval: Duration::from_millis(100), flush_timeout: Self::DEFAULT_FLUSH_TIMEOUT,
            pending_block_records: 1024, pending_block_bytes: 64 * 1024 * 1024, election_seed: uuid::Uuid::new_v4().as_u128() as u64,
            idle_heartbeat: true, claim_wait: None, node_id: Self::default_node_id() }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role { Following, Leading(Term), Stopped }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstalledPrefix {
    pub replica: ReplicaPosition,
    pub source: SourcePosition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommittedErasure {
    pub transaction: TransactionKey,
    pub position: ReplicaPosition,
}

fn first_write_erasure(position: ReplicaPosition, write: &StagedWrite) -> Result<Option<CommittedErasure>> {
    for table in write.tables().values() {
        let Some(batch) = table.batch() else { continue; };
        for row in 0..batch.len() {
            if batch.header(row)?.op == EventOp::Erase {
                return Ok(Some(CommittedErasure { transaction: write.tx_key(), position }));
            }
        }
    }
    Ok(None)
}

#[derive(Clone, Debug)]
pub struct DriverStatus {
    pub role: Role,
    pub highest_term: Term,
    /// Latest position consumed from the transport. Catalog effects at that position may be pending.
    pub replica_position: ReplicaPosition,
    /// Last position through which every recognized effect was installed before this status was published.
    pub installed: Option<InstalledPrefix>,
    pub open_block: BlockIndex,
    pub adopted_block: Option<Arc<BlockCatalog>>,
    pub first_erasure: Option<CommittedErasure>,
    pub promotion_failure: Option<Anomaly>,
    pub last_block_upload_epoch_seconds: i64,
    pub memory_block_cuts: u64,
    pub interactive_ready: bool,
}

#[derive(Clone)]
pub struct ReplicationHandle {
    logs: Arc<PartitionLogs>, progress: Arc<ProgressMonitor>, status: watch::Receiver<DriverStatus>,
    commands: mpsc::Sender<Command>, cancellation: CancellationToken, clock: ReplicationClock,
    partition: Arc<PartitionState>,
}
impl ReplicationHandle {
    pub(crate) fn partition_logs(&self) -> &PartitionLogs { &self.logs }
    pub(crate) fn serving_clock(&self) -> Timestamp { (self.clock)() }
    pub fn progress(&self) -> &Arc<ProgressMonitor> { &self.progress }
    pub fn status(&self) -> DriverStatus { self.status.borrow().clone() }
    pub fn watch_status(&self) -> watch::Receiver<DriverStatus> { self.status.clone() }
    pub fn term(&self) -> Option<TermHandle> {
        match self.status.borrow().role { Role::Leading(term) => Some(TermHandle { handle: self.clone(), term }), _ => None }
    }
    pub async fn submit_source(&self, message: &SourceMessage, timestamp: Timestamp) -> Result<SourcePosition> {
        if self.cancellation.is_cancelled() { return Err(stopped()); }
        self.logs.require_data_intake()?;
        self.logs.append_source(message, timestamp).await
    }
    /// The payload includes its format header; its `Bytes` owner retains the producer's reservation through durability.
    pub async fn submit_source_encoded(&self, payload: Bytes, timestamp: Timestamp) -> Result<SourcePosition> {
        if self.cancellation.is_cancelled() { return Err(stopped()); }
        self.logs.require_data_intake()?;
        self.logs.append_source_encoded(payload, timestamp).await
    }
    pub async fn submit_source_interactive(&self, request: SourceTransaction, timestamp: Timestamp) -> Result<SourcePosition> {
        if self.cancellation.is_cancelled() { return Err(stopped()); }
        if !self.status.borrow().interactive_ready {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb/interactive-receipt", "The writer is preparing its interactive receipt clock"));
        }
        self.logs.require_data_intake()?;
        self.logs.append_source_interactive(request, timestamp).await
    }
    pub async fn await_tx(&self, id: TxId, cancellation: &CancellationToken) -> Result<Option<Arc<TransactionResult>>> { self.progress.await_tx(id, cancellation).await }
    pub async fn await_source(&self, position: SourcePosition, cancellation: &CancellationToken) -> Result<()> { self.progress.await_source(position, cancellation).await }
    pub(crate) fn serving_partition(&self) -> Option<Snapshot> {
        self.partition.current_published_snapshot()
    }
    pub(crate) async fn serving_barrier(&self, cancellation: &CancellationToken) -> Result<()> {
        let required = self.logs.latest_replica_position()?;
        let mut status = self.watch_status();
        let reached = cancellation.run_until_cancelled(status.wait_for(|status| {
            status.role == Role::Stopped || status.installed.is_some_and(|prefix| prefix.replica >= required)
        })).await.ok_or_else(serving::interrupted)?.map_err(|_| serving::unavailable())?;
        if reached.role == Role::Stopped { return Err(serving::unavailable()); }
        Ok(())
    }
    pub(crate) async fn submit_serving_source(&self, message: &SourceMessage, cancellation: &CancellationToken) -> Result<SourcePosition> {
        cancellation.run_until_cancelled(self.submit_source(message, (self.clock)())).await
            .ok_or_else(serving::interrupted)?
    }
    pub(crate) async fn serving_validate(&self, request: NativeRequest, cancellation: &CancellationToken) -> Result<ServingReceipt> {
        let term = self.term().ok_or_else(serving::unavailable)?.term();
        let (completion, receipt) = oneshot::channel();
        cancellation.run_until_cancelled(self.commands.send(Command::ServingValidate { term, request, completion })).await
            .ok_or_else(serving::interrupted)?.map_err(|_| serving::unavailable())?;
        cancellation.run_until_cancelled(receipt).await.ok_or_else(serving::interrupted)?.map_err(|_| serving::interrupted())?
    }
    pub(crate) async fn serving_status(&self, key: Vec<u8>, lookup: serving::ReceiptLookup, cancellation: &CancellationToken) -> Result<ServingReceipt> {
        let (completion, receipt) = oneshot::channel();
        cancellation.run_until_cancelled(self.commands.send(Command::ServingStatus { key, lookup, completion })).await
            .ok_or_else(serving::interrupted)?.map_err(|_| serving::unavailable())?;
        cancellation.run_until_cancelled(receipt).await.ok_or_else(serving::interrupted)?.map_err(|_| serving::unavailable())?
    }
    pub async fn flush(&self) -> Result<SourcePosition> {
        let expected = self.partition.value().table_catalog().block_index().map_or(-1, |block| block.0);
        self.submit_source(&SourceMessage::Protobuf(SourceBody::FlushBlock(proto::FlushBlock { expected_block_idx: Some(expected) })), (self.clock)()).await
    }
    /// Cancels the owned driver; the node must join `ReplicationDriver::run` before releasing its dependencies.
    pub fn shutdown(&self) { self.cancellation.cancel(); }
    pub async fn publish_compaction(&self, message: proto::TriesAdded, cancellation: &CancellationToken) -> Result<SourcePosition> {
        let expected = crate::fact_history::needs_census(&message).then(|| additions(&message.tries)).transpose()?;
        let position = self.submit_source(&SourceMessage::Protobuf(SourceBody::TriesAdded(message)), (self.clock)()).await?;
        self.await_source(position, cancellation).await?;
        if let Some(expected) = expected {
            let installed = self.partition.value();
            for (table, tries) in expected.into_iter().filter(|(table, _)| TableRef::owned_schema(&table.schema)) {
                if tries.iter().any(|trie| installed.trie_catalog().trie(&table, &trie.key)
                    .is_none_or(|installed| installed.data_file_size != trie.data_file_size || installed.published_files != trie.published_files))
                {
                    return Err(crate::fact_history::uncovered("The receiver retained the preceding FactStore history"));
                }
            }
        }
        Ok(position)
    }
    /// Returns once this node has applied the message; an entry the leader did not accept is recorded on no trie.
    pub async fn publish_artefacts(&self, message: proto::ArtefactsAdded, cancellation: &CancellationToken) -> Result<SourcePosition> {
        let position = self.submit_source(&SourceMessage::Protobuf(SourceBody::ArtefactsAdded(message)), (self.clock)()).await?;
        self.await_source(position, cancellation).await?;
        Ok(position)
    }
    /// Returns once this node has applied the removal.
    pub async fn remove_artefacts(&self, message: proto::ArtefactsRemoved, cancellation: &CancellationToken) -> Result<SourcePosition> {
        self.remove_artefacts_retaining(message, cancellation, None).await
    }
    /// Retains the supplied recovery gate through source append.
    pub async fn remove_artefacts_retaining(&self, message: proto::ArtefactsRemoved, cancellation: &CancellationToken,
        recovery_gate: Option<Arc<dyn Send + Sync>>) -> Result<SourcePosition> {
        let source = SourceMessage::Protobuf(SourceBody::ArtefactsRemoved(message));
        let append = self.submit_source(&source, (self.clock)());
        // Block adoption can share the recovery gate before this source position can apply.
        let position = match recovery_gate {
            Some(gate) => IoRegion::retaining(gate, append).await?,
            None => append.await?,
        };
        self.await_source(position, cancellation).await?;
        Ok(position)
    }
}
/// The applied resume token that this term can recover from its synced local history.
pub struct RecoverableExternalToken(Arc<[u8]>);

impl RecoverableExternalToken {
    pub fn as_bytes(&self) -> &[u8] { &self.0 }
}

#[derive(Clone)]
pub struct TermHandle { handle: ReplicationHandle, term: Term }
impl TermHandle {
    pub(crate) async fn pin_checkpoint(&self, pin: PinControl, cancellation: &CancellationToken) -> Result<()> {
        let (completion, receipt) = oneshot::channel();
        cancellation.run_until_cancelled(self.handle.commands.send(Command::PinCheckpoint { term: self.term, pin, completion }))
            .await.ok_or_else(serving::interrupted)?.map_err(|_| term_ended())?;
        match cancellation.run_until_cancelled(receipt).await.ok_or_else(serving::interrupted)?.map_err(|_| term_ended())?? {
            Applied::Control => Ok(()),
            _ => Err(invalid("A checkpoint pin received a different replica result")),
        }
    }

    pub(crate) async fn release_checkpoint(&self, release: ReleaseControl, cancellation: &CancellationToken) -> Result<()> {
        let (completion, receipt) = oneshot::channel();
        cancellation.run_until_cancelled(self.handle.commands.send(Command::ReleaseCheckpoint { term: self.term, release, completion }))
            .await.ok_or_else(serving::interrupted)?.map_err(|_| term_ended())?;
        match cancellation.run_until_cancelled(receipt).await.ok_or_else(serving::interrupted)?.map_err(|_| term_ended())?? {
            Applied::Control => Ok(()),
            _ => Err(invalid("A checkpoint release received a different replica result")),
        }
    }

    pub fn term(&self) -> Term { self.term }
    /// The applied resume token covered by the store and the retained durable journal.
    pub fn recoverable_external_token(&self) -> Option<RecoverableExternalToken> {
        if self.handle.logs.retains_recovery_history() && self.handle.status.borrow().role == Role::Leading(self.term) {
            self.handle.progress.current().external_token.map(RecoverableExternalToken)
        } else {
            None
        }
    }
    /// Obtains the adopted source token only once the hosted recovery owner has installed its root.
    /// A journal that cannot say whether it confirms the root raises its error rather than reporting no token.
    pub async fn recoverable_external_checkpoint(&self) -> Result<Option<RecoverableExternalToken>> {
        let catalog = {
            let status = self.handle.status.borrow();
            if status.role != Role::Leading(self.term) { return Ok(None); }
            let Some(catalog) = status.adopted_block.clone() else { return Ok(None); };
            catalog
        };
        let Some(bytes) = catalog.external_source_token() else { return Ok(None); };
        if !self.handle.logs.confirms_recovery_checkpoint(&catalog).await?
            || self.handle.status.borrow().role != Role::Leading(self.term)
        {
            return Ok(None);
        }
        Ok(Some(RecoverableExternalToken(Arc::from(bytes))))
    }

    /// Replaces a quiescent catalog and returns after its new block has been adopted in this term.
    /// The caller must exclusively operate the database with compaction and GC disabled.
    /// A failed receipt never permits deletion of the old catalog's files.
    pub async fn reset_compactor(&self, primary: PublishedPrimaryReset, reservations: Arc<[Arc<MemoryReservation>]>,
    ) -> Result<BlockIndex> {
        if !Arc::ptr_eq(&self.handle.logs, &primary.source) {
            return Err(invalid("The reset primary belongs to another journal"));
        }
        let (completion, receiver) = oneshot::channel();
        self.handle.commands.send(Command::ResetCompactor { term: self.term, primary, reservations, completion })
            .await.map_err(|_| term_ended())?;
        receiver.await.map_err(|_| term_ended())?
    }
    pub async fn publish_deletions(&self, table: TableRef, keys: Vec<TrieKey>) -> Result<ReplicaPosition> {
        let (sender, receiver) = oneshot::channel();
        self.handle.commands.send(Command::Delete { term: self.term, table, keys, completion: sender }).await.map_err(|_| term_ended())?;
        match receiver.await.map_err(|_| term_ended())?? {
            Applied::TrieRemoval(position) => Ok(position),
            Applied::Transaction(_) | Applied::Control => Err(invalid("Deletion received a different result")),
        }
    }
    /// Returns once this term's installed prefix applies the retirement (trie-cat.allium: RetireTable).
    pub async fn publish_retirements(&self, tables: Vec<TableRef>, as_of: Timestamp) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.handle.commands.send(Command::Retire { term: self.term, tables, as_of, completion: sender }).await.map_err(|_| term_ended())?;
        match receiver.await.map_err(|_| term_ended())?? {
            Applied::Control => Ok(()),
            Applied::Transaction(_) | Applied::TrieRemoval(_) => Err(invalid("Table retirement received a different result")),
        }
    }
    pub async fn submit_external(&self, writer: Box<dyn TransactionWriter>, system_time: Option<Timestamp>, token: Option<Vec<u8>>) -> Result<ExternalReceipt> {
        let (sender, receiver) = oneshot::channel();
        self.handle.commands.send(Command::External { term: self.term, writer, system_time, token, completion: sender }).await.map_err(|_| term_ended())?;
        Ok(ExternalReceipt { receiver })
    }
    pub async fn resolve_external(&self, writer: Box<dyn TransactionWriter>, system_time: Option<Timestamp>, token: Option<Vec<u8>>) -> Result<Arc<TransactionResult>> {
        self.submit_external(writer, system_time, token).await?.applied().await
    }
}

pub struct ExternalReceipt { receiver: oneshot::Receiver<Result<Applied>> }
impl ExternalReceipt {
    pub async fn applied(self) -> Result<Arc<TransactionResult>> {
        match self.receiver.await.map_err(|_| term_ended())?? {
            Applied::Transaction(result) => Ok(result),
            Applied::Control | Applied::TrieRemoval(_) => Err(invalid("External transaction received a control result")),
        }
    }

}

enum Command {
    PinCheckpoint { term: Term, pin: PinControl, completion: oneshot::Sender<Result<Applied>> },
    ReleaseCheckpoint { term: Term, release: ReleaseControl, completion: oneshot::Sender<Result<Applied>> },
    ServingValidate { term: Term, request: NativeRequest, completion: serving::Completion },
    ServingStatus { key: Vec<u8>, lookup: serving::ReceiptLookup, completion: serving::Completion },
    ResetCompactor { term: Term, primary: PublishedPrimaryReset, reservations: Arc<[Arc<MemoryReservation>]>, completion: oneshot::Sender<Result<BlockIndex>> },
    Delete { term: Term, table: TableRef, keys: Vec<TrieKey>, completion: oneshot::Sender<Result<Applied>> },
    Retire { term: Term, tables: Vec<TableRef>, as_of: Timestamp, completion: oneshot::Sender<Result<Applied>> },
    External { term: Term, writer: Box<dyn TransactionWriter>, system_time: Option<Timestamp>, token: Option<Vec<u8>>, completion: oneshot::Sender<Result<Applied>> },
}
impl Command {
    fn term(&self) -> Term { match self { Self::Delete { term, .. } | Self::Retire { term, .. } | Self::External { term, .. } | Self::ResetCompactor { term, .. } | Self::ServingValidate { term, .. } | Self::PinCheckpoint { term, .. } | Self::ReleaseCheckpoint { term, .. } => *term, Self::ServingStatus { .. } => Term(0) } }
    fn fail(self, error: Anomaly) {
        match self {
            Self::Delete { completion, .. } | Self::Retire { completion, .. } | Self::External { completion, .. } | Self::PinCheckpoint { completion, .. } | Self::ReleaseCheckpoint { completion, .. } => { let _ = completion.send(Err(error)); }
            Self::ResetCompactor { completion, .. } => { let _ = completion.send(Err(error)); }
            Self::ServingValidate { completion, .. } => { let _ = completion.send(Err(serving::interrupted())); }
            Self::ServingStatus { completion, .. } => { let _ = completion.send(Err(error)); }
        }
    }
}

struct CompactorReset {
    block: BlockIndex,
    completion: Option<oneshot::Sender<Result<BlockIndex>>>,
    _reservations: Arc<[Arc<rtdb_columnar::MemoryReservation>]>,
}
impl CompactorReset {
    fn complete(mut self) { if let Some(completion) = self.completion.take() { let _ = completion.send(Ok(self.block)); } }
}
impl Drop for CompactorReset { fn drop(&mut self) { if let Some(completion) = self.completion.take() { let _ = completion.send(Err(term_ended())); } } }

enum TailPoll { Legacy(Vec<LogRecord>), Prepared(Option<ReplayHead>) }
type Poll = BoxFuture<'static, (PartitionReplay, Result<TailPoll>)>;
struct SelectedRecord { raw: LogRecord, decode_reservation: Arc<MemoryReservation> }
struct ArchivedPayload { bytes: Vec<u8>, _reservation: Arc<rtdb_columnar::MemoryReservation> }
impl AsRef<[u8]> for ArchivedPayload { fn as_ref(&self) -> &[u8] { &self.bytes } }
fn start_poll(mut tail: PartitionReplay, timeout: Duration) -> Poll {
    Box::pin(async move {
        loop {
            let result = match &mut tail {
                PartitionReplay::Prepared(cursor) => cursor.poll_head(timeout).await.map(TailPoll::Prepared),
                PartitionReplay::Legacy(tail) => match tail.replay() {
                    Some(cursor) => cursor.poll_head(timeout).await.map(TailPoll::Prepared),
                    None => tail.poll(timeout).await.map(TailPoll::Legacy),
                },
            };
            if !matches!(result, Err(StorageError::LogFetchPending { .. })) {
                return (tail, result.map_err(Into::into));
            }
            // The observed prefix still has a record at this cursor. Keep reading
            // it before reporting an empty read to the election or source driver.
            tokio::task::yield_now().await;
        }
    })
}
async fn pending_poll(poll: &mut Option<Poll>) -> (PartitionReplay, Result<TailPoll>) {
    match poll { Some(poll) => poll.await, None => futures::future::pending().await }
}
async fn read_replay_head(tail: &mut PartitionReplay, head: ReplayHead, memory: &Arc<MemoryBudget>, decode_bound: fn(usize) -> Result<usize>, cancellation: &CancellationToken) -> Result<Option<SelectedRecord>> {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Ok(None),
        result = async {
            let replay = tail.replay().ok_or_else(|| invalid("Log tail lost its replay capability"))?;
            let raw_reservation = memory.reserve(head.allocation_bound)?;
            let decode_reservation = memory.reserve(decode_bound(head.payload_len)?)?;
            let raw = replay.read_head(head, ReadReservation::new(head.allocation_bound, raw_reservation)).await?;
            if raw.metadata != head.metadata || raw.payload.len() != head.payload_len {
                return Err(invalid("Log tail changed its prepared replay record"));
            }
            Ok(SelectedRecord { raw, decode_reservation })
        } => result.map(Some),
    }
}
fn source_decode_bound(payload_len: usize) -> Result<usize> {
    payload_len.checked_mul(4).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| invalid("Source decode allocation size overflow"))
}
async fn pending_job(job: &mut Option<JoinHandle<Result<()>>>) -> Result<()> {
    match job { Some(job) => job.await.map_err(join_error)?, None => futures::future::pending().await }
}
struct Completion(Option<oneshot::Sender<Result<Applied>>>);
impl Drop for Completion { fn drop(&mut self) { if let Some(sender) = self.0.take() { let _ = sender.send(Err(term_ended())); } } }
struct Claim {
    term: Term,
    future: BoxFuture<'static, Result<crate::ReplicaMetadata>>,
    observed: Option<ReplicaPosition>,
}
async fn pending_claim(claim: &mut Option<Claim>) -> Result<crate::ReplicaMetadata> {
    match claim { Some(claim) => claim.future.as_mut().await, None => futures::future::pending().await }
}
struct Resolution {
    future: BoxFuture<'static, (TxResolver, Result<Arc<ResolvedTx>>)>,
    completion: Completion,
    direct_admission: Option<PlacedReservation>,
    interactive: Option<Arc<OnceLock<PreparedInteractivePublication>>>,
}
enum Production {
    Writing(std::time::Instant, BoxFuture<'static, Result<CheckpointOutput>>),
    Appending(std::time::Instant, BoxFuture<'static, Result<ReplicaMetadata>>, Option<ServingCheckpointSeal>),
    AwaitingReadback(ReplicaMetadata, ServingCheckpointSeal),
}
enum ProductionResult {
    Written(std::time::Instant, Result<CheckpointOutput>),
    Appended(std::time::Instant, Result<ReplicaMetadata>, Option<ServingCheckpointSeal>),
}
struct CheckpointOutput {
    block: crate::ProducedBlock,
    loaded: Option<crate::state::LoadedBlock>,
    seal: Option<ServingCheckpointSeal>,
}
async fn pending_resolution(resolution: &mut Option<Resolution>) -> (TxResolver, Result<Arc<ResolvedTx>>) {
    match resolution { Some(resolution) => resolution.future.as_mut().await, None => futures::future::pending().await }
}
async fn pending_production(production: &mut Option<Production>) -> ProductionResult {
    match production {
        Some(Production::Writing(started, future)) => ProductionResult::Written(*started, future.as_mut().await),
        Some(Production::Appending(started, future, seal)) => ProductionResult::Appended(*started, future.as_mut().await, seal.take()),
        Some(Production::AwaitingReadback(..)) | None => futures::future::pending().await,
    }
}

struct LeaderState {
    term: Term, resolver: Option<TxResolver>, resolution: Option<Resolution>, production: Option<Production>, confirmed: bool,
    activation: WriterActivation,
    recovery_write: Option<Arc<PendingWrite>>,
    held: VecDeque<AdmittedReplica>, held_bytes: usize, cycle: BlockCycle, pending: VecDeque<Arc<PendingWrite>>,
    source_tail: Option<PartitionReplay>, source_poll: Option<Poll>, source_batch: VecDeque<LogRecord>, source_head: Option<ReplayHead>,
    waiting_strict_source: Option<StrictSource>,
    awaiting_placed: Option<SourceInput>,
    settling_placed: bool,
    appender: AppenderHandle, job: Option<JoinHandle<Result<()>>>, cancellation: CancellationToken,
    deferred_commands: VecDeque<Command>,
    compactor_reset: Option<CompactorReset>,
    trie_removals: Vec<AppliedTrieRemoval>,
    /// The last source record of the batch being resolved: the source tip when its first record began resolving.
    /// As upstream's `SourceLogProcessor` resolves a poll batch in one go, the batch's records resolve back to back,
    /// and the records read back meanwhile wait until it has been resolved.
    source_batch_end: Option<SourcePosition>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SourcePlacement {
    Reserved(PlacedReservation),
    Direct(PlacedReservation),
}

impl SourcePlacement {
    fn attempt(&self) -> rtdb_types::PlacedAttempt {
        match *self {
            Self::Reserved(admission) | Self::Direct(admission) => admission.attempt,
        }
    }
}

struct SourceInput {
    position: SourcePosition,
    log_time: Timestamp,
    work: SourceWork,
    placement: Option<SourcePlacement>,
    _decode_charge: Option<Arc<MemoryReservation>>,
}

enum StrictSource {
    Transaction(SourceInput),
    Interactive(SourceInput),
    Serving { position: SourcePosition, request: SourceRequest },
    Physical { record: LogRecord, decode: Arc<MemoryReservation> },
    DeferredFlush(SourcePosition),
    Compaction {
        position: SourcePosition,
        log_time: Timestamp,
        message: proto::TriesAdded,
        _decode_charge: Arc<MemoryReservation>,
    },
}

enum SourceWork {
    /// The decoded transaction, and its operations stream as it arrived, which a crash log copies.
    Resolve(SourceTransaction, Option<Bytes>),
    Skip(SourceMessage),
    Fence,
}

struct AppliedTrieRemoval {
    sequence: TermSequence,
    position: ReplicaPosition,
    deletions: BTreeMap<TableRef, BTreeSet<TrieKey>>,
    _record: AdmittedReplica,
}
impl LeaderState {
    fn resolver(&self) -> Result<&TxResolver> { self.resolver.as_ref().ok_or_else(|| invalid("Resolver is owned by an in-flight transaction")) }
    fn resolver_mut(&mut self) -> Result<&mut TxResolver> { self.resolver.as_mut().ok_or_else(|| invalid("Resolver is owned by an in-flight transaction")) }
    fn can_poll_source(&self) -> bool {
        self.activation.is_active() && self.recovery_write.is_none() && self.cycle.accepting() && !self.pending_strict_write()
            && self.awaiting_placed.is_none() && !self.settling_placed
    }
    fn pending_strict_write(&self) -> bool {
        self.pending.front().is_some_and(|pending| pending.interactive.is_some() || match &pending.body {
            WriteBody::Resolved(tx, _) => tx.staging_clock_upper().is_some(),
            WriteBody::Message(ReplicaBody::NoOp(message)) => serving::is_funded_original(message),
            _ => false,
        })
    }
    fn accepting(&self) -> bool { self.can_poll_source() && self.resolution.is_none() }
    fn drained_for_strict_source(&self) -> bool { self.pending.is_empty() && self.production.is_none() }
    fn ready_source(&self) -> bool {
        self.accepting() && if self.waiting_strict_source.is_some() {
            self.drained_for_strict_source()
        } else {
            self.source_head.is_some() || !self.source_batch.is_empty()
        }
    }
    fn accepting_commands(&self) -> bool { self.accepting() && self.waiting_strict_source.is_none() }
    fn pending_serving_at(&self, sequence: Option<TermSequence>) -> Option<&proto::NoOp> {
        let sequence = sequence?;
        self.pending.iter().find_map(|write| {
            let WriteBody::Message(ReplicaBody::NoOp(message)) = &write.body else { return None; };
            (message.serving_record.is_some() && write.sequence() == Some(sequence)).then_some(message)
        })
    }
    fn pending_tx_at(&self, sequence: Option<TermSequence>) -> Option<(&Arc<PendingWrite>, &Arc<ResolvedTx>)> {
        self.pending.iter().find_map(|pending| {
            let WriteBody::Resolved(tx, _) = &pending.body else { return None; };
            let expected = pending.sequence()?;
            matching_present_integer(sequence.map(|sequence| sequence.0), expected.0).then_some((pending, tx))
        })
    }
    fn enqueue(&mut self, body: WriteBody, block: BlockIndex, completion: Option<oneshot::Sender<Result<Applied>>>, memory: &Arc<MemoryBudget>) -> Result<()> {
        let write = PendingWrite::new(body, block, completion, memory)?;
        self.appender.enqueue(Arc::clone(&write))?;
        self.pending.push_back(write);
        Ok(())
    }
}

pub struct ReplicationDriver {
    config: ReplicationConfig, logs: Arc<PartitionLogs>, blocks: Arc<BlockStore>, partition: Arc<PartitionState>, progress: Arc<ProgressMonitor>,
    restart: Option<Arc<DriverRestart>>,
    resolve: Arc<ResolveContext>, effects: Arc<dyn DatabaseEffects>, cancellation: CancellationToken, clock: ReplicationClock,
    serving_search: Option<Arc<dyn ServingSearchBuilder>>,
    serving: Option<NativeServing>,
    collect_serving_checkpoints: bool,
    default_erasure: Option<DefaultErasure>,
    default_erasure_read_fence: Option<Arc<dyn crate::DefaultErasureReadFence>>,
    local_erasure_read_fence: Option<Arc<crate::LocalDefaultErasureReadFence>>,
    default_erasure_grant_micros: Option<u64>,
    recovered_confirmation: Option<RecoveredBlockConfirmation>,
    compaction_replay_cutoff: Option<ReplicaPosition>,
    track_catalog_cut: bool,
    recognized_prefix: bool,
    applied_prefix: AppliedPrefix,
    installed: Option<InstalledPrefix>,
    first_erasure: Option<CommittedErasure>,
    historical_confirmations: VecDeque<RecoveredBlockConfirmation>,
    fence: TermFence, read_position: ReplicaPosition, pending_block: Option<PendingBlock>, claim: Option<ReplicaPosition>, claiming: Option<Claim>,
    replica_tail: Option<PartitionReplay>, replica_poll: Option<Poll>, replica_batch: VecDeque<LogRecord>, replica_head: Option<ReplayHead>,
    leader: Option<LeaderState>, commands: mpsc::Receiver<Command>, status: watch::Sender<DriverStatus>, flush_timer: FlushTimer,
    required_append_bytes: Option<u64>,
    deferred_cut: bool,
    placed_slot: Option<OrderedPlaced>,
}

#[derive(Clone, Copy)]
struct OrderedPlaced {
    reservation: PlacedReservation,
    term: Term,
    reserved_at: ReplicaPosition,
}
impl ReplicationDriver {
    pub fn new(config: ReplicationConfig, logs: Arc<PartitionLogs>, blocks: Arc<BlockStore>, recovered: RecoveredPartition, progress: Arc<ProgressMonitor>, resolve: Arc<ResolveContext>, effects: Arc<dyn DatabaseEffects>, cancellation: CancellationToken) -> Result<(ReplicationHandle, Self)> {
        Self::new_with_clock(config, logs, blocks, recovered, progress, resolve, effects, cancellation, Arc::new(now))
    }

    /// Uses the supplied clock for replica writes, default external transaction time candidates, maintenance writes and flush deadlines.
    /// The clock is sampled during construction and when recording block upload time.
    /// Source submissions accept caller timestamps, and log providers determine the timestamps in records.
    /// Returns a handle and an idle driver after checking configuration, recovered progress, log boundaries and memory admission.
    pub fn new_with_clock(
        config: ReplicationConfig, logs: Arc<PartitionLogs>, blocks: Arc<BlockStore>, mut recovered: RecoveredPartition,
        progress: Arc<ProgressMonitor>, resolve: Arc<ResolveContext>, effects: Arc<dyn DatabaseEffects>,
        cancellation: CancellationToken, clock: ReplicationClock,
    ) -> Result<(ReplicationHandle, Self)> {
        if config.assertion_interval.is_zero() || config.pending_block_records == 0 || config.pending_block_bytes == 0 { return Err(invalid("Replication intervals and pending-block limits must be positive")); }
        let applied = progress.current();
        if applied.tx_key != recovered.progress.tx_key || applied.source_position != recovered.progress.source_position || applied.external_token != recovered.progress.external_token {
            return Err(invalid("Progress monitor does not describe the recovered partition"));
        }
        if let Some(boundary) = recovered.block.as_ref().and_then(BlockCatalog::latest_source_position) {
            logs.validate_source_boundary(&resolve.database, boundary)?;
        }
        let read_position = recovered.replica_start(&logs)?;
        let track_catalog_cut = config.enabled && recovered.allows_catalog_cut_tracking();
        let initial = DriverStatus { role: Role::Following, highest_term: recovered.fence.highest_term(), replica_position: read_position,
            installed: None, first_erasure: None,
            open_block: recovered.partition.value().live().block_index(), adopted_block: recovered.block.clone().map(Arc::new), promotion_failure: None, last_block_upload_epoch_seconds: 0, memory_block_cuts: 0, interactive_ready: false };
        let (status, receiver) = watch::channel(initial);
        let (sender, commands) = mpsc::channel(128);
        let handle = ReplicationHandle { logs: Arc::clone(&logs), progress: Arc::clone(&progress), status: receiver, commands: sender, cancellation: cancellation.clone(), clock: Arc::clone(&clock), partition: Arc::clone(&recovered.partition) };
        let persisted_tx = recovered.block.as_ref().and_then(BlockCatalog::latest_completed_tx).map(|key| key.tx_id);
        let flush_timer = FlushTimer::new(now(), persisted_tx, config.flush_timeout)?;
        let recovered_confirmation = if config.enabled && config.may_lead {
            recovered.confirmation(logs.replica_epoch, logs.storage_version, logs.storage_epoch, logs.memory())?
        } else { None };
        let applied_prefix = AppliedPrefix::at(read_position.0);
        let replica_tail = recovered.take_replay_tail(&logs, read_position)?;
        let restart = if config.enabled { progress.prepare_driver_restart() } else { None };
        Ok((handle, Self { config, logs, blocks, partition: recovered.partition, progress, restart, resolve, effects, cancellation, clock, recovered_confirmation, compaction_replay_cutoff: recovered.compaction_replay_cutoff, track_catalog_cut, recognized_prefix: true, applied_prefix, installed: None, first_erasure: None, historical_confirmations: VecDeque::new(), fence: recovered.fence,
            read_position, pending_block: None, claim: None, claiming: None, replica_tail, replica_poll: None, replica_batch: VecDeque::new(), replica_head: None, leader: None, commands, status, flush_timer, serving_search: None, serving: None, collect_serving_checkpoints: false, default_erasure: None, default_erasure_read_fence: None, local_erasure_read_fence: None, default_erasure_grant_micros: None, required_append_bytes: None, deferred_cut: false, placed_slot: None }))
    }

    pub fn with_serving_search(mut self, builder: Arc<dyn ServingSearchBuilder>) -> Self {
        self.serving_search = Some(builder);
        self
    }

    pub async fn attach_native_serving(&mut self, replication: ReplicationHandle, authority: NativeServingAuthority, collect_checkpoints: bool) -> Result<NativeServingHandle> {
        if self.logs.default_erasure_kafka_topics().is_some() {
            if self.local_erasure_read_fence.is_none() || !self.config.enabled || !self.config.may_lead
                || self.config.external_source || collect_checkpoints
            {
                return Err(default_erasure::unsupported());
            }
            self.logs.source_replay(self.progress.source_position()).await?;
        }
        let block = self.status.borrow().adopted_block.clone();
        let checkpoint = match &block {
            Some(block) => authority.read_checkpoint(self.blocks.object_store(), block.as_proto().serving_checkpoint.as_deref(),
                block.as_proto().serving_checkpoint_object.as_deref()).await?,
            None => None,
        };
        if let Some(keys) = &authority.checkpoints
            && checkpoint
                .as_ref()
                .and_then(serving::NativeCheckpointImage::key_generation)
                .is_none()
        {
            let database = uuid::Uuid::from_bytes(authority.incarnation);
            keys.kms
                .create(rtdb_crypt::Generation { database, number: keys.generation })
                .await?;
        }
        let store = Arc::clone(self.resolve.storage.object_store());
        let (mut serving, view) = NativeServing::open(authority.clone(), store, checkpoint.as_ref())?;
        serving.physical_erasure = self.local_erasure_read_fence.is_some();
        if let Some(block) = &block { serving.verify_checkpoint_prefix(block)?; }
        serving.recover_classic(self.partition.snapshot(), block.as_ref().and_then(|block| block.boundary_replica_position()),
            &self.resolve.storage, &self.resolve.memory, &self.cancellation,
            self.resolve.owned_witness_scope.is_none() && !self.config.external_source,
            checkpoint.as_ref().is_some_and(NativeServing::is_shared_checkpoint),
        ).await?;
        if let Some(block) = &block {
            let selected = default_erasure::recover_rewrite(&authority, &self.blocks, &self.logs, block,
                checkpoint.as_ref(), &self.cancellation).await?;
            if serving.pending_erasure().is_some() != selected.is_some() { return Err(default_erasure::invalid()); }
            self.default_erasure = selected.map(DefaultErasure::Selected);
        }
        let source_authority = Arc::clone(&serving.source_authority);
        let source_replies = Arc::clone(&serving.source_replies);
        if !self.config.external_source && self.resolve.decision_clock.as_ref().is_some_and(|clock| clock.supports_funded_offers()) {
            serving.funded_record_limit = self.logs.replica_timed_original_append_limit().map(|limit| limit.saturating_sub(1024));
        }
        let funded_record_limit = serving.funded_record_limit;
        self.collect_serving_checkpoints = collect_checkpoints && self.logs.local_recovery_store().is_some();
        self.serving = Some(serving);
        Ok(NativeServingHandle { replication, view, authority, source_authority, source_replies, funded_record_limit,
            local_read_fence: self.local_erasure_read_fence.clone() })
    }

    async fn apply_transaction(&mut self, write: &StagedWrite, search_delta: Option<SearchDelta>, replica: ReplicaPosition) -> Result<()> {
        let _apply = rtdb_plan::ExecutionSample::new(self.resolve.execution_observer.clone(), rtdb_plan::ExecutionTimer::ReadBackApply);
        if let Some(serving) = &self.serving { serving.require_classic_effects(write)?; }
        self.install_classic_transaction(write, search_delta).await?;
        if let Some(serving) = &mut self.serving { serving.observe_classic(write, replica, &self.resolve.memory)?; }
        Ok(())
    }

    async fn install_classic_transaction(&mut self, write: &StagedWrite, search_delta: Option<SearchDelta>) -> Result<()> {
        let Some(builder) = &self.serving_search else { return self.partition.apply(write); };
        let prepared = self.partition.prepare_write(write)?;
        let search = builder.install_delta(prepared.snapshot(), write, search_delta, &self.cancellation).await?;
        self.partition.install_write(prepared, search)
    }
    pub async fn run(mut self) -> Result<()> {
        let result = self.run_inner().await;
        let shutdown = self.demote().await.map(|_| ());
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() { command.fail(stopped()); }
        let outcome = result.and(shutdown);
        let failure = outcome.as_ref().err()
            .filter(|error| error.category != AnomalyCategory::Interrupted)
            .cloned();
        self.progress.driver_stopped(failure);
        self.publish_status(Role::Stopped);
        outcome
    }
    async fn run_inner(&mut self) -> Result<()> {
        if !self.config.enabled {
            self.cancellation.cancelled().await;
            return Ok(());
        }
        let cancellation = self.cancellation.clone();
        if cancellation.is_cancelled() { return Ok(()); }
        let tail = match self.replica_tail.take() {
            Some(tail) => Ok(tail),
            None => self.logs.replica_replay(self.read_position).await,
        };
        if cancellation.is_cancelled() { return Ok(()); }
        let tail = tail?;
        if self.default_erasure.is_none() {
            let Some(history) = cancellation.run_until_cancelled(self.prove_historical_confirmations()).await else { return Ok(()); };
            self.historical_confirmations = history.unwrap_or_default();
        }
        if self.cancellation.is_cancelled() { return Ok(()); }
        self.require_transferred_debt(self.read_position)?;
        self.update_installed_prefix()?;
        self.progress.driver_initialized(self.restart.take())?;
        self.replica_tail = Some(tail);
        self.publish_status(self.role());
        if self.config.may_lead && self.fence.highest_term() == Term(0) { self.start_claim()?; }
        let capacity_interval = self.config.assertion_interval.clamp(Duration::from_millis(1), Duration::from_secs(1));
        let mut capacity_poll = tokio::time::interval(capacity_interval);
        capacity_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if self.cancellation.is_cancelled() { return Ok(()); }
            self.prepare_turn().await?;
            self.prepare_polls();
            enum Event { ReplicaBatch(PartitionReplay, Result<TailPoll>), Replica, SourceBatch(PartitionReplay, Result<TailPoll>), Source, Command(Option<Command>), Deferred, Appender(Result<()>), Resolved(TxResolver, Result<Arc<ResolvedTx>>), Produced(ProductionResult), Claimed(Result<crate::ReplicaMetadata>), Capacity }
            let ready_replica = self.replica_head.is_some() || !self.replica_batch.is_empty();
            let waiting_for_checkpoint = self.waiting_for_checkpoint();
            let checkpoint_blocks_commands = waiting_for_checkpoint || self.has_deferred_checkpoint();
            let placed_open = self.placed_slot.is_some();
            let serving_timed = self.serving_holds_checkpoint();
            let erasure_pending = self.default_erasure.is_some();
            let ready_source = self.ready_source();
            let accept_commands = self.leader.as_ref().is_none_or(|leader| {
                (!checkpoint_blocks_commands && !placed_open && !serving_timed && !erasure_pending && leader.accepting_commands())
                    || leader.deferred_commands.len() < 128
            });
            let retry_erasure = self.retry_default_erasure();
            let check_capacity = waiting_for_checkpoint && self.pending_block.is_none()
                && self.leader.as_ref().is_some_and(|leader| leader.confirmed && leader.accepting());
            let mut empty_poll = None;
            let mut empty_job = None;
            let mut empty_resolution = None;
            let mut empty_production = None;
            let (source_poll, job, resolution, production, ready_source, ready_deferred) = match self.leader.as_mut() {
                Some(leader) => {
                    let ready_deferred = !checkpoint_blocks_commands && !placed_open && !serving_timed && !erasure_pending && leader.accepting_commands()
                        && !leader.deferred_commands.is_empty();
                    (&mut leader.source_poll, &mut leader.job, &mut leader.resolution, &mut leader.production, ready_source, ready_deferred)
                }
                None => (&mut empty_poll, &mut empty_job, &mut empty_resolution, &mut empty_production, false, false),
            };
            let event = tokio::select! {
                _ = self.cancellation.cancelled() => return Ok(()),
                _ = async {}, if ready_replica => Event::Replica,
                (tail, records) = pending_poll(&mut self.replica_poll) => Event::ReplicaBatch(tail, records),
                result = pending_job(job) => Event::Appender(result),
                result = pending_claim(&mut self.claiming) => Event::Claimed(result),
                (resolver, result) = pending_resolution(resolution) => Event::Resolved(resolver, result),
                result = pending_production(production) => Event::Produced(result),
                (tail, records) = pending_poll(source_poll) => Event::SourceBatch(tail, records),
                _ = async {}, if ready_source => Event::Source,
                _ = async {}, if ready_deferred => Event::Deferred,
                command = self.commands.recv(), if accept_commands => Event::Command(command),
                _ = capacity_poll.tick(), if check_capacity || retry_erasure => Event::Capacity,
            };
            match event {
                Event::ReplicaBatch(tail, records) => {
                    self.replica_poll = None; self.replica_tail = Some(tail);
                    let record_count = match records? {
                        TailPoll::Prepared(head) => {
                            let count = usize::from(head.is_some());
                            self.replica_head = head;
                            count
                        }
                        TailPoll::Legacy(records) => {
                            let count = records.len();
                            self.replica_batch.extend(records);
                            count
                        }
                    };
                    self.lead_after_empty_read(record_count).await?;
                }
                Event::Replica => {
                    if !self.consume_replica_batch().await? { return Ok(()); }
                }
                Event::SourceBatch(tail, records) => self.source_polled(tail, records).await?,
                Event::Source => {
                    if let Some(leader) = &mut self.leader && leader.source_batch_end.is_none() {
                        leader.source_batch_end = Some(self.logs.latest_source_position()?);
                    }
                    self.resolve_next_source().await?;
                    self.continue_source_batch().await?;
                }
                Event::Resolved(resolver, result) => self.resolution_finished(resolver, result).await?,
                Event::Produced(result) => self.production_finished(result).await?,
                Event::Claimed(result) => self.claim_finished(result).await?,
                Event::Deferred => {
                    if let Some(command) = self.leader.as_mut().and_then(|leader| leader.deferred_commands.pop_front()) { self.command(command).await?; }
                }
                Event::Command(Some(command)) => self.command(command).await?,
                Event::Command(None) => return Ok(()),
                Event::Capacity => {
                    if check_capacity && let Ok(Some(capacity)) = self.logs.available_append_capacity().await {
                        self.advance_capacity_checkpoint(capacity).await?;
                    }
                }
                Event::Appender(result) => {
                    if let Some(leader) = &mut self.leader { leader.job = None; }
                    match result {
                        Err(error) if matches!(error.code.as_str(), STRICT_DISPATCH_CUTOFF | STRICT_DISPATCH_UNAVAILABLE) => {
                            // Drop the speculative suffix before the source can be resolved again.
                            for held in self.demote().await? { self.apply_admitted(held).await?; }
                            self.start_claim()?;
                        }
                        Err(error) if error.code == APPEND_CAPACITY => {
                            let required_bytes = error.context.get("required-bytes").and_then(|value| value.as_u64())
                                .ok_or_else(|| invalid("Ordinary append refusal has no byte charge"))?;
                            // The refusal is ordered and the rejected append never entered replay.
                            // Reuse the same journal grant and rebuild the local term from its applied prefix.
                            self.logs.suspend_data(true);
                            for held in self.demote().await? { self.apply_admitted(held).await?; }
                            self.required_append_bytes = Some(required_bytes);
                            self.start_claim()?;
                        }
                        Err(error) => return Err(error),
                        Ok(()) if self.cancellation.is_cancelled() => return Ok(()),
                        Ok(()) => return Err(invalid("Replica append pump exited during an active term")),
                    }
                }
            }
        }
    }
    /// The work each turn of the event loop does before it waits, which a batch also does between its records.
    async fn prepare_turn(&mut self) -> Result<()> {
        self.drive_serving_terminal().await?;
        self.drive_default_erasure().await?;
        self.drive_placed_slot()?;
        if !self.waiting_for_checkpoint() && self.default_erasure.is_none()
            && self.leader.as_ref().is_some_and(LeaderState::accepting_commands)
            && self.block_needs_cut()
        {
            self.cut().await?;
        }
        Ok(())
    }
    /// Whether the leader may resolve its next source record at `now`.
    fn ready_source(&self) -> bool {
        let serving_source_ready = self.default_erasure.is_none() && self.serving.as_ref().is_none_or(NativeServing::source_ready);
        !self.waiting_for_checkpoint() && self.placed_slot.is_none() && serving_source_ready
            && self.leader.as_ref().is_some_and(|leader| leader.ready_source()
                && (self.pending_block.is_none() || !matches!(leader.waiting_strict_source, Some(StrictSource::DeferredFlush(_) | StrictSource::Interactive(_))))
                && (!matches!(leader.waiting_strict_source, Some(StrictSource::Interactive(_)))
                    || !self.serving.as_ref().is_some_and(NativeServing::main_write_pending)))
    }
    async fn source_polled(&mut self, tail: PartitionReplay, records: Result<TailPoll>) -> Result<()> {
        let Some(leader) = &mut self.leader else { return Ok(()); };
        leader.source_poll = None; leader.source_tail = Some(tail);
        match records? {
            TailPoll::Prepared(head) => leader.source_head = head,
            TailPoll::Legacy(records) => leader.source_batch.extend(records),
        }
        self.flush_if_due().await
    }
    /// Returns false when cancelled.
    async fn consume_replica_batch(&mut self) -> Result<bool> {
        while let Some(record) = self.replica_batch.pop_front() {
            self.consume(record).await?;
            if self.cancellation.is_cancelled() { return Ok(false); }
            if !self.replica_batch.is_empty() { self.prepare_turn().await?; }
        }
        if self.replica_head.is_none() { return Ok(true); }
        let end = self.logs.latest_replica_position()?;
        while let Some(head) = self.replica_head {
            let tail = self.replica_tail.as_mut().ok_or_else(|| invalid("Prepared replica head has no tail"))?;
            let memory = self.leader.as_ref().and_then(|leader| leader.pending.front())
                .and_then(|write| write.interactive.as_ref()).map_or(self.logs.memory(), |prepared| &prepared.io);
            let Some(record) = read_replay_head(tail, head, memory, replica_decode_bound, &self.cancellation).await? else { return Ok(false); };
            self.replica_head = None;
            self.consume_record(record.raw, Some(record.decode_reservation)).await?;
            if self.cancellation.is_cancelled() { return Ok(false); }
            if self.read_position >= end { break; }
            self.prepare_turn().await?;
            if self.replica_poll.is_some() || self.replica_head.is_some() || !self.replica_batch.is_empty() { break; }
            let Some(replay) = self.replica_tail.as_mut().and_then(PartitionReplay::replay) else { break; };
            match replay.poll_head(Duration::ZERO).await {
                Ok(head) => self.replica_head = head,
                Err(StorageError::LogFetchPending { .. }) => break,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(true)
    }
    /// Resolves the rest of the source batch, a record at a time, while the leader may resolve and the next record is
    /// already durable and within the batch. A record whose resolution is in flight ends this call; its completion
    /// continues the batch. The batch ends where the leader stops.
    async fn continue_source_batch(&mut self) -> Result<()> {
        loop {
            let Some(leader) = &mut self.leader else { return Ok(()); };
            if leader.resolution.is_some() { return Ok(()); }
            let Some(end) = leader.source_batch_end else { return Ok(()); };
            if leader.source_head.is_none() && leader.source_batch.is_empty() && leader.waiting_strict_source.is_none() {
                if let Some(polled) = leader.source_poll.as_mut().and_then(futures::FutureExt::now_or_never) {
                    let (tail, records) = polled;
                    self.source_polled(tail, records).await?;
                }
            }
            self.prepare_turn().await?;
            let ready = self.ready_source();
            let Some(leader) = &mut self.leader else { return Ok(()); };
            let next = match (leader.source_head, leader.source_batch.front()) {
                (Some(head), _) => Some(head.metadata),
                (None, Some(record)) => Some(record.metadata),
                (None, None) => None,
            };
            let within = match next { Some(metadata) => self.logs.source_position(metadata)? <= end, None => false };
            if !ready || !within || leader.waiting_strict_source.is_some() {
                leader.source_batch_end = None;
                return Ok(());
            }
            self.resolve_next_source().await?;
        }
    }
    fn waiting_for_checkpoint(&self) -> bool {
        self.deferred_cut || self.required_append_bytes.is_some() || self.deferred_checkpoint_backpressured()
    }

    fn has_deferred_checkpoint(&self) -> bool {
        self.pending_block.as_ref().is_some_and(PendingBlock::is_deferred)
    }

    fn deferred_checkpoint_backpressured(&self) -> bool {
        if !self.has_deferred_checkpoint() { return false; }
        let memory = &self.resolve.memory;
        memory.available_bytes() <= (memory.limit_bytes() / 3).max(1)
            || self.leader.as_ref().is_some_and(|leader| self.block_needs_memory_cut(leader))
    }

    fn holds_replica_records(&self) -> bool {
        self.pending_block.as_ref().is_some_and(|pending| !pending.is_deferred())
    }

    fn serving_holds_checkpoint(&self) -> bool {
        self.serving.as_ref().is_some_and(NativeServing::holds_checkpoint)
    }

    async fn drive_serving_terminal(&mut self) -> Result<()> {
        if self.placed_slot.is_some() || !self.leader.as_ref().is_some_and(|leader| {
            leader.confirmed && leader.cycle.accepting() && leader.resolution.is_none()
                && leader.awaiting_placed.is_none() && !leader.settling_placed
                && !leader.pending.front().is_some_and(|pending| pending.interactive.is_some())
        }) { return Ok(()); }
        let Some(serving) = &mut self.serving else { return Ok(()); };
        let now = (self.clock)();
        let upper = Timestamp(now.0.saturating_add(SERVING_CLOCK_UNCERTAINTY_MICROS));
        let Some(message) = serving.progress_timed(upper)? else { return Ok(()); };
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        leader.cycle.resolved(1)?;
        let block = self.partition.value().live().block_index();
        leader.enqueue(WriteBody::Message(ReplicaBody::NoOp(message)), block, None, &self.resolve.memory)?;
        if self.block_needs_cut() { self.cut().await?; }
        Ok(())
    }

    fn adopted_boundary(&self) -> Option<LogOffset> {
        let partition = self.partition.value();
        let boundary = partition.table_catalog().block()?.boundary_replica_position()?;
        Some(LogOffset(boundary.after_offset(self.logs.replica_epoch)))
    }

    async fn advance_capacity_checkpoint(&mut self, capacity: AppendCapacity) -> Result<()> {
        let adopted = self.adopted_boundary();
        if let Some(required) = self.required_append_bytes
            && capacity.last_data.is_none_or(|last| adopted.is_some_and(|boundary| boundary >= last))
        {
            if capacity.available_bytes >= required {
                let tail = self.logs.source_replay(self.progress.current().source_position).await?;
                self.leader.as_mut().ok_or_else(term_ended)?.source_tail = Some(tail);
                self.required_append_bytes = None;
                self.logs.suspend_data(false);
            }
            return Ok(());
        }
        // A chosen block owns its maintenance records until its catalog is published.
        if adopted.is_none_or(|boundary| boundary <= capacity.published_through) {
            self.enqueue_checkpoint_cut()?;
        }
        Ok(())
    }

    async fn prove_historical_confirmations(&mut self) -> Result<VecDeque<RecoveredBlockConfirmation>> {
        let Some(selected) = &self.recovered_confirmation else { return Ok(VecDeque::new()); };
        let selected = &selected.catalog;
        let mut retained = self.blocks.retained_catalogs(selected, self.logs.replica_epoch, &self.cancellation).await?;
        if retained.is_empty() { return Ok(VecDeque::new()); }
        let mut first = 0;
        let mut tail = loop {
            match self.logs.replica_replay(retained[first].boundary).await {
                Ok(tail) => break tail,
                Err(rtdb_storage::Error::UnavailableLogRange { requested, first_retained })
                    if requested < first_retained && first + 1 < retained.len() => first += 1,
                Err(error) => return Err(error.into()),
            }
        };
        let anchor = &retained[first];
        let mut witness = TermFence::from_catalog(&anchor.catalog)?;
        let mut position = anchor.boundary;
        let mut next_boundary = first + 1;
        let end = selected.boundary_replica_position().ok_or_else(|| invalid("Selected checkpoint has no replica boundary"))?;
        loop {
            let head = tail.replay().ok_or_else(|| invalid("Historical replay requires an admitted replica tail"))?
                .poll_head(self.config.assertion_interval.saturating_mul(10)).await?
                .ok_or_else(|| invalid("Retained replica history ended before the selected checkpoint"))?;
            let Some(selected_record) = read_replay_head(&mut tail, head, self.logs.memory(), replica_decode_bound, &self.cancellation).await? else {
                return Ok(VecDeque::new());
            };
            let next = ReplicaPosition::from_epoch_offset(self.logs.replica_epoch, selected_record.raw.metadata.offset.0)?;
            if next <= position || next > end {
                return Err(invalid("Retained replica history did not reach the selected boundary in order"));
            }
            position = next;
            let record = match self.logs.decode_readback(selected_record.raw, Some(selected_record.decode_reservation), None, None)? {
                DecodedReplica::Known(record) => {
                    let admission = witness.admit(record.envelope.term, record.envelope.term_sequence)?;
                    if admission.verdict == Verdict::Apply { Some(self.logs.resolve_oversized(record).await?) } else { None }
                }
                DecodedReplica::Unrecognized { term, sequence } => {
                    if witness.admit(term, sequence)?.verdict == Verdict::Apply {
                        return Err(invalid("Historical replica prefix contains an admitted unrecognized frame"));
                    }
                    None
                }
            };
            if position == end {
                let matches_fence = witness == self.fence
                    || (selected.boundary_term() > Term(0)
                        && selected.boundary_term_sequence().is_none()
                        && witness.highest_term() == self.fence.highest_term()
                        && matches!((self.fence.position(), witness.position()), (TermPosition::Unknown, TermPosition::Unsequenced)));
                if next_boundary != retained.len() || !matches_fence
                    || !record.as_ref().is_some_and(|record| matches_catalog_boundary(selected, record)) {
                    return Err(invalid("Historical replica fence or checkpoint does not match startup"));
                }
                break;
            }
            if let Some(candidate) = retained.get(next_boundary).filter(|candidate| candidate.boundary <= position) {
                if !record.as_ref().is_some_and(|record| matches_catalog_boundary(&candidate.catalog, record)) {
                    return Err(invalid("Historical block catalog boundary is missing from admitted replica history"));
                }
                next_boundary += 1;
            }
            if let Some(record) = record {
                if let ReplicaBody::BlockUploaded(upload) = &record.envelope.message {
                    if let Some(candidate) = retained[first..].iter_mut().find(|candidate| {
                        candidate.boundary < position
                            && matches_confirmation(&candidate.catalog, upload, self.logs.storage_version, self.logs.storage_epoch)
                    }) {
                        validate_uploaded_root(&candidate.catalog, upload, self.logs.requires_checkpoint_digest())?;
                        candidate.confirmed = true;
                    }
                }
            }
        }
        let mut confirmations = VecDeque::new();
        for candidate in retained.into_iter().skip(first).filter(|candidate| !candidate.confirmed) {
            let confirmation = self.blocks.historical_confirmation(&candidate.catalog, self.logs.storage_version, self.logs.storage_epoch, &self.cancellation).await?;
            confirmations.push_back(confirmation);
        }
        Ok(confirmations)
    }

    fn prepare_polls(&mut self) {
        if self.replica_poll.is_none() && self.replica_head.is_none() && self.replica_batch.is_empty() {
            if let Some(tail) = self.replica_tail.take() {
                self.config.election_seed = self.config.election_seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let factor = 5 + self.config.election_seed % 6;
                let timeout = match self.config.claim_wait {
                    Some(wait) if self.config.may_lead && self.leader.is_none() => wait,
                    _ => self.config.assertion_interval.saturating_mul(factor as u32),
                };
                self.replica_poll = Some(start_poll(tail, timeout));
            }
        }
        if !self.waiting_for_checkpoint() && self.default_erasure.is_none() && self.placed_slot.is_none() && let Some(leader) = &mut self.leader {
            if leader.can_poll_source() && leader.waiting_strict_source.is_none()
                && leader.source_poll.is_none() && leader.source_head.is_none() && leader.source_batch.is_empty() {
                if let Some(tail) = leader.source_tail.take() { leader.source_poll = Some(start_poll(tail, self.config.assertion_interval)); }
            }
        }
    }
    /// A journal that fences writer terms promotes its grant holder only after an empty read, so every record of
    /// an earlier term has been admitted before the new term's sequence-zero record can open it.
    async fn lead_after_empty_read(&mut self, record_count: usize) -> Result<()> {
        if !replica_read_is_empty(record_count) || self.cancellation.is_cancelled() { return Ok(()); }
        if self.logs.placed_transfer().is_some_and(|transfer|
            self.read_position.after_offset(self.logs.replica_epoch) < transfer.through)
        {
            return Ok(());
        }
        if !self.logs.fences_writer_terms() { return self.start_claim(); }
        let holds_grant = self.config.may_lead && self.leader.is_none() && self.logs.holds_writer_grant().await?;
        if !crate::recovery_promotion::may_promote(
            self.recognized_prefix,
            self.applied_prefix.is_complete_at(self.read_position.0),
            self.config.may_lead,
            self.leader.is_some(),
            holds_grant,
        ) { return Ok(()); }
        let term = self.fence.claim_term()?;
        if let Err(error) = self.promote_term(term).await {
            self.promotion_failed(error).await?;
        } else { self.status.send_modify(|status| status.promotion_failure = None); }
        Ok(())
    }
    fn start_claim(&mut self) -> Result<()> {
        if !self.recognized_prefix { return Ok(()); }
        if self.logs.placed_transfer().is_some_and(|transfer|
            self.read_position.after_offset(self.logs.replica_epoch) < transfer.through)
        {
            return Ok(());
        }
        // Promotion waits for the next empty read instead of appending a claim.
        if !should_start_claim(self.logs.fences_writer_terms(), self.config.may_lead, self.leader.is_some(),
            self.claim.is_some() || self.claiming.is_some())
        {
            return Ok(());
        }
        let term = self.fence.claim_term()?;
        let envelope = ReplicaEnvelope { term, term_sequence: Some(TermSequence(0)), message: ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }) };
        let logs = Arc::clone(&self.logs);
        let clock = Arc::clone(&self.clock);
        let block = self.partition.value().live().block_index();
        self.claiming = Some(Claim { term, observed: None, future: Box::pin(async move {
            logs.enqueue_replica(&envelope, block, clock()).await?.durable().await
        }) });
        Ok(())
    }
    async fn claim_finished(&mut self, result: Result<crate::ReplicaMetadata>) -> Result<()> {
        let claim = self.claiming.take().ok_or_else(|| invalid("Claim completed without an owned append"))?;
        let Ok(metadata) = result else { return Ok(()); };
        if self.fence.highest_term() > claim.term || matches!(self.fence.position(), crate::TermPosition::Voided) { return Ok(()); }
        if claim.observed == Some(metadata.position) {
            self.try_promote().await?;
        } else if metadata.position > self.read_position {
            self.claim = Some(metadata.position);
        }
        Ok(())
    }
    async fn try_promote(&mut self) -> Result<()> {
        if !self.recognized_prefix { return Ok(()); }
        if let Err(error) = self.promote().await {
            self.promotion_failed(error).await?;
        } else { self.status.send_modify(|status| status.promotion_failure = None); }
        Ok(())
    }

    async fn promotion_failed(&mut self, error: Anomaly) -> Result<()> {
        let held = self.demote().await?;
        for held in held { self.apply_admitted(held).await?; }
        self.status.send_modify(|status| status.promotion_failure = Some(error));
        Ok(())
    }
    async fn promote(&mut self) -> Result<()> { self.promote_term(self.fence.highest_term()).await }
    async fn promote_term(&mut self, term: Term) -> Result<()> {
        let progress = self.progress.current();
        let persisted = self.partition.value().table_catalog().block().and_then(BlockCatalog::latest_completed_tx).map(|key| key.tx_id);
        self.flush_timer = FlushTimer::new(now(), persisted, self.config.flush_timeout)?;
        let cancellation = self.cancellation.child_token();
        // Reopen from the installed source position after adopting an inherited block
        // or settling a recovered timed original.
        let source_tail = if self.pending_block.is_some() || self.waiting_for_checkpoint() || self.serving_holds_checkpoint()
            || self.default_erasure.is_some()
        {
            None
        } else {
            Some(self.logs.source_replay(progress.source_position).await?)
        };
        let (appender, pump) = ReplicaAppender::new(Arc::clone(&self.logs), Arc::clone(&self.resolve.memory),
            term, self.config.assertion_interval, cancellation.clone(), Arc::clone(&self.clock));
        // Without a claim, the term's first appended record opens it.
        let pump = pump.starting_at(TermSequence(initial_term_sequence(self.logs.fences_writer_terms())))
            .with_execution_observer(self.resolve.execution_observer.clone());
        let pump = match self.resolve.execution_observer.clone() {
            Some(observer) => pump.with_durable_commit_observer(move |basis, lag| match basis {
                CommitLagBasis::Ordered => observer.record_durable_commit_lag(lag),
                CommitLagBasis::Staged => observer.record_staged_commit_lag(lag),
            }),
            None => pump,
        };
        let cycle = if self.pending_block.is_some() { BlockCycle::Cut } else { BlockCycle::Filling { rows: self.partition.value().live().block_row_count() } };
        let recovery_write = self.next_recovery_write().await?;
        let produce_inherited = recovery_write.is_none() && self.pending_block.is_some();
        let confirmed = recovery_write.is_none() && !produce_inherited;
        let mut resolver = TxResolver::new(progress.tx_key, progress.source_position, progress.external_token.as_deref().map(<[u8]>::to_vec), &self.resolve.memory)?;
        if self.prepares_local_interactive()
            && let Err(error) = resolver.initialize_receipt_clock(self.partition.snapshot(), &self.resolve).await
        {
            tracing::warn!(database = %self.resolve.database, %error, "Interactive receipt clock is unavailable");
        }
        let job = pump.with_heartbeat(self.config.idle_heartbeat).run(recovery_write.clone());
        self.leader = Some(LeaderState { term, resolver: Some(resolver), resolution: None,
            production: None, confirmed, activation: writer_activation(self.logs.fences_writer_terms()),
            recovery_write, held: VecDeque::new(), held_bytes: 0, cycle,
            pending: VecDeque::new(), source_tail, source_poll: None, source_batch: VecDeque::new(), source_head: None, waiting_strict_source: None, awaiting_placed: None, settling_placed: false, appender, job: Some(tokio::spawn(job)), cancellation, deferred_commands: VecDeque::new(), compactor_reset: None, trie_removals: Vec::new(), source_batch_end: None });
        if produce_inherited {
            self.produce_pending()?;
        }
        self.activate_installed_writer();
        self.publish_status(self.role());
        Ok(())
    }
    async fn demote(&mut self) -> Result<VecDeque<AdmittedReplica>> {
        self.deferred_cut = false;
        if let Some(serving) = &mut self.serving { serving.withdraw(); }
        if let Some(mut leader) = self.leader.take() {
            leader.cancellation.cancel();
            drop(leader.compactor_reset.take());
            fail_pending(&mut leader.pending, term_ended());
            for command in leader.deferred_commands.drain(..) { command.fail(term_ended()); }
            drop(leader.resolution.take());
            if let Some(resolver) = &mut leader.resolver { resolver.discard_staged(); }
            let result = if let Some(job) = leader.job.take() { job.await.map_err(join_error).and_then(|result| result) } else { Ok(()) };
            if let Some(production) = leader.production.take() {
                match production {
                    Production::Writing(_, future) => { if future.await.is_ok() { self.record_block_upload(); } }
                    Production::Appending(_, future, _) => { let _ = future.await; }
                    Production::AwaitingReadback(..) => {}
                }
            }
            self.publish_status(Role::Following);
            result?;
            return Ok(leader.held);
        }
        Ok(VecDeque::new())
    }
    fn activate_installed_writer(&mut self) {
        let prefix_applied = self.applied_prefix.is_complete_at(self.read_position.0);
        if let Some(leader) = &mut self.leader {
            leader.activation = leader.activation.installed(
                leader.confirmed,
                self.recognized_prefix,
                prefix_applied,
                self.read_position.0,
                self.installed.map(|prefix| prefix.replica.0),
            );
        }
    }

    fn role(&self) -> Role {
        self.leader.as_ref().filter(|leader| leader.confirmed && leader.activation.is_active())
            .map_or(Role::Following, |leader| Role::Leading(leader.term))
    }
    fn prepares_local_interactive(&self) -> bool {
        self.logs.uses_volatile_intake() && self.resolve.owned_witness_scope.is_none()
            && !self.config.external_source && self.serving_search.is_none()
    }

    fn publish_status(&self, role: Role) {
        let value = self.partition.value();
        let (promotion_failure, last_block_upload_epoch_seconds, memory_block_cuts, previous_interactive_ready) = {
            let status = self.status.borrow();
            (status.promotion_failure.clone(), status.last_block_upload_epoch_seconds, status.memory_block_cuts,
                status.role == role && status.interactive_ready)
        };
        let interactive_ready = matches!(role, Role::Leading(_)) && self.prepares_local_interactive()
            && self.leader.as_ref().and_then(|leader| leader.resolver.as_ref())
                .map_or(previous_interactive_ready, TxResolver::receipt_clock_ready);
        self.status.send_replace(DriverStatus { role, highest_term: self.fence.highest_term(), replica_position: self.read_position,
            interactive_ready, installed: self.installed, first_erasure: self.first_erasure,
            open_block: value.live().block_index(), adopted_block: value.table_catalog().block().cloned().map(Arc::new), promotion_failure, last_block_upload_epoch_seconds, memory_block_cuts });
    }
    fn observe_erasure(&mut self, position: ReplicaPosition, write: &StagedWrite) -> Result<()> {
        if self.track_catalog_cut && self.first_erasure.is_none() {
            self.first_erasure = first_write_erasure(position, write)?;
        }
        Ok(())
    }

    fn update_installed_prefix(&mut self) -> Result<()> {
        let apply_deferred = self.leader.as_ref().is_some_and(|leader| !leader.held.is_empty());
        let completed = crate::catalog_cut::completed_prefix(
            self.read_position.0, self.recognized_prefix, self.holds_replica_records(), apply_deferred,
        );
        let prefix_applied = self.applied_prefix.is_complete_at(self.read_position.0);
        if let Some(through) = publish_installed_prefix(self.recognized_prefix, completed, prefix_applied) {
            let through = ReplicaPosition(through);
            let checkpoint = self.partition.value();
            let progress = self.progress.current();
            if self.logs.fences_writer_terms() && checkpoint.live().latest_completed_tx() != progress.tx_key {
                return Err(invalid("Installed replica state and transaction progress disagree"));
            }
            if self.track_catalog_cut && checkpoint.table_catalog().block().is_none_or(|block| {
                block.boundary_replica_position().is_some_and(|boundary| {
                    boundary.after_offset(self.logs.replica_epoch) >= 0 && boundary <= through
                })
            }) {
                let catalog = CatalogCut {
                    storage_version: self.logs.storage_version,
                    storage_epoch: self.logs.storage_epoch,
                    replica_epoch: self.logs.replica_epoch,
                    through,
                };
                self.partition.publish_catalog_cut(catalog)?;
                if let Some(serving) = &self.serving {
                    serving.publish_ranked_catalog(catalog);
                }
            }
            let source = progress.source_position;
            self.logs.record_source_applied(source);
            self.installed = Some(InstalledPrefix {
                replica: through,
                source,
            });
            if let Some(leader) = &mut self.leader
                && let Some(reset) = leader.compactor_reset.as_ref()
                && self.partition.value().table_catalog().block_index() == Some(reset.block)
            {
                leader.compactor_reset.take().expect("matched reset block").complete();
            }
        }
        self.activate_installed_writer();
        self.complete_trie_removals();
        Ok(())
    }

    fn complete_trie_removals(&mut self) {
        let Some(leader) = &mut self.leader else { return; };
        if leader.trie_removals.is_empty() { return; }
        let view = self.partition.value();
        let recognized = self.recognized_prefix;
        let installed = self.installed.map(|prefix| prefix.replica);
        let pending = &mut leader.pending;
        leader.trie_removals.retain(|receipt| {
            if recognized && installed.is_none_or(|through| through < receipt.position) { return true; }
            let required = receipt.deletions.iter().any(|(table, keys)| {
                keys.iter().any(|key| view.retains_trie(table, key))
            });
            let confirmed = crate::retirement_receipt::confirms_removal(
                recognized, installed.map_or(i64::MIN, |through| through.0), receipt.position.0, required,
            );
            let outcome = if confirmed { Ok(Applied::TrieRemoval(receipt.position)) } else { Err(trie_removal_unconfirmed()) };
            complete_record(pending, Some(receipt.sequence), outcome);
            false
        });
    }
    /// From the cut to the durable append of its `BlockUploaded`, as upstream's `BlockCutter` times it.
    fn time_block_upload(&self, started: std::time::Instant) {
        if let Some(observer) = &self.resolve.execution_observer { observer.record_duration(rtdb_plan::ExecutionTimer::BlockUpload, started.elapsed()); }
    }
    fn record_block_upload(&self) {
        let upload_time = (self.clock)().0.div_euclid(1_000_000);
        self.status.send_modify(|status| status.last_block_upload_epoch_seconds = upload_time);
    }
    async fn consume(&mut self, raw: LogRecord) -> Result<()> {
        self.consume_record(raw, None).await
    }
    async fn consume_record(&mut self, raw: LogRecord, decode_reservation: Option<Arc<MemoryReservation>>) -> Result<()> {
        let position = ReplicaPosition::from_epoch_offset(self.logs.replica_epoch, raw.metadata.offset.0)?;
        if position <= self.read_position { return Err(invalid("Replica reader did not advance")); }
        if !self.applied_prefix.admit(position.0) { return Err(invalid("Replica record regressed the applied prefix")); }
        let own_term = self.leader.as_ref().map(|leader| leader.term);
        // Buffered read-back may outlive this leader and be applied as follower replay.
        let immediate = !self.holds_replica_records() && self.claim != Some(position)
            && self.leader.as_ref().is_some_and(|leader| leader.resolution.is_none());
        let decoded = {
            let expected = |sequence| own_term.and_then(|term| self.own_pending_tx_at(term, sequence))
                .and_then(|pending| pending.append_stamp()?.fingerprint);
            self.logs.decode_readback(raw, decode_reservation, own_term, immediate.then_some(&expected as &dyn Fn(_) -> _))?
        };
        let (observed_term, admission, record, lost_prefix) = match decoded {
            DecodedReplica::Known(record) => {
                let term = record.envelope.term;
                let (admission, record) = self.fence.admit_record(record)?;
                (term, admission, record, false)
            }
            DecodedReplica::Unrecognized { term, sequence } => {
                let admission = self.fence.admit(term, sequence)?;
                let lost_prefix = admission.verdict == Verdict::Apply;
                if lost_prefix {
                    if let Some(pending) = self.own_pending_tx_at(term, sequence) {
                        let error = readback_mismatch(None);
                        pending.finish_readback_failure(error.clone());
                        return Err(error);
                    }
                    if self.leader.as_ref().is_some_and(|leader| leader.term == term && leader.pending_serving_at(sequence).is_some()) {
                        return Err(invalid("Read-back omitted the writer's serving decision"));
                    }
                }
                if self.logs.is_copied_replica_position(position) {
                    return Err(invalid("The held transfer contains an unrecognized replica frame"));
                }
                if self.logs.fences_writer_terms() && self.leader.is_some() {
                    return Err(invalid("The current writer cannot apply an unrecognized replica frame"));
                }
                self.recognized_prefix &= !lost_prefix;
                (term, admission, None, lost_prefix)
            }
        };
        let wins = self.claim == Some(position) && admission.opens_term();
        if let Some(claim) = &mut self.claiming {
            if record.is_some() && observed_term == claim.term && admission.opens_term() { claim.observed = Some(position); }
        }
        if self.claim == Some(position) { self.claim = None; }
        if self.leader.as_ref().is_some_and(|leader| lost_prefix || admission.highest_term > leader.term || admission.verdict == Verdict::Voided) {
            let held = self.demote().await?;
            for held in held { self.apply_admitted(held).await?; }
        }
        if let Some(record) = record {
            if wins { self.try_promote().await?; }
            let sequence = record.envelope.term_sequence;
            let expected = if immediate && Some(observed_term) == own_term {
                self.own_pending_tx_at(observed_term, sequence).and_then(|pending| pending.append_stamp()?.fingerprint)
            } else { None };
            let record = record.resolve_payload_for_readback(&self.logs, expected).await.inspect_err(|error| {
                if let Some(pending) = self.own_pending_tx_at(observed_term, sequence) {
                    pending.finish_readback_failure(error.clone());
                }
            })?;
            self.validate_own_readback(&record)?;
            if !self.confirm_recovered_block(&record).await? {
                self.apply_admitted(record).await?;
            } else {
                self.record_applied(position)?;
            }
        } else {
            self.record_applied(position)?;
        }
        self.require_transferred_debt(position)?;
        self.read_position = position;
        self.update_installed_prefix()?;
        self.publish_status(self.role());
        if admission.verdict == Verdict::Voided { self.start_claim()?; }
        Ok(())
    }
    fn require_transferred_debt(&self, position: ReplicaPosition) -> Result<()> {
        let Some(transfer) = self.logs.placed_transfer() else { return Ok(()); };
        if position.after_offset(self.logs.replica_epoch) != transfer.through {
            return Ok(());
        }
        match (self.placed_slot, transfer.debt.as_ref()) {
            (None, None) => Ok(()),
            (Some(slot), Some(debt)) if debt.term == slot.term.0
                && debt.reserved_at == slot.reserved_at.after_offset(self.logs.replica_epoch)
                && PlacedReservation::decode(&debt.reservation)? == slot.reservation => Ok(()),
            _ => Err(invalid("The installed transfer differs from its sealed placed debt census")),
        }
    }

    fn own_pending_tx_at(&self, term: Term, sequence: Option<TermSequence>) -> Option<&PendingWrite> {
        let leader = self.leader.as_ref().filter(|leader| own_readback_term(Some(leader.term.0), term.0))?;
        leader.pending_tx_at(sequence).map(|(pending, _)| pending.as_ref())
    }
    fn validate_own_readback(&self, record: &ReplicaRecord) -> Result<()> {
        let Some(leader) = self.leader.as_ref().filter(|leader| own_readback_term(Some(leader.term.0), record.envelope.term.0)) else { return Ok(()); };
        let expected_serving = leader.pending_serving_at(record.envelope.term_sequence);
        let observed_serving = match &record.envelope.message {
            ReplicaBody::NoOp(message) if message.serving_record.is_some() => Some(message),
            _ => None,
        };
        if expected_serving != observed_serving {
            return Err(invalid("Read-back changed the writer's serving decision"));
        }
        let resolved = matches!(&record.envelope.message, ReplicaBody::ResolvedTx(_));
        let Some((pending, _)) = leader.pending_tx_at(record.envelope.term_sequence) else {
            return if resolved { Err(readback_mismatch(replica_tx_id(record))) } else { Ok(()) };
        };
        let appended = pending.append_stamp().and_then(|stamp| stamp.fingerprint.as_ref());
        if readback_matches_transaction(resolved, appended, record.fingerprint.as_ref()) {
            return Ok(());
        }
        let error = readback_mismatch(replica_tx_id(record));
        pending.finish_readback_failure(error.clone());
        Err(error)
    }
    async fn next_recovery_write(&mut self) -> Result<Option<Arc<PendingWrite>>> {
        let block = self.partition.value().live().block_index();
        if let Some(checkpoint) = self.historical_confirmations.front() {
            if self.blocks.require_confirmation_files(checkpoint, &self.cancellation).await.is_ok() {
                if let Ok(write) = PendingWrite::clone_message(&checkpoint.body, block, &self.resolve.memory) {
                    return Ok(Some(write));
                }
            }
            self.historical_confirmations.clear();
        }
        let Some(checkpoint) = &self.recovered_confirmation else { return Ok(None); };
        self.blocks.require_confirmation_files(checkpoint, &self.cancellation).await?;
        PendingWrite::clone_message(&checkpoint.body, block, &self.resolve.memory).map(Some)
    }

    async fn confirm_recovered_block(&mut self, record: &ReplicaRecord) -> Result<bool> {
        let required = self.logs.requires_checkpoint_digest();
        let mut historical = None;
        for (index, checkpoint) in self.historical_confirmations.iter().enumerate() {
            if checkpoint.matches(record, required)? {
                historical = Some(index);
                break;
            }
        }
        let current = match &self.recovered_confirmation {
            Some(checkpoint) => checkpoint.matches(record, required)?,
            None => false,
        };
        if historical.is_none() && !current { return Ok(false); }

        let own = if let Some(leader) = &self.leader {
            if let Some(write) = &leader.recovery_write {
                let sequence = write.sequence().ok_or_else(|| invalid("Recovered block upload has no assigned sequence"))?;
                if record.envelope.term != leader.term || record.envelope.term_sequence != Some(sequence) {
                    return Err(invalid("Recovered block upload does not match this term's pending confirmation"));
                }
                true
            } else { false }
        } else { false };

        if let Some(index) = historical { self.historical_confirmations.remove(index); }
        if current {
            self.observe_default_erasure_upload(record)?;
            self.recovered_confirmation = None;
        }
        if own {
            if let Err(error) = self.continue_recovery().await {
                self.promotion_failed(error).await?;
            }
        }
        Ok(true)
    }

    async fn continue_recovery(&mut self) -> Result<()> {
        let next = self.next_recovery_write().await?;
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        if let Some(write) = next {
            leader.appender.enqueue(Arc::clone(&write))?;
            leader.recovery_write = Some(write);
            return Ok(());
        }

        leader.recovery_write = None;
        leader.confirmed = self.pending_block.is_none();
        if self.pending_block.is_some() {
            self.produce_pending()?;
        }
        Ok(())
    }

    /// Records may apply out of replica order (held behind a pending block or an in-flight resolution),
    /// so `applied_prefix` completes only once every earlier admitted position has applied too.
    fn record_applied(&mut self, position: ReplicaPosition) -> Result<()> {
        if self.applied_prefix.applied(position.0) { Ok(()) }
        else { Err(invalid("Replica record was applied twice or without admission to the applied prefix")) }
    }

    /// Times a record this node applies as a follower, however its handling ends, as the reference's `FollowerLogProcessor` does.
    fn replica_process_timer(&self, message: &ReplicaBody) -> Option<ReplicaProcessTimer> {
        use rtdb_plan::ReplicaRecordKind as Kind;
        if self.leader.is_some() { return None; }
        let kind = match message {
            ReplicaBody::ResolvedTx(_) => Kind::ResolvedTx, ReplicaBody::TriesAdded(_) => Kind::TriesAdded, ReplicaBody::BlockBoundary(_) => Kind::BlockBoundary,
            ReplicaBody::BlockUploaded(_) => Kind::BlockUploaded, ReplicaBody::TriesDeleted(_) => Kind::TriesDeleted, _ => return None,
        };
        let observer = Arc::clone(self.resolve.execution_observer.as_ref()?);
        Some(ReplicaProcessTimer { observer, database: self.resolve.database.clone(), kind, started: std::time::Instant::now() })
    }

    async fn apply_admitted(&mut self, record: AdmittedReplica) -> Result<()> {
        if let ReplicaBody::NoOp(message) = &record.envelope.message
            && message.src_msg_id.is_none()
            && message.placed_slot_reservation.is_none()
            && message.serving_record.is_none()
        {
            return self.record_applied(record.metadata.position);
        }
        if let Some(leader) = &mut self.leader {
            if leader.resolution.is_some() {
                let bytes = leader.held_bytes.checked_add(record.retained_bytes()).ok_or_else(|| invalid("Resolution read-back buffer size overflow"))?;
                if leader.held.len() >= self.config.pending_block_records || bytes > self.config.pending_block_bytes {
                    return Err(Anomaly::fault("xtdb.indexer/leader-buffer-overflow", "In-flight resolution exceeded its retained read-back limit"));
                }
                leader.held_bytes = bytes;
                leader.held.push_back(record);
                return Ok(());
            }
        }
        let mut work = VecDeque::from([record]);
        let mut adopted = false;
        while let Some(record) = work.pop_front() {
            self.require_default_erasure_order(&record)?;
            let closes = match (&self.pending_block, &record.envelope.message) {
                (Some(pending), ReplicaBody::BlockUploaded(message)) => pending.matches_upload(message, self.logs.storage_version, self.logs.storage_epoch),
                _ => false,
            };
            if closes {
                let _timer = self.replica_process_timer(&record.envelope.message);
                if self.leader.is_none() && self.holds_replica_records()
                    && let (Some(observer), Some(pending)) = (&self.resolve.execution_observer, &self.pending_block) {
                    observer.record_replica_block_buffer(&self.resolve.database, pending.opened().elapsed(), pending.buffered_len());
                }
                let collection = if let Some(production) = self.leader.as_mut().and_then(|leader| leader.production.take()) {
                    let collection = match production {
                        Production::Writing(_, future) => { future.await?; self.record_block_upload(); None }
                        Production::Appending(started, future, seal) => {
                            let receipt = future.await?;
                            self.time_block_upload(started);
                            seal.map(|seal| (receipt, seal))
                        }
                        Production::AwaitingReadback(receipt, seal) => Some((receipt, seal)),
                    };
                    if let Some(leader) = &mut self.leader { leader.confirmed = true; }
                    collection
                } else { None };
                let ReplicaBody::BlockUploaded(message) = &record.envelope.message else { return Err(invalid("Matching upload changed type")); };
                let loaded = self.pending_block.as_mut().and_then(|pending| pending.loaded.take());
                let pending = self.pending_block.as_ref().ok_or_else(|| invalid("Matching upload has no pending block"))?;
                let deferred = pending.is_deferred();
                let (catalog, table_blocks) = match loaded {
                    Some(loaded) => loaded,
                    None => self.blocks.load(pending.block_index()).await?,
                };
                crate::blocks::validate_boundary(&catalog, &pending.prepared, &pending.boundary)?;
                validate_uploaded_root(&catalog, message, deferred || self.logs.requires_checkpoint_digest())?;
                let checkpoint = PublishedCheckpoint {
                    block: catalog.block_index(), boundary: pending.boundary.metadata.position,
                    root_sha256: message.catalog_sha256.as_deref().and_then(|digest| digest.try_into().ok()),
                };
                let collect = checkpoint.root_sha256.is_some()
                    && catalog.as_proto().serving_checkpoint_object.is_some()
                    && matches_confirmation(&catalog, message, self.logs.storage_version, self.logs.storage_epoch);
                let tries = additions(&message.tries)?;
                let selected = self.verify_default_erasure_upload(&catalog, &pending.boundary, &record).await?;
                if let Some(selected) = selected {
                    let serving = self.serving.as_ref().ok_or_else(serving::unavailable)?;
                    self.partition.adopt_rewritten_block(pending.prepared.clone(), catalog, &table_blocks, record.metadata.timestamp)?;
                    serving.adopt_erasure_generation(selected.generation);
                    self.default_erasure = Some(DefaultErasure::Selected(selected));
                } else if deferred {
                    self.partition.adopt_sealed_block(pending.prepared.clone(), catalog, &table_blocks, &tries, record.metadata.timestamp)?;
                } else {
                    self.partition.adopt_block(pending.prepared.clone(), catalog, &table_blocks, &tries, record.metadata.timestamp)?;
                }
                self.record_applied(record.metadata.position)?;
                if !deferred {
                    self.progress.applied(message.latest_processed_msg_id.map(SourcePosition), None, message.external_source_token.clone())?;
                }
                let mut pending = self.pending_block.take().ok_or_else(|| invalid("Adopted block disappeared"))?;
                let sealed_serving = pending.deferred.take().and_then(|scope| scope.serving);
                let mut held = pending.into_records();
                held.append(&mut work);
                work = held;
                if let Some(leader) = &mut self.leader {
                    if !deferred || !leader.cycle.accepting() {
                        leader.cycle = BlockCycle::Filling { rows: self.partition.value().live().block_row_count() };
                        adopted = true;
                    }
                    complete_record(&mut leader.pending, record.envelope.term_sequence, Ok(Applied::Control));
                }
                if self.leader.is_some() && self.default_erasure.is_none() && !tries.is_empty() {
                    self.logs.append_source(&SourceMessage::Protobuf(SourceBody::TriesAdded(proto::TriesAdded {
                        storage_version: Some(self.logs.storage_version.0), storage_epoch: Some(self.logs.storage_epoch.0),
                        tries: message.tries.clone(), source_msg_id: None,
                    })), (self.clock)()).await?;
                }
                if collect && self.default_erasure.is_none()
                    && let Role::Leading(term) = self.role() && term == record.envelope.term
                    && let Ok(epoch) = u64::try_from(term.0)
                    && let Some((receipt, seal)) = collection && receipt == record.metadata
                {
                    let result = async {
                        let installed = self.partition.value();
                        let catalog = installed.table_catalog().block().ok_or_else(|| invalid("Adopted block disappeared"))?;
                        if let Some(capture) = &sealed_serving { capture.verify_prefix(catalog)?; }
                        else { self.serving.as_ref().ok_or_else(serving::unavailable)?.verify_checkpoint_prefix(catalog)?; }
                        self.logs.verify_checkpoint_reopen(catalog.latest_source_position(), checkpoint.boundary, receipt.position).await?;
                        self.blocks.collect_serving_checkpoints(epoch, checkpoint, &seal, &self.cancellation).await
                    }.await;
                    if let Err(error) = result && !self.cancellation.is_cancelled() {
                        tracing::warn!(%error, block = checkpoint.block.0, "Could not collect older serving checkpoint files");
                    }
                }
                self.publish_status(self.role());
                continue;
            }
            if let ReplicaBody::BlockUploaded(upload) = &record.envelope.message
                && let Some(installed) = self.partition.value().table_catalog().block()
                && matches_confirmation(installed, upload, self.logs.storage_version, self.logs.storage_epoch)
            {
                validate_uploaded_root(installed, upload, self.logs.requires_checkpoint_digest())?;
                self.observe_default_erasure_upload(&record)?;
            }
            if self.pending_block.is_some() && self.upload_for_installed_block(&record) {
                self.record_applied(record.metadata.position)?;
                self.complete_control(&record);
                continue;
            }
            if let Some(pending) = &mut self.pending_block && !pending.is_deferred() {
                pending.hold(record)?;
                continue;
            }
            if self.stale(&record) {
                self.record_applied(record.metadata.position)?;
                self.complete_control(&record);
                continue;
            }
            let _timer = self.replica_process_timer(&record.envelope.message);
            match &record.envelope.message {
                ReplicaBody::ResolvedTx(message) => {
                    let placed = self.check_placed_terminal(record.envelope.term, message)?;
                    let leading = self.leader.is_some();
                    let own = own_readback_term(self.leader.as_ref().map(|leader| leader.term.0), record.envelope.term.0);
                    if own {
                        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
                        let head = leader.resolver_mut()?.take_head();
                        let (pending, expected) = leader.pending_tx_at(record.envelope.term_sequence).ok_or_else(|| readback_mismatch(message.tx_id))?;
                        let pending = Arc::clone(pending);
                        let expected = Arc::clone(expected);
                        let applied: Result<_> = async {
                            let head = head.filter(|head| Arc::ptr_eq(head, &expected)).ok_or_else(|| readback_mismatch(message.tx_id))?;
                            if let Some(prepared) = &pending.interactive {
                                let _apply = rtdb_plan::ExecutionSample::new(self.resolve.execution_observer.clone(), rtdb_plan::ExecutionTimer::ReadBackApply);
                                let revision = prepared.revision.lock().take().ok_or_else(|| invalid("Interactive publication lost its prepared revision"))?;
                                let classic = prepared.classic.lock().take();
                                if let Some(serving) = &self.serving { serving.require_classic_effects(head.writes())?; }
                                self.partition.install_write(revision, None)?;
                                match (&mut self.serving, classic) {
                                    (Some(serving), Some(classic)) => serving.observe_prepared_classic(classic, head.writes(), record.metadata.position)?,
                                    (None, None) => (),
                                    _ => return Err(invalid("The prepared native observation lost its serving owner")),
                                }
                                if let Some(rejection) = &prepared.rejected { rejection.settle_prepaid(); }
                            } else {
                                let delta = confirmed_search_delta(pending.body.search_delta(), message.search_delta.as_ref());
                                self.apply_transaction(head.writes(), delta, record.metadata.position).await?;
                            }
                            if head.result().committed() {
                                self.observe_erasure(record.metadata.position, head.writes())?;
                                if let Some(operation) = head.db_op() { self.effects.apply_db_op(operation).await?; }
                            }
                            let result = Arc::new(head.result().clone());
                            self.record_applied(record.metadata.position)?;
                            self.progress.applied(Some(head.source_position()), Some(AppliedTransaction::new(Arc::clone(&result), head.placed_attempt())), head.external_token().map(<[u8]>::to_vec))?;
                            Ok(result)
                        }.await;
                        match transaction_completion(TransactionEvent::ApplyFinished(applied)) {
                            Some(Ok(result)) => {
                                if let Some(leader) = &mut self.leader {
                                    complete_record(&mut leader.pending, record.envelope.term_sequence, Ok(Applied::Transaction(result)));
                                }
                            }
                            Some(Err(error)) => {
                                pending.finish(Err(error.clone()));
                                return Err(error);
                            }
                            None => {}
                        }
                    } else {
                        if leading {
                            if let Some(operation) = &message.db_op { self.effects.apply_db_op(operation).await?; }
                        }
                        let value = self.partition.value();
                        let rtdb_tx::ReplayedTransaction { writes, result, placed_attempt } = rtdb_tx::decode_replica(message, value.live().latest_completed_tx(), value.live().block_index(), &self.resolve.memory)?;
                        self.apply_transaction(&writes, message.search_delta.clone().map(SearchDelta::Replayed), record.metadata.position).await?;
                        if result.committed() { self.observe_erasure(record.metadata.position, &writes)?; }
                        if !leading && message.committed.unwrap_or(false) {
                            if let Some(operation) = &message.db_op {
                                if let Err(error) = self.effects.apply_db_op(operation).await { if !error.is_caller_error() { return Err(error); } }
                            }
                        }
                        let source = SourcePosition(crate::replay_progress::replayed_source(
                            message.src_msg_id,
                            self.config.external_source,
                            self.progress.source_position().0,
                            result.tx_key().tx_id.0,
                        ));
                        self.record_applied(record.metadata.position)?;
                        self.progress.applied(Some(source), Some(AppliedTransaction::new(Arc::new(result), placed_attempt)), message.external_source_token.clone())?;
                    }
                    if placed {
                        let fenced = self.placed_slot.is_some_and(|slot| slot.term != record.envelope.term);
                        self.placed_slot = None;
                        if let Some(leader) = &mut self.leader {
                            leader.settling_placed = false;
                            if fenced {
                                // The predecessor's source may still be buffered in a
                                // same-process term change. Start beyond the ordered fence.
                                leader.source_head = None;
                                leader.source_batch.clear();
                                leader.source_poll = None;
                                leader.source_tail = Some(self.logs.source_replay(self.progress.current().source_position).await?);
                            }
                        }
                    }
                    if own && (placed || message.acceptance_stage.is_some())
                        && self.leader.is_some() && (self.block_needs_cut() || self.deferred_cut)
                    {
                        self.cut().await?;
                    }
                }
                ReplicaBody::TriesAdded(message) => {
                    if self.placed_slot.is_some() && message.source_msg_id.is_some() {
                        return Err(invalid("Source progress bypassed an open placed receiver slot"));
                    }
                    if self.compaction_replay_cutoff.is_none_or(|cutoff| record.metadata.position > cutoff)
                        && message.storage_version == Some(self.logs.storage_version.0) && message.storage_epoch.unwrap_or_default() == self.logs.storage_epoch.0
                    {
                        self.partition.add_tries(&additions(&message.tries)?, record.metadata.timestamp)?;
                    }
                    self.record_applied(record.metadata.position)?;
                    self.progress.applied(message.source_msg_id.map(SourcePosition), None, None)?;
                    self.complete_control(&record);
                }
                ReplicaBody::ArtefactsAdded(message) => {
                    if self.placed_slot.is_some() && message.source_msg_id.is_some() {
                        return Err(invalid("Source progress bypassed an open placed receiver slot"));
                    }
                    if message.storage_version == Some(self.logs.storage_version.0) && message.storage_epoch.unwrap_or_default() == self.logs.storage_epoch.0 {
                        if let Ok(table) = legacy_table_ref(message.table_name.as_deref().unwrap_or_default()) {
                            self.partition.add_artefacts_at(&table, artefact_entries(&message.entries), record.metadata.timestamp);
                        }
                    }
                    self.record_applied(record.metadata.position)?;
                    self.progress.applied(message.source_msg_id.map(SourcePosition), None, None)?;
                    self.complete_control(&record);
                }
                ReplicaBody::ArtefactsRemoved(message) => {
                    if self.placed_slot.is_some() && message.source_msg_id.is_some() {
                        return Err(invalid("Source progress bypassed an open placed receiver slot"));
                    }
                    if message.storage_version == Some(self.logs.storage_version.0) && message.storage_epoch.unwrap_or_default() == self.logs.storage_epoch.0 {
                        if let Ok(table) = legacy_table_ref(message.table_name.as_deref().unwrap_or_default()) {
                            let entries = artefact_entries(&message.entries);
                            let slug = self.partition.value().table_catalog().entry(&table).map(|entry| entry.slug.clone());
                            self.partition.remove_artefacts_at(&table, entries.clone(), Some(record.metadata.position));
                            // The lane deletes the objects from the store next; this node's cached copies are its own to drop.
                            let keys = slug.iter().flat_map(|slug| entries.iter().filter_map(move |(trie, record)| {
                                if record.kind() == rtdb_format::entity_directory::RECORD_KIND { return None; }
                                slug.recorded_artefact_key(trie, record).ok()
                            }));
                            let keys = keys.filter_map(|key| rtdb_storage::ObjectKey::new(key).ok()).collect::<BTreeSet<_>>();
                            self.forget_cached(keys).await;
                        }
                    }
                    self.record_applied(record.metadata.position)?;
                    self.progress.applied(message.source_msg_id.map(SourcePosition), None, None)?;
                    self.complete_control(&record);
                }
                ReplicaBody::BlockBoundary(message) => {
                    if self.placed_slot.is_some() {
                        return Err(invalid("A checkpoint boundary cannot abandon an open receiver slot"));
                    }
                    if self.pending_block.is_some() { return Err(invalid("A second checkpoint boundary crossed a deferred checkpoint")); }
                    let deferred = message.deferred_checkpoint == Some(true);
                    let prepared = if deferred {
                        self.partition.seal_block()?
                    } else {
                        self.partition.prepare_block()?
                    };
                    let mut pending = PendingBlock::new(ReplicaRecord::clone(&record), prepared, self.config.pending_block_records, self.config.pending_block_bytes)?;
                    if deferred {
                        pending.deferred = Some(crate::state::DeferredCheckpoint {
                            databases: self.effects.secondary_databases()?,
                            serving: self.serving.as_ref().map(NativeServing::capture_checkpoint),
                        });
                    }
                    self.pending_block = Some(pending);
                    self.record_applied(record.metadata.position)?;
                    if deferred {
                        self.progress.applied(message.latest_processed_msg_id.map(SourcePosition), None, message.external_source_token.clone())?;
                    }
                    if let Some(leader) = &mut self.leader {
                        leader.cycle = if deferred { BlockCycle::Filling { rows: 0 } } else { BlockCycle::Cut };
                    }
                    self.complete_control(&record);
                    if self.leader.is_some() { self.produce_pending()?; }
                }
                ReplicaBody::BlockUploaded(_) => return Err(invalid("Block upload has no matching pending boundary")),
                ReplicaBody::NoOp(message) => {
                    if self.apply_default_erasure_control(message, record.metadata.position).await? {
                        self.record_applied(record.metadata.position)?;
                        self.complete_control(&record);
                        continue;
                    }
                    if let Some(pin) = PinControl::decode(message)? {
                        if self.placed_slot.is_some() { return Err(invalid("A checkpoint pin cannot cross an open receiver slot")); }
                        let serving = self.serving.as_ref().ok_or_else(serving::unavailable)?;
                        pin.check_predecessor(serving.incarnation(), &self.logs, record.metadata.position, self.progress.source_position())?;
                        pin.apply(self.logs.store().as_ref(), record.metadata.position, record.envelope.term, &self.resolve.memory).await?;
                        self.record_applied(record.metadata.position)?;
                        self.complete_control(&record);
                        continue;
                    }
                    if let Some(release) = ReleaseControl::decode(message)? {
                        if self.placed_slot.is_some() { return Err(invalid("A checkpoint release cannot cross an open receiver slot")); }
                        let serving = self.serving.as_ref().ok_or_else(serving::unavailable)?;
                        release.check_predecessor(serving.incarnation(), record.metadata.position)?;
                        release.apply(self.logs.store().as_ref(), record.metadata.position, record.envelope.term).await?;
                        self.record_applied(record.metadata.position)?;
                        self.complete_control(&record);
                        continue;
                    }
                    let mut released_checkpoint = false;
                    if message.serving_record.is_some() {
                        if message.placed_slot_reservation.is_some() || self.placed_slot.is_some() {
                            return Err(invalid("A serving record occupies its own replica position"));
                        }
                        let own = self.leader.as_ref().is_some_and(|leader| leader.term == record.envelope.term);
                        if message.src_msg_id.is_some_and(|source| SourcePosition(source) <= self.progress.source_position()) {
                            return Err(invalid("A serving result did not advance the source prefix"));
                        }
                        let partition = &self.partition;
                        let memory = &self.resolve.memory;
                        let serving = self.serving.as_mut().ok_or_else(serving::unavailable)?;
                        let held = serving.holds_checkpoint();
                        let mut physical = None;
                        let mut first_erasure = None;
                        let mut captured_erasure = None;
                        let logs = &self.logs;
                        let ready = serving.apply_ordered(message, own, Some((record.metadata.position, record.envelope.term)), |source, receipt, prior, effects| {
                            let write = serving::classic::stage_source(partition.snapshot(), prior, source, receipt, effects, memory)?;
                            let key = write.tx_key();
                            let result = match &receipt.outcome {
                                ServingOutcome::Refused { error } => TransactionResult::Aborted { tx_key: key,
                                    error: Some(rtdb_tx::RetainedAnomaly::new(error.clone(), memory)?) },
                                _ => TransactionResult::Committed { tx_key: key },
                            };
                            first_erasure = first_write_erasure(record.metadata.position, &write)?;
                            if receipt.erasure == Some(Erasure::Pending) {
                                captured_erasure = Some(AppliedErasure::capture(ErasureCut {
                                    basis: receipt.basis, source, replica: record.metadata.position, term: record.envelope.term,
                                }, &write, logs)?);
                            }
                            partition.apply(&write)?;
                            physical = Some(Arc::new(result));
                            Ok(())
                        })?;
                        if self.track_catalog_cut && self.first_erasure.is_none() { self.first_erasure = first_erasure; }
                        if let Some(captured) = captured_erasure {
                            if self.default_erasure.is_some()
                                || serving.pending_erasure_installation() != Some(captured.cut.installation())
                            {
                                return Err(default_erasure::invalid());
                            }
                            self.default_erasure = Some(DefaultErasure::Applied(captured));
                        }
                        if let Some((source, reply)) = ready {
                            if let Some(result) = &physical && let Some(leader) = &mut self.leader {
                                leader.resolver_mut()?.observe_applied_serving(result.tx_key())?;
                            }
                            serving.source_replies.publish(source, reply);
                            self.progress.applied(Some(source), physical.map(|result| AppliedTransaction::new(result, None)), None)?;
                        }
                        released_checkpoint = held && !serving.holds_checkpoint();
                        if released_checkpoint && self.default_erasure.is_none() {
                            let source = self.progress.source_position();
                            let resume = !self.waiting_for_checkpoint();
                            if let Some(leader) = &mut self.leader && leader.source_tail.is_none()
                                && leader.source_poll.is_none() && leader.source_head.is_none() && leader.source_batch.is_empty()
                            {
                                leader.resolver_mut()?.advance_source(source)?;
                                if resume { leader.source_tail = Some(self.logs.source_replay(source).await?); }
                            }
                        }
                    } else if let Some(wire) = message.placed_slot_reservation.as_deref() {
                        if message.src_msg_id.is_some() || self.placed_slot.is_some() {
                            return Err(invalid("Placed receiver reservations must occupy a fresh control position"));
                        }
                        let reservation = PlacedReservation::decode(wire)?;
                        if reservation.attempt.position() <= self.progress.current().source_position
                            || (!self.logs.is_copied_replica_position(record.metadata.position)
                                && self.resolve.owned_witness_scope.is_some_and(|scope| !scope.same_history(reservation.attempt.scope())))
                        {
                            return Err(invalid("Placed receiver reservation has a foreign history or source position"));
                        }
                        if let Some(leader) = &self.leader
                            && leader.term == record.envelope.term
                            && leader.awaiting_placed.as_ref().and_then(|source| source.placement) != Some(SourcePlacement::Reserved(reservation))
                        {
                            return Err(invalid("Read-back changed the writer's original placed reservation"));
                        }
                        self.progress.reserved(reservation)?;
                        self.placed_slot = Some(OrderedPlaced {
                            reservation, term: record.envelope.term, reserved_at: record.metadata.position,
                        });
                    } else {
                        if self.placed_slot.is_some() && message.src_msg_id.is_some() {
                            return Err(invalid("Source progress bypassed an open placed receiver slot"));
                        }
                        self.progress.applied(message.src_msg_id.map(SourcePosition), None, None)?;
                    }
                    self.record_applied(record.metadata.position)?;
                    self.complete_control(&record);
                    if released_checkpoint && self.deferred_cut && self.leader.as_ref().is_some_and(|leader| leader.cycle.accepting()) {
                        self.cut().await?;
                    }
                }
                ReplicaBody::TriesDeleted(message) => {
                    let position = record.metadata.position;
                    if self.compaction_replay_cutoff.is_none_or(|cutoff| record.metadata.position > cutoff) {
                        let table = legacy_table_ref(message.table_name.as_deref().unwrap_or_default())?;
                        let keys = message.trie_keys.iter().map(|key| key.parse()).collect::<Result<BTreeSet<_>>>()?;
                        let cached = cached_objects(&self.partition.value(), &table, &keys);
                        let deletions = BTreeMap::from([(table, keys)]);
                        self.partition.delete_tries(&deletions);
                        // Every node holds its own cached copies, and the leader's delete reached only the remote store.
                        self.forget_cached(cached).await;
                        if let Some(leader) = &mut self.leader && leader.term == record.envelope.term
                            && let Some(sequence) = record.envelope.term_sequence
                            && let Some(write) = leader.pending.iter().find(|write| write.sequence() == Some(sequence))
                        {
                            if matches!(&write.body, WriteBody::Message(ReplicaBody::TriesDeleted(expected)) if expected == message) {
                                leader.trie_removals.push(AppliedTrieRemoval { sequence, position: record.metadata.position, deletions, _record: record });
                            } else {
                                complete_record(&mut leader.pending, Some(sequence), Err(trie_removal_unconfirmed()));
                            }
                        }
                    } else if let Some(leader) = &mut self.leader && leader.term == record.envelope.term {
                        complete_record(&mut leader.pending, record.envelope.term_sequence, Err(trie_removal_unconfirmed()));
                    }
                    self.record_applied(position)?;
                }
                ReplicaBody::TablesRetired(message) => {
                    let tables = message.table_names.iter().map(|name| legacy_table_ref(name)).collect::<Result<Vec<_>>>()?;
                    self.partition.retire_tables(tables, Timestamp(message.as_of_micros.unwrap_or_default()));
                    self.record_applied(record.metadata.position)?;
                    self.complete_control(&record);
                }
                ReplicaBody::OversizedMessage(_) => return Err(invalid("Unresolved oversized body reached apply")),
            }
        }
        if adopted {
            let progress = self.progress.current();
            let waiting_for_checkpoint = self.waiting_for_checkpoint();
            if let Some(leader) = &mut self.leader {
                if leader.resolver()?.staged_rows()? != 0 { return Err(invalid("A block adoption retained unresolved transaction writes")); }
                let mut next = TxResolver::new(progress.tx_key, progress.source_position, progress.external_token.as_deref().map(<[u8]>::to_vec), &self.resolve.memory)?;
                next.preserve_receipt_clock(leader.resolver()?);
                leader.resolver = Some(next);
                if !waiting_for_checkpoint && self.default_erasure.is_none() && self.pending_block.is_none() && leader.source_tail.is_none() && leader.source_poll.is_none() {
                    leader.source_tail = Some(self.logs.source_replay(progress.source_position).await?);
                }
            }
        }
        Ok(())
    }
    fn upload_for_installed_block(&self, record: &ReplicaRecord) -> bool {
        let ReplicaBody::BlockUploaded(upload) = &record.envelope.message else { return false; };
        upload.storage_version == Some(self.logs.storage_version.0)
            && upload.storage_epoch.unwrap_or_default() == self.logs.storage_epoch.0
            && upload.block_index.zip(self.partition.value().table_catalog().block_index())
                .is_some_and(|(uploaded, installed)| uploaded <= installed.0)
    }

    fn stale(&self, record: &ReplicaRecord) -> bool {
        let progress = self.progress.current();
        let block = self.partition.value().table_catalog().block_index().map_or(-1, |index| index.0);
        stale_replica_body(&record.envelope.message, progress.tx_id().0, progress.source_position.0, block,
            self.leader.as_ref().is_some_and(|leader| leader.term == record.envelope.term))
    }
    fn complete_control(&mut self, record: &ReplicaRecord) {
        if let Some(leader) = &mut self.leader {
            if leader.term == record.envelope.term { complete_record(&mut leader.pending, record.envelope.term_sequence, Ok(Applied::Control)); }
        }
    }
    fn produce_pending(&mut self) -> Result<()> {
        let pending = self.pending_block.as_ref().ok_or_else(|| invalid("Block production has no retained boundary"))?;
        let prepared = pending.prepared.clone();
        let boundary = pending.boundary.clone();
        let deferred = pending.is_deferred();
        let databases = match &pending.deferred {
            Some(scope) => scope.databases.clone(),
            None => self.effects.secondary_databases()?,
        };
        let capture = match &pending.deferred {
            Some(scope) => scope.serving.clone(),
            None => self.serving.as_ref().map(NativeServing::capture_checkpoint),
        };
        let blocks = Arc::clone(&self.blocks);
        let search = self.serving_search.clone();
        let leader = self.leader.as_mut().ok_or_else(|| invalid("Follower attempted block production"))?;
        if leader.production.is_some() { return Err(invalid("Block production is already running")); }
        let rewrite = match &self.default_erasure {
            Some(DefaultErasure::Applied(captured)) => Some(captured.clone()),
            Some(_) => return Err(default_erasure::invalid()),
            None => None,
        };
        let term = rewrite.as_ref().map_or(leader.term, |_| boundary.envelope.term);
        let serving_checkpoint = match (&mut self.serving, capture) {
            (Some(serving), Some(capture)) => Some(serving.prepare_checkpoint(term, capture)?),
            (None, None) => None,
            _ => return Err(invalid("Deferred checkpoint changed its serving state")),
        };
        let collect = self.collect_serving_checkpoints && rewrite.is_none();
        let logs = Arc::clone(&self.logs);
        let cancellation = leader.cancellation.clone();
        let produce = async move {
            let serving_checkpoint = serving_checkpoint.map(serving::ServingCheckpointBuilder::build).transpose()?;
            let seal = if collect { serving_checkpoint.as_ref().and_then(serving::ServingCheckpoint::seal_for_collection) } else { None };
            if let Some(rewrite) = rewrite {
                if !databases.is_empty() { return Err(default_erasure::unsupported()); }
                let block = rewrite.produce(&blocks, &logs, &prepared, &boundary,
                    serving_checkpoint.ok_or_else(default_erasure::unsupported)?, &cancellation).await?;
                return Ok(CheckpointOutput { block, loaded: None, seal: None });
            }
            let block = blocks.produce_with_search(&prepared, &boundary, databases, &cancellation, search.as_deref(), serving_checkpoint).await?;
            let loaded = if deferred {
                let loaded = blocks.load(prepared.block_index()).await?;
                if checkpoint_digest(&loaded.0)? != checkpoint_digest(&block.catalog)? {
                    return Err(invalid("Deferred checkpoint changed before read-back"));
                }
                Some(loaded)
            } else { None };
            Ok(CheckpointOutput { block, loaded, seal })
        };
        let future: BoxFuture<'static, Result<CheckpointOutput>> = if deferred {
            let job = AbortOnDropHandle::new(tokio::spawn(produce));
            Box::pin(async move { job.await.map_err(join_error)? })
        } else {
            Box::pin(produce)
        };
        leader.production = Some(Production::Writing(std::time::Instant::now(), future));
        Ok(())
    }
    async fn production_finished(&mut self, result: ProductionResult) -> Result<()> {
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        leader.production = None;
        let error = match &result { ProductionResult::Written(_, result) => result.as_ref().err(), ProductionResult::Appended(_, result, _) => result.as_ref().err() };
        if let Some(error) = error {
            let error = error.clone();
            if leader.confirmed { return Err(error); }
            return self.promotion_failed(error).await;
        }
        match result {
            ProductionResult::Written(started, result) => {
                let CheckpointOutput { block: produced, loaded, seal } = result?;
                let upload_time = (self.clock)().0.div_euclid(1_000_000);
                self.status.send_modify(|status| status.last_block_upload_epoch_seconds = upload_time);
                let pending = self.pending_block.as_mut().ok_or_else(|| invalid("Produced block has no retained boundary"))?;
                pending.loaded = loaded;
                let ReplicaBody::BlockBoundary(boundary) = &pending.boundary.envelope.message else { return Err(invalid("Pending boundary changed type")); };
                let message = ReplicaBody::BlockUploaded(proto::BlockUploaded {
                    storage_version: Some(self.logs.storage_version.0), storage_epoch: Some(self.logs.storage_epoch.0), block_index: Some(pending.block_index().0),
                    latest_processed_msg_id: boundary.latest_processed_msg_id, external_source_token: boundary.external_source_token.clone(),
                    catalog_sha256: Some(checkpoint_digest(&produced.catalog)?.to_vec()),
                    tries: produced.tries.iter().flat_map(|(table, tries)| tries.iter().map(|trie| trie.to_proto(table))).collect(),
                    naming_epoch: crate::blocks::uploaded_naming_epoch(&produced.catalog),
                });
                let write = PendingWrite::new(WriteBody::Message(message), pending.block_index(), None, &self.resolve.memory)?;
                leader.pending.push_back(Arc::clone(&write));
                let appender = leader.appender.clone();
                leader.production = Some(Production::Appending(started, Box::pin(async move { appender.append_durable(write).await }), seal));
            }
            ProductionResult::Appended(started, result, seal) => {
                let receipt = result?;
                leader.production = seal.map(|seal| Production::AwaitingReadback(receipt, seal));
                leader.confirmed = true;
                self.time_block_upload(started);
                self.publish_status(self.role());
            }
        }
        Ok(())
    }
    async fn resolution_finished(&mut self, resolver: TxResolver, result: Result<Arc<ResolvedTx>>) -> Result<()> {
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        let mut resolution = leader.resolution.take().ok_or_else(|| invalid("Resolved transaction has no in-flight owner"))?;
        let external = resolution.completion.0.is_some();
        leader.resolver = Some(resolver);
        match result {
            Ok(resolved) => {
                let interactive = resolution.interactive.take().map(|slot| {
                    Arc::try_unwrap(slot).ok().and_then(OnceLock::into_inner)
                        .ok_or_else(|| invalid("Interactive resolution lost its publication owner"))
                }).transpose()?;
                self.enqueue_resolved_with_preparation(resolved, resolution.completion.0.take(), resolution.direct_admission, interactive).await?
            }
            Err(error) => {
                if let Some(sender) = resolution.completion.0.take() { let _ = sender.send(Err(error.clone())); }
                if !external || !error.is_caller_error() { return Err(error); }
            }
        }
        // Records read back meanwhile stay held while the next record of the batch resolves.
        self.continue_source_batch().await?;
        let Some(leader) = self.leader.as_mut() else { return Ok(()); };
        if leader.resolution.is_some() { return Ok(()); }
        let held = std::mem::take(&mut leader.held);
        leader.held_bytes = 0;
        for record in held { self.apply_admitted(record).await?; }
        let previous = self.installed;
        self.update_installed_prefix()?;
        if self.installed != previous { self.publish_status(self.role()); }
        Ok(())
    }
    async fn resolve_next_source(&mut self) -> Result<()> {
        let deferred_checkpoint = self.has_deferred_checkpoint();
        let checkpoint_pending = self.pending_block.is_some();
        let serving_pending = self.serving.as_ref().is_some_and(NativeServing::main_write_pending);
        let permits_interactive = self.prepares_local_interactive();
        let Some(leader) = &mut self.leader else { return Ok(()); };
        let deferred = if leader.waiting_strict_source.is_some() {
            if !leader.ready_source() { return Ok(()); }
            let source = leader.waiting_strict_source.take().expect("checked for a waiting strict source");
            match source {
                StrictSource::Physical { record, decode } => Some((record, Some(decode))),
                StrictSource::DeferredFlush(position) => {
                    leader.resolver_mut()?.advance_source(position)?;
                    return self.cut().await;
                }
                StrictSource::Transaction(source) | StrictSource::Interactive(source) => return self.admit_source(source),
                StrictSource::Serving { position, request } => return self.admit_serving_source(position, Ok(request)).await,
                StrictSource::Compaction { position, log_time, message, _decode_charge } =>
                    return self.admit_fact_compaction(position, log_time, message).await,
            }
        } else { None };
        let (record, decode_reservation) = if let Some(deferred) = deferred { deferred }
        else if let Some(head) = leader.source_head {
            let tail = leader.source_tail.as_mut().ok_or_else(|| invalid("Prepared source head has no tail"))?;
            let local = if self.logs.uses_volatile_intake() {
                tail.replay().ok_or_else(|| invalid("Volatile intake lost its prepared reader"))?.read_local_attachment(head)?
            } else { None };
            let record = if let Some(local) = local {
                (local, None)
            } else {
                let Some(record) = read_replay_head(tail, head, &self.resolve.memory, source_decode_bound, &self.cancellation).await? else { return Ok(()); };
                (record.raw, Some(record.decode_reservation))
            };
            leader.source_head = None;
            record
        } else {
            let Some(record) = leader.source_batch.pop_front() else { return Ok(()); };
            (record, None)
        };
        let metadata = record.metadata;
        let position = self.logs.source_position(metadata)?;
        if let Some(attachment) = record.local_attachment.as_ref() {
            if record.payload.as_ref() != crate::logs::LOCAL_INTERACTIVE_MARKER
                || !permits_interactive
            {
                return Err(invalid("An interactive attachment requires the standalone single journal"));
            }
            let request = attachment.get::<SourceTransaction>().and_then(SourceTransaction::clone_local_interactive)
                .ok_or_else(|| invalid("An interactive attachment has no staged transaction"))?;
            if position <= leader.resolver()?.source_position() { return Err(invalid("Source reader did not advance")); }
            let work = SourceWork::Resolve(request, None);
            let source = SourceInput { position, log_time: metadata.timestamp, work, placement: None, _decode_charge: None };
            if checkpoint_pending || serving_pending || !leader.drained_for_strict_source() {
                leader.waiting_strict_source = Some(StrictSource::Interactive(source));
                return Ok(());
            }
            return self.admit_source(source);
        }
        if record.payload.as_ref() == crate::logs::LOCAL_INTERACTIVE_MARKER {
            return Err(invalid("An interactive intake marker lost its local attachment"));
        }
        let decode_charge = match decode_reservation {
            Some(reservation) => reservation,
            None => self.resolve.memory.reserve(source_decode_bound(record.payload.len())?)?,
        };
        let message = SourceMessage::decode_bytes(record.payload.clone())?;
        if self.serving.as_ref().is_some_and(NativeServing::main_write_pending)
            && matches!(&message, Some(SourceMessage::LegacyTransaction(_) | SourceMessage::Protobuf(
                SourceBody::Tx(_) | SourceBody::AttachDatabase(_) | SourceBody::DetachDatabase(_)
            )))
        {
            leader.waiting_strict_source = Some(StrictSource::Physical { record, decode: decode_charge });
            return Ok(());
        }
        let payload = record.payload;
        let durable = self.partition.snapshot();
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        if position <= leader.resolver()?.source_position() { return Err(invalid("Source reader did not advance")); }
        let block = durable.block_index();
        let resolved = match message {
            Some(message @ (SourceMessage::LegacyTransaction(_) | SourceMessage::Protobuf(SourceBody::Tx(_)))) => {
                if self.config.external_source { return Err(Anomaly::incorrect("xtdb/external-source-transaction", "Client transactions are disabled for an external-source database")); }
                let direct = SourceTransaction::direct_placed_message(&message)?;
                let work = if self.config.skipped_transactions.contains(&position) {
                    SourceWork::Skip(message)
                } else {
                    let operations = match &message {
                        SourceMessage::Protobuf(SourceBody::Tx(tx)) => tx.tx_ops.clone(),
                        SourceMessage::LegacyTransaction(bytes) => Some(bytes.clone()),
                        _ => None,
                    };
                    SourceWork::Resolve(SourceTransaction::decode(&message, &self.resolve.memory)?, operations)
                };
                let original = match &work {
                    SourceWork::Resolve(request, _) => request.placed_attempt(position)?,
                    SourceWork::Skip(message) => SourceTransaction::skipped_attempt(message, position)?,
                    SourceWork::Fence => unreachable!("only recovery constructs a placed fence"),
                };
                let placement = original.map(|attempt| {
                    let admission = PlacedReservation::new(attempt, metadata.timestamp, &payload);
                    if direct { SourcePlacement::Direct(admission) } else { SourcePlacement::Reserved(admission) }
                });
                let needs_reservation = matches!(&placement, Some(SourcePlacement::Reserved(_)));
                let source = SourceInput { position, log_time: metadata.timestamp, work, placement, _decode_charge: Some(decode_charge) };
                // Keep this position as the source head until every previously queued replica
                // write has been read back. In particular, none can still be offloading a body.
                if (needs_reservation || matches!(&source.work, SourceWork::Resolve(request, _) if request.requires_durable_predecessor()))
                    && !leader.drained_for_strict_source()
                {
                    leader.waiting_strict_source = Some(StrictSource::Transaction(source));
                } else {
                    self.admit_source(source)?;
                }
                None
            }
            Some(SourceMessage::Protobuf(SourceBody::ServingTransact(bytes))) => {
                if self.config.external_source {
                    return Err(invalid("External-source database received a serving transaction"));
                }
                let serving = self.serving.as_ref().ok_or_else(serving::unavailable)?;
                let mut request = serving.source_authority.open(&bytes).await?;
                if let Ok(request) = &mut request { request.transaction.request_memory = Some(decode_charge); }
                return self.admit_serving_source(position, request).await;
            }
            Some(SourceMessage::Protobuf(SourceBody::AttachDatabase(message))) => {
                if self.config.external_source { return Err(invalid("External-source database received attach operation")); }
                let operation = DbOp::AttachDatabase(message);
                let verdict = check_db_op(self.effects.as_ref(), leader.resolver()?, &operation);
                Some(leader.resolver_mut()?.resolve_db_op(durable, position, metadata.timestamp, operation, verdict, &self.resolve).await?)
            }
            Some(SourceMessage::Protobuf(SourceBody::DetachDatabase(message))) => {
                if self.config.external_source { return Err(invalid("External-source database received detach operation")); }
                let operation = DbOp::DetachDatabase(message);
                let verdict = check_db_op(self.effects.as_ref(), leader.resolver()?, &operation);
                Some(leader.resolver_mut()?.resolve_db_op(durable, position, metadata.timestamp, operation, verdict, &self.resolve).await?)
            }
            Some(SourceMessage::Protobuf(SourceBody::TriesAdded(mut message))) => {
                if crate::fact_history::needs_census(&message) {
                    if !leader.drained_for_strict_source() {
                        leader.waiting_strict_source = Some(StrictSource::Compaction { position, log_time: metadata.timestamp, message, _decode_charge: decode_charge });
                        return Ok(());
                    }
                    return self.admit_fact_compaction(position, metadata.timestamp, message).await;
                }
                message.source_msg_id = Some(position.0);
                leader.enqueue(WriteBody::Message(ReplicaBody::TriesAdded(message)), block, None, &self.resolve.memory)?;
                leader.resolver_mut()?.advance_source(position)?;
                None
            }
            Some(SourceMessage::Protobuf(SourceBody::ArtefactsAdded(mut message))) => {
                let store = self.resolve.storage.object_store();
                let rejected = accept_artefacts(&mut message, &durable, self.logs.storage_version.0, self.logs.storage_epoch.0,
                    store.requires_search_publication());
                let mut message = admit_search_intents(message, &durable, store.as_ref(), &self.cancellation).await?;
                message.source_msg_id = Some(position.0);
                leader.enqueue(WriteBody::Message(ReplicaBody::ArtefactsAdded(message)), block, None, &self.resolve.memory)?;
                leader.resolver_mut()?.advance_source(position)?;
                // Readers treat a missing artefact as absent, so these deletes are best effort, and must not hold up resolution.
                if !rejected.is_empty() {
                    let store = Arc::clone(self.resolve.storage.object_store());
                    tokio::spawn(async move { for key in rejected { let _ = store.delete(&key).await; } });
                }
                None
            }
            Some(SourceMessage::Protobuf(SourceBody::ArtefactsRemoved(mut message))) => {
                if message.storage_version != Some(self.logs.storage_version.0) || message.storage_epoch.unwrap_or_default() != self.logs.storage_epoch.0 { message.entries.clear(); }
                message.source_msg_id = Some(position.0);
                leader.enqueue(WriteBody::Message(ReplicaBody::ArtefactsRemoved(message)), block, None, &self.resolve.memory)?;
                leader.resolver_mut()?.advance_source(position)?;
                None
            }
            Some(SourceMessage::Protobuf(SourceBody::FlushBlock(message))) => {
                let expected = self.partition.value().table_catalog().block_index().map_or(-1, |index| index.0);
                if deferred_checkpoint && message.expected_block_idx == Some(expected) {
                    leader.waiting_strict_source = Some(StrictSource::DeferredFlush(position));
                    return Ok(());
                }
                leader.resolver_mut()?.advance_source(position)?;
                if message.expected_block_idx == Some(expected) && self.pending_block.is_none() { self.cut().await?; }
                else { leader.enqueue(WriteBody::Message(ReplicaBody::NoOp(proto::NoOp { src_msg_id: Some(position.0), ..Default::default() })), block, None, &self.resolve.memory)?; }
                None
            }
            None => {
                leader.resolver_mut()?.advance_source(position)?;
                leader.enqueue(WriteBody::Message(ReplicaBody::NoOp(proto::NoOp { src_msg_id: Some(position.0), ..Default::default() })), block, None, &self.resolve.memory)?;
                None
            }
        };
        if let Some(resolved) = resolved { self.enqueue_resolved(resolved, None, None).await?; }
        Ok(())
    }

    async fn admit_serving_source(&mut self, position: SourcePosition, request: Result<SourceRequest>) -> Result<()> {
        let request = match request {
            Ok(request) if request.transaction.uses_erasure() && !self.effects.secondary_databases()?.is_empty() =>
                Err(default_erasure::unsupported()),
            request => request,
        };
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        let serving = self.serving.as_ref().ok_or_else(serving::unavailable)?;
        let request = match request {
            Ok(request) if serving.request_pending(&request)
                || (request.transaction.uses_erasure() || request.funding.is_some()) && !leader.drained_for_strict_source()
                || leader.pending.iter().any(|write| matches!(&write.body, WriteBody::Resolved(..))) =>
            {
                leader.waiting_strict_source = Some(StrictSource::Serving { position, request });
                return Ok(());
            }
            request => request,
        };
        let request = match request {
            Ok(request) if self.logs.default_erasure_kafka_topics().is_some() && serving.needs_erasure_history(&request) => {
                self.cancellation.run_until_cancelled(self.logs.validate_default_erasure_kafka_history(position, self.read_position))
                    .await.ok_or_else(stopped)?
                    .map(|()| request)
            }
            request => request,
        };
        let mut request = request;
        if let Ok(candidate) = &mut request
            && candidate.funding.is_some()
        {
            let clock = self.resolve.decision_clock.as_ref().filter(|clock| clock.supports_funded_offers());
            if let Some(clock) = clock.filter(|_| self.logs.replica_timed_original_append_limit().is_some()) {
                self.cancellation.run_until_cancelled(candidate.verify_funding(serving, clock.as_ref()))
                    .await.ok_or_else(stopped)?;
            } else {
                request = Err(rtdb_tx::transact::funding_unsupported());
            }
        }
        let floor = request.as_ref().ok().and_then(|request| {
            request.transaction.store_append().and_then(|_| serving.last_store_reading())
                .into_iter().chain(request.transaction.store_writer_reading_floor()).max()
        });
        let now = self.cancellation.run_until_cancelled(rtdb_tx::transact::sample_store_clock(
            &*self.clock, floor, SERVING_CLOCK_UNCERTAINTY_MICROS,
        )).await.ok_or_else(stopped)?;
        let serving = self.serving.as_mut().ok_or_else(serving::unavailable)?;
        let message = serving.decide_source(position, now, request)?;
        let block = self.partition.value().live().block_index();
        leader.cycle.resolved(1)?;
        leader.enqueue(WriteBody::Message(ReplicaBody::NoOp(message)), block, None, &self.resolve.memory)?;
        leader.resolver_mut()?.advance_source(position)?;
        if self.block_needs_cut() { self.cut().await?; }
        Ok(())
    }

    async fn admit_fact_compaction(&mut self, position: SourcePosition, log_time: Timestamp, mut message: proto::TriesAdded) -> Result<()> {
        let snapshot = self.partition.snapshot();
        let block = snapshot.block_index();
        let admitted = async {
            if message.storage_version != Some(self.logs.storage_version.0) || message.storage_epoch.unwrap_or_default() != self.logs.storage_epoch.0 {
                return Err(crate::fact_history::uncovered("A compaction belongs to another storage epoch"));
            }
            crate::fact_history::admit(self.blocks.object_store(), snapshot, &additions(&message.tries)?,
                log_time, &self.resolve.memory, &self.cancellation).await
        };
        let accepted = self.cancellation.run_until_cancelled(admitted).await.ok_or_else(stopped)?;
        let body = match accepted {
            Ok(()) => {
                message.source_msg_id = Some(position.0);
                ReplicaBody::TriesAdded(message)
            }
            Err(error) => {
                tracing::warn!("FactStore compaction retained the previous history: {}", error.code);
                ReplicaBody::NoOp(proto::NoOp { src_msg_id: Some(position.0), ..Default::default() })
            }
        };
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        leader.enqueue(WriteBody::Message(body), block, None, &self.resolve.memory)?;
        leader.resolver_mut()?.advance_source(position)?;
        Ok(())
    }

    fn admit_source(&mut self, source: SourceInput) -> Result<()> {
        let Some(SourcePlacement::Reserved(reservation)) = source.placement else {
            if let Some(SourcePlacement::Direct(admission)) = source.placement
                && self.resolve.owned_witness_scope != Some(admission.attempt.scope())
            {
                return Err(invalid("The direct placed attempt belongs to another receiver"));
            }
            return self.begin_source_resolution(source);
        };
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        if self.placed_slot.is_some() || leader.awaiting_placed.is_some()
            || self.resolve.owned_witness_scope != Some(reservation.attempt.scope())
        {
            return Err(invalid("Cannot reserve a placed attempt for another receiver"));
        }
        let record = ReplicaBody::NoOp(proto::NoOp {
            src_msg_id: None,
            placed_slot_reservation: Some(reservation.encode()),
            ..Default::default()
        });
        leader.enqueue(WriteBody::Message(record), self.partition.value().live().block_index(), None, &self.resolve.memory)?;
        leader.awaiting_placed = Some(source);
        Ok(())
    }

    fn drive_placed_slot(&mut self) -> Result<()> {
        let Some(slot) = self.placed_slot else { return Ok(()); };
        let Some(leader) = &mut self.leader else { return Ok(()); };
        if !leader.confirmed || !leader.activation.is_active() || leader.settling_placed || leader.resolution.is_some()
            || !leader.drained_for_strict_source() || self.pending_block.is_some()
        {
            return Ok(());
        }
        let source = if slot.term == leader.term {
            leader.awaiting_placed.take()
                .filter(|source| source.placement == Some(SourcePlacement::Reserved(slot.reservation)))
                .ok_or_else(|| invalid("The installed reservation differs from the writer's original input"))?
        } else {
            if slot.term >= leader.term || leader.awaiting_placed.is_some() {
                return Err(invalid("Only a successor of the installed receiver can close its lost input"));
            }
            SourceInput {
                position: slot.reservation.attempt.position(),
                log_time: slot.reservation.submitted_at,
                work: SourceWork::Fence,
                placement: Some(SourcePlacement::Reserved(slot.reservation)),
                _decode_charge: Some(self.resolve.memory.reserve(1024)?),
            }
        };
        leader.settling_placed = true;
        self.begin_source_resolution(source)
    }

    fn check_placed_terminal(&self, term: Term, message: &proto::ResolvedTx) -> Result<bool> {
        if message.checked_direct_placed_admission()?.is_some() {
            return if self.placed_slot.is_none() { Ok(false) } else {
                Err(invalid("A direct placed commit cannot cross an open receiver slot"))
            };
        }
        let terminal = message.placed_slot_terminal.as_deref().map(PlacedTerminal::decode).transpose()?;
        let attempt = message.placed_attempt.as_deref().map(rtdb_format::envelope::decode_replica_outcome_attempt)
            .transpose()?.map(|(attempt, _)| attempt);
        match (self.placed_slot, terminal, attempt) {
            (None, None, None) => Ok(false),
            (Some(slot), Some(terminal), Some(attempt))
                if slot.reservation.attempt == attempt && slot.reservation.commitment == terminal.commitment
                    && message.tx_id == Some(attempt.position().0) && message.src_msg_id == message.tx_id
                    && message.committed.is_some()
                    && ((!terminal.fenced && slot.term == term)
                        || (terminal.fenced && slot.term < term && message.committed == Some(false))) => Ok(true),
            _ => Err(invalid("Placed receiver decision has no exact original reservation")),
        }
    }

    fn begin_source_resolution(&mut self, source: SourceInput) -> Result<()> {
        let durable = self.partition.snapshot();
        let classic_authority = self.serving.as_ref().map(|serving| serving.classic_write_authority().clone());
        let classic_shares_main = self.serving.as_ref().map(NativeServing::shares_main);
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        let mut resolver = leader.resolver.take().ok_or_else(|| invalid("Source resolution has no idle resolver"))?;
        let context = Arc::clone(&self.resolve);
        let clock = Arc::clone(&self.clock);
        let maximum_inline_bytes = self.logs.replica_guarded_append_limit();
        let maximum_timed_original_bytes = self.logs.replica_timed_original_append_limit();
        let node_id = self.config.node_id.clone();
        let partition = Arc::clone(&self.partition);
        let direct_admission = match source.placement { Some(SourcePlacement::Direct(admission)) => Some(admission), _ => None };
        let logs = Arc::clone(&self.logs);
        let skipped = self.config.skipped_transactions.contains(&source.position);
        let interactive = matches!(&source.work, SourceWork::Resolve(request, _) if request.local_interactive_rejection_budget().is_some())
            .then(|| Arc::new(OnceLock::new()));
        let publication = interactive.clone();
        leader.resolution = Some(Resolution { completion: Completion(None), direct_admission, interactive, future: Box::pin(async move {
            let SourceInput { position, log_time, work, placement, _decode_charge } = source;
            let _resolution = rtdb_plan::ExecutionSample::new(context.execution_observer.clone(), rtdb_plan::ExecutionTimer::ResolveSource);
            let result = match work {
                SourceWork::Resolve(request, _) if publication.is_some() => {
                    let result = resolver.resolve_local_interactive(durable, position, log_time, request, &context, classic_authority.as_ref(), skipped,
                        |tx, memory| {
                            let revision = partition.prepare_write(tx.writes())?;
                            let body = tx.to_replica_body(memory)?;
                            let io = logs.prepay_interactive_replica(body.body(), memory)?;
                            let classic = classic_shares_main.map(|shared| NativeServing::prepare_classic_observation(shared, tx.writes(), memory));
                            Ok(PreparedInteractivePublication { revision: parking_lot::Mutex::new(Some(revision)),
                                classic: parking_lot::Mutex::new(classic), body, io, rejected: None })
                        }).await;
                    result.and_then(|(resolved, mut prepared, rejected)| {
                        prepared.rejected = rejected;
                        publication.expect("matched interactive resolution").set(prepared)
                            .map_err(|_| invalid("Interactive publication was prepared twice"))?;
                        Ok(resolved)
                    })
                }
                SourceWork::Resolve(mut request, operations) => {
                    let prepared = async {
                        if request.proposal_generation_reading()?.is_some() {
                            let key = resolver.prepared_publication_key(position, log_time)?;
                            request.prepare_proposal_generation(|| durable.clone(), key, log_time, &context).await?;
                        }
                        Ok::<_, Anomaly>(())
                    }.await;
                    match prepared {
                        Ok(_) => {
                            let result = resolver.resolve_source_with_authority(durable, position, log_time, request, &context,
                                Some(WriterPublication { clock: clock.as_ref(), maximum_inline_bytes, maximum_timed_original_bytes }), classic_authority.as_ref()).await;
                            if let Err(failure) = &result && crate::crash_log::applies(&failure.error) {
                                let value = partition.value();
                                let site = failure.operation.as_ref().map(|operation| crate::crash_log::CrashSite {
                                    table: operation.table.clone(),
                                    message: operation.message,
                                    open_tx: operation.open_tx.as_deref(),
                                    query_rel: None,
                                });
                                let slices = crate::crash_log::Slices { tx_ops: operations.as_ref(), live: value.live(), site };
                                crate::crash_log::write(&**context.storage.object_store(), &node_id, clock(), &failure.error, slices, &context.memory).await;
                            }
                            result.map_err(Anomaly::from)
                        }
                        Err(error) if error.is_caller_error() => resolver.resolve_source_abort(
                            durable, position, log_time, placement.as_ref().map(SourcePlacement::attempt), &context, error, false,
                        ).await,
                        Err(error) => Err(error),
                    }
                }
                SourceWork::Fence => {
                    let Some(SourcePlacement::Reserved(reservation)) = placement else {
                        return (resolver, Err(invalid("Placed fence lost its original slot")));
                    };
                    resolver.resolve_placed_fence(durable, reservation.attempt, log_time, &context).await
                }
                SourceWork::Skip(mut message) => async {
                    let attempt = SourceTransaction::skipped_attempt(&message, position)?;
                    if let SourceMessage::Protobuf(SourceBody::Tx(tx)) = &mut message {
                        tx.user_metadata = None;
                        tx.idempotency_guard = None;
                        tx.bounded_retry = None;
                        tx.replay_submission = None;
                        tx.placed_attempt_seed = None;
                        tx.direct_placed_commit = None;
                    }
                    let key = rtdb_storage::ObjectKey::new(rtdb_format::keys::skipped_tx_key(position))?;
                    let reservation = context.memory.reserve(message.encoded_len().checked_add(128).ok_or_else(|| invalid("Skipped payload exceeds allocation range"))?)?;
                    let payload = bytes::Bytes::from_owner(ArchivedPayload { bytes: message.encode()?, _reservation: reservation });
                    context.storage.object_store().put(&key, payload).await?;
                    resolver.resolve_skipped(durable, position, log_time, attempt, &context).await
                }.await,
            };
            (resolver, result)
        }) });
        Ok(())
    }
    async fn enqueue_resolved(&mut self, resolved: Arc<ResolvedTx>, completion: Option<oneshot::Sender<Result<Applied>>>,
        direct_admission: Option<PlacedReservation>) -> Result<()> {
        self.enqueue_resolved_with_preparation(resolved, completion, direct_admission, None).await
    }

    async fn enqueue_resolved_with_preparation(&mut self, resolved: Arc<ResolvedTx>, completion: Option<oneshot::Sender<Result<Applied>>>,
        direct_admission: Option<PlacedReservation>, interactive: Option<PreparedInteractivePublication>) -> Result<()> {
        let mut completion = Completion(completion);
        let result = async {
            let strict = resolved.staging_clock_upper().is_some();
            let search_delta = if !strict && interactive.is_none() && resolved.result().committed()
                && let Some(builder) = self.serving_search.as_ref().filter(|builder| builder.journals_deltas()) {
                let leader = self.leader.as_ref().ok_or_else(term_ended)?;
                let snapshot = leader.resolver()?.resolving_snapshot(self.partition.value().snapshot())?;
                builder.prepare_delta(snapshot, Arc::clone(resolved.writes()), &leader.cancellation).await?
            } else { None };
            let leader = self.leader.as_mut().ok_or_else(term_ended)?;
            leader.cycle.resolved(resolved.writes().row_count()?)?;
            let block = self.partition.value().live().block_index();
            if let Some(publication) = interactive {
                if direct_admission.is_some() || resolved.placed_attempt().is_some() || completion.0.is_some() {
                    return Err(invalid("Interactive publication cannot carry a placed or external admission"));
                }
                let write = PendingWrite::prepared_interactive(resolved, block, publication)?;
                leader.appender.enqueue(Arc::clone(&write))?;
                leader.pending.push_back(write);
            } else if let Some(admission) = direct_admission {
                if resolved.placed_attempt() != Some(admission.attempt) || self.placed_slot.is_some()
                    || strict || completion.0.is_some()
                {
                    return Err(invalid("The resolved direct commit differs from its admission"));
                }
                let write = PendingWrite::direct_placed(WriteBody::Resolved(resolved, search_delta), block, admission, &self.resolve.memory)?;
                leader.appender.enqueue(Arc::clone(&write))?;
                leader.pending.push_back(write);
                if self.block_needs_cut() { self.cut().await?; }
            } else if let Some(attempt) = resolved.placed_attempt() {
                let slot = self.placed_slot.filter(|slot| slot.reservation.attempt == attempt)
                    .ok_or_else(|| invalid("Placed resolution lost its original receiver reservation"))?;
                if !leader.settling_placed || completion.0.is_some() {
                    return Err(invalid("Placed resolution bypassed receiver settlement"));
                }
                let terminal = PlacedTerminal { commitment: slot.reservation.commitment, fenced: slot.term < leader.term };
                let write = PendingWrite::placed(WriteBody::Resolved(resolved, search_delta), block, terminal, &self.resolve.memory)?;
                leader.appender.enqueue(Arc::clone(&write))?;
                leader.pending.push_back(write);
            } else {
                leader.enqueue(WriteBody::Resolved(resolved, search_delta), block, completion.0.take(), &self.resolve.memory)?;
                if !strict && self.block_needs_cut() { self.cut().await?; }
            }
            Ok(())
        }.await;
        if let Err(error) = &result && let Some(completion) = completion.0.take() {
            let _ = completion.send(Err(error.clone()));
        }
        result
    }
    /// Drops this node's cached copies of objects the log records as deleted.
    /// XTDB keeps no such copies to drop; a copy left behind costs only local disk, so a failure is reported rather than raised.
    fn forget_cached(&self, keys: impl IntoIterator<Item = rtdb_storage::ObjectKey>) -> impl std::future::Future<Output = ()> + Send + 'static {
        let storage = std::sync::Arc::clone(&self.resolve.storage);
        let keys: Vec<_> = keys.into_iter().collect();
        async move {
        for key in keys {
            if let Err(error) = storage.invalidate(&key).await {
                tracing::warn!(%key, %error, "Could not drop this node's cached copy of a deleted object");
            }
        }
        }
    }

    fn block_needs_cut(&self) -> bool {
        if self.pending_block.is_some() { return false; }
        let Some(leader) = &self.leader else { return false; };
        leader.cycle.is_full(self.config.rows_per_block) || self.block_needs_memory_cut(leader)
    }

    fn block_needs_memory_cut(&self, leader: &LeaderState) -> bool {
        if !leader.cycle.accepting() { return false; }
        let block = self.partition.value();
        let bytes = leader.pending.iter().fold(block.live().live_batch_bytes(), |bytes, pending| {
            match &pending.body {
                WriteBody::Resolved(tx, _) if block.live().latest_completed_tx().is_none_or(|key| tx.writes().tx_key().tx_id > key.tx_id) =>
                    bytes.saturating_add(tx.writes().retained_batch_bytes()),
                _ => bytes,
            }
        });
        let memory = &self.resolve.memory;
        leader.cycle.should_cut_for_memory(bytes, memory.available_bytes(), memory.limit_bytes())
    }

    async fn cut(&mut self) -> Result<()> {
        if self.pending_block.is_some() { return Ok(()); }
        if self.placed_slot.is_some() || self.serving_holds_checkpoint()
            || self.leader.as_ref().is_some_and(|leader| leader.awaiting_placed.is_some()) {
            self.deferred_cut = true;
            return Ok(());
        }
        if let Some(boundary) = self.adopted_boundary() {
            match self.logs.available_append_capacity().await {
                Ok(Some(capacity)) if boundary > capacity.published_through => {
                    self.deferred_cut = true;
                    return Ok(());
                }
                Err(_) => {
                    self.deferred_cut = true;
                    return Ok(());
                }
                Ok(_) => {}
            }
        }
        self.enqueue_checkpoint_cut()
    }

    fn enqueue_checkpoint_cut(&mut self) -> Result<()> {
        if self.pending_block.is_some() { return Ok(()); }
        if self.placed_slot.is_some() || self.serving_holds_checkpoint()
            || self.leader.as_ref().is_some_and(|leader| leader.awaiting_placed.is_some()) {
            return Ok(());
        }
        self.deferred_cut = false;
        let value = self.partition.value();
        let block = value.live().block_index();
        let relation_oid_order = if self.config.activate_classic_utf16_relation_oids
            && value.table_catalog().relation_oid_order() == RelationOidOrder::LegacyUtf8
        {
            RelationOidOrder::Utf16.wire_value()
        } else {
            None
        };
        let memory_triggered = self.leader.as_ref().is_some_and(|leader| {
            !leader.cycle.is_full(self.config.rows_per_block) && self.block_needs_memory_cut(leader)
        });
        let deferred_enabled = self.config.deferred_checkpoints || (self.config.hosted_deferred_checkpoints
            && self.logs.fences_writer_terms() && self.logs.requires_checkpoint_digest());
        let deferred_checkpoint = (deferred_enabled && !self.config.external_source
            && relation_oid_order.is_none() && self.default_erasure.is_none()
            && self.local_erasure_read_fence.is_none() && self.default_erasure_read_fence.is_none()
            && self.serving_search.is_none()
            && self.leader.as_ref().is_some_and(|leader| leader.confirmed && leader.compactor_reset.is_none()))
            .then_some(true);
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        leader.cycle.cut()?;
        let boundary = proto::BlockBoundary {
            block_index: Some(block.0), latest_processed_msg_id: Some(leader.resolver()?.source_position().0),
            external_source_token: leader.resolver()?.external_token().map(<[u8]>::to_vec), relation_oid_order, deferred_checkpoint,
        };
        leader.enqueue(WriteBody::Message(ReplicaBody::BlockBoundary(boundary)), block, None, &self.resolve.memory)?;
        if memory_triggered {
            self.status.send_modify(|status| status.memory_block_cuts = status.memory_block_cuts.saturating_add(1));
        }
        Ok(())
    }
    async fn flush_if_due(&mut self) -> Result<()> {
        let persisted = self.partition.value().table_catalog().block().and_then(BlockCatalog::latest_completed_tx).map(|key| key.tx_id);
        if self.default_erasure.is_none()
            && self.pending_block.is_none()
            && !self.waiting_for_checkpoint()
            && self.leader.as_ref().is_some_and(LeaderState::accepting_commands)
            && self.flush_timer.should_flush(now(), persisted)
        {
            self.cut().await?;
        }
        Ok(())
    }
    async fn command(&mut self, command: Command) -> Result<()> {
        if let Command::ServingStatus { key, lookup, completion } = command {
            let status = self.serving.as_ref().ok_or_else(serving::unavailable).and_then(|serving| match lookup {
                serving::ReceiptLookup::Status(scope) => serving.status(&key, scope.as_ref()),
                serving::ReceiptLookup::ExactRequest { scope, fingerprint } => serving.recover_exact(&key, scope.as_ref(), fingerprint),
                serving::ReceiptLookup::InstalledCustody(basis) => serving.installed_custody_receipt(&key, basis),
            });
            let _ = completion.send(status);
            return Ok(());
        }
        let waiting_for_checkpoint = self.waiting_for_checkpoint() || self.has_deferred_checkpoint()
            || self.placed_slot.is_some() || self.serving_holds_checkpoint()
            || self.default_erasure.is_some();
        let Some(leader) = &mut self.leader else { command.fail(term_ended()); return Ok(()); };
        if leader.term != command.term() { command.fail(term_ended()); return Ok(()); }
        if waiting_for_checkpoint || !leader.accepting_commands() { leader.deferred_commands.push_back(command); return Ok(()); }
        match command {
            Command::ServingStatus { .. } => unreachable!("handled before checking the writer term"),
            Command::PinCheckpoint { pin, completion, .. } => {
                let validated = (|| {
                    let serving = self.serving.as_ref().filter(|serving| serving.shares_main()).ok_or_else(serving::unavailable)?;
                    let installed = self.installed.filter(|_| leader.confirmed && self.first_erasure.is_none() && !self.config.external_source)
                        .ok_or_else(serving::unavailable)?;
                    pin.check_predecessor(serving.incarnation(), &self.logs, installed.replica, installed.source)?;
                    pin.check_lifetime((self.clock)())?;
                    pin.encode_record()
                })();
                let message = match validated { Ok(message) => message, Err(error) => { let _ = completion.send(Err(error)); return Ok(()); } };
                leader.enqueue(WriteBody::Message(ReplicaBody::NoOp(message)), self.partition.value().live().block_index(), Some(completion), &self.resolve.memory)
            }
            Command::ReleaseCheckpoint { release, completion, .. } => {
                let validated = (|| {
                    let serving = self.serving.as_ref().filter(|serving| serving.shares_main()).ok_or_else(serving::unavailable)?;
                    let installed = self.installed.filter(|_| leader.confirmed && !self.config.external_source).ok_or_else(serving::unavailable)?;
                    release.check_predecessor(serving.incarnation(), installed.replica)?;
                    release.encode_record()
                })();
                let message = match validated { Ok(message) => message, Err(error) => { let _ = completion.send(Err(error)); return Ok(()); } };
                leader.enqueue(WriteBody::Message(ReplicaBody::NoOp(message)), self.partition.value().live().block_index(), Some(completion), &self.resolve.memory)
            }
            Command::ServingValidate { request, completion, .. } => {
                let receipt = self.serving.as_ref().ok_or_else(serving::unavailable)
                    .and_then(|serving| serving.validate(request, (self.clock)()));
                let _ = completion.send(receipt);
                Ok(())
            }
            Command::ResetCompactor { primary, reservations, completion, .. } => {
                let value = self.partition.value();
                if !leader.confirmed || !leader.pending.is_empty() || self.pending_block.is_some() || leader.production.is_some() || leader.compactor_reset.is_some()
                    || value.live().block_index() != primary.block || !Arc::ptr_eq(value.trie_catalog(), &primary.expected) {
                    let _ = completion.send(Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.compactor-reset/catalog-moved", "Database changed while the reset was being prepared; prepare it again")));
                    return Ok(());
                }
                if let Err(error) = self.partition.install_reset_primary(primary.catalog, primary.target) {
                    let _ = completion.send(Err(error));
                    return Ok(());
                }
                leader.compactor_reset = Some(CompactorReset { block: primary.block, completion: Some(completion), _reservations: reservations });
                self.cut().await
            }
            Command::Delete { table, keys, completion, .. } => {
                if keys.is_empty() || (keys.iter().any(|key| key.level() == 0)
                    && !(self.resolve.storage.object_store().releases_receipt_l0() && table.stores_expiring_transaction_metadata()))
                {
                    let _ = completion.send(Err(Anomaly::incorrect("rtdb.replication/invalid-trie-removal", "The requested trie batch is empty or contains an ineligible L0 file")));
                    return Ok(());
                }
                if keys.iter().any(|key| key.level() == 0) {
                    let collection = self.partition.trie_collection();
                    let eligible: BTreeSet<_> = collection.eligible_receipt_l0(&table, Timestamp::MAX).cloned().collect();
                    if keys.iter().any(|key| key.level() == 0 && collection.current().retains_trie(&table, key)
                        && !eligible.contains(key))
                    {
                        let _ = completion.send(Err(Anomaly::incorrect("rtdb.replication/invalid-trie-removal", "A receipt L0 file still belongs to an admitted view")));
                        return Ok(());
                    }
                }
                let message = proto::TriesDeleted { table_name: Some(table.schema_and_table()), trie_keys: keys.into_iter().map(|key| key.to_string()).collect() };
                leader.enqueue(WriteBody::Message(ReplicaBody::TriesDeleted(message)), self.partition.value().live().block_index(), Some(completion), &self.resolve.memory)
            }
            Command::Retire { tables, as_of, completion, .. } => {
                if tables.is_empty() {
                    let _ = completion.send(Err(Anomaly::incorrect("rtdb.replication/invalid-table-retirement", "A table retirement names at least one table")));
                    return Ok(());
                }
                let message = proto::TablesRetired { table_names: tables.iter().map(TableRef::schema_and_table).collect(), as_of_micros: Some(as_of.0) };
                leader.enqueue(WriteBody::Message(ReplicaBody::TablesRetired(message)), self.partition.value().live().block_index(), Some(completion), &self.resolve.memory)
            }
            Command::External { mut writer, system_time, token, completion, .. } => {
                if !self.config.external_source {
                    let _ = completion.send(Err(Anomaly::incorrect("xtdb/not-external-source", "External writes require an external-source database")));
                    return Ok(());
                }
                let durable = self.partition.snapshot();
                let mut resolver = leader.resolver.take().ok_or_else(|| invalid("External resolution has no idle resolver"))?;
                let context = Arc::clone(&self.resolve);
                let clock = Arc::clone(&self.clock);
                leader.resolution = Some(Resolution { completion: Completion(Some(completion)), direct_admission: None, interactive: None, future: Box::pin(async move {
                    let result = resolver.resolve_external(durable, clock(), system_time, token, writer.as_mut(), &context).await;
                    (resolver, result)
                }) });
                Ok(())
            }
        }
    }
}

pub(crate) fn stale_replica_body(
    body: &ReplicaBody, transaction: i64, source: i64, block: i64, own_term: bool,
) -> bool {
    match body {
        ReplicaBody::ResolvedTx(message) => !own_term && message.tx_id.unwrap_or_default() <= transaction,
        ReplicaBody::TriesAdded(message) => message.source_msg_id.unwrap_or_default() <= source,
        ReplicaBody::ArtefactsAdded(message) => message.source_msg_id.unwrap_or_default() <= source,
        ReplicaBody::ArtefactsRemoved(message) => message.source_msg_id.unwrap_or_default() <= source,
        ReplicaBody::BlockBoundary(message) => message.block_index.unwrap_or_default() <= block,
        ReplicaBody::BlockUploaded(message) => message.block_index.unwrap_or_default() <= block,
        ReplicaBody::NoOp(message) => message.serving_record.is_none()
            && message.src_msg_id.is_some_and(|position| position <= source),
        ReplicaBody::TriesDeleted(_) | ReplicaBody::TablesRetired(_) | ReplicaBody::OversizedMessage(_) => false,
    }
}

pub(crate) fn additions(details: &[proto::TrieDetails]) -> Result<BTreeMap<TableRef, Vec<CatalogTrie>>> {
    let mut result = BTreeMap::new();
    for detail in details {
        let table = legacy_table_ref(detail.table_name.as_deref().unwrap_or_default())?;
        result.entry(table).or_insert_with(Vec::new).push(CatalogTrie::from_proto(detail)?);
    }
    Ok(result)
}
/// Keeps the entries whose trie is live in the durable catalog under this node's storage version and epoch, and returns the object keys of refused entries to delete.
/// Artefacts are advisory, so an entry that does not parse is dropped rather than faulting the log.
/// An entry under another storage version or epoch is dropped without deleting: its key is computed here, under this node's storage root, and may name another builder's file.
/// A group's object is deleted only when the catalog holds none of its members, since a member it still holds may take the same object later or be collected with it.
fn accept_artefacts(message: &mut proto::ArtefactsAdded, durable: &rtdb_index::Snapshot, version: i32, epoch: i32,
    requires_search_publication: bool) -> Vec<rtdb_storage::ObjectKey> {
    let entries = std::mem::take(&mut message.entries);
    let Ok(table) = legacy_table_ref(message.table_name.as_deref().unwrap_or_default()) else { return Vec::new() };
    if message.storage_version != Some(version) || message.storage_epoch.unwrap_or_default() != epoch { return Vec::new(); }
    let slug = durable.table_entry(&table).map(|entry| entry.slug.clone());
    let catalog = durable.trie_catalog();
    let mut rejected = BTreeSet::new();
    for entry in entries {
        let (Ok(key), Some(record)) = (entry.trie_key.as_deref().unwrap_or_default().parse::<TrieKey>(), &entry.record) else { continue };
        if record.kind() == rtdb_format::entity_directory::RECORD_KIND {
            if catalog.accepts_entity_directory(&table, &key, record) { message.entries.push(entry); }
            continue;
        }
        if record.kind() == rtdb_format::search_manifest::RECORD_KIND {
            if key.level() == 0 && (!requires_search_publication || search_publication_is_valid(record))
                && catalog.accepts_search_run(&table, record) {
                message.entries.push(entry);
            }
            continue;
        }
        let Ok(owner) = rtdb_format::keys::ArtefactOwner::of(&key, record) else { continue };
        if catalog.trie(&table, &key).is_some_and(|trie| trie.state == rtdb_index::TrieState::Live) { message.entries.push(entry); continue; }
        if owner.tries().iter().any(|member| member != &key && catalog.trie(&table, member).is_some()) { continue; }
        if let Some(object) = slug.as_ref().and_then(|slug| slug.owner_artefact_key(&owner, record.kind(), record.definition_id(), record.definition_version()).ok()).and_then(|key| rtdb_storage::ObjectKey::new(key).ok()) {
            rejected.insert(object);
        }
    }
    rejected.into_iter().collect()
}

fn search_publication_is_valid(record: &proto::ArtefactRecord) -> bool {
    use rtdb_storage::{object_store::strong_entity_tag, valid_object_version};

    let Ok(change) = proto::SearchRunChange::from_artefact(record) else { return false; };
    if change.retire_only() { return true; }
    change.run.and_then(|run| run.publication).is_some_and(|publication| {
        !publication.bucket().is_empty() && !publication.key().is_empty()
            && if publication.terminal.is_some() {
                publication.terminal_publication().is_some_and(|terminal| strong_entity_tag(&terminal.etag))
            } else {
                valid_object_version(publication.version().as_bytes())
            }
    })
}

async fn admit_search_intents(mut message: proto::ArtefactsAdded, durable: &Snapshot, store: &dyn ObjectStore,
    cancellation: &CancellationToken) -> Result<proto::ArtefactsAdded> {
    let Ok(table) = legacy_table_ref(message.table_name.as_deref().unwrap_or_default()) else { return Ok(message); };
    let admission = async {
        'entries: for entry in std::mem::take(&mut message.entries) {
            if let Some(record) = &entry.record && record.kind() == rtdb_format::search_manifest::RECORD_KIND {
                let Ok(change) = proto::SearchRunChange::from_artefact(record) else { continue; };
                let Some(run) = change.run.as_ref() else { continue; };
                if !change.retire_only() && run.allocation.is_some()
                    && !durable.trie_catalog().search_runs(&table).any(|active| active.digest() == run.digest())
                {
                    let Some(table_entry) = durable.table_entry(&table) else { continue; };
                    let Ok(intent) = run.pending_key(&table_entry.slug).and_then(|key| Ok(ObjectKey::new(key)?))
                        else { continue; };
                    // An earlier source admission is ordered before pruning. A later one needs the intent again.
                    for may_retry in [true, false] {
                        match store.head_fresh(&intent).await {
                            Ok(_) => break,
                            Err(StorageError::MissingObject(_)) => continue 'entries,
                            Err(error) => {
                                let error = Anomaly::from(error);
                                if !may_retry || !matches!(error.category, AnomalyCategory::Unavailable | AnomalyCategory::Busy) {
                                    return Err(error);
                                }
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                }
            }
            message.entries.push(entry);
        }
        Ok(message)
    };
    cancellation.run_until_cancelled(tokio::time::timeout(Duration::from_secs(5), admission)).await.ok_or_else(stopped)?
        .map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.replication/search-intent-timeout",
            "Search staging intent admission timed out"))?
}

/// The objects of collected tries this node may hold copies of: data, meta, and the artefacts their entries record.
/// A group's artefact goes only with the last member the catalog holds.
fn cached_objects(value: &rtdb_index::PartitionValue, table: &TableRef, keys: &BTreeSet<TrieKey>) -> Vec<rtdb_storage::ObjectKey> {
    let Some(entry) = value.table_catalog().entry(table) else { return Vec::new() };
    let tries = value.trie_catalog();
    let objects: BTreeSet<String> = keys.iter().flat_map(|key| {
        let artefacts = tries.trie(table, key).into_iter().flat_map(|trie| trie.artefacts.iter())
            .filter_map(|record| rtdb_format::keys::ArtefactOwner::of(key, record).ok().map(|owner| (owner, record)))
            .filter(|(owner, _)| owner.tries().iter().all(|member| keys.contains(member) || tries.trie(table, member).is_none()))
            .filter_map(|(owner, record)| entry.slug.owner_artefact_key(&owner, record.kind(), record.definition_id(), record.definition_version()).ok());
        [entry.slug.data_key(key), entry.slug.metadata_key(key)].into_iter().chain(artefacts).collect::<Vec<_>>()
    }).collect();
    objects.into_iter().filter_map(|key| rtdb_storage::ObjectKey::new(key).ok()).collect()
}

pub(crate) fn artefact_entries(entries: &[proto::ArtefactEntry]) -> Vec<(TrieKey, proto::ArtefactRecord)> {
    entries.iter().filter_map(|entry| Some((entry.trie_key.as_deref()?.parse().ok()?, entry.record.clone()?))).collect()
}

fn check_db_op(effects: &dyn DatabaseEffects, resolver: &TxResolver, operation: &DbOp) -> Result<()> {
    let name = match operation { DbOp::AttachDatabase(message) => message.db_name.as_deref(), DbOp::DetachDatabase(message) => message.db_name.as_deref() }.unwrap_or_default();
    match (operation, resolver.staged_db_op(name)) {
        (DbOp::AttachDatabase(_), Some(DbOp::AttachDatabase(_))) => Err(Anomaly::new(AnomalyCategory::Conflict, "xtdb/db-exists", "Database already exists").with_context("db-name", name.into())),
        (DbOp::AttachDatabase(_), Some(DbOp::DetachDatabase(_))) => Err(Anomaly::new(AnomalyCategory::Conflict, "xtdb/db-being-detached", "Database is still being detached").with_context("db-name", name.into())),
        (DbOp::DetachDatabase(_), Some(DbOp::AttachDatabase(_))) => Ok(()),
        (DbOp::DetachDatabase(_), Some(DbOp::DetachDatabase(_))) => Err(Anomaly::new(AnomalyCategory::NotFound, "xtdb/no-such-db", "Database does not exist").with_context("db-name", name.into())),
        (_, None) => effects.check_db_op(operation),
    }
}
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::fault("rtdb.replication/invalid-transition", message) }
fn replica_tx_id(record: &ReplicaRecord) -> Option<i64> {
    match &record.envelope.message { ReplicaBody::ResolvedTx(message) => message.tx_id, _ => None }
}
fn confirmed_search_delta(pending: Option<&PreparedSearchDelta>, durable: Option<&bytes::Bytes>) -> Option<SearchDelta> {
    durable.map(|encoded| match pending {
        Some(delta) if delta.encoded == *encoded => SearchDelta::Prepared(delta.clone()),
        _ => SearchDelta::Replayed(encoded.clone()),
    })
}

fn readback_mismatch(tx_id: Option<i64>) -> Anomaly {
    let error = Anomaly::fault("xtdb.indexer/leader-read-back-mismatch", "Replica transaction does not match the oldest staged transaction");
    if let Some(tx_id) = tx_id {
        error.with_context("replica-tx-id", tx_id.into())
    } else {
        error
    }
}

fn trie_removal_unconfirmed() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.replication/trie-removal-unconfirmed", "The installed catalog and checkpoint do not confirm this trie removal")
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;


#[cfg(test)]
#[test]
fn arrow_build_leader_installs_only_the_readback_delta_and_reuses_prepared_buffers() {
    let memory = MemoryBudget::new(1 << 20);
    let encoded = bytes::Bytes::from_static(b"prepared");
    let events = rtdb_format::data::build_event_batch(&[], None, None).unwrap();
    let batch = rtdb_columnar::OwnedBatch::new(events, &memory).unwrap();
    let prepared = PreparedSearchDelta { encoded: encoded.clone(), batch };
    assert!(confirmed_search_delta(Some(&prepared), None).is_none());
    let Some(SearchDelta::Prepared(installed)) = confirmed_search_delta(Some(&prepared), Some(&encoded)) else {
        panic!("confirmed bytes should reuse the prepared batch");
    };
    assert!(Arc::ptr_eq(prepared.batch.reservation(), installed.batch.reservation()));
    let other = bytes::Bytes::from_static(b"replayed");
    let Some(SearchDelta::Replayed(replayed)) = confirmed_search_delta(Some(&prepared), Some(&other)) else {
        panic!("different read-back bytes must use the replay path");
    };
    assert_eq!(other.as_ptr(), replayed.as_ptr());
}

/// Records one follower record's handling time when dropped.
struct ReplicaProcessTimer { observer: Arc<dyn rtdb_plan::ExecutionObserver>, database: String, kind: rtdb_plan::ReplicaRecordKind, started: std::time::Instant }
impl Drop for ReplicaProcessTimer {
    fn drop(&mut self) { self.observer.record_replica_process(&self.database, self.kind, self.started.elapsed()); }
}
