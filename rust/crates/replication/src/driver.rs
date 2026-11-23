use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{catalog::{BlockCatalog, legacy_table_ref}, database::proto::DatabaseConfig, envelope::{ReplicaBody, ReplicaEnvelope, SourceBody, SourceMessage}, keys::TrieKey, log::proto::{self, resolved_tx::DbOp}};
use rtdb_index::{CatalogCut, CatalogTrie, PartitionState, ServingSearch, Snapshot, StagedWrite, TrieCatalog};
use rtdb_storage::{log::{AppendCapacity, LogOffset, LogRecord, LogTail, ReplayHead}, object_store::ReadReservation};
use rtdb_tx::{ResolveContext, ResolvedTx, SourceTransaction, TransactionResult, TransactionWriter, TxResolver, WriterPublication};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, ReplicaPosition, Result, SourcePosition, TableRef, Term, TermSequence, Timestamp, TransactionKey, TxId};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{BlockCycle, BlockStore, FlushTimer, PartitionLogs, PendingBlock, ProgressMonitor, PublishedPrimaryReset, RecoveredPartition, ReplicaRecord, TermFence, TermPosition, Verdict};
use crate::admission::AdmittedReplica;
use crate::appender::{Applied, AppenderHandle, CommitLagBasis, PendingWrite, ReplicaAppender, WriteBody, complete_record, join_error, now};
use crate::blocks::{RecoveredBlockConfirmation, checkpoint_digest, matches_catalog_boundary, matches_confirmation, validate_uploaded_root};
use crate::logs::{APPEND_CAPACITY, DecodedReplica, STRICT_DISPATCH_CUTOFF, STRICT_DISPATCH_UNAVAILABLE, replica_decode_bound};
use crate::progress::{AppliedTransaction, DriverRestart, stopped, term_ended};
use crate::readback_identity::matching_present_integer;
use crate::state::fail_pending;

#[async_trait]
pub trait DatabaseEffects: Send + Sync {
    fn check_db_op(&self, operation: &DbOp) -> Result<()>;
    async fn apply_db_op(&self, operation: &DbOp) -> Result<()>;
    fn secondary_databases(&self) -> Result<BTreeMap<String, DatabaseConfig>>;
}

/// Supplies UTC timestamps in Unix microseconds for replication.
/// The driver, term appender and cloned handles can sample it concurrently.
pub type ReplicationClock = Arc<dyn Fn() -> Timestamp + Send + Sync>;

#[async_trait]
pub trait ServingSearchBuilder: Send + Sync {
    async fn prepare(&self, snapshot: Snapshot, cancellation: &CancellationToken) -> Result<Option<ServingSearch>>;

    fn journals_deltas(&self) -> bool { false }

    async fn prepare_delta(&self, _snapshot: Snapshot, _write: Arc<StagedWrite>, _cancellation: &CancellationToken) -> Result<Option<bytes::Bytes>> { Ok(None) }

    async fn install_delta(&self, snapshot: Snapshot, _write: &StagedWrite, _delta: Option<&[u8]>,
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
}
impl Default for ReplicationConfig {
    fn default() -> Self {
        Self { enabled: true, may_lead: true, external_source: false, skipped_transactions: BTreeSet::new(), rows_per_block: 100_000, assertion_interval: Duration::from_millis(100), flush_timeout: Duration::from_secs(4 * 60 * 60),
            pending_block_records: 1024, pending_block_bytes: 64 * 1024 * 1024, election_seed: uuid::Uuid::new_v4().as_u128() as u64,
            idle_heartbeat: true, claim_wait: None }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role { Following, Leading(Term), Stopped }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstalledPrefix {
    pub replica: ReplicaPosition,
    pub source: SourcePosition,
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
    pub promotion_failure: Option<Anomaly>,
    pub last_block_upload_epoch_seconds: i64,
}

#[derive(Clone)]
pub struct ReplicationHandle {
    logs: Arc<PartitionLogs>, progress: Arc<ProgressMonitor>, status: watch::Receiver<DriverStatus>,
    commands: mpsc::Sender<Command>, cancellation: CancellationToken, clock: ReplicationClock,
    partition: Arc<PartitionState>,
}
impl ReplicationHandle {
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
    pub async fn await_tx(&self, id: TxId, cancellation: &CancellationToken) -> Result<Option<Arc<TransactionResult>>> { self.progress.await_tx(id, cancellation).await }
    pub async fn await_source(&self, position: SourcePosition, cancellation: &CancellationToken) -> Result<()> { self.progress.await_source(position, cancellation).await }
    pub async fn flush(&self) -> Result<SourcePosition> {
        let expected = self.partition.value().table_catalog().block_index().map_or(-1, |block| block.0);
        self.submit_source(&SourceMessage::Protobuf(SourceBody::FlushBlock(proto::FlushBlock { expected_block_idx: Some(expected) })), (self.clock)()).await
    }
    /// Cancels the owned driver; the node must join `ReplicationDriver::run` before releasing its dependencies.
    pub fn shutdown(&self) { self.cancellation.cancel(); }
    pub async fn publish_compaction(&self, message: proto::TriesAdded, cancellation: &CancellationToken) -> Result<SourcePosition> {
        let position = self.submit_source(&SourceMessage::Protobuf(SourceBody::TriesAdded(message)), (self.clock)()).await?;
        self.await_source(position, cancellation).await?;
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
        let position = self.submit_source(&SourceMessage::Protobuf(SourceBody::ArtefactsRemoved(message)), (self.clock)()).await?;
        self.await_source(position, cancellation).await?;
        Ok(position)
    }
}
#[derive(Clone)]
pub struct TermHandle { handle: ReplicationHandle, term: Term }
impl TermHandle {
    pub fn term(&self) -> Term { self.term }
    /// The applied resume token covered by the store and the retained durable journal.
    pub fn recoverable_external_token(&self) -> Option<Arc<[u8]>> {
        if self.handle.logs.retains_recovery_history() && self.handle.status.borrow().role == Role::Leading(self.term) {
            self.handle.progress.current().external_token
        } else {
            None
        }
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
    ResetCompactor { term: Term, primary: PublishedPrimaryReset, reservations: Arc<[Arc<MemoryReservation>]>, completion: oneshot::Sender<Result<BlockIndex>> },
    Delete { term: Term, table: TableRef, keys: Vec<TrieKey>, completion: oneshot::Sender<Result<Applied>> },
    Retire { term: Term, tables: Vec<TableRef>, as_of: Timestamp, completion: oneshot::Sender<Result<Applied>> },
    External { term: Term, writer: Box<dyn TransactionWriter>, system_time: Option<Timestamp>, token: Option<Vec<u8>>, completion: oneshot::Sender<Result<Applied>> },
}
impl Command {
    fn term(&self) -> Term { match self { Self::Delete { term, .. } | Self::Retire { term, .. } | Self::External { term, .. } | Self::ResetCompactor { term, .. } => *term } }
    fn fail(self, error: Anomaly) {
        match self {
            Self::Delete { completion, .. } | Self::Retire { completion, .. } | Self::External { completion, .. } => { let _ = completion.send(Err(error)); }
            Self::ResetCompactor { completion, .. } => { let _ = completion.send(Err(error)); }
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
type Poll = BoxFuture<'static, (Box<dyn LogTail>, Result<TailPoll>)>;
struct SelectedRecord { raw: LogRecord, decode_reservation: Arc<MemoryReservation> }
struct ArchivedPayload { bytes: Vec<u8>, _reservation: Arc<rtdb_columnar::MemoryReservation> }
impl AsRef<[u8]> for ArchivedPayload { fn as_ref(&self) -> &[u8] { &self.bytes } }
fn start_poll(mut tail: Box<dyn LogTail>, timeout: Duration) -> Poll {
    Box::pin(async move {
        let result = match tail.replay() {
            Some(replay) => replay.poll_head(timeout).await.map(TailPoll::Prepared),
            None => tail.poll(timeout).await.map(TailPoll::Legacy),
        };
        (tail, result.map_err(Into::into))
    })
}
async fn pending_poll(poll: &mut Option<Poll>) -> (Box<dyn LogTail>, Result<TailPoll>) {
    match poll { Some(poll) => poll.await, None => futures::future::pending().await }
}
async fn read_replay_head(tail: &mut dyn LogTail, head: ReplayHead, memory: &Arc<MemoryBudget>, decode_bound: fn(usize) -> Result<usize>, cancellation: &CancellationToken) -> Result<Option<SelectedRecord>> {
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
}
enum Production {
    Writing(BoxFuture<'static, Result<crate::ProducedBlock>>),
    Appending(BoxFuture<'static, Result<crate::ReplicaMetadata>>),
}
enum ProductionResult { Written(Result<crate::ProducedBlock>), Appended(Result<crate::ReplicaMetadata>) }
async fn pending_resolution(resolution: &mut Option<Resolution>) -> (TxResolver, Result<Arc<ResolvedTx>>) {
    match resolution { Some(resolution) => resolution.future.as_mut().await, None => futures::future::pending().await }
}
async fn pending_production(production: &mut Option<Production>) -> ProductionResult {
    match production {
        Some(Production::Writing(future)) => ProductionResult::Written(future.as_mut().await),
        Some(Production::Appending(future)) => ProductionResult::Appended(future.as_mut().await),
        None => futures::future::pending().await,
    }
}

struct LeaderState {
    term: Term, resolver: Option<TxResolver>, resolution: Option<Resolution>, production: Option<Production>, confirmed: bool,
    recovery_write: Option<Arc<PendingWrite>>,
    held: VecDeque<AdmittedReplica>, held_bytes: usize, cycle: BlockCycle, pending: VecDeque<Arc<PendingWrite>>,
    source_tail: Option<Box<dyn LogTail>>, source_poll: Option<Poll>, source_batch: VecDeque<LogRecord>, source_head: Option<ReplayHead>,
    waiting_strict_source: Option<SourceInput>,
    appender: AppenderHandle, job: Option<JoinHandle<Result<()>>>, cancellation: CancellationToken,
    deferred_commands: VecDeque<Command>,
    compactor_reset: Option<CompactorReset>,
    trie_removals: Vec<AppliedTrieRemoval>,
}

struct SourceInput {
    position: SourcePosition,
    log_time: Timestamp,
    work: SourceWork,
    _decode_charge: Arc<MemoryReservation>,
}

enum SourceWork {
    Resolve(SourceTransaction),
    Skip(SourceMessage),
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
        self.recovery_write.is_none() && self.cycle.accepting() && !self.pending_strict_write()
    }
    fn pending_strict_write(&self) -> bool {
        self.pending.front().is_some_and(|pending| matches!(&pending.body,
            WriteBody::Resolved(tx, _) if tx.staging_clock_upper().is_some()))
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
    recovered_confirmation: Option<RecoveredBlockConfirmation>,
    compaction_replay_cutoff: Option<ReplicaPosition>,
    track_catalog_cut: bool,
    recognized_prefix: bool,
    installed: Option<InstalledPrefix>,
    historical_confirmations: VecDeque<RecoveredBlockConfirmation>,
    fence: TermFence, read_position: ReplicaPosition, pending_block: Option<PendingBlock>, claim: Option<ReplicaPosition>, claiming: Option<Claim>,
    replica_tail: Option<Box<dyn LogTail>>, replica_poll: Option<Poll>, replica_batch: VecDeque<LogRecord>, replica_head: Option<ReplayHead>,
    leader: Option<LeaderState>, commands: mpsc::Receiver<Command>, status: watch::Sender<DriverStatus>, flush_timer: FlushTimer,
    required_append_bytes: Option<u64>,
    deferred_cut: bool,
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
            installed: None,
            open_block: recovered.partition.value().live().block_index(), adopted_block: recovered.block.clone().map(Arc::new), promotion_failure: None, last_block_upload_epoch_seconds: 0 };
        let (status, receiver) = watch::channel(initial);
        let (sender, commands) = mpsc::channel(128);
        let handle = ReplicationHandle { logs: Arc::clone(&logs), progress: Arc::clone(&progress), status: receiver, commands: sender, cancellation: cancellation.clone(), clock: Arc::clone(&clock), partition: Arc::clone(&recovered.partition) };
        let persisted_tx = recovered.block.as_ref().and_then(BlockCatalog::latest_completed_tx).map(|key| key.tx_id);
        let flush_timer = FlushTimer::new(clock(), persisted_tx, config.flush_timeout)?;
        let recovered_confirmation = if config.enabled && config.may_lead {
            recovered.confirmation(logs.replica_epoch, logs.storage_version, logs.storage_epoch, logs.memory())?
        } else { None };
        let replica_tail = recovered.take_replay_tail(&logs, read_position)?;
        let restart = if config.enabled { progress.prepare_driver_restart() } else { None };
        Ok((handle, Self { config, logs, blocks, partition: recovered.partition, progress, restart, resolve, effects, cancellation, clock, recovered_confirmation, compaction_replay_cutoff: recovered.compaction_replay_cutoff, track_catalog_cut, recognized_prefix: true, installed: None, historical_confirmations: VecDeque::new(), fence: recovered.fence,
            read_position, pending_block: None, claim: None, claiming: None, replica_tail, replica_poll: None, replica_batch: VecDeque::new(), replica_head: None, leader: None, commands, status, flush_timer, serving_search: None, required_append_bytes: None, deferred_cut: false }))
    }

    pub fn with_serving_search(mut self, builder: Arc<dyn ServingSearchBuilder>) -> Self {
        self.serving_search = Some(builder);
        self
    }

    async fn apply_transaction(&mut self, write: &StagedWrite, search_delta: Option<&[u8]>) -> Result<()> {
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
            None => self.logs.replica_tail(self.read_position).await,
        };
        if cancellation.is_cancelled() { return Ok(()); }
        let tail = tail?;
        let Some(history) = cancellation.run_until_cancelled(self.prove_historical_confirmations()).await else { return Ok(()); };
        self.historical_confirmations = history.unwrap_or_default();
        if self.cancellation.is_cancelled() { return Ok(()); }
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
            self.prepare_polls();
            enum Event { ReplicaBatch(Box<dyn LogTail>, Result<TailPoll>), Replica, SourceBatch(Box<dyn LogTail>, Result<TailPoll>), Source, Command(Option<Command>), Deferred, Appender(Result<()>), Resolved(TxResolver, Result<Arc<ResolvedTx>>), Produced(ProductionResult), Claimed(Result<crate::ReplicaMetadata>), Capacity }
            let ready_replica = self.replica_head.is_some() || !self.replica_batch.is_empty();
            let waiting_for_checkpoint = self.waiting_for_checkpoint();
            let accept_commands = self.leader.as_ref().is_none_or(|leader| {
                (!waiting_for_checkpoint && leader.accepting_commands()) || leader.deferred_commands.len() < 128
            });
            let check_capacity = waiting_for_checkpoint && self.pending_block.is_none()
                && self.leader.as_ref().is_some_and(|leader| leader.confirmed && leader.accepting());
            let mut empty_poll = None;
            let mut empty_job = None;
            let mut empty_resolution = None;
            let mut empty_production = None;
            let (source_poll, job, resolution, production, ready_source, ready_deferred) = match self.leader.as_mut() {
                Some(leader) => {
                    let ready_source = !waiting_for_checkpoint && leader.ready_source();
                    let ready_deferred = !waiting_for_checkpoint && leader.accepting_commands() && !leader.deferred_commands.is_empty();
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
                _ = capacity_poll.tick(), if check_capacity => Event::Capacity,
            };
            match event {
                Event::ReplicaBatch(tail, records) => {
                    self.replica_poll = None; self.replica_tail = Some(tail);
                    match records? {
                        TailPoll::Prepared(Some(head)) => self.replica_head = Some(head),
                        TailPoll::Legacy(records) if !records.is_empty() => self.replica_batch.extend(records),
                        TailPoll::Prepared(None) | TailPoll::Legacy(_) => {
                            if !self.cancellation.is_cancelled() { self.lead_after_empty_read().await?; }
                        }
                    }
                }
                Event::Replica => if let Some(head) = self.replica_head {
                    let tail = self.replica_tail.as_deref_mut().ok_or_else(|| invalid("Prepared replica head has no tail"))?;
                    let Some(record) = read_replay_head(tail, head, self.logs.memory(), replica_decode_bound, &self.cancellation).await? else { return Ok(()); };
                    self.replica_head = None;
                    self.consume_record(record.raw, Some(record.decode_reservation)).await?;
                } else if let Some(record) = self.replica_batch.pop_front() {
                    self.consume(record).await?;
                },
                Event::SourceBatch(tail, records) => if let Some(leader) = &mut self.leader {
                    leader.source_poll = None; leader.source_tail = Some(tail);
                    match records? {
                        TailPoll::Prepared(head) => leader.source_head = head,
                        TailPoll::Legacy(records) => leader.source_batch.extend(records),
                    }
                    self.flush_if_due().await?;
                },
                Event::Source => self.resolve_next_source().await?,
                Event::Resolved(resolver, result) => self.resolution_finished(resolver, result).await?,
                Event::Produced(result) => self.production_finished(result).await?,
                Event::Claimed(result) => self.claim_finished(result).await?,
                Event::Deferred => {
                    if let Some(command) = self.leader.as_mut().and_then(|leader| leader.deferred_commands.pop_front()) { self.command(command).await?; }
                }
                Event::Command(Some(command)) => self.command(command).await?,
                Event::Command(None) => return Ok(()),
                Event::Capacity => {
                    if let Ok(Some(capacity)) = self.logs.available_append_capacity().await {
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
    fn waiting_for_checkpoint(&self) -> bool {
        self.deferred_cut || self.required_append_bytes.is_some()
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
                let tail = self.logs.source_tail(self.progress.current().source_position).await?;
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
            match self.logs.open_replica_tail(retained[first].boundary).await {
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
            let Some(selected_record) = read_replay_head(tail.as_mut(), head, self.logs.memory(), replica_decode_bound, &self.cancellation).await? else {
                return Ok(VecDeque::new());
            };
            let next = ReplicaPosition::from_epoch_offset(self.logs.replica_epoch, selected_record.raw.metadata.offset.0)?;
            if next <= position || next > end {
                return Err(invalid("Retained replica history did not reach the selected boundary in order"));
            }
            position = next;
            let record = self.logs.decode_reserved_replica(selected_record.raw, selected_record.decode_reservation)?;
            let record = if let Some(record) = record {
                let admission = witness.admit(record.envelope.term, record.envelope.term_sequence)?;
                if admission.verdict == Verdict::Apply { Some(self.logs.resolve_oversized(record).await?) } else { None }
            } else { None };
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
        if !self.waiting_for_checkpoint() && let Some(leader) = &mut self.leader {
            if leader.can_poll_source() && leader.waiting_strict_source.is_none()
                && leader.source_poll.is_none() && leader.source_head.is_none() && leader.source_batch.is_empty() {
                if let Some(tail) = leader.source_tail.take() { leader.source_poll = Some(start_poll(tail, self.config.assertion_interval)); }
            }
        }
    }
    /// A journal that fences writer terms promotes its grant holder only after an empty read, so every record of
    /// an earlier term has been admitted before the new term's sequence-zero record can open it.
    async fn lead_after_empty_read(&mut self) -> Result<()> {
        if !self.logs.fences_writer_terms() { return self.start_claim(); }
        if !self.config.may_lead || self.leader.is_some() || !self.logs.holds_writer_grant().await? { return Ok(()); }
        let term = self.fence.claim_term()?;
        if let Err(error) = self.promote_term(term).await {
            self.promotion_failed(error).await?;
        } else { self.status.send_modify(|status| status.promotion_failure = None); }
        Ok(())
    }
    fn start_claim(&mut self) -> Result<()> {
        // Promotion waits for the next empty read instead of appending a claim.
        if self.logs.fences_writer_terms() { return Ok(()); }
        if !self.config.may_lead || self.leader.is_some() || self.claim.is_some() || self.claiming.is_some() { return Ok(()); }
        let term = self.fence.claim_term()?;
        let envelope = ReplicaEnvelope { term, term_sequence: Some(TermSequence(0)), message: ReplicaBody::NoOp(proto::NoOp { src_msg_id: None }) };
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
        self.flush_timer = FlushTimer::new((self.clock)(), persisted, self.config.flush_timeout)?;
        let cancellation = self.cancellation.child_token();
        // An inherited block's adopt advances applied progress to the position its boundary covers, so the source opens there.
        let source_tail = if self.pending_block.is_some() || self.waiting_for_checkpoint() { None } else { Some(self.logs.source_tail(progress.source_position).await?) };
        let (appender, pump) = ReplicaAppender::new(Arc::clone(&self.logs), Arc::clone(&self.resolve.memory),
            term, self.config.assertion_interval, cancellation.clone(), Arc::clone(&self.clock));
        // Without a claim, the term's first appended record opens it.
        let pump = if self.logs.fences_writer_terms() { pump.starting_at(TermSequence(0)) } else { pump };
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
        let job = pump.with_heartbeat(self.config.idle_heartbeat).run(recovery_write.clone());
        self.leader = Some(LeaderState { term, resolver: Some(TxResolver::new(progress.tx_key, progress.source_position, progress.external_token.as_deref().map(<[u8]>::to_vec), &self.resolve.memory)?), resolution: None,
            production: None, confirmed, recovery_write, held: VecDeque::new(), held_bytes: 0, cycle,
            pending: VecDeque::new(), source_tail, source_poll: None, source_batch: VecDeque::new(), source_head: None, waiting_strict_source: None, appender, job: Some(tokio::spawn(job)), cancellation, deferred_commands: VecDeque::new(), compactor_reset: None, trie_removals: Vec::new() });
        if produce_inherited {
            self.produce_pending()?;
        }
        self.publish_status(self.role());
        Ok(())
    }
    async fn demote(&mut self) -> Result<VecDeque<AdmittedReplica>> {
        self.deferred_cut = false;
        if let Some(mut leader) = self.leader.take() {
            leader.cancellation.cancel();
            drop(leader.compactor_reset.take());
            fail_pending(&mut leader.pending, term_ended());
            for command in leader.deferred_commands.drain(..) { command.fail(term_ended()); }
            drop(leader.resolution.take());
            if let Some(resolver) = &mut leader.resolver { resolver.discard_staged(); }
            let result = if let Some(job) = leader.job.take() { job.await.map_err(join_error).and_then(|result| result) } else { Ok(()) };
            if let Some(production) = leader.production.take() {
                match production { Production::Writing(future) => { if future.await.is_ok() { self.record_block_upload(); } }, Production::Appending(future) => { let _ = future.await; } }
            }
            self.publish_status(Role::Following);
            result?;
            return Ok(leader.held);
        }
        Ok(VecDeque::new())
    }
    fn role(&self) -> Role { self.leader.as_ref().filter(|leader| leader.confirmed).map_or(Role::Following, |leader| Role::Leading(leader.term)) }
    fn publish_status(&self, role: Role) {
        let value = self.partition.value();
        let (promotion_failure, last_block_upload_epoch_seconds) = { let status = self.status.borrow(); (status.promotion_failure.clone(), status.last_block_upload_epoch_seconds) };
        self.status.send_replace(DriverStatus { role, highest_term: self.fence.highest_term(), replica_position: self.read_position,
            installed: self.installed,
            open_block: value.live().block_index(), adopted_block: value.table_catalog().block().cloned().map(Arc::new), promotion_failure, last_block_upload_epoch_seconds });
    }
    fn update_installed_prefix(&mut self) -> Result<()> {
        let apply_deferred = self.leader.as_ref().is_some_and(|leader| !leader.held.is_empty());
        if let Some(through) = crate::catalog_cut::completed_prefix(
            self.read_position.0, self.recognized_prefix, self.pending_block.is_some(), apply_deferred,
        ) {
            let through = ReplicaPosition(through);
            let checkpoint = self.partition.value();
            if self.track_catalog_cut && checkpoint.table_catalog().block().is_none_or(|block| {
                block.boundary_replica_position().is_some_and(|boundary| {
                    boundary.after_offset(self.logs.replica_epoch) >= 0 && boundary <= through
                })
            }) {
                self.partition.publish_catalog_cut(CatalogCut {
                    storage_version: self.logs.storage_version,
                    storage_epoch: self.logs.storage_epoch,
                    replica_epoch: self.logs.replica_epoch,
                    through,
                })?;
            }
            let source = self.progress.current().source_position;
            self.logs.record_source_applied(source);
            self.installed = Some(InstalledPrefix { replica: through, source });
            if let Some(leader) = &mut self.leader
                && let Some(reset) = leader.compactor_reset.as_ref()
                && self.partition.value().table_catalog().block_index() == Some(reset.block)
            {
                leader.compactor_reset.take().expect("matched reset block").complete();
            }
        }
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
        let own_term = self.leader.as_ref().map(|leader| leader.term);
        let record = match self.logs.decode_readback(raw, decode_reservation, own_term)? {
            DecodedReplica::Known(record) => record,
            DecodedReplica::Unrecognized { term, sequence } => {
                if let Some(pending) = self.own_pending_tx_at(term, sequence)
                    && self.fence.clone().admit(term, sequence)?.verdict == Verdict::Apply
                {
                    let error = readback_mismatch(None);
                    pending.finish(Err(error.clone()));
                    return Err(error);
                }
                // The protobuf body can be omitted or too new for this reader to recognize.
                self.recognized_prefix = false;
                self.read_position = position;
                self.complete_trie_removals();
                return Ok(());
            }
        };
        let observed_term = record.envelope.term;
        let (admission, record) = self.fence.admit_record(record)?;
        let wins = self.claim == Some(position) && admission.opens_term();
        if let Some(claim) = &mut self.claiming {
            if observed_term == claim.term && admission.opens_term() { claim.observed = Some(position); }
        }
        if self.claim == Some(position) { self.claim = None; }
        if self.leader.as_ref().is_some_and(|leader| admission.highest_term > leader.term || admission.verdict == Verdict::Voided) {
            let held = self.demote().await?;
            for held in held { self.apply_admitted(held).await?; }
        }
        if let Some(record) = record {
            if wins { self.try_promote().await?; }
            let sequence = record.envelope.term_sequence;
            let record = record.resolve_payload(&self.logs).await.inspect_err(|error| {
                if let Some(pending) = self.own_pending_tx_at(observed_term, sequence) {
                    pending.finish(Err(error.clone()));
                }
            })?;
            self.validate_own_readback(&record)?;
            if !self.confirm_recovered_block(&record).await? {
                self.apply_admitted(record).await?;
            }
        }
        self.read_position = position;
        self.update_installed_prefix()?;
        self.publish_status(self.role());
        if admission.verdict == Verdict::Voided { self.start_claim()?; }
        Ok(())
    }
    fn own_pending_tx_at(&self, term: Term, sequence: Option<TermSequence>) -> Option<&PendingWrite> {
        let leader = self.leader.as_ref().filter(|leader| leader.term == term)?;
        leader.pending_tx_at(sequence).map(|(pending, _)| pending.as_ref())
    }
    fn validate_own_readback(&self, record: &ReplicaRecord) -> Result<()> {
        let Some(leader) = self.leader.as_ref().filter(|leader| leader.term == record.envelope.term) else { return Ok(()); };
        let resolved = matches!(&record.envelope.message, ReplicaBody::ResolvedTx(_));
        let Some((pending, _)) = leader.pending_tx_at(record.envelope.term_sequence) else {
            return if resolved { Err(readback_mismatch(replica_tx_id(record))) } else { Ok(()) };
        };
        if resolved && pending.append_stamp().and_then(|stamp| stamp.fingerprint)
            .zip(record.fingerprint).is_some_and(|(expected, observed)| expected == observed)
        {
            return Ok(());
        }
        let error = readback_mismatch(replica_tx_id(record));
        pending.finish(Err(error.clone()));
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
        if current { self.recovered_confirmation = None; }
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

    async fn apply_admitted(&mut self, record: AdmittedReplica) -> Result<()> {
        if matches!(&record.envelope.message, ReplicaBody::NoOp(message) if message.src_msg_id.is_none()) { return Ok(()); }
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
            let closes = match (&self.pending_block, &record.envelope.message) {
                (Some(pending), ReplicaBody::BlockUploaded(message)) => pending.matches_upload(message, self.logs.storage_version, self.logs.storage_epoch),
                _ => false,
            };
            if closes {
                if let Some(production) = self.leader.as_mut().and_then(|leader| leader.production.take()) {
                    match production { Production::Writing(future) => { future.await?; self.record_block_upload(); }, Production::Appending(future) => { future.await?; } }
                    if let Some(leader) = &mut self.leader { leader.confirmed = true; }
                }
                let ReplicaBody::BlockUploaded(message) = &record.envelope.message else { return Err(invalid("Matching upload changed type")); };
                let pending = self.pending_block.as_ref().ok_or_else(|| invalid("Matching upload has no pending block"))?;
                let (catalog, table_blocks) = self.blocks.load(pending.block_index()).await?;
                crate::blocks::validate_boundary(&catalog, &pending.prepared, &pending.boundary)?;
                validate_uploaded_root(&catalog, message, self.logs.requires_checkpoint_digest())?;
                let tries = additions(&message.tries)?;
                self.partition.adopt_block(pending.prepared.clone(), catalog, &table_blocks, &tries, record.metadata.timestamp)?;
                self.progress.applied(message.latest_processed_msg_id.map(SourcePosition), None, message.external_source_token.clone())?;
                let pending = self.pending_block.take().ok_or_else(|| invalid("Adopted block disappeared"))?;
                let mut held = pending.into_records();
                held.append(&mut work);
                work = held;
                adopted = true;
                if let Some(leader) = &mut self.leader {
                    leader.cycle.adopted();
                    complete_record(&mut leader.pending, record.envelope.term_sequence, Ok(Applied::Control));
                }
                if self.leader.is_some() && !tries.is_empty() {
                    self.logs.append_source(&SourceMessage::Protobuf(SourceBody::TriesAdded(proto::TriesAdded {
                        storage_version: Some(self.logs.storage_version.0), storage_epoch: Some(self.logs.storage_epoch.0),
                        tries: message.tries.clone(), source_msg_id: None,
                    })), (self.clock)()).await?;
                }
                self.publish_status(self.role());
                continue;
            }
            if let ReplicaBody::BlockUploaded(upload) = &record.envelope.message
                && let Some(installed) = self.partition.value().table_catalog().block()
                && matches_confirmation(installed, upload, self.logs.storage_version, self.logs.storage_epoch)
            {
                validate_uploaded_root(installed, upload, self.logs.requires_checkpoint_digest())?;
            }
            if self.pending_block.is_some() && self.upload_for_installed_block(&record) {
                self.complete_control(&record);
                continue;
            }
            if let Some(pending) = &mut self.pending_block { pending.hold(record)?; continue; }
            if self.stale(&record) { self.complete_control(&record); continue; }
            match &record.envelope.message {
                ReplicaBody::ResolvedTx(message) => {
                    let leading = self.leader.is_some();
                    let own = self.leader.as_ref().is_some_and(|leader| leader.term == record.envelope.term);
                    if own {
                        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
                        let head = leader.resolver_mut()?.take_head();
                        let (pending, expected) = leader.pending_tx_at(record.envelope.term_sequence).ok_or_else(|| readback_mismatch(message.tx_id))?;
                        let pending = Arc::clone(pending);
                        let expected = Arc::clone(expected);
                        let applied: Result<_> = async {
                            let head = head.filter(|head| Arc::ptr_eq(head, &expected)).ok_or_else(|| readback_mismatch(message.tx_id))?;
                            self.apply_transaction(head.writes(), message.search_delta.as_deref()).await?;
                            if head.result().committed() && let Some(operation) = head.db_op() {
                                self.effects.apply_db_op(operation).await?;
                            }
                            let result = Arc::new(head.result().clone());
                            self.progress.applied(Some(head.source_position()), Some(AppliedTransaction::new(Arc::clone(&result), head.placed_attempt())), head.external_token().map(<[u8]>::to_vec))?;
                            Ok(result)
                        }.await;
                        let result = match applied {
                            Ok(result) => result,
                            Err(error) => {
                                pending.finish(Err(error.clone()));
                                return Err(error);
                            }
                        };
                        if let Some(leader) = &mut self.leader {
                            complete_record(&mut leader.pending, record.envelope.term_sequence, Ok(Applied::Transaction(result)));
                            if message.acceptance_stage.is_some() && leader.cycle.is_full(self.config.rows_per_block) {
                                self.cut().await?;
                            }
                        }
                    } else {
                        if leading {
                            if let Some(operation) = &message.db_op { self.effects.apply_db_op(operation).await?; }
                        }
                        let value = self.partition.value();
                        let rtdb_tx::ReplayedTransaction { writes, result, placed_attempt } = rtdb_tx::decode_replica(message, value.live().latest_completed_tx(), value.live().block_index(), &self.resolve.memory)?;
                        self.apply_transaction(&writes, message.search_delta.as_deref()).await?;
                        if !leading && message.committed.unwrap_or(false) {
                            if let Some(operation) = &message.db_op {
                                if let Err(error) = self.effects.apply_db_op(operation).await { if !error.is_caller_error() { return Err(error); } }
                            }
                        }
                        let source = message.src_msg_id.map(SourcePosition).unwrap_or_else(|| if self.config.external_source { self.progress.current().source_position } else { SourcePosition(result.tx_key().tx_id.0) });
                        self.progress.applied(Some(source), Some(AppliedTransaction::new(Arc::new(result), placed_attempt)), message.external_source_token.clone())?;
                    }
                }
                ReplicaBody::TriesAdded(message) => {
                    if self.compaction_replay_cutoff.is_none_or(|cutoff| record.metadata.position > cutoff)
                        && message.storage_version == Some(self.logs.storage_version.0) && message.storage_epoch.unwrap_or_default() == self.logs.storage_epoch.0
                    {
                        self.partition.add_tries(&additions(&message.tries)?, record.metadata.timestamp)?;
                    }
                    self.progress.applied(message.source_msg_id.map(SourcePosition), None, None)?;
                    self.complete_control(&record);
                }
                ReplicaBody::ArtefactsAdded(message) => {
                    if message.storage_version == Some(self.logs.storage_version.0) && message.storage_epoch.unwrap_or_default() == self.logs.storage_epoch.0 {
                        if let Ok(table) = legacy_table_ref(message.table_name.as_deref().unwrap_or_default()) {
                            self.partition.add_artefacts_at(&table, artefact_entries(&message.entries), record.metadata.timestamp);
                        }
                    }
                    self.progress.applied(message.source_msg_id.map(SourcePosition), None, None)?;
                    self.complete_control(&record);
                }
                ReplicaBody::ArtefactsRemoved(message) => {
                    if message.storage_version == Some(self.logs.storage_version.0) && message.storage_epoch.unwrap_or_default() == self.logs.storage_epoch.0 {
                        if let Ok(table) = legacy_table_ref(message.table_name.as_deref().unwrap_or_default()) {
                            let entries = artefact_entries(&message.entries);
                            let slug = self.partition.value().table_catalog().entry(&table).map(|entry| entry.slug.clone());
                            self.partition.remove_artefacts(&table, entries.clone());
                            // The lane deletes the objects from the store next; this node's cached copies are its own to drop.
                            let keys = slug.iter().flat_map(|slug| entries.iter().filter_map(move |(trie, record)| slug.recorded_artefact_key(trie, record).ok()));
                            for key in keys.filter_map(|key| rtdb_storage::ObjectKey::new(key).ok()).collect::<BTreeSet<_>>() { let _ = self.resolve.storage.invalidate(&key).await; }
                        }
                    }
                    self.progress.applied(message.source_msg_id.map(SourcePosition), None, None)?;
                    self.complete_control(&record);
                }
                ReplicaBody::BlockBoundary(_) => {
                    let prepared = self.partition.prepare_block()?;
                    self.pending_block = Some(PendingBlock::new(ReplicaRecord::clone(&record), prepared, self.config.pending_block_records, self.config.pending_block_bytes)?);
                    if let Some(leader) = &mut self.leader { leader.cycle = BlockCycle::Cut; }
                    self.complete_control(&record);
                    if self.leader.is_some() { self.produce_pending()?; }
                }
                ReplicaBody::BlockUploaded(_) => return Err(invalid("Block upload has no matching pending boundary")),
                ReplicaBody::NoOp(message) => { self.progress.applied(message.src_msg_id.map(SourcePosition), None, None)?; self.complete_control(&record); }
                ReplicaBody::TriesDeleted(message) => {
                    if self.compaction_replay_cutoff.is_none_or(|cutoff| record.metadata.position > cutoff) {
                        let table = legacy_table_ref(message.table_name.as_deref().unwrap_or_default())?;
                        let keys = message.trie_keys.iter().map(|key| key.parse()).collect::<Result<BTreeSet<_>>>()?;
                        let cached = cached_objects(&self.partition.value(), &table, &keys);
                        let deletions = BTreeMap::from([(table, keys)]);
                        self.partition.delete_tries(&deletions);
                        // Every node holds its own cached copies, and the leader's delete reached only the remote store.
                        for key in cached { let _ = self.resolve.storage.invalidate(&key).await; }
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
                }
                ReplicaBody::TablesRetired(message) => {
                    let tables = message.table_names.iter().map(|name| legacy_table_ref(name)).collect::<Result<Vec<_>>>()?;
                    self.partition.retire_tables(tables, Timestamp(message.as_of_micros.unwrap_or_default()));
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
                leader.resolver = Some(TxResolver::new(progress.tx_key, progress.source_position, progress.external_token.as_deref().map(<[u8]>::to_vec), &self.resolve.memory)?);
                if !waiting_for_checkpoint && self.pending_block.is_none() && leader.source_tail.is_none() && leader.source_poll.is_none() {
                    leader.source_tail = Some(self.logs.source_tail(progress.source_position).await?);
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
        match &record.envelope.message {
            ReplicaBody::ResolvedTx(message) => self.leader.as_ref().is_none_or(|leader| leader.term != record.envelope.term)
                && message.tx_id.unwrap_or_default() <= progress.tx_id().0,
            ReplicaBody::TriesAdded(message) => message.source_msg_id.unwrap_or_default() <= progress.source_position.0,
            ReplicaBody::ArtefactsAdded(message) => message.source_msg_id.unwrap_or_default() <= progress.source_position.0,
            ReplicaBody::ArtefactsRemoved(message) => message.source_msg_id.unwrap_or_default() <= progress.source_position.0,
            ReplicaBody::BlockBoundary(message) => message.block_index.unwrap_or_default() <= block,
            ReplicaBody::BlockUploaded(message) => message.block_index.unwrap_or_default() <= block,
            ReplicaBody::NoOp(message) => message.src_msg_id.is_some_and(|source| source <= progress.source_position.0),
            ReplicaBody::TriesDeleted(_) | ReplicaBody::TablesRetired(_) | ReplicaBody::OversizedMessage(_) => false,
        }
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
        let databases = self.effects.secondary_databases()?;
        let blocks = Arc::clone(&self.blocks);
        let search = self.serving_search.clone();
        let leader = self.leader.as_mut().ok_or_else(|| invalid("Follower attempted block production"))?;
        if leader.production.is_some() { return Err(invalid("Block production is already running")); }
        let cancellation = leader.cancellation.clone();
        leader.production = Some(Production::Writing(Box::pin(async move {
            blocks.produce_with_search(&prepared, &boundary, databases, &cancellation, search.as_deref()).await
        })));
        Ok(())
    }
    async fn production_finished(&mut self, result: ProductionResult) -> Result<()> {
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        leader.production = None;
        let error = match &result { ProductionResult::Written(result) => result.as_ref().err(), ProductionResult::Appended(result) => result.as_ref().err() };
        if let Some(error) = error {
            let error = error.clone();
            if leader.confirmed { return Err(error); }
            return self.promotion_failed(error).await;
        }
        match result {
            ProductionResult::Written(result) => {
                let produced = result?;
                let upload_time = (self.clock)().0.div_euclid(1_000_000);
                self.status.send_modify(|status| status.last_block_upload_epoch_seconds = upload_time);
                let pending = self.pending_block.as_ref().ok_or_else(|| invalid("Produced block has no retained boundary"))?;
                let ReplicaBody::BlockBoundary(boundary) = &pending.boundary.envelope.message else { return Err(invalid("Pending boundary changed type")); };
                let message = ReplicaBody::BlockUploaded(proto::BlockUploaded {
                    storage_version: Some(self.logs.storage_version.0), storage_epoch: Some(self.logs.storage_epoch.0), block_index: Some(pending.block_index().0),
                    latest_processed_msg_id: boundary.latest_processed_msg_id, external_source_token: boundary.external_source_token.clone(),
                    catalog_sha256: Some(checkpoint_digest(&produced.catalog)?.to_vec()),
                    tries: produced.tries.iter().flat_map(|(table, tries)| tries.iter().map(|trie| trie.to_proto(table))).collect(),
                });
                let write = PendingWrite::new(WriteBody::Message(message), pending.block_index(), None, &self.resolve.memory)?;
                leader.pending.push_back(Arc::clone(&write));
                let appender = leader.appender.clone();
                leader.production = Some(Production::Appending(Box::pin(async move { appender.append_durable(write).await })));
            }
            ProductionResult::Appended(result) => { result?; leader.confirmed = true; self.publish_status(self.role()); }
        }
        Ok(())
    }
    async fn resolution_finished(&mut self, resolver: TxResolver, result: Result<Arc<ResolvedTx>>) -> Result<()> {
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        let mut resolution = leader.resolution.take().ok_or_else(|| invalid("Resolved transaction has no in-flight owner"))?;
        let external = resolution.completion.0.is_some();
        leader.resolver = Some(resolver);
        let held = std::mem::take(&mut leader.held);
        leader.held_bytes = 0;
        match result {
            Ok(resolved) => self.enqueue_resolved(resolved, resolution.completion.0.take()).await?,
            Err(error) => {
                if let Some(sender) = resolution.completion.0.take() { let _ = sender.send(Err(error.clone())); }
                if !external || !error.is_caller_error() { return Err(error); }
            }
        }
        for record in held { self.apply_admitted(record).await?; }
        let previous = self.installed;
        self.update_installed_prefix()?;
        if self.installed != previous { self.publish_status(self.role()); }
        Ok(())
    }
    async fn resolve_next_source(&mut self) -> Result<()> {
        let Some(leader) = &mut self.leader else { return Ok(()); };
        if leader.waiting_strict_source.is_some() {
            if !leader.ready_source() { return Ok(()); }
            let source = leader.waiting_strict_source.take().expect("checked for a waiting strict source");
            return self.begin_source_resolution(source);
        }
        let (LogRecord { metadata, payload, .. }, decode_reservation) = if let Some(head) = leader.source_head {
            let tail = leader.source_tail.as_deref_mut().ok_or_else(|| invalid("Prepared source head has no tail"))?;
            let Some(record) = read_replay_head(tail, head, &self.resolve.memory, source_decode_bound, &self.cancellation).await? else { return Ok(()); };
            leader.source_head = None;
            (record.raw, Some(record.decode_reservation))
        } else {
            let Some(record) = leader.source_batch.pop_front() else { return Ok(()); };
            (record, None)
        };
        let position = self.logs.source_position(metadata)?;
        let decode_charge = match decode_reservation {
            Some(reservation) => reservation,
            None => self.resolve.memory.reserve(source_decode_bound(payload.len())?)?,
        };
        let message = SourceMessage::decode(&payload)?;
        drop(payload);
        let durable = self.partition.snapshot();
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        if position <= leader.resolver()?.source_position() { return Err(invalid("Source reader did not advance")); }
        let block = durable.block_index();
        let resolved = match message {
            Some(message @ (SourceMessage::LegacyTransaction(_) | SourceMessage::Protobuf(SourceBody::Tx(_)))) => {
                if self.config.external_source { return Err(Anomaly::incorrect("xtdb/external-source-transaction", "Client transactions are disabled for an external-source database")); }
                let work = if self.config.skipped_transactions.contains(&position) {
                    SourceWork::Skip(message)
                } else {
                    SourceWork::Resolve(SourceTransaction::decode(&message, &self.resolve.memory)?)
                };
                let source = SourceInput { position, log_time: metadata.timestamp, work, _decode_charge: decode_charge };
                // Keep this position as the source head until every previously queued replica
                // write has been read back. In particular, none can still be offloading a body.
                if matches!(&source.work, SourceWork::Resolve(request) if request.requires_durable_predecessor())
                    && !leader.drained_for_strict_source()
                {
                    leader.waiting_strict_source = Some(source);
                } else {
                    self.begin_source_resolution(source)?;
                }
                None
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
                message.source_msg_id = Some(position.0);
                leader.enqueue(WriteBody::Message(ReplicaBody::TriesAdded(message)), block, None, &self.resolve.memory)?;
                leader.resolver_mut()?.advance_source(position)?;
                None
            }
            Some(SourceMessage::Protobuf(SourceBody::ArtefactsAdded(mut message))) => {
                let rejected = accept_artefacts(&mut message, &durable, self.logs.storage_version.0, self.logs.storage_epoch.0);
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
                leader.resolver_mut()?.advance_source(position)?;
                if message.expected_block_idx == Some(expected) { self.cut().await?; }
                else { leader.enqueue(WriteBody::Message(ReplicaBody::NoOp(proto::NoOp { src_msg_id: Some(position.0) })), block, None, &self.resolve.memory)?; }
                None
            }
            None => {
                leader.resolver_mut()?.advance_source(position)?;
                leader.enqueue(WriteBody::Message(ReplicaBody::NoOp(proto::NoOp { src_msg_id: Some(position.0) })), block, None, &self.resolve.memory)?;
                None
            }
        };
        if let Some(resolved) = resolved { self.enqueue_resolved(resolved, None).await?; }
        Ok(())
    }
    fn begin_source_resolution(&mut self, source: SourceInput) -> Result<()> {
        let durable = self.partition.snapshot();
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        let mut resolver = leader.resolver.take().ok_or_else(|| invalid("Source resolution has no idle resolver"))?;
        let context = Arc::clone(&self.resolve);
        let clock = Arc::clone(&self.clock);
        let maximum_inline_bytes = self.logs.replica_guarded_append_limit();
        let maximum_timed_original_bytes = self.logs.replica_timed_original_append_limit();
        leader.resolution = Some(Resolution { completion: Completion(None), future: Box::pin(async move {
            let SourceInput { position, log_time, work, _decode_charge } = source;
            let result = match work {
                SourceWork::Resolve(request) => resolver.resolve_source_for_writer(durable, position, log_time, request, &context,
                    Some(WriterPublication { clock: clock.as_ref(), maximum_inline_bytes, maximum_timed_original_bytes })).await,
                SourceWork::Skip(mut message) => async {
                    let attempt = SourceTransaction::skipped_attempt(&message, position)?;
                    if let SourceMessage::Protobuf(SourceBody::Tx(tx)) = &mut message {
                        tx.user_metadata = None;
                        tx.idempotency_guard = None;
                        tx.bounded_retry = None;
                        tx.replay_submission = None;
                        tx.placed_attempt_seed = None;
                    }
                    let digits = position.0.to_string();
                    let key = rtdb_storage::ObjectKey::new(format!("skipped-txs/{}{digits}", digits.len() - 1))?;
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
    async fn enqueue_resolved(&mut self, resolved: Arc<ResolvedTx>, completion: Option<oneshot::Sender<Result<Applied>>>) -> Result<()> {
        let strict = resolved.staging_clock_upper().is_some();
        let search_delta = if !strict && resolved.result().committed()
            && let Some(builder) = self.serving_search.as_ref().filter(|builder| builder.journals_deltas()) {
            let leader = self.leader.as_ref().ok_or_else(term_ended)?;
            let snapshot = leader.resolver()?.resolving_snapshot(self.partition.value().snapshot())?;
            builder.prepare_delta(snapshot, Arc::clone(resolved.writes()), &leader.cancellation).await?
        } else { None };
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        leader.cycle.resolved(resolved.writes().row_count()?)?;
        leader.enqueue(WriteBody::Resolved(resolved, search_delta), self.partition.value().live().block_index(), completion, &self.resolve.memory)?;
        if !strict && leader.cycle.is_full(self.config.rows_per_block) { self.cut().await?; }
        Ok(())
    }
    async fn cut(&mut self) -> Result<()> {
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
        self.deferred_cut = false;
        let block = self.partition.value().live().block_index();
        let leader = self.leader.as_mut().ok_or_else(term_ended)?;
        leader.cycle.cut()?;
        let boundary = proto::BlockBoundary { block_index: Some(block.0), latest_processed_msg_id: Some(leader.resolver()?.source_position().0), external_source_token: leader.resolver()?.external_token().map(<[u8]>::to_vec) };
        leader.enqueue(WriteBody::Message(ReplicaBody::BlockBoundary(boundary)), block, None, &self.resolve.memory)
    }
    async fn flush_if_due(&mut self) -> Result<()> {
        let persisted = self.partition.value().table_catalog().block().and_then(BlockCatalog::latest_completed_tx).map(|key| key.tx_id);
        if !self.waiting_for_checkpoint() && self.leader.as_ref().is_some_and(LeaderState::accepting_commands) && self.flush_timer.should_flush((self.clock)(), persisted) { self.cut().await?; }
        Ok(())
    }
    async fn command(&mut self, command: Command) -> Result<()> {
        let waiting_for_checkpoint = self.waiting_for_checkpoint();
        let Some(leader) = &mut self.leader else { command.fail(term_ended()); return Ok(()); };
        if leader.term != command.term() { command.fail(term_ended()); return Ok(()); }
        if waiting_for_checkpoint || !leader.accepting_commands() { leader.deferred_commands.push_back(command); return Ok(()); }
        match command {
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
                leader.resolution = Some(Resolution { completion: Completion(Some(completion)), future: Box::pin(async move {
                    let result = resolver.resolve_external(durable, clock(), system_time, token, writer.as_mut(), &context).await;
                    (resolver, result)
                }) });
                Ok(())
            }
        }
    }
}

fn additions(details: &[proto::TrieDetails]) -> Result<BTreeMap<TableRef, Vec<CatalogTrie>>> {
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
fn accept_artefacts(message: &mut proto::ArtefactsAdded, durable: &rtdb_index::Snapshot, version: i32, epoch: i32) -> Vec<rtdb_storage::ObjectKey> {
    let entries = std::mem::take(&mut message.entries);
    let Ok(table) = legacy_table_ref(message.table_name.as_deref().unwrap_or_default()) else { return Vec::new() };
    if message.storage_version != Some(version) || message.storage_epoch.unwrap_or_default() != epoch { return Vec::new(); }
    let slug = durable.table_entry(&table).map(|entry| entry.slug.clone());
    let catalog = durable.trie_catalog();
    let mut rejected = BTreeSet::new();
    for entry in entries {
        let (Ok(key), Some(record)) = (entry.trie_key.as_deref().unwrap_or_default().parse::<TrieKey>(), &entry.record) else { continue };
        if record.kind() == rtdb_format::search_manifest::RECORD_KIND {
            if key.level() == 0 && catalog.accepts_search_run(&table, record) { message.entries.push(entry); }
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
