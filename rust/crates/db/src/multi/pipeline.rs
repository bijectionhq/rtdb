use std::{
    collections::{BTreeSet, VecDeque},
    net::IpAddr,
    sync::Arc,
};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use journal_contract::{Epoch, JournalError, Lsn};
use rtdb_crypt::tokens::{TokenAuthority, VerifiedClaims};
use rtdb_format::journal::Record;
use rtdb_tx::transact::{
    COUNT_RANGE_CONSTRAINT, Decision, GROUPED_COUNT_CONSTRAINT, LINEARIZABLE, PHYSICAL_ERASURE, RELATIVE_VALID_TIME, Receipt, ScopedTransact, TIMED_TRANSACT, TRANSACT_REQUIRES, Terminal, Transact,
    TransactPipeline, TransactState, TransactionRecord, TxKey, ROW_CUSTODY, prepare_custody, workload,
};
use rtdb_index::branches::{BranchCut, store::{KeyRange, StoreWriterClaim}};
use rtdb_types::{Anomaly, AnomalyCategory, MemoryReservation, Result, Timestamp, rtdb_v1::{Basis, ClaimVerifier, LabelScope}};
use tokio::{
    sync::{Mutex, OnceCell, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

use crate::replay::JournalCoordinates;
use crate::journal::{PreparedTimedAppend, TimedOriginalProof};

use super::{
    Entry, Lease, MultiDatabase, PinnedExport, Role, StoreReadTurn, StoreVersionHoldKey, StoreVersionHoldReply,
    StoreVersionHoldRequest, Stream, Uuid, missing_receipt, next, no_writer,
    store_hold::StoreWatchDrain,
};
use super::timed_store::{TimedStoreAppend, TimedStoreReceipt};

const CAPACITY: usize = 64;
const FEATURES: &[&str] = &[LINEARIZABLE, TIMED_TRANSACT, TRANSACT_REQUIRES, RELATIVE_VALID_TIME, PHYSICAL_ERASURE, COUNT_RANGE_CONSTRAINT, GROUPED_COUNT_CONSTRAINT, "row-visibility", "branches",
    rtdb_types::rtdb_v1::RANKED_INDEX_DECLARATIONS];
const CUSTODY_FEATURES: &[&str] = &[
    LINEARIZABLE, TIMED_TRANSACT, TRANSACT_REQUIRES, RELATIVE_VALID_TIME, PHYSICAL_ERASURE,
    COUNT_RANGE_CONSTRAINT, GROUPED_COUNT_CONSTRAINT, "row-visibility", "branches",
    rtdb_types::rtdb_v1::RANKED_INDEX_DECLARATIONS, ROW_CUSTODY,
];
const SCOPED_FEATURES: &[&str] = &[LINEARIZABLE, TIMED_TRANSACT, TRANSACT_REQUIRES, PHYSICAL_ERASURE, "row-visibility"];
const MANAGED_APPOINTMENT_FEATURES: &[&str] = &[TIMED_TRANSACT, TRANSACT_REQUIRES, RELATIVE_VALID_TIME];
type Reply<T> = oneshot::Sender<Result<T>>;

enum Command {
    Transact(Work),
    TimedStore(Box<TimedStoreAppend>, Reply<TimedStoreReceipt>),
    Funded {
        transaction: Transact,
        scope: Option<Scope>,
        owner: [u8; 32],
        command: rtdb_tx::transact::FundingCommand,
        clock: Arc<dyn rtdb_tx::DecisionClock>,
        managed: Option<Box<workload::ManagedAppointment>>,
        reply: Reply<Receipt>,
    },
    Workload(workload::Request, Reply<workload::Reply>),
    Tick(BTreeSet<IpAddr>, Reply<Role>),
    Shutdown(Reply<()>),
    PublishRanked {
        build: super::RankedBuild,
        inventory: Vec<u8>,
        files: Vec<bytes::Bytes>,
        memory: Arc<rtdb_types::MemoryBudget>,
        reply: Reply<()>,
    },
    PublishStorePurgeFloor {
        since: Basis,
        keep_from: u64,
        retention_micros: i64,
        reads: Vec<KeyRange>,
        writer: Option<StoreWriterClaim>,
        request_memory: Arc<MemoryReservation>,
        reply: Reply<()>,
    },
    StoreVersionHold {
        reader: [u8; 32],
        request: StoreVersionHoldRequest,
        reply: Reply<StoreVersionHoldReply>,
    },
    EnterStoreWatch {
        after: Basis,
        reader: [u8; 32],
        hold: StoreVersionHoldKey,
        receiver: [u8; 16],
        id: [u8; 16],
        drain: StoreWatchDrain,
        reply: Reply<StoreReadTurn>,
    },
    FinishStoreWatch {
        key: StoreVersionHoldKey,
        reader: [u8; 32],
        receiver: [u8; 16],
    },
    BoundJournal { limit: u64, reply: Reply<Option<rtdb_format::journal::ObjectId>> },
    /// A deferred checkpoint built off the database lock, ready to announce.
    AnnounceCheckpoint {
        prepared: Result<super::deferred::PreparedCheckpoint>,
        reply: Reply<Option<rtdb_format::journal::ObjectId>>,
    },
    Export { pin: Uuid, basis: Basis, until: Option<Timestamp>, reply: Reply<Vec<u8>> },
    OpenExport { ticket: Vec<u8>, reply: Reply<PinnedExport> },
    ReleaseExport { ticket: Vec<u8>, reply: Reply<()> },
    Restore { export: PinnedExport, reply: Reply<BranchCut> },
    Retire(Reply<super::DeploymentRetirement>),
}

struct Scope {
    labels: LabelScope,
    namespace: [u8; 32],
}

impl Scope {
    fn authority(&self) -> ScopedTransact<'_> {
        ScopedTransact {
            labels: &self.labels,
            receipt_namespace: self.namespace,
        }
    }
}

struct Work {
    request: Transact,
    scope: Option<Scope>,
    claims: VerifiedClaims,
    reply: Reply<Receipt>,
}

impl Work {
    fn key(&self) -> TxKey {
        TxKey {
            namespace: self.request.receipt_namespace(self.scope.as_ref().map(|scope| scope.namespace)),
            key: self.request.key.clone(),
        }
    }

    fn batchable(&self) -> bool {
        !self.request.uses_erasure() && !self.request.validates_only()
    }

    fn timed(&self) -> bool {
        self.request.window.before.is_some()
    }
}

pub struct WritePipeline {
    input: mpsc::Sender<Command>,
    task: JoinHandle<()>,
    database: Arc<Mutex<MultiDatabase>>,
    tokens: OnceCell<TokenAuthority>,
}

impl WritePipeline {
    pub async fn store_version_hold(
        &self, reader: [u8; 32], request: StoreVersionHoldRequest,
    ) -> Result<StoreVersionHoldReply> {
        self.request(|reply| Command::StoreVersionHold { reader, request, reply }).await
    }

    pub async fn enter_held_store_watch(
        &self, after: Basis, reader: [u8; 32], hold: StoreVersionHoldKey, receiver: [u8; 16],
    ) -> Result<StoreReadTurn> {
        let id = *Uuid::new_v4().as_bytes();
        let key = StoreVersionHoldKey { basis: hold.basis, id };
        let input = self.input.clone();
        let runtime = tokio::runtime::Handle::current();
        let drain = StoreWatchDrain::new(move || {
            runtime.spawn(async move {
                let _ = input.send(Command::FinishStoreWatch { key, reader, receiver }).await;
            });
        });
        // The queue owns the drain before attempting the append. A lost
        // append acknowledgement or a cancelled caller cannot lose it.
        self.request(|reply| Command::EnterStoreWatch { after, reader, hold, receiver, id, drain, reply }).await
    }

    /// Keeps an admitted publication in the writer's queue after the caller drops its reply.
    pub async fn publish_store_purge_floor(
        &self, since: Basis, keep_from: u64, retention_micros: i64, reads: Vec<KeyRange>, writer: Option<StoreWriterClaim>,
        request_memory: Arc<MemoryReservation>,
    ) -> Result<()> {
        self.request(|reply| Command::PublishStorePurgeFloor { since, keep_from, retention_micros, reads, writer, request_memory, reply }).await
    }

    pub async fn publish_ranked_catalog(
        &self, build: super::RankedBuild, inventory: Vec<u8>, files: Vec<bytes::Bytes>,
        memory: Arc<rtdb_types::MemoryBudget>,
    ) -> Result<()> {
        self.request(|reply| Command::PublishRanked { build, inventory, files, memory, reply }).await
    }

    /// Checkpoints between ordered writes once the retained journal suffix
    /// reaches `limit` records.
    pub async fn bound_journal(&self, limit: u64) -> Result<Option<rtdb_format::journal::ObjectId>> {
        self.request(|reply| Command::BoundJournal { limit, reply }).await
    }

    pub fn start(database: Arc<Mutex<MultiDatabase>>) -> Self {
        let (input, receive) = mpsc::channel(CAPACITY);
        let task = tokio::spawn(run(Arc::clone(&database), receive, input.downgrade()));
        Self { input, task, database, tokens: OnceCell::new() }
    }

    pub async fn transact(&self, request: Transact) -> Result<Receipt> {
        self.submit(request, None).await
    }

    /// Orders a Kernel-authorized Store effect as one writer turn. The callback
    /// signs the prepared commitment and returns its companion append without
    /// calling back into this pipeline.
    pub async fn append_timed_store(&self, request: TimedStoreAppend) -> Result<TimedStoreReceipt> {
        self.request(|reply| Command::TimedStore(Box::new(request), reply)).await
    }

    pub async fn transact_scoped(
        &self,
        request: Transact,
        labels: LabelScope,
        receipt_namespace: [u8; 32],
    ) -> Result<Receipt> {
        self.submit(
            request,
            Some(Scope {
                labels,
                namespace: receipt_namespace,
            }),
        )
        .await
    }

    async fn submit(&self, request: Transact, scope: Option<Scope>) -> Result<Receipt> {
        let tokens = self
            .tokens
            .get_or_init(|| async { self.database.lock().await.token_authority() })
            .await;
        let claims = tokens
            .verify_claims_in(request.claims(), scope.as_ref().map(|scope| &scope.labels))
            .await;
        let (reply, result) = oneshot::channel();
        self.input
            .send(Command::Transact(Work {
                request,
                scope,
                claims,
                reply,
            }))
            .await
            .map_err(|_| pipeline_closed())?;
        result.await.map_err(|_| pipeline_closed())?
    }

    pub async fn workload(&self, request: workload::Request) -> Result<workload::Reply> {
        self.request(|reply| Command::Workload(request, reply)).await
    }

    pub async fn transact_funded(
        &self, transaction: Transact, scope: Option<(LabelScope, [u8; 32])>, owner: [u8; 32],
        command: rtdb_tx::transact::FundingCommand, clock: Arc<dyn rtdb_tx::DecisionClock>,
    ) -> Result<Receipt> {
        let scope = scope.map(|(labels, namespace)| Scope { labels, namespace });
        self.request(|reply| Command::Funded { transaction, scope, owner, command, clock, managed: None, reply }).await
    }

    pub async fn transact_managed_appointment(
        &self, transaction: Transact, appointment: workload::ManagedAppointment,
        command: rtdb_tx::transact::AppointmentCommand, clock: Arc<dyn rtdb_tx::DecisionClock>,
    ) -> Result<Receipt> {
        self.request(|reply| Command::Funded {
            transaction, scope: None, owner: appointment.namespace(),
            command: rtdb_tx::transact::FundingCommand::Appointment(command), clock,
            managed: Some(Box::new(appointment)), reply,
        }).await
    }

    pub async fn tick(&self, live: BTreeSet<IpAddr>) -> Result<Role> {
        self.request(|reply| Command::Tick(live, reply)).await
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.request(Command::Shutdown).await
    }

    pub async fn export(&self, pin: Uuid, basis: Basis, until: Option<Timestamp>) -> Result<Vec<u8>> {
        self.request(|reply| Command::Export { pin, basis, until, reply }).await
    }

    pub async fn open_export(&self, ticket: Vec<u8>) -> Result<PinnedExport> {
        self.request(|reply| Command::OpenExport { ticket, reply }).await
    }

    pub async fn release_export(&self, ticket: Vec<u8>) -> Result<()> {
        self.request(|reply| Command::ReleaseExport { ticket, reply }).await
    }

    pub async fn restore(&self, export: PinnedExport) -> Result<BranchCut> {
        self.request(|reply| Command::Restore { export, reply }).await
    }

    pub async fn retire_deployment(&self) -> Result<super::DeploymentRetirement> {
        self.request(Command::Retire).await
    }

    async fn request<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> Result<T> {
        let (reply, result) = oneshot::channel();
        self.input.send(command(reply)).await.map_err(|_| pipeline_closed())?;
        result.await.map_err(|_| pipeline_closed())?
    }
}

impl Drop for WritePipeline {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Staged {
    work: Work,
    write: Append,
    original_timed: bool,
}

pub(super) struct Append {
    pub(super) at: Lsn,
    pub(super) entry: Entry,
    pub(super) admit_before: Option<Timestamp>,
}

pub(super) struct Authority {
    stream: Stream,
    clock: Arc<dyn Fn() -> Timestamp + Send + Sync>,
    drift_micros: i64,
    pub(super) lease: Lease,
    next: Lsn,
}

impl Authority {
    async fn append_funded(
        &self, write: &Append, decision_before: Timestamp, cutoff: rtdb_types::TimedJournalCutoff,
        managed: Option<(&workload::ManagedAppointment, &TransactState)>,
    ) -> std::result::Result<[u8; 32], AttemptError> {
        self.stream.batch.as_ref().filter(|batch| batch.maximum_timed_original_bytes().is_some())
            .ok_or_else(|| AttemptError::Local(rtdb_tx::transact::funding_unsupported()))?;
        if self.next != write.at || self.lease.epoch != write.entry.epoch {
            return Err(AttemptError::Local(order_error("The funded append must follow the acknowledged prefix under the active writer epoch")));
        }
        let prepared = self.stream.prepare_timed_append(write.at, &write.entry, cutoff).await.map_err(AttemptError::Local)?;
        let upper = self.upper();
        if let Some((appointment, state)) = managed {
            appointment.check_current(state, upper).map_err(AttemptError::Local)?;
        }
        if write.admit_before.is_some_and(|expiry| upper >= expiry || cutoff.before() > expiry) {
            return Err(AttemptError::Local(workload::denied()));
        }
        if upper >= decision_before || cutoff.before().0 > self.lease.until {
            return Err(AttemptError::Local(Anomaly::new(AnomalyCategory::Conflict,
                "rtdb/window-expired", "The original timed append missed its admission bounds")));
        }
        Ok(self.dispatch_timed(prepared, decision_before).await?.original.payload_sha256)
    }

    pub(super) async fn dispatch_timed(
        &self,
        prepared: PreparedTimedAppend,
        decision_before: Timestamp,
    ) -> std::result::Result<TimedOriginalProof, AttemptError> {
        let original = prepared.commitment();
        if self.next != original.at || self.lease.epoch != original.epoch {
            return Err(AttemptError::Local(order_error("The timed append no longer follows its writer's acknowledged prefix")));
        }
        self.check_timed_window(decision_before, original.cutoff).map_err(AttemptError::Local)?;
        let proof = self.stream.append_timed_original(prepared, || self.upper() < decision_before)
            .await.map_err(AttemptError::Journal)?;
        proof.verify_exact(original).map_err(AttemptError::Local)?;
        Ok(proof)
    }

    pub(super) fn check_timed_window(&self, decision_before: Timestamp, cutoff: rtdb_types::TimedJournalCutoff) -> Result<()> {
        if cutoff.before().0 > self.lease.until || self.upper() >= decision_before {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb/window-expired", "The original timed append missed its admission bounds"));
        }
        Ok(())
    }

    pub(super) fn upper(&self) -> Timestamp {
        Timestamp((self.clock)().0.saturating_add(self.drift_micros))
    }

    fn limit(&self) -> usize {
        if self.stream.batch.is_some() {
            CAPACITY
        } else {
            1
        }
    }

    /// Returns the clock bound after the append and the digest of the last
    /// record's bytes.
    pub(super) async fn append(
        &self,
        writes: &[Append],
    ) -> std::result::Result<(Timestamp, [u8; 32]), AttemptError> {
        let mut records = Vec::with_capacity(writes.len());
        for (offset, write) in writes.iter().enumerate() {
            if matches!(&write.entry.record, Record::Tx { payload, .. } if TransactionRecord::is_timed_store_record(payload)) {
                return Err(AttemptError::Local(order_error("A timed Store record requires original quorum timing")));
            }
            if self.next.0.checked_add(offset as u64) != Some(write.at.0) {
                return Err(AttemptError::Local(order_error(
                    "The append group must follow the acknowledged prefix",
                )));
            }
            let bytes = self
                .stream
                .encode(write.at, &write.entry)
                .await
                .map_err(AttemptError::Local)?;
            records.push((write.at, bytes));
        }
        let upper = self.upper();
        if upper.0 >= self.lease.until {
            return Err(AttemptError::Local(no_writer()));
        }
        if writes.iter().any(|write| write.admit_before.is_some_and(|cutoff| upper >= cutoff)) {
            return Err(AttemptError::Local(workload::denied()));
        }
        let expected = writes.last().expect("nonempty append group").at;
        let tail = Sha256::digest(&records.last().expect("nonempty append group").1).into();
        let appended = self
            .stream
            .append_group(self.lease.epoch, records)
            .await
            .map_err(AttemptError::Journal)?;
        let upper = self.upper();
        if appended != expected {
            return Err(AttemptError::Local(order_error(
                "The journal acknowledged a different group head",
            )));
        }
        Ok((upper, tail))
    }
}

pub(super) enum AttemptError {
    Local(Anomaly),
    Journal(JournalError),
}

async fn run(
    database: Arc<Mutex<MultiDatabase>>,
    mut input: mpsc::Receiver<Command>,
    announcements: mpsc::WeakSender<Command>,
) {
    let mut barrier = None;
    let mut build = JoinSet::new();
    loop {
        let command = match barrier.take() {
            Some(command) => command,
            None => match input.recv().await {
                Some(command) => command,
                None => return,
            },
        };
        match command {
            Command::TimedStore(request, reply) => {
                let result = Box::pin(async { database.lock().await.append_timed_store_serial(*request).await }).await;
                let _ = reply.send(result);
            }
            Command::Transact(work) if work.batchable() => {
                Box::pin(burst(&database, &mut input, &mut barrier, work)).await;
            }
            Command::Transact(work) => {
                let result = Box::pin(async {
                    database
                        .lock()
                        .await
                        .transact_in(&work.request, work.scope.as_ref().map(Scope::authority))
                        .await
                })
                .await;
                let _ = work.reply.send(result);
            }
            Command::Workload(request, reply) => {
                let result = Box::pin(async { database.lock().await.managed_workload(request).await }).await;
                let _ = reply.send(result);
            }
            Command::Funded { transaction, scope, owner, command, clock, managed, reply } => {
                let result = Box::pin(async {
                    database.lock().await.transact_funded_serial(
                        &transaction, scope.as_ref().map(Scope::authority), owner, command, clock.as_ref(), managed.as_deref(),
                    ).await
                }).await;
                let _ = reply.send(result);
            }
            Command::Tick(live, reply) => {
                let result = Box::pin(async { database.lock().await.tick(&live).await }).await;
                let _ = reply.send(result);
            }
            Command::Shutdown(reply) => {
                input.close();
                build.shutdown().await;
                let result = Box::pin(async { database.lock().await.shutdown_writer().await }).await;
                let _ = reply.send(result);
                return;
            }
            Command::PublishRanked { build, inventory, files, memory, reply } => {
                let result = Box::pin(async { database.lock().await.publish_ranked_catalog(build, inventory, files, &memory).await }).await;
                let _ = reply.send(result);
            }
            Command::PublishStorePurgeFloor { since, keep_from, retention_micros, reads, writer, request_memory: _request_memory, reply } => {
                let result = Box::pin(async {
                    database.lock().await.publish_store_purge_floor(since, keep_from, retention_micros, &reads, writer).await
                }).await;
                let _ = reply.send(result);
            }
            Command::StoreVersionHold { reader, request, reply } => {
                let result = Box::pin(async { database.lock().await.store_version_hold(reader, request).await }).await;
                let _ = reply.send(result);
            }
            Command::EnterStoreWatch { after, reader, hold, receiver, id, drain, reply } => {
                let result = Box::pin(async {
                    database.lock().await.enter_held_store_watch(after, reader, hold, id, receiver).await
                }).await.map(|mut turn| {
                    turn.settle_watch_on_drop(drain);
                    turn
                });
                let _ = reply.send(result);
            }
            Command::FinishStoreWatch { key, reader, receiver } => {
                let result = Box::pin(async {
                    database.lock().await.finish_held_store_watch(key, reader, receiver).await
                }).await;
                if let Err(error) = result {
                    tracing::warn!(code = %error.code, "An ordered Store watch turn still needs settlement");
                }
            }
            Command::BoundJournal { limit, reply } => {
                let mut guard = database.lock().await;
                if !guard.deferred_checkpoints() {
                    let result = Box::pin(guard.checkpoint_retained_suffix(limit)).await;
                    let result = match result {
                        Ok(checkpoint) => guard.reclaim_store_checkpoint_manifests().await.map(|()| checkpoint),
                        Err(error) => Err(error),
                    };
                    answer_checkpoint(reply, result);
                } else if !build.is_empty() {
                    answer_checkpoint(reply, Ok(None));
                } else {
                    match Box::pin(guard.capture_checkpoint(limit)).await {
                        Ok(Some(capture)) => {
                            let builder = guard.checkpoint_builder();
                            drop(guard);
                            let announcements = announcements.clone();
                            build.spawn(async move {
                                // A failed build still reaches the pipeline, which then
                                // accepts the next capture.
                                let mut task = JoinSet::new();
                                task.spawn(async move { builder.build(capture).await });
                                let prepared = task.join_next().await.expect("one checkpoint build")
                                    .unwrap_or_else(|error| Err(Anomaly::fault("rtdb/checkpoint-build", error.to_string())));
                                let stopped = match announcements.upgrade() {
                                    Some(input) => input.send(Command::AnnounceCheckpoint { prepared, reply }).await.is_err(),
                                    None => true,
                                };
                                if stopped {
                                    tracing::warn!("The writer pipeline stopped before announcing a built checkpoint; its manifest is collected as an orphan");
                                }
                            });
                        }
                        Ok(None) => answer_checkpoint(reply, guard.reclaim_store_checkpoint_manifests().await.map(|()| None)),
                        Err(error) => answer_checkpoint(reply, Err(error)),
                    }
                }
            }
            Command::AnnounceCheckpoint { prepared, reply } => {
                build.join_next().await.expect("a build posted its announcement").expect("checkpoint supervisor completed");
                let result = match prepared {
                    Ok(prepared) => Box::pin(async {
                        let mut database = database.lock().await;
                        let checkpoint = database.announce_checkpoint(prepared).await?;
                        database.reclaim_store_checkpoint_manifests().await?;
                        Ok(checkpoint.manifest())
                    }).await,
                    Err(error) => Err(error),
                };
                answer_checkpoint(reply, result);
            }
            Command::Export { pin, basis, until, reply } => {
                let result = Box::pin(async { database.lock().await.export_ticket(pin, basis, until).await }).await;
                let _ = reply.send(result);
            }
            Command::OpenExport { ticket, reply } => {
                let result = Box::pin(async { database.lock().await.open_export_ticket(&ticket).await }).await;
                let _ = reply.send(result);
            }
            Command::ReleaseExport { ticket, reply } => {
                let result = Box::pin(async { database.lock().await.release_export_ticket(&ticket).await }).await;
                let _ = reply.send(result);
            }
            Command::Restore { export, reply } => {
                let result = Box::pin(async { database.lock().await.restore_export(&export).await }).await;
                let _ = reply.send(result);
            }
            Command::Retire(reply) => {
                let result = Box::pin(async { database.lock().await.retire_deployment().await }).await;
                let _ = reply.send(result);
            }
        }
    }
}

/// Answers a journal bound check. A deferred build outlives the request that
/// started it, so a failure whose caller stopped waiting is logged, not lost.
fn answer_checkpoint(reply: Reply<Option<rtdb_format::journal::ObjectId>>, result: Result<Option<rtdb_format::journal::ObjectId>>) {
    if let Err(Err(error)) = reply.send(result) {
        tracing::warn!(code = %error.code, message = error.message.as_deref().unwrap_or(""), "A checkpoint failed with no caller left to report it to");
    }
}

async fn burst(
    database: &Mutex<MultiDatabase>,
    input: &mut mpsc::Receiver<Command>,
    barrier: &mut Option<Command>,
    first: Work,
) {
    let mut queued = vec![first];
    fill(input, barrier, &mut queued, CAPACITY);
    let mut staged = VecDeque::new();
    stage_batch(&mut *database.lock().await, &mut staged, queued).await;
    let mut input_open = true;
    while let Some(front) = staged.front() {
        let authority = database
            .lock()
            .await
            .append_authority(front.write.entry.epoch);
        let authority = match authority {
            Ok(authority) => authority,
            Err(error) => {
                fail(
                    database,
                    &mut staged,
                    Vec::new(),
                    AttemptError::Local(error),
                )
                .await;
                return;
            }
        };
        let mut writes = Vec::new();
        let mut waiters = Vec::new();
        let mut position = front.write.at;
        while writes.len() < authority.limit()
            && staged.front().is_some_and(|next| next.write.at == position)
        {
            let Staged {
                work,
                write,
                original_timed,
            } = staged.pop_front().expect("checked queue");
            position = Lsn(write.at.0.saturating_add(1));
            writes.push(write);
            waiters.push((work, original_timed));
            if original_timed {
                break;
            }
        }
        let result = {
            let append = authority.append(&writes);
            tokio::pin!(append);
            loop {
                let occupied = staged.len() + writes.len();
                tokio::select! {
                    result = &mut append => break result,
                    command = input.recv(), if input_open && barrier.is_none() && occupied < CAPACITY => {
                        match command {
                            Some(Command::Transact(work)) if work.batchable() => {
                                let mut group = vec![work];
                                fill(input, barrier, &mut group, CAPACITY - occupied);
                                stage_batch(&mut *database.lock().await, &mut staged, group).await;
                            }
                            Some(command) => *barrier = Some(command),
                            None => input_open = false,
                        }
                    }
                }
            }
        };
        let (upper, tail) = match result {
            Ok(appended) => appended,
            Err(error) => {
                fail(database, &mut staged, waiters, error).await;
                return;
            }
        };
        let positions: Vec<_> = writes.iter().map(|write| write.at).collect();
        let mut database_guard = database.lock().await;
        let mut receipts = match database_guard.publish_staged(authority.lease, writes, tail).await {
            Ok(receipts) => receipts.into_iter(),
            Err(error) => {
                drop(database_guard);
                fail(database, &mut staged, waiters, AttemptError::Local(error)).await;
                return;
            }
        };
        for ((work, original_timed), position) in waiters.into_iter().zip(positions) {
            if original_timed {
                let result = async {
                    let (write, redecide) = database_guard.decide_terminal(authority.lease, position, upper)?;
                    let queued = take_redecisions(&mut staged, &redecide)?;
                    stage_batch(&mut database_guard, &mut staged, queued).await;
                    Ok(write)
                }.await;
                match result {
                    Ok(write) => staged.push_front(Staged {
                        work,
                        write,
                        original_timed: false,
                    }),
                    Err(error) => {
                        drop(database_guard);
                        fail(
                            database,
                            &mut staged,
                            vec![(work, false)],
                            AttemptError::Local(error),
                        )
                        .await;
                        return;
                    }
                }
            } else {
                let result = match receipts.next() {
                    Some(receipt)
                        if database_guard.state().coordinates.lsn(receipt.basis.position()) == Ok(position)
                            && receipt.key == work.key().key =>
                    {
                        database_guard
                            .project_receipt(receipt, work.scope.as_ref().map(Scope::authority))
                    }
                    _ => Err(missing_receipt()),
                };
                let _ = work.reply.send(result);
            }
        }
    }
}

fn take_redecisions(staged: &mut VecDeque<Staged>, keys: &[TxKey]) -> Result<Vec<Work>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    if !staged
        .iter()
        .map(|item| item.work.key())
        .eq(keys.iter().cloned())
    {
        return Err(order_error(
            "The refused timed write returned a different queued suffix",
        ));
    }
    Ok(staged.drain(..).map(|item| item.work).collect())
}

fn fill(
    input: &mut mpsc::Receiver<Command>,
    barrier: &mut Option<Command>,
    queued: &mut Vec<Work>,
    limit: usize,
) {
    while barrier.is_none() && queued.len() < limit {
        match input.try_recv() {
            Ok(Command::Transact(work)) if work.batchable() => queued.push(work),
            Ok(command) => *barrier = Some(command),
            Err(_) => break,
        }
    }
}

async fn stage_batch(database: &mut MultiDatabase, staged: &mut VecDeque<Staged>, queued: Vec<Work>) {
    if database.stream.batch.is_some()
        && queued.len() > 1
        && queued
            .iter()
            .all(|work| work.scope.is_none() && !work.timed())
    {
        let requests: Vec<_> = queued.iter().map(|work| work.request.clone()).collect();
        let claims = VerifiedClaims::union(queued.iter().map(|work| &work.claims));
        match database.decide_group(&requests, &claims).await {
            Ok(decisions) => {
                for (work, decision) in queued.into_iter().zip(decisions) {
                    stage(database, staged, work, decision);
                }
            }
            Err(error) => {
                for work in queued {
                    let _ = work.reply.send(Err(error.clone()));
                }
            }
        }
    } else {
        for work in queued {
            let decision = database.decide_one(
                &work.request,
                work.scope.as_ref().map(Scope::authority),
                &work.claims,
            ).await;
            stage(database, staged, work, decision);
        }
    }
}

fn stage(
    database: &MultiDatabase,
    staged: &mut VecDeque<Staged>,
    work: Work,
    decision: Result<Planned>,
) {
    match decision {
        Ok(Planned::Append(write)) => staged.push_back(Staged {
            original_timed: work.timed(),
            work,
            write,
        }),
        Ok(Planned::Answer(receipt)) => {
            let receipt =
                database.project_receipt(receipt, work.scope.as_ref().map(Scope::authority));
            let _ = work.reply.send(receipt);
        }
        Err(error) => {
            let _ = work.reply.send(Err(error));
        }
    }
}

async fn fail(
    database: &Mutex<MultiDatabase>,
    staged: &mut VecDeque<Staged>,
    waiters: Vec<(Work, bool)>,
    error: AttemptError,
) {
    let error = database.lock().await.begin_write_recovery(error);
    for (work, _) in waiters {
        let _ = work.reply.send(Err(error.clone()));
    }
    for Staged { work, .. } in staged.drain(..) {
        let _ = work.reply.send(Err(Anomaly::new(
            AnomalyCategory::Interrupted,
            "rtdb/speculative-write-withdrawn",
            "An earlier append failed. Recover the write outcome with TxStatus",
        )));
    }
    let mut database = database.lock().await;
    if database.writer.is_some() {
        database.recover_after_failure().await;
    } else if let Err(error) = database.refresh().await {
        tracing::warn!(database = %database.options.database, code = %error.code, "Refresh after a failed write did not finish; the next read retries it");
    }
}

pub(super) enum Planned {
    Answer(Receipt),
    Append(Append),
}

impl MultiDatabase {
    pub fn supports_funded_transact(&self) -> bool {
        !self.activation_required() && self.authority().is_ok()
            && self.stream.batch.as_ref().and_then(|batch| batch.maximum_timed_original_bytes()).is_some()
    }

    pub fn supports_funded_appointment(&self) -> bool {
        !self.activation_required() && self.authority().is_ok()
            && self.stream.batch.as_ref().and_then(|batch| batch.maximum_timed_original_bytes()).is_some_and(|limit| limit > 1024)
    }

    pub async fn managed_appointment_ready(&mut self, appointment: &workload::ManagedAppointment) -> Result<bool> {
        self.refresh().await?;
        self.require_active()?;
        let lease = self.authority()?;
        self.check_managed_appointment(lease.epoch, appointment, Timestamp::MIN)?;
        Ok(self.supports_funded_appointment())
    }

    pub async fn managed_appointment_status(&mut self, appointment: &workload::ManagedAppointment, key: &[u8]) -> Result<Receipt> {
        appointment.check_key(key)?;
        self.refresh().await?;
        self.require_active()?;
        let lease = self.authority()?;
        self.check_managed_appointment(lease.epoch, appointment, Timestamp::MIN)?;
        appointment.status(self.durable_transactions(), key)
    }

    fn check_managed_appointment(&self, epoch: Epoch, appointment: &workload::ManagedAppointment, at: Timestamp) -> Result<()> {
        let state = self.durable_transactions();
        let now = self.append_authority(epoch)?.upper().max(state.index().head().system_time.next()?).max(at);
        appointment.check_current(state, now)
    }

    async fn transact_funded_serial(
        &mut self, request: &Transact, scope: Option<ScopedTransact<'_>>, owner: [u8; 32],
        command: rtdb_tx::transact::FundingCommand, clock: &dyn rtdb_tx::DecisionClock,
        managed: Option<&workload::ManagedAppointment>,
    ) -> Result<Receipt> {
        if self.activation_required() {
            return Err(Anomaly::unsupported(
                "rtdb/activation-effects",
                "Funded dispatch requires an effect provider commissioned for database activation",
            ));
        }
        self.refresh().await?;
        self.require_active()?;
        if command.is_appointment() && !self.supports_funded_appointment() {
            return Err(rtdb_tx::transact::appointment_unavailable());
        }
        if !self.supports_funded_transact() || !clock.supports_funded_offers() {
            return Err(rtdb_tx::transact::funding_unsupported());
        }
        self.require_armed_rotation()?;
        let record_limit = self.stream.batch.as_ref().and_then(|batch| batch.maximum_timed_original_bytes())
            .ok_or_else(rtdb_tx::transact::funding_unsupported)?.saturating_sub(1024);
        let (mut funding, claims) = if let Some(appointment) = managed {
            let rtdb_tx::transact::FundingCommand::Appointment(command) = command else { return Err(workload::denied()); };
            let lease = self.authority()?;
            self.check_managed_appointment(lease.epoch, appointment, Timestamp::MIN)?;
            appointment.check_request(self.durable_transactions(), request, command)?;
            (rtdb_tx::transact::FundingInput::managed_appointment(command, appointment),
                self.tokens.verify_workload_claims(request.claims(), appointment.claim_binding()?).await)
        } else {
            (rtdb_tx::transact::FundingInput::new(command, owner),
                self.tokens.verify_claims_in(request.claims(), scope.map(|scope| scope.labels)).await)
        };
        funding.verify(request, self.durable_transactions(), scope, clock).await;
        let lease = self.authority()?;
        if let Some(appointment) = managed {
            self.check_managed_appointment(lease.epoch, appointment, funding.appointment_observed_at().unwrap_or(Timestamp::MIN))?;
        }
        let generation = self.state().generation;
        let coordinates = self.state().coordinates;
        let now = (self.clock)();
        let features = if managed.is_some() { MANAGED_APPOINTMENT_FEATURES }
            else if scope.is_some() { SCOPED_FEATURES }
            else { self.write_features() };
        let decision = self.writer.as_mut().ok_or_else(no_writer)?.pipeline
            .decide_funded(request, now, features, &claims, scope, funding, record_limit)?;
        let mut write = match plan(decision, lease, generation, coordinates)? {
            Planned::Answer(receipt) => return self.project_receipt(receipt, scope),
            Planned::Append(write) => write,
        };
        write.admit_before = managed.map(workload::ManagedAppointment::expires_at);
        let at = write.at;
        let funded = match &write.entry.record {
            Record::Tx { payload, .. } => match TransactionRecord::decode(payload)? {
                TransactionRecord::Tx(tx) => tx.funded_guard()?,
                _ => return Err(order_error("The funded command must append one resolved transaction")),
            },
            _ => return Err(order_error("The funded command must append one transaction envelope")),
        };
        let receipts = if let Some((_, decision_before, cutoff)) = funded {
            let authority = self.append_authority(lease.epoch)?;
            let admission = managed.map(|appointment| (appointment, self.durable_transactions()));
            let tail = match authority.append_funded(&write, decision_before, cutoff, admission).await {
                Ok(tail) => tail,
                Err(error) => {
                    let error = self.begin_write_recovery(error);
                    self.recover_after_failure().await;
                    return Err(error);
                }
            };
            self.publish_staged(lease, vec![write], tail).await
                .map_err(|error| self.begin_write_recovery(AttemptError::Local(error)))?
        } else {
            self.commit_one(lease, write).await?.1
        };
        self.project_receipt(receipt_for(receipts, coordinates.position(at)?, &request.key)?, scope)
    }

    pub fn write_features(&self) -> &'static [&'static str] {
        if self.supports_row_custody() { CUSTODY_FEATURES } else { FEATURES }
    }

    pub(super) fn new_pipeline(&self) -> Result<TransactPipeline> {
        let state = self.state();
        let durable = &state.transactions;
        let copy = TransactState::decode_checkpoint(
            &durable.encode_checkpoint()?,
            durable.index().head(),
        )?;
        TransactPipeline::new(copy, state.coordinates.position(next(state.head)?)?, CAPACITY)
    }

    pub(super) async fn decide_one(
        &mut self,
        request: &Transact,
        scope: Option<ScopedTransact<'_>>,
        claims: &dyn ClaimVerifier,
    ) -> Result<Planned> {
        let now = self.sample_ordering_clock(request).await;
        let features = self.write_features();
        let decide = |pipeline: &mut TransactPipeline| match scope {
            Some(scope) => {
                pipeline.decide_scoped(request, now, SCOPED_FEATURES, claims, scope)
            }
            None => pipeline.decide(request, now, features, claims),
        };
        if request.validates_only() {
            let validation = match self.writer.as_mut().filter(|writer| !writer.recovering) {
                Some(writer) => decide(&mut writer.pipeline)?,
                None => decide(&mut self.new_pipeline()?)?,
            };
            return match validation {
                Decision::Answer(receipt) => Ok(Planned::Answer(receipt)),
                Decision::Append { .. } => {
                    Err(order_error("Validation allocated a journal record"))
                }
            };
        }
        let lease = self.authority()?;
        self.require_armed_rotation()?;
        let generation = self.state().generation;
        let coordinates = self.state().coordinates;
        let decision = decide(&mut self.writer.as_mut().ok_or_else(no_writer)?.pipeline)?;
        plan(decision, lease, generation, coordinates)
    }

    async fn decide_group(
        &mut self,
        requests: &[Transact],
        claims: &dyn ClaimVerifier,
    ) -> Result<Vec<Result<Planned>>> {
        let lease = self.authority()?;
        self.require_armed_rotation()?;
        let generation = self.state().generation;
        let coordinates = self.state().coordinates;
        let features = self.write_features();
        let mut decisions = Vec::with_capacity(requests.len());
        for request in requests {
            let now = self.sample_ordering_clock(request).await;
            let decision = self.authority().and_then(|current| {
                if current.epoch != lease.epoch { return Err(no_writer()); }
                self.writer.as_mut().ok_or_else(no_writer)?.pipeline.decide(request, now, features, claims)
            });
            decisions.push(decision.and_then(|decision| plan(decision, lease, generation, coordinates)));
        }
        Ok(decisions)
    }

    pub(super) fn sample_ordering_clock(&self, request: &Transact)
        -> impl std::future::Future<Output = Timestamp> + use<>
    {
        let supplied = request.store_writer_reading_floor();
        let inherited = (request.store_append().is_some() || supplied.is_some())
            .then(|| self.writer.as_ref()?.pipeline.last_store_reading()).flatten();
        let floor = inherited.into_iter().chain(supplied).max();
        let clock = Arc::clone(&self.clock);
        let skew = self.options.drift_micros;
        async move { rtdb_tx::transact::sample_store_clock(&*clock, floor, skew).await }
    }

    pub(super) async fn managed_workload(&mut self, request: workload::Request) -> Result<workload::Reply> {
        let result = self.managed_workload_serial(&request).await;
        if result.is_err() && self.writer.as_ref().is_some_and(|writer| writer.recovering) {
            self.recover_after_failure().await;
        }
        result
    }

    pub async fn open_classic_placement(
        &mut self, binding: [u8; 32], install: &super::ClassicInstallation,
        placement: &workload::ImportedPlacement,
    ) -> Result<workload::Fence> {
        self.refresh().await?;
        self.require_active()?;
        let now = (self.clock)();
        let origin = self.state().coordinates.origin.ok_or_else(workload::denied)?;
        if origin.digest != binding || origin.source.is_some() || origin.through != install.basis.position()
            || install.basis.incarnation() != self.incarnation()
        {
            return Err(workload::denied());
        }
        if let Some(fence) = placement.recover(self.durable_transactions(), now)? {
            return check_classic_placement(fence, install);
        }
        if self.classic_checkpoint_status(binding).await?.as_ref() != Some(install) {
            return Err(workload::denied());
        }
        let lease = self.authority()?;
        let generation = self.state().generation;
        let coordinates = self.state().coordinates;
        let claims = self.tokens.verify_claims_in(std::iter::empty(), None).await;
        let decision_time = now.max(self.durable_transactions().index().head().system_time.next()?);
        let (transaction, admission) = placement.prepare(self.durable_transactions(), decision_time)?;
        let decision = self.writer.as_mut().ok_or_else(no_writer)?.pipeline
            .decide_workload(&transaction, now, FEATURES, &claims, &admission)?;
        if let Planned::Append(write) = plan(decision, lease, generation, coordinates)? {
            self.commit_one(lease, write).await?;
        }
        let fence = placement.recover(self.durable_transactions(), (self.clock)())?
            .ok_or_else(workload::denied)?;
        check_classic_placement(fence, install)
    }

    async fn managed_workload_serial(&mut self, request: &workload::Request) -> Result<workload::Reply> {
        self.refresh().await?;
        self.require_active()?;
        let lease = self.authority()?;
        let claims = match &request.operation {
            workload::Operation::Submit { authority: authority @ workload::Authority::OriginalFamilyWrite { .. }, transaction: Some(transaction), .. } => {
                let scope = authority.original_family_claim_scope(request.namespace)?;
                self.tokens.verify_original_family_claims(transaction.claims(), &scope).await
            }
            workload::Operation::Submit { authority, transaction: Some(transaction), .. } => {
                let binding = authority.assignment_binding()?.ok_or_else(workload::denied)?;
                self.tokens.verify_workload_claims(transaction.claims(), binding).await
            }
            _ => self.tokens.verify_claims_in(std::iter::empty(), None).await,
        };
        let now = (self.clock)();
        let upper = self.append_authority(lease.epoch)?.upper();
        let state = self.durable_transactions();
        let decision_time = now.max(state.index().head().system_time.next()?);
        request.check_expiry(upper.max(decision_time))?;
        let (mut transaction, admission) = match request.prepare(state, decision_time)? {
            workload::Prepared::Answer(reply) => return Ok(reply),
            workload::Prepared::Append { transaction, admission } => (transaction, admission),
        };
        self.require_armed_rotation()?;
        let mut features = FEATURES.to_vec();
        let now = if matches!(admission.family(), workload::Family::CustodyWrite | workload::Family::OriginalFamilyWrite)
            && admission.cutoff().is_some()
        {
            let cutoff = admission.cutoff().filter(|cutoff| decision_time < *cutoff)
                .ok_or_else(workload::denied)?;
            if upper >= cutoff { return Err(workload::denied()); }
            for claim in transaction.claims() {
                claims.verify(claim)?;
            }
            let vault = self.tokens.custody_vault();
            prepare_custody(&mut transaction, &vault).await?;
            let clock = self.append_authority(lease.epoch)?.upper();
            if clock >= cutoff { return Err(workload::denied()); }
            request.check_effect(self.durable_transactions(), clock)?;
            features.push(ROW_CUSTODY);
            (self.clock)()
        } else {
            now
        };
        let generation = self.state().generation;
        let coordinates = self.state().coordinates;
        let decision = self.writer.as_mut().ok_or_else(no_writer)?.pipeline
            .decide_workload(&transaction, now, &features, &claims, &admission)?;
        let mut write = match plan(decision, lease, generation, coordinates)? {
            Planned::Answer(receipt) => return Ok(workload::Reply::Receipt(receipt)),
            Planned::Append(write) => write,
        };
        let original = write.at;
        write.admit_before = Some(admission.cutoff().unwrap_or(request.expires_at));
        let (upper, receipts) = self.commit_one(lease, write).await?;
        let (position, receipts) = if let Some(cutoff) = admission.cutoff() {
            let upper = upper.max(self.append_authority(lease.epoch)?.upper());
            let terminal_clock = if request.check_terminal(self.durable_transactions(), upper, &claims).is_ok() {
                upper
            } else {
                Timestamp::MAX
            };
            let (mut terminal, _) = self.decide_terminal(lease, original, terminal_clock)
                .map_err(|error| self.begin_write_recovery(AttemptError::Local(error)))?;
            if matches!(terminal.entry.record, Record::Settle { .. }) {
                terminal.admit_before = Some(cutoff);
            }
            let at = terminal.at;
            let (_, receipts) = self.commit_one(lease, terminal).await?;
            (at, receipts)
        } else {
            (original, receipts)
        };
        Ok(workload::Reply::Receipt(receipt_for(receipts, coordinates.position(position)?, &transaction.key)?))
    }

    pub(super) fn decide_terminal(
        &mut self,
        lease: Lease,
        at: Lsn,
        upper: Timestamp,
    ) -> Result<(Append, Vec<TxKey>)> {
        let generation = self.state().generation;
        let coordinates = self.state().coordinates;
        let Terminal {
            position,
            record,
            redecide,
        } = self
            .writer
            .as_mut()
            .ok_or_else(no_writer)?
            .pipeline
            .terminal(coordinates.position(at)?, upper)?;
        Ok((
            Append {
                at: coordinates.lsn(position)?,
                admit_before: None,
                entry: Entry {
                    epoch: lease.epoch,
                    generation,
                    record: journal_record(record, coordinates)?,
                },
            },
            redecide,
        ))
    }

    pub(super) fn append_authority(&self, epoch: Epoch) -> Result<Authority> {
        let lease = self.authority()?;
        if lease.epoch != epoch {
            return Err(no_writer());
        }
        let acknowledged = self.writer.as_ref().ok_or_else(no_writer)?.acknowledged;
        Ok(Authority {
            stream: self.stream.clone(),
            clock: Arc::clone(&self.clock),
            drift_micros: self.options.drift_micros,
            lease,
            next: next(acknowledged)?,
        })
    }

    /// `tail` is the digest of the last write's journal bytes.
    pub(super) async fn publish_staged(
        &mut self,
        lease: Lease,
        writes: Vec<Append>,
        tail: [u8; 32],
    ) -> Result<Vec<Receipt>> {
        let through = writes.last().expect("nonempty append group").at;
        let reload = writes.iter().any(|write| reload_writer_fold(&write.entry.record));
        let store_purge_floor = writes.iter().find_map(|write| match &write.entry.record {
            Record::StorePurgeReclaim { keep_from, .. } => Some(*keep_from),
            _ => None,
        });
        if self.committed.is_none() {
            self.catch_up(through).await?;
        }
        let mut state = self.committed.take().ok_or_else(no_writer)?;
        for Append { at, entry, .. } in writes {
            if at > state.head {
                state.apply_entry(&self.stream, self.store.as_ref(), at, entry).await?;
                if at == through {
                    state.read_back(tail);
                }
            }
        }
        self.install_committed(state);
        self.require_authority(lease)?;
        let coordinates = self.state().coordinates;
        let reloaded = reload.then(|| self.new_pipeline()).transpose()?;
        let writer = self
            .writer
            .as_mut()
            .filter(|writer| !writer.recovering && writer.lease.epoch == lease.epoch)
            .ok_or_else(no_writer)?;
        if let Some(pipeline) = reloaded {
            writer.pipeline = pipeline;
            writer.acknowledged = through;
            return Ok(Vec::new());
        }
        if let Some(keep_from) = store_purge_floor {
            writer.pipeline.durable_mut().reclaim_store_history(keep_from);
        }
        match writer.pipeline.acknowledge(coordinates.position(through)?) {
            Ok(receipts) => {
                writer.acknowledged = through;
                Ok(receipts)
            }
            Err(error) => {
                self.withdraw_writer();
                Err(error)
            }
        }
    }

    pub(super) fn begin_write_recovery(&mut self, error: AttemptError) -> Anomaly {
        let anomaly = match error {
            AttemptError::Journal(
                error @ (JournalError::Fenced { .. } | JournalError::Deleted),
            ) => self.mutation_failed(error),
            AttemptError::Journal(error) => super::journal_error(error),
            AttemptError::Local(error) => error,
        };
        if let Some(writer) = &mut self.writer {
            writer.pipeline.withdraw();
            writer.recovering = true;
        }
        interrupted(anomaly)
    }

    /// Recovers a writer that a failed write left recovering, without waiting
    /// for the next tick. The failed write keeps its own error. A recovery
    /// that does not finish leaves the writer recovering, or withdraws it, so
    /// the next tick retries it and reports its error to the placement loop.
    pub(super) async fn recover_after_failure(&mut self) {
        if let Err(error) = self.recover_writer().await {
            tracing::warn!(database = %self.options.database, code = %error.code,
                message = error.message.as_deref().unwrap_or(""), "Writer recovery after a failed write did not finish");
        }
    }

    pub(super) async fn recover_writer(&mut self) -> Result<()> {
        Box::pin(async move {
        let Some(writer) = self.writer.as_ref().filter(|writer| writer.recovering) else {
            return Ok(());
        };
        let lease = writer.lease;
        let acknowledged = writer.acknowledged;
        self.require_authority(lease)?;
        let head = match self.stream.seal(lease.epoch).await {
            Ok(head) => head,
            Err(error @ (JournalError::Fenced { .. } | JournalError::Deleted)) => {
                return Err(self.mutation_failed(error));
            }
            // An operator recovery ended this writer's epoch. Promotion
            // acknowledges it and seals a new one.
            Err(error @ JournalError::Recovered { .. }) => {
                self.withdraw_writer();
                return Err(super::journal_error(error));
            }
            Err(error) => return Err(super::journal_error(error)),
        };
        self.catch_up(head).await?;
        self.require_authority(lease)?;
        let state = self.committed.as_ref().ok_or_else(no_writer)?;
        if state.released == Some(lease.epoch) {
            self.withdraw_writer();
            return Ok(());
        }
        if head < acknowledged {
            // The writer acknowledged commits the recovered journal no longer
            // holds; withdraw with the lost range instead of retrying.
            self.withdraw_writer();
            return Err(crate::journal::journal_shortened(head, acknowledged));
        }
        if state.epoch > lease.epoch || acknowledged.0.saturating_add(1) < state.floor.0 {
            self.withdraw_writer();
            return Err(no_writer());
        }
        let coordinates = state.coordinates;
        let reload = state.records.iter().enumerate().any(|(offset, record)| {
            state.floor.0 + offset as u64 > acknowledged.0 && reload_writer_fold(record)
        });
        if reload {
            let pipeline = self.new_pipeline()?;
            let writer = self.writer.as_mut().ok_or_else(no_writer)?;
            writer.pipeline = pipeline;
            writer.acknowledged = head;
            writer.recovering = false;
            return Ok(());
        }
        let writer = self.writer.as_mut().ok_or_else(no_writer)?;
        for (offset, record) in state.records.iter().enumerate() {
            let position = state.floor.0 + offset as u64;
            if position <= acknowledged.0 {
                continue;
            }
            let logical = coordinates.position(Lsn(position))?;
            let recovered = match record {
                Record::Tx { payload, .. } => Some(writer.pipeline.recover(logical, payload)),
                Record::Settle { tx_lsn } => {
                    let bytes = TransactionRecord::Settle { tx_lsn: coordinates.position(Lsn(*tx_lsn))? }.encode()?;
                    Some(writer.pipeline.recover(logical, &bytes))
                }
                Record::Refuse { tx_lsn } => {
                    let bytes = TransactionRecord::Refuse { tx_lsn: coordinates.position(Lsn(*tx_lsn))? }.encode()?;
                    Some(writer.pipeline.recover(logical, &bytes))
                }
                Record::StorePurgeReclaim { keep_from, .. } => {
                    writer.pipeline.durable_mut().reclaim_store_history(*keep_from);
                    None
                }
                _ => None,
            };
            if let Some(recovered) = recovered {
                recovered?;
            }
            writer.acknowledged = Lsn(position);
        }
        writer.pipeline.resume(coordinates.position(next(head)?)?)?;
        if let Some((epoch, until)) = state.lease
            && epoch == lease.epoch
        {
            writer.lease.until = writer.lease.until.max(until);
        }
        writer.recovering = false;
        if let Some(TransactionRecord::Refuse { tx_lsn }) = state.transactions.recover_pending() {
            let (terminal, _) = self
                .decide_terminal(lease, coordinates.lsn(tx_lsn)?, Timestamp::MAX)
                .map_err(|error| self.begin_write_recovery(AttemptError::Local(error)))?;
            self.commit_one(lease, terminal).await?;
        }
        Ok(())
        }).await
    }

    pub(super) async fn commit_one(
        &mut self,
        lease: Lease,
        write: Append,
    ) -> Result<(Timestamp, Vec<Receipt>)> {
        Box::pin(async move {
        let authority = self.append_authority(lease.epoch)?;
        let writes = vec![write];
        let (upper, tail) = authority
            .append(&writes)
            .await
            .map_err(|error| self.begin_write_recovery(error))?;
        let receipts = self
            .publish_staged(lease, writes, tail)
            .await
            .map_err(|error| self.begin_write_recovery(AttemptError::Local(error)))?;
        Ok((upper, receipts))
        }).await
    }

    pub(super) async fn commit_checkpoint(
        &mut self,
        lease: Lease,
        write: Append,
        covered_through_lsn: Lsn,
    ) -> Result<bool> {
        Box::pin(async move {
            if self.stream.immutable_history() {
                let authority = self.append_authority(lease.epoch)?;
                if authority.next != write.at {
                    let error = order_error("The checkpoint must follow the acknowledged prefix");
                    return Err(self.begin_write_recovery(AttemptError::Local(error)));
                }
                let record = self.stream.encode(write.at, &write.entry).await
                    .map_err(|error| self.begin_write_recovery(AttemptError::Local(error)))?;
                self.require_authority(lease)?;
                let tail = Sha256::digest(&record).into();
                let installation = journal_contract::CheckpointInstallation {
                    operation_id: uuid::Uuid::new_v4().as_u128(),
                    checkpoint_lsn: write.at,
                    covered_through_lsn,
                    record,
                };
                let receipt = self.stream.install_checkpoint(lease.epoch, installation).await
                    .map_err(|error| self.begin_write_recovery(AttemptError::Journal(error)))?;
                if let Some(receipt) = receipt {
                    if receipt.checkpoint_lsn != write.at
                        || receipt.covered_through_lsn != covered_through_lsn
                        || next(covered_through_lsn).ok() != Some(receipt.logical_floor_lsn)
                    {
                        let error = order_error("The journal acknowledged a different checkpoint");
                        return Err(self.begin_write_recovery(AttemptError::Local(error)));
                    }
                    self.publish_staged(lease, vec![write], tail).await
                        .map_err(|error| self.begin_write_recovery(AttemptError::Local(error)))?;
                    return Ok(true);
                }
            }
            self.commit_one(lease, write).await?;
            Ok(false)
        }).await
    }

    pub(super) fn withdraw_writer(&mut self) {
        if let Some(mut writer) = self.writer.take() {
            writer.pipeline.withdraw();
        }
    }
}

fn check_classic_placement(fence: workload::Fence, install: &super::ClassicInstallation) -> Result<workload::Fence> {
    let basis = Basis::from_bytes(&fence.applied.basis)?;
    if basis.incarnation() != install.basis.incarnation() || basis <= install.basis
        || fence.applied.system_time_micros < install.system_time.0
    {
        return Err(workload::denied());
    }
    Ok(fence)
}

pub(super) fn plan(decision: Decision, lease: Lease, generation: u64, coordinates: JournalCoordinates) -> Result<Planned> {
    Ok(match decision {
        Decision::Answer(receipt) => Planned::Answer(receipt),
        Decision::Append { position, record } => Planned::Append(Append {
            at: coordinates.lsn(position)?,
            admit_before: None,
            entry: Entry {
                epoch: lease.epoch,
                generation,
                record: journal_record(record, coordinates)?,
            },
        }),
    })
}

fn reload_writer_fold(record: &Record) -> bool {
    matches!(record, Record::Checkpoint(checkpoint) if checkpoint.restore)
        || matches!(record, Record::KeyRetirement { armed: true, .. })
}

fn journal_record(payload: Bytes, coordinates: JournalCoordinates) -> Result<Record> {
    Ok(match TransactionRecord::decode(&payload)? {
        TransactionRecord::Tx(_) => Record::Tx {
            payload: payload.to_vec(),
            objects: Vec::new(),
        },
        TransactionRecord::Settle { tx_lsn } => Record::Settle { tx_lsn: coordinates.lsn(tx_lsn)?.0 },
        TransactionRecord::Refuse { tx_lsn } => Record::Refuse { tx_lsn: coordinates.lsn(tx_lsn)?.0 },
    })
}

pub(super) fn receipt_for(receipts: Vec<Receipt>, position: u64, key: &[u8]) -> Result<Receipt> {
    receipts
        .into_iter()
        .find(|receipt| receipt.basis.position() == position && receipt.key == key)
        .ok_or_else(missing_receipt)
}

fn interrupted(error: Anomaly) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Interrupted,
        error.code,
        "The write outcome is unknown. Recover it with TxStatus",
    )
}

fn order_error(message: &str) -> Anomaly {
    Anomaly::fault("rtdb/transaction-order", message)
}

fn pipeline_closed() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/write-pipeline-closed",
        "The database write pipeline is closed",
    )
}
