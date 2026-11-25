use std::{
    collections::{BTreeSet, VecDeque},
    net::IpAddr,
    sync::Arc,
};

use bytes::Bytes;
use journal_contract::{Epoch, JournalError, Lsn};
use rtdb_crypt::tokens::{TokenAuthority, VerifiedClaims};
use rtdb_format::journal::Record;
use rtdb_tx::transact::{
    COUNT_RANGE_CONSTRAINT, Decision, GROUPED_COUNT_CONSTRAINT, LINEARIZABLE, PHYSICAL_ERASURE, RELATIVE_VALID_TIME, Receipt, ScopedTransact, TIMED_TRANSACT, TRANSACT_REQUIRES, Terminal, Transact,
    TransactPipeline, TransactState, TransactionRecord, TxKey, ROW_CUSTODY, prepare_custody, workload,
};
use rtdb_index::branches::BranchCut;
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::{Basis, ClaimVerifier, LabelScope}};
use tokio::{
    sync::{Mutex, OnceCell, mpsc, oneshot},
    task::JoinHandle,
};

use crate::replay::JournalCoordinates;

use super::{
    Entry, Lease, MultiDatabase, PinnedExport, Role, Stream, Uuid, missing_receipt, next, no_writer,
};

const CAPACITY: usize = 64;
const FEATURES: &[&str] = &[LINEARIZABLE, TIMED_TRANSACT, TRANSACT_REQUIRES, RELATIVE_VALID_TIME, PHYSICAL_ERASURE, COUNT_RANGE_CONSTRAINT, GROUPED_COUNT_CONSTRAINT, "row-visibility", "branches",
    rtdb_types::rtdb_v1::RANKED_INDEX_DECLARATIONS];
const CUSTODY_FEATURES: &[&str] = &[
    LINEARIZABLE, TIMED_TRANSACT, TRANSACT_REQUIRES, RELATIVE_VALID_TIME, PHYSICAL_ERASURE,
    COUNT_RANGE_CONSTRAINT, GROUPED_COUNT_CONSTRAINT, "row-visibility", "branches",
    rtdb_types::rtdb_v1::RANKED_INDEX_DECLARATIONS, ROW_CUSTODY,
];
const SCOPED_FEATURES: &[&str] = &[LINEARIZABLE, TIMED_TRANSACT, TRANSACT_REQUIRES, PHYSICAL_ERASURE, "row-visibility"];
type Reply<T> = oneshot::Sender<Result<T>>;

enum Command {
    Transact(Work),
    Funded {
        transaction: Transact,
        scope: Option<Scope>,
        owner: [u8; 32],
        command: rtdb_tx::transact::FundingCommand,
        clock: Arc<dyn rtdb_tx::DecisionClock>,
        reply: Reply<Receipt>,
    },
    Workload(workload::Request, Reply<workload::Reply>),
    Tick(BTreeSet<IpAddr>, Reply<Role>),
    PublishRanked {
        build: super::RankedBuild,
        inventory: Vec<u8>,
        files: Vec<bytes::Bytes>,
        memory: Arc<rtdb_types::MemoryBudget>,
        reply: Reply<()>,
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
        !self.request.uses_erasure() && (!self.request.ops.is_empty() || !self.request.otherwise.is_empty())
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
    pub async fn publish_ranked_catalog(
        &self, build: super::RankedBuild, inventory: Vec<u8>, files: Vec<bytes::Bytes>,
        memory: Arc<rtdb_types::MemoryBudget>,
    ) -> Result<()> {
        self.request(|reply| Command::PublishRanked { build, inventory, files, memory, reply }).await
    }

    pub fn start(database: Arc<Mutex<MultiDatabase>>) -> Self {
        let (input, receive) = mpsc::channel(CAPACITY);
        let task = tokio::spawn(run(Arc::clone(&database), receive));
        Self { input, task, database, tokens: OnceCell::new() }
    }

    pub async fn transact(&self, request: Transact) -> Result<Receipt> {
        self.submit(request, None).await
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
        self.request(|reply| Command::Funded { transaction, scope, owner, command, clock, reply }).await
    }

    pub async fn tick(&self, live: BTreeSet<IpAddr>) -> Result<Role> {
        self.request(|reply| Command::Tick(live, reply)).await
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
    ) -> std::result::Result<(), AttemptError> {
        let batch = self.stream.batch.as_ref().filter(|batch| batch.maximum_timed_original_bytes().is_some())
            .ok_or_else(|| AttemptError::Local(rtdb_tx::transact::funding_unsupported()))?;
        if self.next != write.at { return Err(AttemptError::Local(order_error("The funded append must follow the acknowledged prefix"))); }
        let bytes = self.stream.encode(write.at, &write.entry).await.map_err(AttemptError::Local)?;
        let upper = self.upper();
        if upper >= decision_before || cutoff.before().0 > self.lease.until {
            return Err(AttemptError::Local(Anomaly::new(AnomalyCategory::Conflict,
                "rtdb/window-expired", "The original timed append missed its admission bounds")));
        }
        let original = self.stream.timed_original(write.at, self.lease.epoch, &bytes, cutoff);
        let proof = batch.append_timed_original(original, bytes)
            .await.map_err(AttemptError::Journal)?;
        proof.verify_exact(original).map_err(AttemptError::Local)
    }

    fn upper(&self) -> Timestamp {
        Timestamp((self.clock)().0.saturating_add(self.drift_micros))
    }

    fn limit(&self) -> usize {
        if self.stream.batch.is_some() {
            CAPACITY
        } else {
            1
        }
    }

    pub(super) async fn append(
        &self,
        writes: &[Append],
    ) -> std::result::Result<Timestamp, AttemptError> {
        let mut records = Vec::with_capacity(writes.len());
        for (offset, write) in writes.iter().enumerate() {
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
        Ok(upper)
    }
}

pub(super) enum AttemptError {
    Local(Anomaly),
    Journal(JournalError),
}

async fn run(database: Arc<Mutex<MultiDatabase>>, mut input: mpsc::Receiver<Command>) {
    let mut barrier = None;
    loop {
        let command = match barrier.take() {
            Some(command) => command,
            None => match input.recv().await {
                Some(command) => command,
                None => return,
            },
        };
        match command {
            Command::Transact(work) if work.batchable() => {
                burst(&database, &mut input, &mut barrier, work).await;
            }
            Command::Transact(work) => {
                let result = database
                    .lock()
                    .await
                    .transact_in(&work.request, work.scope.as_ref().map(Scope::authority))
                    .await;
                let _ = work.reply.send(result);
            }
            Command::Workload(request, reply) => {
                let result = database.lock().await.managed_workload(request).await;
                let _ = reply.send(result);
            }
            Command::Funded { transaction, scope, owner, command, clock, reply } => {
                let result = database.lock().await.transact_funded_serial(
                    &transaction, scope.as_ref().map(Scope::authority), owner, command, clock.as_ref(),
                ).await;
                let _ = reply.send(result);
            }
            Command::Tick(live, reply) => {
                let result = database.lock().await.tick(&live).await;
                let _ = reply.send(result);
            }
            Command::PublishRanked { build, inventory, files, memory, reply } => {
                let result = database.lock().await.publish_ranked_catalog(build, inventory, files, &memory).await;
                let _ = reply.send(result);
            }
            Command::Export { pin, basis, until, reply } => {
                let result = database.lock().await.export_ticket(pin, basis, until).await;
                let _ = reply.send(result);
            }
            Command::OpenExport { ticket, reply } => {
                let result = database.lock().await.open_export_ticket(&ticket).await;
                let _ = reply.send(result);
            }
            Command::ReleaseExport { ticket, reply } => {
                let result = database.lock().await.release_export_ticket(&ticket).await;
                let _ = reply.send(result);
            }
            Command::Restore { export, reply } => {
                let result = database.lock().await.restore_export(&export).await;
                let _ = reply.send(result);
            }
            Command::Retire(reply) => {
                let result = database.lock().await.retire_deployment().await;
                let _ = reply.send(result);
            }
        }
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
    stage_batch(&mut *database.lock().await, &mut staged, queued);
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
                                stage_batch(&mut *database.lock().await, &mut staged, group);
                            }
                            Some(command) => *barrier = Some(command),
                            None => input_open = false,
                        }
                    }
                }
            }
        };
        let upper = match result {
            Ok(upper) => upper,
            Err(error) => {
                fail(database, &mut staged, waiters, error).await;
                return;
            }
        };
        let positions: Vec<_> = writes.iter().map(|write| write.at).collect();
        let mut database_guard = database.lock().await;
        let mut receipts = match database_guard.publish_staged(authority.lease, writes).await {
            Ok(receipts) => receipts.into_iter(),
            Err(error) => {
                drop(database_guard);
                fail(database, &mut staged, waiters, AttemptError::Local(error)).await;
                return;
            }
        };
        for ((work, original_timed), position) in waiters.into_iter().zip(positions) {
            if original_timed {
                let terminal = database_guard.decide_terminal(authority.lease, position, upper);
                let result = terminal.and_then(|(write, redecide)| {
                    let queued = take_redecisions(&mut staged, &redecide)?;
                    stage_batch(&mut database_guard, &mut staged, queued);
                    Ok(write)
                });
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

fn stage_batch(database: &mut MultiDatabase, staged: &mut VecDeque<Staged>, queued: Vec<Work>) {
    if database.stream.batch.is_some()
        && queued.len() > 1
        && queued
            .iter()
            .all(|work| work.scope.is_none() && !work.timed())
    {
        let requests: Vec<_> = queued.iter().map(|work| work.request.clone()).collect();
        let claims = VerifiedClaims::union(queued.iter().map(|work| &work.claims));
        match database.decide_group(&requests, &claims) {
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
            );
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
        let _ = database.recover_writer().await;
    } else {
        let _ = database.refresh().await;
    }
}

pub(super) enum Planned {
    Answer(Receipt),
    Append(Append),
}

impl MultiDatabase {
    pub fn supports_funded_transact(&self) -> bool {
        self.authority().is_ok() && self.stream.batch.as_ref().and_then(|batch| batch.maximum_timed_original_bytes()).is_some()
    }

    async fn transact_funded_serial(
        &mut self, request: &Transact, scope: Option<ScopedTransact<'_>>, owner: [u8; 32],
        command: rtdb_tx::transact::FundingCommand, clock: &dyn rtdb_tx::DecisionClock,
    ) -> Result<Receipt> {
        self.refresh().await?;
        self.require_active()?;
        if !self.supports_funded_transact() || !clock.supports_funded_offers() {
            return Err(rtdb_tx::transact::funding_unsupported());
        }
        self.require_armed_rotation()?;
        let record_limit = self.stream.batch.as_ref().and_then(|batch| batch.maximum_timed_original_bytes())
            .ok_or_else(rtdb_tx::transact::funding_unsupported)?.saturating_sub(1024);
        let claims = self.tokens.verify_claims_in(request.claims(), scope.map(|scope| scope.labels)).await;
        let mut funding = rtdb_tx::transact::FundingInput::new(command, owner);
        funding.verify(request, self.durable_transactions(), scope, clock).await;
        let lease = self.authority()?;
        let generation = self.state().generation;
        let coordinates = self.state().coordinates;
        let now = (self.clock)();
        let features = if scope.is_some() { SCOPED_FEATURES } else { self.write_features() };
        let decision = self.writer.as_mut().ok_or_else(no_writer)?.pipeline
            .decide_funded(request, now, features, &claims, scope, None, funding, record_limit)?;
        let write = match plan(decision, lease, generation, coordinates)? {
            Planned::Answer(receipt) => return self.project_receipt(receipt, scope),
            Planned::Append(write) => write,
        };
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
            let result = authority.append_funded(&write, decision_before, cutoff).await;
            if let Err(error) = result {
                let error = self.begin_write_recovery(error);
                let _ = self.recover_writer().await;
                return Err(error);
            }
            self.publish_staged(lease, vec![write]).await
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

    pub(super) fn decide_one(
        &mut self,
        request: &Transact,
        scope: Option<ScopedTransact<'_>>,
        claims: &dyn ClaimVerifier,
    ) -> Result<Planned> {
        let now = (self.clock)();
        let features = self.write_features();
        let decide = |pipeline: &mut TransactPipeline| match scope {
            Some(scope) => {
                pipeline.decide_scoped(request, now, SCOPED_FEATURES, claims, scope)
            }
            None => pipeline.decide(request, now, features, claims),
        };
        if request.ops.is_empty() && request.otherwise.is_empty() {
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

    fn decide_group(
        &mut self,
        requests: &[Transact],
        claims: &dyn ClaimVerifier,
    ) -> Result<Vec<Result<Planned>>> {
        let lease = self.authority()?;
        self.require_armed_rotation()?;
        let generation = self.state().generation;
        let coordinates = self.state().coordinates;
        let now = (self.clock)();
        let features = self.write_features();
        let writer = self.writer.as_mut().ok_or_else(no_writer)?;
        Ok(writer
            .pipeline
            .decide_batch(requests, now, features, claims)
            .into_iter()
            .map(|decision| decision.and_then(|decision| plan(decision, lease, generation, coordinates)))
            .collect())
    }

    pub(super) async fn managed_workload(&mut self, request: workload::Request) -> Result<workload::Reply> {
        let result = self.managed_workload_serial(&request).await;
        if result.is_err() && self.writer.as_ref().is_some_and(|writer| writer.recovering) {
            let _ = self.recover_writer().await;
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

    pub(super) async fn publish_staged(
        &mut self,
        lease: Lease,
        writes: Vec<Append>,
    ) -> Result<Vec<Receipt>> {
        let through = writes.last().expect("nonempty append group").at;
        let reload = writes.iter().any(|write| reload_writer_fold(&write.entry.record));
        if self.committed.is_none() {
            self.catch_up(through).await?;
        }
        let mut state = self.committed.take().ok_or_else(no_writer)?;
        for Append { at, entry, .. } in writes {
            if at > state.head {
                state.apply_entry(&self.stream, self.store.as_ref(), at, entry).await?;
            }
        }
        self.committed = Some(state);
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

    pub(super) async fn recover_writer(&mut self) -> Result<()> {
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
            Err(error) => return Err(super::journal_error(error)),
        };
        self.catch_up(head).await?;
        self.require_authority(lease)?;
        let state = self.committed.as_ref().ok_or_else(no_writer)?;
        if state.released == Some(lease.epoch) {
            self.withdraw_writer();
            return Ok(());
        }
        if state.epoch > lease.epoch
            || acknowledged.0.saturating_add(1) < state.floor.0
            || head < acknowledged
        {
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
    }

    pub(super) async fn commit_one(
        &mut self,
        lease: Lease,
        write: Append,
    ) -> Result<(Timestamp, Vec<Receipt>)> {
        let authority = self.append_authority(lease.epoch)?;
        let writes = vec![write];
        let upper = authority
            .append(&writes)
            .await
            .map_err(|error| self.begin_write_recovery(error))?;
        let receipts = self
            .publish_staged(lease, writes)
            .await
            .map_err(|error| self.begin_write_recovery(AttemptError::Local(error)))?;
        Ok((upper, receipts))
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

fn plan(decision: Decision, lease: Lease, generation: u64, coordinates: JournalCoordinates) -> Result<Planned> {
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
