use std::{sync::Arc, time::Duration};
use futures::{StreamExt, future::BoxFuture, stream::FuturesOrdered};
use rtdb_columnar::MemoryBudget;
use rtdb_replication::{DriverStatus, ExternalReceipt, TermHandle};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use crate::{ConfirmationState, DirectMirror, Lsn, PgIndexer, PgTransaction, Remote, ResumeToken, SourceConfig, config::DIRECT_MIRROR_TYPE, indexer::ImportWriter, snapshot::{SnapshotReader, publication_exists}, stream::ChangeStream};

#[derive(Clone, Debug)]
pub struct ReconnectPolicy { pub initial_delay: Duration, pub maximum_delay: Duration, pub jitter: f64, pub reset_after: Duration }
impl Default for ReconnectPolicy { fn default() -> Self { Self { initial_delay: Duration::from_secs(1), maximum_delay: Duration::from_secs(30), jitter: 0.5, reset_after: Duration::from_secs(600) } } }
impl ReconnectPolicy {
    pub fn validate(&self) -> Result<()> { if self.initial_delay.is_zero() || self.maximum_delay < self.initial_delay || self.reset_after.is_zero() || !self.jitter.is_finite() || !(0.0..=1.0).contains(&self.jitter) { Err(Anomaly::incorrect("xtdb.postgres/invalid-reconnect-policy", "Reconnect delays and jitter are out of range")) } else { Ok(()) } }
    pub fn delay_after(&self, failures: u32, random: f64) -> Duration {
        if failures == 0 { return Duration::ZERO; }
        let factor = 1u32.checked_shl(failures.saturating_sub(1)).unwrap_or(u32::MAX);
        let delay = self.initial_delay.saturating_mul(factor).min(self.maximum_delay);
        delay.mul_f64(1.0 - self.jitter * random.clamp(0.0, 1.0))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourcePhase { Starting, Snapshot, Connecting, Streaming, Stopped }
#[derive(Clone, Debug)]
pub struct SourceStatus { pub phase: SourcePhase, pub events: u64, pub commits: u64, pub last_commit_time: Option<Timestamp>, pub last_commit_lag_seconds: Option<f64>, pub received: Lsn, pub held: Lsn, pub confirmed: Lsn }
struct Applied { events: usize, commit_time: Option<Timestamp> }
type Pending = FuturesOrdered<BoxFuture<'static, Result<Applied>>>;

#[derive(Clone)]
pub struct SourceObserver {
    status: watch::Receiver<SourceStatus>, remote: Remote, slot: String, memory: Arc<MemoryBudget>, assignment: CancellationToken,
}
impl SourceObserver {
    pub fn status(&self) -> SourceStatus { self.status.borrow().clone() }
    pub fn watch_status(&self) -> watch::Receiver<SourceStatus> { self.status.clone() }
    pub async fn wal_lag_bytes(&self) -> Result<Option<i64>> {
        if !matches!(self.status.borrow().phase, SourcePhase::Snapshot | SourcePhase::Connecting | SourcePhase::Streaming) { return Ok(None); }
        tokio::select! {
            _ = self.assignment.cancelled() => Ok(None),
            result = async {
                let mut connection = crate::wire::Connection::open(&self.remote, false, Arc::clone(&self.memory)).await?;
                let query = format!("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint FROM pg_replication_slots WHERE slot_name = {}", crate::config::literal(&self.slot)?);
                let result = connection.query(&query).await?;
                result.rows.first().map(|row| row.cell(0)).transpose()?.flatten().map(|value| value.parse().map_err(crate::wire::protocol)).transpose()
            } => result,
        }
    }
}

pub struct SourceDriver {
    remote: Remote, config: SourceConfig, indexer: Arc<dyn PgIndexer>, memory: Arc<MemoryBudget>, reconnect: ReconnectPolicy, maximum_pending: usize, status: watch::Sender<SourceStatus>, sampling: CancellationToken,
}
impl SourceDriver {
    pub fn new(remote: Remote, config: SourceConfig, memory: Arc<MemoryBudget>, indexer: Option<Arc<dyn PgIndexer>>) -> Result<Self> {
        remote.validate()?; config.validate()?;
        let indexer = match indexer {
            Some(indexer) => indexer,
            None if config.indexer.as_ref().is_none_or(|indexer| indexer.type_url == DIRECT_MIRROR_TYPE) => Arc::new(DirectMirror),
            None => return Err(Anomaly::incorrect("xtdb.postgres/unknown-indexer", "A registered native indexer is required for this source configuration")),
        };
        let status = watch::channel(SourceStatus { phase: SourcePhase::Starting, events: 0, commits: 0, last_commit_time: None, last_commit_lag_seconds: None, received: Lsn(0), held: Lsn(0), confirmed: Lsn(0) }).0;
        Ok(Self { remote, config, indexer, memory, reconnect: ReconnectPolicy::default(), maximum_pending: 128, status, sampling: CancellationToken::new() })
    }
    pub fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Result<Self> { policy.validate()?; self.reconnect = policy; Ok(self) }
    pub fn with_maximum_pending(mut self, maximum: usize) -> Result<Self> { if maximum == 0 { return Err(Anomaly::incorrect("xtdb.postgres/invalid-pending-limit", "The source requires a positive pending transaction limit")); } self.maximum_pending = maximum; Ok(self) }
    pub fn watch_status(&self) -> watch::Receiver<SourceStatus> { self.status.subscribe() }
    pub fn observer(&self) -> SourceObserver { SourceObserver { status: self.status.subscribe(), remote: self.remote.clone(), slot: self.config.slot_name.clone(), memory: Arc::clone(&self.memory), assignment: self.sampling.clone() } }
    /// The caller owns this future and must cancel and join it before assigning the source to another term.
    /// `after` is applied progress; the block watch supplies the independently durable confirmation bound.
    pub async fn run(self, term: TermHandle, after: Option<Arc<[u8]>>, mut blocks: watch::Receiver<DriverStatus>, cancellation: CancellationToken) -> Result<()> {
        let _sampling_region = self.sampling.clone().drop_guard();
        let result = tokio::select! { _ = cancellation.cancelled() => Ok(()), result = self.run_inner(&term, after, &mut blocks) => result };
        self.status.send_modify(|status| status.phase = SourcePhase::Stopped);
        result
    }
    async fn run_inner(&self, term: &TermHandle, after: Option<Arc<[u8]>>, blocks: &mut watch::Receiver<DriverStatus>) -> Result<()> {
        if !publication_exists(&self.remote, &self.config, Arc::clone(&self.memory)).await? { return Err(Anomaly::incorrect("xtdb.postgres/missing-publication", "Publication does not exist on the upstream; create it before attaching the source")); }
        let resume = match after { Some(token) => ResumeToken::decode(&token)?.require_complete()?, None => self.snapshot(term).await? };
        self.stream(term, resume, blocks).await
    }
    async fn submit(&self, term: &TermHandle, transaction: PgTransaction, completed: bool) -> Result<BoxFuture<'static, Result<Applied>>> {
        let events = transaction.operations.len(); let commit_time = transaction.commit_time;
        let token = ResumeToken::new(transaction.lsn, completed).encode();
        let receipt = term.submit_external(Box::new(ImportWriter { transaction, indexer: Arc::clone(&self.indexer) }), commit_time, Some(token)).await?;
        Ok(applied(receipt, events, commit_time))
    }
    async fn snapshot(&self, term: &TermHandle) -> Result<Lsn> {
        self.status.send_modify(|status| status.phase = SourcePhase::Snapshot);
        let mut snapshot = SnapshotReader::open(&self.remote, &self.config, Arc::clone(&self.memory)).await?;
        let mut pending = Pending::new();
        while let Some(transaction) = snapshot.next_batch().await? {
            if pending.len() >= self.maximum_pending { if let Some(result) = pending.next().await { result?; } }
            pending.push_back(self.submit(term, transaction, false).await?);
        }
        let lsn = snapshot.lsn;
        pending.push_back(self.submit(term, PgTransaction::empty(lsn), true).await?);
        while let Some(result) = pending.next().await { result?; }
        snapshot.finish().await?;
        Ok(lsn)
    }
    async fn stream(&self, term: &TermHandle, resume: Lsn, blocks: &mut watch::Receiver<DriverStatus>) -> Result<()> {
        let mut confirmation = ConfirmationState::new(resume, durable(blocks)?);
        let mut pending = Pending::new(); let mut failures = 0u32; let mut random = uuid::Uuid::new_v4().as_u128() as u64;
        loop {
            self.status.send_modify(|status| status.phase = SourcePhase::Connecting);
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let delay = self.reconnect.delay_after(failures, (random >> 11) as f64 / ((1u64 << 53) as f64));
            let start = confirmation.held();
            let open = async { tokio::time::sleep(delay).await; ChangeStream::open(&self.remote, &self.config, start, Arc::clone(&self.memory)).await };
            tokio::pin!(open);
            let result = loop {
                tokio::select! {
                    result = &mut open => break result,
                    result = pending.next(), if !pending.is_empty() => if let Some(result) = result { self.record_applied(result?); },
                    change = blocks.changed() => { change.map_err(|_| stopped())?; confirmation.adopted(durable(blocks)?); }
                }
            };
            let mut stream = match result { Ok(stream) => stream, Err(error) if retryable(&error) => { failures = failures.saturating_add(1); continue; }, Err(error) => return Err(error) };
            let opened = tokio::time::Instant::now();
            let mut interval = tokio::time::interval(self.remote.status_interval); interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            self.status.send_modify(|status| status.phase = SourcePhase::Streaming);
            let mut initial_status = true;
            let failure = loop {
                if initial_status {
                    let confirmable = confirmation.confirmable();
                    if let Err(error) = stream.acknowledge(confirmable.max(confirmation.confirmed())).await { break error; }
                    confirmation.confirmed_through(confirmable);
                    initial_status = false;
                }
                enum Event { Read(Result<crate::stream::StreamUpdate>), Applied(Option<Result<Applied>>), Durable(std::result::Result<(), watch::error::RecvError>), Tick }
                let can_read = pending.len() < self.maximum_pending;
                let event = tokio::select! {
                    update = stream.read(), if can_read => Event::Read(update),
                    result = pending.next(), if !pending.is_empty() => Event::Applied(result),
                    change = blocks.changed() => Event::Durable(change),
                    _ = interval.tick() => Event::Tick,
                };
                let force = match event {
                    Event::Read(Ok(update)) => {
                        confirmation.received_through(update.received);
                        if let Some(transaction) = update.transaction {
                            failures = 0;
                            if transaction.lsn > confirmation.held() {
                                let lsn = transaction.lsn;
                                pending.push_back(self.submit(term, transaction, true).await?);
                                confirmation.submitted(lsn);
                            }
                        }
                        update.reply_requested
                    }
                    Event::Read(Err(error)) => break error,
                    Event::Applied(Some(result)) => { self.record_applied(result?); false },
                    Event::Applied(None) => false,
                    Event::Durable(change) => { change.map_err(|_| stopped())?; confirmation.adopted(durable(blocks)?); false },
                    Event::Tick => true,
                };
                let confirmable = confirmation.confirmable();
                if force || confirmable > confirmation.confirmed() {
                    if let Err(error) = stream.acknowledge(confirmable.max(confirmation.confirmed())).await { break error; }
                    confirmation.confirmed_through(confirmable);
                }
                self.status.send_modify(|status| { status.received = confirmation.received(); status.held = confirmation.held(); status.confirmed = confirmation.confirmed(); });
            };
            drop(stream);
            if !retryable(&failure) { return Err(failure); }
            if opened.elapsed() >= self.reconnect.reset_after { failures = 0; }
            failures = failures.saturating_add(1);
        }
    }
    fn record_applied(&self, applied: Applied) {
        self.status.send_modify(|status| { status.events = status.events.saturating_add(applied.events as u64); status.commits = status.commits.saturating_add(1); status.last_commit_time = applied.commit_time; status.last_commit_lag_seconds = applied.commit_time.map(|time| (chrono::Utc::now().timestamp_micros() as f64 - time.0 as f64) / 1_000_000.0); });
    }
}
fn applied(receipt: ExternalReceipt, events: usize, commit_time: Option<Timestamp>) -> BoxFuture<'static, Result<Applied>> {
    Box::pin(async move { receipt.applied().await?; Ok(Applied { events, commit_time }) })
}
fn durable(blocks: &mut watch::Receiver<DriverStatus>) -> Result<Option<Lsn>> {
    blocks.borrow_and_update().adopted_block.as_ref().and_then(|block| block.external_source_token()).map(ResumeToken::decode).transpose().map(|token| token.map(|token| token.lsn))
}
fn retryable(error: &Anomaly) -> bool { error.category == AnomalyCategory::Unavailable && error.code.starts_with("xtdb.postgres/") }
fn stopped() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb.postgres/assignment-ended", "PostgreSQL source assignment ended") }
