use std::collections::VecDeque;
use std::sync::{Arc, OnceLock, atomic::{AtomicI64, Ordering}};
use std::time::Duration;

use parking_lot::Mutex;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{envelope::{ReplicaBody, ReplicaEnvelope}, log::proto};
use rtdb_index::PreparedSearchDelta;
use rtdb_tx::{ResolvedTx, TransactionResult};
use rtdb_types::{Anomaly, BlockIndex, PlacedTerminal, ReplicaPosition, Result, Term, TermSequence, Timestamp};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{PartitionLogs, ReplicaMetadata, ReplicationClock, progress::term_ended};
use crate::readback_identity::{ReplicaFingerprint, TransactionEvent, fingerprint, matching_present_integer, transaction_completion};

pub(crate) enum WriteBody { Resolved(Arc<ResolvedTx>, Option<PreparedSearchDelta>), Message(ReplicaBody) }

impl WriteBody {
    pub(crate) fn search_delta(&self) -> Option<&PreparedSearchDelta> {
        match self {
            Self::Resolved(_, delta) => delta.as_ref(),
            Self::Message(_) => None,
        }
    }
}
#[derive(Clone, Debug)]
pub(crate) enum Applied { Transaction(Arc<TransactionResult>), Control, TrieRemoval(ReplicaPosition) }

pub(crate) struct AppendStamp {
    pub sequence: TermSequence,
    pub fingerprint: Option<ReplicaFingerprint>,
}

pub(crate) struct PendingWrite {
    pub body: WriteBody,
    pub block: BlockIndex,
    placed_terminal: Option<PlacedTerminal>,
    append: OnceLock<AppendStamp>,
    completion: Mutex<Option<oneshot::Sender<Result<Applied>>>>,
    _reservation: Arc<MemoryReservation>,
}
impl PendingWrite {
    pub fn new(body: WriteBody, block: BlockIndex, completion: Option<oneshot::Sender<Result<Applied>>>, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let extra = match &body { WriteBody::Resolved(..) => 1024, WriteBody::Message(body) => Self::message_bound(body)? };
        Ok(Self::reserved(body, block, completion, None, memory.reserve(extra)?))
    }
    pub fn placed(body: WriteBody, block: BlockIndex, terminal: PlacedTerminal, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        Ok(Self::reserved(body, block, None, Some(terminal), memory.reserve(1024)?))
    }
    pub fn clone_message(body: &ReplicaBody, block: BlockIndex, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let reservation = memory.reserve(Self::message_bound(body)?)?;
        Ok(Self::reserved(WriteBody::Message(body.clone()), block, None, None, reservation))
    }
    fn message_bound(body: &ReplicaBody) -> Result<usize> {
        body.encoded_len().checked_mul(4).and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(|| Anomaly::fault("rtdb.replication/payload-size", "Pending message allocation size overflow"))
    }
    fn reserved(body: WriteBody, block: BlockIndex, completion: Option<oneshot::Sender<Result<Applied>>>, placed_terminal: Option<PlacedTerminal>, reservation: Arc<MemoryReservation>) -> Arc<Self> {
        Arc::new(Self { body, block, placed_terminal, append: OnceLock::new(), completion: Mutex::new(completion), _reservation: reservation })
    }
    pub fn append_stamp(&self) -> Option<&AppendStamp> { self.append.get() }
    pub fn sequence(&self) -> Option<TermSequence> { self.append_stamp().map(|stamp| stamp.sequence) }
    fn stamp_append(&self, sequence: TermSequence, frame: &[u8]) -> Result<()> {
        let fingerprint = matches!(&self.body, WriteBody::Resolved(..)).then(|| fingerprint(frame));
        self.append.set(AppendStamp { sequence, fingerprint })
            .map_err(|_| Anomaly::fault("rtdb.replication/invalid-transition", "Pending replica write was submitted twice"))
    }
    pub fn finish(&self, result: Result<Applied>) { if let Some(completion) = self.completion.lock().take() { let _ = completion.send(result); } }
    pub fn finish_readback_failure(&self, error: Anomaly) {
        if let Some(outcome) = transaction_completion(TransactionEvent::ReadbackFailed(error)) {
            self.finish(outcome);
        }
    }
}
impl Drop for PendingWrite { fn drop(&mut self) { if let Some(completion) = self.completion.get_mut().take() { let _ = completion.send(Err(term_ended())); } } }

struct AppendRequest { write: Arc<PendingWrite>, durable: Option<oneshot::Sender<Result<ReplicaMetadata>>> }

#[derive(Clone)]
pub(crate) struct AppenderHandle { sender: mpsc::UnboundedSender<AppendRequest> }
impl AppenderHandle {
    pub fn enqueue(&self, write: Arc<PendingWrite>) -> Result<()> {
        self.sender.send(AppendRequest { write, durable: None }).map_err(|_| term_ended())
    }
    pub async fn append_durable(&self, write: Arc<PendingWrite>) -> Result<ReplicaMetadata> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(AppendRequest { write, durable: Some(sender) }).map_err(|_| term_ended())?;
        receiver.await.map_err(|_| term_ended())?
    }
}

pub(crate) struct ReplicaAppender {
    logs: Arc<PartitionLogs>, memory: Arc<MemoryBudget>, term: Term, sequence: TermSequence,
    queue: mpsc::UnboundedReceiver<AppendRequest>, assertion_interval: Duration, cancellation: CancellationToken, clock: ReplicationClock,
    heartbeat: bool,
    durable_commit_observer: Option<DurableCommitObserver>,
    clock_lease: Option<ClockLease>,
}

/// How long one leader clock lease lasts past the sample that issues it.
/// It exceeds twice the drift bound, as Multi's writer lease does.
pub(crate) const LEADER_CLOCK_LEASE_MICROS: i64 = 2_000_000;

/// The leader's clock lease (T7). A term decides only while its clock plus
/// the drift bound is before the latest lease it issued, so every stamp it
/// takes lies below that lease. It issues its first lease only once its
/// clock passes the latest lease applied before the term plus the drift
/// bound, and renews on the clock's schedule, never per record. The
/// takeover therefore waits once, on the lease alone: never on the last
/// stamp, which would make the wait depend on hidden commits.
pub(crate) struct ClockLease {
    /// The latest lease applied before this term began.
    fence: i64,
    drift: i64,
    /// The latest lease this term has enqueued, read by the driver's decision gate.
    issued: Arc<AtomicI64>,
}

impl ClockLease {
    /// The lease to issue at `now`: once the takeover fence has passed, and
    /// when the issued lease ends within half a period.
    fn due(&self, now: i64) -> Option<i64> {
        (now > self.fence.saturating_add(self.drift)
            && now.saturating_add(LEADER_CLOCK_LEASE_MICROS / 2) >= self.issued.load(Ordering::Acquire))
            .then(|| now.saturating_add(LEADER_CLOCK_LEASE_MICROS))
    }

    /// The lease a record right after a block boundary carries, so a node
    /// resuming after that boundary learns it without replaying earlier records.
    fn carried(&self) -> Option<i64> {
        Some(self.fence.max(self.issued.load(Ordering::Acquire))).filter(|until| *until > i64::MIN)
    }
}

/// A lease record: a no-op carrying only the lease end.
fn clock_lease_record(until: i64) -> ReplicaBody {
    ReplicaBody::NoOp(proto::NoOp { clock_lease_micros: Some(until), ..Default::default() })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommitLagBasis { Ordered, Staged }

type DurableCommitObserver = Arc<dyn Fn(CommitLagBasis, Option<Duration>) + Send + Sync>;

struct DurableCommitObservation {
    ordered_at: Timestamp,
    staging_clock_upper: Option<Timestamp>,
    clock: ReplicationClock,
    observer: DurableCommitObserver,
}

impl DurableCommitObservation {
    fn record(self) {
        let observed = (self.clock)();
        let observed_upper = observed.0.checked_add(1);
        let lag_since = |start: Timestamp| observed_upper
            .and_then(|upper| upper.checked_sub(start.0))
            .filter(|micros| *micros >= 0)
            .and_then(|micros| micros.checked_mul(1_000))
            .map(|nanos| Duration::from_nanos(nanos as u64));
        // An ordered timestamp can lead this wall clock. The stage sample already
        // includes rounding, so it can equal the observation's upper tick.
        let ordered = if observed < self.ordered_at { None } else { lag_since(self.ordered_at) };
        (self.observer)(CommitLagBasis::Ordered, ordered);
        if let Some(staged) = self.staging_clock_upper {
            (self.observer)(CommitLagBasis::Staged, lag_since(staged));
        }
    }
}

impl ReplicaAppender {
    pub fn new(logs: Arc<PartitionLogs>, memory: Arc<MemoryBudget>, term: Term, assertion_interval: Duration, cancellation: CancellationToken, clock: ReplicationClock) -> (AppenderHandle, Self) {
        let (sender, queue) = mpsc::unbounded_channel();
        (AppenderHandle { sender }, Self { logs, memory, term, sequence: TermSequence(1), queue, assertion_interval, cancellation, clock, heartbeat: true, durable_commit_observer: None, clock_lease: None })
    }
    pub fn with_heartbeat(mut self, heartbeat: bool) -> Self { self.heartbeat = heartbeat; self }
    /// Holds a clock lease past `fence`, the latest lease applied before this term,
    /// publishing each enqueued lease end in `issued`.
    pub fn with_clock_lease(mut self, fence: i64, drift: i64, issued: Arc<AtomicI64>) -> Self {
        self.clock_lease = Some(ClockLease { fence, drift, issued });
        self
    }
    pub fn starting_at(mut self, sequence: TermSequence) -> Self { self.sequence = sequence; self }
    pub fn with_durable_commit_observer(mut self, observer: impl Fn(CommitLagBasis, Option<Duration>) + Send + Sync + 'static) -> Self {
        self.durable_commit_observer = Some(Arc::new(observer));
        self
    }
    fn durable_commit_observation(&self, body: &WriteBody) -> Option<DurableCommitObservation> {
        let observer = self.durable_commit_observer.as_ref()?;
        let WriteBody::Resolved(tx, _) = body else { return None; };
        (tx.result().committed() && tx.placed_attempt().is_some()).then(|| DurableCommitObservation {
            ordered_at: tx.result().tx_key().system_time,
            staging_clock_upper: tx.staging_clock_upper(),
            clock: Arc::clone(&self.clock),
            observer: Arc::clone(observer),
        })
    }
    pub async fn run(mut self, mut first: Option<Arc<PendingWrite>>) -> Result<()> {
        let mut timer = tokio::time::interval(self.assertion_interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        timer.tick().await;
        // The lease renews on the clock's own schedule, whatever the traffic.
        let mut lease_timer = tokio::time::interval(Duration::from_micros((LEADER_CLOCK_LEASE_MICROS / 8) as u64));
        lease_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut receipts: futures::stream::FuturesUnordered<futures::future::BoxFuture<'static, Result<()>>> = futures::stream::FuturesUnordered::new();
        use futures::StreamExt;
        let outcome = loop {
            let start_with_confirmation = first.is_some();
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => break Ok(()),
                receipt = receipts.next(), if !receipts.is_empty() => {
                    if let Some(Err(error)) = receipt { break Err(error); }
                },
                _ = lease_timer.tick(), if self.clock_lease.is_some() && !start_with_confirmation && !self.logs.data_suspended() => {
                    let Some(until) = self.clock_lease.as_ref().and_then(|lease| lease.due((self.clock)().0)) else { continue; };
                    let sequence = self.sequence;
                    self.sequence = match sequence.next() { Ok(next) => next, Err(error) => break Err(error) };
                    let envelope = ReplicaEnvelope { term: self.term, term_sequence: Some(sequence), message: clock_lease_record(until) };
                    match observe_enqueue(self.logs.enqueue_replica(&envelope, BlockIndex(0), (self.clock)()), &self.cancellation, &mut receipts).await {
                        Ok(Some(receipt)) => {
                            // Every later record of this term follows the lease in the log,
                            // so a decision stamped under it is durable only after it.
                            if let Some(lease) = &self.clock_lease { lease.issued.fetch_max(until, Ordering::AcqRel); }
                            receipts.push(track_receipt(receipt, None, None, None));
                        }
                        Ok(None) => break Ok(()),
                        Err(error) => break Err(error),
                    }
                }
                _ = timer.tick(), if self.heartbeat && !start_with_confirmation && !self.logs.data_suspended() => {
                    let sequence = self.sequence;
                    self.sequence = match sequence.next() { Ok(next) => next, Err(error) => break Err(error) };
                    let envelope = ReplicaEnvelope { term: self.term, term_sequence: Some(sequence), message: ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }) };
                    match observe_enqueue(self.logs.enqueue_replica(&envelope, BlockIndex(0), (self.clock)()), &self.cancellation, &mut receipts).await {
                        Ok(Some(receipt)) => receipts.push(track_receipt(receipt, None, None, None)),
                        Ok(None) => break Ok(()),
                        Err(error) => break Err(error),
                    }
                }
                request = async {
                    match first.take() {
                        Some(write) => Some(AppendRequest { write, durable: None }),
                        None => self.queue.recv().await,
                    }
                } => {
                    let Some(request) = request else { break Ok(()); };
                    let sequence = self.sequence;
                    self.sequence = match sequence.next() { Ok(next) => next, Err(error) => break Err(error) };
                    let resolved;
                    let (body, allow_offload) = match &request.write.body {
                        WriteBody::Resolved(tx, _) => match tx.inline_replica_body() {
                            Some(body) => (body, false),
                            None => match tx.to_replica_body(&self.memory) {
                                Ok(body) => { resolved = body; (resolved.body(), true) }
                                Err(error) => { request.write.finish(Err(error.clone())); break Err(error); }
                            },
                        },
                        WriteBody::Message(body) => (body, true),
                    };
                    let copy_bound = body.encoded_len().checked_mul(4).and_then(|bytes| bytes.checked_add(8192)).ok_or_else(|| Anomaly::fault("rtdb.replication/payload-size", "Append message copy size overflow"));
                    let _copy_reservation = match copy_bound.and_then(|bytes| self.memory.reserve(bytes)) { Ok(reservation) => reservation, Err(error) => { request.write.finish(Err(error.clone())); break Err(error); } };
                    let updated;
                    let search = request.write.body.search_delta().filter(|_| {
                        matches!(body, ReplicaBody::ResolvedTx(tx) if tx.acceptance_stage.is_none())
                    });
                    let _search_charge = search.and_then(|delta| self.memory.reserve(delta.encoded.len().saturating_mul(8)).ok());
                    let body = match body {
                        ReplicaBody::ResolvedTx(tx) if request.write.placed_terminal.is_some() || _search_charge.is_some() => {
                            let mut tx = tx.clone();
                            if let Some(terminal) = request.write.placed_terminal {
                                tx.placed_slot_terminal = Some(terminal.encode());
                            }
                            if let Some(delta) = search.filter(|_| _search_charge.is_some()) {
                                tx.search_delta = Some(delta.encoded.clone());
                            }
                            updated = ReplicaBody::ResolvedTx(tx);
                            &updated
                        }
                        _ => body,
                    };
                    let boundary = matches!(body, ReplicaBody::BlockBoundary(_));
                    let guarded = match body {
                        ReplicaBody::ResolvedTx(tx) => tx.acceptance_stage.is_some(),
                        ReplicaBody::NoOp(message) => crate::serving::is_funded_original(message),
                        _ => false,
                    };
                    let observation = self.durable_commit_observation(&request.write.body);
                    let enqueue = self.logs.enqueue_replica_with(self.term, Some(sequence), body, request.write.block, || (self.clock)(), allow_offload,
                        |frame| request.write.stamp_append(sequence, frame));
                    let enqueued = observe_enqueue(enqueue, &self.cancellation, &mut receipts).await;
                    drop(_copy_reservation);
                    match enqueued {
                        Ok(Some(receipt)) => {
                            if let WriteBody::Resolved(tx, _) = &request.write.body
                                && let Some(outcome) = transaction_completion(TransactionEvent::Enqueued(tx.result()))
                            {
                                request.write.finish(outcome.map(|result| Applied::Transaction(Arc::new(result.clone()))));
                            }
                            let receipt = track_receipt(receipt, Some(Arc::clone(&request.write)), request.durable, observation);
                            if guarded {
                                // A guarded provider owns the tail until its exact quorum settles.
                                // Keep heartbeats and later requests out of that flight.
                                let result = tokio::select! {
                                    biased;
                                    _ = self.cancellation.cancelled() => break Ok(()),
                                    result = receipt => result,
                                };
                                if let Err(error) = result { break Err(error); }
                            } else {
                                receipts.push(receipt);
                            }
                            timer.reset();
                            // A node resuming after this boundary replays only what follows
                            // it, so the lease is carried in the very next record.
                            if let Some(until) = self.clock_lease.as_ref().filter(|_| boundary).and_then(ClockLease::carried) {
                                let sequence = self.sequence;
                                self.sequence = match sequence.next() { Ok(next) => next, Err(error) => break Err(error) };
                                let envelope = ReplicaEnvelope { term: self.term, term_sequence: Some(sequence), message: clock_lease_record(until) };
                                match observe_enqueue(self.logs.enqueue_replica(&envelope, BlockIndex(0), (self.clock)()), &self.cancellation, &mut receipts).await {
                                    Ok(Some(receipt)) => receipts.push(track_receipt(receipt, None, None, None)),
                                    Ok(None) => break Ok(()),
                                    Err(error) => break Err(error),
                                }
                            }
                        }
                        Ok(None) => { request.write.finish(Err(term_ended())); if let Some(sender) = request.durable { let _ = sender.send(Err(term_ended())); } break Ok(()); }
                        Err(error) => { request.write.finish(Err(error.clone())); if let Some(sender) = request.durable { let _ = sender.send(Err(error.clone())); } break Err(error); }
                    }
                }
            }
        };
        self.queue.close();
        while let Ok(request) = self.queue.try_recv() {
            request.write.finish(Err(term_ended()));
            if let Some(sender) = request.durable { let _ = sender.send(Err(term_ended())); }
        }
        drop(receipts);
        outcome
    }
}

async fn observe_enqueue(
    enqueue: impl std::future::Future<Output = Result<crate::ReplicaReceipt>>,
    cancellation: &CancellationToken,
    receipts: &mut futures::stream::FuturesUnordered<futures::future::BoxFuture<'static, Result<()>>>,
) -> Result<Option<crate::ReplicaReceipt>> {
    use futures::StreamExt;
    tokio::pin!(enqueue);
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(None),
            receipt = receipts.next(), if !receipts.is_empty() => { if let Some(result) = receipt { result?; } }
            receipt = &mut enqueue => return receipt.map(Some),
        }
    }
}

fn track_receipt(receipt: crate::ReplicaReceipt, write: Option<Arc<PendingWrite>>, durable: Option<oneshot::Sender<Result<ReplicaMetadata>>>, observation: Option<DurableCommitObservation>) -> futures::future::BoxFuture<'static, Result<()>> {
    Box::pin(async move {
        let outcome = receipt.durable().await;
        if outcome.is_ok() && let Some(observation) = observation { observation.record(); }
        if let Some(sender) = durable { let _ = sender.send(outcome.clone()); }
        if let Err(error) = &outcome { if let Some(write) = write { write.finish(Err(error.clone())); } }
        outcome.map(|_| ())
    })
}

pub(crate) fn complete_record(pending: &mut VecDeque<Arc<PendingWrite>>, sequence: Option<TermSequence>, result: Result<Applied>) {
    if let Some(index) = pending.iter().position(|item| item.sequence().is_some_and(|expected| {
        matching_present_integer(sequence.map(|sequence| sequence.0), expected.0)
    })) {
        if let Some(item) = pending.remove(index) { item.finish(result); }
    }
}
pub(crate) fn now() -> Timestamp { Timestamp(chrono::Utc::now().timestamp_micros()) }
pub(crate) fn join_error(error: tokio::task::JoinError) -> Anomaly { Anomaly::fault("rtdb.replication/child-task", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use async_trait::async_trait;
    use bytes::Bytes;
    use rtdb_storage::{MemoryStore, cache::BufferPool, log::{AppendReceipt, Log, LogOffset, LogRecord, LogTail, MemoryLog}};
    use rtdb_types::StorageEpoch;

    struct ReceiptFailure { readable: MemoryLog }
    #[async_trait]
    impl Log for ReceiptFailure {
        async fn enqueue(&self, _: Bytes, _: Timestamp) -> rtdb_storage::Result<AppendReceipt> {
            let (sender, receiver) = oneshot::channel();
            let _ = sender.send(Err(rtdb_storage::Error::Closed("provider rejected durability".into())));
            Ok(AppendReceipt::new(receiver))
        }
        fn latest(&self) -> rtdb_storage::Result<LogOffset> { self.readable.latest() }
        async fn tail(&self, after: LogOffset) -> rtdb_storage::Result<Box<dyn LogTail>> { self.readable.tail(after).await }
        async fn range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Box<dyn rtdb_storage::log::LogRange>> { self.readable.range(from, to).await }
        async fn read_range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Vec<LogRecord>> { self.readable.read_range(from, to).await }
    }
    fn pump(interval: Duration) -> (AppenderHandle, ReplicaAppender, Arc<MemoryBudget>) {
        let memory = MemoryBudget::new(1 << 24);
        let store = Arc::new(MemoryStore::default());
        let cache = Arc::new(BufferPool::new(store.clone(), 1 << 20));
        let source = Arc::new(MemoryLog::new(1 << 20));
        let replica = Arc::new(ReceiptFailure { readable: MemoryLog::new(1 << 20) });
        let logs = PartitionLogs::new(source, replica, StorageEpoch(0), StorageEpoch(0), rtdb_format::keys::CURRENT_STORAGE_VERSION, StorageEpoch(0), store, cache, Arc::clone(&memory)).unwrap();
        let (handle, appender) = ReplicaAppender::new(logs, Arc::clone(&memory), Term(1), interval, CancellationToken::new(), Arc::new(now));
        (handle, appender, memory)
    }
    #[tokio::test]
    async fn ordinary_append_receipt_failure_stops_pump_and_fails_its_completion() {
        let (handle, appender, memory) = pump(Duration::from_secs(60));
        let (sender, receiver) = oneshot::channel();
        let write = PendingWrite::new(WriteBody::Message(ReplicaBody::NoOp(proto::NoOp { src_msg_id: Some(1), ..Default::default() })), BlockIndex(0), Some(sender), &memory).unwrap();
        handle.enqueue(write).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), appender.run(None)).await.unwrap().unwrap_err();
        assert!(error.message.as_deref().is_some_and(|message| message.contains("provider rejected durability")));
        assert!(receiver.await.unwrap().unwrap_err().message.as_deref().is_some_and(|message| message.contains("provider rejected durability")));
    }
    #[tokio::test]
    async fn heartbeat_receipt_failure_stops_pump_without_waiting_for_readback() {
        let (_handle, appender, _) = pump(Duration::from_millis(1));
        let error = tokio::time::timeout(Duration::from_secs(1), appender.run(None)).await.unwrap().unwrap_err();
        assert!(error.message.as_deref().is_some_and(|message| message.contains("provider rejected durability")));
    }

    #[tokio::test]
    async fn durability_measurement_uses_the_ordered_stamp_and_excludes_failed_acknowledgements() {
        let (_handle, appender, _) = pump(Duration::from_secs(60));
        let envelope = ReplicaEnvelope { term: Term(1), term_sequence: Some(TermSequence(1)), message: ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }) };
        let receipt = appender.logs.enqueue_replica(&envelope, BlockIndex(0), Timestamp(110)).await.unwrap();
        let samples = Arc::new(Mutex::new(Vec::new()));
        let observer: DurableCommitObserver = {
            let samples = Arc::clone(&samples);
            Arc::new(move |basis, lag| samples.lock().push((basis, lag)))
        };
        let observation = |ordered, staged: Option<i64>, observed| DurableCommitObservation {
            ordered_at: Timestamp(ordered), staging_clock_upper: staged.map(Timestamp),
            clock: Arc::new(move || Timestamp(observed)), observer: Arc::clone(&observer),
        };
        assert!(track_receipt(receipt, None, None, Some(observation(100, Some(115), 120))).await.is_err());
        assert!(samples.lock().is_empty());
        observation(100, Some(115), 120).record();
        observation(100, Some(121), 120).record();
        observation(100, Some(101), 99).record();
        observation(100, None, 120).record();
        assert_eq!(samples.lock().as_slice(), [
            (CommitLagBasis::Ordered, Some(Duration::from_micros(21))),
            (CommitLagBasis::Staged, Some(Duration::from_micros(6))),
            (CommitLagBasis::Ordered, Some(Duration::from_micros(21))),
            (CommitLagBasis::Staged, Some(Duration::ZERO)),
            (CommitLagBasis::Ordered, None),
            (CommitLagBasis::Staged, None),
            (CommitLagBasis::Ordered, Some(Duration::from_micros(21))),
        ]);
    }

    #[tokio::test]
    async fn term_cancellation_drops_backpressured_enqueue_without_waiting_for_provider_shutdown() {
        let cancellation = CancellationToken::new();
        let mut receipts = futures::stream::FuturesUnordered::new();
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        struct DropSignal(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropSignal { fn drop(&mut self) { self.0.store(true, Ordering::Release); } }
        let guard = DropSignal(Arc::clone(&dropped));
        let enqueue = async move { let _guard = guard; std::future::pending::<Result<crate::ReplicaReceipt>>().await };
        let cancel = async { tokio::task::yield_now().await; cancellation.cancel(); };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(1), async { tokio::join!(observe_enqueue(enqueue, &cancellation, &mut receipts), cancel) }).await.unwrap();
        assert!(result.unwrap().is_none());
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn a_leader_with_traffic_appends_no_heartbeats_and_an_idle_one_appends_after_the_interval() {
        let memory = MemoryBudget::new(1 << 24);
        let store = Arc::new(MemoryStore::default());
        let cache = Arc::new(BufferPool::new(store.clone(), 1 << 20));
        let replica = Arc::new(MemoryLog::new(1 << 20));
        let logs = PartitionLogs::new(Arc::new(MemoryLog::new(1 << 20)), replica.clone(), StorageEpoch(0), StorageEpoch(0), rtdb_format::keys::CURRENT_STORAGE_VERSION, StorageEpoch(0), store, cache, Arc::clone(&memory)).unwrap();
        let cancellation = CancellationToken::new();
        let (handle, appender) = ReplicaAppender::new(logs, Arc::clone(&memory), Term(1), Duration::from_millis(200), cancellation.clone(), Arc::new(now));
        let pump = tokio::spawn(appender.run(None));
        for source in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let write = PendingWrite::new(WriteBody::Message(ReplicaBody::NoOp(proto::NoOp { src_msg_id: Some(source), ..Default::default() })), BlockIndex(0), None, &memory).unwrap();
            handle.enqueue(write).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(replica.latest().unwrap(), LogOffset(19), "only the 20 writes were appended");
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert_eq!(replica.latest().unwrap(), LogOffset(20), "one heartbeat after the idle interval");
        cancellation.cancel();
        pump.await.unwrap().unwrap();
    }
}
