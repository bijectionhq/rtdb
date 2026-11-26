use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use rtdb_format::{envelope::{ReplicaBody, ReplicaEnvelope}, log::proto::NoOp};
use rtdb_storage::{Error, Result, log::{AppendReceipt, Log, LogMetadata, LogOffset, LogRange, LogRecord, LogReplayTail, LogTail, MemoryLog, ReplayHead}, object_store::ReadReservation};
use rtdb_types::{Term, TermSequence, Timestamp};
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

type Durable = oneshot::Sender<Result<LogMetadata>>;

struct AcceptedClaim { bytes: Bytes, timestamp: Timestamp, durable: Durable }

#[derive(Default)]
struct BootState {
    open: [bool; 3],
    opened: [bool; 3],
    attempt: [bool; 3],
    attempted: [bool; 3],
    deliver: [bool; 3],
    delivered: [bool; 3],
    accepted: Option<AcceptedClaim>,
}

pub(super) struct BootTransport {
    replica: Arc<MemoryLog>,
    state: watch::Sender<BootState>,
    cancellation: CancellationToken,
}

impl BootTransport {
    pub fn new(replica: Arc<MemoryLog>, cancellation: CancellationToken) -> Arc<Self> {
        Arc::new(Self { replica, state: watch::channel(BootState::default()).0, cancellation })
    }

    pub fn actor(self: &Arc<Self>, actor: usize) -> Arc<dyn Log> {
        Arc::new(ActorLog { actor, boot: Arc::clone(self) })
    }

    async fn wait(&self, ready: impl Fn(&BootState) -> bool) -> Result<bool> {
        let mut state = self.state.subscribe();
        self.cancellation.run_until_cancelled(state.wait_for(ready)).await
            .transpose().map(|grant| grant.is_some()).map_err(|_| invalid("boot monitor closed"))
    }

    async fn require(&self, ready: impl Fn(&BootState) -> bool) -> Result<()> {
        if self.wait(ready).await? { Ok(()) }
        else { Err(Error::Closed("Processor simulation stopped".into())) }
    }

    pub async fn open(&self, actor: usize) -> Result<()> {
        self.state.send_modify(|state| state.open[actor] = true);
        self.require(|state| state.opened[actor]).await
    }

    pub async fn attempt(&self, actor: usize) -> Result<()> {
        self.state.send_modify(|state| state.attempt[actor] = true);
        self.require(|state| state.attempted[actor]).await
    }

    pub async fn commit(&self) -> Result<CommittedClaim> {
        if self.replica.latest()? != LogOffset(-1) {
            return Err(invalid("provider advanced before accepted boot claim durability"));
        }
        let mut accepted = None;
        self.state.send_modify(|state| accepted = state.accepted.take());
        let AcceptedClaim { bytes, timestamp, durable } = accepted.ok_or_else(|| invalid("missing accepted boot claim"))?;
        let metadata = self.replica.enqueue(bytes, timestamp).await?.durable().await?;
        if metadata.offset != LogOffset(0) || metadata.timestamp != timestamp {
            return Err(invalid("provider assigned a different boot claim identity"));
        }
        Ok(CommittedClaim { metadata, durable })
    }

    pub async fn deliver(&self, actor: usize) -> Result<()> {
        self.state.send_modify(|state| state.deliver[actor] = true);
        self.require(|state| state.delivered[actor]).await
    }
}

pub(super) struct CommittedClaim { pub metadata: LogMetadata, durable: Durable }

impl CommittedClaim {
    pub fn acknowledge(self) -> Result<()> {
        self.durable.send(Ok(self.metadata)).map_err(|_| invalid("boot claim caller abandoned its durable receipt"))
    }
}

struct ActorLog { actor: usize, boot: Arc<BootTransport> }

fn invalid(message: &str) -> Error { Error::Task(message.into()) }

#[async_trait]
impl Log for ActorLog {
    async fn enqueue(&self, bytes: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        let initial = !self.boot.state.borrow().attempted[self.actor];
        if self.actor == 0 && !initial { return self.boot.replica.enqueue(bytes, timestamp).await; }
        if bytes.len() > 128 { return Err(invalid("native claim exceeded the bounded boot envelope")); }
        let frame = ReplicaEnvelope::decode(&bytes).map_err(|error| Error::Task(error.to_string()))?
            .ok_or_else(|| invalid("missing native replica envelope"))?;
        let claim = frame.term_sequence == Some(TermSequence(0))
            && matches!(frame.message, ReplicaBody::NoOp(NoOp { src_msg_id: None, placed_slot_reservation: None, serving_record: None, clock_lease_micros: None }));
        if !initial {
            return Err(invalid(if claim { "captured peer refuses replica claim" } else { "refused peer attempted an ordinary replica append" }));
        }
        if !claim || frame.term != Term(1) {
            return Err(invalid("native actor's initial append was not the term-one boot claim"));
        }
        self.boot.require(|state| state.attempt[self.actor]).await?;
        if self.actor != 0 {
            self.boot.state.send_modify(|state| state.attempted[self.actor] = true);
            return Err(invalid("captured peer refuses replica claim"));
        }
        let (durable, receipt) = oneshot::channel();
        self.boot.state.send_modify(|state| {
            state.accepted = Some(AcceptedClaim { bytes, timestamp, durable });
            state.attempted[self.actor] = true;
        });
        Ok(AppendReceipt::new(receipt))
    }

    fn latest(&self) -> Result<LogOffset> { self.boot.replica.latest() }

    async fn tail(&self, after: LogOffset) -> Result<Box<dyn LogTail>> {
        if after != LogOffset(-1) { return Err(invalid("native actor opened a nonempty first replica tail")); }
        self.boot.require(|state| state.open[self.actor]).await?;
        let tail = self.boot.replica.tail(after).await?;
        self.boot.state.send_modify(|state| state.opened[self.actor] = true);
        Ok(Box::new(BootTail { actor: self.actor, boot: Arc::clone(&self.boot), tail, first: true }))
    }

    async fn range(&self, from: LogOffset, to: LogOffset) -> Result<Box<dyn LogRange>> { self.boot.replica.range(from, to).await }
    async fn read_range(&self, from: LogOffset, to: LogOffset) -> Result<Vec<LogRecord>> { self.boot.replica.read_range(from, to).await }
}

struct BootTail { actor: usize, boot: Arc<BootTransport>, tail: Box<dyn LogTail>, first: bool }

#[async_trait]
impl LogTail for BootTail {
    async fn poll(&mut self, _: Duration) -> Result<Vec<LogRecord>> {
        Err(invalid("native processor replay requires admitted single-record delivery"))
    }

    fn replay(&mut self) -> Option<&mut dyn LogReplayTail> { Some(self) }
}

#[async_trait]
impl LogReplayTail for BootTail {
    async fn poll_head(&mut self, timeout: Duration) -> Result<Option<ReplayHead>> {
        if self.first && !self.boot.wait(|state| state.deliver[self.actor]).await? { return Ok(None); }
        self.tail.replay().ok_or_else(|| invalid("provider lacks admitted replay"))?.poll_head(timeout).await
    }

    async fn read_head(&mut self, head: ReplayHead, reservation: ReadReservation) -> Result<LogRecord> {
        if self.first && head.metadata.offset != LogOffset(0) {
            return Err(invalid("initial grant must deliver provider offset zero"));
        }
        let record = self.tail.replay().ok_or_else(|| invalid("provider lacks admitted replay"))?.read_head(head, reservation).await?;
        if self.first {
            self.first = false;
            self.boot.state.send_modify(|state| state.delivered[self.actor] = true);
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim() -> Bytes {
        ReplicaEnvelope {
            term: Term(1), term_sequence: Some(TermSequence(0)), message: ReplicaBody::NoOp(NoOp { src_msg_id: None, ..Default::default() }),
        }.encode().into()
    }

    #[tokio::test]
    async fn stopping_an_ungranted_delivery_leaves_the_replica_log_open() {
        let provider = Arc::new(MemoryLog::new(4096));
        let cancellation = CancellationToken::new();
        let transport = BootTransport::new(Arc::clone(&provider), cancellation.clone());
        let actor = transport.actor(0);
        let (tail, opened) = tokio::join!(actor.tail(LogOffset(-1)), transport.open(0));
        opened.unwrap();
        let mut tail = tail.unwrap();
        cancellation.cancel();
        assert!(tail.replay().unwrap().poll_head(Duration::ZERO).await.unwrap().is_none());
        assert_eq!(provider.enqueue(claim(), Timestamp(4)).await.unwrap().durable().await.unwrap().offset, LogOffset(0));
    }

    #[tokio::test]
    async fn accepted_claim_uses_the_provider_only_at_commit_and_refusal_assigns_no_position() {
        let provider = Arc::new(MemoryLog::new(4096));
        let transport = BootTransport::new(Arc::clone(&provider), CancellationToken::new());
        let rejected = transport.actor(1);
        let (attempt, grant) = tokio::join!(rejected.enqueue(claim(), Timestamp(4)), transport.attempt(1));
        assert!(grant.is_ok());
        assert!(attempt.is_err());
        assert_eq!(provider.latest().unwrap(), LogOffset(-1));

        let incumbent = transport.actor(0);
        let (accepted, grant) = tokio::join!(incumbent.enqueue(claim(), Timestamp(4)), transport.attempt(0));
        grant.unwrap();
        let receipt = accepted.unwrap();
        assert_eq!(provider.latest().unwrap(), LogOffset(-1));
        let committed = transport.commit().await.unwrap();
        assert_eq!(provider.latest().unwrap(), LogOffset(0));
        assert_eq!(committed.metadata.timestamp, Timestamp(4));
        let durable = receipt.durable();
        tokio::pin!(durable);
        assert!(tokio::time::timeout(Duration::ZERO, &mut durable).await.is_err());
        committed.acknowledge().unwrap();
        assert_eq!(durable.await.unwrap().offset, LogOffset(0));
    }
}
