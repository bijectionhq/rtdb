use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionTimer { Query, TxAwait, TxSubmit, TxExecute, PgwireTransaction, TxOp, CompactorJob, DurableCommitLag, StagedCommitLag, BlockUpload, TrieDelete, BlockFileDelete, TableBlockFileDelete, OffloadedPayloadDelete, SourceAppend, ResolveSource, ReplicaAppend, ReadBackApply }
impl ExecutionTimer {
    pub fn name(self) -> &'static str { match self { Self::Query => "query.timer", Self::TxAwait => "node.tx.await", Self::TxSubmit => "node.tx.submit", Self::TxExecute => "node.tx.execute", Self::PgwireTransaction => "pgwire.tx.latency", Self::TxOp => "tx.op.timer", Self::CompactorJob => "compactor.job.timer", Self::DurableCommitLag => "rtdb.replica.commit.ordered_to_durable", Self::StagedCommitLag => "rtdb.replica.commit.staged_to_durable", Self::BlockUpload => "block.upload.timer", Self::TrieDelete => "xtdb.gc.tries.delete.timer", Self::BlockFileDelete => "xtdb.gc.block_files.delete.timer", Self::TableBlockFileDelete => "xtdb.gc.table_block_files.delete.timer", Self::OffloadedPayloadDelete => "xtdb.gc.offloaded_payloads.delete.timer",
        Self::SourceAppend => "rtdb.tx.source.append", Self::ResolveSource => "rtdb.tx.resolve",
        Self::ReplicaAppend => "rtdb.tx.replica.append", Self::ReadBackApply => "rtdb.tx.readback.apply" } }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionError { Query, Transaction }
impl ExecutionError { pub fn name(self) -> &'static str { match self { Self::Query => "query.error", Self::Transaction => "tx.error" } } }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionEvent { Opened, Cancelled, Refused }

/// The replica log records a follower times, as the reference's `FollowerLogProcessor` does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicaRecordKind { ResolvedTx, TriesAdded, BlockBoundary, BlockUploaded, TriesDeleted }
impl ReplicaRecordKind {
    pub const ALL: [Self; 5] = [Self::ResolvedTx, Self::TriesAdded, Self::BlockBoundary, Self::BlockUploaded, Self::TriesDeleted];
    /// The reference's `msg.type` tag.
    pub fn name(self) -> &'static str {
        match self { Self::ResolvedTx => "ResolvedTx", Self::TriesAdded => "TriesAdded", Self::BlockBoundary => "BlockBoundary", Self::BlockUploaded => "BlockUploaded", Self::TriesDeleted => "TriesDeleted" }
    }
}

/// Callbacks complete synchronously and must not panic or retain operation payloads.
pub trait ExecutionObserver: Send + Sync {
    fn record_duration(&self, timer: ExecutionTimer, elapsed: Duration);
    fn record_error(&self, error: ExecutionError);
    fn connection_event(&self, _event: ConnectionEvent) {}
    /// A query opened from a plan that carries warnings, upstream's `query.warning`.
    fn record_query_warning(&self) {}
    /// An acknowledged placed commit, measured from its ordered timestamp to local observation of durability.
    /// `None` reports that the ordered and observed wall-clock coordinates could not be compared.
    fn record_durable_commit_lag(&self, lag: Option<Duration>) {
        if let Some(lag) = lag { self.record_duration(ExecutionTimer::DurableCommitLag, lag); }
    }
    /// An acknowledged strict commit, measured from its rounded writer cutoff sample.
    /// `None` reports an incomparable clock sample.
    fn record_staged_commit_lag(&self, lag: Option<Duration>) {
        if let Some(lag) = lag { self.record_duration(ExecutionTimer::StagedCommitLag, lag); }
    }
    /// A follower applied one replica log record of `database`: the reference's `xtdb.replica.process.timer`.
    fn record_replica_process(&self, _database: &str, _kind: ReplicaRecordKind, _elapsed: Duration) {}
    /// A follower adopted a block of `database`, `elapsed` after its boundary, having held `records` records meanwhile:
    /// the reference's `xtdb.replica.block.buffer.timer` and `xtdb.replica.block.buffered.records`.
    fn record_replica_block_buffer(&self, _database: &str, _elapsed: Duration, _records: usize) {}
}

pub struct ExecutionSample { observer: Option<Arc<dyn ExecutionObserver>>, timer: ExecutionTimer, started: Instant }
impl ExecutionSample {
    /// Dropping the sample records one duration, including a cancelled operation's elapsed time.
    pub fn new(observer: Option<Arc<dyn ExecutionObserver>>, timer: ExecutionTimer) -> Self { Self { observer, timer, started: Instant::now() } }
}
impl Drop for ExecutionSample { fn drop(&mut self) { if let Some(observer) = &self.observer { observer.record_duration(self.timer, self.started.elapsed()); } } }
