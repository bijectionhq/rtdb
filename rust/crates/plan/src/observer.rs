use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionTimer { Query, TxAwait, TxSubmit, TxExecute, PgwireTransaction, TxOp, CompactorJob }
impl ExecutionTimer {
    pub fn name(self) -> &'static str { match self { Self::Query => "query.timer", Self::TxAwait => "node.tx.await", Self::TxSubmit => "node.tx.submit", Self::TxExecute => "node.tx.execute", Self::PgwireTransaction => "pgwire.tx.latency", Self::TxOp => "tx.op.timer", Self::CompactorJob => "compactor.job.timer" } }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionError { Query, Transaction }
impl ExecutionError { pub fn name(self) -> &'static str { match self { Self::Query => "query.error", Self::Transaction => "tx.error" } } }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionEvent { Opened, Cancelled, Refused }

/// Callbacks complete synchronously and must not panic or retain operation payloads.
pub trait ExecutionObserver: Send + Sync {
    fn record_duration(&self, timer: ExecutionTimer, elapsed: Duration);
    fn record_error(&self, error: ExecutionError);
    fn connection_event(&self, _event: ConnectionEvent) {}
}

pub struct ExecutionSample { observer: Option<Arc<dyn ExecutionObserver>>, timer: ExecutionTimer, started: Instant }
impl ExecutionSample {
    /// Dropping the sample records one duration, including a cancelled operation's elapsed time.
    pub fn new(observer: Option<Arc<dyn ExecutionObserver>>, timer: ExecutionTimer) -> Self { Self { observer, timer, started: Instant::now() } }
}
impl Drop for ExecutionSample { fn drop(&mut self) { if let Some(observer) = &self.observer { observer.record_duration(self.timer, self.started.elapsed()); } } }
