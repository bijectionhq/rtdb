mod local;
mod memory;

pub use local::{LocalLog, LocalLogDriver, LocalLogOptions};
pub use memory::MemoryLog;

use crate::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use rtdb_types::Timestamp;
use std::{sync::Arc, time::Duration};
use tokio::sync::{oneshot, watch};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogOffset(pub i64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogMetadata {
    pub offset: LogOffset,
    pub timestamp: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    pub metadata: LogMetadata,
    pub payload: Bytes,
}

pub struct AppendReceipt { durable: oneshot::Receiver<Result<LogMetadata>> }

impl AppendReceipt {
    /// Reports durable append, independently of replica admission or application.
    pub async fn durable(self) -> Result<LogMetadata> {
        self.durable.await.map_err(|_| Error::Closed("append driver ended before reporting durability".into()))?
    }
}

#[async_trait]
pub trait LogTail: Send {
    /// Empty means this cursor reached the durable tip; read errors must be returned as errors.
    async fn poll(&mut self, timeout: Duration) -> Result<Vec<LogRecord>>;
}

#[async_trait]
pub trait Log: Send + Sync {
    /// Fixes append order before returning; the receipt reports whether those bytes became durable.
    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt>;
    fn latest(&self) -> Result<LogOffset>;
    async fn tail(&self, after: LogOffset) -> Result<Box<dyn LogTail>>;
    /// Reads record starts in [from, to), rejecting a missing retained start position.
    async fn read_range(&self, from: LogOffset, to: LogOffset) -> Result<Vec<LogRecord>>;
}

#[derive(Clone)]
enum ProgressState {
    Open(LogOffset),
    Failed { latest: LogOffset, cause: Arc<str> },
    Closed(LogOffset),
}

impl ProgressState {
    fn latest(&self) -> LogOffset {
        match self { Self::Open(position) | Self::Closed(position) => *position, Self::Failed { latest, .. } => *latest }
    }

    fn check(&self) -> Result<()> {
        match self {
            Self::Open(_) => Ok(()),
            Self::Failed { cause, .. } => Err(Error::Closed(cause.to_string())),
            Self::Closed(_) => Err(Error::Closed("shutdown".into())),
        }
    }
}

struct Progress { state: watch::Sender<ProgressState> }

impl Progress {
    fn new(latest: LogOffset) -> Self {
        Self { state: watch::channel(ProgressState::Open(latest)).0 }
    }

    fn latest(&self) -> Result<LogOffset> {
        let state = self.state.borrow();
        state.check()?;
        Ok(state.latest())
    }

    fn advance(&self, offset: LogOffset) { self.state.send_replace(ProgressState::Open(offset)); }

    fn fail(&self, cause: String) {
        self.state.send_modify(|state| *state = ProgressState::Failed { latest: state.latest(), cause: cause.into() });
    }

    fn close(&self) {
        self.state.send_modify(|state| {
            if let ProgressState::Open(position) = state { *state = ProgressState::Closed(*position); }
        });
    }
}

async fn await_tip(progress: &Progress, after: LogOffset, timeout: Duration) -> Result<LogOffset> {
    let mut receiver = progress.state.subscribe();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        {
            let state = receiver.borrow_and_update();
            state.check()?;
            if state.latest() > after { return Ok(state.latest()); }
        }
        if timeout.is_zero() { return progress.latest(); }
        match tokio::time::timeout_at(deadline, receiver.changed()).await {
            Ok(Ok(())) => (),
            Ok(Err(_)) => return Err(Error::Closed("progress monitor ended".into())),
            Err(_) => return progress.latest(),
        }
    }
}
