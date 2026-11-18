mod local;
mod memory;
mod read_only;

pub use local::{LocalLog, LocalLogDriver, LocalLogOptions};
pub use memory::MemoryLog;
pub use read_only::{ReadOnlyLocalLog, ReadOnlyLocalLogOptions};

use crate::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use rtdb_types::Timestamp;
use std::{sync::Arc, time::Duration};
use std::{future::Future, pin::Pin};
use tokio::sync::{oneshot, watch};

pub trait AppendTimestampSource: Send + Sync {
    /// Samples a record's timestamp in append order before publication.
    /// An error fails the append without advancing the durable tip.
    fn timestamp(&self, payload: &[u8], supplied: Timestamp) -> Result<Timestamp>;
}

struct SuppliedTimestamp;
impl AppendTimestampSource for SuppliedTimestamp {
    fn timestamp(&self, _: &[u8], supplied: Timestamp) -> Result<Timestamp> { Ok(supplied) }
}

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

impl LogRecord {
    /// Keeps the caller's pre-read charge alive for every clone of the payload.
    pub fn retain(self, reservation: crate::object_store::ReadReservation) -> Result<Self> {
        if reservation.length() < self.payload.len().saturating_add(256) { return Err(Error::Task("log record exceeds its read reservation".into())); }
        struct Retained { bytes: Bytes, _reservation: crate::object_store::ReadReservation }
        impl AsRef<[u8]> for Retained { fn as_ref(&self) -> &[u8] { &self.bytes } }
        Ok(Self { metadata: self.metadata, payload: Bytes::from_owner(Retained { bytes: self.payload, _reservation: reservation }) })
    }
}

#[async_trait]
pub trait LogRange: Send {
    /// The caller reserves this many bytes before each read; returned payloads retain that charge.
    fn allocation_bound(&self) -> usize;
    /// Returns one record from the captured range, or None at its permanent end.
    async fn next(&mut self, reservation: crate::object_store::ReadReservation) -> Result<Option<LogRecord>>;
}

enum Completion {
    Driver(oneshot::Receiver<Result<LogMetadata>>),
    Observed(Pin<Box<dyn Future<Output = Result<LogMetadata>> + Send>>),
}

pub struct AppendReceipt { completion: Completion }

impl AppendReceipt {
    /// The driver must complete this receiver only after durable provider acknowledgement, or with an append error.
    pub fn new(durable: oneshot::Receiver<Result<LogMetadata>>) -> Self { Self { completion: Completion::Driver(durable) } }

    /// Wraps receipt observation without changing the accepted write's backend ownership.
    /// The future runs only while the caller polls `durable`; dropping it cancels observation, not the accepted append.
    pub fn observed(future: impl Future<Output = Result<LogMetadata>> + Send + 'static) -> Self {
        Self { completion: Completion::Observed(Box::pin(future)) }
    }

    /// Reports durable append, independently of replica admission or application.
    pub async fn durable(self) -> Result<LogMetadata> {
        match self.completion {
            Completion::Driver(durable) => durable.await.map_err(|_| Error::Closed("append driver ended before reporting durability".into()))?,
            Completion::Observed(future) => future.await,
        }
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
    /// Captures record starts in [from, to), bounded by the current durable tip.
    /// A missing start within the captured tip is an error; starts beyond the tip return an empty range.
    async fn range(&self, from: LogOffset, to: LogOffset) -> Result<Box<dyn LogRange>>;
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
