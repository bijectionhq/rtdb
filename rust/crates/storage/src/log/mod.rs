mod local;
mod memory;
mod read_only;

pub use local::{LocalLog, LocalLogDriver, LocalLogOptions};
pub use memory::MemoryLog;
pub use read_only::{ReadOnlyLocalLog, ReadOnlyLocalLogOptions};

use crate::{Error, Result, object_store::ReadReservation};
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

/// The provider samples this clock after preparing the local append and before allocating its position.
pub struct GuardedAppend<'a> {
    clock: &'a (dyn Fn() -> Timestamp + Sync),
    staged_upper: Timestamp,
    cutoff: Timestamp,
    journal_before: Option<Timestamp>,
}

impl<'a> GuardedAppend<'a> {
    pub fn new(clock: &'a (dyn Fn() -> Timestamp + Sync), staged_upper: Timestamp, cutoff: Timestamp, journal_before: Option<Timestamp>) -> Self {
        Self { clock, staged_upper, cutoff, journal_before }
    }

    pub fn journal_before(&self) -> Option<Timestamp> { self.journal_before }

    pub fn sample(self) -> Result<Timestamp> {
        let now = (self.clock)();
        let upper = now.0.checked_add(1).map(Timestamp);
        if self.journal_before.is_some_and(|before| before < self.cutoff)
            || upper.is_none_or(|upper| upper.0 <= 0 || upper < self.staged_upper || upper >= self.cutoff)
        {
            return Err(Error::GuardedAppendCutoff);
        }
        Ok(now)
    }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaObjectScope {
    pub deployment: uuid::Uuid,
    pub incarnation: uuid::Uuid,
    pub group: uuid::Uuid,
    pub generation: u64,
    pub writer_epoch: u64,
}

#[derive(Clone)]
pub struct ReplicaObjectCustody {
    pub scope: ReplicaObjectScope,
    _hold: Arc<dyn Send + Sync>,
}

impl ReplicaObjectCustody {
    pub fn new(scope: ReplicaObjectScope, hold: Arc<dyn Send + Sync>) -> Self {
        Self { scope, _hold: hold }
    }
}

impl std::fmt::Debug for ReplicaObjectCustody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.scope.fmt(formatter)
    }
}

impl PartialEq for ReplicaObjectCustody {
    fn eq(&self, other: &Self) -> bool { self.scope == other.scope }
}
impl Eq for ReplicaObjectCustody {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    pub metadata: LogMetadata,
    pub payload: Bytes,
    pub replica_objects: Option<ReplicaObjectCustody>,
}

const LOG_RECORD_OVERHEAD_BYTES: usize = 256;

impl LogRecord {
    pub fn new(metadata: LogMetadata, payload: Bytes) -> Self {
        Self { metadata, payload, replica_objects: None }
    }

    /// Keeps the caller's pre-read charge alive for every clone of the payload.
    pub fn retain(self, reservation: ReadReservation) -> Result<Self> {
        if reservation.length() < self.payload.len().saturating_add(LOG_RECORD_OVERHEAD_BYTES) { return Err(Error::Task("log record exceeds its read reservation".into())); }
        Ok(self.with_read_reservation(reservation))
    }

    fn with_read_reservation(self, reservation: ReadReservation) -> Self {
        struct Retained { bytes: Bytes, _reservation: ReadReservation }
        impl AsRef<[u8]> for Retained { fn as_ref(&self) -> &[u8] { &self.bytes } }
        Self {
            metadata: self.metadata,
            payload: Bytes::from_owner(Retained { bytes: self.payload, _reservation: reservation }),
            replica_objects: self.replica_objects,
        }
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

    /// Exposes caller-reserved single-record delivery when the backend supports it.
    fn replay(&mut self) -> Option<&mut dyn LogReplayTail> { None }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayHead {
    pub metadata: LogMetadata,
    /// Length of the encoded payload, in bytes.
    pub payload_len: usize,
    /// Minimum caller reservation, in bytes, for delivery by this backend.
    pub allocation_bound: usize,
}

#[async_trait]
pub trait LogReplayTail: Send {
    /// Waits up to the timeout for the next visible record and prepares its delivery bound.
    /// Preparation leaves the delivered position unchanged.
    /// Any allocation of the delivered payload uses the reservation passed to `read_head`.
    /// Returns `None` only when the cursor has reached the observed tip.
    async fn poll_head(&mut self, timeout: Duration) -> Result<Option<ReplayHead>>;

    /// Reads the prepared head using a reservation of at least its allocation bound in bytes.
    /// Success advances the cursor and keeps the reservation with the delivered payload through its final clone.
    /// An insufficient reservation returns an error. Failure or cancellation leaves the cursor at its current position.
    /// Offloaded work keeps the reservation through the read and disposal of an undelivered result.
    async fn read_head(&mut self, head: ReplayHead, reservation: ReadReservation) -> Result<LogRecord>;
}

/// An installed journal's current ordinary budget and selected checkpoint.
#[derive(Clone, Copy, Debug)]
pub struct AppendCapacity {
    pub available_bytes: u64,
    pub published_through: LogOffset,
    pub last_data: Option<LogOffset>,
}

#[async_trait]
pub trait Log: Send + Sync {
    /// This replica log requires accepted uploads to bind the exact catalog bytes.
    fn requires_checkpoint_digest(&self) -> bool { false }
    /// The reopened journal retains its synced prefix, and reads expose only that durable history.
    fn retains_synced_history(&self) -> bool { false }
    /// The provider's fixed size limit for one append, before any provider-specific wrapping.
    /// Absence means callers cannot admit an inline-only write using this provider.
    fn maximum_append_bytes(&self) -> Option<usize> { None }
    /// The provider checks the writer's final ordered staging sample before append.
    fn maximum_guarded_append_bytes(&self) -> Option<usize> { None }
    /// The provider can enforce an exclusive cutoff on the durable sync of an
    /// exact original quorum, including when a successor resolves the append.
    fn maximum_timed_original_append_bytes(&self) -> Option<usize> { None }
    /// Fixes append order before returning; the receipt reports whether those bytes became durable.
    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt>;
    /// Appends a recovery record through reserved maintenance capacity when the provider has it.
    async fn enqueue_maintenance(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        self.enqueue(payload, timestamp).await
    }
    /// The ordinary journal bytes currently available, measured using the provider's charge in AppendCapacity.
    async fn available_append_capacity(&self) -> Result<Option<AppendCapacity>> { Ok(None) }
    /// In-process source owners can release bytes through this durably applied position.
    fn record_applied(&self, _through: LogOffset) {}
    /// Fixes append order only if the provider's final admission sample is inside
    /// the guard. A journal cutoff also requires the timed original quorum.
    async fn enqueue_guarded(&self, _payload: Bytes, _guard: GuardedAppend<'_>) -> Result<AppendReceipt> {
        Err(Error::GuardedAppendUnsupported)
    }
    fn latest(&self) -> Result<LogOffset>;
    /// Reports a prefix floor without opening a cursor. This applies to providers where the first
    /// requested replay offset is the preceding offset plus one. An observation does not reserve records.
    async fn retained_floor(&self) -> Result<Option<LogOffset>> { Ok(None) }
    /// Opens a replay cursor during database recovery, before the log driver starts.
    /// Providers returning a cursor must validate its starting position at admission.
    /// A discarded prefix reports the first requested and first retained offsets.
    /// Providers that defer admission return None and open the cursor when the driver runs.
    async fn recovery_tail(&self, _after: LogOffset) -> Result<Option<Box<dyn LogTail>>> { Ok(None) }
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
