mod guarded_sample;
pub(crate) mod journal_file;
pub(crate) mod local;
mod memory;
mod read_only;

pub use local::{LocalLog, LocalLogDriver, LocalLogOptions};
pub use memory::MemoryLog;
pub use read_only::{ReadOnlyLocalLog, ReadOnlyLocalLogOptions};

use crate::{Error, Result, object_store::ReadReservation};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use rtdb_columnar::MemoryReservation;
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
    journal_before: Option<rtdb_types::TimedJournalCutoff>,
}

impl<'a> GuardedAppend<'a> {
    pub fn new(clock: &'a (dyn Fn() -> Timestamp + Sync), staged_upper: Timestamp, cutoff: Timestamp, journal_before: Option<rtdb_types::TimedJournalCutoff>) -> Self {
        Self { clock, staged_upper, cutoff, journal_before }
    }

    pub fn journal_before(&self) -> Option<rtdb_types::TimedJournalCutoff> { self.journal_before }

    pub fn sample(self) -> Result<Timestamp> {
        let now = (self.clock)();
        guarded_sample::guarded_sample(
            now.0,
            self.staged_upper.0,
            self.cutoff.0,
            self.journal_before.map(|cutoff| cutoff.before().0),
        )
        .map(Timestamp)
        .ok_or(Error::GuardedAppendCutoff)
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

mod replica_objects;
pub use replica_objects::{ReplicaObjectCustody, ReplicaObjectScope, ReplicaPayload, ReplicaPayloadBroker, ReplicaPayloadReference};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    pub metadata: LogMetadata,
    pub payload: Bytes,
    pub replica_objects: Option<ReplicaObjectCustody>,
    /// The committed provider frame's timed authority, separate from the payload's claim.
    pub timed_original: Option<rtdb_types::TimedJournalCutoff>,
}

const LOG_RECORD_OVERHEAD_BYTES: usize = 256;

impl LogRecord {
    pub fn new(metadata: LogMetadata, payload: Bytes) -> Self {
        Self { metadata, payload, replica_objects: None, timed_original: None }
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
            timed_original: self.timed_original,
        }
    }
}

/// An authenticated interval with a custody owner retained through answer release.
pub struct ProtectedLogRange {
    /// Provider-attested original history and terminal coordinate, when available.
    pub authenticated_head: Option<[u8; 32]>,
    pub range: Box<dyn LogRange>,
    pub hold: Arc<dyn Send + Sync>,
}

pub struct ProtectedLogSnapshot {
    pub suffix: Option<ProtectedLogRange>,
    pub provider: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

#[async_trait]
pub trait LogRange: Send {
    /// The caller reserves this many bytes before each read; returned payloads retain that charge.
    fn allocation_bound(&self) -> usize;
    /// Returns one record from the captured range, or None at its permanent end.
    async fn next(&mut self, reservation: crate::object_store::ReadReservation) -> Result<Option<LogRecord>>;
    /// Retain provider file addresses from a delivered record accepted by the catalog replay.
    fn admit_snapshot_files(&mut self, _position: LogOffset) -> Result<()> { Ok(()) }
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

pub struct ReplicaReferenceSource {
    pub store: Arc<dyn crate::ObjectStore>,
    pub cache: Arc<crate::cache::BufferPool>,
    pub memory: Arc<rtdb_columnar::MemoryBudget>,
    pub checkpoint: Option<rtdb_format::catalog::BlockCatalog>,
    pub version: rtdb_types::StorageVersion,
    pub epoch: rtdb_types::StorageEpoch,
    pub file_size_target: i64,
}

/// The ordered source and replica log port for the default log processor.
#[async_trait]
pub trait SharedLog: Send + Sync {
    /// Accepts one record and fixes its append order.
    /// The receipt reports the durable position and the provider's recorded timestamp.
    async fn append(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt>;

    /// Observes the latest durable record position, or the start sentinel for an empty log.
    fn latest(&self) -> Result<LogOffset>;

    /// Opens ordered replay strictly after a record position or the start sentinel.
    /// Replay reports a missing retained prefix as an error.
    async fn read_after(&self, after: LogOffset) -> Result<Box<dyn LogReplayTail>>;
}

pub enum AppendMode<'a> {
    Ordinary(Timestamp),
    Maintenance(Timestamp),
    Guarded(GuardedAppend<'a>),
}

/// A producer's exclusive frame buffer and the charge covering its allocation.
pub struct AppendBuffer {
    bytes: BytesMut,
    prefix: usize,
    reservation: Option<Arc<MemoryReservation>>,
}

impl AppendBuffer {
    pub fn new(bytes: BytesMut, prefix: usize, reservation: Arc<MemoryReservation>) -> Self {
        Self { bytes, prefix, reservation: Some(reservation) }
    }

    pub fn payload(&self) -> &[u8] { &self.bytes[self.prefix..] }
    pub fn headroom(&self) -> (usize, usize) { (self.prefix, self.bytes.capacity() - self.bytes.len()) }
    pub fn storage_mut(&mut self) -> &mut BytesMut { &mut self.bytes }
    pub fn set_prefix(&mut self, prefix: usize) { self.prefix = prefix; }
    pub fn take_reservation(&mut self) -> Option<Arc<MemoryReservation>> { self.reservation.take() }
    pub fn restore_reservation(&mut self, reservation: Option<Arc<MemoryReservation>>) { self.reservation = reservation; }

    pub fn take_bytes(&mut self) -> Bytes {
        std::mem::take(&mut self.bytes).freeze().slice(self.prefix..)
    }

    pub fn into_bytes(mut self) -> Bytes {
        let bytes = self.take_bytes();
        self.retain(bytes)
    }

    fn retain(&self, bytes: Bytes) -> Bytes {
        struct Charged { bytes: Bytes, _reservation: Arc<MemoryReservation> }
        impl AsRef<[u8]> for Charged { fn as_ref(&self) -> &[u8] { &self.bytes } }
        match &self.reservation {
            Some(reservation) => Bytes::from_owner(Charged { bytes, _reservation: Arc::clone(reservation) }),
            None => bytes,
        }
    }

    pub async fn enqueue_shared(&mut self, log: &(impl Log + ?Sized), mode: AppendMode<'_>) -> Result<AppendReceipt> {
        let bytes = std::mem::take(&mut self.bytes).freeze();
        let payload = self.retain(bytes.slice(self.prefix..));
        let result = match mode {
            AppendMode::Ordinary(time) => log.enqueue(payload, time).await,
            AppendMode::Maintenance(time) => log.enqueue_maintenance(payload, time).await,
            AppendMode::Guarded(guard) => log.enqueue_guarded(payload, guard).await,
        };
        if matches!(result, Err(Error::MessageTooLarge { .. })) {
            self.bytes = bytes.try_into_mut().unwrap_or_else(|bytes| BytesMut::from(bytes.as_ref()));
        }
        result
    }

    pub async fn append(&mut self, log: &dyn SharedLog, timestamp: Timestamp) -> Result<AppendReceipt> {
        let bytes = std::mem::take(&mut self.bytes).freeze();
        let result = log.append(self.retain(bytes.slice(self.prefix..)), timestamp).await;
        if matches!(result, Err(Error::MessageTooLarge { .. })) {
            self.bytes = bytes.try_into_mut().unwrap_or_else(|bytes| BytesMut::from(bytes.as_ref()));
        }
        result
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CheckpointPrefix {
    /// The inclusive position covered by the selected checkpoint in this provider's coordinates.
    pub through: i64,
    pub checkpoint_sha256: [u8; 32],
    pub unreadable_by: Timestamp,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
pub enum CheckpointReplicaReference {
    Inline,
    Detached { storage_version: rtdb_types::StorageVersion, storage_epoch: rtdb_types::StorageEpoch, key: String },
}

pub struct PriorCheckpointReferences {
    pub first_live: LogOffset,
    pub detached: Vec<CheckpointReplicaReference>,
}

#[async_trait]
pub trait CheckpointKeyRetirement: Send + Sync {
    /// The immutable topic identity of an original Kafka Default erasure attachment.
    fn default_erasure_topic(&self) -> Option<[u8; 16]> { None }

    async fn validate_default_erasure_history(&self, _through: LogOffset, _recovery_after: LogOffset) -> rtdb_types::Result<()> {
        Err(rtdb_types::Anomaly::unsupported("rtdb/default-erasure-history", "The log has no Default erasure history authority"))
    }

    async fn verifies_checkpoint_prefix(&self, _cut: CheckpointPrefix) -> rtdb_types::Result<bool> {
        Ok(false)
    }

    /// Dense offsets allow the inventory scan to reject a missing record within the retained prefix.
    fn checkpoint_offsets_are_dense(&self) -> bool { false }

    async fn prior_checkpoint_references(&self, _through: LogOffset) -> rtdb_types::Result<Option<PriorCheckpointReferences>> {
        Ok(None)
    }

    /// Reads a completed Local prefix and the first byte offset beyond it.
    /// The object custodian separately verifies retirement of detached payloads.
    async fn settled_local_checkpoint_prefix(&self) -> rtdb_types::Result<Option<(CheckpointPrefix, LogOffset)>> {
        Ok(None)
    }

    /// Checks the provider's exact prefix before key admission changes.
    async fn validate_checkpoint_prefix(&self, cut: CheckpointPrefix) -> rtdb_types::Result<()>;
    /// Closes key admission and settles a batch after the caller has closed older recovery cuts.
    async fn settle_checkpoint_prefix(&self, cut: CheckpointPrefix) -> rtdb_types::Result<bool>;
}

#[async_trait]
pub trait Log: Send + Sync {
    /// Supplies the opened provider's authority to retire its own checkpoint-covered keys.
    fn checkpoint_key_retirement(&self) -> Option<&dyn CheckpointKeyRetirement> { None }
    /// The identity stored in the opened native replica journal, if it has a lineage header.
    fn journal_lineage(&self) -> Option<uuid::Uuid> { None }
    fn shared(&self) -> Option<&dyn SharedLog> { None }
    fn needs_published_recovery_checkpoint(&self) -> bool { false }
    /// This replica log confers offload access only through a delivered payload permit.
    fn requires_replica_payload_permit(&self) -> bool { false }
    fn install_replica_reference_source(&self, _source: ReplicaReferenceSource) -> Result<()> { Ok(()) }
    /// Publishes a detached body under the replica path's custody.
    async fn publish_replica_payload(
        &self, store: &dyn crate::ObjectStore, key: &crate::ObjectKey, body: Bytes,
    ) -> Result<Option<rtdb_types::PhysicalObjectVersion>> {
        store.put(key, body).await?;
        Ok(None)
    }
    /// A hosted owner attests its exact selected recovery root before a source may confirm it.
    async fn confirms_recovery_checkpoint(&self, _block: i64, _through: LogOffset, _root: [u8; 32]) -> Result<bool> {
        Ok(false)
    }
    /// This replica log requires accepted uploads to bind the exact catalog bytes.
    fn requires_checkpoint_digest(&self) -> bool { false }
    /// The journal admits appends only under its current writer grant and makes a new grant holder replay
    /// through the granted cut, so a replica term needs no claim record.
    fn fences_writer_terms(&self) -> bool { false }
    /// Whether this process still holds the journal's current writer grant.
    /// Only a log that fences writer terms can answer false.
    async fn holds_writer_grant(&self) -> Result<bool> { Ok(true) }
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
    /// Extra prefix and suffix bytes a provider can fill in the encoded allocation.
    fn append_headroom(&self) -> (usize, usize) { (0, 0) }
    /// Consumes the buffer on acceptance and preserves its payload on a size refusal.
    async fn enqueue_buffer(&self, buffer: &mut AppendBuffer, mode: AppendMode<'_>) -> Result<AppendReceipt> {
        buffer.enqueue_shared(self, mode).await
    }
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
    /// Holds the requested checkpoint and suffix when the journal owns checkpoint custody.
    async fn protect_snapshot(
        &self, from: LogOffset, to: LogOffset, _root: [u8; 32],
    ) -> Result<ProtectedLogSnapshot> {
        let suffix = if from == to { None } else { Some(self.protect_range(from, to).await?) };
        Ok(ProtectedLogSnapshot { suffix, provider: None })
    }
    /// Acquires the original retained interval and its erasure protection.
    async fn protect_range(&self, _from: LogOffset, _to: LogOffset) -> Result<ProtectedLogRange> {
        Err(Error::Closed("The journal does not admit protected exact replay".into()))
    }
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
