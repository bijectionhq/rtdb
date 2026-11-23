use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{envelope::{ReplicaBody, ReplicaEnvelope, ReplicaFrame, SourceMessage}, keys::oversized_key};
use rtdb_storage::{Error as StorageError, IoRegion, ObjectKey, ObjectStore, cache::BufferPool, log::{AppendCapacity, AppendReceipt, GuardedAppend, Log, LogMetadata, LogOffset, LogRecord, LogTail}};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, ReplicaPosition, Result, SourcePosition, StorageEpoch, StorageVersion, Term, TermSequence, Timestamp};

use crate::readback_identity::{ReplicaFingerprint, fingerprint};

pub(crate) const APPEND_CAPACITY: &str = "rtdb.storage/append-capacity";
pub(crate) const STRICT_DISPATCH_CUTOFF: &str = "rtdb.replication/strict-dispatch-cutoff";
pub(crate) const STRICT_DISPATCH_UNAVAILABLE: &str = "rtdb.replication/strict-dispatch-unavailable";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaMetadata { pub position: ReplicaPosition, pub timestamp: Timestamp }

#[derive(Clone, Debug)]
pub struct ReplicaRecord {
    pub metadata: ReplicaMetadata,
    pub envelope: Arc<ReplicaEnvelope>,
    reservation: Arc<MemoryReservation>,
    pub(crate) fingerprint: Option<ReplicaFingerprint>,
    oversized_delivery: Option<LogRecord>,
}
impl ReplicaRecord { pub fn retained_bytes(&self) -> usize { self.reservation.bytes() } }

pub(crate) enum DecodedReplica {
    Known(ReplicaRecord),
    Unrecognized { term: Term, sequence: Option<TermSequence> },
}

pub struct ReplicaReceipt { receipt: AppendReceipt, epoch: StorageEpoch }
impl ReplicaReceipt {
    pub async fn durable(self) -> Result<ReplicaMetadata> {
        let metadata = self.receipt.durable().await?;
        Ok(ReplicaMetadata { position: ReplicaPosition::from_epoch_offset(self.epoch, metadata.offset.0)?, timestamp: metadata.timestamp })
    }
}

struct EncodedPayload { bytes: Vec<u8>, _reservation: Arc<MemoryReservation> }
impl AsRef<[u8]> for EncodedPayload { fn as_ref(&self) -> &[u8] { &self.bytes } }
impl EncodedPayload {
    fn encode(size: usize, memory: &Arc<MemoryBudget>, encode: impl FnOnce() -> Result<Vec<u8>>) -> Result<Bytes> {
        let mut reservation = memory.reserve(encoded_allocation(size)?)?;
        let bytes = encode()?;
        let retained = bytes.capacity().checked_add(256).ok_or_else(size_overflow)?;
        Arc::get_mut(&mut reservation).expect("payload encoding owns its reservation").shrink_to(retained)?;
        Ok(Bytes::from_owner(Self { bytes, _reservation: reservation }))
    }
}

pub struct PartitionLogs {
    source: Arc<dyn Log>, replica: Arc<dyn Log>,
    pub source_epoch: StorageEpoch, pub replica_epoch: StorageEpoch,
    pub storage_version: StorageVersion, pub storage_epoch: StorageEpoch,
    store: Arc<dyn ObjectStore>, cache: Arc<BufferPool>, memory: Arc<MemoryBudget>,
    data_suspended: AtomicBool,
}
impl PartitionLogs {
    pub fn new(source: Arc<dyn Log>, replica: Arc<dyn Log>, source_epoch: StorageEpoch, replica_epoch: StorageEpoch,
        storage_version: StorageVersion, storage_epoch: StorageEpoch, store: Arc<dyn ObjectStore>, cache: Arc<BufferPool>, memory: Arc<MemoryBudget>) -> Result<Arc<Self>> {
        if Arc::ptr_eq(&source, &replica) { return Err(Anomaly::incorrect("rtdb.replication/aliased-logs", "Source and replica logs must be distinct")); }
        SourcePosition::from_epoch_offset(source_epoch, -1)?;
        ReplicaPosition::from_epoch_offset(replica_epoch, -1)?;
        Ok(Arc::new(Self { source, replica, source_epoch, replica_epoch, storage_version, storage_epoch, store, cache, memory, data_suspended: AtomicBool::new(false) }))
    }
    pub fn source_floor(&self) -> Result<SourcePosition> { SourcePosition::from_epoch_offset(self.source_epoch, -1) }
    pub fn replica_floor(&self) -> Result<ReplicaPosition> { ReplicaPosition::from_epoch_offset(self.replica_epoch, -1) }
    pub(crate) fn storage(&self) -> &Arc<dyn ObjectStore> { &self.store }
    pub(crate) fn memory(&self) -> &Arc<MemoryBudget> { &self.memory }
    pub(crate) fn local_recovery_store(&self) -> Option<&rtdb_storage::LocalStore> {
        if self.retains_recovery_history() { self.store.local_maintenance_store() } else { None }
    }
    pub(crate) fn recovery_block_store(&self, file_size_target: i64) -> Result<Arc<crate::BlockStore>> {
        crate::BlockStore::new(Arc::clone(&self.store), Arc::clone(&self.cache), Arc::clone(&self.memory), file_size_target)
    }
    pub(crate) fn requires_checkpoint_digest(&self) -> bool { self.replica.requires_checkpoint_digest() }
    pub(crate) async fn confirms_recovery_checkpoint(&self, catalog: &rtdb_format::catalog::BlockCatalog) -> bool {
        if !self.replica.needs_published_recovery_checkpoint() { return true; }
        let (Some(boundary), Some(root)) = (catalog.boundary_replica_position(), catalog.publication_sha256()) else { return false; };
        let through = LogOffset(boundary.after_offset(self.replica_epoch));
        through.0 >= 0 && self.replica.confirms_recovery_checkpoint(catalog.block_index().0, through, root).await.unwrap_or(false)
    }
    pub(crate) fn fences_writer_terms(&self) -> bool { self.replica.fences_writer_terms() }
    pub(crate) async fn holds_writer_grant(&self) -> Result<bool> { Ok(self.replica.holds_writer_grant().await?) }
    pub(crate) fn retains_recovery_history(&self) -> bool {
        self.source.retains_synced_history() && self.replica.retains_synced_history() && self.store.syncs_checkpoints()
    }
    pub fn source_position(&self, metadata: LogMetadata) -> Result<SourcePosition> { SourcePosition::from_epoch_offset(self.source_epoch, metadata.offset.0) }
    pub fn latest_source_position(&self) -> Result<SourcePosition> { SourcePosition::from_epoch_offset(self.source_epoch, self.source.latest()?.0) }
    pub fn latest_replica_position(&self) -> Result<ReplicaPosition> { ReplicaPosition::from_epoch_offset(self.replica_epoch, self.replica.latest()?.0) }
    /// An empty epoch has no replica position that startup must wait to read.
    pub fn startup_replica_target(&self) -> Result<Option<ReplicaPosition>> {
        let offset = self.replica.latest()?.0;
        if offset < 0 { Ok(None) } else { Ok(Some(ReplicaPosition::from_epoch_offset(self.replica_epoch, offset)?)) }
    }
    pub async fn source_tail(&self, after: SourcePosition) -> Result<Box<dyn LogTail>> { self.source.tail(LogOffset(after.after_offset(self.source_epoch))).await.map_err(Into::into) }
    pub async fn replica_tail(&self, after: ReplicaPosition) -> Result<Box<dyn LogTail>> { self.open_replica_tail(after).await.map_err(Into::into) }
    pub(crate) async fn open_replica_tail(&self, after: ReplicaPosition) -> rtdb_storage::Result<Box<dyn LogTail>> {
        self.replica.tail(LogOffset(after.after_offset(self.replica_epoch))).await
    }
    pub(crate) async fn recovery_tail(&self, after: ReplicaPosition) -> rtdb_storage::Result<Option<Box<dyn LogTail>>> {
        let offset = after.after_offset(self.replica_epoch);
        let tail = self.replica.recovery_tail(LogOffset(offset)).await?;
        if tail.is_none()
            && let Some(LogOffset(first_retained)) = self.replica.retained_floor().await?
            && let Some(requested) = crate::replay_floor::missing_replay_offset(offset, first_retained)
        {
            return Err(StorageError::UnavailableLogRange { requested, first_retained });
        }
        Ok(tail)
    }
    pub(crate) fn is_recovery_prefix_gap(&self, after: ReplicaPosition, error: &StorageError) -> bool {
        match error {
            StorageError::UnavailableLogRange { requested, first_retained } =>
                crate::replay_floor::missing_replay_offset(after.after_offset(self.replica_epoch), *first_retained) == Some(*requested),
            _ => false,
        }
    }
    pub async fn source_range(&self, from: SourcePosition, to: SourcePosition) -> Result<Box<dyn rtdb_storage::log::LogRange>> {
        let (from, to) = inspection_offsets(from.0, to.0, self.source_epoch)?;
        self.source.range(from, to).await.map_err(Into::into)
    }
    pub async fn replica_range(&self, from: ReplicaPosition, to: ReplicaPosition) -> Result<Box<dyn rtdb_storage::log::LogRange>> {
        let (from, to) = inspection_offsets(from.0, to.0, self.replica_epoch)?;
        self.replica.range(from, to).await.map_err(Into::into)
    }
    pub fn validate_source_boundary(&self, database: &str, after: SourcePosition) -> Result<()> {
        let offset = after.after_offset(self.source_epoch);
        let latest = self.source.latest()?.0;
        if after.0 >= 0 && after.epoch() > self.source_epoch || offset > latest {
            let log_state = if latest == -1 { "the log is empty".to_owned() } else { format!("epoch={}, offset={latest}", self.source_epoch.0) };
            let message = format!("Database '{database}' failed to start due to an invalid transaction log state ({log_state}) \
                that does not correspond with the latest processed message (epoch={} and offset={}).\n\n\
                Please see https://docs.xtdb.com/ops/backup-and-restore/out-of-sync-log.html for more information and next steps.", after.epoch().0, after.offset());
            return Err(Anomaly::fault("xtdb/out-of-sync-log", message)
                .with_context("persisted-position", after.0.into()).with_context("source-latest-offset", latest.into()));
        }
        Ok(())
    }
    pub async fn append_source(&self, message: &SourceMessage, now: Timestamp) -> Result<SourcePosition> {
        let payload = EncodedPayload::encode(message.encoded_len(), &self.memory, || message.encode())?;
        self.append_source_encoded(payload, now).await
    }
    pub async fn append_source_encoded(&self, payload: Bytes, now: Timestamp) -> Result<SourcePosition> {
        let metadata = self.source.enqueue(payload, now).await?.durable().await?;
        self.source_position(metadata)
    }
    pub async fn enqueue_replica(&self, envelope: &ReplicaEnvelope, block: BlockIndex, now: Timestamp) -> Result<ReplicaReceipt> {
        if matches!(&envelope.message, ReplicaBody::ResolvedTx(tx) if tx.acceptance_stage.is_some()) {
            return Err(Anomaly::fault("rtdb.replication/strict-dispatch-clock", "Strict replica writes require the live writer clock"));
        }
        self.enqueue_replica_with(envelope, block, || now, true, |_| Ok(())).await
    }
    pub(crate) fn data_suspended(&self) -> bool { self.data_suspended.load(Ordering::Acquire) }
    pub(crate) fn suspend_data(&self, suspend: bool) { self.data_suspended.store(suspend, Ordering::Release); }
    pub(crate) fn require_data_intake(&self) -> Result<()> {
        if self.data_suspended() {
            return Err(Anomaly::new(AnomalyCategory::Busy, APPEND_CAPACITY, "The replica journal is freeing ordinary write capacity"));
        }
        Ok(())
    }
    pub(crate) async fn available_append_capacity(&self) -> Result<Option<AppendCapacity>> {
        Ok(self.replica.available_append_capacity().await?)
    }
    pub(crate) fn record_source_applied(&self, position: SourcePosition) {
        self.source.record_applied(LogOffset(position.after_offset(self.source_epoch)));
    }
    pub(crate) fn replica_guarded_append_limit(&self) -> Option<usize> { self.replica.maximum_guarded_append_bytes() }
    pub(crate) fn replica_timed_original_append_limit(&self) -> Option<usize> {
        self.replica.maximum_timed_original_append_bytes()
    }
    pub(crate) async fn enqueue_replica_with(&self, envelope: &ReplicaEnvelope, block: BlockIndex, clock: impl Fn() -> Timestamp + Send + Sync,
        allow_offload: bool, before_enqueue: impl FnOnce(&[u8]) -> Result<()>) -> Result<ReplicaReceipt> {
        let stage = match &envelope.message {
            ReplicaBody::ResolvedTx(tx) => tx.checked_acceptance_stage()?,
            _ => None,
        };
        let payload = EncodedPayload::encode(envelope.encoded_len(), &self.memory, || Ok(envelope.encode()))?;
        before_enqueue(&payload)?;
        if let Some(stage) = stage {
            let guard = GuardedAppend::new(&clock, stage.clock_upper, stage.cutoff, stage.journal);
            let receipt = self.replica.enqueue_guarded(payload, guard).await.map_err(|error| match error {
                error @ StorageError::GuardedAppendCutoff => Anomaly::new(AnomalyCategory::Unavailable, STRICT_DISPATCH_CUTOFF, error.to_string()),
                error @ StorageError::GuardedAppendUnavailable => Anomaly::new(AnomalyCategory::Unavailable, STRICT_DISPATCH_UNAVAILABLE, error.to_string()),
                error => error.into(),
            })?;
            return Ok(ReplicaReceipt { receipt, epoch: self.replica_epoch });
        }
        let now = clock();
        // Only the term's initial claim and the block close can consume reserved journal space.
        // Keep that class when a large block upload is represented by an offload reference.
        // A journal that fences writer terms has no claims, so its sequence-zero no-op is ordinary data.
        let maintenance = match &envelope.message {
            ReplicaBody::BlockBoundary(_) | ReplicaBody::BlockUploaded(_) => true,
            ReplicaBody::NoOp(message) => !self.fences_writer_terms()
                && envelope.term_sequence == Some(TermSequence(0)) && message.src_msg_id.is_none(),
            _ => false,
        };
        let enqueue = |payload| async move {
            if maintenance { self.replica.enqueue_maintenance(payload, now).await }
            else { self.replica.enqueue(payload, now).await }
        };
        let receipt = match enqueue(payload.clone()).await {
            Ok(receipt) => receipt,
            Err(StorageError::MessageTooLarge { .. }) if allow_offload => {
                let payload_id = uuid::Uuid::new_v4().to_string();
                let key = ObjectKey::new(oversized_key(block, &payload_id)?)?;
                let mut proxy = envelope.offloaded(self.storage_version, self.storage_epoch, block, payload_id, &payload);
                if let Some(version) = self.store.put_replica_payload(&key, payload).await? {
                    proxy.bind_offload_publication(version)?;
                }
                let payload = EncodedPayload::encode(proxy.encoded_len(), &self.memory, || Ok(proxy.encode()))?;
                enqueue(payload).await?
            }
            Err(error) => return Err(error.into()),
        };
        Ok(ReplicaReceipt { receipt, epoch: self.replica_epoch })
    }
    pub fn decode_replica(&self, record: LogRecord) -> Result<Option<ReplicaRecord>> {
        Ok(self.decode_readback(record, None, None)?.known())
    }
    pub(crate) fn decode_reserved_replica(&self, record: LogRecord, reservation: Arc<MemoryReservation>) -> Result<Option<ReplicaRecord>> {
        Ok(self.decode_readback(record, Some(reservation), None)?.known())
    }
    pub(crate) fn decode_readback(&self, record: LogRecord, reservation: Option<Arc<MemoryReservation>>, own_term: Option<Term>) -> Result<DecodedReplica> {
        let reservation = match reservation {
            Some(reservation) => reservation,
            None => self.memory.reserve(replica_decode_bound(record.payload.len())?)?,
        };
        let metadata = ReplicaMetadata { position: ReplicaPosition::from_epoch_offset(self.replica_epoch, record.metadata.offset.0)?, timestamp: record.metadata.timestamp };
        let frame = ReplicaFrame::decode(&record.payload)?;
        let (term, sequence) = (frame.term, frame.term_sequence);
        let Some(envelope) = frame.into_envelope() else { return Ok(DecodedReplica::Unrecognized { term, sequence }); };
        let fingerprint = (Some(term) == own_term && matches!(&envelope.message, ReplicaBody::ResolvedTx(_) | ReplicaBody::OversizedMessage(_)))
            .then(|| fingerprint(&record.payload));
        let oversized_delivery = matches!(&envelope.message, ReplicaBody::OversizedMessage(_)).then_some(record);
        Ok(DecodedReplica::Known(ReplicaRecord { metadata, envelope: Arc::new(envelope), reservation, fingerprint, oversized_delivery }))
    }
    /// Admission runs on the outer envelope before this fetch; a held block receives the resolved body.
    pub async fn resolve_oversized(&self, mut record: ReplicaRecord) -> Result<ReplicaRecord> {
        if record.oversized_delivery.as_ref().is_some_and(|delivery| delivery.replica_objects.is_some()) {
            return self.resolve_oversized_fresh(record).await;
        }
        let Some(key) = record.envelope.oversized_payload(self.storage_version, self.storage_epoch)? else { return Ok(record); };
        let delivery = record.take_oversized_delivery()?;
        let key = ObjectKey::new(key)?;
        let lease = IoRegion::retaining(Arc::new(delivery), self.cache.get(&key)).await?;
        self.decode_oversized(record, lease.as_slice())
    }

    pub(crate) async fn resolve_oversized_fresh(&self, mut record: ReplicaRecord) -> Result<ReplicaRecord> {
        let Some(key) = record.envelope.oversized_payload(self.storage_version, self.storage_epoch)? else { return Ok(record); };
        let delivery = record.take_oversized_delivery()?;
        let key = ObjectKey::new(key)?;
        if let Some(custody) = &delivery.replica_objects {
            if !custody.matches(delivery.metadata.offset, &key) {
                return Err(Anomaly::fault("rtdb.replication/oversized-delivery",
                    "The replica payload permit does not match this delivery"));
            }
            let payload = self.store.read_replica_payload(custody, &self.memory).await?;
            return self.decode_oversized(record, payload.as_ref());
        }
        IoRegion::retaining(Arc::new(delivery), async {
            let store = &self.store;
            let length = usize::try_from(store.head_fresh(&key).await?.size).map_err(|_| size_overflow())?;
            let allocation = store.exact_object_allocation(&key, length)?;
            let charge = allocation.retained_bytes.checked_add(allocation.scratch_bytes).ok_or_else(size_overflow)?;
            let charge = self.memory.reserve(charge)?;
            let payload = IoRegion::retaining(charge.clone(), store.get_fresh_exact(&key, length)).await?;
            self.decode_oversized(record, &payload)
        }).await
    }

    fn decode_oversized(&self, record: ReplicaRecord, payload: &[u8]) -> Result<ReplicaRecord> {
        let reservation = self.memory.reserve(replica_decode_bound(payload.len())?)?;
        let envelope = record.envelope.unwrap_payload(payload)?;
        let fingerprint = record.fingerprint.map(|_| fingerprint(payload));
        Ok(ReplicaRecord { metadata: record.metadata, envelope: Arc::new(envelope), reservation, fingerprint, oversized_delivery: None })
    }
}

impl ReplicaRecord {
    fn take_oversized_delivery(&mut self) -> Result<LogRecord> {
        self.oversized_delivery.take().ok_or_else(|| Anomaly::fault(
            "rtdb.replication/oversized-delivery", "An offloaded replica record has no admitted delivery",
        ))
    }
}

impl DecodedReplica {
    fn known(self) -> Option<ReplicaRecord> {
        match self { Self::Known(record) => Some(record), Self::Unrecognized { .. } => None }
    }
}

fn inspection_offsets(from: i64, to: i64, epoch: StorageEpoch) -> Result<(LogOffset, LogOffset)> {
    if from >= to { return Ok((LogOffset(0), LogOffset(0))); }
    let base = SourcePosition::from_epoch_offset(epoch, 0)?.0;
    let from = from.checked_sub(base).filter(|offset| (0..(1_i64 << 48)).contains(offset));
    let to = to.checked_sub(base).filter(|offset| (0..=(1_i64 << 48)).contains(offset));
    match (from, to) {
        (Some(from), Some(to)) => Ok((LogOffset(from), LogOffset(to))),
        _ => Err(Anomaly::incorrect("xtdb/log-epoch-mismatch", "Inspection bounds must belong to the configured log epoch")),
    }
}

fn encoded_allocation(payload: usize) -> Result<usize> {
    payload.checked_add(64).and_then(|bytes| bytes.checked_mul(4)).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_overflow)
}
pub(crate) fn replica_decode_bound(payload_len: usize) -> Result<usize> {
    payload_len.checked_mul(4).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_overflow)
}
fn size_overflow() -> Anomaly { Anomaly::fault("rtdb.replication/payload-size", "Replica payload size overflow") }
