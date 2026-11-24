use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use bytes::{Bytes, BytesMut};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{envelope::{ReplicaBody, ReplicaEnvelope, ReplicaFrame, ReplicaPeek, ReplicaPeekBody, SourceMessage}, keys::oversized_key, log::proto};
use rtdb_storage::{Error as StorageError, IoRegion, ObjectKey, ObjectStore, cache::BufferPool, log::{AppendBuffer, AppendCapacity, AppendMode, AppendReceipt, GuardedAppend, Log, LogMetadata, LogOffset, LogRecord, LogTail}};
use rtdb_storage::object_store::ReadReservation;
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, ReplicaPosition, Result, SourcePosition, StorageEpoch, StorageVersion, Term, TermSequence, Timestamp};

use crate::maintenance_class::is_maintenance;
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

enum OversizedDecode {
    Retained(Option<ReplicaFingerprint>),
    Detached,
}

struct ChargedPayload {
    bytes: Bytes,
    _reservation: Arc<MemoryReservation>,
}

impl AsRef<[u8]> for ChargedPayload {
    fn as_ref(&self) -> &[u8] { &self.bytes }
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

pub(crate) enum PartitionReplay {
    Prepared(Box<dyn LogReplayTail>),
    Legacy(Box<dyn LogTail>),
}

impl PartitionReplay {
    pub(crate) fn replay(&mut self) -> Option<&mut dyn LogReplayTail> {
        match self {
            Self::Prepared(tail) => Some(tail.as_mut()),
            Self::Legacy(tail) => tail.replay(),
        }
    }

    async fn open(log: &dyn Log, after: LogOffset) -> rtdb_storage::Result<Self> {
        match log.shared() {
            Some(shared) => shared.read_after(after).await.map(Self::Prepared),
            None => log.tail(after).await.map(Self::Legacy),
        }
    }
}

fn latest(log: &dyn Log) -> rtdb_storage::Result<LogOffset> {
    match log.shared() {
        Some(shared) => shared.latest(),
        None => log.latest(),
    }
}

pub struct PartitionLogs {
    source: Arc<dyn Log>, replica: Arc<dyn Log>,
    pub source_epoch: StorageEpoch, pub replica_epoch: StorageEpoch,
    pub storage_version: StorageVersion, pub storage_epoch: StorageEpoch,
    store: Arc<dyn ObjectStore>, cache: Arc<BufferPool>, memory: Arc<MemoryBudget>,
    data_suspended: AtomicBool,
    placed_transfer: Option<rtdb_types::PlacedTransfer>,
}
impl PartitionLogs {
    pub fn new(source: Arc<dyn Log>, replica: Arc<dyn Log>, source_epoch: StorageEpoch, replica_epoch: StorageEpoch,
        storage_version: StorageVersion, storage_epoch: StorageEpoch, store: Arc<dyn ObjectStore>, cache: Arc<BufferPool>, memory: Arc<MemoryBudget>) -> Result<Arc<Self>> {
        if Arc::ptr_eq(&source, &replica) { return Err(Anomaly::incorrect("rtdb.replication/aliased-logs", "Source and replica logs must be distinct")); }
        SourcePosition::from_epoch_offset(source_epoch, -1)?;
        ReplicaPosition::from_epoch_offset(replica_epoch, -1)?;
        Ok(Arc::new(Self { source, replica, source_epoch, replica_epoch, storage_version, storage_epoch, store, cache, memory, data_suspended: AtomicBool::new(false), placed_transfer: None }))
    }
    pub fn with_placed_transfer(mut self: Arc<Self>, transfer: Option<rtdb_types::PlacedTransfer>) -> Result<Arc<Self>> {
        if let Some(transfer) = transfer {
            let invalid = || Anomaly::fault("rtdb.replication/transfer-cut", "The placed transfer has another replica interval");
            if transfer.after > transfer.through
                || transfer.debt.as_ref().is_some_and(|debt| debt.reserved_at <= transfer.after || debt.reserved_at > transfer.through)
            {
                return Err(invalid());
            }
            ReplicaPosition::from_epoch_offset(self.replica_epoch, transfer.after)?;
            ReplicaPosition::from_epoch_offset(self.replica_epoch, transfer.through)?;
            Arc::get_mut(&mut self).ok_or_else(invalid)?.placed_transfer = Some(transfer);
        }
        Ok(self)
    }

    pub(crate) fn placed_transfer(&self) -> Option<&rtdb_types::PlacedTransfer> {
        self.placed_transfer.as_ref()
    }

    pub(crate) fn is_copied_replica_position(&self, position: ReplicaPosition) -> bool {
        let offset = position.after_offset(self.replica_epoch);
        self.placed_transfer().is_some_and(|transfer| transfer.after < offset && offset <= transfer.through)
    }

    pub fn source_floor(&self) -> Result<SourcePosition> { SourcePosition::from_epoch_offset(self.source_epoch, -1) }
    pub fn install_replica_reference_source(
        &self, checkpoint: Option<rtdb_format::catalog::BlockCatalog>, file_size_target: i64,
    ) -> Result<()> {
        Ok(self.replica.install_replica_reference_source(rtdb_storage::log::ReplicaReferenceSource {
            store: Arc::clone(&self.store),
            cache: Arc::clone(&self.cache),
            memory: Arc::clone(&self.memory),
            checkpoint,
            version: self.storage_version,
            epoch: self.storage_epoch,
            file_size_target,
        })?)
    }
    pub fn replica_floor(&self) -> Result<ReplicaPosition> { ReplicaPosition::from_epoch_offset(self.replica_epoch, -1) }
    pub(crate) fn storage(&self) -> &Arc<dyn ObjectStore> { &self.store }
    pub(crate) fn memory(&self) -> &Arc<MemoryBudget> { &self.memory }
    pub(crate) fn store(&self) -> &Arc<dyn ObjectStore> { &self.store }
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
    pub fn latest_source_position(&self) -> Result<SourcePosition> { SourcePosition::from_epoch_offset(self.source_epoch, latest(self.source.as_ref())?.0) }
    pub fn latest_replica_position(&self) -> Result<ReplicaPosition> { ReplicaPosition::from_epoch_offset(self.replica_epoch, latest(self.replica.as_ref())?.0) }
    /// An empty epoch has no replica position that startup must wait to read.
    pub fn startup_replica_target(&self) -> Result<Option<ReplicaPosition>> {
        let offset = latest(self.replica.as_ref())?.0;
        if offset < 0 { Ok(None) } else { Ok(Some(ReplicaPosition::from_epoch_offset(self.replica_epoch, offset)?)) }
    }
    pub async fn source_tail(&self, after: SourcePosition) -> Result<Box<dyn LogTail>> { self.source.tail(LogOffset(after.after_offset(self.source_epoch))).await.map_err(Into::into) }
    pub async fn replica_tail(&self, after: ReplicaPosition) -> Result<Box<dyn LogTail>> {
        Ok(self.replica.tail(LogOffset(after.after_offset(self.replica_epoch))).await?)
    }
    pub(crate) async fn source_replay(&self, after: SourcePosition) -> Result<PartitionReplay> {
        Ok(PartitionReplay::open(self.source.as_ref(), LogOffset(after.after_offset(self.source_epoch))).await?)
    }
    pub(crate) async fn replica_replay(&self, after: ReplicaPosition) -> rtdb_storage::Result<PartitionReplay> {
        PartitionReplay::open(self.replica.as_ref(), LogOffset(after.after_offset(self.replica_epoch))).await
    }
    pub(crate) async fn recovery_tail(&self, after: ReplicaPosition) -> rtdb_storage::Result<Option<PartitionReplay>> {
        let offset = after.after_offset(self.replica_epoch);
        let tail = if let Some(shared) = self.replica.shared() {
            Some(PartitionReplay::Prepared(shared.read_after(LogOffset(offset)).await?))
        } else {
            self.replica.recovery_tail(LogOffset(offset)).await?.map(PartitionReplay::Legacy)
        };
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
    pub(crate) async fn protected_snapshot_range(
        &self, after: ReplicaPosition, through: ReplicaPosition, root: [u8; 32],
    ) -> Result<rtdb_storage::log::ProtectedLogSnapshot> {
        if after > through { return Err(Anomaly::fault("rtdb.replication/snapshot-range", "The snapshot suffix ends before its checkpoint")); }
        let inclusive = after.0.checked_add(1).ok_or_else(size_overflow)?;
        let exclusive = through.0.checked_add(1).ok_or_else(size_overflow)?;
        let base = ReplicaPosition::from_epoch_offset(self.replica_epoch, 0)?.0;
        let relative = |position: i64| position.checked_sub(base)
            .filter(|offset| (0..=(1i64 << 48)).contains(offset)).map(LogOffset).ok_or_else(size_overflow);
        self.replica.protect_snapshot(relative(inclusive)?, relative(exclusive)?, root).await.map_err(Into::into)
    }

    pub fn validate_source_boundary(&self, database: &str, after: SourcePosition) -> Result<()> {
        let offset = after.after_offset(self.source_epoch);
        let latest = latest(self.source.as_ref())?.0;
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
        let receipt = match self.source.shared() {
            Some(shared) => shared.append(payload, now).await?,
            None => self.source.enqueue(payload, now).await?,
        };
        let metadata = receipt.durable().await?;
        self.source_position(metadata)
    }
    pub async fn enqueue_replica(&self, envelope: &ReplicaEnvelope, block: BlockIndex, now: Timestamp) -> Result<ReplicaReceipt> {
        if matches!(&envelope.message, ReplicaBody::ResolvedTx(tx) if tx.acceptance_stage.is_some()) {
            return Err(Anomaly::fault("rtdb.replication/strict-dispatch-clock", "Strict replica writes require the live writer clock"));
        }
        self.enqueue_replica_with(envelope.term, envelope.term_sequence, &envelope.message, block, || now, true, |_| Ok(())).await
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

    fn encode_replica(&self, term: Term, sequence: Option<TermSequence>, body: &ReplicaBody) -> Result<AppendBuffer> {
        let length = ReplicaEnvelope::encoded_body_len(term, sequence, body);
        let (prefix, suffix) = self.replica.append_headroom();
        let capacity = length.checked_add(prefix).and_then(|bytes| bytes.checked_add(suffix)).ok_or_else(size_overflow)?;
        let mut charge = self.memory.reserve(encoded_allocation(length)?)?;
        let mut output = BytesMut::with_capacity(capacity);
        output.resize(prefix, 0);
        ReplicaEnvelope::encode_body(term, sequence, body, &mut output);
        let retained = output.capacity().max(length.checked_add(256).ok_or_else(size_overflow)?);
        Arc::get_mut(&mut charge).expect("payload encoding owns its reservation").shrink_to(retained)?;
        Ok(AppendBuffer::new(output, prefix, charge))
    }

    pub(crate) async fn enqueue_replica_with(&self, term: Term, sequence: Option<TermSequence>, body: &ReplicaBody, block: BlockIndex, clock: impl Fn() -> Timestamp + Send + Sync,
        allow_offload: bool, before_enqueue: impl FnOnce(&[u8]) -> Result<()>) -> Result<ReplicaReceipt> {
        let stage = match body {
            ReplicaBody::ResolvedTx(tx) => tx.checked_acceptance_stage()?,
            _ => None,
        };
        let mut payload = self.encode_replica(term, sequence, body)?;
        before_enqueue(payload.payload())?;
        if let Some(stage) = stage {
            let guard = GuardedAppend::new(&clock, stage.clock_upper, stage.cutoff, stage.journal);
            let receipt = self.replica.enqueue_buffer(&mut payload, AppendMode::Guarded(guard)).await.map_err(|error| match error {
                error @ StorageError::GuardedAppendCutoff => Anomaly::new(AnomalyCategory::Unavailable, STRICT_DISPATCH_CUTOFF, error.to_string()),
                error @ StorageError::GuardedAppendUnavailable => Anomaly::new(AnomalyCategory::Unavailable, STRICT_DISPATCH_UNAVAILABLE, error.to_string()),
                error => error.into(),
            })?;
            return Ok(ReplicaReceipt { receipt, epoch: self.replica_epoch });
        }
        let now = clock();
        // An offload reference is charged for the body it replaces.
        let fenced = matches!(body, ReplicaBody::NoOp(_)) && self.fences_writer_terms();
        let fenced_terminal = match body {
            ReplicaBody::ResolvedTx(tx) => tx.placed_slot_terminal.as_deref()
                .map(rtdb_types::PlacedTerminal::decode).transpose()?.is_some_and(|terminal| terminal.fenced),
            _ => false,
        };
        let maintenance = is_maintenance(
            body, sequence.map(|sequence| sequence.0), fenced, fenced_terminal,
        );
        let receipt = match self.append_replica_buffer(&mut payload, now, maintenance).await {
            Ok(receipt) => receipt,
            Err(StorageError::MessageTooLarge { .. }) if allow_offload => {
                let payload_id = uuid::Uuid::new_v4().to_string();
                let key = ObjectKey::new(oversized_key(block, &payload_id)?)?;
                let mut proxy = ReplicaEnvelope::offloaded_body(term, sequence, self.storage_version, self.storage_epoch, block, payload_id, payload.payload());
                if let Some(version) = self.replica.publish_replica_payload(self.store.as_ref(), &key, payload.into_bytes()).await? {
                    proxy.bind_offload_publication(version)?;
                }
                let mut payload = self.encode_replica(proxy.term, proxy.term_sequence, &proxy.message)?;
                self.append_replica_buffer(&mut payload, now, maintenance).await?
            }
            Err(error) => return Err(error.into()),
        };
        Ok(ReplicaReceipt { receipt, epoch: self.replica_epoch })
    }
    async fn append_replica_buffer(&self, payload: &mut AppendBuffer, now: Timestamp, maintenance: bool) -> rtdb_storage::Result<AppendReceipt> {
        if let Some(shared) = self.replica.shared() {
            return payload.append(shared, now).await;
        }
        let mode = if maintenance { AppendMode::Maintenance(now) } else { AppendMode::Ordinary(now) };
        self.replica.enqueue_buffer(payload, mode).await
    }
    pub fn decode_replica(&self, record: LogRecord) -> Result<Option<ReplicaRecord>> {
        Ok(self.decode_readback(record, None, None, None)?.known())
    }
    pub(crate) fn decode_detached_replica(&self, record: LogRecord) -> Result<Option<ReplicaRecord>> {
        let reservation = self.memory.reserve(replica_decode_bound(record.payload.len())?)?;
        Ok(self.decode_reserved_replica(record, reservation)?.map(|mut record| {
            charge_detached_tables(Arc::make_mut(&mut record.envelope), &record.reservation);
            record
        }))
    }
    pub(crate) fn decode_reserved_replica(&self, record: LogRecord, reservation: Arc<MemoryReservation>) -> Result<Option<ReplicaRecord>> {
        Ok(self.decode_readback(record, Some(reservation), None, None)?.known())
    }
    pub(crate) fn decode_readback(&self, record: LogRecord, reservation: Option<Arc<MemoryReservation>>, own_term: Option<Term>,
        pending_fingerprint: Option<&dyn Fn(Option<TermSequence>) -> Option<ReplicaFingerprint>>) -> Result<DecodedReplica> {
        let separately_charged_delivery = reservation.is_some();
        let reservation = match reservation {
            Some(reservation) => reservation,
            None => self.memory.reserve(replica_decode_bound(record.payload.len())?)?,
        };
        let metadata = ReplicaMetadata { position: ReplicaPosition::from_epoch_offset(self.replica_epoch, record.metadata.offset.0)?, timestamp: record.metadata.timestamp };
        let mut observed = None;
        if let Some(pending_fingerprint) = pending_fingerprint
            && let Some(peek) = ReplicaPeek::read(&record.payload)
            && Some(peek.term) == own_term
            && let ReplicaPeekBody::Resolved { tx_id, search_delta, .. } = peek.body
            && let Some(expected) = pending_fingerprint(peek.term_sequence)
        {
            let digest = fingerprint(&record.payload);
            if expected == digest {
                let envelope = ReplicaEnvelope { term: peek.term, term_sequence: peek.term_sequence,
                    message: ReplicaBody::ResolvedTx(proto::ResolvedTx {
                        tx_id, search_delta: search_delta.map(|delta| record.payload.slice_ref(delta)), ..Default::default()
                    }) };
                return Ok(DecodedReplica::Known(ReplicaRecord {
                    metadata, envelope: Arc::new(envelope), reservation, fingerprint: Some(digest), oversized_delivery: None,
                }));
            }
            observed = Some(digest);
        }
        // A prepared replay delivery releases its separate read charge after decoding.
        let frame = if separately_charged_delivery { ReplicaFrame::decode(&record.payload)? }
            else { ReplicaFrame::decode_bytes(record.payload.clone())? };
        let (term, sequence) = (frame.term, frame.term_sequence);
        let Some(envelope) = frame.into_envelope() else { return Ok(DecodedReplica::Unrecognized { term, sequence }); };
        let fingerprint = (Some(term) == own_term && matches!(&envelope.message, ReplicaBody::ResolvedTx(_) | ReplicaBody::OversizedMessage(_)))
            .then(|| observed.unwrap_or_else(|| fingerprint(&record.payload)));
        let oversized = matches!(&envelope.message, ReplicaBody::OversizedMessage(_));
        if oversized && self.replica.requires_replica_payload_permit() && record.replica_objects.is_none() {
            return Err(Anomaly::fault("rtdb.replication/oversized-delivery",
                "The hosted replica returned a pointer without its payload permit"));
        }
        let oversized_delivery = oversized.then_some(record);
        Ok(DecodedReplica::Known(ReplicaRecord { metadata, envelope: Arc::new(envelope), reservation, fingerprint, oversized_delivery }))
    }
    /// Admission runs on the outer envelope before this fetch; a held block receives the resolved body.
    pub async fn resolve_oversized(&self, record: ReplicaRecord) -> Result<ReplicaRecord> {
        self.resolve_oversized_for_readback(record, None).await
    }

    pub(crate) async fn resolve_oversized_for_readback(&self, mut record: ReplicaRecord, expected: Option<ReplicaFingerprint>) -> Result<ReplicaRecord> {
        let decode = OversizedDecode::Retained(expected);
        if record.oversized_delivery.as_ref().is_some_and(|delivery| delivery.replica_objects.is_some()) {
            return self.resolve_oversized_fresh_with(record, decode).await;
        }
        let Some(key) = record.envelope.oversized_payload(self.storage_version, self.storage_epoch)? else { return Ok(record); };
        let delivery = record.take_oversized_delivery()?;
        let key = ObjectKey::new(key)?;
        let lease = IoRegion::retaining(Arc::new(delivery), self.cache.get(&key)).await?;
        self.decode_oversized(record, lease, decode)
    }

    pub(crate) async fn resolve_oversized_fresh(&self, record: ReplicaRecord) -> Result<ReplicaRecord> {
        self.resolve_oversized_fresh_with(record, OversizedDecode::Retained(None)).await
    }

    pub(crate) async fn resolve_oversized_fresh_detached(&self, record: ReplicaRecord) -> Result<ReplicaRecord> {
        self.resolve_oversized_fresh_with(record, OversizedDecode::Detached).await
    }

    async fn resolve_oversized_fresh_with(&self, mut record: ReplicaRecord, decode: OversizedDecode) -> Result<ReplicaRecord> {
        let Some(key) = record.envelope.oversized_payload(self.storage_version, self.storage_epoch)? else { return Ok(record); };
        let delivery = record.take_oversized_delivery()?;
        let key = ObjectKey::new(key)?;
        if let Some(custody) = &delivery.replica_objects {
            if !custody.matches(delivery.metadata.offset, &key) {
                return Err(Anomaly::fault("rtdb.replication/oversized-delivery",
                    "The replica payload permit does not match this delivery"));
            }
            let payload = custody.redeem(self.store.as_ref(), &self.memory).await?;
            return self.decode_oversized(record, payload, decode);
        }
        IoRegion::retaining(Arc::new(delivery), async {
            let store = &self.store;
            let length = usize::try_from(store.head_fresh(&key).await?.size).map_err(|_| size_overflow())?;
            let allocation = store.exact_object_allocation(&key, length)?;
            let reservation = self.memory.reserve(allocation.retained_bytes)?;
            let scratch = (allocation.scratch_bytes > 0).then(|| self.memory.reserve(allocation.scratch_bytes)).transpose()?;
            let read = ReadReservation::funded(length, Arc::clone(&reservation), scratch);
            let bytes = read.retaining_io(store.get_fresh_exact(&key, length)).await?;
            drop(read);
            self.decode_oversized(record, ChargedPayload { bytes, _reservation: reservation }, decode)
        }).await
    }

    fn decode_oversized(
        &self, record: ReplicaRecord, payload: impl AsRef<[u8]> + Send + 'static, decode: OversizedDecode,
    ) -> Result<ReplicaRecord> {
        let reservation = self.memory.reserve(replica_decode_bound(payload.as_ref().len())?)?;
        let digest = (record.fingerprint.is_some() || matches!(decode, OversizedDecode::Retained(Some(_))))
            .then(|| fingerprint(payload.as_ref()));
        let envelope = match decode {
            OversizedDecode::Detached => {
                let mut envelope = record.envelope.unwrap_payload(payload.as_ref())?;
                charge_detached_tables(&mut envelope, &reservation);
                envelope
            }
            OversizedDecode::Retained(expected) => {
                let payload = Bytes::from_owner(payload);
                if let (Some(digest), ReplicaBody::OversizedMessage(reference)) = (digest, &record.envelope.message)
                    && Some(digest) == expected && reference.payload_sha256.as_deref() == Some(digest.as_slice())
                    && let Some(peek) = ReplicaPeek::read(&payload)
                    && (peek.term, peek.term_sequence) == (record.envelope.term, record.envelope.term_sequence)
                    && let ReplicaPeekBody::Resolved { tx_id, search_delta, .. } = peek.body
                {
                    ReplicaEnvelope { term: peek.term, term_sequence: peek.term_sequence, message: ReplicaBody::ResolvedTx(proto::ResolvedTx {
                        tx_id, search_delta: search_delta.map(|delta| payload.slice_ref(delta)), ..Default::default()
                    }) }
                } else { record.envelope.unwrap_payload_bytes(payload)? }
            }
        };
        let fingerprint = record.fingerprint.and(digest);
        Ok(ReplicaRecord { metadata: record.metadata, envelope: Arc::new(envelope), reservation, fingerprint, oversized_delivery: None })
    }
}

fn charge_detached_tables(envelope: &mut ReplicaEnvelope, reservation: &Arc<MemoryReservation>) {
    // Arrow can retain the copied table bytes after the decoded record is dropped.
    if let ReplicaBody::ResolvedTx(tx) = &mut envelope.message {
        for bytes in tx.table_data.values_mut().filter(|bytes| !bytes.is_empty()) {
            *bytes = Bytes::from_owner(ChargedPayload {
                bytes: std::mem::take(bytes),
                _reservation: Arc::clone(reservation),
            });
        }
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
