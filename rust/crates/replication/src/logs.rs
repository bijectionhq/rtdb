use std::sync::Arc;

use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{envelope::{ReplicaBody, ReplicaEnvelope, ReplicaFrame, SourceMessage}, keys::oversized_key, log::proto};
use rtdb_storage::{Error as StorageError, ObjectKey, ObjectStore, cache::BufferPool, log::{AppendReceipt, Log, LogMetadata, LogOffset, LogRecord, LogTail}};
use rtdb_types::{Anomaly, BlockIndex, ReplicaPosition, Result, SourcePosition, StorageEpoch, StorageVersion, Term, TermSequence, Timestamp};

use crate::readback_identity::{ReplicaFingerprint, fingerprint};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaMetadata { pub position: ReplicaPosition, pub timestamp: Timestamp }

#[derive(Clone, Debug)]
pub struct ReplicaRecord {
    pub metadata: ReplicaMetadata,
    pub envelope: Arc<ReplicaEnvelope>,
    reservation: Arc<MemoryReservation>,
    pub(crate) fingerprint: Option<ReplicaFingerprint>,
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
}
impl PartitionLogs {
    pub fn new(source: Arc<dyn Log>, replica: Arc<dyn Log>, source_epoch: StorageEpoch, replica_epoch: StorageEpoch,
        storage_version: StorageVersion, storage_epoch: StorageEpoch, store: Arc<dyn ObjectStore>, cache: Arc<BufferPool>, memory: Arc<MemoryBudget>) -> Result<Arc<Self>> {
        if Arc::ptr_eq(&source, &replica) { return Err(Anomaly::incorrect("rtdb.replication/aliased-logs", "Source and replica logs must be distinct")); }
        SourcePosition::from_epoch_offset(source_epoch, -1)?;
        ReplicaPosition::from_epoch_offset(replica_epoch, -1)?;
        Ok(Arc::new(Self { source, replica, source_epoch, replica_epoch, storage_version, storage_epoch, store, cache, memory }))
    }
    pub fn source_floor(&self) -> Result<SourcePosition> { SourcePosition::from_epoch_offset(self.source_epoch, -1) }
    pub fn replica_floor(&self) -> Result<ReplicaPosition> { ReplicaPosition::from_epoch_offset(self.replica_epoch, -1) }
    pub(crate) fn memory(&self) -> &Arc<MemoryBudget> { &self.memory }
    pub(crate) fn local_recovery_store(&self) -> Option<&rtdb_storage::LocalStore> {
        if self.retains_recovery_history() { self.store.local_maintenance_store() } else { None }
    }
    pub(crate) fn recovery_block_store(&self, file_size_target: i64) -> Result<Arc<crate::BlockStore>> {
        crate::BlockStore::new(Arc::clone(&self.store), Arc::clone(&self.cache), Arc::clone(&self.memory), file_size_target)
    }
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
        self.enqueue_replica_with(envelope, block, now, true, |_| Ok(())).await
    }
    pub(crate) fn replica_inline_limit(&self) -> Option<usize> { self.replica.maximum_append_bytes() }
    pub(crate) async fn enqueue_replica_with(&self, envelope: &ReplicaEnvelope, block: BlockIndex, now: Timestamp,
        allow_offload: bool, before_enqueue: impl FnOnce(&[u8]) -> Result<()>) -> Result<ReplicaReceipt> {
        let payload = EncodedPayload::encode(envelope.encoded_len(), &self.memory, || Ok(envelope.encode()))?;
        before_enqueue(&payload)?;
        let receipt = match self.replica.enqueue(payload.clone(), now).await {
            Ok(receipt) => receipt,
            Err(StorageError::MessageTooLarge { .. }) if allow_offload => {
                let payload_id = uuid::Uuid::new_v4().to_string();
                self.store.put(&ObjectKey::new(oversized_key(block, &payload_id)?)?, payload).await?;
                let proxy = ReplicaEnvelope { term: envelope.term, term_sequence: envelope.term_sequence,
                    message: ReplicaBody::OversizedMessage(proto::OversizedMessage { storage_version: Some(self.storage_version.0), storage_epoch: Some(self.storage_epoch.0), block_index: Some(block.0), payload_id: Some(payload_id) }) };
                let payload = EncodedPayload::encode(proxy.encoded_len(), &self.memory, || Ok(proxy.encode()))?;
                self.replica.enqueue(payload, now).await?
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
        Ok(DecodedReplica::Known(ReplicaRecord { metadata, envelope: Arc::new(envelope), reservation, fingerprint }))
    }
    /// Admission runs on the outer envelope before this fetch; a held block receives the resolved body.
    pub async fn resolve_oversized(&self, record: ReplicaRecord) -> Result<ReplicaRecord> {
        let Some(key) = record.envelope.oversized_payload(self.storage_version, self.storage_epoch)? else { return Ok(record); };
        let lease = self.cache.get(&ObjectKey::new(key)?).await?;
        self.decode_oversized(record, lease.as_slice())
    }

    pub(crate) async fn resolve_oversized_fresh(&self, record: ReplicaRecord) -> Result<ReplicaRecord> {
        let Some(key) = record.envelope.oversized_payload(self.storage_version, self.storage_epoch)? else { return Ok(record); };
        let key = ObjectKey::new(key)?;
        let length = usize::try_from(self.store.head_fresh(&key).await?.size).map_err(|_| size_overflow())?;
        let _payload = self.memory.reserve(length)?;
        let payload = self.store.get_fresh_exact(&key, length).await?;
        self.decode_oversized(record, &payload)
    }

    fn decode_oversized(&self, record: ReplicaRecord, payload: &[u8]) -> Result<ReplicaRecord> {
        let reservation = self.memory.reserve(replica_decode_bound(payload.len())?)?;
        let envelope = record.envelope.unwrap_payload(payload)?;
        let fingerprint = record.fingerprint.map(|_| fingerprint(payload));
        Ok(ReplicaRecord { metadata: record.metadata, envelope: Arc::new(envelope), reservation, fingerprint })
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
