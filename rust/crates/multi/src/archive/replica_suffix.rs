//! Copy an admitted replica suffix into the checkpoint capture's key custody.

use base64::Engine as _;
use bytes::Bytes;
use prost::Message;
use rtdb_directory::{Checkpoint, CheckpointSuffixCustody, CheckpointSuffixPin, CheckpointTransferFence, CheckpointTransferTerminal, DeploymentRecord, Directory, HostedTask, Lifecycle, now_ms};
use rtdb_format::{
    catalog::BlockCatalog,
    envelope::{ReplicaBody, ReplicaEnvelope, ReplicaFrame},
    keys::{CURRENT_STORAGE_VERSION, block_key},
    log::proto::NoOp,
};
use rtdb_logkeeper::{ArchivedReplicaEntry, JournalDigest};
use rtdb_types::{Anomaly, BlockIndex, MemoryBudget, Result, SourcePosition, StorageEpoch, Timestamp, TransactionKey, TxId};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

use super::{
    CapturePoint, URL_SAFE_NO_PAD, capture_receipt, checkpoint_storage_root, encode, inconsistent, read_object, require_capture_open,
    unavailable, verified_archive, verify_cut,
};
use super::replica_artifact::{CertifiedSegments, PayloadBytes, ReplicaArtifact, require_chain};
use super::stage::{RegionPublication, with_region};
use crate::{
    admin::{administration_stream, update_installation},
    cipher::Cipher,
    deployment::CellStorage,
    keys::KeyService,
    reference::{PrefixedStore, key},
    store::{EncryptingStore, ROOT_MARKER},
};
use rtdb_storage::{IoRegion, ObjectStore};

pub(crate) const MAX_SEGMENT_BYTES: usize = 8 << 20;
pub(crate) const MAX_SEGMENT_ENTRIES: usize = 4096;
const FORMAT: &[u8] = b"rtdb.replica-suffix.v1\0";

pub(super) struct ServingPayloads<'a> {
    record: &'a DeploymentRecord,
    storage: &'a CellStorage,
    keys: &'a dyn KeyService,
    io: Arc<IoRegion>,
    memory: &'a Arc<MemoryBudget>,
    store: tokio::sync::OnceCell<EncryptingStore>,
}

impl<'a> ServingPayloads<'a> {
    pub(super) fn new(
        record: &'a DeploymentRecord, storage: &'a CellStorage, keys: &'a dyn KeyService,
        io: Arc<IoRegion>, memory: &'a Arc<MemoryBudget>,
    ) -> Self {
        Self { record, storage, keys, io, memory, store: tokio::sync::OnceCell::new() }
    }

    pub(super) async fn read(&self, envelope: &ReplicaEnvelope) -> Result<PayloadBytes> {
        let name = envelope.oversized_payload(CURRENT_STORAGE_VERSION, StorageEpoch(0))?
            .ok_or_else(suffix_unavailable)?;
        let store = self.store.get_or_try_init(|| async {
            let material = self.keys.existing_key(&self.record.key_id()).await?
                .ok_or_else(suffix_unavailable)?;
            let bucket = self.storage.bucket(Arc::clone(&self.io)).await?;
            if self.record.storage_namespaces.is_some() {
                let objects = crate::deployment::existing_storage_namespace(bucket.as_ref()).await?
                    .ok_or_else(suffix_unavailable)?;
                crate::deployment::require_storage_namespaces(self.record, rtdb_directory::StorageNamespaces {
                    objects, keys: self.keys.namespace_id().await?,
                })?;
            }
            let root = format!("{}/{}", self.record.storage_prefix(), checkpoint_storage_root()?);
            let files = PrefixedStore::new(bucket, root);
            let owner = files.observe_fresh_exact(&key(ROOT_MARKER)?, self.record.id.len()).await?;
            if owner.value.as_ref() != self.record.id.as_bytes() {
                return Err(suffix_unavailable());
            }
            EncryptingStore::new(Arc::new(files), &material)
        }).await?;
        PayloadBytes::read(store, &key(&name)?, self.memory).await
    }

    pub(super) async fn resolve_verified(&self, envelope: &ReplicaEnvelope) -> Result<(ReplicaEnvelope, PayloadBytes)> {
        let ReplicaBody::OversizedMessage(reference) = &envelope.message else { return Err(suffix_unavailable()); };
        if self.record.storage_namespaces.is_none() || reference.payload_sha256.as_ref().is_none_or(|digest| digest.len() != 32) {
            return Err(suffix_unavailable());
        }
        let body = self.read(envelope).await?;
        let resolved = envelope.unwrap_payload(&body.bytes)?;
        Ok((resolved, body))
    }
}

pub(super) async fn read_base_catalog(
    bucket: Arc<dyn ObjectStore>, point: &CapturePoint, manifest: &str, keys: &dyn KeyService,
) -> Result<(BlockCatalog, [u8; crate::keys::KEY_BYTES])> {
    let (base, material) = verified_archive(bucket.clone(), point, manifest, keys).await?;
    let root = key(&block_key(BlockIndex(point.checkpoint.block))?)?;
    let root_object = base.objects.iter().find(|object| object.name == root.as_str())
        .ok_or_else(|| inconsistent("The suffix capture omitted its root catalog"))?;
    let files = PrefixedStore::new(bucket, format!("checkpoint-captures/{}/{}/objects", point.deployment, point.operation));
    let root_bytes = read_object(&files, &root, root_object).await?;
    let catalog = BlockCatalog::decode(&Cipher::new(&material)?.open(root.as_str(), &root_bytes)?)?;
    verify_cut(&catalog, point.checkpoint, StorageEpoch(point.log_epoch))?;
    Ok((catalog, material))
}

/// Resolve the original accepted publication to its complete transaction and
/// journal cut inside a previously quorum-certified applied interval.
pub(crate) async fn find_held_publication(
    stream: &rtdb_logkeeper::StreamConfig, proof: &JournalDigest,
    record: &DeploymentRecord, storage: &CellStorage, keys: &dyn KeyService, cut: &rtdb_node::OwnedForkCut,
    base: &BlockCatalog, io: Arc<IoRegion>, memory: &Arc<MemoryBudget>,
) -> Result<(TransactionKey, i64, i64)> {
    let cipher = TerminalReplicaCipher::open(record, keys).await?;
    let payloads = ServingPayloads::new(record, storage, keys, io, memory);
    let mut pages = rtdb_logkeeper::CertifiedRangeReader::new(stream, proof)?;
    let log_epoch = StorageEpoch(record.log_epoch);
    let mut fence = rtdb_replication::TermFence::from_catalog(base)?;
    let mut writer = None;
    let mut selected = None;
    while let Some(mut entries) = pages.next_page().await? {
        if selected.is_some() {
            continue;
        }
        cipher.open_page(&mut entries)?;
        for entry in entries {
            let Some(envelope) = admit_record(&entry, &mut fence, &mut writer, record.epoch)? else { continue; };
            let _payload;
            let envelope = if matches!(&envelope.message, ReplicaBody::OversizedMessage(_)) {
                let (resolved, body) = payloads.resolve_verified(&envelope).await?;
                _payload = body;
                resolved
            } else {
                envelope
            };
            let ReplicaBody::ResolvedTx(row) = envelope.message else { continue; };
            if row.system_time_micros != Some(cut.basis().0) { continue; }
            let basis = TransactionKey {
                tx_id: TxId(row.tx_id.ok_or_else(suffix_unavailable)?),
                system_time: cut.basis(),
            };
            let source = SourcePosition(row.src_msg_id.ok_or_else(suffix_unavailable)?).after_offset(log_epoch);
            if !row.committed.unwrap_or(false)
                || !cut.matches_basis(basis) || row.db_op.is_some() || row.external_source_token.is_some()
                || source < 0
            {
                return Err(suffix_unavailable());
            }
            selected = Some((basis, entry.lsn, source));
            break;
        }
    }
    selected.ok_or_else(suffix_unavailable)
}

pub(crate) async fn capture(
    directory: Arc<dyn Directory>, storage: &CellStorage, archive: &CellStorage, keys: &dyn KeyService,
    hosted_task: Option<&HostedTask>, deployment: Uuid, pin_id: Uuid,
    selected: CheckpointSuffixPin, memory: &Arc<MemoryBudget>,
) -> Result<String> {
    let id = deployment.to_string();
    with_region(
        directory.as_ref(), keys, &id, archive, hosted_task, Some((selected.capture, Some(pin_id))),
        Some(RegionPublication::Capture(Arc::clone(&directory))),
        |record| {
            admit_copy(record, archive, deployment, pin_id, &selected)
        },
        |record, region, io| {
            let (directory, selected, id) = (&directory, &selected, &id);
            async move {
                let (point, pin) = admit(&record, archive, deployment, pin_id, selected)?;
                let bucket = archive.bucket(Arc::clone(&io)).await?;
                let (catalog, material) = read_base_catalog(Arc::clone(&bucket), &point, &pin.manifest_sha256, keys).await?;
                let stream_name = record.stream("replica");
                let artifact = suffix_artifact(bucket.as_ref(), &material, deployment, pin_id, pin, &stream_name)?;

                let digest = if let Some(expected) = &pin.archive_sha256 {
                    artifact.read_manifest(archive_digest(expected)?).await?;
                    expected.clone()
                } else {
                    let stream = administration_stream(directory.as_ref(), &record, 4 << 20).await?;
                    let certificate = JournalDigest::from_digest_bytes(
                        stream_name, pin.journal_interval, pin.journal_sha256,
                    );
                    let remaining = pin.expires_at_ms.saturating_sub(now_ms()).max(0) as u64;
                    let materialize = async {
                        let _pages = memory.reserve(32 << 20)?;
                        let cipher = TerminalReplicaCipher::open(&record, keys).await?;
                        let payloads = ServingPayloads::new(&record, storage, keys, io, memory);
                        let mut segments = CertifiedSegments::new(rtdb_logkeeper::CertifiedRangeReader::new(&stream, &certificate)?);
                        let mut progress = SuffixProgress::new(pin, &catalog)?;
                        let mut chain = artifact.chain();
                        while let Some(mut entries) = segments.next().await? {
                            let first = entries.first().ok_or_else(suffix_unavailable)?.lsn;
                            let through = entries.last().ok_or_else(suffix_unavailable)?.lsn;
                            // Do not let provisional data occupy a segment key. The full
                            // interval proof must also match before sealing the manifest.
                            let segment = rtdb_logkeeper::certify_committed_range(&stream, first, through).await?;
                            rtdb_logkeeper::verify_certified_range(&segment, &entries)?;
                            cipher.open_page(&mut entries)?;
                            artifact.put_segment(&entries, &mut chain).await?;
                            for entry in entries {
                                let Some(envelope) = progress.admit(&entry, pin)? else { continue; };
                                if matches!(&envelope.message, ReplicaBody::OversizedMessage(_)) {
                                    let (resolved, payload) = payloads.resolve_verified(&envelope).await?;
                                    progress.apply(resolved, pin, |_| Ok(()))?;
                                    artifact.put_payload(entry.lsn, &payload.bytes, &mut chain).await?;
                                } else {
                                    progress.apply(envelope, pin, |_| Ok(()))?;
                                }
                            }
                        }
                        progress.finish(pin)?;
                        artifact.put_manifest(chain).await
                    };
                    let digest = tokio::time::timeout(Duration::from_millis(remaining), materialize)
                        .await.map_err(|_| suffix_unavailable())??;
                    URL_SAFE_NO_PAD.encode(digest)
                };
                update_installation(directory.as_ref(), id, |current| {
                    let (_, prior) = admit(current, archive, deployment, pin_id, selected)?;
                    if !current.storage_regions.contains_key(&region)
                        || prior.archive_sha256.as_ref().is_some_and(|existing| existing != &digest)
                    {
                        return Err(suffix_unavailable());
                    }
                    let mut published = prior.clone();
                    published.archive_sha256 = Some(digest.clone());
                    current.checkpoint_suffix_pins.insert(pin_id, published);
                    Ok(())
                }).await?;
                Ok(digest)
            }
        },
    ).await
}

/// Resolve an immutable published sidecar using only the capture's own authority.
pub(super) fn accepted<'a>(
    record: &'a DeploymentRecord, point: &CapturePoint, manifest: &str, suffix: Uuid,
) -> Result<&'a CheckpointSuffixPin> {
    let pin = record.checkpoint_suffix_pins.get(&suffix).ok_or_else(suffix_unavailable)?;
    if pin.capture != point.operation || pin.incarnation != point.incarnation
        || pin.incarnation != record.incarnation || pin.log_epoch != point.log_epoch
        || pin.log_epoch != record.log_epoch || pin.checkpoint != point.checkpoint
        || pin.manifest_sha256 != manifest || pin.archive_sha256.is_none()
        || pin.checkpoint.boundary.checked_add(1) != Some(pin.journal_interval[0])
        || pin.journal_interval[1] != pin.replica_through || pin.source_through <= point.checkpoint.source
        || pin.basis.system_time.0 <= point.checkpoint.system_time
        || !admits_custody(record, suffix, pin)
    {
        return Err(suffix_unavailable());
    }
    Ok(pin)
}

fn admits_custody(record: &DeploymentRecord, suffix: Uuid, pin: &CheckpointSuffixPin) -> bool {
    let now = now_ms();
    match pin.custody {
        CheckpointSuffixCustody::Capture => true,
        CheckpointSuffixCustody::World =>
            pin.expires_at_ms > now || record.retains_world_suffix(suffix, pin, now),
        CheckpointSuffixCustody::RetiringWorld => false,
    }
}

pub(super) async fn apply_archived(
    bucket: &dyn ObjectStore, deployment: Uuid, suffix: Uuid, stream: String,
    pin: &CheckpointSuffixPin, material: &[u8; crate::keys::KEY_BYTES],
    recovered: &mut rtdb_replication::RecoveredPartition, memory: &Arc<rtdb_columnar::MemoryBudget>,
) -> Result<()> {
    let expected = pin.archive_sha256.as_deref().ok_or_else(suffix_unavailable)?;
    let artifact = suffix_artifact(bucket, material, deployment, suffix, pin, &stream)?;
    let expected = artifact.read_manifest(archive_digest(expected)?).await?;
    let catalog = recovered.block.as_ref().ok_or_else(suffix_unavailable)?;
    let mut progress = SuffixProgress::new(pin, catalog)?;
    let mut chain = artifact.chain();
    while progress.next <= pin.replica_through {
        let _charge = memory.reserve(MAX_SEGMENT_BYTES * 4 + 4096 + MAX_SEGMENT_ENTRIES * 128)?;
        let entries = artifact.read_segment(progress.next, pin.replica_through, &mut chain).await?;
        for entry in entries {
            let Some(envelope) = progress.admit(&entry, pin)? else { continue; };
            let payload;
            let envelope = if matches!(&envelope.message, ReplicaBody::OversizedMessage(_)) {
                payload = artifact.read_payload(entry.lsn, &mut chain, memory).await?;
                envelope.unwrap_payload(&payload.bytes)?
            } else {
                envelope
            };
            progress.apply(envelope, pin, |message| recovered.apply_archived_transaction(message, memory))?;
        }
    }
    progress.finish(pin)?;
    require_chain(chain, expected)?;
    let source = SourcePosition::from_epoch_offset(StorageEpoch(pin.log_epoch), pin.source_through)?;
    if recovered.progress.tx_key != Some(pin.basis)
        || recovered.progress.source_position > source
        || recovered.partition.snapshot().tx_basis() != Some(pin.basis)
    {
        return Err(suffix_unavailable());
    }
    // The certified cut can include source maintenance after its last transaction.
    recovered.progress.source_position = source;
    Ok(())
}

fn suffix_artifact<'a>(
    bucket: &'a dyn ObjectStore, material: &[u8], deployment: Uuid, suffix: Uuid,
    pin: &CheckpointSuffixPin, stream: &str,
) -> Result<ReplicaArtifact<'a>> {
    let mut identity = pin.clone();
    // A retry can renew the live pin. The immutable archive proves the
    // journal cut itself independently of that authorization deadline.
    identity.expires_at_ms = 0;
    identity.writer_epoch = 0;
    identity.archive_sha256 = None;
    ReplicaArtifact::new(
        bucket, material, format!("{}/v2", archive_prefix(deployment, suffix, pin)),
        encode(&(stream, identity))?, b"RTDB checkpoint suffix segments v2\0",
    )
}

pub(super) fn archive_prefix(deployment: Uuid, suffix: Uuid, pin: &CheckpointSuffixPin) -> String {
    let scope = match pin.custody {
        CheckpointSuffixCustody::Capture => "replica-suffix",
        CheckpointSuffixCustody::World | CheckpointSuffixCustody::RetiringWorld => "world-replica-suffix",
    };
    format!("checkpoint-captures/{deployment}/{}/{scope}/{suffix}", pin.capture)
}

fn archive_digest(expected: &str) -> Result<[u8; 32]> {
    let decoded = URL_SAFE_NO_PAD.decode(expected).map_err(|_| suffix_unavailable())?;
    decoded.try_into().map_err(|_| suffix_unavailable())
}

pub(super) struct TerminalReplicaCipher {
    cipher: Cipher,
    scope: String,
}

impl TerminalReplicaCipher {
    pub(super) async fn open(source: &DeploymentRecord, keys: &dyn KeyService) -> Result<Self> {
        let material = keys.existing_key(&source.key_id()).await?.ok_or_else(suffix_unavailable)?;
        Ok(Self { cipher: Cipher::new(&material)?, scope: source.replica_seal_scope() })
    }

    pub(super) fn open_page(&self, entries: &mut [ArchivedReplicaEntry]) -> Result<()> {
        for entry in entries {
            entry.payload = self.cipher.open(&self.scope, &entry.payload)?;
        }
        Ok(())
    }
}

/// Exact replay coordinates retained while the terminal interval is read in
/// bounded segments. The recovered partition owns the application term fence.
pub(super) struct TerminalReplayProgress {
    writer_epoch: Option<u64>,
    last_offset: i64,
    replay: LogicalReplay,
}

impl TerminalReplayProgress {
    pub(super) fn new(
        fence: &rtdb_directory::CheckpointTransferFence,
        recovered: &rtdb_replication::RecoveredPartition,
    ) -> Result<Self> {
        let checkpoint = fence.destination.checkpoint;
        let catalog = recovered.block.as_ref().ok_or_else(suffix_unavailable)?;
        Ok(Self {
            writer_epoch: None, last_offset: checkpoint.boundary,
            replay: LogicalReplay::new(checkpoint, fence.destination.source_log_epoch, catalog),
        })
    }

    pub(super) fn admit(
        &mut self, entry: &ArchivedReplicaEntry, fence: &rtdb_directory::CheckpointTransferFence,
        recovered: &mut rtdb_replication::RecoveredPartition,
    ) -> Result<Option<ReplicaEnvelope>> {
        if self.last_offset.checked_add(1) != Some(entry.lsn) {
            return Err(suffix_unavailable());
        }
        let envelope = admit_record(entry, &mut recovered.fence, &mut self.writer_epoch, fence.source_writer_epoch)?;
        self.last_offset = entry.lsn;
        Ok(envelope)
    }

    pub(super) fn apply(
        &mut self, envelope: ReplicaEnvelope,
        recovered: &mut rtdb_replication::RecoveredPartition, memory: &Arc<MemoryBudget>,
    ) -> Result<()> {
        match envelope.message {
            ReplicaBody::ResolvedTx(message) => {
                let next = self.replay.transaction_source(message.src_msg_id)?;
                recovered.apply_archived_transaction(&message, memory)?;
                self.replay.source = next;
            }
            control => {
                let prior = self.replay.source;
                self.replay.apply_control(ReplayControl::read(&control)?)?;
                if self.replay.source > prior {
                    recovered.progress.source_position =
                        SourcePosition::from_epoch_offset(self.replay.epoch, self.replay.source)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish(self, through: i64) -> Result<i64> {
        if self.last_offset != through { return Err(suffix_unavailable()); }
        Ok(self.replay.source)
    }
}

pub(super) async fn with_terminal_source<T, F, Fut>(
    directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, hosted_task: Option<&HostedTask>,
    fence: &CheckpointTransferFence, through: i64, expected: Option<&CheckpointTransferTerminal>, run: F,
) -> Result<T>
where
    F: FnOnce(DeploymentRecord, Arc<IoRegion>) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let stage = &fence.destination;
    let deployment = stage.intent.source.to_string();
    Box::pin(with_region(
        directory, keys, &deployment, storage, hosted_task, Some((stage.intent.capture, None)), None,
        |source| terminal_replay_floor(source, fence, through, expected),
        |source, _, io| run(source, io),
    )).await
}

fn terminal_replay_floor(
    source: &DeploymentRecord, fence: &CheckpointTransferFence, through: i64,
    expected: Option<&CheckpointTransferTerminal>,
) -> Result<Option<i64>> {
    let stage = &fence.destination;
    if source.id != stage.intent.source.to_string() || source.lifecycle != Lifecycle::Restoring
        || source.holder.is_some() || source.epoch != fence.fenced_writer_epoch
        || source.incarnation != stage.source_incarnation || source.log_epoch != stage.source_log_epoch
        || source.checkpoint != Some(stage.checkpoint) || source.checkpoint_transfer_fence.as_deref() != Some(fence)
        || expected.is_some_and(|expected| source.checkpoint_transfer_terminal.as_deref() != Some(expected)
            || expected.replica_through != through || &expected.fence != fence)
        || source.replica_anchor.as_ref().is_some_and(|anchor| anchor.root_incarnation != source.incarnation)
        || through < stage.checkpoint.boundary
    {
        return Err(suffix_unavailable());
    }
    require_capture_open(source, stage.intent.capture)?;
    if through == stage.checkpoint.boundary { return Ok(None); }
    Ok(Some(crate::hosted_recovery::replay_start(source, stage.checkpoint.boundary)?))
}

/// Reopen the original certified source interval without accumulating its raw bytes.
pub(super) async fn replay_certified_terminal(
    source: &DeploymentRecord, storage: &CellStorage, io: Arc<IoRegion>,
    keys: &dyn KeyService, stream: &rtdb_logkeeper::StreamConfig,
    certificate: Option<&JournalDigest>, fence: &rtdb_directory::CheckpointTransferFence, through: i64,
    recovered: &mut rtdb_replication::RecoveredPartition, memory: &Arc<rtdb_columnar::MemoryBudget>,
) -> Result<i64> {
    let mut progress = TerminalReplayProgress::new(fence, recovered)?;
    let first = fence.destination.checkpoint.boundary.checked_add(1).ok_or_else(suffix_unavailable)?;
    if stream.stream != source.stream("replica") {
        return Err(suffix_unavailable());
    }
    if let Some(certificate) = certificate {
        if certificate.interval != [first, through] { return Err(suffix_unavailable()); }
        let cipher = TerminalReplicaCipher::open(source, keys).await?;
        let payloads = ServingPayloads::new(source, storage, keys, io, memory);
        let mut pages = rtdb_logkeeper::CertifiedRangeReader::new(stream, certificate)?;
        while let Some(mut entries) = pages.next_page().await? {
            let bytes = entries.iter().try_fold(0usize, |total, entry|
                total.checked_add(entry.payload.len().saturating_mul(2).saturating_add(64))
                    .ok_or_else(suffix_unavailable))?;
            let _charge = memory.reserve(bytes)?;
            cipher.open_page(&mut entries)?;
            for entry in entries {
                let Some(envelope) = progress.admit(&entry, fence, recovered)? else { continue; };
                let _payload;
                let envelope = if matches!(&envelope.message, ReplicaBody::OversizedMessage(_)) {
                    let (resolved, body) = payloads.resolve_verified(&envelope).await?;
                    _payload = body;
                    resolved
                } else {
                    envelope
                };
                progress.apply(envelope, recovered, memory)?;
            }
        }
    }
    progress.finish(through)
}

fn admit_copy(
    record: &DeploymentRecord, archive: &CellStorage, deployment: Uuid,
    pin_id: Uuid, selected: &CheckpointSuffixPin,
) -> Result<Option<i64>> {
    let (_, pin) = admit(record, archive, deployment, pin_id, selected)?;
    if pin.archive_sha256.is_some() { return Ok(None); }
    if record.replica_anchor.as_ref().is_some_and(|anchor| anchor.root_incarnation != pin.incarnation) {
        return Err(suffix_unavailable());
    }
    Ok(Some(crate::hosted_recovery::replay_start(record, pin.checkpoint.boundary)?))
}

fn admit<'a>(
    record: &'a DeploymentRecord, archive: &CellStorage, deployment: Uuid,
    pin_id: Uuid, selected: &CheckpointSuffixPin,
) -> Result<(CapturePoint, &'a CheckpointSuffixPin)> {
    require_capture_open(record, selected.capture)?;
    let pin = record.checkpoint_suffix_pins.get(&pin_id).ok_or_else(suffix_unavailable)?;
    let mut identity = pin.clone();
    identity.archive_sha256.clone_from(&selected.archive_sha256);
    if &identity != selected || record.incarnation != pin.incarnation || record.log_epoch != pin.log_epoch
        || !admits_custody(record, pin_id, pin)
        || pin.checkpoint.boundary.checked_add(1) != Some(pin.journal_interval[0])
        || pin.replica_through != pin.journal_interval[1]
        || (pin.archive_sha256.is_none() && (pin.expires_at_ms <= now_ms()
            || record.lifecycle != Lifecycle::Live || record.epoch != pin.writer_epoch || record.holder.is_none()))
    {
        return Err(suffix_unavailable());
    }
    let (point, manifest) = capture_receipt(record, archive, deployment, pin.capture)?
        .ok_or_else(suffix_unavailable)?;
    if point.checkpoint != pin.checkpoint || manifest != pin.manifest_sha256 {
        return Err(suffix_unavailable());
    }
    Ok((point, pin))
}

fn suffix_unavailable() -> Anomaly {
    unavailable("The exact replica suffix cannot be materialized")
}

pub(super) fn encode_records(entries: &[ArchivedReplicaEntry]) -> Result<Vec<u8>> {
    let mut out = FORMAT.to_vec();
    for entry in entries {
        entry.encode_length_delimited(&mut out).map_err(|_| suffix_unavailable())?;
        if out.len() > MAX_SEGMENT_BYTES { return Err(suffix_unavailable()); }
    }
    Ok(out)
}

pub(super) fn decode(mut bytes: Bytes) -> Result<Vec<ArchivedReplicaEntry>> {
    if bytes.len() > MAX_SEGMENT_BYTES || !bytes.starts_with(FORMAT) { return Err(suffix_unavailable()); }
    bytes = bytes.slice(FORMAT.len()..);
    let mut entries = Vec::new();
    while !bytes.is_empty() {
        if entries.len() == MAX_SEGMENT_ENTRIES { return Err(suffix_unavailable()); }
        entries.push(ArchivedReplicaEntry::decode_length_delimited(&mut bytes).map_err(|_| suffix_unavailable())?);
    }
    Ok(entries)
}

/// The logical partition advances across replica segments independently of
/// checkpoint boundaries. The archive needs the resolved facts and the
/// exact original transaction and source coordinates.
struct SuffixProgress {
    next: i64,
    tx: TransactionKey,
    replay: LogicalReplay,
    writer_epoch: Option<u64>,
    fence: rtdb_replication::TermFence,
}

impl SuffixProgress {
    fn new(pin: &CheckpointSuffixPin, catalog: &BlockCatalog) -> Result<Self> {
        let tx = catalog.latest_completed_tx().ok_or_else(suffix_unavailable)?;
        if tx.system_time.0 != pin.checkpoint.system_time
            || pin.checkpoint.boundary.checked_add(1) != Some(pin.journal_interval[0])
            || pin.journal_interval[1] != pin.replica_through
            || pin.replica_through == i64::MAX
        {
            return Err(suffix_unavailable());
        }
        Ok(Self {
            next: pin.journal_interval[0], tx,
            replay: LogicalReplay::new(pin.checkpoint, pin.log_epoch, catalog), writer_epoch: None,
            fence: rtdb_replication::TermFence::from_catalog(catalog)?,
        })
    }

    fn admit(&mut self, entry: &ArchivedReplicaEntry, pin: &CheckpointSuffixPin) -> Result<Option<ReplicaEnvelope>> {
        if entry.lsn != self.next || entry.lsn > pin.replica_through {
            return Err(suffix_unavailable());
        }
        let admitted = admit_record(entry, &mut self.fence, &mut self.writer_epoch, pin.writer_epoch)?;
        self.next = entry.lsn.checked_add(1).ok_or_else(suffix_unavailable)?;
        Ok(admitted)
    }

    fn apply(
        &mut self, body: ReplicaEnvelope, pin: &CheckpointSuffixPin,
        apply: impl FnOnce(&rtdb_format::log::proto::ResolvedTx) -> Result<()>,
    ) -> Result<()> {
        match body.message {
            ReplicaBody::ResolvedTx(row) if row.db_op.is_none() && row.external_source_token.is_none() => {
                let id = row.tx_id.ok_or_else(suffix_unavailable)?;
                let time = Timestamp(row.system_time_micros.ok_or_else(suffix_unavailable)?);
                let source = self.replay.transaction_source(row.src_msg_id)?;
                if id <= self.tx.tx_id.0 || time <= self.tx.system_time
                    || id > pin.basis.tx_id.0 || time > pin.basis.system_time || source > pin.source_through
                {
                    return Err(suffix_unavailable());
                }
                apply(&row)?;
                self.tx = TransactionKey { tx_id: TxId(id), system_time: time };
                self.replay.source = source;
            }
            control => self.replay.apply_control(ReplayControl::read(&control)?)?,
        }
        Ok(())
    }

    fn finish(self, pin: &CheckpointSuffixPin) -> Result<()> {
        if self.next.checked_sub(1) != Some(pin.replica_through)
            || self.tx != pin.basis || self.replay.source != pin.source_through
            || self.replay.pending.is_some()
        {
            return Err(suffix_unavailable());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct BlockCut {
    block: i64,
    source: i64,
}

enum ReplayControl {
    Boundary(BlockCut),
    Upload(BlockCut),
    Advance(Option<i64>),
}

impl ReplayControl {
    fn read(body: &ReplicaBody) -> Result<Self> {
        Ok(match body {
            ReplicaBody::BlockBoundary(row) if row.external_source_token.is_none() => Self::Boundary(BlockCut {
                block: row.block_index.ok_or_else(suffix_unavailable)?,
                source: row.latest_processed_msg_id.ok_or_else(suffix_unavailable)?,
            }),
            ReplicaBody::BlockUploaded(row) if row.external_source_token.is_none()
                && row.storage_version == Some(CURRENT_STORAGE_VERSION.0)
                && row.storage_epoch.unwrap_or_default() == 0 => Self::Upload(BlockCut {
                    block: row.block_index.ok_or_else(suffix_unavailable)?,
                    source: row.latest_processed_msg_id.ok_or_else(suffix_unavailable)?,
                }),
            ReplicaBody::NoOp(row) => Self::Advance(row.src_msg_id),
            ReplicaBody::TriesAdded(row) => Self::Advance(row.source_msg_id),
            ReplicaBody::ArtefactsAdded(row) => Self::Advance(row.source_msg_id),
            ReplicaBody::ArtefactsRemoved(row) => Self::Advance(row.source_msg_id),
            ReplicaBody::TriesDeleted(_) => Self::Advance(None),
            _ => return Err(suffix_unavailable()),
        })
    }
}

struct LogicalReplay {
    epoch: StorageEpoch,
    source: i64,
    installed: BlockCut,
    pending: Option<BlockCut>,
}

impl LogicalReplay {
    fn new(checkpoint: Checkpoint, epoch: i32, catalog: &BlockCatalog) -> Self {
        Self {
            epoch: StorageEpoch(epoch), source: checkpoint.source,
            installed: BlockCut {
                block: checkpoint.block,
                source: catalog.latest_source_position().map_or(-1, |position| position.0),
            },
            pending: None,
        }
    }

    fn source_offset(&self, position: i64) -> Result<i64> {
        let offset = SourcePosition(position).after_offset(self.epoch);
        if offset < 0 && position != SourcePosition::from_epoch_offset(self.epoch, -1)?.0 {
            return Err(suffix_unavailable());
        }
        Ok(offset)
    }

    fn transaction_source(&self, position: Option<i64>) -> Result<i64> {
        let next = self.source_offset(position.ok_or_else(suffix_unavailable)?)?;
        if self.pending.is_some() || next <= self.source {
            return Err(suffix_unavailable());
        }
        Ok(next)
    }

    fn apply_control(&mut self, control: ReplayControl) -> Result<()> {
        match control {
            ReplayControl::Boundary(cut) => {
                if self.pending.is_some() || self.installed.block.checked_add(1) != Some(cut.block)
                    || self.source_offset(cut.source)? < self.source
                {
                    return Err(suffix_unavailable());
                }
                self.pending = Some(cut);
            }
            ReplayControl::Upload(cut) => {
                // A restarted writer can confirm earlier retained blocks.
                if cut == self.installed || (cut.block < self.installed.block
                    && cut.source <= SourcePosition::from_epoch_offset(self.epoch, self.source)?.0)
                {
                    return Ok(());
                }
                if self.pending != Some(cut) {
                    return Err(suffix_unavailable());
                }
                self.source = self.source_offset(cut.source)?;
                self.installed = cut;
                self.pending = None;
            }
            ReplayControl::Advance(Some(position)) => {
                let next = self.source_offset(position)?;
                if self.pending.is_some() && next > self.source {
                    return Err(suffix_unavailable());
                }
                self.source = self.source.max(next);
            }
            ReplayControl::Advance(None) => (),
        }
        Ok(())
    }
}

/// The transfer artifact authenticates the original source envelopes. The
/// destination journal carries the same offsets and terms over its own media.
pub(super) fn transfer_control(
    envelope: &ReplicaEnvelope, checkpoint: Checkpoint,
) -> Result<Option<Bytes>> {
    if matches!(&envelope.message, ReplicaBody::ResolvedTx(_) | ReplicaBody::NoOp(_)) {
        return Ok(None);
    }
    let src_msg_id = match ReplayControl::read(&envelope.message)? {
        ReplayControl::Upload(cut) if cut.block == checkpoint.block => return Ok(None),
        ReplayControl::Upload(cut) => Some(cut.source),
        ReplayControl::Advance(position) => position,
        ReplayControl::Boundary(_) => None,
    };
    Ok(Some(ReplicaEnvelope {
        term: envelope.term, term_sequence: envelope.term_sequence,
        message: ReplicaBody::NoOp(NoOp { src_msg_id }),
    }.encode().into()))
}

pub(super) fn admit_record(
    entry: &ArchivedReplicaEntry, fence: &mut rtdb_replication::TermFence,
    previous_writer: &mut Option<u64>, writer_epoch: u64,
) -> Result<Option<ReplicaEnvelope>> {
    if !(1..=writer_epoch).contains(&entry.epoch) || previous_writer.is_some_and(|prior| prior > entry.epoch) {
        return Err(suffix_unavailable());
    }
    let envelope = ReplicaFrame::decode(&entry.payload)?.into_envelope().ok_or_else(suffix_unavailable)?;
    if envelope.term.0 <= 0 || envelope.term_sequence.is_none_or(|sequence| sequence.0 < 0) {
        return Err(suffix_unavailable());
    }
    let admitted = fence.admit(envelope.term, envelope.term_sequence)?;
    if admitted.opens_term() && !matches!(&envelope.message,
        ReplicaBody::NoOp(noop) if noop.src_msg_id.is_none())
    {
        return Err(suffix_unavailable());
    }
    *previous_writer = Some(entry.epoch);
    Ok((admitted.verdict == rtdb_replication::Verdict::Apply).then_some(envelope))
}

#[cfg(test)]
mod custody_tests {
    use rtdb_directory::{Checkpoint, CheckpointCapture, JournalAnchor, MemberId, MemoryDirectory};
    use rtdb_format::log::proto::OversizedMessage;
    use rtdb_replication::{TermFence, TermPosition};
    use rtdb_types::{Term, TermSequence};
    use tokio::sync::Notify;

    use super::*;

    #[test]
    fn archived_records_retain_fenced_offsets_without_admitting_their_payloads() -> Result<()> {
        let mut fence = TermFence::from_boundary(Term(2), Some(TermSequence(5)))?;
        let mut writer = None;
        let mut admit = |term, sequence, epoch, message| {
            let envelope = ReplicaEnvelope { term: Term(term), term_sequence: Some(TermSequence(sequence)), message };
            let entry = ArchivedReplicaEntry { epoch, payload: envelope.encode().into(), ..Default::default() };
            admit_record(&entry, &mut fence, &mut writer, 3)
        };
        let offloaded = || ReplicaBody::OversizedMessage(OversizedMessage::default());
        let claim = || ReplicaBody::NoOp(NoOp { src_msg_id: None });
        assert!(admit(1, 1, 1, offloaded())?.is_none());
        assert!(admit(2, 5, 1, offloaded())?.is_none());
        assert!(admit(2, 7, 2, offloaded())?.is_none());
        assert!(admit(2, 6, 2, offloaded())?.is_none());
        assert!(admit(3, 0, 3, claim())?.is_some());
        assert!(admit(3, 0, 3, claim())?.is_none());
        assert!(admit(3, 1, 3, offloaded())?.is_some());
        assert_eq!(fence.position(), TermPosition::Expecting(TermSequence(2)));
        Ok(())
    }

    #[tokio::test]
    async fn new_custody_requires_the_serving_body_bound_to_the_certified_frame() -> Result<()> {
        let location = tempfile::tempdir().unwrap();
        let storage = CellStorage::Local { root: location.path().join("serving") };
        let keys = crate::keys::LocalKeyService::new(&location.path().join("keys"))?;
        let io = IoRegion::new();
        let bucket = storage.bucket(Arc::clone(&io)).await?;
        let mut record = DeploymentRecord::new("source", "acme", Vec::new(), now_ms());
        record.storage_namespaces = Some(crate::deployment::storage_namespaces(bucket.as_ref(), &keys).await?);
        let root = format!("{}/{}", record.storage_prefix(), checkpoint_storage_root()?);
        let files = PrefixedStore::new(bucket, root);
        files.put(&key(ROOT_MARKER)?, Bytes::copy_from_slice(record.id.as_bytes())).await?;
        let material = keys.key(&record.key_id()).await?;
        let objects = EncryptingStore::new(Arc::new(files), &material)?;
        let memory = MemoryBudget::new(1 << 20);
        let payloads = ServingPayloads::new(&record, &storage, &keys, io, &memory);
        let original = ReplicaEnvelope {
            term: Term(2), term_sequence: Some(TermSequence(1)), message: ReplicaBody::NoOp(NoOp { src_msg_id: Some(17) }),
        };
        let body = original.encode();
        let proxy = original.offloaded(CURRENT_STORAGE_VERSION, StorageEpoch(0), BlockIndex(1), "payload".into(), &body);
        let path = key(&proxy.oversized_payload(CURRENT_STORAGE_VERSION, StorageEpoch(0))?.unwrap())?;
        assert!(payloads.resolve_verified(&proxy).await.is_err());
        let wrong = ReplicaEnvelope { message: ReplicaBody::NoOp(NoOp { src_msg_id: Some(18) }), ..original.clone() };
        objects.put(&path, wrong.encode().into()).await?;
        assert_eq!(payloads.resolve_verified(&proxy).await.err().unwrap().code, "rtdb.format/oversized-payload-mismatch");
        objects.delete(&path).await?;
        objects.put(&path, body.into()).await?;
        assert_eq!(payloads.resolve_verified(&proxy).await?.0, original);
        let mut legacy = proxy;
        if let ReplicaBody::OversizedMessage(reference) = &mut legacy.message { reference.payload_sha256 = None; }
        assert!(payloads.resolve_verified(&legacy).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn a_suffix_copy_keeps_journal_custody_through_provider_drain() -> Result<()> {
        let location = tempfile::tempdir().unwrap();
        let archive = CellStorage::Local { root: location.path().join("archive") };
        let keys = crate::keys::LocalKeyService::new(&location.path().join("keys"))?;
        let directory = MemoryDirectory::new();
        let mut record = DeploymentRecord::new("captured", "acme", Vec::new(), now_ms());
        record.holder = Some(MemberId("writer".into()));
        let deployment = record.id.parse().unwrap();
        let id = record.id.clone();
        let capture = Uuid::new_v4();
        let lease = Uuid::new_v4();
        let checkpoint = Checkpoint {
            block: 1, boundary: 4, source: 2, system_time: 10, root_sha256: Some([1; 32]),
        };
        let pin = CheckpointSuffixPin {
            capture, manifest_sha256: "manifest".into(), checkpoint,
            custody: CheckpointSuffixCustody::Capture,
            incarnation: record.incarnation, log_epoch: record.log_epoch, writer_epoch: record.epoch,
            basis: TransactionKey { tx_id: TxId(3), system_time: Timestamp(20) },
            replica_through: 6, source_through: 3, journal_interval: [5, 6], journal_sha256: [2; 32],
            expires_at_ms: now_ms() + 60_000, archive_sha256: None,
        };
        record.checkpoint_captures.insert(capture, CheckpointCapture {
            archive: super::super::archive_binding(&archive)?,
            incarnation: record.incarnation, log_epoch: record.log_epoch, checkpoint,
            automatic_fork: false, world_replay: None, media_bound: true, manifest_sha256: Some(pin.manifest_sha256.clone()), remote_disposal: None,
        });
        record.checkpoint_suffix_pins.insert(lease, pin.clone());
        let registered = directory.create_deployment(record).await?;
        let mut trimmed = registered.clone();
        trimmed.replica_anchor = Some(Box::new(JournalAnchor {
            root_incarnation: registered.incarnation, interval: [0, 5], digest: [3; 32],
        }));
        assert!(admit_copy(&trimmed, &archive, deployment, lease, &pin).is_err());

        let admitted = Notify::new();
        let started = Arc::new(Notify::new());
        let (drain, provider) = std::sync::mpsc::channel();
        let copy = with_region(
            directory.as_ref(), &keys, &id, &archive, None, Some((capture, Some(lease))), None,
            |record| admit_copy(record, &archive, deployment, lease, &pin),
            |_, _, io| async {
                let provider_started = Arc::clone(&started);
                tokio::spawn(async move {
                    io.run_blocking(move || {
                        provider_started.notify_one();
                        let _ = provider.recv();
                        Ok(())
                    }).await
                });
                started.notified().await;
                admitted.notify_one();
                Ok(())
            },
        );
        let observe = async {
            admitted.notified().await;
            let updated = update_installation(directory.as_ref(), &id, |record| {
                record.checkpoint_suffix_pins.get_mut(&lease).unwrap().archive_sha256 = Some("verified".into());
                Ok(())
            }).await?;
            assert_eq!(updated.required_replica_offsets(pin.expires_at_ms).collect::<Vec<_>>(), [5]);
            assert_eq!(admit_copy(&updated, &archive, deployment, lease, &pin)?, None);
            drain.send(()).unwrap();
            Ok::<(), Anomaly>(())
        };
        tokio::time::timeout(Duration::from_secs(5), async { tokio::try_join!(copy, observe) })
            .await.expect("archive region drains")?;
        let released = directory.deployment(&id).await?.unwrap();
        assert!(released.required_replica_offsets(pin.expires_at_ms).next().is_none());
        Ok(())
    }
}
