//! Copy an admitted replica suffix into the checkpoint capture's key custody.

use base64::Engine as _;
use bytes::Bytes;
use prost::Message;
use rtdb_directory::{CheckpointSuffixPin, DeploymentRecord, Directory, HostedTask, Lifecycle, now_ms};
use rtdb_format::{
    catalog::BlockCatalog,
    envelope::{ReplicaBody, ReplicaFrame},
    keys::block_key,
};
use rtdb_logkeeper::{ArchivedReplicaEntry, JournalDigest};
use rtdb_types::{Anomaly, BlockIndex, Result, SourcePosition, StorageEpoch, Timestamp, TransactionKey, TxId};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

use super::{
    CapturePoint, URL_SAFE_NO_PAD, capture_receipt, encode, inconsistent, read_object, require_capture_open,
    unavailable, verified_archive, verify_cut,
};
use super::replica_artifact::{CertifiedSegments, ReplicaArtifact, require_chain};
use super::stage::{RegionPublication, with_region};
use crate::{
    admin::{administration_stream, update_installation},
    cipher::Cipher,
    deployment::CellStorage,
    keys::KeyService,
    reference::{PrefixedStore, key},
};
use rtdb_storage::ObjectStore;

pub(crate) const MAX_SEGMENT_BYTES: usize = 8 << 20;
pub(crate) const MAX_SEGMENT_ENTRIES: usize = 4096;
const FORMAT: &[u8] = b"rtdb.replica-suffix.v1\0";

/// Resolve the original accepted publication to its complete transaction and
/// journal cut inside a previously quorum-certified applied interval.
pub(crate) async fn find_held_publication(
    stream: &rtdb_logkeeper::StreamConfig, proof: &JournalDigest,
    record: &DeploymentRecord, keys: &dyn KeyService, cut: &rtdb_node::OwnedForkCut,
) -> Result<(TransactionKey, i64, i64)> {
    let cipher = TerminalReplicaCipher::open(record, keys).await?;
    let mut pages = rtdb_logkeeper::CertifiedRangeReader::new(stream, proof)?;
    let log_epoch = StorageEpoch(record.log_epoch);
    let mut selected = None;
    while let Some(mut entries) = pages.next_page().await? {
        cipher.open_page(&mut entries)?;
        for entry in entries {
            let Some(ReplicaBody::ResolvedTx(row)) = ReplicaFrame::decode(&entry.payload)?.message
            else { continue; };
            if row.system_time_micros != Some(cut.basis().0) { continue; }
            let basis = TransactionKey {
                tx_id: TxId(row.tx_id.ok_or_else(suffix_unavailable)?),
                system_time: cut.basis(),
            };
            let source = SourcePosition(row.src_msg_id.ok_or_else(suffix_unavailable)?).after_offset(log_epoch);
            if !(1..=record.epoch).contains(&entry.epoch) || !row.committed.unwrap_or(false)
                || !cut.matches_basis(basis) || row.db_op.is_some() || row.external_source_token.is_some()
                || source < 0 || selected.replace((basis, entry.lsn, source)).is_some()
            {
                return Err(suffix_unavailable());
            }
        }
    }
    selected.ok_or_else(suffix_unavailable)
}

pub(crate) async fn capture(
    directory: Arc<dyn Directory>, archive: &CellStorage, keys: &dyn KeyService,
    hosted_task: Option<&HostedTask>, deployment: Uuid, pin_id: Uuid,
    selected: CheckpointSuffixPin,
) -> Result<String> {
    let id = deployment.to_string();
    with_region(
        directory.as_ref(), keys, &id, archive, hosted_task, Some(selected.capture),
        Some(RegionPublication::Capture(Arc::clone(&directory))),
        |record| {
            admit_copy(record, archive, deployment, pin_id, &selected)
        },
        |record, region, io| {
            let (directory, selected, id) = (&directory, &selected, &id);
            async move {
                let (point, pin) = admit(&record, archive, deployment, pin_id, selected)?;
                let bucket = archive.bucket(io).await?;
                let (base, material) = verified_archive(bucket.clone(), &point, &pin.manifest_sha256, keys).await?;
                let root = key(&block_key(BlockIndex(point.checkpoint.block))?)?;
                let root_object = base.objects.iter().find(|object| object.name == root.as_str())
                    .ok_or_else(|| inconsistent("The suffix capture omitted its root catalog"))?;
                let prefix = format!("checkpoint-captures/{deployment}/{}", pin.capture);
                let files = PrefixedStore::new(bucket.clone(), format!("{prefix}/objects"));
                let cipher = Cipher::new(&material)?;
                let root_bytes = read_object(&files, &root, root_object).await?;
                let catalog = BlockCatalog::decode(&cipher.open(root.as_str(), &root_bytes)?)?;
                verify_cut(&catalog, point.checkpoint, StorageEpoch(point.log_epoch))?;
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
                        let cipher = TerminalReplicaCipher::open(&record, keys).await?;
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
                            progress.apply_segment(&entries, pin, |_| Ok(()))?;
                            artifact.put_segment(&entries, &mut chain).await?;
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
    {
        return Err(suffix_unavailable());
    }
    Ok(pin)
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
        progress.apply_segment(&entries, pin, |message| recovered.apply_archived_transaction(message, memory))?;
    }
    progress.finish(pin)?;
    require_chain(chain, expected)?;
    let epoch = StorageEpoch(pin.log_epoch);
    if recovered.progress.tx_key != Some(pin.basis)
        || recovered.progress.source_position.after_offset(epoch) != pin.source_through
        || recovered.partition.snapshot().tx_basis() != Some(pin.basis)
    {
        return Err(suffix_unavailable());
    }
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
        bucket, material, format!("checkpoint-captures/{deployment}/{}/replica-suffix/{suffix}/v2", pin.capture),
        encode(&(stream, identity))?, b"RTDB checkpoint suffix segments v2\0",
    )
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
    source: i64,
}

impl TerminalReplayProgress {
    pub(super) fn new(fence: &rtdb_directory::CheckpointTransferFence) -> Self {
        let checkpoint = fence.destination.checkpoint;
        Self { writer_epoch: None, last_offset: checkpoint.boundary, source: checkpoint.source }
    }

    pub(super) fn apply_segment(
        &mut self, entries: &[ArchivedReplicaEntry], fence: &rtdb_directory::CheckpointTransferFence,
        recovered: &mut rtdb_replication::RecoveredPartition, memory: &Arc<rtdb_columnar::MemoryBudget>,
    ) -> Result<()> {
        let checkpoint = fence.destination.checkpoint;
        let log_epoch = StorageEpoch(fence.destination.source_log_epoch);
        for entry in entries {
            let body = ReplicaFrame::decode(&entry.payload)?;
            if self.last_offset.checked_add(1) != Some(entry.lsn)
                || !(1..=fence.source_writer_epoch).contains(&entry.epoch)
                || self.writer_epoch.is_some_and(|previous| entry.epoch < previous)
                || body.term.0 <= 0
                || body.term_sequence.is_none_or(|sequence| sequence.0 < 0)
                || !matches!(recovered.fence.position(), rtdb_replication::TermPosition::Expecting(_))
            {
                return Err(suffix_unavailable());
            }
            let admission = recovered.fence.admit(body.term, body.term_sequence)?;
            if admission.verdict != rtdb_replication::Verdict::Apply
                || (admission.opens_term() && !matches!(&body.message,
                    Some(ReplicaBody::NoOp(noop)) if noop.src_msg_id.is_none()))
            {
                return Err(suffix_unavailable());
            }
            self.writer_epoch = Some(entry.epoch);
            self.last_offset = entry.lsn;
            match body.message {
                Some(ReplicaBody::ResolvedTx(message)) => {
                    let next = message.src_msg_id.map(SourcePosition)
                        .map(|position| position.after_offset(log_epoch))
                        .filter(|offset| *offset > self.source)
                        .ok_or_else(suffix_unavailable)?;
                    recovered.apply_archived_transaction(&message, memory)?;
                    self.source = next;
                }
                Some(ReplicaBody::BlockUploaded(upload)) if upload.block_index == Some(checkpoint.block)
                    && upload.storage_epoch == Some(fence.destination.source_log_epoch)
                    && upload.latest_processed_msg_id.map(SourcePosition)
                        .map(|pos| pos.after_offset(StorageEpoch(fence.destination.source_log_epoch))) == Some(checkpoint.source)
                    && upload.external_source_token.is_none() => (),
                Some(ReplicaBody::NoOp(noop)) if noop.src_msg_id.is_none() => (),
                _ => return Err(suffix_unavailable()),
            }
        }
        Ok(())
    }

    pub(super) fn finish(self, through: i64) -> Result<i64> {
        if self.last_offset != through { return Err(suffix_unavailable()); }
        Ok(self.source)
    }
}

/// Reopen the original certified source interval without accumulating its raw bytes.
pub(super) async fn replay_certified_terminal(
    source: &DeploymentRecord, keys: &dyn KeyService, stream: &rtdb_logkeeper::StreamConfig,
    certificate: Option<&JournalDigest>, fence: &rtdb_directory::CheckpointTransferFence, through: i64,
    recovered: &mut rtdb_replication::RecoveredPartition, memory: &Arc<rtdb_columnar::MemoryBudget>,
) -> Result<i64> {
    let mut progress = TerminalReplayProgress::new(fence);
    let first = fence.destination.checkpoint.boundary.checked_add(1).ok_or_else(suffix_unavailable)?;
    if stream.stream != source.stream("replica") {
        return Err(suffix_unavailable());
    }
    if let Some(certificate) = certificate {
        if certificate.interval != [first, through] { return Err(suffix_unavailable()); }
        let cipher = TerminalReplicaCipher::open(source, keys).await?;
        let mut pages = rtdb_logkeeper::CertifiedRangeReader::new(stream, certificate)?;
        while let Some(mut entries) = pages.next_page().await? {
            let bytes = entries.iter().try_fold(0usize, |total, entry|
                total.checked_add(entry.payload.len().saturating_mul(2).saturating_add(64))
                    .ok_or_else(suffix_unavailable))?;
            let _charge = memory.reserve(bytes)?;
            cipher.open_page(&mut entries)?;
            progress.apply_segment(&entries, fence, recovered, memory)?;
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
    source: i64,
    block: i64,
    block_source: i64,
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
            next: pin.journal_interval[0], tx, source: pin.checkpoint.source,
            block: pin.checkpoint.block, block_source: pin.checkpoint.source, writer_epoch: None,
            fence: rtdb_replication::TermFence::from_catalog(catalog)?,
        })
    }

    fn apply_segment(
        &mut self, entries: &[ArchivedReplicaEntry], pin: &CheckpointSuffixPin,
        mut apply: impl FnMut(&rtdb_format::log::proto::ResolvedTx) -> Result<()>,
    ) -> Result<()> {
        let log_epoch = StorageEpoch(pin.log_epoch);
        for entry in entries {
            if entry.lsn != self.next || entry.lsn > pin.replica_through || self.tx == pin.basis
                || !(1..=pin.writer_epoch).contains(&entry.epoch)
                || self.writer_epoch.is_some_and(|prior| prior > entry.epoch)
            {
                return Err(suffix_unavailable());
            }
            let body = ReplicaFrame::decode(&entry.payload)?;
            if body.term.0 <= 0 || body.term_sequence.is_none_or(|sequence| sequence.0 < 0) {
                return Err(suffix_unavailable());
            }
            let admission = self.fence.admit(body.term, body.term_sequence)?;
            if admission.verdict != rtdb_replication::Verdict::Apply
                || (admission.opens_term() && !matches!(&body.message,
                    Some(ReplicaBody::NoOp(noop)) if noop.src_msg_id.is_none()))
            {
                return Err(suffix_unavailable());
            }
            self.writer_epoch = Some(entry.epoch);
            self.next = entry.lsn.checked_add(1).ok_or_else(suffix_unavailable)?;
            match body.message {
                Some(ReplicaBody::ResolvedTx(row)) if row.db_op.is_none() && row.external_source_token.is_none() => {
                    let id = row.tx_id.ok_or_else(suffix_unavailable)?;
                    let time = Timestamp(row.system_time_micros.ok_or_else(suffix_unavailable)?);
                    let source = SourcePosition(row.src_msg_id.ok_or_else(suffix_unavailable)?).after_offset(log_epoch);
                    if id <= self.tx.tx_id.0 || time <= self.tx.system_time || source <= self.source
                        || id > pin.basis.tx_id.0 || time > pin.basis.system_time || source > pin.source_through
                    {
                        return Err(suffix_unavailable());
                    }
                    apply(&row)?;
                    self.tx = TransactionKey { tx_id: TxId(id), system_time: time };
                    self.source = source;
                }
                Some(ReplicaBody::BlockBoundary(boundary))
                    if boundary.block_index.is_some_and(|block| block >= self.block) => (),
                Some(ReplicaBody::BlockUploaded(upload)) if upload.storage_epoch == Some(pin.log_epoch)
                    && upload.external_source_token.is_none() => {
                    let block = upload.block_index.ok_or_else(suffix_unavailable)?;
                    let source = upload.latest_processed_msg_id.map(SourcePosition)
                        .map(|pos| pos.after_offset(log_epoch)).ok_or_else(suffix_unavailable)?;
                    if block < self.block || source < self.block_source || source > self.source
                        || (block == self.block && source != self.block_source)
                    {
                        return Err(suffix_unavailable());
                    }
                    self.block = block;
                    self.block_source = source;
                }
                Some(ReplicaBody::NoOp(noop)) if noop.src_msg_id.is_none() => (),
                _ => return Err(suffix_unavailable()),
            }
        }
        Ok(())
    }

    fn finish(self, pin: &CheckpointSuffixPin) -> Result<()> {
        if self.next.checked_sub(1) != Some(pin.replica_through)
            || self.tx != pin.basis || self.source != pin.source_through
        {
            return Err(suffix_unavailable());
        }
        Ok(())
    }
}

#[cfg(test)]
mod custody_tests {
    use rtdb_directory::{Checkpoint, CheckpointCapture, JournalAnchor, MemberId, MemoryDirectory};
    use tokio::sync::Notify;

    use super::*;

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
            incarnation: record.incarnation, log_epoch: record.log_epoch, writer_epoch: record.epoch,
            basis: TransactionKey { tx_id: TxId(3), system_time: Timestamp(20) },
            replica_through: 6, source_through: 3, journal_interval: [5, 6], journal_sha256: [2; 32],
            expires_at_ms: now_ms() + 60_000, archive_sha256: None,
        };
        record.checkpoint_captures.insert(capture, CheckpointCapture {
            archive: super::super::archive_binding(&archive)?,
            incarnation: record.incarnation, log_epoch: record.log_epoch, checkpoint,
            automatic_fork: false, media_bound: true, manifest_sha256: Some(pin.manifest_sha256.clone()), remote_disposal: None,
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
            directory.as_ref(), &keys, &id, &archive, None, Some(capture), None,
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
