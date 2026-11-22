//! Copy an admitted replica suffix into the checkpoint capture's key custody.

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
    CapturePoint, capture_receipt, digest, inconsistent, read_object, require_capture_open,
    unavailable, verified_archive, verify_cut,
};
use super::stage::{RegionPublication, with_region};
use crate::{
    admin::{administration_stream, update_installation},
    cipher::Cipher,
    deployment::CellStorage,
    keys::KeyService,
    reference::{PrefixedStore, key},
};
use rtdb_storage::ObjectStore;

const MAX_BYTES: usize = 8 << 20;
pub(crate) const MAX_SUFFIX_ENTRIES: usize = 4096;
const FORMAT: &[u8] = b"rtdb.replica-suffix.v1\0";

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
            admit(record, archive, deployment, pin_id, &selected).map(|_| ())
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
                let object = key(&format!("{prefix}/replica-suffix/{pin_id}.v1"))?;
                let certificate = JournalDigest::from_digest_bytes(
                    record.stream("replica"), pin.journal_interval, pin.journal_sha256,
                );

                let digest = if let Some(expected) = &pin.archive_sha256 {
                    let sealed = read_sealed(bucket.as_ref(), &object, expected).await?;
                    let plain = cipher.open(object.as_str(), &sealed)?;
                    validate_records(&decode(plain)?, pin, &certificate, &catalog)?;
                    expected.clone()
                } else {
                    let stream = administration_stream(directory.as_ref(), &record, 4 << 20).await?;
                    let remaining = pin.expires_at_ms.saturating_sub(now_ms()).max(0) as u64;
                    let entries = tokio::time::timeout(Duration::from_millis(remaining),
                        rtdb_logkeeper::copy_certified_range(&stream, &certificate, MAX_BYTES, MAX_SUFFIX_ENTRIES),
                    ).await.map_err(|_| suffix_unavailable())??;
                    validate_records(&entries, pin, &certificate, &catalog)?;
                    let sealed = cipher.seal_deterministic(object.as_str(), &encode_records(&entries)?)?;
                    let digest = digest(&sealed);
                    bucket.put(&object, sealed).await?;
                    read_sealed(bucket.as_ref(), &object, &digest).await?;
                    digest
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

async fn read_sealed(bucket: &dyn ObjectStore, object: &rtdb_storage::ObjectKey, expected: &str) -> Result<Bytes> {
    let size = bucket.head_fresh(object).await?.size;
    if size > MAX_BYTES as u64 + 1024 { return Err(suffix_unavailable()); }
    let sealed = bucket.get_range(object, 0..size).await?;
    if sealed.len() as u64 != size || digest(&sealed) != expected { return Err(suffix_unavailable()); }
    Ok(sealed)
}

fn suffix_unavailable() -> Anomaly {
    unavailable("The exact replica suffix cannot be materialized")
}

fn encode_records(entries: &[ArchivedReplicaEntry]) -> Result<Vec<u8>> {
    let mut out = FORMAT.to_vec();
    for entry in entries {
        entry.encode_length_delimited(&mut out).map_err(|_| suffix_unavailable())?;
        if out.len() > MAX_BYTES { return Err(suffix_unavailable()); }
    }
    Ok(out)
}

fn decode(mut bytes: Bytes) -> Result<Vec<ArchivedReplicaEntry>> {
    if bytes.len() > MAX_BYTES || !bytes.starts_with(FORMAT) { return Err(suffix_unavailable()); }
    bytes = bytes.slice(FORMAT.len()..);
    let mut entries = Vec::new();
    while !bytes.is_empty() {
        if entries.len() == MAX_SUFFIX_ENTRIES { return Err(suffix_unavailable()); }
        entries.push(ArchivedReplicaEntry::decode_length_delimited(&mut bytes).map_err(|_| suffix_unavailable())?);
    }
    Ok(entries)
}

fn validate_records(
    entries: &[ArchivedReplicaEntry], pin: &CheckpointSuffixPin,
    certificate: &JournalDigest, catalog: &BlockCatalog,
) -> Result<()> {
    rtdb_logkeeper::verify_certified_range(certificate, entries)?;
    let log_epoch = StorageEpoch(pin.log_epoch);
    let mut tx = catalog.latest_completed_tx().ok_or_else(suffix_unavailable)?;
    let mut source = pin.checkpoint.source;
    let mut position = None;
    for entry in entries {
        if tx == pin.basis { return Err(suffix_unavailable()); }
        let body = ReplicaFrame::decode(&entry.payload)?;
        let sequence = body.term_sequence.filter(|sequence| sequence.0 >= 0).ok_or_else(suffix_unavailable)?;
        if entry.epoch != pin.writer_epoch || body.term.0 <= 0
            || position.is_some_and(|(term, prior): (_, i64)| body.term != term || prior.checked_add(1) != Some(sequence.0))
        {
            return Err(suffix_unavailable());
        }
        position = Some((body.term, sequence.0));
        match body.message {
            Some(ReplicaBody::ResolvedTx(row)) if row.db_op.is_none() && row.external_source_token.is_none() => {
                let id = row.tx_id.ok_or_else(suffix_unavailable)?;
                let time = Timestamp(row.system_time_micros.ok_or_else(suffix_unavailable)?);
                let offset = SourcePosition(row.src_msg_id.ok_or_else(suffix_unavailable)?).after_offset(log_epoch);
                if id <= tx.tx_id.0 || time <= tx.system_time || offset <= source
                    || id > pin.basis.tx_id.0 || time > pin.basis.system_time || offset > pin.source_through
                {
                    return Err(suffix_unavailable());
                }
                tx = TransactionKey { tx_id: TxId(id), system_time: time };
                source = offset;
            }
            Some(ReplicaBody::BlockUploaded(upload)) if upload.block_index == Some(pin.checkpoint.block)
                && upload.storage_epoch == Some(pin.log_epoch)
                && upload.latest_processed_msg_id.map(SourcePosition).map(|pos| pos.after_offset(log_epoch)) == Some(pin.checkpoint.source)
                && upload.external_source_token.is_none() => (),
            Some(ReplicaBody::NoOp(noop)) if noop.src_msg_id.is_none() => (),
            _ => return Err(suffix_unavailable()),
        }
    }
    if tx != pin.basis || source != pin.source_through { return Err(suffix_unavailable()); }
    Ok(())
}
