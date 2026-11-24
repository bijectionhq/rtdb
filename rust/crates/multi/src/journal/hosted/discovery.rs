use std::sync::Arc;

use crate::keys::{JournalKey, KeyService};

use ordered_logkeeper::ordered::{Checkpoint, Cut};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{
    catalog::BlockCatalog,
    envelope::{ReplicaBody, ReplicaEnvelope, ReplicaFrame},
    keys::{CURRENT_STORAGE_VERSION, block_key},
    log::proto::BlockBoundary,
};
use rtdb_replication::{TermFence, Verdict};
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore};
use rtdb_types::{BlockIndex, ReplicaPosition, Result, StorageEpoch, Term, TermSequence};

use super::{
    CheckpointAuthority, CheckpointCustody, InstalledJournal, Placement, Preparation, checkpoint_for_block, genesis,
    invalid, storage::ReplicaObjects,
};

struct PendingBoundary {
    cut: Cut,
    term: Term,
    sequence: Option<TermSequence>,
    message: BlockBoundary,
}

pub(super) async fn discover(
    placement: &Placement,
    custodian: &InstalledJournal,
    provider: &CheckpointCustody,
    (base, through): (Checkpoint, Cut),
    objects: &ReplicaObjects,
    (keys, io): (&dyn KeyService, &Arc<IoRegion>),
    memory: &Arc<MemoryBudget>,
) -> Result<Option<Preparation>> {
    let (mut fence, expected_block) = if genesis(base) {
        (TermFence::from_boundary(Term(0), None)?, 0)
    } else {
        let opened = provider.open(base).await?;
        let index = opened.checkpoint.block;
        let catalog = read_catalog(opened.store.as_ref(), index, memory).await?;
        (
            TermFence::from_catalog(&catalog)?,
            index
                .checked_add(1)
                .ok_or_else(|| invalid("The checkpoint block index is exhausted"))?,
        )
    };

    let maximum = crate::process::MAXIMUM_LOG_MESSAGE_BYTES
        + ordered_logkeeper::ordered::APPEND_RECORD_CHARGE_BYTES as usize;
    let _page = memory.reserve(maximum * 5)?;
    let mut from = base.cut;
    let mut pending = None;
    while from != through {
        let page = custodian
            .inspect_checkpoint(base, from, through, maximum as u64)
            .await?;
        for row in page.records {
            from = row.result.cut;
            let key = JournalKey::new(custodian.placement(), row.grant.key_generation);
            let bytes = IoRegion::retaining(_page.clone(), key.open(keys, io, row.payload)).await?;
            let frame = ReplicaFrame::decode(&bytes)?;
            if fence.admit(frame.term, frame.term_sequence)?.verdict != Verdict::Apply {
                continue;
            }
            let envelope = frame.into_envelope().ok_or_else(|| {
                invalid("The chosen checkpoint input contains an unknown replica record")
            })?;
            let (envelope, _decoded) =
                resolve_offload(envelope, row.grant.epoch, objects, memory).await?;
            match envelope.message {
                ReplicaBody::BlockBoundary(message)
                    if pending.is_none() && message.block_index == Some(expected_block) =>
                {
                    pending = Some(PendingBoundary {
                        cut: from,
                        term: envelope.term,
                        sequence: envelope.term_sequence,
                        message,
                    });
                }
                ReplicaBody::BlockUploaded(upload)
                    if upload.block_index == Some(expected_block)
                        && upload.storage_version == Some(CURRENT_STORAGE_VERSION.0)
                        && upload.storage_epoch == Some(0) =>
                {
                    let Some(boundary) = &pending else {
                        continue;
                    };
                    let store = objects.open_writer(row.grant.epoch).await?;
                    let catalog = read_catalog(store.as_ref(), expected_block, memory).await?;
                    let checkpoint = checkpoint_for_block(&catalog, placement.worker.log_epoch())
                        .ok_or_else(|| {
                        invalid("The chosen upload has no exact native checkpoint")
                    })?;
                    let boundary_offset = i64::try_from(boundary.cut.next_lsn - 1)
                        .map_err(|_| invalid("The boundary offset exceeds the native epoch"))?;
                    let position = ReplicaPosition::from_epoch_offset(
                        placement.worker.log_epoch(),
                        boundary_offset,
                    )?;
                    let digest = catalog
                        .publication_sha256()
                        .ok_or_else(|| invalid("The catalog has no exact encoding"))?;
                    if catalog.boundary_replica_position() != Some(position)
                        || catalog.boundary_term() != boundary.term
                        || catalog.boundary_term_sequence() != boundary.sequence
                        || boundary.message.latest_processed_msg_id
                            != catalog.as_proto().latest_processed_msg_id
                        || upload.latest_processed_msg_id
                            != catalog.as_proto().latest_processed_msg_id
                        || boundary.message.external_source_token.as_deref()
                            != catalog.external_source_token()
                        || upload.external_source_token.as_deref()
                            != catalog.external_source_token()
                        || upload.catalog_sha256.as_deref() != Some(digest.as_slice())
                    {
                        return Err(invalid(
                            "The accepted catalog does not match its chosen boundary and upload",
                        ));
                    }
                    return Ok(Some(Preparation {
                        authority: CheckpointAuthority::Upload,
                        writer: row.grant,
                        base,
                        through: boundary.cut,
                        checkpoint,
                    }));
                }
                _ => {}
            }
        }
    }
    Ok(None)
}

pub(super) async fn resolve_offload(
    envelope: ReplicaEnvelope,
    grant: u64,
    objects: &ReplicaObjects,
    memory: &Arc<MemoryBudget>,
) -> Result<(ReplicaEnvelope, Option<Arc<MemoryReservation>>)> {
    let ReplicaBody::OversizedMessage(reference) = &envelope.message else {
        return Ok((envelope, None));
    };
    if reference
        .payload_sha256
        .as_ref()
        .is_none_or(|digest| digest.len() != 32)
    {
        return Err(invalid("The chosen replica object has no exact digest"));
    }
    let key = ObjectKey::new(
        envelope
            .oversized_payload(CURRENT_STORAGE_VERSION, StorageEpoch(0))?
            .ok_or_else(|| invalid("The replica record has no offload key"))?,
    )?;
    let store = objects.open_writer(grant).await?;
    let (payload, charge) = read_exact(store.as_ref(), &key, memory).await?;
    Ok((envelope.unwrap_payload(&payload)?, Some(charge)))
}

async fn read_catalog(
    store: &dyn ObjectStore,
    block: i64,
    memory: &Arc<MemoryBudget>,
) -> Result<BlockCatalog> {
    let key = ObjectKey::new(block_key(BlockIndex(block))?)?;
    let (bytes, charge) = read_exact(store, &key, memory).await?;
    BlockCatalog::decode_reserved(&bytes, charge)
}

async fn read_exact(
    store: &dyn ObjectStore,
    key: &ObjectKey,
    memory: &Arc<MemoryBudget>,
) -> Result<(bytes::Bytes, Arc<MemoryReservation>)> {
    let length = usize::try_from(store.head_fresh(key).await?.size)
        .map_err(|_| invalid("The replica object exceeds addressable memory"))?;
    let allocation = store.exact_object_allocation(key, length)?;
    let work = allocation
        .retained_bytes
        .checked_add(allocation.scratch_bytes)
        .and_then(|bytes| length.checked_mul(4)?.checked_add(bytes))
        .ok_or_else(|| invalid("The replica object allocation exceeds addressable memory"))?;
    let charge = memory.reserve(work)?;
    let body = IoRegion::retaining(charge.clone(), store.get_fresh_exact(key, length)).await?;
    Ok((body, charge))
}
