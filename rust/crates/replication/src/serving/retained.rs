use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_format::{catalog::BlockCatalog, journal::ObjectId, log::proto};
use rtdb_index::{Snapshot, StagedWrite};
use rtdb_storage::{ObjectStore, cache::BufferPool};
use rtdb_tx::transact::Outcome;
use rtdb_types::{Anomaly, ReplicaPosition, Result, SourcePosition, Term, rtdb_v1::Basis};
use tokio_util::sync::CancellationToken;

use crate::default_erasure::ErasureRetirement;

use super::{
    NativeCheckpointImage, NativeServing, NativeServingAuthority, classic, invalid,
    require_physical_replay_support,
};

pub(crate) fn checkpoint_incarnation(block: &BlockCatalog) -> Result<Option<[u8; 16]>> {
    let wire = block.as_proto();
    match (
        wire.serving_checkpoint.as_deref(),
        wire.serving_checkpoint_object.as_deref(),
    ) {
        (Some(image), None) => Ok(Some(
            Basis::from_bytes(image.get(8..32).ok_or_else(invalid)?)?.incarnation(),
        )),
        (None, Some(object)) => Ok(Some(*object.parse::<ObjectId>()?.database.as_bytes())),
        (None, None) => Ok(None),
        _ => Err(invalid()),
    }
}

pub(crate) async fn open(
    authority: Option<NativeServingAuthority>,
    block: Option<&BlockCatalog>,
    snapshot: Snapshot,
    storage: &Arc<BufferPool>,
    memory: &Arc<MemoryBudget>,
    cancellation: &CancellationToken,
) -> Result<Option<NativeServing>> {
    let incarnation = block.map(checkpoint_incarnation).transpose()?.flatten();
    let authority = authority.or_else(|| {
        incarnation.map(|incarnation| NativeServingAuthority {
            incarnation,
            checkpoints: None,
        })
    });
    let Some(authority) = authority else {
        return Ok(None);
    };
    let store = storage.object_store();
    let checkpoint: Option<NativeCheckpointImage> = match block {
        Some(block) => {
            authority
                .read_checkpoint(
                    store.as_ref(),
                    block.as_proto().serving_checkpoint.as_deref(),
                    block.as_proto().serving_checkpoint_object.as_deref(),
                )
                .await?
        }
        None => None,
    };
    open_selected(authority, block, snapshot, storage, memory, cancellation, checkpoint.as_ref()).await.map(Some)
}

pub(crate) async fn open_selected(
    authority: NativeServingAuthority, block: Option<&BlockCatalog>, snapshot: Snapshot,
    storage: &Arc<BufferPool>, memory: &Arc<MemoryBudget>, cancellation: &CancellationToken, checkpoint: Option<&NativeCheckpointImage>,
) -> Result<NativeServing> {
    let (mut serving, _) = NativeServing::open(authority, Arc::clone(storage.object_store()), checkpoint, false)?;
    if serving.pending_erasure().is_some() { return Err(erasure_recovery_required()); }
    if let Some(block) = block { serving.verify_checkpoint_prefix(block)?; }
    serving.recover_classic(snapshot, block.and_then(BlockCatalog::boundary_replica_position), storage, memory, cancellation,
        true, checkpoint.is_some_and(NativeServing::is_shared_checkpoint)).await?;
    Ok(serving)
}

pub(crate) fn genesis(
    incarnation: [u8; 16],
    store: &Arc<dyn ObjectStore>,
) -> Result<NativeServing> {
    let authority = NativeServingAuthority {
        incarnation,
        checkpoints: None,
    };
    NativeServing::open(authority, Arc::clone(store), None, false).map(|(serving, _)| serving)
}

pub(crate) struct AppliedDecision {
    pub source: SourcePosition,
    pub write: Option<(StagedWrite, bool)>,
}

pub(crate) fn apply(
    serving: Option<&mut NativeServing>,
    message: &proto::NoOp,
    snapshot: Snapshot,
    previous: SourcePosition,
    replica: ReplicaPosition,
    term: Term,
    memory: &Arc<MemoryBudget>,
) -> Result<Option<AppliedDecision>> {
    if crate::checkpoint_admin::PinControl::decode(message)?.is_some() { return Ok(None); }
    if ErasureRetirement::decode(message)?.is_some() {
        return Err(erasure_recovery_required());
    }
    let Some(serving) = serving else {
        require_physical_replay_support(message)?;
        return Ok(message.src_msg_id.map(|source| AppliedDecision {
            source: SourcePosition(source),
            write: None,
        }));
    };
    if message
        .src_msg_id
        .is_some_and(|source| SourcePosition(source) <= previous)
    {
        return Err(invalid());
    }
    let mut write = None;
    let completion = serving.apply_ordered(
        message,
        false,
        Some((replica, term)),
        |source, receipt, prior, effects| {
            let committed = !matches!(receipt.outcome, Outcome::Refused { .. });
            write = Some((
                classic::stage_source(snapshot, prior, source, receipt, effects, memory)?,
                committed,
            ));
            Ok(())
        },
    )?;
    if serving.pending_erasure().is_some() {
        return Err(erasure_recovery_required());
    }
    Ok(completion.map(|(source, _)| AppliedDecision { source, write }))
}

fn erasure_recovery_required() -> Anomaly {
    Anomaly::unsupported(
        "rtdb/default-erasure-retained-replay",
        "Retained replay requires the Default erasure recovery fence",
    )
}
