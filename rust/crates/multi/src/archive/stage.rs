//! Materialize a captured checkpoint under a fresh, held installation.

use std::{collections::BTreeSet, future::Future, sync::Arc};

use bytes::Bytes;
use rtdb_directory::{
    CellMedia, CheckpointStage, CheckpointStageIntent, DeploymentRecord, Directory,
    DisposalReceipt, HostedTask, Lifecycle,
};
use rtdb_format::catalog::CheckpointContents;
use rtdb_storage::{Error, IoRegion, ObjectStore};
use rtdb_types::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    ArchivedObject, CapturePoint, capture_receipt, checkpoint_storage_root, digest, encode,
    inconsistent, read_object, require_capture_open, unavailable, verified_archive,
};
use crate::{
    admin::{begin_disposal, cell_media, installation_record, update_installation},
    cipher::Cipher,
    deployment::CellStorage,
    keys::KeyService,
    reference::{PrefixedStore, key},
    store::{
        CheckpointReplayPublication, ROOT_MARKER, reconcile_storage_regions, release_region, remote_storage_region,
        track_checkpoint_replay_publication, track_checkpoint_stage_publications, track_checkpoint_transfer_publications, track_publications,
    },
};

pub(super) const STAGED_MANIFEST: &str = "~checkpoint-stage";

#[derive(Deserialize, Serialize)]
#[serde(untagged, deny_unknown_fields)]
pub(super) enum StageInventory {
    Primary(Vec<ArchivedObject>),
    Replay { objects: Vec<ArchivedObject> },
}

impl StageInventory {
    pub(super) fn into_contents(self) -> (CheckpointContents, Vec<ArchivedObject>) {
        match self {
            Self::Primary(objects) => (CheckpointContents::Primary, objects),
            Self::Replay { objects } => (CheckpointContents::Replay, objects),
        }
    }
}

/// Close the exact scratch identity, even when cancellation wins the race
/// with staging. Physical disposal stays with the cell's ordinary reaper.
pub(crate) async fn discard_checkpoint_stage(
    directory: &dyn Directory,
    source: &DeploymentRecord,
    archive: &CellStorage,
    storage: &CellStorage,
    keys: &dyn KeyService,
    request: CheckpointStageIntent,
) -> Result<Option<DisposalReceipt>> {
    let id = request.destination.to_string();
    let media = cell_media(storage, keys)?;
    let record = match directory.deployment(&id).await? {
        Some(record) => record,
        None => {
            let (point, _) = capture_receipt(source, archive, request.source, request.capture)?
                .filter(|(_, digest)| digest == &request.source_manifest_sha256)
                .ok_or_else(|| inconsistent("Discard requires the exact original capture"))?;
            let mut tombstone =
                installation_record(request.destination, &request.name, &request.org)?;
            let bucket = storage.bucket(IoRegion::new()).await?;
            tombstone.storage_namespaces = Some(crate::deployment::storage_namespaces(bucket.as_ref(), keys).await?);
            tombstone.lifecycle = Lifecycle::Deleting;
            tombstone.epoch = 1;
            tombstone.checkpoint_stage = Some(Box::new(CheckpointStage {
                intent: request.clone(),
                source_incarnation: point.incarnation,
                source_log_epoch: point.log_epoch,
                checkpoint: point.checkpoint,
                media: media.clone(),
                transfer_only: false,
                staged_manifest_sha256: None,
            }));
            match directory.create_deployment(tombstone).await {
                Ok(accepted) => accepted,
                Err(error) => directory.deployment(&id).await?.ok_or(error)?,
            }
        }
    };
    staged(&record, &request, &media, false)?;
    if record.incarnation != 1
        || record.holder.is_some()
        || record.checkpoint.is_some()
        || !record.ensemble.is_empty()
        || !record.replica_journals.is_empty()
        || record.move_from.is_some()
        || record.moved_at_ms.is_some()
    {
        return Err(inconsistent("Discard requires a held checkpoint stage"));
    }
    match record.lifecycle {
        Lifecycle::Restoring => {
            begin_disposal(directory, &id).await?;
            Ok(None)
        }
        Lifecycle::Deleting => Ok(None),
        Lifecycle::Deleted => record
            .disposal
            .map(|receipt| Some(*receipt))
            .ok_or_else(|| inconsistent("The stage tombstone has no physical disposal receipt")),
        _ => Err(inconsistent("Discard requires a held checkpoint stage")),
    }
}

/// Stage ciphertext in the capture's original cell. The source keeps its
/// current authority. The target remains held with no placement or log;
/// its historical application state must be readmitted before activation.
pub async fn stage_checkpoint(
    directory: Arc<dyn Directory>,
    archive: &CellStorage,
    storage: &CellStorage,
    keys: &dyn KeyService,
    request: CheckpointStageIntent,
    hosted_task: Option<HostedTask>,
) -> Result<CheckpointStage> {
    stage_checkpoint_for(directory, archive, storage, keys, request, hosted_task, false).await
}

pub(super) async fn stage_checkpoint_for(
    directory: Arc<dyn Directory>,
    archive: &CellStorage,
    storage: &CellStorage,
    keys: &dyn KeyService,
    request: CheckpointStageIntent,
    hosted_task: Option<HostedTask>,
    transfer_only: bool,
) -> Result<CheckpointStage> {
    if request.source.is_nil()
        || request.capture.is_nil()
        || request.operation.is_nil()
        || request.source == request.destination
        || archive.overlaps(storage)?
    {
        return Err(inconsistent(
            "The checkpoint stage requires distinct source archive and target identity",
        ));
    }
    let mut target = installation_record(request.destination, &request.name, &request.org)?;
    let source_id = request.source.to_string();
    let target_id = target.id.clone();
    let media = CellMedia {
        objects: storage.custody_namespace()?,
        keys: keys.custody_namespace()?,
    };
    if let Some(stored) = directory.deployment(&target_id).await? {
        if let Some(complete) = staged(&stored, &request, &media, transfer_only)? {
            return Ok(complete);
        }
    }
    let source = directory
        .deployment(&source_id)
        .await?
        .ok_or_else(|| unavailable("The capture's original directory record is unavailable"))?;
    let (point, manifest) = capture_receipt(&source, archive, request.source, request.capture)?
        .filter(|(_, manifest)| manifest == &request.source_manifest_sha256)
        .ok_or_else(|| inconsistent("The source capture has another manifest"))?;
    require_capture_open(&source, request.capture)?;
    if transfer_only && (source.writer_pool().is_none()
        || !super::same_incarnation(&source, &point) || source.checkpoint != Some(point.checkpoint))
    {
        return Err(inconsistent("A terminal transfer requires the original current source checkpoint"));
    }
    Box::pin(super::admit_lifetime_restore(
        directory.as_ref(), archive, keys, &point, &manifest, hosted_task.as_ref(), transfer_only, false,
    )).await?;
    let intent = CheckpointStage {
        intent: request,
        source_incarnation: point.incarnation,
        source_log_epoch: point.log_epoch,
        checkpoint: point.checkpoint,
        media,
        transfer_only,
        staged_manifest_sha256: None,
    };
    let bucket = storage.bucket(IoRegion::new()).await?;
    let namespaces = crate::deployment::storage_namespaces(bucket.as_ref(), keys).await?;
    target.storage_namespaces = Some(namespaces);
    target.lifecycle = Lifecycle::Restoring;
    target.checkpoint_stage = Some(Box::new(intent.clone()));
    let retained = match directory.deployment(&target_id).await? {
        Some(record) => record,
        None => match directory.create_deployment(target).await {
            Ok(record) => record,
            Err(error) => directory.deployment(&target_id).await?.ok_or(error)?,
        },
    };
    if let Some(complete) = staged(&retained, &intent.intent, &intent.media, transfer_only)? {
        return Ok(complete);
    }
    crate::deployment::require_storage_namespaces(&retained, namespaces)?;
    if retained.lifecycle != Lifecycle::Restoring
        || retained.checkpoint_stage.as_deref() != Some(&intent)
    {
        return Err(inconsistent(
            "The target does not hold this checkpoint stage",
        ));
    }
    reconcile_storage_regions(directory.as_ref(), storage, keys, &retained).await?;

    let staged_digest = with_region(
        directory.as_ref(),
        keys,
        &source_id,
        archive,
        hosted_task.as_ref(),
        Some((point.operation, None)),
        None,
        |current| {
            require_capture_open(current, point.operation)?;
            if capture_receipt(current, archive, point.deployment, point.operation)?
                != Some((point.clone(), manifest.clone()))
            {
                return Err(inconsistent(
                    "The capture lost its retained archive admission",
                ));
            }
            Ok(None)
        },
        |_, _, archive_io| {
            let (directory, target_id, intent) = (&directory, &target_id, &intent);
            let (retained, point, manifest, hosted_task) =
                (&retained, &point, &manifest, hosted_task.as_ref());
            async move {
                with_region(
                    directory.as_ref(),
                    keys,
                    target_id,
                    storage,
                    hosted_task,
                    None,
                    Some(RegionPublication::Stage(Arc::clone(directory), intent.intent.operation)),
                    |current| {
                        if current.lifecycle != Lifecycle::Restoring
                            || current.checkpoint_stage.as_deref() != Some(intent)
                        {
                            return Err(inconsistent(
                                "The target no longer admits this checkpoint stage",
                            ));
                        }
                        Ok(None)
                    },
                    |_, _, target_io| {
                        copy_checkpoint(
                            archive, storage, keys, retained, point, manifest, archive_io,
                            target_io,
                        )
                    },
                )
                .await
            }
        },
    )
    .await?;
    let result = update_installation(directory.as_ref(), &target_id, |current| {
        if let Some(prior) = staged(current, &intent.intent, &intent.media, transfer_only)? {
            if prior.staged_manifest_sha256.as_deref() == Some(staged_digest.as_str()) {
                return Ok(());
            }
            return Err(inconsistent(
                "The stage has another retained object inventory",
            ));
        }
        if current.lifecycle != Lifecycle::Restoring
            || !current.storage_regions.is_empty()
            || current.checkpoint_stage.as_deref() != Some(&intent)
        {
            return Err(unavailable(
                "The staging target changed or still has open storage admissions",
            ));
        }
        current
            .checkpoint_stage
            .as_mut()
            .expect("the exact intent was checked")
            .staged_manifest_sha256 = Some(staged_digest.clone());
        Ok(())
    })
    .await?;
    staged(&result, &intent.intent, &intent.media, transfer_only)?
        .ok_or_else(|| inconsistent("The stage lost its positive receipt"))
}

fn staged(
    record: &DeploymentRecord,
    request: &CheckpointStageIntent,
    media: &CellMedia,
    transfer_only: bool,
) -> Result<Option<CheckpointStage>> {
    let retained = record
        .checkpoint_stage
        .as_deref()
        .or_else(|| record.checkpoint_transfer_activation.as_deref()
            .map(|activation| &activation.replica.bootstrap.terminal.fence.destination))
        .filter(|stage| {
            &stage.intent == request
                && &stage.media == media
                && stage.transfer_only == transfer_only
                && record.name == request.name
                && record.org == request.org
        })
        .ok_or_else(|| {
            inconsistent("The target identity belongs to another installation or stage")
        })?;
    Ok(retained
        .staged_manifest_sha256
        .as_ref()
        .map(|_| retained.clone()))
}

/// Keep the archive key or target upload under its original directory
/// inventory until all asynchronous storage operations have drained.
pub(crate) enum RegionPublication {
    Stage(Arc<dyn Directory>, Uuid),
    CheckpointTransfer(Arc<dyn Directory>, Uuid),
    CheckpointReplay(Arc<dyn Directory>, CheckpointReplayPublication),
    Capture(Arc<dyn Directory>),
}

pub(crate) async fn with_region<T, F, Fut>(
    directory: &dyn Directory,
    keys: &dyn KeyService,
    deployment: &str,
    media: &CellStorage,
    hosted_task: Option<&HostedTask>,
    archive: Option<(Uuid, Option<Uuid>)>,
    publication: Option<RegionPublication>,
    admit: impl Fn(&DeploymentRecord) -> Result<Option<i64>>,
    run: F,
) -> Result<T>
where
    F: FnOnce(DeploymentRecord, Uuid, Arc<IoRegion>) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let region = Uuid::new_v4();
    let (capture, checkpoint_suffix) = archive.map_or((None, None), |(capture, suffix)| (Some(capture), suffix));
    let (state, witness) = match media {
        CellStorage::Local { .. } => (
            rtdb_directory::StorageRegion::Local { archive: capture, checkpoint_suffix, repair_archive: None, tracks_ordered_index_pages: true, checkpoint_metadata_from: None, checkpoint_replay_from: None, },
            keys.hold_local_storage_region(region)?,
        ),
        CellStorage::S3 { .. } => {
            let owner = hosted_task.cloned().ok_or_else(|| {
                inconsistent("Remote checkpoint staging requires its installed ECS task")
            })?;
            let mut state = remote_storage_region(capture, Some(owner));
            if let rtdb_directory::StorageRegion::Remote { checkpoint_suffix: held_suffix, .. } = &mut state {
                *held_suffix = checkpoint_suffix;
            }
            (state, None)
        }
    };
    let witnessed = witness.is_some();
    let mut io =
        IoRegion::new().child_holding(witness.map(|lock| Arc::new(lock) as Arc<dyn Send + Sync>));
    if matches!(media, CellStorage::S3 { .. }) {
        match publication {
            Some(RegionPublication::Stage(owner, operation)) => {
                track_checkpoint_stage_publications(&mut io, owner, deployment.to_owned(), region, operation);
            }
            Some(RegionPublication::CheckpointTransfer(owner, operation)) => {
                track_checkpoint_transfer_publications(&mut io, owner, deployment.to_owned(), region, operation);
            }
            Some(RegionPublication::CheckpointReplay(owner, replay)) => {
                track_checkpoint_replay_publication(&mut io, owner, deployment.to_owned(), region, replay);
            }
            Some(RegionPublication::Capture(owner)) => {
                track_publications(&mut io, owner, deployment.to_owned(), region, None);
            }
            None => (),
        }
    }
    let admitted = update_installation(directory, deployment, |record| {
        let first_required = admit(record)?;
        let mut state = state.clone();
        if let rtdb_directory::StorageRegion::Local { checkpoint_replay_from, .. }
            | rtdb_directory::StorageRegion::Remote { checkpoint_replay_from, .. } = &mut state
        {
            *checkpoint_replay_from = first_required;
        }
        record.storage_regions.insert(region, state);
        Ok(())
    })
    .await;
    let uncertain = admitted.is_err();
    let result = match admitted {
        Ok(record) => {
            let work = run(record, region, io.clone());
            if capture.is_some() {
                let view = rtdb_storage::HeldView::recovery(io.clone());
                IoRegion::reading(view, work).await
            } else {
                work.await
            }
        }
        Err(error) => Err(error),
    };
    io.close_and_wait().await;
    drop(io);
    release_region(directory, deployment, region, uncertain).await?;
    if witnessed {
        keys.forget_local_storage_region(region)?;
    }
    result
}

async fn copy_checkpoint(
    archive: &CellStorage,
    storage: &CellStorage,
    keys: &dyn KeyService,
    target: &DeploymentRecord,
    point: &CapturePoint,
    manifest: &str,
    archive_io: Arc<IoRegion>,
    target_io: Arc<IoRegion>,
) -> Result<String> {
    let bucket = archive.bucket(Arc::clone(&archive_io)).await?;
    let prefix = format!(
        "checkpoint-captures/{}/{}",
        point.deployment, point.operation
    );
    let (capture, source_cipher) = verified_archive(bucket.clone(), point, manifest, keys, archive_io).await?;
    let media_key = keys.key(&target.key_id()).await?;
    if source_cipher.matches(media_key).await? {
        return Err(inconsistent(
            "The staged installation must have its own media key",
        ));
    }
    let target_cipher = Cipher::new(&media_key)?;
    let source = PrefixedStore::new(bucket, format!("{prefix}/objects"));
    let destination = PrefixedStore::new(
        storage.bucket(target_io).await?,
        format!("{}/{}", target.storage_prefix(), checkpoint_storage_root()?),
    );
    crate::store::check_root(&destination, &target.id, true).await?;
    let mut objects = Vec::with_capacity(capture.objects.len());
    let manifest_name = key(STAGED_MANIFEST)?;
    let mut expected = BTreeSet::from([key(ROOT_MARKER)?, manifest_name.clone()]);
    for object in &capture.objects {
        let name = key(&object.name)?;
        let ciphertext = read_object(&source, &name, object).await?;
        let plaintext = object.open(source_cipher.as_ref(), &name, ciphertext).await?;
        let resealed = target_cipher.seal_object(&name, &plaintext)?;
        let staged = ArchivedObject::from_sealed(&name, &resealed)?;
        put_exact(&destination, &name, resealed).await?;
        expected.insert(name);
        objects.push(staged);
    }
    // The final stage receipt binds the exact inventory under this root.
    let inventory = match capture.format {
        CheckpointContents::Primary => StageInventory::Primary(objects),
        CheckpointContents::Replay => StageInventory::Replay { objects },
    };
    let inventory = encode(&inventory)?;
    let digest = digest(&inventory);
    put_exact(&destination, &manifest_name, inventory).await?;
    let observed = destination
        .list(None)
        .await?
        .into_iter()
        .map(|object| object.key)
        .collect::<BTreeSet<_>>();
    if observed != expected {
        return Err(inconsistent(
            "The staging root contains objects outside the accepted inventory",
        ));
    }
    Ok(digest)
}

pub(super) async fn put_exact(
    store: &dyn ObjectStore,
    name: &rtdb_storage::ObjectKey,
    bytes: Bytes,
) -> Result<()> {
    match store.get(name).await {
        Ok(prior) if prior == bytes => Ok(()),
        Ok(_) => Err(Error::ConflictingObject(name.to_string()).into()),
        Err(Error::MissingObject(_)) => Ok(store.put(name, bytes).await?),
        Err(error) => Err(error.into()),
    }
}
