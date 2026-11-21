//! Materialize a captured checkpoint under a fresh, held installation.

use std::{collections::BTreeSet, future::Future, sync::Arc};

use bytes::Bytes;
use rtdb_directory::{
    CellMedia, CheckpointStage, CheckpointStageIntent, DeploymentRecord, Directory, HostedTask,
    Lifecycle,
};
use rtdb_storage::{Error, IoRegion, ObjectStore};
use rtdb_types::Result;
use uuid::Uuid;

use super::{
    ArchivedObject, Capture, CapturePoint, archive_key, capture_receipt, checkpoint_storage_root,
    digest, encode, inconsistent, read, read_object, unavailable,
};
use crate::{
    admin::{installation_record, update_installation},
    cipher::Cipher,
    deployment::CellStorage,
    keys::KeyService,
    reference::{PrefixedStore, key},
    store::{
        ROOT_MARKER, reconcile_storage_regions, release_region, remote_storage_region,
        track_checkpoint_stage_publications,
    },
};

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
        if let Some(complete) = staged(&stored, &request, &media)? {
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
    if source.archive_retirement.is_some() {
        return Err(inconsistent("The source archive has been released"));
    }
    let intent = CheckpointStage {
        intent: request,
        source_incarnation: point.incarnation,
        source_log_epoch: point.log_epoch,
        checkpoint: point.checkpoint,
        media,
        staged_manifest_sha256: None,
    };
    target.lifecycle = Lifecycle::Restoring;
    target.checkpoint_stage = Some(intent.clone());
    let retained = match directory.deployment(&target_id).await? {
        Some(record) => record,
        None => match directory.create_deployment(target).await {
            Ok(record) => record,
            Err(error) => directory.deployment(&target_id).await?.ok_or(error)?,
        },
    };
    if let Some(complete) = staged(&retained, &intent.intent, &intent.media)? {
        return Ok(complete);
    }
    if retained.lifecycle != Lifecycle::Restoring
        || retained.checkpoint_stage.as_ref() != Some(&intent)
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
        Some(point.operation),
        None,
        |current| {
            if current.archive_retirement.is_some()
                || capture_receipt(current, archive, point.deployment, point.operation)?
                    != Some((point.clone(), manifest.clone()))
            {
                return Err(inconsistent(
                    "The capture lost its retained archive admission",
                ));
            }
            Ok(())
        },
        |archive_io| {
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
                    Some((Arc::clone(directory), intent.intent.operation)),
                    |current| {
                        if current.lifecycle != Lifecycle::Restoring
                            || current.checkpoint_stage.as_ref() != Some(intent)
                        {
                            return Err(inconsistent(
                                "The target no longer admits this checkpoint stage",
                            ));
                        }
                        Ok(())
                    },
                    |target_io| {
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
        if let Some(prior) = staged(current, &intent.intent, &intent.media)? {
            if prior.staged_manifest_sha256.as_deref() == Some(staged_digest.as_str()) {
                return Ok(());
            }
            return Err(inconsistent(
                "The stage has another retained object inventory",
            ));
        }
        if current.lifecycle != Lifecycle::Restoring
            || !current.storage_regions.is_empty()
            || current.checkpoint_stage.as_ref() != Some(&intent)
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
    staged(&result, &intent.intent, &intent.media)?
        .ok_or_else(|| inconsistent("The stage lost its positive receipt"))
}

fn staged(
    record: &DeploymentRecord,
    request: &CheckpointStageIntent,
    media: &CellMedia,
) -> Result<Option<CheckpointStage>> {
    let retained = record
        .checkpoint_stage
        .as_ref()
        .filter(|stage| {
            &stage.intent == request
                && &stage.media == media
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
async fn with_region<T, F, Fut>(
    directory: &dyn Directory,
    keys: &dyn KeyService,
    deployment: &str,
    media: &CellStorage,
    hosted_task: Option<&HostedTask>,
    capture: Option<Uuid>,
    publication: Option<(Arc<dyn Directory>, Uuid)>,
    admit: impl Fn(&DeploymentRecord) -> Result<()>,
    run: F,
) -> Result<T>
where
    F: FnOnce(Arc<IoRegion>) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let region = Uuid::new_v4();
    let (state, witness) = match media {
        CellStorage::Local { .. } => (
            rtdb_directory::StorageRegion::Local,
            keys.hold_local_storage_region(region)?,
        ),
        CellStorage::S3 { .. } => {
            let owner = hosted_task.cloned().ok_or_else(|| {
                inconsistent("Remote checkpoint staging requires its installed ECS task")
            })?;
            (remote_storage_region(capture, Some(owner)), None)
        }
    };
    let witnessed = witness.is_some();
    let mut io =
        IoRegion::new().child_holding(witness.map(|lock| Arc::new(lock) as Arc<dyn Send + Sync>));
    if matches!(media, CellStorage::S3 { .. }) {
        if let Some((owner, operation)) = publication {
            track_checkpoint_stage_publications(
                &mut io,
                owner,
                deployment.to_owned(),
                region,
                operation,
            );
        }
    }
    let admitted = update_installation(directory, deployment, |record| {
        admit(record)?;
        record.storage_regions.insert(region, state.clone());
        Ok(())
    })
    .await;
    let uncertain = admitted.is_err();
    let result = match admitted {
        Ok(_) => run(io.clone()).await,
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
    let bucket = archive.bucket(archive_io).await?;
    let prefix = format!(
        "checkpoint-captures/{}/{}",
        point.deployment, point.operation
    );
    let metadata = PrefixedStore::new(bucket.clone(), &prefix);
    let capture = read::<Capture>(&metadata, &key("capture.json")?)
        .await?
        .ok_or_else(|| unavailable("The captured manifest is unavailable"))?;
    if &capture.point != point
        || capture.manifest_sha256()? != manifest
        || read::<CapturePoint>(&metadata, &key("intent.json")?)
            .await?
            .as_ref()
            != Some(point)
    {
        return Err(inconsistent(
            "The archive differs from its accepted capture",
        ));
    }
    let material = archive_key(keys, point).await?;
    capture.verify(bucket.clone(), &prefix, &material).await?;
    let source_cipher = Cipher::new(&material)?;
    let media_key = keys.key(&target.key_id()).await?;
    if media_key == material {
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
    crate::store::confirm_root(&destination, &target.id, true).await?;
    let mut objects = Vec::with_capacity(capture.objects.len());
    let manifest_name = key("~checkpoint-stage")?;
    let mut expected = BTreeSet::from([key(ROOT_MARKER)?, manifest_name.clone()]);
    for object in &capture.objects {
        let name = key(&object.name)?;
        let ciphertext = read_object(&source, &name, object).await?;
        let plaintext = source_cipher.open(name.as_str(), &ciphertext)?;
        let resealed = target_cipher.seal_deterministic(name.as_str(), &plaintext)?;
        let staged = ArchivedObject {
            name: object.name.clone(),
            bytes: resealed.len() as u64,
            sha256: digest(&resealed),
        };
        put_exact(&destination, &name, resealed).await?;
        expected.insert(name);
        objects.push(staged);
    }
    // The final stage receipt binds the exact inventory under this root.
    let inventory = encode(&objects)?;
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

async fn put_exact(
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
