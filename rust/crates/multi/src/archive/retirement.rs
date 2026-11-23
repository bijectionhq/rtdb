//! Independent physical retirement of one captured checkpoint.

use rtdb_directory::{
    CheckpointCaptureRetirement, DeploymentRecord, Directory, Lifecycle, now_ms,
};
use rtdb_storage::IoRegion;
use rtdb_types::Result;
use uuid::Uuid;

use super::{archive_binding, archive_key_id, capture_point, inconsistent, key, unavailable};
use crate::{
    admin::update_installation, deployment::CellStorage, keys::KeyService,
    store::reconcile_storage_regions,
};

pub(crate) fn require_capture_open(record: &DeploymentRecord, capture: Uuid) -> Result<()> {
    if record.archive_retirement.is_some()
        || record.checkpoint_capture_retirements.contains_key(&capture)
    {
        return Err(inconsistent(
            "The checkpoint capture is closed to archive access",
        ));
    }
    Ok(())
}

/// Persist the release before erasing. With no registered capture, the ID
/// still receives a tombstone so a late start cannot create media.
pub(crate) async fn retire_capture(
    directory: &dyn Directory,
    installation: &DeploymentRecord,
    storage: &CellStorage,
    archive: &CellStorage,
    keys: &dyn KeyService,
    capture: Uuid,
    manifest: Option<&str>,
) -> Result<CheckpointCaptureRetirement> {
    retire_capture_inner(directory, installation, storage, archive, keys, capture, manifest, false).await
}

pub(crate) fn obsolete_fork_input(record: &DeploymentRecord, capture: Uuid) -> bool {
    record.checkpoint_captures.get(&capture).is_some_and(|registered|
        registered.automatic_fork && Some(registered.checkpoint) != record.checkpoint
    )
        && !record.checkpoint_capture_retirements.contains_key(&capture)
        && !record.storage_regions.values().any(|region| region.holds_archive(capture))
        && !record.checkpoint_worlds.values().any(|world|
            world.capture == capture && world.until.0 > now_ms().saturating_mul(1000)
        )
        && !record.checkpoint_suffix_pins.values().any(|pin|
            pin.capture == capture && pin.expires_at_ms > now_ms()
        )
        && !record.checkpoint_recovery.as_ref().is_some_and(|repair| repair.operation == capture)
}

pub(crate) async fn retire_obsolete_fork_input(
    directory: &dyn Directory, record: &DeploymentRecord, storage: &CellStorage,
    keys: &dyn KeyService, capture: Uuid,
) -> Result<()> {
    let registration = record.checkpoint_captures.get(&capture)
        .filter(|_| obsolete_fork_input(record, capture))
        .ok_or_else(|| unavailable("The automatic fork input remains retained"))?;
    let archive: CellStorage = serde_json::from_str(&registration.archive)
        .map_err(|_| inconsistent("Invalid retained automatic capture custody"))?;
    retire_capture_inner(
        directory, record, storage, &archive, keys, capture, registration.manifest_sha256.as_deref(), true,
    ).await?;
    Ok(())
}

async fn retire_capture_inner(
    directory: &dyn Directory,
    installation: &DeploymentRecord,
    storage: &CellStorage,
    archive: &CellStorage,
    keys: &dyn KeyService,
    capture: Uuid,
    manifest: Option<&str>,
    require_obsolete_fork: bool,
) -> Result<CheckpointCaptureRetirement> {
    let archive = archive_binding(archive)?;
    if let Some(prior) = installation.checkpoint_capture_retirements.get(&capture) {
        require_same_release(prior, &archive, manifest)?;
        if prior.observed_empty_at_ms.is_some() {
            return Ok(prior.clone());
        }
    } else {
        update_installation(directory, &installation.id, |record| {
            if let Some(prior) = record.checkpoint_capture_retirements.get(&capture) {
                return require_same_release(prior, &archive, manifest);
            }
            if require_obsolete_fork && !obsolete_fork_input(record, capture) {
                return Err(unavailable("The automatic fork input remains retained"));
            }
            require_capture_open(record, capture)?;
            if record.checkpoint_suffix_pins.values().any(|pin| {
                pin.capture == capture && pin.expires_at_ms > now_ms()
            }) {
                return Err(inconsistent("A live suffix pin retains this checkpoint capture"));
            }
            if record.checkpoint_worlds.values().any(|world| {
                world.capture == capture && world.until.0 > now_ms().saturating_mul(1000)
            }) {
                return Err(inconsistent("A live world retains this checkpoint capture"));
            }

            if record
                .checkpoint_recovery
                .as_ref()
                .is_some_and(|repair| repair.operation == capture)
                && (record.lifecycle != Lifecycle::Deleted || record.disposal.is_none())
            {
                return Err(inconsistent(
                    "The serving history still depends on this capture",
                ));
            }
            let registration = record.checkpoint_captures.get(&capture);
            if registration.is_some_and(|capture| capture.world_replay.is_some())
                && record.lifecycle != Lifecycle::Deleted
            {
                return Err(inconsistent("A held restore still requires the replay parent base"));
            }
            let retained = registration.and_then(|value| value.manifest_sha256.as_deref());
            if registration.is_some_and(|value| value.archive != archive)
                || manifest.is_some_and(|expected| retained != Some(expected))
            {
                return Err(inconsistent(
                    "The release names another archived checkpoint",
                ));
            }
            record.checkpoint_capture_retirements.insert(
                capture,
                CheckpointCaptureRetirement {
                    archive: archive.clone(),
                    manifest_sha256: retained.map(str::to_owned),
                    authorized_at_ms: now_ms(),
                    key_withdrawn_at_ms: None,
                    observed_empty_at_ms: None,
                },
            );
            Ok(())
        })
        .await?;
    }
    finish_capture_retirement(directory, storage, keys, &installation.id, capture).await
}

fn require_same_release(
    retained: &CheckpointCaptureRetirement,
    archive: &str,
    manifest: Option<&str>,
) -> Result<()> {
    if retained.archive != archive
        || manifest.is_some_and(|expected| retained.manifest_sha256.as_deref() != Some(expected))
    {
        return Err(inconsistent(
            "The capture was closed under another archive or manifest",
        ));
    }
    Ok(())
}

pub(crate) async fn finish_capture_retirement(
    directory: &dyn Directory,
    storage: &CellStorage,
    keys: &dyn KeyService,
    id: &str,
    capture: Uuid,
) -> Result<CheckpointCaptureRetirement> {
    let mut record = directory
        .deployment(id)
        .await?
        .ok_or_else(|| unavailable("The original capture owner is unavailable"))?;
    let accepted = record
        .checkpoint_capture_retirements
        .get(&capture)
        .ok_or_else(|| inconsistent("The checkpoint capture has no release authority"))?
        .clone();
    if accepted.observed_empty_at_ms.is_some() {
        return Ok(accepted);
    }
    let pending = |record: &DeploymentRecord| {
        record.storage_regions.values().any(|region| region.holds_archive(capture))
    };
    if pending(&record) {
        reconcile_storage_regions(directory, storage, keys, &record).await?;
        record = directory
            .deployment(id)
            .await?
            .ok_or_else(|| unavailable("The original capture owner is unavailable"))?;
        if pending(&record) {
            return Err(unavailable("The capture still has admitted archive I/O"));
        }
    }
    let deployment = Uuid::parse_str(id).map_err(|_| inconsistent("Invalid capture owner"))?;
    if record
        .checkpoint_captures
        .get(&capture)
        .is_some_and(|value| value.media_bound)
    {
        let point = capture_point(&record, deployment, capture, &accepted.archive)?;
        keys.destroy(&archive_key_id(&point)).await?;
    }
    let withdrawn_at = now_ms().max(accepted.authorized_at_ms);
    let archive: CellStorage = serde_json::from_str(&accepted.archive)
        .map_err(|_| inconsistent("Invalid retained capture custody"))?;
    let prefix = key(&format!("checkpoint-captures/{id}/{capture}"))?;
    match &archive {
        CellStorage::Local { .. } => {
            let bucket = archive.bucket(IoRegion::new()).await?;
            for item in bucket.list(Some(&prefix)).await? {
                bucket.delete(&item.key).await?;
            }
            if !bucket.list(Some(&prefix)).await?.is_empty() {
                return Err(unavailable("The checkpoint archive still has objects"));
            }
        }
        CellStorage::S3 { config } => {
            CellStorage::s3_inventory(config)
                .await?
                .purge(&prefix)
                .await?
        }
    }
    let settled = update_installation(directory, id, |current| {
        if pending(current) {
            return Err(inconsistent(
                "The closed capture admitted another archive region",
            ));
        }
        current.checkpoint_suffix_pins.retain(|_, pin| pin.capture != capture);
        let retirement = current
            .checkpoint_capture_retirements
            .get_mut(&capture)
            .filter(|value| value.authorized_at_ms == accepted.authorized_at_ms)
            .ok_or_else(|| inconsistent("The physical release lost its capture fence"))?;
        require_same_release(
            retirement,
            &accepted.archive,
            accepted.manifest_sha256.as_deref(),
        )?;
        retirement.key_withdrawn_at_ms.get_or_insert(withdrawn_at);
        retirement
            .observed_empty_at_ms
            .get_or_insert_with(|| now_ms().max(withdrawn_at));
        Ok(())
    })
    .await?;
    settled
        .checkpoint_capture_retirements
        .get(&capture)
        .cloned()
        .ok_or_else(|| inconsistent("The capture lost its physical release receipt"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::LocalKeyService;
    use rtdb_directory::MemoryDirectory;

    #[tokio::test]
    async fn discarding_an_uncertain_capture_fences_a_late_start() {
        let temp = tempfile::tempdir().unwrap();
        let archive = CellStorage::Local {
            root: temp.path().join("archive"),
        };
        let storage = CellStorage::Local {
            root: temp.path().join("serving"),
        };
        let keys = LocalKeyService::new(temp.path().join("keys")).unwrap();
        let directory = MemoryDirectory::new();
        let record = directory
            .create_deployment(DeploymentRecord::new("tenant", "org", Vec::new(), now_ms()))
            .await
            .unwrap();
        let capture = Uuid::new_v4();
        let first = retire_capture(
            directory.as_ref(),
            &record,
            &storage,
            &archive,
            &keys,
            capture,
            None,
        )
        .await
        .unwrap();
        let retained = directory.deployment(&record.id).await.unwrap().unwrap();
        assert_eq!(retained.lifecycle, Lifecycle::Live);
        assert!(require_capture_open(&retained, capture).is_err());
        assert!(require_capture_open(&retained, Uuid::new_v4()).is_ok());
        assert!(first.observed_empty_at_ms.is_some());
        let replay = retire_capture(
            directory.as_ref(),
            &retained,
            &storage,
            &archive,
            &keys,
            capture,
            None,
        )
        .await
        .unwrap();
        assert_eq!(first, replay);
    }
}
