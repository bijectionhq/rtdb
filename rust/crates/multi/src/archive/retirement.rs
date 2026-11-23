//! Independent physical retirement of one captured checkpoint.

use rtdb_directory::{
    CheckpointCaptureRetirement, CheckpointSuffixCustody, CheckpointSuffixPin, DeploymentRecord,
    Directory, Lifecycle, now_ms,
};
use rtdb_storage::{IoRegion, ObjectKey};
use rtdb_types::Result;
use uuid::Uuid;

use super::{archive_binding, archive_key_id, capture_point, inconsistent, key, require_archive_key_authority, unavailable};
use crate::{
    admin::update_installation, deployment::CellStorage, keys::{ArchiveKeyWithdrawal, KeyService},
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
    prepare_capture_retirement(directory, installation, archive, capture, manifest).await?;
    finish_capture_retirement(directory, storage, keys, &installation.id, capture).await
}

pub(crate) fn obsolete_fork_input(record: &DeploymentRecord, capture: Uuid) -> bool {
    record
        .checkpoint_captures
        .get(&capture)
        .is_some_and(|registered| {
            registered.automatic_fork && Some(registered.checkpoint) != record.checkpoint
        })
        && !record.checkpoint_capture_retirements.contains_key(&capture)
        && !record
            .storage_regions
            .values()
            .any(|region| region.holds_archive(capture))
        && !record
            .checkpoint_worlds
            .values()
            .any(|world| world.capture == capture && world.until.0 > now_ms().saturating_mul(1000))
        && !record.checkpoint_suffix_pins.values().any(|pin| {
            pin.capture == capture
                && (pin.expires_at_ms > now_ms()
                    || (pin.custody == CheckpointSuffixCustody::Capture
                        && pin.archive_sha256.is_some()))
        })
        && !record
            .checkpoint_recovery
            .as_ref()
            .is_some_and(|repair| repair.operation == capture)
}

pub(crate) fn obsolete_world_suffix(
    record: &DeploymentRecord,
    suffix: Uuid,
    pin: &CheckpointSuffixPin,
) -> bool {
    let now = now_ms();
    pin.custody != CheckpointSuffixCustody::Capture
        && pin.expires_at_ms <= now
        && !record.retains_world_suffix(suffix, pin, now)
        && !record
            .storage_regions
            .values()
            .any(|region| region.holds_checkpoint_suffix(pin.capture, suffix))
        && require_capture_open(record, pin.capture).is_ok()
}

pub(crate) async fn retire_world_suffix(
    directory: &dyn Directory,
    id: &str,
    suffix: Uuid,
) -> Result<()> {
    let fenced = update_installation(directory, id, |record| {
        let Some(pin) = record.checkpoint_suffix_pins.get(&suffix) else {
            return Ok(());
        };
        if pin.custody == CheckpointSuffixCustody::World
            && obsolete_world_suffix(record, suffix, pin)
        {
            let mut closed = pin.clone();
            closed.custody = CheckpointSuffixCustody::RetiringWorld;
            record.checkpoint_suffix_pins.insert(suffix, closed);
        }
        Ok(())
    })
    .await?;
    let Some(pin) = fenced.checkpoint_suffix_pins.get(&suffix).filter(|pin| {
        pin.custody == CheckpointSuffixCustody::RetiringWorld
            && obsolete_world_suffix(&fenced, suffix, pin)
    }) else {
        return Ok(());
    };
    let registered = fenced
        .checkpoint_captures
        .get(&pin.capture)
        .filter(|capture| {
            capture.incarnation == pin.incarnation
                && capture.log_epoch == pin.log_epoch
                && capture.checkpoint == pin.checkpoint
                && capture.manifest_sha256.as_ref() == Some(&pin.manifest_sha256)
        })
        .ok_or_else(|| inconsistent("The retiring world suffix lost its original archive"))?;
    let archive: CellStorage = serde_json::from_str(&registered.archive)
        .map_err(|_| inconsistent("Invalid retained world archive custody"))?;
    let deployment = Uuid::parse_str(id).map_err(|_| inconsistent("Invalid capture owner"))?;
    let prefix = key(&super::replica_suffix::archive_prefix(
        deployment, suffix, pin,
    ))?;
    purge_archive_prefix(&archive, &prefix).await?;
    update_installation(directory, id, |current| {
        if current.checkpoint_suffix_pins.get(&suffix) == Some(pin) {
            current.checkpoint_suffix_pins.remove(&suffix);
        }
        Ok(())
    })
    .await?;
    Ok(())
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
    prepare_capture_retirement_with_intent(
        directory, record, &archive, capture, registration.manifest_sha256.as_deref(), DiscardIntent::ObsoleteFork,
    ).await?;
    finish_capture_retirement(directory, storage, keys, &record.id, capture).await?;
    Ok(())
}

pub(crate) async fn prepare_capture_retirement(
    directory: &dyn Directory,
    installation: &DeploymentRecord,
    archive: &CellStorage,
    capture: Uuid,
    manifest: Option<&str>,
) -> Result<CheckpointCaptureRetirement> {
    prepare_capture_retirement_with_intent(directory, installation, archive, capture, manifest, DiscardIntent::Operator).await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DiscardIntent {
    Operator,
    ObsoleteFork,
    Scheduled(i64),
}

pub(crate) async fn schedule_capture_revocation(
    directory: &dyn Directory,
    installation: &DeploymentRecord,
    archive: &CellStorage,
    keys: &dyn KeyService,
    capture: Uuid,
    manifest: &str,
    due_at_ms: i64,
) -> Result<CheckpointCaptureRetirement> {
    let registration = installation.checkpoint_captures.get(&capture)
        .filter(|value| value.media_bound && value.key_namespace.is_some())
        .ok_or_else(|| inconsistent("The capture has no enrolled archive key"))?;
    if due_at_ms <= 0 || !matches!(archive, CellStorage::Local { .. }) {
        return Err(inconsistent("Scheduled revocation requires a positive time and a local archive"));
    }
    require_archive_key_authority(keys, registration).await?;
    let deployment = Uuid::parse_str(&installation.id).map_err(|_| inconsistent("Invalid capture owner"))?;
    let key = archive_key_id(&capture_point(installation, deployment, capture, &archive_binding(archive)?)?);
    if !keys.archive_key_is_operation_only(&key).await? {
        return Err(inconsistent("The archive key has no operation-only schedule authority"));
    }
    let accepted = prepare_capture_retirement_with_intent(
        directory, installation, archive, capture, Some(manifest), DiscardIntent::Scheduled(due_at_ms),
    ).await?;
    keys.schedule_archive_key(&key, due_at_ms).await?;
    Ok(accepted)
}

async fn prepare_capture_retirement_with_intent(
    directory: &dyn Directory,
    installation: &DeploymentRecord,
    archive: &CellStorage,
    capture: Uuid,
    manifest: Option<&str>,
    intent: DiscardIntent,
) -> Result<CheckpointCaptureRetirement> {
    let archive = archive_binding(archive)?;
    let accepted = update_installation(directory, &installation.id, |record| {
        let sampled_at = now_ms();
        if let Some(prior) = record.checkpoint_capture_retirements.get(&capture) {
            if let DiscardIntent::Scheduled(due_at_ms) = intent
                && prior.scheduled_revocation_at_ms != Some(due_at_ms)
            {
                return Err(inconsistent("The capture already has another discard intent"));
            }
            return require_same_release(prior, &archive, manifest);
        }
        if intent == DiscardIntent::ObsoleteFork && !obsolete_fork_input(record, capture) {
            return Err(unavailable("The automatic fork input remains retained"));
        }
        require_capture_open(record, capture)?;
        if record.checkpoint_suffix_pins.values().any(|pin| {
            pin.capture == capture && pin.expires_at_ms > sampled_at
        }) {
            return Err(inconsistent("A live suffix pin retains this checkpoint capture"));
        }
        if record.checkpoint_worlds.values().any(|world| {
            world.capture == capture && world.until.0 > sampled_at.saturating_mul(1000)
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
                authorized_at_ms: sampled_at,
                key_withdrawn_at_ms: None,
                operation_revoked_at_ms: None,
                observed_empty_at_ms: None,
                scheduled_revocation_at_ms: match intent {
                    DiscardIntent::Scheduled(due_at_ms) => Some(due_at_ms),
                    _ => None,
                },
            },
        );
        Ok(())
    })
    .await?;
    retirement_receipt(&accepted, &archive, capture, manifest)
}

pub(crate) fn capture_retirement_receipt(
    installation: &DeploymentRecord,
    archive: &CellStorage,
    capture: Uuid,
    manifest: Option<&str>,
) -> Result<CheckpointCaptureRetirement> {
    retirement_receipt(installation, &archive_binding(archive)?, capture, manifest)
}

fn retirement_receipt(
    installation: &DeploymentRecord,
    archive: &str,
    capture: Uuid,
    manifest: Option<&str>,
) -> Result<CheckpointCaptureRetirement> {
    let retained = installation.checkpoint_capture_retirements.get(&capture)
        .ok_or_else(|| inconsistent("The checkpoint capture has no discard preparation"))?;
    require_same_release(retained, archive, manifest)?;
    Ok(retained.clone())
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
    let accepted = withdraw_capture_key(directory, storage, keys, id, capture, false).await?;
    if accepted.observed_empty_at_ms.is_some() { return Ok(accepted); }
    let withdrawn_at = accepted.key_withdrawn_at_ms
        .ok_or_else(|| inconsistent("The archive key withdrawal was not recorded"))?;
    let archive: CellStorage = serde_json::from_str(&accepted.archive)
        .map_err(|_| inconsistent("Invalid retained capture custody"))?;
    let prefix = key(&format!("checkpoint-captures/{id}/{capture}"))?;
    purge_archive_prefix(&archive, &prefix).await?;
    let settled = update_installation(directory, id, |current| {
        if has_capture_region(current, capture) {
            return Err(inconsistent(
                "The closed capture admitted another archive region",
            ));
        }
        current
            .checkpoint_suffix_pins
            .retain(|_, pin| pin.capture != capture);
        let retirement = checked_retirement(current, capture, &accepted)?;
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

pub(crate) async fn commit_capture_revocation(
    directory: &dyn Directory,
    storage: &CellStorage,
    keys: &dyn KeyService,
    id: &str,
    capture: Uuid,
) -> Result<CheckpointCaptureRetirement> {
    withdraw_capture_key(directory, storage, keys, id, capture, true).await
}

fn has_capture_region(record: &DeploymentRecord, capture: Uuid) -> bool {
    record.storage_regions.values().any(|region| region.holds_archive(capture))
}

async fn withdraw_capture_key(
    directory: &dyn Directory,
    storage: &CellStorage,
    keys: &dyn KeyService,
    id: &str,
    capture: Uuid,
    require_operation_authority: bool,
) -> Result<CheckpointCaptureRetirement> {
    let load = || async {
        directory.deployment(id).await?
            .ok_or_else(|| unavailable("The original capture owner is unavailable"))
    };
    let mut record = load().await?;
    let accepted = record.checkpoint_capture_retirements.get(&capture)
        .ok_or_else(|| inconsistent("The checkpoint capture has no release authority"))?.clone();
    if accepted.key_withdrawn_at_ms.is_some() {
        if require_operation_authority && accepted.operation_revoked_at_ms.is_none() {
            return Err(inconsistent("The capture was withdrawn by an exporting archive vault"));
        }
        return Ok(accepted);
    }
    let registered = record.checkpoint_captures.get(&capture).filter(|value| value.media_bound).cloned();
    let key = if let Some(registration) = &registered {
        require_archive_key_authority(keys, registration).await?;
        let deployment = Uuid::parse_str(id).map_err(|_| inconsistent("Invalid capture owner"))?;
        Some(archive_key_id(&capture_point(&record, deployment, capture, &accepted.archive)?))
    } else {
        None
    };
    let enrolled = registered.as_ref().is_some_and(|value| value.key_namespace.is_some())
        && matches!(serde_json::from_str::<CellStorage>(&accepted.archive), Ok(CellStorage::Local { .. }));
    if require_operation_authority {
        let Some(key) = &key else {
            return Err(inconsistent("The capture has no enrolled archive key"));
        };
        if !enrolled || !keys.archive_key_is_operation_only(key).await?
        {
            return Err(inconsistent("The capture has no enrolled operation-only key authority"));
        }
    }
    if let Some(due_at_ms) = accepted.scheduled_revocation_at_ms {
        let key = key.as_deref().filter(|_| enrolled)
            .ok_or_else(|| inconsistent("The scheduled capture has no enrolled archive key"))?;
        if !keys.schedule_archive_key(key, due_at_ms).await? {
            return Err(unavailable("The archive authority is waiting for its scheduled revocation"));
        }
    }
    if has_capture_region(&record, capture) {
        reconcile_storage_regions(directory, storage, keys, &record).await?;
        record = load().await?;
        if has_capture_region(&record, capture) {
            return Err(unavailable("The capture still has admitted archive I/O"));
        }
        checked_retirement(&mut record, capture, &accepted)?;
        if record.checkpoint_captures.get(&capture).filter(|value| value.media_bound) != registered.as_ref() {
            return Err(inconsistent("The closed capture changed while draining its archive I/O"));
        }
    }
    let operation_only = match key {
        Some(key) => keys.withdraw_archive_key(&key).await? == ArchiveKeyWithdrawal::OperationAuthority,
        None => false,
    };
    if require_operation_authority && !operation_only {
        return Err(inconsistent("The authority returned an exporting-vault withdrawal"));
    }
    let withdrawal_observed_at = now_ms().max(accepted.authorized_at_ms)
        .max(accepted.scheduled_revocation_at_ms.unwrap_or(i64::MIN));
    let current = update_installation(directory, id, |current| {
        if has_capture_region(current, capture)
            || current.checkpoint_captures.get(&capture).filter(|value| value.media_bound) != registered.as_ref()
        {
            return Err(inconsistent("The closed capture changed before recording vault withdrawal"));
        }
        let retirement = checked_retirement(current, capture, &accepted)?;
        let withdrawn_at = *retirement.key_withdrawn_at_ms.get_or_insert(withdrawal_observed_at);
        if operation_only && enrolled {
            retirement.operation_revoked_at_ms.get_or_insert(withdrawn_at);
        }
        Ok(())
    }).await?;
    Ok(current.checkpoint_capture_retirements[&capture].clone())
}

fn checked_retirement<'a>(
    current: &'a mut DeploymentRecord,
    capture: Uuid,
    accepted: &CheckpointCaptureRetirement,
) -> Result<&'a mut CheckpointCaptureRetirement> {
    current.checkpoint_capture_retirements.get_mut(&capture)
        .filter(|value| {
            value.authorized_at_ms == accepted.authorized_at_ms
                && value.archive == accepted.archive
                && value.manifest_sha256 == accepted.manifest_sha256
                && value.scheduled_revocation_at_ms == accepted.scheduled_revocation_at_ms
        })
        .ok_or_else(|| inconsistent("The physical release lost its capture fence"))
}

async fn purge_archive_prefix(archive: &CellStorage, prefix: &ObjectKey) -> Result<()> {
    match archive {
        CellStorage::Local { .. } => {
            let bucket = archive.bucket(IoRegion::new()).await?;
            for item in bucket.list(Some(prefix)).await? {
                bucket.delete(&item.key).await?;
            }
            if !bucket.list(Some(prefix)).await?.is_empty() {
                return Err(unavailable("The checkpoint archive still has objects"));
            }
        }
        CellStorage::S3 { config } => {
            CellStorage::s3_inventory(config)
                .await?
                .purge(prefix)
                .await?
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::LocalKeyService;
    use bytes::Bytes;
    use rtdb_directory::{
        Checkpoint, CheckpointCapture, CheckpointWorldRetention, MemoryDirectory, StorageRegion,
    };
    use rtdb_types::{Timestamp, TransactionKey, TxId, WorldClockMode};

    #[tokio::test]
    async fn world_suffix_retirement_waits_for_world_restore_and_region() -> Result<()> {
        let temp = tempfile::tempdir().unwrap();
        let archive = CellStorage::Local {
            root: temp.path().join("archive"),
        };
        let directory = MemoryDirectory::new();
        let mut record = DeploymentRecord::new("world", "acme", Vec::new(), now_ms());
        let deployment = record.id.parse().unwrap();
        let capture = Uuid::new_v4();
        let suffix = Uuid::new_v4();
        let sibling = Uuid::new_v4();
        let world = Uuid::new_v4();
        let region = Uuid::new_v4();
        let basis = TransactionKey {
            tx_id: TxId(3),
            system_time: Timestamp(20),
        };
        let checkpoint = Checkpoint {
            block: 1,
            boundary: 4,
            source: 2,
            system_time: 10,
            root_sha256: Some([1; 32]),
        };
        let pin = CheckpointSuffixPin {
            capture,
            custody: CheckpointSuffixCustody::World,
            manifest_sha256: "manifest".into(),
            checkpoint,
            incarnation: record.incarnation,
            log_epoch: record.log_epoch,
            writer_epoch: record.epoch,
            basis,
            replica_through: 5,
            source_through: 3,
            journal_interval: [5, 5],
            journal_sha256: [2; 32],
            expires_at_ms: now_ms() - 1,
            archive_sha256: Some("archive".into()),
        };
        record.checkpoint_captures.insert(
            capture,
            CheckpointCapture {
                archive: archive_binding(&archive)?,
                key_namespace: None,
                incarnation: record.incarnation,
                log_epoch: record.log_epoch,
                checkpoint,
                automatic_fork: false,
                world_replay: None,
                media_bound: true,
                manifest_sha256: Some(pin.manifest_sha256.clone()),
                remote_disposal: None,
            },
        );
        record.checkpoint_suffix_pins.insert(suffix, pin.clone());
        record.checkpoint_suffix_pins.insert(sibling, pin.clone());
        record.checkpoint_worlds.insert(
            world,
            CheckpointWorldRetention {
                world,
                owner: "owner".into(),
                capture,
                manifest_sha256: pin.manifest_sha256.clone(),
                schema_digest: [0; 32],
                suffix: Some(suffix),
                basis,
                parent_publication: None,
                mode: WorldClockMode::Retained,
                until: Timestamp::MAX,
            },
        );
        let record = directory.create_deployment(record).await?;
        let region_for = |suffix| StorageRegion::Local {
            archive: Some(capture),
            checkpoint_suffix: Some(suffix),
            repair_archive: None,
            tracks_ordered_index_pages: true,
            checkpoint_metadata_from: None,
            checkpoint_replay_from: None,
        };
        let object = |suffix| {
            key(&format!(
                "{}/v2/manifest",
                super::super::replica_suffix::archive_prefix(deployment, suffix, &pin)
            ))
        };
        let store = archive.bucket(IoRegion::new()).await?;
        store
            .put(&object(suffix)?, Bytes::from_static(b"retiring"))
            .await?;
        store
            .put(&object(sibling)?, Bytes::from_static(b"retained"))
            .await?;

        retire_world_suffix(directory.as_ref(), &record.id, suffix).await?;
        assert_eq!(
            store.get_fresh(&object(suffix)?).await?.as_ref(),
            b"retiring"
        );

        update_installation(directory.as_ref(), &record.id, |current| {
            current.checkpoint_worlds.remove(&world);
            current.checkpoint_captures.get_mut(&capture).unwrap().world_replay = Some([3; 32]);
            Ok(())
        })
        .await?;
        retire_world_suffix(directory.as_ref(), &record.id, suffix).await?;
        assert_eq!(
            store.get_fresh(&object(suffix)?).await?.as_ref(),
            b"retiring"
        );

        update_installation(directory.as_ref(), &record.id, |current| {
            current.checkpoint_captures.get_mut(&capture).unwrap().world_replay = None;
            current.storage_regions.insert(region, region_for(suffix));
            Ok(())
        })
        .await?;
        retire_world_suffix(directory.as_ref(), &record.id, suffix).await?;
        assert_eq!(
            store.get_fresh(&object(suffix)?).await?.as_ref(),
            b"retiring"
        );

        update_installation(directory.as_ref(), &record.id, |current| {
            current.storage_regions.insert(region, region_for(sibling));
            Ok(())
        })
        .await?;
        retire_world_suffix(directory.as_ref(), &record.id, suffix).await?;
        retire_world_suffix(directory.as_ref(), &record.id, suffix).await?;

        let current = directory.deployment(&record.id).await?.unwrap();
        assert!(!current.checkpoint_suffix_pins.contains_key(&suffix));
        assert!(current.checkpoint_suffix_pins.contains_key(&sibling));
        assert!(current.checkpoint_captures.contains_key(&capture));
        assert!(
            store
                .list(Some(&key(&super::super::replica_suffix::archive_prefix(
                    deployment, suffix, &pin
                ))?))
                .await?
                .is_empty()
        );
        assert_eq!(
            store.get_fresh(&object(sibling)?).await?.as_ref(),
            b"retained"
        );
        Ok(())
    }

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
