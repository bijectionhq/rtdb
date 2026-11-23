//! Independent physical retirement of one captured checkpoint.

use rtdb_directory::{
    CheckpointCaptureRetirement, CheckpointSuffixCustody, CheckpointSuffixPin, DeploymentRecord,
    Directory, Lifecycle, now_ms,
};
use rtdb_storage::{IoRegion, ObjectKey};
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

fn serving_retains_capture(record: &DeploymentRecord, capture: Uuid) -> bool {
    let selected = record.serving_replay_capture.map(|source| source.as_u128());
    let repair = record.checkpoint_recovery.as_ref().map(|repair|
        (repair.operation.as_u128(), repair.completed_at_ms.is_none()));
    super::replay_handoff::retains_capture(selected, repair, capture.as_u128())
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
            .any(|world| world.retains_capture(capture, rtdb_types::Timestamp(now_ms().saturating_mul(1000))))
        && !record.checkpoint_suffix_pins.values().any(|pin| {
            pin.capture == capture
                && (pin.expires_at_ms > now_ms()
                    || (pin.custody == CheckpointSuffixCustody::Capture
                        && pin.archive_sha256.is_some()))
        })
        && !serving_retains_capture(record, capture)
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
                world.retains_capture(capture, rtdb_types::Timestamp(now_ms().saturating_mul(1000)))
            }) {
                return Err(inconsistent("A live world retains this checkpoint capture"));
            }

            if serving_retains_capture(record, capture)
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
    purge_archive_prefix(&archive, &prefix).await?;
    let settled = update_installation(directory, id, |current| {
        if pending(current) {
            return Err(inconsistent(
                "The closed capture admitted another archive region",
            ));
        }
        current
            .checkpoint_suffix_pins
            .retain(|_, pin| pin.capture != capture);
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
                ancestor: None,
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
    async fn a_selected_serving_archive_cannot_be_discarded_while_parked() -> Result<()> {
        let files = tempfile::tempdir().unwrap();
        let archive = CellStorage::Local { root: files.path().join("archive") };
        let storage = CellStorage::Local { root: files.path().join("serving") };
        let keys = LocalKeyService::new(files.path().join("keys"))?;
        let directory = MemoryDirectory::new();
        let capture = Uuid::new_v4();
        let mut record = DeploymentRecord::new("tenant", "org", Vec::new(), now_ms());
        record.serving_replay_capture = Some(capture);
        let record = directory.create_deployment(record).await?;

        let error = retire_capture(directory.as_ref(), &record, &storage, &archive, &keys, capture, None)
            .await.unwrap_err();
        assert_eq!(error.message.as_deref(), Some("The serving history still depends on this capture"));
        let retained = directory.deployment(&record.id).await?.unwrap();
        assert_eq!(retained.serving_replay_capture, Some(capture));
        assert!(require_capture_open(&retained, capture).is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn a_superseded_repair_archive_remains_until_its_readers_drain() -> Result<()> {
        let files = tempfile::tempdir().unwrap();
        let archive = CellStorage::Local { root: files.path().join("archive") };
        let storage = CellStorage::Local { root: files.path().join("serving") };
        let keys = LocalKeyService::new(files.path().join("keys"))?;
        let directory = MemoryDirectory::new();
        let capture = Uuid::new_v4();
        let region = Uuid::new_v4();
        let witness = keys.hold_local_storage_region(region)?;
        let mut record = DeploymentRecord::new("tenant", "org", Vec::new(), now_ms());
        let archive_binding = archive_binding(&archive)?;
        let checkpoint = rtdb_directory::Checkpoint {
            block: 0, boundary: 0, source: 0, system_time: 0, root_sha256: Some([1; 32]),
        };
        record.checkpoint_captures.insert(capture, rtdb_directory::CheckpointCapture {
            archive: archive_binding.clone(), incarnation: record.incarnation, log_epoch: record.log_epoch, checkpoint,
            automatic_fork: false, world_replay: None, media_bound: true, manifest_sha256: Some("original".into()),
            remote_disposal: None,
        });
        let receipt = rtdb_directory::CheckpointRecovery {
            operation: capture, checkpoint, epoch: 0, previous_lifecycle: Lifecycle::Live,
            archive: archive_binding.clone(), manifest_sha256: "original".into(), completed_at_ms: Some(now_ms()),
        };
        record.checkpoint_recovery = Some(Box::new(receipt.clone()));
        record.checkpoint_repair_receipts.insert(capture, receipt.clone());
        let record = directory.create_deployment(record).await?;
        let deployment = Uuid::parse_str(&record.id).unwrap();
        let point = capture_point(&record, deployment, capture, &archive_binding)?;
        let archive_key = archive_key_id(&point);
        let material = keys.key(&archive_key).await?;
        let media = archive.bucket(IoRegion::new()).await?;
        let original = key(&format!("checkpoint-captures/{}/{capture}/capture.json", record.id))?;
        media.put(&original, bytes::Bytes::from_static(b"original")).await?;
        let error = retire_capture(directory.as_ref(), &record, &storage, &archive, &keys, capture, None)
            .await.unwrap_err();
        assert_eq!(error.message.as_deref(), Some("The serving history still depends on this capture"));

        let selected = Uuid::new_v4();
        let published = update_installation(directory.as_ref(), &record.id, |current| {
            current.serving_replay_capture = Some(selected);
            current.storage_regions.insert(region, rtdb_directory::StorageRegion::Local {
                archive: None, checkpoint_suffix: None, repair_archive: Some(capture),
                tracks_ordered_index_pages: true, checkpoint_metadata_from: None, checkpoint_replay_from: None,
            });
            Ok(())
        }).await?;
        let error = retire_capture(directory.as_ref(), &published, &storage, &archive, &keys, capture, None)
            .await.unwrap_err();
        assert_eq!(error.message.as_deref(), Some("The capture still has admitted archive I/O"));
        assert_eq!(media.get(&original).await?.as_ref(), b"original");
        assert_eq!(keys.existing_key(&archive_key).await?, Some(material));

        drop(witness);
        let retired = finish_capture_retirement(directory.as_ref(), &storage, &keys, &record.id, capture).await?;
        assert!(retired.observed_empty_at_ms.is_some());
        assert!(matches!(media.get(&original).await, Err(rtdb_storage::Error::MissingObject(_))));
        assert_eq!(keys.existing_key(&archive_key).await.unwrap_err().code, "rtdb.multi/key-destroyed");
        let current = directory.deployment(&record.id).await?.unwrap();
        assert!(current.storage_regions.is_empty());
        assert_eq!(current.serving_replay_capture, Some(selected));
        assert_eq!(current.checkpoint_recovery.unwrap().operation, capture);
        let retried = super::super::restore(
            directory.as_ref(), &storage, &archive, &keys, deployment, capture, 4 << 20, None,
        ).await?;
        assert_eq!(retried, receipt);
        Ok(())
    }

    #[tokio::test]
    async fn pending_restore_keeps_its_own_capture_alongside_serving_replay() -> Result<()> {
        let files = tempfile::tempdir().unwrap();
        let archive = CellStorage::Local { root: files.path().join("archive") };
        let storage = CellStorage::Local { root: files.path().join("serving") };
        let keys = LocalKeyService::new(files.path().join("keys"))?;
        let directory = MemoryDirectory::new();
        let capture = Uuid::new_v4();
        let mut record = DeploymentRecord::new("tenant", "org", Vec::new(), now_ms());
        record.serving_replay_capture = Some(Uuid::new_v4());
        record.lifecycle = Lifecycle::Restoring;
        record.checkpoint_recovery = Some(Box::new(rtdb_directory::CheckpointRecovery {
            operation: capture, checkpoint: rtdb_directory::Checkpoint { block: 0, boundary: 0, source: 0,
                system_time: 0, root_sha256: Some([1; 32]) }, epoch: 0, previous_lifecycle: Lifecycle::Live,
            archive: archive_binding(&archive)?, manifest_sha256: "pending".into(), completed_at_ms: None,
        }));
        let record = directory.create_deployment(record).await?;
        let error = retire_capture(directory.as_ref(), &record, &storage, &archive, &keys, capture, None)
            .await.unwrap_err();
        assert_eq!(error.message.as_deref(), Some("The serving history still depends on this capture"));
        let current = directory.deployment(&record.id).await?.unwrap();
        assert!(require_capture_open(&current, capture).is_ok());
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
