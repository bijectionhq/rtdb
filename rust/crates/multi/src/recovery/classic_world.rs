use rtdb_directory::{
    CheckpointCapture, CheckpointStageIntent, CheckpointSuffixCustody, CheckpointSuffixPin,
    CheckpointWorldRetention, DeploymentRecord, Directory, Lifecycle, StorageNamespaces, Version,
    classic_flight, now_ms,
};
use rtdb_storage::cache::CacheBudget;
use rtdb_types::{Result, Timestamp, WorldClockMode};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    RecoveryService, Reply,
    classic_provider::{FlightFenceSelection, mismatch, observe_ordinary_flight_fence},
    valid_manifest,
};
use crate::archive::{self, CapturePoint, CheckpointReadResources, CheckpointReadWorld};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Selection {
    migration: FlightFenceSelection,
    begin_sha256: [u8; 32],
    original_catalog_sha256: [u8; 32],
    world: Uuid,
    pin: Uuid,
    retention_sha256: [u8; 32],
    suffix_archive_sha256: Option<String>,
}

#[derive(Serialize)]
pub(super) struct Evidence {
    name: String,
    intent: CheckpointStageIntent,
    begin_sha256: [u8; 32],
    original_catalog_sha256: [u8; 32],
    source_point: CapturePoint,
    world: Uuid,
    pin: Uuid,
    retention_sha256: [u8; 32],
    world_archive: CheckpointReadWorld,
    suffix_archive_sha256: Option<String>,
    until: Timestamp,
    directory_version: Version,
}

#[derive(PartialEq, Eq)]
struct Retention<'a> {
    namespaces: StorageNamespaces,
    source_capture: &'a CheckpointCapture,
    world: &'a CheckpointWorldRetention,
    world_capture: &'a CheckpointCapture,
    suffix: Option<&'a CheckpointSuffixPin>,
}

impl RecoveryService {
    pub(super) async fn reopen_classic_retained_world(
        &self,
        directory: &dyn Directory,
        original: DeploymentRecord,
        authority: Uuid,
        capture: Uuid,
        selection: Selection,
    ) -> Result<Reply> {
        let Some(hosted_task) = &self.hosted_task else {
            return Ok(Reply::Refused);
        };
        let Selection {
            migration,
            begin_sha256,
            original_catalog_sha256,
            world,
            pin,
            retention_sha256,
            suffix_archive_sha256,
        } = selection;
        let intent = migration.intent(authority, capture, &self.org)?;
        if world.is_nil()
            || pin.is_nil()
            || begin_sha256 == [0; 32]
            || original_catalog_sha256 == [0; 32]
            || suffix_archive_sha256
                .as_deref()
                .is_some_and(|digest| !valid_manifest(digest))
        {
            return Ok(Reply::Refused);
        }

        let admit = |record: &DeploymentRecord| {
            if record.name != original.name || record.lineage != original.lineage {
                return Err(mismatch());
            }
            select_attempt(record, &intent, begin_sha256, original_catalog_sha256)
        };
        admit(&original)?;
        let retained = select_retention(
            &original,
            &intent,
            world,
            pin,
            retention_sha256,
            suffix_archive_sha256.as_deref(),
        )?;
        let (source_point, _) =
            archive::capture_receipt(&original, &self.archive, authority, capture)?
                .ok_or_else(mismatch)?;
        let world_archive = CheckpointReadWorld {
            deployment: authority,
            capture: retained.world.capture,
            manifest_sha256: retained.world.manifest_sha256.clone(),
            suffix: retained.world.suffix,
            basis: retained.world.basis,
        };
        if self.keys.existing_namespace_id().await? != Some(retained.namespaces.keys) {
            return Ok(Reply::Refused);
        }
        require_unfenced_scratch(directory, &intent, &source_point).await?;

        let options = rtdb_node::NodeOptions {
            read_only: true,
            owned_relations: true,
            ..Default::default()
        };
        let cancellation = CancellationToken::new();
        let reopened = archive::reopen_checkpoint_world_archive(
            directory,
            &self.archive,
            self.keys.as_ref(),
            hosted_task,
            &world_archive,
            CheckpointReadResources {
                node: &options,
                memory: &self.memory,
                cache: CacheBudget::new(options.cache_bytes),
                cancellation: &cancellation,
            },
        )
        .await;

        let current = directory
            .deployment(&original.id)
            .await?
            .ok_or_else(mismatch)?;
        admit(&current)?;
        if retained
            != select_retention(
                &current,
                &intent,
                world,
                pin,
                retention_sha256,
                suffix_archive_sha256.as_deref(),
            )?
        {
            return Ok(Reply::Refused);
        }
        require_unfenced_scratch(directory, &intent, &source_point).await?;
        reopened?;
        Ok(Reply::ClassicMigrationRetainedWorldReopened {
            evidence: Evidence {
                name: current.name,
                intent,
                begin_sha256,
                original_catalog_sha256,
                source_point,
                world,
                pin,
                retention_sha256,
                world_archive,
                suffix_archive_sha256,
                until: retained.world.until,
                directory_version: current.version,
            },
        })
    }
}

fn select_attempt(
    source: &DeploymentRecord,
    intent: &CheckpointStageIntent,
    begin_sha256: [u8; 32],
    original_catalog_sha256: [u8; 32],
) -> Result<()> {
    if source.lifecycle != Lifecycle::Live
        || source.parent.is_some()
        || source.move_from.is_some()
        || source.cell_move.is_some()
        || source.checkpoint_transfer_terminal.is_some()
        || observe_ordinary_flight_fence(source, intent)?.is_some()
    {
        return Err(mismatch());
    }
    let commission = classic_flight::commission(source).ok_or_else(mismatch)?;
    if commission.intent != *intent
        || commission.begin_sha256 != begin_sha256
        || commission.original_catalog_sha256 != original_catalog_sha256
    {
        return Err(mismatch());
    }
    Ok(())
}

fn select_retention<'a>(
    source: &'a DeploymentRecord,
    intent: &CheckpointStageIntent,
    world: Uuid,
    pin: Uuid,
    retention_sha256: [u8; 32],
    suffix_archive_sha256: Option<&str>,
) -> Result<Retention<'a>> {
    let namespaces = source.storage_namespaces.ok_or_else(mismatch)?;
    let retained = source
        .checkpoint_worlds
        .get(&pin)
        .filter(|retained| {
            retained.world == world
                && retained.fork_reservation.is_none()
                && retained.until.0 > now_ms().saturating_mul(1_000)
                && retained.mode == WorldClockMode::Retained
                && retained.ancestors.is_empty()
                && !retained.capture.is_nil()
                && valid_manifest(&retained.manifest_sha256)
        })
        .ok_or_else(mismatch)?;
    if archive::sha256(&archive::encode(&(
        "rtdb.multi/classic-retained-world/v1",
        intent.source,
        pin,
        retained,
    ))?) != retention_sha256
    {
        return Err(mismatch());
    }
    let capture = |operation, manifest: &str| {
        archive::require_capture_open(source, operation)?;
        source
            .checkpoint_captures
            .get(&operation)
            .filter(|capture| {
                capture.incarnation == source.incarnation
                    && capture.log_epoch == source.log_epoch
                    && capture.media_bound
                    && capture.remote_disposal.is_none()
                    && capture.key_namespace == Some(namespaces.keys)
                    && capture.checkpoint.root_sha256.is_some()
                    && capture.manifest_sha256.as_deref() == Some(manifest)
            })
            .ok_or_else(mismatch)
    };
    let source_capture = capture(intent.capture, &intent.source_manifest_sha256)?;
    if source.checkpoint != Some(source_capture.checkpoint) {
        return Err(mismatch());
    }
    let world_capture = capture(retained.capture, &retained.manifest_sha256)?;
    let suffix = match (retained.suffix, suffix_archive_sha256) {
        (None, None) => None,
        (Some(suffix), Some(archive)) => Some(
            source
                .checkpoint_suffix_pins
                .get(&suffix)
                .filter(|suffix| {
                    suffix.custody == CheckpointSuffixCustody::World
                        && suffix.archive_sha256.as_deref() == Some(archive)
                })
                .ok_or_else(mismatch)?,
        ),
        _ => return Err(mismatch()),
    };
    Ok(Retention {
        namespaces,
        source_capture,
        world: retained,
        world_capture,
        suffix,
    })
}

async fn require_unfenced_scratch(
    directory: &dyn Directory,
    intent: &CheckpointStageIntent,
    source: &CapturePoint,
) -> Result<()> {
    let scratch_id = intent.destination.to_string();
    if directory
        .deployment(&scratch_id)
        .await?
        .is_some_and(|scratch| {
            scratch.lifecycle != Lifecycle::Restoring
                || scratch.name != intent.name
                || scratch.org != intent.org
                || scratch.id != scratch_id
                || scratch.lineage != scratch_id
                || scratch.incarnation != 1
                || scratch.parent.is_some()
                || scratch.holder.is_some()
                || scratch.checkpoint_transfer_fence.is_some()
                || scratch.checkpoint_transfer_terminal.is_some()
                || scratch.checkpoint_transfer_bootstrap.is_some()
                || scratch.checkpoint_stage.as_deref().is_none_or(|stage| {
                    stage.intent != *intent
                        || !stage.transfer_only
                        || stage.checkpoint != source.checkpoint
                        || stage.source_incarnation != source.incarnation
                        || stage.source_log_epoch != source.log_epoch
                })
        })
    {
        return Err(mismatch());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use rtdb_directory::{
        Checkpoint, ClassicMigration, ClassicMigrationSource, MemberId, MemoryDirectory,
    };
    use rtdb_types::{TransactionKey, TxId};

    use super::*;

    #[tokio::test]
    async fn retained_world_lookup_requires_the_commissioned_attempt_and_exact_active_pin()
    -> Result<()> {
        let directory = MemoryDirectory::new();
        let mut source = DeploymentRecord::new("original", "org", vec![], now_ms());
        let authority = source.id.parse().unwrap();
        let attempt = Uuid::new_v4();
        let capture = Uuid::new_v4();
        let manifest = URL_SAFE_NO_PAD.encode([1; 32]);
        source.holder = Some(MemberId::fresh());
        source.epoch = 2;
        source.storage_namespaces = Some(StorageNamespaces {
            objects: Uuid::new_v4(),
            keys: [9; 32],
        });
        let checkpoint = Checkpoint {
            block: 2,
            source: 6,
            boundary: 8,
            system_time: 10,
            root_sha256: Some([2; 32]),
        };
        source.checkpoint = Some(checkpoint);
        source.checkpoint_captures.insert(
            capture,
            CheckpointCapture {
                archive: "archive".into(),
                key_namespace: Some([9; 32]),
                incarnation: source.incarnation,
                log_epoch: source.log_epoch,
                checkpoint,
                automatic_fork: false,
                world_replay: None,
                media_bound: true,
                manifest_sha256: Some(manifest.clone()),
                remote_disposal: None,
            },
        );
        let intent = CheckpointStageIntent {
            operation: attempt,
            source: authority,
            capture,
            source_manifest_sha256: manifest.clone(),
            destination: attempt,
            name: format!("checkpoint-stage-{attempt}"),
            org: source.org.clone(),
            classic_migration: Some(ClassicMigration {
                source: ClassicMigrationSource {
                    incarnation: source.incarnation,
                    log_epoch: source.log_epoch,
                    writer_epoch: source.epoch,
                },
                destination: Uuid::new_v4(),
            }),
        };
        let pin = Uuid::new_v4();
        let world = Uuid::new_v4();
        let world_retention = CheckpointWorldRetention {
            fork_reservation: None,
            world,
            ancestors: vec![],
            owner: "owner".into(),
            capture,
            manifest_sha256: manifest,
            schema_digest: [3; 32],
            suffix: None,
            basis: TransactionKey {
                tx_id: TxId(6),
                system_time: Timestamp(10),
            },
            parent_publication: Some(Timestamp(10)),
            mode: WorldClockMode::Retained,
            until: Timestamp(i64::MAX),
        };
        let digest = archive::sha256(&archive::encode(&(
            "rtdb.multi/classic-retained-world/v1",
            authority,
            pin,
            &world_retention,
        ))?);
        source.checkpoint_worlds.insert(pin, world_retention);
        let source = directory.create_deployment(source).await?;
        let commission = classic_flight::Commission {
            intent,
            coverage_sha256: [4; 32],
            begin_sha256: [5; 32],
            original_catalog_sha256: [6; 32],
            retained_path_sha256: [7; 32],
            prior_generations: vec![],
        };
        let intent = &commission.intent;
        assert!(select_attempt(&source, intent, [5; 32], [6; 32]).is_err());
        classic_flight::install(directory.as_ref(), &commission).await?;

        let mut current = directory.deployment(&source.id).await?.unwrap();
        assert!(select_attempt(&current, intent, [5; 32], [6; 32]).is_ok());
        assert!(select_attempt(&current, intent, [8; 32], [6; 32]).is_err());
        assert!(select_retention(&current, intent, world, pin, digest, None).is_ok());
        assert!(select_retention(&current, intent, world, Uuid::new_v4(), digest, None).is_err());
        current
            .checkpoint_worlds
            .get_mut(&pin)
            .unwrap()
            .schema_digest = [8; 32];
        assert!(select_retention(&current, intent, world, pin, digest, None).is_err());
        current.epoch += 1;
        assert!(select_attempt(&current, intent, [5; 32], [6; 32]).is_err());
        Ok(())
    }
}
