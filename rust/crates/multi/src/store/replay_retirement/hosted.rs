use rtdb_directory::{
    Checkpoint, DeploymentRecord, Directory, ExactRetirementGrant, HostedTask, LineageChange, LineageRetirement, LineageState,
    RetirementWork, StorageRegion, now_ms,
};
use rtdb_format::keys::block_key;
use rtdb_s3::{NeverVersionedRetirement, VersionedInventory};
use rtdb_storage::{Error, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, PhysicalObjectVersion, Result, S3TombstoneTarget};
use uuid::Uuid;

use super::{ObservedPack, PackStorage, covered_pack, eligible_checkpoint};
use crate::archive;
use rtdb_storage::{IoRegion, sealing::PublicationSealing};
use crate::deployment::CellStorage;
use crate::keys::KeyService;
use crate::packing::{PACK_DIRECTORY, block_pack, pack_block};
use crate::reference::{Base, References, key};
use crate::store::checkpoint_metadata_block;
use crate::store::exact_retirement::{settle, tombstones::{TombstoneJournal, retire}};

#[derive(Clone, Copy)]
enum Publication<'a> {
    Version { version: &'a PhysicalObjectVersion, target: &'a S3TombstoneTarget },
    Tombstone { allocation: Uuid, target: &'a S3TombstoneTarget },
}

struct ReplayRetirement<'a> {
    deployment: &'a str,
    target: &'a str,
    capture: Uuid,
    checkpoint: Checkpoint,
    publication: Publication<'a>,
}

impl<'a> ReplayRetirement<'a> {
    fn from_work(work: &'a RetirementWork) -> Option<Self> {
        match work {
            RetirementWork::ReplayPack { deployment, target, version, tombstone, capture, checkpoint } =>
                Some(Self { deployment, target, capture: *capture, checkpoint: *checkpoint, publication: Publication::Version { version, target: tombstone } }),
            RetirementWork::ReplayPackTombstone { deployment, target, tombstone, allocation, capture, checkpoint } =>
                Some(Self { deployment, target, capture: *capture, checkpoint: *checkpoint,
                    publication: Publication::Tombstone { allocation: *allocation, target: tombstone } }),
            _ => None,
        }
    }
}

fn eligible(record: &DeploymentRecord) -> bool {
    eligible_checkpoint(record)
        && record.checkpoint.zip(record.serving_replay_capture).is_some_and(|(checkpoint, capture)| {
            record.storage_regions.values().all(|region| region.serves_from_replay_archive(capture, checkpoint))
                && record.catalog_readers.values().all(|reader| record.storage_regions.contains_key(&reader.owner))
        })
}

fn parked_writer_drained(record: &DeploymentRecord) -> bool {
    record.holder.is_none() && record.active_s3_write_lease(now_ms()).is_none()
}

pub(in crate::store) async fn reap_hosted(
    directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, task: &HostedTask,
    bucket: &dyn ObjectStore, provider: &VersionedInventory, candidate: &DeploymentRecord,
) -> Result<()> {
    let lineage = &candidate.lineage;
    let mut state = crate::store::recover_hosted_lineage(directory, lineage).await?;
    if let Some(retirement) = state.retirement() {
        if ReplayRetirement::from_work(&retirement.work).is_some_and(|work| work.deployment == candidate.id) {
            finish(directory, storage, keys, task, bucket, provider, lineage, &state, retirement).await?;
        }
        return Ok(());
    }
    if !eligible(candidate)
        || state.open_generation().is_none() || state.preparations().next().is_some()
    {
        return Ok(());
    }
    let (Some(checkpoint), Some(capture)) = (candidate.checkpoint, candidate.serving_replay_capture) else { return Ok(()); };
    let source = candidate.storage_prefix();
    let store = format!("{source}/{}", archive::checkpoint_storage_root()?);
    let prefix = format!("{store}/");
    if !Base::load(bucket, &store).await?.objects.is_empty() { return Ok(()); }
    let terminal = provider.enrolled_terminal_retirement().await?;
    let deployments = directory.deployments().await?;
    let references = References::scan(bucket, &deployments).await?;
    for pack in bucket.list(Some(&key(&format!("{prefix}{PACK_DIRECTORY}"))?)).await? {
        let Some(block) = pack.key.as_str().strip_prefix(&prefix).and_then(pack_block) else { continue; };
        if block >= checkpoint.block { continue; }
        let Some(observed) = observe_pack(bucket, terminal.as_ref(), &pack.key).await? else { continue; };
        if observed.index.entries.keys().all(|name| checkpoint_metadata_block(name) == Some(block)) {
            continue;
        }
        let work = match observed.storage {
            PackStorage::Raw { version: Some(version), .. } => {
                if references.has_declared_version(&pack.key, &version) { continue; }
                let Some(tombstone) = provider.versioned_retirement_target(&pack.key, &version).await? else { continue; };
                RetirementWork::ReplayPack {
                    deployment: candidate.id.clone(), target: pack.key.to_string(), version, tombstone, capture, checkpoint,
                }
            }
            PackStorage::Terminal { publication, .. } if !references.has_declared_reference(&pack.key) => RetirementWork::ReplayPackTombstone {
                deployment: candidate.id.clone(), target: pack.key.to_string(),
                tombstone: publication.target().clone(), allocation: publication.allocation(), capture, checkpoint,
            },
            _ => continue,
        };
        let retirement = LineageRetirement {
            token: Uuid::new_v4(),
            source: source.clone(),
            work,
        };
        let reserved = directory.change_lineage(lineage, LineageChange::ReserveRetirement {
            retirement: retirement.clone(), observed_references: state.reference_revision(), generation: state.retirement_generation(),
        }).await?;
        if finish(directory, storage, keys, task, bucket, provider, lineage, &reserved, &retirement).await? {
            return Ok(());
        }
        state = directory.lineage_state(lineage).await?;
        if state.open_generation().is_none() || state.preparations().next().is_some() { return Ok(()); }
    }
    Ok(())
}

async fn finish(
    directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, task: &HostedTask,
    bucket: &dyn ObjectStore, provider: &VersionedInventory, lineage: &str, state: &LineageState,
    retirement: &LineageRetirement,
) -> Result<bool> {
    let ReplayRetirement { deployment, .. } = ReplayRetirement::from_work(&retirement.work).ok_or_else(changed)?;
    let authorized;
    let grant = if let Some(grant) = state.exact_retirement_grant(retirement) {
        grant
    } else {
        let prepared = authorize(directory, storage, keys, task, bucket, provider, lineage, retirement).await;
        authorized = directory.lineage_state(lineage).await?;
        if authorized.retirement() != Some(retirement) { return prepared.map(|()| false); }
        let Some(grant) = authorized.exact_retirement_grant(retirement) else {
            directory.change_lineage(lineage, LineageChange::AbandonExactRetirement { retirement: retirement.clone() }).await?;
            crate::store::release_region(directory, deployment, retirement.token, false).await?;
            return prepared.map(|()| false);
        };
        grant
    };
    erase_granted(directory, provider, lineage, grant).await?;
    settle(directory, lineage, deployment, retirement).await?;
    Ok(true)
}

async fn erase_granted(directory: &dyn Directory, provider: &VersionedInventory, lineage: &str, grant: ExactRetirementGrant<'_>) -> Result<()> {
    let retirement = grant.retirement();
    let ReplayRetirement { target, publication, .. } = ReplayRetirement::from_work(&retirement.work).ok_or_else(changed)?;
    let target = key(target)?;
    let observed = || serde_json::json!({
        "lineage": lineage, "revision": grant.revision(), "residence": retirement.source, "retirement": retirement.token,
        "key": target.as_str(), "publication": retirement.work,
    });
    crate::residence_trace::record("authorize-replay-pack", observed);
    match publication {
        Publication::Version { version, target: tombstone } => {
            provider.retire_version_behind_tombstone(&target, version, tombstone, retirement.token).await?;
        }
        Publication::Tombstone { allocation, target: tombstone } => {
            let proof = provider.enrolled_terminal_retirement().await?
                .filter(|proof| proof.allocation() == Some(allocation)).ok_or_else(changed)?;
            let journal = TombstoneJournal::new(directory, lineage, retirement.token);
            retire(&proof, &journal, &target, tombstone).await?;
        }
    }
    crate::residence_trace::record("remove-replay-pack", observed);
    Ok(())
}

async fn authorize(
    directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, task: &HostedTask,
    bucket: &dyn ObjectStore, provider: &VersionedInventory, lineage: &str,
    retirement: &LineageRetirement,
) -> Result<()> {
    let ReplayRetirement { deployment, target, publication, capture, checkpoint } =
        ReplayRetirement::from_work(&retirement.work).ok_or_else(changed)?;
    let target = key(target)?;
    let store = format!("{}/{}", retirement.source, archive::checkpoint_storage_root()?);
    let Some(block) = target.as_str().strip_prefix(&format!("{store}/")).and_then(pack_block)
        else { return Err(changed()); };
    let Some(record) = directory.deployment(deployment).await? else { return Ok(()); };
    if !eligible(&record) || record.lineage != lineage || record.storage_prefix() != retirement.source
        || record.checkpoint != Some(checkpoint) || record.serving_replay_capture != Some(capture)
        || block >= checkpoint.block || !Base::load(bucket, &store).await?.objects.is_empty()
    {
        return Ok(());
    }
    let Some(anchor) = record.replica_anchor.as_deref()
        else { return Ok(()); };
    if record.required_replica_offsets(now_ms()).any(|first| first <= anchor.interval[1]) {
        return Ok(());
    }
    let references = References::scan(bucket, &directory.deployments().await?).await?;
    let retained = match publication {
        Publication::Version { version, .. } => references.has_declared_version(&target, version),
        Publication::Tombstone { .. } => references.has_declared_reference(&target),
    };
    if retained { return Ok(()); }
    let cipher = crate::store::directory_publications(keys, &record, IoRegion::new())?;
    let terminal = match publication {
        Publication::Version { .. } => None,
        Publication::Tombstone { allocation, .. } => {
            let Some(terminal) = provider.enrolled_terminal_retirement().await?
                .filter(|terminal| terminal.allocation() == Some(allocation))
                else { return Ok(()); };
            Some(terminal)
        }
    };
    let allow_compacted_receipt = parked_writer_drained(&record) && record.storage_regions.is_empty();
    let terminal = terminal.as_ref();
    let target = &target;
    let store = &store;
    archive::with_replay_archive(directory, storage, keys, &record, Some(task), |admitted, owner, archive| async move {
        let Some(observed) = observe_pack(bucket, terminal, target).await? else { return Ok(()); };
        let same = match (&publication, &observed.storage) {
            (Publication::Version { version: expected, .. }, PackStorage::Raw { version, .. }) => version.as_ref() == Some(*expected),
            (Publication::Tombstone { target, allocation }, PackStorage::Terminal { publication, .. }) =>
                publication.allocation() == *allocation && publication.target() == *target,
            _ => false,
        };
        if !same { return Ok(()); }
        let Some(boundary) = covered_pack(&observed, block, record.log_epoch, &cipher, &archive, allow_compacted_receipt).await?
            else { return Ok(()); };
        if !crate::checkpoint_retention::replay_archive_covers_pack(block, checkpoint.block, boundary,
            anchor.interval[0], anchor.interval[1], checkpoint.boundary)
            || !successor_present(bucket, terminal, store, checkpoint, &cipher).await?
        {
            return Ok(());
        }
        if let Publication::Version { version, target: tombstone } = publication
            && provider.versioned_retirement_target(target, version).await?.as_ref() != Some(tombstone)
        {
            return Ok(());
        }
        let fenced = crate::admin::update_installation(directory, deployment, |latest| {
            if latest.version != admitted.version
                || !latest.storage_regions.get(&owner).is_some_and(|region| region.admits_archive_read(capture))
            {
                return Err(changed());
            }
            latest.storage_regions.insert(retirement.token, StorageRegion::ReplayPackRetirement {
                object: target.to_string(), capture,
            });
            Ok(())
        }).await?;
        directory.authorize_owned_retirement(deployment, fenced.version, retirement.clone()).await?;
        Ok(())
    }).await?;
    Ok(())
}

async fn observe_pack<'a>(bucket: &'a dyn ObjectStore, terminal: Option<&'a NeverVersionedRetirement<'a>>, key: &'a ObjectKey)
    -> Result<Option<ObservedPack<'a>>> {
    if let Some(terminal) = terminal { return ObservedPack::terminal(terminal, key).await; }
    match ObservedPack::raw(bucket, key).await {
        Ok(pack @ ObservedPack { storage: PackStorage::Raw { version: Some(_), .. }, .. }) => Ok(Some(pack)),
        Ok(_) | Err(Error::MissingObject(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn successor_present(
    bucket: &dyn ObjectStore, terminal: Option<&NeverVersionedRetirement<'_>>,
    store: &str, checkpoint: Checkpoint, cipher: &PublicationSealing,
) -> Result<bool> {
    let root = block_key(BlockIndex(checkpoint.block))?;
    let pack = key(&format!("{store}/{}", block_pack(&root)))?;
    let Some(observed) = observe_pack(bucket, terminal, &pack).await? else { return Ok(false); };
    observed.checkpoint_matches(checkpoint, cipher).await
}

fn changed() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/replay-pack-retirement", "The replay pack retirement evidence changed")
}
