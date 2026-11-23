use rtdb_directory::{
    Checkpoint, DeploymentRecord, Directory, HostedTask, LineageChange, LineageRetirement, LineageState,
    RetirementWork, StorageRegion, now_ms,
};
use rtdb_format::{catalog::BlockCatalog, keys::block_key};
use rtdb_storage::{Error, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, Result};
use uuid::Uuid;

use super::{MAXIMUM_MEMBER_BYTES, covered_pack, eligible, is_block_l0, read_member};
use crate::archive;
use crate::cipher::Cipher;
use crate::deployment::CellStorage;
use crate::keys::KeyService;
use crate::packing::{PACK_DIRECTORY, block_pack, observe_pack_index, pack_block};
use crate::reference::{Base, References, key};
use crate::store::exact_retirement::{RetirementProvider, settle};

pub(in crate::store) async fn reap_hosted(
    directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, task: &HostedTask,
    bucket: &dyn ObjectStore, provider: &dyn RetirementProvider, candidate: &DeploymentRecord, threshold: i64,
) -> Result<()> {
    let lineage = &candidate.lineage;
    let mut state = crate::store::recover_hosted_lineage(directory, lineage).await?;
    if let Some(retirement) = state.retirement() {
        if matches!(&retirement.work, RetirementWork::ReplayPack { deployment, .. } if *deployment == candidate.id) {
            finish(directory, storage, keys, task, bucket, provider, lineage, &state, retirement, threshold).await?;
        }
        return Ok(());
    }
    if !eligible(candidate) || candidate.active_s3_write_lease(now_ms()).is_some()
        || state.open_generation().is_none() || state.preparations().next().is_some()
    {
        return Ok(());
    }
    let (Some(checkpoint), Some(capture)) = (candidate.checkpoint, candidate.serving_replay_capture) else { return Ok(()); };
    let source = candidate.storage_prefix();
    let store = format!("{source}/{}", archive::checkpoint_storage_root()?);
    let prefix = format!("{store}/");
    if !Base::load(bucket, &store).await?.objects.is_empty() { return Ok(()); }
    let deployments = directory.deployments().await?;
    let references = References::scan(bucket, &deployments).await?;
    for pack in bucket.list(Some(&key(&format!("{prefix}{PACK_DIRECTORY}"))?)).await? {
        let Some(block) = pack.key.as_str().strip_prefix(&prefix).and_then(pack_block) else { continue; };
        if block >= checkpoint.block || references.has_declared_reference(&pack.key) { continue; }
        let index = match observe_pack_index(bucket, &pack.key).await {
            Ok(index) => index,
            Err(Error::MissingObject(_)) => continue,
            Err(error) => return Err(error.into()),
        };
        if index.value.written_at_ms.is_none_or(|written| written > threshold)
            || !index.value.entries.keys().any(|name| is_block_l0(name, block))
        {
            continue;
        }
        let Some(version) = index.version else { continue; };
        if !provider.available(&pack.key, &version).await? { continue; }
        let retirement = LineageRetirement {
            token: Uuid::new_v4(),
            source: source.clone(),
            work: RetirementWork::ReplayPack {
                deployment: candidate.id.clone(), target: pack.key.to_string(), version, capture, checkpoint,
            },
        };
        let reserved = directory.change_lineage(lineage, LineageChange::ReserveRetirement {
            retirement: retirement.clone(), observed_references: state.reference_revision(), generation: state.retirement_generation(),
        }).await?;
        if finish(directory, storage, keys, task, bucket, provider, lineage, &reserved, &retirement, threshold).await? {
            return Ok(());
        }
        state = directory.lineage_state(lineage).await?;
        if state.open_generation().is_none() || state.preparations().next().is_some() { return Ok(()); }
    }
    Ok(())
}

async fn finish(
    directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, task: &HostedTask,
    bucket: &dyn ObjectStore, provider: &dyn RetirementProvider, lineage: &str, state: &LineageState,
    retirement: &LineageRetirement, threshold: i64,
) -> Result<bool> {
    let RetirementWork::ReplayPack { deployment, target, version, .. } = &retirement.work else { return Err(changed()); };
    let target = key(target)?;
    if !state.exact_erasure_authorized() {
        let prepared = authorize(directory, storage, keys, task, bucket, provider, lineage, retirement, threshold).await;
        let state = directory.lineage_state(lineage).await?;
        if state.retirement() != Some(retirement) { return prepared.map(|()| false); }
        if !state.exact_erasure_authorized() {
            directory.change_lineage(lineage, LineageChange::AbandonExactRetirement { retirement: retirement.clone() }).await?;
            crate::store::release_region(directory, deployment, retirement.token, false).await?;
            return prepared.map(|()| false);
        }
    }
    provider.erase(&target, version).await?;
    if !provider.absent(&target, version).await? { return Err(changed()); }
    settle(directory, lineage, deployment, retirement).await?;
    Ok(true)
}

async fn authorize(
    directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, task: &HostedTask,
    bucket: &dyn ObjectStore, provider: &dyn RetirementProvider, lineage: &str,
    retirement: &LineageRetirement, threshold: i64,
) -> Result<()> {
    let RetirementWork::ReplayPack { deployment, target, version, capture, checkpoint } = &retirement.work
        else { return Err(changed()); };
    let target = key(target)?;
    let store = format!("{}/{}", retirement.source, archive::checkpoint_storage_root()?);
    let Some(block) = target.as_str().strip_prefix(&format!("{store}/")).and_then(pack_block)
        else { return Err(changed()); };
    let Some(record) = directory.deployment(deployment).await? else { return Ok(()); };
    if !eligible(&record) || record.active_s3_write_lease(now_ms()).is_some()
        || record.lineage != lineage || record.storage_prefix() != retirement.source
        || record.checkpoint != Some(*checkpoint) || record.serving_replay_capture != Some(*capture)
        || block >= checkpoint.block || !Base::load(bucket, &store).await?.objects.is_empty()
    {
        return Ok(());
    }
    let (Some(anchor), Some(material)) = (record.replica_anchor.as_deref(), keys.existing_key(&record.key_id()).await?)
        else { return Ok(()); };
    if record.required_replica_offsets(now_ms()).any(|first| first <= anchor.interval[1]) {
        return Ok(());
    }
    let references = References::scan(bucket, &directory.deployments().await?).await?;
    if references.has_declared_reference(&target) { return Ok(()); }
    let cipher = Cipher::new(&material)?;
    let target = &target;
    let store = &store;
    archive::with_replay_archive(directory, storage, keys, &record, Some(task), |admitted, owner, archive| async move {
        let index = match observe_pack_index(bucket, target).await {
            Ok(index) if index.version.as_ref() == Some(version) => index,
            Ok(_) | Err(Error::MissingObject(_)) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let Some(boundary) = covered_pack(bucket, target, index, block, threshold, record.log_epoch, &cipher, &archive).await?
            else { return Ok(()); };
        if !crate::checkpoint_retention::replay_archive_covers_pack(block, checkpoint.block, boundary,
            anchor.interval[0], anchor.interval[1], checkpoint.boundary)
            || !successor_present(bucket, store, *checkpoint, &cipher).await?
            || !provider.available(target, version).await?
        {
            return Ok(());
        }
        let fenced = crate::admin::update_installation(directory, deployment, |latest| {
            if latest.version != admitted.version
                || !latest.storage_regions.get(&owner).is_some_and(|region| region.admits_archive_read(*capture))
            {
                return Err(changed());
            }
            latest.storage_regions.insert(retirement.token, StorageRegion::ReplayPackRetirement {
                object: target.to_string(), capture: *capture,
            });
            Ok(())
        }).await?;
        directory.authorize_owned_retirement(deployment, fenced.version, retirement.clone()).await?;
        Ok(())
    }).await?;
    Ok(())
}

async fn successor_present(bucket: &dyn ObjectStore, store: &str, checkpoint: Checkpoint, cipher: &Cipher) -> Result<bool> {
    let root = block_key(BlockIndex(checkpoint.block))?;
    let pack = key(&format!("{store}/{}", block_pack(&root)))?;
    let observed = match observe_pack_index(bucket, &pack).await {
        Ok(observed) if observed.version.is_some() => observed,
        Ok(_) | Err(Error::MissingObject(_)) => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let Some(entry) = observed.value.entries.get(&root).filter(|entry| entry.length <= MAXIMUM_MEMBER_BYTES)
        else { return Ok(false); };
    let sealed = read_member(bucket, &pack, entry, observed.version.as_ref()).await?;
    let catalog = BlockCatalog::decode(&cipher.open(&root, &sealed)?)?;
    Ok(catalog.block_index().0 == checkpoint.block && catalog.publication_sha256() == checkpoint.root_sha256)
}

fn changed() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/replay-pack-retirement", "The replay pack retirement evidence changed")
}
