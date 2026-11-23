use std::collections::BTreeSet;

use async_trait::async_trait;
use rtdb_directory::{Checkpoint, DeploymentRecord, Directory, ExactRetirementGrant, Lifecycle, LineageChange, LineageRetirement, LineageState, RetirementWork, StorageRegion, Version};
use rtdb_format::keys::{CURRENT_STORAGE_VERSION, StorageRoot, block_key};
use rtdb_storage::{Error, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, PhysicalObjectVersion, Result, S3TombstoneTarget, StorageEpoch, TerminalObjectPublication};
use uuid::Uuid;

use super::{GARBAGE_DIRECTORY, StoredRelease, can_reap, checkpoint_metadata_block, permits_release, recovery_covers_release};
use crate::checkpoint_retention::checkpoint_pack_collectable;
use crate::cipher::Cipher;
use crate::keys::KeyService;
use crate::packing::{PackIndex, block_pack, observe_pack_index, pack_block};
use crate::reference::{Base, References, stores};

pub(super) mod tombstones;
use tombstones::TombstoneJournal;

const MAXIMUM_RELEASE_BYTES: u64 = 32 << 10;

struct ReleaseWitness {
    record: super::GarbageRecord,
    replica: Option<super::ReplicaPayloadRelease>,
    version: Option<PhysicalObjectVersion>,
    terminal: Option<TerminalObjectPublication>,
    pack: Option<String>,
    release_version: Option<PhysicalObjectVersion>,
}

impl ReleaseWitness {
    fn target_name(&self) -> Option<&str> {
        if self.replica.is_some() { return Some(&self.record.key); }
        match self.pack.as_deref() {
            Some(pack) if (self.version.is_some() || self.terminal.is_some()) && self.record.trie_floor.is_none()
                && pack_block(pack).is_some()
                && checkpoint_metadata_block(&self.record.key).is_some() => Some(pack),
            None if self.record.trie_floor.is_some() => Some(&self.record.key),
            _ => None,
        }
    }

    fn ready_by(&self, threshold: i64) -> bool {
        self.record.trie_floor.is_some() || self.replica.is_some() || self.record.released_at_ms <= threshold
    }

    fn permits(&self, owner: &DeploymentRecord, store: &str) -> bool {
        if let Some(replica) = &self.replica {
            return self.version.as_ref().is_some_and(|version| replica.permits(owner, store, version));
        }
        permits_release(owner, store, &self.record, self.pack.is_none() && (self.version.is_some() || self.terminal.is_some())) && recovery_covers_release(owner, &self.record)
            && super::oversized_payload_collectable(owner, &self.record.key, rtdb_directory::now_ms())
    }
}

async fn read_release(bucket: &dyn ObjectStore, key: &ObjectKey) -> Result<Option<ReleaseWitness>> {
    let size = match bucket.head_fresh(key).await {
        Err(Error::MissingObject(_)) => return Ok(None),
        result => result?.size,
    };
    if size > MAXIMUM_RELEASE_BYTES { return Ok(None); }
    let observed = match bucket.observe_fresh_exact(key, size as usize).await {
        Err(Error::MissingObject(_)) => return Ok(None),
        result => result?,
    };
    let Ok(stored) = serde_json::from_slice::<StoredRelease>(&observed.value) else { return Ok(None); };
    let (record, replica, version, terminal, pack) = match stored {
        StoredRelease::Replica { replica, version } => (replica.record.clone(), Some(replica), Some(version), None, None),
        StoredRelease::Exact { record, version, pack } => (record, None, Some(version), None, pack),
        StoredRelease::Terminal { record, terminal, pack } => (record, None, None, Some(terminal), pack),
        StoredRelease::Path(record) => (record, None, None, None, None),
    };
    Ok(Some(ReleaseWitness { record, replica, version, terminal, pack, release_version: observed.version }))
}


#[async_trait]
pub(super) trait RetirementProvider: Send + Sync {
    async fn available(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool>;
    async fn absent(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool>;
    async fn erase(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<()>;

    async fn terminal_targets(&self, _: &ObjectKey, _: &ObjectKey, _: &TerminalObjectPublication) -> Result<Option<[S3TombstoneTarget; 2]>> {
        Ok(None)
    }

    async fn terminal_pack_index(&self, _: &ObjectKey, _: &TerminalObjectPublication) -> Result<Option<PackIndex>> {
        Ok(None)
    }

    async fn terminal_checkpoint_matches(&self, _: &ObjectKey, _: Checkpoint, _: &Cipher) -> Result<bool> {
        Ok(false)
    }

    async fn retire_tombstones(&self, _: &TombstoneJournal<'_>, _: [(&ObjectKey, &S3TombstoneTarget); 2]) -> Result<()> {
        Err(Anomaly::unsupported("rtdb.multi/tombstone-provider", "The provider cannot retire never-versioned objects"))
    }
}

#[async_trait]
impl RetirementProvider for rtdb_s3::VersionedInventory {
    async fn available(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool> {
        self.exact_retirement_available(key, version).await
    }

    async fn absent(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool> {
        self.exact_retirement_absent(key, version).await
    }

    async fn erase(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<()> {
        self.retire_observed_version(key, version).await
    }

    async fn terminal_targets(&self, key: &ObjectKey, release: &ObjectKey, publication: &TerminalObjectPublication) -> Result<Option<[S3TombstoneTarget; 2]>> {
        self.terminal_release_targets(key, release, publication).await
    }

    async fn terminal_pack_index(&self, key: &ObjectKey, expected: &TerminalObjectPublication) -> Result<Option<PackIndex>> {
        let Some(provider) = self.enrolled_terminal_retirement().await? else { return Ok(None); };
        super::replay_retirement::terminal_pack_index(&provider, key, expected).await
    }

    async fn terminal_checkpoint_matches(&self, key: &ObjectKey, checkpoint: Checkpoint, cipher: &Cipher) -> Result<bool> {
        let Some(provider) = self.enrolled_terminal_retirement().await? else { return Ok(false); };
        super::replay_retirement::terminal_checkpoint_matches(&provider, key, checkpoint, cipher).await
    }

    async fn retire_tombstones(&self, journal: &TombstoneJournal<'_>, objects: [(&ObjectKey, &S3TombstoneTarget); 2]) -> Result<()> {
        let provider = self.never_versioned_retirement().await?;
        for (key, target) in objects { tombstones::retire(&provider, journal, key, target).await?; }
        Ok(())
    }
}

pub(super) async fn reap(directory: &dyn Directory, bucket: &dyn ObjectStore, provider: &dyn RetirementProvider,
    candidate: &DeploymentRecord, threshold: i64, advisory: Option<&References>, keys: Option<&dyn KeyService>) -> Result<()> {
    let lineage = &candidate.lineage;
    let mut state = super::recover_hosted_lineage(directory, lineage).await?;
    if let Some(retirement) = state.retirement() {
        match &retirement.work {
            RetirementWork::S3 { deployment, .. } | RetirementWork::Tombstones { deployment, .. } if *deployment == candidate.id => {
                finish(directory, bucket, provider, lineage, &state, retirement, keys, threshold).await?;
            }
            _ => {},
        }
        return Ok(());
    }
    if !can_reap(candidate) { return Ok(()); }
    if state.preparations().next().is_some() || state.open_generation().is_none() { return Ok(()); }
    publish_settled_replica_candidates(directory, bucket, provider, candidate).await?;
    let source = candidate.storage_prefix();
    for store in stores(bucket, &source).await? {
        let markers = ObjectKey::new(format!("{store}/{GARBAGE_DIRECTORY}"))?;
        let mut eligible = Vec::new();
        for marker in bucket.list(Some(&markers)).await? {
            let Some(witness) = read_release(bucket, &marker.key).await? else { continue; };
            let Some(relative) = witness.target_name().filter(|_| witness.release_version.is_some() || witness.terminal.is_some()) else { continue; };
            if !witness.ready_by(threshold) || !witness.permits(candidate, &store) { continue; }
            let target = ObjectKey::new(format!("{store}/{relative}"))?;
            if advisory.is_some_and(|observed| observed.has_declared_reference(&target) || observed.has_declared_reference(&marker.key)) { continue; }
            eligible.push((marker.key, target, witness));
        }
        let mut checked = BTreeSet::new();
        let mut checked_terminal = BTreeSet::new();
        for (marker, target, witness) in &eligible {
            let work = if let Some(publication) = &witness.terminal {
                if witness.pack.is_some()
                    && (!checked_terminal.insert((target.clone(), publication.etag.clone()))
                        || pack_release_bound(bucket, provider, target, witness, &markers, threshold).await?.is_none())
                {
                    continue;
                }
                let Some([data_tombstone, release_tombstone]) = provider.terminal_targets(target, marker, publication).await? else { continue; };
                RetirementWork::Tombstones { deployment: candidate.id.clone(), target: target.to_string(), release: marker.to_string(), data_tombstone, release_tombstone }
            } else {
                let (Some(version), Some(release_version)) = (&witness.version, &witness.release_version) else { continue; };
                if witness.pack.is_some() {
                    if !checked.insert((target.clone(), version.version.clone())) { continue; }
                    // Keep an incomplete pack from repeatedly closing its lineage.
                    if pack_release_bound(bucket, provider, target, witness, &markers, threshold).await?.is_none() { continue; }
                }
                if !provider.available(target, version).await? || !provider.available(marker, release_version).await? { continue; }
                RetirementWork::S3 { deployment: candidate.id.clone(), target: target.to_string(),
                    release: marker.to_string(), version: version.clone(), release_version: release_version.clone() }
            };
            let retirement = LineageRetirement { token: Uuid::new_v4(), source: source.clone(), work };
            let reserved = directory.change_lineage(lineage, LineageChange::ReserveRetirement {
                retirement: retirement.clone(), observed_references: state.reference_revision(), generation: state.retirement_generation(),
            }).await?;
            if finish(directory, bucket, provider, lineage, &reserved, &retirement, keys, threshold).await? { return Ok(()); }
            state = directory.lineage_state(lineage).await?;
            if state.preparations().next().is_some() || state.open_generation().is_none() { return Ok(()); }
        }
    }
    Ok(())
}

async fn finish(directory: &dyn Directory, bucket: &dyn ObjectStore, provider: &dyn RetirementProvider,
    lineage: &str, state: &LineageState, retirement: &LineageRetirement, keys: Option<&dyn KeyService>, threshold: i64) -> Result<bool> {
    let (deployment, target, release) = match &retirement.work {
        RetirementWork::S3 { deployment, target, release, .. } | RetirementWork::Tombstones { deployment, target, release, .. } => (deployment, target, release),
        _ => return Err(changed()),
    };
    let target = ObjectKey::new(target)?;
    let release = ObjectKey::new(release)?;
    let authorized;
    let grant = if let Some(grant) = state.exact_retirement_grant(retirement) {
        grant
    } else {
        let ready = prepare(directory, bucket, provider, lineage, retirement, &target, &release, keys, threshold).await;
        match ready {
            Ok(Some(owner_version)) => {
                authorized = match directory.authorize_owned_retirement(deployment, owner_version, retirement.clone()).await {
                    Ok(authorized) => authorized,
                    Err(error) => {
                        if directory.lineage_state(lineage).await?.retirement() != Some(retirement) {
                            super::release_region(directory, deployment, retirement.token, false).await?;
                        }
                        return Err(error);
                    }
                };
                authorized.exact_retirement_grant(retirement).ok_or_else(changed)?
            }
            outcome => {
                directory.change_lineage(lineage, LineageChange::AbandonExactRetirement { retirement: retirement.clone() }).await?;
                super::release_region(directory, deployment, retirement.token, false).await?;
                return outcome.map(|_| false);
            }
        }
    };
    erase_granted(directory, provider, lineage, grant).await?;
    settle(directory, lineage, deployment, retirement).await?;
    if let RetirementWork::S3 { version, .. } = &retirement.work {
        forget_settled_replica_candidate(directory, deployment, &target, version).await?;
    }
    Ok(true)
}

async fn erase_granted(directory: &dyn Directory, provider: &dyn RetirementProvider, lineage: &str, grant: ExactRetirementGrant<'_>) -> Result<()> {
    let retirement = grant.retirement();
    match &retirement.work {
        RetirementWork::S3 { target, release, version, release_version, .. } => {
            let target = ObjectKey::new(target)?;
            let release = ObjectKey::new(release)?;
            for (key, publication) in [(&target, version), (&release, release_version)] {
                let observed = || serde_json::json!({
                    "lineage": lineage, "revision": grant.revision(), "residence": retirement.source, "retirement": retirement.token,
                    "key": key.as_str(), "publication": publication,
                });
                crate::residence_trace::record("authorize-version", observed);
                provider.erase(key, publication).await?;
                crate::residence_trace::record("remove-version", observed);
            }
        }
        RetirementWork::Tombstones { target, release, data_tombstone, release_tombstone, .. } => {
            let target = ObjectKey::new(target)?;
            let release = ObjectKey::new(release)?;
            let journal = TombstoneJournal::new(directory, lineage, retirement.token);
            let observed = || serde_json::json!({
                "lineage": lineage, "revision": grant.revision(), "residence": retirement.source, "retirement": retirement.token,
                "key": target.as_str(), "release": release.as_str(), "data": data_tombstone, "marker": release_tombstone,
            });
            crate::residence_trace::record("authorize-tombstones", observed);
            provider.retire_tombstones(&journal, [(&target, data_tombstone), (&release, release_tombstone)]).await?;
            crate::residence_trace::record("remove-tombstones", observed);
        }
        _ => return Err(changed()),
    }
    Ok(())
}

fn replica_payload_store(owner: &DeploymentRecord) -> Result<String> {
    let root = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0))?.key();
    Ok(format!("{}/{root}", owner.storage_prefix()))
}

async fn publish_settled_replica_candidates(directory: &dyn Directory, bucket: &dyn ObjectStore,
    provider: &dyn RetirementProvider, owner: &DeploymentRecord) -> Result<()> {
    let Some(plan) = owner.replica_payload_trim.as_ref().filter(|plan| plan.applied && plan.belongs_to(owner))
        else { return Ok(()); };
    let store = replica_payload_store(owner)?;
    for candidate in &plan.candidates {
        let release = super::ReplicaPayloadRelease {
            record: super::GarbageRecord { key: candidate.key.clone(), released_at_ms: 0, trie_floor: None },
            incarnation: plan.incarnation, log_epoch: plan.log_epoch, position: candidate.position, frame_sha256: candidate.frame_sha256,
        };
        if !release.permits(owner, &store, &candidate.version) { continue; }
        let target = ObjectKey::new(format!("{store}/{}", candidate.key))?;
        if provider.absent(&target, &candidate.version).await? {
            forget_settled_replica_candidate(directory, &owner.id, &target, &candidate.version).await?;
        } else {
            super::publish_replica_payload_release(bucket, owner, candidate.key.clone(), candidate.position,
                candidate.frame_sha256, candidate.version.clone()).await?;
        }
    }
    Ok(())
}

async fn forget_settled_replica_candidate(directory: &dyn Directory, deployment: &str,
    target: &ObjectKey, version: &PhysicalObjectVersion) -> Result<()> {
    let Some(owner) = directory.deployment(deployment).await? else { return Ok(()); };
    let store = replica_payload_store(&owner)?;
    let Some(relative) = target.as_str().strip_prefix(&format!("{store}/"))
        .filter(|key| key.starts_with("oversized/")) else { return Ok(()); };
    let Some(plan) = owner.replica_payload_trim.as_ref().filter(|plan| plan.applied && plan.belongs_to(&owner)) else { return Ok(()); };
    if !plan.candidates.iter().any(|candidate| candidate.key == relative && &candidate.version == version) { return Ok(()); }
    crate::admin::update_installation(directory, deployment, |record| {
        if replica_payload_store(record)? == store
            && let Some(plan) = record.replica_payload_trim.as_mut().filter(|plan| plan.applied
                && plan.incarnation == owner.incarnation && plan.log_epoch == owner.log_epoch)
        {
            plan.candidates.retain(|candidate| candidate.key != relative || &candidate.version != version);
        }
        Ok(())
    }).await?;
    Ok(())
}

pub(super) async fn settle(directory: &dyn Directory, lineage: &str, deployment: &str, retirement: &LineageRetirement) -> Result<()> {
    for _ in 0..8 {
        let Some(owner) = directory.deployment(deployment).await? else { break; };
        if directory.lineage_state(lineage).await?.retirement() != Some(retirement) {
            return Ok(());
        }
        match directory.settle_owned_version(deployment, owner.version, retirement.clone()).await {
            Ok(rtdb_directory::Cas::Applied(_)) => return Ok(()),
            Ok(rtdb_directory::Cas::Conflict(_)) => {},
            Err(error) => {
                if directory.lineage_state(lineage).await?.retirement() != Some(retirement) {
                    return Ok(());
                }
                return Err(error);
            }
        }
    }
    Err(changed())
}

// The pack index fixes the set of members. Only evidence for the same immutable
// publication can cover a member, and each member must be checkpoint metadata.
async fn pack_release_bound(bucket: &dyn ObjectStore, provider: &dyn RetirementProvider,
    target: &ObjectKey, witness: &ReleaseWitness, markers: &ObjectKey, threshold: i64) -> Result<Option<PackReadiness>> {
    let index = if let Some(publication) = &witness.terminal {
        let Some(index) = provider.terminal_pack_index(target, publication).await? else { return Ok(None); };
        index
    } else {
        let Some(version) = witness.version.as_ref() else { return Err(changed()); };
        match observe_pack_index(bucket, target).await {
            Ok(index) if index.version.as_ref() == Some(version) => index.value,
            Ok(_) | Err(Error::MissingObject(_)) => return Ok(provider.absent(target, version).await?.then_some(PackReadiness::AlreadyAbsent)),
            Err(error) => return Err(error.into()),
        }
    };
    if !index.entries.contains_key(&witness.record.key) { return Ok(None); }
    let mut uncovered = BTreeSet::new();
    let Some(pack) = witness.pack.as_deref() else { return Err(changed()); };
    let Some(block) = pack_block(pack) else { return Err(changed()); };
    let catalog = block_key(BlockIndex(block))?;
    if !index.entries.contains_key(&catalog) { return Ok(None); }
    let mut youngest = block;
    for name in index.entries.into_keys() {
        let Some(member) = checkpoint_metadata_block(&name) else { return Ok(None); };
        youngest = youngest.max(member);
        if name != witness.record.key { uncovered.insert(name); }
    }
    if !uncovered.is_empty() {
        for marker in bucket.list(Some(markers)).await? {
            let Some(member) = read_release(bucket, &marker.key).await? else { continue; };
            if member.target_name() == Some(pack) && member.version == witness.version && member.terminal == witness.terminal
                && (member.release_version.is_some() || member.terminal.is_some()) && member.record.released_at_ms <= threshold
            {
                uncovered.remove(&member.record.key);
                if uncovered.is_empty() { break; }
            }
        }
    }
    if !uncovered.is_empty() { return Ok(None); }
    Ok(Some(PackReadiness::Covered { block, youngest }))
}

#[derive(Clone, Copy)]
enum PackReadiness {
    AlreadyAbsent,
    Covered { block: i64, youngest: i64 },
}

async fn prepare(directory: &dyn Directory, bucket: &dyn ObjectStore, provider: &dyn RetirementProvider, lineage: &str, retirement: &LineageRetirement,
    target: &ObjectKey, release: &ObjectKey, keys: Option<&dyn KeyService>, threshold: i64) -> Result<Option<Version>> {
    let root = ObjectKey::new(&retirement.source)?;
    let Some((store, relative)) = target.as_str().strip_prefix(&format!("{root}/")).and_then(|suffix| suffix.split_once('/'))
        else { return Err(changed()); };
    let store = format!("{root}/{store}");
    let markers = ObjectKey::new(format!("{store}/{GARBAGE_DIRECTORY}"))?;
    if !release.is_under(&markers) { return Err(changed()); }
    let Some(witness) = read_release(bucket, release).await? else { return Ok(None); };
    if witness.target_name() != Some(relative) { return Err(changed()); }
    if !witness.ready_by(threshold) { return Ok(None); }
    let deployment = match &retirement.work {
        RetirementWork::S3 { deployment, version, release_version, .. } => {
            if witness.version.as_ref() != Some(version) || witness.release_version.as_ref() != Some(release_version) {
                return Ok(None);
            }
            deployment
        }
        RetirementWork::Tombstones { deployment, data_tombstone, release_tombstone, .. } => {
            let Some(publication) = &witness.terminal else { return Ok(None); };
            if witness.pack.is_none()
                && witness.record.trie_floor.as_ref().is_none_or(|floor| floor.object_namespace != publication.allocation)
            {
                return Ok(None);
            }
            let Some([data, marker]) = provider.terminal_targets(target, release, publication).await? else { return Ok(None); };
            if &data != data_tombstone || &marker != release_tombstone { return Err(changed()); }
            deployment
        }
        _ => return Err(changed()),
    };
    let pack = if witness.pack.is_some() {
        let Some(ready) = pack_release_bound(bucket, provider, target, &witness, &markers, threshold).await? else { return Ok(None); };
        Some(ready)
    } else {
        None
    };
    let release_name = release.as_str().strip_prefix(&format!("{store}/")).ok_or_else(changed)?;

    let deployments = directory.deployments().await?;
    for record in deployments.iter().filter(|record| record.lineage == lineage && record.lifecycle != Lifecycle::Deleted) {
        for referrer in stores(bucket, &record.storage_prefix()).await? {
            let base = Base::load(bucket, &referrer).await?;
            if base.objects.get(relative) == Some(&store) || base.objects.get(release_name) == Some(&store) {
                return Ok(None);
            }
        }
    }

    if pack.is_some() {
        let base = Base::load(bucket, &store).await?;
        if base.objects.contains_key(release_name) { return Ok(None); }
        if matches!(pack, Some(PackReadiness::Covered { .. })) && base.objects.contains_key(relative) { return Ok(None); }
    }
    let mut observed_successor = None;
    if let Some(PackReadiness::Covered { block, youngest }) = pack {
        let Some(owner) = deployments.iter().find(|record| record.id == *deployment) else { return Ok(None); };
        let Some(checkpoint) = owner.checkpoint else { return Ok(None); };
        if !checkpoint_pack_collectable(block, youngest, Some(checkpoint.block), None) { return Ok(None); }
        let catalog = block_key(BlockIndex(checkpoint.block))?;
        let successor = ObjectKey::new(format!("{store}/{}", block_pack(&catalog)))?;
        if witness.terminal.is_some() {
            let Some(keys) = keys else { return Ok(None); };
            let Some(material) = keys.existing_key(&owner.key_id()).await? else { return Ok(None); };
            let cipher = Cipher::new(&material)?;
            if !provider.terminal_checkpoint_matches(&successor, checkpoint, &cipher).await? { return Ok(None); }
        } else {
            match observe_pack_index(bucket, &successor).await {
                Ok(index) if index.version.is_some() && index.value.entries.contains_key(&catalog) => {},
                Ok(_) | Err(Error::MissingObject(_)) => return Ok(None),
                Err(error) => return Err(error.into()),
            }
        }
        observed_successor = Some(checkpoint);
    }

    let owner = crate::admin::update_installation(directory, deployment, |record| {
        if record.lineage != lineage || record.storage_prefix() != retirement.source || !can_reap(record)
            || !witness.permits(record, &store)
        {
            return Err(changed());
        }
        if record.storage_regions.values().any(|region| match region {
            StorageRegion::ReferenceRead { object, archive_source: None, .. } =>
                object.as_deref().is_none_or(|key| key == target.as_str() || key == release.as_str()),
            StorageRegion::Remote { archive: Some(_), archive_source: None, .. }
            | StorageRegion::Local { archive: Some(_), .. } => true,
            _ => false,
        }) { return Err(changed()); }
        if record.storage_regions.values().any(|region| archived_source_holds(region, &retirement.work)) {
            return Err(Anomaly::new(AnomalyCategory::Busy, ARCHIVE_SOURCE_HELD, "An admitted archive copy retains this publication"));
        }
        if let Some(PackReadiness::Covered { block, youngest }) = pack {
            if let Some(publication) = &witness.terminal
                && record.storage_namespaces.is_none_or(|namespaces| namespaces.objects != publication.allocation)
            {
                return Err(changed());
            }
            let oldest = record.storage_regions.values().filter_map(StorageRegion::checkpoint_metadata_from).min();
            if record.checkpoint != observed_successor
                || !checkpoint_pack_collectable(block, youngest, record.checkpoint.map(|checkpoint| checkpoint.block), oldest)
            {
                return Err(changed());
            }
        }
        let fence = StorageRegion::ExactRetirement { object: target.to_string(), release: Some(release.to_string()) };
        if let Some(previous) = record.storage_regions.insert(retirement.token, fence.clone()) && previous != fence {
            return Err(changed());
        }
        Ok(())
    }).await;
    let owner = match owner {
        Err(error) if error.code == ARCHIVE_SOURCE_HELD => return Ok(None),
        owner => owner?,
    };

    if pack.is_none() {
        let base = Base::load(bucket, &store).await?;
        let released = BTreeSet::from([relative.to_owned(), release_name.to_owned()]);
        base.forget(bucket, &store, &released).await?;
    }
    if let (Some(replica), RetirementWork::S3 { version, .. }) = (&witness.replica, &retirement.work)
        && let (Some(trim), Some(anchor)) = (&owner.replica_payload_trim, &owner.replica_anchor)
    {
        crate::residence_trace::record("retirement-recovery-dependency", || serde_json::json!({
            "lineage": owner.lineage, "installation": owner.id, "incarnation": owner.incarnation,
            "epoch": owner.log_epoch, "stream": owner.stream("replica"), "revision": owner.version,
            "residence": retirement.source, "retirement": retirement.token, "key": target.as_str(),
            "publication": version, "position": replica.position, "frame": replica.frame_sha256,
            "trim_through": trim.through, "anchor": anchor.digest,
            "journals": trim.journals.iter().filter_map(|endpoint| endpoint.journal()).collect::<Vec<_>>(),
        }));
    }
    Ok(Some(owner.version))
}

const ARCHIVE_SOURCE_HELD: &str = "rtdb.multi/archive-source-held";

fn archived_source_holds(region: &StorageRegion, work: &RetirementWork) -> bool {
    let (StorageRegion::Remote { archive_source: Some(hold), .. }
        | StorageRegion::ReferenceRead { archive_source: Some(hold), .. }) = region else { return false; };
    match work {
        RetirementWork::S3 { version, release_version, .. } =>
            !hold.permits_retirement(version) || !hold.permits_retirement(release_version),
        _ => true,
    }
}

fn changed() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/exact-retirement", "The source residence has an incompatible storage admission")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use rtdb_s3::{S3Config, S3Credentials};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;
    use rtdb_directory::{CatalogAdmissionFloor, ForkPreparation, MemoryDirectory, ReplayCustody, StorageNamespaces};
    use rtdb_format::keys::{CURRENT_STORAGE_VERSION, StorageRoot};
    use rtdb_storage::MemoryStore;
    use rtdb_types::{CatalogCut, ReplicaPosition, S3ObjectNamespace, StorageEpoch};

    use super::*;

    #[derive(Default)]
    struct VersionedReleaseStore {
        objects: MemoryStore,
        versions: parking_lot::Mutex<BTreeMap<ObjectKey, String>>,
    }

    impl VersionedReleaseStore {
        fn version(&self, key: &ObjectKey) -> PhysicalObjectVersion {
            PhysicalObjectVersion {
                namespace: S3ObjectNamespace { bucket: "cell".into(), endpoint: None, region: None },
                key: format!("cell/{}", key.as_str().replace('~', "%7E")),
                version: self.versions.lock().get(key).cloned().unwrap_or_else(|| "marker".into()),
            }
        }
    }

    #[async_trait]
    impl ObjectStore for VersionedReleaseStore {
        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<rtdb_storage::object_store::StoredObject> {
            self.objects.head(key).await
        }
        async fn observe_head(&self, key: &ObjectKey) -> rtdb_storage::Result<rtdb_storage::ObjectObservation<rtdb_storage::object_store::StoredObject>> {
            Ok(rtdb_storage::ObjectObservation { value: self.head(key).await?, version: Some(self.version(key)) })
        }
        async fn observe_range(&self, key: &ObjectKey, range: std::ops::Range<u64>) -> rtdb_storage::Result<rtdb_storage::ObjectObservation<Bytes>> {
            Ok(rtdb_storage::ObjectObservation { value: self.get_range(key, range).await?, version: Some(self.version(key)) })
        }
        async fn retirement_version(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<PhysicalObjectVersion>> {
            match self.head(key).await {
                Ok(_) => Ok(Some(self.version(key))),
                Err(Error::MissingObject(_)) => Ok(None),
                Err(error) => Err(error),
            }
        }
        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.objects.get(key).await }
        async fn get_range(&self, key: &ObjectKey, range: std::ops::Range<u64>) -> rtdb_storage::Result<Bytes> {
            self.objects.get_range(key, range).await
        }
        async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
            self.objects.put(key, bytes).await
        }
        async fn put_with_publication(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectPublication>> {
            self.put(key, bytes).await?;
            Ok(Some(rtdb_types::PhysicalObjectPublication::Version(self.version(key))))
        }
        async fn put_file(&self, key: &ObjectKey, path: &std::path::Path) -> rtdb_storage::Result<()> {
            self.objects.put_file(key, path).await
        }
        async fn list(&self, key: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<rtdb_storage::object_store::StoredObject>> {
            self.objects.list(key).await
        }
        async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.objects.delete(key).await }
        async fn observe_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<rtdb_storage::ObjectObservation<Bytes>> {
            let value = self.objects.get_fresh_exact(key, length).await?;
            Ok(rtdb_storage::ObjectObservation { value, version: Some(self.version(key)) })
        }
    }

    struct Provider {
        version: PhysicalObjectVersion,
        attempts: AtomicUsize,
        markers: AtomicUsize,
    }

    #[async_trait]
    impl RetirementProvider for Provider {
        async fn available(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool> {
            assert_eq!(version.key, format!("cell/{}", key.as_str().replace('~', "%7E")));
            Ok(true)
        }

        async fn absent(&self, _key: &ObjectKey, _version: &PhysicalObjectVersion) -> Result<bool> { Ok(false) }

        async fn erase(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<()> {
            assert_eq!(version.key, format!("cell/{}", key.as_str().replace('~', "%7E")));
            if key.as_str().contains("/~garbage/") {
                assert_eq!(version.version, "marker");
                self.markers.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            assert_eq!(version, &self.version);
            if self.attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                return Err(Anomaly::new(AnomalyCategory::Unavailable, "fixture/provider-reply-lost", "Provider response was lost"));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_certified_replica_payload_requires_settled_trim_and_retries_the_exact_version() {
        use rtdb_directory::{Checkpoint, JournalAnchor, ReplicaPayloadCandidate, ReplicaPayloadTrim};

        let state = tempfile::tempdir().unwrap();
        MemoryDirectory::initialize(state.path()).unwrap();
        let directory = MemoryDirectory::open(state.path()).unwrap();
        let bucket = VersionedReleaseStore::default();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.checkpoint = Some(Checkpoint { block: 1, boundary: 7, source: 5, system_time: 0, root_sha256: Some([1; 32]) });
        record.recovery_manifest_sha256 = Some([2; 32]);
        record.replica_anchor = Some(Box::new(JournalAnchor { root_incarnation: record.incarnation, interval: [0, 6], digest: [3; 32] }));
        record.replica_payload_trim = Some(Box::new(ReplicaPayloadTrim { incarnation: record.incarnation, log_epoch: record.log_epoch,
            through: 6, journals: [rtdb_types::JournalEndpoint::Member {
                address: "127.0.0.1:1".into(), journal: Uuid::new_v4().to_string(),
            }].into(), applied: false, candidates: Vec::new() }));
        let relative = "oversized/b00-published.binpb";
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0)).unwrap().key();
        let target = ObjectKey::new(format!("{}/{root}/{relative}", record.storage_prefix())).unwrap();
        bucket.put(&target, Bytes::from_static(b"another version")).await.unwrap();
        let provider = Provider { version: PhysicalObjectVersion {
            namespace: S3ObjectNamespace { bucket: "cell".into(), endpoint: None, region: None },
            key: format!("cell/{target}"), version: "retired".into(),
        }, attempts: AtomicUsize::new(0), markers: AtomicUsize::new(0) };
        record.replica_payload_trim.as_mut().unwrap().candidates.push(ReplicaPayloadCandidate {
            key: relative.into(), position: 6, frame_sha256: [4; 32], version: provider.version.clone(),
        });
        let record = directory.create_deployment(record).await.unwrap();
        super::super::publish_replica_payload_release(&bucket, &record, relative.into(), 6, [4; 32], provider.version.clone()).await.unwrap();
        let future = "oversized/b01-future.binpb";
        let future_key = ObjectKey::new(format!("{}/{root}/{future}", record.storage_prefix())).unwrap();
        bucket.put(&future_key, Bytes::from_static(b"still referenced")).await.unwrap();
        let forged = PhysicalObjectVersion { key: format!("cell/{future_key}"), ..provider.version.clone() };
        super::super::publish_replica_payload_release(&bucket, &record, future.into(), 6, [4; 32], forged).await.unwrap();

        reap(directory.as_ref(), &bucket, &provider, &record, 0, None, None).await.unwrap();
        assert_eq!(provider.attempts.load(Ordering::Relaxed), 0);
        let hold = Uuid::new_v4();
        let record = crate::admin::update_installation(directory.as_ref(), &record.id, |record| {
            record.replica_payload_trim.as_mut().unwrap().applied = true;
            record.storage_regions.insert(hold, StorageRegion::ReplicaReplay { first_required: 6, through: 6, custody: ReplayCustody::Local });
            Ok(())
        }).await.unwrap();
        reap(directory.as_ref(), &bucket, &provider, &record, 0, None, None).await.unwrap();
        assert_eq!(provider.attempts.load(Ordering::Relaxed), 0);
        super::super::release_region(directory.as_ref(), &record.id, hold, false).await.unwrap();

        let record = directory.deployment(&record.id).await.unwrap().unwrap();
        let lost_reply = reap(directory.as_ref(), &bucket, &provider, &record, 0, None, None).await.unwrap_err();
        assert_eq!(lost_reply.code, "fixture/provider-reply-lost");
        assert!(directory.lineage_state(&record.lineage).await.unwrap().exact_erasure_authorized());
        drop(directory);
        let directory = MemoryDirectory::open(state.path()).unwrap();
        reap(directory.as_ref(), &bucket, &provider, &record, 0, None, None).await.unwrap();
        assert_eq!(provider.attempts.load(Ordering::Relaxed), 2);
        assert_eq!(bucket.get(&target).await.unwrap().as_ref(), b"another version");
        let record = directory.deployment(&record.id).await.unwrap().unwrap();
        assert!(record.replica_payload_trim.unwrap().candidates.is_empty());
        assert_eq!(bucket.get(&future_key).await.unwrap().as_ref(), b"still referenced");
    }

    #[tokio::test]
    async fn the_exact_reaper_resumes_an_installed_trie_release_after_its_reader_drains() {
        use rtdb_directory::MemberId;
        use rtdb_format::trie_retirement::{RetirementFile, RetirementIntent, intent_digest, journal_directory};
        use rtdb_storage::IoRegion;
        use rtdb_types::TableRef;
        use std::sync::{Arc, atomic::AtomicBool};
        use tokio::sync::oneshot;
        use tokio_util::task::TaskTracker;

        struct Inventory {
            bucket: Arc<VersionedReleaseStore>,
            lose_reply: AtomicBool,
        }

        #[async_trait]
        impl RetirementProvider for Inventory {
            async fn available(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool> {
                Ok(self.bucket.retirement_version(key).await?.as_ref() == Some(version))
            }

            async fn absent(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool> {
                Ok(!self.available(key, version).await?)
            }

            async fn erase(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<()> {
                assert_eq!(&self.bucket.version(key), version);
                self.bucket.delete(key).await?;
                if self.lose_reply.swap(false, Ordering::Relaxed) {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, "fixture/reply-lost", "Provider reply lost"));
                }
                Ok(())
            }
        }

        let directory = MemoryDirectory::new();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        let namespaces = StorageNamespaces { objects: Uuid::new_v4(), keys: [0; 32] };
        let region = Uuid::new_v4();
        record.holder = Some(MemberId::fresh());
        record.storage_namespaces = Some(namespaces);
        record.storage_regions.insert(region, StorageRegion::Local {
            archive: None, checkpoint_suffix: None, repair_archive: None, tracks_ordered_index_pages: true,
            checkpoint_metadata_from: None, checkpoint_replay_from: None,
        });
        record.checkpoint = Some(rtdb_directory::Checkpoint {
            block: 1, boundary: 2, source: 1, system_time: 0, root_sha256: Some([1; 32]),
        });
        record.recovery_manifest_sha256 = Some([1; 32]);
        let record = directory.create_deployment(record).await.unwrap();
        let epoch = StorageEpoch(0);
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, epoch).unwrap().key();
        let scope = format!("{}/{root}", record.storage_prefix());
        let cut = CatalogCut { storage_version: CURRENT_STORAGE_VERSION, storage_epoch: epoch, replica_epoch: epoch, through: ReplicaPosition(0) };
        let bucket = Arc::new(VersionedReleaseStore::default());
        let inventory = Inventory { bucket: bucket.clone(), lose_reply: AtomicBool::new(true) };
        let mut serving = super::super::open_deployment_store(bucket.clone(), &scope, crate::packing::PackDiscovery::List, Arc::new(|| 0)).await.unwrap();
        let tracker = TaskTracker::new();
        Arc::get_mut(&mut serving).unwrap().catalog_readers = Some(crate::catalog_readers::CatalogReaders::new(
            directory.clone(), tracker.clone(), &record, root, namespaces, region, Some(record.epoch),
        ));
        let serving = super::super::EncryptingStore::new(serving, &[7; 32]).unwrap();
        let relative = ObjectKey::new("tables/orders/data/l01-rc-b00.arrow").unwrap();
        let target = ObjectKey::new(format!("{scope}/{relative}")).unwrap();
        bucket.versions.lock().insert(target.clone(), "trie-1".into());
        serving.put(&relative, Bytes::from_static(b"retained trie")).await.unwrap();
        let version = serving.retirement_version(&relative).await.unwrap().unwrap();
        let hold = serving.retain_catalog(Some(cut)).await.unwrap().unwrap();
        let io = IoRegion::new();
        let work = io.child();
        let (entered, running) = oneshot::channel();
        let (release_work, finish_work) = std::sync::mpsc::channel();
        let waiter = tokio::spawn(IoRegion::retaining(hold, async move {
            work.run_blocking(move || {
                entered.send(()).unwrap();
                finish_work.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(())
            }).await
        }));
        tokio::time::timeout(Duration::from_secs(5), running).await.unwrap().unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());

        let plan = RetirementIntent {
            selected: cut, table: TableRef::new("public", "orders"), tries: vec!["l01-rc-b00".into()],
            files: vec![RetirementFile {
                key: relative.to_string(), version: Some(version), matches_publication: true, terminal_publication: None,
            }],
            after_reset: None,
        };
        let installed = ReplicaPosition(1);
        let floor = plan.installed_cut(installed).unwrap();
        let encoded = serde_json::to_vec(&plan).unwrap();
        let receipt = ObjectKey::new(format!("{}/installed/{}", journal_directory(cut), intent_digest(&encoded))).unwrap();
        let contents = Bytes::from([installed.0.to_be_bytes().as_slice(), &encoded].concat());
        serving.put(&receipt, contents.clone()).await.unwrap();
        assert!(!serving.advance_catalog_floor(floor).await.unwrap());
        let held = directory.deployment(&record.id).await.unwrap().unwrap();
        assert_eq!(held.catalog_readers.len(), 1);
        reap(directory.as_ref(), bucket.as_ref(), &inventory, &held, i64::MAX, None, None).await.unwrap();
        assert_eq!(serving.get(&relative).await.unwrap(), Bytes::from_static(b"retained trie"));

        release_work.send(()).unwrap();
        io.close_and_wait().await;
        tracker.close();
        tokio::time::timeout(Duration::from_secs(5), tracker.wait()).await.unwrap();
        assert!(serving.release_trie_files(&receipt, contents).await.unwrap());
        let markers = ObjectKey::new(format!("{scope}/{GARBAGE_DIRECTORY}")).unwrap();
        let queued = bucket.list(Some(&markers)).await.unwrap();
        assert_eq!(queued.len(), 1);
        let current = directory.deployment(&record.id).await.unwrap().unwrap();
        let error = reap(directory.as_ref(), bucket.as_ref(), &inventory, &current, i64::MAX, None, None).await.unwrap_err();
        assert_eq!(error.code, "fixture/reply-lost");
        assert!(matches!(bucket.head(&target).await, Err(Error::MissingObject(_))));
        assert!(bucket.head(&queued[0].key).await.is_ok());
        assert!(directory.lineage_state(&record.lineage).await.unwrap().exact_erasure_authorized());

        reap(directory.as_ref(), bucket.as_ref(), &inventory, &current, i64::MAX, None, None).await.unwrap();
        assert!(bucket.list(Some(&markers)).await.unwrap().is_empty());
        assert!(directory.lineage_state(&record.lineage).await.unwrap().retirement().is_none());
    }

    #[tokio::test]
    async fn exact_retirement_waits_for_forks_and_reads_and_resumes_the_same_version() {
        let directory = MemoryDirectory::new();
        let bucket: crate::reference::Bucket = std::sync::Arc::new(VersionedReleaseStore::default());
        let mut parent = DeploymentRecord::new("shop", "acme", vec![], 0);
        let namespace = Uuid::new_v4();
        parent.storage_namespaces = Some(StorageNamespaces { objects: namespace, keys: [0; 32] });
        let epoch = StorageEpoch(0);
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, epoch).unwrap().key();
        let floor = CatalogAdmissionFloor { residence: parent.storage_prefix(), object_namespace: namespace, cut: CatalogCut {
            storage_version: CURRENT_STORAGE_VERSION, storage_epoch: epoch, replica_epoch: epoch, through: ReplicaPosition(2),
        } };
        parent.catalog_admission_floors.insert(root.clone(), floor.clone());
        parent.checkpoint = Some(rtdb_directory::Checkpoint { block: 0, boundary: 1, source: 0, system_time: 0, root_sha256: Some([1; 32]) });
        parent.recovery_manifest_sha256 = Some([1; 32]);
        let parent = directory.create_deployment(parent).await.unwrap();
        let store = format!("{}/{root}", parent.storage_prefix());
        let relative = ObjectKey::new("tables/orders/data/l01-rc-b00.arrow").unwrap();
        let target = ObjectKey::new(format!("{store}/{relative}")).unwrap();
        let versions = Provider { version: PhysicalObjectVersion {
            namespace: S3ObjectNamespace { bucket: "cell".into(), endpoint: None, region: None },
            key: format!("cell/{target}"), version: "retired".into(),
        }, attempts: AtomicUsize::new(0), markers: AtomicUsize::new(0) };
        bucket.put(&target, Bytes::from_static(b"replacement")).await.unwrap();
        let physical = std::sync::Arc::new(crate::reference::PrefixedStore::new(bucket.clone(), &store));
        let garbage = super::super::GarbageStore::open_with_clock(physical, std::sync::Arc::new(|| 0)).await.unwrap();
        garbage.write_release(&relative, Some(floor.clone()), None).await.unwrap();
        garbage.write_release(&relative, Some(floor), Some(&versions.version)).await.unwrap();

        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, 0, None, None).await.unwrap();
        assert_eq!(versions.attempts.load(Ordering::Relaxed), 0);
        let recovery = Uuid::new_v4();
        let parent = crate::admin::update_installation(directory.as_ref(), &parent.id, |record| {
            record.checkpoint = Some(rtdb_directory::Checkpoint { block: 1, boundary: 2, source: 1, system_time: 0, root_sha256: Some([2; 32]) });
            record.recovery_manifest_sha256 = None;
            Ok(())
        }).await.unwrap();
        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, 0, None, None).await.unwrap();
        assert_eq!(versions.attempts.load(Ordering::Relaxed), 0);
        let parent = crate::admin::update_installation(directory.as_ref(), &parent.id, |record| {
            record.recovery_manifest_sha256 = Some([2; 32]);
            record.storage_regions.insert(recovery, StorageRegion::ReplicaReplay { first_required: 2, through: 2, custody: ReplayCustody::Local });
            Ok(())
        }).await.unwrap();
        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, 0, None, None).await.unwrap();
        assert_eq!(versions.attempts.load(Ordering::Relaxed), 0);
        super::super::release_region(directory.as_ref(), &parent.id, recovery, false).await.unwrap();
        let parent = directory.deployment(&parent.id).await.unwrap().unwrap();

        let preparation = ForkPreparation { token: Uuid::new_v4(), generation: 0 };
        directory.change_lineage(&parent.lineage, LineageChange::PrepareHostedFork { preparation, task: None }).await.unwrap();
        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, 0, None, None).await.unwrap();
        assert!(directory.lineage_state(&parent.lineage).await.unwrap().retirement().is_none());

        let mut child = DeploymentRecord::new("preview", &parent.org, vec![], 0);
        child.lineage.clone_from(&parent.lineage);
        child.parent = Some(parent.id.clone());
        child.storage_namespaces = parent.storage_namespaces;
        directory.stage_hosted_fork(child.clone(), preparation).await.unwrap();
        crate::reference::fork(bucket.as_ref(), &parent.storage_prefix(), &child.storage_prefix(), None).await.unwrap();
        super::super::release_region(directory.as_ref(), &child.id, preparation.token, false).await.unwrap();
        let child = directory.publish_fork(child, parent.version, Some(rtdb_directory::ForkPublication::Hosted(preparation))).await.unwrap();
        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, 0, None, None).await.unwrap();
        assert_eq!(versions.attempts.load(Ordering::Relaxed), 0);
        let child_store = format!("{}/{root}", child.storage_prefix());
        let base = Base::load(bucket.as_ref(), &child_store).await.unwrap();
        assert_eq!(base.objects.get(relative.as_str()), Some(&store));
        base.forget(bucket.as_ref(), &child_store, &[relative.to_string()].into()).await.unwrap();

        let reader = Uuid::new_v4();
        crate::admin::update_installation(directory.as_ref(), &parent.id, |record| {
            record.storage_regions.insert(reader, StorageRegion::ReferenceRead { custody: ReplayCustody::Local, archive_source: None, object: Some(target.to_string()) });
            Ok(())
        }).await.unwrap();
        assert_eq!(reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, 0, None, None).await.unwrap_err().code, "rtdb.multi/exact-retirement");
        assert_eq!(versions.attempts.load(Ordering::Relaxed), 0);
        super::super::release_region(directory.as_ref(), &parent.id, reader, false).await.unwrap();

        assert_eq!(reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, 0, None, None).await.unwrap_err().code, "fixture/provider-reply-lost");
        let authorizing = directory.lineage_state(&parent.lineage).await.unwrap();
        assert!(authorizing.exact_erasure_authorized());
        let token = authorizing.retirement().unwrap().token;
        assert!(matches!(directory.deployment(&parent.id).await.unwrap().unwrap().storage_regions.get(&token), Some(StorageRegion::ExactRetirement { .. })));
        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, 0, None, None).await.unwrap();
        assert_eq!(versions.attempts.load(Ordering::Relaxed), 2);
        assert_eq!(versions.markers.load(Ordering::Relaxed), 1, "the release marker goes through versioned settlement");
        assert!(directory.lineage_state(&parent.lineage).await.unwrap().retirement().is_none());
        assert!(directory.deployment(&parent.id).await.unwrap().unwrap().storage_regions.is_empty());

        let records = directory.deployments().await.unwrap();
        for outcome in super::super::reaper_pass(bucket.as_ref(), &records, i64::MAX, Duration::ZERO).await.unwrap() {
            outcome.unwrap();
        }
        assert_eq!(bucket.get(&target).await.unwrap(), Bytes::from_static(b"replacement"));
    }
    #[async_trait]
    impl RetirementProvider for VersionedReleaseStore {
        async fn available(&self, _key: &ObjectKey, _version: &PhysicalObjectVersion) -> Result<bool> { Ok(true) }

        async fn absent(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool> {
            Ok(self.retirement_version(key).await?.as_ref() != Some(version))
        }

        async fn erase(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<()> {
            if !self.absent(key, version).await? { self.delete(key).await?; }
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_narrowed_archive_source_protects_its_publication_without_blocking_unrelated_retirement() {
        let directory = MemoryDirectory::new();
        let bucket = std::sync::Arc::new(VersionedReleaseStore::default());
        let mut owner = DeploymentRecord::new("shop", "acme", vec![], 0);
        let namespace = Uuid::new_v4();
        owner.storage_namespaces = Some(StorageNamespaces { objects: namespace, keys: [0; 32] });
        let epoch = StorageEpoch(0);
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, epoch).unwrap().key();
        let floor = CatalogAdmissionFloor { residence: owner.storage_prefix(), object_namespace: namespace, cut: CatalogCut {
            storage_version: CURRENT_STORAGE_VERSION, storage_epoch: epoch, replica_epoch: epoch, through: ReplicaPosition(2),
        } };
        owner.catalog_admission_floors.insert(root.clone(), floor.clone());
        owner.checkpoint = Some(rtdb_directory::Checkpoint { block: 1, boundary: 2, source: 1, system_time: 0, root_sha256: Some([1; 32]) });
        owner.recovery_manifest_sha256 = Some([1; 32]);
        let owner = directory.create_deployment(owner).await.unwrap();
        let store = format!("{}/{root}", owner.storage_prefix());
        let physical = std::sync::Arc::new(crate::reference::PrefixedStore::new(bucket.clone(), &store));
        let garbage = super::super::GarbageStore::open_with_clock(physical, std::sync::Arc::new(|| 0)).await.unwrap();
        let retained = ObjectKey::new("tables/orders/data/l01-rc-b00.arrow").unwrap();
        let unrelated = ObjectKey::new("tables/items/data/l01-rc-b00.arrow").unwrap();
        let data_key = |relative| ObjectKey::new(format!("{store}/{relative}")).unwrap();
        for file in [&retained, &unrelated] {
            let file_key = data_key(file);
            bucket.put(&file_key, Bytes::from_static(b"data")).await.unwrap();
            garbage.write_release(file, Some(floor.clone()), Some(&bucket.version(&file_key))).await.unwrap();
        }
        let retained = data_key(&retained);
        let unrelated = data_key(&unrelated);
        let retained_version = bucket.version(&retained);
        let mut region = super::super::remote_storage_region(Some(Uuid::new_v4()), None);
        let StorageRegion::Remote { archive_source, .. } = &mut region else { unreachable!() };
        let hold = rtdb_directory::ArchiveSourceHold::new(
            retained_version.namespace.clone(),
            [(retained_version.key.clone(), retained_version.version.clone())].into(),
        );
        *archive_source = Some(hold.clone());
        let reader = Uuid::new_v4();
        crate::admin::update_installation(directory.as_ref(), &owner.id, |record| {
            record.storage_regions.insert(reader, region.clone());
            Ok(())
        }).await.unwrap();

        let io = rtdb_storage::IoRegion::new();
        let (started, waiting) = tokio::sync::oneshot::channel();
        let (finish, drains) = std::sync::mpsc::channel();
        let pending = tokio::spawn({
            let io = io.clone();
            async move { io.run_blocking(move || {
                started.send(()).unwrap();
                drains.recv_timeout(Duration::from_secs(20)).unwrap();
                Ok(())
            }).await }
        });
        waiting.await.unwrap();
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        let pass = || reap(directory.as_ref(), bucket.as_ref(), bucket.as_ref(), &owner, 0, None, None);
        pass().await.unwrap();
        assert!(matches!(bucket.head(&unrelated).await, Err(Error::MissingObject(_))));
        assert!(bucket.head(&retained).await.is_ok());

        let inherited_reader = Uuid::new_v4();
        crate::admin::update_installation(directory.as_ref(), &owner.id, |record| {
            record.storage_regions.insert(inherited_reader, StorageRegion::ReferenceRead {
                custody: ReplayCustody::Local, object: None, archive_source: Some(hold.clone()),
            });
            Ok(())
        }).await.unwrap();
        super::super::release_region(directory.as_ref(), &owner.id, reader, false).await.unwrap();
        pass().await.unwrap();
        assert!(bucket.head(&retained).await.is_ok());

        finish.send(()).unwrap();
        io.close_and_wait().await;
        super::super::release_region(directory.as_ref(), &owner.id, inherited_reader, false).await.unwrap();
        pass().await.unwrap();
        assert!(matches!(bucket.head(&retained).await, Err(Error::MissingObject(_))));
    }

    #[tokio::test]
    async fn a_hosted_metadata_pack_requires_complete_version_bound_releases_and_no_checkpoint_hold() {
        use super::super::{GarbageStore, reaper_pass};
        use crate::{packing::{PACK_DIRECTORY, PackDiscovery, PackingStore}, reference::PrefixedStore};
        use rtdb_directory::Checkpoint;
        use std::sync::Arc;

        let directory = MemoryDirectory::new();
        let bucket = Arc::new(VersionedReleaseStore::default());
        let mut owner = DeploymentRecord::new("shop", "acme", vec![], 0);
        owner.checkpoint = Some(Checkpoint { block: 1, boundary: 2, source: 1, system_time: 0, root_sha256: None });
        let region = Uuid::new_v4();
        let owner = directory.create_deployment(owner).await.unwrap();
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0)).unwrap().key();
        let store = format!("{}/{root}", owner.storage_prefix());
        let prefixed = Arc::new(PrefixedStore::new(bucket.clone(), &store));
        let clock: super::super::Clock = Arc::new(|| 0);
        let packing = PackingStore::open_with_clock(prefixed.clone(), PackDiscovery::List, clock.clone()).await.unwrap();
        let garbage = GarbageStore::new(packing.clone(), clock, Some(packing));
        let target = ObjectKey::new(format!("{store}/{PACK_DIRECTORY}/b00.pack")).unwrap();
        let successor = ObjectKey::new(format!("{store}/{PACK_DIRECTORY}/b01.pack")).unwrap();
        let catalog = ObjectKey::new("blocks/b00.binpb").unwrap();
        let table = ObjectKey::new("tables/t/blocks/b00.binpb").unwrap();
        let relative_pack = ObjectKey::new(format!("{PACK_DIRECTORY}/b00.pack")).unwrap();
        let markers = ObjectKey::new(GARBAGE_DIRECTORY).unwrap();
        for key in [&table, &catalog] { garbage.put(key, Bytes::from_static(b"old")).await.unwrap(); }
        garbage.put(&ObjectKey::new("blocks/b01.binpb").unwrap(), Bytes::from_static(b"new")).await.unwrap();
        let target_contents = bucket.get(&target).await.unwrap();
        let original = bucket.version(&target);
        let pass = || reap(directory.as_ref(), bucket.as_ref(), bucket.as_ref(), &owner, 0, None, None);

        // Path-only and another version's member release cannot authorize this pack.
        garbage.write_release(&table, None, None).await.unwrap();
        garbage.write_release(&catalog, None, None).await.unwrap();
        for result in reaper_pass(bucket.as_ref(), std::slice::from_ref(&owner), 0, Duration::ZERO).await.unwrap() {
            result.unwrap();
        }
        assert!(bucket.head(&target).await.is_ok(), "hosted packs require exact reaping");
        let different = PhysicalObjectVersion { version: "different".into(), ..original.clone() };
        garbage.write_release_record(&table, None, Some(&different), Some(&relative_pack), None).await.unwrap();
        garbage.delete(&catalog).await.unwrap();
        for _ in 0..2 { pass().await.unwrap(); }
        assert!(bucket.head(&target).await.is_ok(), "every pack member needs a release for the same version");

        garbage.delete(&table).await.unwrap();
        crate::admin::update_installation(directory.as_ref(), &owner.id, |current| {
            current.storage_regions.insert(region, StorageRegion::Local {
                archive: None, checkpoint_suffix: None, repair_archive: None, tracks_ordered_index_pages: false,
                checkpoint_metadata_from: Some(0), checkpoint_replay_from: None,
            });
            Ok(())
        }).await.unwrap();
        assert_eq!(pass().await.expect_err("a checkpoint metadata hold protects its pack").code, "rtdb.multi/exact-retirement");
        assert_eq!(bucket.get(&target).await.unwrap(), target_contents);
        super::super::release_region(directory.as_ref(), &owner.id, region, false).await.unwrap();
        pass().await.unwrap();
        assert!(matches!(bucket.head(&target).await, Err(Error::MissingObject(_))));
        assert!(bucket.head(&successor).await.is_ok());

        // The remaining markers can settle the absent old version even after a
        // later version is visible at the same physical path.
        bucket.put(&target, target_contents.clone()).await.unwrap();
        bucket.versions.lock().insert(target.clone(), "replacement".into());
        for _ in 0..4 { pass().await.unwrap(); }
        assert_eq!(bucket.get(&target).await.unwrap(), target_contents);
        assert_eq!(bucket.version(&target).version, "replacement");
        assert_eq!(prefixed.list(Some(&markers)).await.unwrap().len(), 2, "only the legacy path records remain");
        assert!(directory.lineage_state(&owner.lineage).await.unwrap().retirement().is_none());
    }

    #[tokio::test]
    async fn terminal_metadata_pack_requires_every_member_of_the_same_publication() {
        use rtdb_types::S3TombstoneCondition;

        struct Provider<'a> {
            bucket: &'a MemoryStore,
            pack: ObjectKey,
            publication: TerminalObjectPublication,
        }
        #[async_trait]
        impl RetirementProvider for Provider<'_> {
            async fn available(&self, _: &ObjectKey, _: &PhysicalObjectVersion) -> Result<bool> { unreachable!() }
            async fn absent(&self, _: &ObjectKey, _: &PhysicalObjectVersion) -> Result<bool> { unreachable!() }
            async fn erase(&self, _: &ObjectKey, _: &PhysicalObjectVersion) -> Result<()> { unreachable!() }
            async fn terminal_pack_index(&self, key: &ObjectKey, expected: &TerminalObjectPublication) -> Result<Option<PackIndex>> {
                if key != &self.pack || expected != &self.publication {
                    return Ok(None);
                }
                Ok(Some(observe_pack_index(self.bucket, key).await?.value))
            }
            async fn terminal_checkpoint_matches(&self, key: &ObjectKey, checkpoint: Checkpoint, cipher: &Cipher) -> Result<bool> {
                let index = observe_pack_index(self.bucket, key).await?.value;
                let root = block_key(BlockIndex(checkpoint.block))?;
                let Some(entry) = index.entries.get(&root) else { return Ok(false); };
                let sealed = self.bucket.get_range(key, entry.offset..entry.offset + entry.length).await?;
                Ok(checkpoint.root_sha256 == Some(rtdb_format::catalog::publication_digest(&cipher.open(&root, &sealed)?)))
            }
            async fn terminal_targets(&self, key: &ObjectKey, release: &ObjectKey, publication: &TerminalObjectPublication)
                -> Result<Option<[S3TombstoneTarget; 2]>>
            {
                Ok((key == &self.pack && publication == &self.publication).then(|| {
                    let target = |key, etag| S3TombstoneTarget {
                        namespace: publication.namespace.clone(), key, condition: S3TombstoneCondition::Matches(etag),
                    };
                    [target(publication.key.clone(), publication.etag.clone()), target(release.to_string(), r#""release""#.into())]
                }))
            }
            async fn retire_tombstones(&self, _: &TombstoneJournal<'_>, objects: [(&ObjectKey, &S3TombstoneTarget); 2]) -> Result<()> {
                for (key, _) in objects { self.bucket.delete(key).await?; }
                Ok(())
            }
        }

        let directory = MemoryDirectory::new();
        let bucket = MemoryStore::default();
        let allocation = Uuid::new_v4();
        let mut owner = DeploymentRecord::new("shop", "acme", vec![], 0);
        owner.storage_namespaces = Some(StorageNamespaces { objects: allocation, keys: [0; 32] });
        let key_dir = tempfile::tempdir().unwrap();
        let keys = crate::keys::LocalKeyService::new(key_dir.path()).unwrap();
        let material = keys.key(&owner.key_id()).await.unwrap();
        let cipher = Cipher::new(&material).unwrap();
        let root_bytes = bytes::Bytes::from(rtdb_format::catalog::BlockCatalog::from_proto(rtdb_format::block::proto::Block {
            block_index: Some(1), ..Default::default()
        }).unwrap().encode());
        owner.checkpoint = Some(Checkpoint { block: 1, boundary: 2, source: 1, system_time: 0,
            root_sha256: Some(rtdb_format::catalog::publication_digest(&root_bytes)) });
        let owner = directory.create_deployment(owner).await.unwrap();
        let store = format!("{}/v06", owner.storage_prefix());
        let full = |relative| ObjectKey::new(format!("{store}/{relative}")).unwrap();
        let pack = "~packs/b00.pack";
        let older = full(pack);
        let newer = full("~packs/b01.pack");
        let catalog = "blocks/b00.binpb";
        let table = "tables/t/blocks/b00.binpb";
        let contents = |members: &[&str]| crate::packing::encode_pack(
            &members.iter().map(|name| (name.to_string(), Bytes::from_static(b"metadata"))).collect(), Vec::new(), 0,
        ).0;
        bucket.put(&older, contents(&[catalog, table])).await.unwrap();
        let successor = |bytes: &[u8]| crate::packing::encode_pack(
            &BTreeMap::from([("blocks/b01.binpb".into(), cipher.seal("blocks/b01.binpb", bytes).unwrap())]), Vec::new(), 0,
        ).0;
        bucket.put(&newer, successor(b"unpublished root")).await.unwrap();
        let publication = TerminalObjectPublication {
            namespace: S3ObjectNamespace { bucket: "cell".into(), region: None, endpoint: None },
            allocation, key: older.to_string(), etag: r#""pack""#.into(),
        };
        let provider = Provider { bucket: &bucket, pack: older.clone(), publication: publication.clone() };
        let marker = |member: &str, terminal: TerminalObjectPublication| serde_json::to_vec(&StoredRelease::Terminal {
            record: super::super::GarbageRecord { key: member.into(), released_at_ms: 0, trie_floor: None },
            terminal, pack: Some(pack.into()),
        }).unwrap().into();
        bucket.put(&full("~garbage/root"), marker(catalog, publication.clone())).await.unwrap();
        bucket.put(&full("~garbage/stale"), marker(table, TerminalObjectPublication { etag: r#""stale""#.into(), ..publication.clone() })).await.unwrap();
        let pass = || reap(directory.as_ref(), &bucket, &provider, &owner, 0, None, Some(&keys));
        pass().await.unwrap();
        assert!(bucket.head(&older).await.is_ok());

        bucket.put(&full("~garbage/table"), marker(table, publication)).await.unwrap();
        pass().await.unwrap();
        assert!(bucket.head(&older).await.is_ok());
        bucket.delete(&newer).await.unwrap();
        bucket.put(&newer, successor(&root_bytes)).await.unwrap();

        let reader = Uuid::new_v4();
        crate::admin::update_installation(directory.as_ref(), &owner.id, |current| {
            current.storage_regions.insert(reader, StorageRegion::Local {
                archive: None, checkpoint_suffix: None, repair_archive: None, tracks_ordered_index_pages: false,
                checkpoint_metadata_from: Some(0), checkpoint_replay_from: None,
            });
            Ok(())
        }).await.unwrap();
        assert_eq!(pass().await.unwrap_err().code, changed().code);
        assert!(bucket.head(&older).await.is_ok());
        super::super::release_region(directory.as_ref(), &owner.id, reader, false).await.unwrap();
        pass().await.unwrap();
        assert!(matches!(bucket.head(&older).await, Err(Error::MissingObject(_))));
        assert!(bucket.head(&newer).await.is_ok());
    }

    #[tokio::test]
    async fn the_remote_reaper_keeps_its_exact_reservation_until_both_versions_settle() {
        let data = tempfile::tempdir().unwrap();
        MemoryDirectory::initialize(data.path()).unwrap();
        let directory = MemoryDirectory::open(data.path()).unwrap();
        let media = StorageNamespaces { objects: Uuid::new_v4(), keys: [0; 32] };
        let mut owner = DeploymentRecord::new("shop", "test", Vec::new(), 0);
        owner.storage_namespaces = Some(media);
        let owner = directory.create_deployment(owner).await.unwrap();
        let residence = owner.storage_prefix();
        let root = rtdb_s3::LEGACY_CELL_ROOT;
        let object = format!("{residence}/v06/tables/t/data/l01-rc-b00.arrow");
        let marker = format!("{residence}/v06/~garbage/release");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let mut config = S3Config::new("fixture");
        config.endpoint = Some(endpoint.clone());
        config.region = Some("us-east-1".into());
        config.credentials = Some(S3Credentials { access_key: "test".into(), secret_key: "test-secret".into() });
        config.path_style_access_enabled = true;
        let inventory = crate::deployment::CellStorage::s3_inventory(&config).await.unwrap();

        let namespace = S3ObjectNamespace { bucket: "fixture".into(), endpoint: Some(format!("{endpoint}/")), region: config.region };
        let version = |key: &str, version: &str| PhysicalObjectVersion {
            namespace: namespace.clone(), key: format!("{root}/{}", key.replace('~', "%7E")), version: version.into(),
        };
        let publication = version(&object, "released-data");
        let release_version = version(&marker, "release-record");
        let key = publication.key.clone();
        let queue = release_version.key.clone();
        let retirement = LineageRetirement { token: Uuid::new_v4(), source: residence,
            work: RetirementWork::S3 { deployment: owner.id.clone(), target: object.clone(),
                release: marker.clone(), version: publication, release_version },
        };
        let mut fenced = owner.clone();
        fenced.storage_regions.insert(retirement.token, StorageRegion::ExactRetirement { object, release: Some(marker) });
        let fenced = directory.replace_deployment(fenced, owner.version).await.unwrap().applied().unwrap();
        directory.change_lineage(&owner.lineage, LineageChange::ReserveRetirement {
            retirement: retirement.clone(), observed_references: 0, generation: 0,
        }).await.unwrap();
        let erasing = directory.authorize_owned_retirement(&owner.id, fenced.version, retirement.clone()).await.unwrap();
        let server = tokio::spawn(async move {
            let entry = |key: &str, version: &str, latest: bool| format!(
                "<Version><Key>{key}</Key><VersionId>{version}</VersionId><IsLatest>{latest}</IsLatest></Version>");
            let current = entry(&key, "released-data", true);
            let replacement = entry(&key, "replacement", true);
            for (method, path, version, body) in [
                ("GET", &key, "", current.clone()),
                ("DELETE", &key, "released-data", String::new()),
                ("GET", &key, "", current),
                ("GET", &key, "", format!("{replacement}{}", entry(&key, "released-data", false))),
                ("DELETE", &key, "released-data", String::new()),
                ("GET", &key, "", replacement.clone()),
                ("GET", &queue, "", entry(&queue, "release-record", true)),
                ("DELETE", &queue, "release-record", String::new()),
                ("GET", &queue, "", entry(&queue, "release-record", true)),
                ("GET", &key, "", replacement),
                ("GET", &queue, "", format!("{}{}", entry(&queue, "new-record", true), entry(&queue, "release-record", false))),
                ("DELETE", &queue, "release-record", String::new()),
                ("GET", &queue, "", entry(&queue, "new-record", true)),
            ] {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let mut request = line.split_whitespace();
                assert_eq!(request.next(), Some(method));
                let uri: http::Uri = request.next().unwrap().parse().unwrap();
                let query: BTreeMap<_, _> = uri.query().unwrap().split('&').filter_map(|pair| pair.split_once('=')).collect();
                if method == "DELETE" {
                    assert_eq!(uri.path(), format!("/fixture/{}", path.replace('%', "%25")));
                    assert_eq!(query.get("versionId").copied(), Some(version), "only the authorized version can be removed");
                } else {
                    let raw_prefix = query["prefix"];
                    assert_eq!(raw_prefix.replace("%2F", "/").replace("%25", "%"), *path);
                }
                while line != "\r\n" {
                    line.clear();
                    assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
                }
                let status = if method == "DELETE" { "204 No Content" } else { "200 OK" };
                let body = if method == "DELETE" { body } else { format!("<ListVersionsResult><IsTruncated>false</IsTruncated>{body}</ListVersionsResult>") };
                reader.get_mut().write_all(format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()
                ).as_bytes()).await.unwrap();
            }
        });
        let bucket = MemoryStore::default();
        let first = finish(directory.as_ref(), &bucket, &inventory, &owner.lineage, &erasing, &retirement, None, 0).await.unwrap_err();
        assert_eq!(first.message.as_deref(), Some("The retired object version is still present"),
            "a deletion reply without absence cannot settle the retirement");
        assert_eq!(directory.deployment(&owner.id).await.unwrap().unwrap(), fenced);
        assert_eq!(directory.lineage_state(&owner.lineage).await.unwrap().open_generation(), None);
        drop(directory);

        let directory = MemoryDirectory::open(data.path()).unwrap();
        let state = directory.lineage_state(&owner.lineage).await.unwrap();
        let absent = finish(directory.as_ref(), &bucket, &inventory, &owner.lineage, &state, &retirement, None, 0).await
            .expect_err("the release marker needs its own absence evidence");
        assert_eq!(absent.message.as_deref(), Some("The retired object version is still present"));
        assert_eq!(directory.deployment(&owner.id).await.unwrap().unwrap(), fenced);
        assert_eq!(directory.lineage_state(&owner.lineage).await.unwrap().open_generation(), None);

        finish(directory.as_ref(), &bucket, &inventory, &owner.lineage, &state, &retirement, None, 0).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), server).await.unwrap().unwrap();
        assert!(!directory.deployment(&owner.id).await.unwrap().unwrap().storage_regions.contains_key(&retirement.token),
            "settlement removes the physical owner's fence");
        assert_eq!(directory.lineage_state(&owner.lineage).await.unwrap().open_generation(), Some(1));
    }
    #[tokio::test]
    async fn terminal_retirement_waits_for_reads_and_recovery_and_resumes_its_upload_after_restart() {
        use rtdb_types::S3TombstoneCondition;

        struct Provider {
            publication: TerminalObjectPublication,
            observations: [S3TombstoneTarget; 2],
            token: parking_lot::Mutex<Option<Uuid>>,
            calls: parking_lot::Mutex<Vec<(ObjectKey, S3TombstoneTarget)>>,
        }
        #[async_trait]
        impl RetirementProvider for Provider {
            async fn available(&self, _: &ObjectKey, _: &PhysicalObjectVersion) -> Result<bool> { unreachable!() }
            async fn absent(&self, _: &ObjectKey, _: &PhysicalObjectVersion) -> Result<bool> { unreachable!() }
            async fn erase(&self, _: &ObjectKey, _: &PhysicalObjectVersion) -> Result<()> { unreachable!() }
            async fn terminal_targets(&self, _: &ObjectKey, _: &ObjectKey, publication: &TerminalObjectPublication)
                -> Result<Option<[S3TombstoneTarget; 2]>>
            {
                Ok((publication == &self.publication).then(|| self.observations.clone()))
            }
            async fn retire_tombstones(&self, journal: &TombstoneJournal<'_>, objects: [(&ObjectKey, &S3TombstoneTarget); 2]) -> Result<()> {
                assert_eq!(*self.token.lock().get_or_insert(journal.token()), journal.token());
                let first = {
                    let mut calls = self.calls.lock();
                    let first = calls.is_empty();
                    calls.extend(objects.map(|(key, target)| (key.clone(), target.clone())));
                    first
                };
                let marker = objects[1].0;
                if first {
                    journal.admit(marker, "marker-upload").await?;
                    return Err(Anomaly::fault("fixture/lost-marker-reply", "The release marker is still pending"));
                }
                assert_eq!(journal.upload(marker).await?.as_deref(), Some("marker-upload"));
                journal.settle(marker, "marker-upload").await
            }
        }

        let data_path = tempfile::tempdir().unwrap();
        MemoryDirectory::initialize(data_path.path()).unwrap();
        let directory = MemoryDirectory::open(data_path.path()).unwrap();
        let mut owner = DeploymentRecord::new("shop", "org", Vec::new(), 0);
        let root = owner.storage_prefix();
        let logical = "tables/t/data/l01-rc-b00.arrow";
        let object = ObjectKey::new(format!("{root}/v06/{logical}")).unwrap();
        let marker = ObjectKey::new(format!("{root}/v06/~garbage/release")).unwrap();
        let allocation = Uuid::new_v4();
        owner.storage_namespaces = Some(StorageNamespaces { objects: allocation, keys: [0; 32] });
        let floor = CatalogAdmissionFloor { residence: root.clone(), object_namespace: allocation, cut: CatalogCut {
            storage_version: CURRENT_STORAGE_VERSION, storage_epoch: StorageEpoch(0),
            replica_epoch: StorageEpoch(0), through: ReplicaPosition(1),
        }};
        owner.catalog_admission_floors.insert("v06".into(), floor.clone());
        owner.checkpoint = Some(rtdb_directory::Checkpoint { block: 0, boundary: 0, source: 0, system_time: 0, root_sha256: Some([0; 32]) });
        let held = directory.create_deployment(owner).await.unwrap();
        let namespace = S3ObjectNamespace { bucket: "cell".into(), endpoint: None, region: None };
        let observed = |key: &ObjectKey, tag: &str| S3TombstoneTarget {
            namespace: namespace.clone(), key: format!("cell/{}", key.as_str().replace('~', "%7E")),
            condition: S3TombstoneCondition::Matches(tag.to_owned()),
        };
        let data = observed(&object, "\"original\"");
        let release = observed(&marker, "\"release\"");
        let publication = TerminalObjectPublication { namespace, allocation, key: data.key.clone(), etag: "\"original\"".into() };
        let provider = Provider {
            publication: publication.clone(), observations: [data.clone(), release.clone()], token: Default::default(), calls: Default::default(),
        };
        let bucket = MemoryStore::default();
        let witness = StoredRelease::Terminal {
            record: super::super::GarbageRecord { key: logical.into(), released_at_ms: 1000, trie_floor: Some(floor) },
            terminal: publication,
            pack: None,
        };
        bucket.put(&object, Bytes::from_static(b"original")).await.unwrap();
        bucket.put(&marker, serde_json::to_vec(&witness).unwrap().into()).await.unwrap();
        reap(directory.as_ref(), &bucket, &provider, &held, 0, None, None).await.unwrap();
        assert!(provider.calls.lock().is_empty());
        let mut current = directory.deployment(&held.id).await.unwrap().unwrap();
        current.checkpoint = Some(rtdb_directory::Checkpoint { block: 1, boundary: 1, source: 0, system_time: 0, root_sha256: Some([1; 32]) });
        current.recovery_manifest_sha256 = Some([1; 32]);
        let hold = Uuid::new_v4();
        current.storage_regions.insert(hold, StorageRegion::ReplicaReplay { first_required: 1, through: 1, custody: ReplayCustody::Local });
        let version = current.version;
        let current = directory.replace_deployment(current, version).await.unwrap().applied().unwrap();
        reap(directory.as_ref(), &bucket, &provider, &current, 0, None, None).await.unwrap();
        assert!(provider.calls.lock().is_empty());
        let mut current = directory.deployment(&held.id).await.unwrap().unwrap();
        current.storage_regions.insert(hold, StorageRegion::ReferenceRead { custody: ReplayCustody::Local, archive_source: None, object: Some(object.to_string()) });
        let version = current.version;
        let held = directory.replace_deployment(current, version).await.unwrap().applied().unwrap();
        assert_eq!(reap(directory.as_ref(), &bucket, &provider, &held, 0, None, None).await.unwrap_err().code, changed().code);
        assert!(provider.calls.lock().is_empty());
        let after_blocked_attempt = directory.lineage_state(&held.lineage).await.unwrap().open_generation().unwrap();
        let mut unheld = directory.deployment(&held.id).await.unwrap().unwrap();
        unheld.storage_regions.remove(&hold);
        let version = unheld.version;
        let owner = directory.replace_deployment(unheld, version).await.unwrap().applied().unwrap();

        let error = reap(directory.as_ref(), &bucket, &provider, &owner, 0, None, None).await.unwrap_err();
        assert_eq!(error.code, "fixture/lost-marker-reply");
        let owner_fenced = directory.deployment(&owner.id).await.unwrap().unwrap();
        let state = directory.lineage_state(&owner.lineage).await.unwrap();
        assert!(state.exact_erasure_authorized());
        let retirement = state.retirement().unwrap().clone();
        assert!(matches!(owner_fenced.storage_regions.get(&retirement.token), Some(StorageRegion::ExactRetirement { object: key, release: Some(path) })
            if key == object.as_str() && path == marker.as_str()));
        assert_eq!(state.tombstone_upload(retirement.token, marker.as_str()).unwrap(), Some("marker-upload"));
        assert!(directory.settle_owned_version(&owner.id, owner_fenced.version, retirement.clone()).await.is_err());
        drop(directory);

        let directory = MemoryDirectory::open(data_path.path()).unwrap();
        reap(directory.as_ref(), &bucket, &provider, &owner_fenced, 0, None, None).await.unwrap();
        assert_eq!(*provider.calls.lock(), [(object.clone(), data.clone()), (marker.clone(), release.clone()), (object, data), (marker, release)]);
        assert_eq!(directory.lineage_state(&owner.lineage).await.unwrap().open_generation(), Some(after_blocked_attempt + 1));
        assert!(directory.deployment(&owner.id).await.unwrap().unwrap().storage_regions.is_empty());
    }

}
