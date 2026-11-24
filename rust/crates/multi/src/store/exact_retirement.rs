use std::collections::BTreeSet;

use async_trait::async_trait;
use bytes::Bytes;
use rtdb_directory::{Checkpoint, DeploymentRecord, Directory, ExactRetirementGrant, Lifecycle, LineageChange, LineageRetirement, LineageState, RetirementWork, StorageRegion, Version};
use rtdb_format::keys::{CURRENT_STORAGE_VERSION, StorageRoot, block_key};
use rtdb_storage::{Error, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, PhysicalObjectPublication, PhysicalObjectVersion, Result, S3TombstoneTarget, ServingPublication, StorageEpoch, TerminalObjectPublication};
use uuid::Uuid;

use super::{GARBAGE_DIRECTORY, StoredRelease, can_reap, checkpoint_metadata_block, permits_release, recovery_covers_release};
use crate::checkpoint_retention::checkpoint_pack_collectable;
use rtdb_storage::{IoRegion, sealing::{PUBLICATION_ID_BYTES, PublicationSealing}};
use crate::keys::{KeyService, UnitScope};
use crate::packing::{PackIndex, block_pack, observe_pack_index, pack_block};
use crate::reclamation_decision::matches_object_fence;

const PUBLICATION_HEADER: u64 = (PUBLICATION_ID_BYTES + crate::cipher::object::SEALED_HEADER_BYTES) as u64;
use crate::reference::{Base, References, stores};

pub(super) mod tombstones;
use tombstones::TombstoneJournal;

const MAXIMUM_RELEASE_BYTES: u64 = 32 << 10;
const MAXIMUM_PACK_SLOT_PLAN_BYTES: u64 = 16 << 20;
const EDGE_COMPARISON_PAGE: u64 = 1 << 20;

struct ReleaseWitness {
    record: super::GarbageRecord,
    version: Option<PhysicalObjectVersion>,
    terminal: Option<TerminalObjectPublication>,
    pack: Option<String>,
    release_version: Option<PhysicalObjectVersion>,
}

impl ReleaseWitness {
    fn target_name(&self) -> Option<&str> {
        match self.pack.as_deref() {
            Some(pack) if (self.version.is_some() || self.terminal.is_some()) && self.record.trie_floor.is_none()
                && pack_block(pack).is_some()
                && checkpoint_metadata_block(&self.record.key).is_some() => Some(pack),
            None if self.record.trie_floor.is_some() => Some(&self.record.key),
            _ => None,
        }
    }

    fn permits(&self, owner: &DeploymentRecord, store: &str) -> bool {
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
    let (record, version, terminal, pack) = match stored {
        StoredRelease::Exact { record, version, pack } => (record, Some(version), None, pack),
        StoredRelease::Terminal { record, terminal, pack } => (record, None, Some(terminal), pack),
        StoredRelease::Path(record) => (record, None, None, None),
    };
    Ok(Some(ReleaseWitness { record, version, terminal, pack, release_version: observed.version }))
}

/// A path-only release can withdraw a fork edge when an exact copy
/// already takes precedence. Physical retirement of either copy remains ordered
/// by the publication's own release, recovery dependencies and census.
pub(super) async fn withdraw_inherited_release(
    directory: &dyn Directory, bucket: &dyn ObjectStore, candidate: &DeploymentRecord,
    task: &rtdb_directory::HostedTask,
) -> Result<()> {
    if candidate.parent.is_none() || candidate.storage_generation.is_some() || !can_reap(candidate) { return Ok(()); }
    let storage_root = crate::archive::checkpoint_storage_root()?;
    let root = format!("{}/{storage_root}", candidate.storage_prefix());
    let base = Base::load(bucket, &root).await?;
    if base.publications.is_empty() { return Ok(()); }
    let markers = ObjectKey::new(format!("{root}/{GARBAGE_DIRECTORY}"))?;
    for marker in bucket.list(Some(&markers)).await? {
        let Some(witness) = read_release(bucket, &marker.key).await? else { continue; };
        if witness.version.is_some() || witness.terminal.is_some() || witness.pack.is_some() { continue; }
        for (name, source_root) in &base.objects {
            let pack = name != &witness.record.key;
            if source_root == &root || pack && !crate::packing::is_pack_key(name) { continue; }
            let Some(original) = base.publications.get(name) else { continue; };
            let source = ObjectKey::new(format!("{source_root}/{name}"))?;
            let target = ObjectKey::new(format!("{root}/{name}"))?;
            let Some(replacement) = bucket.retirement_publication(&target).await? else { continue; };
            let withdrawn = super::with_source_lineage(directory, Some(&candidate.lineage), Some(task), async {
                let Some(current) = directory.deployment(&candidate.id).await?
                    .filter(|current| current.storage_prefix() == candidate.storage_prefix()
                        && current.lineage == candidate.lineage && current.org == candidate.org && can_reap(current))
                    else { return Ok(false); };
                let Some((sequence, still_selected)) = Base::load_snapshot(bucket, &root).await? else { return Ok(false); };
                if still_selected.objects.get(name) != Some(source_root) || still_selected.publications.get(name) != Some(original) {
                    return Ok(false);
                }
                let source_registered = directory.deployments().await?.into_iter().any(|owner| {
                    owner.org == current.org && owner.lineage == current.lineage
                        && source_root == &format!("{}/{storage_root}", owner.storage_prefix())
                });
                if !source_registered { return Ok(false); }
                let attempt = crate::residence_trace::enabled().then(Uuid::new_v4);
                let selection = || serde_json::json!({
                    "lineage": current.lineage, "child": current.id, "child_incarnation": current.incarnation,
                    "child_residence": root, "source_residence": source_root, "attempt": attempt,
                    "name": name, "released": witness.record.key, "original": original, "base_sequence": sequence,
                });
                crate::residence_trace::record("fork-edge-selected", || selection());
                let member = if pack {
                    if bucket.retirement_publication(&target).await?.as_ref() != Some(&replacement) {
                        return Ok(false);
                    }
                    let index = observe_pack_index(bucket, &target).await?;
                    if !observes_fork_publication(index.version.as_ref(), &replacement) {
                        return Ok(false);
                    }
                    let Some(member) = index.value.entries.get(&witness.record.key) else { return Ok(false); };
                    Some(member.clone())
                } else {
                    None
                };
                let Some((extent, digest)) = same_fork_body(bucket, (&source, original), (&target, &replacement)).await?
                    else { return Ok(false); };
                let observation = || {
                    let mut fields = selection();
                    fields["replacement"] = serde_json::json!(replacement);
                    fields["pack_member"] = serde_json::json!(member.as_ref().map(|range| serde_json::json!({
                        "key": witness.record.key, "range": range,
                    })));
                    fields["extent"] = serde_json::json!(extent);
                    fields["sha256"] = serde_json::json!(digest);
                    fields
                };
                crate::residence_trace::record("fork-replacement-verified", || observation());
                if !pack && witness.record.trie_floor.is_some() {
                    let record = witness.record.clone();
                    let release = match &replacement {
                        PhysicalObjectPublication::Version(version) => StoredRelease::Exact { record, version: version.clone(), pack: None },
                        PhysicalObjectPublication::Terminal(terminal) => StoredRelease::Terminal { record, terminal: terminal.clone(), pack: None },
                    };
                    let bytes = Bytes::from(serde_json::to_vec(&release).expect("a release serializes"));
                    let id = ring::digest::digest(&ring::digest::SHA256, &bytes);
                    let id = uuid::Uuid::from_bytes(id.as_ref()[..16].try_into().expect("16 digest bytes"));
                    let timestamp = witness.record.released_at_ms;
                    let marker = ObjectKey::new(format!("{markers}/{timestamp:020}-{}{suffix}", id.simple(), suffix = super::TRIE_RETIREMENT_RELEASE))?;
                    bucket.put(&marker, bytes).await?;
                }
                let sequence = still_selected.forget(bucket, &root, &BTreeSet::from([name.clone()])).await?;
                if let Some(sequence) = sequence {
                    crate::residence_trace::record("fork-edge-withdrawn", || {
                        let mut fields = observation();
                        fields["sequence"] = serde_json::json!(sequence);
                        fields
                    });
                }
                Ok(true)
            }).await?;
            if withdrawn { return Ok(()); }
        }
    }
    Ok(())
}

fn observes_fork_publication(version: Option<&PhysicalObjectVersion>, publication: &PhysicalObjectPublication) -> bool {
    match publication {
        PhysicalObjectPublication::Version(expected) => version == Some(expected),
        PhysicalObjectPublication::Terminal(_) => version.is_none(),
    }
}

async fn same_fork_body(bucket: &dyn ObjectStore,
    (source, original): (&ObjectKey, &PhysicalObjectPublication),
    (destination, replacement): (&ObjectKey, &PhysicalObjectPublication),
) -> Result<Option<(u64, Option<[u8; 32]>)>> {
    use crate::reference::require_publication;
    if bucket.retirement_publication(source).await?.as_ref() != Some(original)
        || bucket.retirement_publication(destination).await?.as_ref() != Some(replacement)
    {
        return Ok(None);
    }
    let source_head = bucket.observe_head(source).await?;
    let destination_head = bucket.observe_head(destination).await?;
    let extent = source_head.value.size;
    if destination_head.value.size != extent
        || !observes_fork_publication(source_head.version.as_ref(), original)
        || !observes_fork_publication(destination_head.version.as_ref(), replacement)
    {
        return Ok(None);
    }
    let mut digest = crate::residence_trace::enabled().then(|| ring::digest::Context::new(&ring::digest::SHA256));
    let mut offset = 0;
    while offset < extent {
        let end = offset.saturating_add(EDGE_COMPARISON_PAGE).min(extent);
        let source_bytes = bucket.observe_range(source, offset..end).await?;
        let destination_bytes = bucket.observe_range(destination, offset..end).await?;
        if !observes_fork_publication(source_bytes.version.as_ref(), original)
            || !observes_fork_publication(destination_bytes.version.as_ref(), replacement)
            || source_bytes.value.len() as u64 != end - offset || source_bytes.value != destination_bytes.value
        {
            return Ok(None);
        }
        if let Some(digest) = &mut digest { digest.update(&source_bytes.value); }
        offset = end;
    }
    require_publication(bucket, source, original).await?;
    require_publication(bucket, destination, replacement).await?;
    Ok(Some((extent, digest.map(|context| context.finish().as_ref().try_into().expect("SHA-256 is 32 bytes")))))
}

#[async_trait]
pub(super) trait RetirementProvider: Send + Sync {
    async fn available(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool>;
    async fn absent(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool>;
    async fn erase(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<()>;

    fn versioned_units(&self) -> Option<&rtdb_s3::VersionedInventory> { None }

    async fn terminal_targets(&self, _: &ObjectKey, _: &ObjectKey, _: &TerminalObjectPublication) -> Result<Option<[S3TombstoneTarget; 2]>> {
        Ok(None)
    }

    async fn terminal_unit_header(&self, _: &ObjectKey, _: &TerminalObjectPublication, _: u64) -> Result<Option<Bytes>> {
        Ok(None)
    }

    async fn terminal_pack_index(&self, _: &ObjectKey, _: &TerminalObjectPublication) -> Result<Option<PackIndex>> {
        Ok(None)
    }

    async fn terminal_checkpoint_matches(&self, _: &ObjectKey, _: Checkpoint, _: &PublicationSealing) -> Result<bool> {
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

    fn versioned_units(&self) -> Option<&rtdb_s3::VersionedInventory> { Some(self) }

    async fn terminal_targets(&self, key: &ObjectKey, release: &ObjectKey, publication: &TerminalObjectPublication) -> Result<Option<[S3TombstoneTarget; 2]>> {
        self.terminal_release_targets(key, release, publication).await
    }

    async fn terminal_unit_header(&self, key: &ObjectKey, expected: &TerminalObjectPublication, offset: u64) -> Result<Option<Bytes>> {
        let Some(retirement) = self.enrolled_terminal_retirement().await? else { return Ok(None); };
        let Some(observed) = retirement.observe_readable(key).await? else { return Err(changed()); };
        let target = observed.target();
        if observed.allocation() != expected.allocation || target.namespace != expected.namespace || target.key != expected.key
            || target.condition != rtdb_types::S3TombstoneCondition::Matches(expected.etag.clone())
        {
            return Err(changed());
        }
        let end = offset.checked_add(PUBLICATION_HEADER).ok_or_else(changed)?;
        retirement.read_observed_range(key, &observed, offset..end).await.map(Some)
    }

    async fn terminal_pack_index(&self, key: &ObjectKey, expected: &TerminalObjectPublication) -> Result<Option<PackIndex>> {
        let Some(provider) = self.enrolled_terminal_retirement().await? else { return Ok(None); };
        super::replay_retirement::terminal_pack_index(&provider, key, expected).await
    }

    async fn terminal_checkpoint_matches(&self, key: &ObjectKey, checkpoint: Checkpoint, cipher: &PublicationSealing) -> Result<bool> {
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
    candidate: &DeploymentRecord, advisory: Option<&References>, keys: Option<&dyn KeyService>) -> Result<()> {
    let lineage = &candidate.lineage;
    let mut state = super::recover_hosted_lineage(directory, lineage).await?;
    if let Some(retirement) = state.retirement() {
        match &retirement.work {
            RetirementWork::S3 { deployment, .. } | RetirementWork::Tombstones { deployment, .. } if *deployment == candidate.id => {
                finish(directory, bucket, provider, lineage, &state, retirement, keys).await?;
            }
            _ => {},
        }
        return Ok(());
    }
    if !can_reap(candidate) { return Ok(()); }
    if state.preparations().next().is_some() || state.open_generation().is_none() { return Ok(()); }
    let source = candidate.storage_prefix();
    for store in stores(bucket, &source).await? {
        let markers = ObjectKey::new(format!("{store}/{GARBAGE_DIRECTORY}"))?;
        let mut eligible = Vec::new();
        for marker in bucket.list(Some(&markers)).await? {
            let Some(witness) = read_release(bucket, &marker.key).await? else { continue; };
            let Some(relative) = witness.target_name().filter(|_| witness.release_version.is_some() || witness.terminal.is_some()) else { continue; };
            if !witness.permits(candidate, &store) { continue; }
            let target = ObjectKey::new(format!("{store}/{relative}"))?;
            if advisory.is_some_and(|observed| match (&witness.version, &witness.release_version) {
                (Some(version), Some(release)) =>
                    observed.has_declared_version(&target, version) || observed.has_declared_version(&marker.key, release),
                _ => observed.has_declared_reference(&target) || observed.has_declared_reference(&marker.key),
            }) { continue; }
            eligible.push((marker.key, target, witness));
        }
        let mut checked = BTreeSet::new();
        let mut checked_terminal = BTreeSet::new();
        for (marker, target, witness) in &eligible {
            let work = if let Some(publication) = &witness.terminal {
                if witness.pack.is_some()
                    && (!checked_terminal.insert((target.clone(), publication.etag.clone()))
                        || pack_release_bound(bucket, provider, target, witness, &markers).await?.is_none())
                {
                    continue;
                }
                let Some([data_tombstone, release_tombstone]) = provider.terminal_targets(target, marker, publication).await? else { continue; };
                let unit_identity = if withdrawable_loose_unit(witness) && keys.is_some() {
                    terminal_unit_identity(bucket, provider, candidate, target, witness, keys, &directory.deployments().await?).await?
                } else {
                    None
                };
                let packed_slots = if witness.pack.is_some() && keys.is_some() {
                    terminal_pack_slots(bucket, provider, candidate, target, witness, keys, &directory.deployments().await?)
                        .await?.as_ref().map(plan_digest)
                } else {
                    None
                };
                RetirementWork::Tombstones {
                    deployment: candidate.id.clone(), target: target.to_string(), release: marker.to_string(),
                    data_tombstone, release_tombstone, unit: unit_identity, packed_slots,
                }
            } else {
                let (Some(version), Some(release_version)) = (&witness.version, &witness.release_version) else { continue; };
                if witness.pack.is_some() {
                    if !checked.insert((target.clone(), version.version.clone())) { continue; }
                    // Keep an incomplete pack from repeatedly closing its lineage.
                    if pack_release_bound(bucket, provider, target, witness, &markers).await?.is_none() { continue; }
                }
                if !provider.available(target, version).await? || !provider.available(marker, release_version).await? { continue; }
                let unit_identity = if withdrawable_loose_unit(witness) && keys.is_some() {
                    versioned_unit_identity(provider, candidate, target, witness, keys, &directory.deployments().await?).await?
                } else {
                    None
                };
                let packed_slots = if witness.pack.is_some() && keys.is_some()
                    && !provider.absent(target, version).await?
                {
                    versioned_pack_slots(bucket, provider, candidate, target, witness, keys, &directory.deployments().await?)
                        .await?.as_ref().map(plan_digest)
                } else {
                    None
                };
                RetirementWork::S3 { deployment: candidate.id.clone(), target: target.to_string(),
                    release: marker.to_string(), version: version.clone(), release_version: release_version.clone(),
                    unit: unit_identity, packed_slots }
            };
            let retirement = LineageRetirement { token: Uuid::new_v4(), source: source.clone(), work };
            let reserved = directory.change_lineage(lineage, LineageChange::ReserveRetirement {
                retirement: retirement.clone(), observed_references: state.reference_revision(), generation: state.retirement_generation(),
            }).await?;
            if finish(directory, bucket, provider, lineage, &reserved, &retirement, keys).await? { return Ok(()); }
            state = directory.lineage_state(lineage).await?;
            if state.preparations().next().is_some() || state.open_generation().is_none() { return Ok(()); }
        }
    }
    Ok(())
}

async fn finish(directory: &dyn Directory, bucket: &dyn ObjectStore, provider: &dyn RetirementProvider,
    lineage: &str, state: &LineageState, retirement: &LineageRetirement, keys: Option<&dyn KeyService>) -> Result<bool> {
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
        let ready = prepare(directory, bucket, provider, lineage, retirement, &target, &release, keys).await;
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
    erase_granted(directory, bucket, provider, lineage, grant, keys).await?;
    settle(directory, lineage, deployment, retirement).await?;
    Ok(true)
}

async fn erase_granted(directory: &dyn Directory, bucket: &dyn ObjectStore, provider: &dyn RetirementProvider, lineage: &str,
    grant: ExactRetirementGrant<'_>, keys: Option<&dyn KeyService>) -> Result<()> {
    let retirement = grant.retirement();
    let withdrawal = match &retirement.work {
        RetirementWork::S3 { deployment, target, unit: Some(origin), .. }
        | RetirementWork::Tombstones { deployment, target, unit: Some(origin), .. } => {
            let keys = keys.ok_or_else(changed)?;
            let owner = directory.deployment(deployment).await?.ok_or_else(changed)?;
            let namespace = owner.storage_namespaces.ok_or_else(changed)?;
            if owner.lineage != lineage || owner.storage_prefix() != retirement.source || keys.namespace_id().await? != namespace.keys {
                return Err(changed());
            }
            let (_, logical) = target.strip_prefix(&format!("{}/", retirement.source))
                .and_then(|tail| tail.split_once('/')).ok_or_else(changed)?;
            let logical = ObjectKey::new(logical)?;
            Some((keys, UnitScope::directory_publication_slot(&owner.org, lineage, *origin, &logical)))
        }
        _ => None,
    };
    let pack_withdrawal = match &retirement.work {
        RetirementWork::S3 { deployment, packed_slots: Some(digest), .. }
        | RetirementWork::Tombstones { deployment, packed_slots: Some(digest), .. } => {
            let keys = keys.ok_or_else(changed)?;
            let owner = directory.deployment(deployment).await?.ok_or_else(changed)?;
            let plan = load_pack_slots(bucket, keys, lineage, retirement, &owner, *digest).await?;
            Some((keys, owner.org, plan))
        }
        _ => None,
    };
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
                if !provider.absent(key, publication).await? { return Err(changed()); }
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
    if let Some((keys, org, plan)) = pack_withdrawal {
        withdraw_pack_slots(keys, &org, lineage, plan).await?;
    }
    if let Some((keys, slot)) = withdrawal {
        keys.withdraw_unit_key(slot, &IoRegion::new()).await?;
    }
    Ok(())
}

#[derive(PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "publication", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum InstalledPublication {
    Version { version: PhysicalObjectVersion },
    Terminal { terminal: TerminalObjectPublication },
}

impl InstalledPublication {
    pub(crate) fn matches(&self, observed: &rtdb_types::PhysicalObjectPublication) -> bool {
        use rtdb_types::PhysicalObjectPublication;
        match (self, observed) {
            (Self::Version { version }, PhysicalObjectPublication::Version(observed)) => version == observed,
            (Self::Terminal { terminal }, PhysicalObjectPublication::Terminal(observed)) => terminal == observed,
            _ => false,
        }
    }

    /// One physical provider path has one journal name. The exact publication
    /// is bound separately so a competing grant cannot retire another version.
    pub(crate) fn withdrawal(&self) -> Result<ordered_logkeeper::ordered::ObjectWithdrawal> {
        use ring::digest::{SHA256, digest};

        let exact = serde_json::to_vec(self).map_err(|error| Error::Task(error.to_string()))?;
        let (namespace, key) = self.address();
        Ok(ordered_logkeeper::ordered::ObjectWithdrawal {
            name: installed_object_name(namespace, key)?,
            version: digest(&SHA256, &exact).as_ref().try_into().expect("SHA-256 width"),
        })
    }

    fn address(&self) -> (&rtdb_types::S3ObjectNamespace, &str) {
        match self {
            Self::Version { version } => (&version.namespace, &version.key),
            Self::Terminal { terminal } => (&terminal.namespace, &terminal.key),
        }
    }

    fn serialized_address(&self) -> Result<Vec<u8>> {
        let (namespace, key) = self.address();
        installed_object_address(namespace, key)
    }

    pub(crate) async fn available(&self, key: &ObjectKey, provider: &rtdb_s3::VersionedInventory) -> Result<bool> {
        match self {
            Self::Version { version } => {
                if !provider.owns_publication(key, version) { return Ok(false); }
                provider.exact_retirement_available(key, version).await
            }
            Self::Terminal { terminal } => {
                let Some(provider) = provider.enrolled_terminal_retirement().await? else { return Ok(false); };
                provider.publication_fence_available(key, terminal, installed_publication_token(self.withdrawal()?)).await
            }
        }
    }

    /// Called after `available` has authenticated the provider and target.
    pub(crate) async fn confirmed_absence(&self, key: &ObjectKey, provider: &rtdb_s3::VersionedInventory) -> Result<bool> {
        match self {
            Self::Version { version } => provider.exact_retirement_absent(key, version).await,
            Self::Terminal { .. } => {
                let Some(provider) = provider.enrolled_terminal_retirement().await? else { return Ok(false); };
                provider.confirms(key, installed_publication_token(self.withdrawal()?)).await
            }
        }
    }
}

pub(crate) fn installed_object_name(
    namespace: &rtdb_types::S3ObjectNamespace, key: &str,
) -> Result<ordered_logkeeper::ordered::ObjectName> {
    let address = installed_object_address(namespace, key)?;
    Ok(ring::digest::digest(&ring::digest::SHA256, &address).as_ref().try_into().expect("SHA-256 width"))
}

fn installed_object_address(namespace: &rtdb_types::S3ObjectNamespace, key: &str) -> Result<Vec<u8>> {
    serde_json::to_vec(&(namespace, key)).map_err(|error| Error::Task(error.to_string()).into())
}

fn installed_publication_token(withdrawal: ordered_logkeeper::ordered::ObjectWithdrawal) -> Uuid {
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&withdrawal.version[..16]);
    uuid::Builder::from_random_bytes(bytes).into_uuid()
}

pub(crate) struct InstalledPublicationDeletionGrant<'a> {
    key: &'a ObjectKey,
    publication: &'a InstalledPublication,
    fence: ordered_logkeeper::ordered::ObjectFence,
    placement: ordered_logkeeper::ordered::Placement,
}

impl<'a> InstalledPublicationDeletionGrant<'a> {
    pub(crate) async fn after_provider_drain(
        key: &'a ObjectKey, publication: &'a InstalledPublication, fence: ordered_logkeeper::ordered::ObjectFence,
        placement: ordered_logkeeper::ordered::Placement, census: &rtdb_storage::remote::RemoteStore,
    ) -> Result<Option<Self>> {
        let expected = publication.withdrawal()?;
        if !matches_object_fence(fence.object.name, fence.object.version, expected.name, expected.version) {
            return Err(changed());
        }
        if !census.close_read_admission(key).await? { return Ok(None); }
        Ok(Some(Self { key, publication, fence, placement }))
    }

    pub(crate) async fn settle(self, provider: &rtdb_s3::VersionedInventory) -> Result<()> {
        let observed = || serde_json::json!({
            "deployment": self.placement.deployment, "incarnation": self.placement.incarnation,
            "group": self.placement.group, "generation": self.placement.generation,
            "position": self.fence.position, "name": self.fence.object.name, "version": self.fence.object.version,
            "key": self.key.as_str(), "publication_bytes": serde_json::to_vec(self.publication).expect("publication encodes"),
            "address_bytes": self.publication.serialized_address().expect("provider address encodes"),
        });
        crate::residence_trace::record("authorize-installed-publication", observed);
        match self.publication {
            InstalledPublication::Version { version } => provider.retire_observed_version(self.key, version).await?,
            InstalledPublication::Terminal { terminal } => {
                let provider = provider.enrolled_terminal_retirement().await?.ok_or_else(changed)?;
                provider.fence_create_only_publication(self.key, terminal, installed_publication_token(self.fence.object)).await?;
            }
        }
        crate::residence_trace::record("remove-installed-publication", observed);
        Ok(())
    }
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
    target: &ObjectKey, witness: &ReleaseWitness, markers: &ObjectKey) -> Result<Option<PackReadiness>> {
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
                && (member.release_version.is_some() || member.terminal.is_some())
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

// These release classes are always loose, including after a reference is relocated.
fn withdrawable_loose_unit(witness: &ReleaseWitness) -> bool {
    let logical = &witness.record.key;
    witness.pack.is_none() && witness.record.trie_floor.is_some()
        && (rtdb_format::keys::is_catalog_retirement_file(logical) || rtdb_format::keys::is_receipt_l0_file(logical))
}

async fn terminal_unit_identity(
    bucket: &dyn ObjectStore, provider: &dyn RetirementProvider, owner: &DeploymentRecord,
    target: &ObjectKey, witness: &ReleaseWitness, keys: Option<&dyn KeyService>, deployments: &[DeploymentRecord],
) -> Result<Option<ServingPublication>> {
    let Some(publication) = &witness.terminal else { return Ok(None); };
    if !withdrawable_loose_unit(witness) { return Ok(None); }
    let logical = &witness.record.key;
    let Some(keys) = keys else { return Ok(None); };
    let Some(header) = provider.terminal_unit_header(target, publication, 0).await? else { return Ok(None); };
    let cipher = super::directory_publications(keys, owner, IoRegion::new())?;
    let logical = ObjectKey::new(logical)?;
    if !cipher.applies(&logical) { return Err(changed()); }
    let (_, origin) = cipher.header(&logical, header).await?;

    // A fork may keep the original ciphertext in another residence. The
    // last exact release, including a relocated release, owns withdrawal.
    for peer in deployments.iter().filter(|peer| peer.org == owner.org && peer.lineage == owner.lineage) {
        for store in stores(bucket, &peer.storage_prefix()).await? {
            let copy = ObjectKey::new(format!("{store}/{logical}"))?;
            if &copy == target { continue; }
            let size = match bucket.head_fresh(&copy).await {
                Ok(object) => object.size,
                Err(Error::MissingObject(_)) => continue,
                Err(error) => return Err(error.into()),
            };
            if size < PUBLICATION_HEADER { return Err(changed()); }
            let prefix = bucket.observe_range(&copy, 0..PUBLICATION_ID_BYTES as u64).await?;
            let copy_origin = ServingPublication::from_prefix(&prefix.value).ok_or_else(changed)?;
            if super::publication_retirement::is_serving_alias(copy_origin, origin) { return Ok(None); }
        }
    }
    Ok(Some(origin))
}

pub(super) fn pack_plan_key(token: Uuid) -> Result<ObjectKey> {
    Ok(ObjectKey::new(format!("~serving-slot-retirements/{token}"))?)
}

pub(super) async fn load_pack_slots(
    bucket: &dyn ObjectStore, keys: &dyn KeyService, lineage: &str,
    retirement: &LineageRetirement, owner: &DeploymentRecord, digest: [u8; 32],
) -> Result<Vec<(String, ServingPublication)>> {
    let namespace = owner.storage_namespaces.ok_or_else(changed)?;
    if owner.lineage != lineage || owner.storage_prefix() != retirement.source
        || keys.namespace_id().await? != namespace.keys
    {
        return Err(changed());
    }
    let key = pack_plan_key(retirement.token)?;
    let size = bucket.head_fresh(&key).await?.size;
    if size > MAXIMUM_PACK_SLOT_PLAN_BYTES { return Err(changed()); }
    let bytes = bucket.get_fresh_exact(&key, size as usize).await?;
    if plan_digest(&bytes) != digest { return Err(changed()); }
    serde_json::from_slice(&bytes).map_err(|_| changed())
}

pub(super) async fn withdraw_pack_slots(
    keys: &dyn KeyService, org: &str, lineage: &str, plan: Vec<(String, ServingPublication)>,
) -> Result<()> {
    use futures::{StreamExt, TryStreamExt, stream};
    let io = IoRegion::new();
    let withdrawals = plan.into_iter().map(|(name, unit)| {
        let io = &io;
        async move {
            let name = ObjectKey::new(name)?;
            keys.withdraw_unit_key(UnitScope::directory_publication_slot(org, lineage, unit, &name), io).await
        }
    });
    stream::iter(withdrawals).buffer_unordered(16).try_collect::<Vec<()>>().await?;
    Ok(())
}

pub(super) fn plan_digest(bytes: &Bytes) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, bytes).as_ref().try_into().expect("SHA-256 width")
}

async fn terminal_pack_slots(
    bucket: &dyn ObjectStore, provider: &dyn RetirementProvider, owner: &DeploymentRecord,
    target: &ObjectKey, witness: &ReleaseWitness, keys: Option<&dyn KeyService>, deployments: &[DeploymentRecord],
) -> Result<Option<Bytes>> {
    let (Some(publication), Some(keys), Some(_)) = (&witness.terminal, keys, &witness.pack) else { return Ok(None); };
    let Some(index) = provider.terminal_pack_index(target, publication).await? else { return Ok(None); };
    let io = IoRegion::new();
    let sealing = super::directory_publications(keys, owner, io.clone())?;
    let census = super::publication_retirement::PublicationCensus::new(keys, deployments, &io);
    let mut retiring = Vec::new();
    for (name, entry) in &index.entries {
        let logical = ObjectKey::new(name)?;
        if !sealing.applies(&logical) { continue; }
        if entry.length < PUBLICATION_HEADER { return Err(changed()); }
        let Some(header) = provider.terminal_unit_header(target, publication, entry.offset).await? else { return Ok(None); };
        let (_, origin) = sealing.header(&logical, header).await?;
        if !census.has_another_copy(bucket, owner, target, &logical, origin, true).await? {
            retiring.push((name, origin));
        }
    }
    let bytes = serde_json::to_vec(&retiring).map_err(|_| changed())?;
    if bytes.len() as u64 > MAXIMUM_PACK_SLOT_PLAN_BYTES { return Err(changed()); }
    Ok(Some(Bytes::from(bytes)))
}

async fn versioned_pack_slots(
    bucket: &dyn ObjectStore, provider: &dyn RetirementProvider, owner: &DeploymentRecord,
    target: &ObjectKey, witness: &ReleaseWitness, keys: Option<&dyn KeyService>, deployments: &[DeploymentRecord],
) -> Result<Option<Bytes>> {
    use super::replay_retirement::{ObservedPack, PackStorage};

    let (Some(version), Some(keys), Some(inventory), Some(_)) =
        (&witness.version, keys, provider.versioned_units(), &witness.pack)
    else { return Ok(None); };
    let observed = match ObservedPack::raw(bucket, target).await {
        Ok(observed) if matches!(&observed.storage, PackStorage::Raw { version: Some(exact), .. } if exact == version) => observed,
        Ok(_) | Err(Error::MissingObject(_)) if provider.absent(target, version).await? => return Ok(None),
        Ok(_) | Err(Error::MissingObject(_)) => return Err(changed()),
        Err(error) => return Err(error.into()),
    };
    let io = IoRegion::new();
    let census = super::publication_retirement::PublicationCensus::new(keys, deployments, &io);
    census.pack_plan(bucket, owner, &observed, inventory).await.map(Some)
}

async fn versioned_unit_identity(
    provider: &dyn RetirementProvider, owner: &DeploymentRecord,
    target: &ObjectKey, witness: &ReleaseWitness, keys: Option<&dyn KeyService>, deployments: &[DeploymentRecord],
) -> Result<Option<ServingPublication>> {
    let Some(version) = &witness.version else { return Ok(None); };
    if !withdrawable_loose_unit(witness) { return Ok(None); }
    let (Some(keys), Some(inventory)) = (keys, provider.versioned_units()) else { return Ok(None); };
    if !inventory.data_versions(target).await?.contains(version) { return Ok(None); }
    let logical = ObjectKey::new(&witness.record.key)?;
    let cipher = super::directory_publications(keys, owner, IoRegion::new())?;
    if !cipher.applies(&logical) { return Err(changed()); }
    let header = inventory.read_version_prefix(target, version, PUBLICATION_HEADER).await?;
    let (_, origin) = cipher.header(&logical, header).await?;

    for peer in deployments.iter().filter(|peer| peer.org == owner.org && peer.lineage == owner.lineage) {
        let residence = ObjectKey::new(peer.storage_prefix())?;
        for store in inventory.child_directories(&residence).await? {
            let copy = ObjectKey::new(format!("{residence}/{store}/{logical}"))?;
            for observed in inventory.data_versions(&copy).await? {
                if &copy == target && &observed == version { continue; }
                let prefix = inventory.read_version_prefix(&copy, &observed, PUBLICATION_ID_BYTES as u64).await?;
                let copy_origin = ServingPublication::from_prefix(&prefix).ok_or_else(changed)?;
                if super::publication_retirement::is_serving_alias(copy_origin, origin) { return Ok(None); }
            }
        }
    }
    Ok(Some(origin))
}

async fn prepare(directory: &dyn Directory, bucket: &dyn ObjectStore, provider: &dyn RetirementProvider, lineage: &str, retirement: &LineageRetirement,
    target: &ObjectKey, release: &ObjectKey, keys: Option<&dyn KeyService>) -> Result<Option<Version>> {
    let root = ObjectKey::new(&retirement.source)?;
    let Some((store, relative)) = target.as_str().strip_prefix(&format!("{root}/")).and_then(|suffix| suffix.split_once('/'))
        else { return Err(changed()); };
    let store = format!("{root}/{store}");
    let markers = ObjectKey::new(format!("{store}/{GARBAGE_DIRECTORY}"))?;
    if !release.is_under(&markers) { return Err(changed()); }
    let Some(witness) = read_release(bucket, release).await? else { return Ok(None); };
    if witness.target_name() != Some(relative) { return Err(changed()); }
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
        let Some(ready) = pack_release_bound(bucket, provider, target, &witness, &markers).await? else { return Ok(None); };
        Some(ready)
    } else {
        None
    };
    let release_name = release.as_str().strip_prefix(&format!("{store}/")).ok_or_else(changed)?;

    let deployments = directory.deployments().await?;
    let owner = deployments.iter().find(|owner| owner.id == *deployment).ok_or_else(changed)?;
    let (observed, chosen) = match &retirement.work {
        RetirementWork::Tombstones { unit, .. } =>
            (terminal_unit_identity(bucket, provider, owner, target, &witness, keys, &deployments).await?, unit),
        RetirementWork::S3 { unit, .. } =>
            (versioned_unit_identity(provider, owner, target, &witness, keys, &deployments).await?, unit),
        _ => return Err(changed()),
    };
    if observed != *chosen { return Ok(None); }
    let (version, release_version) = match &retirement.work {
        RetirementWork::S3 { version, release_version, .. } => (Some(version), Some(release_version)),
        _ => (None, None),
    };
    if reference_retains(bucket, &deployments, lineage, &store, &[(relative, version), (release_name, release_version)]).await? {
        return Ok(None);
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
            let cipher = crate::store::directory_publications(keys, owner, IoRegion::new())?;
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

    let (expected_plan, plan) = match &retirement.work {
        RetirementWork::Tombstones { packed_slots, .. } =>
            (packed_slots, terminal_pack_slots(bucket, provider, owner, target, &witness, keys, &deployments).await?),
        RetirementWork::S3 { packed_slots, .. } =>
            (packed_slots, versioned_pack_slots(bucket, provider, owner, target, &witness, keys, &deployments).await?),
        _ => return Err(changed()),
    };
    if plan.as_ref().map(plan_digest) != *expected_plan { return Ok(None); }
    if let Some(plan) = plan { bucket.put(&pack_plan_key(retirement.token)?, plan).await?; }

    let owner = crate::admin::update_installation(directory, deployment, |record| {
        if record.lineage != lineage || record.storage_prefix() != retirement.source || !can_reap(record)
            || !witness.permits(record, &store)
        {
            return Err(changed());
        }
        if general_reference_holds(record, &[target, release]) { return Err(changed()); }
        if record.storage_regions.values().any(|region| match region {
            StorageRegion::Remote { publications, .. } => publications.values().any(|upload| {
                upload.key == target.as_str() || upload.key == release.as_str()
            }),
            _ => false,
        }) { return Err(changed()); }
        if record.storage_regions.values().any(|region| archived_source_holds(region, &[version, release_version])) {
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
    Ok(Some(owner.version))
}

const ARCHIVE_SOURCE_HELD: &str = "rtdb.multi/archive-source-held";

async fn reference_retains(bucket: &dyn ObjectStore, deployments: &[DeploymentRecord], lineage: &str,
    store: &str, files: &[(&str, Option<&PhysicalObjectVersion>)]) -> Result<bool> {
    for record in deployments.iter().filter(|record| record.lineage == lineage && record.lifecycle != Lifecycle::Deleted) {
        for referrer in stores(bucket, &record.storage_prefix()).await? {
            let base = Base::load(bucket, &referrer).await?;
            if files.iter().any(|(name, version)| match version {
                Some(version) => base.retains_version(store, name, version),
                None => base.objects.get(*name).is_some_and(|owner| owner == store),
            }) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn general_reference_holds(record: &DeploymentRecord, targets: &[&ObjectKey]) -> bool {
    record.storage_regions.values().any(|region| match region {
        StorageRegion::ReferenceRead { object, archive_source: None, .. } =>
            object.as_deref().is_none_or(|key| targets.iter().any(|target| target.as_str() == key)),
        StorageRegion::Remote { archive: Some(_), archive_source: None, .. }
        | StorageRegion::Local { archive: Some(_), .. } => true,
        _ => false,
    })
}

fn archived_source_holds(region: &StorageRegion, versions: &[Option<&PhysicalObjectVersion>]) -> bool {
    let (StorageRegion::Remote { archive_source: Some(hold), .. }
        | StorageRegion::ReferenceRead { archive_source: Some(hold), .. }) = region else { return false; };
    versions.iter().any(|version| version.is_none_or(|version| !hold.permits_retirement(version)))
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
        addresses: bool,
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
        fn s3_publication_address(&self, key: &ObjectKey) -> Option<(S3ObjectNamespace, String)> {
            self.addresses.then(|| {
                let version = self.version(key);
                (version.namespace, version.key)
            })
        }

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
        reap(directory.as_ref(), bucket.as_ref(), &inventory, &held, None, None).await.unwrap();
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
        let error = reap(directory.as_ref(), bucket.as_ref(), &inventory, &current, None, None).await.unwrap_err();
        assert_eq!(error.code, "fixture/reply-lost");
        assert!(matches!(bucket.head(&target).await, Err(Error::MissingObject(_))));
        assert!(bucket.head(&queued[0].key).await.is_ok());
        assert!(directory.lineage_state(&record.lineage).await.unwrap().exact_erasure_authorized());

        reap(directory.as_ref(), bucket.as_ref(), &inventory, &current, None, None).await.unwrap();
        assert!(bucket.list(Some(&markers)).await.unwrap().is_empty());
        assert!(directory.lineage_state(&record.lineage).await.unwrap().retirement().is_none());
    }

    #[tokio::test]
    async fn a_fork_pack_edge_withdraws_only_after_its_exact_replacement_contains_the_release() {
        let directory = MemoryDirectory::new();
        let bucket: crate::reference::Bucket = std::sync::Arc::new(VersionedReleaseStore {
            addresses: true, ..Default::default()
        });
        let mut parent = DeploymentRecord::new("shop", "acme", vec![], 0);
        parent.lifecycle = Lifecycle::Live;
        let parent = directory.create_deployment(parent).await.unwrap();
        let mut child = DeploymentRecord::new("preview", &parent.org, vec![], 0);
        child.lineage.clone_from(&parent.lineage);
        child.parent = Some(parent.id.clone());
        child.lifecycle = Lifecycle::Live;
        let child = directory.create_deployment(child).await.unwrap();
        let root = crate::archive::checkpoint_storage_root().unwrap();
        let source_root = format!("{}/{root}", parent.storage_prefix());
        let child_root = format!("{}/{root}", child.storage_prefix());
        let member = ObjectKey::new("tables/orders/blocks/b00.binpb").unwrap();
        let name = "~packs/b00.pack".to_owned();
        let source = ObjectKey::new(format!("{source_root}/{name}")).unwrap();
        let target = ObjectKey::new(format!("{child_root}/{name}")).unwrap();
        let (pack, _) = crate::packing::encode_pack(
            &BTreeMap::from([(member.to_string(), Bytes::from_static(b"member"))]), vec![], 0,
        );
        bucket.put(&source, pack.clone()).await.unwrap();
        let original = bucket.retirement_publication(&source).await.unwrap().unwrap();
        Base {
            objects: BTreeMap::from([(name.clone(), source_root)]),
            publications: BTreeMap::from([(name.clone(), original)]),
        }.initialize(bucket.as_ref(), &child_root).await.unwrap();

        let store = std::sync::Arc::new(crate::reference::PrefixedStore::new(bucket.clone(), &child_root));
        let garbage = super::super::GarbageStore::open_with_clock(store, std::sync::Arc::new(|| 0)).await.unwrap();
        garbage.write_release(&ObjectKey::new("tables/orders/blocks/b01.binpb").unwrap(), None, None).await.unwrap();
        bucket.put(&target, pack).await.unwrap();
        let task = rtdb_directory::HostedTask::new("arn:aws:ecs:us-east-1:000000000000:cluster/cell",
            "arn:aws:ecs:us-east-1:000000000000:task/cell/00000000000000000000000000000001").unwrap();
        withdraw_inherited_release(directory.as_ref(), bucket.as_ref(), &child, &task).await.unwrap();
        assert!(Base::load(bucket.as_ref(), &child_root).await.unwrap().objects.contains_key(&name));

        garbage.write_release(&member, None, None).await.unwrap();
        withdraw_inherited_release(directory.as_ref(), bucket.as_ref(), &child, &task).await.unwrap();
        assert!(Base::load(bucket.as_ref(), &child_root).await.unwrap().objects.is_empty());
        assert!(bucket.head(&source).await.is_ok());
        assert!(bucket.head(&target).await.is_ok());
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

        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, None, None).await.unwrap();
        assert_eq!(versions.attempts.load(Ordering::Relaxed), 0);
        let recovery = Uuid::new_v4();
        let parent = crate::admin::update_installation(directory.as_ref(), &parent.id, |record| {
            record.checkpoint = Some(rtdb_directory::Checkpoint { block: 1, boundary: 2, source: 1, system_time: 0, root_sha256: Some([2; 32]) });
            record.recovery_manifest_sha256 = None;
            Ok(())
        }).await.unwrap();
        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, None, None).await.unwrap();
        assert_eq!(versions.attempts.load(Ordering::Relaxed), 0);
        let parent = crate::admin::update_installation(directory.as_ref(), &parent.id, |record| {
            record.recovery_manifest_sha256 = Some([2; 32]);
            record.storage_regions.insert(recovery, StorageRegion::ReplicaReplay { first_required: 2, through: 2, custody: ReplayCustody::Local });
            Ok(())
        }).await.unwrap();
        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, None, None).await.unwrap();
        assert_eq!(versions.attempts.load(Ordering::Relaxed), 0);
        super::super::release_region(directory.as_ref(), &parent.id, recovery, false).await.unwrap();
        let parent = directory.deployment(&parent.id).await.unwrap().unwrap();

        let preparation = ForkPreparation { token: Uuid::new_v4(), generation: 0 };
        directory.change_lineage(&parent.lineage, LineageChange::PrepareHostedFork { preparation, task: None }).await.unwrap();
        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, None, None).await.unwrap();
        assert!(directory.lineage_state(&parent.lineage).await.unwrap().retirement().is_none());

        let mut child = DeploymentRecord::new("preview", &parent.org, vec![], 0);
        child.lineage.clone_from(&parent.lineage);
        child.parent = Some(parent.id.clone());
        child.storage_namespaces = parent.storage_namespaces;
        directory.stage_fork(child.clone(), preparation).await.unwrap();
        crate::reference::simulate_fork(bucket.as_ref(), &parent.storage_prefix(), &child.storage_prefix(), None).await.unwrap();
        super::super::release_region(directory.as_ref(), &child.id, preparation.token, false).await.unwrap();
        let child = directory.publish_fork(child, parent.version, Some(rtdb_directory::ForkPublication::Hosted(preparation))).await.unwrap();
        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, None, None).await.unwrap();
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
        assert_eq!(reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, None, None).await.unwrap_err().code, "rtdb.multi/exact-retirement");
        assert_eq!(versions.attempts.load(Ordering::Relaxed), 0);
        super::super::release_region(directory.as_ref(), &parent.id, reader, false).await.unwrap();

        assert_eq!(reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, None, None).await.unwrap_err().code, "fixture/provider-reply-lost");
        let authorizing = directory.lineage_state(&parent.lineage).await.unwrap();
        assert!(authorizing.exact_erasure_authorized());
        let token = authorizing.retirement().unwrap().token;
        assert!(matches!(directory.deployment(&parent.id).await.unwrap().unwrap().storage_regions.get(&token), Some(StorageRegion::ExactRetirement { .. })));
        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, None, None).await.unwrap();
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
        let pass = || reap(directory.as_ref(), bucket.as_ref(), bucket.as_ref(), &owner, None, None);
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
        let pass = || reap(directory.as_ref(), bucket.as_ref(), bucket.as_ref(), &owner, None, None);

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
            async fn terminal_checkpoint_matches(&self, key: &ObjectKey, checkpoint: Checkpoint, cipher: &PublicationSealing) -> Result<bool> {
                let index = observe_pack_index(self.bucket, key).await?.value;
                let root = block_key(BlockIndex(checkpoint.block))?;
                let Some(entry) = index.entries.get(&root) else { return Ok(false); };
                let sealed = self.bucket.get_range(key, entry.offset..entry.offset + entry.length).await?;
                Ok(checkpoint.root_sha256 == Some(rtdb_format::catalog::publication_digest(&cipher.open(&ObjectKey::new(root)?, sealed).await?)))
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
        let cipher = crate::store::directory_publications(&keys, &owner, IoRegion::new()).unwrap();
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
        let successor = |sealed| crate::packing::encode_pack(
            &BTreeMap::from([("blocks/b01.binpb".into(), sealed)]), Vec::new(), 0,
        ).0;
        let root_key = ObjectKey::new("blocks/b01.binpb").unwrap();
        let wrong = cipher.seal(&root_key, Bytes::from_static(b"unpublished root")).await.unwrap();
        bucket.put(&newer, successor(wrong)).await.unwrap();
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
        let pass = || reap(directory.as_ref(), &bucket, &provider, &owner, None, Some(&keys));
        pass().await.unwrap();
        assert!(bucket.head(&older).await.is_ok());

        bucket.put(&full("~garbage/table"), marker(table, publication)).await.unwrap();
        pass().await.unwrap();
        assert!(bucket.head(&older).await.is_ok());
        bucket.delete(&newer).await.unwrap();
        bucket.put(&newer, successor(cipher.seal(&root_key, root_bytes).await.unwrap())).await.unwrap();

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
                release: marker.clone(), version: publication, release_version, unit: None, packed_slots: None },
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
        let first = finish(directory.as_ref(), &bucket, &inventory, &owner.lineage, &erasing, &retirement, None).await.unwrap_err();
        assert_eq!(first.message.as_deref(), Some("The retired object version is still present"),
            "a deletion reply without absence cannot settle the retirement");
        assert_eq!(directory.deployment(&owner.id).await.unwrap().unwrap(), fenced);
        assert_eq!(directory.lineage_state(&owner.lineage).await.unwrap().open_generation(), None);
        drop(directory);

        let directory = MemoryDirectory::open(data.path()).unwrap();
        let state = directory.lineage_state(&owner.lineage).await.unwrap();
        let absent = finish(directory.as_ref(), &bucket, &inventory, &owner.lineage, &state, &retirement, None).await
            .expect_err("the release marker needs its own absence evidence");
        assert_eq!(absent.message.as_deref(), Some("The retired object version is still present"));
        assert_eq!(directory.deployment(&owner.id).await.unwrap().unwrap(), fenced);
        assert_eq!(directory.lineage_state(&owner.lineage).await.unwrap().open_generation(), None);

        finish(directory.as_ref(), &bucket, &inventory, &owner.lineage, &state, &retirement, None).await.unwrap();
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
        reap(directory.as_ref(), &bucket, &provider, &held, None, None).await.unwrap();
        assert!(provider.calls.lock().is_empty());
        let mut current = directory.deployment(&held.id).await.unwrap().unwrap();
        current.checkpoint = Some(rtdb_directory::Checkpoint { block: 1, boundary: 1, source: 0, system_time: 0, root_sha256: Some([1; 32]) });
        current.recovery_manifest_sha256 = Some([1; 32]);
        let hold = Uuid::new_v4();
        current.storage_regions.insert(hold, StorageRegion::ReplicaReplay { first_required: 1, through: 1, custody: ReplayCustody::Local });
        let version = current.version;
        let current = directory.replace_deployment(current, version).await.unwrap().applied().unwrap();
        reap(directory.as_ref(), &bucket, &provider, &current, None, None).await.unwrap();
        assert!(provider.calls.lock().is_empty());
        let mut current = directory.deployment(&held.id).await.unwrap().unwrap();
        current.storage_regions.insert(hold, StorageRegion::ReferenceRead { custody: ReplayCustody::Local, archive_source: None, object: Some(object.to_string()) });
        let version = current.version;
        let held = directory.replace_deployment(current, version).await.unwrap().applied().unwrap();
        assert_eq!(reap(directory.as_ref(), &bucket, &provider, &held, None, None).await.unwrap_err().code, changed().code);
        assert!(provider.calls.lock().is_empty());
        let after_blocked_attempt = directory.lineage_state(&held.lineage).await.unwrap().open_generation().unwrap();
        let mut unheld = directory.deployment(&held.id).await.unwrap().unwrap();
        unheld.storage_regions.remove(&hold);
        let version = unheld.version;
        let owner = directory.replace_deployment(unheld, version).await.unwrap().applied().unwrap();

        let error = reap(directory.as_ref(), &bucket, &provider, &owner, None, None).await.unwrap_err();
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
        reap(directory.as_ref(), &bucket, &provider, &owner_fenced, None, None).await.unwrap();
        assert_eq!(*provider.calls.lock(), [(object.clone(), data.clone()), (marker.clone(), release.clone()), (object, data), (marker, release)]);
        assert_eq!(directory.lineage_state(&owner.lineage).await.unwrap().open_generation(), Some(after_blocked_attempt + 1));
        assert!(directory.deployment(&owner.id).await.unwrap().unwrap().storage_regions.is_empty());
    }

}
