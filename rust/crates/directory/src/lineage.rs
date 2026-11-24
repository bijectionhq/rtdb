use std::collections::BTreeMap;

use rtdb_types::{Anomaly, AnomalyCategory, PhysicalObjectVersion, Result, S3TombstoneTarget, ServingPublication};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Checkpoint, DeploymentRecord, HostedTask, Lifecycle, StorageRegion, Version, next_reference_revision, reference_revision};

/// Permission to remove the exact publications in one durable retirement.
///
/// ```compile_fail
/// fn forge(retirement: &rtdb_directory::LineageRetirement) -> rtdb_directory::ExactRetirementGrant<'_> {
///     rtdb_directory::ExactRetirementGrant { retirement, revision: 0 }
/// }
/// ```
pub struct ExactRetirementGrant<'a> {
    retirement: &'a LineageRetirement,
    revision: Version,
}

impl<'a> ExactRetirementGrant<'a> {
    pub fn retirement(&self) -> &'a LineageRetirement { self.retirement }
    pub fn revision(&self) -> Version { self.revision }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageRetirement {
    pub token: Uuid,
    pub source: String,
    #[serde(flatten)]
    pub work: RetirementWork,
}

impl LineageRetirement {
    fn matches_owner(&self, owner: &DeploymentRecord) -> bool {
        let (deployment, target, release) = match &self.work {
            RetirementWork::S3 { deployment, target, release, .. } | RetirementWork::Tombstones { deployment, target, release, .. } => {
                (deployment, target, Some(release))
            }
            RetirementWork::ReplayPack { deployment, target, .. }
            | RetirementWork::ReplayPackTombstone { deployment, target, .. } => (deployment, target, None),
            RetirementWork::Local { .. } => return false,
        };
        let prefix = format!("{}/", self.source);
        owner.id == *deployment && owner.storage_prefix() == self.source
            && target.starts_with(&prefix)
            && release.is_none_or(|release| release.starts_with(&prefix) && target != release)
    }

    fn matches_fence(&self, owner: &DeploymentRecord) -> bool {
        let (target, marker) = match &self.work {
            RetirementWork::S3 { target, release, .. } | RetirementWork::Tombstones { target, release, .. } => (target, Some(release)),
            RetirementWork::ReplayPack { target, capture, .. }
            | RetirementWork::ReplayPackTombstone { target, capture, .. } => {
                return matches!(owner.storage_regions.get(&self.token), Some(StorageRegion::ReplayPackRetirement { object, capture: held })
                    if object == target && held == capture);
            }
            RetirementWork::Local { .. } => return false,
        };
        matches!(owner.storage_regions.get(&self.token), Some(StorageRegion::ExactRetirement { object, release })
            if object == target && release.as_ref() == marker)
    }
}

fn replay_pack_writes_are_fenced(owner: &DeploymentRecord, work: &RetirementWork, target: &str) -> bool {
    let pending = owner.storage_regions.values().any(|region| {
        matches!(region, StorageRegion::Remote { publications, .. }
            if publications.values().any(|upload| upload.key == target))
    });
    if pending { return false; }
    match work {
        RetirementWork::ReplayPack { version, tombstone, .. } =>
            version.namespace == tombstone.namespace && version.key == tombstone.key
                && matches!(&tombstone.condition, rtdb_types::S3TombstoneCondition::Matches(_)),
        RetirementWork::ReplayPackTombstone { allocation, tombstone, .. } =>
            owner.storage_namespaces.is_some_and(|namespaces| namespaces.objects == *allocation)
                && matches!(&tombstone.condition, rtdb_types::S3TombstoneCondition::Matches(_)),
        _ => false,
    }
}

fn replay_pack_regions_are_covered(owner: &DeploymentRecord, fence: Uuid, capture: Uuid, checkpoint: Checkpoint) -> bool {
    let mut sweep = None;
    for (&id, region) in &owner.storage_regions {
        if id == fence || region.serves_from_replay_archive(capture, checkpoint) { continue; }
        if !region.admits_archive_read(capture) || sweep.replace(id).is_some() { return false; }
    }
    let Some(sweep) = sweep else { return false; };
    owner.catalog_readers.values().all(|reader| {
        reader.owner != sweep && reader.owner != fence && owner.storage_regions.contains_key(&reader.owner)
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum RetirementWork {
    Local { local_volume: [u8; 32] },
    S3 {
        deployment: String,
        target: String,
        release: String,
        version: PhysicalObjectVersion,
        release_version: PhysicalObjectVersion,
        unit: Option<ServingPublication>,
    },
    Tombstones {
        deployment: String,
        target: String,
        release: String,
        data_tombstone: S3TombstoneTarget,
        release_tombstone: S3TombstoneTarget,
        unit: Option<ServingPublication>,
        packed_slots: Option<[u8; 32]>,
    },
    ReplayPack {
        deployment: String,
        target: String,
        version: PhysicalObjectVersion,
        tombstone: S3TombstoneTarget,
        capture: Uuid,
        checkpoint: Checkpoint,
    },
    ReplayPackTombstone {
        deployment: String,
        target: String,
        tombstone: S3TombstoneTarget,
        allocation: Uuid,
        capture: Uuid,
        checkpoint: Checkpoint,
    },
}

impl RetirementWork {
    fn uses_tombstones(&self) -> bool {
        matches!(self, Self::Tombstones { .. } | Self::ReplayPackTombstone { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PreparationOwner {
    Local([u8; 32]),
    Hosted { task: Option<HostedTask> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkPreparation {
    pub token: Uuid,
    pub generation: Version,
}

/// A fork publishes with the lineage authority it acquired before preparing storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForkPublication {
    Local(ForkPreparation),
    Hosted(ForkPreparation),
    Remote { generation: Version },
}

/// Admission to dispose one physical residence. Settlement consumes it in the
/// same directory transaction that records the receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceDisposal {
    pub(crate) lineage: String,
    residence: String,
    generation: Version,
}

impl ReferenceDisposal {
    pub fn residence(&self) -> &str { &self.residence }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LineageChange {
    PrepareFork { preparation: ForkPreparation, local_volume: [u8; 32] },
    PrepareHostedFork { preparation: ForkPreparation, task: Option<HostedTask> },
    ReleaseFork { preparation: ForkPreparation },
    ReserveRetirement { retirement: LineageRetirement, observed_references: Version, generation: Version },
    AdmitTombstoneUpload { retirement: Uuid, key: String, upload: String },
    SettleTombstoneUpload { retirement: Uuid, key: String, upload: String },
    AbandonExactRetirement { retirement: LineageRetirement },
    SettleRetirement { retirement: LineageRetirement },
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageState {
    version: Version,
    references: Version,
    retirement_generation: Version,
    admission: Admission,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Admission {
    Open(BTreeMap<Uuid, PreparationOwner>),
    Retiring(LineageRetirement),
    Disposing(String),
    Erasing(LineageRetirement),
    ErasingTombstones { retirement: LineageRetirement, uploads: BTreeMap<String, String> },
}

impl Default for Admission {
    fn default() -> Self { Self::Open(BTreeMap::new()) }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum StoredLineage {
    Current(LineageState),
    Legacy(Version),
}

impl StoredLineage {
    pub(crate) fn state(&self) -> LineageState {
        match self { Self::Current(state) => state.clone(), Self::Legacy(revision) => LineageState::legacy(*revision) }
    }
}

impl LineageState {
    pub(crate) fn legacy(revision: Version) -> Self {
        Self { version: revision, references: revision, ..Self::default() }
    }

    pub(crate) fn version(&self) -> Version { self.version }
    pub(crate) fn legacy_revision(&self) -> Option<Version> {
        (self.version == self.references && self.retirement_generation == 0
            && matches!(&self.admission, Admission::Open(preparations) if preparations.is_empty()))
            .then_some(self.references)
    }
    pub fn reference_revision(&self) -> Version { self.references }
    pub fn retirement_generation(&self) -> Version { self.retirement_generation }
    pub fn preparations(&self) -> impl Iterator<Item = (ForkPreparation, &PreparationOwner)> {
        let preparations = match &self.admission { Admission::Open(preparations) => Some(preparations), _ => None };
        preparations.into_iter().flatten().map(|(&token, owner)| {
            (ForkPreparation { token, generation: self.retirement_generation }, owner)
        })
    }
    pub fn local_preparations(&self, volume: [u8; 32]) -> impl Iterator<Item = ForkPreparation> + '_ {
        self.preparations().filter_map(move |(preparation, owner)| (*owner == PreparationOwner::Local(volume)).then_some(preparation))
    }

    pub fn open_generation(&self) -> Option<Version> {
        matches!(self.admission, Admission::Open(_)).then_some(self.retirement_generation)
    }

    pub(crate) fn begin_disposal(&self, owner: &DeploymentRecord) -> Result<(ReferenceDisposal, Self)> {
        let residence = pending_residence(owner)?;
        let mut next = self.clone();
        match &self.admission {
            Admission::Disposing(current) if *current == residence => {},
            Admission::Open(preparations) if preparations.is_empty() => {
                next.retirement_generation = next_reference_revision(self.retirement_generation)?;
                next.advance_version()?;
                // Keep a version available for the settlement transaction.
                next_reference_revision(next.version)?;
                next.admission = Admission::Disposing(residence.clone());
            }
            _ => return Err(lineage_busy()),
        }
        Ok((ReferenceDisposal { lineage: owner.lineage.clone(), residence, generation: next.retirement_generation }, next))
    }

    pub(crate) fn finish_disposal(&self, permit: &ReferenceDisposal, owner: &DeploymentRecord, settled: &DeploymentRecord) -> Result<Self> {
        if !reference_revision::may_settle_reference_disposal(
            self.retirement_generation, permit.generation,
            matches!(&self.admission, Admission::Disposing(residence) if *residence == permit.residence),
        ) || owner.id != settled.id || owner.name != settled.name || owner.epoch != settled.epoch
            || owner.lineage != permit.lineage || settled.lineage != permit.lineage
            || owner.storage_namespaces != settled.storage_namespaces
            || pending_residence(owner)? != permit.residence || settled.storage_prefix() != permit.residence
            || settled.lifecycle != Lifecycle::Deleted
            || !settled.disposal.as_ref().is_some_and(|receipt| {
                receipt.deployment == owner.id && receipt.incarnation == owner.incarnation && receipt.epoch == owner.epoch
            })
        {
            return Err(disposal_changed());
        }
        let mut next = self.clone();
        next.admission = Admission::default();
        next.advance_version()?;
        Ok(next)
    }
    pub fn retirement(&self) -> Option<&LineageRetirement> {
        match &self.admission {
            Admission::Retiring(retirement) | Admission::Erasing(retirement) | Admission::ErasingTombstones { retirement, .. } => Some(retirement),
            Admission::Open(_) | Admission::Disposing(_) => None,
        }
    }
    pub fn exact_erasure_authorized(&self) -> bool { matches!(self.admission, Admission::Erasing(_) | Admission::ErasingTombstones { .. }) }

    /// An observation of the authorized lineage also suffices after a lost reply.
    pub fn exact_retirement_grant(&self, requested: &LineageRetirement) -> Option<ExactRetirementGrant<'_>> {
        let retirement = self.retirement()?;
        (self.exact_erasure_authorized() && retirement == requested)
            .then_some(ExactRetirementGrant { retirement, revision: self.version })
    }

    pub fn tombstone_upload(&self, token: Uuid, key: &str) -> Result<Option<&str>> {
        Ok(self.tombstone_uploads(token, key)?.get(key).map(String::as_str))
    }

    fn tombstone_uploads(&self, token: Uuid, key: &str) -> Result<&BTreeMap<String, String>> {
        let Admission::ErasingTombstones { retirement, uploads } = &self.admission else { return Err(lineage_busy()); };
        let matches = match &retirement.work {
            RetirementWork::Tombstones { target, release, .. } => key == target || key == release,
            RetirementWork::ReplayPackTombstone { target, .. } => key == target,
            _ => false,
        };
        if retirement.token != token || !matches { return Err(lineage_busy()); }
        Ok(uploads)
    }

    pub(crate) fn authorize_exact_owned(&self, owner: &DeploymentRecord, retirement: &LineageRetirement) -> Result<Self> {
        if self.retirement() != Some(retirement) || !retirement.matches_owner(owner) || !retirement.matches_fence(owner)
            || !matches!(owner.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
        {
            return Err(lineage_busy());
        }
        let mut next = self.clone();
        if !self.exact_erasure_authorized() {
            if let RetirementWork::ReplayPack { capture, checkpoint, target, .. }
                | RetirementWork::ReplayPackTombstone { capture, checkpoint, target, .. } = &retirement.work
                && (owner.lifecycle != Lifecycle::Live
                    || !replay_pack_writes_are_fenced(owner, &retirement.work, target)
                    || owner.serving_replay_capture != Some(*capture) || owner.checkpoint != Some(*checkpoint)
                    || owner.checkpoint_capture_retirements.contains_key(capture)
                    || owner.checkpoint_captures.get(capture).is_none_or(|archive| !archive.media_bound || archive.checkpoint != *checkpoint)
                    || !replay_pack_regions_are_covered(owner, retirement.token, *capture, *checkpoint))
            {
                return Err(lineage_busy());
            }
            next.admission = if retirement.work.uses_tombstones() {
                Admission::ErasingTombstones { retirement: retirement.clone(), uploads: BTreeMap::new() }
            } else {
                Admission::Erasing(retirement.clone())
            };
            next.advance_version()?;
        }
        Ok(next)
    }

    pub(crate) fn settle_exact_owned(&self, owner: &DeploymentRecord, retirement: &LineageRetirement) -> Result<(DeploymentRecord, Self)> {
        let matching_fence = retirement.matches_fence(owner);
        let ready = match &self.admission {
            Admission::Erasing(current) => current == retirement && !current.work.uses_tombstones(),
            Admission::ErasingTombstones { retirement: current, uploads } => current == retirement && uploads.is_empty(),
            _ => false,
        };
        if !reference_revision::may_settle_exact_owned(
            ready,
            retirement.matches_owner(owner),
            matching_fence,
        ) { return Err(lineage_busy()); }
        let mut next_owner = owner.clone();
        next_owner.version = next_reference_revision(owner.version)?;
        next_owner.storage_regions.remove(&retirement.token);

        let mut next = self.clone();
        next.admission = Admission::default();
        next.advance_version()?;
        Ok((next_owner, next))
    }

    fn advance_version(&mut self) -> Result<()> {
        self.version = next_reference_revision(self.version)?;
        Ok(())
    }

    fn prepare(&mut self, preparation: ForkPreparation, owner: PreparationOwner) -> Result<bool> {
        let Admission::Open(preparations) = &mut self.admission else { return Err(lineage_busy()); };
        if preparation.token.is_nil() || preparation.generation != self.retirement_generation { return Err(lineage_busy()); }
        if let Some(held) = preparations.get(&preparation.token) {
            return if *held == owner { Ok(false) } else { Err(lineage_busy()) };
        }
        preparations.insert(preparation.token, owner);
        Ok(true)
    }

    pub(crate) fn changed(&self, change: LineageChange) -> Result<Self> {
        let mut next = self.clone();
        match change {
            LineageChange::PrepareFork { preparation, local_volume } => {
                if !next.prepare(preparation, PreparationOwner::Local(local_volume))? { return Ok(next); }
            }
            LineageChange::PrepareHostedFork { preparation, task } => {
                if !next.prepare(preparation, PreparationOwner::Hosted { task })? { return Ok(next); }
            }
            LineageChange::ReleaseFork { preparation } => {
                let Admission::Open(preparations) = &mut next.admission else { return Ok(next); };
                if preparation.generation != self.retirement_generation || preparations.remove(&preparation.token).is_none() {
                    return Ok(next);
                }
            }
            LineageChange::ReserveRetirement { retirement, observed_references, generation } => {
                if self.retirement() == Some(&retirement) { return Ok(next); }
                if retirement.token.is_nil() || retirement.source.is_empty()
                    || !matches!(&self.admission, Admission::Open(preparations) if preparations.is_empty())
                    || self.references != observed_references || self.retirement_generation != generation
                {
                    return Err(lineage_busy());
                }
                next.retirement_generation = next_reference_revision(self.retirement_generation)?;
                next.admission = Admission::Retiring(retirement);
            }
            LineageChange::AdmitTombstoneUpload { retirement, key, upload } => {
                if upload.is_empty() { return Err(lineage_busy()); }
                match self.tombstone_upload(retirement, &key)? {
                    Some(existing) if existing == upload => return Ok(next),
                    Some(_) => return Err(lineage_busy()),
                    None => {},
                }
                let Admission::ErasingTombstones { uploads, .. } = &mut next.admission else { unreachable!() };
                uploads.insert(key, upload);
            }
            LineageChange::SettleTombstoneUpload { retirement, key, upload } => {
                match self.tombstone_upload(retirement, &key)? {
                    Some(existing) if existing == upload => {},
                    Some(_) => return Err(lineage_busy()),
                    None => return Ok(next),
                }
                let Admission::ErasingTombstones { uploads, .. } = &mut next.admission else { unreachable!() };
                uploads.remove(&key);
            }
            LineageChange::AbandonExactRetirement { retirement } => {
                if matches!(retirement.work, RetirementWork::Local { .. })
                    || !matches!(&self.admission, Admission::Retiring(current) if *current == retirement)
                {
                    return Err(lineage_busy());
                }
                next.admission = Admission::default();
            }
            LineageChange::SettleRetirement { retirement } => {
                if self.retirement() != Some(&retirement)
                    || !matches!(retirement.work, RetirementWork::Local { .. })
                {
                    return Err(lineage_busy());
                }
                next.admission = Admission::default();
            }
        }
        next.advance_version()?;
        Ok(next)
    }

    pub(crate) fn published(&self, preparation: Option<ForkPublication>, shared: bool) -> Result<Self> {
        let mut next = self.clone();
        let open = matches!(self.admission, Admission::Open(_));
        let preparation_kind = preparation;
        match preparation {
            Some(ForkPublication::Local(preparation) | ForkPublication::Hosted(preparation)) => {
                let Admission::Open(preparations) = &mut next.admission else { return Err(lineage_busy()); };
                let owner = preparations.get(&preparation.token);
                let matches_owner = matches!((preparation_kind, owner),
                    (Some(ForkPublication::Local(_)), Some(PreparationOwner::Local(_)))
                    | (Some(ForkPublication::Hosted(_)), Some(PreparationOwner::Hosted { .. })));
                if preparation.generation != self.retirement_generation || !matches_owner { return Err(lineage_busy()); }
                preparations.remove(&preparation.token);
            }
            Some(ForkPublication::Remote { generation }) => {
                if !reference_revision::may_publish_reference(self.retirement_generation, open, Some(generation)) {
                    return Err(lineage_busy());
                }
            }
            None if !open || self.retirement_generation != 0 => return Err(lineage_busy()),
            None => {},
        }
        if shared { next.references = next_reference_revision(self.references)?; }
        next.advance_version()?;
        Ok(next)
    }
}

pub(crate) fn lineage_busy() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.directory/lineage-changed",
        "The lineage no longer admits this operation")
}

fn pending_residence(owner: &DeploymentRecord) -> Result<String> {
    if owner.disposal.is_none() && owner.storage_regions.is_empty()
        && (owner.lifecycle == Lifecycle::Deleting || owner.lifecycle == Lifecycle::Deleted && owner.moved_at_ms.is_some())
    {
        return Ok(owner.storage_prefix());
    }
    Err(disposal_changed())
}

pub(crate) fn disposal_changed() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.directory/reference-disposal-changed",
        "The exact residence is no longer awaiting disposal")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CheckpointCapture;
    use rtdb_types::{S3ObjectNamespace, S3TombstoneCondition};

    #[test]
    fn replay_pack_authority_requires_the_selected_open_capture_and_keeps_its_custody_until_settlement() {
        let mut owner = DeploymentRecord::new("orders", "acme", vec![], 0);
        let capture = Uuid::new_v4();
        let checkpoint = Checkpoint { block: 8, boundary: 16, source: 9, system_time: 10, root_sha256: Some([7; 32]) };
        owner.checkpoint = Some(checkpoint);
        owner.serving_replay_capture = Some(capture);
        owner.checkpoint_captures.insert(capture, CheckpointCapture {
            archive: "independent archive".to_owned(), key_namespace: None,
            incarnation: owner.incarnation, log_epoch: owner.log_epoch, checkpoint,
            automatic_fork: false, world_replay: None, media_bound: true,
            manifest_sha256: Some("accepted".to_owned()), remote_disposal: None,
        });
        let source = owner.storage_prefix();
        let object = format!("{source}/v06/~packs/b00");
        let retirement = LineageRetirement {
            token: Uuid::new_v4(), source,
            work: RetirementWork::ReplayPack {
                deployment: owner.id.clone(), target: object.clone(), capture, checkpoint,
                tombstone: S3TombstoneTarget {
                    namespace: S3ObjectNamespace { bucket: "serving".into(), endpoint: None, region: None },
                    key: format!("cell/{object}"),
                    condition: S3TombstoneCondition::Matches("\"original\"".into()),
                },
                version: PhysicalObjectVersion {
                    namespace: S3ObjectNamespace { bucket: "serving".into(), endpoint: None, region: None },
                    key: format!("cell/{object}"), version: "exact-version".into(),
                },
            },
        };
        let state = LineageState::default().changed(LineageChange::ReserveRetirement {
            retirement: retirement.clone(), observed_references: 0, generation: 0,
        }).unwrap();
        owner.storage_regions.insert(retirement.token, StorageRegion::ReplayPackRetirement { object: object.clone(), capture });
        let pin = &owner.storage_regions[&retirement.token];
        assert!(pin.holds_archive(capture) && pin.retires_reference(&object));
        assert!(!pin.admits_archive_read(capture));
        assert!(state.authorize_exact_owned(&owner, &retirement).is_err());

        let reader = Uuid::new_v4();
        owner.storage_regions.insert(reader, StorageRegion::Local {
            archive: Some(capture), checkpoint_suffix: None, repair_archive: None,
            tracks_ordered_index_pages: false, checkpoint_metadata_from: None, checkpoint_replay_from: None,
        });
        let mut changed = owner.clone();
        changed.serving_replay_capture = Some(Uuid::new_v4());
        assert!(state.authorize_exact_owned(&changed, &retirement).is_err());

        let serving = Uuid::new_v4();
        owner.storage_regions.insert(serving, StorageRegion::Local {
            archive: None, checkpoint_suffix: None, repair_archive: Some(capture),
            tracks_ordered_index_pages: true, checkpoint_metadata_from: Some(checkpoint.block), checkpoint_replay_from: None,
        });
        let held = crate::CatalogReader {
            owner: serving, storage_root: "v06".into(), replica_epoch: owner.log_epoch,
            cut: None, catalog_file_paths: None,
        };
        owner.catalog_readers.insert(Uuid::new_v4(), held);
        let mut changed = owner.clone();
        changed.storage_regions.remove(&serving);
        assert!(state.authorize_exact_owned(&changed, &retirement).is_err());
        let mut changed = owner.clone();
        let raw_reader = Uuid::new_v4();
        changed.storage_regions.insert(raw_reader, StorageRegion::Local {
            archive: None, checkpoint_suffix: None, repair_archive: None,
            tracks_ordered_index_pages: true, checkpoint_metadata_from: Some(checkpoint.block), checkpoint_replay_from: None,
        });
        assert!(state.authorize_exact_owned(&changed, &retirement).is_err());
        owner.holder = Some(crate::MemberId("writer".into()));
        owner.s3_write_lease_until_ms = Some(i64::MAX);
        let mut uploading = owner.clone();
        let pending = Uuid::new_v4();
        uploading.storage_regions.insert(pending, StorageRegion::Remote {
            archive: None, archive_source: None, checkpoint_suffix: None, repair_archive: Some(capture),
            tracks_ordered_index_pages: true, checkpoint_metadata_from: Some(checkpoint.block), checkpoint_replay_from: None,
            owner: None, closed: false, publications: BTreeMap::from([(Uuid::new_v4(), crate::StoragePublication {
                key: object, upload_id: "prepared-pack".into(), unit: None, rejected: false,
            })]),
        });
        assert!(state.authorize_exact_owned(&uploading, &retirement).is_err());
        let state = state.authorize_exact_owned(&owner, &retirement).unwrap();
        let state: LineageState = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
        assert!(state.exact_erasure_authorized());
        assert_eq!(state.retirement(), Some(&retirement));

        changed.storage_regions.remove(&reader);
        assert!(changed.storage_regions[&retirement.token].holds_archive(capture));
        let (settled, reopened) = state.settle_exact_owned(&changed, &retirement).unwrap();
        assert!(!settled.storage_regions.contains_key(&retirement.token));
        assert!(reopened.open_generation().is_some());
    }

    #[test]
    fn terminal_replay_pack_keeps_its_archive_until_the_owned_upload_drains() {
        let mut owner = DeploymentRecord::new("orders", "acme", vec![], 0);
        let source = owner.storage_prefix();
        let target = format!("{source}/v06/~packs/b00");
        let capture = Uuid::new_v4();
        let retirement = LineageRetirement {
            token: Uuid::new_v4(), source, work: RetirementWork::ReplayPackTombstone {
                deployment: owner.id.clone(), target: target.clone(), capture,
                allocation: Uuid::new_v4(),
                checkpoint: Checkpoint { block: 3, boundary: 7, source: 2, system_time: 0, root_sha256: Some([7; 32]) },
                tombstone: S3TombstoneTarget {
                    namespace: S3ObjectNamespace { bucket: "serving".into(), endpoint: None, region: None },
                    key: format!("cell/{target}"), condition: S3TombstoneCondition::Matches("\"original\"".into()),
                },
            },
        };
        owner.storage_regions.insert(retirement.token, StorageRegion::ReplayPackRetirement { object: target.clone(), capture });
        let RetirementWork::ReplayPackTombstone { allocation, checkpoint, .. } = &retirement.work else { unreachable!(); };
        let checkpoint = *checkpoint;
        owner.holder = Some(crate::MemberId("writer".into()));
        owner.s3_write_lease_until_ms = Some(i64::MAX);
        owner.serving_replay_capture = Some(capture);
        owner.checkpoint = Some(checkpoint);
        owner.checkpoint_captures.insert(capture, CheckpointCapture {
            archive: "independent archive".into(), key_namespace: None, incarnation: owner.incarnation,
            log_epoch: owner.log_epoch, checkpoint, automatic_fork: false, world_replay: None,
            media_bound: true, manifest_sha256: Some("accepted".into()), remote_disposal: None,
        });
        owner.storage_regions.insert(Uuid::new_v4(), StorageRegion::Local {
            archive: Some(capture), checkpoint_suffix: None, repair_archive: None,
            tracks_ordered_index_pages: false, checkpoint_metadata_from: None, checkpoint_replay_from: None,
        });
        let writer = Uuid::new_v4();
        owner.storage_regions.insert(writer, StorageRegion::Remote {
            archive: None, archive_source: None, checkpoint_suffix: None, repair_archive: Some(capture),
            tracks_ordered_index_pages: true, checkpoint_metadata_from: None, checkpoint_replay_from: None,
            owner: None, closed: false, publications: BTreeMap::from([(Uuid::new_v4(), crate::StoragePublication {
                key: target.clone(), upload_id: "admitted".into(), unit: None, rejected: false,
            })]),
        });
        let state = LineageState::default().changed(LineageChange::ReserveRetirement {
            retirement: retirement.clone(), observed_references: 0, generation: 0,
        }).unwrap();
        owner.storage_namespaces = Some(crate::StorageNamespaces { objects: *allocation, keys: [0; 32] });
        assert!(state.authorize_exact_owned(&owner, &retirement).is_err());
        let StorageRegion::Remote { publications, .. } = owner.storage_regions.get_mut(&writer).unwrap() else { unreachable!(); };
        publications.clear();
        let state = state.authorize_exact_owned(&owner, &retirement).unwrap();
        let upload = |key: &str| LineageChange::AdmitTombstoneUpload {
            retirement: retirement.token, key: key.to_owned(), upload: "upload".into(),
        };
        assert!(state.changed(upload("another-pack")).is_err());
        let state = state.changed(upload(&target)).unwrap();
        assert!(owner.storage_regions[&retirement.token].holds_archive(capture));
        assert!(state.settle_exact_owned(&owner, &retirement).is_err());
        let state = state.changed(LineageChange::SettleTombstoneUpload {
            retirement: retirement.token, key: target, upload: "upload".into(),
        }).unwrap();
        let (owner, lineage) = state.settle_exact_owned(&owner, &retirement).unwrap();
        assert!(!owner.storage_regions.contains_key(&retirement.token));
        assert!(lineage.open_generation().is_some());
    }
}
