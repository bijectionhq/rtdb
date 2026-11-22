use std::collections::BTreeMap;

use rtdb_types::{Anomaly, AnomalyCategory, PhysicalObjectVersion, Result, S3TombstoneTarget};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{DeploymentRecord, HostedTask, Lifecycle, StorageRegion, Version, next_reference_revision, reference_revision};

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
                (deployment, target, release)
            }
            RetirementWork::Local { .. } => return false,
        };
        let prefix = format!("{}/", self.source);
        owner.id == *deployment && owner.storage_prefix() == self.source
            && target.starts_with(&prefix) && release.starts_with(&prefix) && target != release
    }

    fn matches_fence(&self, owner: &DeploymentRecord) -> bool {
        let (target, marker) = match &self.work {
            RetirementWork::S3 { target, release, release_version, .. } => (target, release_version.as_ref().map(|_| release)),
            RetirementWork::Tombstones { target, release, .. } => (target, Some(release)),
            RetirementWork::Local { .. } => return false,
        };
        matches!(owner.storage_regions.get(&self.token), Some(StorageRegion::ExactRetirement { object, release })
            if object == target && release.as_ref() == marker)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RetirementWork {
    Local { local_volume: [u8; 32] },
    S3 {
        deployment: String,
        target: String,
        release: String,
        version: PhysicalObjectVersion,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        release_version: Option<PhysicalObjectVersion>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        legacy_receipt: Option<String>,
    },
    Tombstones {
        deployment: String,
        target: String,
        release: String,
        data_tombstone: S3TombstoneTarget,
        release_tombstone: S3TombstoneTarget,
    },
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

    pub fn tombstone_upload(&self, token: Uuid, key: &str) -> Result<Option<&str>> {
        Ok(self.tombstone_uploads(token, key)?.get(key).map(String::as_str))
    }

    fn tombstone_uploads(&self, token: Uuid, key: &str) -> Result<&BTreeMap<String, String>> {
        let Admission::ErasingTombstones { retirement, uploads } = &self.admission else { return Err(lineage_busy()); };
        let RetirementWork::Tombstones { target, release, .. } = &retirement.work else { return Err(lineage_busy()); };
        if retirement.token != token || (key != target && key != release) { return Err(lineage_busy()); }
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
            next.admission = match retirement.work {
                RetirementWork::Tombstones { .. } => Admission::ErasingTombstones { retirement: retirement.clone(), uploads: BTreeMap::new() },
                _ => Admission::Erasing(retirement.clone()),
            };
            next.advance_version()?;
        }
        Ok(next)
    }

    pub(crate) fn settle_exact_owned(&self, owner: &DeploymentRecord, retirement: &LineageRetirement) -> Result<(DeploymentRecord, Self)> {
        // An older exact-version attempt may already have cleared its data-only fence.
        let matching_fence = retirement.matches_fence(owner)
            || matches!(retirement.work, RetirementWork::S3 { release_version: None, .. })
                && !owner.storage_regions.contains_key(&retirement.token);
        let ready = match &self.admission {
            Admission::Erasing(current) => current == retirement && !matches!(current.work, RetirementWork::Tombstones { .. }),
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
