use std::collections::BTreeMap;

use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{DeploymentRecord, Lifecycle, Version, next_reference_revision, reference_revision};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageRetirement {
    pub token: Uuid,
    pub source: String,
    pub local_volume: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkPreparation {
    pub token: Uuid,
    pub generation: Version,
}

/// A local preparation owns a durable hold. A remote preparation carries the
/// generation it observed before reading storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForkPublication {
    Local(ForkPreparation),
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LineageChange {
    PrepareFork { preparation: ForkPreparation, local_volume: [u8; 32] },
    ReleaseFork { preparation: ForkPreparation },
    ReserveRetirement { retirement: LineageRetirement, observed_references: Version, generation: Version },
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
    Open(BTreeMap<Uuid, [u8; 32]>),
    Retiring(LineageRetirement),
    Disposing(String),
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
    pub fn local_preparations(&self, volume: [u8; 32]) -> impl Iterator<Item = ForkPreparation> + '_ {
        let preparations = match &self.admission { Admission::Open(preparations) => Some(preparations), Admission::Retiring(_) | Admission::Disposing(_) => None };
        preparations.into_iter().flatten().filter_map(move |(&token, &owner)| {
            (owner == volume).then_some(ForkPreparation { token, generation: self.retirement_generation })
        })
    }
    pub fn retirement(&self) -> Option<&LineageRetirement> {
        match &self.admission { Admission::Retiring(retirement) => Some(retirement), Admission::Open(_) | Admission::Disposing(_) => None }
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

    fn advance_version(&mut self) -> Result<()> {
        self.version = next_reference_revision(self.version)?;
        Ok(())
    }

    pub(crate) fn changed(&self, change: LineageChange) -> Result<Self> {
        let mut next = self.clone();
        match change {
            LineageChange::PrepareFork { preparation, local_volume } => {
                let Admission::Open(preparations) = &mut next.admission else { return Err(lineage_busy()); };
                if preparation.token.is_nil() || preparation.generation != self.retirement_generation { return Err(lineage_busy()); }
                if let Some(held) = preparations.get(&preparation.token) {
                    if *held == local_volume { return Ok(next); }
                    return Err(lineage_busy());
                }
                preparations.insert(preparation.token, local_volume);
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
            LineageChange::SettleRetirement { retirement } => {
                if self.retirement() != Some(&retirement) { return Err(lineage_busy()); }
                next.admission = Admission::default();
            }
        }
        next.advance_version()?;
        Ok(next)
    }

    pub(crate) fn published(&self, preparation: Option<ForkPublication>, shared: bool) -> Result<Self> {
        let mut next = self.clone();
        let open = matches!(self.admission, Admission::Open(_));
        match preparation {
            Some(ForkPublication::Local(preparation)) => {
                let Admission::Open(preparations) = &mut next.admission else { return Err(lineage_busy()); };
                if preparation.generation != self.retirement_generation || preparations.remove(&preparation.token).is_none() {
                    return Err(lineage_busy());
                }
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
