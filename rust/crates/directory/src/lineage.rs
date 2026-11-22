use std::collections::BTreeMap;

use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Version, next_reference_revision};

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
        let preparations = match &self.admission { Admission::Open(preparations) => Some(preparations), Admission::Retiring(_) => None };
        preparations.into_iter().flatten().filter_map(move |(&token, &owner)| {
            (owner == volume).then_some(ForkPreparation { token, generation: self.retirement_generation })
        })
    }
    pub fn retirement(&self) -> Option<&LineageRetirement> {
        match &self.admission { Admission::Retiring(retirement) => Some(retirement), Admission::Open(_) => None }
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

    pub(crate) fn published(&self, preparation: Option<ForkPreparation>, shared: bool) -> Result<Self> {
        let mut next = self.clone();
        let Admission::Open(preparations) = &mut next.admission else { return Err(lineage_busy()); };
        if let Some(preparation) = preparation {
            if preparation.generation != self.retirement_generation || preparations.remove(&preparation.token).is_none() {
                return Err(lineage_busy());
            }
        } else if self.retirement_generation != 0 {
            return Err(lineage_busy());
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
