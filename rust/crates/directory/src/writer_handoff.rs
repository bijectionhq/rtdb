//! Retained prepared writer moves in the deployment's placement order.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{DeploymentRecord, Lifecycle, MemberId, Version};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

pub const WRITER_HANDOFF_BYTES: usize = 218;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Generation {
    installation: Uuid,
    deployment: Uuid,
    cell: Uuid,
    epoch: u64,
    generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterHandoff {
    incumbent: Generation,
    prepared: Generation,
    source: Uuid,
    tablet: String,
    predecessor_epoch: u64,
    successor_epoch: u64,
    switch_at_ns: u64,
}

impl WriterHandoff {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != WRITER_HANDOFF_BYTES {
            return None;
        }
        fn u64_at(bytes: &[u8], offset: usize) -> u64 {
            u64::from_be_bytes(
                bytes[offset..offset + 8]
                    .try_into()
                    .expect("fixed handoff field"),
            )
        }
        fn generation(bytes: &[u8]) -> Option<Generation> {
            Some(Generation {
                installation: Uuid::from_slice(&bytes[..16]).ok()?,
                deployment: Uuid::from_slice(&bytes[16..32]).ok()?,
                cell: Uuid::from_slice(&bytes[32..48]).ok()?,
                epoch: u64_at(bytes, 48),
                generation: u64_at(bytes, 56),
            })
        }
        if bytes[128..132] != 36u32.to_be_bytes() || bytes[168..172] != 22u32.to_be_bytes() {
            return None;
        }
        let source_text = std::str::from_utf8(&bytes[132..168]).ok()?;
        let source = Uuid::parse_str(source_text).ok()?;
        if source.to_string() != source_text {
            return None;
        }
        let handoff = Self {
            incumbent: generation(&bytes[..64])?,
            prepared: generation(&bytes[64..128])?,
            source,
            tablet: std::str::from_utf8(&bytes[172..194]).ok()?.to_owned(),
            predecessor_epoch: u64_at(bytes, 194),
            successor_epoch: u64_at(bytes, 202),
            switch_at_ns: u64_at(bytes, 210),
        };
        handoff.valid().then_some(handoff)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(WRITER_HANDOFF_BYTES);
        for generation in [self.incumbent, self.prepared] {
            bytes.extend_from_slice(generation.installation.as_bytes());
            bytes.extend_from_slice(generation.deployment.as_bytes());
            bytes.extend_from_slice(generation.cell.as_bytes());
            bytes.extend_from_slice(&generation.epoch.to_be_bytes());
            bytes.extend_from_slice(&generation.generation.to_be_bytes());
        }
        bytes.extend_from_slice(&36u32.to_be_bytes());
        bytes.extend_from_slice(self.source.to_string().as_bytes());
        bytes.extend_from_slice(&22u32.to_be_bytes());
        bytes.extend_from_slice(self.tablet.as_bytes());
        bytes.extend_from_slice(&self.predecessor_epoch.to_be_bytes());
        bytes.extend_from_slice(&self.successor_epoch.to_be_bytes());
        bytes.extend_from_slice(&self.switch_at_ns.to_be_bytes());
        bytes
    }

    pub fn valid(&self) -> bool {
        let incumbent = self.incumbent;
        let prepared = self.prepared;
        !incumbent.installation.is_nil()
            && !incumbent.deployment.is_nil()
            && !incumbent.cell.is_nil()
            && incumbent.epoch > 0
            && incumbent.generation > 0
            && Generation {
                generation: prepared.generation,
                ..incumbent
            } == prepared
            && incumbent.generation < prepared.generation
            && !self.source.is_nil()
            && self.tablet.len() == 22
            && self
                .tablet
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            && self
                .tablet
                .as_bytes()
                .last()
                .is_some_and(|byte| matches!(byte, b'A' | b'Q' | b'g' | b'w'))
            && self.predecessor_epoch > 0
            && Some(self.successor_epoch) == self.predecessor_epoch.checked_add(1)
            && self.switch_at_ns > 0
            && self.switch_at_ns <= i64::MAX as u64
    }

    pub fn source(&self) -> Uuid {
        self.source
    }
    pub fn predecessor_epoch(&self) -> u64 {
        self.predecessor_epoch
    }
    pub fn successor_epoch(&self) -> u64 {
        self.successor_epoch
    }
    pub fn switch_at_ns(&self) -> u64 {
        self.switch_at_ns
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterHandoffPlan {
    pub identity: WriterHandoff,
    pub incumbent: MemberId,
    pub successor: MemberId,
    pub incarnation: u64,
    pub maximum_clock_error_ns: u64,
    pub switch_deadline_ns: u64,
    pub incumbent_drain_until_ns: u64,
}

impl WriterHandoffPlan {
    pub fn matches(&self, record: &DeploymentRecord) -> bool {
        self.identity.valid()
            && self.identity.source().to_string() == record.id
            && self.incarnation == record.incarnation
            && self.incumbent != self.successor
            && self.maximum_clock_error_ns > 0
            && self.maximum_clock_error_ns < self.identity.switch_at_ns()
            && self.identity.switch_at_ns() < self.switch_deadline_ns
            && self.switch_deadline_ns <= self.incumbent_drain_until_ns
            && self.incumbent_drain_until_ns <= i64::MAX as u64
            && record.lifecycle == Lifecycle::Live
            && record.linearizable_reads
            && !record.is_resident_idle()
            && record.checkpoint_recovery.is_none()
            && record.checkpoint_stage.is_none()
            && record.cell_transfer.is_none()
            && record.move_from.is_none()
            && record.cell_move.is_none()
            && record.placement_move.is_none()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriterHandoffState {
    Reserved {
        coordinate: Version,
    },
    Prepared {
        coordinate: Version,
        prepared_upper_ns: u64,
    },
    Fenced {
        coordinate: Version,
        prepared_upper_ns: u64,
        fence: Version,
    },
    Opened {
        coordinate: Version,
        prepared_upper_ns: u64,
        fence: Version,
        opening: Version,
        opened_upper_ns: u64,
    },
    Abandoned,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetainedWriterHandoff {
    pub plan: WriterHandoffPlan,
    pub state: WriterHandoffState,
}

impl RetainedWriterHandoff {
    pub fn pending(&self) -> bool {
        matches!(
            self.state,
            WriterHandoffState::Reserved { .. }
                | WriterHandoffState::Prepared { .. }
                | WriterHandoffState::Fenced { .. }
        )
    }

    pub fn retains_incumbent(&self, member: &MemberId, epoch: u64) -> bool {
        &self.plan.incumbent == member
            && self.plan.identity.predecessor_epoch() == epoch
            && matches!(
                self.state,
                WriterHandoffState::Fenced { .. } | WriterHandoffState::Opened { .. }
            )
    }

    pub fn preparation(&self) -> Option<Vec<u8>> {
        let (coordinate, prepared_upper_ns) = match self.state {
            WriterHandoffState::Prepared {
                coordinate,
                prepared_upper_ns,
            }
            | WriterHandoffState::Fenced {
                coordinate,
                prepared_upper_ns,
                ..
            }
            | WriterHandoffState::Opened {
                coordinate,
                prepared_upper_ns,
                ..
            } => (coordinate, prepared_upper_ns),
            WriterHandoffState::Reserved { .. } | WriterHandoffState::Abandoned => return None,
        };
        let mut bytes = self.plan.identity.encode();
        bytes.extend_from_slice(&coordinate.to_be_bytes());
        bytes.extend_from_slice(&prepared_upper_ns.to_be_bytes());
        Some(bytes)
    }

    pub fn settlement(&self) -> Option<Vec<u8>> {
        let WriterHandoffState::Opened {
            fence,
            opening,
            opened_upper_ns,
            ..
        } = self.state
        else {
            return None;
        };
        if opened_upper_ns >= self.plan.switch_deadline_ns {
            return None;
        }
        let mut bytes = self.preparation()?;
        bytes.extend_from_slice(&fence.to_be_bytes());
        bytes.extend_from_slice(&opening.to_be_bytes());
        Some(bytes)
    }
}

pub(crate) fn require_writer_handoff_update(
    current: &DeploymentRecord,
    next: &DeploymentRecord,
) -> Result<()> {
    use WriterHandoffState::*;
    let refused = || {
        Anomaly::new(
            AnomalyCategory::Conflict,
            "rtdb.directory/writer-handoff",
            "The retained writer handoff does not allow this placement transition",
        )
    };
    let before = &current.writer_handoffs;
    let after = &next.writer_handoffs;
    let same_holder = current.epoch == next.epoch && current.holder == next.holder;
    let same_placement = current.incarnation == next.incarnation
        && current.log_epoch == next.log_epoch
        && current.ensemble == next.ensemble
        && current.log_copy_servers == next.log_copy_servers
        && current.org == next.org
        && current.lineage == next.lineage
        && current.storage_generation == next.storage_generation
        && current.storage_namespaces == next.storage_namespaces;
    if before == after {
        if before.last().is_some_and(RetainedWriterHandoff::pending)
            && (!same_holder || !same_placement || !before.last().unwrap().plan.matches(next))
        {
            return Err(refused());
        }
        return Ok(());
    }
    let new = after.last().ok_or_else(refused)?;
    let version = current.version.checked_add(1).ok_or_else(refused)?;
    if !same_placement || !new.plan.matches(current) || !new.plan.matches(next) {
        return Err(refused());
    }
    let old_holder = |record: &DeploymentRecord| {
        record.epoch == new.plan.identity.predecessor_epoch()
            && record.holder.as_ref() == Some(&new.plan.incumbent)
    };
    let new_holder = |record: &DeploymentRecord| {
        record.epoch == new.plan.identity.successor_epoch()
            && record.holder.as_ref() == Some(&new.plan.successor)
    };
    if after.len() == before.len() + 1 && after.starts_with(before) {
        if before.last().is_none_or(|last| !last.pending())
            && before
                .iter()
                .all(|last| last.plan.identity.prepared != new.plan.identity.prepared)
            && old_holder(current)
            && same_holder
            && new.state
                == (Reserved {
                    coordinate: version,
                })
        {
            return Ok(());
        }
        return Err(refused());
    }
    let old = before.last().ok_or_else(refused)?;
    if before.len() != after.len()
        || before[..before.len() - 1] != after[..after.len() - 1]
        || old.plan != new.plan
    {
        return Err(refused());
    }
    let permitted = match (old.state, new.state) {
        (
            Reserved { coordinate },
            Prepared {
                coordinate: prepared,
                prepared_upper_ns,
            },
        ) => {
            same_holder
                && old_holder(current)
                && coordinate == prepared
                && prepared_upper_ns > 0
                && prepared_upper_ns < new.plan.identity.switch_at_ns()
        }
        (
            Prepared {
                coordinate,
                prepared_upper_ns,
            },
            Fenced {
                coordinate: prior,
                prepared_upper_ns: upper,
                fence,
            },
        ) => {
            old_holder(current)
                && new_holder(next)
                && (coordinate, prepared_upper_ns, version) == (prior, upper, fence)
        }
        (
            Fenced {
                coordinate,
                prepared_upper_ns,
                fence,
            },
            Opened {
                coordinate: prior,
                prepared_upper_ns: upper,
                fence: observed,
                opening,
                opened_upper_ns,
            },
        ) => {
            same_holder
                && new_holder(current)
                && (coordinate, prepared_upper_ns, fence, version)
                    == (prior, upper, observed, opening)
                && opened_upper_ns >= new.plan.identity.switch_at_ns()
        }
        (Reserved { .. } | Prepared { .. }, Abandoned) => same_holder && old_holder(current),
        _ => false,
    };
    if permitted { Ok(()) } else { Err(refused()) }
}
