//! Source-directory order for ordinary Classic FreshFlight.

use std::collections::BTreeMap;

use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Cas, CheckpointStageIntent, CheckpointTransferFence, ClassicMigrationSource, DeploymentRecord, Directory, Lifecycle, Version};

pub const MAX_GENERATIONS: usize = 128;
const MAX_CAS_ATTEMPTS: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Commission {
    pub intent: CheckpointStageIntent,
    pub coverage_sha256: [u8; 32],
    pub begin_sha256: [u8; 32],
    pub original_catalog_sha256: [u8; 32],
    pub retained_path_sha256: [u8; 32],
    pub prior_generations: Vec<Generation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Gateway,
    Receiver,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Generation {
    pub generation: Uuid,
    pub role: Role,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Admission {
    commission: Commission,
    owners: BTreeMap<Uuid, Owner>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    frozen: Option<Frozen>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Owner {
    role: Role,
    admitted_at: Version,
    joined: Option<Join>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Join {
    at: Version,
    disposal_sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Frozen {
    at: Version,
    drained_at: Option<Version>,
}

#[derive(Serialize)]
pub struct Terminal<'a> {
    pub commission: &'a Commission,
    pub fenced_at: Version,
    pub drained_at: Version,
    pub gateway_generations: usize,
    pub receiver_generations: usize,
    owners: &'a BTreeMap<Uuid, Owner>,
}

pub fn commission(record: &DeploymentRecord) -> Option<&Commission> {
    record.classic_flight.as_ref().map(|admission| &admission.commission)
}

pub fn excludes_unclassified(record: &DeploymentRecord) -> bool {
    record.classic_flight.is_some()
        || record.checkpoint_transfer_fence.as_ref().is_some_and(|fence| {
            fence.destination.intent.source.to_string() == record.id
                && fence.destination.intent.classic_migration.is_some()
        })
}

fn selects_live(record: &DeploymentRecord, commission: &Commission) -> bool {
    let intent = &commission.intent;
    intent.source.to_string() == record.id
        && intent.org == record.org
        && record.lifecycle == Lifecycle::Live
        && record.holder.is_some()
        && record.checkpoint_transfer_fence.is_none()
        && record.move_from.is_none()
        && intent.classic_migration.is_some_and(|migration| {
            migration.source == ClassicMigrationSource {
                incarnation: record.incarnation, log_epoch: record.log_epoch, writer_epoch: record.epoch,
            }
        })
}

fn selects_fence(commission: &Commission, fence: &CheckpointTransferFence) -> bool {
    let intent = &commission.intent;
    intent == &fence.destination.intent
        && intent.classic_migration.is_some_and(|migration| {
            migration.source.incarnation == fence.destination.source_incarnation
                && migration.source.log_epoch == fence.destination.source_log_epoch
                && migration.source.writer_epoch == fence.source_writer_epoch
        })
}

fn counts(admission: &Admission) -> (usize, usize) {
    let gateways = admission.owners.values().filter(|owner| owner.role == Role::Gateway).count();
    (gateways, admission.owners.len() - gateways)
}

fn completely_joined(admission: &Admission) -> bool {
    let (gateways, receivers) = counts(admission);
    gateways > 0 && receivers > 0 && admission.owners.len() <= MAX_GENERATIONS
        && admission.owners.values().all(|owner| owner.joined.is_some())
}

pub(crate) fn freeze(record: &mut DeploymentRecord, fence: &CheckpointTransferFence, at: Version) {
    if let Some(admission) = &mut record.classic_flight
        && selects_fence(&admission.commission, fence)
    {
        let drained_at = completely_joined(admission).then_some(at);
        admission.frozen = Some(Frozen { at, drained_at });
    }
}

pub fn terminal<'a>(record: &'a DeploymentRecord, fence: &CheckpointTransferFence) -> Option<Terminal<'a>> {
    let admission = record.classic_flight.as_deref()?;
    let frozen = admission.frozen.as_ref()?;
    let drained_at = frozen.drained_at?;
    if record.checkpoint_transfer_fence.as_deref() != Some(fence)
        || record.id != fence.destination.intent.source.to_string()
        || record.epoch != fence.fenced_writer_epoch
        || !selects_fence(&admission.commission, fence)
        || !(frozen.at..=record.version).contains(&drained_at)
        || !completely_joined(admission)
        || admission.owners.values().any(|owner| {
            owner.admitted_at >= frozen.at || owner.joined.as_ref().is_none_or(|join| {
                join.at <= owner.admitted_at || join.at > drained_at || join.disposal_sha256 == [0; 32]
            })
        })
    {
        return None;
    }
    let (gateway_generations, receiver_generations) = counts(admission);
    Some(Terminal {
        commission: &admission.commission, fenced_at: frozen.at, drained_at,
        gateway_generations, receiver_generations, owners: &admission.owners,
    })
}

#[derive(Clone, Copy)]
enum Change<'a> {
    Commission(&'a Commission),
    Enroll { generation: Uuid, role: Role },
    Join { generation: Uuid, role: Role, disposal_sha256: [u8; 32] },
}

fn transition(record: &DeploymentRecord, change: Change<'_>) -> Result<Admission> {
    let at = record.version.checked_add(1).ok_or_else(conflict)?;
    if let Change::Commission(selected) = change {
        if let Some(current) = &record.classic_flight {
            return if current.commission == *selected { Ok((**current).clone()) } else { Err(conflict()) };
        }
        if !selects_live(record, selected)
            || selected.coverage_sha256 == [0; 32] || selected.begin_sha256 == [0; 32]
            || selected.original_catalog_sha256 == [0; 32]
            || selected.retained_path_sha256 == [0; 32]
            || !record.checkpoint_captures.get(&selected.intent.capture).is_some_and(|capture| {
                capture.incarnation == record.incarnation && capture.log_epoch == record.log_epoch
                    && capture.media_bound && capture.remote_disposal.is_none()
                    && capture.manifest_sha256.as_deref() == Some(selected.intent.source_manifest_sha256.as_str())
            })
            || record.checkpoint_capture_retirements.contains_key(&selected.intent.capture)
        {
            return Err(conflict());
        }
        if selected.prior_generations.len() > MAX_GENERATIONS { return Err(conflict()); }
        let mut owners = BTreeMap::new();
        for prior in &selected.prior_generations {
            if prior.generation.is_nil() || owners.insert(prior.generation, Owner {
                role: prior.role, admitted_at: at, joined: None,
            }).is_some() {
                return Err(conflict());
            }
        }
        return Ok(Admission { commission: selected.clone(), owners, frozen: None });
    }
    let mut admission = record.classic_flight.as_deref().ok_or_else(conflict)?.clone();
    match change {
        Change::Commission(_) => unreachable!(),
        Change::Enroll { generation, role } => {
            if generation.is_nil() || admission.frozen.is_some() || !selects_live(record, &admission.commission) {
                return Err(conflict());
            }
            if let Some(owner) = admission.owners.get(&generation) {
                return if owner.role == role && owner.joined.is_none() { Ok(admission) } else { Err(conflict()) };
            }
            if admission.owners.len() >= MAX_GENERATIONS { return Err(conflict()); }
            admission.owners.insert(generation, Owner { role, admitted_at: at, joined: None });
        }
        Change::Join { generation, role, disposal_sha256 } => {
            let owner = admission.owners.get_mut(&generation).filter(|owner| owner.role == role).ok_or_else(conflict)?;
            if disposal_sha256 == [0; 32] { return Err(conflict()); }
            if let Some(joined) = &owner.joined {
                return if joined.disposal_sha256 == disposal_sha256 { Ok(admission) } else { Err(conflict()) };
            }
            owner.joined = Some(Join { at, disposal_sha256 });
            if completely_joined(&admission)
                && let Some(frozen) = &mut admission.frozen
            {
                frozen.drained_at = Some(at);
            }
        }
    }
    Ok(admission)
}

pub(crate) fn require_update(current: &DeploymentRecord, next: &DeploymentRecord) -> Result<()> {
    if current.classic_flight == next.classic_flight { return Ok(()); }
    let proposed = next.classic_flight.as_deref().ok_or_else(conflict)?;
    let change = match current.classic_flight.as_deref() {
        None => Change::Commission(&proposed.commission),
        Some(prior) => {
            let mut changed = proposed.owners.iter().filter(|(id, owner)| prior.owners.get(*id) != Some(*owner));
            let (&generation, owner) = changed.next().ok_or_else(conflict)?;
            if changed.next().is_some() { return Err(conflict()); }
            match &owner.joined {
                None => Change::Enroll { generation, role: owner.role },
                Some(join) => Change::Join { generation, role: owner.role, disposal_sha256: join.disposal_sha256 },
            }
        }
    };
    let mut expected = current.clone();
    expected.classic_flight = Some(Box::new(transition(current, change)?));
    if next == &expected { Ok(()) } else { Err(conflict()) }
}

async fn apply(directory: &dyn Directory, source: Uuid, exact: &Commission, change: Change<'_>) -> Result<()> {
    let source = source.to_string();
    for _ in 0..MAX_CAS_ATTEMPTS {
        let mut current = directory.deployment(&source).await?.ok_or_else(conflict)?;
        if current.classic_flight.as_ref().is_some_and(|admission| admission.commission != *exact) {
            return Err(conflict());
        }
        let next = transition(&current, change)?;
        if current.classic_flight.as_deref() == Some(&next) { return Ok(()); }
        let version = current.version;
        current.classic_flight = Some(Box::new(next));
        if matches!(directory.replace_deployment(current, version).await?, Cas::Applied(_)) {
            return Ok(());
        }
    }
    Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.directory/classic-flight-order",
        "The Classic Flight admission order is busy"))
}

pub async fn install(directory: &dyn Directory, commission: &Commission) -> Result<()> {
    apply(directory, commission.intent.source, commission, Change::Commission(commission)).await
}

pub async fn enroll(directory: &dyn Directory, commission: &Commission, generation: Uuid, role: Role) -> Result<()> {
    apply(directory, commission.intent.source, commission, Change::Enroll { generation, role }).await
}

pub async fn join(
    directory: &dyn Directory, commission: &Commission, generation: Uuid, role: Role, disposal_sha256: [u8; 32],
) -> Result<()> {
    apply(directory, commission.intent.source, commission, Change::Join { generation, role, disposal_sha256 }).await
}

fn conflict() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.directory/classic-flight-order",
        "The exact Classic Flight admission or owner generation is unavailable")
}

#[cfg(test)]
pub(super) fn check_transfer_order(
    source: &DeploymentRecord, scratch: &DeploymentRecord, selection: &crate::CheckpointTransferSelection,
) {
    let commission = Commission {
        intent: selection.stage.intent.clone(),
        coverage_sha256: [1; 32], begin_sha256: [2; 32], original_catalog_sha256: [3; 32],
        retained_path_sha256: [7; 32],
        prior_generations: Vec::new(),
    };
    let apply = |record: &DeploymentRecord, change| {
        let mut next = record.clone();
        next.classic_flight = Some(Box::new(transition(record, change).unwrap()));
        require_update(record, &next).unwrap();
        next.version += 1;
        next
    };
    let mut source = apply(source, Change::Commission(&commission));
    let gateway = Uuid::new_v4();
    let receiver = Uuid::new_v4();
    source = apply(&source, Change::Enroll { generation: gateway, role: Role::Gateway });
    source = apply(&source, Change::Enroll { generation: receiver, role: Role::Receiver });
    let gateway_join = Change::Join { generation: gateway, role: Role::Gateway, disposal_sha256: [4; 32] };
    source = apply(&source, gateway_join);
    assert!(transition(&source, Change::Enroll { generation: gateway, role: Role::Gateway }).is_err());
    assert!(crate::prepare_checkpoint_transfer(&source, scratch, selection).is_err());
    let selection = crate::CheckpointTransferSelection {
        stage: selection.stage.clone(), source_version: source.version,
        destination_version: selection.destination_version, drained_source_through: selection.drained_source_through,
    };
    let (fenced, _, fence) = crate::prepare_checkpoint_transfer(&source, scratch, &selection).unwrap();
    assert!(terminal(&fenced, &fence).is_none());
    assert!(transition(&fenced, Change::Enroll { generation: Uuid::new_v4(), role: Role::Receiver }).is_err());
    let receiver_join = Change::Join { generation: receiver, role: Role::Receiver, disposal_sha256: [5; 32] };
    let drained = apply(&fenced, receiver_join);
    assert_eq!(transition(&drained, receiver_join).unwrap(), *drained.classic_flight.as_deref().unwrap());
    assert!(transition(&drained, Change::Join { generation: receiver, role: Role::Receiver, disposal_sha256: [6; 32] }).is_err());
    assert!(require_update(&drained, &fenced).is_err());
    let reopened: DeploymentRecord = serde_json::from_slice(&serde_json::to_vec(&drained).unwrap()).unwrap();
    let terminal = terminal(&reopened, &fence).unwrap();
    assert_eq!((terminal.fenced_at, terminal.drained_at), (fenced.version, drained.version));
    assert_eq!((terminal.gateway_generations, terminal.receiver_generations), (1, 1));
}
