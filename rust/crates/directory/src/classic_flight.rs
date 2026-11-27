//! Source-directory order for ordinary Classic FreshFlight.

use std::collections::BTreeMap;

use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Checkpoint, CheckpointStage, CheckpointStageIntent, CheckpointTransferFence, ClassicMigrationSource, DeploymentRecord, Directory, Lifecycle, ReplaceOutcome, Version};

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
        && record.checkpoint_stage.is_none()
        && record.checkpoint_transfer_fence.is_none()
        && record.checkpoint_transfer_terminal.is_none()
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

pub(crate) fn freeze(record: &mut DeploymentRecord, fence: &CheckpointTransferFence, at: Version) -> Result<()> {
    if let Some(admission) = &mut record.classic_flight {
        if !selects_fence(&admission.commission, fence) { return Err(conflict()); }
        let drained_at = completely_joined(admission).then_some(at);
        admission.frozen = Some(Frozen { at, drained_at });
    }
    Ok(())
}

fn exact_scratch<'a>(record: &'a DeploymentRecord, intent: &CheckpointStageIntent) -> Option<&'a CheckpointStage> {
    let migration = intent.classic_migration?;
    record.checkpoint_stage.as_deref().filter(|stage| {
        stage.intent == *intent && stage.transfer_only
            && stage.source_incarnation == migration.source.incarnation
            && stage.source_log_epoch == migration.source.log_epoch
            && record.id == intent.destination.to_string() && record.lineage == record.id
            && record.name == intent.name && record.org == intent.org && record.incarnation == 1
            && record.holder.is_none() && record.parent.is_none() && record.fork_stage.is_none()
            && record.checkpoint.is_none() && record.ensemble.is_empty() && record.replica_journals.is_empty()
            && record.move_from.is_none() && record.moved_at_ms.is_none()
            && record.checkpoint_transfer_fence.is_none() && record.checkpoint_transfer_terminal.is_none()
            && record.checkpoint_transfer_bootstrap.is_none() && record.checkpoint_transfer_replica.is_none()
            && record.checkpoint_transfer_activation.is_none()
    })
}

fn registered_checkpoint(record: &DeploymentRecord, intent: &CheckpointStageIntent) -> Option<Checkpoint> {
    if record.checkpoint_capture_retirements.contains_key(&intent.capture) { return None; }
    record.checkpoint_captures.get(&intent.capture).filter(|capture| {
        capture.incarnation == record.incarnation && capture.log_epoch == record.log_epoch
            && capture.media_bound && capture.remote_disposal.is_none()
            && capture.manifest_sha256.as_deref() == Some(intent.source_manifest_sha256.as_str())
    }).map(|capture| capture.checkpoint)
}

pub(crate) fn prepare_rebind(
    source: &DeploymentRecord, excluded: &DeploymentRecord, replacement_scratch: Option<&DeploymentRecord>,
    previous: &Commission, replacement: &Commission,
) -> Result<DeploymentRecord> {
    let mut admission = source.classic_flight.as_deref().filter(|admission| {
        admission.commission == *previous && admission.frozen.is_none()
    }).ok_or_else(conflict)?.clone();
    let prior = &previous.intent;
    let next = &replacement.intent;
    if !selects_live(source, previous) || next.source != prior.source || next.org != prior.org
        || next.classic_migration != prior.classic_migration
        || next.capture == prior.capture || next.capture.is_nil()
        || next.operation != next.destination || next.name != format!("checkpoint-stage-{}", next.operation)
        || next.operation.is_nil() || next.operation == prior.operation || next.destination == prior.destination
        || next.destination == next.source
    {
        return Err(conflict());
    }
    admission.commission.intent = next.clone();
    if admission.commission != *replacement { return Err(conflict()); }
    let old_stage = exact_scratch(excluded, prior).ok_or_else(conflict)?;
    if !matches!(excluded.lifecycle, Lifecycle::Deleting | Lifecycle::Deleted)
        || excluded.lifecycle == Lifecycle::Deleted && excluded.disposal.is_none()
    {
        return Err(conflict());
    }
    let checkpoint = registered_checkpoint(source, next).ok_or_else(conflict)?;
    if source.checkpoint != Some(checkpoint) || checkpoint.root_sha256.is_none()
        || !checkpoint.overtakes(old_stage.checkpoint)
        || replacement_scratch.is_some_and(|record| {
            record.lifecycle != Lifecycle::Restoring
                || exact_scratch(record, next).is_none_or(|stage| stage.checkpoint != checkpoint)
        })
    {
        return Err(conflict());
    }
    let mut rebound = source.clone();
    rebound.version = source.version.checked_add(1).ok_or_else(conflict)?;
    rebound.classic_flight = Some(Box::new(admission));
    Ok(rebound)
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
            || registered_checkpoint(record, &selected.intent).is_none_or(|checkpoint| record.checkpoint != Some(checkpoint))
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
        match directory.replace_deployment(current, version).await {
            ReplaceOutcome::Applied(_) => return Ok(()),
            ReplaceOutcome::Conflict(_) => {}
            ReplaceOutcome::Refused(error) | ReplaceOutcome::Unknown(error) => return Err(error),
        }
    }
    Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.directory/classic-flight-order",
        "The Classic Flight admission order is busy"))
}

pub async fn install(directory: &dyn Directory, commission: &Commission) -> Result<()> {
    let source = directory.deployment(&commission.intent.source.to_string()).await?.ok_or_else(conflict)?;
    if let Some(previous) = self::commission(&source) && previous != commission {
        return directory.rebind_classic_flight(previous, commission).await;
    }
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

pub(crate) fn conflict() -> Anomaly {
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

#[tokio::test]
async fn overtaken_capture_rebind_preserves_joined_and_live_generations() {
    use crate::{CellMedia, CheckpointCapture, CheckpointTransferSelection, ClassicMigration, MemberId, MemoryDirectory};

    let directory = MemoryDirectory::new();
    let mut source = DeploymentRecord::new("original", "cell", vec![], 0);
    source.epoch = 4;
    source.log_epoch = 2;
    source.holder = Some(MemberId::fresh());
    let migration = ClassicMigration {
        source: ClassicMigrationSource { incarnation: source.incarnation, log_epoch: source.log_epoch, writer_epoch: source.epoch },
        destination: Uuid::new_v4(),
    };
    let stage = |block, capture, manifest: &str| {
        let scratch = Uuid::new_v4();
        let mut record = DeploymentRecord::new(format!("checkpoint-stage-{scratch}"), "cell", vec![], 0);
        record.id = scratch.to_string();
        record.lineage = record.id.clone();
        record.lifecycle = Lifecycle::Restoring;
        let checkpoint = Checkpoint { block, boundary: block + 1, source: block, system_time: block, root_sha256: Some([1; 32]) };
        let stage = CheckpointStage {
            intent: CheckpointStageIntent {
                operation: scratch, destination: scratch, name: record.name.clone(), org: record.org.clone(),
                source: source.id.parse().unwrap(), capture, source_manifest_sha256: manifest.into(),
                classic_migration: Some(migration),
            },
            checkpoint, source_incarnation: source.incarnation, source_log_epoch: source.log_epoch,
            media: CellMedia { objects: [2; 32], keys: "keys".into() },
            transfer_only: true, staged_manifest_sha256: Some("staged".into()),
        };
        record.checkpoint_stage = Some(Box::new(stage.clone()));
        let registration = CheckpointCapture {
            archive: "archive".into(), key_namespace: None, incarnation: stage.source_incarnation,
            log_epoch: stage.source_log_epoch, checkpoint, automatic_fork: false, world_replay: None,
            media_bound: true, manifest_sha256: Some(manifest.into()), remote_disposal: None,
        };
        (record, stage, registration)
    };
    let (old_scratch, old_stage, old_capture) = stage(1, Uuid::new_v4(), "old");
    let (new_scratch, new_stage, new_capture) = stage(2, Uuid::new_v4(), "replacement");
    source.checkpoint = Some(old_stage.checkpoint);
    source.checkpoint_captures.insert(old_stage.intent.capture, old_capture);
    let source = directory.create_deployment(source).await.unwrap();
    let gateway = Uuid::new_v4();
    let receiver = Uuid::new_v4();
    let previous = Commission {
        intent: old_stage.intent, coverage_sha256: [1; 32], begin_sha256: [2; 32], original_catalog_sha256: [3; 32],
        retained_path_sha256: [4; 32], prior_generations: vec![Generation { generation: gateway, role: Role::Gateway }],
    };
    let replacement = Commission { intent: new_stage.intent.clone(), ..previous.clone() };
    install(directory.as_ref(), &previous).await.unwrap();
    enroll(directory.as_ref(), &previous, receiver, Role::Receiver).await.unwrap();
    join(directory.as_ref(), &previous, gateway, Role::Gateway, [5; 32]).await.unwrap();

    let mut advanced = directory.deployment(&source.id).await.unwrap().unwrap();
    advanced.checkpoint = Some(new_stage.checkpoint);
    advanced.checkpoint_captures.insert(new_stage.intent.capture, new_capture);
    let version = advanced.version;
    let advanced = directory.replace_deployment(advanced, version).await.applied().unwrap();
    let roster = advanced.classic_flight.as_deref().unwrap().owners.clone();
    let new_scratch = directory.create_deployment(new_scratch).await.unwrap();
    let mut seal = CheckpointTransferSelection {
        stage: new_stage, source_version: advanced.version, destination_version: new_scratch.version,
        drained_source_through: Some(2),
    };
    assert!(directory.fence_checkpoint_transfer(&seal).await.is_err());
    assert!(install(directory.as_ref(), &replacement).await.is_err());
    let mut old_scratch = directory.create_deployment(old_scratch).await.unwrap();
    assert!(install(directory.as_ref(), &replacement).await.is_err());
    old_scratch.lifecycle = Lifecycle::Deleting;
    let mut ambiguous = old_scratch.clone();
    ambiguous.checkpoint_transfer_fence = Some(Box::new(CheckpointTransferFence {
        destination: ambiguous.checkpoint_stage.as_deref().unwrap().clone(),
        source_writer_epoch: advanced.epoch,
        fenced_writer_epoch: advanced.epoch + 1,
        drained_source_through: Some(1),
    }));
    assert!(prepare_rebind(&advanced, &ambiguous, Some(&new_scratch), &previous, &replacement).is_err());
    let version = old_scratch.version;
    assert!(directory.replace_deployment(old_scratch, version).await.applied().is_some());
    let wrong_coverage = Commission { coverage_sha256: [8; 32], ..replacement.clone() };
    assert!(install(directory.as_ref(), &wrong_coverage).await.is_err());

    install(directory.as_ref(), &replacement).await.unwrap();
    let rebound = directory.deployment(&source.id).await.unwrap().unwrap();
    assert!(excludes_unclassified(&rebound));
    assert_eq!(rebound.classic_flight.as_deref().unwrap().owners, roster);
    assert!(install(directory.as_ref(), &previous).await.is_err());
    assert!(directory.fence_checkpoint_transfer(&seal).await.is_err());
    seal.source_version = rebound.version;
    let fence = directory.fence_checkpoint_transfer(&seal).await.unwrap();
    install(directory.as_ref(), &replacement).await.unwrap();
    let fenced = directory.deployment(&source.id).await.unwrap().unwrap();
    assert!(terminal(&fenced, &fence).is_none());
    assert!(install(directory.as_ref(), &previous).await.is_err());
    join(directory.as_ref(), &replacement, receiver, Role::Receiver, [6; 32]).await.unwrap();
    let drained = directory.deployment(&source.id).await.unwrap().unwrap();
    let reopened: DeploymentRecord = serde_json::from_slice(&serde_json::to_vec(&drained).unwrap()).unwrap();
    let terminal = terminal(&reopened, &fence).unwrap();
    assert_eq!(terminal.commission, &replacement);
    assert_eq!((terminal.gateway_generations, terminal.receiver_generations), (1, 1));
    assert_eq!(terminal.owners.get(&gateway), roster.get(&gateway));
}
