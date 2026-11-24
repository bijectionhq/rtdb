//! The `Directory` contract suite: every backend runs it.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{claim, now_ms, park, Cas, DeploymentRecord, Directory, DisposalReceipt, ForkPreparation, ForkPublication, Lifecycle, ServingInventory, LineageChange, LineageRetirement, MemberId, MemberRecord, MemberRole, MemberState};

/// Panics on the first violation.
/// Uses fresh ids and names, so it can run against a shared, non-empty directory.
pub async fn run(directory: &dyn Directory) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let name = format!("contract-{suffix}");
    let created = directory.create_deployment(DeploymentRecord::new(&name, "acme", vec![], 0)).await.expect("create");
    assert_eq!(created.version, 1, "a new deployment starts at version 1");
    assert!(directory.create_deployment(DeploymentRecord::new(&name, "acme", vec![], 0)).await.is_err(), "names are unique");
    let installed = DeploymentRecord::new(format!("{name}-installed"), "acme", vec![], 0);
    let id = installed.id.parse().unwrap();
    directory.reserve_installed_journal(id).await.unwrap();
    directory.reserve_installed_journal(id).await.unwrap();
    assert_eq!(directory.create_deployment(installed.clone()).await.unwrap_err().code, "rtdb.directory/exists");
    assert!(matches!(directory.replace_deployment(installed.clone(), 1).await.unwrap(), Cas::Conflict(None)));
    assert!(directory.deployment(&installed.id).await.unwrap().is_none());
    assert!(directory.deployment_by_name(&installed.name).await.unwrap().is_none());
    assert!(directory.deployments().await.unwrap().iter().all(|record| record.id != installed.id));
    assert_eq!(directory.reserve_installed_journal(created.id.parse().unwrap()).await.unwrap_err().code, "rtdb.directory/exists");

    assert_eq!(directory.deployment(&created.id).await.unwrap().as_ref(), Some(&created));
    assert_eq!(directory.deployment_by_name(&name).await.unwrap().as_ref(), Some(&created));
    assert!(directory.deployments().await.unwrap().iter().any(|record| record.id == created.id));

    let holder = MemberId(format!("m-{suffix}"));
    let mut claimed = created.clone();
    claimed.epoch = 1;
    claimed.holder = Some(holder.clone());
    let applied = directory.replace_deployment(claimed.clone(), created.version).await.unwrap().applied().expect("a replace at the current version applies");
    assert_eq!(applied.version, created.version + 1);
    match directory.replace_deployment(claimed.clone(), created.version).await.unwrap() {
        Cas::Conflict(Some(current)) => assert_eq!(current, applied, "a stale replace sees the current record"),
        other => panic!("a stale replace must conflict: {other:?}"),
    }
    assert!(directory.deployments_held_by(&holder).await.unwrap().iter().any(|record| record.id == created.id));
    let mut renamed = applied.clone();
    renamed.name = format!("{name}-renamed");
    assert!(directory.replace_deployment(renamed, applied.version).await.is_err(), "a deployment's name never changes");

    let mut child = DeploymentRecord::new(format!("{name}-child"), &applied.org, vec![], 0);
    child.parent = Some(applied.id.clone());
    child.lineage = applied.lineage.clone();
    assert_eq!(directory.shared_reference_revision(&applied.lineage).await.unwrap(), 0);
    let error = directory.create_fork(child.clone(), created.version).await.unwrap_err();
    assert_eq!(error.code, "rtdb.directory/fork-parent-changed");
    assert!(directory.deployment(&child.id).await.unwrap().is_none());
    assert!(directory.deployment_by_name(&child.name).await.unwrap().is_none());
    let child = directory.create_fork(child, applied.version).await.unwrap();
    assert_eq!(directory.deployment_by_name(&child.name).await.unwrap().as_ref(), Some(&child));
    assert_eq!(directory.shared_reference_revision(&applied.lineage).await.unwrap(), 1, "publishing a shared fork advances its lineage cut");
    assert!(directory.create_fork(child.clone(), applied.version).await.is_err());
    assert_eq!(directory.shared_reference_revision(&applied.lineage).await.unwrap(), 1, "a failed publication must leave the lineage cut unchanged");

    let mut grandchild = DeploymentRecord::new(format!("{name}-grandchild"), &applied.org, vec![], 0);
    grandchild.parent = Some(child.id.clone());
    grandchild.lineage.clone_from(&child.lineage);
    let grandchild = directory.create_fork(grandchild, child.version).await.unwrap();
    assert_eq!(directory.deployment_by_name(&grandchild.name).await.unwrap().as_ref(), Some(&grandchild));
    assert_eq!(directory.shared_reference_revision(&applied.lineage).await.unwrap(), 2, "a fork of a fork must advance the physical lineage cut");

    let mut external = DeploymentRecord::new(format!("{name}-external"), "other-org", vec![], 0);
    external.parent = Some(applied.id.clone());
    let mut foreign_lineage = external.clone();
    foreign_lineage.lineage.clone_from(&applied.lineage);
    assert_eq!(directory.create_fork(foreign_lineage, applied.version).await.unwrap_err().code, "rtdb.directory/fork-parent-changed");
    let external = directory.create_fork(external, applied.version).await.unwrap();
    assert_eq!(external.lineage, external.id);
    assert_eq!(directory.deployment_by_name(&external.name).await.unwrap().as_ref(), Some(&external));
    assert_eq!(directory.shared_reference_revision(&applied.lineage).await.unwrap(), 2, "an independent fork does not change the source's reference inventory");
    assert_eq!(directory.shared_reference_revision(&external.lineage).await.unwrap(), 0);
    local_retirement(directory, &applied, &name).await;
    exact_retirement(directory, &applied, false).await;
    exact_retirement(directory, &applied, true).await;
    let applied = directory.deployment(&applied.id).await.unwrap().unwrap();

    let reserved = format!("{}{id}", super::REFERENCE_REVISION_PREFIX, id = applied.lineage);
    let fake = DeploymentRecord::new(&reserved, &applied.org, vec![], 0);
    assert_eq!(directory.create_deployment(fake).await.unwrap_err().code, "rtdb.directory/name");
    assert!(directory.deployment_by_name(&reserved).await.unwrap().is_none());
    hosted_fork_staging(directory, &applied, &name).await;

    let mut moved = DeploymentRecord::new(format!("{name}-moved"), &applied.org, vec![], 0);
    moved.incarnation = 2;
    moved.lineage = uuid::Uuid::new_v4().to_string();
    let moved = directory.create_deployment(moved).await.unwrap();
    assert!(directory.deployment(&moved.lineage).await.unwrap().is_none());
    let mut here = DeploymentRecord::new(format!("{name}-moved-child"), &moved.org, vec![], 0);
    here.parent = Some(moved.id.clone());
    here.lineage.clone_from(&moved.lineage);
    directory.create_fork(here, moved.version).await.unwrap();
    assert_eq!(directory.shared_reference_revision(&moved.lineage).await.unwrap(), 1, "a moved lineage can publish in a cell without its original root");

    let mut deleting = applied.clone();
    deleting.lifecycle = Lifecycle::Deleting;
    deleting.holder = None;
    deleting.epoch += 1;
    let deleting = directory.replace_deployment(deleting, applied.version).await.unwrap().applied().unwrap();
    assert!(directory.deployments_held_by(&holder).await.unwrap().is_empty());
    let mut refused = DeploymentRecord::new(format!("{name}-refused"), &deleting.org, vec![], 0);
    refused.parent = Some(deleting.id.clone());
    refused.lineage = deleting.lineage.clone();
    for version in [applied.version, deleting.version] {
        assert_eq!(directory.create_fork(refused.clone(), version).await.unwrap_err().code, "rtdb.directory/fork-parent-changed");
        assert!(directory.deployment(&refused.id).await.unwrap().is_none());
        assert!(directory.deployment_by_name(&refused.name).await.unwrap().is_none());
    }

    let mut later_fork = DeploymentRecord::new(format!("{name}-after-disposal"), &child.org, vec![], 0);
    later_fork.parent = Some(child.id.clone());
    later_fork.lineage.clone_from(&child.lineage);
    let previous_generation = directory.lineage_state(&child.lineage).await.unwrap().open_generation().unwrap();
    let stale_cut = ForkPublication::Remote { generation: previous_generation };
    let next_cut = ForkPublication::Remote { generation: previous_generation + 1 };
    let permit = directory.begin_reference_disposal(&deleting.id, deleting.version).await.unwrap();
    assert_eq!(directory.begin_reference_disposal(&deleting.id, deleting.version).await.unwrap(), permit);
    assert_eq!(directory.lineage_state(&child.lineage).await.unwrap().open_generation(), None);
    for cut in [None, Some(stale_cut), Some(next_cut)] {
        let refused = directory.publish_fork(later_fork.clone(), child.version, cut).await.unwrap_err();
        assert_eq!(refused.code, "rtdb.directory/lineage-changed");
    }
    assert!(directory.deployment_by_name(&later_fork.name).await.unwrap().is_none());
    let mut settled = deleting.clone();
    settled.lifecycle = Lifecycle::Deleted;
    assert!(directory.settle_reference_disposal(&permit, settled.clone(), deleting.version).await.is_err());
    assert_eq!(directory.lineage_state(&child.lineage).await.unwrap().open_generation(), None);
    settled.disposal = Some(Box::new(DisposalReceipt {
        deployment: deleting.id.clone(), incarnation: deleting.incarnation, epoch: deleting.epoch,
        journals: Default::default(), journals_retired_at_ms: 0, key_withdrawn_at_ms: 0,
        serving_prefix_observed_empty_at_ms: 0, serving_inventory: ServingInventory::LocalDirectory,
    }));
    directory.settle_reference_disposal(&permit, settled, deleting.version).await.unwrap().applied().unwrap();
    assert_eq!(directory.lineage_state(&child.lineage).await.unwrap().open_generation(), Some(previous_generation + 1));
    let stale = directory.publish_fork(later_fork.clone(), child.version, Some(stale_cut)).await.unwrap_err();
    assert_eq!(stale.code, "rtdb.directory/lineage-changed");
    assert!(directory.deployment(&later_fork.id).await.unwrap().is_none());
    assert!(directory.deployment_by_name(&later_fork.name).await.unwrap().is_none());
    directory.publish_fork(later_fork, child.version, Some(next_cut)).await.unwrap();

    let member = MemberRecord { id: MemberId(format!("member-{suffix}")), role: MemberRole::Node, address: "127.0.0.1:1".into(), control_address: Some("127.0.0.1:2".into()),
        availability_zone: "a".into(), engine_version: "0".into(), lease_expires_at_ms: 10, state: MemberState::Joining, drain_target_engine: None, pool: Default::default(), storage_namespaces: None, version: 0 };
    let registered = directory.register_member(member.clone()).await.unwrap();
    assert_eq!(registered.version, 1);
    assert!(directory.register_member(member).await.is_err(), "member ids are unique");
    let mut renewed = registered.clone();
    renewed.lease_expires_at_ms = 20;
    renewed.state = MemberState::Eligible;
    let renewed = directory.replace_member(renewed, registered.version).await.unwrap().applied().expect("lease renewal applies");
    assert!(matches!(directory.replace_member(renewed.clone(), registered.version).await.unwrap(), Cas::Conflict(Some(_))));
    assert_eq!(directory.member(&renewed.id).await.unwrap().as_ref(), Some(&renewed));
    assert!(directory.members().await.unwrap().iter().any(|member| member.id == renewed.id));

    let missing = DeploymentRecord::new(format!("missing-{suffix}"), "acme", vec![], 0);
    assert!(matches!(directory.replace_deployment(missing, 1).await.unwrap(), Cas::Conflict(None)), "replacing an absent record conflicts");
}

async fn hosted_fork_staging(directory: &dyn Directory, parent: &DeploymentRecord, name: &str) {
    let mut child = DeploymentRecord::new(format!("{name}-hosted"), &parent.org, vec![], 0);
    child.parent = Some(parent.id.clone());
    child.lineage.clone_from(&parent.lineage);
    child.storage_namespaces = parent.storage_namespaces;
    let mut rejected = child.clone();
    rejected.id = uuid::Uuid::new_v4().to_string();
    let lineage = directory.lineage_state(&parent.lineage).await.unwrap();
    let preparation = ForkPreparation { token: uuid::Uuid::new_v4(), generation: lineage.retirement_generation() };
    let abandoned = ForkPreparation { token: uuid::Uuid::new_v4(), ..preparation };
    let publication = |preparation| Some(ForkPublication::Hosted(preparation));
    for preparation in [preparation, abandoned] {
        directory.change_lineage(&parent.lineage, LineageChange::PrepareHostedFork { preparation, task: None }).await.unwrap();
    }
    assert_eq!(directory.publish_fork(child.clone(), parent.version, publication(preparation)).await.unwrap_err().code, "rtdb.directory/fork-stage-changed");
    let staged = directory.stage_hosted_fork(child.clone(), preparation).await.unwrap();
    let abandoned_stage = directory.stage_hosted_fork(rejected.clone(), abandoned).await.unwrap();
    assert_eq!(directory.deployment(&staged.id).await.unwrap().as_ref(), Some(&staged));
    assert!(directory.deployment_by_name(&staged.name).await.unwrap().is_none());
    assert_eq!(directory.create_deployment(child.clone()).await.unwrap_err().code, "rtdb.directory/exists");
    assert_eq!(directory.replace_deployment(child.clone(), staged.version).await.unwrap_err().code, "rtdb.directory/fork-stage-changed");
    let mut altered = child.clone();
    altered.incarnation += 1;
    assert_eq!(directory.publish_fork(child.clone(), parent.version, publication(preparation)).await.unwrap_err().code, "rtdb.directory/fork-stage-changed");
    let mut drained = staged.clone();
    assert!(drained.storage_regions.remove(&preparation.token).is_some());
    let drained = directory.replace_deployment(drained, staged.version).await.unwrap().applied().unwrap();
    assert_eq!(directory.publish_fork(altered, parent.version, publication(preparation)).await.unwrap_err().code, "rtdb.directory/fork-stage-changed");

    let published = directory.publish_fork(child, parent.version, publication(preparation)).await.unwrap();
    assert_eq!(published.version, drained.version + 1);
    assert_eq!(published.hosted_fork_stage, None);
    assert_eq!(directory.deployment_by_name(&published.name).await.unwrap(), Some(published.clone()));
    let mut stale_close = drained.clone();
    stale_close.lifecycle = Lifecycle::Deleting;
    stale_close.epoch += 1;
    assert_eq!(directory.replace_deployment(stale_close, drained.version).await.unwrap(), Cas::Conflict(Some(published.clone())));
    let mut drained = abandoned_stage.clone();
    assert!(drained.storage_regions.remove(&abandoned.token).is_some());
    let drained = directory.replace_deployment(drained, abandoned_stage.version).await.unwrap().applied().unwrap();
    assert_eq!(directory.publish_fork(rejected.clone(), parent.version, publication(abandoned)).await.unwrap_err().code, "rtdb.directory/exists");
    let mut closed = drained.clone();
    closed.epoch += 1;
    closed.lifecycle = Lifecycle::Deleting;
    let closed = directory.replace_deployment(closed, drained.version).await.unwrap().applied().unwrap();
    assert_eq!(directory.publish_fork(rejected, parent.version, publication(abandoned)).await.unwrap_err().code, "rtdb.directory/fork-stage-changed");
    assert_eq!(directory.deployment_by_name(&closed.name).await.unwrap(), Some(published));
    directory.change_lineage(&parent.lineage, LineageChange::ReleaseFork { preparation: abandoned }).await.unwrap();
    assert_eq!(directory.shared_reference_revision(&parent.lineage).await.unwrap(), lineage.reference_revision() + 1);
}

async fn local_retirement(directory: &dyn Directory, parent: &DeploymentRecord, name: &str) {
    let lineage = &parent.lineage;
    let local_volume = [3; 32];
    let initial = directory.lineage_state(lineage).await.unwrap();
    let generation = initial.retirement_generation();
    let preparation = ForkPreparation { token: uuid::Uuid::new_v4(), generation };
    let prepare = LineageChange::PrepareFork { preparation, local_volume };
    let retirement = LineageRetirement { token: uuid::Uuid::new_v4(), source: parent.storage_prefix(), work: crate::RetirementWork::Local { local_volume } };
    let reserve = LineageChange::ReserveRetirement {
        retirement: retirement.clone(), observed_references: initial.reference_revision(), generation,
    };
    directory.change_lineage(lineage, prepare.clone()).await.unwrap();
    assert_eq!(directory.change_lineage(lineage, reserve.clone()).await.unwrap_err().code, "rtdb.directory/lineage-changed");

    let mut child = DeploymentRecord::new(format!("{name}-prepared"), &parent.org, vec![], 0);
    child.parent = Some(parent.id.clone());
    child.lineage.clone_from(lineage);
    directory.publish_fork(child, parent.version, Some(crate::ForkPublication::Local(preparation))).await.unwrap();
    assert_eq!(directory.change_lineage(lineage, reserve).await.unwrap_err().code, "rtdb.directory/lineage-changed");

    let published = directory.lineage_state(lineage).await.unwrap();
    assert!(published.local_preparations(local_volume).next().is_none());
    assert_eq!(published.reference_revision(), initial.reference_revision() + 1);
    directory.change_lineage(lineage, LineageChange::ReserveRetirement {
        retirement: retirement.clone(), observed_references: published.reference_revision(), generation,
    }).await.unwrap();
    let mut late = DeploymentRecord::new(format!("{name}-later"), &parent.org, vec![], 0);
    late.parent = Some(parent.id.clone());
    late.lineage.clone_from(lineage);
    assert_eq!(directory.create_fork(late.clone(), parent.version).await.unwrap_err().code, "rtdb.directory/lineage-changed");
    directory.change_lineage(lineage, LineageChange::SettleRetirement { retirement }).await.unwrap();
    assert_eq!(directory.change_lineage(lineage, prepare).await.unwrap_err().code, "rtdb.directory/lineage-changed");
    assert_eq!(directory.publish_fork(late.clone(), parent.version, Some(crate::ForkPublication::Local(preparation))).await.unwrap_err().code, "rtdb.directory/lineage-changed");
    assert!(directory.deployment(&late.id).await.unwrap().is_none());

    let current = ForkPreparation { token: uuid::Uuid::new_v4(), generation: generation + 1 };
    directory.change_lineage(lineage, LineageChange::PrepareFork { preparation: current, local_volume }).await.unwrap();
    directory.publish_fork(late, parent.version, Some(crate::ForkPublication::Local(current))).await.unwrap();
}

async fn exact_retirement(directory: &dyn Directory, parent: &DeploymentRecord, tombstones: bool) {
    use rtdb_types::{PhysicalObjectVersion, S3ObjectNamespace, S3TombstoneCondition, S3TombstoneTarget};

    let lineage = &parent.lineage;
    let state = directory.lineage_state(lineage).await.unwrap();
    let scope = parent.storage_prefix();
    let target = format!("{scope}/v06/payload");
    let release = format!("{scope}/v06/~garbage/release");
    let namespace = S3ObjectNamespace { bucket: "cell".into(), endpoint: None, region: None };
    let physical_key = |logical: &str| format!("cell/{}", logical.replace('~', "%7E"));
    let version = |key: &str, version: &str| PhysicalObjectVersion {
        namespace: namespace.clone(), key: physical_key(key), version: version.to_owned(),
    };
    let tombstone = |key: &str, tag: &str| S3TombstoneTarget {
        namespace: namespace.clone(), key: physical_key(key), condition: S3TombstoneCondition::Matches(tag.to_owned()),
    };
    let work = if tombstones {
        crate::RetirementWork::Tombstones {
            deployment: parent.id.clone(), target: target.clone(), release: release.clone(),
            data_tombstone: tombstone(&target, "\"original\""), release_tombstone: tombstone(&release, "\"record\""),
            slot_origin: None,
            packed_slots: None,
        }
    } else {
        crate::RetirementWork::S3 {
            deployment: parent.id.clone(), target: target.clone(), release: release.clone(),
            version: version(&target, "original"), release_version: version(&release, "record"),
            slot_origin: None,
        }
    };
    let retirement = LineageRetirement { token: uuid::Uuid::new_v4(), source: scope, work };
    let reserve = LineageChange::ReserveRetirement {
        retirement: retirement.clone(), observed_references: state.reference_revision(), generation: state.retirement_generation(),
    };
    let reserved = directory.change_lineage(lineage, reserve).await.unwrap();
    let settle = LineageChange::SettleRetirement { retirement: retirement.clone() };
    assert_eq!(directory.change_lineage(lineage, settle.clone()).await.unwrap_err().code, "rtdb.directory/lineage-changed");
    let attempted = ForkPreparation { token: uuid::Uuid::new_v4(), generation: reserved.retirement_generation() };
    assert_eq!(directory.change_lineage(lineage, LineageChange::PrepareHostedFork { preparation: attempted, task: None }).await.unwrap_err().code,
        "rtdb.directory/lineage-changed");

    let owner = directory.deployment(&parent.id).await.unwrap().unwrap();
    assert!(directory.authorize_owned_retirement(&owner.id, owner.version, retirement.clone()).await.is_err());
    let mut partial = owner.clone();
    partial.storage_regions.insert(retirement.token, crate::StorageRegion::ExactRetirement { object: target.clone(), release: None });
    let partial = directory.replace_deployment(partial, owner.version).await.unwrap().applied().unwrap();
    assert!(directory.authorize_owned_retirement(&owner.id, partial.version, retirement.clone()).await.is_err());
    let mut fenced = partial.clone();
    fenced.storage_regions.insert(retirement.token, crate::StorageRegion::ExactRetirement { object: target, release: Some(release) });
    let fenced = directory.replace_deployment(fenced, partial.version).await.unwrap().applied().unwrap();
    assert!(directory.authorize_owned_retirement(&owner.id, partial.version, retirement.clone()).await.is_err());
    assert!(!directory.lineage_state(lineage).await.unwrap().exact_erasure_authorized());
    assert!(directory.settle_owned_version(&owner.id, fenced.version, retirement.clone()).await.is_err());

    let current = directory.authorize_owned_retirement(&owner.id, fenced.version, retirement.clone()).await.unwrap();
    assert!(current.exact_erasure_authorized());
    assert_eq!(current.retirement(), Some(&retirement));
    assert_eq!(directory.authorize_owned_retirement(&owner.id, fenced.version, retirement.clone()).await.unwrap(), current);
    assert_eq!(directory.change_lineage(lineage, LineageChange::AbandonExactRetirement { retirement: retirement.clone() }).await.unwrap_err().code,
        "rtdb.directory/lineage-changed");
    assert!(directory.change_lineage(lineage, settle).await.is_err());
    if tombstones {
        let target = format!("{}/v06/payload", retirement.source);
        let change = |key: &str, upload: &str| LineageChange::AdmitTombstoneUpload {
            retirement: retirement.token, key: key.to_owned(), upload: upload.to_owned(),
        };
        assert!(directory.change_lineage(lineage, change("outside", "one")).await.is_err());
        let uploaded = directory.change_lineage(lineage, change(&target, "one")).await.unwrap();
        assert_eq!(uploaded.tombstone_upload(retirement.token, &target).unwrap(), Some("one"));
        assert_eq!(directory.change_lineage(lineage, change(&target, "one")).await.unwrap(), uploaded);
        assert!(directory.change_lineage(lineage, change(&target, "two")).await.is_err());
        assert!(directory.settle_owned_version(&owner.id, fenced.version, retirement.clone()).await.is_err());
        let drained = |upload: &str| LineageChange::SettleTombstoneUpload {
            retirement: retirement.token, key: target.clone(), upload: upload.to_owned(),
        };
        assert!(directory.change_lineage(lineage, drained("two")).await.is_err());
        let state = directory.change_lineage(lineage, drained("one")).await.unwrap();
        assert_eq!(state.tombstone_upload(retirement.token, &target).unwrap(), None);
    }
    let wrong = directory.settle_owned_version(&owner.id, partial.version, retirement.clone()).await.unwrap();
    assert!(matches!(wrong, Cas::Conflict(Some(_))));
    let settled = directory.settle_owned_version(&owner.id, fenced.version, retirement.clone()).await.unwrap().applied().unwrap();
    assert!(!settled.storage_regions.contains_key(&retirement.token));
    assert!(directory.lineage_state(lineage).await.unwrap().retirement().is_none());
    directory.change_lineage(lineage, LineageChange::PrepareHostedFork { preparation: attempted, task: None }).await.unwrap();
    directory.change_lineage(lineage, LineageChange::ReleaseFork { preparation: attempted }).await.unwrap();
}

/// `EveryEpochHasOneClaimant` under contention: in each round, every claimant reads the deployment and races its claim through one of `directories`, and exactly one wins.
/// The winner then parks, so the next round races for the next epoch.
/// `directories` may be distinct clients of one shared directory.
pub async fn concurrent_claims(directories: &[Arc<dyn Directory>], claimants: usize, rounds: usize) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let admin = &directories[0];
    let lease = now_ms() + 3_600_000;
    let mut members = BTreeMap::new();
    for index in 0..claimants {
        let member = MemberRecord { id: MemberId(format!("claimant-{index}-{suffix}")), role: MemberRole::Node, address: String::new(), control_address: None,
            availability_zone: "a".into(), engine_version: "0".into(), lease_expires_at_ms: lease, state: MemberState::Eligible, drain_target_engine: None, pool: Default::default(), storage_namespaces: None, version: 0 };
        let registered = admin.register_member(member).await.expect("register claimant");
        members.insert(registered.id.clone(), registered);
    }
    let members = Arc::new(members);
    let id = admin.create_deployment(DeploymentRecord::new(format!("race-{suffix}"), "acme", vec![], 0)).await.expect("create").id;

    let mut last_epoch = 0;
    for round in 0..rounds {
        let mut attempts = tokio::task::JoinSet::new();
        for (index, claimant) in members.keys().cloned().enumerate() {
            let directory = directories[index % directories.len()].clone();
            let (members, id) = (members.clone(), id.clone());
            attempts.spawn(async move {
                let current = directory.deployment(&id).await.expect("read").expect("the deployment exists");
                let next = claim(&current, &claimant, &members, now_ms())?;
                directory.replace_deployment(next, current.version).await.expect("replace").applied()
            });
        }
        let mut winners = Vec::new();
        while let Some(attempt) = attempts.join_next().await {
            winners.extend(attempt.expect("claimant task"));
        }
        assert_eq!(winners.len(), 1, "round {round}: exactly one claimant wins, not {winners:?}");
        let won = winners.pop().unwrap();
        assert!(won.epoch > last_epoch, "EpochsOnlyRise: {} after {last_epoch}", won.epoch);
        let holder = won.holder.clone().expect("a claim names its holder");
        let parked = park(&won, &holder, won.epoch).expect("the winner may park");
        last_epoch = admin.replace_deployment(parked, won.version).await.expect("park").applied().expect("nobody else writes a held deployment").epoch;
    }
}
