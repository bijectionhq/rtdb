//! The `Directory` contract suite: every backend runs it.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{claim, now_ms, park, Cas, DeploymentRecord, Directory, Lifecycle, MemberId, MemberRecord, MemberRole, MemberState};

/// Panics on the first violation.
/// Uses fresh ids and names, so it can run against a shared, non-empty directory.
pub async fn run(directory: &dyn Directory) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let name = format!("contract-{suffix}");
    let created = directory.create_deployment(DeploymentRecord::new(&name, "acme", vec![], 0)).await.expect("create");
    assert_eq!(created.version, 1, "a new deployment starts at version 1");
    assert!(directory.create_deployment(DeploymentRecord::new(&name, "acme", vec![], 0)).await.is_err(), "names are unique");

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

    let reserved = format!("{}{id}", super::REFERENCE_REVISION_PREFIX, id = applied.lineage);
    let fake = DeploymentRecord::new(&reserved, &applied.org, vec![], 0);
    assert_eq!(directory.create_deployment(fake).await.unwrap_err().code, "rtdb.directory/name");
    assert!(directory.deployment_by_name(&reserved).await.unwrap().is_none());

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
