//! The `Directory` contract suite: every backend runs it.

use crate::{Cas, DeploymentRecord, Directory, MemberId, MemberRecord, MemberRole, MemberState};

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

    let member = MemberRecord { id: MemberId(format!("member-{suffix}")), role: MemberRole::Node, address: "127.0.0.1:1".into(), control_address: Some("127.0.0.1:2".into()),
        availability_zone: "a".into(), engine_version: "0".into(), lease_expires_at_ms: 10, state: MemberState::Joining, version: 0 };
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
