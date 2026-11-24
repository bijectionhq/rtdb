use super::*;

fn grouped(deadline: i64, groups: &[(&str, &[u8])]) -> Vec<u8> {
    let mut body = deadline.to_be_bytes().to_vec();
    body.extend_from_slice(&(groups.len() as u16).to_be_bytes());
    for (name, ipc) in groups {
        body.extend_from_slice(&(name.len() as u16).to_be_bytes());
        body.extend_from_slice(name.as_bytes());
        body.extend_from_slice(&(ipc.len() as u32).to_be_bytes());
        body.extend_from_slice(ipc);
    }
    body
}

#[test]
fn hosted_refusal_carries_the_public_clock_version_group() {
    let attempt = ("fact_attempts", &b"attempt"[..]);
    let head = ("fact_publications", &b"head"[..]);
    let clock = (rtdb_types::OWNED_PUBLIC_ROOT_VERSIONS_RELATION, &b"clock"[..]);
    let ordinary = grouped(0, &[attempt, head]);
    let public = grouped(0, &[attempt, head, clock]);
    let foreign = grouped(0, &[attempt, head, ("items", b"data")]);

    assert_eq!(hosted_refusal_fact_groups(&ordinary[8..]).unwrap().len(), 2);
    let parsed = hosted_refusal_fact_groups(&public[8..]).unwrap();
    assert_eq!(parsed[2].0.name(), clock.0);
    assert_eq!(parsed[2].1, clock.1);
    assert!(hosted_refusal_fact_groups(&foreign[8..]).is_err());
}

#[test]
fn grouped_commit_frames_each_ipc_stream_in_mutation_order() {
    let body = grouped(-4, &[("items", b"first"), ("orders", b"second")]);
    let request = CommitRequest::parse(&body, CommitFormat::Grouped).unwrap();
    assert_eq!(request.deadline, Timestamp(-4));
    assert_eq!(request.groups[0].0.name(), "items");
    assert_eq!(request.groups[0].1, b"first");
    assert_eq!(request.groups[1].0.name(), "orders");
    assert_eq!(request.groups[1].1, b"second");
}

#[test]
fn grouped_commit_accepts_a_deadline_without_relation_groups() {
    let body = grouped(17, &[]);
    let request = CommitRequest::parse(&body, CommitFormat::Grouped).unwrap();
    assert_eq!(request.deadline, Timestamp(17));
    assert!(request.groups.is_empty());
}

#[test]
fn grouped_commit_refuses_duplicate_truncated_and_trailing_groups() {
    for groups in [
        &[("items", &b"first"[..]), ("items", &b"second"[..])][..],
        &[("items", &b""[..])],
    ] {
        assert!(CommitRequest::parse(&grouped(1, groups), CommitFormat::Grouped).is_err());
    }
    let oversized = grouped(1, &vec![("items", &b"ipc"[..]); MAX_COMMIT_GROUPS + 1]);
    assert!(CommitRequest::parse(&oversized, CommitFormat::Grouped).is_err());
    let mut body = grouped(1, &[("items", b"first"), ("orders", b"second")]);
    let truncated = &body[..body.len() - 1];
    assert!(CommitRequest::parse(truncated, CommitFormat::Grouped).is_err());
    body.push(0);
    assert!(CommitRequest::parse(&body, CommitFormat::Grouped).is_err());
}

#[test]
fn single_commit_produces_the_same_group_view() {
    let mut body = b"\0\x05items".to_vec();
    body.extend_from_slice(&(-4i64).to_be_bytes());
    body.extend_from_slice(b"first");
    let request = CommitRequest::parse(&body, CommitFormat::Single).unwrap();
    assert_eq!(request.deadline, Timestamp(-4));
    assert_eq!(request.groups.len(), 1);
    assert_eq!(request.groups[0].0.name(), "items");
    assert_eq!(request.groups[0].1, b"first");
}

#[test]
fn capacity_declaration_decodes_one_exact_root_projection() {
    let root = OwnedRelation::new("items").unwrap();
    let mut fields = StructFields::from([
        ("column".into(), Value::Utf8("partition".into())),
        ("prefix_bytes".into(), Value::I64(0)),
        ("cap".into(), Value::I64(3)),
        ("when".into(), Value::Binary(vec![])),
    ]);
    assert!(matches!(
        owned_capacity_declaration(root.clone(), &fields).unwrap(),
        TxOp::OwnedDeclareCapacity { relation, column, prefix_bytes: 0, cap: 3, when }
            if relation == root && column == "partition" && when.is_empty()
    ));
    fields.insert("extra".into(), Value::I64(1));
    assert!(owned_capacity_declaration(root.clone(), &fields).is_err());
    fields.remove("extra");
    fields.insert("cap".into(), Value::Utf8("3".into()));
    assert!(owned_capacity_declaration(root, &fields).is_err());
}

#[test]
fn conditional_hosted_spatial_readiness_requires_a_root_catalog_replacement() {
    let scope = rtdb_types::PlacedAttemptScope::new("spatial-with-conditions", 1).unwrap();
    let root = OwnedRelation::new("bj_fact_root_owner").unwrap();
    let row = || StructFields::from([("_id".into(), Value::Utf8("root".into()))]);
    let write = rtdb_tx::HostedControlWrite::new(root.clone(), scope, Some(Timestamp(1)), row()).unwrap();
    let check = rtdb_tx::HostedControlWrite::check(root.clone(), scope, Some(Timestamp(1)), row()).unwrap();
    let fresh = rtdb_tx::HostedControlWrite::new(root, scope, None, row()).unwrap();

    let effects = CommitEffects::hosted(false, true, &[write.clone()]);
    assert!(matches!(effects, CommitEffects::HostedCatalog));
    assert!(effects.allows_spatial_requirement());
    assert!(!CommitEffects::hosted(false, true, &[check, fresh]).allows_spatial_requirement());
    assert!(!CommitEffects::hosted(true, true, &[write]).allows_spatial_requirement());
}
