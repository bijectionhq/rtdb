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
