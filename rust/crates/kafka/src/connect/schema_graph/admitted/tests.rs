use std::cell::{Cell, RefCell};
use super::*;

fn builder() -> (GraphBuilder, NodeId) {
    let mut builder = GraphBuilder::new();
    let bytes = builder.insert(Node { kind: NodeKind::Scalar(Scalar::Bytes), metadata: Metadata { name: Some("base with a longer name".into()), default: Some(Value::Binary(vec![1; 4096])), parameters: BTreeMap::from([("base".into(), "base".into()), ("shared".into(), "base".into())]), ..Default::default() } });
    let inner = builder.insert(Node { kind: NodeKind::Alias(bytes), metadata: Metadata { name: Some("inner".into()), default: Some(Value::Binary(vec![2; 8192])), parameters: BTreeMap::from([("inner".into(), "inner".into()), ("shared".into(), "inner".into())]), ..Default::default() } });
    let outer = builder.insert(Node { kind: NodeKind::Alias(inner), metadata: Metadata { optional: true, version: Some(7), parameters: BTreeMap::from([("outer".into(), "outer".into()), ("shared".into(), "outer".into())]), ..Default::default() } });
    let array = builder.insert(Node { kind: NodeKind::Array(outer), metadata: Metadata::default() });
    let key = builder.insert(Node { kind: NodeKind::Scalar(Scalar::String), metadata: Metadata::default() });
    let map = builder.insert(Node { kind: NodeKind::Map(key, outer), metadata: Metadata::default() });
    let root = builder.reserve();
    let next = builder.insert(Node { kind: NodeKind::Alias(root), metadata: Metadata { optional: true, ..Default::default() } });
    builder.define(root, Node { kind: NodeKind::Struct(vec![("payload".into(), outer), ("list".into(), array), ("map".into(), map), ("next".into(), next)]), metadata: Metadata { name: Some("Root".into()), ..Default::default() } }).unwrap();
    (builder, root)
}
fn same(left: SchemaView<'_>, right: SchemaView<'_>, seen: &mut BTreeSet<((usize, usize), (usize, usize))>) {
    if let (Some(left), Some(right)) = (left.identity(), right.identity()) { if !seen.insert((left, right)) { return; } }
    assert_eq!(left.optional(), right.optional());
    assert_eq!(left.name(), right.name());
    assert_eq!(left.version(), right.version());
    assert_eq!(left.doc(), right.doc());
    assert_eq!(left.parameters().collect::<Vec<_>>(), right.parameters().collect::<Vec<_>>());
    assert_eq!(left.default_value(), right.default_value());
    match (left.kind(), right.kind()) {
        (ViewKind::Scalar(left), ViewKind::Scalar(right)) => assert_eq!(left, right),
        (ViewKind::Array(left), ViewKind::Array(right)) => same(left, right, seen),
        (ViewKind::Map(lk, lv), ViewKind::Map(rk, rv)) => { same(lk, rk, seen); same(lv, rv, seen); },
        (ViewKind::Struct(left), ViewKind::Struct(right)) => {
            assert_eq!(left.len(), right.len());
            for (left, right) in left.zip(right) { assert_eq!(left.name, right.name); same(left.schema, right.schema, seen); }
        }
        _ => panic!("materialized node kinds differ"),
    }
}

#[test]
fn admitted_graph_matches_legacy_metadata_kinds_cycles_and_callback_order() {
    let legacy_order = RefCell::new(Vec::new());
    let admitted_order = RefCell::new(Vec::new());
    let (legacy, root) = builder();
    let legacy = legacy.finish_with(root, |id, schema| { legacy_order.borrow_mut().push(id); Ok(Metadata::from_schema(schema)) }).unwrap();
    let (admitted, root) = builder();
    let admitted = admitted.finish_with_admission(root, |_| Ok(()), |id, view, admit| { admitted_order.borrow_mut().push(id); view.metadata_owned(admit) }).unwrap();
    assert_eq!(legacy_order.into_inner(), admitted_order.into_inner());
    same(legacy.borrowed(), admitted.borrowed(), &mut BTreeSet::new());
    assert_eq!(admitted.field("payload").unwrap().schema.name.as_deref(), Some("inner"));
    assert_eq!(admitted.field("payload").unwrap().schema.parameters.get("shared").map(String::as_str), Some("outer"));
}

#[test]
fn every_node_materializes_under_its_checked_owned_bound() {
    let (builder, root) = builder();
    let root = builder.finish_with_admission(root, |_| Ok(()), |_, view, admit| view.metadata_owned(admit)).unwrap();
    let SchemaKind::Struct(fields) = &root.kind else { panic!("expected Struct") };
    let SchemaKind::Reference(reference) = &fields[0].schema.kind else { panic!("expected graph child") };
    for index in 0..reference.graph.nodes.len() {
        let id = NodeId(index);
        assert!(owned_view_bound(&reference.graph, id).unwrap() > 0);
        let owned = owned_view(&reference.graph, id);
        same(SchemaView::node(&reference.graph, id), owned.borrowed(), &mut BTreeSet::new());
    }
}

#[test]
fn structural_rejection_precedes_all_metadata_callbacks() {
    let calls = Cell::new(0);
    let (builder, root) = builder();
    let result = builder.finish_with_admission(root, |_| Err(error("admission-test", "reject structural storage")), |_, view, admit| { calls.set(calls.get() + 1); view.metadata_owned(admit) });
    assert!(result.is_err());
    assert_eq!(calls.get(), 0);
}

#[test]
fn invalid_nodes_edges_duplicates_and_alias_cycles_never_reach_borrowed_callbacks() {
    let cases = [
        (GraphBuilder { nodes: vec![None] }, NodeId(0)),
        (GraphBuilder { nodes: vec![Some(Node { kind: NodeKind::Scalar(Scalar::I32), metadata: Metadata::default() })] }, NodeId(1)),
        (GraphBuilder { nodes: vec![Some(Node { kind: NodeKind::Array(NodeId(9)), metadata: Metadata::default() })] }, NodeId(0)),
        (GraphBuilder { nodes: vec![Some(Node { kind: NodeKind::Alias(NodeId(0)), metadata: Metadata::default() })] }, NodeId(0)),
        (GraphBuilder { nodes: vec![Some(Node { kind: NodeKind::Struct(vec![("same".into(), NodeId(0)), ("same".into(), NodeId(0))]), metadata: Metadata::default() })] }, NodeId(0)),
    ];
    for (builder, root) in cases {
        let calls = Cell::new(0);
        assert!(builder.finish_with_admission(root, |_| Ok(()), |_, view, admit| { calls.set(calls.get() + 1); view.metadata_owned(admit) }).is_err());
        assert_eq!(calls.get(), 0);
    }
}

#[test]
fn default_copy_rejection_propagates_before_owned_metadata_is_returned() {
    let mut schema = ConnectSchema::new(SchemaKind::Bytes);
    schema.default = Some(Value::Binary(vec![7; 65536]));
    let source = schema.default.as_ref().unwrap() as *const Value;
    let calls = Cell::new(0);
    let result = schema.borrowed().metadata_owned(&mut |bytes| { calls.set(calls.get() + 1); assert!(bytes >= 65536); Err(error("admission-test", "reject default copy")) });
    assert!(result.is_err());
    assert_eq!(calls.get(), 1);
    assert_eq!(schema.default.as_ref().unwrap() as *const Value, source);
}

#[test]
fn final_root_rejection_happens_after_decorations_without_publishing_a_graph() {
    let (builder, root) = builder();
    let nodes = builder.nodes.len();
    let calls = Cell::new(0);
    let decorations = Cell::new(0);
    let result = builder.finish_with_admission(root, |_| {
        calls.set(calls.get() + 1);
        if calls.get() == nodes + 2 { Err(error("admission-test", "reject final root")) } else { Ok(()) }
    }, |_, view, admit| { decorations.set(decorations.get() + 1); view.metadata_owned(admit) });
    assert!(result.is_err());
    assert_eq!(decorations.get(), nodes);
    assert_eq!(calls.get(), nodes + 2);
}

#[test]
fn exact_additional_limit_admits_and_one_byte_less_rejects_with_last_lease_release() {
    let sizes = RefCell::new(Vec::new());
    let (first, root) = builder();
    drop(first.finish_with_admission(root, |bytes| { sizes.borrow_mut().push(bytes); Ok(()) }, |_, view, admit| view.metadata_owned(admit)).unwrap());
    let total = sizes.into_inner().into_iter().try_fold(0usize, add).unwrap();
    for (limit, success) in [(total, true), (total - 1, false)] {
        let budget = rtdb_types::MemoryBudget::new(limit);
        let mut leases = Vec::new();
        let (builder, root) = builder();
        let output = builder.finish_with_admission(root, |bytes| { leases.push(budget.reserve(bytes)?); Ok(()) }, |_, view, admit| view.metadata_owned(admit));
        assert_eq!(output.is_ok(), success);
        if success { assert_eq!(budget.used_bytes(), total); }
        drop(output);
        drop(leases);
        assert_eq!(budget.used_bytes(), 0);
    }
}

#[test]
fn nullable_defaults_normalize_before_callback_and_after_metadata_replacement() {
    let mut builder = GraphBuilder::new();
    let root = builder.insert(Node { kind: NodeKind::Scalar(Scalar::I64), metadata: Metadata { default: Some(Value::Null), ..Default::default() } });
    let root = builder.finish_with_admission(root, |_| Ok(()), |_, view, admit| {
        assert_eq!(view.default_value(), None);
        let mut metadata = view.metadata_owned(admit)?;
        metadata.default = Some(Value::Null);
        Ok(metadata)
    }).unwrap();
    assert_eq!(root.default_value(), None);
}

#[test]
fn legacy_view_bounds_include_all_alias_defaults_and_child_materializations() {
    let (builder, root) = builder();
    let root = builder.finish_with_admission(root, |_| Ok(()), |_, view, admit| view.metadata_owned(admit)).unwrap();
    let SchemaKind::Struct(fields) = &root.kind else { panic!("expected Struct") };
    let SchemaKind::Reference(reference) = &fields[0].schema.kind else { panic!("expected graph child") };
    let graph = &reference.graph;
    let aliases = legacy_metadata_bound(graph, NodeId(2)).unwrap();
    assert!(aliases > graph.nodes[..3].iter().map(|node| node.metadata.default.as_ref().map_or(0, |value| value.retained_bytes().unwrap())).sum::<usize>());
    for index in 0..graph.nodes.len() { assert!(graph.view_bytes >= legacy_view_bound(graph, NodeId(index)).unwrap()); }
}

#[test]
fn checked_structural_and_metadata_arithmetic_rejects_overflow() {
    assert!(add(usize::MAX, 1).is_err());
    assert!(mul(usize::MAX, 2).is_err());
    assert!(tree_bound(usize::MAX, size_of::<(String, String)>()).is_err());
}

#[test]
fn concrete_outer_default_is_counted_separately_from_its_graph_node() {
    let mut builder = GraphBuilder::new();
    let scalar = builder.insert(Node { kind: NodeKind::Scalar(Scalar::Bytes), metadata: Metadata::default() });
    let root = builder.insert(Node { kind: NodeKind::Array(scalar), metadata: Metadata::default() });
    let root = builder.finish_with_admission(root, |_| Ok(()), |_, view, admit| view.metadata_owned(admit)).unwrap();
    let SchemaKind::Array(element) = &root.kind else { panic!("expected array") };
    let mut concrete = element.as_ref().clone();
    let before = concrete.borrowed().legacy_resolution_bound().unwrap();
    concrete.default = Some(Value::Binary(vec![9; 1_048_576]));
    let after = concrete.borrowed().legacy_resolution_bound().unwrap();
    assert!(after >= before + 1_048_576);
    assert_eq!(ConnectSchema::new(SchemaKind::Bytes).borrowed().legacy_resolution_bound().unwrap(), 0);
}

#[test]
fn repeated_field_views_charge_and_copy_the_same_default_for_each_child_owner() {
    let mut builder = GraphBuilder::new();
    let child = builder.insert(Node { kind: NodeKind::Scalar(Scalar::Bytes), metadata: Metadata { default: Some(Value::Binary(vec![6; 65536])), ..Default::default() } });
    let root = builder.insert(Node { kind: NodeKind::Struct(vec![("first".into(), child), ("second".into(), child)]), metadata: Metadata::default() });
    let last = Cell::new(0);
    let root = builder.finish_with_admission(root, |bytes| { last.set(bytes); Ok(()) }, |_, view, admit| view.metadata_owned(admit)).unwrap();
    assert!(last.get() >= 2 * 65536);
    let SchemaKind::Struct(fields) = &root.kind else { panic!("expected Struct") };
    let Value::Binary(first) = fields[0].schema.default_value().unwrap() else { panic!("expected binary default") };
    let Value::Binary(second) = fields[1].schema.default_value().unwrap() else { panic!("expected binary default") };
    assert_eq!(first, second);
    assert_ne!(first.as_ptr(), second.as_ptr());
}
