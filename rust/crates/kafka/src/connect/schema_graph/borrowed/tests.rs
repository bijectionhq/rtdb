use super::*;

fn metadata(name: Option<&str>, optional: bool, parameters: &[(&str, &str)], default: Option<Value>) -> Metadata {
    Metadata { name: name.map(str::to_owned), optional, parameters: parameters.iter().map(|(name, value)| ((*name).into(), (*value).into())).collect(), default, ..Default::default() }
}

#[test]
fn graph_alias_metadata_keeps_outer_precedence_and_sorted_unique_parameters() {
    let graph = SchemaGraph {
        nodes: vec![
            Node { kind: NodeKind::Scalar(Scalar::Bytes), metadata: Metadata { version: Some(1), doc: Some("base doc".into()), ..metadata(Some("base"), true, &[("base", "b"), ("shared", "base")], Some(Value::Binary(vec![1, 2]))) } },
            Node { kind: NodeKind::Alias(NodeId(0)), metadata: Metadata { doc: Some("inner doc".into()), ..metadata(Some("inner"), false, &[("inner", "i"), ("shared", "inner")], Some(Value::Binary(vec![3, 4]))) } },
            Node { kind: NodeKind::Alias(NodeId(1)), metadata: Metadata { version: Some(9), ..metadata(None, false, &[("outer", "o"), ("shared", "outer")], None) } },
        ], retained_bytes: 0, view_bytes: 0,
    };
    let view = SchemaView::node(&graph, NodeId(2));
    assert!(view.optional());
    assert_eq!(view.name(), Some("inner"));
    assert_eq!(view.version(), Some(9));
    assert_eq!(view.doc(), Some("inner doc"));
    assert_eq!(view.default_value(), Some(&Value::Binary(vec![3, 4])));
    assert!(std::ptr::eq(view.default_value().unwrap(), graph.nodes[1].metadata.default.as_ref().unwrap()));
    assert_eq!(view.parameter("shared"), Some("outer"));
    assert_eq!(view.parameter("missing"), None);
    assert_eq!(view.parameters().collect::<Vec<_>>(), [("base", "b"), ("inner", "i"), ("outer", "o"), ("shared", "outer")]);
    assert!(matches!(view.kind(), ViewKind::Scalar(Scalar::Bytes)));
}

#[test]
fn concrete_reference_metadata_is_authoritative_even_when_graph_metadata_is_present() {
    let mut builder = GraphBuilder::new();
    let scalar = builder.insert(Node { kind: NodeKind::Scalar(Scalar::I32), metadata: metadata(Some("graph child"), true, &[("child", "value")], Some(Value::I32(4))) });
    let root = builder.insert(Node { kind: NodeKind::Struct(vec![("first".into(), scalar)]), metadata: metadata(Some("graph root"), true, &[("root", "value")], None) });
    let schema = builder.finish(root).unwrap();
    let mut concrete = schema.as_ref().clone();
    let SchemaKind::Struct(fields) = &mut concrete.kind else { panic!("expected Struct") };
    let child = Arc::make_mut(&mut fields[0].schema);
    child.optional = false;
    child.name = None;
    child.version = None;
    child.doc = None;
    child.parameters.clear();
    child.default = None;
    let view = child.borrowed();
    assert!(!view.optional());
    assert_eq!(view.name(), None);
    assert_eq!(view.version(), None);
    assert_eq!(view.doc(), None);
    assert_eq!(view.default_value(), None);
    assert_eq!(view.parameters().count(), 0);
    assert!(matches!(view.kind(), ViewKind::Scalar(Scalar::I32)));
    let resolved = child.resolved();
    assert_eq!(view.optional(), resolved.optional);
    assert_eq!(view.name(), resolved.name.as_deref());
    assert_eq!(view.default_value(), resolved.default_value());
}

#[test]
fn recursive_children_borrow_graph_without_new_arc_owners_or_default_clones() {
    let mut builder = GraphBuilder::new();
    let root = builder.reserve();
    let bytes = builder.insert(Node { kind: NodeKind::Scalar(Scalar::Bytes), metadata: metadata(None, false, &[], Some(Value::Binary(vec![7; 4096]))) });
    let next = builder.insert(Node { kind: NodeKind::Alias(root), metadata: metadata(None, true, &[], None) });
    builder.define(root, Node { kind: NodeKind::Struct(vec![("payload".into(), bytes), ("next".into(), next)]), metadata: metadata(Some("Node"), false, &[], None) }).unwrap();
    let root = builder.finish(root).unwrap();
    let detached = root.field("next").unwrap().schema;
    drop(root);
    let SchemaKind::Reference(reference) = &detached.kind else { panic!("expected reference") };
    let owners = Arc::strong_count(&reference.graph);
    let view = detached.borrowed();
    assert!(view.optional());
    let payload = view.field("payload").unwrap();
    assert_eq!(payload.name, "payload");
    assert!(std::ptr::eq(payload.schema.default_value().unwrap(), reference.graph.nodes[bytes.0].metadata.default.as_ref().unwrap()));
    assert_eq!(view.field("next").unwrap().schema.identity(), view.identity());
    assert!(view.field("next").unwrap().schema.optional());
    assert_eq!(Arc::strong_count(&reference.graph), owners);
}

#[test]
fn field_iteration_preserves_schema_order_and_exact_remaining_length() {
    let schema = ConnectSchema::new(SchemaKind::Struct(vec![
        ConnectField { name: "z".into(), schema: Arc::new(ConnectSchema::new(SchemaKind::String)) },
        ConnectField { name: "a".into(), schema: Arc::new(ConnectSchema::new(SchemaKind::I64)) },
    ]));
    let ViewKind::Struct(mut fields) = schema.borrowed().kind() else { panic!("expected Struct") };
    assert_eq!(fields.size_hint(), (2, Some(2)));
    let first = fields.next().unwrap();
    assert_eq!(first.name, "z");
    assert!(matches!(first.schema.kind(), ViewKind::Scalar(Scalar::String)));
    assert_eq!(fields.len(), 1);
    assert_eq!(fields.next().unwrap().name, "a");
    assert_eq!(fields.len(), 0);
    assert!(fields.next().is_none());
    assert!(schema.borrowed().field("absent").is_none());
}

#[test]
fn graph_array_map_and_alias_children_remain_borrowed_concrete_kinds() {
    let graph = SchemaGraph {
        nodes: vec![
            Node { kind: NodeKind::Scalar(Scalar::String), metadata: Metadata::default() },
            Node { kind: NodeKind::Scalar(Scalar::I64), metadata: Metadata::default() },
            Node { kind: NodeKind::Map(NodeId(0), NodeId(1)), metadata: Metadata::default() },
            Node { kind: NodeKind::Alias(NodeId(2)), metadata: Metadata::default() },
            Node { kind: NodeKind::Array(NodeId(3)), metadata: Metadata::default() },
        ], retained_bytes: 0, view_bytes: 0,
    };
    let ViewKind::Array(element) = SchemaView::node(&graph, NodeId(4)).kind() else { panic!("expected array") };
    let ViewKind::Map(key, value) = element.kind() else { panic!("expected map") };
    assert!(matches!(key.kind(), ViewKind::Scalar(Scalar::String)));
    assert!(matches!(value.kind(), ViewKind::Scalar(Scalar::I64)));
}

#[test]
fn concrete_scalar_and_null_collection_kinds_do_not_require_graph_materialization() {
    for (kind, expected) in [
        (SchemaKind::Bool, Scalar::Bool), (SchemaKind::I8, Scalar::I8), (SchemaKind::I16, Scalar::I16),
        (SchemaKind::I32, Scalar::I32), (SchemaKind::I64, Scalar::I64), (SchemaKind::F32, Scalar::F32),
        (SchemaKind::F64, Scalar::F64), (SchemaKind::String, Scalar::String), (SchemaKind::Bytes, Scalar::Bytes),
    ] {
        let schema = ConnectSchema::new(kind);
        let ViewKind::Scalar(actual) = schema.borrowed().kind() else { panic!("expected scalar") };
        assert_eq!(actual, expected);
        assert_eq!(schema.borrowed().identity(), None);
    }
    let array = ConnectSchema::new(SchemaKind::Array(Arc::new(ConnectSchema::new(SchemaKind::I16))));
    let ViewKind::Array(element) = array.borrowed().kind() else { panic!("expected concrete array") };
    assert!(matches!(element.kind(), ViewKind::Scalar(Scalar::I16)));
    let map = ConnectSchema::new(SchemaKind::Map(Arc::new(ConnectSchema::new(SchemaKind::String)), Arc::new(array)));
    let ViewKind::Map(key, value) = map.borrowed().kind() else { panic!("expected concrete map") };
    assert!(matches!(key.kind(), ViewKind::Scalar(Scalar::String)));
    assert!(matches!(value.kind(), ViewKind::Array(_)));
    assert!(matches!(ConnectSchema::new(SchemaKind::ArrayOfNull).borrowed().kind(), ViewKind::ArrayOfNull));
    assert!(matches!(ConnectSchema::new(SchemaKind::MapOfNull).borrowed().kind(), ViewKind::MapOfNull));
    let schema = ConnectSchema::new(SchemaKind::MapWithNullValues(Arc::new(ConnectSchema::new(SchemaKind::String))));
    let ViewKind::MapWithNullValues(key) = schema.borrowed().kind() else { panic!("expected null-valued map") };
    assert!(matches!(key.kind(), ViewKind::Scalar(Scalar::String)));
}

#[test]
fn null_default_is_absent_and_empty_metadata_values_still_override() {
    let mut schema = ConnectSchema::new(SchemaKind::Bytes);
    schema.default = Some(Value::Null);
    schema.name = Some(String::new());
    schema.doc = Some(String::new());
    schema.version = Some(0);
    schema.parameters.insert(String::new(), String::new());
    let view = schema.borrowed();
    assert_eq!(view.default_value(), None);
    assert_eq!(view.name(), Some(""));
    assert_eq!(view.doc(), Some(""));
    assert_eq!(view.version(), Some(0));
    assert_eq!(view.parameter(""), Some(""));
    assert_eq!(view.parameters().collect::<Vec<_>>(), [("", "")]);
}
