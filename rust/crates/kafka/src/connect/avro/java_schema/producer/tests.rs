use super::*;
use super::super::test_support as support;

fn policy(validate_defaults: bool) -> SourcePolicy { SourcePolicy { validate_defaults, fixed_bytes: FixedBytesLimit::DEFAULT } }
fn source(text: &str, context: &mut ConversionContext) -> SchemaHandle { parse(text, &[], policy(false), context).unwrap().root }
fn field(source: &SchemaHandle, index: usize) -> &Field {
    let Kind::Record { fields: Some(fields), .. } = &source.view().node().kind else { panic!("expected record") };
    &fields[index]
}

#[test]
fn original_documents_create_one_recursive_object_and_preserve_fresh_primitive_uses() {
    let mut context = support::context();
    let root = source(r#"{"type":"record","name":"ns.R","fields":[{"name":"next","type":["null","R"]},{"name":"a","type":"int"},{"name":"b","type":"int"}]}"#, &mut context);
    let Kind::Union(variants) = &root.owner.nodes[field(&root, 0).schema.index].kind else { panic!("expected union") };
    assert_eq!(variants[1], root.id());
    assert!(root.at(variants[1]).unwrap().same_object(&root));
    assert_ne!(field(&root, 1).schema, field(&root, 2).schema);
}

#[test]
fn reference_documents_use_source_encounter_order_and_restore_document_namespace() {
    let mut context = support::context();
    let references = vec![r#"{"type":"record","name":"ns.Local","fields":[]}"#.into(), r#"{"type":"fixed","name":"Global","size":1}"#.into()];
    let root = parse(r#"{"type":"record","name":"ns.R","fields":[{"name":"local","type":"Local"},{"name":"global","type":"Global"}]}"#, &references, policy(false), &mut context).unwrap().root;
    let global = root.at(field(&root, 1).schema).unwrap();
    assert!(named(&global.view().node().kind).unwrap().full().unwrap().view().units().eq("Global".encode_utf16()));
    let forward = vec![r#"{"type":"record","name":"First","fields":[{"name":"later","type":"Later"}]}"#.into(), r#"{"type":"fixed","name":"Later","size":1}"#.into()];
    assert!(parse("\"First\"", &forward, policy(false), &mut context).err().unwrap().message.unwrap().contains("Undefined"));
}

#[test]
fn textual_reference_fallback_is_not_applied_to_object_type_references_or_aliases() {
    let mut context = support::context();
    let references = vec![r#"{"type":"fixed","name":"Global","aliases":["OldGlobal"],"size":1}"#.into()];
    assert!(parse(r#"{"type":"record","name":"ns.R","fields":[{"name":"x","type":"Global"}]}"#, &references, policy(false), &mut context).is_ok());
    assert!(parse(r#"{"type":"record","name":"ns.R","fields":[{"name":"x","type":{"type":"Global"}}]}"#, &references, policy(false), &mut context).is_err());
    assert!(parse("\"OldGlobal\"", &references, policy(false), &mut context).is_err());
}

#[test]
fn child_parse_and_double_text_conversion_precede_field_name_validation() {
    let mut context = support::context();
    let missing = parse(r#"{"type":"record","name":"R","fields":[{"name":"1bad","type":"Missing"}]}"#, &[], policy(false), &mut context).err().unwrap();
    assert!(missing.message.unwrap().contains("Undefined"));
    let number = parse(r#"{"type":"record","name":"R","fields":[{"name":"1bad","type":"double","default":"bad-double"}]}"#, &[], policy(false), &mut context).err().unwrap();
    assert!(number.code.ends_with("avro-schema-unchecked"));
    let duplicated = parse(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"a","type":"Missing"}]}"#, &[], policy(false), &mut context).err().unwrap();
    assert!(duplicated.message.unwrap().contains("Undefined"));
    assert!(parse(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"a","type":"long"}]}"#, &[], policy(false), &mut context).err().unwrap().message.unwrap().contains("Duplicate"));
}

#[test]
fn textual_float_defaults_become_double_nodes_only_for_direct_float_or_double_schema() {
    let mut context = support::context();
    let root = source(r#"{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":" 0x1.000002p0f "},{"name":"d","type":"double","default":"-NaN"},{"name":"u","type":["null","double"],"default":"NaN"}]}"#, &mut context);
    assert!(matches!(field(&root, 0).default.as_ref().unwrap().value(), JsonValue::F64(value) if value.to_bits() == 1.0000001192092896f64.to_bits()));
    assert!(matches!(field(&root, 1).default.as_ref().unwrap().value(), JsonValue::F64(value) if value.is_nan()));
    assert!(matches!(field(&root, 2).default.as_ref().unwrap().value(), JsonValue::Text(_)));
}

#[test]
fn source_default_policy_preserves_invalid_nodes_or_validates_actual_node_classes() {
    let mut context = support::context();
    let text = r#"{"type":"record","name":"R","fields":[{"name":"x","type":"int","default":2147483648}]}"#;
    let retained = parse(text, &[], policy(false), &mut context).unwrap().root;
    assert!(matches!(field(&retained, 0).default.as_ref().unwrap().value(), JsonValue::I64(2147483648)));
    assert!(parse(text, &[], policy(true), &mut context).is_err());
    let text = r#"{"type":"record","name":"R","fields":[{"name":"x","type":{"type":"record","name":"C","fields":[{"name":"n","type":"int"}]},"default":{"n":1,"extra":2}}]}"#;
    let retained = parse(text, &[], policy(true), &mut context).unwrap().root;
    assert!(field(&retained, 0).default.as_ref().unwrap().get("extra").is_some());
}

#[test]
fn source_order_uses_english_uppercase_and_enum_default_is_reserved_for_all_node_classes() {
    let mut context = support::context();
    let root = source(r#"{"type":"record","name":"R","fields":[{"name":"x","type":"int","order":"deſcendıng"},{"name":"y","type":"int","order":"IgNoRe"}]}"#, &mut context);
    assert_eq!(field(&root, 0).order, FieldOrder::Descending);
    assert_eq!(field(&root, 1).order, FieldOrder::Ignore);
    let enumeration = source(r#"{"type":"enum","name":"E","symbols":["OK"],"default":42,"x":1}"#, &mut context);
    let expected = support::properties(vec![("x", support::node(JsonValue::I32(1), &mut context))], &mut context);
    assert!(enumeration.view().node().properties.equal(&expected, &mut context, 0).unwrap());
    assert!(matches!(&enumeration.view().node().kind, Kind::Enum { default: None, .. }));
}

#[test]
fn schema_document_numeric_classes_and_duplicate_member_positions_remain_source_values() {
    let mut context = support::context();
    let root = source(r#"{"type":"int","x":1,"y":1.0,"x":2,"z":9223372036854775808}"#, &mut context);
    let expected = support::properties(vec![("x", support::node(JsonValue::I32(2), &mut context)), ("y", support::node(JsonValue::F64(1.0), &mut context)), ("z", JsonNode::integer_decimal(JavaText::from_utf8("9223372036854775808"), &mut context).unwrap())], &mut context);
    assert!(root.view().node().properties.equal(&expected, &mut context, 0).unwrap());
    let input = input::parse(r#"{"b":0,"a":1,"b":2}"#, &mut context).unwrap();
    let JsonValue::Object(members) = input.value() else { panic!("expected object") };
    assert!(members.entry(0).0.view().units().eq("b".encode_utf16()));
    assert!(matches!(members.entry(0).1.value(), JsonValue::I32(2)));
}

#[test]
fn original_utf8_documents_retain_unicode_names_and_explicitly_reject_unpaired_escape_profile() {
    let mut context = support::context();
    let root = source(r#"{"type":"record","name":"Événement","namespace":"𐐀","fields":[{"name":"donnée","type":"int"}]}"#, &mut context);
    assert!(named(&root.view().node().kind).unwrap().full().unwrap().view().units().eq("𐐀.Événement".encode_utf16()));
    assert!(field(&root, 0).name.view().units().eq("donnée".encode_utf16()));
    assert!(parse(r#"{"type":"record","name":"R","namespace":"\uD800","fields":[]}"#, &[], policy(false), &mut context).err().unwrap().code.ends_with("avro-native-json-input"));
}

#[test]
fn retained_original_defaults_and_properties_survive_the_input_context() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let root = source(r#"{"type":"record","name":"R","x":{"raw":"property"},"fields":[{"name":"x","type":"string","default":"kept"}]}"#, &mut context);
    drop(context);
    assert!(budget.used_bytes() > 0);
    assert!(field(&root, 0).default.as_ref().unwrap().as_text().unwrap().view().units().eq("kept".encode_utf16()));
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), 0);
    assert!(root.retain(&mut denied).is_err());
    drop(root);
    drop(denied);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn connect_default_explicit_null_stays_a_source_node_without_rewriting_the_a09_observation() {
    let mut context = support::context();
    let root = source(r#"{"type":"array","items":"string","connect.default":[null]}"#, &mut context);
    let null = support::node(JsonValue::Null, &mut context);
    let array = JsonNode::with(4096, &mut context, |_| Ok(JsonValue::Array(vec![null]))).unwrap();
    let expected = support::properties(vec![("connect.default", array)], &mut context);
    assert!(root.view().node().properties.equal(&expected, &mut context, 0).unwrap());
}
