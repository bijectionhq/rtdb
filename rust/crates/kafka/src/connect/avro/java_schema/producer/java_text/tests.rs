use super::*;
use super::super::super::test_support as support;

fn policy() -> SourcePolicy { SourcePolicy { validate_defaults: false, fixed_bytes: FixedBytesLimit::DEFAULT } }
fn text(value: &str, context: &mut ConversionContext) -> Text {
    Text::copy_units(&value.encode_utf16().collect::<Vec<_>>(), context).unwrap()
}

#[test]
fn projection_preserves_scalar_units_across_utf8_and_utf16_backing() {
    let mut context = support::context();
    let expected = "Aé😀";
    let units = expected.encode_utf16().collect::<Vec<_>>();
    let utf8 = project(JavaText::from_utf8(expected), &mut context).unwrap();
    let utf16 = project(JavaText::from_utf16(&units), &mut context).unwrap();
    assert_eq!(utf8.text, expected);
    assert_eq!(utf16.text, expected);
    assert_eq!(length(JavaText::from_utf16(&units)).unwrap(), expected.len());
    assert_ne!(units.len(), expected.len());
}

#[test]
fn unmatched_units_are_rejected_without_a_replacement_projection() {
    let mut context = support::context();
    for units in [vec![0xd800], vec![0xdc00], vec![65, 0xd800, 66]] {
        assert!(project(JavaText::from_utf16(&units), &mut context).err().unwrap().code.ends_with("avro-java-text-reader-required"));
    }
    let pair = [0xd800, 0xdc00];
    let split = JavaText::from_utf16(&pair).slice_units(0..1).unwrap();
    assert!(project(split, &mut context).is_err());
    assert_eq!(pair, [0xd800, 0xdc00]);
}

#[test]
fn projection_admission_precedes_the_string_copy() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let units = [65, 0xd800, 0xdc00, 66];
    let bytes = length(JavaText::from_utf16(&units)).unwrap();
    let bound = bytes.checked_mul(16).unwrap().checked_add(256).unwrap();
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), bound - 1);
    assert!(project(JavaText::from_utf16(&units), &mut denied).err().unwrap().code.ends_with("batch-too-large"));
    assert_eq!(denied.allocation_remaining(), bound - 1);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn projected_input_owner_keeps_payload_and_control_charges_after_the_context() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let root = text(r#"{"type":"record","name":"R","fields":[],"doc":"A😀"}"#, &mut context);
    let reference = text(r#"{"type":"fixed","name":"F","size":1}"#, &mut context);
    let parsed = parse(root.view(), &[reference.view()], policy(), &mut logical::Unregistered, &mut context).unwrap();
    let control = Arc::downgrade(&parsed.inputs._control);
    let payload = Arc::downgrade(&parsed.inputs.payloads[0]);
    drop(root);
    drop(reference);
    drop(context);
    assert!(control.upgrade().is_some());
    assert!(payload.upgrade().is_some());
    assert_eq!(parsed.inputs.references.len(), 1);
    assert!(parsed.inputs.root.contains("A😀"));
    let ParsedInput { canonical, inputs } = parsed;
    drop(inputs);
    assert!(control.upgrade().is_none());
    assert!(payload.upgrade().is_none());
    assert!(budget.used_bytes() > 0);
    assert!(canonical.root.view().node().doc.as_ref().unwrap().view().units().eq("A😀".encode_utf16()));
    drop(canonical);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn an_earlier_reference_semantic_failure_precedes_a_later_unsupported_projection() {
    let mut context = support::context();
    let later = [0xd800];
    let references = [JavaText::from_utf8("\"Missing\""), JavaText::from_utf16(&later)];
    let failure = parse(JavaText::from_utf8("\"int\""), &references, policy(), &mut logical::Unregistered, &mut context).err().unwrap();
    assert_eq!(failure.message.as_deref(), Some("Undefined canonical schema reference"));
}

#[test]
fn named_reference_dependency_failure_precedes_projection_of_an_unsupported_root() {
    let mut context = support::context();
    let root = [0xdc00];
    let references = [JavaText::from_utf8(r#"{"type":"record","name":"R","fields":[{"name":"x","type":"Later"}]}"#)];
    let failure = parse(JavaText::from_utf16(&root), &references, policy(), &mut logical::Unregistered, &mut context).err().unwrap();
    assert_eq!(failure.message.as_deref(), Some("Undefined canonical schema reference"));
}

#[test]
fn borrowed_utf16_pair_reuses_actual_root_definition_and_recursive_reference_owners() {
    let mut context = support::context();
    let reference = text(r#"{"type":"record","name":"ns.R","fields":[{"name":"next","type":["null","R"]}]}"#, &mut context);
    let root = text("\"ns.R\"", &mut context);
    let pair = ParsedPair::parse_java_text(root.view(), &[reference.view()], &BTreeMap::new(), policy(), &mut context).unwrap();
    let canonical = pair.root().canonical().unwrap();
    assert!(pair.definition("ns.R").unwrap().canonical().unwrap().same_object(&canonical));
    assert!(pair.root().field(0).unwrap().variant(1).unwrap().canonical().unwrap().same_object(&canonical));
    drop(reference);
    drop(root);
    assert!(pair.root().canonical().unwrap().same_object(&canonical));
}

#[test]
fn utf16_projection_counts_native_bytes_and_matches_existing_source_node_relations() {
    let mut context = support::context();
    let source = r#"{"type":"record","name":"R","doc":"é😀","fields":[{"name":"x","type":"double","default":"0x1.8p1"}]}"#;
    let input = text(source, &mut context);
    let projected = parse(input.view(), &[], policy(), &mut logical::Unregistered, &mut context).unwrap();
    let existing = super::super::parse(source, &[], policy(), &mut context).unwrap();
    assert_eq!(projected.inputs.root, source);
    assert_eq!(projected.canonical.native.bound().unwrap(), existing.native.bound().unwrap());
    let mut seen = RecursionState::new();
    let mut witness = support::Observed::default();
    let mut evaluation = Evaluation::new(&mut seen, &mut context, &mut witness).unwrap();
    assert!(evaluation.equal(&projected.canonical.root, &existing.root).unwrap());
    assert_ne!(projected.canonical.root.id(), existing.root.id());
}

#[test]
fn borrowed_java_text_datum_entry_retains_source_after_input_and_creation_context_drop() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let input = text(r#"{"type":"record","name":"R","doc":"A😀","fields":[{"name":"x","type":"int"}]}"#, &mut context);
    let datum = Arc::new(crate::connect::avro::AvroDatumPlan::parse_admitted_java_text(input.view(), &[], &BTreeMap::new(), policy(), &mut context).unwrap());
    let connect = datum.connect_plan(None, Some(3), &mut 0, &mut context).unwrap();
    drop(input);
    drop(context);
    assert!(budget.used_bytes() > 0);
    let canonical = datum.paired_root().unwrap().canonical().unwrap();
    assert!(canonical.view().node().doc.as_ref().unwrap().view().units().eq("A😀".encode_utf16()));
    let mut next = ConversionContext::new(Arc::clone(&budget));
    let converted = connect.decode(&[14], &mut next).unwrap();
    assert_eq!(converted.value, rtdb_types::Value::Struct(BTreeMap::from([("x".into(), rtdb_types::Value::I32(7))])));
    assert_eq!(converted.schema.as_ref().unwrap().version, Some(3));
    drop(canonical);
    drop(datum);
    drop(connect);
    drop(converted);
    drop(next);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn empty_and_escaped_unmatched_documents_keep_the_declared_decoder_boundary() {
    let mut context = support::context();
    assert!(ParsedPair::parse_java_text(JavaText::from_utf16(&[]), &[], &BTreeMap::new(), policy(), &mut context).is_err());
    assert!(ParsedPair::parse_java_text(JavaText::from_utf8(r#"{"type":"int","note":"\ud800"}"#), &[], &BTreeMap::new(), policy(), &mut context).is_err());
}

#[test]
fn borrowed_java_text_entry_uses_the_supplied_source_logical_environment() {
    struct Factory { calls: usize }
    impl LogicalEnvironment for Factory {
        fn factory(&mut self, name: JavaText<'_>, _: SchemaView<'_>, _: &mut ConversionContext) -> Result<FactoryOutcome> {
            assert!(name.units().eq("custom".encode_utf16()));
            self.calls += 1;
            Ok(FactoryOutcome::RuntimeException)
        }
        fn fixed_decimal_precision(&mut self, _: i32, _: &mut ConversionContext) -> Result<i64> { unreachable!() }
    }
    let mut context = support::context();
    let input = text(r#"{"type":"int","logicalType":"custom"}"#, &mut context);
    let mut factory = Factory { calls: 0 };
    let pair = ParsedPair::parse_java_text_with_logical(input.view(), &[], &BTreeMap::new(), policy(), &mut factory, &mut context).unwrap();
    let canonical = pair.root().canonical().unwrap();
    assert_eq!(factory.calls, 1);
    assert_eq!(canonical.view().logical_type(), None);
    assert!(canonical.view().raw_logical_name().unwrap().units().eq("custom".encode_utf16()));
}
