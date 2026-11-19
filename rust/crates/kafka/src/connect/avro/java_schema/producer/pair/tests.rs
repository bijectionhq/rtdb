use rtdb_types::StructFields;
use super::*;
use super::super::super::test_support as support;

fn policy() -> SourcePolicy { SourcePolicy { validate_defaults: false, fixed_bytes: FixedBytesLimit::DEFAULT } }
fn parse(text: &str, references: &[String], context: &mut ConversionContext) -> ParsedPair {
    ParsedPair::parse(text, references, &BTreeMap::new(), policy(), context).unwrap()
}

#[test]
fn same_owner_root_native_ref_resolved_fields_and_definition_share_one_canonical_object() {
    let mut context = support::context();
    let pair = parse(r#"{"type":"record","name":"ns.R","fields":[{"name":"next","type":["null","R"]},{"name":"value","type":"int"}]}"#, &[], &mut context);
    let root = pair.root().canonical().unwrap();
    let recursive = pair.root().field(0).unwrap().variant(1).unwrap();
    assert!(matches!(recursive.native(), Schema::Ref { .. }));
    assert!(recursive.canonical().unwrap().same_object(&root));
    assert!(pair.definition("ns.R").unwrap().canonical().unwrap().same_object(&root));
    assert!(recursive.field(1).unwrap().canonical().unwrap().same_object(&pair.root().field(1).unwrap().canonical().unwrap()));
    let resolved = pair.resolved(recursive.native()).unwrap();
    assert!(!std::ptr::eq(pair.root().native(), resolved));
}

#[test]
fn independent_same_name_input_groups_never_share_canonical_source_identity() {
    let mut context = support::context();
    let text = r#"{"type":"record","name":"R","fields":[]}"#;
    let first = parse(text, &[], &mut context);
    let second = parse(text, &[], &mut context);
    assert!(!first.root().canonical().unwrap().same_object(&second.root().canonical().unwrap()));
    assert!(first.definition("R").unwrap().canonical().unwrap().same_object(&first.root().canonical().unwrap()));
    assert!(second.definition("R").unwrap().canonical().unwrap().same_object(&second.root().canonical().unwrap()));
}

#[test]
fn a_named_root_reference_and_its_resolved_recursive_definition_share_the_original_owner() {
    let mut context = support::context();
    let references = vec![r#"{"type":"record","name":"ns.R","fields":[{"name":"next","type":["null","R"]}]}"#.into()];
    let pair = parse("\"ns.R\"", &references, &mut context);
    assert!(matches!(pair.root().native(), Schema::Ref { .. }));
    assert!(matches!(pair.definition("ns.R").unwrap().native(), Schema::Record(_)));
    let root = pair.root().canonical().unwrap();
    assert!(pair.definition("ns.R").unwrap().canonical().unwrap().same_object(&root));
    assert!(pair.root().field(0).unwrap().variant(1).unwrap().canonical().unwrap().same_object(&root));
}

#[test]
fn declared_references_follow_namespaces_without_alias_index_or_field_alias_overwrite() {
    let mut context = support::context();
    let references = vec![r#"{"type":"fixed","name":"Global","size":1}"#.into(), r#"{"type":"fixed","name":"ns.Global","size":2}"#.into()];
    let pair = parse(r#"{"type":"record","name":"ns.R","aliases":["OldR"],"fields":[{"name":"a","aliases":["b"],"type":"Global"},{"name":"b","type":"int"}]}"#, &references, &mut context);
    assert!(pair.root().field(0).unwrap().canonical().unwrap().same_object(&pair.definition("ns.Global").unwrap().canonical().unwrap()));
    let Schema::Record(record) = pair.root().native() else { panic!("expected record") };
    assert_eq!(record.lookup.get("a"), Some(&0));
    assert_eq!(record.lookup.get("b"), Some(&1));
    assert!(pair.definition("ns.OldR").is_err());
}

#[test]
fn logical_fixed_references_select_the_original_named_logical_owner() {
    let mut context = support::context();
    let references = vec![r#"{"type":"fixed","name":"Money","size":2,"logicalType":"decimal","precision":4,"scale":2}"#.into()];
    let pair = parse(r#"{"type":"record","name":"R","fields":[{"name":"value","type":"Money"}]}"#, &references, &mut context);
    let selected = pair.root().field(0).unwrap();
    assert!(matches!(selected.native(), Schema::Ref { .. }));
    assert!(matches!(pair.definition("Money").unwrap().native(), Schema::Decimal(_)));
    assert!(selected.canonical().unwrap().same_object(&pair.definition("Money").unwrap().canonical().unwrap()));
}

#[test]
fn array_and_map_children_are_selected_from_the_actual_pair_owner() {
    let mut context = support::context();
    let pair = parse(r#"{"type":"record","name":"R","fields":[{"name":"items","type":{"type":"array","items":"R"}},{"name":"lookup","type":{"type":"map","values":"R"}}]}"#, &[], &mut context);
    let root = pair.root().canonical().unwrap();
    assert!(pair.root().field(0).unwrap().element().unwrap().canonical().unwrap().same_object(&root));
    assert!(pair.root().field(1).unwrap().map_value().unwrap().canonical().unwrap().same_object(&root));
    assert!(pair.root().variant(0).is_err());
    assert!(pair.root().field(4).is_err());
}

#[test]
fn cached_pair_retains_native_and_source_leases_and_canonical_handles_own_their_payloads() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let pair = parse(r#"{"type":"record","name":"R","fields":[{"name":"x","type":"string","default":"kept"}]}"#, &[], &mut context);
    let native_charge = Arc::downgrade(&pair.charge);
    let native_bytes = pair.charge.bytes();
    drop(context);
    assert!(native_charge.upgrade().is_some());
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), native_bytes - 1);
    assert!(pair.retain(&mut denied).is_err());
    let canonical = pair.root().canonical().unwrap();
    drop(pair);
    assert!(native_charge.upgrade().is_none());
    assert!(budget.used_bytes() > 0);
    drop(canonical);
    drop(denied);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn native_admission_occurs_before_the_native_parse_closure() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let source = super::super::parse(r#"{"type":"record","name":"R","fields":[]}"#, &[], policy(), &mut context).unwrap();
    let bytes = source.native.bound().unwrap();
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), bytes - 1);
    let mut entered = false;
    assert!(admit_native(bytes, &mut denied, || { entered = true; Ok(()) }).is_err());
    assert!(!entered);
}

#[test]
fn a_fresh_scope_recounts_the_native_lease_without_reserving_another_native_owner() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let pair = parse(r#"{"type":"record","name":"R","fields":[]}"#, &[], &mut context);
    let weak = Arc::downgrade(&pair.charge);
    drop(context);
    assert_eq!(Arc::strong_count(&pair.charge), 1);
    let mut next = ConversionContext::new(Arc::clone(&budget));
    pair.retain(&mut next).unwrap();
    assert_eq!(Arc::strong_count(&pair.charge), 2);
    pair.retain(&mut next).unwrap();
    assert_eq!(Arc::strong_count(&pair.charge), 2);
    assert!(matches!(pair.root().native(), Schema::Record(_)));
    drop(pair);
    assert!(weak.upgrade().is_some());
    drop(next);
    assert!(weak.upgrade().is_none());
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn paired_field_types_use_nominal_keyword_names_and_canonical_field_properties() {
    let mut context = support::context();
    let references = vec![
        r#"{"type":"enum","name":"enum","symbols":["A"]}"#.into(),
        r#"{"type":"fixed","name":"fixed","size":1}"#.into(),
        r#"{"type":"record","name":"array","fields":[]}"#.into(),
        r#"{"type":"fixed","name":"map","size":2}"#.into(),
    ];
    let pair = parse(r#"{"type":"record","name":"R","fields":[{"name":"e","type":"enum"},{"name":"f","type":"fixed"},{"name":"a","type":"array"},{"name":"m","type":"map"},{"name":"flat","type":"int","logicalType":"date"}]}"#, &references, &mut context);
    for (index, name) in ["enum", "fixed", "array", "map"].into_iter().enumerate() {
        let selected = pair.root().field(index).unwrap();
        assert!(matches!(selected.native(), Schema::Ref { .. }));
        assert!(selected.canonical().unwrap().same_object(&pair.definition(name).unwrap().canonical().unwrap()));
    }
    assert!(matches!(pair.root().field(4).unwrap().native(), Schema::Int));
    let root = pair.root().canonical().unwrap();
    let Kind::Record { fields: Some(fields), .. } = &root.view().node().kind else { panic!("expected record") };
    let text = support::text("date", &mut context);
    let node = support::node(JsonValue::Text(text), &mut context);
    let expected = support::properties(vec![("logicalType", node)], &mut context);
    assert!(fields[4].properties.equal(&expected, &mut context, 0).unwrap());
}

#[test]
fn declared_document_dependencies_share_nominal_pair_associations_in_encounter_order() {
    let mut context = support::context();
    let references = vec![
        r#"{"type":"fixed","name":"ns.Base","size":1}"#.into(),
        r#"{"type":"record","name":"Global","fields":[{"name":"base","type":"ns.Base"}]}"#.into(),
        r#"{"type":"record","name":"ns.Last","fields":[{"name":"prior","type":"Global"},{"name":"next","type":["null","Last"]}]}"#.into(),
    ];
    let pair = parse("\"ns.Last\"", &references, &mut context);
    let prior = pair.root().field(0).unwrap();
    assert!(matches!(prior.native(), Schema::Ref { .. }));
    assert!(prior.canonical().unwrap().same_object(&pair.definition("Global").unwrap().canonical().unwrap()));
    let base = prior.field(0).unwrap();
    assert!(matches!(base.native(), Schema::Ref { .. }));
    assert!(base.canonical().unwrap().same_object(&pair.definition("ns.Base").unwrap().canonical().unwrap()));
    assert!(pair.root().field(1).unwrap().variant(1).unwrap().canonical().unwrap().same_object(&pair.root().canonical().unwrap()));
    let reversed = references.into_iter().rev().collect::<Vec<_>>();
    let error = ParsedPair::parse("\"ns.Last\"", &reversed, &BTreeMap::new(), policy(), &mut context).err().unwrap();
    assert!(error.code.ends_with("avro-schema"));
    assert!(error.message.unwrap().contains("Undefined"));
}

#[test]
fn consuming_the_pair_moves_native_storage_and_preserves_source_identity_in_the_datum_owner() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let pair = parse(r#"{"type":"record","name":"R","fields":[{"name":"next","type":["null","R"]}]}"#, &[], &mut context);
    let source = pair.root().canonical().unwrap();
    let native_fields = match pair.root().native() { Schema::Record(record) => record.fields.as_ptr(), _ => panic!("expected record") };
    let charge = Arc::downgrade(&pair.charge);
    let datum = Arc::new(pair.into_datum());
    drop(context);
    let selected = datum.paired_root().unwrap();
    assert!(selected.canonical().unwrap().same_object(&source));
    assert!(selected.field(0).unwrap().variant(1).unwrap().canonical().unwrap().same_object(&source));
    assert!(datum.paired_definition("R").unwrap().unwrap().canonical().unwrap().same_object(&source));
    assert!(matches!(selected.native(), Schema::Record(record) if record.fields.as_ptr() == native_fields));
    assert!(charge.upgrade().is_some());
    drop(datum);
    assert!(charge.upgrade().is_none());
    assert!(budget.used_bytes() > 0);
    drop(source);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn admitted_datum_assembly_runs_through_the_real_connect_plan_and_decode_owner() {
    use crate::connect::avro::Avro;
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let datum = Arc::new(AvroDatumPlan::parse_admitted_source(r#"{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}"#, &[], &BTreeMap::new(), policy(), &mut context).unwrap());
    let source = Arc::downgrade(&datum.source.as_ref().unwrap().completion);
    let connect = datum.connect_plan(None, Some(3), &mut 0, &mut context).unwrap();
    drop(context);
    let mut next = ConversionContext::new(Arc::clone(&budget));
    let physical = datum.decode_datum(&[14], &mut next).unwrap();
    assert_eq!(physical.value, Avro::Record(vec![("x".into(), Avro::Int(7))]));
    let converted = connect.convert(physical, &mut 0, &mut next).unwrap();
    assert_eq!(converted.value, rtdb_types::Value::Struct(StructFields::from([("x".into(), rtdb_types::Value::I32(7))])));
    assert_eq!(converted.schema.as_ref().unwrap().version, Some(3));
    assert!(next.retains(&datum.source.as_ref().unwrap().completion));
    drop(datum);
    drop(connect);
    assert!(source.upgrade().is_some());
    drop(converted);
    drop(next);
    assert!(source.upgrade().is_none());
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn repeated_admitted_source_entries_do_not_repeat_canonical_retention_scratch() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let datum = parse(r#"{"type":"record","name":"R","fields":[]}"#, &[], &mut context).into_datum();
    drop(context);
    let mut next = ConversionContext::new(Arc::clone(&budget));
    datum.retain_source(&mut next).unwrap();
    let remaining = next.allocation_remaining();
    let used = budget.used_bytes();
    datum.retain_source(&mut next).unwrap();
    assert_eq!(next.allocation_remaining(), remaining);
    assert_eq!(budget.used_bytes(), used);
    assert_eq!(Arc::strong_count(&datum.source.as_ref().unwrap().completion), 2);
    drop(datum);
    drop(next);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn a_failed_source_retain_does_not_publish_the_completion_marker() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let datum = parse(r#"{"type":"record","name":"R","fields":[{"name":"x","type":"string","default":"kept"}]}"#, &[], &mut context).into_datum();
    drop(context);
    let completion = &datum.source.as_ref().unwrap().completion;
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), completion.bytes());
    assert!(datum.retain_source(&mut denied).is_err());
    assert!(!denied.retains(completion));
    assert!(datum.retain_source(&mut denied).is_err());
    assert!(!denied.retains(completion));
    assert_eq!(Arc::strong_count(completion), 1);
    let mut admitted = ConversionContext::new(Arc::clone(&budget));
    datum.retain_source(&mut admitted).unwrap();
    assert!(admitted.retains(completion));
    drop(datum);
    drop(denied);
    drop(admitted);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn retained_reservation_queries_use_pointer_identity_and_do_not_change_the_scope() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let retained = context.reserve(64).unwrap();
    let distinct = budget.reserve(64).unwrap();
    let remaining = context.allocation_remaining();
    assert!(context.retains(&Arc::clone(&retained)));
    assert!(!context.retains(&distinct));
    assert_eq!(context.allocation_remaining(), remaining);
    context.retain(Arc::clone(&distinct)).unwrap();
    assert!(context.retains(&distinct));
    assert_eq!(context.allocation_remaining(), remaining - 64);
    context.retain(Arc::clone(&distinct)).unwrap();
    assert_eq!(context.allocation_remaining(), remaining - 64);
}

#[test]
fn admitted_resolution_keeps_both_source_owners_and_applies_the_existing_reader_default() {
    use crate::connect::avro::{Avro, AvroResolution};
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let writer = Arc::new(AvroDatumPlan::parse_admitted_source(r#"{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}"#, &[], &BTreeMap::new(), policy(), &mut context).unwrap());
    let reader = Arc::new(AvroDatumPlan::parse_admitted_source(r#"{"type":"record","name":"R","fields":[{"name":"x","type":"long"},{"name":"y","type":"string","default":"fallback"}]}"#, &[], &BTreeMap::new(), policy(), &mut context).unwrap());
    let writer_charge = Arc::downgrade(&writer.source.as_ref().unwrap().completion);
    let reader_charge = Arc::downgrade(&reader.source.as_ref().unwrap().completion);
    let resolution = AvroResolution::compile(&writer, &reader, &mut context).unwrap();
    drop(context);
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), 0);
    assert!(resolution.decode_datum(&[], &mut denied).err().unwrap().code.ends_with("batch-too-large"));
    let mut next = ConversionContext::new(Arc::clone(&budget));
    let decoded = resolution.decode_datum(&[14], &mut next).unwrap();
    assert_eq!(decoded.value, Avro::Record(vec![("x".into(), Avro::Long(7)), ("y".into(), Avro::String("fallback".into()))]));
    assert!(decoded.plan.paired_root().unwrap().canonical().unwrap().same_object(&reader.paired_root().unwrap().canonical().unwrap()));
    assert!(next.retains(&writer.source.as_ref().unwrap().completion));
    assert!(next.retains(&reader.source.as_ref().unwrap().completion));
    drop(writer);
    drop(reader);
    drop(resolution);
    assert!(writer_charge.upgrade().is_some());
    assert!(reader_charge.upgrade().is_some());
    drop(decoded);
    drop(next);
    drop(denied);
    assert!(writer_charge.upgrade().is_none());
    assert!(reader_charge.upgrade().is_none());
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn admitted_migration_recounts_the_actual_datum_source_before_json_conversion() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let datum = Arc::new(AvroDatumPlan::parse_admitted_source(r#"{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}"#, &[], &BTreeMap::new(), policy(), &mut context).unwrap());
    drop(context);
    let mut next = ConversionContext::new(Arc::clone(&budget));
    let value = datum.from_json_datum(serde_json::json!({"x":7}), &mut next).unwrap();
    assert!(next.retains(&datum.source.as_ref().unwrap().completion));
    assert!(value.plan.paired_root().unwrap().canonical().unwrap().same_object(&datum.paired_root().unwrap().canonical().unwrap()));
    assert_eq!(datum.to_json_datum(value, &mut next).unwrap(), serde_json::json!({"x":7}));
    drop(datum);
    drop(next);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn legacy_parsing_keeps_no_admitted_source_and_no_source_retain_work() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let datum = AvroDatumPlan::parse("\"int\"", &[], &BTreeMap::new()).unwrap();
    assert!(datum.paired_root().is_none());
    assert!(datum.paired_definition("R").unwrap().is_none());
    let mut context = ConversionContext::with_limit(Arc::clone(&budget), 0);
    datum.retain_source(&mut context).unwrap();
    assert_eq!(context.allocation_remaining(), 0);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn cross_plan_output_keeps_both_admitted_source_families_in_the_conversion_scope() {
    use crate::connect::avro::AvroOutput;
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let text = r#"{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}"#;
    let actual = Arc::new(AvroDatumPlan::parse_admitted_source(text, &[], &BTreeMap::new(), policy(), &mut context).unwrap());
    let target = Arc::new(AvroDatumPlan::parse_admitted_source(text, &[], &BTreeMap::new(), policy(), &mut context).unwrap());
    let connect = target.connect_plan(None, None, &mut 0, &mut context).unwrap();
    assert!(!actual.paired_root().unwrap().canonical().unwrap().same_object(&target.paired_root().unwrap().canonical().unwrap()));
    drop(context);
    let mut next = ConversionContext::new(Arc::clone(&budget));
    let datum = actual.decode_datum(&[14], &mut next).unwrap();
    let (selected, datum) = actual.prepare_output(&target, datum, &mut next).unwrap();
    assert_eq!(selected, AvroOutput::Reader { branch: None });
    let output = connect.convert(datum, &mut 0, &mut next).unwrap();
    assert_eq!(output.value, rtdb_types::Value::Struct(StructFields::from([("x".into(), rtdb_types::Value::I32(7))])));
    assert!(next.retains(&actual.source.as_ref().unwrap().completion));
    assert!(next.retains(&target.source.as_ref().unwrap().completion));
    assert_eq!(Arc::strong_count(&actual.source.as_ref().unwrap().completion), 2);
    assert_eq!(Arc::strong_count(&target.source.as_ref().unwrap().completion), 2);
    drop(actual);
    drop(target);
    drop(connect);
    drop(output);
    drop(next);
    assert_eq!(budget.used_bytes(), 0);
}
