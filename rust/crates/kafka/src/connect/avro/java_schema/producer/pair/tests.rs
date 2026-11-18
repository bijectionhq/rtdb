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
