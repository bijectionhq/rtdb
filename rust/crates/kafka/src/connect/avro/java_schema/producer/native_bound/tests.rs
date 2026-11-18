use super::*;
use super::super::super::test_support as support;

fn policy() -> SourcePolicy { SourcePolicy { validate_defaults: false, fixed_bytes: FixedBytesLimit::DEFAULT } }

#[test]
fn expanded_reference_names_can_increase_the_bound_while_original_input_bytes_decrease() {
    let mut context = support::context();
    let namespace = "N".repeat(256);
    let references = vec![format!(r#"{{"type":"fixed","name":"{namespace}.F","size":1}}"#)];
    let fields = |kind| (0..12).map(|index| format!(r#"{{"name":"f{index}","type":"{kind}"}}"#)).collect::<Vec<_>>().join(",");
    let root = |kind| format!(r#"{{"type":"record","name":"R","namespace":"{namespace}","fields":[{}]}}"#, fields(kind));
    let primitive = super::super::parse(&root("int"), &references, policy(), &mut context).unwrap().native;
    let references = super::super::parse(&root("F"), &references, policy(), &mut context).unwrap().native;
    assert!(references.document_bytes < primitive.document_bytes);
    assert!(references.full_name_units > primitive.full_name_units + namespace.len() * 12);
    assert!(references.bound().unwrap() > primitive.bound().unwrap());
}

#[test]
fn nested_named_ancestry_increases_live_copy_allowance_without_following_recursive_refs() {
    let mut context = support::context();
    let flat = super::super::parse(r#"{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}"#, &[], policy(), &mut context).unwrap().native;
    let nested = super::super::parse(r#"{"type":"record","name":"R","fields":[{"name":"n","type":{"type":"record","name":"C","fields":[{"name":"next","type":["null","R"]}]}}]}"#, &[], policy(), &mut context).unwrap().native;
    assert!(nested.depth > flat.depth);
    assert!(nested.bound().unwrap() > flat.bound().unwrap());
    assert!(nested.depth < 8);
}

#[test]
fn native_bound_overflow_is_reported_before_admission_or_native_construction() {
    let footprint = NativeFootprint { document_bytes: usize::MAX, ..NativeFootprint::new() };
    assert!(footprint.bound().unwrap_err().code.ends_with("allocation-overflow"));
    let footprint = NativeFootprint { full_name_units: usize::MAX, ..NativeFootprint::new() };
    assert!(footprint.bound().is_err());
    let footprint = NativeFootprint { depth: usize::MAX, ..NativeFootprint::new() };
    assert!(footprint.bound().is_err());
}

#[test]
fn repeated_raw_named_aliases_are_charged_before_canonical_dedup_and_retained_natively() {
    let mut context = support::context();
    let plain = r#"{"type":"record","name":"scope.R","aliases":["Old","Else"],"fields":[]}"#;
    let repeated = r#"{"type":"record","name":"scope.R","aliases":["Old","Old","scope.Old","Else","Else"],"fields":[]}"#;
    let plain = super::super::parse(plain, &[], policy(), &mut context).unwrap();
    let repeated_source = super::super::parse(repeated, &[], policy(), &mut context).unwrap();
    assert_eq!(plain.native.full_name_units, 26);
    assert_eq!(repeated_source.native.full_name_units, 54);
    assert_eq!(repeated_source.root.view().node().aliases.len(), 2);
    assert!(repeated_source.root.view().node().aliases[0].full().unwrap().view().units().eq("scope.Old".encode_utf16()));
    assert!(repeated_source.root.view().node().aliases[1].full().unwrap().view().units().eq("scope.Else".encode_utf16()));
    assert!(repeated_source.native.bound().unwrap() > plain.native.bound().unwrap());
    let pair = super::super::ParsedPair::parse(repeated, &[], &BTreeMap::new(), policy(), &mut context).unwrap();
    let apache_avro::Schema::Record(native) = pair.root().native() else { panic!("expected record") };
    let aliases = native.aliases.as_ref().unwrap();
    assert_eq!(aliases.len(), 5);
    assert_eq!(aliases.iter().map(|alias| alias.fullname(None)).collect::<Vec<_>>(), ["scope.Old", "scope.Old", "scope.Old", "scope.Else", "scope.Else"]);
    assert_eq!(pair.root().canonical().unwrap().view().node().aliases.len(), 2);
    let name = support::name("scope.R", &mut context);
    assert!(NativeFootprint::new().name_occurrences(&name, usize::MAX).is_err());
}
