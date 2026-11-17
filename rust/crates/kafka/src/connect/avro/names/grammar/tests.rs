use super::*;
use serde_json::json;

fn validate(documents: &[Json]) -> Result<()> { validate_ordered(documents, |_| Ok(())) }
fn record(name: &str) -> Json { json!({"type":"record", "name":name, "fields":[]}) }

#[test]
fn ordered_documents_accept_prior_definitions_and_reject_forward_definitions() {
    let dependency = record("dep.Child");
    let parent = json!({"type":"record", "name":"Parent", "fields":[{"name":"child", "type":"dep.Child"}]});
    assert!(validate(&[dependency.clone(), parent.clone()]).is_ok());
    assert!(validate(&[parent, dependency]).is_err());
}
#[test]
fn records_register_before_fields_and_nested_definitions_follow_field_order() {
    assert!(validate(&[json!({"type":"record", "name":"R", "fields":[{"name":"self", "type":["null","R"]}]})]).is_ok());
    let declaration = json!({"name":"a", "type":{"type":"record", "name":"Child", "fields":[]}});
    let reference = json!({"name":"b", "type":"Child"});
    assert!(validate(&[json!({"type":"record", "name":"R", "fields":[declaration.clone(),reference.clone()]})]).is_ok());
    assert!(validate(&[json!({"type":"record", "name":"R", "fields":[reference,declaration]})]).is_err());
}
#[test]
fn aliases_are_validated_metadata_and_never_canonical_references() {
    let definition = json!({"type":"record", "name":"R", "aliases":["Alias"], "fields":[]});
    assert!(validate(&[definition.clone(), json!("R")]).is_ok());
    assert!(validate(&[definition.clone(), json!("Alias")]).is_err());
    assert!(validate(&[definition, json!({"type":"Alias"})]).is_err());
    for aliases in [json!(null), json!("Alias"), json!([1]), json!(["bad-name"])] {
        assert!(validate(&[json!({"type":"record", "name":"R", "aliases":aliases, "fields":[]})]).is_err());
    }
}
#[test]
fn textual_references_allow_global_fallback_but_object_references_require_primary_name() {
    let global = record("A");
    let textual = json!({"type":"record", "name":"n.R", "fields":[{"name":"a", "type":"A"}]});
    let object = json!({"type":"record", "name":"n.R", "fields":[{"name":"a", "type":{"type":"A"}}]});
    assert!(validate(&[global.clone(), textual]).is_ok());
    assert!(validate(&[global.clone(), object]).is_err());
    assert!(validate(&[global, json!({"type":"record", "name":"n.R", "fields":[{"name":"a", "type":{"type":".A"}}]})]).is_ok());
}
#[test]
fn canonical_names_honor_last_dot_empty_namespace_and_enclosing_namespace_restoration() {
    assert!(validate(&[record(".A"), record("A")]).is_err());
    assert!(validate(&[json!({"type":"record", "name":"A", "namespace":"", "fields":[]}), record("A")]).is_err());
    assert!(validate(&[json!({"type":"record", "name":"n.R", "fields":[
        {"name":"global", "type":{"type":"record", "name":".A", "fields":[]}},
        {"name":"local", "type":{"type":"record", "name":"B", "fields":[]}},
        {"name":"reference", "type":"n.B"}
    ]}), json!("A"), json!("n.B")]).is_ok());
}
#[test]
fn distinct_namespaces_and_raw_aliases_do_not_create_duplicate_definitions() {
    assert!(validate(&[record("a.R"), record("b.R")]).is_ok());
    assert!(validate(&[json!({"type":"record", "name":"R", "aliases":["S"], "fields":[]}), record("S")]).is_ok());
    assert!(validate(&[record("a.R"), json!({"type":"enum", "name":"R", "namespace":"a", "symbols":["X"]})]).is_err());
}
#[test]
fn primitive_names_cannot_be_named_declarations_but_qualified_suffixes_can() {
    for primitive in ["null","boolean","int","long","float","double","bytes","string"] {
        assert!(validate(&[record(primitive)]).is_err());
        assert!(validate(&[record(&format!("n.{primitive}"))]).is_ok());
        assert!(validate(&[json!(primitive), json!({"type":primitive})]).is_ok());
    }
}
#[test]
fn object_type_wrappers_fail_before_nested_named_declarations() {
    for kind in [json!(["null","int"]), record("A"), json!(null), json!(1)] {
        let calls = std::cell::Cell::new(0);
        assert!(validate_ordered(&[json!({"type":kind})], |_| { calls.set(calls.get()+1); Ok(()) }).is_err());
        assert_eq!(calls.get(), 0);
    }
}
#[test]
fn field_aliases_are_raw_strings_while_fields_and_enum_symbols_use_java_units() {
    assert!(validate(&[json!({"type":"record", "name":"é", "fields":[{"name":"é", "type":"int", "aliases":["space alias", "bad-name"]}]})]).is_ok());
    assert!(validate(&[json!({"type":"record", "name":"R", "fields":[{"name":"bad-name", "type":"int"}]})]).is_err());
    assert!(validate(&[json!({"type":"enum", "name":"E", "symbols":["é","A١"]})]).is_ok());
    assert!(validate(&[json!({"type":"enum", "name":"E", "symbols":["A","A"]})]).is_err());
    assert!(validate(&[json!({"type":"enum", "name":"E", "symbols":["A"], "default":"B"})]).is_err());
}
#[test]
fn declaration_admission_precedes_insertion_and_stops_later_schema_walks() {
    let documents = [record("A"), json!({"type":"record", "name":"B", "fields":[{"name":"bad", "type":"Unknown"}]})];
    let calls = std::cell::Cell::new(0);
    let result = validate_ordered(&documents, |_| { calls.set(calls.get()+1); Err(error("admission-test", "stop")) });
    assert!(result.unwrap_err().to_string().contains("stop"));
    assert_eq!(calls.get(), 1);
}
#[test]
fn names_borrow_namespace_payloads_and_admission_counts_each_new_definition() {
    let namespace = "n".repeat(65536);
    let documents = [json!({"type":"record", "name":"R", "namespace":namespace, "fields":[
        {"name":"a", "type":{"type":"record", "name":"A", "fields":[]}},
        {"name":"b", "type":{"type":"record", "name":"B", "fields":[]}}
    ]})];
    let sizes = std::cell::RefCell::new(Vec::new());
    validate_ordered(&documents, |bytes| { sizes.borrow_mut().push(bytes); Ok(()) }).unwrap();
    let sizes = sizes.into_inner();
    assert_eq!(sizes.len(), 3);
    assert!(sizes.iter().all(|bytes| *bytes < 65536));
}

#[test]
fn duplicate_record_fields_fail_after_all_field_schemas_have_been_walked() {
    let duplicate = json!({"type":"record", "name":"R", "fields":[
        {"name":"a", "type":"int"},
        {"name":"a", "type":{"type":"record", "name":"Later", "fields":[]}}
    ]});
    let calls = std::cell::Cell::new(0);
    let result = validate_ordered(&[duplicate], |_| { calls.set(calls.get()+1); Ok(()) });
    assert!(result.unwrap_err().to_string().contains("Record field name is duplicated"));
    assert_eq!(calls.get(), 2);
    let later_failure = json!({"type":"record", "name":"R", "fields":[
        {"name":"a", "type":"int"}, {"name":"a", "type":"int"}, {"name":"b", "type":"Unknown"}
    ]});
    assert!(validate(&[later_failure]).unwrap_err().to_string().contains("Undefined canonical schema name"));
}
