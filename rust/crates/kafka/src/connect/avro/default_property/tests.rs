use super::*;
use super::super::{default_scalar::{self,ConversionPath,ScalarInput},default_input::DefaultAttempt};
use super::super::super::{ConnectSchema,SchemaKind};

fn raw(text:&str)->DefaultJson {DefaultJson::from_str(text).unwrap()}
#[test]
fn absent_and_out_of_long_properties_produce_no_node_while_explicit_null_survives() {
    assert!(node(None).unwrap().is_none());
    for value in [raw("9223372036854775808"),raw("-9223372036854775809")] {assert!(node(Some(&value)).unwrap().is_none());}
    let explicit=DefaultJson::Null;let normalized=node(Some(&explicit)).unwrap().unwrap();
    assert!(std::ptr::eq(normalized.raw(),&explicit));assert!(normalized.raw().is_null());
}
#[test]
fn nested_java_null_fallthrough_is_an_unchecked_encoding_failure() {
    for text in ["[9223372036854775808]",r#"{"key":9223372036854775808}"#,r#"{"key":[-9223372036854775809]}"#] {
        let value=raw(text);assert!(node(Some(&value)).unwrap_err().code.ends_with("avro-default-unchecked"));
    }
    for text in ["[null]",r#"{"key":null}"#] {let value=raw(text);assert!(node(Some(&value)).unwrap().is_some());}
}
#[test]
fn the_complete_untyped_object_scan_precedes_encoding_an_earlier_java_null() {
    let mut nested=DefaultJson::Null;for _ in 0..130 {nested=DefaultJson::Array(vec![nested]);}
    let value=DefaultJson::Array(vec![raw("9223372036854775808"),nested]);
    assert!(node(Some(&value)).unwrap_err().code.ends_with("batch-too-large"));
}
#[test]
fn successful_property_views_borrow_every_payload_without_rewriting_raw_schema_data() {
    let value=raw(r#"{"z":[null,1e400,-0,"é"],"a":true}"#);let before=value.clone();
    let normalized=node(Some(&value)).unwrap().unwrap();assert!(std::ptr::eq(normalized.raw(),&value));assert_eq!(value,before);
    let keys=normalized.raw().as_object().unwrap().iter().map(|(key,_)|key.as_str()).collect::<Vec<_>>();assert_eq!(keys,vec!["z","a"]);
}
#[test]
fn borrowed_float_and_integer_lexemes_retain_actual_node_accessor_semantics() {
    for (literal,kind,schema) in [("1e400",SchemaKind::F64,apache_avro::Schema::Double),("-0",SchemaKind::I32,apache_avro::Schema::Int)] {
        let value=raw(literal);let property=node(Some(&value)).unwrap().unwrap();
        let destination=ConnectSchema::new(kind);
        let prepared=match default_scalar::prepare(ScalarInput::JsonNode(property.at(&schema)),destination.borrowed(),ConversionPath::Wrapper,None).unwrap() {DefaultAttempt::Value(value)=>value,DefaultAttempt::Invalid(cause)=>panic!("{cause}")};
        assert!(!prepared.is_missing());assert!(!prepared.is_null());assert!(prepared.bound().unwrap()>0);
    }
}
#[test]
fn explicit_null_property_produces_a_nullnode_accessor_input_not_a_missing_node() {
    let value=DefaultJson::Null;let schema=apache_avro::Schema::String;let destination=ConnectSchema::new(SchemaKind::String);
    let property=node(Some(&value)).unwrap().unwrap();let prepared=match default_scalar::prepare(ScalarInput::JsonNode(property.at(&schema)),destination.borrowed(),ConversionPath::Wrapper,None).unwrap() {DefaultAttempt::Value(value)=>value,DefaultAttempt::Invalid(cause)=>panic!("{cause}")};
    assert!(!prepared.is_missing());assert!(!prepared.is_null());
}

#[test]
fn registry_decimal_carriers_cannot_silently_replace_the_schema_parser_profile() {
    let value=DefaultJson::from_registry_str("1.5").unwrap();
    assert!(node(Some(&value)).unwrap_err().code.ends_with("unsupported-avro-default"));
    let nested=DefaultJson::Array(vec![value]);assert!(node(Some(&nested)).unwrap_err().code.ends_with("unsupported-avro-default"));
    let normal=raw("1.5");assert!(node(Some(&normal)).unwrap().is_some());
}

#[test]
fn supplied_field_objects_skip_encoding_after_the_property_object_stage() {
    let value=raw(r#"{"key":9223372036854775808}"#);let property=object(Some(&value)).unwrap().unwrap();
    let field_raw=raw(r#""field default""#);let schema=apache_avro::Schema::String;let names=std::collections::BTreeMap::new();
    let field_default=Some(match super::super::default_input::FieldObject::prepare(&field_raw,&schema,&names).unwrap() {DefaultAttempt::Value(value)=>value,DefaultAttempt::Invalid(cause)=>panic!("{cause}")});
    let encoded=if field_default.is_none() {Some(property.node().unwrap())} else {None};
    assert!(encoded.is_none());assert!(matches!(field_default.unwrap().kind().unwrap(),super::super::default_input::ObjectKind::Text("field default")));
    assert!(property.node().unwrap_err().code.ends_with("avro-default-unchecked"));
    let explicit=DefaultJson::Null;let sentinel=object(Some(&explicit)).unwrap().unwrap();assert!(sentinel.node().unwrap().raw().is_null());
    assert!(object(Some(&raw("9223372036854775808"))).unwrap().is_none());
}
