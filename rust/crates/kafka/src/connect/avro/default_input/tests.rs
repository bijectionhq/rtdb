use super::*;

fn raw(text:&str)->DefaultJson { DefaultJson::from_str(text).unwrap() }
fn parsed_schema(text:&str)->Schema { Schema::parse_str(text).unwrap() }
fn definitions(schema:&Schema)->BTreeMap<String,Schema> { let mut names=BTreeMap::new(); super::super::collect_names(schema,&mut names); names }
fn prepared<'a>(value:&'a DefaultJson,schema:&'a Schema,names:&'a BTreeMap<String,Schema>)->FieldObject<'a> {
    match FieldObject::prepare(value,schema,names).unwrap() { DefaultAttempt::Value(value)=>value,DefaultAttempt::Invalid(cause)=>panic!("{cause}") }
}

#[test]
fn record_source_fields_are_checked_before_a_scrubbed_destination_can_ignore_them() {
    let schema=parsed_schema(r#"{"type":"record","name":"R","fields":[{"name":"_C3_A9","type":"int"}]}"#); let names=definitions(&schema);
    let value=raw(r#"{"_C3_A9":1,"unused":2}"#);
    assert!(matches!(FieldObject::prepare(&value,&schema,&names).unwrap(),DefaultAttempt::Invalid(_)));
}
#[test]
fn raw_record_aliases_do_not_supply_jackson_source_fields() {
    let schema=parsed_schema(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int","aliases":["alias"]}]}"#); let names=definitions(&schema);
    let value=raw(r#"{"alias":1}"#);
    assert!(matches!(FieldObject::prepare(&value,&schema,&names).unwrap(),DefaultAttempt::Invalid(_)));
}
#[test]
fn field_object_selection_keeps_the_actual_child_schema_and_can_repeat_a_child() {
    let schema=parsed_schema(r#"{"type":"record","name":"R","fields":[{"name":"_C3_A9","type":"long"}]}"#); let names=definitions(&schema); let value=raw(r#"{"_C3_A9":1}"#);
    let input=prepared(&value,&schema,&names);
    let first=input.field("é",true).unwrap().unwrap(); let second=input.field("_C3_A9",true).unwrap().unwrap();
    assert!(std::ptr::eq(first.raw(),second.raw()));
    assert!(std::ptr::eq(first.schema.unwrap(),second.schema.unwrap()));
    assert!(matches!(first.kind().unwrap(),ObjectKind::Long(1)));
    assert!(input.field("é",false).unwrap().is_none());
}
#[test]
fn present_null_is_distinct_from_an_absent_field_default_map_key() {
    let schema=parsed_schema(r#"{"type":"record","name":"R","fields":[{"name":"a","type":["null","int"]},{"name":"b","type":"int"}]}"#); let names=definitions(&schema); let value=raw(r#"{"a":null}"#);
    let input=prepared(&value,&schema,&names);
    assert!(matches!(input.field("a",false).unwrap().unwrap().kind().unwrap(),ObjectKind::Null));
    assert!(input.field("b",false).unwrap().is_none());
}
#[test]
fn json_node_record_selection_uses_raw_names_and_preserves_missing_and_explicit_null() {
    let schema=parsed_schema(r#"{"type":"record","name":"R","fields":[]}"#); let value=raw(r#"{"é":1,"_C3_A9":2,"null":null}"#);
    let input=JsonNode::new(Some(&value),&schema);
    assert_eq!(input.field("é",&Schema::Int).raw().unwrap().int_value(),Some(1));
    assert_eq!(input.field("_C3_A9",&Schema::Int).raw().unwrap().int_value(),Some(2));
    assert!(input.field("null",&Schema::Int).raw().unwrap().is_null());
    assert!(input.field("missing",&Schema::Int).raw().is_none());
    assert!(std::ptr::eq(input.schema(),&schema));
}
#[test]
fn primitive_source_incompatibility_becomes_java_null_without_rewriting_boolean_class() {
    let names=BTreeMap::new(); let string=raw(r#""text""#); let boolean=raw("true"); let integer=raw("1");
    assert!(matches!(prepared(&string,&Schema::Int,&names).kind().unwrap(),ObjectKind::Null));
    assert!(matches!(prepared(&boolean,&Schema::Int,&names).kind().unwrap(),ObjectKind::Boolean(true)));
    assert!(matches!(prepared(&integer,&Schema::Boolean,&names).kind().unwrap(),ObjectKind::Null));
    assert!(matches!(prepared(&string,&Schema::Bytes,&names).kind().unwrap(),ObjectKind::BinaryText("text")));
}
#[test]
fn integral_jackson_classes_follow_the_physical_schema_and_integer_width() {
    let names=BTreeMap::new(); let small=raw("1"); let large=raw("2147483648"); let huge=raw("9223372036854775808");
    assert!(matches!(prepared(&small,&Schema::Int,&names).kind().unwrap(),ObjectKind::Int(1)));
    assert!(matches!(prepared(&small,&Schema::Long,&names).kind().unwrap(),ObjectKind::Long(1)));
    assert!(matches!(prepared(&small,&Schema::Float,&names).kind().unwrap(),ObjectKind::Float(value) if value==1.0));
    assert!(matches!(prepared(&small,&Schema::Double,&names).kind().unwrap(),ObjectKind::Double(value) if value==1.0));
    assert!(matches!(prepared(&large,&Schema::Int,&names).kind().unwrap(),ObjectKind::Long(2147483648)));
    assert!(matches!(prepared(&huge,&Schema::Long,&names).kind().unwrap(),ObjectKind::Null));
}
#[test]
fn floating_json_nodes_coerce_only_to_float_or_double_object_classes() {
    let names=BTreeMap::new(); let floating=raw("1.5");
    assert!(matches!(prepared(&floating,&Schema::Float,&names).kind().unwrap(),ObjectKind::Float(value) if value==1.5));
    assert!(matches!(prepared(&floating,&Schema::Double,&names).kind().unwrap(),ObjectKind::Double(value) if value==1.5));
    assert!(matches!(prepared(&floating,&Schema::Int,&names).kind().unwrap(),ObjectKind::Null));
}
#[test]
fn array_on_nonarray_source_is_invalid_before_map_conversion() {
    let names=BTreeMap::new(); let value=raw("[1]");
    assert!(matches!(FieldObject::prepare(&value,&Schema::Int,&names).unwrap(),DefaultAttempt::Invalid(_)));
    let array=parsed_schema(r#"{"type":"array","items":"int"}"#);
    assert!(matches!(prepared(&value,&array,&names).kind().unwrap(),ObjectKind::Array(values) if values.len()==1));
}
#[test]
fn object_on_scalar_source_has_untyped_children_while_nested_record_unknown_keys_fail() {
    let names=BTreeMap::new(); let value=raw(r#"{"key":1}"#);
    let input=prepared(&value,&Schema::Int,&names);
    assert!(matches!(input.kind().unwrap(),ObjectKind::Object(values) if values.len()==1));
    assert!(matches!(input.field("key",false).unwrap().unwrap().kind().unwrap(),ObjectKind::Int(1)));
    let schema=parsed_schema(r#"{"type":"record","name":"Outer","fields":[{"name":"child","type":{"type":"record","name":"Inner","fields":[{"name":"a","type":"int"}]}}]}"#); let names=definitions(&schema); let value=raw(r#"{"child":{"unused":1}}"#);
    assert!(matches!(FieldObject::prepare(&value,&schema,&names).unwrap(),DefaultAttempt::Invalid(_)));
}
#[test]
fn schema_refs_and_first_union_members_control_field_object_classes() {
    let schema=parsed_schema(r#"{"type":"record","name":"R","fields":[{"name":"self","type":["null","R"]}]}"#); let names=definitions(&schema); let value=raw(r#"{"self":null}"#);
    let input=prepared(&value,&schema,&names);
    assert!(matches!(input.field("self",false).unwrap().unwrap().kind().unwrap(),ObjectKind::Null));
    let schema=parsed_schema(r#"["long","string"]"#); let number=raw("1");
    assert!(matches!(prepared(&number,&schema,&names).kind().unwrap(),ObjectKind::Long(1)));
}
#[test]
fn native_traversal_limits_propagate_outside_semantic_invalid_defaults() {
    let names=BTreeMap::new(); let mut value=DefaultJson::Null;
    for _ in 0..130 { value=DefaultJson::Array(vec![value]); }
    let input=FieldObject {value:&value,schema:None,names:&names};
    assert!(input.validate_source(0).unwrap_err().code.ends_with("batch-too-large"));
}

#[test]
fn empty_arrays_do_not_demand_a_source_element_schema_before_scrubbed_selection() {
    let names=BTreeMap::new(); let empty=raw("[]"); let nonempty=raw("[1]");
    assert!(matches!(prepared(&empty,&Schema::Int,&names).kind().unwrap(),ObjectKind::Array(values) if values.is_empty()));
    assert!(matches!(FieldObject::prepare(&nonempty,&Schema::Int,&names).unwrap(),DefaultAttempt::Invalid(_)));
    let schema=Schema::parse_str_with_list_with_name_policy(r#"{"type":"record","name":"R","fields":[{"name":"é","type":"int"},{"name":"_C3_A9","type":"int"}]}"#,Vec::<&str>::new(),&super::super::names::JavaNames).unwrap().0;
    let names=definitions(&schema); let empty=raw(r#"{"é":[],"_C3_A9":1}"#);
    assert!(matches!(prepared(&empty,&schema,&names).field("é",true).unwrap().unwrap().kind().unwrap(),ObjectKind::Int(1)));
    let nonempty=raw(r#"{"é":[1],"_C3_A9":1}"#);
    assert!(matches!(FieldObject::prepare(&nonempty,&schema,&names).unwrap(),DefaultAttempt::Invalid(_)));
}

#[test]
fn an_empty_union_is_semantically_invalid_before_json_but_undemanded_fields_stay_absent() {
    let schema=parsed_schema("[]"); let names=BTreeMap::new();
    for text in ["null","1","[]","{}"] {
        let value=raw(text);
        assert!(matches!(FieldObject::prepare(&value,&schema,&names).unwrap(),DefaultAttempt::Invalid(_)));
    }
    let schema=parsed_schema(r#"{"type":"record","name":"R","fields":[{"name":"empty","type":[]}]}"#); let names=definitions(&schema);
    let absent=raw("{}"); assert!(prepared(&absent,&schema,&names).field("empty",false).unwrap().is_none());
    let present=raw(r#"{"empty":null}"#); assert!(matches!(FieldObject::prepare(&present,&schema,&names).unwrap(),DefaultAttempt::Invalid(_)));
    let array=parsed_schema(r#"{"type":"array","items":[]}"#); let empty=raw("[]");
    assert!(matches!(prepared(&empty,&array,&names).kind().unwrap(),ObjectKind::Array(values) if values.is_empty()));
    let nonempty=raw("[null]"); assert!(matches!(FieldObject::prepare(&nonempty,&array,&names).unwrap(),DefaultAttempt::Invalid(_)));
}
#[test]
fn integral_float_defaults_round_through_the_jackson_double_accessor() {
    let value=raw("1152921573326323713"); let names=BTreeMap::new();
    let ObjectKind::Float(value)=prepared(&value,&Schema::Float,&names).kind().unwrap() else { panic!("expected Float") };
    assert_eq!(value.to_bits(),0x5d800000);
}
