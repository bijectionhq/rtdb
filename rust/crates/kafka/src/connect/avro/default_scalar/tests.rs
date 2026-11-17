use std::{collections::BTreeMap, sync::Arc};
use super::*;
use super::super::super::{ConnectSchema, ConversionContext, SchemaKind};
use rtdb_columnar::{MemoryBudget, MemoryReservation};

#[derive(Debug)]
struct Output { value: Value, _reservation: Arc<MemoryReservation> }
impl PartialEq<Value> for Output { fn eq(&self, other: &Value) -> bool { self.value == *other } }

fn raw(text: &str) -> DefaultJson { DefaultJson::from_str(text).unwrap() }
fn node<'a>(value: Option<&'a DefaultJson>, schema: &'a Schema) -> ScalarInput<'a> { ScalarInput::JsonNode(JsonNode::new(value, schema)) }
fn field<'a>(value: &'a DefaultJson, schema: &'a Schema, names: &'a BTreeMap<String, Schema>) -> ScalarInput<'a> {
    match FieldObject::prepare(value, schema, names).unwrap() { DefaultAttempt::Value(value) => ScalarInput::FieldObject(value), DefaultAttempt::Invalid(cause) => panic!("{cause}") }
}
fn prepared<'a>(input: ScalarInput<'a>, schema: &ConnectSchema, logical: Option<&str>) -> PreparedScalar<'a> {
    match prepare(input, schema.borrowed(), ConversionPath::Wrapper, logical).unwrap() { DefaultAttempt::Value(value) => value, DefaultAttempt::Invalid(cause) => panic!("{cause}") }
}
fn output(input: ScalarInput<'_>, schema: &ConnectSchema, logical: Option<&str>) -> Output {
    admitted(prepared(input, schema, logical))
}
fn admitted(value: PreparedScalar<'_>) -> Output {
    let budget = MemoryBudget::new(value.bound().unwrap());
    let reservation = budget.reserve(value.bound().unwrap()).unwrap();
    let result = value.materialize().unwrap();
    assert!(reservation.bytes() >= result.retained_bytes().unwrap());
    Output { value: result, _reservation: reservation }
}

#[test]
fn field_objects_keep_actual_jackson_classes_and_float_double_rounding() {
    let names = BTreeMap::new(); let one = raw("1");
    assert_eq!(output(field(&one, &Schema::Int, &names), &ConnectSchema::new(SchemaKind::I32), None), Value::I32(1));
    assert_eq!(output(field(&one, &Schema::Long, &names), &ConnectSchema::new(SchemaKind::I64), None), Value::I64(1));
    assert!(matches!(prepare(field(&one, &Schema::Long, &names), ConnectSchema::new(SchemaKind::I32).borrowed(), ConversionPath::Wrapper, None).unwrap(), DefaultAttempt::Invalid(_)));
    for (literal, bits) in [("1152921573326323713", 0x5d800000), ("-1152921573326323713", 0xdd800000)] {
        let value = raw(literal);
        let admitted = output(field(&value, &Schema::Float, &names), &ConnectSchema::new(SchemaKind::F32), None);
        let Value::F32(value) = &admitted.value else { panic!("expected Float") };
        assert_eq!(value.to_bits(), bits);
    }
}

#[test]
fn native_child_defaults_are_reconverted_by_java_class_instead_of_cloned() {
    assert_eq!(output(ScalarInput::Native(&Value::I32(257)), &ConnectSchema::new(SchemaKind::I8), None), Value::I8(1));
    for (value, kind) in [(Value::I8(1), SchemaKind::I8), (Value::I16(1), SchemaKind::I16), (Value::I64(1), SchemaKind::I32), (Value::F64(1.0), SchemaKind::F32)] {
        assert!(matches!(prepare(ScalarInput::Native(&value), ConnectSchema::new(kind).borrowed(), ConversionPath::MapChild, None).unwrap(), DefaultAttempt::Invalid(_)));
    }
    let date = super::super::super::data::date_value(0);
    assert!(matches!(prepare(ScalarInput::Native(&date), ConnectSchema::new(SchemaKind::I32).borrowed(), ConversionPath::MapChild, Some("org.apache.kafka.connect.data.Date")).unwrap(), DefaultAttempt::Invalid(_)));
    let value = Value::Decimal(DecimalValue::new(vec![1;16], 32, 0, 128).unwrap());
    assert!(matches!(prepare(ScalarInput::Native(&value), ConnectSchema::new(SchemaKind::Bytes).borrowed(), ConversionPath::MapChild, Some("org.apache.kafka.connect.data.Decimal")).unwrap(), DefaultAttempt::Invalid(_)));
}

#[test]
fn nullnode_accessors_remain_distinct_from_missing_and_field_object_null() {
    let value = DefaultJson::Null; let names = BTreeMap::new(); let int = ConnectSchema::new(SchemaKind::I32);
    let actual = Schema::Int;
    let missing = prepared(node(None, &actual), &int, None);
    assert!(missing.is_missing()); assert!(!missing.is_null());
    let null = prepared(field(&value, &actual, &names), &int, None);
    assert!(null.is_null()); assert!(!null.is_missing());
    assert_eq!(output(node(Some(&value), &Schema::Int), &int, None), Value::I32(0));
    assert_eq!(output(node(Some(&value), &Schema::String), &ConnectSchema::new(SchemaKind::String), None), Value::Utf8("null".into()));
    assert_eq!(output(node(Some(&value), &Schema::Boolean), &ConnectSchema::new(SchemaKind::Bool), None), Value::Bool(false));
    assert!(prepared(node(Some(&value), &Schema::Bytes), &ConnectSchema::new(SchemaKind::Bytes), None).is_null());
}

#[test]
fn null_and_missing_fast_paths_do_not_demand_a_scalar_container_or_child_schema() {
    let container = ConnectSchema::new(SchemaKind::Struct(vec![])); let names = BTreeMap::new();
    let value = DefaultJson::Null;
    assert!(prepared(ScalarInput::Native(&Value::Null), &container, Some("org.apache.kafka.connect.data.Date")).is_null());
    assert!(prepared(field(&value, &Schema::Int, &names), &container, None).is_null());
    let reference = Schema::Ref { name: apache_avro::schema::Name { name: "Missing".into(), namespace: None } };
    assert!(prepared(node(None, &reference), &container, None).is_missing());
    assert!(prepared(node(Some(&raw("1")), &Schema::Null), &container, None).is_null());
}

#[test]
fn json_primitive_accessors_keep_low_bits_saturation_and_source_schema_dispatch() {
    for (text, expected) in [("18446744073709551617",1), ("-18446744073709551617",-1)] {
        let value = raw(text);
        assert_eq!(output(node(Some(&value), &Schema::Int), &ConnectSchema::new(SchemaKind::I32), None), Value::I32(expected));
        assert_eq!(output(node(Some(&value), &Schema::Long), &ConnectSchema::new(SchemaKind::I64), None), Value::I64(i64::from(expected)));
    }
    assert_eq!(output(node(Some(&raw("2147483648.0")), &Schema::Int), &ConnectSchema::new(SchemaKind::I16), None), Value::I16(-1));
    assert_eq!(output(node(Some(&raw("3.9")), &Schema::Int), &ConnectSchema::new(SchemaKind::I32), None), Value::I32(3));
    assert_eq!(output(node(Some(&raw(r#""17""#)), &Schema::Int), &ConnectSchema::new(SchemaKind::I32), None), Value::I32(0));
    assert!(prepared(node(Some(&raw("1")), &Schema::Int), &ConnectSchema::new(SchemaKind::I64), None).is_null());
    for (literal, bits) in [("1152921573326323713",0x5d800000),("-1152921573326323713",0xdd800000)] {
        let admitted = output(node(Some(&raw(literal)), &Schema::Float), &ConnectSchema::new(SchemaKind::F32), None);
        let Value::F32(value) = &admitted.value else { panic!("expected Float") };
        assert_eq!(value.to_bits(),bits);
    }
}

#[test]
fn json_boolean_accessors_distinguish_integral_floating_decimal_and_java_trimmed_text() {
    let destination = ConnectSchema::new(SchemaKind::Bool);
    for (text, expected) in [("1",true), ("18446744073709551616",true), ("0",false), ("-0",false), ("1.0",false), ("true",true), (r#""\t true\u0020""#,true), (r#""\u00a0true""#,false), (r#""TRUE""#,false)] {
        assert_eq!(output(node(Some(&raw(text)), &Schema::Boolean), &destination, None), Value::Bool(expected), "{text}");
    }
    let decimal = DefaultJson::from_registry_str("1.0").unwrap();
    assert_eq!(output(node(Some(&decimal), &Schema::Boolean), &destination, None), Value::Bool(false));
}

#[test]
fn json_text_accessors_preserve_node_classes_and_defer_owned_formatting() {
    let destination = ConnectSchema::new(SchemaKind::String);
    for (text, expected) in [("null","null"), ("true","true"), ("-0","0"), ("-0.0","-0.0"), ("1e20","1.0E20"), ("[]",""), ("{}","")] {
        assert_eq!(output(node(Some(&raw(text)), &Schema::String), &destination, None), Value::Utf8(expected.into()));
    }
    let decimal = DefaultJson::from_registry_str("1.200").unwrap();
    let actual = Schema::String;
    let prepared = prepared(node(Some(&decimal), &actual), &destination, None);
    assert!(matches!(prepared.value, PreparedValue::Text(Text::Decimal(_))));
    assert!(prepared.bound().unwrap() >= 4096);
    assert_eq!(admitted(prepared), Value::Utf8("1.200".into()));
}

#[test]
fn deferred_decimal_numeric_accessors_allocate_only_during_materialization() {
    let value = DefaultJson::from_registry_str("1.25").unwrap();
    let float = ConnectSchema::new(SchemaKind::F32); let double = ConnectSchema::new(SchemaKind::F64);
    let actual = Schema::Float;
    let prepared = prepared(node(Some(&value), &actual), &float, None);
    assert!(matches!(prepared.value, PreparedValue::DecimalNumber(_, Scalar::F32)));
    assert_eq!(admitted(prepared), Value::F32(1.25));
    assert_eq!(output(node(Some(&value), &Schema::Double), &double, None), Value::F64(1.25));
}

#[test]
fn binary_source_text_is_borrowed_until_admitted_latin1_materialization() {
    let value = raw(r#""é☃😀\u0000""#); let names = BTreeMap::new(); let schema = ConnectSchema::new(SchemaKind::Bytes);
    let actual = Schema::Bytes;
    let prepared = prepared(field(&value, &actual, &names), &schema, None);
    assert!(matches!(prepared.value, PreparedValue::Bytes(Bytes::Latin1(_))));
    assert_eq!(admitted(prepared), Value::Binary(vec![233,63,63,0]));
    assert_eq!(output(node(Some(&value), &Schema::Bytes), &schema, None), Value::Binary(vec![233,63,63,0]));
    assert!(prepared_binary_null(&schema));
}
fn prepared_binary_null(schema: &ConnectSchema) -> bool { prepared(node(Some(&raw("1")), &Schema::Bytes), schema, None).is_null() }

#[test]
fn builder_time_logical_name_override_does_not_read_the_final_declared_name() {
    let mut schema = ConnectSchema::new(SchemaKind::I32); schema.name = Some("org.apache.kafka.connect.data.Date".into());
    assert_eq!(output(ScalarInput::Native(&Value::I32(1)), &schema, None), Value::I32(1));
    assert_eq!(output(ScalarInput::Native(&Value::I32(1)), &schema, Some("org.apache.kafka.connect.data.Date")), super::super::super::data::date_value(86_400_000));
}

#[test]
fn prepared_class_validation_waits_for_the_callers_chosen_default_boundary() {
    let schema = ConnectSchema::new(SchemaKind::I32); let raw = raw("1");
    let actual = Schema::Long;
    let value = prepared(node(Some(&raw), &actual), &schema, None);
    assert!(matches!(value.validate_destination(schema.borrowed(), None), DefaultAttempt::Invalid(_)));
    let mut schema = schema; schema.name = Some("org.apache.kafka.connect.data.Date".into());
    let native = Value::I32(1);
    let value = prepared(ScalarInput::Native(&native), &schema, None);
    assert!(matches!(value.validate_destination(schema.borrowed(), None), DefaultAttempt::Value(())));
    assert!(matches!(value.validate_destination(schema.borrowed(), Some("org.apache.kafka.connect.data.Date")), DefaultAttempt::Invalid(_)));
    let date = prepared(ScalarInput::Native(&native), &schema, Some("org.apache.kafka.connect.data.Date"));
    assert!(matches!(date.validate_destination(schema.borrowed(), Some("org.apache.kafka.connect.data.Date")), DefaultAttempt::Value(())));
    let native = Value::Null;
    let null = prepared(ScalarInput::Native(&native), &schema, None);
    assert!(matches!(null.validate_destination(schema.borrowed(), None), DefaultAttempt::Value(())));
}

#[test]
fn date_defaults_keep_primitive_width_range_and_jackson_numeric_accessor_order() {
    let schema = ConnectSchema::new(SchemaKind::I32);
    assert_eq!(output(node(Some(&raw("1e50")), &Schema::Int), &schema, Some("org.apache.kafka.connect.data.Date")), super::super::super::data::date_value(i64::from(i32::MAX)*86_400_000));
    assert_eq!(output(ScalarInput::Native(&Value::I32(86_400_000)), &schema, Some("org.apache.kafka.connect.data.Time")), super::super::super::data::date_value(86_400_000));
    for value in [-1,86_400_001] {
        assert!(matches!(prepare(ScalarInput::Native(&Value::I32(value)), schema.borrowed(), ConversionPath::MapChild, Some("org.apache.kafka.connect.data.Time")).unwrap(), DefaultAttempt::Invalid(_)));
    }
    for value in [i64::MIN,i64::MAX] {
        assert_eq!(output(ScalarInput::Native(&Value::I64(value)), &ConnectSchema::new(SchemaKind::I64), Some("org.apache.kafka.connect.data.Timestamp")), super::super::super::data::date_value(value));
    }
    assert!(matches!(prepare(ScalarInput::Native(&Value::I32(1)), ConnectSchema::new(SchemaKind::I8).borrowed(), ConversionPath::Wrapper, Some("org.apache.kafka.connect.data.Date")).unwrap(), DefaultAttempt::Invalid(_)));
}

#[test]
fn decimal_empty_coefficients_are_unchecked_before_semantic_scale_failures() {
    let schema = ConnectSchema::new(SchemaKind::Bytes); let empty = Value::Binary(vec![]); let nonempty = Value::Binary(vec![1]);
    let error = prepare(ScalarInput::Native(&empty), schema.borrowed(), ConversionPath::Wrapper, Some("org.apache.kafka.connect.data.Decimal")).unwrap_err();
    assert!(error.code.ends_with("avro-default-unchecked")); assert_eq!(error.message.as_deref(), Some("Decimal coefficient bytes are empty"));
    let DefaultAttempt::Invalid(error) = prepare(ScalarInput::Native(&nonempty), schema.borrowed(), ConversionPath::Wrapper, Some("org.apache.kafka.connect.data.Decimal")).unwrap() else { panic!("expected semantic scale error") };
    assert_eq!(error.message.as_deref(), Some("Decimal schema has no scale"));
    let mut invalid = schema; invalid.parameters.insert("scale".into(),"bad".into());
    assert!(prepare(ScalarInput::Native(&empty), invalid.borrowed(), ConversionPath::Wrapper, Some("org.apache.kafka.connect.data.Decimal")).is_err());
    let huge = Value::Binary(vec![127;8192]);
    let DefaultAttempt::Invalid(error) = prepare(ScalarInput::Native(&huge), invalid.borrowed(), ConversionPath::Wrapper, Some("org.apache.kafka.connect.data.Decimal")).unwrap() else { panic!("expected scale before precision") };
    assert_eq!(error.message.as_deref(), Some("Decimal scale is not a signed 32-bit integer"));
}

#[test]
fn decimal_stack_coefficients_keep_sign_scale_padding_and_writer_width() {
    let mut schema = ConnectSchema::new(SchemaKind::Bytes); schema.parameters.insert("scale".into(),"-2147483648".into());
    for (bytes, negative) in [(vec![0,0,1],false),(vec![255,255],true)] {
        let value = Value::Binary(bytes); let prepared = prepared(ScalarInput::Native(&value), &schema, Some("org.apache.kafka.connect.data.Decimal"));
        assert!(matches!(prepared.value, PreparedValue::Decimal { width:16, .. }));
        let admitted = admitted(prepared);
        let Value::Decimal(value) = &admitted.value else { panic!("expected Decimal") };
        assert_eq!((value.precision,value.scale,value.unscaled.len()),(32,i32::MIN,16));
        assert_eq!(value.unscaled[0]&128!=0,negative);
    }
    let boundary = Value::Binary(vec![4,238,45,109,65,91,133,172,239,129,0,0,0,0]);
    let admitted = output(ScalarInput::Native(&boundary), &schema, Some("org.apache.kafka.connect.data.Decimal"));
    let Value::Decimal(value) = &admitted.value else { panic!("expected wider Decimal") };
    assert_eq!((value.precision,value.unscaled.len()),(64,32));
    assert_eq!(scale_value("+٢"),Some(2)); assert_eq!(scale_value("𝟚"),None); assert_eq!(scale_value(" 2"),None);
}

#[test]
fn scalar_bounds_cover_output_and_deferred_scratch_under_caller_owned_admission() {
    let source = Value::Utf8("x".repeat(8192)); let schema = ConnectSchema::new(SchemaKind::String);
    let prepared = prepared(ScalarInput::Native(&source), &schema, None); let bound = prepared.bound().unwrap();
    assert!(MemoryBudget::new(bound-1).reserve(bound).is_err());
    let budget = MemoryBudget::new(bound); let mut context = ConversionContext::new(Arc::clone(&budget));
    let held = context.reserve(bound).unwrap(); let value = prepared.materialize().unwrap();
    assert!(bound >= value.retained_bytes().unwrap()); drop(context); assert_eq!(budget.used_bytes(),bound);
    drop(value); drop(held); assert_eq!(budget.used_bytes(),0);
    assert!(add(usize::MAX,1).is_err()); assert!(mul(usize::MAX,4).is_err());
}

#[test]
fn unresolved_or_container_inputs_remain_outer_errors_instead_of_suppressed_defaults() {
    let value = raw("1"); let scalar = ConnectSchema::new(SchemaKind::I32);
    let reference = Schema::Ref { name: apache_avro::schema::Name { name:"R".into(),namespace:None } };
    assert!(prepare(node(Some(&value), &reference), scalar.borrowed(), ConversionPath::Wrapper, None).is_err());
    assert!(prepare(node(Some(&value), &Schema::Int), scalar.borrowed(), ConversionPath::MapChild, None).is_err());
    assert!(prepare(ScalarInput::Native(&Value::I32(1)), ConnectSchema::new(SchemaKind::Struct(vec![])).borrowed(), ConversionPath::Wrapper, None).is_err());
    let mut decimal = ConnectSchema::new(SchemaKind::Bytes); decimal.parameters.insert("scale".into(),"0".into()); let value = Value::Binary(vec![127;8192]);
    assert!(prepare(ScalarInput::Native(&value), decimal.borrowed(), ConversionPath::Wrapper, Some("org.apache.kafka.connect.data.Decimal")).is_err());
}
