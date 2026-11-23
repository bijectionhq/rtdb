use std::sync::Arc;
use super::*;
use super::super::{Avro, AvroPlan, AvroResolution, ConversionContext};
use rtdb_columnar::MemoryBudget;
use rtdb_types::Value;

fn options(values: &[(&str, &str)]) -> BTreeMap<String, String> {
    values.iter().map(|(key, value)| ((*key).into(), (*value).into())).collect()
}
fn plan(text: &str, values: &[(&str, &str)]) -> Arc<AvroDatumPlan> {
    Arc::new(AvroDatumPlan::parse(text, &[], &options(values)).unwrap())
}
fn context() -> ConversionContext { ConversionContext::new(MemoryBudget::new(1 << 24)) }

#[test]
fn both_reader_options_admit_bare_primitives_and_reserved_properties() {
    for mode in ["specific.avro.reader", "schema.reflection"] {
        for schema in [r#""null""#, r#""boolean""#, r#""int""#, r#""long""#, r#""float""#, r#""double""#, r#""string""#, r#""bytes""#,
            r#"{"type":"int"}"#, r#"{"type":"int","doc":"ignored schema property"}"#] {
            assert!(plan(schema, &[(mode, "true")]).validate_reader(None).is_ok(), "{mode}: {schema}");
        }
    }
}

#[test]
fn property_bearing_primitives_do_not_take_the_generic_reader_shortcut() {
    for mode in ["specific.avro.reader", "schema.reflection"] {
        for schema in [r#"{"type":"int","connect.type":"int8"}"#, r#"{"type":"int","logicalType":"date"}"#,
            r#"{"type":"string","avro.java.string":"String"}"#, r#"{"type":"bytes","custom":null}"#] {
            assert!(plan(schema, &[(mode, "true")]).validate_reader(None).is_err(), "{mode}: {schema}");
        }
    }
}

#[test]
fn primitive_values_decode_under_specific_and_reflection_modes() {
    for mode in ["specific.avro.reader", "schema.reflection"] {
        for (schema, bytes, expected) in [
            (r#""boolean""#, &[1][..], Value::Bool(true)),
            (r#""int""#, &[14][..], Value::I32(7)),
            (r#""long""#, &[14][..], Value::I64(7)),
            (r#""float""#, &[0, 0, 0x80, 0x3f][..], Value::F32(1.0)),
            (r#""double""#, &[0, 0, 0, 0, 0, 0, 0xf0, 0x3f][..], Value::F64(1.0)),
            (r#""string""#, &[2, b'x'][..], Value::Utf8("x".into())),
            (r#""bytes""#, &[2, 4, 6][..], Value::Binary(vec![2, 4, 6])),
        ] {
            let plan = AvroPlan::parse_version(schema, &[], &options(&[(mode, "true")]), None).unwrap();
            assert_eq!(plan.decode(bytes, &mut context()).unwrap().value, expected, "{mode}: {schema}");
        }
    }
}

#[test]
fn specific_anonymous_containers_skip_root_class_construction() {
    for schema in [r#"{"type":"array","items":"int"}"#, r#"{"type":"map","values":["null","string"]}"#,
        r#"["null",{"type":"array","items":{"type":"map","values":"long"}}]"#] {
        assert!(plan(schema, &[("specific.avro.reader", "true")]).validate_reader(None).is_ok());
        assert!(plan(schema, &[("schema.reflection", "true")]).validate_reader(None).is_err());
        assert!(plan(schema, &[("specific.avro.reader", "true"), ("schema.reflection", "true")]).validate_reader(None).is_err());
    }
    let array = AvroPlan::parse_version(r#"{"type":"array","items":"int"}"#, &[], &options(&[("specific.avro.reader", "true")]), None).unwrap();
    assert_eq!(array.decode(&[4, 2, 4, 0], &mut context()).unwrap().value, Value::list(vec![Value::I32(1), Value::I32(2)]));
    let map = AvroPlan::parse_version(r#"{"type":"map","values":"long"}"#, &[], &options(&[("specific.avro.reader", "true")]), None).unwrap();
    assert_eq!(map.decode(&[2, 2, b'k', 14, 0], &mut context()).unwrap().value, Value::Map { entries: vec![(Value::Utf8("k".into()), Value::I64(7))], sorted: false });
    let union = AvroPlan::parse_version(r#"["null","int"]"#, &[], &options(&[("specific.avro.reader", "true")]), None).unwrap();
    assert_eq!(union.decode(&[2, 14], &mut context()).unwrap().value, Value::I32(7));
}

#[test]
fn named_schemas_and_java_class_properties_require_an_adapter() {
    for schema in [r#"{"type":"record","name":"Missing","fields":[]}"#, r#"{"type":"enum","name":"Missing","symbols":["A"]}"#,
        r#"{"type":"fixed","name":"Missing","size":1}"#, r#"{"type":"array","items":{"type":"record","name":"Missing","fields":[]}}"#,
        r#"{"type":"map","values":"string","java-key-class":"java.net.URI"}"#,
        r#"["null",{"type":"string","java-class":"java.net.URI"}]"#] {
        for mode in ["specific.avro.reader", "schema.reflection"] {
            let writer = plan(schema, &[(mode, "true")]);
            assert!(writer.validate_reader(None).is_err(), "{mode}: {schema}");
            assert!(writer.validate_reader(Some(&writer)).is_err(), "{mode}: {schema}");
        }
    }
}

#[test]
fn explicit_specific_reader_bypasses_property_primitive_class_lookup() {
    let writer = plan(r#"{"type":"int","custom":"property"}"#, &[("specific.avro.reader", "true")]);
    let reader = plan(r#""long""#, &[("specific.avro.reader", "true")]);
    assert!(writer.validate_reader(None).is_err());
    writer.validate_reader(Some(&reader)).unwrap();
    let mut context = context();
    let resolution = AvroResolution::compile(&writer, &reader, &mut context).unwrap();
    assert_eq!(resolution.decode_datum(&[14], &mut context).unwrap().value, Avro::Long(7));
}

#[test]
fn bare_writer_keeps_generic_reader_with_an_explicit_java_class_annotation() {
    let reader = plan(r#"{"type":"string","java-class":"missing.CustomString"}"#, &[]);
    for mode in ["specific.avro.reader", "schema.reflection"] {
        let writer = plan(r#""string""#, &[(mode, "true")]);
        writer.validate_reader(Some(&reader)).unwrap();
    }
}

#[test]
fn enabled_logical_conversion_does_not_silently_keep_physical_values() {
    let values = [("specific.avro.reader", "true"), ("avro.use.logical.type.converters", "true")];
    let writer = plan(r#""int""#, &values);
    assert!(writer.validate_reader(None).is_ok());
    let reader = plan(r#"{"type":"int","logicalType":"date"}"#, &values);
    assert!(writer.validate_reader(Some(&reader)).is_err());
    assert!(plan(r#"{"type":"array","items":{"type":"int","logicalType":"date"}}"#, &values).validate_reader(None).is_err());
}

#[test]
fn migration_writer_decode_is_generic_and_reconstruction_checks_the_selected_mode() {
    let writer = plan(r#"{"type":"record","name":"Missing","fields":[]}"#, &[("schema.reflection", "true")]);
    assert!(writer.validate_reader(None).is_err());
    assert!(matches!(writer.decode_datum(&[], &mut context()).unwrap().value, Avro::Record(_)));
    assert!(writer.from_json_datum(serde_json::json!({}), &mut context()).is_err());
    let reader = plan(r#""int""#, &[("schema.reflection", "true")]);
    assert_eq!(reader.from_json_datum(serde_json::json!(7), &mut context()).unwrap().value, Avro::Int(7));
}
