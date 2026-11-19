use rtdb_types::StructFields;
use super::*;
use std::sync::Arc;
use rtdb_columnar::MemoryBudget;
use rtdb_types::Value;
use serde_json::json;
fn plan(text: &str) -> Arc<AvroDatumPlan> { Arc::new(AvroDatumPlan::parse(text, &[], &BTreeMap::new()).unwrap()) }
fn context() -> ConversionContext { ConversionContext::new(MemoryBudget::new(1 << 28)) }

#[test]
fn record_json_keeps_nested_union_tags_and_drops_the_root_union_tag() {
    let plan = plan(r#"["null",{"type":"record","name":"ns.Row","fields":[{"name":"child","type":["null",{"type":"record","name":"Child","fields":[{"name":"n","type":"int"}]}]}]}]"#);
    let mut context = context(); let datum = plan.decode_datum(&[2, 2, 14], &mut context).unwrap();
    assert_eq!(plan.to_json_datum(datum, &mut context).unwrap(), json!({"child":{"ns.Child":{"n":7}}}));
}

#[test]
fn migration_json_does_not_apply_reader_defaults_or_ignore_buffered_fields() {
    let reader = plan(r#"{"type":"record","name":"R","fields":[{"name":"n","type":"int","default":3}]}"#);
    let mut context = context();
    assert!(reader.from_json_datum(json!({}), &mut context).is_err());
    assert!(reader.from_json_datum(json!({"unknown":0,"n":1}), &mut context).is_err());
    assert_eq!(reader.from_json_datum(json!({"n":1,"unknown":0}), &mut context).unwrap().value, Avro::Record(vec![("n".into(), Avro::Int(1))]));
}

#[test]
fn json_field_aliases_match_the_stream_but_not_saved_alias_keys() {
    let reader = plan(r#"{"type":"record","name":"R","fields":[{"name":"b","type":"int"},{"name":"a","aliases":["old"],"type":"int"}]}"#);
    let mut context = context();
    assert!(reader.from_json_datum(json!({"old":1,"b":2}), &mut context).is_err());
    assert_eq!(reader.from_json_datum(json!({"b":2,"old":1}), &mut context).unwrap().value, Avro::Record(vec![("b".into(), Avro::Int(2)), ("a".into(), Avro::Int(1))]));
}

#[test]
fn migration_json_bytes_use_iso_8859_1_and_fixed_lengths_remain_strict() {
    let writer = plan(r#"{"type":"record","name":"R","fields":[{"name":"b","type":"bytes"}]}"#);
    let mut context = context(); let datum = writer.decode_datum(&[6, 0, 128, 255], &mut context).unwrap();
    let json = writer.to_json_datum(datum, &mut context).unwrap(); assert_eq!(json, json!({"b":"\u{0}\u{80}ÿ"}));
    assert_eq!(writer.from_json_datum(json, &mut context).unwrap().value, Avro::Record(vec![("b".into(), Avro::Bytes(vec![0, 128, 255]))]));
    let fixed = plan(r#"{"type":"fixed","name":"F","size":3}"#);
    assert!(fixed.from_json_datum(json!("a"), &mut context).is_err());
    assert_eq!(fixed.from_json_datum(json!("a☃😀"), &mut context).unwrap().value, Avro::Fixed(3, vec![97, 63, 63]));
}

#[test]
fn migration_json_numeric_reader_uses_checked_jackson_narrowing() {
    let int = plan(r#""int""#); let mut context = context();
    assert_eq!(int.from_json_datum(json!(3.9), &mut context).unwrap().value, Avro::Int(3));
    assert!(int.from_json_datum(json!(2147483648i64), &mut context).is_err());
    let float = plan(r#""float""#);
    let datum = float.decode_datum(&f32::NAN.to_le_bytes(), &mut context).unwrap();
    let json = float.to_json_datum(datum, &mut context).unwrap(); assert_eq!(json, json!("NaN"));
    assert!(float.from_json_datum(json, &mut context).is_err());
}

#[test]
fn root_map_infers_from_java_bucket_order_and_coerces_other_numeric_values() {
    let writer = plan(r#"{"type":"map","values":["int","double"]}"#); let mut context = context();
    let mut bytes = vec![4, 2, b'b', 2]; bytes.extend_from_slice(&9.75f64.to_le_bytes()); bytes.extend_from_slice(&[2, b'a', 0, 14, 0]);
    let datum = writer.decode_datum(&bytes, &mut context).unwrap();
    let json = writer.to_json_datum(datum, &mut context).unwrap(); assert_eq!(json, json!({"a":7,"b":9}));
    assert!(writer.from_json_datum(json.clone(), &mut context).is_err());
    let reader = plan(r#"{"type":"map","values":"long"}"#);
    let Avro::Map(values) = reader.from_json_datum(json, &mut context).unwrap().value else { panic!("expected map") };
    let values = values.ordered().unwrap().into_iter().map(|(key, value)| (key.into_text(), value)).collect::<BTreeMap<_, _>>();
    assert_eq!(values, BTreeMap::from([("a".into(), Avro::Long(7)), ("b".into(), Avro::Long(9))]));
}

#[test]
fn root_map_duplicates_replace_values_without_reordering_the_key() {
    let writer = plan(r#"{"type":"map","values":["int","double"]}"#); let mut context = context();
    let mut bytes = vec![6, 2, b'a', 0, 14, 2, b'b', 0, 18, 2, b'a', 2]; bytes.extend_from_slice(&3.5f64.to_le_bytes()); bytes.push(0);
    let datum = writer.decode_datum(&bytes, &mut context).unwrap();
    assert_eq!(writer.to_json_datum(datum, &mut context).unwrap(), json!({"a":3.5,"b":9.0}));
}

#[test]
fn nested_inferred_maps_retain_their_own_insertion_provenance() {
    let writer = plan(r#"{"type":"map","values":{"type":"map","values":["int","double"]}}"#);
    let mut context = context();
    let mut bytes = vec![2, 2, b'x', 4, 2, b'b', 2]; bytes.extend_from_slice(&2.5f64.to_le_bytes()); bytes.extend_from_slice(&[2, b'a', 0, 2, 0, 0]);
    let datum = writer.decode_datum(&bytes, &mut context).unwrap();
    assert_eq!(writer.to_json_datum(datum, &mut context).unwrap(), json!({"x":{"a":1,"b":2}}));
}

#[test]
fn reader_union_reordering_does_not_relabel_writer_connect_union_values() {
    let writer = plan(r#"["int","long"]"#); let reader = plan(r#"["long","int"]"#); let mut context = context();
    let datum = writer.decode_datum(&[0, 14], &mut context).unwrap();
    let datum = super::super::AvroResolution::compile(&writer, &reader, &mut context).unwrap().resolve(datum, &mut context).unwrap();
    let output = writer.connect_plan(None, None, &mut 0, &mut context).unwrap();
    assert_eq!(output.convert(datum, &mut 0, &mut context).unwrap().value, Value::Struct(StructFields::from([("int".into(), Value::I32(7)), ("long".into(), Value::Null)])));
}

#[test]
fn migration_string_to_bytes_has_no_second_binary_string_coercion() {
    let writer = plan(r#""string""#); let reader = plan(r#""bytes""#); let mut context = context();
    let datum = writer.decode_datum(&[2, b'x'], &mut context).unwrap(); let datum = writer.binary_result(datum, &mut context).unwrap();
    let json = writer.to_json_datum(datum, &mut context).unwrap(); let datum = reader.from_json_datum(json, &mut context).unwrap();
    let (_, datum) = writer.prepare_output(&reader, datum, &mut context).unwrap();
    assert!(writer.connect_plan(None, None, &mut 0, &mut context).unwrap().convert(datum, &mut 0, &mut context).is_err());
}

#[test]
fn graph_and_datum_expansion_charges_survive_cache_hits() {
    let memory = MemoryBudget::new(1 << 20); let schema = plan(r#""int""#);
    let mut first = ConversionContext::new(Arc::clone(&memory)); let output = schema.connect_plan(None, None, &mut 0, &mut first).unwrap();
    drop(first); assert!(memory.used_bytes() > 0);
    let mut next = ConversionContext::new(Arc::clone(&memory)); let datum = schema.decode_datum(&[2], &mut next).unwrap();
    assert_eq!(output.convert(datum, &mut 0, &mut next).unwrap().value, Value::I32(1));
    drop(output); assert!(memory.used_bytes() > 0); drop(next); assert_eq!(memory.used_bytes(), 0);
}
