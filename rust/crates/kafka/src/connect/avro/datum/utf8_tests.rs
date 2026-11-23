use rtdb_types::StructFields;
use super::*;
use rtdb_columnar::MemoryBudget;
use serde_json::json;

fn plan(text: &str) -> Arc<AvroDatumPlan> { Arc::new(AvroDatumPlan::parse(text, &[], &BTreeMap::new()).unwrap()) }
fn context() -> ConversionContext { ConversionContext::new(MemoryBudget::new(1 << 24)) }
fn long(value: i64, bytes: &mut Vec<u8>) {
    let mut value = ((value << 1) ^ (value >> 63)) as u64;
    while value > 127 { bytes.push((value as u8 & 127) | 128); value >>= 7; }
    bytes.push(value as u8);
}
fn string(value: &[u8]) -> Vec<u8> { let mut bytes = Vec::new(); long(value.len() as i64, &mut bytes); bytes.extend_from_slice(value); bytes }
fn map(entries: &[(&[u8], i64)]) -> Vec<u8> {
    let mut bytes = Vec::new(); long(entries.len() as i64, &mut bytes);
    for (key, value) in entries { bytes.extend(string(key)); long(*value, &mut bytes); }
    bytes.push(0); bytes
}

#[test]
fn binary_strings_replace_malformed_sequences_at_connect_and_json_boundaries() {
    let cases: &[(&[u8], &str)] = &[
        (b"valid\0text", "valid\0text"), ("é😀".as_bytes(), "é😀"),
        (&[0x80, 0xff], "��"), (&[0xc0, 0x80], "��"), (&[0xe0, 0x80, 0x80], "���"),
        (&[0xed, 0xa0, 0x80], "�"), (&[0xed, 0xa0], "�"), (&[0xed, 0xa0, b'x'], "�x"),
        (&[0xe2, 0x82], "�"), (&[0xe2, b'x', 0x82], "�x�"),
        (&[0xf0, 0x90, 0x80], "�"), (&[0xf0, 0x80, 0x80, 0x80], "����"),
        (&[0xf4, 0x90, 0x80, 0x80], "����"),
    ];
    let schema = plan(r#""string""#);
    for (raw, expected) in cases {
        let mut context = context(); let bytes = string(raw);
        let datum = schema.decode_datum(&bytes, &mut context).unwrap();
        assert_eq!(schema.to_json_datum(datum, &mut context).unwrap(), json!(expected));
        let datum = schema.decode_datum(&bytes, &mut context).unwrap();
        assert_eq!(schema.connect_plan(None, None, &mut 0, &mut context).unwrap().convert(datum, &mut 0, &mut context).unwrap().value, Value::Utf8((*expected).into()));
    }
}

#[test]
fn malformed_string_bytes_survive_promotions_in_both_directions() {
    let writer = plan(r#""string""#); let reader = plan(r#""bytes""#); let mut context = context();
    let raw = vec![0xed, 0xa0, 0x80, 0xff];
    let datum = writer.decode_datum(&string(&raw), &mut context).unwrap();
    let datum = AvroResolution::compile(&writer, &reader, &mut context).unwrap().resolve(datum, &mut context).unwrap();
    assert_eq!(datum.value, Avro::Bytes(raw.clone()));
    let datum = AvroResolution::compile(&reader, &writer, &mut context).unwrap().resolve(datum, &mut context).unwrap();
    assert_eq!(datum.value, Avro::Utf8(raw));
    let datum = writer.binary_result(datum, &mut context).unwrap();
    assert_eq!(datum.value, Avro::String("��".into()));
}

#[test]
fn malformed_map_keys_remain_distinct_until_java_string_conversion() {
    let schema = plan(r#"{"type":"map","values":"long"}"#); let mut context = context();
    let bytes = map(&[(&[0x80], 1), (&[0x81], 2)]);
    let datum = schema.decode_datum(&bytes, &mut context).unwrap();
    let Avro::Map(values) = &datum.value else { panic!("expected map") };
    assert_eq!(values.clone().ordered().unwrap(), vec![(MapKey::Utf8(vec![0x81]), Avro::Long(2)), (MapKey::Utf8(vec![0x80]), Avro::Long(1))]);
    assert_eq!(schema.to_json_datum(datum, &mut context).unwrap(), json!({"�":1}));
    let datum = schema.decode_datum(&bytes, &mut context).unwrap();
    let output = schema.connect_plan(None, None, &mut 0, &mut context).unwrap().convert(datum, &mut 0, &mut context).unwrap();
    assert_eq!(output.value, Value::Map { entries: vec![(Value::Utf8("�".into()), Value::I64(1))], sorted: false });
}

#[test]
fn reader_key_class_controls_duplicate_replacement_without_losing_writer_bytes() {
    let utf8 = plan(r#"{"type":"map","values":"long"}"#);
    let strings = plan(r#"{"type":"map","values":"long","avro.java.string":"String"}"#);
    let mut context = context(); let bytes = map(&[(&[0x80], 1), (&[0x81], 2)]);
    for (writer, reader, expected) in [(&utf8, &strings, 2), (&strings, &utf8, 1)] {
        let datum = writer.decode_datum(&bytes, &mut context).unwrap();
        let datum = AvroResolution::compile(writer, reader, &mut context).unwrap().resolve(datum, &mut context).unwrap();
        assert_eq!(reader.to_json_datum(datum, &mut context).unwrap(), json!({"�":expected}));
    }
    let datum = strings.decode_datum(&bytes, &mut context).unwrap();
    assert_eq!(strings.to_json_datum(datum, &mut context).unwrap(), json!({"�":2}));
}

#[test]
fn repeated_raw_keys_replace_values_without_collapsing_distinct_invalid_bytes() {
    let schema = plan(r#"{"type":"map","values":"long"}"#); let mut context = context();
    let datum = schema.decode_datum(&map(&[(&[0x80], 1), (&[0x81], 2), (&[0x80], 3)]), &mut context).unwrap();
    let Avro::Map(values) = datum.value else { panic!("expected map") };
    assert_eq!(values.ordered().unwrap(), vec![(MapKey::Utf8(vec![0x81]), Avro::Long(2)), (MapKey::Utf8(vec![0x80]), Avro::Long(3))]);
}

#[test]
fn connect_reader_bridge_only_converts_the_surviving_duplicate_value() {
    let writer = plan(r#"{"type":"map","values":["int","string"]}"#);
    let reader = plan(r#"{"type":"map","values":["long","string"]}"#);
    let mut context = context();
    let datum = writer.decode_datum(&[4, 2, 0x80, 0, 2, 2, 0x80, 2, 2, b'x', 0], &mut context).unwrap();
    let datum = AvroResolution::compile(&writer, &reader, &mut context).unwrap().resolve(datum, &mut context).unwrap();
    let output = writer.connect_plan(None, None, &mut 0, &mut context).unwrap().convert(datum, &mut 0, &mut context).unwrap();
    assert_eq!(output.value, Value::Map { entries: vec![(Value::Utf8("�".into()), Value::Struct(StructFields::from([("int".into(), Value::Null), ("string".into(), Value::Utf8("x".into()))])))], sorted: false });
}

#[test]
fn reader_resolution_still_visits_an_overwritten_incompatible_union_branch() {
    let writer = plan(r#"{"type":"map","values":["int","string"]}"#);
    let reader = plan(r#"{"type":"map","values":"string"}"#);
    let mut context = context();
    let datum = writer.decode_datum(&[4, 2, 0x80, 0, 2, 2, 0x80, 2, 2, b'x', 0], &mut context).unwrap();
    assert!(AvroResolution::compile(&writer, &reader, &mut context).unwrap().resolve(datum, &mut context).is_err());
}

#[test]
fn root_map_inference_visits_values_under_distinct_malformed_keys() {
    let schema = plan(r#"{"type":"map","values":["string","int"]}"#); let mut context = context();
    let bytes = [4, 2, 0x80, 0, 2, b'x', 2, 0x81, 2, 4, 0];
    let datum = schema.decode_datum(&bytes, &mut context).unwrap();
    assert!(schema.to_json_datum(datum, &mut context).is_err());
}

#[test]
fn nested_record_array_union_maps_keep_raw_key_identity() {
    let schema = plan(r#"{"type":"record","name":"R","fields":[{"name":"maps","type":{"type":"array","items":["null",{"type":"map","values":"long"}]}}]}"#);
    let mut bytes = vec![2, 2]; bytes.extend(map(&[(&[0x80], 1), (&[0x81], 2)])); bytes.push(0);
    let mut context = context(); let datum = schema.decode_datum(&bytes, &mut context).unwrap();
    assert_eq!(schema.to_json_datum(datum, &mut context).unwrap(), json!({"maps":[{"map":{"�":1}}]}));
    let datum = schema.decode_datum(&bytes, &mut context).unwrap();
    let output = schema.connect_plan(None, None, &mut 0, &mut context).unwrap().convert(datum, &mut 0, &mut context).unwrap();
    assert_eq!(output.value, Value::Struct(StructFields::from([("maps".into(), Value::list(vec![Value::Map { entries: vec![(Value::Utf8("�".into()), Value::I64(1))], sorted: false }]))])));
}

#[test]
fn malformed_key_blocks_keep_initial_capacity_and_reject_truncation() {
    let schema = plan(r#"{"type":"map","values":"long"}"#); let mut context = context();
    let bytes = [1, 6, 2, 0x80, 2, 2, 2, 0x81, 4, 0];
    let datum = schema.decode_datum(&bytes, &mut context).unwrap();
    assert_eq!(schema.to_json_datum(datum, &mut context).unwrap(), json!({"�":1}));
    for truncated in [&bytes[..3], &bytes[..9]] { assert!(schema.decode_datum(truncated, &mut context).is_err()); }
    let datum = schema.decode_datum(&[1, 8, 2, 0x80, 2, 0], &mut context).unwrap();
    assert_eq!(schema.to_json_datum(datum, &mut context).unwrap(), json!({"�":1}));
}

#[test]
fn raw_utf8_and_map_expansion_are_admitted_before_decoding_and_released_with_context() {
    let schema = plan(r#"{"type":"map","values":"long"}"#);
    let budget = MemoryBudget::new(1 << 20); let mut context = ConversionContext::new(Arc::clone(&budget));
    let bytes = map(&[(&[0x80; 8192], 1), (&[0x81; 8192], 2)]);
    let datum = schema.decode_datum(&bytes, &mut context).unwrap();
    assert!(budget.used_bytes() >= bytes.len());
    let json = schema.to_json_datum(datum, &mut context).unwrap();
    assert!(budget.used_bytes() >= json.to_string().len());
    drop(json); drop(context); assert_eq!(budget.used_bytes(), 0);
    let mut tiny = ConversionContext::new(MemoryBudget::new(4096));
    assert!(schema.decode_datum(&bytes, &mut tiny).is_err());
}

#[test]
fn physical_decoder_preserves_java_boolean_and_varint_width_rules() {
    let mut context = context();
    let boolean = plan(r#""boolean""#);
    for (byte, expected) in [(0, false), (1, true), (2, false), (255, false)] {
        assert_eq!(boolean.decode_datum(&[byte], &mut context).unwrap().value, Avro::Boolean(expected));
    }
    let int = plan(r#""int""#);
    assert_eq!(int.decode_datum(&[0x80, 0x80, 0x80, 0x80, 0x10], &mut context).unwrap().value, Avro::Int(0));
    assert!(int.decode_datum(&[0x80, 0x80, 0x80, 0x80, 0x80, 0], &mut context).is_err());
    let long = plan(r#""long""#);
    assert_eq!(long.decode_datum(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 2], &mut context).unwrap().value, Avro::Long(0));
    assert!(long.decode_datum(&[0x80; 10], &mut context).is_err());
}

#[test]
fn root_string_replacement_reserves_its_expansion_before_allocating() {
    let schema = plan(r#""string""#); let budget = MemoryBudget::new(1 << 20);
    let mut context = ConversionContext::with_limit(Arc::clone(&budget), 50_000);
    let datum = schema.decode_datum(&string(&[0x80; 8192]), &mut context).unwrap();
    let admitted = budget.used_bytes();
    assert!(schema.binary_result(datum, &mut context).is_err());
    assert_eq!(budget.used_bytes(), admitted);
    drop(context); assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn reader_map_defaults_preserve_json_member_order_and_duplicate_replacement() {
    let writer = plan(r#"{"type":"record","name":"R","fields":[]}"#);
    let reader = plan(r#"{"type":"record","name":"R","fields":[{"name":"m","type":{"type":"map","values":"int"},"default":{"BB":2,"Aa":1,"BB":3}}]}"#);
    let mut context = context();
    let datum = writer.decode_datum(&[], &mut context).unwrap();
    let datum = AvroResolution::compile(&writer, &reader, &mut context).unwrap().resolve(datum, &mut context).unwrap();
    let Avro::Record(mut fields) = datum.value else { panic!("expected record") };
    let Avro::Map(values) = fields.remove(0).1 else { panic!("expected map") };
    assert_eq!(values.ordered().unwrap(), vec![(MapKey::Utf8(b"BB".to_vec()), Avro::Int(3)), (MapKey::Utf8(b"Aa".to_vec()), Avro::Int(1))]);
}
