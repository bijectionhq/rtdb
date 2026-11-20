use rtdb_types::StructFields;
use super::*;
use super::super::{AvroOutput, SchemaKind};
use rtdb_columnar::MemoryBudget;
use rtdb_types::Value;

fn plan(text: &str) -> Arc<AvroDatumPlan> { Arc::new(AvroDatumPlan::parse(text, &[], &BTreeMap::new()).unwrap()) }
fn context() -> ConversionContext { ConversionContext::new(MemoryBudget::new(1 << 28)) }
fn resolved(writer: &str, reader: &str, datum: Avro) -> Result<Avro> {
    let writer = plan(writer); let reader = plan(reader); let mut context = context();
    Ok(AvroResolution::compile(&writer, &reader, &mut context)?.resolve(AvroDatum::untracked(Arc::clone(&writer), datum), &mut context)?.value)
}
fn record(fields: &[(&str, Avro)]) -> Avro { Avro::Record(fields.iter().map(|(name, value)| ((*name).into(), value.clone())).collect()) }

#[test]
fn reader_aliases_project_reorder_and_supply_defaults() {
    let writer = r#"{"type":"record","name":"old.Row","fields":[{"name":"old_name","type":"int"},{"name":"discarded","type":"string"}]}"#;
    let reader = r#"{"type":"record","name":"new.Row","aliases":["old.Row"],"fields":[{"name":"added","type":"string","default":"default"},{"name":"name","aliases":["old_name"],"type":"long"}]}"#;
    assert_eq!(resolved(writer, reader, record(&[("old_name", Avro::Int(42)), ("discarded", Avro::String("skip".into()))])).unwrap(), record(&[("added", Avro::String("default".into())), ("name", Avro::Long(42))]));
}

#[test]
fn direct_records_ignore_names_but_fixed_and_enum_compare_short_names() {
    assert_eq!(resolved(r#"{"type":"record","name":"A","fields":[]}"#, r#"{"type":"record","name":"B","fields":[]}"#, record(&[])).unwrap(), record(&[]));
    assert_eq!(resolved(r#"{"type":"fixed","name":"a.F","size":2}"#, r#"{"type":"fixed","name":"b.F","size":2}"#, Avro::Fixed(2, vec![1, 2])).unwrap(), Avro::Fixed(2, vec![1, 2]));
    assert!(resolved(r#"{"type":"fixed","name":"a.F","size":2}"#, r#"{"type":"fixed","name":"b.G","size":2}"#, Avro::Fixed(2, vec![1, 2])).is_err());
}

#[test]
fn alias_collisions_fail_before_a_writer_union_branch_is_selected() {
    let writer = plan(r#"["int",{"type":"record","name":"Old","fields":[{"name":"a","type":"int"},{"name":"b","type":"int"}]}]"#);
    let reader = plan(r#"["int",{"type":"record","name":"New","aliases":["Old"],"fields":[{"name":"a","aliases":["b"],"type":"int"}]}]"#);
    assert!(AvroResolution::compile(&writer, &reader, &mut context()).is_err());
}

#[test]
fn reader_union_prefers_exact_type_before_numeric_promotion() {
    assert_eq!(resolved(r#""int""#, r#"["long","int"]"#, Avro::Int(7)).unwrap(), Avro::Union(1, Box::new(Avro::Int(7))));
    assert_eq!(resolved(r#""int""#, r#"["double","long"]"#, Avro::Int(7)).unwrap(), Avro::Union(0, Box::new(Avro::Double(7.0))));
}

#[test]
fn reader_record_union_keeps_the_last_soft_match_with_the_same_short_name() {
    let writer = r#"{"type":"record","name":"w.R","fields":[{"name":"x","type":"int"}]}"#;
    let reader = r#"[{"type":"record","name":"a.R","fields":[{"name":"x","type":"long"}]},{"type":"record","name":"b.R","fields":[{"name":"x","type":"long"}]}]"#;
    assert_eq!(resolved(writer, reader, record(&[("x", Avro::Int(1))])).unwrap(), Avro::Union(1, Box::new(record(&[("x", Avro::Long(1))]))));
}

#[test]
fn structural_union_selection_checks_immediate_field_errors_only() {
    let writer = r#"{"type":"record","name":"W","fields":[{"name":"x","type":{"type":"array","items":"int"}}]}"#;
    let reader = r#"[{"type":"record","name":"A","fields":[{"name":"x","type":{"type":"array","items":"string"}}]},{"type":"record","name":"B","fields":[{"name":"x","type":{"type":"array","items":"long"}}]}]"#;
    assert_eq!(resolved(writer, reader, record(&[("x", Avro::Array(vec![]))])).unwrap(), Avro::Union(0, Box::new(record(&[("x", Avro::Array(vec![]))]))));
    assert!(resolved(writer, reader, record(&[("x", Avro::Array(vec![Avro::Int(1)]))])).is_err());
}

#[test]
fn incompatible_writer_union_members_fail_only_when_read() {
    assert_eq!(resolved(r#"["int","string"]"#, r#""long""#, Avro::Union(0, Box::new(Avro::Int(3)))).unwrap(), Avro::Long(3));
    assert!(resolved(r#"["int","string"]"#, r#""long""#, Avro::Union(1, Box::new(Avro::String("x".into())))).is_err());
}

#[test]
fn equivalent_union_preserves_branch_index_across_namespace_changes() {
    let writer = r#"[{"type":"record","name":"w.R","fields":[{"name":"x","type":"int"}]},"null"]"#;
    let reader = r#"[{"type":"record","name":"r.R","fields":[{"name":"x","type":"int"}]},"null"]"#;
    let value = Avro::Union(0, Box::new(record(&[("x", Avro::Int(4))])));
    assert_eq!(resolved(writer, reader, value.clone()).unwrap(), value);
}

#[test]
fn enum_missing_symbols_use_default_or_fail_on_that_symbol() {
    let writer = r#"{"type":"enum","name":"w.E","symbols":["A","B"]}"#;
    let reader = r#"{"type":"enum","name":"r.E","symbols":["A","X"],"default":"X"}"#;
    assert_eq!(resolved(writer, reader, Avro::Enum(1, "B".into())).unwrap(), Avro::Enum(1, "X".into()));
    let reader = r#"{"type":"enum","name":"r.E","symbols":["A"]}"#;
    assert_eq!(resolved(writer, reader, Avro::Enum(0, "A".into())).unwrap(), Avro::Enum(0, "A".into()));
    assert!(resolved(writer, reader, Avro::Enum(1, "B".into())).is_err());
}

#[test]
fn selected_defaults_use_first_union_branch_and_fixed_padding() {
    let writer = r#"{"type":"record","name":"R","fields":[]}"#;
    let reader = r#"{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"fixed","name":"F","size":3},"default":"a"},{"name":"u","type":["long","null"],"default":5}]}"#;
    assert_eq!(resolved(writer, reader, record(&[])).unwrap(), record(&[("f", Avro::Fixed(3, vec![97, 0, 0])), ("u", Avro::Union(0, Box::new(Avro::Long(5))))]));
    let reader = plan(r#"{"type":"record","name":"R","fields":[{"name":"u","type":["null","long"],"default":5}]}"#);
    assert!(AvroResolution::compile(&plan(writer), &reader, &mut context()).is_err());
}

#[test]
fn default_integer_spelling_keeps_low_bits_beyond_unsigned_long_range() {
    let writer = r#"{"type":"record","name":"R","fields":[]}"#;
    let reader = r#"{"type":"record","name":"R","fields":[{"name":"i","type":"int","default":18446744073709551617},{"name":"l","type":"long","default":-18446744073709551617}]}"#;
    assert_eq!(resolved(writer, reader, record(&[])).unwrap(), record(&[("i", Avro::Int(1)), ("l", Avro::Long(-1))]));
    let reader = plan(reader); let mut context = context();
    let output = reader.connect_plan(None, None, &mut 0, &mut context).unwrap();
    assert_eq!(output.connect.field("i").unwrap().schema.default, None);
    assert_eq!(output.connect.field("l").unwrap().schema.default, None);
}

#[test]
fn floating_default_exponents_survive_the_schema_metadata_parser() {
    let writer = r#"{"type":"record","name":"R","fields":[]}"#;
    let reader = r#"{"type":"record","name":"R","fields":[{"name":"f","type":"double","default":1e400}]}"#;
    assert_eq!(resolved(writer, reader, record(&[])).unwrap(), record(&[("f", Avro::Double(f64::INFINITY))]));
    let reader = plan(reader); let mut context = context();
    let output = reader.connect_plan(None, None, &mut 0, &mut context).unwrap();
    assert_eq!(output.connect.field("f").unwrap().schema.default, Some(Value::F64(f64::INFINITY)));
}

#[test]
fn recursive_schema_pairs_reuse_actions_and_reorder_nested_records() {
    let writer = r#"{"type":"record","name":"Node","fields":[{"name":"x","type":"int"},{"name":"next","type":["null","Node"]}]}"#;
    let reader = r#"{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]},{"name":"x","type":"long"}]}"#;
    let tail = record(&[("x", Avro::Int(2)), ("next", Avro::Union(0, Box::new(Avro::Null)))]);
    let head = record(&[("x", Avro::Int(1)), ("next", Avro::Union(1, Box::new(tail)))]);
    let tail = record(&[("next", Avro::Union(0, Box::new(Avro::Null))), ("x", Avro::Long(2))]);
    assert_eq!(resolved(writer, reader, head).unwrap(), record(&[("next", Avro::Union(1, Box::new(tail))), ("x", Avro::Long(1))]));
}

#[test]
fn logical_metadata_is_applied_after_physical_reader_resolution() {
    let writer = plan(r#"{"type":"record","name":"R","fields":[{"name":"x","type":{"type":"int","logicalType":"date"}}]}"#);
    let reader = plan(r#"{"type":"record","name":"R","fields":[{"name":"x","type":"long"}]}"#);
    let mut context = context();
    let raw = writer.decode_datum(&[2], &mut context).unwrap(); assert_eq!(raw.value, record(&[("x", Avro::Int(1))]));
    let value = AvroResolution::compile(&writer, &reader, &mut context).unwrap().resolve(raw, &mut context).unwrap();
    let output = reader.connect_plan(None, Some(4), &mut 0, &mut context).unwrap();
    assert_eq!(output.convert(value, &mut 0, &mut context).unwrap().value, Value::Struct(StructFields::from([("x".into(), Value::I64(1))])));
}

#[test]
fn writer_fields_with_no_connect_mapping_can_be_projected_away() {
    let writer = plan(r#"{"type":"record","name":"R","fields":[{"name":"ignored","type":"null"},{"name":"x","type":"int"}]}"#);
    let reader = plan(r#"{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}"#);
    let mut context = context(); let value = writer.decode_datum(&[2], &mut context).unwrap();
    let value = AvroResolution::compile(&writer, &reader, &mut context).unwrap().resolve(value, &mut context).unwrap();
    assert_eq!(reader.connect_plan(None, None, &mut 0, &mut context).unwrap().convert(value, &mut 0, &mut context).unwrap().value, Value::Struct(StructFields::from([("x".into(), Value::I32(1))])));
}

#[test]
fn record_reader_union_output_uses_the_selected_record_schema_and_writer_version() {
    let writer = plan(r#"{"type":"record","name":"R","fields":[]}"#);
    let reader = plan(r#"["null",{"type":"record","name":"R","fields":[]}]"#);
    let mut context = context(); let value = AvroResolution::compile(&writer, &reader, &mut context).unwrap().resolve(AvroDatum::untracked(Arc::clone(&writer), record(&[])), &mut context).unwrap();
    let (selection, value) = writer.prepare_output(&reader, value, &mut context).unwrap();
    assert_eq!(selection, AvroOutput::Reader { branch: Some(1) });
    let output = reader.connect_plan(Some(1), Some(8), &mut 0, &mut context).unwrap().convert(value, &mut 0, &mut context).unwrap();
    assert_eq!(output.schema.as_ref().unwrap().version, Some(8));
    assert!(matches!(output.schema.unwrap().kind, SchemaKind::Struct(_)));
}

#[test]
fn nonrecord_promotions_still_use_writer_connect_type() {
    let writer = plan(r#""int""#); let reader = plan(r#""long""#); let mut context = context();
    let value = AvroResolution::compile(&writer, &reader, &mut context).unwrap().resolve(AvroDatum::untracked(Arc::clone(&writer), Avro::Int(1)), &mut context).unwrap();
    let (selection, value) = writer.prepare_output(&reader, value, &mut context).unwrap(); assert_eq!(selection, AvroOutput::Writer);
    assert!(writer.connect_plan(None, None, &mut 0, &mut context).unwrap().convert(value, &mut 0, &mut context).is_err());
}

#[test]
fn string_to_bytes_binary_result_uses_java_byte_buffer_description() {
    let writer = plan(r#""string""#); let reader = plan(r#""bytes""#); let mut context = context();
    let value = AvroResolution::compile(&writer, &reader, &mut context).unwrap().resolve(AvroDatum::untracked(Arc::clone(&writer), Avro::String("é".into())), &mut context).unwrap();
    assert_eq!(writer.binary_result(value, &mut context).unwrap().value, Avro::String("java.nio.HeapByteBuffer[pos=0 lim=2 cap=2]".into()));
}

#[test]
fn bytes_to_string_uses_java_malformed_utf8_grouping() {
    for (input, expected) in [(vec![0xed, 0xa0, 0x80], "�"), (vec![0xed, 0xa0], "�"), (vec![0xed, 0xa0, b'x'], "�x"), (vec![0xc0, 0x80], "��")] {
        let resolved = resolved(r#""bytes""#, r#""string""#, Avro::Bytes(input)).unwrap();
        let Avro::Utf8(bytes) = resolved else { panic!("expected raw Utf8") };
        assert_eq!(super::super::datum::java_utf8(&bytes), expected);
    }
}

#[test]
fn cached_resolution_retains_its_charge_and_admits_output_before_allocation() {
    let memory = MemoryBudget::new(1 << 20); let mut first = ConversionContext::new(Arc::clone(&memory));
    let writer = plan(r#""string""#); let resolution = AvroResolution::compile(&writer, &plan(r#""bytes""#), &mut first).unwrap();
    drop(first); assert!(memory.used_bytes() > 0);
    let mut next = ConversionContext::new(Arc::clone(&memory));
    assert_eq!(resolution.resolve(AvroDatum::untracked(writer, Avro::String("hello".into())), &mut next).unwrap().value, Avro::Bytes(b"hello".to_vec()));
    drop(resolution); assert!(memory.used_bytes() > 0); drop(next); assert_eq!(memory.used_bytes(), 0);
    let mut tiny = ConversionContext::new(MemoryBudget::new(1));
    assert!(AvroResolution::compile(&plan(r#""int""#), &plan(r#""long""#), &mut tiny).is_err());
}
