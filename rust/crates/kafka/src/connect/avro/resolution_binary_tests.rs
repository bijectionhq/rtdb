use super::*;
use rtdb_columnar::MemoryBudget;

fn plan(text: &str) -> Arc<AvroDatumPlan> { Arc::new(AvroDatumPlan::parse(text, &[], &BTreeMap::new()).unwrap()) }
fn context() -> ConversionContext { ConversionContext::new(MemoryBudget::new(1 << 24)) }
fn projected(field: &str, bytes: &[u8]) -> Result<Avro> {
    let writer = plan(&format!(r#"{{"type":"record","name":"R","fields":[{{"name":"ignored","type":{field}}},{{"name":"keep","type":"int"}}]}}"#));
    let reader = plan(r#"{"type":"record","name":"R","fields":[{"name":"keep","type":"long"}]}"#);
    let mut context = context();
    Ok(AvroResolution::compile(&writer, &reader, &mut context)?.decode_datum(bytes, &mut context)?.value)
}
fn kept() -> Avro { Avro::Record(vec![("keep".into(), Avro::Long(7))]) }

#[test]
fn dropped_enum_indices_are_skipped_and_selected_indices_are_validated() {
    let enumeration = r#"{"type":"enum","name":"E","symbols":["A"]}"#;
    for bytes in [[2, 14], [1, 14]] { assert_eq!(projected(enumeration, &bytes).unwrap(), kept()); }
    let schema = plan(enumeration); let mut context = context();
    let resolution = AvroResolution::compile(&schema, &schema, &mut context).unwrap();
    assert!(schema.decode_datum(&[2], &mut context).is_err());
    assert!(resolution.decode_datum(&[2], &mut context).is_err());
    assert_eq!(resolution.decode_datum(&[0], &mut context).unwrap().value, Avro::Enum(0, "A".into()));
}

#[test]
fn binary_datum_readers_ignore_suffixes_after_one_value() {
    let schema = plan(r#""string""#); let mut context = context();
    assert_eq!(schema.decode_datum(&[2, b'x', 255], &mut context).unwrap().value, Avro::String("x".into()));
    let resolution = AvroResolution::compile(&schema, &plan(r#""bytes""#), &mut context).unwrap();
    assert_eq!(resolution.decode_datum(&[2, b'x', 255], &mut context).unwrap().value, Avro::Bytes(vec![b'x']));
    assert_eq!(projected(r#"{"type":"enum","name":"E","symbols":["A"]}"#, &[2, 14, 255]).unwrap(), kept());
    assert!(schema.decode_datum(&[4, b'x'], &mut context).is_err());
    assert!(resolution.decode_datum(&[4, b'x'], &mut context).is_err());
    let raw = plan(r#""bytes""#);
    assert_eq!(raw.decode_datum(&[2, b'x', 255], &mut context).unwrap().value, Avro::Bytes(vec![2, b'x', 255]));
}

#[test]
fn projected_collections_skip_negative_blocks_and_walk_positive_blocks() {
    let array = r#"{"type":"array","items":{"type":"enum","name":"E","symbols":["A"]}}"#;
    for bytes in [&[1, 2, 2, 0, 14][..], &[2, 2, 0, 14], &[1, 0, 0, 14]] { assert_eq!(projected(array, bytes).unwrap(), kept()); }
    let map = r#"{"type":"map","values":{"type":"enum","name":"E","symbols":["A"]}}"#;
    for bytes in [&[1, 2, 255, 0, 14][..], &[2, 2, b'k', 2, 0, 14]] { assert_eq!(projected(map, bytes).unwrap(), kept()); }
    assert!(projected(array, &[1, 8, 2, 0, 14]).is_err());
    assert!(projected(map, &[2, 4, b'k']).is_err());
    let selected = plan(array); let mut context = context();
    assert!(AvroResolution::compile(&selected, &selected, &mut context).unwrap().decode_datum(&[1, 2, 2, 0], &mut context).is_err());
}

#[test]
fn projected_fixed_and_union_values_still_consume_their_wire_shapes() {
    let fixed = r#"{"type":"fixed","name":"F","size":2}"#;
    assert_eq!(projected(fixed, &[7, 8, 14]).unwrap(), kept());
    assert!(projected(fixed, &[7]).is_err());
    assert!(projected(fixed, &[7, 8]).is_err());
    let union = r#"[{"type":"enum","name":"E","symbols":["A"]},"null"]"#;
    assert_eq!(projected(union, &[0, 2, 14]).unwrap(), kept());
    assert!(projected(union, &[4, 14]).is_err());
    assert!(projected(r#""bytes""#, &[4, 7]).is_err());
}

#[test]
fn selected_collection_blocks_discard_the_declared_byte_length() {
    let schema = plan(r#"{"type":"array","items":"int"}"#); let mut context = context();
    let resolution = AvroResolution::compile(&schema, &schema, &mut context).unwrap();
    for bytes in [[1, 0, 2, 0], [1, 8, 2, 0], [1, 1, 2, 0]] {
        for datum in [schema.decode_datum(&bytes, &mut context).unwrap(), resolution.decode_datum(&bytes, &mut context).unwrap()] {
            let Avro::PackedArray(packed) = datum.value else { panic!("expected packed array"); };
            assert_eq!(packed.values.array().unwrap().data_type(), &arrow_schema::DataType::Int32);
            assert_eq!(packed.into_physical_values(), vec![Avro::Int(1)]);
        }
    }
    assert!(resolution.decode_datum(&[1, 0], &mut context).is_err());
}

#[test]
fn selected_nested_records_keep_aliases_defaults_and_reader_union_indices() {
    let writer = plan(r#"{"type":"array","items":{"type":"record","name":"Old","fields":[{"name":"ignored","type":{"type":"enum","name":"E","symbols":["A"]}},{"name":"old","type":"int"}]}}"#);
    let reader = plan(r#"{"type":"array","items":["null",{"type":"record","name":"New","aliases":["Old"],"fields":[{"name":"added","type":"string","default":"x"},{"name":"value","aliases":["old"],"type":"long"}]}]}"#);
    let mut context = context();
    let value = AvroResolution::compile(&writer, &reader, &mut context).unwrap().decode_datum(&[2, 2, 14, 0], &mut context).unwrap().value;
    assert_eq!(value, Avro::Array(vec![Avro::Union(1, Box::new(Avro::Record(vec![("added".into(), Avro::String("x".into())), ("value".into(), Avro::Long(7))])))]));
}

#[test]
fn binary_reader_resolves_overwritten_map_values_before_replacement() {
    let writer = plan(r#"{"type":"map","values":["int","string"]}"#);
    let reader = plan(r#"{"type":"map","values":"string"}"#); let mut context = context();
    let resolution = AvroResolution::compile(&writer, &reader, &mut context).unwrap();
    assert!(resolution.decode_datum(&[4, 2, 0x80, 0, 2, 2, 0x80, 2, 2, b'x', 0], &mut context).is_err());
}

#[test]
fn binary_reader_retains_raw_keys_until_reader_key_class_is_applied() {
    let writer = plan(r#"{"type":"map","values":"int"}"#); let mut context = context();
    for (annotation, expected) in [("", 2), (r#", "avro.java.string":"String""#, 1)] {
        let reader = plan(&format!(r#"{{"type":"map","values":"long"{annotation}}}"#));
        let datum = AvroResolution::compile(&writer, &reader, &mut context).unwrap().decode_datum(&[4, 2, 0x80, 2, 2, 0x81, 4, 0], &mut context).unwrap();
        let Avro::Map(values) = datum.value else { panic!("expected map") };
        let values = values.ordered().unwrap(); assert_eq!(values.len(), expected);
        if expected == 1 { assert_eq!(values, vec![(MapKey::String("�".into()), Avro::Long(2))]); }
    }
}

#[test]
fn projected_zero_width_blocks_do_not_materialize_or_walk_each_element() {
    assert_eq!(projected(r#"{"type":"array","items":"null"}"#, &[0x80, 0x80, 0x80, 0x80, 8, 0, 14]).unwrap(), kept());
}

#[test]
fn skipped_negative_byte_lengths_can_rewind_inside_the_decoder_window() {
    for schema in [r#""bytes""#, r#""string""#] {
        assert_eq!(projected(schema, &[1]).unwrap(), Avro::Record(vec![("keep".into(), Avro::Long(-1))]));
        assert!(projected(schema, &[3]).unwrap_err().message.as_deref().is_some_and(|message| message.contains("before the admitted decoder window")));
    }
    let schema = plan(r#""string""#); let mut context = context();
    assert!(schema.decode_datum(&[1], &mut context).is_err());
    assert!(AvroResolution::compile(&schema, &schema, &mut context).unwrap().decode_datum(&[1], &mut context).is_err());
}

#[test]
fn skipped_negative_blocks_cannot_revisit_more_than_the_admitted_input_length() {
    let cause = projected(r#"{"type":"array","items":"null"}"#, &[1, 3]).unwrap_err();
    assert!(cause.message.as_deref().is_some_and(|message| message.contains("rewind allowance")));
    let cause = projected(r#"{"type":"array","items":"bytes"}"#, &[8, 1, 0]).unwrap_err();
    assert!(cause.message.as_deref().is_some_and(|message| message.contains("rewind allowance")));
}

#[test]
fn skipped_rewinds_cannot_cross_the_first_short_primitive_compaction() {
    let writer = plan(r#"{"type":"record","name":"R","fields":[{"name":"padding","type":{"type":"fixed","name":"F","size":12}},{"name":"ignored","type":"bytes"},{"name":"keep","type":"int"},{"name":"tail","type":{"type":"fixed","name":"T","size":7}}]}"#);
    let reader = plan(r#"{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}"#);
    let mut context = context(); let resolution = AvroResolution::compile(&writer, &reader, &mut context).unwrap();
    let mut bytes = vec![0; 21]; bytes[12] = 3;
    assert!(resolution.decode_datum(&bytes, &mut context).err().unwrap().message.as_deref().is_some_and(|message| message.contains("before the admitted decoder window")));
}

#[test]
fn guided_decode_admits_selected_allocations_and_retains_cached_charges() {
    let budget = MemoryBudget::new(1 << 20); let mut first = ConversionContext::new(Arc::clone(&budget));
    let writer = plan(r#"{"type":"record","name":"R","fields":[{"name":"ignored","type":"bytes"},{"name":"keep","type":"int"}]}"#);
    let reader = plan(r#"{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}"#);
    let resolution = AvroResolution::compile(&writer, &reader, &mut first).unwrap(); drop(first);
    let cached = budget.used_bytes(); let mut next = ConversionContext::with_limit(Arc::clone(&budget), cached + 16_384);
    let mut bytes = vec![0x80, 0x80, 1]; bytes.resize(8195, 255); bytes.push(14);
    assert_eq!(resolution.decode_datum(&bytes, &mut next).unwrap().value, Avro::Record(vec![("keep".into(), Avro::Int(7))]));
    assert!(writer.decode_datum(&bytes, &mut next).is_err());
    drop(resolution); assert!(budget.used_bytes() > 0); drop(next); assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn optional_float_blocks_keep_nan_payloads_and_negative_zero_in_both_binary_readers() {
    use arrow_array::{Array, Float32Array};
    let schema = plan(r#"{"type":"array","items":["float","null"]}"#);
    let mut context = context();
    let resolution = AvroResolution::compile(&schema, &schema, &mut context).unwrap();
    let float_bits = 0x7fc1_2345_u32;
    let mut bytes = vec![3, 10, 0];
    bytes.extend_from_slice(&float_bits.to_le_bytes());
    bytes.push(2);
    bytes.extend([2, 0]);
    bytes.extend_from_slice(&(-0.0_f32).to_le_bytes());
    bytes.push(0);
    for datum in [schema.decode_datum(&bytes, &mut context).unwrap(), resolution.decode_datum(&bytes, &mut context).unwrap()] {
        let Avro::PackedArray(packed) = datum.value else { panic!("expected packed array"); };
        let array = packed.values.array().unwrap().as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(array.value(0).to_bits(), float_bits);
        assert!(array.is_null(1));
        assert_eq!(array.value(2).to_bits(), (-0.0_f32).to_bits());
    }
}

#[test]
fn reader_resolution_promotes_numeric_array_items_without_losing_double_bits() {
    use arrow_array::{Float32Array, Float64Array};
    let writer = plan(r#"{"type":"array","items":"float"}"#);
    let reader = plan(r#"{"type":"array","items":"double"}"#);
    let mut context = context();
    let resolution = AvroResolution::compile(&writer, &reader, &mut context).unwrap();
    let value = f32::from_bits(0x7fc1_2345);
    let mut bytes = vec![4];
    bytes.extend_from_slice(&value.to_le_bytes());
    bytes.extend_from_slice(&(-0.0_f32).to_le_bytes());
    bytes.push(0);
    let raw = writer.decode_datum(&bytes, &mut context).unwrap();
    let Avro::PackedArray(packed) = &raw.value else { panic!("expected packed array"); };
    assert!(packed.values.array().unwrap().as_any().is::<Float32Array>());
    for datum in [resolution.decode_datum(&bytes, &mut context).unwrap(), resolution.resolve(raw, &mut context).unwrap()] {
        let Avro::PackedArray(packed) = datum.value else { panic!("expected promoted packed array"); };
        let array = packed.values.array().unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(array.value(0).to_bits(), f64::from(value).to_bits());
        assert_eq!(array.value(1).to_bits(), (-0.0_f64).to_bits());
    }
}

#[test]
fn resolution_handles_reordered_nullable_numeric_arrays_in_typed_buffers() {
    use arrow_array::{Array, Float64Array};
    let writer = plan(r#"{"type":"array","items":["int","null"]}"#);
    let reader = plan(r#"{"type":"array","items":["null","double"]}"#);
    let mut context = context();
    let resolution = AvroResolution::compile(&writer, &reader, &mut context).unwrap();
    let bytes = [4, 0, 14, 2, 0];
    let binary = resolution.decode_datum(&bytes, &mut context).unwrap();
    let memory = resolution.resolve(writer.decode_datum(&bytes, &mut context).unwrap(), &mut context).unwrap();
    for datum in [binary, memory] {
        let Avro::PackedArray(packed) = datum.value else { panic!("expected promoted nullable array"); };
        assert_eq!(packed.branches, Some((Some(0), 1)));
        let array = packed.values.array().unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(array.value(0), 7.0);
        assert!(array.is_null(1));
    }
}

#[test]
fn generalized_numeric_writer_branches_resolve_to_one_typed_reader_child() {
    use arrow_array::{Array, Float64Array};

    let writer = plan(r#"{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":["int","float","null","double","string"]}}]}"#);
    let reader = plan(r#"{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":["null","double"]}}]}"#);
    let mut context = context();
    let resolution = AvroResolution::compile(&writer, &reader, &mut context).unwrap();
    let nan = f32::from_bits(0x7fc1_2345);
    let mut bytes = vec![8, 0, 13, 2];
    bytes.extend_from_slice(&nan.to_le_bytes());
    bytes.extend([4, 6]);
    bytes.extend_from_slice(&(-0.0_f64).to_le_bytes());
    bytes.push(0);
    let binary = resolution.decode_datum(&bytes, &mut context).unwrap();
    let legacy = resolution.resolve(writer.decode_datum(&bytes, &mut context).unwrap(), &mut context).unwrap();
    let Avro::Record(fields) = &binary.value else { panic!("expected resolved record"); };
    let Avro::PackedArray(packed) = &fields[0].1 else { panic!("expected packed resolution"); };
    let child = packed.values.array().unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(child.value(0), -7.0);
    assert_eq!(child.value(1).to_bits(), f64::from(nan).to_bits());
    assert!(child.is_null(2));
    assert_eq!(child.value(3).to_bits(), (-0.0_f64).to_bits());

    let connect = reader.connect_plan(None, None, &mut 0, &mut context).unwrap();
    for datum in [binary, legacy] {
        let rtdb_types::Value::Struct(fields) = connect.convert(datum, &mut 0, &mut context).unwrap().value else { panic!("expected Connect record"); };
        let rtdb_types::Value::List(list) = &fields["a"] else { panic!("expected Connect array"); };
        let out = list.array().unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(out.value(1).to_bits(), f64::from(nan).to_bits());
        assert!(out.is_null(2));
        assert_eq!(out.value(3).to_bits(), (-0.0_f64).to_bits());
    }

    let incompatible = resolution.decode_datum(&[2, 8, 2, b'a', 0], &mut context).err().unwrap();
    assert_eq!(incompatible.code, "xtdb.kafka-connect-source/avro-resolution");
    assert_eq!(incompatible.message.as_deref(), Some("No matching reader union branch"));
    let invalid = resolution.decode_datum(&[2, 10, 0], &mut context).err().unwrap();
    assert_eq!(invalid.code, "xtdb.kafka-connect-source/avro-decode");
    assert_eq!(invalid.message.as_deref(), Some("Union index is out of range"));
}

#[test]
fn numeric_arrays_keep_buffers_when_entering_singleton_or_nullable_reader_unions() {
    use arrow_array::Float32Array;
    let writer = plan(r#"{"type":"array","items":"float"}"#);
    let mut bytes = vec![2];
    bytes.extend_from_slice(&(-0.0_f32).to_le_bytes());
    bytes.push(0);
    let mut context = context();
    let raw = writer.decode_datum(&bytes, &mut context).unwrap();
    let Avro::PackedArray(packed) = &raw.value else { panic!("expected packed writer"); };
    let values = packed.values.array().unwrap().as_any().downcast_ref::<Float32Array>().unwrap().values().as_ptr();

    for (schema, branches) in [
        (r#"{"type":"array","items":["float"]}"#, Some((None, 0))),
        (r#"{"type":"array","items":["null","float"]}"#, Some((Some(0), 1))),
    ] {
        let reader = plan(schema);
        let resolution = AvroResolution::compile(&writer, &reader, &mut context).unwrap();
        let datum = resolution.resolve(AvroDatum::untracked(Arc::clone(&writer), raw.value.clone()), &mut context).unwrap();
        let Avro::PackedArray(packed) = &datum.value else { panic!("expected packed reader"); };
        assert_eq!(packed.branches, branches);
        assert_eq!(packed.values.array().unwrap().as_any().downcast_ref::<Float32Array>().unwrap().values().as_ptr(), values);
        let bridge = writer.connect_plan(None, None, &mut 0, &mut context).unwrap();
        let rtdb_types::Value::List(list) = bridge.convert(datum, &mut 0, &mut context).unwrap().value else { panic!("expected packed writer output"); };
        assert_eq!(list.array().unwrap().as_any().downcast_ref::<Float32Array>().unwrap().values().as_ptr(), values);
    }
}

#[test]
fn a_required_reader_keeps_packed_values_when_no_writer_item_selects_null() {
    use arrow_array::Float32Array;

    let writer = plan(r#"{"type":"array","items":["null","float"]}"#);
    let reader = plan(r#"{"type":"array","items":"float"}"#);
    let mut context = context();
    let resolution = AvroResolution::compile(&writer, &reader, &mut context).unwrap();
    let mut bytes = vec![2, 2];
    bytes.extend_from_slice(&(-0.0_f32).to_le_bytes());
    bytes.push(0);
    let raw = writer.decode_datum(&bytes, &mut context).unwrap();
    let Avro::PackedArray(packed) = &raw.value else { panic!("expected packed writer"); };
    let array = packed.values.array().unwrap().as_any().downcast_ref::<Float32Array>().unwrap();
    let source_values = array.values().as_ptr();
    let binary = resolution.decode_datum(&bytes, &mut context).unwrap();
    let memory = resolution.resolve(raw, &mut context).unwrap();
    for datum in [&binary, &memory] {
        let Avro::PackedArray(packed) = &datum.value else { panic!("expected packed reader"); };
        let array = packed.values.array().unwrap().as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(packed.branches, None);
        assert_eq!(array.value(0).to_bits(), (-0.0_f32).to_bits());
    }
    let Avro::PackedArray(packed) = memory.value else { unreachable!(); };
    let array = packed.values.array().unwrap().as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(array.values().as_ptr(), source_values);

    let null = [2, 0, 0];
    let binary_error = resolution.decode_datum(&null, &mut context).err().unwrap();
    let raw = writer.decode_datum(&null, &mut context).unwrap();
    let memory_error = resolution.resolve(raw, &mut context).err().unwrap();
    assert_eq!(binary_error.code, "xtdb.kafka-connect-source/avro-resolution");
    assert_eq!((binary_error.code, binary_error.message), (memory_error.code, memory_error.message));
}
