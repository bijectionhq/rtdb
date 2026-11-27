use rtdb_format::transit::{TransitValue as V, decode_json};

#[test]
fn array_maps_cache_keys_before_values_and_keep_wire_order() {
    assert_eq!(decode_json(br#"["^ ","~:zulu",1,"~:alpha","^0","^1",2]"#).unwrap(), V::Map(vec![
        (V::Keyword("zulu".into()), V::Integer(1)),
        (V::Keyword("alpha".into()), V::Keyword("zulu".into())),
        (V::Keyword("alpha".into()), V::Integer(2)),
    ]));
}

#[test]
fn object_maps_preserve_cache_order_and_duplicate_keys() {
    assert_eq!(decode_json(br#"{"zulu":1,"alpha":"^0","alpha":"^1"}"#).unwrap(), V::Map(vec![
        (V::String("zulu".into()), V::Integer(1)),
        (V::String("alpha".into()), V::String("zulu".into())),
        (V::String("alpha".into()), V::String("alpha".into())),
    ]));
}

#[test]
fn wire_json_tags_quotes_escaped_markers_and_integer_boundaries() {
    assert_eq!(decode_json(br#"["~#'",["~i9223372036854775807","~i-9223372036854775808","~^ ","~bAID/",["~#time/instant","2024-01-01T00:00:00Z"]]]"#).unwrap(), V::Array(vec![
        V::Integer(i64::MAX), V::Integer(i64::MIN), V::String("^ ".into()), V::Bytes(vec![0, 128, 255]),
        V::Tagged { tag: "time/instant".into(), representation: Box::new(V::String("2024-01-01T00:00:00Z".into())) },
    ]));
    assert_eq!(decode_json(br#"{"~#cmap":[["~:key"],true]}"#).unwrap(), V::Map(vec![(V::Array(vec![V::Keyword("key".into())]), V::Bool(true))]));
}

#[test]
fn wire_json_rejects_unmatched_keys_invalid_references_and_trailing_values() {
    for input in [br#"["^ ","key"]"#.as_slice(), br#"["^0"]"#, br#"["~#tag",1,2]"#, b"null true", b"18446744073709551615"] {
        assert!(decode_json(input).is_err(), "{input:?}");
    }
}

#[test]
fn jvm_arrow_leaves_stream_the_existing_transit_json_bytes() {
    use rtdb_columnar::{ArrowCell, copy_array, to_field};
    use rtdb_format::transit::{encode_json, encode_msgpack, value_to_transit, write_arrow_json};
    use rtdb_types::{DecimalValue, Keyword, TimeUnit, Value};

    let embedded_transit = V::Map(vec![(V::Keyword("map-key".into()), V::Bytes(vec![0xFE, 0x81]))]);
    let controls: String = (0_u8..32).map(char::from).collect();
    let text = format!("~{controls}é😀\u{2028}\u{2029}\u{7f}/\\\"");
    let keyword = Keyword::from_name(format!("^namespace/é{controls}\\\""));
    let uri = "https://example.com/é%22~?q=%5C#片";
    let values = [
        Value::I64(i64::MAX), Value::U64(u64::MAX), Value::F32(0.2), Value::F64(-0.0),
        Value::F64(0.19736842105263158), Value::F64(1e-201), Value::F64(f64::INFINITY),
        Value::Utf8("^escaped\\\n\"".into()), Value::FixedSizeBinary(vec![0, 128, 255]),
        Value::Utf8(text.clone()), Value::Utf8("`".into()), Value::Utf8("".into()),
        Value::Keyword(Keyword::from_name("named/é")), Value::Uuid([13; 16]), Value::Uri("https://example.com/a".into()),
        Value::Keyword(keyword.clone()), Value::Uri(uri.into()),
        Value::RegClass(41), Value::DateDay(-1), Value::Time { value: 1310, unit: TimeUnit::Millisecond, bit_width: 32 },
        Value::Timestamp { value: -123, unit: TimeUnit::Nanosecond, timezone: Some("UTC".into()) },
        Value::IntervalDayTime { days: 2, milliseconds: 129 },
        Value::TimestampTzRange { lower: -123, upper: i64::MAX },
        Value::Map { entries: vec![(Value::Keyword(Keyword::from_name("map-key")), Value::DateDay(-1))], sorted: false },
        Value::Map { entries: vec![(Value::Utf8(text.clone()), Value::Keyword(keyword.clone()))], sorted: false },
        Value::Map { entries: vec![(Value::Uri(uri.into()), Value::Keyword(keyword))], sorted: false },
        Value::Map { sorted: false, entries: vec![
            (Value::Uuid([13; 16]), Value::I64(1)),
            (Value::FixedSizeBinary(vec![0xfe]), Value::I64(2)),
            (Value::Decimal(DecimalValue::new((-123i128).to_be_bytes().to_vec(), 20, 2, 128).unwrap()), Value::I64(3)),
            (Value::Transit(encode_msgpack(&V::Keyword("nested/key".into())).unwrap()), Value::I64(4)),
        ] },
        Value::Map { entries: vec![(Value::RegClass(41), Value::I64(1))], sorted: false },
        Value::Map { entries: vec![(Value::list(vec![Value::Utf8(text)]), Value::Uri(uri.into()))], sorted: false },
        Value::Transit(encode_msgpack(&embedded_transit).unwrap()),
    ];
    for value in values {
        let ty = value.logical_type();
        let array = copy_array(&ty, std::slice::from_ref(&value)).unwrap();
        let field = to_field("value", &ty).unwrap();
        let cell = ArrowCell { field: &field, array: array.as_ref(), row: 0 };
        let mut actual = Vec::new();
        write_arrow_json(&mut actual, cell).unwrap();
        let expected = encode_json(&value_to_transit(&value).unwrap()).unwrap();
        assert_eq!(String::from_utf8(actual).unwrap(), String::from_utf8(expected).unwrap(), "{value:?}");
    }
}

#[test]
fn arrow_transit_rejects_invalid_uri_before_writing_its_tag() {
    use std::sync::Arc;
    use arrow_array::{Array, Int64Array, MapArray, StringArray, StructArray};
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::{DataType, Field};
    use rtdb_columnar::{ArrowCell, to_field};
    use rtdb_format::transit::write_arrow_json;
    use rtdb_types::{LogicalType, ScalarType, UriParts};

    let invalid = "https://example.com/%zz";
    let field = to_field("uri", &LogicalType::Scalar(ScalarType::Uri)).unwrap();
    let array = StringArray::from(vec![invalid]);
    let cell = ArrowCell { field: &field, array: &array, row: 0 };
    let mut output = Vec::new();
    assert_eq!(write_arrow_json(&mut output, cell).unwrap_err(), UriParts::parse(invalid).unwrap_err());
    assert!(output.is_empty());

    let entries = StructArray::new(
        vec![field.with_name("key"), Field::new("value", DataType::Int64, false)].into(),
        vec![Arc::new(StringArray::from(vec!["https://example.com/good", invalid])), Arc::new(Int64Array::from(vec![1, 2]))],
        None,
    );
    let entry_field = Field::new("entries", entries.data_type().clone(), false);
    let map = MapArray::try_new(Arc::new(entry_field), OffsetBuffer::new(vec![0, 2].into()), entries, None, false).unwrap();
    let field = Field::new("map", map.data_type().clone(), false);
    let cell = ArrowCell { field: &field, array: &map, row: 0 };
    assert_eq!(write_arrow_json(&mut output, cell).unwrap_err(), UriParts::parse(invalid).unwrap_err());
    assert!(output.is_empty());
}

#[test]
fn arrow_transit_propagates_a_writer_failure_inside_escaped_text() {
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field};
    use rtdb_columnar::ArrowCell;
    use rtdb_format::transit::{encode_json, write_arrow_json};

    let value = "~\"é";
    let field = Field::new("value", DataType::Utf8, false);
    let array = StringArray::from(vec![value]);
    let cell = ArrowCell { field: &field, array: &array, row: 0 };
    let mut output = [0; 11];
    let error = write_arrow_json(&mut output.as_mut_slice(), cell).unwrap_err();
    assert_eq!(error.code, "rtdb.format/invalid-transit");
    let expected = encode_json(&V::String(value.into())).unwrap();
    assert_eq!(output.as_slice(), &expected[..output.len()]);
}
