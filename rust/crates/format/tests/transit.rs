use rtdb_format::transit::{TransitValue as V, decode_anomaly, decode_msgpack, encode_anomaly, encode_msgpack, from_json_value, to_json_value};
use rtdb_types::Anomaly;

#[test]
fn keyword_and_symbol_cache_references_preserve_their_types() {
    let value = V::Array(vec![V::Keyword("repeated".into()), V::Keyword("repeated".into()), V::Symbol("repeated".into()), V::Symbol("repeated".into())]);
    let bytes = encode_msgpack(&value).unwrap();
    assert!(bytes.windows(2).any(|pair| pair == b"^0"));
    assert!(bytes.windows(2).any(|pair| pair == b"^1"));
    assert_eq!(decode_msgpack(&bytes).unwrap(), value);
}

#[test]
fn errors_keep_xtdb_tags_and_keyword_context() {
    let error = Anomaly::incorrect("xtdb/bad-id", "Invalid ID").with_context("value", 42.into());
    assert_eq!(decode_msgpack(&encode_anomaly(&error).unwrap()).unwrap(), V::Tagged { tag: "xt/error".into(), representation: Box::new(V::Array(vec![
        V::Keyword("incorrect".into()), V::Keyword("xtdb/bad-id".into()), V::String("Invalid ID".into()),
        V::Map(vec![(V::Keyword("value".into()), V::Integer(42))]),
    ])) });
}

#[test]
fn legacy_errors_and_unknown_extension_tags_are_retained() {
    for tag in ["xtdb/illegal-arg", "xtdb/runtime-err", "xtdb/exception-info", "xtdb/clj-form", "future/type"] {
        let value = V::Tagged { tag: tag.into(), representation: Box::new(V::Array(vec![V::String("message".into()), V::Map(Vec::new())])) };
        assert_eq!(decode_msgpack(&encode_msgpack(&value).unwrap()).unwrap(), value);
    }
}

#[test]
fn escaped_strings_binary_and_signed_width_boundaries_roundtrip() {
    let mut values: Vec<_> = [i64::MIN, -32_769, -32_768, -129, -128, -33, -32, 0, 127, 128, 255, 256, 65_535, 65_536, i64::MAX].into_iter().map(V::Integer).collect();
    values.extend([V::String("~:literal".into()), V::String("^0".into()), V::String("`reserved".into()), V::Bytes(vec![0, 128, 255])]);
    for value in values { assert_eq!(decode_msgpack(&encode_msgpack(&value).unwrap()).unwrap(), value); }
}

#[test]
fn cache_wrap_and_utf16_cache_length_match_the_transit_protocol() {
    let mut values: Vec<_> = (0..2000).map(|index| V::Keyword(format!("key{index}"))).collect();
    values.extend([V::Keyword("key1999".into()), V::String("🐈🐈".into())]);
    let value = V::Array(values);
    assert_eq!(decode_msgpack(&encode_msgpack(&value).unwrap()).unwrap(), value);
}

#[test]
fn truncated_and_invalid_cache_records_are_rejected() {
    for bytes in [&[0xa2, b'^', b'0'][..], &[0xdd, 0xff, 0xff, 0xff, 0xff], &[0xd9, 5, b'a'], &[0x92, 0xc0], &[0xc0, 0xc0]] {
        assert!(decode_msgpack(bytes).is_err(), "{bytes:?}");
    }
}

#[test]
fn nested_error_context_retains_keywords_symbols_bytes_maps_and_extension_tags() {
    let context = V::Map(vec![(V::Keyword("details".into()), V::Array(vec![
        V::Keyword("public/accounts".into()), V::Symbol("?_0".into()), V::Bytes(vec![0, 128, 255]),
        V::Map(vec![(V::Keyword("id".into()), V::Integer(42)), (V::String("id".into()), V::Null)]),
        V::Tagged { tag: "time/instant".into(), representation: Box::new(V::String("2024-01-01T00:00:00Z".into())) },
        V::Tagged { tag: "keyword".into(), representation: Box::new(V::Integer(1)) },
        V::Map(vec![(V::String("$transit/type".into()), V::String("keyword".into())), (V::String("value".into()), V::String("ordinary-map".into()))]),
    ]))]);
    let error = V::Tagged { tag: "xt/error".into(), representation: Box::new(V::Array(vec![V::Keyword("incorrect".into()), V::Null, V::String("Invalid input".into()), context])) };
    let decoded = decode_anomaly(&encode_msgpack(&error).unwrap()).unwrap();
    assert_eq!(decoded.code, "");
    assert_eq!(decode_msgpack(&encode_anomaly(&decoded).unwrap()).unwrap(), error);
}

#[test]
fn compound_map_keys_use_cmap_and_keep_cache_order() {
    let value = V::Map(vec![
        (V::Array(vec![V::Keyword("repeated".into())]), V::Keyword("repeated".into())),
        (V::Tagged { tag: "xt/table".into(), representation: Box::new(V::Array(vec![V::String("public".into()), V::String("docs".into())])) }, V::Integer(1)),
    ]);
    let bytes = encode_msgpack(&value).unwrap();
    assert!(bytes.windows(6).any(|slice| slice == b"~#cmap"));
    assert_eq!(decode_msgpack(&bytes).unwrap(), value);
    assert_eq!(from_json_value(&to_json_value(&value).unwrap()).unwrap(), value);
}

#[test]
fn special_numbers_keep_their_class_through_error_context_json() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let encoded = encode_msgpack(&V::Float(value)).unwrap();
        let decoded = decode_msgpack(&encoded).unwrap();
        let V::Float(decoded) = from_json_value(&to_json_value(&decoded).unwrap()).unwrap() else { panic!("float type changed") };
        if value.is_nan() { assert!(decoded.is_nan()); } else { assert_eq!(decoded, value); }
    }
}

#[test]
fn persisted_legacy_illegal_argument_and_runtime_error_tags_decode_as_incorrect() {
    for tag in ["xtdb/illegal-arg", "xtdb/runtime-err"] {
        let wire = V::Tagged { tag: tag.into(), representation: Box::new(V::Array(vec![V::Keyword("legacy/code".into()), V::String("message".into()), V::Null, V::Null])) };
        let decoded = decode_anomaly(&encode_msgpack(&wire).unwrap()).unwrap();
        assert_eq!(decoded, Anomaly::incorrect("legacy/code", "message"));
    }
}

#[test]
fn unsigned_json_context_integer_does_not_round_to_a_double() {
    assert_eq!(from_json_value(&serde_json::json!(u64::MAX)).unwrap(), V::Tagged { tag: "n".into(), representation: Box::new(V::String(u64::MAX.to_string())) });
}

#[test]
fn legacy_exception_info_normalizes_to_the_public_unknown_fault() {
    let wire = V::Tagged { tag: "xtdb/exception-info".into(), representation: Box::new(V::Array(vec![V::String("old exception".into()), V::Map(vec![(V::Keyword("old/data".into()), V::Integer(42))])])) };
    assert_eq!(decode_anomaly(&encode_msgpack(&wire).unwrap()).unwrap(), Anomaly::fault("xtdb.error/unknown", "old exception"));
}

#[test]
fn transit_json_wire_quotes_scalars_and_preserves_large_integer_map_keys() {
    use rtdb_format::transit::{encode_json, decode_json, TransitValue as T};
    assert_eq!(encode_json(&T::String("~escape".into())).unwrap(), br#"["~#'","~~escape"]"#);
    assert_eq!(encode_json(&T::Integer(i64::MAX)).unwrap(), br#"["~#'","~i9223372036854775807"]"#);
    let value = T::Map(vec![(T::Null, T::Bool(true)), (T::Integer(7), T::Bytes(vec![0, 255])), (T::Bool(false), T::Keyword("kind".into()))]);
    let bytes = encode_json(&value).unwrap();
    assert_eq!(bytes, br#"["^ ","~_",true,"~i7","~bAP8=","~?f","~:kind"]"#);
    assert_eq!(decode_json(&bytes).unwrap(), value);
}

#[test]
fn transit_json_composite_map_and_tagged_values_round_trip() {
    use rtdb_format::transit::{encode_json, decode_json, TransitValue as T};
    let value = T::Map(vec![(T::Array(vec![T::Integer(1)]), T::Tagged { tag: "xt/custom".into(), representation: Box::new(T::String("^literal".into())) })]);
    assert_eq!(decode_json(&encode_json(&value).unwrap()).unwrap(), value);
}

#[test]
fn transit_sequences_reset_cache_per_top_level_and_reject_partial_tail() {
    use rtdb_format::transit::{decode_json_sequence, decode_msgpack_sequence, encode_msgpack, TransitValue as T};
    let values = vec![T::Array(vec![T::Keyword("alpha".into()), T::Keyword("alpha".into())]), T::Array(vec![T::Keyword("bravo".into()), T::Keyword("bravo".into())])];
    assert_eq!(decode_json_sequence(br#"["~:alpha","^0"] ["~:bravo","^0"]"#).unwrap(), values);
    let mut bytes = encode_msgpack(&values[0]).unwrap(); bytes.extend(encode_msgpack(&values[1]).unwrap());
    assert_eq!(decode_msgpack_sequence(&bytes).unwrap(), values);
    bytes.push(0xd9);
    assert!(decode_msgpack_sequence(&bytes).is_err());
    assert!(decode_json_sequence(br#"["~:alpha"] ["unfinished"#).is_err());
    assert!(decode_json_sequence(b"").unwrap().is_empty());
}

#[test]
fn msgpack_allocation_admission_preserves_the_encoders_depth_boundary() {
    use rtdb_format::transit::msgpack_allocation_bound;
    for leaf in [V::Null, V::Tagged { tag: "i".into(), representation: Box::new(V::String("7".into())) }] {
        let mut value = leaf;
        for _ in 0..128 { value = V::Array(vec![value]); }
        assert!(msgpack_allocation_bound(&value).unwrap() >= encode_msgpack(&value).unwrap().len());
        value = V::Array(vec![value]);
        assert_eq!(msgpack_allocation_bound(&value).unwrap_err().message, "Transit nesting limit exceeded");
        assert_eq!(encode_msgpack(&value).unwrap_err().message, "Transit nesting limit exceeded");
    }
    let mut value = V::Tagged { tag: "xt/custom".into(), representation: Box::new(V::String("payload".into())) };
    for _ in 0..128 { value = V::Array(vec![value]); }
    assert_eq!(msgpack_allocation_bound(&value).unwrap_err().message, "Transit nesting limit exceeded");
    assert_eq!(encode_msgpack(&value).unwrap_err().message, "Transit nesting limit exceeded");
}
