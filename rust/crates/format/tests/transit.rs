use rtdb_format::transit::{TransitValue as V, decode_msgpack, encode_anomaly, encode_msgpack};
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
