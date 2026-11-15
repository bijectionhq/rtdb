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
