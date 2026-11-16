use super::*;

#[test]
fn integer_nodes_preserve_digits_and_jackson_wrapping_accessors() {
    for (text, long, int) in [("18446744073709551617", 1, 1), ("-18446744073709551617", -1, -1), ("4294967297", 4_294_967_297, 1)] {
        let value = JsonValue::from_str(text).unwrap();
        assert!(value.is_integral());
        assert_eq!(value.long_value(), Some(long));
        assert_eq!(value.int_value(), Some(int));
        assert_eq!(value.to_vec().unwrap(), text.as_bytes());
    }
    assert!(!JsonValue::from_str("2147483648").unwrap().is_int());
    assert!(!JsonValue::from_str("1.0").unwrap().is_integral());
    assert_eq!(JsonValue::from_str("1e100").unwrap().long_value(), Some(i64::MAX));
    assert_eq!(JsonValue::from_str("-1e100").unwrap().int_value(), Some(i32::MIN));
}

#[test]
fn integer_float_access_does_not_round_through_double() {
    let value = JsonValue::from_str("18014399583223809").unwrap();
    assert_eq!(value.float_value(), Some(f32::from_bits(0x5a800001)));
    assert_ne!(value.float_value(), value.double_value().map(|value| value as f32));
    assert_eq!(JsonValue::from_str("-0").unwrap().float_value().unwrap().to_bits(), 0);
}

#[test]
fn json_output_uses_java_double_text_and_retains_integer_type() {
    let value = JsonValue::from_str(r#"[10000000,1e7,1e20,0.001,0.0001,-0,-0.0,18446744073709551617]"#).unwrap();
    let bytes = value.to_vec().unwrap();
    assert_eq!(bytes, br#"[10000000,1.0E7,1.0E20,0.001,1.0E-4,0,-0.0,18446744073709551617]"#);
    assert!(bytes.len() <= value.encoded_bound().unwrap());
    let value = JsonValue::from_serde(serde_json::json!({"number": 1e20}));
    assert_eq!(value.to_vec().unwrap(), br#"{"number":1.0E20}"#);
}

#[test]
fn duplicate_object_keys_keep_first_order_and_last_values() {
    let value = JsonValue::from_str(r#"{"a":1,"b":{"c":2},"a":18446744073709551617}"#).unwrap();
    assert_eq!(value.to_vec().unwrap(), br#"{"a":18446744073709551617,"b":{"c":2}}"#);
    assert_eq!(value.get("a").unwrap().long_value(), Some(1));
    assert_eq!(value.pointer("/b/c").unwrap().int_value(), Some(2));
    let reversed = JsonValue::from_str(r#"{"b":{"c":2},"a":18446744073709551617}"#).unwrap();
    assert_eq!(value, reversed);
}

#[test]
fn registry_decimal_nodes_keep_scale_narrow_without_double_rounding_and_compare_by_value() {
    let parse = JsonValue::from_registry_str;
    let value = parse("18446744073709551617.99").unwrap();
    assert_eq!(value.long_value(), Some(1));
    assert_eq!(value.int_value(), Some(1));
    assert_eq!(value.to_vec().unwrap(), b"18446744073709551617.99");
    assert!(!value.is_integral());
    assert_eq!(parse("-18446744073709551617.99").unwrap().long_value(), Some(-1));
    assert_eq!(parse("1e64").unwrap().long_value(), Some(0));
    assert_eq!(parse("1e-2147483647").unwrap().long_value(), Some(0));
    assert_eq!(parse("1e2147483648").unwrap().long_value(), Some(0));
    assert!(parse("1e2147483649").is_err());
    assert!(parse("1e-2147483648").is_err());
    assert_eq!(parse("[1.00,1e7,0.000001,0.0000001,-0.0,0e7]").unwrap().to_vec().unwrap(), b"[1.00,1E+7,0.000001,1E-7,0.0,0E+7]");
    assert_eq!(parse("1.0").unwrap(), parse("1.00").unwrap());
    assert_eq!(parse("-0.0").unwrap(), parse("0.00").unwrap());
    assert_ne!(parse("1.0").unwrap(), parse("1").unwrap());
    assert_eq!(JsonValue::from_str("1.0").unwrap(), JsonValue::from_str("1.00").unwrap());
    assert_ne!(JsonValue::from_str("-0.0").unwrap(), JsonValue::from_str("0.0").unwrap());
    assert_ne!(parse("1.0").unwrap(), JsonValue::from_str("1.0").unwrap());
    assert_eq!(parse("18014399583223809.0").unwrap().float_value().unwrap().to_bits(), 0x5a800001);
}
