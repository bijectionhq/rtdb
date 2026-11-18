use super::*;
use super::super::super::test_support as support;

#[test]
fn document_number_tokens_preserve_jackson_default_factory_classes_and_integer_negative_zero() {
    let mut context = support::context();
    assert!(matches!(parse("-0", &mut context).unwrap().value(), JsonValue::I32(0)));
    assert!(matches!(parse("2147483648", &mut context).unwrap().value(), JsonValue::I64(2147483648)));
    assert!(matches!(parse("9223372036854775808", &mut context).unwrap().value(), JsonValue::Integer(_)));
    assert!(matches!(parse("1.0", &mut context).unwrap().value(), JsonValue::F64(value) if *value == 1.0));
    assert!(matches!(parse("-0.0", &mut context).unwrap().value(), JsonValue::F64(value) if value.to_bits() == (-0.0f64).to_bits()));
}

#[test]
fn native_lexical_profile_limits_are_explicit_and_do_not_claim_source_constraint_errors() {
    let mut context = support::context();
    for input in [" ".repeat(MAX_DOCUMENT_BYTES + 1), format!("\"{}\"", "x".repeat(MAX_TEXT_UNITS + 1)), "1".repeat(MAX_NUMBER_BYTES + 1), format!("{{\"{}\":0}}", "x".repeat(MAX_TEXT_UNITS + 1))] {
        assert!(parse(&input, &mut context).err().unwrap().code.ends_with("unsupported-avro-input-profile"));
    }
    assert!(parse(&"1".repeat(MAX_NUMBER_BYTES), &mut context).is_ok());
    assert!(parse(&format!("\"{}\"", "x".repeat(MAX_TEXT_UNITS)), &mut context).is_ok());
}

#[test]
fn field_double_text_uses_java_hex_suffix_sign_and_control_space_grammar() {
    let mut context = support::context();
    assert_eq!(double(JavaText::from_utf8("\u{0}\t0x1p-1074d\n"), &mut context).unwrap().to_bits(), 1);
    assert_eq!(double(JavaText::from_utf8("-0"), &mut context).unwrap().to_bits(), (-0.0f64).to_bits());
    assert!(double(JavaText::from_utf8("+Infinity"), &mut context).unwrap().is_infinite());
    assert!(double(JavaText::from_utf8("-NaN"), &mut context).unwrap().is_nan());
    assert!(double(JavaText::from_utf8("NaNf"), &mut context).is_err());
}
