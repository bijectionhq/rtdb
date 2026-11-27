use rtdb_expr::{arithmetic, cast, function_type};
use rtdb_plan::BinaryOp as B;
use rtdb_types::{LogicalType as L, ScalarType as S, Value as V};

fn error(result: rtdb_types::Result<impl std::fmt::Debug>) -> (String, String) {
    let error = result.unwrap_err();
    (error.code, error.message.unwrap_or_default())
}

#[test]
fn a_type_mismatch_names_the_function_and_its_argument_type_heads() {
    assert_eq!(error(arithmetic(B::Add, &V::I64(1), &V::Utf8("2".into()))),
               ("xtdb.expression/function-type-mismatch".into(), "+ not applicable to types i64 and utf8".into()));
    assert_eq!(error(function_type("upper", &[L::Scalar(S::I64)])),
               ("xtdb.expression/function-type-mismatch".into(), "upper not applicable to types i64".into()));
    assert_eq!(error(function_type("mod", &[L::Scalar(S::Utf8), L::Scalar(S::F64)])).1, "mod not applicable to types utf8 and f64");
}

#[test]
fn text_that_is_not_a_number_raises_the_java_number_format_error() {
    let to = |ty: S, text: &str| cast(&V::Utf8(text.into()), &L::Scalar(ty), "UTC");
    let number_format = |message: &str| ("xtdb.expression/number-format-error".to_string(), message.to_string());
    assert_eq!(error(to(S::I64, "x")), number_format("For input string: \"x\""));
    assert_eq!(error(to(S::I64, "9223372036854775808")), number_format("For input string: \"9223372036854775808\""));
    assert_eq!(error(to(S::I16, "40000")), number_format("Value out of range. Value:\"40000\" Radix:10"));
    assert_eq!(to(S::I32, "+12").unwrap(), V::I32(12));
    assert_eq!(to(S::I16, "-１２٣").unwrap(), V::I16(-123));
    assert_eq!(to(S::I64, "-９223372036854775808").unwrap(), V::I64(i64::MIN));
    assert_eq!(error(to(S::I8, "𝟙")), number_format("For input string: \"𝟙\""));
    assert_eq!(error(to(S::F64, " ")), number_format("empty String"));
    assert_eq!(error(to(S::F64, "inf")), number_format("For input string: \"inf\""));
    assert_eq!(error(to(S::F64, "Infinityd")), number_format("For input string: \"Infinityd\""));
    assert_eq!(to(S::F64, " 1.5d\n").unwrap(), V::F64(1.5));
    assert_eq!(to(S::F64, "-Infinity").unwrap(), V::F64(f64::NEG_INFINITY));
}

#[test]
fn decimal_and_hexadecimal_float_text_round_directly_at_the_target_width() {
    let to = |ty: S, text: &str| cast(&V::Utf8(text.into()), &L::Scalar(ty), "UTC").unwrap();
    for (text, bits) in [
        ("1.0000000596046447753906250000000000001", 0x3f80_0001),
        ("0x1.000001p0", 0x3f80_0000),
        ("0x1.00000100000000000001p0", 0x3f80_0001),
        ("-0x1p-150", 0x8000_0000),
        ("0x1.fffffep-127", 0x0080_0000),
        ("-NaN", f32::NAN.to_bits()),
    ] {
        let V::F32(value) = to(S::F32, text) else { panic!("expected REAL") };
        assert_eq!(value.to_bits(), bits, "{text}");
    }
    assert_eq!(to(S::F64, " -0X1.8P1d\n"), V::F64(-3.0));
    assert_eq!(to(S::F64, "0x1.0000000000000800001p0"), V::F64(f64::from_bits(0x3ff0_0000_0000_0001)));
}
