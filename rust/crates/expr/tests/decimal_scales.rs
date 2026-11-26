use std::cmp::Ordering;
use rtdb_expr::{arithmetic, cast, compare, decimal_long_value, decimal_rescale, decimal_text, decimal_to_f64, parse_decimal, scalar_call, sql_eq, CompiledExpr, EvalContext};
use rtdb_plan::{BinaryOp as B, Expr, Schema, function_type_with_args};
use rtdb_types::{DecimalValue, LogicalType as T, ScalarType as S, Value as V};

fn decimal(coefficient: i128, scale: i32) -> DecimalValue { DecimalValue::new(coefficient.to_be_bytes().to_vec(), 32, scale, 128).unwrap() }
fn value(coefficient: i128, scale: i32) -> V { V::Decimal(decimal(coefficient, scale)) }

#[test]
fn java_decimal_text_checks_final_scale_and_rejects_rust_underscore_extensions() {
    for (text, scale) in [("1E+2147483648", i32::MIN), ("1E-2147483647", i32::MAX), ("1.0E+2147483649", i32::MIN), ("+1.", 0), ("-.1", 1), ("1e00000000000000000001", -1)] {
        assert_eq!(parse_decimal(text).unwrap().as_bigint_and_exponent().1, i64::from(scale), "{text}");
    }
    for text in ["1E-2147483648", "1E+2147483649", "1e10000000000", "1_0.1", "1.0_", " 1", "1 ", ".", "1e", "1e++1"] {
        assert!(parse_decimal(text).is_err(), "{text}");
    }
}

#[test]
fn java_decimal_parser_normalizes_bmp_coefficient_and_exponent_digits() {
    for (text, coefficient, scale) in [
        ("+١२.３४e-๕", 1234, 7), ("-.٠١", -1, 2), ("١E+٢١٤٧٤٨٣٦٤٨", 1, i32::MIN),
        ("१.०E+२१४७४८३६४९", 10, i32::MIN), ("１E-２１４７４８３６４７", 1, i32::MAX),
    ] {
        let (actual_coefficient, actual_scale) = parse_decimal(text).unwrap().as_bigint_and_exponent();
        assert_eq!(actual_coefficient, coefficient.into(), "{text}");
        assert_eq!(actual_scale, i64::from(scale), "{text}");
        assert_eq!(cast(&V::Utf8(text.into()), &T::Scalar(S::Decimal { precision: 32, scale, bit_width: 128 }), "UTC").unwrap(), value(coefficient, scale));
    }
    for text in ["𝟙.0", "1e𝟚", "−١", "١٫٢", "١E-٢١٤٧٤٨٣٦٤٨"] { assert!(parse_decimal(text).is_err(), "{text}"); }
    let (coefficient, scale) = parse_decimal("١٢٣٤٥٦٧٨٩٠１２３４５６７８９０૧૨૩૪૫૬૭૮૯૦.٠٠").unwrap().as_bigint_and_exponent();
    assert_eq!(coefficient.to_string(), "12345678901234567890123456789000");
    assert_eq!(scale, 2);
}

#[test]
fn java_canonical_text_keeps_full_scale_without_exponent_sized_output() {
    for (coefficient, scale, expected) in [
        (123, 2, "1.23"), (12300, 4, "1.2300"), (1, 6, "0.000001"), (1, 7, "1E-7"),
        (-123, -1, "-1.23E+3"), (0, 3, "0.000"), (0, -1, "0E+1"),
        (1, i32::MAX, "1E-2147483647"), (1, i32::MIN, "1E+2147483648"),
        (0, i32::MIN, "0E+2147483648"), (0, i32::MAX, "0E-2147483647"),
    ] {
        let number = decimal(coefficient, scale);
        assert_eq!(decimal_text(&number), expected);
        assert_eq!(cast(&V::Decimal(number), &T::Scalar(S::Utf8), "UTC").unwrap(), V::Utf8(expected.into()));
    }
    assert_eq!(decimal_to_f64(&decimal(1, i32::MIN)), f64::INFINITY);
    assert_eq!(decimal_to_f64(&decimal(-1, i32::MAX)).to_bits(), (-0.0f64).to_bits());
}

#[test]
fn java_long_value_truncates_and_wraps_without_scale_sized_work() {
    for (coefficient, scale, expected) in [(125, 2, 1), (-125, 2, -1), (1, i32::MAX, 0), (1, i32::MIN, 0), (1, -64, 0), (1, -63, i64::MIN), ((1i128 << 64) + 7, 0, 7)] {
        let number = decimal(coefficient, scale);
        assert_eq!(decimal_long_value(&number), expected);
    }
}

#[test]
fn clojure_decimal_integer_casts_follow_checked_double_conversion() {
    assert_eq!(cast(&value(9_007_199_254_740_993, 0), &T::Scalar(S::I64), "UTC").unwrap(), V::I64(9_007_199_254_740_992));
    assert_eq!(cast(&value(i128::from(i64::MAX), 0), &T::Scalar(S::I64), "UTC").unwrap(), V::I64(i64::MAX));
    assert_eq!(cast(&value(1, i32::MAX), &T::Scalar(S::I64), "UTC").unwrap(), V::I64(0));
    assert!(cast(&value(1, i32::MIN), &T::Scalar(S::I64), "UTC").is_err());
    assert!(cast(&value(1i128 << 64, 0), &T::Scalar(S::I64), "UTC").is_err());
}

#[test]
fn half_even_rescaling_bounds_coefficients_and_preserves_zero_scale() {
    for (coefficient, expected) in [(125, 12), (135, 14), (-125, -12), (-135, -14)] {
        assert_eq!(decimal_rescale(&decimal(coefficient, 1001), 32, 1000, 128).unwrap(), decimal(expected, 1000));
    }
    assert_eq!(decimal_rescale(&decimal(1, i32::MAX), 32, 0, 128).unwrap(), decimal(0, 0));
    assert_eq!(decimal_rescale(&decimal(0, i32::MIN), 32, i32::MAX, 128).unwrap(), decimal(0, i32::MAX));
    assert!(decimal_rescale(&decimal(1, i32::MAX), 32, i32::MIN, 128).is_err());
    assert!(decimal_rescale(&decimal(1, i32::MIN), 32, 0, 128).is_err());
    assert!(decimal_rescale(&decimal(1, 0), 32, i32::MAX, 128).is_err());
    assert!(decimal_rescale(&decimal(0, 0), 0, 2000, 128).is_err());
}

#[test]
fn wide_scale_comparisons_keep_exact_order_and_cohort_equality() {
    for scale in [i32::MIN, -1000, 1000, i32::MAX - 1] {
        assert_eq!(sql_eq(&value(1, scale), &value(10, scale + 1), &equality_scope()).unwrap(), Some(true));
        assert_eq!(compare(&value(-1, scale), &value(-9, scale + 1), false).unwrap(), Ordering::Less);
    }
    assert_eq!(compare(&value(1, i32::MIN), &value(1, i32::MAX), false).unwrap(), Ordering::Greater);
    assert_eq!(sql_eq(&value(0, i32::MIN), &value(0, i32::MAX), &equality_scope()).unwrap(), Some(true));
}

#[test]
fn decimal_arithmetic_uses_exact_bounded_coefficients_and_declared_scales() {
    for (op, left, right, expected) in [
        (B::Add, value(1, -1000), value(1, -997), value(1001, -997)),
        (B::Subtract, value(1, i32::MAX), value(1, i32::MAX), value(0, i32::MAX)),
        (B::Multiply, value(12, -1000), value(3, 2000), value(36, 1000)),
        (B::Divide, value(1, 200), value(2, -200), value(0, 6)),
        (B::Divide, value(1, 0), value(128, 0), value(7812, 6)),
        (B::Divide, value(3, 0), value(128, 0), value(23438, 6)),
        (B::Modulo, value(15, 1000), value(4, 1000), value(3, 1000)),
    ] {
        let expression = Expr::binary(op, Expr::Literal(left.clone()), Expr::Literal(right.clone()));
        assert_eq!(expression.data_type(), expected.logical_type());
        assert_eq!(arithmetic(op, &left, &right).unwrap(), expected);
    }
    assert!(arithmetic(B::Add, &value(1, i32::MIN), &value(1, i32::MAX)).is_err());
    assert!(arithmetic(B::Divide, &value(1, i32::MAX), &value(1, i32::MIN)).is_err());
    let overflow = Expr::binary(B::Multiply, Expr::Literal(value(0, i32::MAX)), Expr::Literal(value(0, 1)));
    assert!(CompiledExpr::compile(&overflow, &Schema::default()).is_err());
    assert!(arithmetic(B::Multiply, &value(0, i32::MAX), &value(0, 1)).is_err());
}

#[test]
fn modulo_keeps_lub_metadata_and_remainder_scale_write_failures() {
    let expected = V::Decimal(DecimalValue::new(0i128.to_be_bytes().to_vec(), 33, 1001, 128).unwrap());
    let left = value(5, 1000); let right = value(2, 1001);
    assert_eq!(Expr::binary(B::Modulo, Expr::Literal(left.clone()), Expr::Literal(right.clone())).data_type(), expected.logical_type());
    assert_eq!(arithmetic(B::Modulo, &left, &right).unwrap(), expected);
    assert!(arithmetic(B::Modulo, &value(4, 1000), &value(2, 1001)).is_err());
    assert!(arithmetic(B::Modulo, &value(-4, 1000), &value(2, 1001)).is_err());
    assert!(arithmetic(B::Modulo, &value(0, 1000), &value(2, 1001)).is_err());
    assert!(arithmetic(B::Modulo, &value(1, 1000), &value(20, 1001)).is_err());
    assert_eq!(arithmetic(B::Modulo, &value(-1, 1000), &value(20, 1001)).unwrap(),
        V::Decimal(DecimalValue::new(10i128.to_be_bytes().to_vec(), 33, 1001, 128).unwrap()));
    assert_eq!(arithmetic(B::Modulo, &value(1, i32::MIN), &V::I64(1)).unwrap(), value(0, i32::MIN));
    assert_eq!(arithmetic(B::Modulo, &value(-1, i32::MIN), &V::I64(1)).unwrap(), value(0, i32::MIN));
    assert_eq!(arithmetic(B::Modulo, &value(0, i32::MIN), &V::I64(3)).unwrap(), value(0, i32::MIN));
    assert_eq!(arithmetic(B::Modulo, &value(0, i32::MAX), &V::I64(-3)).unwrap(), value(0, i32::MAX));
    assert!(arithmetic(B::Modulo, &V::I64(0), &value(1, -1000)).is_err());
    assert!(arithmetic(B::Modulo, &value(1, i32::MIN), &value(3, i32::MAX)).is_err());
    assert!(arithmetic(B::Modulo, &value(-1, i32::MIN), &value(3, i32::MAX)).is_err());
}

#[test]
fn round_changes_decimal_metadata_and_rejects_dynamic_integer_scales() {
    let context = EvalContext::default();
    for (source, scale, expected) in [(value(125, 1001), 1000, value(13, 1000)), (value(-125, 1001), 1000, value(-13, 1000)), (value(1, i32::MAX), 0, value(0, 0))] {
        let args = vec![Expr::Literal(source.clone()), Expr::Literal(V::I64(scale))];
        assert_eq!(function_type_with_args("round", &args).unwrap(), expected.logical_type());
        assert_eq!(scalar_call("round", &[source, V::I64(scale)], &context).unwrap(), expected);
    }
    assert_eq!(scalar_call("floor", &[value(-125, 2)], &context).unwrap(), V::F64(-2.0));
    assert_eq!(scalar_call("ceil", &[value(125, 2)], &context).unwrap(), V::F64(2.0));
    let scale = Expr::Parameter { index: 0, data_type: T::Scalar(S::I64) };
    for (argument, input) in [(value(125, 2), "DECIMAL"), (V::I64(125), "INTEGER")] {
        let error = function_type_with_args("round", &[Expr::Literal(argument), scale.clone()]).unwrap_err();
        assert_eq!(error.code, "xtdb.expression/non-literal-scale");
        assert_eq!(error.message.as_deref(), Some(format!("ROUND({input}, INTEGER) with non-constant scale is not supported. Use ROUND(val::DOUBLE, scale) for dynamic scales.").as_str()));
    }
}

#[test]
fn round_of_a_double_adds_a_half_and_truncates_as_the_reference_does() {
    let context = EvalContext::default();
    for (input, expected) in [
        (0.49999999999999994, 1.0), (-0.49999999999999994, -1.0), (4503599627370497.0, 4503599627370498.0),
        (-4503599627370497.0, -4503599627370498.0), (0.5, 1.0), (1.5, 2.0), (2.5, 3.0), (-2.5, -3.0), (0.4, 0.0), (-0.4, -0.0),
    ] {
        assert_eq!(scalar_call("round", &[V::F64(input)], &context).unwrap(), V::F64(expected), "{input}");
    }
    let V::F64(zero) = scalar_call("round", &[V::F64(-0.0)], &context).unwrap() else { panic!() };
    assert!(zero == 0.0 && zero.is_sign_positive());
    assert_eq!(scalar_call("round", &[V::F64(0.49999999999999994), V::I64(0)], &context).unwrap(), V::F64(1.0));
    assert_eq!(scalar_call("round", &[V::F64(-25.0), V::I64(-1)], &context).unwrap(), V::F64(-30.0));
    assert!(matches!(scalar_call("round", &[V::F64(f64::NAN)], &context).unwrap(), V::F64(value) if value.is_nan()));
}

fn equality_scope() -> rtdb_expr::EvaluationScope {
    rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap()
}
