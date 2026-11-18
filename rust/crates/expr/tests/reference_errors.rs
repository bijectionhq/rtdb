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
    assert_eq!(error(to(S::F64, " ")), number_format("empty String"));
    assert_eq!(error(to(S::F64, "inf")), number_format("For input string: \"inf\""));
    assert_eq!(to(S::F64, " 1.5d\n").unwrap(), V::F64(1.5));
    assert_eq!(to(S::F64, "-Infinity").unwrap(), V::F64(f64::NEG_INFINITY));
}
