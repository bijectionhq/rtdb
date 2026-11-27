use rtdb_expr::{EvalContext, function_type, scalar_call};
use rtdb_types::{DecimalValue, LogicalType as L, ScalarType as S, Value as V};

fn call(name: &str, args: &[V]) -> rtdb_types::Result<V> { scalar_call(name, args, &EvalContext::default()) }

#[test]
fn numeric_catalogue_dispatches_logarithm_hyperbolic_double_and_modulo_calls() {
    for (name, input, expected) in [("log10", V::I64(150), 2.1760912590556813), ("sinh", V::I32(0), 0.0), ("cosh", V::F32(0.0), 1.0), ("tanh", V::F64(f64::INFINITY), 1.0), ("double", V::I16(7), 7.0)] {
        assert_eq!(function_type(name, &[input.logical_type()]).unwrap(), L::Scalar(S::F64));
        assert_eq!(call(name, &[input]).unwrap(), V::F64(expected));
        assert_eq!(function_type(name, &[L::Null]).unwrap(), L::Null);
        assert_eq!(call(name, &[V::Null]).unwrap(), V::Null);
        assert!(function_type(name, &[L::Scalar(S::Utf8)]).is_err());
    }
    for (left, right, expected) in [(-5, 3, 1), (5, -3, -1), (-5, -3, -2)] {
        assert_eq!(call("mod", &[V::I64(left), V::I64(right)]).unwrap(), V::I64(expected));
    }
    assert_eq!(call("mod", &[V::I64(1), V::I64(0)]).unwrap_err().code, "xtdb.expression/division-by-zero");
    assert_eq!(call("mod", &[V::Null, V::I64(0)]).unwrap(), V::Null);
    assert!(function_type("mod", &[L::Scalar(S::I64)]).is_err());
    let decimal = V::Decimal(DecimalValue::new(25i128.to_be_bytes().to_vec(), 10, 1, 128).unwrap());
    assert_eq!(call("double", &[decimal]).unwrap(), V::F64(2.5));
}

#[test]
fn log_requires_base_and_operand_as_in_xtdb() {
    assert_eq!(function_type("log", &[L::Scalar(S::F64)]).unwrap_err().code, "xtdb/invalid-function-arity");
    assert_eq!(function_type("log", &[L::Null]).unwrap_err().code, "xtdb/invalid-function-arity");
    assert_eq!(call("log", &[V::F64(100.0)]).unwrap_err().code, "xtdb/invalid-function-arity");
    assert_eq!(call("log", &[V::Null]).unwrap_err().code, "xtdb/invalid-function-arity");
    assert_eq!(function_type("log", &[L::Scalar(S::F64), L::Scalar(S::F64)]).unwrap(), L::Scalar(S::F64));
    assert_eq!(call("log", &[V::F64(2.0), V::F64(8.0)]).unwrap(), V::F64(3.0));
}

#[test]
fn hyperbolic_calls_preserve_signed_zero_nan_and_infinite_limits() {
    for name in ["sinh", "tanh", "double"] {
        let V::F64(value) = call(name, &[V::F64(-0.0)]).unwrap() else { panic!() };
        assert_eq!(value.to_bits(), (-0.0f64).to_bits());
    }
    for name in ["log10", "sinh", "cosh", "tanh", "double"] {
        let V::F64(value) = call(name, &[V::F64(f64::NAN)]).unwrap() else { panic!() };
        assert!(value.is_nan());
    }
    assert_eq!(call("sinh", &[V::F64(f64::NEG_INFINITY)]).unwrap(), V::F64(f64::NEG_INFINITY));
    assert_eq!(call("cosh", &[V::F64(f64::NEG_INFINITY)]).unwrap(), V::F64(f64::INFINITY));
}

#[test]
fn floating_mod_matches_primitive_reference_results_and_errors() {
    let modulus = |a, b| call("mod", &[V::F64(a), V::F64(b)]);
    for divisor in [0.0, -0.0] {
        assert_eq!(modulus(7.0, divisor).unwrap_err().code, "xtdb.expression/unknown-arithmetic-error");
    }
    for dividend in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(modulus(dividend, 2.0).unwrap_err().code, "xtdb.expression/number-format-error");
    }
    for (a, b, expected) in [(7.5, -3.0, -1.5f64), (-7.5, 3.0, 1.5), (-0.0, 3.0, -0.0), (1e30, 3.0, 0.0)] {
        let V::F64(actual) = modulus(a, b).unwrap() else { panic!("Expected floating remainder"); };
        assert_eq!(actual.to_bits(), expected.to_bits());
    }
    assert_eq!(call("mod", &[V::F32(-7.5), V::I16(3)]).unwrap(), V::F32(1.5));
    assert_eq!(call("mod", &[V::I64(i64::MIN), V::I64(-1)]).unwrap(), V::I64(0));
}

#[test]
fn abs_keeps_each_numeric_leg_of_a_union_argument() {
    let union = L::join_all(&[L::Scalar(S::I64), L::Scalar(S::F64), L::Null]);
    let result = function_type("abs", std::slice::from_ref(&union)).unwrap();
    for (input, expected) in [(V::I64(-2), V::I64(2)), (V::F64(-3.4), V::F64(3.4)), (V::Null, V::Null)] {
        let output = call("abs", &[input]).unwrap();
        assert!(result.legs().contains(&output.logical_type()), "{output:?} in {result:?}");
        assert_eq!(output, expected);
    }
}
