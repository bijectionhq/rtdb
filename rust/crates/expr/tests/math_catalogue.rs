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
