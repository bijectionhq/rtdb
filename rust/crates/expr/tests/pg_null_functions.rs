use rtdb_expr::{EvalContext, function_type, scalar_call};
use rtdb_types::{LogicalType as L, ScalarType as S, Value as V};

#[test]
fn postgres_description_and_access_method_property_functions_return_null() {
    for args in [vec![V::I64(12345), V::I64(1)], vec![V::Oid(12345), V::I16(1)], vec![V::Null, V::Null]] {
        assert_eq!(function_type("col_description", &args.iter().map(V::logical_type).collect::<Vec<_>>()).unwrap(), L::Null);
        assert_eq!(scalar_call("col_description", &args, &EvalContext::default()).unwrap(), V::Null);
    }
    assert_eq!(scalar_call("pg_indexam_has_property", &[V::I32(403), V::Utf8("can_order".into())], &EvalContext::default()).unwrap(), V::Null);
    for (name, types) in [
        ("col_description", vec![L::Scalar(S::Utf8), L::Scalar(S::I64)]),
        ("col_description", vec![L::Scalar(S::I64), L::Scalar(S::F64)]),
        ("pg_indexam_has_property", vec![L::Scalar(S::Oid), L::Scalar(S::Utf8)]),
        ("pg_indexam_has_property", vec![L::Scalar(S::I64), L::Scalar(S::I64)]),
    ] { assert_eq!(function_type(name, &types).unwrap_err().code, "xtdb.expression/function-type-mismatch"); }
    assert_eq!(function_type("col_description", &[L::Scalar(S::I64)]).unwrap_err().code, "xtdb/invalid-function-arity");
}
