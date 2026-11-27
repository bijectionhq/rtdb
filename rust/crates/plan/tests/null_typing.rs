use rtdb_plan::{BinaryOp as B, Expr, UnaryOp as U, function_type, function_type_with_args};
use rtdb_types::{LogicalType as L, PgType, ScalarType as S, Value as V};

#[test]
fn strict_comparisons_and_not_with_pure_null_report_null_type_and_text_oid() {
    let null = Expr::Literal(V::Null);
    let one = Expr::Literal(V::I64(1));
    let nullable_bool = Expr::Parameter { index: 0, data_type: L::Scalar(S::Bool).maybe() };
    let nullable_int = Expr::Parameter { index: 0, data_type: L::Scalar(S::I64).maybe() };

    for expression in [
        Expr::unary(U::Not, null.clone()),
        Expr::binary(B::Eq, null.clone(), null.clone()),
        Expr::binary(B::Eq, null.clone(), one.clone()),
        Expr::binary(B::NotEq, one.clone(), null.clone()),
        Expr::binary(B::Lt, null.clone(), one.clone()),
        Expr::binary(B::LtEq, one.clone(), null.clone()),
        Expr::binary(B::Gt, null.clone(), one.clone()),
        Expr::binary(B::GtEq, one.clone(), null.clone()),
    ] {
        assert_eq!(expression.data_type(), L::Null, "{expression:?}");
        assert_eq!(PgType::from_logical(&expression.data_type()).unwrap().descriptor().oid, 25, "{expression:?}");
    }

    assert_eq!(Expr::unary(U::Not, nullable_bool).data_type(), L::Scalar(S::Bool).maybe());
    assert_eq!(Expr::binary(B::Eq, nullable_int.clone(), one.clone()).data_type(), L::Scalar(S::Bool).maybe());
    assert_eq!(Expr::binary(B::Lt, nullable_int, one.clone()).data_type(), L::Scalar(S::Bool).maybe());
    assert_eq!(Expr::binary(B::And, null.clone(), Expr::Literal(V::Bool(true))).data_type(), L::Scalar(S::Bool).maybe());
    assert_eq!(Expr::binary(B::Or, null.clone(), Expr::Literal(V::Bool(false))).data_type(), L::Scalar(S::Bool).maybe());
    assert_eq!(Expr::binary(B::IsDistinctFrom, null.clone(), null.clone()).data_type(), L::Scalar(S::Bool));
    assert_eq!(Expr::binary(B::IsNotDistinctFrom, null, one).data_type(), L::Scalar(S::Bool));
}

#[test]
fn strict_scalar_functions_with_pure_null_report_null_type_and_text_oid() {
    let int = L::Scalar(S::I64);
    let maybe_int = int.clone().maybe();

    for (name, args) in [
        ("least", vec![int.clone(), L::Null, int.clone()]),
        ("greatest", vec![int.clone(), L::Null, int.clone()]),
        ("length", vec![L::Null]),
        ("cardinality", vec![L::Null]),
        ("floor", vec![L::Null]),
        ("ceil", vec![L::Null]),
        ("ceiling", vec![L::Null]),
        ("round", vec![L::Null]),
        ("round", vec![int.clone(), L::Null]),
        ("round", vec![L::Scalar(S::I8), L::Null]),
        ("round", vec![L::Scalar(S::F64), L::Null]),
        ("power", vec![int.clone(), L::Null]),
        ("log", vec![int.clone(), L::Null]),
        ("sqrt", vec![L::Null]),
        ("exp", vec![L::Null]),
        ("ln", vec![L::Null]),
        ("sin", vec![L::Null]),
        ("keyword", vec![L::Null]),
        ("namespace", vec![L::Null]),
        ("local_name", vec![L::Null]),
        ("_iid", vec![L::Null]),
    ] {
        let ty = function_type(name, &args).unwrap();
        assert_eq!(ty, L::Null, "{name}({args:?})");
        assert_eq!(PgType::from_logical(&ty).unwrap().descriptor().oid, 25, "{name}({args:?})");
    }

    assert_eq!(function_type("least", &[int.clone(), maybe_int.clone(), int.clone()]).unwrap(), maybe_int);
    assert_eq!(PgType::from_logical(&function_type("least", &[int.clone(), maybe_int, int.clone()]).unwrap()).unwrap().descriptor().oid, 20);
    assert_eq!(function_type("nullif", &[int.clone(), L::Null]).unwrap(), int.clone().maybe());
    assert_eq!(function_type("nullif", &[L::Null, int]).unwrap(), L::Null);
    assert_eq!(function_type_with_args("round", &[Expr::Literal(V::I64(1)), Expr::Literal(V::Null)]).unwrap(), L::Null);
    assert_eq!(function_type_with_args("round", &[Expr::Literal(V::F64(1.5)), Expr::Literal(V::Null)]).unwrap(), L::Null);
    assert_eq!(function_type_with_args("round", &[Expr::Literal(V::Null), Expr::Literal(V::I64(2))]).unwrap(), L::Null);
}
