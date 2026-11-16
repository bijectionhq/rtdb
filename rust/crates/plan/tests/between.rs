use rtdb_plan::{BinaryOp as B,Expr,function_type_with_args};
use rtdb_types::{LogicalType as L,ScalarType as S,TimeUnit,Value as V};
fn int(n: i64) -> Expr { Expr::Literal(V::I64(n)) }

#[test]
fn simple_subjects_remain_inline_for_predicate_pushdown() {
    for operand in [int(7),Expr::Parameter { index: 0,data_type: L::Scalar(S::I64) }] {
        assert!(matches!(Expr::between(operand.clone(),int(1),int(9),false),Expr::Binary { op: B::And,.. }));
        assert!(matches!(Expr::between(operand,int(9),int(1),true),Expr::Binary { op: B::Or,.. }));
    }
}

#[test]
fn complex_subject_and_both_bounds_participate_in_metadata_and_serialization() {
    let args = vec![Expr::Literal(V::Duration { value: 1,unit: TimeUnit::Millisecond })];
    let sleep = Expr::Call { name: "sleep".into(),return_type: function_type_with_args("sleep",&args).unwrap(),args };
    let expr = Expr::between(sleep,Expr::Parameter { index: 1,data_type: L::Scalar(S::I64) },Expr::Parameter { index: 4,data_type: L::Scalar(S::I64) },true);
    assert!(matches!(expr,Expr::Between { symmetric: true,.. }));
    assert_eq!(expr.parameter_count(),5);assert!(expr.is_volatile());assert!(expr.requires_suspension());
    assert_eq!(expr.data_type(),L::Scalar(S::Bool).maybe());
    assert_eq!(serde_json::from_slice::<Expr>(&serde_json::to_vec(&expr).unwrap()).unwrap(),expr);
}

#[test]
fn ordered_predicate_admission_keeps_generic_pairs_and_rejects_unregistered_heads() {
    use rtdb_plan::validate_ordered_predicate_types as admit;
    use rtdb_types::{IntervalUnit,ListKind};
    assert!(admit(&L::Scalar(S::I64),&L::Scalar(S::Utf8)).is_ok());
    assert!(admit(&L::Scalar(S::F16),&L::Null).is_ok());
    assert!(admit(&L::Nothing,&L::Scalar(S::F16)).is_ok());
    for ty in [L::Scalar(S::F16),L::List { kind: ListKind::FixedSizeList(1),element: Box::new(L::Scalar(S::I64)) }] {
        assert_eq!(admit(&ty,&ty).unwrap_err().code,"xtdb.expression/function-type-mismatch");
        let expr = Expr::between(Expr::Coalesce(vec![Expr::Parameter { index: 0,data_type: ty.clone() }]),Expr::Parameter { index: 1,data_type: ty.clone() },int(1),false);
        assert_eq!(expr.validate_columns(&Default::default()).unwrap_err().code,"xtdb.expression/function-type-mismatch");
        let expr = Expr::binary(B::Lt,Expr::Parameter { index: 0,data_type: ty },int(1));
        assert_eq!(expr.validate_columns(&Default::default()).unwrap_err().code,"xtdb.expression/function-type-mismatch");
    }
    assert_eq!(admit(&L::Scalar(S::Interval(IntervalUnit::YearMonth)),&L::Scalar(S::Interval(IntervalUnit::MonthDayNano))).unwrap_err().code,"xtdb.expression/cannot-compare-different-unit-intervals");
}
