use std::collections::BTreeMap;
use rtdb_plan::{BinaryOp, Expr, function_type, validate_equality_types};
use rtdb_types::{ListKind, LogicalType as L, ScalarType as S};

fn set() -> L { L::List { kind: ListKind::Set, element: Box::new(L::Scalar(S::I64)) } }
fn record(name: &str, value: L) -> L { L::Struct(BTreeMap::from([(name.into(),value)])) }

#[test]
fn set_equality_admits_all_nested_declared_pairs_before_values_are_available() {
    let set = set();
    for (left,right) in [
        (set.clone(),set.clone()),
        (set.clone().maybe(),set.clone()),
        (set.join(&L::Scalar(S::I64)),set.clone()),
        (L::list(set.clone()),L::list(set.clone())),
        (record("a",L::list(set.clone())),record("a",L::list(set.clone()))),
    ] {
        assert_eq!(validate_equality_types(&left,&right).unwrap_err().code,"xtdb.expression/set-equality");
        for op in [BinaryOp::Eq,BinaryOp::NotEq,BinaryOp::IsDistinctFrom,BinaryOp::IsNotDistinctFrom] {
            let expression = Expr::binary(op,Expr::Parameter { index: 0,data_type: left.clone() },Expr::Parameter { index: 1,data_type: right.clone() });
            assert!(expression.validate_columns(&Default::default()).is_err());
        }
        for name in ["null_eq","distinct_from","nullif"] { assert!(function_type(name,&[left.clone(),right.clone()]).is_err()); }
    }
    for (left,right) in [(L::Null,set.clone()),(set.clone(),L::Scalar(S::I64)),(L::list(L::Nothing),L::list(set.clone())),(record("a",set.clone()),record("b",set.clone()))] {
        validate_equality_types(&left,&right).unwrap();
    }
    function_type("===",&[set.clone(),set]).unwrap();
}
