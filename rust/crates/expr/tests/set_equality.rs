use std::collections::BTreeMap;
use rtdb_expr::{CompiledExpr, EvalContext, evaluate, evaluate_async, scalar_call, sql_eq, sql_not_eq_in_timezone, strict_eq};
use rtdb_plan::{BinaryOp as B, Expr, Schema};
use rtdb_types::{ListKind, LogicalType as L, ScalarType as S, Value as V};

fn set() -> V { V::Set(vec![]) }
fn set_type() -> L { L::List { kind: ListKind::Set,element: Box::new(L::Scalar(S::I64)) } }

#[test]
fn direct_sql_equality_rejects_sets_and_union_child_pairs_before_length_or_value_checks() {
    for (left,right) in [
        (set(),set()),
        (V::List(vec![set()]),V::List(vec![set(),V::I64(1)])),
        (V::List(vec![V::I64(1),set()]),V::List(vec![set(),V::I64(2)])),
        (V::Struct(BTreeMap::from([("a".into(),set())])),V::Struct(BTreeMap::from([("a".into(),set())]))),
    ] {
        assert_eq!(sql_eq(&left,&right).unwrap_err().code,"xtdb.expression/set-equality");
        assert!(sql_not_eq_in_timezone(&left,&right,"UTC").is_err());
        for name in ["distinct_from","null_eq"] { assert!(scalar_call(name,&[left.clone(),right.clone()],&EvalContext::default()).is_err()); }
    }
    assert_eq!(sql_eq(&V::Null,&set()).unwrap(),None);
    assert_eq!(sql_eq(&set(),&V::I64(1)).unwrap(),Some(false));
    assert_eq!(sql_eq(&V::Struct(BTreeMap::from([("a".into(),set())])),&V::Struct(BTreeMap::from([("b".into(),set())]))).unwrap(),Some(false));
    assert!(strict_eq(&set(),&set()).unwrap());
}

#[tokio::test]
async fn declared_set_pairs_reject_empty_lists_and_unselected_branches() {
    let empty = Expr::Parameter { index: 0,data_type: L::list(set_type()) };
    let expression = Expr::binary(B::Eq,empty.clone(),empty);
    let arguments = [V::List(vec![])];
    let context = EvalContext { parameters: &arguments,..Default::default() };
    assert!(CompiledExpr::compile(&expression,&Schema::default()).is_err());
    assert_eq!(evaluate(&expression,&Schema::default(),&[],&context).unwrap_err().code,"xtdb.expression/set-equality");
    assert!(evaluate_async(&expression,&Schema::default(),&[],&context).await.is_err());
    let hidden = Expr::Case { branches: vec![(Expr::Literal(V::Bool(false)),expression)],otherwise: Box::new(Expr::Literal(V::Bool(true))) };
    assert!(evaluate_async(&hidden,&Schema::default(),&[],&context).await.is_err());
    let union = set_type().join(&L::Scalar(S::I64));
    let expression = Expr::binary(B::Eq,Expr::Parameter { index: 0,data_type: union.clone() },Expr::Parameter { index: 0,data_type: union });
    assert!(evaluate(&expression,&Schema::default(),&[],&EvalContext { parameters: &[V::I64(1)],..Default::default() }).is_err());
}
