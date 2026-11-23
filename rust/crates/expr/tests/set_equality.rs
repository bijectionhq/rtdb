use rtdb_types::StructFields;
use rtdb_expr::{CompiledExpr, EvalContext, evaluate, evaluate_async, scalar_call, sql_eq, sql_not_eq_in_timezone, strict_eq};
use rtdb_plan::{BinaryOp as B, Expr, Schema};
use rtdb_types::{ListKind, LogicalType as L, ScalarType as S, Value as V};

fn set() -> V { V::Set(vec![]) }
fn set_type() -> L { L::List { kind: ListKind::Set,element: Box::new(L::Scalar(S::I64)) } }

#[test]
fn direct_sql_equality_rejects_sets_and_union_child_pairs_before_length_or_value_checks() {
    for (left,right) in [
        (set(),set()),
        (V::list(vec![set()]),V::list(vec![set(),V::I64(1)])),
        (V::list(vec![V::I64(1),set()]),V::list(vec![set(),V::I64(2)])),
        (V::Struct(StructFields::from([("a".into(),set())])),V::Struct(StructFields::from([("a".into(),set())]))),
    ] {
        assert_eq!(sql_eq(&left,&right, &equality_scope()).unwrap_err().code,"xtdb.expression/set-equality");
        assert!(sql_not_eq_in_timezone(&left,&right,"UTC", &equality_scope()).is_err());
        for name in ["distinct_from","null_eq"] { assert!(scalar_call(name,&[left.clone(),right.clone()],&EvalContext::default()).is_err()); }
    }
    assert_eq!(sql_eq(&V::Null,&set(), &equality_scope()).unwrap(),None);
    assert_eq!(sql_eq(&set(),&V::I64(1), &equality_scope()).unwrap(),Some(false));
    assert_eq!(sql_eq(&V::Struct(StructFields::from([("a".into(),set())])),&V::Struct(StructFields::from([("b".into(),set())])), &equality_scope()).unwrap(),Some(false));
    assert!(strict_eq(&set(),&set(), &equality_scope()).unwrap());
}

#[tokio::test]
async fn declared_set_pairs_reject_empty_lists_and_unselected_branches() {
    let empty = Expr::Parameter { index: 0,data_type: L::list(set_type()) };
    let expression = Expr::binary(B::Eq,empty.clone(),empty);
    let arguments = [V::list(vec![])];
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

#[tokio::test]
async fn strict_set_expressions_compare_reader_identity_without_demanding_elements() {
    fn constructed_set() -> Expr {
        let divide = Expr::binary(B::Divide, Expr::Literal(V::I64(1)), Expr::Literal(V::I64(0)));
        Expr::Call { name: "set".into(), args: vec![divide], return_type: set_type() }
    }
    for (left, right) in [
        (constructed_set(), constructed_set()),
        (Expr::Literal(set()), Expr::Literal(set())),
        (Expr::List(vec![constructed_set()]), Expr::List(vec![constructed_set()])),
        (Expr::Struct(vec![("field".into(), constructed_set())]), Expr::Struct(vec![("field".into(), constructed_set())])),
    ] {
        let expression = Expr::Call { name: "===".into(), args: vec![left, right], return_type: L::Scalar(S::Bool) };
        assert_eq!(evaluate(&expression, &Schema::default(), &[], &EvalContext::default()).unwrap(), V::Bool(false));
        assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext::default()).await.unwrap(), V::Bool(false));
    }
}

fn equality_scope() -> rtdb_expr::EvaluationScope {
    rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap()
}
