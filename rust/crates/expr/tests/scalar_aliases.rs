use rtdb_expr::{EvalContext, evaluate, evaluate_async, scalar_call};
use rtdb_plan::{BinaryOp as B, CastTarget, Expr, Schema, function_type};
use rtdb_types::{LogicalType as L, ScalarType as S, TimeUnit as U, Value as V};

fn call(name: &str,args: Vec<Expr>) -> Expr {
    Expr::Call { name: name.into(),return_type: function_type(name,&args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap(),args }
}

#[test]
fn scalar_aliases_keep_null_and_nested_distinctness_and_temporal_metadata_units() {
    let context = EvalContext::default();
    for (name,args,expected) in [
        ("is_true",vec![V::Bool(true)],V::Bool(true)),("is-false",vec![V::Bool(false)],V::Bool(true)),
        ("is_true",vec![V::I64(1)],V::Bool(false)),("is_false",vec![V::Null],V::Bool(false)),
        ("distinct-from",vec![V::Null,V::Null],V::Bool(false)),("distinct_from",vec![V::Null,V::I64(1)],V::Bool(true)),
        ("distinct_from",vec![V::I64(1),V::I32(1)],V::Bool(false)),
        ("distinct_from",vec![V::list(vec![V::Null]),V::list(vec![V::Null])],V::Null),
        ("_meta_double",vec![V::I64(-3)],V::F64(-3.0)),("_meta-double",vec![V::DateDay(-1)],V::F64(-86_400.0)),
        ("_meta_double",vec![V::DateMilli(86_400_001)],V::F64(86_400.0)),
        ("_meta_double",vec![V::Timestamp { value: 1234,unit: U::Millisecond,timezone: Some("UTC".into()) }],V::F64(1.234)),
        ("_meta_double",vec![V::Time { value: 1234,unit: U::Microsecond,bit_width: 64 }],V::F64(0.001234)),
        ("_meta_double",vec![V::Duration { value: -500_000_000,unit: U::Nanosecond }],V::F64(-0.5)),
        ("_meta_double",vec![V::Utf8("12".into())],V::Null),("_meta_double",vec![V::list(vec![V::I64(1)])],V::Null),
    ] { assert_eq!(scalar_call(name,&args,&context).unwrap(),expected,"{name}"); }
    let source = L::Scalar(S::I64).join(&L::Scalar(S::Utf8));
    assert_eq!(function_type("_meta_double",&[source]).unwrap(),L::Scalar(S::F64).maybe());
    assert!(function_type("_meta_double",&[L::Scalar(S::F16)]).is_err());
    assert!(scalar_call("_meta_double",&[V::F16(0)],&context).is_err());
    for (name,args) in [("is_true",vec![]),("is_false",vec![L::Null,L::Null]),("distinct_from",vec![L::Null]),("_meta_double",vec![])] {
        assert!(function_type(name,&args).is_err());
    }
}

#[tokio::test]
async fn scalar_aliases_select_types_and_nulls_before_payloads_but_force_controls_and_boxes() {
    let divide = Expr::binary(B::Divide,Expr::Literal(V::I64(1)),Expr::Literal(V::I64(0)));
    let text = Expr::Cast { expr: Box::new(divide.clone()),target: CastTarget::Fixed(L::Scalar(S::Utf8)) };
    for (expression,expected) in [
        (call("is_true",vec![divide.clone()]),V::Bool(false)),
        (call("is_false",vec![divide.clone()]),V::Bool(false)),
        (call("_meta_double",vec![text.clone()]),V::Null),
        (call("distinct_from",vec![divide.clone(),Expr::Literal(V::Null)]),V::Bool(true)),
        (call("distinct_from",vec![Expr::Literal(V::Null),divide.clone()]),V::Bool(true)),
        (call("distinct_from",vec![Expr::List(vec![Expr::Literal(V::Null)]),Expr::List(vec![Expr::Literal(V::Null)])]),V::Null),
    ] {
        assert_eq!(evaluate(&expression,&Schema::default(),&[],&EvalContext::default()).unwrap(),expected);
        assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap(),expected);
    }
    for expression in [
        call("_meta_double",vec![Expr::Case { branches: vec![(Expr::Literal(V::Bool(true)),text)],otherwise: Box::new(Expr::Literal(V::I64(1))) }]),
        call("is_true",vec![Expr::Case { branches: vec![(Expr::binary(B::Eq,divide,Expr::Literal(V::I64(0))),Expr::Literal(V::I64(1)))],otherwise: Box::new(Expr::Literal(V::I64(2))) }]),
    ] {
        assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap_err().code,"xtdb.expression/division-by-zero");
    }
}
