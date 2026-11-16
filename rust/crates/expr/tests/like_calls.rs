use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate,evaluate_async,scalar_call};
use rtdb_plan::{BinaryOp as B,CastTarget,Expr,Schema,function_type};
use rtdb_types::{LogicalType as L,ScalarType as S,Value as V};

fn text(value: &str) -> V { V::Utf8(value.into()) }
fn call(name: &str,args: Vec<Expr>) -> Expr {
    let return_type = function_type(name,&args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap();
    Expr::Call { name: name.into(),args,return_type }
}
fn cast(value: Expr,ty: S) -> Expr { Expr::Cast { expr: Box::new(value),target: CastTarget::fixed(L::Scalar(ty)) } }

#[test]
fn like_calls_use_java_unicode_case_line_terminators_and_binary_hex_wildcards() {
    for (name,value,pattern,expected) in [
        ("like",text("a.b"),text("a.b"),true),("like",text("axb"),text("a.b"),false),
        ("like",text("\\Qx\\E"),text("\\Qx\\E"),true),
        ("like",text("😀"),text("_"),true),("like",text(""),text("%"),true),
        ("ilike",text("ı"),text("I"),true),("ilike",text("İ"),text("i"),true),
        ("ilike",text("K"),text("k"),true),("like",text("I"),text("i"),false),
        ("like",V::Binary(vec![0,1,2]),V::Binary(vec![0,b'%',2]),true),
        ("like",V::FixedSizeBinary(vec![0,1]),V::Binary(vec![b'_',1]),true),
        ("like",V::Binary(vec![15]),V::Binary(vec![2,95]),true),
    ] { assert_eq!(scalar_call(name,&[value,pattern],&EvalContext::default()).unwrap(),V::Bool(expected)); }
    for terminator in ['\n','\r','\u{85}','\u{2028}','\u{2029}'] {
        for pattern in ["_","%"] { assert_eq!(scalar_call("like",&[text(&terminator.to_string()),text(pattern)],&EvalContext::default()).unwrap(),V::Bool(false)); }
    }
    for args in [vec![V::Bool(true),text("%")],vec![text("x"),V::Binary(vec![b'%'])]] { assert!(scalar_call("like",&args,&EvalContext::default()).is_err()); }
    assert!(scalar_call("ilike",&[V::Binary(vec![]),V::Binary(vec![])],&EvalContext::default()).is_err());
    for name in ["like","ilike"] {
        for args in [vec![],vec![V::Null],vec![V::Null,V::Null,V::Null]] { assert!(scalar_call(name,&args,&EvalContext::default()).is_err()); }
        assert_eq!(scalar_call(name,&[V::Null,V::Bool(true)],&EvalContext::default()).unwrap(),V::Null);
    }
}

#[tokio::test]
async fn like_selects_controls_before_payloads_and_demands_dynamic_patterns_before_sources() {
    let fail = cast(Expr::binary(B::Divide,Expr::Literal(V::I64(1)),Expr::Literal(V::I64(0))),S::Utf8);
    for name in ["like","ilike"] {
        let expression = call(name,vec![fail.clone(),Expr::Literal(V::Null)]);
        assert_eq!(evaluate(&expression,&Schema::default(),&[],&EvalContext::default()).unwrap(),V::Null);
        assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap(),V::Null);
        let boxed = Expr::Case { branches: vec![(Expr::Literal(V::Bool(true)),fail.clone())],otherwise: Box::new(Expr::Literal(V::Null)) };
        assert_eq!(evaluate_async(&call(name,vec![boxed,Expr::Literal(V::Null)]),&Schema::default(),&[],&EvalContext::default()).await.unwrap_err().code,"xtdb.expression/division-by-zero");
        let bad_pattern = cast(cast(Expr::Literal(text("bad integer")),S::I64),S::Utf8);
        assert_eq!(evaluate_async(&call(name,vec![fail.clone(),bad_pattern]),&Schema::default(),&[],&EvalContext::default()).await.unwrap_err().code,"xtdb/invalid-argument-types");
    }
    let expression = Expr::Like { expr: Box::new(fail),pattern: Box::new(Expr::Literal(V::Null)),escape: None,case_insensitive: false,negated: false };
    assert_eq!(expression.data_type(),L::Null);
    assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap(),V::Null);
}

#[test]
fn like_matcher_reserves_its_translation_and_matching_state_before_use() {
    let memory = MemoryBudget::new(1 << 20);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope),..Default::default() };
    assert!(scalar_call("like",&[text("a"),text("%")],&context).is_err());
    drop(scope);assert_eq!(memory.used_bytes(),0);
}
