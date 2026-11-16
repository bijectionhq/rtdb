use std::time::Duration;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate,evaluate_async,scalar_call};
use rtdb_plan::{BinaryOp as B,CastTarget,Expr,Schema,function_type_with_args};
use rtdb_types::{LogicalType as L,Result,ScalarType as S,TimeUnit,Value as V};

fn value(value: V) -> Expr { Expr::Literal(value) }
fn text(text: &str) -> Expr { value(V::Utf8(text.into())) }
fn null() -> Expr { value(V::Null) }
fn divide() -> Expr { Expr::binary(B::Divide,value(V::I64(1)),value(V::I64(0))) }
fn as_text(expr: Expr) -> Expr { Expr::Cast { expr: Box::new(expr),target: CastTarget::fixed(L::Scalar(S::Utf8)) } }
fn branch(condition: Expr,yes: Expr,no: Expr) -> Expr { Expr::Case { branches: vec![(condition,yes)],otherwise: Box::new(no) } }
fn call(name: &str,args: Vec<Expr>) -> Expr { Expr::Call { name: name.into(),return_type: function_type_with_args(name,&args).unwrap(),args } }
fn search(input: Expr,pattern: Expr,flags: Expr) -> Expr { call("like_regex",vec![input,pattern,flags]) }
fn run(expr: &Expr) -> Result<V> { evaluate(expr,&Schema::default(),&[],&EvalContext::default()) }

#[test]
fn regex_patterns_compile_before_input_payload_and_null_selection_discards_both() {
    assert_eq!(run(&search(as_text(divide()),text("["),text(""))).unwrap_err().code,"xtdb.expression/illegal-argument-exception");
    assert_eq!(run(&search(as_text(divide()),text("."),text(""))).unwrap_err().code,"xtdb.expression/division-by-zero");
    assert_eq!(run(&search(null(),text("["),text(""))).unwrap(),V::Null);
    assert_eq!(run(&search(as_text(divide()),text("["),null())).unwrap(),V::Null);
    assert_eq!(run(&call("like_regex",vec![as_text(divide()),null()])).unwrap(),V::Null);
    let forced = branch(Expr::binary(B::Eq,divide(),value(V::I64(0))),text("a"),text("b"));
    assert_eq!(run(&search(forced,text("["),null())).unwrap_err().code,"xtdb.expression/division-by-zero");
    let boxed = branch(value(V::Bool(true)),as_text(divide()),null());
    assert_eq!(run(&search(boxed,text("["),null())).unwrap_err().code,"xtdb.expression/division-by-zero");
}

#[test]
fn regex_flags_use_literal_metadata_and_keep_nonliteral_type_selection() {
    assert_eq!(run(&search(text("A"),text("a"),text("i"))).unwrap(),V::Bool(true));
    for flags in [as_text(divide()),call("concat",vec![text("i"),text("")]),branch(value(V::Bool(true)),text("i"),text(""))] {
        assert_eq!(run(&search(text("A"),text("a"),flags)).unwrap(),V::Bool(false));
    }
    let forced = branch(Expr::binary(B::Eq,divide(),value(V::I64(0))),text("i"),text(""));
    assert_eq!(run(&search(text("A"),text("["),forced.clone())).unwrap_err().code,"xtdb.expression/division-by-zero");
    assert_eq!(run(&search(null(),text("["),forced)).unwrap(),V::Null);
    let boxed = branch(value(V::Bool(true)),as_text(divide()),null());
    assert_eq!(run(&search(text("A"),text("a"),boxed)).unwrap_err().code,"xtdb.expression/division-by-zero");
    assert_eq!(scalar_call("like_regex",&[V::Utf8("A".into()),V::Utf8("a".into()),V::Utf8("i".into())],&EvalContext::default()).unwrap(),V::Bool(true));
    assert_eq!(scalar_call("like_regex",&[V::Null],&EvalContext::default()).unwrap(),V::Null);
}

#[test]
fn like_compiles_selected_pattern_before_input_payload() {
    let oversized = text(&"a".repeat(65_537));
    for name in ["like","ilike"] {
        assert_eq!(run(&call(name,vec![as_text(divide()),oversized.clone()])).unwrap_err().code,"rtdb.expr/regex-limit");
    }
    let expression = Expr::Like { expr: Box::new(as_text(divide())),pattern: Box::new(text("!")),escape: Some('!'),case_insensitive: false,negated: false };
    assert_eq!(run(&expression).unwrap_err().code,"xtdb/invalid-like-pattern");
}

#[tokio::test(start_paused = true)]
async fn compiled_pattern_is_owned_by_the_cancelled_input_continuation() {
    let memory = MemoryBudget::new(128 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let sleep = call("sleep",vec![value(V::Duration { value: i64::MAX,unit: TimeUnit::Millisecond })]);
        let input = call("format",vec![text("%s"),sleep]);
        let expression = search(input,text("a"),text(""));
        assert!(tokio::time::timeout(Duration::from_millis(1),evaluate_async(&expression,&Schema::default(),&[],&context)).await.is_err());
        assert!(memory.used_bytes() > 0);
    }
    assert_eq!(memory.used_bytes(),0);
}
