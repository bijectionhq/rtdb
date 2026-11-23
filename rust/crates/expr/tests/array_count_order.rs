use std::time::Duration;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext, EvaluationScope, evaluate, evaluate_async, function_type};
use rtdb_plan::{BinaryOp as B, CastTarget, Expr, Schema, UnaryOp};
use rtdb_types::{LogicalType as L, ScalarType as S, TimeUnit, Value as V};

fn literal(value: i64) -> Expr { Expr::Literal(V::I64(value)) }
fn nil() -> Expr { Expr::Literal(V::Null) }
fn call(name: &str,args: Vec<Expr>) -> Expr {
    let return_type = function_type(name,&args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap();
    Expr::Call { name: name.into(),args,return_type }
}
fn trim(input: Expr,count: Expr) -> Expr { call("trim_array",vec![input,count]) }
fn oversized() -> Expr { trim(Expr::List(vec![]),literal(-2_147_483_648)) }
fn divide() -> Expr { Expr::Binary { op: B::Divide,left: Box::new(literal(1)),right: Box::new(literal(0)) } }
fn add(left: Expr,right: Expr) -> Expr { Expr::Binary { op: B::Add,left: Box::new(left),right: Box::new(right) } }
fn condition() -> Expr { Expr::Binary { op: B::Eq,left: Box::new(divide()),right: Box::new(literal(0)) } }
fn choose(condition: Expr,selected: Expr,other: Expr) -> Expr { Expr::Case { branches: vec![(condition,selected)],otherwise: Box::new(other) } }
fn yes() -> Expr { Expr::Literal(V::Bool(true)) }
fn run(expr: &Expr) -> rtdb_types::Result<V> { evaluate(expr,&Schema::default(),&[],&EvalContext::default()) }
fn error(count: Expr) -> String { run(&trim(oversized(),count)).unwrap_err().code }
fn sleep(millis: i64) -> Expr { call("sleep",vec![Expr::Literal(V::Duration { value: millis,unit: TimeUnit::Millisecond })]) }

#[test]
fn trim_observes_input_size_before_inline_arithmetic_and_cast_payloads() {
    for count in [divide(),add(literal(1),divide()),Expr::Cast { expr: Box::new(Expr::Literal(V::Utf8("bad".into()))),target: CastTarget::Fixed(L::Scalar(S::I64)) }] {
        assert_eq!(error(count),"xtdb.expression/overflow-error");
    }
    assert_eq!(run(&trim(Expr::List(vec![]),divide())).unwrap_err().code,"xtdb.expression/division-by-zero");
    let cast = Expr::Cast { expr: Box::new(Expr::Literal(V::Utf8("bad-array".into()))),target: CastTarget::List(Box::new(CastTarget::Fixed(L::Scalar(S::I64)))) };
    assert_eq!(run(&trim(Expr::List(vec![literal(7)]),call("array_lower",vec![cast,literal(1)]))).unwrap(),V::list(vec![]));
}

#[test]
fn case_selects_before_size_but_its_monomorphic_selected_payload_stays_deferred() {
    assert_eq!(error(choose(condition(),literal(0),literal(1))),"xtdb.expression/division-by-zero");
    assert_eq!(error(choose(yes(),divide(),literal(0))),"xtdb.expression/overflow-error");
    assert_eq!(error(choose(yes(),divide(),nil())),"xtdb.expression/division-by-zero");
    assert_eq!(run(&trim(oversized(),choose(yes(),nil(),divide()))).unwrap(),V::Null);
    assert_eq!(error(add(choose(condition(),literal(0),literal(1)),divide())),"xtdb.expression/division-by-zero");
    assert_eq!(error(choose(call("is_null",vec![divide()]),literal(0),literal(1))),"xtdb.expression/overflow-error");
}

#[test]
fn coalesce_binds_nonlast_values_and_preserves_the_final_continuation_type() {
    assert_eq!(error(Expr::Coalesce(vec![divide(),literal(0)])),"xtdb.expression/division-by-zero");
    for count in [Expr::Coalesce(vec![nil(),divide()]),call("coalesce",vec![nil(),divide()]),add(literal(1),Expr::Coalesce(vec![nil(),divide()]))] {
        assert_eq!(error(count),"xtdb.expression/overflow-error");
    }
    assert_eq!(error(Expr::Coalesce(vec![nil(),choose(condition(),literal(0),literal(1))])),"xtdb.expression/division-by-zero");
    assert_eq!(error(Expr::Coalesce(vec![sleep(-1),divide()])),"xtdb.expression/overflow-error");
    assert_eq!(error(call("greatest",vec![divide(),literal(0)])),"xtdb.expression/division-by-zero");
    assert_eq!(run(&trim(oversized(),call("nullif",vec![literal(1),literal(1)]))).unwrap(),V::Null);
}

#[test]
fn nth_and_nullable_count_sources_choose_their_leg_before_input_size() {
    assert_eq!(error(call("nth",vec![Expr::List(vec![divide()]),literal(0)])),"xtdb.expression/division-by-zero");
    assert_eq!(run(&trim(oversized(),call("nth",vec![Expr::List(vec![divide()]),literal(-1)]))).unwrap(),V::Null);
    let count = Expr::Parameter { index: 0,data_type: L::Scalar(S::I64).maybe() };
    let expression = trim(oversized(),count);
    assert_eq!(evaluate(&expression,&Schema::default(),&[],&EvalContext { parameters: &[V::Null],..Default::default() }).unwrap(),V::Null);
    assert_eq!(evaluate(&expression,&Schema::default(),&[],&EvalContext { parameters: &[V::I64(0)],..Default::default() }).unwrap_err().code,"xtdb.expression/overflow-error");
}

#[tokio::test(start_paused = true)]
async fn null_count_payloads_are_discarded_but_polymorphic_case_boxing_runs_effects() {
    let start = tokio::time::Instant::now();
    assert_eq!(evaluate_async(&trim(oversized(),sleep(i64::MAX)),&Schema::default(),&[],&EvalContext::default()).await.unwrap(),V::Null);
    assert_eq!(tokio::time::Instant::now(),start);
    let duration = Expr::Literal(V::Duration { value: 0,unit: TimeUnit::Millisecond });
    assert_eq!(error(call("sleep",vec![choose(condition(),duration.clone(),duration)])),"xtdb.expression/division-by-zero");
    let count = choose(yes(),sleep(7),literal(0));
    assert_eq!(evaluate_async(&trim(oversized(),count),&Schema::default(),&[],&EvalContext::default()).await.unwrap(),V::Null);
    assert_eq!(tokio::time::Instant::now() - start,Duration::from_millis(7));
    let count = Expr::Unary { op: UnaryOp::IsNull,expr: Box::new(sleep(-1)) };
    assert_eq!(run(&trim(oversized(),choose(count,literal(0),literal(1)))).unwrap_err().code,"xtdb.expression/overflow-error");
}

#[tokio::test(start_paused = true)]
async fn cancellation_releases_prepared_count_payloads_and_allocation_failure_is_bounded() {
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let expression = trim(oversized(),choose(yes(),sleep(i64::MAX),literal(0)));
        assert!(tokio::time::timeout(Duration::from_millis(5),evaluate_async(&expression,&Schema::default(),&[],&context)).await.is_err());
        assert!(memory.used_bytes() > 0);
    }
    assert_eq!(memory.used_bytes(),0);
    let memory = MemoryBudget::new(8192);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        assert!(evaluate(&trim(Expr::List(vec![]),literal(0)),&Schema::default(),&[],&context).is_err());
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn unregistered_type_predicates_shortcut_null_before_count_control() {
    for name in ["boolean?","number?","integer?","float?","string?","keyword?","uuid?","list?","set?","map?"] {
        let condition = Expr::Coalesce(vec![call(name,vec![nil()]),yes()]);
        assert_eq!(run(&trim(Expr::List(vec![literal(7)]),choose(condition,literal(0),literal(1)))).unwrap(),V::list(vec![V::I64(7)]),"{name}");
        assert_eq!(rtdb_plan::function_type_with_args(name,&[divide()]).unwrap_err().code,"xtdb.expression/function-type-mismatch","{name}");
    }
}

#[test]
fn array_argument_controls_precede_list_payload_and_dimensions_can_discard_it() {
    let bad_list = || Expr::Cast { expr: Box::new(Expr::Literal(V::Utf8("bad-array".into()))),target: CastTarget::List(Box::new(CastTarget::Fixed(L::Scalar(S::I64)))) };
    assert_eq!(run(&trim(bad_list(),choose(condition(),literal(0),literal(1)))).unwrap_err().code,"xtdb.expression/division-by-zero");
    assert_eq!(run(&trim(bad_list(),nil())).unwrap(),V::Null);
    assert_eq!(run(&call("array_lower",vec![bad_list(),literal(1)])).unwrap(),V::I32(1));
    for name in ["array_upper","array_lower","array_length"] {
        assert_eq!(run(&call(name,vec![bad_list(),literal(2)])).unwrap_err().code,"xtdb.expression/array-dimension-error");
        assert_eq!(run(&call(name,vec![bad_list(),choose(condition(),literal(1),literal(1))])).unwrap_err().code,"xtdb.expression/division-by-zero");
    }
    assert_eq!(run(&trim(oversized(),call("nth",vec![divide(),divide()]))).unwrap(),V::Null);
}

fn text(value: &str) -> Expr { Expr::Literal(V::Utf8(value.into())) }
fn like(input: Expr,pattern: Expr) -> Expr { Expr::Like { expr: Box::new(input),pattern: Box::new(pattern),escape: None,case_insensitive: false,negated: false } }
fn rendered_length(input: Expr) -> Expr { call("length",vec![call("str",vec![input])]) }

#[test]
fn like_prepares_operand_controls_before_deferring_the_pattern_match() {
    assert_eq!(error(rendered_length(like(choose(condition(),text("a"),text("b")),text("a")))),"xtdb.expression/division-by-zero");
    assert_eq!(error(rendered_length(like(text("a"),choose(condition(),text("a"),text("b"))))),"xtdb.expression/division-by-zero");
    assert_eq!(error(rendered_length(like(call("str",vec![divide()]),text("a")))),"xtdb.expression/overflow-error");
    assert_eq!(error(rendered_length(like(choose(yes(),call("str",vec![divide()]),nil()),text("a")))),"xtdb.expression/division-by-zero");
    assert_eq!(error(rendered_length(like(call("str",vec![divide()]),nil()))),"xtdb.expression/overflow-error");
}

#[test]
fn monomorphic_boolean_payloads_defer_nested_controls_while_nullable_booleans_select_first() {
    for op in [B::And,B::Or] {
        let left = if op == B::And { yes() } else { Expr::Literal(V::Bool(false)) };
        let right = choose(condition(),yes(),Expr::Literal(V::Bool(false)));
        assert_eq!(error(rendered_length(Expr::binary(op,left,right.clone()))),"xtdb.expression/overflow-error");
        assert_eq!(error(rendered_length(Expr::binary(op,nil(),right))),"xtdb.expression/division-by-zero");
    }
    let condition = Expr::binary(B::IsDistinctFrom,divide(),nil());
    assert_eq!(error(choose(condition,literal(0),literal(1))),"xtdb.expression/overflow-error");
    assert_eq!(error(choose(call("null_eq",vec![divide(),nil()]),literal(0),literal(1))),"xtdb.expression/overflow-error");
}

#[test]
fn polymorphic_count_intermediates_apply_value_box_date_admission() {
    let wide = Expr::Literal(V::calendar_date(rtdb_types::CivilDate::MAX,rtdb_types::DateUnit::Day));
    let count = rendered_length(choose(yes(),wide.clone(),nil()));
    assert_eq!(run(&trim(Expr::List(vec![]),count)).unwrap_err().code,"xtdb.expression/overflow-error");
    let count = rendered_length(choose(yes(),wide.clone(),wide));
    assert_eq!(run(&trim(Expr::List(vec![]),count)).unwrap_err().code,"xtdb.expression/array-element-error");
}
