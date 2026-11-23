use std::time::Duration;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext, EvaluationScope, evaluate, evaluate_async, function_type, scalar_call_async};
use rtdb_plan::{BinaryOp, Expr, Schema};
use rtdb_types::{LogicalType, ScalarType, TimeUnit, Value};

fn call(name: &str, args: Vec<Expr>) -> Expr {
    let return_type = function_type(name, &args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap();
    Expr::Call { name: name.into(), args, return_type }
}
fn sleep(millis: i64) -> Expr { call("sleep", vec![Expr::Literal(Value::Duration { value: millis, unit: TimeUnit::Millisecond })]) }

#[test]
fn expression_random_has_no_arguments_and_returns_values_in_the_reference_range() {
    let expression = call("random", vec![]);
    assert_eq!(expression.data_type(), LogicalType::Scalar(ScalarType::F64));
    assert!(expression.is_volatile());
    assert!(!expression.requires_suspension());
    assert!(function_type("random", &[LogicalType::Null]).is_err());
    for _ in 0..128 {
        let Value::F64(value) = evaluate(&expression, &Schema::default(), &[], &EvalContext::default()).unwrap() else { panic!() };
        assert!((0.0..1.0).contains(&value));
    }
}

#[tokio::test(start_paused = true)]
async fn sleep_waits_on_the_callers_timer_and_preserves_null_result() {
    let start = tokio::time::Instant::now();
    assert_eq!(evaluate_async(&sleep(125), &Schema::default(), &[], &EvalContext::default()).await.unwrap(), Value::Null);
    assert_eq!(tokio::time::Instant::now() - start, Duration::from_millis(125));
    assert_eq!(scalar_call_async("sleep", &[Value::Null], &EvalContext::default()).await.unwrap(), Value::Null);
    assert!(function_type("sleep", &[LogicalType::Scalar(ScalarType::I64)]).is_err());
}

#[tokio::test(start_paused = true)]
async fn suspended_evaluator_keeps_case_coalesce_and_boolean_short_circuiting() {
    let failure = sleep(-1);
    let cases = [
        (Expr::binary(BinaryOp::And, Expr::Literal(Value::Bool(false)), failure.clone()), Value::Bool(false)),
        (Expr::binary(BinaryOp::Or, Expr::Literal(Value::Bool(true)), failure.clone()), Value::Bool(true)),
        (Expr::Case { branches: vec![(Expr::Literal(Value::Bool(true)), Expr::Literal(Value::I64(7)))], otherwise: Box::new(failure.clone()) }, Value::I64(7)),
        (Expr::Coalesce(vec![Expr::Literal(Value::I64(9)), failure]), Value::I64(9)),
    ];
    let start = tokio::time::Instant::now();
    for (expression, expected) in cases {
        assert!(expression.requires_suspension());
        assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext::default()).await.unwrap(), expected);
    }
    assert_eq!(tokio::time::Instant::now(), start);
}

#[tokio::test(start_paused = true)]
async fn dropping_sleep_releases_its_owned_future_and_allocation_scope() {
    let memory = MemoryBudget::new(1 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope), ..Default::default() };
        let expression = sleep(i64::MAX);
        assert!(tokio::time::timeout(Duration::from_millis(5), evaluate_async(&expression, &Schema::default(), &[], &context)).await.is_err());
        assert!(memory.used_bytes() > 0);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn ordinary_async_evaluation_uses_the_same_allocation_scope_as_sync_evaluation() {
    let expression = Expr::List(vec![Expr::Literal(Value::Utf8("kept".into()))]);
    let memory = MemoryBudget::new(1 << 20);
    let charged;
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope), ..Default::default() };
        assert_eq!(evaluate(&expression, &Schema::default(), &[], &context).unwrap(), Value::list(vec![Value::Utf8("kept".into())]));
        charged = memory.used_bytes();
    }
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope), ..Default::default() };
    assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &context).await.unwrap(), Value::list(vec![Value::Utf8("kept".into())]));
    assert_eq!(memory.used_bytes(), charged);
}
