use std::time::Duration;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext, EvaluationScope, evaluate, evaluate_async, function_type, scalar_call};
use rtdb_plan::{CastTarget, Expr, Schema};
use rtdb_types::{CivilDate, DateUnit, ListKind, LogicalType as L, ScalarType as S, TimeUnit, Value as V};

fn call(name: &str, args: Vec<Expr>) -> Expr {
    let return_type = function_type(name, &args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap();
    Expr::Call { name: name.into(), args, return_type }
}
fn count(value: i64) -> Expr { Expr::Literal(V::I64(value)) }
fn list() -> Expr { Expr::List(vec![count(42), count(43)]) }
fn trim(input: Expr, amount: i64) -> Expr { call("trim_array", vec![input, count(amount)]) }
fn run(expr: &Expr) -> rtdb_types::Result<V> { evaluate(expr, &Schema::default(), &[], &EvalContext::default()) }
fn sleep(millis: i64) -> Expr { call("sleep", vec![Expr::Literal(V::Duration { value: millis, unit: TimeUnit::Millisecond })]) }

#[test]
fn array_bounds_use_one_dimension_and_empty_arrays_keep_lower_bound_one() {
    for (input, size) in [(list(), 2), (Expr::List(vec![]), 0)] {
        for name in ["array_upper", "array_lower", "array_length"] {
            assert_eq!(run(&call(name, vec![input.clone(), count(1)])).unwrap(), V::I32(if name == "array_lower" { 1 } else { size }));
            for dimension in [0, 2, -1] {
                let error = run(&call(name, vec![input.clone(), count(dimension)])).unwrap_err();
                assert_eq!(error.code, "xtdb.expression/array-dimension-error");
                assert_eq!(error.context["dim"], serde_json::json!(dimension));
                assert_eq!(error.message, Some(format!("Unsupported: {} for dimension != 1", name.to_uppercase())));
            }
        }
    }
}

#[test]
fn trim_preserves_elements_and_reports_overtrimmed_length() {
    for (amount, expected) in [(0, vec![V::I64(42), V::I64(43)]), (1, vec![V::I64(42)]), (2, vec![])] {
        assert_eq!(run(&trim(list(), amount)).unwrap(), V::list(expected.clone()));
        assert_eq!(scalar_call("trim_array", &[V::list(vec![V::I64(42), V::I64(43)]), V::I64(amount)], &EvalContext::default()).unwrap(), V::list(expected));
    }
    let error = run(&trim(list(), 3)).unwrap_err();
    assert_eq!(error.code, "xtdb.expression/array-element-error");
    assert_eq!(error.message.as_deref(), Some("Data exception - array element error."));
    assert_eq!(error.context["nlen"], serde_json::json!(-1));
    assert_eq!(run(&trim(Expr::List(vec![]), 0)).unwrap(), V::list(vec![]));
}

#[test]
fn trim_and_bounds_keep_unread_cells_and_null_arguments_lazy() {
    let input = Expr::List(vec![count(7), Expr::Literal(V::calendar_date(CivilDate::MAX, DateUnit::Day)), sleep(-1)]);
    assert_eq!(run(&trim(input.clone(), 2)).unwrap(), V::list(vec![V::I64(7)]));
    for name in ["array_upper", "array_length"] {
        assert_eq!(run(&call(name, vec![input.clone(), count(1)])).unwrap(), V::I32(3));
    }
    assert_eq!(run(&call("array_lower", vec![input, count(1)])).unwrap(), V::I32(1));
    for name in ["array_upper", "array_lower", "array_length", "trim_array"] {
        assert_eq!(run(&call(name, vec![Expr::Literal(V::Null), sleep(-1)])).unwrap(), V::Null);
        assert_eq!(run(&call(name, vec![list(), Expr::Literal(V::Null)])).unwrap(), V::Null);
    }
    let input = Expr::Cast { expr: Box::new(Expr::List(vec![Expr::Literal(V::Utf8("8".into())), Expr::Literal(V::Utf8("invalid".into()))])), target: CastTarget::List(Box::new(CastTarget::Fixed(L::Scalar(S::I64)))) };
    assert_eq!(run(&trim(input, 1)).unwrap(), V::list(vec![V::I64(8)]));
}

#[test]
fn negative_trim_counts_extend_the_view_and_size_cast_is_deferred() {
    let extended = trim(list(), -1);
    assert_eq!(run(&call("cardinality", vec![extended.clone()])).unwrap(), V::I32(3));
    assert_eq!(run(&call("nth", vec![extended.clone(), count(0)])).unwrap(), V::I64(42));
    assert!(run(&extended).is_err());
    let oversized = trim(Expr::List(vec![]), -2_147_483_648);
    assert_eq!(run(&call("nth", vec![oversized.clone(), count(-1)])).unwrap(), V::Null);
    assert_eq!(run(&call("array_lower", vec![oversized.clone(), count(1)])).unwrap(), V::I32(1));
    let error = run(&call("cardinality", vec![oversized])).unwrap_err();
    assert_eq!(error.code, "xtdb.expression/overflow-error");
    assert_eq!(error.message.as_deref(), Some("data exception - overflow error"));
    let error = run(&trim(list(), i64::MIN)).unwrap_err();
    assert_eq!(error.context["nlen"], serde_json::json!(i64::MIN + 2));
}

#[test]
fn array_dispatch_preserves_the_pinned_signed_integer_and_list_matrix() {
    let list = L::list(L::Scalar(S::Utf8));
    for count in [S::I8, S::I16, S::I32, S::I64] {
        assert_eq!(function_type("trim_array", &[list.clone(), L::Scalar(count.clone())]).unwrap(), list);
        assert_eq!(function_type("array_length", &[list.clone(), L::Scalar(count.clone())]).is_ok(), count == S::I64);
    }
    for invalid in [L::Scalar(S::Utf8), L::List { kind: ListKind::Set, element: Box::new(L::Null) }, L::List { kind: ListKind::FixedSizeList(1), element: Box::new(L::Null) }] {
        assert!(function_type("trim_array", &[invalid, L::Scalar(S::I64)]).is_err());
    }
    assert_eq!(function_type("trim_array", &[list.clone().maybe(), L::Scalar(S::I64)]).unwrap(), list.maybe());
    assert!(function_type("array_length", &[L::Null]).is_err());
}

#[tokio::test(start_paused = true)]
async fn trimmed_sleep_is_unstarted_and_selected_sleep_cancellation_releases_memory() {
    let start = tokio::time::Instant::now();
    assert_eq!(evaluate_async(&trim(Expr::List(vec![count(7), sleep(i64::MAX)]), 1), &Schema::default(), &[], &EvalContext::default()).await.unwrap(), V::list(vec![V::I64(7)]));
    assert_eq!(tokio::time::Instant::now(), start);
    let memory = MemoryBudget::new(1 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope), ..Default::default() };
        let expression = trim(Expr::List(vec![sleep(i64::MAX)]), 0);
        assert!(tokio::time::timeout(Duration::from_millis(5), evaluate_async(&expression, &Schema::default(), &[], &context)).await.is_err());
        assert!(memory.used_bytes() > 0);
    }
    assert_eq!(memory.used_bytes(), 0);
    let memory = MemoryBudget::new(8192);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope), ..Default::default() };
        assert!(evaluate(&trim(list(), 0), &Schema::default(), &[], &context).is_err());
    }
    assert_eq!(memory.used_bytes(), 0);
}
