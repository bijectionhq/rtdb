use rtdb_expr::{EvalContext, evaluate, evaluate_async, function_type};
use rtdb_plan::{BinaryOp, Expr, Schema};
use rtdb_types::{TimeUnit, Value as V};

fn value(value: V) -> Expr { Expr::Literal(value) }
fn eq(left: Expr, right: Expr) -> Expr { Expr::binary(BinaryOp::Eq, left, right) }
fn divide() -> Expr { Expr::binary(BinaryOp::Divide, value(V::I64(1)), value(V::I64(0))) }
fn sleep() -> Expr {
    let args = vec![value(V::Duration { value: 7, unit: TimeUnit::Millisecond })];
    let return_type = function_type("sleep", &args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap();
    Expr::Call { name: "sleep".into(), args, return_type }
}

#[tokio::test]
async fn equality_keeps_scalar_composite_and_literal_coercion_results_on_both_entry_points() {
    for (left, right, expected) in [
        (V::I32(7), V::I64(7), V::Bool(true)),
        (V::I64(7), V::I64(8), V::Bool(false)),
        (V::F64(f64::NAN), V::F64(f64::NAN), V::Bool(false)),
        (V::Null, V::I64(7), V::Null),
        (V::DateDay(0), V::Utf8("1970-01-01".into()), V::Bool(true)),
        (V::list(vec![V::I64(1),V::Null]), V::list(vec![V::I64(1),V::I64(2)]), V::Null),
        (V::list(vec![V::I64(1)]), V::list(vec![V::I64(1),V::Null]), V::Bool(false)),
    ] {
        let expression = eq(value(left), value(right));
        assert_eq!(evaluate(&expression, &Schema::default(), &[], &EvalContext::default()).unwrap(), expected);
        assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext::default()).await.unwrap(), expected);
    }
}

#[tokio::test]
async fn equality_discards_pending_payloads_on_null_but_demands_selected_controls_and_union_boxes() {
    for expression in [eq(divide(), value(V::Null)), eq(value(V::Null), divide())] {
        assert_eq!(evaluate(&expression, &Schema::default(), &[], &EvalContext::default()).unwrap(), V::Null);
        assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext::default()).await.unwrap(), V::Null);
    }
    for expression in [
        eq(Expr::Case { branches: vec![(value(V::Bool(true)), divide())], otherwise: Box::new(value(V::Null)) }, value(V::Null)),
        eq(divide(), Expr::Case { branches: vec![(eq(divide(), value(V::I64(1))), value(V::I64(1)))], otherwise: Box::new(value(V::I64(2))) }),
    ] {
        assert_eq!(evaluate(&expression, &Schema::default(), &[], &EvalContext::default()).unwrap_err().code, "xtdb.expression/division-by-zero");
        assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext::default()).await.unwrap_err().code, "xtdb.expression/division-by-zero");
    }
}

#[tokio::test(start_paused = true)]
async fn equality_effect_demand_follows_the_selected_continuation() {
    let start = tokio::time::Instant::now();
    let expression = eq(sleep(), value(V::Null));
    assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext::default()).await.unwrap(), V::Null);
    assert_eq!(tokio::time::Instant::now(), start);
    let expression = eq(Expr::Case { branches: vec![(value(V::Bool(true)), sleep())], otherwise: Box::new(value(V::I64(1))) }, value(V::Null));
    assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext::default()).await.unwrap(), V::Null);
    assert_eq!(tokio::time::Instant::now() - start, std::time::Duration::from_millis(7));
}
