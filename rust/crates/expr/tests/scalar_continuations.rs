use rtdb_expr::{EvalContext, evaluate, evaluate_async, function_type};
use rtdb_plan::{BinaryOp, Expr, Schema};
use rtdb_types::{TimeUnit, Value};

fn literal(value: i64) -> Expr { Expr::Literal(Value::I64(value)) }
fn nil() -> Expr { Expr::Literal(Value::Null) }
fn divide() -> Expr { Expr::binary(BinaryOp::Divide,literal(1),literal(0)) }
fn call(name: &str,args: Vec<Expr>) -> Expr {
    let return_type = function_type(name,&args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap();
    Expr::Call { name: name.into(),args,return_type }
}
fn choose(condition: Expr,selected: Expr,other: Expr) -> Expr { Expr::Case { branches: vec![(condition,selected)],otherwise: Box::new(other) } }
fn yes() -> Expr { Expr::Literal(Value::Bool(true)) }
fn run(expression: &Expr) -> rtdb_types::Result<Value> { evaluate(expression,&Schema::default(),&[],&EvalContext::default()) }
fn sleep(millis: i64) -> Expr { call("sleep",vec![Expr::Literal(Value::Duration { value: millis,unit: TimeUnit::Millisecond })]) }

#[test]
fn catalogue_null_calls_discard_inline_arguments_but_keep_control_and_boxing() {
    for name in ["col_description","pg_indexam_has_property"] {
        let second = if name == "col_description" { literal(1) } else { Expr::Literal(Value::Utf8("x".into())) };
        assert_eq!(run(&call(name,vec![divide(),second.clone()])).unwrap(),Value::Null);
        assert_eq!(run(&call(name,vec![nil(),divide()])).unwrap(),Value::Null);
        let condition = Expr::binary(BinaryOp::Eq,divide(),literal(0));
        assert_eq!(run(&call(name,vec![choose(condition,literal(1),literal(2)),second.clone()])).unwrap_err().code,"xtdb.expression/division-by-zero");
        assert_eq!(run(&call(name,vec![choose(yes(),divide(),nil()),second])).unwrap_err().code,"xtdb.expression/division-by-zero");
    }
}

#[test]
fn mod_null_selection_discards_both_pending_payload_orders() {
    for args in [vec![nil(),divide()],vec![divide(),nil()],vec![divide(),sleep(-1)]] {
        assert_eq!(run(&call("mod",args)).unwrap(),Value::Null);
    }
    let condition = Expr::binary(BinaryOp::Eq,divide(),literal(0));
    assert_eq!(run(&call("mod",vec![choose(condition,literal(1),literal(2)),nil()])).unwrap_err().code,"xtdb.expression/division-by-zero");
    assert_eq!(run(&call("mod",vec![choose(yes(),divide(),nil()),nil()])).unwrap_err().code,"xtdb.expression/division-by-zero");
    assert_eq!(run(&call("mod",vec![literal(-7),literal(3)])).unwrap(),Value::I64(2));
}

#[tokio::test(start_paused = true)]
async fn ignored_catalogue_payloads_still_run_effects_required_by_polymorphic_boxing() {
    let start = tokio::time::Instant::now();
    let expression = call("col_description",vec![sleep(-1),literal(1)]);
    assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap(),Value::Null);
    assert_eq!(tokio::time::Instant::now(),start);
    let expression = call("col_description",vec![choose(yes(),sleep(7),literal(1)),literal(1)]);
    assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap(),Value::Null);
    assert_eq!(tokio::time::Instant::now() - start,std::time::Duration::from_millis(7));
}
