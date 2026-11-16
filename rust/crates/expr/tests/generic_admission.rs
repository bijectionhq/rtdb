use rtdb_expr::{EvalContext,evaluate,evaluate_async,scalar_call};
use rtdb_plan::{BinaryOp as B,Expr,Schema,function_type_with_args};
use rtdb_types::{Result,TimeUnit,Value as V};

fn int(value: i64) -> Expr { Expr::Literal(V::I64(value)) }
fn null() -> Expr { Expr::Literal(V::Null) }
fn divide() -> Expr { Expr::binary(B::Divide,int(1),int(0)) }
fn call(name: &str,args: Vec<Expr>) -> Expr { let return_type = function_type_with_args(name,&args).unwrap();Expr::Call { name: name.into(),args,return_type } }
fn run(expr: &Expr) -> Result<V> { evaluate(expr,&Schema::default(),&[],&EvalContext::default()) }

#[test]
fn unregistered_call_null_specializations_keep_control_selection_and_discard_payloads() {
    let control = Expr::Case { branches: vec![(Expr::binary(B::Eq,divide(),int(0)),int(1))],otherwise: Box::new(int(2)) };
    for name in ["trunc","uuid","uri","substr","char_length","bit_length","list?","number?","unregistered_fn"] {
        assert_eq!(run(&call(name,vec![divide(),null()])).unwrap(),V::Null,"{name}");
        assert_eq!(run(&call(name,vec![null(),control.clone()])).unwrap(),V::Null,"{name}");
        assert_eq!(run(&call(name,vec![control.clone(),null()])).unwrap_err().code,"xtdb.expression/division-by-zero","{name}");
        assert_eq!(scalar_call(name,&[V::Null],&EvalContext::default()).unwrap(),V::Null,"{name}");
        assert_eq!(scalar_call(name,&[V::I64(1)],&EvalContext::default()).unwrap_err().code,"xtdb.expression/function-type-mismatch","{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn unregistered_null_call_discards_sleep_payloads() {
    let sleep = call("sleep",vec![Expr::Literal(V::Duration { value: 5,unit: TimeUnit::Millisecond })]);
    let start = tokio::time::Instant::now();
    assert_eq!(evaluate_async(&call("list?",vec![sleep]),&Schema::default(),&[],&EvalContext::default()).await.unwrap(),V::Null);
    assert_eq!(tokio::time::Instant::now(),start);
}
