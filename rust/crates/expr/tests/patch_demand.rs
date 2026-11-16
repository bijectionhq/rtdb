use std::time::Duration;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate_async};
use rtdb_plan::{BinaryOp as B,Expr,Schema,UnaryOp as U,function_type_with_args};
use rtdb_types::{Result,TimeUnit,Value as V};

fn int(value: i64) -> Expr { Expr::Literal(V::I64(value)) }
fn null() -> Expr { Expr::Literal(V::Null) }
fn divide() -> Expr { Expr::binary(B::Divide,int(1),int(0)) }
fn call(name: &str,args: Vec<Expr>) -> Expr { let return_type = function_type_with_args(name,&args).unwrap();Expr::Call { name: name.into(),args,return_type } }
fn sleep() -> Expr { call("sleep",vec![Expr::Literal(V::Duration { value: 5,unit: TimeUnit::Millisecond })]) }
fn record(name: &str,value: Expr) -> Expr { Expr::Struct(vec![(name.into(),value)]) }
fn field(expr: Expr,name: &str) -> Expr { let data_type = rtdb_plan::field_type(&expr.data_type(),name);Expr::Field { expr: Box::new(expr),name: name.into(),data_type } }
async fn run(expr: &Expr) -> Result<V> {
    let memory = MemoryBudget::new(32 << 20);
    let result = { let scope = EvaluationScope::new(&memory)?;let context = EvalContext { allocation: Some(&scope),..Default::default() };evaluate_async(expr,&Schema::default(),&[],&context).await };
    assert_eq!(memory.used_bytes(),0);result
}

#[tokio::test(start_paused = true)]
async fn patch_null_left_payload_is_discarded_but_struct_controls_remain_eager() {
    let start = tokio::time::Instant::now();
    assert_eq!(run(&field(call("_patch",vec![sleep(),record("v",int(7))]),"v")).await.unwrap(),V::I64(7));
    assert_eq!(run(&Expr::unary(U::IsNull,call("_patch",vec![sleep(),record("v",int(7))]))).await.unwrap(),V::Bool(false));
    assert_eq!(tokio::time::Instant::now(),start);
    let right = record("v",call("nullif",vec![sleep(),null()]));
    assert_eq!(run(&Expr::unary(U::IsNull,call("_patch",vec![sleep(),right]))).await.unwrap(),V::Bool(false));
    assert_eq!(tokio::time::Instant::now()-start,Duration::from_millis(5));
    let boxed = Expr::Case { branches: vec![(Expr::Literal(V::Bool(true)),sleep())],otherwise: Box::new(Expr::Struct(vec![])) };
    assert_eq!(run(&field(call("_patch",vec![boxed,record("v",int(7))]),"v")).await.unwrap(),V::I64(7));
    assert_eq!(tokio::time::Instant::now()-start,Duration::from_millis(10));
}

#[tokio::test]
async fn patch_preserves_struct_construction_and_lazy_nested_field_readers() {
    let expression = field(call("_patch",vec![record("v",divide()),record("v",int(7))]),"v");
    assert_eq!(run(&expression).await.unwrap_err().code,"xtdb.expression/division-by-zero");
    let left = Expr::Struct(vec![("v".into(),int(7)),("unused".into(),Expr::List(vec![divide()]))]);
    assert_eq!(run(&field(call("_patch",vec![left,record("v",null())]),"v")).await.unwrap(),V::I64(7));
}
