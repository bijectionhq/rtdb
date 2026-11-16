use std::time::Duration;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate,evaluate_async};
use rtdb_plan::{BinaryOp as B,CastTarget,Expr,Schema,function_type_with_args};
use rtdb_types::{LogicalType as L,Result,ScalarType as S,TimeUnit,Value as V};

fn value(value: V) -> Expr { Expr::Literal(value) }
fn int(n: i64) -> Expr { value(V::I64(n)) }
fn null() -> Expr { value(V::Null) }
fn divide() -> Expr { Expr::binary(B::Divide,int(1),int(0)) }
fn call(name: &str,args: Vec<Expr>) -> Expr {
    let return_type = function_type_with_args(name,&args).unwrap();
    Expr::Call { name: name.into(),args,return_type }
}
fn cast(expr: Expr,target: S) -> Expr { Expr::Cast { expr: Box::new(expr),target: CastTarget::fixed(L::Scalar(target)) } }
fn sleep() -> Expr { call("sleep",vec![value(V::Duration { value: 5,unit: TimeUnit::Millisecond })]) }
fn branch(condition: Expr,yes: Expr,no: Expr) -> Expr { Expr::Case { branches: vec![(condition,yes)],otherwise: Box::new(no) } }
fn run(expr: &Expr) -> Result<V> { evaluate(expr,&Schema::default(),&[],&EvalContext::default()) }
async fn run_async(expr: &Expr) -> Result<V> {
    let memory = MemoryBudget::new(32 << 20);
    let result = {
        let scope = EvaluationScope::new(&memory)?;
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        evaluate_async(expr,&Schema::default(),&[],&context).await
    };
    assert_eq!(memory.used_bytes(),0);
    result
}


fn simple(operand: Expr,branches: Vec<(Expr,Expr)>,otherwise: Expr) -> Expr { Expr::SimpleCase { operand: Box::new(operand),branches,otherwise: Box::new(otherwise) } }

#[tokio::test(start_paused = true)]
async fn simple_case_binds_a_null_operand_once_before_skipping_candidate_controls() {
    let condition = branch(Expr::binary(B::Eq,divide(),int(0)),int(1),int(2));
    let expr = simple(sleep(),vec![(condition,divide()),(divide(),int(2))],int(7));
    let start = tokio::time::Instant::now();
    assert_eq!(run_async(&expr).await.unwrap(),V::I64(7));
    assert_eq!(tokio::time::Instant::now()-start,Duration::from_millis(5));
    let start = tokio::time::Instant::now();
    assert_eq!(run_async(&Expr::binary(B::Add,expr,null())).await.unwrap(),V::Null);
    assert_eq!(tokio::time::Instant::now()-start,Duration::from_millis(5));
}

#[tokio::test(start_paused = true)]
async fn simple_case_reuses_one_nonnull_operand_across_candidates() {
    let operand = Expr::Coalesce(vec![call("nullif",vec![sleep(),null()]),int(7)]);
    let expr = simple(operand,vec![(int(1),divide()),(int(7),int(9)),(divide(),int(10))],int(0));
    let start = tokio::time::Instant::now();
    assert_eq!(run_async(&expr).await.unwrap(),V::I64(9));
    assert_eq!(tokio::time::Instant::now()-start,Duration::from_millis(5));
}

#[test]
fn simple_case_keeps_candidate_controls_and_selected_result_boxing() {
    assert_eq!(run(&simple(divide(),vec![(null(),int(1))],int(2))).unwrap_err().code,"xtdb.expression/division-by-zero");
    assert_eq!(run(&simple(int(1),vec![(divide(),int(1))],int(2))).unwrap_err().code,"xtdb.expression/division-by-zero");
    let selected = simple(int(1),vec![(int(1),divide())],int(2));
    assert_eq!(run(&Expr::binary(B::Add,selected,null())).unwrap(),V::Null);
    let selected = simple(int(1),vec![(int(1),divide())],null());
    assert_eq!(run(&Expr::binary(B::Add,selected,null())).unwrap_err().code,"xtdb.expression/division-by-zero");
    let candidate = simple(int(1),vec![(divide(),int(1))],int(2));
    assert_eq!(run(&Expr::binary(B::Add,candidate,null())).unwrap_err().code,"xtdb.expression/division-by-zero");
}

#[test]
fn simple_case_operand_binding_removes_only_its_literal_temporal_coercion() {
    let day = value(V::DateDay(0));
    let text = value(V::Utf8("1970-01-01".into()));
    assert_eq!(run(&simple(text.clone(),vec![(day.clone(),int(1))],int(2))).unwrap(),V::I64(2));
    assert_eq!(run(&simple(day,vec![(text,int(1))],int(2))).unwrap(),V::I64(1));
    assert_eq!(run(&cast(simple(int(1),vec![(int(1),int(7))],int(8)),S::Utf8)).unwrap(),V::Utf8("7".into()));
}

#[tokio::test(start_paused = true)]
async fn cancelled_simple_case_operand_releases_the_caller_scope() {
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let expr = simple(call("sleep",vec![value(V::Duration { value: i64::MAX,unit: TimeUnit::Millisecond })]),vec![(int(1),int(1))],int(2));
        assert!(tokio::time::timeout(Duration::from_millis(1),evaluate_async(&expr,&Schema::default(),&[],&context)).await.is_err());
    }
    assert_eq!(memory.used_bytes(),0);
}
