use std::time::Duration;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate_async};
use rtdb_plan::{BinaryOp as B,Expr,Schema,UnaryOp as U,function_type_with_args};
use rtdb_types::{Result,TimeUnit,Value as V};
fn int(n: i64) -> Expr { Expr::Literal(V::I64(n)) }
fn null() -> Expr { Expr::Literal(V::Null) }
fn divide() -> Expr { Expr::binary(B::Divide,int(1),int(0)) }
fn call(name: &str,args: Vec<Expr>) -> Expr { let return_type = function_type_with_args(name,&args).unwrap();Expr::Call { name: name.into(),args,return_type } }
fn sleep() -> Expr { call("sleep",vec![Expr::Literal(V::Duration { value: 5,unit: TimeUnit::Millisecond })]) }
fn delayed(n: i64) -> Expr { Expr::Coalesce(vec![call("nullif",vec![sleep(),null()]),int(n)]) }
async fn run(expr: &Expr) -> Result<V> {
    let memory = MemoryBudget::new(1 << 25);
    let result = {
        let scope = EvaluationScope::new(&memory)?;
        evaluate_async(expr,&Schema::default(),&[],&EvalContext { allocation: Some(&scope),..Default::default() }).await
    };
    assert_eq!(memory.used_bytes(),0);result
}

#[tokio::test(start_paused = true)]
async fn between_binds_nonnull_and_null_subjects_once_before_bound_control() {
    for symmetric in [false,true] {
        let start = tokio::time::Instant::now();
        assert_eq!(run(&Expr::between(delayed(7),int(1),int(9),symmetric)).await.unwrap(),V::Bool(true));
        assert_eq!(tokio::time::Instant::now()-start,Duration::from_millis(5));
        let start = tokio::time::Instant::now();
        assert_eq!(run(&Expr::between(sleep(),divide(),divide(),symmetric)).await.unwrap(),V::Null);
        assert_eq!(tokio::time::Instant::now()-start,Duration::from_millis(5));
    }
}

#[tokio::test(start_paused = true)]
async fn symmetric_between_repeats_reached_bounds_without_rebinding_subject() {
    let start = tokio::time::Instant::now();
    assert_eq!(run(&Expr::between(delayed(7),delayed(10),int(5),true)).await.unwrap(),V::Bool(true));
    assert_eq!(tokio::time::Instant::now()-start,Duration::from_millis(15));
    assert_eq!(run(&Expr::between(Expr::Coalesce(vec![int(7)]),int(10),divide(),false)).await.unwrap(),V::Bool(false));
    assert_eq!(run(&Expr::between(Expr::Coalesce(vec![int(7)]),null(),int(5),false)).await.unwrap(),V::Bool(false));
    assert_eq!(run(&Expr::between(Expr::Coalesce(vec![int(7)]),null(),int(9),false)).await.unwrap(),V::Null);
    assert_eq!(run(&Expr::between(Expr::Coalesce(vec![int(7)]),null(),int(5),true)).await.unwrap(),V::Null);
}

#[tokio::test(start_paused = true)]
async fn null_inspection_preserves_eager_subject_and_nullable_bound_controls() {
    let start = tokio::time::Instant::now();
    let expr = Expr::unary(U::IsNull,Expr::between(delayed(7),divide(),int(9),false));
    assert_eq!(run(&expr).await.unwrap(),V::Bool(false));
    assert_eq!(tokio::time::Instant::now()-start,Duration::from_millis(5));
    let expr = Expr::unary(U::IsNull,Expr::between(Expr::Coalesce(vec![int(7)]),divide(),null(),false));
    assert_eq!(run(&expr).await.unwrap_err().code,"xtdb.expression/division-by-zero");
    let expr = Expr::unary(U::IsNull,Expr::between(divide(),int(1),int(9),false));
    assert_eq!(run(&expr).await.unwrap_err().code,"xtdb.expression/division-by-zero");
}

#[tokio::test]
async fn ordered_admission_runs_before_a_parent_discards_the_comparison() {
    let left = Expr::Literal(V::IntervalYearMonth(1));
    let right = Expr::Literal(V::IntervalMonthDayNano { months: 0,days: 1,nanoseconds: 0 });
    for expr in [Expr::between(Expr::Coalesce(vec![left.clone()]),right.clone(),right.clone(),false),Expr::binary(B::Lt,left,right)] {
        assert_eq!(run(&Expr::unary(U::IsNull,expr)).await.unwrap_err().code,"xtdb.expression/cannot-compare-different-unit-intervals");
    }
    let expr = Expr::between(Expr::Coalesce(vec![int(1)]),Expr::Literal(V::Utf8("a".into())),Expr::Literal(V::Utf8("z".into())),false);
    assert_eq!(run(&Expr::unary(U::IsNull,expr)).await.unwrap(),V::Bool(false));
}

#[tokio::test]
async fn complex_subject_binding_removes_subject_literal_temporal_coercion_only() {
    let text = Expr::Literal(V::Utf8("1970-01-01".into()));
    let day = Expr::Literal(V::DateDay(0));
    assert_eq!(run(&Expr::between(text.clone(),day.clone(),day.clone(),false)).await.unwrap(),V::Bool(true));
    assert_eq!(run(&Expr::between(Expr::Coalesce(vec![day.clone()]),text.clone(),text.clone(),false)).await.unwrap(),V::Bool(true));
    assert!(run(&Expr::between(Expr::Coalesce(vec![text]),day.clone(),day,false)).await.is_err());
}
