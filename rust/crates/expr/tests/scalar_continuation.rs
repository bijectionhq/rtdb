use std::time::Duration;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate,evaluate_async};
use rtdb_plan::{BinaryOp as B,CastTarget,Expr,Schema,UnaryOp as U,function_type_with_args};
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
fn forced_control() -> Expr { branch(Expr::binary(B::Eq,divide(),int(0)),int(1),int(2)) }
fn forced_box() -> Expr { branch(value(V::Bool(true)),divide(),null()) }
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

#[test]
fn ordinary_binary_operators_select_nulls_before_monomorphic_payloads() {
    for op in [B::Add,B::Subtract,B::Multiply,B::Divide,B::Modulo,B::Eq,B::NotEq,B::Lt,B::LtEq,B::Gt,B::GtEq] {
        for (left,right) in [(divide(),null()),(null(),divide())] {
            assert_eq!(run(&Expr::binary(op,left,right)).unwrap(),V::Null,"{op:?}");
        }
        assert_eq!(run(&Expr::binary(op,forced_control(),null())).unwrap_err().code,"xtdb.expression/division-by-zero","{op:?}");
        assert_eq!(run(&Expr::binary(op,forced_box(),null())).unwrap_err().code,"xtdb.expression/division-by-zero","{op:?}");
        assert_eq!(run(&Expr::binary(op,null(),forced_control())).unwrap(),V::Null,"{op:?}");
    }
    for op in [B::IsDistinctFrom,B::IsNotDistinctFrom] {
        assert_eq!(run(&Expr::binary(op,divide(),null())).unwrap(),V::Bool(op == B::IsDistinctFrom));
    }
    assert_eq!(run(&Expr::binary(B::Concat,cast(divide(),S::Utf8),null())).unwrap(),V::Null);
}

#[test]
fn ordinary_functions_and_type_constants_discard_unused_payloads() {
    for name in ["power","log"] {
        for args in [vec![divide(),null()],vec![null(),divide()]] { assert_eq!(run(&call(name,args)).unwrap(),V::Null,"{name}"); }
        assert_eq!(run(&call(name,vec![forced_control(),null()])).unwrap_err().code,"xtdb.expression/division-by-zero");
    }
    for (name,args) in [
        ("substring",vec![cast(divide(),S::Utf8),null()]),
        ("replace",vec![cast(divide(),S::Utf8),value(V::Utf8("x".into())),null()]),
        ("concat",vec![cast(divide(),S::Utf8),null()]),
        ("string_to_array",vec![null(),cast(divide(),S::Utf8)]),
        ("format_type",vec![null(),divide()]),
    ] { assert_eq!(run(&call(name,args)).unwrap(),V::Null,"{name}"); }
    for (name,expected) in [("nil?",false),("is_null",false),("true?",false),("false?",false),("boolean",true)] {
        assert_eq!(run(&call(name,vec![divide()])).unwrap(),V::Bool(expected),"{name}");
        assert_eq!(run(&call(name,vec![forced_control()])).unwrap_err().code,"xtdb.expression/division-by-zero");
    }
    for (op,expected) in [(U::IsNull,false),(U::IsUnknown,false),(U::IsNotNull,true),(U::IsTrue,false),(U::IsFalse,false)] {
        assert_eq!(run(&Expr::unary(op,divide())).unwrap(),V::Bool(expected),"{op:?}");
    }
}

#[test]
fn control_forms_keep_boolean_coercion_and_payload_bindings_distinct() {
    assert_eq!(run(&branch(divide(),int(1),int(2))).unwrap(),V::I64(1));
    assert_eq!(run(&branch(Expr::unary(U::IsTrue,divide()),int(1),int(2))).unwrap(),V::I64(2));
    assert_eq!(run(&Expr::binary(B::And,int(0),value(V::Bool(true)))).unwrap(),V::Bool(true));
    assert_eq!(run(&Expr::binary(B::Or,value(V::Bool(false)),int(0))).unwrap(),V::Bool(true));
    assert_eq!(run(&Expr::binary(B::And,divide(),value(V::Bool(false)))).unwrap_err().code,"xtdb.expression/division-by-zero");
    assert_eq!(run(&Expr::binary(B::Add,Expr::binary(B::And,value(V::Bool(true)),Expr::binary(B::Eq,divide(),int(0))),null())).unwrap(),V::Null);
    assert_eq!(run(&call("greatest",vec![divide(),null()])).unwrap_err().code,"xtdb.expression/division-by-zero");
    assert_eq!(run(&call("nullif",vec![divide(),null()])).unwrap_err().code,"xtdb.expression/division-by-zero");
}

#[test]
fn floating_round_skips_scale_payload_but_keeps_scale_type_selection() {
    for input in [f64::INFINITY,f64::NEG_INFINITY] {
        assert_eq!(run(&call("round",vec![value(V::F64(input)),divide()])).unwrap(),V::F64(input));
        assert_eq!(run(&call("round",vec![value(V::F64(input)),forced_control()])).unwrap_err().code,"xtdb.expression/division-by-zero");
        assert_eq!(run(&call("round",vec![value(V::F64(input)),forced_box()])).unwrap_err().code,"xtdb.expression/division-by-zero");
    }
    assert_eq!(run(&call("round",vec![value(V::F64(1.5)),divide()])).unwrap_err().code,"xtdb.expression/division-by-zero");
}

#[tokio::test(start_paused = true)]
async fn scalar_null_selection_discards_sleep_and_macro_bindings_preserve_it() {
    let start = tokio::time::Instant::now();
    for expr in [
        Expr::binary(B::Add,divide(),sleep()),cast(sleep(),S::I64),Expr::unary(U::Negate,sleep()),Expr::unary(U::Not,sleep()),
        call("abs",vec![sleep()]),call("current_setting",vec![sleep()]),call("uri_scheme",vec![sleep()]),
        Expr::Coalesce(vec![sleep(),int(7)]),call("greatest",vec![sleep(),divide()]),
        branch(sleep(),int(1),int(7)),Expr::binary(B::And,sleep(),value(V::Bool(false))),
    ] { run_async(&expr).await.unwrap(); }
    assert_eq!(tokio::time::Instant::now(),start);
    assert_eq!(run_async(&call("nullif",vec![sleep(),divide()])).await.unwrap(),V::Null);
    assert_eq!(tokio::time::Instant::now()-start,Duration::from_millis(5));
    assert_eq!(run_async(&call("format",vec![value(V::Utf8("%s".into())),sleep()])).await.unwrap(),V::Utf8(String::new()));
    assert_eq!(tokio::time::Instant::now()-start,Duration::from_millis(10));
}

#[test]
fn logarithm_consumes_x_before_base_after_argument_selection() {
    let x = cast(value(V::Utf8("invalid-number".into())),S::F64);
    assert_eq!(run(&call("log",vec![divide(),x])).unwrap_err().code,"xtdb.expression/number-format-error");
    assert_eq!(run(&call("log",vec![forced_control(),cast(value(V::Utf8("invalid-number".into())),S::F64)])).unwrap_err().code,"xtdb.expression/division-by-zero");
}

#[tokio::test(start_paused = true)]
async fn cancelled_required_scalar_binding_releases_its_scope() {
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let expr = call("nullif",vec![call("sleep",vec![value(V::Duration { value: i64::MAX,unit: TimeUnit::Millisecond })]),null()]);
        assert!(tokio::time::timeout(Duration::from_millis(1),evaluate_async(&expr,&Schema::default(),&[],&context)).await.is_err());
        assert!(memory.used_bytes() > 0);
    }
    assert_eq!(memory.used_bytes(),0);
}
