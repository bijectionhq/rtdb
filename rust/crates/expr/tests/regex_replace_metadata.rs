use std::{sync::Arc,time::Duration};
use arrow_array::RecordBatch;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{CompiledExpr,EvalContext,EvaluationScope,ExpressionInvocation,evaluate,evaluate_async,evaluate_batch};
use rtdb_plan::{BinaryOp as B,CastTarget,Expr,RegexForm as F,RegexReplacement,Schema,function_type_with_args};
use rtdb_types::{LogicalType as L,Result,ScalarType as S,TimeUnit,Value as V};

fn literal(value: V) -> Expr { Expr::Literal(value) }
fn text(value: &str) -> Expr { literal(V::Utf8(value.into())) }
fn form(value: &str) -> F { raw_literal(V::Utf8(value.into())) }
fn replace(source: Expr,pattern: &str) -> Expr { Expr::regex_replace(source,RegexReplacement { pattern: form(pattern),replacement: form("x"),..Default::default() }) }
fn call(name: &str,args: Vec<Expr>) -> Expr { Expr::Call { name: name.into(),return_type: function_type_with_args(name,&args).unwrap(),args } }
fn divide() -> Expr { Expr::binary(B::Divide,literal(V::I64(1)),literal(V::I64(0))) }
fn as_text(expr: Expr) -> Expr { Expr::Cast { expr: Box::new(expr),target: CastTarget::fixed(L::Scalar(S::Utf8)) } }
fn run(expr: &Expr) -> Result<V> { evaluate(expr,&Schema::default(),&[],&EvalContext::default()) }

#[test]
fn replacement_bindings_run_before_controls_and_unread_list_elements() {
    let unused = replace(text("a"),"[");
    for expression in [
        Expr::Case { branches: vec![(literal(V::Bool(true)),text("ok"))],otherwise: Box::new(unused.clone()) },
        call("cardinality",vec![Expr::List(vec![unused])]),
        replace(as_text(divide()),"["),
    ] { assert_eq!(run(&expression).unwrap_err().code,"xtdb.expression/illegal-argument-exception"); }
    let nested = replace(replace(text("a"),"["),&"a".repeat(65_537));
    assert_eq!(run(&nested).unwrap_err().code,"rtdb.expr/regex-limit");
}

#[test]
fn replacement_metadata_is_uninterpreted_for_null_and_omitted_coalesce_tails() {
    let invalid = RegexReplacement { pattern: raw_symbol("unresolved".into()),replacement: F::default(),start: raw_literal(V::I64(1)),flags: raw_literal(V::Bool(false)),..Default::default() };
    assert_eq!(run(&Expr::regex_replace(literal(V::Null),invalid.clone())).unwrap(),V::Null);
    let unused = Expr::regex_replace(text("a"),invalid);
    assert_eq!(run(&Expr::Coalesce(vec![text("kept"),unused])).unwrap(),V::Utf8("kept".into()));
    assert_eq!(run(&Expr::Coalesce(vec![literal(V::Null),replace(text("a"),"[")])).unwrap_err().code,"xtdb.expression/illegal-argument-exception");
    let invalid_source = replace(literal(V::I64(1)),"a");
    assert_eq!(run(&invalid_source).unwrap_err().code,"xtdb.expression/function-type-mismatch");
    let discarded = Expr::Coalesce(vec![text("kept"),invalid_source]);
    let compiled = CompiledExpr::compile(&discarded,&Schema::default()).unwrap();
    assert_eq!(compiled.evaluate(&[],&EvalContext::default()).unwrap(),V::Utf8("kept".into()));
}

#[test]
fn full_parameter_vector_types_determine_replacement_admission() {
    let expression = replace(Expr::Parameter { index: 0,data_type: L::Nothing },"[");
    let values = [V::Null];
    let null = [L::Null];
    let nullable_text = [L::Scalar(S::Utf8).maybe()];
    let context = EvalContext { parameters: rtdb_expr::Parameters::Values(&values),parameter_types: Some(&null),..Default::default() };
    assert_eq!(evaluate(&expression,&Schema::default(),&[],&context).unwrap(),V::Null);
    let context = EvalContext { parameters: rtdb_expr::Parameters::Values(&values),parameter_types: Some(&nullable_text),..Default::default() };
    assert_eq!(evaluate(&expression,&Schema::default(),&[],&context).unwrap_err().code,"xtdb.expression/illegal-argument-exception");
}

#[test]
fn empty_batches_prepare_replacement_metadata_and_release_their_scope() {
    let expression = CompiledExpr::compile(&replace(text("a"),"["),&Schema::default()).unwrap();
    let batch = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
    let memory = MemoryBudget::new(128 << 20);
    assert_eq!(evaluate_batch(&expression,&batch,&EvalContext::default(),&memory).err().unwrap().code,"xtdb.expression/illegal-argument-exception");
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn one_invocation_owns_pattern_charges_across_independent_row_scopes() {
    let memory = MemoryBudget::new(128 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let expression = replace(text("a a"),"a");
        let schema = Schema::default();
        let invocation = ExpressionInvocation::prepare(&expression,&schema,&context).unwrap();
        let prepared_bytes = memory.used_bytes();
        for _ in 0..3 {
            {
                let row_scope = EvaluationScope::new(&memory).unwrap();
                let row_context = EvalContext { allocation: Some(&row_scope),..Default::default() };
                assert_eq!(invocation.evaluate_projection(&[],&row_context).unwrap(),V::Utf8("x x".into()));
            }
            assert_eq!(memory.used_bytes(),prepared_bytes);
        }
    }
    assert_eq!(memory.used_bytes(),0);
}

#[tokio::test(start_paused = true)]
async fn cancelled_replacement_source_drops_row_work_and_invocation_charges() {
    let memory = MemoryBudget::new(128 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let sleep = call("sleep",vec![literal(V::Duration { value: i64::MAX,unit: TimeUnit::Millisecond })]);
        let expression = replace(call("format",vec![text("%s"),sleep]),"a");
        assert!(tokio::time::timeout(Duration::from_millis(1),evaluate_async(&expression,&Schema::default(),&[],&context)).await.is_err());
    }
    assert_eq!(memory.used_bytes(),0);
}

fn raw_build(emit: impl FnOnce(&mut rtdb_plan::RegexFormBuilder<'_>) -> rtdb_types::Result<()>) -> rtdb_plan::RegexForm {
    let mut builder = rtdb_plan::RegexFormBuilder::new_untracked();
    emit(&mut builder).unwrap();
    builder.finish().unwrap()
}
fn raw_literal(value: rtdb_types::Value) -> rtdb_plan::RegexForm { raw_build(|builder| builder.literal(&value)) }
fn raw_symbol(value: String) -> rtdb_plan::RegexForm { raw_build(|builder| builder.symbol(&value)) }
