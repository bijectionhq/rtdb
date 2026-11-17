use std::sync::Arc;
use arrow_array::RecordBatch;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{CompiledExpr,EvalContext,EvaluationScope,ExpressionInvocation,evaluate_batch};
use rtdb_plan::{Expr,RegexForm as F,RegexReplacement,Schema};
use rtdb_types::Value as V;

fn string(value: &str) -> F { F::Literal(V::Utf8(value.into())) }
fn call(name: &str,args: Vec<F>) -> F { F::List(std::iter::once(F::Symbol(name.into())).chain(args).collect()) }
fn replace(pattern: F) -> Expr { Expr::regex_replace(Expr::Literal(V::Utf8("a".into())),RegexReplacement { pattern,replacement: string("x"),..Default::default() }) }

#[test]
fn host_preparation_runs_for_zero_rows_and_releases_failed_invocation_charges() {
    let expression = replace(call("str",vec![string("[")]));
    let compiled = CompiledExpr::compile(&expression,&Schema::default()).unwrap();
    let batch = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
    let memory = MemoryBudget::new(128 << 20);
    assert_eq!(evaluate_batch(&compiled,&batch,&EvalContext::default(),&memory).err().unwrap().code,"xtdb.expression/illegal-argument-exception");
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn host_program_temporaries_stay_on_the_preparation_scope() {
    let memory = MemoryBudget::new(128 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let expression = replace(call("str",vec![string("a")]));
        let schema = Schema::default();
        let invocation = ExpressionInvocation::prepare(&expression,&schema,&context).unwrap();
        let prepared = memory.used_bytes();
        for _ in 0..3 {
            {
                let scope = EvaluationScope::new(&memory).unwrap();
                assert_eq!(invocation.evaluate(&[],&EvalContext { allocation: Some(&scope),..Default::default() }).unwrap(),V::Utf8("x".into()));
            }
            assert_eq!(memory.used_bytes(),prepared);
        }
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn host_numeric_literal_storage_is_admitted_before_parsing_and_released_on_failure() {
    let expression = replace(call("str",vec![F::BigInteger("9".repeat(4096))]));
    let memory = MemoryBudget::new(64 << 10);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let schema = Schema::default();
        assert_eq!(ExpressionInvocation::prepare(&expression,&schema,&context).err().unwrap().code,"rtdb/memory-budget-exceeded");
        assert!(memory.peak_bytes() <= memory.limit_bytes());
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn host_arithmetic_reserves_scratch_before_big_integer_products() {
    let number = F::BigInteger("9".repeat(512));
    let expression = replace(call("str",vec![call("*",vec![number.clone(),number])]));
    let memory = MemoryBudget::new(64 << 10);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let schema = Schema::default();
        assert_eq!(ExpressionInvocation::prepare(&expression,&schema,&context).err().unwrap().code,"rtdb/memory-budget-exceeded");
        assert!(memory.peak_bytes() <= memory.limit_bytes());
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn host_arithmetic_admits_decimal_scale_growth_before_constructing_powers() {
    for name in ["+","inc"] {
        let mut args = vec![F::Decimal("1E-2147483647".into())];
        if name == "+" { args.push(F::Literal(V::I64(1))); }
        let expression = replace(call("str",vec![call(name,args)]));
        let memory = MemoryBudget::new(128 << 20);
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            let context = EvalContext { allocation: Some(&scope),..Default::default() };
            let schema = Schema::default();
            assert_eq!(ExpressionInvocation::prepare(&expression,&schema,&context).err().unwrap().code,"rtdb.expr/regex-host-limit");
            assert!(memory.peak_bytes() < 64 << 10);
        }
        assert_eq!(memory.used_bytes(),0);
    }
}

#[test]
fn host_arithmetic_bounds_generated_nary_instruction_depth() {
    let expression = replace(call("str",vec![call("+",vec![F::Literal(V::I64(1));129])]));
    assert_eq!(ExpressionInvocation::prepare(&expression,&Schema::default(),&EvalContext::default()).err().unwrap().code,"rtdb.expr/regex-host-limit");
}
