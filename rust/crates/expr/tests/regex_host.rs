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

#[test]
fn host_numeric_comparison_reserves_cross_product_storage_before_allocation() {
    let number = F::BigInteger("9".repeat(512));
    let expression = replace(call("str",vec![call("==",vec![number.clone(),number])]));
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
fn host_decimal_comparison_admits_extreme_scale_without_expanding_coefficients() {
    let pattern = call("str",vec![call("<",vec![F::Decimal("1E-2147483647".into()),F::Decimal("1E+2147483647".into())])]);
    let expression = Expr::regex_replace(Expr::Literal(V::Utf8("true".into())),RegexReplacement { pattern,replacement: string("x"),..Default::default() });
    let memory = MemoryBudget::new(128 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let schema = Schema::default();
        let invocation = ExpressionInvocation::prepare(&expression,&schema,&context).unwrap();
        assert_eq!(invocation.evaluate(&[],&context).unwrap(),V::Utf8("x".into()));
        assert!(memory.peak_bytes() < memory.limit_bytes());
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn host_integral_division_admits_products_and_scale_expansion_before_allocation() {
    for (left,right,budget,code) in [
        (F::BigInteger("9".repeat(512)),F::BigInteger("7".repeat(512)),64 << 10,"rtdb/memory-budget-exceeded"),
        (call("/",vec![F::BigInteger("9".repeat(128)),F::Literal(V::I64(7))]),call("/",vec![F::Literal(V::I64(1)),F::Literal(V::I64(3))]),64 << 10,"rtdb/memory-budget-exceeded"),
        (F::Decimal("1E+2147483647".into()),F::Literal(V::I64(3)),128 << 20,"rtdb.expr/regex-host-limit"),
    ] {
        for operation in ["quot","rem","mod"] {
            let expression = replace(call("str",vec![call(operation,vec![left.clone(),right.clone()])]));
            let memory = MemoryBudget::new(budget);
            {
                let scope = EvaluationScope::new(&memory).unwrap();
                let context = EvalContext { allocation: Some(&scope),..Default::default() };
                assert_eq!(ExpressionInvocation::prepare(&expression,&Schema::default(),&context).err().unwrap().code,code);
                assert!(memory.peak_bytes() <= memory.limit_bytes());
            }
            assert_eq!(memory.used_bytes(),0);
        }
    }
}

#[test]
fn host_decimal_zero_quotient_keeps_extreme_scales_without_expanding_coefficients() {
    for operation in ["quot","rem","mod"] {
        let result = call(operation,vec![F::Decimal("1E-2147483647".into()),F::Decimal("1E+2147483647".into())]);
        let expected = operation == "quot";
        let pattern = call("if",vec![call("zero?",vec![result]),string(if expected { "a" } else { "b" }),string(if expected { "b" } else { "a" })]);
        let expression = replace(pattern);
        let memory = MemoryBudget::new(128 << 20);
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            let context = EvalContext { allocation: Some(&scope),..Default::default() };
            let schema = Schema::default();
            let invocation = ExpressionInvocation::prepare(&expression,&schema,&context).unwrap();
            assert_eq!(invocation.evaluate(&[],&context).unwrap(),V::Utf8("x".into()));
            assert!(memory.peak_bytes() < memory.limit_bytes());
        }
        assert_eq!(memory.used_bytes(),0);
    }
}

#[test]
fn host_bit_nary_expansion_retains_the_instruction_depth_limit() {
    for operation in ["bit-and","bit-or","bit-xor","bit-and-not"] {
        let expression = replace(call("str",vec![call(operation,vec![F::Literal(V::I64(1));129])]));
        assert_eq!(ExpressionInvocation::prepare(&expression,&Schema::default(),&EvalContext::default()).err().unwrap().code,"rtdb.expr/regex-host-limit");
    }
}

#[test]
fn host_bit_rejection_borrows_big_integer_payloads_and_releases_preparation_charges() {
    let expression = replace(call("str",vec![call("bit-and",vec![F::BigInteger("9".repeat(1024)),string("not numeric")])]));
    let memory = MemoryBudget::new(64 << 10);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let error = ExpressionInvocation::prepare(&expression,&Schema::default(),&context).err().unwrap();
        assert_eq!(error.message,"bit operation not supported for: class clojure.lang.BigInt");
        assert!(memory.peak_bytes() < memory.limit_bytes());
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn host_bit_count_magnitude_does_not_expand_preparation_storage() {
    let mut retained = None;
    for count in [0,-1,64,4294967296,i64::MIN,i64::MAX] {
        let pattern = call("str",vec![call("bit-shift-right",vec![F::Literal(V::I64(-1)),F::Literal(V::I64(count))])]);
        let expression = Expr::regex_replace(Expr::Literal(V::Utf8("-1".into())),RegexReplacement { pattern,replacement: string("x"),..Default::default() });
        let memory = MemoryBudget::new(128 << 20);
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            let context = EvalContext { allocation: Some(&scope),..Default::default() };
            let schema = Schema::default();
            let invocation = ExpressionInvocation::prepare(&expression,&schema,&context).unwrap();
            let prepared = memory.used_bytes();
            if let Some(expected) = retained { assert_eq!(prepared,expected); } else { retained = Some(prepared); }
            assert_eq!(invocation.evaluate(&[],&context).unwrap(),V::Utf8("x".into()));
        }
        assert_eq!(memory.used_bytes(),0);
    }
}

#[test]
fn host_extrema_nary_expansion_retains_the_instruction_depth_limit() {
    for operation in ["min","max"] {
        let expression = replace(call("str",vec![call(operation,vec![F::Literal(V::I64(1));129])]));
        assert_eq!(ExpressionInvocation::prepare(&expression,&Schema::default(),&EvalContext::default()).err().unwrap().code,"rtdb.expr/regex-host-limit");
    }
}

#[test]
fn host_extrema_and_absolute_value_reserve_numeric_scratch_before_allocation() {
    for (operation,arguments) in [
        ("min",vec![F::BigInteger("9".repeat(512)),F::BigInteger("8".repeat(512))]),
        ("max",vec![F::BigInteger("9".repeat(512)),F::BigInteger("8".repeat(512))]),
        ("abs",vec![F::BigInteger(format!("-{}","9".repeat(512)))]),
    ] {
        let expression = replace(call("str",vec![call(operation,arguments)]));
        let memory = MemoryBudget::new(64 << 10);
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            let context = EvalContext { allocation: Some(&scope),..Default::default() };
            assert_eq!(ExpressionInvocation::prepare(&expression,&Schema::default(),&context).err().unwrap().code,"rtdb/memory-budget-exceeded");
            assert!(memory.peak_bytes() <= memory.limit_bytes());
        }
        assert_eq!(memory.used_bytes(),0);
    }
}

#[test]
fn host_decimal_extrema_and_abs_keep_extreme_scales_without_expanding_coefficients() {
    for pattern in [
        call("if",vec![call("pos?",vec![call("min",vec![F::Decimal("1E-2147483647".into()),F::Decimal("1E+2147483647".into())])]),string("a"),string("b")]),
        call("if",vec![call("pos?",vec![call("abs",vec![F::Decimal("-1E-2147483647".into())])]),string("a"),string("b")]),
    ] {
        let expression = replace(pattern);
        let memory = MemoryBudget::new(128 << 20);
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            let context = EvalContext { allocation: Some(&scope),..Default::default() };
            let schema = Schema::default();
            let invocation = ExpressionInvocation::prepare(&expression,&schema,&context).unwrap();
            assert_eq!(invocation.evaluate(&[],&context).unwrap(),V::Utf8("x".into()));
            assert!(memory.peak_bytes() < memory.limit_bytes());
        }
        assert_eq!(memory.used_bytes(),0);
    }
}
