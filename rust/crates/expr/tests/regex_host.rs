use std::sync::Arc;
use arrow_array::RecordBatch;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{CompiledExpr,EvalContext,EvaluationScope,ExpressionInvocation,evaluate_batch};
use rtdb_plan::{Expr,RegexForm as F,RegexReplacement,Schema};
use rtdb_types::Value as V;

fn string(value: &str) -> F { raw_literal(V::Utf8(value.into())) }
fn call(name: &str,args: Vec<F>) -> F { raw_list(std::iter::once(raw_symbol(name.into())).chain(args).collect()) }
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
    let expression = replace(call("str",vec![raw_big_integer("9".repeat(4096))]));
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
fn host_ratio_carriers_admit_both_components_and_reject_invalid_exact_values() {
    let schema = Schema::default();
    let large = "9".repeat(4096);
    for spelling in [format!("{large}/2"), format!("1/{large}"), format!("{large}/0")] {
        let recipe = raw_build(|builder| builder.ratio(&spelling));
        let expression = replace(call("str", vec![recipe]));
        let memory = MemoryBudget::new(64 << 10);
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            let context = EvalContext { allocation: Some(&scope), ..Default::default() };
            let error = ExpressionInvocation::prepare(&expression, &schema, &context).err().unwrap();
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            assert!(memory.peak_bytes() <= memory.limit_bytes());
        }
        assert_eq!(memory.used_bytes(), 0);
    }

    for spelling in ["2/0", "4/2", "2/-3", "2/3/4"] {
        let recipe = raw_build(|builder| builder.ratio(spelling));
        let expression = replace(call("str", vec![recipe]));
        let error = ExpressionInvocation::prepare(&expression, &schema, &EvalContext::default()).err().unwrap();
        assert_eq!(error.message.as_deref(), Some("Invalid host numeric literal"), "{spelling}");
    }
}

#[test]
fn host_arithmetic_reserves_scratch_before_big_integer_products() {
    let number = raw_big_integer("9".repeat(512));
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
        let mut args = vec![raw_decimal("1E-2147483647".into())];
        if name == "+" { args.push(raw_literal(V::I64(1))); }
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
    let expression = replace(call("str",vec![call("+",vec![raw_literal(V::I64(1));129])]));
    assert_eq!(ExpressionInvocation::prepare(&expression,&Schema::default(),&EvalContext::default()).err().unwrap().code,"rtdb.expr/regex-host-limit");
}

#[test]
fn host_classifications_admit_exact_numbers_without_scale_expansion_and_release_scopes() {
    for (argument,predicate) in [
        (raw_big_integer("9".repeat(8192)),"integer?"),
        (raw_decimal("1E-2147483647".into()),"decimal?"),
        (raw_decimal("1E+2147483647".into()),"rational?"),
        (call("/",vec![raw_literal(V::I64(1)),raw_literal(V::I64(3))]),"ratio?"),
    ] {
        let expression = replace(call("if",vec![call(predicate,vec![argument]),string("a"),string("b")]));
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
    let expression = replace(call("str",vec![call("number?",vec![call("subs",vec![string("x"),raw_literal(V::I64(-1))])])]));
    let memory = MemoryBudget::new(128 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        assert_eq!(ExpressionInvocation::prepare(&expression,&Schema::default(),&context).err().unwrap().message.as_deref(),Some("Host substring indexes are out of range"));
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn host_classifications_reject_undefined_literal_carriers_without_reading_quoted_contents() {
    let decimal = rtdb_types::DecimalValue::new(vec![0;16],1,0,128).unwrap();
    for value in [V::I8(1),V::I16(1),V::I32(1),V::U8(1),V::U16(1),V::U32(1),V::U64(1),V::F16(0),V::F32(1.0),V::Decimal(decimal),V::Transit(vec![0xff])] {
        let expression = replace(call("str",vec![call("number?",vec![raw_literal(value.clone())])]));
        assert_eq!(ExpressionInvocation::prepare(&expression,&Schema::default(),&EvalContext::default()).err().unwrap().code,"rtdb.expr/regex-host-form");
        let quoted = call("quote",vec![raw_vector(vec![raw_literal(value)])]);
        let expression = replace(call("if",vec![call("number?",vec![quoted]),string("b"),string("a")]));
        let schema = Schema::default();
        let context = EvalContext::default();
        assert_eq!(ExpressionInvocation::prepare(&expression,&schema,&context).unwrap().evaluate(&[],&context).unwrap(),V::Utf8("x".into()));
    }
}

#[test]
fn host_call_admission_rejects_unknown_carriers_before_arguments_and_ignores_quoted_children() {
    let failing = call("subs",vec![string("x"),raw_literal(V::I64(-1))]);
    let decimal = rtdb_types::DecimalValue::new(vec![0;16],1,0,128).unwrap();
    for raw in [raw_literal(V::I8(1)),raw_literal(V::F32(1.0)),raw_literal(V::U64(1)),raw_literal(V::Decimal(decimal)),raw_literal(V::Transit(vec![0xff])),raw_tagged("unresolved-host-class".into(), F::default())] {
        let function = call("quote",vec![raw.clone()]);
        let expression = replace(raw_list(vec![function,failing.clone()]));
        let error = ExpressionInvocation::prepare(&expression,&Schema::default(),&EvalContext::default()).err().unwrap();
        assert_eq!(error.code,"rtdb.expr/regex-host-form");
        assert_eq!(error.message.as_deref(),Some("Host call admission requires an established host literal carrier"));
        for container in [raw_vector(vec![raw.clone()]),raw_map(vec![(string("k"),raw.clone())]),raw_set(vec![raw.clone()])] {
            let expression = replace(raw_list(vec![call("quote",vec![container]),failing.clone()]));
            assert_eq!(ExpressionInvocation::prepare(&expression,&Schema::default(),&EvalContext::default()).err().unwrap().message.as_deref(),Some("Host substring indexes are out of range"));
        }
    }
}

#[test]
fn host_call_admission_failure_releases_preparation_scope() {
    let failing = call("subs",vec![string("x"),raw_literal(V::I64(-1))]);
    let expression = replace(raw_list(vec![raw_literal(V::Bool(false)),failing]));
    let memory = MemoryBudget::new(128 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        assert_eq!(ExpressionInvocation::prepare(&expression,&Schema::default(),&context).err().unwrap().message.as_deref(),Some("Host value is not callable"));
        assert!(memory.peak_bytes() < memory.limit_bytes());
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn host_numeric_comparison_reserves_cross_product_storage_before_allocation() {
    let number = raw_big_integer("9".repeat(512));
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
    let pattern = call("str",vec![call("<",vec![raw_decimal("1E-2147483647".into()),raw_decimal("1E+2147483647".into())])]);
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
        (raw_big_integer("9".repeat(512)),raw_big_integer("7".repeat(512)),64 << 10,"rtdb/memory-budget-exceeded"),
        (call("/",vec![raw_big_integer("9".repeat(128)),raw_literal(V::I64(7))]),call("/",vec![raw_literal(V::I64(1)),raw_literal(V::I64(3))]),64 << 10,"rtdb/memory-budget-exceeded"),
        (raw_decimal("1E+2147483647".into()),raw_literal(V::I64(3)),128 << 20,"rtdb.expr/regex-host-limit"),
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
        let result = call(operation,vec![raw_decimal("1E-2147483647".into()),raw_decimal("1E+2147483647".into())]);
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
        let expression = replace(call("str",vec![call(operation,vec![raw_literal(V::I64(1));129])]));
        assert_eq!(ExpressionInvocation::prepare(&expression,&Schema::default(),&EvalContext::default()).err().unwrap().code,"rtdb.expr/regex-host-limit");
    }
}

#[test]
fn host_bit_rejection_borrows_big_integer_payloads_and_releases_preparation_charges() {
    let expression = replace(call("str",vec![call("bit-and",vec![raw_big_integer("9".repeat(1024)),string("not numeric")])]));
    let memory = MemoryBudget::new(64 << 10);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let error = ExpressionInvocation::prepare(&expression,&Schema::default(),&context).err().unwrap();
        assert_eq!(error.message.as_deref(),Some("bit operation not supported for: class clojure.lang.BigInt"));
        assert!(memory.peak_bytes() < memory.limit_bytes());
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn host_bit_count_magnitude_does_not_expand_preparation_storage() {
    let mut retained = None;
    for count in [0,-1,64,4294967296,i64::MIN,i64::MAX] {
        let pattern = call("str",vec![call("bit-shift-right",vec![raw_literal(V::I64(-1)),raw_literal(V::I64(count))])]);
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
        let expression = replace(call("str",vec![call(operation,vec![raw_literal(V::I64(1));129])]));
        assert_eq!(ExpressionInvocation::prepare(&expression,&Schema::default(),&EvalContext::default()).err().unwrap().code,"rtdb.expr/regex-host-limit");
    }
}

#[test]
fn host_extrema_and_absolute_value_reserve_numeric_scratch_before_allocation() {
    for (operation,arguments) in [
        ("min",vec![raw_big_integer("9".repeat(512)),raw_big_integer("8".repeat(512))]),
        ("max",vec![raw_big_integer("9".repeat(512)),raw_big_integer("8".repeat(512))]),
        ("abs",vec![raw_big_integer(format!("-{}","9".repeat(512)))]),
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
        call("if",vec![call("pos?",vec![call("min",vec![raw_decimal("1E-2147483647".into()),raw_decimal("1E+2147483647".into())])]),string("a"),string("b")]),
        call("if",vec![call("pos?",vec![call("abs",vec![raw_decimal("-1E-2147483647".into())])]),string("a"),string("b")]),
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

#[test]
fn host_substring_halves_recombine_before_pattern_compilation_without_changing_raw_wire() {
    let pattern = call("str",vec![
        call("subs",vec![string("😀"),raw_literal(V::I64(0)),raw_literal(V::I64(1))]),
        call("subs",vec![string("😀"),raw_literal(V::I64(1)),raw_literal(V::I64(2))]),
    ]);
    let expression = Expr::regex_replace(Expr::Literal(V::Utf8("😀".into())),RegexReplacement { pattern,replacement: string("x"),..Default::default() });
    let wire = serde_json::to_string(&expression).unwrap();
    let memory = MemoryBudget::new(128 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let schema = Schema::default();
        let invocation = ExpressionInvocation::prepare(&expression,&schema,&context).unwrap();
        assert_eq!(invocation.evaluate(&[],&context).unwrap(),V::Utf8("x".into()));
        assert_eq!(serde_json::to_string(&expression).unwrap(),wire);
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn host_isolated_substrings_compile_without_becoming_question_or_replacement_characters() {
    for (start,end) in [(0,1),(1,2)] {
        for source in ["?","�"] {
            let pattern = call("subs",vec![string("😀"),raw_literal(V::I64(start)),raw_literal(V::I64(end))]);
            let expression = Expr::regex_replace(Expr::Literal(V::Utf8(source.into())),RegexReplacement { pattern,replacement: string("x"),..Default::default() });
            let memory = MemoryBudget::new(128 << 20);
            {
                let scope = EvaluationScope::new(&memory).unwrap();
                let context = EvalContext { allocation: Some(&scope),..Default::default() };
                let schema = Schema::default();
                let invocation = ExpressionInvocation::prepare(&expression,&schema,&context).unwrap();
                assert_eq!(invocation.evaluate(&[],&context).unwrap(),V::Utf8(source.into()));
            }
            assert_eq!(memory.used_bytes(),0);
        }
    }
}

#[test]
fn empty_batches_prepare_invalid_unit_patterns_and_release_their_retained_text() {
    let expression = replace(call("str",vec![call("subs",vec![string("😀"),raw_literal(V::I64(0)),raw_literal(V::I64(1))]),string("[")]));
    let compiled = CompiledExpr::compile(&expression,&Schema::default()).unwrap();
    let batch = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
    let memory = MemoryBudget::new(128 << 20);
    assert_eq!(evaluate_batch(&compiled,&batch,&EvalContext::default(),&memory).err().unwrap().code,"xtdb.expression/illegal-argument-exception");
    assert_eq!(memory.used_bytes(),0);
}

fn raw_build(emit: impl FnOnce(&mut rtdb_plan::RegexFormBuilder<'_>) -> rtdb_types::Result<()>) -> rtdb_plan::RegexForm {
    let mut builder = rtdb_plan::RegexFormBuilder::new_untracked();
    emit(&mut builder).unwrap();
    builder.finish().unwrap()
}
fn raw_literal(value: rtdb_types::Value) -> rtdb_plan::RegexForm { raw_build(|builder| builder.literal(&value)) }
fn raw_symbol(value: String) -> rtdb_plan::RegexForm { raw_build(|builder| builder.symbol(&value)) }
fn raw_big_integer(value: String) -> rtdb_plan::RegexForm { raw_build(|builder| builder.big_integer(&value)) }
fn raw_decimal(value: String) -> rtdb_plan::RegexForm { raw_build(|builder| builder.decimal(&value)) }
fn raw_list(values: Vec<rtdb_plan::RegexForm>) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_list()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
fn raw_vector(values: Vec<rtdb_plan::RegexForm>) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_vector()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
fn raw_set(values: Vec<rtdb_plan::RegexForm>) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_set()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
fn raw_map(values: Vec<(rtdb_plan::RegexForm, rtdb_plan::RegexForm)>) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_map()?; for (key, value) in values { builder.append_view(key.view())?; builder.append_view(value.view())?; } builder.end() }) }
fn raw_tagged(tag: String, value: rtdb_plan::RegexForm) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_tagged(&tag)?; builder.append_view(value.view())?; builder.end() }) }
