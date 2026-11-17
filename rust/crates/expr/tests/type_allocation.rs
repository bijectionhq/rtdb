use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext, EvaluationScope, expression_type};
use rtdb_plan::{Expr, Schema};
use rtdb_types::{LogicalType, ScalarType, Value};

#[test]
fn repeated_typing_releases_scratch_while_the_caller_scope_remains_alive() {
    let expression = Expr::Call { name: "character_length".into(), args: vec![Expr::Literal(Value::Utf8("x".into()))], return_type: LogicalType::Scalar(ScalarType::I32) };
    let memory = MemoryBudget::new(64 << 10);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope), ..Default::default() };
    for _ in 0..128 {
        assert_eq!(expression_type(&expression, &Schema::default(), &context).unwrap(), LogicalType::Scalar(ScalarType::I32));
    }
    assert!(memory.used_bytes() < 48 << 10);
    drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn ordinary_function_typing_borrows_large_literal_payloads() {
    let expression = Expr::Call { name: "character_length".into(), args: vec![Expr::Literal(Value::Utf8("x".repeat(1 << 20)))], return_type: LogicalType::Scalar(ScalarType::I32) };
    let memory = MemoryBudget::new(64 << 10);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope), ..Default::default() };
        assert_eq!(expression_type(&expression, &Schema::default(), &context).unwrap(), LogicalType::Scalar(ScalarType::I32));
        assert!(memory.peak_bytes() < memory.limit_bytes());
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn required_constant_typing_admits_payload_copies_before_allocation() {
    let expression = Expr::Call { name: "date_trunc".into(), args: vec![Expr::Literal(Value::Utf8("x".repeat(1 << 20))), Expr::Literal(Value::DateDay(0))], return_type: LogicalType::Scalar(ScalarType::DateDay) };
    let memory = MemoryBudget::new(64 << 10);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope), ..Default::default() };
        assert_eq!(expression_type(&expression, &Schema::default(), &context).unwrap_err().code, "rtdb/memory-budget-exceeded");
        assert!(memory.peak_bytes() < memory.limit_bytes());
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn required_constant_typing_admits_nested_field_names_before_cloning() {
    let scale = Expr::Field { expr: Box::new(Expr::Literal(Value::Null)), name: "x".repeat(1 << 20), data_type: LogicalType::Scalar(ScalarType::I64) };
    let expression = Expr::Call { name: "round".into(), args: vec![Expr::Literal(Value::F64(1.25)), scale], return_type: LogicalType::Scalar(ScalarType::F64) };
    let memory = MemoryBudget::new(64 << 10);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope), ..Default::default() };
        assert_eq!(expression_type(&expression, &Schema::default(), &context).unwrap_err().code, "rtdb/memory-budget-exceeded");
        assert!(memory.peak_bytes() < memory.limit_bytes());
    }
    assert_eq!(memory.used_bytes(), 0);
}
