use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use rtdb_columnar::{MemoryBudget, copy_array, read_value, to_field};
use rtdb_expr::{
    ArrowParameterCell, CompiledExpr, EvalContext, EvaluationScope, ExpressionInvocation,
    ExpressionRow, RowInput, evaluate_batch, evaluate_projection,
};
use rtdb_plan::{
    BinaryOp as B, CastTarget, ColumnId, Expr, Field, Schema, function_type_with_args,
};
use rtdb_types::{DecimalValue, LogicalType, Result, ScalarType, Value};

fn decimal(coefficient: i128, precision: i32, scale: i32) -> Value {
    Value::Decimal(
        DecimalValue::new(coefficient.to_be_bytes().to_vec(), precision, scale, 128).unwrap(),
    )
}

fn factor() -> Expr {
    Expr::Literal(decimal(10_i128.pow(31), 32, 0))
}

fn square(input: Expr) -> Expr {
    Expr::binary(B::Multiply, input.clone(), input)
}

fn cast(input: Expr, target: CastTarget) -> Expr {
    Expr::Cast {
        expr: Box::new(input),
        target,
    }
}

fn fixed(input: Expr, precision: i64, scale: i64) -> Expr {
    cast(
        input,
        CastTarget::decimal(Some(precision), Some(scale)).unwrap(),
    )
}

fn text(input: Expr) -> Expr {
    cast(
        input,
        CastTarget::fixed(LogicalType::Scalar(ScalarType::Utf8)),
    )
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    let return_type = function_type_with_args(name, &args).unwrap();
    Expr::Call {
        name: name.into(),
        args,
        return_type,
    }
}

fn project(expr: &Expr) -> Result<Value> {
    evaluate_projection(expr, &Schema::default(), &[], &EvalContext::default())
}

#[test]
fn nested_decimal_arithmetic_can_cancel_a_coefficient_wider_than_decimal256() {
    let big = square(square(factor()));
    let one = Expr::Literal(Value::I64(1));
    let next = Expr::binary(B::Add, big.clone(), one.clone());
    let difference = Expr::binary(B::Subtract, next.clone(), big.clone());
    assert_eq!(project(&difference).unwrap(), decimal(1, 32, 0));
    assert_eq!(
        project(&Expr::binary(B::Gt, next.clone(), big.clone())).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        project(&Expr::binary(B::Eq, next.clone(), big.clone())).unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        project(&call("compare", vec![next, big.clone()])).unwrap(),
        Value::I32(0)
    );
    assert_eq!(
        project(&text(big.clone())).unwrap(),
        Value::Utf8(format!("1{}", "0".repeat(124)))
    );
    assert_eq!(
        project(&big).unwrap_err().code,
        "xtdb.expression/overflow-error"
    );

    let divide = Expr::binary(B::Divide, square(factor()), factor());
    let rounded = fixed(divide, 32, 0);
    assert_eq!(project(&rounded).unwrap(), decimal(10_i128.pow(31), 32, 0));
}

#[test]
fn three_way_comparison_of_a_computed_decimal_retains_signed_numeric_reads() {
    let zero = Expr::binary(B::Subtract, square(factor()), square(factor()));
    let negative_one = Expr::Literal(Value::U64(u64::MAX));
    assert_eq!(
        project(&call("compare", vec![zero, negative_one])).unwrap(),
        Value::I32(1)
    );
}

#[test]
fn explicit_precision_is_checked_when_the_selected_result_is_materialized() {
    let text_literal = Expr::Literal(Value::Utf8("123.455".into()));
    let narrow = fixed(text_literal, 2, 2);
    let selected = Expr::Case {
        branches: vec![(Expr::Literal(Value::Bool(true)), narrow.clone())],
        otherwise: Box::new(Expr::Literal(Value::Null)),
    };
    assert_eq!(
        project(&text(selected.clone())).unwrap(),
        Value::Utf8("123.46".into())
    );
    assert_eq!(
        project(&text(cast(selected, CastTarget::DecimalDefault))).unwrap(),
        Value::Utf8("123.46".into())
    );
    assert_eq!(
        project(&fixed(narrow.clone(), 5, 2)).unwrap(),
        decimal(12346, 5, 2)
    );
    assert_eq!(
        project(&fixed(narrow.clone(), 2, -1)).unwrap(),
        decimal(12, 2, -1)
    );
    assert_eq!(
        project(&narrow).unwrap_err().code,
        "xtdb.expression/overflow-error"
    );
    assert_eq!(
        project(&Expr::List(vec![narrow])).unwrap_err().code,
        "xtdb.expression/overflow-error"
    );

    let wider = fixed(square(factor()), 64, 0);
    let Value::Decimal(result) = project(&wider).unwrap() else {
        panic!("expected decimal");
    };
    assert_eq!(
        (result.precision, result.scale, result.bit_width),
        (64, 0, 256)
    );
    assert_eq!(
        rtdb_expr::decimal_text(&result),
        format!("1{}", "0".repeat(62))
    );
}

#[test]
fn signed_storage_width_is_checked_even_when_declared_precision_fits() {
    let above_i128_max = (i128::MAX as u128 + 1).to_string();
    let field = LogicalType::Scalar(ScalarType::Decimal {
        precision: 39,
        scale: 0,
        bit_width: 128,
    });
    let too_wide = cast(
        Expr::Literal(Value::Utf8(above_i128_max.clone())),
        CastTarget::fixed(field),
    );
    assert_eq!(
        project(&text(too_wide.clone())).unwrap(),
        Value::Utf8(above_i128_max)
    );
    assert_eq!(
        project(&too_wide).unwrap_err().code,
        "xtdb.expression/overflow-error"
    );
}

#[test]
fn decimal_consumers_preserve_null_shortcuts_and_reached_errors() {
    let wide = square(factor());
    let null = Expr::Literal(Value::Null);
    let divide = Expr::binary(B::Divide, wide.clone(), Expr::Literal(Value::I64(0)));
    assert_eq!(
        project(&text(divide.clone())).unwrap_err().code,
        "xtdb.expression/division-by-zero"
    );
    assert_eq!(
        project(&Expr::binary(B::Add, divide.clone(), null.clone())).unwrap(),
        Value::Null
    );
    let boxed = Expr::Case {
        branches: vec![(Expr::Literal(Value::Bool(true)), divide)],
        otherwise: Box::new(null.clone()),
    };
    assert_eq!(
        project(&Expr::binary(B::Add, boxed, null.clone()))
            .unwrap_err()
            .code,
        "xtdb.expression/division-by-zero"
    );
    assert_eq!(
        project(&call("nullif", vec![wide.clone(), wide.clone()])).unwrap(),
        Value::Null
    );
    assert_eq!(
        project(&Expr::binary(B::IsDistinctFrom, wide.clone(), null.clone())).unwrap(),
        Value::Bool(true)
    );
    let contains = Expr::In {
        expr: Box::new(wide.clone()),
        values: vec![null, wide],
        negated: false,
    };
    assert_eq!(project(&contains).unwrap(), Value::Bool(true));
}

#[test]
fn coefficient_admission_bounds_alignment_without_expanding_the_scale_of_comparison() {
    let exponent = Expr::Literal(decimal(1, 32, -4096));
    let zero = Expr::Literal(Value::I64(0));
    assert_eq!(
        project(&Expr::binary(B::Gt, exponent.clone(), zero.clone())).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        project(&text(exponent.clone())).unwrap(),
        Value::Utf8("1E+4096".into())
    );
    let aligned = Expr::binary(B::Add, exponent, zero);
    assert_eq!(
        project(&text(aligned)).unwrap_err().code,
        "xtdb.expression/overflow-error"
    );
}

#[test]
fn scalar_decimal_growth_is_charged_to_the_evaluation_scope() {
    let render = |scale| {
        text(Expr::binary(
            B::Add,
            Expr::Literal(decimal(1, 32, scale)),
            Expr::Literal(Value::I64(0)),
        ))
    };
    let schema = Schema::default();
    let memory = MemoryBudget::new(256 << 10);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext {
        allocation: Some(&scope),
        ..Default::default()
    };
    assert_eq!(
        evaluate_projection(&render(-1), &schema, &[], &context).unwrap(),
        Value::Utf8("10".into())
    );
    scope.reset().unwrap();
    let large = render(-4095);
    assert_eq!(
        evaluate_projection(&large, &schema, &[], &context)
            .unwrap_err()
            .code,
        "rtdb/memory-budget-exceeded"
    );
    drop(scope);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(
        project(&large).unwrap(),
        Value::Utf8(format!("1{}", "0".repeat(4095)))
    );
}

#[test]
fn scalar_modulo_passes_its_remainder_scale_to_text() {
    let left = Expr::Literal(decimal(4, 32, 1000));
    let right = Expr::Literal(decimal(2, 32, 1001));
    let remainder = call("mod", vec![left, right]);
    assert_eq!(
        project(&text(remainder.clone())).unwrap(),
        Value::Utf8("0E-1000".into())
    );
    assert_eq!(
        project(&remainder).unwrap_err().code,
        "xtdb.expression/overflow-error"
    );
}

struct ArrowRow<'a> {
    field: &'a arrow_schema::Field,
    array: &'a ArrayRef,
}

impl RowInput for ArrowRow<'_> {
    fn value(&self, index: usize, _: Option<&EvaluationScope>) -> Result<Option<Value>> {
        (index == 0)
            .then(|| read_value(self.field, self.array.as_ref(), 0))
            .transpose()
    }

    fn arrow_cell(&self, index: usize) -> Option<ArrowParameterCell<'_>> {
        (index == 0).then_some(ArrowParameterCell {
            field: self.field,
            array: self.array.as_ref(),
            row: 0,
        })
    }
}

#[tokio::test]
async fn arrow_inputs_keep_wide_decimal_arithmetic_until_the_projection_result() {
    let value = decimal(10_i128.pow(31), 32, 0);
    let ty = value.logical_type();
    let field = to_field("factor", &ty).unwrap();
    let array = copy_array(&ty, &[value]).unwrap();
    let schema = Schema::new(vec![Field {
        id: ColumnId(0),
        name: "factor".into(),
        qualifier: None,
        qualified_only: false,
        unnamed: false,
        data_type: ty,
    }])
    .unwrap();
    let row = ArrowRow {
        field: &field,
        array: &array,
    };
    let wide = square(schema.fields()[0].expression());
    let expression = text(wide.clone());
    let memory = MemoryBudget::new(16 << 20);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext {
        allocation: Some(&scope),
        ..Default::default()
    };
    let invocation = ExpressionInvocation::prepare(&expression, &schema, &context).unwrap();
    let result = invocation
        .evaluate_projection_row_async(ExpressionRow::Borrowed(&row), &context)
        .await
        .unwrap();
    assert_eq!(result, Value::Utf8(format!("1{}", "0".repeat(62))));

    let input = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![field])),
        vec![array],
    )
    .unwrap();
    let bound = CompiledExpr::compile(&wide, &schema).unwrap();
    assert_eq!(
        evaluate_batch(&bound, &input, &EvalContext::default(), &memory)
            .err()
            .unwrap()
            .code,
        "xtdb.expression/overflow-error"
    );
}
