use rtdb_types::StructFields;
use rtdb_expr::{arithmetic, decimal_rescale, decimal_sum, decimal_text};
use rtdb_plan::BinaryOp;
use rtdb_types::{DecimalValue, Value};

fn decimal(coefficient: i128, precision: i32, scale: i32) -> DecimalValue {
    DecimalValue::new(coefficient.to_be_bytes().to_vec(), precision, scale, 128).unwrap()
}

#[test]
fn signed_precision_never_sizes_decimal_rescaling_or_arithmetic_work() {
    for precision in [77, 256, i32::MAX] {
        assert!(decimal_rescale(&decimal(1, precision, 0), precision, i32::MAX, 128).is_err());
        assert!(decimal_rescale(&decimal(1, precision, i32::MIN), precision, 0, 128).is_err());
        assert_eq!(decimal_rescale(&decimal(i128::MAX, precision, 0), precision, 0, 128).unwrap(), decimal(i128::MAX, precision, 0));
        assert!(decimal_rescale(&decimal(i128::MAX, precision, 0), precision, 1, 128).is_err());
        assert_eq!(decimal_rescale(&decimal(0, precision, i32::MIN), precision, i32::MAX, 128).unwrap(), decimal(0, precision, i32::MAX));
        assert_eq!(decimal_text(&decimal(1, precision, i32::MAX)), "1E-2147483647");
        assert!(arithmetic(BinaryOp::Modulo, &Value::Decimal(decimal(1, precision, i32::MIN)), &Value::Decimal(decimal(-3, precision, i32::MAX))).is_err());
        let Value::Decimal(remainder) = arithmetic(BinaryOp::Modulo, &Value::Decimal(decimal(10, precision, 1)), &Value::Decimal(decimal(-3, precision, 1))).unwrap() else { panic!("Expected decimal") };
        assert_eq!((remainder.precision, remainder.scale, remainder.bit_width), (precision, 1, 256));
        assert_eq!(decimal_text(&remainder), "-0.2");
    }
    for precision in [i32::MIN, -1, 0] {
        assert!(decimal_rescale(&decimal(0, 32, 0), precision, 0, 128).is_err());
        assert!(decimal_rescale(&decimal(1, 32, 0), precision, 0, 128).is_err());
    }
}

#[test]
fn object_decimal_inference_checks_storage_and_uses_actual_digits() {
    use rtdb_expr::infer_decimal;
    assert_eq!(infer_decimal(&decimal(18, -1, i32::MAX)).unwrap(), decimal(18, 32, i32::MAX));
    let wide = infer_decimal(&decimal(i128::MIN, 1, i32::MIN)).unwrap();
    assert_eq!((wide.precision, wide.scale, wide.bit_width), (64, i32::MIN, 256));
    assert_eq!(&wide.unscaled[..16], &[255; 16]);
    assert_eq!(&wide.unscaled[16..], &i128::MIN.to_be_bytes());
    assert!(infer_decimal(&DecimalValue { unscaled: vec![0], precision: 32, scale: 0, bit_width: 128 }).is_err());
    let unsupported = infer_decimal(&DecimalValue::new(vec![127; 32], i32::MAX, 0, 256).unwrap()).unwrap_err();
    assert_eq!(unsupported.category, rtdb_types::AnomalyCategory::Unsupported);
    assert_eq!(unsupported.message.as_deref(), Some("Unsupported precision: 77"));
}

#[test]
fn decimal_vector_sum_ignores_declared_precision_but_checks_signed_storage_width() {
    let first = decimal(9, 1, 0);
    let sum = decimal_sum(None, Some(&first), 1, 0, 128).unwrap();
    assert_eq!(decimal_sum(Some(&sum), Some(&first), 1, 0, 128).unwrap(), decimal(18, 1, 0));
    let maximum = decimal(i128::MAX, i32::MAX, 0);
    assert!(decimal_sum(Some(&maximum), Some(&decimal(1, 1, 0)), i32::MAX, 0, 128).is_err());
    let minimum = decimal(i128::MIN, i32::MAX, 0);
    assert!(decimal_sum(Some(&minimum), Some(&decimal(-1, 1, 0)), i32::MAX, 0, 128).is_err());
    assert_eq!(decimal_sum(Some(&maximum), Some(&decimal(-i128::MAX, 39, 0)), i32::MAX, 0, 128).unwrap(), decimal(0, i32::MAX, 0));
    assert_eq!(decimal_sum(None, None, i32::MIN, i32::MAX, 128).unwrap(), decimal(0, i32::MIN, i32::MAX));
}

#[test]
fn decimal_vector_sum_nulls_and_differing_scales_store_the_addition_coefficient() {
    let zero = decimal_sum(None, None, 1, -2, 128).unwrap();
    assert_eq!(zero, decimal(0, 1, -2));
    let first = decimal_sum(Some(&zero), Some(&decimal(1, 1, -2)), 1, -2, 128).unwrap();
    assert_eq!(first, decimal(1, 1, -2));
    let after_null = decimal_sum(Some(&first), None, 1, -2, 128).unwrap();
    assert_eq!(after_null, decimal(100, 1, -2));
    assert_eq!(decimal_sum(Some(&after_null), Some(&decimal(2, 1, 0)), 1, -2, 128).unwrap(), decimal(10002, 1, -2));
    assert!(decimal_sum(Some(&decimal(1, 1, i32::MIN)), None, 1, i32::MIN, 128).is_err());
    assert!(decimal_sum(Some(&first), None, 2, -2, 128).is_err());
}

#[test]
fn batch_evaluation_copies_raw_decimal_outputs_and_retains_their_allocation() {
    use std::sync::Arc;
    use arrow_array::RecordBatch;
    use rtdb_columnar::{MemoryBudget, copy_array, from_field, read_value, to_field};
    use rtdb_expr::{CompiledExpr, EvalContext, evaluate_batch};
    use rtdb_plan::{ColumnId, Expr, Field, Schema};
    for precision in [-1, 1, 256, i32::MAX] {
        let value = Value::Decimal(decimal(123, precision, i32::MAX));
        let ty = value.logical_type();
        let schema = Schema::new(vec![Field { id: ColumnId(0), name: "v".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty.clone() }]).unwrap();
        let expression = CompiledExpr::compile(&Expr::column(ColumnId(0), ty.clone()), &schema).unwrap();
        let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(vec![to_field("v", &ty).unwrap()])), vec![copy_array(&ty, &[value.clone()]).unwrap()]).unwrap();
        let memory = MemoryBudget::new(16 << 20);
        let output = evaluate_batch(&expression, &batch, &EvalContext::default(), &memory).unwrap();
        assert_eq!(from_field(output.batch().schema().field(0)).unwrap(), ty);
        assert_eq!(read_value(output.batch().schema().field(0), output.batch().column(0).as_ref(), 0).unwrap(), value);
        let held = output.batch().clone();
        drop(output);
        assert!(memory.used_bytes() > 0);
        drop(held);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn computed_projections_write_selected_decimal_results_but_identity_copies_raw_cells() {
    use rtdb_expr::{EvalContext, evaluate, evaluate_projection};
    use rtdb_plan::{CastTarget, ColumnId, Expr, Field, Schema};
    let first = decimal(9, 1, 0);
    let previous = decimal_sum(None, Some(&first), 1, 0, 128).unwrap();
    let value = Value::Decimal(decimal_sum(Some(&previous), Some(&first), 1, 0, 128).unwrap());
    let ty = value.logical_type();
    let schema = Schema::new(vec![Field { id: ColumnId(0), name: "v".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty.clone() }]).unwrap();
    let column = Expr::column(ColumnId(0), ty.clone());
    let context = EvalContext::default();
    let row = [value.clone()];
    assert_eq!(evaluate_projection(&column, &schema, &row, &context).unwrap(), value);
    let packed = Expr::RowCopy(schema.fields().to_vec());
    assert_eq!(evaluate_projection(&packed, &schema, &row, &context).unwrap(), Value::Struct(StructFields::from([("v".into(), value.clone())])));
    assert!(evaluate_projection(&Expr::Coalesce(vec![packed, Expr::Literal(Value::Null)]), &schema, &row, &context).is_err());
    let default_cast = Expr::Cast { expr: Box::new(column.clone()), target: CastTarget::DecimalDefault };
    let fixed_cast = Expr::Cast { expr: Box::new(column.clone()), target: CastTarget::fixed(ty.clone()) };
    for expression in [
        default_cast.clone(), fixed_cast.clone(),
        Expr::Coalesce(vec![column.clone(), Expr::Literal(Value::Null)]),
        Expr::Call { name: "coalesce".into(), args: vec![column.clone(), Expr::Literal(Value::Null)], return_type: ty.clone() },
        Expr::Case { branches: vec![(Expr::Literal(Value::Bool(true)), column.clone())], otherwise: Box::new(Expr::Literal(Value::Null)) },
        Expr::List(vec![column.clone()]),
        Expr::Struct(vec![("v".into(), column.clone())]),
        Expr::Cast { expr: Box::new(Expr::List(vec![column.clone()])), target: CastTarget::List(Box::new(CastTarget::DecimalDefault)) },
    ] {
        assert!(evaluate_projection(&expression, &schema, &row, &context).is_err(), "{expression:?}");
    }
    for cast in [default_cast, fixed_cast] {
        assert_eq!(evaluate(&cast, &schema, &row, &context).unwrap(), value);
        let predicate = Expr::binary(BinaryOp::Gt, cast.clone(), Expr::Literal(Value::I64(0)));
        assert_eq!(evaluate_projection(&predicate, &schema, &row, &context).unwrap(), Value::Bool(true));
        let text = Expr::Cast { expr: Box::new(cast), target: CastTarget::fixed(rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Utf8)) };
        assert_eq!(evaluate_projection(&text, &schema, &row, &context).unwrap(), Value::Utf8("18".into()));
    }
    let skipped = Expr::Case { branches: vec![(Expr::Literal(Value::Bool(false)), column.clone())], otherwise: Box::new(Expr::Literal(Value::Null)) };
    assert_eq!(evaluate_projection(&skipped, &schema, &row, &context).unwrap(), Value::Null);
    let larger = Expr::Cast { expr: Box::new(column), target: CastTarget::decimal(Some(2), Some(0)).unwrap() };
    assert_eq!(evaluate_projection(&larger, &schema, &row, &context).unwrap(), Value::Decimal(decimal(18, 2, 0)));
}

#[test]
fn batch_projection_failure_releases_its_decimal_validation_reservation() {
    use std::sync::Arc;
    use arrow_array::RecordBatch;
    use rtdb_columnar::{MemoryBudget, copy_array, to_field};
    use rtdb_expr::{CompiledExpr, EvalContext, evaluate_batch};
    use rtdb_plan::{CastTarget, ColumnId, Expr, Field, Schema};
    let value = Value::Decimal(decimal(18, 1, 0));
    let ty = value.logical_type();
    let schema = Schema::new(vec![Field { id: ColumnId(0), name: "v".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty.clone() }]).unwrap();
    let cast = Expr::Cast { expr: Box::new(schema.fields()[0].expression()), target: CastTarget::fixed(ty.clone()) };
    let expression = CompiledExpr::compile(&cast, &schema).unwrap();
    let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(vec![to_field("v", &ty).unwrap()])), vec![copy_array(&ty, &[value]).unwrap()]).unwrap();
    let memory = MemoryBudget::new(16 << 20);
    assert_eq!(evaluate_batch(&expression, &batch, &EvalContext::default(), &memory).err().unwrap().code, "rtdb/decimal-precision");
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn projection_rejects_nonpositive_precision_and_checks_nested_cells_only_when_written() {
    use rtdb_expr::{EvalContext, evaluate_projection};
    use rtdb_plan::{CastTarget, ColumnId, Expr, Field, Schema};
    for precision in [i32::MIN, -1, 0] {
        let value = Value::Decimal(decimal(0, precision, i32::MAX));
        let ty = value.logical_type();
        let parameter = Expr::Parameter { index: 0, data_type: ty.clone().maybe() };
        let cast = Expr::Cast { expr: Box::new(parameter), target: CastTarget::fixed(ty.clone()) };
        assert!(evaluate_projection(&cast, &Schema::default(), &[], &EvalContext { parameters: &[value.clone()], ..Default::default() }).is_err());
        assert_eq!(evaluate_projection(&cast, &Schema::default(), &[], &EvalContext { parameters: &[Value::Null], ..Default::default() }).unwrap(), Value::Null);
        let nested = Value::list(vec![Value::Struct(StructFields::from([("v".into(), value)]))]);
        let schema = Schema::new(vec![Field { id: ColumnId(0), name: "nested".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: nested.logical_type() }]).unwrap();
        let column = schema.fields()[0].expression();
        assert_eq!(evaluate_projection(&column, &schema, &[nested.clone()], &EvalContext::default()).unwrap(), nested);
        let computed = Expr::Coalesce(vec![column, Expr::Literal(Value::Null)]);
        assert!(evaluate_projection(&computed, &schema, &[nested], &EvalContext::default()).is_err());
    }
}
