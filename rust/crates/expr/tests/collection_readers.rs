use rtdb_types::StructFields;
use std::{sync::Arc, time::Duration};
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,ExpressionInvocation,ExpressionRow,ParameterLookup,Parameters,RowInput,evaluate,evaluate_async,function_type};
use rtdb_plan::{BinaryOp,CastTarget,ColumnId,Expr,Field,Schema,UnaryOp};
use rtdb_types::{CivilDate,DateUnit,LogicalType,ScalarType,TimeUnit,Value};

fn call(name: &str,args: Vec<Expr>) -> Expr {
    let return_type = function_type(name,&args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap();
    Expr::Call { name: name.into(),args,return_type }
}
fn literal(value: Value) -> Expr { Expr::Literal(value) }
fn sleep(millis: i64) -> Expr { call("sleep",vec![literal(Value::Duration { value: millis,unit: TimeUnit::Millisecond })]) }
fn wide(unit: DateUnit) -> Expr { literal(Value::calendar_date(CivilDate::MAX,unit)) }
fn text(expr: Expr) -> Expr { Expr::Cast { expr: Box::new(expr),target: CastTarget::Fixed(LogicalType::Scalar(ScalarType::Utf8)) } }
fn field(expr: Expr,name: &str) -> Expr { let data_type = match expr.data_type() { LogicalType::Struct(fields) => fields[name].clone(),_ => panic!() };Expr::Field { expr: Box::new(expr),name: name.into(),data_type } }
fn nth(expr: Expr,index: i64) -> Expr { call("nth",vec![expr,literal(Value::I64(index))]) }
fn run(expr: &Expr) -> rtdb_types::Result<Value> { evaluate(expr,&Schema::default(),&[],&EvalContext::default()) }

#[test]
fn hot_expression_prepared_literals_reuse_packed_buffers_across_rows() {
    let negative_zero = Expr::unary(UnaryOp::Negate, literal(Value::F64(0.0)));
    let expression = Expr::List(vec![literal(Value::I64(2)), negative_zero, literal(Value::Null)]);
    let schema = Schema::default();
    let memory = MemoryBudget::new(1 << 20);
    let preparation = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&preparation), ..EvalContext::default() };
    let invocation = ExpressionInvocation::prepare(&expression, &schema, &context).unwrap();
    let first_row = EvaluationScope::new(&memory).unwrap();
    let second_row = EvaluationScope::new(&memory).unwrap();
    let first = invocation.evaluate(&[], &EvalContext { allocation: Some(&first_row), ..context }).unwrap();
    let second = invocation.evaluate(&[], &EvalContext { allocation: Some(&second_row), ..context }).unwrap();
    let (Value::List(first), Value::List(second)) = (first, second) else { panic!("expected lists"); };
    let (_, first_array) = first.packed_column().unwrap();
    let (_, second_array) = second.packed_column().unwrap();
    assert!(first_array.to_data().ptr_eq(&second_array.to_data()));
    assert_eq!(first.get(0).unwrap().as_ref(), &Value::I64(2));
    let Value::F64(zero) = *first.get(1).unwrap() else { panic!("expected a double"); };
    assert_eq!(zero.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(first.get(2).unwrap().as_ref(), &Value::Null);
}

#[tokio::test]
async fn hot_expression_cosine_borrows_plain_columns_and_parameters() {
    use arrow_array::RecordBatch;
    use rtdb_columnar::{ArrowCell, copy_array, to_field};
    use rtdb_types::{Anomaly, Result};

    struct ArrowInput { batch: RecordBatch, row: usize }
    fn unexpected_read() -> Anomaly { Anomaly::fault("test/scalar-read", "Scalar input was demanded") }
    impl RowInput for ArrowInput {
        fn value(&self, _: usize, _: Option<&EvaluationScope>) -> Result<Option<Value>> { Err(unexpected_read()) }
        fn arrow_cell(&self, index: usize) -> Option<ArrowCell<'_>> {
            Some(ArrowCell {
                field: self.batch.schema_ref().fields().get(index)?,
                array: self.batch.columns().get(index)?.as_ref(),
                row: self.row,
            })
        }
    }
    impl ParameterLookup for ArrowInput {
        fn width(&self) -> usize { self.batch.num_columns() }
        fn value(&self, _: usize) -> Result<Option<&Value>> { Err(unexpected_read()) }
        fn known_value(&self, _: usize) -> Option<&Value> { None }
        fn arrow_cell(&self, index: usize) -> Option<ArrowCell<'_>> { RowInput::arrow_cell(self, index) }
    }

    let ty = LogicalType::list(LogicalType::Scalar(ScalarType::F64).maybe()).maybe();
    let vector = |values: &[f64]| Value::list(values.iter().copied().map(Value::F64).collect());
    let left = vector(&[4.0, -0.0, 1e-90]);
    let right = vector(&[-2.0, 6.0, 2e90]);
    let bad = Value::list(vec![Value::F64(1.0), Value::Null, Value::F64(2.0)]);
    let schema = Schema::new(["v", "w", "unreadable"].into_iter().enumerate().map(|(index, name)| Field {
        id: ColumnId(index as u32), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty.clone(),
    }).collect()).unwrap();
    let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(vec![to_field("v", &ty).unwrap(), to_field("w", &ty).unwrap()])), vec![
        copy_array(&ty, &[left.clone(), Value::Null, vector(&[0.0; 3]), bad]).unwrap(),
        copy_array(&ty, &[right.clone(), right.clone(), right.clone(), right.clone()]).unwrap(),
    ]).unwrap();
    let parameter = ArrowInput { batch: batch.clone(), row: 0 };
    let parameter_types = [ty.clone(), ty.clone()];
    let context = EvalContext { parameters: Parameters::Lookup(&parameter), parameter_types: Some(&parameter_types), ..EvalContext::default() };
    let column = |id| Expr::Column { id: ColumnId(id), data_type: ty.clone() };
    let expected = run(&call("cosine_distance", vec![literal(left), literal(right)])).unwrap();
    let Value::F64(expected) = expected else { panic!("expected a score"); };
    for (other, parameter_types) in [
        (column(1), Some(parameter_types.as_slice())),
        (Expr::Parameter { index: 1, data_type: ty.clone() }, Some(parameter_types.as_slice())),
        (Expr::Parameter { index: 1, data_type: LogicalType::Nothing }, None),
    ] {
        let context = EvalContext { parameter_types, ..context };
        let expression = call("cosine_distance", vec![column(0), other]);
        let invocation = ExpressionInvocation::prepare(&expression, &schema, &context).unwrap();
        for row in 0..4 {
            let input = ArrowInput { batch: batch.clone(), row };
            let result = invocation.evaluate_projection_row_async(ExpressionRow::Borrowed(&input), &context).await;
            match row {
                0 => {
                    let Value::F64(value) = result.unwrap() else { panic!("expected a score"); };
                    assert_eq!(value.to_bits(), expected.to_bits());
                }
                1 => assert_eq!(result.unwrap(), Value::Null),
                2 => assert_eq!(result.unwrap_err().code, "rtdb.vector/zero-norm"),
                _ => assert_eq!(result.unwrap_err().code, "rtdb.vector/component"),
            }
        }
    }

    let expression = call("cosine_distance", vec![column(0), column(2)]);
    let invocation = ExpressionInvocation::prepare(&expression, &schema, &context).unwrap();
    for row in [1, 2, 3] {
        let input = ArrowInput { batch: batch.clone(), row };
        let error = invocation.evaluate_projection_row_async(ExpressionRow::Borrowed(&input), &context).await.unwrap_err();
        assert_eq!(error.code, "test/scalar-read");
    }
    let expression = call("cosine_distance", vec![column(0), column(1)]);
    let invocation = ExpressionInvocation::prepare(&expression, &schema, &context).unwrap();
    let wide_date = Value::list(vec![Value::calendar_date(CivilDate::MAX, DateUnit::Day)]);
    assert_eq!(invocation.evaluate(&[Value::Null, wide_date.clone()], &context).unwrap(), Value::Null);
    assert_eq!(invocation.evaluate(&[wide_date, Value::I64(1)], &context).unwrap_err().code, "rtdb.vector/type");
}

#[test]
fn struct_fields_are_boxed_before_field_selection_and_outer_text_cast() {
    assert_eq!(run(&text(wide(DateUnit::Day))).unwrap(),Value::Utf8(CivilDate::MAX.to_string()));
    for selected in ["wide","other"] {
        let record = Expr::Struct(vec![("wide".into(),wide(DateUnit::Day)),("other".into(),literal(Value::I64(7)))]);
        assert_eq!(run(&text(field(record,selected))).unwrap_err().code,"xtdb.expression/overflow-error");
    }
}

#[test]
fn list_size_type_and_null_observers_do_not_read_cells() {
    let list = Expr::List(vec![wide(DateUnit::Day),sleep(-1)]);
    for (expr,expected) in [
        (call("cardinality",vec![list.clone()]),Value::I32(2)),
        (call("length",vec![list.clone()]),Value::I32(2)),
        (call("boolean",vec![list.clone()]),Value::Bool(true)),
        (call("nil?",vec![list.clone()]),Value::Bool(false)),
        (Expr::Unary { op: UnaryOp::IsNotNull,expr: Box::new(list.clone()) },Value::Bool(true)),
        (nth(list.clone(),-1),Value::Null),(nth(list,2),Value::Null),
    ] { assert_eq!(run(&expr).unwrap(),expected); }
}

#[test]
fn nth_boxes_only_its_selected_cell_and_nested_struct_fields() {
    let list = Expr::List(vec![literal(Value::I64(7)),wide(DateUnit::Day),Expr::Struct(vec![("d".into(),wide(DateUnit::Day))])]);
    assert_eq!(run(&nth(list.clone(),0)).unwrap(),Value::I64(7));
    assert!(run(&text(nth(list.clone(),1))).is_err());
    assert!(run(&nth(list,2)).is_err());
    let nested = Expr::List(vec![Expr::List(vec![wide(DateUnit::Day)])]);
    assert_eq!(run(&call("cardinality",vec![nth(nested,0)])).unwrap(),Value::I32(1));
}

#[test]
fn list_comparison_checks_sizes_then_short_circuits_definite_inequality() {
    let bad = || Expr::List(vec![wide(DateUnit::Day)]);
    assert_eq!(run(&Expr::binary(BinaryOp::Eq,bad(),Expr::List(vec![]))).unwrap(),Value::Bool(false));
    let left = Expr::List(vec![literal(Value::I64(1)),wide(DateUnit::Day)]);
    let right = Expr::List(vec![literal(Value::I64(2)),wide(DateUnit::Day)]);
    assert_eq!(run(&Expr::binary(BinaryOp::Eq,left.clone(),right.clone())).unwrap(),Value::Bool(false));
    assert_eq!(run(&call("===",vec![left,right])).unwrap(),Value::Bool(false));
    assert!(run(&Expr::binary(BinaryOp::Eq,bad(),bad())).is_err());
}

#[test]
fn list_cast_keeps_unread_cells_deferred() {
    let cast = Expr::Cast { expr: Box::new(Expr::List(vec![literal(Value::Utf8("7".into())),literal(Value::Utf8("bad".into()))])),target: CastTarget::List(Box::new(CastTarget::Fixed(LogicalType::Scalar(ScalarType::I64)))) };
    assert_eq!(run(&call("cardinality",vec![cast.clone()])).unwrap(),Value::I32(2));
    assert_eq!(run(&nth(cast.clone(),0)).unwrap(),Value::I64(7));
    assert!(run(&nth(cast,1)).is_err());
}

#[test]
fn list_union_cast_retains_unread_nested_lists_and_remaps_selected_scalars() {
    let integer = LogicalType::Scalar(ScalarType::I64);
    let date = LogicalType::Scalar(ScalarType::DateDay);
    let element = LogicalType::join_all(&[integer,LogicalType::list(date)]);
    let value = Expr::List(vec![literal(Value::I64(4)),Expr::List(vec![wide(DateUnit::Day)])]);
    let cast = Expr::Cast { expr: Box::new(value),target: CastTarget::List(Box::new(CastTarget::Fixed(element))) };
    assert_eq!(run(&nth(cast.clone(),0)).unwrap(),Value::I64(4));
    assert_eq!(run(&call("cardinality",vec![nth(cast,1)])).unwrap(),Value::I32(1));
}

#[test]
fn set_size_observation_preserves_distinct_forms_that_evaluate_to_equal_values() {
    let set = call("set",vec![literal(Value::I64(1)),Expr::binary(BinaryOp::Add,literal(Value::I64(0)),literal(Value::I64(1))),wide(DateUnit::Day)]);
    assert_eq!(run(&call("length",vec![set])).unwrap(),Value::I32(3));
}

#[test]
fn millisecond_date_boxes_preserve_full_days_until_vector_output() {
    let record = Expr::Struct(vec![("date".into(),wide(DateUnit::Millisecond))]);
    assert_eq!(run(&text(field(record.clone(),"date"))).unwrap(),Value::Utf8(CivilDate::MAX.to_string()));
    assert_eq!(run(&text(nth(Expr::List(vec![wide(DateUnit::Millisecond)]),0))).unwrap(),Value::Utf8(CivilDate::MAX.to_string()));
    assert_eq!(run(&record).unwrap_err().code, "xtdb.expression/overflow-error");
}

#[test]
fn struct_identity_cast_and_patch_keep_nested_lists_unread() {
    let record = Expr::Struct(vec![("list".into(),Expr::List(vec![wide(DateUnit::Day)]))]);
    let target = CastTarget::Fixed(record.data_type());
    let cast = Expr::Cast { expr: Box::new(record.clone()),target };
    assert_eq!(run(&call("cardinality",vec![field(cast,"list")])).unwrap(),Value::I32(1));
    let patch = call("_patch",vec![record,Expr::Struct(vec![("other".into(),literal(Value::I64(5)))])]);
    assert_eq!(run(&call("cardinality",vec![field(patch,"list")])).unwrap(),Value::I32(1));
}

#[tokio::test(start_paused = true)]
async fn list_nth_repeats_suspension_after_comparison_without_memoizing_cells() {
    let list = Expr::List(vec![sleep(5),literal(Value::I64(1))]);
    let other = Expr::List(vec![literal(Value::Null),literal(Value::I64(2))]);
    let expression = call("nullif",vec![list,other]);
    let start = tokio::time::Instant::now();
    assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap(),Value::list(vec![Value::Null,Value::I64(1)]));
    assert_eq!(tokio::time::Instant::now() - start,Duration::from_millis(10));
}

#[tokio::test(start_paused = true)]
async fn struct_scalar_cells_are_eager_and_not_reevaluated_after_comparison() {
    let record = Expr::Struct(vec![("sleep".into(),sleep(5)),("value".into(),literal(Value::I64(1)))]);
    let other = Expr::Struct(vec![("sleep".into(),literal(Value::Null)),("value".into(),literal(Value::I64(2)))]);
    let expression = call("nullif",vec![record,other]);
    let start = tokio::time::Instant::now();
    assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap(),Value::Struct(StructFields::from([("sleep".into(),Value::Null),("value".into(),Value::I64(1))])));
    assert_eq!(tokio::time::Instant::now() - start,Duration::from_millis(5));
}

#[tokio::test(start_paused = true)]
async fn async_size_and_unselected_cells_never_start_sleep() {
    let list = Expr::List(vec![sleep(i64::MAX),literal(Value::I64(4))]);
    let start = tokio::time::Instant::now();
    assert_eq!(evaluate_async(&call("cardinality",vec![list.clone()]),&Schema::default(),&[],&EvalContext::default()).await.unwrap(),Value::I32(2));
    assert_eq!(evaluate_async(&nth(list,1),&Schema::default(),&[],&EvalContext::default()).await.unwrap(),Value::I64(4));
    assert_eq!(tokio::time::Instant::now(),start);
}

#[tokio::test(start_paused = true)]
async fn cancelled_selected_list_cell_releases_the_invocation_scope() {
    let memory = MemoryBudget::new(1 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let expr = nth(Expr::List(vec![sleep(i64::MAX)]),0);
        assert!(tokio::time::timeout(Duration::from_millis(5),evaluate_async(&expr,&Schema::default(),&[],&context)).await.is_err());
        assert!(memory.used_bytes() > 0);
    }
    assert_eq!(memory.used_bytes(),0);
}
