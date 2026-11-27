use super::*;
use crate::{columnar::{BatchCursor, BatchExpressions, BatchFilter, BatchProject, ColumnBatch}, cursor::OwnedRow, pages::{OwnedPage, PageBuilder, PageCursor}};
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{BinaryOp,ColumnId,Field};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{LogicalType,ScalarType,Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryContext,QueryOptions,context::OuterRows,parameters::BoundParameters};

struct Input { rows: Option<Vec<i64>>,memory: Arc<MemoryBudget> }
#[async_trait]
impl PageCursor for Input {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        let Some(rows) = self.rows.take() else { return Ok(None); };
        let mut page = PageBuilder::new(rows.len(),&self.memory)?;
        for row in rows { page.push(OwnedRow::new(vec![Value::I64(row)],&self.memory)?)?; }
        Ok(Some(page.finish()))
    }
}
fn execution() -> Execution {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    let context = QueryContext::new(catalog,MemoryBudget::new(1 << 26),Timestamp(0),"UTC",QueryOptions { batch_rows: 1,..Default::default() },&CancellationToken::new()).unwrap();
    Execution { parameters: Arc::new(BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),context,bindings: None,analysis: None }
}
fn field(id: u32,data_type: LogicalType) -> Field { Field { id: ColumnId(id),name: format!("c{id}"),qualifier: None,qualified_only: false,unnamed: false,data_type } }
fn cursor(rows: Option<Vec<i64>>, expressions: Vec<Expr>, execution: &Execution) -> BatchExpressions {
    let schema = Schema::new(vec![field(0,LogicalType::Scalar(ScalarType::I64))]).unwrap();
    BatchExpressions::new(
        Box::new(BatchInput(Input { rows, memory: Arc::clone(&execution.context.memory) })),
        schema, expressions, execution.clone(),
    )
}

struct BatchInput(Input);
#[async_trait]
impl BatchCursor for BatchInput {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.0.next_page().await?.map(|page| ColumnBatch::from_page(&page, &self.0.memory)).transpose()
    }
}

fn int(value: i64) -> Expr { Expr::Literal(Value::I64(value)) }
fn invalid_pattern() -> Expr {
    Expr::regex_replace(Expr::Literal(Value::Utf8("value".into())),rtdb_plan::RegexReplacement {
        pattern: raw_literal(Value::Utf8("[".into())),
        replacement: raw_literal(Value::Utf8("replacement".into())),
        ..Default::default()
    })
}

#[tokio::test]
async fn projection_evaluates_every_row_of_the_first_column_before_the_second_column() {
    let execution = execution();
    let first = Expr::binary(BinaryOp::Divide,int(10),Expr::column(ColumnId(0),LogicalType::Scalar(ScalarType::I64)));
    let second = Expr::Cast { expr: Box::new(Expr::Literal(Value::Utf8("invalid integer".into()))),target: rtdb_plan::CastTarget::fixed(LogicalType::Scalar(ScalarType::I64)) };
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = cursor(Some(vec![1,0]),vec![first,second],&execution);
    assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb.expression/division-by-zero");
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn empty_projection_pages_still_admit_each_expression_column() {
    let execution = execution();
    let expression = Expr::binary(BinaryOp::Eq,Expr::Literal(Value::Set(vec![])),Expr::Literal(Value::Set(vec![])));
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = cursor(Some(vec![]),vec![expression],&execution);
    assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb.expression/set-equality");
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn preceding_column_payload_failure_precedes_later_column_pattern_compilation() {
    let execution = execution();
    let first = Expr::binary(BinaryOp::Divide,int(10),Expr::column(ColumnId(0),LogicalType::Scalar(ScalarType::I64)));
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = cursor(Some(vec![1,0]),vec![first,invalid_pattern()],&execution);
    assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb.expression/division-by-zero");
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn empty_projection_page_compiles_pattern_but_absent_page_does_not() {
    let execution = execution();
    let baseline = execution.context.memory.used_bytes();
    let mut empty = cursor(Some(vec![]),vec![invalid_pattern()],&execution);
    assert_eq!(empty.next_batch().await.err().unwrap().code,"xtdb.expression/illegal-argument-exception");
    drop(empty);assert_eq!(execution.context.memory.used_bytes(),baseline);
    let mut absent = cursor(None,vec![invalid_pattern()],&execution);
    assert!(absent.next_batch().await.unwrap().is_none());
    drop(absent);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn filter_prepares_pattern_for_an_empty_child_page() {
    let execution = execution();
    let baseline = execution.context.memory.used_bytes();
    let predicate = Expr::binary(BinaryOp::Eq,invalid_pattern(),Expr::Literal(Value::Utf8("x".into())));
    let input = Box::new(BatchInput(Input { rows: Some(vec![]), memory: Arc::clone(&execution.context.memory) }));
    let schema = Schema::new(vec![field(0, LogicalType::Scalar(ScalarType::I64))]).unwrap();
    let mut filter = BatchFilter::new(input, schema, predicate, execution.clone());
    assert_eq!(filter.next_batch().await.err().unwrap().code,"xtdb.expression/illegal-argument-exception");
    drop(filter);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn projection_publishes_complete_rows_with_retained_cell_charges() {
    let execution = execution();
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = cursor(Some(vec![1,2]),vec![Expr::column(ColumnId(0),LogicalType::Scalar(ScalarType::I64)),Expr::Literal(Value::Utf8("text".into()))],&execution);
    let page = cursor.next_batch().await.unwrap().unwrap();
    drop(cursor);
    assert_eq!(page.len,2);
    for (row, value) in [1, 2].into_iter().enumerate() {
        let values = page.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>();
        assert_eq!(values, [Value::I64(value), Value::Utf8("text".into())]);
    }
    drop(page);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn passthrough_aliases_reuse_selections_and_charge_value_copies() {
    use arrow_array::StringArray;
    use crate::columnar::{Column, SelectedColumn, single_batch};

    let execution = execution();
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let ty = LogicalType::Scalar(ScalarType::Utf8);
    let field = Arc::new(rtdb_columnar::to_field("source", &ty).unwrap());
    let payload = "s".repeat(1 << 17);
    let selected = Column::Selected(SelectedColumn::new(
        vec![Some((Arc::new(StringArray::from(vec![payload.as_str()])), Arc::clone(&field), Some(ty), None))],
        Arc::new(vec![(0, 0)]),
    ));
    let mut batch = ColumnBatch::settled(Vec::new(), 1, memory.reserve(256).unwrap()).unwrap();
    batch.append_column(selected, memory.reserve(0).unwrap(), memory).unwrap();
    let copied = "v".repeat(4096);
    batch.append_column(Column::Values(vec![Value::Utf8(copied.clone())]), memory.reserve(0).unwrap(), memory).unwrap();
    let kept = memory.used_bytes();
    batch.append_column(Column::Values(vec![Value::Utf8("unused".repeat(1 << 14))]), memory.reserve(0).unwrap(), memory).unwrap();

    let pressure = memory.reserve(memory.available_bytes() - (16 << 10)).unwrap();
    let input = single_batch(Some(batch), Arc::clone(&execution.context));
    let mut project = BatchProject::new(input, vec![1, 0, 0, 1], Arc::clone(memory));
    let page = project.next_batch().await.unwrap().unwrap();
    drop(pressure);
    assert!(memory.used_bytes() >= kept + copied.len());
    assert!(memory.used_bytes() < kept + (16 << 10));
    assert_eq!(page.columns.iter().map(|column| column.value(0).unwrap()).collect::<Vec<_>>(), [
        Value::Utf8(copied.clone()), Value::Utf8(payload.clone()), Value::Utf8(payload), Value::Utf8(copied),
    ]);

    let input = single_batch(Some(page), Arc::clone(&execution.context));
    let mut project = BatchProject::new(input, Vec::new(), Arc::clone(memory));
    let empty = project.next_batch().await.unwrap().unwrap();
    assert_eq!(empty.len, 1);
    assert!(empty.columns.is_empty());
    drop(empty);

    let zero_rows = ColumnBatch::empty_with_width(1, memory).unwrap();
    let input = single_batch(Some(zero_rows), Arc::clone(&execution.context));
    let mut project = BatchProject::new(input, vec![0, 0], Arc::clone(memory));
    let empty = project.next_batch().await.unwrap().unwrap();
    assert_eq!((empty.len, empty.columns.len()), (0, 2));
    drop(empty);
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn expression_structs_retain_each_source_charge_across_aliases() {
    use arrow_array::{StringArray, StructArray};
    use crate::columnar::{Column, single_batch};

    let execution = execution();
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let ty = LogicalType::Scalar(ScalarType::Utf8);
    let left = field(0, ty.clone());
    let right = field(1, ty.clone());
    let schema = Schema::new(vec![left.clone(), right.clone()]).unwrap();
    let mut batch = ColumnBatch::settled(Vec::new(), 1, memory.reserve(256).unwrap()).unwrap();
    for (source, text) in [(&left, "a"), (&right, "b")] {
        let column = Column::Arrow {
            array: Arc::new(StringArray::from(vec![text.repeat(1 << 17)])),
            field: Arc::new(rtdb_columnar::to_field(&source.name, &ty).unwrap()),
            logical: Some(ty.clone()),
        };
        batch.append_column(column, memory.reserve(0).unwrap(), memory).unwrap();
    }
    let record = Expr::Struct(vec![("right".into(), right.expression()), ("left".into(), left.expression()), ("again".into(), right.expression())]);
    let rows = Expr::RowCopy(vec![right.clone(), left.clone()]);
    let projections = vec![right.expression(), left.expression(), record, rows, int(9)];
    let pressure = memory.reserve(memory.available_bytes() - (16 << 10)).unwrap();
    let input = single_batch(Some(batch), Arc::clone(&execution.context));
    let mut project = BatchExpressions::new(input, schema, projections, execution.clone());
    let page = project.next_batch().await.unwrap().unwrap();
    drop(pressure);
    assert_eq!(page.working_set_bytes(memory).unwrap(), memory.used_bytes() - baseline);
    let [Column::Arrow { array: right_array, .. }, Column::Arrow { array: left_array, .. },
        Column::Arrow { array: assembled, .. }, Column::Arrow { array: copied, .. }, constant] = page.columns.as_slice()
    else { panic!("Expected shared Arrow children and a computed constant"); };
    let assembled = assembled.as_any().downcast_ref::<StructArray>().unwrap();
    let copied = copied.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(assembled.fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), ["right", "left", "again"]);
    assert_eq!(copied.fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), [right.name.as_str(), left.name.as_str()]);
    for record in [assembled, copied] {
        assert!(Arc::ptr_eq(record.column(0), right_array));
        assert!(Arc::ptr_eq(record.column(1), left_array));
    }
    assert!(Arc::ptr_eq(assembled.column(2), right_array));
    assert_eq!(constant.value(0).unwrap(), Value::I64(9));
    drop(page);
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn nullable_struct_projection_admits_validity_and_keeps_child_payload() {
    use arrow_array::{Array, StringArray, StructArray};
    use arrow_buffer::NullBuffer;
    use arrow_schema::{DataType, Field as ArrowField};
    use crate::columnar::{Column, single_batch};

    let execution = execution();
    let memory = &execution.context.memory;
    let payload = "x".repeat(1 << 17);
    let values = Arc::new(StringArray::from(vec![Some("keep"), Some(payload.as_str()), None]));
    let address = values.value_data().as_ptr();
    let struct_array = StructArray::new(
        vec![ArrowField::new("child", DataType::Utf8, true)].into(), vec![values],
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let physical = Arc::new(ArrowField::new("record", struct_array.data_type().clone(), true));
    let logical = rtdb_columnar::from_field(&physical).unwrap();
    let input_field = field(0, logical.clone());
    let schema = Schema::new(vec![input_field.clone()]).unwrap();
    let expr = Expr::Field {
        expr: Box::new(input_field.expression()), name: "child".into(), data_type: LogicalType::Scalar(ScalarType::Utf8).maybe(),
    };
    let batch = ColumnBatch::settled(vec![Column::Arrow { array: Arc::new(struct_array), field: physical, logical: Some(logical) }],
        3, memory.reserve(0).unwrap()).unwrap();
    let pressure = memory.reserve(memory.available_bytes() - (8 << 10)).unwrap();
    let input = single_batch(Some(batch), Arc::clone(&execution.context));
    let mut project = BatchExpressions::new(input, schema, vec![expr], execution.clone());
    let page = project.next_batch().await.unwrap().unwrap();
    drop(pressure);
    let Column::Arrow { array, field, logical } = &page.columns[0] else { panic!("Expected an Arrow child"); };
    assert!(field.is_nullable());
    assert_eq!(*logical, Some(LogicalType::Scalar(ScalarType::Utf8).maybe()));
    let child = array.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(child.value_data().as_ptr(), address);
    assert_eq!(child.iter().collect::<Vec<_>>(), [Some("keep"), None, None]);
}

#[tokio::test]
async fn cancellation_in_a_later_column_releases_the_unpublished_page() {
    let execution = execution();
    let sleep = Expr::Call { name: "sleep".into(),args: vec![Expr::Literal(Value::Duration { value: 1000, unit: rtdb_types::TimeUnit::Millisecond })],return_type: LogicalType::Null };
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = cursor(Some(vec![1,2]),vec![Expr::Literal(Value::Utf8("retained".repeat(1024))),sleep],&execution);
    let mut pending = Box::pin(cursor.next_batch());
    assert!(matches!(futures::poll!(pending.as_mut()),std::task::Poll::Pending));
    execution.context.cancellation.cancel();
    assert_eq!(pending.await.err().unwrap().code,"xtdb/query-cancelled");
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

fn raw_build(emit: impl FnOnce(&mut rtdb_plan::RegexFormBuilder<'_>) -> rtdb_types::Result<()>) -> rtdb_plan::RegexForm {
    let mut builder = rtdb_plan::RegexFormBuilder::new_untracked();
    emit(&mut builder).unwrap();
    builder.finish().unwrap()
}
fn raw_literal(value: rtdb_types::Value) -> rtdb_plan::RegexForm { raw_build(|builder| builder.literal(&value)) }

#[tokio::test]
async fn arrow_wave4_selected_boolean_projection_and_filter_match_scalar_evaluation() {
    use crate::columnar::{BatchCursor, BatchExpressions, Column, ColumnBatch, SelectedColumn};
    let execution = execution();
    let context = &execution.context;
    let ty = LogicalType::Scalar(ScalarType::F32).maybe();
    let field = field(0, ty.clone());
    let schema = Schema::new(vec![field.clone()]).unwrap();
    let predicate = Expr::binary(BinaryOp::Lt, field.expression(), Expr::Literal(Value::F32(0.0)));
    let array = Arc::new(arrow_array::Float32Array::from(vec![Some(-0.0), None, Some(1.0)]));
    let nullable = Arc::new(rtdb_columnar::to_field(&field.name, &ty).unwrap());
    let column = Column::Selected(SelectedColumn::new(
        vec![Some((array, nullable, Some(ty), None)), None],
        Arc::new(vec![(1, 0), (0, 2), (0, 0), (0, 1)]),
    ));
    let batch = ColumnBatch::settled(vec![column], 4, context.memory.reserve(4096).unwrap()).unwrap();
    let scope = execution.scope().unwrap();
    let mut expected = Vec::new();
    for row in 0..batch.len {
        expected.push(execution.evaluate(&scope, &predicate, &schema, &[batch.columns[0].value(row).unwrap()]).await.unwrap());
        scope.reset().unwrap();
    }
    let mask = crate::columnar::typed_predicate_mask(&batch, &predicate, &schema, execution.parameters.evaluator()).unwrap().unwrap();
    assert_eq!(mask, expected.iter().map(|value| *value == Value::Bool(true)).collect::<Vec<_>>());
    let input = crate::columnar::single_batch(Some(batch), context.clone());
    let mut projections = BatchExpressions::new(input, schema, vec![predicate], execution.clone());
    let output = projections.next_batch().await.unwrap().unwrap();
    assert!(matches!(&output.columns[0], Column::Arrow { array, .. } if array.data_type() == &arrow_schema::DataType::Boolean));
    assert_eq!((0..output.len).map(|row| output.columns[0].value(row).unwrap()).collect::<Vec<_>>(), expected);
}

#[tokio::test]
async fn arrow_wave4_selected_checked_arithmetic_falls_back_at_the_first_failing_row() {
    use crate::columnar::{BatchCursor, BatchExpressions, Column, ColumnBatch, SelectedColumn};
    let execution = execution();
    let context = &execution.context;
    let ty = LogicalType::Scalar(ScalarType::I64).maybe();
    let field = field(0, ty.clone());
    let schema = Schema::new(vec![field.clone()]).unwrap();
    let expression = Expr::binary(BinaryOp::Add, field.expression(), Expr::Literal(Value::I64(2)));
    let array = Arc::new(arrow_array::Int64Array::from(vec![Some(40), None, Some(i64::MAX)]));
    let nullable = Arc::new(rtdb_columnar::to_field(&field.name, &ty).unwrap());
    let batch = |rows: Vec<(usize, usize)>| {
        let len = rows.len();
        let column = Column::Selected(SelectedColumn::new(
            vec![Some((array.clone(), nullable.clone(), Some(ty.clone()), None)), None],
            Arc::new(rows),
        ));
        ColumnBatch::settled(vec![column], len, context.memory.reserve(4096).unwrap()).unwrap()
    };
    let selected = batch(vec![(1, 0), (0, 0), (0, 1)]);
    let result = crate::columnar::functions::column(&expression, &schema, &selected, execution.parameters.evaluator(), &context.memory).unwrap().unwrap();
    let expected = [Value::Null, Value::I64(42), Value::Null];
    assert_eq!((0..selected.len).map(|row| result.value(row).unwrap()).collect::<Vec<_>>(), expected);
    let selected = batch(vec![(0, 1), (0, 2), (0, 0)]);
    let input = crate::columnar::single_batch(Some(selected), context.clone());
    let mut projection = BatchExpressions::new(input, schema.clone(), vec![expression.clone(), invalid_pattern()], execution.clone());
    let scope = execution.scope().unwrap();
    let expected = execution.evaluate(&scope, &expression, &schema, &[Value::I64(i64::MAX)]).await.unwrap_err();
    let error = projection.next_batch().await.err().unwrap();
    assert_eq!((error.code, error.message), (expected.code, expected.message));
}
