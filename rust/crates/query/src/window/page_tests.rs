use super::*;
use crate::{cursor::OwnedRow, pages::PageBuilder};
use std::collections::{BTreeMap,VecDeque};
use rtdb_columnar::MemoryBudget;
use rtdb_plan::Field;
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryContext,QueryOptions,context::OuterRows,parameters::BoundParameters};

struct Input { pages: VecDeque<Vec<Vec<Value>>>,memory: Arc<MemoryBudget> }
#[async_trait]
impl BatchCursor for Input {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        let Some(rows) = self.pages.pop_front() else { return Ok(None); };
        if rows.is_empty() { return ColumnBatch::empty_with_width(2, &self.memory).map(Some); }
        let mut page = PageBuilder::new(rows.len(),&self.memory)?;
        for row in rows { page.push(OwnedRow::new(row,&self.memory)?)?; }
        ColumnBatch::from_page(&page.finish(), &self.memory).map(Some)
    }
}
fn execution(batch_rows: usize) -> Execution {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    let context = QueryContext::new(catalog,MemoryBudget::new(1 << 26),Timestamp(0),"UTC",QueryOptions { batch_rows,..Default::default() },&CancellationToken::new()).unwrap();
    Execution { parameters: Arc::new(BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),context,bindings: None,analysis: None }
}
fn schema() -> Schema {
    Schema::new((0..2).map(|id|Field { id: ColumnId(id),name: format!("c{id}"),qualifier: None,qualified_only: false,unnamed: false,data_type: LogicalType::Scalar(ScalarType::I64) }).collect()).unwrap()
}
fn input(pages: Vec<Vec<Vec<Value>>>,execution: &Execution) -> BoxBatchCursor { Box::new(Input { pages: pages.into(),memory: Arc::clone(&execution.context.memory) }) }
fn row(group: i64,value: i64) -> Vec<Value> { vec![Value::I64(group),Value::I64(value)] }
fn expressions(functions: &[WindowFunction]) -> Vec<WindowExpr> {
    functions.iter().enumerate().map(|(index,function)|WindowExpr { field: Field { id: ColumnId(index as u32 + 2),name: format!("w{index}"),qualifier: None,qualified_only: false,unnamed: false,data_type: function.result_type(&schema()).unwrap() },function: function.clone() }).collect()
}

#[tokio::test]
async fn window_shift_keeps_identity_and_cross_page_spans_after_cursor_drop() {
    let execution = execution(4);
    let baseline = execution.context.memory.used_bytes();
    let functions = [
        WindowFunction::Lag { column: ColumnId(1), offset: 0 },
        WindowFunction::Lead { column: ColumnId(1), offset: 2 },
        WindowFunction::Lag { column: ColumnId(1), offset: 2 },
    ];
    let expressions = expressions(&functions);
    assert!(expressions.iter().all(|expr| expr.field.data_type.nullable()));
    let pages = vec![
        vec![row(1, 10), row(1, 20)],
        vec![row(1, 30), row(1, 40), row(1, 50), row(1, 60)],
        vec![row(1, 70), row(1, 80)],
    ];
    let mut cursor = BatchWindow::new(input(pages, &execution), schema(), &[], vec![], &expressions, false, execution.clone()).unwrap();
    let first = cursor.next_batch().await.unwrap().unwrap();
    let middle = cursor.next_batch().await.unwrap().unwrap();
    let last = cursor.next_batch().await.unwrap().unwrap();
    let (source, ..) = middle.columns[1].arrow_cell(0).unwrap().unwrap();
    for row in 0..2 {
        let (original, ..) = first.columns[1].arrow_cell(row).unwrap().unwrap();
        let (identity, _, _, offset) = first.columns[2].arrow_cell(row).unwrap().unwrap();
        assert!(std::ptr::eq(identity, original));
        assert_eq!(offset, row);
        let (lead, _, _, offset) = first.columns[3].arrow_cell(row).unwrap().unwrap();
        assert!(std::ptr::eq(lead, source));
        assert_eq!(offset, row);
        let (lag, _, _, offset) = last.columns[4].arrow_cell(row).unwrap().unwrap();
        assert!(std::ptr::eq(lag, source));
        assert_eq!(offset, row + 2);
    }
    let values = |batch: &ColumnBatch, position: usize| {
        (0..batch.len).map(|row| batch.columns[position].value(row).unwrap()).collect::<Vec<_>>()
    };
    assert_eq!(values(&middle, 3), [Value::I64(50), Value::I64(60), Value::I64(70), Value::I64(80)]);
    assert_eq!(values(&first, 4), [Value::Null, Value::Null]);
    assert_eq!(values(&last, 3), [Value::Null, Value::Null]);
    drop((cursor, middle));
    assert_eq!(values(&first, 3), [Value::I64(30), Value::I64(40)]);
    assert_eq!(values(&last, 4), [Value::I64(50), Value::I64(60)]);
    drop((first, last));
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn window_shifts_keep_compact_runs_and_unpolled_source_pages_alive() {
    let execution = execution(1024);
    let baseline = execution.context.memory.used_bytes();
    let functions = [
        WindowFunction::Lead { column: ColumnId(1), offset: 256 },
        WindowFunction::Lag { column: ColumnId(1), offset: 65 },
    ];
    let pages = [0..2, 2..258, 258..322, 322..386, 386..450].into_iter()
        .map(|rows| rows.map(|value| row(1, value)).collect()).collect();
    let mut cursor = BatchWindow::new(
        input(pages, &execution), schema(), &[], vec![], &expressions(&functions), false, execution.clone(),
    ).unwrap();
    let first = cursor.next_batch().await.unwrap().unwrap();
    let shifted = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!((first.len, shifted.len), (2, 256));
    let lag_source = Arc::downgrade(first.column_reservation(1));
    for position in [2, 3] {
        assert!(shifted.column_reservation(position).bytes() < 2048);
    }
    drop((first, cursor));
    assert!(lag_source.upgrade().is_some());
    let lead = &shifted.columns[2];
    let lag = &shifted.columns[3];
    for index in 0..shifted.len {
        assert_eq!(lead.value(index).unwrap(), if index < 192 { Value::I64(258 + index as i64) } else { Value::Null });
        assert_eq!(lag.value(index).unwrap(), if index < 63 { Value::Null } else { Value::I64(index as i64 - 63) });
    }
    drop(shifted);
    assert!(lag_source.upgrade().is_none());
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn window_shifts_keep_both_partition_edges_within_one_output_page() {
    let execution = execution(3);
    let baseline = execution.context.memory.used_bytes();
    let functions = [
        WindowFunction::Lead { column: ColumnId(1), offset: 1 },
        WindowFunction::Lag { column: ColumnId(1), offset: 1 },
    ];
    let pages = [0..3, 3..6, 6..9, 9..10].into_iter()
        .map(|rows| rows.map(|value| row(value / 5, value)).collect()).collect();
    let mut cursor = BatchWindow::new(
        input(pages, &execution), schema(), &[ColumnId(0)], vec![], &expressions(&functions), false, execution.clone(),
    ).unwrap();
    let first = cursor.next_batch().await.unwrap().unwrap();
    let boundary = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!((first.len, boundary.len), (3, 3));
    let preceding = Arc::downgrade(first.column_reservation(1));
    drop((first, cursor));
    assert!(preceding.upgrade().is_some());
    let values = |position: usize| (0..boundary.len).map(|row| boundary.columns[position].value(row).unwrap()).collect::<Vec<_>>();
    assert_eq!(values(1), [Value::I64(3), Value::I64(4), Value::I64(5)]);
    assert_eq!(values(2), [Value::I64(4), Value::Null, Value::I64(6)]);
    assert_eq!(values(3), [Value::I64(2), Value::I64(3), Value::Null]);
    drop(boundary);
    assert!(preceding.upgrade().is_none());
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn window_shift_gathers_mixed_source_layouts_in_order() {
    let execution = execution(4);
    let functions = [WindowFunction::Lead { column: ColumnId(1), offset: 2 }];
    let mut fields = schema().fields().to_vec();
    fields[1].data_type = fields[1].data_type.join(&Value::Bool(true).logical_type());
    let schema = Schema::new(fields).unwrap();
    let mut shifts = expressions(&functions);
    shifts[0].field.data_type = functions[0].result_type(&schema).unwrap();
    let pages = vec![vec![row(1, 0), row(1, 1)], vec![row(1, 2)], vec![vec![Value::I64(1), Value::Bool(true)]]];
    let mut cursor = BatchWindow::new(input(pages, &execution), schema, &[], vec![], &shifts, false, execution.clone()).unwrap();
    let first = cursor.next_batch().await.unwrap().unwrap();
    drop(cursor);
    assert_eq!(first.len, 2);
    assert_eq!(first.columns[2].value(0).unwrap(), Value::I64(2));
    assert_eq!(first.columns[2].value(1).unwrap(), Value::Bool(true));
}

#[tokio::test]
async fn columnar_window_validates_unprojected_bottom_cells_before_emitting_ordinals() {
    use arrow_array::NullArray;
    use crate::columnar::Column;
    struct Batches(Option<ColumnBatch>);
    #[async_trait]
    impl BatchCursor for Batches {
        async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { Ok(self.0.take()) }
    }

    let execution = execution(1);
    let baseline = execution.context.memory.used_bytes();
    let logical = LogicalType::Nothing;
    let field = rtdb_columnar::to_field("bottom", &logical).unwrap();
    let column = Column::Arrow { array: Arc::new(NullArray::new(1)), field: Arc::new(field), logical: Some(logical) };
    let batch = ColumnBatch::settled(vec![column], 1, execution.context.memory.reserve(1024).unwrap()).unwrap();
    let mut cursor = BatchWindow::new(Box::new(Batches(Some(batch))), Schema::default(), &[], vec![],
        &expressions(&[WindowFunction::RowNumber]), false, execution.clone()).unwrap();
    assert_eq!(cursor.next_batch().await.err().unwrap().code, "xtdb/zero-leg-read");
    assert!(cursor.next_batch().await.unwrap().is_none());
    drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn window_columns_share_complete_partitioned_input_and_publish_once() {
    for batch_rows in [1,1024] {
        let execution = execution(batch_rows);let baseline = execution.context.memory.used_bytes();
        let order = vec![SortExpr { expr: schema().fields()[1].expression(),descending: false,nulls_first: false }];
        let functions = [WindowFunction::RowNumber,WindowFunction::Lead { column: ColumnId(1),offset: 1 },WindowFunction::Lag { column: ColumnId(1),offset: 1 }];
        let mut cursor = BatchWindow::new(input(vec![vec![row(1,3),row(2,4)],vec![],vec![row(1,1),row(2,2)]],&execution),schema(), &[ColumnId(0)], order, &expressions(&functions), false, execution.clone()).unwrap();
        let mut actual = Vec::new();
        while let Some(batch) = cursor.next_batch().await.unwrap() {
            actual.extend((0..batch.len).map(|row| batch.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>()));
        }
        drop(cursor);assert_eq!(execution.context.spill_bytes(),0);
        assert_eq!(actual, [
            vec![Value::I64(1),Value::I64(1),Value::I64(1),Value::I64(3),Value::Null],
            vec![Value::I64(1),Value::I64(3),Value::I64(2),Value::Null,Value::I64(1)],
            vec![Value::I64(2),Value::I64(2),Value::I64(1),Value::I64(4),Value::Null],
            vec![Value::I64(2),Value::I64(4),Value::I64(2),Value::Null,Value::I64(2)],
        ]);
        assert_eq!(execution.context.memory.used_bytes(),baseline);
    }
}

#[tokio::test]
async fn later_window_column_overflow_discards_completed_prior_column_and_sorted_input() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let functions = [WindowFunction::RowNumber,WindowFunction::Lead { column: ColumnId(1),offset: i64::MAX as u64 }];
    let mut cursor = BatchWindow::new(input(vec![vec![row(1,1)],vec![row(1,2)]],&execution),schema(), &[], vec![], &expressions(&functions), false, execution.clone()).unwrap();
    assert_eq!(cursor.next_batch().await.err().unwrap().code,"rtdb.query/window-offset");
    assert!(cursor.next_batch().await.unwrap().is_none());drop(cursor);
    assert_eq!(execution.context.spill_bytes(),0);assert_eq!(execution.context.memory.used_bytes(),baseline);
}


#[tokio::test]
async fn partition_hash_precedes_cross_unit_timestamp_equality() {
    use rtdb_types::TimeUnit;

    let execution = execution(1);
    let first = Value::Timestamp { value: i64::MAX, unit: TimeUnit::Second, timezone: Some("UTC".into()) };
    let second = Value::Timestamp { value: 0, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) };
    let mut key = schema().fields()[0].clone();
    key.data_type = first.logical_type().join(&second.logical_type());
    let schema = Schema::new(vec![key]).unwrap();
    let input = input(vec![vec![vec![first], vec![second]]], &execution);
    let mut window = BatchWindow::new(input, schema, &[ColumnId(0)], vec![],
        &expressions(&[WindowFunction::RowNumber]), false, execution.clone()).unwrap();
    let mut ranks = Vec::new();
    while let Some(batch) = window.next_batch().await.unwrap() {
        ranks.extend((0..batch.len).map(|row| batch.columns[1].value(row).unwrap()));
    }
    assert_eq!(ranks, [Value::I64(1), Value::I64(1)]);
}

#[tokio::test]
async fn window_assigns_nullable_composite_partitions_in_first_seen_order() {
    let execution = execution(2);
    let baseline = execution.context.memory.used_bytes();
    let pages = vec![
        vec![vec![Value::I64(7), Value::Null], row(8, 4)],
        vec![row(7, 4), vec![Value::I64(7), Value::Null], row(8, 4)],
    ];
    let mut fields = schema().fields().to_vec();
    fields[1].data_type = fields[1].data_type.clone().maybe();
    let schema = Schema::new(fields).unwrap();
    let mut cursor = BatchWindow::new(
        input(pages, &execution), schema, &[ColumnId(1), ColumnId(0)], vec![],
        &expressions(&[WindowFunction::RowNumber]), false, execution.clone(),
    ).unwrap();
    let mut actual = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        actual.extend((0..batch.len).map(|row| batch.columns.iter()
            .map(|column| column.value(row).unwrap()).collect::<Vec<_>>()));
    }
    assert_eq!(actual, [
        vec![Value::I64(7), Value::Null, Value::I64(1)],
        vec![Value::I64(7), Value::Null, Value::I64(2)],
        vec![Value::I64(8), Value::I64(4), Value::I64(1)],
        vec![Value::I64(8), Value::I64(4), Value::I64(2)],
        vec![Value::I64(7), Value::I64(4), Value::I64(1)],
    ]);
    drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn incompatible_physical_window_order_key_replays_the_prefix_and_tail() {
    use crate::runtime_schema::EmittedRelation;
    use rtdb_plan::{Expr, Relation, RelationNode};

    let execution = execution(1);
    let baseline = execution.context.memory.used_bytes();
    let schema = schema();
    let [partition, value] = schema.fields() else { unreachable!() };
    let sort_key = |expr| SortExpr { expr, descending: false, nulls_first: false };
    let partition_by = [partition.id];
    let order_by = vec![sort_key(value.expression())];
    let source = Arc::new(Relation::new(schema.clone(), RelationNode::Values {
        rows: vec![vec![Expr::Literal(Value::I64(1)), Expr::Literal(Value::I64(10))]],
    }).unwrap());
    let plan = Arc::new(Relation::new(schema.clone(), RelationNode::Sort {
        input: source, order_by: vec![sort_key(partition.expression()), order_by[0].clone()],
    }).unwrap());
    let runtime = EmittedRelation::derive(plan, &execution.context.catalog, execution.parameters.types(), &execution.context.memory).unwrap();
    let proof = InputOrder::bind(&runtime, &partition_by, &order_by, &execution.context.memory).unwrap();
    let pages = vec![vec![row(1, 10)], vec![vec![Value::I64(1), Value::I32(2)]], vec![row(2, 3)]];
    let mut cursor = BatchWindow::with_input_order(
        input(pages, &execution), schema, &partition_by, order_by, &expressions(&[WindowFunction::RowNumber]),
        false, execution.clone(), Some(proof),
    ).unwrap();
    let mut result = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        result.extend((0..batch.len).map(|row| batch.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>()));
    }
    assert_eq!(result, [
        vec![Value::I64(1), Value::I32(2), Value::I64(1)],
        vec![Value::I64(1), Value::I64(10), Value::I64(2)],
        vec![Value::I64(2), Value::I64(3), Value::I64(1)],
    ]);
    drop((cursor, runtime));
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}
