use super::*;
use std::collections::{BTreeMap,VecDeque};
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{Expr,Field,Relation,RelationNode};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{ScalarType,Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryContext,QueryOptions,context::OuterRows,parameters::BoundParameters,runtime_schema::EmittedRelation};

struct Input { pages: VecDeque<Vec<Vec<Value>>>,memory: Arc<MemoryBudget> }
#[async_trait]
impl PageCursor for Input {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        let Some(rows) = self.pages.pop_front() else { return Ok(None); };
        let mut page = PageBuilder::new(rows.len(),&self.memory)?;
        for row in rows { page.push(OwnedRow::new(row,&self.memory)?)?; }
        Ok(Some(page.finish()))
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
fn runtime(execution: &Execution) -> Arc<crate::runtime_schema::EmittedRelation> {
    let relation = Relation::new(schema(),RelationNode::Values { rows: vec![] }).unwrap();
    EmittedRelation::derive(Arc::new(relation),&execution.context.catalog,execution.parameters.types(),&execution.context.memory).unwrap()
}
fn input(pages: Vec<Vec<Vec<Value>>>,execution: &Execution) -> BoxPageCursor { Box::new(Input { pages: pages.into(),memory: Arc::clone(&execution.context.memory) }) }
fn row(group: i64,value: i64) -> Vec<Value> { vec![Value::I64(group),Value::I64(value)] }
fn expressions(functions: &[WindowFunction]) -> Vec<WindowExpr> {
    functions.iter().enumerate().map(|(index,function)|WindowExpr { field: Field { id: ColumnId(index as u32 + 2),name: format!("w{index}"),qualifier: None,qualified_only: false,unnamed: false,data_type: function.result_type(&schema()).unwrap() },function: function.clone() }).collect()
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
async fn partition_mapping_preserves_semantic_pages_including_empty_pages() {
    for batch_rows in [1,1024] {
        let execution = execution(batch_rows);let baseline = execution.context.memory.used_bytes();
        let mut cursor = PartitionPages { input: input(vec![vec![row(1,3),row(2,4)],vec![],vec![row(1,5)]],&execution),positions: vec![0],runtime: runtime(&execution),keys: Vec::new(),index: Default::default(),count: 0,execution: execution.clone() };
        let mut first = cursor.next_page().await.unwrap().unwrap();assert_eq!(first.len(),2);
        assert_eq!(first.next().unwrap().values,[Value::I64(1),Value::I64(3),Value::U64(0)]);
        assert_eq!(first.next().unwrap().values,[Value::I64(2),Value::I64(4),Value::U64(1)]);
        assert_eq!(cursor.next_page().await.unwrap().unwrap().len(),0);
        assert_eq!(cursor.next_page().await.unwrap().unwrap().next().unwrap().values,[Value::I64(1),Value::I64(5),Value::U64(0)]);
        assert!(cursor.next_page().await.unwrap().is_none());
        drop(first);drop(cursor);assert_eq!(execution.context.spill_bytes(),0);assert_eq!(execution.context.memory.used_bytes(),baseline);
    }
}

#[tokio::test]
async fn window_columns_share_complete_partitioned_input_and_publish_once() {
    for batch_rows in [1,1024] {
        let execution = execution(batch_rows);let baseline = execution.context.memory.used_bytes();
        let order = vec![SortExpr { expr: schema().fields()[1].expression(),descending: false,nulls_first: false }];
        let functions = [WindowFunction::RowNumber,WindowFunction::Lead { column: ColumnId(1),offset: 1 },WindowFunction::Lag { column: ColumnId(1),offset: 1 }];
        let mut cursor = WindowCursor::new(input(vec![vec![row(1,3),row(2,4)],vec![],vec![row(1,1),row(2,2)]],&execution),runtime(&execution),vec![ColumnId(0)],order,expressions(&functions),execution.clone());
        let mut page = cursor.next_page().await.unwrap().unwrap();assert_eq!(page.len(),4);assert!(cursor.next_page().await.unwrap().is_none());
        drop(cursor);assert_eq!(execution.context.spill_bytes(),0);
        for expected in [
            vec![Value::I64(1),Value::I64(1),Value::I64(1),Value::I64(3),Value::Null],
            vec![Value::I64(1),Value::I64(3),Value::I64(2),Value::Null,Value::I64(1)],
            vec![Value::I64(2),Value::I64(2),Value::I64(1),Value::I64(4),Value::Null],
            vec![Value::I64(2),Value::I64(4),Value::I64(2),Value::Null,Value::I64(2)],
        ] { assert_eq!(page.next().unwrap().values,expected); }
        drop(page);assert_eq!(execution.context.memory.used_bytes(),baseline);
    }
}

#[tokio::test]
async fn later_window_column_overflow_discards_completed_prior_column_and_sorted_input() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let functions = [WindowFunction::RowNumber,WindowFunction::Lead { column: ColumnId(1),offset: i64::MAX as u64 }];
    let mut cursor = WindowCursor::new(input(vec![vec![row(1,1),row(1,2)]],&execution),runtime(&execution),vec![],vec![],expressions(&functions),execution.clone());
    assert_eq!(cursor.next_page().await.err().unwrap().code,"rtdb.query/window-offset");
    assert!(cursor.next_page().await.unwrap().is_none());drop(cursor);
    assert_eq!(execution.context.spill_bytes(),0);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn empty_semantic_window_input_still_prepares_its_computed_order_key() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let expr = Expr::regex_replace(Expr::Literal(Value::Utf8("a".into())),rtdb_plan::RegexReplacement { pattern: raw_literal(Value::Utf8("[".into())),replacement: raw_literal(Value::Utf8("x".into())),..Default::default() });
    let order = vec![SortExpr { expr,descending: false,nulls_first: false }];
    let mut cursor = WindowCursor::new(input(vec![vec![]],&execution),runtime(&execution),vec![],order.clone(),expressions(&[WindowFunction::RowNumber]),execution.clone());
    assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb.expression/illegal-argument-exception");drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    let mut cursor = WindowCursor::new(input(vec![],&execution),runtime(&execution),vec![],order,expressions(&[WindowFunction::RowNumber]),execution.clone());
    assert!(cursor.next_page().await.unwrap().is_none());drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn partition_hash_precedes_cross_unit_timestamp_equality() {
    use rtdb_types::TimeUnit;
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let first = Value::Timestamp { value: i64::MAX,unit: TimeUnit::Second,timezone: Some("UTC".into()) };
    let second = Value::Timestamp { value: 0,unit: TimeUnit::Millisecond,timezone: Some("UTC".into()) };
    let ty = first.logical_type().join(&second.logical_type());
    let schema = Schema::new(vec![Field { id: ColumnId(0),name: "x".into(),qualifier: None,qualified_only: false,unnamed: false,data_type: ty }]).unwrap();
    let relation = Relation::new(schema,RelationNode::Values { rows: vec![] }).unwrap();
    let runtime = EmittedRelation::derive(Arc::new(relation),&execution.context.catalog,execution.parameters.types(),&execution.context.memory).unwrap();
    let mut cursor = PartitionPages { input: input(vec![vec![vec![first],vec![second]]],&execution),positions: vec![0],runtime,keys: Vec::new(),index: Default::default(),count: 0,execution: execution.clone() };
    let mut page = cursor.next_page().await.unwrap().unwrap();
    assert_eq!(partition_id(&page.next().unwrap().values).unwrap(),0);
    assert_eq!(partition_id(&page.next().unwrap().values).unwrap(),1);
    drop(page);drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

fn raw_build(emit: impl FnOnce(&mut rtdb_plan::RegexFormBuilder<'_>) -> rtdb_types::Result<()>) -> rtdb_plan::RegexForm {
    let mut builder = rtdb_plan::RegexFormBuilder::new_untracked();
    emit(&mut builder).unwrap();
    builder.finish().unwrap()
}
fn raw_literal(value: rtdb_types::Value) -> rtdb_plan::RegexForm { raw_build(|builder| builder.literal(&value)) }
