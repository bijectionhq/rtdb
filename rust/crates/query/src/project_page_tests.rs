use super::*;
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{BinaryOp,ColumnId,Field};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{LogicalType,ScalarType,Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryOptions,context::OuterRows,parameters::BoundParameters};

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
fn cursor(rows: Vec<i64>,expressions: Vec<Expr>,execution: &Execution) -> PageProject {
    let schema = Schema::new(vec![field(0,LogicalType::Scalar(ScalarType::I64))]).unwrap();
    PageProject { input: Box::new(Input { rows: Some(rows),memory: Arc::clone(&execution.context.memory) }),schema,
        expressions: expressions.into_iter().enumerate().map(|(index,expr)|NamedExpr { field: field(index as u32 + 1,expr.data_type()),expr }).collect(),execution: execution.clone() }
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
    let mut cursor = cursor(vec![1,0],vec![first,second],&execution);
    assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb.expression/division-by-zero");
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn empty_projection_pages_still_admit_each_expression_column() {
    let execution = execution();
    let expression = Expr::binary(BinaryOp::Eq,Expr::Literal(Value::Set(vec![])),Expr::Literal(Value::Set(vec![])));
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = cursor(vec![],vec![expression],&execution);
    assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb.expression/set-equality");
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn preceding_column_payload_failure_precedes_later_column_pattern_compilation() {
    let execution = execution();
    let first = Expr::binary(BinaryOp::Divide,int(10),Expr::column(ColumnId(0),LogicalType::Scalar(ScalarType::I64)));
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = cursor(vec![1,0],vec![first,invalid_pattern()],&execution);
    assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb.expression/division-by-zero");
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn empty_projection_page_compiles_pattern_but_absent_page_does_not() {
    let execution = execution();
    let baseline = execution.context.memory.used_bytes();
    let mut empty = cursor(vec![],vec![invalid_pattern()],&execution);
    assert_eq!(empty.next_page().await.err().unwrap().code,"xtdb.expression/illegal-argument-exception");
    drop(empty);assert_eq!(execution.context.memory.used_bytes(),baseline);
    let mut absent = cursor(vec![],vec![invalid_pattern()],&execution);
    absent.input = Box::new(Input { rows: None,memory: Arc::clone(&execution.context.memory) });
    assert!(absent.next_page().await.unwrap().is_none());
    drop(absent);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn filter_prepares_pattern_for_an_empty_child_page() {
    let execution = execution();
    let baseline = execution.context.memory.used_bytes();
    let predicate = Expr::binary(BinaryOp::Eq,invalid_pattern(),Expr::Literal(Value::Utf8("x".into())));
    let mut filter = PageFilter {
        input: Box::new(Input { rows: Some(vec![]),memory: Arc::clone(&execution.context.memory) }),
        schema: Schema::new(vec![field(0,LogicalType::Scalar(ScalarType::I64))]).unwrap(),
        stages: vec![(predicate,execution.clone())],_collapsed: execution.context.memory.reserve(0).unwrap(),execution: execution.clone(),
    };
    assert_eq!(filter.next_page().await.err().unwrap().code,"xtdb.expression/illegal-argument-exception");
    drop(filter);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn projection_publishes_complete_rows_with_retained_cell_charges() {
    let execution = execution();
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = cursor(vec![1,2],vec![Expr::column(ColumnId(0),LogicalType::Scalar(ScalarType::I64)),Expr::Literal(Value::Utf8("text".into()))],&execution);
    let mut page = cursor.next_page().await.unwrap().unwrap();
    drop(cursor);
    assert_eq!(page.len(),2);
    assert_eq!(page.next().unwrap().values,[Value::I64(1),Value::Utf8("text".into())]);
    assert_eq!(page.next().unwrap().values,[Value::I64(2),Value::Utf8("text".into())]);
    drop(page);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn cancellation_in_a_later_column_releases_the_unpublished_page() {
    let execution = execution();
    let sleep = Expr::Call { name: "sleep".into(),args: vec![Expr::Literal(Value::Duration { value: 1000, unit: rtdb_types::TimeUnit::Millisecond })],return_type: LogicalType::Null };
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = cursor(vec![1,2],vec![Expr::Literal(Value::Utf8("retained".repeat(1024))),sleep],&execution);
    let mut pending = Box::pin(cursor.next_page());
    assert!(matches!(futures::poll!(pending.as_mut()),std::task::Poll::Pending));
    execution.context.cancellation.cancel();
    assert_eq!(pending.await.err().unwrap().code,"xtdb/query-cancelled");
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[test]
fn failed_cell_growth_keeps_the_previous_row_and_reservation() {
    let memory = MemoryBudget::new(1024);
    let mut row = OwnedRow::new(vec![Value::Null],&memory).unwrap();
    let before = row.bytes();
    assert_eq!(row.replace_value(0,Value::Utf8("x".repeat(2048)),&memory).unwrap_err().code,"rtdb/memory-budget-exceeded");
    assert_eq!(row.values,[Value::Null]);assert_eq!(row.bytes(),before);assert_eq!(memory.used_bytes(),before);
    drop(row);assert_eq!(memory.used_bytes(),0);
}

fn raw_build(emit: impl FnOnce(&mut rtdb_plan::RegexFormBuilder<'_>) -> rtdb_types::Result<()>) -> rtdb_plan::RegexForm {
    let mut builder = rtdb_plan::RegexFormBuilder::new_untracked();
    emit(&mut builder).unwrap();
    builder.finish().unwrap()
}
fn raw_literal(value: rtdb_types::Value) -> rtdb_plan::RegexForm { raw_build(|builder| builder.literal(&value)) }
