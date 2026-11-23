use super::*;
use crate::{cursor::OwnedRow, pages::PageBuilder};
use std::{collections::{BTreeMap,VecDeque},sync::{Arc,atomic::{AtomicBool,AtomicUsize,Ordering}}};
use rtdb_columnar::MemoryBudget;
use rtdb_plan::Field;
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{LogicalType, ScalarType, TimeUnit};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryContext,QueryOptions,context::OuterRows};

fn timestamp(value: Timestamp, upper: bool) -> Value {
    if upper && value == Timestamp::MAX { Value::Null }
    else { Value::Timestamp { value: value.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }
}

type InputRow = (u8,i64,i64);
struct Input { pages: VecDeque<Vec<InputRow>>, memory: Arc<MemoryBudget>, polls: Arc<AtomicUsize>, dropped: Arc<AtomicBool>, extra: bool }
#[async_trait]
impl BatchCursor for Input {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.polls.fetch_add(1,Ordering::SeqCst);
        let Some(rows) = self.pages.pop_front() else { return Ok(None); };
        if rows.is_empty() { return ColumnBatch::empty_with_width(4 + usize::from(self.extra), &self.memory).map(Some); }
        let mut page = PageBuilder::new(rows.len(),&self.memory)?;
        for (iid,from,to) in rows {
            let mut values = vec![Value::FixedSizeBinary(vec![iid;16]),timestamp(Timestamp(from),false),timestamp(Timestamp(to),true),Value::I64(7)];
            if self.extra { values.push(Value::Utf8("unwritten".into())); }
            page.push(OwnedRow::new(values,&self.memory)?)?;
        }
        ColumnBatch::from_page(&page.finish(), &self.memory).map(Some)
    }
}
impl Drop for Input { fn drop(&mut self) { self.dropped.store(true,Ordering::SeqCst); } }

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    QueryContext::new(catalog,MemoryBudget::new(1 << 24),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap()
}
async fn cursor(context: &Arc<QueryContext>, pages: Vec<Vec<InputRow>>, from: i64,to: i64) -> (BatchPatchGaps,Arc<AtomicUsize>,Arc<AtomicBool>) {
    cursor_with_extra(context,pages,from,to,false).await
}
async fn cursor_with_extra(context: &Arc<QueryContext>, pages: Vec<Vec<InputRow>>, from: i64,to: i64, extra: bool) -> (BatchPatchGaps,Arc<AtomicUsize>,Arc<AtomicBool>) {
    let polls = Arc::new(AtomicUsize::new(0));let dropped = Arc::new(AtomicBool::new(false));
    let input = Box::new(Input { pages: pages.into(),memory: Arc::clone(&context.memory),polls: Arc::clone(&polls),dropped: Arc::clone(&dropped),extra });
    let mut fields: Vec<_> = [LogicalType::Scalar(ScalarType::FixedSizeBinary(16)),timestamp(Timestamp(0),false).logical_type(),timestamp(Timestamp(0),false).logical_type().maybe(),LogicalType::Scalar(ScalarType::I64)].into_iter().enumerate().map(|(index,data_type)| Field { id: ColumnId(index as u32),name: format!("c{index}"),qualifier: None,qualified_only: false,unnamed: false,data_type }).collect();
    if extra { fields.push(Field { id: ColumnId(4),name: "extra".into(),qualifier: None,qualified_only: false,unnamed: false,data_type: LogicalType::Scalar(ScalarType::Utf8) }); }
    let schema = Schema::new(fields).unwrap();
    let execution = Execution { context: Arc::clone(context),parameters: Arc::new(crate::parameters::BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),bindings: None,analysis: None };
    (BatchPatchGaps::new(input,&schema,[ColumnId(0),ColumnId(1),ColumnId(2),ColumnId(3)],&Expr::Literal(timestamp(Timestamp(from),false)),&Expr::Literal(timestamp(Timestamp(to),false)),execution).unwrap(),polls,dropped)
}

#[tokio::test]
async fn only_the_four_selected_columns_are_written_on_copied_rows() {
    let context = context();
    let (mut cursor,_,_) = cursor_with_extra(&context,vec![vec![(1,10,20)]],0,30,true).await;
    let page = cursor.next_batch().await.unwrap().unwrap();
    let mut documents = Vec::new();
    for row in 0..page.len { assert_eq!(page.columns[4].value(row).unwrap(),Value::Null);documents.push(page.columns[3].value(row).unwrap()); }
    assert_eq!(documents,[Value::Null,Value::I64(7),Value::Null]);
    drop(page);drop(cursor);assert_eq!(context.memory.used_bytes(),0);
}
fn intervals(page: ColumnBatch) -> Vec<(i64,i64,bool)> {
    let mut rows = Vec::new();
    for row in 0..page.len {
        rows.push((rtdb_expr::timestamp_micros(&page.columns[1].value(row).unwrap(),"UTC").unwrap().0,match &page.columns[2].value(row).unwrap() { Value::Null => i64::MAX,value => rtdb_expr::timestamp_micros(value,"UTC").unwrap().0 },matches!(page.columns[3].value(row).unwrap(),Value::Null)));
    }
    rows
}

#[tokio::test]
async fn page_boundaries_reset_iid_progress_and_empty_pages_remain_visible() {
    let context = context();
    let (mut cursor,polls,dropped) = cursor(&context,vec![vec![],vec![(1,10,20)],vec![(1,30,40)]],0,50).await;
    assert!(intervals(cursor.next_batch().await.unwrap().unwrap()).is_empty());
    assert_eq!(intervals(cursor.next_batch().await.unwrap().unwrap()),[(0,10,true),(10,20,false),(20,50,true)]);
    assert_eq!(intervals(cursor.next_batch().await.unwrap().unwrap()),[(0,30,true),(30,40,false),(40,50,true)]);
    assert!(cursor.next_batch().await.unwrap().is_none());
    assert_eq!(polls.load(Ordering::SeqCst),4);
    drop(cursor);assert!(dropped.load(Ordering::SeqCst));assert_eq!(context.memory.used_bytes(),0);
}

#[tokio::test]
async fn clipped_rows_and_raw_gap_progress_preserve_empty_and_inverted_intervals() {
    let context = context();
    for (bounds,input,expected) in [
        ((5,5),vec![(1,0,10)],vec![(5,5,false)]),
        ((2,3),vec![(1,0,1)],vec![(2,1,false),(1,3,true)]),
        ((0,10),vec![(1,20,30),(1,40,50)],vec![(0,10,true),(20,10,false),(30,10,true),(40,10,false)]),
        ((0,10),vec![(2,5,8),(1,2,4)],vec![(0,5,true),(5,8,false),(8,10,true),(0,2,true),(2,4,false),(4,10,true)]),
        ((0,10),vec![(1,5,3)],vec![(0,5,true),(5,3,false),(3,10,true)]),
    ] {
        let (mut cursor,_,_) = cursor(&context,vec![input],bounds.0,bounds.1).await;
        assert_eq!(intervals(cursor.next_batch().await.unwrap().unwrap()),expected);
        drop(cursor);assert_eq!(context.memory.used_bytes(),0);
    }
}

#[tokio::test]
async fn returned_page_outlives_cursor_and_cancellation_does_not_pull_another_page() {
    let context = context();
    let (mut cursor,polls,dropped) = cursor(&context,vec![vec![(1,10,20)],vec![(1,30,40)]],0,50).await;
    let page = cursor.next_batch().await.unwrap().unwrap();
    context.cancellation.cancel();
    assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb/query-cancelled");
    assert_eq!(polls.load(Ordering::SeqCst),1);
    drop(cursor);assert!(dropped.load(Ordering::SeqCst));assert!(context.memory.used_bytes() > 0);
    drop(page);assert_eq!(context.memory.used_bytes(),0);
}
