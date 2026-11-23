use super::*;
use std::{collections::VecDeque,sync::atomic::{AtomicUsize,Ordering}};
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{ColumnId,Field};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{LogicalType,ScalarType,Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryOptions,context::OuterRows,parameters::BoundParameters,runtime_schema::{RuntimeSchema, EmittedRelation}};

struct Input { pages: VecDeque<Vec<i64>>,polls: Arc<AtomicUsize>,failure: Option<usize>,memory: Arc<MemoryBudget> }
#[async_trait]
impl PageCursor for Input {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        let poll = self.polls.fetch_add(1,Ordering::SeqCst);
        if self.failure == Some(poll) { return Err(rtdb_types::Anomaly::fault("test/page","Source page failed")); }
        let Some(rows) = self.pages.pop_front() else { return Ok(None); };
        let mut page = PageBuilder::new(rows.len(),&self.memory)?;
        for value in rows { page.push(OwnedRow::new(vec![Value::I64(value)],&self.memory)?)?; }
        Ok(Some(page.finish()))
    }
}
fn execution(batch_rows: usize) -> Execution {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    let context = QueryContext::new(catalog,MemoryBudget::new(1 << 26),Timestamp(0),"UTC",QueryOptions { batch_rows,..Default::default() },&CancellationToken::new()).unwrap();
    Execution { parameters: Arc::new(BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),context,bindings: None,analysis: None }
}
fn input(pages: Vec<Vec<i64>>,execution: &Execution) -> (BoxPageCursor,Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    (Box::new(Input { pages: pages.into(),polls: Arc::clone(&polls),failure: None,memory: Arc::clone(&execution.context.memory) }),polls)
}
fn values(mut page: OwnedPage) -> Vec<i64> {
    let mut output = Vec::new();
    while let Some(row) = page.next() { let Value::I64(value) = row.values[0] else { panic!() };output.push(value); }
    output
}
fn schema() -> Schema { Schema::new(vec![Field { id: ColumnId(0),name: "n".into(),qualifier: None,qualified_only: false,unnamed: false,data_type: LogicalType::Scalar(ScalarType::I64) }]).unwrap() }
fn table(rows: &[i64]) -> Arc<Relation> {
    Arc::new(Relation::new(schema(),RelationNode::Values { rows: rows.iter().map(|value|vec![Expr::Literal(Value::I64(*value))]).collect() }).unwrap())
}

#[tokio::test]
async fn top_slices_source_pages_and_does_not_pull_after_its_limit() {
    for batch_rows in [1,128] {
        let execution = execution(batch_rows);
        let (input,polls) = input(vec![vec![],vec![0,1,2],vec![3,4,5],vec![6]],&execution);
        let baseline = execution.context.memory.used_bytes();
        let mut cursor = Limit { input: Some(input),offset: 1,remaining: Some(4),execution: execution.clone() };
        assert_eq!(values(cursor.next_page().await.unwrap().unwrap()),[1,2]);
        assert_eq!(values(cursor.next_page().await.unwrap().unwrap()),[3,4]);
        assert!(cursor.next_page().await.unwrap().is_none());
        assert_eq!(polls.load(Ordering::SeqCst),3);
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);
    }
}

#[tokio::test]
async fn zero_limit_without_offset_does_not_advance_its_input() {
    let execution = execution(1);
    let (input,polls) = input(vec![vec![1]],&execution);
    let mut cursor = Limit { input: Some(input),offset: 0,remaining: Some(0),execution };
    assert!(cursor.next_page().await.unwrap().is_none());
    assert_eq!(polls.load(Ordering::SeqCst),0);
}

#[tokio::test]
async fn zero_limit_with_positive_offset_consumes_complete_pages_until_the_offset() {
    let execution = execution(1);
    let (input,polls) = input(vec![vec![0,1,2,3],vec![4,5,6,7],vec![8,9,10,11],vec![12]],&execution);
    let mut cursor = Limit { input: Some(input),offset: 10,remaining: Some(0),execution };
    assert!(cursor.next_page().await.unwrap().is_none());
    assert_eq!(polls.load(Ordering::SeqCst),3);
}

#[tokio::test]
async fn zero_limit_with_positive_offset_preserves_input_errors_before_the_offset() {
    let execution = execution(1);
    let polls = Arc::new(AtomicUsize::new(0));
    let input = Box::new(Input { pages: vec![vec![0,1,2]].into(),polls: Arc::clone(&polls),failure: Some(1),memory: Arc::clone(&execution.context.memory) });
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = Limit { input: Some(input),offset: 10,remaining: Some(0),execution: execution.clone() };
    assert_eq!(cursor.next_page().await.err().unwrap().code,"test/page");
    assert_eq!(polls.load(Ordering::SeqCst),2);
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn union_and_distinct_keep_complete_nonempty_source_selections() {
    let execution = execution(1);
    let (left,_) = input(vec![vec![],vec![1,1,2],vec![2]],&execution);
    let (right,_) = input(vec![vec![],vec![2,3,4],vec![4]],&execution);
    let runtime = RuntimeSchema::fixed(&schema(),&execution.context.memory).unwrap();
    let baseline = execution.context.memory.used_bytes();
    let input = Box::new(Union { left: Some(left),right });
    let mut cursor = Distinct { input,seen: SeenRows::new(&execution.context,Arc::clone(&runtime)).unwrap(),execution: execution.clone() };
    assert_eq!(values(cursor.next_page().await.unwrap().unwrap()),[1,2]);
    assert_eq!(values(cursor.next_page().await.unwrap().unwrap()),[3,4]);
    assert!(cursor.next_page().await.unwrap().is_none());
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn intersection_and_difference_preserve_left_pages_and_multiplicities() {
    let execution = execution(1);
    for (operator,all,expected) in [
        (SetOperator::Intersect,true,vec![vec![1,2]]),
        (SetOperator::Except,true,vec![vec![2,3],vec![2,4]]),
        (SetOperator::Intersect,false,vec![vec![1,2]]),
        (SetOperator::Except,false,vec![vec![3],vec![4]]),
    ] {
        let relation = Arc::new(Relation::new(schema(),RelationNode::Set { left: Arc::new(Relation::empty(schema())),right: Arc::new(Relation::empty(schema())),operator,all }).unwrap());
        let runtime = EmittedRelation::derive(Arc::clone(&relation),&execution.context.catalog,execution.parameters.types(),&execution.context.memory).unwrap();
        let (left,_) = input(vec![vec![1,2,2,3],vec![2,4]],&execution);
        let (right,_) = input(vec![vec![],vec![1,2]],&execution);
        let baseline = execution.context.memory.used_bytes();
        let mut cursor = SetCursor::new(left,right,operator,all,Arc::clone(&runtime),execution.clone()).unwrap();
        let mut observed = Vec::new();
        while let Some(page) = cursor.next_page().await.unwrap() { observed.push(values(page)); }
        assert_eq!(observed,expected);
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn retained_selection_outlives_cursor_and_cancellation_prevents_the_next_page() {
    let execution = execution(1);
    let (input,_) = input(vec![vec![1,2],vec![3]],&execution);
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = Limit { input: Some(input),offset: 0,remaining: None,execution: execution.clone() };
    let page = cursor.next_page().await.unwrap().unwrap();
    execution.context.cancellation.cancel();
    assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb/query-cancelled");
    drop(cursor);assert!(execution.context.memory.used_bytes() > baseline);
    assert_eq!(values(page),[1,2]);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn production_construction_keeps_set_and_top_pages_independent_of_client_batches() {
    for batch_rows in [1,1024] {
        let execution = execution(batch_rows);
        let set = Arc::new(Relation::new(schema(),RelationNode::Set { left: table(&[1,1,2]),right: table(&[2,3,4]),operator: SetOperator::Union,all: false }).unwrap());
        let relation = Arc::new(Relation::new(schema(),RelationNode::Limit { input: set,offset: Some(Expr::Literal(Value::I64(1))),fetch: Some(Expr::Literal(Value::I64(2))) }).unwrap());
        let runtime = EmittedRelation::derive(Arc::clone(&relation),&execution.context.catalog,execution.parameters.types(),&execution.context.memory).unwrap();
        let baseline = execution.context.memory.used_bytes();
        let mut cursor = build_pages_constrained(Arc::clone(&runtime),execution.clone(),crate::inspection::Constraints::default(),None).await.unwrap();
        assert_eq!(values(cursor.next_page().await.unwrap().unwrap()),[2]);
        assert_eq!(values(cursor.next_page().await.unwrap().unwrap()),[3]);
        assert!(cursor.next_page().await.unwrap().is_none());
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn production_sort_limit_applies_skip_after_the_complete_limited_sort_page() {
    let execution = execution(1);
    let relation = Arc::new(Relation::new(schema(),RelationNode::SortLimit {
        input: table(&[5,1,4,2,3]),order_by: vec![rtdb_plan::SortExpr { expr: schema().fields()[0].expression(),descending: false,nulls_first: false }],
        offset: Some(Expr::Literal(Value::I64(1))),fetch: Some(Expr::Literal(Value::I64(2))),
    }).unwrap());
    let runtime = EmittedRelation::derive(Arc::clone(&relation),&execution.context.catalog,execution.parameters.types(),&execution.context.memory).unwrap();
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = build_pages_constrained(Arc::clone(&runtime),execution.clone(),crate::inspection::Constraints::default(),None).await.unwrap();
    assert_eq!(values(cursor.next_page().await.unwrap().unwrap()),[2,3]);
    assert!(cursor.next_page().await.unwrap().is_none());
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

fn output_field(id: u32,data_type: LogicalType) -> Field { Field { id: ColumnId(id),name: format!("c{id}"),qualifier: None,qualified_only: false,unnamed: false,data_type } }
fn apply(input: Arc<Relation>,dependent_values: Vec<Expr>) -> Arc<Relation> {
    let output = output_field(1,LogicalType::Scalar(ScalarType::I64));
    let dependent = Arc::new(Relation::new(Schema::new(vec![output.clone()]).unwrap(),RelationNode::Values { rows: dependent_values.into_iter().map(|value|vec![value]).collect() }).unwrap());
    Arc::new(Relation::new(input.schema().concat(dependent.schema()).unwrap(),RelationNode::Apply {
        input,dependent,arguments: vec![(0,schema().fields()[0].expression())],kind: ApplyKind::Inner,output: None,
    }).unwrap())
}
fn unnest(input: Arc<Relation>,expr: Expr) -> Arc<Relation> {
    let element = output_field(1,LogicalType::Scalar(ScalarType::I64));
    let ordinality = output_field(2,LogicalType::Scalar(ScalarType::I32));
    let schema = input.schema().concat(&Schema::new(vec![element.clone(),ordinality.clone()]).unwrap()).unwrap();
    Arc::new(Relation::new(schema,RelationNode::Unnest { input,expr,element,ordinality: Some(ordinality),outer: false }).unwrap())
}
async fn open(relation: Arc<Relation>,execution: &Execution) -> BoxPageCursor {
    let runtime = EmittedRelation::derive(Arc::clone(&relation),&execution.context.catalog,execution.parameters.types(),&execution.context.memory).unwrap();
    build_pages_constrained(Arc::clone(&runtime),execution.clone(),crate::inspection::Constraints::default(),None).await.unwrap()
}

#[tokio::test]
async fn apply_and_unnest_expand_each_complete_independent_page() {
    for batch_rows in [1,1024] {
        let execution = execution(batch_rows);
        let independent = Arc::new(Relation::new(schema(),RelationNode::Set { left: table(&[1,2]),right: table(&[3]),operator: SetOperator::Union,all: true }).unwrap());
        let parameter = Expr::Parameter { index: 0,data_type: LogicalType::Scalar(ScalarType::I64) };
        let applied = apply(Arc::clone(&independent),vec![parameter.clone(),Expr::binary(rtdb_plan::BinaryOp::Add,parameter,Expr::Literal(Value::I64(10)))]);
        let column = schema().fields()[0].expression();
        let expanded = unnest(independent,Expr::List(vec![column.clone(),Expr::binary(rtdb_plan::BinaryOp::Add,column,Expr::Literal(Value::I64(10)))]));
        let baseline = execution.context.memory.used_bytes();
        for relation in [applied,expanded] {
            let mut cursor = open(relation,&execution).await;
            let mut first = cursor.next_page().await.unwrap().unwrap();assert_eq!(first.len(),4);
            let mut selected = Vec::new();while let Some(row) = first.next() { selected.push(row.values[1].clone()); }
            assert_eq!(selected,[Value::I64(1),Value::I64(11),Value::I64(2),Value::I64(12)]);
            let mut second = cursor.next_page().await.unwrap().unwrap();assert_eq!(second.len(),2);
            assert_eq!(second.next().unwrap().values[1],Value::I64(3));assert_eq!(second.next().unwrap().values[1],Value::I64(13));
            assert!(cursor.next_page().await.unwrap().is_none());
            drop(first);drop(second);drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
        }
    }
}

#[tokio::test]
async fn downstream_limit_does_not_hide_a_later_error_in_the_same_expansion_page() {
    let execution = execution(1);
    let parameter = Expr::Parameter { index: 0,data_type: LogicalType::Scalar(ScalarType::I64) };
    let applied = apply(table(&[1,0]),vec![Expr::binary(rtdb_plan::BinaryOp::Divide,Expr::Literal(Value::I64(1)),parameter)]);
    let expanded = unnest(table(&[1,0]),Expr::List(vec![Expr::binary(rtdb_plan::BinaryOp::Divide,Expr::Literal(Value::I64(1)),schema().fields()[0].expression())]));
    let baseline = execution.context.memory.used_bytes();
    for input in [applied,expanded] {
        let relation = Arc::new(Relation::new(input.schema().clone(),RelationNode::Limit { input,offset: None,fetch: Some(Expr::Literal(Value::I64(1))) }).unwrap());
        let mut cursor = open(relation,&execution).await;
        assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb.expression/division-by-zero");
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn apply_retains_empty_input_pages_while_unnest_skips_empty_expansions() {
    let execution = execution(1);
    let dependent = Arc::new(Relation::new(Schema::new(vec![output_field(1,LogicalType::Scalar(ScalarType::I64))]).unwrap(),RelationNode::Values { rows: vec![vec![Expr::Literal(Value::I64(1))]] }).unwrap());
    let runtime = EmittedRelation::derive(Arc::clone(&dependent),&execution.context.catalog,execution.parameters.types(),&execution.context.memory).unwrap();
    let (input,_) = input(vec![vec![]],&execution);
    let mut applied = Apply { input,schema: schema(),dependent: Arc::clone(&runtime),arguments: vec![],kind: ApplyKind::Inner,execution: execution.clone() };
    assert_eq!(applied.next_page().await.unwrap().unwrap().len(),0);
    assert!(applied.next_page().await.unwrap().is_none());
    let mut expanded = open(unnest(table(&[1]),Expr::List(vec![])),&execution).await;
    assert!(expanded.next_page().await.unwrap().is_none());
}

#[tokio::test]
async fn empty_values_have_no_callback_but_unit_has_one_zero_column_row() {
    let execution = execution(1);
    let mut empty = open(table(&[]),&execution).await;
    assert!(empty.next_page().await.unwrap().is_none());
    let mut unit = open(Arc::new(Relation::unit()),&execution).await;
    let mut page = unit.next_page().await.unwrap().unwrap();assert_eq!(page.len(),1);assert!(page.next().unwrap().values.is_empty());
    assert!(unit.next_page().await.unwrap().is_none());
    let mut applied = open(apply(table(&[]),vec![Expr::Literal(Value::I64(1))]),&execution).await;
    assert!(applied.next_page().await.unwrap().is_none());
}

fn grouped_count(input: Arc<Relation>,filter: Option<Expr>,global: bool) -> Arc<Relation> {
    let groups = if global { vec![] } else { vec![NamedExpr { field: output_field(1,LogicalType::Scalar(ScalarType::I64)),expr: schema().fields()[0].expression() }] };
    let aggregate = rtdb_plan::AggregateExpr { field: output_field(2,LogicalType::Scalar(ScalarType::I64)),function: rtdb_plan::AggregateFunction::CountStar,argument: None,distinct: false,filter,order_by: vec![] };
    let schema = Schema::new(groups.iter().map(|group|group.field.clone()).chain([aggregate.field.clone()]).collect()).unwrap();
    Arc::new(Relation::new(schema,RelationNode::Aggregate { input,groups,aggregates: vec![aggregate] }).unwrap())
}
fn row_numbers(input: Arc<Relation>) -> Arc<Relation> {
    let expression = rtdb_plan::WindowExpr { field: output_field(1,LogicalType::Scalar(ScalarType::I64)),function: rtdb_plan::WindowFunction::RowNumber };
    let schema = input.schema().concat(&Schema::new(vec![expression.field.clone()]).unwrap()).unwrap();
    Arc::new(Relation::new(schema,RelationNode::Window { input,partition_by: vec![],order_by: vec![],expressions: vec![expression] }).unwrap())
}

#[tokio::test]
async fn aggregate_and_window_publish_one_complete_result_page() {
    for batch_rows in [1,1024] {
        let execution = execution(batch_rows);let baseline = execution.context.memory.used_bytes();
        for (relation,count) in [(grouped_count(table(&[1,2,1]),None,false),2),(row_numbers(table(&[1,2,3])),3)] {
            let mut cursor = open(relation,&execution).await;
            let page = cursor.next_page().await.unwrap().unwrap();assert_eq!(page.len(),count);
            assert!(cursor.next_page().await.unwrap().is_none());
            drop(cursor);drop(page);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
        }
    }
}

#[tokio::test]
async fn aggregate_and_window_empty_results_preserve_global_count_row_only() {
    let execution = execution(1);
    for relation in [grouped_count(table(&[]),None,false),row_numbers(table(&[]))] {
        let mut cursor = open(relation,&execution).await;assert!(cursor.next_page().await.unwrap().is_none());
    }
    let mut global = open(grouped_count(table(&[]),None,true),&execution).await;
    let mut page = global.next_page().await.unwrap().unwrap();assert_eq!(page.len(),1);assert_eq!(page.next().unwrap().values,[Value::I64(0)]);
    assert!(global.next_page().await.unwrap().is_none());
}

#[tokio::test]
async fn limit_cannot_hide_a_filter_error_in_a_later_aggregate_result_group() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let filter = Expr::binary(rtdb_plan::BinaryOp::Eq,Expr::binary(rtdb_plan::BinaryOp::Divide,Expr::Literal(Value::I64(1)),schema().fields()[0].expression()),Expr::Literal(Value::I64(1)));
    let input = grouped_count(table(&[1,0]),Some(filter),false);
    let relation = Arc::new(Relation::new(input.schema().clone(),RelationNode::Limit { input,offset: None,fetch: Some(Expr::Literal(Value::I64(1))) }).unwrap());
    let mut cursor = open(relation,&execution).await;
    assert_eq!(cursor.next_page().await.err().unwrap().code,"xtdb.expression/division-by-zero");
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn evaluated_values_feed_the_batch_executor_as_arrow() {
    let execution = execution(2);
    let rows = [1, 3].map(|value| vec![Expr::binary(
        rtdb_plan::BinaryOp::Add, Expr::Literal(Value::I64(value)), Expr::Literal(Value::I64(1)),
    )]);
    let relation = Arc::new(Relation::new(schema(), RelationNode::Values { rows: rows.into() }).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], &execution.context.memory).unwrap();
    let mut batches = build_columns(runtime, execution).await.unwrap().unwrap();
    let batch = batches.next_batch().await.unwrap().unwrap();
    let [crate::columnar::Column::Arrow { array, .. }] = batch.columns.as_slice() else {
        panic!("expected computed Arrow column");
    };
    let values = array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
    assert_eq!(values.values().as_ref(), [2, 4]);
}

#[tokio::test]
async fn unnesting_a_computed_list_keeps_parent_payloads_in_arrow() {
    let execution = execution(128);
    let input = table(&[2, 3]);
    let parent = input.schema().fields()[0].expression();
    let element = Field { id: ColumnId(1), name: "element".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
    let ordinal = Field { id: ColumnId(2), name: "ordinal".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I32) };
    let expr = Expr::List(vec![parent, Expr::Literal(Value::I64(9))]);
    let schema = Schema::new(vec![input.schema().fields()[0].clone(), element.clone(), ordinal.clone()]).unwrap();
    let relation = Arc::new(Relation::new(schema, RelationNode::Unnest { input, expr, element, ordinality: Some(ordinal), outer: false }).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], &execution.context.memory).unwrap();
    let mut batches = build_columns(runtime, execution).await.unwrap().unwrap();
    let batch = batches.next_batch().await.unwrap().unwrap();
    assert_eq!(batch.len, 4);
    assert!(!matches!(batch.columns[0], crate::columnar::Column::Values(_)));
    assert_eq!(batch.columns[1].value(2).unwrap(), Value::I64(3));
    assert_eq!(batch.columns[2].value(3).unwrap(), Value::I32(2));
}

#[tokio::test]
async fn batch_analysis_counts_arrow_rows() {
    let mut execution = execution(128);
    let relation = table(&[2, 3, 5]);
    let analysis = crate::analysis::AnalysisNode::new(&relation, &execution.context).unwrap();
    execution.analysis = Some(Arc::clone(&analysis));
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], &execution.context.memory).unwrap();
    let mut batches = build_columns(runtime, execution).await.unwrap().unwrap();
    assert_eq!(batches.next_batch().await.unwrap().unwrap().len, 3);
    assert!(batches.next_batch().await.unwrap().is_none());
    let summary = analysis.rows(0).unwrap();
    assert_eq!(summary[0][6], Value::I64(1));
    assert_eq!(summary[0][7], Value::I64(3));
}
