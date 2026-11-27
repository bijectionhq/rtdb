use super::*;
use crate::columnar::{BatchCursor, BoxBatchCursor, ColumnBatch};
use crate::{cursor::OwnedRow, pages::PageBuilder};
use std::{collections::VecDeque,sync::atomic::{AtomicUsize,Ordering}};
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{ColumnId,Field};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{LogicalType,ScalarType,Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryContext,QueryOptions,context::OuterRows,parameters::BoundParameters};

struct Input { pages: VecDeque<Vec<i64>>,polls: Arc<AtomicUsize>,failure: Option<usize>,memory: Arc<MemoryBudget> }
#[async_trait]
impl BatchCursor for Input {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        let poll = self.polls.fetch_add(1,Ordering::SeqCst);
        if self.failure == Some(poll) { return Err(rtdb_types::Anomaly::fault("test/page","Source page failed")); }
        let Some(rows) = self.pages.pop_front() else { return Ok(None); };
        let mut page = PageBuilder::new(rows.len(),&self.memory)?;
        for value in rows { page.push(OwnedRow::new(vec![Value::I64(value)],&self.memory)?)?; }
        ColumnBatch::from_page(&page.finish(), &self.memory).map(Some)
    }
}
fn execution(batch_rows: usize) -> Execution {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    let context = QueryContext::new(catalog,MemoryBudget::new(1 << 26),Timestamp(0),"UTC",QueryOptions { batch_rows,..Default::default() },&CancellationToken::new()).unwrap();
    Execution { parameters: Arc::new(BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),context,bindings: None,analysis: None }
}
fn input(pages: Vec<Vec<i64>>,execution: &Execution) -> (BoxBatchCursor,Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    (Box::new(Input { pages: pages.into(),polls: Arc::clone(&polls),failure: None,memory: Arc::clone(&execution.context.memory) }),polls)
}
fn values(batch: ColumnBatch) -> Vec<i64> {
    (0..batch.len).map(|row| rtdb_expr::integer(&batch.columns[0].value(row).unwrap()).unwrap() as i64).collect()
}
fn schema() -> Schema { Schema::new(vec![Field { id: ColumnId(0),name: "n".into(),qualifier: None,qualified_only: false,unnamed: false,data_type: LogicalType::Scalar(ScalarType::I64) }]).unwrap() }
fn table(rows: &[i64]) -> Arc<Relation> {
    Arc::new(Relation::new(schema(),RelationNode::Values { rows: rows.iter().map(|value|vec![Expr::Literal(Value::I64(*value))]).collect() }).unwrap())
}

#[tokio::test]
async fn computed_unnest_lends_reached_text_and_marks_only_outer_rows_without_ordinality() {
    use arrow_array::{Array, ListArray, StringArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::{DataType, Field as ArrowField};
    use crate::columnar::{Column, single_batch};

    let execution = execution(1);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let source_memory = MemoryBudget::new(1 << 20);
    let values = StringArray::from(vec![Some("alpha"), None, Some("unreached")]);
    let address = values.value(0).as_ptr();
    let child = Arc::new(ArrowField::new("item", DataType::Utf8, true));
    let lists = ListArray::new(child, OffsetBuffer::new(vec![0_i32, 2, 2, 3].into()),
        Arc::new(values), Some(NullBuffer::from(vec![true, true, false])));
    let physical = Arc::new(ArrowField::new("items", lists.data_type().clone(), true));
    let data_type = rtdb_columnar::from_field(&physical).unwrap();
    let items = Field {
        id: ColumnId(0), name: "items".into(), qualifier: None, qualified_only: false, unnamed: false,
        data_type: data_type.clone(),
    };
    let schema = Schema::new(vec![items.clone()]).unwrap();
    let input = ColumnBatch::settled(vec![Column::Arrow { array: Arc::new(lists), field: physical, logical: Some(data_type) }],
        3, source_memory.reserve(4096).unwrap()).unwrap();
    let expr = Expr::Case {
        branches: vec![(Expr::Literal(Value::Bool(true)), items.expression())],
        otherwise: Box::new(Expr::Literal(Value::Null)),
    };
    let mut cursor = BatchUnnest {
        input: single_batch(Some(input), Arc::clone(&execution.context)), position: None, expr, schema, projected: false,
        ordinality: Some("n".into()), outer: true, execution: execution.clone(),
    };
    let result = cursor.next_batch().await.unwrap().unwrap();
    assert!(cursor.next_batch().await.unwrap().is_none());
    drop(cursor);

    let Column::Selected(elements) = &result.columns[1] else { panic!("Expected selected child spans"); };
    let ((array, _, logical), range) = elements.fragment_source(0..2).unwrap();
    assert_eq!(range, 0..2);
    assert_eq!(logical, Some(LogicalType::Scalar(ScalarType::Utf8).maybe()));
    let string = array.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(string.value(0).as_ptr(), address);
    let output = (0..result.len).map(|row| [result.columns[1].value(row).unwrap(), result.columns[2].value(row).unwrap()])
        .collect::<Vec<_>>();
    assert_eq!(output, [
        [Value::Utf8("alpha".into()), Value::I32(1)],
        [Value::Null, Value::I32(2)],
        [Value::Null, Value::Null],
        [Value::Null, Value::Null],
    ]);
    assert!(source_memory.used_bytes() > 0);
    drop((array, result));
    assert_eq!(source_memory.used_bytes(), 0);
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn computed_unnest_interleaves_compatible_text_columns_into_one_child_vector() {
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field as ArrowField};
    use crate::columnar::{Column, single_batch};

    let execution = execution(1);
    let memory = &execution.context.memory;
    let ty = LogicalType::Scalar(ScalarType::Utf8).maybe();
    let fields = [output_field(0, ty.clone()), output_field(1, ty.clone())];
    let schema = Schema::new(fields.to_vec()).unwrap();
    let columns = [vec![Some("a"), Some("c")], vec![None, Some("d")]].into_iter().zip(&fields).map(|(values, field)| {
        Column::Arrow {
            array: Arc::new(StringArray::from(values)),
            field: Arc::new(ArrowField::new(&field.name, DataType::Utf8, true)),
            logical: Some(ty.clone()),
        }
    }).collect();
    let batch = ColumnBatch::settled(columns, 2, memory.reserve(4096).unwrap()).unwrap();
    let expr = Expr::List(fields.iter().map(Field::expression).collect());
    let mut cursor = BatchUnnest {
        input: single_batch(Some(batch), Arc::clone(&execution.context)), position: None, expr, schema, projected: false,
        ordinality: Some("n".into()), outer: false, execution: execution.clone(),
    };
    let result = cursor.next_batch().await.unwrap().unwrap();
    let Column::Selected(elements) = &result.columns[2] else { panic!("Expected selected child spans"); };
    let ((array, _, logical), rows) = elements.fragment_source(0..result.len).unwrap();
    assert_eq!((logical, rows), (Some(ty), 0..4));
    let values = array.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(values.iter().collect::<Vec<_>>(), [Some("a"), None, Some("c"), Some("d")]);
    let ordinals = (0..result.len).map(|row| result.columns[3].value(row).unwrap()).collect::<Vec<_>>();
    assert_eq!(ordinals, [Value::I32(1), Value::I32(2), Value::I32(1), Value::I32(2)]);
}

#[tokio::test]
async fn top_slices_source_pages_and_does_not_pull_after_its_limit() {
    for batch_rows in [1,128] {
        let execution = execution(batch_rows);
        let (input,polls) = input(vec![vec![],vec![0,1,2],vec![3,4,5],vec![6]],&execution);
        let baseline = execution.context.memory.used_bytes();
        let mut cursor = BatchLimit { input: Some(input),offset: 1,remaining: Some(4),context: Arc::clone(&execution.context) };
        assert_eq!(values(cursor.next_batch().await.unwrap().unwrap()),[1,2]);
        assert_eq!(values(cursor.next_batch().await.unwrap().unwrap()),[3,4]);
        assert!(cursor.next_batch().await.unwrap().is_none());
        assert_eq!(polls.load(Ordering::SeqCst),3);
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);
    }
}

#[tokio::test]
async fn zero_limit_without_offset_does_not_advance_its_input() {
    let execution = execution(1);
    let (input,polls) = input(vec![vec![1]],&execution);
    let mut cursor = BatchLimit { input: Some(input),offset: 0,remaining: Some(0),context: Arc::clone(&execution.context) };
    assert!(cursor.next_batch().await.unwrap().is_none());
    assert_eq!(polls.load(Ordering::SeqCst),0);
}

#[tokio::test]
async fn zero_limit_with_positive_offset_consumes_complete_pages_until_the_offset() {
    let execution = execution(1);
    let (input,polls) = input(vec![vec![0,1,2,3],vec![4,5,6,7],vec![8,9,10,11],vec![12]],&execution);
    let mut cursor = BatchLimit { input: Some(input),offset: 10,remaining: Some(0),context: Arc::clone(&execution.context) };
    assert!(cursor.next_batch().await.unwrap().is_none());
    assert_eq!(polls.load(Ordering::SeqCst),3);
}

#[tokio::test]
async fn zero_limit_with_positive_offset_preserves_input_errors_before_the_offset() {
    let execution = execution(1);
    let polls = Arc::new(AtomicUsize::new(0));
    let input = Box::new(Input { pages: vec![vec![0,1,2]].into(),polls: Arc::clone(&polls),failure: Some(1),memory: Arc::clone(&execution.context.memory) });
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = BatchLimit { input: Some(input),offset: 10,remaining: Some(0),context: Arc::clone(&execution.context) };
    assert_eq!(cursor.next_batch().await.err().unwrap().code,"test/page");
    assert_eq!(polls.load(Ordering::SeqCst),2);
    drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn union_and_distinct_keep_complete_nonempty_source_selections() {
    let execution = execution(1);
    let (left,_) = input(vec![vec![],vec![1,1,2],vec![2]],&execution);
    let (right,_) = input(vec![vec![],vec![2,3,4],vec![4]],&execution);
    let runtime = RuntimeSchema::fixed(&schema(), &execution.context.memory).unwrap();
    let baseline = execution.context.memory.used_bytes();
    let input = Box::new(BatchUnion { left: Some(left), right });
    let mut cursor = BatchDistinct::new(input, Arc::clone(&runtime), execution.clone(), false).unwrap();
    assert_eq!(values(cursor.next_batch().await.unwrap().unwrap()),[1,2]);
    assert_eq!(values(cursor.next_batch().await.unwrap().unwrap()),[3,4]);
    assert!(cursor.next_batch().await.unwrap().is_none());
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
        let mut cursor = BatchSet {
            left, right: Some((right, false)), built: None, operator, all, seen: crate::set_keys::BatchKeyIndex::new(&execution.context).unwrap(),
            left_projected: false, runtime: Arc::clone(&runtime), execution: execution.clone(),
        };
        let mut observed = Vec::new();
        while let Some(page) = cursor.next_batch().await.unwrap() { observed.push(values(page)); }
        assert_eq!(observed,expected);
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn retained_selection_outlives_cursor_and_cancellation_prevents_the_next_page() {
    let execution = execution(1);
    let (input,_) = input(vec![vec![1,2],vec![3]],&execution);
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = BatchLimit { input: Some(input),offset: 0,remaining: None,context: Arc::clone(&execution.context) };
    let page = cursor.next_batch().await.unwrap().unwrap();
    execution.context.cancellation.cancel();
    assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb/query-cancelled");
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
        let mut cursor = build_column_pages(Arc::clone(&runtime),execution.clone()).await.unwrap().unwrap();
        assert_eq!(values(cursor.next_batch().await.unwrap().unwrap()),[2]);
        assert_eq!(values(cursor.next_batch().await.unwrap().unwrap()),[3]);
        assert!(cursor.next_batch().await.unwrap().is_none());
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn sort_and_top_k_keep_semantic_page_boundaries_through_arrow_spills() {
    for threshold in [1, usize::MAX] {
        let mut execution = execution(1);
        Arc::get_mut(&mut execution.context).unwrap().options.sort_memory_bytes = threshold;
        let baseline = execution.context.memory.used_bytes();
        for (offset, fetch, expected) in [
            (None, None, vec![1, 2, 3, 4, 5]),
            (Some(1), Some(2), vec![2, 3]),
        ] {
            let literal = |value| Expr::Literal(Value::I64(value));
            let relation = Arc::new(Relation::new(schema(), RelationNode::SortLimit {
                input: table(&[5, 1, 4, 2, 3]),
                order_by: vec![rtdb_plan::SortExpr { expr: schema().fields()[0].expression(), descending: false, nulls_first: false }],
                offset: offset.map(literal), fetch: fetch.map(literal),
            }).unwrap());
            let mut cursor = open(relation, &execution).await;
            let batch = cursor.next_batch().await.unwrap().unwrap();
            assert!(batch.columns[0].expression_cell(0).is_some());
            assert_eq!(values(batch), expected);
            assert!(cursor.next_batch().await.unwrap().is_none());
            drop(cursor);
            assert_eq!(execution.context.memory.used_bytes(), baseline);
            assert_eq!(execution.context.spill_bytes(), 0);
        }
        assert_eq!(execution.context.spill_peak_bytes() > 0, threshold == 1);
    }
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
async fn open(relation: Arc<Relation>,execution: &Execution) -> BoxBatchCursor {
    let runtime = EmittedRelation::derive(Arc::clone(&relation),&execution.context.catalog,execution.parameters.types(),&execution.context.memory).unwrap();
    build_column_pages(Arc::clone(&runtime),execution.clone()).await.unwrap().unwrap()
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
            let first = cursor.next_batch().await.unwrap().unwrap();assert_eq!(first.len,4);
            let selected = (0..first.len).map(|row| first.columns[1].value(row).unwrap()).collect::<Vec<_>>();
            assert_eq!(selected,[Value::I64(1),Value::I64(11),Value::I64(2),Value::I64(12)]);
            let second = cursor.next_batch().await.unwrap().unwrap();assert_eq!(second.len,2);
            assert_eq!(second.columns[1].value(0).unwrap(),Value::I64(3));assert_eq!(second.columns[1].value(1).unwrap(),Value::I64(13));
            assert!(cursor.next_batch().await.unwrap().is_none());
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
        assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb.expression/division-by-zero");
        drop(cursor);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn apply_and_unnest_do_not_evaluate_a_dependent_expression_for_empty_input() {
    let execution = execution(1);
    let input = Arc::new(Relation::new(schema(), RelationNode::LiteralPages { pages: vec![vec![]] }).unwrap());
    let fails = Expr::binary(rtdb_plan::BinaryOp::Divide, Expr::Literal(Value::I64(1)), Expr::Literal(Value::I64(0)));
    let mut applied = open(apply(input, vec![fails]), &execution).await;
    assert!(applied.next_batch().await.unwrap().is_none());
    let mut expanded = open(unnest(table(&[1]),Expr::List(vec![])),&execution).await;
    assert!(expanded.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn empty_values_have_no_callback_but_unit_has_one_zero_column_row() {
    let execution = execution(1);
    let mut empty = open(table(&[]),&execution).await;
    assert!(empty.next_batch().await.unwrap().is_none());
    let mut unit = open(Arc::new(Relation::unit()),&execution).await;
    let page = unit.next_batch().await.unwrap().unwrap();assert_eq!(page.len,1);assert!(page.columns.is_empty());
    assert!(unit.next_batch().await.unwrap().is_none());
    let mut applied = open(apply(table(&[]),vec![Expr::Literal(Value::I64(1))]),&execution).await;
    assert!(applied.next_batch().await.unwrap().is_none());
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
            let page = cursor.next_batch().await.unwrap().unwrap();assert_eq!(page.len,count);
            assert!(cursor.next_batch().await.unwrap().is_none());
            drop(cursor);drop(page);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
        }
    }
}

#[tokio::test]
async fn aggregate_and_window_empty_results_preserve_global_count_row_only() {
    let execution = execution(1);
    for relation in [grouped_count(table(&[]),None,false),row_numbers(table(&[]))] {
        let mut cursor = open(relation,&execution).await;assert!(cursor.next_batch().await.unwrap().is_none());
    }
    let mut global = open(grouped_count(table(&[]),None,true),&execution).await;
    let page = global.next_batch().await.unwrap().unwrap();assert_eq!(values(page),[0]);
    assert!(global.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn limit_cannot_hide_a_filter_error_in_a_later_aggregate_result_group() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let filter = Expr::binary(rtdb_plan::BinaryOp::Eq,Expr::binary(rtdb_plan::BinaryOp::Divide,Expr::Literal(Value::I64(1)),schema().fields()[0].expression()),Expr::Literal(Value::I64(1)));
    let input = grouped_count(table(&[1,0]),Some(filter),false);
    let relation = Arc::new(Relation::new(input.schema().clone(),RelationNode::Limit { input,offset: None,fetch: Some(Expr::Literal(Value::I64(1))) }).unwrap());
    let mut cursor = open(relation,&execution).await;
    assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb.expression/division-by-zero");
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
async fn numeric_list_batch_unnest_shares_one_child_array_across_parents() {
    use rtdb_plan::BinaryOp;

    let execution = execution(128);
    let baseline = execution.context.memory.used_bytes();
    let input = table(&[2, 3]);
    let parent = input.schema().fields()[0].expression();
    let element_type = LogicalType::from_legs([LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::F32), LogicalType::Null]);
    let element = Field { id: ColumnId(1), name: "element".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: element_type };
    let ordinal = Field { id: ColumnId(2), name: "ordinal".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I32) };
    let expr = Expr::List(vec![
        Expr::binary(BinaryOp::Add, parent, Expr::Literal(Value::I64(7))),
        Expr::Literal(Value::F32(-0.0)),
        Expr::Literal(Value::Null),
    ]);
    let schema = Schema::new(vec![input.schema().fields()[0].clone(), element.clone(), ordinal.clone()]).unwrap();
    let relation = Arc::new(Relation::new(schema, RelationNode::Unnest { input, expr, element, ordinality: Some(ordinal), outer: false }).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], &execution.context.memory).unwrap();
    let mut batches = build_columns(runtime, execution.clone()).await.unwrap().unwrap();
    let batch = batches.next_batch().await.unwrap().unwrap();
    assert!(batches.next_batch().await.unwrap().is_none());
    drop(batches);
    assert_eq!(batch.len, 6);
    assert!(!matches!(batch.columns[0], crate::columnar::Column::Values(_)));
    let (child, ..) = batch.columns[1].arrow_cell(0).unwrap().unwrap();
    assert_eq!(child.len(), 6);
    let expected = [
        (2, Value::I64(9), 1), (2, Value::F32(-0.0), 2), (2, Value::Null, 3),
        (3, Value::I64(10), 1), (3, Value::F32(-0.0), 2), (3, Value::Null, 3),
    ];
    for (row, (parent, element, ordinal)) in expected.into_iter().enumerate() {
        let (array, ..) = batch.columns[1].arrow_cell(row).unwrap().unwrap();
        assert!(std::ptr::eq(array, child));
        assert_eq!(batch.columns[0].value(row).unwrap(), Value::I64(parent));
        let actual = batch.columns[1].value(row).unwrap();
        assert_eq!(actual, element);
        if let Value::F32(value) = actual { assert_eq!(value.to_bits(), (-0.0f32).to_bits()); }
        assert_eq!(batch.columns[2].value(row).unwrap(), Value::I32(ordinal));
    }
    drop(batch);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn numeric_list_batch_projection_retains_the_list_and_element_layout() {
    use arrow_array::{Array, ListArray};
    use rtdb_plan::{BinaryOp, NamedExpr};

    let execution = execution(128);
    let input = table(&[2, 3]);
    let parent = input.schema().fields()[0].expression();
    let expr = Expr::List(vec![
        Expr::binary(BinaryOp::Add, parent, Expr::Literal(Value::I64(7))),
        Expr::Literal(Value::Null),
    ]);
    let field = Field { id: ColumnId(1), name: "items".into(), data_type: expr.data_type(), ..input.schema().fields()[0].clone() };
    let relation = Arc::new(input.project(vec![NamedExpr { field, expr }]).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], &execution.context.memory).unwrap();
    let mut batches = build_columns(runtime, execution).await.unwrap().unwrap();
    let batch = batches.next_batch().await.unwrap().unwrap();
    let crate::columnar::Column::Arrow { array, .. } = &batch.columns[0] else { panic!("expected one list array"); };
    let array = array.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(array.value_offsets(), [0, 2, 4]);
    assert_eq!(array.null_count(), 0);
    assert_eq!(batch.columns[0].value(0).unwrap(), Value::list(vec![Value::I64(9), Value::Null]));
    assert_eq!(batch.columns[0].value(1).unwrap(), Value::list(vec![Value::I64(10), Value::Null]));
}

#[tokio::test]
async fn numeric_list_batch_unnest_reports_the_first_reached_element_error() {
    use rtdb_plan::BinaryOp;

    let execution = execution(128);
    let baseline = execution.context.memory.used_bytes();
    let input = table(&[0, 1]);
    let element = Field { id: ColumnId(1), name: "element".into(), ..input.schema().fields()[0].clone() };
    let parent = input.schema().fields()[0].expression();
    let expr = Expr::List(vec![
        Expr::binary(BinaryOp::Add, Expr::Literal(Value::I64(i64::MAX)), parent.clone()),
        Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), parent),
    ]);
    let schema = Schema::new(vec![input.schema().fields()[0].clone(), element.clone()]).unwrap();
    let relation = Arc::new(Relation::new(schema, RelationNode::Unnest { input, expr, element, ordinality: None, outer: false }).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], &execution.context.memory).unwrap();
    let mut batches = build_columns(runtime, execution.clone()).await.unwrap().unwrap();
    assert_eq!(batches.next_batch().await.err().unwrap().code, "xtdb.expression/division-by-zero");
    drop(batches);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn coalescing_keeps_semantic_batch_order_and_drops_partial_output_on_failure() {
    let execution = execution(60);
    let context = &execution.context;
    let pages = (0..5).map(|page| (page * 60..(page + 1) * 60).collect::<Vec<i64>>()).collect::<Vec<_>>();
    let (input, _) = input(pages.clone(), &execution);
    let mut cursor = crate::columnar::coalesce_pages(input, Arc::clone(context));
    for span in [0..60, 60..120, 120..240, 240..300] {
        let batch = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(values(batch), span.collect::<Vec<_>>());
    }
    assert!(cursor.next_batch().await.unwrap().is_none());
    drop(cursor);

    let input = Box::new(Input {
        pages: pages.into(), polls: Arc::new(AtomicUsize::new(0)), failure: Some(3), memory: Arc::clone(&context.memory),
    });
    let baseline = context.memory.used_bytes();
    let mut cursor = crate::columnar::coalesce_pages(input, Arc::clone(context));
    drop(cursor.next_batch().await.unwrap().unwrap());
    drop(cursor.next_batch().await.unwrap().unwrap());
    assert_eq!(cursor.next_batch().await.err().unwrap().code, "test/page");
    drop(cursor);
    assert_eq!(context.memory.used_bytes(), baseline);
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
