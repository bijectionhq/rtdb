use rtdb_types::StructFields;
use std::collections::BTreeMap;
use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{AggregateExpr, AggregateFunction, ApplyKind, BinaryOp, ColumnId, Expr, Field, JoinKind, NamedExpr, Relation, RelationNode, Schema, SetOperator, SortExpr};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, Result, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn scalar(ty: ScalarType) -> LogicalType { LogicalType::Scalar(ty) }
fn field(id: u32, name: &str, ty: LogicalType) -> Field { Field { id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty } }
fn query_context(options: QueryOptions) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 24)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(100), "UTC", options, &CancellationToken::new()).unwrap()
}

#[tokio::test]
async fn direct_parameters_admit_signed_values_before_rows_and_keep_advertised_fields() {
    let unsigned = scalar(ScalarType::U64).maybe();
    let schema = Schema::new(vec![field(0, "v", unsigned.clone())]).unwrap();
    let relation = Arc::new(Relation::new(schema, RelationNode::Values { rows: vec![vec![Expr::Parameter { index: 0, data_type: unsigned.clone() }]] }).unwrap());
    let context = query_context(Default::default());
    let baseline = context.memory.used_bytes();
    let prepared = PreparedQuery::new(relation).unwrap();
    for (raw, expected) in [(Value::U64(u64::MAX), Value::I64(-1)), (Value::Null, Value::Null)] {
        let mut cursor = prepared.execute(Arc::clone(&context), vec![raw]).await.unwrap();
        assert_eq!(cursor.schema().fields()[0].data_type, unsigned);
        assert_eq!(cursor.runtime_schema().fields()[0].data_type, scalar(ScalarType::I64).maybe());
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), expected);
        drop(page); drop(cursor);
        assert_eq!(context.memory.used_bytes(), baseline);
    }
}

#[tokio::test]
async fn direct_query_uses_the_argument_type_to_open_an_undeclared_field() {
    let schema = Schema::new(vec![field(0, "v", LogicalType::Null)]).unwrap();
    let relation = Arc::new(Relation::new(schema, RelationNode::Values {
        rows: vec![vec![Expr::Parameter { index: 0, data_type: LogicalType::Null }]],
    }).unwrap());
    let context = query_context(Default::default());
    let prepared = PreparedQuery::new(relation).unwrap();
    assert_eq!(prepared.result_schema(&context).unwrap().schema().fields()[0].data_type, LogicalType::Null);
    let mut cursor = prepared.execute(context, vec![Value::I64(7)]).await.unwrap();
    assert_eq!(cursor.schema().fields()[0].data_type, scalar(ScalarType::I64));
    assert_eq!(cursor.runtime_schema(), cursor.schema());
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), Value::I64(7));
}

#[tokio::test]
async fn admitted_unsigned_values_sort_exactly_through_spills_and_release_on_cancel() {
    let ty = scalar(ScalarType::U64);
    let field = field(0, "v", ty.clone());
    let rows = (0..4).map(|index| vec![Expr::Parameter { index, data_type: ty.clone() }]).collect();
    let input = Arc::new(Relation::new(Schema::new(vec![field.clone()]).unwrap(), RelationNode::Values { rows }).unwrap());
    let sorted = Arc::new(input.sort(vec![SortExpr { expr: field.expression(), descending: false, nulls_first: false }]).unwrap());
    let context = query_context(QueryOptions { sort_memory_bytes: 1, batch_rows: 1, ..Default::default() });
    let prepared = PreparedQuery::new(sorted).unwrap();
    let values = vec![Value::U64((1 << 53) + 1), Value::U64(u64::MAX), Value::U64(1 << 63), Value::U64(1 << 53)];
    let mut cursor = prepared.execute(Arc::clone(&context), values.clone()).await.unwrap();
    let mut actual = Vec::new();
    while let Some(page) = cursor.next_batch().await.unwrap() { actual.push(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap()); }
    assert_eq!(actual, [Value::I64(i64::MIN), Value::I64(-1), Value::I64(1 << 53), Value::I64((1 << 53) + 1)]);
    drop(cursor);
    assert!(context.spill_peak_bytes() > 0);
    assert_eq!(context.spill_bytes(), 0);
    let mut cursor = prepared.execute(Arc::clone(&context), values).await.unwrap();
    let page = cursor.next_batch().await.unwrap().unwrap();
    cursor.cancel(); drop(cursor);
    assert_eq!(context.spill_bytes(), 0);
    assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), Value::I64(i64::MIN));
    drop(page);
    assert_eq!(context.memory.used_bytes(), 0);
}

#[derive(Default)]
struct Metrics { durations: std::sync::Mutex<Vec<rtdb_plan::ExecutionTimer>>, errors: std::sync::Mutex<Vec<rtdb_plan::ExecutionError>> }

#[tokio::test]
async fn tracing_cursor_parentage_counts_actual_operator_and_output_pages() {
    let context = query_context(QueryOptions { batch_rows: 2, ..Default::default() });
    let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(Arc::clone(&context.memory), 16, true, true).unwrap();
    let context = context.with_tracer(Some(tracer.clone()), None);
    let prepared = PreparedQuery::new(numbers(0, "n", &[1, 2, 3])).unwrap();
    let mut cursor = prepared.execute_with_text(Arc::clone(&context), vec![], Some("VALUES (1), (2), (3)")).await.unwrap();
    assert_eq!(cursor.next_batch().await.unwrap().unwrap().batch().num_rows(), 2);
    assert_eq!(cursor.next_batch().await.unwrap().unwrap().batch().num_rows(), 1);
    assert!(cursor.next_batch().await.unwrap().is_none()); drop(cursor); tracer.close();
    let mut spans = Vec::new(); while let Some(span) = receiver.next().await { spans.push(span); }
    let root = spans.iter().find(|span| span.name == "xtdb.query").unwrap();
    let output = spans.iter().find(|span| span.name == "query.cursor.output").unwrap();
    let table = spans.iter().find(|span| span.name == "query.cursor.table").unwrap();
    assert_eq!(output.parent, Some(root.context)); assert_eq!(table.parent, Some(root.context));
    assert!(root.attributes().any(|(name, value)| name == "query.peak_memory_bytes" && value.parse::<usize>().unwrap() > 0));
    assert!(output.attributes().any(|attribute| attribute == ("cursor.page_count", "2")));
    assert!(table.attributes().any(|attribute| attribute == ("cursor.page_count", "1")));
    assert!(table.attributes().any(|(name, value)| name == "cursor.peak_memory_bytes" && value.parse::<usize>().unwrap() > 0));
    assert!(!table.attributes().any(|(name, _)| name == "cursor.pull_count"));
    assert!(spans.iter().all(|span| span.ended >= span.started));
}

#[tokio::test]
async fn analyze_reports_the_arrow_union_it_actually_runs() {
    let context = query_context(Default::default());
    let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(MemoryBudget::new(1 << 20), 32, true, false).unwrap();
    let context = context.with_tracer(Some(tracer.clone()), None);
    let left = numbers(0, "v", &[1, 2]);
    let right = numbers(0, "v", &[3]);
    let union = Arc::new(Relation::new(left.schema().clone(), RelationNode::Set {
        left, right, operator: SetOperator::Union, all: true,
    }).unwrap());
    let prepared = PreparedQuery::from_prepared(rtdb_plan::PreparedStatement {
        statement: rtdb_plan::Statement::Explain { relation: union, mode: rtdb_plan::ExplainMode::Analyze },
        parameter_types: Default::default(), source: String::new(), notices: Vec::new(), settings: Default::default(),
    }, &context.memory).unwrap();
    let mut cursor = prepared.execute_columns(context, vec![]).await.unwrap();
    let mut union_peaks = None;
    while let Some(page) = cursor.next_batch().await.unwrap() {
        let batch = page.batch();
        let value = |row, col| read_value(batch.schema().field(col), batch.column(col).as_ref(), row).unwrap();
        for row in 0..batch.num_rows() {
            if matches!(value(row, 1), Value::Keyword(op) if op.as_str() == "union-all") {
                assert_eq!(value(row, 7), Value::I64(3));
                let (Value::I64(own), Value::I64(query)) = (value(row, 9), value(row, 10)) else { panic!("Expected memory peaks"); };
                assert!(own > 0 && query >= own);
                union_peaks = Some(own);
            }
        }
    }
    drop(cursor);
    tracer.close();
    let mut actual = None;
    while let Some(span) = receiver.next().await {
        if span.name == "query.cursor.union-all" {
            let attributes: BTreeMap<_, _> = span.attributes().collect();
            assert_eq!(attributes["union_all_columnar"], "true");
            actual = Some(attributes["cursor.peak_memory_bytes"].parse::<i64>().unwrap());
        }
    }
    assert!(actual.is_some());
    assert_eq!(actual, union_peaks);
}

#[tokio::test]
async fn prepared_projection_reuses_compatible_first_cells_and_funds_each_copy() -> Result<()> {
    use arrow_array::{ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, cast::AsArray};
    use arrow_buffer::NullBuffer;
    use rtdb_columnar::{OwnedBatch, to_field};

    let context = query_context(Default::default());
    let baseline = context.memory.used_bytes();
    let wide = scalar(ScalarType::I64);
    let types = [wide.clone(), scalar(ScalarType::Bool), scalar(ScalarType::I32), wide.clone().maybe()];
    let fields = ["wide", "flag", "narrow", "nullable"].into_iter().zip(&types);
    let schema = Schema::new(fields.clone().enumerate().map(|(id, (name, ty))| field(id as u32, name, ty.clone())).collect())?;
    let input = Arc::new(Relation::new(schema.clone(), RelationNode::Values {
        rows: vec![types.iter().enumerate().map(|(index, ty)| Expr::Parameter { index, data_type: ty.clone() }).collect()],
    })?);
    let alias = |id, name, position: usize| NamedExpr {
        field: field(id, name, types[position].clone()),
        expr: schema.fields()[position].expression(),
    };
    let addition = NamedExpr {
        field: field(5, "total", wide),
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(schema.fields()[0].expression()),
            right: Box::new(Expr::Literal(Value::I64(2))),
        },
    };
    let plan = input.project(vec![alias(4, "v", 2), addition, alias(6, "v", 2), alias(7, "flag", 1), alias(8, "nullable", 3)])?;
    let prepared = PreparedQuery::new(Arc::new(plan))?;

    let source_fields = fields.map(|(name, ty)| to_field(name, ty)).collect::<Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(source_fields)), vec![
        Arc::new(Int64Array::from(vec![17, 20])),
        Arc::new(BooleanArray::from(vec![true, true])),
        Arc::new(Int32Array::from(vec![7, 33])),
        Arc::new(Int64Array::try_new(vec![91, 42].into(), Some(NullBuffer::from(vec![false, true]))).unwrap()),
    ]).unwrap();
    let arguments = OwnedBatch::new(batch, &context.memory)?;
    let argument_charge = Arc::downgrade(arguments.reservation());
    let narrow_buffer = |array: &ArrayRef| array.as_any().downcast_ref::<Int32Array>().unwrap().values().as_ptr();
    let source = narrow_buffer(arguments.batch().column(2));

    let mut cursor = prepared.execute_parameter_projection(Arc::clone(&context), &arguments, &types, None).await?.unwrap();
    let page = cursor.next_batch().await?.unwrap();
    let row_charge = Arc::downgrade(page.reservation());
    let batch = page.batch();
    assert!(page.owns_schema());
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.schema_ref().fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(),
        ["v", "total", "v", "flag", "nullable"]);
    assert_eq!(narrow_buffer(batch.column(0)), source);
    assert_eq!(narrow_buffer(batch.column(2)), source);
    let values = batch.columns().iter().zip(batch.schema_ref().fields()).map(|(array, field)| {
        read_value(field, array.as_ref(), 0)
    }).collect::<Result<Vec<_>>>()?;
    assert_eq!(values, [Value::I32(7), Value::I64(19), Value::I32(7), Value::Bool(true), Value::Null]);
    assert_eq!(batch.column(3).as_boolean().values().values(), &[1]);
    let nullable = Arc::clone(batch.column(4));
    assert_eq!(nullable.as_any().downcast_ref::<Int64Array>().unwrap().values().as_ref(), &[0]);
    let shared = Arc::clone(batch.column(0));
    let computed = Arc::clone(batch.column(1));
    assert!(cursor.next_batch().await?.is_none());
    drop((arguments, page, cursor));
    assert!(row_charge.upgrade().is_none());
    assert!(argument_charge.upgrade().is_some());
    drop(shared);
    assert!(argument_charge.upgrade().is_none());
    let with_computed = context.memory.used_bytes();
    drop(computed);
    assert!(context.memory.used_bytes() < with_computed);
    assert!(context.memory.used_bytes() > baseline);
    drop(nullable);
    assert_eq!(context.memory.used_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn prepared_projection_detaches_a_tiny_view_of_an_oversized_argument() -> Result<()> {
    use arrow_array::{ArrayRef, RecordBatch, StringArray};
    use rtdb_columnar::OwnedBatch;

    let ty = scalar(ScalarType::Utf8);
    let schema = Schema::new(vec![field(0, "v", ty.clone())])?;
    let relation = Relation::new(schema, RelationNode::Values {
        rows: vec![vec![Expr::Parameter { index: 0, data_type: ty.clone() }]],
    })?;
    let prepared = PreparedQuery::new(Arc::new(relation))?;
    let context = query_context(Default::default());
    let large = "x".repeat(64 * 1024);
    let batch = RecordBatch::try_from_iter([("input", Arc::new(StringArray::from(vec!["first", large.as_str()])) as ArrayRef)]).unwrap();
    let arguments = OwnedBatch::new(batch, &context.memory)?.slice(0, 1)?;
    let source = Arc::downgrade(arguments.reservation());
    let mut cursor = prepared.execute_parameter_projection(context, &arguments, &[ty], None).await?.unwrap();
    let output = cursor.next_batch().await?.unwrap();
    let array = Arc::clone(output.batch().column(0));
    drop((arguments, output, cursor));
    assert!(source.upgrade().is_none());
    let text = array.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(text.value(0), "first");
    assert_eq!(text.value_data(), b"first");
    Ok(())
}

#[tokio::test]
async fn directly_projected_arrow_parameters_emit_an_operator_peak() {
    use arrow_array::{Int64Array, RecordBatch};
    let context = query_context(Default::default());
    let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(MemoryBudget::new(1 << 20), 16, true, false).unwrap();
    let context = context.with_tracer(Some(tracer.clone()), None);
    let ty = scalar(ScalarType::I64);
    let schema = Schema::new(vec![field(0, "v", ty.clone())]).unwrap();
    let plan = Arc::new(Relation::new(schema, RelationNode::Values {
        rows: vec![vec![Expr::Parameter { index: 0, data_type: ty.clone() }]],
    }).unwrap());
    let batch = RecordBatch::try_from_iter([("v", Arc::new(Int64Array::from(vec![7])) as arrow_array::ArrayRef)]).unwrap();
    let arguments = rtdb_columnar::OwnedBatch::new(batch, &context.memory).unwrap();
    let prepared = PreparedQuery::new(plan).unwrap();
    let mut cursor = prepared.execute_parameter_projection(context, &arguments, &[ty], Some("SELECT ? AS v")).await.unwrap().unwrap();
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(page.batch().num_rows(), 1);
    assert!(Arc::ptr_eq(page.batch().column(0), arguments.batch().column(0)));
    assert!(cursor.next_batch().await.unwrap().is_none());
    drop(cursor);
    tracer.close();
    let mut peak = None;
    while let Some(span) = receiver.next().await {
        if span.name == "query.cursor.table" {
            let attributes: BTreeMap<_, _> = span.attributes().collect();
            assert_eq!(attributes["cursor.row_count"], "1");
            peak = Some(attributes["cursor.peak_memory_bytes"].parse::<usize>().unwrap());
        }
    }
    assert!(peak.is_some_and(|bytes| bytes > 0));
}

#[tokio::test]
async fn cancelled_query_closes_started_operator_spans_and_unpulled_query_has_no_operator_spans() {
    let context = query_context(QueryOptions { batch_rows: 1, ..Default::default() });
    let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(Arc::clone(&context.memory), 16, true, true).unwrap();
    let context = context.with_tracer(Some(tracer.clone()), None);
    let prepared = PreparedQuery::new(numbers(0, "n", &[1, 2])).unwrap();
    drop(prepared.execute(Arc::clone(&context), vec![]).await.unwrap());
    let mut cursor = prepared.execute(Arc::clone(&context), vec![]).await.unwrap();
    let batch = cursor.next_batch().await.unwrap().unwrap();
    cursor.cancel(); drop(cursor); tracer.close();
    let mut spans = Vec::new(); while let Some(span) = receiver.next().await { spans.push(span); }
    assert_eq!(spans.iter().filter(|span| span.name == "xtdb.query").count(), 2);
    assert_eq!(spans.iter().filter(|span| span.name == "query.cursor.table").count(), 1);
    assert_eq!(spans.iter().filter(|span| span.name == "query.cursor.output").count(), 1);
    assert_eq!(batch.batch().num_rows(), 1);
    assert!(!context.cancellation.is_cancelled());
}
impl rtdb_plan::ExecutionObserver for Metrics {
    fn record_duration(&self, timer: rtdb_plan::ExecutionTimer, _: std::time::Duration) { self.durations.lock().unwrap().push(timer); }
    fn record_error(&self, error: rtdb_plan::ExecutionError) { self.errors.lock().unwrap().push(error); }
}

#[tokio::test]
async fn metered_cursor_records_lifetime_once_and_counts_pull_and_consumer_errors() {
    let metrics = Arc::new(Metrics::default());
    let context = query_context(Default::default()).with_observer(Some(metrics.clone()));
    let prepared = PreparedQuery::new(numbers(0, "n", &[1])).unwrap();
    let mut first = prepared.execute(Arc::clone(&context), vec![]).await.unwrap();
    let mut second = prepared.execute(Arc::clone(&context), vec![]).await.unwrap();
    assert!(first.next_batch().await.unwrap().is_some());
    assert!(first.next_batch().await.unwrap().is_none());
    assert!(metrics.durations.lock().unwrap().is_empty());
    first.record_consumer_error();
    first.cancel(); drop(first);
    assert_eq!(*metrics.durations.lock().unwrap(), [rtdb_plan::ExecutionTimer::Query]);
    assert!(second.next_batch().await.unwrap().is_some());
    context.cancellation.cancel();
    assert!(second.next_batch().await.is_err()); drop(second);
    assert_eq!(metrics.durations.lock().unwrap().len(), 2);
    assert_eq!(*metrics.errors.lock().unwrap(), [rtdb_plan::ExecutionError::Query, rtdb_plan::ExecutionError::Query]);
    drop(prepared.execute(query_context(Default::default()), vec![]).await.unwrap());
    assert_eq!(metrics.durations.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn patch_operator_preserves_a_values_page_across_output_batches() {
    let instant = scalar(ScalarType::Timestamp { unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) });
    let timestamp = |value| Value::Timestamp { value, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    let document = Value::Struct(StructFields::from([("v".into(), Value::I64(7))]));
    let fields = vec![field(0, "_iid", scalar(ScalarType::FixedSizeBinary(16))), field(1, "_valid_from", instant.clone()), field(2, "_valid_to", instant.clone().maybe()), field(3, "doc", document.logical_type())];
    let rows = [(1, 10, 20), (1, 30, 40), (2, 0, 50)].into_iter().map(|(iid, from, to)| vec![Value::FixedSizeBinary(vec![iid; 16]), timestamp(from), timestamp(to), document.clone()].into_iter().map(Expr::Literal).collect()).collect();
    let input = Arc::new(Relation::new(Schema::new(fields.clone()).unwrap(), RelationNode::Values { rows }).unwrap());
    let mut output = fields;
    output[3].data_type = output[3].data_type.clone().maybe();
    let relation = Arc::new(Relation::new(Schema::new(output).unwrap(), RelationNode::PatchGaps { input, iid: ColumnId(0), valid_from: ColumnId(1), valid_to: ColumnId(2), document: ColumnId(3), from: Expr::Literal(timestamp(0)), to: Expr::Literal(timestamp(50)) }).unwrap());
    let cursor = PreparedQuery::new(Arc::clone(&relation)).unwrap().execute(query_context(QueryOptions::default()), vec![]).await.unwrap();
    assert_eq!(cursor.runtime_schema(), cursor.schema());
    assert!(cursor.runtime_schema().get(ColumnId(3)).unwrap().data_type.nullable());
    drop(cursor);
    let rows = collect(relation, query_context(QueryOptions { batch_rows: 1, ..Default::default() })).await.unwrap();
    let intervals = rows.iter().map(|row| (rtdb_expr::timestamp_micros(&row[1], "UTC").unwrap().0, rtdb_expr::timestamp_micros(&row[2], "UTC").unwrap().0)).collect::<Vec<_>>();
    assert_eq!(intervals, [(0, 10), (10, 20), (20, 30), (30, 40), (40, 50), (0, 50)]);
    assert_eq!(rows.iter().map(|row| row[3].clone()).collect::<Vec<_>>(), [Value::Null, document.clone(), Value::Null, document.clone(), Value::Null, document]);
}
fn values(id: u32, name: &str, ty: LogicalType, rows: Vec<Value>) -> Arc<Relation> {
    Arc::new(Relation::new(Schema::new(vec![field(id, name, ty)]).unwrap(), RelationNode::Values { rows: rows.into_iter().map(|value| vec![Expr::Literal(value)]).collect() }).unwrap())
}
fn numbers(id: u32, name: &str, rows: &[i64]) -> Arc<Relation> { values(id, name, scalar(ScalarType::I64), rows.iter().copied().map(Value::I64).collect()) }
async fn collect(relation: Arc<Relation>, context: Arc<QueryContext>) -> Result<Vec<Vec<Value>>> {
    let mut cursor = PreparedQuery::new(relation)?.execute(context, vec![]).await?;
    let mut result = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() {
            result.push(batch.batch().schema().fields().iter().enumerate().map(|(index, field)| read_value(field, batch.batch().column(index).as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    Ok(result)
}
fn aggregate(input: Arc<Relation>, groups: Vec<NamedExpr>, aggregates: Vec<AggregateExpr>) -> Arc<Relation> {
    let schema = Schema::new(groups.iter().map(|group| group.field.clone()).chain(aggregates.iter().map(|aggregate| aggregate.field.clone())).collect()).unwrap();
    Arc::new(Relation::new(schema, RelationNode::Aggregate { input, groups, aggregates }).unwrap())
}
fn agg(id: u32, function: AggregateFunction, argument: Option<Expr>, ty: LogicalType) -> AggregateExpr {
    AggregateExpr { field: field(id, "result", ty), function, argument, distinct: false, filter: None, order_by: vec![] }
}
fn join(left: Arc<Relation>, right: Arc<Relation>, kind: JoinKind, condition: Option<Expr>) -> Arc<Relation> {
    let schema = match kind {
        JoinKind::Semi | JoinKind::Anti => left.schema().clone(),
        JoinKind::Left => left.schema().concat(&right.schema().nullable().unwrap()).unwrap(),
        JoinKind::Right => left.schema().nullable().unwrap().concat(right.schema()).unwrap(),
        JoinKind::Full => left.schema().nullable().unwrap().concat(&right.schema().nullable().unwrap()).unwrap(),
        _ => left.schema().concat(right.schema()).unwrap(),
    };
    Arc::new(Relation::new(schema, RelationNode::Join { left, right, kind, condition }).unwrap())
}

#[test]
fn transaction_catalog_contains_exactly_one_database_and_resolving_partition() {
    let state = rtdb_index::PartitionState::empty(rtdb_trie::TrieConfig::default()).unwrap();
    let snapshot = state.value().snapshot();
    let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
    for partitions in [vec![], vec![snapshot.clone(), snapshot.clone()]] {
        let databases = BTreeMap::from([("xtdb".into(), DatabaseSnapshot { partitions, storage: Arc::clone(&storage) })]);
        let error = match CatalogSnapshot::new("xtdb", databases, true) { Ok(_) => panic!("transaction catalog accepted a sibling partition"), Err(error) => error };
        assert_eq!(error.code, "rtdb.query/transaction-partition-scope");
    }
    let database = DatabaseSnapshot { partitions: vec![snapshot], storage };
    assert!(CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database.clone())]), true).is_ok());
    let error = match CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database.clone()), ("other".into(), database)]), true) {
        Ok(_) => panic!("transaction catalog accepted another database"), Err(error) => error,
    };
    assert_eq!(error.code, "rtdb.query/transaction-catalog-scope");
}

#[tokio::test]
async fn filters_projects_and_parameter_limits_obey_three_valued_predicates() {
    let input = numbers(0, "n", &[0, 1, 2, 3]);
    let column = input.schema().fields()[0].expression();
    let input = Arc::new(input.filter(Expr::binary(BinaryOp::Gt, column.clone(), Expr::Literal(Value::I64(0)))).unwrap());
    let projected = Arc::new(input.project(vec![NamedExpr { field: field(1, "doubled", scalar(ScalarType::I64)), expr: Expr::binary(BinaryOp::Multiply, column, Expr::Literal(Value::I64(2))) }]).unwrap());
    let limited = Arc::new(projected.limit(Some(Expr::Parameter { index: 0, data_type: scalar(ScalarType::I64) }), Some(Expr::Literal(Value::I64(1)))).unwrap());
    let mut cursor = PreparedQuery::new(limited).unwrap().execute(query_context(QueryOptions::default()), vec![Value::I64(1)]).await.unwrap();
    let batch = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(read_value(&batch.batch().schema().fields()[0], batch.batch().column(0).as_ref(), 0).unwrap(), Value::I64(4));
    assert!(cursor.next_batch().await.unwrap().is_none());
    let input = Arc::new(numbers(0, "n", &[1]).filter(Expr::Literal(Value::Null)).unwrap());
    assert!(collect(input, query_context(QueryOptions::default())).await.unwrap().is_empty());
}

#[tokio::test]
async fn external_sort_preserves_ties_and_explicit_null_order_and_releases_spills() {
    let input = values(0, "n", scalar(ScalarType::I64).maybe(), vec![Value::I64(1), Value::Null, Value::I64(3), Value::I64(2), Value::I64(3)]);
    let expression = input.schema().fields()[0].expression();
    let sorted = Arc::new(input.sort(vec![SortExpr { expr: expression, descending: true, nulls_first: true }]).unwrap());
    let options = QueryOptions { sort_memory_bytes: 1, batch_rows: 2, ..QueryOptions::default() };
    let context = query_context(options);
    assert_eq!(collect(sorted, Arc::clone(&context)).await.unwrap(), vec![vec![Value::Null], vec![Value::I64(3)], vec![Value::I64(3)], vec![Value::I64(2)], vec![Value::I64(1)]]);
    assert!(context.spill_peak_bytes() > 0);
    assert_eq!(context.spill_bytes(), 0);
    assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn cancelling_one_execution_preserves_its_sibling_and_retained_output_buffers() {
    let context = query_context(QueryOptions { batch_rows: 1, sort_memory_bytes: 1, ..QueryOptions::default() });
    let input = numbers(0, "n", &[3, 2, 1]);
    let expr = input.schema().fields()[0].expression();
    let prepared = PreparedQuery::new(Arc::new(input.sort(vec![SortExpr { expr, descending: false, nulls_first: false }]).unwrap())).unwrap();
    let mut first = prepared.execute(Arc::clone(&context), vec![]).await.unwrap();
    let mut sibling = prepared.execute(Arc::clone(&context), vec![]).await.unwrap();
    let batch = first.next_batch().await.unwrap().unwrap();
    let retained = Arc::clone(batch.batch().column(0));
    first.cancel();
    drop(batch);
    assert!(context.memory.used_bytes() > 0);
    assert_eq!(first.next_batch().await.unwrap_err().code, "xtdb/query-cancelled");
    assert!(sibling.next_batch().await.unwrap().is_some());
    drop(first);
    drop(sibling);
    assert_eq!(context.spill_bytes(), 0);
    assert!(context.memory.used_bytes() > 0);
    drop(retained);
    assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn spill_quota_failure_releases_partial_runs() {
    let context = query_context(QueryOptions { sort_memory_bytes: 1, spill_limit_bytes: 1, ..QueryOptions::default() });
    let input = numbers(0, "n", &[1, 2]);
    let expr = input.schema().fields()[0].expression();
    let sorted = Arc::new(input.sort(vec![SortExpr { expr, descending: false, nulls_first: false }]).unwrap());
    assert_eq!(collect(sorted, Arc::clone(&context)).await.unwrap_err().code, "rtdb.query/spill-limit");
    assert_eq!(context.spill_bytes(), 0);
    assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn distinct_groups_top_level_nulls_and_keeps_nonreflexive_compound_keys() {
    let compound = Value::Struct(StructFields::from([("x".into(), Value::Null)]));
    let rows = vec![Value::Null, Value::Null, compound.clone(), compound.clone(), Value::F64(f64::NAN), Value::F64(f64::NAN)];
    let ty = LogicalType::join_all(&rows.iter().map(Value::logical_type).collect::<Vec<_>>());
    let input = values(0, "key", ty, rows);
    let distinct = Arc::new(Relation::new(input.schema().clone(), RelationNode::Distinct { input }).unwrap());
    let rows = collect(distinct, query_context(QueryOptions::default())).await.unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0], vec![Value::Null]);
    assert_eq!(rows[1], vec![compound.clone()]);
    assert_eq!(rows[2], vec![compound]);
    assert!(matches!(rows[3][0], Value::F64(value) if value.is_nan()));
    assert!(matches!(rows[4][0], Value::F64(value) if value.is_nan()));
}

#[tokio::test]
async fn full_semi_anti_and_cross_joins_preserve_multiplicity_and_null_padding() {
    let left = numbers(0, "a", &[1, 2, 2]);
    let right = numbers(1, "b", &[2, 3]);
    let condition = Expr::binary(BinaryOp::Eq, left.schema().fields()[0].expression(), right.schema().fields()[0].expression());
    let rows = collect(join(Arc::clone(&left), Arc::clone(&right), JoinKind::Full, Some(condition.clone())), query_context(QueryOptions::default())).await.unwrap();
    assert_eq!(rows.len(), 4);
    for (expected, count) in [(vec![Value::I64(1), Value::Null], 1), (vec![Value::I64(2), Value::I64(2)], 2), (vec![Value::Null, Value::I64(3)], 1)] {
        assert_eq!(rows.iter().filter(|row| **row == expected).count(), count);
    }
    assert_eq!(collect(join(Arc::clone(&left), Arc::clone(&right), JoinKind::Semi, Some(condition.clone())), query_context(QueryOptions::default())).await.unwrap(), vec![vec![Value::I64(2)], vec![Value::I64(2)]]);
    assert_eq!(collect(join(Arc::clone(&left), Arc::clone(&right), JoinKind::Anti, Some(condition)), query_context(QueryOptions::default())).await.unwrap(), vec![vec![Value::I64(1)]]);
    assert_eq!(collect(join(left, right, JoinKind::Cross, None), query_context(QueryOptions::default())).await.unwrap().len(), 6);
}

#[tokio::test]
async fn outer_join_with_an_empty_zero_column_right_emits_one_row_per_left() {
    let left = numbers(0, "a", &[1, 2]);
    let right = Arc::new(Relation::empty(Schema::default()));
    assert_eq!(collect(join(left, right, JoinKind::Full, None), query_context(QueryOptions::default())).await.unwrap(), vec![vec![Value::I64(1)], vec![Value::I64(2)]]);
}

#[tokio::test]
async fn set_all_consumes_right_occurrences_and_distinct_coalesces_nulls() {
    for (operator, all, expected) in [
        (SetOperator::Intersect, true, vec![Value::I64(2), Value::I64(2), Value::Null]),
        (SetOperator::Except, true, vec![Value::I64(1), Value::I64(2), Value::Null]),
        (SetOperator::Intersect, false, vec![Value::I64(2), Value::Null]),
        (SetOperator::Except, false, vec![Value::I64(1)]),
    ] {
        let ty = scalar(ScalarType::I64).maybe();
        let left = values(0, "v", ty.clone(), vec![Value::I64(1), Value::I64(2), Value::I64(2), Value::I64(2), Value::Null, Value::Null]);
        let right = values(1, "v", ty, vec![Value::I64(2), Value::I64(2), Value::Null]);
        let plan = Arc::new(Relation::new(left.schema().clone(), RelationNode::Set { left, right, operator, all }).unwrap());
        assert_eq!(collect(plan, query_context(QueryOptions::default())).await.unwrap(), expected.into_iter().map(|value| vec![value]).collect::<Vec<_>>());
    }
}

#[tokio::test]
async fn empty_global_aggregates_and_groups_excluded_by_filter_keep_their_rows() {
    let input = numbers(0, "v", &[]);
    let argument = input.schema().fields()[0].expression();
    let plan = aggregate(input, vec![], vec![agg(1, AggregateFunction::CountStar, None, scalar(ScalarType::I64)), agg(2, AggregateFunction::Sum, Some(argument.clone()), scalar(ScalarType::I64).maybe()), agg(3, AggregateFunction::ArrayAgg, Some(argument), LogicalType::list(scalar(ScalarType::I64)).maybe())]);
    assert_eq!(collect(plan, query_context(QueryOptions::default())).await.unwrap(), vec![vec![Value::I64(0), Value::Null, Value::Null]]);
    let input = numbers(0, "v", &[1, 2]);
    let argument = input.schema().fields()[0].expression();
    let mut sum = agg(2, AggregateFunction::Sum, Some(argument.clone()), scalar(ScalarType::I64).maybe());
    sum.filter = Some(Expr::Literal(Value::Bool(false)));
    let group = NamedExpr { field: field(1, "v", scalar(ScalarType::I64)), expr: argument };
    assert_eq!(collect(aggregate(input, vec![group], vec![sum]), query_context(QueryOptions::default())).await.unwrap(), vec![vec![Value::I64(1), Value::Null], vec![Value::I64(2), Value::Null]]);
}

#[tokio::test]
async fn group_ids_preserve_nonreflexive_keys_and_top_level_null_groups() {
    let nested = Value::list(vec![Value::Null]);
    let ty = LogicalType::list(LogicalType::Null).maybe();
    let input = values(0, "key", ty.clone(), vec![Value::Null, Value::Null, nested.clone(), nested.clone()]);
    let group = NamedExpr { field: field(1, "key", ty), expr: input.schema().fields()[0].expression() };
    let count = agg(2, AggregateFunction::CountStar, None, scalar(ScalarType::I64));
    assert_eq!(collect(aggregate(input, vec![group], vec![count]), query_context(QueryOptions::default())).await.unwrap(), vec![vec![Value::Null, Value::I64(2)], vec![nested.clone(), Value::I64(1)], vec![nested, Value::I64(1)]]);
}

#[tokio::test]
async fn grouping_packs_computed_text_keys_under_pressure() -> Result<()> {
    use arrow_array::{RecordBatch, StringArray};
    use rtdb_columnar::{OwnedBatch, to_field};
    use rtdb_plan::LiteralArrowPages;

    let context = query_context(Default::default());
    let memory = &context.memory;
    let baseline = memory.used_bytes();
    let pressure = memory.reserve(memory.available_bytes() - (4 << 20))?;
    let text = scalar(ScalarType::Utf8).maybe();
    let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![to_field("name", &text)?]));
    let keys: Vec<_> = (0..64).map(|index| match index {
        0 => None,
        1 => Some(String::new()),
        _ => Some(format!("{index}-éß")),
    }).collect();
    let page = |repeats| -> Result<OwnedBatch> {
        let rows = keys.len() * repeats;
        let array = StringArray::from_iter(keys.iter().cycle().take(rows).map(|key| key.as_deref()));
        let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), vec![Arc::new(array)]).unwrap();
        OwnedBatch::new(batch, memory)
    };
    let input = Arc::new(Relation::new(Schema::new(vec![field(0, "name", text.clone())])?, RelationNode::ArrowLiteralPages {
        pages: LiteralArrowPages::new(vec![page(1)?, page(128)?]),
    })?);
    let group = NamedExpr {
        field: field(1, "key", text),
        expr: Expr::binary(BinaryOp::Concat, input.schema().fields()[0].expression(), Expr::Literal(Value::Utf8(String::new()))),
    };
    let count = agg(2, AggregateFunction::CountStar, None, scalar(ScalarType::I64));
    let expected: Vec<_> = keys.into_iter().map(|key| vec![key.map_or(Value::Null, Value::Utf8), Value::I64(129)]).collect();
    assert_eq!(collect(aggregate(input, vec![group], vec![count]), Arc::clone(&context)).await?, expected);
    drop(pressure);
    assert_eq!(memory.used_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn array_aggregate_orders_selected_rows_and_distinct_retains_null_arguments() {
    let input = values(0, "v", scalar(ScalarType::I64).maybe(), vec![Value::I64(2), Value::Null, Value::I64(1), Value::I64(2), Value::Null]);
    let argument = input.schema().fields()[0].expression();
    let mut array = agg(1, AggregateFunction::ArrayAgg, Some(argument.clone()), LogicalType::list(scalar(ScalarType::I64).maybe()).maybe());
    array.distinct = true;
    array.order_by.push(SortExpr { expr: argument, descending: false, nulls_first: true });
    assert_eq!(collect(aggregate(input, vec![], vec![array]), query_context(QueryOptions { sort_memory_bytes: 1, ..QueryOptions::default() })).await.unwrap(), vec![vec![Value::list(vec![Value::Null, Value::Null, Value::I64(1), Value::I64(2)])]]);
}

#[tokio::test]
async fn filtered_array_and_vec_aggregate_groups_promote_output_nullability() {
    for function in [AggregateFunction::ArrayAgg, AggregateFunction::VecAgg] {
        let input = numbers(0, "v", &[1, 2]);
        let argument = input.schema().fields()[0].expression();
        let group = NamedExpr { field: field(1, "v", scalar(ScalarType::I64)), expr: argument.clone() };
        let mut array = agg(2, function, Some(argument), LogicalType::list(scalar(ScalarType::I64)));
        array.filter = Some(Expr::Literal(Value::Bool(false)));
        let relation = aggregate(input, vec![group], vec![array]);
        let context = query_context(QueryOptions { batch_rows: 1, ..Default::default() });
        let prepared = PreparedQuery::new(relation).unwrap();
        let mut cursor = prepared.execute(Arc::clone(&context), vec![]).await.unwrap();
        assert_eq!(cursor.schema(), prepared.result_schema(&context).unwrap().schema());
        assert!(!cursor.schema().fields()[1].data_type.nullable());
        assert!(cursor.runtime_schema().fields()[1].data_type.nullable());
        for key in [1, 2] {
            let page = cursor.next_batch().await.unwrap().unwrap();
            assert_eq!(read_value(&page.batch().schema().fields()[0], page.batch().column(0).as_ref(), 0).unwrap(), Value::I64(key));
            assert_eq!(read_value(&page.batch().schema().fields()[1], page.batch().column(1).as_ref(), 0).unwrap(), Value::Null);
        }
        assert!(cursor.next_batch().await.unwrap().is_none());
        drop(cursor);
        assert_eq!(context.memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn min_max_preserve_narrow_integer_and_time_types() {
    use rtdb_types::TimeUnit;
    let time = |value, unit, bit_width| Value::Time { value, unit, bit_width };
    for (small, large) in [
        (Value::I8(-7), Value::I8(9)), (Value::I16(-257), Value::I16(1300)),
        (time(1, TimeUnit::Second, 32), time(7, TimeUnit::Second, 32)),
        (time(1, TimeUnit::Millisecond, 32), time(7000, TimeUnit::Millisecond, 32)),
        (time(1, TimeUnit::Microsecond, 64), time(7000, TimeUnit::Microsecond, 64)),
        (time(1, TimeUnit::Nanosecond, 64), time(7000, TimeUnit::Nanosecond, 64)),
    ] {
        let ty = small.logical_type().maybe();
        for (function, expected) in [(AggregateFunction::Min, small.clone()), (AggregateFunction::Max, large.clone())] {
            for filtered in [false, true] {
                let input = values(0, "v", ty.clone(), vec![large.clone(), Value::Null, small.clone()]);
                let mut expression = agg(1, function.clone(), Some(input.schema().fields()[0].expression()), ty.clone());
                if filtered { expression.filter = Some(Expr::Literal(Value::Bool(false))); }
                let context = query_context(QueryOptions { batch_rows: 1, ..Default::default() });
                let output = collect(aggregate(input, vec![], vec![expression]), context).await.unwrap();
                assert_eq!(output, [vec![if filtered { Value::Null } else { expected.clone() }]]);
            }
        }
    }
}

#[tokio::test]
async fn min_max_compare_calendar_days_and_return_the_winning_raw_date64_cells() {
    let ty = scalar(ScalarType::DateMilli).maybe();
    let input = values(0, "v", ty.clone(), vec![Value::DateMilli(86_400_001), Value::Null, Value::DateMilli(-1)]);
    let argument = input.schema().fields()[0].expression();
    let min = agg(1, AggregateFunction::Min, Some(argument.clone()), ty.clone());
    let mut max = agg(2, AggregateFunction::Max, Some(argument), ty);
    max.field.name = "max".into();
    let relation = aggregate(input, vec![], vec![min, max]);
    for arrow in [false, true] {
        let context = query_context(QueryOptions { batch_rows: 1, ..Default::default() });
        let prepared = PreparedQuery::new(Arc::clone(&relation)).unwrap();
        let mut cursor = if arrow { prepared.execute_columns(context, vec![]).await } else { prepared.execute(context, vec![]).await }.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        let batch = page.batch();
        assert_eq!(batch.num_rows(), 1);
        let read = |col| read_value(batch.schema().field(col), batch.column(col).as_ref(), 0).unwrap();
        assert_eq!([read(0), read(1)], [Value::DateMilli(-1), Value::DateMilli(86_400_001)]);
        assert!(cursor.next_batch().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn byte_and_short_sum_reject_nonempty_filtered_input_and_keep_empty_input_null() {
    for (ty, value) in [(scalar(ScalarType::I8), Value::I8(1)), (scalar(ScalarType::I16), Value::I16(1))] {
        let input = values(0, "v", ty.clone(), vec![value]);
        let mut sum = agg(1, AggregateFunction::Sum, Some(input.schema().fields()[0].expression()), ty.clone().maybe());
        sum.filter = Some(Expr::Literal(Value::Bool(false)));
        assert_eq!(collect(aggregate(input, vec![], vec![sum]), query_context(Default::default())).await.unwrap_err().code,
            "rtdb.query/small-integer-sum");
        let input = values(0, "v", ty.clone(), vec![]);
        let mut sum = agg(1, AggregateFunction::Sum, Some(input.schema().fields()[0].expression()), ty.maybe());
        sum.filter = Some(Expr::Literal(Value::Bool(false)));
        assert_eq!(collect(aggregate(input, vec![], vec![sum]), query_context(Default::default())).await.unwrap(), [vec![Value::Null]]);
    }
}

#[tokio::test]
async fn sum_wraps_reference_integer_width_and_variance_uses_reference_formula() {
    let input = numbers(0, "v", &[i64::MAX, 1]);
    let sum = agg(1, AggregateFunction::Sum, Some(input.schema().fields()[0].expression()), scalar(ScalarType::I64).maybe());
    assert_eq!(collect(aggregate(input, vec![], vec![sum]), query_context(QueryOptions::default())).await.unwrap(), vec![vec![Value::I64(i64::MIN)]]);
    let input = numbers(0, "v", &[1, 2, 3]);
    let argument = input.schema().fields()[0].expression();
    let variance = agg(1, AggregateFunction::VarianceSample, Some(argument.clone()), scalar(ScalarType::F64).maybe());
    let average = agg(2, AggregateFunction::Average, Some(argument), scalar(ScalarType::F64).maybe());
    assert_eq!(collect(aggregate(input, vec![], vec![variance, average]), query_context(QueryOptions::default())).await.unwrap(), vec![vec![Value::F64(1.0), Value::F64(2.0)]]);
}

fn decimal_cell(coefficient: i128, precision: i32, scale: i32, width: u16) -> Value {
    let bytes = coefficient.to_be_bytes(); let length = usize::from(width / 8);
    let mut unscaled = vec![if coefficient < 0 { 0xff } else { 0 }; length];
    let copied = length.min(bytes.len()); unscaled[length-copied..].copy_from_slice(&bytes[bytes.len()-copied..]);
    Value::Decimal(rtdb_types::DecimalValue::new(unscaled, precision, scale, width).unwrap())
}

async fn decimal_total(input_type: LogicalType, rows: Vec<Value>) -> Result<Vec<Vec<Value>>> {
    let output = LogicalType::least_upper_bound(std::slice::from_ref(&input_type)).unwrap().maybe();
    let input = values(0, "v", input_type, rows);
    let sum = agg(1, AggregateFunction::Sum, Some(input.schema().fields()[0].expression()), output);
    collect(aggregate(input, vec![], vec![sum]), query_context(QueryOptions::default())).await
}

#[tokio::test]
async fn decimal_vector_sum_preserves_precision_overflow_and_null_cell_storage() {
    let ty = scalar(ScalarType::Decimal { precision: 1, scale: 0, bit_width: 128 }).maybe();
    assert_eq!(decimal_total(ty.clone(), vec![Value::Null, decimal_cell(9, 1, 0, 128), decimal_cell(9, 1, 0, 128)]).await.unwrap(), vec![vec![decimal_cell(18, 1, 0, 128)]]);
    assert_eq!(decimal_total(ty.clone(), vec![Value::Null]).await.unwrap(), vec![vec![decimal_cell(0, 1, 0, 128)]]);
    assert_eq!(decimal_total(ty, vec![]).await.unwrap(), vec![vec![Value::Null]]);
    let ty = scalar(ScalarType::Decimal { precision: 1, scale: -2, bit_width: 128 }).maybe();
    assert_eq!(decimal_total(ty.clone(), vec![Value::Null, decimal_cell(1, 1, -2, 128)]).await.unwrap(), vec![vec![decimal_cell(1, 1, -2, 128)]]);
    assert_eq!(decimal_total(ty, vec![decimal_cell(1, 1, -2, 128), Value::Null]).await.unwrap(), vec![vec![decimal_cell(100, 1, -2, 128)]]);
}

#[tokio::test]
async fn decimal_sum_unions_use_output_scale_and_distinguish_null_and_integer_legs() {
    let decimal = |precision, scale| scalar(ScalarType::Decimal { precision, scale, bit_width: 128 });
    let ty = LogicalType::from_legs([decimal(1, 0), decimal(1, 2), LogicalType::Null]);
    assert_eq!(decimal_total(ty, vec![Value::Null, decimal_cell(1, 1, 0, 128), decimal_cell(2, 1, 2, 128), Value::Null]).await.unwrap(), vec![vec![decimal_cell(102, 3, 2, 32)]]);
    let ty = LogicalType::from_legs([decimal(1, -2), decimal(1, -1), LogicalType::Null]);
    assert_eq!(decimal_total(ty.clone(), vec![Value::Null]).await.unwrap(), vec![vec![Value::Null]]);
    assert_eq!(decimal_total(ty, vec![decimal_cell(1, 1, -2, 128), Value::Null]).await.unwrap(), vec![vec![decimal_cell(10, 2, -1, 32)]]);
    let ty = LogicalType::from_legs([decimal(1, 0), scalar(ScalarType::I64)]);
    assert!(decimal_total(ty, vec![decimal_cell(1, 1, 0, 128)]).await.is_err());
}

#[tokio::test]
async fn decimal_sum_accepts_wide_and_wrapped_precision_and_rejects_physical_overflow() {
    let decimal = |precision, scale, bit_width| scalar(ScalarType::Decimal { precision, scale, bit_width });
    let ty = LogicalType::from_legs([decimal(32, -100, 128), decimal(32, 100, 128)]);
    assert_eq!(decimal_total(ty, vec![decimal_cell(0, 32, -100, 128), decimal_cell(1, 32, 100, 128)]).await.unwrap(), vec![vec![decimal_cell(1, 232, 100, 256)]]);
    let ty = LogicalType::from_legs([decimal(32, 0, 128), decimal(32, i32::MAX, 128)]);
    assert_eq!(decimal_total(ty, vec![decimal_cell(0, 32, 0, 128), decimal_cell(0, 32, i32::MAX, 128)]).await.unwrap(), vec![vec![decimal_cell(0, -2_147_483_617, i32::MAX, 32)]]);
    let ty = decimal(i32::MAX, 0, 128);
    assert!(decimal_total(ty, vec![decimal_cell(i128::MAX, i32::MAX, 0, 128), decimal_cell(1, i32::MAX, 0, 128)]).await.is_err());
}

#[tokio::test]
async fn unnest_keeps_list_order_and_outer_ordinality_is_null() {
    let ty = LogicalType::list(scalar(ScalarType::I64));
    let input = values(0, "xs", ty, vec![Value::list(vec![Value::I64(4), Value::I64(5)]), Value::list(vec![])]);
    let expr = input.schema().fields()[0].expression();
    let element = field(1, "x", scalar(ScalarType::I64).maybe());
    let ordinal = field(2, "n", scalar(ScalarType::I32).maybe());
    let schema = input.schema().concat(&Schema::new(vec![element.clone(), ordinal.clone()]).unwrap()).unwrap();
    let plan = Arc::new(Relation::new(schema, RelationNode::Unnest { input, expr, element, ordinality: Some(ordinal), outer: true }).unwrap());
    let rows = collect(plan, query_context(QueryOptions::default())).await.unwrap();
    assert_eq!(rows.iter().map(|row| row[1..].to_vec()).collect::<Vec<_>>(), vec![vec![Value::I64(4), Value::I32(1)], vec![Value::I64(5), Value::I32(2)], vec![Value::Null, Value::Null]]);
}

#[tokio::test]
async fn computed_unnest_preserves_arrow_list_order_and_outer_ordinals() {
    use arrow_array::{Array, types::Float32Type, cast::AsArray};

    let input = values(0, "xs", LogicalType::list(scalar(ScalarType::F32).maybe()), vec![
        Value::list(vec![Value::F32(3.75), Value::Null, Value::F32(8.5)]),
        Value::list(vec![Value::F32(5.0)]),
    ]);
    let xs = &input.schema().fields()[0];
    let expr = Expr::Call {
        name: "trim_array".into(), args: vec![xs.expression(), Expr::Literal(Value::I64(1))],
        return_type: xs.data_type.clone(),
    };
    let element = field(1, "embedding_component", scalar(ScalarType::F32).maybe());
    let ordinal = field(2, "n", scalar(ScalarType::I32).maybe());
    let schema = input.schema().concat(&Schema::new(vec![element.clone(), ordinal.clone()]).unwrap()).unwrap();
    let plan = Arc::new(Relation::new(schema, RelationNode::Unnest { input, expr, element, ordinality: Some(ordinal), outer: true }).unwrap());
    let mut cursor = PreparedQuery::new(plan).unwrap().execute_columns(query_context(QueryOptions::default()), vec![]).await.unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            let component = batch.column(1);
            let value = if component.is_null(row) || component.data_type() == &arrow_schema::DataType::Null { None }
                else { Some(component.as_primitive::<Float32Type>().value(row)) };
            let ordinal = rtdb_columnar::read_value(batch.schema_ref().field(2), batch.column(2).as_ref(), row).unwrap();
            rows.push((value, ordinal));
        }
    }
    assert_eq!(rows, [(Some(3.75), Value::I32(1)), (None, Value::I32(2)), (None, Value::Null)]);
}

#[tokio::test]
async fn apply_binds_per_row_parameters_and_scalar_empty_returns_null() {
    let input = numbers(0, "a", &[0, 1]);
    let dependent = numbers(1, "b", &[1]);
    let predicate = Expr::binary(BinaryOp::Eq, dependent.schema().fields()[0].expression(), Expr::Parameter { index: 3, data_type: scalar(ScalarType::I64) });
    let dependent = Arc::new(dependent.filter(predicate).unwrap());
    let output = field(2, "answer", scalar(ScalarType::I64).maybe());
    let schema = input.schema().concat(&Schema::new(vec![output.clone()]).unwrap()).unwrap();
    let arguments = vec![(3, input.schema().fields()[0].expression())];
    let plan = Arc::new(Relation::new(schema, RelationNode::Apply { input, dependent, arguments, kind: ApplyKind::Scalar, output: Some(output) }).unwrap());
    assert_eq!(plan.parameter_count(), 0);
    assert_eq!(collect(plan, query_context(QueryOptions::default())).await.unwrap(), vec![vec![Value::I64(0), Value::Null], vec![Value::I64(1), Value::I64(1)]]);
}

#[tokio::test]
async fn left_apply_keeps_dependent_page_order_and_pads_an_unmatched_outer_row() {
    let input = numbers(0, "outer", &[1, 0, 1]);
    let first = numbers(1, "dependent", &[8, 6]);
    let second = numbers(1, "dependent", &[7]);
    let pages = Arc::new(Relation::new(first.schema().clone(), RelationNode::Set {
        left: first, right: second, operator: SetOperator::Union, all: true,
    }).unwrap());
    let enabled = Expr::binary(BinaryOp::Eq, Expr::Parameter { index: 0, data_type: scalar(ScalarType::I64) },
        Expr::Literal(Value::I64(1)));
    let dependent = Arc::new(pages.filter(enabled).unwrap());
    let schema = input.schema().concat(&dependent.schema().nullable().unwrap()).unwrap();
    let arguments = vec![(0, input.schema().fields()[0].expression())];
    let plan = Arc::new(Relation::new(schema, RelationNode::Apply {
        input, dependent, arguments, kind: ApplyKind::Left, output: None,
    }).unwrap());
    let rows = collect(plan, query_context(QueryOptions { batch_rows: 2, ..Default::default() })).await.unwrap();
    assert_eq!(rows, [
        vec![Value::I64(1), Value::I64(8)],
        vec![Value::I64(1), Value::I64(6)],
        vec![Value::I64(1), Value::I64(7)],
        vec![Value::I64(0), Value::Null],
        vec![Value::I64(1), Value::I64(8)],
        vec![Value::I64(1), Value::I64(6)],
        vec![Value::I64(1), Value::I64(7)],
    ]);
}

#[tokio::test]
async fn spread_and_skewed_join_fanout_keep_probe_payloads_in_order_through_spill() {
    let int = scalar(ScalarType::I64);
    let float = scalar(ScalarType::F64).maybe();
    let probe_key = field(0, "probe", int.clone());
    let probe_payload = field(1, "payload", float);
    let build_key = field(2, "build", int.clone());
    let build_payload = field(3, "match_id", int);
    let probe_values = [Value::F64(2.0), Value::F64(3.0), Value::Null, Value::F64(11.0)]
        .into_iter().chain(std::iter::repeat_n(Value::F64(5.0), 28)).collect::<Vec<_>>();
    let probe_rows = probe_values.iter().enumerate().map(|(key, payload)| {
        vec![Expr::Literal(Value::I64(key as i64)), Expr::Literal(payload.clone())]
    }).collect();
    let probe = Arc::new(Relation::new(
        Schema::new(vec![probe_key.clone(), probe_payload]).unwrap(),
        RelationNode::Values { rows: probe_rows },
    ).unwrap());

    for multiplicities in [[9, 9, 9], [1, 23, 1]] {
        let build_rows = multiplicities.into_iter().enumerate().flat_map(|(key, count)| {
            (0..count).map(move |offset| {
                vec![Expr::Literal(Value::I64(key as i64)), Expr::Literal(Value::I64(offset))]
            })
        }).collect();
        let build = Arc::new(Relation::new(
            Schema::new(vec![build_key.clone(), build_payload.clone()]).unwrap(),
            RelationNode::Values { rows: build_rows },
        ).unwrap());
        let condition = Expr::binary(BinaryOp::Eq, probe_key.expression(), build_key.expression());
        let joined = join(Arc::clone(&probe), build, JoinKind::Left, Some(condition));

        let mut expected = Vec::new();
        for (key, payload) in probe_values.iter().enumerate() {
            let count = multiplicities.get(key).copied().unwrap_or(0);
            let key = Value::I64(key as i64);
            if count == 0 {
                expected.push(vec![key, payload.clone(), Value::Null, Value::Null]);
            } else {
                for offset in 0..count {
                    expected.push(vec![key.clone(), payload.clone(), key.clone(), Value::I64(offset)]);
                }
            }
        }
        for join_build_rows in [i64::MAX, 0] {
            let context = query_context(QueryOptions { join_build_rows, batch_rows: 4, ..Default::default() });
            let mut actual = collect(Arc::clone(&joined), Arc::clone(&context)).await.unwrap();
            if join_build_rows == 0 {
                actual.sort_by_key(|row| match &row[0] {
                    Value::I64(key) => *key,
                    _ => panic!("Expected an integer probe key"),
                });
            }
            assert_eq!(actual, expected);
            assert_eq!(context.spill_peak_bytes() > 0, join_build_rows == 0);
            assert_eq!(context.memory.used_bytes(), 0);
            assert_eq!(context.spill_bytes(), 0);
        }

        let start = multiplicities[0] + 1;
        let limited = Arc::new(joined.limit(Some(Expr::Literal(Value::I64(start))), Some(Expr::Literal(Value::I64(7)))).unwrap());
        let actual = collect(limited, query_context(QueryOptions { batch_rows: 4, ..Default::default() })).await.unwrap();
        assert_eq!(actual, expected[start as usize..start as usize + 7]);
    }
}

#[tokio::test]
async fn single_join_preserves_probe_rows_and_checks_cardinality_in_memory_and_on_disk() {
    for join_build_rows in [0, i64::MAX] {
        for build in [&[2, 3][..], &[2, 2][..]] {
            let options = QueryOptions { join_build_rows, ..Default::default() };
            let context = query_context(options);
            let left = numbers(0, "probe", &[1, 2]);
            let right = numbers(1, "build", build);
            let condition = Expr::binary(BinaryOp::Eq, left.schema().fields()[0].expression(), right.schema().fields()[0].expression());
            let schema = left.schema().concat(&right.schema().nullable().unwrap()).unwrap();
            let plan = Arc::new(Relation::new(schema, RelationNode::Join { left, right, kind: JoinKind::Single, condition: Some(condition) }).unwrap());
            let result = collect(plan, Arc::clone(&context)).await;
            if build[0] == build[1] {
                assert_eq!(result.unwrap_err().code, "xtdb.single-join/cardinality-violation");
            } else {
                let mut rows = result.unwrap();
                rows.sort_by_key(|row| match row[0] { Value::I64(value) => value, _ => panic!("Expected probe key") });
                assert_eq!(rows, [vec![Value::I64(1), Value::Null], vec![Value::I64(2), Value::I64(2)]]);
            }
            assert_eq!(context.spill_peak_bytes() > 0, join_build_rows == 0);
            assert_eq!(context.memory.used_bytes(), 0);
            assert_eq!(context.spill_bytes(), 0);
        }
    }
}

#[tokio::test]
async fn scalar_apply_rejects_multiple_rows_and_pull_many_distinguishes_empty_document() {
    let input = Arc::new(Relation::unit());
    let dependent = numbers(1, "v", &[1, 2]);
    let output = field(2, "answer", scalar(ScalarType::I64).maybe());
    let plan = Arc::new(Relation::new(Schema::new(vec![output.clone()]).unwrap(), RelationNode::Apply { input: Arc::clone(&input), dependent, arguments: vec![], kind: ApplyKind::Scalar, output: Some(output) }).unwrap());
    assert_eq!(collect(plan, query_context(QueryOptions::default())).await.unwrap_err().code, "xtdb.single-join/cardinality-violation");
    for (dependent, expected) in [(Arc::new(Relation::empty(Schema::default())), Value::Null), (Arc::new(Relation::unit()), Value::list(vec![Value::Struct(StructFields::new())]))] {
        let output = field(2, "answer", LogicalType::list(LogicalType::Struct(StructFields::new())).maybe());
        let plan = Arc::new(Relation::new(Schema::new(vec![output.clone()]).unwrap(), RelationNode::Apply { input: Arc::clone(&input), dependent, arguments: vec![], kind: ApplyKind::PullMany, output: Some(output) }).unwrap());
        assert_eq!(collect(plan, query_context(QueryOptions::default())).await.unwrap(), vec![vec![expected]]);
    }
}

#[tokio::test]
async fn pull_many_keeps_document_field_order_offsets_and_array_custody() {
    use arrow_array::{Array, cast::AsArray};

    let integer = scalar(ScalarType::I64);
    let keyword = scalar(ScalarType::Keyword);
    let x = Value::Keyword(rtdb_types::Keyword::parse("test/x").unwrap());
    let y = Value::Keyword(rtdb_types::Keyword::parse("test/y").unwrap());
    let fields = Schema::new(vec![field(1, "z", integer.clone()), field(2, "a", keyword.clone())]).unwrap();
    let dependent = Arc::new(Relation::new(fields, RelationNode::Values { rows: vec![
        vec![Expr::Literal(Value::I64(2)), Expr::Literal(x.clone())],
        vec![Expr::Literal(Value::I64(3)), Expr::Literal(y.clone())],
    ] }).unwrap());
    let enabled = Expr::binary(BinaryOp::Eq, Expr::Parameter { index: 0, data_type: integer.clone() },
        Expr::Literal(Value::I64(1)));
    let dependent = Arc::new(dependent.filter(enabled).unwrap());
    let doc = LogicalType::Struct(StructFields::from([("z".into(), integer), ("a".into(), keyword.clone())]));
    let output = field(3, "docs", LogicalType::list(doc).maybe());

    let context = query_context(QueryOptions::default());
    let baseline = context.memory.used_bytes();
    for (last, offsets, expected) in [
        (0, [0, 2, 2, 2], vec![x.clone(), y.clone()]),
        (1, [0, 2, 2, 4], vec![x.clone(), y.clone(), x, y]),
    ] {
        let input = numbers(0, "outer", &[1, 0, last]);
        let schema = input.schema().concat(&Schema::new(vec![output.clone()]).unwrap()).unwrap();
        let arguments = vec![(0, input.schema().fields()[0].expression())];
        let plan = Arc::new(Relation::new(schema, RelationNode::Apply {
            input, dependent: Arc::clone(&dependent), arguments, kind: ApplyKind::PullMany, output: Some(output.clone()),
        }).unwrap());
        let mut cursor = PreparedQuery::new(plan).unwrap().execute_columns(Arc::clone(&context), vec![]).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        let retained = Arc::clone(page.batch().column(1));
        drop(page);
        drop(cursor);
        assert!(context.memory.used_bytes() > baseline);
        let lists = retained.as_list::<i32>();
        assert_eq!(lists.value_offsets(), &offsets);
        assert_eq!((lists.is_valid(0), lists.is_valid(1), lists.is_valid(2)), (true, false, last == 1));
        let docs = lists.values().as_struct();
        assert_eq!(docs.fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), ["z", "a"]);
        assert_eq!(rtdb_columnar::from_field(&docs.fields()[1]).unwrap(), keyword);
        let actual = (0..docs.len()).map(|row| read_value(&docs.fields()[1], docs.column(1).as_ref(), row).unwrap()).collect::<Vec<_>>();
        assert_eq!(actual, expected);
        drop(retained);
        assert_eq!(context.memory.used_bytes(), baseline);
    }
}

#[test]
fn catalog_bindings_reserve_before_enumeration_and_clones_retain_the_charge() {
    let context = query_context(Default::default()); let memory = MemoryBudget::new(8 << 20);
    let tables = context.catalog.tables_reserved("xtdb", &memory).unwrap(); assert!(!tables.rows().is_empty()); let used = memory.used_bytes(); assert!(used > 0);
    let retained = tables.clone(); drop(tables); assert_eq!(memory.used_bytes(), used); drop(retained); assert_eq!(memory.used_bytes(), 0);
    let exhausted = MemoryBudget::new(1); assert!(context.catalog.tables_reserved("xtdb", &exhausted).is_err()); assert_eq!(exhausted.used_bytes(), 0);
}

#[test]
fn table_names_and_one_selected_binding_fit_without_funding_the_whole_catalog() {
    use rtdb_plan::QualifiedTable;
    use rtdb_types::TableRef;

    let context = query_context(Default::default());
    let memory = MemoryBudget::new(1 << 16);
    assert!(context.catalog.tables_reserved("xtdb", &memory).is_err());
    let names = context.catalog.table_names_reserved("xtdb", &memory).unwrap();
    let table = TableRef::new("pg_catalog", "pg_views");
    assert!(names.rows().binary_search(&table).is_ok());
    let name = QualifiedTable { database: "xtdb".into(), table };
    let (binding, charge) = context.catalog.table_binding_reserved(&name, &memory).unwrap().unwrap();
    assert_eq!(binding.name, name);
    assert!(binding.columns.contains_key("viewname"));
    drop(binding);
    drop(charge);
    drop(names);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn extension_retains_input_fields_and_resolves_appended_fields_in_input_scope() {
    let context = query_context(QueryOptions { batch_rows: 1, ..Default::default() });
    let baseline = context.memory.used_bytes();
    let input_field = field(0, "v", scalar(ScalarType::U64));
    let input = Arc::new(Relation::new(
        Schema::new(vec![input_field.clone()]).unwrap(),
        RelationNode::Values { rows: vec![vec![Expr::Parameter { index: 0, data_type: input_field.data_type.clone() }]] },
    ).unwrap());
    let copy = field(1, "copy", input_field.data_type.clone());
    let computed = field(2, "increment", scalar(ScalarType::I64));
    let extension = Arc::new(input.extend(vec![
        NamedExpr { field: copy.clone(), expr: input_field.expression() },
        NamedExpr { field: computed.clone(), expr: Expr::binary(BinaryOp::Add, input_field.expression(), Expr::Literal(Value::I64(1))) },
    ]).unwrap());
    assert_eq!(extension.schema().fields(), &[input_field, copy, computed]);

    let mut cursor = PreparedQuery::new(extension).unwrap().execute(Arc::clone(&context), vec![Value::U64(u64::MAX)]).await.unwrap();
    assert_eq!(cursor.runtime_schema().fields().iter().map(|field| (&*field.name, field.data_type.clone())).collect::<Vec<_>>(), [
        ("v", scalar(ScalarType::I64)), ("copy", scalar(ScalarType::I64)), ("increment", scalar(ScalarType::I64)),
    ]);
    let result = cursor.next_batch().await.unwrap().unwrap();
    let row = result.batch().schema().fields().iter().zip(result.batch().columns())
        .map(|(field, column)| read_value(field, column.as_ref(), 0).unwrap()).collect::<Vec<_>>();
    assert_eq!(row, [Value::I64(-1), Value::I64(-1), Value::I64(0)]);
    assert!(cursor.next_batch().await.unwrap().is_none());
    drop(result);
    drop(cursor);
    assert_eq!(context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn extension_materializes_a_full_input_page_before_limit_can_consume_a_row() {
    let context = query_context(QueryOptions { batch_rows: 1, ..Default::default() });
    let baseline = context.memory.used_bytes();
    let input = numbers(0, "v", &[1, 0]);
    let extended = Arc::new(Arc::clone(&input).extend(vec![NamedExpr {
        field: field(1, "reduced", scalar(ScalarType::I64)),
        expr: Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), input.schema().fields()[0].expression()),
    }]).unwrap());
    let limited = Arc::new(extended.limit(None, Some(Expr::Literal(Value::I64(1)))).unwrap());
    let mut cursor = PreparedQuery::new(limited).unwrap().execute(Arc::clone(&context), vec![]).await.unwrap();
    let error = match cursor.next_batch().await { Err(error) => error, Ok(_) => panic!("extension published a partial input page") };
    assert_eq!(error.code, "xtdb.expression/division-by-zero");
    drop(cursor);
    assert_eq!(context.memory.used_bytes(), baseline);
}

#[test]
fn extension_rejects_reused_columns_and_appended_sibling_references() {
    let input = numbers(0, "v", &[1]);
    let existing = input.schema().fields()[0].clone();
    let reused = Arc::clone(&input).extend(vec![NamedExpr { field: existing.clone(), expr: existing.expression() }]);
    assert!(reused.is_err());
    let appended = field(1, "appended", scalar(ScalarType::I64));
    let sibling = input.extend(vec![
        NamedExpr { field: appended.clone(), expr: existing.expression() },
        NamedExpr { field: field(2, "sibling", scalar(ScalarType::I64)), expr: appended.expression() },
    ]).unwrap_err();
    assert_eq!(sibling.code, "rtdb.plan/unbound-column");
}

fn spill_payload_columns() -> Vec<(&'static str, Vec<Value>)> {
    use rtdb_types::{Keyword, TimeUnit};
    let uuid = |seed: u8| Value::Uuid(std::array::from_fn(|index| seed.wrapping_mul(17).wrapping_add(index as u8)));
    let structure = |pairs: Vec<(&str, Value)>| Value::Struct(pairs.into_iter().map(|(name, value)| (name.to_owned(), value)).collect());
    vec![
        ("uuid", vec![uuid(1), Value::Null, uuid(2), Value::Uuid([0; 16]), Value::Uuid([0xff; 16]), uuid(3), Value::Null, uuid(4)]),
        ("fsb", vec![Value::FixedSizeBinary(vec![1, 2, 3, 4]), Value::Null, Value::FixedSizeBinary(vec![0; 4]), Value::FixedSizeBinary(vec![0xff; 4]), Value::FixedSizeBinary(vec![9, 8, 7, 6]), Value::Null, Value::FixedSizeBinary(vec![0, 1, 0, 1]), Value::FixedSizeBinary(vec![5; 4])]),
        ("mixed", vec![Value::I64(1), Value::Utf8("x".into()), Value::F64(2.5), Value::Bool(true), Value::Null, Value::Keyword(Keyword::parse("a/b").unwrap()), uuid(5), Value::list(vec![Value::I64(1), Value::Null])]),
        ("f64", vec![Value::F64(f64::NAN), Value::F64(f64::INFINITY), Value::F64(f64::NEG_INFINITY), Value::F64(-0.0), Value::F64(0.0), Value::F64(f64::MIN_POSITIVE), Value::Null, Value::F64(-1.5)]),
        ("f32", vec![Value::F32(f32::NAN), Value::F32(f32::INFINITY), Value::F32(f32::NEG_INFINITY), Value::F32(-0.0), Value::Null, Value::F32(f32::MAX), Value::F32(f32::MIN_POSITIVE), Value::F32(0.25)]),
        ("decimal", vec![decimal_cell(12_345, 32, 2, 128), decimal_cell(-1, 32, 2, 128), Value::Null, decimal_cell(0, 32, 2, 128), decimal_cell(99_999_999, 32, 2, 128), decimal_cell(-99_999_999, 32, 2, 128), decimal_cell(1, 32, 2, 128), Value::Null]),
        ("temporal", vec![
            Value::DateDay(19_000),
            Value::Timestamp { value: 1_700_000_000_123_456, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) },
            Value::Timestamp { value: -1, unit: TimeUnit::Microsecond, timezone: None },
            Value::Time { value: 86_399_999_999_999, unit: TimeUnit::Nanosecond, bit_width: 64 },
            Value::Duration { value: -42, unit: TimeUnit::Microsecond },
            Value::IntervalMonthDayNano { months: 1, days: -2, nanoseconds: 3 },
            Value::Null,
            Value::Timestamp { value: 5, unit: TimeUnit::Microsecond, timezone: Some("Europe/London".into()) },
        ]),
        ("nested", vec![
            Value::list(vec![structure(vec![("a", Value::I64(1)), ("b", Value::list(vec![Value::Utf8("x".into())]))])]),
            Value::Set(vec![Value::I64(3), Value::I64(1)]),
            Value::Map { entries: vec![(Value::Utf8("k".into()), Value::list(vec![Value::F64(-0.0)]))], sorted: false },
            structure(vec![("a", Value::Null), ("c", structure(vec![("d", uuid(6))]))]),
            Value::Null,
            Value::list(vec![]),
            Value::list(vec![Value::Null, Value::Set(vec![])]),
            structure(vec![]),
        ]),
        ("scalars", vec![Value::I8(-8), Value::I16(-16), Value::I32(i32::MIN), Value::Binary(vec![0, 0xff]), Value::Utf8(String::new()), Value::Uri("https://example.com/a?b".into()), Value::Bool(false), Value::Null]),
    ]
}

fn spill_payload(id_column: u32, id_name: &str, ids: &[i64]) -> Arc<Relation> {
    let columns = spill_payload_columns();
    let mut fields = vec![field(id_column, id_name, scalar(ScalarType::I64))];
    fields.extend(columns.iter().enumerate().map(|(index, (name, cells))| {
        field(id_column + 1 + index as u32, &format!("{id_name}_{name}"), LogicalType::join_all(&cells.iter().map(Value::logical_type).collect::<Vec<_>>()))
    }));
    let rows = ids.iter().map(|&id| {
        let row = usize::try_from(id).unwrap() % columns[0].1.len();
        std::iter::once(Expr::Literal(Value::I64(id))).chain(columns.iter().map(|(_, cells)| Expr::Literal(cells[row].clone()))).collect()
    }).collect();
    Arc::new(Relation::new(Schema::new(fields).unwrap(), RelationNode::Values { rows }).unwrap())
}

fn bit_exact(rows: &[Vec<Value>]) -> serde_json::Value { serde_json::to_value(rows).unwrap() }

#[tokio::test]
async fn every_value_kind_survives_spilled_sort_and_join_unchanged() {
    let ids = [5, 2, 7, 0, 3, 6, 1, 4, 13, 8, 11, 10, 9, 15, 14, 12];
    let input = spill_payload(0, "l", &ids);
    let key = input.schema().fields()[0].expression();
    let sorted = Arc::new(Arc::clone(&input).sort(vec![SortExpr { expr: key.clone(), descending: false, nulls_first: false }]).unwrap());
    let in_memory = query_context(QueryOptions::default());
    let expected = collect(Arc::clone(&sorted), Arc::clone(&in_memory)).await.unwrap();
    assert_eq!(in_memory.spill_peak_bytes(), 0);
    assert_eq!(expected.iter().map(|row| row[0].clone()).collect::<Vec<_>>(), (0..16).map(Value::I64).collect::<Vec<_>>());
    let spilled = query_context(QueryOptions { sort_memory_bytes: 1, batch_rows: 3, ..QueryOptions::default() });
    let actual = collect(sorted, Arc::clone(&spilled)).await.unwrap();
    assert!(spilled.spill_peak_bytes() > 0);
    assert_eq!(bit_exact(&actual), bit_exact(&expected));
    assert_eq!(spilled.spill_bytes(), 0);
    assert_eq!(spilled.memory.used_bytes(), 0);

    let right = spill_payload(100, "r", &ids[..12]);
    let condition = Expr::binary(BinaryOp::Eq, key, right.schema().fields()[0].expression());
    let joined = join(input, right, JoinKind::Inner, Some(condition));
    let by_key = |mut rows: Vec<Vec<Value>>| { rows.sort_by_key(|row| match row[0] { Value::I64(id) => id, _ => panic!("Expected I64 join key") }); rows };
    let in_memory = query_context(QueryOptions { join_build_rows: i64::MAX, ..QueryOptions::default() });
    let expected = by_key(collect(Arc::clone(&joined), Arc::clone(&in_memory)).await.unwrap());
    assert_eq!(in_memory.spill_peak_bytes(), 0);
    assert_eq!(expected.len(), 12);
    assert!(expected.iter().all(|row| bit_exact(&[row[1..10].to_vec()]) == bit_exact(&[row[11..].to_vec()])));
    let spilled = query_context(QueryOptions { join_build_rows: 0, batch_rows: 3, ..QueryOptions::default() });
    let actual = by_key(collect(joined, Arc::clone(&spilled)).await.unwrap());
    assert!(spilled.spill_peak_bytes() > 0);
    assert_eq!(bit_exact(&actual), bit_exact(&expected));
    assert_eq!(spilled.spill_bytes(), 0);
    assert_eq!(spilled.memory.used_bytes(), 0);
}
