use std::collections::BTreeMap;
use std::sync::Arc;
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{AggregateExpr, AggregateFunction, ColumnId, Expr, Field, NamedExpr, Relation, RelationNode, Schema, SetOperator, SortExpr, WindowExpr, WindowFunction};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, Result, ScalarType, TimeUnit, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn timestamp(value: i64, zoned: bool) -> Value { Value::Timestamp { value, unit: TimeUnit::Second, timezone: zoned.then(|| "UTC".into()) } }
fn field(id: u32, name: &str, data_type: LogicalType) -> Field { Field { id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type } }
fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 24), Timestamp(0), "+02:00",
        QueryOptions { batch_rows: 1, sort_memory_bytes: 1, ..Default::default() }, &CancellationToken::new()).unwrap()
}
fn values(rows: Vec<Value>) -> Arc<Relation> {
    let data_type = LogicalType::join_all(&rows.iter().map(Value::logical_type).collect::<Vec<_>>());
    Arc::new(Relation::new(Schema::new(vec![field(0, "v", data_type)]).unwrap(), RelationNode::Values {
        rows: rows.into_iter().map(|value| vec![Expr::Literal(value)]).collect(),
    }).unwrap())
}
async fn collect(relation: Arc<Relation>, context: &Arc<QueryContext>) -> Result<Vec<Vec<Value>>> {
    let mut cursor = PreparedQuery::new(relation)?.execute(Arc::clone(context), vec![]).await?;
    let mut result = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() {
            result.push(batch.batch().schema().fields().iter().enumerate().map(|(index, field)| read_value(field, batch.batch().column(index).as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    Ok(result)
}

#[tokio::test]
async fn spilled_sort_uses_session_order_and_extrema_reject_mixed_timestamp_vectors() {
    let context = context();
    let input = values(vec![timestamp(0, true), timestamp(0, false), timestamp(10_800, false)]);
    let column = input.schema().fields()[0].expression();
    let sort = Arc::new(Relation::new(input.schema().clone(), RelationNode::Sort { input: Arc::clone(&input), order_by: vec![SortExpr { expr: column.clone(), descending: false, nulls_first: false }] }).unwrap());
    assert_eq!(collect(sort, &context).await.unwrap(), vec![vec![timestamp(0, false)], vec![timestamp(0, true)], vec![timestamp(10_800, false)]]);
    let aggregates = [AggregateFunction::Min, AggregateFunction::Max].into_iter().enumerate().map(|(index, function)| AggregateExpr {
        field: field(1 + index as u32, "extreme", input.schema().fields()[0].data_type.clone().maybe()), function,
        argument: Some(column.clone()), distinct: false, filter: None, order_by: vec![],
    }).collect::<Vec<_>>();
    let schema = Schema::new(aggregates.iter().map(|aggregate| aggregate.field.clone()).collect()).unwrap();
    let input_type = input.schema().fields()[0].data_type.clone();
    let aggregate = Arc::new(Relation::new(schema, RelationNode::Aggregate { input, groups: vec![], aggregates }).unwrap());
    let error = collect(aggregate, &context).await.unwrap_err();
    assert_eq!(error.category, rtdb_types::AnomalyCategory::Incorrect);
    assert_eq!(error.code, "xtdb.group-by/incomparable-min-max-types");
    let from_type = format!("#xt/type {}", input_type.to_edn().unwrap());
    assert_eq!(error.message.as_deref(), Some(format!("Incomparable types in min/max aggregate: {from_type}").as_str()));
    let json = serde_json::to_value(rtdb_format::diagnostics::JsonAnomaly::from(&error)).unwrap();
    assert_eq!(json["data"]["from-type"], from_type);
}

#[tokio::test]
async fn spilled_temporal_distinct_grouping_and_intersection_keep_their_key_rules() {
    let context = context();
    let input = values(vec![timestamp(0, false), timestamp(-7_200, true), timestamp(1, false)]);
    let distinct = Arc::new(Relation::new(input.schema().clone(), RelationNode::Distinct { input: Arc::clone(&input) }).unwrap());
    assert_eq!(collect(distinct, &context).await.unwrap(), vec![vec![timestamp(0, false)], vec![timestamp(1, false)]]);
    let group = NamedExpr { field: field(1, "group", input.schema().fields()[0].data_type.clone()), expr: input.schema().fields()[0].expression() };
    let count = AggregateExpr { field: field(2, "n", LogicalType::Scalar(ScalarType::I64)), function: AggregateFunction::CountStar,
        argument: None, distinct: false, filter: None, order_by: vec![] };
    let schema = Schema::new(vec![group.field.clone(), count.field.clone()]).unwrap();
    let grouped = Arc::new(Relation::new(schema, RelationNode::Aggregate { input: Arc::clone(&input), groups: vec![group], aggregates: vec![count] }).unwrap());
    assert_eq!(collect(grouped, &context).await.unwrap(), vec![vec![timestamp(0, false), Value::I64(1)], vec![timestamp(-7_200, true), Value::I64(1)], vec![timestamp(1, false), Value::I64(1)]]);
    let right = values(vec![timestamp(-7_200, true)]);
    let intersection = Arc::new(Relation::new(input.schema().clone(), RelationNode::Set { left: input, right, operator: SetOperator::Intersect, all: true }).unwrap());
    assert_eq!(collect(intersection, &context).await.unwrap(), vec![vec![timestamp(0, false)]]);
}

#[tokio::test]
async fn window_partitions_preserve_distinct_temporal_vector_hashes() {
    let context = context();
    let input = values(vec![Value::DateDay(0), timestamp(-7_200, true), Value::DateMilli(86_400_000)]);
    let output = field(1, "rn", LogicalType::Scalar(ScalarType::I64));
    let schema = Schema::new(input.schema().fields().iter().cloned().chain([output.clone()]).collect()).unwrap();
    let window = Arc::new(Relation::new(schema, RelationNode::Window { input, partition_by: vec![ColumnId(0)], order_by: vec![],
        expressions: vec![WindowExpr { field: output, function: WindowFunction::RowNumber }] }).unwrap());
    let rows = collect(window, &context).await.unwrap();
    assert_eq!(rows.iter().map(|row| row[1].clone()).collect::<Vec<_>>(), [Value::I64(1), Value::I64(1), Value::I64(1)]);
}

#[tokio::test]
async fn temporal_set_probes_preserve_each_left_type_and_multiplicity() {
    let context = context();
    let local = timestamp(0, false);
    let zoned = timestamp(-7_200, true);
    let other = timestamp(1, false);
    let left = values(vec![local.clone(), zoned.clone(), local.clone(), Value::Null, Value::Null, other.clone()]);
    let right = values(vec![zoned.clone(), Value::Null]);

    for (operator, all, expected) in [
        (SetOperator::Intersect, false, vec![local.clone(), Value::Null]),
        (SetOperator::Except, false, vec![other.clone()]),
        (SetOperator::Intersect, true, vec![local.clone(), Value::Null]),
        (SetOperator::Except, true, vec![zoned, local, Value::Null, other]),
    ] {
        let set = Arc::new(Relation::new(left.schema().clone(), RelationNode::Set {
            left: Arc::clone(&left), right: Arc::clone(&right), operator, all,
        }).unwrap());
        let expected: Vec<_> = expected.into_iter().map(|value| vec![value]).collect();
        assert_eq!(collect(set, &context).await.unwrap(), expected);
    }
    assert_eq!(context.memory.used_bytes(), 0);
}
