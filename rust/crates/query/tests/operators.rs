use std::collections::BTreeMap;
use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{AggregateExpr, AggregateFunction, ApplyKind, BinaryOp, ColumnId, Expr, Field, JoinKind, NamedExpr, Relation, RelationNode, Schema, SetOperator, SortExpr};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, Result, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn scalar(ty: ScalarType) -> LogicalType { LogicalType::Scalar(ty) }
fn field(id: u32, name: &str, ty: LogicalType) -> Field { Field { id: ColumnId(id), name: name.into(), qualifier: None, data_type: ty } }
fn query_context(options: QueryOptions) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 24)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(100), "UTC", options, &CancellationToken::new()).unwrap()
}
fn values(id: u32, name: &str, ty: LogicalType, rows: Vec<Value>) -> Arc<Relation> {
    Arc::new(Relation::new(Schema::new(vec![field(id, name, ty)]).unwrap(), RelationNode::Values { rows: rows.into_iter().map(|value| vec![Expr::Literal(value)]).collect() }).unwrap())
}
fn numbers(id: u32, name: &str, rows: &[i64]) -> Arc<Relation> { values(id, name, scalar(ScalarType::I64), rows.iter().copied().map(Value::I64).collect()) }
async fn collect(relation: Arc<Relation>, context: Arc<QueryContext>) -> Result<Vec<Vec<Value>>> {
    let mut cursor = PreparedQuery::new(relation)?.execute(context, vec![])?;
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
    let mut cursor = PreparedQuery::new(limited).unwrap().execute(query_context(QueryOptions::default()), vec![Value::I64(1)]).unwrap();
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
    let options = QueryOptions { sort_memory_bytes: 1, merge_fan_in: 2, batch_rows: 2, ..QueryOptions::default() };
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
    let mut first = prepared.execute(Arc::clone(&context), vec![]).unwrap();
    let mut sibling = prepared.execute(Arc::clone(&context), vec![]).unwrap();
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
    let compound = Value::Struct(BTreeMap::from([("x".into(), Value::Null)]));
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
    assert_eq!(rows, vec![vec![Value::I64(1), Value::Null], vec![Value::I64(2), Value::I64(2)], vec![Value::I64(2), Value::I64(2)], vec![Value::Null, Value::I64(3)]]);
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
    let nested = Value::List(vec![Value::Null]);
    let ty = LogicalType::list(LogicalType::Null).maybe();
    let input = values(0, "key", ty.clone(), vec![Value::Null, Value::Null, nested.clone(), nested.clone()]);
    let group = NamedExpr { field: field(1, "key", ty), expr: input.schema().fields()[0].expression() };
    let count = agg(2, AggregateFunction::CountStar, None, scalar(ScalarType::I64));
    assert_eq!(collect(aggregate(input, vec![group], vec![count]), query_context(QueryOptions::default())).await.unwrap(), vec![vec![Value::Null, Value::I64(2)], vec![nested.clone(), Value::I64(1)], vec![nested, Value::I64(1)]]);
}

#[tokio::test]
async fn array_aggregate_orders_selected_rows_and_distinct_retains_null_arguments() {
    let input = values(0, "v", scalar(ScalarType::I64).maybe(), vec![Value::I64(2), Value::Null, Value::I64(1), Value::I64(2), Value::Null]);
    let argument = input.schema().fields()[0].expression();
    let mut array = agg(1, AggregateFunction::ArrayAgg, Some(argument.clone()), LogicalType::list(scalar(ScalarType::I64).maybe()).maybe());
    array.distinct = true;
    array.order_by.push(SortExpr { expr: argument, descending: false, nulls_first: true });
    assert_eq!(collect(aggregate(input, vec![], vec![array]), query_context(QueryOptions { sort_memory_bytes: 1, merge_fan_in: 2, ..QueryOptions::default() })).await.unwrap(), vec![vec![Value::List(vec![Value::Null, Value::Null, Value::I64(1), Value::I64(2)])]]);
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

#[tokio::test]
async fn unnest_keeps_list_order_and_outer_ordinality_is_null() {
    let ty = LogicalType::list(scalar(ScalarType::I64));
    let input = values(0, "xs", ty, vec![Value::List(vec![Value::I64(4), Value::I64(5)]), Value::List(vec![])]);
    let expr = input.schema().fields()[0].expression();
    let element = field(1, "x", scalar(ScalarType::I64).maybe());
    let ordinal = field(2, "n", scalar(ScalarType::I32).maybe());
    let schema = input.schema().concat(&Schema::new(vec![element.clone(), ordinal.clone()]).unwrap()).unwrap();
    let plan = Arc::new(Relation::new(schema, RelationNode::Unnest { input, expr, element, ordinality: Some(ordinal), outer: true }).unwrap());
    let rows = collect(plan, query_context(QueryOptions::default())).await.unwrap();
    assert_eq!(rows.iter().map(|row| row[1..].to_vec()).collect::<Vec<_>>(), vec![vec![Value::I64(4), Value::I32(1)], vec![Value::I64(5), Value::I32(2)], vec![Value::Null, Value::Null]]);
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
async fn scalar_apply_rejects_multiple_rows_and_pull_many_distinguishes_empty_document() {
    let input = Arc::new(Relation::unit());
    let dependent = numbers(1, "v", &[1, 2]);
    let output = field(2, "answer", scalar(ScalarType::I64).maybe());
    let plan = Arc::new(Relation::new(Schema::new(vec![output.clone()]).unwrap(), RelationNode::Apply { input: Arc::clone(&input), dependent, arguments: vec![], kind: ApplyKind::Scalar, output: Some(output) }).unwrap());
    assert_eq!(collect(plan, query_context(QueryOptions::default())).await.unwrap_err().code, "rtdb.query/cardinality-violation");
    for (dependent, expected) in [(Arc::new(Relation::empty(Schema::default())), Value::Null), (Arc::new(Relation::unit()), Value::List(vec![Value::Struct(BTreeMap::new())]))] {
        let output = field(2, "answer", LogicalType::list(LogicalType::Struct(BTreeMap::new())).maybe());
        let plan = Arc::new(Relation::new(Schema::new(vec![output.clone()]).unwrap(), RelationNode::Apply { input: Arc::clone(&input), dependent, arguments: vec![], kind: ApplyKind::PullMany, output: Some(output) }).unwrap());
        assert_eq!(collect(plan, query_context(QueryOptions::default())).await.unwrap(), vec![vec![expected]]);
    }
}
