use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{AggregateExpr, AggregateFunction, ColumnId, Expr, Field, NamedExpr, PercentileFraction, PercentileKind, Relation, RelationNode, Schema, SortExpr};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn field(id: u32, name: &str, ty: LogicalType) -> Field { Field { id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty } }
fn context(spill_limit_bytes: u64) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 25), Timestamp(0), "UTC", QueryOptions { batch_rows: 1, sort_memory_bytes: 128, spill_limit_bytes, ..Default::default() }, &CancellationToken::new()).unwrap()
}
fn percentile(input: Arc<Relation>, kind: PercentileKind, fraction: PercentileFraction, descending: bool, grouped: bool) -> Arc<Relation> {
    let argument = input.schema().fields().last().unwrap().expression();
    let ty = if kind == PercentileKind::Continuous { LogicalType::Scalar(ScalarType::F64) } else { argument.data_type() }.maybe();
    let aggregate = AggregateExpr { field: field(10, "p", ty), function: AggregateFunction::Percentile { kind, fraction, descending }, argument: Some(argument), distinct: false, filter: None, order_by: Vec::new() };
    let groups = if grouped { vec![NamedExpr { field: field(11, "g", input.schema().fields()[0].data_type.clone()), expr: input.schema().fields()[0].expression() }] } else { vec![] };
    let schema = Schema::new(groups.iter().map(|group| group.field.clone()).chain([aggregate.field.clone()]).collect()).unwrap();
    Arc::new(Relation::new(schema, RelationNode::Aggregate { input, groups, aggregates: vec![aggregate] }).unwrap())
}
fn integers(count: i64) -> Arc<Relation> {
    Arc::new(Relation::new(Schema::new(vec![field(0, "n", LogicalType::Scalar(ScalarType::I64))]).unwrap(), RelationNode::Values {
        rows: (1..=count).rev().map(|value| vec![Expr::Literal(Value::I64(value))]).collect(),
    }).unwrap())
}

#[tokio::test]
async fn untyped_sort_parameters_validate_populated_values_even_for_singletons() {
    for count in [0, 1] {
        let input = integers(count);
        let relation = Arc::new(Relation::new(input.schema().clone(), RelationNode::Sort { input, order_by: vec![SortExpr { expr: Expr::Parameter { index: 0, data_type: LogicalType::Nothing }, descending: false, nulls_first: false }] }).unwrap());
        let mut cursor = PreparedQuery::new(relation).unwrap().execute(context(1 << 20), vec![Value::list(vec![Value::I64(1)])]).await.unwrap();
        if count == 0 { assert!(cursor.next_batch().await.unwrap().is_none()); }
        else { assert_eq!(cursor.next_batch().await.err().unwrap().code, "xtdb.expression/function-type-mismatch"); }
    }
}

#[tokio::test]
async fn secondary_unknown_sort_keys_validate_union_types_across_spill_runs() {
    let fields = vec![field(0, "n", LogicalType::Scalar(ScalarType::I64)), field(1, "v", LogicalType::Nothing)];
    let input = Arc::new(Relation::new(Schema::new(fields.clone()).unwrap(), RelationNode::Values { rows: vec![
        vec![Expr::Literal(Value::I64(1)), Expr::Parameter { index: 0, data_type: LogicalType::Nothing }],
        vec![Expr::Literal(Value::I64(2)), Expr::Parameter { index: 1, data_type: LogicalType::Nothing }],
    ] }).unwrap());
    let sorted = Arc::new(Relation::new(input.schema().clone(), RelationNode::Sort { input, order_by: fields.iter().map(|field| SortExpr { expr: field.expression(), descending: false, nulls_first: false }).collect() }).unwrap());
    let projected = Arc::new(sorted.project(vec![NamedExpr { field: fields[0].clone(), expr: fields[0].expression() }]).unwrap());
    let mut cursor = PreparedQuery::new(projected).unwrap().execute(context(1 << 20), vec![Value::I64(1), Value::Utf8("x".into())]).await.unwrap();
    assert_eq!(cursor.next_batch().await.err().unwrap().code, "xtdb.expression/function-type-mismatch");
}

#[tokio::test]
async fn sorted_percentiles_use_spilled_record_backing_and_release_every_file() {
    for (kind, expected) in [(PercentileKind::Continuous, Value::F64(256.75)), (PercentileKind::Discrete, Value::I64(256))] {
        let context = context(1 << 24);
        let plan = percentile(integers(1024), kind, PercentileFraction::Literal(0.25), false, false);
        let mut cursor = PreparedQuery::new(plan).unwrap().execute(Arc::clone(&context), vec![]).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), expected);
        assert!(context.spill_peak_bytes() > 128);
        assert!(cursor.next_batch().await.unwrap().is_none());
        drop(page); drop(cursor);
        assert_eq!(context.memory.used_bytes(), 0);
        assert_eq!(context.spill_bytes(), 0);
    }
}

#[tokio::test]
async fn trailing_null_group_survives_1024_populated_groups() {
    let input_schema = Schema::new(vec![field(0, "g", LogicalType::Scalar(ScalarType::I64)), field(1, "n", LogicalType::Scalar(ScalarType::I64).maybe())]).unwrap();
    let mut rows = (0..1024).flat_map(|group| (0..2).map(move |_| vec![Expr::Literal(Value::I64(group)), Expr::Literal(Value::I64(20))])).collect::<Vec<_>>();
    rows.push(vec![Expr::Literal(Value::I64(1024)), Expr::Literal(Value::Null)]);
    let input = Arc::new(Relation::new(input_schema, RelationNode::Values { rows }).unwrap());
    let plan = percentile(input, PercentileKind::Continuous, PercentileFraction::Literal(0.5), false, true);
    let context = context(1 << 24);
    let mut cursor = PreparedQuery::new(plan).unwrap().execute(Arc::clone(&context), vec![]).await.unwrap();
    let mut count = 0;
    while let Some(page) = cursor.next_batch().await.unwrap() {
        let group = read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap();
        let value = read_value(page.batch().schema().field(1), page.batch().column(1).as_ref(), 0).unwrap();
        assert_eq!(value, if group == Value::I64(1024) { Value::Null } else { Value::F64(20.0) });
        count += page.batch().num_rows();
    }
    assert_eq!(count, 1025);
    drop(cursor);
    assert_eq!(context.memory.used_bytes(), 0);
    assert_eq!(context.spill_bytes(), 0);
}

#[tokio::test]
async fn cancellation_between_group_pages_releases_percentile_spools() {
    let input = Arc::new(Relation::new(Schema::new(vec![field(0, "g", LogicalType::Scalar(ScalarType::I64)), field(1, "n", LogicalType::Scalar(ScalarType::I64))]).unwrap(), RelationNode::Values {
        rows: (0..40).map(|value| vec![Expr::Literal(Value::I64(value / 20)), Expr::Literal(Value::I64(value))]).collect(),
    }).unwrap());
    let context = context(1 << 20);
    let plan = percentile(input, PercentileKind::Continuous, PercentileFraction::Literal(0.5), false, true);
    let mut cursor = PreparedQuery::new(plan).unwrap().execute(Arc::clone(&context), vec![]).await.unwrap();
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(read_value(page.batch().schema().field(1), page.batch().column(1).as_ref(), 0).unwrap(), Value::F64(9.5));
    cursor.cancel();
    assert!(cursor.next_batch().await.is_err());
    drop(page); drop(cursor);
    assert_eq!(context.memory.used_bytes(), 0);
    assert_eq!(context.spill_bytes(), 0);
    assert!(!context.cancellation.is_cancelled());
}

#[tokio::test]
async fn percentile_spill_quota_failure_cleans_retained_input_and_record_backing() {
    let context = context(4096);
    let plan = percentile(integers(1024), PercentileKind::Discrete, PercentileFraction::Literal(0.5), false, false);
    let mut cursor = PreparedQuery::new(plan).unwrap().execute(Arc::clone(&context), vec![]).await.unwrap();
    assert!(cursor.next_batch().await.is_err());
    drop(cursor);
    assert_eq!(context.memory.used_bytes(), 0);
    assert_eq!(context.spill_bytes(), 0);
}

#[test]
fn shared_percentile_parameters_and_invalid_modifiers_are_validated() {
    let plan = percentile(integers(2), PercentileKind::Discrete, PercentileFraction::Parameter { index: 3 }, false, false);
    assert_eq!(plan.parameter_indices().into_iter().collect::<Vec<_>>(), [3]);
    assert_eq!(plan.parameter_count(), 4);
    let RelationNode::Aggregate { input, groups, aggregates } = plan.node() else { panic!("expected aggregate") };
    for distinct in [false, true] {
        let mut aggregate = aggregates[0].clone();
        aggregate.distinct = distinct;
        if !distinct { aggregate.order_by = vec![SortExpr { expr: input.schema().fields()[0].expression(), descending: false, nulls_first: false }]; }
        assert!(Relation::new(plan.schema().clone(), RelationNode::Aggregate { input: Arc::clone(input), groups: groups.clone(), aggregates: vec![aggregate] }).is_err());
    }
    let mut aggregate = aggregates[0].clone();
    aggregate.argument = Some(Expr::Literal(Value::I64(1)));
    assert!(Relation::new(plan.schema().clone(), RelationNode::Aggregate { input: Arc::clone(input), groups: groups.clone(), aggregates: vec![aggregate] }).is_err());
}

#[test]
fn percentile_literal_serialization_retains_nonfinite_and_signed_zero_bits() {
    for value in [0.5, -0.0, f64::INFINITY, f64::NEG_INFINITY, f64::from_bits(0x7ff8_0000_0000_0001)] {
        let bytes = serde_json::to_vec(&PercentileFraction::Literal(value)).unwrap();
        let PercentileFraction::Literal(decoded) = serde_json::from_slice::<PercentileFraction>(&bytes).unwrap() else { panic!("expected literal") };
        assert_eq!(decoded.to_bits(), value.to_bits());
    }
}
