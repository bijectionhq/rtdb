use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::{PartitionState, TransactionBuffer};
use rtdb_plan::{ApplyKind, Relation, RelationNode, Statement};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{AnomalyCategory, Interval, Result, TableRef, Timestamp, TransactionKey, TxId, Value};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let memory = MemoryBudget::new(128 << 20);
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut transaction = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) }, memory.clone()).unwrap();
    for (table, rows) in [
        ("x", vec![vec![("_id", Value::I64(1)), ("x", Value::I64(1)), ("y", Value::I64(1))]]),
        ("z", vec![vec![("_id", Value::I64(1)), ("x", Value::I64(1)), ("z", Value::I64(1))]]),
        ("y", vec![vec![("_id", Value::I64(1)), ("b", Value::Utf8("one".into()))]]),
        ("nested_table", vec![vec![("_id", Value::I64(1)), ("nest", Value::List(vec![Value::I64(1), Value::I64(2), Value::I64(3)]))]]),
        ("other_table", (1..=3).map(|id| vec![("_id", Value::I64(id))]).collect()),
    ] {
        for row in rows { transaction.put(TableRef::new("public", table), row.into_iter().map(|(name, value)| (name.into(), value)).collect(), Interval::ALL).unwrap(); }
    }
    state.apply(&transaction.stage().unwrap()).unwrap();
    let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, memory, Timestamp(100), "UTC", QueryOptions { batch_rows: 1, ..Default::default() }, &CancellationToken::new()).unwrap()
}

async fn query(sql: &str, parameters: Vec<Value>) -> Result<Vec<Vec<Value>>> {
    let context = context();
    let options = PrepareOptions { parameter_types: parameters.iter().enumerate().map(|(index, value)| (index, value.logical_type())).collect(), ..Default::default() };
    let prepared = prepare(sql, &context.catalog, &options)?;
    let Statement::Query(relation) = prepared.statement else { panic!("Expected query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(context, parameters).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() {
            rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field, column)| read_value(field, column.as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    Ok(rows)
}

#[tokio::test]
async fn lateral_derived_table_and_join_lateral_preserve_original_query_forms() {
    for sql in [
        "SELECT x.y, y.z FROM x, LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y",
        "SELECT x.y, y.z FROM x CROSS JOIN LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y",
        "SELECT x.y, y.z FROM x LEFT JOIN LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y ON TRUE",
    ] { assert_eq!(query(sql, vec![]).await.unwrap(), vec![vec![Value::I64(1), Value::I64(1)]], "{sql}"); }
    assert_eq!(query("SELECT y.z FROM LATERAL (SELECT z.z FROM z WHERE z.z = 1) AS y", vec![]).await.unwrap(), vec![vec![Value::I64(1)]]);
    assert_eq!(query("SELECT x, x.y, y.z FROM x NATURAL JOIN LATERAL (SELECT x, z.z FROM z WHERE z.z = x.y) AS y", vec![]).await.unwrap(), vec![vec![Value::I64(1), Value::I64(1), Value::I64(1)]]);
    assert_eq!(query("SELECT nt._id nid, o._id oid FROM nested_table nt CROSS JOIN LATERAL (SELECT * FROM other_table o WHERE o._id = nt._id) o", vec![]).await.unwrap(), vec![vec![Value::I64(1), Value::I64(1)]]);
}

#[tokio::test]
async fn select_star_lateral_join_and_lateral_with_unnest_4009_keep_aliases_and_arrays() {
    assert_eq!(query("SELECT * FROM (SELECT y.b FROM y) AS x (x_b), LATERAL (SELECT y.b AS z_b, x_b FROM y) AS z (z_b, outer_b)", vec![]).await.unwrap(), vec![vec![Value::Utf8("one".into()), Value::Utf8("one".into()), Value::Utf8("one".into())]]);
    let array = Value::List(vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
    assert_eq!(query("SELECT * FROM nested_table nt, LATERAL (SELECT nt.nest) t(a)", vec![]).await.unwrap(), vec![vec![Value::I64(1), array.clone(), array.clone()]]);
    assert_eq!(query("SELECT * FROM nested_table nt, LATERAL (FROM UNNEST(nt.nest) AS foo(id)) t(a)", vec![]).await.unwrap(), (1..=3).map(|id| vec![Value::I64(1), array.clone(), Value::I64(id)]).collect::<Vec<_>>());
    assert_eq!(query("SELECT nt._id, value, ordinal FROM nested_table nt, UNNEST(nt.nest) WITH ORDINALITY AS items(value, ordinal)", vec![]).await.unwrap(), (1..=3).map(|id| vec![Value::I64(1), Value::I64(id), Value::I64(id)]).collect::<Vec<_>>());
}

#[tokio::test]
async fn left_lateral_filters_before_null_extension_and_preserves_many_rows() {
    for suffix in ["WHERE y = l.x) AS r ON TRUE", ") AS r ON r.y = l.x", ") AS r ON EXISTS (SELECT 1 WHERE r.y = l.x)"] {
        let sql = format!("SELECT l.x, r.y FROM (VALUES (1), (2)) AS l(x) LEFT JOIN LATERAL (SELECT y FROM (VALUES (2), (2), (3)) AS t(y) WHERE l.x > 0 {}", suffix.replace("WHERE y", "AND y"));
        assert_eq!(query(&sql, vec![]).await.unwrap(), vec![vec![Value::I64(1), Value::Null], vec![Value::I64(2), Value::I64(2)], vec![Value::I64(2), Value::I64(2)]], "{sql}");
    }
    assert_eq!(query("SELECT l.x, r.y FROM (VALUES (1), (2)) AS l(x) LEFT JOIN LATERAL (SELECT l.x AS y WHERE FALSE) AS r ON TRUE", vec![]).await.unwrap(), vec![vec![Value::I64(1), Value::Null], vec![Value::I64(2), Value::Null]]);
    assert_eq!(query("SELECT l.x, r.y FROM (VALUES (1), (2)) AS l(x) CROSS JOIN LATERAL (SELECT l.x AS y WHERE FALSE) AS r", vec![]).await.unwrap(), Vec::<Vec<Value>>::new());
}

#[tokio::test]
async fn named_lateral_joins_keep_common_columns_and_qualified_copies() {
    for join in ["JOIN", "LEFT JOIN", "NATURAL JOIN", "NATURAL LEFT JOIN"] {
        let condition = if join.starts_with("NATURAL") { "" } else { " USING (x)" };
        let sql = format!("SELECT x AS common_x, l.x AS left_x, r.x AS right_x FROM (VALUES (1), (2)) AS l(x) {join} LATERAL (SELECT l.x AS x WHERE l.x = 2) AS r{condition}");
        let mut expected = vec![vec![Value::I64(2), Value::I64(2), Value::I64(2)]];
        if join.contains("LEFT") { expected.insert(0, vec![Value::I64(1), Value::I64(1), Value::Null]); }
        assert_eq!(query(&sql, vec![]).await.unwrap(), expected, "{sql}");
    }
}

#[tokio::test]
async fn lateral_scopes_follow_from_order_and_remain_lexical_across_nested_queries() {
    for sql in [
        "SELECT c.z FROM (VALUES (2)) AS a(x), LATERAL (SELECT a.x + 1 AS y) AS b, LATERAL (SELECT a.x + b.y AS z) AS c",
        "SELECT c.z FROM (VALUES (2)) AS a(x), (VALUES (3)) AS b(y) JOIN LATERAL (SELECT a.x + b.y AS z) AS c ON TRUE",
        "SELECT c.z FROM (VALUES (2)) AS a(x), LATERAL (WITH v AS (SELECT a.x AS x) SELECT (SELECT v.x + 3) AS z FROM v) AS c",
        "SELECT (SELECT r.z FROM LATERAL (SELECT a.x + 3 AS z) AS r) FROM (VALUES (2)) AS a(x)",
    ] { assert_eq!(query(sql, vec![]).await.unwrap(), vec![vec![Value::I64(5)]], "{sql}"); }
    assert_eq!(query("SELECT r.z FROM (VALUES (2)) AS a(x), LATERAL (SELECT t.x AS z FROM (VALUES (9)) AS t(x)) AS r", vec![]).await.unwrap(), vec![vec![Value::I64(9)]]);
    for sql in [
        "SELECT * FROM (VALUES (1)) AS a(x), (SELECT a.x) AS r",
        "SELECT * FROM (VALUES (1)) AS a(x), (SELECT * FROM LATERAL (SELECT a.x) AS inner_r) AS r",
        "SELECT * FROM LATERAL (SELECT later.x) AS r, (VALUES (1)) AS later(x)",
        "SELECT * FROM (VALUES (1)) AS a(x), (VALUES (2)) AS b(y) CROSS JOIN LATERAL (SELECT a.x) AS r",
        "SELECT (SELECT r.z FROM (VALUES (3)) AS b(y) CROSS JOIN LATERAL (SELECT a.x + b.y AS z) AS r) FROM (VALUES (2)) AS a(x)",
    ] { assert!(query(sql, vec![]).await.is_err(), "{sql}"); }
}

#[tokio::test]
async fn lateral_captures_keep_external_parameter_arity_and_empty_input_short_circuit() {
    let sql = "SELECT a.x, b.y FROM (VALUES (?)) AS a(x) CROSS JOIN LATERAL (SELECT ? + a.x AS y) AS b";
    assert_eq!(query(sql, vec![Value::I64(2), Value::I64(10)]).await.unwrap(), vec![vec![Value::I64(2), Value::I64(12)]]);
    let context = context();
    let options = PrepareOptions { parameter_types: BTreeMap::from([(0, Value::I64(0).logical_type()), (1, Value::I64(0).logical_type())]), ..Default::default() };
    let prepared = prepare(sql, &context.catalog, &options).unwrap();
    assert_eq!(prepared.parameter_types.len(), 2);
    let Statement::Query(relation) = prepared.statement else { panic!("Expected query") };
    assert_eq!(relation.parameter_count(), 2);
    fn contains_apply(relation: &Relation) -> bool { matches!(relation.node(), RelationNode::Apply { kind: ApplyKind::Inner, .. }) || relation.children().iter().any(|child| contains_apply(child)) }
    assert!(contains_apply(&relation));
    assert_eq!(query("SELECT r.y FROM (SELECT 1 AS x WHERE FALSE) AS l CROSS JOIN LATERAL (SELECT l.x / 0 AS y) AS r", vec![]).await.unwrap(), Vec::<Vec<Value>>::new());
}

#[tokio::test]
async fn lateral_cursor_cancellation_releases_dependent_state_but_retains_returned_batches() {
    let context = context();
    let baseline = context.memory.used_bytes();
    let Statement::Query(relation) = prepare("SELECT r.y FROM (VALUES (1), (2)) AS l(x) CROSS JOIN LATERAL (SELECT l.x + y AS y FROM (VALUES (10), (20)) AS t(y)) AS r", &context.catalog, &PrepareOptions::default()).unwrap().statement else { panic!("Expected query") };
    let prepared = PreparedQuery::new(relation).unwrap();
    let mut cursor = prepared.execute(context.clone(), vec![]).await.unwrap();
    let batch = cursor.next_batch().await.unwrap().unwrap();
    let retained = batch.batch().clone();
    drop(batch);
    cursor.cancel();
    assert_eq!(cursor.next_batch().await.unwrap_err().category, AnomalyCategory::Interrupted);
    drop(cursor);
    assert!(context.memory.used_bytes() > baseline);
    assert_eq!(read_value(retained.schema().field(0), retained.column(0).as_ref(), 0).unwrap(), Value::I64(11));
    drop(retained);
    assert_eq!(context.memory.used_bytes(), baseline);
    let mut next = prepared.execute(context.clone(), vec![]).await.unwrap();
    assert!(next.next_batch().await.unwrap().is_some());
}

#[tokio::test]
async fn uncorrelated_lateral_right_and_full_joins_keep_ordinary_outer_join_results() {
    assert_eq!(query("SELECT l.x, r.y FROM (VALUES (1)) AS l(x) RIGHT JOIN LATERAL (SELECT 2 AS y) AS r ON FALSE", vec![]).await.unwrap(), vec![vec![Value::Null, Value::I64(2)]]);
    let rows = query("SELECT l.x, r.y FROM (VALUES (1)) AS l(x) FULL JOIN LATERAL (SELECT 2 AS y) AS r ON FALSE", vec![]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&vec![Value::I64(1), Value::Null]));
    assert!(rows.contains(&vec![Value::Null, Value::I64(2)]));
    for join in ["RIGHT JOIN", "FULL JOIN"] {
        assert!(query(&format!("SELECT * FROM (VALUES (1)) AS l(x) {join} LATERAL (SELECT l.x AS y) AS r ON TRUE"), vec![]).await.is_err());
    }
    assert!(query("SELECT * FROM LATERAL (SELECT 1)", vec![]).await.is_err());
}

#[test]
fn recursive_ctes_retain_the_reference_unsupported_anomaly() {
    let context = context();
    for sql in ["WITH RECURSIVE c AS (SELECT 1 AS x) SELECT * FROM c", "WITH RECURSIVE c AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM c WHERE x < 3) SELECT * FROM c"] {
        let error = prepare(sql, &context.catalog, &PrepareOptions::default()).unwrap_err();
        assert_eq!(error.category, AnomalyCategory::Unsupported);
        assert_eq!(error.code, "xtdb.sql/recursive-ctes");
        assert_eq!(error.message.as_deref(), Some("Recursive CTEs are not supported yet"));
    }
}
