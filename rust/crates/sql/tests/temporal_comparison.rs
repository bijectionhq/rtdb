use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Result, Timestamp, Value};
use tokio_util::sync::CancellationToken;

async fn query(sql: &str, timezone: &str) -> Result<Vec<Value>> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false)?;
    let context = QueryContext::new(catalog, MemoryBudget::new(1 << 24), Timestamp(0), timezone, QueryOptions::default(), &CancellationToken::new())?;
    let Statement::Query(relation) = prepare(sql, &context.catalog, &PrepareOptions::default())?.statement else { panic!("expected query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(context, vec![]).await?;
    let batch = cursor.next_batch().await?.unwrap();
    assert_eq!(batch.batch().num_rows(), 1);
    let row = batch.batch().schema().fields().iter().enumerate().map(|(index, field)| read_value(field, batch.batch().column(index).as_ref(), 0)).collect::<Result<_>>()?;
    assert!(cursor.next_batch().await?.is_none());
    Ok(row)
}

#[tokio::test]
async fn sql_temporal_predicates_and_nullif_borrow_the_session_zone() {
    assert_eq!(query("SELECT DATE '1970-01-01' = TIMESTAMP WITH TIME ZONE '1969-12-31 22:00:00+00:00', TIMESTAMP '1970-01-01 00:00:00' < TIMESTAMP WITH TIME ZONE '1970-01-01 00:00:00+00:00', NULLIF(DATE '1970-01-01', TIMESTAMP WITH TIME ZONE '1969-12-31 22:00:00+00:00')", "+02:00").await.unwrap(),
        [Value::Bool(true), Value::Bool(true), Value::Null]);
}

#[tokio::test]
async fn short_sql_in_lists_keep_literal_casts_and_larger_lists_keep_row_values() {
    for count in [1, 8, 9] {
        let items = vec!["'1970-01-01'"; count].join(",");
        assert_eq!(query(&format!("SELECT DATE '1970-01-01' IN ({items}), DATE '1970-01-01' NOT IN ({items})"), "UTC").await.unwrap(),
            [Value::Bool(count <= 8), Value::Bool(count > 8)]);
    }
    assert_eq!(query("SELECT DATE '1970-01-01' = '1970-01-01', '1970-01-01' <> DATE '1970-01-01'", "UTC").await.unwrap(), [Value::Bool(true), Value::Bool(false)]);
    assert_eq!(query("SELECT NULLIF(DATE '1970-01-01', '1970-01-01'), NULLIF('1970-01-01', DATE '1970-01-01') AS test_output_2", "UTC").await.unwrap(), [Value::Null, Value::Utf8("1970-01-01".into())]);
}

#[tokio::test]
async fn greatest_and_least_reject_empty_calls_and_keep_null_short_circuit_results() {
    for source in ["SELECT GREATEST()", "SELECT LEAST()"] { assert!(query(source, "UTC").await.is_err()); }
    assert_eq!(query("SELECT GREATEST(NULL, 1 / 0), LEAST(NULL, 1 / 0)", "UTC").await.unwrap(), [Value::Null, Value::Null]);
}
