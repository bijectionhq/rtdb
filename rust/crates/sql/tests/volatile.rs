use std::{collections::BTreeMap, sync::Arc, time::Duration};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Result, Timestamp, Value};
use tokio_util::sync::CancellationToken;

async fn query(source: &str) -> Result<Vec<Vec<Value>>> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false)?;
    let context = QueryContext::new(catalog, MemoryBudget::new(1 << 25), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new())?;
    let prepared = PreparedQuery::from_prepared(prepare(source, &context.catalog, &PrepareOptions::default())?, &context.memory)?;
    let mut cursor = prepared.execute(context, vec![]).await?;
    let mut rows = Vec::new();
    while let Some(page) = cursor.next_batch().await? {
        for row in 0..page.batch().num_rows() {
            rows.push(page.batch().schema().fields().iter().zip(page.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    Ok(rows)
}

#[tokio::test(start_paused = true)]
async fn pg2_sleep_functions_cast_seconds_and_periods_to_milliseconds() {
    for (sql, millis) in [
        ("SELECT PG_SLEEP(0.1)", 100),
        ("SELECT PG_SLEEP(0.1 + 0.1)", 200),
        ("SELECT PG_SLEEP_FOR('0.100 second')", 100),
        ("SELECT PG_SLEEP_FOR('0.01 minute')", 600),
    ] {
        let start = tokio::time::Instant::now();
        assert_eq!(query(sql).await.unwrap(), [vec![Value::Null]], "{sql}");
        assert_eq!(tokio::time::Instant::now() - start, Duration::from_millis(millis), "{sql}");
    }
}

#[tokio::test(start_paused = true)]
async fn sql_sleep_short_circuit_and_unevaluated_settings_do_not_wait() {
    let start = tokio::time::Instant::now();
    assert_eq!(query("SELECT CASE WHEN TRUE THEN 7 ELSE PG_SLEEP(60.0) END, COALESCE(9, PG_SLEEP(60.0)), PG_SLEEP(NULL)").await.unwrap(), [vec![Value::I64(7), Value::I64(9), Value::Null]]);
    assert_eq!(query("SETTING SNAPSHOT_TIME=PG_SLEEP(60.0) SELECT 1").await.unwrap_err().code, "rtdb.query/invalid-setting-value");
    assert_eq!(tokio::time::Instant::now(), start);
    assert_eq!(query("SELECT PG_SLEEP(-0.1)").await.unwrap_err().message.as_deref(), Some("timeout value is negative"));
    assert!(query("SELECT PG_SLEEP(1)").await.is_err());
}

#[tokio::test]
async fn sql_rejects_expression_only_random_calls() {
    for expression in ["RANDOM()", "custom.random()", r#""random"()"#] {
        assert!(query(&format!("SELECT {expression} FROM (VALUES (1), (2)) AS input(n)")).await.is_err(), "{expression}");
    }
}
