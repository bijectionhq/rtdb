use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Result, Timestamp, Value as V};
use tokio_util::sync::CancellationToken;

async fn query(end_exclusive: bool) -> Result<Vec<Vec<V>>> {
    let database = DatabaseSnapshot { partitions: Vec::new(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false)?;
    let context = QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions { batch_rows: 1, ..Default::default() }, &CancellationToken::new())?;
    let options = PrepareOptions { generate_series_end_exclusive: end_exclusive, ..Default::default() };
    let sql = "SELECT GENERATE_SERIES(1,4) AS outer_series,t.exclusive,t.inclusive FROM (XTQL $$(-> (rel [{}] []) (return {:exclusive (generate-series 1 4 1) :inclusive (generate-series 1 4 1 true)}))$$) AS t";
    let Statement::Query(relation) = prepare(sql, &context.catalog, &options)?.statement else { panic!("query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(Arc::clone(&context), vec![]).await?;
    let mut rows = Vec::new();
    while let Some(page) = cursor.next_batch().await? {
        let batch = page.batch();
        for row in 0..batch.num_rows() {
            rows.push(batch.schema().fields().iter().zip(batch.columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    drop(cursor);
    assert_eq!(context.memory.used_bytes(), 0);
    assert_eq!(context.spill_bytes(), 0);
    Ok(rows)
}

fn integers(values: &[i64]) -> V { V::list(values.iter().copied().map(V::I64).collect()) }

#[tokio::test]
async fn sql_series_endpoint_policy_preserves_embedded_xtql_explicit_endpoints() {
    for (end_exclusive, outer) in [(false, integers(&[1,2,3,4])), (true, integers(&[1,2,3]))] {
        assert_eq!(query(end_exclusive).await.unwrap(), [vec![outer, integers(&[1,2,3]), integers(&[1,2,3,4])]]);
    }
}
