use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::PartitionState;
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_trie::TrieConfig;
use rtdb_types::{Result, Timestamp, Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap()
}
async fn query(source: &str) -> Result<Vec<Vec<V>>> {
    let context = context();
    let Statement::Query(relation) = prepare(source, &context.catalog, &PrepareOptions::default())?.statement else { panic!("query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(context, vec![]).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() { rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?); }
    }
    Ok(rows)
}

#[tokio::test]
async fn a_wildcard_over_grouped_columns_keeps_the_source_column_order() {
    let mut rows = query("SELECT * FROM (VALUES (83, 0, 38), (26, 0, 79)) AS t (col0, col1, col2) GROUP BY t.col1, t.col2, t.col0").await.unwrap();
    rows.sort_by_key(|row| match row[0] { V::I64(value) => value, _ => 0 });
    assert_eq!(rows, [vec![V::I64(26), V::I64(0), V::I64(79)], vec![V::I64(83), V::I64(0), V::I64(38)]]);
}

#[tokio::test]
async fn a_repeated_grouping_key_groups_once() {
    let mut rows = query("SELECT - t.col2 FROM (VALUES (1, 58), (2, 79), (3, 58)) AS t (col0, col2) GROUP BY t.col2, t.col2").await.unwrap();
    rows.sort_by_key(|row| match row[0] { V::I64(value) => value, _ => 0 });
    assert_eq!(rows, [vec![V::I64(-79)], vec![V::I64(-58)]]);
}
