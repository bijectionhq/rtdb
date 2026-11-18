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
async fn access_control_functions_are_true_with_or_without_a_user() {
    for call in ["has_any_column_privilege('xtdb','docs', 'select')", "has_any_column_privilege('docs', 'select')", "pg_catalog.has_any_column_privilege('docs', 'select')",
        "has_table_privilege('xtdb','docs', 'select')", "has_table_privilege('docs', 'select')", "pg_catalog.has_table_privilege('docs', 'select')",
        "has_schema_privilege('xtdb', 'public', 'select')", "has_schema_privilege('public', 'select')", "pg_catalog.has_schema_privilege('public', 'select')",
        "has_table_privilege(current_user, 'docs', 'select')", "has_schema_privilege(current_user, 'public', 'select')"] {
        assert_eq!(query(&format!("SELECT {call}")).await.unwrap(), [vec![V::Bool(true)]], "{call}");
    }
    assert_eq!(query("SELECT t.x FROM (VALUES (3)) t(x) WHERE has_table_privilege('docs', 'select')").await.unwrap(), [vec![V::I64(3)]]);
    assert_eq!(query("SELECT has_table_privilege(no_such_column, 'select')").await.unwrap(), [vec![V::Bool(true)]]);
    assert!(query("SELECT has_table_privilege('docs')").await.is_err());
}

#[tokio::test]
async fn catalog_introspection_functions_are_constants() {
    assert_eq!(query("SELECT pg_catalog.pg_get_userbyid(t.owner), pg_table_is_visible(t.oid) FROM (VALUES (10, 20)) t(owner, oid)").await.unwrap(),
        [vec![V::Utf8("xtdb".into()), V::Bool(true)]]);
    assert!(query("SELECT pg_get_userbyid(10)").await.is_err());
    assert_eq!(query("SELECT pg_get_expr(1, 2), pg_catalog.pg_get_expr(1, 2, true), pg_get_expr, pg_get_indexdef(1), pg_get_indexdef(1, 2, false), _pg_expandarray(ARRAY[1]), information_schema._pg_expandarray").await.unwrap(),
        [vec![V::Null; 7]]);
    assert!(query("SELECT pg_get_indexdef(1, 2)").await.is_err());
}

#[tokio::test]
async fn metabase_catalog_probes_plan_over_pg_catalog() {
    assert_eq!(query("SELECT tmp.table_schema, tmp.table_name,
            TRIM(BOTH '\"' FROM pg_catalog.pg_get_indexdef(tmp.ci_oid, tmp.pos, false)) AS field_name
        FROM (SELECT n.nspname AS table_schema, ct.relname AS table_name, ci.oid AS ci_oid,
                     (information_schema._pg_expandarray(i.indkey)).n AS pos
              FROM pg_catalog.pg_class ct
              JOIN pg_catalog.pg_namespace n ON ct.relnamespace = n.oid
              JOIN pg_catalog.pg_index i ON ct.oid = i.indrelid
              JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
              WHERE pg_catalog.pg_get_expr(i.indpred, i.indrelid) IS NULL
                AND n.nspname !~ '^information_schema|catalog_history|pg_') tmp
        WHERE tmp.pos = 1").await.unwrap(), Vec::<Vec<V>>::new());
    query("SELECT c.relname, pg_catalog.pg_get_userbyid(c.relowner) AS owner
        FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE pg_catalog.pg_table_is_visible(c.oid)").await.unwrap();
}
