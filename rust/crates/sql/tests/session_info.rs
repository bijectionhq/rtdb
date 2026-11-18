use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::PartitionState;
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_trie::TrieConfig;
use rtdb_types::{LogicalType, Result, Timestamp, Value as V};
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
fn text(value: &str) -> V { V::Utf8(value.into()) }

#[tokio::test]
async fn session_information_keywords_aliases_and_scalar_calls_share_reference_values() {
    assert_eq!(query("SELECT CURRENT_USER,CURRENT_CATALOG,CURRENT_SCHEMA,CURRENT_SCHEMA() AS test_output_4,CURRENT_DATABASE(),pg_catalog.CURRENT_SCHEMA(),CURRENT_SCHEMAS(TRUE),CURRENT_SCHEMAS(FALSE) AS test_output_8,CURRENT_SETTING('search_path'),CURRENT_SETTING('server_version_num') AS test_output_10").await.unwrap(),
        [vec![text("xtdb"),text("xtdb"),text("public"),text("public"),text("xtdb"),text("public"),V::List(vec![text("pg_catalog"),text("public")]),V::List(vec![text("public")]),text("public"),text("160000")]]);
    assert_eq!(query("SELECT CURRENT_SCHEMAS(NULL),CURRENT_SETTING(NULL)").await.unwrap(), [vec![V::Null,V::Null]]);
    assert_eq!(query("SELECT CURRENT_SETTING('xtdb.version')").await.unwrap(), [vec![text(rtdb_types::runtime_version())]]);
    for source in ["SELECT VERSION()", "SELECT pg_catalog.VERSION()"] {
        assert_eq!(query(source).await.unwrap(), [vec![text(&format!("PostgreSQL 16 (XTDB {})", rtdb_types::runtime_version()))]]);
    }
    let expected = match rtdb_types::runtime_revision() {
        Some(revision) => format!("XTDB @ {} [{revision}]",rtdb_types::runtime_version()),
        None => format!("XTDB @ {}",rtdb_types::runtime_version()),
    };
    assert_eq!(query("SELECT xt.VERSION()").await.unwrap(), [vec![text(&expected)]]);
}

#[tokio::test]
async fn unsupported_session_settings_keep_the_requested_case_and_key() {
    let error=query("SELECT CURRENT_SETTING('Search_Path')").await.unwrap_err();
    assert_eq!(error.code,"xtdb.expression/unsupported-setting");
    assert_eq!(error.context["setting-name"],"Search_Path");
    for sql in ["SELECT CURRENT_SCHEMAS(1)","SELECT CURRENT_SETTING(TRUE)","SELECT CURRENT_DATABASE(1)"] {
        assert!(prepare(sql,&context().catalog,&PrepareOptions::default()).is_err(),"{sql}");
    }
}
