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
    let state = PartitionState::empty(TrieConfig::new(32,32).unwrap()).unwrap();
    let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    QueryContext::new(catalog,MemoryBudget::new(1 << 27),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap()
}
async fn query(source: &str) -> Result<Vec<Vec<V>>> {
    let context = context();
    let Statement::Query(relation) = prepare(source,&context.catalog,&PrepareOptions::default())?.statement else { panic!("query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(context,vec![]).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() { rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field,array)| read_value(field,array.as_ref(),row)).collect::<Result<_>>()?); }
    }
    Ok(rows)
}
fn text(value: &str) -> V { V::Utf8(value.into()) }

#[tokio::test]
async fn sql_uri_literals_casts_and_all_seven_extractors_reach_the_shared_parser() {
    let source = "HtTp://user%3Apass@EXAMPLE.com:80/path%20name?q=%ED%A0%80+#";
    assert_eq!(query(&format!("SELECT URI '{source}',CAST('{source}' AS URI),CAST(URI '{source}' AS VARCHAR)")).await.unwrap(),[vec![V::Uri(source.into()),V::Uri(source.into()),text(source)]]);
    assert_eq!(query(&format!("SELECT URI_SCHEME(u),URI_USER_INFO(u),URI_HOST(u),URI_PORT(u),URI_PATH(u),URI_QUERY(u),URI_FRAGMENT(u) FROM (VALUES (URI '{source}')) AS t(u)")).await.unwrap(),
        [vec![text("HtTp"),text("user:pass"),text("EXAMPLE.com"),V::I32(80),text("/path name"),text("q=�+"),text("")]]);
    assert_eq!(query("SELECT URI_PATH(URI 'mailto:x?y'),URI_QUERY(URI 'mailto:x?y'),URI_HOST(URI 'http://bad_host'),URI_PORT(URI 'http://host:'),URI_PATH(URI 'http://host'),URI_QUERY(URI 'http://host?')").await.unwrap(),
        [vec![V::Null,V::Null,V::Null,V::Null,text(""),text("")]]);
    assert_eq!(query("SELECT URI_PATH(URI $u$/a%20b$u$),URI_HOST(URI E'https://example.com')").await.unwrap(),[vec![text("/a b"),text("example.com")]]);
    assert_eq!(query("SELECT CASE WHEN FALSE THEN CAST('bad space' AS URI) ELSE URI 'relative' END").await.unwrap(),[vec![V::Uri("relative".into())]]);
}

#[tokio::test]
async fn sql_uri_extraction_nulls_do_not_construct_a_uri() {
    assert_eq!(query("SELECT URI_SCHEME(NULL),URI_USER_INFO(NULL),URI_HOST(NULL),URI_PORT(NULL),URI_PATH(NULL),URI_QUERY(NULL),URI_FRAGMENT(NULL)").await.unwrap(),[vec![V::Null;7]]);
}

#[test]
fn sql_uri_literals_validate_during_binding_and_extractors_keep_dedicated_admission() {
    let context = context();
    for expression in ["URI 'bad space'", "URI 'http://[bad]'", "CASE WHEN FALSE THEN URI 'bad space' ELSE NULL END", "URI_PATH('http://host')", "URI_PATH()", "URI_PATH(NULL,NULL)", "custom.URI_PATH(NULL)", "\"uri_path\"(NULL)"] {
        assert!(prepare(&format!("SELECT {expression}"),&context.catalog,&PrepareOptions::default()).is_err(),"{expression}");
    }
    for expression in ["URI_PATH(NULL)", "URI_HOST(URI 'http://host')"] {
        assert!(prepare(&format!("SETTING SNAPSHOT_TIME={expression} SELECT 1"),&context.catalog,&PrepareOptions::default()).is_ok(),"{expression}");
    }
    for expression in ["URI_PATH()", "URI_PATH(NULL,NULL)", "custom.URI_PATH(NULL)", "\"uri_path\"(NULL)", "URI_PATH(URI 'bad space')"] {
        assert!(prepare(&format!("SETTING SNAPSHOT_TIME={expression} SELECT 1"),&context.catalog,&PrepareOptions::default()).is_err(),"{expression}");
    }
}

#[tokio::test]
async fn sql_uri_equality_and_raw_vector_candidates_keep_their_distinct_contracts() {
    assert_eq!(query("SELECT URI 'HTTP://EXAMPLE.com/%ab' = URI 'http://example.com/%AB', URI 'HTTP://EXAMPLE.com/%ab' <> URI 'http://example.com/%AB', URI 'HTTP://EXAMPLE.com/%ab' IS NOT DISTINCT FROM URI 'http://example.com/%AB'").await.unwrap(),[vec![V::Bool(true),V::Bool(false),V::Bool(true)]]);
    let values = "(VALUES (URI 'HTTP://EXAMPLE.com/%ab'),(URI 'http://example.com/%AB'),(URI 'HTTP://EXAMPLE.com/%ab')) AS t(u)";
    assert_eq!(query(&format!("SELECT COUNT(DISTINCT u),MIN(u),MAX(u) FROM {values}")).await.unwrap(),[vec![V::I64(2),V::Uri("HTTP://EXAMPLE.com/%ab".into()),V::Uri("http://example.com/%AB".into())]]);
    assert_eq!(query(&format!("SELECT DISTINCT u FROM {values}")).await.unwrap().len(),2);
    let groups = query(&format!("SELECT COUNT(*) AS n FROM {values} GROUP BY u ORDER BY n")).await.unwrap();
    assert_eq!(groups,[vec![V::I64(1)],vec![V::I64(2)]]);
    let window = query(&format!("SELECT ROW_NUMBER() OVER (PARTITION BY u) AS n FROM {values} ORDER BY n")).await.unwrap();
    assert_eq!(window,[vec![V::I64(1)],vec![V::I64(1)],vec![V::I64(2)]]);
    assert_eq!(query("SELECT URI 'HTTP://EXAMPLE.com/%ab' UNION SELECT URI 'http://example.com/%AB'").await.unwrap().len(),2);
    assert!(query("SELECT URI 'HTTP://EXAMPLE.com/%ab' INTERSECT SELECT URI 'http://example.com/%AB'").await.unwrap().is_empty());
    assert_eq!(query("SELECT URI 'HTTP://EXAMPLE.com/%ab' EXCEPT SELECT URI 'http://example.com/%AB'").await.unwrap().len(),1);
    assert_eq!(query("SELECT COUNT(*) FROM (VALUES (URI 'HTTP://EXAMPLE.com/%ab')) AS a(u) JOIN (VALUES (URI 'http://example.com/%AB')) AS b(u) ON a.u=b.u").await.unwrap(),[vec![V::I64(0)]]);
    assert_eq!(query("SELECT COUNT(DISTINCT a) FROM (VALUES (ARRAY[URI 'HTTP://EXAMPLE.com/%ab']),(ARRAY[URI 'http://example.com/%AB'])) AS t(a)").await.unwrap(),[vec![V::I64(2)]]);
}

#[tokio::test]
async fn sql_uri_ordering_reports_the_pinned_comparator_failure_only_when_reached() {
    for source in [
        "SELECT URI 'a' < URI 'b'", "SELECT URI 'a' <= URI 'b'", "SELECT URI 'a' > URI 'b'", "SELECT URI 'a' >= URI 'b'",
        "SELECT LEAST(URI 'a',URI 'b')", "SELECT GREATEST(URI 'a',URI 'b')",
        "SELECT u FROM (VALUES (URI 'a'),(URI 'b')) AS t(u) ORDER BY u",
    ] { assert!(query(source).await.is_err(),"{source}"); }
    assert_eq!(query("SELECT URI 'a' < NULL").await.unwrap(),[vec![V::Null]]);
    assert_eq!(query("SELECT u FROM (VALUES (URI 'a')) AS t(u) ORDER BY u").await.unwrap(),[vec![V::Uri("a".into())]]);
}
