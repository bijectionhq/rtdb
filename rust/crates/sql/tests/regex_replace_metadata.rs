use std::{collections::BTreeMap,sync::Arc};
use rtdb_columnar::{MemoryBudget,read_value};
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot,DatabaseSnapshot,PreparedQuery,QueryContext,QueryOptions};
use rtdb_sql::{PrepareOptions,prepare};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{Result,Timestamp,Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    QueryContext::new(catalog,MemoryBudget::new(1 << 27),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap()
}
async fn query(source: &str,arguments: Vec<V>) -> Result<Vec<Vec<V>>> {
    let context = context();
    let result = async {
        let Statement::Query(relation) = prepare(source,&context.catalog,&PrepareOptions::default())?.statement else { panic!() };
        let mut cursor = PreparedQuery::new(relation)?.execute(Arc::clone(&context),arguments).await?;
        let mut rows = vec![];
        while let Some(batch) = cursor.next_batch().await? {
            for row in 0..batch.batch().num_rows() { rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field,array)|read_value(field,array.as_ref(),row)).collect::<Result<_>>()?); }
        }
        Ok(rows)
    }.await;
    assert_eq!(context.memory.used_bytes(),0);
    result
}

#[tokio::test]
async fn sql_replacement_syntax_retains_literal_tail_and_null_only_admission() {
    for (source,expected) in [
        ("SELECT REGEXP_REPLACE('A A','a','x','i')",V::Utf8("x x".into())),
        ("SELECT REGEXP_REPLACE(NULL,'[','x',1,2,'i')",V::Null),
        ("SELECT REGEXP_REPLACE($1,'[','x',0)",V::Null),
        ("SELECT COALESCE('kept',REGEXP_REPLACE('a','a','x',0))",V::Utf8("kept".into())),
        ("SELECT COALESCE('kept',REGEXP_REPLACE(1,'a','x'))",V::Utf8("kept".into())),
    ] {
        let arguments = if source.contains("$1") { vec![V::Null] } else { vec![] };
        assert_eq!(query(source,arguments).await.unwrap(),[vec![expected]],"{source}");
    }
    for source in ["SELECT REGEXP_REPLACE('a','a','x',0)","SELECT REGEXP_REPLACE('a','a','x',1,0,'i')"] {
        assert_eq!(query(source,vec![]).await.unwrap_err().code,"xtdb.expression/unsupported");
    }
    for source in ["SELECT REGEXP_REPLACE('a',LOWER('A'),'x')","SELECT REGEXP_REPLACE('a','a','x',NULL)","SELECT REGEXP_REPLACE('a','a','x',1+1)","SELECT REGEXP_REPLACE(NULL,'a','x',9223372036854775808)"] {
        assert!(prepare(source,&context().catalog,&PrepareOptions::default()).is_err(),"{source}");
    }
}

#[tokio::test]
async fn sql_replacement_patterns_prepare_before_rows_and_unselected_payloads() {
    for source in [
        "SELECT CASE WHEN TRUE THEN 'kept' ELSE REGEXP_REPLACE('a','[','x') END",
        "SELECT REGEXP_REPLACE(CAST(1/0 AS TEXT),'[','x')",
        "SELECT CARDINALITY(ARRAY[REGEXP_REPLACE('a','[','x')])",
        "SELECT CASE WHEN v IS NULL THEN 'kept' ELSE REGEXP_REPLACE(v,'[','x') END FROM (VALUES (NULL),('a')) AS t(v)",
    ] { assert_eq!(query(source,vec![]).await.unwrap_err().code,"xtdb.expression/illegal-argument-exception","{source}"); }
    assert_eq!(query("SELECT REGEXP_REPLACE('a','a','x',1),REGEXP_REPLACE('b','[','x')",vec![]).await.unwrap_err().code,"xtdb.expression/unsupported");
}
