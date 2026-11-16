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
async fn ordinary_sql_java_patterns_preserve_utf16_positions_without_marker_atoms() {
    assert_eq!(query("SELECT REGEXP_REPLACE('😀','','x'),REGEXP_REPLACE('😀','','')",vec![]).await.unwrap(),[vec![V::Utf8("x?x?x".into()),V::Utf8("😀".into())]]);
    assert_eq!(query(r"SELECT REGEXP_REPLACE('😀b','(?<=(.))b','$1'), '😀b' LIKE_REGEX '(?<=\uDE00)b', '😀' LIKE_REGEX '\uDE00'",vec![]).await.unwrap(),[vec![V::Utf8("😀?".into()),V::Bool(true),V::Bool(false)]]);
    assert_eq!(query(r"SELECT REGEXP_REPLACE('😀','\G','x')",vec![]).await.unwrap(),[vec![V::Utf8("x😀".into())]]);
}
