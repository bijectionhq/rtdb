use std::{collections::BTreeMap,sync::Arc};
use rtdb_columnar::{MemoryBudget,read_value};
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot,DatabaseSnapshot,PreparedQuery,QueryContext,QueryOptions};
use rtdb_sql::{PrepareOptions,prepare};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{Result,Timestamp,Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: Vec::new(),storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    QueryContext::new(catalog,MemoryBudget::new(1 << 27),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap()
}
async fn query(sql: &str,arguments: Vec<V>) -> Result<Vec<Vec<V>>> {
    let context = context();
    let result = async {
        let Statement::Query(relation) = prepare(sql,&context.catalog,&PrepareOptions::default())?.statement else { panic!("query") };
        let mut cursor = PreparedQuery::new(relation)?.execute(Arc::clone(&context),arguments).await?;
        let mut rows = Vec::new();
        while let Some(page) = cursor.next_batch().await? {
            let batch = page.batch();
            for row in 0..batch.num_rows() { rows.push(batch.schema().fields().iter().zip(batch.columns()).map(|(field,array)|read_value(field,array.as_ref(),row)).collect::<Result<_>>()?); }
        }
        Ok(rows)
    }.await;
    assert_eq!(context.memory.used_bytes(),0);
    result
}

#[tokio::test]
async fn sql_like_uses_java_pattern_semantics_and_ignores_escape_expressions() {
    assert_eq!(query("SELECT 'ı' ILIKE 'I','İ' ILIKE 'i','a.b' LIKE 'a.b','axb' LIKE 'a.b'",vec![]).await.unwrap(),
        [vec![V::Bool(true),V::Bool(true),V::Bool(true),V::Bool(false)]]);
    for escape in ["'!'","''","'long'","1/0","missing_column","unknown_function(1)"] {
        assert_eq!(query(&format!("SELECT 'a!x' LIKE 'a!_' ESCAPE {escape}"),vec![]).await.unwrap(),[vec![V::Bool(true)]],"{escape}");
        assert_eq!(query(&format!("SETTING SNAPSHOT_TIME=('a!x' LIKE 'a!_' ESCAPE {escape}) SELECT 1"),vec![]).await.unwrap(),[vec![V::I64(1)]],"{escape}");
    }
    assert_eq!(query("SELECT CAST(1/0 AS VARCHAR) LIKE NULL,NULL ILIKE CAST(1/0 AS VARCHAR)",vec![]).await.unwrap(),[vec![V::Null,V::Null]]);
    assert_eq!(query("SELECT CHARACTER_LENGTH('x' LIKE NULL)",vec![]).await.unwrap(),[vec![V::Null]]);
    assert_eq!(query("SELECT $1 LIKE $2",vec![V::Binary(vec![15]),V::Binary(vec![2,95])]).await.unwrap(),[vec![V::Bool(true)]]);
    assert!(query("SELECT $1 ILIKE $2",vec![V::Binary(vec![15]),V::Binary(vec![2,95])]).await.is_err());
    let context = context();
    for source in ["SELECT 1 LIKE 'x'","SELECT like('x','x')","SELECT ilike('x','x')"] {
        assert!(prepare(source,&context.catalog,&PrepareOptions::default()).is_err(),"{source}");
    }
}
