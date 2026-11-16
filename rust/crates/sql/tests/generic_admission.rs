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
async fn sql_catalog_keeps_character_length_alias_and_rejects_unregistered_functions() {
    assert_eq!(query("SELECT CHAR_LENGTH('abc'),CHARACTER_LENGTH('abc')",vec![]).await.unwrap(),[vec![V::I32(3),V::I32(3)]]);
    let context = context();
    for name in ["TRUNC","ATAN2","DEGREES","RADIANS","UUID","URI","SUBSTR","BIT_LENGTH","UNREGISTERED_FN"] {
        assert!(prepare(&format!("SELECT {name}(NULL)"),&context.catalog,&PrepareOptions::default()).is_err(),"{name}");
    }
}
