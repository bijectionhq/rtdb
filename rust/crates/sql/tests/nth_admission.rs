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
async fn subscripts_refine_and_validate_parameter_index_types() {
    assert_eq!(query("SELECT ARRAY[4,5][$1]",vec![V::I16(2)]).await.unwrap(),[vec![V::I64(5)]]);
    assert_eq!(query("SELECT ARRAY[4,5][$1]",vec![V::Null]).await.unwrap(),[vec![V::Null]]);
    for index in [V::F32(1.0),V::F64(1.0)] {
        assert_eq!(query("SELECT ARRAY[4,5][$1]",vec![index]).await.unwrap_err().code,"xtdb.expression/function-type-mismatch");
    }
    let context = context();
    assert_eq!(prepare("SELECT ARRAY[4,5][1.0]",&context.catalog,&PrepareOptions::default()).unwrap_err().code,"xtdb.expression/function-type-mismatch");
    assert_eq!(query("SELECT CHARACTER_LENGTH(7 -> 0)",vec![]).await.unwrap(),[vec![V::Null]]);
}
