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
async fn sql_row_limits_accept_unsigned_literals_and_nonnegative_integer_parameters() {
    let rows = "(VALUES (1),(2),(3)) AS t(v)";
    assert_eq!(query(&format!("SELECT v FROM {rows} ORDER BY v LIMIT 1 OFFSET 1"),vec![]).await.unwrap(),[vec![V::I64(2)]]);
    assert_eq!(query(&format!("SELECT v FROM {rows} ORDER BY v OFFSET $1 ROWS FETCH NEXT $2 ROWS ONLY"),vec![V::I16(1),V::I64(1)]).await.unwrap(),[vec![V::I64(2)]]);
    assert_eq!(query(&format!("SELECT v FROM {rows} ORDER BY v FETCH FIRST ROW ONLY"),vec![]).await.unwrap(),[vec![V::I64(1)]]);
    assert_eq!(query(&format!("SELECT v FROM {rows} ORDER BY v FETCH FIRST ROW ONLY OFFSET 1 ROW"),vec![]).await.unwrap(),[vec![V::I64(2)]]);
    assert!(query("SELECT 1 LIMIT 0",vec![]).await.unwrap().is_empty());
    assert!(query("SELECT 1 OFFSET 9223372036854775807",vec![]).await.unwrap().is_empty());
    for value in [V::Null,V::I64(-1),V::F64(1.0),V::Utf8("1".into()),V::Bool(true),V::U64(u64::MAX)] {
        for source in ["SELECT 1 LIMIT $1","SELECT 1 OFFSET $1"] {
            assert_eq!(query(source,vec![value.clone()]).await.unwrap_err().code,"xtdb/expected-number","{source}: {value:?}");
        }
    }
}

#[tokio::test]
async fn sql_row_limit_opens_its_input_before_reading_the_limit_parameter() {
    let error = query("VALUES (1 / 0) LIMIT $1", vec![V::Utf8("1".into())]).await.unwrap_err();
    assert_eq!(error.code, "xtdb.expression/division-by-zero");
}

#[test]
fn sql_row_limit_syntax_rejects_expression_bounds_and_erased_limit_all() {
    let context = context();
    for source in [
        "SELECT 1 LIMIT -1","SELECT 1 LIMIT +1","SELECT 1 LIMIT (1)","SELECT 1 LIMIT 1+1",
        "SELECT 1 LIMIT NULL","SELECT 1 LIMIT ALL","SELECT 1 LIMIT ALL OFFSET 1","SELECT 1 OFFSET 1 LIMIT ALL",
        "SELECT 1 LIMIT 1.0","SELECT 1 LIMIT 1e0","SELECT 1 LIMIT 9223372036854775808",
        "SELECT 1 OFFSET ABS(1)","SELECT 1 OFFSET (1)","SELECT 1 FETCH FIRST (1) ROWS ONLY",
        "SELECT 1 FETCH FIRST 1 PERCENT ROWS ONLY","SELECT 1 FETCH FIRST 1 ROW WITH TIES",
        "SELECT 1 LIMIT 1 FETCH FIRST 2 ROWS ONLY","SELECT 1 FETCH FIRST 1 ROW ONLY LIMIT 2",
    ] { assert!(prepare(source,&context.catalog,&PrepareOptions::default()).is_err(),"{source}"); }
}
