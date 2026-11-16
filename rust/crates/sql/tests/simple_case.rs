use std::{collections::BTreeMap,sync::Arc};
use rtdb_columnar::{MemoryBudget,read_value};
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
        let prepared = prepare(source,&context.catalog,&PrepareOptions::default())?;
        let mut cursor = PreparedQuery::from_prepared(prepared,&context.memory)?.execute(Arc::clone(&context),arguments).await?;
        let mut rows = vec![];
        while let Some(batch) = cursor.next_batch().await? {
            for row in 0..batch.batch().num_rows() { rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field,array)|read_value(field,array.as_ref(),row)).collect::<Result<_>>()?); }
        }
        Ok(rows)
    }.await;
    assert_eq!(context.memory.used_bytes(),0);
    result
}


#[tokio::test(start_paused = true)]
async fn sql_simple_case_binds_null_and_nonnull_operands_exactly_once() {
    let start = tokio::time::Instant::now();
    assert_eq!(query("SELECT CASE PG_SLEEP(0.005) WHEN 1 / 0 THEN 1 WHEN NULL THEN 2 ELSE 7 END",vec![]).await.unwrap(),[vec![V::I64(7)]]);
    assert_eq!(tokio::time::Instant::now()-start,std::time::Duration::from_millis(5));
    let start = tokio::time::Instant::now();
    assert_eq!(query("SELECT CASE COALESCE(NULLIF(PG_SLEEP(0.005),NULL),7) WHEN 1 THEN 1 WHEN 7 THEN 9 WHEN 1 / 0 THEN 10 ELSE 0 END",vec![]).await.unwrap(),[vec![V::I64(9)]]);
    assert_eq!(tokio::time::Instant::now()-start,std::time::Duration::from_millis(5));
    assert_eq!(query("SELECT (CASE 1 WHEN 1 THEN 1 / 0 ELSE 2 END) + NULL",vec![]).await.unwrap(),[vec![V::Null]]);
    assert_eq!(query("SELECT (CASE 1 WHEN 1 THEN 1 / 0 ELSE NULL END) + NULL",vec![]).await.unwrap_err().code,"xtdb.expression/division-by-zero");
}

#[tokio::test]
async fn sql_simple_case_admits_later_when_types_and_preserves_literal_coercion() {
    assert_eq!(query("SELECT CASE $1 WHEN 0 THEN 1 WHEN $2 THEN 2 ELSE 3 END",vec![V::Set(vec![]),V::Set(vec![])]).await.unwrap_err().code,"xtdb.expression/set-equality");
    assert_eq!(query("SELECT CASE WHEN TRUE THEN 1 ELSE CASE $1 WHEN $2 THEN 2 ELSE 3 END END",vec![V::Set(vec![]),V::Set(vec![])]).await.unwrap_err().code,"xtdb.expression/set-equality");
    assert_eq!(query("SELECT CASE '1970-01-01' WHEN DATE '1970-01-01' THEN 1 ELSE 2 END,CASE DATE '1970-01-01' WHEN '1970-01-01' THEN 1 ELSE 2 END",vec![]).await.unwrap(),[vec![V::I64(2),V::I64(1)]]);
}

#[tokio::test]
async fn simple_case_survives_column_replacement_and_explain_rendering() {
    assert_eq!(query("SELECT CASE v WHEN 1 THEN v + 10 WHEN 2 THEN v + 20 ELSE 0 END FROM (VALUES (1),(2),(3)) AS t(v) ORDER BY v",vec![]).await.unwrap(),[vec![V::I64(11)],vec![V::I64(22)],vec![V::I64(0)]]);
    let rows = query("EXPLAIN SELECT CASE 1 WHEN 1 THEN 2 ELSE 3 END AS v",vec![]).await.unwrap();
    let V::Struct(explain) = &rows[0][0] else { panic!() };
    let V::Utf8(project) = &explain["project"] else { panic!() };
    assert!(project.contains("(case 1 1 2 3)"),"{project}");
}
