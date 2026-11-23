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
async fn sql_between_symmetry_and_complex_subject_once_binding() {
    assert_eq!(query("SELECT 7 BETWEEN ASYMMETRIC 10 AND 5,7 BETWEEN SYMMETRIC 10 AND 5,7 NOT BETWEEN SYMMETRIC 10 AND 5",vec![]).await.unwrap(),[vec![V::Bool(false),V::Bool(true),V::Bool(false)]]);
    let start = tokio::time::Instant::now();
    assert_eq!(query("SELECT COALESCE(NULLIF(PG_SLEEP(0.005),NULL),7) BETWEEN SYMMETRIC 10 AND 5",vec![]).await.unwrap(),[vec![V::Bool(true)]]);
    assert_eq!(tokio::time::Instant::now()-start,std::time::Duration::from_millis(5));
    let start = tokio::time::Instant::now();
    assert_eq!(query("SELECT PG_SLEEP(0.005) BETWEEN 1 / 0 AND 1 / 0",vec![]).await.unwrap(),[vec![V::Null]]);
    assert_eq!(tokio::time::Instant::now()-start,std::time::Duration::from_millis(5));
    assert_eq!(query("SELECT v BETWEEN SYMMETRIC $1 AND $2 FROM (VALUES (1),(7),(12)) AS t(v) ORDER BY v",vec![V::I64(10),V::I64(5)]).await.unwrap(),[vec![V::Bool(false)],vec![V::Bool(true)],vec![V::Bool(false)]]);
}

#[test]
fn between_modes_keep_the_common_value_grammar_boundary() {
    for sql in ["SELECT 1 BETWEEN SYMMETRIC NOT TRUE AND 2","SELECT 1 BETWEEN ASYMMETRIC 0 AND NOT TRUE","SELECT 1 BETWEEN SYMMETRIC ASYMMETRIC 0 AND 2"] { assert!(rtdb_sql::parse(sql).is_err(),"{sql}"); }
    assert!(rtdb_sql::parse("SELECT (1 BETWEEN SYMMETRIC 0 AND 2) BETWEEN ASYMMETRIC FALSE AND TRUE").is_ok());
}

#[tokio::test]
async fn runtime_between_specializations_are_admitted_before_payload_selection() {
    let sql = "SELECT (COALESCE($1,$1) BETWEEN $2 AND $2) IS NULL";
    assert_eq!(query(sql,vec![V::IntervalYearMonth(1),V::IntervalMonthDayNano { months: 0,days: 1,nanoseconds: 0 }]).await.unwrap_err().code,"xtdb.expression/cannot-compare-different-unit-intervals");
    assert_eq!(query(sql,vec![V::FixedSizeList(vec![V::I64(1)].into()),V::FixedSizeList(vec![V::I64(2)].into())]).await.unwrap_err().code,"xtdb.expression/function-type-mismatch");
    assert_eq!(query("SELECT (COALESCE(1,2) BETWEEN 'a' AND 'z') IS NULL",vec![]).await.unwrap(),[vec![V::Bool(false)]]);
}

#[tokio::test]
async fn sql_between_preserves_literal_bounds_after_binding_a_complex_subject() {
    assert_eq!(query("SELECT '1970-01-01' BETWEEN DATE '1970-01-01' AND DATE '1970-01-01',COALESCE(DATE '1970-01-01') BETWEEN '1970-01-01' AND '1970-01-01'",vec![]).await.unwrap(),[vec![V::Bool(true),V::Bool(true)]]);
    assert!(query("SELECT COALESCE('1970-01-01') BETWEEN DATE '1970-01-01' AND DATE '1970-01-01'",vec![]).await.is_err());
}
