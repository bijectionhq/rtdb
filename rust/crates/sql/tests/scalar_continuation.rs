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

#[tokio::test(start_paused = true)]
async fn sql_scalar_arguments_select_nulls_before_payloads() {
    let start = tokio::time::Instant::now();
    for expression in [
        "1 / 0 + NULL", "NULL + 1 / 0", "1 / 0 < NULL", "1 / 0 <> NULL",
        "POWER(1 / 0,NULL)", "POWER(NULL,1 / 0)", "LOG(1 / 0,NULL)",
        "SUBSTRING(CAST(1 / 0 AS TEXT) FROM NULL)", "CAST(1 / 0 AS TEXT) || NULL",
        "REPLACE(CAST(1 / 0 AS TEXT),'x',NULL)", "STRING_TO_ARRAY(NULL,CAST(1 / 0 AS TEXT))", "FORMAT_TYPE(NULL,1 / 0)",
        "CAST(PG_SLEEP(0.005) AS BIGINT)", "-PG_SLEEP(0.005)", "NOT PG_SLEEP(0.005)",
        "ABS(PG_SLEEP(0.005))", "CURRENT_SCHEMAS(PG_SLEEP(0.005))", "CURRENT_SETTING(PG_SLEEP(0.005))", "URI_PATH(PG_SLEEP(0.005))", "CHAR_LENGTH(PG_SLEEP(0.005))",
        "GREATEST(PG_SLEEP(0.005),1 / 0)",
    ] { assert_eq!(query(&format!("SELECT {expression}"),vec![]).await.unwrap(),[vec![V::Null]],"{expression}"); }
    assert_eq!(tokio::time::Instant::now(),start);
    assert_eq!(query("SELECT (1 / 0) IS NULL,(1 / 0) IS TRUE,(1 / 0) IS FALSE,COALESCE(PG_SLEEP(0.005),7)",vec![]).await.unwrap(),[vec![V::Bool(false),V::Bool(false),V::Bool(false),V::I64(7)]]);
    assert_eq!(tokio::time::Instant::now(),start);
}

#[tokio::test]
async fn sql_case_tests_use_true_predicates_and_keep_control_selection() {
    assert_eq!(query("SELECT CASE WHEN 1 / 0 THEN 1 ELSE 2 END",vec![]).await.unwrap(),[vec![V::I64(2)]]);
    for expression in [
        "POWER(CASE WHEN 1 / 0 = 0 THEN 1 ELSE 2 END,NULL)",
        "POWER(CASE WHEN TRUE THEN 1 / 0 ELSE NULL END,NULL)",
        "GREATEST(1 / 0,NULL)", "NULLIF(1 / 0,NULL)",
    ] { assert_eq!(query(&format!("SELECT {expression}"),vec![]).await.unwrap_err().code,"xtdb.expression/division-by-zero","{expression}"); }
    assert_eq!(query("SELECT POWER(NULL,CASE WHEN 1 / 0 = 0 THEN 1 ELSE 2 END)",vec![]).await.unwrap(),[vec![V::Null]]);
    assert_eq!(query("SELECT ROUND('Infinity'::DOUBLE,1 / 0)",vec![]).await.unwrap(),[vec![V::F64(f64::INFINITY)]]);
    assert_eq!(query("SELECT LOG(1 / 0,CAST('invalid-number' AS DOUBLE))",vec![]).await.unwrap_err().code,"xtdb.expression/number-format-error");
}

#[tokio::test(start_paused = true)]
async fn sql_nullif_let_and_format_preserve_null_payload_effects() {
    let start = tokio::time::Instant::now();
    assert_eq!(query("SELECT NULLIF(PG_SLEEP(0.005),1 / 0)",vec![]).await.unwrap(),[vec![V::Null]]);
    assert_eq!(tokio::time::Instant::now()-start,std::time::Duration::from_millis(5));
    assert_eq!(query("SELECT FORMAT('%s',PG_SLEEP(0.005))",vec![]).await.unwrap(),[vec![V::Utf8(String::new())]]);
    assert_eq!(tokio::time::Instant::now()-start,std::time::Duration::from_millis(10));
}
