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
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap()
}
async fn query(source: &str) -> Result<Vec<Vec<V>>> {
    let context = context();
    let Statement::Query(relation) = prepare(source, &context.catalog, &PrepareOptions::default())?.statement else { panic!("query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(context, vec![]).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() { rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?); }
    }
    Ok(rows)
}
#[tokio::test]
async fn sql_registered_math_calls_reach_the_numeric_core() {
    assert_eq!(query("SELECT LOG10(100),SINH(0),COSH(0),TANH(0),MOD(-5,3),MOD(5,-3) AS test_output_6").await.unwrap(),
        [vec![V::F64(2.0),V::F64(0.0),V::F64(1.0),V::F64(0.0),V::I64(1),V::I64(-1)]]);
    assert_eq!(query("SELECT LOG10(NULL),COSH(NULL),MOD(1,NULL)").await.unwrap(), [vec![V::Null,V::Null,V::Null]]);
    assert_eq!(query("SELECT FLOOR(1.9),CEIL(-1.9),CEILING(1.2)").await.unwrap(), [vec![V::F64(1.0),V::F64(-1.0),V::F64(2.0)]]);
    assert_eq!(query("SELECT MOD(1,0)").await.unwrap_err().code, "xtdb.expression/division-by-zero");
    assert_eq!(query("SELECT MOD(-5.5,3.0) AS positive, MOD(5.5,-3.0) AS negative").await.unwrap(),
        [vec![V::F64(0.5),V::F64(-0.5)]]);
    assert!(query("SELECT SINH('invalid')").await.is_err());
}

#[tokio::test(start_paused = true)]
async fn sql_mod_discards_payloads_at_either_null_argument_and_preserves_selection_demands() {
    let start = tokio::time::Instant::now();
    for expression in ["MOD(NULL,1 / 0)", "MOD(1 / 0,NULL)", "MOD(PG_SLEEP(60.0),1 / 0)", "MOD(1 / 0,PG_SLEEP(60.0))"] {
        assert_eq!(query(&format!("SELECT {expression}")).await.unwrap(), [vec![V::Null]], "{expression}");
    }
    assert_eq!(tokio::time::Instant::now(), start);
    for expression in ["MOD(CASE WHEN 1 / 0 = 0 THEN 1 ELSE 2 END,NULL)", "MOD(CASE WHEN TRUE THEN 1 / 0 ELSE NULL END,NULL)"] {
        assert_eq!(query(&format!("SELECT {expression}")).await.unwrap_err().code, "xtdb.expression/division-by-zero", "{expression}");
    }
}

#[tokio::test]
async fn sql_numeric_admission_uses_the_sql_registry_before_shared_expression_normalization() {
    assert_eq!(query(r#"SELECT "log10"(100),custom.LOG10(100),"Custom"."sinh"(0),"mod"(-5,3)"#).await.unwrap(),
        [vec![V::F64(2.0),V::F64(2.0),V::F64(0.0),V::I64(1)]]);
    for expression in [
        r#""LOG10"(100)"#, r#"custom."SINH"(0)"#, r#""double"(7)"#, "custom.double(7)",
        "RANDOM()", "SLEEP(NULL)", "TRUNC(1)", "ATAN2(1,1)", "DEGREES(1)", "RADIANS(1)",
        "LOG(10)", "LEAST()", "GREATEST()",
    ] {
        assert!(query(&format!("SELECT {expression}")).await.is_err(), "{expression}");
        assert!(query(&format!("SETTING SNAPSHOT_TIME={expression} SELECT 1")).await.is_err(), "{expression}");
    }
}
