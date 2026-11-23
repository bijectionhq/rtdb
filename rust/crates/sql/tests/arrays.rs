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
async fn sql_array_bounds_and_trim_preserve_reference_results() {
    assert_eq!(query("SELECT ARRAY_UPPER(ARRAY[1,2],1),ARRAY_LOWER(ARRAY[],1),ARRAY_LENGTH(ARRAY[],1),TRIM_ARRAY(ARRAY[42,43],1)").await.unwrap(),
        [vec![V::I32(2), V::I32(1), V::I32(0), V::list(vec![V::I64(42)])]]);
    assert_eq!(query("SELECT ARRAY_LENGTH(TRIM_ARRAY(ARRAY[1,2],-1),1),ARRAY_LOWER(TRIM_ARRAY(ARRAY[],-2147483648),1),TRIM_ARRAY(NULL,1),TRIM_ARRAY(ARRAY[1],NULL) AS test_output_4").await.unwrap(),
        [vec![V::I32(3), V::I32(1), V::Null, V::Null]]);
    for sql in ["SELECT ARRAY_UPPER(ARRAY[1],2)", "SELECT ARRAY_LENGTH(ARRAY[1],0)", "SELECT ARRAY_LOWER(ARRAY[],2)"] {
        assert_eq!(query(sql).await.unwrap_err().code, "xtdb.expression/array-dimension-error");
    }
    assert_eq!(query("SELECT TRIM_ARRAY(ARRAY[1,2,3],4)").await.unwrap_err().code, "xtdb.expression/array-element-error");
    assert!(query("SELECT ARRAY_LENGTH(ARRAY[1],CAST(1 AS INT))").await.is_err());
    assert_eq!(query("SELECT TRIM_ARRAY(ARRAY[1,2],CAST(1 AS INT))").await.unwrap(), [vec![V::list(vec![V::I64(1)])]]);
}

#[tokio::test]
async fn array_views_accept_relational_columns_and_keep_casts_deferred() {
    assert_eq!(query("SELECT TRIM_ARRAY(v,1),ARRAY_LENGTH(v,1) FROM (VALUES (ARRAY[1,2]),(ARRAY[3,4,5])) AS t(v)").await.unwrap(),
        [vec![V::list(vec![V::I64(1)]), V::I32(2)], vec![V::list(vec![V::I64(3), V::I64(4)]), V::I32(3)]]);
    assert_eq!(query("SELECT TRIM_ARRAY(CAST(ARRAY['7','bad'] AS BIGINT ARRAY),1)").await.unwrap(), [vec![V::list(vec![V::I64(7)])]]);
    assert_eq!(query("SELECT ARRAY_LENGTH(ARRAY[PG_SLEEP(-0.1)],1)").await.unwrap(), [vec![V::I32(1)]]);
}

#[tokio::test]
async fn sql_trim_count_continuations_preserve_competing_failure_order() {
    for count in ["1/0", "CASE WHEN TRUE THEN 1/0 ELSE 0 END", "COALESCE(NULL,1/0)"] {
        assert_eq!(query(&format!("SELECT TRIM_ARRAY(TRIM_ARRAY(ARRAY[],-2147483648),{count})")).await.unwrap_err().code,"xtdb.expression/overflow-error");
    }
    for count in ["CASE WHEN 1/0=0 THEN 0 ELSE 1 END", "CASE WHEN TRUE THEN 1/0 ELSE NULL END", "COALESCE(1/0,0)"] {
        assert_eq!(query(&format!("SELECT TRIM_ARRAY(TRIM_ARRAY(ARRAY[],-2147483648),{count})")).await.unwrap_err().code,"xtdb.expression/division-by-zero");
    }
    assert_eq!(query("SELECT TRIM_ARRAY(TRIM_ARRAY(ARRAY[],-2147483648),CASE WHEN TRUE THEN NULL ELSE 1/0 END)").await.unwrap(),[vec![V::Null]]);
}

#[tokio::test]
async fn list_concatenation_is_not_a_matching_concat_clause() {
    let error = query("SELECT [1, 2] || [3]").await.unwrap_err();
    assert_eq!((error.code.as_str(), error.message.as_deref()), ("xtdb.error/illegal-arg", Some("No matching clause: [<List> <Int(64, true)>]")));
}

#[tokio::test]
async fn cosine_distance_accepts_sql_lists_and_propagates_nulls() {
    let output = query("SELECT COSINE_DISTANCE(ARRAY[1,0], ARRAY[3,0]), COSINE_DISTANCE(ARRAY[1,0], ARRAY[0,3]), COSINE_DISTANCE(ARRAY[1,0], ARRAY[-3,0]), COSINE_DISTANCE(NULL, ARRAY[1])").await.unwrap();
    assert_eq!(output, [vec![V::F64(0.0), V::F64(1.0), V::F64(2.0), V::Null]]);

    let output = query("SELECT COSINE_DISTANCE(ARRAY[1E300,1E300], ARRAY[1E-300,-1E-300])").await.unwrap();
    assert_eq!(output, [vec![V::F64(1.0)]]);
}

#[tokio::test]
async fn cosine_distance_rejects_inputs_without_a_defined_distance() {
    for (arguments, code) in [
        ("ARRAY[1], ARRAY[1,2]", "rtdb.vector/dimensions"),
        ("ARRAY[], ARRAY[]", "rtdb.vector/dimensions"),
        ("ARRAY[1], ARRAY[0]", "rtdb.vector/zero-norm"),
        ("ARRAY[NULL], ARRAY[1]", "rtdb.vector/component"),
        ("ARRAY['vector'], ARRAY[1]", "rtdb.vector/component"),
        ("ARRAY[CAST('Infinity' AS DOUBLE PRECISION)], ARRAY[1]", "rtdb.vector/component"),
        ("1, ARRAY[1]", "rtdb.vector/type"),
        ("ARRAY[1]", "xtdb/sql-error"),
    ] {
        let statement = format!("SELECT COSINE_DISTANCE({arguments})");
        assert_eq!(query(&statement).await.unwrap_err().code, code, "{statement}");
    }
}
