use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{Schema, Statement};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType as T, Result, ScalarType as S, Timestamp, Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: Vec::new(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions { batch_rows: 1, sort_memory_bytes: 1, merge_fan_in: 2, ..Default::default() }, &CancellationToken::new()).unwrap()
}

async fn query(sql: &str) -> Result<(Schema, Vec<Vec<V>>)> {
    let context = context();
    let Statement::Query(relation) = prepare(sql, &context.catalog, &PrepareOptions::default())?.statement else { panic!("Expected query") };
    let schema = relation.schema().clone();
    let mut cursor = PreparedQuery::new(relation)?.execute(Arc::clone(&context), vec![]).await?;
    let mut rows = Vec::new();
    while let Some(page) = cursor.next_batch().await? {
        let batch = page.batch();
        for row in 0..batch.num_rows() {
            rows.push(batch.schema().fields().iter().zip(batch.columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    drop(cursor);
    assert_eq!(context.spill_bytes(), 0);
    assert_eq!(context.memory.used_bytes(), 0);
    Ok((schema, rows))
}

#[tokio::test]
async fn row_number_sorts_each_partition_and_keeps_first_seen_partition_and_peer_order() {
    let (schema, rows) = query("SELECT p,v,ROW_NUMBER() OVER(PARTITION BY p ORDER BY v) FROM (VALUES (2,30),(1,20),(2,10),(1,20),(NULL,9),(NULL,8)) AS t(p,v)").await.unwrap();
    assert_eq!(schema.fields()[2].name, "_column_3");
    assert_eq!(schema.fields()[2].data_type, T::Scalar(S::I64));
    assert_eq!(rows, [vec![V::I64(2),V::I64(10),V::I64(1)],vec![V::I64(2),V::I64(30),V::I64(2)],
        vec![V::I64(1),V::I64(20),V::I64(1)],vec![V::I64(1),V::I64(20),V::I64(2)],
        vec![V::Null,V::I64(8),V::I64(1)],vec![V::Null,V::I64(9),V::I64(2)]]);
    let (_, rows) = query("SELECT label,ROW_NUMBER() OVER(ORDER BY n) AS rn FROM (VALUES ('b',1),('a',1),('c',1)) AS t(label,n)").await.unwrap();
    assert_eq!(rows, [vec![V::Utf8("b".into()),V::I64(1)],vec![V::Utf8("a".into()),V::I64(2)],vec![V::Utf8("c".into()),V::I64(3)]]);
}

#[tokio::test]
async fn row_number_supports_no_clauses_computed_order_and_null_order() {
    let input = "FROM (VALUES (3),(NULL),(1),(2)) AS t(v)";
    assert_eq!(query(&format!("SELECT v,ROW_NUMBER() OVER() AS rn {input}")).await.unwrap().1,
        [vec![V::I64(3),V::I64(1)],vec![V::Null,V::I64(2)],vec![V::I64(1),V::I64(3)],vec![V::I64(2),V::I64(4)]]);
    assert_eq!(query(&format!("SELECT v,ROW_NUMBER() OVER(ORDER BY -v NULLS FIRST) AS rn {input}")).await.unwrap().1,
        [vec![V::Null,V::I64(1)],vec![V::I64(3),V::I64(2)],vec![V::I64(2),V::I64(3)],vec![V::I64(1),V::I64(4)]]);
    assert_eq!(query(&format!("SELECT v,ROW_NUMBER() OVER(ORDER BY v DESC) AS rn {input}")).await.unwrap().1,
        [vec![V::Null,V::I64(1)],vec![V::I64(3),V::I64(2)],vec![V::I64(2),V::I64(3)],vec![V::I64(1),V::I64(4)]]);
    assert_eq!(query("SELECT ROW_NUMBER() OVER() AS rn FROM (VALUES (1)) AS t(v) WHERE FALSE").await.unwrap().1, Vec::<Vec<V>>::new());
    assert_eq!(query("SELECT ROW_NUMBER() OVER()+10 AS n").await.unwrap().1, [vec![V::I64(11)]]);
}

#[tokio::test]
async fn lead_lag_offsets_respect_nulls_and_partition_boundaries_across_spilled_batches() {
    let input = "FROM (VALUES (1,1,10),(1,2,NULL),(2,1,40),(1,3,30),(2,2,50)) AS t(p,n,v)";
    for (call, expected) in [
        ("LEAD(v)", vec![V::Null,V::I64(30),V::Null,V::I64(50),V::Null]),
        ("LAG(v)", vec![V::Null,V::I64(10),V::Null,V::Null,V::I64(40)]),
        ("LEAD(v,2)", vec![V::I64(30),V::Null,V::Null,V::Null,V::Null]),
        ("LAG(v,2)", vec![V::Null,V::Null,V::I64(10),V::Null,V::Null]),
        ("LEAD(v,0)", vec![V::I64(10),V::Null,V::I64(30),V::I64(40),V::I64(50)]),
        ("LAG(v,1000000)", vec![V::Null;5]),
    ] {
        let (schema, rows) = query(&format!("SELECT {call} OVER(PARTITION BY p ORDER BY n) AS shifted {input}")).await.unwrap();
        assert_eq!(schema.fields()[0].data_type, T::Scalar(S::I64).maybe());
        assert_eq!(rows, expected.into_iter().map(|value| vec![value]).collect::<Vec<_>>(), "{call}");
    }
    let (schema, rows) = query("SELECT LEAD(v,0) OVER() AS shifted FROM (VALUES (1)) AS t(v)").await.unwrap();
    assert!(schema.fields()[0].data_type.nullable());
    assert_eq!(rows, [vec![V::I64(1)]]);
    assert_eq!(query("SELECT LEAD(v) OVER(ORDER BY n) AS shifted FROM (VALUES (1,1),(2,'two'),(3,3.0)) AS t(n,v)").await.unwrap().1,
        [vec![V::Utf8("two".into())],vec![V::F64(3.0)],vec![V::Null]]);
}

#[tokio::test]
async fn window_results_work_in_outer_filters_aggregate_inputs_and_wildcards() {
    assert_eq!(query("SELECT v FROM (SELECT v,ROW_NUMBER() OVER(ORDER BY v DESC) AS rn FROM (VALUES (1),(3),(2)) AS t(v)) AS q WHERE rn=2").await.unwrap().1, [vec![V::I64(2)]]);
    assert_eq!(query("SELECT LEAD(SUM(v)) OVER(ORDER BY p) AS next_sum FROM (VALUES (1,10),(1,20),(2,40)) AS t(p,v) GROUP BY p").await.unwrap().1,
        [vec![V::I64(40)],vec![V::Null]]);
    let (schema, rows) = query("SELECT *,ROW_NUMBER() OVER() AS rn FROM (VALUES (7)) AS t(v)").await.unwrap();
    assert_eq!(schema.fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["v","rn"]);
    assert_eq!(rows, [vec![V::I64(7),V::I64(1)]]);
}

#[tokio::test]
async fn inline_existing_names_and_window_definitions_are_inert_in_pinned_planner() {
    for sql in [
        "SELECT v,ROW_NUMBER() OVER(w) AS rn FROM (VALUES (2),(1)) AS t(v)",
        "SELECT v,ROW_NUMBER() OVER(w) AS rn FROM (VALUES (2),(1)) AS t(v) WINDOW w AS (ORDER BY v)",
        "SELECT v,ROW_NUMBER() OVER() AS rn FROM (VALUES (2),(1)) AS t(v) WINDOW w AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
    ] { assert_eq!(query(sql).await.unwrap().1, [vec![V::I64(2),V::I64(1)],vec![V::I64(1),V::I64(2)]], "{sql}"); }
}

#[tokio::test]
async fn unsupported_window_surfaces_preserve_pinned_failures() {
    for sql in [
        "SELECT ROW_NUMBER() OVER(),ROW_NUMBER() OVER()",
        "SELECT ROW_NUMBER() OVER w WINDOW w AS ()",
        "SELECT ROW_NUMBER() OVER(ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
        "SELECT ROW_NUMBER() OVER(RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
        "SELECT ROW_NUMBER() OVER(GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
        "SELECT ROW_NUMBER() OVER() AS rn QUALIFY rn=1",
        "SELECT RANK() OVER()", "SELECT SUM(1) OVER()",
        "SELECT LEAD(v,1,0) OVER() FROM (VALUES (1)) AS t(v)",
        "SELECT LEAD(v) IGNORE NULLS OVER() FROM (VALUES (1)) AS t(v)",
        "SELECT LEAD(v) RESPECT NULLS OVER() FROM (VALUES (1)) AS t(v)",
        "SELECT LEAD(v+1) OVER() FROM (VALUES (1)) AS t(v)",
        "SELECT LAG(v,-1) OVER() FROM (VALUES (1)) AS t(v)",
        "SELECT LAG(v,?) OVER() FROM (VALUES (1)) AS t(v)",
        "SELECT ROW_NUMBER() OVER(PARTITION BY v+1) FROM (VALUES (1)) AS t(v)",
        "SELECT ROW_NUMBER() OVER(ORDER BY 1) FROM (VALUES (1)) AS t(v)",
        "SELECT ROW_NUMBER() OVER(ORDER BY (SELECT 1)) FROM (VALUES (1)) AS t(v)",
        "SELECT ROW_NUMBER() OVER(ORDER BY alias) AS alias FROM (VALUES (1)) AS t(v)",
        "SELECT v FROM (VALUES (1)) AS t(v) WHERE ROW_NUMBER() OVER()=1",
    ] { assert!(query(sql).await.is_err(), "{sql}"); }
}
