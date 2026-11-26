use std::{collections::BTreeMap, sync::Arc, time::Duration};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{Expr, Relation, RelationNode, Statement};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Result, TimeUnit, Timestamp, Value as V};
use tokio_util::sync::CancellationToken;

fn context(cancellation: &CancellationToken, spill_limit_bytes: u64) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: Vec::new(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions { batch_rows: 1, spill_limit_bytes, ..Default::default() }, cancellation).unwrap()
}

async fn query_at(context: Arc<QueryContext>, sql: &str, arguments: Vec<V>) -> Result<Vec<Vec<V>>> {
    let result = async {
        let options = PrepareOptions { parameter_types: arguments.iter().enumerate().map(|(index, value)| (index, value.logical_type())).collect(), ..Default::default() };
        let Statement::Query(relation) = prepare(sql, &context.catalog, &options)?.statement else { panic!("query") };
        let mut cursor = PreparedQuery::new(relation)?.execute(Arc::clone(&context), arguments).await?;
        let mut rows = Vec::new();
        while let Some(page) = cursor.next_batch().await? {
            let batch = page.batch();
            for row in 0..batch.num_rows() {
                rows.push(batch.schema().fields().iter().zip(batch.columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?);
            }
        }
        Ok(rows)
    }.await;
    assert_eq!(context.memory.used_bytes(), 0);
    assert_eq!(context.spill_bytes(), 0);
    result
}
async fn query(sql: &str, arguments: Vec<V>) -> Result<Vec<Vec<V>>> { query_at(context(&CancellationToken::new(), 1 << 27), sql, arguments).await }

#[tokio::test]
async fn in_list_marks_keep_matches_unknowns_negation_and_correlated_rows() {
    assert_eq!(query("SELECT x IN (1,2,3,4,5,6,7,8,9),x NOT IN (1,2,3,4,5,6,7,8,9),x IN (1,2,3,4,5,6,7,8,NULL) FROM (VALUES (1),(10),(NULL)) t(x)", vec![]).await.unwrap(),
        [vec![V::Bool(true),V::Bool(false),V::Bool(true)],vec![V::Bool(false),V::Bool(true),V::Null],vec![V::Null,V::Null,V::Null]]);
    assert_eq!(query("SELECT x IN (x,2,3,4,5,6,7,8,NULL),x IN (ROW(x)) FROM (VALUES (1),(10),(NULL)) t(x)", vec![]).await.unwrap(),
        [vec![V::Bool(true),V::Bool(true)],vec![V::Bool(true),V::Bool(true)],vec![V::Null,V::Null]]);
    assert_eq!(query("SELECT l.x,r.y FROM (VALUES (1),(10)) l(x) JOIN (VALUES (1),(2)) r(y) ON l.x IN (r.y,2,3,4,5,6,7,8,9)", vec![]).await.unwrap(), [vec![V::I64(1),V::I64(1)]]);
    assert_eq!(query("SELECT ARRAY[1,NULL] IN (ARRAY[1,2],ARRAY[2,2],ARRAY[3,2],ARRAY[4,2],ARRAY[5,2],ARRAY[6,2],ARRAY[7,2],ARRAY[8,2],ARRAY[9,2])", vec![]).await.unwrap(), [vec![V::Null]]);
    assert_eq!(query("SELECT DATE '2020-01-01' IN ('2020-01-01','a','b','c','d','e','f','g'),DATE '2020-01-01' IN ('2020-01-01','a','b','c','d','e','f','g','h')", vec![]).await.unwrap(), [vec![V::Bool(true),V::Bool(false)]]);
}

#[tokio::test(start_paused = true)]
async fn relation_lists_construct_every_candidate_once_before_comparisons() {
    let start = tokio::time::Instant::now();
    assert_eq!(query("SELECT 1 IN (1,2,3,4,5,6,7,COALESCE(NULLIF(PG_SLEEP(1.0),NULL),8))", vec![]).await.unwrap(), [vec![V::Bool(true)]]);
    assert_eq!(tokio::time::Instant::now(), start);
    assert_eq!(query("SELECT x IN (1,2,3,4,5,6,7,8,COALESCE(NULLIF(PG_SLEEP(1.0),NULL),9)) FROM (VALUES (1),(2)) t(x)", vec![]).await.unwrap(), [vec![V::Bool(true)],vec![V::Bool(true)]]);
    assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(1));
    assert_eq!(query("SELECT x IN (x,2,3,4,5,6,7,8,COALESCE(NULLIF(PG_SLEEP(1.0),NULL),9)) FROM (VALUES (1),(2)) t(x)", vec![]).await.unwrap(), [vec![V::Bool(true)],vec![V::Bool(true)]]);
    assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(3));
    for sql in ["SELECT 1 IN (1,2,3,4,5,6,7,8,1/0)", "SELECT NULL IN (1,2,3,4,5,6,7,8,1/0)"] {
        assert_eq!(query(sql, vec![]).await.unwrap_err().code, "xtdb.expression/division-by-zero");
    }
}

#[tokio::test(start_paused = true)]
async fn short_list_equality_selects_continuations_before_demanding_payloads() {
    let start = tokio::time::Instant::now();
    for sql in [
        "SELECT (1/0) IN (NULL)",
        "SELECT NULL IN (1/0)",
        "SELECT NULL IN (CASE WHEN 1/0=0 THEN 1 ELSE 2 END)",
        "SELECT PG_SLEEP(60.0) IN (NULL)",
    ] { assert_eq!(query(sql, vec![]).await.unwrap(), [vec![V::Null]], "{sql}"); }
    assert_eq!(tokio::time::Instant::now(), start);
    let cast_error = query("SELECT CAST('bad' AS BIGINT)", vec![]).await.unwrap_err().code;
    assert_eq!(query("SELECT (1/0) IN (CASE WHEN CAST('bad' AS BIGINT)=0 THEN 1 ELSE 2 END)", vec![]).await.unwrap_err().code, cast_error);
    assert_eq!(query("SELECT (CASE WHEN TRUE THEN 1/0 ELSE NULL END) IN (NULL)", vec![]).await.unwrap_err().code, "xtdb.expression/division-by-zero");
}

#[tokio::test]
async fn uncorrelated_build_precedes_needle_and_correlated_build_follows_it() {
    let cast_error = query("SELECT CAST('bad' AS BIGINT)", vec![]).await.unwrap_err().code;
    assert_eq!(query("SELECT 1/0 IN (CAST('bad' AS BIGINT),2,3,4,5,6,7,8,9)", vec![]).await.unwrap_err().code, cast_error);
    assert_eq!(query("SELECT 1/0 IN (x+CAST('bad' AS BIGINT),2,3,4,5,6,7,8,9) FROM (VALUES (1)) t(x)", vec![]).await.unwrap_err().code, "xtdb.expression/division-by-zero");
    assert_eq!(query("SELECT 1 IN (1,2,3,4,5,6,7,8,CASE WHEN 1/0=0 THEN 9 ELSE 10 END)", vec![]).await.unwrap_err().code, "xtdb.expression/division-by-zero");
}

#[tokio::test]
async fn uncorrelated_marks_stop_comparison_but_correlated_values_project_the_whole_batch() {
    let needle = V::list(vec![V::Timestamp { value: i64::MAX, unit: TimeUnit::Second, timezone: Some("UTC".into()) }]);
    let later = V::list(vec![V::Timestamp { value: 0, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) }]);
    assert_eq!(query("SELECT $1 IN ($1,$2,NULL,NULL,NULL,NULL,NULL,NULL,NULL)", vec![needle.clone(),later.clone()]).await.unwrap(), [vec![V::Bool(true)]]);
    assert_eq!(query("SELECT x IN (x,$2,NULL,NULL,NULL,NULL,NULL,NULL,NULL) FROM (VALUES ($1)) t(x)", vec![needle,later]).await.unwrap_err().code, "xtdb.expression/overflow-error");
}

#[tokio::test]
async fn scalar_shared_marks_select_hash_candidates_before_temporal_comparison() {
    let needle = V::Timestamp { value: i64::MAX, unit: TimeUnit::Second, timezone: Some("UTC".into()) };
    let candidate = V::Timestamp { value: 0, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) };
    assert_eq!(query("SELECT $1 IN ($2,$2,$2,$2,$2,$2,$2,$2,$2)", vec![needle.clone(),candidate.clone()]).await.unwrap(), [vec![V::Bool(false)]]);
    assert_eq!(query("SELECT $1 IN ($2,$2,$2,$2,$2,$2,$2,$2,NULL)", vec![needle.clone(),candidate.clone()]).await.unwrap(), [vec![V::Null]]);
    assert_eq!(query("SELECT NULL IN ($1,$1,$1,$1,$1,$1,$1,$1,$1)", vec![needle.clone()]).await.unwrap(), [vec![V::Null]]);
    assert_eq!(query("SELECT x IN ($2,$2,$2,$2,$2,$2,$2,$2,x) FROM (VALUES ($1)) t(x)", vec![needle,candidate]).await.unwrap_err().code, "xtdb.expression/overflow-error");
}

#[tokio::test]
async fn scalar_shared_marks_keep_raw_uri_hash_selection_and_nested_object_equality() {
    let needle = V::Uri("HTTP://EXAMPLE.com/%ab".into());
    let candidate = V::Uri("http://example.com/%AB".into());
    assert_eq!(query("SELECT $1 IN ($2)", vec![needle.clone(),candidate.clone()]).await.unwrap(), [vec![V::Bool(true)]]);
    assert_eq!(query("SELECT $1 IN ($2,$2,$2,$2,$2,$2,$2,$2,$2)", vec![needle.clone(),candidate.clone()]).await.unwrap(), [vec![V::Bool(false)]]);
    assert_eq!(query("SELECT $1 IN ($2,$2,$2,$2,$2,$2,$2,$2,NULL)", vec![needle.clone(),candidate.clone()]).await.unwrap(), [vec![V::Null]]);
    assert_eq!(query("SELECT $1 IN ($2,$2,$2,$2,$2,$2,$2,$2,$2)", vec![V::list(vec![needle]),V::list(vec![candidate])]).await.unwrap(), [vec![V::Bool(true)]]);
}

#[tokio::test]
async fn spilled_scalar_shared_marks_select_raw_hash_candidates_before_comparing() {
    let cancellation = CancellationToken::new();
    let base = context(&cancellation, 1 << 27);
    let spilled = QueryContext::new(
        base.catalog.clone(), Arc::clone(&base.memory), Timestamp(0), "UTC",
        QueryOptions { join_build_rows: 0, ..base.options.clone() }, &cancellation,
    ).unwrap();
    let needle = V::Timestamp { value: i64::MAX, unit: TimeUnit::Second, timezone: Some("UTC".into()) };
    let candidate = V::Timestamp { value: 0, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) };
    assert_eq!(query_at(Arc::clone(&spilled), "SELECT $1 IN ($2,$2,$2,$2,$2,$2,$2,$2,$2)", vec![needle,candidate]).await.unwrap(), [vec![V::Bool(false)]]);
    let needle = V::Uri("HTTP://EXAMPLE.com/%ab".into());
    let candidate = V::Uri("http://example.com/%AB".into());
    assert_eq!(query_at(spilled, "SELECT $1 IN ($2,$2,$2,$2,$2,$2,$2,$2,NULL)", vec![needle,candidate]).await.unwrap(), [vec![V::Null]]);
}

#[test]
fn list_candidates_share_scalar_admission_and_row_arity_at_every_length() {
    let context = context(&CancellationToken::new(), 1 << 27);
    assert!(prepare("SELECT x IN ((SELECT 1)) FROM (VALUES (1)) t(x) GROUP BY x", &context.catalog, &PrepareOptions::default()).is_ok());
    for suffix in [",2", ",2,3,4,5,6,7,8,9"] {
        for candidate in ["COALESCE((SELECT 1),0)","MAX(x)","ROW_NUMBER() OVER ()"] {
            let sql = format!("SELECT x IN ({candidate}{suffix}) FROM (VALUES (1)) t(x) GROUP BY x");
            let error = prepare(&sql, &context.catalog, &PrepareOptions::default()).unwrap_err();
            assert!(error.message.as_deref().is_some_and(|message| message.contains("not allowed in this context")), "{sql}: {error}");
        }
    }
    for candidates in ["(1,2)","ROW(1,2)","ROW()","1,(2,3)"] {
        assert!(prepare(&format!("SELECT 1 IN ({candidates})"), &context.catalog, &PrepareOptions::default()).is_err());
    }
    assert!(prepare("SELECT 1 IN ()", &context.catalog, &PrepareOptions::default()).is_err());
    fn marks(relation: &Relation) -> usize { usize::from(matches!(relation.node(), RelationNode::Mark { .. })) + relation.children().iter().map(|child| marks(child)).sum::<usize>() }
    for (candidates, count) in [("1,2,3,4,5,6,7,8",0),("1,2,3,4,5,6,7,8,9",1),("ROW(1)",1)] {
        let Statement::Query(relation) = prepare(&format!("SELECT 1 IN ({candidates})"), &context.catalog, &PrepareOptions::default()).unwrap().statement else { panic!("query") };
        assert_eq!(marks(&relation), count);
        for expression in relation.expressions() { expression.visit(&mut |expr| assert!(!matches!(expr, Expr::In { .. }))); }
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_candidate_construction_and_spill_failure_release_partial_owners() {
    use std::{future::{Future, poll_fn}, task::Poll};
    let cancellation = CancellationToken::new();
    let context = context(&cancellation, 1 << 27);
    let mut query = Box::pin(query_at(Arc::clone(&context), "SELECT 1 IN (1,2,3,4,5,6,7,8,COALESCE(NULLIF(PG_SLEEP(60.0),NULL),9))", vec![]));
    poll_fn(|cx| { assert!(query.as_mut().poll(cx).is_pending()); Poll::Ready(()) }).await;
    assert!(context.memory.used_bytes() > 0);
    assert_eq!(context.spill_bytes(), 0);
    cancellation.cancel();
    assert!(query.await.is_err());
    assert_eq!(context.memory.used_bytes(), 0);
    assert_eq!(context.spill_bytes(), 0);
    let base = self::context(&CancellationToken::new(), 1);
    let limited = QueryContext::new(base.catalog.clone(), MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions { join_build_rows: 0, ..base.options.clone() }, &CancellationToken::new()).unwrap();
    assert_eq!(query_at(limited, "SELECT 1 IN (1,2,3,4,5,6,7,8,9)", vec![]).await.unwrap_err().code,"rtdb.query/spill-limit");
}
