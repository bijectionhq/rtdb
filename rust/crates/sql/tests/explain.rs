use rtdb_types::StructFields;
use std::{collections::BTreeMap, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::{PartitionState, TransactionBuffer};
use rtdb_plan::{ExplainMode, Statement};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, StatementKind, parse, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Interval, Keyword, Result, TableRef, TimeUnit, Timestamp, TransactionKey, TxId, Value};
use tokio_util::sync::CancellationToken;

fn context(batch_rows: usize) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions { batch_rows, ..Default::default() }, &CancellationToken::new()).unwrap()
}
async fn query(context: &Arc<QueryContext>, sql: &str) -> Result<Vec<StructFields<Value>>> {
    let prepared = prepare(sql, &context.catalog, &PrepareOptions::default())?;
    let query = PreparedQuery::from_prepared(prepared, &context.memory)?;
    let mut cursor = query.execute(Arc::clone(context), vec![]).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() {
            rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field, array)| Ok((field.name().clone(), read_value(field, array.as_ref(), row)?))).collect::<Result<_>>()?);
        }
    }
    Ok(rows)
}
fn op(name: &str) -> Value { Value::Keyword(Keyword::from_name(name)) }

#[tokio::test]
async fn analyze_distinguishes_independent_scalar_join_and_correlated_apply() {
    let context = context(2);
    for (source, expected) in [
        ("EXPLAIN ANALYZE SELECT (SELECT 7) FROM (VALUES (1),(2)) t(n)", "single-join"),
        ("EXPLAIN ANALYZE SELECT (SELECT t.n + 7) FROM (VALUES (1),(2)) t(n)", "apply-single-join"),
    ] {
        let rows = query(&context, source).await.unwrap();
        assert!(rows.iter().any(|row| row.get("op") == Some(&op(expected))), "{source}: {rows:?}");
    }
}

#[tokio::test]
async fn explain_temporal_casts_retain_requested_precision_and_interval_fields() {
    let context = context(2);
    for (sql,options) in [
        ("EXPLAIN SELECT CAST('05:34:56.123456789' AS TIME(7)) AS v","{:precision 7}"),
        ("EXPLAIN SELECT CAST('1 10:10:10.111111111' AS INTERVAL DAY TO SECOND(7)) AS v","{:start-field \"DAY\", :end-field \"SECOND\", :leading-precision 2, :fractional-precision 7}"),
    ] {
        let rows = query(&context,sql).await.unwrap();
        let Value::Struct(explanation) = &rows[0]["explain"] else { panic!() };
        let Value::Utf8(project) = &explanation["project"] else { panic!() };
        assert!(project.contains(options),"{project}");
    }
}
fn duration(value: &Value) -> i64 { let Value::Duration { value, unit: TimeUnit::Microsecond } = value else { panic!("Expected duration in microseconds") }; *value }
fn depth(row: &StructFields<Value>) -> usize { let Value::Utf8(value) = &row["depth"] else { panic!("Expected depth") }; value.len() }

#[derive(Default)]
struct Samples(AtomicUsize);
impl rtdb_plan::ExecutionObserver for Samples {
    fn record_duration(&self, _: rtdb_plan::ExecutionTimer, _: std::time::Duration) { self.0.fetch_add(1, Ordering::SeqCst); }
    fn record_error(&self, _: rtdb_plan::ExecutionError) {}
}

#[tokio::test]
async fn analyze_retains_one_execution_sample_until_result_cursor_close() {
    let samples = Arc::new(Samples::default());
    let context = context(2).with_observer(Some(samples.clone()));
    let prepared = prepare("EXPLAIN ANALYZE SELECT 1", &context.catalog, &PrepareOptions::default()).unwrap();
    let query = PreparedQuery::from_prepared(prepared, &context.memory).unwrap();
    let mut cursor = query.execute(Arc::clone(&context), vec![]).await.unwrap();
    assert_eq!(samples.0.load(Ordering::SeqCst), 0);
    while cursor.next_batch().await.unwrap().is_some() {}
    assert_eq!(samples.0.load(Ordering::SeqCst), 0);
    drop(cursor);
    assert_eq!(samples.0.load(Ordering::SeqCst), 1);
}

#[test]
fn retained_preparation_reserves_filter_expression_payload_before_optimization() {
    let context = context(2);
    let text = "x".repeat(65_536);
    let prepared = prepare(&format!("SELECT 1 WHERE '{text}' = '{text}'"), &context.catalog, &PrepareOptions::default()).unwrap();
    let memory = MemoryBudget::new(1 << 20);
    assert!(PreparedQuery::from_prepared(prepared, &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn materialized_cte_prefix_retains_nested_multiple_and_quoted_name_scopes() {
    let context = context(2);
    for (sql, expected) in [
        ("WITH \"MATERIALIZED\" AS (SELECT 1 AS n) SELECT n FROM \"MATERIALIZED\"", 1),
        ("WITH MATERIALIZED a AS (SELECT 1 AS n), MATERIALIZED b AS (SELECT n + 1 AS n FROM a) SELECT n FROM b", 2),
        ("WITH MATERIALIZED a AS (WITH MATERIALIZED a AS (SELECT 7 AS n) SELECT n FROM a) SELECT n FROM a", 7),
    ] {
        assert_eq!(query(&context, sql).await.unwrap(), [BTreeMap::from([("n".into(), Value::I64(expected))])]);
        assert_eq!(context.spill_bytes(), 0);
        assert_eq!(context.memory.used_bytes(), 0);
    }
}

#[test]
fn materialized_cte_rejects_postfix_forms_in_every_lexical_scope() {
    for sql in [
        "WITH a AS MATERIALIZED (SELECT 1) SELECT * FROM a",
        "WITH a AS NOT MATERIALIZED (SELECT 1) SELECT * FROM a",
        "WITH MATERIALIZED a AS (SELECT 1), b AS MATERIALIZED (SELECT 2) SELECT * FROM b",
        "WITH MATERIALIZED a AS (WITH b AS MATERIALIZED (SELECT 1) SELECT * FROM b) SELECT * FROM a",
    ] { assert_eq!(rtdb_sql::parse(sql).unwrap_err().code, "xtdb/sql-error", "{sql}"); }
}

#[tokio::test]
async fn optimized_scan_predicate_runs_after_latest_visible_version_resolution() {
    let memory = MemoryBudget::new(1 << 27);
    let state = PartitionState::empty(Default::default()).unwrap();
    for (tx, value) in [(0, 1), (1, 2)] {
        let mut transaction = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(tx), system_time: Timestamp(tx + 1) }, Arc::clone(&memory)).unwrap();
        transaction.put(TableRef::new("public", "history"), StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::I64(value))]), Interval::ALL).unwrap();
        state.apply(&transaction.stage().unwrap()).unwrap();
    }
    let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let context = QueryContext::new(catalog, memory, Timestamp(3), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
    assert!(query(&context, "SELECT v FROM history WHERE v = 1").await.unwrap().is_empty());
    assert_eq!(query(&context, "SELECT v FROM history WHERE v = 2").await.unwrap(), [BTreeMap::from([("v".into(), Value::I64(2))])]);
}

#[tokio::test]
async fn api_test_explain_projection_rename_and_scan_predicate_describe_executed_columns() {
    let memory = MemoryBudget::new(1 << 27);
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut transaction = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(0), system_time: Timestamp(1) }, Arc::clone(&memory)).unwrap();
    transaction.put(TableRef::new("public", "foo"), StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::Utf8("one".into())), ("unused".into(), Value::I64(99))]), Interval::ALL).unwrap();
    state.apply(&transaction.stage().unwrap()).unwrap();
    let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let context = QueryContext::new(catalog, memory, Timestamp(1), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
    let rows = query(&context, "EXPLAIN SELECT v FROM foo WHERE _id = 1").await.unwrap();
    assert_eq!(rows.iter().map(|row| row["op"].clone()).collect::<Vec<_>>(), [op("project"), op("rename"), op("scan")]);
    let Value::Struct(project) = &rows[0]["explain"] else { panic!("Expected projection metadata") };
    assert_eq!(project["project"], Value::Utf8("[{v foo.1/v}]".into()));
    let Value::Struct(scan) = &rows[2]["explain"] else { panic!("Expected scan metadata") };
    assert_eq!(scan["table"], Value::Utf8("xtdb.public.foo".into()));
    assert_eq!(scan["columns"], Value::list(vec![Value::Utf8("v".into()), Value::Utf8("_id".into())]));
    assert_eq!(scan["predicates"], Value::list(vec![Value::Utf8("(== _id 1)".into())]));
    let present = |explain: &StructFields<Value>| explain.iter().filter(|(_, value)| !matches!(value, Value::Null | Value::Undefined)).map(|(name, _)| name.clone()).collect::<Vec<_>>();
    assert_eq!(present(project), ["project", "append?"], "keys follow each operator's reference map literal");
    assert_eq!(present(scan), ["columns", "table", "predicates"], "a key every rename declares precedes the scan's own");
    let measured = query(&context, "EXPLAIN ANALYZE SELECT v FROM foo WHERE _id = 1").await.unwrap();
    assert!(measured.iter().all(|row| row["row_count"] == Value::I64(1)));
    let Value::Struct(attributes) = &measured[2]["attributes"] else { panic!("Expected actual scan counters") };
    assert_eq!(attributes["scan_source"], Value::Utf8("public.foo".into()));
    assert!(matches!(attributes["scan_rows_read"], Value::I64(value) if value >= 1));
}

#[test]
fn sql_parser_explain_analyze_and_query_settings_remain_query_statements() {
    let context = context(2);
    for (sql, expected) in [
        ("EXPLAIN SELECT 1", ExplainMode::Plan),
        ("EXPLAIN ANALYZE SELECT 1", ExplainMode::Analyze),
        ("EXPLAIN ANALYZE SETTING CLOCK_TIME = TIMESTAMP '2000-01-01T00:00:00Z' SELECT CURRENT_TIMESTAMP", ExplainMode::Analyze),
    ] {
        assert_eq!(parse(sql).unwrap()[0].kind(), StatementKind::Query);
        assert!(matches!(prepare(sql, &context.catalog, &PrepareOptions::default()).unwrap().statement, Statement::Explain { mode, .. } if mode == expected));
    }
}

#[tokio::test]
async fn explain_plan_emits_nested_projection_attributes_without_executing_the_query() {
    let context = context(2);
    let rows = query(&context, "EXPLAIN SELECT 1 / 0 AS value").await.unwrap();
    assert_eq!(rows[0]["depth"], Value::Utf8("->".into()));
    assert_eq!(rows[0]["op"], op("project"));
    let Value::Struct(explanation) = &rows[0]["explain"] else { panic!("Expected structural explain metadata") };
    assert_eq!(explanation["append?"], Value::Bool(false));
    assert_eq!(explanation["project"], Value::Utf8("[{value (/ 1 0)}]".into()));
    assert!(query(&context, "EXPLAIN ANALYZE SELECT 1 / 0 AS value").await.is_err());
    assert_eq!(context.memory.used_bytes(), 0);
    assert_eq!(context.spill_bytes(), 0);
}

#[tokio::test]
async fn api_test_explain_analyze_counts_production_pages_and_subtracts_direct_children() {
    let context = context(2);
    let rows = query(&context, "EXPLAIN ANALYZE SELECT n FROM (VALUES (1), (2), (3)) AS t(n)").await.unwrap();
    assert_eq!(rows.iter().map(|row| &row["op"]).collect::<Vec<_>>(), [&op("project"), &op("rename"), &op("table")]);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row["page_count"], Value::I64(1));
        assert_eq!(row["row_count"], Value::I64(3));
        assert!(matches!(row["time_to_first_page"], Value::Duration { .. }));
        assert!(matches!(row["peak_memory_bytes"], Value::I64(value) if value > 0));
        let children = rows[index + 1..].iter().take_while(|child| depth(child) > depth(row)).filter(|child| depth(child) == depth(row) + 2);
        assert_eq!(duration(&row["self_time"]), duration(&row["total_time"]) - children.map(|child| duration(&child["total_time"])).sum::<i64>());
    }
    assert!(rows.windows(2).all(|rows| rows[0]["peak_memory_bytes"] == rows[1]["peak_memory_bytes"]));
    assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn emitted_sort_combines_order_and_fetch_at_the_production_cursor_boundary() {
    let context = context(2);
    let sql = "SELECT n FROM (VALUES (3), (1), (2)) AS t(n) ORDER BY n LIMIT 2";
    let plan = query(&context, &format!("EXPLAIN {sql}")).await.unwrap();
    let measured = query(&context, &format!("EXPLAIN ANALYZE {sql}")).await.unwrap();
    assert_eq!(plan.iter().map(|row| (&row["depth"], &row["op"])).collect::<Vec<_>>(), measured.iter().map(|row| (&row["depth"], &row["op"])).collect::<Vec<_>>());
    let sorts = plan.iter().filter(|row| row["op"] == op("sort")).collect::<Vec<_>>();
    assert_eq!(sorts.len(), 1);
    let Value::Struct(explanation) = &sorts[0]["explain"] else { panic!("Expected sort metadata") };
    assert_eq!(explanation["limit"], Value::Utf8("2".into()));
    assert!(matches!(explanation["order-specs"], Value::Utf8(_)));
    assert_eq!(measured[0]["row_count"], Value::I64(2));
}

#[tokio::test]
async fn api_test_unadvanced_limit_and_unused_materialized_binding_retain_zero_counters() {
    let context = context(2);
    for sql in [
        "EXPLAIN ANALYZE SELECT n FROM (VALUES (1), (2)) AS t(n) LIMIT 0",
        "EXPLAIN ANALYZE WITH MATERIALIZED unused AS (SELECT 1 / 0 AS n) SELECT 1 AS one",
    ] {
        let rows = query(&context, sql).await.unwrap();
        let unadvanced = rows.iter().filter(|row| row["time_to_first_page"] == Value::Null).collect::<Vec<_>>();
        assert!(!unadvanced.is_empty());
        for row in unadvanced { assert_eq!(row["page_count"], Value::I64(0)); assert_eq!(row["row_count"], Value::I64(0)); }
        assert_eq!(context.memory.used_bytes(), 0);
        assert_eq!(context.spill_bytes(), 0);
    }
}

#[tokio::test]
async fn materialized_reference_opens_before_limit_and_shares_one_replay_across_references() {
    let context = context(2);
    let prepared = prepare("WITH MATERIALIZED c AS (SELECT 1 / 0 AS n) SELECT n FROM c LIMIT 0", &context.catalog, &PrepareOptions::default()).unwrap();
    let query = PreparedQuery::from_prepared(prepared, &context.memory).unwrap();
    assert!(query.execute(Arc::clone(&context), vec![]).await.is_err());
    drop(query);
    assert_eq!(context.memory.used_bytes(), 0);
    let rows = self::query(&context, "EXPLAIN ANALYZE WITH MATERIALIZED c AS (SELECT n FROM (VALUES (1), (2)) AS t(n)) SELECT a.n, b.n AS other_n FROM c AS a CROSS JOIN c AS b").await.unwrap();
    assert_eq!(rows[0]["op"], op("let"));
    let references = rows.iter().filter(|row| row["op"] == op("relation")).collect::<Vec<_>>();
    assert_eq!(references.len(), 2);
    assert!(references.iter().all(|row| row["row_count"] == Value::I64(2)));
    let tables = rows.iter().filter(|row| row["op"] == op("table")).collect::<Vec<_>>();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0]["row_count"], Value::I64(2));
    assert!(context.spill_peak_bytes() > 0);
    assert_eq!(context.spill_bytes(), 0);
    assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn canceled_analyze_open_and_spill_failure_release_partial_materialization() {
    let context = context(1);
    let prepared = prepare("EXPLAIN ANALYZE SELECT 1", &context.catalog, &PrepareOptions::default()).unwrap();
    let query = PreparedQuery::from_prepared(prepared, &context.memory).unwrap();
    context.cancellation.cancel();
    let error = match query.execute(Arc::clone(&context), vec![]).await { Ok(_) => panic!("Canceled query opened"), Err(error) => error };
    assert_eq!(error.code, "xtdb/query-cancelled");
    drop(query);
    assert_eq!(context.memory.used_bytes(), 0);

    let base = self::context(1);
    let context = QueryContext::new(base.catalog.clone(), Arc::clone(&base.memory), Timestamp(0), "UTC", QueryOptions { spill_limit_bytes: 1, ..Default::default() }, &CancellationToken::new()).unwrap();
    assert!(self::query(&context, "WITH MATERIALIZED c AS (SELECT 1 AS n) SELECT n FROM c").await.is_err());
    assert_eq!(context.spill_bytes(), 0);
    assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn explain_uri_literals_use_java_ascii_rendering() {
    let rows = query(&context(2),"EXPLAIN SELECT URI 'HTTP://host/é' AS u").await.unwrap();
    let Value::Struct(explanation) = &rows[0]["explain"] else { panic!("explanation") };
    let Value::Utf8(project) = &explanation["project"] else { panic!("projection") };
    assert!(project.contains("#xt/uri \"HTTP://host/%C3%A9\""),"{project}");
}
