use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_node::{PreparedSql, ResultSignaturePolicy};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Keyword, Timestamp, Value};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap()
}

#[tokio::test]
async fn shared_preparation_exposes_explain_schema_and_analyze_executes_during_open() {
    let context = context();
    let plan = PreparedSql::prepare("EXPLAIN SELECT $1 AS value", &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
    assert_eq!(plan.prepared_schema().unwrap().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["depth", "op", "explain"]);
    let mut cursor = plan.open_query(Arc::clone(&context), vec![Value::I64(1)]).await.unwrap();
    let batch = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(read_value(batch.batch().schema().field(1), batch.batch().column(1).as_ref(), 0).unwrap(), Value::Keyword(Keyword::from_name("project")));
    drop(batch); drop(cursor); drop(plan);

    let analyze = PreparedSql::prepare_postgres("EXPLAIN ANALYZE SELECT 1 / $1 AS value", &context, vec!["public".into()], &[20]).unwrap();
    assert_eq!(analyze.prepared_schema().unwrap().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["depth", "op", "attributes", "total_time", "self_time", "time_to_first_page", "page_count", "row_count", "pushdowns", "peak_memory_bytes", "query_peak_memory_bytes"]);
    assert!(analyze.open_query(Arc::clone(&context), vec![Value::I64(0)]).await.is_err());
    let mut cursor = analyze.open_query(Arc::clone(&context), vec![Value::I64(1)]).await.unwrap();
    assert!(cursor.next_batch().await.unwrap().is_some());
    drop(cursor); drop(analyze);
    assert_eq!(context.memory.used_bytes(), 0);
    assert_eq!(context.spill_bytes(), 0);
}

#[tokio::test]
async fn explain_description_and_open_check_aggregate_factory_types() {
    let context = context();
    let invalid = PreparedSql::prepare("EXPLAIN SELECT MIN(v) FROM (VALUES (1), ('a')) AS t(v)", &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let error = invalid.result_schema(&context, &[]).err().unwrap();
    assert_eq!(error.code, "xtdb.group-by/incomparable-min-max-types");
    assert_eq!(invalid.open_query(Arc::clone(&context), vec![]).await.err().unwrap(), error);

    let valid = PreparedSql::prepare("EXPLAIN SELECT MIN(v) FROM (VALUES (1)) AS t(v)", &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let described = valid.result_schema(&context, &[]).unwrap();
    let mut cursor = valid.open_query(context, vec![]).await.unwrap();
    assert_eq!(described.schema(), cursor.schema());
    assert!(cursor.next_batch().await.unwrap().is_some());
}

async fn operators(sql: &str, context: &Arc<QueryContext>) -> Vec<(Value, Value)> {
    let prepared = PreparedSql::prepare(sql, context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = prepared.open_query(Arc::clone(context), vec![]).await.unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        let schema = batch.batch().schema();
        for row in 0..batch.batch().num_rows() {
            let value = |index: usize| read_value(schema.field(index), batch.batch().column(index).as_ref(), row).unwrap();
            rows.push((value(0), value(1)));
        }
    }
    rows
}

#[tokio::test]
async fn xtdb_api_test_explain_analyze_covers_the_whole_plan_including_apply_dependents() {
    let context = context();
    for (bounds, expected) in [("", "single-join"), (" ORDER BY b.y LIMIT 1", "apply-single-join")] {
        let query = format!("SELECT a.x, (SELECT b.y FROM (VALUES (1)) AS b(y) WHERE b.y = a.x{bounds}) AS y FROM (VALUES (1), (2)) AS a(x)");
        let explained = operators(&format!("EXPLAIN {query}"), &context).await;
        assert!(explained.iter().any(|(_, op)| matches!(op, Value::Keyword(op) if op.as_str() == expected)), "{explained:?}");
        assert_eq!(explained.iter().filter(|(_, op)| matches!(op, Value::Keyword(op) if op.as_str() == "table")).count(), 2);
        assert_eq!(explained, operators(&format!("EXPLAIN ANALYZE {query}"), &context).await);
    }
}

#[tokio::test]
async fn plain_explain_emits_types_without_opening_a_values_table() {
    let context = context();
    let prepared = PreparedSql::prepare("EXPLAIN SELECT v FROM (VALUES (1 / 0)) AS t(v)", &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let described = prepared.result_schema(&context, &[]).unwrap();
    let mut cursor = prepared.open_query(context, vec![]).await.unwrap();
    assert_eq!(described.schema(), cursor.schema());
    assert!(cursor.next_batch().await.unwrap().is_some());
}

#[tokio::test]
async fn prepared_explain_keeps_bound_character_length_forms_across_rebinds() {
    let context = context();
    let prepared = {
        let source = String::from(
            "EXPLAIN SELECT CHAR_LENGTH($1) AS nchars, CHARACTER_LENGTH($1 USING CHARACTERS) AS explicit_chars, \
             CHAR_LENGTH($2 USING OCTETS) AS nbytes, OCTET_LENGTH($2) AS generic",
        );
        PreparedSql::prepare(&source, &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap()
    };
    let expected = Value::Utf8(
        "[{nchars (character-length ?_0)} {explicit_chars (character-length ?_0)} \
         {nbytes (octet-length ?_1)} {generic (octet_length ?_1)}]".into(),
    );

    for second in [Value::Utf8("é😀".into()), Value::Binary(vec![0, 255]), Value::Null] {
        let arguments = vec![Value::Utf8("é😀".into()), second];
        let types = arguments.iter().map(Value::logical_type).collect::<Vec<_>>();
        let described = prepared.result_schema(&context, &types).unwrap();
        let mut cursor = prepared.open_query(Arc::clone(&context), arguments).await.unwrap();
        assert_eq!(cursor.schema(), described.schema());
        let batch = cursor.next_batch().await.unwrap().unwrap();
        let explanation = read_value(batch.batch().schema().field(2), batch.batch().column(2).as_ref(), 0).unwrap();
        let Value::Struct(explanation) = explanation else { panic!("Expected projection metadata") };
        assert_eq!(explanation["project"], expected);
    }
    drop(prepared);
    assert_eq!(context.memory.used_bytes(), 0);
}
