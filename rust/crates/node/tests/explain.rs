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
    assert_eq!(analyze.prepared_schema().unwrap().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["depth", "op", "attributes", "total_time", "self_time", "time_to_first_page", "page_count", "row_count", "pushdowns", "peak_memory_bytes"]);
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
