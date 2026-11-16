use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::MemoryBudget;
use rtdb_node::{PreparedSql, ResultSignaturePolicy};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_tx::{SqlArguments, TxOp};
use rtdb_types::{Timestamp, Value};
use tokio_util::sync::CancellationToken;

#[test]
fn prepared_dml_requires_prefix_arguments_and_retains_full_source_sql() {
    let memory = MemoryBudget::new(1 << 25);
    let database = DatabaseSnapshot { partitions: Vec::new(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
    for (sql, values) in [
        ("UPDATE absent SET value=? WHERE _id=? RETURNING $99,?", vec![Value::I64(7), Value::I64(1)]),
        ("INSERT INTO absent RECORDS {_id:1} RETURNING ?,unknown_function(1)", vec![]),
    ] {
        let prepared = PreparedSql::prepare(sql, &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
        assert!(prepared.is_dml()); assert_eq!(prepared.parameter_count(), values.len());
        let arguments = if values.is_empty() { None } else { Some(SqlArguments::from_rows(vec![values], &memory).unwrap()) };
        let TxOp::Sql { sql: retained, .. } = prepared.dml_operation(arguments).unwrap() else { panic!("Expected SQL operation") };
        assert_eq!(retained, sql);
    }
    assert_eq!(memory.used_bytes(), 0);
}
