use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::PartitionState;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_tx::{ResolveContext, SourceTransaction, SqlArguments, SubmitOptions, TransactionAuthorization, TxOp, TxResolver, prepare_submission};
use rtdb_types::{Result, SourcePosition, Timestamp, Value};
use tokio_util::sync::CancellationToken;

struct Authorization;
impl TransactionAuthorization for Authorization {
    fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { Ok(()) }
}

#[tokio::test]
async fn ignore_returning_keys_3668_mutates_and_reads_the_same_transaction() {
    let memory = MemoryBudget::new(1 << 28);
    {
        let state = PartitionState::empty(Default::default()).unwrap();
        let context = ResolveContext { decision_clock: None, write_event_origins: false, placed_receipt_retirement: false, owned_witness_scope: None, world_constraints: None, database: "xtdb".into(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)), memory: Arc::clone(&memory), query_options: QueryOptions::default(), scan_accelerators: Default::default(), cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(Authorization), execution_observer: None, tracer: None };
        let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &memory).unwrap();
        let operations = [
            "INSERT INTO docs (_id,foo) VALUES (1,'bar') RETURNING * EXCLUDE foo",
            "INSERT INTO docs (_id,foo) VALUES (2,'bar'),(3,'wuf') RETURNING _id",
            "UPDATE docs SET foo='toto' WHERE _id=1 RETURNING _id+1,foo AS bar",
            "DELETE FROM docs WHERE _id=3 RETURNING *",
            "INSERT INTO docs RECORDS {_id:4,foo:'records'} RETURNING missing,unknown_function(1),1/0,?",
        ].into_iter().map(|sql| TxOp::Sql { sql: sql.into(), args: None }).collect();
        let source = SourceTransaction::new(operations, SubmitOptions::default(), &memory).unwrap();
        let resolved = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), source, &context).await.unwrap();
        assert!(resolved.result().committed(), "{:?}", resolved.result());
        drop(resolver.take_head().unwrap()); state.apply(resolved.writes()).unwrap();
        let arguments = SqlArguments::from_rows(vec![vec![Value::Utf8("parameter".into()), Value::I64(4)]], &memory).unwrap();
        let source = SourceTransaction::new(vec![TxOp::Sql { sql: "UPDATE docs SET foo=? WHERE _id=? RETURNING $99,?,1/0".into(), args: Some(arguments) }], SubmitOptions::default(), &memory).unwrap();
        let resolved = resolver.resolve_source(state.value().snapshot(), SourcePosition(1), Timestamp(101), source, &context).await.unwrap();
        assert!(resolved.result().committed(), "{:?}", resolved.result());
        drop(resolver.take_head().unwrap()); state.apply(resolved.writes()).unwrap();

        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::clone(&context.storage) })]), false).unwrap();
        let query_context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(101), "UTC", QueryOptions::default(), &context.cancellation).unwrap();
        let plan = rtdb_sql::prepare("SELECT _id,foo FROM docs ORDER BY _id", &query_context.catalog, &Default::default()).unwrap();
        let prepared = PreparedQuery::from_prepared(plan, &memory).unwrap();
        let mut cursor = prepared.execute(Arc::clone(&query_context), vec![]).await.unwrap();
        let mut rows = Vec::new();
        while let Some(page) = cursor.next_batch().await.unwrap() {
            for row in 0..page.batch().num_rows() {
                rows.push(page.batch().schema().fields().iter().zip(page.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<Vec<_>>>().unwrap());
            }
        }
        assert_eq!(rows, [vec![Value::I64(1), Value::Utf8("toto".into())], vec![Value::I64(2), Value::Utf8("bar".into())], vec![Value::I64(4), Value::Utf8("parameter".into())]]);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn ignored_returning_expressions_preserve_static_source_operations() {
    let memory = MemoryBudget::new(1 << 27);
    for prefix in ["INSERT INTO docs (_id,foo) VALUES (1,'bar')", "INSERT INTO docs RECORDS {_id:1,foo:'records'}"] {
        let expanded = |sql: String| prepare_submission(vec![TxOp::Sql { sql, args: None }], SubmitOptions::default(), "xtdb", &memory).unwrap().encode().unwrap();
        let expected = expanded(prefix.into());
        let actual = expanded(format!("{prefix} RETURNING missing,unknown_function(1),1/0,?"));
        assert_eq!(actual.as_slice(), expected.as_slice());
    }
    assert_eq!(memory.used_bytes(), 0);
}
