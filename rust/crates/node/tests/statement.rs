use std::collections::BTreeMap;
use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::{PartitionState, TransactionBuffer};
use rtdb_node::{PreparedSql, ResultSignaturePolicy};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions};
use rtdb_sql::PrepareOptions;
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_tx::{SqlArguments, TxOp};
use rtdb_types::{Interval, LogicalType, PgType, ScalarType, TableRef, Timestamp, TransactionKey, TxId, Value};
use tokio_util::sync::CancellationToken;

fn context(state: Option<&PartitionState>, memory: &Arc<MemoryBudget>) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: state.map(|state| vec![state.value().snapshot()]).unwrap_or_default(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, Arc::clone(memory), Timestamp(100), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap()
}
fn write(state: &PartitionState, tx: i64, value: Value, memory: &Arc<MemoryBudget>) {
    let mut buffer = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(tx), system_time: Timestamp(tx) }, Arc::clone(memory)).unwrap();
    buffer.put(TableRef::new("public", "docs"), BTreeMap::from([("_id".into(), Value::I64(1)), ("v".into(), value)]), Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
    state.apply(&buffer.stage().unwrap()).unwrap();
}

#[tokio::test]
async fn adbc_prepared_parameters_are_unnamed_and_each_execution_binds_actual_types() {
    let memory = MemoryBudget::new(1 << 26);
    let context = context(None, &memory);
    let statement = PreparedSql::prepare("SELECT $1 AS value", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    assert_eq!(statement.parameter_count(), 1);
    assert_eq!(statement.parameter_schema().unwrap().fields()[0].name, "");
    assert_eq!(statement.parameter_schema().unwrap().fields()[0].data_type, LogicalType::Null);
    for value in [Value::I32(7), Value::Utf8("next".into())] {
        let mut cursor = statement.open_query(Arc::clone(&context), vec![value.clone()]).unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), value);
    }
}

#[tokio::test]
async fn pgwire_prepared_query_out_of_date_detects_current_snapshot_type_changes() {
    let memory = MemoryBudget::new(1 << 27);
    let state = PartitionState::empty(Default::default()).unwrap();
    write(&state, 1, Value::I32(1), &memory);
    let initial = context(Some(&state), &memory);
    let statement = PreparedSql::prepare_postgres("SELECT v FROM docs", &initial, vec!["public".into()], &[]).unwrap();
    assert_eq!(statement.postgres_types().unwrap(), &[PgType::Int4]);
    write(&state, 2, Value::I64(2), &memory);
    let updated = context(Some(&state), &memory);
    let error = match statement.open_query(updated, vec![]) { Ok(_) => panic!("accepted changed result type"), Err(error) => error };
    assert_eq!(error.code, "xtdb/prepared-query-out-of-date");
    let mut pinned = statement.open_query(initial, vec![]).unwrap();
    assert!(pinned.next_batch().await.unwrap().is_some());
}

#[test]
fn prepared_dml_defers_table_binding_and_preserves_empty_batch() {
    let memory = MemoryBudget::new(1 << 25);
    let context = context(None, &memory);
    let statement = PreparedSql::prepare("UPDATE not_yet_created SET value = $1", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let args = SqlArguments::new(vec![LogicalType::Scalar(ScalarType::I64)], vec![], &memory).unwrap();
    let TxOp::Sql { args: Some(args), .. } = statement.dml_operation(Some(args)).unwrap() else { panic!("empty batch was lost") };
    assert_eq!(args.row_count(), 0);
    assert!(statement.dml_operation(None).is_err());
}

#[test]
fn prepared_handle_clones_share_metadata_reservation() {
    let memory = MemoryBudget::new(1 << 25);
    let context = context(None, &memory);
    let baseline = memory.used_bytes();
    let statement = PreparedSql::prepare("SELECT 1", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let used = memory.used_bytes();
    assert!(used > baseline);
    let retained = statement.clone();
    drop(statement);
    assert_eq!(memory.used_bytes(), used);
    drop(retained);
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn postgres_value_dependent_parameter_publishes_default_then_rebinds_unnest() {
    let memory = MemoryBudget::new(1 << 25);
    let context = context(None, &memory);
    let statement = PreparedSql::prepare_postgres("SELECT * FROM UNNEST($1) AS items(value)", &context, vec!["public".into()], &[114]).unwrap();
    assert_eq!(statement.postgres_types().unwrap(), &[PgType::Default]);
    let mut cursor = statement.open_query(context, vec![Value::List(vec![Value::I64(1)])]).unwrap();
    assert_eq!(cursor.next_batch().await.unwrap().unwrap().batch().num_rows(), 1);
}
