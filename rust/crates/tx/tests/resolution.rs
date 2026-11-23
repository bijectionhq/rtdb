use rtdb_types::StructFields;
use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_format::data::StoredOperation;
use rtdb_index::PartitionState;
use rtdb_query::QueryOptions;
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_tx::*;
use rtdb_types::{Anomaly, Interval, Result, SourcePosition, TableRef, Timestamp, Value};
use tokio_util::sync::CancellationToken;

struct Denied;
impl TransactionAuthorization for Denied {
    fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { Err(Anomaly::new(rtdb_types::AnomalyCategory::Forbidden, "xtdb/forbidden", "Not authorized")) }
}
fn context() -> ResolveContext {
    ResolveContext { database: "xtdb".into(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)), memory: MemoryBudget::new(1 << 29), query_options: QueryOptions { batch_rows: 1, ..Default::default() }, scan_accelerators: Default::default(), cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(Denied), execution_observer: None, tracer: None }
}
fn table() -> TableRef { TableRef::new("public", "docs") }
fn document(value: i64) -> StructFields<Value> { StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::I64(value)), ("keep".into(), Value::Utf8("retained".into()))]) }
fn request(ops: Vec<TxOp>, context: &ResolveContext) -> SourceTransaction { SourceTransaction::new(ops, SubmitOptions::default(), &context.memory).unwrap() }
fn put(context: &ResolveContext, value: i64, from: i64, to: i64) -> TxOp { TxOp::PutDocs { table: table(), documents: Documents::new(vec![document(value)], &context.memory).unwrap(), valid_from: Some(Timestamp(from)), valid_to: Some(Timestamp(to)) } }
fn sql(text: &str) -> TxOp { TxOp::Sql { sql: text.into(), args: None } }

#[tokio::test]
async fn external_writer_query_admits_sql_and_reads_its_own_staged_rows() {
    struct ReadBackWriter(Arc<MemoryBudget>);

    #[async_trait::async_trait]
    impl TransactionWriter for ReadBackWriter {
        async fn write(&mut self, transaction: &mut OpenTransaction<'_>) -> Result<WriterResult> {
            let baseline = self.0.used_bytes();
            let malformed = format!("SELECT '{}'; SELECT @", "é".repeat(64));
            let syntax = rtdb_sql::statement_syntax_estimate(malformed.len()).unwrap();
            let expected = rtdb_sql::parse_in(&malformed, &self.0).unwrap_err();
            assert_eq!(expected.code, "xtdb/sql-error");
            let held = self.0.reserve(self.0.limit_bytes() - baseline - syntax + 1)?;
            let error = transaction.open_query(&malformed, Vec::new()).await.err().unwrap();
            let retained = baseline + held.bytes();
            let message = format!("Cannot reserve {syntax} bytes from budget {} with {retained} retained", self.0.limit_bytes());
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            assert_eq!(error.message.as_deref(), Some(message.as_str()));
            assert_eq!(self.0.used_bytes(), retained);
            drop(held);
            assert_eq!(transaction.open_query(&malformed, Vec::new()).await.err().unwrap(), expected);
            assert_eq!(self.0.used_bytes(), baseline);

            transaction.append(table(), rtdb_format::data::StoredEvent {
                iid: Value::I64(1).entity_id()?.iid(), system_from: transaction.tx_key().system_time,
                valid_time: Interval::ALL, operation: StoredOperation::Put(document(7)), recency: None,
            })?;
            let staged = self.0.used_bytes();
            let mut query = transaction.open_query("SELECT v FROM docs", Vec::new()).await?;
            let page = query.next_batch().await?.unwrap();
            let batch = page.batch();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(rtdb_columnar::read_value(batch.schema().field(0), batch.column(0).as_ref(), 0)?, Value::I64(7));
            drop(page);
            assert!(query.next_batch().await?.is_none());
            drop(query);
            let cached = self.0.used_bytes();
            assert!(cached >= staged, "the transaction keeps the segment its reads see");
            let mut again = transaction.open_query("SELECT v FROM docs", Vec::new()).await?;
            while again.next_batch().await?.is_some() {}
            drop(again);
            assert_eq!(self.0.used_bytes(), cached, "a second read of unchanged writes retains nothing more");
            Ok(WriterResult::Committed { user_metadata: None })
        }
    }

    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let mut writer = ReadBackWriter(Arc::clone(&context.memory));
    let resolved = resolver.resolve_external(state.value().snapshot(), Timestamp(100), None, Some(vec![42]), &mut writer, &context).await.unwrap();
    assert!(resolved.result().committed());
    assert_eq!(resolved.external_token(), Some(&[42_u8][..]));
    assert_eq!(resolved.writes().tables()[&table()].batch().unwrap().len(), 1);
    assert!(!state.value().snapshot().tables().contains(&table()));
}

#[tokio::test]
async fn insert_queries_build_independent_subqueries_before_probing_an_empty_input() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    for source in [
        "INSERT INTO docs (_id, v) SELECT 1, (SELECT 1 / 0) WHERE FALSE",
        "INSERT INTO docs (_id, v) SELECT 2, ARRAY(SELECT 1 / 0) WHERE FALSE",
    ] {
        let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
        let resolved = resolver.resolve_source(
            state.value().snapshot(), SourcePosition(0), Timestamp(100), request(vec![sql(source)], &context), &context,
        ).await.unwrap();
        assert_eq!(resolved.result().error().unwrap().code, "xtdb.expression/division-by-zero", "{source}");
        assert!(!resolved.writes().tables().contains_key(&table()), "{source}");
    }
}

#[tokio::test]
async fn replayed_unsigned_argument_payloads_bind_signed_and_keep_source_bytes() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let args = SqlArguments::new(vec![rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::U64)], vec![vec![Value::U64(u64::MAX)]], &context.memory).unwrap();
    let source = request(vec![TxOp::Sql { sql: "INSERT INTO docs (_id, v) SELECT 1, ? + 2".into(), args: Some(args) }], &context);
    let encoded = source.encode().unwrap();
    let message = rtdb_format::envelope::SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
    let decoded = SourceTransaction::decode(&message, &context.memory).unwrap();
    assert_eq!(decoded.encode().unwrap().as_slice(), encoded.as_slice());
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let resolved = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), decoded, &context).await.unwrap();
    assert!(resolved.result().committed(), "{:?}", resolved.result());
    let batch = resolved.writes().tables()[&table()].batch().unwrap();
    let StoredOperation::Put(document) = batch.view().read_event(0).unwrap().operation else { panic!("Expected put") };
    assert_eq!(document["v"], Value::I64(1));
    let body = resolved.to_replica_body(&context.memory).unwrap();
    let rtdb_format::envelope::ReplicaBody::ResolvedTx(body) = body.body() else { panic!("Expected resolved payload") };
    let (replica, _) = decode_replica(body, None, state.value().snapshot().block_index(), &context.memory).unwrap();
    assert_eq!(replica.tables()[&table()].batch().unwrap().view().read_event(0).unwrap().operation, StoredOperation::Put(document));
}

#[tokio::test]
async fn transaction_trace_contains_one_sql_span_per_operation_and_skipped_zero_count() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut context = context();
    let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(Arc::clone(&context.memory), 16, false, true).unwrap();
    context.tracer = Some(tracer.clone());
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let arguments = SqlArguments::from_rows(vec![vec![Value::Bool(true)], vec![Value::Bool(true)]], &context.memory).unwrap();
    let request = request(vec![TxOp::Sql { sql: "ASSERT ?".into(), args: Some(arguments) }], &context);
    assert!(resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), request, &context).await.unwrap().result().committed());
    resolver.resolve_skipped(state.value().snapshot(), SourcePosition(1), Timestamp(101), &context).unwrap();
    tracer.close();
    let mut spans = Vec::new(); while let Some(span) = receiver.next().await { spans.push(span); }
    assert_eq!(spans.len(), 3);
    let sql = spans.iter().find(|span| span.name == "xtdb.transaction.sql").unwrap();
    let transaction = spans.iter().find(|span| span.attributes().any(|attribute| attribute == ("operations.count", "1"))).unwrap();
    assert_eq!(sql.parent, Some(transaction.context));
    assert!(sql.attributes().any(|attribute| attribute == ("query.text", "ASSERT ?")));
    assert!(spans.iter().any(|span| span.attributes().any(|attribute| attribute == ("operations.count", "0"))));
}

#[derive(Default)]
struct Metrics(std::sync::atomic::AtomicUsize);
impl rtdb_plan::ExecutionObserver for Metrics {
    fn record_duration(&self, _: rtdb_plan::ExecutionTimer, _: std::time::Duration) {}
    fn record_error(&self, error: rtdb_plan::ExecutionError) { assert_eq!(error, rtdb_plan::ExecutionError::Transaction); self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
}

#[tokio::test]
async fn ingestion_metrics_count_abort_and_invalid_time_but_exclude_skipped_transaction() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let metrics = Arc::new(Metrics::default()); let mut context = context(); context.execution_observer = Some(metrics.clone());
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), request(vec![sql("ASSERT FALSE")], &context), &context).await.unwrap();
    resolver.resolve_skipped(state.value().snapshot(), SourcePosition(1), Timestamp(101), &context).unwrap();
    let request = SourceTransaction::new(vec![], SubmitOptions { system_time: Some(Timestamp(100)), ..Default::default() }, &context.memory).unwrap();
    resolver.resolve_source(state.value().snapshot(), SourcePosition(2), Timestamp(102), request, &context).await.unwrap();
    assert_eq!(metrics.0.load(std::sync::atomic::Ordering::Relaxed), 2);
}

#[tokio::test]
async fn guarded_system_time_cannot_precede_the_source_log() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let metadata = UserMetadata::new(
        Value::Struct(StructFields::from([("rtdb_ordered_system_time".into(), Value::Bool(true))])),
        &context.memory,
    ).unwrap();
    let guarded = |time| SourceTransaction::new(
        vec![put(&context, 9, 0, i64::MAX)],
        SubmitOptions { system_time: Some(Timestamp(time)), user_metadata: Some(metadata.clone()), ..Default::default() },
        &context.memory,
    ).unwrap();

    let aborted = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), guarded(99), &context).await.unwrap();
    assert_eq!(aborted.result().error().unwrap().code, "rtdb/ordered-system-time");
    assert_eq!(aborted.result().tx_key().system_time, Timestamp(100));
    assert_eq!(aborted.writes().tables().keys().collect::<Vec<_>>(), [&TableRef::new("xt", "txs")]);

    let committed = resolver.resolve_source(state.value().snapshot(), SourcePosition(1), Timestamp(101), guarded(101), &context).await.unwrap();
    assert!(committed.result().committed());
    assert_eq!(committed.result().tx_key().system_time, Timestamp(101));
    assert_eq!(committed.writes().tables()[&table()].batch().unwrap().len(), 1);
}

#[tokio::test]
async fn skipped_transaction_stages_only_its_aborted_row_and_smooths_time() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let first = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), request(vec![put(&context, 9, 0, i64::MAX)], &context), &context).await.unwrap();
    let skipped = resolver.resolve_skipped(state.value().snapshot(), SourcePosition(1), Timestamp(50), &context).unwrap();
    assert_eq!(skipped.result().error().unwrap().code, "xtdb/skipped-tx");
    assert_eq!(skipped.result().tx_key().system_time, Timestamp(101));
    assert_eq!(skipped.writes().tables().keys().collect::<Vec<_>>(), [&TableRef::new("xt", "txs")]);
    assert_eq!(resolver.source_position(), SourcePosition(1));
    assert_eq!(state.value().live().latest_completed_tx(), None);
    assert_eq!(resolver.take_head().unwrap().result().tx_key(), first.result().tx_key());
    state.apply(first.writes()).unwrap();
    assert_eq!(resolver.take_head().unwrap().result().tx_key(), skipped.result().tx_key());
    state.apply(skipped.writes()).unwrap();
    assert_eq!(state.value().live().latest_completed_tx(), Some(skipped.result().tx_key()));
}

#[tokio::test]
async fn staged_txs_read_your_writes_across_a_batch_and_each_argument_row() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let first = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), request(vec![put(&context, 1, 0, i64::MAX)], &context), &context).await.unwrap();
    assert!(first.result().committed());
    assert!(!state.value().snapshot().tables().contains(&table()));
    let args = SqlArguments::from_rows(vec![vec![Value::I64(2)], vec![Value::I64(3)]], &context.memory).unwrap();
    let second = resolver.resolve_source(state.value().snapshot(), SourcePosition(1), Timestamp(101), request(vec![TxOp::Sql { sql: "UPDATE docs SET v = v + ?".into(), args: Some(args) }], &context), &context).await.unwrap();
    assert!(second.result().committed(), "{:?}", second.result());
    let batch = second.writes().tables()[&table()].batch().unwrap();
    let values: Vec<_> = (0..batch.len()).map(|row| match batch.view().read_event(row).unwrap().operation { StoredOperation::Put(document) => (document["v"].clone(), document["keep"].clone()), _ => panic!("Expected put") }).collect();
    assert_eq!(values, [(Value::I64(3), Value::Utf8("retained".into())), (Value::I64(6), Value::Utf8("retained".into()))]);
    assert_eq!(resolver.take_head().unwrap().result().tx_key(), first.result().tx_key());
    state.apply(first.writes()).unwrap();
    assert_eq!(resolver.resolving_snapshot(state.value().snapshot()).unwrap().tx_basis(), Some(second.result().tx_key()));
}

#[tokio::test]
async fn indexer_cleans_up_aborted_transactions_2489_and_advances_the_clock() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let aborted = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), request(vec![put(&context, 1, 0, i64::MAX), sql("ASSERT FALSE")], &context), &context).await.unwrap();
    assert_eq!(aborted.result().error().unwrap().code, "xtdb/assert-failed");
    assert_eq!(aborted.writes().tables().keys().collect::<Vec<_>>(), [&TableRef::new("xt", "txs")]);
    let next = resolver.resolve_source(state.value().snapshot(), SourcePosition(1), Timestamp(50), request(vec![], &context), &context).await.unwrap();
    assert_eq!(next.result().tx_key().system_time, Timestamp(101));
    let request = SourceTransaction::new(vec![], SubmitOptions { system_time: Some(Timestamp(101)), ..Default::default() }, &context.memory).unwrap();
    let invalid = resolver.resolve_source(state.value().snapshot(), SourcePosition(2), Timestamp(50), request, &context).await.unwrap();
    assert_eq!(invalid.result().error().unwrap().code, "invalid-system-time");
    assert_eq!(invalid.result().tx_key().system_time, Timestamp(102));
}

#[tokio::test]
async fn patch_operator_3879_fills_both_gaps_and_preserves_null_rhs_fields() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), request(vec![put(&context, 7, 10, 20)], &context), &context).await.unwrap();
    let patch = TxOp::PatchDocs { table: table(), documents: Documents::new(vec![StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::Null), ("added".into(), Value::Bool(true))])], &context.memory).unwrap(), valid_from: Some(Timestamp(0)), valid_to: Some(Timestamp(30)) };
    let resolved = resolver.resolve_source(state.value().snapshot(), SourcePosition(1), Timestamp(101), request(vec![patch], &context), &context).await.unwrap();
    assert!(resolved.result().committed(), "{:?}", resolved.result());
    let batch = resolved.writes().tables()[&table()].batch().unwrap();
    let mut events = (0..batch.len()).map(|row| batch.view().read_event(row).unwrap()).collect::<Vec<_>>();
    events.sort_by_key(|event| event.valid_time.lower());
    assert_eq!(events.iter().map(|event| event.valid_time).collect::<Vec<_>>(), [Interval::from_micros(0, 10).unwrap(), Interval::from_micros(10, 20).unwrap(), Interval::from_micros(20, 30).unwrap()]);
    for (index, event) in events.iter().enumerate() {
        let StoredOperation::Put(document) = &event.operation else { panic!("Expected patch put") };
        assert_eq!(document["added"], Value::Bool(true));
        assert_eq!(document["v"], if index == 1 { Value::I64(7) } else { Value::Null });
    }
}

#[tokio::test]
async fn staged_empty_create_table_visible_across_a_batch_5507() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let declared = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), request(vec![sql("CREATE TABLE empty_table (_id, v)")], &context), &context).await.unwrap();
    assert!(declared.result().committed(), "{:?}", declared.result());
    let empty = TableRef::new("public", "empty_table");
    assert_eq!(declared.writes().tables()[&empty].row_count(), 0);
    let next = resolver.resolve_source(state.value().snapshot(), SourcePosition(1), Timestamp(101), request(vec![sql("INSERT INTO untouched (_id, v) SELECT _id, v FROM empty_table")], &context), &context).await.unwrap();
    assert!(next.result().committed(), "{:?}", next.result());
    assert!(!next.writes().tables().contains_key(&TableRef::new("public", "untouched")));
    let body = declared.to_replica_body(&context.memory).unwrap();
    let rtdb_format::envelope::ReplicaBody::ResolvedTx(body) = body.body() else { panic!("Expected resolved payload") };
    let (decoded, result) = decode_replica(body, None, state.value().snapshot().block_index(), &context.memory).unwrap();
    assert_eq!(&result, declared.result());
    assert_eq!(decoded.tables()[&empty].column_types(), declared.writes().tables()[&empty].column_types());
}

#[tokio::test]
async fn explicit_empty_sql_batch_executes_zero_times_and_absent_arguments_abort() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let args = SqlArguments::new(vec![rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::I64)], vec![], &context.memory).unwrap();
    let empty = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), request(vec![TxOp::Sql { sql: "INSERT INTO docs (_id) VALUES (?)".into(), args: Some(args) }], &context), &context).await.unwrap();
    assert!(empty.result().committed());
    assert!(!empty.writes().tables().contains_key(&table()));
    let missing = resolver.resolve_source(state.value().snapshot(), SourcePosition(1), Timestamp(101), request(vec![sql("INSERT INTO docs (_id) VALUES (?)")], &context), &context).await.unwrap();
    assert_eq!(missing.result().error().unwrap().code, "xtdb.indexer/missing-sql-args");
}

#[tokio::test]
async fn grant_and_revoke_require_explicit_authorization() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let denied = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), request(vec![sql("GRANT reader TO alice")], &context), &context).await.unwrap();
    assert_eq!(denied.result().error().unwrap().category, rtdb_types::AnomalyCategory::Forbidden);
    assert!(!denied.writes().tables().contains_key(&TableRef::new("xt", "role_membership")));
}

#[tokio::test]
async fn raw_decimal_cells_survive_source_replay_resolution_sum_and_replica_staging() {
    use rtdb_types::DecimalValue;
    for precision in [-1, 1, 256, i32::MAX] {
        let state = PartitionState::empty(Default::default()).unwrap();
        let context = context();
        let value = Value::Decimal(DecimalValue::new(18i128.to_be_bytes().to_vec(), precision, 129, 128).unwrap());
        let documents = Documents::new([1, 2].into_iter().map(|id| StructFields::from([("_id".into(), Value::I64(id)), ("v".into(), value.clone())])).collect(), &context.memory).unwrap();
        let source = request(vec![
            TxOp::PutDocs { table: table(), documents, valid_from: None, valid_to: None },
            sql("INSERT INTO sums (_id, v) SELECT 1, SUM(v) FROM docs"),
        ], &context);
        let encoded = source.encode().unwrap();
        let message = rtdb_format::envelope::SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        let decoded = SourceTransaction::decode(&message, &context.memory).unwrap();
        let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
        let resolved = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), decoded, &context).await.unwrap();
        assert!(resolved.result().committed(), "{:?}", resolved.result());
        let docs = resolved.writes().tables()[&table()].batch().unwrap();
        assert_eq!(docs.len(), 2);
        for row in 0..docs.len() {
            let StoredOperation::Put(document) = docs.view().read_event(row).unwrap().operation else { panic!("Expected put") };
            assert_eq!(document["v"], value);
        }
        let sums = TableRef::new("public", "sums");
        let total = resolved.writes().tables()[&sums].batch().unwrap();
        let StoredOperation::Put(document) = total.view().read_event(0).unwrap().operation else { panic!("Expected sum put") };
        assert_eq!(document["v"], Value::Decimal(DecimalValue::new(36i128.to_be_bytes().to_vec(), precision, 129, 128).unwrap()));
        let body = resolved.to_replica_body(&context.memory).unwrap();
        let rtdb_format::envelope::ReplicaBody::ResolvedTx(body) = body.body() else { panic!("Expected resolved payload") };
        let (replica, result) = decode_replica(body, None, state.value().snapshot().block_index(), &context.memory).unwrap();
        assert_eq!(&result, resolved.result());
        for table in [table(), sums] {
            let expected = resolved.writes().tables()[&table].batch().unwrap();
            let actual = replica.tables()[&table].batch().unwrap();
            assert_eq!(actual.put_types(), expected.put_types());
            for row in 0..expected.len() { assert_eq!(actual.view().read_event(row).unwrap(), expected.view().read_event(row).unwrap()); }
            let events = (0..actual.len()).map(|row| actual.view().read_event(row).unwrap()).collect::<Vec<_>>();
            let flushed = rtdb_index::IndexBatch::copy_events_with_put_schema(&events, actual.put_types(), &context.memory).unwrap();
            for row in 0..actual.len() { assert_eq!(flushed.view().read_event(row).unwrap(), actual.view().read_event(row).unwrap()); }
        }
        state.apply(&replica).unwrap();
        assert!(state.value().snapshot().tables().contains(&TableRef::new("public", "sums")));
    }
}

#[tokio::test]
async fn compiled_values_rejects_raw_decimal_arguments_at_the_expression_writer() {
    for precision in [-1, 0, 1] {
        let state = PartitionState::empty(Default::default()).unwrap();
        let context = context();
        let value = Value::Decimal(rtdb_types::DecimalValue::new(18i128.to_be_bytes().to_vec(), precision, 129, 128).unwrap());
        let arguments = SqlArguments::new(vec![value.logical_type()], vec![vec![value]], &context.memory).unwrap();
        let source = request(vec![TxOp::Sql { sql: "INSERT INTO docs (_id, v) VALUES (2, ?)".into(), args: Some(arguments) }], &context);
        let encoded = source.encode().unwrap();
        let message = rtdb_format::envelope::SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        let decoded = SourceTransaction::decode(&message, &context.memory).unwrap();
        let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
        let resolved = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), decoded, &context).await.unwrap();
        assert_eq!(resolved.result().error().unwrap().code, "rtdb/decimal-precision");
        assert!(!resolved.writes().tables().contains_key(&table()));
    }
}

#[tokio::test]
async fn resolved_series_dml_uses_the_resolving_nodes_endpoint_policy() {
    for end_exclusive in [false,true] {
        let state = PartitionState::empty(Default::default()).unwrap();
        let mut context = context();
        context.query_options.generate_series_end_exclusive = end_exclusive;
        let source = request(vec![sql("INSERT INTO docs (_id,v) SELECT 1,GENERATE_SERIES(1,3)")],&context);
        let mut resolver = TxResolver::new(None,SourcePosition(-1),None,&context.memory).unwrap();
        let resolved = resolver.resolve_source(state.value().snapshot(),SourcePosition(0),Timestamp(100),source,&context).await.unwrap();
        assert!(resolved.result().committed(),"{:?}",resolved.result());
        let batch = resolved.writes().tables()[&table()].batch().unwrap();
        let StoredOperation::Put(document) = batch.view().read_event(0).unwrap().operation else { panic!("put") };
        assert_eq!(document["v"],Value::list((1..if end_exclusive {3} else {4}).map(Value::I64).collect()));
    }
}
