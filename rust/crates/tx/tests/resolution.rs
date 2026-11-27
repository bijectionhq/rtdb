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
    ResolveContext { footprint_grants: Default::default(), decision_clock: None, write_event_origins: false, placed_receipt_retirement: false, owned_witness_scope: None, world_constraints: None, database: "xtdb".into(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)), memory: MemoryBudget::new(1 << 29), query_options: QueryOptions { batch_rows: 1, ..Default::default() }, scan_accelerators: Default::default(), cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(Denied), execution_observer: None, tracer: None }
}
fn table() -> TableRef { TableRef::new("public", "docs") }
fn document(value: i64) -> StructFields<Value> { StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::I64(value)), ("keep".into(), Value::Utf8("retained".into()))]) }
fn request(ops: Vec<TxOp>, context: &ResolveContext) -> SourceTransaction { SourceTransaction::new(ops, SubmitOptions::default(), &context.memory).unwrap() }
fn put(context: &ResolveContext, value: i64, from: i64, to: i64) -> TxOp { TxOp::PutDocs { table: table(), documents: Documents::new(vec![document(value)], &context.memory).unwrap(), valid_from: Some(Timestamp(from)), valid_to: Some(Timestamp(to)) } }
fn sql(text: &str) -> TxOp { TxOp::Sql { sql: text.into(), args: None } }

#[derive(Default)]
struct QueryRuns(std::sync::atomic::AtomicUsize);
impl rtdb_plan::Tracer for QueryRuns {
    fn query_enabled(&self) -> bool { true }
    fn transaction_enabled(&self) -> bool { false }
    fn start(&self, _: Option<rtdb_plan::SpanContext>, name: &str, _: &[(&str, rtdb_plan::TraceValue<'_>)])
        -> Option<Box<dyn rtdb_plan::TraceSpan>> {
        if name == "xtdb.query" { self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
        None
    }
}

#[tokio::test]
async fn a_static_insert_preserves_interleaved_assertion_order_for_one_identity() {
    use rtdb_format::envelope::SourceMessage;

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let source = prepare_submission(vec![
        sql("INSERT INTO docs (_id, v, _valid_from, _valid_to) VALUES
            (1, 10, TIMESTAMP '2020-01-01Z', TIMESTAMP '2020-04-01Z'),
            (1, 20, TIMESTAMP '2020-02-01Z', TIMESTAMP '2020-03-01Z'),
            (1, 30, TIMESTAMP '2020-01-01Z', TIMESTAMP '2020-04-01Z')"),
        sql("ASSERT (SELECT v FROM docs FOR VALID_TIME AS OF TIMESTAMP '2020-02-02Z' WHERE _id = 1) = 30"),
    ], SubmitOptions::default(), "xtdb", &context.memory).unwrap();
    let encoded = source.encode().unwrap();
    let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
    let decoded = SourceTransaction::decode(&message, &context.memory).unwrap();
    let resolved = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), decoded, &context).await.unwrap();
    assert!(resolved.result().committed(), "{:?}", resolved.result().error());
}

#[tokio::test]
async fn ordinary_sql_asserts_the_ordered_ceiling_and_places_offsets_from_the_same_time() {
    use rtdb_format::envelope::SourceMessage;

    let context = context();
    let source = prepare_submission(vec![
        sql("ASSERT CURRENT_TIMESTAMP < TIMESTAMP '1970-01-01T00:00:00.000102Z'"),
        sql("INSERT INTO docs (_id, v, _valid_from, _valid_to)
            VALUES ('at-' || CAST(CURRENT_TIMESTAMP + INTERVAL '1' SECOND AS VARCHAR),
                    CURRENT_TIMESTAMP + INTERVAL '1' SECOND,
                    CURRENT_TIMESTAMP + INTERVAL '2' SECOND,
                    CURRENT_TIMESTAMP + INTERVAL '3' SECOND)"),
    ], SubmitOptions::default(), "xtdb", &context.memory).unwrap();
    let encoded = source.encode().unwrap();
    let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
    let replay = || SourceTransaction::decode(&message, &context.memory).unwrap();

    for previous in [100, 101] {
        let state = PartitionState::empty(Default::default()).unwrap();
        let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
        resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(previous), request(vec![], &context), &context).await.unwrap();
        state.apply(resolver.take_head().unwrap().writes()).unwrap();
        let result = resolver.resolve_source(state.value().snapshot(), SourcePosition(1), Timestamp(90), replay(), &context).await.unwrap();
        assert_eq!(result.result().tx_key().system_time, Timestamp(previous + 1));
        if previous == 101 {
            assert_eq!(result.result().error().unwrap().code, "xtdb/assert-failed");
            assert!(!result.writes().tables().contains_key(&table()));
            continue;
        }
        assert!(result.result().committed(), "{:?}", result.result().error());
        let event = result.writes().tables()[&table()].batch().unwrap().view().read_event(0).unwrap();
        assert_eq!(event.valid_time, Interval::from_micros(2_000_101, 3_000_101).unwrap());
        let StoredOperation::Put(row) = event.operation else { panic!("Expected a put"); };
        let Value::Utf8(id) = &row["_id"] else { panic!("Expected an encoded key"); };
        let expected = Timestamp(1_000_101);
        assert_eq!(rtdb_types::parse_sql_timestamp_instant(id.strip_prefix("at-").unwrap(), "UTC").unwrap().as_micros().unwrap(), expected);
        assert_eq!(rtdb_expr::timestamp_micros(&row["v"], "UTC").unwrap(), expected);
    }
}

#[tokio::test]
async fn a_supplied_system_time_commits_only_strictly_after_its_predecessor_even_ahead_of_the_log() {
    use rtdb_format::envelope::SourceMessage;

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let at = |value, time: Option<i64>| {
        let source = SourceTransaction::new(vec![put(&context, value, 0, i64::MAX)], SubmitOptions { system_time: time.map(Timestamp), ..Default::default() }, &context.memory).unwrap();
        let encoded = source.encode().unwrap();
        SourceTransaction::decode(&SourceMessage::decode(encoded.as_slice()).unwrap().unwrap(), &context.memory).unwrap()
    };
    // As in XTDB: a supplied time later than the log's append time commits, and the transactions after it take
    // system times one tick past it until the log catches up; only a time that does not advance aborts.
    for (position, appended, requested, outcome, system_time) in [
        (0, 100, Some(50), None, 50),
        (1, 100, Some(60), None, 60),
        (2, 100, Some(60), Some("invalid-system-time"), 100),
        (3, 150, Some(99), Some("invalid-system-time"), 150),
        (4, 200, Some(1_000), None, 1_000),
        (5, 250, Some(1_000), Some("invalid-system-time"), 1_001),
        (6, 300, None, None, 1_002),
        (7, 350, Some(1_003), None, 1_003),
        (8, 2_000, None, None, 2_000),
    ] {
        let resolved = resolver.resolve_source(state.value().snapshot(), SourcePosition(position), Timestamp(appended), at(position, requested), &context).await.unwrap();
        assert_eq!(resolved.result().error().map(|error| error.code.as_str()), outcome, "position {position}");
        assert_eq!(resolved.result().tx_key().system_time, Timestamp(system_time), "position {position}");
        assert_eq!(resolved.writes().tables().contains_key(&table()), outcome.is_none(), "position {position}");
        state.apply(resolved.writes()).unwrap();
        resolver.take_head().unwrap();
    }
}

#[tokio::test]
async fn an_external_writer_time_steps_past_its_predecessor_and_keeps_a_future_time() {
    struct Commit;
    #[async_trait::async_trait]
    impl TransactionWriter for Commit {
        async fn write(&mut self, _: &mut OpenTransaction<'_>) -> Result<WriterResult> { Ok(WriterResult::Committed { user_metadata: None }) }
    }

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    for (now, requested, system_time) in [
        (100, Some(50), 50), (100, Some(50), 51), (100, Some(40), 52), (100, Some(500), 500), (200, None, 501), (600, None, 600),
    ] {
        let resolved = resolver.resolve_external(state.value().snapshot(), Timestamp(now), requested.map(Timestamp), None, &mut Commit, &context).await.unwrap();
        assert!(resolved.result().committed(), "{:?}", resolved.result());
        assert_eq!(resolved.result().tx_key().system_time, Timestamp(system_time));
        state.apply(resolved.writes()).unwrap();
        resolver.take_head().unwrap();
    }
}

#[tokio::test]
async fn sql_source_replay_keeps_unreached_parameter_cells_in_arrow() {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use rtdb_columnar::OwnedBatch;
    use rtdb_format::envelope::SourceMessage;

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("payload", DataType::Utf8, false)]));
    let text = "x".repeat(1 << 21);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(StringArray::from(vec![text.as_str(), text.as_str()]))]).unwrap();
    let arguments = SqlArguments::from_arrow(OwnedBatch::new(batch, &context.memory).unwrap(), &context.memory).unwrap();
    let source = request(vec![TxOp::Sql {
        sql: "INSERT INTO docs (_id, v) SELECT $1, CASE WHEN $1 < 0 THEN CAST(CHAR_LENGTH($2) AS BIGINT) ELSE $1 END".into(),
        args: Some(arguments),
    }], &context);
    let encoded = source.encode().unwrap();
    let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
    let decoded = SourceTransaction::decode(&message, &context.memory).unwrap();
    drop((source, encoded, message));
    let headroom = 1 << 20;
    let _pressure = context.memory.reserve(context.memory.limit_bytes() - context.memory.used_bytes() - headroom).unwrap();

    let resolved = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), decoded, &context).await.unwrap();
    assert!(resolved.result().committed(), "{:?}", resolved.result().error());
    let writes = resolved.writes().tables();
    let docs = writes.get(&table()).unwrap().batch().unwrap().view();
    let values = (0..docs.len()).map(|row| {
        let event = docs.read_event(row).unwrap();
        let StoredOperation::Put(document) = event.operation else { panic!("Expected a put"); };
        document["v"].clone()
    }).collect::<Vec<_>>();
    assert_eq!(values, [Value::I64(1), Value::I64(2)]);
}

#[tokio::test]
async fn sql_source_replay_validates_an_unreached_uri_before_evaluation() {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use rtdb_columnar::{OwnedBatch, to_field};
    use rtdb_types::{LogicalType, ScalarType};

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let fields = vec![Field::new("id", DataType::Int64, false), to_field("payload", &LogicalType::Scalar(ScalarType::Uri)).unwrap()];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![Arc::new(Int64Array::from(vec![1])), Arc::new(StringArray::from(vec!["http://["]))]).unwrap();
    let arguments = SqlArguments::from_arrow(OwnedBatch::new(batch, &context.memory).unwrap(), &context.memory).unwrap();
    let statement = "INSERT INTO docs (_id, v) SELECT $1, CASE WHEN $1 < 0 THEN $2 IS NOT NULL ELSE TRUE END";
    let source = request(vec![TxOp::Sql {
        sql: statement.into(),
        args: Some(arguments),
    }], &context);
    let encoded = source.encode().unwrap();
    let message = rtdb_format::envelope::SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
    let decoded = SourceTransaction::decode(&message, &context.memory).unwrap();

    let result = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), decoded, &context).await.unwrap();
    let error = result.result().error().unwrap();
    assert_eq!(error.code, "xtdb/invalid-uri");
    assert_eq!(error.context["sql"], statement);
    assert_eq!(error.context["tx-op-idx"], 0);
    assert!(error.context.get("arg-idx").is_none());
}

#[tokio::test]
async fn old_resolvers_abort_a_fenced_source_record_before_a_replay_can_commit() {
    use rtdb_format::envelope::{SourceBody, SourceMessage};

    let context = context();
    let key = "00112233445566778899aabbccddeeff/0";
    let permit = ReplaySubmission::admit(key, Timestamp(100), Timestamp(400)).unwrap();
    let encoded = request(vec![put(&context, 7, 0, i64::MAX)], &context)
        .with_replay_submission(permit).encode().unwrap();
    let Some(SourceMessage::Protobuf(SourceBody::Tx(command))) = SourceMessage::decode(encoded.as_slice()).unwrap() else {
        panic!("expected structured source transaction");
    };

    for legacy_understands_permits in [false, true] {
        let state = PartitionState::empty(Default::default()).unwrap();
        let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
        let mut legacy_command = command.clone();
        if legacy_understands_permits {
            legacy_command.replay_submission.as_mut().unwrap().abort_on_legacy_resolver = None;
        } else {
            legacy_command.replay_submission = None;
        }
        let mut resolve = async |position, command| {
            let request = SourceTransaction::decode(&SourceMessage::Protobuf(SourceBody::Tx(command)), &context.memory).unwrap();
            let resolved = resolver.resolve_source(state.value().snapshot(), SourcePosition(position), Timestamp(100 + position), request, &context).await.unwrap();
            state.apply(resolver.take_head().unwrap().writes()).unwrap();
            resolved
        };
        let old = resolve(0, legacy_command.clone()).await;
        assert_eq!(old.result().error().unwrap().code, "xtdb/assert-failed");
        assert_eq!(old.writes().tables().keys().collect::<Vec<_>>(), [&TableRef::new("xt", "txs")]);

        let first = resolve(1, command.clone()).await;
        assert!(first.result().committed());
        assert!(first.writes().tables().contains_key(&table()));
        let old_after_commit = resolve(2, legacy_command).await;
        assert!(!old_after_commit.writes().tables().contains_key(&table()));
        let duplicate = resolve(3, command.clone()).await;
        assert_eq!(private_replay_original(duplicate.result().error().unwrap()).unwrap(), Some(first.result().tx_key()));
        assert_eq!(duplicate.writes().tables().keys().collect::<Vec<_>>(), [&TableRef::new("xt", "txs")]);
    }
}

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

            transaction.append(table(), rtdb_format::data::StoredEvent { position: None,
                origin: None,
                commit_position: 0,
                entity_key: None,
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
    let ReplayedTransaction { writes: replica, .. } = decode_replica(body, None, state.value().snapshot().block_index(), &context.memory).unwrap();
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
    resolver.resolve_skipped(state.value().snapshot(), SourcePosition(1), Timestamp(101), None, &context).await.unwrap();
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
async fn ingestion_metrics_count_aborts_but_exclude_skipped_transactions() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let metrics = Arc::new(Metrics::default()); let mut context = context(); context.execution_observer = Some(metrics.clone());
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), request(vec![sql("ASSERT FALSE")], &context), &context).await.unwrap();
    resolver.resolve_skipped(state.value().snapshot(), SourcePosition(1), Timestamp(101), None, &context).await.unwrap();
    resolver.resolve_source(state.value().snapshot(), SourcePosition(2), Timestamp(102), request(vec![sql("ASSERT FALSE")], &context), &context).await.unwrap();
    assert_eq!(metrics.0.load(std::sync::atomic::Ordering::Relaxed), 2);
}

#[tokio::test]
async fn skipped_transaction_stages_only_its_aborted_row_and_smooths_time() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let first = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), request(vec![put(&context, 9, 0, i64::MAX)], &context), &context).await.unwrap();
    let skipped = resolver.resolve_skipped(state.value().snapshot(), SourcePosition(1), Timestamp(50), None, &context).await.unwrap();
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
async fn create_only_insert_checks_staged_intervals_and_replays_atomic_outcomes() {
    use rtdb_format::envelope::{ReplicaBody, SourceMessage};

    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let insert = |id, from, to| TxOp::InsertDocs {
        table: table(),
        documents: Documents::new(vec![StructFields::from([
            ("_id".into(), Value::I64(id)), ("v".into(), Value::I64(id)),
            ("_valid_from".into(), Value::Null),
        ])], &context.memory).unwrap(),
        valid_from: Some(Timestamp(from)),
        valid_to: Some(Timestamp(to)),
    };
    let logged = |operations| {
        let bytes = request(operations, &context).encode().unwrap();
        let message = SourceMessage::decode(bytes.as_slice()).unwrap().unwrap();
        SourceTransaction::decode(&message, &context.memory).unwrap()
    };

    let first = resolver.resolve_source(
        state.snapshot(), SourcePosition(0), Timestamp(100), logged(vec![insert(1, 10, 20)]), &context,
    ).await.unwrap();
    assert!(first.result().committed());
    let adjacent = resolver.resolve_source(
        state.snapshot(), SourcePosition(1), Timestamp(101), logged(vec![insert(1, 20, 30)]), &context,
    ).await.unwrap();
    assert!(adjacent.result().committed());
    let overlapping = resolver.resolve_source(
        state.snapshot(), SourcePosition(2), Timestamp(102), logged(vec![insert(2, 10, 20), insert(1, 19, 21)]), &context,
    ).await.unwrap();
    assert_eq!(overlapping.result().error().unwrap().code, "rtdb.tx/exists");
    assert!(!overlapping.writes().tables().contains_key(&table()));

    let intra_transaction = resolver.resolve_source(
        state.snapshot(), SourcePosition(3), Timestamp(103), logged(vec![insert(3, 10, 20), insert(3, 11, 12)]), &context,
    ).await.unwrap();
    assert_eq!(intra_transaction.result().error().unwrap().code, "rtdb.tx/exists");
    assert!(!intra_transaction.writes().tables().contains_key(&table()));

    let same_operation = TxOp::InsertDocs {
        table: table(),
        documents: Documents::new(vec![document(4), document(4)], &context.memory).unwrap(),
        valid_from: Some(Timestamp(10)),
        valid_to: Some(Timestamp(20)),
    };
    let intra_batch = resolver.resolve_source(
        state.snapshot(), SourcePosition(4), Timestamp(104), logged(vec![same_operation]), &context,
    ).await.unwrap();
    assert_eq!(intra_batch.result().error().unwrap().code, "rtdb.tx/exists");
    assert!(!intra_batch.writes().tables().contains_key(&table()));

    let follower = PartitionState::empty(Default::default()).unwrap();
    for resolved in [first, adjacent, overlapping, intra_transaction, intra_batch] {
        let body = resolved.to_replica_body(&context.memory).unwrap();
        let ReplicaBody::ResolvedTx(body) = body.body() else { panic!("expected resolved payload"); };
        let snapshot = follower.snapshot();
        let replayed = decode_replica(body, snapshot.tx_basis(), snapshot.block_index(), &context.memory).unwrap();
        assert_eq!(&replayed.result, resolved.result());
        follower.apply(&replayed.writes).unwrap();
    }
    let snapshot = follower.snapshot();
    let events = snapshot.segments(&table()).iter().flat_map(|segment| segment.batches()).map(|batch| batch.len()).sum::<usize>();
    assert_eq!(events, 2);
}

#[tokio::test]
async fn arrow_replay_insert_batches_keep_replica_bytes_and_first_failure() {
    use rtdb_format::{envelope::SourceMessage, ipc::write_stream};

    let mut context = context();
    let runs = Arc::new(QueryRuns::default());
    context.tracer = Some(runs.clone());
    let state = PartitionState::empty(Default::default()).unwrap();
    let timestamp = |value| Value::Timestamp {
        value, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()),
    };
    let row = |id, from, to, bits: Option<u64>| StructFields::from([
        ("_id".into(), Value::I64(id)),
        ("_valid_from".into(), timestamp(from)),
        ("_valid_to".into(), timestamp(to)),
        ("reading".into(), bits.map(|bits| Value::F64(f64::from_bits(bits))).unwrap_or(Value::Null)),
    ]);
    let insert = |rows| TxOp::InsertDocs {
        table: table(), documents: Documents::new(rows, &context.memory).unwrap(), valid_from: None, valid_to: None,
    };
    let replay = |operations| {
        let bytes = request(operations, &context).encode().unwrap();
        let message = SourceMessage::decode(bytes.as_slice()).unwrap().unwrap();
        SourceTransaction::decode(&message, &context.memory).unwrap()
    };
    let existing = insert(vec![row(1, 20, 30, Some(0))]);
    let rows = vec![row(1, 30, 40, Some(1 << 63)), row(2, 10, 45, Some(0x7ff8000000000123)), row(1, 10, 20, None)];
    let replica_bytes = |resolved: &ResolvedTx| {
        let batch = resolved.writes().tables()[&table()].batch().unwrap().arrow();
        write_stream(batch.schema_ref(), std::slice::from_ref(batch)).unwrap()
    };
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let mut reference = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let one_at_a_time = std::iter::once(existing.clone()).chain(rows.iter().cloned().map(|row| insert(vec![row]))).collect();
    let expected = reference.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100), replay(one_at_a_time), &context).await.unwrap();
    runs.0.store(0, std::sync::atomic::Ordering::Relaxed);
    let grouped = resolver.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100), replay(vec![existing, insert(rows)]), &context).await.unwrap();
    assert!(grouped.result().committed(), "{:?}", grouped.result().error());
    assert_eq!(runs.0.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(replica_bytes(&grouped), replica_bytes(&expected));

    for (position, rows, code) in [
        (1, vec![row(1, 25, 26, None), row(2, 60, 50, None)], "rtdb.tx/exists"),
        (2, vec![row(2, 60, 50, None), row(1, 25, 26, None)], "xtdb.indexer/invalid-valid-times"),
        (3, vec![row(3, 15, 16, None), row(1, 25, 26, None)], "rtdb.tx/exists"),
    ] {
        let result = resolver.resolve_source(state.snapshot(), SourcePosition(position), Timestamp(100 + position), replay(vec![insert(rows)]), &context).await.unwrap();
        assert_eq!(result.result().error().unwrap().code, code);
        assert!(!result.writes().tables().contains_key(&table()));
    }
}

#[tokio::test]
async fn later_argument_rows_resolve_relation_names_against_the_transactions_own_tables() {
    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)], vec![Value::I64(3)]];
    let insert = TxOp::Sql {
        sql: "INSERT INTO docs (_id, made) VALUES (?, 'made_here'::regclass)".into(),
        args: Some(SqlArguments::from_rows(rows, &context.memory).unwrap()),
    };
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let resolved = resolver.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100), request(vec![
        sql("INSERT INTO made_here (_id) VALUES (1)"),
        insert,
        sql("ASSERT (SELECT COUNT(*) FROM docs WHERE made = 'made_here'::regclass) = 3"),
    ], &context), &context).await.unwrap();
    assert!(resolved.result().committed(), "{:?}", resolved.result().error());
}

#[tokio::test]
async fn arrow_replay_dynamic_insert_batches_independent_bindings_without_changing_replica_bytes() {
    use rtdb_format::{envelope::SourceMessage, ipc::write_stream};

    let mut context = context();
    let runs = Arc::new(QueryRuns::default());
    context.tracer = Some(runs.clone());
    let state = PartitionState::empty(Default::default()).unwrap();
    let rows = vec![
        vec![Value::I64(7), Value::F64(-0.0)],
        vec![Value::I64(7), Value::F64(f64::from_bits(0x7ff8000000000123))],
        vec![Value::I64(2), Value::F64(1.5)],
    ];
    let bound = |sql: &str, rows| TxOp::Sql {
        sql: sql.into(), args: Some(SqlArguments::from_rows(rows, &context.memory).unwrap()),
    };
    let replay = |operations| {
        let source = prepare_submission(operations, SubmitOptions::default(), "xtdb", &context.memory).unwrap();
        let encoded = source.encode().unwrap();
        let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        let replay = SourceTransaction::decode(&message, &context.memory).unwrap();
        assert_eq!(encoded.as_slice(), replay.encode().unwrap().as_slice());
        replay
    };
    let mut serial = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let mut grouped = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let insert = "INSERT INTO docs (_id, v) SELECT $1 + 0, CASE WHEN $1 < 0 THEN -$2 ELSE $2 END";
    let expected = serial.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100), replay(rows.iter().cloned().map(|row| bound(insert, vec![row])).collect()), &context).await.unwrap();
    runs.0.store(0, std::sync::atomic::Ordering::Relaxed);
    let actual = grouped.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100), replay(vec![bound(insert, rows)]), &context).await.unwrap();
    assert!(actual.result().committed(), "{:?}", actual.result().error());
    assert_eq!(runs.0.load(std::sync::atomic::Ordering::Relaxed), 1);
    let replica_bytes = |resolved: &ResolvedTx| {
        let batch = resolved.writes().tables()[&table()].batch().unwrap().arrow();
        write_stream(batch.schema_ref(), std::slice::from_ref(batch)).unwrap()
    };
    assert_eq!(replica_bytes(&actual), replica_bytes(&expected));

    let invalid = "INSERT INTO docs (_id, v) SELECT $1, 100 / $1";
    let args = vec![vec![Value::I64(2)], vec![Value::I64(0)], vec![Value::I64(1)]];
    let result = grouped.resolve_source(state.snapshot(), SourcePosition(1), Timestamp(101), replay(vec![bound(invalid, args)]), &context).await.unwrap();
    let error = result.result().error().unwrap();
    assert_eq!(error.context["arg-idx"], 1);
    assert!(!result.writes().tables().contains_key(&table()));

    let update = "UPDATE docs SET v = v + $1 WHERE _id = 2";
    let args = vec![vec![Value::F64(3.0)], vec![Value::F64(10.0)]];
    let result = grouped.resolve_source(state.snapshot(), SourcePosition(2), Timestamp(102), replay(vec![bound(update, args), sql("ASSERT (SELECT v FROM docs WHERE _id = 2) = 14.5")]), &context).await.unwrap();
    assert!(result.result().committed(), "{:?}", result.result().error());
}

#[tokio::test]
async fn owned_insert_owner_and_policy_survive_source_and_replica_decoding() {
    use rtdb_format::envelope::{ReplicaBody, SourceMessage};
    use rtdb_types::OwnedRelation;

    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let relation = OwnedRelation::new("orders").unwrap();
    let operation = TxOp::OwnedInsertDocs {
        relation: relation.clone(),
        documents: Documents::new(vec![document(7)], &context.memory).unwrap(),
        valid_from: None,
        valid_to: None,
    };
    let source = SourceTransaction::new(
        vec![operation],
        SubmitOptions { user: Some("owner".into()), ..SubmitOptions::default() },
        &context.memory,
    ).unwrap();
    let bytes = source.encode().unwrap();
    let message = SourceMessage::decode(bytes.as_slice()).unwrap().unwrap();
    let request = SourceTransaction::decode(&message, &context.memory).unwrap();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let snapshot = state.snapshot();
    let resolved = resolver.resolve_source(snapshot.clone(), SourcePosition(0), Timestamp(100), request, &context).await.unwrap();
    assert!(resolved.result().committed());
    let physical = relation.table("owner");
    assert!(resolved.writes().tables().contains_key(&physical));
    assert!(!resolved.writes().tables().contains_key(&relation.table("another-owner")));

    let encoded = resolved.to_replica_body(&context.memory).unwrap();
    let ReplicaBody::ResolvedTx(body) = encoded.body() else { panic!("expected resolved payload"); };
    let decoded = decode_replica(body, None, snapshot.block_index(), &context.memory).unwrap();
    assert!(decoded.writes.tables().contains_key(&physical));
    let mut unknown = body.clone();
    let data = unknown.table_data.remove(&physical.schema_and_table()).unwrap();
    unknown.table_data.insert(format!("rtdb_owned_v2/{}", physical.table), data);
    let error = decode_replica(&unknown, None, snapshot.block_index(), &context.memory).err().unwrap();
    assert_eq!(error.code, "rtdb.tx/invalid-replica-transaction");
}

#[tokio::test]
async fn owned_commit_checks_deadline_and_exact_point_before_mutation() {
    use rtdb_format::envelope::SourceMessage;
    use rtdb_types::{EntityId, OwnedRelation};

    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let relation = OwnedRelation::new("commits").unwrap();
    let id = EntityId::Integer(1);
    let documents = || Documents::new(vec![document(7)], &context.memory).unwrap();
    let source = |operations| {
        let source = SourceTransaction::new(
            operations, SubmitOptions { user: Some("owner".into()), ..SubmitOptions::default() }, &context.memory,
        ).unwrap();
        let bytes = source.encode().unwrap();
        let message = SourceMessage::decode(bytes.as_slice()).unwrap().unwrap();
        SourceTransaction::decode(&message, &context.memory).unwrap()
    };
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let create = TxOp::OwnedInsertDocs { relation: relation.clone(), documents: documents(), valid_from: Some(Timestamp(0)), valid_to: None };
    let replace = || TxOp::OwnedReplaceDocs { relation: relation.clone(), documents: documents(), valid_from: Some(Timestamp(0)), valid_to: None };
    let delete = || TxOp::OwnedDelete { relation: relation.clone(), id: id.clone(), valid_from: Some(Timestamp(0)), valid_to: None };
    let check = |id, expected| TxOp::OwnedCheck { relation: relation.clone(), id, valid_at: Timestamp(10), expected, revealed: None };
    let first = resolver.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100), source(vec![create]), &context).await.unwrap();
    assert!(first.result().committed());
    let point_version = |resolved: &rtdb_tx::ResolvedTx| rtdb_types::OwnedPointVersion {
        history: resolved.result().tx_key().system_time,
        position: resolved.writes().tables()[&relation.table("owner")]
            .batch().unwrap().header(0).unwrap().position.unwrap(),
    };
    let first_revision = point_version(&first);

    let expired = resolver.resolve_source(
        state.snapshot(), SourcePosition(1), Timestamp(101),
        source(vec![replace(), TxOp::owned_deadline(Timestamp(101))]), &context,
    ).await.unwrap();
    assert_eq!(expired.result().error().unwrap().code, "rtdb.owned/expired");

    let replaced = resolver.resolve_source(
        state.snapshot(), SourcePosition(2), Timestamp(102),
        source(vec![replace(), check(id.clone(), Some(first_revision)), TxOp::owned_deadline(Timestamp(110))]), &context,
    ).await.unwrap();
    assert!(replaced.result().committed());
    let replacement_revision = point_version(&replaced);

    let stale = resolver.resolve_source(
        state.snapshot(), SourcePosition(3), Timestamp(103),
        source(vec![delete(), check(id.clone(), Some(rtdb_types::OwnedPointVersion {
            history: replacement_revision.history, position: first_revision.position,
        }))]), &context,
    ).await.unwrap();
    assert_eq!(stale.result().error().unwrap().code, "rtdb.owned/stale-point");
    assert!(!stale.writes().tables().contains_key(&relation.table("owner")));

    let alias = EntityId::Uuid(id.iid().0);
    let mut alias_document = document(8);
    alias_document.insert("_id".into(), alias.as_value());
    let insert_alias = TxOp::OwnedInsertDocs {
        relation: relation.clone(),
        documents: Documents::new(vec![alias_document], &context.memory).unwrap(),
        valid_from: Some(Timestamp(0)), valid_to: None,
    };
    let collision = resolver.resolve_source(
        state.snapshot(), SourcePosition(4), Timestamp(104),
        source(vec![check(alias.clone(), None), insert_alias]), &context,
    ).await.unwrap();
    assert!(collision.result().committed(), "the exact absence admits a distinct key at the same IID");
    let alias_revision = point_version(&collision);

    let deleted = resolver.resolve_source(
        state.snapshot(), SourcePosition(5), Timestamp(105),
        source(vec![delete(), check(id.clone(), Some(replacement_revision))]), &context,
    ).await.unwrap();
    assert!(deleted.result().committed());
    let absent = resolver.resolve_source(
        state.snapshot(), SourcePosition(6), Timestamp(106),
        source(vec![check(id, None), check(alias, Some(alias_revision))]), &context,
    ).await.unwrap();
    assert!(absent.result().committed());
}

#[tokio::test]
async fn resolving_owned_batch_checks_prior_writes_and_keeps_its_receipt_clock_on_recovery() {
    use rtdb_types::OwnedRelation;

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let relation = OwnedRelation::new("batched_commits").unwrap();
    let create = |from, until| SourceTransaction::new(vec![TxOp::OwnedInsertDocs {
        relation: relation.clone(),
        documents: Documents::new(vec![document(7)], &context.memory).unwrap(),
        valid_from: Some(Timestamp(from)),
        valid_to: Some(Timestamp(until)),
    }], SubmitOptions { user: Some("owner".into()), ..SubmitOptions::default() }, &context.memory).unwrap();
    let receipt_time = |resolved: &rtdb_tx::ResolvedTx| {
        let history = &resolved.writes().tables()[&TableRef::new("xt", "txs")];
        let StoredOperation::Put(row) = history.batch().unwrap().view().read_event(0).unwrap().operation else {
            panic!("expected transaction result row");
        };
        row["receipt_time"].clone()
    };

    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let first = resolver.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100), create(0, 10), &context).await.unwrap();
    let duplicate = resolver.resolve_source(state.snapshot(), SourcePosition(1), Timestamp(99), create(5, 15), &context).await.unwrap();
    let adjacent = resolver.resolve_source(state.snapshot(), SourcePosition(2), Timestamp(98), create(10, 20), &context).await.unwrap();
    assert!(first.result().committed());
    assert_eq!(duplicate.result().error().unwrap().code, "rtdb.tx/exists");
    assert!(!duplicate.writes().tables().contains_key(&relation.table("owner")));
    assert!(adjacent.result().committed());
    assert!(matches!(receipt_time(&first), Value::Timestamp { value: 100, .. }));
    assert_eq!(receipt_time(&duplicate), receipt_time(&first));
    assert_eq!(receipt_time(&adjacent), receipt_time(&first));
    assert_eq!(state.snapshot().tx_basis(), None);

    while let Some(resolved) = resolver.take_head() {
        state.apply(resolved.writes()).unwrap();
    }
    let mut recovered = TxResolver::new(Some(adjacent.result().tx_key()), SourcePosition(2), None, &context.memory).unwrap();
    let next = recovered.resolve_source(state.snapshot(), SourcePosition(3), Timestamp(97),
        request(vec![], &context), &context).await.unwrap();
    assert!(next.result().committed());
    assert_eq!(receipt_time(&next), receipt_time(&first));
}

#[tokio::test]
async fn staged_snapshot_requires_the_predecessor_and_block_of_each_overlay() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let captured = state.value().snapshot();
    let context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let first = resolver.resolve_source(captured.clone(), SourcePosition(0), Timestamp(100),
        request(vec![put(&context, 10, 0, i64::MAX)], &context), &context).await.unwrap();
    let second = resolver.resolve_source(captured.clone(), SourcePosition(1), Timestamp(101),
        request(vec![put(&context, 20, 0, i64::MAX)], &context), &context).await.unwrap();

    let reversed = [Arc::clone(second.writes()), Arc::clone(first.writes())];
    assert!(matches!(captured.with_staged(&reversed), Err(error) if error.code == "rtdb.index/overlay-order"));
    let wrong_block = rtdb_types::BlockIndex(captured.block_index().0 + 1);
    let wrong = rtdb_index::StagedWrite::new(first.result().tx_key(), None, wrong_block, first.writes().tables().clone()).unwrap();
    assert!(matches!(captured.with_staged(&[Arc::new(wrong)]), Err(error) if error.code == "rtdb.index/overlay-order"));

    let before_apply = resolver.resolving_snapshot(captured.clone()).unwrap();
    assert_eq!(before_apply.tx_basis(), Some(second.result().tx_key()));
    assert_eq!(before_apply.iter_segments(&table()).count(), 2);
    assert_eq!(captured.tx_basis(), None);
    assert!(!captured.contains_table(&table()));

    state.apply(resolver.take_head().unwrap().writes()).unwrap();
    let after_apply = resolver.resolving_snapshot(state.value().snapshot()).unwrap();
    assert_eq!(after_apply.tx_basis(), before_apply.tx_basis());
    assert_eq!(after_apply.iter_segments(&table()).count(), 2);
    assert!(Arc::ptr_eq(&before_apply.segments(&table())[0], &after_apply.segments(&table())[0]));
    assert_eq!(state.value().snapshot().tx_basis(), Some(first.result().tx_key()));
    assert!(!captured.contains_table(&table()));
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
    let following = resolver.resolve_source(state.value().snapshot(), SourcePosition(2), Timestamp(50), request(vec![], &context), &context).await.unwrap();
    assert!(following.result().committed());
    assert_eq!(following.result().tx_key().system_time, Timestamp(102));
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
    let ReplayedTransaction { writes: decoded, result, .. } = decode_replica(body, None, state.value().snapshot().block_index(), &context.memory).unwrap();
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
        let ReplayedTransaction { writes: replica, result, .. } = decode_replica(body, None, state.value().snapshot().block_index(), &context.memory).unwrap();
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

#[tokio::test]
async fn owned_time_advances_while_replica_receipts_preserve_the_original_append() {
    use rtdb_format::envelope::{ReplicaBody, SourceMessage};
    use rtdb_types::{OwnedRelation, TxId};

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let owned = |value| {
        let source = SourceTransaction::new(
            vec![TxOp::OwnedReplaceDocs {
                relation: OwnedRelation::new("clocks").unwrap(),
                documents: Documents::new(vec![document(value)], &context.memory).unwrap(),
                valid_from: Some(Timestamp(0)), valid_to: None,
            }],
            SubmitOptions { user: Some("owner".into()), ..Default::default() },
            &context.memory,
        ).unwrap();
        let encoded = source.encode().unwrap();
        SourceTransaction::decode(&SourceMessage::decode(encoded.as_slice()).unwrap().unwrap(), &context.memory).unwrap()
    };
    let results = TableRef::new("xt", "txs");
    for (position, sample) in [(0, 100), (1, 100), (2, 99)] {
        let snapshot = state.snapshot();
        let resolved = resolver.resolve_source(snapshot.clone(), SourcePosition(position), Timestamp(sample), owned(position), &context).await.unwrap();
        assert!(resolved.result().committed(), "{:?}", resolved.result());
        let key = resolved.result().tx_key();
        assert_eq!(key.tx_id, TxId(position));
        assert_eq!(key.system_time, Timestamp(100 + position));
        let body = resolved.to_replica_body(&context.memory).unwrap();
        let ReplicaBody::ResolvedTx(body) = body.body() else { panic!("resolved transaction"); };
        let replay = decode_replica(body, None, snapshot.block_index(), &context.memory).unwrap();
        let events = replay.writes.tables()[&results].batch().unwrap();
        let StoredOperation::Put(row) = events.view().read_event(0).unwrap().operation else { panic!("transaction row"); };
        assert!(matches!(row["source_log_time"], Value::Timestamp { value, .. } if value == sample));
        state.apply(resolver.take_head().unwrap().writes()).unwrap();
    }
}

#[tokio::test]
async fn a_legacy_resolver_cannot_execute_a_bounded_retry_record() {
    use rtdb_format::envelope::{SourceBody, SourceMessage};

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let metadata = UserMetadata::new(Value::Struct(StructFields::from([
        ("rtdb_retry_v1".into(), Value::Struct(StructFields::from([
            ("nonce".into(), Value::Utf8("a0000000000000000000000000000001".into())),
            ("not_after_micros".into(), Value::I64(200)),
        ]))),
    ])), &context.memory).unwrap();
    let source = SourceTransaction::new(vec![put(&context, 7, 0, i64::MAX)], SubmitOptions {
        user_metadata: Some(metadata), ..Default::default()
    }, &context.memory).unwrap();
    let bytes = source.encode().unwrap();
    let SourceMessage::Protobuf(SourceBody::Tx(mut command)) = SourceMessage::decode(bytes.as_slice()).unwrap().unwrap() else {
        panic!("expected a source transaction");
    };
    assert!(command.bounded_retry.take().is_some());
    let legacy = SourceTransaction::decode(&SourceMessage::Protobuf(SourceBody::Tx(command)), &context.memory).unwrap();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let resolved = resolver.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100), legacy, &context).await.unwrap();
    assert_eq!(resolved.result().error().unwrap().code, "xtdb/assert-failed");
    assert!(!resolved.writes().tables().contains_key(&table()));
}

#[tokio::test]
async fn a_bounded_identity_survives_a_writer_epoch_change_but_is_local_to_its_placed_history() {
    use rtdb_types::PlacedAttemptScope;

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let metadata = UserMetadata::new(Value::Struct(StructFields::from([
        ("rtdb_retry_v1".into(), Value::Struct(StructFields::from([
            ("nonce".into(), Value::Utf8("a0000000000000000000000000000001".into())),
            ("not_after_micros".into(), Value::I64(200)),
        ]))),
    ])), &context.memory).unwrap();
    let request = |history, epoch| {
        SourceTransaction::new(vec![put(&context, 42, 0, i64::MAX)], SubmitOptions {
            user: Some("alice".into()), user_metadata: Some(metadata.clone()), ..Default::default()
        }, &context.memory).unwrap().with_placed_seed(Some(PlacedAttemptScope::new(history, epoch).unwrap().issue()))
    };
    let mut resolve = async |position, source| {
        let resolved = resolver.resolve_source(state.snapshot(), SourcePosition(position), Timestamp(100), source, &context).await.unwrap();
        state.apply(resolver.take_head().unwrap().writes()).unwrap();
        resolved
    };
    let original = resolve(0, request("original/history", 1)).await;
    assert!(original.result().committed());
    let duplicate = resolve(1, request("original/history", 2)).await;
    assert_eq!(bounded_retry_original(duplicate.result().error().unwrap()).unwrap(), Some(original.result().tx_key()));
    let fork = resolve(2, request("fork/history", 1)).await;
    assert!(fork.result().committed());
}

#[tokio::test]
async fn a_retry_identity_answers_its_own_body_and_refuses_another_within_its_window() {
    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let keyed = |user: &str, id: i64| {
        let metadata = UserMetadata::new(Value::Struct(StructFields::from([("idempotency_key".into(), Value::Utf8("order-1".into()))])), &context.memory).unwrap();
        SourceTransaction::new(vec![put(&context, id, 0, i64::MAX)], SubmitOptions { user: Some(user.into()), user_metadata: Some(metadata), ..Default::default() }, &context.memory).unwrap()
    };
    let mut position = 0;
    let mut resolve = async |request: SourceTransaction, log_time: i64| {
        let resolved = resolver.resolve_source(state.value().snapshot(), SourcePosition(position), Timestamp(log_time), request, &context).await.unwrap();
        position += 1;
        state.apply(resolver.take_head().unwrap().writes()).unwrap();
        resolved
    };

    assert!(resolve(request(vec![], &context), 100).await.result().committed());
    let original = resolve(keyed("alice", 1), 100).await;
    assert!(original.result().committed());
    assert_eq!(original.result().tx_key().system_time, Timestamp(101));
    let duplicate = resolve(keyed("alice", 1), 100).await;
    let error = duplicate.result().error().unwrap();
    assert_eq!(error.code, "rtdb/idempotency-duplicate");
    assert_eq!(error.context["original-tx-id"], original.result().tx_key().tx_id.0);
    assert_eq!(duplicate.writes().tables().keys().collect::<Vec<_>>(), [&TableRef::new("xt", "idempotency_receipts"), &TableRef::new("xt", "txs")], "a duplicate writes only its own outcome and expiring metadata");
    let conflict = resolve(keyed("alice", 2), RECEIPT_WINDOW_MICROS).await;
    assert_eq!(conflict.result().error().unwrap().code, "rtdb/idempotency-conflict");
    assert!(resolve(keyed("bob", 2), 400).await.result().committed(), "another principal's identity is independent");
    let regressed = resolve(keyed("alice", 1), 99).await;
    assert_eq!(regressed.result().error().unwrap().code, "rtdb/idempotency-duplicate");
    let cut = resolve(request(vec![], &context), 100 + RECEIPT_WINDOW_MICROS).await;
    assert!(cut.result().committed());
    let after = resolve(keyed("alice", 3), 99).await;
    assert!(after.result().committed(), "the identity is free again once the window has passed");
    let retried_after_regression = resolve(keyed("alice", 3), 99).await;
    let error = retried_after_regression.result().error().unwrap();
    assert_eq!(error.code, "rtdb/idempotency-duplicate");
    assert_eq!(error.context["original-tx-id"], after.result().tx_key().tx_id.0);
}

#[tokio::test]
async fn external_event_writer_cannot_erase_private_replay_markers() {
    struct ExternalWriter;

    #[async_trait::async_trait]
    impl TransactionWriter for ExternalWriter {
        async fn write(&mut self, transaction: &mut OpenTransaction<'_>) -> Result<WriterResult> {
            let error = transaction.append(TableRef::new("xt", "rtdb_replay_keys"), rtdb_format::data::StoredEvent { position: None,
                origin: None,
                commit_position: 0,
                entity_key: None,
                iid: Value::Utf8("00112233445566778899aabbccddeeff/0".into()).entity_id()?.iid(),
                system_from: transaction.tx_key().system_time,
                valid_time: Interval::ALL,
                operation: StoredOperation::Erase,
                recency: None,
            }).unwrap_err();
            Ok(WriterResult::Aborted { error, user_metadata: None })
        }
    }

    let state = PartitionState::empty(Default::default()).unwrap();
    let context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let resolved = resolver.resolve_external(state.value().snapshot(), Timestamp(100), None, None, &mut ExternalWriter, &context).await.unwrap();
    assert_eq!(resolved.result().error().unwrap().code, "xtdb/forbidden-table");
    assert!(!resolved.writes().tables().contains_key(&TableRef::new("xt", "rtdb_replay_keys")));
}

#[tokio::test]
async fn declared_owned_schema_distinguishes_omission_and_null_after_source_decode() {
    use rtdb_format::envelope::SourceMessage;
    use rtdb_types::{OwnedRelation, OwnedSchema};

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let relation = OwnedRelation::new("typed-orders").unwrap();
    let schema = OwnedSchema::from_json(br#"{"fields":[
        {"name":"amount","type":{"kind":"i64"},"optional":true,"nullable":false}
    ]}"#).unwrap();
    let source = |operations| {
        let request = SourceTransaction::new(
            operations, SubmitOptions { user: Some("owner".into()), ..SubmitOptions::default() }, &context.memory,
        ).unwrap();
        let encoded = request.encode().unwrap();
        let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        SourceTransaction::decode(&message, &context.memory).unwrap()
    };
    let documents = |value| {
        let mut row = StructFields::from([("_id".into(), Value::I64(1))]);
        if let Some(value) = value { row.insert("amount".into(), value); }
        Documents::new(vec![row], &context.memory).unwrap()
    };
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let declare = TxOp::OwnedDeclareRelation { relation: relation.clone(), schema };
    let declared = resolver.resolve_source(
        state.snapshot(), SourcePosition(0), Timestamp(100), source(vec![declare]), &context,
    ).await.unwrap();
    assert!(declared.result().committed());
    assert!(declared.writes().tables().contains_key(&OwnedRelation::relation_catalog("owner")));

    let create = TxOp::OwnedInsertDocs {
        relation: relation.clone(), documents: documents(None), valid_from: None, valid_to: None,
    };
    let created = resolver.resolve_source(
        state.snapshot(), SourcePosition(1), Timestamp(101), source(vec![create]), &context,
    ).await.unwrap();
    assert!(created.result().committed());

    let replace = TxOp::OwnedReplaceDocs {
        relation: relation.clone(), documents: documents(Some(Value::Null)), valid_from: None, valid_to: None,
    };
    let refused = resolver.resolve_source(
        state.snapshot(), SourcePosition(2), Timestamp(102), source(vec![replace]), &context,
    ).await.unwrap();
    assert_eq!(refused.result().error().unwrap().code, "rtdb.owned/nonconforming");
    assert!(!refused.writes().tables().contains_key(&relation.table("owner")));
}

#[tokio::test]
async fn owned_arrow_replay_preserves_lifetime_and_explicit_bounds() {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use rtdb_columnar::OwnedBatch;
    use rtdb_format::envelope::SourceMessage;
    use rtdb_types::{OwnedRelation, OwnedSchema};

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let lifetime = OwnedRelation::new("lifetime-facts").unwrap();
    let period = OwnedRelation::new("period-facts").unwrap();
    let source = |operations| {
        let request = SourceTransaction::new(
            operations, SubmitOptions { user: Some("owner".into()), ..SubmitOptions::default() }, &context.memory,
        ).unwrap();
        let encoded = request.encode().unwrap();
        let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        SourceTransaction::decode(&message, &context.memory).unwrap()
    };
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let declared = resolver.resolve_source(
        state.snapshot(), SourcePosition(0), Timestamp(10), source(vec![
            TxOp::OwnedDeclareRelation {
                relation: lifetime.clone(),
                schema: OwnedSchema::from_json(br#"{"fields":[],"validity":"acceptance","lifetime_micros":10}"#).unwrap(),
            },
            TxOp::OwnedDeclareRelation {
                relation: period.clone(),
                schema: OwnedSchema::from_json(br#"{"fields":[],"validity":"bounded_period"}"#).unwrap(),
            },
        ]), &context,
    ).await.unwrap();
    assert!(declared.result().committed(), "{:?}", declared.result().error());
    let batch = RecordBatch::try_from_iter([("_id", Arc::new(Int64Array::from(vec![7])) as ArrayRef)]).unwrap();
    let batch = OwnedBatch::new(batch, &context.memory).unwrap();
    let docs = ArrowDocuments::from_owned_payload(&batch, &context.memory).unwrap().unwrap();
    let write = |relation: &OwnedRelation, from, to| TxOp::OwnedArrowDocs {
        relation: relation.clone(), documents: docs.clone(), create_only: true, valid_from: from, valid_to: to,
    };
    let committed = resolver.resolve_source(
        state.snapshot(), SourcePosition(1), Timestamp(20), source(vec![
            write(&lifetime, None, None),
            write(&period, Some(Timestamp(3)), Some(Timestamp(5))),
        ]), &context,
    ).await.unwrap();
    assert!(committed.result().committed(), "{:?}", committed.result().error());
    for (relation, from, to) in [(lifetime, 20, 30), (period, 3, 5)] {
        let write = &committed.writes().tables()[&relation.table("owner")];
        let event = write.batch().unwrap().view().read_event(0).unwrap();
        assert_eq!(event.header().valid_time, Interval::new(Timestamp(from), Timestamp(to)).unwrap());
    }
}

#[tokio::test]
async fn legacy_owned_arrow_resolution_emits_the_same_replica_bytes_as_row_resolution() {
    use arrow_array::Float32Array;
    use rtdb_format::envelope::{ReplicaBody, SourceMessage};
    use rtdb_types::{ListValue, OwnedRelation};

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let relation = OwnedRelation::new("embeddings").unwrap();
    let vector = |bits| Value::FixedSizeList(ListValue::from_f32(Float32Array::from(vec![
        Some(f32::from_bits(bits)), Some(-0.0), None,
    ])));
    let documents = Documents::new(vec![
        StructFields::from([("_id".into(), Value::I64(1)), ("embedding".into(), vector(0x7fc0_1234))]),
        StructFields::from([("_id".into(), Value::I64(2)), ("embedding".into(), Value::Null)]),
        StructFields::from([("_id".into(), Value::I64(3))]),
        StructFields::from([("_id".into(), Value::I64(4)), ("embedding".into(), vector(0x7fc0_5678)),
            ("_valid_from".into(), Value::Timestamp { value: 90, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) })]),
    ], &context.memory).unwrap();
    let original = SourceTransaction::new(vec![TxOp::OwnedInsertDocs {
        relation, documents, valid_from: None, valid_to: None,
    }], SubmitOptions { user: Some("owner".into()), ..SubmitOptions::default() }, &context.memory).unwrap();
    let encoded = original.encode().unwrap();
    let decoded = SourceTransaction::decode(&SourceMessage::decode(encoded.as_slice()).unwrap().unwrap(), &context.memory).unwrap();
    let mut replicas = Vec::new();
    for source in [original, decoded] {
        let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
        let result = resolver.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100), source, &context).await.unwrap();
        assert!(result.result().committed(), "{:?}", result.result().error());
        let replica = result.to_replica_body(&context.memory).unwrap();
        let ReplicaBody::ResolvedTx(body) = replica.body() else { panic!("expected a resolved transaction"); };
        replicas.push(body.table_data.clone());
    }
    assert_eq!(replicas[0], replicas[1]);
}

#[tokio::test]
async fn world_mutations_refuse_source_operations_without_held_admission() {
    use rtdb_types::{EntityId, Iid, OwnedRelation, OwnedWorld, TransactionKey, TxId, WorldSchemas, WorldShadow};

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let world = OwnedWorld {
        ancestors: Vec::new(),
        fork_lineage: Vec::new(),
        id: Iid([8; 16]), pin: Iid([8; 16]), capture: Iid([9; 16]), suffix: None, manifest: "a".repeat(43),
        basis: TransactionKey { tx_id: TxId(0), system_time: Timestamp(10) },
        parent_publication: None, mode: rtdb_types::WorldClockMode::Held { start: Timestamp(10) },
        until: Timestamp(200), schemas: WorldSchemas::new(Default::default()).unwrap(), dropped: false,
    };
    let catalog = OwnedRelation::world_catalog();
    let overlay = OwnedRelation::new("orders").unwrap().in_world(world.id).unwrap();
    let root = TxOp::OwnedInsertDocs {
        relation: catalog, documents: Documents::new(vec![world.record()], &context.memory).unwrap(),
        valid_from: None, valid_to: None,
    };
    let write = TxOp::OwnedReplaceDocs {
        relation: overlay, documents: Documents::new(
            vec![WorldShadow::Deleted.record(&EntityId::Integer(1)).unwrap()], &context.memory,
        ).unwrap(),
        valid_from: Some(Timestamp(50)), valid_to: Some(Timestamp(100)),
    };
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    for (position, operation) in [root, write].into_iter().enumerate() {
        let source = SourceTransaction::new(
            vec![operation], SubmitOptions { user: Some("owner".into()), ..SubmitOptions::default() }, &context.memory,
        ).unwrap();
        let refused = resolver.resolve_source(
            state.snapshot(), SourcePosition(position as i64), Timestamp(100 + position as i64), source, &context,
        ).await.unwrap();
        assert_eq!(refused.result().error().unwrap().code, "rtdb.world/unavailable");
    }
}

#[tokio::test]
async fn owned_range_witness_checks_only_its_relation_before_mutation() {
    use rtdb_format::envelope::SourceMessage;
    use rtdb_types::{OwnedRelation, PlacedAttemptScope};

    let mut context = context();
    let scope = PlacedAttemptScope::new("installed-replica", 1).unwrap();
    context.owned_witness_scope = Some(scope);
    let state = PartitionState::empty(Default::default()).unwrap();
    let watched = OwnedRelation::new("watched").unwrap();
    let other = OwnedRelation::new("other").unwrap();
    let put = |relation: &OwnedRelation| TxOp::OwnedReplaceDocs {
        relation: relation.clone(), documents: Documents::new(vec![document(7)], &context.memory).unwrap(),
        valid_from: Some(Timestamp(400)), valid_to: Some(Timestamp(500)),
    };
    let source = |operations| {
        let request = SourceTransaction::new(
            operations, SubmitOptions { user: Some("owner".into()), ..SubmitOptions::default() }, &context.memory,
        ).unwrap().with_placed_seed(Some(scope.issue()));
        let encoded = request.encode().unwrap();
        let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        SourceTransaction::decode(&message, &context.memory).unwrap()
    };
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let initial = resolver.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100),
        source(vec![put(&watched)]), &context).await.unwrap();
    assert!(initial.result().committed());
    let check = |witness_scope, until| TxOp::OwnedCheckRange {
        relation: watched.clone(), basis: initial.result().tx_key(), scope: witness_scope, frontier: 1, until: Timestamp(until),
        region: None,
        revealed: None,
    };
    let unrelated = resolver.resolve_source(state.snapshot(), SourcePosition(1), Timestamp(101),
        source(vec![put(&other)]), &context).await.unwrap();
    assert!(unrelated.result().committed());
    let accepted = resolver.resolve_source(state.snapshot(), SourcePosition(2), Timestamp(102),
        source(vec![put(&other), check(scope, 200)]), &context).await.unwrap();
    assert!(accepted.result().committed());

    let wrong_scope = PlacedAttemptScope::new("another-replica", 1).unwrap();
    let moved = resolver.resolve_source(state.snapshot(), SourcePosition(3), Timestamp(103),
        source(vec![check(wrong_scope, 200)]), &context).await.unwrap();
    assert_eq!(moved.result().error().unwrap().code, "rtdb.owned/range-witness-unavailable");
    let expired = resolver.resolve_source(state.snapshot(), SourcePosition(4), Timestamp(104),
        source(vec![check(scope, 104)]), &context).await.unwrap();
    assert_eq!(expired.result().error().unwrap().code, "rtdb.owned/range-witness-unavailable");

    let correction = resolver.resolve_source(state.snapshot(), SourcePosition(5), Timestamp(105),
        source(vec![put(&watched)]), &context).await.unwrap();
    assert!(correction.result().committed());
    let stale = resolver.resolve_source(state.snapshot(), SourcePosition(6), Timestamp(106),
        source(vec![put(&other), check(scope, 200)]), &context).await.unwrap();
    assert_eq!(stale.result().error().unwrap().code, "rtdb.owned/stale-range");
    assert!(!stale.writes().tables().contains_key(&other.table("owner")));
}

#[tokio::test]
async fn entity_region_refuses_a_shadowed_phantom_and_allows_disjoint_assertions() {
    use rtdb_format::envelope::SourceMessage;
    use rtdb_types::{EntityId, Interval, OwnedEntityRegion, OwnedRangeEvidence, OwnedRelation, PlacedAttemptScope};

    let mut context = context();
    let scope = PlacedAttemptScope::new("installed-replica", 1).unwrap();
    context.owned_witness_scope = Some(scope);
    let state = PartitionState::empty(Default::default()).unwrap();
    let relation = OwnedRelation::new("facts").unwrap();
    let put = |id: &str, from, to| TxOp::OwnedReplaceDocs {
        relation: relation.clone(),
        documents: Documents::new(vec![StructFields::from([
            ("_id".into(), Value::Utf8(id.into())), ("value".into(), Value::I64(from)),
        ])], &context.memory).unwrap(),
        valid_from: Some(Timestamp(from)), valid_to: Some(Timestamp(to)),
    };
    let source = |operations| {
        let request = SourceTransaction::new(
            operations, SubmitOptions { user: Some("owner".into()), ..SubmitOptions::default() }, &context.memory,
        ).unwrap().with_placed_seed(Some(scope.issue()));
        let encoded = request.encode().unwrap();
        let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        SourceTransaction::decode(&message, &context.memory).unwrap()
    };
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let initial = resolver.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100),
        source(vec![put("other", 400, 500)]), &context).await.unwrap();
    assert!(initial.result().committed());

    let id = EntityId::String("wanted".into());
    let valid = Interval::from_micros(400, 500).unwrap();
    let check = || TxOp::OwnedCheckRange {
        relation: relation.clone(), basis: initial.result().tx_key(), scope, frontier: 1,
        until: Timestamp(200),
        region: Some(Arc::new(OwnedRangeEvidence::Entity {
            anchor: 1, region: OwnedEntityRegion { id: id.clone(), valid },
        })),
        revealed: None,
    };
    let disjoint = resolver.resolve_source(state.snapshot(), SourcePosition(1), Timestamp(101),
        source(vec![put("other", 425, 475), put("wanted", 500, 600)]), &context).await.unwrap();
    assert!(disjoint.result().committed());
    let accepted = resolver.resolve_source(state.snapshot(), SourcePosition(2), Timestamp(102),
        source(vec![check()]), &context).await.unwrap();
    assert!(accepted.result().committed(), "{:?}", accepted.result().error());

    let shadowed = resolver.resolve_source(state.snapshot(), SourcePosition(3), Timestamp(103),
        source(vec![put("wanted", 425, 475), TxOp::OwnedDelete {
            relation: relation.clone(), id: id.clone(),
            valid_from: Some(Timestamp(425)), valid_to: Some(Timestamp(475)),
        }]), &context).await.unwrap();
    assert!(shadowed.result().committed(), "{:?}", shadowed.result().error());
    let stale = resolver.resolve_source(state.snapshot(), SourcePosition(4), Timestamp(104),
        source(vec![check()]), &context).await.unwrap();
    assert_eq!(stale.result().error().unwrap().code, "rtdb.owned/stale-range");
}

#[tokio::test]
async fn fixed_range_checks_full_old_and_new_keys_at_the_pinned_valid_instant() {
    use rtdb_format::envelope::SourceMessage;
    use rtdb_types::{FixedRangeEvidence, FixedRangeKey, FixedRangeRule, OwnedRelation, OwnedSchema, PlacedAttemptScope};

    let mut context = context();
    let scope = PlacedAttemptScope::new("installed-replica", 1).unwrap();
    context.owned_witness_scope = Some(scope);
    let state = PartitionState::empty(Default::default()).unwrap();
    let relation = OwnedRelation::new("fixed").unwrap();
    let rule = FixedRangeRule { path: "by-key".into(), column: "key".into(), valid_at: Timestamp(450) };
    let schema = OwnedSchema::from_json(br#"{"fields":[{"name":"key","type":{"kind":"binary"},"optional":false,"nullable":false}]}"#).unwrap();
    let put = |id: &str, key, from, to| TxOp::OwnedReplaceDocs {
        relation: relation.clone(),
        documents: Documents::new(vec![StructFields::from([
            ("_id".into(), Value::Utf8(id.into())), ("key".into(), Value::Binary(vec![key])),
        ])], &context.memory).unwrap(),
        valid_from: Some(Timestamp(from)), valid_to: Some(Timestamp(to)),
    };
    let source = |operations| {
        let request = SourceTransaction::new(
            operations, SubmitOptions { user: Some("owner".into()), ..SubmitOptions::default() }, &context.memory,
        ).unwrap().with_placed_seed(Some(scope.issue()));
        let encoded = request.encode().unwrap();
        let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        SourceTransaction::decode(&message, &context.memory).unwrap()
    };
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let initial = resolver.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100), source(vec![
        TxOp::OwnedDeclareRelation { relation: relation.clone(), schema },
        TxOp::OwnedDeclareFixedRange { relation: relation.clone(), rule: rule.clone() },
        put("middle", 5, 400, 500),
    ]), &context).await.unwrap();
    assert!(initial.result().committed(), "{:?}", initial.result().error());

    let lower = FixedRangeKey::from_bound(&StructFields::from([
        ("key".into(), Value::Binary(vec![5])), ("_id".into(), Value::Utf8("a".into())),
    ]), "key").unwrap();
    let upper = FixedRangeKey::from_bound(&StructFields::from([
        ("key".into(), Value::Binary(vec![5])), ("_id".into(), Value::Utf8("z".into())),
    ]), "key").unwrap();
    let check = |basis, frontier, revision| TxOp::OwnedCheckRange {
        relation: relation.clone(), basis, scope, frontier, until: Timestamp(200),
        region: Some(Arc::new(rtdb_types::OwnedRangeEvidence::FixedPath(FixedRangeEvidence { rule: rule.clone(), revision, lower: Some(lower.clone()), upper: Some(upper.clone()) }))),
        revealed: None,
    };
    let other = resolver.resolve_source(state.snapshot(), SourcePosition(1), Timestamp(101),
        source(vec![put("zz", 5, 400, 500), put("middle", 8, 500, 550)]), &context).await.unwrap();
    assert!(other.result().committed());
    let accepted = resolver.resolve_source(state.snapshot(), SourcePosition(2), Timestamp(102),
        source(vec![check(initial.result().tx_key(), 1, 1)]), &context).await.unwrap();
    assert!(accepted.result().committed(), "{:?}", accepted.result().error());

    let leave = resolver.resolve_source(state.snapshot(), SourcePosition(3), Timestamp(103),
        source(vec![TxOp::OwnedDelete {
            relation: relation.clone(), id: rtdb_types::EntityId::String("middle".into()),
            valid_from: Some(Timestamp(425)), valid_to: Some(Timestamp(475)),
        }]), &context).await.unwrap();
    assert!(leave.result().committed());
    let stale = resolver.resolve_source(state.snapshot(), SourcePosition(4), Timestamp(104),
        source(vec![check(initial.result().tx_key(), 1, 1)]), &context).await.unwrap();
    assert_eq!(stale.result().error().unwrap().code, "rtdb.owned/stale-range");

    let enter = resolver.resolve_source(state.snapshot(), SourcePosition(5), Timestamp(105),
        source(vec![put("new", 5, 400, 500)]), &context).await.unwrap();
    assert!(enter.result().committed());
    let stale_empty = resolver.resolve_source(state.snapshot(), SourcePosition(6), Timestamp(106),
        source(vec![check(leave.result().tx_key(), 4, 3)]), &context).await.unwrap();
    assert_eq!(stale_empty.result().error().unwrap().code, "rtdb.owned/stale-range");

    let shadowed = resolver.resolve_source(state.snapshot(), SourcePosition(7), Timestamp(107),
        source(vec![
            put("briefly", 5, 400, 500),
            put("briefly", 9, 500, 550),
            put("briefly", 8, 400, 500),
        ]), &context).await.unwrap();
    assert!(shadowed.result().committed(), "{:?}", shadowed.result().error());
    let journal = relation.change_journal("owner").unwrap();
    let journal_batch = shadowed.writes().tables()[&journal].batch().unwrap();
    let StoredOperation::Put(entry) = journal_batch.view().read_event(0).unwrap().operation else {
        panic!("Expected an owned journal entry");
    };
    let key = |key| FixedRangeKey::from_cells(
        &Value::Binary(vec![key]), &rtdb_types::EntityId::String("briefly".into()),
    ).unwrap().to_value();
    let side = |slot, before, after| Value::Struct(StructFields::from([
        ("event_slot".into(), Value::I64(slot)),
        ("valid_at".into(), Value::I64(450)),
        ("before".into(), before),
        ("after".into(), after),
    ]));
    assert_eq!(entry["fixed_assertion_keys"], Value::Struct(StructFields::from([
        ("event_count".into(), Value::I64(3)),
        ("sides".into(), Value::list(vec![side(0, Value::Null, key(5)), side(2, key(5), key(8))])),
    ])));
    let stale_shadowed = resolver.resolve_source(state.snapshot(), SourcePosition(8), Timestamp(108),
        source(vec![check(enter.result().tx_key(), 5, 4)]), &context).await.unwrap();
    assert_eq!(stale_shadowed.result().error().unwrap().code, "rtdb.owned/stale-range");
}

#[tokio::test]
async fn hosted_control_replacement_and_owned_facts_abort_together() {
    use rtdb_format::envelope::SourceMessage;
    use rtdb_types::{OwnedRelation, PlacedAttemptScope};

    let mut context = context();
    let scope = PlacedAttemptScope::new("installed-replica", 1).unwrap();
    context.owned_witness_scope = Some(scope);
    context.placed_receipt_retirement = true;
    let state = PartitionState::empty(Default::default()).unwrap();
    let facts = OwnedRelation::new("facts").unwrap();
    let control = OwnedRelation::new("bj_control").unwrap();
    let table = rtdb_types::TableRef::new("public", control.name());
    let authority = OwnedRelation::new("bj_installation").unwrap();
    let claim = |writer: &str| TxOp::PutDocs {
        table: TableRef::new("public", authority.name()),
        documents: Documents::new(vec![StructFields::from([
            ("_id".into(), Value::Utf8("authority".into())),
            ("kind".into(), Value::Utf8("hosted".into())),
            ("incarnation".into(), Value::Binary(vec![7; 16])),
            ("writer".into(), Value::Utf8(writer.into())),
        ])], &context.memory).unwrap(),
        valid_from: Some(Timestamp(0)), valid_to: None,
    };
    let authority_check = |stamp: i64, grant: u8| {
        let mut bytes = [grant; rtdb_tx::HostedAuthority::WIDTH];
        bytes[..8].copy_from_slice(&stamp.to_be_bytes());
        bytes[8..24].fill(7);
        let mut controls = [rtdb_tx::HostedControlWrite::check(
            authority.clone(), scope, Some(Timestamp(stamp)),
            StructFields::from([("_id".into(), Value::Utf8("authority".into()))]),
        ).unwrap()];
        rtdb_tx::bind_installed_authority(&mut controls, rtdb_tx::HostedAuthority::decode(&bytes).unwrap()).unwrap();
        let [control] = controls;
        TxOp::HostedControl(control)
    };
    let control_row = |n| StructFields::from([
        ("_id".into(), Value::Utf8("head".into())), ("n".into(), Value::I64(n)),
    ]);
    let native_fact = || TxOp::OwnedInsertDocs {
        relation: facts.clone(), documents: Documents::new(vec![document(7)], &context.memory).unwrap(),
        valid_from: None, valid_to: None,
    };
    let replacement = |expected, n| TxOp::HostedControl(rtdb_tx::HostedControlWrite::new(
        control.clone(), scope, Some(Timestamp(expected)), control_row(n),
    ).unwrap());
    let source = |operations| {
        let request = SourceTransaction::new(
            operations, SubmitOptions { user: Some("owner".into()), ..SubmitOptions::default() }, &context.memory,
        ).unwrap().with_placed_seed(Some(scope.issue()));
        let encoded = request.encode().unwrap();
        let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        SourceTransaction::decode(&message, &context.memory).unwrap()
    };
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let initial = resolver.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100),
        source(vec![claim("11111111111111111111111111111111"), TxOp::PutDocs {
            table: table.clone(), documents: Documents::new(vec![control_row(1)], &context.memory).unwrap(),
            valid_from: Some(Timestamp(0)), valid_to: None,
        }]), &context).await.unwrap();
    assert!(initial.result().committed());
    let deadline = || TxOp::owned_deadline(Timestamp(200));
    let stale = resolver.resolve_source(state.snapshot(), SourcePosition(1), Timestamp(101),
        source(vec![deadline(), authority_check(100, 0x11), replacement(99, 2), native_fact()]), &context).await.unwrap();
    assert_eq!(stale.result().error().unwrap().code, "rtdb.hosted/stale-control");
    assert!(!stale.writes().tables().contains_key(&facts.table("owner")));

    let check = |expected| TxOp::HostedControl(rtdb_tx::HostedControlWrite::check(
        control.clone(), scope, Some(Timestamp(expected)),
        StructFields::from([("_id".into(), Value::Utf8("head".into()))]),
    ).unwrap());
    let verified = resolver.resolve_source(state.snapshot(), SourcePosition(2), Timestamp(102),
        source(vec![deadline(), authority_check(100, 0x11), check(100)]), &context).await.unwrap();
    assert!(verified.result().committed(), "{:?}", verified.result().error());
    assert!(!verified.writes().tables().contains_key(&table));
    let changed = resolver.resolve_source(state.snapshot(), SourcePosition(3), Timestamp(103),
        source(vec![deadline(), authority_check(100, 0x11), check(101), native_fact()]), &context).await.unwrap();
    assert_eq!(changed.result().error().unwrap().code, "rtdb.hosted/stale-control");
    assert!(!changed.writes().tables().contains_key(&facts.table("owner")));

    let staging_deadline = || TxOp::OwnedDeadline { until: Timestamp(200), recheck_at_staging: true, journal_before: None };
    let apply_staged = |resolver: &mut TxResolver| {
        while let Some(staged) = resolver.take_head() { state.apply(staged.writes()).unwrap(); }
    };
    apply_staged(&mut resolver);
    let expired = resolver.resolve_source_for_writer(state.snapshot(), SourcePosition(4), Timestamp(50),
        source(vec![staging_deadline(), authority_check(100, 0x11), replacement(100, 2), native_fact()]), &context,
        Some(WriterPublication {
            clock: &|| Timestamp(199),
            maximum_inline_bytes: Some(1 << 20),
            maximum_timed_original_bytes: None,
        }),
    ).await.unwrap();
    assert_eq!(expired.result().error().unwrap().code, "rtdb.owned/expired");
    assert!(expired.placed_attempt().is_some());
    assert!(!expired.writes().tables().contains_key(&table));
    assert!(!expired.writes().tables().contains_key(&facts.table("owner")));
    let history = &expired.writes().tables()[&TableRef::new("xt", "txs")];
    let StoredOperation::Put(history) = history.batch().unwrap().view().read_event(0).unwrap().operation else {
        panic!("expected rebuilt abort history");
    };
    assert!(matches!(history["source_log_time"], Value::Timestamp { value: 50, .. }));
    assert!(matches!(history["receipt_time"], Value::Timestamp { value: 103, .. }));
    assert!(matches!(history["system_time"], Value::Timestamp { value: 104, .. }));

    apply_staged(&mut resolver);
    let oversized = resolver.resolve_source_for_writer(state.snapshot(), SourcePosition(5), Timestamp(105),
        source(vec![staging_deadline(), authority_check(100, 0x11), replacement(100, 2), native_fact()]), &context,
        Some(WriterPublication {
            clock: &|| Timestamp(198),
            maximum_inline_bytes: Some(100),
            maximum_timed_original_bytes: None,
        }),
    ).await.unwrap();
    assert_eq!(oversized.result().error().unwrap().code, "rtdb.owned/replica-inline-bound");
    assert!(oversized.placed_attempt().is_some());
    assert!(!oversized.writes().tables().contains_key(&table));
    assert!(!oversized.writes().tables().contains_key(&facts.table("owner")));

    apply_staged(&mut resolver);
    let mut staging_only = TxResolver::new(
        Some(oversized.result().tx_key()), SourcePosition(5), None, &context.memory,
    ).unwrap();
    let physical = TxOp::OwnedDeadline {
        until: Timestamp(200),
        recheck_at_staging: true,
        journal_before: Some(rtdb_types::TimedJournalCutoff::new(Timestamp(220), [7; 32]).unwrap()),
    };
    let refused = staging_only.resolve_source_for_writer(
        state.snapshot(), SourcePosition(6), Timestamp(106),
        source(vec![physical, authority_check(100, 0x11), replacement(100, 2), native_fact()]), &context,
        Some(WriterPublication {
            clock: &|| Timestamp(198),
            maximum_inline_bytes: Some(1 << 20),
            maximum_timed_original_bytes: None,
        }),
    ).await.unwrap();
    assert_eq!(refused.result().error().unwrap().code, "rtdb.owned/timed-original-quorum-unavailable");
    assert!(!refused.writes().tables().contains_key(&table));
    assert!(!refused.writes().tables().contains_key(&facts.table("owner")));
    assert!(refused.staging_clock_upper().is_none());

    let accepted = resolver.resolve_source_for_writer(state.snapshot(), SourcePosition(6), Timestamp(106),
        source(vec![staging_deadline(), authority_check(100, 0x11), replacement(100, 2), native_fact()]), &context,
        Some(WriterPublication {
            clock: &|| Timestamp(198),
            maximum_inline_bytes: Some(1 << 20),
            maximum_timed_original_bytes: None,
        }),
    ).await.unwrap();
    assert!(accepted.result().committed(), "{:?}", accepted.result().error());
    assert!(accepted.writes().tables().contains_key(&table));
    assert!(accepted.writes().tables().contains_key(&facts.table("owner")));
    assert_eq!(accepted.staging_clock_upper(), Some(Timestamp(199)));
    let Some(rtdb_format::envelope::ReplicaBody::ResolvedTx(message)) = accepted.inline_replica_body() else { panic!("expected an inline replica transaction"); };
    let (attempt, retirement) = rtdb_format::envelope::decode_replica_outcome_attempt(message.placed_attempt.as_deref().unwrap()).unwrap();
    assert_eq!(Some(attempt), accepted.placed_attempt());
    assert!(retirement);
    let stage = message.checked_acceptance_stage().unwrap().unwrap();
    assert_eq!(stage.clock_upper, Timestamp(199));
    assert_eq!(stage.cutoff, Timestamp(200));
    let replayed = rtdb_tx::decode_replica(message, Some(oversized.result().tx_key()), accepted.writes().block_index(), &context.memory).unwrap();
    assert!(replayed.writes.tables().contains_key(&table));
    assert!(replayed.writes.tables().contains_key(&facts.table("owner")));
    let refused = resolver.resolve_source(state.snapshot(), SourcePosition(7), Timestamp(107),
        source(vec![deadline(), authority_check(100, 0x11), replacement(106, 3), native_fact()]), &context).await.unwrap();
    assert!(!refused.result().committed());
    assert!(!refused.writes().tables().contains_key(&table));

    let malformed = resolver.resolve_source(state.snapshot(), SourcePosition(8), Timestamp(108),
        source(vec![TxOp::PutDocs {
            table: table.clone(), documents: Documents::new(vec![control_row(4)], &context.memory).unwrap(),
            valid_from: Some(Timestamp(12)), valid_to: None,
        }]), &context).await.unwrap();
    assert!(malformed.result().committed());
    let refused_slice = resolver.resolve_source(state.snapshot(), SourcePosition(9), Timestamp(109),
        source(vec![deadline(), authority_check(100, 0x11), check(108)]), &context).await.unwrap();
    assert_eq!(refused_slice.result().error().unwrap().code, "rtdb.hosted/control-scope");
    assert!(!refused_slice.writes().tables().contains_key(&table));

    let replaced = resolver.resolve_source(state.snapshot(), SourcePosition(10), Timestamp(110),
        source(vec![claim("22222222222222222222222222222222")]), &context).await.unwrap();
    assert!(replaced.result().committed());
    for (offset, fence, error) in [
        (11, None, "rtdb.hosted/authority-required"),
        (12, Some(authority_check(100, 0x11)), "rtdb.hosted/stale-authority"),
        (13, Some(authority_check(110, 0x11)), "rtdb.hosted/stale-authority"),
    ] {
        let operations = [deadline()].into_iter().chain(fence)
            .chain([replacement(108, 5), native_fact()]).collect();
        let refused = resolver.resolve_source(state.snapshot(), SourcePosition(offset), Timestamp(100 + offset),
            source(operations), &context).await.unwrap();
        assert_eq!(refused.result().error().unwrap().code, error);
        assert!(!refused.writes().tables().contains_key(&table));
        assert!(!refused.writes().tables().contains_key(&facts.table("owner")));
    }
}

struct ClocklessRecord;

#[async_trait::async_trait]
impl TransactionWriter for ClocklessRecord {
    async fn write(&mut self, _: &mut OpenTransaction<'_>) -> Result<WriterResult> {
        Ok(WriterResult::Committed { user_metadata: None })
    }
}

#[tokio::test]
async fn an_external_successor_cannot_reopen_an_expired_bounded_retry() {
    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let metadata = Value::Struct(StructFields::from([(
        "rtdb_retry_v1".into(), Value::Struct(StructFields::from([
            ("nonce".into(), Value::Utf8("00112233445566778899aabbccddeeff".into())),
            ("not_after_micros".into(), Value::I64(1_000_000)),
        ])),
    )]));
    let source = || SourceTransaction::new(vec![put(&context, 1, 0, i64::MAX)], SubmitOptions {
        user_metadata: Some(UserMetadata::new(metadata.clone(), &context.memory).unwrap()),
        ..Default::default()
    }, &context.memory).unwrap();
    let original = resolver.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100), source(), &context).await.unwrap();
    assert!(original.result().committed());
    state.apply(resolver.take_head().unwrap().writes()).unwrap();
    let cut = resolver.resolve_source(state.snapshot(), SourcePosition(1), Timestamp(1_000_000), request(vec![], &context), &context).await.unwrap();
    let receipts = &cut.writes().tables()[&TableRef::new("xt", "bounded_retry_receipts")];
    assert!(matches!(receipts.batch().unwrap().view().read_event(0).unwrap().operation, StoredOperation::Erase));
    state.apply(resolver.take_head().unwrap().writes()).unwrap();
    let clockless = resolver.resolve_external(state.snapshot(), Timestamp(1_000_001), None, None, &mut ClocklessRecord, &context).await.unwrap();
    let history = &clockless.writes().tables()[&TableRef::new("xt", "txs")];
    let StoredOperation::Put(history) = history.batch().unwrap().view().read_event(0).unwrap().operation else {
        panic!("expected clockless transaction history");
    };
    assert_eq!(history.get("source_log_time"), Some(&Value::Null));
    assert!(matches!(history.get("receipt_time"), Some(Value::Timestamp { value: 1_000_001, .. })));
    state.apply(resolver.take_head().unwrap().writes()).unwrap();

    let mut reopened = TxResolver::new(Some(clockless.result().tx_key()), SourcePosition(2), None, &context.memory).unwrap();
    let retry = reopened.resolve_source(state.snapshot(), SourcePosition(3), Timestamp(99), source(), &context).await.unwrap();
    assert_eq!(retry.result().error().unwrap().code, "rtdb/idempotency-expired");
}

#[tokio::test]
async fn erasing_a_record_retires_only_metadata_bound_to_that_record() {
    use rtdb_types::EntityId;

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let bound = |entity| {
        let metadata = Value::Struct(StructFields::from([(
            "rtdb_erasure_unit_v1".into(), Value::Struct(StructFields::from([
                ("table".into(), Value::Utf8("public.docs".into())),
                ("id".into(), Value::I64(entity)),
            ])),
        )]));
        SourceTransaction::new(vec![put(&context, 42, 0, i64::MAX)], SubmitOptions {
            user_metadata: Some(UserMetadata::new(metadata, &context.memory).unwrap()),
            ..Default::default()
        }, &context.memory).unwrap()
    };
    let sidecar = TableRef::new("xt", "idempotency_receipts");
    for (position, request, retained) in [(0, bound(1), true), (1, bound(2), false)] {
        let accepted = resolver.resolve_source(state.snapshot(), SourcePosition(position), Timestamp(100), request, &context).await.unwrap();
        assert!(accepted.result().committed(), "{:?}", accepted.result().error());
        assert_eq!(accepted.writes().tables().contains_key(&sidecar), retained);
        state.apply(resolver.take_head().unwrap().writes()).unwrap();
    }
    let erase = request(vec![sql("ERASE FROM docs WHERE _id = 1")], &context);
    let erased = resolver.resolve_source(state.snapshot(), SourcePosition(2), Timestamp(101), erase, &context).await.unwrap();
    assert!(erased.result().committed(), "{:?}", erased.result().error());
    let events = erased.writes().tables()[&sidecar].batch().unwrap();
    assert_eq!(events.len(), 1);
    let event = events.view().read_event(0).unwrap();
    assert_eq!(event.iid, EntityId::Integer(0).iid());
    assert!(matches!(event.operation, StoredOperation::Erase));
    let tombstone = erased.writes().tables()[&table()].batch().unwrap().header(0).unwrap();
    assert!(tombstone.position.unwrap().ordinal < event.position.unwrap().ordinal);
}

#[tokio::test]
async fn retiring_a_legacy_receipt_uses_the_id_hash_even_with_a_noncanonical_stored_iid() -> Result<()> {
    use rtdb_format::data::StoredEvent;
    use rtdb_index::TransactionBuffer;
    use rtdb_types::{EntityId, Iid, TransactionKey, TxId};

    let context = context();
    let state = PartitionState::empty(Default::default())?;
    let predecessor = TransactionKey { tx_id: TxId(63), system_time: Timestamp(100) };
    let receipts = TableRef::new("xt", "idempotency_receipts");
    let id = EntityId::String("historical/reçu".into());
    let stored_iid = Iid([0x5a; 16]);
    let mut legacy = TransactionBuffer::new(state.snapshot(), predecessor, Arc::clone(&context.memory))?;
    legacy.append(receipts.clone(), StoredEvent {
        position: None, origin: None, commit_position: 0, entity_key: None,
        iid: stored_iid, system_from: predecessor.system_time, valid_time: Interval::from_micros(0, 90)?,
        operation: StoredOperation::Put(StructFields::from([("_id".into(), id.as_value())])), recency: None,
    })?;
    state.apply(&legacy.stage()?)?;

    let mut resolver = TxResolver::new(Some(predecessor), SourcePosition(63), None, &context.memory)?;
    let retired = resolver.resolve_source(state.snapshot(), SourcePosition(64), Timestamp(110), request(vec![], &context), &context).await?;
    assert!(retired.result().committed(), "{:?}", retired.result().error());
    let batch = retired.writes().tables()[&receipts].batch().unwrap();
    assert_eq!(batch.len(), 1);
    let event = batch.view().read_event(0)?;
    assert_ne!(event.iid, stored_iid);
    assert_eq!(event.iid, id.iid());
    assert_eq!(event.operation, StoredOperation::Erase);
    assert_eq!(event.valid_time, Interval::ALL);
    assert_eq!(event.system_from, retired.result().tx_key().system_time);
    Ok(())
}

#[tokio::test]
async fn source_metadata_expires_at_the_installed_cut_but_its_historical_outcome_survives() {
    use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext};
    use rtdb_types::EntityId;

    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let metadata = Value::Struct(StructFields::from([("note".into(), Value::Utf8("transient".into()))]));
    let source = SourceTransaction::new(vec![], SubmitOptions {
        user_metadata: Some(UserMetadata::new(metadata.clone(), &context.memory).unwrap()),
        ..Default::default()
    }, &context.memory).unwrap();
    let initial = resolver.resolve_source(state.snapshot(), SourcePosition(0), Timestamp(100), source, &context).await.unwrap();
    let history = &initial.writes().tables()[&TableRef::new("xt", "txs")];
    let StoredOperation::Put(row) = history.batch().unwrap().view().read_event(0).unwrap().operation else {
        panic!("expected history");
    };
    assert_eq!(row["user_metadata"], Value::Null);
    let body = initial.to_replica_body(&context.memory).unwrap();
    let rtdb_format::envelope::ReplicaBody::ResolvedTx(mut copied) = body.body().clone() else {
        panic!("expected copied replica frame");
    };
    copied.tx_id = Some(10);
    copied.src_msg_id = Some(10);
    let recovered = decode_replica(&copied, None, state.snapshot().block_index(), &context.memory).unwrap();
    state.apply(&recovered.writes).unwrap();
    let mut resolver = TxResolver::new(Some(recovered.result.tx_key()), SourcePosition(10), None, &context.memory).unwrap();

    let query = async |snapshot: rtdb_index::Snapshot, text: &str| {
        let snapshot = snapshot.with_system_time_cap(Some(Timestamp(100))).unwrap();
        let catalog = CatalogSnapshot::new("xtdb", [(
            "xtdb".into(), DatabaseSnapshot { partitions: vec![snapshot], storage: Arc::clone(&context.storage) },
        )].into(), false).unwrap();
        let query_context = QueryContext::new(catalog, Arc::clone(&context.memory), Timestamp(100), "UTC", context.query_options.clone(), &context.cancellation).unwrap();
        let sql = rtdb_sql::prepare_in(text, &query_context.catalog, &Default::default(), &context.memory).unwrap();
        let mut result = PreparedQuery::from_prepared(sql, &context.memory).unwrap().execute(query_context, vec![]).await.unwrap();
        let mut values = Vec::new();
        while let Some(batch) = result.next_batch().await.unwrap() {
            let page = batch.batch();
            for row in 0..page.num_rows() {
                values.push((0..page.num_columns()).map(|col| {
                    rtdb_columnar::read_value(page.schema().field(col), page.column(col).as_ref(), row).unwrap()
                }).collect::<Vec<_>>());
            }
        }
        values
    };
    let history = "SELECT committed, user_metadata FROM xt.txs FOR ALL VALID_TIME WHERE _id = 0";
    let predicate = "SELECT committed, user_metadata FROM xt.txs FOR ALL VALID_TIME WHERE _id = 0 AND user_metadata IS NOT NULL";
    let clockless = resolver.resolve_external(state.snapshot(), Timestamp(101), None, None, &mut ClocklessRecord, &context).await.unwrap();
    assert!(clockless.result().committed());
    state.apply(resolver.take_head().unwrap().writes()).unwrap();
    assert_eq!(query(state.snapshot(), predicate).await, [vec![Value::Bool(true), metadata]]);
    let ordinals = "SELECT attname, attnum, atttypid FROM pg_catalog.pg_attribute         WHERE attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 'txs')         AND attname IN ('receipt_time', 'source_log_time', 'system_time') ORDER BY attnum";
    assert_eq!(query(state.snapshot(), ordinals).await, [
        vec![Value::Utf8("receipt_time".into()), Value::I32(8), Value::I32(1184)],
        vec![Value::Utf8("source_log_time".into()), Value::I32(9), Value::I32(1184)],
        vec![Value::Utf8("system_time".into()), Value::I32(10), Value::I32(1184)],
    ]);
    let expiry = 100 + RECEIPT_WINDOW_MICROS;
    let cut = resolver.resolve_source(state.snapshot(), SourcePosition(12), Timestamp(expiry), request(vec![], &context), &context).await.unwrap();
    let sidecar = cut.writes().tables()[&TableRef::new("xt", "idempotency_receipts")].batch().unwrap();
    let erased = sidecar.view().read_event(0).unwrap();
    assert_eq!(erased.iid, EntityId::Integer(0).iid());
    assert!(matches!(erased.operation, StoredOperation::Erase));
    state.apply(resolver.take_head().unwrap().writes()).unwrap();
    assert_eq!(query(state.snapshot(), history).await, [vec![Value::Bool(true), Value::Null]]);
    assert!(query(state.snapshot(), predicate).await.is_empty());
}

#[tokio::test]
async fn a_versioned_placed_outcome_retires_expired_receipts_and_replays_its_current_proof() {
    use rtdb_format::envelope::ReplicaBody;
    use rtdb_types::{EntityId, PlacedAttempt, PlacedAttemptScope, PLACED_ATTEMPT_OUTCOME_TABLE};

    let state = PartitionState::empty(Default::default()).unwrap();
    let mut context = context();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let placed = |epoch, context: &ResolveContext| {
        request(vec![], context).with_placed_seed(Some(PlacedAttemptScope::new("same/history", epoch).unwrap().issue()))
    };
    let table = TableRef::new("xt", PLACED_ATTEMPT_OUTCOME_TABLE);
    let mut old = Vec::new();
    let mut future = None;
    for position in 0..3 {
        let predecessor = state.snapshot().tx_basis();
        let resolved = resolver.resolve_source(
            state.snapshot(), SourcePosition(position), Timestamp(100 + position), placed(1, &context), &context,
        ).await.unwrap();
        let body = resolved.to_replica_body(&context.memory).unwrap();
        let ReplicaBody::ResolvedTx(tx) = body.body() else { panic!("expected legacy placed frame"); };
        let attempt = PlacedAttempt::decode(tx.placed_attempt.as_deref().unwrap()).unwrap();
        if position < 2 {
            old.push(EntityId::String(attempt.outcome_key()).iid());
        } else {
            future = Some(attempt);
        }
        let recovered = decode_replica(tx, predecessor, state.snapshot().block_index(), &context.memory).unwrap();
        state.apply(&recovered.writes).unwrap();
        resolver.take_head().unwrap();
    }

    let predecessor = state.snapshot().tx_basis();
    let mut resolver = TxResolver::new(predecessor, SourcePosition(2), None, &context.memory).unwrap();
    context.placed_receipt_retirement = true;
    let resolved = resolver.resolve_source(
        state.snapshot(), SourcePosition(3), Timestamp(101 + RECEIPT_WINDOW_MICROS), placed(2, &context), &context,
    ).await.unwrap();
    let attempt = resolved.placed_attempt().unwrap();
    let body = resolved.to_replica_body(&context.memory).unwrap();
    let ReplicaBody::ResolvedTx(tx) = body.body() else { panic!("expected versioned placed frame"); };
    assert!(PlacedAttempt::decode(tx.placed_attempt.as_deref().unwrap()).is_err());
    let mut wrong_decision = tx.clone();
    wrong_decision.committed = tx.committed.map(|committed| !committed);
    assert!(decode_replica(&wrong_decision, predecessor, state.snapshot().block_index(), &context.memory).is_err());
    wrong_decision.committed = None;
    assert!(decode_replica(&wrong_decision, predecessor, state.snapshot().block_index(), &context.memory).is_err());

    let mut unmarked = tx.clone();
    unmarked.placed_attempt = Some(attempt.encode().to_vec().into());
    assert!(decode_replica(&unmarked, predecessor, state.snapshot().block_index(), &context.memory).is_err());
    unmarked.placed_attempt = None;
    assert!(decode_replica(&unmarked, predecessor, state.snapshot().block_index(), &context.memory).is_err());

    let recovered = decode_replica(tx, predecessor, state.snapshot().block_index(), &context.memory).unwrap();
    assert_eq!(recovered.placed_attempt, Some(attempt));
    let rows = recovered.writes.tables()[&table].batch().unwrap();
    let retired: std::collections::BTreeSet<_> = (0..rows.len()).filter_map(|row| {
        let event = rows.view().read_event(row).unwrap();
        matches!(event.operation, StoredOperation::Erase).then_some(event.iid)
    }).collect();
    assert_eq!(retired, old.into_iter().collect());
    state.apply(&recovered.writes).unwrap();
    let mut reopened = TxResolver::new(Some(recovered.result.tx_key()), SourcePosition(3), None, &context.memory).unwrap();
    let assertion = format!(
        "ASSERT (SELECT COUNT(*) FROM xt.placed_attempt_outcomes FOR ALL VALID_TIME WHERE _id IN ('{}', '{}')) = 2 \
            AND (SELECT COUNT(*) FROM xt.placed_attempt_outcomes FOR ALL VALID_TIME) = 2",
        attempt.outcome_key(), future.unwrap().outcome_key(),
    );
    let read = reopened.resolve_source(
        state.snapshot(), SourcePosition(4), Timestamp(99), request(vec![sql(&assertion)], &context), &context,
    ).await.unwrap();
    assert!(read.result().committed(), "{:?}", read.result().error());
}

/// Statements a long-lived resolver plans once and reuses across transactions must resolve exactly as a resolver that
/// plans each from scratch: same outcome, same column types, same events. The schema changes as the statements run,
/// so reused plans meet catalogs other than the one they were planned against.
#[tokio::test]
async fn keyed_writes_resolve_the_same_whether_their_plans_are_reused_or_planned_afresh() {
    use rtdb_format::envelope::SourceMessage;

    let context = context();
    let time = |micros: i64| Value::Timestamp { value: micros, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = move |bound: u64| { seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (seed >> 33) % bound };
    let statement = |choice: u64, id: i64, value: i64, from: i64| -> (&'static str, Vec<Value>) {
        match choice {
            0 => ("INSERT INTO docs (_id, v, name) VALUES (?, ?, ?)", vec![Value::I64(id), Value::I64(value), Value::Utf8(format!("n{value}"))]),
            1 => ("UPDATE docs SET v = v + 1 WHERE _id = ?", vec![Value::I64(id)]),
            2 => ("UPDATE docs SET name = ? WHERE _id = ?", vec![Value::Utf8(format!("renamed{value}")), Value::I64(id)]),
            3 => ("UPDATE docs FOR PORTION OF VALID_TIME FROM ? TO ? SET v = ? WHERE _id = ?",
                  vec![time(from), time(from + 50), Value::I64(value), Value::I64(id)]),
            4 => ("DELETE FROM docs WHERE _id = ?", vec![Value::I64(id)]),
            5 => ("DELETE FROM docs FOR PORTION OF VALID_TIME FROM ? TO ? WHERE _id = ?", vec![time(from), time(from + 30), Value::I64(id)]),
            6 => ("UPDATE docs SET v = ? WHERE _id = ?", vec![Value::Utf8(format!("text{value}")), Value::I64(id)]),
            7 => ("INSERT INTO docs (_id, extra) VALUES (?, ?)", vec![Value::I64(id), Value::F64(value as f64 / 4.0)]),
            8 => ("UPDATE docs SET v = v WHERE _id = ?", vec![Value::I64(id)]),
            9 => ("UPDATE docs SET extra = ? WHERE _id = ? AND v > 3", vec![Value::I64(value), Value::I64(id)]),
            _ => ("ERASE FROM docs WHERE _id = ?", vec![Value::I64(id)]),
        }
    };
    let reused = PartitionState::empty(Default::default()).unwrap();
    let fresh = PartitionState::empty(Default::default()).unwrap();
    let mut long_lived = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();
    let mut previous = None;
    let (mut committed, mut rows) = (0, 0);
    for position in 0..400i64 {
        let operations = 1 + next(3);
        let ops: Vec<_> = (0..operations).map(|_| {
            let choice = if position < 8 { 0 } else { next(11) };
            let (sql, args) = statement(choice, next(6) as i64, next(10) as i64, 100 + 10 * position + next(40) as i64);
            TxOp::Sql { sql: sql.into(), args: Some(SqlArguments::from_rows(vec![args], &context.memory).unwrap()) }
        }).collect();
        let encoded = request(ops, &context).encode().unwrap();
        let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        let decode = || SourceTransaction::decode(&message, &context.memory).unwrap();
        let log_time = Timestamp(1_000 + 10 * position);
        let mut afresh = TxResolver::new(previous, SourcePosition(position - 1), None, &context.memory).unwrap();
        let planned = afresh.resolve_source(fresh.value().snapshot(), SourcePosition(position), log_time, decode(), &context).await.unwrap();
        let kept = long_lived.resolve_source(reused.value().snapshot(), SourcePosition(position), log_time, decode(), &context).await.unwrap();
        let outcome = |resolved: &ResolvedTx| (resolved.result().committed(), resolved.result().error().map(|error| error.code.clone()), resolved.result().tx_key());
        assert_eq!(outcome(&kept), outcome(&planned), "position {position}");
        let (kept_writes, planned_writes) = (kept.writes(), planned.writes());
        assert_eq!(kept_writes.tables().keys().collect::<Vec<_>>(), planned_writes.tables().keys().collect::<Vec<_>>(), "position {position}");
        for (table, write) in kept_writes.tables() {
            let other = &planned_writes.tables()[table];
            assert_eq!(write.column_types(), other.column_types(), "position {position} {table:?}");
            assert_eq!(write.declared_columns(), other.declared_columns(), "position {position} {table:?}");
            assert_eq!(write.batch().map(|batch| batch.arrow().clone()), other.batch().map(|batch| batch.arrow().clone()), "position {position} {table:?}");
        }
        if planned.result().committed() { committed += 1; }
        rows += planned_writes.tables().get(&table()).map_or(0, |write| write.row_count());
        previous = Some(planned.result().tx_key());
        reused.apply(kept_writes).unwrap();
        fresh.apply(planned_writes).unwrap();
        long_lived.take_head().unwrap();
        afresh.take_head().unwrap();
    }
    assert!(committed > 300 && rows > 250, "the statements must mostly commit and write: {committed} committed, {rows} rows");
}

#[tokio::test]
async fn empty_delete_docs_skips_valid_time_validation_while_still_forbidding_system_tables() {
    let context = context();
    let state = PartitionState::empty(Default::default()).unwrap();
    let mut resolver = TxResolver::new(None, SourcePosition(-1), None, &context.memory).unwrap();

    let empty = TxOp::DeleteDocs {
        table: table(),
        ids: EntityIds::new(vec![], &context.memory).unwrap(),
        valid_from: Some(Timestamp(200)),
        valid_to: Some(Timestamp(100)),
    };
    let resolved = resolver.resolve_source(state.value().snapshot(), SourcePosition(0), Timestamp(100), request(vec![empty], &context), &context).await.unwrap();
    assert!(resolved.result().committed(), "empty DeleteDocs should be a no-op before valid-time validation: {:?}", resolved.result().error());

    let forbidden = TxOp::DeleteDocs {
        table: TableRef::new("xt", "txs"),
        ids: EntityIds::new(vec![], &context.memory).unwrap(),
        valid_from: None,
        valid_to: None,
    };
    let resolved = resolver.resolve_source(state.value().snapshot(), SourcePosition(1), Timestamp(101), request(vec![forbidden], &context), &context).await.unwrap();
    assert!(!resolved.result().committed(), "forbidden tables must still be rejected even when ids are empty");

    let non_empty_inverted = TxOp::DeleteDocs {
        table: table(),
        ids: EntityIds::new(vec![rtdb_types::EntityId::Integer(1)], &context.memory).unwrap(),
        valid_from: Some(Timestamp(200)),
        valid_to: Some(Timestamp(100)),
    };
    let resolved = resolver.resolve_source(state.value().snapshot(), SourcePosition(2), Timestamp(102), request(vec![non_empty_inverted], &context), &context).await.unwrap();
    assert!(!resolved.result().committed(), "non-empty DeleteDocs with inverted valid time must abort");
}
