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

#[tokio::test]
async fn prepared_temporal_cast_schema_rebinds_to_execution_timezone() {
    let memory = MemoryBudget::new(1 << 26);
    let initial = context(None,&memory);
    let database = DatabaseSnapshot { partitions: Vec::new(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    let later = QueryContext::new(catalog,Arc::clone(&memory),Timestamp(100),"Europe/Paris",QueryOptions::default(),&CancellationToken::new()).unwrap();
    let statement = PreparedSql::prepare("SELECT CAST('1970-01-01Z' AS TIMESTAMPTZ) AS value",&initial,PrepareOptions::default(),ResultSignaturePolicy::Dynamic).unwrap();
    let expected = LogicalType::Scalar(ScalarType::Timestamp { unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("Europe/Paris".into()) });
    assert_eq!(statement.result_schema(&later,&[]).unwrap().schema().fields()[0].data_type,expected);
    let mut cursor = statement.open_query(Arc::clone(&later),vec![]).await.unwrap();
    assert_eq!(cursor.runtime_schema().fields()[0].data_type,expected);
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(read_value(page.batch().schema().field(0),page.batch().column(0).as_ref(),0).unwrap(),Value::Timestamp { value: 0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("Europe/Paris".into()) });
    let exact = PreparedSql::prepare("SELECT CAST('1970-01-01Z' AS TIMESTAMPTZ) AS value",&initial,PrepareOptions::default(),ResultSignaturePolicy::Exact).unwrap();
    assert_eq!(exact.open_query(later,vec![]).await.err().unwrap().code,"xtdb/prepared-query-out-of-date");
}
fn write(state: &PartitionState, tx: i64, value: Value, memory: &Arc<MemoryBudget>) {
    let mut buffer = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(tx), system_time: Timestamp(tx) }, Arc::clone(memory)).unwrap();
    buffer.put(TableRef::new("public", "docs"), BTreeMap::from([("_id".into(), Value::I64(1)), ("v".into(), value)]), Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
    state.apply(&buffer.stage().unwrap()).unwrap();
}

#[tokio::test]
async fn minmax_type_errors_are_emitted_without_reading_input_rows() {
    let context = context(None, &MemoryBudget::new(1 << 26));
    for (values, category, code) in [
        ("(12), ('twelve')", rtdb_types::AnomalyCategory::Incorrect, "xtdb.group-by/incomparable-min-max-types"),
        ("(true)", rtdb_types::AnomalyCategory::Unsupported, "xtdb.group-by/unsupported-min-max-type"),
        ("(ARRAY[12])", rtdb_types::AnomalyCategory::Unsupported, "xtdb.group-by/unsupported-min-max-type"),
    ] {
        let sql = format!("SELECT MIN(v) AS m FROM (VALUES {values}) AS t(v) WHERE FALSE");
        let statement = PreparedSql::prepare(&sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
        assert_eq!(statement.prepared_schema().unwrap().fields()[0].name, "m");
        let description_error = statement.result_schema(&context, &[]).err().unwrap();
        assert_eq!(description_error.category, category);
        assert_eq!(description_error.code, code);
        let json = serde_json::to_value(rtdb_format::diagnostics::JsonAnomaly::from(&description_error)).unwrap();
        if values == "(true)" {
            assert_eq!(json["message"], "Unsupported type in min/max aggregate: :bool");
            assert_eq!(json["data"]["unsupported-type"], "bool");
        }
        if values == "(ARRAY[12])" {
            assert_eq!(json["message"], "Unsupported type in min/max aggregate: [:list :i64]");
            assert_eq!(json["data"]["unsupported-type"], serde_json::json!(["list", "i64"]));
        }
        let opening_error = statement.open_query(Arc::clone(&context), vec![]).await.err().unwrap();
        assert_eq!(description_error, opening_error);
    }

    let statement = PreparedSql::prepare("SELECT MAX(v) AS m FROM (VALUES (1)) AS t(v) WHERE FALSE", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let described = statement.result_schema(&context, &[]).unwrap();
    let mut cursor = statement.open_query(context, vec![]).await.unwrap();
    assert_eq!(described.schema(), cursor.runtime_schema());
    let batch = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(read_value(batch.batch().schema().field(0), batch.batch().column(0).as_ref(), 0).unwrap(), Value::Null);
}

#[tokio::test]
async fn emitted_array_aggregate_fields_agree_for_description_and_open() {
    let context = context(None, &MemoryBudget::new(1 << 26));
    let sql = "SELECT k, ARRAY_AGG(v) AS a FROM (VALUES (1, 12), (1, 13)) AS t(k, v) GROUP BY k";
    let statement = PreparedSql::prepare(sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let described = statement.result_schema(&context, &[]).unwrap();
    let cursor = statement.open_query(context, vec![]).await.unwrap();
    assert_eq!(described.schema().fields()[1].data_type, LogicalType::list(LogicalType::Scalar(ScalarType::I64)));
    assert_eq!(described.schema(), cursor.schema());
    assert_eq!(described.schema(), cursor.runtime_schema());
}

#[tokio::test]
async fn aggregate_factory_errors_precede_sql_setting_values() {
    let context = context(None, &MemoryBudget::new(1 << 26));
    let sql = "SETTING CLOCK_TIME=? SELECT MIN(v) FROM (VALUES (1), ('a')) AS t(v)";
    let statement = PreparedSql::prepare(sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let description_error = statement.result_schema(&context, &[LogicalType::Scalar(ScalarType::Utf8)]).err().unwrap();
    assert_eq!(description_error.code, "xtdb.group-by/incomparable-min-max-types");
    let opening_error = statement.open_query(context, vec![Value::Utf8("bad".into())]).await.err().unwrap();
    assert_eq!(opening_error, description_error);
}

#[tokio::test]
async fn unsigned_query_arguments_share_signed_scalar_and_aggregate_semantics() {
    let context = context(None, &MemoryBudget::new(1 << 27));
    let statement = PreparedSql::prepare("SELECT $1 AS value,$1=-1 AS equal,CAST($1 AS BIGINT) AS cast_value,STR($1) AS text,$1+2 AS added", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = statement.open_query(Arc::clone(&context), vec![Value::U64(u64::MAX)]).await.unwrap();
    let page = cursor.next_batch().await.unwrap().unwrap();
    let row = page.batch().schema().fields().iter().zip(page.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), 0).unwrap()).collect::<Vec<_>>();
    assert_eq!(row, [Value::I64(-1), Value::Bool(true), Value::I64(-1), Value::Utf8("-1".into()), Value::I64(1)]);
    assert_eq!(cursor.runtime_schema().fields()[0].data_type, LogicalType::Scalar(ScalarType::I64));
    let statement = PreparedSql::prepare("SELECT SUM(v) AS sum,AVG(v) AS average,PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) AS discrete,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v) AS continuous,COUNT(DISTINCT v) AS distinct_count FROM (VALUES ($1),($2)) t(v)", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = statement.open_query(Arc::clone(&context), vec![Value::U64(u64::MAX), Value::U64(1)]).await.unwrap();
    let page = cursor.next_batch().await.unwrap().unwrap();
    let row = page.batch().schema().fields().iter().zip(page.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), 0).unwrap()).collect::<Vec<_>>();
    assert_eq!(row, [Value::I64(0), Value::F64(0.0), Value::I64(-1), Value::F64(0.0), Value::I64(2)]);
    let statement = PreparedSql::prepare("SELECT COUNT(DISTINCT v) AS n FROM (VALUES ($1),($2)) t(v)", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = statement.open_query(context, vec![Value::U64(1 << 53), Value::U64((1 << 53) + 1)]).await.unwrap();
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), Value::I64(2));
}

#[tokio::test]
async fn typed_unsigned_nulls_and_empty_lists_keep_opened_vector_fields() {
    let context = context(None, &MemoryBudget::new(1 << 26));
    let statement = PreparedSql::prepare("SELECT $1 AS value", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    for (ty, value, expected) in [
        (LogicalType::Scalar(ScalarType::U64).maybe(), Value::Null, LogicalType::Scalar(ScalarType::I64).maybe()),
        (LogicalType::list(LogicalType::Scalar(ScalarType::U8)), Value::List(vec![]), LogicalType::list(LogicalType::Scalar(ScalarType::I8))),
    ] {
        assert_eq!(statement.result_schema(&context, std::slice::from_ref(&ty)).unwrap().schema().fields()[0].data_type, ty);
        let mut cursor = statement.open_query_typed(Arc::clone(&context), vec![value.clone()], &[ty]).await.unwrap();
        assert_eq!(cursor.runtime_schema().fields()[0].data_type, expected);
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(rtdb_columnar::from_field(page.batch().schema().field(0)).unwrap(), expected);
        assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), value);
    }
}

#[tokio::test]
async fn unsigned_stored_aggregate_outputs_open_signed_and_keep_declared_setter_failures() {
    let memory = MemoryBudget::new(1 << 27);
    let state = PartitionState::empty(rtdb_trie::TrieConfig::new(32, 32).unwrap()).unwrap();
    write(&state, 1, Value::U64(u64::MAX), &memory);
    let context = context(Some(&state), &memory);
    let statement = PreparedSql::prepare("SELECT SUM(v) AS total,AVG(v) AS average,PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v) AS median FROM docs", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = statement.open_query(Arc::clone(&context), vec![]).await.unwrap();
    assert_eq!(cursor.schema().fields()[0].data_type, LogicalType::Scalar(ScalarType::U64).maybe());
    assert_eq!(cursor.runtime_schema().fields()[0].data_type, LogicalType::Scalar(ScalarType::I64).maybe());
    let page = cursor.next_batch().await.unwrap().unwrap();
    let row = page.batch().schema().fields().iter().zip(page.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), 0).unwrap()).collect::<Vec<_>>();
    assert_eq!(row, [Value::I64(-1), Value::F64(-1.0), Value::I64(-1)]);
    let statement = PreparedSql::prepare("SELECT MIN(v) FROM docs", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = statement.open_query(context, vec![]).await.unwrap();
    assert_eq!(cursor.next_batch().await.err().unwrap().code, "rtdb.query/min-max-setter");
}

#[tokio::test]
async fn shared_prepared_query_rebinds_clock_and_snapshot_settings_for_every_execution() {
    let memory = MemoryBudget::new(1 << 27);
    let state = PartitionState::empty(rtdb_trie::TrieConfig::new(32, 32).unwrap()).unwrap();
    write(&state, 100, Value::Utf8("old".into()), &memory);
    write(&state, 200, Value::Utf8("new".into()), &memory);
    let context = context(Some(&state), &memory);
    let statement = PreparedSql::prepare("SETTING SNAPSHOT_TOKEN=?, CLOCK_TIME=? SELECT v,CURRENT_TIMESTAMP,SNAPSHOT_TOKEN FROM docs", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    assert_eq!(statement.parameter_count(), 2);
    for (time, value) in [(150, "old"), (200, "new")] {
        let basis = rtdb_types::TimeBasis { databases: BTreeMap::from([("xtdb".into(), vec![Some(Timestamp(time).into())])]) };
        let token = rtdb_format::basis::encode_time_basis(&basis);
        let clock = Value::Timestamp { value: time + 1000, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
        let mut cursor = statement.open_query(Arc::clone(&context), vec![Value::Utf8(token.clone()), clock.clone()]).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        let row = page.batch().schema().fields().iter().zip(page.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), 0).unwrap()).collect::<Vec<_>>();
        assert_eq!(row, [Value::Utf8(value.into()), clock, Value::Utf8(token)]);
    }
    assert_eq!(context.current_time, Timestamp(100).into());
}

#[test]
fn prepared_settings_validate_typed_literals_even_when_snapshot_time_is_not_applied() {
    let context = context(None, &MemoryBudget::new(1 << 24));
    let error = PreparedSql::prepare("SETTING SNAPSHOT_TIME=TIMESTAMP 'bad' SELECT 1", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).err().unwrap();
    assert_eq!(error.code, "xtdb/sql-error");
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
        let mut cursor = statement.open_query(Arc::clone(&context), vec![value.clone()]).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), value);
    }
}

#[tokio::test]
async fn inline_xtql_prepared_query_rebinds_external_parameters_through_sql() {
    let context = context(None,&MemoryBudget::new(1<<26));
    let statement = PreparedSql::prepare("SELECT ? AS prefix,t.* FROM (XTQL $$#(rel [{:value %}] [value])$$) t",&context,PrepareOptions::default(),ResultSignaturePolicy::Dynamic).unwrap();
    assert_eq!(statement.parameter_count(),2);
    for value in [Value::I32(7),Value::Utf8("next".into())] {
        let mut cursor = statement.open_query(Arc::clone(&context),vec![Value::I64(9),value.clone()]).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(read_value(page.batch().schema().field(0),page.batch().column(0).as_ref(),0).unwrap(),Value::I64(9));
        assert_eq!(read_value(page.batch().schema().field(1),page.batch().column(1).as_ref(),0).unwrap(),value);
    }
}

#[tokio::test]
async fn prepared_unqualified_decimal_rebinds_scale_and_preserves_annotated_decimal_parameters() {
    let context = context(None, &MemoryBudget::new(1 << 26));
    let statement = PreparedSql::prepare("SELECT $1::DECIMAL AS value", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    for scale in [2, 4, -1000, 1000, i32::MIN, i32::MAX] {
        let value = Value::Decimal(rtdb_types::DecimalValue::new(125i128.to_be_bytes().to_vec(), 10, scale, 128).unwrap());
        let mut cursor = statement.open_query(Arc::clone(&context), vec![value.clone()]).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(rtdb_columnar::from_field(page.batch().schema().field(0)).unwrap(), value.logical_type());
        assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), value);
    }
    let data_type = LogicalType::Scalar(ScalarType::Decimal { precision: 10, scale: 4, bit_width: 128 });
    let options = PrepareOptions { parameter_types: BTreeMap::from([(0, data_type.clone())]), ..Default::default() };
    let annotated = PreparedSql::prepare("SELECT $1::DECIMAL AS value", &context, options, ResultSignaturePolicy::Exact).unwrap();
    assert_eq!(annotated.prepared_schema().unwrap().fields()[0].data_type, data_type);
    let mut cursor = annotated.open_query(context, vec![Value::Utf8("1.25".into())]).await.unwrap();
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), Value::Decimal(rtdb_types::DecimalValue::new(12500i128.to_be_bytes().to_vec(), 10, 4, 128).unwrap()));
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
    let error = match statement.open_query(updated, vec![]).await { Ok(_) => panic!("accepted changed result type"), Err(error) => error };
    assert_eq!(error.code, "xtdb/prepared-query-out-of-date");
    let mut pinned = statement.open_query(initial, vec![]).await.unwrap();
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
    assert!(matches!(statement.dml_operation(None).unwrap(), TxOp::Sql { args: None, .. }));
    let extra = SqlArguments::from_rows(vec![vec![Value::I64(1), Value::I64(2)]], &memory).unwrap();
    let TxOp::Sql { args: Some(extra), .. } = statement.dml_operation(Some(extra)).unwrap() else { panic!("extra argument was lost") };
    assert_eq!(extra.types().len(), 2);
}

#[tokio::test]
async fn prepared_warnings_retain_occurrences_without_growing_during_rebinding() {
    let memory = MemoryBudget::new(1 << 26);
    let context = context(None, &memory);
    let source = "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v NULLS FIRST) AS first, percentile_cont(0.5) WITHIN GROUP (ORDER BY v NULLS FIRST) AS second FROM (VALUES (1), (3)) AS t(v)";
    let statement = PreparedSql::prepare(source, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    assert_eq!(statement.notices().len(), 2);
    assert!(statement.notices().iter().all(|notice| notice.message == "NULL ordering is ignored for percentile_cont (nulls are excluded from ordered-set aggregates)"));
    for _ in 0..2 {
        let mut cursor = statement.open_query(Arc::clone(&context), vec![]).await.unwrap();
        assert!(cursor.next_batch().await.unwrap().is_some());
        assert_eq!(statement.notices().len(), 2);
    }
    drop(statement);
    drop(context);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn postgres_dml_warning_planning_is_best_effort_and_skips_supplied_parameter_oids() {
    let memory = MemoryBudget::new(1 << 26);
    let context = context(None, &memory);
    let source = "INSERT INTO docs (_id,v) SELECT 1, percentile_disc($1) WITHIN GROUP (ORDER BY v NULLS LAST) FROM (VALUES (1), (3)) AS t(v)";
    let interactive = PreparedSql::prepare_postgres(source, &context, vec!["public".into()], &[]).unwrap();
    assert!(interactive.is_dml());
    assert_eq!(interactive.notices().len(), 1);
    let typed = PreparedSql::prepare_postgres(source, &context, vec!["public".into()], &[701]).unwrap();
    assert!(typed.notices().is_empty());
    let failing = PreparedSql::prepare_postgres("INSERT INTO docs (_id) SELECT unavailable_scalar(1)", &context, vec!["public".into()], &[]).unwrap();
    assert!(failing.is_dml());
    assert!(failing.notices().is_empty());
}

#[test]
fn postgres_preparation_ignores_excess_query_oids_and_defers_dml_argument_width() {
    let memory = MemoryBudget::new(1 << 26);
    let context = context(None, &memory);
    let query = PreparedSql::prepare_postgres("SELECT $1 AS value", &context, vec!["public".into()], &[23, 999_999]).unwrap();
    assert_eq!(query.parameter_count(), 1);
    assert_eq!(query.postgres_types().unwrap(), &[PgType::Int4]);
    let dml = PreparedSql::prepare_postgres("DELETE FROM absent", &context, vec!["public".into()], &[23, 999_999]).unwrap();
    let args = SqlArguments::from_rows(vec![vec![Value::I32(1), Value::Null]], &memory).unwrap();
    let TxOp::Sql { args: Some(args), .. } = dml.dml_operation(Some(args)).unwrap() else { panic!("expected SQL arguments") };
    assert_eq!(args.types().len(), 2);
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
    let mut cursor = statement.open_query(context, vec![Value::List(vec![Value::I64(1)])]).await.unwrap();
    assert_eq!(cursor.next_batch().await.unwrap().unwrap().batch().num_rows(), 1);
}

#[tokio::test]
async fn prepared_series_uses_the_nodes_captured_endpoint_policy() {
    for end_exclusive in [false,true] {
        let memory = MemoryBudget::new(1 << 26);
        let base = context(None,&memory);
        let options = QueryOptions { generate_series_end_exclusive: end_exclusive, ..Default::default() };
        let context = QueryContext::new(base.catalog.clone(),Arc::clone(&memory),Timestamp(100),"UTC",options,&CancellationToken::new()).unwrap();
        let prepare = PrepareOptions { generate_series_end_exclusive: !end_exclusive, ..Default::default() };
        let statement = PreparedSql::prepare("SELECT GENERATE_SERIES(1,3) AS value",&context,prepare,ResultSignaturePolicy::Dynamic).unwrap();
        let mut cursor = statement.open_query(context,vec![]).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        let value = read_value(page.batch().schema().field(0),page.batch().column(0).as_ref(),0).unwrap();
        assert_eq!(value,Value::List((1..if end_exclusive {3} else {4}).map(Value::I64).collect()));
    }
}
