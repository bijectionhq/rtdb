use rtdb_types::StructFields;
use std::collections::BTreeMap;
use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::{PartitionState, TransactionBuffer};
use rtdb_node::{PreparedSql, ResultSignaturePolicy};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions};
use rtdb_sql::PrepareOptions;
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_tx::{SqlArguments, TxOp};
use rtdb_types::{Anomaly, Interval, LogicalType, PgType, ScalarType, TableRef, Timestamp, TransactionKey, TxId, Value};
use tokio_util::sync::CancellationToken;

fn context(state: Option<&PartitionState>, memory: &Arc<MemoryBudget>) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: state.map(|state| vec![state.value().snapshot()]).unwrap_or_default(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, Arc::clone(memory), Timestamp(100), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap()
}

#[tokio::test]
async fn prepared_parameter_projection_keeps_the_bound_arrow_buffers() {
    use arrow_array::{Int64Array, RecordBatch, StringArray, cast::AsArray, types::Int64Type};
    use arrow_schema::{DataType, Field, Schema};
    use rtdb_columnar::OwnedBatch;

    let memory = MemoryBudget::new(1 << 24);
    let context = context(None, &memory);
    let schema = Arc::new(Schema::new(vec![Field::new("client-int", DataType::Int64, false), Field::new("client-text", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42])), Arc::new(StringArray::from(vec!["bound"]))]).unwrap();
    let bound = OwnedBatch::new(batch, &memory).unwrap();
    let types = [LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::Utf8)];
    let pointer = bound.batch().column(0).as_primitive::<Int64Type>().values().as_ptr();
    let query = PreparedSql::prepare("SELECT $2 AS label, $1 AS n", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let advertised = query.result_schema(&context, &types).unwrap();

    let empty = PreparedSql::prepare("SELECT FROM (VALUES ($1, $2)) AS t(a, b)", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut empty = empty.try_open_parameter_projection(Arc::clone(&context), &bound, &types).await.unwrap().unwrap();
    let empty_row = empty.next_batch().await.unwrap().unwrap();
    assert_eq!((empty_row.batch().num_columns(), empty_row.batch().num_rows()), (0, 1));

    let mut cursor = query.try_open_parameter_projection(context, &bound, &types).await.unwrap().unwrap();
    drop(bound);
    let output = cursor.next_batch().await.unwrap().unwrap();

    assert_eq!(cursor.schema(), advertised.schema());
    assert_eq!(output.batch().column(0).as_string::<i32>().value(0), "bound");
    assert_eq!(output.batch().column(1).as_primitive::<Int64Type>().value(0), 42);
    assert_eq!(output.batch().column(1).as_primitive::<Int64Type>().values().as_ptr(), pointer);
    assert!(cursor.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn prepared_query_reads_an_arrow_parameter_only_when_reached() {
    use arrow_array::{Int64Array, RecordBatch, StringArray, cast::AsArray, types::Int64Type};
    use arrow_schema::{DataType, Field, Schema};
    use rtdb_columnar::OwnedBatch;

    let memory = MemoryBudget::new(1 << 20);
    let source = MemoryBudget::new(1 << 24);
    let context = context(None, &memory);
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false), Field::new("text", DataType::Utf8, false)]));
    let text = "x".repeat(1 << 21);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![3])), Arc::new(StringArray::from(vec![text]))]).unwrap();
    let bound = OwnedBatch::new(batch, &source).unwrap();
    let types = [LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::Utf8)];
    let query = PreparedSql::prepare(
        "SELECT CASE WHEN v < 0 THEN CAST(CHAR_LENGTH($2) AS BIGINT) ELSE v + $1 END AS n FROM (VALUES (10), (20)) AS t(v)",
        &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic,
    ).unwrap();
    let mut cursor = query.try_open_query_arrow(context, &bound, &types).await.unwrap().unwrap();
    drop(bound);
    assert!(source.used_bytes() > 0);
    let output = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(output.batch().column(0).as_primitive::<Int64Type>().values().as_ref(), &[13, 23]);
    drop(cursor);
    assert_eq!(source.used_bytes(), 0);
}

#[tokio::test]
async fn vector_query_does_not_decode_an_unreached_case_arm() {
    use arrow_array::{RecordBatch, StringArray, cast::AsArray};
    use arrow_schema::{DataType, Field, Schema};
    use rtdb_columnar::OwnedBatch;

    let memory = MemoryBudget::new(1 << 24);
    let source = MemoryBudget::new(1 << 24);
    let state = PartitionState::empty(Default::default()).unwrap();
    write(&state, 1, Value::I64(10), &memory);
    let context = context(Some(&state), &memory);
    let schema = Arc::new(Schema::new(vec![Field::new("left", DataType::Utf8, false), Field::new("right", DataType::Utf8, false)]));
    let text = "x".repeat(1 << 21);
    let array = |values: Vec<&str>| Arc::new(StringArray::from(values));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array(vec!["small"]), array(vec![text.as_str()])]).unwrap();
    let bound = OwnedBatch::new(batch, &source).unwrap();
    let types = [LogicalType::Scalar(ScalarType::Utf8), LogicalType::Scalar(ScalarType::Utf8)];
    let query = PreparedSql::prepare(
        "SELECT CASE WHEN v < 0 THEN $2 ELSE $1 END AS label FROM docs",
        &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic,
    ).unwrap();
    let mut cursor = query.try_open_query_arrow(Arc::clone(&context), &bound, &types).await.unwrap().unwrap();
    let before = memory.used_bytes();
    let output = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(output.batch().column(0).as_string::<i32>().value(0), "small");
    assert!(memory.used_bytes().saturating_sub(before) < 1 << 20);
    drop((output, cursor, bound));
    assert_eq!(source.used_bytes(), 0);

    let batch = RecordBatch::try_new(schema, vec![array(vec!["first", text.as_str()]), array(vec!["other", text.as_str()])]).unwrap();
    let bound = OwnedBatch::new(batch, &source).unwrap();
    let mut cursor = query.try_open_query_arrow(context, &bound, &types).await.unwrap().unwrap();
    drop(bound);
    assert!(source.used_bytes() < 1 << 16);
    let output = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(output.batch().column(0).as_string::<i32>().value(0), "first");
}

#[tokio::test]
async fn prepared_parameter_projection_canonicalizes_masked_and_boolean_buffers() {
    use arrow_array::{Array, BooleanArray, Int64Array, NullArray, RecordBatch, cast::AsArray, types::Int64Type};
    use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field, Schema};
    use rtdb_columnar::OwnedBatch;

    let memory = MemoryBudget::new(1 << 24);
    let context = context(None, &memory);
    let schema = Arc::new(Schema::new(vec![
        Field::new("client-int", DataType::Int64, true),
        Field::new("client-bool", DataType::Boolean, false),
        Field::new("client-null", DataType::Null, true),
    ]));
    let masked = Int64Array::new(ScalarBuffer::from(vec![99]), Some(NullBuffer::new_null(1)));
    let padded = BooleanArray::new(BooleanBuffer::new(Buffer::from(vec![0xff]), 0, 1), None);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(masked), Arc::new(padded), Arc::new(NullArray::new(1))]).unwrap();
    let bound = OwnedBatch::new(batch, &memory).unwrap();
    let types = [LogicalType::Scalar(ScalarType::I64).maybe(), LogicalType::Scalar(ScalarType::Bool), LogicalType::Null];
    let query = PreparedSql::prepare("SELECT $2 AS b, $1 AS n, $3 AS empty", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut projected = query.try_open_parameter_projection(Arc::clone(&context), &bound, &types).await.unwrap().unwrap();
    drop(bound);
    let mut ordinary = query.open_query_columns_typed(context, vec![Value::Null, Value::Bool(true), Value::Null], &types).await.unwrap();
    let output = projected.next_batch().await.unwrap().unwrap();
    let expected = ordinary.next_batch().await.unwrap().unwrap();

    assert_eq!(projected.schema(), ordinary.schema());
    assert_eq!(output.batch(), expected.batch());
    assert_eq!(output.batch().column(0).as_boolean().values().values(), &[1]);
    assert!(output.batch().column(1).is_null(0));
    assert_eq!(output.batch().column(1).as_primitive::<Int64Type>().value(0), 0);
    assert_eq!(output.batch().column(2).data_type(), &DataType::Null);
}

#[tokio::test]
async fn prepared_parameter_projection_selects_the_first_row_and_releases_the_batch() {
    use arrow_array::{RecordBatch, StringArray, cast::AsArray};
    use arrow_schema::{DataType, Field, Schema};
    use rtdb_columnar::OwnedBatch;

    let memory = MemoryBudget::new(1 << 24);
    let context = context(None, &memory);
    let schema = Arc::new(Schema::new(vec![Field::new("input", DataType::Utf8, false)]));
    let unused = "x".repeat(1 << 20);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["first", unused.as_str()]))]).unwrap();
    let bound = OwnedBatch::new(batch, &memory).unwrap();
    let upload = Arc::downgrade(bound.reservation());
    let types = [LogicalType::Scalar(ScalarType::Utf8)];
    let query = PreparedSql::prepare("SELECT $1 AS label", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = query.try_open_parameter_projection(context, &bound, &types).await.unwrap().unwrap();
    drop(bound);

    assert!(upload.upgrade().is_none());
    let output = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(output.batch().column(0).as_string::<i32>().iter().collect::<Vec<_>>(), [Some("first")]);
    assert!(cursor.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn computed_parameter_projection_uses_arrow_arithmetic_and_preserves_nested_payloads() {
    use arrow_array::{Array, Int64Array, RecordBatch, cast::AsArray, types::Int64Type};
    use arrow_schema::{DataType, Field, Schema};
    use rtdb_columnar::{OwnedBatch, copy_array, to_field};

    let memory = MemoryBudget::new(1 << 24);
    let context = context(None, &memory);
    let long = LogicalType::Scalar(ScalarType::I64);
    let payload = Value::list(vec![Value::Struct([("label".into(), Value::Utf8("kept".into()))].into()), Value::I64(7)]);
    let payload_type = payload.logical_type();
    let types = [long.clone(), long.clone(), payload_type.clone()];
    let fields = vec![Field::new("a", DataType::Int64, false), Field::new("b", DataType::Int64, false), to_field("payload", &payload_type).unwrap()];
    let bound = |a| {
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields.clone())), vec![
            Arc::new(Int64Array::from(vec![a])),
            Arc::new(Int64Array::from(vec![3])),
            copy_array(&payload_type, std::slice::from_ref(&payload)).unwrap(),
        ]).unwrap();
        OwnedBatch::new(batch, &memory).unwrap()
    };
    let query = PreparedSql::prepare("SELECT ($1 + $2) * $2 AS total, $3 AS payload", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut projected = query.try_open_parameter_projection(Arc::clone(&context), &bound(11), &types).await.unwrap().unwrap();
    let mut ordinary = query.open_query_columns_typed(Arc::clone(&context), vec![Value::I64(11), Value::I64(3), payload.clone()], &types).await.unwrap();
    let actual = projected.next_batch().await.unwrap().unwrap();
    let expected = ordinary.next_batch().await.unwrap().unwrap();

    assert_eq!(projected.schema(), ordinary.schema());
    assert_eq!(actual.batch(), expected.batch());
    assert_eq!(actual.batch().column(0).as_primitive::<Int64Type>().value(0), 42);
    assert!(actual.batch().column(1).as_list::<i32>().is_valid(0));
    assert!(query.try_open_parameter_projection(context, &bound(i64::MAX), &types).await.unwrap().is_none());
}

#[tokio::test]
async fn prepared_vector_projection_reads_arrow_operands_for_division_comparison_and_text() {
    use arrow_array::{Int64Array, RecordBatch, StringArray, cast::AsArray, types::Int64Type};
    use arrow_schema::{DataType, Field, Schema};
    use rtdb_columnar::OwnedBatch;

    let memory = MemoryBudget::new(1 << 24);
    let context = context(None, &memory);
    let schema = Arc::new(Schema::new(vec![
        Field::new("left", DataType::Int64, false),
        Field::new("right", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
    ]));
    let bound = |divisor| {
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
            Arc::new(Int64Array::from(vec![84])),
            Arc::new(Int64Array::from(vec![divisor])),
            Arc::new(StringArray::from(vec!["Straße"])),
        ]).unwrap();
        OwnedBatch::new(batch, &memory).unwrap()
    };
    let numeric = LogicalType::Scalar(ScalarType::I64);
    let types = [numeric.clone(), numeric, LogicalType::Scalar(ScalarType::Utf8)];
    let query = PreparedSql::prepare("SELECT $1 / $2 AS quotient, $1 > $2 AS larger, UPPER($3) AS label",
        &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = query.try_open_parameter_projection(Arc::clone(&context), &bound(2), &types).await.unwrap().unwrap();
    let output = cursor.next_batch().await.unwrap().unwrap();

    assert_eq!(output.batch().column(0).as_primitive::<Int64Type>().value(0), 42);
    assert!(output.batch().column(1).as_boolean().value(0));
    assert_eq!(output.batch().column(2).as_string::<i32>().value(0), "STRASSE");
    assert!(query.try_open_parameter_projection(context, &bound(0), &types).await.unwrap().is_none());
}

#[tokio::test]
async fn prepared_vector_projection_admits_the_masked_utf8_span_before_evaluation() {
    use arrow_array::{RecordBatch, StringArray};
    use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field, Schema};
    use rtdb_columnar::OwnedBatch;

    let memory = MemoryBudget::new(1 << 20);
    let context = context(None, &memory);
    let schema = Arc::new(Schema::new(vec![Field::new("input", DataType::Utf8, true)]));
    let bound = |masked_bytes| {
        let strings = StringArray::try_new(
            OffsetBuffer::new(ScalarBuffer::from(vec![0, masked_bytes])),
            Buffer::from_vec(vec![b'x'; masked_bytes as usize]),
            Some(NullBuffer::new_null(1)),
        ).unwrap();
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(strings)]).unwrap();
        OwnedBatch::new(batch, &memory).unwrap()
    };
    let types = [LogicalType::Scalar(ScalarType::Utf8).maybe()];
    let query = PreparedSql::prepare("SELECT CHAR_LENGTH(UPPER($1)) AS size", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();

    let mut small = query.try_open_parameter_projection(Arc::clone(&context), &bound(1), &types).await.unwrap().unwrap();
    let page = small.next_batch().await.unwrap().unwrap();
    assert_eq!(page.batch().column(0).null_count(), 1);
    drop((small, page));
    assert!(query.try_open_parameter_projection(Arc::clone(&context), &bound(1 << 19), &types).await.unwrap().is_none());
    let mut cursor = query.open_query_columns_typed(context, vec![Value::Null], &types).await.unwrap();
    let output = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(read_value(output.batch().schema_ref().field(0), output.batch().column(0).as_ref(), 0).unwrap(), Value::Null);
}

#[tokio::test]
async fn prepared_parameter_projection_copies_only_the_first_fixed_size_list() {
    use arrow_array::{RecordBatch, cast::AsArray};
    use arrow_schema::Schema;
    use rtdb_columnar::{OwnedBatch, copy_array, to_field};

    let memory = MemoryBudget::new(1 << 24);
    let context = context(None, &memory);
    let first = Value::FixedSizeList(vec![Value::I64(20), Value::I64(22)].into());
    let second = Value::FixedSizeList(vec![Value::I64(0), Value::I64(0)].into());
    let ty = first.logical_type();
    let schema = Arc::new(Schema::new(vec![to_field("input", &ty).unwrap()]));
    let batch = RecordBatch::try_new(schema, vec![copy_array(&ty, &[first.clone(), second]).unwrap()]).unwrap();
    let bound = OwnedBatch::new(batch, &memory).unwrap();
    let upload = Arc::downgrade(bound.reservation());
    let query = PreparedSql::prepare("SELECT $1 AS fixed", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = query.try_open_parameter_projection(context, &bound, &[ty]).await.unwrap().unwrap();
    drop(bound);
    assert!(upload.upgrade().is_none());

    let page = cursor.next_batch().await.unwrap().unwrap();
    let batch = page.batch();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(0).as_fixed_size_list().value_length(), 2);
    assert_eq!(read_value(batch.schema_ref().field(0), batch.column(0).as_ref(), 0).unwrap(), first);
}

#[tokio::test]
async fn postgres_preparation_keeps_inline_xtql_source_and_result_fields() {
    let memory = MemoryBudget::new(1 << 26);
    let context = context(None, &memory);
    let source = r#"XTQL $$(-> (rel [{}] []) (return {:v (regexp-replace "abc" "a" "x")}))$$"#;
    let prepared = PreparedSql::prepare_postgres(source, &context, vec!["public".into()], &[]).unwrap();
    assert_eq!(prepared.source(), source);
    assert_eq!(prepared.prepared_schema().unwrap().fields()[0].name, "v");
    let mut cursor = prepared.open_query(context, vec![]).await.unwrap();
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), Value::Utf8("xbc".into()));
    drop((page, cursor, prepared));
    assert_eq!(memory.used_bytes(), 0);
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
#[tokio::test]
async fn an_xtql_query_of_a_table_absent_when_prepared_and_executed_returns_no_rows() {
    let memory = MemoryBudget::new(1 << 26);
    let context = context(None, &memory);
    let prepared = PreparedSql::prepare("XTQL $$(from :users [first-name])$$", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = prepared.open_query(context, vec![]).await.unwrap();
    while let Some(page) = cursor.next_batch().await.unwrap() { assert_eq!(page.batch().num_rows(), 0); }
}
#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_large_sorted_series_stops_it_promptly() {
    let memory = MemoryBudget::new(1 << 30);
    let token = CancellationToken::new();
    let database = DatabaseSnapshot { partitions: Vec::new(), storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(100), "UTC", QueryOptions::default(), &token).unwrap();
    let prepared = PreparedSql::prepare("FROM GENERATE_SERIES(1,10000000) AS system(_id) ORDER BY _id", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let canceller = { let token = token.clone(); tokio::spawn(async move { tokio::time::sleep(std::time::Duration::from_millis(2000)).await; token.cancel(); }) };
    let started = std::time::Instant::now();
    let result = async { let mut cursor = prepared.open_query(context, vec![]).await?; while cursor.next_batch().await?.is_some() {} Ok::<_, Anomaly>(()) }.await;
    canceller.await.unwrap();
    assert!(result.is_err(), "the query ran to completion");
    assert!(started.elapsed() < std::time::Duration::from_secs(3), "cancellation took {:?}", started.elapsed());
}
fn write(state: &PartitionState, tx: i64, value: Value, memory: &Arc<MemoryBudget>) {
    let mut buffer = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(tx), system_time: Timestamp(tx) }, Arc::clone(memory)).unwrap();
    buffer.put(TableRef::new("public", "docs"), StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), value)]), Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
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
async fn values_rows_are_evaluated_when_the_table_opens() {
    let context = context(None, &MemoryBudget::new(1 << 26));
    for sql in [
        "VALUES (1 / 0)",
        "SELECT v FROM (VALUES (1), (1 / 0)) AS t(v) LIMIT 1",
        "SELECT v FROM (VALUES (1), (1 / 0)) AS t(v) LIMIT 0",
        "SELECT v FROM (VALUES (1 / 0)) AS t(v) WHERE FALSE",
        "SELECT 7 FROM (VALUES (1 / 0)) AS t(v)",
        "EXPLAIN ANALYZE SELECT v FROM (VALUES (1 / 0)) AS t(v)",
    ] {
        let statement = PreparedSql::prepare(sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
        assert!(statement.result_schema(&context, &[]).is_ok(), "{sql}");
        let error = statement.open_query(Arc::clone(&context), vec![]).await.err().unwrap();
        assert_eq!(error.code, "xtdb.expression/division-by-zero", "{sql}");
    }
    assert_eq!(context.memory.used_bytes(), 0);
    let projection = PreparedSql::prepare("SELECT 1 / 0 FROM (VALUES (1)) AS t(v) LIMIT 0", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = projection.open_query(context, vec![]).await.unwrap();
    assert!(cursor.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn values_cte_opening_depends_on_inline_or_materialized_lifetime() {
    let context = context(None, &MemoryBudget::new(1 << 26));
    for sql in [
        "WITH unused AS (VALUES (1 / 0)) SELECT 7 AS value",
        "WITH MATERIALIZED unused AS (SELECT 1 / 0 AS n) SELECT 7 AS value",
    ] {
        let unused = PreparedSql::prepare(sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
        let mut cursor = unused.open_query(Arc::clone(&context), vec![]).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), Value::I64(7), "{sql}");
    }
    for sql in [
        "WITH used AS (SELECT v FROM (VALUES (1 / 0)) AS t(v)) SELECT v FROM used LIMIT 0",
        "WITH used AS (SELECT v FROM (VALUES (1 / 0)) AS t(v)) SELECT 7 AS value FROM used",
        "WITH MATERIALIZED unused AS (VALUES (1 / 0)) SELECT 7 AS value",
        "WITH MATERIALIZED used AS (SELECT v FROM (VALUES (1 / 0)) AS t(v)) SELECT v FROM used LIMIT 0",
    ] {
        let used = PreparedSql::prepare(sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
        let error = used.open_query(Arc::clone(&context), vec![]).await.err().unwrap_or_else(|| panic!("opened {sql}"));
        assert_eq!(error.code, "xtdb.expression/division-by-zero", "{sql}");
    }
    assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn sorted_and_unsorted_limits_open_their_inputs_before_resolving_bounds() {
    let context = context(None, &MemoryBudget::new(1 << 26));
    for (sql, code) in [
        ("VALUES (1 / 0) LIMIT $1", "xtdb.expression/division-by-zero"),
        ("SELECT v FROM (VALUES (1 / 0)) t(v) ORDER BY v LIMIT $1", "xtdb.expression/division-by-zero"),
        ("SELECT 1 / 0 LIMIT $1", "xtdb/expected-number"),
        ("SELECT v FROM (VALUES (1)) t(v) ORDER BY v LIMIT $1", "xtdb/expected-number"),
    ] {
        let before = context.memory.used_bytes();
        let statement = PreparedSql::prepare(sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
        let error = statement.open_query(Arc::clone(&context), vec![Value::Utf8("1".into())]).await.err().unwrap();
        assert_eq!(error.code, code, "{sql}");
        drop(statement);
        assert_eq!(context.memory.used_bytes(), before, "{sql}: a failed open leaves nothing beyond the statement, which keeps its plan");
    }
    assert_eq!(context.memory.used_bytes(), 0);
}

#[tokio::test]
async fn lateral_values_open_with_the_dependent_arguments_during_pull() {
    let context = context(None, &MemoryBudget::new(1 << 26));
    let sql = "SELECT t.v, w.b FROM (VALUES (1), (0)) AS t(v), LATERAL (VALUES (1/t.v)) AS w(b) LIMIT 1";
    let statement = PreparedSql::prepare(sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = statement.open_query(context, vec![]).await.unwrap();
    assert_eq!(cursor.next_batch().await.err().unwrap().code, "xtdb.expression/division-by-zero");
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
        (LogicalType::list(LogicalType::Scalar(ScalarType::U8)), Value::list(vec![]), LogicalType::list(LogicalType::Scalar(ScalarType::I8))),
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
async fn prepared_query_refuses_a_snapshot_token_without_its_owner() {
    let memory = MemoryBudget::new(1 << 27);
    let context = context(None, &memory);
    let statement = PreparedSql::prepare("SETTING SNAPSHOT_TOKEN=? SELECT CURRENT_TIMESTAMP", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    assert_eq!(statement.parameter_count(), 1);
    let failure = statement.open_query(Arc::clone(&context), vec![Value::Utf8("unavailable".into())]).await.err().unwrap();
    assert_eq!(failure.code, "rtdb.query/snapshot-unretained");
}

#[test]
fn prepared_settings_validate_typed_snapshot_time_literals() {
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
    let mut cursor = statement.open_query(context, vec![Value::list(vec![Value::I64(1)])]).await.unwrap();
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
        assert_eq!(value,Value::list((1..if end_exclusive {3} else {4}).map(Value::I64).collect()));
    }
}

#[test]
fn programmatic_preparation_reports_the_direct_grammar_listener_text() {
    let memory = MemoryBudget::new(64 << 20);
    let context = context(None, &memory);
    for (source, report) in [
        ("SELECT 1 LIMIT ALL", "line 1:15 mismatched input 'ALL' expecting {UNSIGNED_INTEGER, POSTGRES_PARAMETER_SPECIFICATION, '?'}"),
        ("SELECT 1; SELEC 1", "line 1:10 mismatched input 'SELEC' expecting <EOF>"),
        ("SELECT 1 +", "line 1:9 extraneous input '+' expecting {<EOF>, ';'}"),
    ] {
        let error = PreparedSql::prepare(source, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).err().unwrap();
        assert_eq!(error, Anomaly::incorrect("xtdb/sql-error", format!("Errors parsing SQL statement:\n  - {report}")), "{source}");
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn programmatic_preparation_admits_syntax_after_source_and_cancellation() {
    let source = format!("SELECT '{}'; SELECT @", "é".repeat(64));
    let source_bytes = source.len() + 256;
    let syntax_bytes = rtdb_sql::statement_syntax_estimate(source.len()).unwrap();
    let prepare = |context: &Arc<QueryContext>| PreparedSql::prepare(&source, context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic);
    let short = MemoryBudget::new(source_bytes + syntax_bytes - 1);
    let short_context = context(None, &short);
    assert_eq!(prepare(&short_context).err().unwrap().code, "rtdb/memory-budget-exceeded");
    assert_eq!(short.peak_bytes(), source_bytes);
    assert_eq!(short.used_bytes(), 0);

    short_context.cancellation.cancel();
    assert_eq!(prepare(&short_context).err().unwrap().code, "xtdb/query-cancelled");
    assert_eq!(short.used_bytes(), 0);

    let exact = MemoryBudget::new(source_bytes + syntax_bytes);
    let expected = rtdb_sql::parse_in(&source, &exact).unwrap_err();
    assert_eq!(expected.code, "xtdb/sql-error");
    assert_eq!(prepare(&context(None, &exact)).err().unwrap().code, "rtdb/memory-budget-exceeded");
    assert_eq!(exact.peak_bytes(), source_bytes + syntax_bytes);
    assert_eq!(exact.used_bytes(), 0);

    let roomy = MemoryBudget::new(64 << 20);
    assert_eq!(prepare(&context(None, &roomy)).err().unwrap(), expected);
    assert_eq!(roomy.used_bytes(), 0);
}

#[test]
fn programmatic_preparation_keeps_the_syntax_charge_through_owner_admission() {
    let source = "DELETE FROM docs";
    let syntax_bytes = rtdb_sql::statement_syntax_estimate(source.len()).unwrap();
    let prepare = |memory: &Arc<MemoryBudget>| PreparedSql::prepare(source, &context(None, memory), PrepareOptions::default(), ResultSignaturePolicy::Dynamic);
    let probe = MemoryBudget::new(1 << 20);
    let prepared = prepare(&probe).unwrap();
    assert_eq!(prepared.source(), source);
    let retained = probe.used_bytes();
    let temporary = syntax_bytes + source.len() + 256 + 1024;
    assert_eq!(probe.peak_bytes(), temporary + retained);
    drop(prepared);
    assert_eq!(probe.used_bytes(), 0);

    let short = MemoryBudget::new(temporary + retained - 1);
    let error = prepare(&short).err().unwrap();
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(short.peak_bytes(), temporary);
    assert_eq!(short.used_bytes(), 0);
}

#[test]
fn query_rebinding_admits_syntax_in_the_reached_budget_through_plan_admission() {
    let source = "SELECT 1 AS value";
    let syntax_bytes = rtdb_sql::statement_syntax_estimate(source.len()).unwrap();
    let origin = MemoryBudget::new(1 << 20);
    let initial = context(None, &origin);
    let options = PrepareOptions::default();
    let statement = PreparedSql::prepare(source, &initial, options.clone(), ResultSignaturePolicy::Dynamic).unwrap();
    let dml = PreparedSql::prepare("DELETE FROM docs", &initial, options.clone(), ResultSignaturePolicy::Dynamic).unwrap();
    let origin_retained = origin.used_bytes();

    let short = MemoryBudget::new(syntax_bytes - 1);
    let reached = context(None, &short);
    assert_eq!(dml.result_schema(&reached, &[]).err().unwrap().code, "rtdb.node/statement-kind");
    assert_eq!(statement.result_schema(&reached, &[LogicalType::Null]).err().unwrap().code, "xtdb/parameter-count-mismatch");
    assert_eq!(statement.result_schema(&reached, &[]).err().unwrap().code, "rtdb/memory-budget-exceeded");
    reached.cancellation.cancel();
    assert_eq!(statement.result_schema(&reached, &[]).err().unwrap().code, "xtdb/query-cancelled");
    assert_eq!(short.peak_bytes(), 0);
    assert_eq!(short.used_bytes(), 0);

    let probe = MemoryBudget::new(1 << 20);
    let reached = context(None, &probe);
    let raw = rtdb_sql::prepare_in(source, &reached.catalog, &options, &probe).unwrap();
    let plan = rtdb_query::PreparedQuery::from_prepared(raw, &probe).unwrap();
    let plan_bytes = probe.used_bytes();
    let schema = plan.result_schema(&reached).unwrap();
    let schema_bytes = probe.used_bytes() - plan_bytes;
    drop((schema, plan));
    assert_eq!(probe.used_bytes(), 0);

    let plan_short = MemoryBudget::new(syntax_bytes + plan_bytes - 1);
    let error = statement.result_schema(&context(None, &plan_short), &[]).err().unwrap();
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(plan_short.peak_bytes(), syntax_bytes);
    assert_eq!(plan_short.used_bytes(), 0);

    let schema = statement.result_schema(&reached, &[]).unwrap();
    assert_eq!(schema.schema().fields()[0].name, "value");
    assert_eq!(probe.used_bytes(), schema_bytes);
    assert!(origin.used_bytes() > origin_retained, "the statement keeps the plan it bound, charged to its own budget");
    drop(schema);
    drop((statement, dml));
    assert_eq!(probe.used_bytes(), 0);
    assert_eq!(origin.used_bytes(), 0);
}

#[test]
fn programmatic_and_postgres_preparation_keep_their_syntax_and_cancellation_order() {
    let malformed = "SELECT @";
    let memory = MemoryBudget::new(malformed.len() + 255);
    let context = context(None, &memory);
    let Err(programmatic) = PreparedSql::prepare(malformed, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic)
        else { panic!("programmatic source admission"); };
    let Err(postgres) = PreparedSql::prepare_postgres(malformed, &context, Vec::new(), &[])
        else { panic!("PostgreSQL source error"); };
    assert_eq!(programmatic.code, "rtdb/memory-budget-exceeded");
    assert_eq!(postgres, rtdb_sql::parse_in(malformed, &memory).unwrap_err());
    assert_eq!(postgres.code, "xtdb/sql-error");

    context.cancellation.cancel();
    let Err(programmatic) = PreparedSql::prepare(malformed, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic)
        else { panic!("programmatic cancellation"); };
    let Err(postgres_after_cancel) = PreparedSql::prepare_postgres(malformed, &context, Vec::new(), &[])
        else { panic!("PostgreSQL source error after cancellation"); };
    assert_eq!(programmatic.code, "xtdb/query-cancelled");
    assert_eq!(postgres_after_cancel, postgres);

    let Err(query) = PreparedSql::prepare_postgres("SELECT 1", &context, Vec::new(), &[])
        else { panic!("PostgreSQL OID admission"); };
    let Err(dml) = PreparedSql::prepare_postgres("DELETE FROM docs", &context, Vec::new(), &[])
        else { panic!("PostgreSQL DML cancellation"); };
    assert_eq!(query.code, "rtdb/memory-budget-exceeded");
    assert!(query.message.as_deref().unwrap().starts_with("Cannot reserve 1024 bytes"));
    assert_eq!(dml.code, "xtdb/query-cancelled");
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(memory.peak_bytes(), 0);
}

#[test]
fn postgres_dml_reuses_the_admitted_syntax_and_retains_the_exact_source() {
    let calls = [r#"(regexp-replace "abc" "a" "x")"#; 32].join(" ");
    let source = format!("-- original source\nINSERT INTO docs (_id, v) SELECT $1, v FROM (XTQL $$(-> (rel [{{}}] []) (return {{:v (coalesce {calls})}}))$$) AS src; ");
    let parse_probe = MemoryBudget::new(1 << 20);
    let parsed = rtdb_sql::parse_in(&source, &parse_probe).unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].kind(), rtdb_sql::StatementKind::Dml);
    let parameter_count = parsed[0].parameter_count();
    assert_eq!(parameter_count, 1);
    let parse_peak = parse_probe.peak_bytes();
    let syntax_bytes = parse_probe.used_bytes();
    assert!(syntax_bytes > 0);
    drop(parsed);
    assert_eq!(parse_probe.used_bytes(), 0);

    let prepare = |memory: &Arc<MemoryBudget>| {
        PreparedSql::prepare_postgres(&source, &context(None, memory), vec!["public".into()], &[23])
    };
    let probe = MemoryBudget::new(1 << 20);
    let prepared = prepare(&probe).unwrap();
    assert!(prepared.is_dml());
    assert!(prepared.notices().is_empty());
    let retained = probe.used_bytes();
    let source_bytes = source.len() + 256;
    let parameter_bytes = (parameter_count + 1) * 1024;
    let exact_limit = parse_peak.max(syntax_bytes + source_bytes + parameter_bytes + retained);
    assert_eq!(probe.peak_bytes(), exact_limit);
    assert!(exact_limit < syntax_bytes + source_bytes + parse_peak);
    drop(prepared);
    assert_eq!(probe.used_bytes(), 0);

    let memory = MemoryBudget::new(exact_limit);
    let prepared = prepare(&memory).unwrap();
    assert_eq!(prepared.source(), source);
    assert_eq!(prepared.parameter_count(), parameter_count);
    let TxOp::Sql { sql, args: None } = prepared.dml_operation(None).unwrap() else { panic!("durable SQL operation"); };
    assert_eq!(sql, source);
    assert_eq!(memory.used_bytes(), retained);
    assert_eq!(memory.peak_bytes(), exact_limit);
    drop(prepared);
    assert_eq!(memory.used_bytes(), 0);

    let short = MemoryBudget::new(exact_limit - 1);
    let Err(error) = prepare(&short) else { panic!("prepared statement admission"); };
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(short.used_bytes(), 0);
}

#[tokio::test]
async fn postgres_prepares_the_supplied_inline_syntax_and_rebinds_its_parameters() {
    let memory = MemoryBudget::new(1 << 26);
    let context = context(None, &memory);
    let source = "-- original\nSELECT ?::INTEGER AS prefix, t.value::BIGINT AS value FROM (XTQL $$#(rel [{:value %}] [value])$$) t; ";
    let syntax = rtdb_sql::parse_syntax_in(source, &memory).unwrap();
    let prepared = PreparedSql::prepare_postgres_syntax(syntax, &context, vec!["public".into()], &[23, 20]).unwrap();
    assert_eq!(prepared.source(), source);
    assert_eq!(prepared.parameter_count(), 2);
    assert_eq!(prepared.postgres_types().unwrap(), &[PgType::Int4, PgType::Int8]);

    let mut cursor = prepared.open_query(Arc::clone(&context), vec![Value::I32(7), Value::I64(42)]).await.unwrap();
    let page = cursor.next_batch().await.unwrap().unwrap();
    let batch = page.batch();
    let values = batch.schema().fields().iter().zip(batch.columns())
        .map(|(field, array)| read_value(field, array.as_ref(), 0).unwrap()).collect::<Vec<_>>();
    assert_eq!(values, [Value::I32(7), Value::I64(42)]);
    drop(page);
    drop(cursor);
    drop(prepared);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn postgres_rejects_foreign_syntax_budget_even_for_empty_input() {
    let origin = MemoryBudget::new(1 << 20);
    let foreign = MemoryBudget::new(1 << 20);
    let context = context(None, &foreign);
    let raw = r#"XTQL $$(-> (rel [{}] []) (return {:v (regexp-replace "abc" "a" "x")}))$$"#;
    for source in ["", raw] {
        let syntax = rtdb_sql::parse_syntax_in(source, &origin).unwrap();
        let Err(error) = PreparedSql::prepare_postgres_syntax(syntax, &context, Vec::new(), &[])
            else { panic!("expected a budget identity error"); };
        assert_eq!(error.code, "rtdb.sql/syntax-budget");
        assert_eq!(origin.peak_bytes(), 0);
        assert_eq!(foreign.peak_bytes(), 0);
    }

    let syntax = rtdb_sql::parse_syntax_in(raw, &origin).unwrap();
    let same_budget = Arc::clone(&origin);
    let resolved = syntax.resolve_in(&same_budget).unwrap();
    assert!(origin.used_bytes() > 0);
    drop(resolved);
    assert_eq!(origin.used_bytes(), 0);
    assert_eq!(foreign.used_bytes(), 0);
}

#[test]
fn postgres_validates_the_complete_syntax_input_before_retaining_dml_source() {
    let memory = MemoryBudget::new(1 << 20);
    let context = context(None, &memory);
    let multiple = "INSERT INTO docs (_id) VALUES ($1); SELECT 8";
    let unresolved = "INSERT INTO docs (_id) VALUES ($1); XTQL $$[$$";
    for (source, code) in [(multiple, "xtdb/sql-error"), (unresolved, "xtql/edn-reader")] {
        let syntax = rtdb_sql::parse_syntax_in(source, &memory).unwrap();
        let Err(consuming) = PreparedSql::prepare_postgres_syntax(syntax, &context, Vec::new(), &[23])
            else { panic!("expected a complete-source error"); };
        let Err(text) = PreparedSql::prepare_postgres(source, &context, Vec::new(), &[23])
            else { panic!("expected the same source error"); };
        assert_eq!(consuming, text);
        assert_eq!(consuming.code, code);
        if source == multiple {
            assert_eq!(consuming.message.as_deref(), Some("A prepared statement must contain exactly one SQL statement"));
        }
        assert_eq!(memory.used_bytes(), 0);
    }

    let source = "-- original\nINSERT INTO docs (_id) VALUES ($1); ";
    let syntax = rtdb_sql::parse_syntax_in(source, &memory).unwrap();
    let prepared = PreparedSql::prepare_postgres_syntax(syntax, &context, Vec::new(), &[23]).unwrap();
    assert_eq!(prepared.parameter_count(), 1);
    let TxOp::Sql { sql, args: None } = prepared.dml_operation(None).unwrap() else { panic!("expected SQL operation"); };
    assert_eq!(sql, source);
    drop(prepared);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn statements_outside_the_reference_grammar_are_sql_errors_while_session_control_stays_on_the_session_api() {
    let memory = MemoryBudget::new(1 << 26);
    let context = context(None, &memory);
    for sql in ["CREATE VIEW v AS SELECT 1", "DROP TABLE foo", "CREATE UNIQUE INDEX i ON foo (a)", "TRUNCATE foo", "ALTER TABLE foo ADD COLUMN x INT"] {
        let error = PreparedSql::prepare(sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).err().unwrap();
        assert_eq!((error.code.as_str(), error.category), ("xtdb/sql-error", rtdb_types::AnomalyCategory::Incorrect), "{sql}");
        let error = PreparedSql::prepare_postgres(sql, &context, vec!["public".into()], &[]).err().unwrap();
        assert_eq!(error.code, "xtdb/sql-error", "{sql}");
    }
    for sql in ["BEGIN", "COMMIT", "SET TIME ZONE 'UTC'", "SHOW timezone"] {
        let error = PreparedSql::prepare(sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).err().unwrap();
        assert_eq!(error.code, "rtdb.node/prepared-session-statement", "{sql}");
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn records_query_unions_record_keys_and_leaves_missing_keys_null() {
    let context = context(None, &MemoryBudget::new(1 << 26));
    for sql in [
        "RECORDS {_id: 1, x: 1}, {x: 2.0}",
        "SELECT * FROM (RECORDS {_id: 1, x: 1}, {x: 2.0}) AS r",
        "RECORDS {_id: 1, x: 1} UNION ALL RECORDS {_id: NULL, x: 2.0}",
    ] {
        let statement = PreparedSql::prepare(sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
        let names: Vec<_> = statement.prepared_schema().unwrap().fields().iter().map(|field| field.name.clone()).collect();
        assert_eq!(names, ["_id", "x"], "{sql}");
        let mut cursor = statement.open_query(Arc::clone(&context), vec![]).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        let batch = page.batch();
        let rows: Vec<Vec<Value>> = (0..batch.num_rows()).map(|row| (0..batch.num_columns())
            .map(|column| read_value(batch.schema().field(column), batch.column(column).as_ref(), row).unwrap()).collect()).collect();
        assert_eq!(rows, [vec![Value::I64(1), Value::I64(1)], vec![Value::Null, Value::F64(2.0)]], "{sql}");
    }
    let error = PreparedSql::prepare("RECORDS ?", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).err().unwrap();
    assert_eq!(error, Anomaly::incorrect("xtdb.sql/records-param-outside-DML", "RECORDS ? not supported outside of DML"));
}

#[tokio::test]
async fn a_top_k_over_unorderable_keys_rejects_them_only_when_it_has_rows() {
    let context = context(None, &MemoryBudget::new(1 << 26));
    let empty = PreparedSql::prepare("SELECT v FROM (VALUES ({a: 1})) t(v) WHERE FALSE ORDER BY v LIMIT 10", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = empty.open_query(Arc::clone(&context), vec![]).await.unwrap();
    let mut rows = 0;
    while let Some(batch) = cursor.next_batch().await.unwrap() { rows += batch.batch().num_rows(); }
    assert_eq!(rows, 0);
    drop(cursor);
    let rows = PreparedSql::prepare("SELECT v FROM (VALUES ({a: 1}), ({a: 2})) t(v) ORDER BY v LIMIT 10", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let error = match rows.open_query(Arc::clone(&context), vec![]).await {
        Err(error) => error,
        Ok(mut cursor) => loop { match cursor.next_batch().await { Err(error) => break error, Ok(Some(_)) => continue, Ok(None) => panic!("sorted unorderable keys") } },
    };
    assert_eq!((error.code.as_str(), error.message.as_deref()), ("xtdb.expression/function-type-mismatch", Some("compare is not applicable to these vector types")));
}

#[tokio::test]
async fn a_selection_pushed_beneath_another_drops_its_rows_before_the_other_reads_them() {
    let context = context(None, &MemoryBudget::new(1 << 26));
    let sql = "SELECT s.c FROM (SELECT t.c, t.a FROM (VALUES (0, CAST(NULL AS BIGINT))) t(c, a) WHERE 1 / t.c > 0) s WHERE s.a > 1";
    let statement = PreparedSql::prepare(sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = statement.open_query(Arc::clone(&context), vec![]).await.unwrap();
    let mut rows = 0;
    while let Some(batch) = cursor.next_batch().await.unwrap() { rows += batch.batch().num_rows(); }
    assert_eq!(rows, 0);
}

#[test]
fn a_thousand_unpushable_conjuncts_plan_and_run_on_a_worker_stack() {
    std::thread::Builder::new().stack_size(rtdb_node::WORKER_STACK_BYTES).spawn(move || {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let context = context(None, &MemoryBudget::new(1 << 28));
            let conjuncts = (0..1000).map(|value| format!("t.v <> {value}")).collect::<Vec<_>>().join(" AND ");
            let sql = format!("SELECT t.v FROM (VALUES (5000), (7)) t(v) WHERE {conjuncts}");
            let statement = PreparedSql::prepare(&sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
            let mut cursor = statement.open_query(Arc::clone(&context), vec![]).await.unwrap();
            let mut rows = 0;
            while let Some(batch) = cursor.next_batch().await.unwrap() { rows += batch.batch().num_rows(); }
            assert_eq!(rows, 1);
        })
    }).unwrap().join().unwrap();
}

#[tokio::test]
async fn a_list_holding_a_null_among_mixed_types_reads_back_through_every_valid_time() {
    let memory = MemoryBudget::new(1 << 26);
    let state = PartitionState::empty(rtdb_trie::TrieConfig::new(32, 32).unwrap()).unwrap();
    write(&state, 1, Value::list(vec![Value::Bool(false), Value::Null, Value::I64(0)]), &memory);
    let context = context(Some(&state), &memory);
    let statement = PreparedSql::prepare("SELECT v FROM docs FOR VALID_TIME ALL", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = statement.open_query(Arc::clone(&context), vec![]).await.unwrap();
    let batch = cursor.next_batch().await.unwrap().unwrap();
    let field = batch.batch().schema().field(0).clone();
    assert_eq!(read_value(&field, batch.batch().column(0).as_ref(), 0).unwrap(), Value::list(vec![Value::Bool(false), Value::Null, Value::I64(0)]));
}

#[tokio::test]
async fn a_join_on_two_set_columns_raises_set_equality() {
    let memory = MemoryBudget::new(1 << 26);
    let state = PartitionState::empty(rtdb_trie::TrieConfig::new(32, 32).unwrap()).unwrap();
    write(&state, 1, Value::Set(vec![Value::I64(1)]), &memory);
    let context = context(Some(&state), &memory);
    let sql = "SELECT a._id FROM docs a JOIN docs b ON a.v = b.v";
    let error = match PreparedSql::prepare(sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic) {
        Err(error) => error,
        Ok(statement) => match statement.open_query(Arc::clone(&context), vec![]).await {
            Err(error) => error,
            Ok(mut cursor) => loop { match cursor.next_batch().await { Err(error) => break error, Ok(Some(_)) => continue, Ok(None) => panic!("joined sets") } },
        },
    };
    assert_eq!(error.code, "xtdb.expression/set-equality");
}

#[tokio::test]
async fn a_column_join_returns_the_row_joins_rows_in_its_order() {
    let memory = MemoryBudget::new(1 << 28);
    let state = PartitionState::empty(rtdb_trie::TrieConfig::new(32, 32).unwrap()).unwrap();
    let mut buffer = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(1), system_time: Timestamp(1) }, Arc::clone(&memory)).unwrap();
    for id in 0..400i64 {
        let customer = match id % 11 { 0 => Value::Null, 1 => Value::Utf8(format!("c{}", id % 7)), _ => Value::I64(id % 37) };
        buffer.put(TableRef::new("public", "orders"), StructFields::from([("_id".into(), Value::I64(id)), ("customer".into(), customer), ("amount".into(), Value::F64(id as f64 / 8.0))]), Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
    }
    for id in 0..45i64 {
        let key = if id % 9 == 0 { Value::Utf8(format!("c{}", id % 7)) } else { Value::I64(id) };
        buffer.put(TableRef::new("public", "customers"), StructFields::from([("_id".into(), key), ("region".into(), Value::Utf8(["eu", "us", "apac"][id as usize % 3].into()))]), Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
    }
    state.apply(&buffer.stage().unwrap()).unwrap();
    let mut results = Vec::new();
    for build_rows in [100_000, 50] {
        let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let options = QueryOptions { join_build_rows: build_rows, ..QueryOptions::default() };
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(100), "UTC", options, &CancellationToken::new()).unwrap();
        for suffix in ["", " LIMIT 5000"] {
            let sql = format!("SELECT c.region, o._id, o.amount, c._id AS customer FROM orders o JOIN customers c ON o.customer = c._id ORDER BY c.region{suffix}");
            let statement = PreparedSql::prepare(&sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
            results.push(rows_of(statement.open_query(Arc::clone(&context), vec![]).await.unwrap()).await);
        }
    }
    assert!(results[0].len() > 300, "the join should match most orders, matched {}", results[0].len());
    assert_eq!(results[0], results[1], "in memory, the column join returns the row join's rows in its order");
    let sorted = |rows: &Vec<Vec<Value>>| { let mut rows = rows.iter().map(|row| format!("{row:?}")).collect::<Vec<_>>(); rows.sort(); rows };
    assert_eq!(sorted(&results[2]), sorted(&results[0]), "a spilled row join returns the same rows");
    assert_eq!(sorted(&results[3]), sorted(&results[0]), "a column join that hands a large build to the row join returns the same rows");
}

async fn rows_of(mut cursor: rtdb_query::QueryCursor) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        let schema = batch.batch().schema();
        for row in 0..batch.batch().num_rows() {
            rows.push((0..batch.batch().num_columns()).map(|column| read_value(schema.field(column), batch.batch().column(column).as_ref(), row).unwrap()).collect());
        }
    }
    rows
}

#[tokio::test]
async fn a_mixed_width_id_column_filters_and_reads_as_the_row_path_does() {
    let memory = MemoryBudget::new(1 << 28);
    let state = PartitionState::empty(rtdb_trie::TrieConfig::new(32, 32).unwrap()).unwrap();
    let mut buffer = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(1), system_time: Timestamp(1) }, Arc::clone(&memory)).unwrap();
    for n in 0..600i64 {
        let id = match n % 3 { 0 => Value::I16(n as i16), 1 => Value::I32(40_000 + n as i32), _ => Value::I64(n) };
        buffer.put(TableRef::new("public", "mixed"), StructFields::from([("_id".into(), id), ("v".into(), Value::F64(n as f64))]), Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
    }
    state.apply(&buffer.stage().unwrap()).unwrap();
    let context = context(Some(&state), &memory);
    let run = |sql: &'static str| { let context = Arc::clone(&context); async move {
        let statement = PreparedSql::prepare(sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
        rows_of(statement.open_query(Arc::clone(&context), vec![]).await.unwrap()).await
    } };
    let filtered = run("SELECT COUNT(*), SUM(v) FROM mixed WHERE _id < 300").await;
    let all = run("SELECT _id, v FROM mixed").await;
    let expected = all.iter().filter(|row| matches!(row[0], Value::I16(_) | Value::I32(_) | Value::I64(_)) && match row[0] { Value::I16(id) => id < 300, Value::I32(id) => id < 300, Value::I64(id) => id < 300, _ => false }).collect::<Vec<_>>();
    let sum: f64 = expected.iter().map(|row| match row[1] { Value::F64(v) => v, _ => 0.0 }).sum();
    assert_eq!(filtered, vec![vec![Value::I64(expected.len() as i64), Value::F64(sum)]]);
    let ordered = run("SELECT _id, v FROM mixed ORDER BY v LIMIT 1000").await;
    let rows = run("SELECT _id, v FROM mixed ORDER BY v").await;
    assert_eq!(ordered, rows);
}

#[tokio::test]
async fn a_column_written_at_several_widths_across_transactions_reads_and_filters_as_written() {
    let memory = MemoryBudget::new(1 << 28);
    let state = PartitionState::empty(rtdb_trie::TrieConfig::new(32, 32).unwrap()).unwrap();
    let mut written = Vec::new();
    for tx in 0..4i64 {
        let mut buffer = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(tx + 1), system_time: Timestamp(tx + 1) }, Arc::clone(&memory)).unwrap();
        for n in (tx * 150)..(tx * 150 + 150) {
            let id = match (tx, n % 3) { (0, _) | (3, 0) => Value::I16(n as i16), (1, _) | (3, 1) => Value::I32(40_000 + n as i32), _ => Value::I64(n) };
            let w = if tx % 2 == 0 { Value::I64(n) } else { Value::Utf8(format!("w{n}")) };
            buffer.put(TableRef::new("public", "widths"), StructFields::from([("_id".into(), id.clone()), ("v".into(), Value::F64(n as f64)), ("w".into(), w.clone())]), Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
            written.push(vec![id, Value::F64(n as f64), w]);
        }
        state.apply(&buffer.stage().unwrap()).unwrap();
    }
    let context = context(Some(&state), &memory);
    let run = |sql: &'static str| { let context = Arc::clone(&context); async move {
        let statement = PreparedSql::prepare(sql, &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
        rows_of(statement.open_query(Arc::clone(&context), vec![]).await.unwrap()).await
    } };
    let key = |row: &Vec<Value>| match row[1] { Value::F64(v) => v as i64, _ => panic!("v is a float") };
    let mut rows = run("SELECT _id, v, w FROM widths").await;
    rows.sort_by_key(key);
    written.sort_by_key(key);
    assert_eq!(rows, written);
    let id = |value: &Value| match value { Value::I16(id) => i64::from(*id), Value::I32(id) => i64::from(*id), Value::I64(id) => *id, _ => panic!("_id is an integer") };
    let expected = written.iter().filter(|row| id(&row[0]) < 300).collect::<Vec<_>>();
    let sum: f64 = expected.iter().map(|row| match row[1] { Value::F64(v) => v, _ => 0.0 }).sum();
    assert_eq!(run("SELECT COUNT(*), SUM(v) FROM widths WHERE _id < 300").await, vec![vec![Value::I64(expected.len() as i64), Value::F64(sum)]]);
}

#[tokio::test]
async fn a_prepared_query_replans_once_a_write_changes_the_columns_it_read() {
    let memory = MemoryBudget::new(1 << 28);
    let state = PartitionState::empty(rtdb_trie::TrieConfig::new(32, 32).unwrap()).unwrap();
    let write = |tx: i64, doc: StructFields<Value>| {
        let mut buffer = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(tx), system_time: Timestamp(tx) }, Arc::clone(&memory)).unwrap();
        buffer.put(TableRef::new("public", "docs"), doc, Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
        state.apply(&buffer.stage().unwrap()).unwrap();
    };
    write(1, StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::I64(10))]));
    let statement = PreparedSql::prepare("SELECT * FROM docs WHERE _id > $1 ORDER BY _id", &context(Some(&state), &memory), PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let run = || async { rows_of(statement.open_query(context(Some(&state), &memory), vec![Value::I64(0)]).await.unwrap()).await };
    assert_eq!(run().await, vec![vec![Value::I64(1), Value::I64(10)]]);
    write(2, StructFields::from([("_id".into(), Value::I64(2)), ("v".into(), Value::I64(20))]));
    assert_eq!(run().await, vec![vec![Value::I64(1), Value::I64(10)], vec![Value::I64(2), Value::I64(20)]], "a later snapshot with the same columns reads its new rows");
    write(3, StructFields::from([("_id".into(), Value::I64(3)), ("v".into(), Value::I64(30)), ("w".into(), Value::Utf8("new".into()))]));
    assert_eq!(run().await, vec![
        vec![Value::I64(1), Value::I64(10), Value::Null],
        vec![Value::I64(2), Value::I64(20), Value::Null],
        vec![Value::I64(3), Value::I64(30), Value::Utf8("new".into())],
    ]);
}

#[tokio::test]
async fn a_join_and_aggregate_over_integer_keys_of_several_widths_and_null_keys_match_the_data() {
    let memory = MemoryBudget::new(1 << 28);
    let state = PartitionState::empty(rtdb_trie::TrieConfig::new(32, 32).unwrap()).unwrap();
    let regions = ["eu", "us", "apac"];
    let mut tx = 0;
    let mut write = |table: &str, docs: Vec<StructFields<Value>>| {
        tx += 1;
        let mut buffer = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(tx), system_time: Timestamp(tx) }, Arc::clone(&memory)).unwrap();
        for doc in docs { buffer.put(TableRef::new("public", table), doc, Interval::from_micros(0, i64::MAX).unwrap()).unwrap(); }
        state.apply(&buffer.stage().unwrap()).unwrap();
    };
    write("customers", (0..30i64).map(|id| StructFields::from([("_id".into(), Value::I64(id)), ("region".into(), Value::Utf8(regions[(id % 3) as usize].into()))])).collect());
    let mut expected: BTreeMap<&str, (i64, f64)> = BTreeMap::new();
    for (batch, width) in [(0i64, 16), (1, 32), (2, 64)] {
        let docs = (batch * 100..batch * 100 + 100).map(|order| {
            let customer = order % 37;
            let key = if order % 11 == 0 { Value::Null } else { match width { 16 => Value::I16(customer as i16), 32 => Value::I32(customer as i32), _ => Value::I64(customer) } };
            if !matches!(key, Value::Null) && customer < 30 {
                let entry = expected.entry(regions[(customer % 3) as usize]).or_default();
                entry.0 += 1;
                entry.1 += order as f64 / 4.0;
            }
            StructFields::from([("_id".into(), Value::I64(order)), ("customer_id".into(), key), ("amount".into(), Value::F64(order as f64 / 4.0))])
        }).collect();
        write("orders", docs);
    }
    let context = context(Some(&state), &memory);
    let statement = PreparedSql::prepare("SELECT c.region, COUNT(*), SUM(o.amount) FROM orders o JOIN customers c ON o.customer_id = c._id GROUP BY c.region ORDER BY c.region", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let rows = rows_of(statement.open_query(Arc::clone(&context), vec![]).await.unwrap()).await;
    assert_eq!(rows.len(), expected.len());
    for (row, (region, (count, sum))) in rows.iter().zip(&expected) {
        assert_eq!(row[0], Value::Utf8((*region).into()));
        assert_eq!(row[1], Value::I64(*count));
        let Value::F64(actual) = row[2] else { panic!("SUM over floats is a float: {:?}", row[2]) };
        assert!((actual - sum).abs() < 1e-9, "{region}: {actual} against {sum}");
    }
}

#[tokio::test]
async fn an_inner_join_whose_left_input_exceeds_the_build_limit_builds_its_right_input_and_keeps_its_columns() {
    let memory = MemoryBudget::new(1 << 28);
    let state = PartitionState::empty(rtdb_trie::TrieConfig::new(32, 32).unwrap()).unwrap();
    let mut buffer = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(1), system_time: Timestamp(1) }, Arc::clone(&memory)).unwrap();
    for id in 0..30i64 { buffer.put(TableRef::new("public", "customers"), StructFields::from([("_id".into(), Value::I64(id)), ("name".into(), Value::Utf8(format!("c{id}")))]), Interval::from_micros(0, i64::MAX).unwrap()).unwrap(); }
    for id in 0..300i64 { buffer.put(TableRef::new("public", "orders"), StructFields::from([("_id".into(), Value::I64(1000 + id)), ("customer_id".into(), Value::I64(id % 40))]), Interval::from_micros(0, i64::MAX).unwrap()).unwrap(); }
    state.apply(&buffer.stage().unwrap()).unwrap();
    let base = context(Some(&state), &memory);
    let context = QueryContext::new(base.catalog.clone(), Arc::clone(&memory), Timestamp(100), "UTC", QueryOptions { join_build_rows: 50, ..Default::default() }, &CancellationToken::new()).unwrap();
    let statement = PreparedSql::prepare("SELECT c.name, COUNT(*), SUM(o._id) FROM orders o JOIN customers c ON o.customer_id = c._id GROUP BY c.name ORDER BY c.name", &context, PrepareOptions::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let rows = rows_of(statement.open_query(Arc::clone(&context), vec![]).await.unwrap()).await;
    let mut expected: BTreeMap<String, (i64, i64)> = BTreeMap::new();
    for id in (0..300i64).filter(|id| id % 40 < 30) { let entry = expected.entry(format!("c{}", id % 40)).or_default(); entry.0 += 1; entry.1 += 1000 + id; }
    let expected = expected.into_iter().map(|(name, (count, sum))| vec![Value::Utf8(name), Value::I64(count), Value::I64(sum)]).collect::<Vec<_>>();
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn declared_preparation_casts_a_bound_fixed_float_list_column() {
    use arrow_array::{Array, FixedSizeListArray, Float32Array, RecordBatch, cast::AsArray, types::Float64Type};
    use arrow_schema::{DataType, Field, Schema};
    use rtdb_columnar::OwnedBatch;

    let memory = MemoryBudget::new(16 << 20);
    let context = context(None, &memory);
    let item = Arc::new(Field::new("$data$", DataType::Float32, false));
    let vector = FixedSizeListArray::new(item, 2, Arc::new(Float32Array::from(vec![-0.0, f32::MIN_POSITIVE])), None);
    let field = Field::new("vector", vector.data_type().clone(), false);
    let ty = rtdb_columnar::from_field(&field).unwrap();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(vector)]).unwrap();
    let batch = OwnedBatch::new(batch, &memory).unwrap();
    let options = PrepareOptions {
        parameter_types: BTreeMap::from([(0, LogicalType::list(LogicalType::Scalar(ScalarType::F64)))]),
        ..Default::default()
    };
    let query = PreparedSql::prepare("SELECT $1 AS vector", &context, options, ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = query.try_open_query_arrow(context, &batch, &[ty]).await.unwrap().unwrap();
    let result = cursor.next_batch().await.unwrap().unwrap();
    let vector = result.batch().column(0).as_list::<i32>().value(0);
    let values = vector.as_primitive::<Float64Type>();
    assert_eq!(values.value(0).to_bits(), (-0.0f64).to_bits());
    assert_eq!(values.value(1).to_bits(), f64::from(f32::MIN_POSITIVE).to_bits());
}
