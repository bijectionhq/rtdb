use rtdb_types::StructFields;
use std::sync::Arc;
use rtdb_columnar::{BatchBuilder, MemoryBudget, MemoryReservation, arrow_relayout_bound, relayout_arrow_column, to_field};
use rtdb_format::transit::{TransitValue as T, encode_json};
use rtdb_node::AutonomousTxOptions;
use rtdb_tx::{ArrowDocumentWrite, ArrowDocuments, Documents, SqlArguments, TxOp, UserMetadata};
use rtdb_types::{ListValue, ListView, LogicalType, Result, ScalarType, TableRef, Timestamp, Value};
use tokio_util::sync::CancellationToken;
use crate::statement::SessionHandle;

pub(crate) fn encode(value: &Value) -> Result<Vec<u8>> { encode_json(&output(value, false)?) }
#[cfg(test)]
pub(crate) fn encode_row(row: &Value) -> Result<Vec<u8>> {
    let encoded = encode_json(&T::Array(vec![output(row, true)?]))?;
    Ok(encoded[1..encoded.len() - 1].to_vec())
}
fn output(value: &Value, typed_structs: bool) -> Result<T> {
    let tag = |name: &str, value| T::Tagged { tag: name.into(), representation: Box::new(value) };
    Ok(match value {
        Value::I8(value) => tag("i8", T::Integer(i64::from(*value))), Value::I16(value) => tag("i16", T::Integer(i64::from(*value))), Value::I32(value) => tag("i32", T::Integer(i64::from(*value))),
        Value::F32(value) => tag("f32", T::Float(f64::from(*value))),
        Value::List(_) | Value::FixedSizeList(_) => T::Array(value.list_view().unwrap().iter().map(|value| output(value.as_ref(), typed_structs)).collect::<Result<_>>()?),
        Value::Set(values) => tag("set", T::Array(values.iter().map(|value| output(value, typed_structs)).collect::<Result<_>>()?)),
        Value::Struct(fields) => {
            let fields = T::Map(fields.iter().filter(|(_, value)| !matches!(value, Value::Null | Value::Undefined)).map(|(key, value)| Ok((T::String(key.clone()), output(value, typed_structs)?))).collect::<Result<_>>()?);
            if typed_structs { tag("rtdb/struct", fields) } else { fields }
        }
        Value::Map { entries, .. } => T::Map(entries.iter().map(|(key, value)| Ok((output(key, typed_structs)?, output(value, typed_structs)?))).collect::<Result<_>>()?),
        value => rtdb_pg_codec::to_transit(value)?,
    })
}

pub(crate) fn bind_arguments(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<SqlArguments> {
    let (value, _decode_reservation) = rtdb_pg_codec::decode_transit_msgpack(bytes, memory)?.into_parts();
    let Value::List(rows) = value else { return Err(crate::invalid("Transit bind requires an array of argument rows")); };
    let rows = rows.into_values().into_iter().map(|row| match row {
        Value::List(row) => Ok(row.into_values()),
        _ => Err(crate::invalid("Each Transit argument row must be an array")),
    }).collect::<Result<Vec<_>>>()?;
    SqlArguments::from_packed_rows(rows, memory)
}

pub(crate) struct TransactionInput {
    value: Value,
    reservation: Arc<MemoryReservation>,
}

impl TransactionInput {
    pub(crate) fn decode(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let (value, reservation) = rtdb_pg_codec::decode_transit_transaction(bytes, memory)?.into_parts();
        Ok(Self { value, reservation })
    }
}

pub(crate) fn transaction(session: &Arc<SessionHandle>, token: &CancellationToken, input: TransactionInput) -> Result<Value> {
    let memory = session.region.memory();
    let TransactionInput { value, reservation: _reservation } = input;
    let mut request = fields(value)?;
    let asynchronous = match request.remove("asynchronous").unwrap_or(Value::Bool(false)) { Value::Bool(value) => value, _ => return Err(crate::invalid("asynchronous must be boolean")) };
    let mut options = fields(request.remove("options").unwrap_or_else(|| Value::Struct(StructFields::new())))?;
    let timezone = optional_string(options.remove("default_timezone"))?;
    let database = optional_string(options.remove("database"))?;
    let system_time = optional_time(options.remove("system_time"), timezone.as_deref().unwrap_or("UTC"))?;
    let user_metadata = options.remove("metadata").filter(|value| !matches!(value, Value::Null)).map(|value| UserMetadata::new(value, memory)).transpose()?;
    if !options.is_empty() { return Err(crate::invalid("Unknown transaction option")); }
    let operations = list(request.remove("operations").ok_or_else(|| crate::invalid("operations is required"))?)?;
    let operations = operations.into_values().into_iter().filter_map(|operation| operation_value(operation, memory).transpose()).collect::<Result<Vec<_>>>()?;
    if !request.is_empty() { return Err(crate::invalid("Unknown transaction request field")); }
    session.region.run(async {
        let mut state = session.state.lock();
        let options = AutonomousTxOptions { database, default_timezone: timezone, system_time, user_metadata };
        token.run_until_cancelled(async {
            let mut values = StructFields::new();
            if asynchronous {
                let submitted = state.state_mut().submit_tx(operations, options).await?;
                values.insert("tx_id".into(), Value::I64(submitted.tx_id.0));
                if let Some(receipt) = submitted.placed_receipt { values.insert("await_token".into(), Value::Utf8(receipt)); }
            } else {
                let result = state.state_mut().execute_tx(operations, options).await?;
                if let Some(error) = result.error() { return Err(error.clone()); }
                if !result.committed() { return Err(rtdb_types::Anomaly::fault("xtdb/tx-aborted", "Transaction aborted")); }
                let key = result.tx_key();
                values.insert("tx_id".into(), Value::I64(key.tx_id.0));
                values.insert("system_time".into(), Value::Timestamp { value: key.system_time.0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) });
            }
            Ok(Value::Struct(values))
        }).await.ok_or_else(crate::cancelled)?
    })
}
/// Deletes and erases are submitted as SQL DML, as the reference Clojure client submits them over ADBC,
/// so an empty or inverted valid-time range deletes nothing rather than aborting the transaction.
fn operation_value(value: Value, memory: &Arc<MemoryBudget>) -> Result<Option<TxOp>> {
    let mut value = fields(value)?;
    let kind = string(value.remove("op").ok_or_else(|| crate::invalid("Transaction operation requires op"))?)?;
    if kind == "sql" {
        let sql = string(value.remove("sql").ok_or_else(|| crate::invalid("SQL operation requires sql"))?)?;
        let arguments = value.remove("arguments").filter(|value| !matches!(value, Value::Null));
        let rows = arguments.map(|value| list(value)?.into_values().into_iter()
            .map(|row| list(row).map(ListValue::into_values)).collect::<Result<Vec<_>>>()).transpose()?;
        if !value.is_empty() { return Err(crate::invalid("Unknown SQL operation field")); }
        if rows.as_ref().is_some_and(Vec::is_empty) { return Ok(None); }
        let args = rows.map(|rows| SqlArguments::from_packed_rows(rows, memory)).transpose()?;
        return Ok(Some(TxOp::Sql { sql, args }));
    }
    let schema = optional_string(value.remove("schema"))?.unwrap_or_else(|| "public".into());
    let table = TableRef::new(schema, string(value.remove("table").ok_or_else(|| crate::invalid("Document operation requires table"))?)?);
    let valid_from = optional_time(value.remove("valid_from"), "UTC")?;
    let valid_to = optional_time(value.remove("valid_to"), "UTC")?;
    if kind == "erase-docs" && (valid_from.is_some() || valid_to.is_some()) { return Err(crate::invalid("Erase operations do not accept valid-time bounds")); }
    let operation = match kind.as_str() {
        "put-docs" | "patch-docs" => {
            let documents = Documents::new(list(value.remove("documents").ok_or_else(|| crate::invalid("Document operation requires documents"))?)?.into_values().into_iter().map(fields).collect::<Result<_>>()?, memory)?;
            if let Some(documents) = ArrowDocuments::from_submitted_rows(&documents, memory) {
                let write = if kind == "patch-docs" { ArrowDocumentWrite::Patch } else { ArrowDocumentWrite::Put };
                TxOp::WriteArrowDocs { table, documents, write, valid_from, valid_to }
            } else if kind == "patch-docs" {
                TxOp::PatchDocs { table, documents, valid_from, valid_to }
            } else {
                TxOp::PutDocs { table, documents, valid_from, valid_to }
            }
        }
        "delete-docs" | "erase-docs" => {
            let ids = list(value.remove("ids").ok_or_else(|| crate::invalid("Document operation requires ids"))?)?;
            if !value.is_empty() { return Err(crate::invalid("Unknown document operation field")); }
            if ids.is_empty() { return Ok(None); }
            let name = format!("{}.{}", quoted(&table.schema), quoted(&table.table));
            let bounded = valid_from.is_some() || valid_to.is_some();
            let sql = if kind == "erase-docs" { format!("ERASE FROM {name} WHERE _id = ?") }
                else if bounded { format!("DELETE FROM {name} FOR VALID_TIME FROM ? TO ? WHERE _id = ?") }
                else { format!("DELETE FROM {name} WHERE _id = ?") };
            let times = [valid_from, valid_to];
            let times = if bounded { &times[..] } else { &[] };
            let args = delete_arguments(&ids, times, memory)?;
            return Ok(Some(TxOp::Sql { sql, args: Some(args) }));
        }
        _ => return Err(crate::invalid("Unknown transaction operation")),
    };
    if !value.is_empty() { return Err(crate::invalid("Unknown document operation field")); }
    Ok(Some(operation))
}
fn delete_arguments(ids: &ListValue, times: &[Option<Timestamp>], memory: &Arc<MemoryBudget>) -> Result<SqlArguments> {
    if let Some((field, column)) = ids.packed_column() {
        let ty = ids.element_type();
        let supported = ty.legs().iter().all(|leg| matches!(leg, LogicalType::Scalar(
            ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64 | ScalarType::Utf8 | ScalarType::Keyword | ScalarType::Uuid
        )));
        if supported
            && let Ok(args) = packed_delete_arguments(&ty, &field, column, times, memory) {
            return Ok(args);
        }
    }
    for id in ids.iter() { id.entity_id()?; }
    let rows = ids.iter().map(|id| {
        times.iter().copied().map(delete_time).chain(std::iter::once(id.into_owned())).collect()
    }).collect();
    SqlArguments::from_packed_rows(rows, memory)
}

fn delete_time(time: Option<Timestamp>) -> Value {
    time.map_or(Value::Null, |time| Value::Timestamp {
        value: time.0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()),
    })
}

fn packed_delete_arguments(
    ty: &LogicalType, source: &arrow_schema::Field, ids: arrow_array::ArrayRef,
    times: &[Option<Timestamp>], memory: &Arc<MemoryBudget>,
) -> Result<SqlArguments> {
    use arrow_array::{RecordBatch, TimestampMicrosecondArray, new_null_array};

    let len = ids.len();
    let mut builder = BatchBuilder::new(4096, memory)?;
    let mut columns = Vec::with_capacity(times.len() + 1);
    let mut fields = Vec::with_capacity(columns.capacity());
    for (index, time) in times.iter().copied().enumerate() {
        let field = to_field(&format!("?_{index}"), &delete_time(time).logical_type())?;
        let bound = crate::add(crate::mul(len, 8)?, 4096)?;
        columns.push(builder.column(bound, || Ok(match time {
            Some(time) => Arc::new(TimestampMicrosecondArray::from_value(time.0, len).with_timezone("UTC")),
            None => new_null_array(field.data_type(), len),
        }))?);
        fields.push(field);
    }
    let target = to_field(&format!("?_{}", fields.len()), ty)?;
    let ids = if *source == target.clone().with_name(source.name()) {
        builder.column(ids.get_array_memory_size(), || Ok(ids))?
    } else {
        let bound = arrow_relayout_bound(ids.as_ref(), source, &target)?.max(ids.get_array_memory_size());
        builder.column(bound, || relayout_arrow_column(ids, source, &target).map(|(_, array)| array))?
    };
    columns.push(ids);
    fields.push(target);
    let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns).map_err(|error| crate::invalid(error.to_string()))?;
    SqlArguments::from_row_encoded_arrow(builder.finish(batch)?, memory)
}

fn quoted(identifier: &str) -> String { format!("\"{}\"", identifier.replace('"', "\"\"")) }
fn fields(value: Value) -> Result<StructFields<Value>> { match value { Value::Struct(value) => Ok(value), _ => Err(crate::invalid("Expected a Transit map")) } }
fn list(value: Value) -> Result<ListValue> { match value { Value::List(value) => Ok(value), _ => Err(crate::invalid("Expected a Transit array")) } }
fn string(value: Value) -> Result<String> { match value { Value::Utf8(value) => Ok(value), _ => Err(crate::invalid("Expected a Transit string")) } }
fn optional_string(value: Option<Value>) -> Result<Option<String>> { value.filter(|value| !matches!(value, Value::Null)).map(string).transpose() }
fn optional_time(value: Option<Value>, timezone: &str) -> Result<Option<Timestamp>> {
    value.filter(|value| !matches!(value, Value::Null)).map(|value| rtdb_expr::timestamp_micros(&value, timezone).map(|value| Timestamp(value.0))).transpose()
}

#[test]
fn transaction_input_releases_transit_scratch_before_submission() {
    let message = T::Map(vec![(T::String("operations".into()), T::Array(vec![T::String("SELECT 1".into())]))]);
    let payload = rtdb_format::transit::encode_msgpack(&message).unwrap();
    let memory = MemoryBudget::new(1 << 20);
    let input = TransactionInput::decode(&payload, &memory).unwrap();
    assert_eq!(memory.used_bytes(), input.value.retained_bytes().unwrap() + 8192);
    assert!(memory.used_bytes() < memory.peak_bytes());
    assert_eq!(input.value, Value::Struct(StructFields::from([("operations".into(), Value::list(vec![Value::Utf8("SELECT 1".into())]))])));
    drop(input);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn signed_delete_ids_keep_the_buffer_and_canonical_source_layout() {
    let memory = MemoryBudget::new(1 << 24);
    let ids = ListValue::from(vec![Value::I64(1), Value::I64(2)]);
    let args = delete_arguments(&ids, &[Some(Timestamp(1)), None], &memory).unwrap();
    let page = args.single_arrow_page().unwrap().batch();
    let source = ids.array().unwrap().to_data();
    assert_eq!(page.column(2).to_data().buffers()[0].as_ptr(), source.buffers()[0].as_ptr());

    for ids in [ids, ListValue::from(vec![Value::I16(1), Value::I32(2), Value::I8(3)])] {
        let times = [Some(Timestamp(1)), None];
        let arrow = delete_arguments(&ids, &times, &memory).unwrap();
        let old = SqlArguments::from_rows(ids.into_values().into_iter().map(|id| {
            vec![delete_time(times[0]), Value::Null, id]
        }).collect(), &memory).unwrap();
        let encode = |args| rtdb_tx::SourceTransaction::new(
            vec![TxOp::Sql { sql: "DELETE FROM docs FOR VALID_TIME FROM ? TO ? WHERE _id = ?".into(), args: Some(args) }],
            Default::default(), &memory,
        ).unwrap().encode().unwrap();
        assert_eq!(encode(arrow).as_slice(), encode(old).as_slice());
    }
}

#[test]
fn transit_delete_and_erase_ids_bind_the_decoded_columns_with_legacy_source_bytes() {
    use arrow_array::{Array, cast::AsArray};

    let memory = MemoryBudget::new(1 << 25);
    let tag = |tag: &str, representation| T::Tagged { tag: tag.into(), representation: Box::new(representation) };
    let ids = [
        T::String("places/one".into()),
        T::Keyword("places/one".into()),
        tag("u", T::Array(vec![T::Integer(i64::MIN), T::Integer(-2)])),
        tag("i16", T::Integer(5)),
        T::Integer(i64::MIN),
        tag("u", T::String("6ba7b810-9dad-11d1-80b4-00c04fd430c8".into())),
    ];
    let encode = |sql: String, args| rtdb_tx::SourceTransaction::new(
        vec![TxOp::Sql { sql, args: Some(args) }], Default::default(), &memory,
    ).unwrap().encode().unwrap();

    for (kind, ids, time) in [
        ("delete-docs", ids[..1].to_vec(), Some(Timestamp(1))),
        ("erase-docs", ids[1..2].to_vec(), None),
        ("erase-docs", ids[2..3].to_vec(), None),
        ("delete-docs", ids.iter().cycle().take(263).cloned().collect(), Some(Timestamp(2))),
    ] {
        let mut operation = vec![
            (T::String("op".into()), T::String(kind.into())),
            (T::String("table".into()), T::String("docs".into())),
            (T::Keyword("ids".into()), tag("list", T::Array(ids.clone()))),
        ];
        if let Some(time) = time {
            operation.push((T::String("valid_from".into()), tag("time/instant", T::String(format!("1970-01-01T00:00:00.00000{}Z", time.0)))));
        }
        let request = T::Map(vec![(T::String("operations".into()), T::Array(vec![T::Map(operation)]))]);
        let payload = rtdb_format::transit::encode_msgpack(&request).unwrap();
        let TransactionInput { value, reservation: _wire } = TransactionInput::decode(&payload, &memory).unwrap();
        let mut request = fields(value).unwrap();
        let [operation] = list(request.remove("operations").unwrap()).unwrap().into_values().try_into().unwrap();
        let Some(Value::List(decoded)) = operation.field("ids") else { panic!("expected delete IDs"); };
        let ListView::Scalars(_) = decoded.view() else { panic!("expected packed IDs"); };
        let (source, array) = decoded.packed_column().unwrap();
        let Some(TxOp::Sql { sql, args: Some(arrow) }) = operation_value(operation, &memory).unwrap() else { panic!("expected SQL"); };
        let page = arrow.single_arrow_page().unwrap().batch();
        let index = page.num_columns() - 1;
        assert_eq!(page.schema_ref().field(index), &source.as_ref().clone().with_name(format!("?_{index}")));
        let column = page.column(index);
        assert_eq!(column.to_data().buffers()[0].as_ptr(), array.to_data().buffers()[0].as_ptr());
        if let Some(union) = column.as_union_opt() {
            assert_eq!(union.type_ids().as_ptr(), array.as_union().type_ids().as_ptr());
        }

        let legacy = SqlArguments::from_rows(ids.into_iter().map(|id| {
            let id = rtdb_pg_codec::from_transit(id).unwrap();
            if time.is_some() { vec![delete_time(time), Value::Null, id] } else { vec![id] }
        }).collect(), &memory).unwrap();
        assert_eq!(encode(sql.clone(), arrow).as_slice(), encode(sql, legacy).as_slice());
    }
}

#[test]
fn transit_delete_ids_preserve_native_tag_errors_and_invalid_id_errors() {
    let memory = MemoryBudget::new(1 << 24);
    let tagged = |tag: &str, repr| T::Tagged { tag: tag.into(), representation: Box::new(repr) };
    for (id, code) in [
        (T::Null, "xtdb/invalid-id"),
        (tagged("time/date", T::String("2020-01-01".into())), "xtdb/invalid-id"),
        (tagged("u", T::String("invalid".into())), "xtdb.postgres/invalid-column-value"),
    ] {
        let operation = T::Map(vec![
            (T::String("op".into()), T::String("delete-docs".into())),
            (T::String("table".into()), T::String("docs".into())),
            (T::String("ids".into()), T::Array(vec![T::String("id".into()), id])),
        ]);
        let request = T::Map(vec![(T::String("operations".into()), T::Array(vec![operation.clone()]))]);
        let payload = rtdb_format::transit::encode_msgpack(&request).unwrap();
        let legacy = rtdb_pg_codec::from_transit(operation).and_then(|value| operation_value(value, &memory)).unwrap_err();
        assert_eq!(legacy.code, code);
        let actual = TransactionInput::decode(&payload, &memory).and_then(|input| {
            let mut request = fields(input.value)?;
            let [operation] = list(request.remove("operations").unwrap())?.into_values().try_into().unwrap();
            operation_value(operation, &memory)
        }).unwrap_err();
        assert_eq!(actual, legacy);
    }
}

#[test]
fn deletes_and_erases_are_submitted_as_sql_dml() {
    let memory = MemoryBudget::new(1 << 20);
    let time = Value::Timestamp { value: 1_000, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    let op = |kind: &str, ids: Vec<Value>, bounds: bool| {
        let mut fields = StructFields::from([("op".into(), Value::Utf8(kind.into())), ("table".into(), Value::Utf8("docs".into())), ("ids".into(), Value::list(ids))]);
        if bounds { fields.insert("valid_from".into(), time.clone()); }
        operation_value(Value::Struct(fields), &memory).unwrap()
    };
    let Some(TxOp::Sql { sql, args: Some(args) }) = op("delete-docs", vec![Value::I64(1)], true) else { panic!("expected SQL") };
    assert!(args.single_arrow_page().is_some());
    assert_eq!(sql, "DELETE FROM \"public\".\"docs\" FOR VALID_TIME FROM ? TO ? WHERE _id = ?");
    assert_eq!(args.rows().map(|row| row.unwrap().to_vec()).collect::<Vec<_>>(), [&[time.clone(), Value::Null, Value::I64(1)][..]]);
    let Some(TxOp::Sql { sql, .. }) = op("delete-docs", vec![Value::I64(1)], false) else { panic!("expected SQL") };
    assert_eq!(sql, "DELETE FROM \"public\".\"docs\" WHERE _id = ?");
    let Some(TxOp::Sql { sql, .. }) = op("erase-docs", vec![Value::I64(1)], false) else { panic!("expected SQL") };
    assert_eq!(sql, "ERASE FROM \"public\".\"docs\" WHERE _id = ?");
    assert!(op("delete-docs", vec![], false).is_none());
}


#[test]
fn transit_bind_and_transaction_keep_float_arguments_in_canonical_arrow() {
    use arrow_array::{Array, cast::AsArray, types::Float64Type};

    let memory = MemoryBudget::new(1 << 24);
    let arguments = T::Array(vec![T::Array(vec![T::Array(vec![T::Float(-0.0), T::Null, T::Float(2.5)])]), T::Array(vec![T::Null])]);
    let payload = rtdb_format::transit::encode_msgpack(&arguments).unwrap();
    let args = bind_arguments(&payload, &memory).unwrap();
    let arrow = args.single_arrow_page().unwrap().batch().column(0).as_list::<i32>();
    assert!(arrow.is_null(1));
    let floats = arrow.values().as_primitive::<Float64Type>();
    assert_eq!(floats.value(0).to_bits(), (-0.0f64).to_bits());
    assert!(floats.is_null(1));

    let request = T::Map(vec![
        (T::String("op".into()), T::String("sql".into())),
        (T::String("sql".into()), T::String("SELECT ?".into())),
        (T::String("arguments".into()), arguments.clone()),
    ]);
    let payload = rtdb_format::transit::encode_msgpack(&request).unwrap();
    let input = TransactionInput::decode(&payload, &memory).unwrap();
    let TxOp::Sql { sql, args: Some(transaction) } = operation_value(input.value, &memory).unwrap().unwrap() else { panic!("expected SQL"); };
    assert!(transaction.single_arrow_page().is_some());
    let Value::List(rows) = rtdb_pg_codec::from_transit(arguments).unwrap() else { panic!("expected rows"); };
    let rows = rows.into_values().into_iter().map(|value| list(value).unwrap().into_values()).collect();
    let legacy = SqlArguments::from_rows(rows, &memory).unwrap();
    let encode = |args| {
        rtdb_tx::SourceTransaction::new(vec![TxOp::Sql { sql: sql.clone(), args: Some(args) }], Default::default(), &memory)
            .unwrap().encode().unwrap()
    };
    assert_eq!(encode(transaction).as_slice(), encode(legacy).as_slice());
}

#[test]
fn transit_transaction_documents_use_the_row_writer_layout_for_arrow_puts() {
    let memory = MemoryBudget::new(1 << 24);
    let mut body = StructFields::from([
        ("op".into(), Value::Utf8("put-docs".into())),
        ("table".into(), Value::Utf8("docs".into())),
    ]);
    let rows = vec![
        StructFields::from([("_id".into(), Value::I64(1)), ("embedding".into(), Value::list(vec![Value::F32(-0.0), Value::Null, Value::F64(1.0)]))]),
        StructFields::from([("_id".into(), Value::I64(2)), ("embedding".into(), Value::Null)]),
        StructFields::from([("_id".into(), Value::I64(3))]),
    ];
    body.insert("documents".into(), Value::list(rows.iter().cloned().map(Value::Struct).collect()));
    let arrow = operation_value(Value::Struct(body), &memory).unwrap().unwrap();
    let TxOp::WriteArrowDocs { ref documents, write: ArrowDocumentWrite::Put, .. } = arrow else { panic!("expected Arrow documents"); };
    assert_eq!(documents.row_count(), 3);
    let legacy = TxOp::PutDocs { table: TableRef::new("public", "docs"),
        documents: Documents::new(rows, &memory).unwrap(), valid_from: None, valid_to: None };
    let encode = |op| rtdb_tx::SourceTransaction::new(vec![op], Default::default(), &memory).unwrap().encode().unwrap();
    assert_eq!(encode(arrow).as_slice(), encode(legacy).as_slice());
}
