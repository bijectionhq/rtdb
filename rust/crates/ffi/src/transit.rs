use rtdb_types::StructFields;
use std::sync::Arc;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::transit::{TransitValue as T, encode_json};
use rtdb_node::AutonomousTxOptions;
use rtdb_tx::{Documents, SqlArguments, TxOp, UserMetadata};
use rtdb_types::{Result, TableRef, Timestamp, Value};
use tokio_util::sync::CancellationToken;
use crate::statement::SessionHandle;

pub(crate) fn encode(value: &Value) -> Result<Vec<u8>> { encode_json(&output(value, false)?) }
/// One row of a cursor page's Transit JSON array; the cursor encodes and charges a page one row at a time.
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

pub(crate) struct TransactionInput {
    value: Value,
    reservation: Arc<MemoryReservation>,
}

impl TransactionInput {
    pub(crate) fn decode(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let (value, reservation) = rtdb_pg_codec::decode_transit_msgpack(bytes, memory)?.into_parts();
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
    let operations = operations.into_iter().filter_map(|operation| operation_value(operation, memory).transpose()).collect::<Result<Vec<_>>>()?;
    if !request.is_empty() { return Err(crate::invalid("Unknown transaction request field")); }
    session.region.run(async {
        let mut state = session.state.lock();
        let options = AutonomousTxOptions { database, default_timezone: timezone, system_time, user_metadata };
        token.run_until_cancelled(async {
            let mut values = StructFields::new();
            if asynchronous {
                let submitted = state.state_mut().submit_tx(operations, options).await?;
                values.insert("tx_id".into(), Value::I64(submitted.tx_id.0));
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
        let rows = arguments.map(|value| list(value)?.into_iter().map(list).collect::<Result<Vec<_>>>()).transpose()?;
        if !value.is_empty() { return Err(crate::invalid("Unknown SQL operation field")); }
        if rows.as_ref().is_some_and(Vec::is_empty) { return Ok(None); }
        let args = rows.map(|rows| SqlArguments::from_rows(rows, memory)).transpose()?;
        return Ok(Some(TxOp::Sql { sql, args }));
    }
    let schema = optional_string(value.remove("schema"))?.unwrap_or_else(|| "public".into());
    let table = TableRef::new(schema, string(value.remove("table").ok_or_else(|| crate::invalid("Document operation requires table"))?)?);
    let valid_from = optional_time(value.remove("valid_from"), "UTC")?;
    let valid_to = optional_time(value.remove("valid_to"), "UTC")?;
    if kind == "erase-docs" && (valid_from.is_some() || valid_to.is_some()) { return Err(crate::invalid("Erase operations do not accept valid-time bounds")); }
    let operation = match kind.as_str() {
        "put-docs" | "patch-docs" => {
            let documents = Documents::new(list(value.remove("documents").ok_or_else(|| crate::invalid("Document operation requires documents"))?)?.into_iter().map(fields).collect::<Result<_>>()?, memory)?;
            if kind == "put-docs" { TxOp::PutDocs { table, documents, valid_from, valid_to } } else { TxOp::PatchDocs { table, documents, valid_from, valid_to } }
        }
        "delete-docs" | "erase-docs" => {
            let ids = list(value.remove("ids").ok_or_else(|| crate::invalid("Document operation requires ids"))?)?;
            if !value.is_empty() { return Err(crate::invalid("Unknown document operation field")); }
            for id in &ids { id.entity_id()?; }
            if ids.is_empty() { return Ok(None); }
            let name = format!("{}.{}", quoted(&table.schema), quoted(&table.table));
            let bounded = valid_from.is_some() || valid_to.is_some();
            let sql = if kind == "erase-docs" { format!("ERASE FROM {name} WHERE _id = ?") }
                else if bounded { format!("DELETE FROM {name} FOR VALID_TIME FROM ? TO ? WHERE _id = ?") }
                else { format!("DELETE FROM {name} WHERE _id = ?") };
            let time = |time: Option<Timestamp>| time.map_or(Value::Null, |time| Value::Timestamp { value: time.0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) });
            let rows = ids.into_iter().map(|id| if bounded && kind == "delete-docs" { vec![time(valid_from), time(valid_to), id] } else { vec![id] }).collect();
            return Ok(Some(TxOp::Sql { sql, args: Some(SqlArguments::from_rows(rows, memory)?) }));
        }
        _ => return Err(crate::invalid("Unknown transaction operation")),
    };
    if !value.is_empty() { return Err(crate::invalid("Unknown document operation field")); }
    Ok(Some(operation))
}
fn quoted(identifier: &str) -> String { format!("\"{}\"", identifier.replace('"', "\"\"")) }
fn fields(value: Value) -> Result<StructFields<Value>> { match value { Value::Struct(value) => Ok(value), _ => Err(crate::invalid("Expected a Transit map")) } }
fn list(value: Value) -> Result<Vec<Value>> { match value { Value::List(value) => Ok(value.into_values()), _ => Err(crate::invalid("Expected a Transit array")) } }
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
fn deletes_and_erases_are_submitted_as_sql_dml() {
    let memory = MemoryBudget::new(1 << 20);
    let time = Value::Timestamp { value: 1_000, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    let op = |kind: &str, ids: Vec<Value>, bounds: bool| {
        let mut fields = StructFields::from([("op".into(), Value::Utf8(kind.into())), ("table".into(), Value::Utf8("docs".into())), ("ids".into(), Value::list(ids))]);
        if bounds { fields.insert("valid_from".into(), time.clone()); }
        operation_value(Value::Struct(fields), &memory).unwrap()
    };
    let Some(TxOp::Sql { sql, args: Some(args) }) = op("delete-docs", vec![Value::I64(1)], true) else { panic!("expected SQL") };
    assert_eq!(sql, "DELETE FROM \"public\".\"docs\" FOR VALID_TIME FROM ? TO ? WHERE _id = ?");
    assert_eq!(args.rows().collect::<Vec<_>>(), [&[time.clone(), Value::Null, Value::I64(1)][..]]);
    let Some(TxOp::Sql { sql, .. }) = op("delete-docs", vec![Value::I64(1)], false) else { panic!("expected SQL") };
    assert_eq!(sql, "DELETE FROM \"public\".\"docs\" WHERE _id = ?");
    let Some(TxOp::Sql { sql, .. }) = op("erase-docs", vec![Value::I64(1)], false) else { panic!("expected SQL") };
    assert_eq!(sql, "ERASE FROM \"public\".\"docs\" WHERE _id = ?");
    assert!(op("delete-docs", vec![], false).is_none());
}
