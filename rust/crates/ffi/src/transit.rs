use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::MemoryBudget;
use rtdb_format::transit::{TransitValue as T, encode_json};
use rtdb_node::AutonomousTxOptions;
use rtdb_tx::{Documents, EntityIds, SqlArguments, TxOp, UserMetadata};
use rtdb_types::{Result, TableRef, Timestamp, Value};
use tokio_util::sync::CancellationToken;
use crate::statement::SessionHandle;

pub(crate) fn encode(value: &Value) -> Result<Vec<u8>> { encode_json(&output(value, false)?) }
pub(crate) fn encode_rows(value: &Value) -> Result<Vec<u8>> { encode_json(&output(value, true)?) }
fn output(value: &Value, typed_structs: bool) -> Result<T> {
    let tag = |name: &str, value| T::Tagged { tag: name.into(), representation: Box::new(value) };
    Ok(match value {
        Value::I8(value) => tag("i8", T::Integer(i64::from(*value))), Value::I16(value) => tag("i16", T::Integer(i64::from(*value))), Value::I32(value) => tag("i32", T::Integer(i64::from(*value))),
        Value::F32(value) => tag("f32", T::Float(f64::from(*value))),
        Value::List(values) | Value::FixedSizeList(values) => T::Array(values.iter().map(|value| output(value, typed_structs)).collect::<Result<_>>()?),
        Value::Set(values) => tag("set", T::Array(values.iter().map(|value| output(value, typed_structs)).collect::<Result<_>>()?)),
        Value::Struct(fields) => {
            let fields = T::Map(fields.iter().filter(|(_, value)| !matches!(value, Value::Null | Value::Undefined)).map(|(key, value)| Ok((T::String(key.clone()), output(value, typed_structs)?))).collect::<Result<_>>()?);
            if typed_structs { tag("rtdb/struct", fields) } else { fields }
        }
        Value::Map { entries, .. } => T::Map(entries.iter().map(|(key, value)| Ok((output(key, typed_structs)?, output(value, typed_structs)?))).collect::<Result<_>>()?),
        value => rtdb_pg_codec::to_transit(value)?,
    })
}

pub(crate) fn transaction(session: &Arc<SessionHandle>, token: &CancellationToken, request: Value) -> Result<Value> {
    let memory = session.region.memory();
    let mut request = fields(request)?;
    let asynchronous = match request.remove("asynchronous").unwrap_or(Value::Bool(false)) { Value::Bool(value) => value, _ => return Err(crate::invalid("asynchronous must be boolean")) };
    let mut options = fields(request.remove("options").unwrap_or_else(|| Value::Struct(BTreeMap::new())))?;
    let timezone = optional_string(options.remove("default_timezone"))?;
    let database = optional_string(options.remove("database"))?;
    let system_time = optional_time(options.remove("system_time"), timezone.as_deref().unwrap_or("UTC"))?;
    let user_metadata = options.remove("metadata").filter(|value| !matches!(value, Value::Null)).map(|value| UserMetadata::new(value, memory)).transpose()?;
    if !options.is_empty() { return Err(crate::invalid("Unknown transaction option")); }
    let operations = list(request.remove("operations").ok_or_else(|| crate::invalid("operations is required"))?)?;
    let operations = operations.into_iter().map(|operation| operation_value(operation, memory)).collect::<Result<Vec<_>>>()?;
    if !request.is_empty() { return Err(crate::invalid("Unknown transaction request field")); }
    session.region.run(async {
        let mut state = session.state.lock();
        let options = AutonomousTxOptions { database, default_timezone: timezone, system_time, user_metadata };
        token.run_until_cancelled(async {
            let mut values = BTreeMap::new();
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
fn operation_value(value: Value, memory: &Arc<MemoryBudget>) -> Result<TxOp> {
    let mut value = fields(value)?;
    let kind = string(value.remove("op").ok_or_else(|| crate::invalid("Transaction operation requires op"))?)?;
    if kind == "sql" {
        let sql = string(value.remove("sql").ok_or_else(|| crate::invalid("SQL operation requires sql"))?)?;
        let arguments = value.remove("arguments").filter(|value| !matches!(value, Value::Null));
        let args = arguments.map(|value| list(value)?.into_iter().map(list).collect::<Result<Vec<_>>>()).transpose()?.map(|rows| SqlArguments::from_rows(rows, memory)).transpose()?;
        if !value.is_empty() { return Err(crate::invalid("Unknown SQL operation field")); }
        return Ok(TxOp::Sql { sql, args });
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
            let ids = EntityIds::new(list(value.remove("ids").ok_or_else(|| crate::invalid("Document operation requires ids"))?)?.iter().map(Value::entity_id).collect::<Result<_>>()?, memory)?;
            if kind == "delete-docs" { TxOp::DeleteDocs { table, ids, valid_from, valid_to } } else { TxOp::EraseDocs { table, ids } }
        }
        _ => return Err(crate::invalid("Unknown transaction operation")),
    };
    if !value.is_empty() { return Err(crate::invalid("Unknown document operation field")); }
    Ok(operation)
}
fn fields(value: Value) -> Result<BTreeMap<String, Value>> { match value { Value::Struct(value) => Ok(value), _ => Err(crate::invalid("Expected a Transit map")) } }
fn list(value: Value) -> Result<Vec<Value>> { match value { Value::List(value) => Ok(value), _ => Err(crate::invalid("Expected a Transit array")) } }
fn string(value: Value) -> Result<String> { match value { Value::Utf8(value) => Ok(value), _ => Err(crate::invalid("Expected a Transit string")) } }
fn optional_string(value: Option<Value>) -> Result<Option<String>> { value.filter(|value| !matches!(value, Value::Null)).map(string).transpose() }
fn optional_time(value: Option<Value>, timezone: &str) -> Result<Option<Timestamp>> {
    value.filter(|value| !matches!(value, Value::Null)).map(|value| rtdb_expr::timestamp_micros(&value, timezone).map(|value| Timestamp(value.0))).transpose()
}
