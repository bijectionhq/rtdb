use rtdb_types::StructFields;
use std::sync::Arc;
use async_trait::async_trait;
use rtdb_columnar::MemoryReservation;
use rtdb_format::data::{StoredEvent, StoredOperation};
use rtdb_replication::TermHandle;
use rtdb_tx::{OpenTransaction, TransactionWriter, WriterResult};
use rtdb_types::{Interval, Result, TableRef, Timestamp, Value};
use super::{ResumeToken, SchemaKind, SinkRecord, error};

pub struct SourceTransactions<'a> { pub(crate) term: &'a TermHandle }
impl SourceTransactions<'_> {
    /// Resolves and waits for admitted read-back before returning; offsets are never committed to Kafka.
    pub async fn submit(&mut self, writer: Box<dyn TransactionWriter>, timestamp_millis: Option<i64>, token: ResumeToken) -> Result<()> {
        let timestamp = timestamp_millis.map(|value| value.checked_mul(1000).map(Timestamp).ok_or_else(|| error("invalid-timestamp", "Kafka timestamp exceeds microsecond range"))).transpose()?;
        self.term.resolve_external(writer, timestamp, Some(token.encode())).await?;
        Ok(())
    }
}
#[async_trait]
pub trait RecordIndexer: Send {
    async fn index_records(&mut self, records: Vec<SinkRecord>, transactions: &mut SourceTransactions<'_>) -> Result<()>;
}
pub struct DocsIndexer { table: TableRef }
impl DocsIndexer {
    pub fn new(table: TableRef) -> Self { Self { table } }
    pub fn writer(&self, mut record: SinkRecord) -> Result<Box<dyn TransactionWriter>> {
        if let Some(reservation) = record.reservations.first() {
            let growth = document_growth(&record.value.value)?;
            let views = record.value.schema.as_ref().map_or(0, |schema| schema.view_allocation_bound());
            let bound = record.retained_bytes()?.checked_mul(4).and_then(|size| size.checked_add(growth)).and_then(|size| size.checked_add((growth / 4096).checked_mul(views)?)).ok_or_else(|| error("allocation-overflow", "Document allocation overflow"))?;
            let charge = reservation.budget().reserve(bound)?; record.reservations.push(charge);
        }
        let id = resolve_id(&record)?;
        let iid = id.entity_id()?.iid();
        let operation = if record.value.value == Value::Null { StoredOperation::Delete } else {
            let value = convert_with_schema(std::mem::replace(&mut record.value.value, Value::Null), record.value.schema.as_deref()).map_err(|cause| record.context(cause))?;
            let Value::Struct(mut fields) = value else { return Err(record.context(error("docs-non-struct-value", "Expected Struct or Map at top level"))); };
            fields.insert("_id".into(), id);
            StoredOperation::Put(fields)
        };
        Ok(Box::new(DocWriter { table: self.table.clone(), iid, operation: Some(operation), _reservations: record.reservations }))
    }
}
#[async_trait]
impl RecordIndexer for DocsIndexer {
    async fn index_records(&mut self, records: Vec<SinkRecord>, transactions: &mut SourceTransactions<'_>) -> Result<()> {
        for record in records {
            let (timestamp, token) = (record.timestamp_millis, ResumeToken(record.offset));
            let writer = self.writer(record)?;
            transactions.submit(writer, timestamp, token).await?;
        }
        Ok(())
    }
}
pub fn resolve_id(record: &SinkRecord) -> Result<Value> {
    let id = if let (Some(schema), Value::Struct(fields)) = (&record.key.schema, &record.key.value) {
        let schema = schema.resolved();
        if let SchemaKind::Struct(schema_fields) = &schema.kind {
            if schema_fields.len() == 1 {
                fields.get(&schema_fields[0].name).filter(|value| **value != Value::Null).cloned().or_else(|| schema_fields[0].schema.default.clone()).unwrap_or(Value::Null)
            } else { record.key.value.clone() }
        } else { record.key.value.clone() }
    } else { record.key.value.clone() };
    if id == Value::Null { return Err(record.context(error("docs-no-key", "Record has no usable key"))); }
    id.entity_id().map_err(|_| record.context(error("docs-invalid-key", "Record key cannot be used as _id")).with_context("keyType", format!("{:?}", id.logical_type()).into()))?;
    Ok(id)
}
fn convert_document(value: Value) -> Result<Value> {
    Ok(match value {
        Value::Struct(fields) => Value::Struct(fields.into_iter().map(|(key, value)| Ok((key, convert_document(value)?))).collect::<Result<_>>()?),
        Value::Map { entries, .. } => {
            let mut fields = StructFields::new();
            for (key, value) in entries { fields.insert(map_key(key)?, convert_document(value)?); }
            Value::Struct(fields)
        }
        Value::List(values) => Value::list(values.into_values().into_iter().map(convert_document).collect::<Result<_>>()?),
        Value::Timestamp { value, unit: rtdb_types::TimeUnit::Millisecond, timezone } => Value::Timestamp {
            value: value.checked_mul(1000).ok_or_else(|| error("conversion-error", "Timestamp exceeds the document writer's microsecond range"))?, unit: rtdb_types::TimeUnit::Microsecond, timezone,
        },
        Value::Time { value, unit: rtdb_types::TimeUnit::Millisecond, .. } => Value::Time {
            value: value.checked_mul(1_000_000).ok_or_else(|| error("conversion-error", "Time exceeds the document writer's nanosecond range"))?, unit: rtdb_types::TimeUnit::Nanosecond, bit_width: 64,
        },
        value => value,
    })
}
fn convert_with_schema(value: Value, schema: Option<&super::ConnectSchema>) -> Result<Value> { convert_schema_value(value, schema, 0) }
fn convert_schema_value(value: Value, schema: Option<&super::ConnectSchema>, depth: usize) -> Result<Value> {
    if depth > 128 { return Err(error("conversion-error", "Connect document nesting limit exceeded")); }
    if value == Value::Null { return Ok(Value::Null); }
    let schema = schema.map(super::ConnectSchema::resolved);
    let schema = schema.as_deref();
    if let Some(name) = schema.and_then(|schema|schema.name.as_deref()) {
        if matches!(name,"org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp") {
            let millis = super::data::date_millis(&value).ok_or_else(||error("conversion-error","Logical date value is not a java.util.Date representation"))?;
            return Ok(match name {
                "org.apache.kafka.connect.data.Date" => Value::calendar_date(rtdb_types::CivilDate::from_epoch_day(millis.div_euclid(86_400_000))?,rtdb_types::DateUnit::Day),
                "org.apache.kafka.connect.data.Time" => Value::Time { value: millis.rem_euclid(86_400_000) * 1_000_000,unit: rtdb_types::TimeUnit::Nanosecond,bit_width: 64 },
                _ => return convert_document(super::data::date_value(millis)),
            });
        }
    }
    match (value, schema.map(|schema| &schema.kind)) {
        (Value::Struct(mut values), Some(SchemaKind::Struct(fields))) => {
            let mut result = StructFields::new();
            for field in fields {
                let value = values.remove(&field.name).filter(|value| *value != Value::Null).or_else(|| field.schema.default.clone()).unwrap_or(Value::Null);
                result.insert(field.name.clone(), convert_schema_value(value, Some(&field.schema), depth + 1)?);
            }
            Ok(Value::Struct(result))
        }
        (Value::List(values), Some(SchemaKind::Array(element))) => Ok(Value::list(values.into_values().into_iter().map(|value| convert_schema_value(value, Some(element), depth + 1)).collect::<Result<_>>()?)),
        (Value::Map { entries, .. }, Some(SchemaKind::Map(_, value_schema))) => {
            let mut result = StructFields::new();
            for (key, value) in entries { result.insert(map_key(key)?, convert_schema_value(value, Some(value_schema), depth + 1)?); }
            Ok(Value::Struct(result))
        }
        (value, _) => convert_document(value),
    }
}
fn map_key(value: Value) -> Result<String> {
    Ok(match value { Value::Null => "null".into(), Value::Utf8(value) => value, Value::Bool(value) => value.to_string(),
        Value::I8(value) => value.to_string(), Value::I16(value) => value.to_string(), Value::I32(value) => value.to_string(), Value::I64(value) => value.to_string(),
        Value::F32(value) => value.to_string(), Value::F64(value) => value.to_string(),
        _ => return Err(error("conversion-error", "Connect map key has no native Java string representation")),
    })
}
struct DocWriter { table: TableRef, iid: rtdb_types::Iid, operation: Option<StoredOperation>, _reservations: Vec<Arc<MemoryReservation>> }
#[async_trait]
impl TransactionWriter for DocWriter {
    async fn write(&mut self, transaction: &mut OpenTransaction<'_>) -> Result<WriterResult> {
        let system_from = transaction.tx_key().system_time;
        let operation = self.operation.take().ok_or_else(|| error("indexer-reused", "Document writer was already resolved"))?;
        transaction.append(self.table.clone(), StoredEvent { iid: self.iid, system_from, valid_time: Interval::new(system_from, Timestamp::MAX)?, operation, recency: None })?;
        Ok(WriterResult::Committed { user_metadata: None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::{ConnectData, ConnectField, ConnectSchema};
    fn record(key: ConnectData, value: Value) -> SinkRecord { SinkRecord::new("events".into(), 0, 2, key, ConnectData::schemaless(value)) }
    #[test]
    fn id_comes_only_from_key_and_single_field_struct_unwraps() {
        let key = ConnectData { schema: Some(Arc::new(ConnectSchema::new(SchemaKind::Struct(vec![ConnectField { name: "key".into(), schema: Arc::new(ConnectSchema::new(SchemaKind::I64)) }])))), value: Value::Struct(StructFields::from([("key".into(), Value::I64(8))])) };
        assert_eq!(resolve_id(&record(key, Value::Null)).unwrap(), Value::I64(8));
        assert!(resolve_id(&record(ConnectData::schemaless(Value::Struct(StructFields::from([("key".into(), Value::I64(8))]))), Value::Null)).unwrap_err().code.ends_with("docs-invalid-key"));
        assert!(resolve_id(&record(ConnectData::null(), Value::Struct(StructFields::from([("_id".into(), Value::I64(8))])))).unwrap_err().code.ends_with("docs-no-key"));
    }
    #[test]
    fn docs_reject_scalar_value_and_accept_tombstones_before_submission() {
        let indexer = DocsIndexer::new(TableRef::parse("events").unwrap());
        assert!(indexer.writer(record(ConnectData::schemaless(Value::I64(8)), Value::I64(9))).is_err());
        assert!(indexer.writer(record(ConnectData::schemaless(Value::I64(8)), Value::Null)).is_ok());
        assert_eq!(convert_document(Value::Map { entries: vec![(Value::I32(7), Value::Bool(true))], sorted: false }).unwrap(), Value::Struct(StructFields::from([("7".into(), Value::Bool(true))])));
    }
    fn logical(name: &str) -> super::super::ConnectSchema {
        let mut schema = super::super::ConnectSchema::new(if name == "Timestamp" { SchemaKind::I64 } else { SchemaKind::I32 });
        schema.name = Some(format!("org.apache.kafka.connect.data.{name}"));schema
    }
    #[test]
    fn logical_dates_are_projected_by_schema_only_at_document_admission() {
        let raw = super::super::data::date_value(-1);
        assert_eq!(convert_with_schema(raw.clone(),Some(&logical("Date"))).unwrap(),Value::DateDay(-1));
        assert_eq!(convert_with_schema(raw.clone(),Some(&logical("Time"))).unwrap(),Value::Time { value: 86_399_999_000_000,unit: rtdb_types::TimeUnit::Nanosecond,bit_width: 64 });
        assert_eq!(convert_with_schema(raw.clone(),Some(&logical("Timestamp"))).unwrap(),Value::Timestamp { value: -1000,unit: rtdb_types::TimeUnit::Microsecond,timezone: Some("UTC".into()) });
        assert_eq!(convert_with_schema(raw,None).unwrap(),Value::Timestamp { value: -1000,unit: rtdb_types::TimeUnit::Microsecond,timezone: Some("UTC".into()) });
        let end = super::super::data::date_value(86_400_000);
        assert_eq!(convert_with_schema(end,Some(&logical("Time"))).unwrap(),Value::Time { value: 0,unit: rtdb_types::TimeUnit::Nanosecond,bit_width: 64 });
    }
    #[test]
    fn logical_date_document_conversion_preserves_full_days_before_the_object_writer() {
        for millis in [i64::MIN,i64::MAX] {
            let mut date = convert_with_schema(super::super::data::date_value(millis),Some(&logical("Date"))).unwrap();
            assert_eq!(date.date_days(),Some(millis.div_euclid(86_400_000)));
            date.admit_object_dates();
            assert_eq!(date,Value::DateDay(millis.div_euclid(86_400_000) as i32));
            assert!(convert_with_schema(super::super::data::date_value(millis),Some(&logical("Timestamp"))).is_err());
        }
    }
}

fn document_growth(value: &Value) -> Result<usize> {
    let add = |bytes: usize, value: &Value| bytes.checked_add(4096).and_then(|bytes| bytes.checked_add(document_growth(value).ok()?)).ok_or_else(|| error("allocation-overflow", "Document allocation overflow"));
    match value {
        Value::Struct(fields) => fields.values().try_fold(4096usize, add),
        Value::Map { entries, .. } => entries.iter().map(|(_, value)| value).try_fold(4096usize, add),
        Value::List(values) => values.iter().try_fold(4096usize, |bytes, value| add(bytes, value.as_ref())),
        _ => Ok(4096),
    }
}
