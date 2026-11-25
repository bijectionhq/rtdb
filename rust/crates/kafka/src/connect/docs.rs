use std::{borrow::Cow, sync::Arc};
use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::data::{StoredEvent, StoredOperation};
use rtdb_replication::TermHandle;
use rtdb_tx::{OpenTransaction, TransactionWriter, WriterResult};
use rtdb_types::{Interval, ListValue, Result, StructFields, TableRef, Timestamp, Value};
use super::{ConnectSchema, ResumeToken, SchemaKind, SinkRecord, error};

const DOCUMENT_NODE_BYTES: usize = 4096;
const MAX_DOCUMENT_DEPTH: usize = 128;

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
        let memory = record.reservations.first().map(|charge| Arc::clone(charge.budget()));
        let conversion = DocumentConversion { memory: memory.as_ref() };
        if let Some(memory) = &memory {
            let growth = conversion.growth(&record.value.value, record.value.schema.as_deref(), 0)?;
            // The source leases already cover Arrow payloads moved into the document.
            let bound = record.retained_bytes()?.checked_sub(growth.reused_payload)
                .and_then(|bytes| bytes.checked_mul(4))
                .and_then(|bytes| bytes.checked_add(growth.scratch))
                .ok_or_else(document_overflow)?;
            record.reservations.push(memory.reserve(bound)?);
        }
        let id = conversion.resolve_id(&record)?;
        let iid = id.entity_id()?.iid();
        let operation = if record.value.value == Value::Null { StoredOperation::Delete } else {
            let value = conversion.convert_with_schema(std::mem::replace(&mut record.value.value, Value::Null), record.value.schema.as_deref(), 0).map_err(|cause| record.context(cause))?;
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
fn convert_document(value: Value) -> Result<Value> {
    Ok(match value {
        Value::Struct(fields) => Value::Struct(fields.into_iter().map(|(key, value)| Ok((key, convert_document(value)?))).collect::<Result<_>>()?),
        Value::Map { entries, .. } => {
            let mut fields = StructFields::new();
            for (key, value) in entries { fields.insert(map_key(key)?, convert_document(value)?); }
            Value::Struct(fields)
        }
        Value::List(values) if retains_array(&values, None) => Value::List(values),
        Value::List(values) => Value::List(rtdb_types::ListValue::try_from_cells(values.into_values().into_iter().map(convert_document))?),
        Value::Timestamp { value, unit: rtdb_types::TimeUnit::Millisecond, timezone } => Value::Timestamp {
            value: value.checked_mul(1000).ok_or_else(|| error("conversion-error", "Timestamp exceeds the document writer's microsecond range"))?, unit: rtdb_types::TimeUnit::Microsecond, timezone,
        },
        Value::Time { value, unit: rtdb_types::TimeUnit::Millisecond, .. } => Value::Time {
            value: value.checked_mul(1_000_000).ok_or_else(|| error("conversion-error", "Time exceeds the document writer's nanosecond range"))?, unit: rtdb_types::TimeUnit::Nanosecond, bit_width: 64,
        },
        value => value,
    })
}
#[cfg(test)]
pub(super) fn convert_with_schema(value: Value, schema: Option<&ConnectSchema>) -> Result<Value> {
    DocumentConversion { memory: None }.convert_with_schema(value, schema, 0)
}

fn logical_date_name(schema: &ConnectSchema) -> Option<&str> {
    schema.name.as_deref().filter(|name| matches!(*name,
        "org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp"))
}

fn retains_array(values: &ListValue, element: Option<(&ConnectSchema, usize)>) -> bool {
    values.array().is_some()
        && element.is_none_or(|(element, depth)| depth < MAX_DOCUMENT_DEPTH && logical_date_name(element).is_none())
}

fn document_overflow() -> rtdb_types::Anomaly {
    error("allocation-overflow", "Document allocation overflow")
}

struct DocumentGrowth {
    scratch: usize,
    reused_payload: usize,
}

impl DocumentGrowth {
    fn node() -> Self {
        Self { scratch: DOCUMENT_NODE_BYTES, reused_payload: 0 }
    }

    fn include_child(&mut self, child: Self) -> Result<()> {
        self.scratch = self.scratch.checked_add(DOCUMENT_NODE_BYTES)
            .and_then(|bytes| bytes.checked_add(child.scratch))
            .ok_or_else(document_overflow)?;
        self.reused_payload = self.reused_payload.checked_add(child.reused_payload)
            .ok_or_else(document_overflow)?;
        Ok(())
    }
}

struct DocumentConversion<'a> {
    memory: Option<&'a Arc<MemoryBudget>>,
}

impl DocumentConversion<'_> {
    fn schema_view<'s>(&self, schema: Option<&'s ConnectSchema>) -> Result<(Option<Arc<MemoryReservation>>, Option<Cow<'s, ConnectSchema>>)> {
        let lease = match (self.memory, schema) {
            (Some(memory), Some(schema)) if matches!(schema.kind, SchemaKind::Reference(_)) =>
                Some(memory.reserve(schema.view_allocation_bound())?),
            _ => None,
        };
        Ok((lease, schema.map(ConnectSchema::resolved)))
    }

    fn resolve_id(&self, record: &SinkRecord) -> Result<Value> {
        let id = if let (Some(schema), Value::Struct(fields)) = (&record.key.schema, &record.key.value) {
            let (_lease, schema) = self.schema_view(Some(schema))?;
            match schema.as_deref().map(|schema| &schema.kind) {
                Some(SchemaKind::Struct(schema_fields)) if schema_fields.len() == 1 => {
                    let field = &schema_fields[0];
                    fields.get(&field.name).filter(|value| **value != Value::Null).cloned()
                        .or_else(|| field.schema.default.clone()).unwrap_or(Value::Null)
                }
                _ => record.key.value.clone(),
            }
        } else { record.key.value.clone() };
        if id == Value::Null { return Err(record.context(error("docs-no-key", "Record has no usable key"))); }
        id.entity_id().map_err(|_| record.context(error("docs-invalid-key", "Record key cannot be used as _id")).with_context("keyType", format!("{:?}", id.logical_type()).into()))?;
        Ok(id)
    }

    fn convert_with_schema(&self, value: Value, schema: Option<&ConnectSchema>, depth: usize) -> Result<Value> {
        if depth > MAX_DOCUMENT_DEPTH { return Err(error("conversion-error", "Connect document nesting limit exceeded")); }
        if value == Value::Null { return Ok(Value::Null); }
        if let Some(name) = schema.and_then(logical_date_name) {
            let millis = super::data::date_millis(&value).ok_or_else(|| error("conversion-error", "Logical date value is not a java.util.Date representation"))?;
            return Ok(match name {
                "org.apache.kafka.connect.data.Date" => Value::calendar_date(rtdb_types::CivilDate::from_epoch_day(millis.div_euclid(86_400_000))?, rtdb_types::DateUnit::Day),
                "org.apache.kafka.connect.data.Time" => Value::Time { value: millis.rem_euclid(86_400_000) * 1_000_000, unit: rtdb_types::TimeUnit::Nanosecond, bit_width: 64 },
                _ => return convert_document(super::data::date_value(millis)),
            });
        }
        let (_lease, schema) = self.schema_view(schema)?;
        match (value, schema.as_deref().map(|schema| &schema.kind)) {
            (Value::Struct(mut values), Some(SchemaKind::Struct(fields))) => {
                let mut result = StructFields::new();
                for field in fields {
                    let value = values.remove(&field.name).filter(|value| *value != Value::Null)
                        .or_else(|| field.schema.default.clone()).unwrap_or(Value::Null);
                    result.insert(field.name.clone(), self.convert_with_schema(value, Some(&field.schema), depth + 1)?);
                }
                Ok(Value::Struct(result))
            }
            (Value::List(values), Some(SchemaKind::Array(element))) => {
                if retains_array(&values, Some((element, depth))) {
                    return Ok(Value::List(values));
                }
                let convert = |value| self.convert_with_schema(value, Some(element), depth + 1);
                let converted = if values.array().is_some() {
                    ListValue::try_from_cells(values.iter().map(|value| convert(value.into_owned())))?
                } else {
                    ListValue::try_from_cells(values.into_values().into_iter().map(convert))?
                };
                Ok(Value::List(converted))
            }
            (Value::Map { entries, .. }, Some(SchemaKind::Map(_, value_schema))) => {
                let mut result = StructFields::new();
                for (key, value) in entries {
                    result.insert(map_key(key)?, self.convert_with_schema(value, Some(value_schema), depth + 1)?);
                }
                Ok(Value::Struct(result))
            }
            (value, _) => convert_document(value),
        }
    }

    fn growth(&self, value: &Value, schema: Option<&ConnectSchema>, depth: usize) -> Result<DocumentGrowth> {
        let mut growth = DocumentGrowth::node();
        if *value == Value::Null || schema.is_some_and(|schema| depth > MAX_DOCUMENT_DEPTH || logical_date_name(schema).is_some()) {
            return Ok(growth);
        }
        let (_lease, schema) = self.schema_view(schema)?;
        let kind = schema.as_deref().map(|schema| &schema.kind);
        match (value, kind) {
            (Value::Struct(values), Some(SchemaKind::Struct(fields))) => {
                let _seen_lease = self.memory.map(|memory| memory.reserve(values.len())).transpose()?;
                let mut seen = vec![false; values.len()];
                for field in fields {
                    // Conversion removes the source field on its first occurrence.
                    let value = values.get_full(&field.name).and_then(|(index, _, value)| {
                        let already_seen = std::mem::replace(&mut seen[index], true);
                        (!already_seen && *value != Value::Null).then_some(value)
                    });
                    let schema = Some(field.schema.as_ref());
                    let mut child = if let Some(value) = value {
                        self.growth(value, schema, depth + 1)?
                    } else if let Some(value) = field.schema.default_value() {
                        let mut child = self.growth(value, schema, depth + 1)?;
                        let copied = value.retained_bytes()?.checked_sub(child.reused_payload).ok_or_else(document_overflow)?;
                        child.scratch = child.scratch.checked_add(copied).ok_or_else(document_overflow)?;
                        child.reused_payload = 0;
                        child
                    } else {
                        DocumentGrowth::node()
                    };
                    child.scratch = child.scratch.checked_add(field.name.len()).ok_or_else(document_overflow)?;
                    growth.include_child(child)?;
                }
            }
            (Value::Struct(fields), _) => {
                for value in fields.values() {
                    growth.include_child(self.growth(value, None, depth + 1)?)?;
                }
            }
            (Value::Map { entries, .. }, kind) => {
                let element = match kind {
                    Some(SchemaKind::Map(_, element)) => Some(element.as_ref()),
                    _ => None,
                };
                for (_, value) in entries {
                    growth.include_child(self.growth(value, element, depth + 1)?)?;
                }
            }
            (Value::List(values), kind) => {
                let element = match kind {
                    Some(SchemaKind::Array(element)) => Some(element.as_ref()),
                    _ => None,
                };
                if retains_array(values, element.map(|element| (element, depth))) {
                    growth.reused_payload = values.retained_payload_bytes()?;
                } else if values.array().is_some() {
                    growth.scratch = values.len().checked_mul(2 * DOCUMENT_NODE_BYTES)
                        .and_then(|bytes| bytes.checked_add(DOCUMENT_NODE_BYTES))
                        .ok_or_else(document_overflow)?;
                } else {
                    for value in values.iter() {
                        growth.include_child(self.growth(value.as_ref(), element, depth + 1)?)?;
                    }
                }
            }
            _ => (),
        }
        Ok(growth)
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
        transaction.append(self.table.clone(), StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid: self.iid, system_from, valid_time: Interval::new(system_from, Timestamp::MAX)?, operation, recency: None })?;
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
        let conversion = DocumentConversion { memory: None };
        let key = ConnectData { schema: Some(Arc::new(ConnectSchema::new(SchemaKind::Struct(vec![ConnectField { name: "key".into(), schema: Arc::new(ConnectSchema::new(SchemaKind::I64)) }])))), value: Value::Struct(StructFields::from([("key".into(), Value::I64(8))])) };
        assert_eq!(conversion.resolve_id(&record(key, Value::Null)).unwrap(), Value::I64(8));
        assert!(conversion.resolve_id(&record(ConnectData::schemaless(Value::Struct(StructFields::from([("key".into(), Value::I64(8))]))), Value::Null)).unwrap_err().code.ends_with("docs-invalid-key"));
        assert!(conversion.resolve_id(&record(ConnectData::null(), Value::Struct(StructFields::from([("_id".into(), Value::I64(8))])))).unwrap_err().code.ends_with("docs-no-key"));
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
            assert_eq!(date.admit_object_dates().unwrap_err().code, "xtdb/date-out-of-range");
            assert!(convert_with_schema(super::super::data::date_value(millis),Some(&logical("Timestamp"))).is_err());
        }
    }

    #[test]
    fn document_admission_keeps_packed_float_buffers_with_constant_scratch() {
        use arrow_array::Float32Array;

        let array: Float32Array = [Some(-0.0_f32), None, Some(f32::from_bits(0x7fc1_2345))]
            .into_iter().collect();
        let source = array.values().as_ptr();
        let list = ListValue::from_f32(array);
        let payload = list.retained_payload_bytes().unwrap();
        let mut element = ConnectSchema::new(SchemaKind::F32);
        element.optional = true;
        let schema = ConnectSchema::new(SchemaKind::Array(Arc::new(element)));
        let conversion = DocumentConversion { memory: None };
        for schema in [Some(&schema), None] {
            let value = Value::List(list.clone());
            let growth = conversion.growth(&value, schema, 0).unwrap();
            assert_eq!(growth.scratch, DOCUMENT_NODE_BYTES);
            assert_eq!(growth.reused_payload, payload);
            let Value::List(list) = convert_with_schema(value, schema).unwrap() else { panic!("expected Arrow list"); };
            let array = list.array().unwrap().as_any().downcast_ref::<Float32Array>().unwrap();
            assert_eq!(source, array.values().as_ptr());
        }
    }

    #[test]
    fn nested_logical_list_default_keeps_materialization_scratch() {
        use arrow_array::{Array, TimestampMillisecondArray};
        use arrow_schema::Field;

        let array = TimestampMillisecondArray::from(vec![Some(-1), None, Some(86_400_000)]);
        let field = Arc::new(Field::new("value", array.data_type().clone(), true));
        let list = ListValue::from_scalar_arrow(field, Arc::new(array)).unwrap();
        let mut element = logical("Date");
        element.optional = true;
        let mut dates = ConnectSchema::new(SchemaKind::Array(Arc::new(element)));
        dates.default = Some(Value::List(list.clone()));
        let schema = ConnectSchema::new(SchemaKind::Struct(vec![ConnectField {
            name: "dates".into(), schema: Arc::new(dates),
        }]));
        let value = Value::Struct(StructFields::from([("dates".into(), Value::Null)]));

        let conversion = DocumentConversion { memory: None };
        let growth = conversion.growth(&value, Some(&schema), 0).unwrap();
        let default_bytes = Value::List(list).retained_bytes().unwrap();
        assert_eq!(growth.scratch, 9 * DOCUMENT_NODE_BYTES + "dates".len() + default_bytes);
        assert_eq!(growth.reused_payload, 0);
        let Value::Struct(document) = convert_with_schema(value, Some(&schema)).unwrap() else { panic!("expected document"); };
        let Value::List(dates) = document.get("dates").unwrap() else { panic!("expected dates"); };
        assert!(dates.array().is_none());
        assert_eq!(dates.get(0).unwrap().date_days(), Some(-1));
        assert_eq!(dates.get(1).unwrap().as_ref(), &Value::Null);
        assert_eq!(dates.get(2).unwrap().date_days(), Some(1));
    }

    #[test]
    fn writer_admits_graph_backed_packed_lists_and_releases_schema_view_scratch() {
        use crate::connect::schema_graph::{GraphBuilder, Node, NodeKind, Scalar};
        use arrow_array::Float32Array;

        let mut graph = GraphBuilder::new();
        let element = graph.insert(Node { kind: NodeKind::Scalar(Scalar::F32), metadata: Default::default() });
        let list = graph.insert(Node { kind: NodeKind::Array(element), metadata: Default::default() });
        let root = graph.insert(Node {
            kind: NodeKind::Struct(vec![("scores".into(), list)]), metadata: Default::default(),
        });
        let schema = graph.finish(root).unwrap();
        let view_bytes = schema.view_allocation_bound();
        assert!(view_bytes > 0);

        let array = Float32Array::from(vec![1.25; 1024]);
        let list = ListValue::from_f32(array);
        let payload = list.retained_payload_bytes().unwrap();
        let mut record = record(ConnectData::schemaless(Value::I64(8)),
            Value::Struct(StructFields::from([("scores".into(), Value::List(list))])));
        record.value.schema = Some(schema);
        let source_bytes = record.retained_bytes().unwrap();
        let writer_bytes = (source_bytes - payload) * 4 + 3 * DOCUMENT_NODE_BYTES + "scores".len();
        let memory = MemoryBudget::new(source_bytes + writer_bytes + view_bytes);
        record.reservations.push(memory.reserve(source_bytes).unwrap());

        let writer = DocsIndexer::new(TableRef::parse("events").unwrap()).writer(record).unwrap();
        assert_eq!(memory.used_bytes(), source_bytes + writer_bytes);
        assert_eq!(memory.peak_bytes(), source_bytes + writer_bytes + view_bytes);
        drop(writer);
        assert_eq!(memory.used_bytes(), 0);
    }
}
