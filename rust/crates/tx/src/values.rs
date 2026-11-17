use std::collections::BTreeMap;
use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, EntityId, LogicalType, Result, TableRef, Timestamp, TransactionKey, TxId, Value};

#[derive(Clone, Debug)]
pub struct RetainedBytes { data: Arc<Vec<u8>>, _reservation: Arc<MemoryReservation> }
impl RetainedBytes {
    /// Adopts an existing allocation; producers of large payloads must reserve before allocating.
    pub fn new(data: Vec<u8>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(data.capacity().checked_add(128).ok_or_else(overflow)?)?;
        Ok(Self { data: Arc::new(data), _reservation: reservation })
    }
    pub fn as_slice(&self) -> &[u8] { &self.data }
    pub fn into_bytes(self) -> bytes::Bytes { bytes::Bytes::from_owner(self) }
    pub fn with_reservation(data: Vec<u8>, reservation: Arc<MemoryReservation>) -> Result<Self> {
        if data.capacity() > reservation.bytes() { return Err(Anomaly::fault("rtdb.tx/byte-reservation", "Encoded payload exceeds its reservation")); }
        Ok(Self { data: Arc::new(data), _reservation: reservation })
    }
}
impl AsRef<[u8]> for RetainedBytes { fn as_ref(&self) -> &[u8] { self.as_slice() } }

#[derive(Clone, Debug)]
pub struct Documents { rows: Arc<Vec<BTreeMap<String, Value>>>, _reservation: Arc<MemoryReservation> }
impl Documents {
    pub fn new(mut rows: Vec<BTreeMap<String, Value>>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        for row in &mut rows { row.values_mut().for_each(Value::admit_object_dates); }
        let bytes = rows.iter().try_fold(rows.capacity().checked_mul(std::mem::size_of::<BTreeMap<String, Value>>()).ok_or_else(overflow)?, |bytes, row| {
            bytes.checked_add(Value::document_retained_bytes(row)?).ok_or_else(overflow)
        })?;
        Ok(Self { rows: Arc::new(rows), _reservation: memory.reserve(bytes.checked_add(128).ok_or_else(overflow)?)? })
    }
    pub fn rows(&self) -> &[BTreeMap<String, Value>] { &self.rows }
}

#[derive(Clone, Debug)]
pub struct EntityIds { ids: Arc<Vec<EntityId>>, _reservation: Arc<MemoryReservation> }
impl EntityIds {
    pub fn new(ids: Vec<EntityId>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bytes = ids.iter().try_fold(ids.capacity().checked_mul(std::mem::size_of::<EntityId>()).ok_or_else(overflow)?, |bytes, id| {
            let extra = match id { EntityId::String(value) => value.capacity(), EntityId::Keyword(value) => value.retained_bytes(), _ => 0 };
            bytes.checked_add(extra).ok_or_else(overflow)
        })?;
        Ok(Self { ids: Arc::new(ids), _reservation: memory.reserve(bytes.checked_add(128).ok_or_else(overflow)?)? })
    }
    pub fn ids(&self) -> &[EntityId] { &self.ids }
}

#[derive(Clone, Debug)]
pub struct UserMetadata { value: Arc<Value>, _reservation: Arc<MemoryReservation> }
impl UserMetadata {
    pub fn new(mut value: Value, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if !matches!(value, Value::Struct(_)) { return Err(Anomaly::incorrect("xtdb/invalid-user-metadata", "Transaction metadata must be a document")); }
        value.admit_object_dates();
        let reservation = memory.reserve(value.retained_bytes()?.checked_add(128).ok_or_else(overflow)?)?;
        Ok(Self { value: Arc::new(value), _reservation: reservation })
    }
    pub fn value(&self) -> &Value { &self.value }
}

#[derive(Clone, Debug)]
struct ArgumentChunk { rows: Vec<Vec<Value>>, _reservation: Arc<MemoryReservation> }

#[derive(Clone, Debug)]
pub struct SqlArguments {
    types: Arc<Vec<LogicalType>>,
    chunks: Arc<Vec<(usize, Arc<ArgumentChunk>)>>,
    memory: Arc<MemoryBudget>,
    schema_reservations: Arc<Vec<Arc<MemoryReservation>>>,
    row_count: usize,
}
impl SqlArguments {
    /// Explicit column types preserve typed nulls and the schema of an empty argument batch.
    pub fn new(types: Vec<LogicalType>, mut rows: Vec<Vec<Value>>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        for ty in &types { ty.validate()?; }
        if rows.iter().any(|row| row.len() != types.len()) { return Err(Anomaly::incorrect("xtdb.indexer/incorrect-sql-arg-count", "Argument rows have different widths")); }
        for row in &mut rows { row.iter_mut().for_each(Value::admit_object_dates); }
        let mut bytes = rows.capacity().checked_mul(std::mem::size_of::<Vec<Value>>()).ok_or_else(overflow)?;
        for row in &rows {
            bytes = bytes.checked_add(row.capacity().checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?).ok_or_else(overflow)?;
            for value in row { bytes = bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow)?; }
        }
        let schema_reservation = memory.reserve(schema_bytes(&types)?)?;
        let row_count = rows.len();
        let chunk = Arc::new(ArgumentChunk { rows, _reservation: memory.reserve(bytes.checked_add(256).ok_or_else(overflow)?)? });
        Ok(Self { types: Arc::new(types), chunks: Arc::new(vec![(0, chunk)]), memory: Arc::clone(memory), schema_reservations: Arc::new(vec![schema_reservation]), row_count })
    }
    pub fn from_rows(rows: Vec<Vec<Value>>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let width = rows.first().map_or(0, Vec::len);
        let mut types = vec![LogicalType::Nothing; width];
        for row in &rows {
            if row.len() != width { return Err(Anomaly::incorrect("xtdb.indexer/incorrect-sql-arg-count", "Argument rows have different widths")); }
            for (ty, value) in types.iter_mut().zip(row) { *ty = ty.join(&value.logical_type()); }
        }
        Self::new(types, rows, memory)
    }
    pub fn types(&self) -> &[LogicalType] { &self.types }
    /// Opened-vector field types for query or DML binding; source payload fields remain available through `types`.
    pub fn execution_types(&self) -> impl Iterator<Item = LogicalType> + '_ { self.types.iter().map(rtdb_columnar::execution_type) }
    pub fn row_count(&self) -> usize { self.row_count }
    pub fn rows(&self) -> impl Iterator<Item = &[Value]> { self.chunks.iter().flat_map(|(_, chunk)| chunk.rows.iter().map(Vec::as_slice)) }
    pub(crate) fn indexed_rows(&self) -> impl Iterator<Item = (usize, &[Value])> { self.chunks.iter().flat_map(|(start, chunk)| chunk.rows.iter().enumerate().map(move |(index, row)| (*start + index, row.as_slice()))) }
    /// Appends argument rows in order and widens each positional column's type.
    pub fn append(&mut self, other: Self) -> Result<()> {
        self.append_rows(other, self.row_count)
    }
    pub(crate) fn append_page(&mut self, other: Self) -> Result<()> { self.append_rows(other, 0) }
    fn append_rows(&mut self, other: Self, start: usize) -> Result<()> {
        if self.types.len() != other.types.len() { return Err(Anomaly::incorrect("xtdb.indexer/incorrect-sql-arg-count", "Coalesced SQL arguments have different widths")); }
        let count = self.row_count.checked_add(other.row_count).ok_or_else(overflow)?;
        let bytes = schema_bytes(&self.types)?.checked_add(schema_bytes(&other.types)?).and_then(|bytes| bytes.checked_add(self.chunks.len().checked_add(other.chunks.len())?.checked_mul(128)?)).and_then(|bytes| bytes.checked_add(self.schema_reservations.len().checked_add(other.schema_reservations.len())?.checked_mul(128)?)).ok_or_else(overflow)?;
        let reservation = self.memory.reserve(bytes)?;
        self.types = Arc::new(self.types.iter().zip(other.types.iter()).map(|(left, right)| left.join(right)).collect());
        let mut chunks = self.chunks.as_ref().clone();
        for (offset, chunk) in other.chunks.iter() { chunks.push((start.checked_add(*offset).ok_or_else(overflow)?, Arc::clone(chunk))); }
        self.chunks = Arc::new(chunks);
        let mut reservations: Vec<_> = self.schema_reservations.iter().chain(other.schema_reservations.iter()).cloned().collect();
        reservations.push(reservation);
        self.schema_reservations = Arc::new(reservations);
        self.row_count = count;
        Ok(())
    }
}

fn schema_bytes(types: &[LogicalType]) -> Result<usize> {
    types.iter().try_fold(256usize, |bytes, ty| bytes.checked_add(rtdb_columnar::estimate_builder_bytes(ty, &[])?).ok_or_else(overflow))
}

#[derive(Clone, Debug)]
pub enum TxOp {
    Sql { sql: String, args: Option<SqlArguments> },
    SqlBytes { sql: String, arg_bytes: RetainedBytes },
    PutDocs { table: TableRef, documents: Documents, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    PatchDocs { table: TableRef, documents: Documents, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    DeleteDocs { table: TableRef, ids: EntityIds, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    EraseDocs { table: TableRef, ids: EntityIds },
}

#[derive(Clone, Debug)]
pub struct SubmitOptions {
    pub default_tz: String,
    pub system_time: Option<Timestamp>,
    pub user: Option<String>,
    pub user_metadata: Option<UserMetadata>,
}
impl Default for SubmitOptions {
    fn default() -> Self { Self { default_tz: "UTC".into(), system_time: None, user: None, user_metadata: None } }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubmittedTx { pub tx_id: TxId }

#[derive(Debug)]
struct AnomalyAllocation { error: Anomaly, wire: Option<Arc<Vec<u8>>>, reservation: Arc<MemoryReservation> }

#[derive(Clone, Debug)]
pub struct RetainedAnomaly(Arc<AnomalyAllocation>);
impl RetainedAnomaly {
    pub fn new(error: Anomaly, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(anomaly_bytes(&error)?.checked_add(128).ok_or_else(overflow)?)?;
        Ok(Self(Arc::new(AnomalyAllocation { error, wire: None, reservation })))
    }
    /// Retains the exact persisted error representation, including historical Throwable tags.
    pub fn from_transit(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let _transient = memory.reserve(bytes.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?)?;
        let error = rtdb_format::transit::decode_anomaly(bytes)?;
        let reservation = memory.reserve(anomaly_bytes(&error)?.checked_add(bytes.len()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?)?;
        Ok(Self(Arc::new(AnomalyAllocation { error, wire: Some(Arc::new(bytes.to_vec())), reservation })))
    }
    pub fn encode(&self) -> Result<RetainedBytes> {
        if let Some(wire) = &self.0.wire { return Ok(RetainedBytes { data: Arc::clone(wire), _reservation: Arc::clone(&self.0.reservation) }); }
        let bound = anomaly_bytes(&self.0.error)?.checked_mul(8).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
        let reservation = self.0.reservation.budget().reserve(bound)?;
        RetainedBytes::with_reservation(rtdb_format::transit::encode_anomaly(&self.0.error)?, reservation)
    }
}
impl std::ops::Deref for RetainedAnomaly { type Target = Anomaly; fn deref(&self) -> &Anomaly { &self.0.error } }
impl PartialEq for RetainedAnomaly { fn eq(&self, other: &Self) -> bool { self.0.error == other.0.error } }
impl Eq for RetainedAnomaly {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransactionResult { Committed { tx_key: TransactionKey }, Aborted { tx_key: TransactionKey, error: Option<RetainedAnomaly> } }
impl TransactionResult {
    pub fn tx_key(&self) -> TransactionKey { match self { Self::Committed { tx_key } | Self::Aborted { tx_key, .. } => *tx_key } }
    pub fn committed(&self) -> bool { matches!(self, Self::Committed { .. }) }
    pub fn error(&self) -> Option<&Anomaly> { match self { Self::Committed { .. } => None, Self::Aborted { error, .. } => error.as_deref() } }
    pub fn encoded_error(&self) -> Result<Option<RetainedBytes>> { match self { Self::Committed { .. } => Ok(None), Self::Aborted { error, .. } => error.as_ref().map(RetainedAnomaly::encode).transpose() } }
}

pub(crate) fn anomaly_bytes(error: &Anomaly) -> Result<usize> {
    fn json(value: &serde_json::Value) -> Result<usize> {
        match value {
            serde_json::Value::String(value) => value.capacity().checked_add(128).ok_or_else(overflow),
            serde_json::Value::Array(values) => values.iter().try_fold(values.capacity().checked_mul(128).ok_or_else(overflow)?, |bytes, value| bytes.checked_add(json(value)?).ok_or_else(overflow)),
            serde_json::Value::Object(values) => values.iter().try_fold(128usize, |bytes, (key, value)| bytes.checked_add(key.capacity()).and_then(|bytes| bytes.checked_add(256)).and_then(|bytes| bytes.checked_add(json(value).ok()?)).ok_or_else(overflow)),
            _ => Ok(128),
        }
    }
    error.context.iter().try_fold(error.code.capacity().checked_add(error.message.as_ref().map_or(0, String::capacity)).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?, |bytes, (key, value)| bytes.checked_add(key.capacity()).and_then(|bytes| bytes.checked_add(256)).and_then(|bytes| bytes.checked_add(json(value).ok()?)).ok_or_else(overflow))
}

fn overflow() -> Anomaly { Anomaly::incorrect("rtdb.tx/allocation-overflow", "Transaction allocation exceeds address space") }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_allocated_messages_retain_their_capacity_and_absent_messages_allocate_none() {
        let absent = Anomaly::new_nullable(rtdb_types::AnomalyCategory::Fault, "test/error", None);
        let capacity = String::with_capacity(4096);
        let bytes = capacity.capacity();
        let present = Anomaly::new_nullable(rtdb_types::AnomalyCategory::Fault, "test/error", Some(capacity));
        assert_eq!(anomaly_bytes(&present).unwrap() - anomaly_bytes(&absent).unwrap(), bytes);
        let memory = MemoryBudget::new(anomaly_bytes(&absent).unwrap() + 128);
        assert!(RetainedAnomaly::new(present, &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
        let retained = RetainedAnomaly::new(absent, &memory).unwrap();
        assert!(memory.used_bytes() > 0);
        drop(retained); assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn decoded_parameter_pages_reset_error_indices_but_coalesced_batches_continue() {
        let memory = MemoryBudget::new(1 << 20);
        let mut args = SqlArguments::from_rows(vec![vec![Value::I64(1)], vec![Value::I64(2)]], &memory).unwrap();
        args.append_page(SqlArguments::from_rows(vec![vec![Value::I64(3)]], &memory).unwrap()).unwrap();
        assert_eq!(args.indexed_rows().map(|(index, _)| index).collect::<Vec<_>>(), [0, 1, 0]);
        let mut coalesced = SqlArguments::from_rows(vec![vec![Value::I64(1)]], &memory).unwrap();
        coalesced.append(SqlArguments::from_rows(vec![vec![Value::I64(2)]], &memory).unwrap()).unwrap();
        assert_eq!(coalesced.indexed_rows().map(|(index, _)| index).collect::<Vec<_>>(), [0, 1]);
    }
}
