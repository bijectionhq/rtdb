use std::{collections::HashMap, io::Cursor, sync::Arc};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::Buffer;
use arrow_flight::{FlightData, SchemaAsIpc, SchemaResult};
use arrow_ipc::writer::{CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{Schema, SchemaRef};
use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, estimate_read_value_bytes, read_value, from_field};
use rtdb_tx::SqlArguments;
use rtdb_types::Value;
use tonic::Status;
use crate::error::{arrow, invalid, status};

struct RetainedBytes { bytes: Bytes, _reservation: Arc<MemoryReservation> }
impl AsRef<[u8]> for RetainedBytes { fn as_ref(&self) -> &[u8] { &self.bytes } }
pub(crate) fn retain(bytes: impl Into<Bytes>, reservation: Arc<MemoryReservation>) -> Bytes { Bytes::from_owner(RetainedBytes { bytes: bytes.into(), _reservation: reservation }) }
pub(crate) fn add(left: usize, right: usize) -> Result<usize, Status> { left.checked_add(right).ok_or_else(|| invalid("Flight allocation size overflow")) }
pub(crate) fn mul(value: usize, factor: usize) -> Result<usize, Status> { value.checked_mul(factor).ok_or_else(|| invalid("Flight allocation size overflow")) }
pub(crate) fn schema_bound(schema: &Schema) -> Result<usize, Status> {
    let metadata = schema.metadata().iter().try_fold(8192, |size, (key, value)| add(size, add(mul(add(key.len(), value.len())?, 8)?, 128)?))?;
    schema.fields().iter().try_fold(metadata, |size, field| {
        add(size, add(mul(field.size(), 8)?, 2048)?)
    })
}
pub(crate) struct OwnedSchema { schema: Schema, _reservation: Arc<MemoryReservation> }
impl std::ops::Deref for OwnedSchema { type Target = Schema; fn deref(&self) -> &Schema { &self.schema } }
impl OwnedSchema {
    pub fn build(bound: usize, memory: &Arc<MemoryBudget>, build: impl FnOnce() -> Result<Schema, Status>) -> Result<Self, Status> {
        let reservation = memory.reserve(bound).map_err(status)?;
        Ok(Self { schema: build()?, _reservation: reservation })
    }
    pub fn empty(memory: &Arc<MemoryBudget>) -> Result<Self, Status> { Self::build(8192, memory, || Ok(Schema::empty())) }
}
pub(crate) fn plan_schema(schema: &rtdb_plan::Schema, memory: &Arc<MemoryBudget>) -> Result<OwnedSchema, Status> {
    let bound = schema.fields().iter().try_fold(8192, |bytes, field| add(bytes, add(mul(add(field.name.len(), field.data_type.retained_bytes().map_err(status)?)?, 64)?, 4096)?))?;
    OwnedSchema::build(bound, memory, || Ok(Schema::new(schema.fields().iter().map(|field| rtdb_columnar::to_field(&field.name, &field.data_type).map_err(status)).collect::<Result<Vec<_>, _>>()?)))
}
pub(crate) fn schema_message(schema: &Schema, memory: &Arc<MemoryBudget>) -> Result<Bytes, Status> {
    let charge = memory.reserve(schema_bound(schema)?).map_err(status)?;
    let result: SchemaResult = SchemaAsIpc::new(schema, &IpcWriteOptions::default()).try_into().map_err(arrow)?;
    Ok(retain(result.schema, charge))
}
pub(crate) fn schema_data(schema: &Schema, memory: &Arc<MemoryBudget>) -> Result<FlightData, Status> {
    let charge = memory.reserve(schema_bound(schema)?).map_err(status)?;
    let mut result: FlightData = SchemaAsIpc::new(schema, &IpcWriteOptions::default()).into();
    result.data_header = retain(result.data_header, charge); Ok(result)
}
pub(crate) struct Encoder { dictionaries: DictionaryTracker, compression: CompressionContext }
impl Encoder {
    pub fn new() -> Self { Self { dictionaries: DictionaryTracker::new(false), compression: CompressionContext::default() } }
    pub fn batch(&mut self, batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<Vec<FlightData>, Status> {
        let charge = memory.reserve(add(mul(batch.get_array_memory_size(), 4)?, schema_bound(batch.schema().as_ref())?)?).map_err(status)?;
        let (dictionaries, data) = IpcDataGenerator::default().encode(batch, &mut self.dictionaries, &IpcWriteOptions::default(), &mut self.compression).map_err(arrow)?;
        Ok(dictionaries.into_iter().chain(std::iter::once(data)).map(|encoded| { let mut data: FlightData = encoded.into(); data.data_header = retain(data.data_header, Arc::clone(&charge)); data.data_body = retain(data.data_body, Arc::clone(&charge)); data }).collect())
    }
}
pub(crate) struct Decoder { schema: Option<SchemaRef>, dictionaries: HashMap<i64, ArrayRef>, permanent: Vec<Arc<MemoryReservation>>, memory: Arc<MemoryBudget> }
impl Decoder {
    pub fn new(memory: Arc<MemoryBudget>) -> Self { Self { schema: None, dictionaries: HashMap::new(), permanent: Vec::new(), memory } }
    pub fn schema(&self) -> Option<&SchemaRef> { self.schema.as_ref() }
    pub fn decode(&mut self, data: FlightData) -> Result<Option<OwnedBatch>, Status> {
        if data.data_header.is_empty() { return Ok(None); }
        let charge = self.memory.reserve(message_bound(&data)?).map_err(status)?;
        let message = arrow_ipc::root_as_message(&data.data_header).map_err(arrow)?;
        match message.header_type() {
            arrow_ipc::MessageHeader::NONE => Ok(None),
            arrow_ipc::MessageHeader::Schema => {
                if self.schema.is_some() { return Err(invalid("Flight upload contains more than one schema")); }
                self.schema = Some(Arc::new(Schema::try_from(&data).map_err(arrow)?)); self.permanent.push(charge); Ok(None)
            }
            arrow_ipc::MessageHeader::DictionaryBatch => {
                let schema = self.schema.as_ref().ok_or_else(|| invalid("Dictionary arrived before schema"))?;
                let dictionary = message.header_as_dictionary_batch().ok_or_else(|| invalid("Invalid dictionary message"))?;
                arrow_ipc::reader::read_dictionary(&Buffer::from(data.data_body.as_ref()), dictionary, schema, &mut self.dictionaries, &message.version()).map_err(arrow)?;
                self.permanent.push(charge); Ok(None)
            }
            arrow_ipc::MessageHeader::RecordBatch => {
                let schema = self.schema.clone().ok_or_else(|| invalid("Record batch arrived before schema"))?;
                let batch = arrow_flight::utils::flight_data_to_arrow_batch(&data, schema, &self.dictionaries).map_err(arrow)?;
                let retained = self.memory.reserve(batch.get_array_memory_size()).map_err(status)?;
                let owned = OwnedBatch::with_reservation(batch, retained).map_err(status)?;
                drop(charge); Ok(Some(owned))
            }
            _ => Err(invalid("Unexpected Arrow Flight payload")),
        }
    }
}
fn message_bound(data: &FlightData) -> Result<usize, Status> {
    message_slices_bound(&data.data_header, &data.data_body)
}
fn message_slices_bound(header: &[u8], body: &[u8]) -> Result<usize, Status> {
    let message = arrow_ipc::root_as_message(header).map_err(arrow)?;
    if let Some(schema) = message.header_as_schema() { rtdb_format::ipc::validate_decimal_metadata(schema).map_err(status)?; }
    let mut bound = add(8192, add(mul(header.len(), 64)?, mul(body.len(), 4)?)?)?;
    if usize::try_from(message.bodyLength()).map_err(arrow)? > body.len() { return Err(invalid("Arrow message body is truncated")); }
    if let Some(batch) = message.header_as_record_batch().or_else(|| message.header_as_dictionary_batch().and_then(|batch| batch.data())) {
        if let Some(nodes) = batch.nodes() { for node in nodes { bound = add(bound, mul(add(usize::try_from(node.length()).map_err(arrow)?, 1)?, 4096)?)?; } }
        if let Some(buffers) = batch.buffers() { for buffer in buffers {
            let start = usize::try_from(buffer.offset()).map_err(arrow)?; let len = usize::try_from(buffer.length()).map_err(arrow)?;
            let bytes = body.get(start..add(start, len)?).ok_or_else(|| invalid("Arrow buffer exceeds message body"))?;
            let decoded = if batch.compression().is_some() && !bytes.is_empty() {
                let prefix: [u8; 8] = bytes.get(..8).ok_or_else(|| invalid("Compressed Arrow buffer has no length"))?.try_into().map_err(arrow)?;
                let length = i64::from_le_bytes(prefix); if length == -1 { bytes.len() - 8 } else { usize::try_from(length).map_err(arrow)? }
            } else { len };
            bound = add(bound, add(mul(decoded, 4)?, 512)?)?;
        } }
    }
    Ok(bound)
}
pub(crate) fn parameter_ipc(batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<Option<Bytes>, Status> {
    if batch.num_columns() == 0 { return Ok(None); }
    let charge = memory.reserve(add(mul(batch.get_array_memory_size(), 4)?, schema_bound(batch.schema().as_ref())?)?).map_err(status)?;
    let mut output = Vec::new();
    { let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut output, batch.schema().as_ref()).map_err(arrow)?; writer.write(batch).map_err(arrow)?; writer.finish().map_err(arrow)?; }
    Ok(Some(retain(output, charge)))
}
pub(crate) fn parameter_batch(bytes: &Bytes, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch, Status> {
    let charge = memory.reserve(ipc_bound(bytes)?).map_err(status)?;
    let mut reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes.as_ref()), None).map_err(arrow)?;
    let batch = reader.next().transpose().map_err(arrow)?.unwrap_or_else(|| RecordBatch::new_empty(reader.schema()));
    OwnedBatch::with_reservation(batch, charge).map_err(status)
}
fn ipc_bound(bytes: &[u8]) -> Result<usize, Status> {
    let mut position = 0usize; let mut bound = 8192;
    while position < bytes.len() {
        let mut length = read_i32(bytes, &mut position)?;
        if length == -1 { length = read_i32(bytes, &mut position)?; }
        if length == 0 { break; }
        let end = add(position, usize::try_from(length).map_err(arrow)?)?;
        let header = bytes.get(position..end).ok_or_else(|| invalid("Truncated parameter schema"))?;
        let message = arrow_ipc::root_as_message(header).map_err(arrow)?;
        let body_end = add(end, usize::try_from(message.bodyLength()).map_err(arrow)?)?;
        let body = bytes.get(end..body_end).ok_or_else(|| invalid("Truncated parameter body"))?;
        bound = add(bound, message_slices_bound(header, body)?)?;
        position = body_end;
    }
    Ok(bound)
}
fn read_i32(bytes: &[u8], position: &mut usize) -> Result<i32, Status> { let end = add(*position, 4)?; let value = i32::from_le_bytes(bytes.get(*position..end).ok_or_else(|| invalid("Truncated parameter IPC"))?.try_into().map_err(arrow)?); *position = end; Ok(value) }
pub(crate) fn arguments(batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<SqlArguments, Status> {
    let schema = batch.schema(); let mut bound = 8192;
    for row in 0..batch.num_rows() { for (column, field) in schema.fields().iter().enumerate() { bound = add(bound, estimate_read_value_bytes(field, batch.column(column).as_ref(), row).map_err(status)?)?; } }
    let _scratch = memory.reserve(bound).map_err(status)?;
    let types = schema.fields().iter().map(|field| from_field(field).map_err(status)).collect::<Result<Vec<_>, _>>()?;
    let rows = (0..batch.num_rows()).map(|row| schema.fields().iter().enumerate().map(|(column, field)| read_value(field, batch.column(column).as_ref(), row).map_err(status)).collect::<Result<Vec<Value>, _>>()).collect::<Result<Vec<_>, _>>()?;
    SqlArguments::new(types, rows, memory).map_err(status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    #[test]
    fn streaming_arrow_buffers_remain_charged_after_encoder_and_batch_drop() {
        let memory = MemoryBudget::new(1 << 20); let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![Some(42), None]))]).unwrap();
        let mut encoder = Encoder::new(); let data = encoder.batch(&batch, &memory).unwrap(); drop(batch); drop(encoder); assert!(memory.used_bytes() > 0);
        let mut decoder = Decoder::new(memory.clone()); decoder.decode(schema_data(&schema, &memory).unwrap()).unwrap(); let batch = decoder.decode(data.into_iter().next().unwrap()).unwrap().unwrap();
        let cloned = batch.batch().column(0).clone(); drop(batch); drop(decoder); assert!(memory.used_bytes() > 0); assert_eq!(cloned.as_any().downcast_ref::<Int64Array>().unwrap().value(0), 42);
        drop(cloned); assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn empty_parameter_columns_and_empty_parameter_rows_remain_distinct() {
        let memory = MemoryBudget::new(1 << 20); assert!(parameter_ipc(&RecordBatch::new_empty(Arc::new(Schema::empty())), &memory).unwrap().is_none());
        let schema = Arc::new(Schema::new(vec![Field::new("arbitrary-name", DataType::Int64, true)]));
        let bytes = parameter_ipc(&RecordBatch::new_empty(schema), &memory).unwrap().unwrap(); let batch = parameter_batch(&bytes, &memory).unwrap(); let args = arguments(batch.batch(), &memory).unwrap();
        assert_eq!(args.row_count(), 0); assert_eq!(args.types().len(), 1);
    }
    #[test]
    fn truncated_parameter_stream_cannot_allocate_from_untrusted_body_length() {
        let memory = MemoryBudget::new(1 << 20); let before = memory.used_bytes();
        assert!(parameter_batch(&Bytes::from_static(&[255,255,255,255,127,255,255,127]), &memory).is_err()); assert_eq!(memory.used_bytes(), before);
    }
}
