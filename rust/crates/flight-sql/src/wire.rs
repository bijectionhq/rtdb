use std::{collections::HashMap, sync::Arc};
#[cfg(test)]
use std::io::Cursor;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::Buffer;
use arrow_flight::FlightData;
use arrow_ipc::writer::{CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{Schema, SchemaRef};
use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, from_field};
use rtdb_tx::SqlArguments;
use rtdb_types::LogicalType;
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
    Ok(retain(rtdb_format::ipc::write_schema_message_with_options(schema, &IpcWriteOptions::default()).map_err(status)?, charge))
}
pub(crate) fn schema_data(schema: &Schema, memory: &Arc<MemoryBudget>) -> Result<FlightData, Status> {
    let charge = memory.reserve(schema_bound(schema)?).map_err(status)?;
    Ok(FlightData { data_header: retain(rtdb_format::ipc::write_schema_header_with_options(schema, &IpcWriteOptions::default()).map_err(status)?, charge), ..Default::default() })
}
pub(crate) struct Encoder { schema: SchemaRef, _reservation: Arc<MemoryReservation>, dictionaries: DictionaryTracker, compression: CompressionContext }
impl Encoder {
    pub fn new(schema: &Schema, memory: &Arc<MemoryBudget>) -> Result<Self, Status> {
        let reservation = memory.reserve(schema_bound(schema)?).map_err(status)?;
        Ok(Self { schema: Arc::new(schema.clone()), _reservation: reservation, dictionaries: DictionaryTracker::new(false), compression: CompressionContext::default() })
    }
    pub fn batch(&mut self, batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<Vec<FlightData>, Status> {
        if batch.schema().as_ref() != self.schema.as_ref() { return Err(invalid("Flight batch schema differs from its declared schema")); }
        self.encode(batch, memory)
    }
    pub fn declared_batch(&mut self, batch: &rtdb_columnar::DeclaredBatch, memory: &Arc<MemoryBudget>) -> Result<Vec<FlightData>, Status> {
        if batch.declared_schema().as_ref() != self.schema.as_ref() { return Err(invalid("Flight batch schema differs from its declared schema")); }
        self.encode(batch.batch(), memory)
    }
    fn encode(&mut self, batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<Vec<FlightData>, Status> {
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
                self.schema = Some(Arc::new(rtdb_format::ipc::read_schema_header(&data.data_header).map_err(status)?)); self.permanent.push(charge); Ok(None)
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
                let retained = self.memory.reserve(rtdb_columnar::shared_allocation_size(&batch).map_err(status)?).map_err(status)?;
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
#[cfg(test)]
fn parameter_ipc(batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<Option<Bytes>, Status> {
    if batch.num_columns() == 0 { return Ok(None); }
    let charge = memory.reserve(add(mul(batch.get_array_memory_size(), 4)?, schema_bound(batch.schema().as_ref())?)?).map_err(status)?;
    let output = rtdb_format::ipc::write_arrow_stream(batch.schema().as_ref(), std::slice::from_ref(batch), IpcWriteOptions::default()).map_err(status)?;
    Ok(Some(retain(output, charge)))
}
#[cfg(test)]
fn parameter_batch(bytes: &Bytes, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch, Status> {
    let mut charge = memory.reserve(ipc_bound(bytes)?).map_err(status)?;
    let mut reader = rtdb_format::ipc::ArrowStream::from_reader(Cursor::new(bytes.as_ref()), None).map_err(status)?;
    let batch = reader.next().transpose().map_err(status)?.unwrap_or_else(|| RecordBatch::new_empty(reader.schema()));
    drop(reader);
    let retained = add(rtdb_columnar::shared_allocation_size(&batch).map_err(status)?, schema_bound(batch.schema_ref())?)?;
    let reservation = Arc::get_mut(&mut charge).expect("ticket decoding owns its reservation");
    reservation.shrink_to(retained.min(reservation.bytes())).map_err(status)?;
    OwnedBatch::with_reservation(batch, charge).map_err(status)
}

pub(crate) struct BoundParameters {
    batch: OwnedBatch,
    _schema: Arc<MemoryReservation>,
}

impl BoundParameters {
    pub fn new(batch: OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>, Status> {
        let schema = memory.reserve(schema_bound(batch.batch().schema_ref())?).map_err(status)?;
        let batch = match rtdb_format::ipc::adapt_stream_batch(batch.batch()).map_err(status)? {
            Some(adapted) => OwnedBatch::with_reservation(adapted, Arc::clone(batch.reservation())).map_err(status)?,
            None => batch,
        };
        Ok(Arc::new(Self { batch, _schema: schema }))
    }

    pub fn empty(schema: &SchemaRef, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>, Status> {
        let bound = schema_bound(schema)?;
        let scratch = memory.reserve(mul(bound, 2)?).map_err(status)?;
        let batch = RecordBatch::new_empty(Arc::clone(schema));
        let batch = OwnedBatch::new(batch, memory).map_err(status)?;
        let bound = Self::new(batch, memory)?;
        drop(scratch);
        Ok(bound)
    }

    pub fn batch(&self) -> &RecordBatch { self.batch.batch() }

    pub fn owned_batch(&self) -> &OwnedBatch { &self.batch }

    pub fn arguments(&self, memory: &Arc<MemoryBudget>) -> Result<SqlArguments, Status> {
        arguments(&self.batch, memory)
    }
}
#[cfg(test)]
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
#[cfg(test)]
fn read_i32(bytes: &[u8], position: &mut usize) -> Result<i32, Status> { let end = add(*position, 4)?; let value = i32::from_le_bytes(bytes.get(*position..end).ok_or_else(|| invalid("Truncated parameter IPC"))?.try_into().map_err(arrow)?); *position = end; Ok(value) }
fn logical_parameter_types(schema: &Schema) -> Result<Vec<LogicalType>, Status> {
    schema.fields().iter().map(|field| from_field(field).map_err(status)).collect()
}

pub(crate) struct ParameterTypes {
    types: Vec<LogicalType>,
    _reservation: Arc<MemoryReservation>,
}
impl ParameterTypes {
    pub fn from_batch(batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<Self, Status> {
        let schema = batch.schema_ref();
        let reservation = memory.reserve(schema_bound(schema)?).map_err(status)?;
        Ok(Self { types: logical_parameter_types(schema)?, _reservation: reservation })
    }

    pub fn as_slice(&self) -> &[LogicalType] { &self.types }
}

pub(crate) fn arguments(batch: &OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<SqlArguments, Status> {
    let record = batch.batch();
    let _check = memory.reserve(schema_bound(record.schema_ref())?).map_err(status)?;
    let arguments = SqlArguments::from_arrow(batch.clone(), memory).map_err(status)?;
    for ((field, array), logical) in record.schema_ref().fields().iter().zip(record.columns()).zip(arguments.types()) {
        rtdb_columnar::validate_readable_column(field, array.as_ref(), logical, memory).map_err(status)?;
    }
    Ok(arguments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use rtdb_columnar::read_value;
    use rtdb_types::Value;
    use arrow_schema::{DataType, Field};
    #[test]
    fn argument_rows_are_each_cell_read_under_its_columns_field() {
        let memory = MemoryBudget::new(1 << 22);
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true), Field::new("s", DataType::Utf8, false),
            Field::new("t", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())), true)]));
        let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(1), None])), Arc::new(arrow_array::StringArray::from(vec!["a", "b"])),
            Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![None, Some(5)]).with_timezone("UTC"))];
        let batch = OwnedBatch::new(RecordBatch::try_new(Arc::clone(&schema), columns).unwrap(), &memory).unwrap();
        let arguments = arguments(&batch, &memory).unwrap();
        let expected: Vec<Vec<Value>> = (0..2).map(|row| (0..3).map(|column| read_value(schema.field(column), batch.batch().column(column).as_ref(), row).unwrap()).collect()).collect();
        assert_eq!(arguments.rows().map(|row| row.unwrap().to_vec()).collect::<Vec<_>>(), expected);
    }
    #[test]
    fn declared_flight_schema_keeps_nonnullable_timestamp_with_conformed_null_buffers() {
        let memory = MemoryBudget::new(1 << 22);
        let target = Schema::new(vec![Field::new("v", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())), false)]);
        let source = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("v", DataType::Null, true)])), vec![Arc::new(arrow_array::NullArray::new(1))]).unwrap();
        let page = rtdb_columnar::conform_batch(OwnedBatch::new(source, &memory).unwrap(), &target, &memory).unwrap();
        let header = schema_data(&target, &memory).unwrap();
        let message = arrow_ipc::root_as_message(&header.data_header).unwrap();
        assert!(!message.header_as_schema().unwrap().fields().unwrap().get(0).nullable());
        let mut encoder = Encoder::new(&target, &memory).unwrap();
        assert!(encoder.batch(page.batch(), &memory).is_err());
        let data = encoder.declared_batch(&page, &memory).unwrap();
        let message = arrow_ipc::root_as_message(&data[0].data_header).unwrap();
        let record = message.header_as_record_batch().unwrap();
        assert_eq!(record.length(), 1);
        assert_eq!(record.nodes().unwrap().get(0).null_count(), 1);
        assert_eq!(record.buffers().unwrap().len(), 2);
        drop(page); drop(data); drop(header); drop(encoder);
        assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn streaming_arrow_buffers_remain_charged_after_encoder_and_batch_drop() {
        let memory = MemoryBudget::new(1 << 20); let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![Some(42), None]))]).unwrap();
        let mut encoder = Encoder::new(&schema, &memory).unwrap(); let data = encoder.batch(&batch, &memory).unwrap(); drop(batch); drop(encoder); assert!(memory.used_bytes() > 0);
        let mut decoder = Decoder::new(memory.clone()); decoder.decode(schema_data(&schema, &memory).unwrap()).unwrap(); let batch = decoder.decode(data.into_iter().next().unwrap()).unwrap().unwrap();
        let cloned = batch.batch().column(0).clone(); drop(batch); drop(decoder); assert!(memory.used_bytes() > 0); assert_eq!(cloned.as_any().downcast_ref::<Int64Array>().unwrap().value(0), 42);
        drop(cloned); assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn empty_parameter_columns_and_empty_parameter_rows_remain_distinct() {
        let memory = MemoryBudget::new(1 << 20); assert!(parameter_ipc(&RecordBatch::new_empty(Arc::new(Schema::empty())), &memory).unwrap().is_none());
        let schema = Arc::new(Schema::new(vec![Field::new("arbitrary-name", DataType::Int64, true)]));
        let bytes = parameter_ipc(&RecordBatch::new_empty(schema), &memory).unwrap().unwrap(); let batch = parameter_batch(&bytes, &memory).unwrap(); let args = arguments(&batch, &memory).unwrap();
        assert_eq!(args.row_count(), 0); assert_eq!(args.types().len(), 1);
    }

    #[test]
    fn schema_only_parameter_bindings_keep_the_typed_empty_batch_charged() {
        let memory = MemoryBudget::new(1 << 20);
        let schema = Arc::new(Schema::new(vec![Field::new("arg", DataType::Int64, false)]));

        let bound = BoundParameters::empty(&schema, &memory).unwrap();
        let args = bound.arguments(&memory).unwrap();
        drop(bound);

        assert!(memory.used_bytes() > 0);
        assert_eq!(args.types(), &[LogicalType::Scalar(rtdb_types::ScalarType::I64)]);
        assert_eq!(args.row_count(), 0);
        drop(args);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn parameter_type_projection_admits_and_releases_metadata_for_empty_batches() {
        let child_names = (0..16).map(|index| format!("field_{}_{index}", "x".repeat(512))).collect::<Vec<_>>();
        let children = child_names.iter().map(|name| Field::new(name.as_str(), DataType::Int64, false)).collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![Field::new("arg", DataType::Struct(children.into()), false)]));
        let expected = LogicalType::Struct(child_names.iter().map(|name| (name.clone(), LogicalType::Scalar(rtdb_types::ScalarType::I64))).collect());
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let bound = schema_bound(&schema).unwrap();

        let batch_memory = MemoryBudget::new(1 << 24);
        let owned = OwnedBatch::new(batch.clone(), &batch_memory).unwrap();
        let short = MemoryBudget::new(bound - 1);
        for error in [
            ParameterTypes::from_batch(&batch, &short).err().unwrap(),
            arguments(&owned, &short).err().unwrap(),
        ] {
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            let bytes = error.metadata().get_bin("xtdb-anomaly-bin").unwrap().to_bytes().unwrap();
            let anomaly: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(anomaly["code"], "rtdb/memory-budget-exceeded");
        }
        assert_eq!(short.peak_bytes(), 0);

        let memory = MemoryBudget::new(bound);
        let types = ParameterTypes::from_batch(&batch, &memory).unwrap();
        assert_eq!(types.as_slice(), std::slice::from_ref(&expected));
        assert_eq!(memory.used_bytes(), bound);
        drop(types);
        assert_eq!(memory.used_bytes(), 0);

        let encoded = MemoryBudget::new(1 << 24);
        let bytes = parameter_ipc(&batch, &encoded).unwrap().unwrap();
        let decoded = MemoryBudget::new(1 << 24);
        let round_trip = parameter_batch(&bytes, &decoded).unwrap();
        let decoded_bound = schema_bound(round_trip.batch().schema_ref()).unwrap();
        let types = ParameterTypes::from_batch(round_trip.batch(), &decoded).unwrap();
        drop(round_trip);
        drop(bytes);
        assert_eq!(encoded.used_bytes(), 0);
        assert_eq!(types.as_slice(), std::slice::from_ref(&expected));
        assert_eq!(decoded.used_bytes(), decoded_bound);
        drop(types);
        assert_eq!(decoded.used_bytes(), 0);

        let memory = MemoryBudget::new(1 << 24);
        let args = arguments(&owned, &memory).unwrap();
        drop(owned);
        drop(batch);
        assert_eq!(args.types(), std::slice::from_ref(&expected));
        assert_eq!(args.row_count(), 0);
        assert!(batch_memory.used_bytes() > 0);
        assert_eq!(memory.peak_bytes(), bound + memory.used_bytes());
        drop(args);
        assert_eq!(batch_memory.used_bytes(), 0);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn bound_arguments_retain_arrow_and_admit_rows_only_when_read() {
        let memory = MemoryBudget::new(1 << 20);
        let schema = Arc::new(Schema::new(vec![Field::new("client-name", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![None, Some(17)]))]).unwrap();
        let bound = BoundParameters::new(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
        let restore = bound.arguments(&memory).unwrap();
        drop(bound);
        let obstruction = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();

        let error = restore.rows().next().unwrap().err().unwrap();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");

        drop(obstruction);
        assert_eq!(restore.rows().map(|row| row.unwrap().to_vec()).collect::<Vec<_>>(), [vec![Value::Null], vec![Value::I64(17)]]);
        drop(restore);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn ticket_arguments_reject_an_unreadable_value_after_the_first_row() {
        let memory = MemoryBudget::new(1 << 20);
        let field = rtdb_columnar::to_field("arg", &LogicalType::Scalar(rtdb_types::ScalarType::Uri)).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(arrow_array::StringArray::from(vec!["https://xtdb.com", "bad uri"]))],
        ).unwrap();
        let bytes = parameter_ipc(&batch, &memory).unwrap().unwrap();
        let decoded = parameter_batch(&bytes, &memory).unwrap();

        let error = arguments(&decoded, &memory).err().unwrap();

        let bytes = error.metadata().get_bin("xtdb-anomaly-bin").unwrap().to_bytes().unwrap();
        let anomaly: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(anomaly["code"], "xtdb/invalid-uri");
    }

    #[test]
    fn ticket_arguments_keep_only_the_retained_arrow_allocation_after_decoding() {
        let encoded = MemoryBudget::new(1 << 22);
        let schema = Arc::new(Schema::new(vec![Field::new("arg", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42; 2048]))]).unwrap();
        let bytes = parameter_ipc(&batch, &encoded).unwrap().unwrap();
        let memory = MemoryBudget::new(ipc_bound(&bytes).unwrap());
        let decoded = parameter_batch(&bytes, &memory).unwrap();

        let args = arguments(&decoded, &memory).unwrap();
        drop(decoded);
        assert_eq!(args.row_count(), 2048);
        assert_eq!(&*args.rows().next().unwrap().unwrap(), &[Value::I64(42)]);
        assert!(memory.used_bytes() < memory.limit_bytes() / 16);
        drop(args);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn decoded_parameter_buffers_charge_the_shared_ipc_body_once() {
        let memory = MemoryBudget::new(1 << 28);
        let fields = (0..32).map(|column| Field::new(format!("col_{column}"), DataType::Int64, false)).collect::<Vec<_>>();
        let columns = (0..32).map(|_| {
            Arc::new(Int64Array::from((0..128_i64).collect::<Vec<_>>())) as ArrayRef
        }).collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let mut encoder = Encoder::new(&schema, &memory).unwrap();
        let data = encoder.batch(&batch, &memory).unwrap().pop().unwrap();
        let body = data.data_body.len();
        let mut decoder = Decoder::new(Arc::clone(&memory));
        decoder.decode(schema_data(&schema, &memory).unwrap()).unwrap();
        let decoded = decoder.decode(data).unwrap().unwrap();

        assert!(decoded.reservation().bytes() < body * 3);
        assert_eq!(decoded.batch(), &batch);
    }

    #[test]
    fn nested_union_parameter_metadata_matches_ticket_decoding_without_copying_cells() {
        use arrow_array::{Array, NullArray, UnionArray, cast::AsArray, types::Int64Type};
        use arrow_buffer::ScalarBuffer;
        use arrow_schema::UnionFields;

        let memory = MemoryBudget::new(1 << 22);
        let legs = UnionFields::try_new([0, 1], [
            Arc::new(Field::new("bottom", DataType::Null, false)),
            Arc::new(Field::new("i64", DataType::Int64, false)),
        ]).unwrap();
        let union = UnionArray::try_new(legs, ScalarBuffer::from(vec![1_i8]), Some(ScalarBuffer::from(vec![0_i32])), vec![
            Arc::new(NullArray::new(0)),
            Arc::new(Int64Array::from(vec![100])),
        ]).unwrap();
        let child = Arc::new(Field::new("value", union.data_type().clone(), false));
        let record = rtdb_columnar::build_struct_array(vec![child].into(), &[Arc::new(union)], 1, None).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("arg", record.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(schema, vec![record]).unwrap();
        let pointer = |batch: &RecordBatch| batch.column(0).as_struct().column(0).as_union().child(1)
            .as_primitive::<Int64Type>().values().as_ptr();
        let address = pointer(&batch);
        let bytes = parameter_ipc(&batch, &memory).unwrap().unwrap();
        let decoded = parameter_batch(&bytes, &memory).unwrap();

        let retained = BoundParameters::new(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();

        assert_eq!(retained.batch().schema_ref(), decoded.batch().schema_ref());
        assert_eq!(pointer(retained.batch()), address);
    }

    #[test]
    fn unsupported_parameter_type_releases_the_projection_charge() {
        let schema = Arc::new(Schema::new(vec![Field::new("arg", DataType::LargeUtf8, true)]));
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let bound = schema_bound(&schema).unwrap();
        let memory = MemoryBudget::new(bound);
        let error = ParameterTypes::from_batch(&batch, &memory).err().unwrap();
        assert_eq!(error.code(), tonic::Code::Unimplemented);
        assert_eq!(memory.peak_bytes(), bound);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn truncated_parameter_stream_cannot_allocate_from_untrusted_body_length() {
        let memory = MemoryBudget::new(1 << 20); let before = memory.used_bytes();
        assert!(parameter_batch(&Bytes::from_static(&[255,255,255,255,127,255,255,127]), &memory).is_err()); assert_eq!(memory.used_bytes(), before);
    }
    #[test]
    fn flight_schema_batches_and_parameters_preserve_signed_decimal_parameters() {
        for (precision, scale) in [(3, i32::MIN), (3, -129), (3, 128), (3, i32::MAX), (i32::MIN, 0), (-1, i32::MIN), (0, i32::MAX), (256, 1), (i32::MAX, i32::MAX)] {
            let memory = MemoryBudget::new(16 * 1024 * 1024);
            let ty = rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Decimal { precision, scale, bit_width: 128 });
            let value = Value::Decimal(rtdb_types::DecimalValue::new((-123i128).to_be_bytes().to_vec(), precision, scale, 128).unwrap());
            let schema = Arc::new(Schema::new(vec![rtdb_columnar::to_field("value", &ty).unwrap()]));
            let batch = RecordBatch::try_new(schema.clone(), vec![rtdb_columnar::copy_array(&ty, std::slice::from_ref(&value)).unwrap()]).unwrap();
            let header = schema_data(&schema, &memory).unwrap();
            let message = arrow_ipc::root_as_message(&header.data_header).unwrap();
            let field = message.header_as_schema().unwrap().fields().unwrap().get(0);
            assert_eq!(field.type_as_decimal().unwrap().scale(), scale);
            assert_eq!(field.type_as_decimal().unwrap().precision(), precision);
            assert!(field.custom_metadata().is_none_or(|entries| entries.iter().all(|entry| entry.key() != Some(rtdb_columnar::INTERNAL_DECIMAL_SCALE_KEY))));
            assert!(field.custom_metadata().is_none_or(|entries| entries.iter().all(|entry| entry.key() != Some(rtdb_columnar::INTERNAL_DECIMAL_PRECISION_KEY))));
            let bytes = schema_message(&schema, &memory).unwrap();
            assert_eq!(from_field(rtdb_format::ipc::read_schema_message(&bytes).unwrap().field(0)).unwrap(), ty);
            drop(bytes);
            let mut decoder = Decoder::new(memory.clone()); decoder.decode(header).unwrap();
            let encoded = Encoder::new(&schema, &memory).unwrap().batch(&batch, &memory).unwrap();
            let decoded = decoder.decode(encoded.into_iter().next().unwrap()).unwrap().unwrap();
            assert_eq!(read_value(decoded.batch().schema().field(0), decoded.batch().column(0).as_ref(), 0).unwrap(), value);
            let bytes = parameter_ipc(&batch, &memory).unwrap().unwrap(); let parameters = parameter_batch(&bytes, &memory).unwrap();
            let args = arguments(&parameters, &memory).unwrap();
            assert_eq!(args.types(), &[ty]); assert_eq!(&*args.rows().next().unwrap().unwrap(), &[value]);
            drop(args); drop(parameters); drop(bytes); drop(decoded); drop(decoder); assert_eq!(memory.used_bytes(), 0);
        }
    }
    #[test]
    fn flight_encoder_rejects_a_different_full_scale_before_allocating_output() {
        let memory = MemoryBudget::new(1 << 20);
        let schema = |scale| Schema::new(vec![rtdb_columnar::to_field("value", &rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Decimal { precision: 3, scale, bit_width: 128 })).unwrap()]);
        let mut encoder = Encoder::new(&schema(129), &memory).unwrap();
        let before = memory.used_bytes();
        assert!(encoder.batch(&RecordBatch::new_empty(Arc::new(schema(130))), &memory).is_err());
        assert_eq!(memory.used_bytes(), before); drop(encoder); assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn narrow_flight_schema_bytes_match_arrow_schema_as_ipc() {
        let memory = MemoryBudget::new(1 << 20);
        let schema = Schema::new(vec![Field::new("value", DataType::Decimal128(3, 1), true)]);
        let options = IpcWriteOptions::default();
        let expected: arrow_flight::SchemaResult = arrow_flight::SchemaAsIpc::new(&schema, &options).try_into().unwrap();
        assert_eq!(schema_message(&schema, &memory).unwrap().as_ref(), expected.schema.as_ref());
        let expected: FlightData = arrow_flight::SchemaAsIpc::new(&schema, &options).into();
        assert_eq!(schema_data(&schema, &memory).unwrap().data_header, expected.data_header);
        assert_eq!(memory.used_bytes(), 0);
    }
}
