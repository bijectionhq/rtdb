use std::{collections::HashMap, sync::Arc};
#[cfg(test)]
use std::io::Cursor;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::Buffer;
use arrow_flight::FlightData;
use arrow_ipc::writer::{CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{DataType, Schema, SchemaRef};
use bytes::Bytes;
use rtdb_columnar::{BufferIdentity, MemoryBudget, MemoryReservation, OwnedBatch, RetainedArray, from_field};
use rtdb_tx::SqlArguments;
use rtdb_types::{LogicalType, Value};
use tokio::sync::OnceCell;
use tonic::Status;
use crate::error::{arrow, invalid, status};

mod dictionary;

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
pub(crate) struct OwnedSchema { schema: Schema, reservation: Arc<MemoryReservation> }
impl std::ops::Deref for OwnedSchema { type Target = Schema; fn deref(&self) -> &Schema { &self.schema } }
impl OwnedSchema {
    pub fn build(bound: usize, memory: &Arc<MemoryBudget>, build: impl FnOnce() -> Result<Schema, Status>) -> Result<Self, Status> {
        let reservation = memory.reserve(bound).map_err(status)?;
        Ok(Self { schema: build()?, reservation })
    }
    pub fn empty(memory: &Arc<MemoryBudget>) -> Result<Self, Status> { Self::build(8192, memory, || Ok(Schema::empty())) }
    pub(crate) fn into_declared(self) -> rtdb_columnar::DeclaredSchema {
        rtdb_columnar::DeclaredSchema::with_reservation(self.schema, self.reservation)
    }
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
pub(crate) fn reserve_batch(batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<Arc<MemoryReservation>, Status> {
    memory.reserve(batch_bound(batch)?).map_err(status)
}
pub(crate) fn batch_bound(batch: &RecordBatch) -> Result<usize, Status> {
    add(mul(batch.get_array_memory_size(), 4)?, schema_bound(batch.schema().as_ref())?)
}

struct EncodedIpcBytes {
    bytes: Vec<u8>,
    _reservation: Arc<std::sync::OnceLock<MemoryReservation>>,
}

impl AsRef<[u8]> for EncodedIpcBytes {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}

pub(crate) struct Encoder { schema: SchemaRef, _reservation: Arc<MemoryReservation>, dictionaries: DictionaryTracker, compression: CompressionContext }
impl Encoder {
    pub fn new(schema: &Schema, memory: &Arc<MemoryBudget>) -> Result<Self, Status> {
        let reservation = memory.reserve(schema_bound(schema)?).map_err(status)?;
        Ok(Self { schema: Arc::new(schema.clone()), _reservation: reservation, dictionaries: DictionaryTracker::new(false), compression: CompressionContext::default() })
    }
    pub fn batch(&mut self, batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<Vec<FlightData>, Status> {
        if batch.schema().as_ref() != self.schema.as_ref() { return Err(invalid("Flight batch schema differs from its declared schema")); }
        self.encode_reserved(batch, reserve_batch(batch, memory)?)
    }
    pub fn declared_batch(&mut self, batch: &rtdb_columnar::DeclaredBatch, memory: &Arc<MemoryBudget>) -> Result<Vec<FlightData>, Status> {
        if batch.declared_schema().as_ref() != self.schema.as_ref() { return Err(invalid("Flight batch schema differs from its declared schema")); }
        self.encode_reserved(batch.batch(), reserve_batch(batch.batch(), memory)?)
    }
    pub(crate) fn reserved_batch(&mut self, batch: &RecordBatch, charge: Arc<MemoryReservation>) -> Result<Vec<FlightData>, Status> {
        if batch.schema().as_ref() != self.schema.as_ref() { return Err(invalid("Flight batch schema differs from its declared schema")); }
        if charge.bytes() < batch_bound(batch)? { return Err(invalid("Flight batch exceeds its reservation")); }
        self.encode_reserved(batch, charge)
    }
    fn encode_reserved(&mut self, batch: &RecordBatch, charge: Arc<MemoryReservation>) -> Result<Vec<FlightData>, Status> {
        use std::{mem::size_of, sync::OnceLock};

        let mut charge = Arc::try_unwrap(charge)
            .map_err(|_| Status::internal("Flight batch encoding requires an exclusive reservation"))?;
        let (dictionaries, data) = IpcDataGenerator::default()
            .encode(batch, &mut self.dictionaries, &IpcWriteOptions::default(), &mut self.compression)
            .map_err(arrow)?;

        let reservation = Arc::new(OnceLock::new());
        let mut frames = Vec::with_capacity(add(dictionaries.len(), 1)?);
        let bytes_owner = size_of::<EncodedIpcBytes>() + size_of::<usize>();
        let reservation_owner = size_of::<OnceLock<MemoryReservation>>() + 2 * size_of::<usize>();
        let mut retained = add(mul(frames.capacity(), size_of::<FlightData>())?, reservation_owner)?;
        for encoded in dictionaries.into_iter().chain(std::iter::once(data)) {
            retained = add(retained, add(encoded.ipc_message.capacity(), encoded.arrow_data.capacity())?)?;
            retained = add(retained, 2 * bytes_owner)?;
            frames.push(FlightData {
                data_header: Bytes::from_owner(EncodedIpcBytes {
                    bytes: encoded.ipc_message,
                    _reservation: Arc::clone(&reservation),
                }),
                data_body: Bytes::from_owner(EncodedIpcBytes {
                    bytes: encoded.arrow_data,
                    _reservation: Arc::clone(&reservation),
                }),
                ..Default::default()
            });
        }
        charge.shrink_to(retained)
            .map_err(|_| Status::internal("Flight IPC output exceeds its encoding reservation"))?;
        reservation.set(charge).expect("Flight IPC output reservation is installed once");
        Ok(frames)
    }
}
struct UploadSchema {
    schema: SchemaRef,
    reservation: Arc<MemoryReservation>,
}

impl UploadSchema {
    fn decode(header: &[u8], grant: &mut MemoryReservation) -> Result<Self, Status> {
        let schema = rtdb_format::ipc::read_schema_header(header).map_err(status)?;
        let bytes = add(128, mul(schema.metadata().capacity(), 64)?)?;
        let bytes = schema.metadata().iter().try_fold(bytes, |bytes, (key, value)| {
            add(bytes, add(key.capacity(), value.capacity())?)
        })?;
        let bytes = schema.fields().iter().try_fold(bytes, |bytes, field| add(bytes, add(field.size(), 32)?))?;
        let reservation = grant.split_off(bytes).map_err(status)?;
        Ok(Self { schema: Arc::new(schema), reservation })
    }
}

pub(crate) struct Decoder {
    schema: Option<UploadSchema>,
    dictionaries: HashMap<i64, RetainedArray>,
    memory: Arc<MemoryBudget>,
}

impl Decoder {
    pub fn new(memory: Arc<MemoryBudget>) -> Self { Self { schema: None, dictionaries: HashMap::new(), memory } }

    pub fn empty_batch(&self) -> Result<Option<OwnedBatch>, Status> {
        let Some(schema) = self.schema.as_ref().filter(|schema| !schema.schema.fields().is_empty()) else { return Ok(None); };
        let mut scratch = self.memory.reserve(mul(schema_bound(&schema.schema)?, 2)?).map_err(status)?;
        let grant = Arc::get_mut(&mut scratch).expect("empty upload owns its grant");
        let batch = RecordBatch::new_empty(Arc::clone(&schema.schema));
        OwnedBatch::with_shared_buffers(batch, grant, Arc::clone(&schema.reservation), |_| false).map(Some).map_err(status)
    }

    fn funds(&self, buffer: BufferIdentity) -> bool {
        self.dictionaries.values().any(|dictionary| dictionary.funds(buffer))
    }

    fn dictionary_refs(&self, grant: &mut MemoryReservation, extra: usize) -> Result<HashMap<i64, ArrayRef>, Status> {
        let scratch = self.dictionaries.values().try_fold(extra, |bytes, dictionary| {
            add(bytes, add(128, mul(dictionary.descriptor_bytes(), 4)?)?)
        })?;
        grant.grow_to(add(grant.bytes(), scratch)?).map_err(status)?;
        Ok(self.dictionaries.iter().map(|(&id, dictionary)| (id, Arc::clone(dictionary.array()))).collect())
    }

    pub fn decode(&mut self, data: FlightData) -> Result<Option<OwnedBatch>, Status> {
        if data.data_header.is_empty() { return Ok(None); }
        let message = arrow_ipc::root_as_message(&data.data_header).map_err(arrow)?;
        let schema = self.schema.as_ref().map(|schema| schema.schema.as_ref());
        let (bound, codec) = message_bound(&message, data.data_header.len(), &data.data_body, schema)?;
        let mut charge = self.memory.reserve(bound).map_err(status)?;
        let grant = Arc::get_mut(&mut charge).expect("Flight message owns its grant");
        let codec_workspace = codec.map(|codec| self.memory.reserve(codec_workspace(codec))).transpose().map_err(status)?;
        if codec == Some(arrow_ipc::CompressionType::LZ4_FRAME) {
            validate_lz4_lengths(&message, &data.data_body)?;
        }
        match message.header_type() {
            arrow_ipc::MessageHeader::NONE => Ok(None),
            arrow_ipc::MessageHeader::Schema => {
                if self.schema.is_some() { return Err(invalid("Flight upload contains more than one schema")); }
                self.schema = Some(UploadSchema::decode(&data.data_header, grant)?);
                Ok(None)
            }
            arrow_ipc::MessageHeader::DictionaryBatch => {
                let schema = self.schema.as_ref().ok_or_else(|| invalid("Dictionary arrived before schema"))?;
                let dictionary = message.header_as_dictionary_batch().ok_or_else(|| invalid("Invalid dictionary message"))?;
                let id = dictionary.id();
                let is_delta = dictionary.isDelta();
                let dictionaries = self.dictionary_refs(grant, schema.reservation.bytes())?;
                let values = dictionary::decode(dictionary, &schema.schema, &dictionaries, data.data_body, &message.version())?;
                drop(codec_workspace);
                drop(dictionaries);
                let incoming = RetainedArray::new(values, grant, |buffer| self.funds(buffer)).map_err(status)?;
                drop(data.data_header);
                drop(charge);
                let owner = if is_delta {
                    let current = self.dictionaries.get(&id).ok_or_else(|| invalid(format!("No existing dictionary for delta dictionary with id '{id}'")))?;
                    dictionary::append(current, &incoming, &self.memory, |buffer| self.funds(buffer))?
                } else {
                    incoming
                };
                self.dictionaries.insert(id, owner);
                Ok(None)
            }
            arrow_ipc::MessageHeader::RecordBatch => {
                let schema = self.schema.as_ref().ok_or_else(|| invalid("Record batch arrived before schema"))?;
                let batch = message.header_as_record_batch().ok_or_else(|| arrow(arrow_schema::ArrowError::ParseError(
                    "Unable to convert flight data header to a record batch".to_owned(),
                )))?;
                let dictionaries = self.dictionary_refs(grant, 0)?;
                let body = rtdb_format::ipc::align_ipc_body(Buffer::from(data.data_body));
                let batch = arrow_ipc::reader::read_record_batch(
                    &body, batch, Arc::clone(&schema.schema), &dictionaries, None, &message.version(),
                ).map_err(arrow)?;
                OwnedBatch::with_shared_buffers(batch, grant, Arc::clone(&schema.reservation), |buffer| self.funds(buffer))
                    .map(Some).map_err(status)
            }
            _ => Err(invalid("Unexpected Arrow Flight payload")),
        }
    }
}
fn message_batch<'a>(message: &arrow_ipc::Message<'a>) -> Option<arrow_ipc::RecordBatch<'a>> {
    message.header_as_record_batch().or_else(|| message.header_as_dictionary_batch().and_then(|dictionary| dictionary.data()))
}

fn message_bound(
    message: &arrow_ipc::Message<'_>, header_len: usize, body: &[u8], schema: Option<&Schema>,
) -> Result<(usize, Option<arrow_ipc::CompressionType>), Status> {
    if let Some(schema) = message.header_as_schema() { rtdb_format::ipc::validate_decimal_metadata(schema).map_err(status)?; }
    let declared_body = body.get(..usize::try_from(message.bodyLength()).map_err(arrow)?)
        .ok_or_else(|| invalid("Arrow message body is truncated"))?;
    let mut bound = add(8192, mul(header_len, 64)?)?;
    let Some(batch) = message_batch(message) else { return Ok((bound, None)); };
    // A misaligned body can require a full copy rounded to 64 bytes by Arrow.
    bound = add(bound, add(body.len(), 63)?)?;

    let compression = batch.compression().map(|compression| {
        if compression.method() != arrow_ipc::BodyCompressionMethod::BUFFER {
            return Err(invalid("Unknown Arrow compression method"));
        }
        match compression.codec() {
            codec @ (arrow_ipc::CompressionType::LZ4_FRAME | arrow_ipc::CompressionType::ZSTD) => Ok(codec),
            _ => Err(invalid("Unknown Arrow compression codec")),
        }
    }).transpose()?;
    let nodes = batch.nodes().ok_or_else(|| invalid("Arrow batch has no field nodes"))?;
    let buffers = batch.buffers().ok_or_else(|| invalid("Arrow batch has no buffers"))?;
    let rows = usize::try_from(batch.length()).map_err(arrow)?;
    let mut codec = None;
    let mut lengths = buffers.iter().map(|buffer| {
        let bytes = message_buffer(declared_body, buffer)?;
        let decoded = if compression.is_some() && !bytes.is_empty() {
            match compressed_length(bytes)? {
                -1 => bytes.len() - 8,
                0 => 0,
                len => {
                    codec = compression;
                    usize::try_from(len).map_err(arrow)?
                }
            }
        } else {
            bytes.len()
        };
        // Decompressed storage, a possible alignment copy, and allocation rounding.
        bound = add(bound, add(mul(decoded, 4)?, 512)?)?;
        Ok(decoded)
    });

    let flat_schema = schema.filter(|schema| {
        message.header_type() == arrow_ipc::MessageHeader::RecordBatch && !schema.fields().is_empty()
            && schema.fields().iter().all(|field| flat_layout(field.data_type()).is_some())
    });
    let descriptor_bound = if let Some(schema) = flat_schema {
        if nodes.len() != schema.fields().len() || batch.variadicBufferCounts().is_some_and(|counts| !counts.is_empty()) {
            return Err(invalid("Arrow flat batch does not match its schema"));
        }
        let mut descriptors = 0;
        for (field, node) in schema.fields().iter().zip(nodes) {
            let nulls = usize::try_from(node.null_count()).map_err(arrow)?;
            if usize::try_from(node.length()).map_err(arrow)? != rows || nulls > rows {
                return Err(invalid("Arrow flat field node does not match its batch length"));
            }
            let bitmap = rows.div_ceil(8);
            require_buffer(&mut lengths, if nulls == 0 { 0 } else { bitmap })?;
            match flat_layout(field.data_type()).expect("flat schema checked above") {
                FlatLayout::Bits => { require_buffer(&mut lengths, bitmap)?; }
                FlatLayout::Fixed(width) => { require_buffer(&mut lengths, mul(rows, width)?)?; }
                FlatLayout::Offsets(width) => {
                    let offsets = require_buffer(&mut lengths, if rows == 0 { 0 } else { mul(add(rows, 1)?, width)? })?;
                    if offsets % width != 0 { return Err(invalid("Arrow flat offsets do not match their width")); }
                    require_buffer(&mut lengths, 0)?;
                }
            }
            // Flat arrays and their ownership wrappers allocate descriptors per field.
            descriptors = add(descriptors, add(4096, mul(field.size(), 8)?)?)?;
        }
        if lengths.len() != 0 { return Err(invalid("Arrow flat batch has extra buffers")); }
        descriptors
    } else {
        for length in lengths { length?; }
        let nodes_bound = nodes.iter().try_fold(0, |total, node| {
            add(total, mul(add(usize::try_from(node.length()).map_err(arrow)?, 1)?, 4096)?)
        })?;
        // A batch can declare logical rows without field nodes, as with an empty schema.
        nodes_bound.max(mul(add(rows, 1)?, 4096)?)
    };
    Ok((add(bound, descriptor_bound)?, codec))
}

enum FlatLayout { Bits, Fixed(usize), Offsets(usize) }

fn flat_layout(data_type: &DataType) -> Option<FlatLayout> {
    match data_type {
        DataType::Boolean => Some(FlatLayout::Bits),
        DataType::Binary | DataType::Utf8 => Some(FlatLayout::Offsets(4)),
        DataType::LargeBinary | DataType::LargeUtf8 => Some(FlatLayout::Offsets(8)),
        DataType::FixedSizeBinary(width) if *width > 0 => Some(FlatLayout::Fixed(*width as usize)),
        data_type => data_type.primitive_width().map(FlatLayout::Fixed),
    }
}

fn require_buffer(lengths: &mut impl Iterator<Item = Result<usize, Status>>, minimum: usize) -> Result<usize, Status> {
    let length = lengths.next().ok_or_else(|| invalid("Arrow flat batch is missing a buffer"))??;
    if length < minimum { return Err(invalid("Arrow flat buffer is shorter than its layout requires")); }
    Ok(length)
}

fn message_buffer<'a>(body: &'a [u8], buffer: &arrow_ipc::Buffer) -> Result<&'a [u8], Status> {
    let start = usize::try_from(buffer.offset()).map_err(arrow)?;
    let len = usize::try_from(buffer.length()).map_err(arrow)?;
    body.get(start..add(start, len)?).ok_or_else(|| invalid("Arrow buffer exceeds message body"))
}

fn compressed_length(bytes: &[u8]) -> Result<i64, Status> {
    let prefix = bytes.get(..8).ok_or_else(|| invalid("Compressed Arrow buffer has no length"))?;
    Ok(i64::from_le_bytes(prefix.try_into().map_err(arrow)?))
}

fn codec_workspace(codec: arrow_ipc::CompressionType) -> usize {
    match codec {
        arrow_ipc::CompressionType::LZ4_FRAME => 64 << 20,
        arrow_ipc::CompressionType::ZSTD => 1 << 20,
        _ => unreachable!("compression codec checked before reservation"),
    }
}

fn validate_lz4_lengths(message: &arrow_ipc::Message<'_>, body: &[u8]) -> Result<(), Status> {
    use std::io::BufRead;

    let buffers = message_batch(message).and_then(|batch| batch.buffers()).expect("LZ4 buffers checked before reservation");
    for buffer in buffers {
        let bytes = message_buffer(body, buffer)?;
        if bytes.is_empty() { continue; }
        let expected = compressed_length(bytes)?;
        if expected <= 0 { continue; }
        let expected = usize::try_from(expected).map_err(arrow)?;

        // Arrow's LZ4 decoder grows its output before it checks the declared length.
        let mut decoder = lz4_flex::frame::FrameDecoder::new(&bytes[8..]);
        let mut decoded = 0;
        loop {
            let block = decoder.fill_buf().map_err(arrow)?.len();
            if block == 0 { break; }
            decoded = add(decoded, block)?;
            if decoded > expected { break; }
            decoder.consume(block);
        }
        if decoded != expected {
            return Err(invalid("LZ4 data differs from its declared Arrow buffer length"));
        }
    }
    Ok(())
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
    _schema: Option<Arc<MemoryReservation>>,
    query_types: OnceCell<ParameterTypes>,
}

impl BoundParameters {
    pub fn new(batch: OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>, Status> {
        let schema = memory.reserve(schema_bound(batch.batch().schema_ref())?).map_err(status)?;
        let (batch, schema) = match rtdb_format::ipc::adapt_stream_batch(batch.batch()).map_err(status)? {
            Some(adapted) => (batch.with_retained_buffers(adapted), Some(schema)),
            None if batch.owns_schema() => (batch, None),
            None => (batch, Some(schema)),
        };
        Ok(Arc::new(Self { batch, _schema: schema, query_types: OnceCell::new() }))
    }

    #[cfg(test)]
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

    pub async fn query_types(&self, memory: &Arc<MemoryBudget>) -> Result<&[LogicalType], Status> {
        let types = self.query_types.get_or_try_init(|| async {
            let mut types = ParameterTypes::from_batch(self.batch(), memory)?;
            let spare = types.types.capacity() - types.types.len();
            let retained = types.types.iter().try_fold(add(256, mul(spare, std::mem::size_of::<LogicalType>())?)?, |bytes, ty| {
                add(bytes, ty.retained_bytes().map_err(status)?)
            })?;
            let grant = Arc::get_mut(&mut types.reservation).expect("parameter types own their grant");
            if retained <= grant.bytes() {
                grant.shrink_to(retained).map_err(status)?;
            } else {
                grant.grow_to(retained).map_err(status)?;
            }
            Ok::<_, Status>(types)
        }).await?;
        Ok(types.as_slice())
    }

    pub async fn validated_query_types(&self, memory: &Arc<MemoryBudget>) -> Result<&[LogicalType], Status> {
        let types = self.query_types(memory).await?;
        validate_argument_columns(self.batch(), types, memory)?;
        Ok(types)
    }

    pub fn first_query_row(&self, types: &[LogicalType], memory: &Arc<MemoryBudget>) -> Result<(Vec<Value>, Arc<MemoryReservation>), Status> {
        let batch = self.batch();
        if batch.num_rows() == 0 { return Err(invalid("Query parameter batch has no rows")); }
        let columns = || batch.schema_ref().fields().iter().zip(batch.columns());
        let bound = columns().try_fold(mul(types.len(), std::mem::size_of::<Value>())?, |bytes, (field, array)| {
            add(bytes, rtdb_columnar::estimate_read_value_bytes(field, array.as_ref(), 0).map_err(status)?)
        })?;
        let charge = memory.reserve(bound).map_err(status)?;
        let mut row = Vec::with_capacity(types.len());
        for ((field, array), ty) in columns().zip(types) {
            row.push(rtdb_columnar::read_value_as(ty, field, array.as_ref(), 0).map_err(status)?);
        }
        row.iter_mut().try_for_each(Value::admit_object_dates).map_err(status)?;
        Ok((row, charge))
    }

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
        let (decode_bound, codec) = message_bound(&message, header.len(), body, None)?;
        bound = add(bound, add(decode_bound, codec.map_or(0, codec_workspace))?)?;
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
    reservation: Arc<MemoryReservation>,
}
impl ParameterTypes {
    pub fn from_batch(batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<Self, Status> {
        let schema = batch.schema_ref();
        let reservation = memory.reserve(schema_bound(schema)?).map_err(status)?;
        Ok(Self { types: logical_parameter_types(schema)?, reservation })
    }

    pub fn as_slice(&self) -> &[LogicalType] { &self.types }
}

pub(crate) fn arguments(batch: &OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<SqlArguments, Status> {
    let record = batch.batch();
    let _check = memory.reserve(schema_bound(record.schema_ref())?).map_err(status)?;
    let arguments = SqlArguments::from_arrow(batch.clone(), memory).map_err(status)?;
    validate_argument_columns(record, arguments.types(), memory)?;
    Ok(arguments)
}

fn validate_argument_columns(record: &RecordBatch, types: &[LogicalType], memory: &Arc<MemoryBudget>) -> Result<(), Status> {
    for ((field, array), logical) in record.schema_ref().fields().iter().zip(record.columns()).zip(types) {
        rtdb_columnar::validate_readable_column(field, array.as_ref(), logical, memory).map_err(status)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use rtdb_columnar::read_value;
    use rtdb_types::Value;
    use arrow_schema::Field;

    #[test]
    fn flight_upload_retains_record_and_dictionary_bodies() {
        use arrow_array::builder::StringDictionaryBuilder;
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int32Type;

        let mut strings = StringDictionaryBuilder::<Int32Type>::new();
        strings.append("one").unwrap();
        strings.append("two").unwrap();
        let batch = RecordBatch::try_from_iter([("label", Arc::new(strings.finish()) as ArrayRef)]).unwrap();
        let upload = MemoryBudget::new(1 << 20);
        let response = MemoryBudget::new(1 << 20);
        let mut tracker = DictionaryTracker::new(false);
        let options = IpcWriteOptions::default();
        let encoder = IpcDataGenerator::default();
        let schema = encoder.schema_to_bytes_with_dictionary_tracker(batch.schema_ref(), &mut tracker, &options);
        let mut decoder = Decoder::new(Arc::clone(&response));
        assert!(decoder.decode(schema.into()).unwrap().is_none());
        let (dictionaries, record) = encoder.encode(&batch, &mut tracker, &options, &mut CompressionContext::default()).unwrap();
        let mut frames = dictionaries.into_iter().chain(std::iter::once(record)).map(|encoded| {
            let mut data: FlightData = encoded.into();
            let charge = upload.reserve(data.data_body.len()).unwrap();
            data.data_body = retain(data.data_body, charge);
            data
        });
        let dictionary = frames.next().unwrap();
        let dictionary_body = dictionary.data_body.as_ptr_range();
        assert!(decoder.decode(dictionary).unwrap().is_none());
        let record = frames.next().unwrap();
        let record_body = record.data_body.as_ptr_range();
        let decoded = decoder.decode(record).unwrap().unwrap();
        drop(decoder);

        let retained = decoded.batch().column(0).clone();
        drop(decoded);
        let array = retained.as_dictionary::<Int32Type>();
        assert!(record_body.contains(&array.keys().values().as_ptr().cast()));
        assert!(dictionary_body.contains(&array.values().as_string::<i32>().value_data().as_ptr()));
        assert_eq!(retained.as_ref(), batch.column(0).as_ref());
        assert!(response.used_bytes() > 0);
        drop(retained);
        assert_eq!(upload.used_bytes(), 0);
        assert_eq!(response.used_bytes(), 0);
    }

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

    #[tokio::test]
    async fn ticket_arguments_reject_an_unreadable_value_after_the_first_row() {
        let memory = MemoryBudget::new(1 << 20);
        let field = rtdb_columnar::to_field("arg", &LogicalType::Scalar(rtdb_types::ScalarType::Uri)).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(arrow_array::StringArray::from(vec!["https://xtdb.com", "bad uri"]))],
        ).unwrap();
        let bytes = parameter_ipc(&batch, &memory).unwrap().unwrap();
        let bound = BoundParameters::new(parameter_batch(&bytes, &memory).unwrap(), &memory).unwrap();

        assert!(bound.query_types(&memory).await.is_ok());
        let error = bound.validated_query_types(&memory).await.unwrap_err();

        let bytes = error.metadata().get_bin("xtdb-anomaly-bin").unwrap().to_bytes().unwrap();
        let anomaly: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(anomaly["code"], "xtdb/invalid-uri");
    }

    #[tokio::test]
    async fn query_types_retry_after_memory_pressure_and_stay_with_the_bound_rows() {
        let memory = MemoryBudget::new(1 << 20);
        let schema = Arc::new(Schema::new(vec![Field::new("arg", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![None, Some(17)]))]).unwrap();
        let bound = BoundParameters::new(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();

        let obstruction = memory.reserve(memory.available_bytes()).unwrap();
        assert!(bound.query_types(&memory).await.is_err());
        drop(obstruction);

        let types = bound.query_types(&memory).await.unwrap();
        assert_eq!(types, &[LogicalType::Scalar(rtdb_types::ScalarType::I64).maybe()]);
        let obstruction = memory.reserve(memory.available_bytes()).unwrap();
        assert_eq!(bound.validated_query_types(&memory).await.unwrap(), types);
        drop(obstruction);

        let (row, row_charge) = bound.first_query_row(types, &memory).unwrap();
        assert_eq!(row, [Value::Null]);
        drop(row);
        drop(row_charge);
        drop(bound);
        assert_eq!(memory.used_bytes(), 0);
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
    fn flight_upload_admits_flat_int64_columns_under_an_eight_mib_budget() {
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![42; 65_536])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![43; 65_536])) as ArrayRef),
        ]).unwrap();
        let encoding = MemoryBudget::new(16 << 20);
        let mut encoder = Encoder::new(batch.schema_ref(), &encoding).unwrap();
        let data = encoder.batch(&batch, &encoding).unwrap().pop().unwrap();
        let mut decoder = Decoder::new(MemoryBudget::new(8 << 20));
        decoder.decode(schema_data(batch.schema_ref(), &encoding).unwrap()).unwrap();

        let decoded = decoder.decode(data).unwrap().unwrap();

        assert_eq!(decoded.batch(), &batch);
    }

    #[test]
    fn flight_upload_rejects_a_flat_value_buffer_shorter_than_the_declared_width() {
        let batch = RecordBatch::try_from_iter([
            ("value", Arc::new(arrow_array::Int8Array::from(vec![42; 64])) as ArrayRef),
        ]).unwrap();
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let memory = MemoryBudget::new(1 << 20);
        let mut encoder = Encoder::new(batch.schema_ref(), &memory).unwrap();
        let data = encoder.batch(&batch, &memory).unwrap().pop().unwrap();
        let mut decoder = Decoder::new(Arc::clone(&memory));
        decoder.decode(schema_data(&schema, &memory).unwrap()).unwrap();
        let peak = memory.peak_bytes();

        let error = decoder.decode(data).err().unwrap();

        assert_eq!(error.message(), "Arrow flat buffer is shorter than its layout requires");
        assert_eq!(memory.peak_bytes(), peak);
    }

    #[test]
    fn flight_upload_charges_nested_logical_rows_before_decoding() {
        let items = Arc::new(Int64Array::from(vec![42; 4096])) as ArrayRef;
        let nested = arrow_array::StructArray::from(vec![(Arc::new(Field::new("item", DataType::Int64, false)), items)]);
        let batch = RecordBatch::try_from_iter([("value", Arc::new(nested) as ArrayRef)]).unwrap();
        let encoding = MemoryBudget::new(1 << 20);
        let mut encoder = Encoder::new(batch.schema_ref(), &encoding).unwrap();
        let data = encoder.batch(&batch, &encoding).unwrap().pop().unwrap();
        let mut decoder = Decoder::new(MemoryBudget::new(1 << 20));
        decoder.decode(schema_data(batch.schema_ref(), &encoding).unwrap()).unwrap();

        let error = decoder.decode(data).err().unwrap();

        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    }

    #[test]
    fn flight_upload_accepts_lz4_and_rejects_an_understated_lz4_value_length() {
        use arrow_ipc::CompressionType;

        let text = "x".repeat(4096);
        let batch = RecordBatch::try_from_iter([
            ("value", Arc::new(arrow_array::StringArray::from(vec![text.as_str()])) as ArrayRef),
        ]).unwrap();
        let options = IpcWriteOptions::default().try_with_compression(Some(CompressionType::LZ4_FRAME)).unwrap();
        let mut tracker = DictionaryTracker::new(false);
        let (dictionaries, record) = IpcDataGenerator::default().encode(
            &batch, &mut tracker, &options, &mut CompressionContext::default(),
        ).unwrap();
        assert!(dictionaries.is_empty());
        let data: FlightData = record.into();
        let memory = MemoryBudget::new(128 << 20);
        let mut decoder = Decoder::new(Arc::clone(&memory));
        decoder.decode(schema_data(batch.schema_ref(), &memory).unwrap()).unwrap();
        assert_eq!(decoder.decode(data.clone()).unwrap().unwrap().batch(), &batch);

        let message = arrow_ipc::root_as_message(&data.data_header).unwrap();
        let values = message.header_as_record_batch().unwrap().buffers().unwrap().get(2);
        let offset = values.offset() as usize;
        let mut body = data.data_body.to_vec();
        assert_eq!(compressed_length(&body[offset..]).unwrap(), 4096);
        body[offset..offset + 8].copy_from_slice(&8_i64.to_le_bytes());
        let error = decoder.decode(FlightData { data_body: body.into(), ..data }).err().unwrap();

        assert_eq!(error.message(), "LZ4 data differs from its declared Arrow buffer length");
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
