use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::collections::HashMap;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::Buffer;
use arrow_ipc::reader::FileDecoder;
use arrow_ipc::writer::{DictionaryTracker, FileWriter, IpcDataGenerator, IpcWriteOptions, StreamWriter, write_message};
use arrow_schema::{Schema, SchemaRef};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, Result};

pub struct ArrowFile {
    reader: Box<dyn RetainedReader>,
    decoder: FileDecoder,
    schema: SchemaRef,
    blocks: Vec<arrow_ipc::Block>,
    next_page: usize,
    encoded_page_sizes: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileLayout {
    pub footer_encoded_bytes: usize,
    pub dictionary_encoded_bytes: usize,
    pub page_encoded_bytes: Vec<usize>,
    pub uncompressed: bool,
}

trait RetainedReader: Read + Seek + Send {}
impl<T: Read + Seek + Send> RetainedReader for T {}

impl ArrowFile {
    pub fn open(bytes: Arc<[u8]>, projection: Option<Vec<usize>>) -> Result<Self> {
        Self::from_reader(Cursor::new(bytes), projection)
    }

    pub fn from_reader(reader: impl Read + Seek + Send + 'static, projection: Option<Vec<usize>>) -> Result<Self> {
        let mut reader: Box<dyn RetainedReader> = Box::new(reader);
        let layout = inspect_file(reader.as_mut())?;
        Self::from_inspected_reader(reader, projection, layout)
    }

    /// The layout must describe this immutable reader, as returned by `inspect_file`.
    /// The caller retains a decoding reservation through this reader and every returned buffer or schema.
    pub fn from_inspected_reader(reader: impl Read + Seek + Send + 'static, projection: Option<Vec<usize>>, layout: FileLayout) -> Result<Self> {
        let mut reader: Box<dyn RetainedReader> = Box::new(reader);
        reader.seek(SeekFrom::End(-10)).map_err(invalid)?;
        let mut tail = [0; 10]; reader.read_exact(&mut tail).map_err(invalid)?;
        let length = arrow_ipc::reader::read_footer_length(tail).map_err(invalid)?;
        reader.seek(SeekFrom::End(-10 - i64::try_from(length).map_err(invalid)?)).map_err(invalid)?;
        let mut bytes = vec![0; length]; reader.read_exact(&mut bytes).map_err(invalid)?;
        let footer = arrow_ipc::root_as_footer(&bytes).map_err(invalid)?;
        let ipc_schema = footer.schema().ok_or_else(|| invalid("Arrow footer has no schema"))?;
        if !ipc_schema.endianness().equals_to_target_endianness() { return Err(invalid("Arrow file endianness differs from the host")); }
        let full_schema = Arc::new(crate::ipc_decimal::decode(ipc_schema)?);
        let schema = if let Some(projection) = &projection { Arc::new(full_schema.project(projection).map_err(invalid)?) } else { Arc::clone(&full_schema) };
        let mut decoder = FileDecoder::new(full_schema, footer.version());
        if let Some(projection) = projection { decoder = decoder.with_projection(projection); }
        if let Some(dictionaries) = footer.dictionaries() {
            for block in dictionaries { let buffer = read_block(reader.as_mut(), block)?; decoder.read_dictionary(block, &buffer).map_err(invalid)?; }
        }
        let blocks = footer.recordBatches().ok_or_else(|| invalid("Arrow footer has no record-batch inventory"))?.iter().copied().collect();
        Ok(Self { reader, decoder, schema, blocks, next_page: 0, encoded_page_sizes: layout.page_encoded_bytes })
    }

    pub fn schema(&self) -> SchemaRef { Arc::clone(&self.schema) }
    pub fn page_count(&self) -> usize { self.blocks.len() }

    /// Encoded metadata and body bytes; compressed pages can require a larger decoded allocation.
    pub fn encoded_page_size(&self, page: usize) -> Result<usize> {
        self.encoded_page_sizes.get(page).copied().ok_or_else(|| invalid("Arrow page exceeds its file"))
    }

    /// Returned batches retain their buffers independently of this reader.
    pub fn read_page(&mut self, page: usize) -> Result<RecordBatch> {
        if page >= self.page_count() {
            return Err(invalid(format!("Page {page} is outside a {}-page file", self.page_count())));
        }
        let block = &self.blocks[page];
        let buffer = read_block(self.reader.as_mut(), block)?;
        let batch = self.decoder.read_record_batch(block, &buffer).map_err(invalid)?.ok_or_else(|| invalid("Arrow file ended before its declared page"))?;
        self.next_page = page + 1;
        Ok(batch)
    }
}

impl Iterator for ArrowFile {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_page >= self.page_count() { return None; }
        let page = self.next_page; self.next_page += 1;
        Some(self.read_page(page))
    }
}
fn read_block(reader: &mut dyn RetainedReader, block: &arrow_ipc::Block) -> Result<Buffer> {
    let size = allocation_add(usize::try_from(block.metaDataLength()).map_err(invalid)?, usize::try_from(block.bodyLength()).map_err(invalid)?)?;
    reader.seek(SeekFrom::Start(u64::try_from(block.offset()).map_err(invalid)?)).map_err(invalid)?;
    let mut bytes = vec![0; size]; reader.read_exact(&mut bytes).map_err(invalid)?;
    Ok(Buffer::from(bytes))
}

pub fn inspect_file<R: Read + Seek + ?Sized>(reader: &mut R) -> Result<FileLayout> {
    inspect_file_reserved(reader, |_| Ok(()))
}

/// Acquires a caller-owned guard before each footer or message allocation and retains it while those bytes are inspected.
pub fn inspect_file_reserved<R: Read + Seek + ?Sized, G>(reader: &mut R, mut reserve: impl FnMut(usize) -> Result<G>) -> Result<FileLayout> {
    let end = reader.seek(SeekFrom::End(0)).map_err(invalid)?;
    if end < 10 { return Err(invalid("Arrow file is shorter than its footer")); }
    reader.seek(SeekFrom::End(-10)).map_err(invalid)?;
    let mut tail = [0u8; 10];
    reader.read_exact(&mut tail).map_err(invalid)?;
    let footer_length = arrow_ipc::reader::read_footer_length(tail).map_err(invalid)?;
    let footer_start = end.checked_sub(10 + footer_length as u64).ok_or_else(|| invalid("Arrow footer exceeds its file"))?;
    let _footer = reserve(footer_length.checked_mul(2).and_then(|size| size.checked_add(65_536)).ok_or_else(|| invalid("Arrow inspection allocation overflow"))?)?;
    reader.seek(SeekFrom::Start(footer_start)).map_err(invalid)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(footer_length).map_err(invalid)?;
    bytes.resize(footer_length, 0);
    reader.read_exact(&mut bytes).map_err(invalid)?;
    let footer = arrow_ipc::root_as_footer(&bytes).map_err(invalid)?;
    validate_decimal_metadata(footer.schema().ok_or_else(|| invalid("Arrow footer has no schema"))?)?;
    let size = |block: &arrow_ipc::Block| -> Result<usize> {
        let metadata = usize::try_from(block.metaDataLength()).map_err(invalid)?;
        let body = usize::try_from(block.bodyLength()).map_err(invalid)?;
        metadata.checked_add(body).ok_or_else(|| invalid("Arrow page length overflow"))
    };
    let mut layout = FileLayout { footer_encoded_bytes: footer_length + 10, dictionary_encoded_bytes: 0, page_encoded_bytes: Vec::new(), uncompressed: true };
    for block in footer.dictionaries().into_iter().flat_map(|blocks| blocks.iter()).chain(footer.recordBatches().into_iter().flat_map(|blocks| blocks.iter())) {
        let offset = u64::try_from(block.offset()).map_err(invalid)?;
        let block_size = size(block)?;
        if offset.checked_add(block_size as u64).is_none_or(|end| end > footer_start) { return Err(invalid("Arrow block exceeds data region")); }
        let metadata_length = usize::try_from(block.metaDataLength()).map_err(invalid)?;
        let _message = reserve(metadata_length)?;
        let mut metadata = vec![0; metadata_length];
        reader.seek(SeekFrom::Start(offset)).map_err(invalid)?;
        reader.read_exact(&mut metadata).map_err(invalid)?;
        let prefix = if metadata.starts_with(&[0xff; 4]) { 8 } else { 4 };
        let message = arrow_ipc::root_as_message(metadata.get(prefix..).ok_or_else(|| invalid("Arrow message has no body"))?).map_err(invalid)?;
        if let Some(batch) = message.header_as_record_batch() { layout.uncompressed &= batch.compression().is_none(); }
        else if let Some(dictionary) = message.header_as_dictionary_batch() {
            layout.uncompressed &= dictionary.data().ok_or_else(|| invalid("Arrow dictionary has no batch"))?.compression().is_none();
        } else { return Err(invalid("Arrow data block has no record batch")); }
    }
    if let Some(dictionaries) = footer.dictionaries() {
        for block in dictionaries { layout.dictionary_encoded_bytes = layout.dictionary_encoded_bytes.checked_add(size(block)?).ok_or_else(|| invalid("Arrow dictionary size overflow"))?; }
    }
    if let Some(blocks) = footer.recordBatches() { layout.page_encoded_bytes = blocks.iter().map(size).collect::<Result<_>>()?; }
    Ok(layout)
}

pub struct ArrowStream<R = Cursor<Arc<[u8]>>> {
    reader: R,
    schema: SchemaRef,
    projected: SchemaRef,
    projection: Option<Vec<usize>>,
    dictionaries: HashMap<i64, ArrayRef>,
    finished: bool,
}

impl ArrowStream<Cursor<Arc<[u8]>>> {
    pub fn open(bytes: Arc<[u8]>, projection: Option<Vec<usize>>) -> Result<Self> {
        Self::from_reader(Cursor::new(bytes), projection)
    }
}
impl<R: Read> ArrowStream<R> {
    /// The caller reserves schema, dictionaries and the largest decoded message before opening the reader.
    /// Returned buffers and schemas may outlive the reader; retain their reservation until they are released.
    pub fn from_reader(mut reader: R, projection: Option<Vec<usize>>) -> Result<Self> {
        let (metadata, body) = read_message(&mut reader)?.ok_or_else(|| invalid("Arrow stream has no schema"))?;
        if !body.is_empty() { return Err(invalid("Arrow schema has a body")); }
        let schema = Arc::new(read_schema_header(&metadata)?);
        let projected = if let Some(projection) = &projection { Arc::new(schema.project(projection).map_err(invalid)?) } else { Arc::clone(&schema) };
        Ok(Self { reader, schema, projected, projection, dictionaries: HashMap::new(), finished: false })
    }
    pub fn schema(&self) -> SchemaRef { Arc::clone(&self.projected) }
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        while let Some((metadata, body)) = read_message(&mut self.reader)? {
            let message = arrow_ipc::root_as_message(&metadata).map_err(invalid)?;
            let body = Buffer::from(body);
            if let Some(batch) = message.header_as_record_batch() {
                return arrow_ipc::reader::read_record_batch(&body, batch, Arc::clone(&self.schema), &self.dictionaries, self.projection.as_deref(), &message.version()).map(Some).map_err(invalid);
            }
            if let Some(dictionary) = message.header_as_dictionary_batch() {
                arrow_ipc::reader::read_dictionary(&body, dictionary, &self.schema, &mut self.dictionaries, &message.version()).map_err(invalid)?;
            } else { return Err(invalid("Unexpected Arrow stream message")); }
        }
        Ok(None)
    }
}
impl<R: Read> Iterator for ArrowStream<R> {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished { return None; }
        match self.next_batch() { Ok(Some(batch)) => Some(Ok(batch)), Ok(None) => { self.finished = true; None }, Err(error) => { self.finished = true; Some(Err(error)) } }
    }
}

fn read_message(reader: &mut impl Read) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    let mut prefix = [0; 4];
    if reader.read(&mut prefix[..1]).map_err(invalid)? == 0 { return Ok(None); }
    reader.read_exact(&mut prefix[1..]).map_err(invalid)?;
    let mut length = i32::from_le_bytes(prefix);
    if length == -1 { reader.read_exact(&mut prefix).map_err(invalid)?; length = i32::from_le_bytes(prefix); }
    if length == 0 { return Ok(None); }
    let mut metadata = vec![0; usize::try_from(length).map_err(invalid)?]; reader.read_exact(&mut metadata).map_err(invalid)?;
    let message = arrow_ipc::root_as_message(&metadata).map_err(invalid)?;
    let mut body = vec![0; usize::try_from(message.bodyLength()).map_err(invalid)?]; reader.read_exact(&mut body).map_err(invalid)?;
    Ok(Some((metadata, body)))
}

pub fn read_stream(bytes: Arc<[u8]>) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let reader = ArrowStream::open(bytes, None)?;
    let schema = reader.schema();
    Ok((schema, reader.collect::<Result<Vec<_>>>()?))
}

/// Bounds message decoding scratch, including declared row descriptors and compressed buffer lengths.
/// Reserve the returned byte count before invoking the Arrow reader and retain it through decoded-buffer use.
pub fn message_allocation_bound(header: &[u8], body: &[u8]) -> Result<usize> {
    let message = arrow_ipc::root_as_message(header).map_err(invalid)?;
    if let Some(schema) = message.header_as_schema() { validate_decimal_metadata(schema)?; }
    let mut bound = allocation_add(8192, allocation_add(allocation_mul(header.len(), 64)?, allocation_mul(body.len(), 4)?)?)?;
    if usize::try_from(message.bodyLength()).map_err(invalid)? > body.len() { return Err(invalid("Arrow message body is truncated")); }
    if let Some(batch) = message.header_as_record_batch().or_else(|| message.header_as_dictionary_batch().and_then(|batch| batch.data())) {
        if let Some(nodes) = batch.nodes() {
            for node in nodes { bound = allocation_add(bound, allocation_mul(allocation_add(usize::try_from(node.length()).map_err(invalid)?, 1)?, 4096)?)?; }
        }
        if let Some(buffers) = batch.buffers() {
            for buffer in buffers {
                let start = usize::try_from(buffer.offset()).map_err(invalid)?;
                let length = usize::try_from(buffer.length()).map_err(invalid)?;
                let bytes = body.get(start..allocation_add(start, length)?).ok_or_else(|| invalid("Arrow buffer exceeds its message body"))?;
                let decoded = if batch.compression().is_some() && !bytes.is_empty() {
                    let prefix: [u8; 8] = bytes.get(..8).ok_or_else(|| invalid("Compressed Arrow buffer has no length"))?.try_into().map_err(invalid)?;
                    let length = i64::from_le_bytes(prefix);
                    if length == -1 { bytes.len() - 8 } else { usize::try_from(length).map_err(invalid)? }
                } else { length };
                bound = allocation_add(bound, allocation_add(allocation_mul(decoded, 4)?, 512)?)?;
            }
        }
    }
    Ok(bound)
}

/// Bounds all messages retained by a decoded IPC stream, including dictionaries.
/// The encoded input has a separate lifetime and must retain its own allocation charge.
pub fn stream_allocation_bound(bytes: &[u8]) -> Result<usize> {
    let mut position = 0;
    let mut bound = 8192;
    while position < bytes.len() {
        let mut length = inspection_i32(bytes, &mut position)?;
        if length == -1 { length = inspection_i32(bytes, &mut position)?; }
        if length == 0 { break; }
        let end = allocation_add(position, usize::try_from(length).map_err(invalid)?)?;
        let header = bytes.get(position..end).ok_or_else(|| invalid("Arrow stream metadata is truncated"))?;
        let message = arrow_ipc::root_as_message(header).map_err(invalid)?;
        let body_end = allocation_add(end, usize::try_from(message.bodyLength()).map_err(invalid)?)?;
        let body = bytes.get(end..body_end).ok_or_else(|| invalid("Arrow stream body is truncated"))?;
        bound = allocation_add(bound, message_allocation_bound(header, body)?)?;
        position = body_end;
    }
    Ok(bound)
}

/// Bounds the footer, dictionaries and all record batches decoded from an immutable IPC file.
/// The encoded input has a separate lifetime and must retain its own allocation charge.
pub fn file_allocation_bound(bytes: &[u8]) -> Result<usize> {
    let tail = bytes.get(bytes.len().checked_sub(10).ok_or_else(|| invalid("Arrow file is shorter than its footer"))?..).ok_or_else(|| invalid("Arrow footer is truncated"))?;
    let footer_length = arrow_ipc::reader::read_footer_length(tail.try_into().map_err(invalid)?).map_err(invalid)?;
    let footer_start = bytes.len().checked_sub(allocation_add(10, footer_length)?).ok_or_else(|| invalid("Arrow footer exceeds its file"))?;
    let footer = arrow_ipc::root_as_footer(&bytes[footer_start..bytes.len() - 10]).map_err(invalid)?;
    validate_decimal_metadata(footer.schema().ok_or_else(|| invalid("Arrow footer has no schema"))?)?;
    let mut bound = allocation_add(allocation_mul(footer_length, 64)?, 65_536)?;
    for block in footer.dictionaries().into_iter().flat_map(|blocks| blocks.iter()).chain(footer.recordBatches().into_iter().flat_map(|blocks| blocks.iter())) {
        let start = usize::try_from(block.offset()).map_err(invalid)?;
        let metadata_end = allocation_add(start, usize::try_from(block.metaDataLength()).map_err(invalid)?)?;
        let body_end = allocation_add(metadata_end, usize::try_from(block.bodyLength()).map_err(invalid)?)?;
        if body_end > footer_start { return Err(invalid("Arrow block exceeds its data region")); }
        let mut header_start = start;
        let mut length = inspection_i32(bytes, &mut header_start)?;
        if length == -1 { length = inspection_i32(bytes, &mut header_start)?; }
        let header_end = allocation_add(header_start, usize::try_from(length).map_err(invalid)?)?;
        if header_end > metadata_end { return Err(invalid("Arrow block header exceeds its metadata")); }
        bound = allocation_add(bound, message_allocation_bound(&bytes[header_start..header_end], &bytes[metadata_end..body_end])?)?;
    }
    Ok(bound)
}

fn inspection_i32(bytes: &[u8], position: &mut usize) -> Result<i32> {
    let end = allocation_add(*position, 4)?;
    let value = i32::from_le_bytes(bytes.get(*position..end).ok_or_else(|| invalid("Arrow IPC prefix is truncated"))?.try_into().map_err(invalid)?);
    *position = end;
    Ok(value)
}
fn allocation_add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(|| invalid("Arrow allocation exceeds addressable memory")) }
fn allocation_mul(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(|| invalid("Arrow allocation exceeds addressable memory")) }

/// Bounds schema clones, decimal translation tables and encoded schema/footer scratch without allocating.
/// Reserve before encoding, and retain the guard until the derived schema objects are released.
pub fn schema_translation_bound(schema: &Schema) -> Result<usize> { Ok(schema_metrics(schema)?.0) }
fn dictionary_count(schema: &Schema) -> Result<usize> { Ok(schema_metrics(schema)?.1) }
fn schema_metrics(schema: &Schema) -> Result<(usize, usize)> {
    fn metadata(entries: &HashMap<String, String>) -> Result<usize> {
        entries.iter().try_fold(0, |bytes, (key, value)| allocation_add(bytes, allocation_add(512, allocation_mul(allocation_add(key.len(), value.len())?, 16)?)?))
    }
    fn field(value: &arrow_schema::Field, depth: usize) -> Result<(usize, usize)> {
        let (bytes, dictionaries) = data_type(value.data_type(), depth)?;
        Ok((allocation_add(bytes, allocation_add(allocation_mul(value.name().len(), 16)?, metadata(value.metadata())?)?)?, dictionaries))
    }
    fn data_type(value: &arrow_schema::DataType, depth: usize) -> Result<(usize, usize)> {
        use arrow_schema::DataType;
        if depth > 64 { return Err(invalid("Arrow schema exceeds the encoding depth limit")); }
        let mut bytes = 4096; let mut dictionaries = 0;
        let mut add = |(next_bytes, next_dictionaries)| -> Result<()> { bytes = allocation_add(bytes, next_bytes)?; dictionaries = allocation_add(dictionaries, next_dictionaries)?; Ok(()) };
        match value {
            DataType::Struct(fields) => for value in fields { add(field(value, depth + 1)?)?; },
            DataType::Union(fields, _) => for (_, value) in fields.iter() { add(field(value, depth + 1)?)?; },
            DataType::List(value) | DataType::LargeList(value) | DataType::ListView(value) | DataType::LargeListView(value) | DataType::FixedSizeList(value, _) | DataType::Map(value, _) => add(field(value, depth + 1)?)?,
            DataType::RunEndEncoded(ends, values) => { add(field(ends, depth + 1)?)?; add(field(values, depth + 1)?)?; }
            DataType::Dictionary(key, value) => { add(data_type(key, depth + 1)?)?; add(data_type(value, depth + 1)?)?; add((0, 1))?; }
            DataType::Timestamp(_, Some(zone)) => add((allocation_mul(zone.len(), 16)?, 0))?,
            _ => {}
        }
        Ok((bytes, dictionaries))
    }
    schema.fields().iter().try_fold((allocation_add(65_536, metadata(schema.metadata())?)?, 0), |(bytes, dictionaries), value| {
        let (next_bytes, next_dictionaries) = field(value, 0)?;
        Ok((allocation_add(bytes, next_bytes)?, allocation_add(dictionaries, next_dictionaries)?))
    })
}

pub fn write_file(schema: &Schema, pages: &[RecordBatch]) -> Result<Vec<u8>> {
    let decimal_schema = crate::ipc_decimal::DecimalSchema::new(schema)?;
    for page in pages { decimal_schema.validate_batch(page)?; }
    let mut writer = FileWriter::try_new_with_options(Vec::new(), &decimal_schema.wire, write_options()?).map_err(invalid)?;
    for page in pages { writer.write(page).map_err(invalid)?; }
    writer.finish().map_err(invalid)?;
    let mut bytes = writer.into_inner().map_err(invalid)?;
    let footer_length_at = bytes.len().checked_sub(10).ok_or_else(|| invalid("Arrow writer omitted its footer"))?;
    let footer_length = i32::from_le_bytes(bytes[footer_length_at..footer_length_at + 4].try_into().map_err(invalid)?);
    let footer_length = usize::try_from(footer_length).map_err(invalid)?;
    let footer_start = footer_length_at.checked_sub(footer_length).ok_or_else(|| invalid("Arrow footer exceeds its file"))?;
    decimal_schema.patch_footer(&mut bytes[footer_start..footer_length_at])?;
    patch_schema_frame(&mut bytes[8..], &decimal_schema)?;
    jvm_end_marker(&mut bytes, footer_start)?;
    Ok(bytes)
}

pub struct ArrowFileWriter { writer: FileWriter<LimitedFile>, decimal_schema: crate::ipc_decimal::DecimalSchema, allocation: WriterAllocation }

enum WriterAllocation {
    CallerReserved,
    Owned { memory: Arc<MemoryBudget>, _schema: Arc<MemoryReservation>, blocks: Vec<Arc<MemoryReservation>>, block_bound: usize },
}

struct LimitedFile { file: File, position: u64, limit: u64 }

impl Write for LimitedFile {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.position.checked_add(bytes.len() as u64).is_none_or(|position| position > self.limit) {
            return Err(std::io::Error::other("Arrow output byte limit exceeded"));
        }
        let count = self.file.write(bytes)?;
        self.position += count as u64;
        Ok(count)
    }
    fn flush(&mut self) -> std::io::Result<()> { self.file.flush() }
}

impl ArrowFileWriter {
    /// The file must be empty and positioned at offset zero.
    /// The caller reserves schema translation, Arrow encoding and footer scratch through `finish`.
    pub fn new(file: File, schema: &Schema) -> Result<Self> {
        Self::new_bounded(file, schema, u64::MAX)
    }

    /// Rejects a write before the file would exceed `byte_limit`, including its footer.
    /// The caller reserves schema translation, Arrow encoding and footer scratch through `finish`.
    pub fn new_bounded(mut file: File, schema: &Schema, byte_limit: u64) -> Result<Self> {
        if file.metadata().map_err(invalid)?.len() != 0 || file.stream_position().map_err(invalid)? != 0 { return Err(invalid("Arrow output file must be empty and positioned at zero")); }
        let decimal_schema = crate::ipc_decimal::DecimalSchema::new(schema)?;
        let writer = FileWriter::try_new_with_options(LimitedFile { file, position: 0, limit: byte_limit }, &decimal_schema.wire, write_options()?).map_err(invalid)?;
        Ok(Self { writer, decimal_schema, allocation: WriterAllocation::CallerReserved })
    }

    /// Reserves schema translation, block inventory and footer copies for this writer's lifetime.
    /// The file must be empty at offset zero; callers separately reserve each page's encoding scratch.
    pub fn new_reserved(file: File, schema: &Schema, memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::new_bounded_reserved(file, schema, u64::MAX, memory)
    }

    /// Applies both the output-byte limit and the shared schema/footer memory budget.
    /// The file must be empty at offset zero; callers separately reserve each page's encoding scratch.
    pub fn new_bounded_reserved(file: File, schema: &Schema, byte_limit: u64, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(schema_translation_bound(schema)?)?;
        let block_bound = allocation_mul(allocation_add(dictionary_count(schema)?, 1)?, 512)?;
        let mut writer = Self::new_bounded(file, schema, byte_limit)?;
        writer.allocation = WriterAllocation::Owned { memory: Arc::clone(memory), _schema: reservation, blocks: Vec::new(), block_bound };
        Ok(writer)
    }

    pub fn bytes_written(&mut self) -> u64 { self.writer.get_mut().position }

    /// The owner may lower the remaining allowance between writes; already-written bytes must still fit.
    pub fn set_byte_limit(&mut self, byte_limit: u64) -> Result<()> {
        let sink = self.writer.get_mut();
        if sink.position > byte_limit { return Err(invalid("Arrow output already exceeds the requested byte limit")); }
        sink.limit = byte_limit;
        Ok(())
    }

    pub fn write(&mut self, page: &RecordBatch) -> Result<()> {
        self.decimal_schema.validate_batch(page)?;
        if let WriterAllocation::Owned { memory, blocks, block_bound, .. } = &mut self.allocation { blocks.push(memory.reserve(*block_bound)?); }
        self.writer.write(page).map_err(invalid)
    }

    /// Finishes the JVM-compatible end marker and returns the caller-owned file at its end.
    pub fn finish(mut self) -> Result<File> {
        let _footer = match &self.allocation {
            WriterAllocation::CallerReserved => None,
            WriterAllocation::Owned { memory, _schema, blocks, block_bound } => Some(memory.reserve(allocation_add(_schema.bytes(), allocation_mul(blocks.len(), *block_bound)?)?)?),
        };
        self.writer.finish().map_err(invalid)?;
        let mut file = self.writer.into_inner().map_err(invalid)?.file;
        patch_file_schemas(&mut file, &self.decimal_schema)?;
        let end = file.seek(SeekFrom::End(0)).map_err(invalid)?;
        file.seek(SeekFrom::End(-10)).map_err(invalid)?;
        let mut tail = [0; 10];
        file.read_exact(&mut tail).map_err(invalid)?;
        let length = arrow_ipc::reader::read_footer_length(tail).map_err(invalid)? as u64;
        let footer_start = end.checked_sub(length + 10).ok_or_else(|| invalid("Arrow footer exceeds its file"))?;
        let marker = footer_start.checked_sub(8).ok_or_else(|| invalid("Arrow file has no end marker"))?;
        file.seek(SeekFrom::Start(marker)).map_err(invalid)?;
        let mut expected = [0; 8];
        file.read_exact(&mut expected).map_err(invalid)?;
        if expected != [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0] { return Err(invalid("Arrow writer produced an unexpected end marker")); }
        file.seek(SeekFrom::Start(marker)).map_err(invalid)?;
        file.write_all(&[0; 4]).map_err(invalid)?;
        let mut buffer = [0; 8192];
        let mut position = footer_start;
        while position < end {
            let count = usize::try_from((end - position).min(buffer.len() as u64)).map_err(invalid)?;
            file.seek(SeekFrom::Start(position)).map_err(invalid)?;
            file.read_exact(&mut buffer[..count]).map_err(invalid)?;
            file.seek(SeekFrom::Start(position - 4)).map_err(invalid)?;
            file.write_all(&buffer[..count]).map_err(invalid)?;
            position += count as u64;
        }
        file.set_len(end - 4).map_err(invalid)?;
        file.seek(SeekFrom::End(0)).map_err(invalid)?;
        file.flush().map_err(invalid)?;
        Ok(file)
    }
}

pub fn write_stream(schema: &Schema, pages: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut bytes = write_arrow_stream(schema, pages, write_options()?)?;
    let end = bytes.len();
    jvm_end_marker(&mut bytes, end)?;
    Ok(bytes)
}

/// Encodes a standard Arrow stream, retaining the caller's alignment and end-marker convention.
/// The caller reserves schema translation and encoding scratch, and retains the returned bytes' allocation charge.
pub fn write_arrow_stream(schema: &Schema, pages: &[RecordBatch], options: IpcWriteOptions) -> Result<Vec<u8>> {
    let decimal_schema = crate::ipc_decimal::DecimalSchema::new(schema)?;
    for page in pages { decimal_schema.validate_batch(page)?; }
    let mut writer = StreamWriter::try_new_with_options(Vec::new(), &decimal_schema.wire, options).map_err(invalid)?;
    for page in pages { writer.write(page).map_err(invalid)?; }
    writer.finish().map_err(invalid)?;
    let mut bytes = writer.into_inner().map_err(invalid)?;
    patch_schema_frame(&mut bytes, &decimal_schema)?;
    Ok(bytes)
}

/// Writes pages without buffering the complete stream; the sink owns and enforces its allocation bound.
/// The caller reserves the schema translation and initial encoded schema frame through this call.
/// A page with different native field metadata is rejected before encoding; the sink may already contain a valid prefix.
pub fn write_stream_to<'a, W: Write>(schema: &Schema, pages: impl IntoIterator<Item = &'a RecordBatch>, sink: W) -> Result<W> {
    let decimal_schema = crate::ipc_decimal::DecimalSchema::new(schema)?;
    let sink = FirstSchemaWriter { inner: sink, schema: &decimal_schema, pending: Vec::new(), expected: None, complete: false };
    let mut writer = StreamWriter::try_new_with_options(TailWriter { inner: sink, pending: Vec::with_capacity(8) }, &decimal_schema.wire, write_options()?).map_err(invalid)?;
    for page in pages { decimal_schema.validate_batch(page)?; writer.write(page).map_err(invalid)?; }
    writer.finish().map_err(invalid)?;
    let mut sink = writer.into_inner().map_err(invalid)?;
    if sink.pending.as_slice() != [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0] { return Err(invalid("Arrow writer produced an unexpected end marker")); }
    sink.inner.write_all(&[0; 4]).map_err(invalid)?;
    sink.inner.flush().map_err(invalid)?;
    if !sink.inner.complete { return Err(invalid("Arrow stream ended before its schema")); }
    Ok(sink.inner.inner)
}

struct TailWriter<W> { inner: W, pending: Vec<u8> }

impl<W: Write> Write for TailWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let count = self.pending.len().saturating_add(bytes.len()).saturating_sub(8);
        let old = count.min(self.pending.len());
        self.inner.write_all(&self.pending[..old])?;
        self.pending.drain(..old);
        self.inner.write_all(&bytes[..count - old])?;
        self.pending.extend_from_slice(&bytes[count - old..]);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { self.inner.flush() }
}

pub fn read_schema_message(bytes: &[u8]) -> Result<Schema> {
    crate::ipc_decimal::decode(schema_message_metadata(bytes)?)
}

/// Decodes an unframed IPC Schema message with full-width decimal scales.
/// The caller reserves metadata scratch before calling this function.
pub fn read_schema_header(bytes: &[u8]) -> Result<Schema> {
    crate::ipc_decimal::decode(arrow_ipc::root_as_message(bytes).map_err(invalid)?.header_as_schema().ok_or_else(|| invalid("Arrow message has no schema"))?)
}

/// Checks external decimal precision, bit width, endianness and reserved metadata recursively.
/// Every signed 32-bit scale is accepted; coefficients and other Arrow kinds are checked by their readers.
pub fn validate_decimal_metadata(schema: arrow_ipc::Schema<'_>) -> Result<()> {
    crate::ipc_decimal::validate(schema)
}

fn schema_message_metadata(bytes: &[u8]) -> Result<arrow_ipc::Schema<'_>> {
    let mut position = 0;
    let mut length = inspection_i32(bytes, &mut position)?;
    if length == -1 { length = inspection_i32(bytes, &mut position)?; }
    let end = allocation_add(position, usize::try_from(length).map_err(invalid)?)?;
    let header = bytes.get(position..end).ok_or_else(|| invalid("Arrow schema message is truncated"))?;
    arrow_ipc::root_as_message(header).map_err(invalid)?.header_as_schema().ok_or_else(|| invalid("Arrow message has no schema"))
}

pub fn write_schema_message(schema: &Schema) -> Result<Vec<u8>> {
    write_schema_message_with_options(schema, &write_options()?)
}

/// Encodes a framed schema using the caller's Arrow alignment and legacy framing options.
/// The caller reserves codec scratch and retains the returned bytes' allocation charge.
pub fn write_schema_message_with_options(schema: &Schema, options: &IpcWriteOptions) -> Result<Vec<u8>> {
    let decimal_schema = crate::ipc_decimal::DecimalSchema::new(schema)?;
    let mut encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(&decimal_schema.wire, &mut DictionaryTracker::new(true), options);
    decimal_schema.patch_message(&mut encoded.ipc_message)?;
    let mut bytes = Vec::new();
    write_message(&mut bytes, encoded, options).map_err(invalid)?;
    Ok(bytes)
}

/// Encodes an unframed IPC Schema message with genuine signed 32-bit decimal metadata.
/// The caller reserves codec scratch and retains the returned bytes' allocation charge.
pub fn write_schema_header(schema: &Schema) -> Result<Vec<u8>> {
    write_schema_header_with_options(schema, &write_options()?)
}

/// Encodes an unframed schema using the caller's Arrow metadata version.
/// The caller reserves codec scratch and retains the returned bytes' allocation charge.
pub fn write_schema_header_with_options(schema: &Schema, options: &IpcWriteOptions) -> Result<Vec<u8>> {
    let decimal_schema = crate::ipc_decimal::DecimalSchema::new(schema)?;
    let mut encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(&decimal_schema.wire, &mut DictionaryTracker::new(true), options);
    decimal_schema.patch_message(&mut encoded.ipc_message)?;
    Ok(encoded.ipc_message)
}

fn patch_schema_frame(bytes: &mut [u8], schema: &crate::ipc_decimal::DecimalSchema) -> Result<()> {
    let mut position = 0; let mut length = inspection_i32(bytes, &mut position)?;
    if length == -1 { length = inspection_i32(bytes, &mut position)?; }
    let end = allocation_add(position, usize::try_from(length).map_err(invalid)?)?;
    schema.patch_message(bytes.get_mut(position..end).ok_or_else(|| invalid("Arrow schema frame is truncated"))?)
}
fn patch_file_schemas(file: &mut File, schema: &crate::ipc_decimal::DecimalSchema) -> Result<()> {
    file.seek(SeekFrom::Start(8)).map_err(invalid)?;
    let mut prefix = [0; 4]; file.read_exact(&mut prefix).map_err(invalid)?;
    let mut length = i32::from_le_bytes(prefix);
    if length == -1 { file.read_exact(&mut prefix).map_err(invalid)?; length = i32::from_le_bytes(prefix); }
    let start = file.stream_position().map_err(invalid)?;
    let mut bytes = vec![0; usize::try_from(length).map_err(invalid)?]; file.read_exact(&mut bytes).map_err(invalid)?;
    schema.patch_message(&mut bytes)?;
    file.seek(SeekFrom::Start(start)).map_err(invalid)?; file.write_all(&bytes).map_err(invalid)?;
    drop(bytes);
    file.seek(SeekFrom::End(-10)).map_err(invalid)?;
    let mut tail = [0; 10]; file.read_exact(&mut tail).map_err(invalid)?;
    let length = arrow_ipc::reader::read_footer_length(tail).map_err(invalid)?;
    file.seek(SeekFrom::End(-10 - i64::try_from(length).map_err(invalid)?)).map_err(invalid)?;
    let start = file.stream_position().map_err(invalid)?;
    let mut bytes = vec![0; length]; file.read_exact(&mut bytes).map_err(invalid)?;
    schema.patch_footer(&mut bytes)?;
    file.seek(SeekFrom::Start(start)).map_err(invalid)?; file.write_all(&bytes).map_err(invalid)?;
    Ok(())
}

struct FirstSchemaWriter<'a, W> { inner: W, schema: &'a crate::ipc_decimal::DecimalSchema, pending: Vec<u8>, expected: Option<usize>, complete: bool }
impl<W: Write> Write for FirstSchemaWriter<'_, W> {
    fn write(&mut self, mut bytes: &[u8]) -> std::io::Result<usize> {
        let length = bytes.len();
        while !bytes.is_empty() {
            if self.complete { self.inner.write_all(bytes)?; break; }
            let target = self.expected.unwrap_or(8);
            let count = (target - self.pending.len()).min(bytes.len());
            self.pending.extend_from_slice(&bytes[..count]); bytes = &bytes[count..];
            if self.pending.len() == target {
                if self.expected.is_none() {
                    let mut position = 0;
                    let mut size = inspection_i32(&self.pending, &mut position).map_err(std::io::Error::other)?;
                    if size == -1 { size = inspection_i32(&self.pending, &mut position).map_err(std::io::Error::other)?; }
                    self.expected = Some(allocation_add(position, usize::try_from(size).map_err(std::io::Error::other)?).map_err(std::io::Error::other)?);
                    if self.expected.is_some_and(|end| end < self.pending.len()) { return Err(std::io::Error::other("Invalid Arrow schema frame")); }
                }
                if self.expected == Some(self.pending.len()) {
                    patch_schema_frame(&mut self.pending, self.schema).map_err(std::io::Error::other)?;
                    self.inner.write_all(&self.pending)?; self.pending.clear(); self.complete = true;
                }
            }
        }
        Ok(length)
    }
    fn flush(&mut self) -> std::io::Result<()> { self.inner.flush() }
}

fn write_options() -> Result<IpcWriteOptions> {
    IpcWriteOptions::try_new(8, false, arrow_ipc::MetadataVersion::V5).map_err(invalid)
}

fn jvm_end_marker(bytes: &mut Vec<u8>, end: usize) -> Result<()> {
    let marker = end.checked_sub(8).ok_or_else(|| invalid("Arrow writer omitted its end marker"))?;
    if bytes.get(marker..end) != Some(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]) {
        return Err(invalid("Arrow writer produced an unexpected end marker"));
    }
    // ArrowUnloader.Mode.STREAM ends with one zero i32, also immediately before a file footer.
    bytes.drain(marker..marker + 4);
    Ok(())
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-arrow", error.to_string())
}
