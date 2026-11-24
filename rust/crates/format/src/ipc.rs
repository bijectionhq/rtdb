use std::borrow::Cow;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::collections::HashMap;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::Buffer;
use arrow_ipc::reader::FileDecoder;
use arrow_ipc::writer::{
    CompressionContext, DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions,
    StreamWriter, write_message,
};
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef, UnionMode};
use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_types::{Anomaly, Result};

mod decoded_schema;
mod event_projection;
mod java_layout;
mod nulls;
mod projection;
mod undefined;

pub struct ArrowFile {
    reader: Box<dyn RetainedReader>,
    event_projection: Option<event_projection::EventProjection>,
    pages: PageDecoder,
    next_page: usize,
    encoded_page_sizes: Vec<usize>,
    page_decoding_bounds: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileLayout {
    pub footer_encoded_bytes: usize,
    pub dictionary_encoded_bytes: usize,
    pub page_encoded_bytes: Vec<usize>,
    pub dictionary_decoding_bound: usize,
    pub page_decoding_bounds: Vec<usize>,
    pub uncompressed: bool,
}

impl FileLayout {
    /// Reserve this bound while the reader, its dictionaries or its schema remain reachable.
    pub fn opening_allocation_bound(&self) -> Result<usize> {
        allocation_add(allocation_add(allocation_mul(self.footer_encoded_bytes, 16)?, 65_536)?, self.dictionary_decoding_bound)
    }

    /// Reserve this bound while retaining every decoded batch, dictionary and schema from the file.
    pub fn full_allocation_bound(&self) -> Result<usize> {
        self.page_decoding_bounds.iter().try_fold(self.opening_allocation_bound()?, |bytes, page| allocation_add(bytes, *page))
    }
}

/// A file of `length` bytes of which only some ranges are at hand, added as a reader needs them; reading outside them fails.
/// Clones share the ranges, so a caller can add a page's range to a reader it handed to an `ArrowFile`.
#[derive(Clone)]
pub struct SparseFile { length: u64, position: u64, ranges: Arc<std::sync::Mutex<Vec<(u64, Arc<dyn AsRef<[u8]> + Send + Sync>)>>> }
impl SparseFile {
    pub fn new(length: u64) -> Self { Self { length, position: 0, ranges: Default::default() } }
    /// Makes `bytes` the file's contents from `start`; a later range shadows an earlier one where they overlap.
    pub fn add(&self, start: u64, bytes: Arc<dyn AsRef<[u8]> + Send + Sync>) {
        if let Ok(mut ranges) = self.ranges.lock() { ranges.push((start, bytes)); }
    }
    /// Keeps only the ranges at and after `keep`, so that pages read before it release their bytes.
    pub fn retain_from(&self, keep: usize) {
        if let Ok(mut ranges) = self.ranges.lock() { let drop = ranges.len().saturating_sub(keep); ranges.drain(..drop); }
    }
}
impl Read for SparseFile {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() || self.position >= self.length { return Ok(0); }
        let ranges = self.ranges.lock().map_err(|_| std::io::Error::other("Sparse file ranges are poisoned"))?;
        let found = ranges.iter().rev().find(|(start, bytes)| *start <= self.position && self.position < start + bytes.as_ref().as_ref().len() as u64);
        let Some((start, bytes)) = found else { return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Read outside the ranges fetched of a sparse file")) };
        let bytes = bytes.as_ref().as_ref();
        let offset = (self.position - start) as usize;
        let count = buffer.len().min(bytes.len() - offset);
        buffer[..count].copy_from_slice(&bytes[offset..offset + count]);
        self.position += count as u64;
        Ok(count)
    }
}
impl Seek for SparseFile {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        let next = match position {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::End(offset) => self.length.checked_add_signed(offset),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
        }.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Seek outside a sparse file"))?;
        self.position = next;
        Ok(next)
    }
}

trait RetainedReader: Read + Seek + Send {
    fn block_slice(&self, _range: std::ops::Range<usize>) -> Option<Buffer> { None }
}

struct StreamReader<R>(R);
impl<R: Read> Read for StreamReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> { self.0.read(bytes) }
}
impl<R: Seek> Seek for StreamReader<R> {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> { self.0.seek(position) }
}
impl<R: Read + Seek + Send> RetainedReader for StreamReader<R> {}

struct BufferedReader {
    buffer: Buffer,
    position: u64,
}
impl Read for BufferedReader {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let mut cursor = Cursor::new(self.buffer.as_slice());
        cursor.set_position(self.position);
        let read = cursor.read(bytes)?;
        self.position = cursor.position();
        Ok(read)
    }
}
impl Seek for BufferedReader {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        let mut cursor = Cursor::new(self.buffer.as_slice());
        cursor.set_position(self.position);
        self.position = cursor.seek(position)?;
        Ok(self.position)
    }
}
impl RetainedReader for BufferedReader {
    fn block_slice(&self, range: std::ops::Range<usize>) -> Option<Buffer> {
        (range.end <= self.buffer.len()).then(|| {
            let view = self.buffer.slice_with_length(range.start, range.len());
            rtdb_columnar::retain_buffer_with_owner(&view, Arc::new(()))
        })
    }
}

impl ArrowFile {
    pub fn open(bytes: Arc<[u8]>, projection: Option<Vec<usize>>) -> Result<Self> {
        let pointer = std::ptr::NonNull::new(bytes.as_ptr().cast_mut()).expect("Arc slice pointer is non-null");
        let length = bytes.len();
        // SAFETY: the owner holds the immutable slice at this address until the last Buffer is dropped.
        let buffer = unsafe { Buffer::from_custom_allocation(pointer, length, Arc::new(bytes)) };
        Self::from_buffer(buffer, projection)
    }

    pub fn from_buffer(buffer: Buffer, projection: Option<Vec<usize>>) -> Result<Self> {
        let mut reader = BufferedReader { buffer, position: 0 };
        let layout = inspect_file(&mut reader)?;
        Self::from_inspected_buffer(reader.buffer, projection, layout)
    }

    pub fn from_inspected_buffer(buffer: Buffer, projection: Option<Vec<usize>>, layout: FileLayout) -> Result<Self> {
        Self::open_inspected(Box::new(BufferedReader { buffer, position: 0 }), projection, layout)
    }

    pub fn from_reader(mut reader: impl Read + Seek + Send + 'static, projection: Option<Vec<usize>>) -> Result<Self> {
        let layout = inspect_file(&mut reader)?;
        Self::from_inspected_reader(reader, projection, layout)
    }

    /// The layout must describe this immutable reader, as returned by `inspect_file`.
    /// The caller retains a decoding reservation through this reader and every returned buffer or schema.
    pub fn from_inspected_reader(reader: impl Read + Seek + Send + 'static, projection: Option<Vec<usize>>, layout: FileLayout) -> Result<Self> {
        Self::open_inspected(Box::new(StreamReader(reader)), projection, layout)
    }

    fn open_inspected(mut reader: Box<dyn RetainedReader>, projection: Option<Vec<usize>>, layout: FileLayout) -> Result<Self> {
        reader.seek(SeekFrom::End(-10)).map_err(invalid)?;
        let mut tail = [0; 10]; reader.read_exact(&mut tail).map_err(invalid)?;
        let length = arrow_ipc::reader::read_footer_length(tail).map_err(invalid)?;
        reader.seek(SeekFrom::End(-10 - i64::try_from(length).map_err(invalid)?)).map_err(invalid)?;
        let mut bytes = vec![0; length]; reader.read_exact(&mut bytes).map_err(invalid)?;
        let data_end = reader.stream_position().map_err(invalid)?.checked_sub(length as u64).ok_or_else(|| invalid("Arrow footer exceeds its file"))?;
        let mut pages = PageDecoder::from_footer(&bytes, data_end, projection)?;
        for index in 0..pages.dictionary_count() {
            let block = pages.dictionaries[index];
            let bytes = read_block(reader.as_mut(), &block)?;
            pages.load_dictionary(index, bytes)?;
        }
        Ok(Self { reader, event_projection: None, pages, next_page: 0, encoded_page_sizes: layout.page_encoded_bytes, page_decoding_bounds: layout.page_decoding_bounds })
    }

    pub fn schema(&self) -> SchemaRef {
        self.event_projection.as_ref().map_or_else(|| self.pages.schema(), |projection| Arc::clone(&projection.schema))
    }

    /// Selects event headers, the document ID, and the named direct children of the put struct.
    pub fn project_event_payload(&mut self, columns: &[String]) {
        if self.pages.dictionary_count() == 0 && self.pages.schema.fields().len() == self.pages.source_schema.fields().len() {
            self.event_projection = event_projection::EventProjection::new(&self.pages.schema, columns);
        }
    }
    pub fn page_count(&self) -> usize { self.pages.blocks.len() }

    /// Encoded metadata and body bytes; compressed pages can require a larger decoded allocation.
    pub fn encoded_page_size(&self, page: usize) -> Result<usize> {
        self.encoded_page_sizes.get(page).copied().ok_or_else(|| invalid("Arrow page exceeds its file"))
    }

    /// Reserve before reading the page and retain the charge through the returned buffers' lifetime.
    pub fn page_allocation_bound(&self, page: usize) -> Result<usize> {
        self.page_decoding_bounds.get(page).copied().ok_or_else(|| invalid("Arrow page exceeds its file"))
    }

    /// Returned batches retain their buffers independently of this reader.
    /// The bytes of the file that hold `page`, which `read_page` reads and nothing else.
    pub fn page_range(&self, page: usize) -> Result<std::ops::Range<u64>> {
        self.pages.page_range(page)
    }

    pub fn read_page(&mut self, page: usize) -> Result<RecordBatch> {
        let block = self.pages.block(page)?;
        let buffer = read_block(self.reader.as_mut(), block)?;
        let batch = if let Some(projection) = &self.event_projection {
            let bytes = prepare_block(block, buffer, &self.pages.source_schema, false)?;
            let (header, body_start) = block_message(block, &bytes)?;
            let message = arrow_ipc::root_as_message(&bytes[header]).map_err(invalid)?;
            if (message.version() == self.pages.version || self.pages.version == arrow_ipc::MetadataVersion::V1)
                && let Some(batch) = projection.decode(&self.pages.source_schema, message, &bytes.slice(body_start)) {
                batch
            } else {
                // Reference-writer undefined unions need normalization by the full decoder.
                let batch = self.pages.decode_prepared(block, bytes)?;
                projection.select_decoded(&batch)?
            }
        } else {
            self.pages.decode_page(page, buffer)?
        };
        self.next_page = page + 1;
        Ok(batch)
    }

    /// Reads an event page with only the requested children of its put leg.
    pub fn read_event_page(&mut self, page: usize, put_columns: &[String]) -> Result<RecordBatch> {
        let buffer = read_block(self.reader.as_mut(), self.pages.block(page)?)?;
        let batch = self.pages.decode_event_page(page, buffer, put_columns)?;
        self.next_page = page + 1;
        Ok(batch)
    }

}

pub struct PageDecoder {
    decoder: FileDecoder,
    schema: SchemaRef,
    source_schema: SchemaRef,
    decoded_schema: SchemaRef,
    version: arrow_ipc::MetadataVersion,
    blocks: Vec<arrow_ipc::Block>,
    dictionaries: Vec<arrow_ipc::Block>,
}

impl PageDecoder {
    pub fn from_footer(bytes: &[u8], data_end: u64, projection: Option<Vec<usize>>) -> Result<Self> {
        let footer = arrow_ipc::root_as_footer(bytes).map_err(invalid)?;
        let ipc_schema = footer.schema().ok_or_else(|| invalid("Arrow footer has no schema"))?;
        if !ipc_schema.endianness().equals_to_target_endianness() { return Err(invalid("Arrow file endianness differs from the host")); }
        let source_schema = Arc::new(crate::ipc_decimal::decode(ipc_schema)?);
        let decoded = decoded_schema::adapt(&source_schema)?;
        let schema = if let Some(projection) = &projection { Arc::new(decoded.project(projection).map_err(invalid)?) } else { Arc::clone(&decoded) };
        // SAFETY: persisted blocks come from the database's Arrow writer. The reference writer's undefined union
        // cells use type ID -1. Decode checks each block's framing and repairs those cells before validating the page.
        let mut decoder = unsafe { FileDecoder::new(Arc::clone(&decoded), footer.version()).with_skip_validation(true) };
        if let Some(projection) = projection { decoder = decoder.with_projection(projection); }
        let blocks: Vec<_> = footer.recordBatches().ok_or_else(|| invalid("Arrow footer has no record-batch inventory"))?.iter().copied().collect();
        let dictionaries: Vec<_> = footer.dictionaries().into_iter().flat_map(|blocks| blocks.iter().copied()).collect();
        for block in blocks.iter().chain(&dictionaries) {
            let range = block_range(block)?;
            if range.start < 8 || range.end > data_end {
                return Err(invalid("Arrow block exceeds data region"));
            }
        }
        Ok(Self { decoder, schema, source_schema, decoded_schema: decoded, version: footer.version(), blocks, dictionaries })
    }

    pub fn schema(&self) -> SchemaRef { Arc::clone(&self.schema) }

    pub fn page_count(&self) -> usize { self.blocks.len() }

    fn block(&self, page: usize) -> Result<&arrow_ipc::Block> {
        self.blocks.get(page).ok_or_else(|| invalid(format!("Page {page} is outside its Arrow file")))
    }

    pub fn page_range(&self, page: usize) -> Result<std::ops::Range<u64>> {
        block_range(self.block(page)?)
    }

    pub fn page_metadata_range(&self, page: usize) -> Result<std::ops::Range<u64>> {
        let block = self.block(page)?;
        let start = u64::try_from(block.offset()).map_err(invalid)?;
        let length = u64::try_from(block.metaDataLength()).map_err(invalid)?;
        Ok(start..start.checked_add(length).ok_or_else(|| invalid("Arrow block end overflow"))?)
    }

    pub fn page_rows(&self, page: usize, metadata: &[u8]) -> Result<usize> {
        let block = self.block(page)?;
        if metadata.len() != usize::try_from(block.metaDataLength()).map_err(invalid)? {
            return Err(invalid("Arrow block differs from its footer length"));
        }
        let message = arrow_ipc::root_as_message(&metadata[block_header(metadata)?]).map_err(invalid)?;
        check_block_message(&message, usize::try_from(block.bodyLength()).map_err(invalid)?, false)?;
        let batch = message.header_as_record_batch().ok_or_else(|| invalid("Arrow file ended before its declared page"))?;
        usize::try_from(batch.length()).map_err(invalid)
    }

    pub fn dictionary_count(&self) -> usize { self.dictionaries.len() }

    fn dictionary(&self, index: usize) -> Result<&arrow_ipc::Block> {
        self.dictionaries.get(index).ok_or_else(|| invalid("Dictionary is outside its Arrow file"))
    }

    pub fn dictionary_range(&self, index: usize) -> Result<std::ops::Range<u64>> {
        block_range(self.dictionary(index)?)
    }

    pub fn page_bound(&self, page: usize, bytes: &[u8]) -> Result<usize> {
        block_bound(self.block(page)?, bytes, false)
    }

    pub fn dictionary_bound(&self, index: usize, bytes: &[u8]) -> Result<usize> {
        block_bound(self.dictionary(index)?, bytes, true)
    }

    pub fn load_dictionary(&mut self, index: usize, bytes: Buffer) -> Result<()> {
        let block = *self.dictionary(index)?;
        let bytes = prepare_block(&block, bytes, &self.source_schema, true)?;
        self.decoder.read_dictionary(&block, &bytes).map_err(invalid)
    }

    pub fn decode_iid_page(&self, page: usize, bytes: Buffer) -> Result<RecordBatch> {
        let iid = self.source_schema.index_of("_iid").map_err(crate::data::event_arrow_error)?;
        match self.decode_projected(page, &bytes, projection::Selection::Iid) {
            Ok(batch) => Ok(batch),
            Err(_) => self.decode_page(page, bytes)?.project(&[iid]).map_err(invalid),
        }
    }

    pub fn decode_event_page(&self, page: usize, bytes: Buffer, put_columns: &[String]) -> Result<RecordBatch> {
        match self.decode_projected(page, &bytes, projection::Selection::PutColumns(put_columns)) {
            Ok(batch) => Ok(batch),
            // Some reference layouts use undefined union legs or unknown null counts.
            Err(_) => self.decode_page(page, bytes),
        }
    }

    fn decode_projected(&self, page: usize, bytes: &Buffer, selection: projection::Selection<'_>) -> Result<RecordBatch> {
        let block = self.block(page)?;
        let (header, metadata_end) = block_message(block, bytes)?;
        let message = arrow_ipc::root_as_message(&bytes[header]).map_err(invalid)?;
        check_block_message(&message, bytes.len() - metadata_end, false)?;
        let batch = message.header_as_record_batch().ok_or_else(|| invalid("Arrow file ended before its declared page"))?;
        projection::decode(&bytes.slice(metadata_end), batch, &self.decoded_schema, message.version(), selection)
    }

    pub fn decode_page(&self, page: usize, bytes: Buffer) -> Result<RecordBatch> {
        let block = self.block(page)?;
        let bytes = prepare_block(block, bytes, &self.source_schema, false)?;
        self.decode_prepared(block, bytes)
    }

    fn decode_prepared(&self, block: &arrow_ipc::Block, bytes: Buffer) -> Result<RecordBatch> {
        let batch = self.decoder.read_record_batch(block, &bytes).map_err(invalid)?.ok_or_else(|| invalid("Arrow file ended before its declared page"))?;
        undefined::reinstate(batch)
    }
}

fn block_range(block: &arrow_ipc::Block) -> Result<std::ops::Range<u64>> {
    let start = u64::try_from(block.offset()).map_err(invalid)?;
    let length = u64::try_from(block.metaDataLength()).map_err(invalid)?.checked_add(u64::try_from(block.bodyLength()).map_err(invalid)?).ok_or_else(|| invalid("Arrow block length overflow"))?;
    Ok(start..start.checked_add(length).ok_or_else(|| invalid("Arrow block end overflow"))?)
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
    let range = block_range(block)?;
    let start = usize::try_from(range.start).map_err(invalid)?;
    let end = usize::try_from(range.end).map_err(invalid)?;
    if let Some(buffer) = reader.block_slice(start..end) { return Ok(buffer); }
    reader.seek(SeekFrom::Start(range.start)).map_err(invalid)?;
    let mut bytes = vec![0; usize::try_from(range.end - range.start).map_err(invalid)?];
    reader.read_exact(&mut bytes).map_err(invalid)?;
    Ok(Buffer::from(bytes))
}

fn block_message(block: &arrow_ipc::Block, bytes: &[u8]) -> Result<(std::ops::Range<usize>, usize)> {
    let metadata_end = usize::try_from(block.metaDataLength()).map_err(invalid)?;
    let body_length = usize::try_from(block.bodyLength()).map_err(invalid)?;
    if bytes.len() != allocation_add(metadata_end, body_length)? {
        return Err(invalid("Arrow block differs from its footer length"));
    }
    Ok((block_header(&bytes[..metadata_end])?, metadata_end))
}

fn block_header(metadata: &[u8]) -> Result<std::ops::Range<usize>> {
    let mut prefix = 0;
    let mut length = inspection_i32(metadata, &mut prefix)?;
    if length == -1 { length = inspection_i32(metadata, &mut prefix)?; }
    let header_end = allocation_add(prefix, usize::try_from(length).map_err(invalid)?)?;
    if header_end > metadata.len() { return Err(invalid("Arrow block header exceeds its metadata")); }
    Ok(prefix..header_end)
}

fn block_bound(block: &arrow_ipc::Block, bytes: &[u8], dictionary: bool) -> Result<usize> {
    let (header, metadata_end) = block_message(block, bytes)?;
    let message = arrow_ipc::root_as_message(&bytes[header.clone()]).map_err(invalid)?;
    check_block_message(&message, bytes.len() - metadata_end, dictionary)?;
    framed_message_bound(&bytes[header], &bytes[metadata_end..], metadata_end, arrow_descriptors)
}

fn check_block_message(message: &arrow_ipc::Message<'_>, body_length: usize, dictionary: bool) -> Result<()> {
    if usize::try_from(message.bodyLength()).map_err(invalid)? != body_length
        || if dictionary { message.header_as_dictionary_batch().is_none() } else { message.header_as_record_batch().is_none() } {
        return Err(invalid("Arrow block does not match its footer inventory"));
    }
    Ok(())
}

fn prepare_block(block: &arrow_ipc::Block, bytes: Buffer, schema: &Schema, dictionary: bool) -> Result<Buffer> {
    let (header, metadata_end) = block_message(block, &bytes)?;
    let message = arrow_ipc::root_as_message(&bytes[header.clone()]).map_err(invalid)?;
    check_block_message(&message, bytes.len() - metadata_end, dictionary)?;
    validate_lz4_lengths(&message, &bytes[metadata_end..])?;
    let batch = message.header_as_record_batch().or_else(|| message.header_as_dictionary_batch().and_then(|dictionary| dictionary.data()));
    let needs_normalization = batch.and_then(|batch| batch.nodes()).is_some_and(|nodes| nodes.iter().any(|node| node.null_count() < 0));
    if !needs_normalization { return Ok(bytes); }

    let mut owned = bytes.into_vec::<u8>().unwrap_or_else(|bytes| bytes.to_vec());
    let (metadata, body) = owned.split_at_mut(metadata_end);
    nulls::normalize(&mut metadata[header], body, schema)?;
    Ok(Buffer::from(owned))
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
    let mut layout = FileLayout { footer_encoded_bytes: footer_length + 10, dictionary_encoded_bytes: 0, page_encoded_bytes: Vec::new(), dictionary_decoding_bound: 0, page_decoding_bounds: Vec::new(), uncompressed: true };
    for (block, dictionary) in footer.dictionaries().into_iter().flat_map(|blocks| blocks.iter().map(|block| (block, true))).chain(footer.recordBatches().into_iter().flat_map(|blocks| blocks.iter().map(|block| (block, false)))) {
        let offset = u64::try_from(block.offset()).map_err(invalid)?;
        let block_size = size(block)?;
        if offset.checked_add(block_size as u64).is_none_or(|end| end > footer_start) { return Err(invalid("Arrow block exceeds data region")); }
        let metadata_length = usize::try_from(block.metaDataLength()).map_err(invalid)?;
        let _message = reserve(metadata_length)?;
        let mut metadata = vec![0; metadata_length];
        reader.seek(SeekFrom::Start(offset)).map_err(invalid)?;
        reader.read_exact(&mut metadata).map_err(invalid)?;
        let mut prefix = 0;
        let mut length = inspection_i32(&metadata, &mut prefix)?;
        if length == -1 { length = inspection_i32(&metadata, &mut prefix)?; }
        let header_end = allocation_add(prefix, usize::try_from(length).map_err(invalid)?)?;
        let header = metadata.get(prefix..header_end).ok_or_else(|| invalid("Arrow block header exceeds its metadata"))?;
        let message = arrow_ipc::root_as_message(header).map_err(invalid)?;
        if dictionary != message.header_as_dictionary_batch().is_some() { return Err(invalid("Arrow block type differs from its footer inventory")); }
        let body_length = usize::try_from(block.bodyLength()).map_err(invalid)?;
        if usize::try_from(message.bodyLength()).map_err(invalid)? != body_length { return Err(invalid("Arrow block and message body lengths differ")); }
        let body_start = offset.checked_add(metadata_length as u64).ok_or_else(|| invalid("Arrow body offset overflow"))?;
        let bound = message_bound(&message, metadata_length, body_length, arrow_descriptors, |start| {
            reader.seek(SeekFrom::Start(body_start.checked_add(start as u64).ok_or_else(|| invalid("Arrow buffer offset overflow"))?)).map_err(invalid)?;
            let mut bytes = [0; 8]; reader.read_exact(&mut bytes).map_err(invalid)?;
            Ok(i64::from_le_bytes(bytes))
        })?;
        if let Some(batch) = message.header_as_record_batch() {
            layout.uncompressed &= batch.compression().is_none();
            layout.page_encoded_bytes.push(block_size);
            layout.page_decoding_bounds.push(bound);
        } else if let Some(dictionary) = message.header_as_dictionary_batch() {
            layout.uncompressed &= dictionary.data().ok_or_else(|| invalid("Arrow dictionary has no batch"))?.compression().is_none();
            layout.dictionary_encoded_bytes = allocation_add(layout.dictionary_encoded_bytes, block_size)?;
            layout.dictionary_decoding_bound = allocation_add(layout.dictionary_decoding_bound, bound)?;
        } else { return Err(invalid("Arrow data block has no record batch")); }
    }
    Ok(layout)
}

enum StreamInput<R> {
    Reader(R),
    Retained { bytes: Bytes, position: usize },
}

impl<R: Read> StreamInput<R> {
    fn next_message(&mut self) -> Result<Option<(Cow<'_, [u8]>, Buffer)>> {
        match self {
            Self::Reader(reader) => Ok(read_message(reader)?.map(|(metadata, body)| {
                let body = if body.is_empty() { Buffer::default() } else { Buffer::from(body) };
                (Cow::Owned(metadata), body)
            })),
            Self::Retained { bytes, position } => {
                if *position == bytes.len() { return Ok(None); }
                let Some((metadata, body)) = stream_message(bytes, position)? else { return Ok(None); };
                let body = if body.is_empty() {
                    Buffer::default()
                } else {
                    let body = bytes.slice(body);
                    // Arrow's per-array alignment copies can change the batch's later memory charge.
                    if body.as_ptr().align_offset(std::mem::align_of::<u128>()) == 0 {
                        Buffer::from(body)
                    } else {
                        Buffer::from(body.to_vec())
                    }
                };
                Ok(Some((Cow::Borrowed(metadata), body)))
            }
        }
    }
}

pub struct ArrowStream<R = Cursor<Arc<[u8]>>> {
    input: StreamInput<R>,
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
    /// Decodes one batch at a time from admitted IPC bytes that own their allocation charge.
    /// Decoded buffers may retain the input bytes after the iterator is dropped.
    /// The caller retains the decoder reservation as in [`Self::from_reader`].
    pub fn from_bytes(bytes: Bytes, projection: Option<Vec<usize>>) -> Result<Self> {
        Self::from_input(StreamInput::Retained { bytes, position: 0 }, projection)
    }
}
impl<R: Read> ArrowStream<R> {
    /// The caller reserves schema, dictionaries and the largest decoded message before opening the reader.
    /// Returned buffers and schemas may outlive the reader; retain their reservation until they are released.
    pub fn from_reader(reader: R, projection: Option<Vec<usize>>) -> Result<Self> {
        Self::from_input(StreamInput::Reader(reader), projection)
    }
    fn from_input(mut input: StreamInput<R>, projection: Option<Vec<usize>>) -> Result<Self> {
        let schema = {
            let (metadata, body) = input.next_message()?.ok_or_else(|| invalid("Arrow stream has no schema"))?;
            if !body.is_empty() { return Err(invalid("Arrow schema has a body")); }
            decoded_schema::adapt(&Arc::new(read_schema_header(&metadata)?))?
        };
        let projected = if let Some(projection) = &projection { Arc::new(schema.project(projection).map_err(invalid)?) } else { Arc::clone(&schema) };
        Ok(Self { input, schema, projected, projection, dictionaries: HashMap::new(), finished: false })
    }
    pub fn schema(&self) -> SchemaRef { Arc::clone(&self.projected) }
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        while let Some((metadata, body)) = self.input.next_message()? {
            if let Some(batch) = decode_stream_batch(metadata, align_ipc_body(body), &self.schema, &mut self.dictionaries, self.projection.as_deref())? { return Ok(Some(batch)); }
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

/// Restores the writer schema of a page read from the same event spill.
/// The caller holds the decoded page and reserves scratch for the retyped descriptors.
pub fn restore_spill_page_schema(batch: &RecordBatch, schema: &SchemaRef) -> Result<Option<RecordBatch>> {
    if decoded_schema::adapt(schema)?.as_ref() != batch.schema_ref().as_ref() { return Ok(None); }
    decoded_schema::with_schema(batch, Arc::clone(schema)).map(Some)
}

/// Applies IPC stream field nullability to an already decoded batch without copying its value buffers.
/// Returns a replacement only when the stream reader would change the schema.
pub fn adapt_stream_batch(batch: &RecordBatch) -> Result<Option<RecordBatch>> { decoded_schema::adapt_batch(batch) }

/// Owns a complete IPC stream's schema and decoded Arrow buffers under one reservation.
pub struct OwnedStream {
    schema: SchemaRef,
    pages: Vec<OwnedBatch>,
    _reservation: Arc<MemoryReservation>,
}

impl OwnedStream {
    /// The input must end with an IPC end marker. Its allocation belongs to the caller.
    pub fn read(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bound = stream_bound(bytes, true, arrow_descriptors)?;
        let reservation = memory.reserve(bound)?;
        let reader = ArrowStream::from_reader(bytes, None)?;
        let schema = reader.schema();
        let pages = reader.map(|page| OwnedBatch::with_reservation(page?, Arc::clone(&reservation))).collect::<Result<_>>()?;
        Ok(Self { schema, pages, _reservation: reservation })
    }

    /// The input must end with an IPC end marker.
    /// Decoded arrays can retain the input allocation, so carry its existing charge in the buffer owner.
    pub fn read_buffer(bytes: Buffer, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(stream_bound(&bytes, true, arrow_descriptors)?)?;
        let bytes = &bytes;
        let mut position = 0;
        Self::decode_messages(std::iter::from_fn(|| {
            stream_message(bytes, &mut position).transpose().map(|message| {
                message.map(|(header, body)| (header, bytes.slice_with_length(body.start, body.len())))
            })
        }), reservation)
    }

    /// Like a complete IPC stream, with each message held in a separate buffer.
    pub fn read_frames(frames: &[Buffer], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let (end, frames) = frames.split_last().ok_or_else(|| invalid("Arrow stream has no end marker"))?;
        if end.as_slice() != [255, 255, 255, 255, 0, 0, 0, 0] {
            return Err(invalid("Arrow stream has no end marker"));
        }
        let messages = || frames.iter().map(|frame| {
            let mut position = 0;
            let (header, body) = stream_message(frame, &mut position)?
                .ok_or_else(|| invalid("Arrow stream ended before its last frame"))?;
            if position != frame.len() { return Err(invalid("Arrow stream frame contains trailing bytes")); }
            Ok((header, frame.slice_with_length(body.start, body.len())))
        });
        let bound = messages().try_fold(8192, |bound, message| {
            let (header, body) = message?;
            allocation_add(bound, framed_message_bound(header, &body, header.len(), arrow_descriptors)?)
        })?;
        Self::decode_messages(messages(), memory.reserve(bound)?)
    }

    fn decode_messages<'a>(
        mut messages: impl Iterator<Item = Result<(&'a [u8], Buffer)>>,
        reservation: Arc<MemoryReservation>,
    ) -> Result<Self> {
        let (header, body) = messages.next().transpose()?.ok_or_else(|| invalid("Arrow stream has no schema"))?;
        if !body.is_empty() { return Err(invalid("Arrow schema has a body")); }
        let schema = decoded_schema::adapt(&Arc::new(read_schema_header(header)?))?;
        let dense_unions = schema.fields().iter().any(|field| contains_dense_union(field.data_type()));
        let mut dictionaries = HashMap::new();
        let mut pages = Vec::new();
        for message in messages {
            let (header, body) = message?;
            let body = if dense_unions { align_ipc_body(body) } else { body };
            if let Some(batch) = decode_stream_batch(Cow::Borrowed(header), body, &schema, &mut dictionaries, None)? {
                pages.push(OwnedBatch::with_reservation(batch, Arc::clone(&reservation))?);
            }
        }
        Ok(Self { schema, pages, _reservation: reservation })
    }

    pub fn schema(&self) -> &SchemaRef { &self.schema }
    pub fn pages(&self) -> &[OwnedBatch] { &self.pages }
    pub fn into_pages(self) -> Vec<OwnedBatch> { self.pages }
}

fn contains_dense_union(ty: &DataType) -> bool {
    match ty {
        DataType::Union(_, UnionMode::Dense) => true,
        DataType::Union(legs, UnionMode::Sparse) => legs.iter().any(|(_, field)| contains_dense_union(field.data_type())),
        DataType::Struct(fields) => fields.iter().any(|field| contains_dense_union(field.data_type())),
        DataType::List(field) | DataType::LargeList(field) | DataType::ListView(field)
        | DataType::LargeListView(field) | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) | DataType::RunEndEncoded(_, field) => contains_dense_union(field.data_type()),
        DataType::Dictionary(_, value) => contains_dense_union(value),
        _ => false,
    }
}

/// Ensures dense union offsets can be read before Arrow aligns the decoded arrays.
pub fn align_ipc_body(body: Buffer) -> Buffer {
    if body.as_ptr().align_offset(std::mem::align_of::<i32>()) == 0 {
        body
    } else {
        Buffer::from_slice_ref(body.as_slice())
    }
}

fn decode_stream_batch(
    mut metadata: Cow<'_, [u8]>, body: Buffer, schema: &SchemaRef,
    dictionaries: &mut HashMap<i64, ArrayRef>, projection: Option<&[usize]>,
) -> Result<Option<RecordBatch>> {
    let message = arrow_ipc::root_as_message(&metadata).map_err(invalid)?;
    validate_lz4_lengths(&message, &body)?;
    let data = message.header_as_record_batch().or_else(|| message.header_as_dictionary_batch().and_then(|dictionary| dictionary.data()));
    if let Some(data) = data {
        let nodes = data.nodes().ok_or_else(|| invalid("Arrow batch has no field nodes"))?;
        if nodes.iter().any(|node| node.null_count() < 0) {
            nulls::normalize(metadata.to_mut(), &body, schema)?;
        }
    }
    let message = arrow_ipc::root_as_message(&metadata).map_err(invalid)?;
    if let Some(batch) = message.header_as_record_batch() {
        return arrow_ipc::reader::read_record_batch(&body, batch, Arc::clone(schema), dictionaries, projection, &message.version()).map(Some).map_err(invalid);
    }
    let dictionary = message.header_as_dictionary_batch().ok_or_else(|| invalid("Unexpected Arrow stream message"))?;
    arrow_ipc::reader::read_dictionary(&body, dictionary, schema, dictionaries, &message.version()).map_err(invalid)?;
    Ok(None)
}

fn row_descriptors(rows: usize) -> Result<usize> { allocation_mul(allocation_add(rows, 1)?, 4096) }

fn arrow_descriptors(rows: usize) -> Result<usize> {
    allocation_add(4096, allocation_mul(rows.div_ceil(8), 4)?)
}

/// Bounds message decoding scratch, including declared row descriptors and compressed buffer lengths.
/// Reserve the returned byte count before invoking the Arrow reader and retain it through decoded-buffer use.
pub fn message_allocation_bound(header: &[u8], body: &[u8]) -> Result<usize> {
    framed_message_allocation_bound(header, body, header.len())
}

fn framed_message_allocation_bound(header: &[u8], body: &[u8], metadata_length: usize) -> Result<usize> {
    framed_message_bound(header, body, metadata_length, row_descriptors)
}

fn framed_message_bound(header: &[u8], body: &[u8], metadata_length: usize, field_bound: fn(usize) -> Result<usize>) -> Result<usize> {
    let message = arrow_ipc::root_as_message(header).map_err(invalid)?;
    if let Some(schema) = message.header_as_schema() { validate_decimal_metadata(schema)?; }
    message_bound(&message, metadata_length, body.len(), field_bound, |start| {
        let prefix: [u8; 8] = body.get(start..allocation_add(start, 8)?).ok_or_else(|| invalid("Compressed Arrow buffer has no length"))?.try_into().map_err(invalid)?;
        Ok(i64::from_le_bytes(prefix))
    })
}

fn message_bound(message: &arrow_ipc::Message<'_>, header_length: usize, body_length: usize, field_bound: fn(usize) -> Result<usize>, mut compressed_length: impl FnMut(usize) -> Result<i64>) -> Result<usize> {
    let mut bound = allocation_add(8192, allocation_add(allocation_mul(header_length, 16)?, allocation_mul(body_length, 4)?)?)?;
    if usize::try_from(message.bodyLength()).map_err(invalid)? > body_length { return Err(invalid("Arrow message body is truncated")); }
    if let Some(batch) = message.header_as_record_batch().or_else(|| message.header_as_dictionary_batch().and_then(|batch| batch.data())) {
        if let Some(compression) = batch.compression() {
            let scratch = match compression.codec() {
                arrow_ipc::CompressionType::LZ4_FRAME => 64 << 20,
                arrow_ipc::CompressionType::ZSTD => 1 << 20,
                _ => return Err(invalid("Unknown Arrow compression codec")),
            };
            bound = allocation_add(bound, scratch)?;
        }
        if let Some(nodes) = batch.nodes() {
            for node in nodes { bound = allocation_add(bound, field_bound(usize::try_from(node.length()).map_err(invalid)?)?)?; }
        }
        if let Some(buffers) = batch.buffers() {
            for buffer in buffers {
                let start = usize::try_from(buffer.offset()).map_err(invalid)?;
                let length = usize::try_from(buffer.length()).map_err(invalid)?;
                if allocation_add(start, length)? > body_length { return Err(invalid("Arrow buffer exceeds its message body")); }
                let decoded = if batch.compression().is_some() && length != 0 {
                    if length < 8 { return Err(invalid("Compressed Arrow buffer has no length")); }
                    let decoded = compressed_length(start)?;
                    if decoded == -1 { length - 8 } else { usize::try_from(decoded).map_err(invalid)? }
                } else { length };
                bound = allocation_add(bound, allocation_add(allocation_mul(decoded, 4)?, 512)?)?;
            }
        }
    }
    Ok(bound)
}

fn validate_lz4_lengths(message: &arrow_ipc::Message<'_>, body: &[u8]) -> Result<()> {
    let Some(batch) = message.header_as_record_batch().or_else(|| message.header_as_dictionary_batch().and_then(|batch| batch.data())) else { return Ok(()); };
    validate_lz4_batch(batch, body)
}

fn validate_lz4_batch(batch: arrow_ipc::RecordBatch<'_>, body: &[u8]) -> Result<()> {
    if !batch.compression().is_some_and(|compression| compression.codec() == arrow_ipc::CompressionType::LZ4_FRAME) { return Ok(()); }
    if let Some(buffers) = batch.buffers() {
        for buffer in buffers {
            let start = usize::try_from(buffer.offset()).map_err(invalid)?;
            let length = usize::try_from(buffer.length()).map_err(invalid)?;
            let bytes = body.get(start..allocation_add(start, length)?).ok_or_else(|| invalid("Arrow buffer exceeds its message body"))?;
            if bytes.is_empty() { continue; }
            let prefix: [u8; 8] = bytes.get(..8).ok_or_else(|| invalid("Compressed Arrow buffer has no length"))?.try_into().map_err(invalid)?;
            let expected = i64::from_le_bytes(prefix);
            if expected == -1 || expected == 0 { continue; }
            let expected = usize::try_from(expected).map_err(invalid)?;
            let mut decoder = lz4_flex::frame::FrameDecoder::new(&bytes[8..]);
            let mut scratch = [0; 8192];
            let mut decoded = 0;
            loop {
                let read = decoder.read(&mut scratch).map_err(invalid)?;
                if read == 0 { break; }
                decoded = allocation_add(decoded, read)?;
                if decoded > expected { return Err(invalid("LZ4 data exceeds its declared Arrow buffer length")); }
            }
            if decoded != expected { return Err(invalid("LZ4 data differs from its declared Arrow buffer length")); }
        }
    }
    Ok(())
}

/// Bounds all messages retained by a decoded IPC stream, including dictionaries.
/// The encoded input has a separate lifetime and must retain its own allocation charge.
pub fn stream_allocation_bound(bytes: &[u8]) -> Result<usize> {
    stream_bound(bytes, false, row_descriptors)
}

fn stream_bound(bytes: &[u8], terminated: bool, field_bound: fn(usize) -> Result<usize>) -> Result<usize> {
    let mut position = 0;
    let mut bound = 8192;
    while position < bytes.len() {
        let Some((header, body)) = stream_message(bytes, &mut position)? else {
            if terminated && position != bytes.len() { return Err(invalid("Bytes follow IPC stream end marker")); }
            return Ok(bound);
        };
        bound = allocation_add(bound, framed_message_bound(header, &bytes[body], header.len(), field_bound)?)?;
    }
    if terminated { Err(invalid("IPC stream has no end marker")) } else { Ok(bound) }
}

fn stream_message<'a>(bytes: &'a [u8], position: &mut usize) -> Result<Option<(&'a [u8], std::ops::Range<usize>)>> {
    let mut length = inspection_i32(bytes, position)?;
    if length == -1 { length = inspection_i32(bytes, position)?; }
    if length == 0 { return Ok(None); }
    let end = allocation_add(*position, usize::try_from(length).map_err(invalid)?)?;
    let header = bytes.get(*position..end).ok_or_else(|| invalid("Arrow stream metadata is truncated"))?;
    let message = arrow_ipc::root_as_message(header).map_err(invalid)?;
    let body_end = allocation_add(end, usize::try_from(message.bodyLength()).map_err(invalid)?)?;
    if body_end > bytes.len() { return Err(invalid("Arrow stream body is truncated")); }
    *position = body_end;
    Ok(Some((header, end..body_end)))
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
        bound = allocation_add(bound, framed_message_allocation_bound(&bytes[header_start..header_end], &bytes[metadata_end..body_end], metadata_end - start)?)?;
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
    write_file_pages(schema, pages)
}

pub fn write_file_pages<'a>(schema: &Schema, pages: impl IntoIterator<Item = &'a RecordBatch>) -> Result<Vec<u8>> {
    let mut writer = JavaFile::new(Vec::new(), schema)?;
    for page in pages { writer.write(page)?; }
    writer.finish()
}

/// `ArrowUnloader`'s file mode, `xtdb.arrow.ArrowUnloader.Mode.FILE`: the magic padded to eight bytes, the stream's messages in the
/// reference's Java layout, its single zero end marker, then the Java-layout footer, its length and the magic.
struct JavaFile<W: Write> {
    writer: StreamWriter<TailWriter<FirstSchemaWriter<java_layout::JavaLayout<W>>>>,
    decimal_schema: Arc<crate::ipc_decimal::DecimalSchema>,
}

const FILE_MAGIC: &[u8; 6] = b"ARROW1";

impl<W: Write> JavaFile<W> {
    fn new(mut sink: W, schema: &Schema) -> Result<Self> {
        let decimal_schema = Arc::new(crate::ipc_decimal::DecimalSchema::new(schema)?);
        sink.write_all(FILE_MAGIC).map_err(invalid)?;
        sink.write_all(&[0; 2]).map_err(invalid)?;
        let layout = java_layout::JavaLayout::starting_at(sink, 8);
        let first = FirstSchemaWriter { inner: layout, schema: Arc::clone(&decimal_schema), pending: Vec::new(), expected: None, complete: false };
        let writer = StreamWriter::try_new_with_options(TailWriter { inner: first, pending: Vec::with_capacity(8) }, &decimal_schema.wire, write_options()?).map_err(invalid)?;
        Ok(Self { writer, decimal_schema })
    }
    fn write(&mut self, page: &RecordBatch) -> Result<()> {
        self.decimal_schema.validate_batch(page)?;
        self.write_translated(&undefined::undefine(page)?)
    }
    fn write_translated(&mut self, page: &RecordBatch) -> Result<()> { self.writer.write(page).map_err(invalid) }
    fn sink(&mut self) -> &mut W { &mut self.writer.get_mut().inner.inner.inner }
    /// Bytes the sink has received plus those the end-marker lookahead still holds.
    fn pending_bytes(&mut self) -> u64 { self.writer.get_mut().pending.len() as u64 }
    fn finish(mut self) -> Result<W> {
        self.writer.finish().map_err(invalid)?;
        let mut tail = self.writer.into_inner().map_err(invalid)?;
        if tail.pending.as_slice() != [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0] { return Err(invalid("Arrow writer produced an unexpected end marker")); }
        tail.inner.write_all(&[0; 4]).map_err(invalid)?;
        if !tail.inner.complete { return Err(invalid("Arrow file ended before its schema")); }
        let layout = tail.inner.inner;
        let schema = layout.schema.as_deref().ok_or_else(|| invalid("Arrow file has no schema message"))?;
        let footer = java_layout::footer(schema, &layout.blocks).map_err(invalid)?;
        let mut sink = layout.inner;
        sink.write_all(&footer).map_err(invalid)?;
        sink.write_all(&i32::try_from(footer.len()).map_err(invalid)?.to_le_bytes()).map_err(invalid)?;
        sink.write_all(FILE_MAGIC).map_err(invalid)?;
        sink.flush().map_err(invalid)?;
        Ok(sink)
    }
}

pub struct ArrowFileWriter { writer: JavaFile<LimitedFile>, allocation: WriterAllocation }

enum WriterAllocation {
    CallerReserved,
    Owned { memory: Arc<MemoryBudget>, _schema: Arc<MemoryReservation>, blocks: Vec<Arc<MemoryReservation>>, block_bound: usize },
}

struct LimitedFile { file: File, position: u64, limit: u64, sha256: Option<sha2::Sha256> }

impl Write for LimitedFile {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.position.checked_add(bytes.len() as u64).is_none_or(|position| position > self.limit) {
            return Err(std::io::Error::other("Arrow output byte limit exceeded"));
        }
        let count = self.file.write(bytes)?;
        self.position += count as u64;
        if let Some(hash) = &mut self.sha256 {
            sha2::Digest::update(hash, &bytes[..count]);
        }
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
    pub fn new_bounded(file: File, schema: &Schema, byte_limit: u64) -> Result<Self> {
        Self::new_file(file, schema, byte_limit, None)
    }

    fn new_file(mut file: File, schema: &Schema, limit: u64, sha256: Option<sha2::Sha256>) -> Result<Self> {
        if file.metadata().map_err(invalid)?.len() != 0 || file.stream_position().map_err(invalid)? != 0 { return Err(invalid("Arrow output file must be empty and positioned at zero")); }
        let writer = JavaFile::new(LimitedFile { file, position: 0, limit, sha256 }, schema)?;
        Ok(Self { writer, allocation: WriterAllocation::CallerReserved })
    }

    /// Reserves schema translation, block inventory and footer copies for this writer's lifetime.
    /// The file must be empty at offset zero; callers separately reserve each page's encoding scratch.
    pub fn new_reserved(file: File, schema: &Schema, memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::new_bounded_reserved(file, schema, u64::MAX, memory)
    }

    /// Applies both the output-byte limit and the shared schema/footer memory budget.
    /// The file must be empty at offset zero; callers separately reserve each page's encoding scratch.
    pub fn new_bounded_reserved(file: File, schema: &Schema, byte_limit: u64, memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::new_funded_file(file, schema, byte_limit, memory, None)
    }

    /// Writes the same IPC format while hashing exactly the bytes accepted by the file.
    pub fn new_hashed_bounded_reserved(file: File, schema: &Schema, byte_limit: u64, memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::new_funded_file(file, schema, byte_limit, memory, Some(sha2::Digest::new()))
    }

    fn new_funded_file(file: File, schema: &Schema, byte_limit: u64, memory: &Arc<MemoryBudget>, sha256: Option<sha2::Sha256>) -> Result<Self> {
        let reservation = memory.reserve(schema_translation_bound(schema)?)?;
        let block_bound = allocation_mul(allocation_add(dictionary_count(schema)?, 1)?, 512)?;
        let mut writer = Self::new_file(file, schema, byte_limit, sha256)?;
        writer.allocation = WriterAllocation::Owned { memory: Arc::clone(memory), _schema: reservation, blocks: Vec::new(), block_bound };
        Ok(writer)
    }

    pub fn bytes_written(&mut self) -> u64 { self.writer.pending_bytes() + self.writer.sink().position }

    /// The owner may lower the remaining allowance between writes; already-written bytes must still fit.
    pub fn set_byte_limit(&mut self, byte_limit: u64) -> Result<()> {
        let pending = self.writer.pending_bytes();
        let sink = self.writer.sink();
        if sink.position + pending > byte_limit { return Err(invalid("Arrow output already exceeds the requested byte limit")); }
        sink.limit = byte_limit;
        Ok(())
    }

    pub fn write(&mut self, page: &RecordBatch) -> Result<()> {
        self.write_page(page, &mut None)
    }

    /// Writes a compact event page, charging IPC scratch and nested union cleanup through the write.
    pub fn write_event_page(&mut self, page: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<()> {
        let bound = page.columns().iter().try_fold(schema_translation_bound(page.schema_ref())?, |bytes, column| {
            allocation_add(bytes, allocation_mul(column.get_array_memory_size(), 4)?)
        })?;
        let _encoding = memory.reserve(bound)?;
        let mut copies = Some(undefined::EventCopies { memory, retained: Vec::new() });
        self.write_page(page, &mut copies)
    }

    fn write_page(&mut self, page: &RecordBatch, copies: &mut Option<undefined::EventCopies<'_>>) -> Result<()> {
        self.writer.decimal_schema.validate_batch(page)?;
        let block = match &self.allocation {
            WriterAllocation::CallerReserved => None,
            WriterAllocation::Owned { memory, block_bound, .. } => Some(memory.reserve(*block_bound)?),
        };
        self.writer.write_translated(&undefined::undefine_page(page, copies)?)?;
        if let (WriterAllocation::Owned { blocks, .. }, Some(block)) = (&mut self.allocation, block) {
            blocks.push(block);
        }
        Ok(())
    }

    /// Finishes the JVM-compatible end marker and returns the caller-owned file at its end.
    pub fn finish(self) -> Result<File> {
        Ok(self.finish_file()?.file)
    }

    pub fn finish_hashed(self) -> Result<(u64, [u8; 32])> {
        let file = self.finish_file()?;
        let hash = file.sha256.ok_or_else(|| invalid("Arrow writer was opened without hashing"))?;
        Ok((file.position, sha2::Digest::finalize(hash).into()))
    }

    fn finish_file(self) -> Result<LimitedFile> {
        let _footer = match &self.allocation {
            WriterAllocation::CallerReserved => None,
            WriterAllocation::Owned { memory, _schema, blocks, block_bound } => Some(memory.reserve(allocation_add(_schema.bytes(), allocation_mul(blocks.len(), *block_bound)?)?)?),
        };
        self.writer.finish()
    }
}

pub fn write_stream(schema: &Schema, pages: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut bytes = write_arrow_stream(schema, pages, write_options()?)?;
    let end = bytes.len();
    jvm_end_marker(&mut bytes, end)?;
    Ok(bytes)
}

/// Prepares the schema frame for independently encoded single-row stream messages.
/// The caller reserves codec scratch and accounts for the retained frames.
pub struct RowStreamEncoder {
    schema: SchemaRef,
    schema_frame: Vec<u8>,
    dictionaries: usize,
    options: IpcWriteOptions,
}

/// Detached dictionary and record messages for one row of a V5 stream with 8-byte alignment.
pub struct PreparedStreamRow {
    dictionaries: Box<[EncodedData]>,
    record: EncodedData,
    framed_len: usize,
}

impl PreparedStreamRow {
    pub fn framed_len(&self) -> usize {
        self.framed_len
    }

    /// Appends the messages without allocating an intermediate framed copy of their bodies.
    pub fn append_to(&self, bytes: &mut Vec<u8>) {
        for message in self.dictionaries.iter().chain(std::iter::once(&self.record)) {
            let metadata_len = message.ipc_message.len().next_multiple_of(STREAM_ALIGNMENT);
            bytes.extend_from_slice(&[0xff; 4]);
            bytes.extend_from_slice(&(metadata_len as i32).to_le_bytes());
            bytes.extend_from_slice(&message.ipc_message);
            let padding = metadata_len - message.ipc_message.len();
            bytes.extend_from_slice(&[0; STREAM_ALIGNMENT][..padding]);
            bytes.extend_from_slice(&message.arrow_data);
        }
    }
}

impl RowStreamEncoder {
    pub fn new(schema: &SchemaRef) -> Result<Self> {
        let options = write_options()?;
        let decimal_schema = crate::ipc_decimal::DecimalSchema::new(schema)?;
        let mut tracker = DictionaryTracker::new(false);
        let mut message = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
            &decimal_schema.wire,
            &mut tracker,
            &options,
        );
        decimal_schema.patch_message(&mut message.ipc_message)?;
        let mut bytes = Vec::new();
        write_message(&mut bytes, message, &options).map_err(invalid)?;
        Ok(Self {
            schema: Arc::clone(schema),
            schema_frame: bytes,
            dictionaries: tracker.dict_id().len(),
            options,
        })
    }

    pub fn schema_frame(&self) -> &[u8] {
        &self.schema_frame
    }

    pub fn has_dictionaries(&self) -> bool {
        self.dictionaries != 0
    }

    /// Encodes a row's dictionary and record frames. A complete stream also needs its schema and end marker.
    pub fn encode(&self, row: &RecordBatch) -> Result<Vec<u8>> {
        let (dictionaries, record) = self.messages(row)?;
        let mut bytes = Vec::new();
        for message in dictionaries.into_iter().chain(std::iter::once(record)) {
            write_message(&mut bytes, message, &self.options).map_err(invalid)?;
        }
        Ok(bytes)
    }

    /// Validates framing and measures the exact wire length while encoding the candidate row.
    pub fn prepare(&self, row: &RecordBatch) -> Result<PreparedStreamRow> {
        let (dictionaries, record) = self.messages(row)?;
        let framed_len = dictionaries
            .iter()
            .chain(std::iter::once(&record))
            .try_fold(0, |len, message| {
                if !message.arrow_data.len().is_multiple_of(STREAM_ALIGNMENT) {
                    return Err(invalid(ArrowError::MemoryError("Arrow data not aligned".into())));
                }
                let metadata_len = message.ipc_message.len().next_multiple_of(STREAM_ALIGNMENT);
                let framed_metadata = allocation_add(8, metadata_len)?;
                allocation_add(len, allocation_add(framed_metadata, message.arrow_data.len())?)
            })?;
        Ok(PreparedStreamRow {
            dictionaries: dictionaries.into_boxed_slice(),
            record,
            framed_len,
        })
    }

    fn messages(&self, row: &RecordBatch) -> Result<(Vec<EncodedData>, EncodedData)> {
        if row.num_rows() != 1 || row.schema_ref() != &self.schema {
            return Err(invalid("Arrow row differs from its declared stream schema or row count"));
        }
        let mut tracker = DictionaryTracker::new(false);
        for _ in 0..self.dictionaries {
            tracker.next_dict_id();
        }
        IpcDataGenerator::default()
            .encode(row, &mut tracker, &self.options, &mut CompressionContext::default())
            .map_err(invalid)
    }
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

/// Writes pages without buffering the complete stream, with metadata in the reference's Java layout; the sink owns and enforces its allocation bound.
/// The caller reserves the schema translation and initial encoded schema frame through this call.
/// A page with different native field metadata is rejected before encoding; the sink may already contain a valid prefix.
pub fn write_stream_to<'a, W: Write>(schema: &Schema, pages: impl IntoIterator<Item = &'a RecordBatch>, sink: W) -> Result<W> {
    let decimal_schema = Arc::new(crate::ipc_decimal::DecimalSchema::new(schema)?);
    let sink = FirstSchemaWriter { inner: java_layout::JavaLayout::new(sink), schema: Arc::clone(&decimal_schema), pending: Vec::new(), expected: None, complete: false };
    let mut writer = StreamWriter::try_new_with_options(TailWriter { inner: sink, pending: Vec::with_capacity(8) }, &decimal_schema.wire, write_options()?).map_err(invalid)?;
    for page in pages { decimal_schema.validate_batch(page)?; writer.write(page).map_err(invalid)?; }
    writer.finish().map_err(invalid)?;
    let mut sink = writer.into_inner().map_err(invalid)?;
    if sink.pending.as_slice() != [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0] { return Err(invalid("Arrow writer produced an unexpected end marker")); }
    sink.inner.write_all(&[0; 4]).map_err(invalid)?;
    sink.inner.flush().map_err(invalid)?;
    if !sink.inner.complete { return Err(invalid("Arrow stream ended before its schema")); }
    Ok(sink.inner.inner.inner)
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
struct FirstSchemaWriter<W> { inner: W, schema: Arc<crate::ipc_decimal::DecimalSchema>, pending: Vec<u8>, expected: Option<usize>, complete: bool }
impl<W: Write> Write for FirstSchemaWriter<W> {
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
                    patch_schema_frame(&mut self.pending, &self.schema).map_err(std::io::Error::other)?;
                    self.inner.write_all(&self.pending)?; self.pending.clear(); self.complete = true;
                }
            }
        }
        Ok(length)
    }
    fn flush(&mut self) -> std::io::Result<()> { self.inner.flush() }
}

const STREAM_ALIGNMENT: usize = 8;

fn write_options() -> Result<IpcWriteOptions> {
    IpcWriteOptions::try_new(STREAM_ALIGNMENT, false, arrow_ipc::MetadataVersion::V5).map_err(invalid)
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


#[test]
fn stream_reader_borrows_aligned_bodies_with_the_same_batch_memory_size() {
    use arrow_array::Int64Array;

    struct AdmittedBytes {
        bytes: Buffer,
        _charge: Arc<MemoryReservation>,
    }
    impl AsRef<[u8]> for AdmittedBytes {
        fn as_ref(&self) -> &[u8] { self.bytes.as_slice() }
    }

    let page = RecordBatch::try_from_iter([("n", Arc::new(Int64Array::from(vec![3, -5, 7])) as ArrayRef)]).unwrap();
    let options = IpcWriteOptions::try_new(64, false, arrow_ipc::MetadataVersion::V5).unwrap();
    let stream = write_arrow_stream(page.schema_ref(), std::slice::from_ref(&page), options).unwrap();
    let reference = ArrowStream::from_reader(stream.as_slice(), None).unwrap().next().unwrap().unwrap();
    let memory = MemoryBudget::new(1 << 20);
    for offset in [0, 1] {
        let mut envelope = vec![0; offset];
        envelope.extend_from_slice(&stream);
        let charge = memory.reserve(envelope.len()).unwrap();
        let bytes = Bytes::from_owner(AdmittedBytes { bytes: Buffer::from_slice_ref(&envelope), _charge: charge }).slice(offset..);
        let source = bytes.as_ptr() as usize;
        let length = bytes.len();
        let mut reader = ArrowStream::from_bytes(bytes, None).unwrap();
        let decoded = reader.next().unwrap().unwrap();
        let values = decoded.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values();
        assert_eq!((source..source + length).contains(&(values.as_ptr() as usize)), offset == 0);
        assert_eq!(decoded.get_array_memory_size(), reference.get_array_memory_size());
        assert!(reader.next().is_none());
        drop(reader);
        assert_eq!(memory.used_bytes(), if offset == 0 { length } else { 0 });
        assert_eq!(decoded, reference);
        drop(decoded);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn ordered_index_uncompressed_reader_borrows_retained_ipc_body() {
    use arrow_array::Int64Array;
    use arrow_ipc::writer::FileWriter;
    let batch = RecordBatch::try_from_iter([("key", Arc::new(Int64Array::from(vec![3, -5, 7])) as ArrayRef)]).unwrap();
    let mut writer = FileWriter::try_new(Vec::new(), batch.schema_ref()).unwrap();
    for _ in 0..8 { writer.write(&batch).unwrap(); }
    let bytes = Buffer::from(writer.into_inner().unwrap());
    let mut cursor = Cursor::new(bytes.as_slice());
    let layout = inspect_file(&mut cursor).unwrap();
    let mut reader = ArrowFile::from_inspected_buffer(bytes.clone(), None, layout).unwrap();
    let decoded = reader.read_page(0).unwrap();
    let source = bytes.as_ptr() as usize;
    let output = decoded.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr() as usize;
    assert!((source..source + bytes.len()).contains(&output));
    assert!(rtdb_columnar::shared_allocation_size(&decoded).unwrap() < bytes.len());
    drop((reader, bytes));
    assert_eq!(decoded, batch);
}

#[test]
fn owned_ipc_decodes_dense_unions_from_an_unaligned_envelope() {
    use arrow_array::{Int64Array, StringArray, UnionArray};
    use arrow_schema::{DataType, Field, UnionFields};

    let fields = UnionFields::try_new([0, 1], [
        Field::new("i64", DataType::Int64, false),
        Field::new("utf8", DataType::Utf8, false),
    ]).unwrap();
    let union = UnionArray::try_new(fields, vec![0, 1, 0].into(), Some(vec![0, 0, 1].into()), vec![
        Arc::new(Int64Array::from(vec![7, -4])), Arc::new(StringArray::from(vec!["hello"])),
    ]).unwrap();
    let page = RecordBatch::try_from_iter([("value", Arc::new(union) as ArrayRef)]).unwrap();
    let mut envelope = vec![0];
    write_stream_to(page.schema_ref(), [&page], &mut envelope).unwrap();
    let bytes = Buffer::from_slice_ref(&envelope).slice(1);
    let mut position = 0;
    stream_message(&bytes, &mut position).unwrap().unwrap();
    let (_, body) = stream_message(&bytes, &mut position).unwrap().unwrap();
    assert_ne!(bytes[body].as_ptr().align_offset(std::mem::align_of::<i32>()), 0);

    let stream = OwnedStream::read_buffer(bytes, &MemoryBudget::new(1 << 20)).unwrap();
    assert_eq!(stream.pages()[0].batch(), &page);
}
