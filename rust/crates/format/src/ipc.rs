use std::borrow::Cow;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::ops::Range;
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
use rtdb_columnar::{BufferIdentity, MemoryBudget, MemoryReservation, OwnedBatch, RetainedArray};
use rtdb_types::{Anomaly, Result};

mod decoded_schema;
mod event_projection;
mod java_layout;
mod java_stream;
mod nulls;
mod plain_layout;
mod projection;
mod record_spans;
mod undefined;

pub use record_spans::RecordSpans;

pub struct ArrowFile {
    reader: Box<dyn RetainedReader>,
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
    fn block_slice(&mut self, _range: Range<usize>, _record: bool) -> Option<Buffer> { None }
}

struct StreamReader<R>(R);
impl<R: Read> Read for StreamReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> { self.0.read(bytes) }
}
impl<R: Seek> Seek for StreamReader<R> {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> { self.0.seek(position) }
}
impl<R: Read + Seek + Send> RetainedReader for StreamReader<R> {}

struct RangeReader<R, F> {
    reader: StreamReader<R>,
    retain: F,
    file_blocks: bool,
}

impl<R: Read, F> Read for RangeReader<R, F> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> { self.reader.read(out) }
}

impl<R: Seek, F> Seek for RangeReader<R, F> {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> { self.reader.seek(position) }
}

impl<R, F> RetainedReader for RangeReader<R, F>
where
    R: Read + Seek + Send,
    F: FnMut(Range<u64>) -> Option<Bytes> + Send,
{
    fn block_slice(&mut self, range: Range<usize>, record: bool) -> Option<Buffer> {
        if !record || range.is_empty() { return None; }
        let bytes = (self.retain)(range.start as u64..range.end as u64)?;
        if bytes.len() != range.len() || bytes.as_ptr().align_offset(std::mem::align_of::<u128>()) != 0 {
            return None;
        }
        if self.file_blocks {
            let message = arrow_ipc::root_as_message(&bytes[block_header(&bytes).ok()?]).ok()?;
            let nodes = message.header_as_record_batch()?.nodes()?;
            // File normalization edits the encoded block in place on the ordinary read path.
            if nodes.iter().any(|node| node.null_count() < 0) { return None; }
        }
        Some(Buffer::from(bytes))
    }
}

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
    fn block_slice(&mut self, range: Range<usize>, _record: bool) -> Option<Buffer> {
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

    /// Offers retained bytes for record blocks. Returned bytes own their allocation charge.
    ///
    /// # Safety
    /// The reader and every supplied view must expose the same immutable file bytes.
    pub unsafe fn from_reader_with_retained_ranges(
        mut reader: impl Read + Seek + Send + 'static, projection: Option<Vec<usize>>,
        retain: impl FnMut(Range<u64>) -> Option<Bytes> + Send + 'static,
    ) -> Result<Self> {
        let layout = inspect_file(&mut reader)?;
        Self::open_inspected(Box::new(RangeReader { reader: StreamReader(reader), retain, file_blocks: true }), projection, layout)
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
            let bytes = read_block(reader.as_mut(), &block, false)?;
            pages.load_dictionary(index, bytes)?;
        }
        Ok(Self { reader, pages, next_page: 0, encoded_page_sizes: layout.page_encoded_bytes, page_decoding_bounds: layout.page_decoding_bounds })
    }

    pub fn schema(&self) -> SchemaRef {
        self.pages.event_projection.as_ref().map_or_else(|| self.pages.schema(), |projection| Arc::clone(&projection.schema))
    }

    /// Selects event headers, the document ID, and the named direct children of the put struct.
    pub fn project_event_payload(&mut self, columns: &[String]) {
        self.pages.project_event_payload(columns);
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
        let buffer = read_block(self.reader.as_mut(), self.pages.block(page)?, true)?;
        let batch = self.pages.decode_file_page(page, buffer)?;
        self.next_page = page + 1;
        Ok(batch)
    }

    /// Reads an event page with only the requested children of its put leg.
    pub fn read_event_page(&mut self, page: usize, put_columns: &[String]) -> Result<RecordBatch> {
        let buffer = read_block(self.reader.as_mut(), self.pages.block(page)?, true)?;
        let batch = self.pages.decode_event_page(page, buffer, put_columns)?;
        self.next_page = page + 1;
        Ok(batch)
    }

    /// Funds newly read wire bytes and the selected decode independently.
    /// Existing retained ranges must carry their reader's allocation charge.
    // arrow-flow-scope: rtdb.event.projected
    pub fn read_event_page_reserved(
        &mut self, page: usize, put_columns: &[String], memory: &Arc<MemoryBudget>,
    ) -> Result<(RecordBatch, Arc<MemoryReservation>)> {
        use rtdb_columnar::arrow_flow::{self, Measures};

        let flow = arrow_flow::sample();
        let range = self.pages.page_range(page)?;
        let start = usize::try_from(range.start).map_err(invalid)?;
        let end = usize::try_from(range.end).map_err(invalid)?;
        let wire = match self.reader.block_slice(start..end, true) {
            Some(buffer) => {
                // arrow-flow: rtdb.event.projected/wire-slice stored-page retained-ipc borrow event-page retained - | Slice a page from storage backing already held by its reader.
                if let Some(flow) = flow {
                    flow.record("rtdb.event.projected/wire-slice", Measures {
                        copied_bytes: Some(0),
                        new_backing_bytes: Some(0),
                        output_buffer: Some(1),
                        ..Measures::default()
                    });
                }
                buffer
            }
            None => {
                let charge = memory.reserve(end - start)?;
                // arrow-flow: rtdb.event.projected/wire-read stored-page retained-ipc read event-page required read_block_copy | Read the range when the reader cannot lend it.
                let buffer = read_block_copy(self.reader.as_mut(), range)?;
                if let Some(flow) = flow {
                    flow.record("rtdb.event.projected/wire-read", Measures {
                        copied_bytes: Some(buffer.len()),
                        new_backing_bytes: Some(buffer.capacity()),
                        reservation_bytes: Some(charge.bytes()),
                        output_buffer: Some(1),
                        ..Measures::default()
                    });
                }
                rtdb_columnar::retain_buffer_with_owner(&buffer, charge)
            }
        };
        let measured_wire = flow.map(|_| wire.clone());
        // arrow-flow: rtdb.event.projected/selected retained-ipc decoded-page decode event-page required decode_projected_buffer_reserved | Decode an admitted projection when the record supports the requested columns.
        let (batch, charge, edge, fallback_reason) = match self.pages.decode_projected_buffer_reserved(
            page, &wire, PageProjection::PutColumns(put_columns), |bytes| memory.reserve(bytes),
        )? {
            ProjectionAttempt::Selected(batch, charge) => (batch, charge, "rtdb.event.projected/selected", None),
            ProjectionAttempt::Fallback(reason) => {
                let charge = memory.reserve(self.pages.full_page_decode_bound(page, &wire)?)?;
                // arrow-flow: rtdb.event.projected/full-page retained-ipc decoded-page decode event-page candidate decode_page | Decode the full page after projection planning declines.
                let batch = self.pages.decode_page(page, wire)?;
                (batch, charge, "rtdb.event.projected/full-page", Some(reason))
            }
        };
        if let (Some(flow), Some(input)) = (flow, measured_wire) {
            flow.record(edge, Measures {
                fallback_reason: fallback_reason.map(ProjectionFallback::label),
                rows: Some(batch.num_rows()),
                new_backing_bytes: rtdb_columnar::new_buffer_backing_bytes(&batch, &input),
                reservation_bytes: Some(charge.bytes()),
                input_buffer: Some(1),
                ..Measures::default()
            });
        }
        self.next_page = page + 1;
        Ok((batch, charge))
    }
    // arrow-flow-end

}

pub struct PageDecoder {
    decoder: FileDecoder,
    projection: Option<Vec<usize>>,
    event_projection: Option<event_projection::EventProjection>,
    schema: SchemaRef,
    source_schema: SchemaRef,
    decoded_schema: SchemaRef,
    version: arrow_ipc::MetadataVersion,
    blocks: Vec<arrow_ipc::Block>,
    dictionaries: Vec<arrow_ipc::Block>,
}

#[derive(Clone, Copy)]
pub enum PageProjection<'a> {
    Iid,
    PutColumns(&'a [String]),
}

enum ProjectionAttempt<G> {
    Selected(RecordBatch, G),
    Fallback(ProjectionFallback),
}

enum ProjectionFallback {
    PlanDeclined,
}

impl ProjectionFallback {
    fn label(self) -> &'static str {
        match self {
            Self::PlanDeclined => "plan-declined",
        }
    }
}

impl PageDecoder {
    pub fn from_footer(bytes: &[u8], data_end: u64, projection: Option<Vec<usize>>) -> Result<Self> {
        let footer = arrow_ipc::root_as_footer(bytes).map_err(invalid)?;
        let ipc_schema = footer.schema().ok_or_else(|| invalid("Arrow footer has no schema"))?;
        if !ipc_schema.endianness().equals_to_target_endianness() { return Err(invalid("Arrow file endianness differs from the host")); }
        let source_schema = Arc::new(crate::ipc_decimal::decode(ipc_schema)?);
        let decoded = decoded_schema::adapt(&source_schema)?;
        let schema = if let Some(projection) = &projection { Arc::new(decoded.project(projection).map_err(invalid)?) } else { Arc::clone(&decoded) };
        // SAFETY: the reference writer's undefined union cells use type ID -1, which arrow-rs validation rejects.
        // This decoder is private to `load_dictionary` and `decode_prepared`, and no batch leaves `decode_prepared`
        // before `undefined::reinstate` has rewritten those cells and run `validate_full` over every column, dictionary
        // values included; `corrupt_persisted_pages_fail_full_decode_validation` checks that.
        let mut decoder = unsafe { FileDecoder::new(Arc::clone(&decoded), footer.version()).with_skip_validation(true) };
        if let Some(projection) = projection.clone() { decoder = decoder.with_projection(projection); }
        let blocks: Vec<_> = footer.recordBatches().ok_or_else(|| invalid("Arrow footer has no record-batch inventory"))?.iter().copied().collect();
        let dictionaries: Vec<_> = footer.dictionaries().into_iter().flat_map(|blocks| blocks.iter().copied()).collect();
        for block in blocks.iter().chain(&dictionaries) {
            let range = block_range(block)?;
            if range.start < 8 || range.end > data_end {
                return Err(invalid("Arrow block exceeds data region"));
            }
        }
        Ok(Self { decoder, projection, event_projection: None, schema, source_schema, decoded_schema: decoded, version: footer.version(), blocks, dictionaries })
    }

    pub fn schema(&self) -> SchemaRef { Arc::clone(&self.schema) }

    pub fn page_count(&self) -> usize { self.blocks.len() }

    pub fn block_inventory_bytes(&self) -> usize {
        (self.blocks.capacity() + self.dictionaries.capacity()) * size_of::<arrow_ipc::Block>()
    }

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

    /// Includes the schema allocations needed when a full page backs a projected query.
    pub fn full_page_decode_bound(&self, page: usize, bytes: &[u8]) -> Result<usize> {
        let body = self.page_bound(page, bytes)?;
        let (schema, _) = schema_metrics(&self.schema, decoded_schema::MAX_DEPTH)?;
        allocation_add(body, schema)
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
        match self.decode_projected_page(page, &bytes, PageProjection::Iid)? {
            Some(batch) => Ok(batch),
            None => self.decode_page(page, bytes)?.project(&[iid]).map_err(invalid),
        }
    }

    pub fn decode_event_page(&self, page: usize, bytes: Buffer, put_columns: &[String]) -> Result<RecordBatch> {
        match self.decode_projected_page(page, &bytes, PageProjection::PutColumns(put_columns))? {
            Some(batch) => Ok(batch),
            None => self.decode_page(page, bytes),
        }
    }

    pub fn narrow_put_projection<'a>(&self, columns: &'a [String]) -> Option<PageProjection<'a>> {
        let op = self.decoded_schema.field_with_name("op").ok()?;
        let DataType::Union(legs, _) = op.data_type() else { return None; };
        legs.iter().any(|(_, leg)| {
            leg.name() == "put" && matches!(leg.data_type(), DataType::Struct(fields)
                if fields.iter().any(|field| !columns.contains(field.name())))
        }).then_some(PageProjection::PutColumns(columns))
    }

    /// Configures `decode_file_page` to select event headers, the document ID, and the named put fields.
    pub fn project_event_payload(&mut self, columns: &[String]) {
        if self.dictionary_count() == 0 && self.schema.fields().len() == self.source_schema.fields().len() {
            self.event_projection = event_projection::EventProjection::new(&self.schema, columns);
        }
    }

    /// Decodes one exact record block using the file's configured event projection.
    /// The caller funds the wire bytes and `page_bound` through use of the batch.
    pub fn decode_file_page(&self, page: usize, bytes: Buffer) -> Result<RecordBatch> {
        let Some(projection) = &self.event_projection else { return self.decode_page(page, bytes); };
        let block = self.block(page)?;
        let (header, body_start) = block_message(block, &bytes)?;
        let (normalized, has_undefined_union) = prepare_block_header(&bytes, header.clone(), body_start, &self.source_schema, false)?;
        let message_bytes = normalized.as_deref().unwrap_or(&bytes[header.clone()]);
        let message = arrow_ipc::root_as_message(message_bytes).map_err(invalid)?;
        if (message.version() == self.version || self.version == arrow_ipc::MetadataVersion::V1)
            && let Some(batch) = projection.decode(&self.decoded_schema, message, &bytes.slice(body_start), Arc::ptr_eq(&self.schema, &self.decoded_schema))? {
            return Ok(batch);
        }
        projection.select_decoded(&self.decode_prepared_header(block, bytes, header, body_start, normalized, has_undefined_union)?)
    }

    /// Admits selected decode allocations while `wire` owns the already-funded IPC bytes.
    /// Wide descriptor selections can use a temporary reservation during planning.
    /// `None` leaves full-page admission to the caller.
    pub fn decode_projected_page_reserved<G>(
        &self, page: usize, wire: impl AsRef<[u8]> + Send + 'static, selection: PageProjection<'_>,
        reserve: impl FnMut(usize) -> Result<G>,
    ) -> Result<Option<(RecordBatch, G)>> {
        let wire = Buffer::from(Bytes::from_owner(wire));
        Ok(match self.decode_projected_buffer_reserved(page, &wire, selection, reserve)? {
            ProjectionAttempt::Selected(batch, grant) => Some((batch, grant)),
            ProjectionAttempt::Fallback(_) => None,
        })
    }

    fn decode_projected_buffer_reserved<G>(
        &self, page: usize, bytes: &Buffer, selection: PageProjection<'_>,
        mut reserve: impl FnMut(usize) -> Result<G>,
    ) -> Result<ProjectionAttempt<G>> {
        let indices = self.projection_indices(selection)?;
        let (batch, version, body) = self.projected_block(page, bytes)?;
        let body = bytes.slice(body);
        let Some(plan) = projection::prepare(&body, batch, &self.decoded_schema, version,
            |_, field| selection.field(field), projection::Preparation::Measure, &mut reserve)?
            .filter(|plan| !plan.declines(self.native(version))) else {
            return Ok(ProjectionAttempt::Fallback(ProjectionFallback::PlanDeclined));
        };
        let charge = reserve(plan.bound)?;
        let _workspace = (plan.workspace != 0).then(|| reserve(plan.workspace)).transpose()?;
        let batch = self.decode_projection(&plan, version, selection, indices.as_ref().map(|indices| indices.as_slice()))?;
        Ok(ProjectionAttempt::Selected(batch, charge))
    }

    /// `None` where only the full decoder reads the page.
    fn decode_projected_page(&self, page: usize, bytes: &Buffer, selection: PageProjection<'_>) -> Result<Option<RecordBatch>> {
        let indices = self.projection_indices(selection)?;
        let (batch, version, body) = self.projected_block(page, bytes)?;
        let body = bytes.slice(body);
        let Some(plan) = projection::prepare(&body, batch, &self.decoded_schema, version,
            |_, field| selection.field(field), projection::Preparation::ReadPage, |_| Ok(()))?
            .filter(|plan| !plan.declines(self.native(version))) else { return Ok(None) };
        self.decode_projection(&plan, version, selection, indices.as_ref().map(|indices| indices.as_slice())).map(Some)
    }

    fn native(&self, version: arrow_ipc::MetadataVersion) -> bool {
        version == self.version || self.version == arrow_ipc::MetadataVersion::V1
    }

    fn projection_indices(&self, selection: PageProjection<'_>) -> Result<Option<[usize; 1]>> {
        if !matches!(selection, PageProjection::Iid) { return Ok(None); }
        let iid = self.source_schema.index_of("_iid").map_err(crate::data::event_arrow_error)?;
        Ok(Some([iid]))
    }

    fn decode_projection<G>(
        &self, plan: &projection::Plan<'_, G>, version: arrow_ipc::MetadataVersion,
        selection: PageProjection<'_>, indices: Option<&[usize]>,
    ) -> Result<RecordBatch> {
        plan.decode(self.native(version), &self.decoded_schema, indices,
            |nested| selection.output_schema(&self.decoded_schema, nested), None)
    }

    fn projected_block<'a>(&self, page: usize, bytes: &'a [u8]) -> Result<(arrow_ipc::RecordBatch<'a>, arrow_ipc::MetadataVersion, usize)> {
        let block = self.block(page)?;
        let (header, metadata_end) = block_message(block, bytes)?;
        let message = arrow_ipc::root_as_message(&bytes[header]).map_err(invalid)?;
        check_block_message(&message, bytes.len() - metadata_end, false)?;
        let batch = message.header_as_record_batch().ok_or_else(|| invalid("Arrow file ended before its declared page"))?;
        Ok((batch, message.version(), metadata_end))
    }

    pub fn decode_page(&self, page: usize, bytes: Buffer) -> Result<RecordBatch> {
        let block = self.block(page)?;
        let (header, body_start) = block_message(block, &bytes)?;
        let (normalized, has_undefined_union) = prepare_block_header(&bytes, header.clone(), body_start, &self.source_schema, false)?;
        self.decode_prepared_header(block, bytes, header, body_start, normalized, has_undefined_union)
    }

    fn decode_prepared_header(
        &self, block: &arrow_ipc::Block, bytes: Buffer, header: Range<usize>, body_start: usize,
        normalized: Option<Vec<u8>>, has_undefined_union: bool,
    ) -> Result<RecordBatch> {
        if let Some(normalized) = normalized {
            // Normalize only the small metadata header so aligned body buffers stay zero-copy
            // slices of `bytes` when no union carries the reference's `-1` undefined cells.
            if !has_undefined_union && self.dictionaries.is_empty() {
                let message = arrow_ipc::root_as_message(&normalized).map_err(invalid)?;
                if self.version != arrow_ipc::MetadataVersion::V1 && message.version() != self.version {
                    return Err(invalid("Could not read IPC message as metadata versions mismatch"));
                }
                let record_batch = message.header_as_record_batch().ok_or_else(|| invalid("Arrow file ended before its declared page"))?;
                let body = bytes.slice(body_start);
                let batch = arrow_ipc::reader::read_record_batch(
                    &body, record_batch, Arc::clone(&self.decoded_schema), &HashMap::new(),
                    self.projection.as_deref(), &message.version(),
                ).map_err(invalid)?;
                for column in batch.columns() {
                    column.to_data().validate_full().map_err(invalid)?;
                }
                return Ok(batch);
            }
            let mut owned = bytes.into_vec::<u8>().unwrap_or_else(|bytes| bytes.to_vec());
            owned[header].copy_from_slice(&normalized);
            return self.decode_prepared(block, Buffer::from(owned));
        }
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
fn read_block(reader: &mut dyn RetainedReader, block: &arrow_ipc::Block, record: bool) -> Result<Buffer> {
    let range = block_range(block)?;
    let start = usize::try_from(range.start).map_err(invalid)?;
    let end = usize::try_from(range.end).map_err(invalid)?;
    if let Some(buffer) = reader.block_slice(start..end, record) { return Ok(buffer); }
    read_block_copy(reader, range)
}

fn read_block_copy(reader: &mut dyn RetainedReader, range: Range<u64>) -> Result<Buffer> {
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

fn prepare_block_header(
    bytes: &[u8], header: Range<usize>, metadata_end: usize, schema: &Schema, dictionary: bool,
) -> Result<(Option<Vec<u8>>, bool)> {
    let message = arrow_ipc::root_as_message(&bytes[header.clone()]).map_err(invalid)?;
    check_block_message(&message, bytes.len() - metadata_end, dictionary)?;
    let body = &bytes[metadata_end..];
    validate_lz4_lengths(&message, body)?;
    let batch = message.header_as_record_batch().or_else(|| message.header_as_dictionary_batch().and_then(|dictionary| dictionary.data()));
    let needs_normalization = batch.and_then(|batch| batch.nodes()).is_some_and(|nodes| nodes.iter().any(|node| node.null_count() < 0));
    if !needs_normalization { return Ok((None, false)); }

    let mut normalized = bytes[header].to_vec();
    let has_undefined_union = nulls::normalize_inspecting_unions(&mut normalized, body, schema)?;
    Ok((Some(normalized), has_undefined_union))
}

fn prepare_block(block: &arrow_ipc::Block, bytes: Buffer, schema: &Schema, dictionary: bool) -> Result<Buffer> {
    let (header, metadata_end) = block_message(block, &bytes)?;
    let (Some(normalized), _) = prepare_block_header(&bytes, header.clone(), metadata_end, schema, dictionary)? else {
        return Ok(bytes);
    };
    let mut owned = bytes.into_vec::<u8>().unwrap_or_else(|bytes| bytes.to_vec());
    owned[header].copy_from_slice(&normalized);
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
    Ranges(Box<dyn RetainedReader>),
    Retained { bytes: Bytes, position: usize },
}

impl<R: Read> StreamInput<R> {
    fn next_message(&mut self) -> Result<Option<(Cow<'_, [u8]>, Buffer)>> {
        match self {
            Self::Reader(reader) => {
                let Some((metadata, length, _)) = read_message_header(reader)? else { return Ok(None); };
                Ok(Some((Cow::Owned(metadata), read_message_body(reader, length)?)))
            }
            Self::Ranges(reader) => {
                let Some((metadata, length, record)) = read_message_header(reader.as_mut())? else { return Ok(None); };
                if record && length != 0 {
                    let start = reader.stream_position().map_err(invalid)?;
                    if let Ok(start) = usize::try_from(start) && let Some(end) = start.checked_add(length) {
                        if let Some(body) = reader.block_slice(start..end, true) {
                            reader.seek(SeekFrom::Start(end as u64)).map_err(invalid)?;
                            return Ok(Some((Cow::Owned(metadata), body)));
                        }
                    }
                }
                let body = read_message_body(reader.as_mut(), length)?;
                Ok(Some((Cow::Owned(metadata), body)))
            }
            Self::Retained { bytes, position } => {
                if *position == bytes.len() { return Ok(None); }
                let Some(StreamFrame { header, body, .. }) = stream_message(bytes, position)? else { return Ok(None); };
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
                Ok(Some((Cow::Borrowed(header), body)))
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

    /// Offers retained bytes for record bodies. A supplied range must match the reader and own its allocation charge.
    pub fn from_reader_with_retained_ranges(
        reader: impl Read + Seek + Send + 'static, projection: Option<Vec<usize>>,
        retain: impl FnMut(Range<u64>) -> Option<Bytes> + Send + 'static,
    ) -> Result<Self> {
        Self::from_input(StreamInput::Ranges(Box::new(RangeReader { reader: StreamReader(reader), retain, file_blocks: false })), projection)
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

fn read_message_header(reader: &mut (impl Read + ?Sized)) -> Result<Option<(Vec<u8>, usize, bool)>> {
    let mut prefix = [0; 4];
    if reader.read(&mut prefix[..1]).map_err(invalid)? == 0 { return Ok(None); }
    reader.read_exact(&mut prefix[1..]).map_err(invalid)?;
    let mut length = i32::from_le_bytes(prefix);
    if length == -1 { reader.read_exact(&mut prefix).map_err(invalid)?; length = i32::from_le_bytes(prefix); }
    if length == 0 { return Ok(None); }
    let mut metadata = vec![0; usize::try_from(length).map_err(invalid)?]; reader.read_exact(&mut metadata).map_err(invalid)?;
    let message = arrow_ipc::root_as_message(&metadata).map_err(invalid)?;
    let length = usize::try_from(message.bodyLength()).map_err(invalid)?;
    let record = message.header_type() == arrow_ipc::MessageHeader::RecordBatch;
    Ok(Some((metadata, length, record)))
}

fn read_message_body(reader: &mut (impl Read + ?Sized), length: usize) -> Result<Buffer> {
    let mut body = vec![0; length];
    reader.read_exact(&mut body).map_err(invalid)?;
    Ok(if body.is_empty() { Buffer::default() } else { Buffer::from(body) })
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

/// Applies Java stream validity and the decoded schema, including the omission of empty timestamp timezones.
/// The caller establishes that the Java stream writer models the batch layout.
pub fn normalize_java_stream_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    decoded_schema::normalize_stream_validity(batch)
}

/// Rebase sliced runs and their enclosing arrays for a native V5 stream layout.
/// The input batch already owns its value buffers, and the schema charge covers the new descriptors.
pub fn normalize_native_stream_batch(batch: &OwnedBatch, schema: Arc<MemoryReservation>) -> Result<OwnedBatch> {
    let normalized = decoded_schema::normalize_native_stream(batch.batch(), schema.budget())?;
    Ok(OwnedBatch::with_funded_columns(normalized, Arc::clone(&schema)).with_schema_reservation(schema))
}

/// Owns a complete IPC stream's decoded schema and independently funded pages.
pub struct OwnedStream {
    schema: SchemaRef,
    pages: Vec<OwnedBatch>,
    _schema_reservation: Arc<MemoryReservation>,
}

impl OwnedStream {
    /// The input must end with an IPC end marker. Its allocation belongs to the caller.
    pub fn read(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::read_contiguous(StreamSource::Borrowed(bytes), memory)
    }

    /// Requires a complete IPC stream whose input bytes own their allocation charge.
    pub fn read_bytes(bytes: Bytes, memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::read_contiguous(StreamSource::Bytes(&bytes), memory)
    }

    /// The input must end with an IPC end marker.
    /// Decoded arrays can retain the input allocation, so carry its existing charge in the buffer owner.
    pub fn read_buffer(bytes: Buffer, memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::read_contiguous(StreamSource::Buffer(&bytes), memory)
    }

    fn read_contiguous(source: StreamSource<'_>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let mut admission = StreamAdmission::new(memory);
        visit_stream_frames(source.bytes(), true, |frame| admission.push(source, frame))?;
        admission.decode(memory)
    }

    /// Reads a complete IPC stream with each message held in a separate funded buffer.
    pub fn read_frames(frames: &[Buffer], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let (end, frames) = frames.split_last().ok_or_else(|| invalid("Arrow stream has no end marker"))?;
        if end.as_slice() != [255, 255, 255, 255, 0, 0, 0, 0] {
            return Err(invalid("Arrow stream has no end marker"));
        }
        let mut admission = StreamAdmission::new(memory);
        for frame in frames {
            let mut position = 0;
            let parsed = stream_message(frame, &mut position)?
                .ok_or_else(|| invalid("Arrow stream ended before its last frame"))?;
            if position != frame.len() { return Err(invalid("Arrow stream frame contains trailing bytes")); }
            admission.push(StreamSource::Buffer(frame), parsed)?;
        }
        admission.decode(memory)
    }

    /// Borrow the schema under this stream's charge. Owned pages also carry the shared schema charge.
    pub fn schema(&self) -> &SchemaRef { &self.schema }
    pub fn pages(&self) -> &[OwnedBatch] { &self.pages }
    pub fn into_pages(self) -> Vec<OwnedBatch> { self.pages }
}

#[derive(Clone, Copy)]
enum StreamSource<'a> {
    Borrowed(&'a [u8]),
    Bytes(&'a Bytes),
    Buffer(&'a Buffer),
}

impl<'a> StreamSource<'a> {
    fn bytes(self) -> &'a [u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Bytes(bytes) => bytes.as_ref(),
            Self::Buffer(bytes) => bytes.as_slice(),
        }
    }

    fn needs_copy(self, bytes: &[u8], dense_unions: bool) -> bool {
        !bytes.is_empty() && match self {
            Self::Borrowed(_) => true,
            Self::Bytes(_) => bytes.as_ptr().align_offset(std::mem::align_of::<u128>()) != 0,
            Self::Buffer(_) => dense_unions && bytes.as_ptr().align_offset(std::mem::align_of::<i32>()) != 0,
        }
    }

    fn body(self, range: Range<usize>, dense_unions: bool, grant: &mut MemoryReservation) -> Result<Buffer> {
        if range.is_empty() { return Ok(Buffer::default()); }
        let bytes = &self.bytes()[range.clone()];
        if self.needs_copy(bytes, dense_unions) {
            let mut backing = grant.split_off(allocation_add(bytes.len(), 256)?)?;
            let body = Buffer::from_slice_ref(bytes);
            Arc::get_mut(&mut backing).expect("IPC body owns its grant")
                .shrink_to(allocation_add(body.capacity(), 128)?)?;
            return Ok(rtdb_columnar::retain_buffer(&body, &backing));
        }
        Ok(match self {
            Self::Bytes(bytes) => Buffer::from(bytes.slice(range)),
            Self::Buffer(bytes) => {
                let view = bytes.slice_with_length(range.start, range.len());
                rtdb_columnar::retain_buffer_with_owner(&view, Arc::new(()))
            }
            Self::Borrowed(_) => unreachable!("Borrowed IPC bodies are copied"),
        })
    }
}

struct AdmittedFrame<'a> {
    source: StreamSource<'a>,
    frame: StreamFrame<'a>,
    storage: usize,
}

struct StreamAdmission<'a> {
    frames: Result<(Vec<AdmittedFrame<'a>>, Arc<MemoryReservation>)>,
    storage: usize,
    scratch: usize,
    pages: usize,
    dictionaries: usize,
}

impl<'a> StreamAdmission<'a> {
    fn new(memory: &Arc<MemoryBudget>) -> Self {
        Self { frames: memory.reserve(0).map(|grant| (Vec::new(), grant)), storage: 0, scratch: 0,
            pages: 0, dictionaries: 0 }
    }

    fn push(&mut self, source: StreamSource<'a>, frame: StreamFrame<'a>) -> Result<()> {
        let body = &source.bytes()[frame.body.clone()];
        let message = &frame.message;
        let header_work = allocation_add(8192, allocation_mul(frame.header.len(), 16)?)?;
        let mut storage = 8192;
        let mut scratch = header_work;
        if let Some(schema) = message.header_as_schema() {
            validate_decimal_metadata(schema)?;
            storage = allocation_add(header_work, stream_schema_fields_bound(schema.fields().into_iter().flatten())?)?;
            storage = allocation_add(storage, stream_metadata_bound(schema.custom_metadata().into_iter().flatten())?)?;
            scratch = storage;
        }
        if let Some(batch) = stream_batch_header(message) {
            let workspace = codec_workspace(batch)?;
            storage = record_field_bound(storage, batch, arrow_descriptors)?;
            scratch = allocation_add(scratch, allocation_add(storage, workspace)?)?;
            visit_decoded_buffer_lengths(batch, body.len(), |start| compressed_buffer_length(body, start), |decoded| {
                storage = allocation_add(storage, allocation_add(decoded, 512)?)?;
                scratch = allocation_add(scratch, allocation_add(allocation_mul(decoded, 3)?, 256)?)?;
                Ok(())
            })?;
        }
        if source.needs_copy(body, true) {
            storage = allocation_add(storage, allocation_add(body.len(), 256)?)?;
        }
        self.storage = allocation_add(self.storage, storage)?;
        self.scratch = self.scratch.max(scratch);
        self.pages = allocation_add(self.pages, usize::from(message.header_type() == arrow_ipc::MessageHeader::RecordBatch))?;
        self.dictionaries = allocation_add(self.dictionaries, usize::from(message.header_type() == arrow_ipc::MessageHeader::DictionaryBatch))?;
        if let Ok((frames, grant)) = &mut self.frames {
            match cache_stream_frame(frames, grant) {
                Ok(()) => frames.push(AdmittedFrame { source, frame, storage }),
                // A late malformed frame takes precedence over cache admission.
                Err(error) => self.frames = Err(error),
            }
        }
        Ok(())
    }

    fn decode(self, memory: &Arc<MemoryBudget>) -> Result<OwnedStream> {
        let inventory = allocation_mul(self.dictionaries, 256)?;
        let bound = allocation_add(allocation_add(self.storage, self.scratch)?, inventory)?;
        let (frames, _frame_charge) = self.frames?;
        let mut charge = memory.reserve(bound)?;
        let grant = Arc::get_mut(&mut charge).expect("IPC stream owns its grant");
        let mut frames = frames.into_iter();
        let first = frames.next().ok_or_else(|| invalid("Arrow stream has no schema"))?;
        if !first.frame.body.is_empty() { return Err(invalid("Arrow schema has a body")); }
        let mut schema_charge = grant.split_off(first.storage)?;
        let schema = decoded_schema::adapt(&Arc::new(decode_schema_message(first.frame.message)?))?;
        Arc::get_mut(&mut schema_charge).expect("IPC schema owns its grant").shrink_to(retained_stream_schema_bound(&schema)?)?;
        let dense_unions = schema.fields().iter().any(|field| contains_dense_union(field.data_type()));
        let mut dictionaries = StreamDictionaries::new(&schema)?;
        let mut pages = Vec::with_capacity(self.pages);
        for frame in frames {
            let mut charge = grant.split_off(frame.storage)?;
            let frame_grant = Arc::get_mut(&mut charge).expect("IPC frame owns its grant");
            if let Some(page) = frame.decode(&schema, &schema_charge, dense_unions, &mut dictionaries, frame_grant)? {
                pages.push(page);
            }
        }
        Ok(OwnedStream { schema, pages, _schema_reservation: schema_charge })
    }
}

fn cache_stream_frame(frames: &mut Vec<AdmittedFrame<'_>>, grant: &mut Arc<MemoryReservation>) -> Result<()> {
    if frames.len() != frames.capacity() { return Ok(()); }
    let capacity = allocation_mul(frames.capacity(), 2)?.max(8);
    let grant = Arc::get_mut(grant).expect("IPC frame cache owns its grant");
    let bytes = allocation_mul(capacity, std::mem::size_of::<AdmittedFrame<'_>>())?;
    grant.grow_to(allocation_add(grant.bytes(), bytes)?)?;
    frames.try_reserve_exact(capacity - frames.len()).map_err(invalid)?;
    grant.shrink_to(bytes)
}

impl AdmittedFrame<'_> {
    fn decode(self, schema: &SchemaRef, schema_charge: &Arc<MemoryReservation>, dense_unions: bool,
        dictionaries: &mut StreamDictionaries, grant: &mut MemoryReservation,
    ) -> Result<Option<OwnedBatch>> {
        let body = self.source.body(self.frame.body, dense_unions, grant)?;
        let normalized;
        let message = if stream_batch_needs_normalization(&self.frame.message, &body)? {
            let mut metadata = self.frame.header.to_vec();
            nulls::normalize(&mut metadata, &body, schema)?;
            normalized = metadata;
            revalidate_normalized_message(self.frame.message, &normalized)?
        } else {
            self.frame.message
        };
        let body_identity = BufferIdentity::of(&body);
        dictionaries.admit_descriptors(grant)?;
        let is_funded = |buffer| buffer == body_identity || dictionaries.funds(buffer);
        if let Some(batch) = message.header_as_record_batch() {
            let batch = arrow_ipc::reader::read_record_batch(&body, batch, Arc::clone(schema), &dictionaries.arrow_values, None, &message.version()).map_err(invalid)?;
            return OwnedBatch::with_independent_buffers(batch, grant, Arc::clone(schema_charge), is_funded).map(Some);
        }
        let dictionary = message.header_as_dictionary_batch().ok_or_else(|| invalid("Unexpected Arrow stream message"))?;
        let values = decode_stream_dictionary(&body, dictionary, schema, &dictionaries.arrow_values, &message.version())?;
        let incoming = RetainedArray::new(values, grant, is_funded)?;
        let next = if dictionary.isDelta() {
            dictionaries.append(dictionary.id(), &incoming, grant.budget())?
        } else {
            incoming
        };
        dictionaries.install(dictionary.id(), next);
        Ok(None)
    }
}

struct StreamDictionaries {
    references: HashMap<i64, (usize, usize)>,
    values: HashMap<i64, RetainedArray>,
    arrow_values: HashMap<i64, ArrayRef>,
}

impl StreamDictionaries {
    #[allow(deprecated)]
    fn new(schema: &Schema) -> Result<Self> {
        let mut references = HashMap::new();
        for field in schema.flattened_fields() {
            if let (Some(id), DataType::Dictionary(_, values)) = (field.dict_id(), field.data_type()) {
                let (uses, missing_work) = references.entry(id).or_insert((0, 0));
                *uses = allocation_add(*uses, 1)?;
                *missing_work = allocation_add(*missing_work, allocation_mul(values.size(), 16)?)?;
            }
        }
        Ok(Self { references, values: HashMap::new(), arrow_values: HashMap::new() })
    }

    fn funds(&self, buffer: BufferIdentity) -> bool {
        self.values.values().any(|dictionary| dictionary.funds(buffer))
    }

    fn admit_descriptors(&self, grant: &mut MemoryReservation) -> Result<()> {
        let work = self.references.iter().try_fold(0, |bytes, (id, &(uses, missing_work))| {
            let extra = match self.values.get(id) {
                Some(dictionary) => allocation_mul(uses, allocation_mul(dictionary.descriptor_bytes(), 4)?)?,
                None => missing_work,
            };
            allocation_add(bytes, extra)
        })?;
        if work != 0 { grant.grow_to(allocation_add(grant.bytes(), work)?)?; }
        Ok(())
    }

    fn install(&mut self, id: i64, values: RetainedArray) {
        self.arrow_values.insert(id, Arc::clone(values.array()));
        self.values.insert(id, values);
    }

    fn append(&self, id: i64, incoming: &RetainedArray, memory: &Arc<MemoryBudget>) -> Result<RetainedArray> {
        let current = self.values.get(&id).ok_or_else(|| invalid(ArrowError::InvalidArgumentError(
            format!("No existing dictionary for delta dictionary with id '{id}'"),
        )))?;
        let inspection = memory.reserve(allocation_add(4096, allocation_mul(
            allocation_add(current.descriptor_bytes(), incoming.descriptor_bytes())?, 2,
        )?)?)?;
        let left = current.array().to_data();
        let right = incoming.array().to_data();
        let rows = allocation_add(left.len(), right.len())?;
        let bound = allocation_add(dictionary_concat_bound(&left, rows, 1)?, dictionary_concat_bound(&right, rows, 1)?)?;
        drop((left, right, inspection));
        let mut charge = memory.reserve(bound)?;
        let combined = arrow_select::concat::concat(&[current.array().as_ref(), incoming.array().as_ref()]).map_err(|error| {
            invalid(ArrowError::InvalidArgumentError(format!("Failed to concat delta dictionary: {error}")))
        })?;
        let grant = Arc::get_mut(&mut charge).expect("IPC dictionary concatenation owns its grant");
        RetainedArray::new(combined, grant, |buffer| self.funds(buffer) || incoming.funds(buffer))
    }
}

#[allow(deprecated)]
fn decode_stream_dictionary(body: &Buffer, dictionary: arrow_ipc::DictionaryBatch<'_>, schema: &Schema,
    dictionaries: &HashMap<i64, ArrayRef>, version: &arrow_ipc::MetadataVersion,
) -> Result<ArrayRef> {
    let fields = schema.fields_with_dict_id(dictionary.id());
    let Some(DataType::Dictionary(_, values)) = fields.first().map(|field| field.data_type()) else {
        return Err(invalid(ArrowError::InvalidArgumentError(format!("dictionary id {} not found in schema", dictionary.id()))));
    };
    let batch = dictionary.data().ok_or_else(|| invalid("Dictionary message has no record batch"))?;
    let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new("", values.as_ref().clone(), true)]));
    let batch = arrow_ipc::reader::read_record_batch(body, batch, schema, dictionaries, None, version).map_err(invalid)?;
    Ok(Arc::clone(batch.column(0)))
}

fn dictionary_concat_bound(data: &arrow_data::ArrayData, inherited_rows: usize, copies: usize) -> Result<usize> {
    let rows = inherited_rows.max(allocation_mul(data.len(), copies)?);
    let width = match data.data_type() {
        DataType::FixedSizeBinary(width) => usize::try_from(*width).map_err(invalid)?,
        data_type => data_type.primitive_width().unwrap_or(16),
    };
    let mut bytes = allocation_add(4096, allocation_mul(rows, allocation_add(width, 1)?)?)?;
    for buffer in data.buffers() {
        bytes = allocation_add(bytes, allocation_add(512, allocation_mul(allocation_mul(buffer.len(), copies)?, 4)?)?)?;
    }
    let (child_rows, child_copies) = match data.data_type() {
        DataType::FixedSizeList(_, width) => (allocation_mul(rows, usize::try_from(*width).map_err(invalid)?)?, copies),
        DataType::Dictionary(_, _) => {
            let values = allocation_mul(data.child_data()[0].len(), copies)?;
            bytes = allocation_add(bytes, allocation_mul(values, 256)?)?;
            (values, copies)
        }
        DataType::Union(_, UnionMode::Dense) if stream_dense_offsets_repeat(data) => (rows, allocation_mul(copies, data.len())?),
        DataType::ListView(_) if stream_list_view_offsets_overlap::<i32>(data) => (rows, allocation_mul(copies, data.len())?),
        DataType::LargeListView(_) if stream_list_view_offsets_overlap::<i64>(data) => (rows, allocation_mul(copies, data.len())?),
        _ => (rows, copies),
    };
    for child in data.child_data() { bytes = allocation_add(bytes, dictionary_concat_bound(child, child_rows, child_copies)?)?; }
    Ok(bytes)
}

fn stream_dense_offsets_repeat(data: &arrow_data::ArrayData) -> bool {
    let mut last = [-1; 128];
    data.buffer::<i8>(0).iter().zip(data.buffer::<i32>(1)).take(data.len()).any(|(&leg, &offset)| {
        let Ok(leg) = usize::try_from(leg) else { return true; };
        let previous = &mut last[leg];
        let repeats = offset <= *previous;
        *previous = offset;
        repeats
    })
}

fn stream_list_view_offsets_overlap<T: arrow_buffer::ArrowNativeType>(data: &arrow_data::ArrayData) -> bool {
    let mut end = 0;
    data.buffer::<T>(0).iter().zip(data.buffer::<T>(1)).take(data.len()).any(|(&offset, &size)| {
        let start = offset.as_usize();
        let overlaps = size.as_usize() != 0 && start < end;
        end = end.max(start.saturating_add(size.as_usize()));
        overlaps
    })
}

fn stream_schema_fields_bound<'a>(fields: impl Iterator<Item = arrow_ipc::Field<'a>>) -> Result<usize> {
    fields.into_iter().try_fold(0, |bytes, field| {
        let name = field.name().map_or(0, str::len);
        let timezone = field.type_as_timestamp().and_then(|ty| ty.timezone()).map_or(0, str::len);
        let union_ids = field.type_as_union().and_then(|ty| ty.typeIds()).map_or(0, |ids| ids.len());
        let strings = allocation_mul(allocation_add(name, timezone)?, 8)?;
        let entries = stream_metadata_bound(field.custom_metadata().into_iter().flatten())?;
        let children = stream_schema_fields_bound(field.children().into_iter().flatten())?;
        let field = allocation_add(2048, allocation_add(allocation_mul(union_ids, 16)?, strings)?)?;
        allocation_add(bytes, allocation_add(field, allocation_add(entries, children)?)?)
    })
}

fn stream_metadata_bound<'a>(entries: impl Iterator<Item = arrow_ipc::KeyValue<'a>>) -> Result<usize> {
    entries.into_iter().try_fold(0, |bytes, entry| {
        let length = allocation_add(entry.key().map_or(0, str::len), entry.value().map_or(0, str::len))?;
        allocation_add(bytes, allocation_add(512, allocation_mul(length, 8)?)?)
    })
}

fn retained_stream_schema_bound(schema: &Schema) -> Result<usize> {
    let metadata = schema.metadata();
    let slots = allocation_mul(metadata.capacity(), 64)?;
    let bytes = metadata.iter().try_fold(allocation_add(256, slots)?, |bytes, (key, value)| {
        allocation_add(bytes, allocation_add(key.capacity(), value.capacity())?)
    })?;
    schema.fields().iter().try_fold(bytes, |bytes, field| allocation_add(bytes, allocation_add(64, field.size())?))
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
    let message = if stream_batch_needs_normalization(&message, &body)? {
        nulls::normalize(metadata.to_mut(), &body, schema)?;
        arrow_ipc::root_as_message(&metadata).map_err(invalid)?
    } else {
        message
    };
    if let Some(batch) = message.header_as_record_batch() {
        return arrow_ipc::reader::read_record_batch(&body, batch, Arc::clone(schema), dictionaries, projection, &message.version()).map(Some).map_err(invalid);
    }
    let dictionary = message.header_as_dictionary_batch().ok_or_else(|| invalid("Unexpected Arrow stream message"))?;
    arrow_ipc::reader::read_dictionary(&body, dictionary, schema, dictionaries, &message.version()).map_err(invalid)?;
    Ok(None)
}

fn stream_batch_header<'a>(message: &arrow_ipc::Message<'a>) -> Option<arrow_ipc::RecordBatch<'a>> {
    message.header_as_record_batch().or_else(|| message.header_as_dictionary_batch().and_then(|dictionary| dictionary.data()))
}

fn stream_batch_needs_normalization(message: &arrow_ipc::Message<'_>, body: &[u8]) -> Result<bool> {
    let Some(batch) = stream_batch_header(message) else { return Ok(false); };
    validate_lz4_batch(batch, body)?;
    let nodes = batch.nodes().ok_or_else(|| invalid("Arrow batch has no field nodes"))?;
    Ok(nodes.iter().any(|node| node.null_count() < 0))
}

fn revalidate_normalized_message<'a>(original: arrow_ipc::Message<'_>, metadata: &'a [u8]) -> Result<arrow_ipc::Message<'a>> {
    let normalized = arrow_ipc::root_as_message(metadata).map_err(invalid)?;
    let changed = || invalid("Arrow null-count normalization changed the admitted batch layout");
    let before = stream_batch_header(&original).ok_or_else(changed)?;
    let after = stream_batch_header(&normalized).ok_or_else(changed)?;
    let compression = |batch: arrow_ipc::RecordBatch<'_>| batch.compression().map(|value| (value.method(), value.codec()));
    let dictionary = |message: arrow_ipc::Message<'_>| message.header_as_dictionary_batch().map(|value| (value.id(), value.isDelta()));
    if original.version() != normalized.version()
        || original.header_type() != normalized.header_type()
        || original.bodyLength() != normalized.bodyLength()
        || dictionary(original) != dictionary(normalized)
        || before.length() != after.length()
        || compression(before) != compression(after)
        || !before.buffers().into_iter().flatten().eq(after.buffers().into_iter().flatten())
        || !before.nodes().into_iter().flatten().map(|node| node.length())
            .eq(after.nodes().into_iter().flatten().map(|node| node.length()))
        || !before.variadicBufferCounts().into_iter().flatten().eq(after.variadicBufferCounts().into_iter().flatten())
    {
        return Err(changed());
    }
    Ok(normalized)
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
    message_bound(&message, metadata_length, body.len(), field_bound, |start| compressed_buffer_length(body, start))
}

fn message_bound(message: &arrow_ipc::Message<'_>, header_length: usize, body_length: usize, field_bound: fn(usize) -> Result<usize>, compressed_length: impl FnMut(usize) -> Result<i64>) -> Result<usize> {
    let mut bound = allocation_add(8192, allocation_add(allocation_mul(header_length, 16)?, allocation_mul(body_length, 4)?)?)?;
    if usize::try_from(message.bodyLength()).map_err(invalid)? > body_length { return Err(invalid("Arrow message body is truncated")); }
    if let Some(batch) = stream_batch_header(message) {
        bound = allocation_add(bound, codec_workspace(batch)?)?;
        bound = record_field_bound(bound, batch, field_bound)?;
        visit_decoded_buffer_lengths(batch, body_length, compressed_length, |decoded| {
            bound = allocation_add(bound, decoded_buffer_bound(decoded)?)?;
            Ok(())
        })?;
    }
    Ok(bound)
}

fn record_field_bound(mut bound: usize, batch: arrow_ipc::RecordBatch<'_>, field_bound: fn(usize) -> Result<usize>) -> Result<usize> {
    if let Some(nodes) = batch.nodes() {
        for node in nodes { bound = allocation_add(bound, field_bound(usize::try_from(node.length()).map_err(invalid)?)?)?; }
    }
    Ok(bound)
}

fn visit_decoded_buffer_lengths(batch: arrow_ipc::RecordBatch<'_>, body_length: usize,
    mut compressed_length: impl FnMut(usize) -> Result<i64>, mut visit: impl FnMut(usize) -> Result<()>,
) -> Result<()> {
    let compressed = batch.compression().is_some();
    if let Some(buffers) = batch.buffers() {
        for buffer in buffers {
            let (decoded, _) = decoded_buffer_length(buffer, body_length, compressed, &mut compressed_length)?;
            visit(decoded)?;
        }
    }
    Ok(())
}

fn codec_workspace(batch: arrow_ipc::RecordBatch<'_>) -> Result<usize> {
    match batch.compression().map(|compression| compression.codec()) {
        None => Ok(0),
        Some(arrow_ipc::CompressionType::LZ4_FRAME) => Ok(64 << 20),
        Some(arrow_ipc::CompressionType::ZSTD) => Ok(1 << 20),
        _ => Err(invalid("Unknown Arrow compression codec")),
    }
}

fn compressed_buffer_length(body: &[u8], start: usize) -> Result<i64> {
    let prefix: [u8; 8] = body.get(start..allocation_add(start, 8)?).ok_or_else(|| invalid("Compressed Arrow buffer has no length"))?.try_into().map_err(invalid)?;
    Ok(i64::from_le_bytes(prefix))
}

fn decoded_buffer_length(buffer: &arrow_ipc::Buffer, body_length: usize, compressed: bool, compressed_length: impl FnOnce(usize) -> Result<i64>) -> Result<(usize, bool)> {
    let (_, decoded, decompresses) = decoded_buffer_span(buffer, body_length, compressed, compressed_length)?;
    Ok((decoded, decompresses))
}

fn decoded_buffer_span(
    buffer: &arrow_ipc::Buffer, body_length: usize, compressed: bool,
    compressed_length: impl FnOnce(usize) -> Result<i64>,
) -> Result<(Range<usize>, usize, bool)> {
    let start = usize::try_from(buffer.offset()).map_err(invalid)?;
    let length = usize::try_from(buffer.length()).map_err(invalid)?;
    let end = allocation_add(start, length)?;
    if end > body_length { return Err(invalid("Arrow buffer exceeds its message body")); }
    let span = start..end;
    if !compressed || length == 0 { return Ok((span, length, false)); }
    if length < 8 { return Err(invalid("Compressed Arrow buffer has no length")); }
    let decoded = compressed_length(start)?;
    if decoded == -1 { Ok((span, length - 8, false)) } else { Ok((span, usize::try_from(decoded).map_err(invalid)?, decoded > 0)) }
}

fn decoded_buffer_bound(length: usize) -> Result<usize> { allocation_add(allocation_mul(length, 4)?, 512) }

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
            validate_lz4_frame(&bytes[8..], expected)?;
        }
    }
    Ok(())
}

fn validate_lz4_frame(bytes: &[u8], expected: usize) -> Result<()> {
    let mut decoder = lz4_flex::frame::FrameDecoder::new(bytes);
    let mut scratch = [0; 8192];
    let mut decoded = 0;
    loop {
        let read = decoder.read(&mut scratch).map_err(invalid)?;
        if read == 0 { break; }
        decoded = allocation_add(decoded, read)?;
        if decoded > expected { return Err(invalid("LZ4 data exceeds its declared Arrow buffer length")); }
    }
    if decoded != expected { return Err(invalid("LZ4 data differs from its declared Arrow buffer length")); }
    Ok(())
}

/// Bounds all messages retained by a decoded IPC stream, including dictionaries.
/// The encoded input has a separate lifetime and must retain its own allocation charge.
pub fn stream_allocation_bound(bytes: &[u8]) -> Result<usize> {
    stream_bound(bytes, false, row_descriptors)
}

/// Checks a complete, bounded IPC stream before decoding it one batch at a time.
pub fn validate_complete_stream(bytes: &[u8]) -> Result<()> {
    stream_bound(bytes, true, arrow_descriptors).map(drop)
}

fn stream_bound(bytes: &[u8], terminated: bool, field_bound: fn(usize) -> Result<usize>) -> Result<usize> {
    let mut bound = 8192;
    visit_stream_frames(bytes, terminated, |frame| {
        if let Some(schema) = frame.message.header_as_schema() { validate_decimal_metadata(schema)?; }
        let body = &bytes[frame.body];
        let message = message_bound(&frame.message, frame.header.len(), body.len(), field_bound, |start| compressed_buffer_length(body, start))?;
        bound = allocation_add(bound, message)?;
        Ok(())
    })?;
    Ok(bound)
}

fn visit_stream_frames<'a>(bytes: &'a [u8], terminated: bool, mut visit: impl FnMut(StreamFrame<'a>) -> Result<()>) -> Result<()> {
    let mut position = 0;
    while position < bytes.len() {
        let Some(frame) = stream_message(bytes, &mut position)? else {
            if terminated && position != bytes.len() { return Err(invalid("Bytes follow IPC stream end marker")); }
            return Ok(());
        };
        visit(frame)?;
    }
    if terminated { Err(invalid("IPC stream has no end marker")) } else { Ok(()) }
}

struct StreamFrame<'a> {
    header: &'a [u8],
    message: arrow_ipc::Message<'a>,
    body: Range<usize>,
}

fn stream_message<'a>(bytes: &'a [u8], position: &mut usize) -> Result<Option<StreamFrame<'a>>> {
    let mut length = inspection_i32(bytes, position)?;
    if length == -1 { length = inspection_i32(bytes, position)?; }
    if length == 0 { return Ok(None); }
    let end = allocation_add(*position, usize::try_from(length).map_err(invalid)?)?;
    let header = bytes.get(*position..end).ok_or_else(|| invalid("Arrow stream metadata is truncated"))?;
    let message = arrow_ipc::root_as_message(header).map_err(invalid)?;
    let body_end = allocation_add(end, usize::try_from(message.bodyLength()).map_err(invalid)?)?;
    if body_end > bytes.len() { return Err(invalid("Arrow stream body is truncated")); }
    *position = body_end;
    Ok(Some(StreamFrame { header, message, body: end..body_end }))
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
pub fn schema_translation_bound(schema: &Schema) -> Result<usize> { Ok(schema_metrics(schema, 64)?.0) }
fn dictionary_count(schema: &Schema) -> Result<usize> { Ok(schema_metrics(schema, 64)?.1) }
fn schema_metadata_bound(entries: &HashMap<String, String>) -> Result<usize> {
    entries.iter().try_fold(0, |bytes, (key, value)| allocation_add(bytes, allocation_add(512, allocation_mul(allocation_add(key.len(), value.len())?, 16)?)?))
}

fn field_metadata_bound(field: &arrow_schema::Field) -> Result<usize> {
    allocation_add(allocation_mul(field.name().len(), 16)?, schema_metadata_bound(field.metadata())?)
}

fn schema_metrics(schema: &Schema, max_depth: usize) -> Result<(usize, usize)> {
    fn field(value: &arrow_schema::Field, depth: usize, max_depth: usize) -> Result<(usize, usize)> {
        let (bytes, dictionaries) = data_type(value.data_type(), depth, max_depth)?;
        Ok((allocation_add(bytes, field_metadata_bound(value)?)?, dictionaries))
    }
    fn data_type(value: &arrow_schema::DataType, depth: usize, max_depth: usize) -> Result<(usize, usize)> {
        use arrow_schema::DataType;
        if depth > max_depth { return Err(invalid("Arrow schema exceeds the encoding depth limit")); }
        let mut bytes = 4096; let mut dictionaries = 0;
        let mut add = |(next_bytes, next_dictionaries)| -> Result<()> { bytes = allocation_add(bytes, next_bytes)?; dictionaries = allocation_add(dictionaries, next_dictionaries)?; Ok(()) };
        match value {
            DataType::Struct(fields) => for value in fields { add(field(value, depth + 1, max_depth)?)?; },
            DataType::Union(fields, _) => for (_, value) in fields.iter() { add(field(value, depth + 1, max_depth)?)?; },
            DataType::List(value) | DataType::LargeList(value) | DataType::ListView(value) | DataType::LargeListView(value) | DataType::FixedSizeList(value, _) | DataType::Map(value, _) => add(field(value, depth + 1, max_depth)?)?,
            DataType::RunEndEncoded(ends, values) => { add(field(ends, depth + 1, max_depth)?)?; add(field(values, depth + 1, max_depth)?)?; }
            DataType::Dictionary(key, value) => { add(data_type(key, depth + 1, max_depth)?)?; add(data_type(value, depth + 1, max_depth)?)?; add((0, 1))?; }
            DataType::Timestamp(_, Some(zone)) => add((allocation_mul(zone.len(), 16)?, 0))?,
            _ => {}
        }
        Ok((bytes, dictionaries))
    }
    schema.fields().iter().try_fold((allocation_add(65_536, schema_metadata_bound(schema.metadata())?)?, 0), |(bytes, dictionaries), value| {
        let (next_bytes, next_dictionaries) = field(value, 0, max_depth)?;
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

/// Incremental JVM-compatible IPC. The sink owns its byte admission, while
/// the encoder funds schema, per-page scratch, and the final footer.
pub struct ArrowFileEncoder<W: Write> {
    file: JavaFile<W>,
    schema: Arc<MemoryReservation>,
    blocks: Arc<MemoryReservation>,
    per_page: usize,
}

impl<W: Write> ArrowFileEncoder<W> {
    pub fn new(sink: W, schema: &Schema, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let grant = memory.reserve(schema_translation_bound(schema)?)?;
        let per_page = allocation_mul(allocation_add(dictionary_count(schema)?, 1)?, 512)?;
        let blocks = memory.reserve(0)?;
        let file = JavaFile::new(sink, schema)?;
        Ok(Self { file, schema: grant, blocks, per_page })
    }

    pub fn write(&mut self, page: &RecordBatch) -> Result<()> {
        let scratch = allocation_add(self.schema.bytes(), allocation_mul(page.get_array_memory_size(), 4)?)?;
        let _scratch = self.schema.budget().reserve(scratch)?;
        let retained = allocation_add(self.blocks.bytes(), self.per_page)?;
        Arc::get_mut(&mut self.blocks).expect("Encoder owns its footer blocks").grow_to(retained)?;
        self.file.write(page)
    }

    pub fn finish(self) -> Result<W> {
        let footer = allocation_add(self.schema.bytes(), self.blocks.bytes())?;
        let _footer = self.schema.budget().reserve(footer)?;
        self.file.finish()
    }
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
        let layout = java_layout::JavaLayout::new_file(sink, 8);
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
        let footer = layout.file_footer().map_err(invalid)?;
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

struct LimitedFile {
    file: File,
    position: u64,
    limit: u64,
    sha256: Option<sha2::Sha256>,
    page_sha256: Option<sha2::Sha256>,
}

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
        if let Some(hash) = &mut self.page_sha256 {
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
        let writer = JavaFile::new(LimitedFile { file, position: 0, limit, sha256, page_sha256: None }, schema)?;
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

    /// Returns the exact file range and digest of a dictionary-free record page.
    pub fn write_hashed_page(&mut self, page: &RecordBatch) -> Result<(Range<u64>, [u8; 32])> {
        if dictionary_count(page.schema_ref())? != 0 {
            return Err(invalid("Arrow page hashing requires a dictionary-free schema"));
        }
        // The lookahead normally carries the previous frame's last eight bytes into the next write.
        self.writer.writer.get_mut().write_pending().map_err(invalid)?;
        let sink = self.writer.sink();
        let start = sink.position;
        sink.page_sha256 = Some(sha2::Digest::new());
        let written = self.write(page).and_then(|()| {
            self.writer.writer.get_mut().write_pending().map_err(invalid)
        });
        let sink = self.writer.sink();
        let hash = sink.page_sha256.take().expect("page hashing is active");
        written?;
        Ok((start..sink.position, sha2::Digest::finalize(hash).into()))
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

    /// Borrows the input schema supplied to `new`.
    pub fn schema_ref(&self) -> &SchemaRef {
        &self.schema
    }

    /// Bounds the retained schema and its wire frame, including the encoder's inline storage.
    pub fn retained_bytes(&self) -> Result<usize> {
        let schema = retained_stream_schema_bound(&self.schema)?;
        let frame = allocation_add(schema, self.schema_frame.capacity())?;
        allocation_add(std::mem::size_of::<Self>(), frame)
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
    write_prefixed_arrow_stream(schema, pages, options, &[])
}

/// Encodes the IPC stream into the same allocation as its transport prefix.
pub fn write_prefixed_arrow_stream(schema: &Schema, pages: &[RecordBatch], options: IpcWriteOptions, prefix: &[u8]) -> Result<Vec<u8>> {
    let decimal_schema = crate::ipc_decimal::DecimalSchema::new(schema)?;
    for page in pages { decimal_schema.validate_batch(page)?; }
    let mut writer = StreamWriter::try_new_with_options(prefix.to_vec(), &decimal_schema.wire, options).map_err(invalid)?;
    for page in pages { writer.write(page).map_err(invalid)?; }
    writer.finish().map_err(invalid)?;
    let mut bytes = writer.into_inner().map_err(invalid)?;
    patch_schema_frame(&mut bytes[prefix.len()..], &decimal_schema)?;
    Ok(bytes)
}

pub enum UncompressedBuffer<'bytes, 'spans> {
    Slice(&'bytes [u8]),
    Spans { length: usize, spans: &'spans mut dyn Iterator<Item = &'bytes [u8]> },
}

impl UncompressedBuffer<'_, '_> {
    fn len(&self) -> usize {
        match self {
            Self::Slice(bytes) => bytes.len(),
            Self::Spans { length, .. } => *length,
        }
    }
}

/// Writes one null-free, dictionary-free batch as a native Arrow V5 stream with 64-byte alignment.
/// Supply node lengths in field traversal order and logical buffers in IPC order, including empty validity slots.
/// A logical buffer's spans must total its declared length and share one padding boundary.
/// The sink can contain an incomplete stream on error and the caller owns flushing.
pub fn write_uncompressed_stream_from_buffers(
    mut output: impl Write, schema: &Schema, rows: usize, node_lengths: &[usize], buffers: &mut [UncompressedBuffer<'_, '_>],
) -> std::result::Result<(), ArrowError> {
    const ALIGNMENT: usize = 64;
    const PADDING: [u8; ALIGNMENT] = [0; ALIGNMENT];
    let length_error = || ArrowError::InvalidArgumentError("Arrow IPC length exceeds the format".into());
    let ipc_length = |length: usize| i64::try_from(length).map_err(|_| length_error());
    let options = IpcWriteOptions::try_new(ALIGNMENT, false, arrow_ipc::MetadataVersion::V5)?;
    let nodes = node_lengths.iter().map(|&len| Ok(arrow_ipc::FieldNode::new(ipc_length(len)?, 0)))
        .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
    let mut body_length = 0usize;
    let descriptors = buffers.iter().map(|buffer| {
        let length = buffer.len();
        let descriptor = arrow_ipc::Buffer::new(ipc_length(body_length)?, ipc_length(length)?);
        let padded_length = length.checked_next_multiple_of(ALIGNMENT).ok_or_else(length_error)?;
        body_length = body_length.checked_add(padded_length).ok_or_else(length_error)?;
        Ok(descriptor)
    }).collect::<std::result::Result<Vec<_>, ArrowError>>()?;

    // Immutable-object retries require the FlatBuffer bytes produced by Arrow's native StreamWriter.
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let descriptors = builder.create_vector(&descriptors);
    let nodes = builder.create_vector(&nodes);
    let mut batch = arrow_ipc::RecordBatchBuilder::new(&mut builder);
    batch.add_length(ipc_length(rows)?);
    batch.add_nodes(nodes);
    batch.add_buffers(descriptors);
    let batch = batch.finish();
    let mut message = arrow_ipc::MessageBuilder::new(&mut builder);
    message.add_version(arrow_ipc::MetadataVersion::V5);
    message.add_header_type(arrow_ipc::MessageHeader::RecordBatch);
    message.add_bodyLength(ipc_length(body_length)?);
    message.add_header(batch.as_union_value());
    let message = message.finish();
    builder.finish(message, None);

    let schema = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
        schema, &mut DictionaryTracker::new(false), &options,
    );
    write_message(&mut output, schema, &options)?;
    let batch_metadata = EncodedData { ipc_message: builder.finished_data().to_vec(), arrow_data: Vec::new() };
    write_message(&mut output, batch_metadata, &options)?;
    for buffer in buffers {
        let length = buffer.len();
        match buffer {
            UncompressedBuffer::Slice(bytes) => output.write_all(bytes)?,
            UncompressedBuffer::Spans { spans, .. } => {
                let mut remaining = length;
                for bytes in spans {
                    remaining = remaining.checked_sub(bytes.len()).ok_or_else(|| {
                        ArrowError::InvalidArgumentError("Arrow IPC buffer spans exceed the declared length".into())
                    })?;
                    output.write_all(bytes)?;
                }
                if remaining != 0 {
                    return Err(ArrowError::InvalidArgumentError("Arrow IPC buffer spans fall short of the declared length".into()));
                }
            }
        }
        output.write_all(&PADDING[..length.next_multiple_of(ALIGNMENT) - length])?;
    }
    output.write_all(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0])?;
    Ok(())
}

/// Writes pages without buffering the complete stream, with metadata in the reference's Java layout; the sink owns and enforces its allocation bound.
/// The caller reserves the schema translation and initial encoded schema frame through this call.
/// A page with different native field metadata is rejected before encoding; the sink may already contain a valid prefix.
/// Dictionary, view, list-view and run-end columns, which XTDB never writes, are written in the plain layout holding the same values,
/// so every stream stays readable by XTDB.
pub fn write_stream_to<'a, W: Write>(schema: &Schema, pages: impl IntoIterator<Item = &'a RecordBatch>, sink: W) -> Result<W> {
    if java_stream::supports(schema) {
        return java_stream::write_stream(schema, pages, sink);
    }
    let plain = Arc::new(plain_layout::plain_schema(schema));
    if !java_stream::supports(&plain) {
        return Err(invalid(format!("Arrow layout XTDB does not write: {schema}")));
    }
    let pages = pages.into_iter().map(|page| {
        if page.schema_ref().as_ref() != schema {
            return Err(Anomaly::incorrect("rtdb.format/arrow-schema", "Arrow page schema differs from its declared stream schema"));
        }
        plain_layout::plain_page(&plain, page)
    }).collect::<Result<Vec<_>>>()?;
    java_stream::write_stream(&plain, &pages, sink)
}

struct TailWriter<W> { inner: W, pending: Vec<u8> }

impl<W: Write> TailWriter<W> {
    fn write_pending(&mut self) -> std::io::Result<()> {
        self.inner.write_all(&self.pending)?;
        self.pending.clear();
        Ok(())
    }
}

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
    decode_schema_message(arrow_ipc::root_as_message(bytes).map_err(invalid)?)
}

fn decode_schema_message(message: arrow_ipc::Message<'_>) -> Result<Schema> {
    crate::ipc_decimal::decode(message.header_as_schema().ok_or_else(|| invalid("Arrow message has no schema"))?)
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
fn native_stream_normalization_rebases_sliced_list_runs_without_copying_values() {
    use arrow_array::{Array, Int16Array, Int64Array, ListArray, RunArray, TimestampMicrosecondArray, cast::AsArray, types::Int16Type};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::Field;

    let details = arrow_array::StructArray::new(
        vec![Field::new("code", DataType::Int64, false)].into(),
        vec![Arc::new(Int64Array::from(vec![Some(10), Some(20), None, Some(40)]))],
        Some(NullBuffer::from(vec![true, true, false, true])),
    );
    let runs = RunArray::<Int16Type>::try_new(&Int16Array::from(vec![2, 4, 6, 8]), &details).unwrap();
    let lists = ListArray::new(
        Arc::new(Field::new("item", runs.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 3, 5, 7, 8].into()),
        Arc::new(runs),
        None,
    ).slice(1, 2);
    let times = TimestampMicrosecondArray::from(vec![83, 85]).with_timezone("");
    let batch = RecordBatch::try_from_iter([
        ("run_lists", Arc::new(lists) as ArrayRef),
        ("observed", Arc::new(times) as ArrayRef),
    ]).unwrap();
    let memory = MemoryBudget::new(16 << 20);
    let input = OwnedBatch::new(batch, &memory).unwrap();
    let batch = input.batch();
    let reference = write_stream(batch.schema_ref(), std::slice::from_ref(batch)).unwrap();
    let reference = OwnedStream::read(&reference, &memory).unwrap();
    let charge = memory.reserve(schema_translation_bound(batch.schema_ref()).unwrap()).unwrap();
    let normalized = normalize_native_stream_batch(&input, charge).unwrap();
    let normalized = normalized.batch();
    assert_eq!(normalized, reference.pages()[0].batch());
    let lists = normalized.column(0).as_list::<i32>();
    assert_eq!(lists.value_offsets(), &[0, 2, 4]);
    let runs = lists.values().as_any().downcast_ref::<RunArray<Int16Type>>().unwrap();
    assert_eq!(runs.run_ends().values(), &[1, 3, 4]);
    let before = batch.column(0).as_list::<i32>().values().as_any().downcast_ref::<RunArray<Int16Type>>().unwrap();
    let before = before.values().as_struct().column(0).as_primitive::<arrow_array::types::Int64Type>().values();
    let after = runs.values().as_struct().column(0).as_primitive::<arrow_array::types::Int64Type>().values();
    assert_eq!(after.as_ptr(), before[1..].as_ptr());
    assert!(runs.values().as_struct().is_null(1));
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
fn corrupt_persisted_pages_fail_full_decode_validation() {
    use arrow_array::StringArray;
    use arrow_ipc::writer::FileWriter;

    let batch = RecordBatch::try_from_iter([("_iid", Arc::new(StringArray::from(vec!["ab", "cd"])) as ArrayRef)]).unwrap();
    let mut writer = FileWriter::try_new(Vec::new(), batch.schema_ref()).unwrap();
    writer.write(&batch).unwrap();
    let mut bytes = writer.into_inner().unwrap();
    let offsets: Vec<u8> = [0i32, 2, 4].iter().flat_map(|offset| offset.to_le_bytes()).collect();
    let at = bytes.windows(offsets.len()).position(|window| window == offsets.as_slice()).unwrap();
    // The last offset now points past the two-row values buffer; only full validation notices.
    bytes[at + 8..at + 12].copy_from_slice(&4096i32.to_le_bytes());
    let bytes = Buffer::from(bytes);
    let layout = inspect_file(&mut Cursor::new(bytes.as_slice())).unwrap();
    let file = ArrowFile::from_inspected_buffer(bytes.clone(), None, layout).unwrap();
    let range = file.pages.page_range(0).unwrap();
    let wire = bytes.slice_with_length(range.start as usize, (range.end - range.start) as usize);

    assert!(file.pages.decode_page(0, wire.clone()).is_err());
    assert!(file.pages.decode_iid_page(0, wire.clone()).is_err(), "a corrupt projected page must not be retried in full");
    assert!(file.pages.decode_event_page(0, wire, &[]).is_err());
}

#[test]
fn projected_page_declines_dictionaries_and_mismatched_schemas_before_decode() {
    use arrow_array::{builder::StringDictionaryBuilder, types::Int8Type};
    use arrow_ipc::writer::FileWriter;

    let mut values = StringDictionaryBuilder::<Int8Type>::new();
    values.append("selected").unwrap();
    let batch = RecordBatch::try_from_iter([("_iid", Arc::new(values.finish()) as ArrayRef)]).unwrap();
    let mut writer = FileWriter::try_new(Vec::new(), batch.schema_ref()).unwrap();
    writer.write(&batch).unwrap();
    let bytes = Buffer::from(writer.into_inner().unwrap());
    let layout = inspect_file(&mut Cursor::new(bytes.as_slice())).unwrap();
    let mut file = ArrowFile::from_inspected_buffer(bytes.clone(), None, layout).unwrap();
    let range = file.pages.page_range(0).unwrap();
    let wire = bytes.slice_with_length(range.start as usize, (range.end - range.start) as usize);
    let memory = MemoryBudget::new(1 << 20);

    // The projected decoder has no dictionaries, so a selected dictionary column is declined before decode.
    let attempt = file.pages.decode_projected_buffer_reserved(0, &wire, PageProjection::Iid, |size| memory.reserve(size)).unwrap();
    assert!(matches!(attempt, ProjectionAttempt::Fallback(ProjectionFallback::PlanDeclined)));
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(file.read_page(0).unwrap(), batch);

    // Use a mismatched selected schema to make the planner decline before decode.
    file.pages.decoded_schema = Arc::new(Schema::empty());
    let attempt = file.pages.decode_projected_buffer_reserved(0, &wire, PageProjection::PutColumns(&[]), |size| memory.reserve(size)).unwrap();
    assert!(matches!(attempt, ProjectionAttempt::Fallback(ProjectionFallback::PlanDeclined)));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn seekable_event_projection_funds_only_selected_decode_and_wire_with_unknown_null_counts() {
    use crate::data::{EventBatch, StoredEvent, StoredOperation, build_event_batch};
    use rtdb_types::{Iid, Interval, StructFields, Timestamp, Value};

    let event = StoredEvent {
        position: None, origin: None, entity_key: None, iid: Iid([4; 16]),
        system_from: Timestamp(7), commit_position: 0, valid_time: Interval::ALL, recency: None,
        operation: StoredOperation::Put(StructFields::from([
            ("_id".into(), Value::I64(1)),
            ("selected".into(), Value::I64(42)),
            ("ignored".into(), Value::Utf8("x".repeat(256 << 10))),
        ])),
    };
    let StoredOperation::Put(document) = &event.operation else { unreachable!() };
    let types = document.iter().map(|(name, value)| (name.clone(), value.logical_type())).collect();
    let batch = build_event_batch(std::slice::from_ref(&event), Some(types), None).unwrap();
    let mut writer = arrow_ipc::writer::FileWriter::try_new(Vec::new(), batch.schema_ref()).unwrap();
    writer.write(&batch).unwrap();
    let mut bytes = writer.into_inner().unwrap();
    let tail = bytes.len() - 10;
    let footer = tail - arrow_ipc::reader::read_footer_length(bytes[tail..].try_into().unwrap()).unwrap();
    let pages = PageDecoder::from_footer(&bytes[footer..tail], footer as u64, None).unwrap();
    let range = pages.page_range(0).unwrap();
    let block = &bytes[range.start as usize..range.end as usize];
    let (header, _) = block_message(pages.block(0).unwrap(), block).unwrap();
    let message = arrow_ipc::root_as_message(&block[header]).unwrap();
    let offsets: Vec<_> = message.header_as_record_batch().unwrap().nodes().unwrap().iter()
        .map(|node| node.0.as_ptr() as usize - bytes.as_ptr() as usize + 8).collect();
    for offset in offsets { bytes[offset..offset + 8].copy_from_slice(&(-1_i64).to_le_bytes()); }
    let mut reader = ArrowFile::from_reader(Cursor::new(bytes), None).unwrap();
    let memory = MemoryBudget::new(512 << 10);
    assert!(reader.page_allocation_bound(0).unwrap() > memory.limit_bytes());
    let (page, charge) = reader.read_event_page_reserved(0, &["selected".into()], &memory).unwrap();
    assert!(memory.used_bytes() >= reader.encoded_page_size(0).unwrap() + charge.bytes());
    let page = EventBatch::new(page).unwrap();
    let actual = page.read_event(0).unwrap();
    assert_eq!(actual.header(), event.header());
    let StoredOperation::Put(fields) = actual.operation else { panic!("Expected the put leg") };
    assert_eq!(fields.get("selected"), Some(&Value::I64(42)));
    assert!(!fields.contains_key("ignored"));
    drop((page, charge));
    assert_eq!(memory.used_bytes(), 0);
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
    let StreamFrame { body, .. } = stream_message(&bytes, &mut position).unwrap().unwrap();
    assert_ne!(bytes[body].as_ptr().align_offset(std::mem::align_of::<i32>()), 0);

    let stream = OwnedStream::read_buffer(bytes, &MemoryBudget::new(1 << 20)).unwrap();
    assert_eq!(stream.pages()[0].batch(), &page);
}

#[test]
fn owned_stream_frames_hold_only_the_wire_for_live_dictionary_generations() {
    use arrow_array::{DictionaryArray, Int32Array, StringArray, types::Int32Type};

    let dictionary_page = |value: &str| {
        let dictionary = DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), Arc::new(StringArray::from(vec![value]))).unwrap();
        RecordBatch::try_from_iter([("value", Arc::new(dictionary) as ArrayRef)]).unwrap()
    };
    let first = dictionary_page("first dictionary generation");
    let second = dictionary_page("second dictionary generation");
    let bytes = write_arrow_stream(first.schema_ref(), &[first.clone(), second.clone()], IpcWriteOptions::default()).unwrap();
    let wire = MemoryBudget::new(1 << 20);
    let mut frames = Vec::new();
    let mut position = 0;
    let mut start = 0;
    let mut expected_wire = 0;
    while position < bytes.len() {
        let _frame = stream_message(&bytes, &mut position).unwrap();
        let frame = Buffer::from_slice_ref(&bytes[start..position]);
        let charge = wire.reserve(frame.capacity()).unwrap();
        if frames.len() == 1 { expected_wire = charge.bytes(); }
        frames.push(rtdb_columnar::retain_buffer(&frame, &charge));
        start = position;
    }
    let memory = MemoryBudget::new(1 << 20);
    let stream = OwnedStream::read_frames(&frames, &memory).unwrap();
    assert_eq!(stream.pages()[0].batch(), &first);
    assert_eq!(stream.pages()[1].batch(), &second);

    let values = Arc::clone(stream.pages()[0].batch().column(0)
        .as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap().values());
    let all = wire.used_bytes();
    drop(frames);
    assert!(wire.used_bytes() < all);
    drop(stream);
    assert_eq!(wire.used_bytes(), expected_wire);
    assert_eq!(values.as_any().downcast_ref::<StringArray>().unwrap().value(0), "first dictionary generation");
    drop(values);
    assert_eq!(wire.used_bytes(), 0);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn owned_stream_delta_dictionaries_leave_earlier_pages_unchanged() {
    use arrow_array::{DictionaryArray, Int32Array, StringArray, types::Int32Type};
    use arrow_ipc::writer::DictionaryHandling;

    let dictionary_page = |key: i32, values: Vec<&str>| {
        let dictionary = DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![key]), Arc::new(StringArray::from(values))).unwrap();
        RecordBatch::try_from_iter([("value", Arc::new(dictionary) as ArrayRef)]).unwrap()
    };
    let first = dictionary_page(0, vec!["first"]);
    let second = dictionary_page(1, vec!["first", "second"]);
    let options = IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
    let bytes = write_arrow_stream(first.schema_ref(), &[first.clone(), second.clone()], options).unwrap();
    let mut has_delta = false;
    visit_stream_frames(&bytes, true, |frame| {
        has_delta |= frame.message.header_as_dictionary_batch().is_some_and(|dictionary| dictionary.isDelta());
        Ok(())
    }).unwrap();
    assert!(has_delta);
    let memory = MemoryBudget::new(1 << 20);
    let stream = OwnedStream::read(&bytes, &memory).unwrap();
    assert_eq!(stream.pages()[0].batch(), &first);
    assert_eq!(stream.pages()[1].batch(), &second);
    drop(stream);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn complete_stream_reports_late_framing_errors_when_its_frame_cache_cannot_be_funded() {
    use arrow_array::Int64Array;

    let batch = RecordBatch::try_from_iter([("n", Arc::new(Int64Array::from(vec![1])) as ArrayRef)]).unwrap();
    let bytes = write_arrow_stream(batch.schema_ref(), &[batch.clone(), batch.clone()], IpcWriteOptions::default()).unwrap();
    let mut position = 0;
    stream_message(&bytes, &mut position).unwrap().unwrap();
    stream_message(&bytes, &mut position).unwrap().unwrap();

    let memory = MemoryBudget::new(0);
    let valid = OwnedStream::read(&bytes, &memory).err().unwrap();
    assert_eq!(valid.code, "rtdb/memory-budget-exceeded");
    let malformed = OwnedStream::read(&bytes[..position + 1], &memory).err().unwrap();
    assert_eq!(malformed.message.as_deref(), Some("Arrow IPC prefix is truncated"));
    let mut trailing = bytes;
    trailing.push(1);
    let malformed = OwnedStream::read(&trailing, &memory).err().unwrap();
    assert_eq!(malformed.message.as_deref(), Some("Bytes follow IPC stream end marker"));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn owned_stream_rejects_lz4_output_larger_than_its_declared_buffer() {
    use arrow_array::Int64Array;

    let batch = RecordBatch::try_from_iter([("n", Arc::new(Int64Array::from(vec![7; 4096])) as ArrayRef)]).unwrap();
    let options = IpcWriteOptions::default().try_with_compression(Some(arrow_ipc::CompressionType::LZ4_FRAME)).unwrap();
    let mut bytes = write_arrow_stream(batch.schema_ref(), std::slice::from_ref(&batch), options).unwrap();
    let start = {
        let mut position = 0;
        stream_message(&bytes, &mut position).unwrap().unwrap();
        let frame = stream_message(&bytes, &mut position).unwrap().unwrap();
        let batch = frame.message.header_as_record_batch().unwrap();
        let buffer = batch.buffers().unwrap().iter().find(|buffer| buffer.length() > 0).unwrap();
        frame.body.start + buffer.offset() as usize
    };
    bytes[start..start + 8].copy_from_slice(&1i64.to_le_bytes());
    let memory = MemoryBudget::new(72 << 20);
    let error = OwnedStream::read(&bytes, &memory).err().unwrap();
    assert_eq!(error.message.as_deref(), Some("LZ4 data exceeds its declared Arrow buffer length"));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn owned_stream_revalidates_batch_layout_after_normalizing_metadata() {
    use arrow_array::Int64Array;

    let page = |rows| RecordBatch::try_from_iter([("n", Arc::new(Int64Array::from(vec![1; rows])) as ArrayRef)]).unwrap();
    let first = page(4);
    let second = page(8);
    let bytes = write_arrow_stream(first.schema_ref(), &[first.clone(), second], IpcWriteOptions::default()).unwrap();
    let mut position = 0;
    stream_message(&bytes, &mut position).unwrap().unwrap();
    let first = stream_message(&bytes, &mut position).unwrap().unwrap();
    let second = stream_message(&bytes, &mut position).unwrap().unwrap();

    assert!(revalidate_normalized_message(first.message, first.header).is_ok());
    let error = revalidate_normalized_message(first.message, second.header).unwrap_err();
    assert_eq!(error.message.as_deref(), Some("Arrow null-count normalization changed the admitted batch layout"));
}
