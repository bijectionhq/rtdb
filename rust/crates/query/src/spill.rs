use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use arrow_array::{OffsetSizeTrait, RecordBatch};
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_ipc::{
    MetadataVersion,
    reader::{StreamDecoder, read_record_batch},
    writer::{CompressionContext, DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions, write_message},
};
use arrow_schema::{ArrowError, DataType, SchemaRef};
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_format::ipc::RecordSpans;
use rtdb_types::Result;
use tempfile::{NamedTempFile, TempDir, TempPath};

use crate::{
    columnar::ColumnBatch,
    fragment::{FragmentBytes, decode_batches, encode_batch},
    invalid,
};

const IPC_ALIGNMENT: usize = 64;

/// A query's spill files, in a directory created by the first of them: most queries never spill.
pub(crate) struct SpillRegion { directory: std::sync::OnceLock<TempDir>, limit: u64, retained: AtomicU64, peak: AtomicU64 }
impl SpillRegion {
    pub fn new(limit: u64) -> Result<Self> { Ok(Self { directory: std::sync::OnceLock::new(), limit, retained: AtomicU64::new(0), peak: AtomicU64::new(0) }) }
    fn directory(&self) -> Result<&std::path::Path> {
        if let Some(directory) = self.directory.get() { return Ok(directory.path()); }
        let created = TempDir::new().map_err(io_error)?;
        Ok(self.directory.get_or_init(|| created).path())
    }
    pub fn retained_bytes(&self) -> u64 { self.retained.load(Ordering::Acquire) }
    pub fn peak_bytes(&self) -> u64 { self.peak.load(Ordering::Relaxed) }
    fn reserve(&self, bytes: u64) -> Result<()> {
        let old = self.retained.fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| old.checked_add(bytes).filter(|next| *next <= self.limit))
            .map_err(|_| invalid("spill-limit", "Query spill byte limit exceeded"))?;
        self.peak.fetch_max(old + bytes, Ordering::Relaxed);
        Ok(())
    }
    pub fn create(self: &Arc<Self>) -> Result<SpillWriter> {
        let (file, path) = NamedTempFile::new_in(self.directory()?).map_err(io_error)?.into_parts();
        Ok(SpillWriter { file: BufWriter::new(file), position: 0, retained: Arc::new(SpillFile { path, region: Arc::clone(self), bytes: AtomicU64::new(0) }) })
    }
}

pub(crate) struct SpillFile { path: TempPath, region: Arc<SpillRegion>, bytes: AtomicU64 }
impl Drop for SpillFile {
    fn drop(&mut self) { self.region.retained.fetch_sub(self.bytes.load(Ordering::Relaxed), Ordering::AcqRel); }
}
impl SpillFile {
    pub fn reader(self: &Arc<Self>, memory: Arc<MemoryBudget>) -> Result<SpillReader> {
        let file = File::open(&self.path).map_err(io_error)?;
        Ok(SpillReader { file: BufReader::new(file), retained: Arc::clone(self), memory })
    }
}

pub(crate) struct SpillWriter { file: BufWriter<File>, position: u64, retained: Arc<SpillFile> }
impl SpillWriter {
    pub fn position(&mut self) -> Result<u64> { Ok(self.position) }
    pub fn write_frame(&mut self, bytes: &[u8]) -> Result<()> {
        self.write_frame_parts(&[bytes])
    }

    fn write_frame_parts(&mut self, parts: &[&[u8]]) -> Result<()> {
        let overflow = || invalid("row-size", "Spill row size overflow");
        let length = parts.iter().try_fold(0u64, |bytes, part| bytes.checked_add(part.len() as u64).ok_or_else(overflow))?;
        self.write_frame_with(None, length, |file| {
            for part in parts { file.write_all(part).map_err(io_error)?; }
            Ok(())
        })
    }

    fn write_frame_with(
        &mut self, marker: Option<u8>, length: u64, write: impl FnOnce(&mut BufWriter<File>) -> Result<()>,
    ) -> Result<()> {
        let count = length.checked_add(8 + if marker.is_some() { 9 } else { 0 })
            .ok_or_else(|| invalid("row-size", "Spill row size overflow"))?;
        self.retained.region.reserve(count)?;
        self.retained.bytes.fetch_add(count, Ordering::Relaxed);
        if let Some(marker) = marker {
            self.file.write_all(&1u64.to_le_bytes()).map_err(io_error)?;
            self.file.write_all(&[marker]).map_err(io_error)?;
        }
        self.file.write_all(&length.to_le_bytes()).map_err(io_error)?;
        write(&mut self.file)?;
        self.position += count;
        Ok(())
    }

    /// Writes a batch whose fixed schema the caller retains through spill and replay.
    pub fn write_record(&mut self, batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<()> {
        let overflow = || invalid("join-spill", "Arrow spill encoding exceeds addressable memory");
        let bytes = batch.schema_ref().fields().iter().try_fold(8192usize, |bytes, field| {
            field.size().checked_mul(8).and_then(|size| bytes.checked_add(size)).ok_or_else(overflow)
        })?;
        let bound = rtdb_columnar::shared_allocation_size(batch)?.checked_mul(6)
            .and_then(|body| body.checked_add(bytes)).ok_or_else(overflow)?;
        let _scratch = memory.reserve(bound)?;
        let (dictionaries, data) = IpcDataGenerator::default().encode(
            batch, &mut DictionaryTracker::new(false), &IpcWriteOptions::default(), &mut CompressionContext::default(),
        ).map_err(ipc_error)?;
        if !dictionaries.is_empty() { return Err(invalid("join-spill", "Spilled records require a dictionary-free schema")); }
        let header = (data.ipc_message.len() as u64).to_le_bytes();
        self.write_frame_parts(&[&header, &data.ipc_message, &data.arrow_data])
    }

    pub fn write_spanned_record(&mut self, record: &RecordSpans) -> Result<()> {
        let (metadata, body) = record.metadata().map_err(ipc_error)?;
        let header = (metadata.len() as u64).to_le_bytes();
        let length = 8u64.checked_add(metadata.len() as u64).and_then(|len| len.checked_add(body as u64))
            .ok_or_else(|| invalid("row-size", "Spill row size overflow"))?;
        self.write_frame_with(None, length, |file| {
            file.write_all(&header).map_err(io_error)?;
            file.write_all(&metadata).map_err(io_error)?;
            record.write_body(file).map_err(io_error)
        })
    }

    fn write_arrow(&mut self, marker: Option<u8>, batch: &OwnedBatch, schema_bytes: usize, memory: &Arc<MemoryBudget>, error: &str) -> Result<()> {
        let batch = batch.batch();
        const MESSAGE_PREFIX_BYTES: usize = 8;
        const END_MARKER: [u8; MESSAGE_PREFIX_BYTES] = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];
        let ipc_error = |cause| match cause {
            ArrowError::IoError(_, cause) => io_error(cause),
            cause => invalid(error, cause),
        };
        let options = IpcWriteOptions::try_new(IPC_ALIGNMENT, false, MetadataVersion::V5).map_err(ipc_error)?;
        let generator = IpcDataGenerator::default();
        let spans = match spill_record_spans(batch, memory) {
            Ok(spans) => spans,
            Err(error) if matches!(error.code.as_str(), "rtdb/memory-budget-exceeded" | "rtdb.format/invalid-arrow") => None,
            Err(error) => return Err(error),
        };
        let _scratch = if spans.is_none() {
            let bound = batch.get_array_memory_size().checked_mul(6)
                .and_then(|bytes| bytes.checked_add(schema_bytes.checked_mul(8)?))
                .and_then(|bytes| bytes.checked_add(8192))
                .ok_or_else(|| invalid(error, "Arrow spill allocation bound overflow"))?;
            Some(memory.reserve(bound)?)
        } else { None };
        let mut tracker = DictionaryTracker::new(false);
        let schema = generator.schema_to_bytes_with_dictionary_tracker(batch.schema_ref(), &mut tracker, &options);
        let (dictionaries, record, span_body) = if let Some(spans) = &spans {
            let (ipc_message, body) = spans.metadata().map_err(|cause| invalid(error, cause))?;
            (Vec::new(), EncodedData { ipc_message, arrow_data: Vec::new() }, body)
        } else {
            let (dictionaries, record) = generator.encode(batch, &mut tracker, &options, &mut CompressionContext::default())
                .map_err(ipc_error)?;
            (dictionaries, record, 0)
        };
        let overflow = || invalid(error, "Arrow spill frame size overflow");
        let length = [&schema].into_iter().chain(&dictionaries).chain([&record])
            .try_fold(END_MARKER.len().checked_add(span_body).ok_or_else(overflow)?, |length, message| {
                let metadata = message.ipc_message.len().checked_add(MESSAGE_PREFIX_BYTES)
                    .and_then(|bytes| bytes.checked_next_multiple_of(IPC_ALIGNMENT)).ok_or_else(overflow)?;
                i32::try_from(metadata - MESSAGE_PREFIX_BYTES).map_err(|_| overflow())?;
                length.checked_add(metadata)
                    .and_then(|bytes| bytes.checked_add(message.arrow_data.len())).ok_or_else(overflow)
            })?;
        self.write_frame_with(marker, u64::try_from(length).map_err(|_| overflow())?, |file| {
            for message in [schema].into_iter().chain(dictionaries).chain([record]) {
                write_message(&mut *file, message, &options).map_err(ipc_error)?;
            }
            if let Some(spans) = &spans { spans.write_body(file).map_err(io_error)?; }
            file.write_all(&END_MARKER).map_err(io_error)
        })
    }

    pub fn write_columns(&mut self, batch: &ColumnBatch, schema: &rtdb_plan::Schema, memory: &Arc<MemoryBudget>) -> Result<()> {
        if batch.len == 0 { return self.write_frame(&[0]); }
        match batch.spill_record(schema, memory) {
            Ok(Some(record)) => {
                let schema_bytes = crate::runtime_schema::schema_bound(schema)?;
                match self.write_arrow(Some(1), &record, schema_bytes, memory, "spill-columns") {
                    Ok(()) => return Ok(()),
                    Err(error) if matches!(error.code.as_str(), "rtdb/memory-budget-exceeded" | "rtdb.query/spill-limit") => (),
                    Err(error) => return Err(error),
                }
            }
            Ok(None) => (),
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => (),
            Err(error) => return Err(error),
        }
        let mut fragments = FragmentBytes::new(memory)?;
        encode_batch(batch, memory, &mut fragments)?;
        let bytes = fragments.as_slice();
        self.write_frame_with(Some(2), bytes.len() as u64, |file| file.write_all(bytes).map_err(io_error))
    }

    /// Drops unpublished frames written after the supplied frame boundary.
    pub fn discard_from(&mut self, offset: u64) -> Result<()> {
        let bytes = self.position.checked_sub(offset).ok_or_else(|| invalid("invalid-spill", "Spill rollback exceeds its file"))?;
        self.file.flush().map_err(io_error)?;
        self.file.get_ref().set_len(offset).map_err(io_error)?;
        self.file.seek(SeekFrom::Start(offset)).map_err(io_error)?;
        self.position = offset;
        self.retained.bytes.fetch_sub(bytes, Ordering::Relaxed);
        self.retained.region.retained.fetch_sub(bytes, Ordering::AcqRel);
        Ok(())
    }

    pub fn finish(mut self) -> Result<Arc<SpillFile>> {
        self.file.flush().map_err(io_error)?;
        Ok(self.retained)
    }
}

fn spill_record_spans(batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<Option<RecordSpans>> {
    let layout = memory.reserve(rtdb_format::ipc::schema_translation_bound(batch.schema_ref())?)?;
    if !batch.columns().iter().all(|array| exact_span_layout(&array.to_data(), 0, array.len())) {
        return Ok(None);
    }
    drop(layout);
    RecordSpans::concat(&[batch], IPC_ALIGNMENT, memory)
}

fn exact_span_layout(data: &ArrayData, start: usize, len: usize) -> bool {
    if let Some(nulls) = data.nulls().filter(|nulls| nulls.null_count() != 0) {
        let bits = nulls.inner();
        let offset = bits.offset() + start;
        if !span_bitmap_matches(bits.inner(), offset, len) && bits.inner().count_set_bits_offset(offset, len) != len {
            return false;
        }
    }
    let offset = data.offset() + start;
    match data.data_type() {
        DataType::Boolean => span_bitmap_matches(&data.buffers()[0], offset, len),
        DataType::Struct(_) => data.child_data().iter().all(|child| exact_span_layout(child, start, len)),
        DataType::List(_) => exact_span_list::<i32>(data, start, len),
        DataType::LargeList(_) => exact_span_list::<i64>(data, start, len),
        DataType::FixedSizeList(_, width) => {
            let width = *width as usize;
            exact_span_layout(&data.child_data()[0], offset * width, len * width)
        }
        DataType::Null | DataType::FixedSizeBinary(_) | DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => true,
        ty => ty.primitive_width().is_some(),
    }
}

fn exact_span_list<O: OffsetSizeTrait>(data: &ArrayData, start: usize, len: usize) -> bool {
    let offsets = data.buffer::<O>(0);
    let child_start = offsets[start].as_usize();
    let child_end = offsets[start + len].as_usize();
    exact_span_layout(&data.child_data()[0], child_start, child_end - child_start)
}

fn span_bitmap_matches(buffer: &Buffer, offset: usize, len: usize) -> bool {
    // Arrow 58.4.1 preserves the unused bits in a byte-aligned slice of a bitmap.
    let tail = len % 8;
    tail == 0 || offset % 8 != 0 || buffer[offset / 8 + len / 8] >> tail == 0
}

pub(crate) struct SpillReader { file: BufReader<File>, retained: Arc<SpillFile>, memory: Arc<MemoryBudget> }
impl SpillReader {
    pub fn seek(&mut self, offset: u64) -> Result<()> {
        if offset > self.retained.bytes.load(Ordering::Relaxed) { return Err(invalid("invalid-spill","Spill offset exceeds its file")); }
        self.file.seek(SeekFrom::Start(offset)).map_err(io_error)?;
        Ok(())
    }
    pub fn rewind(&mut self) -> Result<()> { self.file.seek(SeekFrom::Start(0)).map_err(io_error)?; Ok(()) }
    fn read_frame_length(&mut self) -> std::io::Result<Option<u64>> {
        let mut prefix = [0u8; 8];
        if self.file.read(&mut prefix[..1])? == 0 { return Ok(None); }
        self.file.read_exact(&mut prefix[1..])?;
        Ok(Some(u64::from_le_bytes(prefix)))
    }

    pub fn read_frame(&mut self, expansion: usize) -> Result<Option<(Vec<u8>, Arc<MemoryReservation>)>> {
        let Some(length) = self.read_frame_length().map_err(io_error)? else { return Ok(None); };
        let length = usize::try_from(length).map_err(io_error)?;
        if length as u64 > self.retained.bytes.load(Ordering::Relaxed) { return Err(invalid("invalid-spill", "Spill row exceeds its file")); }
        let bound = length.checked_mul(expansion).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("row-size", "Decoded spill size overflow"))?;
        let reservation = self.memory.reserve(bound)?;
        let mut bytes = vec![0; length];
        self.file.read_exact(&mut bytes).map_err(io_error)?;
        Ok(Some((bytes, reservation)))
    }

    pub fn read_arrow(&mut self, allow_empty: bool, error: &str) -> Result<Option<OwnedBatch>> {
        let Some((bytes, mut reservation)) = self.read_frame(6)? else { return Ok(None); };
        let ipc_error = |cause: arrow_schema::ArrowError| invalid(error, cause);
        let mut bytes = Buffer::from_vec(bytes);
        let mut ipc = StreamDecoder::new();
        let batch = ipc.decode(&mut bytes).map_err(ipc_error)?
            .ok_or_else(|| invalid("invalid-spill", "Arrow spill frame contains no batch"))?;
        if (!allow_empty && batch.num_rows() == 0) || ipc.decode(&mut bytes).map_err(ipc_error)?.is_some() || !bytes.is_empty() {
            return Err(invalid("invalid-spill", "Arrow spill frame must contain exactly one admissible batch"));
        }
        ipc.finish().map_err(ipc_error)?;
        drop(ipc);
        let overflow = || invalid(error, "Arrow spill size overflow");
        let schema = batch.schema_ref().fields().iter().try_fold(256usize, |bytes, field| {
            field.size().checked_mul(4).and_then(|size| bytes.checked_add(size))
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<crate::columnar::Column>())).ok_or_else(overflow)
        })?;
        let bytes = rtdb_columnar::shared_allocation_size(&batch)?.checked_add(schema).ok_or_else(overflow)?;
        let lease = Arc::get_mut(&mut reservation).expect("The spill decoder owns its reservation");
        if bytes > lease.bytes() { lease.grow_to(bytes)?; } else { lease.shrink_to(bytes)?; }
        OwnedBatch::with_reservation(batch, reservation).map(Some)
    }

    pub fn read_columns(&mut self, width: usize) -> Result<Option<ColumnBatch>> {
        let Some(length) = self.read_frame_length()
            .map_err(|error| io_error(format_args!("Reading spill page marker length: {error}")))? else { return Ok(None); };
        if length != 1 { return Err(invalid("invalid-spill", "Spill page marker must be one byte")); }
        let mut marker = [0];
        self.file.read_exact(&mut marker)
            .map_err(|error| io_error(format_args!("Reading spill page marker: {error}")))?;
        self.read_columns_frame(&marker, width)
    }

    pub fn read_columns_frame(&mut self, marker: &[u8], width: usize) -> Result<Option<ColumnBatch>> {
        match marker {
            [0] => ColumnBatch::empty_with_width(width, &self.memory).map(Some),
            [1] => {
                let batch = self.read_arrow(false, "spill-columns")?
                    .ok_or_else(|| invalid("invalid-spill", "Spilled Arrow page is absent"))?;
                ColumnBatch::from_owned(batch).map(Some)
            }
            [2] => {
                let (bytes, charge) = self.read_frame(1)?
                    .ok_or_else(|| invalid("invalid-spill", "Spilled Arrow fragments are absent"))?;
                let bytes = rtdb_columnar::retain_buffer(&Buffer::from_vec(bytes), &charge);
                drop(charge);
                let mut batches = decode_batches(bytes, &self.memory)?;
                if batches.len() == 1 { return Ok(batches.pop()); }
                if batches.is_empty() { return Err(invalid("invalid-spill", "Spilled page has no Arrow fragments")); }
                let rows = batches.iter().try_fold(0usize, |count, batch| {
                    count.checked_add(batch.len).ok_or_else(|| invalid("invalid-spill", "Spilled page row count overflow"))
                })?;
                let bound = rows.checked_mul(std::mem::size_of::<(usize, usize)>())
                    .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("invalid-spill", "Spilled page selection overflow"))?;
                let _picks = self.memory.reserve(bound)?;
                let picks = batches.iter().enumerate().flat_map(|(index, batch)| (0..batch.len).map(move |row| (index, row))).collect::<Vec<_>>();
                let columns = (0..width).collect::<Vec<_>>();
                ColumnBatch::take_rows(&batches, &picks, &columns, &self.memory).map(Some)
            }
            _ => Err(invalid("invalid-spill", "Invalid spill page header")),
        }
    }

    pub fn read_record(&mut self, schema: &SchemaRef, retained_metadata: usize) -> Result<Option<OwnedBatch>> {
        let Some((frame, mut reservation)) = self.read_frame(8)? else { return Ok(None); };
        let bytes = Buffer::from_vec(frame);
        let type_error = || invalid("join-spill", "Malformed Arrow record frame");
        let size = bytes.get(..8).and_then(|prefix| prefix.try_into().ok())
            .map(u64::from_le_bytes).and_then(|size| usize::try_from(size).ok()).ok_or_else(type_error)?;
        let offset = size.checked_add(8).filter(|size| *size <= bytes.len()).ok_or_else(type_error)?;
        let message = arrow_ipc::root_as_message(&bytes[8..offset]).map_err(ipc_error)?;
        let metadata = message.header_as_record_batch().ok_or_else(type_error)?;
        let body = bytes.slice(offset);
        if message.bodyLength() as u64 != body.len() as u64 { return Err(type_error()); }
        if metadata.compression().is_some() {
            return Err(type_error());
        }
        let buffers = metadata.buffers().ok_or_else(type_error)?;
        for buffer in buffers {
            let start = usize::try_from(buffer.offset()).map_err(|_| type_error())?;
            let length = usize::try_from(buffer.length()).map_err(|_| type_error())?;
            if start.checked_add(length).is_none_or(|end| end > body.len()) { return Err(type_error()); }
        }
        let batch = read_record_batch(&body, metadata, Arc::clone(schema), &Default::default(), None, &message.version()).map_err(ipc_error)?;
        let retained = rtdb_columnar::shared_allocation_size(&batch)?.checked_add(512)
            .and_then(|bytes| bytes.checked_add(retained_metadata))
            .ok_or_else(|| invalid("join-spill", "Decoded spill size exceeds addressable memory"))?;
        let lease = Arc::get_mut(&mut reservation).expect("The spill decoder owns its reservation");
        if retained > lease.bytes() { lease.grow_to(retained)?; } else { lease.shrink_to(retained)?; }
        OwnedBatch::with_reservation(batch, reservation).map(Some)
    }
}

fn ipc_error(error: impl std::fmt::Display) -> rtdb_types::Anomaly { invalid("join-spill", error) }

fn io_error(error: impl std::fmt::Display) -> rtdb_types::Anomaly { rtdb_types::Anomaly::fault("rtdb.query/spill-io", error.to_string()) }

#[test]
fn empty_spill_page_needs_only_its_output_reservation() {
    let memory = MemoryBudget::new(256);
    let spill = Arc::new(SpillRegion::new(9).unwrap());
    let batch = ColumnBatch::empty_with_width(0, &memory).unwrap();
    let mut writer = spill.create().unwrap();
    writer.write_columns(&batch, &rtdb_plan::Schema::default(), &memory).unwrap();
    drop(batch);

    let mut reader = writer.finish().unwrap().reader(Arc::clone(&memory)).unwrap();
    let page = reader.read_columns(0).unwrap().unwrap();
    assert_eq!(page.len, 0);
    assert!(page.columns.is_empty());
    assert!(reader.read_columns(0).unwrap().is_none());
    drop(page);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn malformed_column_spill_markers_fail_before_body_admission() {
    let memory = MemoryBudget::new(0);
    let spill = Arc::new(SpillRegion::new(64).unwrap());
    let missing_marker = 1u64.to_le_bytes();
    let zero_width = 0u64.to_le_bytes();
    let wide_marker = u64::MAX.to_le_bytes();
    let unknown_marker = [1, 0, 0, 0, 0, 0, 0, 0, 3];
    for (bytes, code, message) in [
        (&missing_marker[..1], "rtdb.query/spill-io", "Reading spill page marker length"),
        (&missing_marker[..], "rtdb.query/spill-io", "Reading spill page marker:"),
        (&zero_width[..], "rtdb.query/invalid-spill", "Spill page marker must be one byte"),
        (&wide_marker[..], "rtdb.query/invalid-spill", "Spill page marker must be one byte"),
        (&unknown_marker[..], "rtdb.query/invalid-spill", "Invalid spill page header"),
    ] {
        let mut writer = spill.create().unwrap();
        writer.file.write_all(bytes).unwrap();
        let mut reader = writer.finish().unwrap().reader(Arc::clone(&memory)).unwrap();
        let error = reader.read_columns(0).err().unwrap();
        assert_eq!(error.code, code);
        assert!(error.message.as_deref().unwrap().starts_with(message));
    }
}

#[test]
fn arrow_records_share_one_schema_across_spill_offsets() {
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let memory = MemoryBudget::new(1 << 20);
    let spill = Arc::new(SpillRegion::new(1536).unwrap());
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let batch = |key, value| RecordBatch::try_new(Arc::clone(&schema), vec![
        Arc::new(Int64Array::from(vec![key])), Arc::new(StringArray::from(vec![value])),
    ]).unwrap();
    let first = batch(1, Some("first"));
    let second = batch(2, None);
    let mut writer = spill.create().unwrap();
    writer.write_record(&first, &memory).unwrap();
    let offset = writer.position().unwrap();
    writer.write_record(&second, &memory).unwrap();
    let mut reader = writer.finish().unwrap().reader(Arc::clone(&memory)).unwrap();
    reader.seek(offset).unwrap();
    assert_eq!(reader.read_record(&schema, 0).unwrap().unwrap().batch(), &second);
    assert!(reader.read_record(&schema, 0).unwrap().is_none());
    reader.rewind().unwrap();
    assert_eq!(reader.read_record(&schema, 0).unwrap().unwrap().batch(), &first);
    drop(reader);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(spill.retained_bytes(), 0);
}


#[test]
fn homogeneous_spill_writes_exact_ipc_framing_and_checks_quota_before_writing() {
    use arrow_array::{Array, builder::StringDictionaryBuilder, types::Int32Type};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{Field, Schema};

    let memory = MemoryBudget::new(1 << 20);
    let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
    dictionary.append_value("left");
    dictionary.append_value("right");
    dictionary.append_value("left");
    let dictionary = dictionary.finish();
    let schema = Arc::new(Schema::new(vec![Field::new("key", dictionary.data_type().clone(), false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(dictionary)]).unwrap();
    let batch = OwnedBatch::new(batch, &memory).unwrap();
    let mut reference = StreamWriter::try_new(Vec::new(), batch.batch().schema_ref()).unwrap();
    reference.write(batch.batch()).unwrap();
    let expected = reference.into_inner().unwrap();
    let size = expected.len() as u64 + 8;

    let spill = Arc::new(SpillRegion::new(size).unwrap());
    let mut writer = spill.create().unwrap();
    writer.write_arrow(None, &batch, 0, &memory, "spill-columns").unwrap();
    assert_eq!(writer.write_arrow(None, &batch, 0, &memory, "spill-columns").unwrap_err().code, "rtdb.query/spill-limit");
    assert_eq!(writer.position().unwrap(), size);
    assert_eq!(spill.retained_bytes(), size);
    let mut reader = writer.finish().unwrap().reader(Arc::clone(&memory)).unwrap();
    assert_eq!(reader.read_frame(1).unwrap().unwrap().0, expected);
    assert!(reader.read_frame(1).unwrap().is_none());
    drop((reader, batch));
    assert_eq!(spill.retained_bytes(), 0);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn homogeneous_spill_streams_exact_ipc_with_only_span_scratch() {
    use arrow_array::{ArrayRef, BooleanArray, Int64Array, ListArray, types::Int32Type};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{Field, Schema};

    let memory = MemoryBudget::new(1 << 24);
    let scratch = MemoryBudget::new(1 << 18);
    let len = (1 << 15) + 1;
    let numeric = Arc::new(Int64Array::from(vec![17; len])) as ArrayRef;
    let flags: BooleanArray = (0..len + 1).map(|row| match row % 3 {
        0 => Some(false), 1 => Some(true), _ => None,
    }).collect();
    let flags = Arc::new(flags.slice(1, len)) as ArrayRef;
    let list = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        (0..len).map(|row| (row % 2 == 0).then_some([Some(2), None])),
    )) as ArrayRef;
    let schema = Arc::new(Schema::new_with_metadata(vec![
        Field::new("number", numeric.data_type().clone(), false).with_metadata([("unit".into(), "rows".into())].into()),
        Field::new("flag", flags.data_type().clone(), true),
        Field::new("list", list.data_type().clone(), true),
    ], [("page".into(), "spill".into())].into()));
    let batch = OwnedBatch::new(RecordBatch::try_new(schema, vec![numeric, flags, list]).unwrap(), &memory).unwrap();
    let mut reference = StreamWriter::try_new(Vec::new(), batch.batch().schema_ref()).unwrap();
    reference.write(batch.batch()).unwrap();
    let expected = reference.into_inner().unwrap();
    let size = expected.len() as u64 + 17;

    let spill = Arc::new(SpillRegion::new(size + 9).unwrap());
    let mut writer = spill.create().unwrap();
    writer.write_arrow(Some(1), &batch, 0, &scratch, "spill-columns").unwrap();
    let error = writer.write_arrow(Some(1), &batch, 0, &scratch, "spill-columns").unwrap_err();
    assert_eq!(error.code, "rtdb.query/spill-limit");
    assert_eq!(writer.position().unwrap(), size);
    assert_eq!(spill.retained_bytes(), size);
    assert_eq!(spill.peak_bytes(), size);
    assert_eq!(scratch.used_bytes(), 0);
    let mut reader = writer.finish().unwrap().reader(Arc::clone(&memory)).unwrap();
    assert_eq!(reader.read_frame(1).unwrap().unwrap().0, [1]);
    assert_eq!(reader.read_frame(1).unwrap().unwrap().0, expected);
    assert!(reader.read_frame(1).unwrap().is_none());
}

#[test]
fn homogeneous_spill_preserves_unused_bits_in_aligned_bitmap_slices() {
    use arrow_array::{ArrayRef, BooleanArray, Int32Array};
    use arrow_ipc::writer::StreamWriter;

    let memory = MemoryBudget::new(1 << 20);
    let spill = Arc::new(SpillRegion::new(1 << 16).unwrap());
    let arrays = [
        Arc::new(BooleanArray::from(vec![true; 8]).slice(0, 3)) as ArrayRef,
        Arc::new(Int32Array::from(vec![None, Some(1), None, Some(2), Some(3)]).slice(0, 3)) as ArrayRef,
    ];
    let mut writer = spill.create().unwrap();
    let expected = arrays.into_iter().map(|array| {
        let record = RecordBatch::try_from_iter([("value", array)]).unwrap();
        assert!(spill_record_spans(&record, &memory).unwrap().is_none());
        let batch = OwnedBatch::new(record, &memory).unwrap();
        let mut reference = StreamWriter::try_new(Vec::new(), batch.batch().schema_ref()).unwrap();
        reference.write(batch.batch()).unwrap();
        writer.write_arrow(None, &batch, 0, &memory, "spill-columns").unwrap();
        reference.into_inner().unwrap()
    }).collect::<Vec<_>>();
    let mut reader = writer.finish().unwrap().reader(Arc::clone(&memory)).unwrap();
    for frame in expected { assert_eq!(reader.read_frame(1).unwrap().unwrap().0, frame); }
    assert!(reader.read_frame(1).unwrap().is_none());
}

#[test]
fn spill_replays_mixed_struct_fields_as_arrow_positions() {
    use crate::columnar::{Column, ColumnBatch, pack_evaluated_column};
    use rtdb_plan::{Binder, Schema};
    use rtdb_types::{StructFields, Value};

    let memory = MemoryBudget::new(1 << 23);
    let spill = Arc::new(SpillRegion::new(1 << 20).unwrap());
    let values = [
        Value::Struct(StructFields::from([("a".into(), Value::I64(3))])),
        Value::Struct(StructFields::from([("b".into(), Value::Utf8("text".into()))])),
        Value::Struct(StructFields::from([("a".into(), Value::I64(5))])),
    ];
    let ty = values.iter().map(Value::logical_type).reduce(|left, right| left.join(&right)).unwrap();
    let schema = Schema::new(vec![Binder::default().field("doc", None, ty).unwrap()]).unwrap();
    let (column, charge) = pack_evaluated_column(&values, &memory).unwrap();
    let mut batch = ColumnBatch::settled(vec![], values.len(), memory.reserve(256).unwrap()).unwrap();
    batch.append_column(column, charge, &memory).unwrap();

    let mut writer = spill.create().unwrap();
    writer.write_columns(&batch, &schema, &memory).unwrap();
    let file = writer.finish().unwrap();
    let wire_bytes = usize::try_from(spill.retained_bytes()).unwrap();
    let tight_memory = MemoryBudget::new(wire_bytes + (14 << 10));
    let mut tight_reader = file.reader(Arc::clone(&tight_memory)).unwrap();
    let detached = tight_reader.read_columns(1).unwrap().unwrap();
    for (row, expected) in values.iter().enumerate() {
        assert_eq!(&detached.columns[0].value(row).unwrap(), expected);
    }
    assert!(tight_reader.read_columns(1).unwrap().is_none());
    drop((tight_reader, detached));
    assert_eq!(tight_memory.used_bytes(), 0);

    let replay_memory = MemoryBudget::new(1 << 23);
    let mut reader = file.reader(Arc::clone(&replay_memory)).unwrap();
    drop(file);
    let replayed = reader.read_columns(1).unwrap().unwrap();
    let Column::Selected(selected) = &replayed.columns[0] else { panic!("Expected mixed Arrow fields") };
    let retained = {
        let (source, _, _) = selected.source_layouts().next().unwrap();
        let source = source.as_any().downcast_ref::<arrow_array::StructArray>().unwrap();
        Arc::clone(source.column(0))
    };
    for (row, expected) in values.into_iter().enumerate() {
        assert_eq!(replayed.columns[0].value(row).unwrap(), expected);
    }
    assert!(reader.read_columns(1).unwrap().is_none());
    drop((reader, replayed));
    assert_eq!(spill.retained_bytes(), 0);
    assert!(replay_memory.used_bytes() > 0);
    drop(retained);
    assert_eq!(replay_memory.used_bytes(), 0);
}
