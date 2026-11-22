use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::{reader::{StreamReader, read_record_batch}, writer::{CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions, StreamWriter}};
use arrow_schema::SchemaRef;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_types::{Result, Value};
use tempfile::{NamedTempFile, TempDir, TempPath};

use crate::{cursor::OwnedRow, invalid};

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
    pub fn reader(&mut self, memory: Arc<MemoryBudget>) -> Result<SpillReader> {
        self.file.flush().map_err(io_error)?;
        self.retained.reader(memory)
    }
    pub fn write(&mut self, values: &[Value], memory: &Arc<MemoryBudget>) -> Result<()> {
        let retained = values.iter().try_fold(1024usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(|| invalid("row-size", "Spill row size overflow")))?;
        let _temporary = memory.reserve(retained.checked_mul(8).ok_or_else(|| invalid("row-size", "Encoded spill row size overflow"))?)?;
        let bytes = serde_json::to_vec(values).map_err(io_error)?;
        self.write_frame(&bytes)
    }

    pub fn write_frame(&mut self, bytes: &[u8]) -> Result<()> {
        self.write_frame_parts(&[bytes])
    }

    fn write_frame_parts(&mut self, parts: &[&[u8]]) -> Result<()> {
        let overflow = || invalid("row-size", "Spill row size overflow");
        let length = parts.iter().try_fold(0u64, |bytes, part| bytes.checked_add(part.len() as u64).ok_or_else(overflow))?;
        let count = length.checked_add(8).ok_or_else(overflow)?;
        self.retained.region.reserve(count)?;
        self.retained.bytes.fetch_add(count, Ordering::Relaxed);
        self.file.write_all(&length.to_le_bytes()).map_err(io_error)?;
        for part in parts { self.file.write_all(part).map_err(io_error)?; }
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

    pub fn write_arrow(&mut self, batch: &OwnedBatch, schema_bytes: usize, memory: &Arc<MemoryBudget>, error: &str) -> Result<()> {
        let batch = batch.batch();
        let bound = batch.get_array_memory_size().checked_mul(6)
            .and_then(|bytes| bytes.checked_add(schema_bytes.checked_mul(8)?))
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(|| invalid(error, "Arrow spill allocation bound overflow"))?;
        let _scratch = memory.reserve(bound)?;
        let ipc_error = |cause: arrow_schema::ArrowError| invalid(error, cause);
        let mut ipc = StreamWriter::try_new(Vec::new(), batch.schema().as_ref()).map_err(ipc_error)?;
        ipc.write(batch).map_err(ipc_error)?;
        self.write_frame(&ipc.into_inner().map_err(ipc_error)?)
    }

    pub fn finish(mut self) -> Result<Arc<SpillFile>> {
        self.file.flush().map_err(io_error)?;
        Ok(self.retained)
    }
}

pub(crate) struct SpillReader { file: BufReader<File>, retained: Arc<SpillFile>, memory: Arc<MemoryBudget> }
impl SpillReader {
    pub fn seek(&mut self, offset: u64) -> Result<()> {
        if offset > self.retained.bytes.load(Ordering::Relaxed) { return Err(invalid("invalid-spill","Spill offset exceeds its file")); }
        self.file.seek(SeekFrom::Start(offset)).map_err(io_error)?;
        Ok(())
    }
    pub fn rewind(&mut self) -> Result<()> { self.file.seek(SeekFrom::Start(0)).map_err(io_error)?; Ok(()) }
    pub fn next(&mut self) -> Result<Option<OwnedRow>> {
        let Some((bytes, reservation)) = self.read_frame(64)? else { return Ok(None); };
        let values = serde_json::from_slice::<Vec<Value>>(&bytes).map_err(io_error)?;
        drop(bytes);
        let row = OwnedRow::new(values, &self.memory)?;
        drop(reservation);
        Ok(Some(row))
    }

    pub fn read_frame(&mut self, expansion: usize) -> Result<Option<(Vec<u8>, Arc<MemoryReservation>)>> {
        let mut prefix = [0u8; 8];
        match self.file.read(&mut prefix[..1]).map_err(io_error)? { 0 => return Ok(None), _ => {} }
        self.file.read_exact(&mut prefix[1..]).map_err(io_error)?;
        let length = usize::try_from(u64::from_le_bytes(prefix)).map_err(io_error)?;
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
        let mut ipc = StreamReader::try_new(Cursor::new(bytes), None).map_err(ipc_error)?;
        let batch = ipc.next().ok_or_else(|| invalid("invalid-spill", "Arrow spill frame contains no batch"))?.map_err(ipc_error)?;
        if (!allow_empty && batch.num_rows() == 0) || ipc.next().transpose().map_err(ipc_error)?.is_some() {
            return Err(invalid("invalid-spill", "Arrow spill frame must contain exactly one admissible batch"));
        }
        drop(ipc);
        let bytes = batch.get_array_memory_size().checked_add(256)
            .ok_or_else(|| invalid(error, "Arrow spill size overflow"))?;
        let lease = Arc::get_mut(&mut reservation).expect("The spill decoder owns its reservation");
        if bytes > lease.bytes() { lease.grow_to(bytes)?; } else { lease.shrink_to(bytes)?; }
        OwnedBatch::with_reservation(batch, reservation).map(Some)
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
