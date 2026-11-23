use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
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
        let metadata = values.len().saturating_mul(1024).saturating_add(4096);
        let mut builder = rtdb_columnar::BatchBuilder::new(metadata, memory)?;
        let mut columns = Vec::with_capacity(values.len());
        let mut fields = Vec::with_capacity(values.len());
        for (index, value) in values.iter().enumerate() {
            let name = index.to_string();
            let logical = value.logical_type();
            let input = std::slice::from_ref(value);
            let physical = if row_object(value) { None } else {
                rtdb_columnar::to_field(&name, &logical).ok()
            };
            if let Some(field) = physical {
                let estimate = rtdb_columnar::estimate_builder_bytes(&logical, input)?;
                columns.push(builder.column(estimate, || rtdb_columnar::copy_array(&logical, input))?);
                fields.push(field);
            } else {
                let bound = value.retained_bytes()?.saturating_mul(8).saturating_add(4096);
                columns.push(builder.column(bound, || {
                    let bytes = serde_json::to_vec(value).map_err(io_error)?;
                    Ok(Arc::new(arrow_array::BinaryArray::from_vec(vec![bytes.as_slice()])) as ArrayRef)
                })?);
                let metadata = std::collections::HashMap::from([(ROW_OBJECT.into(), String::new())]);
                fields.push(Field::new(name, DataType::Binary, false).with_metadata(metadata));
            }
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new_with_options(schema, columns, &RecordBatchOptions::new().with_row_count(Some(1))).map_err(io_error)?;
        self.write_arrow(&builder.finish(batch)?, metadata, memory, "sort-spill-row")
    }

    pub fn write_frame(&mut self, bytes: &[u8]) -> Result<()> {
        let count = (bytes.len() as u64).checked_add(8).ok_or_else(|| invalid("row-size", "Spill row size overflow"))?;
        self.retained.region.reserve(count)?;
        self.retained.bytes.fetch_add(count, Ordering::Relaxed);
        self.file.write_all(&(bytes.len() as u64).to_le_bytes()).map_err(io_error)?;
        self.file.write_all(bytes).map_err(io_error)?;
        self.position += count;
        Ok(())
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
        let Some(batch) = self.read_arrow(false, "sort-spill-row")? else { return Ok(None); };
        let batch = batch.batch();
        if batch.num_rows() != 1 { return Err(invalid("invalid-spill", "Row spill frame has the wrong row count")); }
        let bound = batch.schema_ref().fields().iter().zip(batch.columns()).try_fold(4096usize, |bytes, (field, column)| {
            let size = if field.metadata().contains_key(ROW_OBJECT) { column.get_array_memory_size().saturating_mul(64) }
                else if field.data_type() == &DataType::Null { 64 }
                else { rtdb_columnar::estimate_read_value_bytes(field, column.as_ref(), 0)? };
            bytes.checked_add(size).ok_or_else(|| invalid("row-size", "Spill row size overflow"))
        })?;
        let _scratch = self.memory.reserve(bound)?;
        let values = batch.schema_ref().fields().iter().zip(batch.columns()).map(|(field, column)| {
            if field.metadata().contains_key(ROW_OBJECT) {
                let binary = column.as_any().downcast_ref::<arrow_array::BinaryArray>()
                    .ok_or_else(|| invalid("invalid-spill", "Spill object has the wrong layout"))?;
                serde_json::from_slice(binary.value(0)).map_err(io_error)
            } else if field.data_type() == &DataType::Null && !field.is_nullable() {
                Ok(Value::Undefined)
            } else {
                rtdb_columnar::read_value(field, column.as_ref(), 0)
            }
        }).collect::<Result<_>>()?;
        OwnedRow::new(values, &self.memory).map(Some)
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
}

const ROW_OBJECT: &str = "rtdb.sort.row-object";

fn row_object(value: &Value) -> bool {
    match value {
        Value::Date(_) => true,
        Value::List(values) | Value::Set(values) | Value::FixedSizeList(values) => {
            values.iter().any(|value| matches!(value, Value::Undefined) || row_object(value))
        }
        Value::Struct(fields) => fields.values().any(row_object),
        Value::Map { entries, .. } => entries.iter().any(|(key, value)| {
            matches!(key, Value::Undefined) || matches!(value, Value::Undefined) || row_object(key) || row_object(value)
        }),
        _ => false,
    }
}

fn io_error(error: impl std::fmt::Display) -> rtdb_types::Anomaly { rtdb_types::Anomaly::fault("rtdb.query/spill-io", error.to_string()) }
