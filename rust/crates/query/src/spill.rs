use std::fs::File;
use std::borrow::Cow;
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use arrow_array::{Array, ArrayRef, BinaryArray, FixedSizeListArray, Float32Array, Float64Array, RecordBatch, UnionArray};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::MemoryBudget;
use rtdb_types::{ListValue, ListView, Result, StructFields, Value};
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempDir, TempPath};

use crate::{cursor::OwnedRow, invalid};

const ARROW_ROW: u64 = 1 << 63;

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
        let (encoding, bytes) = if values.iter().any(has_vector) {
            (ARROW_ROW, encode_arrow_row(values)?)
        } else { (0, serde_json::to_vec(values).map_err(io_error)?) };
        let length = u64::try_from(bytes.len()).map_err(io_error)?;
        if length >= ARROW_ROW { return Err(invalid("row-size", "Spill row size overflow")); }
        let count = length + 8;
        self.retained.region.reserve(count)?;
        self.retained.bytes.fetch_add(count, Ordering::Relaxed);
        self.file.write_all(&(length | encoding).to_le_bytes()).map_err(io_error)?;
        self.file.write_all(&bytes).map_err(io_error)?;
        self.position += count;
        Ok(())
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
        let mut prefix = [0u8; 8];
        match self.file.read(&mut prefix[..1]).map_err(io_error)? { 0 => return Ok(None), _ => {} }
        self.file.read_exact(&mut prefix[1..]).map_err(io_error)?;
        let header = u64::from_le_bytes(prefix);
        let length = usize::try_from(header & !ARROW_ROW).map_err(io_error)?;
        if length as u64 > self.retained.bytes.load(Ordering::Relaxed) { return Err(invalid("invalid-spill", "Spill row exceeds its file")); }
        let bound = length.checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("row-size", "Decoded spill size overflow"))?;
        let reservation = self.memory.reserve(bound)?;
        let mut bytes = vec![0; length];
        self.file.read_exact(&mut bytes).map_err(io_error)?;
        let values = if header & ARROW_ROW != 0 { decode_arrow_row(&bytes)? }
            else { serde_json::from_slice::<Vec<Value>>(&bytes).map_err(io_error)? };
        drop(bytes);
        let row = OwnedRow::new(values, &self.memory)?;
        drop(reservation);
        Ok(Some(row))
    }
}

fn has_vector(value: &Value) -> bool {
    match value {
        Value::List(list) | Value::FixedSizeList(list) => match list.view() {
            ListView::Values(values) => values.iter().any(has_vector),
            typed => !typed.is_empty(),
        },
        Value::Set(values) => values.iter().any(has_vector),
        Value::Struct(fields) => fields.values().any(has_vector),
        Value::Map { entries, .. } => entries.iter().any(|(key, value)| has_vector(key) || has_vector(value)),
        _ => false,
    }
}

#[derive(Serialize, Deserialize)]
enum SpillCell<'a> {
    Scalar(Cow<'a, Value>),
    Vector { index: usize, fixed: bool },
    List(Vec<SpillCell<'a>>),
    Fixed(Vec<SpillCell<'a>>),
    Set(Vec<SpillCell<'a>>),
    Struct(StructFields<SpillCell<'a>>),
    Map { entries: Vec<(SpillCell<'a>, SpillCell<'a>)>, sorted: bool },
}

impl<'a> SpillCell<'a> {
    fn extract(value: &'a Value, arrays: &mut Vec<ArrayRef>) -> Result<Self> {
        fn sequence<'a>(values: &'a [Value], arrays: &mut Vec<ArrayRef>) -> Result<Vec<SpillCell<'a>>> {
            values.iter().map(|value| SpillCell::extract(value, arrays)).collect()
        }
        Ok(match value {
            Value::List(list) | Value::FixedSizeList(list) => {
                let fixed = matches!(value, Value::FixedSizeList(_));
                let list_cell = |values| if fixed { Self::Fixed(values) } else { Self::List(values) };
                let components: ArrayRef = match list.view() {
                    ListView::Values(values) => return Ok(list_cell(sequence(values, arrays)?)),
                    view if view.is_empty() => return Ok(list_cell(Vec::new())),
                    ListView::F32(array) => Arc::new(array.clone()),
                    ListView::F64(array) => Arc::new(array.clone()),
                    ListView::Union(array) => Arc::new(array.clone()),
                };
                let index = arrays.len();
                let width = i32::try_from(components.len()).map_err(io_error)?;
                let item = Arc::new(Field::new("item", components.data_type().clone(), components.null_count() != 0));
                arrays.push(Arc::new(FixedSizeListArray::try_new(item, width, components, None).map_err(io_error)?));
                Self::Vector { index, fixed }
            }
            Value::Set(values) => Self::Set(sequence(values, arrays)?),
            Value::Struct(fields) => Self::Struct(fields.iter().map(|(name, value)| {
                Ok((name.clone(), Self::extract(value, arrays)?))
            }).collect::<Result<_>>()?),
            Value::Map { entries, sorted } => Self::Map {
                entries: entries.iter().map(|(key, value)| Ok((Self::extract(key, arrays)?, Self::extract(value, arrays)?))).collect::<Result<_>>()?,
                sorted: *sorted,
            },
            _ => Self::Scalar(Cow::Borrowed(value)),
        })
    }

    fn restore(self, vectors: &[ArrayRef]) -> Result<Value> {
        Ok(match self {
            Self::Scalar(value) => value.into_owned(),
            Self::Vector { index, fixed } => {
                let list = vectors.get(index).and_then(|array| array.as_any().downcast_ref::<FixedSizeListArray>())
                    .filter(|array| array.len() == 1 && array.is_valid(0))
                    .ok_or_else(|| invalid("invalid-spill", "Invalid spilled embedding column"))?;
                let values = list.values();
                let vector = match values.data_type() {
                    DataType::Float32 => values.as_any().downcast_ref::<Float32Array>().cloned().map(ListValue::from_f32),
                    DataType::Float64 => values.as_any().downcast_ref::<Float64Array>().cloned().map(ListValue::from_f64),
                    DataType::Union(..) => values.as_any().downcast_ref::<UnionArray>().cloned().and_then(ListValue::from_float_union),
                    _ => None,
                }.ok_or_else(|| invalid("invalid-spill", "Invalid spilled embedding type"))?;
                if fixed { Value::FixedSizeList(vector) } else { Value::List(vector) }
            }
            Self::List(values) => Value::list(Self::restore_list(values, vectors)?),
            Self::Fixed(values) => Value::FixedSizeList(Self::restore_list(values, vectors)?.into()),
            Self::Set(values) => Value::Set(Self::restore_list(values, vectors)?),
            Self::Struct(fields) => Value::Struct(fields.into_iter().map(|(name, value)| Ok((name, value.restore(vectors)?))).collect::<Result<_>>()?),
            Self::Map { entries, sorted } => Value::Map {
                entries: entries.into_iter().map(|(key, value)| Ok((key.restore(vectors)?, value.restore(vectors)?))).collect::<Result<_>>()?,
                sorted,
            },
        })
    }

    fn restore_list(values: Vec<Self>, vectors: &[ArrayRef]) -> Result<Vec<Value>> {
        values.into_iter().map(|value| value.restore(vectors)).collect()
    }
}

fn encode_arrow_row(values: &[Value]) -> Result<Vec<u8>> {
    let mut vectors = Vec::new();
    let cells = values.iter().map(|value| SpillCell::extract(value, &mut vectors)).collect::<Result<Vec<_>>>()?;
    let row = serde_json::to_vec(&cells).map_err(io_error)?;
    let mut columns: Vec<ArrayRef> = vec![Arc::new(BinaryArray::from_iter_values([row.as_slice()]))];
    columns.extend(vectors);
    let schema = Arc::new(Schema::new(columns.iter().enumerate().map(|(index, array)| {
        Field::new(index.to_string(), array.data_type().clone(), false)
    }).collect::<Vec<_>>()));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(io_error)?;
    let mut bytes = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema).map_err(io_error)?;
    writer.write(&batch).map_err(io_error)?;
    writer.finish().map_err(io_error)?;
    drop(writer);
    Ok(bytes)
}

fn decode_arrow_row(bytes: &[u8]) -> Result<Vec<Value>> {
    let mut reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes), None).map_err(io_error)?;
    let batch = reader.next().transpose().map_err(io_error)?
        .filter(|batch| batch.num_rows() == 1 && batch.num_columns() > 0)
        .ok_or_else(|| invalid("invalid-spill", "Missing Arrow spill row"))?;
    let row = batch.column(0).as_any().downcast_ref::<BinaryArray>()
        .filter(|values| values.is_valid(0)).ok_or_else(|| invalid("invalid-spill", "Missing Arrow spill descriptor"))?;
    let cells = serde_json::from_slice::<Vec<SpillCell<'_>>>(row.value(0)).map_err(io_error)?;
    SpillCell::restore_list(cells, &batch.columns()[1..])
}

fn io_error(error: impl std::fmt::Display) -> rtdb_types::Anomaly { rtdb_types::Anomaly::fault("rtdb.query/spill-io", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_embedding_survives_spill_and_random_access() {
        let memory = MemoryBudget::new(16 << 20);
        let region = Arc::new(SpillRegion::new(16 << 20).unwrap());
        let mut writer = region.create().unwrap();
        writer.write(&[Value::I64(5)], &memory).unwrap();
        let position = writer.position().unwrap();
        let components = Float64Array::from(vec![Some(-0.0), None, Some(f64::from_bits(0x7ff8000000000003))]);
        let document = Value::Struct(StructFields::from([("embedding".into(), Value::FixedSizeList(ListValue::from_f64(components)))]));
        writer.write(&[Value::Utf8("doc".into()), document], &memory).unwrap();
        let file = writer.finish().unwrap();
        let mut reader = file.reader(memory).unwrap();
        reader.seek(position).unwrap();
        let row = reader.next().unwrap().unwrap();
        let vector = row.values[1].field("embedding").unwrap();
        let Some(ListView::F64(array)) = vector.list_view() else { panic!("expected Arrow components"); };
        assert_eq!(array.value(0).to_bits(), (-0.0_f64).to_bits());
        assert!(array.is_null(1));
        assert_eq!(array.value(2).to_bits(), 0x7ff8000000000003);
        assert!(reader.next().unwrap().is_none());
        reader.rewind().unwrap();
        assert_eq!(reader.next().unwrap().unwrap().values, [Value::I64(5)]);
    }
}
