use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use rtdb_columnar::MemoryBudget;
use rtdb_types::{Result, Value};
use tempfile::{NamedTempFile, TempDir, TempPath};

use crate::{cursor::OwnedRow, invalid};

pub(crate) struct SpillRegion { directory: TempDir, limit: u64, retained: AtomicU64, peak: AtomicU64 }
impl SpillRegion {
    pub fn new(limit: u64) -> Result<Self> { Ok(Self { directory: TempDir::new().map_err(io_error)?, limit, retained: AtomicU64::new(0), peak: AtomicU64::new(0) }) }
    pub fn retained_bytes(&self) -> u64 { self.retained.load(Ordering::Acquire) }
    pub fn peak_bytes(&self) -> u64 { self.peak.load(Ordering::Relaxed) }
    fn reserve(&self, bytes: u64) -> Result<()> {
        let old = self.retained.fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| old.checked_add(bytes).filter(|next| *next <= self.limit))
            .map_err(|_| invalid("spill-limit", "Query spill byte limit exceeded"))?;
        self.peak.fetch_max(old + bytes, Ordering::Relaxed);
        Ok(())
    }
    pub fn create(self: &Arc<Self>) -> Result<SpillWriter> {
        let (file, path) = NamedTempFile::new_in(self.directory.path()).map_err(io_error)?.into_parts();
        Ok(SpillWriter { file, retained: Arc::new(SpillFile { path, region: Arc::clone(self), bytes: AtomicU64::new(0) }) })
    }
}

pub(crate) struct SpillFile { path: TempPath, region: Arc<SpillRegion>, bytes: AtomicU64 }
impl Drop for SpillFile {
    fn drop(&mut self) { self.region.retained.fetch_sub(self.bytes.load(Ordering::Relaxed), Ordering::AcqRel); }
}
impl SpillFile {
    pub fn reader(self: &Arc<Self>, memory: Arc<MemoryBudget>) -> Result<SpillReader> {
        let file = File::open(&self.path).map_err(io_error)?;
        Ok(SpillReader { file, retained: Arc::clone(self), memory })
    }
}

pub(crate) struct SpillWriter { file: File, retained: Arc<SpillFile> }
impl SpillWriter {
    pub fn reader(&mut self, memory: Arc<MemoryBudget>) -> Result<SpillReader> {
        self.file.flush().map_err(io_error)?;
        self.retained.reader(memory)
    }
    pub fn write(&mut self, values: &[Value], memory: &Arc<MemoryBudget>) -> Result<()> {
        let retained = values.iter().try_fold(1024usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(|| invalid("row-size", "Spill row size overflow")))?;
        let _temporary = memory.reserve(retained.checked_mul(8).ok_or_else(|| invalid("row-size", "Encoded spill row size overflow"))?)?;
        let bytes = serde_json::to_vec(values).map_err(io_error)?;
        let count = (bytes.len() as u64).checked_add(8).ok_or_else(|| invalid("row-size", "Spill row size overflow"))?;
        self.retained.region.reserve(count)?;
        self.retained.bytes.fetch_add(count, Ordering::Relaxed);
        self.file.write_all(&(bytes.len() as u64).to_le_bytes()).map_err(io_error)?;
        self.file.write_all(&bytes).map_err(io_error)
    }
    pub fn finish(mut self) -> Result<Arc<SpillFile>> {
        self.file.flush().map_err(io_error)?;
        Ok(self.retained)
    }
}

pub(crate) struct SpillReader { file: File, retained: Arc<SpillFile>, memory: Arc<MemoryBudget> }
impl SpillReader {
    pub fn rewind(&mut self) -> Result<()> { self.file.seek(SeekFrom::Start(0)).map_err(io_error)?; Ok(()) }
    pub fn next(&mut self) -> Result<Option<OwnedRow>> {
        let mut prefix = [0u8; 8];
        match self.file.read(&mut prefix[..1]).map_err(io_error)? { 0 => return Ok(None), _ => {} }
        self.file.read_exact(&mut prefix[1..]).map_err(io_error)?;
        let length = usize::try_from(u64::from_le_bytes(prefix)).map_err(io_error)?;
        if length as u64 > self.retained.bytes.load(Ordering::Relaxed) { return Err(invalid("invalid-spill", "Spill row exceeds its file")); }
        let bound = length.checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("row-size", "Decoded spill size overflow"))?;
        let reservation = self.memory.reserve(bound)?;
        let mut bytes = vec![0; length];
        self.file.read_exact(&mut bytes).map_err(io_error)?;
        let values = serde_json::from_slice::<Vec<Value>>(&bytes).map_err(io_error)?;
        OwnedRow::with_reservation(values, reservation).map(Some)
    }
}

fn io_error(error: impl std::fmt::Display) -> rtdb_types::Anomaly { rtdb_types::Anomaly::fault("rtdb.query/spill-io", error.to_string()) }
