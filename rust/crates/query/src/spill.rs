use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::{
    MetadataVersion,
    reader::{StreamDecoder, read_record_batch},
    writer::{CompressionContext, DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions, write_message},
};
use arrow_schema::{ArrowError, SchemaRef};
use ctr::cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_format::ipc::RecordSpans;
use rtdb_types::Result;
use tempfile::{NamedTempFile, TempDir, TempPath};

use crate::{
    columnar::{BatchCursor, ColumnBatch},
    fragment::{FragmentBytes, decode_batches, encode_batch},
    invalid,
};

const IPC_ALIGNMENT: usize = 64;

/// How a query's spill files hold their rows on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SpillSealing {
    /// As written, which is what the reference does.
    Plain,
    /// Encrypted under a key made for each file and held only in the query's memory, for a query reading a store under
    /// local erasure custody: no readable copy of an erased row may outlive the query, and a file a crash leaves behind
    /// is unreadable once the process that held its key has gone.
    Sealed,
}

/// The stream cipher a sealed spill file is encrypted with: positional, so a frame reads back from any offset and a
/// rolled-back tail is rewritten in place, and length-preserving, so the spill quota counts the same bytes either way.
type SpillCipher = ctr::Ctr128BE<aes::Aes128>;

/// The key of one sealed spill file; each file has its own, so one counter sequence never encrypts two files.
#[derive(Clone, Copy)]
struct SpillKey([u8; 16]);

impl SpillKey {
    fn generate() -> Result<Self> {
        use ring::rand::SecureRandom;
        let mut key = [0; 16];
        ring::rand::SystemRandom::new().fill(&mut key).map_err(|_| io_error("The system random source refused a spill key"))?;
        Ok(Self(key))
    }

    /// The cipher positioned at byte `offset` of the file.
    fn cipher_at(&self, offset: u64) -> std::io::Result<SpillCipher> {
        let mut cipher = SpillCipher::new_from_slices(&self.0, &[0; 16]).map_err(|_| std::io::Error::other("The spill key has the wrong length"))?;
        cipher.try_seek(offset).map_err(|_| keystream_exhausted())?;
        Ok(cipher)
    }
}

fn keystream_exhausted() -> std::io::Error { std::io::Error::other("The spill file exceeds its cipher's keystream") }

/// The bytes one cipher step takes from the caller at a time, encrypted on the stack rather than in a heap buffer.
const SEAL_CHUNK_BYTES: usize = 4 << 10;

/// A spill file as its writer sees it: the bytes it writes reach the disk encrypted when the file has a key.
struct SpillSink { file: File, cipher: Option<(SpillKey, SpillCipher)> }

impl Write for SpillSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let Some((_, cipher)) = &mut self.cipher else { return self.file.write(bytes) };
        let mut chunk = [0; SEAL_CHUNK_BYTES];
        let length = bytes.len().min(SEAL_CHUNK_BYTES);
        chunk[..length].copy_from_slice(&bytes[..length]);
        cipher.try_apply_keystream(&mut chunk[..length]).map_err(|_| keystream_exhausted())?;
        // A partial write would leave the keystream ahead of the file, so the chunk is written whole or not at all.
        self.file.write_all(&chunk[..length])?;
        Ok(length)
    }

    fn flush(&mut self) -> std::io::Result<()> { self.file.flush() }
}

impl Seek for SpillSink {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        let offset = self.file.seek(position)?;
        if let Some((key, cipher)) = &mut self.cipher { *cipher = key.cipher_at(offset)?; }
        Ok(offset)
    }
}

/// A spill file as one of its readers sees it, decrypting what it reads when the file has a key.
struct SpillSource { file: File, cipher: Option<(SpillKey, SpillCipher)> }

impl Read for SpillSource {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let read = self.file.read(bytes)?;
        if let Some((_, cipher)) = &mut self.cipher { cipher.try_apply_keystream(&mut bytes[..read]).map_err(|_| keystream_exhausted())?; }
        Ok(read)
    }
}

impl Seek for SpillSource {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        let offset = self.file.seek(position)?;
        if let Some((key, cipher)) = &mut self.cipher { *cipher = key.cipher_at(offset)?; }
        Ok(offset)
    }
}

/// A query's spill files, in a directory created by the first of them: most queries never spill.
pub(crate) struct SpillRegion { directory: std::sync::OnceLock<TempDir>, limit: u64, sealing: SpillSealing, retained: AtomicU64, peak: AtomicU64 }
impl SpillRegion {
    /// Spill files that hold their rows as written.
    pub fn new(limit: u64) -> Result<Self> { Ok(Self::with_sealing(limit, SpillSealing::Plain)) }
    /// Spill files each encrypted under a key only this region's memory holds; see [`SpillSealing::Sealed`].
    pub fn sealed(limit: u64) -> Result<Self> { Ok(Self::with_sealing(limit, SpillSealing::Sealed)) }
    fn with_sealing(limit: u64, sealing: SpillSealing) -> Self {
        Self { directory: std::sync::OnceLock::new(), limit, sealing, retained: AtomicU64::new(0), peak: AtomicU64::new(0) }
    }
    fn directory(&self) -> Result<&std::path::Path> {
        if let Some(directory) = self.directory.get() { return Ok(directory.path()); }
        let created = TempDir::new().map_err(io_error)?;
        Ok(self.directory.get_or_init(|| created).path())
    }
    pub fn retained_bytes(&self) -> u64 { self.retained.load(Ordering::Acquire) }
    pub fn peak_bytes(&self) -> u64 { self.peak.load(Ordering::Relaxed) }
    fn reserve(&self, bytes: u64) -> Result<()> {
        let old = self.retained.fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| old.checked_add(bytes).filter(|next| *next <= self.limit))
            .map_err(|_| spill_limit("Query spill byte limit exceeded"))?;
        self.peak.fetch_max(old + bytes, Ordering::Relaxed);
        Ok(())
    }
    pub fn create(self: &Arc<Self>) -> Result<SpillWriter> {
        if self.limit == 0 { return Err(spill_limit("Query spilling is disabled")); }
        let key = match self.sealing { SpillSealing::Plain => None, SpillSealing::Sealed => Some(SpillKey::generate()?) };
        let (file, path) = NamedTempFile::new_in(self.directory()?).map_err(io_error)?.into_parts();
        let cipher = key.map(|key| key.cipher_at(0).map(|cipher| (key, cipher))).transpose().map_err(io_error)?;
        let sink = SpillSink { file, cipher };
        Ok(SpillWriter { file: BufWriter::with_capacity(WRITER_BUFFER_BYTES, sink), position: 0, retained: Arc::new(SpillFile { path, key, region: Arc::clone(self), bytes: AtomicU64::new(0) }) })
    }
}

/// The spill quota refusing more bytes: SQLSTATE 53400, which PostgreSQL reports when a query's temporary files
/// exceed `temp_file_limit`.
fn spill_limit(message: &str) -> rtdb_types::Anomaly {
    invalid("spill-limit", message).with_kind(rtdb_types::AnomalyKind::ResourceExhausted).with_context("sql-state", "53400".into())
}

pub(crate) struct SpillFile { path: TempPath, key: Option<SpillKey>, region: Arc<SpillRegion>, bytes: AtomicU64 }
impl Drop for SpillFile {
    fn drop(&mut self) { self.region.retained.fetch_sub(self.bytes.load(Ordering::Relaxed), Ordering::AcqRel); }
}
impl SpillFile {
    pub fn reader(self: &Arc<Self>, memory: Arc<MemoryBudget>) -> Result<SpillReader> {
        let file = File::open(&self.path).map_err(io_error)?;
        let cipher = self.key.map(|key| key.cipher_at(0).map(|cipher| (key, cipher))).transpose().map_err(io_error)?;
        Ok(SpillReader { file: BufReader::new(SpillSource { file, cipher }), retained: Arc::clone(self), memory })
    }
}

pub(crate) struct SpillWriter { file: BufWriter<SpillSink>, position: u64, retained: Arc<SpillFile> }
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
        &mut self, marker: Option<u8>, length: u64, write: impl FnOnce(&mut BufWriter<SpillSink>) -> Result<()>,
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

    pub fn write_single_join_record(&mut self, batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<()> {
        self.write_frame(&[3])?;
        if let Some(spans) = exact_spill_spans(batch, memory)? {
            return self.write_spanned_record(&spans);
        }
        self.write_record(batch, memory)
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
        let spans = exact_spill_spans(batch, memory)?;
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
        self.file.get_ref().file.set_len(offset).map_err(io_error)?;
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

fn exact_spill_spans(batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<Option<RecordSpans>> {
    match RecordSpans::exact(batch, IPC_ALIGNMENT, memory) {
        // A small record can fit its generic encoding reservation even when span planning runs out of scratch.
        Err(error) if matches!(error.code.as_str(), "rtdb/memory-budget-exceeded" | "rtdb.format/invalid-arrow") => Ok(None),
        result => result,
    }
}

pub(crate) struct SpillReader { file: BufReader<SpillSource>, retained: Arc<SpillFile>, memory: Arc<MemoryBudget> }
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
                let batches = decode_batches(bytes, &self.memory)?;
                ColumnBatch::from_spill_fragments(batches, width, &self.memory).map(Some)
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

/// Writes a page, halving it while its encoding does not fit the memory budget.
pub(crate) fn write_page(writer: &mut SpillWriter, batch: &ColumnBatch, schema: &rtdb_plan::Schema, memory: &Arc<MemoryBudget>) -> Result<()> {
    let mut spans = vec![0..batch.len];
    let positions = (0..schema.len()).collect::<Vec<_>>();
    while let Some(span) = spans.pop() {
        let start = writer.position()?;
        let result = if span.len() == batch.len {
            writer.write_columns(batch, schema, memory)
        } else {
            let bytes = span.len().checked_mul(std::mem::size_of::<(usize, usize)>())
                .ok_or_else(|| invalid("row-size", "Spill page selection exceeds addressable memory"))?;
            let _picks = memory.reserve(bytes)?;
            let picks = span.clone().map(|row| (0, row)).collect::<Vec<_>>();
            let slice = ColumnBatch::take_rows_with_custody(&[batch], &picks, &positions, true, memory)?;
            writer.write_columns(&slice, schema, memory)
        };
        if let Err(error) = result {
            if error.code != "rtdb/memory-budget-exceeded" || span.len() < 2 { return Err(error); }
            writer.discard_from(start)?;
            let mid = span.start + span.len() / 2;
            spans.push(mid..span.end);
            spans.push(span.start..mid);
        }
    }
    Ok(())
}

/// Whether a hash operator at partitioning level `depth`, grown by `grown` bytes since its first page, should send rows
/// of new keys to disk: past its configured share, and while it has partitioning levels left.
/// The share alone decides, never what the budget has free.
pub(crate) fn hash_spill_due(options: &crate::QueryOptions, depth: u32, grown: usize) -> bool {
    options.spill_limit_bytes != 0 && depth < options.max_spill_depth && grown > options.hash_memory_bytes
}

/// The buffer of each spill writer, charged by the hash partitions that hold one writer per partition.
const WRITER_BUFFER_BYTES: usize = 8 << 10;

/// Rows routed by key hash to one spill file per partition, so that every row of a key lands in one file, in input order.
pub(crate) struct HashPartitions { writers: Vec<Option<SpillWriter>>, depth: u32, _buffers: Arc<MemoryReservation> }

impl HashPartitions {
    pub fn new(depth: u32, context: &crate::QueryContext) -> Result<Self> {
        let partitions = context.options.spill_partitions;
        let bytes = partitions.checked_mul(WRITER_BUFFER_BYTES + 256)
            .ok_or_else(|| invalid("row-size", "Spill partition buffers exceed addressable memory"))?;
        let buffers = context.memory.reserve(bytes)?;
        Ok(Self { writers: (0..partitions).map(|_| None).collect(), depth, _buffers: buffers })
    }

    /// Each level mixes the hash with its depth, so that one partition's keys spread again at the next level.
    pub fn partition(&self, hash: u32) -> usize {
        let mut mixed = hash ^ self.depth.wrapping_add(1).wrapping_mul(0x9e37_79b9);
        mixed ^= mixed >> 16;
        mixed = mixed.wrapping_mul(0x85eb_ca6b);
        mixed ^= mixed >> 13;
        mixed = mixed.wrapping_mul(0xc2b2_ae35);
        mixed ^= mixed >> 16;
        mixed as usize % self.writers.len()
    }

    /// Appends each row to the partition `targets` names for it; a row without one stays with the caller.
    pub fn write(&mut self, batch: &ColumnBatch, targets: &[Option<usize>], schema: &rtdb_plan::Schema, context: &crate::QueryContext) -> Result<()> {
        if targets.len() != batch.len { return Err(invalid("invalid-spill", "Spill partition targets disagree with their page")); }
        let memory = &context.memory;
        let bytes = batch.len.checked_mul(std::mem::size_of::<(usize, usize)>())
            .and_then(|bytes| bytes.checked_add(batch.columns.len().checked_mul(std::mem::size_of::<usize>())?))
            .ok_or_else(|| invalid("row-size", "Spill partition selection exceeds addressable memory"))?;
        let _picks = memory.reserve(bytes)?;
        let positions = (0..batch.columns.len()).collect::<Vec<_>>();
        for partition in 0..self.writers.len() {
            context.check_cancelled()?;
            let rows = targets.iter().enumerate().filter(|(_, target)| **target == Some(partition))
                .map(|(row, _)| (0, row)).collect::<Vec<_>>();
            if rows.is_empty() { continue; }
            let page = ColumnBatch::take_rows(&[batch], &rows, &positions, memory)?;
            if self.writers[partition].is_none() { self.writers[partition] = Some(context.spill.create()?); }
            let writer = self.writers[partition].as_mut().expect("the partition's writer is open");
            write_page(writer, &page, schema, memory)?;
        }
        Ok(())
    }

    /// The partitions that received rows; dropping one deletes its file.
    pub fn finish(self) -> Result<Vec<Arc<SpillFile>>> {
        self.writers.into_iter().flatten().map(SpillWriter::finish).collect()
    }
}

/// Replays the pages of one spill file, which is deleted once the replay is dropped.
pub(crate) struct SpillReplay { reader: SpillReader, width: usize, context: Arc<crate::QueryContext> }

impl SpillReplay {
    pub fn new(file: Arc<SpillFile>, width: usize, context: &Arc<crate::QueryContext>) -> Result<Self> {
        Ok(Self { reader: file.reader(Arc::clone(&context.memory))?, width, context: Arc::clone(context) })
    }
}

#[async_trait::async_trait]
impl BatchCursor for SpillReplay {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.context.check_cancelled()?;
        let page = self.reader.read_columns(self.width)?;
        tokio::task::consume_budget().await;
        Ok(page)
    }
}

impl crate::QueryOptions {
    /// Half the free space on the filesystem of the directory queries spill to, at the moment of the call, and never
    /// below the default `spill_limit_bytes`.
    /// A node's startup configuration calls it; the default options never read the disk.
    pub fn spill_limit_from_free_disk() -> u64 { spill_limit_for(free_bytes(&std::env::temp_dir())) }
}

fn spill_limit_for(free: Option<u64>) -> u64 {
    let floor = crate::QueryOptions::default().spill_limit_bytes;
    free.map_or(floor, |free| (free / 2).max(floor))
}

#[cfg(unix)]
#[allow(clippy::unnecessary_cast)]
fn free_bytes(path: &std::path::Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `stat` points to writable storage for one `statvfs`.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 { return None; }
    // SAFETY: a successful `statvfs` initialised `stat`.
    let stat = unsafe { stat.assume_init() };
    (stat.f_bavail as u64).checked_mul(stat.f_frsize as u64)
}

#[cfg(not(unix))]
fn free_bytes(_path: &std::path::Path) -> Option<u64> { None }

fn ipc_error(error: impl std::fmt::Display) -> rtdb_types::Anomaly { invalid("join-spill", error) }

fn io_error(error: impl std::fmt::Display) -> rtdb_types::Anomaly { rtdb_types::Anomaly::fault("rtdb.query/spill-io", error.to_string()) }

#[test]
fn the_free_disk_spill_limit_is_half_the_free_disk_and_never_below_the_default() {
    let floor = crate::QueryOptions::default().spill_limit_bytes;
    assert_eq!(floor, 1 << 30);
    assert_eq!(spill_limit_for(None), floor);
    assert_eq!(spill_limit_for(Some(1 << 30)), floor);
    assert_eq!(spill_limit_for(Some(64 << 30)), 32 << 30);
    assert!(crate::QueryOptions::spill_limit_from_free_disk() >= floor);
}

#[cfg(test)]
fn options_context(options: crate::QueryOptions) -> Arc<crate::QueryContext> {
    use std::collections::BTreeMap;
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    let database = crate::DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = crate::CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    crate::QueryContext::new(catalog, MemoryBudget::new(1 << 20), rtdb_types::Timestamp(0), "UTC", options, &tokio_util::sync::CancellationToken::new()).unwrap()
}

#[test]
fn hash_partitions_separate_a_partitions_keys_at_the_next_level() {
    let context = options_context(crate::QueryOptions::default());
    let partitions = context.options.spill_partitions;
    let (top, next) = (HashPartitions::new(0, &context).unwrap(), HashPartitions::new(1, &context).unwrap());
    let hashes = (0..4096u32).map(|key| key.wrapping_mul(0x2545_f491)).collect::<Vec<_>>();
    let first = hashes.iter().copied().filter(|&hash| top.partition(hash) == 0).collect::<Vec<_>>();
    assert!(!first.is_empty());
    let spread = first.iter().map(|&hash| next.partition(hash)).collect::<std::collections::BTreeSet<_>>();
    assert!(spread.len() > partitions / 2, "{spread:?}");
}

#[test]
fn hash_partitions_fan_out_to_the_configured_partitions_and_charge_their_writer_buffers() {
    let context = options_context(crate::QueryOptions { spill_partitions: 3, ..Default::default() });
    let partitions = HashPartitions::new(0, &context).unwrap();
    assert_eq!(context.memory.used_bytes(), 3 * (WRITER_BUFFER_BYTES + 256));
    let used = (0..4096u32).map(|hash| partitions.partition(hash)).collect::<std::collections::BTreeSet<_>>();
    assert_eq!(used, std::collections::BTreeSet::from([0, 1, 2]));
}

#[test]
fn a_hash_operator_spills_past_its_share_alone_while_levels_remain() {
    let options = crate::QueryOptions { hash_memory_bytes: 1 << 10, max_spill_depth: 2, ..Default::default() };
    assert!(!hash_spill_due(&options, 0, 1 << 10));
    assert!(hash_spill_due(&options, 0, (1 << 10) + 1));
    assert!(hash_spill_due(&options, 1, usize::MAX));
    assert!(!hash_spill_due(&options, 2, usize::MAX), "the last level holds what remains");
    assert!(!hash_spill_due(&crate::QueryOptions { spill_limit_bytes: 0, ..options }, 0, usize::MAX), "spilling is disabled");
}

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
fn join_spill_replays_small_records_across_offsets_with_generic_scratch() {
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let memory = MemoryBudget::new(1 << 20);
    let scratch = MemoryBudget::new(32 << 10);
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
    writer.write_single_join_record(&first, &scratch).unwrap();
    let offset = writer.position().unwrap();
    writer.write_single_join_record(&second, &scratch).unwrap();
    assert_eq!(scratch.used_bytes(), 0);
    let mut reader = writer.finish().unwrap().reader(Arc::clone(&memory)).unwrap();
    reader.seek(offset).unwrap();
    assert_eq!(reader.read_frame(1).unwrap().unwrap().0, [3]);
    assert_eq!(reader.read_record(&schema, 0).unwrap().unwrap().batch(), &second);
    assert!(reader.read_record(&schema, 0).unwrap().is_none());
    reader.rewind().unwrap();
    assert_eq!(reader.read_frame(1).unwrap().unwrap().0, [3]);
    assert_eq!(reader.read_record(&schema, 0).unwrap().unwrap().batch(), &first);
    drop(reader);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(spill.retained_bytes(), 0);
}

#[test]
fn single_join_spill_matches_the_encoded_record_with_only_span_scratch() {
    use arrow_array::{ArrayRef, BooleanArray, Int64Array};

    let memory = MemoryBudget::new(1 << 24);
    let scratch = MemoryBudget::new(1 << 17);
    let rows = 1 << 15;
    let flags: BooleanArray = (0..=rows).map(|row| (row % 3 != 0).then_some(row % 2 == 0)).collect();
    let batch = RecordBatch::try_from_iter([
        ("key", Arc::new(Int64Array::from(vec![17; rows])) as ArrayRef),
        ("flag", Arc::new(flags.slice(1, rows)) as ArrayRef),
    ]).unwrap();
    let owned = OwnedBatch::new(batch, &memory).unwrap();
    let batch = owned.batch();
    let (dictionaries, encoded) = IpcDataGenerator::default().encode(
        batch, &mut DictionaryTracker::new(false), &IpcWriteOptions::default(), &mut CompressionContext::default(),
    ).unwrap();
    assert!(dictionaries.is_empty());
    assert!(encoded.arrow_data.len() > scratch.limit_bytes());
    let mut expected = Vec::new();
    expected.extend_from_slice(&(encoded.ipc_message.len() as u64).to_le_bytes());
    expected.extend_from_slice(&encoded.ipc_message);
    expected.extend_from_slice(&encoded.arrow_data);
    let size = expected.len() as u64 + 17;

    let spill = Arc::new(SpillRegion::new(size + 9).unwrap());
    let mut writer = spill.create().unwrap();
    writer.write_single_join_record(batch, &scratch).unwrap();
    assert_eq!(writer.position().unwrap(), size);
    let error = writer.write_single_join_record(batch, &scratch).unwrap_err();
    assert_eq!(error.code, "rtdb.query/spill-limit");
    assert_eq!(writer.position().unwrap(), size + 9);
    assert_eq!(spill.retained_bytes(), size + 9);
    assert_eq!(scratch.used_bytes(), 0);

    let mut reader = writer.finish().unwrap().reader(Arc::clone(&memory)).unwrap();
    assert_eq!(reader.read_frame(1).unwrap().unwrap().0, [3]);
    assert_eq!(reader.read_frame(1).unwrap().unwrap().0, expected);
    assert_eq!(reader.read_frame(1).unwrap().unwrap().0, [3]);
    assert!(reader.read_frame(1).unwrap().is_none());
    reader.rewind().unwrap();
    assert_eq!(reader.read_frame(1).unwrap().unwrap().0, [3]);
    assert_eq!(reader.read_record(batch.schema_ref(), 0).unwrap().unwrap().batch(), batch);
    drop((reader, owned));
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
        assert!(RecordSpans::exact(&record, IPC_ALIGNMENT, &memory).unwrap().is_none());
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
    let tight_memory = MemoryBudget::new(wire_bytes + (56 << 10));
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
    let mut wide_reader = file.reader(Arc::clone(&replay_memory)).unwrap();
    assert_eq!(wide_reader.read_columns(2).err().unwrap().code, "rtdb.query/column-gather");
    drop(wide_reader);
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

#[test]
fn mixed_spill_assembly_keeps_whole_fragments_and_distinguishes_missing_cells() {
    use arrow_array::{ArrayRef, Int64Array, NullArray, StructArray};
    use arrow_schema::{DataType, Field, Schema};
    use rtdb_types::{StructFields, Value};

    let backing = MemoryBudget::new(1 << 20);
    let memory = MemoryBudget::new(4096);
    let short = ColumnBatch::empty_with_width(0, &backing).unwrap();
    let error = ColumnBatch::from_spill_fragments(vec![short], 1, &memory).err().unwrap();
    assert_eq!(error.code, "rtdb.query/column-gather");
    let page = |array: ArrayRef| {
        let schema = Arc::new(Schema::new(vec![Field::new("x", array.data_type().clone(), true)]));
        let record = RecordBatch::try_new(schema, vec![array]).unwrap();
        ColumnBatch::from_owned(OwnedBatch::new(record, &backing).unwrap()).unwrap()
    };
    let rows = 4096;
    let input = vec![7i64; rows];
    let nested = StructArray::from(vec![(
        Arc::new(Field::new("a", DataType::Int64, false)), Arc::new(Int64Array::from(input)) as ArrayRef,
    )]);
    let fragments = vec![
        page(Arc::new(nested)),
        ColumnBatch::empty_with_width(1, &backing).unwrap(),
        page(Arc::new(Int64Array::from(vec![None, Some(42)]))),
        page(Arc::new(NullArray::new(1))),
    ];
    let replay = ColumnBatch::from_spill_fragments(fragments, 1, &memory).unwrap();
    assert_eq!(replay.len, rows + 3);
    assert_eq!(replay.columns.len(), 1);
    let column = &replay.columns[0];
    let nested = Value::Struct(StructFields::from([("a".into(), Value::I64(7))]));
    assert_eq!(column.value(0).unwrap(), nested);
    assert_eq!(column.value(rows - 1).unwrap(), nested);
    assert!(matches!(column.arrow_cell(rows), Some(Some((array, _, _, row))) if array.is_null(row)));
    assert_eq!(column.value(rows + 1).unwrap(), Value::I64(42));
    assert!(matches!(column.arrow_cell(rows + 2), Some(None)));
    assert!(backing.used_bytes() > 0);
    drop(replay);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(backing.used_bytes(), 0);
}

#[test]
fn malformed_spill_fragments_keep_the_decoder_error() {
    let memory = MemoryBudget::new(1 << 20);
    let spill = Arc::new(SpillRegion::new(64).unwrap());
    let mut writer = spill.create().unwrap();
    writer.write_frame(&[2]).unwrap();
    writer.write_frame(&[0; 7]).unwrap();
    let mut reader = writer.finish().unwrap().reader(Arc::clone(&memory)).unwrap();
    let error = reader.read_columns(1).err().unwrap();
    assert_eq!(error.code, "rtdb.query/fragment-decode");
    assert!(error.message.as_deref().unwrap().contains("fragment frame is truncated"));
}

#[test]
fn sealed_spill_files_hold_no_readable_rows_and_replay_from_any_offset() {
    let memory = MemoryBudget::new(1 << 20);
    let secret = b"an erased row's payload, which must not reach the disk in the clear".to_vec();
    let long = vec![7; SEAL_CHUNK_BYTES * 2 + 3];
    let mut ciphertexts = Vec::new();
    for (region, readable) in [(SpillRegion::new(1 << 16).unwrap(), true), (SpillRegion::sealed(1 << 16).unwrap(), false), (SpillRegion::sealed(1 << 16).unwrap(), false)] {
        let spill = Arc::new(region);
        let mut writer = spill.create().unwrap();
        writer.write_frame(b"first").unwrap();
        let start = writer.position().unwrap();
        writer.write_frame(&secret).unwrap();
        let rollback = writer.position().unwrap();
        writer.write_frame(b"rolled back").unwrap();
        writer.discard_from(rollback).unwrap();
        writer.write_frame(&long).unwrap();
        let file = writer.finish().unwrap();
        let raw = std::fs::read(&*file.path).unwrap();
        assert_eq!(raw.len() as u64, spill.retained_bytes(), "sealing keeps every frame's length");
        assert_eq!(raw.windows(secret.len()).any(|window| window == secret.as_slice()), readable);
        if !readable { ciphertexts.push(raw); }
        let mut reader = file.reader(Arc::clone(&memory)).unwrap();
        reader.seek(start).unwrap();
        assert_eq!(reader.read_frame(1).unwrap().unwrap().0, secret);
        assert_eq!(reader.read_frame(1).unwrap().unwrap().0, long);
        assert!(reader.read_frame(1).unwrap().is_none());
        reader.rewind().unwrap();
        assert_eq!(reader.read_frame(1).unwrap().unwrap().0, b"first".to_vec());
        drop((reader, file));
        assert_eq!(spill.retained_bytes(), 0);
    }
    assert_ne!(ciphertexts[0], ciphertexts[1], "each sealed file has its own key");
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn a_refused_spill_reports_an_exhausted_temporary_file_limit() {
    for spill in [SpillRegion::new(0).unwrap(), SpillRegion::sealed(0).unwrap()] {
        let error = Arc::new(spill).create().err().unwrap();
        assert_eq!(error.code, "rtdb.query/spill-limit");
        assert!(error.is(rtdb_types::AnomalyKind::ResourceExhausted));
        assert_eq!(error.context.get("sql-state").and_then(serde_json::Value::as_str), Some("53400"));
    }
}
