use super::{AppendReceipt, AppendTimestampSource, LOG_RECORD_OVERHEAD_BYTES, Log, LogMetadata, LogOffset, LogRecord, LogReplayTail, LogTail, Progress, ReplayHead, SuppliedTimestamp, await_tip};
use super::journal_file::JournalFile;
use crate::{AppleSync, Error, IoRegion, Result, object_store::ReadReservation, sealing::{ClassicKeyVault, ClassicObjectKeys, LOG_SEAL_OVERHEAD}};
use async_trait::async_trait;
use bytes::Bytes;
use rtdb_types::Timestamp;
use std::collections::VecDeque;
use std::fs;
use std::io::{self, ErrorKind, IoSlice, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(crate) mod erasure;


const HEADER_BYTES: u64 = 13;
const TRAILER_BYTES: u64 = 8;
const RECORD_SEPARATOR: u8 = 0x1e;
const RETIRED_SEPARATOR: u8 = 0x1d;
const SEALED_SEPARATOR: u8 = 0x1c;
const RETIRED_SEALED_SEPARATOR: u8 = 0x1b;

pub(super) fn is_record_separator(byte: u8) -> bool {
    matches!(byte, RECORD_SEPARATOR | RETIRED_SEPARATOR | SEALED_SEPARATOR | RETIRED_SEALED_SEPARATOR)
}
const APPEND_BATCH_BYTES: usize = 256 * 1024;
const APPEND_BATCH_RECORDS: usize = 10;
const MAXIMUM_POLL_RECORDS: usize = 1024;
const DEFAULT_MAXIMUM_POLL_PAYLOAD_BYTES: usize = 128 * 1024 * 1024;
const RECENT_FRAMES: usize = 256;
const INLINE_READ_BYTES: usize = 64 * 1024;

pub struct LocalLogOptions {
    pub maximum_message_bytes: usize,
    pub queue_capacity: usize,
}

struct Queued { payload: Bytes, timestamp: Timestamp, durable: oneshot::Sender<Result<LogMetadata>> }

impl Queued {
    fn frame_bytes(&self, sealed: bool) -> usize {
        self.payload.len() + (HEADER_BYTES + TRAILER_BYTES) as usize
            + if sealed { LOG_SEAL_OVERHEAD } else { 0 }
    }
}

pub struct LocalLog {
    path: Arc<PathBuf>,
    lineage: Option<Uuid>,
    queue: mpsc::Sender<Queued>,
    progress: Arc<Progress>,
    maximum_message_bytes: usize,
    keys: Option<Arc<ClassicObjectKeys>>,
    reads: Arc<IoRegion>,
    recent: Arc<RecentFrames>,
    apple_sync: AppleSync,
}

pub struct LocalLogDriver {
    writer: Writer,
    queue: mpsc::Receiver<Queued>,
    progress: Arc<Progress>,
    recent: Arc<RecentFrames>,
}

struct Writer {
    file: JournalFile,
    end: u64,
    path: Arc<PathBuf>,
    timestamps: Arc<dyn AppendTimestampSource>,
    keys: Option<Arc<ClassicObjectKeys>>,
}

pub(super) fn corrupt(path: &Path, offset: u64, reason: impl Into<String>) -> Error {
    Error::CorruptLog { path: path.to_path_buf(), offset, reason: reason.into() }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct RecordHeader {
    metadata: LogMetadata,
    payload_size: usize,
    retired: bool,
    sealed: bool,
}

impl RecordHeader {
    fn end(&self) -> u64 {
        self.metadata.offset.0 as u64 + HEADER_BYTES + self.payload_size as u64 + TRAILER_BYTES
    }

    fn require_live(&self) -> Result<()> {
        if self.retired {
            return Err(Error::UnavailableLogRange { requested: self.metadata.offset.0, first_retained: self.end() as i64 });
        }
        Ok(())
    }

    pub(super) fn replay_head(&self) -> ReplayHead {
        ReplayHead {
            metadata: self.metadata,
            payload_len: self.payload_size - if self.sealed { LOG_SEAL_OVERHEAD } else { 0 },
            allocation_bound: self.payload_size.saturating_mul(if self.sealed { 2 } else { 1 })
                .saturating_add(LOG_RECORD_OVERHEAD_BYTES),
        }
    }
}

/// The heads of the frames this process last made durable, published before the durable tip moves over them,
/// so that a tail prepares a frame its own writer has just written without reading the file.
#[derive(Default)]
struct RecentFrames(parking_lot::Mutex<VecDeque<RecordHeader>>);

impl RecentFrames {
    fn publish(&self, frames: &[RecordHeader]) {
        let mut recent = self.0.lock();
        recent.extend(frames.iter().copied());
        let excess = recent.len().saturating_sub(RECENT_FRAMES);
        recent.drain(..excess);
    }

    fn get(&self, offset: u64) -> Option<RecordHeader> {
        let recent = self.0.lock();
        let index = recent.binary_search_by_key(&(offset as i64), |frame| frame.metadata.offset.0).ok()?;
        Some(recent[index])
    }
}

fn read_record_header(file: &mut JournalFile, path: &Path, maximum: usize) -> Result<RecordHeader> {
    let offset = file.stream_position().map_err(|error| Error::io(path, error))?;
    let length = file.records_len().map_err(|error| Error::io(path, error))?;
    parse_record_header(file, path, maximum, offset, length)
}

pub(super) fn read_record_header_at(file: &mut JournalFile, path: &Path, maximum: usize, offset: u64, length: u64) -> Result<RecordHeader> {
    file.seek(SeekFrom::Start(offset)).map_err(|error| Error::io(path, error))?;
    parse_record_header(file, path, maximum, offset, length)
}

fn parse_record_header(file: &mut JournalFile, path: &Path, maximum: usize, offset: u64, length: u64) -> Result<RecordHeader> {
    let mut header = [0; HEADER_BYTES as usize];
    file.read_exact(&mut header).map_err(|error| corrupt(path, offset, format!("incomplete record header: {error}")))?;
    let header = decode_record_header(&header, path, maximum, offset)?;
    if header.end() > length {
        return Err(corrupt(path, offset, format!("incomplete record ends at {}, file length {length}", header.end())));
    }
    Ok(header)
}

fn decode_record_header(header: &[u8; HEADER_BYTES as usize], path: &Path, maximum: usize, offset: u64) -> Result<RecordHeader> {
    if !is_record_separator(header[0]) { return Err(corrupt(path, offset, "invalid local log record separator")); }
    let size = i32::from_be_bytes(header[1..5].try_into().expect("fixed header size"));
    let sealed = matches!(header[0], SEALED_SEPARATOR | RETIRED_SEALED_SEPARATOR);
    if size < 0 || size as usize > maximum || (sealed && (size as usize) < LOG_SEAL_OVERHEAD) {
        return Err(corrupt(path, offset, format!("invalid payload size {size}, maximum {maximum}")));
    }
    offset.checked_add(HEADER_BYTES + size as u64 + TRAILER_BYTES)
        .ok_or_else(|| corrupt(path, offset, "record end overflows"))?;
    let timestamp = Timestamp(i64::from_be_bytes(header[5..13].try_into().expect("fixed header size")));
    Ok(RecordHeader { metadata: LogMetadata { offset: LogOffset(offset as i64), timestamp },
        payload_size: size as usize, retired: matches!(header[0], RETIRED_SEPARATOR | RETIRED_SEALED_SEPARATOR), sealed })
}

fn read_record_trailer(file: &mut JournalFile, path: &Path, offset: LogOffset) -> Result<()> {
    let mut trailer = [0; TRAILER_BYTES as usize];
    file.read_exact(&mut trailer).map_err(|error| Error::io(path, error))?;
    validate_record_trailer(trailer, path, offset)
}

fn validate_record_trailer(trailer: [u8; TRAILER_BYTES as usize], path: &Path, offset: LogOffset) -> Result<()> {
    if i64::from_be_bytes(trailer) != offset.0 {
        return Err(corrupt(path, offset.0 as u64, "record trailer does not name its start offset"));
    }
    Ok(())
}

pub(super) fn read_record(file: &mut JournalFile, path: &Path, maximum: usize, keys: Option<&ClassicObjectKeys>) -> Result<LogRecord> {
    let header = read_record_header(file, path, maximum)?;
    read_record_body(file, path, header, keys)
}

pub(super) fn read_record_body(file: &mut JournalFile, path: &Path, header: RecordHeader, keys: Option<&ClassicObjectKeys>) -> Result<LogRecord> {
    header.require_live()?;
    let mut payload = vec![0; header.payload_size];
    file.read_exact(&mut payload).map_err(|error| Error::io(path, error))?;
    read_record_trailer(file, path, header.metadata.offset)?;
    let payload = match (header.sealed, keys) {
        (true, Some(keys)) => keys.open_record(header.metadata.offset.0, header.metadata.timestamp.0, &payload)?,
        (false, None) => Bytes::from(payload),
        _ => return Err(corrupt(path, header.metadata.offset.0 as u64, "the log frame uses a different custody profile")),
    };
    Ok(LogRecord::new(header.metadata, payload))
}

pub(super) fn inspect_record(file: &mut JournalFile, path: &Path, maximum: usize) -> Result<LogMetadata> {
    let header = read_record_header(file, path, maximum)?;
    inspect_record_body(file, path, header)
}

pub(super) fn inspect_record_body(file: &mut JournalFile, path: &Path, header: RecordHeader) -> Result<LogMetadata> {
    if header.retired {
        file.seek(SeekFrom::Current(header.payload_size as i64)).map_err(|error| Error::io(path, error))?;
        read_record_trailer(file, path, header.metadata.offset)?;
        return Ok(header.metadata);
    }
    let mut remaining = header.payload_size;
    let mut scratch = [0; 8 * 1024];
    while remaining > 0 {
        let size = remaining.min(scratch.len());
        file.read_exact(&mut scratch[..size]).map_err(|error| Error::io(path, error))?;
        remaining -= size;
    }
    read_record_trailer(file, path, header.metadata.offset)?;
    Ok(header.metadata)
}

pub(crate) fn prefix_end(file: &mut JournalFile, path: &Path, maximum: usize, boundary: LogOffset, inclusive: bool) -> Result<u64> {
    let mut next = 0;
    let length = file.records_len().map_err(|error| Error::io(path, error))?;
    let boundary = u64::try_from(boundary.0).map_err(|_| corrupt(path, 0, "negative checkpoint log boundary"))?;
    while next <= boundary && next < length {
        let header = read_record_header_at(file, path, maximum, next, length)?;
        let end = next + HEADER_BYTES + header.payload_size as u64 + TRAILER_BYTES;
        file.seek(SeekFrom::Start(end - TRAILER_BYTES)).map_err(|error| Error::io(path, error))?;
        read_record_trailer(file, path, header.metadata.offset)?;
        if next == boundary { return Ok(if inclusive { end } else { next }); }
        next = end;
    }
    Err(corrupt(path, boundary, "the checkpoint boundary does not name a complete record"))
}

pub(crate) fn settle_prefix(file: &mut JournalFile, path: &Path, maximum: usize, end: u64, keys: Option<&ClassicObjectKeys>) -> Result<()> {
    let zeros = [0; 8192];
    let mut next = 0;
    while next < end {
        let header = read_record_header_at(file, path, maximum, next, end)?;
        if header.sealed != keys.is_some() {
            return Err(corrupt(path, next, "the log frame uses a different custody profile"));
        }
        if !header.retired {
            if let Some(keys) = keys {
                let mut sealed = vec![0; header.payload_size];
                file.read_exact(&mut sealed).map_err(|error| Error::io(path, error))?;
                read_record_trailer(file, path, header.metadata.offset)?;
                keys.retire_record(header.metadata.offset.0, header.metadata.timestamp.0, &sealed)?;
            }
            file.seek(SeekFrom::Start(next)).map_err(|error| Error::io(path, error))?;
            file.write_all(&[if header.sealed { RETIRED_SEALED_SEPARATOR } else { RETIRED_SEPARATOR }])
                .map_err(|error| Error::io(path, error))?;
            if header.sealed {
                // The frame identity survives until its retirement marker is durable.
                file.sync().map_err(|error| Error::io(path, error))?;
            }
        }
        file.seek(SeekFrom::Start(next + HEADER_BYTES)).map_err(|error| Error::io(path, error))?;
        let mut remaining = header.payload_size;
        while remaining > 0 {
            let count = remaining.min(zeros.len());
            file.write_all(&zeros[..count]).map_err(|error| Error::io(path, error))?;
            remaining -= count;
        }
        next += HEADER_BYTES + header.payload_size as u64 + TRAILER_BYTES;
    }
    file.sync().map_err(|error| Error::io(path, error))
}

fn inspect(file: &mut JournalFile, path: &Path, maximum: usize, sealed: bool) -> Result<LogOffset> {
    let length = file.records_len().map_err(|error| Error::io(path, error))?;
    if length >= 1 << 48 { return Err(corrupt(path, length, "local log exceeds the 48-bit offset space")); }
    file.seek(SeekFrom::Start(0)).map_err(|error| Error::io(path, error))?;
    let mut latest = LogOffset(-1);
    while file.stream_position().map_err(|error| Error::io(path, error))? < length {
        let header = read_record_header(file, path, maximum)?;
        if header.sealed != sealed {
            return Err(corrupt(path, header.metadata.offset.0 as u64, "the log frame uses a different custody profile"));
        }
        latest = inspect_record_body(file, path, header)?.offset;
    }
    Ok(latest)
}

pub(crate) fn physical_maximum(maximum: usize, sealed: bool) -> Result<usize> {
    maximum.checked_add(if sealed { LOG_SEAL_OVERHEAD } else { 0 })
        .filter(|size| *size <= i32::MAX as usize)
        .ok_or_else(|| Error::Task("local log payload and seal exceed i32 framing".into()))
}

impl LocalLog {
    /// Opens the exact LOG/REPLICA_LOG file selected by the caller, coordinating its syncs with no other log.
    /// The database owner runs the returned driver, cancels and joins it, then calls `close_and_wait` before runtime shutdown.
    pub fn open(path: impl AsRef<Path>, options: LocalLogOptions) -> Result<(Arc<Self>, LocalLogDriver)> {
        Self::open_with_sync(path, options, AppleSync::Full)
    }

    /// Opens the exact LOG/REPLICA_LOG file selected by the caller with an explicit Apple sync policy.
    pub fn open_with_sync(path: impl AsRef<Path>, options: LocalLogOptions, apple_sync: AppleSync) -> Result<(Arc<Self>, LocalLogDriver)> {
        Self::open_with_key_slots_and_sync(path, options, Arc::new(SuppliedTimestamp), IoRegion::new(), None, apple_sync)
    }

    /// Uses `timestamps` in the writer; enqueued timestamps remain available to that policy.
    /// The returned driver has the same run/cancel/join obligation as `open`.
    pub fn open_with_timestamp_source(path: impl AsRef<Path>, options: LocalLogOptions, timestamps: Arc<dyn AppendTimestampSource>) -> Result<(Arc<Self>, LocalLogDriver)> {
        Self::open_with_timestamp_source_in(path, options, timestamps, IoRegion::new())
    }

    /// Uses a caller-owned reader I/O region; the caller closes that region after consumers and the writer join.
    pub fn open_with_timestamp_source_in(path: impl AsRef<Path>, options: LocalLogOptions, timestamps: Arc<dyn AppendTimestampSource>, reads: Arc<IoRegion>) -> Result<(Arc<Self>, LocalLogDriver)> {
        Self::open_with_key_slots(path, options, timestamps, reads, None)
    }

    pub fn open_with_key_slots(path: impl AsRef<Path>, options: LocalLogOptions, timestamps: Arc<dyn AppendTimestampSource>,
        reads: Arc<IoRegion>, keys: Option<Arc<ClassicObjectKeys>>,
    ) -> Result<(Arc<Self>, LocalLogDriver)> {
        Self::open_with_key_slots_and_sync(path, options, timestamps, reads, keys, AppleSync::Full)
    }

    pub fn open_with_key_slots_and_sync(path: impl AsRef<Path>, options: LocalLogOptions, timestamps: Arc<dyn AppendTimestampSource>,
        reads: Arc<IoRegion>, keys: Option<Arc<ClassicObjectKeys>>, apple_sync: AppleSync,
    ) -> Result<(Arc<Self>, LocalLogDriver)> {
        Self::open_journal(path.as_ref(), options, timestamps, reads, keys, false, apple_sync)
    }

    /// Durably binds a native replica journal's lineage, preserving any existing record offsets.
    /// The returned driver has the same run/cancel/join obligation as [`Self::open`].
    pub fn open_replica(path: impl AsRef<Path>, options: LocalLogOptions, timestamps: Arc<dyn AppendTimestampSource>,
        reads: Arc<IoRegion>, keys: Option<Arc<ClassicObjectKeys>>,
    ) -> Result<(Arc<Self>, LocalLogDriver)> {
        Self::open_replica_with_sync(path, options, timestamps, reads, keys, AppleSync::Full)
    }

    pub fn open_replica_with_sync(path: impl AsRef<Path>, options: LocalLogOptions, timestamps: Arc<dyn AppendTimestampSource>,
        reads: Arc<IoRegion>, keys: Option<Arc<ClassicObjectKeys>>, apple_sync: AppleSync,
    ) -> Result<(Arc<Self>, LocalLogDriver)> {
        Self::open_journal(path.as_ref(), options, timestamps, reads, keys, true, apple_sync)
    }

    /// The durability policy used for local filesystem syncs on Apple platforms.
    pub fn apple_sync(&self) -> AppleSync { self.apple_sync }

    fn open_journal(path: &Path, options: LocalLogOptions, timestamps: Arc<dyn AppendTimestampSource>,
        reads: Arc<IoRegion>, keys: Option<Arc<ClassicObjectKeys>>, bind_lineage: bool, apple_sync: AppleSync,
    ) -> Result<(Arc<Self>, LocalLogDriver)> {
        let path = Arc::new(path.to_path_buf());
        let _operation = reads.admit_read(path.clone(), ())?;
        let file_path = path.as_path();
        if keys.is_none() {
            ClassicKeyVault::require_unsealed_log(file_path).map_err(|error| Error::Task(error.to_string()))?;
        }
        let maximum = physical_maximum(options.maximum_message_bytes, keys.is_some())?;
        if options.queue_capacity == 0 || options.maximum_message_bytes > i32::MAX as usize {
            return Err(Error::Task("local log requires a positive queue and an i32 payload limit".into()));
        }
        if let Some(parent) = file_path.parent() { fs::create_dir_all(parent).map_err(|error| Error::io(parent, error))?; }
        let mut file = JournalFile::lock(file_path, true, apple_sync)?;
        let mut cached = file.cached_lineage(file_path)?;
        let latest = inspect(&mut file, file_path, maximum, keys.is_some())?;
        if file.lineage().is_none() && latest == LogOffset(-1) && cached.take().is_some() {
            // A crash after publishing a fresh UUID would leave a mismatched sidecar and block reopening.
            file.sync_open(file_path)?;
            JournalFile::remove_lineage_sidecar(file_path, apple_sync)?;
        }
        if file.lineage().is_none() && (bind_lineage || cached.is_some()) {
            file = file.bind_lineage(file_path, cached.unwrap_or_else(Uuid::new_v4))?;
        } else {
            file.sync_open(file_path)?;
        }
        if cached.is_some() {
            JournalFile::remove_lineage_sidecar(file_path, apple_sync)?;
        }
        let lineage = file.lineage();
        let writer = Writer::new(file, path.clone(), timestamps, keys.clone())?;
        let progress = Arc::new(Progress::new(latest));
        let recent = Arc::new(RecentFrames::default());
        let (send, queue) = mpsc::channel(options.queue_capacity);
        let log = Arc::new(Self { path, lineage, queue: send, progress: progress.clone(),
            maximum_message_bytes: options.maximum_message_bytes, keys, reads, recent: recent.clone(), apple_sync });
        Ok((log, LocalLogDriver { writer, queue, progress, recent }))
    }

    /// Closes and drains reader I/O; the caller must cancel and join the writer driver first.
    /// When the region is shared, this closes admission for every store and log using it.
    pub async fn close_and_wait(&self) { self.reads.close_and_wait().await; }
}

impl Writer {
    fn new(mut file: JournalFile, path: Arc<PathBuf>, timestamps: Arc<dyn AppendTimestampSource>, keys: Option<Arc<ClassicObjectKeys>>) -> Result<Self> {
        let end = file.seek(SeekFrom::End(0)).map_err(|error| Error::io(path.as_ref(), error))?;
        Ok(Self { file, end, path, timestamps, keys })
    }

    fn append(&mut self, records: &[Queued]) -> Result<Vec<RecordHeader>> {
        let start = self.end;
        let result = (|| {
            let sealed = self.keys.is_some();
            let separator = if sealed { SEALED_SEPARATOR } else { RECORD_SEPARATOR };
            let mut frames = Vec::with_capacity(records.len());
            let mut heads = Vec::with_capacity(records.len());
            let mut offset = start;
            for record in records {
                let timestamp = self.timestamps.timestamp(&record.payload, record.timestamp)?;
                let payload = match &self.keys {
                    Some(keys) => keys.seal_record(offset as i64, timestamp.0, &record.payload)?,
                    None => record.payload.clone(),
                };
                let end = offset.checked_add(HEADER_BYTES + payload.len() as u64 + TRAILER_BYTES)
                    .filter(|end| *end < 1 << 48)
                    .ok_or_else(|| corrupt(&self.path, offset, "local log offset space exhausted"))?;
                heads.push(RecordHeader { metadata: LogMetadata { offset: LogOffset(offset as i64), timestamp },
                    payload_size: payload.len(), retired: false, sealed });
                frames.push((record_header(separator, payload.len(), timestamp), payload, (offset as i64).to_be_bytes()));
                offset = end;
            }
            let mut slices: Vec<_> = frames.iter().flat_map(|(header, payload, trailer)| {
                [IoSlice::new(header), IoSlice::new(payload), IoSlice::new(trailer)]
            }).collect();
            write_frames(&mut self.file, &mut slices).map_err(|error| Error::io(self.path.as_ref(), error))?;
            self.file.sync().map_err(|error| Error::io(self.path.as_ref(), error))?;
            self.end = offset;
            Ok(heads)
        })();
        if result.is_err() {
            let rollback = self.file.truncate_records(start)
                .and_then(|_| self.file.sync())
                .and_then(|_| self.file.seek(SeekFrom::Start(start)));
            if let Err(rollback) = rollback {
                return Err(corrupt(&self.path, start, format!("append failed: {}; rollback failed: {rollback}", result.as_ref().unwrap_err())));
            }
        }
        result
    }
}

fn record_header(separator: u8, payload_len: usize, timestamp: Timestamp) -> [u8; HEADER_BYTES as usize] {
    let mut header = [separator; HEADER_BYTES as usize];
    header[1..5].copy_from_slice(&(payload_len as i32).to_be_bytes());
    header[5..].copy_from_slice(&timestamp.0.to_be_bytes());
    header
}

fn write_frames(writer: &mut impl Write, mut remaining: &mut [IoSlice<'_>]) -> io::Result<()> {
    while !remaining.is_empty() {
        match writer.write_vectored(&remaining[..remaining.len().min(1024)]) {
            Ok(0) => return Err(io::Error::new(ErrorKind::WriteZero, "failed to write whole buffer")),
            Ok(written) => IoSlice::advance_slices(&mut remaining, written),
            Err(error) if error.kind() == ErrorKind::Interrupted => (),
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
fn write_record(writer: &mut impl Write, separator: u8, payload: &[u8], timestamp: Timestamp, offset: u64) -> io::Result<()> {
    let header = record_header(separator, payload.len(), timestamp);
    let trailer = (offset as i64).to_be_bytes();
    write_frames(writer, &mut [IoSlice::new(&header), IoSlice::new(payload), IoSlice::new(&trailer)])
}

#[cfg(test)]
#[test]
fn arrow_byte_path_vectored_record_keeps_the_payload_slice_and_wire_bytes_after_short_writes() {
    struct ShortWriter<'a> { payload: &'a [u8], output: Vec<u8>, calls: usize }
    impl Write for ShortWriter<'_> {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> { panic!("expected a vectored write") }
        fn flush(&mut self) -> io::Result<()> { Ok(()) }
        fn write_vectored(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize> {
            self.calls += 1;
            if self.calls == 1 {
                assert_eq!(slices[1].as_ptr(), self.payload.as_ptr());
                return Err(ErrorKind::Interrupted.into());
            }
            let prior = self.output.len();
            self.output.extend(slices.iter().flat_map(|slice| slice.iter().copied()).take(6));
            Ok(self.output.len() - prior)
        }
    }
    for payload in [b"".as_slice(), b"0123456789".as_slice()] {
        let mut writer = ShortWriter { payload, output: Vec::new(), calls: 0 };
        write_record(&mut writer, RECORD_SEPARATOR, payload, Timestamp(-3), 21).unwrap();
        let mut expected = vec![0x1e];
        expected.extend((payload.len() as i32).to_be_bytes());
        expected.extend((-3_i64).to_be_bytes());
        expected.extend(payload);
        expected.extend(21_i64.to_be_bytes());
        assert_eq!(writer.output, expected);
    }
}

impl LocalLogDriver {
    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let shutdown = shutdown.child_token();
        let _cancel_on_drop = shutdown.clone().drop_guard();
        let runtime = Handle::current();
        let progress = Arc::clone(&self.progress);
        let (finished, completion) = oneshot::channel();
        let worker = thread::Builder::new().name("rtdb-local-log".into()).spawn(move || {
            let result = runtime.block_on(self.write_batches(shutdown));
            let _ = finished.send(());
            result
        }).map_err(|error| {
            let reason = format!("could not start local log writer: {error}");
            progress.fail(reason.clone());
            Error::Task(reason)
        })?;
        let _ = completion.await;
        worker.join().map_err(|_| {
            let reason = "local log writer panicked".to_string();
            progress.fail(reason.clone());
            Error::Task(reason)
        })?
    }

    async fn write_batches(mut self, shutdown: CancellationToken) -> Result<()> {
        let mut next = None;
        loop {
            let first = tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                record = async { match next.take() { Some(record) => Some(record), None => self.queue.recv().await } } =>
                    match record { Some(record) => record, None => break },
            };
            let (records, deferred) = self.ready_batch(first);
            next = deferred;
            match self.writer.append(&records) {
                Ok(frames) => {
                    self.recent.publish(&frames);
                    if let Some(last) = frames.last() { self.progress.advance(last.metadata.offset); }
                    for (record, frame) in records.into_iter().zip(frames) { let _ = record.durable.send(Ok(frame.metadata)); }
                }
                Err(error) => {
                    let reason = error.to_string();
                    self.progress.fail(reason.clone());
                    for record in records { let _ = record.durable.send(Err(Error::Closed(reason.clone()))); }
                    self.reject_pending(next, reason);
                    return Err(error);
                }
            }
        }
        self.reject_pending(next, "local log shutdown before append".into());
        self.progress.close();
        Ok(())
    }

    fn ready_batch(&mut self, first: Queued) -> (Vec<Queued>, Option<Queued>) {
        let sealed = self.writer.keys.is_some();
        let mut bytes = first.frame_bytes(sealed);
        let mut records = vec![first];
        while records.len() < APPEND_BATCH_RECORDS && bytes < APPEND_BATCH_BYTES {
            let Ok(record) = self.queue.try_recv() else { break; };
            let size = record.frame_bytes(sealed);
            if size > APPEND_BATCH_BYTES - bytes {
                return (records, Some(record));
            }
            records.push(record);
            bytes += size;
        }
        (records, None)
    }

    fn reject_pending(&mut self, deferred: Option<Queued>, reason: String) {
        self.queue.close();
        if let Some(record) = deferred { let _ = record.durable.send(Err(Error::Closed(reason.clone()))); }
        while let Ok(record) = self.queue.try_recv() { let _ = record.durable.send(Err(Error::Closed(reason.clone()))); }
    }
}

impl Drop for LocalLogDriver {
    fn drop(&mut self) { self.progress.close(); }
}

impl LocalLog {
    async fn append_record(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        if payload.len() > self.maximum_message_bytes {
            return Err(Error::MessageTooLarge { size: payload.len(), maximum: self.maximum_message_bytes });
        }
        self.progress.latest()?;
        let (send, durable) = oneshot::channel();
        self.queue.send(Queued { payload, timestamp, durable: send }).await
            .map_err(|_| Error::Closed("local append queue ended".into()))?;
        Ok(AppendReceipt::new(durable))
    }

    async fn open_tail(&self, after: LogOffset) -> Result<LocalTail> {
        let maximum = physical_maximum(self.maximum_message_bytes, self.keys.is_some())?;
        let (path, progress) = (self.path.clone(), self.progress.clone());
        let lineage = self.lineage;
        let latest = progress.latest()?;
        if after.0 < -1 || after > latest {
            return Err(Error::UnavailableLogRange { requested: after.0, first_retained: 0 });
        }
        let held = if after.0 < 0 { None } else {
            Some(self.reads.read(path.clone(), after, move |path, &after| {
                let mut held = HeldJournal::at(None, path, lineage, after.0 as u64)?;
                let header = held.read_header(path, maximum)?;
                inspect_record_body(&mut held.file, path, header)?;
                held.position = header.end();
                Ok(held)
            }).await?)
        };
        let next = held.as_ref().map_or(0, |held| held.position);
        if let Some(keys) = &self.keys { keys.require_local_offset(next as i64)?; }
        Ok(LocalTail { path, lineage, progress, maximum, after, next, reads: Arc::clone(&self.reads), keys: self.keys.clone(), held,
            recent: Arc::clone(&self.recent) })
    }
}

#[async_trait]
impl super::SharedLog for LocalLog {
    async fn append(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        self.append_record(payload, timestamp).await
    }

    fn latest(&self) -> Result<LogOffset> { self.progress.latest() }

    async fn read_after(&self, after: LogOffset) -> Result<Box<dyn LogReplayTail>> {
        Ok(Box::new(self.open_tail(after).await?))
    }
}

#[async_trait]
impl Log for LocalLog {
    fn checkpoint_key_retirement(&self) -> Option<&dyn super::CheckpointKeyRetirement> {
        self.keys.as_ref().map(|_| self as &dyn super::CheckpointKeyRetirement)
    }
    fn journal_lineage(&self) -> Option<Uuid> { self.lineage }
    fn shared(&self) -> Option<&dyn super::SharedLog> { Some(self) }
    fn retains_synced_history(&self) -> bool { true }
    fn maximum_append_bytes(&self) -> Option<usize> { Some(self.maximum_message_bytes) }

    async fn range(&self, from: LogOffset, to: LogOffset) -> Result<Box<dyn super::LogRange>> {
        let maximum = physical_maximum(self.maximum_message_bytes, self.keys.is_some())?;
        super::read_only::file_range(self.reads.clone(), self.path.clone(), maximum, from, to, Some(self.progress.latest()?), self.keys.clone(), self.lineage)
    }
    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        self.append_record(payload, timestamp).await
    }

    fn latest(&self) -> Result<LogOffset> { super::SharedLog::latest(self) }

    async fn tail(&self, after: LogOffset) -> Result<Box<dyn LogTail>> {
        Ok(Box::new(self.open_tail(after).await?))
    }

    async fn read_range(&self, from: LogOffset, to: LogOffset) -> Result<Vec<LogRecord>> {
        let latest = self.progress.latest()?;
        let maximum = physical_maximum(self.maximum_message_bytes, self.keys.is_some())?;
        let keys = self.keys.clone();
        let lineage = self.lineage;
        if from.0 < 0 { return Err(Error::UnavailableLogRange { requested: from.0, first_retained: 0 }); }
        if to < from { return Err(Error::Task("log range end precedes its start".into())); }
        self.reads.read(self.path.clone(), (from, to, latest), move |path, &(from, to, latest)| {
            let mut file = open_at(path, lineage, 0)?;
            if from > latest {
                let end = if latest.0 < 0 { 0 } else {
                    file.seek(SeekFrom::Start(latest.0 as u64)).map_err(|error| Error::io(path.as_path(), error))?;
                    read_record(&mut file, path, maximum, keys.as_deref())?;
                    file.stream_position().map_err(|error| Error::io(path.as_path(), error))? as i64
                };
                if from.0 != end { return Err(Error::UnavailableLogRange { requested: from.0, first_retained: 0 }); }
                return Ok(Vec::new());
            }
            file.seek(SeekFrom::Start(from.0 as u64)).map_err(|error| Error::io(path.as_path(), error))?;
            let first = read_record(&mut file, path, maximum, keys.as_deref())?;
            let mut result = Vec::new();
            if from < to { result.push(first); }
            else { return Ok(result); }
            loop {
                let next = file.stream_position().map_err(|error| Error::io(path.as_path(), error))? as i64;
                if next >= to.0 || next > latest.0 { break; }
                result.push(read_record(&mut file, path, maximum, keys.as_deref())?);
            }
            Ok(result)
        }).await
    }
}

struct LocalTail {
    path: Arc<PathBuf>, lineage: Option<Uuid>, progress: Arc<Progress>, maximum: usize, after: LogOffset, next: u64,
    reads: Arc<IoRegion>,
    keys: Option<Arc<ClassicObjectKeys>>,
    held: Option<HeldJournal>,
    recent: Arc<RecentFrames>,
}

impl LocalTail {
    async fn poll_with_payload_limit(&mut self, timeout: Duration, payload_limit: usize) -> Result<Vec<LogRecord>> {
        let tip = await_tip(&self.progress, self.after, timeout).await?;
        if tip <= self.after { return Ok(Vec::new()); }
        let maximum = self.maximum;
        let keys = self.keys.clone();
        let payload_limit = payload_limit.max(maximum);
        let lineage = self.lineage;
        let held = self.held.take();
        let (records, held) = self.reads.read(self.path.clone(), (self.next, tip), move |path, &(next, tip)| {
            let mut held = HeldJournal::at(held, path, lineage, next)?;
            let length = held.file.records_len().map_err(|error| Error::io(path, error))?;
            let mut records = Vec::new();
            let mut payload_bytes = 0usize;
            while held.position <= tip.0 as u64 {
                if records.len() == MAXIMUM_POLL_RECORDS
                    || payload_bytes.checked_add(maximum).is_none_or(|bytes| bytes > payload_limit)
                {
                    break;
                }
                let header = parse_record_header(&mut held.file, path, maximum, held.position, length)?;
                let record = read_record_body(&mut held.file, path, header, keys.as_deref())?;
                held.position = header.end();
                payload_bytes += record.payload.len();
                records.push(record);
            }
            Ok((records, held))
        }).await?;
        let last = records.last().ok_or_else(|| corrupt(&self.path, self.next, "durable tip yielded no record"))?;
        self.after = last.metadata.offset;
        self.next = held.position;
        self.held = Some(held);
        Ok(records)
    }
}

#[async_trait]
impl LogTail for LocalTail {
    async fn poll(&mut self, timeout: Duration) -> Result<Vec<LogRecord>> {
        self.poll_with_payload_limit(timeout, DEFAULT_MAXIMUM_POLL_PAYLOAD_BYTES).await
    }

    fn replay(&mut self) -> Option<&mut dyn LogReplayTail> { Some(self) }
}

struct HeldJournal {
    file: JournalFile,
    position: u64,
}

impl HeldJournal {
    fn at(held: Option<Self>, path: &Path, lineage: Option<Uuid>, next: u64) -> Result<Self> {
        let Some(mut held) = held.filter(|held| held.file.is_at(path).unwrap_or(false)) else {
            return Ok(Self { file: open_at(path, lineage, next)?, position: next });
        };
        if held.position != next {
            held.file.seek(SeekFrom::Start(next)).map_err(|error| Error::io(path, error))?;
            held.position = next;
        }
        Ok(held)
    }

    fn read_header(&mut self, path: &Path, maximum: usize) -> Result<RecordHeader> {
        let length = self.file.records_len().map_err(|error| Error::io(path, error))?;
        let header = parse_record_header(&mut self.file, path, maximum, self.position, length)?;
        self.position += HEADER_BYTES;
        Ok(header)
    }
}

fn open_at(path: &Path, lineage: Option<Uuid>, next: u64) -> Result<JournalFile> {
    let mut file = JournalFile::open(path, lineage)?.ok_or_else(|| corrupt(path, next, "local log file disappeared"))?;
    file.seek(SeekFrom::Start(next)).map_err(|error| Error::io(path, error))?;
    Ok(file)
}

fn check_replay_head(path: &Path, header: RecordHeader, head: ReplayHead, reservation: &ReadReservation) -> Result<()> {
    if header.replay_head() != head {
        return Err(corrupt(path, header.metadata.offset.0 as u64, "log replay head changed before delivery"));
    }
    if reservation.length() < head.allocation_bound {
        return Err(Error::Task("log read reservation is too small".into()));
    }
    Ok(())
}

fn read_recent_plain(file: &mut JournalFile, path: &Path, maximum: usize, expected: RecordHeader) -> Result<LogRecord> {
    let offset = expected.metadata.offset.0 as u64;
    let payload = HEADER_BYTES as usize..HEADER_BYTES as usize + expected.payload_size;
    let mut bytes = vec![0; payload.end + TRAILER_BYTES as usize];
    file.read_exact(&mut bytes).map_err(|error| match error.kind() {
        ErrorKind::UnexpectedEof => corrupt(path, offset, "incomplete local log frame"),
        _ => Error::io(path, error),
    })?;
    let header = decode_record_header(bytes[..payload.start].try_into().expect("fixed header size"), path, maximum, offset)?;
    if header.sealed || header.replay_head() != expected.replay_head() {
        return Err(corrupt(path, offset, "log replay head changed before delivery"));
    }
    header.require_live()?;
    validate_record_trailer(bytes[payload.end..].try_into().expect("fixed trailer size"), path, header.metadata.offset)?;
    Ok(LogRecord::new(header.metadata, Bytes::from(bytes).slice(payload)))
}

#[async_trait]
impl LogReplayTail for LocalTail {
    async fn poll_head(&mut self, timeout: Duration) -> Result<Option<ReplayHead>> {
        let tip = await_tip(&self.progress, self.after, timeout).await?;
        if tip <= self.after { return Ok(None); }
        if self.next > tip.0 as u64 { return Err(corrupt(&self.path, self.next, "durable tip yielded no record")); }
        if let Some(frame) = self.recent.get(self.next) { return Ok(Some(frame.replay_head())); }

        let maximum = self.maximum;
        let lineage = self.lineage;
        let held = self.held.take();
        let (head, held) = self.reads.read(self.path.clone(), self.next, move |path, &next| {
            let mut held = HeldJournal::at(held, path, lineage, next)?;
            Ok((held.read_header(path, maximum)?.replay_head(), held))
        }).await?;
        self.held = Some(held);
        Ok(Some(head))
    }

    async fn read_head(&mut self, head: ReplayHead, reservation: ReadReservation) -> Result<LogRecord> {
        let tip = self.progress.latest()?;
        if head.metadata.offset.0 != self.next as i64 {
            return Err(Error::Task("log replay head does not match the next position".into()));
        }
        if head.metadata.offset > tip {
            return Err(Error::Task("log replay head exceeds the durable tip".into()));
        }
        let maximum = self.maximum;
        let lineage = self.lineage;
        let held = self.held.take();
        let recent_plain = self.recent.get(self.next)
            .filter(|frame| self.keys.is_none() && !frame.sealed && frame.payload_size <= INLINE_READ_BYTES);
        let (record, held) = if let Some(frame) = recent_plain {
            self.reads.read_inline(|| {
                check_replay_head(&self.path, frame, head, &reservation)?;
                let mut held = HeldJournal::at(held, &self.path, lineage, self.next)?;
                let record = read_recent_plain(&mut held.file, &self.path, maximum, frame)?.retain(reservation)?;
                held.position = frame.end();
                Ok((record, held))
            })?
        } else {
            let keys = self.keys.clone();
            self.reads.read(self.path.clone(), self.next, move |path, &next| {
                let mut held = HeldJournal::at(held, path, lineage, next)?;
                let header = held.read_header(path, maximum)?;
                check_replay_head(path, header, head, &reservation)?;
                let record = read_record_body(&mut held.file, path, header, keys.as_deref())?.retain(reservation)?;
                held.position = header.end();
                Ok((record, held))
            }).await?
        };
        self.after = record.metadata.offset;
        self.next = held.position;
        self.held = Some(held);
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::ReadOnlyLocalLog;
    use std::fs::OpenOptions;

    fn replica_options() -> LocalLogOptions { LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 2 } }

    fn open_replica(path: &Path) -> Result<(Arc<LocalLog>, LocalLogDriver)> {
        LocalLog::open_replica(path, replica_options(), Arc::new(SuppliedTimestamp), IoRegion::new(), None)
    }

    #[test]
    fn replica_journal_lineage_accepts_an_empty_legacy_journal_with_a_sidecar() -> Result<()> {
        for logical_search in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("REPLICA_LOG");
            let sidecar = path.with_extension("lineage");
            let cached = Uuid::new_v4();
            fs::write(&path, []).unwrap();
            fs::write(&sidecar, cached.as_bytes()).unwrap();

            let reader = ReadOnlyLocalLog::open(&path, Default::default())?;
            assert_eq!(reader.latest()?, LogOffset(-1));
            assert_eq!(reader.journal_lineage(), None);
            let locked = JournalFile::lock(&path, false, AppleSync::Full)?;
            assert_eq!(locked.cached_lineage(&path)?, Some(cached));
            drop(locked);

            let (writer, _driver) = if logical_search { open_replica(&path)? } else { LocalLog::open(&path, replica_options())? };
            assert_eq!(writer.latest()?, LogOffset(-1));
            assert_eq!(writer.journal_lineage().is_some(), logical_search);
            assert_ne!(writer.journal_lineage(), Some(cached));
            assert!(!sidecar.exists());
        }
        Ok(())
    }

    #[test]
    fn replica_journal_lineage_rejects_a_mismatched_sidecar_and_finishes_interrupted_cleanup() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("REPLICA_LOG");
        let sidecar = path.with_extension("lineage");
        let (log, driver) = open_replica(&path)?;
        let lineage = log.journal_lineage().unwrap();
        drop((log, driver));

        fs::write(&sidecar, Uuid::from_u128(1).as_bytes()).unwrap();
        let mismatch = |error| matches!(error, Error::CorruptLog { reason, .. } if reason.contains("lineage disagrees"));
        assert!(open_replica(&path).is_err_and(mismatch));
        assert!(ReadOnlyLocalLog::open(&path, Default::default()).is_err_and(mismatch));

        fs::write(&sidecar, lineage.as_bytes()).unwrap();
        let reader = ReadOnlyLocalLog::open(&path, Default::default())?;
        assert_eq!(reader.journal_lineage(), Some(lineage));
        assert!(sidecar.exists());
        let (reopened, _driver) = LocalLog::open(&path, replica_options())?;
        assert_eq!(reopened.journal_lineage(), Some(lineage));
        assert!(!sidecar.exists());
        Ok(())
    }

    #[tokio::test]
    async fn replica_journal_lineage_migrates_legacy_offsets_and_keeps_the_identity_after_retirement() -> Result<()> {
        for with_sidecar in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("REPLICA_LOG");
            let sidecar = path.with_extension("lineage");
            let (legacy, driver) = LocalLog::open(&path, replica_options())?;
            let shutdown = CancellationToken::new();
            let task = tokio::spawn(driver.run(shutdown.clone()));
            let first = legacy.enqueue(Bytes::from_static(b"legacy"), Timestamp(1)).await?.durable().await?;
            #[cfg(unix)]
            let mut old_range = legacy.range(first.offset, LogOffset(i64::MAX)).await?;
            shutdown.cancel();
            task.await.unwrap()?;
            let original = fs::read(&path).unwrap();
            assert!(legacy.journal_lineage().is_none());

            let cached = Uuid::new_v4();
            if with_sidecar { fs::write(&sidecar, cached.as_bytes()).unwrap(); }
            let legacy_reader = ReadOnlyLocalLog::open(&path, Default::default())?;
            assert_eq!(legacy_reader.journal_lineage(), None);
            assert_eq!(legacy_reader.latest()?, first.offset);
            let interrupted = path.with_file_name(".REPLICA_LOG.rtdb-lineage-migration");
            fs::write(&interrupted, b"interrupted staging contents").unwrap();
            let (migrated, driver) = open_replica(&path)?;
            let lineage = migrated.journal_lineage().unwrap();
            assert!(!interrupted.exists());
            if with_sidecar { assert_eq!(lineage, cached); }
            assert_eq!(migrated.latest()?, first.offset);
            assert!(fs::read(&path).unwrap().ends_with(&original));
            assert!(!sidecar.exists());

            let shutdown = CancellationToken::new();
            let task = tokio::spawn(driver.run(shutdown.clone()));
            let second = migrated.enqueue(Bytes::from_static(b"after migration"), Timestamp(2)).await?.durable().await?;
            assert_eq!(second.offset, LogOffset(original.len() as i64));
            assert_eq!(legacy_reader.latest()?, second.offset);
            assert_eq!(legacy_reader.journal_lineage(), None);
            let mut tail = migrated.tail(LogOffset(-1)).await?;
            let records = tail.poll(Duration::ZERO).await?;
            assert_eq!(records.iter().map(|record| record.metadata).collect::<Vec<_>>(), [first, second]);

            let reader = ReadOnlyLocalLog::open_in(&path, Default::default(), IoRegion::new())?;
            assert_eq!(reader.journal_lineage(), Some(lineage));
            assert_eq!(reader.read_range(first.offset, second.offset).await?[0].payload.as_ref(), b"legacy");
            let shared = reader.shared().expect("coordinated read-only local log");
            let mut cursor = shared.read_after(first.offset).await?;
            let head = cursor.poll_head(Duration::ZERO).await?.unwrap();
            let delivery = cursor.read_head(head, ReadReservation::new(head.allocation_bound, ())).await?;
            assert_eq!(delivery.metadata, second);
            assert_eq!(delivery.payload.as_ref(), b"after migration");
            let mut range = migrated.range(first.offset, second.offset).await?;
            let charge = range.allocation_bound();
            let memory = rtdb_types::MemoryBudget::new(charge);
            let reservation = ReadReservation::new(charge, memory.reserve(charge).unwrap());
            assert_eq!(range.next(reservation).await?.unwrap().metadata, first);
            drop(range);
            let mut replay = crate::log::SharedLog::read_after(migrated.as_ref(), first.offset).await?;
            let head = replay.poll_head(Duration::ZERO).await?.unwrap();
            let reservation = ReadReservation::new(head.allocation_bound, memory.reserve(head.allocation_bound).unwrap());
            assert_eq!(replay.read_head(head, reservation).await?.metadata, second);
            #[cfg(unix)]
            {
                let reservation = ReadReservation::new(charge, memory.reserve(charge).unwrap());
                assert!(matches!(old_range.next(reservation).await, Err(Error::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::InvalidData));
            }
            shutdown.cancel();
            task.await.unwrap()?;

            let mut file = JournalFile::lock(&path, false, AppleSync::Full)?;
            let end = prefix_end(&mut file, &path, 1024, second.offset, false)?;
            settle_prefix(&mut file, &path, 1024, end, None)?;
            drop(file);
            let (reopened, driver) = open_replica(&path)?;
            assert_eq!(reopened.journal_lineage(), Some(lineage));
            assert_eq!(reopened.latest()?, second.offset);
            let mut tail = reader.tail(first.offset).await?;
            let replay = tail.replay().unwrap();
            let head = replay.poll_head(Duration::ZERO).await?.unwrap();
            let reservation = ReadReservation::new(head.allocation_bound, memory.reserve(head.allocation_bound).unwrap());
            assert_eq!(replay.read_head(head, reservation).await?.metadata, second);
            drop((reopened, driver));

            fs::remove_file(&path).unwrap();
            fs::write(&path, &original).unwrap();
            let (replacement, _driver) = open_replica(&path)?;
            assert_eq!(replacement.latest()?, first.offset);
            assert_ne!(replacement.journal_lineage().unwrap(), lineage);
        }
        Ok(())
    }

    #[test]
    fn sealing_byte_path_writes_original_record_bytes_after_short_writes() {
        struct ShortWriter<'a> {
            payload: &'a [u8],
            output: Vec<u8>,
            interrupted: bool,
        }

        impl Write for ShortWriter<'_> {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                panic!("expected a vectored write")
            }
            fn flush(&mut self) -> io::Result<()> { Ok(()) }
            fn write_vectored(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize> {
                if !self.interrupted {
                    assert_eq!(slices[1].as_ptr(), self.payload.as_ptr());
                    self.interrupted = true;
                    return Err(ErrorKind::Interrupted.into());
                }
                let prior = self.output.len();
                self.output.extend(slices.iter().flat_map(|slice| slice.iter().copied()).take(6));
                Ok(self.output.len() - prior)
            }
        }

        let sealed = [0x32; LOG_SEAL_OVERHEAD + 17];
        for (separator, payload) in [(0x1c, sealed.as_slice()), (0x1e, b"")] {
            let mut writer = ShortWriter { payload, output: Vec::new(), interrupted: false };
            write_record(&mut writer, separator, payload, Timestamp(-3), 21).unwrap();
            let mut expected = vec![separator];
            expected.extend((payload.len() as i32).to_be_bytes());
            expected.extend((-3_i64).to_be_bytes());
            expected.extend(payload);
            expected.extend(21_i64.to_be_bytes());
            assert_eq!(writer.output, expected);
        }
    }

    #[tokio::test]
    async fn sealing_byte_path_replays_local_frames_with_the_original_lineage() -> rtdb_types::Result<()> {
        let keys = tempfile::tempdir().unwrap();
        let files = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(keys.path(), fs::Permissions::from_mode(0o700)).unwrap();
        }
        let vault = ClassicKeyVault::open(keys.path())?;
        let path = files.path().join("LOG");
        let io = IoRegion::new();
        let slots = vault.log_keys(&path, true, Arc::clone(&io))?;
        let (log, driver) = LocalLog::open_with_key_slots(
            &path, LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 2 },
            Arc::new(SuppliedTimestamp), Arc::clone(&io), Some(slots),
        )?;
        let shutdown = CancellationToken::new();
        let writer = tokio::spawn(driver.run(shutdown.clone()));
        let body = Bytes::from_static(b"sealed application payload");
        let chosen = log.enqueue(body.clone(), Timestamp(7)).await?.durable().await?;
        let raw = fs::read(&path).unwrap();
        assert!(!raw.windows(body.len()).any(|window| window == body.as_ref()));
        let length = body.len() + LOG_SEAL_OVERHEAD;
        assert_eq!(raw[0], 0x1c);
        assert_eq!(&raw[1..5], &(length as i32).to_be_bytes());
        assert_eq!(&raw[5..13], &7_i64.to_be_bytes());
        assert_eq!(&raw[13 + length..], &0_i64.to_be_bytes());
        let slots = vault.log_keys(&path, false, Arc::clone(&io))?;
        let reader = super::super::ReadOnlyLocalLog::open_with_key_slots(&path, Default::default(), io, slots)?;
        let records = reader.read_range(chosen.offset, LogOffset(chosen.offset.0 + 1)).await?;
        assert_eq!(records[0].payload, body);
        shutdown.cancel();
        writer.await.unwrap()?;
        Ok(())
    }

    #[tokio::test]
    async fn local_append_batch_defers_the_first_frame_that_exceeds_its_byte_budget() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let (log, mut driver) = LocalLog::open(directory.path().join("LOG"), LocalLogOptions {
            maximum_message_bytes: APPEND_BATCH_BYTES,
            queue_capacity: 3,
        })?;
        let size = APPEND_BATCH_BYTES / 2;
        let _first = log.enqueue(Bytes::from(vec![1; size]), Timestamp(1)).await?;
        let _second = log.enqueue(Bytes::from(vec![2; size]), Timestamp(2)).await?;
        let _third = log.enqueue(Bytes::from_static(b"last"), Timestamp(3)).await?;
        let first_record = driver.queue.try_recv().unwrap();
        let (batch, deferred) = driver.ready_batch(first_record);
        assert_eq!(batch.len(), 1);
        assert_eq!(deferred.unwrap().timestamp, Timestamp(2));
        assert_eq!(driver.queue.try_recv().unwrap().timestamp, Timestamp(3));
        Ok(())
    }

    #[tokio::test]
    async fn local_append_batches_take_at_most_ten_ready_frames_in_fifo_order() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let (log, mut driver) = LocalLog::open(directory.path().join("LOG"), LocalLogOptions {
            maximum_message_bytes: 1024,
            queue_capacity: 16,
        })?;
        for time in 0..16 {
            let _ = log.enqueue(Bytes::from_static(b"frame"), Timestamp(time)).await?;
        }
        let first = driver.queue.try_recv().unwrap();
        let (ready, deferred) = driver.ready_batch(first);
        assert!(deferred.is_none());
        assert_eq!(ready.iter().map(|record| record.timestamp.0).collect::<Vec<_>>(), (0..10).collect::<Vec<_>>());

        let first = driver.queue.try_recv().unwrap();
        let (remaining, deferred) = driver.ready_batch(first);
        assert!(deferred.is_none());
        assert_eq!(remaining.iter().map(|record| record.timestamp.0).collect::<Vec<_>>(), (10..16).collect::<Vec<_>>());
        Ok(())
    }

    #[tokio::test]
    async fn a_batch_takes_only_the_frames_already_queued_and_does_not_wait_for_more() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let (log, mut driver) = LocalLog::open(directory.path().join("LOG"), LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 16 })?;
        let _first = log.enqueue(Bytes::from_static(b"first"), Timestamp(1)).await?;
        let first = driver.queue.try_recv().unwrap();
        let (batch, deferred) = driver.ready_batch(first);
        assert_eq!(batch.len(), 1);
        assert!(deferred.is_none());
        Ok(())
    }

    #[test]
    fn local_append_flushes_with_the_runtime_blocking_pool_busy() -> Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1).enable_all().build().unwrap();
        runtime.block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("LOG");
            let (log, driver) = LocalLog::open(&path, replica_options())?;
            let (release, blocked) = std::sync::mpsc::channel::<()>();
            let (started, running) = oneshot::channel();
            let worker = tokio::task::spawn_blocking(move || {
                let _ = started.send(());
                let _ = blocked.recv();
            });
            running.await.unwrap();

            let receipt = log.enqueue(Bytes::from_static(b"frame"), Timestamp(5)).await?;
            assert_eq!(log.latest()?, LogOffset(-1));
            let shutdown = CancellationToken::new();
            let writer = tokio::spawn(driver.run(shutdown.clone()));
            let metadata = tokio::time::timeout(Duration::from_secs(5), receipt.durable()).await.unwrap()?;
            assert_eq!(log.latest()?, metadata.offset);
            shutdown.cancel();
            writer.await.unwrap()?;

            drop(release);
            worker.await.unwrap();
            let (reopened, _driver) = LocalLog::open(&path, replica_options())?;
            assert_eq!(reopened.read_range(LogOffset(0), LogOffset(i64::MAX)).await?[0].metadata, metadata);
            Ok(())
        })
    }

    #[tokio::test]
    async fn a_failed_ready_batch_keeps_the_previous_durable_prefix_and_reuses_its_end_on_reopen() -> Result<()> {
        struct RejectBadPayload;

        impl AppendTimestampSource for RejectBadPayload {
            fn timestamp(&self, payload: &[u8], supplied: Timestamp) -> Result<Timestamp> {
                if payload == b"bad" {
                    return Err(Error::Task("timestamp unavailable".into()));
                }
                Ok(supplied)
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("LOG");
        let mut prefix = Vec::new();
        write_record(&mut prefix, RECORD_SEPARATOR, b"committed", Timestamp(1), 0).unwrap();
        fs::write(&path, &prefix).unwrap();
        let (log, driver) = LocalLog::open_with_timestamp_source(&path, replica_options(), Arc::new(RejectBadPayload))?;
        let good = log.enqueue(Bytes::from_static(b"good"), Timestamp(2)).await?;
        let bad = log.enqueue(Bytes::from_static(b"bad"), Timestamp(3)).await?;
        assert_eq!(log.latest()?, LogOffset(0));
        assert!(matches!(driver.run(CancellationToken::new()).await, Err(Error::Task(reason)) if reason == "timestamp unavailable"));
        assert!(matches!(good.durable().await, Err(Error::Closed(_))));
        assert!(matches!(bad.durable().await, Err(Error::Closed(_))));
        assert!(matches!(log.latest(), Err(Error::Closed(_))));
        assert_eq!(fs::read(&path).unwrap(), prefix);

        let (reopened, driver) = LocalLog::open(&path, replica_options())?;
        let shutdown = CancellationToken::new();
        let writer = tokio::spawn(driver.run(shutdown.clone()));
        let receipt = reopened.enqueue(Bytes::from_static(b"next"), Timestamp(4)).await?;
        assert_eq!(receipt.durable().await?.offset, LogOffset(prefix.len() as i64));
        shutdown.cancel();
        writer.await.unwrap()?;
        Ok(())
    }

    #[tokio::test]
    async fn local_append_batch_reopen_reads_a_complete_prefix_and_rejects_a_torn_frame() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("LOG");
        let options = || LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 3 };
        let (_, mut driver) = LocalLog::open(&path, options())?;
        let writer = &mut driver.writer;
        let record = |payload: &'static [u8], time| Queued {
            payload: Bytes::from_static(payload), timestamp: Timestamp(time), durable: oneshot::channel().0,
        };
        let committed = writer.append(&[record(b"acknowledged", 1)])?[0].metadata;
        let metadata: Vec<_> = writer.append(&[record(b"complete", 2), record(b"torn", 3), record(b"absent", 4)])?
            .into_iter().map(|frame| frame.metadata).collect();
        writer.file.truncate_records(metadata[1].offset.0 as u64 + HEADER_BYTES + 2)
            .map_err(|error| Error::io(&path, error))?;
        drop(driver);
        assert!(matches!(LocalLog::open(&path, options()), Err(Error::CorruptLog { offset, .. })
            if offset == metadata[1].offset.0 as u64));

        let file = OpenOptions::new().write(true).open(&path).map_err(|error| Error::io(&path, error))?;
        file.set_len(metadata[1].offset.0 as u64).map_err(|error| Error::io(&path, error))?;
        drop(file);
        let (reopened, _driver) = LocalLog::open(&path, options())?;
        assert_eq!(reopened.latest()?, metadata[0].offset);
        let records = reopened.read_range(committed.offset, metadata[1].offset).await?;
        assert_eq!(records.iter().map(|record| record.payload.as_ref()).collect::<Vec<_>>(),
            [b"acknowledged".as_slice(), b"complete".as_slice()]);
        Ok(())
    }

    async fn replay_heads(log: &LocalLog, count: usize) -> Result<Vec<ReplayHead>> {
        let mut tail = log.tail(LogOffset(-1)).await?;
        let replay = tail.replay().unwrap();
        let mut heads = Vec::new();
        for _ in 0..count {
            let head = replay.poll_head(Duration::ZERO).await?.unwrap();
            heads.push(head);
            replay.read_head(head, ReadReservation::new(head.allocation_bound, ())).await?;
        }
        assert!(replay.poll_head(Duration::ZERO).await?.is_none());
        Ok(heads)
    }

    #[tokio::test]
    async fn a_tail_prepares_frames_its_writer_just_made_durable_without_reader_io_and_as_the_journal_records_them() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("LOG");
        let options = || LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 4 };
        let (log, driver) = LocalLog::open(&path, options())?;
        let shutdown = CancellationToken::new();
        let writer = tokio::spawn(driver.run(shutdown.clone()));
        let payloads: [&'static [u8]; 3] = [b"first", b"", b"third frame"];
        let mut written = Vec::new();
        for (time, payload) in payloads.iter().enumerate() {
            written.push(log.enqueue(Bytes::from_static(payload), Timestamp(time as i64)).await?.durable().await?);
        }
        let prepared = replay_heads(&log, payloads.len()).await?;
        assert_eq!(prepared.iter().map(|head| head.metadata).collect::<Vec<_>>(), written);
        assert_eq!(prepared.iter().map(|head| head.payload_len).collect::<Vec<_>>(),
            payloads.iter().map(|payload| payload.len()).collect::<Vec<_>>());

        let mut tail = log.tail(LogOffset(-1)).await?;
        log.reads.close();
        let replay = tail.replay().unwrap();
        let head = replay.poll_head(Duration::ZERO).await?.unwrap();
        assert_eq!(head, prepared[0]);
        assert!(matches!(replay.read_head(head, ReadReservation::new(head.allocation_bound, ())).await, Err(Error::Closed(_))));
        shutdown.cancel();
        writer.await.unwrap()?;
        drop((tail, log));

        let (reopened, _driver) = LocalLog::open(&path, options())?;
        assert_eq!(replay_heads(&reopened, payloads.len()).await?, prepared);
        Ok(())
    }

    async fn replay_payloads(log: &LocalLog, count: usize) -> Result<Vec<(LogMetadata, Bytes)>> {
        let mut tail = log.tail(LogOffset(-1)).await?;
        let replay = tail.replay().unwrap();
        let mut records = Vec::new();
        for _ in 0..count {
            let head = replay.poll_head(Duration::ZERO).await?.unwrap();
            let short = ReadReservation::new(head.allocation_bound - 1, ());
            assert!(matches!(replay.read_head(head, short).await, Err(Error::Task(_))));
            let record = replay.read_head(head, ReadReservation::new(head.allocation_bound, ())).await?;
            records.push((record.metadata, record.payload));
        }
        assert!(replay.poll_head(Duration::ZERO).await?.is_none());
        Ok(records)
    }

    #[tokio::test]
    async fn a_tail_delivers_just_written_frames_of_any_size_as_it_delivers_them_from_a_reopened_journal() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("LOG");
        let options = || LocalLogOptions { maximum_message_bytes: 4 * INLINE_READ_BYTES, queue_capacity: 4 };
        let (log, driver) = LocalLog::open(&path, options())?;
        let shutdown = CancellationToken::new();
        let writer = tokio::spawn(driver.run(shutdown.clone()));
        let payloads = [Bytes::from_static(b"small"), Bytes::from(vec![7; INLINE_READ_BYTES + 1]), Bytes::new()];
        let mut written = Vec::new();
        for (time, payload) in payloads.iter().enumerate() {
            let metadata = log.enqueue(payload.clone(), Timestamp(time as i64)).await?.durable().await?;
            written.push((metadata, payload.clone()));
        }
        assert_eq!(replay_payloads(&log, payloads.len()).await?, written);
        shutdown.cancel();
        writer.await.unwrap()?;
        drop(log);

        let (reopened, _driver) = LocalLog::open(&path, options())?;
        assert_eq!(replay_payloads(&reopened, payloads.len()).await?, written);
        Ok(())
    }

    #[tokio::test]
    async fn a_frame_prepared_from_its_writer_and_retired_before_delivery_reports_the_retired_range() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("LOG");
        let (log, mut driver) = LocalLog::open(&path, LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 2 })?;
        let writer = &mut driver.writer;
        let record = |payload: &'static [u8]| Queued {
            payload: Bytes::from_static(payload), timestamp: Timestamp(5), durable: oneshot::channel().0,
        };
        let frames = writer.append(&[record(b"old receipt"), record(b"surviving suffix")])?;
        driver.recent.publish(&frames);
        driver.progress.advance(frames[1].metadata.offset);

        let mut tail = log.tail(LogOffset(-1)).await?;
        let replay = tail.replay().unwrap();
        let head = replay.poll_head(Duration::ZERO).await?.unwrap();
        assert_eq!(head.metadata, frames[0].metadata);
        let end = prefix_end(&mut writer.file, &path, 1024, frames[1].metadata.offset, false)?;
        settle_prefix(&mut writer.file, &path, 1024, end, None)?;
        assert!(matches!(replay.read_head(head, ReadReservation::new(head.allocation_bound, ())).await,
            Err(Error::UnavailableLogRange { requested: 0, .. })));
        Ok(())
    }

    #[tokio::test]
    async fn recent_frame_delivery_checks_the_on_disk_header_and_retries_at_the_same_offset() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("LOG");
        let (log, driver) = LocalLog::open(&path, replica_options())?;
        let shutdown = CancellationToken::new();
        let writer = tokio::spawn(driver.run(shutdown.clone()));
        let payload = Bytes::from_static(b"recent frame");
        let metadata = log.enqueue(payload.clone(), Timestamp(5)).await?.durable().await?;
        let mut tail = log.tail(LogOffset(-1)).await?;
        let replay = tail.replay().unwrap();
        let head = replay.poll_head(Duration::ZERO).await?.unwrap();
        let reservation = || ReadReservation::new(head.allocation_bound, ());

        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(5)).unwrap();
        file.write_all(&6_i64.to_be_bytes()).unwrap();
        assert!(matches!(replay.read_head(head, reservation()).await, Err(Error::CorruptLog { offset: 0, .. })));
        assert_eq!(replay.poll_head(Duration::ZERO).await?, Some(head));

        file.seek(SeekFrom::Start(5)).unwrap();
        file.write_all(&metadata.timestamp.0.to_be_bytes()).unwrap();
        let record = replay.read_head(head, reservation()).await?;
        assert_eq!((record.metadata, record.payload), (metadata, payload));
        shutdown.cancel();
        writer.await.unwrap()?;
        Ok(())
    }

    #[tokio::test]
    async fn local_tail_checks_room_for_a_maximum_size_message_before_reading_the_next_payload() {
        let directory = tempfile::tempdir().unwrap();
        let (log, driver) = LocalLog::open(directory.path().join("LOG"), LocalLogOptions {
            maximum_message_bytes: 6,
            queue_capacity: 5,
        }).unwrap();
        let shutdown = CancellationToken::new();
        let writer = tokio::spawn(driver.run(shutdown.clone()));
        let payloads: [&'static [u8]; 5] = [b"ab", b"wxyz", b"x", b"uvwxyz", b"y"];
        let mut metadata = Vec::new();
        for (position, &payload) in payloads.iter().enumerate() {
            metadata.push(log.enqueue(Bytes::from_static(payload), Timestamp(position as i64)).await.unwrap().durable().await.unwrap());
        }
        let mut tail = LocalTail {
            path: Arc::clone(&log.path),
            lineage: log.lineage,
            progress: Arc::clone(&log.progress),
            maximum: log.maximum_message_bytes,
            after: LogOffset(-1),
            next: 0,
            reads: Arc::clone(&log.reads),
            keys: None,
            held: None,
            recent: Arc::clone(&log.recent),
        };
        for (limit, from, to) in [(8, 0, 2), (5, 2, 3), (5, 3, 4), (5, 4, 5)] {
            let batch = tail.poll_with_payload_limit(Duration::ZERO, limit).await.unwrap();
            let observed: Vec<_> = batch.iter().map(|record| record.payload.as_ref()).collect();
            assert_eq!(observed.as_slice(), &payloads[from..to]);
            assert!(batch.iter().zip(&metadata[from..to]).all(|(record, expected)| record.metadata == *expected));
        }
        assert!(tail.poll_with_payload_limit(Duration::ZERO, 5).await.unwrap().is_empty());
        shutdown.cancel();
        writer.await.unwrap().unwrap();
        log.close_and_wait().await;
    }

    #[test]
    fn plain_local_log_writes_xtdbs_exact_0x1e_framing_and_extensions_fail_0x1e_check() -> Result<()> {
        let scratch = tempfile::tempdir().map_err(|error| Error::Task(error.to_string()))?;
        let plain_path = Arc::new(scratch.path().join("LOG"));
        let mut writer = Writer::new(JournalFile::lock(&plain_path, true, AppleSync::Full)?,
            Arc::clone(&plain_path), Arc::new(SuppliedTimestamp), None)?;
        let payload = b"xtdb-record";
        let timestamp = Timestamp(1_700_000_000_000_000);
        let frames = writer.append(&[Queued {
            payload: Bytes::from_static(payload), timestamp, durable: oneshot::channel().0,
        }])?;
        assert_eq!(frames[0].metadata.offset, LogOffset(0));
        let mut expected = Vec::new();
        expected.push(RECORD_SEPARATOR);
        expected.extend_from_slice(&(payload.len() as i32).to_be_bytes());
        expected.extend_from_slice(&timestamp.0.to_be_bytes());
        expected.extend_from_slice(payload);
        expected.extend_from_slice(&0i64.to_be_bytes());
        assert_eq!(fs::read(plain_path.as_ref()).map_err(|error| Error::io(plain_path.as_ref(), error))?, expected);

        // Retired and sealed separators are rejected by XTDB's `check(sep == 0x1E)`.
        assert_ne!(RETIRED_SEPARATOR, RECORD_SEPARATOR);
        assert_ne!(SEALED_SEPARATOR, RECORD_SEPARATOR);
        assert_ne!(RETIRED_SEALED_SEPARATOR, RECORD_SEPARATOR);
        let end = prefix_end(&mut writer.file, &plain_path, 1024, LogOffset(0), true)?;
        settle_prefix(&mut writer.file, &plain_path, 1024, end, None)?;
        assert_eq!(fs::read(plain_path.as_ref()).map_err(|error| Error::io(plain_path.as_ref(), error))?[0], RETIRED_SEPARATOR);

        // Lineage-bound journals start with 0x1A (`\x1aRTDBJ01`), which XTDB's 0x1E check also rejects.
        let lineage_path = scratch.path().join("REPLICA_LOG");
        let (_log, driver) = open_replica(&lineage_path)?;
        drop(driver);
        assert_eq!(fs::read(&lineage_path).map_err(|error| Error::io(&lineage_path, error))?[0], 0x1a);
        assert_ne!(0x1a, RECORD_SEPARATOR);
        Ok(())
    }

    #[test]
    fn multi_partition_log_directory_is_refused_loudly() {
        let scratch = tempfile::tempdir().unwrap();
        let log_dir = scratch.path().join("LOG");
        fs::create_dir_all(&log_dir).unwrap();
        fs::write(log_dir.join("0"), []).unwrap();
        assert!(LocalLog::open(&log_dir, replica_options()).is_err());
        assert!(crate::log::ReadOnlyLocalLog::open(&log_dir, crate::log::ReadOnlyLocalLogOptions::default()).is_err());
    }
}

#[cfg(test)]
mod retention_tests {
    use super::*;

    #[test]
    fn sealed_prefix_resumes_after_key_withdrawal_before_frame_settlement() -> Result<()> {
        let payloads = tempfile::tempdir().map_err(|error| Error::Task(error.to_string()))?;
        use std::os::unix::fs::PermissionsExt;
        let vault = tempfile::Builder::new().permissions(fs::Permissions::from_mode(0o700))
            .tempdir().map_err(|error| Error::Task(error.to_string()))?;
        let key_vault = ClassicKeyVault::open(vault.path()).map_err(|error| Error::Task(error.to_string()))?;
        let path = Arc::new(payloads.path().join("REPLICA_LOG"));
        let keys = key_vault.log_keys(&path, true, IoRegion::new()).map_err(|error| Error::Task(error.to_string()))?;
        let mut writer = Writer::new(JournalFile::lock(&path, true, AppleSync::Full)?,
            Arc::clone(&path), Arc::new(SuppliedTimestamp), Some(Arc::clone(&keys)))?;
        let record = |payload: &'static [u8]| Queued {
            payload: Bytes::from_static(payload), timestamp: Timestamp(5), durable: oneshot::channel().0,
        };
        let records: Vec<_> = writer.append(&[record(b"old payload"), record(b"retained boundary")])?
            .into_iter().map(|frame| frame.metadata).collect();
        let maximum = physical_maximum(1024, true)?;
        let end = prefix_end(&mut writer.file, &path, maximum, records[1].offset, false)?;
        let header = read_record_header_at(&mut writer.file, &path, maximum, 0, end)?;
        let mut sealed = vec![0; header.payload_size];
        writer.file.read_exact(&mut sealed).map_err(|error| Error::io(path.as_ref(), error))?;
        keys.retire_record(0, records[0].timestamp.0, &sealed)?;
        assert!(matches!(keys.open_record(0, records[0].timestamp.0, &sealed), Err(Error::ErasedUnderView)));
        for _ in 0..2 {
            settle_prefix(&mut writer.file, &path, maximum, end, Some(&keys))?;
        }
        writer.file.seek(SeekFrom::Start(end)).map_err(|error| Error::io(path.as_ref(), error))?;
        assert_eq!(read_record(&mut writer.file, &path, maximum, Some(&keys))?.payload, Bytes::from_static(b"retained boundary"));
        Ok(())
    }

    #[test]
    fn settling_a_prefix_keeps_frame_offsets_and_excludes_its_payloads() -> Result<()> {
        let scratch = tempfile::NamedTempFile::new().map_err(|error| Error::Task(error.to_string()))?;
        let path = Arc::new(scratch.path().to_path_buf());
        let mut writer = Writer::new(JournalFile::lock(&path, false, AppleSync::Full)?,
            Arc::clone(&path), Arc::new(SuppliedTimestamp), None)?;
        let record = |payload: &'static [u8]| Queued {
            payload: Bytes::from_static(payload), timestamp: Timestamp(5), durable: oneshot::channel().0,
        };
        let records: Vec<_> = writer.append(&[record(b"old receipt"), record(b"surviving suffix")])?
            .into_iter().map(|frame| frame.metadata).collect();
        let before = records[1].offset;
        let end = prefix_end(&mut writer.file, &path, 1024, before, false)?;
        for _ in 0..2 { settle_prefix(&mut writer.file, &path, 1024, end, None)?; }

        let bytes = fs::read(path.as_ref()).map_err(|error| Error::io(path.as_ref(), error))?;
        assert!(bytes[HEADER_BYTES as usize..HEADER_BYTES as usize + b"old receipt".len()].iter().all(|byte| *byte == 0));
        assert_eq!(inspect(&mut writer.file, &path, 1024, false)?, before);
        writer.file.seek(SeekFrom::Start(0)).map_err(|error| Error::io(path.as_ref(), error))?;
        assert!(matches!(read_record(&mut writer.file, &path, 1024, None), Err(Error::UnavailableLogRange { requested: 0, .. })));
        writer.file.seek(SeekFrom::Start(before.0 as u64)).map_err(|error| Error::io(path.as_ref(), error))?;
        assert_eq!(read_record(&mut writer.file, &path, 1024, None)?.payload.as_ref(), b"surviving suffix");
        Ok(())
    }

}

#[cfg(test)]
mod held_journal_tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn a_tail_reads_through_its_open_journal_until_the_path_names_another() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("LOG");
        fs::write(&path, b"first").unwrap();
        let mut held = HeldJournal::at(None, &path, None, 1).unwrap();
        let mut text = String::new();
        held.file.read_to_string(&mut text).unwrap();
        held.position += text.len() as u64;
        assert_eq!(text, "irst");

        let replacement = directory.path().join("LOG.next");
        fs::write(&replacement, b"second").unwrap();
        fs::rename(&replacement, &path).unwrap();
        let mut reopened = HeldJournal::at(Some(held), &path, None, 0).unwrap();
        text.clear();
        reopened.file.read_to_string(&mut text).unwrap();
        reopened.position += text.len() as u64;
        assert_eq!(text, "second");

        fs::remove_file(&path).unwrap();
        assert!(HeldJournal::at(Some(reopened), &path, None, 0).is_err());
    }

    #[tokio::test]
    async fn local_logs_carry_independent_apple_sync_policies() -> Result<()> {
        let first_dir = tempfile::tempdir().map_err(|error| Error::Task(error.to_string()))?;
        let second_dir = tempfile::tempdir().map_err(|error| Error::Task(error.to_string()))?;
        let options = || LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 2 };
        let (full, full_driver) = LocalLog::open_with_sync(first_dir.path().join("LOG"), options(), AppleSync::Full)?;
        let (fsync, fsync_driver) = LocalLog::open_with_sync(second_dir.path().join("LOG"), options(), AppleSync::Fsync)?;
        assert_eq!(full.apple_sync(), AppleSync::Full);
        assert_eq!(fsync.apple_sync(), AppleSync::Fsync);
        let shutdown = CancellationToken::new();
        let full_task = tokio::spawn(full_driver.run(shutdown.clone()));
        let fsync_task = tokio::spawn(fsync_driver.run(shutdown.clone()));
        full.enqueue(Bytes::from_static(b"full"), Timestamp(1)).await?.durable().await?;
        fsync.enqueue(Bytes::from_static(b"fsync"), Timestamp(2)).await?.durable().await?;
        shutdown.cancel();
        full_task.await.map_err(|error| Error::Task(error.to_string()))??;
        fsync_task.await.map_err(|error| Error::Task(error.to_string()))??;
        Ok(())
    }
}
