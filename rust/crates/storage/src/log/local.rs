use super::{AppendReceipt, AppendTimestampSource, LOG_RECORD_OVERHEAD_BYTES, Log, LogMetadata, LogOffset, LogRecord, LogReplayTail, LogTail, Progress, ReplayHead, SuppliedTimestamp, await_tip};
use crate::{Error, IoRegion, Result, object_store::ReadReservation};
use async_trait::async_trait;
use bytes::Bytes;
use fs2::FileExt;
use rtdb_types::Timestamp;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const HEADER_BYTES: u64 = 13;
const TRAILER_BYTES: u64 = 8;
const RECORD_SEPARATOR: u8 = 0x1e;
const RETIRED_SEPARATOR: u8 = 0x1d;

pub(super) fn is_record_separator(byte: u8) -> bool {
    matches!(byte, RECORD_SEPARATOR | RETIRED_SEPARATOR)
}
const MAXIMUM_POLL_RECORDS: usize = 1024;
const DEFAULT_MAXIMUM_POLL_PAYLOAD_BYTES: usize = 128 * 1024 * 1024;

pub struct LocalLogOptions {
    pub maximum_message_bytes: usize,
    pub queue_capacity: usize,
}

struct Queued { payload: Bytes, timestamp: Timestamp, durable: oneshot::Sender<Result<LogMetadata>> }

pub struct LocalLog {
    path: Arc<PathBuf>,
    queue: mpsc::Sender<Queued>,
    progress: Arc<Progress>,
    maximum_message_bytes: usize,
    reads: Arc<IoRegion>,
}

pub struct LocalLogDriver {
    writer: Option<Writer>,
    queue: mpsc::Receiver<Queued>,
    progress: Arc<Progress>,
}

struct Writer { file: File, path: Arc<PathBuf>, timestamps: Arc<dyn AppendTimestampSource> }

pub(super) fn corrupt(path: &Path, offset: u64, reason: impl Into<String>) -> Error {
    Error::CorruptLog { path: path.to_path_buf(), offset, reason: reason.into() }
}

pub(super) struct RecordHeader {
    metadata: LogMetadata,
    payload_size: usize,
    retired: bool,
}

impl RecordHeader {
    pub(super) fn replay_head(&self) -> ReplayHead {
        ReplayHead {
            metadata: self.metadata,
            payload_len: self.payload_size,
            allocation_bound: self.payload_size.saturating_add(LOG_RECORD_OVERHEAD_BYTES),
        }
    }
}

fn read_record_header(file: &mut File, path: &Path, maximum: usize) -> Result<RecordHeader> {
    let offset = file.stream_position().map_err(|error| Error::io(path, error))?;
    let length = file.metadata().map_err(|error| Error::io(path, error))?.len();
    parse_record_header(file, path, maximum, offset, length)
}

pub(super) fn read_record_header_at(file: &mut File, path: &Path, maximum: usize, offset: u64, length: u64) -> Result<RecordHeader> {
    file.seek(SeekFrom::Start(offset)).map_err(|error| Error::io(path, error))?;
    parse_record_header(file, path, maximum, offset, length)
}

fn parse_record_header(file: &mut File, path: &Path, maximum: usize, offset: u64, length: u64) -> Result<RecordHeader> {
    let mut header = [0; HEADER_BYTES as usize];
    file.read_exact(&mut header).map_err(|error| corrupt(path, offset, format!("incomplete record header: {error}")))?;
    if !is_record_separator(header[0]) { return Err(corrupt(path, offset, "invalid local log record separator")); }
    let size = i32::from_be_bytes(header[1..5].try_into().expect("fixed header size"));
    if size < 0 || size as usize > maximum {
        return Err(corrupt(path, offset, format!("invalid payload size {size}, maximum {maximum}")));
    }
    let end = offset.checked_add(HEADER_BYTES + size as u64 + TRAILER_BYTES)
        .ok_or_else(|| corrupt(path, offset, "record end overflows"))?;
    if end > length { return Err(corrupt(path, offset, format!("incomplete record ends at {end}, file length {length}"))); }
    let timestamp = Timestamp(i64::from_be_bytes(header[5..13].try_into().expect("fixed header size")));
    Ok(RecordHeader { metadata: LogMetadata { offset: LogOffset(offset as i64), timestamp },
        payload_size: size as usize, retired: header[0] == RETIRED_SEPARATOR })
}

fn read_record_trailer(file: &mut File, path: &Path, offset: LogOffset) -> Result<()> {
    let mut trailer = [0; TRAILER_BYTES as usize];
    file.read_exact(&mut trailer).map_err(|error| Error::io(path, error))?;
    if i64::from_be_bytes(trailer) != offset.0 {
        return Err(corrupt(path, offset.0 as u64, "record trailer does not name its start offset"));
    }
    Ok(())
}

pub(super) fn read_record(file: &mut File, path: &Path, maximum: usize) -> Result<LogRecord> {
    let header = read_record_header(file, path, maximum)?;
    read_record_body(file, path, header)
}

pub(super) fn read_record_body(file: &mut File, path: &Path, header: RecordHeader) -> Result<LogRecord> {
    if header.retired {
        let requested = header.metadata.offset.0;
        return Err(Error::UnavailableLogRange {
            requested, first_retained: requested + HEADER_BYTES as i64 + header.payload_size as i64 + TRAILER_BYTES as i64,
        });
    }
    let mut payload = vec![0; header.payload_size];
    file.read_exact(&mut payload).map_err(|error| Error::io(path, error))?;
    read_record_trailer(file, path, header.metadata.offset)?;
    Ok(LogRecord::new(header.metadata, Bytes::from(payload)))
}

pub(super) fn inspect_record(file: &mut File, path: &Path, maximum: usize) -> Result<LogMetadata> {
    let header = read_record_header(file, path, maximum)?;
    inspect_record_body(file, path, header)
}

pub(super) fn inspect_record_body(file: &mut File, path: &Path, header: RecordHeader) -> Result<LogMetadata> {
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

pub(crate) fn prefix_end(file: &mut File, path: &Path, maximum: usize, boundary: LogOffset, inclusive: bool) -> Result<u64> {
    let mut next = 0;
    let length = file.metadata().map_err(|error| Error::io(path, error))?.len();
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

pub(crate) fn settle_prefix(file: &mut File, path: &Path, maximum: usize, end: u64) -> Result<()> {
    let zeros = [0; 8192];
    let mut next = 0;
    while next < end {
        let header = read_record_header_at(file, path, maximum, next, end)?;
        file.seek(SeekFrom::Start(next)).map_err(|error| Error::io(path, error))?;
        file.write_all(&[RETIRED_SEPARATOR]).map_err(|error| Error::io(path, error))?;
        file.seek(SeekFrom::Start(next + HEADER_BYTES)).map_err(|error| Error::io(path, error))?;
        let mut remaining = header.payload_size;
        while remaining > 0 {
            let count = remaining.min(zeros.len());
            file.write_all(&zeros[..count]).map_err(|error| Error::io(path, error))?;
            remaining -= count;
        }
        next += HEADER_BYTES + header.payload_size as u64 + TRAILER_BYTES;
    }
    crate::sync_file(file).map_err(|error| Error::io(path, error))
}

fn inspect(file: &mut File, path: &Path, maximum: usize) -> Result<LogOffset> {
    let length = file.metadata().map_err(|error| Error::io(path, error))?.len();
    if length >= 1 << 48 { return Err(corrupt(path, length, "local log exceeds the 48-bit offset space")); }
    file.seek(SeekFrom::Start(0)).map_err(|error| Error::io(path, error))?;
    let mut latest = LogOffset(-1);
    while file.stream_position().map_err(|error| Error::io(path, error))? < length {
        latest = inspect_record(file, path, maximum)?.offset;
    }
    Ok(latest)
}

impl LocalLog {
    /// Opens the exact LOG/REPLICA_LOG file selected by the caller.
    /// The database owner runs the returned driver, cancels and joins it, then calls `close_and_wait` before runtime shutdown.
    pub fn open(path: impl AsRef<Path>, options: LocalLogOptions) -> Result<(Arc<Self>, LocalLogDriver)> {
        Self::open_with_timestamp_source(path, options, Arc::new(SuppliedTimestamp))
    }

    /// Uses `timestamps` in the writer; enqueued timestamps remain available to that policy.
    /// The returned driver has the same run/cancel/join obligation as `open`.
    pub fn open_with_timestamp_source(path: impl AsRef<Path>, options: LocalLogOptions, timestamps: Arc<dyn AppendTimestampSource>) -> Result<(Arc<Self>, LocalLogDriver)> {
        Self::open_with_timestamp_source_in(path, options, timestamps, IoRegion::new())
    }

    /// Uses a caller-owned reader I/O region; the caller closes that region after consumers and the writer join.
    pub fn open_with_timestamp_source_in(path: impl AsRef<Path>, options: LocalLogOptions, timestamps: Arc<dyn AppendTimestampSource>, reads: Arc<IoRegion>) -> Result<(Arc<Self>, LocalLogDriver)> {
        let _operation = reads.admit()?;
        let path = path.as_ref();
        if options.queue_capacity == 0 || options.maximum_message_bytes > i32::MAX as usize {
            return Err(Error::Task("local log requires a positive queue and an i32 payload limit".into()));
        }
        if let Some(parent) = path.parent() { fs::create_dir_all(parent).map_err(|error| Error::io(parent, error))?; }
        let mut file = OpenOptions::new().create(true).truncate(false).read(true).write(true).open(path)
            .map_err(|error| Error::io(path, error))?;
        file.try_lock_exclusive().map_err(|error| Error::io(path, error))?;
        let latest = inspect(&mut file, path, options.maximum_message_bytes)?;
        crate::sync_file(&file).map_err(|error| Error::io(path, error))?;
        if let Some(parent) = path.parent() {
            crate::sync_directory(parent).map_err(|error| Error::io(parent, error))?;
        }
        let path = Arc::new(path.to_path_buf());
        let progress = Arc::new(Progress::new(latest));
        let (send, queue) = mpsc::channel(options.queue_capacity);
        let log = Arc::new(Self { path: path.clone(), queue: send, progress: progress.clone(), maximum_message_bytes: options.maximum_message_bytes, reads });
        Ok((log, LocalLogDriver { writer: Some(Writer { file, path, timestamps }), queue, progress }))
    }

    /// Closes and drains reader I/O; the caller must cancel and join the writer driver first.
    /// When the region is shared, this closes admission for every store and log using it.
    pub async fn close_and_wait(&self) { self.reads.close_and_wait().await; }
}

impl Writer {
    fn append(&mut self, records: &[Queued]) -> Result<Vec<LogMetadata>> {
        let start = self.file.seek(SeekFrom::End(0)).map_err(|error| Error::io(self.path.as_ref(), error))?;
        let result = (|| {
            let mut metadata = Vec::with_capacity(records.len());
            for record in records {
                let timestamp = self.timestamps.timestamp(&record.payload, record.timestamp)?;
                let offset = self.file.stream_position().map_err(|error| Error::io(self.path.as_ref(), error))?;
                let end = offset.checked_add(HEADER_BYTES + record.payload.len() as u64 + TRAILER_BYTES)
                    .filter(|end| *end < 1 << 48)
                    .ok_or_else(|| corrupt(&self.path, offset, "local log offset space exhausted"))?;
                let mut bytes = Vec::with_capacity((end - offset) as usize);
                bytes.push(RECORD_SEPARATOR);
                bytes.extend_from_slice(&(record.payload.len() as i32).to_be_bytes());
                bytes.extend_from_slice(&timestamp.0.to_be_bytes());
                bytes.extend_from_slice(&record.payload);
                bytes.extend_from_slice(&(offset as i64).to_be_bytes());
                self.file.write_all(&bytes).map_err(|error| Error::io(self.path.as_ref(), error))?;
                metadata.push(LogMetadata { offset: LogOffset(offset as i64), timestamp });
            }
            crate::sync_file(&self.file).map_err(|error| Error::io(self.path.as_ref(), error))?;
            Ok(metadata)
        })();
        if result.is_err() {
            if let Err(rollback) = self.file.set_len(start).and_then(|_| crate::sync_file(&self.file)) {
                return Err(corrupt(&self.path, start, format!("append failed: {}; rollback failed: {rollback}", result.as_ref().unwrap_err())));
            }
        }
        result
    }
}

impl LocalLogDriver {
    pub async fn run(mut self, shutdown: CancellationToken) -> Result<()> {
        loop {
            let first = tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                record = self.queue.recv() => match record { Some(record) => record, None => break },
            };
            let mut records = vec![first];
            while records.len() < 10 {
                match self.queue.try_recv() { Ok(record) => records.push(record), Err(_) => break }
            }
            let mut writer = self.writer.take().ok_or_else(|| Error::Closed("local writer unavailable".into()))?;
            let joined = tokio::task::spawn_blocking(move || {
                let result = writer.append(&records);
                (writer, records, result)
            }).await;
            let (writer, records, result) = match joined {
                Ok(result) => result,
                Err(error) => {
                    self.progress.fail(error.to_string());
                    self.reject_pending(error.to_string());
                    return Err(Error::Task(error.to_string()));
                }
            };
            self.writer = Some(writer);
            match result {
                Ok(metadata) => {
                    if let Some(last) = metadata.last() { self.progress.advance(last.offset); }
                    for (record, metadata) in records.into_iter().zip(metadata) { let _ = record.durable.send(Ok(metadata)); }
                }
                Err(error) => {
                    let reason = error.to_string();
                    self.progress.fail(reason.clone());
                    for record in records { let _ = record.durable.send(Err(Error::Closed(reason.clone()))); }
                    self.reject_pending(reason);
                    return Err(error);
                }
            }
        }
        self.reject_pending("local log shutdown before append".into());
        self.progress.close();
        Ok(())
    }

    fn reject_pending(&mut self, reason: String) {
        self.queue.close();
        while let Ok(record) = self.queue.try_recv() { let _ = record.durable.send(Err(Error::Closed(reason.clone()))); }
    }
}

impl Drop for LocalLogDriver {
    fn drop(&mut self) { self.progress.close(); }
}

#[async_trait]
impl Log for LocalLog {
    fn retains_synced_history(&self) -> bool { true }
    fn maximum_append_bytes(&self) -> Option<usize> { Some(self.maximum_message_bytes) }

    async fn range(&self, from: LogOffset, to: LogOffset) -> Result<Box<dyn super::LogRange>> {
        super::read_only::file_range(Arc::clone(&self.path), self.maximum_message_bytes, from, to, Some(self.progress.latest()?))
    }
    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> Result<AppendReceipt> {
        if payload.len() > self.maximum_message_bytes {
            return Err(Error::MessageTooLarge { size: payload.len(), maximum: self.maximum_message_bytes });
        }
        self.progress.latest()?;
        let (send, durable) = oneshot::channel();
        self.queue.send(Queued { payload, timestamp, durable: send }).await
            .map_err(|_| Error::Closed("local append queue ended".into()))?;
        Ok(AppendReceipt::new(durable))
    }

    fn latest(&self) -> Result<LogOffset> { self.progress.latest() }

    async fn tail(&self, after: LogOffset) -> Result<Box<dyn LogTail>> {
        let (path, maximum, progress) = (self.path.clone(), self.maximum_message_bytes, self.progress.clone());
        let latest = progress.latest()?;
        if after.0 < -1 || after > latest {
            return Err(Error::UnavailableLogRange { requested: after.0, first_retained: 0 });
        }
        let next = if after.0 < 0 { 0 } else {
            let path = path.clone();
            self.reads.run(move || {
                let mut file = File::open(path.as_ref()).map_err(|error| Error::io(path.as_ref(), error))?;
                file.seek(SeekFrom::Start(after.0 as u64)).map_err(|error| Error::io(path.as_ref(), error))?;
                inspect_record(&mut file, &path, maximum)?;
                file.stream_position().map_err(|error| Error::io(path.as_ref(), error))
            }).await?
        };
        Ok(Box::new(LocalTail { path, progress, maximum, after, next, reads: Arc::clone(&self.reads) }))
    }

    async fn read_range(&self, from: LogOffset, to: LogOffset) -> Result<Vec<LogRecord>> {
        let latest = self.progress.latest()?;
        let (path, maximum) = (self.path.clone(), self.maximum_message_bytes);
        if from.0 < 0 { return Err(Error::UnavailableLogRange { requested: from.0, first_retained: 0 }); }
        if to < from { return Err(Error::Task("log range end precedes its start".into())); }
        self.reads.run(move || {
            let mut file = File::open(path.as_ref()).map_err(|error| Error::io(path.as_ref(), error))?;
            if from > latest {
                let end = if latest.0 < 0 { 0 } else {
                    file.seek(SeekFrom::Start(latest.0 as u64)).map_err(|error| Error::io(path.as_ref(), error))?;
                    read_record(&mut file, &path, maximum)?;
                    file.stream_position().map_err(|error| Error::io(path.as_ref(), error))? as i64
                };
                if from.0 != end { return Err(Error::UnavailableLogRange { requested: from.0, first_retained: 0 }); }
                return Ok(Vec::new());
            }
            file.seek(SeekFrom::Start(from.0 as u64)).map_err(|error| Error::io(path.as_ref(), error))?;
            let first = read_record(&mut file, &path, maximum)?;
            let mut result = Vec::new();
            if from < to { result.push(first); }
            else { return Ok(result); }
            loop {
                let next = file.stream_position().map_err(|error| Error::io(path.as_ref(), error))? as i64;
                if next >= to.0 || next > latest.0 { break; }
                result.push(read_record(&mut file, &path, maximum)?);
            }
            Ok(result)
        }).await
    }
}

struct LocalTail {
    path: Arc<PathBuf>, progress: Arc<Progress>, maximum: usize, after: LogOffset, next: u64,
    reads: Arc<IoRegion>,
}

impl LocalTail {
    async fn poll_with_payload_limit(&mut self, timeout: Duration, payload_limit: usize) -> Result<Vec<LogRecord>> {
        let tip = await_tip(&self.progress, self.after, timeout).await?;
        if tip <= self.after { return Ok(Vec::new()); }
        let (path, maximum, next) = (self.path.clone(), self.maximum, self.next);
        let payload_limit = payload_limit.max(maximum);
        let (records, next) = self.reads.run(move || {
            let mut file = File::open(path.as_ref()).map_err(|error| Error::io(path.as_ref(), error))?;
            file.seek(SeekFrom::Start(next)).map_err(|error| Error::io(path.as_ref(), error))?;
            let mut records = Vec::new();
            let mut payload_bytes = 0usize;
            while file.stream_position().map_err(|error| Error::io(path.as_ref(), error))? <= tip.0 as u64 {
                if records.len() == MAXIMUM_POLL_RECORDS
                    || payload_bytes.checked_add(maximum).is_none_or(|bytes| bytes > payload_limit)
                {
                    break;
                }
                let record = read_record(&mut file, &path, maximum)?;
                payload_bytes += record.payload.len();
                records.push(record);
            }
            let next = file.stream_position().map_err(|error| Error::io(path.as_ref(), error))?;
            Ok::<_, Error>((records, next))
        }).await?;
        let last = records.last().ok_or_else(|| corrupt(&self.path, self.next, "durable tip yielded no record"))?;
        self.after = last.metadata.offset;
        self.next = next;
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

fn open_at(path: &Path, next: u64) -> Result<File> {
    let mut file = File::open(path).map_err(|error| Error::io(path, error))?;
    file.seek(SeekFrom::Start(next)).map_err(|error| Error::io(path, error))?;
    Ok(file)
}

#[async_trait]
impl LogReplayTail for LocalTail {
    async fn poll_head(&mut self, timeout: Duration) -> Result<Option<ReplayHead>> {
        let tip = await_tip(&self.progress, self.after, timeout).await?;
        if tip <= self.after { return Ok(None); }
        if self.next > tip.0 as u64 { return Err(corrupt(&self.path, self.next, "durable tip yielded no record")); }

        let (path, maximum, next) = (Arc::clone(&self.path), self.maximum, self.next);
        self.reads.run(move || {
            let mut file = open_at(&path, next)?;
            Ok(Some(read_record_header(&mut file, &path, maximum)?.replay_head()))
        }).await
    }

    async fn read_head(&mut self, head: ReplayHead, reservation: ReadReservation) -> Result<LogRecord> {
        let tip = self.progress.latest()?;
        if head.metadata.offset.0 != self.next as i64 {
            return Err(Error::Task("log replay head does not match the next position".into()));
        }
        if head.metadata.offset > tip {
            return Err(Error::Task("log replay head exceeds the durable tip".into()));
        }
        let (path, maximum, next) = (Arc::clone(&self.path), self.maximum, self.next);
        let (record, next) = self.reads.run(move || {
            let mut file = open_at(&path, next)?;
            let header = read_record_header(&mut file, &path, maximum)?;
            if header.replay_head() != head {
                return Err(corrupt(&path, next, "log replay head changed before delivery"));
            }
            if reservation.length() < head.allocation_bound {
                return Err(Error::Task("log read reservation is too small".into()));
            }
            let record = read_record_body(&mut file, &path, header)?.retain(reservation)?;
            let next = file.stream_position().map_err(|error| Error::io(path.as_ref(), error))?;
            Ok((record, next))
        }).await?;
        self.after = record.metadata.offset;
        self.next = next;
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            progress: Arc::clone(&log.progress),
            maximum: log.maximum_message_bytes,
            after: LogOffset(-1),
            next: 0,
            reads: Arc::clone(&log.reads),
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
}

#[cfg(test)]
mod retention_tests {
    use super::*;

    #[test]
    fn settling_a_prefix_keeps_frame_offsets_and_excludes_its_payloads() -> Result<()> {
        let scratch = tempfile::NamedTempFile::new().map_err(|error| Error::Task(error.to_string()))?;
        let path = Arc::new(scratch.path().to_path_buf());
        let mut writer = Writer {
            file: scratch.reopen().map_err(|error| Error::io(&path, error))?,
            path: Arc::clone(&path),
            timestamps: Arc::new(SuppliedTimestamp),
        };
        let record = |payload: &'static [u8]| Queued {
            payload: Bytes::from_static(payload), timestamp: Timestamp(5), durable: oneshot::channel().0,
        };
        let records = writer.append(&[record(b"old receipt"), record(b"surviving suffix")])?;
        let before = records[1].offset;
        let end = prefix_end(&mut writer.file, &path, 1024, before, false)?;
        for _ in 0..2 { settle_prefix(&mut writer.file, &path, 1024, end)?; }

        let bytes = fs::read(path.as_ref()).map_err(|error| Error::io(&path, error))?;
        assert!(bytes[HEADER_BYTES as usize..HEADER_BYTES as usize + b"old receipt".len()].iter().all(|byte| *byte == 0));
        assert_eq!(inspect(&mut writer.file, &path, 1024)?, before);
        writer.file.seek(SeekFrom::Start(0)).map_err(|error| Error::io(&path, error))?;
        assert!(matches!(read_record(&mut writer.file, &path, 1024), Err(Error::UnavailableLogRange { requested: 0, .. })));
        writer.file.seek(SeekFrom::Start(before.0 as u64)).map_err(|error| Error::io(&path, error))?;
        assert_eq!(read_record(&mut writer.file, &path, 1024)?.payload.as_ref(), b"surviving suffix");
        Ok(())
    }
}
