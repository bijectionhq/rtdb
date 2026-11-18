use super::{AppendReceipt, AppendTimestampSource, Log, LogMetadata, LogOffset, LogRecord, LogTail, Progress, SuppliedTimestamp, await_tip};
use crate::{Error, IoRegion, Result};
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

pub(super) fn read_record(file: &mut File, path: &Path, maximum: usize) -> Result<LogRecord> {
    let offset = file.stream_position().map_err(|error| Error::io(path, error))?;
    let length = file.metadata().map_err(|error| Error::io(path, error))?.len();
    let mut header = [0; HEADER_BYTES as usize];
    file.read_exact(&mut header).map_err(|error| corrupt(path, offset, format!("incomplete record header: {error}")))?;
    if header[0] != RECORD_SEPARATOR { return Err(corrupt(path, offset, "expected record separator 0x1e")); }
    let size = i32::from_be_bytes(header[1..5].try_into().expect("fixed header size"));
    if size < 0 || size as usize > maximum {
        return Err(corrupt(path, offset, format!("invalid payload size {size}, maximum {maximum}")));
    }
    let end = offset.checked_add(HEADER_BYTES + size as u64 + TRAILER_BYTES)
        .ok_or_else(|| corrupt(path, offset, "record end overflows"))?;
    if end > length { return Err(corrupt(path, offset, format!("incomplete record ends at {end}, file length {length}"))); }
    let timestamp = Timestamp(i64::from_be_bytes(header[5..13].try_into().expect("fixed header size")));
    let mut payload = vec![0; size as usize];
    file.read_exact(&mut payload).map_err(|error| Error::io(path, error))?;
    let mut trailer = [0; TRAILER_BYTES as usize];
    file.read_exact(&mut trailer).map_err(|error| Error::io(path, error))?;
    if i64::from_be_bytes(trailer) != offset as i64 {
        return Err(corrupt(path, offset, "record trailer does not name its start offset"));
    }
    Ok(LogRecord { metadata: LogMetadata { offset: LogOffset(offset as i64), timestamp }, payload: Bytes::from(payload) })
}

fn inspect(file: &mut File, path: &Path, maximum: usize) -> Result<LogOffset> {
    let length = file.metadata().map_err(|error| Error::io(path, error))?.len();
    if length >= 1 << 48 { return Err(corrupt(path, length, "local log exceeds the 48-bit offset space")); }
    file.seek(SeekFrom::Start(0)).map_err(|error| Error::io(path, error))?;
    let mut latest = LogOffset(-1);
    while file.stream_position().map_err(|error| Error::io(path, error))? < length {
        latest = read_record(file, path, maximum)?.metadata.offset;
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
        file.sync_all().map_err(|error| Error::io(path, error))?;
        if let Some(parent) = path.parent() {
            File::open(parent).and_then(|directory| directory.sync_all()).map_err(|error| Error::io(parent, error))?;
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
            self.file.sync_all().map_err(|error| Error::io(self.path.as_ref(), error))?;
            Ok(metadata)
        })();
        if result.is_err() {
            if let Err(rollback) = self.file.set_len(start).and_then(|_| self.file.sync_all()) {
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
                read_record(&mut file, &path, maximum)?;
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

#[async_trait]
impl LogTail for LocalTail {
    async fn poll(&mut self, timeout: Duration) -> Result<Vec<LogRecord>> {
        let tip = await_tip(&self.progress, self.after, timeout).await?;
        if tip <= self.after { return Ok(Vec::new()); }
        let (path, maximum, next) = (self.path.clone(), self.maximum, self.next);
        let (records, next) = self.reads.run(move || {
            let mut file = File::open(path.as_ref()).map_err(|error| Error::io(path.as_ref(), error))?;
            file.seek(SeekFrom::Start(next)).map_err(|error| Error::io(path.as_ref(), error))?;
            let mut records = Vec::new();
            while file.stream_position().map_err(|error| Error::io(path.as_ref(), error))? <= tip.0 as u64 {
                records.push(read_record(&mut file, &path, maximum)?);
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
