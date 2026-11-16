use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use bytes::Bytes;
use rtdb_types::Timestamp;
use super::{AppendReceipt, Log, LogOffset, LogRecord, LogTail};
use super::local::{corrupt, read_record};
use crate::{Error, Result};

#[derive(Clone, Debug)]
pub struct ReadOnlyLocalLogOptions {
    pub maximum_message_bytes: usize,
    pub poll_interval: Duration,
    pub maximum_poll_records: usize,
    /// Payload bytes retained by one poll; record descriptors are bounded separately.
    pub maximum_poll_bytes: usize,
}

impl Default for ReadOnlyLocalLogOptions {
    fn default() -> Self {
        Self { maximum_message_bytes: 64 * 1024 * 1024, poll_interval: Duration::from_millis(10), maximum_poll_records: 1024, maximum_poll_bytes: 128 * 1024 * 1024 }
    }
}

pub struct ReadOnlyLocalLog { path: Arc<PathBuf>, options: ReadOnlyLocalLogOptions }

impl ReadOnlyLocalLog {
    /// Opens the caller-selected LOG or REPLICA_LOG path without creating it or taking a writer lock.
    /// The external writer must preserve the append-only file and its record framing.
    pub fn open(path: impl AsRef<Path>, options: ReadOnlyLocalLogOptions) -> Result<Arc<Self>> {
        if options.maximum_message_bytes > i32::MAX as usize || options.poll_interval.is_zero() || options.maximum_poll_records == 0 || options.maximum_poll_bytes < options.maximum_message_bytes {
            return Err(Error::Task("read-only log requires a positive polling interval and record limit, an i32 message limit, and room for a maximum-size message".into()));
        }
        let log = Arc::new(Self { path: Arc::new(path.as_ref().to_path_buf()), options });
        log.latest()?;
        Ok(log)
    }
}

fn open_existing(path: &Path) -> Result<Option<File>> {
    match File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(Error::io(path, error)),
    }
}

fn latest_in_file(file: &mut File, path: &Path, maximum: usize) -> Result<(LogOffset, u64)> {
    let length = file.metadata().map_err(|error| Error::io(path, error))?.len();
    if length == 0 { return Ok((LogOffset(-1), 0)); }
    if length < 21 || length >= 1 << 48 { return Err(corrupt(path, length, "local log length is outside its framing bounds")); }
    file.seek(SeekFrom::Start(length - 8)).map_err(|error| Error::io(path, error))?;
    let mut trailer = [0; 8];
    file.read_exact(&mut trailer).map_err(|error| corrupt(path, length - 8, format!("incomplete final offset: {error}")))?;
    let offset = i64::from_be_bytes(trailer);
    if offset < 0 || offset as u64 >= length { return Err(corrupt(path, length - 8, "final offset is outside the log")); }
    file.seek(SeekFrom::Start(offset as u64)).map_err(|error| Error::io(path, error))?;
    let record = read_record(file, path, maximum)?;
    if file.stream_position().map_err(|error| Error::io(path, error))? != length { return Err(corrupt(path, offset as u64, "final record does not end at the observed file length")); }
    Ok((record.metadata.offset, length))
}

fn unavailable(position: LogOffset) -> Error { Error::UnavailableLogRange { requested: position.0, first_retained: 0 } }

fn read_batch(file: &mut File, path: &Path, options: &ReadOnlyLocalLogOptions, from: u64, to: u64, latest: LogOffset) -> Result<(Vec<LogRecord>, u64)> {
    file.seek(SeekFrom::Start(from)).map_err(|error| Error::io(path, error))?;
    let mut records = Vec::new();
    let mut bytes = 0usize;
    let mut next = from;
    while next < to && latest.0 >= 0 && next <= latest.0 as u64 {
        if records.len() == options.maximum_poll_records || bytes.checked_add(options.maximum_message_bytes).is_none_or(|size| size > options.maximum_poll_bytes) { break; }
        let record = read_record(file, path, options.maximum_message_bytes)?;
        bytes += record.payload.len();
        records.push(record);
        next = file.stream_position().map_err(|error| Error::io(path, error))?;
    }
    Ok((records, next))
}

#[async_trait]
impl Log for ReadOnlyLocalLog {
    async fn range(&self, from: LogOffset, to: LogOffset) -> Result<Box<dyn super::LogRange>> {
        file_range(Arc::clone(&self.path), self.options.maximum_message_bytes, from, to, None)
    }
    async fn enqueue(&self, _: Bytes, _: Timestamp) -> Result<AppendReceipt> { Err(Error::ReadOnlyLog(self.path.as_ref().clone())) }

    fn latest(&self) -> Result<LogOffset> {
        let Some(mut file) = open_existing(&self.path)? else { return Ok(LogOffset(-1)); };
        latest_in_file(&mut file, &self.path, self.options.maximum_message_bytes).map(|(latest, _)| latest)
    }

    async fn tail(&self, after: LogOffset) -> Result<Box<dyn LogTail>> {
        if after.0 < -1 { return Err(unavailable(after)); }
        let (next, observed_length) = match open_existing(&self.path)? {
            None if after.0 == -1 => (0, 0),
            None => return Err(unavailable(after)),
            Some(mut file) => {
                let (latest, end) = latest_in_file(&mut file, &self.path, self.options.maximum_message_bytes)?;
                if after > latest { return Err(unavailable(after)); }
                let next = if after.0 == -1 { 0 } else {
                    file.seek(SeekFrom::Start(after.0 as u64)).map_err(|error| Error::io(self.path.as_ref(), error))?;
                    read_record(&mut file, &self.path, self.options.maximum_message_bytes)?;
                    file.stream_position().map_err(|error| Error::io(self.path.as_ref(), error))?
                };
                (next, end)
            }
        };
        Ok(Box::new(ReadOnlyTail { path: Arc::clone(&self.path), options: self.options.clone(), after, next, observed_length }))
    }

    async fn read_range(&self, from: LogOffset, to: LogOffset) -> Result<Vec<LogRecord>> {
        if from.0 < 0 { return Err(unavailable(from)); }
        if to < from { return Err(Error::Task("log range end precedes its start".into())); }
        let Some(mut file) = open_existing(&self.path)? else {
            return if from.0 == 0 { Ok(Vec::new()) } else { Err(unavailable(from)) };
        };
        let (latest, end) = latest_in_file(&mut file, &self.path, self.options.maximum_message_bytes)?;
        if from.0 as u64 > end { return Err(unavailable(from)); }
        if from.0 as u64 == end { return Ok(Vec::new()); }
        if from == to {
            file.seek(SeekFrom::Start(from.0 as u64)).map_err(|error| Error::io(self.path.as_ref(), error))?;
            read_record(&mut file, &self.path, self.options.maximum_message_bytes)?;
            return Ok(Vec::new());
        }
        let (records, next) = read_batch(&mut file, &self.path, &self.options, from.0 as u64, to.0 as u64, latest)?;
        if next < (to.0 as u64).min(end) { return Err(Error::LogReadLimit { maximum_records: self.options.maximum_poll_records, maximum_payload_bytes: self.options.maximum_poll_bytes }); }
        Ok(records)
    }
}

fn frame_end(file: &mut File, path: &Path, start: u64, maximum: usize) -> Result<u64> {
    file.seek(SeekFrom::Start(start)).map_err(|error| Error::io(path, error))?;
    let mut header = [0; 13];
    file.read_exact(&mut header).map_err(|error| corrupt(path, start, error.to_string()))?;
    let size = i32::from_be_bytes(header[1..5].try_into().expect("fixed header"));
    if header[0] != 0x1e || size < 0 || size as usize > maximum { return Err(corrupt(path, start, "invalid record header")); }
    let end = start.checked_add(21 + size as u64).ok_or_else(|| corrupt(path, start, "record end overflow"))?;
    file.seek(SeekFrom::Start(end - 8)).map_err(|error| Error::io(path, error))?;
    let mut trailer = [0; 8];
    file.read_exact(&mut trailer).map_err(|error| corrupt(path, start, error.to_string()))?;
    if i64::from_be_bytes(trailer) != start as i64 { return Err(corrupt(path, start, "record trailer does not name its start")); }
    Ok(end)
}

pub(super) fn file_range(path: Arc<PathBuf>, maximum: usize, from: LogOffset, to: LogOffset, durable: Option<LogOffset>) -> Result<Box<dyn super::LogRange>> {
    if from.0 < 0 { return Err(unavailable(from)); }
    if to < from { return Err(Error::Task("log range end precedes its start".into())); }
    let mut file = open_existing(&path)?;
    let end = if let Some(file) = &mut file {
        let length = file.metadata().map_err(|error| Error::io(path.as_ref(), error))?.len();
        let latest = match durable {
            Some(latest) => latest,
            None if length == 0 => LogOffset(-1),
            None => {
                if length < 21 || length >= 1 << 48 { return Err(corrupt(&path, length, "invalid log length")); }
                file.seek(SeekFrom::Start(length - 8)).map_err(|error| Error::io(path.as_ref(), error))?;
                let mut trailer = [0; 8];
                file.read_exact(&mut trailer).map_err(|error| corrupt(&path, length - 8, error.to_string()))?;
                LogOffset(i64::from_be_bytes(trailer))
            }
        };
        let end = if latest.0 == -1 { 0 } else {
            if latest.0 < 0 || latest.0 as u64 >= length { return Err(corrupt(&path, length, "invalid final offset")); }
            frame_end(file, &path, latest.0 as u64, maximum)?
        };
        if durable.is_none() && end != length { return Err(corrupt(&path, end, "final record does not end at file length")); }
        if (from.0 as u64) < end { frame_end(file, &path, from.0 as u64, maximum)?; }
        end.min(to.0 as u64)
    } else if durable.is_some_and(|latest| latest.0 >= 0) {
        return Err(corrupt(&path, 0, "durable log file disappeared"));
    } else {
        0
    };
    Ok(Box::new(FileRange { file, path, maximum, next: from.0 as u64, end }))
}

struct FileRange { file: Option<File>, path: Arc<PathBuf>, maximum: usize, next: u64, end: u64 }
#[async_trait]
impl super::LogRange for FileRange {
    fn allocation_bound(&self) -> usize { self.maximum.saturating_add(256) }
    async fn next(&mut self, reservation: crate::object_store::ReadReservation) -> Result<Option<LogRecord>> {
        if reservation.length() < self.allocation_bound() { return Err(Error::Task("log read reservation is too small".into())); }
        if self.next >= self.end { return Ok(None); }
        let file = self.file.as_mut().ok_or_else(|| unavailable(LogOffset(self.next as i64)))?;
        file.seek(SeekFrom::Start(self.next)).map_err(|error| Error::io(self.path.as_ref(), error))?;
        let record = read_record(file, &self.path, self.maximum)?;
        self.next = file.stream_position().map_err(|error| Error::io(self.path.as_ref(), error))?;
        record.retain(reservation).map(Some)
    }
}

struct ReadOnlyTail { path: Arc<PathBuf>, options: ReadOnlyLocalLogOptions, after: LogOffset, next: u64, observed_length: u64 }

impl ReadOnlyTail {
    fn read_available(&mut self) -> Result<Vec<LogRecord>> {
        let Some(mut file) = open_existing(&self.path)? else {
            return if self.observed_length == 0 { Ok(Vec::new()) } else { Err(corrupt(&self.path, self.next, "observed log disappeared")) };
        };
        let (latest, end) = latest_in_file(&mut file, &self.path, self.options.maximum_message_bytes)?;
        if end < self.observed_length || end < self.next || latest < self.after { return Err(corrupt(&self.path, end, "observed log was truncated")); }
        self.observed_length = end;
        if latest == self.after { return Ok(Vec::new()); }
        let (records, next) = read_batch(&mut file, &self.path, &self.options, self.next, end, latest)?;
        let last = records.last().ok_or_else(|| corrupt(&self.path, self.next, "complete tip yielded no record"))?;
        self.after = last.metadata.offset;
        self.next = next;
        Ok(records)
    }
}

#[async_trait]
impl LogTail for ReadOnlyTail {
    async fn poll(&mut self, timeout: Duration) -> Result<Vec<LogRecord>> {
        let deadline = tokio::time::Instant::now().checked_add(timeout).ok_or_else(|| Error::Task("local log poll deadline overflow".into()))?;
        loop {
            let records = self.read_available()?;
            if !records.is_empty() { return Ok(records); }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() { return Ok(Vec::new()); }
            tokio::time::sleep(remaining.min(self.options.poll_interval)).await;
        }
    }
}
