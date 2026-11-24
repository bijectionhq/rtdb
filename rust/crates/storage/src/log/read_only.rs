use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use bytes::Bytes;
use rtdb_types::Timestamp;
use super::{AppendReceipt, Log, LogOffset, LogRecord, LogReplayTail, LogTail, ReplayHead};
use super::local::{corrupt, inspect_record, inspect_record_body, is_record_separator, physical_maximum, read_record, read_record_body, read_record_header_at};
use crate::{Error, IoRegion, Result, object_store::ReadReservation, sealing::{ClassicKeyVault, ClassicObjectKeys}};

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

pub struct ReadOnlyLocalLog {
    path: Arc<PathBuf>,
    options: ReadOnlyLocalLogOptions,
    reads: Arc<IoRegion>,
    reserved_replay: bool,
    keys: Option<Arc<ClassicObjectKeys>>,
}

impl ReadOnlyLocalLog {
    /// Opens the caller-selected LOG or REPLICA_LOG path without creating it or taking a writer lock.
    /// The external writer must preserve the append-only file and its record framing.
    pub fn open(path: impl AsRef<Path>, options: ReadOnlyLocalLogOptions) -> Result<Arc<Self>> {
        Self::open_with_reads(path.as_ref(), options, None, None)
    }

    /// Opens the external append-only log as in [`Self::open`] and enables reserved replay.
    /// Opening and tip lookup use synchronous I/O. Replay readiness and delivery run in the caller's region.
    /// A closed region rejects opening and new replay reads.
    /// The caller stops its consumers and closes and drains the region before shutting down the runtime.
    pub fn open_in(path: impl AsRef<Path>, options: ReadOnlyLocalLogOptions, reads: Arc<IoRegion>) -> Result<Arc<Self>> {
        Self::open_with_reads(path.as_ref(), options, Some(reads), None)
    }

    pub fn open_with_key_slots(path: impl AsRef<Path>, options: ReadOnlyLocalLogOptions,
        reads: Arc<IoRegion>, keys: Arc<ClassicObjectKeys>,
    ) -> Result<Arc<Self>> {
        Self::open_with_reads(path.as_ref(), options, Some(reads), Some(keys))
    }

    fn open_with_reads(path: &Path, mut options: ReadOnlyLocalLogOptions, reads: Option<Arc<IoRegion>>,
        keys: Option<Arc<ClassicObjectKeys>>,
    ) -> Result<Arc<Self>> {
        let reserved_replay = reads.is_some();
        let reads = reads.unwrap_or_else(IoRegion::new);
        let path = Arc::new(path.to_path_buf());
        let _discovery = reads.admit_read(path.clone(), ())?;
        if keys.is_none() {
            ClassicKeyVault::require_unsealed_log(&path).map_err(|error| Error::Task(error.to_string()))?;
        }
        let expanded = physical_maximum(options.maximum_message_bytes, keys.is_some())?;
        if keys.is_some() {
            options.maximum_poll_bytes = options.maximum_poll_bytes.max(expanded);
        }
        options.maximum_message_bytes = expanded;
        if options.maximum_message_bytes > i32::MAX as usize || options.poll_interval.is_zero() || options.maximum_poll_records == 0 || options.maximum_poll_bytes < options.maximum_message_bytes {
            return Err(Error::Task("read-only log requires a positive polling interval and record limit, an i32 message limit, and room for a maximum-size message".into()));
        }
        let log = Arc::new(Self { path, options, reads, reserved_replay, keys });
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
    Ok((latest_at_length(file, path, maximum, length, TipInspection::CurrentFile)?, length))
}

enum TipInspection { CurrentFile, CapturedPrefix }

fn latest_at_length(file: &mut File, path: &Path, maximum: usize, length: u64, inspection: TipInspection) -> Result<LogOffset> {
    if length == 0 { return Ok(LogOffset(-1)); }
    if length < 21 || length >= 1 << 48 { return Err(corrupt(path, length, "local log length is outside its framing bounds")); }
    file.seek(SeekFrom::Start(length - 8)).map_err(|error| Error::io(path, error))?;
    let mut trailer = [0; 8];
    file.read_exact(&mut trailer).map_err(|error| corrupt(path, length - 8, format!("incomplete final offset: {error}")))?;
    let offset = i64::from_be_bytes(trailer);
    if offset < 0 || offset as u64 >= length { return Err(corrupt(path, length - 8, "final offset is outside the log")); }
    let metadata = match inspection {
        TipInspection::CurrentFile => {
            file.seek(SeekFrom::Start(offset as u64)).map_err(|error| Error::io(path, error))?;
            inspect_record(file, path, maximum)?
        }
        TipInspection::CapturedPrefix => {
            let header = read_record_header_at(file, path, maximum, offset as u64, length)?;
            inspect_record_body(file, path, header)?
        }
    };
    if file.stream_position().map_err(|error| Error::io(path, error))? != length { return Err(corrupt(path, offset as u64, "final record does not end at the observed file length")); }
    Ok(metadata.offset)
}

fn unavailable(position: LogOffset) -> Error { Error::UnavailableLogRange { requested: position.0, first_retained: 0 } }

fn read_batch(file: &mut File, path: &Path, options: &ReadOnlyLocalLogOptions, from: u64, to: u64, latest: LogOffset, keys: Option<&ClassicObjectKeys>) -> Result<(Vec<LogRecord>, u64)> {
    file.seek(SeekFrom::Start(from)).map_err(|error| Error::io(path, error))?;
    let mut records = Vec::new();
    let mut bytes = 0usize;
    let mut next = from;
    while next < to && latest.0 >= 0 && next <= latest.0 as u64 {
        if records.len() == options.maximum_poll_records || bytes.checked_add(options.maximum_message_bytes).is_none_or(|size| size > options.maximum_poll_bytes) { break; }
        let record = read_record(file, path, options.maximum_message_bytes, keys)?;
        bytes += record.payload.len();
        records.push(record);
        next = file.stream_position().map_err(|error| Error::io(path, error))?;
    }
    Ok((records, next))
}

#[async_trait]
impl Log for ReadOnlyLocalLog {
    async fn range(&self, from: LogOffset, to: LogOffset) -> Result<Box<dyn super::LogRange>> {
        file_range(self.reads.clone(), self.path.clone(), self.options.maximum_message_bytes, from, to, None, self.keys.clone())
    }
    async fn enqueue(&self, _: Bytes, _: Timestamp) -> Result<AppendReceipt> { Err(Error::ReadOnlyLog(self.path.as_ref().clone())) }

    fn latest(&self) -> Result<LogOffset> {
        let _discovery = self.reads.admit_read(self.path.clone(), ())?;
        let Some(mut file) = open_existing(&self.path)? else { return Ok(LogOffset(-1)); };
        latest_in_file(&mut file, &self.path, self.options.maximum_message_bytes).map(|(latest, _)| latest)
    }

    async fn tail(&self, after: LogOffset) -> Result<Box<dyn LogTail>> {
        if after.0 < -1 { return Err(unavailable(after)); }
        let _discovery = self.reads.admit_read(self.path.clone(), after)?;
        let (next, observed_length) = match open_existing(&self.path)? {
            None if after.0 == -1 => (0, 0),
            None => return Err(unavailable(after)),
            Some(mut file) => {
                let (latest, end) = latest_in_file(&mut file, &self.path, self.options.maximum_message_bytes)?;
                if after > latest { return Err(unavailable(after)); }
                let next = if after.0 == -1 { 0 } else {
                    file.seek(SeekFrom::Start(after.0 as u64)).map_err(|error| Error::io(self.path.as_ref(), error))?;
                    inspect_record(&mut file, &self.path, self.options.maximum_message_bytes)?;
                    file.stream_position().map_err(|error| Error::io(self.path.as_ref(), error))?
                };
                (next, end)
            }
        };
        let tail = ReadOnlyTail {
            path: Arc::clone(&self.path),
            options: self.options.clone(),
            cursor: ReadOnlyCursor { after, next, observed_length },
            reads: self.reads.clone(),
            reserved_replay: self.reserved_replay,
            keys: self.keys.clone(),
        };
        Ok(Box::new(tail))
    }

    async fn read_range(&self, from: LogOffset, to: LogOffset) -> Result<Vec<LogRecord>> {
        if from.0 < 0 { return Err(unavailable(from)); }
        if to < from { return Err(Error::Task("log range end precedes its start".into())); }
        let _read = self.reads.admit_read(self.path.clone(), (from, to))?;
        let Some(mut file) = open_existing(&self.path)? else {
            return if from.0 == 0 { Ok(Vec::new()) } else { Err(unavailable(from)) };
        };
        let (latest, end) = latest_in_file(&mut file, &self.path, self.options.maximum_message_bytes)?;
        if from.0 as u64 > end { return Err(unavailable(from)); }
        if from.0 as u64 == end { return Ok(Vec::new()); }
        if from == to {
            file.seek(SeekFrom::Start(from.0 as u64)).map_err(|error| Error::io(self.path.as_ref(), error))?;
            read_record(&mut file, &self.path, self.options.maximum_message_bytes, self.keys.as_deref())?;
            return Ok(Vec::new());
        }
        let (records, next) = read_batch(&mut file, &self.path, &self.options, from.0 as u64, to.0 as u64, latest, self.keys.as_deref())?;
        if next < (to.0 as u64).min(end) { return Err(Error::LogReadLimit { maximum_records: self.options.maximum_poll_records, maximum_payload_bytes: self.options.maximum_poll_bytes }); }
        Ok(records)
    }
}

fn frame_end(file: &mut File, path: &Path, start: u64, maximum: usize) -> Result<u64> {
    file.seek(SeekFrom::Start(start)).map_err(|error| Error::io(path, error))?;
    let mut header = [0; 13];
    file.read_exact(&mut header).map_err(|error| corrupt(path, start, error.to_string()))?;
    let size = i32::from_be_bytes(header[1..5].try_into().expect("fixed header"));
    if !is_record_separator(header[0]) || size < 0 || size as usize > maximum { return Err(corrupt(path, start, "invalid record header")); }
    let end = start.checked_add(21 + size as u64).ok_or_else(|| corrupt(path, start, "record end overflow"))?;
    file.seek(SeekFrom::Start(end - 8)).map_err(|error| Error::io(path, error))?;
    let mut trailer = [0; 8];
    file.read_exact(&mut trailer).map_err(|error| corrupt(path, start, error.to_string()))?;
    if i64::from_be_bytes(trailer) != start as i64 { return Err(corrupt(path, start, "record trailer does not name its start")); }
    Ok(end)
}

pub(super) fn file_range(reads: Arc<IoRegion>, path: Arc<PathBuf>, maximum: usize, from: LogOffset, to: LogOffset, durable: Option<LogOffset>, keys: Option<Arc<ClassicObjectKeys>>) -> Result<Box<dyn super::LogRange>> {
    if from.0 < 0 { return Err(unavailable(from)); }
    if to < from { return Err(Error::Task("log range end precedes its start".into())); }
    let _discovery = reads.admit_read(path.clone(), (from, to, durable))?;
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
    Ok(Box::new(FileRange { file, path, reads, maximum, next: from.0 as u64, end, keys }))
}

struct FileRange { file: Option<File>, path: Arc<PathBuf>, reads: Arc<IoRegion>, maximum: usize, next: u64, end: u64, keys: Option<Arc<ClassicObjectKeys>> }
#[async_trait]
impl super::LogRange for FileRange {
    fn allocation_bound(&self) -> usize { self.maximum.saturating_mul(if self.keys.is_some() { 2 } else { 1 }).saturating_add(256) }
    async fn next(&mut self, reservation: crate::object_store::ReadReservation) -> Result<Option<LogRecord>> {
        if reservation.length() < self.allocation_bound() { return Err(Error::Task("log read reservation is too small".into())); }
        if self.next >= self.end { return Ok(None); }
        let _read = self.reads.admit_read(self.path.clone(), (self.next, self.end))?;
        let file = self.file.as_mut().ok_or_else(|| unavailable(LogOffset(self.next as i64)))?;
        file.seek(SeekFrom::Start(self.next)).map_err(|error| Error::io(self.path.as_ref(), error))?;
        let record = read_record(file, &self.path, self.maximum, self.keys.as_deref())?;
        self.next = file.stream_position().map_err(|error| Error::io(self.path.as_ref(), error))?;
        record.retain(reservation).map(Some)
    }
}

#[derive(Clone, Copy)]
struct ReadOnlyCursor { after: LogOffset, next: u64, observed_length: u64 }

impl ReadOnlyCursor {
    fn observe(self, path: &Path, maximum: usize) -> Result<Option<(File, LogOffset, u64)>> {
        let Some(mut file) = open_existing(path)? else {
            return if self.observed_length == 0 { Ok(None) } else { Err(corrupt(path, self.next, "observed log disappeared")) };
        };
        let (latest, end) = latest_in_file(&mut file, path, maximum)?;
        if end < self.observed_length || end < self.next || latest < self.after {
            return Err(corrupt(path, end, "observed log was truncated"));
        }
        Ok(Some((file, latest, end)))
    }

    fn prepare_head(self, path: &Path, maximum: usize) -> Result<(Option<ReplayHead>, u64)> {
        let Some((mut file, latest, end)) = self.observe(path, maximum)? else { return Ok((None, 0)); };
        if latest == self.after { return Ok((None, end)); }
        if self.next > latest.0 as u64 { return Err(corrupt(path, self.next, "complete tip yielded no record")); }
        let header = read_record_header_at(&mut file, path, maximum, self.next, end)?;
        Ok((Some(header.replay_head()), end))
    }

    fn materialize_head(self, path: &Path, maximum: usize, head: ReplayHead, reservation: ReadReservation, keys: Option<&ClassicObjectKeys>) -> Result<(LogRecord, u64)> {
        if head.metadata.offset.0 != self.next as i64 {
            return Err(Error::Task("log replay head does not match the next position".into()));
        }
        let mut file = open_existing(path)?.ok_or_else(|| corrupt(path, self.next, "observed log disappeared"))?;
        let length = file.metadata().map_err(|error| Error::io(path, error))?.len();
        if length < self.observed_length { return Err(corrupt(path, length, "observed log was truncated")); }
        let latest = latest_at_length(&mut file, path, maximum, self.observed_length, TipInspection::CapturedPrefix)?;
        if head.metadata.offset > latest { return Err(Error::Task("log replay head exceeds the observed tip".into())); }
        let header = read_record_header_at(&mut file, path, maximum, self.next, self.observed_length)?;
        if header.replay_head() != head {
            return Err(corrupt(path, self.next, "log replay head changed before delivery"));
        }
        if reservation.length() < head.allocation_bound {
            return Err(Error::Task("log read reservation is too small".into()));
        }
        let record = read_record_body(&mut file, path, header, keys)?.retain(reservation)?;
        let next = file.stream_position().map_err(|error| Error::io(path, error))?;
        Ok((record, next))
    }
}

struct ReadOnlyTail {
    path: Arc<PathBuf>,
    options: ReadOnlyLocalLogOptions,
    cursor: ReadOnlyCursor,
    reads: Arc<IoRegion>,
    reserved_replay: bool,
    keys: Option<Arc<ClassicObjectKeys>>,
}

impl ReadOnlyTail {
    fn read_available(&mut self) -> Result<Vec<LogRecord>> {
        let _read = self.reads.admit_read(self.path.clone(), self.cursor.next)?;
        let Some((mut file, latest, end)) = self.cursor.observe(&self.path, self.options.maximum_message_bytes)? else { return Ok(Vec::new()); };
        self.cursor.observed_length = end;
        if latest == self.cursor.after { return Ok(Vec::new()); }
        let (records, next) = read_batch(&mut file, &self.path, &self.options, self.cursor.next, end, latest, self.keys.as_deref())?;
        let last = records.last().ok_or_else(|| corrupt(&self.path, self.cursor.next, "complete tip yielded no record"))?;
        self.cursor.after = last.metadata.offset;
        self.cursor.next = next;
        Ok(records)
    }

    async fn wait_to_poll(&self, deadline: tokio::time::Instant) -> bool {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() { return false; }
        tokio::time::sleep(remaining.min(self.options.poll_interval)).await;
        true
    }
}

fn poll_deadline(timeout: Duration) -> Result<tokio::time::Instant> {
    tokio::time::Instant::now().checked_add(timeout).ok_or_else(|| Error::Task("local log poll deadline overflow".into()))
}

#[async_trait]
impl LogTail for ReadOnlyTail {
    async fn poll(&mut self, timeout: Duration) -> Result<Vec<LogRecord>> {
        let deadline = poll_deadline(timeout)?;
        loop {
            let records = self.read_available()?;
            if !records.is_empty() { return Ok(records); }
            if !self.wait_to_poll(deadline).await { return Ok(Vec::new()); }
        }
    }
    fn replay(&mut self) -> Option<&mut dyn LogReplayTail> {
        if self.reserved_replay { Some(self) } else { None }
    }
}

#[async_trait]
impl LogReplayTail for ReadOnlyTail {
    async fn poll_head(&mut self, timeout: Duration) -> Result<Option<ReplayHead>> {
        let deadline = poll_deadline(timeout)?;
        loop {
            let maximum = self.options.maximum_message_bytes;
            let (head, observed_length) = self.reads.read(self.path.clone(), self.cursor, move |path, cursor| {
                cursor.prepare_head(path, maximum)
            }).await?;
            self.cursor.observed_length = observed_length;
            if head.is_some() { return Ok(head); }
            if !self.wait_to_poll(deadline).await { return Ok(None); }
        }
    }

    async fn read_head(&mut self, head: ReplayHead, reservation: ReadReservation) -> Result<LogRecord> {
        let maximum = self.options.maximum_message_bytes;
        let keys = self.keys.clone();
        let (record, next) = self.reads.read(self.path.clone(), self.cursor, move |path, cursor| {
            cursor.materialize_head(path, maximum, head, reservation, keys.as_deref())
        }).await?;
        self.cursor.after = record.metadata.offset;
        self.cursor.next = next;
        Ok(record)
    }
}
