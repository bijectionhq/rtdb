//! Durable storage: every stream's events in one sequence of shared segment files, in decision order.
//! Each record is framed as a big-endian u32 length, a CRC-32 of the body, and a prost body naming its stream by a server-wide id that a `Declare` record binds to the stream's name.
//! A round of writes, across every stream, is one write and at most one sync.
//! A compaction writes a base segment holding a snapshot of every stream and then deletes every earlier segment; replay starts at the last base segment.
//! A torn final record of the last segment is cut off on recovery, as it was never acknowledged.
//! A retained entry's payload stays in the segment its record was written to, and a reader reads it from there; memory holds only each entry's location and a bounded cache of payloads.

use super::payload::{Cache, Item, Location, Source, Stored};
use super::state::{Event, Held, StreamState, Window};
use super::store::{Slot, Store};
use crate::history::RetainedChain;
use crate::protocol::{Entries, Entry};
use bytes::{BufMut, Bytes, BytesMut};
use fs2::FileExt;
use parking_lot::Mutex;
use prost::Message;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::SystemTime;
use tokio::sync::{mpsc, oneshot};

const SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// A compaction writes its base segment in pieces of about this size.
const COMPACTION_WRITE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, PartialEq, Message)]
struct Snapshot {
    #[prost(uint64, tag = "1")]
    promised: u64,
    #[prost(uint64, tag = "2")]
    synced: u64,
    #[prost(sint64, tag = "3")]
    trim: i64,
    #[prost(sint64, tag = "4")]
    commit: i64,
    #[prost(bool, tag = "5")]
    retired: bool,
    #[prost(bytes = "bytes", tag = "6")]
    active_ensemble: Bytes,
}

#[derive(Clone, PartialEq, Message)]
struct ObservedSync {
    #[prost(sint64, tag = "1")]
    lsn: i64,
    #[prost(uint64, tag = "2")]
    epoch: u64,
    #[prost(sint64, tag = "3")]
    timestamp: i64,
    #[prost(sfixed64, tag = "4")]
    clock_upper_micros: i64,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum Kind {
    /// Binds the record's stream id to this name.
    #[prost(string, tag = "1")]
    Declare(String),
    #[prost(message, tag = "2")]
    Snapshot(Snapshot),
    #[prost(uint64, tag = "3")]
    Promise(u64),
    #[prost(uint64, tag = "4")]
    Synced(u64),
    #[prost(sint64, tag = "5")]
    Truncate(i64),
    #[prost(message, tag = "6")]
    Append(Entry),
    #[prost(sint64, tag = "7")]
    Commit(i64),
    #[prost(sint64, tag = "8")]
    Trim(i64),
    /// The first record of a base segment: every earlier segment is superseded.
    #[prost(bool, tag = "10")]
    Base(bool),
    #[prost(uint64, tag = "11")]
    Drop(u64),
    #[prost(message, tag = "13")]
    ObservedSync(ObservedSync),
}

#[derive(Clone, PartialEq, Message)]
struct Record {
    #[prost(uint64, tag = "9")]
    stream: u64,
    #[prost(oneof = "Kind", tags = "1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 13")]
    kind: Option<Kind>,
    // An activation is an optional refinement of an existing synced record.
    #[prost(bytes = "bytes", tag = "12")]
    active_ensemble: Bytes,
    // Only guarded entries name their local origin. This is persisted with
    // the entry itself, before the post-sync clock observation can exist.
    #[prost(bool, optional, tag = "14")]
    original_append: Option<bool>,
}

fn record(event: &Event) -> Kind {
    match event {
        Event::Promise(epoch) => Kind::Promise(*epoch),
        Event::Synced(epoch) => Kind::Synced(epoch.unwrap_or(0)),
        Event::Activate { epoch, .. } => Kind::Synced(*epoch),
        Event::Truncate(from) => Kind::Truncate(*from),
        Event::Append(entry) => Kind::Append(entry.clone()),
        Event::Commit(lsn) => Kind::Commit(*lsn),
        Event::Trim(through) => Kind::Trim(*through),
        Event::Drop(epoch) => Kind::Drop(*epoch),
    }
}

fn frame(stream: u64, kind: Kind, buffer: &mut BytesMut) {
    frame_record(Record { stream, kind: Some(kind), active_ensemble: Bytes::new(), original_append: None }, buffer)
}

fn frame_record(record: Record, buffer: &mut BytesMut) {
    let body = record.encode_to_vec();
    buffer.put_u32(body.len() as u32);
    buffer.put_u32(crc32fast::hash(&body));
    buffer.put_slice(&body);
}

fn io_error(path: &Path, error: std::io::Error) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.logkeeper/io", format!("I/O at {}: {error}", path.display()))
}

fn corrupt(path: &Path, reason: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.logkeeper/corrupt-journal", format!("journal {}: {reason}", path.display()))
}

/// A stream's state as of a point in the journal's order, for a base segment.
pub(crate) struct Captured {
    pub slot: Arc<Slot>,
    pub state: StreamState<Stored>,
}

pub(crate) enum Write {
    Events(Arc<Slot>, Vec<Event>),
    /// Every stream, captured with no decision of any of them between the capture and this work in the queue.
    Compact(Vec<Captured>),
}

pub(crate) struct Work {
    pub write: Write,
    pub done: oneshot::Sender<std::result::Result<(), String>>,
    pub _admission: Option<tokio::sync::OwnedSemaphorePermit>,
    pub authority_revision: Option<u64>,
    pub observed_append: Option<(i64, Arc<OnceLock<i64>>)>,
}

/// The segment being appended to; touched only by the journal's rounds.
struct Current {
    file: Arc<File>,
    path: PathBuf,
    sequence: u64,
    bytes: u64,
    /// Streams whose `Declare` is in a segment from the last base on.
    declared: HashSet<u64>,
}

pub(crate) struct Journal {
    segments: PathBuf,
    fsync: bool,
    _lock: File,
    current: Mutex<Current>,
    /// Bytes in the segments from the last base on.
    bytes: AtomicU64,
    next_id: AtomicU64,
    compaction_queued: AtomicBool,
    compaction_finished: tokio::sync::Notify,
    compacted: Mutex<HashMap<u64, u64>>,
    /// A handle on every segment from the last base on, for reads.
    files: Mutex<BTreeMap<u64, Arc<File>>>,
    cache: Mutex<Cache>,
}

pub(crate) struct Recovered {
    pub id: u64,
    pub name: String,
    pub state: StreamState<Stored>,
}

/// An `Append` record in a round's buffer, whose entry learns its location once the round is written.
struct Placed<'a> {
    slot: &'a Arc<Slot>,
    entry: &'a Entry,
    start: usize,
    length: usize,
    observation: Option<&'a Arc<OnceLock<i64>>>,
}

fn clock_upper_micros() -> Option<i64> {
    i64::try_from(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).ok()?.as_micros())
        .ok()?.checked_add(1)
}

#[cfg(unix)]
fn read_exact_at(file: &File, bytes: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt as _;
    file.read_exact_at(bytes, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    use std::os::windows::fs::FileExt as _;

    // Reads always supply their offset; segment writes use append-only
    // handles, so moving the Windows file cursor cannot redirect a write.
    while !bytes.is_empty() {
        match file.seek_read(bytes, offset) {
            Ok(0) => return Err(Error::new(ErrorKind::UnexpectedEof, "incomplete journal frame")),
            Ok(read) => {
                offset = offset.checked_add(read as u64)
                    .ok_or_else(|| Error::new(ErrorKind::InvalidData, "journal offset overflow"))?;
                bytes = &mut bytes[read..];
            }
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn segment_path(segments: &Path, sequence: u64) -> PathBuf { segments.join(format!("{sequence:020}.seg")) }

fn sync_directory(directory: &Path, fsync: bool) -> Result<()> {
    if !fsync { return Ok(()); }
    rtdb_storage::sync_directory(directory).map_err(|error| io_error(directory, error))
}

fn list_segments(segments: &Path, remove_temporary: bool) -> Result<BTreeMap<u64, PathBuf>> {
    let mut sequences = BTreeMap::new();
    for entry in fs::read_dir(segments).map_err(|error| io_error(segments, error))? {
        let path = entry.map_err(|error| io_error(segments, error))?.path();
        match path.extension().and_then(|extension| extension.to_str()) {
            Some("seg") => {
                let sequence = path.file_stem().and_then(|stem| stem.to_str()).and_then(|stem| stem.parse::<u64>().ok()).ok_or_else(|| corrupt(&path, "not a segment name"))?;
                if sequences.insert(sequence, path.clone()).is_some() {
                    return Err(corrupt(&path, format!("duplicate segment sequence {sequence}")));
                }
            }
            Some("tmp") if remove_temporary => fs::remove_file(&path).map_err(|error| io_error(&path, error))?,
            _ => (),
        }
    }
    Ok(sequences)
}

fn open_segment(path: &Path) -> Result<Arc<File>> {
    OpenOptions::new().create(true).read(true).append(true).open(path).map(Arc::new).map_err(|error| io_error(path, error))
}

/// Whether a segment starts with a `Base` record.
fn is_base(path: &Path) -> Result<bool> {
    let file = File::open(path).map_err(|error| io_error(path, error))?;
    let length = file.metadata().map_err(|error| io_error(path, error))?.len();
    let mut reader = BufReader::new(file);
    Ok(matches!(read_record(&mut reader, length).map_err(|error| io_error(path, error))?, Some((Record { kind: Some(Kind::Base(_)), .. }, _))))
}

fn current_chain(segments: &Path, remove_temporary: bool) -> Result<Vec<(u64, PathBuf)>> {
    let mut chain = Vec::new();
    for (sequence, path) in list_segments(segments, remove_temporary)?.into_iter().rev() {
        let base = is_base(&path)?;
        chain.push((sequence, path));
        if base { break; }
    }
    chain.reverse();
    Ok(chain)
}

/// Reads the next whole record of a segment of `remaining` bytes from here, with its framed length; none at the first incomplete or damaged one.
fn read_record(reader: &mut impl std::io::Read, remaining: u64) -> std::io::Result<Option<(Record, u64)>> {
    if remaining < 8 { return Ok(None); }
    let mut header = [0u8; 8];
    reader.read_exact(&mut header)?;
    let length = u32::from_be_bytes(header[0..4].try_into().expect("four bytes")) as u64;
    let checksum = u32::from_be_bytes(header[4..8].try_into().expect("four bytes"));
    if remaining - 8 < length { return Ok(None); }
    let mut body = vec![0; length as usize];
    reader.read_exact(&mut body)?;
    if crc32fast::hash(&body) != checksum { return Ok(None); }
    Ok(Record::decode(Bytes::from(body)).ok().map(|record| (record, 8 + length)))
}

fn remove_before(segments: &Path, sequence: u64) -> Result<()> {
    for (_, path) in list_segments(segments, true)?.range(..sequence) { fs::remove_file(path).map_err(|error| io_error(path, error))?; }
    Ok(())
}

fn read_identity(path: &Path, length: usize) -> Result<Option<String>> {
    use std::io::Read as _;

    let input = match File::open(path) {
        Ok(input) => input,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(path, error)),
    };
    let mut identity = String::new();
    input.take((length + 1) as u64).read_to_string(&mut identity)
        .map_err(|error| io_error(path, error))?;
    if identity.len() != length { return Err(corrupt(path, "invalid journal identity")); }
    Ok(Some(identity))
}

fn write_identity(data_dir: &Path, path: &Path, identity: &str, fsync: bool) -> Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new().create(true).truncate(true).write(true)
        .open(&temporary).map_err(|error| io_error(&temporary, error))?;
    file.write_all(identity.as_bytes()).map_err(|error| io_error(&temporary, error))?;
    if fsync { rtdb_storage::sync_file(&file).map_err(|error| io_error(&temporary, error))?; }
    fs::rename(&temporary, path).map_err(|error| io_error(path, error))?;
    sync_directory(data_dir, fsync)
}

pub(super) enum MemberIdentity {
    Open(String),
    Reopen(String),
}

impl Journal {
    /// Replays the segments from the last base on, deleting any earlier ones a crash left behind, and opens the last for appending.
    pub fn open(
        data_dir: &Path,
        fsync: bool,
        cache_bytes: usize,
        member: Option<MemberIdentity>,
    ) -> Result<(Self, Vec<Recovered>, String)> {
        let segments = data_dir.join("segments");
        if !matches!(member, Some(MemberIdentity::Reopen(_))) {
            fs::create_dir_all(&segments).map_err(|error| io_error(&segments, error))?;
        }
        let lock_path = data_dir.join("LOCK");
        let lock = OpenOptions::new().create(true).truncate(false).write(true).open(&lock_path).map_err(|error| io_error(&lock_path, error))?;
        lock.try_lock_exclusive().map_err(|error| io_error(&lock_path, error))?;
        let files = list_segments(&segments, true)?;
        let member_path = data_dir.join("MEMBER");
        let local_path = data_dir.join("JOURNAL_ID");
        let (identity_path, candidate) = match member.as_ref() {
            Some(MemberIdentity::Open(candidate) | MemberIdentity::Reopen(candidate)) => {
                if local_path.try_exists().map_err(|error| io_error(&local_path, error))? {
                    return Err(corrupt(&local_path, "an anonymous journal cannot register as a member"));
                }
                (&member_path, candidate.clone())
            }
            None => {
                if member_path.try_exists().map_err(|error| io_error(&member_path, error))? {
                    return Err(corrupt(&member_path, "a member journal requires its member configuration"));
                }
                (&local_path, uuid::Uuid::new_v4().to_string())
            }
        };
        let identity = read_identity(identity_path, candidate.len())?;
        if matches!(member, Some(MemberIdentity::Reopen(_))) && identity.as_deref() != Some(candidate.as_str()) {
            return Err(corrupt(identity_path, "the original journal member identity is missing or different"));
        }
        if identity.is_some() && files.is_empty() {
            return Err(corrupt(identity_path, "the installed journal has lost its segments"));
        }
        if member.is_some() && identity.is_none() {
            // A crash can leave the initial empty segment before the member file is installed.
            let empty_first_segment = files.len() == 1 && files.get(&0).is_some_and(|path|
                fs::metadata(path).is_ok_and(|metadata| metadata.len() == 0));
            if !files.is_empty() && !empty_first_segment {
                return Err(corrupt(&member_path, "the journal has no installed member identity"));
            }
        }
        let mut chain = Vec::new();
        for (sequence, path) in files.into_iter().rev() {
            let base = is_base(&path)?;
            chain.push((sequence, path));
            if base { break; }
        }
        chain.reverse();
        if let Some((first, _)) = chain.first() {
            remove_before(&segments, *first)?;
            sync_directory(&segments, fsync)?;
        }
        let mut replay = Replay::default();
        let mut total = 0u64;
        let mut files = BTreeMap::new();
        let last_index = chain.len().checked_sub(1);
        for (index, (sequence, path)) in chain.iter().enumerate() {
            let (end, length) = replay.segment(*sequence, path, None)?;
            if end < length {
                if Some(index) != last_index { return Err(corrupt(path, format!("a damaged record at {end} before the last segment"))); }
                let file = OpenOptions::new().write(true).open(path).map_err(|error| io_error(path, error))?;
                file.set_len(end).and_then(|_| rtdb_storage::sync_file(&file)).map_err(|error| io_error(path, error))?;
            }
            if Some(index) != last_index { files.insert(*sequence, Arc::new(File::open(path).map_err(|error| io_error(path, error))?)); }
            total += end;
        }
        let (sequence, path) = chain.last().map_or_else(|| (0, segment_path(&segments, 0)), |(sequence, path)| (*sequence, path.clone()));
        let file = open_segment(&path)?;
        files.insert(sequence, Arc::clone(&file));
        if chain.is_empty() { sync_directory(&segments, fsync)?; }
        let bytes = file.metadata().map_err(|error| io_error(&path, error))?.len();
        let declared = replay.names.keys().copied().collect();
        let next_id = replay.names.keys().max().map_or(0, |id| id + 1);
        let recovered = replay.names.into_iter().map(|(id, name)| Recovered { id, name, state: replay.states.remove(&id).unwrap_or_default() }).collect();
        let identity = if let Some(identity) = identity {
            identity
        } else {
            if fsync { rtdb_storage::sync_file(&file).map_err(|error| io_error(&path, error))?; }
            sync_directory(&segments, fsync)?;
            write_identity(data_dir, identity_path, &candidate, fsync)?;
            candidate
        };
        let journal = Self {
            segments,
            fsync,
            _lock: lock,
            current: Mutex::new(Current { file, path, sequence, bytes, declared }),
            bytes: AtomicU64::new(total),
            next_id: AtomicU64::new(next_id),
            compaction_queued: AtomicBool::new(false),
            compaction_finished: tokio::sync::Notify::new(),
            compacted: Mutex::new(replay.compacted),
            files: Mutex::new(files),
            cache: Mutex::new(Cache::new(cache_bytes)),
        };
        Ok((journal, recovered, identity))
    }

    pub fn allocate_id(&self) -> u64 { self.next_id.fetch_add(1, Ordering::Relaxed) }

    pub fn bytes(&self) -> u64 { self.bytes.load(Ordering::Relaxed) }

    /// Claims the one compaction that may be queued at a time; the journal releases it once the compaction's round is written.
    pub fn claim_compaction(&self) -> bool { !self.compaction_queued.swap(true, Ordering::AcqRel) }

    pub fn release_compaction(&self) {
        self.compaction_queued.store(false, Ordering::Release);
        self.compaction_finished.notify_waiters();
    }

    pub fn compaction_finished(&self) -> impl std::future::Future<Output = ()> + '_ {
        self.compaction_finished.notified()
    }

    pub fn is_durable(&self) -> bool { self.fsync }

    pub fn compacted_retirement(&self, stream: u64, epoch: u64) -> bool {
        self.compacted.lock().get(&stream).is_some_and(|stamp| *stamp >= epoch)
    }

    /// Writes one round: every work in order, then one sync of the segment.
    /// Commit records are synced like any other: a sealed loglet's commit lsn is written once, by the move that ends it, and no later writer re-establishes it.
    fn write_round(&self, works: &[Work]) -> Result<()> {
        let mut current = self.current.lock();
        let mut buffer = BytesMut::new();
        let mut placed = Vec::new();
        let mut sync = false;
        for work in works {
            match &work.write {
                Write::Events(_, events) if events.is_empty() => (),
                Write::Events(slot, events) => {
                    if current.declared.insert(slot.id) { frame(slot.id, Kind::Declare(slot.name.clone()), &mut buffer); }
                    for event in events {
                        let start = buffer.len();
                        let active_ensemble = match event {
                            Event::Activate { ensemble, .. } => Bytes::copy_from_slice(ensemble),
                            _ => Bytes::new(),
                        };
                        let observation = match event {
                            Event::Append(entry) => work.observed_append.as_ref().filter(|(lsn, _)| *lsn == entry.lsn)
                                .map(|(_, sample)| sample),
                            _ => None,
                        };
                        let original_append = matches!(event, Event::Append(entry) if entry.guarded).then_some(observation.is_some());
                        frame_record(Record { stream: slot.id, kind: Some(record(event)), active_ensemble, original_append }, &mut buffer);
                        if let Event::Append(entry) = event {
                            placed.push(Placed { slot, entry, start, length: buffer.len() - start, observation });
                        }
                    }
                    sync = true;
                }
                Write::Compact(captured) => {
                    self.flush(&mut current, &buffer, sync, &placed)?;
                    buffer.clear();
                    placed.clear();
                    sync = false;
                    self.compact(&mut current, captured)?;
                }
            }
        }
        self.flush(&mut current, &buffer, sync, &placed)
    }

    /// Appends a buffer, then points each entry it placed at its record.
    /// An entry already replaced by a later decision keeps that decision's payload, which a later record carries.
    fn flush(&self, current: &mut Current, buffer: &[u8], sync: bool, placed: &[Placed]) -> Result<()> {
        let (segment, base) = (current.sequence, current.bytes);
        self.append(current, buffer, sync)?;
        let observed_at = (self.fsync && placed.iter().any(|placed| placed.observation.is_some()))
            .then(clock_upper_micros).flatten();
        let mut witnesses = BytesMut::new();
        let mut observations = Vec::new();
        for placed in placed {
            let entry = placed.entry;
            let location = Location { segment, offset: base + placed.start as u64, length: placed.length as u32, payload: entry.payload.len() as u32 };
            let mut state = placed.slot.state.lock();
            let Some(held) = state.held_mut(entry.lsn) else { continue };
            let same = held.epoch == entry.epoch && held.timestamp == entry.timestamp
                && held.guarded == entry.guarded
                && matches!(&held.payload, Stored::Pending(payload) if payload.as_ptr() == entry.payload.as_ptr() && payload.len() == entry.payload.len());
            if same {
                held.payload = Stored::Written(location);
                self.cache.lock().insert(location, &entry.payload);
                if let Some(clock_upper_micros) = observed_at
                    && let Some(sample) = placed.observation
                    && held.observed_sync.as_ref().is_some_and(|owner| Arc::ptr_eq(owner, sample))
                {
                    frame(placed.slot.id, Kind::ObservedSync(ObservedSync { lsn: entry.lsn,
                        epoch: entry.epoch, timestamp: entry.timestamp, clock_upper_micros }), &mut witnesses);
                    observations.push(sample);
                }
            }
        }
        self.append(current, &witnesses, true)?;
        if let Some(observed_at) = observed_at {
            for sample in observations { let _ = sample.set(observed_at); }
        }
        Ok(())
    }

    fn append(&self, current: &mut Current, buffer: &[u8], sync: bool) -> Result<()> {
        if buffer.is_empty() { return Ok(()); }
        current.file.write_all(buffer).map_err(|error| io_error(&current.path, error))?;
        if sync { self.sync(&current.file, &current.path)?; }
        current.bytes += buffer.len() as u64;
        self.bytes.fetch_add(buffer.len() as u64, Ordering::Relaxed);
        if current.bytes >= SEGMENT_BYTES {
            if !sync { self.sync(&current.file, &current.path)?; }
            let sequence = current.sequence + 1;
            let path = segment_path(&self.segments, sequence);
            let file = open_segment(&path)?;
            sync_directory(&self.segments, self.fsync)?;
            self.files.lock().insert(sequence, Arc::clone(&file));
            *current = Current { file, path, sequence, bytes: 0, declared: std::mem::take(&mut current.declared) };
        }
        Ok(())
    }

    /// Writes a base segment holding every captured stream, copying each retained payload from where it is, then points each stream's entries at their copies and deletes every earlier segment.
    fn compact(&self, current: &mut Current, captured: &[Captured]) -> Result<()> {
        let sequence = current.sequence + 1;
        let path = segment_path(&self.segments, sequence);
        let temporary = path.with_extension("tmp");
        let mut file = File::create(&temporary).map_err(|error| io_error(&temporary, error))?;
        let mut buffer = BytesMut::new();
        let mut written = 0u64;
        let mut copies = Vec::with_capacity(captured.len());
        frame(0, Kind::Base(true), &mut buffer);
        for stream in captured {
            let (id, state) = (stream.slot.id, &stream.state);
            frame(id, Kind::Declare(stream.slot.name.clone()), &mut buffer);
            frame(id, Kind::Snapshot(Snapshot {
                promised: state.promised, synced: state.synced.unwrap_or(0), trim: state.trim, commit: state.commit, retired: state.dropped,
                active_ensemble: state.active_ensemble.as_ref().map_or_else(Bytes::new, |ensemble| Bytes::copy_from_slice(ensemble)),
            }), &mut buffer);
            let mut locations = Vec::with_capacity(state.entries.len());
            for (held, lsn) in state.entries.iter().zip(state.trim + 1..) {
                let payload = match &held.payload {
                    Stored::Pending(payload) => payload.clone(),
                    Stored::Written(location) => self.payload(lsn, *location)?,
                };
                let start = buffer.len();
                let length = payload.len() as u32;
                let entry = Entry { lsn, epoch: held.epoch, timestamp: held.timestamp, guarded: held.guarded, payload };
                let original_append = held.guarded.then_some(held.observed_sync.is_some());
                frame_record(Record { stream: id, kind: Some(Kind::Append(entry)), active_ensemble: Bytes::new(), original_append }, &mut buffer);
                locations.push(Location { segment: sequence, offset: written + start as u64, length: (buffer.len() - start) as u32, payload: length });
                if let Some(clock_upper_micros) = held.observed_sync.as_ref().and_then(|sample| sample.get().copied()) {
                    frame(id, Kind::ObservedSync(ObservedSync { lsn, epoch: held.epoch,
                        timestamp: held.timestamp, clock_upper_micros }), &mut buffer);
                }
                if buffer.len() >= COMPACTION_WRITE_BYTES {
                    file.write_all(&buffer).map_err(|error| io_error(&temporary, error))?;
                    written += buffer.len() as u64;
                    buffer.clear();
                }
            }
            copies.push(locations);
        }
        file.write_all(&buffer).map_err(|error| io_error(&temporary, error))?;
        written += buffer.len() as u64;
        self.sync(&file, &temporary)?;
        drop(file);
        fs::rename(&temporary, &path).map_err(|error| io_error(&path, error))?;
        sync_directory(&self.segments, self.fsync)?;
        let file = open_segment(&path)?;
        self.files.lock().insert(sequence, Arc::clone(&file));
        *current = Current { file, path, sequence, bytes: written, declared: captured.iter().map(|stream| stream.slot.id).collect() };
        self.bytes.store(written, Ordering::Relaxed);
        for (stream, locations) in captured.iter().zip(copies) { self.moved(stream, &locations); }
        remove_before(&self.segments, sequence)?;
        self.files.lock().retain(|segment, _| *segment >= sequence);
        sync_directory(&self.segments, self.fsync)?;
        *self.compacted.lock() = captured.iter().filter(|stream| stream.state.dropped)
            .map(|stream| (stream.slot.id, stream.state.promised)).collect();
        Ok(())
    }

    /// Points a captured stream's entries at their copies in the base segment.
    /// Every entry decided before the capture has been written by now and every one decided after is still pending, so a written entry is the one captured.
    fn moved(&self, stream: &Captured, locations: &[Location]) {
        let mut state = stream.slot.state.lock();
        for ((captured, lsn), copy) in stream.state.entries.iter().zip(stream.state.trim + 1..).zip(locations) {
            let Some(held) = state.held_mut(lsn) else { continue };
            let Stored::Written(old) = held.payload else { continue };
            if (held.epoch, held.timestamp) != (captured.epoch, captured.timestamp) { continue; }
            held.payload = Stored::Written(*copy);
            self.cache.lock().moved(old, *copy);
        }
    }

    /// Where each entry's payload is to be read from, under its stream's lock.
    pub fn source(&self, stored: &Stored) -> Source {
        match stored {
            Stored::Pending(payload) => Source::Ready(payload.clone()),
            Stored::Written(location) => match self.cache.lock().get(*location) {
                Some(payload) => Source::Ready(payload),
                None => Source::Disk(self.files.lock().get(&location.segment).cloned(), *location),
            },
        }
    }

    /// Reads a written payload, from the cache or from its segment.
    fn payload(&self, lsn: i64, location: Location) -> Result<Bytes> {
        if let Some(payload) = self.cache.lock().get(location) { return Ok(payload); }
        let file = self.files.lock().get(&location.segment).cloned();
        self.read(file.as_deref(), lsn, location)
    }

    fn read(&self, file: Option<&File>, lsn: i64, location: Location) -> Result<Bytes> {
        let path = segment_path(&self.segments, location.segment);
        let file = file.ok_or_else(|| Anomaly::fault("rtdb.logkeeper/segment-retired", format!("lsn {lsn} points at retired segment {}", path.display())))?;
        read_payload(file, &path, lsn, location)
    }

    /// Reads every payload a window still needs from its segment, caching what it reads; blocks.
    pub fn resolve(&self, window: Window<Item>) -> Result<Entries> {
        let mut entries = Vec::with_capacity(window.items.len());
        for item in window.items {
            let payload = match item.source {
                Source::Ready(payload) => payload,
                Source::Disk(file, location) => {
                    let payload = self.read(file.as_deref(), item.lsn, location)?;
                    self.cache.lock().insert(location, &payload);
                    payload
                }
            };
            entries.push(Entry { lsn: item.lsn, epoch: item.epoch, timestamp: item.timestamp, guarded: item.guarded, payload });
        }
        Ok(Entries { entries, commit_lsn: window.commit_lsn, trim_lsn: window.trim_lsn, last_lsn: window.last_lsn })
    }

    #[cfg(test)]
    pub fn cache_bytes(&self) -> usize { self.cache.lock().bytes() }

    fn sync(&self, file: &File, path: &Path) -> Result<()> {
        if self.fsync { rtdb_storage::sync_file(file).map_err(|error| io_error(path, error))?; }
        Ok(())
    }
}

#[derive(Default)]
struct Replay {
    names: HashMap<u64, String>,
    states: HashMap<u64, StreamState<Stored>>,
    compacted: HashMap<u64, u64>,
}

impl Replay {
    /// Applies a segment's records, reading it once in order, returning where the last whole record ends and the segment's length.
    fn segment(&mut self, sequence: u64, path: &Path, selected_stream: Option<&str>) -> Result<(u64, u64)> {
        let file = File::open(path).map_err(|error| io_error(path, error))?;
        let length = file.metadata().map_err(|error| io_error(path, error))?.len();
        let mut reader = BufReader::with_capacity(1 << 20, file);
        let mut position = 0u64;
        while let Some((Record { stream, kind, active_ensemble, original_append }, framed)) = read_record(&mut reader, length - position).map_err(|error| io_error(path, error))? {
            let at = position;
            position += framed;
            let kind = kind.ok_or_else(|| corrupt(path, "a record without a kind"))?;
            if let Kind::Declare(name) = &kind {
                if selected_stream.is_some_and(|selected| name != selected) { continue; }
                self.names.insert(stream, name.clone());
                self.states.entry(stream).or_default();
                continue;
            }
            if let Kind::Base(_) = kind { continue; }
            let state = match self.states.get_mut(&stream) {
                Some(state) => state,
                None if selected_stream.is_some() => continue,
                None => return Err(corrupt(path, format!("stream {stream} was never declared"))),
            };
            match kind {
                Kind::Declare(_) | Kind::Base(_) => unreachable!("handled above"),
                Kind::Snapshot(snapshot) => {
                    if snapshot.retired { self.compacted.insert(stream, snapshot.promised); }
                    *state = StreamState {
                        promised: snapshot.promised, synced: (snapshot.synced != 0).then_some(snapshot.synced),
                        active_ensemble: decode_ensemble(path, &snapshot.active_ensemble)?,
                        trim: snapshot.trim, commit: snapshot.commit, dropped: snapshot.retired, ..StreamState::default()
                    };
                }
                Kind::Promise(epoch) => state.apply(&Event::Promise(epoch)),
                Kind::Synced(epoch) => {
                    state.apply(&Event::Synced((epoch != 0).then_some(epoch)));
                    if let Some(ensemble) = decode_ensemble(path, &active_ensemble)? {
                        if epoch == 0 || epoch != state.promised || state.active_ensemble.is_some_and(|active| active != ensemble) {
                            return Err(corrupt(path, "activation does not match the stream's promised epoch or ensemble"));
                        }
                        state.apply(&Event::Activate { epoch, ensemble });
                    }
                }
                Kind::Truncate(from) => state.apply(&Event::Truncate(from)),
                Kind::Append(entry) => {
                    if entry.lsn != state.last() + 1 { return Err(corrupt(path, format!("stream {stream}'s entry {} follows {}", entry.lsn, state.last()))); }
                    if entry.guarded != original_append.is_some() { return Err(corrupt(path, "entry kind and append provenance disagree")); }
                    let location = Location { segment: sequence, offset: at, length: framed as u32, payload: entry.payload.len() as u32 };
                    state.append_held(entry.lsn, Held { epoch: entry.epoch, timestamp: entry.timestamp,
                        guarded: entry.guarded, payload: Stored::Written(location),
                        observed_sync: (original_append == Some(true)).then(|| Arc::new(OnceLock::new())) });
                }
                Kind::ObservedSync(observation) => {
                    let sample = state.held(observation.lsn).filter(|entry| entry.guarded
                        && entry.epoch == observation.epoch && entry.timestamp == observation.timestamp)
                        .and_then(|entry| entry.observed_sync.as_ref());
                    if observation.clock_upper_micros <= 0 || !sample.is_some_and(|sample| sample.set(observation.clock_upper_micros).is_ok()) {
                        return Err(corrupt(path, "invalid sync observation"));
                    }
                }
                Kind::Commit(lsn) => state.apply(&Event::Commit(lsn)),
                Kind::Trim(through) => state.apply(&Event::Trim(through)),
                Kind::Drop(epoch) => state.apply(&Event::Drop(epoch)),
            }
        }
        Ok((position, length))
    }
}

fn decode_ensemble(path: &Path, encoded: &[u8]) -> Result<Option<[u8; 32]>> {
    if encoded.is_empty() { return Ok(None); }
    encoded.try_into().map(Some).map_err(|_| corrupt(path, "invalid active ensemble digest"))
}

/// Stops at the first incomplete or damaged record.
fn next_record(bytes: &Bytes, position: &mut usize) -> Option<Record> {
    let header = bytes.get(*position..*position + 8)?;
    let length = u32::from_be_bytes(header[0..4].try_into().expect("four bytes")) as usize;
    let checksum = u32::from_be_bytes(header[4..8].try_into().expect("four bytes"));
    let start = *position + 8;
    let body = bytes.get(start..start.checked_add(length)?)?;
    if crc32fast::hash(body) != checksum { return None; }
    let record = Record::decode(bytes.slice(start..start + length)).ok()?;
    *position = start + length;
    Some(record)
}

fn read_payload(file: &File, path: &Path, lsn: i64, location: Location) -> Result<Bytes> {
    let mut frame = vec![0; location.length as usize];
    read_exact_at(file, &mut frame, location.offset).map_err(|error| io_error(path, error))?;
    match next_record(&Bytes::from(frame), &mut 0) {
        Some(Record { kind: Some(Kind::Append(entry)), .. }) if entry.lsn == lsn => Ok(entry.payload),
        _ => Err(corrupt(path, format!("no entry {lsn} at {}", location.offset))),
    }
}

/// A fingerprint of the committed interval read from a stopped logkeeper.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalDigest { pub stream: String, pub interval: [i64; 2], pub digest: String }

impl JournalDigest {
    pub fn from_digest_bytes(stream: String, interval: [i64; 2], digest: [u8; 32]) -> Self {
        Self { stream, interval, digest: RetainedChain::from_bytes(digest).digest() }
    }

    pub fn digest_bytes(&self) -> Option<[u8; 32]> {
        RetainedChain::continue_from(&self.digest).map(|digest| digest.bytes())
    }
}

/// Reads a stopped logkeeper's journal without changing its files.
pub fn inspect_retained(data_dir: &Path, stream: &str, requested: Option<[i64; 2]>) -> Result<Option<JournalDigest>> {
    inspect_retained_with_prefix(data_dir, stream, requested, None)
}

/// Extends a retained digest using the committed entries immediately after its interval.
pub fn inspect_retained_with_prefix(data_dir: &Path, stream: &str, requested: Option<[i64; 2]>, prefix: Option<&JournalDigest>) -> Result<Option<JournalDigest>> {
    let lock_path = data_dir.join("LOCK");
    let lock = File::open(&lock_path).map_err(|error| io_error(&lock_path, error))?;
    FileExt::try_lock_shared(&lock).map_err(|error| io_error(&lock_path, error))?;
    let segments = data_dir.join("segments");
    let chain = current_chain(&segments, false)?;
    if chain.windows(2).any(|pair| pair[0].0.checked_add(1) != Some(pair[1].0)) {
        return Err(corrupt(&segments, "the journal's current chain has a missing segment"));
    }
    let mut replay = Replay::default();
    for (sequence, path) in &chain {
        let (end, length) = replay.segment(*sequence, path, Some(stream))?;
        if end != length { return Err(corrupt(path, format!("a damaged record at {end}"))); }
    }
    let mut ids = replay.names.iter().filter(|(_, name)| name.as_str() == stream).map(|(id, _)| *id);
    let id = ids.next()
        .ok_or_else(|| Anomaly::incorrect("rtdb.logkeeper/unknown-stream", format!("The journal has no stream named {stream}")))?;
    if ids.next().is_some() { return Err(corrupt(&segments, format!("more than one stream id names {stream}"))); }
    let state = replay.states.remove(&id).expect("each declared stream has state");
    if state.commit > state.last() { return Err(corrupt(&segments, format!("the commit position for {stream} exceeds its last record"))); }
    let retained = state.trim.saturating_add(1);
    let through = state.commit;
    let [first, last] = match requested {
        Some([first, last]) if first >= retained && last <= through && first <= last => [first, last],
        Some(_) => return Err(Anomaly::incorrect("rtdb.logkeeper/audit-range", format!("The journal's retained committed interval is {retained} through {through}"))),
        None if retained > through && prefix.is_none() => return Ok(None),
        None if retained > through => return Err(Anomaly::incorrect("rtdb.logkeeper/audit-range", "The journal has no committed suffix to audit")),
        None => [retained, through],
    };
    let (chain_first, mut digest) = if let Some(prefix) = prefix {
        let [prefix_first, prefix_last] = prefix.interval;
        if prefix.stream != stream || prefix_first < 0 || prefix_first > prefix_last || prefix_last.checked_add(1) != Some(first) {
            return Err(Anomaly::incorrect("rtdb.logkeeper/audit-prefix", "The prefix must name this stream and end immediately before the audited interval"));
        }
        let digest = RetainedChain::continue_from(&prefix.digest)
            .ok_or_else(|| Anomaly::incorrect("rtdb.logkeeper/audit-prefix", "The prefix digest must be a lowercase SHA-256 value"))?;
        (prefix_first, digest)
    } else {
        (first, RetainedChain::new(stream, first))
    };
    let paths: BTreeMap<_, _> = chain.into_iter().collect();
    let mut file: Option<(u64, File)> = None;
    for lsn in first..=last {
        let held = state.entries.get((lsn - retained) as usize).ok_or_else(|| corrupt(&segments, format!("missing committed position {lsn}")))?;
        let Stored::Written(location) = held.payload else { return Err(corrupt(&segments, format!("unfinished committed position {lsn}"))); };
        let path = paths.get(&location.segment).ok_or_else(|| corrupt(&segments, format!("missing segment for position {lsn}")))?;
        if file.as_ref().is_none_or(|(sequence, _)| *sequence != location.segment) {
            file = Some((location.segment, File::open(path).map_err(|error| io_error(path, error))?));
        }
        let payload = read_payload(&file.as_ref().expect("opened segment").1, path, lsn, location)?;
        digest.append(&Entry { lsn, epoch: held.epoch, timestamp: held.timestamp, guarded: held.guarded, payload });
    }
    Ok(Some(JournalDigest { stream: stream.to_owned(), interval: [chain_first, last], digest: digest.digest() }))
}

/// Group commit: whatever has queued while the previous round was syncing goes into the next round together, across every stream.
pub(crate) async fn run(journal: Arc<Journal>, store: Weak<Store>, mut queue: mpsc::UnboundedReceiver<Work>) -> Result<()> {
    const MAXIMUM_ROUND: usize = 8192;
    while let Some(first) = queue.recv().await {
        let mut works = vec![first];
        while works.len() < MAXIMUM_ROUND {
            match queue.try_recv() { Ok(work) => works.push(work), Err(_) => break }
        }
        let journal = Arc::clone(&journal);
        let (works, result) = tokio::task::spawn_blocking(move || {
            let result = journal.write_round(&works);
            if works.iter().any(|work| matches!(work.write, Write::Compact(_))) { journal.release_compaction(); }
            (works, result)
        }).await.map_err(|error| Anomaly::fault("rtdb.logkeeper/journal-task", error.to_string()))?;
        match result {
            Ok(()) => {
                let mut released = false;
                for work in works {
                    match &work.write {
                        Write::Events(slot, events) => {
                            if let Some(revision) = work.authority_revision {
                                slot.publish_authority_revision(revision);
                            }
                            released |= events.iter().any(|event| matches!(event, Event::Trim(_) | Event::Drop(_)));
                        }
                        Write::Compact(_) => released = false,
                    }
                    let observed = work.observed_append.as_ref().is_none_or(|(_, sample)| sample.get().is_some());
                    let _ = work.done.send(if observed { Ok(()) } else { Err("the guarded append has no retained sync observation".into()) });
                }
                if released && let Some(store) = store.upgrade() { store.compact_if_mostly_dead(); }
            }
            Err(error) => {
                for work in works { let _ = work.done.send(Err(error.to_string())); }
                queue.close();
                while let Some(work) = queue.recv().await { let _ = work.done.send(Err(error.to_string())); }
                return Err(error);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNBOUNDED: usize = usize::MAX;

    fn entry(lsn: i64) -> Entry { Entry { lsn, epoch: 1, timestamp: lsn, guarded: false, payload: Bytes::from(format!("entry {lsn}")) } }

    fn sized(lsn: i64, epoch: u64, bytes: usize) -> Entry { Entry { lsn, epoch, timestamp: lsn, guarded: false, payload: Bytes::from(format!("{lsn}@{epoch}:{}", "x".repeat(bytes))) } }

    fn work(write: Write) -> Work { Work { write, done: oneshot::channel().0, _admission: None, authority_revision: None, observed_append: None } }

    fn slot(journal: &Journal, name: &str) -> Arc<Slot> {
        Arc::new(Slot::new(journal.allocate_id(), name.into(), StreamState::default()))
    }

    fn recovered_slots(recovered: Vec<Recovered>) -> BTreeMap<String, Arc<Slot>> {
        recovered.into_iter().map(|stream| (stream.name.clone(), Arc::new(Slot::new(stream.id, stream.name, stream.state)))).collect()
    }

    /// Decides on the slot's own state and writes the decision as one round, as the store and the journal task do.
    fn decide(journal: &Journal, slot: &Arc<Slot>, decide: impl FnOnce(&mut StreamState<Stored>) -> super::super::state::Decision) {
        let events = decide(&mut slot.state.lock()).events;
        journal.write_round(&[work(Write::Events(Arc::clone(slot), events))]).unwrap();
    }

    fn compact(journal: &Journal, slots: &[&Arc<Slot>]) {
        let captured = slots.iter().map(|slot| Captured { slot: Arc::clone(slot), state: slot.state.lock().clone() }).collect();
        journal.write_round(&[work(Write::Compact(captured))]).unwrap();
    }

    fn synced(journal: &Journal, slot: &Arc<Slot>, epoch: u64) {
        decide(journal, slot, |state| state.seal(epoch));
        decide(journal, slot, |state| state.overwrite(epoch, state.last() + 1, Vec::new(), true, -1));
    }

    /// Every retained entry, with its payload resolved as a reader's would be.
    fn retained(journal: &Journal, slot: &Slot) -> Vec<Entry> {
        let window = {
            let state = slot.state.lock();
            state.window(state.trim + 1, state.last(), UNBOUNDED, UNBOUNDED, |lsn, held| Item { lsn, epoch: held.epoch, timestamp: held.timestamp, guarded: held.guarded, source: journal.source(&held.payload) })
        };
        journal.resolve(window).unwrap().entries
    }

    fn written(slot: &Slot) -> bool { slot.state.lock().entries.iter().all(|held| matches!(held.payload, Stored::Written(_))) }

    fn segment_files(directory: &Path) -> usize { fs::read_dir(directory.join("segments")).unwrap().count() }

    #[test]
    fn an_installed_member_rejects_missing_segments() {
        let directory = tempfile::tempdir().unwrap();
        let first = "c2a4eba6-83a7-455d-a1c1-b80c5cf5f2ba";
        let (journal, _, identity) = Journal::open(directory.path(), true, 0, Some(MemberIdentity::Open(first.into()))).unwrap();
        assert_eq!(identity, first);
        drop(journal);
        fs::remove_dir_all(directory.path().join("segments")).unwrap();
        assert!(Journal::open(directory.path(), true, 0, Some(MemberIdentity::Reopen(first.into()))).is_err());
    }

    #[test]
    fn activation_survives_replay_and_compaction_until_a_higher_promise() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, _, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        let stream = slot(&journal, "active-loglet");
        let ensemble = [4; 32];
        synced(&journal, &stream, 1);
        decide(&journal, &stream, |state| state.activate(1, ensemble));
        drop(journal);

        let (journal, recovered, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        let stream = recovered_slots(recovered).remove("active-loglet").unwrap();
        assert!(matches!(stream.state.lock().read_index(1, ensemble), crate::protocol::ResponseBody::Accepted(_)));
        assert!(matches!(stream.state.lock().activate(1, [5; 32]).reply, crate::protocol::ResponseBody::Refused(_)));
        compact(&journal, &[&stream]);
        drop(journal);

        let (journal, recovered, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        let stream = recovered_slots(recovered).remove("active-loglet").unwrap();
        assert!(matches!(stream.state.lock().read_index(1, ensemble), crate::protocol::ResponseBody::Accepted(_)));
        synced(&journal, &stream, 2);
        assert!(matches!(stream.state.lock().read_index(2, ensemble), crate::protocol::ResponseBody::Refused(_)));
    }

    #[test]
    fn a_guarded_sync_observation_survives_an_earlier_capture_and_never_moves_to_a_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, _, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        let stream = slot(&journal, "timed");
        synced(&journal, &stream, 1);
        let observation = Arc::new(OnceLock::new());
        let events = {
            let mut state = stream.state.lock();
            let events = state.append(1, 0, vec![Entry { guarded: true, ..entry(0) }], -1).events;
            assert!(state.track_sync(0, Arc::clone(&observation)));
            events
        };
        let mut queued = work(Write::Events(Arc::clone(&stream), events));
        queued.observed_append = Some((0, Arc::clone(&observation)));
        let captured = Captured { slot: Arc::clone(&stream), state: stream.state.lock().clone() };
        journal.write_round(&[queued, work(Write::Compact(vec![captured]))]).unwrap();
        let original = *observation.get().unwrap();
        drop(journal);

        let (journal, recovered, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        let stream = recovered_slots(recovered).remove("timed").unwrap();
        assert_eq!(stream.state.lock().sync_observation(0).unwrap().get().copied(), Some(original));
        decide(&journal, &stream, |state| state.seal(2));
        assert!(stream.state.lock().guarded_at_seal(2, 0));
        let duplicate = stream.state.lock().overwrite(2, 0, vec![Entry { guarded: true, ..entry(0) }], true, 0);
        assert!(matches!(duplicate.reply, crate::protocol::ResponseBody::Failed(_)));
        decide(&journal, &stream, |state| state.overwrite(2, 0, vec![Entry { guarded: true, ..sized(0, 2, 3) }], true, 0));
        decide(&journal, &stream, |state| state.seal(3));
        assert!(stream.state.lock().sync_observation(0).is_none());
        drop(journal);

        let (_, recovered, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        let stream = recovered_slots(recovered).remove("timed").unwrap();
        assert!(stream.state.lock().sync_observation(0).is_none());
        assert!(stream.state.lock().guarded_at_seal(3, 0));
    }

    #[test]
    fn replay_and_compaction_remember_an_original_without_its_clock_sample() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, _, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        let stream = slot(&journal, "lost-sample");
        synced(&journal, &stream, 1);
        let mut raw = BytesMut::new();
        frame_record(Record {
            stream: stream.id, kind: Some(Kind::Append(Entry { guarded: true, ..entry(0) })),
            active_ensemble: Bytes::new(), original_append: Some(true),
        }, &mut raw);
        // Stop after persisting the raw append, before the second sync can publish its sample.
        journal.append(&mut journal.current.lock(), &raw, true).unwrap();
        drop(journal);

        let (journal, recovered, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        let stream = recovered_slots(recovered).remove("lost-sample").unwrap();
        assert!(stream.state.lock().sync_observation(0).is_some_and(|original| original.get().is_none()));
        compact(&journal, &[&stream]);
        drop(journal);

        let (_, recovered, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        let stream = recovered_slots(recovered).remove("lost-sample").unwrap();
        assert!(stream.state.lock().sync_observation(0).is_some_and(|original| original.get().is_none()));
    }

    #[test]
    fn streams_sharing_segments_replay_to_their_decided_states_across_a_compaction() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, _, _) = Journal::open(directory.path(), true, UNBOUNDED, None).unwrap();
        let (first, second) = (slot(&journal, "deployment/1/replica"), slot(&journal, "deployment/2/replica"));
        let events = {
            let mut one = first.state.lock();
            let mut events = one.seal(1).events;
            events.extend(one.overwrite(1, 0, Vec::new(), true, -1).events);
            events.extend(one.append(1, 0, (0..4).map(entry).collect(), 2).events);
            events
        };
        let two_events = second.state.lock().seal(3).events;
        journal.write_round(&[work(Write::Events(Arc::clone(&first), events)), work(Write::Events(Arc::clone(&second), two_events))]).unwrap();
        first.state.lock().trim(1);
        let captured = [&first, &second].iter().map(|slot| Captured { slot: Arc::clone(slot), state: slot.state.lock().clone() }).collect();
        let after = first.state.lock().append(1, 4, vec![entry(4)], 4).events;
        journal.write_round(&[work(Write::Compact(captured)), work(Write::Events(Arc::clone(&first), after))]).unwrap();
        assert!(written(&first), "the entry appended after the capture is written after the base");
        let expected = retained(&journal, &first);
        assert_eq!(expected, (2..5).map(entry).collect::<Vec<_>>());
        drop(journal);
        let (journal, recovered, _) = Journal::open(directory.path(), true, UNBOUNDED, None).unwrap();
        let slots = recovered_slots(recovered);
        let (one, two) = (&slots[&first.name], &slots[&second.name]);
        assert_eq!(*one.state.lock(), *first.state.lock(), "entries are recovered at the locations they were left at");
        assert_eq!(*two.state.lock(), *second.state.lock());
        assert_eq!(retained(&journal, one), expected);
        assert_eq!(segment_files(directory.path()), 1, "a compaction deletes the segments it supersedes");
        assert_eq!(journal.allocate_id(), 2, "ids are not reused");
    }

    #[test]
    fn entries_evicted_from_the_cache_are_read_from_their_segments_and_the_cache_keeps_its_budget() {
        let directory = tempfile::tempdir().unwrap();
        let budget = 8 * 1024;
        let (journal, _, _) = Journal::open(directory.path(), true, budget, None).unwrap();
        let stream = slot(&journal, "a/b");
        synced(&journal, &stream, 1);
        let entries: Vec<Entry> = (0..400).map(|lsn| sized(lsn, 1, 1000)).collect();
        for chunk in entries.chunks(10) {
            decide(&journal, &stream, |state| state.append(1, chunk[0].lsn, chunk.to_vec(), chunk[0].lsn));
            assert!(journal.cache_bytes() <= budget, "the cache holds {} bytes of a {budget} budget", journal.cache_bytes());
        }
        assert!(written(&stream));
        assert_eq!(retained(&journal, &stream), entries, "every entry is served, most from disk");
        assert!(journal.cache_bytes() <= budget);
        drop(journal);
        let (journal, recovered, _) = Journal::open(directory.path(), true, budget, None).unwrap();
        assert_eq!(journal.cache_bytes(), 0, "recovery reads no payload into memory");
        let slots = recovered_slots(recovered);
        assert!(written(&slots["a/b"]));
        assert_eq!(retained(&journal, &slots["a/b"]), entries);
        assert!(journal.cache_bytes() <= budget);
    }

    #[test]
    fn a_truncation_and_a_trim_of_evicted_entries_read_correctly_and_a_compaction_retires_their_segments() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, _, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        let stream = slot(&journal, "a/b");
        synced(&journal, &stream, 1);
        let mut expected: Vec<Entry> = (0..100).map(|lsn| sized(lsn, 1, 100)).collect();
        decide(&journal, &stream, |state| state.append(1, 0, expected.clone(), 49));
        decide(&journal, &stream, |state| state.seal(2));
        let replacement: Vec<Entry> = (50..120).map(|lsn| sized(lsn, 2, 200)).collect();
        decide(&journal, &stream, |state| state.overwrite(2, 50, replacement.clone(), true, 119));
        expected.truncate(50);
        expected.extend(replacement);
        assert_eq!(retained(&journal, &stream), expected, "a later writer's entries replace the truncated suffix");
        decide(&journal, &stream, |state| state.trim(79));
        expected.drain(..80);
        assert_eq!(retained(&journal, &stream), expected);
        let before = journal.current.lock().sequence;
        compact(&journal, &[&stream]);
        assert_eq!(segment_files(directory.path()), 1, "the segments holding only trimmed or truncated entries are deleted");
        assert!(journal.files.lock().keys().all(|segment| *segment > before), "no handle on a retired segment is kept");
        assert_eq!(retained(&journal, &stream), expected, "entries are read from their copies in the base segment");
        decide(&journal, &stream, |state| state.append(2, 120, vec![sized(120, 2, 10)], 120));
        expected.push(sized(120, 2, 10));
        drop(journal);
        let (journal, recovered, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        assert_eq!(retained(&journal, &recovered_slots(recovered)["a/b"]), expected);
    }

    #[test]
    fn a_replacement_decided_before_the_replaced_entry_is_written_keeps_its_own_payload() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, _, _) = Journal::open(directory.path(), true, 0, None).unwrap();
        let stream = slot(&journal, "a/b");
        synced(&journal, &stream, 1);
        let first = stream.state.lock().append(1, 0, vec![sized(0, 1, 10)], -1).events;
        let sealed = stream.state.lock().seal(2).events;
        let replaced = stream.state.lock().overwrite(2, 0, vec![sized(0, 2, 10)], true, 0).events;
        journal.write_round(&[work(Write::Events(Arc::clone(&stream), first))]).unwrap();
        assert!(matches!(stream.state.lock().entries[0].payload, Stored::Pending(_)), "the replaced entry's record does not claim its replacement");
        journal.write_round(&[work(Write::Events(Arc::clone(&stream), sealed)), work(Write::Events(Arc::clone(&stream), replaced))]).unwrap();
        assert_eq!(retained(&journal, &stream), vec![sized(0, 2, 10)]);
    }

    #[test]
    fn a_base_segment_supersedes_segments_a_crash_left_behind() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, _, _) = Journal::open(directory.path(), true, UNBOUNDED, None).unwrap();
        let stream = slot(&journal, "a/b");
        decide(&journal, &stream, |state| state.seal(2));
        let stale = fs::read(segment_path(&journal.segments, 0)).unwrap();
        compact(&journal, &[&stream]);
        decide(&journal, &stream, |state| state.seal(5));
        fs::write(segment_path(&journal.segments, 0), stale).unwrap();
        drop(journal);
        let (_, recovered, _) = Journal::open(directory.path(), true, UNBOUNDED, None).unwrap();
        assert_eq!(recovered_slots(recovered)["a/b"].state.lock().promised, 5);
        assert_eq!(segment_files(directory.path()), 1);
    }

    #[test]
    fn a_torn_final_record_is_cut_off_and_earlier_records_replay() {
        let directory = tempfile::tempdir().unwrap();
        let segments = directory.path().join("segments");
        fs::create_dir_all(&segments).unwrap();
        let path = segment_path(&segments, 0);
        let mut buffer = BytesMut::new();
        frame(7, Kind::Declare("a/b".into()), &mut buffer);
        frame(7, Kind::Promise(4), &mut buffer);
        let good = buffer.len();
        frame(7, Kind::Append(Entry { lsn: 0, epoch: 4, timestamp: 1, guarded: false, payload: Bytes::from_static(b"torn") }), &mut buffer);
        fs::write(&path, &buffer[..buffer.len() - 3]).unwrap();
        let (journal, recovered, _) = Journal::open(directory.path(), true, UNBOUNDED, None).unwrap();
        assert_eq!((recovered.len(), recovered[0].name.as_str(), recovered[0].state.promised, recovered[0].state.last()), (1, "a/b", 4, -1));
        assert_eq!(fs::metadata(&path).unwrap().len(), good as u64);
        assert_eq!(journal.allocate_id(), 8);
    }
}
