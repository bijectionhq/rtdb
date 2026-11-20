//! Durable storage: every stream's events in one sequence of shared segment files, in decision order.
//! Each record is framed as a big-endian u32 length, a CRC-32 of the body, and a prost body naming its stream by a server-wide id that a `Declare` record binds to the stream's name.
//! A round of writes, across every stream, is one write and at most one sync.
//! A compaction writes a base segment holding a snapshot of every stream and then deletes every earlier segment; replay starts at the last base segment.
//! A torn final record of the last segment is cut off on recovery, as it was never acknowledged.

use super::state::{Event, StreamState};
use super::store::Slot;
use crate::protocol::Entry;
use bytes::{BufMut, Bytes, BytesMut};
use fs2::FileExt;
use parking_lot::Mutex;
use prost::Message;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot};

const SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

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
}

#[derive(Clone, PartialEq, Message)]
struct Record {
    #[prost(uint64, tag = "9")]
    stream: u64,
    #[prost(oneof = "Kind", tags = "1, 2, 3, 4, 5, 6, 7, 8, 10")]
    kind: Option<Kind>,
}

fn record(event: &Event) -> Kind {
    match event {
        Event::Promise(epoch) => Kind::Promise(*epoch),
        Event::Synced(epoch) => Kind::Synced(epoch.unwrap_or(0)),
        Event::Truncate(from) => Kind::Truncate(*from),
        Event::Append(entry) => Kind::Append(entry.clone()),
        Event::Commit(lsn) => Kind::Commit(*lsn),
        Event::Trim(through) => Kind::Trim(*through),
    }
}

fn frame(stream: u64, kind: Kind, buffer: &mut BytesMut) {
    let body = Record { stream, kind: Some(kind) }.encode_to_vec();
    buffer.put_u32(body.len() as u32);
    buffer.put_u32(crc32fast::hash(&body));
    buffer.put_slice(&body);
}

fn frame_state(stream: &Captured, buffer: &mut BytesMut) {
    let (id, state) = (stream.id, &stream.state);
    frame(id, Kind::Declare(stream.name.clone()), buffer);
    frame(id, Kind::Snapshot(Snapshot { promised: state.promised, synced: state.synced.unwrap_or(0), trim: state.trim, commit: state.commit }), buffer);
    for (held, lsn) in state.entries.iter().zip(state.trim + 1..) {
        frame(id, Kind::Append(Entry { lsn, epoch: held.epoch, timestamp: held.timestamp, payload: held.payload.clone() }), buffer);
    }
}

fn io_error(path: &Path, error: std::io::Error) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.logkeeper/io", format!("I/O at {}: {error}", path.display()))
}

fn corrupt(path: &Path, reason: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.logkeeper/corrupt-journal", format!("journal {}: {reason}", path.display()))
}

/// A stream's state as of a point in the journal's order, for a base segment.
pub(crate) struct Captured {
    pub id: u64,
    pub name: String,
    pub state: StreamState,
}

pub(crate) enum Write {
    Events(Arc<Slot>, Vec<Event>),
    /// Every stream, captured with no decision of any of them between the capture and this work in the queue.
    Compact(Vec<Captured>),
}

pub(crate) struct Work {
    pub write: Write,
    pub done: oneshot::Sender<std::result::Result<(), String>>,
}

/// The segment being appended to; touched only by the journal's rounds.
struct Current {
    file: File,
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
}

pub(crate) struct Recovered {
    pub id: u64,
    pub name: String,
    pub state: StreamState,
}

fn segment_path(segments: &Path, sequence: u64) -> PathBuf { segments.join(format!("{sequence:020}.seg")) }

fn sync_directory(directory: &Path, fsync: bool) -> Result<()> {
    if !fsync { return Ok(()); }
    File::open(directory).and_then(|directory| rtdb_storage::sync_file(&directory)).map_err(|error| io_error(directory, error))
}

/// Every segment file by sequence, removing temporary files a crashed compaction left.
fn list_segments(segments: &Path) -> Result<BTreeMap<u64, PathBuf>> {
    let mut sequences = BTreeMap::new();
    for entry in fs::read_dir(segments).map_err(|error| io_error(segments, error))? {
        let path = entry.map_err(|error| io_error(segments, error))?.path();
        match path.extension().and_then(|extension| extension.to_str()) {
            Some("seg") => {
                let sequence = path.file_stem().and_then(|stem| stem.to_str()).and_then(|stem| stem.parse::<u64>().ok()).ok_or_else(|| corrupt(&path, "not a segment name"))?;
                sequences.insert(sequence, path);
            }
            Some("tmp") => fs::remove_file(&path).map_err(|error| io_error(&path, error))?,
            _ => (),
        }
    }
    Ok(sequences)
}

fn remove_before(segments: &Path, sequence: u64) -> Result<()> {
    for (_, path) in list_segments(segments)?.range(..sequence) { fs::remove_file(path).map_err(|error| io_error(path, error))?; }
    Ok(())
}

impl Journal {
    /// Replays the segments from the last base on, deleting any earlier ones a crash left behind, and opens the last for appending.
    pub fn open(data_dir: &Path, fsync: bool) -> Result<(Self, Vec<Recovered>)> {
        let segments = data_dir.join("segments");
        fs::create_dir_all(&segments).map_err(|error| io_error(&segments, error))?;
        let lock_path = data_dir.join("LOCK");
        let lock = OpenOptions::new().create(true).truncate(false).write(true).open(&lock_path).map_err(|error| io_error(&lock_path, error))?;
        lock.try_lock_exclusive().map_err(|error| io_error(&lock_path, error))?;
        let mut contents = Vec::new();
        for (sequence, path) in list_segments(&segments)?.into_iter().rev() {
            let bytes = Bytes::from(fs::read(&path).map_err(|error| io_error(&path, error))?);
            let base = matches!(next_record(&bytes, &mut 0), Some(Record { kind: Some(Kind::Base(_)), .. }));
            contents.push((sequence, path, bytes));
            if base { break; }
        }
        contents.reverse();
        if let Some((first, _, _)) = contents.first() { remove_before(&segments, *first)?; }
        let mut replay = Replay::default();
        let mut total = 0u64;
        let last_index = contents.len().checked_sub(1);
        for (index, (_, path, bytes)) in contents.iter().enumerate() {
            let end = replay.segment(path, bytes)?;
            if end < bytes.len() {
                if Some(index) != last_index { return Err(corrupt(path, format!("a damaged record at {end} before the last segment"))); }
                let file = OpenOptions::new().write(true).open(path).map_err(|error| io_error(path, error))?;
                file.set_len(end as u64).and_then(|_| rtdb_storage::sync_file(&file)).map_err(|error| io_error(path, error))?;
            }
            total += end as u64;
        }
        let (sequence, path) = contents.last().map_or_else(|| (0, segment_path(&segments, 0)), |(sequence, path, _)| (*sequence, path.clone()));
        let file = OpenOptions::new().create(true).append(true).open(&path).map_err(|error| io_error(&path, error))?;
        if contents.is_empty() { sync_directory(&segments, fsync)?; }
        let bytes = file.metadata().map_err(|error| io_error(&path, error))?.len();
        let declared = replay.names.keys().copied().collect();
        let next_id = replay.names.keys().max().map_or(0, |id| id + 1);
        let recovered = replay.names.into_iter().map(|(id, name)| Recovered { id, name, state: replay.states.remove(&id).unwrap_or_default() }).collect();
        let journal = Self {
            segments,
            fsync,
            _lock: lock,
            current: Mutex::new(Current { file, path, sequence, bytes, declared }),
            bytes: AtomicU64::new(total),
            next_id: AtomicU64::new(next_id),
            compaction_queued: AtomicBool::new(false),
        };
        Ok((journal, recovered))
    }

    pub fn allocate_id(&self) -> u64 { self.next_id.fetch_add(1, Ordering::Relaxed) }

    pub fn bytes(&self) -> u64 { self.bytes.load(Ordering::Relaxed) }

    /// Claims the one compaction that may be queued at a time; the journal releases it once the compaction's round is written.
    pub fn claim_compaction(&self) -> bool { !self.compaction_queued.swap(true, Ordering::AcqRel) }

    pub fn release_compaction(&self) { self.compaction_queued.store(false, Ordering::Release); }

    /// Writes one round: every work in order, then one sync of the segment.
    /// A round of commit records alone is not synced: a commit lsn is a hint to readers that the next writer re-establishes, and the entries it covers were synced by earlier rounds.
    fn write_round(&self, works: &[Work]) -> Result<()> {
        let mut current = self.current.lock();
        let mut buffer = BytesMut::new();
        let mut sync = false;
        for work in works {
            match &work.write {
                Write::Events(_, events) if events.is_empty() => (),
                Write::Events(slot, events) => {
                    if current.declared.insert(slot.id) {
                        frame(slot.id, Kind::Declare(slot.name.clone()), &mut buffer);
                        sync = true;
                    }
                    for event in events { frame(slot.id, record(event), &mut buffer); }
                    sync |= events.iter().any(|event| !matches!(event, Event::Commit(_)));
                }
                Write::Compact(captured) => {
                    self.append(&mut current, &buffer, sync)?;
                    buffer.clear();
                    sync = false;
                    self.compact(&mut current, captured)?;
                }
            }
        }
        self.append(&mut current, &buffer, sync)
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
            let file = OpenOptions::new().create(true).append(true).open(&path).map_err(|error| io_error(&path, error))?;
            sync_directory(&self.segments, self.fsync)?;
            *current = Current { file, path, sequence, bytes: 0, declared: std::mem::take(&mut current.declared) };
        }
        Ok(())
    }

    /// Writes a base segment holding every captured stream, then deletes every earlier segment.
    fn compact(&self, current: &mut Current, captured: &[Captured]) -> Result<()> {
        let mut buffer = BytesMut::new();
        frame(0, Kind::Base(true), &mut buffer);
        for stream in captured { frame_state(stream, &mut buffer); }
        let sequence = current.sequence + 1;
        let path = segment_path(&self.segments, sequence);
        let temporary = path.with_extension("tmp");
        let mut file = File::create(&temporary).map_err(|error| io_error(&temporary, error))?;
        file.write_all(&buffer).map_err(|error| io_error(&temporary, error))?;
        self.sync(&file, &temporary)?;
        fs::rename(&temporary, &path).map_err(|error| io_error(&path, error))?;
        sync_directory(&self.segments, self.fsync)?;
        let file = OpenOptions::new().append(true).open(&path).map_err(|error| io_error(&path, error))?;
        *current = Current { file, path, sequence, bytes: buffer.len() as u64, declared: captured.iter().map(|stream| stream.id).collect() };
        self.bytes.store(buffer.len() as u64, Ordering::Relaxed);
        remove_before(&self.segments, sequence)?;
        sync_directory(&self.segments, self.fsync)
    }

    fn sync(&self, file: &File, path: &Path) -> Result<()> {
        if self.fsync { rtdb_storage::sync_file(file).map_err(|error| io_error(path, error))?; }
        Ok(())
    }
}

#[derive(Default)]
struct Replay {
    names: HashMap<u64, String>,
    states: HashMap<u64, StreamState>,
}

impl Replay {
    /// Applies a segment's records, returning where the last whole record ends.
    fn segment(&mut self, path: &Path, bytes: &Bytes) -> Result<usize> {
        let mut position = 0usize;
        while let Some(Record { stream, kind }) = next_record(bytes, &mut position) {
            let kind = kind.ok_or_else(|| corrupt(path, "a record without a kind"))?;
            if let Kind::Declare(name) = &kind {
                self.names.insert(stream, name.clone());
                self.states.entry(stream).or_default();
                continue;
            }
            if let Kind::Base(_) = kind { continue; }
            let state = self.states.get_mut(&stream).ok_or_else(|| corrupt(path, format!("stream {stream} was never declared")))?;
            match kind {
                Kind::Declare(_) | Kind::Base(_) => unreachable!("handled above"),
                Kind::Snapshot(snapshot) => {
                    *state = StreamState { promised: snapshot.promised, synced: (snapshot.synced != 0).then_some(snapshot.synced), trim: snapshot.trim, commit: snapshot.commit, entries: Default::default() };
                }
                Kind::Promise(epoch) => state.apply(&Event::Promise(epoch)),
                Kind::Synced(epoch) => state.apply(&Event::Synced((epoch != 0).then_some(epoch))),
                Kind::Truncate(from) => state.apply(&Event::Truncate(from)),
                Kind::Append(entry) => {
                    if entry.lsn != state.last() + 1 { return Err(corrupt(path, format!("stream {stream}'s entry {} follows {}", entry.lsn, state.last()))); }
                    state.apply(&Event::Append(entry));
                }
                Kind::Commit(lsn) => state.apply(&Event::Commit(lsn)),
                Kind::Trim(through) => state.apply(&Event::Trim(through)),
            }
        }
        Ok(position)
    }
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

/// Group commit: whatever has queued while the previous round was syncing goes into the next round together, across every stream.
pub(crate) async fn run(journal: Arc<Journal>, mut queue: mpsc::UnboundedReceiver<Work>) -> Result<()> {
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
            Ok(()) => for work in works { let _ = work.done.send(Ok(())); },
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

    fn entry(lsn: i64) -> Entry { Entry { lsn, epoch: 1, timestamp: lsn, payload: Bytes::from(format!("entry {lsn}")) } }

    fn work(write: Write) -> Work { Work { write, done: oneshot::channel().0 } }

    fn slot(journal: &Journal, name: &str) -> Arc<Slot> {
        Arc::new(Slot { id: journal.allocate_id(), name: name.into(), state: Default::default(), progressed: Default::default() })
    }

    fn captured(slot: &Slot, state: &StreamState) -> Captured { Captured { id: slot.id, name: slot.name.clone(), state: state.clone() } }

    fn by_name(recovered: Vec<Recovered>) -> BTreeMap<String, StreamState> {
        recovered.into_iter().map(|stream| (stream.name, stream.state)).collect()
    }

    fn segment_files(directory: &Path) -> usize { fs::read_dir(directory.join("segments")).unwrap().count() }

    #[test]
    fn streams_sharing_segments_replay_to_their_decided_states_across_a_compaction() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, _) = Journal::open(directory.path(), true).unwrap();
        let (first, second) = (slot(&journal, "deployment/1/replica"), slot(&journal, "deployment/2/replica"));
        let (mut one, mut two) = (StreamState::default(), StreamState::default());
        let mut events = one.seal(1).events;
        events.extend(one.overwrite(1, 0, Vec::new(), true, -1).events);
        events.extend(one.append(1, 0, (0..4).map(entry).collect(), 2).events);
        journal.write_round(&[work(Write::Events(Arc::clone(&first), events)), work(Write::Events(Arc::clone(&second), two.seal(3).events))]).unwrap();
        one.trim(1);
        let compaction = Write::Compact(vec![captured(&first, &one), captured(&second, &two)]);
        journal.write_round(&[work(compaction), work(Write::Events(Arc::clone(&first), one.append(1, 4, vec![entry(4)], 4).events))]).unwrap();
        drop(journal);
        let (journal, recovered) = Journal::open(directory.path(), true).unwrap();
        assert_eq!(by_name(recovered), BTreeMap::from([(first.name.clone(), one), (second.name.clone(), two)]));
        assert_eq!(segment_files(directory.path()), 1, "a compaction deletes the segments it supersedes");
        assert_eq!(journal.allocate_id(), 2, "ids are not reused");
    }

    #[test]
    fn a_base_segment_supersedes_segments_a_crash_left_behind() {
        let directory = tempfile::tempdir().unwrap();
        let (journal, _) = Journal::open(directory.path(), true).unwrap();
        let stream = slot(&journal, "a/b");
        let mut state = StreamState::default();
        journal.write_round(&[work(Write::Events(Arc::clone(&stream), state.seal(2).events))]).unwrap();
        let stale = fs::read(segment_path(&journal.segments, 0)).unwrap();
        journal.write_round(&[work(Write::Compact(vec![captured(&stream, &state)]))]).unwrap();
        journal.write_round(&[work(Write::Events(Arc::clone(&stream), state.seal(5).events))]).unwrap();
        fs::write(segment_path(&journal.segments, 0), stale).unwrap();
        drop(journal);
        let (_, recovered) = Journal::open(directory.path(), true).unwrap();
        assert_eq!(by_name(recovered)["a/b"].promised, 5);
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
        frame(7, Kind::Append(Entry { lsn: 0, epoch: 4, timestamp: 1, payload: Bytes::from_static(b"torn") }), &mut buffer);
        fs::write(&path, &buffer[..buffer.len() - 3]).unwrap();
        let (journal, recovered) = Journal::open(directory.path(), true).unwrap();
        assert_eq!((recovered.len(), recovered[0].name.as_str(), recovered[0].state.promised, recovered[0].state.last()), (1, "a/b", 4, -1));
        assert_eq!(fs::metadata(&path).unwrap().len(), good as u64);
        assert_eq!(journal.allocate_id(), 8);
    }
}
