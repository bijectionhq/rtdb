//! Durable storage: one append-only journal file per stream, holding the stream's events in decision order.
//! Each record is framed as a big-endian u32 length, a CRC-32 of the body, and a prost body.
//! A trim may rewrite the file as a snapshot of the stream; a torn final record is cut off on recovery, as it was never acknowledged.

use super::state::{Event, StreamState};
use super::store::Slot;
use crate::protocol::Entry;
use bytes::{BufMut, Bytes, BytesMut};
use fs2::FileExt;
use prost::Message;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

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
    #[prost(string, tag = "1")]
    Header(String),
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
}

#[derive(Clone, PartialEq, Message)]
struct Record {
    #[prost(oneof = "Kind", tags = "1, 2, 3, 4, 5, 6, 7, 8")]
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

fn frame(kind: Kind, buffer: &mut BytesMut) {
    let body = Record { kind: Some(kind) }.encode_to_vec();
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

pub(crate) enum Write {
    Events(Vec<Event>),
    /// Replaces the file with the stream's state as of this point in its order.
    Rewrite(StreamState),
}

pub(crate) struct Work {
    pub slot: Arc<Slot>,
    pub write: Write,
    pub done: oneshot::Sender<std::result::Result<(), String>>,
}

/// What the journal knows of a stream's file; touched only by the journal's rounds and by trim's rewrite decision.
#[derive(Default)]
pub(crate) struct FileInfo {
    pub exists: bool,
    pub bytes: u64,
}

pub(crate) struct Journal {
    streams: PathBuf,
    fsync: bool,
    _lock: File,
}

pub(crate) struct Recovered {
    pub name: String,
    pub path: PathBuf,
    pub state: StreamState,
    pub bytes: u64,
}

impl Journal {
    pub fn open(data_dir: &Path, fsync: bool) -> Result<Self> {
        let streams = data_dir.join("streams");
        fs::create_dir_all(&streams).map_err(|error| io_error(&streams, error))?;
        let lock_path = data_dir.join("LOCK");
        let lock = OpenOptions::new().create(true).truncate(false).write(true).open(&lock_path).map_err(|error| io_error(&lock_path, error))?;
        lock.try_lock_exclusive().map_err(|error| io_error(&lock_path, error))?;
        Ok(Self { streams, fsync, _lock: lock })
    }

    pub fn path_for(&self, stream: &str) -> PathBuf {
        let digest = Sha256::digest(stream.as_bytes());
        let name: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        self.streams.join(format!("{name}.log"))
    }

    pub fn recover(&self) -> Result<Vec<Recovered>> {
        let mut recovered = Vec::new();
        for entry in fs::read_dir(&self.streams).map_err(|error| io_error(&self.streams, error))? {
            let path = entry.map_err(|error| io_error(&self.streams, error))?.path();
            match path.extension().and_then(|extension| extension.to_str()) {
                Some("log") => recovered.push(replay(&path)?),
                Some("tmp") => fs::remove_file(&path).map_err(|error| io_error(&path, error))?,
                _ => (),
            }
        }
        Ok(recovered)
    }

    /// Writes one round: every work in order, then one sync per touched file.
    /// A file that received only commit records is not synced: a commit lsn is a hint to readers that the next writer re-establishes, and the entries it covers were synced by earlier rounds.
    fn write_round(&self, works: &[Work]) -> Result<()> {
        let mut open: HashMap<*const Slot, (File, bool)> = HashMap::new();
        let mut created_directory_entry = false;
        for work in works {
            let key = Arc::as_ptr(&work.slot);
            match &work.write {
                Write::Events(events) if events.is_empty() => (),
                Write::Events(events) => {
                    let mut info = work.slot.file.lock();
                    let mut buffer = BytesMut::new();
                    if !info.exists { frame(Kind::Header(work.slot.name.clone()), &mut buffer); }
                    for event in events { frame(record(event), &mut buffer); }
                    if !open.contains_key(&key) {
                        let file = OpenOptions::new().create(true).append(true).open(&work.slot.path).map_err(|error| io_error(&work.slot.path, error))?;
                        open.insert(key, (file, false));
                    }
                    let (file, sync) = open.get_mut(&key).expect("opened above");
                    *sync |= !info.exists || events.iter().any(|event| !matches!(event, Event::Commit(_)));
                    file.write_all(&buffer).map_err(|error| io_error(&work.slot.path, error))?;
                    created_directory_entry |= !info.exists;
                    info.exists = true;
                    info.bytes += buffer.len() as u64;
                }
                Write::Rewrite(state) => {
                    if let Some((file, true)) = open.remove(&key) { self.sync(&file, &work.slot.path)?; }
                    let bytes = self.rewrite(&work.slot, state)?;
                    *work.slot.file.lock() = FileInfo { exists: true, bytes };
                }
            }
        }
        for work in works {
            if let Some((file, true)) = open.remove(&Arc::as_ptr(&work.slot)) { self.sync(&file, &work.slot.path)?; }
        }
        if created_directory_entry { self.sync_directory()?; }
        Ok(())
    }

    fn rewrite(&self, slot: &Slot, state: &StreamState) -> Result<u64> {
        let mut buffer = BytesMut::new();
        frame(Kind::Header(slot.name.clone()), &mut buffer);
        frame(Kind::Snapshot(Snapshot { promised: state.promised, synced: state.synced.unwrap_or(0), trim: state.trim, commit: state.commit }), &mut buffer);
        for (held, lsn) in state.entries.iter().zip(state.trim + 1..) {
            frame(Kind::Append(Entry { lsn, epoch: held.epoch, timestamp: held.timestamp, payload: held.payload.clone() }), &mut buffer);
        }
        let temporary = slot.path.with_extension("tmp");
        let mut file = File::create(&temporary).map_err(|error| io_error(&temporary, error))?;
        file.write_all(&buffer).map_err(|error| io_error(&temporary, error))?;
        self.sync(&file, &temporary)?;
        fs::rename(&temporary, &slot.path).map_err(|error| io_error(&slot.path, error))?;
        self.sync_directory()?;
        Ok(buffer.len() as u64)
    }

    fn sync(&self, file: &File, path: &Path) -> Result<()> {
        if self.fsync { rtdb_storage::sync_file(file).map_err(|error| io_error(path, error))?; }
        Ok(())
    }

    fn sync_directory(&self) -> Result<()> {
        if !self.fsync { return Ok(()); }
        File::open(&self.streams).and_then(|directory| rtdb_storage::sync_file(&directory)).map_err(|error| io_error(&self.streams, error))
    }
}

fn replay(path: &Path) -> Result<Recovered> {
    let bytes = Bytes::from(fs::read(path).map_err(|error| io_error(path, error))?);
    let mut name = None;
    let mut state = StreamState::default();
    let mut position = 0usize;
    while let Some(kind) = next_record(&bytes, &mut position) {
        match kind {
            Kind::Header(stream) => name = Some(stream),
            Kind::Snapshot(snapshot) => {
                state = StreamState { promised: snapshot.promised, synced: (snapshot.synced != 0).then_some(snapshot.synced), trim: snapshot.trim, commit: snapshot.commit, entries: Default::default() };
            }
            Kind::Promise(epoch) => state.apply(&Event::Promise(epoch)),
            Kind::Synced(epoch) => state.apply(&Event::Synced((epoch != 0).then_some(epoch))),
            Kind::Truncate(from) => state.apply(&Event::Truncate(from)),
            Kind::Append(entry) => {
                if entry.lsn != state.last() + 1 { return Err(corrupt(path, format!("entry {} follows {}", entry.lsn, state.last()))); }
                state.apply(&Event::Append(entry));
            }
            Kind::Commit(lsn) => state.apply(&Event::Commit(lsn)),
            Kind::Trim(through) => state.apply(&Event::Trim(through)),
        }
    }
    let name = name.ok_or_else(|| corrupt(path, "no header record"))?;
    if position < bytes.len() {
        let file = OpenOptions::new().write(true).open(path).map_err(|error| io_error(path, error))?;
        file.set_len(position as u64).and_then(|_| rtdb_storage::sync_file(&file)).map_err(|error| io_error(path, error))?;
    }
    Ok(Recovered { name, path: path.to_path_buf(), state, bytes: position as u64 })
}

/// Stops at the first incomplete or damaged record.
fn next_record(bytes: &Bytes, position: &mut usize) -> Option<Kind> {
    let header = bytes.get(*position..*position + 8)?;
    let length = u32::from_be_bytes(header[0..4].try_into().expect("four bytes")) as usize;
    let checksum = u32::from_be_bytes(header[4..8].try_into().expect("four bytes"));
    let start = *position + 8;
    let body = bytes.get(start..start.checked_add(length)?)?;
    if crc32fast::hash(body) != checksum { return None; }
    let kind = Record::decode(bytes.slice(start..start + length)).ok()?.kind?;
    *position = start + length;
    Some(kind)
}

/// Group commit: whatever has queued while the previous round was syncing goes into the next round together.
pub(crate) async fn run(journal: Arc<Journal>, mut queue: mpsc::UnboundedReceiver<Work>) -> Result<()> {
    const MAXIMUM_ROUND: usize = 8192;
    while let Some(first) = queue.recv().await {
        let mut works = vec![first];
        while works.len() < MAXIMUM_ROUND {
            match queue.try_recv() { Ok(work) => works.push(work), Err(_) => break }
        }
        let journal = Arc::clone(&journal);
        let (works, result) = tokio::task::spawn_blocking(move || { let result = journal.write_round(&works); (works, result) }).await
            .map_err(|error| Anomaly::fault("rtdb.logkeeper/journal-task", error.to_string()))?;
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

    fn round(journal: &Journal, slot: &Arc<Slot>, write: Write) {
        let (done, _) = oneshot::channel();
        journal.write_round(&[Work { slot: Arc::clone(slot), write, done }]).unwrap();
    }

    #[test]
    fn appended_and_rewritten_journals_replay_to_the_decided_state() {
        let directory = tempfile::tempdir().unwrap();
        let journal = Journal::open(directory.path(), true).unwrap();
        let slot = Arc::new(Slot { name: "deployment/1/replica".into(), path: journal.path_for("deployment/1/replica"), state: Default::default(), file: Default::default(), progressed: Default::default() });
        let mut state = StreamState::default();
        let mut events = state.seal(1).events;
        events.extend(state.overwrite(1, 0, Vec::new(), true, -1).events);
        events.extend(state.append(1, 0, (0..4).map(entry).collect(), 2).events);
        round(&journal, &slot, Write::Events(events));
        assert_eq!(journal.recover().unwrap()[0].state, state);
        state.trim(1);
        round(&journal, &slot, Write::Rewrite(state.clone()));
        round(&journal, &slot, Write::Events(state.append(1, 4, vec![entry(4)], 4).events));
        let recovered = journal.recover().unwrap();
        assert_eq!((recovered.len(), recovered[0].name.as_str()), (1, "deployment/1/replica"));
        assert_eq!(recovered[0].state, state);
    }

    #[test]
    fn a_torn_final_record_is_cut_off_and_earlier_records_replay() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("stream.log");
        let mut buffer = BytesMut::new();
        frame(Kind::Header("a/b".into()), &mut buffer);
        frame(Kind::Promise(4), &mut buffer);
        let good = buffer.len();
        frame(Kind::Append(Entry { lsn: 0, epoch: 4, timestamp: 1, payload: Bytes::from_static(b"torn") }), &mut buffer);
        fs::write(&path, &buffer[..buffer.len() - 3]).unwrap();
        let recovered = replay(&path).unwrap();
        assert_eq!(recovered.name, "a/b");
        assert_eq!(recovered.state.promised, 4);
        assert_eq!(recovered.state.last(), -1);
        assert_eq!(fs::metadata(&path).unwrap().len(), good as u64);
    }
}
