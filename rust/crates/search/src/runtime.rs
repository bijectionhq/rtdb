//! A node's shared search state: what scans and the lane reuse across queries, jobs and databases.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::Mutex;
use rtdb_columnar::MemoryBudget;
use rtdb_index::Snapshot;
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Iid, Instant, Result};
use tokio_util::sync::CancellationToken;

use crate::{SearchAccelerator, SearchArtefacts, SearchDefinition, SearchFile, definitions_in};

/// Entries are evicted least recently used first, once the weight of the rest exceeds the limit.
struct Bounded<K, V> { entries: BTreeMap<K, (V, u64, usize)>, clock: u64, weight: usize, limit: usize }

impl<K: Ord + Clone, V: Clone> Bounded<K, V> {
    fn new(limit: usize) -> Self { Self { entries: BTreeMap::new(), clock: 0, weight: 0, limit } }
    fn get(&mut self, key: &K) -> Option<V> {
        self.clock += 1;
        let clock = self.clock;
        self.entries.get_mut(key).map(|(value, used, _)| { *used = clock; value.clone() })
    }
    fn insert(&mut self, key: K, value: V, weight: usize) {
        self.clock += 1;
        if let Some((_, _, previous)) = self.entries.insert(key, (value, self.clock, weight)) { self.weight -= previous; }
        self.weight += weight;
        while self.weight > self.limit && self.entries.len() > 1 {
            let Some(oldest) = self.entries.iter().min_by_key(|(_, (_, used, _))| *used).map(|(key, _)| key.clone()) else { break };
            if let Some((_, _, weight)) = self.entries.remove(&oldest) { self.weight -= weight; }
        }
    }
    fn remove(&mut self, key: &K) { if let Some((_, _, weight)) = self.entries.remove(key) { self.weight -= weight; } }
    fn clear(&mut self) { self.entries.clear(); self.weight = 0; }
}

type DefinitionKey = (String, Vec<(Option<(i64, i64)>, Option<i64>)>);

/// A recorded search file a scan could not read, awaiting the lane's check; its owner is the trie or partition group the file was built over.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Suspect { pub database: String, pub table: rtdb_types::TableRef, pub owner: rtdb_format::keys::ArtefactOwner, pub artefact_id: String, pub version: i64 }

/// Postings over a live segment's rows for one column, valid for every segment that starts with the same batches.
struct LivePostings { batches: Vec<std::sync::Weak<rtdb_index::IndexBatch>>, rows: u32, bytes: usize, postings: std::collections::HashMap<String, Vec<u32>> }

impl LivePostings {
    /// How many leading batches `segment` shares with these postings.
    fn shared_prefix(&self, segment: &rtdb_index::LiveTable) -> usize {
        self.batches.iter().zip(segment.batches()).take_while(|(cached, batch)| cached.upgrade().is_some_and(|cached| Arc::ptr_eq(&cached, batch))).count()
    }
    fn extend(&mut self, segment: &rtdb_index::LiveTable, column: &str) -> Result<()> {
        for row in self.rows..segment.row_count() {
            let event = segment.row(row)?.event()?;
            let rtdb_format::data::StoredOperation::Put(document) = &event.operation else { continue };
            let Some(rtdb_types::Value::Utf8(text)) = document.get(column) else { continue };
            let mut seen = std::collections::BTreeSet::new();
            for token in rtdb_text::tokens(text) {
                if seen.insert(token.clone()) { self.bytes += token.len() + 4; self.postings.entry(token).or_default().push(row); }
            }
        }
        self.batches = segment.batches().map(Arc::downgrade).collect();
        self.rows = segment.row_count();
        Ok(())
    }
}

type LiveKey = (String, rtdb_types::TableRef, String, usize);

/// Every event of a live segment by IID, with its row, valid for every segment that starts with the same batches.
struct LiveKeys { batches: Vec<std::sync::Weak<rtdb_index::IndexBatch>>, rows: u32, events: std::collections::HashMap<[u8; 16], Vec<(u32, crate::file::KeyedEvent)>> }

impl LiveKeys {
    fn shared_prefix(&self, segment: &rtdb_index::LiveTable) -> usize {
        self.batches.iter().zip(segment.batches()).take_while(|(cached, batch)| cached.upgrade().is_some_and(|cached| Arc::ptr_eq(&cached, batch))).count()
    }
    fn extend(&mut self, segment: &rtdb_index::LiveTable) -> Result<()> {
        for row in self.rows..segment.row_count() {
            let header = segment.row(row)?.header()?;
            let event = crate::file::KeyedEvent { system_from: header.system_from.0, valid_from: header.valid_time.lower().0, valid_to: header.valid_time.upper().0, op: header.op };
            self.events.entry(header.iid.0).or_default().push((row, event));
        }
        self.batches = segment.batches().map(Arc::downgrade).collect();
        self.rows = segment.row_count();
        Ok(())
    }
}

/// Why a recorded search file could not be used; only a file that is gone or whose bytes are wrong is worth rebuilding.
#[derive(Debug)]
pub enum FileError { Missing, Corrupt(rtdb_types::Anomaly), Unavailable(rtdb_types::Anomaly) }

impl FileError {
    pub fn needs_repair(&self) -> bool { !matches!(self, Self::Unavailable(_)) }
}

/// An opened file, and the buffer pool it was read through: a pool is one database's storage for as long as it is open.
type OpenedFile = (std::sync::Weak<BufferPool>, Arc<SearchFile>);

/// A trie's search file as a scan reads it: the trie's data file, the identity the file's kept answers go by, the file, and which of its members the trie is.
/// The tries of a partition group share one file, each its own member.
pub struct TrieFile { pub trie: rtdb_format::keys::TrieKey, pub data: rtdb_storage::ObjectKey, pub identity: String, pub file: Arc<SearchFile>, pub member: u32, pub(crate) canonical_uuid_iids: bool }

pub struct SearchRuntime {
    definitions: Mutex<Bounded<DefinitionKey, Arc<Vec<SearchDefinition>>>>,
    live: Mutex<Bounded<LiveKey, Arc<Mutex<LivePostings>>>>,
    live_keys: Mutex<Bounded<(String, rtdb_types::TableRef, usize), Arc<Mutex<LiveKeys>>>>,
    files: Mutex<Bounded<(String, String), OpenedFile>>,
    built: Mutex<Bounded<(String, String), OpenedFile>>,
    /// Builds in progress, which a second query for the same file awaits rather than repeats.
    building: Mutex<BTreeMap<(String, String), Arc<tokio::sync::OnceCell<Arc<SearchFile>>>>>,
    mapped: Mutex<Bounded<(String, String), OpenedFile>>,
    match_sets: Mutex<Bounded<(String, String, String, rtdb_text::Query), (std::sync::Weak<SearchFile>, Arc<crate::file::MatchSet>)>>,
    suspects: Mutex<std::collections::BTreeSet<Suspect>>,
    reported: Mutex<BTreeMap<String, Arc<tokio::sync::Notify>>>,
}

impl SearchRuntime {
    /// `file_bytes` bounds the recorded search files kept across queries, each keeping its buffer-pool lease so its bytes stay charged to the pool; half as much bounds the files a node builds itself, and 32 times as much the files mapped from this node's disk, of which only the pages a query reads are resident.
    pub fn new(file_bytes: usize) -> Arc<Self> {
        Arc::new(Self { definitions: Mutex::new(Bounded::new(256)), live: Mutex::new(Bounded::new(64 << 20)), live_keys: Mutex::new(Bounded::new(256)), files: Mutex::new(Bounded::new(file_bytes)), built: Mutex::new(Bounded::new(file_bytes / 2)), mapped: Mutex::new(Bounded::new(file_bytes.saturating_mul(32))), building: Default::default(), match_sets: Mutex::new(Bounded::new(64 << 20)), suspects: Default::default(), reported: Default::default() })
    }
    pub fn accelerator(self: &Arc<Self>) -> Arc<SearchAccelerator> { Arc::new(SearchAccelerator::with_runtime(Arc::clone(self))) }
    pub fn artefacts(self: &Arc<Self>) -> Arc<SearchArtefacts> { Arc::new(SearchArtefacts::with_runtime(Arc::clone(self))) }

    /// The definitions in force in a database's partitions, read once per committed basis.
    /// They are read as of the latest basis's own system time in valid time, as well as in system time, so the bases alone decide them; partitions carrying a transaction's own writes are read afresh, at `now`.
    pub async fn definitions(&self, database: &str, partitions: &[Snapshot], storage: &Arc<BufferPool>, memory: &Arc<MemoryBudget>, now: Instant, cancellation: &CancellationToken) -> Result<Arc<Vec<SearchDefinition>>> {
        let committed = partitions.iter().all(|snapshot| !snapshot.has_overlays());
        let key = committed.then(|| (database.to_string(), partitions.iter().map(|snapshot| (snapshot.tx_basis().map(|basis| (basis.tx_id.0, basis.system_time.0)), snapshot.system_time_cap().map(|cap| cap.0))).collect()));
        if let Some(definitions) = key.as_ref().and_then(|key| self.definitions.lock().get(key)) { return Ok(definitions); }
        let cap = partitions.iter().map(Snapshot::system_time_cap).collect::<Option<Vec<_>>>().and_then(|caps| caps.into_iter().max());
        let at = match (committed, cap) { (true, Some(cap)) => cap.into(), _ => now };
        let definitions = Arc::new(definitions_in(database, partitions.to_vec(), Arc::clone(storage), Arc::clone(memory), at, cancellation).await?);
        if let Some(key) = key { self.definitions.lock().insert(key, Arc::clone(&definitions), 1); }
        Ok(definitions)
    }

    /// An opened search file, from this node's cache or freshly read; an object key names immutable bytes, so a cached file is never stale.
    /// A kept file is used only while the pool it was read through is the database's pool, so a database detached and attached anew starts afresh.
    pub async fn file(&self, database: &str, storage: &Arc<BufferPool>, key: &rtdb_storage::ObjectKey, columns: &[String]) -> std::result::Result<Arc<SearchFile>, FileError> {
        let cache_key = (database.to_string(), key.as_str().to_string());
        for kept in [&self.mapped, &self.files] {
            if let Some((pool, file)) = kept.lock().get(&cache_key) {
                if pool.upgrade().is_some_and(|pool| Arc::ptr_eq(&pool, storage)) { return Ok(file); }
            }
        }
        let unavailable = |error: rtdb_storage::Error| match error { rtdb_storage::Error::MissingObject(_) => FileError::Missing, error => FileError::Unavailable(error.into()) };
        if let Some(local) = storage.object_store().local_file(key).await.map_err(unavailable)? {
            let size = usize::try_from(local.file().metadata().map_err(|error| FileError::Unavailable(rtdb_types::Anomaly::fault("rtdb.search/file", error.to_string())))?.len()).unwrap_or(usize::MAX);
            let file = Arc::new(SearchFile::open(crate::Source::mapped(local).map_err(FileError::Corrupt)?, columns).map_err(FileError::Corrupt)?);
            self.mapped.lock().insert(cache_key, (Arc::downgrade(storage), Arc::clone(&file)), size);
            return Ok(file);
        }
        let lease = match storage.get(key).await {
            Ok(lease) => lease,
            // Every kept file holds its lease, so a pool they fill refuses the next read; they give way to it.
            Err(rtdb_storage::Error::MemoryLimit { .. }) if self.files.lock().weight > 0 => {
                self.files.lock().clear();
                storage.get(key).await.map_err(unavailable)?
            }
            Err(error) => return Err(unavailable(error)),
        };
        let size = lease.as_slice().len();
        let file = Arc::new(SearchFile::open(lease, columns).map_err(FileError::Corrupt)?);
        self.files.lock().insert(cache_key, (Arc::downgrade(storage), Arc::clone(&file)), size);
        Ok(file)
    }

    /// MATCH of a query on every document of a trie's file, kept for as long as the file is the one this node holds under its identity.
    pub fn match_set(&self, database: &str, trie: &TrieFile, column: &str, query: &rtdb_text::Query) -> Result<Arc<crate::file::MatchSet>> {
        self.match_set_for_scan(database, trie, column, query, None)
    }

    pub(crate) fn match_set_for_scan(&self, database: &str, trie: &TrieFile, column: &str, query: &rtdb_text::Query, iids: Option<&BTreeSet<Iid>>) -> Result<Arc<crate::file::MatchSet>> {
        let cache_key = (database.to_string(), trie.identity.clone(), column.to_string(), query.clone());
        if let Some((opened, set)) = self.match_sets.lock().get(&cache_key) {
            if opened.upgrade().is_some_and(|opened| Arc::ptr_eq(&opened, &trie.file)) { return Ok(set); }
        }
        if let Some(iids) = iids.filter(|iids| iids.len() <= 128) {
            return Ok(Arc::new(trie.file.match_selected_iids(column, query, iids)?));
        }
        let set = Arc::new(trie.file.match_set(column, query)?);
        self.match_sets.lock().insert(cache_key, (Arc::downgrade(&trie.file), Arc::clone(&set)), set.bytes());
        Ok(set)
    }

    /// A search file built on this node from the data file of a trie the lane has not indexed; a data file is immutable, so a kept one is never stale.
    /// The build is charged to `memory` while it runs; the files kept are bounded apart from the recorded ones, so building never evicts those.
    pub async fn built(&self, database: &str, storage: &Arc<BufferPool>, trie: &rtdb_format::keys::TrieKey, data: &rtdb_storage::ObjectKey, columns: &[String], memory: &Arc<MemoryBudget>, cancellation: &CancellationToken) -> Result<TrieFile> {
        let identity = format!("{data}#{columns:?}");
        let cache_key = (database.to_string(), identity.clone());
        if let Some((pool, file)) = self.built.lock().get(&cache_key) {
            if pool.upgrade().is_some_and(|pool| Arc::ptr_eq(&pool, storage)) { return Ok(TrieFile { trie: trie.clone(), data: data.clone(), identity, file, member: 0, canonical_uuid_iids: false }); }
        }
        // A build that fails or whose query is cancelled leaves the cell empty, and the next caller builds.
        let cell = Arc::clone(self.building.lock().entry(cache_key.clone()).or_default());
        let outcome = cell.get_or_try_init(|| async {
            let lease = storage.get(data).await?;
            let ceiling = crate::file::ceiling_of(trie.recency());
            let indexing = memory.reserve(crate::lane::indexing_allowance(i64::try_from(lease.as_slice().len()).unwrap_or(i64::MAX)))?;
            let (indexed, token) = (columns.to_vec(), cancellation.clone());
            let memory = Arc::clone(memory);
            let bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
                let _indexing = indexing;
                let mut bytes = Vec::new();
                let pages = crate::lane::search_pages_with_budget(lease, &indexed, &token, memory)?;
                crate::file::write_search_file(&indexed, pages, "built", ceiling, tantivy::directory::RamDirectory::create(), &mut bytes)?;
                Ok(bytes)
            }).await.map_err(|error| rtdb_types::Anomaly::fault("rtdb.search/build", error.to_string()))??;
            let size = bytes.len();
            let file = Arc::new(SearchFile::open(bytes, columns)?);
            self.built.lock().insert(cache_key.clone(), (Arc::downgrade(storage), Arc::clone(&file)), size);
            Ok::<_, rtdb_types::Anomaly>(file)
        }).await.cloned();
        let mut building = self.building.lock();
        if building.get(&cache_key).is_some_and(|current| Arc::ptr_eq(current, &cell)) { building.remove(&cache_key); }
        drop(building);
        Ok(TrieFile { trie: trie.clone(), data: data.clone(), identity, file: outcome?, member: 0, canonical_uuid_iids: false })
    }

    pub fn forget_file(&self, database: &str, key: &rtdb_storage::ObjectKey) {
        let cache_key = (database.to_string(), key.as_str().to_string());
        for kept in [&self.mapped, &self.files] { kept.lock().remove(&cache_key); }
    }

    /// The rows of a live segment whose column holds every term of the query; phrases are left to the caller.
    /// Postings are kept per segment and extended by the rows appended since, so a growing live index is tokenised once.
    pub fn live_rows(&self, database: &str, segment: &rtdb_index::LiveTable, column: &str, query: &rtdb_text::Query) -> Result<Vec<u32>> {
        let Some(first) = segment.batches().next() else { return Ok(Vec::new()) };
        let key = (database.to_string(), segment.table().clone(), column.to_string(), Arc::as_ptr(first) as usize);
        let entry = {
            let mut live = self.live.lock();
            match live.get(&key) {
                Some(entry) => entry,
                None => {
                    let entry = Arc::new(Mutex::new(LivePostings { batches: Vec::new(), rows: 0, bytes: 0, postings: Default::default() }));
                    live.insert(key.clone(), Arc::clone(&entry), 1);
                    entry
                }
            }
        };
        let mut postings = entry.lock();
        let mut shared = postings.shared_prefix(segment);
        // A segment ending inside the cached batches is an older snapshot of the same rows, answered by filtering; one that diverges from them is indexed afresh.
        if shared < segment.batches().count().min(postings.batches.len()) {
            *postings = LivePostings { batches: Vec::new(), rows: 0, bytes: 0, postings: Default::default() };
            shared = 0;
        }
        if shared == postings.batches.len() && postings.rows < segment.row_count() {
            postings.extend(segment, column)?;
            let bytes = postings.bytes;
            self.live.lock().insert(key, Arc::clone(&entry), bytes.max(1));
        }
        let visible = segment.row_count();
        let mut lists: Vec<&Vec<u32>> = Vec::with_capacity(query.terms().len());
        for term in query.terms() { match postings.postings.get(term) { Some(rows) => lists.push(rows), None => return Ok(Vec::new()) } }
        lists.sort_by_key(|rows| rows.len());
        let Some((shortest, rest)) = lists.split_first() else { return Ok(Vec::new()) };
        Ok(shortest.iter().copied().filter(|row| *row < visible && rest.iter().all(|rows| rows.binary_search(row).is_ok())).collect())
    }

    /// Every event of an entity a live segment holds, system time descending; a segment's keys are kept and extended by the rows appended since, as its postings are.
    pub fn live_events(&self, database: &str, segment: &rtdb_index::LiveTable, iid: &[u8; 16]) -> Result<Vec<crate::file::KeyedEvent>> {
        Ok(self.live_rows_of(database, segment, iid)?.into_iter().map(|(_, event)| event).collect())
    }

    /// Every event of an entity in descending system time and descending live row order.
    fn live_rows_of(&self, database: &str, segment: &rtdb_index::LiveTable, iid: &[u8; 16]) -> Result<Vec<(u32, crate::file::KeyedEvent)>> {
        let keys = self.live_keys_of(database, segment)?;
        let visible = segment.row_count();
        let keys = keys.lock();
        let mut events: Vec<(u32, crate::file::KeyedEvent)> = keys.events.get(iid).map_or_else(Vec::new, |events| events.iter().filter(|(row, _)| *row < visible).copied().collect());
        events.sort_by_key(|(row, event)| (std::cmp::Reverse(event.system_from), std::cmp::Reverse(*row)));
        Ok(events)
    }

    fn live_keys_of(&self, database: &str, segment: &rtdb_index::LiveTable) -> Result<Arc<Mutex<LiveKeys>>> {
        let Some(first) = segment.batches().next() else { return Ok(Arc::new(Mutex::new(LiveKeys { batches: Vec::new(), rows: 0, events: Default::default() }))) };
        let key = (database.to_string(), segment.table().clone(), Arc::as_ptr(first) as usize);
        let existing = self.live_keys.lock().get(&key);
        let entry = match existing {
            Some(entry) => entry,
            None => {
                let entry = Arc::new(Mutex::new(LiveKeys { batches: Vec::new(), rows: 0, events: Default::default() }));
                self.live_keys.lock().insert(key.clone(), Arc::clone(&entry), 1);
                entry
            }
        };
        {
            let mut keys = entry.lock();
            let shared = keys.shared_prefix(segment);
            if shared < segment.batches().count().min(keys.batches.len()) { *keys = LiveKeys { batches: Vec::new(), rows: 0, events: Default::default() }; }
            if keys.shared_prefix(segment) == keys.batches.len() && keys.rows < segment.row_count() { keys.extend(segment)?; }
        }
        Ok(entry)
    }

    /// Hands a recorded file a scan could not read to the database's lane.
    pub fn report(&self, suspect: Suspect) {
        let database = suspect.database.clone();
        if self.suspects.lock().insert(suspect) { self.notifier(&database).notify_one(); }
    }
    fn notifier(&self, database: &str) -> Arc<tokio::sync::Notify> { Arc::clone(self.reported.lock().entry(database.to_string()).or_default()) }
    pub fn suspects(&self, database: &str) -> Vec<Suspect> { self.suspects.lock().iter().filter(|suspect| suspect.database == database).cloned().collect() }
    pub fn is_suspect(&self, suspect: &Suspect) -> bool { self.suspects.lock().contains(suspect) }
    pub fn clear(&self, suspect: &Suspect) { self.suspects.lock().remove(suspect); }
    pub async fn reported(&self, database: &str) { self.notifier(database).notified().await }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_least_recently_used_entries_go_once_the_weight_exceeds_the_limit() {
        let mut cache = Bounded::new(10);
        cache.insert("a", 1, 4);
        cache.insert("b", 2, 4);
        assert_eq!(cache.get(&"a"), Some(1));
        cache.insert("c", 3, 4);
        assert_eq!((cache.get(&"a"), cache.get(&"b"), cache.get(&"c")), (Some(1), None, Some(3)));
        cache.insert("huge", 4, 100);
        assert_eq!(cache.get(&"huge"), Some(4), "an entry heavier than the limit is still kept alone");
        cache.remove(&"huge");
        assert_eq!(cache.weight, 0);
    }

    #[tokio::test]
    async fn a_search_file_on_this_nodes_disk_is_mapped_rather_than_read_into_memory() {
        use rtdb_storage::{LocalStore, ObjectKey, ObjectStore};
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalStore::open(directory.path()).unwrap());
        let pool = Arc::new(BufferPool::new(store.clone(), 1 << 24));
        let columns = vec!["body".to_string()];
        let rows = vec![crate::SearchRow { identity: None, iid: [1; 16], system_from: 0, valid_from: 0, valid_to: i64::MAX, op: rtdb_types::EventOp::Put, values: vec![Some(rtdb_text::tokens("an overdue invoice"))] }];
        let key = ObjectKey::new("tables/docs/artefacts/l01-rc-b00/search/docs~a2f3-v1.bin").unwrap();
        store.put(&key, crate::build_search_file(&columns, &rows, "job").unwrap().into()).await.unwrap();
        let runtime = SearchRuntime::new(1 << 20);
        let file = runtime.file("xtdb", &pool, &key, &columns).await.unwrap();
        assert_eq!(pool.retained_bytes(), 0, "nothing was read through the buffer pool");
        store.delete(&key).await.unwrap();
        assert_eq!(file.matches("body", &rtdb_text::Query::parse("overdue"), rtdb_types::TemporalBounds { valid_time: rtdb_types::Interval::ALL, system_time: rtdb_types::Interval::ALL }).unwrap().len(), 1, "the mapping outlives the delete");
        assert!(Arc::ptr_eq(&file, &runtime.file("xtdb", &pool, &key, &columns).await.unwrap()), "a mapped file is kept");
        runtime.forget_file("xtdb", &key);
        assert!(matches!(runtime.file("xtdb", &pool, &key, &columns).await, Err(FileError::Missing)));
        store.close_and_wait().await;
    }

    #[tokio::test]
    async fn queries_building_the_same_file_at_once_share_one_build() {
        use rtdb_storage::{MemoryStore, ObjectKey, ObjectStore};
        let store = Arc::new(MemoryStore::default());
        let pool = Arc::new(BufferPool::new(store.clone(), 1 << 24));
        let (memory, cancellation) = (rtdb_columnar::MemoryBudget::new(1 << 30), CancellationToken::new());
        let data = ObjectKey::new("tables/docs/data/l00-rc-b00.arrow").unwrap();
        let data_file = || {
            let event = rtdb_format::data::StoredEvent { commit_position: 0, entity_key: None, iid: rtdb_types::Iid([1; 16]), system_from: rtdb_types::Timestamp(1), valid_time: rtdb_types::Interval::new(rtdb_types::Timestamp(0), rtdb_types::Timestamp::MAX).unwrap(),
                operation: rtdb_format::data::StoredOperation::Put(rtdb_types::StructFields::from([("body".into(), rtdb_types::Value::Utf8("an overdue invoice".into()))])), recency: None };
            let batch = rtdb_format::data::build_event_batch(&[event], Some(rtdb_types::StructFields::from([("body".into(), rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Utf8))])), None).unwrap();
            bytes::Bytes::from(rtdb_format::ipc::write_file(&batch.schema(), &[batch]).unwrap())
        };
        store.put(&data, data_file()).await.unwrap();
        let columns = vec!["body".to_string()];
        let runtime = SearchRuntime::new(1 << 20);
        let trie: rtdb_format::keys::TrieKey = "l00-rc-b00".parse().unwrap();
        let (first, second) = tokio::join!(runtime.built("xtdb", &pool, &trie, &data, &columns, &memory, &cancellation), runtime.built("xtdb", &pool, &trie, &data, &columns, &memory, &cancellation));
        assert!(Arc::ptr_eq(&first.unwrap().file, &second.unwrap().file));
        assert!(runtime.building.lock().is_empty(), "a finished build leaves nothing in flight");
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let other = ObjectKey::new("tables/docs/data/l00-rc-b01.arrow").unwrap();
        store.put(&other, data_file()).await.unwrap();
        let _ = runtime.built("xtdb", &pool, &"l00-rc-b01".parse().unwrap(), &other, &columns, &memory, &cancelled).await;
        assert!(runtime.built("xtdb", &pool, &"l00-rc-b01".parse().unwrap(), &other, &columns, &memory, &cancellation).await.is_ok(), "a cancelled build leaves the next caller free to build");
    }

    #[tokio::test]
    async fn kept_files_give_way_to_a_buffer_pool_they_have_filled() {
        use rtdb_storage::{MemoryStore, ObjectKey, ObjectStore};
        let store = Arc::new(MemoryStore::default());
        let columns = vec!["body".to_string()];
        let rows: Vec<_> = (0..3000).map(|index| crate::SearchRow { identity: None, iid: [(index / 12) as u8; 16], system_from: -(index as i64), valid_from: 0, valid_to: i64::MAX, op: rtdb_types::EventOp::Put,
            values: vec![Some(rtdb_text::tokens(&format!("word{index} another{} overdue", index * 7)))] }).collect();
        let bytes = crate::build_search_file(&columns, &rows, "job").unwrap();
        let pool = Arc::new(BufferPool::new(store.clone(), bytes.len() * 2 + bytes.len() / 2));
        let runtime = SearchRuntime::new(1 << 30);
        let keys: Vec<_> = (0..4).map(|index| ObjectKey::new(format!("tables/docs/artefacts/l02-rc-p{index}-b00/search/docs~a2f3-v1.bin")).unwrap()).collect();
        for key in &keys { store.put(key, bytes.clone().into()).await.unwrap(); }
        for key in &keys { assert!(runtime.file("xtdb", &pool, key, &columns).await.is_ok(), "{key}"); }
    }

    #[tokio::test]
    async fn an_opened_search_file_is_read_once_and_reopened_only_after_it_is_forgotten() {
        use rtdb_storage::{MemoryStore, ObjectKey, ObjectStore};
        let store = Arc::new(MemoryStore::default());
        let pool = Arc::new(BufferPool::new(store.clone(), 1 << 24));
        let columns = vec!["body".to_string()];
        let key = ObjectKey::new("tables/docs/artefacts/l01-rc-b00/search/docs~a1f1-v1.bin").unwrap();
        store.put(&key, crate::build_search_file(&columns, &[], "job").unwrap().into()).await.unwrap();
        let runtime = SearchRuntime::new(1 << 20);
        let first = runtime.file("xtdb", &pool, &key, &columns).await.unwrap();
        store.delete(&key).await.unwrap();
        pool.invalidate(&key).await.unwrap();
        assert!(Arc::ptr_eq(&first, &runtime.file("xtdb", &pool, &key, &columns).await.unwrap()), "a cached file needs no read");
        assert!(runtime.file("other", &pool, &key, &columns).await.is_err(), "databases do not share entries");
        runtime.forget_file("xtdb", &key);
        assert!(matches!(runtime.file("xtdb", &pool, &key, &columns).await, Err(FileError::Missing)));
        let reattached = Arc::new(BufferPool::new(store.clone(), 1 << 24));
        store.put(&key, b"garbage".to_vec().into()).await.unwrap();
        assert!(matches!(runtime.file("xtdb", &reattached, &key, &columns).await, Err(FileError::Corrupt(_))), "a pool the entry was not read through starts afresh");
    }
}
