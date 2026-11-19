//! A node's shared search state: what scans and the lane reuse across queries, jobs and databases.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;
use rtdb_columnar::MemoryBudget;
use rtdb_index::Snapshot;
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Instant, Result};
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
}

type DefinitionKey = (String, Option<(i64, i64)>, Option<i64>);

/// A recorded search file a scan could not read, awaiting the lane's check.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Suspect { pub database: String, pub table: rtdb_types::TableRef, pub trie: rtdb_format::keys::TrieKey, pub artefact_id: String, pub version: i64 }

/// Postings over a live segment's rows for one column, valid for every segment that starts with the same batches.
struct LivePostings { batches: Vec<std::sync::Weak<rtdb_index::IndexBatch>>, rows: u32, postings: std::collections::HashMap<String, Vec<u32>> }

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
            for token in rtdb_text::tokens(text) { if seen.insert(token.clone()) { self.postings.entry(token).or_default().push(row); } }
        }
        self.batches = segment.batches().map(Arc::downgrade).collect();
        self.rows = segment.row_count();
        Ok(())
    }
}

type LiveKey = (String, rtdb_types::TableRef, String, usize);

pub struct SearchRuntime {
    definitions: Mutex<Bounded<DefinitionKey, Arc<Vec<SearchDefinition>>>>,
    live: Mutex<Bounded<LiveKey, Arc<Mutex<LivePostings>>>>,
    files: Mutex<Bounded<(String, String), Arc<SearchFile>>>,
    suspects: Mutex<std::collections::BTreeSet<Suspect>>,
    reported: tokio::sync::Notify,
}

impl SearchRuntime {
    /// `file_bytes` bounds the opened search files kept across queries; each keeps its buffer-pool lease, so its bytes stay charged to the pool.
    pub fn new(file_bytes: usize) -> Arc<Self> {
        Arc::new(Self { definitions: Mutex::new(Bounded::new(256)), live: Mutex::new(Bounded::new(4 << 20)), files: Mutex::new(Bounded::new(file_bytes)), suspects: Default::default(), reported: tokio::sync::Notify::new() })
    }
    pub fn accelerator(self: &Arc<Self>) -> Arc<SearchAccelerator> { Arc::new(SearchAccelerator::with_runtime(Arc::clone(self))) }
    pub fn artefacts(self: &Arc<Self>) -> Arc<SearchArtefacts> { Arc::new(SearchArtefacts::with_runtime(Arc::clone(self))) }

    /// The definitions in force at a snapshot, read once per committed basis.
    /// Definitions are documents valid from their transaction onwards, so the basis alone decides them; a snapshot carrying a transaction's own writes is read afresh.
    pub async fn definitions(&self, database: &str, snapshot: &Snapshot, storage: &Arc<BufferPool>, memory: &Arc<MemoryBudget>, now: Instant, cancellation: &CancellationToken) -> Result<Arc<Vec<SearchDefinition>>> {
        let key = (!snapshot.has_overlays()).then(|| (database.to_string(), snapshot.tx_basis().map(|basis| (basis.tx_id.0, basis.system_time.0)), snapshot.system_time_cap().map(|cap| cap.0)));
        if let Some(definitions) = key.as_ref().and_then(|key| self.definitions.lock().get(key)) { return Ok(definitions); }
        let definitions = Arc::new(definitions_in(database, vec![snapshot.clone()], Arc::clone(storage), Arc::clone(memory), now, cancellation).await?);
        if let Some(key) = key { self.definitions.lock().insert(key, Arc::clone(&definitions), 1); }
        Ok(definitions)
    }

    /// An opened search file, from this node's cache or freshly read; an object key names immutable bytes, so a cached file is never stale.
    pub async fn file(&self, database: &str, storage: &BufferPool, key: &rtdb_storage::ObjectKey, columns: &[String]) -> Result<Arc<SearchFile>> {
        let cache_key = (database.to_string(), key.as_str().to_string());
        if let Some(file) = self.files.lock().get(&cache_key) { return Ok(file); }
        let lease = storage.get(key).await?;
        let size = lease.as_slice().len();
        let file = Arc::new(SearchFile::open(lease, columns)?);
        self.files.lock().insert(cache_key, Arc::clone(&file), size);
        Ok(file)
    }

    pub fn forget_file(&self, database: &str, key: &rtdb_storage::ObjectKey) { self.files.lock().remove(&(database.to_string(), key.as_str().to_string())); }

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
                    let entry = Arc::new(Mutex::new(LivePostings { batches: Vec::new(), rows: 0, postings: Default::default() }));
                    live.insert(key.clone(), Arc::clone(&entry), segment.row_count().max(1) as usize);
                    entry
                }
            }
        };
        let mut postings = entry.lock();
        let shared = postings.shared_prefix(segment);
        let batches = segment.batches().count();
        // A segment ending inside the cached batches is an older snapshot of the same rows, answered by filtering; anything else that diverges is rebuilt.
        if shared < batches.min(postings.batches.len()) { *postings = LivePostings { batches: Vec::new(), rows: 0, postings: Default::default() }; }
        if shared == postings.batches.len() && postings.rows < segment.row_count() {
            postings.extend(segment, column)?;
            self.live.lock().insert(key, Arc::clone(&entry), segment.row_count().max(1) as usize);
        }
        let visible = segment.row_count();
        let mut lists: Vec<&Vec<u32>> = Vec::with_capacity(query.terms().len());
        for term in query.terms() { match postings.postings.get(term) { Some(rows) => lists.push(rows), None => return Ok(Vec::new()) } }
        lists.sort_by_key(|rows| rows.len());
        let Some((shortest, rest)) = lists.split_first() else { return Ok(Vec::new()) };
        Ok(shortest.iter().copied().filter(|row| *row < visible && rest.iter().all(|rows| rows.binary_search(row).is_ok())).collect())
    }

    /// Hands a recorded file a scan could not read to the database's lane.
    pub fn report(&self, suspect: Suspect) { if self.suspects.lock().insert(suspect) { self.reported.notify_one(); } }
    pub fn suspects(&self, database: &str) -> Vec<Suspect> { self.suspects.lock().iter().filter(|suspect| suspect.database == database).cloned().collect() }
    pub fn is_suspect(&self, suspect: &Suspect) -> bool { self.suspects.lock().contains(suspect) }
    pub fn clear(&self, suspect: &Suspect) { self.suspects.lock().remove(suspect); }
    pub async fn reported(&self) { self.reported.notified().await }
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
    async fn an_opened_search_file_is_read_once_and_reopened_only_after_it_is_forgotten() {
        use rtdb_storage::{MemoryStore, ObjectKey, ObjectStore};
        let store = Arc::new(MemoryStore::default());
        let pool = BufferPool::new(store.clone(), 1 << 24);
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
        assert!(runtime.file("xtdb", &pool, &key, &columns).await.is_err());
    }
}
