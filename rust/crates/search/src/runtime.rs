//! A node's shared search state: what scans and the lane reuse across queries, jobs and databases.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, LargeListArray, StringArray, UInt32Array,
    builder::{LargeListBuilder, StringBuilder, UInt32Builder}, cast::AsArray, types::UInt32Type};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use parking_lot::Mutex;
use rtdb_columnar::MemoryBudget;
use rtdb_format::data::PutText;
use rtdb_index::Snapshot;
use rtdb_storage::{ObjectKey, cache::BufferPool};
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

const LIVE_POSTING_BYTES: usize = 64 << 20;

type DefinitionKey = (String, Vec<(Option<(i64, i64)>, Option<i64>)>);

/// A recorded search file a scan could not read, awaiting the lane's check; its owner is the trie or partition group the file was built over.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Suspect { pub database: String, pub table: rtdb_types::TableRef, pub owner: rtdb_format::keys::ArtefactOwner, pub artefact_id: String, pub version: i64 }

pub(crate) struct LivePostingPage {
    first_row: u32,
    terms: StringArray,
    rows: LargeListArray,
    tokens: LargeListArray,
    peak_scores: Float64Array,
    peak_ids: Int64Array,
    minimum_id: Option<i64>,
    _reservation: Option<Arc<rtdb_columnar::MemoryReservation>>,
}

impl LivePostingPage {
    fn bytes(&self) -> usize {
        self.terms.get_array_memory_size() + self.rows.get_array_memory_size() + self.tokens.get_array_memory_size()
            + self.peak_scores.get_array_memory_size() + self.peak_ids.get_array_memory_size() + 256
    }

    fn from_rows(segment: &rtdb_index::LiveTable, column: &str, from: u32, to: u32,
        available: usize, cancellation: &CancellationToken,
        prepared: Option<&dyn Fn(u32) -> Option<StringArray>>) -> Result<Option<Self>> {
        let mut postings = BTreeMap::<String, (Vec<u32>, crate::rank_cutoff::RankKey)>::new();
        let mut tokens = LargeListBuilder::new(StringBuilder::new());
        let mut minimum_id: Option<i64> = Some(i64::MAX);
        let mut estimated = 4096 + (to - from) as usize * 32;
        for row in from..to {
            if cancellation.is_cancelled() { return Ok(None); }
            let source = segment.row(row)?;
            let event = source.batch.view().borrow_event(source.row)?;
            if let PutText::Utf8(text) = event.put_text(column)? {
                estimated = estimated.saturating_add(text.len().saturating_mul(48));
                if estimated > available { return Ok(None); }
                let id = crate::lane::integer_sql_id(&event);
                minimum_id = minimum_id.zip(id).map(|(left, right)| left.min(right));
                let analyzed = if let Some(prepared) = prepared {
                    prepared(row).ok_or_else(|| rtdb_types::Anomaly::fault("rtdb.search/invalid-delta", "Committed text row has no token sequence"))?
                } else { rtdb_text::analyze(text) };
                let mut frequencies = BTreeMap::<&str, u32>::new();
                for token in analyzed.iter().flatten() {
                    tokens.values().append_value(token);
                    *frequencies.entry(token).or_default() += 1;
                }
                for (term, frequency) in frequencies {
                    let score = rtdb_text::unit_term_score(frequency);
                    let (rows, peak) = postings.entry(term.to_owned()).or_insert_with(|| {
                        (Vec::new(), crate::rank_cutoff::RankKey::integer(score, id))
                    });
                    rows.push(row);
                    if score > peak.score {
                        *peak = crate::rank_cutoff::RankKey::integer(score, id);
                    } else if score == peak.score {
                        let minimum = peak.integer_id().zip(id).map(|(left, right)| left.min(right));
                        *peak = crate::rank_cutoff::RankKey::integer(peak.score, minimum);
                    }
                }
                tokens.append(true);
            } else {
                tokens.append(false);
            }
        }
        let (terms, entries): (Vec<_>, Vec<_>) = postings.into_iter().unzip();
        let (postings, peaks): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
        let offsets = OffsetBuffer::from_lengths(postings.iter().map(Vec::len));
        let rows = UInt32Array::from_iter_values(postings.into_iter().flatten());
        let field = Arc::new(Field::new("item", DataType::UInt32, false));
        Ok(Some(Self { first_row: from, terms: StringArray::from(terms),
            rows: LargeListArray::new(field, offsets, Arc::new(rows), None), tokens: tokens.finish(),
            peak_scores: Float64Array::from_iter_values(peaks.iter().map(|peak| peak.score)),
            peak_ids: Int64Array::from_iter(peaks.into_iter().map(crate::rank_cutoff::RankKey::integer_id)), minimum_id, _reservation: None }))
    }

    fn term_index(&self, term: &str) -> Option<usize> {
        let mut first = 0;
        let mut end = self.terms.len();
        while first < end {
            let middle = first + (end - first) / 2;
            if self.terms.value(middle) < term { first = middle + 1; } else { end = middle; }
        }
        (first < self.terms.len() && self.terms.value(first) == term).then_some(first)
    }

    fn postings(&self, term: &str) -> Option<&[u32]> {
        let first = self.term_index(term)?;
        let offsets = self.rows.value_offsets();
        let rows = self.rows.values().as_primitive::<UInt32Type>();
        Some(&rows.values()[offsets[first] as usize..offsets[first + 1] as usize])
    }

    pub(crate) fn bound(&self, matching: &rtdb_text::Query, scoring: &rtdb_text::Query, ordered_ids: bool) -> Option<crate::rank_cutoff::RankKey> {
        if matching.terms().is_empty() || matching.terms().iter().any(|term| self.term_index(term).is_none()) { return None; }
        let score = scoring.terms().iter().filter_map(|term| self.term_index(term))
            .map(|index| self.peak_scores.value(index)).fold(0.0, |left, right| left + right);
        let id = if ordered_ids {
            match scoring.terms() {
                [term] if score > 0.0 => self.term_index(term)
                    .filter(|&index| self.peak_ids.is_valid(index)).map(|index| self.peak_ids.value(index)).or(self.minimum_id),
                _ => self.minimum_id,
            }
        } else { None };
        Some(crate::rank_cutoff::RankKey::integer(score, id))
    }

    pub(crate) fn visit(&self, query: &rtdb_text::Query, phrases: bool,
        mut visit: impl FnMut(u32, &dyn Fn() -> StringArray) -> Result<bool>) -> Result<bool> {
        let Some(mut lists) = query.terms().iter().map(|term| self.postings(term)).collect::<Option<Vec<_>>>() else { return Ok(true); };
        lists.sort_by_key(|rows| rows.len());
        let Some((shortest, rest)) = lists.split_first() else { return Ok(true); };
        for &row in *shortest {
            rtdb_storage::audience_work::scan_items(1)?;
            if !rest.iter().all(|other| other.binary_search(&row).is_ok()) { continue; }
            if phrases && !query.matches_arrow(&self.tokens(row)) { continue; }
            if !visit(row, &|| self.tokens(row))? { return Ok(false); }
        }
        Ok(true)
    }

    fn tokens(&self, row: u32) -> StringArray {
        let row = (row - self.first_row) as usize;
        let offsets = self.tokens.value_offsets();
        let first = offsets[row] as usize;
        self.tokens.values().as_string::<i32>().slice(first, offsets[row + 1] as usize - first)
    }
}

#[derive(Clone)]
struct ServingColumn {
    segment: std::sync::Weak<rtdb_index::LiveTable>,
    postings: LivePostings,
}

pub(crate) struct ServingPostings {
    block: rtdb_types::BlockIndex,
    columns: BTreeMap<(rtdb_types::TableRef, String), ServingColumn>,
}

impl ServingPostings {
    pub fn prepare(snapshot: &Snapshot, definitions: &[SearchDefinition], budget: &Arc<MemoryBudget>,
        cancellation: &CancellationToken) -> Result<Self> {
        let columns: BTreeSet<_> = definitions.iter().flat_map(|definition| {
            definition.columns.iter().map(|column| (definition.table.clone(), column.clone()))
        }).collect();
        Self::extend(snapshot, columns, None, budget, cancellation)
    }

    fn extend(snapshot: &Snapshot, columns: BTreeSet<crate::delta::Column>,
        commit: Option<(&crate::delta::Delta, &rtdb_index::StagedWrite)>, budget: &Arc<MemoryBudget>,
        cancellation: &CancellationToken) -> Result<Self> {
        let block = snapshot.block_index();
        let previous = snapshot.serving_search::<Self>().filter(|previous| previous.block == block);
        let mut indexed = if commit.is_some() { Self::preserve(snapshot).columns } else { BTreeMap::new() };
        for key in columns {
            let (table, column) = &key;
            let Some(segment) = snapshot.iter_segments(table).next() else { continue; };
            let mut postings = previous.and_then(|previous| previous.columns.get(&key)).map(|entry| entry.postings.clone()).unwrap_or_default();
            let first = match commit {
                Some((_, write)) => segment.row_count().checked_sub(write.tables()[table].row_count() as u32)
                    .filter(|first| *first == postings.rows),
                None => Some(postings.rows),
            };
            let Some(first) = first else { continue; };
            let tokens = |row: u32| commit.and_then(|(delta, _)| delta.tokens(&key, (row - first) as usize));
            let prepared = commit.map(|_| &tokens as &dyn Fn(u32) -> Option<StringArray>);
            while postings.rows < segment.row_count() {
                if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
                let from = postings.rows;
                let to = from.saturating_add(1024).min(segment.row_count());
                let mut bytes = 16_384 + (to - from) as usize * 128;
                for row in from..to {
                    let source = segment.row(row)?;
                    if let PutText::Utf8(value) = source.batch.view().borrow_event(source.row)?.put_text(column)? {
                        bytes = bytes.saturating_add(value.len().saturating_mul(96));
                    }
                }
                let Ok(mut reservation) = budget.reserve(bytes) else { break; };
                let Some(mut page) = LivePostingPage::from_rows(segment, column, from, to, bytes, cancellation, prepared)? else { break; };
                let retained = page.bytes();
                if retained > bytes { break; }
                Arc::get_mut(&mut reservation).expect("unpublished search page owns its reservation").shrink_to(retained)?;
                page._reservation = Some(reservation);
                postings.pages.push(Arc::new(page));
                postings.rows = to;
                postings.bytes += retained;
            }
            let column = ServingColumn { segment: Arc::downgrade(segment), postings };
            indexed.insert(key, column);
        }
        Ok(Self { block, columns: indexed })
    }

    fn preserve(snapshot: &Snapshot) -> Self {
        let block = snapshot.block_index();
        let columns = snapshot.serving_search::<Self>().filter(|previous| previous.block == block)
            .into_iter().flat_map(|previous| &previous.columns).filter_map(|(key, entry)| {
                let segment = snapshot.iter_segments(&key.0).next()?;
                Some((key.clone(), ServingColumn { segment: Arc::downgrade(segment), postings: entry.postings.clone() }))
            }).collect();
        Self { block, columns }
    }

    fn pages(&self, segment: &rtdb_index::LiveTable, column: &str) -> Option<(Vec<Arc<LivePostingPage>>, u32)> {
        let entry = self.columns.get(&(segment.table().clone(), column.to_owned()))?;
        if !std::ptr::eq(entry.segment.as_ptr(), segment) { return None; }
        Some((entry.postings.pages.clone(), entry.postings.rows))
    }

    pub(crate) fn tokens_for_row(&self, segment: &rtdb_index::LiveTable, column: &str, row: u32) -> Option<StringArray> {
        let entry = self.columns.get(&(segment.table().clone(), column.to_owned()))?;
        if !std::ptr::eq(entry.segment.as_ptr(), segment) || row >= entry.postings.rows { return None; }
        let index = entry.postings.pages.partition_point(|page| page.first_row <= row).checked_sub(1)?;
        let page = &entry.postings.pages[index];
        page.tokens.is_valid((row - page.first_row) as usize).then(|| page.tokens(row))
    }
}

#[derive(Clone, Default)]
struct LivePostings {
    lineage: Option<rtdb_index::LiveRowProof>,
    rows: u32,
    pages: Vec<Arc<LivePostingPage>>,
    bytes: usize,
}

impl LivePostings {
    fn extend(&mut self, segment: &rtdb_index::LiveTable, column: &str, cancellation: &CancellationToken) -> Result<()> {
        let mut from = self.rows;
        while from < segment.row_count() {
            let to = from.saturating_add(1024).min(segment.row_count());
            let remaining = LIVE_POSTING_BYTES.saturating_sub(self.bytes);
            let Some(page) = LivePostingPage::from_rows(segment, column, from, to, remaining, cancellation, None)? else { break; };
            let bytes = page.bytes();
            if bytes > remaining { break; }
            self.pages.push(Arc::new(page));
            self.bytes += bytes;
            from = to;
        }
        self.lineage = Some(segment.row_lineage().proof());
        self.rows = from;
        Ok(())
    }
}

type LiveKey = (String, rtdb_types::TableRef, String, usize);

/// Why a recorded search file could not be used; only a file that is gone or whose bytes are wrong is worth rebuilding.
#[derive(Debug)]
pub enum FileError { Missing, Corrupt(rtdb_types::Anomaly), Unavailable(rtdb_types::Anomaly) }

impl FileError {
    pub fn needs_repair(&self) -> bool { !matches!(self, Self::Unavailable(_)) }

    pub(crate) fn from_anomaly(error: rtdb_types::Anomaly) -> Self {
        match error.code.as_str() {
            "rtdb.storage/missing-object" => Self::Missing,
            "rtdb.search/file" | "rtdb.search/corrupt-file" | "rtdb.storage/invalid-range" => Self::Corrupt(error),
            _ => Self::Unavailable(error),
        }
    }
}

/// An opened file, and the buffer pool it was read through: a pool is one database's storage for as long as it is open.
type OpenedFile = (std::sync::Weak<BufferPool>, Arc<SearchFile>);
type OpenedTreeKey = (usize, ObjectKey, [u8; 32]);
type OpenedTree = (std::sync::Weak<BufferPool>, Arc<crate::file::TreeFile>);

/// A trie's search file as a scan reads it: the trie's data file, the identity the file's kept answers go by, the file, and which of its members the trie is.
/// The tries of a partition group share one file, each its own member.
pub struct TrieFile { pub trie: rtdb_format::keys::TrieKey, pub data: rtdb_storage::ObjectKey, pub identity: String, pub file: Arc<SearchFile>, pub member: u32 }

pub struct SearchRuntime {
    write_format: i32,
    search_pages: Arc<crate::file::PageCache>,
    trees: Mutex<Bounded<OpenedTreeKey, OpenedTree>>,
    routing: Mutex<Bounded<crate::accelerate::routing::RoutingKey, Arc<crate::accelerate::routing::TableRouting>>>,
    file_admission: Arc<MemoryBudget>,
    serving_admission: Arc<MemoryBudget>,
    serving_gaps: std::sync::atomic::AtomicU64,
    logical_run_reads: std::sync::atomic::AtomicU64,
    tree_conversions: Mutex<BTreeMap<OpenedTreeKey, Arc<tokio::sync::OnceCell<Arc<SearchFile>>>>>,
    definitions: Mutex<Bounded<DefinitionKey, Arc<Vec<SearchDefinition>>>>,
    live: Mutex<Bounded<LiveKey, Arc<LivePostings>>>,
    files: Mutex<Bounded<(String, String), OpenedFile>>,
    built: Mutex<Bounded<(String, String), OpenedFile>>,
    /// Builds in progress, which a second query for the same file awaits rather than repeats.
    building: Mutex<BTreeMap<(usize, String, String), Arc<tokio::sync::OnceCell<Arc<SearchFile>>>>>,
    mapped: Mutex<Bounded<(String, String), OpenedFile>>,
    match_sets: Mutex<Bounded<(String, String, String, rtdb_text::Query), (std::sync::Weak<SearchFile>, Arc<crate::file::MatchSet>)>>,
    suspects: Mutex<std::collections::BTreeSet<Suspect>>,
    suspect_runs: Mutex<BTreeSet<(String, rtdb_types::TableRef, [u8; 32])>>,
    suspect_term_directories: Mutex<BTreeSet<(String, rtdb_types::TableRef, String, i64)>>,
    reported: Mutex<BTreeMap<String, Arc<tokio::sync::Notify>>>,
}

impl SearchRuntime {
    /// `file_bytes` weights retained recorded files by whole-object or remote metadata size.
    /// Locally built files have half the allowance and mapped compatibility files have 32 times the allowance.
    /// The format 14 page cache has one quarter of the allowance, and the opened tree cache has eight times the allowance for roots and mapped objects.
    /// Compatibility conversion shares a separate allowance of `file_bytes`.
    pub fn new(file_bytes: usize) -> Arc<Self> {
        Self::with_paged_files(file_bytes, true)
    }

    pub fn with_paged_files(file_bytes: usize, paged_files: bool) -> Arc<Self> {
        let write_format = if paged_files { crate::file::FORMAT_VERSION } else { crate::file::LEGACY_FORMAT_VERSION };
        Self::with_format(file_bytes, write_format)
    }

    pub fn with_tree_files(file_bytes: usize) -> Arc<Self> { Self::with_format(file_bytes, crate::file::TREE_FORMAT_VERSION) }
    pub fn with_search_packs(file_bytes: usize) -> Arc<Self> { Self::with_format(file_bytes, crate::file::PACKED_FORMAT_VERSION) }
    pub fn with_logical_search(file_bytes: usize) -> Arc<Self> { Self::with_format(file_bytes, crate::file::LOGICAL_FORMAT_VERSION) }
    pub fn with_tantivy_files(file_bytes: usize) -> Arc<Self> { Self::with_format(file_bytes, crate::file::TANTIVY_FORMAT_VERSION) }

    pub(crate) fn for_query(memory: Arc<MemoryBudget>) -> Arc<Self> {
        Self::with_admission(0, crate::file::FORMAT_VERSION, memory)
    }

    fn with_format(file_bytes: usize, write_format: i32) -> Arc<Self> {
        Self::with_admission(file_bytes, write_format, MemoryBudget::new(file_bytes))
    }

    fn with_admission(file_bytes: usize, write_format: i32, file_admission: Arc<MemoryBudget>) -> Arc<Self> {
        Arc::new(Self { write_format, search_pages: Arc::new(crate::file::PageCache::new(file_bytes / 4)),
            trees: Mutex::new(Bounded::new(file_bytes.saturating_mul(8))),
            routing: Mutex::new(Bounded::new(file_bytes / 16)),
            file_admission, serving_admission: MemoryBudget::new(LIVE_POSTING_BYTES.min(file_bytes)), serving_gaps: Default::default(),
            logical_run_reads: Default::default(),
            tree_conversions: Default::default(), definitions: Mutex::new(Bounded::new(256)), live: Mutex::new(Bounded::new(LIVE_POSTING_BYTES)),
            files: Mutex::new(Bounded::new(file_bytes)), built: Mutex::new(Bounded::new(file_bytes / 2)),
            mapped: Mutex::new(Bounded::new(file_bytes.saturating_mul(32))), building: Default::default(),
            match_sets: Mutex::new(Bounded::new(64 << 20)), suspects: Default::default(), suspect_runs: Default::default(),
            suspect_term_directories: Default::default(),
            reported: Default::default() })
    }

    pub(crate) fn write_format(&self) -> i32 { self.write_format }

    pub fn serving_gaps(&self) -> u64 { self.serving_gaps.load(std::sync::atomic::Ordering::Relaxed) }

    pub(crate) fn record_serving_gap(&self) { self.serving_gaps.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }

    pub fn logical_run_reads(&self) -> u64 { self.logical_run_reads.load(std::sync::atomic::Ordering::Relaxed) }

    pub(crate) fn record_logical_run_read(&self) { self.logical_run_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }

    pub(crate) fn table_routing(&self, snapshot: &Snapshot, definition: &SearchDefinition, storage: &BufferPool, include_packs: bool, history: bool)
        -> Result<Arc<crate::accelerate::routing::TableRouting>> {
        use crate::accelerate::routing::TableRouting;

        let key = TableRouting::key(snapshot, definition, if history { 2 } else { u8::from(include_packs) });
        let share = storage.prepaid_metadata_budget().is_none();
        if share && let Some(routing) = self.routing.lock().get(&key).filter(|routing| routing.belongs_to(snapshot)) {
            return Ok(routing);
        }
        let budget = storage.prepaid_metadata_budget();
        let routing = Arc::new(if history { TableRouting::history(snapshot, definition, budget)? }
            else { TableRouting::open(snapshot, definition, include_packs, budget)? });
        if share {
            let mut kept = self.routing.lock();
            if routing.bytes <= kept.limit { kept.insert(key, Arc::clone(&routing), routing.bytes); }
        }
        Ok(routing)
    }

    pub(crate) fn prepare_serving_search(&self, snapshot: &Snapshot, definitions: &[SearchDefinition],
        cancellation: &CancellationToken) -> Result<rtdb_index::ServingSearch> {
        Ok(rtdb_index::ServingSearch::new(ServingPostings::prepare(snapshot, definitions, &self.serving_admission, cancellation)?))
    }

    pub(crate) fn prepare_delta(&self, definitions: &[SearchDefinition], write: &rtdb_index::StagedWrite,
        cancellation: &CancellationToken) -> Result<Option<bytes::Bytes>> {
        crate::delta::prepare(definitions, write, &self.serving_admission, cancellation)
    }

    pub(crate) fn install_delta(&self, snapshot: &Snapshot, write: &rtdb_index::StagedWrite, encoded: Option<&[u8]>,
        cancellation: &CancellationToken) -> Result<rtdb_index::ServingSearch> {
        let Some(encoded) = encoded else { return Ok(rtdb_index::ServingSearch::new(ServingPostings::preserve(snapshot))); };
        let result = crate::delta::Delta::read(encoded, write, &self.serving_admission).and_then(|delta| {
            let columns = delta.columns().cloned().collect();
            ServingPostings::extend(snapshot, columns, Some((&delta, write)), &self.serving_admission, cancellation)
        });
        match result {
            Ok(index) => Ok(rtdb_index::ServingSearch::new(index)),
            Err(_) if !cancellation.is_cancelled() => {
                self.record_serving_gap();
                Ok(rtdb_index::ServingSearch::new(ServingPostings::preserve(snapshot)))
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn can_scan_live_gap(&self, segment: &rtdb_index::LiveTable, column: &str, covered: u32) -> Result<bool> {
        if self.write_format < crate::file::PACKED_FORMAT_VERSION { return Ok(true); }
        if segment.row_count().saturating_sub(covered) > 2048 { return Ok(false); }
        let mut bytes = 0usize;
        for row in covered..segment.row_count() {
            let source = segment.row(row)?;
            rtdb_storage::audience_work::scan_items(1)?;
            if let PutText::Utf8(text) = source.batch.view().borrow_event(source.row)?.put_text(column)? {
                bytes = bytes.saturating_add(text.len());
                if bytes > 1024 * 1024 { return Ok(false); }
            }
        }
        Ok(true)
    }

    pub(crate) async fn tree_file(&self, storage: &Arc<BufferPool>, key: &ObjectKey,
        locator: &crate::file::TreeLocator, columns: &[String], identity: &str, version: i64, fresh: bool)
        -> std::result::Result<Arc<crate::file::TreeFile>, FileError> {
        let share = !fresh && storage.prepaid_metadata_budget().is_none();
        let cache_key = (Arc::as_ptr(storage) as usize, key.clone(), locator.digest());
        if fresh {
            self.search_pages.invalidate(storage, key);
            self.trees.lock().remove(&cache_key);
        } else if share && let Some((_, tree)) = self.trees.lock().get(&cache_key) {
            return Ok(tree);
        }
        let cache = share.then(|| (Arc::clone(storage), key.clone(), Arc::clone(&self.search_pages)));
        let store = storage.object_store();
        let remote = !fresh && store.supports_scan_ranges(key);
        let fresh_locator = fresh.then(|| locator.without_inline_root());
        let locator = fresh_locator.as_ref().unwrap_or(locator);
        let result: rtdb_types::Result<_> = async {
            let local = if remote { store.cached_local_file(key).await? } else { store.local_file(key).await? };
            if let Some(local) = local {
                let bytes = bytes::Bytes::from_owner(crate::Source::mapped(local)?);
                let weight = bytes.len().saturating_add(65_536);
                let file = crate::file::TreeFile::local(bytes.into(), cache, locator, columns, identity, version).await?;
                Ok((file, Some(weight)))
            } else if remote {
                let pages = if share { Arc::clone(&self.search_pages) } else { Arc::new(crate::file::PageCache::new(0)) };
                let file = crate::file::TreeFile::remote(Arc::clone(storage), key.clone(), pages, locator, columns, identity, version).await?;
                Ok((file, Some(65_536)))
            } else {
                let bytes = bytes::Bytes::from_owner(storage.get(key).await?);
                let file = crate::file::TreeFile::local(bytes.into(), None, locator, columns, identity, version).await?;
                Ok((file, None))
            }
        }.await;
        let (tree, weight) = result.map_err(FileError::from_anomaly)?;
        let tree = Arc::new(tree);
        if share && let Some(weight) = weight {
            let mut cache = self.trees.lock();
            if weight <= cache.limit { cache.insert(cache_key, (Arc::downgrade(storage), Arc::clone(&tree)), weight); }
        }
        Ok(tree)
    }

    pub(crate) async fn logical_file(&self, storage: &Arc<BufferPool>, slug: &rtdb_format::keys::TableSlug,
        run: &rtdb_format::log::proto::SearchRun, columns: &[String]) -> Result<Arc<crate::file::TreeFile>> {
        self.open_logical(storage, slug, run, columns, false).await
    }

    pub(crate) async fn open_logical(&self, storage: &Arc<BufferPool>, slug: &rtdb_format::keys::TableSlug,
        run: &rtdb_format::log::proto::SearchRun, columns: &[String], fresh: bool) -> Result<Arc<crate::file::TreeFile>> {
        let key = ObjectKey::new(run.object_key(slug)?)?;
        let locator = std::str::from_utf8(run.locator()).ok().and_then(crate::file::TreeLocator::decode)
            .filter(|locator| locator.format() == crate::file::LOGICAL_FORMAT_VERSION && locator.length == run.file_size() as u64)
            .ok_or_else(|| rtdb_types::Anomaly::fault("rtdb.search/corrupt-file", "Search run has no matching root locator"))?;
        let identity = crate::lane::run_identity(slug, run.definition_id(), run.definition_version(), &run.blocks, run.rebuild(), run.allocation());
        if fresh { storage.invalidate(&key).await?; }
        self.tree_file(storage, &key, &locator, columns, &identity, run.definition_version(), fresh).await.map_err(|error| match error {
            FileError::Missing => rtdb_types::Anomaly::fault("rtdb.storage/missing-object", key.as_str()),
            FileError::Corrupt(error) | FileError::Unavailable(error) => error,
        })
    }

    pub(crate) async fn materialized_tree(
        &self, database: &str, storage: &Arc<BufferPool>, key: &ObjectKey, columns: &[String], identity: &str,
        version: i64, locator: &crate::file::TreeLocator,
    ) -> std::result::Result<Arc<SearchFile>, FileError> {
        let weight = usize::try_from(locator.length).unwrap_or(usize::MAX).saturating_mul(3);
        if let Some(memory) = storage.prepaid_metadata_budget() {
            let reservation = memory.reserve(weight).map_err(FileError::from_anomaly)?;
            let tree = self.tree_file(storage, key, locator, columns, identity, version, false).await?;
            return tree.materialize(columns, reservation).await.map(Arc::new).map_err(FileError::from_anomaly);
        }
        let cache_key = (database.to_owned(), key.as_str().to_owned());
        if let Some((pool, file)) = self.files.lock().get(&cache_key)
            && pool.upgrade().is_some_and(|pool| Arc::ptr_eq(&pool, storage)) {
            return Ok(file);
        }
        let conversion_key = (Arc::as_ptr(storage) as usize, key.clone(), locator.digest());
        let cell = Arc::clone(self.tree_conversions.lock().entry(conversion_key.clone()).or_default());
        let converted = cell.get_or_try_init(|| async {
            let tree = self.tree_file(storage, key, locator, columns, identity, version, false).await?;
            let reservation = match self.file_admission.reserve(weight) {
                Err(_) if self.files.lock().weight > 0 => {
                    self.release_file_cache();
                    self.file_admission.reserve(weight)
                }
                value => value,
            }.map_err(FileError::from_anomaly)?;
            let file = Arc::new(tree.materialize(columns, reservation).await.map_err(FileError::from_anomaly)?);
            self.files.lock().insert(cache_key.clone(), (Arc::downgrade(storage), Arc::clone(&file)), weight);
            Ok(file)
        }).await.map(Arc::clone);
        let mut building = self.tree_conversions.lock();
        if building.get(&conversion_key).is_some_and(|current| Arc::ptr_eq(current, &cell)) { building.remove(&conversion_key); }
        converted
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
        self.file_with_locator(database, storage, key, columns, None).await
    }

    pub(crate) async fn file_with_locator(&self, database: &str, storage: &Arc<BufferPool>, key: &rtdb_storage::ObjectKey,
        columns: &[String], locator: Option<&crate::file::FooterLocator>) -> std::result::Result<Arc<SearchFile>, FileError> {
        self.open_file(database, storage, key, columns, locator, false).await
    }

    pub(crate) async fn file_for_repair(&self, database: &str, storage: &Arc<BufferPool>, key: &rtdb_storage::ObjectKey,
        columns: &[String], locator: Option<&crate::file::FooterLocator>) -> std::result::Result<Arc<SearchFile>, FileError> {
        self.open_file(database, storage, key, columns, locator, true).await
    }

    async fn open_file(&self, database: &str, storage: &Arc<BufferPool>, key: &rtdb_storage::ObjectKey,
        columns: &[String], locator: Option<&crate::file::FooterLocator>, repairing: bool) -> std::result::Result<Arc<SearchFile>, FileError> {
        let cache_key = (database.to_string(), key.as_str().to_string());
        let share = !repairing && storage.prepaid_metadata_budget().is_none();
        let scratch = storage.prepaid_metadata_budget().unwrap_or(&self.file_admission);
        if share {
            for kept in [&self.mapped, &self.files] {
                if let Some((pool, file)) = kept.lock().get(&cache_key) {
                    if pool.upgrade().is_some_and(|pool| Arc::ptr_eq(&pool, storage)) { return Ok(file); }
                }
            }
        }
        let unavailable = |error: rtdb_storage::Error| match error { rtdb_storage::Error::MissingObject(_) => FileError::Missing, error => FileError::Unavailable(error.into()) };
        let ranged = locator.filter(|_| !repairing && storage.object_store().supports_scan_ranges(key));
        let local = if ranged.is_some() { storage.object_store().cached_local_file(key).await }
            else { storage.object_store().local_file(key).await };
        if let Some(local) = local.map_err(unavailable)? {
            let size = usize::try_from(local.file().metadata().map_err(|error| FileError::Unavailable(rtdb_types::Anomaly::fault("rtdb.search/file", error.to_string())))?.len()).unwrap_or(usize::MAX);
            let source = crate::Source::mapped(local).map_err(FileError::Corrupt)?;
            let (file, mapped_bytes) = SearchFile::open_catalog(source, columns, locator, scratch).await.map_err(FileError::from_anomaly)?;
            let file = Arc::new(file);
            if share { self.mapped.lock().insert(cache_key, (Arc::downgrade(storage), Arc::clone(&file)), mapped_bytes.unwrap_or(size)); }
            return Ok(file);
        }
        if let Some(locator) = ranged {
            let opened = match SearchFile::open_remote(storage, key, locator, columns).await {
                Err(error) if error.code == "rtdb.storage/memory-limit" && self.files.lock().weight > 0 => {
                    self.release_file_cache();
                    SearchFile::open_remote(storage, key, locator, columns).await
                }
                result => result,
            };
            let file = Arc::new(opened.map_err(FileError::from_anomaly)?);
            if share {
                self.files.lock().insert(cache_key, (Arc::downgrade(storage), Arc::clone(&file)), file.metadata_bytes());
            }
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
        let (file, mapped_bytes) = SearchFile::open_catalog(lease.into(), columns, locator, scratch).await.map_err(FileError::from_anomaly)?;
        let file = Arc::new(file);
        if share {
            let (cache, weight) = mapped_bytes.map_or((&self.files, size), |bytes| (&self.mapped, bytes));
            cache.lock().insert(cache_key, (Arc::downgrade(storage), Arc::clone(&file)), weight);
        }
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
        let share = storage.prepaid_metadata_budget().is_none();
        let cache_key = (database.to_string(), identity.clone());
        let build_key = (Arc::as_ptr(storage) as usize, database.to_string(), identity.clone());
        if share && let Some((pool, file)) = self.built.lock().get(&cache_key) {
            if pool.upgrade().is_some_and(|pool| Arc::ptr_eq(&pool, storage)) { return Ok(TrieFile { trie: trie.clone(), data: data.clone(), identity, file, member: 0 }); }
        }
        // A build that fails or whose query is cancelled leaves the cell empty, and the next caller builds.
        let cell = if share { Arc::clone(self.building.lock().entry(build_key.clone()).or_default()) }
            else { Arc::new(tokio::sync::OnceCell::new()) };
        let outcome = cell.get_or_try_init(|| async {
            let lease = storage.get(data).await?;
            let ceiling = crate::file::ceiling_of(trie.recency());
            let indexing = memory.reserve(crate::lane::indexing_allowance(i64::try_from(lease.as_slice().len()).unwrap_or(i64::MAX)))?;
            let (indexed, token) = (columns.to_vec(), cancellation.clone());
            let input_memory = Arc::clone(memory);
            let bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
                let mut bytes = Vec::new();
                let pages = crate::lane::search_pages_from_reader(std::io::Cursor::new(lease), token, input_memory)?;
                crate::file::write_query_pages(&indexed, pages, ceiling, &mut bytes, indexing)?;
                Ok(bytes)
            }).await.map_err(|error| rtdb_types::Anomaly::fault("rtdb.search/build", error.to_string()))??;
            let size = bytes.len();
            let mut file = SearchFile::open(bytes, columns)?;
            if !share { file = file.with_reservation(memory.reserve(size)?); }
            let file = Arc::new(file);
            if share { self.built.lock().insert(cache_key.clone(), (Arc::downgrade(storage), Arc::clone(&file)), size); }
            Ok::<_, rtdb_types::Anomaly>(file)
        }).await.cloned();
        if share {
            let mut building = self.building.lock();
            if building.get(&build_key).is_some_and(|current| Arc::ptr_eq(current, &cell)) { building.remove(&build_key); }
        }
        Ok(TrieFile { trie: trie.clone(), data: data.clone(), identity, file: outcome?, member: 0 })
    }

    pub fn forget_file(&self, database: &str, key: &rtdb_storage::ObjectKey) {
        let cache_key = (database.to_string(), key.as_str().to_string());
        for kept in [&self.mapped, &self.files] { kept.lock().remove(&cache_key); }
    }

    pub(crate) fn release_file_cache(&self) { self.files.lock().clear(); }

    /// The rows of a live segment whose column holds every term of the query; phrases are left to the caller.
    /// Postings are kept per segment and extended by the rows appended since, so a growing live index is tokenised once.
    pub fn live_rows(&self, database: &str, segment: &rtdb_index::LiveTable, column: &str, query: &rtdb_text::Query) -> Result<UInt32Array> {
        let mut matching = UInt32Builder::new();
        self.visit_live_matches(database, segment, column, query, false, &CancellationToken::new(), |row, _| {
            matching.append_value(row);
            Ok(true)
        })?;
        Ok(matching.finish())
    }

    fn live_key(database: &str, segment: &rtdb_index::LiveTable, column: &str) -> LiveKey {
        (database.to_string(), segment.table().clone(), column.to_string(), segment.row_lineage().generation_address())
    }

    pub(crate) fn publish_live(&self, database: &str, segment: &rtdb_index::LiveTable, column: &str, cancellation: &CancellationToken) -> Result<()> {
        let key = Self::live_key(database, segment, column);
        let lineage = segment.row_lineage();
        let cached = self.live.lock().get(&key);
        let mut next = cached.as_deref().filter(|cached| cached.lineage.as_ref().is_some_and(|prefix| prefix.is_prefix_of(lineage)))
            .cloned().unwrap_or_default();
        if next.rows >= segment.row_count() { return Ok(()); }
        next.extend(segment, column, cancellation)?;
        if cancellation.is_cancelled() || next.rows == 0 { return Ok(()); }
        let weight = next.bytes.max(1);
        let mut live = self.live.lock();
        if live.get(&key).is_none_or(|current| {
            current.rows < next.rows || current.lineage.as_ref().is_none_or(|cached| !cached.descends_from(lineage))
        }) {
            live.insert(key, Arc::new(next), weight);
        }
        Ok(())
    }

    pub(crate) fn live_pages(&self, database: &str, segment: &rtdb_index::LiveTable, column: &str)
        -> (Vec<Arc<LivePostingPage>>, u32) {
        let lineage = segment.row_lineage();
        let postings = self.live.lock().get(&Self::live_key(database, segment, column)).filter(|entry| {
            entry.lineage.as_ref().is_some_and(|cached| cached.admits(lineage))
        });
        let visible = segment.row_count();
        let pages: Vec<_> = postings.iter().flat_map(|entry| &entry.pages)
            .take_while(|page| page.first_row + page.tokens.len() as u32 <= visible).cloned().collect();
        let covered = pages.last().map_or(0, |page| page.first_row + page.tokens.len() as u32);
        (pages, covered)
    }

    pub(crate) fn visit_live_matches(&self, database: &str, segment: &rtdb_index::LiveTable, column: &str,
        query: &rtdb_text::Query, phrases: bool, cancellation: &CancellationToken, visit: impl FnMut(u32, &dyn Fn() -> StringArray) -> Result<bool>,
    ) -> Result<bool> {
        self.visit_live_matches_at(None, database, segment, column, query, phrases, cancellation, visit)
    }

    pub(crate) fn live_pages_at(&self, snapshot: Option<&Snapshot>, database: &str, segment: &rtdb_index::LiveTable, column: &str)
        -> (Vec<Arc<LivePostingPage>>, u32) {
        snapshot.and_then(Snapshot::serving_search::<ServingPostings>)
            .and_then(|search| search.pages(segment, column)).unwrap_or_else(|| self.live_pages(database, segment, column))
    }

    pub(crate) fn visit_live_matches_at(&self, snapshot: Option<&Snapshot>, database: &str, segment: &rtdb_index::LiveTable, column: &str,
        query: &rtdb_text::Query, phrases: bool, cancellation: &CancellationToken, mut visit: impl FnMut(u32, &dyn Fn() -> StringArray) -> Result<bool>,
    ) -> Result<bool> {
        let (pages, covered) = self.live_pages_at(snapshot, database, segment, column);
        if snapshot.is_some() && !self.can_scan_live_gap(segment, column, covered)? { return Ok(false); }
        for page in pages {
            if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
            if !page.visit(query, phrases, &mut visit)? { return Ok(false); }
        }
        Self::visit_live_suffix(segment, column, query, phrases, covered, cancellation, visit)
    }

    pub(crate) fn visit_live_suffix(segment: &rtdb_index::LiveTable, column: &str, query: &rtdb_text::Query,
        phrases: bool, covered: u32, cancellation: &CancellationToken, mut visit: impl FnMut(u32, &dyn Fn() -> StringArray) -> Result<bool>,
    ) -> Result<bool> {
        if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
        if query.terms().is_empty() { return Ok(true); }
        for row in covered..segment.row_count() {
            if row % 128 == 0 && cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
            rtdb_storage::audience_work::scan_items(1)?;
            let source = segment.row(row)?;
            let event = source.batch.view().borrow_event(source.row)?;
            let PutText::Utf8(text) = event.put_text(column)? else { continue; };
            rtdb_storage::audience_work::decode_bytes(text.len().saturating_mul(32))?;
            let tokens = rtdb_text::analyze(text);
            if !query.terms().iter().all(|term| tokens.iter().any(|token| token == Some(term.as_str()))) { continue; }
            if phrases && !query.matches_arrow(&tokens) { continue; }
            if !visit(row, &|| tokens.clone())? { return Ok(false); }
        }
        Ok(true)
    }

    /// Hands a recorded file a scan could not read to the database's lane.
    pub fn report(&self, suspect: Suspect) {
        let database = suspect.database.clone();
        if self.suspects.lock().insert(suspect) { self.notifier(&database).notify_one(); }
    }
    pub(crate) fn report_run(&self, database: &str, table: &rtdb_types::TableRef,
        run: &rtdb_format::log::proto::SearchRun, error: &rtdb_types::Anomaly) {
        if !FileError::from_anomaly(error.clone()).needs_repair() { return; }
        if let Ok(id) = run.identity() {
            let mut suspects = self.suspect_runs.lock();
            if suspects.len() < 1024 && suspects.insert((database.to_owned(), table.clone(), id)) {
                self.notifier(database).notify_one();
            }
        }
    }

    pub(crate) fn suspect_runs(&self, database: &str) -> Vec<(rtdb_types::TableRef, [u8; 32])> {
        self.suspect_runs.lock().iter().filter(|(name, _, _)| name == database).map(|(_, table, id)| (table.clone(), *id)).collect()
    }

    pub(crate) fn clear_run(&self, database: &str, table: &rtdb_types::TableRef, id: [u8; 32]) {
        self.suspect_runs.lock().remove(&(database.to_owned(), table.clone(), id));
    }

    pub(crate) fn report_term_directory(&self, database: &str, table: &rtdb_types::TableRef, id: &str, version: i64) {
        self.record_serving_gap();
        let mut suspects = self.suspect_term_directories.lock();
        if suspects.len() < 1024 && suspects.insert((database.to_owned(), table.clone(), id.to_owned(), version)) {
            self.notifier(database).notify_one();
        }
    }

    pub(crate) fn term_directory_suspect(&self, database: &str, table: &rtdb_types::TableRef, id: &str, version: i64) -> bool {
        self.suspect_term_directories.lock().contains(&(database.to_owned(), table.clone(), id.to_owned(), version))
    }

    pub(crate) fn clear_term_directory(&self, database: &str, table: &rtdb_types::TableRef, id: &str, version: i64) {
        self.suspect_term_directories.lock().remove(&(database.to_owned(), table.clone(), id.to_owned(), version));
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
        let rows = vec![crate::SearchRow { position: None, identity: None, iid: [1; 16], system_from: 0, valid_from: 0, valid_to: i64::MAX, op: rtdb_types::EventOp::Put, values: vec![Some(rtdb_text::tokens("an overdue invoice"))] }];
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
            let event = rtdb_format::data::StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid: rtdb_types::Iid([1; 16]), system_from: rtdb_types::Timestamp(1), valid_time: rtdb_types::Interval::new(rtdb_types::Timestamp(0), rtdb_types::Timestamp::MAX).unwrap(),
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
        let rows: Vec<_> = (0..3000).map(|index| crate::SearchRow { position: None, identity: None, iid: [(index / 12) as u8; 16], system_from: -(index as i64), valid_from: 0, valid_to: i64::MAX, op: rtdb_types::EventOp::Put,
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

#[cfg(test)]
mod live_coverage_tests {
    use super::*;
    use rtdb_format::data::{StoredEvent, StoredOperation};
    use rtdb_index::{IndexBatch, LiveTable, TableWrite};
    use rtdb_types::{BlockIndex, EntityId, Interval, StructFields, TableRef, Timestamp, Value};

    #[test]
    fn live_coverages_reuse_the_prefix_across_merged_chunks_and_reject_a_divergent_suffix() {
        let memory = MemoryBudget::new(8 << 20);
        let runtime = SearchRuntime::new(4 << 20);
        let token = CancellationToken::new();
        let root = LiveTable::new(TableRef::new("public", "docs"), BlockIndex(0), Default::default());
        let append = |table: &LiveTable, id: i64, body: &str| {
            let identity = EntityId::Integer(id);
            let event = StoredEvent { position: None, origin: None, entity_key: None, iid: identity.iid(), system_from: Timestamp(id),
                commit_position: 0, valid_time: Interval::ALL, recency: None,
                operation: StoredOperation::Put(StructFields::from([
                    ("_id".into(), Value::I64(id)), ("body".into(), Value::Utf8(body.into())),
                ])) };
            let batch = IndexBatch::from_events(&[event], &StructFields::new(), &memory).unwrap();
            table.append(&TableWrite::new(StructFields::new(), Some(batch)), Timestamp(id)).unwrap()
        };
        let prefix = append(&root, 1, "report paid");
        let extended = append(&prefix, 2, "report open");
        let divergent = append(&prefix, 2, "novel");
        assert_eq!(extended.batches().count(), 1);
        assert!(!Arc::ptr_eq(prefix.batches().next().unwrap(), extended.batches().next().unwrap()));

        runtime.publish_live("db", &prefix, "body", &token).unwrap();
        let (initial, covered) = runtime.live_pages("db", &extended, "body");
        assert_eq!(covered, 1);
        let matching = rtdb_text::Query::parse("report");
        assert_eq!(runtime.live_rows("db", &extended, "body", &matching).unwrap().values(), &[0, 1]);

        runtime.publish_live("db", &extended, "body", &token).unwrap();
        let (complete, covered) = runtime.live_pages("db", &extended, "body");
        assert_eq!(covered, 2);
        assert!(Arc::ptr_eq(&complete[0], &initial[0]));
        assert_eq!(runtime.live_pages("db", &prefix, "body").1, 1);
        assert_eq!(runtime.live_pages("db", &divergent, "body").1, 0);
        assert_eq!(runtime.live_rows("db", &divergent, "body", &matching).unwrap().values(), &[0]);
        token.cancel();
        let query = rtdb_text::Query::parse("absent");
        let interrupted = runtime.visit_live_matches("db", &divergent, "body", &query, true, &token,
            |_, _| panic!("a nonmatching row cannot be visited")).unwrap_err();
        assert_eq!(interrupted.code, "rtdb.search/cancelled");

        drop((root, prefix, extended, divergent));
        assert_eq!(memory.used_bytes(), 0);
    }
}
