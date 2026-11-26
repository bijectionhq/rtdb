use std::collections::BTreeMap;
use std::io::Write;
use std::sync::{Arc, Weak};

use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch, StructArray, UInt8Array, UInt32Array, UInt64Array,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder}, cast::AsArray, types::{UInt8Type, UInt32Type, UInt64Type}};
use arrow_buffer::Buffer;
use arrow_ipc::{MetadataVersion, reader::FileDecoder};
use parking_lot::Mutex;
use futures::{StreamExt, TryStreamExt};
use rtdb_storage::{ObjectKey, audience_work, cache::BufferPool};
use rtdb_types::{MemoryBudget, MemoryReservation, Result};
use rtdb_types::AnomalyKind;
use tokio::sync::OnceCell;

use super::{corrupt, fault, layout::{AuthenticatedPage, PageRef, Table, Writer, schema, table}};

const FANOUT: usize = 128;
const MAX_HEIGHT: u8 = 8;

pub(super) const DIRECTORY_WALK_BYTES: usize =
    (1 + MAX_HEIGHT as usize * FANOUT) * size_of::<(PageRef, u8)>() + FANOUT * size_of::<DecodedChild>();

#[derive(Clone, Copy)]
pub(super) enum PageKind { Root, Directory, Terms, Rank, Postings, Documents, Visibility, PrimaryLayout }

#[derive(Clone, Copy, Default)]
pub(crate) struct PageReads {
    pub requests: usize,
    pub bytes: u64,
}

#[derive(Default)]
pub(crate) struct IoStats {
    pub requests: usize,
    pub bytes: u64,
    pub cache_hits: usize,
    reads: [PageReads; 8],
}

impl IoStats {
    #[cfg(feature = "experiments")]
    pub(crate) fn by_kind(&self) -> impl Iterator<Item = (&'static str, PageReads)> + '_ {
        ["root", "directory", "terms", "rank", "postings", "documents", "visibility", "primary_layout"]
            .into_iter().zip(self.reads.iter().copied())
    }
}

tokio::task_local! {
    static SEARCH_IO: std::cell::RefCell<IoStats>;
}

pub(crate) async fn observe_io<T>(read: impl std::future::Future<Output = T>) -> (T, IoStats) {
    if !cfg!(feature = "experiments") { return (read.await, IoStats::default()); }
    SEARCH_IO.scope(Default::default(), async {
        let result = read.await;
        (result, SEARCH_IO.with(|stats| std::mem::take(&mut *stats.borrow_mut())))
    }).await
}

fn observe_request(kind: PageKind, bytes: u64) {
    if cfg!(feature = "experiments") {
        let _ = SEARCH_IO.try_with(|stats| {
            let mut stats = stats.borrow_mut();
            stats.requests += 1;
            stats.bytes = stats.bytes.saturating_add(bytes);
            let read = &mut stats.reads[kind as usize];
            read.requests += 1;
            read.bytes = read.bytes.saturating_add(bytes);
        });
    }
}

fn observe_hit() {
    if cfg!(feature = "experiments") {
        let _ = SEARCH_IO.try_with(|stats| stats.borrow_mut().cache_hits += 1);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct DirectoryRef {
    pub page: PageRef,
    pub height: u8,
}

#[derive(Clone)]
pub(super) struct Child {
    pub lower: Vec<u8>,
    pub upper: Vec<u8>,
    pub page: PageRef,
}

pub(super) struct DecodedChild {
    pub lower: Buffer,
    pub upper: Buffer,
    pub page: PageRef,
}

impl DecodedChild {
    fn new(lower: &BinaryArray, upper: &BinaryArray, row: usize, page: PageRef) -> Self {
        fn fence(column: &BinaryArray, row: usize) -> Buffer {
            let offsets = column.value_offsets();
            let start = offsets[row] as usize;
            column.values().slice_with_length(start, offsets[row + 1] as usize - start)
        }

        Self { lower: fence(lower, row), upper: fence(upper, row), page }
    }
}

pub(super) fn inline_routes(routes: &[(u8, Child)]) -> Result<StructArray> {
    let mut lower = BinaryBuilder::new();
    let mut upper = BinaryBuilder::new();
    for (_, child) in routes {
        lower.append_value(&child.lower);
        upper.append_value(&child.upper);
    }
    let mut columns: Vec<ArrayRef> = vec![Arc::new(UInt8Array::from_iter_values(routes.iter().map(|(kind, _)| *kind))),
        Arc::new(lower.finish()), Arc::new(upper.finish())];
    columns.extend(locator_columns(routes.iter().map(|(_, child)| child.page))?);
    Table::Routes.array(columns)
}

pub(super) fn read_inline_routes(root: &RecordBatch, directories: &BTreeMap<u8, DirectoryRef>) -> Result<BTreeMap<u8, Vec<DecodedChild>>> {
    let routes = table(root, Table::Routes);
    if routes.len() > 48 || routes.columns().iter().any(|column| column.null_count() != 0) {
        return Err(corrupt("Search root has an invalid inline directory"));
    }
    let (kinds, lower, upper) = (routes.column(0).as_primitive::<UInt8Type>(), routes.column(1).as_binary::<i32>(), routes.column(2).as_binary::<i32>());
    let mut by_kind = BTreeMap::<u8, Vec<DecodedChild>>::new();
    for row in 0..routes.len() {
        let kind = kinds.value(row);
        let page = locator_at(&routes, 3, row)?;
        let children = by_kind.entry(kind).or_default();
        if !directories.get(&kind).is_some_and(|directory| directory.height == 1 && page.range().is_ok_and(|range| range.end <= directory.page.offset))
            || lower.value(row) > upper.value(row) || children.last().is_some_and(|child| child.upper.as_slice() >= lower.value(row)) {
            return Err(corrupt("Search root has unordered inline directory children"));
        }
        children.push(DecodedChild::new(lower, upper, row, page));
    }
    Ok(by_kind)
}

fn locator_columns(locators: impl IntoIterator<Item = PageRef>) -> Result<Vec<ArrayRef>> {
    let locators: Vec<_> = locators.into_iter().collect();
    let mut digests = FixedSizeBinaryBuilder::with_capacity(locators.len(), 32);
    for locator in &locators { digests.append_value(locator.digest).map_err(fault)?; }
    Ok(vec![
        Arc::new(UInt64Array::from_iter_values(locators.iter().map(|page| page.offset))),
        Arc::new(UInt32Array::from_iter_values(locators.iter().map(|page| page.metadata))),
        Arc::new(UInt64Array::from_iter_values(locators.iter().map(|page| page.body))),
        Arc::new(digests.finish()),
    ])
}

fn locator_at(array: &StructArray, column: usize, row: usize) -> Result<PageRef> {
    let digest = array.column(column + 3).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .ok_or_else(|| corrupt("Search directory has invalid digests"))?;
    let page = PageRef {
        offset: array.column(column).as_primitive::<UInt64Type>().value(row),
        metadata: array.column(column + 1).as_primitive::<UInt32Type>().value(row),
        body: array.column(column + 2).as_primitive::<UInt64Type>().value(row),
        digest: digest.value(row).try_into().map_err(fault)?,
    };
    page.range()?;
    Ok(page)
}

pub(super) fn write_directory(writer: &mut Writer<impl Write>, mut level: Vec<Child>) -> Result<DirectoryRef> {
    let mut height = 1;
    loop {
        let mut parents = Vec::new();
        for children in level.chunks(FANOUT) {
            if children.len() > 1 && children.windows(2).any(|pair| pair[0].upper >= pair[1].lower) {
                return Err(fault("Search directory children overlap"));
            }
            let mut lower = BinaryBuilder::new();
            let mut upper = BinaryBuilder::new();
            for child in children {
                if child.lower > child.upper { return Err(fault("Search directory child has an inverted range")); }
                lower.append_value(&child.lower);
                upper.append_value(&child.upper);
            }
            let mut columns: Vec<ArrayRef> = vec![Arc::new(lower.finish()), Arc::new(upper.finish())];
            columns.extend(locator_columns(children.iter().map(|child| child.page))?);
            let page = writer.page([(Table::Directory, Table::Directory.array(columns)?)])?;
            parents.push(Child { lower: children[0].lower.clone(), upper: children[children.len() - 1].upper.clone(), page });
        }
        match parents.len() {
            0 => {
                let mut columns: Vec<ArrayRef> = vec![Arc::new(BinaryBuilder::new().finish()), Arc::new(BinaryBuilder::new().finish())];
                columns.extend(locator_columns([])?);
                let page = writer.page([(Table::Directory, Table::Directory.array(columns)?)])?;
                return Ok(DirectoryRef { page, height });
            }
            1 => return Ok(DirectoryRef { page: parents[0].page, height }),
            _ if height < MAX_HEIGHT => {
                height += 1;
                level = parents;
            }
            _ => return Err(fault("Search directory exceeds its maximum depth")),
        }
    }
}

pub(super) fn root_table(directories: &[(u8, DirectoryRef)]) -> Result<StructArray> {
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt8Array::from_iter_values(directories.iter().map(|(kind, _)| *kind))),
        Arc::new(UInt8Array::from_iter_values(directories.iter().map(|(_, dir)| dir.height))),
    ];
    columns.extend(locator_columns(directories.iter().map(|(_, dir)| dir.page))?);
    Table::Root.array(columns)
}

pub(super) fn read_root(root: &RecordBatch) -> Result<BTreeMap<u8, DirectoryRef>> {
    let root = table(root, Table::Root);
    if root.num_columns() != 6 || root.len() > 8 || root.columns().iter().any(|column| column.null_count() != 0) {
        return Err(corrupt("Search root has invalid directory declarations"));
    }
    let kinds = root.column(0).as_primitive::<UInt8Type>();
    let heights = root.column(1).as_primitive::<UInt8Type>();
    let mut directories = BTreeMap::new();
    for row in 0..root.len() {
        let height = heights.value(row);
        if !(1..=MAX_HEIGHT).contains(&height) || directories.insert(kinds.value(row), DirectoryRef {
            page: locator_at(&root, 2, row)?, height,
        }).is_some() {
            return Err(corrupt("Search root declares an invalid directory"));
        }
    }
    Ok(directories)
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct CacheKey { pool: usize, object: ObjectKey, page: PageRef }

struct Entry { touched: u64, bytes: usize, fill: Arc<OnceCell<RecordBatch>>, _pool: Weak<BufferPool> }

#[derive(Default)]
struct CacheState { entries: BTreeMap<CacheKey, Entry>, bytes: usize, clock: u64 }

pub(crate) struct PageCache { limit: usize, state: Mutex<CacheState> }

impl PageCache {
    pub fn new(limit: usize) -> Self { Self { limit, state: Mutex::new(CacheState::default()) } }

    fn reserve(&self, memory: &Arc<MemoryBudget>, bytes: usize) -> Result<Arc<MemoryReservation>> {
        loop {
            let error = match memory.reserve(bytes) {
                Ok(charge) => return Ok(charge),
                Err(error) if error.is(AnomalyKind::ResourceExhausted) && bytes <= memory.limit_bytes() => error,
                Err(error) => return Err(error),
            };
            let mut state = self.state.lock();
            let Some(key) = state.entries.iter().filter(|(_, entry)| entry.fill.initialized())
                .min_by_key(|(_, entry)| entry.touched).map(|(key, _)| CacheKey {
                    pool: key.pool, object: key.object.clone(), page: key.page,
                }) else { return Err(error); };
            state.bytes -= state.entries.remove(&key).expect("cache entry held under its lock").bytes;
        }
    }

    pub fn invalidate(&self, storage: &Arc<BufferPool>, object: &ObjectKey) {
        let mut state = self.state.lock();
        state.entries.retain(|key, _| key.pool != Arc::as_ptr(storage) as usize || &key.object != object);
        state.bytes = state.entries.values().map(|entry| entry.bytes).sum();
    }

    fn entry(&self, pool: &Arc<BufferPool>, object: &ObjectKey, page: PageRef, bytes: usize) -> Arc<OnceCell<RecordBatch>> {
        let key = CacheKey { pool: Arc::as_ptr(pool) as usize, object: object.clone(), page };
        let mut state = self.state.lock();
        state.clock += 1;
        let touched = state.clock;
        if let Some(entry) = state.entries.get_mut(&key) {
            entry.touched = touched;
            return Arc::clone(&entry.fill);
        }
        let fill = Arc::new(OnceCell::new());
        if bytes > self.limit { return fill; }
        while state.bytes > self.limit - bytes {
            let Some(key) = state.entries.iter()
                .filter(|(_, entry)| entry.fill.initialized() || Arc::strong_count(&entry.fill) == 1)
                .min_by_key(|(_, entry)| entry.touched).map(|(key, _)| CacheKey {
                    pool: key.pool, object: key.object.clone(), page: key.page,
                }) else { return fill; };
            state.bytes -= state.entries.remove(&key).expect("key is held under the cache lock").bytes;
        }
        state.bytes += bytes;
        state.entries.insert(key, Entry { touched, bytes, fill: Arc::clone(&fill), _pool: Arc::downgrade(pool) });
        fill
    }
}

pub(super) struct PageOwner {
    storage: Weak<BufferPool>,
    object: ObjectKey,
    cache: Arc<PageCache>,
}

impl PageOwner {
    pub fn new(storage: &Arc<BufferPool>, object: ObjectKey, cache: Arc<PageCache>) -> Self {
        Self { storage: Arc::downgrade(storage), object, cache }
    }

    fn storage(&self) -> Result<Arc<BufferPool>> {
        self.storage.upgrade().ok_or_else(|| rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "rtdb.search/storage-released", "Search storage has been released"))
    }
}

pub(super) enum PageSource {
    Local { bytes: Buffer, owner: Option<PageOwner> },
    Remote(PageOwner),
}

pub(super) struct TreeReader {
    source: PageSource,
    length: u64,
    decoder: FileDecoder,
    packed: bool,
    memory: Arc<MemoryBudget>,
}

impl TreeReader {
    pub fn new(source: PageSource, length: u64, format: i32, memory: Arc<MemoryBudget>) -> Self {
        Self { source, length, decoder: FileDecoder::new(Arc::clone(schema(format)), MetadataVersion::V5),
            packed: format >= super::PACKED_FORMAT_VERSION, memory }
    }

    pub fn inline_page(&self, page: PageRef, bytes: bytes::Bytes) -> Result<RecordBatch> {
        self.decode(page.authenticate(Buffer::from(bytes))?, None, false, 0)
    }

    pub fn authenticated_page(&self, page: AuthenticatedPage, expanded: usize) -> Result<RecordBatch> {
        self.decode(page, None, false, expanded)
    }

    fn decode(&self, page: AuthenticatedPage, cache: Option<&PageCache>, detach: bool, wire_credit: usize) -> Result<RecordBatch> {
        page.decode_admitted(&self.decoder, |bytes| match cache {
            Some(cache) => cache.reserve(&self.memory, bytes),
            None => self.memory.reserve(bytes),
        }, detach, wire_credit)
    }

    pub async fn page(&self, page: PageRef, kind: PageKind) -> Result<RecordBatch> {
        let range = page.range()?;
        if range.end > self.length { return Err(corrupt("Search directory points outside its object")); }
        let len = usize::try_from(range.end - range.start).map_err(fault)?;
        audience_work::decode_bytes(len)?;
        match &self.source {
            PageSource::Local { bytes, owner } => {
                let start = usize::try_from(range.start).map_err(fault)?;
                let input = || page.authenticate(bytes.slice_with_length(start, len));
                if let Some(owner) = owner {
                    let fill = owner.cache.entry(&owner.storage()?, &owner.object, page, len);
                    if let Some(batch) = fill.get() { observe_hit(); return Ok(batch.clone()); }
                    Ok(fill.get_or_try_init(|| async {
                        self.decode(input()?, Some(&owner.cache), len <= owner.cache.limit, len)
                    }).await?.clone())
                } else {
                    self.decode(input()?, None, false, len)
                }
            }
            PageSource::Remote(owner) => {
                let storage = owner.storage()?;
                let fill = owner.cache.entry(&storage, &owner.object, page, len);
                if let Some(batch) = fill.get() { observe_hit(); return Ok(batch.clone()); }
                let batch = fill.get_or_try_init(|| async {
                    observe_request(kind, range.end - range.start);
                    let lease = storage.get_scan_range(&owner.object, range).await?;
                    let page = page.authenticate(Buffer::from(bytes::Bytes::from_owner(lease)))?;
                    self.decode(page, Some(&owner.cache), false, len)
                }).await?;
                Ok(batch.clone())
            }
        }
    }

    pub async fn pages(&self, references: Vec<PageRef>, kind: PageKind) -> Result<BTreeMap<PageRef, RecordBatch>> {
        let PageSource::Remote(owner) = &self.source else {
            let mut result = BTreeMap::new();
            for reference in references { result.insert(reference, self.page(reference, kind).await?); }
            return Ok(result);
        };
        let storage = owner.storage()?;
        let (object, cache) = (&owner.object, &owner.cache);
        let gap = if self.packed { storage.scan_merge_gap() } else { 64 * 1024 };
        let max_range = if self.packed { (storage.scan_byte_limit() as u64 / 16).clamp(64 * 1024, 4 * 1024 * 1024) } else { 1024 * 1024 };
        let mut result = BTreeMap::new();
        let mut needed = BTreeMap::new();
        for reference in references {
            let range = reference.range()?;
            if range.end > self.length { return Err(corrupt("Search directory points outside its object")); }
            let len = usize::try_from(range.end - range.start).map_err(fault)?;
            audience_work::decode_bytes(len)?;
            let fill = cache.entry(&storage, object, reference, len);
            if let Some(batch) = fill.get() { observe_hit(); result.insert(reference, batch.clone()); }
            else { needed.insert(reference, (range, fill)); }
        }
        let bytes = needed.values().map(|(range, _)| range.end - range.start).sum::<u64>();
        let whole = !needed.is_empty() && bytes >= self.length - self.length / 4
            && (!self.packed || self.length <= storage.scan_byte_limit() as u64 / 4);
        let mut groups: Vec<(std::ops::Range<u64>, Vec<_>)> = Vec::new();
        for (reference, (range, fill)) in needed {
            let entry = (reference, range.clone(), fill);
            if let Some((previous, pages)) = groups.last_mut()
                && (whole || range.start.saturating_sub(previous.end) <= gap && range.end - previous.start <= max_range) {
                previous.end = previous.end.max(range.end);
                pages.push(entry);
            } else { groups.push((range, vec![entry])); }
        }
        if whole { groups[0].0 = 0..self.length; }
        let storage = &storage;
        let fetched = futures::stream::iter(groups).map(|(group, pages)| async move {
            let shared = OnceCell::<Buffer>::new();
            let mut result = Vec::with_capacity(pages.len());
            for (reference, range, fill) in pages {
                let batch = fill.get_or_try_init(|| async {
                    let bytes = shared.get_or_try_init(|| async {
                        observe_request(kind, group.end - group.start);
                        let lease = if whole { storage.get(object).await? }
                            else { storage.get_scan_range(object, group.clone()).await? };
                        Ok::<_, rtdb_types::Anomaly>(Buffer::from(bytes::Bytes::from_owner(lease)))
                    }).await?;
                    let start = usize::try_from(range.start - group.start).map_err(fault)?;
                    let length = usize::try_from(range.end - range.start).map_err(fault)?;
                    if start.checked_add(length).is_none_or(|end| end > bytes.len()) {
                        return Err(corrupt("Search planned range exceeds its returned bytes"));
                    }
                    let input = reference.authenticate(bytes.slice_with_length(start, length))?;
                    self.decode(input, Some(cache), length <= cache.limit && length != bytes.len(), length)
                }).await?;
                result.push((reference, batch.clone()));
            }
            Ok::<_, rtdb_types::Anomaly>(result)
        }).buffer_unordered(8).try_collect::<Vec<_>>().await?;
        result.extend(fetched.into_iter().flatten());
        Ok(result)
    }

    async fn children(&self, parent: PageRef) -> Result<Vec<DecodedChild>> {
        let page = self.page(parent, PageKind::Directory).await?;
        Self::decode_children(parent, &page)
    }

    fn decode_children(parent: PageRef, page: &RecordBatch) -> Result<Vec<DecodedChild>> {
        let children = table(page, Table::Directory);
        if children.len() > FANOUT || children.columns().iter().any(|column| column.null_count() != 0) {
            return Err(corrupt("Search directory page is invalid"));
        }
        let lower = children.column(0).as_binary::<i32>();
        let upper = children.column(1).as_binary::<i32>();
        let mut decoded: Vec<DecodedChild> = Vec::with_capacity(children.len());
        for row in 0..children.len() {
            let page = locator_at(&children, 2, row)?;
            let previous = decoded.last().is_some_and(|child| child.upper.as_slice() >= lower.value(row));
            if previous || lower.value(row) > upper.value(row) || page.range()?.end > parent.offset {
                return Err(corrupt("Search directory child is out of order"));
            }
            decoded.push(DecodedChild::new(lower, upper, row, page));
        }
        Ok(decoded)
    }

    pub async fn covering(&self, directory: DirectoryRef, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<PageRef>> {
        if ranges.is_empty() { return Ok(Vec::new()); }
        let mut level = BTreeMap::from([(directory.page, 0..ranges.len())]);
        for _ in 0..directory.height {
            let pages = self.pages(level.keys().copied().collect(), PageKind::Directory).await?;
            let mut following = BTreeMap::new();
            for (parent, scope) in level {
                let candidates = &ranges[scope.clone()];
                for child in Self::decode_children(parent, &pages[&parent])? {
                    let first = candidates.partition_point(|(_, last)| last.as_slice() < child.lower.as_slice());
                    let end = candidates.partition_point(|(first, _)| first.as_slice() <= child.upper.as_slice());
                    if first < end { following.insert(child.page, scope.start + first..scope.start + end); }
                }
            }
            level = following;
        }
        Ok(level.into_keys().collect())
    }

    pub async fn find(&self, directory: DirectoryRef, key: &[u8]) -> Result<Option<PageRef>> {
        let mut current = directory.page;
        for _ in 0..directory.height {
            let nodes = self.children(current).await?;
            let index = nodes.partition_point(|child| child.upper.as_slice() < key);
            let Some(node) = nodes.get(index).filter(|child| child.lower.as_slice() <= key) else { return Ok(None); };
            current = node.page;
        }
        Ok(Some(current))
    }

    pub async fn find_ordinals(&self, directory: DirectoryRef, ordinals: Vec<usize>) -> Result<Vec<(usize, PageRef)>> {
        let keys: Vec<_> = ordinals.iter().map(|&ordinal| super::tree_file::ordinal(ordinal)).collect();
        ordinals.into_iter().zip(self.find_sorted_keys(directory, &keys).await?).map(|(ordinal, page)| {
            page.map(|page| (ordinal, page)).ok_or_else(|| corrupt("Search page is absent from its directory"))
        }).collect()
    }

    pub(super) async fn find_sorted_keys<K: AsRef<[u8]> + Sync>(&self, directory: DirectoryRef, keys: &[K]) -> Result<Vec<Option<PageRef>>> {
        if keys.is_empty() { return Ok(Vec::new()); }
        // Bounded audience reads spend their allowance on one page at a time.
        let concurrency = if audience_work::is_bounded() { 1 } else { 8 };
        let mut level = vec![(directory.page, 0..keys.len())];
        for _ in 0..directory.height {
            level = futures::stream::iter(level).map(|(page, rows)| async move {
                let children = self.children(page).await?;
                let selected = &keys[rows.clone()];
                let mut following = Vec::new();
                for child in children {
                    let first = selected.partition_point(|key| key.as_ref() < child.lower.as_slice());
                    let last = selected.partition_point(|key| key.as_ref() <= child.upper.as_slice());
                    if first < last { following.push((child.page, rows.start + first..rows.start + last)); }
                }
                Ok::<_, rtdb_types::Anomaly>(following)
            }).buffered(concurrency).try_concat().await?;
        }
        let mut found = vec![None; keys.len()];
        for (page, rows) in level { found[rows].fill(Some(page)); }
        Ok(found)
    }

    pub async fn leaves(&self, directory: DirectoryRef) -> Result<Vec<PageRef>> {
        self.between(directory, None).await
    }

    pub async fn between(&self, directory: DirectoryRef, interval: Option<(&[u8], &[u8])>) -> Result<Vec<PageRef>> {
        let mut pages = Vec::new();
        self.visit_leaves(directory, interval, |page| { pages.push(page); Ok(()) }).await?;
        Ok(pages)
    }

    pub(super) async fn visit_leaves(&self, directory: DirectoryRef, interval: Option<(&[u8], &[u8])>,
        mut visit: impl FnMut(PageRef) -> Result<()> + Send,
    ) -> Result<()> {
        let mut todo = Vec::with_capacity(1 + directory.height as usize * FANOUT);
        todo.push((directory.page, directory.height));
        while let Some((page, height)) = todo.pop() {
            if height == 0 { visit(page)?; continue; }
            todo.extend(self.children(page).await?.iter().rev()
                .filter(|child| interval.is_none_or(|(lower, upper)| child.upper.as_slice() >= lower && child.lower.as_slice() <= upper))
                .map(|child| (child.page, height - 1)));
        }
        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{TREE_FORMAT_VERSION, tree_file::ordinal};
    use rtdb_storage::{MemoryStore, ObjectStore};

    #[tokio::test]
    async fn directory_fences_share_leased_bytes_and_ordinals_release_each_page() -> Result<()> {
        let first = 0;
        let second = 0x8000_0000;
        let child = |ordinal_value, page| {
            let key = ordinal(ordinal_value);
            Child { lower: key.to_vec(), upper: key.to_vec(), page }
        };
        let mut bytes = Vec::new();
        let mut writer = Writer::new(&mut bytes, TREE_FORMAT_VERSION)?;
        let first_leaf = writer.page([])?;
        let second_leaf = writer.page([])?;
        let first_directory = write_directory(&mut writer, vec![child(first, first_leaf)])?.page;
        let second_directory = write_directory(&mut writer, vec![child(second, second_leaf)])?.page;
        let root = write_directory(&mut writer, vec![child(first, first_directory), child(second, second_directory)])?.page;
        writer.finish()?;
        let directory = DirectoryRef { page: root, height: 2 };

        let page_bytes = |page: PageRef| page.metadata as usize + page.body as usize;
        let limit = page_bytes(root).max(page_bytes(first_directory)).max(page_bytes(second_directory));
        assert!(page_bytes(first_directory) + page_bytes(second_directory) > limit);
        let length = bytes.len() as u64;
        let object = ObjectKey::new("search/directory-fences")?;
        let store = Arc::new(MemoryStore::default());
        store.put(&object, bytes.into()).await?;
        let pool = Arc::new(BufferPool::new(store, limit));
        let source = PageSource::Remote(PageOwner::new(&pool, object, Arc::new(PageCache::new(0))));
        let reader = TreeReader::new(source, length, TREE_FORMAT_VERSION, MemoryBudget::new(1 << 20));

        let batch = reader.page(root, PageKind::Directory).await?;
        let fields = table(&batch, Table::Directory);
        let mut children = TreeReader::decode_children(root, &batch)?;
        let last = children.pop().unwrap();
        assert_eq!(last.lower.as_ptr(), fields.column(0).as_binary::<i32>().value(1).as_ptr());
        assert_eq!(last.upper.as_ptr(), fields.column(1).as_binary::<i32>().value(1).as_ptr());
        drop((batch, fields, children));
        assert_eq!(last.lower.as_slice(), &ordinal(second));
        assert_eq!(last.upper.as_slice(), &ordinal(second));
        assert_eq!(pool.pool_retained_bytes(), page_bytes(root));
        drop(last);
        assert_eq!(pool.pool_retained_bytes(), 0);

        assert_eq!(reader.find(directory, &ordinal(second)).await?, Some(second_leaf));
        assert_eq!(reader.find(directory, &ordinal(1)).await?, None);
        assert_eq!(
            reader.find_sorted_keys(directory, &[ordinal(first), ordinal(first), ordinal(1), ordinal(second)]).await?,
            vec![Some(first_leaf), Some(first_leaf), None, Some(second_leaf)],
        );
        assert_eq!(reader.find_ordinals(directory, vec![first, second]).await?, vec![(first, first_leaf), (second, second_leaf)]);
        assert_eq!(pool.pool_retained_bytes(), 0);
        Ok(())
    }
}
