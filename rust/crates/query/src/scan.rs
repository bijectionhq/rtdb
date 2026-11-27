mod retained_metadata;

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::io::Cursor;
use std::sync::{Arc, OnceLock};

use arrow_array::{Array, TimestampMicrosecondArray,
    builder::{BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder, TimestampMicrosecondBuilder},
    cast::AsArray};
use arrow_buffer::Buffer;
use bytes::Bytes;
use arrow_schema::DataType;
use async_trait::async_trait;
use rtdb_columnar::{IidView, MemoryBudget, MemoryReservation, OwnedBatch, estimate_builder_bytes, from_field};
use rtdb_format::{data::{EventBatch, PutText}, ipc::{PageDecoder, PageProjection, inspect_file_reserved}, keys::{TableSlug, TrieKey}, metadata::{MetadataFile, MetadataNode}};
use rtdb_index::{CatalogTrie, IndexBatch, LiveTable, LocatedRows, RowSource, Snapshot, TrieState};
use rtdb_plan::{Expr, Scan, Schema, TemporalFilter, UnaryOp};
use rtdb_storage::{ObjectKey, audience_work, cache::{BufferPool, CacheLease, Cached, View}};
use rtdb_temporal::{HistoryResolver, ResolvedRow};
use rtdb_trie::{PageSummary, PruningOptions, TemporalSummary, select_pages_from_iter, select_pages_with_scratch};
use rtdb_types::{ARCHIVE_TX_FROM, ARCHIVE_TX_TO, HOSTED_RECEIPT_TX_ID, OWNED_EVENT_POSITION_COLUMN,
    Anomaly, EntityId, EventHeader, EventOp, EventPosition, Iid, Interval, Result, TemporalBounds, Timestamp, Value};

use crate::columnar::{BatchCursor, Column};
use crate::{CandidateIids, ConstantEvaluator, PreparedSearchQuery, QueryContext, accelerate::{ExecutionConstants, SearchQueries}, context::Execution, fragment::{ScanFragment, ScanFragmenter}, pages::{OwnedPage,PageBuilder}, scan_tasks::{PageRange,ScanTask,ScanTaskPlan}, invalid};

struct DataFileReader {
    storage: Arc<BufferPool>,
    key: ObjectKey,
    opened: Option<OpenedLayout>,
}

struct OpenedLayout {
    layout: Arc<CachedLayout>,
    _opening: Arc<MemoryReservation>,
    whole: Option<CacheLease>,
}

#[derive(Clone, Copy)]
enum DataProjection<'a> {
    Full,
    PutColumns(&'a [String]),
}

impl DataFileReader {
    fn new(storage: &Arc<BufferPool>, key: ObjectKey) -> Self {
        Self { storage: Arc::clone(storage), key, opened: None }
    }

    async fn open(&mut self, memory: &Arc<MemoryBudget>) -> Result<&OpenedLayout> {
        if self.opened.is_none() {
            let whole = if self.storage.object_store().supports_scan_ranges(&self.key) {
                None
            } else {
                Some(self.storage.get_or(&self.key, memory).await?)
            };
            let (layout, opening) = self.open_layout(whole.as_ref(), memory).await?;
            self.opened = Some(OpenedLayout { layout, _opening: opening, whole });
        }
        Ok(self.opened.as_ref().expect("the data file is open"))
    }

    async fn page_rows(&mut self, page: usize, memory: &Arc<MemoryBudget>) -> Result<usize> {
        self.open(memory).await?;
        let opened = self.opened.as_ref().expect("the data file is open");
        let decoder = &opened.layout.decoder;
        let metadata = self.read_range(opened.whole.as_ref(), decoder.page_metadata_range(page)?, memory).await?;
        decoder.page_rows(page, metadata.as_slice())
    }

    async fn decode_event_page(
        &mut self, page: usize, memory: &Arc<MemoryBudget>, projection: DataProjection<'_>, include_builder_bound: bool,
    ) -> Result<(arrow_array::RecordBatch, Arc<MemoryReservation>)> {
        self.open(memory).await?;
        let opened = self.opened.as_ref().expect("the data file is open");
        let reader = &opened.layout.decoder;
        let bytes = self.read_range(opened.whole.as_ref(), reader.page_range(page)?, memory).await?;
        let bound = opened.layout.page_decode_bound(page, &bytes, include_builder_bound)?;
        audience_work::decode_bytes(bound)?;
        let charged = memory.reserve(bound)?;
        let bytes = page_buffer(bytes);
        let batch = match projection {
            DataProjection::Full => reader.decode_page(page, bytes)?,
            DataProjection::PutColumns(columns) => reader.decode_event_page(page, bytes, columns)?,
        };
        Ok((batch, charged))
    }

    async fn decode_scan_page(
        &mut self, page: usize, memory: &Arc<MemoryBudget>, put_columns: impl FnOnce() -> Vec<String>,
    ) -> Result<(Arc<IndexBatch>, Option<Arc<MemoryReservation>>)> {
        self.open(memory).await?;
        let opened = self.opened.as_ref().expect("the data file is open");
        let reader = &opened.layout.decoder;
        let wire = self.read_range(opened.whole.as_ref(), reader.page_range(page)?, memory).await?;
        let bound = opened.layout.page_decode_bound(page, &wire, true)?;

        if let Some(reservation) = self.storage.admit(bound) {
            let range = reader.page_metadata_range(page)?;
            let end = usize::try_from(range.end - range.start).map_err(|error| invalid("page-metadata", error))?;
            let metadata = wire.as_slice().get(..end).ok_or_else(|| invalid("page-metadata", "Arrow page header exceeds its block"))?;
            let rows = reader.page_rows(page, metadata)?;
            // A full fill must fit the cache and query budgets at the same time.
            if let Some(headers) = self.storage.admit(IndexBatch::header_bytes(rows)?)
                && let Ok(charged) = memory.reserve(bound)
            {
                audience_work::decode_bytes(bound)?;
                let batch = reader.decode_page(page, page_buffer(wire))?;
                audience_work::scan_items(batch.num_rows())?;
                let batch = IndexBatch::from_owned_with_headers(OwnedBatch::with_reservation(batch, reservation)?, headers)?;
                return Ok((batch, Some(charged)));
            }
        }

        let selected = put_columns();
        let projected = match reader.narrow_put_projection(&selected) {
            Some(projection) => decode_projected_page(reader, page, &wire, projection, memory)?,
            None => None,
        };
        let (batch, charged) = if let Some(projected) = projected {
            projected
        } else {
            audience_work::decode_bytes(bound)?;
            let charged = memory.reserve(bound)?;
            (reader.decode_page(page, page_buffer(wire))?, charged)
        };
        audience_work::scan_items(batch.num_rows())?;
        Ok((IndexBatch::from_owned(OwnedBatch::with_reservation(batch, charged)?)?, None))
    }

    async fn decode_spatial_page(&mut self, page: usize, memory: &Arc<MemoryBudget>, projection: PageProjection<'_>, expected_metadata: Option<&[u8]>) -> Result<(arrow_array::RecordBatch, Arc<MemoryReservation>)> {
        self.open(memory).await?;
        let opened = self.opened.as_ref().expect("the data file is open");
        let reader = &opened.layout.decoder;
        let wire = self.read_range(opened.whole.as_ref(), reader.page_range(page)?, memory).await?;
        if expected_metadata.is_some_and(|metadata| !wire.as_slice().starts_with(metadata)) {
            return Err(invalid("page-metadata", "Arrow page header changed after row admission"));
        }
        if let Some(projected) = decode_projected_page(reader, page, &wire, projection, memory)? {
            return Ok(projected);
        }

        let bound = reader.full_page_decode_bound(page, wire.as_slice())?;
        audience_work::decode_bytes(bound)?;
        let charge = memory.reserve(bound)?;
        let bytes = match projection {
            PageProjection::Iid => Buffer::from(Bytes::from_owner(wire)),
            PageProjection::PutColumns(_) => page_buffer(wire),
        };
        let batch = reader.decode_page(page, bytes)?;
        Ok((batch, charge))
    }

    async fn read_range(&self, whole: Option<&CacheLease>, range: std::ops::Range<u64>, memory: &Arc<MemoryBudget>) -> Result<CacheLease> {
        if audience_work::is_bounded() {
            let bytes = range.end.checked_sub(range.start).and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| invalid("page-range", "Scan range exceeds addressable memory"))?;
            audience_work::decode_bytes(bytes)?;
        }
        if let Some(whole) = whole {
            let start = usize::try_from(range.start).map_err(|error| invalid("page-range", error))?;
            let end = usize::try_from(range.end).map_err(|error| invalid("page-range", error))?;
            return Ok(whole.slice(start..end)?);
        }
        Ok(self.storage.scan_range(&self.key, range, memory).await?)
    }

    async fn open_layout(&self, whole: Option<&CacheLease>, memory: &Arc<MemoryBudget>) -> Result<(Arc<CachedLayout>, Arc<MemoryReservation>)> {
        if let Some(cached) = self.storage.view::<CachedLayout>(&self.key, View::Layout) {
            audience_work::decode_bytes(cached._reservation.bytes())?;
            let opening = memory.reserve(cached._reservation.bytes())?;
            return Ok((cached, opening));
        }
        let size = match whole { Some(bytes) => bytes.as_slice().len() as u64, None => self.storage.scan_length(&self.key).await? };
        let tail = size.checked_sub(10).ok_or_else(|| invalid("file-size", "Arrow file is shorter than its footer"))?;
        let trailer = self.read_range(whole, tail..size, memory).await?;
        let trailer = trailer.as_slice().try_into().map_err(|_| invalid("file-size", "Arrow footer trailer is truncated"))?;
        let length = arrow_ipc::reader::read_footer_length(trailer).map_err(|error| invalid("file-size", error))?;
        let footer = tail.checked_sub(length as u64).ok_or_else(|| invalid("file-size", "Arrow footer exceeds its file"))?;
        let bytes = self.read_range(whole, footer..tail, memory).await?;
        let bound = length.checked_mul(16).and_then(|bytes| bytes.checked_add(65_536))
            .ok_or_else(|| invalid("file-layout-size", "Arrow footer allocation exceeds addressable memory"))?;
        audience_work::decode_bytes(bound)?;
        let opening = memory.reserve(bound)?;
        let mut decoder = PageDecoder::from_footer(bytes.as_slice(), footer, None)?;
        let mut dictionaries = Vec::new();
        for index in 0..decoder.dictionary_count() {
            let bytes = self.read_range(whole, decoder.dictionary_range(index)?, memory).await?;
            let bound = decoder.dictionary_bound(index, bytes.as_slice())?;
            audience_work::decode_bytes(bound)?;
            dictionaries.push(memory.reserve(bound)?);
            decoder.load_dictionary(index, page_buffer(bytes))?;
        }
        // Dictionary buffers are charged to this query. Cache only directories that do not retain those buffers.
        if dictionaries.is_empty() && let Some(reservation) = self.storage.admit(bound) {
            let layout = Arc::new(CachedLayout {
                decoder, builder_bound: OnceLock::new(), _reservation: reservation, _dictionaries: dictionaries,
            });
            self.storage.insert_view(&self.key, View::Layout, Arc::clone(&layout));
            return Ok((layout, opening));
        }
        let layout = Arc::new(CachedLayout {
            decoder, builder_bound: OnceLock::new(), _reservation: Arc::clone(&opening), _dictionaries: dictionaries,
        });
        Ok((layout, opening))
    }
}

fn decode_projected_page(
    reader: &PageDecoder, page: usize, wire: &CacheLease, projection: PageProjection<'_>, memory: &Arc<MemoryBudget>,
) -> Result<Option<(arrow_array::RecordBatch, Arc<MemoryReservation>)>> {
    let Some((mut batch, charge)) = reader.decode_projected_page_reserved(page, wire.clone(), projection, |bound| {
        audience_work::decode_bytes(bound)?;
        memory.reserve(bound)
    })? else { return Ok(None); };
    if matches!(projection, PageProjection::PutColumns(_)) && batch.get_array_memory_size() > charge.bytes() {
        // Arrow reports the wire owner's full capacity on each slice, even though the lease already funds it.
        let columns = batch.columns().iter().map(|array| rtdb_columnar::retain_array(Arc::clone(array), &charge))
            .collect::<Result<Vec<_>>>()?;
        batch = arrow_array::RecordBatch::try_new(batch.schema(), columns).map_err(|error| invalid("page-size", error))?;
    }
    Ok(Some((batch, charge)))
}

/// Visits pages up to the row limit and returns the number of rows visited.
/// `None` means the next page exceeded the limit after any earlier pages were visited.
#[doc(hidden)]
pub async fn visit_trie_iids(
    context: &crate::QueryContext, storage: Arc<BufferPool>, slug: &TableSlug, trie: &TrieKey,
    limit: usize, mut visit: impl FnMut(Iid) -> Result<()>,
) -> Result<Option<usize>> {
    context.check_cancelled()?;
    let mut file = DataFileReader::new(&storage, ObjectKey::new(slug.data_key(trie))?);
    let memory = &context.memory;
    let pages = file.open(memory).await?.layout.decoder.page_count();
    let mut rows = 0;
    for page in 0..pages {
        context.check_cancelled()?;
        if let Some(held) = file.storage.view::<CachedPage>(&file.key, View::Page(page)) {
            let batch = held.batch.view();
            if batch.len() > limit - rows { return Ok(None); }
            visit_iids(context, batch.iids()?, &mut visit)?;
            rows += batch.len();
        } else {
            let opened = file.opened.as_ref().expect("the data file is open");
            let decoder = &opened.layout.decoder;
            let metadata = file.read_range(opened.whole.as_ref(), decoder.page_metadata_range(page)?, memory).await?;
            let page_rows = decoder.page_rows(page, metadata.as_slice())?;
            if page_rows > limit - rows { return Ok(None); }

            context.check_cancelled()?;
            let (batch, _charged) = file.decode_spatial_page(page, memory, PageProjection::Iid, Some(metadata.as_slice())).await?;
            let column = batch.column_by_name("_iid").ok_or_else(|| invalid("page-iid", "Arrow page has no IID field"))?;
            let iids = IidView::new(column.as_ref())?;
            if iids.len() != page_rows {
                return Err(invalid("page-rows", "Arrow IID count differs from the admitted page row count"));
            }
            visit_iids(context, iids, &mut visit)?;
            rows += page_rows;
        }
    }
    Ok(Some(rows))
}

#[doc(hidden)]
pub async fn visit_trie_events(
    storage: &Arc<BufferPool>, memory: &Arc<MemoryBudget>, cancellation: &tokio_util::sync::CancellationToken,
    slug: &TableSlug, trie: &TrieKey, put_columns: &[String], visit: impl FnMut(&EventBatch) -> Result<()>,
) -> Result<()> {
    visit_trie_events_as(storage, memory, cancellation, slug, trie, DataProjection::PutColumns(put_columns), visit).await
}

#[doc(hidden)]
pub async fn visit_full_trie_events(
    storage: &Arc<BufferPool>, memory: &Arc<MemoryBudget>, cancellation: &tokio_util::sync::CancellationToken,
    slug: &TableSlug, trie: &TrieKey, visit: impl FnMut(&EventBatch) -> Result<()>,
) -> Result<()> {
    visit_trie_events_as(storage, memory, cancellation, slug, trie, DataProjection::Full, visit).await
}

async fn visit_trie_events_as(
    storage: &Arc<BufferPool>, memory: &Arc<MemoryBudget>, cancellation: &tokio_util::sync::CancellationToken,
    slug: &TableSlug, trie: &TrieKey, projection: DataProjection<'_>, mut visit: impl FnMut(&EventBatch) -> Result<()>,
) -> Result<()> {
    let mut file = DataFileReader::new(storage, ObjectKey::new(slug.data_key(trie))?);
    let pages = file.open(memory).await?.layout.decoder.page_count();
    for page in 0..pages {
        if cancellation.is_cancelled() { return Err(point_cancelled()); }
        let (batch, _charge) = file.decode_event_page(page, memory, projection, true).await?;
        visit(&EventBatch::new(batch)?)?;
    }
    Ok(())
}

#[doc(hidden)]
pub async fn visit_projected_data_events(
    storage: &Arc<BufferPool>, key: &ObjectKey, memory: &Arc<MemoryBudget>, put_columns: &[String],
    check_cancelled: impl Fn() -> Result<()>, mut visit: impl FnMut(&EventBatch) -> Result<()>,
) -> Result<()> {
    check_cancelled()?;
    let mut file = DataFileReader::new(storage, key.clone());
    let pages = file.open(memory).await?.layout.decoder.page_count();
    for page in 0..pages {
        check_cancelled()?;
        let (batch, _charge) = file.decode_event_page(page, memory, DataProjection::PutColumns(put_columns), false).await?;
        check_cancelled()?;
        visit(&EventBatch::new(batch)?)?;
    }
    Ok(())
}

/// Counts rows from persisted IPC message metadata, stopping above the requested limit.
#[doc(hidden)]
pub async fn trie_row_count_up_to(
    context: &crate::QueryContext, storage: Arc<BufferPool>, slug: &TableSlug, trie: &TrieKey, limit: usize,
) -> Result<Option<usize>> {
    context.check_cancelled()?;
    let mut file = DataFileReader::new(&storage, ObjectKey::new(slug.data_key(trie))?);
    let pages = file.open(&context.memory).await?.layout.decoder.page_count();
    let mut rows = 0usize;
    for page in 0..pages {
        context.check_cancelled()?;
        let page_rows = file.page_rows(page, &context.memory).await?;
        if page_rows > limit - rows { return Ok(None); }
        rows += page_rows;
    }
    Ok(Some(rows))
}

fn visit_iids(context: &crate::QueryContext, iids: IidView<'_>, visit: &mut impl FnMut(Iid) -> Result<()>) -> Result<()> {
    for row in 0..iids.len() {
        context.check_cancelled_at(row)?;
        visit(iids.get(row)?)?;
    }
    Ok(())
}

async fn read_metadata(storage: &Arc<BufferPool>, key: &ObjectKey, memory: &Arc<MemoryBudget>) -> Result<(Arc<MetadataFile>, Arc<MemoryReservation>)> {
    if let Some(cached) = storage.view::<CachedMetadata>(key, View::File) {
        audience_work::decode_bytes(cached.bound)?;
        return Ok((Arc::clone(&cached.metadata), memory.reserve(cached.bound)?));
    }
    let (lease, cache_owned) = match storage.get(key).await {
        Ok(lease) => (lease, true),
        Err(rtdb_storage::Error::MemoryLimit { .. }) => (storage.get_or(key, memory).await?, false),
        Err(error) => return Err(error.into()),
    };
    audience_work::decode_bytes(lease.as_slice().len())?;
    let mut cursor = Cursor::new(lease);
    let layout = inspect_file_reserved(&mut cursor, |bytes| memory.reserve(bytes))?;
    let bound = layout.full_allocation_bound()?;
    audience_work::decode_bytes(bound)?;
    let reservation = memory.reserve(bound)?;
    let file = if cache_owned {
        rtdb_format::ipc::ArrowFile::from_inspected_buffer(page_buffer(cursor.into_inner()), None, layout)?
    } else {
        rtdb_format::ipc::ArrowFile::from_inspected_reader(cursor, None, layout)?
    };
    let metadata = Arc::new(MetadataFile::read_file(file)?);
    if let Some(retained) = storage.admit(metadata.stored_bytes())
        && storage.insert_view(key, View::File, Arc::new(CachedMetadata { metadata: Arc::clone(&metadata), bound, _reservation: retained })) {
        storage.evict(key);
    }
    Ok((metadata, reservation))
}

/// The events of `keys`, or of every entity, in one primary trie, read from the data pages whose metadata path covers them.
/// `None` once more than `max_rows` rows would be inspected.
pub(crate) async fn primary_point_events(
    request: &crate::ScanRequest<'_>, slug: &TableSlug, trie: &TrieKey, keys: Option<crate::IidScope<'_>>, max_rows: usize,
    mut visit: impl FnMut(crate::LocatedHistoryEvent<()>, Option<&[u8]>) -> Result<bool>,
) -> Result<Option<usize>> {
    let memory = request.memory;
    let metadata_key = ObjectKey::new(slug.metadata_key(trie))?;
    let (metadata, _metadata) = read_metadata(request.storage, &metadata_key, memory).await?;
    let mut pages = BTreeSet::new();
    let mut pending = metadata.root_index().map(|root| vec![(root, Vec::<u8>::new())]).unwrap_or_default();
    while let Some((node, path)) = pending.pop() {
        if request.cancellation.is_cancelled() { return Err(point_cancelled()); }
        audience_work::scan_items(1)?;
        let (lower, upper) = path_range(&path)?;
        if keys.is_some_and(|keys| !keys.intersects(lower, upper)) { continue; }
        match &metadata.nodes()[node] {
            MetadataNode::Nil => {}
            MetadataNode::Leaf { data_page_index, .. } => { pages.insert(*data_page_index); }
            MetadataNode::IidBranch(children) => for (bucket, child) in children.iter().enumerate() {
                if let Some(child) = child { pending.push((*child, [path.as_slice(), &[bucket as u8]].concat())); }
            },
            MetadataNode::RecencyBranch(children) => for (_, child) in children { pending.push((*child, path.clone())); },
        }
    }
    let mut file = DataFileReader::new(request.storage, ObjectKey::new(slug.data_key(trie))?);
    let mut inspected = 0usize;
    for page in pages {
        if request.cancellation.is_cancelled() { return Err(point_cancelled()); }
        let remaining = max_rows - inspected;
        let held = file.storage.view::<CachedPage>(&file.key, View::Page(page));
        let preflight_rows = match &held {
            Some(held) => Some(held.batch.view().len()),
            None if max_rows != usize::MAX => Some(file.page_rows(page, memory).await?),
            None => None,
        };
        if let Some(rows) = preflight_rows {
            if request.cancellation.is_cancelled() { return Err(point_cancelled()); }
            if rows > remaining { return Ok(None); }
            audience_work::scan_items(rows)?;
        }
        let (batch, _charged) = match held {
            Some(held) => {
                audience_work::decode_bytes(held.bound)?;
                (held.batch.view().clone(), memory.reserve(held.bound)?)
            }
            None => {
                let (batch, charge) = file.decode_event_page(page, memory, DataProjection::PutColumns(&[]), true).await?;
                (EventBatch::new(batch)?, charge)
            }
        };
        let rows = batch.len();
        match preflight_rows {
            Some(expected) if rows != expected => {
                return Err(Anomaly::fault("rtdb.query/point-page-rows", "Primary page row count differs from its metadata"));
            }
            Some(_) => {}
            None => {
                if rows > remaining { return Ok(None); }
                audience_work::scan_items(rows)?;
            }
        }
        inspected += rows;
        let page = u32::try_from(page).map_err(|error| invalid("point-page", error))?;
        for row in 0..rows {
            let iid = batch.iid(row)?;
            if keys.is_some_and(|keys| !keys.contains(&iid)) { continue; }
            let event = crate::LocatedHistoryEvent { header: batch.header(row)?, origin: batch.origin(row)?, witness: None,
                location: crate::SearchRowLocation::Persisted { trie: trie.clone(), page,
                    row: u32::try_from(row).map_err(|error| invalid("point-row", error))? } };
            if !visit(event, batch.entity_key(row)?)? { return Ok(None); }
        }
    }
    Ok(Some(inspected))
}

fn point_cancelled() -> Anomaly { Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "rtdb.query/cancelled", "Primary point read cancelled") }

fn page_buffer(bytes: CacheLease) -> Buffer {
    // A cached page must release the rest of a whole object when the query is done reading it.
    if bytes.is_partial_allocation() { Buffer::from(bytes.as_slice().to_vec()) }
    else { Buffer::from(Bytes::from_owner(bytes)) }
}

struct DataFile {
    reader: DataFileReader,
    /// The trie's metadata file, where an ordered scan finds the statistics of the leaves it comes to.
    metadata: ObjectKey,
}

#[derive(Default)]
struct FileDescriptors {
    entries: Vec<DataFile>,
    _reservation: Option<Arc<MemoryReservation>>,
}

impl FileDescriptors {
    fn admit(&mut self, count: usize, memory: &Arc<MemoryBudget>) -> Result<()> {
        if self._reservation.is_some() { return Ok(()); }
        let bytes = count.checked_mul(std::mem::size_of::<DataFile>()).and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(|| invalid("scan-file-size", "Scan file descriptor capacity exceeds addressable memory"))?;
        let reservation = memory.reserve(bytes)?;
        self.entries.try_reserve_exact(count).map_err(|error| invalid("scan-file-size", error))?;
        self._reservation = Some(reservation);
        Ok(())
    }
}

enum PageSource { Live { table: Arc<LiveTable>, rows: Arc<LocatedRows> }, File { file: usize, page: usize, trie: Arc<TrieKey> } }
/// `unread` names the metadata node whose statistics have not been tested yet: until they are, the page counts as matching and has no temporal bounds.
struct Page { lower: Iid, upper: Iid, priority: usize, source: PageSource, summary: PageSummary, content_matches: bool, unread: Option<usize> }

#[derive(Default)]
struct PageDescriptors {
    entries: Vec<Page>,
    capacity: usize,
    _reservation: Option<Arc<MemoryReservation>>,
}

impl PageDescriptors {
    fn push(&mut self, page: Page, memory: &Arc<MemoryBudget>) -> Result<()> {
        if self.entries.len() == self.capacity {
            let size_error = || invalid("scan-page-size", "Scan page descriptor capacity exceeds addressable memory");
            let capacity = self.capacity.checked_mul(2).ok_or_else(size_error)?.max(1);
            let bytes = capacity.checked_mul(std::mem::size_of::<Page>())
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
            let reservation = memory.reserve(bytes)?;
            self.entries.try_reserve_exact(capacity - self.entries.len()).map_err(|error| invalid("scan-page-size", error))?;
            self.capacity = capacity;
            self._reservation = Some(reservation);
        }
        self.entries.push(page);
        Ok(())
    }
}

const MAX_IID_DEPTH: usize = 64;

#[derive(Clone, Copy)]
struct MetadataVisit { node: usize, path: [u8; MAX_IID_DEPTH], depth: usize, recency: Timestamp }

#[derive(Default)]
struct MetadataStack {
    entries: Vec<MetadataVisit>,
    capacity: usize,
    _reservation: Option<Arc<MemoryReservation>>,
}

impl MetadataStack {
    fn push(&mut self, visit: MetadataVisit, memory: &Arc<MemoryBudget>) -> Result<()> {
        if self.entries.len() == self.capacity {
            let size_error = || invalid("scan-metadata-size", "Scan metadata traversal capacity exceeds addressable memory");
            let capacity = self.capacity.checked_mul(2).ok_or_else(size_error)?.max(1);
            let bytes = capacity.checked_mul(std::mem::size_of::<MetadataVisit>())
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
            let reservation = memory.reserve(bytes)?;
            self.entries.try_reserve_exact(capacity - self.entries.len()).map_err(|error| invalid("scan-metadata-size", error))?;
            self.capacity = capacity;
            self._reservation = Some(reservation);
        }
        self.entries.push(visit);
        Ok(())
    }
}

fn summary_bytes(count: usize) -> Result<usize> {
    count.checked_mul(std::mem::size_of::<PageSummary>()).and_then(|bytes| bytes.checked_add(256))
        .ok_or_else(|| invalid("scan-summary-size", "Scan summary capacity exceeds addressable memory"))
}

/// A loaded page's rows.
enum ActiveRows { Live { table: Arc<LiveTable>, rows: Arc<LocatedRows> }, File(Arc<IndexBatch>) }
struct LoadedPage { rows: ActiveRows, _reservation: Arc<MemoryReservation>, _charge: Option<Arc<MemoryReservation>> }
impl ActiveRows {
    fn len(&self) -> usize { match self { Self::Live { rows, .. } => rows.located.len(), Self::File(batch) => batch.len() } }
    fn row(&self, index: usize) -> Result<RowSource> {
        match self { Self::Live { table, rows } => table.row_at(rows.located[index]), Self::File(batch) => Ok(RowSource { batch: Arc::clone(batch), row: index }) }
    }
    /// The batch holding row `index`, the slot that names it within this page, and the row's position in it.
    fn batch(&self, index: usize) -> Result<(usize, &Arc<IndexBatch>, u32)> {
        match self {
            Self::Live { table, rows } => { let at = rows.located[index]; let (batch, row) = table.batch_at(at)?; Ok((at.0 as usize, batch, row)) }
            Self::File(batch) => Ok((0, batch, u32::try_from(index).map_err(|_| invalid("page-size", "A page row exceeds u32 addressing"))?)),
        }
    }
    fn header(&self, index: usize) -> Result<EventHeader> {
        match self { Self::Live { rows, .. } => Ok(rows.headers[index]), Self::File(batch) => batch.header(index) }
    }

    fn after_iid(&self, head: Head, upper: Iid) -> Result<Option<Head>> {
        let low = self.seek(head.ordinal + 1, |header| header.iid <= head.header.iid)?;
        if low == self.len() { return Ok(None); }
        let header = self.header(low)?;
        Ok((header.iid <= upper).then_some(Head { header, ordinal: low, ..head }))
    }

    fn seek(&self, mut low: usize, before: impl Fn(EventHeader) -> bool) -> Result<usize> {
        let mut high = self.len();
        while low < high {
            let middle = (low + high) / 2;
            if before(self.header(middle)?) { low = middle + 1; }
            else { high = middle; }
        }
        Ok(low)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Head { header: EventHeader, active: usize, priority: usize, ordinal: usize }
impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        other.header.iid.cmp(&self.header.iid).then_with(|| self.header.precedence().cmp(&other.header.precedence()))
            .then_with(|| other.priority.cmp(&self.priority)).then_with(|| other.ordinal.cmp(&self.ordinal))
            .then_with(|| other.active.cmp(&self.active))
    }
}
impl PartialOrd for Head { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }

fn covers_history(header: EventHeader, bounds: TemporalBounds) -> bool {
    header.op == EventOp::Erase
        || (header.system_from <= bounds.system_time.lower()
            && header.valid_time.lower() <= bounds.valid_time.lower()
            && header.valid_time.upper() >= bounds.valid_time.upper())
}

#[derive(Clone)]
struct RankProjection {
    output: rtdb_plan::ColumnId,
    source: String,
    scorer: Option<Arc<rtdb_text::Scorer>>,
    verified: Option<crate::CandidateScores>,
    live: Option<Arc<dyn crate::LiveSearchTerms>>,
}

struct ScanMatch {
    query: Option<Arc<rtdb_text::Query>>,
    files: Option<crate::MatchAnswers>,
    live: Option<Arc<dyn crate::LiveSearchTerms>>,
}

struct ResolvedScanTask {
    selected: Vec<usize>,
    groups: Vec<Vec<ResolvedRow<(u32, u32)>>>,
    batches: Vec<Arc<IndexBatch>>,
    reservation: Arc<MemoryReservation>,
    _group_reservations: Vec<Arc<MemoryReservation>>,
}

pub(crate) struct PartitionScan {
    snapshot: Snapshot,
    storage: Arc<BufferPool>,
    scan: Scan,
    execution: Execution,
    bounds: TemporalBounds,
    initialized: bool,
    files: FileDescriptors,
    pages: PageDescriptors,
    tasks: Option<ScanTaskPlan>,
    loaded: BTreeMap<usize, LoadedPage>,
    output: VecDeque<crate::columnar::ColumnBatch>,
    output_reservation: Option<Arc<MemoryReservation>>,
    counts: ScanCounts,
    pushdowns: Option<Arc<crate::pushdown::Pushdowns>>,
    canonical_entity_iids: bool,
    /// The inclusive IID range the scan is confined to, by its fragment's prefix and its `_id` range predicates; every IID, otherwise.
    confined: Option<(Iid, Iid)>,
    /// Set where the scan's rows must leave it in the order of their IIDs; they leave page by page, otherwise.
    order: Option<IidOrder>,
    /// The scan's predicates as tests of a page's statistics, kept by an ordered scan for the leaves it has yet to reach.
    page_test: Option<crate::page_metadata::PageTest>,
    /// Parallel to the scan's matches: the query (`None` where it is null, so MATCH is too), and an accelerator's answers for some of the partition's data files.
    matches: Vec<ScanMatch>,
    ranking: Option<RankProjection>,
    candidate_iids: Option<crate::CandidateIids>,
    /// Parallel to the scan's columns: the match each evaluates, if any.
    slots: Vec<Option<usize>>,
    metadata_time: Option<Option<Value>>,
}

use crate::analysis::ScanCounts;

/// A scan selecting fewer IIDs than a leaf's rows over this locates only those IIDs' rows of the leaf rather than all of them.
const LOCATED_IID_FRACTION: usize = 8;

/// The IIDs an `_id` predicate selects, as the reference's scan narrows to them: `_id = v`, or an OR of such, with every
/// `v` a literal or parameter of an ID type. `None` where some `v` can't be proven to name known IIDs — `_id = 7.0` still
/// matches the integer ID 7 — so the scan reads every IID; the predicate itself is applied either way.
fn id_selection(scan: &Scan, parameters: rtdb_expr::Parameters<'_>) -> Option<std::collections::BTreeSet<Iid>> {
    let id = scan.columns.iter().find(|field| field.name == "_id")?.id;
    let eid = |expr: &Expr| -> Option<(Iid, Option<Iid>)> {
        if let Some(uuid) = uuid_constant(expr, parameters) { return Some((Iid(uuid), None)); }
        let value = match expr {
            Expr::Literal(value) => value,
            Expr::Parameter { index, .. } => {
                if let Some(cell) = parameters.arrow_cell(*index) { return arrow_parameter_iids(cell); }
                parameters.known(*index)?
            }
            _ => return None,
        };
        value_iids(value)
    };
    let equality = |expr: &Expr| {
        let Expr::Binary { op: rtdb_plan::BinaryOp::Eq, left, right } = expr else { return None };
        match (left.as_ref(), right.as_ref()) {
            (Expr::Column { id: column, .. }, value) | (value, Expr::Column { id: column, .. }) if *column == id => eid(value),
            _ => None,
        }
    };
    fn disjuncts<'e>(expr: &'e Expr, out: &mut Vec<&'e Expr>) {
        match expr { Expr::Binary { op: rtdb_plan::BinaryOp::Or, left, right } => { disjuncts(left, out); disjuncts(right, out); } other => out.push(other) }
    }
    scan.predicates.iter().filter(|predicate| predicate.referenced_columns() == std::collections::BTreeSet::from([id])).find_map(|predicate| {
        let mut clauses = Vec::new();
        disjuncts(predicate, &mut clauses);
        let mut iids = std::collections::BTreeSet::new();
        for clause in clauses {
            if let Expr::Call { name, args, .. } = clause
                && name == "_id_eq_any"
                && let [Expr::Column { id: column, .. }, Expr::Parameter { index, .. }] = args.as_slice()
                && *column == id
            {
                collect_array_iids(parameters, *index, &mut iids)?;
                continue;
            }
            let (iid, reference) = equality(clause)?;
            iids.insert(iid);
            iids.extend(reference);
        }
        Some(iids)
    })
}

pub(crate) fn claim_regions(scan: &Scan, context: &QueryContext, arguments: &[Value]) -> Vec<rtdb_types::rtdb_v1::ClaimRegion> {
    use rtdb_types::rtdb_v1::{ClaimRegion, MAX_REGIONS_PER_TABLE};

    let table = &scan.table.name.table;
    let parameters = rtdb_expr::Parameters::Values(arguments);
    if let Some(iids) = scan.iids.clone().or_else(|| id_selection(scan, parameters)) {
        if iids.is_empty() || iids.len() > MAX_REGIONS_PER_TABLE {
            return vec![ClaimRegion::Table(table.clone())];
        }
        return iids.into_iter().map(|iid| ClaimRegion::Row { table: table.clone(), iid }).collect();
    }
    if context.catalog.database(&scan.table.name.database).is_ok_and(|database| stores_uuid_ids(table, &database.partitions))
        && let Ok(Some((lower, upper))) = id_interval(scan, parameters)
    {
        return vec![ClaimRegion::IidRange { table: table.clone(), lower, upper }];
    }
    vec![ClaimRegion::Table(table.clone())]
}

fn value_iids(value: &Value) -> Option<(Iid, Option<Iid>)> {
    match value {
        Value::Keyword(value) => Some(keyword_iids(value.as_str())),
        _ => value.entity_id().ok().map(|entity| (entity.iid(), None)),
    }
}

fn collect_array_iids(parameters: rtdb_expr::Parameters<'_>, index: usize, iids: &mut BTreeSet<Iid>) -> Option<()> {
    const LIMIT: usize = 4096;
    let mut insert = |(iid, alternate)| {
        iids.insert(iid);
        iids.extend(alternate);
    };
    if let Some(array) = parameters.arrow_cell(index) {
        if array.is_null().ok()? { return Some(()); }
        let cells = array.list()?;
        if cells.len() > LIMIT { return None; }
        for cell in cells {
            let cell = cell.physical().ok()?;
            if !cell.is_null().ok()? { insert(arrow_parameter_iids(cell)?); }
        }
    } else {
        let parameter = parameters.known(index)?;
        if matches!(parameter, Value::Null | Value::Undefined) { return Some(()); }
        let Value::List(_) = parameter else { return None; };
        let values = parameter.list_view()?;
        if values.len() > LIMIT { return None; }
        for value in values.iter() {
            if !matches!(value.as_ref(), Value::Null | Value::Undefined) { insert(value_iids(&value)?); }
        }
    }
    Some(())
}
/// The order of a scan's rows by IID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IidOrder { Ascending, Descending }

/// Whether every `_id` the snapshots hold for the table is a UUID, each then being its own IID. A plan records the type
/// it was prepared against, and a row of another ID type may have been written since.
pub(crate) fn stores_uuid_ids(table: &rtdb_types::TableRef, snapshots: &[Snapshot]) -> bool {
    let uuid = rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Uuid);
    snapshots.iter().all(|snapshot| snapshot.column_types_for(table, ["_id".to_owned()]).get("_id").is_some_and(|ty| *ty == uuid || *ty == rtdb_types::LogicalType::Nothing))
}

/// The UUID a literal or parameter is, or that the cast of a string one to UUID gives, which is how `UUID '...'` is bound.
fn uuid_constant(expr: &Expr, parameters: rtdb_expr::Parameters<'_>) -> Option<[u8; 16]> {
    fn constant<'v>(expr: &'v Expr, parameters: rtdb_expr::Parameters<'v>) -> Option<&'v Value> {
        match expr { Expr::Literal(value) => Some(value), Expr::Parameter { index, .. } => parameters.known(*index), _ => None }
    }
    let uuid = rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Uuid);
    let (operand, cast) = match expr {
        Expr::Cast { expr, target: rtdb_plan::CastTarget::Fixed(target) } if *target == uuid => (expr.as_ref(), true),
        _ => (expr, false),
    };
    if let Expr::Parameter { index, .. } = operand && let Some(cell) = parameters.arrow_cell(*index) {
        if cell.array.is_null(cell.row) { return None; }
        let extension = cell.field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY).map(String::as_str);
        return match (cell.field.data_type(), extension, cast) {
            (DataType::FixedSizeBinary(16), Some("uuid"), _) => cell.array.as_fixed_size_binary().value(cell.row).try_into().ok(),
            (DataType::Utf8, None, true) => EntityId::uuid(cell.array.as_string::<i32>().value(cell.row)).ok().map(|id| id.iid().0),
            _ => None,
        };
    }
    let value = match expr {
        Expr::Cast { expr, target: rtdb_plan::CastTarget::Fixed(target) } if *target == uuid => match constant(expr, parameters)? {
            text @ Value::Utf8(_) => return match rtdb_expr::cast(text, &uuid, "UTC").ok()? { Value::Uuid(bytes) => Some(bytes), _ => None },
            value => value,
        },
        expr => constant(expr, parameters)?,
    };
    match value { Value::Uuid(bytes) => Some(*bytes), _ => None }
}

fn keyword_iids(name: &str) -> (Iid, Option<Iid>) {
    let [native, reference] = EntityId::keyword_lookup_iids(name);
    (native, Some(reference))
}

fn arrow_parameter_iids(cell: rtdb_expr::ArrowParameterCell<'_>) -> Option<(Iid, Option<Iid>)> {
    if cell.array.is_null(cell.row) { return None; }
    let extension = cell.field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY).map(String::as_str);
    match (cell.field.data_type(), extension) {
        (DataType::FixedSizeBinary(16), Some("uuid")) =>
            Some((Iid(cell.array.as_fixed_size_binary().value(cell.row).try_into().ok()?), None)),
        (DataType::Utf8, None) => Some((EntityId::string_iid(cell.array.as_string::<i32>().value(cell.row)), None)),
        (DataType::Utf8, Some("xt/clj-keyword")) => Some(keyword_iids(cell.array.as_string::<i32>().value(cell.row))),
        _ => cell.signed_integer().map(|value| (EntityId::Integer(value).iid(), None)),
    }
}
/// The inclusive IID interval that `_id` range predicates select: the conjuncts comparing `_id` with a UUID literal or
/// parameter by `<`, `<=`, `>`, `>=` or BETWEEN, where every `_id` of the table is a UUID. A UUID is its own IID and
/// UUIDs order as their bytes do, so those conjuncts hold exactly for the rows whose IID lies in the interval. An ID
/// of any other type is hashed into its IID, so a table holding one is not narrowed. `Err` where no IID satisfies
/// the conjuncts. The predicates are applied to the rows read either way.
fn id_interval(scan: &Scan, parameters: rtdb_expr::Parameters<'_>) -> std::result::Result<Option<(Iid, Iid)>, Unsatisfiable> {
    use rtdb_plan::BinaryOp as Op;
    let Some(field) = scan.columns.iter().find(|field| field.name == "_id") else { return Ok(None) };
    if field.data_type != rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Uuid) { return Ok(None); }
    let is_id = |expr: &Expr| matches!(expr, Expr::Column { id, .. } if *id == field.id);
    let uuid = |expr: &Expr| uuid_constant(expr, parameters).map(u128::from_be_bytes);
    fn conjuncts<'e>(expr: &'e Expr, out: &mut Vec<&'e Expr>) {
        match expr { Expr::Binary { op: rtdb_plan::BinaryOp::And, left, right } => { conjuncts(left, out); conjuncts(right, out); } other => out.push(other) }
    }
    let mut clauses = Vec::new();
    for predicate in &scan.predicates { conjuncts(predicate, &mut clauses); }
    let (mut lower, mut upper) = (u128::MIN, u128::MAX);
    for clause in clauses {
        match clause {
            Expr::Binary { op, left, right } => {
                let (op, bound) = if is_id(left) { (*op, uuid(right)) } else if is_id(right) {
                    (match op { Op::Lt => Op::Gt, Op::LtEq => Op::GtEq, Op::Gt => Op::Lt, Op::GtEq => Op::LtEq, other => *other }, uuid(left))
                } else { continue };
                let Some(bound) = bound else { continue };
                match op {
                    Op::GtEq => lower = lower.max(bound),
                    Op::Gt => lower = lower.max(bound.checked_add(1).ok_or(Unsatisfiable)?),
                    Op::LtEq => upper = upper.min(bound),
                    Op::Lt => upper = upper.min(bound.checked_sub(1).ok_or(Unsatisfiable)?),
                    _ => {}
                }
            }
            Expr::Between { operand, low, high, symmetric: false } if is_id(operand) => {
                if let Some(low) = uuid(low) { lower = lower.max(low); }
                if let Some(high) = uuid(high) { upper = upper.min(high); }
            }
            _ => {}
        }
    }
    if lower > upper { return Err(Unsatisfiable); }
    Ok((lower != u128::MIN || upper != u128::MAX).then(|| (Iid(lower.to_be_bytes()), Iid(upper.to_be_bytes()))))
}
/// No IID satisfies a scan's `_id` range predicates.
#[derive(Debug, PartialEq, Eq)]
struct Unsatisfiable;

/// The IIDs in both inclusive ranges, where each `None` is every IID; `Err` where they share none.
fn intersect(left: Option<(Iid, Iid)>, right: Option<(Iid, Iid)>) -> std::result::Result<Option<(Iid, Iid)>, Unsatisfiable> {
    match (left, right) {
        (Some((first, last)), Some((lower, upper))) => {
            let range = (first.max(lower), last.min(upper));
            if range.0 > range.1 { Err(Unsatisfiable) } else { Ok(Some(range)) }
        }
        (range, None) | (None, range) => Ok(range),
    }
}
/// One source of a scan's rows: a partition this node reads, or a fragment of one that another node may read for it.
enum Part { Local(PartitionScan), Fragment(Fragment), Resolved(Option<crate::columnar::ColumnBatch>) }

/// A fragment dispatched to the node it belongs to, read here instead when that node is this one or does not answer.
struct Fragment { request: Arc<ScanFragment>, fallback: PartitionScan, state: FragmentState }
enum FragmentState { Pending, Dispatched(Dispatch), Remote(VecDeque<crate::columnar::ColumnBatch>), Local, Failed }

/// A dispatch in flight, abandoned with the scan.
struct Dispatch(tokio::task::JoinHandle<Result<Option<crate::FragmentReply>>>);
impl Drop for Dispatch { fn drop(&mut self) { self.0.abort(); } }

impl Fragment {
    fn dispatch(&mut self, fragmenter: &Arc<dyn ScanFragmenter>, memory: &Arc<MemoryBudget>) {
        if !matches!(self.state, FragmentState::Pending) { return; }
        let (fragmenter, request) = (Arc::clone(fragmenter), Arc::clone(&self.request));
        let memory = Arc::clone(memory);
        let work = rtdb_storage::IoRegion::inherit_reading(async move { fragmenter.dispatch_reply(&request, &memory).await });
        self.state = FragmentState::Dispatched(Dispatch(tokio::spawn(rtdb_columnar::arrow_flow::traced(work))));
    }
    async fn next_batch(&mut self, memory: &Arc<MemoryBudget>) -> Result<Option<crate::columnar::ColumnBatch>> {
        if matches!(self.state, FragmentState::Dispatched(_)) {
            let FragmentState::Dispatched(mut dispatch) = std::mem::replace(&mut self.state, FragmentState::Failed) else { unreachable!() };
            self.state = Self::answered(&mut dispatch, memory).await?;
        }
        match &mut self.state {
            FragmentState::Remote(batches) => Ok(batches.pop_front()),
            FragmentState::Pending | FragmentState::Local => self.fallback.next_batch().await,
            FragmentState::Dispatched(_) => Err(invalid("fragment-state", "A dispatched fragment was not awaited")),
            FragmentState::Failed => Err(invalid("fragment-state", "A failed fragment has no further rows")),
        }
    }
    /// Only a declined fragment or an unavailable reader leaves the fragment to this node; every other failure fails the scan.
    async fn answered(dispatch: &mut Dispatch, memory: &Arc<MemoryBudget>) -> Result<FragmentState> {
        let answer = (&mut dispatch.0).await
            .map_err(|error| Anomaly::fault("rtdb.query/fragment-dispatch", format!("A fragment dispatch did not complete: {error}")))?;
        let bytes = match answer {
            Ok(Some(reply)) => reply.await,
            Ok(None) => return Ok(FragmentState::Local),
            Err(error) => Err(error),
        };
        match bytes {
            Ok(bytes) => Ok(FragmentState::Remote(crate::fragment::decode_batches(bytes.into_ipc_buffer(), memory)?.into())),
            Err(error) if error.category == rtdb_types::AnomalyCategory::Unavailable => Ok(FragmentState::Local),
            Err(error) => Err(error),
        }
    }
}

/// The bytes of persisted tries the snapshot's database holds across its tables.
fn persisted_bytes(snapshot: &Snapshot) -> u64 {
    let catalog = snapshot.trie_catalog();
    catalog.tables().flat_map(|table| catalog.iter_tries(table)).filter(|trie| trie.state == TrieState::Live)
        .map(|trie| u64::try_from(trie.data_file_size).unwrap_or(0)).fold(0, u64::saturating_add)
}

/// `fragmenter` is held until the first pull dispatches the fragments.
pub(crate) struct ScanCursor { parts: VecDeque<Part>, fragmenter: Option<Arc<dyn ScanFragmenter>>, completed: ScanCounts, database: String, source: String, memory: Arc<MemoryBudget>, _reservation: Arc<MemoryReservation> }
impl ScanCursor {
    pub async fn new(scan: Scan, execution: Execution, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>) -> Result<Self> {
        let context = Arc::clone(&execution.context);
        let database = context.catalog.database(&scan.table.name.database)?;
        let restricted_iids = context.catalog.owned_candidates(&scan.table.name);
        if restricted_iids.is_some() && scan.iids.is_some() {
            return Err(invalid("scan-iid-scope", "An owned spatial candidate scan already has a fixed IID scope"));
        }
        Self::over_with_queries(scan, execution, pushdowns, &database.partitions, &database.storage,
            true, None, &SearchQueries::default(), restricted_iids).await
    }
    /// A scan of the given partitions of its database, which share `storage`.
    /// With an `order`, each partition's rows leave in that order of their IIDs, and the partition is read here rather than split into fragments.
    async fn over(scan: Scan, execution: Execution, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>,
        database_partitions: &[Snapshot], storage: &Arc<BufferPool>, accelerate: bool, order: Option<IidOrder>) -> Result<Self> {
        Self::over_with_queries(scan, execution, pushdowns, database_partitions, storage, accelerate, order, &SearchQueries::default(), None).await
    }

    async fn over_with_queries(mut scan: Scan, execution: Execution, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>, database_partitions: &[Snapshot], storage: &Arc<BufferPool>, accelerate: bool, order: Option<IidOrder>, search_queries: &SearchQueries, restricted_iids: Option<&crate::CandidateIids>) -> Result<Self> {
        if !execution.context.catalog.permits_table(&scan.table.name) {
            return Err(Anomaly::incorrect("rtdb.owned/scan-scope", "The relation is outside the admitted scan scope"));
        }
        let archive_history = projects_archive_history(&scan);
        if archive_history && (!execution.context.catalog.admits_archive_history(&scan.table.name)
            || !matches!(&scan.system_time, TemporalFilter::All)
            || !scan.matches.is_empty() || scan.top_k.is_some() || scan.ordered_top_k.is_some())
        {
            return Err(invalid("archive-history", "An admitted archive relation with FOR ALL SYSTEM_TIME is required"));
        }
        let virtual_metadata = retained_metadata::is_projected(&scan)
            && database_partitions.iter().any(retained_metadata::has_sidecar);
        let pushdowns = if virtual_metadata || archive_history { None } else { pushdowns };
        let accelerate = accelerate && !virtual_metadata && !archive_history;
        if virtual_metadata {
            // The filter above this scan evaluates the projected values.
            let metadata = scan.columns.iter().find(|field| field.name == "user_metadata").map(|field| field.id);
            scan.predicates.retain(|predicate| metadata.is_none_or(|id| !predicate.referenced_columns().contains(&id)));
        }
        let fixed_iids = scan.iids.is_some() || restricted_iids.is_some();
        let accelerate = accelerate && !execution.context.catalog.uses_owned_event_cut();
        if !fixed_iids { scan.iids = id_selection(&scan, execution.parameters.evaluator()); }
        let interval = if !stores_uuid_ids(&scan.table.name.table, database_partitions) { None } else {
            match id_interval(&scan, execution.parameters.evaluator()) {
                Ok(interval) => interval,
                Err(Unsatisfiable) => { scan.iids = Some(Default::default()); None }
            }
        };
        let bytes = scan.table.name.database.len().checked_add(scan.table.name.table.schema.len()).and_then(|bytes| bytes.checked_add(scan.table.name.table.table.len())).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("scan-size", "Scan labels exceed allocation range"))?;
        let reservation = execution.context.memory.reserve(bytes)?;
        let canonical_entity_iids = scan.iids.is_none() && restricted_iids.is_none()
            && pushdowns.as_ref().is_some_and(|filters| filters.needs_entity_iid_proof(&scan.columns))
            && database_partitions.iter().all(|snapshot| snapshot.has_canonical_entity_iids(&scan.table.name.table));
        if let (Some(analysis), Some(pushdowns)) = (&execution.analysis, &pushdowns) {
            analysis.set_pushdowns(pushdowns, &scan.columns, canonical_entity_iids, &execution.context.memory)?;
        }
        let name = scan.table.name.database.clone();
        let source = format!("{}.{}", scan.table.name.table.schema, scan.table.name.table.table);
        let constants = ExecutionConstants { execution: &execution, search_queries };
        let mut queries = Vec::with_capacity(scan.matches.len());
        for evaluated in &scan.matches {
            queries.push(constants.search_query(&evaluated.query, evaluated.analyzer).await?);
        }
        let slots: Vec<Option<usize>> = scan.columns.iter().map(|field| scan.matches.iter().position(|evaluated| evaluated.id == field.id)).collect();
        let ranking = if let Some(top) = scan.top_k.as_ref()
            && let Some(output) = top.score
            && let Some(source) = scan.columns.iter().find(|field| field.id == top.column)
        {
            let scorer = constants.search_query(&top.query, top.analyzer).await?.map(|query| Arc::clone(query.scorer()));
            Some(RankProjection { output, source: source.name.clone(), scorer, verified: None, live: None })
        } else { None };
        let fragmenter = if virtual_metadata || archive_history { None } else { execution.context.catalog.fragmenter.clone() };
        use futures::TryStreamExt;
        let _verifier_charge = execution.context.memory.reserve(database_partitions.len().saturating_mul(size_of::<ScanVerifier<'_>>()))?;
        let ranked_projection_memory = (accelerate && scan.top_k.is_some()).then(|| {
            let memory = &execution.context.memory;
            memory.scope_with_limit((memory.limit_bytes() / 32).min(4 * 1024 * 1024))
        });
        let verifiers: Vec<_> = database_partitions.iter().map(|partition| ScanVerifier {
            scan: &scan, execution: &execution, constants: &constants, pushdowns: pushdowns.clone(), partition, storage, canonical_entity_iids, counts: Default::default(),
            ranked_projection_memory: ranked_projection_memory.as_ref(),
        }).collect();
        let query_winners = Box::pin(async {
            let Some(top) = scan.top_k.as_ref() else { return Ok(None); };
            if !accelerate || order.is_some() || execution.context.accelerators.is_empty() { return Ok(None); }
            let Some(snapshot) = database_partitions.first() else { return Ok(None); };
            let context = &execution.context;
            let request = crate::ScanRequest { database: &name, scan: &scan, candidate_iids: None, snapshot, storage,
                bounds: bounds(&scan, snapshot, &execution).await?, memory: &context.memory, current_time: context.current_time,
                cancellation: &context.cancellation, constants: &constants, partitions: database_partitions, verifier: &verifiers[0] };
            let Some(limit) = request.ranked_prefix_len(top).await.and_then(std::num::NonZeroUsize::new) else { return Ok(None); };
            let ranking = crate::QueryRanking::new(limit, database_partitions.len(), &context.memory)?;
            let mut workers = futures::stream::FuturesOrdered::new();
            for (partition, verifier) in verifiers.iter().enumerate() {
                workers.push_back(Box::pin(crate::run_rank_partition(&ranking, partition, async move {
                    let snapshot = verifier.partition;
                    let bounds = bounds(request.scan, snapshot, verifier.execution).await?;
                    if !bounds.valid_time.is_point() || !bounds.system_time.is_point() { return Ok(None); }
                    let request = crate::ScanRequest { snapshot, bounds, verifier, ..request };
                    for accelerator in context.accelerators.iter() {
                        if let Some(source) = accelerator.rank_source(request).await? { return Ok(Some(source)); }
                    }
                    Ok(None)
                }, &context.cancellation)));
            }
            while let Some(source) = workers.try_next().await? {
                if source.is_none() { return Ok(None); }
            }
            drop(workers);
            Ok::<_, Anomaly>(ranking.winners())
        }).await;
        // The search accelerators are RTDB's indexes over the scan XTDB runs; one the budget cannot afford leaves the query to that scan.
        let mut winners = match query_winners {
            Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => None,
            result => result?,
        };
        let mut parts = VecDeque::new();
        let mut verified_counts = ScanCounts::default();
        for (partition, verifier) in verifiers.iter().enumerate() {
            let snapshot = verifier.partition;
            let winners = winners.as_mut().map(|winners| winners.take_partition(partition));
            let ranked_here = winners.is_some();
            let (scan, execution, pushdowns, ranking, name, queries, slots, fragmenter, constants) =
                (&scan, &execution, &pushdowns, &ranking, &name, &queries, &slots, &fragmenter, &constants);
            let (partition, counts) = async move {
                let mut parts = VecDeque::new();
                let bounds = bounds(scan, snapshot, execution).await?;
                let mut ranking = ranking.clone();
                // An unsatisfiable SQL interval leaves an explicit empty scan.
                let mut candidate_iids = restricted_iids.filter(|_| scan.iids.is_none()).cloned();
                let mut verified = None;
                if let Some(winners) = winners {
                    let request = crate::ScanRequest { database: name, scan, candidate_iids: None, snapshot, storage, bounds, memory: &execution.context.memory,
                        current_time: execution.context.current_time, cancellation: &execution.context.cancellation,
                        constants, verifier, partitions: database_partitions };
                    match request.ranked_selection(winners).await {
                        Ok(Some(selection)) => {
                            candidate_iids = Some(selection.iids);
                            if let Some(ranking) = &mut ranking { ranking.verified = selection.scores; }
                            else { verified = selection.scores; }
                        }
                        Ok(None) => {}
                        // An unaffordable ranked selection leaves the query to the plain scan, as above.
                        Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => {}
                        Err(error) => return Err(error),
                    }
                }
                if accelerate && order.is_none() && interval.is_none() && pushdowns.is_none()
                    && scan.top_k.is_none() && scan.ordered_top_k.is_none() && scan.matches.is_empty()
                    && bounds.valid_time.is_point() && bounds.system_time.is_point()
                    && let Some(iids) = scan.iids.as_ref().filter(|iids| (1..=128).contains(&iids.len())) {
                    if let Some(batch) = verifier.point_batch(scan, bounds, database_partitions, iids).await? {
                        if let Some(batch) = batch { parts.push_back(Part::Resolved(Some(batch))); }
                        return Ok((parts, *verifier.counts.lock()));
                    }
                }
                for accelerator in execution.context.accelerators.iter().filter(|_| accelerate && !fixed_iids && !ranked_here) {
                    let legacy = legacy_candidate_scan(scan, candidate_iids.as_ref(), accelerator.as_ref());
                    let request = crate::ScanRequest { database: name, scan: legacy.as_ref().unwrap_or(scan),
                        candidate_iids: candidate_iids.as_ref(), snapshot, storage, bounds, memory: &execution.context.memory,
                        current_time: execution.context.current_time, cancellation: &execution.context.cancellation, constants,
                        verifier, partitions: database_partitions };
                    let selection = {
                        let mut selection = accelerator.select_candidates(&request);
                        std::future::poll_fn(|context| {
                            stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || selection.as_mut().poll(context))
                        }).await?
                    };
                    if let Some(crate::ScanSelection { iids: mut candidates, scores }) = selection {
                        if pushdowns.is_some() || scan.iids.is_some() || candidate_iids.is_some() {
                            candidates = match candidates.retain(|iid| {
                                pushdowns.as_ref().is_none_or(|filters| filters.iid_matches(&scan.columns, iid, canonical_entity_iids))
                                    && candidate_iids.as_ref().map_or_else(
                                        || scan.iids.as_ref().is_none_or(|existing| existing.contains(&iid)),
                                        |existing: &crate::CandidateIids| existing.contains(iid))
                            }, &execution.context.memory) {
                                // An unaffordable candidate set leaves the query to the plain scan, as above.
                                Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => continue,
                                result => result?,
                            };
                        }
                        if let Some(ranking) = &mut ranking
                            && scores.is_some()
                        {
                            ranking.verified = scores;
                        }
                        candidate_iids = Some(candidates);
                    }
                }
                let verified_counts = *verifier.counts.lock();
                if ranked_here && candidate_iids.as_ref().is_some_and(crate::CandidateIids::is_empty) {
                    return Ok((parts, verified_counts));
                }
                if bounds.valid_time.is_point() && bounds.system_time.is_point()
                    && let Some(iids) = candidate_iids.as_ref()
                    && let Some(scores) = ranking.as_ref().and_then(|rank| rank.verified.as_ref()).or(verified.as_ref()) {
                    let score = ranking.as_ref().map(|rank| rank.output);
                    match retained_ranked_batch(scan, score, iids, scores, &execution.context.memory) {
                        Ok(Some(batch)) => {
                            parts.push_back(Part::Resolved(Some(batch)));
                            return Ok::<_, Anomaly>((parts, verified_counts));
                        }
                        Ok(None) => {}
                        // Unaffordable ranked rows leave the query to the plain scan, as above.
                        Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => {
                            if let Some(rank) = &mut ranking && let Some(scores) = &mut rank.verified { scores.release_rows(); }
                            drop(verified.take());
                        }
                        Err(error) => return Err(error),
                    }
                }
                let mut matches = Vec::with_capacity(scan.matches.len());
                let request = crate::ScanRequest { database: name, scan, candidate_iids: candidate_iids.as_ref(), snapshot, storage, bounds,
                    memory: &execution.context.memory, current_time: execution.context.current_time,
                    cancellation: &execution.context.cancellation, constants, verifier, partitions: database_partitions };
                let live_terms = |column: &str, analyzer| execution.context.accelerators.iter().filter(|_| accelerate)
                    .find_map(|accelerator| accelerator.live_search_terms(&request, column, analyzer));
                let ranked_analyzer = ranking.as_ref().and_then(|ranking| ranking.scorer.as_ref()).map(|scorer| scorer.query().analyzer());
                if let Some(ranking) = &mut ranking && let Some(analyzer) = ranked_analyzer { ranking.live = live_terms(&ranking.source, analyzer); }
                for (evaluated, query) in scan.matches.iter().zip(queries) {
                    let mut answers = None;
                    let mut live = None;
                    if let Some(query) = query {
                        live = ranking.as_ref().filter(|ranking| ranking.source == evaluated.column && ranked_analyzer == Some(evaluated.analyzer))
                            .and_then(|ranking| ranking.live.clone())
                            .or_else(|| live_terms(&evaluated.column, evaluated.analyzer));
                        for accelerator in execution.context.accelerators.iter().filter(|_| accelerate && !ranked_here) {
                            let legacy = legacy_candidate_scan(scan, candidate_iids.as_ref(), accelerator.as_ref());
                            let request = crate::ScanRequest { database: name, scan: legacy.as_ref().unwrap_or(scan),
                                candidate_iids: candidate_iids.as_ref(), snapshot, storage, bounds, memory: &execution.context.memory,
                                current_time: execution.context.current_time, cancellation: &execution.context.cancellation, constants,
                                verifier, partitions: database_partitions };
                            if let Some(found) = accelerator.match_answers_prepared(&request, &evaluated.column, query).await? {
                                answers = Some(found);
                                break;
                            }
                        }
                    }
                    matches.push(ScanMatch { query: query.as_deref().map(PreparedSearchQuery::parsed), files: answers, live });
                }
                let mut local_scan = scan.clone();
                if candidate_iids.is_some() { local_scan.iids = None; }
                let partition = |confined, matches| PartitionScan {
                    snapshot: snapshot.clone(), storage: Arc::clone(storage), matches, ranking: ranking.clone(), candidate_iids: candidate_iids.clone(), metadata_time: None,
                    slots: slots.clone(), scan: local_scan.clone(), execution: execution.clone(), bounds,
                    initialized: false, files: FileDescriptors::default(), pages: PageDescriptors::default(),
                    tasks: None, loaded: BTreeMap::new(), output: VecDeque::new(),
                    output_reservation: None, counts: ScanCounts::default(), pushdowns: pushdowns.clone(),
                    canonical_entity_iids, confined, order, page_test: None,
                };
                // A scan narrowed to named IIDs reads a few pages wherever they are; only a whole scan of a single-partition database is split.
                let split = match (fragmenter, snapshot.tx_basis()) {
                    (Some(fragmenter), Some(basis)) if order.is_none() && candidate_iids.is_none() && scan.iids.is_none() && scan.matches.is_empty() && ranking.is_none() && database_partitions.len() == 1 && !snapshot.has_overlays() =>
                        fragmenter.depth(name, &scan.table.name.table, persisted_bytes(snapshot)).map(|depth| (depth, basis)),
                    _ => None,
                };
                match split {
                    Some((depth, basis)) => for prefix in depth.prefixes() {
                        // A prefix holding none of the interval's IIDs has no rows to read.
                        let Ok(confined) = intersect(Some(prefix.range()), interval) else { continue };
                        let fallback = partition(confined, Vec::new());
                        let residence = execution.context.catalog.fragment_residence.clone();
                        let request = Arc::new(ScanFragment::of(scan, prefix, basis, residence, bounds, &execution.context.timezone));
                        parts.push_back(Part::Fragment(Fragment { request, fallback, state: FragmentState::Pending }));
                    },
                    None => parts.push_back(Part::Local(partition(interval, matches))),
                }
                Ok::<_, Anomaly>((parts, verified_counts))
            }.await?;
            parts.extend(partition);
            verified_counts.add(counts);
        }
        Ok(Self { parts, fragmenter, completed: verified_counts, database: name, source, memory: Arc::clone(&execution.context.memory), _reservation: reservation })
    }
}

impl PartitionScan {
    #[allow(clippy::too_many_arguments)]
    fn new(snapshot: Snapshot, storage: Arc<BufferPool>, mut scan: Scan, execution: Execution, bounds: TemporalBounds, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>, canonical_entity_iids: bool, fragment: Option<(Iid, Iid)>) -> Self {
        let slots = vec![None; scan.columns.len()];
        let interval = if stores_uuid_ids(&scan.table.name.table, std::slice::from_ref(&snapshot)) { id_interval(&scan, execution.parameters.evaluator()) } else { Ok(None) };
        let confined = interval.and_then(|interval| intersect(fragment, interval)).unwrap_or_else(|Unsatisfiable| {
            scan.iids = Some(Default::default());
            fragment
        });
        Self { snapshot, storage, scan, execution, bounds, matches: Vec::new(), ranking: None, candidate_iids: None, slots, metadata_time: None, initialized: false, files: FileDescriptors::default(), pages: PageDescriptors::default(), tasks: None, loaded: BTreeMap::new(),
            output: VecDeque::new(), output_reservation: None, counts: ScanCounts::default(), pushdowns, canonical_entity_iids, confined, order: None, page_test: None }
    }
    /// The scan another node asked this one for: its fragment's rows within the bounds that node evaluated.
    pub(crate) fn fragment(snapshot: Snapshot, storage: Arc<BufferPool>, fragment: &ScanFragment, execution: Execution) -> Self {
        Self::new(snapshot, storage, fragment.scan.clone(), execution, fragment.bounds, None, false, Some(fragment.prefix.range()))
    }
}

impl ScanCursor {
    pub(crate) async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        // Every fragment is dispatched at the first pull, so the nodes they belong to read them side by side.
        if let Some(fragmenter) = self.fragmenter.take() {
            for part in &mut self.parts { if let Part::Fragment(fragment) = part { fragment.dispatch(&fragmenter, &self.memory); } }
        }
        while let Some(part) = self.parts.front_mut() {
            let batch = match part { Part::Local(partition) => partition.next_batch().await?, Part::Fragment(fragment) => fragment.next_batch(&self.memory).await?,
                Part::Resolved(batch) => batch.take() };
            if let Some(batch) = batch { return Ok(Some(batch)); }
            self.completed.add(part.counts());
            self.parts.pop_front();
        }
        Ok(None)
    }
}

#[cfg(test)]
impl ScanCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.next_batch().await?.map(|batch| batch.rows(&self.memory)).transpose()
    }

    fn front_partition(&self) -> Option<&PartitionScan> { self.parts.front().and_then(|part| match part { Part::Local(partition) => Some(partition), Part::Fragment(fragment) => Some(&fragment.fallback), Part::Resolved(_) => None }) }
    fn front_partition_mut(&mut self) -> Option<&mut PartitionScan> { self.parts.front_mut().and_then(|part| match part { Part::Local(partition) => Some(partition), Part::Fragment(fragment) => Some(&mut fragment.fallback), Part::Resolved(_) => None }) }
}

impl Part {
    fn counts(&self) -> ScanCounts { match self { Self::Local(partition) => partition.counts, Self::Fragment(fragment) => fragment.fallback.counts, Self::Resolved(_) => ScanCounts::default() } }
}

fn legacy_candidate_scan(scan: &Scan, candidates: Option<&crate::CandidateIids>, accelerator: &dyn crate::ScanAccelerator) -> Option<Scan> {
    let candidates = candidates.filter(|_| !accelerator.accepts_arrow_iids())?;
    Some(Scan { iids: Some(candidates.iter().collect()), ..scan.clone() })
}

fn retained_ranked_batch(scan: &Scan, score: Option<rtdb_plan::ColumnId>, iids: &crate::CandidateIids,
    scores: &crate::CandidateScores, memory: &Arc<MemoryBudget>) -> Result<Option<crate::columnar::ColumnBatch>> {
    if iids.is_empty() { return Ok(None); }
    let position = match score {
        Some(score) => {
            let Some(position) = scan.columns.iter().position(|field| field.id == score) else { return Ok(None); };
            Some(position)
        }
        None => None,
    };
    let Some(rows) = iids.iter().map(|iid| scores.row(iid).zip(scores.score(iid))).collect::<Option<Vec<_>>>() else { return Ok(None); };
    let slots = (0..rows.len()).zip(rows.iter().map(|(row, _)| row.row)).collect::<Vec<_>>();
    let sources: Vec<_> = rows.iter().map(|(row, _)| Arc::clone(&row.batch)).collect();
    let columns: Vec<_> = (0..scan.columns.len() - usize::from(position.is_some())).collect();
    let mut output = crate::columnar::ColumnBatch::take_rows(&sources, &slots, &columns, memory)?;
    let Some(position) = position else { return Ok(Some(output)); };
    let reservation = memory.reserve(256 + rows.len() * 8)?;
    output.append_column(crate::columnar::Column::Arrow {
        array: Arc::new(arrow_array::Float64Array::from_iter_values(rows.into_iter().map(|(_, score)| score))),
        field: Arc::new(arrow_schema::Field::new("search_score", DataType::Float64, true)),
        logical: Some(rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::F64).maybe()),
    }, reservation, memory)?;
    output.columns[position..].rotate_right(1);
    Ok(Some(output))
}

async fn ordered(scan: &Scan, execution: &Execution, window: std::ops::Range<usize>, projection: &[usize]) -> Result<Option<crate::OrderedScan>> {
    if scan.ordered_top_k.is_none() || scan.iids.is_some() || !scan.matches.is_empty() || retained_metadata::is_projected(scan) {
        return Ok(None);
    }
    let context = &execution.context;
    let database = context.catalog.database(&scan.table.name.database)?;
    let [snapshot] = database.partitions.as_slice() else { return Ok(None); };
    let storage = &database.storage;
    let search_queries = SearchQueries::default();
    let constants = &ExecutionConstants { execution, search_queries: &search_queries };
    let bounds = bounds(scan, snapshot, execution).await?;
    let verifier = ScanVerifier { scan, execution, constants, pushdowns: None, partition: snapshot, storage, canonical_entity_iids: false,
        counts: Default::default(), ranked_projection_memory: None };
    let request = crate::ScanRequest {
        database: &scan.table.name.database, scan, snapshot, storage, bounds, memory: &context.memory,
        current_time: context.current_time, cancellation: &context.cancellation,
        candidate_iids: None, constants,
        verifier: &verifier, partitions: &database.partitions,
    };
    for accelerator in context.accelerators.iter() {
        if let Some(output) = accelerator.ordered(&request, window.clone(), projection).await? {
            return Ok(Some(output));
        }
    }
    Ok(None)
}

/// The scan's rows in the order its `ORDER BY _id` asks for, where every `_id` of the table is a UUID: a UUID is its
/// own IID, so the rows are read in IID order and whoever takes them stops the read by taking no more. The scan's
/// predicates and the residual predicates annotated on it are applied here, as an ordered index applies them, and
/// the rows from `offset` up to `limit` leave as the columns at `positions`.
async fn iid_ordered_input(scan: &Scan, execution: &Execution, limit: usize, offset: usize, positions: &[usize]) -> Result<Option<crate::columnar::BoxBatchCursor>> {
    let Some(top) = &scan.ordered_top_k else { return Ok(None) };
    let ([key], false, true) = (top.order.as_slice(), top.distinct, scan.matches.is_empty()) else { return Ok(None) };
    let Some(id) = scan.columns.iter().find(|field| field.name == "_id") else { return Ok(None) };
    if id.data_type != rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Uuid) || !matches!(&key.expr, Expr::Column { id: column, .. } if *column == id.id) { return Ok(None); }
    let table = &scan.table.name.table;
    if crate::inspection::is_inspection(table) || crate::system_schema::definition(table).is_some() { return Ok(None); }
    let database = execution.context.catalog.database(&scan.table.name.database)?;
    // Partitions are read one after another, which orders nothing across them.
    if database.partitions.len() != 1 || !stores_uuid_ids(table, &database.partitions) { return Ok(None); }
    let order = if key.descending { IidOrder::Descending } else { IidOrder::Ascending };
    let schema = Schema::new(scan.columns.clone())?;
    let predicate = scan.predicates.iter().chain(&top.residuals).cloned().reduce(|left, right| Expr::binary(rtdb_plan::BinaryOp::And, left, right));
    let mut unordered = scan.clone();
    unordered.ordered_top_k = None;
    unordered.top_k = None;
    let rows: crate::columnar::BoxBatchCursor = Box::new(ScanCursor::over(unordered, execution.clone(), None, &database.partitions, &database.storage, true, Some(order)).await?);
    let rows = match predicate {
        Some(predicate) => Box::new(crate::columnar::BatchFilter::new(rows, schema, predicate, execution.clone())) as crate::columnar::BoxBatchCursor,
        None => rows,
    };
    let window = Box::new(OrderedWindow { input: Some(rows), counts: None, skip: offset, remaining: limit.saturating_sub(offset), memory: Arc::clone(&execution.context.memory) });
    let all = positions.len() == scan.columns.len() && positions.iter().enumerate().all(|(index, position)| index == *position);
    Ok(Some(if all { window } else {
        Box::new(crate::columnar::BatchProject::new(window, positions.to_vec(), Arc::clone(&execution.context.memory)))
    }))
}

/// The rows of an ordered input after the first `skip`, up to `remaining` of them; the input is dropped, and so read no further, once they have left.
struct OrderedWindow { input: Option<crate::columnar::BoxBatchCursor>, counts: Option<ScanCounts>, skip: usize, remaining: usize, memory: Arc<MemoryBudget> }

#[async_trait]
impl crate::columnar::BatchCursor for OrderedWindow {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        loop {
            if self.remaining == 0 { self.close(); }
            let Some(input) = &mut self.input else { return Ok(None) };
            let Some(batch) = input.next_batch().await? else { self.close(); return Ok(None) };
            if self.skip >= batch.len { self.skip -= batch.len; continue; }
            let first = std::mem::take(&mut self.skip);
            let taken = (batch.len - first).min(self.remaining);
            self.remaining -= taken;
            if first == 0 && taken == batch.len { return Ok(Some(batch)); }
            let mask: Vec<bool> = (0..batch.len).map(|row| row >= first && row < first + taken).collect();
            return batch.select(&mask, &self.memory).map(Some);
        }
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { if let Some(input) = &self.input { input.trace_attributes(span); } }
    fn scan_counts(&self) -> Option<ScanCounts> { self.input.as_ref().map_or(self.counts, |input| input.scan_counts()) }
}
impl OrderedWindow {
    /// Drops the input, keeping what it counted.
    fn close(&mut self) { if let Some(input) = self.input.take() { self.counts = input.scan_counts(); } }
}

pub(crate) async fn ordered_input(scan: &Scan, execution: &Execution, limit: usize, offset: usize, positions: &[usize]) -> Result<Option<crate::columnar::BoxBatchCursor>> {
    if !execution.context.catalog.permits_table(&scan.table.name) {
        return Err(Anomaly::incorrect("rtdb.owned/scan-scope", "The relation is outside the admitted scan scope"));
    }
    if projects_archive_history(scan) { return Ok(None); }
    let Some(output) = ordered(scan, execution, offset..limit, positions).await? else { return iid_ordered_input(scan, execution, limit, offset, positions).await; };
    Ok(Some(match output {
        crate::OrderedScan::Covered(batches) => {
            Box::new(CoveredBatches { batches, context: Arc::clone(&execution.context) })
        }
        crate::OrderedScan::Payload { candidates, verify } => {
            let mut payload = scan.clone();
            let predicate = verify.then(|| {
                scan.predicates.iter().chain(scan.ordered_top_k.iter().flat_map(|top| &top.residuals))
                    .cloned().reduce(|left, right| Expr::binary(rtdb_plan::BinaryOp::And, left, right))
            }).flatten();
            let mut needed = predicate.as_ref().map_or_else(BTreeSet::new, Expr::referenced_columns);
            let output = positions.iter().map(|&position| scan.columns.get(position)
                .map(|field| field.id).ok_or_else(|| invalid("ordered-column", "Ordered projection is outside the scan schema")))
                .collect::<Result<Vec<_>>>()?;
            needed.extend(output.iter().copied());
            payload.columns.retain(|field| needed.contains(&field.id));
            let iid_at = include_iid(&mut payload)?;
            let schema = Schema::new(payload.columns.clone())?;
            let projection = output.into_iter().map(|id| schema.position(id)
                .ok_or_else(|| invalid("ordered-column", "Ordered output column is missing")))
                .collect::<Result<Vec<_>>>()?;
            let memory = execution.context.memory.reserve(4096 + payload.columns.len().saturating_mul(512))?;
            payload.ordered_top_k = None;
            payload.top_k = None;
            payload.predicates.clear();
            Box::new(OrderedPayload {
                scan: payload, schema, execution: execution.clone(), candidates: Some(candidates), predicate, verify,
                projection, iid_at, skip: offset, remaining: limit.saturating_sub(offset), _memory: memory,
                source: None, found: Default::default(), join_memory: None,
            })
        }
    }))
}

struct CoveredBatches {
    batches: Box<dyn crate::OrderedBatches>,
    context: Arc<crate::QueryContext>,
}

#[async_trait]
impl crate::columnar::BatchCursor for CoveredBatches {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        self.context.check_cancelled()?;
        self.batches.next_batch().await?.map(crate::columnar::ColumnBatch::from_owned).transpose()
    }

    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        span.attribute("scan_ordered_seek", rtdb_plan::TraceValue::Boolean(true));
        span.attribute("scan_ordered_covered", rtdb_plan::TraceValue::Boolean(true));
        self.batches.trace_attributes(span);
    }
}

fn include_iid(scan: &mut Scan) -> Result<usize> {
    if let Some(position) = scan.columns.iter().position(|field| field.name == "_iid") {
        return Ok(position);
    }
    let position = scan.columns.len();
    let id = scan.columns.iter().map(|field| field.id.0).max().unwrap_or(0)
        .checked_add(1).ok_or_else(|| invalid("scan-column", "Column identifier space is exhausted"))?;
    scan.columns.push(rtdb_plan::Field { id: rtdb_plan::ColumnId(id), name: "_iid".into(), qualifier: None,
        qualified_only: false, unnamed: true, data_type: rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::FixedSizeBinary(16)) });
    Ok(position)
}

struct OrderedPayload {
    scan: Scan,
    source: Option<Box<PartitionScan>>,
    found: std::collections::HashMap<Iid, Option<(usize, usize)>>,
    join_memory: Option<Arc<MemoryReservation>>,
    execution: Execution,
    schema: Schema,
    candidates: Option<Box<dyn crate::OrderedCandidates>>,
    predicate: Option<Expr>,
    verify: bool,
    projection: Vec<usize>,
    iid_at: usize,
    skip: usize,
    remaining: usize,
    _memory: Arc<MemoryReservation>,
}

#[async_trait]
impl crate::columnar::BatchCursor for OrderedPayload {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        use crate::columnar::{Column, ColumnBatch, predicate_selection};

        let memory = &self.execution.context.memory;
        while self.remaining != 0 {
            self.execution.context.check_cancelled()?;
            let Some(candidates) = self.candidates.as_mut() else { return Ok(None) };
            let Some(ids) = candidates.next_iids(self.remaining.saturating_add(self.skip)).await? else {
                self.candidates = None;
                self.source = None;
                self.found = Default::default();
                self.join_memory = None;
                return Ok(None);
            };
            let bytes = ids.len().saturating_mul(256);
            match self.join_memory.as_mut() {
                Some(charge) if charge.bytes() < bytes =>
                    Arc::get_mut(charge).expect("the payload cursor owns its join reservation").grow_to(bytes)?,
                None => self.join_memory = Some(memory.reserve(bytes)?),
                _ => {}
            }
            let database = self.execution.context.catalog.database(&self.scan.table.name.database)?;
            let [snapshot] = database.partitions.as_slice() else {
                unreachable!("ordered acceleration requires one partition");
            };
            let point_scan = self.execution.context.catalog.owned_position_mask.is_none().then(|| {
                let mut scan = self.scan.clone();
                let mut keys = BTreeSet::new();
                for iid in ids.iter() { keys.insert(iid); }
                scan.iids = Some(keys);
                scan
            });
            let mut point = None;
            if let Some(scan) = &point_scan
                && let Some(keys) = scan.iids.as_ref().filter(|keys| keys.len() <= 128)
            {
                let bounds = match &self.source {
                    Some(source) => source.bounds,
                    None => bounds(scan, snapshot, &self.execution).await?,
                };
                if bounds.valid_time.is_point() && bounds.system_time.is_point() {
                    let search_queries = SearchQueries::default();
                    let constants = &ExecutionConstants { execution: &self.execution, search_queries: &search_queries };
                    let verifier = ScanVerifier { scan, execution: &self.execution, constants, pushdowns: None,
                        partition: snapshot, storage: &database.storage, canonical_entity_iids: false,
                        counts: Default::default(), ranked_projection_memory: None };
                    point = verifier.point_batch(scan, bounds, &database.partitions, keys).await?;
                }
            }
            let mut batches = Vec::new();
            self.found.clear();
            let source = if point.is_none() {
                if self.source.is_none() {
                    let read_bounds = bounds(&self.scan, snapshot, &self.execution).await?;
                    self.source = Some(Box::new(PartitionScan::new(snapshot.clone(), Arc::clone(&database.storage),
                        self.scan.clone(), self.execution.clone(), read_bounds, None, false, None)));
                }
                let source = self.source.as_mut().unwrap();
                let membership = match point_scan.and_then(|scan| scan.iids) {
                    Some(keys) => CandidateIids::from_set(keys, memory)?,
                    None => ids.sorted_membership(memory)?,
                };
                source.reset_candidates(membership);
                Some(source)
            } else {
                drop(point_scan);
                None
            };
            let mut source = source;
            loop {
                let batch = match source.as_mut() {
                    Some(source) => source.next_batch().await?,
                    None => point.take().flatten(),
                };
                let Some(batch) = batch else { break; };
                let Column::Arrow { array, .. } = &batch.columns[self.iid_at] else {
                    return Err(invalid("ordered-row-iid", "Ordered scan returned no IID array"));
                };
                let iids = rtdb_columnar::IidView::new(array.as_ref())?;
                let selection = match &self.predicate {
                    Some(predicate) => Some(predicate_selection(&batch, predicate, &self.schema, &self.execution).await?),
                    None => None,
                };
                for row in 0..batch.len {
                    let iid = iids.get(row)?;
                    let selected = selection.as_ref().is_none_or(|selection| selection.contains(row));
                    let position = selected.then_some((batches.len(), row));
                    if self.found.insert(iid, position).is_some() {
                        return Err(invalid("ordered-row-duplicate", "Ordered point scan returned more than one row for an IID"));
                    }
                }
                batches.push(batch);
            }
            if let Some(source) = source { source.candidate_iids = None; }
            let mut order = Vec::with_capacity(ids.len().min(self.remaining));
            for iid in ids.iter() {
                match self.found.remove(&iid) {
                    None if !self.verify => return Err(invalid("ordered-row-missing", "A row in the ordered index is absent at the read basis")),
                    None | Some(None) => continue,
                    Some(Some(_)) if self.skip != 0 => self.skip -= 1,
                    Some(Some(position)) => {
                        order.push(position);
                        if order.len() == self.remaining { break; }
                    }
                }
            }
            drop(ids);
            if !order.is_empty() {
                self.remaining -= order.len();
                let batch = ColumnBatch::take_rows(&batches, &order, &self.projection, memory);
                if self.remaining == 0 {
                    drop(order);
                    drop(batches);
                    self.candidates = None;
                    self.source = None;
                    self.found = Default::default();
                    self.join_memory = None;
                }
                return batch.map(Some);
            }
        }
        Ok(None)
    }

    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        span.attribute("scan_ordered_seek", rtdb_plan::TraceValue::Boolean(true));
        span.attribute("scan_ordered_payload_batches", rtdb_plan::TraceValue::Boolean(true));
    }
}

/// Runs the scan itself over one partition, restricted to a batch of IIDs, its predicates applied as the scan's filter applies them; narrowing is skipped because the IIDs are fixed.
struct ScanVerifier<'a> {
    scan: &'a Scan,
    execution: &'a Execution,
    constants: &'a ExecutionConstants<'a>,
    pushdowns: Option<Arc<crate::pushdown::Pushdowns>>,
    partition: &'a Snapshot,
    storage: &'a Arc<BufferPool>,
    canonical_entity_iids: bool,
    counts: parking_lot::Mutex<ScanCounts>,
    ranked_projection_memory: Option<&'a Arc<MemoryBudget>>,
}

struct RankedGather<'a> {
    columns: &'a [rtdb_plan::ColumnId],
    batches: &'a [crate::columnar::ColumnBatch],
    rows: &'a [(usize, usize)],
}

impl RankedGather<'_> {
    fn position(&self, id: rtdb_plan::ColumnId) -> Option<usize> {
        self.columns.iter().position(|column| *column == id)
    }
}

fn ranked_integer_id(column: &crate::columnar::Column, row: usize) -> Result<Option<i64>> {
    match column.arrow_cell(row) {
        Some(Some((array, field, logical, offset))) => {
            match rtdb_columnar::read_execution_entity_id_as(logical, field, array, offset) {
                Ok(Some(rtdb_columnar::EntityIdRef::Integer(id))) => Ok(Some(id)),
                Ok(_) => Ok(None),
                Err(error) if error.code == "xtdb/invalid-id" => Ok(None),
                Err(error) => Err(error),
            }
        }
        Some(None) => Ok(None),
        None => Ok(match column.value(row)?.entity_id() {
            Ok(EntityId::Integer(id)) => Some(id),
            _ => None,
        }),
    }
}


impl ScanVerifier<'_> {
    async fn point_batch(&self, scan: &Scan, bounds: TemporalBounds, partitions: &[Snapshot],
        iids: &BTreeSet<Iid>) -> Result<Option<Option<crate::columnar::ColumnBatch>>> {
        let execution = self.execution;
        let request = crate::ScanRequest { database: &scan.table.name.database, scan, candidate_iids: None, snapshot: self.partition, storage: self.storage,
            bounds, memory: &execution.context.memory, current_time: execution.context.current_time,
            cancellation: &execution.context.cancellation, constants: self.constants,
            verifier: self, partitions };
        let mut resolved = crate::table_point_rows(&request, iids).await?;
        if resolved.is_none() {
            for accelerator in execution.context.accelerators.iter() {
                resolved = accelerator.point_rows(&request, iids).await?;
                if resolved.is_some() { break; }
            }
        }
        let Some(rows) = resolved else { return Ok(None); };
        if rows.is_empty() { return Ok(Some(None)); }
        let mut batch = self.gather_search_rows(scan, &rows, None).await?;
        if let Some(predicate) = scan.predicates.iter().cloned().reduce(|a, b| Expr::binary(rtdb_plan::BinaryOp::And, a, b)) {
            let schema = Schema::new(scan.columns.clone())?;
            let selection = crate::columnar::predicate_selection(&batch, &predicate, &schema, execution).await?;
            batch = batch.select_predicate(&selection, &execution.context.memory)?;
        }
        Ok(Some((batch.len != 0).then_some(batch)))
    }
}

#[async_trait]
impl crate::CandidateVerifier for ScanVerifier<'_> {
    async fn score_visible(&self, mut iids: BTreeSet<Iid>, column: rtdb_plan::ColumnId, ascending_id: Option<rtdb_plan::ColumnId>, scorer: &rtdb_text::Scorer) -> Result<crate::CandidateScores> {
        iids.retain(|&iid| self.within_pushdowns(iid));
        self.score_iids(iids, column, ascending_id, scorer).await
    }

    async fn verify_ranked(&self, iids: &BTreeSet<Iid>, rows: Option<Vec<crate::ResolvedSearchRow>>, column: rtdb_plan::ColumnId,
        ascending_id: Option<rtdb_plan::ColumnId>, scorer: &rtdb_text::Scorer) -> Result<Option<Vec<crate::VerifiedRankRow>>> {
        if self.scan.table.name.table.is_owned_event_data() || self.execution.context.catalog.uses_owned_event_cut() {
            return Ok(None);
        }
        if let Some(rows) = rows {
            match self.verify_ranked_rows(rows, column, ascending_id, scorer).await {
                Err(error) if error.code.starts_with("rtdb.storage/") || error.code == "rtdb.query/search-row" => {}
                result => return result,
            }
        }
        let rows = self.primary_ranked_rows(iids).await?;
        self.verify_ranked_rows(rows, column, ascending_id, scorer).await
    }

    async fn retain_visible(&self, iids: &crate::CandidateIids) -> Result<Option<crate::CandidateIids>> {
        let memory = &self.execution.context.memory;
        let iids = iids.clone().retain(|iid| self.within_pushdowns(iid), memory)?;
        if iids.is_empty() { return Ok(Some(iids)); }
        let mut scan = self.scan.clone();
        let required: BTreeSet<_> = scan.predicates.iter().flat_map(Expr::referenced_columns).collect();
        scan.columns.retain(|field| required.contains(&field.id));
        scan.matches.retain(|evaluated| required.contains(&evaluated.id));
        scan.top_k = None;
        scan.ordered_top_k = None;
        let iid_at = include_iid(&mut scan)?;
        let schema = Schema::new(scan.columns.clone())?;
        let predicate = scan.predicates.iter().cloned().reduce(|left, right| Expr::binary(rtdb_plan::BinaryOp::And, left, right));
        scan.iids = None;
        let mut cursor = Box::pin(ScanCursor::over_with_queries(scan, self.execution.clone(), self.pushdowns.clone(),
            std::slice::from_ref(self.partition), self.storage, false, None, self.constants.search_queries, Some(&iids))).await?;
        let _visibility = memory.reserve(256usize.saturating_add(iids.len()))?;
        let mut visible = vec![false; iids.len()];
        while let Some(batch) = cursor.next_batch().await? {
            let crate::columnar::Column::Arrow { array, .. } = &batch.columns[iid_at] else {
                return Err(invalid("verifier-iid", "Verified scan returned no IID array"));
            };
            let resolved = rtdb_columnar::IidView::new(array.as_ref())?;
            let selection = match &predicate {
                Some(predicate) => Some(crate::columnar::predicate_selection(&batch, predicate, &schema, self.execution).await?),
                None => None,
            };
            for row in 0..batch.len {
                if selection.as_ref().is_some_and(|selection| !selection.contains(row)) { continue; }
                let position = iids.position(resolved.get(row)?).ok_or_else(|| invalid("verifier-iid", "Verified scan returned an IID outside its candidate selection"))?;
                visible[position] = true;
            }
        }
        if let Some(counts) = cursor.scan_counts() { self.counts.lock().add(counts); }
        drop(cursor);
        Ok(Some(iids.select_visible(&visible, memory)?))
    }

    async fn gather_ranked(&self, rows: Vec<crate::VerifiedRankRow>) -> Result<BTreeMap<Iid, crate::SnapshotRow>> {
        if rows.is_empty() { return Ok(BTreeMap::new()); }
        let mut scan = self.scan.clone();
        if let Some(score) = scan.top_k.as_ref().and_then(|top| top.score) {
            scan.columns.retain(|field| field.id != score);
        }
        let Some(columns) = rows.iter().find_map(|row| row.projection.as_ref().map(|projection| Arc::clone(&projection.columns))) else {
            let rows = rows.into_iter().map(|winner| winner.row).collect::<Vec<_>>();
            return self.gather_ranked_group(&scan, &rows, None).await;
        };
        let _scratch = self.execution.context.memory.reserve(4096usize.saturating_add(rows.len().saturating_mul(256)))?;
        let reusable = |row: &crate::VerifiedRankRow| {
            row.projection.as_ref().is_some_and(|projection| projection.columns == columns)
        };
        let has_late_fields = scan.columns.iter().any(|field| !columns.contains(&field.id));
        // Gather one projection per primary page, including pages shared by retained and uncopied winners.
        let mut full_pages = BTreeMap::<_, BTreeSet<_>>::new();
        for row in rows.iter().filter(|row| has_late_fields && !reusable(row)) {
            if let Some((trie, page)) = row.row.location.persisted_page() {
                full_pages.entry(trie.clone()).or_default().insert(page);
            }
        }
        let mut full = Vec::new();
        let mut reused = Vec::new();
        let mut batches = Vec::new();
        for winner in rows {
            let uses_full_page = winner.row.location.persisted_page()
                .is_some_and(|(trie, page)| full_pages.get(trie).is_some_and(|pages| pages.contains(&page)));
            if reusable(&winner) && !uses_full_page {
                batches.push(winner.projection.expect("A reusable winner has a projection").batch);
                reused.push(winner.row);
            } else {
                full.push(winner.row);
            }
        }
        let mut output = self.gather_ranked_group(&scan, &full, None).await?;
        let picks = (0..reused.len()).map(|index| (index, 0)).collect::<Vec<_>>();
        let projection = RankedGather { columns: &columns, batches: &batches, rows: &picks };
        output.extend(self.gather_ranked_group(&scan, &reused, Some(&projection)).await?);
        Ok(output)
    }

}

impl ScanVerifier<'_> {
    async fn gather_ranked_group(&self, scan: &Scan, rows: &[crate::ResolvedSearchRow], reuse: Option<&RankedGather<'_>>)
        -> Result<BTreeMap<Iid, crate::SnapshotRow>> {
        if rows.is_empty() { return Ok(BTreeMap::new()); }
        let batch = Arc::new(self.gather_search_rows(scan, rows, reuse).await?);
        Ok(rows.iter().enumerate().map(|(row, resolved)| {
            (resolved.version.iid, crate::SnapshotRow { batch: Arc::clone(&batch), row })
        }).collect())
    }

    async fn primary_ranked_rows(&self, iids: &BTreeSet<Iid>) -> Result<Vec<crate::ResolvedSearchRow>> {
        use crate::{ResolvedSearchRow, SearchRowLocation};

        let bounds = bounds(self.scan, self.partition, self.execution).await?;
        let mut scan = self.scan.clone();
        scan.iids = Some(iids.iter().copied().filter(|&iid| self.within_pushdowns(iid)).collect());
        scan.top_k = None;
        let mut scan = PartitionScan::new(self.partition.clone(), Arc::clone(self.storage), scan, self.execution.clone(),
            bounds, self.pushdowns.clone(), self.canonical_entity_iids, None);
        let mut resolved = Vec::new();
        while let Some(task) = scan.next_resolved_task().await? {
            for (group, rows) in task.groups.iter().enumerate() {
                let source = &scan.pages.entries[task.selected[group]].source;
                for row in rows {
                    let (batch, index) = row.source;
                    let batch = &task.batches[batch as usize];
                    let header = batch.header(index as usize)?;
                    let origin = batch.view().origin(index as usize)?;
                    let location = match source {
                        PageSource::File { trie, page, .. } => SearchRowLocation::Persisted {
                            trie: (**trie).clone(), page: u32::try_from(*page).map_err(|error| invalid("search-row", error))?, row: index,
                        },
                        PageSource::Live { .. } => SearchRowLocation::Live(RowSource { batch: Arc::clone(batch), row: index as usize }),
                    };
                    resolved.push(ResolvedSearchRow { version: header.version(), origin, location,
                        visibility: TemporalBounds { valid_time: row.valid_time, system_time: row.system_time } });
                }
            }
        }
        self.counts.lock().add(scan.counts);
        Ok(resolved)
    }

    fn verification_scan(&self, column: rtdb_plan::ColumnId, ascending_id: Option<rtdb_plan::ColumnId>) -> Scan {
        let mut scan = self.scan.clone();
        let mut required = BTreeSet::from([column]);
        required.extend(ascending_id);
        for predicate in &scan.predicates { required.extend(predicate.referenced_columns()); }
        scan.columns.retain(|field| required.contains(&field.id));
        scan.matches.retain(|evaluated| required.contains(&evaluated.id));
        scan.top_k = None;
        scan.ordered_top_k = None;
        scan
    }

    async fn verify_ranked_rows(&self, mut rows: Vec<crate::ResolvedSearchRow>, column: rtdb_plan::ColumnId,
        ascending_id: Option<rtdb_plan::ColumnId>, scorer: &rtdb_text::Scorer) -> Result<Option<Vec<crate::VerifiedRankRow>>> {
        use crate::rank_cutoff::RankKey;

        rows.retain(|row| self.within_pushdowns(row.version.iid));
        if rows.is_empty() { return Ok(Some(Vec::new())); }
        let mut unique = BTreeSet::new();
        if rows.iter().any(|row| !unique.insert(row.version.iid)) { return Ok(None); }
        let memory = &self.execution.context.memory;
        let scan = self.verification_scan(column, ascending_id);
        let position = scan.columns.iter().position(|field| field.id == column)
            .ok_or_else(|| invalid("verifier-column", "Verified column is not projected by the scan"))?;
        let id = ascending_id.and_then(|id| scan.columns.iter().position(|field| field.id == id));
        let batch = self.gather_search_rows(&scan, &rows, None).await?;
        let reuse = self.ranked_projection_memory.filter(|_| rows.iter().any(|row| row.location.persisted_page().is_some())).and_then(|memory| {
            let score = self.scan.top_k.as_ref().and_then(|top| top.score);
            let (positions, columns): (Vec<_>, Vec<_>) = scan.columns.iter().enumerate()
                .filter(|(_, field)| Some(field.id) != score).map(|(position, field)| (position, field.id)).unzip();
            (!positions.is_empty()).then(|| (memory, positions, Arc::<[_]>::from(columns)))
        });
        let predicate = scan.predicates.iter().cloned().reduce(|left, right| Expr::binary(rtdb_plan::BinaryOp::And, left, right));
        let selection = match predicate {
            Some(predicate) => Some(crate::columnar::predicate_selection(&batch, &predicate, &Schema::new(scan.columns)?, self.execution).await?),
            None => None,
        };
        let mut verified = Vec::new();
        let mut types = crate::columnar::search::TextTypes::default();
        for (position_in_batch, row) in rows.into_iter().enumerate() {
            self.execution.context.check_cancelled_at(position_in_batch)?;
            if selection.as_ref().is_some_and(|selection| !selection.contains(position_in_batch)) { continue; }
            let PutText::Utf8(text) = types.text(&batch.columns[position], position_in_batch)? else { continue; };
            let token_bytes = text.len().saturating_mul(32);
            audience_work::decode_bytes(token_bytes)?;
            let scratch = memory.reserve(256usize.saturating_add(token_bytes))?;
            let score = scorer.bm25_text(text);
            drop(scratch);
            let key = match id {
                Some(id) => {
                    let column = &batch.columns[id];
                    if let Some(id) = ranked_integer_id(column, position_in_batch)? {
                        RankKey::integer(score, Some(id))
                    } else if let Ok(EntityId::Uuid(id)) = column.value(position_in_batch)?.entity_id() {
                        RankKey::uuid(score, id)
                    } else { return Ok(None); }
                }
                None => RankKey::integer(score, None),
            };
            if !score.is_finite() || score < 0.0 { return Ok(None); }
            let projection = reuse.as_ref().and_then(|(memory, positions, columns)| {
                row.location.persisted_page()?;
                let batch = batch.compact_ranked_projection(position_in_batch, positions, memory).ok()??;
                Some(crate::ranking::VerifiedRankProjection { columns: Arc::clone(columns), batch })
            });
            verified.push(crate::VerifiedRankRow { key, row, projection });
        }
        Ok(Some(verified))
    }

    fn within_pushdowns(&self, iid: Iid) -> bool {
        self.pushdowns.as_ref().is_none_or(|pushdowns| pushdowns.iid_matches(&self.scan.columns, iid, self.canonical_entity_iids))
    }

    async fn score_iids(&self, iids: BTreeSet<Iid>, column: rtdb_plan::ColumnId, ascending_id: Option<rtdb_plan::ColumnId>,
        scorer: &rtdb_text::Scorer) -> Result<crate::CandidateScores> {
        let memory = &self.execution.context.memory;
        let mut reservation = memory.reserve(256)?;
        let mut selected = FixedSizeBinaryBuilder::with_capacity(0, 16);
        let mut scores = Float64Builder::with_capacity(0);
        if iids.is_empty() { return Ok(crate::CandidateScores::new(selected.finish(), scores.finish(), reservation)); }
        let mut scan = self.verification_scan(column, ascending_id);
        let iid_at = include_iid(&mut scan)?;
        let position = scan.columns.iter().position(|field| field.id == column)
            .ok_or_else(|| invalid("verifier-column", "Verified column is not projected by the scan"))?;
        let id_position = ascending_id.and_then(|id| scan.columns.iter().position(|field| field.id == id));
        let mut ranked_ids = id_position.map(|_| arrow_array::builder::Int64Builder::new());
        let schema = Schema::new(scan.columns.clone())?;
        let predicate = scan.predicates.iter().cloned().reduce(|left, right| Expr::binary(rtdb_plan::BinaryOp::And, left, right));
        scan.iids = Some(iids);
        let mut cursor = Box::pin(ScanCursor::over_with_queries(scan, self.execution.clone(), self.pushdowns.clone(),
            std::slice::from_ref(self.partition), self.storage, false, None, self.constants.search_queries, None)).await?;
        let mut selected_rows = 0usize;
        while let Some(batch) = cursor.next_batch().await? {
            let crate::columnar::Column::Arrow { array, .. } = &batch.columns[iid_at] else {
                return Err(invalid("verifier-iid", "Verified scan returned no IID array"));
            };
            let iids = rtdb_columnar::IidView::new(array.as_ref())?;
            let selection = match &predicate {
                Some(predicate) => Some(crate::columnar::predicate_selection(&batch, predicate, &schema, self.execution).await?),
                None => None,
            };
            let mut types = crate::columnar::search::TextTypes::default();
            for row in 0..batch.len {
                if selection.as_ref().is_some_and(|selection| !selection.contains(row)) { continue; }
                let PutText::Utf8(text) = types.text(&batch.columns[position], row)? else { continue; };
                let _scratch = memory.reserve(256usize.saturating_add(text.len().saturating_mul(32)))?;
                let score = scorer.bm25_text(text);
                selected_rows += 1;
                let lease = Arc::get_mut(&mut reservation).expect("Verification owns its output reservation");
                lease.grow_to(256usize.saturating_add(selected_rows.saturating_mul(48)))?;
                let iid = iids.get(row)?;
                selected.append_value(iid.0).map_err(|error| invalid("verifier-iid", error))?;
                scores.append_value(score);
                if let Some((position, ids)) = id_position.zip(ranked_ids.as_mut()) {
                    ids.append_option(ranked_integer_id(&batch.columns[position], row)?);
                }
            }
        }
        if let Some(counts) = cursor.scan_counts() { self.counts.lock().add(counts); }
        Ok(crate::CandidateScores::new(selected.finish(), scores.finish(), reservation)
            .with_ids(ranked_ids.map(|mut ids| ids.finish())))
    }

    async fn gather_search_rows(&self, scan: &Scan, rows: &[crate::ResolvedSearchRow], reuse: Option<&RankedGather<'_>>)
        -> Result<crate::columnar::ColumnBatch> {
        use crate::SearchRowLocation;
        use futures::{StreamExt, TryStreamExt};

        let table = &scan.table.name.table;
        let memory = &self.execution.context.memory;
        let _scratch = memory.reserve(4096usize.saturating_add(rows.len().saturating_mul(768)))?;
        let reused_position = |id| reuse.and_then(|reuse| reuse.position(id));
        if let Some(reuse) = reuse
            && let Some(positions) = scan.columns.iter().map(|field| reuse.position(field.id)).collect::<Option<Vec<_>>>() {
            self.execution.context.check_cancelled()?;
            return crate::columnar::ColumnBatch::take_rows(reuse.batches, reuse.rows, &positions, memory);
        }
        let mut inputs = BTreeMap::<_, BTreeSet<u32>>::new();
        for row in rows {
            if let SearchRowLocation::Persisted { trie, page, .. } = &row.location {
                inputs.entry(trie.clone()).or_default().insert(*page);
            }
        }
        let put_columns: Vec<_> = scan.columns.iter().filter(|field| reused_position(field.id).is_none()).map(|field| field.name.clone())
            .chain(scan.matches.iter().filter(|matched| reused_position(matched.id).is_none()).map(|matched| matched.column.clone())).collect();
        let put_columns = put_columns.as_slice();
        let entry = self.partition.table_entry(table);
        let files = futures::stream::iter(inputs).map(|(trie, pages)| async move {
            self.execution.context.check_cancelled()?;
            let entry = entry.ok_or_else(|| invalid("search-row", "Resolved search row has no table entry"))?;
            if !self.partition.trie_catalog().trie(table, &trie).is_some_and(|entry| entry.state == TrieState::Live) {
                return Err(invalid("search-row", "Resolved search row has no live source in this snapshot"));
            }
            let mut file = DataFileReader::new(self.storage, ObjectKey::new(entry.slug.data_key(&trie))?);
            let mut loaded = Vec::with_capacity(pages.len());
            for page in pages {
                let cached = (!memory.is_prepaid()).then(|| file.storage.view::<CachedPage>(&file.key, View::Page(page as usize))).flatten();
                let batch = match cached {
                    Some(cached) => {
                        audience_work::decode_bytes(cached.bound)?;
                        Arc::clone(&cached.batch)
                    }
                    None => {
                        let (batch, lease) = file.decode_event_page(page as usize, memory, DataProjection::PutColumns(put_columns), true).await?;
                        audience_work::scan_items(batch.num_rows())?;
                        IndexBatch::from_owned(OwnedBatch::with_reservation(batch, lease)?)?
                    }
                };
                loaded.push(((trie.clone(), page), batch));
            }
            Ok::<_, Anomaly>(loaded)
        }).buffered(8).try_collect::<Vec<_>>().await?;
        let mut sources = Vec::new();
        let mut located = BTreeMap::new();
        for (key, batch) in files.into_iter().flatten() {
            located.insert(key, sources.len() as u32);
            sources.push(batch);
        }
        let pages_used = located.len();
        let mut resolved = Vec::with_capacity(rows.len());
        for row in rows {
            let (batch, position) = match &row.location {
                SearchRowLocation::Persisted { trie, page, row } => (located[&(trie.clone(), *page)], *row),
                SearchRowLocation::Live(source) => {
                    let batch = sources.len() as u32;
                    sources.push(Arc::clone(&source.batch));
                    (batch, u32::try_from(source.row).map_err(|error| invalid("search-row", error))?)
                }
            };
            let source = &sources[batch as usize];
            let header = source.header(position as usize)?;
            if header.version() != row.version || header.op != EventOp::Put
                || (row.origin.is_some() && source.view().origin(position as usize)? != row.origin) {
                return Err(invalid("search-row", "Resolved search location names a different version"));
            }
            resolved.push(ResolvedRow { source: (batch, position), valid_time: row.visibility.valid_time, system_time: row.visibility.system_time });
        }
        let mut matches = BTreeMap::new();
        let database = &scan.table.name.database;
        let request = crate::ScanRequest { database, scan, candidate_iids: None, snapshot: self.partition, storage: self.storage, bounds: bounds(scan, self.partition, self.execution).await?, memory,
            current_time: self.execution.context.current_time, cancellation: &self.execution.context.cancellation,
            constants: self.constants, verifier: self, partitions: &self.execution.context.catalog.database(database)?.partitions };
        for evaluated in &scan.matches {
            if reused_position(evaluated.id).is_some() { continue; }
            let query = self.constants.search_query(&evaluated.query, evaluated.analyzer).await?;
            let live = if query.is_none() { None } else {
                self.execution.context.accelerators.iter().find_map(|accelerator| accelerator.live_search_terms(&request, &evaluated.column, evaluated.analyzer))
            };
            matches.insert(evaluated.id, (&evaluated.column, query, live));
        }
        let result = crate::columnar::gather_with_funded_columns(&resolved, &sources, &scan.columns, memory, |position| {
            let id = scan.columns[position].id;
            if let Some(reuse) = reuse && let Some(position) = reuse.position(id) {
                let (column, charge) = crate::columnar::gather_column_rows(|index| &reuse.batches[index], reuse.rows, position, memory)?;
                return Ok(Some((column, Some(charge))));
            }
            matches.get(&id).map(|(name, query, live)| {
                resolved_match_column(name, query.as_deref().map(PreparedSearchQuery::query), &resolved, &sources, memory, |_, row, query| {
                    live_match(live.as_deref(), row, &sources, query)
                }).map(|column| (column, None))
            }).transpose()
        })?;
        let mut counts = self.counts.lock();
        counts.pages_used = counts.pages_used.saturating_add(pages_used as u64);
        counts.rows_read = counts.rows_read.saturating_add(resolved.len() as u64);
        Ok(result)
    }
}

#[async_trait]
impl BatchCursor for ScanCursor {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> { ScanCursor::next_batch(self).await }

    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        let mut counts = self.completed; for part in &self.parts { counts.add(part.counts()); }
        span.attribute("scan_db", rtdb_plan::TraceValue::Text(&self.database));
        span.attribute("scan_source", rtdb_plan::TraceValue::Text(&self.source));
        for (name, value) in [("scan_files_pruned", counts.files_pruned), ("scan_files_used", counts.files_used), ("scan_pages_pruned", counts.pages_pruned), ("scan_pages_used", counts.pages_used), ("scan_rows_read", counts.rows_read)] { span.attribute(name, rtdb_plan::TraceValue::Unsigned(value)); }
    }
    fn scan_counts(&self) -> Option<ScanCounts> {
        let mut counts = self.completed; for part in &self.parts { counts.add(part.counts()); }
        Some(counts)
    }
}

fn projects_archive_history(scan: &Scan) -> bool {
    scan.columns.iter().any(|field| matches!(field.name.as_str(), ARCHIVE_TX_FROM | ARCHIVE_TX_TO))
}

impl PartitionScan {
    fn reset_candidates(&mut self, candidates: crate::CandidateIids) {
        debug_assert!(self.loaded.is_empty() && self.output.is_empty());
        self.candidate_iids = Some(candidates);
        self.initialized = false;
        self.files = FileDescriptors::default();
        self.pages = PageDescriptors::default();
        self.tasks = None;
        self.page_test = None;
    }

    fn pruning(&self) -> PruningOptions {
        let iid = match &self.candidate_iids {
            Some(iids) if iids.len() == 1 => iids.iter().next(),
            _ => self.scan.iids.as_ref().filter(|iids| iids.len() == 1).and_then(|iids| iids.first().copied()),
        };
        PruningOptions { bounds: self.bounds, projects_temporal_columns: self.scan.columns.iter().any(|field| matches!(field.name.as_str(), "_valid_from" | "_valid_to" | "_system_from" | "_system_to")),
            clamp_valid_time: self.scan.clamp_valid_time,
            bypass_temporal_pruning: self.execution.context.options.bypass_temporal_pruning
                || self.execution.context.catalog.uses_owned_event_cut() || projects_archive_history(&self.scan),
            iid }
    }
    fn path_matches(&self, lower: Iid, upper: Iid) -> bool {
        if self.confined.is_some_and(|(first, last)| upper < first || last < lower) { return false; }
        if let Some(iids) = &self.candidate_iids { return iids.intersects(lower, upper); }
        match &self.scan.iids {
            Some(iids) => iids.range(lower..=upper).next().is_some(),
            None => self.pushdowns.as_ref().is_none_or(|filters| filters.path_matches(&self.scan.columns, lower, upper, self.canonical_entity_iids)),
        }
    }
    async fn initialize(&mut self) -> Result<()> {
        self.execution.context.check_cancelled()?;
        if self.candidate_iids.as_ref().is_some_and(crate::CandidateIids::is_empty)
            || self.scan.iids.as_ref().is_some_and(BTreeSet::is_empty) || self.bounds.valid_time.is_empty() || self.bounds.system_time.is_empty() {
            self.initialized = true;
            return Ok(());
        }
        let page_test = crate::page_metadata::PageTest::for_scan(&self.scan, &self.execution).await?;
        let table = &self.scan.table.name.table;
        let catalog = self.snapshot.trie_catalog();
        // A trie without rows has no temporal bounds to record, and a candidate without bounds stops the selection below
        // from pruning any other: compaction writes such a current trie whenever every row it merges has a finite recency.
        let candidate = |trie: &CatalogTrie| trie.state == TrieState::Live
            && !trie.metadata.as_ref().is_some_and(|metadata| metadata.row_count == Some(0));
        let current_tries = || catalog.iter_tries(table).filter(|trie| candidate(trie));
        let trie_count = if audience_work::is_bounded() {
            catalog.iter_tries(table).try_fold(0, |count, trie| -> Result<usize> {
                audience_work::scan_items(1)?;
                Ok(count + candidate(trie) as usize)
            })?
        } else {
            current_tries().count()
        };
        let candidate_reservation = if trie_count == 0 { None } else {
            let bytes = trie_count.checked_mul(std::mem::size_of::<&CatalogTrie>()).and_then(|bytes| bytes.checked_add(256))
                .ok_or_else(|| invalid("scan-candidate-size", "Scan trie candidate capacity exceeds addressable memory"))?;
            Some(self.execution.context.memory.reserve(bytes)?)
        };
        let mut tries = Vec::new();
        tries.try_reserve_exact(trie_count).map_err(|error| invalid("scan-candidate-size", error))?;
        tries.extend(current_tries());
        tries.sort_unstable_by(|left, right| right.key.block_index().cmp(&left.key.block_index()).then_with(|| left.key.cmp(&right.key)));
        let memory = &self.execution.context.memory;
        let mut summary_reservation = if tries.is_empty() { None } else { Some(memory.reserve(summary_bytes(tries.len())?)?) };
        let mut combined = Vec::new();
        combined.try_reserve_exact(tries.len()).map_err(|error| invalid("scan-summary-size", error))?;
        for (index, trie) in tries.iter().enumerate() {
            let temporal = trie.metadata.as_ref().and_then(|metadata| metadata.temporal_metadata.as_ref()).map(|metadata| TemporalSummary {
                min_valid_from: Timestamp(metadata.min_valid_from.unwrap_or(i64::MIN)), max_valid_to: Timestamp(metadata.max_valid_to.unwrap_or(i64::MAX)),
                min_system_from: Timestamp(metadata.min_system_from.unwrap_or(i64::MIN)), max_system_from: Timestamp(metadata.max_system_from.unwrap_or(i64::MAX)),
            });
            combined.push(PageSummary { page_index: u32::try_from(index).map_err(|error| invalid("file-count", error))?, temporal,
                recency: Some(recency(trie.key.recency())?), iid_range: Some(path_range(trie.key.part())?) });
        }
        let slug = self.snapshot.table_entry(table).map(|entry| entry.slug.clone());
        let mut pages = PageDescriptors::default();
        let mut live_segment_count = 0;
        for segment in self.snapshot.iter_segments(table) {
            audience_work::scan_items(1)?;
            let priority = live_segment_count;
            live_segment_count += 1;
            segment.trie().try_visit_leaves(|leaf| {
                audience_work::scan_items(1)?;
                let (lower, upper) = path_range(leaf.path().as_slice())?;
                if !self.path_matches(lower, upper) { return Ok(()); }
                audience_work::scan_items(leaf.len())?;
                let rows = if audience_work::is_bounded() {
                    segment.located_rows_in(leaf, &self.execution.context.memory)?
                } else if let Some(iids) = self.scan.iids.as_ref()
                    .filter(|iids| self.candidate_iids.is_none() && iids.len().saturating_mul(LOCATED_IID_FRACTION) < leaf.len())
                {
                    // The merge drops rows of other IIDs, so the page never needs them located; a page left with no rows
                    // contributes nothing, and an empty summary would stop the file pages' pruning.
                    let rows = segment.located_rows_of(leaf, iids, &self.execution.context.memory)?;
                    if rows.located.is_empty() { return Ok(()); }
                    rows
                } else {
                    segment.located_rows(leaf)?
                };
                let temporal = summarize_headers(rows.headers.iter().copied());
                pages.push(Page { lower, upper, priority, source: PageSource::Live { table: Arc::clone(segment), rows }, content_matches: true, unread: None,
                    summary: PageSummary { page_index: 0, temporal, recency: Some(Timestamp::MAX), iid_range: Some((lower, upper)) } }, &self.execution.context.memory)
            })?;
        }
        let live_count = pages.entries.len();
        if live_count != 0 {
            let count = tries.len().checked_add(live_count).ok_or_else(|| invalid("page-count", "Candidate count overflow"))?;
            let reservation = memory.reserve(summary_bytes(count)?)?;
            combined.try_reserve_exact(live_count).map_err(|error| invalid("scan-summary-size", error))?;
            summary_reservation = Some(reservation);
        }
        for (index, page) in pages.entries.iter().enumerate() {
            let mut summary = page.summary.clone();
            summary.page_index = u32::try_from(tries.len().checked_add(index).ok_or_else(|| invalid("page-count", "Candidate count overflow"))?).map_err(|error| invalid("page-count", error))?;
            combined.push(summary);
        }
        let (selected, selected_reservation) = {
            let count = combined.len();
            let _selector_reservation = if count == 0 { None } else {
                let bytes = count.checked_mul(std::mem::size_of::<&PageSummary>()).and_then(|bytes| bytes.checked_add(256))
                    .and_then(|bytes| bytes.checked_mul(3))
                    .ok_or_else(|| invalid("scan-selector-size", "Scan selector capacity exceeds addressable memory"))?;
                Some(memory.reserve(bytes)?)
            };
            let references = || -> Result<_> {
                let mut entries = Vec::new();
                entries.try_reserve_exact(count).map_err(|error| invalid("scan-selector-size", error))?;
                Ok(entries)
            };
            let mut eligible = references()?;
            let mut emitted = references()?;
            let mut dependent = references()?;
            select_pages_with_scratch(&combined, self.pruning(), |_| true, &mut eligible, &mut emitted, &mut dependent);
            emitted.retain(|summary| summary.iid_range.is_none_or(|(lower, upper)| self.path_matches(lower, upper))
                && (summary.page_index as usize) < tries.len());
            let count = emitted.len();
            let selected_reservation = if count == 0 { None } else {
                let bytes = count.checked_mul(std::mem::size_of::<usize>()).and_then(|bytes| bytes.checked_add(256))
                    .ok_or_else(|| invalid("scan-selected-file-size", "Selected scan index capacity exceeds addressable memory"))?;
                Some(memory.reserve(bytes)?)
            };
            let mut selected = Vec::new();
            selected.try_reserve_exact(count).map_err(|error| invalid("scan-selected-file-size", error))?;
            selected.extend(emitted.iter().map(|summary| summary.page_index as usize));
            (selected, selected_reservation)
        };
        drop(combined);
        drop(summary_reservation);
        let file_count = selected.len();
        self.counts.files_pruned = (tries.len() - file_count) as u64;
        self.counts.files_used = file_count as u64;
        let mut files = FileDescriptors::default();
        for index in selected {
            let trie = &tries[index];
            let (lower, upper) = path_range(trie.key.part())?;
            if !self.path_matches(lower, upper) { continue; }
            let slug = slug.as_ref().ok_or_else(|| invalid("missing-table-slug", "Persisted trie has no recorded table registry entry"))?;
            let key = ObjectKey::new(slug.metadata_key(&trie.key))?;
            let (metadata, _reservation) = read_metadata(&self.storage, &key, &self.execution.context.memory).await?;
            files.admit(file_count, &self.execution.context.memory)?;
            let file = files.entries.len();
            files.entries.push(DataFile {
                reader: DataFileReader::new(&self.storage, ObjectKey::new(slug.data_key(&trie.key))?),
                metadata: key,
            });
            let trie_recency = recency(trie.key.recency())?;
            let trie_key = Arc::new(trie.key.clone());
            let memory = &self.execution.context.memory;
            let mut pending = MetadataStack::default();
            if let Some(node) = metadata.root_index() {
                pending.push(MetadataVisit { node, path: [0; MAX_IID_DEPTH], depth: 0, recency: trie_recency }, memory)?;
            }
            while let Some(visit) = pending.entries.pop() {
                audience_work::scan_items(1)?;
                self.execution.context.check_cancelled()?;
                let path = visit.path.get(..visit.depth).ok_or_else(invalid_iid_path)?;
                let (lower, upper) = path_range(path)?;
                if !self.path_matches(lower, upper) { continue; }
                match &metadata.nodes()[visit.node] {
                    MetadataNode::Nil => {}
                    MetadataNode::IidBranch(children) => for (bucket, child) in children.iter().enumerate().rev() {
                        if let Some(node) = child {
                            let mut next = MetadataVisit { node: *node, depth: visit.depth + 1, ..visit };
                            if let Some(slot) = next.path.get_mut(visit.depth) { *slot = bucket as u8; }
                            pending.push(next, memory)?;
                        }
                    },
                    MetadataNode::RecencyBranch(children) => {
                        audience_work::scan_items(children.len())?;
                        for (upper, node) in children.iter().rev() {
                            pending.push(MetadataVisit { node: *node, recency: visit.recency.min(*upper), ..visit }, memory)?;
                        }
                    }
                    MetadataNode::Leaf { data_page_index, columns } => {
                        // An ordered scan may stop after its first tasks, so it reads a leaf's statistics when it reaches the leaf's task.
                        let (content_matches, temporal, unread) = if self.order.is_some() { (true, None, Some(visit.node)) } else {
                            let columns = &columns.statistics()?;
                            (self.leaf_matches(columns, &page_test)?, page_temporal(columns), None)
                        };
                        pages.push(Page { lower, upper, priority: live_segment_count + index, content_matches, unread,
                            source: PageSource::File { file, page: *data_page_index, trie: Arc::clone(&trie_key) }, summary: PageSummary { page_index: 0, temporal,
                                recency: Some(visit.recency), iid_range: Some((lower, upper)) } }, &self.execution.context.memory)?
                    }
                }
            }
        }
        drop(selected_reservation);
        drop(tries);
        drop(candidate_reservation);
        for (index, page) in pages.entries.iter_mut().enumerate() { page.summary.page_index = u32::try_from(index).map_err(|error| invalid("page-count", error))?; }
        self.tasks = Some(ScanTaskPlan::new(pages.entries.iter().map(|page|PageRange { lower: page.lower,upper: page.upper }),self.order == Some(IidOrder::Descending),&self.execution.context.memory)?);
        self.files = files;
        self.pages = pages;
        self.page_test = self.order.map(|_| page_test);
        self.initialized = true;
        Ok(())
    }
    /// A redacted candidate needs temporal resolution even when no put can satisfy the predicates.
    fn leaf_matches(&self, columns: &rtdb_format::metadata::LeafStatistics<'_>, page_test: &crate::page_metadata::PageTest) -> Result<bool> {
        if columns.has_redacted_assertions() { return Ok(true); }
        Ok(self.pushdowns.as_ref().map(|filters| filters.page_matches(&self.scan.columns, columns, &self.execution.context.memory)).transpose()?.unwrap_or(true)
            && may_satisfy_not_null(&self.scan, columns) && page_test.matches(columns)?)
    }
    /// Tests the statistics an ordered scan left unread for the pages of the task it has reached.
    /// The metadata is read from the pool that `initialize` decoded it into; where the pool no longer holds a file, its
    /// pages stay as they are, read and resolved without pruning.
    fn read_statistics(&mut self, pages: &[usize]) -> Result<()> {
        let Some(page_test) = &self.page_test else { return Ok(()) };
        for index in pages {
            let page = &self.pages.entries[*index];
            let (Some(node), PageSource::File { file, .. }) = (page.unread, &page.source) else { continue };
            let file = &self.files.entries[*file];
            let read = match file.reader.storage.view::<CachedMetadata>(&file.metadata, View::File) {
                Some(cached) => {
                    audience_work::decode_bytes(cached.bound)?;
                    let _charge = self.execution.context.memory.reserve(cached.bound)?;
                    let Some(MetadataNode::Leaf { columns, .. }) = cached.metadata.nodes().get(node) else { return Err(invalid("metadata-node", "A page's metadata node is not a leaf")) };
                    let columns = &columns.statistics()?;
                    Some((self.leaf_matches(columns, page_test)?, page_temporal(columns)))
                }
                None => None,
            };
            let page = &mut self.pages.entries[*index];
            page.unread = None;
            if let Some((content_matches, temporal)) = read { page.content_matches = content_matches; page.summary.temporal = temporal; }
        }
        Ok(())
    }
    async fn load(&mut self, index: usize) -> Result<()> {
        self.execution.context.check_cancelled()?;
        if self.loaded.contains_key(&index) { return Ok(()); }
        let reservation = self.execution.context.memory.reserve(std::mem::size_of::<LoadedPage>() + 256)?;
        let page = &self.pages.entries[index];
        let mut charge = None;
        let mut cached = false;
        let mut admitted: Option<(Arc<IndexBatch>, usize)> = None;
        let hit = match &page.source {
            PageSource::File { file, page: page_index, .. } => {
                let data = &self.files.entries[*file].reader;
                data.storage.view::<CachedPage>(&data.key, View::Page(*page_index))
            }
            PageSource::Live { .. } => None,
        };
        let rows = match (&page.source, hit) {
            (PageSource::Live { table, rows }, _) => ActiveRows::Live { table: Arc::clone(table), rows: Arc::clone(rows) },
            (PageSource::File { .. }, Some(held)) => {
                if !self.execution.context.spatial_refinement {
                    audience_work::decode_bytes(held.bound)?;
                    charge = Some(self.execution.context.memory.reserve(held.bound)?);
                }
                cached = true;
                ActiveRows::File(Arc::clone(&held.batch))
            }
            (PageSource::File { file, page: page_index, .. }, None) => {
                let data = &mut self.files.entries[*file].reader;
                let context = &self.execution.context;
                let batch = if context.spatial_refinement {
                    let selected = self.scan.columns.iter().map(|field| field.name.clone()).collect::<Vec<_>>();
                    let (page, charged) = data.decode_spatial_page(*page_index, &context.memory, PageProjection::PutColumns(&selected), None).await?;
                    audience_work::scan_items(page.num_rows())?;
                    IndexBatch::from_owned(OwnedBatch::with_reservation(page, charged)?)?
                } else {
                    let scan = &self.scan;
                    let ranking = &self.ranking;
                    let (batch, charged) = data.decode_scan_page(*page_index, &context.memory, || {
                        scan.columns.iter().map(|field| &field.name)
                            .chain(scan.matches.iter().map(|matched| &matched.column))
                            .chain(ranking.iter().map(|rank| &rank.source)).cloned().collect()
                    }).await?;
                    if let Some(charged) = &charged {
                        admitted = Some((Arc::clone(&batch), charged.bytes()));
                    }
                    charge = charged;
                    batch
                };
                ActiveRows::File(batch)
            }
        };
        if let ActiveRows::File(batch) = &rows
            && batch.view().has_entity_keys() != self.scan.table.name.table.is_owned_event_data()
        {
            return Err(invalid("owned-entity-key", "Data page has the wrong physical entity-key layout"));
        }
        let mut previous: Option<EventHeader> = None;
        // A live page is a slice of this process's own trie, ordered as it was built; a file page is checked when first decoded.
        let validated = if matches!(rows, ActiveRows::Live { .. }) || cached { 0 } else { rows.len() };
        for index in 0..validated {
            let source = rows.row(index)?;
            let header = source.header()?;
            if header.iid < page.lower || header.iid > page.upper { return Err(invalid("page-path", "Data page contains an IID outside its metadata path")); }
            if previous.is_some_and(|previous| previous.iid > header.iid || (previous.iid == header.iid && previous.precedence() < header.precedence())) {
                return Err(invalid("page-order", "Data page is not in IID and descending system-position order"));
            }
            previous = Some(header);
        }
        if let (Some((batch, bound)), PageSource::File { file, page: page_index, .. }) = (admitted, &page.source) {
            let data = &self.files.entries[*file].reader;
            data.storage.insert_view(&data.key, View::Page(*page_index), Arc::new(CachedPage { batch, bound }));
        }
        self.loaded.insert(index,LoadedPage { rows,_reservation: reservation,_charge: charge });
        Ok(())
    }
    pub(crate) async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        loop {
            self.execution.context.check_cancelled()?;
            if let Some(page) = self.output.pop_front() { return Ok(Some(page)); }
            self.output_reservation = None;
            let Some(task) = self.next_resolved_task().await? else { return Ok(None); };
            self.buffer_task(task).await?;
        }
    }

    async fn prune_covered_iid_pages(&mut self, selected: &mut Vec<usize>, iid: Iid) -> Result<()> {
        if selected.len() < 2 || self.scan.table.name.table.is_owned_event_data()
            || self.candidate_iids.as_ref().is_some_and(|iids| iids.len() != 1)
        {
            return Ok(());
        }
        let cut = self.bounds.system_time.lower();
        let page_time = |index: usize| self.pages.entries[index].summary.temporal;
        let Some((index, newest)) = selected.iter().copied().filter_map(|index| {
            page_time(index).filter(|summary| summary.min_system_from <= cut)
                .map(|summary| (index, summary.max_system_from))
        }).max_by_key(|(_, newest)| *newest) else { return Ok(()); };
        if !selected.iter().any(|&index| page_time(index).is_some_and(|summary| summary.max_system_from < newest)) {
            return Ok(());
        }
        self.load(index).await?;
        let rows = &self.loaded[&index].rows;
        let row = rows.seek(0, |header| header.iid < iid || (header.iid == iid && header.system_from > cut))?;
        if row == rows.len() { return Ok(()); }
        let anchor = rows.header(row)?;
        if anchor.iid == iid && covers_history(anchor, self.bounds) {
            // A newer page may still shadow the anchor. Every page at its timestamp may affect an equal-time tie.
            selected.retain(|&index| self.pages.entries[index].summary.temporal
                .is_none_or(|summary| summary.max_system_from >= anchor.system_from));
        }
        Ok(())
    }

    async fn next_resolved_task(&mut self) -> Result<Option<ResolvedScanTask>> {
        if !self.initialized { self.initialize().await?; }
        loop {
            self.execution.context.check_cancelled()?;
            let task = self.tasks.as_mut().map(ScanTaskPlan::next).transpose()?.flatten();
            let Some(task) = task else { self.loaded.clear(); return Ok(None); };
            audience_work::scan_items(1)?;
            if self.path_matches(task.range.lower, task.range.upper) {
                return self.resolve_task(task).await.map(Some);
            }
        }
    }

    async fn resolve_task(&mut self, task: ScanTask) -> Result<ResolvedScanTask> {
        audience_work::scan_items(task.pages.len())?;
        let bound = task.pages.len().checked_mul(std::mem::size_of::<PageSummary>() + std::mem::size_of::<Head>() + std::mem::size_of::<PageBuilder>() + std::mem::size_of::<OwnedPage>() + 256).and_then(|bytes|bytes.checked_add(4096)).ok_or_else(||invalid("scan-task-size","Scan task scratch exceeds addressable memory"))?;
        let reservation = self.execution.context.memory.reserve(bound)?;
        self.read_statistics(&task.pages)?;
        let entries = &self.pages.entries;
        let summaries = task.pages.iter().map(|&index| &entries[index].summary);
        let pruning = self.pruning();
        let mut selected: Vec<_> = select_pages_from_iter(summaries, pruning, |summary| entries[summary.page_index as usize].content_matches)
            .into_iter().map(|summary| summary.page_index as usize).collect();
        if let Some(iid) = pruning.iid.filter(|_| !pruning.bypass_temporal_pruning) {
            self.prune_covered_iid_pages(&mut selected, iid).await?;
        }
        self.counts.pages_pruned = self.counts.pages_pruned.saturating_add((task.pages.len() - selected.len()) as u64);
        self.counts.pages_used = self.counts.pages_used.saturating_add(selected.len() as u64);
        selected.sort_by_key(|index| (matches!(self.pages.entries[*index].source,PageSource::Live { .. }),self.pages.entries[*index].priority,*index));
        // The task's rows the scan is confined to; `path_matches` admitted the task, so the two ranges share an IID.
        let (range_lower, range_upper) = match self.confined {
            Some((first, last)) => (task.range.lower.max(first), task.range.upper.min(last)),
            None => (task.range.lower, task.range.upper),
        };
        let mut groups = Vec::with_capacity(selected.len());
        let mut group_reservations = Vec::with_capacity(selected.len());
        let mut heads = BinaryHeap::with_capacity(selected.len());
        for (group,index) in selected.iter().copied().enumerate() {
            self.load(index).await?;
            let rows = &self.loaded[&index].rows;
            self.counts.rows_read = self.counts.rows_read.saturating_add(rows.len() as u64);
            groups.push(Vec::new());
            group_reservations.push(self.execution.context.memory.reserve(256)?);
            let low = rows.seek(0, |header| header.iid < range_lower)?;
            if low < rows.len() {
                let header = rows.header(low)?;
                if header.iid <= range_upper { heads.push(Head { header,active: group,priority: self.pages.entries[index].priority,ordinal: low }); }
            }
        }
        let archive_history = projects_archive_history(&self.scan);
        if archive_history && !self.execution.context.catalog.admits_archive_history(&self.scan.table.name) {
            return Err(invalid("archive-history", "The scan has no frozen archive authority"));
        }
        let mut resolver_bounds = self.bounds;
        if archive_history {
            // The held SQL cap still limits input events. On those events,
            // resolve the system dimension by transaction position.
            resolver_bounds.system_time = Interval::ALL;
        }
        let mut resolver = HistoryResolver::new_in(resolver_bounds, self.scan.clamp_valid_time, Arc::clone(&self.execution.context.memory));
        let keyed = self.scan.table.name.table.is_owned_event_data();
        let position_mask = self.execution.context.catalog.owned_position_mask;
        let transaction_cut = self.execution.context.catalog.owned_transaction_cut;
        let requested_key = if keyed { self.execution.context.catalog.owned_identity().map(EntityId::handle_bytes) } else { None };
        let requested_artefacts = self.execution.context.catalog.owned_artefact_keys();
        // Every equal physical IID stays within one task. A key's events still
        // arrive in descending system time, independently of its neighbours.
        let mut bucket = None;
        let mut identities = BTreeMap::<&[u8], (HistoryResolver, Arc<MemoryReservation>)>::new();
        let mut visited = 0usize;
        let active: Vec<&ActiveRows> = selected.iter().map(|index| &self.loaded[index].rows).collect();
        // Resolved rows name their batch by its place here, so a row costs no handle on its batch.
        let mut batches: Vec<Arc<IndexBatch>> = Vec::new();
        let mut handles: Vec<Vec<u32>> = vec![Vec::new(); active.len()];
        let mut next = heads.pop();
        while let Some(head) = next.take() {
            audience_work::scan_items(1)?;
            self.execution.context.check_cancelled_at(visited)?;
            visited += 1;
            let rows = active[head.active];
            let ordinal = head.ordinal + 1;
            let following = if ordinal < rows.len() {
                let header = rows.header(ordinal)?;
                (header.iid <= range_upper).then_some(Head { header,ordinal,..head })
            } else { None };
            // A page's next row that still precedes every other page's head is taken without passing through the heap.
            next = match following {
                Some(following) if heads.peek().is_none_or(|top| following > *top) => Some(following),
                Some(following) => { heads.push(following); heads.pop() }
                None => heads.pop(),
            };
            if let Some(iids) = &self.candidate_iids {
                if !iids.contains(head.header.iid) { continue; }
            } else if let Some(iids) = &self.scan.iids {
                if !iids.contains(&head.header.iid) { continue; }
            } else if self.pushdowns.as_ref().is_some_and(|filters| !filters.iid_matches(&self.scan.columns, head.header.iid, self.canonical_entity_iids)) {
                continue;
            }
            if let Some(cut) = transaction_cut && !rtdb_temporal::event_applies_at_cut(&head.header, cut)? {
                continue;
            }
            // The masked commit is named by its position: a tied commit
            // shares its time and stays visible (T7).
            if position_mask.is_some_and(|(commit, from)| {
                head.header.written_in(commit)
                    && head.header.commit_position >= from
                    && head.header.op != EventOp::Erase
            }) { continue; }
            let mut header = head.header;
            if archive_history {
                if !self.bounds.system_time.contains(header.system_from) { continue; }
                let position = header.position.filter(|position| (0..i64::MAX).contains(&position.transaction.0))
                    .ok_or_else(|| invalid("archive-event-position", "An archive control lacks its original position"))?;
                header.system_from = Timestamp(position.transaction.0);
            }
            let (slot, batch, row) = rows.batch(head.ordinal)?;
            let resolver = if keyed {
                let key = batch.view().entity_key(row as usize)?.ok_or_else(|| {
                    invalid("owned-entity-key", "Governed data requires complete physical entity keys")
                })?;
                if requested_key.as_deref().is_some_and(|requested| requested != key) { continue; }
                if requested_artefacts.is_some_and(|requested| !requested.by_iid.get(&head.header.iid)
                    .is_some_and(|keys| keys.iter().any(|known| known.as_slice() == key))) { continue; }
                if bucket != Some(head.header.iid) {
                    identities.clear();
                    bucket = Some(head.header.iid);
                }
                match identities.entry(key) {
                    std::collections::btree_map::Entry::Occupied(entry) => &mut entry.into_mut().0,
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let reservation = self.execution.context.memory.reserve(key.len().saturating_add(512))?;
                        rtdb_columnar::EntityIdRef::from_handle_bytes(key)?;
                        &mut entry.insert((HistoryResolver::new_in(
                            self.bounds, self.scan.clamp_valid_time, Arc::clone(&self.execution.context.memory),
                        ), reservation)).0
                    }
                }
            } else {
                &mut resolver
            };
            let page = &mut handles[head.active];
            if page.len() <= slot { page.resize(slot + 1, u32::MAX); }
            if page[slot] == u32::MAX { batches.push(Arc::clone(batch)); page[slot] = u32::try_from(batches.len() - 1).map_err(|_| invalid("scan-row-size", "A task reads more batches than u32 names"))?; }
            let source = (page[slot], row);
            // The merge visits rows in IID order across the task's pages, which one group keeps and a group per page does not.
            let group: &mut Vec<_> = &mut groups[if self.order.is_some() { 0 } else { head.active }];
            let memory = &self.execution.context.memory;
            resolver.resolve_fragments(&header, |valid_time, system_time| {
                if group.len() == group.capacity() {
                    let capacity = group.capacity().saturating_mul(2).max(64);
                    let bytes = capacity.checked_mul(std::mem::size_of::<ResolvedRow<(u32, u32)>>()).ok_or_else(|| invalid("scan-row-size", "Scan rows exceed addressable memory"))?;
                    group_reservations.push(memory.reserve(bytes)?);
                    group.reserve_exact(capacity - group.len());
                }
                group.push(ResolvedRow { source, valid_time, system_time });
                Ok(())
            })?;
            let entity = head.header.iid;
            if covers_history(head.header, self.bounds) && (!keyed || requested_key.is_some()) {
                while let Some(pending) = next.filter(|pending| pending.header.iid == entity) {
                    if let Some(following) = active[pending.active].after_iid(pending, range_upper)? {
                        heads.push(following);
                    }
                    next = heads.pop();
                }
            }
        }
        if self.order == Some(IidOrder::Descending) && let Some(rows) = groups.first_mut() { rows.reverse(); }
        // A page is kept for the tasks still to come that read it: those above this one, or below it in a descending scan.
        if self.order == Some(IidOrder::Descending) { self.loaded.retain(|index,_| self.pages.entries[*index].lower < task.range.lower); }
        else { self.loaded.retain(|index,_| self.pages.entries[*index].upper > task.range.upper); }
        Ok(ResolvedScanTask { selected, groups, batches, reservation, _group_reservations: group_reservations })
    }

    async fn buffer_task(&mut self, task: ResolvedScanTask) -> Result<()> {
        let ResolvedScanTask { selected, groups, batches, reservation, _group_reservations } = task;
        let archive_history = projects_archive_history(&self.scan);
        let keyed = self.scan.table.name.table.is_owned_event_data();
        let owned = self.execution.context.catalog.admits_owned_event_positions(&self.scan.table.name);
        for (group, rows) in groups.iter().enumerate().filter(|(_, rows)| !rows.is_empty()) {
            let mut metadata = Box::pin(self.retained_transaction_metadata(rows, &batches)).await?;
            let page = &self.pages.entries[selected[group]].source;
            let memory = &self.execution.context.memory;
            let output = crate::columnar::gather_with_funded_columns(rows, &batches, &self.scan.columns, memory, |position| {
                let field = &self.scan.columns[position];
                if let Some(ranking) = &self.ranking && field.id == ranking.output {
                    return self.rank_column(ranking, rows, &batches).map(|column| Some((column, None)));
                }
                if let Some((column, charge)) = history_column(&field.name, rows, &batches, archive_history, keyed, owned, memory)? {
                    return Ok(Some((column, Some(charge))));
                }
                if !archive_history && field.name == "user_metadata" && let Some((column, charge)) = metadata.take() {
                    return Ok(Some((column, Some(charge))));
                }
                self.slots[position].map(|slot| self.match_column(slot, page, rows, &batches).map(|column| (column, None))).transpose()
            })?;
            self.output.push_back(output);
        }
        self.output_reservation = Some(reservation);
        Ok(())
    }

    fn match_column(&self, evaluated: usize, source: &PageSource, rows: &[ResolvedRow<(u32, u32)>], batches: &[Arc<IndexBatch>]) -> Result<crate::columnar::Column> {
        let ScanMatch { query, files, live } = &self.matches[evaluated];
        let page_answers = match (files, source) {
            (Some(answers), PageSource::File { file, page, .. }) => answers.file(&self.files.entries[*file].reader.key).map(|matches| (matches, *page)),
            _ => None,
        };
        resolved_match_column(&self.scan.matches[evaluated].column, query.as_deref(), rows, batches, &self.execution.context.memory, |_, row, query| {
            page_answers.and_then(|(matches, page)| matches.value(page, row.source.1 as usize).ok())
                .or_else(|| live_match(live.as_deref(), row, batches, query))
        })
    }

    fn rank_column(&self, ranking: &RankProjection, rows: &[ResolvedRow<(u32, u32)>], batches: &[Arc<IndexBatch>]) -> Result<crate::columnar::Column> {
        let mut values = Float64Builder::with_capacity(rows.len());
        for row in rows {
            let Some(scorer) = &ranking.scorer else {
                values.append_null();
                continue;
            };
            let batch = &batches[row.source.0 as usize];
            let position = row.source.1 as usize;
            let iid = batch.header(position)?.iid;
            if let Some(score) = ranking.verified.as_ref().and_then(|verified| verified.score(iid)) {
                values.append_value(score);
                continue;
            }
            if let Some(score) = ranking.live.as_ref().and_then(|live| live.score(batch, position, scorer)) {
                values.append_value(score);
                continue;
            }
            values.append_option(match resolved_text(&ranking.source, row, batches)? {
                PutText::Missing => None,
                PutText::Other => Some(scorer.bm25_text("")),
                PutText::Utf8(text) => {
                    let _tokens = self.execution.context.memory.reserve(256usize.saturating_add(text.len().saturating_mul(32)))?;
                    Some(scorer.bm25_text(text))
                }
            });
        }
        Ok(crate::columnar::Column::Arrow {
            array: Arc::new(values.finish()),
            field: Arc::new(arrow_schema::Field::new("search_score", DataType::Float64, true)),
            logical: Some(rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::F64).maybe()),
        })
    }
}

fn history_column(name: &str, rows: &[ResolvedRow<(u32, u32)>], batches: &[Arc<IndexBatch>], archive: bool,
    keyed: bool, owned: bool, memory: &Arc<MemoryBudget>) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let headers = || rows.iter().map(move |row| batches[row.source.0 as usize].header(row.source.1 as usize));
    let len = rows.len();
    let temporal_bytes = size_of::<i64>() + 1;
    let column = match name {
        "_assertion_ordinal" if keyed => history_integers(name, len, false, memory,
            headers().map(|header| Ok(Some(i64::from(header?.commit_position)))))?,
        ARCHIVE_TX_FROM if archive => history_integers(name, len, false, memory,
            rows.iter().map(|row| Ok(Some(row.system_time.lower().0))))?,
        ARCHIVE_TX_TO if archive => history_integers(name, len, true, memory, rows.iter().map(|row| {
            let end = row.system_time.upper();
            Ok((end != Timestamp::MAX).then_some(end.0))
        }))?,
        "_system_from" if archive => history_vector(name, len, temporal_bytes, false, memory, || {
            let mut times = TimestampMicrosecondBuilder::with_capacity(len);
            for header in headers() { times.append_value(header?.system_from.0); }
            Ok(times.finish().with_timezone("UTC"))
        })?,
        "_system_to" if archive => history_vector(name, len, temporal_bytes, true, memory, || {
            Ok(TimestampMicrosecondArray::from_iter((0..len).map(|_| None::<i64>)).with_timezone("UTC"))
        })?,
        OWNED_EVENT_POSITION_COLUMN if !archive && owned => {
            let width = EventPosition::WIDTH as usize;
            let max_positions = i32::MAX as usize / width;
            history_vector(name, len, width + size_of::<i32>() + 1, true, memory, || {
                let mut positions = BinaryBuilder::with_capacity(len, len.min(max_positions) * width);
                let mut present = 0;
                for header in headers() {
                    if let Some(position) = header?.position {
                        if present == max_positions {
                            return Err(invalid("column-gather", "Event position bytes exceed binary offsets"));
                        }
                        positions.append_value(position.to_bytes());
                        present += 1;
                    } else {
                        positions.append_null();
                    }
                }
                Ok(positions.finish())
            })?
        }
        HOSTED_RECEIPT_TX_ID if !archive && owned => history_integers(name, len, true, memory,
            headers().map(|header| Ok(header?.position.map(|position| position.transaction.0))))?,
        _ => return Ok(None),
    };
    Ok(Some(column))
}

fn history_integers(name: &str, len: usize, nullable: bool, memory: &Arc<MemoryBudget>,
    cells: impl Iterator<Item = Result<Option<i64>>>) -> Result<(Column, Arc<MemoryReservation>)> {
    history_vector(name, len, size_of::<i64>() + 1, nullable, memory, || {
        let mut integers = Int64Builder::with_capacity(len);
        for cell in cells { integers.append_option(cell?); }
        Ok(integers.finish())
    })
}

fn history_vector<A: Array + 'static>(name: &str, len: usize, cell_bytes: usize, nullable: bool,
    memory: &Arc<MemoryBudget>, build: impl FnOnce() -> Result<A>) -> Result<(Column, Arc<MemoryReservation>)> {
    let size_error = || invalid("column-size", "History column exceeds addressable memory");
    let bound = len.checked_mul(cell_bytes).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(size_error)?;
    let mut charge = memory.reserve(bound)?;
    let array = Arc::new(build()?);
    let field = Arc::new(arrow_schema::Field::new(name, array.data_type().clone(), nullable));
    let logical = from_field(&field)?;
    let logical_bytes = logical.retained_bytes()?;
    let bytes = array.get_array_memory_size().checked_add(field.size())
        .and_then(|bytes| bytes.checked_add(logical_bytes)).ok_or_else(size_error)?;
    let lease = Arc::get_mut(&mut charge).expect("The history column owns its reservation");
    if bytes <= lease.bytes() { lease.shrink_to(bytes)?; } else { lease.grow_to(bytes)?; }
    Ok((Column::Arrow { array, field, logical: Some(logical) }, charge))
}

fn live_match(live: Option<&dyn crate::LiveSearchTerms>, row: &ResolvedRow<(u32, u32)>, batches: &[Arc<IndexBatch>],
    query: &rtdb_text::Query) -> Option<Option<bool>> {
    live?.matches(&batches[row.source.0 as usize], row.source.1 as usize, query).map(Some)
}

fn resolved_match_column(name: &str, query: Option<&rtdb_text::Query>,
    rows: &[ResolvedRow<(u32, u32)>], batches: &[Arc<IndexBatch>], memory: &Arc<MemoryBudget>,
    answer: impl Fn(usize, &ResolvedRow<(u32, u32)>, &rtdb_text::Query) -> Option<Option<bool>>) -> Result<crate::columnar::Column> {
    let mut values = BooleanBuilder::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        let Some(query) = query else {
            values.append_null();
            continue;
        };
        if let Some(value) = answer(index, row, query) {
            values.append_option(value);
            continue;
        }
        let text = resolved_text(name, row, batches)?;
        values.append_option(match text {
            PutText::Missing => None,
            PutText::Other => Some(false),
            PutText::Utf8(text) => {
                let _tokens = memory.reserve(256usize.saturating_add(text.len().saturating_mul(32)))?;
                Some(query.matches_text(text))
            }
        });
    }
    Ok(crate::columnar::Column::Arrow {
        array: Arc::new(values.finish()),
        field: Arc::new(arrow_schema::Field::new(name, DataType::Boolean, true)),
        logical: Some(rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Bool).maybe()),
    })
}

fn resolved_text<'a>(name: &str, row: &ResolvedRow<(u32, u32)>, batches: &'a [Arc<IndexBatch>]) -> Result<PutText<'a>> {
    Ok(match name {
        "_valid_to" if row.valid_time.upper() == Timestamp::MAX => PutText::Missing,
        "_system_to" if row.system_time.upper() == Timestamp::MAX => PutText::Missing,
        "_iid" | "_valid_from" | "_valid_to" | "_system_from" | "_system_to" => PutText::Other,
        _ => batches[row.source.0 as usize].view().borrow_event(row.source.1 as usize)?.put_text(name)?,
    })
}

async fn bounds(scan: &Scan, snapshot: &Snapshot, execution: &Execution) -> Result<TemporalBounds> {
    let valid_time = dimension(&scan.valid_time, execution.context.current_time, execution).await?;
    let basis = snapshot.system_time_cap();
    let system = if basis.is_none() && matches!(scan.system_time, TemporalFilter::Default) { Interval::ALL }
        else { dimension(&scan.system_time, basis.map(Into::into).unwrap_or(execution.context.current_time), execution).await? };
    let cap = basis.map(|basis| basis.0.saturating_add(1)).map(Timestamp).unwrap_or(Timestamp::MAX);
    // A point after the basis reads as the basis: what is known at the basis is all that can be known then.
    let system = match (&scan.system_time, basis) {
        (TemporalFilter::At(_), Some(basis)) if system.lower() > basis => Interval::new(basis, cap)?,
        _ => system,
    };
    let upper = cap.min(system.upper());
    let system_time = Interval::new(system.lower().min(upper), upper)?;
    Ok(TemporalBounds { valid_time, system_time })
}
async fn dimension(filter: &TemporalFilter, default: rtdb_types::Instant, execution: &Execution) -> Result<Interval> {
    let scope = execution.scope()?;
    async fn value(expression: &rtdb_plan::Expr, fallback: rtdb_types::Instant, execution: &Execution, scope: &rtdb_expr::EvaluationScope) -> Result<Timestamp> {
        let value = execution.evaluate(scope, expression, &Schema::default(), &[]).await?;
        if matches!(value, Value::Null | Value::Undefined) { fallback.as_micros() } else { rtdb_expr::timestamp_micros(&value, &execution.context.timezone) }
    }
    let point = |time: Timestamp| Interval::new(time, Timestamp(time.0.saturating_add(1)));
    match filter {
        TemporalFilter::Default => point(default.as_micros()?), TemporalFilter::All => Ok(Interval::ALL),
        TemporalFilter::At(at) => point(value(at, execution.context.current_time, execution, &scope).await?),
        TemporalFilter::FromTo { from, to } => {
            let from = value(from, Timestamp::MIN.into(), execution, &scope).await?;
            Interval::new(from, value(to, Timestamp::MAX.into(), execution, &scope).await?.max(from))
        }
        TemporalFilter::Between { from, to } => {
            let from = value(from, Timestamp::MIN.into(), execution, &scope).await?;
            let to = value(to, Timestamp::MAX.into(), execution, &scope).await?;
            if to < from { Interval::new(from, from) } else { Interval::between(from, to) }
        }
    }
}
/// Decoded trie metadata, shared by every scan of a storage: a trie file never changes once written, and a point
/// lookup otherwise spends most of its time decoding the same files again. A query still admits the decoded size of
/// every file it reads. Bounded by file count and decoded size.
/// A decoded trie metadata file, and the bound a query admits for it, as it would to decode it.
struct CachedMetadata { metadata: Arc<MetadataFile>, bound: usize, _reservation: Arc<MemoryReservation> }
impl Cached for CachedMetadata { fn in_use(&self) -> bool { Arc::strong_count(&self.metadata) > 1 } }

struct CachedLayout {
    decoder: PageDecoder,
    builder_bound: OnceLock<Option<usize>>,
    _reservation: Arc<MemoryReservation>,
    _dictionaries: Vec<Arc<MemoryReservation>>,
}

impl CachedLayout {
    fn page_decode_bound(&self, page: usize, wire: &CacheLease, include_builder_bound: bool) -> Result<usize> {
        let bound = self.decoder.page_bound(page, wire.as_slice())?;
        // Arrow-only visitors can project a file whose full schema has no SQL builder layout.
        if !include_builder_bound {
            return Ok(bound);
        }
        match self.builder_bound.get_or_init(|| self.add_builder_bound(0).ok()) {
            Some(builder) => bound.checked_add(*builder)
                .ok_or_else(|| invalid("page-size", "Arrow schema allocation bound overflow")),
            // Preserve field error order relative to overflow for this page's starting bound.
            None => self.add_builder_bound(bound),
        }
    }

    fn add_builder_bound(&self, bound: usize) -> Result<usize> {
        self.decoder.schema().fields().iter().try_fold(bound, |bound, field| {
            bound.checked_add(estimate_builder_bytes(&from_field(field)?, &[])?)
                .ok_or_else(|| invalid("page-size", "Arrow schema allocation bound overflow"))
        })
    }
}

impl Cached for CachedLayout {}

/// A decoded data page, whose batch retains the pool's reservation, and the bound a query admits for it.
struct CachedPage { batch: Arc<IndexBatch>, bound: usize }
impl Cached for CachedPage { fn in_use(&self) -> bool { Arc::strong_count(&self.batch) > 1 } }

fn invalid_iid_path() -> Anomaly { invalid("iid-path", "Trie path exceeds 64 base-four buckets") }
fn path_range(path: &[u8]) -> Result<(Iid, Iid)> {
    if path.len() > MAX_IID_DEPTH || path.iter().any(|bucket| *bucket > 3) { return Err(invalid_iid_path()); }
    let mut lower = [0u8; 16];
    let mut upper = [u8::MAX; 16];
    for (depth, bucket) in path.iter().enumerate() {
        let shift = 6 - 2 * (depth % 4);
        lower[depth / 4] |= bucket << shift;
        upper[depth / 4] = (upper[depth / 4] & !(3 << shift)) | (bucket << shift);
    }
    Ok((Iid(lower), Iid(upper)))
}
fn recency(date: Option<rtdb_types::CivilDate>) -> Result<Timestamp> {
    date.map(|date| date.epoch_day().checked_mul(86_400_000_000).map(Timestamp).ok_or_else(|| invalid("recency", "Trie recency exceeds microsecond range"))).unwrap_or(Ok(Timestamp::MAX))
}
fn summarize_headers(headers: impl IntoIterator<Item = EventHeader>) -> Option<TemporalSummary> {
    let mut result: Option<TemporalSummary> = None;
    for event in headers {
        let summary = result.get_or_insert(TemporalSummary { min_valid_from: event.valid_time.lower(), max_valid_to: event.valid_time.upper(), min_system_from: event.system_from, max_system_from: event.system_from });
        summary.min_valid_from = summary.min_valid_from.min(event.valid_time.lower()); summary.max_valid_to = summary.max_valid_to.max(event.valid_time.upper());
        summary.min_system_from = summary.min_system_from.min(event.system_from); summary.max_system_from = summary.max_system_from.max(event.system_from);
    }
    result
}
fn may_satisfy_not_null(scan: &Scan, columns: &rtdb_format::metadata::LeafStatistics<'_>) -> bool {
    let [Expr::Unary { op: UnaryOp::IsNotNull, expr }] = scan.predicates.as_slice() else {
        return true;
    };
    let Expr::Column { id, .. } = expr.as_ref() else {
        return true;
    };
    if scan.matches.iter().any(|evaluated| evaluated.id == *id) { return true; }
    let mut fields = scan.columns.iter().filter(|field| field.id == *id);
    let (Some(field), None) = (fields.next(), fields.next()) else {
        return true;
    };
    if matches!(
        field.name.as_str(),
        "_iid" | "_valid_from" | "_valid_to" | "_system_from" | "_system_to" | "_valid_time" | "_system_time" | "_recency"
    ) {
        return true;
    }
    let mut matching = columns.named(&field.name).filter(|column| column.flag("root-col?") != Some(false));
    let (Some(column), None) = (matching.next(), matching.next()) else {
        return true;
    };
    (column.flag("root-col?"), column.count()) != (Some(true), Some(0))
}

fn page_temporal(columns: &rtdb_format::metadata::LeafStatistics<'_>) -> Option<TemporalSummary> {
    let bound = |name: &str, key: &str, lower: bool| -> Option<Timestamp> {
        let seconds = columns.root_column(name)?.bound("date-times", key)?;
        if !seconds.is_finite() { return None; }
        let epsilon = seconds.abs() * f64::EPSILON * 4.0 + 0.000001;
        let micros = (seconds + if lower { -epsilon } else { epsilon }) * 1_000_000.0;
        Some(Timestamp(if lower { micros.floor() as i64 } else { micros.ceil() as i64 }))
    };
    Some(TemporalSummary { min_valid_from: bound("_valid_from", "min", true)?, max_valid_to: bound("_valid_to", "max", false)?,
        min_system_from: bound("_system_from", "min", true)?, max_system_from: bound("_system_from", "max", false)? })
}

#[cfg(test)]
mod tests {
    struct SemanticPages {
        batches: crate::columnar::BoxBatchCursor,
        memory: Arc<MemoryBudget>,
    }

    impl SemanticPages {
        async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
            self.batches.next_batch().await?.map(|batch| batch.rows(&self.memory)).transpose()
        }
    }

    async fn semantic_pages(runtime: Arc<crate::runtime_schema::EmittedRelation>, execution: Execution) -> Result<SemanticPages> {
        let memory = Arc::clone(&execution.context.memory);
        let batches = crate::operators::build_column_pages(runtime, execution).await?.expect("Expected Arrow semantic pages");
        Ok(SemanticPages { batches, memory })
    }

    use rtdb_types::StructFields;
    use super::*;
    use std::ops::Range;
    use crate::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions, context::OuterRows, parameters::BoundParameters};
    use rtdb_format::{block::proto as pb, catalog::BlockCatalog, data::{StoredEvent, StoredOperation, build_event_batch},
        ipc::write_file, keys::{TrieKey, block_key}, metadata::MetadataBuilder};
    use rtdb_index::{PartitionState, TransactionBuffer};
    use rtdb_plan::{Catalog, ColumnId, Field, QualifiedTable};
    use rtdb_storage::{MemoryStore, ObjectStore, Result as StorageResult, StoredObject};
    use rtdb_trie::TrieConfig;
    use rtdb_types::{EntityId, LogicalType, ScalarType, TableRef, TimeUnit, TransactionKey, TxId};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn owned_spatial_refinement_scans_only_retained_iids_and_preserves_empty_projection() -> Result<()> {
        use crate::{CandidateIids, OwnedSpatialProjection, open_owned_spatial_refinement};
        use rtdb_types::OwnedRelation;

        let state = PartitionState::empty(TrieConfig::default())?;
        let memory = MemoryBudget::new(1 << 24);
        let relation = OwnedRelation::new("places")?;
        let key = TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) };
        let selected = EntityId::Uuid([0x40; 16]);
        let excluded = EntityId::Uuid([0x80; 16]);
        let expired = EntityId::Uuid([0; 16]);
        let mut write = TransactionBuffer::new(state.snapshot(), key, Arc::clone(&memory))?;
        for (owner, id, location, valid_time) in [
            ("alice", &selected, 1, Interval::ALL),
            ("alice", &excluded, 2, Interval::ALL),
            ("bob", &selected, 3, Interval::ALL),
            ("alice", &expired, 4, Interval::new(Timestamp(0), Timestamp(50))?),
        ] {
            write.append(relation.table(owner), StoredEvent {
                position: None, origin: None, commit_position: 0, recency: None,
                entity_key: Some(id.handle_bytes().into()), iid: id.iid(),
                system_from: key.system_time, valid_time,
                operation: StoredOperation::Put(StructFields::from([
                    ("_id".into(), id.as_value()), ("location".into(), Value::Binary(vec![location])),
                ])),
            })?;
        }
        state.apply(&write.stage()?)?;

        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let database = DatabaseSnapshot { partitions: vec![state.snapshot()], storage };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false)?;
        let context = QueryContext::new(catalog, Arc::clone(&memory), key.system_time, "UTC",
            QueryOptions::default(), &CancellationToken::new())?;
        let projection = OwnedSpatialProjection::SelectedDocuments(&["location"]);
        let open = |iids, projection| open_owned_spatial_refinement(
            Arc::clone(&context), "alice", &relation, None, &[], key.system_time, iids, projection, true,
        );
        let empty = CandidateIids::from_sorted(&[], &memory)?;
        assert!(open(empty.clone(), OwnedSpatialProjection::Identities).await?.is_none());
        let empty = open(empty, projection).await?.unwrap();
        let mut cursor = empty.cursor;
        assert!(cursor.next_batch().await?.is_none());
        let empty_schema = cursor.runtime_schema().clone();
        assert_eq!(empty.document_columns.iter().map(|&index| empty_schema.fields()[index].name.as_str())
            .collect::<Vec<_>>(), ["_id", "location", "_system_from"]);

        for count in [1, 4096u128] {
            let sorted = (0..(count - 1)).map(|index| Iid(index.to_be_bytes()))
                .chain([selected.iid()]).collect::<Vec<_>>();
            let iids = CandidateIids::from_sorted(&sorted, &memory)?;
            let mut scan = open(iids, projection).await?.unwrap();
            assert_eq!(scan.cursor.runtime_schema(), &empty_schema);
            let page = scan.cursor.next_batch().await?.unwrap();
            let batch = page.batch();
            assert_eq!(batch.num_rows(), 1);
            let physical_iids = IidView::new(batch.column_by_name("_iid").unwrap().as_ref())?;
            assert_eq!(physical_iids.get(0)?, selected.iid());
            let (index, field) = batch.schema_ref().column_with_name("location").unwrap();
            assert_eq!(rtdb_columnar::binary_cell(field, batch.column(index).as_ref(), 0)?, Some(&[1][..]));
            assert!(scan.cursor.next_batch().await?.is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn fts_arrow_scan_keeps_shared_queries_and_candidate_vectors_between_accelerators() {
        use crate::{CandidateIids, ScanAccelerator, ScanRequest, ScanSelection};

        struct Select(Iid);
        #[async_trait]
        impl ScanAccelerator for Select {
            async fn candidate_iids(&self, _: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
                Ok(Some([self.0].into()))
            }
        }

        struct Verify(Iid, Expr);
        #[async_trait]
        impl ScanAccelerator for Verify {
            fn accepts_arrow_iids(&self) -> bool { true }

            async fn candidate_iids(&self, _: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> { Ok(None) }

            async fn select_candidates(&self, request: &ScanRequest<'_>) -> Result<Option<ScanSelection>> {
                let scope = request.candidate_iids.expect("the previous accelerator selected Arrow candidates");
                assert!(request.scan.iids.is_none());
                assert_eq!(scope.iter().collect::<Vec<_>>(), [self.0]);
                let first = request.constants.search_query(&self.1, rtdb_text::Analyzer::Simple).await?.unwrap();
                let second = request.constants.search_query(&self.1, rtdb_text::Analyzer::Simple).await?.unwrap();
                assert!(Arc::ptr_eq(&first, &second));
                assert!(Arc::ptr_eq(first.scorer(), second.scorer()));
                Ok(Some(ScanSelection { iids: CandidateIids::from_sorted(&[self.0], request.memory)?, scores: None }))
            }

            async fn match_answers_prepared(&self, request: &ScanRequest<'_>, _: &str, query: &PreparedSearchQuery) -> Result<Option<crate::MatchAnswers>> {
                let prepared = request.constants.search_query(&self.1, rtdb_text::Analyzer::Simple).await?.unwrap();
                assert!(std::ptr::eq(query, prepared.as_ref()));
                assert!(request.iid_scope().unwrap().contains(&self.0));
                Ok(None)
            }
        }

        let (state, table, events) = live_table();
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (mut scan, mut execution, _) = descriptor_scan(state.value().snapshot(), table, storage);
        let expression = Expr::Literal(Value::Utf8("v".into()));
        scan.matches.push(rtdb_plan::ScanMatch { id: ColumnId(99), column: "v".into(), query: expression.clone(), analyzer: rtdb_text::Analyzer::Simple });
        let matched = events[1].iid;
        let accelerators: Arc<[Arc<dyn ScanAccelerator>]> = Arc::from([Arc::new(Select(matched)) as Arc<dyn ScanAccelerator>, Arc::new(Verify(matched, expression))]);
        execution.context = execution.context.clone().with_accelerators(accelerators);
        let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(page.len, 1);
        assert_eq!(page.columns[0].value(0).unwrap(), Value::I64(2));
        assert!(cursor.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn arrow_visibility_uses_the_candidate_cut_and_sql_predicate() {
        use crate::{CandidateIids, CandidateVerifier};

        let (state, table, events) = live_table();
        let snapshot = state.value().snapshot();
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (mut scan, execution, memory) = descriptor_scan(snapshot.clone(), table, Arc::clone(&storage));
        scan.predicates.push(Expr::binary(rtdb_plan::BinaryOp::Gt, scan.columns[0].expression(), Expr::Literal(Value::I64(1))));
        let search_queries = SearchQueries::default();
        let constants = ExecutionConstants { execution: &execution, search_queries: &search_queries };
        let verifier = ScanVerifier { scan: &scan, execution: &execution, constants: &constants, pushdowns: None,
            partition: &snapshot, storage: &storage, canonical_entity_iids: false, counts: Default::default(), ranked_projection_memory: None };
        let unavailable = Iid([0xc0; 16]);
        let candidates = CandidateIids::from_sorted(&[events[0].iid, events[1].iid, unavailable], &memory).unwrap();
        let visible = verifier.retain_visible(&candidates).await.unwrap().unwrap();
        assert_eq!(visible.iter().collect::<Vec<_>>(), [events[1].iid]);
        assert_eq!(candidates.iter().collect::<Vec<_>>(), [events[0].iid, events[1].iid, unavailable]);
    }

    #[test]
    fn ranked_integer_ids_follow_execution_types_and_selected_offsets() {
        use crate::columnar::{Column, SelectedColumn};

        let values = [Value::U8(u8::MAX), Value::Null, Value::I16(-20), Value::Utf8("other".into()), Value::Binary(vec![0; 16])];
        let logical = LogicalType::join_all(&values.iter().map(Value::logical_type).collect::<Vec<_>>());
        let field = Arc::new(rtdb_columnar::to_field("_id", &logical).unwrap());
        let array = rtdb_columnar::copy_array(&logical, &values).unwrap();
        let columns = Column::Selected(SelectedColumn::new(
            vec![Some((array, field, Some(logical), None)), None],
            Arc::new(vec![(0, 2), (0, 0), (0, 1), (0, 3), (0, 4), (1, 0)]),
        ));
        let actual = (0..6).map(|row| ranked_integer_id(&columns, row).unwrap()).collect::<Vec<_>>();
        assert_eq!(actual, [Some(-20), Some(-1), None, None, None, None]);

        let uri = LogicalType::Scalar(ScalarType::Uri);
        let unreadable = Column::Arrow {
            array: Arc::new(arrow_array::StringArray::from(vec!["not a URI"])),
            field: Arc::new(rtdb_columnar::to_field("_id", &uri).unwrap()),
            logical: Some(uri),
        };
        assert_eq!(ranked_integer_id(&unreadable, 0).unwrap_err(), unreadable.value(0).unwrap_err());
    }

    #[test]
    fn an_id_equality_or_disjunction_narrows_the_scan_only_when_every_value_names_known_iids() {
        let id = Field { id: ColumnId(0), name: "_id".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
        let table = rtdb_plan::TableBinding { name: QualifiedTable { database: "xtdb".into(), table: TableRef::new("public", "docs") }, oid: None, columns: Default::default() };
        let scan = |predicate: Expr| Scan { table: table.clone(), columns: vec![id.clone()], valid_time: TemporalFilter::Default, system_time: TemporalFilter::Default,
            clamp_valid_time: false, iids: None, predicates: vec![predicate], top_k: None, ordered_top_k: None, matches: Vec::new() };
        let equals = |value: Expr| Expr::binary(rtdb_plan::BinaryOp::Eq, id.expression(), value);
        let parameter = Expr::Parameter { index: 0, data_type: LogicalType::Scalar(ScalarType::Utf8) };
        let iid = |value: Value| value.entity_id().unwrap().iid();
        assert_eq!(id_selection(&scan(equals(Expr::Literal(Value::I64(7)))), rtdb_expr::Parameters::Values(&[])), Some([iid(Value::I64(7))].into()));
        let either = Expr::binary(rtdb_plan::BinaryOp::Or, equals(Expr::Literal(Value::I64(7))), Expr::binary(rtdb_plan::BinaryOp::Eq, parameter.clone(), id.expression()));
        assert_eq!(id_selection(&scan(either.clone()), rtdb_expr::Parameters::Values(&[Value::Utf8("seven".into())])), Some([iid(Value::I64(7)), iid(Value::Utf8("seven".into()))].into()));
        assert_eq!(id_selection(&scan(equals(Expr::Literal(Value::F64(7.0)))), rtdb_expr::Parameters::Values(&[])), None, "7.0 still matches the integer ID 7");
        assert_eq!(id_selection(&scan(either), rtdb_expr::Parameters::Values(&[Value::Null])), None);
        let range = Expr::binary(rtdb_plan::BinaryOp::Lt, id.expression(), Expr::Literal(Value::I64(7)));
        assert_eq!(id_selection(&scan(range), rtdb_expr::Parameters::Values(&[])), None);

        let any = scan(Expr::Call { name: "_id_eq_any".into(), args: vec![id.expression(), parameter],
            return_type: LogicalType::Scalar(ScalarType::Bool).maybe() });
        let select = |values| id_selection(&any, rtdb_expr::Parameters::Values(&[Value::list(values)]));
        assert_eq!(select(vec![Value::I64(7), Value::Null, Value::I64(7), Value::I64(9)]),
            Some([iid(Value::I64(7)), iid(Value::I64(9))].into()));
        assert_eq!(select(vec![Value::Null]), Some(BTreeSet::new()));
        assert_eq!(select(vec![Value::F64(7.0)]), None);
    }

    #[test]
    fn keyword_id_equality_includes_both_persisted_digests() {
        let id = Field { id: ColumnId(0), name: "_id".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::Keyword) };
        let literal = Expr::Literal(Value::Keyword(rtdb_types::Keyword::from_name("solo")));
        let table = rtdb_plan::TableBinding { name: QualifiedTable { database: "xtdb".into(), table: TableRef::new("public", "docs") }, oid: None, columns: Default::default() };
        let scan = Scan { table, columns: vec![id.clone()], valid_time: TemporalFilter::Default, system_time: TemporalFilter::Default,
            clamp_valid_time: false, iids: None, predicates: vec![Expr::binary(rtdb_plan::BinaryOp::Eq, id.expression(), literal)],
            top_k: None, ordered_top_k: None, matches: Vec::new() };
        let canonical: Iid = "e5c907a6fac1f49cb3f8599a43a7cbf9".parse().unwrap();
        let legacy: Iid = "cf1a1098ce21209264ea9942c920ee6a".parse().unwrap();
        assert_eq!(id_selection(&scan, rtdb_expr::Parameters::Values(&[])), Some([canonical, legacy].into()));

        let field = arrow_schema::Field::new("?_0", DataType::Utf8, false)
            .with_metadata([(rtdb_columnar::EXTENSION_NAME_KEY.into(), "xt/clj-keyword".into())].into());
        let array = arrow_array::StringArray::from(vec!["solo"]);
        let cell = rtdb_expr::ArrowParameterCell { field: &field, array: &array, row: 0 };
        assert_eq!(arrow_parameter_iids(cell), Some((legacy, Some(canonical))));
    }

    #[test]
    fn id_range_predicates_confine_a_scan_of_uuid_ids_to_an_inclusive_iid_interval() {
        let uuid_type = LogicalType::Scalar(ScalarType::Uuid);
        let field = |data_type: &LogicalType| Field { id: ColumnId(0), name: "_id".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: data_type.clone() };
        let other = Field { id: ColumnId(1), name: "v".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
        let table = rtdb_plan::TableBinding { name: QualifiedTable { database: "xtdb".into(), table: TableRef::new("public", "docs") }, oid: None, columns: Default::default() };
        let scan = |id_type: &LogicalType, predicates: Vec<Expr>| Scan { table: table.clone(), columns: vec![field(id_type), other.clone()], valid_time: TemporalFilter::Default, system_time: TemporalFilter::Default,
            clamp_valid_time: false, iids: None, predicates, top_k: None, ordered_top_k: None, matches: Vec::new() };
        let id = field(&uuid_type).expression();
        let at = |value: u128| Expr::Literal(Value::Uuid(value.to_be_bytes()));
        let compare = |op, value: u128| Expr::binary(op, id.clone(), at(value));
        let range = |lower: u128, upper: u128| Ok(Some((Iid(lower.to_be_bytes()), Iid(upper.to_be_bytes()))));
        let interval = |predicates: Vec<Expr>| id_interval(&scan(&uuid_type, predicates), rtdb_expr::Parameters::Values(&[]));
        use rtdb_plan::BinaryOp::{And, Eq, Gt, GtEq, Lt, LtEq, Or};

        assert_eq!(interval(vec![compare(GtEq, 10), compare(LtEq, 20)]), range(10, 20));
        assert_eq!(interval(vec![compare(Gt, 10), compare(Lt, 20)]), range(11, 19), "strict bounds exclude the bound itself");
        assert_eq!(interval(vec![Expr::binary(And, compare(GtEq, 10), compare(Lt, 20))]), range(10, 19), "conjuncts of one predicate");
        assert_eq!(interval(vec![compare(GtEq, 10)]), range(10, u128::MAX));
        assert_eq!(interval(vec![compare(Lt, 20)]), range(0, 19));
        assert_eq!(interval(vec![Expr::binary(Lt, at(10), id.clone()), Expr::binary(GtEq, at(20), id.clone())]), range(11, 20), "the bound on the left reverses the comparison");
        assert_eq!(interval(vec![Expr::between(id.clone(), at(10), at(20), false)]), range(10, 20));
        assert_eq!(interval(vec![compare(GtEq, 10), compare(GtEq, 15), compare(LtEq, 30), compare(Lt, 20)]), range(15, 19), "the tightest bounds win");
        assert_eq!(interval(vec![compare(GtEq, 10), Expr::binary(Eq, other.expression(), Expr::Literal(Value::I64(1)))]), range(10, u128::MAX), "other columns' conjuncts leave the interval alone");

        let parameter = |index| Expr::Parameter { index, data_type: uuid_type.clone() };
        let bound = scan(&uuid_type, vec![Expr::binary(GtEq, id.clone(), parameter(0)), Expr::binary(Lt, id.clone(), parameter(1))]);
        assert_eq!(id_interval(&bound, rtdb_expr::Parameters::Values(&[Value::Uuid(10u128.to_be_bytes()), Value::Uuid(20u128.to_be_bytes())])), range(10, 19));
        assert_eq!(id_interval(&bound, rtdb_expr::Parameters::Values(&[Value::Uuid(10u128.to_be_bytes()), Value::Null])), range(10, u128::MAX), "a bound that is no UUID confines nothing");
        assert_eq!(id_interval(&bound, rtdb_expr::Parameters::Values(&[Value::Utf8("a".into()), Value::I64(7)])), Ok(None));

        let typed = |text: &str| Expr::Cast { expr: Box::new(Expr::Literal(Value::Utf8(text.into()))), target: rtdb_plan::CastTarget::Fixed(uuid_type.clone()) };
        assert_eq!(interval(vec![Expr::binary(GtEq, id.clone(), typed("00000000-0000-0000-0000-00000000000a")), Expr::binary(Lt, id.clone(), typed("00000000-0000-0000-0000-000000000014"))]), range(10, 19), "UUID '...' is a cast of its text");
        assert_eq!(interval(vec![Expr::binary(GtEq, id.clone(), typed("not a uuid"))]), Ok(None));
        assert_eq!(id_selection(&scan(&uuid_type, vec![Expr::binary(Eq, id.clone(), typed("00000000-0000-0000-0000-00000000000a"))]), rtdb_expr::Parameters::Values(&[])), Some([Iid(10u128.to_be_bytes())].into()));

        assert_eq!(interval(vec![compare(GtEq, 21), compare(LtEq, 20)]), Err(Unsatisfiable));
        assert_eq!(interval(vec![compare(GtEq, 20), compare(LtEq, 20)]), range(20, 20));
        assert_eq!(interval(vec![compare(Gt, u128::MAX)]), Err(Unsatisfiable));
        assert_eq!(interval(vec![compare(Lt, 0)]), Err(Unsatisfiable));
        assert_eq!(interval(vec![compare(GtEq, 0), compare(LtEq, u128::MAX)]), Ok(None), "every IID is no confinement");
        assert_eq!(interval(vec![Expr::binary(Or, compare(GtEq, 10), compare(LtEq, 5))]), Ok(None), "a disjunction is not an interval");
        assert_eq!(interval(vec![Expr::between(id.clone(), at(20), at(10), true)]), Ok(None), "a symmetric BETWEEN orders its own bounds");
        assert_eq!(interval(vec![Expr::unary(UnaryOp::Not, compare(GtEq, 10))]), Ok(None));

        // An ID of any other type is hashed into its IID, so its order says nothing about the IID's.
        for id_type in [LogicalType::Scalar(ScalarType::Utf8), LogicalType::Scalar(ScalarType::I64), LogicalType::join_all(&[uuid_type.clone(), LogicalType::Scalar(ScalarType::Utf8)])] {
            let id = field(&id_type).expression();
            assert_eq!(id_interval(&scan(&id_type, vec![Expr::binary(GtEq, id, at(10))]), rtdb_expr::Parameters::Values(&[])), Ok(None), "{id_type:?}");
        }

        assert_eq!(intersect(Some((Iid([1; 16]), Iid([5; 16]))), Some((Iid([3; 16]), Iid([9; 16])))), Ok(Some((Iid([3; 16]), Iid([5; 16])))));
        assert_eq!(intersect(Some((Iid([1; 16]), Iid([2; 16]))), Some((Iid([3; 16]), Iid([9; 16])))), Err(Unsatisfiable));
        assert_eq!(intersect(None, Some((Iid([3; 16]), Iid([9; 16])))), Ok(Some((Iid([3; 16]), Iid([9; 16])))));
        assert_eq!(intersect(None, None), Ok(None));
    }

    fn live_table() -> (PartitionState, TableRef, Vec<StoredEvent>) {
        live_table_with_values((1..=3).map(Value::I64))
    }

    fn live_table_with_values(values: impl IntoIterator<Item = Value>) -> (PartitionState, TableRef, Vec<StoredEvent>) {
        let state = PartitionState::empty(TrieConfig::new(1, 1).unwrap()).unwrap();
        let table = TableRef::new("public", "docs");
        let key = TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) };
        let memory = MemoryBudget::new(1 << 20);
        let mut write = TransactionBuffer::new(state.value().snapshot(), key, memory).unwrap();
        let events = [0x00, 0x40, 0x80].into_iter().zip(values).enumerate().map(|(index, (prefix, value))| {
            StoredEvent { position: None,
                origin: None,
                commit_position: 0,
                entity_key: None,
                iid: Iid([prefix; 16]),
                system_from: key.system_time,
                valid_time: Interval::ALL,
                operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(index as i64 + 1)), ("v".into(), value)])),
                recency: None,
            }
        }).collect::<Vec<_>>();
        for event in &events {
            write.append(table.clone(), event.clone()).unwrap();
        }
        state.apply(&write.stage().unwrap()).unwrap();
        (state, table, events)
    }

    fn descriptor_scan(snapshot: Snapshot, table: TableRef, storage: Arc<BufferPool>) -> (Scan, Execution, Arc<MemoryBudget>) {
        let database = DatabaseSnapshot { partitions: vec![snapshot], storage };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let binding = catalog.table(&QualifiedTable { database: "xtdb".into(), table }).unwrap().unwrap();
        let field = Field { id: ColumnId(0), name: "v".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: binding.columns["v"].clone() };
        let scan = Scan {
            table: binding, columns: vec![field], valid_time: TemporalFilter::All, system_time: TemporalFilter::All,
            clamp_valid_time: false, iids: None, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new(),
        };
        let memory = MemoryBudget::new(1 << 24);
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(100), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
        let execution = Execution {
            parameters: Arc::new(BoundParameters::infer(Vec::new(), &memory).unwrap()),
            outer: OuterRows::empty(&memory).unwrap(), context, bindings: None, analysis: None,
        };
        (scan, execution, memory)
    }

    #[tokio::test]
    async fn persisted_redaction_survives_payload_predicate_pruning() {
        let (state, table, events) = live_table();
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let captured = state.value();
        let put_fields = captured.live().tables()[&table].column_types().clone();
        let (mut scan, execution, _) = descriptor_scan(captured.snapshot(), table, storage);
        scan.predicates.push(Expr::unary(UnaryOp::IsNotNull, scan.columns[0].expression()));
        let cursor = ScanCursor::new(scan, execution, None).await.unwrap();
        let part = cursor.front_partition().unwrap();
        let test = crate::page_metadata::PageTest::for_scan(&part.scan, &part.execution).await.unwrap();

        for (operation, retained) in [(StoredOperation::Delete, false), (StoredOperation::Redacted, true)] {
            let id = EntityId::Integer(1);
            let event = StoredEvent { position: None,
                iid: id.iid(), entity_key: Some(id.handle_bytes().into()), commit_position: 1,
                origin: Some(rtdb_types::EventOrigin([9; 32])), operation, ..events[0].clone()
            };
            let page = build_event_batch(&[event], Some(put_fields.clone()), None).unwrap();
            let mut builder = MetadataBuilder::new(false, false);
            builder.write_leaf(&page).unwrap();
            let (metadata, _) = builder.finish().unwrap();
            let stored = MetadataFile::read(metadata.write().unwrap().into()).unwrap();
            let MetadataNode::Leaf { columns, .. } = &stored.nodes()[0] else { panic!("Expected a leaf"); };
            let columns = columns.statistics().unwrap();
            assert!(!may_satisfy_not_null(&part.scan, &columns));
            assert_eq!(part.leaf_matches(&columns, &test).unwrap(), retained);
        }
    }

    #[tokio::test]
    async fn a_covered_point_seeks_past_older_rows_in_the_live_merge() {
        let state = PartitionState::empty(TrieConfig::default()).unwrap();
        let table = TableRef::new("public", "docs");
        let memory = MemoryBudget::new(1 << 27);
        let first = Iid([0; 16]);
        let second = Iid([1; 16]);
        for (tx_id, (system_time, rows)) in [
            (Timestamp(99), vec![(first, 1); 2048]),
            (Timestamp(100), vec![(first, 2), (second, 3)]),
        ].into_iter().enumerate() {
            let mut write = TransactionBuffer::new(state.value().snapshot(),
                TransactionKey { tx_id: TxId(tx_id as i64), system_time }, Arc::clone(&memory)).unwrap();
            for (iid, value) in rows {
                write.append(table.clone(), StoredEvent { position: None,
                    origin: None, commit_position: 0, entity_key: None, iid, system_from: system_time, valid_time: Interval::ALL,
                    operation: StoredOperation::Put(StructFields::from([("v".into(), Value::I64(value))])), recency: None,
                }).unwrap();
            }
            state.apply(&write.stage().unwrap()).unwrap();
        }
        let snapshot = state.value().snapshot();
        let store = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (mut scan, execution, _) = descriptor_scan(snapshot, table, store);
        scan.system_time = TemporalFilter::Default;
        let limits = audience_work::Limits { provider_requests: 0, provider_bytes: 0, provider_metadata_bytes: 0, scan_items: 2300, decode_bytes: 0 };
        let mut values = audience_work::within(Some(limits), async {
            let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
            let mut values = Vec::new();
            while let Some(page) = cursor.next_page().await.unwrap() {
                values.extend(page.rows().iter().map(|row| row.values[0].clone()));
            }
            values
        }).await;
        values.sort_by_key(|value| match value { Value::I64(value) => *value, other => panic!("{other:?}") });
        assert_eq!(values, [Value::I64(2), Value::I64(3)]);
    }

    #[tokio::test]
    async fn prepared_receipt_uses_its_assigned_position_even_when_history_times_tie() {
        let state = PartitionState::empty(TrieConfig::default()).unwrap();
        let memory = MemoryBudget::new(1 << 24);
        let table = TableRef::new("public", rtdb_types::HOSTED_ROOT_RECEIPTS_TABLE);
        let id = EntityId::String("receipt".to_owned());
        let key = |position| TransactionKey { tx_id: TxId(position), system_time: Timestamp(10) };
        let stage = |position| {
            let mut prepare = TransactionBuffer::new(state.value().snapshot(), key(position), Arc::clone(&memory)).unwrap();
            prepare.put(table.clone(), StructFields::from([("_id".into(), id.as_value())]), Interval::ALL).unwrap();
            prepare.stage().unwrap()
        };
        state.apply(&stage(0)).unwrap();
        let mut ordered = TransactionBuffer::new(state.value().snapshot(), key(4), Arc::clone(&memory)).unwrap().stage().unwrap();
        assert!(ordered.install_prepared_generation(stage(3), &memory).is_err());
        ordered.install_prepared_generation(stage(4), &memory).unwrap();
        state.apply(&ordered).unwrap();

        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let context = QueryContext::new(catalog, memory, Timestamp(10), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
        let mut cursor = crate::owned_relation::open_owned_table_point(
            context, table, id.iid(), Some(&id), Some(Interval::ALL),
            Some(&[rtdb_types::HOSTED_RECEIPT_TX_ID]), false,
        ).await.unwrap().unwrap();
        let row = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(crate::owned_receipt_transaction(row.batch(), 0).unwrap(), TxId(4));
    }

    #[tokio::test]
    async fn tied_root_publications_keep_the_original_marker_at_a_position_cut() {
        let state = PartitionState::empty(TrieConfig::default()).unwrap();
        let relation = rtdb_types::OwnedRelation::new(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION).unwrap();
        let data = rtdb_types::OwnedRelation::new("docs").unwrap();
        let id = EntityId::String("head".into());
        let memory = MemoryBudget::new(1 << 24);
        let original = TransactionKey { tx_id: TxId(4), system_time: Timestamp(100) };
        let next = TransactionKey { tx_id: TxId(7), system_time: original.system_time };
        for key in [original, next] {
            let mut write = TransactionBuffer::new(state.value().snapshot(), key, Arc::clone(&memory)).unwrap();
            for relation in [&relation, &data] {
                write.append(relation.table("alice"), StoredEvent {
                    position: None, origin: None, commit_position: 0, recency: None,
                    entity_key: Some(id.handle_bytes().into()), iid: id.iid(),
                    system_from: key.system_time, valid_time: Interval::ALL,
                    operation: StoredOperation::Put(StructFields::from([("_id".into(), id.as_value())])),
                }).unwrap();
            }
            state.apply(&write.stage().unwrap()).unwrap();
        }
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let database = DatabaseSnapshot { partitions: vec![state.snapshot()], storage };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let cancellation = CancellationToken::new();
        let context = QueryContext::new(catalog, memory, original.system_time, "UTC", QueryOptions::default(), &cancellation).unwrap();
        assert_eq!(crate::read_owned_root_publication(Arc::clone(&context), "alice").await.unwrap(), Some(next));
        let context = context.with_owned_transaction_cut(original, &cancellation).unwrap();
        assert_eq!(crate::read_owned_root_publication(Arc::clone(&context), "alice").await.unwrap(), Some(original));
        let mut projection = crate::open_owned_root_projection(
            context, "alice", &data, original.system_time, &["_id", OWNED_EVENT_POSITION_COLUMN],
        ).await.unwrap().unwrap();
        let batch = projection.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.batch().num_rows(), 1);
        assert_eq!(crate::owned_row_event_position(batch.batch(), 0).unwrap().transaction, original.tx_id);
    }

    #[tokio::test]
    async fn world_clock_selects_an_earlier_reading_with_tied_history_time() {
        let state = PartitionState::empty(TrieConfig::default()).unwrap();
        let memory = MemoryBudget::new(1 << 24);
        let world = Iid([12; 16]);
        let id = EntityId::Uuid(world.0);
        let table = rtdb_types::OwnedRelation::world_clock_table("alice");
        let root = TransactionKey { tx_id: TxId(1), system_time: Timestamp(85) };
        let original = TransactionKey { tx_id: TxId(4), system_time: Timestamp(100) };
        let later = TransactionKey { tx_id: TxId(7), system_time: original.system_time };
        for (key, reading) in [(original, 200), (later, 300)] {
            let mut write = TransactionBuffer::new(state.value().snapshot(), key, Arc::clone(&memory)).unwrap();
            write.append(table.clone(), StoredEvent {
                position: None, origin: None, commit_position: 0, recency: None,
                entity_key: None, iid: world, system_from: key.system_time, valid_time: Interval::ALL,
                operation: StoredOperation::Put(StructFields::from([
                    ("_id".into(), id.as_value()), ("clock".into(), Value::I64(reading)),
                ])),
            }).unwrap();
            state.apply(&write.stage().unwrap()).unwrap();
        }
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let database = DatabaseSnapshot { partitions: vec![state.snapshot()], storage };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let cancellation = CancellationToken::new();
        let context = QueryContext::new(catalog, memory, later.system_time, "UTC", QueryOptions::default(), &cancellation).unwrap();
        for (requested, expected) in [
            (199, None),
            (200, Some((original, Timestamp(200)))),
            (299, Some((original, Timestamp(200)))),
            (300, Some((later, Timestamp(300)))),
        ] {
            let selected = crate::select_owned_world_clock(&context, "alice", world, root, Timestamp(150), Timestamp(requested))
                .await.unwrap();
            assert_eq!(selected, expected);
        }
        let original_view = context.with_owned_transaction_cut(original, &cancellation).unwrap();
        assert_eq!(crate::read_owned_world_clock_publication(original_view, "alice", world).await.unwrap(),
            Some((Timestamp(200), original)));
    }

    #[tokio::test]
    async fn arrow_search_ranked_verification_scores_the_resolved_payload() {
        use crate::CandidateVerifier;

        let (state, table, _) = live_table_with_values([Value::Utf8("open invoice".into())]);
        let snapshot = state.value().snapshot();
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (mut scan, execution, _) = descriptor_scan(snapshot.clone(), table.clone(), Arc::clone(&storage));
        let query = rtdb_text::Query::parse("invoice");
        scan.predicates.push(Expr::Call { name: "match".into(), args: vec![
            scan.columns[0].expression(), Expr::Literal(Value::Utf8("invoice".into())),
        ], return_type: LogicalType::Scalar(ScalarType::Bool).maybe() });
        let bounds = bounds(&scan, &snapshot, &execution).await.unwrap();
        let search_queries = SearchQueries::default();
        let constants = &ExecutionConstants { execution: &execution, search_queries: &search_queries };
        let verifier = ScanVerifier { scan: &scan, execution: &execution, constants, pushdowns: None, partition: &snapshot, storage: &storage,
            canonical_entity_iids: false, counts: Default::default(), ranked_projection_memory: None };
        let source = snapshot.iter_segments(&table).next().unwrap().row(0).unwrap();
        let version = source.header().unwrap().version();
        let row = crate::ResolvedSearchRow { version, origin: None, location: crate::SearchRowLocation::Live(source), visibility: bounds };
        let scorer = rtdb_text::Scorer::new(query.clone(), None);
        let expected = scorer.bm25_arrow(&rtdb_text::Analyzer::Simple.analyze("open invoice"));
        let result = verifier.verify_ranked(&BTreeSet::from([version.iid]), Some(vec![row]), ColumnId(0), None, &scorer)
            .await.unwrap().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].version(), version);
        assert_eq!(result[0].score().to_bits(), expected.to_bits());
    }

    #[tokio::test]
    async fn arrow_search_owned_candidate_cursor_scopes_full_keys_at_colliding_locators() {
        let state = PartitionState::empty(TrieConfig::default()).unwrap();
        let relation = rtdb_types::OwnedRelation::new("docs").unwrap();
        let table = relation.table("alice");
        let memory = MemoryBudget::new(1 << 24);
        let ids = [EntityId::Integer(1), EntityId::Integer(2), EntityId::Integer(3)];
        let colliding = EntityId::Uuid(ids[0].iid().0);
        let time = Timestamp(100);
        let mut write = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(0), system_time: time }, Arc::clone(&memory)).unwrap();
        for id in ids.iter().chain([&colliding]) {
            write.append(table.clone(), StoredEvent { position: None, origin: None, commit_position: 0,
                entity_key: Some(id.handle_bytes().into()), iid: id.iid(), system_from: time, valid_time: Interval::ALL,
                operation: StoredOperation::Put(StructFields::from([
                    ("_id".into(), id.as_value()), ("v".into(), Value::Utf8("invoice".into())),
                ])), recency: None }).unwrap();
        }
        state.apply(&write.stage().unwrap()).unwrap();
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let context = QueryContext::new(catalog, Arc::clone(&memory), time, "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
        let keys = [&ids[0], &ids[1]].map(|id| (id.iid(), arrow_buffer::Buffer::from_vec(id.handle_bytes())));
        let mut cursor = crate::open_owned_artefact_candidates(context, "alice", &relation, Interval::from_micros(25, 26).unwrap(), "v",
            keys).await.unwrap().unwrap();
        let mut found = BTreeSet::new();
        while let Some(batch) = cursor.next_batch().await.unwrap() {
            let batch = batch.batch();
            let (position, field) = batch.schema_ref().column_with_name("_id").unwrap();
            for row in 0..batch.num_rows() {
                let id = rtdb_columnar::read_entity_id(field, batch.column(position).as_ref(), row).unwrap();
                assert_eq!(crate::owned_row_revision_for(batch, row, id, &memory).unwrap(), time);
                found.insert(id.into_owned());
            }
        }
        assert_eq!(found, BTreeSet::from([ids[0].clone(), ids[1].clone()]));
    }

    #[tokio::test]
    async fn held_lifetime_lookup_requires_each_complete_key_and_uses_its_original_expiry() {
        use arrow_array::{ArrayRef, RecordBatch, StringArray};

        let state = PartitionState::empty(TrieConfig::default()).unwrap();
        let relation = rtdb_types::OwnedRelation::new("docs").unwrap();
        let table = relation.table("alice");
        let memory = MemoryBudget::new(1 << 24);
        let first = EntityId::String("first".into());
        let colliding = EntityId::Uuid(first.iid().0);
        let present = Timestamp(400);
        for (tx_id, (id, revision, created, expires)) in [
            (EntityId::String("malformed".into()), 250, 150, 1151),
            (EntityId::String("second".into()), 275, 200, 1200),
            (first, 350, 100, 1100),
            (colliding, present.0, 50, 1050),
        ].into_iter().enumerate() {
            let key = TransactionKey { tx_id: TxId(tx_id as i64), system_time: Timestamp(revision) };
            let mut write = TransactionBuffer::new(state.value().snapshot(), key, Arc::clone(&memory)).unwrap();
            write.append(table.clone(), StoredEvent {
                position: None, origin: None, commit_position: 0,
                entity_key: Some(id.handle_bytes().into()), iid: id.iid(), system_from: key.system_time,
                valid_time: Interval::from_micros(created, expires).unwrap(),
                operation: StoredOperation::Put(StructFields::from([("_id".into(), id.as_value())])), recency: None,
            }).unwrap();
            state.apply(&write.stage().unwrap()).unwrap();
        }
        let snapshot = state.value().snapshot().with_system_time_cap(Some(present)).unwrap();
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let database = DatabaseSnapshot { partitions: vec![snapshot], storage };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let context = QueryContext::new(catalog, memory, present, "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
        let page = |ids: [&str; 2]| {
            RecordBatch::try_from_iter([("_id", Arc::new(StringArray::from(Vec::from(ids))) as ArrayRef)]).unwrap()
        };
        let valid = Interval::at(present).unwrap();
        for (ids, expiry) in [
            (["second", "first"], Some(Timestamp(1100))),
            (["missing", "first"], None),
            (["first", "first"], None),
        ] {
            let expiry_for_page = crate::try_owned_lifetime_range_expiry(&context, "alice", &relation, &page(ids), valid, 1000).await.unwrap();
            assert_eq!(expiry_for_page, expiry);
        }
        let error = crate::try_owned_lifetime_range_expiry(&context, "alice", &relation, &page(["first", "malformed"]), valid, 1000)
            .await.unwrap_err();
        assert_eq!(error.code, "rtdb.owned/creation-lifetime");
    }

    #[tokio::test]
    async fn owned_position_points_and_windows_hide_only_the_selected_commit_suffix() {
        let state = PartitionState::empty(TrieConfig::default()).unwrap();
        let relation = rtdb_types::OwnedRelation::new("docs").unwrap();
        let table = relation.table("alice");
        let id = EntityId::Integer(1);
        let memory = MemoryBudget::new(1 << 24);
        let store = Arc::new(MemoryStore::default());
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        for (tx_id, time, start, values) in [
            (0, 90, 0, &[1][..]),
            (1, 100, 10, &[2, 3][..]),
            (2, 110, 20, &[4][..]),
        ] {
            let key = TransactionKey { tx_id: TxId(tx_id), system_time: Timestamp(time) };
            let mut write = TransactionBuffer::new(state.value().snapshot(), key, Arc::clone(&memory)).unwrap();
            if values.len() > 1 { write.retain_owned_positions().unwrap(); }
            for &value in values {
                write.append(table.clone(), StoredEvent { position: None,
                    origin: None,
                    commit_position: 0,
                    entity_key: Some(id.handle_bytes().into()),
                    iid: id.iid(),
                    system_from: key.system_time,
                    valid_time: Interval::from_micros(start, 30).unwrap(),
                    operation: StoredOperation::Put(StructFields::from([
                        ("_id".into(), Value::I64(1)),
                        ("v".into(), Value::I64(value)),
                        ("k".into(), Value::Binary(vec![u8::try_from(value).unwrap()])),
                    ])),
                    recency: None,
                }).unwrap();
            }
            state.apply(&write.stage().unwrap()).unwrap();
        }
        let snapshot = state.value().snapshot().with_system_time_cap(Some(Timestamp(110))).unwrap();
        let database = DatabaseSnapshot { partitions: vec![snapshot], storage };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let context = QueryContext::new(catalog, memory, Timestamp(110), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
        for (system, valid, first_masked, expected, ordinal) in [
            (100, 15, 3, 3, 2),
            (100, 15, 2, 2, 1),
            (100, 15, 0, 1, 0),
            (110, 15, 2, 2, 1),
            (110, 25, 2, 4, 0),
        ] {
            let mut result = crate::open_owned_point_with_position_mask(
                Arc::clone(&context), "alice", &relation, &id,
                Interval::at(Timestamp(valid)).unwrap(), Timestamp(system),
                (TransactionKey { tx_id: TxId(1), system_time: Timestamp(100) }, first_masked),
            ).await.unwrap().unwrap();
            let batch = result.next_batch().await.unwrap().unwrap();
            let batch = batch.batch();
            let (column, field) = batch.schema_ref().column_with_name("v").unwrap();
            assert_eq!(rtdb_columnar::read_value(field, batch.column(column).as_ref(), 0).unwrap(), Value::I64(expected));
            let (column, field) = batch.schema_ref().column_with_name("_assertion_ordinal").unwrap();
            assert_eq!(rtdb_columnar::read_value(field, batch.column(column).as_ref(), 0).unwrap(), Value::I64(ordinal));
            assert!(result.next_batch().await.unwrap().is_none());
        }
        for (system, expected) in [
            (100, vec![(5, 10, 1), (10, 28, 2)]),
            (110, vec![(5, 10, 1), (10, 20, 2), (20, 28, 4)]),
        ] {
            let mut result = crate::open_owned_valid_during_with_position_mask(
                Arc::clone(&context), "alice", &relation, &id,
                Interval::from_micros(5, 28).unwrap(), Timestamp(system),
                (TransactionKey { tx_id: TxId(1), system_time: Timestamp(100) }, 2),
            ).await.unwrap().unwrap();
            let mut slices = Vec::new();
            while let Some(page) = result.next_batch().await.unwrap() {
                let page = page.batch();
                let timestamp = |name: &str| page.column_by_name(name).unwrap().as_any()
                    .downcast_ref::<arrow_array::TimestampMicrosecondArray>().unwrap();
                let (column, field) = page.schema_ref().column_with_name("v").unwrap();
                for row in 0..page.num_rows() {
                    let Value::I64(value) = rtdb_columnar::read_value(field, page.column(column).as_ref(), row).unwrap()
                    else { panic!("Expected the winning fact value"); };
                    slices.push((timestamp("_valid_from").value(row), timestamp("_valid_to").value(row), value));
                }
            }
            assert_eq!(slices, expected);
        }
        for (key, mask, expected) in [
            (Some([2u8].as_slice()), None, vec![]),
            (Some([2u8].as_slice()), Some((TransactionKey { tx_id: TxId(1), system_time: Timestamp(100) }, 2)), vec![(10, 20, vec![2])]),
            (None, Some((TransactionKey { tx_id: TxId(1), system_time: Timestamp(100) }, 2)), vec![(5, 10, vec![1]), (10, 20, vec![2]), (20, 28, vec![4])]),
        ] {
            let mut cursor = crate::open_owned_unique_window(
                Arc::clone(&context), "alice", &relation, "k", key,
                Interval::from_micros(5, 28).unwrap(), Timestamp(110), mask, 4,
            ).await.unwrap().unwrap();
            let mut claims = Vec::new();
            while let Some(page) = cursor.next_batch().await.unwrap() {
                let page = page.batch();
                let stamp = |name: &str| page.column_by_name(name).unwrap().as_any()
                    .downcast_ref::<arrow_array::TimestampMicrosecondArray>().unwrap();
                let (column, field) = page.schema_ref().column_with_name("k").unwrap();
                for row in 0..page.num_rows() {
                    let Value::Binary(key) = rtdb_columnar::read_value(field, page.column(column).as_ref(), row).unwrap()
                    else { panic!("Expected a full Binary claim"); };
                    claims.push((stamp("_valid_from").value(row), stamp("_valid_to").value(row), key));
                }
            }
            claims.sort_unstable();
            assert_eq!(claims, expected);
        }
    }

    async fn assert_descriptor_lifetime(snapshot: Snapshot, table: TableRef, store: Arc<MemoryStore>, persisted: Option<usize>) {
        let metadata_charge = persisted.unwrap_or_default();
        let mut held = 0;
        let files_used = usize::from(persisted.is_some());
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (scan, execution, memory) = descriptor_scan(snapshot, table, Arc::clone(&storage));
        let before_cursor = memory.used_bytes();
        let mut cursor = ScanCursor::new(scan.clone(), execution.clone(), None).await.unwrap();
        let cursor_open = memory.used_bytes();
        let candidate_extent = if files_used == 0 { 0 } else { std::mem::size_of::<&CatalogTrie>() + 256 };
        let selected_extent = if files_used == 0 { 0 } else { std::mem::size_of::<usize>() + 256 };
        let file_extent = if files_used == 0 { 0 } else { std::mem::size_of::<DataFile>() + 256 };
        let stack_extent = if files_used == 0 { 0 } else { 4 * std::mem::size_of::<MetadataVisit>() + 256 };
        if files_used != 0 {
            let pressure = memory.reserve(memory.limit_bytes() - cursor_open - (candidate_extent - 1)).unwrap();
            let retained = cursor_open + pressure.bytes();
            let error = cursor.next_page().await.err().unwrap();
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            let expected = format!("Cannot reserve {candidate_extent} bytes from budget {} with {retained} retained", memory.limit_bytes());
            assert_eq!(error.message.as_deref(), Some(expected.as_str()));
            let partition = cursor.front_partition().unwrap();
            assert!(!partition.initialized);
            assert!(partition.files.entries.is_empty());
            assert!(partition.pages.entries.is_empty());
            assert!(partition.tasks.is_none());
            assert!(partition.loaded.is_empty());
            assert!(partition.output.is_empty());
            assert_eq!(storage.retained_bytes(), 0);
            assert_eq!(memory.used_bytes(), retained);
            drop(pressure);
            assert_eq!(memory.used_bytes(), cursor_open);

            let summary_extent = std::mem::size_of::<PageSummary>() + 256;
            let selector_extent = 3 * (std::mem::size_of::<&PageSummary>() + 256);
            for (requested, earlier) in [
                (summary_extent, candidate_extent),
                (selector_extent, candidate_extent + summary_extent),
                (selected_extent, candidate_extent + summary_extent + selector_extent),
            ] {
                let pressure = memory.reserve(memory.limit_bytes() - cursor_open - earlier - requested + 1).unwrap();
                let retained = cursor_open + pressure.bytes() + earlier;
                let error = cursor.next_page().await.err().unwrap();
                let expected = format!("Cannot reserve {requested} bytes from budget {} with {retained} retained", memory.limit_bytes());
                assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                assert_eq!(error.message.as_deref(), Some(expected.as_str()));
                let partition = cursor.front_partition().unwrap();
                assert!(!partition.initialized);
                assert!(partition.files.entries.is_empty());
                assert!(partition.pages.entries.is_empty());
                assert!(partition.tasks.is_none());
                assert_eq!(storage.retained_bytes(), 0);
                assert_eq!(memory.used_bytes(), cursor_open + pressure.bytes());
            }
            assert_eq!(memory.used_bytes(), cursor_open);

            let headroom = candidate_extent + selected_extent + metadata_charge + file_extent - 1;
            let pressure = memory.reserve(memory.limit_bytes() - cursor_open - headroom).unwrap();
            let retained = cursor_open + pressure.bytes() + candidate_extent + selected_extent + metadata_charge;
            let error = cursor.next_page().await.err().unwrap();
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            let expected = format!("Cannot reserve {file_extent} bytes from budget {} with {retained} retained", memory.limit_bytes());
            assert_eq!(error.message.as_deref(), Some(expected.as_str()));
            let partition = cursor.front_partition().unwrap();
            assert!(partition.files.entries.is_empty());
            assert!(partition.files._reservation.is_none());
            assert!(partition.tasks.is_none());
            assert!(partition.loaded.is_empty());
            held = storage.retained_bytes();
            assert!(held > 0, "the pool holds the decoded metadata file");
            assert_eq!(memory.used_bytes(), cursor_open + pressure.bytes());
            drop((cursor, pressure));
            assert_eq!(memory.used_bytes(), before_cursor);
            cursor = ScanCursor::new(scan.clone(), execution.clone(), None).await.unwrap();
            assert_eq!(memory.used_bytes(), cursor_open);

            let previous_stack = 2 * std::mem::size_of::<MetadataVisit>() + 256;
            let headroom = candidate_extent + selected_extent + metadata_charge + file_extent + previous_stack + stack_extent - 1;
            let pressure = memory.reserve(memory.limit_bytes() - cursor_open - headroom).unwrap();
            let retained = cursor_open + pressure.bytes() + candidate_extent + selected_extent + metadata_charge + file_extent + previous_stack;
            let error = cursor.next_page().await.err().unwrap();
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            let expected = format!("Cannot reserve {stack_extent} bytes from budget {} with {retained} retained", memory.limit_bytes());
            assert_eq!(error.message.as_deref(), Some(expected.as_str()));
            let partition = cursor.front_partition().unwrap();
            assert!(!partition.initialized);
            assert!(partition.files.entries.is_empty());
            assert!(partition.pages.entries.is_empty());
            assert!(partition.tasks.is_none());
            assert!(partition.loaded.is_empty());
            assert!(partition.output.is_empty());
            assert_eq!(storage.retained_bytes(), held);
            assert_eq!(memory.used_bytes(), cursor_open + pressure.bytes());
            drop(pressure);
            assert_eq!(memory.used_bytes(), cursor_open);
        }
        let old_extent = 2 * std::mem::size_of::<Page>() + 256;
        let replacement_extent = 4 * std::mem::size_of::<Page>() + 256;
        let headroom = candidate_extent + selected_extent + metadata_charge + file_extent + stack_extent + old_extent + replacement_extent - 1;
        let pressure = memory.reserve(memory.limit_bytes() - cursor_open - headroom).unwrap();
        let retained_at_growth = cursor_open + pressure.bytes() + candidate_extent + selected_extent + metadata_charge + file_extent + stack_extent + old_extent;
        let error = cursor.next_page().await.err().unwrap();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        let expected = format!("Cannot reserve {replacement_extent} bytes from budget {} with {retained_at_growth} retained", memory.limit_bytes());
        assert_eq!(error.message.as_deref(), Some(expected.as_str()));
        let partition = cursor.front_partition().unwrap();
        assert!(partition.pages.entries.is_empty());
        assert!(partition.pages._reservation.is_none());
        assert!(partition.tasks.is_none());
        assert!(partition.loaded.is_empty());
        assert!(partition.files.entries.is_empty());
        assert!(partition.files._reservation.is_none());
        assert_eq!(storage.retained_bytes(), held);
        assert_eq!(memory.used_bytes(), cursor_open + pressure.bytes());
        drop((cursor, pressure));
        assert_eq!(memory.used_bytes(), before_cursor);

        if files_used != 0 {
            let mut cursor = ScanCursor::new(scan.clone(), execution.clone(), None).await.unwrap();
            let partition = cursor.front_partition_mut().unwrap();
            partition.initialize().await.unwrap();
            let plan_memory = MemoryBudget::new(memory.limit_bytes());
            let plan = ScanTaskPlan::new(partition.pages.entries.iter().map(|page| PageRange { lower: page.lower, upper: page.upper }), false, &plan_memory).unwrap();
            assert_eq!(memory.used_bytes(), cursor_open + file_extent + replacement_extent + plan_memory.used_bytes());
            drop(plan);
            drop(partition.tasks.take().unwrap());
            assert_eq!(memory.used_bytes(), cursor_open + file_extent + replacement_extent);
            drop(cursor);
            assert_eq!(memory.used_bytes(), before_cursor);
        }

        let mut cursor = ScanCursor::new(scan, execution.clone(), None).await.unwrap();
        assert_eq!(memory.used_bytes(), cursor_open);
        let mut rows = Vec::new();
        while let Some(page) = cursor.next_page().await.unwrap() {
            let partition = cursor.front_partition().unwrap();
            assert_eq!(partition.files.entries.len(), files_used);
            assert_eq!(partition.files._reservation.as_ref().map_or(0, |lease| lease.bytes()), file_extent);
            let descriptors = &partition.pages;
            assert_eq!(descriptors.entries.len(), 3);
            assert_eq!(descriptors._reservation.as_ref().unwrap().bytes(), replacement_extent);
            rows.extend(page.rows().iter().map(|row| row.values[0].clone()));
        }
        assert_eq!(rows, [Value::I64(1), Value::I64(2), Value::I64(3)]);
        let counts = cursor.scan_counts().unwrap();
        assert_eq!(counts.files_used, files_used as u64);
        assert_eq!((counts.pages_used, counts.rows_read), (3, 3));
        assert!(cursor.parts.is_empty());
        assert_eq!(memory.used_bytes(), cursor_open);
        drop(cursor);
        assert_eq!(memory.used_bytes(), before_cursor);
        drop(execution);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[tokio::test]
    async fn live_page_descriptors_admit_growth_and_release_on_partition_close() {
        let (state, table, _) = live_table();
        let snapshot = state.value().snapshot();
        let segments = snapshot.segments(&table);
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].trie().leaves().len(), 3);
        assert!(snapshot.trie_entries(&table).is_empty());
        assert_descriptor_lifetime(snapshot, table, Arc::new(MemoryStore::default()), None).await;
    }

    async fn persisted_page_fixture() -> (PartitionState, TableRef, Vec<StoredEvent>, Arc<MemoryStore>, usize) {
        persisted_pages(false).await
    }

    #[tokio::test]
    async fn spatial_arrow_trie_fallback_visits_iids_and_counts_persisted_events() {
        let (state, table, events, store, _) = persisted_page_fixture().await;
        let snapshot = state.value().snapshot();
        let slug = snapshot.table_entry(&table).unwrap().slug.clone();
        let trie = snapshot.trie_entries(&table).into_iter().find(|trie| trie.key.level() == 0).unwrap().key.clone();
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (_, execution, _) = descriptor_scan(snapshot, table, Arc::clone(&storage));
        let context = execution.context.as_ref();
        let mut iids = Vec::new();
        let rows = visit_trie_iids(context, Arc::clone(&storage), &slug, &trie, 3, |iid| {
            iids.push(iid);
            Ok(())
        }).await.unwrap();
        assert_eq!(rows, Some(3));
        assert_eq!(iids, events.iter().map(|event| event.iid).collect::<Vec<_>>());
        assert_eq!(trie_row_count_up_to(context, Arc::clone(&storage), &slug, &trie, 3).await.unwrap(), Some(3));
        assert_eq!(trie_row_count_up_to(context, storage, &slug, &trie, 2).await.unwrap(), None);
    }

    #[tokio::test]
    async fn spatial_iid_nomination_admits_wide_pages_and_refuses_over_limit_before_reading_body() {
        let (state, table, mut events) = live_table();
        let captured = state.value();
        let snapshot = captured.snapshot();
        let slug = snapshot.table_entry(&table).unwrap().slug.clone();
        let trie = TrieKey::l0(captured.live().block_index()).unwrap();
        let key = ObjectKey::new(slug.data_key(&trie)).unwrap();
        for (index, event) in events.iter_mut().enumerate() {
            let StoredOperation::Put(document) = &mut event.operation else { panic!("Expected a put") };
            let payload = if index == 0 { "small".into() } else { "payload".repeat(64 << 10) };
            document.insert("v".into(), Value::Utf8(payload));
        }
        let fields = StructFields::from([
            ("_id".into(), LogicalType::Scalar(ScalarType::I64)),
            ("v".into(), LogicalType::Scalar(ScalarType::Utf8)),
        ]);
        let pages = [&events[..1], &events[1..]].map(|events| {
            build_event_batch(events, Some(fields.clone()), Some(rtdb_types::RunEndWidth::I32)).unwrap()
        });
        let data = write_file(pages[0].schema_ref(), &pages).unwrap();
        let store = Arc::new(MemoryStore::default());
        store.put(&key, data.into()).await.unwrap();
        let storage = Arc::new(BufferPool::new(store.clone(), 128 << 20));
        let (_, execution, _) = descriptor_scan(snapshot, table, Arc::clone(&storage));
        let mut context = execution.context.as_ref().clone();
        context.memory = MemoryBudget::new(256 << 10);
        let mut iids = Vec::new();
        let mut visit = |iid| { iids.push(iid); Ok(()) };

        let small_pool = Arc::new(BufferPool::new(store, 128 << 10));
        assert_eq!(visit_trie_iids(&context, Arc::clone(&small_pool), &slug, &trie, 2, &mut visit).await.unwrap(), None);
        assert_eq!(visit_trie_iids(&context, small_pool, &slug, &trie, 3, &mut visit).await.unwrap_err().code,
            "rtdb.storage/memory-limit");

        assert_eq!(visit_trie_iids(&context, Arc::clone(&storage), &slug, &trie, 3, &mut visit).await.unwrap(), Some(3));
        let mut reader = DataFileReader::new(&storage, key.clone());
        let warmup = MemoryBudget::new(16 << 20);
        let layout = Arc::clone(&reader.open(&warmup).await.unwrap().layout);
        assert!(layout.builder_bound.get().is_none());
        let builder_bound = layout.add_builder_bound(0).unwrap();
        for page in 0..2 {
            let (batch, charged) = reader.decode_scan_page(page, &warmup, Vec::new).await.unwrap();
            assert_eq!(layout.builder_bound.get(), Some(&Some(builder_bound)));
            let bound = charged.unwrap().bytes();
            if page == 1 { assert!(bound > context.memory.limit_bytes()); }
            assert!(storage.insert_view(&key, View::Page(page), Arc::new(CachedPage { batch, bound })));
        }

        assert_eq!(visit_trie_iids(&context, Arc::clone(&storage), &slug, &trie, 2, &mut visit).await.unwrap(), None);
        assert_eq!(visit_trie_iids(&context, storage, &slug, &trie, 3, &mut visit).await.unwrap(), Some(3));
        assert_eq!(iids, [0, 0, 0, 1, 2, 0, 0, 1, 2].map(|index| events[index].iid));
        assert_eq!(context.memory.used_bytes(), 0);
    }

    /// Persists one page per event; `misplaced` writes the second event's page in the first one's place too.
    async fn persisted_pages(misplaced: bool) -> (PartitionState, TableRef, Vec<StoredEvent>, Arc<MemoryStore>, usize) {
        let (state, table, events) = live_table();
        let captured = state.value();
        let table_entry = captured.table_catalog().entry(&table).unwrap();
        let trie = TrieKey::l0(captured.live().block_index()).unwrap();
        let put_fields = captured.live().tables()[&table].column_types().clone();
        let mut builder = MetadataBuilder::new(false, false);
        let mut children = [None; 4];
        let mut pages = Vec::new();
        for (slot, event) in children.iter_mut().zip(&events) {
            let page = build_event_batch(std::slice::from_ref(event), Some(put_fields.clone()), None).unwrap();
            *slot = Some(builder.write_leaf(&page).unwrap());
            pages.push(page);
        }
        builder.write_iid_branch(children).unwrap();
        let (metadata, statistics) = builder.finish().unwrap();
        let meta = metadata.write().unwrap();
        if misplaced { pages[0] = pages[1].clone(); }
        let data = write_file(pages[0].schema().as_ref(), &pages).unwrap();
        let probe = MemoryBudget::new(1 << 24);
        let layout = inspect_file_reserved(&mut Cursor::new(meta.as_slice()), |bytes| probe.reserve(bytes)).unwrap();
        let metadata_charge = layout.full_allocation_bound().unwrap();
        assert!(probe.peak_bytes() <= metadata_charge);
        assert_eq!(probe.used_bytes(), 0);

        let store = Arc::new(MemoryStore::default());
        let addition = CatalogTrie::notification(trie.clone(), data.len() as i64, Some(statistics)).unwrap();
        let nascent = CatalogTrie::notification("l01-r20200101-b00".parse().unwrap(), 0, None).unwrap();
        store.put(&ObjectKey::new(table_entry.slug.metadata_key(&trie)).unwrap(), meta.into()).await.unwrap();
        store.put(&ObjectKey::new(table_entry.slug.data_key(&trie)).unwrap(), data.into()).await.unwrap();
        state.add_tries(&BTreeMap::from([(table.clone(), vec![addition, nascent])]), Timestamp(200)).unwrap();
        (state, table, events, store, metadata_charge)
    }

    #[tokio::test]
    async fn cold_sparse_projection_skips_uncacheable_put_children_and_keeps_arrow_lists() {
        let values = (1..=3).map(|n| Value::Struct(StructFields::from([
            ("set".into(), Value::Set(vec![Value::I64(n)])),
            ("pair".into(), Value::FixedSizeList(vec![Value::I64(n), Value::I64(-n)].into())),
        ]))).collect::<Vec<_>>();
        let (state, table, mut events) = live_table_with_values(values.clone());
        for event in &mut events {
            let StoredOperation::Put(document) = &mut event.operation else { panic!("Expected a put") };
            document.insert("unselected".into(), Value::Utf8("x".repeat(128 << 10)));
        }
        let captured = state.value();
        let table_entry = captured.table_catalog().entry(&table).unwrap();
        let trie = TrieKey::l0(captured.live().block_index()).unwrap();
        let mut put_fields = captured.live().tables()[&table].column_types().clone();
        put_fields.insert("unselected".into(), LogicalType::Scalar(ScalarType::Utf8));
        let page = build_event_batch(&events, Some(put_fields), None).unwrap();
        let mut metadata = MetadataBuilder::new(false, false);
        metadata.write_leaf(&page).unwrap();
        let (metadata, statistics) = metadata.finish().unwrap();
        let data = write_file(page.schema().as_ref(), &[page]).unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let file = rtdb_format::ipc::inspect_file(&mut Cursor::new(data.as_slice())).unwrap();
        assert!(file.page_decoding_bounds[0] > memory.limit_bytes());
        let addition = CatalogTrie::notification(trie.clone(), data.len() as i64, Some(statistics)).unwrap();
        let store = Arc::new(MemoryStore::default());
        store.put(&ObjectKey::new(table_entry.slug.metadata_key(&trie)).unwrap(), metadata.write().unwrap().into()).await.unwrap();
        let key = ObjectKey::new(table_entry.slug.data_key(&trie)).unwrap();
        store.put(&key, data.into()).await.unwrap();
        let nascent = CatalogTrie::notification("l01-r20200101-b00".parse().unwrap(), 0, None).unwrap();
        state.add_tries(&BTreeMap::from([(table.clone(), vec![addition, nascent])]), Timestamp(200)).unwrap();
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (mut scan, mut execution, _) = descriptor_scan(state.value().snapshot(), table, Arc::clone(&storage));
        Arc::make_mut(&mut execution.context).memory = Arc::clone(&memory);
        scan.iids = Some([events[0].iid, events[2].iid].into());
        let logical = scan.columns[0].data_type.clone();
        let field = rtdb_columnar::to_field("v", &logical).unwrap();
        let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();

        let batch = cursor.next_batch().await.unwrap().unwrap();
        let selected = &batch.columns[0];
        assert!(matches!(selected, crate::columnar::Column::Selected(_)));
        let (result, _scratch) = crate::columnar::relayout_output(selected, &logical, &field, &memory).unwrap().unwrap();

        assert_eq!(result.len(), 2);
        let structs = result.as_any().downcast_ref::<arrow_array::StructArray>().unwrap();
        assert!(structs.column(0).as_any().is::<arrow_array::ListArray>());
        assert!(structs.column(1).as_any().is::<arrow_array::FixedSizeListArray>());
        for (row, expected) in [&values[0], &values[2]].into_iter().enumerate() {
            assert_eq!(rtdb_columnar::read_value(&field, result.as_ref(), row).unwrap(), *expected);
        }
        assert!(cursor.next_batch().await.unwrap().is_none());
        let counts = cursor.scan_counts().unwrap();
        assert_eq!((counts.files_used, counts.pages_used, counts.rows_read), (1, 1, 3));
        assert!(storage.view::<CachedPage>(&key, View::Page(0)).is_none());
    }

    #[tokio::test]
    async fn fts_arrow_point_scan_gathers_the_selected_persisted_event() {
        use crate::{ResolvedSearchRow, ScanAccelerator, ScanRequest, SearchRowLocation};

        struct Point { trie: TrieKey, version: rtdb_types::RowVersion }
        #[async_trait]
        impl ScanAccelerator for Point {
            async fn candidate_iids(&self, _: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> { Ok(None) }

            async fn point_rows(&self, request: &ScanRequest<'_>, keys: &BTreeSet<Iid>) -> Result<Option<Vec<ResolvedSearchRow>>> {
                assert_eq!(keys.iter().copied().collect::<Vec<_>>(), [self.version.iid]);
                Ok(Some(vec![ResolvedSearchRow { version: self.version, origin: None, visibility: request.bounds,
                    location: SearchRowLocation::Persisted { trie: self.trie.clone(), page: 1, row: 0 } }]))
            }
        }

        let (state, table, events, store, _) = persisted_page_fixture().await;
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let snapshot = state.value().snapshot();
        let trie = snapshot.trie_catalog().iter_tries(&table).find(|trie| trie.state == TrieState::Live).unwrap().key.clone();
        let (mut scan, mut execution, _) = descriptor_scan(snapshot, table, storage);
        let version = events[1].header().version();
        scan.iids = Some([version.iid].into());
        scan.valid_time = TemporalFilter::Default;
        scan.system_time = TemporalFilter::Default;
        execution.context = execution.context.clone().with_accelerators(Arc::from([Arc::new(Point { trie, version }) as Arc<dyn ScanAccelerator>]));
        let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
        assert!(matches!(cursor.parts.front(), Some(Part::Resolved(_))));
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(page.len, 1);
        assert_eq!(page.columns[0].value(0).unwrap(), Value::I64(2));
    }

    struct ScanStore {
        inner: Arc<MemoryStore>,
        data: ObjectKey,
        ranged: bool,
        reads: std::sync::Mutex<Vec<Range<u64>>>,
    }

    #[async_trait]
    impl ObjectStore for ScanStore {
        fn supports_scan_ranges(&self, key: &ObjectKey) -> bool { key != &self.data || self.ranged }
        async fn head(&self, key: &ObjectKey) -> StorageResult<StoredObject> { self.inner.head(key).await }
        async fn get(&self, key: &ObjectKey) -> StorageResult<Bytes> { self.get_range(key, 0..self.head(key).await?.size).await }
        async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> StorageResult<Bytes> {
            if key == &self.data { self.reads.lock().unwrap().push(range.clone()); }
            self.inner.get_range(key, range).await
        }
        async fn put(&self, key: &ObjectKey, bytes: Bytes) -> StorageResult<()> { self.inner.put(key, bytes).await }
        async fn put_file(&self, key: &ObjectKey, path: &std::path::Path) -> StorageResult<()> { self.inner.put_file(key, path).await }
        async fn list(&self, directory: Option<&ObjectKey>) -> StorageResult<Vec<StoredObject>> { self.inner.list(directory).await }
        async fn delete(&self, key: &ObjectKey) -> StorageResult<()> { self.inner.delete(key).await }
    }

    #[tokio::test]
    async fn a_cold_point_scan_returns_arrow_after_reading_the_selected_page() {
        let (state, table, events, store, _) = persisted_page_fixture().await;
        let data = store.list(None).await.unwrap().into_iter()
            .find(|object| object.key.as_str().contains("/data/")).unwrap().key;
        let bytes = store.get(&data).await.unwrap();
        let tail = bytes.len() - 10;
        let footer = tail - arrow_ipc::reader::read_footer_length(bytes[tail..].try_into().unwrap()).unwrap();
        let decoder = PageDecoder::from_footer(&bytes[footer..tail], footer as u64, None).unwrap();
        for ranged in [true, false] {
            let store = Arc::new(ScanStore { inner: Arc::clone(&store), data: data.clone(), ranged, reads: Default::default() });
            let storage = Arc::new(BufferPool::new(store.clone(), 1 << 20));
            let (mut scan, execution, _) = descriptor_scan(state.value().snapshot(), table.clone(), storage);
            scan.iids = Some([events[1].iid].into());
            let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
            let page = cursor.next_batch().await.unwrap().unwrap();
            let [crate::columnar::Column::Arrow { array, .. }] = page.columns.as_slice() else {
                panic!("Expected a projected Arrow column");
            };
            let values = array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
            assert_eq!(values.values().as_ref(), &[2]);
            assert_eq!(page.len, 1);
            assert!(cursor.next_batch().await.unwrap().is_none());

            let expected = if ranged {
                vec![tail as u64..bytes.len() as u64, footer as u64..tail as u64, decoder.page_range(1).unwrap()]
            } else {
                vec![0..bytes.len() as u64]
            };
            assert_eq!(*store.reads.lock().unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn a_point_read_resolves_a_trie_without_table_history_from_its_selected_page() {
        let (state, table, events, store, _) = persisted_page_fixture().await;
        let value = state.value();
        assert!(value.trie_catalog().iter_tries(&table).all(|trie| trie.artefact(rtdb_format::entity_history::KIND,
            rtdb_format::entity_history::ID, rtdb_format::entity_history::VERSION).is_none()));
        let (mut scan, execution, _) = descriptor_scan(state.value().snapshot(), table, Arc::new(BufferPool::new(store, 1 << 20)));
        let at = |micros| TemporalFilter::At(Expr::Literal(Value::Timestamp { value: micros, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) }));
        (scan.valid_time, scan.system_time) = (at(150), at(150));
        scan.iids = Some([events[1].iid].into());
        let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        let [crate::columnar::Column::Arrow { array, .. }] = page.columns.as_slice() else { panic!("Expected a projected Arrow column"); };
        assert_eq!(array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap().values().as_ref(), &[2]);
        assert!(cursor.next_batch().await.unwrap().is_none());
        let counts = cursor.scan_counts().unwrap();
        assert_eq!((counts.files_used, counts.pages_used, counts.rows_read), (0, 1, 1), "the point path reads one page and opens no scan");
    }

    #[tokio::test]
    async fn primary_points_keep_the_origins_locations_and_exact_keys_of_colliding_rows() {
        let (state, table, events, store, _) = persisted_page_fixture().await;
        let value = state.value();
        let entry = value.table_catalog().entry(&table).unwrap();
        let trie = TrieKey::l0(value.live().block_index()).unwrap();
        let origins = [0, 1].map(|row| rtdb_types::EventOrigin::at([1; 16], row, trie.block_index()));
        let mut first = events[0].clone();
        first.entity_key = Some(vec![1].into());
        first.origin = Some(origins[0]);
        let mut second = first.clone();
        second.entity_key = Some(vec![2].into());
        second.origin = Some(origins[1]);
        second.system_from = Timestamp(75);
        let puts = value.live().tables()[&table].column_types().clone();
        let page = build_event_batch(&[first, second], Some(puts), None).unwrap();
        let data = write_file(page.schema().as_ref(), &[page]).unwrap();
        let key = ObjectKey::new(entry.slug.data_key(&trie)).unwrap();
        store.delete(&key).await.unwrap();
        store.put(&key, data.into()).await.unwrap();
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let snapshot = value.snapshot();
        let (scan, execution, memory) = descriptor_scan(snapshot.clone(), table, storage.clone());
        let search_queries = SearchQueries::default();
        let constants = &ExecutionConstants { execution: &execution, search_queries: &search_queries };
        let verifier = ScanVerifier { scan: &scan, execution: &execution, constants, pushdowns: None, partition: &snapshot,
            storage: &storage, canonical_entity_iids: false, counts: Default::default(), ranked_projection_memory: None };
        let point = Interval::at(Timestamp(150)).unwrap();
        let request = crate::ScanRequest { database: "xtdb", scan: &scan, candidate_iids: None, snapshot: &snapshot, storage: &storage,
            bounds: TemporalBounds { valid_time: point, system_time: point }, memory: &memory,
            current_time: execution.context.current_time, cancellation: &execution.context.cancellation,
            constants, verifier: &verifier, partitions: std::slice::from_ref(&snapshot) };
        let keys = [events[0].iid].into();
        let mut recorded = Vec::new();
        assert_eq!(primary_point_events(&request, &entry.slug, &trie, Some(crate::IidScope::Set(&keys)), usize::MAX, |event, key| {
            let crate::SearchRowLocation::Persisted { trie: actual, page, row } = event.location else {
                panic!("Expected a persisted point location");
            };
            assert_eq!(actual, trie);
            recorded.push((page, row, event.origin, key.unwrap().to_vec()));
            Ok(true)
        }).await.unwrap(), Some(2));
        assert_eq!(recorded, [(0, 0, Some(origins[0]), vec![1]), (0, 1, Some(origins[1]), vec![2])]);
        assert!(crate::table_point_rows(&request, &keys).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn point_and_gap_reads_surface_a_missing_data_file_instead_of_declining() {
        let (state, table, events, store, _) = persisted_page_fixture().await;
        let value = state.value();
        let entry = value.table_catalog().entry(&table).unwrap();
        let trie = TrieKey::l0(value.live().block_index()).unwrap();
        store.delete(&ObjectKey::new(entry.slug.data_key(&trie)).unwrap()).await.unwrap();
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let snapshot = value.snapshot();
        let (scan, execution, memory) = descriptor_scan(snapshot.clone(), table, storage.clone());
        let search_queries = SearchQueries::default();
        let constants = &ExecutionConstants { execution: &execution, search_queries: &search_queries };
        let verifier = ScanVerifier { scan: &scan, execution: &execution, constants, pushdowns: None, partition: &snapshot,
            storage: &storage, canonical_entity_iids: false, counts: Default::default(), ranked_projection_memory: None };
        let point = Interval::at(Timestamp(150)).unwrap();
        let mut request = crate::ScanRequest { database: "xtdb", scan: &scan, candidate_iids: None, snapshot: &snapshot, storage: &storage,
            bounds: TemporalBounds { valid_time: point, system_time: point }, memory: &memory,
            current_time: execution.context.current_time, cancellation: &execution.context.cancellation,
            constants, verifier: &verifier, partitions: std::slice::from_ref(&snapshot) };
        let keys = [events[0].iid].into();
        assert!(crate::table_point_rows(&request, &keys).await.is_err(), "a missing data file must not read as an inapplicable fast path");
        request.bounds = TemporalBounds { valid_time: Interval::ALL, system_time: Interval::ALL };
        assert!(crate::uncovered_history_iids(&request, &BTreeSet::new(), None, 16).await.is_err());
    }

    #[tokio::test]
    async fn a_gap_read_charges_the_page_and_counts_filtered_primary_rows() {
        let (state, table, events, store, _) = persisted_page_fixture().await;
        let value = state.value();
        let entry = value.table_catalog().entry(&table).unwrap();
        let trie = TrieKey::l0(value.live().block_index()).unwrap();
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let snapshot = value.snapshot();
        let (scan, execution, memory) = descriptor_scan(snapshot.clone(), table, storage.clone());
        let search_queries = SearchQueries::default();
        let constants = &ExecutionConstants { execution: &execution, search_queries: &search_queries };
        let verifier = ScanVerifier { scan: &scan, execution: &execution, constants, pushdowns: None, partition: &snapshot,
            storage: &storage, canonical_entity_iids: false, counts: Default::default(), ranked_projection_memory: None };
        let request = crate::ScanRequest { database: "xtdb", scan: &scan, candidate_iids: None, snapshot: &snapshot, storage: &storage,
            bounds: TemporalBounds { valid_time: Interval::ALL, system_time: Interval::ALL }, memory: &memory,
            current_time: execution.context.current_time, cancellation: &execution.context.cancellation,
            constants, verifier: &verifier, partitions: std::slice::from_ref(&snapshot) };
        let before = memory.used_bytes();
        let mut visited = Vec::new();
        assert_eq!(primary_point_events(&request, &entry.slug, &trie, None, 2, |event, _| {
            assert!(memory.used_bytes() > before);
            if event.header.iid == events[0].iid { visited.push(event.header.iid); }
            Ok(true)
        }).await.unwrap(), None);
        assert_eq!(visited, [events[0].iid]);
        assert_eq!(memory.used_bytes(), before);
    }

    #[tokio::test]
    async fn gap_nomination_without_entity_history_declines_a_wide_first_page_before_reading_its_body() {
        let (state, table, events) = live_table_with_values([Value::Utf8("wide".into())]);
        let value = state.value();
        let entry = value.table_catalog().entry(&table).unwrap();
        let trie = TrieKey::l0(value.live().block_index()).unwrap();
        let put_fields = value.live().tables()[&table].column_types().clone();
        let mut event = events[0].clone();
        event.operation = StoredOperation::Put(StructFields::from([
            ("_id".into(), Value::I64(1)), ("v".into(), Value::Utf8("wide".repeat(16_384))),
        ]));
        let rows: Vec<_> = (0..33).rev().map(|row| StoredEvent { system_from: Timestamp(100 + row), ..event.clone() }).collect();
        let page = build_event_batch(&rows, Some(put_fields), None).unwrap();
        let mut metadata = MetadataBuilder::new(false, false);
        metadata.write_leaf(&page).unwrap();
        let (metadata, statistics) = metadata.finish().unwrap();
        let bytes = write_file(page.schema().as_ref(), &[page]).unwrap();
        let tail = bytes.len() - 10;
        let footer = tail - arrow_ipc::reader::read_footer_length(bytes[tail..].try_into().unwrap()).unwrap();
        let decoder = PageDecoder::from_footer(&bytes[footer..tail], footer as u64, None).unwrap();
        let expected = [tail as u64..bytes.len() as u64, footer as u64..tail as u64, decoder.page_metadata_range(0).unwrap()];
        let inner = Arc::new(MemoryStore::default());
        let data = ObjectKey::new(entry.slug.data_key(&trie)).unwrap();
        let addition = CatalogTrie::notification(trie.clone(), bytes.len() as i64, Some(statistics)).unwrap();
        inner.put(&data, bytes.into()).await.unwrap();
        inner.put(&ObjectKey::new(entry.slug.metadata_key(&trie)).unwrap(), metadata.write().unwrap().into()).await.unwrap();
        state.add_tries(&BTreeMap::from([(table.clone(), vec![addition])]), Timestamp(200)).unwrap();

        let store = Arc::new(ScanStore { inner, data, ranged: true, reads: Default::default() });
        let storage = Arc::new(BufferPool::new(store.clone(), 1 << 20));
        let snapshot = state.value().snapshot();
        let live_trie = snapshot.trie_catalog().trie(&table, &trie).unwrap();
        let (scan, execution, memory) = descriptor_scan(snapshot.clone(), table, storage.clone());
        let search_queries = SearchQueries::default();
        let constants = &ExecutionConstants { execution: &execution, search_queries: &search_queries };
        let verifier = ScanVerifier { scan: &scan, execution: &execution, constants, pushdowns: None, partition: &snapshot,
            storage: &storage, canonical_entity_iids: false, counts: Default::default(), ranked_projection_memory: None };
        let request = crate::ScanRequest { database: "xtdb", scan: &scan, candidate_iids: None, snapshot: &snapshot, storage: &storage,
            bounds: TemporalBounds { valid_time: Interval::ALL, system_time: Interval::ALL }, memory: &memory,
            current_time: execution.context.current_time, cancellation: &execution.context.cancellation,
            constants, verifier: &verifier, partitions: std::slice::from_ref(&snapshot) };

        let inspected = crate::entity_history::visit_trie_events(&request, &entry.slug, live_trie, None, 32, |_, _| {
            panic!("An over-limit page must never reach the visitor");
        }).await.unwrap();
        assert_eq!(inspected, None);
        assert_eq!(*store.reads.lock().unwrap(), expected);
    }

    #[tokio::test]
    async fn logical_gap_nomination_reads_compacted_rows_even_without_l0_descriptors() {
        use rtdb_format::{
            catalog::TableBlock,
            cow_tree::{self, Edit, NoSummary},
            entity_directory::{self, LogicalEvent},
            log::proto::{EntityDirectoryChange, EntityDirectoryRoot},
        };
        use rtdb_index::TrieCatalog;
        use rtdb_types::{BlockIndex, EventOrigin};
        use sha2::{Digest, Sha256};

        let (state, table, events) = live_table();
        let value = state.value();
        let slug = &value.table_catalog().entry(&table).unwrap().slug;
        let put_fields = value.live().tables()[&table].column_types().clone();
        let store = Arc::new(MemoryStore::default());
        let storage = Arc::new(BufferPool::new(store.clone(), 1 << 20));
        let newest = 11;
        let newest_rows = 40;
        let iid = |block| Iid([block as u8; 16]);
        let mut tries = Vec::new();
        let mut directory_rows = Vec::new();
        for block in 0..=newest {
            let trie = TrieKey::l0(BlockIndex(block)).unwrap();
            let count = if block == newest { newest_rows } else { 8 };
            let rows: Vec<_> = (0..count).rev().map(|row| StoredEvent {
                iid: iid(block),
                origin: Some(EventOrigin::at([block as u8; 16], row as u64, BlockIndex(block))),
                system_from: Timestamp(100 + block * 100 + row as i64),
                ..events[0].clone()
            }).collect();
            for (row, event) in rows.iter().enumerate() {
                directory_rows.push(LogicalEvent {
                    header: event.header(), origin: event.origin.unwrap(), entity_key: None,
                    payload_hint: Some((trie.clone(), 0, row as u32)),
                }.encode().unwrap());
            }
            let page = build_event_batch(&rows, Some(put_fields.clone()), None).unwrap();
            let mut metadata = MetadataBuilder::new(false, false);
            metadata.write_leaf(&page).unwrap();
            let (metadata, statistics) = metadata.finish().unwrap();
            let data = write_file(page.schema().as_ref(), &[page]).unwrap();
            tries.push(CatalogTrie::notification(trie.clone(), data.len() as i64, Some(statistics)).unwrap());
            store.put(&ObjectKey::new(slug.data_key(&trie)).unwrap(), data.into()).await.unwrap();
            store.put(&ObjectKey::new(slug.metadata_key(&trie)).unwrap(), metadata.write().unwrap().into()).await.unwrap();
        }
        let source_rows = directory_rows.len() as i64;
        directory_rows.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        let pages = crate::ObjectPageStore {
            store, storage: storage.clone(), prefix: entity_directory::prefix(slug),
        };
        let (root, _) = cow_tree::apply(&pages, None, directory_rows.into_iter().map(Edit::Put).collect(),
            &NoSummary, &MemoryBudget::new(1 << 24)).await.unwrap();
        let directory = EntityDirectoryChange {
            root: Some(EntityDirectoryRoot {
                root: Some(root.unwrap().encode()), through_block: Some(newest), source_rows: Some(source_rows),
                has_erases: Some(false), allocation: Some(vec![0; 16]), writer_fence: Some(0),
                erase_frontier_digest: Some(Sha256::digest([]).to_vec()),
            }),
            replaces_root: None,
        };
        let newest_trie = tries.last().unwrap().key.clone();
        state.add_tries(&BTreeMap::from([(table.clone(), tries)]), Timestamp(1400)).unwrap();
        state.add_artefacts(&table, vec![(newest_trie, directory.to_artefact().unwrap())]);

        let nominate = async |snapshot: &Snapshot, covered: &BTreeSet<BlockIndex>| {
            let (scan, execution, memory) = descriptor_scan(snapshot.clone(), table.clone(), storage.clone());
            let search_queries = SearchQueries::default();
            let constants = &ExecutionConstants { execution: &execution, search_queries: &search_queries };
            let verifier = ScanVerifier { scan: &scan, execution: &execution, constants, pushdowns: None, partition: snapshot,
                storage: &storage, canonical_entity_iids: false, counts: Default::default(), ranked_projection_memory: None };
            let request = crate::ScanRequest { database: "xtdb", scan: &scan, snapshot, storage: &storage,
                bounds: TemporalBounds { valid_time: Interval::ALL, system_time: Interval::ALL }, memory: &memory,
                current_time: execution.context.current_time, cancellation: &execution.context.cancellation,
                candidate_iids: None, constants, verifier: &verifier, partitions: std::slice::from_ref(snapshot) };
            crate::uncovered_history_iids(&request, covered, None, 2).await.unwrap()
        };
        let mut covered: BTreeSet<_> = (0..newest).map(BlockIndex).collect();
        let latest_page_work = audience_work::Limits {
            provider_requests: usize::MAX, provider_bytes: usize::MAX, provider_metadata_bytes: usize::MAX,
            scan_items: newest_rows + 1, decode_bytes: usize::MAX,
        };
        audience_work::within(Some(latest_page_work), async {
            assert_eq!(nominate(&state.value().snapshot(), &covered).await, Some(BTreeSet::from([iid(newest)])));
            assert!(audience_work::scan_items(1).is_err());
        }).await;

        let compacted = CatalogTrie::notification(TrieKey::new(1, None, vec![], BlockIndex(newest - 1)).unwrap(), 0, None).unwrap();
        state.add_tries(&BTreeMap::from([(table.clone(), vec![compacted])]), Timestamp(1500)).unwrap();
        let value = state.value();
        let snapshot = value.snapshot();
        audience_work::within(Some(latest_page_work), async {
            assert_eq!(nominate(&snapshot, &covered).await, Some(BTreeSet::from([iid(newest)])));
            assert!(audience_work::scan_items(1).is_err());
        }).await;

        let missing = BlockIndex(3);
        covered.remove(&missing);
        let expected = Some(BTreeSet::from([iid(missing.0), iid(newest)]));
        assert_eq!(nominate(&snapshot, &covered).await, expected);

        let catalog = value.trie_catalog();
        let mut partitions = catalog.partitions(&table).unwrap();
        let missing_key = TrieKey::l0(missing).unwrap();
        let missing_name = missing_key.to_string();
        for partition in &mut partitions {
            partition.tries.retain(|trie| trie.trie_key() != missing_name);
        }
        let block = TableBlock::from_parts(BTreeMap::new(), &StructFields::default(), source_rows, partitions).unwrap();
        let block = catalog.with_entity_directory(&table, block);
        let loaded = TrieCatalog::load(&BTreeMap::from([(table.clone(), block)]), catalog.file_size_target(), Timestamp(1500)).unwrap();
        assert!(loaded.trie(&table, &missing_key).is_none());
        let reloaded = Snapshot::new(Arc::new(loaded), value.table_catalog().clone(), value.live().clone());
        assert_eq!(nominate(&reloaded, &covered).await, expected);

        covered.insert(missing);
        audience_work::within(Some(latest_page_work), async {
            assert_eq!(nominate(&reloaded, &covered).await, Some(BTreeSet::from([iid(newest)])));
            assert!(audience_work::scan_items(1).is_err());
        }).await;
    }

    #[tokio::test]
    async fn stacked_filters_and_limit_keep_cold_scan_vectors() {
        use rtdb_plan::{BinaryOp, NamedExpr, Relation};

        let (state, table, _, store, _) = persisted_page_fixture().await;
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (scan, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let mut output = scan.columns[0].clone();
        let expression = output.expression();
        output.id = ColumnId(10);
        output.name = "payload".into();
        let relation = Arc::new(Relation::scan(scan).unwrap());
        let relation = Arc::new(relation.project(vec![NamedExpr { field: output.clone(), expr: expression }]).unwrap());
        let integer = |n| Expr::Literal(Value::I64(n));
        let relation = Arc::new(relation.filter(Expr::binary(BinaryOp::Gt, output.expression(), integer(0))).unwrap());
        let relation = Arc::new(relation.filter(Expr::binary(BinaryOp::Lt, output.expression(), integer(4))).unwrap());
        let relation = Arc::new(relation.limit(Some(integer(1)), Some(integer(1))).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
        let mut cursor = crate::operators::build_columns(runtime, execution).await.unwrap().expect("Expected a batch cursor");
        let page = cursor.next_batch().await.unwrap().unwrap();
        let [crate::columnar::Column::Arrow { array, .. }] = page.columns.as_slice() else {
            panic!("Expected the cold scan payload to stay in Arrow");
        };
        let values = array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(values.values().as_ref(), &[2]);
        assert!(cursor.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn computed_sort_key_keeps_payload_columns_through_arrow_spill() {
        use rtdb_plan::{BinaryOp, Relation, SortExpr};

        let (state, table, _, store, _) = persisted_page_fixture().await;
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (scan, mut execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let context = Arc::get_mut(&mut execution.context).unwrap();
        context.options.sort_memory_bytes = 1;
        context.options.batch_rows = 1;
        let value = scan.columns[0].expression();
        let sort_key = |expr| SortExpr { expr, descending: false, nulls_first: false };
        let key = sort_key(Expr::binary(BinaryOp::Subtract, Expr::Literal(Value::I64(0)), value.clone()));
        let first = sort_key(Expr::binary(BinaryOp::Subtract, value.clone(), value.clone()));
        let relation = Arc::new(Relation::scan(scan).unwrap());
        for order in [vec![key.clone()], vec![first, key, sort_key(value)]] {
            let relation = Arc::new(Arc::clone(&relation).sort(order).unwrap());
            let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
            let mut cursor = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected a batch sort");
            let mut values = Vec::new();
            while let Some(page) = cursor.next_batch().await.unwrap() {
                let [crate::columnar::Column::Arrow { array, .. }] = page.columns.as_slice() else {
                    panic!("Expected only the selected Arrow payload");
                };
                let array = array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
                values.extend(array.values().iter().copied());
            }
            assert_eq!(values, [3, 2, 1]);
        }
        assert!(execution.context.spill_peak_bytes() > 0);
    }

    #[tokio::test]
    async fn columnar_sort_limit_opens_aggregate_input_before_evaluating_fetch() {
        use rtdb_plan::{AggregateExpr, AggregateFunction, Relation, RelationNode, Schema, SortExpr};

        let (state, table, _, store, _) = persisted_page_fixture().await;
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (mut scan, mut execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        scan.valid_time = TemporalFilter::At(Expr::Literal(Value::Utf8("invalid instant".into())));
        execution.parameters = Arc::new(BoundParameters::infer(vec![Value::Utf8("invalid fetch".into())], &memory).unwrap());
        let field = Field { id: ColumnId(10), name: "n".into(), qualifier: None, qualified_only: false, unnamed: false,
            data_type: LogicalType::Scalar(ScalarType::I64) };
        let key = field.expression();
        let schema = Schema::new(vec![field.clone()]).unwrap();
        let input = Arc::new(Relation::scan(scan).unwrap());
        let input = Arc::new(Relation::new(schema.clone(), RelationNode::Aggregate {
            input, groups: vec![], aggregates: vec![AggregateExpr {
                field, function: AggregateFunction::CountStar, argument: None, distinct: false, filter: None, order_by: vec![],
            }],
        }).unwrap());
        let relation = Arc::new(Relation::new(schema, RelationNode::SortLimit {
            input, order_by: vec![SortExpr { expr: key, descending: false, nulls_first: false }],
            offset: None, fetch: Some(Expr::Parameter { index: 0, data_type: LogicalType::Scalar(ScalarType::I64) }),
        }).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, execution.parameters.types(), &memory).unwrap();
        let row = semantic_pages(Arc::clone(&runtime), execution.clone()).await.err().unwrap();
        let columnar = crate::operators::build_columns(runtime, execution).await.err().unwrap();
        assert_ne!(row.code, "xtdb/expected-number");
        assert_eq!(columnar.code, row.code);
    }

    #[tokio::test]
    async fn columnar_zero_fetch_with_offset_still_evaluates_the_order_key() {
        use rtdb_plan::{BinaryOp, Relation, RelationNode, SortExpr};

        let (state, table, _, store, _) = persisted_page_fixture().await;
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (scan, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let column = scan.columns[0].expression();
        let zero = Expr::binary(BinaryOp::Subtract, column.clone(), column);
        let key = Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), zero);
        let input = Arc::new(Relation::scan(scan).unwrap());
        let root = Arc::new(Relation::new(input.schema().clone(), RelationNode::SortLimit {
            input, order_by: vec![SortExpr { expr: key, descending: false, nulls_first: false }],
            offset: Some(Expr::Literal(Value::I64(1))), fetch: Some(Expr::Literal(Value::I64(0))),
        }).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(root, &execution.context.catalog, &[], &memory).unwrap();
        let mut row = semantic_pages(Arc::clone(&runtime), execution.clone()).await.unwrap();
        let mut columnar = crate::operators::build_columns(runtime, execution).await.unwrap().unwrap();
        let error = row.next_page().await.err().unwrap();
        assert_eq!(error.code, "xtdb.expression/division-by-zero");
        assert_eq!(columnar.next_batch().await.err().unwrap().code, error.code);
    }

    #[tokio::test]
    async fn multi_column_distinct_and_set_operations_select_arrow_rows() {
        use rtdb_plan::{BinaryOp, NamedExpr, Relation, RelationNode, SetOperator};

        let (state, table, _, store, _) = persisted_page_fixture().await;
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (scan, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let first = scan.columns[0].clone();
        let mut second = first.clone();
        second.id = ColumnId(10);
        second.name = "again".into();
        let project = Arc::new(Arc::new(Relation::scan(scan).unwrap()).project(vec![
            NamedExpr { field: first.clone(), expr: first.expression() },
            NamedExpr { field: second, expr: first.expression() },
        ]).unwrap());
        let set = |left: Arc<Relation>, right, operator, all| {
            Arc::new(Relation::new(left.schema().clone(), RelationNode::Set { left, right, operator, all }).unwrap())
        };
        let repeated = set(Arc::clone(&project), Arc::clone(&project), SetOperator::Union, true);
        let right = Arc::new(Arc::clone(&project).filter(Expr::binary(BinaryOp::Eq, first.expression(), Expr::Literal(Value::I64(2)))).unwrap());
        let distinct = Arc::new(Relation::new(repeated.schema().clone(), RelationNode::Distinct { input: Arc::clone(&repeated) }).unwrap());
        for (relation, expected) in [
            (distinct, vec![1, 2, 3]),
            (set(Arc::clone(&repeated), Arc::clone(&right), SetOperator::Union, false), vec![1, 2, 3]),
            (set(Arc::clone(&repeated), Arc::clone(&right), SetOperator::Intersect, true), vec![2]),
            (set(Arc::clone(&repeated), Arc::clone(&right), SetOperator::Except, true), vec![1, 3, 1, 2, 3]),
            (set(Arc::clone(&repeated), Arc::clone(&right), SetOperator::Except, false), vec![1, 3]),
        ] {
            let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
            let mut cursor = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected a set batch cursor");
            let mut actual = Vec::new();
            while let Some(page) = cursor.next_batch().await.unwrap() {
                let [crate::columnar::Column::Arrow { array: first, .. }, crate::columnar::Column::Arrow { array: second, .. }] = page.columns.as_slice() else {
                    panic!("Expected the set output to retain Arrow columns");
                };
                let first = first.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
                let second = second.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
                assert_eq!(first.values(), second.values());
                actual.extend(first.values().iter().copied());
            }
            assert_eq!(actual, expected);
        }
    }

    #[tokio::test]
    async fn inner_equi_join_of_cold_pages_keeps_multiple_key_columns_in_arrow() {
        use rtdb_plan::{BinaryOp, JoinKind, Relation};

        let (state, table, _, store, _) = persisted_page_fixture().await;
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (mut left, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let id = Field { id: ColumnId(1), name: "_id".into(), qualifier: None, qualified_only: false, unnamed: false,
            data_type: LogicalType::Scalar(ScalarType::I64) };
        left.columns.push(id);
        let mut right = left.clone();
        for field in &mut right.columns { field.id.0 += 10; }
        let key = |index: usize| Expr::binary(BinaryOp::Eq, left.columns[index].expression(), right.columns[index].expression());
        let condition = Expr::binary(BinaryOp::And, key(0), key(1));
        let relation = Arc::new(Relation::join(Arc::new(Relation::scan(left).unwrap()), Arc::new(Relation::scan(right).unwrap()),
            JoinKind::Inner, Some(condition)).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
        let mut cursor = crate::operators::build_columns(runtime, execution).await.unwrap().expect("Expected Arrow equijoin execution");
        let mut rows = Vec::new();
        while let Some(batch) = cursor.next_batch().await.unwrap() {
            assert!(batch.columns.iter().all(|column| !matches!(column, crate::columnar::Column::Values(_))));
            for row in 0..batch.len {
                rows.push(batch.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>());
            }
        }
        assert_eq!(rows, (1..=3).map(|value| vec![Value::I64(value); 4]).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn equijoin_spill_replays_arrow_partitions_in_the_original_order() {
        use rtdb_plan::{BinaryOp, JoinKind, Relation};

        let (state, table, _, store, _) = persisted_page_fixture().await;
        for spill_limit in [8192, 1] {
            let storage = Arc::new(BufferPool::new(store.clone(), 1 << 20));
            let (left, mut execution, memory) = descriptor_scan(state.value().snapshot(), table.clone(), storage);
            let context = Arc::get_mut(&mut execution.context).unwrap();
            context.options.join_build_rows = 1;
            context.spill = Arc::new(crate::spill::SpillRegion::new(spill_limit).unwrap());
            let mut right = left.clone();
            right.columns[0].id = ColumnId(10);
            let condition = Expr::binary(BinaryOp::Eq, left.columns[0].expression(), right.columns[0].expression());
            let relation = Arc::new(Relation::join(
                Arc::new(Relation::scan(left).unwrap()), Arc::new(Relation::scan(right).unwrap()),
                JoinKind::Inner, Some(condition),
            ).unwrap());
            let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
            if spill_limit == 1 {
                let mut cursor = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected Arrow join");
                assert_eq!(cursor.next_batch().await.err().unwrap().code, "rtdb.query/spill-limit");
                assert_eq!(execution.context.spill_bytes(), 0);
                continue;
            }
            let mut baseline = semantic_pages(Arc::clone(&runtime), execution.clone()).await.unwrap();
            let mut expected = Vec::new();
            while let Some(page) = baseline.next_page().await.unwrap() {
                expected.extend(page.rows().iter().map(|row| row.values.clone()));
            }
            drop(baseline);
            assert_eq!(execution.context.spill_bytes(), 0);
            let mut cursor = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected columnar join");
            let mut actual = Vec::new();
            while let Some(batch) = cursor.next_batch().await.unwrap() {
                assert!(batch.columns.iter().all(|column| !matches!(column, crate::columnar::Column::Values(_))));
                actual.extend((0..batch.len).map(|row| batch.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>()));
            }
            assert_eq!(actual, expected);
            assert_eq!(actual.len(), 3);
            for value in 1..=3 {
                assert!(actual.contains(&vec![Value::I64(value); 2]));
            }
            assert!(execution.context.spill_peak_bytes() > 0);
            assert_eq!(execution.context.spill_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn vector_payloads_keep_their_arrow_children_through_join_and_sort_spills() {
        use arrow_array::{Float32Array, Float64Array};
        use rtdb_plan::{BinaryOp, JoinKind, Relation, RelationNode, SortExpr};
        use rtdb_types::ListKind;

        let nan32 = f32::from_bits(0x7fc0_2357);
        let nan64 = f64::from_bits(0x7ff8_0000_0000_4321);
        let (state, table, _) = live_table_with_values([
            Value::FixedSizeList(vec![Value::F32(nan32), Value::F32(-0.0), Value::Null].into()),
            Value::Null,
            Value::list(vec![Value::F32(-0.0), Value::F64(nan64), Value::Null]),
        ]);
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (mut scan, mut execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let options = &mut Arc::get_mut(&mut execution.context).unwrap().options;
        options.join_build_rows = 0;
        options.sort_memory_bytes = 1;
        let id = Field { id: ColumnId(1), name: "_id".into(), qualifier: None, qualified_only: false, unnamed: false,
            data_type: LogicalType::Scalar(ScalarType::I64) };
        scan.columns.push(id.clone());
        let right_id = Field { id: ColumnId(2), ..id.clone() };
        let right = Arc::new(Relation::new(Schema::new(vec![right_id.clone()]).unwrap(), RelationNode::Values {
            rows: (1..=3).rev().map(|value| vec![Expr::Literal(Value::I64(value))]).collect(),
        }).unwrap());
        let condition = Expr::binary(BinaryOp::Eq, id.expression(), right_id.expression());
        let join = Arc::new(Relation::join(Arc::new(Relation::scan(scan).unwrap()), right, JoinKind::Inner, Some(condition)).unwrap());
        let plan = Arc::new(join.sort(vec![SortExpr { expr: id.expression(), descending: false, nulls_first: false }]).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(plan, &execution.context.catalog, &[], &memory).unwrap();
        let baseline = memory.used_bytes();
        let mut output = crate::operators::build_columns(runtime.clone(), execution.clone()).await.unwrap().unwrap();
        let mut seen = 0;
        while let Some(batch) = output.next_batch().await.unwrap() {
            for row in 0..batch.len {
                seen += 1;
                assert_eq!(batch.columns[1].value(row).unwrap(), Value::I64(seen));
                let cell = batch.columns[0].expression_cell(row).expect("Expected a vector or a null at an Arrow position");
                if seen == 2 {
                    assert!(cell.is_null().unwrap());
                    continue;
                }
                assert_eq!(cell.sequence_kind(), Some(if seen == 1 { ListKind::FixedSizeList(3) } else { ListKind::List }));
                let [first, second, last] = cell.sequence().unwrap().map(|cell| cell.physical().unwrap())
                    .collect::<Vec<_>>().try_into().unwrap_or_else(|_| panic!("Expected three vector components"));
                let first_bits = first.array.as_any().downcast_ref::<Float32Array>().unwrap().value(first.row).to_bits();
                if seen == 1 {
                    assert_eq!(first_bits, nan32.to_bits());
                    let second = second.array.as_any().downcast_ref::<Float32Array>().unwrap().value(second.row);
                    assert_eq!(second.to_bits(), (-0.0f32).to_bits());
                } else {
                    assert_eq!(first_bits, (-0.0f32).to_bits());
                    let second = second.array.as_any().downcast_ref::<Float64Array>().unwrap().value(second.row);
                    assert_eq!(second.to_bits(), nan64.to_bits());
                }
                assert!(last.is_null().unwrap());
            }
        }
        assert_eq!(seen, 3);
        assert!(execution.context.spill_peak_bytes() > 0);
        assert_eq!(execution.context.spill_bytes(), 0);
        drop(output);
        assert_eq!(memory.used_bytes(), baseline);
    }

    #[tokio::test]
    async fn spilled_full_and_right_joins_keep_unmatched_arrow_positions() {
        use rtdb_plan::{BinaryOp, JoinKind, Relation};
        let (state, table, _, store, _) = persisted_page_fixture().await;
        for kind in [JoinKind::Full, JoinKind::Right] {
            let storage = Arc::new(BufferPool::new(store.clone(), 1 << 20));
            let (left, mut execution, memory) = descriptor_scan(state.value().snapshot(), table.clone(), storage);
            let context = Arc::get_mut(&mut execution.context).unwrap();
            context.options.join_build_rows = 1;
            let mut right = left.clone();
            right.columns[0].id = ColumnId(10);
            let key = Expr::binary(BinaryOp::Add, left.columns[0].expression(), Expr::Literal(Value::I64(1)));
            let condition = Expr::binary(BinaryOp::Eq, key, right.columns[0].expression());
            let relation = Arc::new(Relation::join(
                Arc::new(Relation::scan(left).unwrap()), Arc::new(Relation::scan(right).unwrap()), kind, Some(condition),
            ).unwrap());
            let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
            let mut baseline = semantic_pages(Arc::clone(&runtime), execution.clone()).await.unwrap();
            let mut expected = Vec::new();
            while let Some(page) = baseline.next_page().await.unwrap() { expected.extend(page.rows().iter().map(|row| row.values.clone())); }
            drop(baseline);
            let mut columns = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected Arrow spill");
            let mut actual = Vec::new();
            while let Some(batch) = columns.next_batch().await.unwrap() {
                assert!(execution.context.spill_bytes() > 0);
                assert!(batch.columns.iter().any(|column| !matches!(column, crate::columnar::Column::Values(_))));
                actual.extend((0..batch.len).map(|row| batch.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>()));
            }
            assert_eq!(actual, expected);
            assert_eq!(actual.len(), if kind == JoinKind::Full { 4 } else { 3 });
            for row in [[Value::I64(1), Value::I64(2)], [Value::I64(2), Value::I64(3)], [Value::Null, Value::I64(1)]] {
                assert!(actual.contains(&row.to_vec()));
            }
            if kind == JoinKind::Full {
                assert!(actual.contains(&vec![Value::I64(3), Value::Null]));
            }
            assert_eq!(execution.context.spill_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn computed_projection_keeps_cold_scan_payloads_and_list_children_in_arrow() {
        use rtdb_plan::{NamedExpr, Relation};

        let (state, table, _, store, _) = persisted_page_fixture().await;
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (scan, mut execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        Arc::get_mut(&mut execution.context).unwrap().options.batch_rows = 1;
        let value = scan.columns[0].clone();
        let pair = Expr::List(vec![value.expression(), value.expression()]);
        let remainder = Expr::Call {
            name: "mod".into(), args: vec![value.expression(), Expr::Literal(Value::I64(2))],
            return_type: LogicalType::Scalar(ScalarType::I64),
        };
        assert!(crate::columnar::projection::shaped(&remainder));
        let named = |id, name: &str, expr: Expr| NamedExpr {
            field: Field { id: ColumnId(id), name: name.into(), data_type: expr.data_type(), ..value.clone() },
            expr,
        };
        let scan = Arc::new(Relation::scan(scan).unwrap());
        let relation = Arc::new(scan.project(vec![
            named(1, "source", value.expression()), named(2, "pair", pair), named(3, "remainder", remainder),
        ]).unwrap());
        let prepared = crate::PreparedQuery::new(Arc::clone(&relation)).unwrap();
        let mut output = prepared.execute_columns(Arc::clone(&execution.context), vec![]).await.unwrap();
        let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
        let mut baseline = semantic_pages(Arc::clone(&runtime), execution.clone()).await.unwrap();
        let mut cursor = crate::operators::build_columns(runtime, execution).await.unwrap().expect("Expected columnar projection");
        let mut observed = 0;
        while let Some(batch) = cursor.next_batch().await.unwrap() {
            assert!(matches!(batch.columns[0], crate::columnar::Column::Arrow { .. }));
            let crate::columnar::Column::Arrow { array, .. } = &batch.columns[1] else { panic!("Expected an Arrow list"); };
            assert!(array.as_any().is::<arrow_array::ListArray>());
            let crate::columnar::Column::Arrow { array, .. } = &batch.columns[2] else { panic!("Expected an Arrow remainder"); };
            assert!(array.as_any().is::<arrow_array::Int64Array>());
            let page = baseline.next_page().await.unwrap().unwrap();
            assert_eq!(page.len(), batch.len);
            for (row, expected) in page.rows().iter().enumerate() {
                let actual = batch.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>();
                assert_eq!(actual, expected.values);
                observed += 1;
            }
        }
        assert_eq!(observed, 3);
        assert!(baseline.next_page().await.unwrap().is_none());
        for _ in 0..observed {
            assert_eq!(output.next_batch().await.unwrap().unwrap().batch().num_rows(), 1);
        }
        assert!(output.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn semi_and_anti_joins_pick_probe_vectors_once_per_row() {
        use rtdb_plan::{BinaryOp, JoinKind, Relation};

        let (state, table, _) = live_table_with_values(vec![
            Value::I64(1), Value::I64(1), Value::Null,
        ]);
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (left, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let mut right = left.clone();
        right.columns[0].id = ColumnId(10);
        let left_value = left.columns[0].expression();
        let right_value = right.columns[0].expression();
        right.predicates.push(Expr::binary(BinaryOp::Eq, right_value.clone(), Expr::Literal(Value::I64(1))));
        let condition = Expr::binary(BinaryOp::Eq, left_value, right_value);
        let left = Arc::new(Relation::scan(left).unwrap());
        let right = Arc::new(Relation::scan(right).unwrap());
        for kind in [JoinKind::Semi, JoinKind::Anti] {
            let relation = Arc::new(Relation::join(Arc::clone(&left), Arc::clone(&right), kind, Some(condition.clone())).unwrap());
            let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
            let mut expected = semantic_pages(Arc::clone(&runtime), execution.clone()).await.unwrap();
            let mut cursor = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected columnar join");
            let mut actual = Vec::new();
            let mut baseline = Vec::new();
            while let Some(batch) = cursor.next_batch().await.unwrap() {
                assert!(matches!(batch.columns.as_slice(), [crate::columnar::Column::Arrow { .. } | crate::columnar::Column::Selected(_)]));
                actual.extend((0..batch.len).map(|row| batch.columns[0].value(row).unwrap()));
            }
            while let Some(page) = expected.next_page().await.unwrap() {
                baseline.extend(page.rows().iter().map(|row| row.values[0].clone()));
            }
            assert_eq!(actual, baseline);
            let values = match kind { JoinKind::Semi => vec![Value::I64(1); 2], _ => vec![Value::Null] };
            assert_eq!(actual, values);
        }
    }

    #[tokio::test]
    async fn computed_join_with_a_residual_expression_keeps_arrow_payloads() {
        use rtdb_plan::{BinaryOp, JoinKind, Relation};

        let (state, table, _) = live_table_with_values([Value::I64(1), Value::I64(2), Value::I64(3), Value::Null]);
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (left, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let mut right = left.clone();
        right.columns[0].id = ColumnId(10);
        let left_value = left.columns[0].expression();
        let right_value = right.columns[0].expression();
        let key = Expr::binary(BinaryOp::Add, left_value.clone(), Expr::Literal(Value::I64(1)));
        let equality = Expr::binary(BinaryOp::Eq, key, right_value.clone());
        let residual = Expr::binary(BinaryOp::Lt,
            Expr::binary(BinaryOp::Add, left_value, Expr::Literal(Value::I64(0))), right_value);
        let relation = Arc::new(Relation::join(
            Arc::new(Relation::scan(left).unwrap()), Arc::new(Relation::scan(right).unwrap()),
            JoinKind::Inner, Some(Expr::binary(BinaryOp::And, equality, residual)),
        ).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
        let mut rows = semantic_pages(Arc::clone(&runtime), execution.clone()).await.unwrap();
        let mut columns = crate::operators::build_columns(runtime, execution).await.unwrap().expect("Expected Arrow join");
        let mut expected = Vec::new();
        let mut actual = Vec::new();
        while let Some(page) = rows.next_page().await.unwrap() {
            expected.extend(page.rows().iter().map(|row| row.values.clone()));
        }
        while let Some(page) = columns.next_batch().await.unwrap() {
            assert_eq!(page.columns.len(), 2);
            assert!(page.columns.iter().all(|column| !matches!(column, crate::columnar::Column::Values(_))));
            actual.extend((0..page.len).map(|row| page.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>()));
        }
        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 2);
        for value in 1..=2 {
            assert!(actual.contains(&vec![Value::I64(value), Value::I64(value + 1)]));
        }
    }

    #[tokio::test]
    async fn ordered_list_and_percentile_aggregates_select_arrow_positions() {
        use rtdb_plan::{AggregateExpr, AggregateFunction, BinaryOp, NamedExpr, PercentileFraction, PercentileKind, Relation, RelationNode, Schema, SetOperator, SortExpr};
        let (state, table, _) = live_table_with_values([Value::I64(1), Value::I64(3), Value::I64(2)]);
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (scan, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let expression = scan.columns[0].expression();
        let value_field = scan.columns[0].clone();
        let mut order = value_field.clone();
        order.id = ColumnId(8);
        order.name = "order_key".into();
        order.data_type = LogicalType::Scalar(ScalarType::I64).maybe();
        let scan = Arc::new(Relation::scan(scan).unwrap());
        let schema = scan.schema().clone();
        let tail = Arc::new(Relation::new(schema.clone(), RelationNode::Values {
            rows: vec![vec![Expr::Literal(Value::I64(2))], vec![Expr::Literal(Value::Null)]],
        }).unwrap());
        let joined = Arc::new(Relation::new(schema, RelationNode::Set { left: scan, right: tail, operator: SetOperator::Union, all: true }).unwrap());
        let input = Arc::new(joined.project(vec![
            NamedExpr { field: value_field.clone(), expr: expression.clone() },
            NamedExpr { field: order.clone(), expr: Expr::binary(BinaryOp::Subtract, Expr::Literal(Value::I64(0)), expression.clone()) },
        ]).unwrap());
        let mut field = value_field;
        field.id = ColumnId(9);
        field.name = "collected".into();
        field.data_type = LogicalType::list(expression.data_type()).maybe();
        let mut discrete = field.clone();
        discrete.id = ColumnId(10);
        discrete.name = "discrete".into();
        discrete.data_type = expression.data_type().maybe();
        let mut continuous = discrete.clone();
        continuous.id = ColumnId(11);
        continuous.name = "continuous".into();
        continuous.data_type = LogicalType::Scalar(ScalarType::F64).maybe();
        let percentile = |field: rtdb_plan::Field, kind| AggregateExpr { field, function: AggregateFunction::Percentile {
            kind, fraction: PercentileFraction::Literal(0.75), descending: false,
        }, argument: Some(expression.clone()), distinct: false, filter: None, order_by: vec![] };
        let relation = Arc::new(Relation::new(Schema::new(vec![field.clone(), discrete.clone(), continuous.clone()]).unwrap(), RelationNode::Aggregate {
            input, groups: vec![],
            aggregates: vec![AggregateExpr {
                field, function: AggregateFunction::ArrayAgg, argument: Some(expression.clone()),
                distinct: true, filter: Some(Expr::binary(BinaryOp::Gt, expression.clone(), Expr::Literal(Value::I64(1)))),
                order_by: vec![SortExpr { expr: order.expression(), descending: false, nulls_first: false }],
            }, percentile(discrete, PercentileKind::Discrete), percentile(continuous, PercentileKind::Continuous)],
        }).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
        let mut rows = semantic_pages(Arc::clone(&runtime), execution.clone()).await.unwrap();
        let mut columns = crate::operators::build_columns(runtime, execution).await.unwrap().expect("Expected Arrow aggregate");
        let page = columns.next_batch().await.unwrap().unwrap();
        assert!(page.columns.iter().all(|column| !matches!(column, crate::columnar::Column::Values(_))));
        let actual = page.columns.iter().map(|column| column.value(0).unwrap()).collect::<Vec<_>>();
        assert_eq!(actual, [Value::list(vec![Value::I64(3), Value::I64(2)]), Value::I64(2), Value::F64(2.25)]);
        assert_eq!(actual, rows.next_page().await.unwrap().unwrap().rows()[0].values);
        assert!(columns.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn limit_zero_leaves_the_arrow_membership_build_unopened() {
        use rtdb_plan::{BinaryOp, Relation, RelationNode, Schema, TemporalFilter};
        let (state, table, _) = live_table_with_values([Value::I64(1)]);
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (probe, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let mut build = probe.clone();
        build.columns[0].id = ColumnId(10);
        build.valid_time = TemporalFilter::At(Expr::Literal(Value::Bool(true)));
        let comparison = Expr::binary(BinaryOp::Eq, probe.columns[0].expression(), build.columns[0].expression());
        let input = Arc::new(Relation::scan(probe).unwrap());
        let dependent = Arc::new(Relation::scan(build).unwrap());
        let output = rtdb_plan::Field { id: ColumnId(11), name: "member".into(), qualifier: None,
            qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::Bool).maybe() };
        let schema = input.schema().concat(&Schema::new(vec![output.clone()]).unwrap()).unwrap();
        let mark = Arc::new(Relation::new(schema, RelationNode::Mark { input, dependent, arguments: vec![],
            condition: None, comparison, output }).unwrap());
        let relation = Arc::new(mark.limit(None, Some(Expr::Literal(Value::I64(0)))).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
        let mut rows = semantic_pages(Arc::clone(&runtime), execution.clone()).await.unwrap();
        let mut columns = crate::operators::build_columns(runtime, execution).await.unwrap().expect("Expected Arrow membership");
        assert!(rows.next_page().await.unwrap().is_none());
        assert!(columns.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn uuid_groups_keep_their_first_arrow_key_positions() {
        use rtdb_plan::{AggregateExpr, AggregateFunction, NamedExpr, Relation, RelationNode, Schema};
        let one = Value::Uuid([1; 16]);
        let two = Value::Uuid([2; 16]);
        let (state, table, _) = live_table_with_values([two.clone(), one.clone(), two.clone()]);
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (scan, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let key = scan.columns[0].clone();
        let count = rtdb_plan::Field { id: ColumnId(8), name: "count".into(), qualifier: None,
            qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
        let relation = Arc::new(Relation::new(Schema::new(vec![key.clone(), count.clone()]).unwrap(), RelationNode::Aggregate {
            input: Arc::new(Relation::scan(scan).unwrap()), groups: vec![NamedExpr { field: key.clone(), expr: key.expression() }],
            aggregates: vec![AggregateExpr { field: count, function: AggregateFunction::CountStar, argument: None, distinct: false, filter: None, order_by: vec![] }],
        }).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
        let mut pages = semantic_pages(Arc::clone(&runtime), execution.clone()).await.unwrap();
        let mut columns = crate::operators::build_columns(runtime, execution).await.unwrap().expect("Expected Arrow grouping");
        let batch = columns.next_batch().await.unwrap().unwrap();
        assert!(batch.columns.iter().all(|column| !matches!(column, crate::columnar::Column::Values(_))));
        let actual = (0..batch.len).map(|row| batch.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>()).collect::<Vec<_>>();
        assert_eq!(actual, [vec![two, Value::I64(2)], vec![one, Value::I64(1)]]);
        assert_eq!(actual, pages.next_page().await.unwrap().unwrap().rows().iter().map(|row| row.values.clone()).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_projected_arrow_join_fills_flight_transport_batches() {
        use rtdb_plan::{BinaryOp, JoinKind, NamedExpr, Relation};

        let (state, table, _) = live_table_with_values([Value::I64(1), Value::I64(1), Value::I64(1)]);
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (left, mut execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        Arc::get_mut(&mut execution.context).unwrap().options.batch_rows = 2;
        let mut right = left.clone();
        right.columns[0].id = ColumnId(10);
        let field = left.columns[0].clone();
        let equality = Expr::binary(BinaryOp::Eq, field.expression(), right.columns[0].expression());
        let join = Arc::new(Relation::join(Arc::new(Relation::scan(left).unwrap()), Arc::new(Relation::scan(right).unwrap()),
            JoinKind::Inner, Some(equality)).unwrap());
        let relation = Arc::new(join.project(vec![NamedExpr { expr: field.expression(), field }]).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, &[], &memory).unwrap();
        assert!(crate::operators::build_columns(runtime, execution.clone()).await.unwrap().is_some());
        let prepared = crate::PreparedQuery::new(relation).unwrap();
        let mut cursor = prepared.execute_columns(execution.context, vec![]).await.unwrap();
        let mut batches = Vec::new();
        while let Some(batch) = cursor.next_batch().await.unwrap() { batches.push(batch.batch().num_rows()); }
        assert_eq!(batches, [2, 2, 2, 2, 1]);
    }

    #[tokio::test]
    async fn columnar_literal_unions_fill_the_transport_batch_before_publishing_rows() {
        use rtdb_plan::{NamedExpr, Relation, RelationNode, Schema, SetOperator};

        let (state, table, _) = live_table_with_values([Value::I64(1)]);
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (scan, mut execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        Arc::get_mut(&mut execution.context).unwrap().options.batch_rows = 2;
        let field = scan.columns[0].clone();
        let schema = Schema::new(vec![field.clone()]).unwrap();
        let values = |value| Arc::new(Relation::new(schema.clone(), RelationNode::Values {
            rows: vec![vec![Expr::Literal(Value::I64(value))]],
        }).unwrap());
        let union = |right| Arc::new(Relation::new(schema.clone(), RelationNode::Set {
            left: values(7), right, operator: SetOperator::Union, all: true,
        }).unwrap());
        let success = crate::PreparedQuery::new(union(values(8))).unwrap();
        let mut batches = success.execute_columns(Arc::clone(&execution.context), vec![]).await.unwrap();
        let batch = batches.next_batch().await.unwrap().unwrap();
        let values_out = batch.batch().column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(values_out.values().as_ref(), &[7, 8]);
        assert!(batches.next_batch().await.unwrap().is_none());

        let projection = Arc::new(values(0).project(vec![NamedExpr {
            field: field.clone(), expr: Expr::Call {
                name: "mod".into(), args: vec![Expr::Literal(Value::I64(5)), field.expression()],
                return_type: field.data_type.clone(),
            },
        }]).unwrap());
        let relation = union(projection);
        let runtime = crate::runtime_schema::EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, &[], &memory).unwrap();
        assert!(crate::operators::build_columns(runtime, execution.clone()).await.unwrap().is_some());
        let prepared = crate::PreparedQuery::new(relation).unwrap();
        let mut rows = prepared.execute(Arc::clone(&execution.context), vec![]).await.unwrap();
        let mut batches = prepared.execute_columns(execution.context, vec![]).await.unwrap();
        let expected = rows.next_batch().await.err().expect("The second row fails before the first batch is published");
        let actual = batches.next_batch().await.err().expect("The second row fails before the first batch is published");
        assert_eq!(actual.code, expected.code);
        assert!(batches.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn computed_scan_and_literal_rows_feed_a_struct_mutation_projection_as_vectors() {
        use rtdb_plan::{NamedExpr, Relation, RelationNode, Schema};

        let (state, table, _, store, _) = persisted_page_fixture().await;
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (scan, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let source = scan.columns[0].clone();
        let named = |id, name: &str, expr: Expr| NamedExpr {
            field: Field { id: ColumnId(id), name: name.into(), data_type: expr.data_type(), ..source.clone() },
            expr,
        };
        let remainder = Expr::Call {
            name: "mod".into(), args: vec![source.expression(), Expr::Literal(Value::I64(2))],
            return_type: LogicalType::Scalar(ScalarType::I64),
        };
        let literal = Relation::new(Schema::new(vec![source.clone()]).unwrap(), RelationNode::Values {
            rows: (1..=3).map(|value| vec![Expr::Literal(Value::I64(value))]).collect(),
        }).unwrap();
        for input in [Relation::scan(scan).unwrap(), literal] {
            let projected = Arc::new(Arc::new(input).project(vec![
                named(1, "source", source.expression()), named(2, "remainder", remainder.clone()),
            ]).unwrap());
            let iid = Expr::Call {
                name: "_iid".into(), args: vec![projected.schema().fields()[0].expression()],
                return_type: LogicalType::Scalar(ScalarType::FixedSizeBinary(16)),
            };
            let document = Expr::RowCopy(projected.schema().fields().to_vec());
            let relation = Arc::new(projected.project(vec![named(3, "_iid", iid), named(4, "doc", document)]).unwrap());
            let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
            let mut baseline = semantic_pages(Arc::clone(&runtime), execution.clone()).await.unwrap();
            let mut cursor = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected a columnar document projection");
            let mut seen = 0;
            while let Some(batch) = cursor.next_batch().await.unwrap() {
                assert!(matches!(batch.columns[0], crate::columnar::Column::Arrow { .. }));
                let crate::columnar::Column::Arrow { array, .. } = &batch.columns[1] else { panic!("Expected an Arrow document"); };
                let document = array.as_any().downcast_ref::<arrow_array::StructArray>().unwrap();
                assert_eq!(document.num_columns(), 2);
                assert!(document.columns().iter().all(|column| column.as_any().is::<arrow_array::Int64Array>()));
                let page = baseline.next_page().await.unwrap().unwrap();
                assert_eq!(page.len(), batch.len);
                for (row, expected) in page.rows().iter().enumerate() {
                    let actual = batch.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>();
                    assert_eq!(actual, expected.values);
                    seen += 1;
                }
            }
            assert_eq!(seen, 3);
            assert!(baseline.next_page().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn columnar_outer_joins_preserve_null_keys_unmatched_positions_and_empty_inputs() {
        use rtdb_plan::{BinaryOp, JoinKind, Relation};

        let (state, table, _) = live_table_with_values([Value::I64(1), Value::I64(2), Value::Null]);
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (mut left, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        left.columns.push(Field { id: ColumnId(1), name: "_id".into(), qualifier: None, qualified_only: false,
            unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) });
        let mut right = left.clone();
        for field in &mut right.columns { field.id.0 += 10; }
        let condition = Expr::binary(BinaryOp::Eq, left.columns[0].expression(), right.columns[0].expression());
        left.predicates.push(Expr::binary(BinaryOp::NotEq, left.columns[1].expression(), Expr::Literal(Value::I64(2))));
        right.predicates.push(Expr::binary(BinaryOp::NotEq, right.columns[1].expression(), Expr::Literal(Value::I64(1))));
        for (kind, empty_side) in [
            (JoinKind::Full, None), (JoinKind::Left, None), (JoinKind::Right, None), (JoinKind::Single, None),
            (JoinKind::Full, Some(0)), (JoinKind::Full, Some(1)),
        ] {
            let (mut left, mut right) = (left.clone(), right.clone());
            if let Some(side) = empty_side {
                let input = if side == 0 { &mut left } else { &mut right };
                input.predicates.push(Expr::Literal(Value::Bool(false)));
            }
            let plan = Arc::new(Relation::join(
                Arc::new(Relation::scan(left).unwrap()), Arc::new(Relation::scan(right).unwrap()), kind, Some(condition.clone()),
            ).unwrap());
            let runtime = crate::runtime_schema::EmittedRelation::derive(plan, &execution.context.catalog, &[], &memory).unwrap();
            let mut expected = semantic_pages(Arc::clone(&runtime), execution.clone()).await.unwrap();
            let mut cursor = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected columnar join");
            let mut actual = Vec::new();
            let mut baseline = Vec::new();
            while let Some(batch) = cursor.next_batch().await.unwrap() {
                assert!(batch.columns.iter().all(|column| !matches!(column, crate::columnar::Column::Values(_))));
                actual.extend((0..batch.len).map(|row| batch.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>()));
            }
            while let Some(page) = expected.next_page().await.unwrap() {
                baseline.extend(page.rows().iter().map(|row| row.values.clone()));
            }
            assert_eq!(actual, baseline, "{kind:?} with empty side {empty_side:?}");
            assert!(!actual.is_empty());
        }
    }

    #[tokio::test]
    async fn columnar_single_join_detects_duplicate_matches_before_emitting_a_page() {
        use rtdb_plan::{BinaryOp, JoinKind, Relation};

        let (state, table, _) = live_table_with_values([Value::I64(1), Value::I64(1)]);
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (left, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let mut right = left.clone();
        right.columns[0].id = ColumnId(10);
        let condition = Expr::binary(BinaryOp::Eq, left.columns[0].expression(), right.columns[0].expression());
        let plan = Arc::new(Relation::join(
            Arc::new(Relation::scan(left).unwrap()), Arc::new(Relation::scan(right).unwrap()), JoinKind::Single, Some(condition),
        ).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(plan, &execution.context.catalog, &[], &memory).unwrap();
        let mut cursor = crate::operators::build_columns(runtime, execution).await.unwrap().expect("Expected columnar join");
        assert_eq!(cursor.next_batch().await.err().unwrap().code, crate::cardinality_violation().code);
        assert!(cursor.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn outer_unnest_expands_list_positions_and_keeps_arrow_payloads() {
        use rtdb_plan::{Relation, RelationNode, Schema};

        let (state, table, _) = live_table_with_values(vec![
            Value::list(vec![Value::I64(10), Value::I64(20)]),
            Value::Null,
            Value::list(vec![Value::I64(30)]),
        ]);
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (scan, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let expr = scan.columns[0].expression();
        let mut element = scan.columns[0].clone();
        element.id = ColumnId(10);
        element.name = "element".into();
        element.data_type = LogicalType::Scalar(ScalarType::I64).maybe();
        let mut ordinal = element.clone();
        ordinal.id = ColumnId(11);
        ordinal.name = "ordinal".into();
        ordinal.data_type = LogicalType::Scalar(ScalarType::I32).maybe();
        let scan = Arc::new(Relation::scan(scan).unwrap());
        let schema = scan.schema().concat(&Schema::new(vec![element.clone(), ordinal.clone()]).unwrap()).unwrap();
        let relation = Arc::new(Relation::new(schema, RelationNode::Unnest {
            input: scan, expr, element, ordinality: Some(ordinal), outer: true,
        }).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
        let mut cursor = crate::operators::build_columns(runtime, execution).await.unwrap().expect("Expected a batch unnest");
        let mut elements = Vec::new();
        let mut ordinals = Vec::new();
        while let Some(page) = cursor.next_batch().await.unwrap() {
            assert!(matches!(page.columns[1], crate::columnar::Column::Selected(_)));
            assert!(matches!(page.columns[2], crate::columnar::Column::Arrow { .. }));
            for row in 0..page.len {
                elements.push(page.columns[1].value(row).unwrap());
                ordinals.push(page.columns[2].value(row).unwrap());
            }
        }
        assert_eq!(elements, [Value::I64(10), Value::I64(20), Value::Null, Value::I64(30)]);
        assert_eq!(ordinals, [Value::I32(1), Value::I32(2), Value::Null, Value::I32(1)]);
    }

    #[tokio::test]
    async fn row_number_and_relation_ordinality_append_vectors_to_scan_batches() {
        use rtdb_plan::{OrdinalityScope, Relation, RelationNode, Schema, SortExpr, WindowExpr, WindowFunction};

        let (state, table, _, store, _) = persisted_page_fixture().await;
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (scan, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let key = scan.columns[0].expression();
        let mut field = scan.columns[0].clone();
        field.id = ColumnId(10);
        field.name = "row_no".into();
        field.data_type = LogicalType::Scalar(ScalarType::I64);
        let scan = Arc::new(Relation::scan(scan).unwrap());
        let ordinal = Arc::new(Arc::clone(&scan).with_ordinality(field.clone(), OrdinalityScope::Relation).unwrap());
        let schema = scan.schema().concat(&Schema::new(vec![field.clone()]).unwrap()).unwrap();
        let window = Arc::new(Relation::new(schema, RelationNode::Window {
            input: scan, partition_by: vec![], order_by: vec![SortExpr { expr: key, descending: true, nulls_first: false }],
            expressions: vec![WindowExpr { field, function: WindowFunction::RowNumber }],
        }).unwrap());
        for (relation, expected) in [(ordinal, [1, 2, 3]), (window, [3, 2, 1])] {
            let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
            let mut cursor = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected a batch cursor");
            let mut values = Vec::new();
            let mut ordinals = Vec::new();
            while let Some(page) = cursor.next_batch().await.unwrap() {
                let [crate::columnar::Column::Arrow { array: value, .. }, crate::columnar::Column::Arrow { array: ordinal, .. }] = page.columns.as_slice() else {
                    panic!("Expected an Arrow payload and ordinality");
                };
                let get = |array: &dyn arrow_array::Array| array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap().values().to_vec();
                values.extend(get(value.as_ref()));
                ordinals.extend(get(ordinal.as_ref()));
            }
            assert_eq!(values, expected);
            assert_eq!(ordinals, [1, 2, 3]);
        }
    }

    #[tokio::test]
    async fn materialized_cold_scan_replays_arrow_batches_for_both_references() {
        use rtdb_plan::{BinaryOp, MaterializedBinding, NamedExpr, Relation, RelationNode, SetOperator};

        let (state, table, _, store, _) = persisted_page_fixture().await;
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (scan, execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        let scan = Arc::new(Relation::scan(scan).unwrap());
        let schema = scan.schema().clone();
        let binding = Arc::new(MaterializedBinding { name: "cte".into(), relation: scan });
        let reference = Arc::new(Relation::new(schema.clone(), RelationNode::Reference { binding: Arc::clone(&binding) }).unwrap());
        let body = Arc::new(Relation::new(schema.clone(), RelationNode::Set {
            left: Arc::clone(&reference), right: reference, operator: SetOperator::Union, all: true,
        }).unwrap());
        let relation = Arc::new(Relation::new(schema, RelationNode::Let { binding, body }).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, &[], &memory).unwrap();
        for (row_threshold, expect_spill) in [(100, false), (2, true)] {
            let mut execution = execution.clone();
            Arc::make_mut(&mut execution.context).options.materialized_spill_rows = row_threshold;
            let mut cursor = crate::operators::build_columns(Arc::clone(&runtime), execution.clone()).await.unwrap().expect("Expected Arrow CTE execution");
            assert_eq!(execution.context.spill_bytes() > 0, expect_spill);
            let mut values = Vec::new();
            while let Some(batch) = cursor.next_batch().await.unwrap() {
                let [crate::columnar::Column::Arrow { array, .. }] = batch.columns.as_slice() else { panic!("Expected Arrow output"); };
                let array = array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
                values.extend(array.values().iter().copied());
            }
            assert_eq!(values, [1, 2, 3, 1, 2, 3]);
            drop(cursor);
            assert_eq!(execution.context.spill_bytes(), 0);
        }

        let mut field = relation.schema().fields()[0].clone();
        let expr = Expr::binary(BinaryOp::Add, field.expression(), Expr::Literal(Value::I64(1)));
        field.id = ColumnId(10);
        let projected = Arc::new(relation.project(vec![NamedExpr { field, expr }]).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(Arc::clone(&projected), &execution.context.catalog, &[], &memory).unwrap();
        let mut cursor = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected a nested Arrow CTE");
        let mut values = Vec::new();
        while let Some(batch) = cursor.next_batch().await.unwrap() {
            assert!(matches!(batch.columns[0], crate::columnar::Column::Arrow { .. }));
            values.extend((0..batch.len).map(|row| batch.columns[0].value(row).unwrap()));
        }
        assert_eq!(values, [2, 3, 4, 2, 3, 4].map(Value::I64));
        drop(cursor);
        assert_eq!(execution.context.spill_bytes(), 0);
        let prepared = crate::PreparedQuery::new(projected).unwrap();
        let mut cursor = prepared.execute_columns(execution.context, vec![]).await.unwrap();
        assert_eq!(cursor.next_batch().await.unwrap().unwrap().batch().num_rows(), 6);
        assert!(cursor.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn window_shifts_gather_arrow_cells_across_sorted_batches() {
        use rtdb_plan::{Relation, RelationNode, Schema, SortExpr, WindowExpr, WindowFunction};

        let (state, table, _, store, _) = persisted_page_fixture().await;
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (scan, mut execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        Arc::get_mut(&mut execution.context).unwrap().options.batch_rows = 1;
        let key = scan.columns[0].expression();
        let column = scan.columns[0].id;
        let scan = Arc::new(Relation::scan(scan).unwrap());
        for offset in [1, i64::MAX as u64] {
            let expressions: Vec<_> = [
                ("next", WindowFunction::Lead { column, offset }),
                ("previous", WindowFunction::Lag { column, offset: 1 }),
            ].into_iter().enumerate().map(|(index, (name, function))| {
                let mut field = scan.schema().get(column).unwrap().clone();
                field.id = ColumnId(10 + index as u32);
                field.name = name.into();
                field.data_type = field.data_type.maybe();
                WindowExpr { field, function }
            }).collect();
            let suffix = Schema::new(expressions.iter().map(|expr| expr.field.clone()).collect()).unwrap();
            let schema = scan.schema().concat(&suffix).unwrap();
            let relation = Arc::new(Relation::new(schema, RelationNode::Window {
                input: Arc::clone(&scan), partition_by: vec![], order_by: vec![SortExpr { expr: key.clone(), descending: true, nulls_first: false }], expressions,
            }).unwrap());
            let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
            let mut cursor = crate::operators::build_columns(runtime, execution.clone()).await.unwrap().expect("Expected a batch cursor");
            if offset > 1 {
                let error = cursor.next_batch().await.err().expect("Expected an offset overflow");
                assert!(error.code.ends_with("window-offset"));
                assert!(cursor.next_batch().await.unwrap().is_none());
                continue;
            }
            let mut rows = Vec::new();
            while let Some(page) = cursor.next_batch().await.unwrap() {
                assert_eq!(page.len, 1);
                assert!(matches!(page.columns[0], crate::columnar::Column::Arrow { .. }));
                assert!(page.columns[1..].iter().all(|column| matches!(column, crate::columnar::Column::Selected(_))));
                rows.push(page.columns.iter().map(|column| column.value(0).unwrap()).collect::<Vec<_>>());
            }
            assert_eq!(rows, [
                vec![Value::I64(3), Value::I64(2), Value::Null],
                vec![Value::I64(2), Value::I64(1), Value::I64(3)],
                vec![Value::I64(1), Value::Null, Value::I64(2)],
            ]);
        }
    }

    #[tokio::test]
    async fn partitioned_window_keeps_arrow_values_and_shifts_inside_each_group() {
        use rtdb_plan::{Relation, RelationNode, Schema, SortExpr, WindowExpr, WindowFunction};

        let (state, table, _) = live_table_with_values([Value::I64(1), Value::I64(2), Value::I64(1)]);
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (mut scan, mut execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        Arc::get_mut(&mut execution.context).unwrap().options.batch_rows = 1;
        let group = scan.columns[0].id;
        let mut id = scan.columns[0].clone();
        id.id = ColumnId(1);
        id.name = "_id".into();
        let sort = id.expression();
        scan.columns.push(id.clone());
        let scan = Arc::new(Relation::scan(scan).unwrap());
        let mut ordinal = id.clone();
        ordinal.id = ColumnId(2);
        ordinal.name = "ordinal".into();
        let mut next = id.clone();
        next.id = ColumnId(3);
        next.name = "next_id".into();
        next.data_type = next.data_type.maybe();
        let schema = scan.schema().concat(&Schema::new(vec![ordinal.clone(), next.clone()]).unwrap()).unwrap();
        let relation = Arc::new(Relation::new(schema, RelationNode::Window {
            input: scan,
            partition_by: vec![group],
            order_by: vec![SortExpr { expr: sort, descending: true, nulls_first: false }],
            expressions: vec![
                WindowExpr { field: ordinal, function: WindowFunction::RowNumber },
                WindowExpr { field: next, function: WindowFunction::Lead { column: id.id, offset: 1 } },
            ],
        }).unwrap());
        let runtime = crate::runtime_schema::EmittedRelation::derive(relation, &execution.context.catalog, &[], &memory).unwrap();
        let mut cursor = crate::operators::build_columns(Arc::clone(&runtime), execution.clone()).await.unwrap().expect("Expected Arrow window");
        let mut rows = Vec::new();
        while let Some(page) = cursor.next_batch().await.unwrap() {
            assert_eq!(page.len, 1);
            assert!(page.columns[..3].iter().all(|column| matches!(column, crate::columnar::Column::Arrow { .. })));
            assert!(matches!(page.columns[3], crate::columnar::Column::Selected(_)));
            rows.push(page.columns.iter().map(|column| column.value(0).unwrap()).collect::<Vec<_>>());
        }
        assert_eq!(rows, [
            vec![Value::I64(1), Value::I64(3), Value::I64(1), Value::I64(1)],
            vec![Value::I64(1), Value::I64(1), Value::I64(2), Value::Null],
            vec![Value::I64(2), Value::I64(2), Value::I64(1), Value::Null],
        ]);
        let mut pages = semantic_pages(runtime, execution).await.unwrap();
        let mut reference = Vec::new();
        while let Some(page) = pages.next_page().await.unwrap() {
            reference.extend(page.rows().iter().map(|row| row.values.clone()));
        }
        assert_eq!(rows, reference);
    }

    #[tokio::test]
    async fn grouped_reductions_keep_first_seen_keys_and_emit_counts_as_arrow() {
        use rtdb_plan::{AggregateExpr, AggregateFunction, NamedExpr, Relation, RelationNode, Schema};

        let (state, table, _) = live_table_with_values([Value::I64(2), Value::I64(1), Value::I64(2)]);
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let (scan, mut execution, memory) = descriptor_scan(state.value().snapshot(), table, storage);
        Arc::get_mut(&mut execution.context).unwrap().options.batch_rows = 1;
        let group = NamedExpr { field: scan.columns[0].clone(), expr: scan.columns[0].expression() };
        let mut count = group.field.clone();
        count.id = ColumnId(3);
        count.name = "n".into();
        let schema = Schema::new(vec![group.field.clone(), count.clone()]).unwrap();
        let scan = Arc::new(Relation::scan(scan).unwrap());
        let plan = Arc::new(Relation::new(schema, RelationNode::Aggregate {
            input: scan, groups: vec![group], aggregates: vec![AggregateExpr {
                field: count, function: AggregateFunction::CountStar, argument: None, distinct: false, filter: None, order_by: vec![],
            }],
        }).unwrap());
        let prepared = crate::PreparedQuery::new(Arc::clone(&plan)).unwrap();
        let mut output = prepared.execute_columns(Arc::clone(&execution.context), vec![]).await.unwrap();
        let runtime = crate::runtime_schema::EmittedRelation::derive(plan, &execution.context.catalog, &[], &memory).unwrap();
        let mut arrow = crate::operators::build_columns(Arc::clone(&runtime), execution.clone()).await.unwrap().expect("Expected Arrow aggregate");
        let batch = arrow.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.len, 2);
        assert!(matches!(batch.columns[0], crate::columnar::Column::Selected(_)));
        assert!(matches!(batch.columns[1], crate::columnar::Column::Arrow { .. }));
        let rows: Vec<_> = (0..batch.len).map(|row| batch.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>()).collect();
        assert_eq!(rows, [vec![Value::I64(2), Value::I64(2)], vec![Value::I64(1), Value::I64(1)]]);
        assert!(arrow.next_batch().await.unwrap().is_none());
        let mut pages = semantic_pages(runtime, execution).await.unwrap();
        let data = pages.next_page().await.unwrap().unwrap();
        assert_eq!(rows, data.rows().iter().map(|row| row.values.clone()).collect::<Vec<_>>());
        for _ in &rows {
            assert_eq!(output.next_batch().await.unwrap().unwrap().batch().num_rows(), 1);
        }
        assert!(output.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_data_page_outside_its_metadata_path_fails_every_scan_that_reads_it() {
        let (state, table, _, store, _) = persisted_pages(true).await;
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        for scan_number in 0..2 {
            let (scan, execution, _) = descriptor_scan(state.value().snapshot(), table.clone(), Arc::clone(&storage));
            let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
            let error = loop {
                match cursor.next_page().await {
                    Ok(Some(_)) => continue,
                    Ok(None) => panic!("scan {scan_number} served the misplaced page"),
                    Err(error) => break error,
                }
            };
            assert!(error.code.ends_with("page-path"), "scan {scan_number}: {error:?}");
        }
    }

    async fn scanned_values(snapshot: Snapshot, table: TableRef, storage: &Arc<BufferPool>) -> Vec<Value> {
        let (scan, execution, _) = descriptor_scan(snapshot, table, Arc::clone(storage));
        let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
        let mut values = Vec::new();
        while let Some(page) = cursor.next_page().await.unwrap() { values.extend(page.rows().iter().map(|row| row.values[0].clone())); }
        values
    }

    #[tokio::test]
    async fn a_rescan_reads_the_decoded_metadata_and_pages_from_the_pool_without_its_objects() {
        let (state, table, _, store, _) = persisted_page_fixture().await;
        let storage = Arc::new(BufferPool::new(Arc::clone(&store) as Arc<dyn ObjectStore>, 1 << 20));
        let expected = [Value::I64(1), Value::I64(2), Value::I64(3)];
        assert_eq!(scanned_values(state.value().snapshot(), table.clone(), &storage).await, expected);
        let objects = store.list(None).await.unwrap();
        assert!(objects.iter().all(|object| !storage.holds(&object.key)), "the pool keeps decoded forms rather than the raw objects");
        assert!(storage.retained_bytes() > 0);
        for object in objects { store.delete(&object.key).await.unwrap(); }
        assert_eq!(scanned_values(state.value().snapshot(), table, &storage).await, expected);
    }

    #[tokio::test]
    async fn scans_succeed_on_pools_sharing_a_cache_smaller_than_their_files() {
        let budget = rtdb_storage::cache::CacheBudget::new(64);
        let mut fixtures = Vec::new();
        for _ in 0..2 {
            let (state, table, _, store, _) = persisted_page_fixture().await;
            fixtures.push((state, table, Arc::new(BufferPool::with_budget(store, Arc::clone(&budget)))));
        }
        for _ in 0..2 {
            for (state, table, storage) in &fixtures {
                assert_eq!(scanned_values(state.value().snapshot(), table.clone(), storage).await, [Value::I64(1), Value::I64(2), Value::I64(3)]);
                assert!(budget.retained_bytes() <= budget.limit_bytes());
            }
        }
    }

    #[tokio::test]
    async fn persisted_page_descriptors_admit_growth_before_opening_the_data_file() {
        let (state, table, _, store, metadata) = persisted_page_fixture().await;
        let snapshot = state.value().snapshot();
        assert_eq!(snapshot.trie_catalog().iter_tries(&table).count(), 2);
        assert_eq!(snapshot.trie_entries(&table).len(), 1);
        assert!(snapshot.segments(&table).is_empty());
        assert_descriptor_lifetime(snapshot.clone(), table.clone(), Arc::clone(&store), Some(metadata)).await;
        assert_combined_summary_replacement(snapshot, table, store).await;
    }

    #[tokio::test]
    async fn a_newer_point_skips_older_files_for_the_same_iid() {
        let (state, table, events, store, _) = persisted_page_fixture().await;
        let memory = MemoryBudget::new(1 << 20);
        let system_time = Timestamp(150);
        let mut write = TransactionBuffer::new(state.value().snapshot(),
            TransactionKey { tx_id: TxId(1), system_time }, memory).unwrap();
        let mut newer = events[0].clone();
        newer.system_from = system_time;
        newer.operation = StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::I64(7))]));
        write.append(table.clone(), newer).unwrap();
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (mut scan, execution, _) = descriptor_scan(write.snapshot().unwrap(), table, storage);
        scan.iids = Some([events[0].iid].into());
        scan.system_time = TemporalFilter::Default;
        let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
        assert_eq!(cursor.next_page().await.unwrap().unwrap().rows()[0].values, [Value::I64(7)]);
        assert!(cursor.next_page().await.unwrap().is_none());
        let counts = cursor.scan_counts().unwrap();
        assert_eq!((counts.pages_used, counts.rows_read), (1, 1));
        assert!(counts.pages_pruned > 0);
    }

    #[tokio::test]
    async fn live_segment_priority_counts_pruned_segments_before_persisted_pages() {
        let (state, table, events, store, _) = persisted_page_fixture().await;
        let prepared = state.prepare_block().unwrap();
        let catalog = prepared.table_catalog().unwrap();
        let basis = prepared.value().live().latest_completed_tx().unwrap();
        let block = BlockCatalog::from_proto(pb::Block {
            block_index: Some(prepared.block_index().0),
            latest_completed_tx: Some(pb::TxKey { tx_id: Some(basis.tx_id.0), system_time: Some(basis.system_time.0) }),
            tables: catalog.entries().map(|entry| entry.to_proto()).collect(),
            ..pb::Block::default()
        }).unwrap();
        let additions = BTreeMap::new();
        let table_blocks = prepared.table_blocks(&additions, Timestamp(200)).unwrap();
        for (table, table_block) in &table_blocks {
            let key = catalog.entry(table).unwrap().slug.block_key(prepared.block_index()).unwrap();
            store.put(&ObjectKey::new(key).unwrap(), table_block.encode().into()).await.unwrap();
        }
        let key = block_key(prepared.block_index()).unwrap();
        store.put(&ObjectKey::new(key).unwrap(), block.encode().into()).await.unwrap();
        state.adopt_block(prepared, block, &table_blocks, &additions, Timestamp(200)).unwrap();

        let memory = MemoryBudget::new(1 << 20);
        let key = TransactionKey { tx_id: TxId(1), system_time: basis.system_time };
        let mut applied = TransactionBuffer::new(state.value().snapshot(), key, Arc::clone(&memory)).unwrap();
        for event in [&events[0], &events[2]] { applied.append(table.clone(), event.clone()).unwrap(); }
        state.apply(&applied.stage().unwrap()).unwrap();

        let put_overlay = |snapshot, tx_id, value| {
            let key = TransactionKey { tx_id: TxId(tx_id), system_time: basis.system_time };
            let mut write = TransactionBuffer::new(snapshot, key, Arc::clone(&memory)).unwrap();
            let mut event = events[1].clone();
            event.operation = StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(2)), ("v".into(), Value::I64(value))]));
            write.append(table.clone(), event).unwrap();
            write.snapshot().unwrap()
        };
        let older = put_overlay(state.value().snapshot(), 2, 4);
        let mixed = put_overlay(older, 3, 5);
        let segments = mixed.segments(&table);
        assert_eq!(segments.len(), 3);
        let paths: Vec<_> = segments[2].trie().leaves().into_iter().map(|leaf| leaf.path().as_slice()).collect();
        assert_eq!(paths.as_slice(), &[&[0u8][..], &[2u8][..]]);

        let (mut scan, execution, _) = descriptor_scan(mixed, table, Arc::new(BufferPool::new(store, 1 << 20)));
        scan.iids = Some([events[1].iid].into_iter().collect());
        let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
        let partition = cursor.front_partition_mut().unwrap();
        partition.initialize().await.unwrap();
        let pages = &partition.pages.entries;
        assert_eq!(pages.iter().map(|page| page.priority).collect::<Vec<_>>(), [0, 1, 3]);
        assert!(matches!(&pages[0].source, PageSource::Live { .. }));
        assert!(matches!(&pages[1].source, PageSource::Live { .. }));
        assert!(matches!(&pages[2].source, PageSource::File { file: 0, page: 1, .. }));

        let mut rows = Vec::new();
        while let Some(page) = cursor.next_page().await.unwrap() {
            rows.extend(page.rows().iter().map(|row| row.values[0].clone()));
        }
        assert_eq!(rows, [Value::I64(5)]);
    }

    async fn assert_combined_summary_replacement(snapshot: Snapshot, table: TableRef, store: Arc<MemoryStore>) {
        let key = TransactionKey { tx_id: TxId(1), system_time: Timestamp(110) };
        let mut write = TransactionBuffer::new(snapshot, key, MemoryBudget::new(1 << 20)).unwrap();
        write.append(table.clone(), StoredEvent { position: None,
            origin: None,
            commit_position: 0,
            entity_key: None,
            iid: Iid([0xc0; 16]),
            system_from: key.system_time,
            valid_time: Interval::ALL,
            operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(4)), ("v".into(), Value::I64(4))])),
            recency: None,
        }).unwrap();
        let mixed = write.snapshot().unwrap();
        assert_eq!(mixed.trie_entries(&table).len(), 1);
        assert_eq!(mixed.segments(&table).len(), 1);
        assert_eq!(mixed.segments(&table)[0].trie().leaves().len(), 1);
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (scan, execution, memory) = descriptor_scan(mixed, table, Arc::clone(&storage));
        let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
        let baseline = memory.used_bytes();
        let candidate = std::mem::size_of::<&CatalogTrie>() + 256;
        let old_summary = std::mem::size_of::<PageSummary>() + 256;
        let new_summary = 2 * std::mem::size_of::<PageSummary>() + 256;
        let live_page = std::mem::size_of::<Page>() + 256;
        let headroom = candidate + old_summary + live_page + new_summary - 1;
        let pressure = memory.reserve(memory.limit_bytes() - baseline - headroom).unwrap();
        let retained = baseline + pressure.bytes() + candidate + old_summary + live_page;
        let error = cursor.next_page().await.err().unwrap();
        let expected = format!("Cannot reserve {new_summary} bytes from budget {} with {retained} retained", memory.limit_bytes());
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(error.message.as_deref(), Some(expected.as_str()));
        let partition = cursor.front_partition().unwrap();
        assert!(!partition.initialized);
        assert!(partition.pages.entries.is_empty());
        assert!(partition.tasks.is_none());
        assert_eq!(storage.retained_bytes(), 0);
        assert_eq!(memory.used_bytes(), baseline + pressure.bytes());
        drop(pressure);
        assert_eq!(memory.used_bytes(), baseline);

        let mut rows = Vec::new();
        while let Some(page) = cursor.next_page().await.unwrap() {
            rows.extend(page.rows().iter().map(|row| row.values[0].clone()));
        }
        assert_eq!(rows, [Value::I64(1), Value::I64(2), Value::I64(3), Value::I64(4)]);
        assert_eq!(cursor.scan_counts().unwrap().files_used, 1);
        drop(cursor);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[tokio::test]
    async fn retained_metadata_uses_the_captured_clock_and_keeps_receipt_and_original_cells() {
        let state = PartitionState::empty(TrieConfig::default()).unwrap();
        let memory = MemoryBudget::new(1 << 24);
        let table = TableRef::new("xt", "txs");
        let receipts = TableRef::new("xt", "idempotency_receipts");
        let head = TransactionKey { tx_id: TxId(2), system_time: Timestamp(100) };
        let timestamp = |value| Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
        let metadata = |byte| Value::Struct(StructFields::from([("payload".into(), Value::Binary(vec![byte; 64]))]));
        let mut write = TransactionBuffer::new(state.value().snapshot(), head, Arc::clone(&memory)).unwrap();
        for id in 0..3 {
            write.put(table.clone(), StructFields::from([
                ("_id".into(), Value::I64(id)), ("system_time".into(), timestamp(100)), ("receipt_time".into(), timestamp(20)),
                ("user_metadata".into(), metadata(id as u8)),
            ]), Interval::ALL).unwrap();
        }
        for (id, value) in [(0, metadata(42)), (1, Value::Null)] {
            write.put(receipts.clone(), StructFields::from([
                ("_id".into(), Value::I64(id)), ("user_metadata".into(), value),
            ]), Interval::from_micros(10, 50).unwrap()).unwrap();
        }
        state.apply(&write.stage().unwrap()).unwrap();

        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let table = catalog.table(&QualifiedTable { database: "xtdb".into(), table }).unwrap().unwrap();
        let columns = ["_id", "user_metadata"].into_iter().enumerate().map(|(index, name)| Field {
            id: ColumnId(index as u32), name: name.into(), qualifier: None, qualified_only: false,
            unnamed: false, data_type: table.scan_column_type(name),
        }).collect();
        let scan = Scan {
            table, columns, valid_time: TemporalFilter::All, system_time: TemporalFilter::Default,
            clamp_valid_time: false, iids: None, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new(),
        };
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(1_000), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
        let execution = Execution {
            parameters: Arc::new(BoundParameters::infer(Vec::new(), &memory).unwrap()),
            outer: OuterRows::empty(&memory).unwrap(), context, bindings: None, analysis: None,
        };
        let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
        let mut pages = Vec::new();
        while let Some(page) = cursor.next_batch().await.unwrap() { pages.push(page); }
        drop((cursor, state));
        let mut values = BTreeMap::new();
        for page in pages {
            assert_eq!(page.columns.len(), 2);
            assert!(matches!(&page.columns[1], Column::Selected(_)));
            for row in 0..page.len {
                let Value::I64(id) = page.columns[0].value(row).unwrap() else { panic!("Expected a transaction id"); };
                values.insert(id, page.columns[1].value(row).unwrap());
            }
        }
        assert_eq!(values, BTreeMap::from([(0, metadata(42)), (1, Value::Null), (2, metadata(2))]));
    }

    #[test]
    fn owned_event_columns_keep_binary_order_and_missing_positions() {
        let memory = MemoryBudget::new(1 << 20);
        let position = EventPosition { transaction: TxId(42), ordinal: 300 };
        let event = |position, commit_position| StoredEvent {
            position, commit_position, origin: None, entity_key: None, iid: Iid([0; 16]), recency: None,
            system_from: Timestamp(100), valid_time: Interval::ALL,
            operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(1))])),
        };
        let batches = [event(Some(position), 2), event(None, 7)].map(|event| {
            IndexBatch::from_events(&[event], &StructFields::new(), &memory).unwrap()
        });
        let rows = [1, 0, 1].map(|source| ResolvedRow { source: (source, 0), valid_time: Interval::ALL, system_time: Interval::ALL });
        for (name, physical, expected) in [
            (OWNED_EVENT_POSITION_COLUMN, DataType::Binary, [Value::Null, Value::Binary(position.to_bytes().to_vec()), Value::Null]),
            (HOSTED_RECEIPT_TX_ID, DataType::Int64, [Value::Null, Value::I64(42), Value::Null]),
            ("_assertion_ordinal", DataType::Int64, [Value::I64(7), Value::I64(2), Value::I64(7)]),
        ] {
            let (column, _charge) = history_column(name, &rows, &batches, false, true, true, &memory).unwrap().unwrap();
            let Column::Arrow { array, field, .. } = &column else { panic!("Expected typed history output"); };
            assert_eq!(array.data_type(), &physical);
            assert_eq!(field.is_nullable(), name != "_assertion_ordinal");
            assert_eq!((0..rows.len()).map(|row| column.value(row).unwrap()).collect::<Vec<_>>(), expected);
        }
    }

    #[tokio::test]
    async fn frozen_control_history_resolves_ties_by_position_and_respects_erase() {
        let state = PartitionState::empty(TrieConfig::default()).unwrap();
        let table = TableRef::new("public", "bj_archive_fixture");
        let memory = MemoryBudget::new(1 << 24);
        for (id, value) in [(0, 9), (1, 10)] {
            let key = TransactionKey { tx_id: TxId(id), system_time: Timestamp(100) };
            let mut write = TransactionBuffer::new(state.value().snapshot(), key, Arc::clone(&memory)).unwrap();
            for entity in [1, 2] {
                write.put(table.clone(), StructFields::from([
                    ("_id".into(), Value::I64(entity)), ("v".into(), Value::I64(value)),
                ]), Interval::ALL).unwrap();
            }
            state.apply(&write.stage().unwrap()).unwrap();
        }
        let key = TransactionKey { tx_id: TxId(2), system_time: Timestamp(100) };
        let mut write = TransactionBuffer::new(state.value().snapshot(), key, Arc::clone(&memory)).unwrap();
        write.erase(table.clone(), &EntityId::Integer(2)).unwrap();
        state.apply(&write.stage().unwrap()).unwrap();

        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let name = QualifiedTable { database: "xtdb".into(), table };
        assert!(!catalog.table(&name).unwrap().unwrap().columns.contains_key(ARCHIVE_TX_FROM));
        let catalog = catalog.with_archive_history();
        let binding = catalog.table(&name).unwrap().unwrap();
        let columns = ["v", ARCHIVE_TX_FROM, ARCHIVE_TX_TO, "_system_from", "_system_to"].into_iter().enumerate()
            .map(|(index, name)| Field {
                id: ColumnId(index as u32), name: name.into(), qualifier: None, qualified_only: false,
                unnamed: false, data_type: binding.scan_column_type(name),
            }).collect();
        let scan = Scan {
            table: binding, columns, valid_time: TemporalFilter::All, system_time: TemporalFilter::All,
            clamp_valid_time: false, iids: None, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new(),
        };
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(100), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
        let execution = Execution {
            parameters: Arc::new(BoundParameters::infer(Vec::new(), &memory).unwrap()),
            outer: OuterRows::empty(&memory).unwrap(), context, bindings: None, analysis: None,
        };
        let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
        let mut rows = Vec::new();
        while let Some(page) = cursor.next_batch().await.unwrap() {
            let timestamp = DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()));
            for (column, ty) in page.columns[1..].iter().zip([DataType::Int64, DataType::Int64, timestamp.clone(), timestamp]) {
                assert!(matches!(column, Column::Arrow { array, .. } if array.data_type() == &ty));
            }
            for row in 0..page.len {
                rows.push(page.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>());
            }
        }
        let time = Value::Timestamp { value: 100, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
        assert_eq!(rows, [
            vec![Value::I64(10), Value::I64(1), Value::Null, time.clone(), Value::Null],
            vec![Value::I64(9), Value::I64(0), Value::I64(1), time, Value::Null],
        ]);
    }

    #[tokio::test]
    async fn live_scan_admits_resolved_history_before_projecting_fragments() {
        let state = PartitionState::empty(TrieConfig::default()).unwrap();
        let table = TableRef::new("public", "docs");
        let write_memory = MemoryBudget::new(1 << 20);
        let put = TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) };
        let mut write = TransactionBuffer::new(state.value().snapshot(), put, Arc::clone(&write_memory)).unwrap();
        write.put(table.clone(), StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::I64(9))]), Interval::from_micros(0, 30).unwrap()).unwrap();
        state.apply(&write.stage().unwrap()).unwrap();
        let delete = TransactionKey { tx_id: TxId(1), system_time: Timestamp(200) };
        let mut write = TransactionBuffer::new(state.value().snapshot(), delete, write_memory).unwrap();
        write.delete(table.clone(), &EntityId::Integer(1), Interval::from_micros(10, 20).unwrap()).unwrap();
        state.apply(&write.stage().unwrap()).unwrap();

        let snapshot = state.value().snapshot();
        assert_eq!(snapshot.system_time_cap(), Some(delete.system_time));
        let segments = snapshot.segments(&table);
        assert_eq!(segments.len(), 1);
        let leaves = segments[0].trie().leaves();
        assert_eq!(leaves.len(), 1);
        assert!(leaves[0].path().as_slice().is_empty());
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let database = DatabaseSnapshot { partitions: vec![snapshot], storage };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let binding = catalog.table(&QualifiedTable { database: "xtdb".into(), table }).unwrap().unwrap();
        let time = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
        let columns = [
            ("v", binding.columns["v"].clone()), ("_valid_from", time.clone()), ("_valid_to", time.clone()),
            ("_system_from", time.clone()), ("_system_to", time.maybe()),
        ].into_iter().enumerate().map(|(index, (name, data_type))| Field {
            id: ColumnId(index as u32), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type,
        }).collect();
        let scan = Scan {
            table: binding, columns, valid_time: TemporalFilter::All, system_time: TemporalFilter::All,
            clamp_valid_time: false, iids: None, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new(),
        };
        let root = MemoryBudget::new(1 << 24);
        let memory = root.scope();
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(200), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
        let execution = Execution {
            parameters: Arc::new(BoundParameters::infer(Vec::new(), &memory).unwrap()),
            outer: OuterRows::empty(&memory).unwrap(), context, bindings: None, analysis: None,
        };
        let baseline = root.used_bytes();
        let scratch = std::mem::size_of::<PageSummary>() + std::mem::size_of::<Head>() + std::mem::size_of::<PageBuilder>()
            + std::mem::size_of::<OwnedPage>() + 256 + 4096 + 256;
        let timestamp_bytes = std::mem::size_of::<Timestamp>();
        let previous_ceiling = (2 * 3 + 1) * timestamp_bytes + 256;
        let replacement_ceiling = (2 * 6 + 1) * timestamp_bytes + 256;
        let history = 64 * std::mem::size_of::<ResolvedRow<(u32, u32)>>();
        for (requested, charged) in [
            (replacement_ceiling, scratch + previous_ceiling),
            (history, scratch + previous_ceiling + replacement_ceiling),
        ] {
            let mut cursor = ScanCursor::new(scan.clone(), execution.clone(), None).await.unwrap();
            let partition = cursor.front_partition_mut().unwrap();
            partition.initialize().await.unwrap();
            assert_eq!(partition.pages.entries.len(), 1);
            partition.load(0).await.unwrap();
            let limit = root.limit_bytes();
            let pressure = root.reserve(limit - root.used_bytes() - charged - requested + 1).unwrap();
            let retained = root.used_bytes() + charged;
            let error = cursor.next_page().await.err().unwrap();
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            let expected = format!("Cannot reserve {requested} bytes from budget {limit} with {retained} retained");
            assert_eq!(error.message.as_deref(), Some(expected.as_str()));
            assert!(cursor.front_partition().unwrap().output.is_empty());
            drop((cursor, pressure));
            assert_eq!(root.used_bytes(), baseline);
            assert_eq!(memory.used_bytes(), baseline);
        }

        let mut cursor = ScanCursor::new(scan, execution.clone(), None).await.unwrap();
        let cursor_open = root.used_bytes();
        let mut rows = Vec::new();
        while let Some(page) = cursor.next_page().await.unwrap() {
            rows.extend(page.rows().iter().map(|row| row.values.clone()));
        }
        let timestamp = |value| Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
        assert_eq!(rows, [
            vec![Value::I64(9), timestamp(0), timestamp(10), timestamp(100), Value::Null],
            vec![Value::I64(9), timestamp(10), timestamp(20), timestamp(100), timestamp(200)],
            vec![Value::I64(9), timestamp(20), timestamp(30), timestamp(100), Value::Null],
        ]);
        assert!(cursor.parts.is_empty());
        assert_eq!(root.used_bytes(), cursor_open);
        drop((cursor, execution));
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(root.used_bytes(), 0);
    }
}
