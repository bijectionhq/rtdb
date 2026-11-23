use rtdb_types::{OrderedSet, StructFields};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock, Weak};

use arrow_array::{Array, RecordBatch, StructArray, UnionArray};
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, estimate_read_value_bytes, from_field, read_execution_value};
use rtdb_format::data::{EventBatch, EventRef, StoredEvent, StoredOperation, owned_event_batch, keyed_put_batch};
use rtdb_trie::{Bucketer, EventRow, IidReader, MemoryHashTrie, TrieConfig, TrieNode};
use rtdb_types::{Anomaly, BlockIndex, EntityId, EventHeader, EventOp, Iid, Interval, LogicalType, Result, RowVersion, TableRef, Timestamp, TransactionKey, Value};

use crate::{Snapshot, TableMeta, absent_type, declare_column_types, merge_column_types};

#[derive(Debug)]
pub struct IndexBatch {
    owned: OwnedBatch,
    events: EventBatch,
    put_types: StructFields<LogicalType>,
    /// The exact logical type of each put struct child, in field order, for reading cells without re-deriving it.
    put_field_types: Vec<LogicalType>,
    system_times: Option<(Timestamp, Timestamp)>,
    canonical_iids: OnceLock<bool>,
    /// Every row's header, decoded once with the batch: a scan reads each row's several times, and a batch is shared by every query.
    headers: (Box<[EventHeader]>, Arc<MemoryReservation>),
}

#[derive(Clone, Copy)]
enum EventBuild { Write, CopyInferred, CopyCaptured }

impl IndexBatch {
    pub fn new(batch: RecordBatch, budget: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        Self::from_owned(OwnedBatch::new(batch, budget)?)
    }
    pub fn from_owned(owned: OwnedBatch) -> Result<Arc<Self>> {
        let headers = owned.reservation().budget().reserve(Self::header_bytes(owned.batch().num_rows())?)?;
        Self::from_owned_with_headers(owned, headers)
    }
    /// The bytes `from_owned` charges for the decoded headers of a batch of `rows` events.
    pub fn header_bytes(rows: usize) -> Result<usize> {
        rows.checked_mul(std::mem::size_of::<EventHeader>()).ok_or_else(|| invalid("Event headers exceed addressable memory"))
    }
    /// As `from_owned`, with the headers charged to a reservation the caller already holds for `header_bytes`.
    pub fn from_owned_with_headers(owned: OwnedBatch, header_reservation: Arc<MemoryReservation>) -> Result<Arc<Self>> {
        let events = EventBatch::new(owned.batch().clone())?;
        let schema = owned.batch().schema();
        let index = schema.index_of("op").map_err(invalid)?;
        let ty = from_field(schema.field(index))?;
        let put_types = ty.legs().into_iter().find_map(|leg| match leg { LogicalType::Struct(fields) => Some(fields), _ => None }).unwrap_or_default();
        let put_field_types = match schema.field(index).data_type() {
            arrow_schema::DataType::Union(legs, _) => match legs.iter().find(|(_, leg)| leg.name() == "put").map(|(_, leg)| leg.data_type()) {
                Some(arrow_schema::DataType::Struct(fields)) => fields.iter().map(|field| from_field(field)).collect::<Result<Vec<_>>>()?,
                _ => Vec::new(),
            },
            _ => Vec::new(),
        };
        let mut system_times: Option<(Timestamp, Timestamp)> = None;
        let mut headers = Vec::with_capacity(events.len());
        for row in 0..events.len() {
            let header = events.header(row)?;
            let time = header.system_from;
            system_times = Some(system_times.map_or((time, time), |(first, last)| (first.min(time), last.max(time))));
            headers.push(header);
        }
        Ok(Arc::new(Self { owned, events, put_types, put_field_types, system_times, canonical_iids: OnceLock::new(), headers: (headers.into_boxed_slice(), header_reservation) }))
    }
    pub fn from_events(events: &[StoredEvent], declared: &StructFields<LogicalType>, budget: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        Self::build_events(events, declared, EventBuild::Write, budget)
    }
    /// Copies typed event cells, joining their exact types with declarations without numeric coercion.
    /// Raw decimal coefficients retain their parameters; returned arrays retain their reservation independently.
    pub fn copy_events(events: &[StoredEvent], declared: &StructFields<LogicalType>, budget: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        Self::build_events(events, declared, EventBuild::CopyInferred, budget)
    }
    /// Reconstructs stored event rows with the captured put schema and unchanged decimal parameters and coefficients.
    /// The returned arrays retain their reservation independently of this index batch.
    pub fn copy_events_with_put_schema(events: &[StoredEvent], declared: &StructFields<LogicalType>, budget: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        Self::build_events(events, declared, EventBuild::CopyCaptured, budget)
    }
    fn build_events(events: &[StoredEvent], declared: &StructFields<LogicalType>, mode: EventBuild, budget: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let mut schema_bytes = 4096usize;
        for (name, ty) in declared {
            schema_bytes = schema_bytes.checked_add(name.len()).and_then(|bytes| bytes.checked_add(ty.retained_bytes().ok()?)).ok_or_else(|| invalid("Put schema allocation size overflow"))?;
        }
        for event in events {
            if let StoredOperation::Put(document) = &event.operation {
                schema_bytes = schema_bytes.checked_add(Value::document_retained_bytes(document)?).ok_or_else(|| invalid("Put schema allocation size overflow"))?;
            }
        }
        let _schema_scratch = budget.reserve(schema_bytes.checked_mul(4).ok_or_else(|| invalid("Put schema allocation size overflow"))?)?;
        let mut has_put = matches!(mode, EventBuild::CopyCaptured) || !declared.is_empty();
        let mut documents = Vec::new();
        for event in events {
            if let StoredOperation::Put(document) = &event.operation {
                if !matches!(mode, EventBuild::CopyCaptured) {
                    documents.push(document.iter().map(|(name, value)| (name.clone(), value.logical_type())).collect::<StructFields<_>>());
                }
                has_put = true;
            }
        }
        let rows = row_column_types(&documents);
        let fields = match rows { Some(rows) => declare_column_types(&rows, declared), None => declared.clone() };
        let copy = !matches!(mode, EventBuild::Write);
        Self::from_owned(owned_event_batch(events, has_put.then_some(fields), copy, budget)?)
    }
    fn put_id_matches(&self, row: usize, iid: Iid) -> Option<bool> {
        let operations = self.arrow().column_by_name("op")?.as_any().downcast_ref::<UnionArray>()?;
        let put = operations.child(operations.type_id(row)).as_any().downcast_ref::<StructArray>()?;
        let (index, field) = put.fields().iter().enumerate().find(|(_, field)| field.name() == "_id")?;
        let column = put.column(index);
        let offset = operations.value_offset(row);
        let bound = estimate_read_value_bytes(field, column.as_ref(), offset).ok()?
            .checked_mul(3)?.checked_add(1024)?;
        let _scratch = self.owned.reservation().budget().reserve(bound).ok()?;
        let value = read_execution_value(field, column.as_ref(), offset).ok()?;
        Some(value.entity_id().ok()?.iid() == iid)
    }
    fn has_system_time(&self, system_time: Timestamp) -> bool {
        self.system_times.is_none_or(|times| times == (system_time, system_time))
    }
    fn has_canonical_iids(&self) -> Option<bool> {
        if let Some(proof) = self.canonical_iids.get() { return Some(*proof); }
        for row in 0..self.len() {
            let header = self.events.header(row).ok()?;
            if header.op == EventOp::Put && !self.put_id_matches(row, header.iid)? {
                let _ = self.canonical_iids.set(false);
                return Some(false);
            }
        }
        let _ = self.canonical_iids.set(true);
        Some(true)
    }
    pub fn view(&self) -> &EventBatch { &self.events }
    pub fn header(&self, row: usize) -> Result<EventHeader> {
        self.headers.0.get(row).copied().ok_or_else(|| Anomaly::incorrect("rtdb.index/event-row", "Event row exceeds batch length"))
    }
    pub fn arrow(&self) -> &RecordBatch { self.owned.batch() }
    /// Any retained Arrow child can keep this entire allocation charged.
    pub fn arrow_reservation(&self) -> &Arc<MemoryReservation> { self.owned.reservation() }
    pub fn len(&self) -> usize { self.events.len() }
    pub fn is_empty(&self) -> bool { self.events.is_empty() }
    pub fn put_types(&self) -> &StructFields<LogicalType> { &self.put_types }
    /// The logical type of the put struct's `index`th child field, as `from_field` derives it.
    pub fn put_field_type(&self, index: usize) -> Option<&LogicalType> { self.put_field_types.get(index) }
}

#[derive(Clone, Debug)]
pub struct RowSource { pub batch: Arc<IndexBatch>, pub row: usize }
impl RowSource {
    pub fn header(&self) -> Result<EventHeader> { self.batch.header(self.row) }
    pub fn event(&self) -> Result<StoredEvent> { self.batch.view().read_event(self.row) }
}

#[derive(Clone, Debug)]
pub struct LiveRowLineage {
    generation: Arc<()>,
    revision: Option<Arc<LiveRowRevision>>,
}

#[derive(Clone)]
pub struct LiveRowProof {
    generation: Weak<()>,
    revision: Option<Weak<LiveRowRevision>>,
}

impl LiveRowProof {
    pub fn admits(&self, lineage: &LiveRowLineage) -> bool {
        self.descends_from(lineage) || self.is_prefix_of(lineage)
    }

    pub fn is_prefix_of(&self, lineage: &LiveRowLineage) -> bool {
        if !std::ptr::eq(self.generation.as_ptr(), Arc::as_ptr(&lineage.generation)) { return false; }
        match &self.revision {
            None => true,
            Some(revision) => revision.upgrade().is_some_and(|prefix| is_prefix(&prefix, lineage.revision.as_ref())),
        }
    }

    pub fn descends_from(&self, lineage: &LiveRowLineage) -> bool {
        if !std::ptr::eq(self.generation.as_ptr(), Arc::as_ptr(&lineage.generation)) { return false; }
        let Some(prefix) = lineage.revision.as_ref() else { return true; };
        let current = self.revision.as_ref().and_then(Weak::upgrade);
        is_prefix(prefix, current.as_ref())
    }
}

fn is_prefix(prefix: &Arc<LiveRowRevision>, mut current: Option<&Arc<LiveRowRevision>>) -> bool {
    while let Some(revision) = current {
        if revision.rows <= prefix.rows { return Arc::ptr_eq(revision, prefix); }
        current = revision.parent.as_ref();
    }
    false
}

struct LiveRowRevision {
    rows: u32,
    parent: Option<Arc<LiveRowRevision>>,
    depth: u16,
}

impl std::fmt::Debug for LiveRowRevision {
    fn fmt(&self, format: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format.debug_struct("LiveRowRevision").field("rows", &self.rows).finish_non_exhaustive()
    }
}

impl Drop for LiveRowRevision {
    fn drop(&mut self) {
        let mut parent = self.parent.take();
        while let Some(mut revision) = parent {
            let Some(unique) = Arc::get_mut(&mut revision) else { break; };
            parent = unique.parent.take();
        }
    }
}

impl LiveRowLineage {
    fn empty() -> Self { Self { generation: Arc::new(()), revision: None } }

    fn append(&mut self, rows: u32) {
        const MAX_REVISIONS: u16 = 4096;
        let depth = self.revision.as_ref().map_or(0, |revision| revision.depth + 1);
        if depth == MAX_REVISIONS {
            *self = Self::empty();
        }
        let parent = self.revision.take();
        let depth = parent.as_ref().map_or(0, |revision| revision.depth + 1);
        self.revision = Some(Arc::new(LiveRowRevision { rows, parent, depth }));
    }

    pub fn generation_address(&self) -> usize { Arc::as_ptr(&self.generation) as usize }

    pub fn proof(&self) -> LiveRowProof {
        LiveRowProof { generation: Arc::downgrade(&self.generation), revision: self.revision.as_ref().map(Arc::downgrade) }
    }
}

#[derive(Clone, Debug)]
pub struct LiveTable {
    table: TableRef,
    block_index: BlockIndex,
    chunks: Vec<(u32, Arc<IndexBatch>)>,
    row_count: u32,
    lineage: LiveRowLineage,
    column_types: StructFields<LogicalType>,
    trie: MemoryHashTrie,
    last_system_time: Option<Timestamp>,
    canonical_entity_iids: bool,
    located: LocatedLeaves,
}

/// A trie leaf's rows in iid order, each located in its table's chunks with its header copied out.
#[derive(Debug)]
pub struct LocatedRows { pub located: Vec<(u32, u32)>, pub headers: Vec<EventHeader>, _reservation: Option<Arc<MemoryReservation>> }

/// The leaves a snapshot's scans have located, keyed by the leaf's address in the snapshot's own trie.
/// A row's location depends on the chunk layout, which every append can change, so a cloned table starts empty.
#[derive(Debug, Default)]
struct LocatedLeaves(std::sync::Mutex<std::collections::HashMap<usize, Arc<LocatedRows>>>);
impl Clone for LocatedLeaves { fn clone(&self) -> Self { Self::default() } }

impl LiveTable {
    pub fn new(table: TableRef, block_index: BlockIndex, config: TrieConfig) -> Self {
        Self { table, block_index, chunks: Vec::new(), row_count: 0, lineage: LiveRowLineage::empty(), column_types: StructFields::new(), trie: MemoryHashTrie::new(config), last_system_time: None, canonical_entity_iids: true, located: LocatedLeaves::default() }
    }
    pub fn table(&self) -> &TableRef { &self.table }
    pub fn block_index(&self) -> BlockIndex { self.block_index }
    pub fn row_count(&self) -> u32 { self.row_count }
    pub fn column_types(&self) -> &StructFields<LogicalType> { &self.column_types }
    pub fn trie(&self) -> &MemoryHashTrie { &self.trie }
    pub(crate) fn has_canonical_entity_iids(&self) -> bool { self.canonical_entity_iids }
    pub fn batches(&self) -> impl Iterator<Item = &Arc<IndexBatch>> { self.chunks.iter().map(|(_, batch)| batch) }
    pub fn row_lineage(&self) -> &LiveRowLineage { &self.lineage }

    /// The unconsumed batches and the prefix to skip in the first one.
    /// A live table preserves row addresses when it merges its Arrow chunks.
    pub fn batches_from(&self, row: u32) -> impl Iterator<Item = (&Arc<IndexBatch>, usize)> {
        let first = self.chunks.partition_point(|(start, batch)| u64::from(*start) + batch.len() as u64 <= u64::from(row));
        self.chunks[first..].iter().map(move |(start, batch)| (batch, row.saturating_sub(*start) as usize))
    }
    fn locate_row(&self, row: u32) -> Result<(usize, u32)> {
        if row >= self.row_count { return Err(invalid("Live table row exceeds its retained batches")); }
        crate::row_address::last_started_chunk(&self.chunks, row)
            .ok_or_else(|| invalid("Live table row has no owning batch"))
    }
    pub fn locate(&self, row: u32) -> Result<(u32, u32)> {
        let (index, offset) = self.locate_row(row)?;
        let index = u32::try_from(index).map_err(|_| invalid("Live table chunk exceeds u32 address space"))?;
        Ok((index, offset))
    }
    /// `leaf`'s rows located and their headers copied, once per snapshot; `leaf` is one of this table's trie leaves.
    pub fn located_rows(&self, leaf: &rtdb_trie::Leaf) -> Result<Arc<LocatedRows>> {
        let key = leaf as *const rtdb_trie::Leaf as usize;
        if let Some(rows) = self.located.0.lock().map_err(|_| invalid("Located leaf cache is poisoned"))?.get(&key) { return Ok(Arc::clone(rows)); }
        let rows = leaf.sorted_rows(self)?;
        let bytes = Self::located_row_bytes(rows.len())?;
        let reservation = self.chunks.first().map(|(_, batch)| batch.owned.reservation().budget().reserve(bytes)).transpose()?;
        let rows = self.copy_located_rows(&rows, reservation)?;
        Ok(Arc::clone(self.located.0.lock().map_err(|_| invalid("Located leaf cache is poisoned"))?.entry(key).or_insert(rows)))
    }

    /// Locates the leaf under the caller's prepaid memory with no shared cache dependency.
    pub fn located_rows_in(&self, leaf: &rtdb_trie::Leaf, memory: &Arc<MemoryBudget>) -> Result<Arc<LocatedRows>> {
        let reservation = memory.reserve(Self::located_row_bytes(leaf.len())?)?;
        let (rows, _sorted) = leaf.sorted_rows_in(self, memory)?;
        self.copy_located_rows(&rows, Some(reservation))
    }

    fn located_row_bytes(count: usize) -> Result<usize> {
        count.checked_mul(std::mem::size_of::<(u32, u32)>() + std::mem::size_of::<EventHeader>()).and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(|| invalid("Located leaf rows exceed addressable memory"))
    }

    fn copy_located_rows(&self, rows: &[u32], _reservation: Option<Arc<MemoryReservation>>) -> Result<Arc<LocatedRows>> {
        let mut located = Vec::with_capacity(rows.len());
        let mut headers = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let at = self.locate(*row)?;
            headers.push(self.header_at(at)?);
            located.push(at);
        }
        Ok(Arc::new(LocatedRows { located, headers, _reservation }))
    }
    /// The header of a row `locate` found.
    pub fn header_at(&self, (chunk, offset): (u32, u32)) -> Result<EventHeader> {
        self.chunks.get(chunk as usize).ok_or_else(|| invalid("Live table chunk is absent"))?.1.header(offset as usize)
    }
    /// The batch and position of the row `locate` found, without taking a handle on the batch.
    pub fn batch_at(&self, (chunk, offset): (u32, u32)) -> Result<(&Arc<IndexBatch>, u32)> {
        Ok((&self.chunks.get(chunk as usize).ok_or_else(|| invalid("Live table chunk is absent"))?.1, offset))
    }
    /// The row `locate` found.
    pub fn row_at(&self, (chunk, offset): (u32, u32)) -> Result<RowSource> {
        let (_, batch) = self.chunks.get(chunk as usize).ok_or_else(|| invalid("Live table chunk is absent"))?;
        Ok(RowSource { batch: Arc::clone(batch), row: offset as usize })
    }
    /// The header of `row`, without taking a handle on its batch.
    pub fn header(&self, row: u32) -> Result<EventHeader> {
        let (index, offset) = self.locate_row(row)?;
        self.chunks[index].1.header(offset as usize)
    }
    pub fn row(&self, row: u32) -> Result<RowSource> {
        let (index, offset) = self.locate_row(row)?;
        Ok(RowSource { batch: Arc::clone(&self.chunks[index].1), row: offset as usize })
    }

    fn iid_rows(&self, iid: Iid, prepaid: Option<&Arc<MemoryBudget>>) -> Result<(Option<Arc<LocatedRows>>, std::ops::Range<usize>)> {
        let mut node = self.trie.root().as_ref();
        let leaf = loop {
            match node {
                TrieNode::Leaf(leaf) => break Some(leaf),
                TrieNode::Branch { path, children } => {
                    let bucket = Bucketer::DEFAULT.bucket(iid, path.len())? as usize;
                    let Some(child) = &children[bucket] else { break None; };
                    node = child;
                }
            }
        };
        let rows = leaf.map(|leaf| match prepaid {
            Some(memory) => self.located_rows_in(leaf, memory),
            None => self.located_rows(leaf),
        }).transpose()?;
        let (first, end) = rows.as_ref().map_or((0, 0), |rows| {
            let first = rows.headers.partition_point(|header| header.iid < iid);
            let end = first + rows.headers[first..].partition_point(|header| header.iid == iid);
            (first, end)
        });
        Ok((rows, first..end))
    }

    /// Returns the stored headers for an IID in the live snapshot's history order.
    pub fn iid_headers(&self, iid: Iid) -> Result<impl Iterator<Item = EventHeader>> {
        let (rows, range) = self.iid_rows(iid, None)?;
        Ok(range.map(move |row| rows.as_ref().expect("nonempty history has a leaf").headers[row]))
    }

    pub fn iid_sources(&self, iid: Iid, memory: &Arc<MemoryBudget>) -> Result<impl Iterator<Item = Result<(EventHeader, RowSource)>>> {
        let (rows, range) = self.iid_rows(iid, memory.is_prepaid().then_some(memory))?;
        Ok(range.map(move |row| {
            let rows = rows.as_ref().expect("nonempty history has a leaf");
            Ok((rows.headers[row], self.row_at(rows.located[row])?))
        }))
    }

    pub fn row_of_version(&self, version: RowVersion) -> Result<Option<u32>> {
        let (rows, range) = self.iid_rows(version.iid, None)?;
        let Some(rows) = rows else { return Ok(None); };
        let mut matches = range.filter(|&row| rows.headers[row].version() == version);
        let Some(row) = matches.next() else { return Ok(None); };
        if matches.next().is_some() { return Ok(None); }
        let (chunk, offset) = rows.located[row];
        Ok(self.chunks[chunk as usize].0.checked_add(offset))
    }
    pub fn append(&self, write: &TableWrite, system_time: Timestamp) -> Result<Self> {
        if self.last_system_time.is_some_and(|previous| previous > system_time) { return Err(invalid("Live table system time moved backwards")); }
        let mut next = self.clone();
        next.column_types = merge_column_types(&next.column_types, &write.column_types);
        if let Some(batch) = &write.batch {
            if batch.view().has_entity_keys() != self.table.is_owned_event_data() {
                return Err(invalid("The table write has the wrong physical entity-key layout"));
            }
            if !batch.has_system_time(system_time) { return Err(invalid("Imported event system time differs from its transaction")); }
            if batch.view().has_entity_keys() && !batch.is_empty() {
                let mut previous = if self.last_system_time == Some(system_time) && self.row_count > 0 {
                    let header = self.header(self.row_count - 1)?;
                    (header.system_from == system_time).then_some(header.commit_position)
                } else {
                    None
                };
                for header in batch.headers.0.iter() {
                    let position = header.commit_position;
                    if previous.is_some_and(|previous| {
                        if previous == 0 { position != 0 } else { position <= previous }
                    }) {
                        return Err(invalid("Owned commit positions must increase in transaction row order"));
                    }
                    previous = Some(position);
                }
            }
            if next.canonical_entity_iids {
                next.canonical_entity_iids = batch.has_canonical_iids().unwrap_or(false);
            }
            let count = u32::try_from(batch.len()).map_err(invalid)?;
            let start = next.row_count;
            next.row_count = crate::row_address::append_chunk(&mut next.chunks, start, count, Arc::clone(batch))
                .ok_or_else(|| invalid("Live table exceeds u32 row address space"))?;
            next.trie = self.trie.add_range(&next, start, count)?;
            if count > 0 { next.lineage.append(next.row_count); }
            next.merge_small_chunks()?;
        }
        next.last_system_time = Some(system_time);
        Ok(next)
    }
    /// Merges the newest chunk into the one before it while it is at least as large, they share a layout and together
    /// stay small, so a table written a row per transaction is read from chunks of up to `MERGED_CHUNK_ROWS` rows.
    /// Chunks keep their row addresses, since a merged chunk starts where the earlier one did.
    fn merge_small_chunks(&mut self) -> Result<()> {
        const MERGED_CHUNK_ROWS: usize = 8192;
        while let [.., (_, previous), (_, last)] = self.chunks.as_slice() {
            if last.len() < previous.len() || previous.len() + last.len() > MERGED_CHUNK_ROWS || previous.arrow().schema() != last.arrow().schema() { break; }
            let schema = previous.arrow().schema();
            let budget = Arc::clone(previous.owned.reservation().budget());
            // A concatenation's buffers can outgrow its inputs' (variable-width columns round and grow theirs), so it is reserved for twice them and shrunk to what it holds.
            let bound = previous.arrow().get_array_memory_size().checked_add(last.arrow().get_array_memory_size()).and_then(|bytes| bytes.checked_mul(2)).and_then(|bytes| bytes.checked_add(4096))
                .ok_or_else(|| invalid("Merged live chunk exceeds addressable memory"))?;
            let mut reservation = budget.reserve(bound)?;
            let merged = arrow_select::concat::concat_batches(&schema, [previous.arrow(), last.arrow()]).map_err(invalid)?;
            // Concatenating a dense union sizes every leg for all of its rows, so the merge can outgrow its inputs.
            let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("A merged chunk's charge is shared"))?;
            lease.grow_to(lease.bytes().max(merged.get_array_memory_size()))?;
            lease.shrink_to(merged.get_array_memory_size())?;
            let merged = IndexBatch::from_owned(OwnedBatch::with_reservation(merged, reservation)?)?;
            crate::row_address::replace_last_two(&mut self.chunks, merged);
        }
        Ok(())
    }
    pub fn ordered_events(&self) -> Result<Vec<EventRow<RowSource>>> {
        self.trie.sorted_rows(self)?.into_iter().map(|row| {
            let source = self.row(row)?;
            Ok(EventRow { header: source.header()?, source })
        }).collect()
    }
    pub fn block_pages(&self, memory: &Arc<MemoryBudget>) -> Result<BlockPages<'_>> {
        let _reservation = memory.reserve(self.chunks.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("Invalid block page inventory size"))?)?;
        let empty = IndexBatch::copy_events_with_put_schema(&[], &self.column_types, memory)?;
        let schema = empty.arrow().schema();
        let sources = self.chunks.iter().all(|(_, batch)| batch.arrow().schema() == schema)
            .then(|| self.chunks.iter().map(|(_, batch)| &batch.owned).collect());
        Ok(BlockPages { table: self, sources, memory: Arc::clone(memory), _reservation })
    }
    pub fn block_metadata(&self) -> Result<TableMeta> {
        let mut hlls: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for (_, batch) in &self.chunks {
            for (name, delta) in rtdb_format::hll::from_event_batch(batch.arrow(), 0..batch.len())? {
                let combined = match hlls.get(&name) {
                    Some(previous) => rtdb_format::hll::combine(previous, &delta)?, None => delta,
                };
                hlls.insert(name, combined);
            }
        }
        Ok(TableMeta { column_types: self.column_types.clone(), row_count: i64::from(self.row_count), hlls })
    }
}

impl IidReader for LiveTable {
    fn iid(&self, row: u32) -> Result<Iid> { self.header(row).map(|header| header.iid) }
}

pub struct BlockPages<'a> {
    table: &'a LiveTable,
    sources: Option<Vec<&'a OwnedBatch>>,
    memory: Arc<MemoryBudget>,
    _reservation: Arc<MemoryReservation>,
}

impl BlockPages<'_> {
    pub fn live_table(&self) -> &LiveTable { self.table }

    /// Writes a leaf's distinct, ordered rows using the captured table layout.
    pub fn copy(&self, rows: &[u32]) -> Result<OwnedBatch> {
        if let Some(sources) = &self.sources {
            let bytes = rows.len().checked_mul(32).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(|| invalid("Invalid block row selection size"))?;
            let result = self.memory.reserve(bytes).and_then(|_scratch| {
                let positions = rows.iter().map(|&row| {
                    let (index, offset) = crate::row_address::last_started_chunk(&self.table.chunks, row)
                        .ok_or_else(|| invalid("Live table row has no owning batch"))?;
                    Ok((index, offset as usize))
                }).collect::<Result<Vec<_>>>()?;
                OwnedBatch::interleave_distinct_rows(sources, &positions, &self.memory)
            });
            match result {
                Ok(batch) => return Ok(batch),
                Err(error) if error.code == "rtdb/memory-budget-exceeded" => (),
                Err(error) => return Err(error),
            }
        }
        let base = rows.len().checked_mul(std::mem::size_of::<EventRef<'_>>())
            .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("Page row allocation overflow"))?;
        let bound = self.table.column_types.iter().try_fold(base, |bytes, (name, ty)| {
            bytes.checked_add(document_type_bytes(name, ty, None)?).ok_or_else(|| invalid("Page schema allocation overflow"))
        })?;
        let _scratch = self.memory.reserve(bound)?;
        let events = rows.iter().map(|&row| {
            let (chunk, offset) = self.table.locate_row(row)?;
            let batch = &self.table.chunks[chunk].1;
            batch.view().event_ref(offset as usize, &batch.put_field_types)
        }).collect::<Result<Vec<_>>>()?;
        owned_event_batch(&events, Some(self.table.column_types.clone()), true, &self.memory)
    }
}

#[derive(Clone, Debug)]
pub struct TableWrite { column_types: StructFields<LogicalType>, batch: Option<Arc<IndexBatch>> }
impl TableWrite {
    pub fn new(column_types: StructFields<LogicalType>, batch: Option<Arc<IndexBatch>>) -> Self {
        let column_types = match &batch { Some(batch) => declare_column_types(batch.put_types(), &column_types), None => column_types };
        Self { column_types, batch }
    }
    pub fn column_types(&self) -> &StructFields<LogicalType> { &self.column_types }
    pub fn batch(&self) -> Option<&Arc<IndexBatch>> { self.batch.as_ref() }
    pub fn row_count(&self) -> usize { self.batch.as_ref().map_or(0, |batch| batch.len()) }
}

#[derive(Clone, Debug)]
pub struct StagedWrite {
    tx_key: TransactionKey,
    predecessor: Option<TransactionKey>,
    block_index: BlockIndex,
    tables: BTreeMap<TableRef, TableWrite>,
}
impl StagedWrite {
    pub fn new(tx_key: TransactionKey, predecessor: Option<TransactionKey>, block_index: BlockIndex, tables: BTreeMap<TableRef, TableWrite>) -> Result<Self> {
        if predecessor.is_some_and(|previous| previous.tx_id >= tx_key.tx_id || previous.system_time > tx_key.system_time) {
            return Err(invalid("Transaction key does not advance its predecessor"));
        }
        for write in tables.values() {
            if write.batch().is_some_and(|batch| !batch.has_system_time(tx_key.system_time)) {
                return Err(invalid("Staged event has another transaction's system time"));
            }
        }
        Ok(Self { tx_key, predecessor, block_index, tables })
    }
    pub fn tx_key(&self) -> TransactionKey { self.tx_key }
    pub fn predecessor(&self) -> Option<TransactionKey> { self.predecessor }
    pub fn block_index(&self) -> BlockIndex { self.block_index }
    pub fn tables(&self) -> &BTreeMap<TableRef, TableWrite> { &self.tables }
    pub fn row_count(&self) -> Result<u64> { self.tables.values().try_fold(0u64, |count, write| count.checked_add(write.row_count() as u64).ok_or_else(|| invalid("Transaction row count overflow"))) }
}

#[derive(Clone, Debug)]
pub struct LiveIndex {
    block_index: BlockIndex,
    latest_completed_tx: Option<TransactionKey>,
    block_row_count: u64,
    tables: BTreeMap<TableRef, Arc<LiveTable>>,
    pub(crate) search: Option<ServingSearch>,
    config: TrieConfig,
}

#[derive(Clone)]
pub struct ServingSearch(Arc<dyn std::any::Any + Send + Sync>);

impl ServingSearch {
    pub fn new(index: impl std::any::Any + Send + Sync) -> Self { Self(Arc::new(index)) }
    pub fn get<T: std::any::Any>(&self) -> Option<&T> { self.0.downcast_ref() }
}

impl std::fmt::Debug for ServingSearch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServingSearch").finish_non_exhaustive()
    }
}

impl LiveIndex {
    pub fn new(block_index: BlockIndex, latest_completed_tx: Option<TransactionKey>, config: TrieConfig) -> Self {
        Self { block_index, latest_completed_tx, block_row_count: 0, tables: BTreeMap::new(), search: None, config }
    }
    pub fn block_index(&self) -> BlockIndex { self.block_index }
    pub fn latest_completed_tx(&self) -> Option<TransactionKey> { self.latest_completed_tx }
    pub fn block_row_count(&self) -> u64 { self.block_row_count }
    pub fn live_batch_bytes(&self) -> u64 {
        self.tables.values().flat_map(|table| table.batches()).fold(0u64, |bytes, batch| {
            bytes.saturating_add(batch.owned.reservation().bytes() as u64).saturating_add(batch.headers.1.bytes() as u64)
        })
    }
    pub fn tables(&self) -> &BTreeMap<TableRef, Arc<LiveTable>> { &self.tables }
    pub fn apply(&self, staged: &StagedWrite) -> Result<Self> {
        if staged.block_index != self.block_index || staged.predecessor != self.latest_completed_tx { return Err(invalid("Staged transaction no longer follows this live index")); }
        let mut next = self.clone();
        for (table, write) in &staged.tables {
            let previous = self.tables.get(table).cloned().unwrap_or_else(|| Arc::new(LiveTable::new(table.clone(), self.block_index, self.config)));
            next.tables.insert(table.clone(), Arc::new(previous.append(write, staged.tx_key.system_time)?));
        }
        next.block_row_count = next.block_row_count.checked_add(staged.row_count()?).ok_or_else(|| invalid("Block row count overflow"))?;
        next.latest_completed_tx = Some(staged.tx_key);
        Ok(next)
    }
    pub fn next_block(&self) -> Result<Self> {
        Ok(Self::new(BlockIndex(self.block_index.0.checked_add(1).ok_or_else(|| invalid("Block index overflow"))?), self.latest_completed_tx, self.config))
    }
    pub fn block_metadata(&self) -> Result<BTreeMap<TableRef, TableMeta>> {
        self.tables.iter().map(|(table, rows)| Ok((table.clone(), rows.block_metadata()?))).collect()
    }
}

#[derive(Default)]
struct PendingTable {
    declared: StructFields<LogicalType>,
    events: Vec<StoredEvent>,
    batches: Vec<(usize, Arc<IndexBatch>)>,
    reservations: Vec<Arc<MemoryReservation>>,
}

impl PendingTable {
    fn position(&self) -> WritePosition {
        WritePosition { events: self.events.len(), batches: self.batches.len() }
    }

    fn copy_since(&self, written: WritePosition, budget: &Arc<MemoryBudget>) -> Result<Arc<IndexBatch>> {
        let events = &self.events[written.events..];
        let batches = &self.batches[written.batches..];
        if batches.is_empty() {
            return IndexBatch::copy_events(events, &self.declared, budget);
        }
        if let [(_, batch)] = batches && events.is_empty() && batch.put_types == self.declared {
            return Ok(Arc::clone(batch));
        }
        if events.is_empty() && let Some((_, first)) = batches.first() && first.put_types == self.declared {
            let schema = first.arrow().schema();
            if batches.iter().all(|(_, batch)| batch.arrow().schema() == schema) {
                let bytes = batches.iter().try_fold(4096usize, |bytes, (_, batch)| {
                    batch.arrow().get_array_memory_size().checked_mul(4)
                        .and_then(|size| bytes.checked_add(size)).ok_or_else(|| invalid("Write batch size overflow"))
                })?;
                let mut reservation = budget.reserve(bytes)?;
                let combined = arrow_select::concat::concat_batches(&schema, batches.iter().map(|(_, batch)| batch.arrow())).map_err(invalid)?;
                Arc::get_mut(&mut reservation).expect("write concatenation owns its reservation").shrink_to(combined.get_array_memory_size())?;
                return IndexBatch::from_owned(OwnedBatch::with_reservation(combined, reservation)?);
            }
        }
        let size_error = || invalid("Write batch size overflow");
        let positions = || batches.iter().map(|(offset, batch)| (*offset, Some(batch)))
            .chain(std::iter::once((self.events.len(), None)));
        let event_count = batches.iter().try_fold(events.len(), |count, (_, batch)| {
            count.checked_add(batch.len()).ok_or_else(size_error)
        })?;
        let scratch = event_count.checked_mul(512).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
        let scratch = events.iter().try_fold(scratch, |bytes, event| match &event.operation {
            StoredOperation::Put(document) => Value::document_retained_bytes(document)?.checked_mul(4)
                .and_then(|size| bytes.checked_add(size)).ok_or_else(size_error),
            _ => Ok(bytes),
        })?;
        let scratch = batches.iter().try_fold(scratch, |bytes, (_, batch)| {
            batch.put_types.iter().try_fold(bytes, |bytes, (name, ty)| {
                ty.retained_bytes()?.checked_add(name.len()).and_then(|size| size.checked_mul(4))
                    .and_then(|size| bytes.checked_add(size)).ok_or_else(size_error)
            })
        })?;
        let _scratch = budget.reserve(scratch)?;
        let mut document_types = Vec::new();
        let mut next_event = written.events;
        for (upto, batch) in positions() {
            for event in &self.events[next_event..upto] {
                if let StoredOperation::Put(document) = &event.operation {
                    document_types.push(document.iter().map(|(name, value)| (name.clone(), value.logical_type())).collect());
                }
            }
            if let Some(batch) = batch
                && batch.headers.0.iter().any(|header| header.op == EventOp::Put) {
                document_types.push(batch.put_types.clone());
            }
            next_event = upto;
        }
        let fields = row_column_types(&document_types).map_or_else(|| self.declared.clone(), |types| {
            declare_column_types(&types, &self.declared)
        });
        let has_put = !document_types.is_empty() || !self.declared.is_empty();

        let mut next_event = written.events;
        let mut ordered = Vec::with_capacity(event_count);
        for (upto, batch) in positions() {
            if next_event != upto {
                let rows = &self.events[next_event..upto];
                let batch = owned_event_batch(rows, has_put.then(|| fields.clone()), true, budget)?;
                ordered.extend(EventBatch::new(batch.batch().clone())?.page_events()?);
            }
            if let Some(batch) = batch {
                ordered.extend(batch.events.page_events()?);
            }
            next_event = upto;
        }
        IndexBatch::from_owned(owned_event_batch(&ordered, has_put.then_some(fields), true, budget)?)
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct WritePosition { events: usize, batches: usize }

enum OwnedCommitPositions {
    Landing(u32),
    Archive(std::vec::IntoIter<u32>),
}

pub struct TransactionBuffer {
    base: Snapshot,
    tx_key: TransactionKey,
    budget: Arc<MemoryBudget>,
    tables: BTreeMap<TableRef, PendingTable>,
    owned_positions: Option<OwnedCommitPositions>,
    /// Each table's transaction segment as last read, with the events and declarations it holds; a read appends only what was written since.
    segments: std::sync::Mutex<BTreeMap<TableRef, (Arc<LiveTable>, WritePosition, StructFields<LogicalType>)>>,
}

impl TransactionBuffer {
    pub fn new(base: Snapshot, tx_key: TransactionKey, budget: Arc<MemoryBudget>) -> Result<Self> {
        StagedWrite::new(tx_key, base.tx_basis(), base.block_index(), BTreeMap::new())?;
        Ok(Self { base, tx_key, budget, tables: BTreeMap::new(), owned_positions: None, segments: Default::default() })
    }
    pub fn tx_key(&self) -> TransactionKey { self.tx_key }
    pub fn retain_owned_positions(&mut self) -> Result<()> {
        self.select_owned_positions(OwnedCommitPositions::Landing(0))
    }
    pub fn retain_archived_owned_positions(&mut self, positions: Vec<u32>) -> Result<()> {
        if positions.first().is_none_or(|first| *first == 0)
            || !positions.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(invalid("Archived commit positions must be positive and strictly ordered"));
        }
        self.select_owned_positions(OwnedCommitPositions::Archive(positions.into_iter()))
    }
    fn select_owned_positions(&mut self, positions: OwnedCommitPositions) -> Result<()> {
        if self.owned_positions.is_some() || !self.tables.is_empty() {
            return Err(invalid("Commit positions must be selected before staging owned effects"));
        }
        self.owned_positions = Some(positions);
        Ok(())
    }
    pub fn retains_owned_positions(&self) -> bool { self.owned_positions.is_some() }
    pub fn require_archived_positions_consumed(&self) -> Result<()> {
        if matches!(&self.owned_positions, Some(OwnedCommitPositions::Archive(remaining)) if remaining.len() != 0) {
            return Err(invalid("The archived transaction omitted positioned effects"));
        }
        Ok(())
    }
    pub fn declare_table(&mut self, table: TableRef, columns: impl IntoIterator<Item = String>) -> Result<()> {
        let columns: Vec<_> = columns.into_iter().collect();
        let table_bytes = table.schema.capacity().checked_add(table.table.capacity()).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("Declared table size overflow"))?;
        let bytes = columns.iter().try_fold(table_bytes, |bytes, name| bytes.checked_add(name.capacity()).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("Declared schema size overflow")))?;
        let reservation = self.budget.reserve(bytes)?;
        let pending = self.tables.entry(table).or_default();
        for name in columns { pending.declared.entry(name).or_insert(LogicalType::Nothing); }
        pending.reservations.push(reservation);
        Ok(())
    }
    pub fn declare_document_types<'a>(&mut self, table: &TableRef, fields: impl IntoIterator<Item = (&'a String, &'a LogicalType)>) -> Result<()> {
        let pending = self.tables.get_mut(table).ok_or_else(|| invalid("Put schema requires an open table write"))?;
        for (name, ty) in fields {
            let existing = pending.declared.get(name);
            if existing == Some(ty) { continue; }
            let reservation = self.budget.reserve(document_type_bytes(name, ty, existing)?)?;
            let merged = existing.map_or_else(|| ty.clone(), |existing| existing.join(ty));
            pending.declared.insert(name.clone(), merged);
            pending.reservations.push(reservation);
        }
        Ok(())
    }
    pub fn put(&mut self, table: TableRef, document: StructFields<Value>, valid_time: Interval) -> Result<()> {
        let id = document.get("_id").ok_or_else(|| invalid("Put document has no _id"))?.entity_id()?;
        self.append(table, StoredEvent { commit_position: 0, entity_key: None, iid: id.iid(), system_from: self.tx_key.system_time, valid_time, operation: StoredOperation::Put(document), recency: None })
    }
    pub fn delete(&mut self, table: TableRef, id: &EntityId, valid_time: Interval) -> Result<()> {
        self.append(table, StoredEvent { commit_position: 0, entity_key: None, iid: id.iid(), system_from: self.tx_key.system_time, valid_time, operation: StoredOperation::Delete, recency: None })
    }
    pub fn erase(&mut self, table: TableRef, id: &EntityId) -> Result<()> {
        self.append(table, StoredEvent { commit_position: 0, entity_key: None, iid: id.iid(), system_from: self.tx_key.system_time, valid_time: Interval::ALL, operation: StoredOperation::Erase, recency: None })
    }
    pub fn append(&mut self, table: TableRef, event: StoredEvent) -> Result<()> { self.append_to(&table, event) }
    /// As `append`, copying the table reference only for the table's first write.
    pub fn append_to(&mut self, table: &TableRef, mut event: StoredEvent) -> Result<()> {
        if event.entity_key.is_some() != table.is_owned_event_data() {
            return Err(invalid("The transaction event has the wrong physical entity-key layout"));
        }
        if event.system_from != self.tx_key.system_time { return Err(invalid("Transaction event has another system time")); }
        if event.commit_position != 0 {
            return Err(invalid("The transaction buffer assigns owned commit positions"));
        }
        let position = self.next_owned_position(table)?;
        if let StoredOperation::Put(document) = &mut event.operation { document.values_mut().try_for_each(Value::admit_object_dates)?; }
        let document_bytes = match &event.operation { StoredOperation::Put(document) => Value::document_retained_bytes(document)?, _ => 0 };
        let bytes = document_bytes.checked_add(event.entity_key.as_ref().map_or(0, |key| key.len()))
            .and_then(|bytes| bytes.checked_add(4096)).and_then(|bytes| bytes.checked_add(table.schema.len())).and_then(|bytes| bytes.checked_add(table.table.len()))
            .ok_or_else(|| invalid("Transaction event size overflow"))?;
        let reservation = self.budget.reserve(bytes)?;
        if !self.tables.contains_key(table) { self.tables.insert(table.clone(), PendingTable::default()); }
        if let Some(position) = position {
            event.commit_position = position;
            self.consume_owned_position(position);
        }
        let pending = self.tables.get_mut(table).ok_or_else(|| invalid("Transaction table write disappeared"))?;
        pending.events.push(event);
        pending.reservations.push(reservation);
        Ok(())
    }
    fn next_owned_position(&self, table: &TableRef) -> Result<Option<u32>> {
        Ok(if table.is_owned_event_data() {
            match &self.owned_positions {
                Some(OwnedCommitPositions::Landing(previous)) => {
                    Some(previous.checked_add(1).ok_or_else(|| invalid("Owned commit position overflow"))?)
                },
                Some(OwnedCommitPositions::Archive(remaining)) => {
                    Some(*remaining.as_slice().first().ok_or_else(|| invalid("The archive contains an extra owned effect"))?)
                },
                None => None,
            }
        } else {
            None
        })
    }

    fn consume_owned_position(&mut self, position: u32) {
        match &mut self.owned_positions {
            Some(OwnedCommitPositions::Landing(previous)) => *previous = position,
            Some(OwnedCommitPositions::Archive(remaining)) => { let _ = remaining.next(); },
            None => unreachable!("only an ordered transaction has an owned position"),
        }
    }

    pub fn append_owned_put(
        &mut self, table: &TableRef, content: arrow_array::ArrayRef, mut header: EventHeader,
        key: &[u8], source_bytes: Option<usize>,
    ) -> Result<()> {
        if !table.is_owned_event_data() || header.commit_position != 0 {
            return Err(invalid("An owned Arrow put requires a governed table and an unassigned position"));
        }
        let position = self.next_owned_position(table)?;
        if let Some(position) = position { header.commit_position = position; }
        let _key = self.budget.reserve(key.len().checked_add(4096).ok_or_else(|| invalid("Owned key is too large"))?)?;
        let keys = arrow_array::BinaryArray::from_iter_values([key]);
        let batch = keyed_put_batch(content, &[header], keys, source_bytes, &self.budget)?;
        self.append_batch_inner(table, batch)?;
        if let Some(position) = position { self.consume_owned_position(position); }
        Ok(())
    }

    pub fn append_batch(&mut self, table: &TableRef, batch: OwnedBatch) -> Result<()> {
        if self.retains_owned_positions() && table.is_owned_event_data() {
            return Err(invalid("An ordered landing stages governed events in assertion order"));
        }
        self.append_batch_inner(table, batch)
    }

    fn append_batch_inner(&mut self, table: &TableRef, batch: OwnedBatch) -> Result<()> {
        let batch = IndexBatch::from_owned(batch)?;
        if batch.is_empty() { return Ok(()); }
        if !batch.has_system_time(self.tx_key.system_time) {
            return Err(invalid("Transaction batch has another system time"));
        }
        let reservation = self.budget.reserve(table.schema.len() + table.table.len() + 4096)?;
        let pending = self.tables.get(table);
        let mut declarations = Vec::new();
        for (name, ty) in &batch.put_types {
            let existing = pending.and_then(|pending| pending.declared.get(name));
            if existing == Some(ty) { continue; }
            let reservation = self.budget.reserve(document_type_bytes(name, ty, existing)?)?;
            let merged = existing.map_or_else(|| ty.clone(), |existing| existing.join(ty));
            declarations.push((name.clone(), merged, reservation));
        }
        if pending.is_none() { self.tables.insert(table.clone(), PendingTable::default()); }
        let pending = self.tables.get_mut(table).ok_or_else(|| invalid("Transaction table write disappeared"))?;
        for (name, ty, reservation) in declarations {
            pending.declared.insert(name, ty);
            pending.reservations.push(reservation);
        }
        pending.batches.push((pending.events.len(), batch));
        pending.reservations.push(reservation);
        Ok(())
    }

    /// The snapshot the transaction started from, without its own writes, for a statement that reads no table.
    pub fn base_snapshot(&self) -> Snapshot { self.base.clone() }
    pub fn has_pending_table(&self, table: &TableRef) -> bool { self.tables.contains_key(table) }
    pub fn snapshot(&self) -> Result<Snapshot> {
        let mut segments = self.segments.lock().map_err(|_| invalid("Transaction segment cache was poisoned"))?;
        for (table, pending) in &self.tables {
            let (segment, done, declared) = match segments.get(table) {
                Some((segment, done, declared)) => (Arc::clone(segment), *done, Some(declared)),
                None => (Arc::new(LiveTable::new(table.clone(), self.base.block_index(), TrieConfig::default())), WritePosition::default(), None),
            };
            let position = pending.position();
            if done == position && declared == Some(&pending.declared) { continue; }
            let batch = (done != position).then(|| pending.copy_since(done, &self.budget)).transpose()?;
            let segment = Arc::new(segment.append(&TableWrite::new(pending.declared.clone(), batch), self.tx_key.system_time)?);
            segments.insert(table.clone(), (segment, position, pending.declared.clone()));
        }
        let current = segments.iter().map(|(table, (segment, ..))| (table.clone(), Arc::clone(segment))).collect();
        self.base.with_transaction_segments(self.tx_key, &current)
    }
    pub fn stage(mut self) -> Result<StagedWrite> {
        self.require_archived_positions_consumed()?;
        // The segments only served the transaction's own reads; the staged write supersedes them.
        if let Ok(segments) = self.segments.get_mut() { segments.clear(); }
        self.materialize()
    }
    fn materialize(&self) -> Result<StagedWrite> {
        let tables = self.tables.iter().map(|(table, pending)| {
            let batch = Some(pending.copy_since(WritePosition::default(), &self.budget)?);
            Ok((table.clone(), TableWrite::new(pending.declared.clone(), batch)))
        }).collect::<Result<_>>()?;
        StagedWrite::new(self.tx_key, self.base.tx_basis(), self.base.block_index(), tables)
    }
}

fn document_type_bytes(name: &str, ty: &LogicalType, previous: Option<&LogicalType>) -> Result<usize> {
    ty.retained_bytes()?.checked_add(previous.map_or(Ok(0), LogicalType::retained_bytes)?)
        .and_then(|bytes| bytes.checked_add(name.len())).and_then(|bytes| bytes.checked_add(512))
        .and_then(|bytes| bytes.checked_mul(8)).ok_or_else(|| invalid("Declared schema size overflow"))
}

fn invalid(message: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.index/invalid-live-index", message.to_string()) }

#[cfg(test)]
#[test]
fn block_pages_preserve_order_and_layout_across_live_batches() {
    let memory = MemoryBudget::new(32 << 20);
    let table = TableRef::new("public", "docs");
    let rows = |start, time, text| (start..start + 2).map(|id| StoredEvent {
        commit_position: 0,
        entity_key: None,
        iid: EntityId::Integer(id).iid(),
        system_from: Timestamp(time),
        valid_time: Interval::ALL,
        operation: StoredOperation::Put(StructFields::from([
            ("_id".into(), Value::I64(id)),
            ("value".into(), if text { Value::Utf8(id.to_string()) } else { Value::I64(id) }),
        ])),
        recency: None,
    }).collect::<Vec<_>>();
    let first = rows(0, 1, false);
    for layout_changes in [false, true] {
        let second = rows(2, 2, layout_changes);
        let write = |events: &[StoredEvent]| TableWrite::new(StructFields::new(), Some(IndexBatch::copy_events(events, &StructFields::new(), &memory).unwrap()));
        let live = LiveTable::new(table.clone(), BlockIndex(0), TrieConfig::default())
            .append(&write(&first), Timestamp(1)).unwrap()
            .append(&write(&second), Timestamp(2)).unwrap();
        let pages = live.block_pages(&memory).unwrap();
        assert_eq!(pages.sources.is_some(), !layout_changes);
        let actual = pages.copy(&[3, 0, 2]).unwrap();
        let selected = vec![second[1].clone(), first[0].clone(), second[0].clone()];
        let expected = IndexBatch::copy_events_with_put_schema(&selected, live.column_types(), &memory).unwrap();
        assert_eq!(actual.batch(), expected.arrow());
    }
}

#[cfg(test)]
#[test]
fn evolving_live_layouts_flush_with_column_bounded_scratch() {
    let source = MemoryBudget::new(128 << 20);
    let table = TableRef::new("public", "docs");
    let make_event = |id, value| {
        let mut document = StructFields::from([("_id".into(), Value::I64(id)), ("value".into(), value)]);
        for column in 0..6 { document.insert(format!("payload_{column}"), Value::Binary(vec![id as u8; 1 << 17])); }
        StoredEvent {
            commit_position: 0,
            entity_key: None,
            iid: EntityId::Integer(id).iid(), system_from: Timestamp(id), valid_time: Interval::ALL,
            operation: StoredOperation::Put(document), recency: None,
        }
    };
    let events = vec![make_event(2, Value::I64(2)), make_event(1, Value::Utf8("one".into()))];
    let mut live = LiveTable::new(table, BlockIndex(0), TrieConfig::default());
    for event in events.iter().rev() {
        let batch = IndexBatch::copy_events(std::slice::from_ref(event), &StructFields::new(), &source).unwrap();
        live = live.append(&TableWrite::new(StructFields::new(), Some(batch)), event.system_from).unwrap();
    }
    let expected = IndexBatch::copy_events_with_put_schema(&events, live.column_types(), &source).unwrap();
    let output = MemoryBudget::new(4 << 20);
    assert!(IndexBatch::copy_events_with_put_schema(&events, live.column_types(), &output).is_err());

    let pages = live.block_pages(&output).unwrap();
    assert!(pages.sources.is_none());
    let result = pages.copy(&[1, 0]).unwrap();
    assert_eq!(result.batch(), expected.arrow());
    drop(result);
    drop(pages);
    assert_eq!(output.used_bytes(), 0);
}

#[cfg(test)]
#[test]
fn event_batches_build_wide_document_columns_with_bounded_scratch() {
    let memory = MemoryBudget::new(128 << 20);
    let events = (0..100).map(|id| {
        let mut doc = StructFields::from([("_id".into(), Value::I64(id))]);
        for key in 0..10 { doc.insert(format!("field_{id}_{key}"), Value::I64(id + key)); }
        StoredEvent {
            commit_position: 0,
            entity_key: None,
            iid: EntityId::Integer(id).iid(),
            system_from: Timestamp(1),
            valid_time: Interval::ALL,
            operation: StoredOperation::Put(doc),
            recency: None,
        }
    }).collect::<Vec<_>>();
    let batch = IndexBatch::copy_events(&events, &StructFields::new(), &memory).unwrap();
    assert_eq!(batch.len(), 100);
    assert_eq!(batch.put_types().len(), 1001);
    let StoredOperation::Put(doc) = batch.view().read_event(99).unwrap().operation else { panic!("Expected put"); };
    assert_eq!(doc["field_99_9"], Value::I64(108));
    assert_eq!(doc["field_0_0"], Value::Null);
}

#[cfg(test)]
#[test]
fn staged_batches_release_event_builder_scratch_before_they_enter_the_live_index() {
    let memory = MemoryBudget::new(16 << 20);
    let events = (0..1024).map(|id| StoredEvent {
        commit_position: 0,
        entity_key: None,
        iid: EntityId::Integer(id).iid(),
        system_from: Timestamp(1),
        valid_time: Interval::ALL,
        operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(id))])),
        recency: None,
    }).collect::<Vec<_>>();
    let batches = (0..32).map(|_| IndexBatch::copy_events(&events, &StructFields::new(), &memory).unwrap()).collect::<Vec<_>>();
    assert_eq!(batches.iter().map(|batch| batch.len()).sum::<usize>(), 32_768);
    assert!(memory.used_bytes() < 4 << 20);
    let arrow = batches[0].arrow().clone();
    drop(batches);
    assert_ne!(memory.used_bytes(), 0);
    drop(arrow);
    assert_eq!(memory.used_bytes(), 0);
}

#[cfg(test)]
#[test]
fn a_denied_append_keeps_the_transaction_schema_and_budget_unchanged() {
    let memory = MemoryBudget::new(1 << 20);
    let table = TableRef::new("public", "docs");
    let system_from = Timestamp(10);
    let event = StoredEvent {
        commit_position: 0,
        entity_key: None,
        iid: EntityId::Integer(1).iid(), system_from, valid_time: Interval::ALL,
        operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::I64(2))])),
        recency: None,
    };
    let batch = IndexBatch::copy_events(&[event.clone()], &StructFields::new(), &memory).unwrap();
    let first = document_type_bytes("_id", &batch.put_types()["_id"], None).unwrap();
    let budget = MemoryBudget::new(table.schema.len() + table.table.len() + 4096 + first);
    let partition = crate::PartitionState::empty(Default::default()).unwrap();
    let key = TransactionKey { tx_id: rtdb_types::TxId(0), system_time: system_from };
    let mut transaction = TransactionBuffer::new(partition.value().snapshot(), key, Arc::clone(&budget)).unwrap();

    assert_eq!(transaction.append_batch(&table, batch.owned.clone()).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert!(transaction.tables.is_empty());
    assert_eq!(budget.used_bytes(), 0);
    transaction.append_to(&table, event).unwrap();
}

#[cfg(test)]
#[test]
fn denied_iid_verification_preserves_the_write_and_cannot_promote_its_generation() {
    let memory = MemoryBudget::new(1 << 20);
    let table = TableRef::new("public", "docs");
    let write = |id, system_from| {
        let event = StoredEvent {
            commit_position: 0,
            entity_key: None,
            iid: EntityId::Uuid(id).iid(),
            system_from,
            valid_time: Interval::ALL,
            operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::Uuid(id))])),
            recency: None,
        };
        TableWrite::new(StructFields::new(), Some(IndexBatch::from_events(&[event], &StructFields::new(), &memory).unwrap()))
    };
    let first_time = Timestamp(1);
    let second_time = Timestamp(2);
    let first = write([1; 16], first_time);
    let second = write([2; 16], second_time);
    let empty = LiveTable::new(table, BlockIndex(0), TrieConfig::default());
    let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
    let unproved = empty.append(&first, first_time).unwrap();
    assert_eq!(unproved.row(0).unwrap().header().unwrap().iid, Iid([1; 16]));
    assert!(!unproved.has_canonical_entity_iids());
    drop(blocker);

    let extended = unproved.append(&second, second_time).unwrap();
    assert_eq!(extended.row_count(), 2);
    assert!(!extended.has_canonical_entity_iids());
    assert!(empty.append(&first, first_time).unwrap().has_canonical_entity_iids());
    let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
    assert!(empty.append(&first, first_time).unwrap().has_canonical_entity_iids());
    drop(blocker);
}

/// Joins every document's column types once per column, which equals folding `merge_column_types` over the documents
/// in order: each document lacking a column contributes its own absence.
fn row_column_types(documents: &[StructFields<LogicalType>]) -> Option<StructFields<LogicalType>> {
    let (first, rest) = documents.split_first()?;
    if rest.iter().all(|document| document == first) { return Some(first.clone()); }
    let absences: Vec<_> = documents.iter().map(absent_type).collect();
    let names: OrderedSet<&String> = documents.iter().flat_map(|document| document.keys()).collect();
    Some(names.into_iter().map(|name| {
        let joined = LogicalType::join_all(documents.iter().zip(&absences).map(|(document, absent)| document.get(name).unwrap_or(absent)));
        (name.clone(), joined)
    }).collect())
}

#[cfg(test)]
mod row_column_type_tests {
    use super::*;
    use rtdb_types::{ListKind, ScalarType};

    #[test]
    fn joining_each_column_once_matches_folding_documents_in_order() {
        let scalar = |scalar| LogicalType::Scalar(scalar);
        let list = |element| LogicalType::List { kind: ListKind::List, element: Box::new(element) };
        let structure = |fields: &[(&str, LogicalType)]| LogicalType::Struct(fields.iter().map(|(name, ty)| ((*name).into(), ty.clone())).collect());
        let document = |fields: &[(&str, LogicalType)]| fields.iter().map(|(name, ty)| ((*name).to_owned(), ty.clone())).collect::<StructFields<_>>();
        let documents = vec![
            document(&[("_id", scalar(ScalarType::I64)), ("a", list(scalar(ScalarType::Utf8)))]),
            document(&[("_id", scalar(ScalarType::I64)), ("b", structure(&[("x", scalar(ScalarType::Bool))]))]),
            document(&[("_id", scalar(ScalarType::Utf8)), ("a", list(structure(&[("y", scalar(ScalarType::F64))]))), ("b", scalar(ScalarType::I64))]),
            document(&[("_id", scalar(ScalarType::I64)), ("c", LogicalType::Null)]),
            document(&[("_id", scalar(ScalarType::I64)), ("b", structure(&[("x", scalar(ScalarType::I64)), ("z", list(scalar(ScalarType::Bool)))]))]),
        ];
        let folded = documents.iter().cloned().reduce(|left, right| if left == right { left } else { merge_column_types(&left, &right) });
        assert_eq!(row_column_types(&documents), folded);
        assert_eq!(row_column_types(&documents).unwrap().keys().map(String::as_str).collect::<Vec<_>>(), ["_id", "a", "b", "c"], "columns keep the order they were first seen");
        let LogicalType::Union(ids) = &row_column_types(&documents).unwrap()["_id"] else { panic!("_id joins two types") };
        assert_eq!(ids.iter().cloned().collect::<Vec<_>>(), [scalar(ScalarType::I64), scalar(ScalarType::Utf8)]);
        assert_eq!(row_column_types(&documents[..1]), Some(documents[0].clone()));
        assert_eq!(row_column_types(&[]), None);
    }
}

#[cfg(test)]
mod chunk_tests {
    use super::*;
    use rtdb_types::EntityId;

    #[test]
    fn governed_arrow_puts_consume_positions_in_order_with_row_events() {
        let memory = MemoryBudget::new(1 << 24);
        let state = crate::PartitionState::empty(Default::default()).unwrap();
        let key = TransactionKey { tx_id: rtdb_types::TxId(0), system_time: Timestamp(1) };
        let mut transaction = TransactionBuffer::new(state.snapshot(), key, memory).unwrap();
        transaction.retain_archived_owned_positions(vec![3, 7]).unwrap();
        let table = rtdb_types::OwnedRelation::new("locations").unwrap().table("owner");
        let id = EntityId::String("place".into());
        let row = RecordBatch::try_from_iter([
            ("_id", Arc::new(arrow_array::StringArray::from(vec!["place"])) as arrow_array::ArrayRef),
            ("location", Arc::new(arrow_array::BinaryArray::from_iter_values([b"location"]))),
        ]).unwrap();
        let header = EventHeader { commit_position: 0, iid: id.iid(), system_from: key.system_time, valid_time: Interval::ALL, op: EventOp::Put };
        transaction.append_owned_put(&table, Arc::new(StructArray::from(row)), header, &id.handle_bytes(), None).unwrap();
        transaction.append_to(&table, StoredEvent {
            commit_position: 0, entity_key: Some(id.handle_bytes().into()), iid: id.iid(),
            system_from: key.system_time, valid_time: Interval::ALL,
            operation: StoredOperation::Delete, recency: None,
        }).unwrap();
        let staged = transaction.stage().unwrap();
        let batch = staged.tables()[&table].batch().unwrap();
        let first = batch.view().read_event(0).unwrap();
        let second = batch.view().read_event(1).unwrap();
        assert_eq!((first.header().commit_position, first.header().op), (3, EventOp::Put));
        assert_eq!((second.header().commit_position, second.header().op), (7, EventOp::Delete));
    }

    #[test]
    fn a_table_written_a_row_per_transaction_is_held_in_merged_chunks_with_its_rows_in_place() {
        let memory = MemoryBudget::new(1 << 26);
        let table = TableRef::new("public", "docs");
        let mut live = LiveTable::new(table, BlockIndex(0), TrieConfig::default());
        for id in 0..10_000i64 {
            let event = StoredEvent { commit_position: 0, entity_key: None, iid: EntityId::Integer(id).iid(), system_from: Timestamp(id), valid_time: Interval::ALL,
                operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(id)), ("v".into(), Value::F64(id as f64))])), recency: None };
            live = live.append(&TableWrite::new(StructFields::new(), Some(IndexBatch::copy_events(&[event], &StructFields::new(), &memory).unwrap())), Timestamp(id)).unwrap();
        }
        assert!(live.chunks.len() <= 8, "{} chunks", live.chunks.len());
        for id in [0u32, 1, 1023, 1024, 8191, 8192, 9999] {
            assert_eq!(live.row(id).unwrap().header().unwrap().system_from, Timestamp(i64::from(id)));
        }
    }

    fn put(id: i64, memory: &Arc<MemoryBudget>) -> TableWrite {
        let event = StoredEvent { commit_position: 0, entity_key: None, iid: EntityId::Integer(id).iid(), system_from: Timestamp(id), valid_time: Interval::ALL,
            operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(id))])), recency: None };
        TableWrite::new(StructFields::new(), Some(IndexBatch::copy_events(&[event], &StructFields::new(), memory).unwrap()))
    }

    #[test]
    fn chunks_of_a_column_written_at_several_widths_merge_within_their_charge() {
        let memory = MemoryBudget::new(1 << 28);
        let mut live = LiveTable::new(TableRef::new("public", "docs"), BlockIndex(0), TrieConfig::default());
        for tx in 0..128i64 {
            let events = (tx * 64..tx * 64 + 64).map(|n| {
                let id = match (n - tx * 64) % 3 { 0 => Value::I16(n as i16), 1 => Value::I32(n as i32), _ => Value::I64(n) };
                StoredEvent { commit_position: 0, entity_key: None, iid: EntityId::Integer(n).iid(), system_from: Timestamp(tx), valid_time: Interval::ALL,
                    operation: StoredOperation::Put(StructFields::from([("_id".into(), id), ("sku".into(), Value::Utf8(format!("sku-{n}")))])), recency: None }
            }).collect::<Vec<_>>();
            live = live.append(&TableWrite::new(StructFields::new(), Some(IndexBatch::copy_events(&events, &StructFields::new(), &memory).unwrap())), Timestamp(tx)).unwrap();
        }
        assert!(live.chunks.len() <= 4, "{} chunks", live.chunks.len());
        assert_eq!(live.row_count, 8192);
    }

    #[test]
    fn a_snapshot_locates_a_leaf_once_and_a_later_append_relocates_it() {
        let memory = MemoryBudget::new(1 << 26);
        let mut live = LiveTable::new(TableRef::new("public", "docs"), BlockIndex(0), TrieConfig::default());
        for id in 0..3 { live = live.append(&put(id, &memory), Timestamp(id)).unwrap(); }
        let leaf = live.trie().leaves()[0];
        let located = live.located_rows(leaf).unwrap();
        assert!(Arc::ptr_eq(&located, &live.located_rows(leaf).unwrap()));
        assert_eq!(located.headers.iter().map(|header| header.system_from).collect::<OrderedSet<_>>(), (0..3).map(Timestamp).collect());
        let appended = live.append(&put(3, &memory), Timestamp(3)).unwrap();
        assert_eq!(appended.chunks.len(), 1);
        for id in 0..3 {
            let iid = EntityId::Integer(i64::from(id)).iid();
            assert_eq!(live.iid(id).unwrap(), iid);
            assert_eq!(appended.iid(id).unwrap(), iid);
            assert_eq!(appended.header_at(appended.locate(id).unwrap()).unwrap(), live.header(id).unwrap());
        }
        let relocated = appended.located_rows(appended.trie().leaves()[0]).unwrap();
        assert!(!Arc::ptr_eq(&located, &relocated));
        assert_eq!(relocated.headers.len(), 4);
        for (at, header) in relocated.located.iter().zip(&relocated.headers) { assert_eq!(appended.header_at(*at).unwrap(), *header); }
        drop((located, relocated, live, appended));
        assert_eq!(memory.used_bytes(), 0, "located rows are released with the table that holds them");
    }

    #[test]
    fn a_table_written_a_row_per_transaction_with_long_string_ids_merges_its_chunks() {
        let memory = MemoryBudget::new(1 << 26);
        let table = TableRef::new("public", "keys");
        let mut live = LiveIndex::new(BlockIndex(0), None, TrieConfig::default());
        for id in 0..1000i64 {
            let key = format!("{id:032x}/0");
            let event = StoredEvent { commit_position: 0, entity_key: None, iid: EntityId::String(key.clone()).iid(), system_from: Timestamp(id), valid_time: Interval::ALL,
                operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::Utf8(key))])), recency: None };
            let batch = IndexBatch::copy_events(&[event], &StructFields::new(), &memory).unwrap();
            let write = TableWrite::new(StructFields::new(), Some(batch));
            let transaction = TransactionKey { tx_id: rtdb_types::TxId(id), system_time: Timestamp(id) };
            let staged = StagedWrite::new(transaction, live.latest_completed_tx(), live.block_index(), BTreeMap::from([(table.clone(), write)])).unwrap();
            live = live.apply(&staged).unwrap_or_else(|error| panic!("row {id}: {error:?}"));
        }
        let chunks = &live.tables()[&table].chunks;
        assert!(chunks.len() <= 16, "{} chunks", chunks.len());
        assert_eq!(live.live_batch_bytes(), memory.used_bytes() as u64);
        assert_eq!(live.next_block().unwrap().live_batch_bytes(), 0);
    }
}
