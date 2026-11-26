use rtdb_types::{OrderedSet, StructFields};
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::{Arc, OnceLock, Weak};

use arrow_array::{Array, ArrayRef, NullArray, RecordBatch, StructArray, UnionArray};
use arrow_schema::{DataType, Field, SchemaRef, UnionMode};
use rtdb_columnar::{
    ArrowCell, BatchBuilder, MemoryBudget, MemoryReservation, OwnedBatch, copy_event_cells,
    estimate_event_copy_cell_bytes, estimate_read_value_bytes, event_relayout_bound, from_field,
    read_entity_id, read_execution_value, relayout_event_column, to_field,
};
use rtdb_format::data::{EventBatch, EventRef, StoredEvent, StoredOperation, owned_event_batch, keyed_put_batch};
use rtdb_trie::{Bucketer, EventRow, IidReader, MemoryHashTrie, TrieConfig, TrieNode};
use rtdb_types::{Anomaly, BlockIndex, EntityId, EventHeader, EventOp, EventPosition, Iid, Interval, LogicalType, Result, RowVersion, TableRef, Timestamp, TransactionKey, Value};

use crate::prepared_order::{next_landing_position, same_stage_order, shift_metadata_position};
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
        let captured = matches!(mode, EventBuild::CopyCaptured);
        let rows = if captured { None } else { event_column_types(events) };
        let has_put = captured || rows.is_some() || !declared.is_empty();
        let fields = match rows { Some(rows) => declare_column_types(&rows, declared), None => declared.clone() };
        let copy = !matches!(mode, EventBuild::Write);
        Self::from_owned(owned_event_batch(events, has_put.then_some(fields), copy, budget)?)
    }
    fn has_system_time(&self, system_time: Timestamp) -> bool {
        self.system_times.is_none_or(|times| times == (system_time, system_time))
    }
    fn has_canonical_iids(&self) -> Option<bool> {
        self.verify_canonical_iids(self.arrow_reservation().budget(), &mut None)
    }
    fn verify_canonical_iids(
        &self, budget: &Arc<MemoryBudget>, scratch: &mut Option<Arc<MemoryReservation>>,
    ) -> Option<bool> {
        if let Some(proof) = self.canonical_iids.get() { return Some(*proof); }
        let mut puts = self.headers.0.iter().enumerate().filter(|(_, header)| header.op == EventOp::Put);
        if let Some(first) = puts.next() {
            let operations = self.arrow().column_by_name("op")?.as_any().downcast_ref::<UnionArray>()?;
            let arrow_schema::DataType::Union(legs, _) = operations.data_type() else { return None; };
            let (put_tag, _) = legs.iter().find(|(_, leg)| leg.name() == "put")?;
            let put = operations.child(put_tag).as_any().downcast_ref::<StructArray>()?;
            let (index, field) = put.fields().iter().enumerate().find(|(_, field)| field.name() == "_id")?;
            let array = put.column(index).as_ref();
            if scratch.is_none() { *scratch = Some(budget.reserve(0).ok()?); }
            let charge = Arc::get_mut(scratch.as_mut()?)?;
            let mut typed = None;
            for (row, header) in std::iter::once(first).chain(puts) {
                let offset = operations.value_offset(row);
                let bound = estimate_read_value_bytes(field, array, offset).ok()?
                    .checked_mul(3)?.checked_add(1024)?;
                if bound > charge.bytes() { charge.grow_to(bound).ok()?; }
                let typed = *typed.get_or_insert_with(|| self.put_field_types.get(index)
                    .and_then(|logical| to_field(field.name(), logical).ok())
                    .is_some_and(|canonical| canonical.data_type() == field.data_type() && canonical.metadata() == field.metadata()));
                let iid = if typed {
                    let cell = ArrowCell { field, array, row: offset }.physical().ok()?;
                    cell.signed_integer().map(rtdb_columnar::EntityIdRef::Integer)
                        .or_else(|| read_entity_id(cell.field, cell.array, cell.row).ok())?.iid()
                } else {
                    read_execution_value(field, array, offset).ok()?.entity_id().ok()?.iid()
                };
                if iid != header.iid {
                    let _ = self.canonical_iids.set(false);
                    return Some(false);
                }
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
    declared_columns: StructFields<LogicalType>,
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
        Self { table, block_index, chunks: Vec::new(), row_count: 0, lineage: LiveRowLineage::empty(), column_types: StructFields::new(), declared_columns: StructFields::new(), trie: MemoryHashTrie::new(config), last_system_time: None, canonical_entity_iids: true, located: LocatedLeaves::default() }
    }
    pub fn table(&self) -> &TableRef { &self.table }
    pub fn block_index(&self) -> BlockIndex { self.block_index }
    pub fn row_count(&self) -> u32 { self.row_count }
    pub fn column_types(&self) -> &StructFields<LogicalType> { &self.column_types }
    pub fn declared_columns(&self) -> &StructFields<LogicalType> { &self.declared_columns }
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
        let mut next = self.with_appended_schema(&write.column_types, system_time)?;
        crate::catalog::add_declarations(&mut next.declared_columns, &write.declared_columns);
        if let Some(batch) = &write.batch {
            self.validate_append(std::iter::once(batch.as_ref()), write.adopted_history, system_time)?;
            next.canonical_entity_iids = self.canonical_entity_iids && batch.has_canonical_iids().unwrap_or(false);
            next.append_indexed_chunk(self.row_count, Arc::clone(batch))?;
            next.merge_small_chunks()?;
        }
        Ok(next)
    }

    fn with_appended_schema(&self, column_types: &StructFields<LogicalType>, system_time: Timestamp) -> Result<Self> {
        if self.last_system_time.is_some_and(|previous| previous > system_time) {
            return Err(invalid("Live table system time moved backwards"));
        }
        let mut next = self.clone();
        next.column_types = merge_column_types(&next.column_types, column_types);
        next.last_system_time = Some(system_time);
        Ok(next)
    }

    fn validate_append<'a>(
        &self, batches: impl Iterator<Item = &'a IndexBatch> + Clone, adopted_history: bool, system_time: Timestamp,
    ) -> Result<()> {
        let keyed = self.table.is_owned_event_data();
        if batches.clone().any(|batch| batch.view().has_entity_keys() != keyed) {
            return Err(invalid("The table write has the wrong physical entity-key layout"));
        }
        if adopted_history {
            if self.row_count != 0 || !self.chunks.is_empty() {
                return Err(invalid("An adopted history must start a virgin live table"));
            }
        } else if batches.clone().any(|batch| !batch.has_system_time(system_time)) {
            return Err(invalid("Imported event system time differs from its transaction"));
        }
        if !adopted_history && keyed {
            let mut headers = batches.flat_map(|batch| batch.headers.0.iter()).peekable();
            if let Some(first) = headers.peek() {
                let mut previous = if self.last_system_time == Some(system_time) && self.row_count > 0 {
                    let header = self.header(self.row_count - 1)?;
                    (header.system_from == system_time
                        && header.position.map(|position| position.transaction)
                            == first.position.map(|position| position.transaction))
                        .then_some(header.commit_position)
                } else {
                    None
                };
                for header in headers {
                    let position = header.commit_position;
                    if previous.is_some_and(|previous| {
                        if previous == 0 { position != 0 } else { position <= previous }
                    }) {
                        return Err(invalid("Owned commit positions must increase in transaction row order"));
                    }
                    previous = Some(position);
                }
            }
        }
        Ok(())
    }

    fn append_indexed_chunk(&mut self, chunk_start: u32, batch: Arc<IndexBatch>) -> Result<()> {
        let count = u32::try_from(batch.len()).map_err(invalid)?;
        let first_new_row = self.row_count;
        self.row_count = crate::row_address::append_chunk(&mut self.chunks, chunk_start, count, batch)
            .ok_or_else(|| invalid("Live table exceeds u32 row address space"))?;
        let added = self.row_count.checked_sub(first_new_row).ok_or_else(|| invalid("Merged chunk does not cover the retained rows"))?;
        self.trie = self.trie.add_range(self, first_new_row, added)?;
        if added > 0 { self.lineage.append(self.row_count); }
        Ok(())
    }

    fn can_merge_chunk(previous: &IndexBatch, rows: usize, schema: &SchemaRef) -> bool {
        const MERGED_CHUNK_ROWS: usize = 8192;
        previous.len() <= rows && rows <= MERGED_CHUNK_ROWS.saturating_sub(previous.len())
            && previous.arrow().schema_ref() == schema
    }

    fn coalesce_pending_runs<'a>(
        &'a self, runs: impl Iterator<Item = &'a IndexBatch> + Clone, declared: &StructFields<LogicalType>,
        system_time: Timestamp, budget: &Arc<MemoryBudget>,
    ) -> Result<Option<Self>> {
        let Some(first) = runs.clone().next() else { return Ok(None); };
        let schema = first.arrow().schema_ref();
        let mut rows = runs.clone().try_fold(0usize, |rows, batch| {
            rows.checked_add(batch.len()).ok_or_else(|| invalid("Write batch size overflow"))
        })?;
        let mut start_chunk = self.chunks.len();
        // Earlier chunks can become eligible as the candidate grows, so plan the full tail before copying.
        for (_, previous) in self.chunks.iter().rev() {
            if !Self::can_merge_chunk(previous, rows, schema) { break; }
            rows += previous.len();
            start_chunk -= 1;
        }
        let tail = &self.chunks[start_chunk..];
        let Some((start, oldest)) = tail.first() else { return Ok(None); };

        let column_types = declare_column_types(first.put_types(), declared);
        let mut next = self.with_appended_schema(&column_types, system_time)?;
        self.validate_append(runs.clone(), false, system_time)?;
        if next.canonical_entity_iids {
            let mut scratch = None;
            next.canonical_entity_iids = runs.clone().all(|batch| batch.verify_canonical_iids(budget, &mut scratch).unwrap_or(false));
        }
        let inputs = tail.iter().map(|(_, batch)| batch.as_ref()).chain(runs);
        let merged = Self::concat_live_chunks(schema, inputs, oldest.arrow_reservation().budget())?;
        next.chunks.truncate(start_chunk);
        next.append_indexed_chunk(*start, merged)?;
        Ok(Some(next))
    }

    /// Merges the newest chunk into the one before it while it is at least as large, they share a layout and together
    /// stay small, so a table written a row per transaction is read from chunks of up to 8192 rows.
    /// Chunks keep their row addresses, since a merged chunk starts where the earlier one did.
    fn merge_small_chunks(&mut self) -> Result<()> {
        while let [.., (_, previous), (_, last)] = self.chunks.as_slice() {
            if !Self::can_merge_chunk(previous, last.len(), last.arrow().schema_ref()) { break; }
            let merged = Self::concat_live_chunks(previous.arrow().schema_ref(), [previous.as_ref(), last.as_ref()].into_iter(),
                previous.arrow_reservation().budget())?;
            crate::row_address::replace_last_two(&mut self.chunks, merged);
        }
        Ok(())
    }

    fn concat_live_chunks<'a>(
        schema: &SchemaRef, batches: impl Iterator<Item = &'a IndexBatch> + Clone, budget: &Arc<MemoryBudget>,
    ) -> Result<Arc<IndexBatch>> {
        let bound = batches.clone().try_fold(4096usize, |bytes, batch| {
            batch.arrow().get_array_memory_size().checked_mul(2).and_then(|size| bytes.checked_add(size))
                .ok_or_else(|| invalid("Merged live chunk exceeds addressable memory"))
        })?;
        let mut reservation = budget.reserve(bound)?;
        let merged = arrow_select::concat::concat_batches(schema, batches.map(IndexBatch::arrow)).map_err(invalid)?;
        // Dense union legs can outgrow the initial bound even when the chunks have the same layout.
        let size = merged.get_array_memory_size();
        let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("A merged chunk's charge is shared"))?;
        lease.grow_to(lease.bytes().max(size))?;
        lease.shrink_to(size)?;
        IndexBatch::from_owned(OwnedBatch::with_reservation(merged, reservation)?)
    }
    pub fn ordered_events(&self) -> Result<Vec<EventRow<RowSource>>> {
        self.trie.sorted_rows(self)?.into_iter().map(|row| {
            let source = self.row(row)?;
            Ok(EventRow { header: source.header()?, source })
        }).collect()
    }
    pub fn block_pages(&self, memory: &Arc<MemoryBudget>) -> Result<BlockPages<'_>> {
        let _reservation = memory.reserve(self.chunks.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("Invalid block page inventory size"))?)?;
        let include_origins = self.chunks.iter().any(|(_, batch)| batch.view().has_origin_column());
        let keyed = self.table.is_owned_event_data();
        let recency = !self.chunks.is_empty() && self.chunks.iter().all(|(_, batch)| batch.arrow().column_by_name("_recency").is_some());
        let (schema, schema_reservation) = rtdb_format::data::owned_live_event_schema(&self.column_types, recency, memory)?;
        // Run ends inside a PUT still go through the live index's plain union admission.
        if let Ok(op) = schema.field_with_name("op") && has_run_end(op.data_type()) {
            from_field(op)?;
        }
        let include_positions = self.chunks.iter().any(|(_, batch)| batch.view().has_position_column());
        let topology = self.table.is_owned_world_topology();
        let schema = if include_origins || include_positions || topology || keyed {
            let mut fields = schema.fields().to_vec();
            if keyed {
                fields.push(Arc::new(rtdb_format::data::entity_key_field()));
            }
            if keyed || topology { fields.push(Arc::new(rtdb_format::data::commit_position_field())); }
            if include_origins {
                fields.push(Arc::new(rtdb_format::data::event_origin_field()));
            }
            if include_positions { fields.push(Arc::new(rtdb_format::data::event_position_field())); }
            arrow_schema::Schema::new(fields)
        } else { schema };
        let sources: Option<Vec<_>> = (!keyed && !recency && self.chunks.iter().all(|(_, batch)| batch.arrow().schema_ref().as_ref() == &schema))
            .then(|| self.chunks.iter().map(|(_, batch)| &batch.owned).collect());
        let (schema, _schema_reservation) = match sources.as_ref().and_then(|batches| batches.first()) {
            Some(batch) => (batch.batch().schema(), None),
            None => (Arc::new(schema), Some(schema_reservation)),
        };
        Ok(BlockPages { table: self, sources, schema, include_origins, memory: Arc::clone(memory), _reservation, _schema_reservation })
    }
    pub fn block_metadata(&self) -> Result<(TableMeta, Option<Arc<MemoryReservation>>)> {
        let (hlls, reservation) = if let Some((_, first)) = self.chunks.first() {
            let budget = first.arrow_reservation().budget();
            let mut reservation = budget.snapshot_budget().unwrap_or(budget).reserve(0)?;
            let batches = self.batches().map(|batch| (batch.arrow(), 0..batch.len()));
            let hlls = rtdb_format::hll::from_event_batches(batches, Arc::get_mut(&mut reservation).expect("HLL fold owns its reservation"))?;
            (hlls, Some(reservation))
        } else {
            (BTreeMap::new(), None)
        };
        let metadata = TableMeta { column_types: self.column_types.clone(), declared_columns: self.declared_columns.clone(), row_count: i64::from(self.row_count), hlls };
        Ok((metadata, reservation))
    }
}

impl IidReader for LiveTable {
    fn iid(&self, row: u32) -> Result<Iid> { self.header(row).map(|header| header.iid) }
}

pub struct BlockPages<'a> {
    table: &'a LiveTable,
    sources: Option<Vec<&'a OwnedBatch>>,
    schema: SchemaRef,
    include_origins: bool,
    memory: Arc<MemoryBudget>,
    _reservation: Arc<MemoryReservation>,
    _schema_reservation: Option<Arc<MemoryReservation>>,
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
        if self.sources.is_none() {
            match self.copy_relayout(rows) {
                Ok(Some(batch)) => return Ok(batch),
                Ok(None) => (),
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
        let batch = owned_event_batch(&events, Some(self.table.column_types.clone()), true, &self.memory)?;
        let batch = if self.table.table.is_owned_world_topology() {
            rtdb_format::data::with_topology_commit_positions(batch, &self.memory)?
        } else {
            batch
        };
        if self.include_origins && batch.batch().column_by_name(rtdb_format::data::EVENT_ORIGIN_FIELD).is_none() {
            return rtdb_format::data::append_origins(&batch, std::iter::repeat_n(None, events.len()), &self.memory);
        }
        Ok(batch)
    }

    fn copy_relayout(&self, rows: &[u32]) -> Result<Option<OwnedBatch>> {
        if rows.is_empty() { return Ok(None); }
        let bound = rows.len().checked_mul(96).and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| invalid("Invalid block row selection size"))?;
        let _selection = self.memory.reserve(bound)?;
        let mut selected = BTreeMap::<usize, Vec<usize>>::new();
        let mut positions = Vec::with_capacity(rows.len());
        for &row in rows {
            let (chunk, offset) = self.table.locate_row(row)?;
            selected.entry(chunk).or_default().push(offset as usize);
            positions.push((chunk, offset as usize));
        }
        let mut reservation = Vec::new();
        let mut aligned = Vec::with_capacity(selected.len());
        let mut slots = BTreeMap::new();
        for (chunk, rows) in selected {
            let input = self.table.chunks[chunk].1.arrow();
            let Some(columns) = self.align_chunk(input, &rows, &mut reservation)? else { return Ok(None); };
            slots.insert(chunk, aligned.len());
            aligned.push(columns);
        }
        let positions = positions.into_iter().map(|(chunk, row)| Some((slots[&chunk], row))).collect::<Vec<_>>();
        let mut builder = BatchBuilder::new(bound, &self.memory)?;
        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for (index, field) in self.schema.fields().iter().enumerate() {
            let arrays = aligned.iter().map(|columns| columns[index].as_ref()).collect::<Vec<_>>();
            let column = if field.name() == "op" {
                self.copy_operations(field, &arrays, &positions, &mut builder)?
            } else {
                copy_block_column(field, &arrays, &positions, &mut builder)?
            };
            columns.push(column);
        }
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(invalid)?;
        builder.finish(batch).map(Some)
    }

    fn copy_operations(
        &self, field: &Field, arrays: &[&dyn Array], positions: &[Option<(usize, usize)>], builder: &mut BatchBuilder,
    ) -> Result<ArrayRef> {
        let DataType::Union(legs, UnionMode::Dense) = field.data_type() else { return Err(invalid("Live operations must be a union")); };
        let ops = arrays.iter().map(|array| array.as_any().downcast_ref::<UnionArray>()
            .ok_or_else(|| invalid("Live operations must be a union"))).collect::<Result<Vec<_>>>()?;
        let mut picks = vec![Vec::new(); legs.len()];
        let mut tags = Vec::with_capacity(positions.len());
        let mut offsets = Vec::with_capacity(positions.len());
        for &(source, row) in positions.iter().flatten() {
            let tag = ops[source].type_id(row);
            let index = legs.iter().position(|(candidate, _)| candidate == tag)
                .ok_or_else(|| invalid("Live operations have an unknown leg"))?;
            tags.push(tag);
            offsets.push(i32::try_from(picks[index].len()).map_err(invalid)?);
            picks[index].push(Some((source, ops[source].value_offset(row))));
        }
        let children = legs.iter().zip(picks).map(|((tag, field), picks)| {
            if field.data_type() == &DataType::Null {
                return Ok(Arc::new(NullArray::new(picks.len())) as ArrayRef);
            }
            let DataType::Struct(fields) = field.data_type() else { return Err(invalid("Live put must be a struct")); };
            let puts = ops.iter().map(|op| op.child(tag).as_any().downcast_ref::<StructArray>()
                .ok_or_else(|| invalid("Live put must be a struct"))).collect::<Result<Vec<_>>>()?;
            let columns = fields.iter().enumerate().map(|(index, field)| {
                let arrays = puts.iter().map(|put| put.column(index).as_ref()).collect::<Vec<_>>();
                copy_block_column(field, &arrays, &picks, builder)
            }).collect::<Result<Vec<_>>>()?;
            rtdb_columnar::build_struct_array(fields.clone(), &columns, picks.len(), None)
        }).collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(UnionArray::try_new(legs.clone(), tags.into(), Some(offsets.into()), children).map_err(invalid)?))
    }

    fn align_chunk(
        &self, input: &RecordBatch, selected: &[usize], scratch: &mut Vec<Arc<MemoryReservation>>,
    ) -> Result<Option<Vec<ArrayRef>>> {
        if input.schema_ref().fields().iter().any(|field| self.schema.index_of(field.name()).is_err()) {
            return Ok(None);
        }
        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            let source = input.schema_ref().column_with_name(field.name());
            let array = match source {
                Some((index, source)) if source == field.as_ref() && field.name() != "op" => Arc::clone(input.column(index)),
                Some((index, source)) if field.name() == "op" => {
                    let Some(array) = self.align_operations(source, input.column(index), field, selected, scratch)? else { return Ok(None); };
                    array
                }
                None if field.name() == rtdb_format::data::EVENT_ORIGIN_FIELD => {
                    let bytes = input.num_rows().checked_mul(64).and_then(|bytes| bytes.checked_add(1024))
                        .ok_or_else(|| invalid("Page row allocation overflow"))?;
                    scratch.push(self.memory.reserve(bytes)?);
                    arrow_array::new_null_array(field.data_type(), input.num_rows())
                }
                _ => return Ok(None),
            };
            columns.push(array);
        }
        Ok(Some(columns))
    }

    fn align_operations(
        &self, source: &Field, array: &ArrayRef, target: &Field, selected: &[usize],
        scratch: &mut Vec<Arc<MemoryReservation>>,
    ) -> Result<Option<ArrayRef>> {
        let DataType::Union(source_legs, UnionMode::Dense) = source.data_type() else { return Ok(None); };
        let DataType::Union(target_legs, UnionMode::Dense) = target.data_type() else { return Ok(None); };
        if source_legs.iter().any(|(tag, field)| !target_legs.iter().any(|(candidate, leg)| candidate == tag && leg.name() == field.name())) {
            return Ok(None);
        }
        let ops = array.as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("Live operations must be a union"))?;
        let mut children = Vec::with_capacity(target_legs.len());
        for (tag, target) in target_legs.iter() {
            let Some((_, source)) = source_legs.iter().find(|(candidate, _)| *candidate == tag) else {
                children.push(arrow_array::new_empty_array(target.data_type()));
                continue;
            };
            let source_array = ops.child(tag);
            if target.name() == "put" {
                if has_run_end(target.data_type()) || source_array.null_count() != 0
                    || selected.iter().any(|&row| ops.type_id(row) == tag
                        && !rtdb_format::data::can_copy_put_cell(source, source_array.as_ref(), ops.value_offset(row))) {
                    return Ok(None);
                }
            }
            let aligned = if source == target {
                Arc::clone(source_array)
            } else {
                let Ok(bound) = event_relayout_bound(source_array.as_ref(), source, target) else { return Ok(None); };
                scratch.push(self.memory.reserve(bound)?);
                let Ok((field, aligned)) = relayout_event_column(Arc::clone(source_array), source, target) else { return Ok(None); };
                if field != **target { return Ok(None); }
                aligned
            };
            children.push(aligned);
        }
        let array = UnionArray::try_new(target_legs.clone(), ops.type_ids().clone(), ops.offsets().cloned(), children).map_err(invalid)?;
        Ok(Some(Arc::new(array)))
    }
}

fn copy_block_column(
    field: &Field, arrays: &[&dyn Array], positions: &[Option<(usize, usize)>], builder: &mut BatchBuilder,
) -> Result<ArrayRef> {
    let bound = positions.iter().flatten().try_fold(4096usize, |bytes, &(source, row)| {
        estimate_event_copy_cell_bytes(field, arrays[source], row)?.checked_mul(4)
            .and_then(|size| bytes.checked_add(size)).ok_or_else(|| invalid("Page row allocation overflow"))
    })?;
    builder.column(bound, || copy_event_cells(arrays, positions))
}

fn has_run_end(data_type: &DataType) -> bool {
    match data_type {
        DataType::RunEndEncoded(..) => true,
        DataType::Union(fields, _) => fields.iter().any(|(_, field)| has_run_end(field.data_type())),
        DataType::Struct(fields) => fields.iter().any(|field| has_run_end(field.data_type())),
        DataType::List(field) | DataType::FixedSizeList(field, _) | DataType::Map(field, _) => has_run_end(field.data_type()),
        _ => false,
    }
}

#[derive(Clone, Debug)]
pub struct TableWrite {
    column_types: StructFields<LogicalType>,
    declared_columns: StructFields<LogicalType>,
    batch: Option<Arc<IndexBatch>>,
    adopted_history: bool,
}
impl TableWrite {
    pub fn new(column_types: StructFields<LogicalType>, batch: Option<Arc<IndexBatch>>) -> Self {
        let column_types = match &batch { Some(batch) => declare_column_types(batch.put_types(), &column_types), None => column_types };
        Self { column_types, declared_columns: StructFields::new(), batch, adopted_history: false }
    }
    pub fn with_declarations(mut self, declared_columns: StructFields<LogicalType>) -> Self {
        self.column_types = declare_column_types(&self.column_types, &declared_columns);
        self.declared_columns = declared_columns;
        self
    }
    #[doc(hidden)]
    pub fn as_adopted_history(mut self) -> Self {
        self.adopted_history = true;
        self
    }
    pub fn is_adopted_history(&self) -> bool { self.adopted_history }
    pub fn declared_columns(&self) -> &StructFields<LogicalType> { &self.declared_columns }
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
    segments: OnceLock<BTreeMap<TableRef, Arc<LiveTable>>>,
}
impl StagedWrite {
    pub fn new(tx_key: TransactionKey, predecessor: Option<TransactionKey>, block_index: BlockIndex, tables: BTreeMap<TableRef, TableWrite>) -> Result<Self> {
        if predecessor.is_some_and(|previous| previous.tx_id >= tx_key.tx_id || previous.system_time > tx_key.system_time) {
            return Err(invalid("Transaction key does not advance its predecessor"));
        }
        let mut adopted = BTreeMap::<&str, (bool, bool)>::new();
        let mut positions = std::collections::BTreeSet::new();
        for (table, write) in &tables {
            if write.adopted_history {
                let relation = table.owned_effect_relation_name()
                    .filter(|name| rtdb_types::OwnedRelation::new(*name).is_ok_and(|relation| relation.is_root_data()))
                    .ok_or_else(|| invalid("An adopted history must be a Root data or journal table"))?;
                let roles = adopted.entry(relation).or_default();
                let role = if table.is_owned_change_journal() { &mut roles.1 } else { &mut roles.0 };
                if std::mem::replace(role, true) {
                    return Err(invalid("An adoption repeats a history role"));
                }
                let batch = write.batch().filter(|batch| !batch.is_empty())
                    .ok_or_else(|| invalid("An adopted history must have events"))?;
                let mut previous = None;
                for header in batch.headers.0.iter() {
                    let position = header.position.ok_or_else(|| invalid("An adopted event has no original position"))?;
                    if predecessor.is_none_or(|basis| position.transaction > basis.tx_id || header.system_from > basis.system_time)
                        || header.system_from >= tx_key.system_time
                        || position.transaction.0 < 0
                        || previous.is_some_and(|prior: EventPosition| position <= prior)
                        || !positions.insert(position)
                        || !matches!(header.op, EventOp::Put | EventOp::Delete)
                        || (table.is_owned_change_journal() && (header.op != EventOp::Put || header.valid_time != Interval::ALL))
                    {
                        return Err(invalid("An adopted event is outside the original ordered history"));
                    }
                    previous = Some(position);
                }
            }
            if let Some(batch) = write.batch() {
                if !write.adopted_history && !batch.has_system_time(tx_key.system_time) {
                    return Err(invalid("Staged event has another transaction's system time"));
                }
                if batch.view().has_commit_positions() && !table.is_owned_event_data() && !table.is_owned_world_topology()
                    && batch.headers.0.iter().any(|header| header.commit_position != 0)
                {
                    return Err(invalid("A commit position requires an owned fact or world topology"));
                }
            }
        }
        if !adopted.is_empty() && (adopted.len() != 1 || adopted.values().next() != Some(&(true, true))) {
            return Err(invalid("An adoption requires one data and journal pair"));
        }
        Ok(Self { tx_key, predecessor, block_index, tables, segments: OnceLock::new() })
    }
    pub fn tx_key(&self) -> TransactionKey { self.tx_key }
    pub fn predecessor(&self) -> Option<TransactionKey> { self.predecessor }
    pub fn block_index(&self) -> BlockIndex { self.block_index }
    pub fn tables(&self) -> &BTreeMap<TableRef, TableWrite> { &self.tables }
    pub fn row_count(&self) -> Result<u64> { self.tables.values().try_fold(0u64, |count, write| count.checked_add(write.row_count() as u64).ok_or_else(|| invalid("Transaction row count overflow"))) }

    pub(crate) fn staged_segments(&self) -> Result<&BTreeMap<TableRef, Arc<LiveTable>>> {
        if let Some(segments) = self.segments.get() { return Ok(segments); }
        let segments = self.tables.iter().map(|(table, write)| {
            let segment = LiveTable::new(table.clone(), self.block_index, TrieConfig::default())
                .append(write, self.tx_key.system_time)?;
            Ok((table.clone(), Arc::new(segment)))
        }).collect::<Result<_>>()?;
        Ok(self.segments.get_or_init(|| segments))
    }

    /// Attach sealed application tables to the order-assigned transaction metadata.
    /// Both stages belong to the same assigned source transaction and durable predecessor.
    pub fn install_prepared_generation(&mut self, prepared: Self, memory: &Arc<MemoryBudget>) -> Result<()> {
        let coordinates = |key: TransactionKey| (key.tx_id.0, key.system_time.0);
        if !same_stage_order(
            self.predecessor.map(coordinates), prepared.predecessor.map(coordinates),
            self.block_index.0, prepared.block_index.0, coordinates(self.tx_key), coordinates(prepared.tx_key),
        ) || prepared.tables.keys().any(|table| self.tables.contains_key(table))
        {
            return Err(invalid("Prepared generation differs from the ordered transaction"));
        }
        let effect_count = prepared.row_count()?;
        self.segments.take();
        self.shift_event_ordinals(effect_count, memory)?;
        self.tables.extend(prepared.tables);
        Ok(())
    }

    fn shift_event_ordinals(&mut self, shift: u64, memory: &Arc<MemoryBudget>) -> Result<()> {
        let original = self.tx_key.tx_id;
        for write in self.tables.values_mut() {
            let Some(batch) = &write.batch else { continue; };
            if batch.is_empty() { continue; }
            let _positions = memory.reserve(batch.len().checked_mul(32).ok_or_else(|| invalid("Event positions exceed memory"))?)?;
            let positions = batch.headers.0.iter().map(|header| {
                let event = header.position.map(|position| (position.transaction.0, position.ordinal));
                let (transaction, ordinal) = shift_metadata_position(event, original.0, shift)
                    .ok_or_else(|| invalid("Prepared event has another original order or exhausted its ordinal"))?;
                Ok(EventPosition { transaction: rtdb_types::TxId(transaction), ordinal })
            }).collect::<Result<Vec<_>>>()?;
            let rebound = rtdb_format::data::append_event_positions(&batch.owned, positions, memory)?;
            write.batch = Some(IndexBatch::from_owned(rebound)?);
        }
        Ok(())
    }

    pub fn assign_event_origins(mut self, memory: &Arc<MemoryBudget>) -> Result<Self> {
        self.segments.take();
        let commit = rtdb_types::EventOrigin::new_commit();
        let block = self.block_index;
        let mut first = 0u64;
        for table in self.tables.values_mut() {
            let Some(batch) = &table.batch else { continue; };
            if batch.is_empty() || table.adopted_history { continue; }
            let last = first.checked_add(batch.len() as u64).ok_or_else(|| invalid("Event ordinal overflow"))?;
            if !batch.view().has_origin_column() {
                let origins = (first..last).map(|ordinal| Some(rtdb_types::EventOrigin::at(commit, ordinal, block)));
                let resolved = rtdb_format::data::append_origins(&batch.owned, origins, memory)?;
                table.batch = Some(IndexBatch::from_owned(resolved)?);
            }
            first = last;
        }
        Ok(self)
    }
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
pub struct PreparedSearchDelta {
    pub encoded: bytes::Bytes,
    pub batch: OwnedBatch,
}

pub enum SearchDelta {
    Prepared(PreparedSearchDelta),
    Replayed(bytes::Bytes),
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
}

#[derive(Default)]
struct PendingTable {
    declared: StructFields<LogicalType>,
    sql_declarations: StructFields<LogicalType>,
    adopted_history: Option<Arc<IndexBatch>>,
    events: Vec<StoredEvent>,
    batches: Vec<PendingBatch>,
    reservations: Vec<Arc<MemoryReservation>>,
}

struct PendingBatch {
    preceding_events: usize,
    batch: Arc<IndexBatch>,
    can_concat: bool,
}

impl PendingTable {
    fn position(&self) -> WritePosition {
        WritePosition { events: self.events.len(), batches: self.batches.len() }
    }

    fn compatible_runs_since(&self, written: WritePosition) -> Option<&[PendingBatch]> {
        let runs = &self.batches[written.batches..];
        if written.events != self.events.len() || runs.len() < 2 { return None; }
        let first = &runs[0].batch;
        let schema = first.arrow().schema_ref();
        (first.put_types == self.declared && runs.iter().all(|run| {
            run.can_concat && run.batch.arrow().schema_ref() == schema
        })).then_some(runs)
    }

    fn concat_runs(runs: &[PendingBatch], budget: &Arc<MemoryBudget>) -> Result<Arc<IndexBatch>> {
        let schema = runs[0].batch.arrow().schema_ref();
        let bytes = runs.iter().try_fold(4096usize, |bytes, run| {
            run.batch.arrow().get_array_memory_size().checked_mul(4)
                .and_then(|size| bytes.checked_add(size)).ok_or_else(|| invalid("Write batch size overflow"))
        })?;
        let mut reservation = budget.reserve(bytes)?;
        let combined = arrow_select::concat::concat_batches(schema, runs.iter().map(|run| run.batch.arrow())).map_err(invalid)?;
        Arc::get_mut(&mut reservation).expect("write concatenation owns its reservation").shrink_to(combined.get_array_memory_size())?;
        IndexBatch::from_owned(OwnedBatch::with_reservation(combined, reservation)?)
    }

    fn copy_since(&self, written: WritePosition, budget: &Arc<MemoryBudget>) -> Result<Arc<IndexBatch>> {
        if let Some(runs) = self.compatible_runs_since(written) {
            return Self::concat_runs(runs, budget);
        }
        self.assemble_since(written, budget)
    }

    fn append_since(
        &self, written: WritePosition, segment: &LiveTable, system_time: Timestamp, budget: &Arc<MemoryBudget>,
    ) -> Result<LiveTable> {
        let batch = if written == self.position() {
            None
        } else if let Some(runs) = self.compatible_runs_since(written) {
            let batches = runs.iter().map(|run| run.batch.as_ref());
            if let Some(next) = segment.coalesce_pending_runs(batches, &self.declared, system_time, budget)? {
                return Ok(next);
            }
            Some(Self::concat_runs(runs, budget)?)
        } else {
            Some(self.assemble_since(written, budget)?)
        };
        segment.append(&TableWrite::new(self.declared.clone(), batch), system_time)
    }

    fn assemble_since(&self, written: WritePosition, budget: &Arc<MemoryBudget>) -> Result<Arc<IndexBatch>> {
        let events = &self.events[written.events..];
        let batches = &self.batches[written.batches..];
        if batches.is_empty() {
            return IndexBatch::copy_events(events, &self.declared, budget);
        }
        if let [only] = batches && events.is_empty() && only.batch.put_types == self.declared {
            return Ok(Arc::clone(&only.batch));
        }
        let size_error = || invalid("Write batch size overflow");
        let positions = || batches.iter().map(|page| (page.preceding_events, Some(&page.batch)))
            .chain(std::iter::once((self.events.len(), None)));
        let event_count = batches.iter().try_fold(events.len(), |count, page| {
            count.checked_add(page.batch.len()).ok_or_else(size_error)
        })?;
        let scratch = event_count.checked_mul(512).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
        let scratch = events.iter().try_fold(scratch, |bytes, event| match &event.operation {
            StoredOperation::Put(document) => Value::document_retained_bytes(document)?.checked_mul(4)
                .and_then(|size| bytes.checked_add(size)).ok_or_else(size_error),
            _ => Ok(bytes),
        })?;
        let scratch = batches.iter().try_fold(scratch, |bytes, page| {
            page.batch.put_types.iter().try_fold(bytes, |bytes, (name, ty)| {
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
                EventBatch::new(batch.batch().clone())?.extend_page_events(&mut ordered)?;
            }
            if let Some(batch) = batch {
                batch.events.extend_page_events(&mut ordered)?;
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
    WorldCarriers(u32),
    Archive {
        remaining: std::collections::BTreeSet<u32>,
        direct: Option<DirectArchiveEffect>,
    },
}

struct DirectArchiveEffect {
    table: TableRef,
    position: u32,
    staged: bool,
}

pub struct TransactionBuffer {
    base: Snapshot,
    tx_key: TransactionKey,
    budget: Arc<MemoryBudget>,
    tables: BTreeMap<TableRef, PendingTable>,
    owned_positions: Option<OwnedCommitPositions>,
    next_event_ordinal: u64,
    /// Each table's transaction segment as last read, with the events and declarations it holds; a read appends only what was written since.
    segments: std::sync::Mutex<BTreeMap<TableRef, (Arc<LiveTable>, WritePosition, StructFields<LogicalType>)>>,
}

impl TransactionBuffer {
    pub fn new(base: Snapshot, tx_key: TransactionKey, budget: Arc<MemoryBudget>) -> Result<Self> {
        StagedWrite::new(tx_key, base.tx_basis(), base.block_index(), BTreeMap::new())?;
        Ok(Self { base, tx_key, budget, tables: BTreeMap::new(), owned_positions: None, next_event_ordinal: 0, segments: Default::default() })
    }
    pub fn tx_key(&self) -> TransactionKey { self.tx_key }
    pub fn retain_owned_positions(&mut self) -> Result<()> {
        self.select_owned_positions(OwnedCommitPositions::Landing(0))
    }
    pub fn retain_world_carrier_positions(&mut self) -> Result<()> {
        self.select_owned_positions(OwnedCommitPositions::WorldCarriers(0))
    }

    fn positions_world_carrier(table: &TableRef) -> bool {
        table.owned_world_root_table().and_then(|root| {
            root.owned_storage_name().map(|name| name == "fact_unit_column_payloads")
        }) == Some(true)
    }

    pub fn retain_archived_owned_positions(&mut self, positions: Vec<u32>) -> Result<()> {
        if positions.first().is_some_and(|first| *first == 0)
            || !positions.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(invalid("Archived commit positions must be positive and strictly ordered"));
        }
        self.select_owned_positions(OwnedCommitPositions::Archive {
            remaining: positions.into_iter().collect(),
            direct: None,
        })
    }

    pub fn begin_direct_archive_effect(&mut self, table: TableRef, position: u32) -> Result<()> {
        let Some(OwnedCommitPositions::Archive { remaining, direct }) = &mut self.owned_positions
        else { return Err(invalid("A direct archive effect requires an explicit replay plan")); };
        if !table.is_owned_event_data() || direct.is_some()
            || (position != 0 && !remaining.contains(&position))
        {
            return Err(invalid("A direct archive effect has an unexpected or repeated position"));
        }
        *direct = Some(DirectArchiveEffect { table, position, staged: false });
        Ok(())
    }

    pub fn finish_direct_archive_effect(&mut self) -> Result<()> {
        let Some(OwnedCommitPositions::Archive { direct, .. }) = &mut self.owned_positions
        else { return Err(invalid("A direct archive effect requires an explicit replay plan")); };
        if !direct.take().is_some_and(|effect| effect.staged) {
            return Err(invalid("A direct archive operation must stage exactly one owned event"));
        }
        Ok(())
    }

    fn select_owned_positions(&mut self, positions: OwnedCommitPositions) -> Result<()> {
        if self.owned_positions.is_some() || !self.tables.is_empty() {
            return Err(invalid("Commit positions must be selected before staging owned effects"));
        }
        self.owned_positions = Some(positions);
        Ok(())
    }
    pub fn retains_owned_positions(&self) -> bool {
        matches!(self.owned_positions, Some(OwnedCommitPositions::Landing(_) | OwnedCommitPositions::Archive { .. }))
    }
    pub fn require_archived_positions_consumed(&self) -> Result<()> {
        if matches!(&self.owned_positions, Some(OwnedCommitPositions::Archive { remaining, direct, .. }) if !remaining.is_empty() || direct.is_some()) {
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
    pub fn record_sql_declaration(
        &mut self, table: &TableRef, fields: &[(String, LogicalType)],
    ) -> Result<()> {
        self.declare_document_types(table, fields.iter().map(|(name, ty)| (name, ty)))?;
        let pending = self.tables.get_mut(table).ok_or_else(|| invalid("SQL declaration requires an open table write"))?;
        for (name, ty) in fields {
            let old = pending.sql_declarations.get(name);
            let reservation = self.budget.reserve(document_type_bytes(name, ty, old)?)?;
            let merged = old.map_or_else(|| ty.clone(), |old| old.join(ty));
            pending.sql_declarations.insert(name.clone(), merged);
            pending.reservations.push(reservation);
        }
        Ok(())
    }

    pub fn put(&mut self, table: TableRef, document: StructFields<Value>, valid_time: Interval) -> Result<()> {
        let id = document.get("_id").ok_or_else(|| invalid("Put document has no _id"))?.entity_id()?;
        self.append(table, StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid: id.iid(), system_from: self.tx_key.system_time, valid_time, operation: StoredOperation::Put(document), recency: None })
    }
    pub fn delete(&mut self, table: TableRef, id: &EntityId, valid_time: Interval) -> Result<()> {
        self.append(table, StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid: id.iid(), system_from: self.tx_key.system_time, valid_time, operation: StoredOperation::Delete, recency: None })
    }
    pub fn erase(&mut self, table: TableRef, id: &EntityId) -> Result<()> {
        self.erase_iid(&table, id.iid())
    }
    pub fn erase_iid(&mut self, table: &TableRef, iid: Iid) -> Result<()> {
        self.append_to(table, StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid, system_from: self.tx_key.system_time, valid_time: Interval::ALL, operation: StoredOperation::Erase, recency: None }).map(drop)
    }
    pub fn puts_metadata_unit(&self, table: &TableRef, iid: Iid, key: Option<&[u8]>) -> Result<bool> {
        let Some(pending) = self.tables.get(table) else { return Ok(false); };
        let mut put = false;
        for event in &pending.events {
            if event.iid != iid || event.entity_key.as_deref() != key { continue; }
            match &event.operation {
                StoredOperation::Erase => return Ok(false),
                StoredOperation::Put(_) => put = true,
                _ => {}
            }
        }
        for PendingBatch { batch, .. } in &pending.batches {
            for row in 0..batch.len() {
                let header = batch.header(row)?;
                if header.iid != iid || batch.view().entity_key(row)? != key { continue; }
                match header.op {
                    EventOp::Erase => return Ok(false),
                    EventOp::Put => put = true,
                    _ => {}
                }
            }
        }
        Ok(put)
    }

    pub fn append(&mut self, table: TableRef, event: StoredEvent) -> Result<()> { self.append_to(&table, event).map(drop) }
    /// As `append`, copying the table reference only for the table's first write.
    pub fn append_to(&mut self, table: &TableRef, event: StoredEvent) -> Result<Option<NonZeroU32>> {
        let position = self.next_owned_position(table)?;
        self.stage_event(table, event, position.unwrap_or(0))?;
        if let Some(position) = position {
            self.consume_owned_position(position);
        }
        Ok(position.and_then(NonZeroU32::new))
    }

    /// Stage an archive reconstruction that has no original assertion ordinal.
    pub fn append_unpositioned_replay(&mut self, table: &TableRef, event: StoredEvent) -> Result<()> {
        if !table.is_owned_event_data() || matches!(&self.owned_positions, Some(OwnedCommitPositions::Landing(_))) {
            return Err(invalid("An unpositioned replay event requires an owned archive relation"));
        }
        self.stage_event(table, event, 0)
    }

    /// Stage an archived world overlay or topology effect with its original ordinal.
    pub fn append_archived_world_effect(
        &mut self,
        table: &TableRef,
        event: StoredEvent,
        position: NonZeroU32,
    ) -> Result<()> {
        if !(table.is_owned_event_data() && table.owned_world_scope().1.is_some())
            && !table.is_owned_world_topology()
        {
            return Err(invalid("A positioned archive world effect requires its owned relation"));
        }
        self.require_archived_position(position)?;
        self.stage_event(table, event, position.get())?;
        self.consume_owned_position(position.get());
        Ok(())
    }

    /// Account for an original overlay excluded by the selected restore scope.
    pub fn omit_archived_world_effect(&mut self, position: NonZeroU32) -> Result<()> {
        self.require_archived_position(position)?;
        self.consume_owned_position(position.get());
        Ok(())
    }

    fn require_archived_position(&self, position: NonZeroU32) -> Result<()> {
        if !matches!(&self.owned_positions, Some(OwnedCommitPositions::Archive { remaining, .. }) if remaining.contains(&position.get())) {
            return Err(invalid("The archive world effect has an unexpected or repeated position"));
        }
        Ok(())
    }

    fn stage_event(&mut self, table: &TableRef, mut event: StoredEvent, position: u32) -> Result<()> {
        if event.entity_key.is_some() != table.is_owned_event_data() {
            return Err(invalid("The transaction event has the wrong physical entity-key layout"));
        }
        if !crate::event_revision::matches_transaction(event.system_from.0, self.tx_key.system_time.0) {
            return Err(invalid("Transaction event has another system time"));
        }
        if event.commit_position != 0 || event.position.is_some() {
            return Err(invalid("The transaction buffer assigns commit and event positions"));
        }
        if let StoredOperation::Put(document) = &mut event.operation { document.values_mut().try_for_each(Value::admit_object_dates)?; }
        let document_bytes = match &event.operation { StoredOperation::Put(document) => Value::document_retained_bytes(document)?, _ => 0 };
        let bytes = document_bytes.checked_add(event.entity_key.as_ref().map_or(0, |key| key.len()))
            .and_then(|bytes| bytes.checked_add(4096)).and_then(|bytes| bytes.checked_add(table.schema.len())).and_then(|bytes| bytes.checked_add(table.table.len()))
            .ok_or_else(|| invalid("Transaction event size overflow"))?;
        let reservation = self.budget.reserve(bytes)?;
        if !self.tables.contains_key(table) { self.tables.insert(table.clone(), PendingTable::default()); }
        let ordinal = self.next_event_ordinal;
        let next = ordinal.checked_add(1).ok_or_else(|| invalid("Event ordinal exhausted"))?;
        event.commit_position = position;
        event.position = Some(EventPosition { transaction: self.tx_key.tx_id, ordinal });
        let pending = self.tables.get_mut(table).ok_or_else(|| invalid("Transaction table write disappeared"))?;
        pending.events.push(event);
        pending.reservations.push(reservation);
        self.next_event_ordinal = next;
        Ok(())
    }
    fn next_owned_position(&self, table: &TableRef) -> Result<Option<u32>> {
        Ok(if table.is_owned_event_data() {
            match &self.owned_positions {
                Some(OwnedCommitPositions::Landing(previous)) => {
                    Some(next_landing_position(*previous).ok_or_else(|| invalid("Owned commit position overflow"))?)
                },
                Some(OwnedCommitPositions::WorldCarriers(previous)) if Self::positions_world_carrier(table) => {
                    Some(next_landing_position(*previous).ok_or_else(|| invalid("Owned commit position overflow"))?)
                },
                Some(OwnedCommitPositions::WorldCarriers(_)) => None,
                Some(OwnedCommitPositions::Archive { direct, .. }) => {
                    let effect = direct.as_ref().filter(|effect| &effect.table == table && !effect.staged)
                        .ok_or_else(|| invalid("An owned event differs from its direct archive effect"))?;
                    Some(effect.position)
                },
                None => None,
            }
        } else {
            None
        })
    }

    fn consume_owned_position(&mut self, position: u32) {
        match &mut self.owned_positions {
            Some(OwnedCommitPositions::Landing(previous) | OwnedCommitPositions::WorldCarriers(previous)) => *previous = position,
            Some(OwnedCommitPositions::Archive { remaining, direct, .. }) => {
                if let Some(effect) = direct {
                    effect.staged = true;
                }
                remaining.remove(&position);
            },
            None => unreachable!("only an ordered transaction has an owned position"),
        }
    }

    pub fn append_owned_put(
        &mut self, table: &TableRef, content: arrow_array::ArrayRef, mut header: EventHeader,
        key: &[u8], source_bytes: Option<usize>,
    ) -> Result<Option<NonZeroU32>> {
        if !table.is_owned_event_data() || header.commit_position != 0 {
            return Err(invalid("An owned Arrow put requires a governed table and an unassigned position"));
        }
        let position = self.next_owned_position(table)?;
        if let Some(position) = position { header.commit_position = position; }
        let _key = self.budget.reserve(key.len().checked_add(4096).ok_or_else(|| invalid("Owned key is too large"))?)?;
        let keys = arrow_array::BinaryArray::from_iter_values([key]);
        let batch = keyed_put_batch(content, &[header], keys, source_bytes, &self.budget)?;
        self.append_batch_inner(table, batch, None)?;
        if let Some(position) = position { self.consume_owned_position(position); }
        Ok(position.and_then(NonZeroU32::new))
    }

    pub fn append_batch(&mut self, table: &TableRef, batch: OwnedBatch) -> Result<()> {
        self.append_unpositioned_batch(table, batch, None)
    }

    /// Stages puts assembled from differing schemas, retaining their original declarations.
    pub fn append_assembled_put_batch(
        &mut self, table: &TableRef, batch: OwnedBatch, declarations: &StructFields<LogicalType>,
    ) -> Result<()> {
        self.append_unpositioned_batch(table, batch, Some(declarations))
    }

    fn append_unpositioned_batch(
        &mut self, table: &TableRef, batch: OwnedBatch, assembled_declarations: Option<&StructFields<LogicalType>>,
    ) -> Result<()> {
        if table.is_owned_event_data() && (self.retains_owned_positions()
            || matches!(self.owned_positions, Some(OwnedCommitPositions::WorldCarriers(_)))
                && Self::positions_world_carrier(table))
        {
            return Err(invalid("An ordered landing stages governed events in assertion order"));
        }
        self.append_batch_inner(table, batch, assembled_declarations)
    }

    fn append_batch_inner(
        &mut self, table: &TableRef, batch: OwnedBatch, assembled_declarations: Option<&StructFields<LogicalType>>,
    ) -> Result<()> {
        let rows = batch.batch().num_rows();
        if rows == 0 { return Ok(()); }
        if batch.batch().schema().index_of(rtdb_format::data::EVENT_POSITION_FIELD).is_ok() {
            return Err(invalid("The transaction buffer assigns Arrow event positions"));
        }
        let start = self.next_event_ordinal;
        let end = start.checked_add(rows as u64).ok_or_else(|| invalid("Event ordinal exhausted"))?;
        let positions = (start..end).map(|ordinal| EventPosition { transaction: self.tx_key.tx_id, ordinal });
        let batch = IndexBatch::from_owned(rtdb_format::data::append_event_positions(&batch, positions, &self.budget)?)?;
        if !batch.has_system_time(self.tx_key.system_time) {
            return Err(invalid("Transaction batch has another system time"));
        }
        let reservation = self.budget.reserve(table.schema.len() + table.table.len() + 4096)?;
        let pending = self.tables.get(table);
        let mut declarations = Vec::new();
        for (name, ty) in assembled_declarations.unwrap_or(&batch.put_types) {
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
        pending.batches.push(PendingBatch {
            preceding_events: pending.events.len(), batch,
            // Rows from differing schemas require event assembly when combined with another write.
            can_concat: assembled_declarations.is_none(),
        });
        pending.reservations.push(reservation);
        self.next_event_ordinal = end;
        Ok(())
    }

    /// The snapshot the transaction started from, without its own writes, for a statement that reads no table.
    pub fn base_snapshot(&self) -> Snapshot { self.base.clone() }
    #[doc(hidden)]
    pub fn adopt_owned_history(&mut self, table: TableRef, batch: Arc<IndexBatch>) -> Result<()> {
        if self.base.contains_table(&table) || self.tables.contains_key(&table) {
            return Err(invalid("An adopted history requires a virgin destination"));
        }
        let pending = PendingTable { adopted_history: Some(batch), ..PendingTable::default() };
        self.tables.insert(table, pending);
        Ok(())
    }
    pub fn has_pending_table(&self, table: &TableRef) -> bool { self.tables.contains_key(table) }

    pub fn pending_tables(&self) -> impl Iterator<Item = &TableRef> { self.tables.keys() }
    pub fn snapshot(&self) -> Result<Snapshot> {
        let mut segments = self.segments.lock().map_err(|_| invalid("Transaction segment cache was poisoned"))?;
        for (table, pending) in &self.tables {
            if let Some(batch) = &pending.adopted_history {
                let segment = Arc::new(LiveTable::new(table.clone(), self.base.block_index(), TrieConfig::default())
                    .append(&TableWrite::new(StructFields::new(), Some(Arc::clone(batch))).as_adopted_history(), self.tx_key.system_time)?);
                segments.insert(table.clone(), (segment, WritePosition::default(), StructFields::new()));
                continue;
            }
            let (segment, done, declared) = match segments.get(table) {
                Some((segment, done, declared)) => (Arc::clone(segment), *done, Some(declared)),
                None => (Arc::new(LiveTable::new(table.clone(), self.base.block_index(), TrieConfig::default())), WritePosition::default(), None),
            };
            let position = pending.position();
            if done == position && declared == Some(&pending.declared) { continue; }
            let segment = Arc::new(pending.append_since(done, &segment, self.tx_key.system_time, &self.budget)?);
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
            if let Some(batch) = &pending.adopted_history {
                if !pending.events.is_empty() || !pending.batches.is_empty() || !pending.sql_declarations.is_empty() {
                    return Err(invalid("An adopted history cannot share its table with fresh writes"));
                }
                return Ok((table.clone(), TableWrite::new(StructFields::new(), Some(Arc::clone(batch))).as_adopted_history()));
            }
            let batch = Some(pending.copy_since(WritePosition::default(), &self.budget)?);
            Ok((table.clone(), TableWrite::new(pending.declared.clone(), batch)
                .with_declarations(pending.sql_declarations.clone())))
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
    for (keyed, recency) in [(false, false), (true, false), (true, true)] {
        let table = if keyed { rtdb_types::OwnedRelation::new("docs").unwrap().table("owner") }
            else { TableRef::new("public", "docs") };
        let rows = |start, time, text| (start..start + 2).map(|id| {
            let id = EntityId::Integer(id);
            StoredEvent {
                position: None,
                origin: Some(rtdb_types::EventOrigin([time as u8; 32])),
                commit_position: if keyed { time as u32 } else { 0 },
                entity_key: keyed.then(|| id.handle_bytes().into()),
                iid: id.iid(),
                system_from: Timestamp(time),
                valid_time: Interval::ALL,
                operation: StoredOperation::Put(StructFields::from([
                    ("_id".into(), id.as_value()),
                    ("value".into(), if text { Value::Utf8(time.to_string()) } else { Value::I64(time) }),
                ])),
                recency: recency.then_some(Timestamp(time * 100)),
            }
        }).enumerate().map(|(offset, mut event)| {
            event.position = Some(EventPosition { transaction: rtdb_types::TxId(time), ordinal: offset as u64 });
            if keyed { event.commit_position += offset as u32; }
            event
        }).collect::<Vec<_>>();
        let first = rows(0, 1, false);
        for layout_changes in [false, true] {
            let second = rows(2, 2, layout_changes);
            let write = |events: &[StoredEvent]| TableWrite::new(StructFields::new(), Some(IndexBatch::copy_events(events, &StructFields::new(), &memory).unwrap()));
            let live = LiveTable::new(table.clone(), BlockIndex(0), TrieConfig::default())
                .append(&write(&first), Timestamp(1)).unwrap()
                .append(&write(&second), Timestamp(2)).unwrap();
            let pages = live.block_pages(&memory).unwrap();
            assert_eq!(pages.sources.is_some(), !layout_changes && !keyed && !recency);
            let actual = pages.copy(&[3, 0, 2]).unwrap();
            let selected = vec![second[1].clone(), first[0].clone(), second[0].clone()];
            let expected = IndexBatch::copy_events_with_put_schema(&selected, live.column_types(), &memory).unwrap();
            assert_eq!(&pages.schema, expected.arrow().schema_ref());
            assert_eq!(actual.batch(), expected.arrow());
            if layout_changes || keyed {
                assert!(pages.copy_relayout(&[3, 0, 2]).unwrap().is_some());
            }
            let bytes = |batch: &RecordBatch| rtdb_format::ipc::write_file(batch.schema_ref(), std::slice::from_ref(batch)).unwrap();
            assert_eq!(bytes(actual.batch()), bytes(expected.arrow()));
        }
    }
}

#[cfg(test)]
#[test]
fn empty_topology_block_declares_commit_positions_under_its_schema_reservation() {
    let memory = MemoryBudget::new(16 << 10);
    let table = rtdb_types::OwnedRelation::world_clock_table("owner");
    let live = LiveTable::new(table, BlockIndex(0), TrieConfig::default());
    let pages = live.block_pages(&memory).unwrap();
    let names = pages.schema.fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>();
    assert_eq!(names, ["_iid", "_system_from", "_valid_from", "_valid_to", "op", "_commit_position"]);
    let DataType::Union(legs, UnionMode::Dense) = pages.schema.field(4).data_type() else { panic!("Expected a dense operation union"); };
    assert_eq!(legs.iter().map(|(id, field)| (id, field.name().as_str())).collect::<Vec<_>>(),
        [(0, "delete"), (1, "erase")]);
    assert_eq!(pages.schema.field(5), &rtdb_format::data::commit_position_field());
    assert!(pages._schema_reservation.is_some());
    drop(pages);
    assert_eq!(memory.used_bytes(), 0);
}

#[cfg(test)]
#[test]
fn evolving_live_layouts_flush_with_column_bounded_scratch() {
    let source = MemoryBudget::new(128 << 20);
    let table = TableRef::new("public", "docs");
    let make_event = |id, value| {
        let mut document = StructFields::from([("_id".into(), Value::I64(id)), ("value".into(), value)]);
        for column in 0..6 { document.insert(format!("payload_{column}"), Value::Binary(vec![id as u8; 1 << 17])); }
        StoredEvent { position: None,
            origin: None,
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
        StoredEvent { position: None,
            origin: None,
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
    let events = (0..1024).map(|id| StoredEvent { position: None,
        origin: None,
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
    // The decoded headers stay charged with their batch; the Arrow columns and schema fit well inside 2.5 MiB without builder scratch.
    let headers = 32 * IndexBatch::header_bytes(1024).unwrap();
    assert!(memory.used_bytes() < headers + (5 << 19), "{} bytes retained", memory.used_bytes());
    let arrow = batches[0].arrow().clone();
    drop(batches);
    assert_ne!(memory.used_bytes(), 0);
    drop(arrow);
    assert_eq!(memory.used_bytes(), 0);
}

#[cfg(test)]
#[test]
fn staged_mixed_row_and_arrow_writes_keep_vector_layout_and_command_order() {
    use rtdb_types::ScalarType;

    let memory = MemoryBudget::new(16 << 20);
    let base = crate::PartitionState::empty(Default::default()).unwrap();
    let key = TransactionKey { tx_id: rtdb_types::TxId(0), system_time: Timestamp(10) };
    let mut transaction = TransactionBuffer::new(base.value().snapshot(), key, Arc::clone(&memory)).unwrap();
    let table = TableRef::new("public", "vectors");
    let event = |id, value| StoredEvent {
        position: None,
        iid: EntityId::Integer(id).iid(), system_from: key.system_time, valid_time: Interval::ALL,
        origin: None, commit_position: 0, entity_key: None, recency: None,
        operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(id)), ("v".into(), value)])),
    };
    let first = event(1, Value::list(vec![Value::F32(-0.0), Value::Null]));
    let middle = event(2, Value::FixedSizeList(vec![Value::F32(f32::from_bits(0x7fc0_0042)), Value::F64(-0.0), Value::Null].into()));
    let last = event(3, Value::Null);
    let StoredOperation::Put(document) = &middle.operation else { unreachable!() };
    let fields = StructFields::from([("_id".into(), LogicalType::Scalar(ScalarType::I64)), ("v".into(), document["v"].logical_type())]);
    let arrow = owned_event_batch(std::slice::from_ref(&middle), Some(fields), true, &memory).unwrap();

    transaction.append_to(&table, first.clone()).unwrap();
    transaction.append_batch(&table, arrow).unwrap();
    transaction.append_to(&table, last.clone()).unwrap();
    let staged = transaction.stage().unwrap();
    let batch = staged.tables[&table].batch.as_ref().unwrap();
    let mut reference = [first, middle, last];
    for (ordinal, event) in reference.iter_mut().enumerate() {
        event.position = Some(EventPosition { transaction: key.tx_id, ordinal: ordinal as u64 });
    }
    let expected = owned_event_batch(&reference, Some(batch.put_types().clone()), true, &memory).unwrap();
    let ipc = |batch: &RecordBatch| rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(batch)).unwrap();
    assert_eq!(ipc(batch.arrow()), ipc(expected.batch()));
}

#[cfg(test)]
#[test]
fn a_denied_append_keeps_the_transaction_schema_and_budget_unchanged() {
    let memory = MemoryBudget::new(1 << 20);
    let table = TableRef::new("public", "docs");
    let system_from = Timestamp(10);
    let event = StoredEvent { position: None,
        origin: None,
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
        let event = StoredEvent { position: None,
            origin: None,
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

#[cfg(test)]
#[test]
fn canonical_iid_verification_reads_unsigned_execution_bits_and_active_legs() {
    let memory = MemoryBudget::new(1 << 20);
    let system_from = Timestamp(1);
    let documents = [
        (EntityId::Integer(-1), Value::U16(u16::MAX)),
        (EntityId::String("second".into()), Value::Utf8("second".into())),
    ];
    let events = documents.iter().map(|(id, value)| StoredEvent {
        position: None,
        origin: None, commit_position: 0, entity_key: None, iid: id.iid(), system_from, valid_time: Interval::ALL,
        operation: StoredOperation::Put(StructFields::from([("_id".into(), value.clone())])), recency: None,
    }).collect::<Vec<_>>();
    let batch = IndexBatch::copy_events(&events, &StructFields::new(), &memory).unwrap();
    let after_batch = memory.used_bytes();
    assert_eq!(batch.has_canonical_iids(), Some(true));
    assert_eq!(memory.used_bytes(), after_batch);
    let mut wrong = events;
    wrong[1].iid = wrong[0].iid;
    let batch = IndexBatch::copy_events(&wrong, &StructFields::new(), &memory).unwrap();
    assert_eq!(batch.has_canonical_iids(), Some(false));
}

#[cfg(test)]
#[test]
fn foreign_id_aliases_cannot_attest_canonical_iids() {
    let memory = MemoryBudget::new(1 << 20);
    let ids = UnionArray::try_new(
        arrow_schema::UnionFields::try_new([0, 1], [
            Field::new("i64", DataType::Utf8, false),
            Field::new("utf8", DataType::Int64, false),
        ]).unwrap(),
        vec![0, 1].into(), Some(vec![0, 0].into()),
        vec![Arc::new(arrow_array::StringArray::from(vec!["id"])), Arc::new(arrow_array::Int64Array::from(vec![1]))],
    ).unwrap();
    let fields = vec![Field::new("_id", ids.data_type().clone(), false)].into();
    let docs = Arc::new(StructArray::try_new(fields, vec![Arc::new(ids)], None).unwrap());
    let headers = [EntityId::String("id".into()), EntityId::Integer(1)].map(|id| EventHeader {
        position: None,
        commit_position: 0, iid: id.iid(), system_from: Timestamp(1), valid_time: Interval::ALL, op: EventOp::Put,
    });
    let batch = rtdb_format::data::owned_put_batch(docs, &headers, None, &memory).unwrap();
    let batch = IndexBatch::from_owned(batch).unwrap();
    assert_eq!(batch.has_canonical_iids(), None);
}

/// `row_column_types` of the events' put documents, without collecting each document's types when they all match the first's.
fn event_column_types(events: &[StoredEvent]) -> Option<StructFields<LogicalType>> {
    let mut documents = events.iter().filter_map(|event| match &event.operation {
        StoredOperation::Put(document) => Some(document),
        _ => None,
    });
    let column_types = |document: &StructFields<Value>| {
        document.iter().map(|(name, value)| (name.clone(), value.logical_type())).collect::<StructFields<_>>()
    };
    let first = column_types(documents.next()?);
    if documents.clone().all(|document| {
        document.len() == first.len()
            && document.iter().zip(&first).all(|((name, value), (first_name, ty))| name == first_name && value.logical_type() == *ty)
    }) {
        return Some(first);
    }
    let types: Vec<_> = std::iter::once(first).chain(documents.map(column_types)).collect();
    row_column_types(&types)
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
    fn event_schema_preserves_field_order_and_joins_rows_with_missing_or_retyped_cells() {
        let put = |document| StoredEvent {
            position: None, origin: None, entity_key: None, commit_position: 0,
            iid: Iid([1; 16]),
            system_from: Timestamp(10),
            valid_time: Interval::ALL,
            operation: StoredOperation::Put(document),
            recency: None,
        };
        let events = [
            put(StructFields::from([("_id".into(), Value::I64(1)), ("name".into(), Value::Utf8("a".into()))])),
            put(StructFields::from([("_id".into(), Value::I64(2)), ("name".into(), Value::Utf8("b".into()))])),
            put(StructFields::from([("name".into(), Value::Utf8("c".into())), ("_id".into(), Value::I64(3))])),
            put(StructFields::from([("_id".into(), Value::I64(4)), ("name".into(), Value::I64(5))])),
            put(StructFields::from([("_id".into(), Value::I64(5)), ("age".into(), Value::I64(6))])),
        ];
        let document_types: Vec<_> = events.iter().map(|event| {
            let StoredOperation::Put(document) = &event.operation else { panic!("expected a put"); };
            document.iter().map(|(name, value)| (name.clone(), value.logical_type())).collect()
        }).collect();
        for size in 0..=events.len() {
            assert_eq!(event_column_types(&events[..size]), row_column_types(&document_types[..size]));
        }
        for size in 1..=events.len() {
            let fields = event_column_types(&events[..size]).unwrap();
            let expected = if size == events.len() { vec!["_id", "name", "age"] } else { vec!["_id", "name"] };
            assert_eq!(fields.keys().map(String::as_str).collect::<Vec<_>>(), expected);
        }
    }

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
    fn direct_replay_distinguishes_original_effects_from_regenerated_writes() {
        let state = crate::PartitionState::empty(Default::default()).unwrap();
        let key = TransactionKey { tx_id: rtdb_types::TxId(0), system_time: Timestamp(1) };
        let mut transaction = TransactionBuffer::new(state.snapshot(), key, MemoryBudget::new(1 << 24)).unwrap();
        transaction.retain_archived_owned_positions(vec![1, 3]).unwrap();
        let facts = rtdb_types::OwnedRelation::new("facts").unwrap().table("owner");
        let support = rtdb_types::OwnedRelation::new("support").unwrap().table("owner");
        let id = EntityId::String("one".into());
        let event = || StoredEvent {
            position: None, origin: None, commit_position: 0,
            entity_key: Some(id.handle_bytes().into()), iid: id.iid(),
            system_from: key.system_time, valid_time: Interval::ALL,
            operation: StoredOperation::Delete, recency: None,
        };

        assert!(transaction.append_to(&facts, event()).is_err());
        assert!(transaction.begin_direct_archive_effect(facts.clone(), 2).is_err());
        transaction.begin_direct_archive_effect(support.clone(), 0).unwrap();
        assert!(transaction.append_to(&facts, event()).is_err());
        assert_eq!(transaction.append_to(&support, event()).unwrap(), None);
        assert!(transaction.append_to(&support, event()).is_err());
        transaction.finish_direct_archive_effect().unwrap();

        transaction.begin_direct_archive_effect(facts.clone(), 1).unwrap();
        assert!(transaction.finish_direct_archive_effect().is_err());
        transaction.begin_direct_archive_effect(facts.clone(), 1).unwrap();
        assert_eq!(transaction.append_to(&facts, event()).unwrap(), NonZeroU32::new(1));
        transaction.finish_direct_archive_effect().unwrap();
        assert!(transaction.begin_direct_archive_effect(facts.clone(), 1).is_err());
        assert!(transaction.require_archived_positions_consumed().is_err());

        transaction.begin_direct_archive_effect(facts.clone(), 3).unwrap();
        assert_eq!(transaction.append_to(&facts, event()).unwrap(), NonZeroU32::new(3));
        transaction.finish_direct_archive_effect().unwrap();
        let staged = transaction.stage().unwrap();
        let ordinal = |table: &TableRef, row| {
            staged.tables()[table].batch().unwrap().header(row).unwrap().commit_position
        };
        assert_eq!([ordinal(&facts, 0), ordinal(&facts, 1), ordinal(&support, 0)], [1, 3, 0]);
    }

    #[test]
    fn archive_world_effects_claim_their_own_positions_without_advancing_root_effects() {
        let memory = MemoryBudget::new(1 << 24);
        let state = crate::PartitionState::empty(Default::default()).unwrap();
        let key = TransactionKey { tx_id: rtdb_types::TxId(0), system_time: Timestamp(1) };
        let mut transaction = TransactionBuffer::new(state.snapshot(), key, memory).unwrap();
        transaction.retain_archived_owned_positions(vec![3, 4, 6, 7]).unwrap();
        let table = rtdb_types::OwnedRelation::new("locations").unwrap().table("owner");
        let id = EntityId::String("place".into());
        let row = RecordBatch::try_from_iter([
            ("_id", Arc::new(arrow_array::StringArray::from(vec!["place"])) as arrow_array::ArrayRef),
            ("location", Arc::new(arrow_array::BinaryArray::from_iter_values([b"location"]))),
        ]).unwrap();
        let header = EventHeader { position: None, commit_position: 0, iid: id.iid(), system_from: key.system_time, valid_time: Interval::ALL, op: EventOp::Put };
        let overlay = rtdb_types::OwnedRelation::new("locations").unwrap()
            .in_world(Iid([1; 16])).unwrap().table("owner");
        let shadow = || StoredEvent { position: None,
            origin: None,
            commit_position: 0, entity_key: Some(id.handle_bytes().into()), iid: id.iid(),
            system_from: key.system_time, valid_time: Interval::ALL,
            operation: StoredOperation::Put(StructFields::from([("_id".into(), id.as_value())])),
            recency: None,
        };
        let copied = NonZeroU32::new(6).unwrap();
        transaction.append_archived_world_effect(&overlay, shadow(), copied).unwrap();
        assert!(transaction.append_archived_world_effect(&overlay, shadow(), copied).is_err());
        let omitted = NonZeroU32::new(4).unwrap();
        transaction.omit_archived_world_effect(omitted).unwrap();
        assert!(transaction.omit_archived_world_effect(omitted).is_err());
        transaction.append_unpositioned_replay(&overlay, shadow()).unwrap();
        transaction.begin_direct_archive_effect(table.clone(), 3).unwrap();
        let put = transaction.append_owned_put(
            &table, Arc::new(StructArray::from(row)), header, &id.handle_bytes(), None,
        ).unwrap();
        transaction.finish_direct_archive_effect().unwrap();
        transaction.begin_direct_archive_effect(table.clone(), 7).unwrap();
        let delete = transaction.append_to(&table, StoredEvent { position: None,
            origin: None,
            commit_position: 0, entity_key: Some(id.handle_bytes().into()), iid: id.iid(),
            system_from: key.system_time, valid_time: Interval::ALL,
            operation: StoredOperation::Delete, recency: None,
        }).unwrap();
        transaction.finish_direct_archive_effect().unwrap();
        assert_eq!(put, NonZeroU32::new(3));
        assert_eq!(delete, NonZeroU32::new(7));
        let staged = transaction.stage().unwrap();
        let rebuilt = staged.tables()[&overlay].batch().unwrap();
        let mut positions = (0..rebuilt.len())
            .map(|row| rebuilt.view().read_event(row).unwrap().header().commit_position)
            .collect::<Vec<_>>();
        positions.sort_unstable();
        assert_eq!(positions, [0, 6]);
        let batch = staged.tables()[&table].batch().unwrap();
        let first = batch.view().read_event(0).unwrap();
        let second = batch.view().read_event(1).unwrap();
        assert_eq!((first.header().commit_position, first.header().op), (3, EventOp::Put));
        assert_eq!((second.header().commit_position, second.header().op), (7, EventOp::Delete));
    }

    #[test]
    fn incremental_snapshot_coalesces_funded_runs_within_one_copy_budget() {
        let memory = MemoryBudget::new(32 << 20);
        let state = crate::PartitionState::empty(Default::default()).unwrap();
        let table = TableRef::new("public", "docs");
        let key = TransactionKey { tx_id: rtdb_types::TxId(0), system_time: Timestamp(1) };
        let mut transaction = TransactionBuffer::new(state.snapshot(), key, Arc::clone(&memory)).unwrap();
        let event = |id: u8| StoredEvent {
            position: None,
            origin: None,
            commit_position: 0,
            entity_key: None,
            iid: EntityId::Integer(i64::from(id)).iid(),
            system_from: key.system_time,
            valid_time: Interval::ALL,
            operation: StoredOperation::Put(StructFields::from([
                ("_id".into(), Value::I64(i64::from(id))),
                ("payload".into(), Value::Binary(vec![id; if id < 3 { 1 } else { 1 << 18 }])),
            ])),
            recency: None,
        };
        let append = |transaction: &mut TransactionBuffer, ids: std::ops::Range<u8>| {
            let events = ids.map(&event).collect::<Vec<_>>();
            let batch = owned_event_batch(&events, Some(StructFields::from([
                ("_id".into(), Value::I64(0).logical_type()),
                ("payload".into(), Value::Binary(Vec::new()).logical_type()),
            ])), true, &memory).unwrap();
            transaction.append_batch(&table, batch).unwrap();
        };

        append(&mut transaction, 0..2);
        drop(transaction.snapshot().unwrap());
        append(&mut transaction, 2..3);
        let previous = transaction.snapshot().unwrap().segments(&table).remove(0);
        assert_eq!(previous.batches().map(|batch| batch.len()).collect::<Vec<_>>(), [2, 1]);
        let proof = previous.row_lineage().proof();
        append(&mut transaction, 3..4);
        append(&mut transaction, 4..5);

        let denied = memory.reserve(memory.available_bytes()).unwrap();
        assert_eq!(transaction.snapshot().err().unwrap().code, "rtdb/memory-budget-exceeded");
        drop(denied);

        let pending_bytes = transaction.tables[&table].batches[2..].iter()
            .map(|run| run.batch.arrow().get_array_memory_size()).sum::<usize>();
        let tail_bytes = previous.batches().map(|batch| batch.arrow().get_array_memory_size()).sum::<usize>();
        assert!(2 * tail_bytes < pending_bytes);
        let headroom = 4096 + 3 * pending_bytes;
        let hold = memory.reserve(memory.available_bytes() - headroom).unwrap();
        let current = transaction.snapshot().unwrap().segments(&table).remove(0);
        drop(hold);

        assert_eq!(current.batches().count(), 1);
        assert_eq!(current.row_count(), 5);
        assert!(proof.is_prefix_of(current.row_lineage()));
        assert_eq!(previous.row_count(), 3);
        assert_eq!(previous.locate(2).unwrap(), (1, 0));
        for id in 0..5u8 {
            let mut expected = event(id);
            expected.position = Some(EventPosition { transaction: key.tx_id, ordinal: u64::from(id) });
            let row = u32::from(id);
            assert_eq!(current.locate(row).unwrap(), (0, row));
            assert_eq!(current.header(row).unwrap(), expected.header());
            assert_eq!(current.row(row).unwrap().event().unwrap(), expected);
            if row < previous.row_count() {
                assert_eq!(previous.header(row).unwrap(), current.header(row).unwrap());
            }
        }
        assert!(!state.open_snapshot(None).contains_table(&table));
        let staged = transaction.stage().unwrap();
        assert_eq!(staged.tables()[&table].batch().unwrap().arrow(), current.batches().next().unwrap().arrow());
        state.apply(&staged).unwrap();
        assert_eq!(state.open_snapshot(Some(key.tx_id)).segments(&table)[0].row_count(), 5);

        let row = current.row(4).unwrap();
        drop((state, staged, current, previous));
        assert!(memory.used_bytes() > 0);
        assert_eq!(row.event().unwrap().operation, event(4).operation);
        drop(row);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn a_table_written_a_row_per_transaction_is_held_in_merged_chunks_with_its_rows_in_place() {
        let memory = MemoryBudget::new(1 << 26);
        let table = TableRef::new("public", "docs");
        let mut live = LiveTable::new(table, BlockIndex(0), TrieConfig::default());
        for id in 0..10_000i64 {
            let event = StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid: EntityId::Integer(id).iid(), system_from: Timestamp(id), valid_time: Interval::ALL,
                operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(id)), ("v".into(), Value::F64(id as f64))])), recency: None };
            live = live.append(&TableWrite::new(StructFields::new(), Some(IndexBatch::copy_events(&[event], &StructFields::new(), &memory).unwrap())), Timestamp(id)).unwrap();
        }
        assert!(live.chunks.len() <= 8, "{} chunks", live.chunks.len());
        for id in [0u32, 1, 1023, 1024, 8191, 8192, 9999] {
            assert_eq!(live.row(id).unwrap().header().unwrap().system_from, Timestamp(i64::from(id)));
        }
    }

    fn put(id: i64, memory: &Arc<MemoryBudget>) -> TableWrite {
        let event = StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid: EntityId::Integer(id).iid(), system_from: Timestamp(id), valid_time: Interval::ALL,
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
                StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid: EntityId::Integer(n).iid(), system_from: Timestamp(tx), valid_time: Interval::ALL,
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
            let event = StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid: EntityId::String(key.clone()).iid(), system_from: Timestamp(id), valid_time: Interval::ALL,
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
