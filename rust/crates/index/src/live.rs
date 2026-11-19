use rtdb_types::{OrderedSet, StructFields};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use arrow_array::{Array, RecordBatch, StructArray, UnionArray};
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, estimate_read_value_bytes, from_field, read_execution_value};
use rtdb_format::data::{EventBatch, StoredEvent, StoredOperation, owned_event_batch};
use rtdb_trie::{EventRow, IidReader, MemoryHashTrie, TrieConfig};
use rtdb_types::{Anomaly, BlockIndex, EntityId, EventHeader, EventOp, Iid, Interval, LogicalType, Result, TableRef, Timestamp, TransactionKey, Value};

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
}

#[derive(Clone, Copy)]
enum EventBuild { Write, CopyInferred, CopyCaptured }

impl IndexBatch {
    pub fn new(batch: RecordBatch, budget: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        Self::from_owned(OwnedBatch::new(batch, budget)?)
    }
    pub fn from_owned(owned: OwnedBatch) -> Result<Arc<Self>> {
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
        for row in 0..events.len() {
            let time = events.header(row)?.system_from;
            system_times = Some(system_times.map_or((time, time), |(first, last)| (first.min(time), last.max(time))));
        }
        Ok(Arc::new(Self { owned, events, put_types, put_field_types, system_times, canonical_iids: OnceLock::new() }))
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
    pub fn arrow(&self) -> &RecordBatch { self.owned.batch() }
    pub fn len(&self) -> usize { self.events.len() }
    pub fn is_empty(&self) -> bool { self.events.is_empty() }
    pub fn put_types(&self) -> &StructFields<LogicalType> { &self.put_types }
    /// The logical type of the put struct's `index`th child field, as `from_field` derives it.
    pub fn put_field_type(&self, index: usize) -> Option<&LogicalType> { self.put_field_types.get(index) }
}

#[derive(Clone, Debug)]
pub struct RowSource { pub batch: Arc<IndexBatch>, pub row: usize }
impl RowSource {
    pub fn header(&self) -> Result<EventHeader> { self.batch.view().header(self.row) }
    pub fn event(&self) -> Result<StoredEvent> { self.batch.view().read_event(self.row) }
}

#[derive(Clone, Debug)]
pub struct LiveTable {
    table: TableRef,
    block_index: BlockIndex,
    chunks: Vec<(u32, Arc<IndexBatch>)>,
    row_count: u32,
    column_types: StructFields<LogicalType>,
    trie: MemoryHashTrie,
    last_system_time: Option<Timestamp>,
    canonical_entity_iids: bool,
}

impl LiveTable {
    pub fn new(table: TableRef, block_index: BlockIndex, config: TrieConfig) -> Self {
        Self { table, block_index, chunks: Vec::new(), row_count: 0, column_types: StructFields::new(), trie: MemoryHashTrie::new(config), last_system_time: None, canonical_entity_iids: true }
    }
    pub fn table(&self) -> &TableRef { &self.table }
    pub fn block_index(&self) -> BlockIndex { self.block_index }
    pub fn row_count(&self) -> u32 { self.row_count }
    pub fn column_types(&self) -> &StructFields<LogicalType> { &self.column_types }
    pub fn trie(&self) -> &MemoryHashTrie { &self.trie }
    pub(crate) fn has_canonical_entity_iids(&self) -> bool { self.canonical_entity_iids }
    pub fn batches(&self) -> impl Iterator<Item = &Arc<IndexBatch>> { self.chunks.iter().map(|(_, batch)| batch) }
    pub fn row(&self, row: u32) -> Result<RowSource> {
        if row >= self.row_count { return Err(invalid("Live table row exceeds its retained batches")); }
        let index = self.chunks.partition_point(|(start, _)| *start <= row).checked_sub(1).ok_or_else(|| invalid("Live table row has no owning batch"))?;
        let (start, batch) = &self.chunks[index];
        Ok(RowSource { batch: Arc::clone(batch), row: (row - start) as usize })
    }
    pub fn append(&self, write: &TableWrite, system_time: Timestamp) -> Result<Self> {
        if self.last_system_time.is_some_and(|previous| previous > system_time) { return Err(invalid("Live table system time moved backwards")); }
        let mut next = self.clone();
        next.column_types = merge_column_types(&next.column_types, &write.column_types);
        if let Some(batch) = &write.batch {
            if !batch.has_system_time(system_time) { return Err(invalid("Imported event system time differs from its transaction")); }
            if next.canonical_entity_iids {
                next.canonical_entity_iids = batch.has_canonical_iids().unwrap_or(false);
            }
            let count = u32::try_from(batch.len()).map_err(invalid)?;
            let start = next.row_count;
            next.row_count = start.checked_add(count).ok_or_else(|| invalid("Live table exceeds u32 row address space"))?;
            next.chunks.push((start, Arc::clone(batch)));
            next.trie = self.trie.add_range(&next, start, count)?;
        }
        next.last_system_time = Some(system_time);
        Ok(next)
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
    fn iid(&self, row: u32) -> Result<Iid> {
        let source = self.row(row)?;
        source.batch.view().iid(source.row)
    }
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
                    let index = self.table.chunks.partition_point(|(start, _)| *start <= row).checked_sub(1).ok_or_else(|| invalid("Live table row has no owning batch"))?;
                    let start = self.table.chunks[index].0;
                    Ok((index, (row - start) as usize))
                }).collect::<Result<Vec<_>>>()?;
                OwnedBatch::interleave_distinct_rows(sources, &positions, &self.memory)
            });
            match result {
                Ok(batch) => return Ok(batch),
                Err(error) if error.code == "rtdb/memory-budget-exceeded" => (),
                Err(error) => return Err(error),
            }
        }
        let mut bound = rows.len().checked_mul(256).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("Page row allocation overflow"))?;
        let mut schema_charged = Vec::new();
        for &row in rows {
            let source = self.table.row(row)?;
            let schema = source.batch.arrow().schema();
            let index = schema.index_of("op").map_err(invalid)?;
            let (field, column) = (schema.field(index), source.batch.arrow().column(index).as_ref());
            // Each batch derives its put type once, so only its first row is charged the schema.
            let first = !schema_charged.iter().any(|batch| Arc::ptr_eq(batch, &source.batch));
            let cell = if first { schema_charged.push(Arc::clone(&source.batch)); estimate_read_value_bytes(field, column, source.row)? }
                else { rtdb_columnar::estimate_read_cell_bytes(field, column, source.row)? };
            bound = bound.checked_add(cell).ok_or_else(|| invalid("Page allocation size overflow"))?;
        }
        let _scratch = self.memory.reserve(bound)?;
        let events = rows.iter().map(|&row| self.table.row(row)?.event()).collect::<Result<Vec<_>>>()?;
        Ok(IndexBatch::copy_events_with_put_schema(&events, &self.table.column_types, &self.memory)?.owned.clone())
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
    config: TrieConfig,
}
impl LiveIndex {
    pub fn new(block_index: BlockIndex, latest_completed_tx: Option<TransactionKey>, config: TrieConfig) -> Self {
        Self { block_index, latest_completed_tx, block_row_count: 0, tables: BTreeMap::new(), config }
    }
    pub fn block_index(&self) -> BlockIndex { self.block_index }
    pub fn latest_completed_tx(&self) -> Option<TransactionKey> { self.latest_completed_tx }
    pub fn block_row_count(&self) -> u64 { self.block_row_count }
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
struct PendingTable { declared: StructFields<LogicalType>, events: Vec<StoredEvent>, reservations: Vec<Arc<MemoryReservation>> }

pub struct TransactionBuffer {
    base: Snapshot,
    tx_key: TransactionKey,
    budget: Arc<MemoryBudget>,
    tables: BTreeMap<TableRef, PendingTable>,
}

impl TransactionBuffer {
    pub fn new(base: Snapshot, tx_key: TransactionKey, budget: Arc<MemoryBudget>) -> Result<Self> {
        StagedWrite::new(tx_key, base.tx_basis(), base.block_index(), BTreeMap::new())?;
        Ok(Self { base, tx_key, budget, tables: BTreeMap::new() })
    }
    pub fn tx_key(&self) -> TransactionKey { self.tx_key }
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
            let bytes = ty.retained_bytes()?.checked_add(existing.map_or(Ok(0), LogicalType::retained_bytes)?)
                .and_then(|bytes| bytes.checked_add(name.len())).and_then(|bytes| bytes.checked_add(512))
                .and_then(|bytes| bytes.checked_mul(8)).ok_or_else(|| invalid("Declared schema size overflow"))?;
            let reservation = self.budget.reserve(bytes)?;
            let merged = existing.map_or_else(|| ty.clone(), |existing| existing.join(ty));
            pending.declared.insert(name.clone(), merged);
            pending.reservations.push(reservation);
        }
        Ok(())
    }
    pub fn put(&mut self, table: TableRef, document: StructFields<Value>, valid_time: Interval) -> Result<()> {
        let id = document.get("_id").ok_or_else(|| invalid("Put document has no _id"))?.entity_id()?;
        self.append(table, StoredEvent { iid: id.iid(), system_from: self.tx_key.system_time, valid_time, operation: StoredOperation::Put(document), recency: None })
    }
    pub fn delete(&mut self, table: TableRef, id: &EntityId, valid_time: Interval) -> Result<()> {
        self.append(table, StoredEvent { iid: id.iid(), system_from: self.tx_key.system_time, valid_time, operation: StoredOperation::Delete, recency: None })
    }
    pub fn erase(&mut self, table: TableRef, id: &EntityId) -> Result<()> {
        self.append(table, StoredEvent { iid: id.iid(), system_from: self.tx_key.system_time, valid_time: Interval::ALL, operation: StoredOperation::Erase, recency: None })
    }
    pub fn append(&mut self, table: TableRef, mut event: StoredEvent) -> Result<()> {
        if event.system_from != self.tx_key.system_time { return Err(invalid("Transaction event has another system time")); }
        if let StoredOperation::Put(document) = &mut event.operation { document.values_mut().for_each(Value::admit_object_dates); }
        let document_bytes = match &event.operation { StoredOperation::Put(document) => Value::document_retained_bytes(document)?, _ => 0 };
        let bytes = document_bytes.checked_add(4096).and_then(|bytes| bytes.checked_add(table.schema.capacity())).and_then(|bytes| bytes.checked_add(table.table.capacity()))
            .ok_or_else(|| invalid("Transaction event size overflow"))?;
        let reservation = self.budget.reserve(bytes)?;
        let pending = self.tables.entry(table).or_default();
        pending.events.push(event);
        pending.reservations.push(reservation);
        Ok(())
    }
    pub fn snapshot(&self) -> Result<Snapshot> { self.base.with_staged(&[Arc::new(self.materialize()?)]) }
    pub fn stage(self) -> Result<StagedWrite> { self.materialize() }
    fn materialize(&self) -> Result<StagedWrite> {
        let tables = self.tables.iter().map(|(table, pending)| {
            let batch = Some(IndexBatch::copy_events(&pending.events, &pending.declared, &self.budget)?);
            Ok((table.clone(), TableWrite::new(pending.declared.clone(), batch)))
        }).collect::<Result<_>>()?;
        StagedWrite::new(self.tx_key, self.base.tx_basis(), self.base.block_index(), tables)
    }
}

fn invalid(message: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.index/invalid-live-index", message.to_string()) }

#[cfg(test)]
#[test]
fn block_pages_preserve_order_and_layout_across_live_batches() {
    let memory = MemoryBudget::new(32 << 20);
    let table = TableRef::new("public", "docs");
    let rows = |start, time, text| (start..start + 2).map(|id| StoredEvent {
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
fn event_batches_build_wide_document_columns_with_bounded_scratch() {
    let memory = MemoryBudget::new(128 << 20);
    let events = (0..100).map(|id| {
        let mut doc = StructFields::from([("_id".into(), Value::I64(id))]);
        for key in 0..10 { doc.insert(format!("field_{id}_{key}"), Value::I64(id + key)); }
        StoredEvent {
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
fn denied_iid_verification_preserves_the_write_and_cannot_promote_its_generation() {
    let memory = MemoryBudget::new(1 << 20);
    let table = TableRef::new("public", "docs");
    let write = |id, system_from| {
        let event = StoredEvent {
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
