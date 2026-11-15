use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, estimate_builder_bytes, estimate_builder_bytes_refs, from_field};
use rtdb_format::data::{EventBatch, StoredEvent, StoredOperation, build_event_batch};
use rtdb_trie::{EventRow, IidReader, MemoryHashTrie, TrieConfig};
use rtdb_types::{Anomaly, BlockIndex, EntityId, EventHeader, Iid, Interval, LogicalType, Result, TableRef, Timestamp, TransactionKey, Value};

use crate::{Snapshot, TableMeta, merge_column_types};

#[derive(Debug)]
pub struct IndexBatch { owned: OwnedBatch, events: EventBatch, put_types: BTreeMap<String, LogicalType> }

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
        for row in 0..events.len() { events.header(row)?; }
        Ok(Arc::new(Self { owned, events, put_types }))
    }
    pub fn from_events(events: &[StoredEvent], declared: &BTreeMap<String, LogicalType>, budget: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        Self::build_events(events, declared, false, budget)
    }
    pub fn from_events_with_put_schema(events: &[StoredEvent], declared: &BTreeMap<String, LogicalType>, budget: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        Self::build_events(events, declared, true, budget)
    }
    fn build_events(events: &[StoredEvent], declared: &BTreeMap<String, LogicalType>, force_put: bool, budget: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let mut fields = declared.clone();
        let mut has_put = force_put || !declared.is_empty();
        for event in events {
            if let StoredOperation::Put(document) = &event.operation {
                let next = document.iter().map(|(name, value)| (name.clone(), value.logical_type())).collect();
                fields = merge_column_types(&fields, &next);
                has_put = true;
            }
        }
        let mut bytes = events.len().checked_mul(256).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| invalid("Event allocation size overflow"))?;
        if has_put {
            bytes = bytes.checked_add(estimate_builder_bytes(&LogicalType::Struct(fields.clone()), &[])?).ok_or_else(|| invalid("Put schema allocation size overflow"))?;
            for (name, ty) in &fields {
                let values = events.iter().filter_map(|event| match &event.operation {
                    StoredOperation::Put(document) => Some(document.get(name).unwrap_or(&Value::Undefined)), _ => None,
                });
                bytes = bytes.checked_add(estimate_builder_bytes_refs(ty, values)?).ok_or_else(|| invalid("Put column allocation size overflow"))?;
            }
            for event in events {
                if let StoredOperation::Put(document) = &event.operation {
                    bytes = bytes.checked_add(Value::document_retained_bytes(document)?.checked_mul(4).ok_or_else(|| invalid("Document clone size overflow"))?)
                        .ok_or_else(|| invalid("Document allocation size overflow"))?;
                }
            }
        }
        let reservation = budget.reserve(bytes)?;
        let batch = build_event_batch(events, has_put.then_some(fields), None)?;
        Self::from_owned(OwnedBatch::with_reservation(batch, reservation)?)
    }
    pub fn view(&self) -> &EventBatch { &self.events }
    pub fn arrow(&self) -> &RecordBatch { self.owned.batch() }
    pub fn len(&self) -> usize { self.events.len() }
    pub fn is_empty(&self) -> bool { self.events.is_empty() }
    pub fn put_types(&self) -> &BTreeMap<String, LogicalType> { &self.put_types }
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
    column_types: BTreeMap<String, LogicalType>,
    trie: MemoryHashTrie,
    last_system_time: Option<Timestamp>,
}

impl LiveTable {
    pub fn new(table: TableRef, block_index: BlockIndex, config: TrieConfig) -> Self {
        Self { table, block_index, chunks: Vec::new(), row_count: 0, column_types: BTreeMap::new(), trie: MemoryHashTrie::new(config), last_system_time: None }
    }
    pub fn table(&self) -> &TableRef { &self.table }
    pub fn block_index(&self) -> BlockIndex { self.block_index }
    pub fn row_count(&self) -> u32 { self.row_count }
    pub fn column_types(&self) -> &BTreeMap<String, LogicalType> { &self.column_types }
    pub fn trie(&self) -> &MemoryHashTrie { &self.trie }
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
            for row in 0..batch.len() {
                if batch.view().header(row)?.system_from != system_time { return Err(invalid("Imported event system time differs from its transaction")); }
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

impl IidReader for LiveTable { fn iid(&self, row: u32) -> Result<Iid> { Ok(self.row(row)?.header()?.iid) } }

#[derive(Clone, Debug)]
pub struct TableWrite { column_types: BTreeMap<String, LogicalType>, batch: Option<Arc<IndexBatch>> }
impl TableWrite {
    pub fn new(column_types: BTreeMap<String, LogicalType>, batch: Option<Arc<IndexBatch>>) -> Self {
        let column_types = match &batch { Some(batch) => merge_column_types(&column_types, batch.put_types()), None => column_types };
        Self { column_types, batch }
    }
    pub fn column_types(&self) -> &BTreeMap<String, LogicalType> { &self.column_types }
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
            if let Some(batch) = write.batch() {
                for row in 0..batch.len() {
                    if batch.view().header(row)?.system_from != tx_key.system_time { return Err(invalid("Staged event has another transaction's system time")); }
                }
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
struct PendingTable { declared: BTreeMap<String, LogicalType>, events: Vec<StoredEvent>, reservations: Vec<Arc<MemoryReservation>> }

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
    pub fn put(&mut self, table: TableRef, document: BTreeMap<String, Value>, valid_time: Interval) -> Result<()> {
        let id = document.get("_id").ok_or_else(|| invalid("Put document has no _id"))?.entity_id()?;
        self.append(table, StoredEvent { iid: id.iid(), system_from: self.tx_key.system_time, valid_time, operation: StoredOperation::Put(document), recency: None })
    }
    pub fn delete(&mut self, table: TableRef, id: &EntityId, valid_time: Interval) -> Result<()> {
        self.append(table, StoredEvent { iid: id.iid(), system_from: self.tx_key.system_time, valid_time, operation: StoredOperation::Delete, recency: None })
    }
    pub fn erase(&mut self, table: TableRef, id: &EntityId) -> Result<()> {
        self.append(table, StoredEvent { iid: id.iid(), system_from: self.tx_key.system_time, valid_time: Interval::ALL, operation: StoredOperation::Erase, recency: None })
    }
    pub fn append(&mut self, table: TableRef, event: StoredEvent) -> Result<()> {
        if event.system_from != self.tx_key.system_time { return Err(invalid("Transaction event has another system time")); }
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
            let batch = Some(IndexBatch::from_events(&pending.events, &pending.declared, &self.budget)?);
            Ok((table.clone(), TableWrite::new(pending.declared.clone(), batch)))
        }).collect::<Result<_>>()?;
        StagedWrite::new(self.tx_key, self.base.tx_basis(), self.base.block_index(), tables)
    }
}

fn invalid(message: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.index/invalid-live-index", message.to_string()) }
