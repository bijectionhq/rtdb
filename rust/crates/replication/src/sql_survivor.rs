//! Builds a spill-free primary checkpoint from an authenticated SQL erasure cut.

mod selection;

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    io::{self, Cursor, Write},
    sync::Arc,
};

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, FixedSizeBinaryArray, Float16Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
    StructArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::Buffer;
use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{
    block::proto as pb,
    catalog::{BlockCatalog, TableEntry, publication_digest},
    data::{EventBatch, StoredEvent, StoredOperation, owned_event_batch},
    ipc::{ArrowFile, ArrowFileEncoder, inspect_file_reserved},
    keys::{TrieKey, block_key},
    metadata::MetadataBuilder,
};
use rtdb_index::{BlockPages, CatalogTrie, IndexBatch, LiveTable, Snapshot, TableWrite, TrieCatalog};
use rtdb_storage::{IoRegion, MemoryStore, ObjectKey, ObjectStore, ReadReservation, RetainedBytes};
use rtdb_trie::{TrieConfig, TrieNode};
use rtdb_types::{
    Anomaly, AnomalyCategory, AnomalyKind, BlockIndex, CatalogCut, EventHeader, EventOp, EventOrigin,
    EventPosition, Iid, LogicalType, Result, ScalarType, SourcePosition, StructFields, Value,
};
use tokio_util::sync::CancellationToken;

use crate::{ErasureTarget, SqlErasureSource, TermFence, TermPosition};
use selection::{keep_inherited_event, keep_local_event, write_survivor_trie};

pub struct SqlSurvivor {
    pub catalog: BlockCatalog,
    store: MemoryStore,
}

impl SqlSurvivor {
    pub fn store(&self) -> &dyn ObjectStore {
        &self.store
    }
}

impl SqlErasureSource {
    pub async fn build_survivor(
        &self,
        provider_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<SqlSurvivor> {
        SurvivorInput {
            snapshot: self.snapshot(), targets: self.targets(), cut: self.cut(), source_position: self.source_position(),
            inherited_erasure: self.inherited_erasure.as_ref(), custody: &self.custody, store: self.store.as_ref(),
            memory: &self.memory, fence: &self.fence, external_token: self.external_token.as_deref(),
        }.build(provider_bytes, cancellation).await
    }
}

pub(crate) struct SurvivorInput<'a> {
    pub snapshot: &'a Snapshot,
    pub targets: &'a [ErasureTarget],
    pub cut: CatalogCut,
    pub source_position: SourcePosition,
    pub inherited_erasure: Option<&'a BTreeSet<EventOrigin>>,
    pub custody: &'a Arc<dyn Send + Sync>,
    pub store: &'a dyn ObjectStore,
    pub memory: &'a Arc<MemoryBudget>,
    pub fence: &'a TermFence,
    pub external_token: Option<&'a [u8]>,
}

impl SurvivorInput<'_> {
    pub(crate) async fn build(self, provider_bytes: u64, cancellation: &CancellationToken) -> Result<SqlSurvivor> {
        let snapshot = self.snapshot;
        let tx = snapshot
            .tx_basis()
            .ok_or_else(|| invalid("The erasure head has no transaction basis"))?;
        if snapshot.table_catalog().block().is_some_and(|block| {
            !block.secondary_databases().is_empty() || block.external_source_token().is_some()
        }) || self.external_token.is_some()
        {
            return Err(invalid(
                "This SQL erasure needs a separate secondary or external source custody owner",
            ));
        }
        if self
            .targets
            .iter()
            .any(|target| target.entity_key.is_some())
        {
            return Err(invalid(
                "Governed relations require their original erasure authority",
            ));
        }
        let sequence = match self.fence.position() {
            TermPosition::Expecting(next) => Some(
                next.0
                    .checked_sub(1)
                    .ok_or_else(|| invalid("Invalid last term sequence"))?,
            ),
            TermPosition::Unsequenced | TermPosition::Unknown => None,
            TermPosition::Voided => {
                return Err(invalid("The incumbent term has no readable successor"));
            }
        };
        let storage = MemoryStore::default();
        let mut destination = Candidate {
            store: &storage,
            memory: self.memory,
            remaining: provider_bytes,
        };
        let tables = snapshot.tables();
        let registry = snapshot.table_catalog().with_entries_for(tables)?;
        let block = snapshot.block_index();
        let trie = TrieKey::l0(block)?;
        let naming_epoch = snapshot
            .trie_catalog()
            .naming_epoch()
            .checked_add(1)
            .and_then(|epoch| i64::try_from(epoch).ok())
            .ok_or_else(|| invalid("The trie naming epoch is exhausted"))?;
        let mut digests = BTreeMap::new();
        let mut seen_targets = 0;
        for entry in registry.entries() {
            check_cancelled(cancellation)?;
            let targets = self
                .targets
                .iter()
                .filter(|target| target.table == entry.table)
                .fold(
                    BTreeMap::new(),
                    |mut map: BTreeMap<Iid, EventPosition>, target| {
                        map.entry(target.iid)
                            .and_modify(|position| *position = (*position).max(target.before))
                            .or_insert(target.before);
                        map
                    },
                );
            let retired = snapshot.trie_catalog().retired_as_of(&entry.table);
            let declarations = snapshot.declared_columns(&entry.table);
            let mut used_positions = false;
            let (rebuilt, marked) = match self.rebuild_table(entry, &targets, &declarations, true, &mut used_positions, cancellation).await {
                Err(error) if used_positions && error.is(AnomalyKind::ResourceExhausted) => {
                    self.rebuild_table(entry, &targets, &declarations, false, &mut used_positions, cancellation).await?
                }
                result => result?,
            };
            seen_targets += marked;
            let catalog = match destination
                .write_trie(entry, &trie, &rebuilt, cancellation)
                .await?
            {
                Some(notification) => TrieCatalog::default().with_tries(
                    entry.table.clone(),
                    [notification],
                    tx.system_time,
                )?,
                None => TrieCatalog::default(),
            };
            let (metadata, _hll_reservation) = rebuilt.block_metadata()?;
            let table_block = metadata
                .to_block(catalog.partitions(&entry.table)?)?
                .with_retired_as_of(retired.map(|instant| instant.0));
            let bytes = table_block.encode();
            digests.insert(entry.oid, publication_digest(&bytes).to_vec());
            destination
                .write(ObjectKey::new(entry.slug.block_key(block)?)?, bytes)
                .await?;
        }
        let required = self
            .targets
            .iter()
            .map(|target| (&target.table, target.iid))
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        if seen_targets != required {
            return Err(invalid(
                "The surviving catalog does not account for every selected erase marker",
            ));
        }
        let mut catalog = BlockCatalog::from_proto(pb::Block {
            block_index: Some(block.0),
            latest_completed_tx: Some(pb::TxKey {
                tx_id: Some(tx.tx_id.0),
                system_time: Some(tx.system_time.0),
            }),
            latest_processed_msg_id: Some(self.source_position.0),
            table_names: registry
                .entries()
                .map(|entry| entry.table.schema_and_table())
                .collect(),
            tables: registry.entries().map(TableEntry::to_proto).collect(),
            boundary_replica_msg_id: Some(self.cut.through.0),
            term_id: Some(self.fence.highest_term().0),
            boundary_term_seq: sequence,
            naming_epoch: Some(naming_epoch),
            relation_oid_order: registry.relation_oid_order().wire_value(),
            table_block_manifest: Some(pb::TableBlockManifest {
                sha256_by_oid: digests,
            }),
            ..Default::default()
        })?;
        destination
            .write(
                ObjectKey::new(block_key(block)?)?,
                catalog.encode_for_publication(),
            )
            .await?;
        Ok(SqlSurvivor {
            catalog,
            store: storage,
        })
    }

    async fn rebuild_table(
        &self,
        entry: &TableEntry,
        targets: &BTreeMap<Iid, EventPosition>,
        declarations: &StructFields<LogicalType>,
        positions: bool,
        used_positions: &mut bool,
        cancellation: &CancellationToken,
    ) -> Result<(LiveTable, usize)> {
        let mut rows = Events {
            memory: self.memory,
            rows: BTreeMap::new(),
            targets,
            inherited_erasure: self.inherited_erasure,
            positions: positions && declarations.is_empty(),
            used_positions,
        };
        if self.snapshot.trie_catalog().retired_as_of(&entry.table).is_none() {
            for primary in self.snapshot.trie_entries(&entry.table) {
                check_cancelled(cancellation)?;
                let key = ObjectKey::new(entry.slug.data_key(&primary.key))?;
                let length = usize::try_from(primary.data_file_size)
                    .map_err(|_| invalid("A source trie exceeds addressable memory"))?;
                IoRegion::retaining(
                    Arc::clone(self.custody),
                    read_trie(self.store, &key, length, &mut rows, cancellation),
                ).await?;
            }
            for segment in self.snapshot.iter_segments(&entry.table) {
                for batch in segment.batches() {
                    check_cancelled(cancellation)?;
                    rows.add(batch.view(), None)?;
                }
            }
        }
        let marked = targets.iter().filter(|(iid, position)| {
            rows.rows.get(*position).is_some_and(|row| {
                row.header.iid == **iid && row.header.op == EventOp::Erase
            })
        }).count();
        let live = rows.into_live(entry, self.snapshot.block_index(), declarations, cancellation)?;
        Ok((live, marked))
    }
}

struct Events<'a> {
    memory: &'a Arc<MemoryBudget>,
    rows: BTreeMap<EventPosition, SurvivorRow>,
    targets: &'a BTreeMap<Iid, EventPosition>,
    inherited_erasure: Option<&'a BTreeSet<EventOrigin>>,
    positions: bool,
    used_positions: &'a mut bool,
}

struct SurvivorRow {
    header: EventHeader,
    payload: SurvivorPayload,
    reservation: Arc<MemoryReservation>,
}

enum SurvivorPayload {
    Position(Arc<SurvivorPage>, usize),
    Scalar(StoredEvent),
}

struct SurvivorPage {
    batch: EventBatch,
    _disk: Option<(Arc<MemoryReservation>, Arc<MemoryReservation>)>,
    _reservation: Arc<MemoryReservation>,
}

impl Events<'_> {
    fn add(
        &mut self, batch: &EventBatch, disk: Option<(&Arc<MemoryReservation>, &Arc<MemoryReservation>)>,
    ) -> Result<()> {
        let mut source = None;
        for row in 0..batch.len() {
            let header = batch.header(row)?;
            if let Some(origins) = self.inherited_erasure {
                let origin = batch.origin(row)?.ok_or_else(|| invalid("A dependent event has no authenticated origin"))?;
                if !keep_inherited_event(origins.contains(&origin)) {
                    continue;
                }
            }
            let position = header
                .position
                .ok_or_else(|| invalid("A surviving event has no original transaction position"))?;
            let before = self.targets.get(&header.iid)
                .map(|before| (before.transaction.0, before.ordinal));
            if !keep_local_event(position.transaction.0, position.ordinal, before) {
                continue;
            }
            match self.rows.entry(position) {
                Entry::Occupied(entry) => {
                    let (candidate, _reservation) = normalized_event(batch, row, self.memory)?;
                    let equal = match &entry.get().payload {
                        SurvivorPayload::Scalar(event) => *event == candidate,
                        SurvivorPayload::Position(page, row) => {
                            let (event, _reservation) = normalized_event(&page.batch, *row, self.memory)?;
                            event == candidate
                        }
                    };
                    if !equal {
                        return Err(invalid("Two primary rows claim the same original event"));
                    }
                }
                Entry::Vacant(entry) => {
                    let positioned = self.positions && !batch.has_entity_keys()
                        && (header.op != EventOp::Put || flat_put(batch, row)?.is_some());
                    if positioned && source.is_none() {
                        let bytes = batch.arrow().num_columns().checked_mul(32)
                            .and_then(|bytes| bytes.checked_add(1024))
                            .ok_or_else(|| invalid("A surviving page exceeds addressable memory"))?;
                        source = self.memory.try_reserve(bytes).map(|reservation| Arc::new(SurvivorPage {
                            batch: batch.clone(),
                            _disk: disk.map(|(opening, decoded)| (Arc::clone(opening), Arc::clone(decoded))),
                            _reservation: reservation,
                        }));
                    }
                    let (payload, reservation) = match source.as_ref().filter(|_| positioned) {
                        Some(page) => {
                            *self.used_positions = true;
                            let reservation = self.memory.reserve(1024)?;
                            (SurvivorPayload::Position(Arc::clone(page), row), reservation)
                        }
                        None => {
                            let (event, reservation) = normalized_event(batch, row, self.memory)?;
                            (SurvivorPayload::Scalar(event), reservation)
                        }
                    };
                    entry.insert(SurvivorRow { header, payload, reservation });
                }
            }
        }
        Ok(())
    }

    fn into_live(
        self,
        entry: &TableEntry,
        block: BlockIndex,
        declarations: &StructFields<LogicalType>,
        cancellation: &CancellationToken,
    ) -> Result<LiveTable> {
        let mut live = LiveTable::new(entry.table.clone(), block, TrieConfig::default());
        let empty = TableWrite::new(declarations.clone(), None).with_declarations(declarations.clone());
        live = live.append(&empty, rtdb_types::Timestamp(i64::MIN))?;
        let mut ordered = self.rows.into_iter().peekable();
        while let Some((position, first)) = ordered.next() {
            check_cancelled(cancellation)?;
            let time = first.header.system_from;
            let mut rows = vec![first];
            while ordered.peek().is_some_and(|(following, _)| following.transaction == position.transaction) {
                rows.push(ordered.next().expect("peeked row").1);
            }
            let batch = match copy_positions(rows.iter(), self.memory)? {
                Some(batch) => batch,
                None => {
                    let mut events = Vec::with_capacity(rows.len());
                    let mut reservations = Vec::with_capacity(rows.len());
                    for row in rows {
                        let (event, reservation) = match row.payload {
                            SurvivorPayload::Scalar(event) => (event, row.reservation),
                            SurvivorPayload::Position(page, row) => normalized_event(&page.batch, row, self.memory)?,
                        };
                        events.push(event);
                        reservations.push(reservation);
                    }
                    IndexBatch::copy_events(&events, declarations, self.memory)?
                }
            };
            live = live.append(&TableWrite::new(declarations.clone(), Some(batch)), time)?;
        }
        Ok(live)
    }
}

fn flat_type(array: &dyn Array) -> Option<ScalarType> {
    let physical = array.as_any();
    macro_rules! scalar_types {
        ($($array:ty => $scalar:ident),+ $(,)?) => {
            $(if physical.is::<$array>() { return Some(ScalarType::$scalar); })+
        };
    }
    scalar_types!(
        BooleanArray => Bool, StringArray => Utf8, BinaryArray => Binary,
        Int8Array => I8, Int16Array => I16, Int32Array => I32, Int64Array => I64,
        UInt8Array => U8, UInt16Array => U16, UInt32Array => U32, UInt64Array => U64,
        Float16Array => F16, Float32Array => F32, Float64Array => F64,
        Date32Array => DateDay, Date64Array => DateMilli,
    );
    physical.downcast_ref::<FixedSizeBinaryArray>()
        .filter(|array| array.value_length() > 0)
        .map(|array| ScalarType::FixedSizeBinary(array.value_length()))
}

fn flat_put(batch: &EventBatch, row: usize) -> Result<Option<&StructArray>> {
    batch.borrow_event(row)?;
    let put = batch.put_document()?.ok_or_else(|| invalid("A put operation must contain a document struct"))?;
    let offset = batch.operations()?.value_offset(row);
    Ok(put.fields().iter().zip(put.columns()).all(|(field, array)| {
        field.metadata().is_empty() && array.is_valid(offset) && flat_type(array.as_ref()).is_some()
    }).then_some(put))
}

fn copy_positions<'a>(
    rows: impl Iterator<Item = &'a SurvivorRow> + Clone,
    memory: &Arc<MemoryBudget>,
) -> Result<Option<Arc<IndexBatch>>> {
    let mut put = None;
    for row in rows.clone() {
        let SurvivorPayload::Position(page, index) = &row.payload else { return Ok(None); };
        if row.header.op == EventOp::Put {
            let document = flat_put(&page.batch, *index)?.expect("admitted flat put");
            if put.is_some_and(|first: &StructArray| first.fields() != document.fields()) {
                return Ok(None);
            }
            put = Some(document);
        }
    }
    let bound = put.map_or(Ok(4096usize), |put| {
        put.fields().iter().try_fold(4096usize, |bytes, field| {
            field.size().checked_mul(8).and_then(|size| bytes.checked_add(size))
                .ok_or_else(|| invalid("Survivor schema exceeds addressable memory"))
        })
    })?;
    let _schema = memory.reserve(bound)?;
    let fields = put.map(|put| put.fields().iter().zip(put.columns()).map(|(field, array)| {
        (field.name().clone(), LogicalType::Scalar(flat_type(array.as_ref()).expect("admitted flat column")))
    }).collect());
    let events = rows.map(|row| {
        let SurvivorPayload::Position(page, index) = &row.payload else { unreachable!("admitted position"); };
        Ok(page.batch.borrow_event(*index)?.without_recency())
    }).collect::<Result<Vec<_>>>()?;
    let batch = owned_event_batch(&events, fields, true, memory)?;
    IndexBatch::from_owned(batch).map(Some)
}

fn normalized_event(batch: &EventBatch, row: usize, memory: &Arc<MemoryBudget>) -> Result<(StoredEvent, Arc<MemoryReservation>)> {
    let borrowed = batch.borrow_event(row)?;
    let bound = borrowed.document_allocation_bound()?
        .checked_add(borrowed.physical_entity_key().map_or(0, |key| key.len()))
        .and_then(|bytes| bytes.checked_add(1024))
        .ok_or_else(|| invalid("A surviving row exceeds addressable memory"))?;
    let reservation = memory.reserve(bound)?;
    let mut event = batch.read_event(row)?;
    event.recency = None;
    if let StoredOperation::Put(document) = &mut event.operation {
        remove_padding(document);
    }
    Ok((event, reservation))
}

fn remove_padding(fields: &mut StructFields<Value>) {
    let empty: Vec<_> = fields
        .iter()
        .filter(|(_, value)| matches!(value, Value::Null | Value::Undefined))
        .map(|(name, _)| name.clone())
        .collect();
    for name in empty {
        fields.remove(&name);
    }
    fields.values_mut().for_each(remove_nested_padding);
}

fn remove_nested_padding(value: &mut Value) {
    match value {
        Value::Struct(fields) => remove_padding(fields),
        Value::List(values) | Value::FixedSizeList(values) => {
            if let Some(values) = values.values_mut() {
                values.iter_mut().for_each(remove_nested_padding);
            }
        }
        Value::Set(values) => values.iter_mut().for_each(remove_nested_padding),
        Value::Map { entries, .. } => {
            for (_, value) in entries {
                remove_nested_padding(value);
            }
        }
        _ => (),
    }
}

struct TrieInput(RetainedBytes);

impl AsRef<[u8]> for TrieInput {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

fn open_survivor_input(bytes: Bytes, memory: &Arc<MemoryBudget>) -> Result<(ArrowFile, Arc<MemoryReservation>)> {
    let layout = inspect_file_reserved(&mut Cursor::new(bytes.as_ref()), |amount| memory.reserve(amount))?;
    let opening = memory.reserve(layout.opening_allocation_bound()?)?;
    let file = if layout.uncompressed && layout.dictionary_encoded_bytes == 0 {
        ArrowFile::from_inspected_buffer(Buffer::from(bytes), None, layout)?
    } else {
        ArrowFile::from_inspected_reader(Cursor::new(bytes), None, layout)?
    };
    Ok((file, opening))
}

async fn read_trie(
    store: &dyn ObjectStore,
    key: &ObjectKey,
    length: usize,
    events: &mut Events<'_>,
    cancellation: &CancellationToken,
) -> Result<()> {
    let allocation = store.whole_object_allocation(key, length)?;
    let retained = events.memory.reserve(allocation.retained_bytes)?;
    let scratch = (allocation.scratch_bytes != 0)
        .then(|| events.memory.reserve(allocation.scratch_bytes))
        .transpose()?;
    let bytes = store
        .get_reserved_object(key, ReadReservation::funded(length, retained, scratch))
        .await?;
    let (mut file, opening) = open_survivor_input(Bytes::from_owner(TrieInput(bytes)), events.memory)?;
    for page in 0..file.page_count() {
        check_cancelled(cancellation)?;
        let decoded = events.memory.reserve(file.page_allocation_bound(page)?)?;
        events.add(&EventBatch::new(file.read_page(page)?)?, Some((&opening, &decoded)))?;
        tokio::task::yield_now().await;
    }
    Ok(())
}

struct Candidate<'a> {
    store: &'a MemoryStore,
    memory: &'a Arc<MemoryBudget>,
    remaining: u64,
}

struct ChargedOutput {
    bytes: Vec<u8>,
    _allocation: Arc<MemoryReservation>,
}

impl AsRef<[u8]> for ChargedOutput {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

struct TrieOutputSink<'a> {
    key: &'a ObjectKey,
    provider_bytes: u64,
    output: &'a mut ChargedOutput,
    refusal: &'a mut Option<Anomaly>,
}

impl TrieOutputSink<'_> {
    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        let current = &mut self.output.bytes;
        let next = current.len().checked_add(bytes.len()).ok_or_else(|| invalid("Survivor trie output size overflows"))?;
        if rtdb_storage::sealing::cipher::object::sealed_size(self.key, next)? as u64 > self.provider_bytes {
            return Err(invalid("The survivor closure exceeds its provider reservation"));
        }
        if next > current.capacity() {
            let provider_bytes = usize::try_from(self.provider_bytes).unwrap_or(usize::MAX);
            let growth = current.capacity().saturating_mul(2).min(provider_bytes);
            let capacity = next.max(growth);
            let grant = Arc::get_mut(&mut self.output._allocation).expect("Survivor output has unique custody");
            // The allocator may briefly hold both the old and new buffers.
            let peak = grant.bytes().checked_add(capacity).ok_or_else(|| invalid("The survivor allocation overflows"))?;
            grant.grow_to(peak)?;
            current.try_reserve_exact(capacity - current.len()).map_err(|_| invalid("Cannot allocate the survivor trie"))?;
            let retained = current.capacity().checked_add(256).ok_or_else(|| invalid("The survivor allocation overflows"))?;
            grant.shrink_to(retained)?;
        }
        current.extend_from_slice(bytes);
        Ok(())
    }
}

impl Write for TrieOutputSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self.append(bytes) {
            Ok(()) => Ok(bytes.len()),
            Err(error) => {
                let io_error = io::Error::other(error.to_string());
                *self.refusal = Some(error);
                Err(io_error)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

impl Candidate<'_> {
    async fn write(&mut self, key: ObjectKey, bytes: Vec<u8>) -> Result<()> {
        self.charge_provider(&key, bytes.len())?;
        let _allocation = self.memory.reserve(
            bytes
                .capacity()
                .checked_add(256)
                .ok_or_else(|| invalid("The survivor allocation overflows"))?,
        )?;
        self.publish(key, ChargedOutput { bytes, _allocation }).await
    }

    fn charge_provider(&mut self, key: &ObjectKey, size: usize) -> Result<()> {
        let sealed = rtdb_storage::sealing::cipher::object::sealed_size(key, size)? as u64;
        self.remaining = self.remaining.checked_sub(sealed)
            .ok_or_else(|| invalid("The survivor closure exceeds its provider reservation"))?;
        Ok(())
    }

    async fn publish(&self, key: ObjectKey, output: ChargedOutput) -> Result<()> {
        self.store
            .put(&key, Bytes::from_owner(output))
            .await?;
        Ok(())
    }

    async fn write_trie(
        &mut self,
        table: &TableEntry,
        trie: &TrieKey,
        live: &LiveTable,
        cancellation: &CancellationToken,
    ) -> Result<Option<CatalogTrie>> {
        if !write_survivor_trie(live.row_count()) {
            return Ok(None);
        }
        let pages = live.block_pages(self.memory)?;
        let key = ObjectKey::new(table.slug.data_key(trie))?;
        let mut data = ChargedOutput { bytes: Vec::new(), _allocation: self.memory.reserve(256)? };
        let mut refusal = None;
        let metadata = {
            let mut output = TrieOutput {
                metadata: MetadataBuilder::new(false, false),
                sink: Some(TrieOutputSink { key: &key, provider_bytes: self.remaining, output: &mut data, refusal: &mut refusal }),
                encoder: None,
                retained: Vec::new(),
            };
            output.copy(live.trie().root(), &pages, self.memory, cancellation)
                .and_then(|_| output.finish())
        };
        if let Some(refusal) = refusal { return Err(refusal); }
        let (metadata, _retained) = metadata?;
        let size = i64::try_from(data.bytes.len())
            .map_err(|_| invalid("Survivor trie size exceeds its format"))?;
        self.charge_provider(&key, data.bytes.len())?;
        self.publish(key, data).await?;
        let _encoding = self
            .memory
            .reserve(metadata.serialization_bound()?)?;
        let (meta, summary) = metadata.finish()?;
        self.write(
            ObjectKey::new(table.slug.metadata_key(trie))?,
            meta.write()?,
        )
        .await?;
        Ok(Some(CatalogTrie::notification(
            trie.clone(),
            size,
            Some(summary),
        )?))
    }
}

struct TrieOutput<'a> {
    metadata: MetadataBuilder,
    sink: Option<TrieOutputSink<'a>>,
    encoder: Option<ArrowFileEncoder<TrieOutputSink<'a>>>,
    retained: Vec<Arc<MemoryReservation>>,
}

impl TrieOutput<'_> {
    fn finish(self) -> Result<(MetadataBuilder, Vec<Arc<MemoryReservation>>)> {
        self.encoder.ok_or_else(|| invalid("Nonempty survivor trie has no data pages"))?.finish()?;
        Ok((self.metadata, self.retained))
    }

    fn copy(
        &mut self,
        node: &TrieNode,
        pages: &BlockPages<'_>,
        memory: &Arc<MemoryBudget>,
        cancellation: &CancellationToken,
    ) -> Result<usize> {
        check_cancelled(cancellation)?;
        match node {
            TrieNode::Branch { children, .. } => {
                let mut offsets = [None; 4];
                for (slot, child) in offsets.iter_mut().zip(children) {
                    if let Some(child) = child {
                        *slot = Some(self.copy(child, pages, memory, cancellation)?);
                    }
                }
                self.metadata.write_iid_branch(offsets)
            }
            TrieNode::Leaf(leaf) => {
                let (rows, _sort) = leaf.sorted_rows_in(pages.live_table(), memory)?;
                let page = pages.copy(&rows)?;
                let (index, allocation) =
                    self.metadata.write_leaf_reserved(page.batch(), memory)?;
                self.retained.push(allocation);
                if let Some(sink) = self.sink.take() {
                    self.encoder = Some(ArrowFileEncoder::new(sink, page.batch().schema_ref(), memory)?);
                }
                self.encoder.as_mut().expect("A leaf owns its output encoder").write(page.batch())?;
                Ok(index)
            }
        }
    }
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() {
        return Err(crate::progress::cancelled());
    }
    Ok(())
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb/sql-survivor", message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::ipc::write_file_pages;
    use rtdb_types::{BlockIndex, Interval, ScalarType, TableRef, Timestamp, TxId};

    #[test]
    fn survivor_stream_preserves_ipc_bytes_and_retains_only_final_capacity() -> Result<()> {
        use arrow_array::{ArrayRef, BinaryArray, RecordBatch};

        let pages = [b"old survivor".as_slice(), b"later survivor".as_slice()].map(|payload| {
            let cells = BinaryArray::from_iter_values([payload]);
            RecordBatch::try_from_iter([("body", Arc::new(cells) as ArrayRef)]).unwrap()
        });
        let schema = pages[0].schema_ref();
        let expected = write_file_pages(schema, &pages)?;
        let key = ObjectKey::new("tables/test/data/000.arrow")?;
        let sealed = rtdb_storage::sealing::cipher::object::sealed_size(&key, expected.len())? as u64;
        let memory = MemoryBudget::new(1 << 21);

        for limit in [sealed, sealed - 1] {
            let mut output = ChargedOutput { bytes: Vec::new(), _allocation: memory.reserve(256)? };
            let mut refusal = None;
            let result = (|| {
                let sink = TrieOutputSink { key: &key, provider_bytes: limit, output: &mut output, refusal: &mut refusal };
                let mut file = ArrowFileEncoder::new(sink, schema, &memory)?;
                for page in &pages { file.write(page)?; }
                file.finish()?;
                Ok::<(), Anomaly>(())
            })();
            if limit == sealed {
                result?;
                assert!(refusal.is_none());
                assert_eq!(output.bytes, expected);
                assert_eq!(memory.used_bytes(), output.bytes.capacity() + 256);
            } else {
                assert!(result.is_err());
                assert_eq!(refusal.unwrap().code, "rtdb/sql-survivor");
            }
            drop(output);
            assert_eq!(memory.used_bytes(), 0);
        }
        Ok(())
    }

    #[test]
    fn compatible_survivor_input_reuses_the_funded_file_body() -> Result<()> {
        use arrow_array::{ArrayRef, BinaryArray, RecordBatch};

        struct Aligned {
            buffer: Buffer,
            _reservation: Arc<MemoryReservation>,
        }
        impl AsRef<[u8]> for Aligned {
            fn as_ref(&self) -> &[u8] { self.buffer.as_slice() }
        }

        let cells = BinaryArray::from_iter_values([b"survivor".as_slice()]);
        let batch = RecordBatch::try_from_iter([("body", Arc::new(cells) as ArrayRef)]).unwrap();
        let file = write_file_pages(batch.schema_ref(), [&batch])?;
        let memory = MemoryBudget::new(1 << 20);
        let size = file.len();
        let bytes = Bytes::from_owner(Aligned {
            buffer: Buffer::from_slice_ref(&file), _reservation: memory.reserve(size)?,
        });
        let span = bytes.as_ref().as_ptr_range();
        let (mut file, opening) = open_survivor_input(bytes, &memory)?;
        let page = file.read_page(0)?;
        let output = page.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(span.contains(&output.value(0).as_ptr()));
        assert_eq!(output.value(0), b"survivor");
        drop((file, opening));
        assert_eq!(memory.used_bytes(), size);
        drop(page);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn positioned_survivors_match_scalar_checkpoint_bytes() -> Result<()> {
        use rtdb_format::data::build_event_batch;
        use rtdb_format::ipc::write_file;

        let memory = MemoryBudget::new(32 << 20);
        let cancellation = CancellationToken::new();
        let entry = TableEntry::new(1, TableRef::new("public", "orders"))?;
        let block = BlockIndex(2);
        let trie = TrieKey::l0(block)?;
        let declarations = StructFields::new();
        let iid = Iid([5; 16]);
        let position = |ordinal| EventPosition { transaction: TxId(10), ordinal };
        let event = |ordinal, operation, recency| StoredEvent {
            position: Some(position(ordinal)),
            origin: Some(EventOrigin([ordinal as u8; EventOrigin::WIDTH as usize])),
            entity_key: None,
            iid,
            system_from: Timestamp(10),
            commit_position: 0,
            valid_time: Interval::ALL,
            operation,
            recency: Some(Timestamp(recency)),
        };
        let document = |zero| StoredOperation::Put(StructFields::from([
            ("name".into(), Value::Utf8("survivor".into())),
            ("count".into(), Value::I64(42)),
            ("enabled".into(), Value::Bool(true)),
            ("i8".into(), Value::I8(i8::MIN)),
            ("i16".into(), Value::I16(i16::MIN)),
            ("i32".into(), Value::I32(i32::MIN)),
            ("u8".into(), Value::U8(u8::MAX)),
            ("u16".into(), Value::U16(u16::MAX)),
            ("u32".into(), Value::U32(u32::MAX)),
            ("u64".into(), Value::U64(u64::MAX)),
            ("half".into(), Value::F16(0x8000)),
            ("zero".into(), Value::F32(zero)),
            ("double".into(), Value::F64(-0.0)),
            ("raw".into(), Value::Binary(vec![0xff, 0, 0x42])),
            ("fixed".into(), Value::FixedSizeBinary(vec![0x42, 0])),
            ("day".into(), Value::DateDay(-1)),
            ("millis".into(), Value::DateMilli(-86_400_000)),
        ]));
        let StoredOperation::Put(example) = document(-0.0) else { unreachable!() };
        let fields = Some(example.into_iter().map(|(name, value)| (name, value.logical_type())).collect());
        let disk = build_event_batch(&[
            event(3, document(-0.0), 110),
            event(1, document(-0.0), 110),
            event(2, StoredOperation::Erase, 110),
        ], fields.clone(), Some(rtdb_types::RunEndWidth::I32))?;
        let live = EventBatch::new(build_event_batch(&[
            event(3, document(0.0), 220),
            event(4, document(0.0), 220),
        ], fields, None)?)?;
        let source = MemoryStore::default();
        let key = ObjectKey::new(entry.slug.data_key(&trie))?;
        let bytes = write_file(disk.schema_ref(), std::slice::from_ref(&disk))?;
        let length = bytes.len();
        source.put(&key, Bytes::from(bytes)).await?;
        let targets = BTreeMap::from([(iid, position(2))]);
        let mut outputs = Vec::new();
        for positions in [true, false] {
            let store = MemoryStore::default();
            let mut used_positions = false;
            let mut rows = Events {
                memory: &memory, rows: BTreeMap::new(), targets: &targets,
                inherited_erasure: None, positions, used_positions: &mut used_positions,
            };
            read_trie(&source, &key, length, &mut rows, &cancellation).await?;
            rows.add(&live, None)?;
            assert_eq!(rows.rows.keys().copied().collect::<Vec<_>>(), [position(2), position(3), position(4)]);
            assert!(rows.rows.values().all(|row| matches!(&row.payload, SurvivorPayload::Position(..)) == positions));
            assert_eq!(copy_positions(rows.rows.values(), &memory)?.is_some(), positions);
            let rebuilt = rows.into_live(&entry, block, &declarations, &cancellation)?;
            assert_eq!(used_positions, positions);
            let mut output = Candidate { store: &store, memory: &memory, remaining: 16 << 20 };
            output.write_trie(&entry, &trie, &rebuilt, &cancellation).await?.unwrap();
            let image = store.get(&key).await?;
            let mut file = ArrowFile::from_reader(Cursor::new(image.clone()), None)?;
            let page = EventBatch::new(file.read_page(0)?)?;
            assert_eq!(page.len(), 3);
            for row in 0..page.len() {
                let event = page.read_event(row)?;
                assert_eq!(event.iid, iid);
                assert_eq!(event.recency, None);
                let ordinal = event.position.unwrap().ordinal;
                assert_eq!(event.origin, Some(EventOrigin([ordinal as u8; EventOrigin::WIDTH as usize])));
                if let StoredOperation::Put(document) = event.operation {
                    let Value::F32(zero) = document["zero"] else { panic!("Expected a typed float") };
                    assert_eq!(zero.to_bits(), if ordinal == 3 { (-0.0_f32).to_bits() } else { 0 });
                }
            }
            let metadata = store.get(&ObjectKey::new(entry.slug.metadata_key(&trie))?).await?;
            outputs.push((image, metadata, rebuilt.block_metadata()?.0));
        }
        assert_eq!(outputs[0], outputs[1]);
        drop(outputs);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[test]
    fn survivor_duplicate_equality_normalizes_nested_padding_and_rejects_conflicts() -> Result<()> {
        let memory = MemoryBudget::new(4 << 20);
        let position = EventPosition { transaction: TxId(10), ordinal: 0 };
        let batch = |nested| -> Result<Arc<IndexBatch>> {
            IndexBatch::copy_events(&[StoredEvent {
                position: Some(position), origin: None, entity_key: None,
                iid: Iid([5; 16]), system_from: Timestamp(10), commit_position: 0,
                valid_time: Interval::ALL, recency: None,
                operation: StoredOperation::Put(StructFields::from([
                    ("nested".into(), Value::list(vec![Value::Struct(nested)])),
                ])),
            }], &StructFields::new(), &memory)
        };
        let padded = batch(StructFields::from([("padding".into(), Value::Null)]))?;
        let empty = batch(StructFields::new())?;
        let different = batch(StructFields::from([("padding".into(), Value::I64(9))]))?;
        let mut used_positions = false;
        let mut rows = Events { memory: &memory, rows: BTreeMap::new(), targets: &BTreeMap::new(),
            inherited_erasure: None, positions: true, used_positions: &mut used_positions };
        rows.add(padded.view(), None)?;
        rows.add(empty.view(), None)?;
        assert_eq!(rows.rows.len(), 1);
        assert!(matches!(&rows.rows[&position].payload, SurvivorPayload::Scalar(_)));
        let error = rows.add(different.view(), None).unwrap_err();
        assert_eq!(error.message.as_deref(), Some("Two primary rows claim the same original event"));
        Ok(())
    }

    #[test]
    fn survivor_schema_excludes_erased_inference_and_retains_a_later_put() -> Result<()> {
        let memory = MemoryBudget::new(16 << 20);
        let iid = Iid([5; 16]);
        let other = Iid([6; 16]);
        let position = |id| EventPosition {
            transaction: TxId(id),
            ordinal: 0,
        };
        let event = |id, iid, operation| StoredEvent {
            position: Some(position(id)),
            origin: None,
            entity_key: None,
            iid,
            system_from: Timestamp(id),
            commit_position: 0,
            valid_time: Interval::ALL,
            operation,
            recency: None,
        };
        let put = |field: &str, value| {
            StoredOperation::Put(StructFields::from([(field.to_owned(), value)]))
        };
        let batch = IndexBatch::copy_events(
            &[
                event(1, iid, put("erase_only", Value::Utf8("target".into()))),
                event(2, other, put("survives", Value::I64(2))),
                event(3, iid, StoredOperation::Erase),
                event(4, iid, put("later", Value::Bool(true))),
            ],
            &StructFields::new(),
            &memory,
        )?;
        let targets = BTreeMap::from([(iid, position(3))]);
        let mut used_positions = false;
        let mut rows = Events { memory: &memory, rows: BTreeMap::new(), targets: &targets,
            inherited_erasure: None, positions: false, used_positions: &mut used_positions };
        rows.add(batch.view(), None)?;
        let entry = TableEntry::new(1, TableRef::new("public", "orders"))?;
        let declarations =
            StructFields::from([("ddl_only".into(), LogicalType::Scalar(ScalarType::Utf8))]);
        let live = rows.into_live(
            &entry,
            BlockIndex(2),
            &declarations,
            &CancellationToken::new(),
        )?;
        let (metadata, _hll_reservation) = live.block_metadata()?;
        assert_eq!(metadata.declared_columns, declarations);
        assert!(!metadata.column_types.contains_key("erase_only"));
        assert!(metadata.column_types.contains_key("survives"));
        assert!(metadata.column_types.contains_key("later"));
        assert_eq!(live.row_count(), 3);
        Ok(())
    }
}
