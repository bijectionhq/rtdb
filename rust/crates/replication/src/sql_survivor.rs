//! Builds a spill-free primary checkpoint from an authenticated SQL erasure cut.

mod selection;

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    io::{self, Cursor, Write},
    sync::Arc,
};

use arrow_buffer::Buffer;
use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{
    block::proto as pb,
    catalog::{BlockCatalog, TableEntry, publication_digest},
    data::{EventBatch, StoredEvent, StoredOperation},
    ipc::{ArrowFile, ArrowFileEncoder, inspect_file_reserved},
    keys::{TrieKey, block_key},
    metadata::MetadataBuilder,
};
use rtdb_index::{BlockPages, CatalogTrie, IndexBatch, LiveTable, Snapshot, TableWrite, TrieCatalog};
use rtdb_storage::{IoRegion, MemoryStore, ObjectKey, ObjectStore, ReadReservation, RetainedBytes};
use rtdb_trie::{TrieConfig, TrieNode};
use rtdb_types::{
    Anomaly, AnomalyCategory, CatalogCut, EventOrigin, EventPosition, Iid, LogicalType, Result, SourcePosition, StructFields, Value,
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
            let mut rows = Events {
                memory: self.memory,
                rows: BTreeMap::new(),
                targets,
                inherited_erasure: self.inherited_erasure,
            };
            let retired = snapshot.trie_catalog().retired_as_of(&entry.table);
            if retired.is_none() {
                for primary in snapshot.trie_entries(&entry.table) {
                    check_cancelled(cancellation)?;
                    let key = ObjectKey::new(entry.slug.data_key(&primary.key))?;
                    let length = usize::try_from(primary.data_file_size)
                        .map_err(|_| invalid("A source trie exceeds addressable memory"))?;
                    IoRegion::retaining(
                        Arc::clone(self.custody),
                        read_trie(self.store, &key, length, &mut rows, cancellation),
                    )
                    .await?;
                }
                for segment in snapshot.iter_segments(&entry.table) {
                    for batch in segment.batches() {
                        check_cancelled(cancellation)?;
                        rows.add(batch.view())?;
                    }
                }
            }
            seen_targets += rows
                .targets
                .iter()
                .filter(|(iid, position)| {
                    rows.rows.get(*position).is_some_and(|(event, _)| {
                        event.iid == **iid && matches!(event.operation, StoredOperation::Erase)
                    })
                })
                .count();
            let declarations = snapshot.declared_columns(&entry.table);
            let rebuilt = rows.into_live(entry, block, declarations, cancellation)?;
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
}

struct Events<'a> {
    memory: &'a Arc<MemoryBudget>,
    rows: BTreeMap<EventPosition, (StoredEvent, Arc<MemoryReservation>)>,
    targets: BTreeMap<Iid, EventPosition>,
    inherited_erasure: Option<&'a BTreeSet<EventOrigin>>,
}

impl Events<'_> {
    fn add(&mut self, batch: &EventBatch) -> Result<()> {
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
            let borrowed = batch.borrow_event(row)?;
            let bound = borrowed
                .document_allocation_bound()?
                .checked_add(borrowed.physical_entity_key().map_or(0, |key| key.len()))
                .and_then(|size| size.checked_add(1024))
                .ok_or_else(|| invalid("A surviving row exceeds addressable memory"))?;
            let reservation = self.memory.reserve(bound)?;
            let mut event = batch.read_event(row)?;
            event.recency = None;
            if let StoredOperation::Put(document) = &mut event.operation {
                remove_padding(document);
            }
            match self.rows.entry(position) {
                Entry::Vacant(entry) => {
                    entry.insert((event, reservation));
                }
                Entry::Occupied(entry) if entry.get().0 == event => {}
                Entry::Occupied(_) => {
                    return Err(invalid("Two primary rows claim the same original event"));
                }
            }
        }
        Ok(())
    }

    fn into_live(
        self,
        entry: &TableEntry,
        block: rtdb_types::BlockIndex,
        declarations: StructFields<LogicalType>,
        cancellation: &CancellationToken,
    ) -> Result<LiveTable> {
        let mut live = LiveTable::new(entry.table.clone(), block, TrieConfig::default());
        let empty =
            TableWrite::new(declarations.clone(), None).with_declarations(declarations.clone());
        live = live.append(&empty, rtdb_types::Timestamp(i64::MIN))?;
        let mut ordered = self.rows.into_iter().peekable();
        while let Some((position, (first, reservation))) = ordered.next() {
            check_cancelled(cancellation)?;
            let time = first.system_from;
            let mut events = vec![first];
            let mut reservations = vec![reservation];
            while ordered
                .peek()
                .is_some_and(|(following, _)| following.transaction == position.transaction)
            {
                let (_, (event, reservation)) = ordered.next().expect("peeked row");
                events.push(event);
                reservations.push(reservation);
            }
            let batch = IndexBatch::copy_events(&events, &declarations, self.memory)?;
            live = live.append(&TableWrite::new(declarations.clone(), Some(batch)), time)?;
        }
        Ok(live)
    }
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
    let (mut file, _opening) = open_survivor_input(Bytes::from_owner(TrieInput(bytes)), events.memory)?;
    for page in 0..file.page_count() {
        check_cancelled(cancellation)?;
        let _decoded = events.memory.reserve(file.page_allocation_bound(page)?)?;
        events.add(&EventBatch::new(file.read_page(page)?)?)?;
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
        let mut rows = Events {
            memory: &memory,
            rows: BTreeMap::new(),
            targets: BTreeMap::from([(iid, position(3))]),
            inherited_erasure: None,
        };
        rows.add(batch.view())?;
        let entry = TableEntry::new(1, TableRef::new("public", "orders"))?;
        let declarations =
            StructFields::from([("ddl_only".into(), LogicalType::Scalar(ScalarType::Utf8))]);
        let live = rows.into_live(
            &entry,
            BlockIndex(2),
            declarations.clone(),
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
