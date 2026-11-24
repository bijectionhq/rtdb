//! Builds a spill-free primary checkpoint from an authenticated SQL erasure cut.

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    io::Cursor,
    sync::Arc,
};

use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_format::{
    block::proto as pb,
    catalog::{BlockCatalog, TableEntry, publication_digest},
    data::{EventBatch, StoredEvent, StoredOperation},
    ipc::{ArrowFile, inspect_file_reserved, write_file_pages},
    keys::{TrieKey, block_key},
    metadata::MetadataBuilder,
};
use rtdb_index::{BlockPages, CatalogTrie, IndexBatch, LiveTable, TableWrite, TrieCatalog};
use rtdb_storage::{IoRegion, MemoryStore, ObjectKey, ObjectStore, ReadReservation, RetainedBytes};
use rtdb_trie::{TrieConfig, TrieNode};
use rtdb_types::{
    Anomaly, AnomalyCategory, EventOrigin, EventPosition, Iid, LogicalType, Result, StructFields, Value,
};
use tokio_util::sync::CancellationToken;

use crate::{SqlErasureSource, TermPosition};

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
        let snapshot = self.snapshot();
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
            .targets()
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
            memory: &self.memory,
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
                .targets()
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
                memory: &self.memory,
                rows: BTreeMap::new(),
                targets,
                inherited_erasure: self.inherited_erasure.as_ref(),
            };
            let retired = snapshot.trie_catalog().retired_as_of(&entry.table);
            if retired.is_none() {
                for primary in snapshot.trie_entries(&entry.table) {
                    check_cancelled(cancellation)?;
                    let key = ObjectKey::new(entry.slug.data_key(&primary.key))?;
                    let length = usize::try_from(primary.data_file_size)
                        .map_err(|_| invalid("A source trie exceeds addressable memory"))?;
                    IoRegion::retaining(
                        Arc::clone(&self.custody),
                        read_trie(self.store.as_ref(), &key, length, &mut rows, cancellation),
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
            let table_block = rebuilt
                .block_metadata()?
                .to_block(catalog.partitions(&entry.table)?)?
                .with_retired_as_of(retired.map(|instant| instant.0));
            let bytes = table_block.encode();
            digests.insert(entry.oid, publication_digest(&bytes).to_vec());
            destination
                .write(ObjectKey::new(entry.slug.block_key(block)?)?, bytes)
                .await?;
        }
        let required = self
            .targets()
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
            latest_processed_msg_id: Some(self.source_position().0),
            table_names: registry
                .entries()
                .map(|entry| entry.table.schema_and_table())
                .collect(),
            tables: registry.entries().map(TableEntry::to_proto).collect(),
            boundary_replica_msg_id: Some(self.cut().through.0),
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
                if origins.contains(&origin) { continue; }
            }
            let position = header
                .position
                .ok_or_else(|| invalid("A surviving event has no original transaction position"))?;
            if self
                .targets
                .get(&header.iid)
                .is_some_and(|before| position < *before)
            {
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
    let mut file = Cursor::new(TrieInput(bytes));
    let layout = inspect_file_reserved(&mut file, |amount| events.memory.reserve(amount))?;
    let _opening = events.memory.reserve(layout.opening_allocation_bound()?)?;
    let mut file = ArrowFile::from_inspected_reader(file, None, layout)?;
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

impl Candidate<'_> {
    async fn write(&mut self, key: ObjectKey, bytes: Vec<u8>) -> Result<()> {
        let sealed = rtdb_storage::sealing::cipher::object::sealed_size(&key, bytes.len())? as u64;
        self.remaining = self
            .remaining
            .checked_sub(sealed)
            .ok_or_else(|| invalid("The survivor closure exceeds its provider reservation"))?;
        let _allocation = self.memory.reserve(
            bytes
                .capacity()
                .checked_add(256)
                .ok_or_else(|| invalid("The survivor allocation overflows"))?,
        )?;
        self.store
            .put(
                &key,
                Bytes::from_owner(ChargedOutput { bytes, _allocation }),
            )
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
        if live.row_count() == 0 {
            return Ok(None);
        }
        let pages = live.block_pages(self.memory)?;
        let mut output = TrieOutput {
            metadata: MetadataBuilder::new(false, false),
            pages: Vec::new(),
            retained: Vec::new(),
        };
        output.copy(live.trie().root(), &pages, self.memory, cancellation)?;
        let first = output
            .pages
            .first()
            .ok_or_else(|| invalid("Nonempty survivor trie has no data pages"))?;
        let schema = first.batch().schema_ref();
        let bound = output.pages.iter().try_fold(
            rtdb_format::ipc::schema_translation_bound(schema)?,
            |size, page| {
                page.batch()
                    .get_array_memory_size()
                    .checked_mul(4)
                    .and_then(|bytes| bytes.checked_add(size))
                    .ok_or_else(|| invalid("Survivor trie serialization exceeds memory"))
            },
        )?;
        let _encoding = self.memory.reserve(bound)?;
        let data = write_file_pages(schema, output.pages.iter().map(OwnedBatch::batch))?;
        let size = i64::try_from(data.len())
            .map_err(|_| invalid("Survivor trie size exceeds its format"))?;
        self.write(ObjectKey::new(table.slug.data_key(trie))?, data)
            .await?;
        let _encoding = self
            .memory
            .reserve(output.metadata.serialization_bound()?)?;
        let (meta, summary) = output.metadata.finish()?;
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

struct TrieOutput {
    metadata: MetadataBuilder,
    pages: Vec<OwnedBatch>,
    retained: Vec<Arc<MemoryReservation>>,
}

impl TrieOutput {
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
                self.pages.push(page);
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
    use rtdb_types::{BlockIndex, Interval, ScalarType, TableRef, Timestamp, TxId};

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
        let metadata = live.block_metadata()?;
        assert_eq!(metadata.declared_columns, declarations);
        assert!(!metadata.column_types.contains_key("erase_only"));
        assert!(metadata.column_types.contains_key("survives"));
        assert!(metadata.column_types.contains_key("later"));
        assert_eq!(live.row_count(), 3);
        Ok(())
    }
}
