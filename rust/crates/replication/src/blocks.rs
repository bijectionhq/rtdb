use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use rtdb_columnar::{MemoryBudget, MemoryReservation, estimate_read_value_bytes};
use rtdb_format::{block::proto as block_pb, catalog::{BlockCatalog, TableBlock}, database::proto::DatabaseConfig, envelope::ReplicaBody, ipc::ArrowFileWriter, keys::{block_index_from_key, block_key, TrieKey}, metadata::MetadataBuilder};
use rtdb_index::{BlockPreparation, CatalogTrie, IndexBatch, LiveTable, PartitionState, PartitionValue, TableCatalog, TrieCatalog};
use rtdb_storage::{ObjectKey, ObjectStore, cache::BufferPool};
use rtdb_trie::{TrieConfig, TrieNode};
use rtdb_types::{Anomaly, BlockIndex, Result, SourcePosition, Timestamp};
use tokio_util::sync::CancellationToken;

use crate::{AppliedProgress, ReplicaRecord, TermFence};

pub struct RecoveredPartition {
    pub partition: Arc<PartitionState>,
    pub block: Option<BlockCatalog>,
    pub progress: AppliedProgress,
    pub fence: TermFence,
}
impl RecoveredPartition {
    /// Selects initial source progress for the configured log epoch while retaining persisted identities.
    pub fn with_source_floor(mut self, source_floor: SourcePosition) -> Self {
        self.progress.source_position = self.progress.source_position.max(source_floor);
        self
    }
}

#[derive(Clone, Debug)]
pub struct ProducedBlock {
    pub catalog: BlockCatalog,
    pub tries: BTreeMap<rtdb_types::TableRef, Vec<CatalogTrie>>,
}

pub struct BlockStore {
    store: Arc<dyn ObjectStore>,
    cache: Arc<BufferPool>,
    memory: Arc<MemoryBudget>,
    file_size_target: i64,
}
impl BlockStore {
    /// The object store and cache must share this database partition's storage prefix.
    pub fn new(store: Arc<dyn ObjectStore>, cache: Arc<BufferPool>, memory: Arc<MemoryBudget>, file_size_target: i64) -> Result<Arc<Self>> {
        TrieCatalog::new(file_size_target)?;
        Ok(Arc::new(Self { store, cache, memory, file_size_target }))
    }
    pub async fn recover(&self, config: TrieConfig, source_floor: SourcePosition, now: Timestamp) -> Result<RecoveredPartition> {
        let directory = ObjectKey::new("blocks")?;
        let latest = self.store.list(Some(&directory)).await?.into_iter()
            .filter_map(|entry| block_index_from_key(entry.key.as_str()).ok().map(|index| (index, entry.key)))
            .max_by_key(|(index, _)| *index);
        let (block, table_blocks) = match latest {
            Some((index, _)) => { let loaded = self.load(index).await?; (Some(loaded.0), loaded.1) }
            None => (None, BTreeMap::new()),
        };
        let tables = TableCatalog::load(block.clone(), &table_blocks)?.with_system_tables()?;
        let tries = TrieCatalog::load(&table_blocks, self.file_size_target, now)?;
        let partition = Arc::new(PartitionState::new(PartitionValue::loaded(tries, tables, config)?));
        let progress = AppliedProgress {
            source_position: block.as_ref().and_then(BlockCatalog::latest_source_position).unwrap_or(source_floor),
            tx_key: block.as_ref().and_then(BlockCatalog::latest_completed_tx), latest_result: None,
            external_token: block.as_ref().and_then(BlockCatalog::external_source_token).map(Arc::from),
        };
        let fence = match &block { Some(block) => TermFence::from_catalog(block)?, None => TermFence::from_boundary(rtdb_types::Term(0), None)? };
        Ok(RecoveredPartition { partition, block, progress, fence }.with_source_floor(source_floor))
    }
    pub async fn load(&self, index: BlockIndex) -> Result<(BlockCatalog, BTreeMap<rtdb_types::TableRef, TableBlock>)> {
        let lease = self.cache.get(&ObjectKey::new(block_key(index)?)?).await?;
        let reservation = self.reserve_decode(lease.as_slice().len())?;
        let block = BlockCatalog::decode_reserved(lease.as_slice(), reservation)?;
        if block.block_index() != index { return Err(invalid("Block filename and payload index differ")); }
        let mut tables = BTreeMap::new();
        for entry in block.table_entries()? {
            let lease = self.cache.get(&ObjectKey::new(entry.slug.block_key(index)?)?).await?;
            let reservation = self.reserve_decode(lease.as_slice().len())?;
            tables.insert(entry.table, TableBlock::decode_reserved(lease.as_slice(), reservation)?);
        }
        Ok((block, tables))
    }
    pub async fn produce(&self, prepared: &BlockPreparation, boundary: &ReplicaRecord, databases: BTreeMap<String, DatabaseConfig>) -> Result<ProducedBlock> {
        self.produce_cancellable(prepared, boundary, databases, &CancellationToken::new()).await
    }
    /// Cancellation is observed between pages and completed storage operations; an upload already started is awaited.
    pub async fn produce_cancellable(&self, prepared: &BlockPreparation, boundary: &ReplicaRecord, databases: BTreeMap<String, DatabaseConfig>, cancellation: &CancellationToken) -> Result<ProducedBlock> {
        check_cancelled(cancellation)?;
        let ReplicaBody::BlockBoundary(message) = &boundary.envelope.message else { return Err(invalid("Block production requires a boundary")); };
        let index = BlockIndex(message.block_index.unwrap_or_default());
        if index != prepared.block_index() { return Err(invalid("Boundary does not match the current live block")); }
        let key = ObjectKey::new(block_key(index)?)?;
        let exists = match self.store.head(&key).await {
            Ok(_) => true,
            Err(rtdb_storage::Error::MissingObject(_)) => false,
            Err(error) => return Err(error.into()),
        };
        if exists {
            let (catalog, tables) = self.load(index).await?;
            validate_boundary(&catalog, prepared, boundary)?;
            return Ok(ProducedBlock { catalog, tries: l0_tries(index, &tables)? });
        }
        let table_catalog = prepared.table_catalog()?;
        let mut tries = BTreeMap::new();
        for (table, live) in prepared.value().live().tables() {
            check_cancelled(cancellation)?;
            if live.row_count() == 0 { continue; }
            let entry = table_catalog.entry(table).ok_or_else(|| invalid("Live table has no recorded storage slug"))?;
            let trie = TrieKey::l0(index)?;
            let mut metadata = MetadataBuilder::new(false, true);
            let mut reservations = Vec::new();
            let temporary = tempfile::NamedTempFile::new().map_err(io_error)?;
            let mut writer = None;
            write_node(live.trie().root(), live, &self.memory, &temporary, &mut writer, &mut metadata, &mut reservations, cancellation).await?;
            let file = writer.ok_or_else(|| invalid("Nonempty live table produced no pages"))?.finish()?;
            file.sync_all().map_err(io_error)?;
            let size = i64::try_from(file.metadata().map_err(io_error)?.len()).map_err(io_error)?;
            self.store.put_file(&ObjectKey::new(entry.slug.data_key(&trie))?, temporary.path()).await?;
            check_cancelled(cancellation)?;
            let (metadata_file, summary) = metadata.finish()?;
            let bytes = metadata_file.write()?;
            self.store.put(&ObjectKey::new(entry.slug.metadata_key(&trie))?, Bytes::from(bytes)).await?;
            tries.insert(table.clone(), vec![CatalogTrie::notification(trie, size, Some(summary))?]);
            tokio::task::yield_now().await;
        }
        let table_blocks = prepared.table_blocks(&tries, boundary.metadata.timestamp)?;
        for entry in table_catalog.entries() {
            check_cancelled(cancellation)?;
            let block = table_blocks.get(&entry.table).ok_or_else(|| invalid("Recorded table has no block metadata"))?;
            self.store.put(&ObjectKey::new(entry.slug.block_key(index)?)?, Bytes::from(block.encode())).await?;
        }
        let tx_key = prepared.value().live().latest_completed_tx();
        check_cancelled(cancellation)?;
        let catalog = BlockCatalog::from_proto(block_pb::Block {
            block_index: Some(index.0), latest_completed_tx: tx_key.map(|key| block_pb::TxKey { tx_id: Some(key.tx_id.0), system_time: Some(key.system_time.0) }),
            latest_processed_msg_id: message.latest_processed_msg_id,
            table_names: table_catalog.entries().map(|entry| entry.table.schema_and_table()).collect(),
            tables: table_catalog.entries().map(|entry| entry.to_proto()).collect(), secondary_databases: databases,
            boundary_replica_msg_id: Some(boundary.metadata.position.0), external_source_token: message.external_source_token.clone(),
            term_id: Some(boundary.envelope.term.0), boundary_term_seq: boundary.envelope.term_sequence.map(|sequence| sequence.0),
        })?;
        self.store.put(&key, Bytes::from(catalog.encode())).await?;
        Ok(ProducedBlock { catalog, tries })
    }
    fn reserve_decode(&self, bytes: usize) -> Result<Arc<MemoryReservation>> {
        self.memory.reserve(bytes.checked_mul(16).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| invalid("Catalog allocation size overflow"))?)
    }
}

fn write_node<'a>(node: &'a TrieNode, live: &'a LiveTable, memory: &'a Arc<MemoryBudget>, temporary: &'a tempfile::NamedTempFile, writer: &'a mut Option<ArrowFileWriter>, metadata: &'a mut MetadataBuilder, retained: &'a mut Vec<Arc<MemoryReservation>>, cancellation: &'a CancellationToken) -> BoxFuture<'a, Result<usize>> {
    Box::pin(async move {
    check_cancelled(cancellation)?;
    match node {
        TrieNode::Branch { children, .. } => {
            let mut offsets = [None; 4];
            for (slot, child) in offsets.iter_mut().zip(children) {
                if let Some(child) = child { *slot = Some(write_node(child, live, memory, temporary, writer, metadata, retained, cancellation).await?); }
            }
            metadata.write_iid_branch(offsets)
        }
        TrieNode::Leaf(leaf) => {
            let rows = leaf.sorted_rows(live)?;
            let mut bound = rows.len().checked_mul(256).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("Page row allocation overflow"))?;
            for row in rows.iter() {
                let source = live.row(*row)?;
                let schema = source.batch.arrow().schema();
                let index = schema.index_of("op").map_err(io_error)?;
                bound = bound.checked_add(estimate_read_value_bytes(schema.field(index), source.batch.arrow().column(index).as_ref(), source.row)?).ok_or_else(|| invalid("Page allocation size overflow"))?;
            }
            let scratch = memory.reserve(bound)?;
            let events = rows.iter().map(|row| live.row(*row)?.event()).collect::<Result<Vec<_>>>()?;
            let batch = IndexBatch::copy_events_with_put_schema(&events, live.column_types(), memory)?;
            if writer.is_none() { *writer = Some(ArrowFileWriter::new_reserved(temporary.reopen().map_err(io_error)?, batch.arrow().schema().as_ref(), memory)?); }
            writer.as_mut().ok_or_else(|| invalid("Missing page writer"))?.write(batch.arrow())?;
            let metadata_charge = memory.reserve(rtdb_format::metadata::page_metadata_reservation(batch.arrow())?)?;
            let node = metadata.write_leaf(batch.arrow())?;
            retained.push(metadata_charge);
            drop(batch);
            drop(events);
            drop(scratch);
            tokio::task::yield_now().await;
            Ok(node)
        }
    }
    })
}

pub(crate) fn l0_tries(index: BlockIndex, tables: &BTreeMap<rtdb_types::TableRef, TableBlock>) -> Result<BTreeMap<rtdb_types::TableRef, Vec<CatalogTrie>>> {
    let key = TrieKey::l0(index)?;
    let mut result = BTreeMap::new();
    for (table, block) in tables {
        for detail in block.as_proto().partitions.iter().flat_map(|partition| &partition.tries).chain(&block.as_proto().tries) {
            let trie = CatalogTrie::from_proto(detail)?;
            if trie.key == key { result.entry(table.clone()).or_insert_with(Vec::new).push(trie); }
        }
    }
    Ok(result)
}

pub(crate) fn validate_boundary(catalog: &BlockCatalog, prepared: &BlockPreparation, boundary: &ReplicaRecord) -> Result<()> {
    let ReplicaBody::BlockBoundary(message) = &boundary.envelope.message else { return Err(invalid("Block validation requires a boundary")); };
    if catalog.block_index() != prepared.block_index() || catalog.latest_completed_tx() != prepared.value().live().latest_completed_tx()
        || catalog.boundary_replica_position() != Some(boundary.metadata.position)
        || catalog.boundary_term() != boundary.envelope.term || catalog.boundary_term_sequence() != boundary.envelope.term_sequence
        || catalog.as_proto().latest_processed_msg_id != message.latest_processed_msg_id
        || catalog.external_source_token() != message.external_source_token.as_deref() {
        return Err(invalid("Published block does not describe the retained boundary"));
    }
    Ok(())
}
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::fault("rtdb.replication/invalid-block", message) }
fn io_error(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.replication/block-io", error.to_string()) }
fn check_cancelled(cancellation: &CancellationToken) -> Result<()> { if cancellation.is_cancelled() { Err(crate::progress::cancelled()) } else { Ok(()) } }
