use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{block::proto as block_pb, catalog::{BlockCatalog, TableBlock, legacy_table_ref}, database::proto::DatabaseConfig, envelope::ReplicaBody, ipc::ArrowFileWriter, keys::{block_index_from_key, block_key, TrieKey}, log::proto, metadata::MetadataBuilder};
use rtdb_index::{BlockPages, BlockPreparation, CatalogTrie, PartitionState, PartitionValue, TableCatalog, TrieCatalog};
use rtdb_storage::{ObjectKey, ObjectStore, cache::BufferPool};
use rtdb_trie::{TrieConfig, TrieNode};
use rtdb_types::{Anomaly, BlockIndex, ReplicaPosition, Result, SourcePosition, StorageEpoch, StorageVersion, Timestamp};
use tokio_util::sync::CancellationToken;

use crate::{AppliedProgress, ReplicaRecord, TermFence};

pub struct RecoveredPartition {
    pub partition: Arc<PartitionState>,
    pub block: Option<BlockCatalog>,
    pub progress: AppliedProgress,
    pub fence: TermFence,
    table_blocks: BTreeMap<rtdb_types::TableRef, TableBlock>,
}
impl RecoveredPartition {
    /// Selects initial source progress for the configured log epoch while retaining persisted identities.
    pub fn with_source_floor(mut self, source_floor: SourcePosition) -> Self {
        self.progress.source_position = self.progress.source_position.max(source_floor);
        self
    }

    pub(crate) fn confirmation(&self, replica_epoch: StorageEpoch, storage_version: StorageVersion, storage_epoch: StorageEpoch, memory: &Arc<MemoryBudget>) -> Result<Option<RecoveredBlockConfirmation>> {
        let Some(catalog) = &self.block else { return Ok(None); };
        let Some(boundary) = catalog.boundary_replica_position() else { return Ok(None); };
        if boundary.after_offset(replica_epoch) < 0 { return Ok(None); }

        RecoveredBlockConfirmation::new(catalog.clone(), &self.table_blocks, storage_version, storage_epoch, memory).map(Some)
    }
}

pub(crate) struct RecoveredBlockConfirmation {
    pub catalog: BlockCatalog,
    pub body: ReplicaBody,
    _reservation: Arc<MemoryReservation>,
}

impl RecoveredBlockConfirmation {
    fn new(catalog: BlockCatalog, table_blocks: &BTreeMap<rtdb_types::TableRef, TableBlock>, storage_version: StorageVersion, storage_epoch: StorageEpoch, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let key = TrieKey::l0(catalog.block_index())?.to_string();
        let token_bytes = catalog.external_source_token().map_or(0, <[u8]>::len);
        let bytes = table_blocks.iter().try_fold(token_bytes, |bytes, (table, block)| {
            let count = l0_details(block, &key).count();
            let detail_bytes = table.schema.len().checked_add(table.table.len()).and_then(|len| len.checked_add(key.len()))
                .and_then(|len| len.checked_add(1024)).and_then(|len| len.checked_mul(count));
            detail_bytes.and_then(|detail_bytes| bytes.checked_add(detail_bytes))
        }).and_then(|bytes| bytes.checked_mul(4)).and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(|| invalid("Recovered block notification exceeds addressable memory"))?;
        let reservation = memory.reserve(bytes)?;
        let tries = l0_tries(catalog.block_index(), table_blocks)?;
        let upload = proto::BlockUploaded {
            storage_version: Some(storage_version.0), storage_epoch: Some(storage_epoch.0), block_index: Some(catalog.block_index().0),
            latest_processed_msg_id: catalog.as_proto().latest_processed_msg_id,
            external_source_token: catalog.external_source_token().map(<[u8]>::to_vec),
            tries: tries.iter().flat_map(|(table, tries)| tries.iter().map(|trie| trie.to_proto(table))).collect(),
        };
        Ok(Self { catalog, body: ReplicaBody::BlockUploaded(upload), _reservation: reservation })
    }

    pub fn matches(&self, record: &ReplicaRecord) -> bool {
        let (ReplicaBody::BlockUploaded(expected), ReplicaBody::BlockUploaded(observed)) = (&self.body, &record.envelope.message) else { return false; };
        self.catalog.boundary_replica_position().is_some_and(|boundary| record.metadata.position > boundary)
            && matches_confirmation(&self.catalog, observed, StorageVersion(expected.storage_version.unwrap_or_default()), StorageEpoch(expected.storage_epoch.unwrap_or_default()))
    }
}

pub(crate) struct HistoricalCatalog {
    pub catalog: BlockCatalog,
    pub boundary: ReplicaPosition,
    pub confirmed: bool,
    _reservation: Arc<MemoryReservation>,
}

pub(crate) fn matches_confirmation(catalog: &BlockCatalog, upload: &proto::BlockUploaded, storage_version: StorageVersion, storage_epoch: StorageEpoch) -> bool {
    upload.storage_version == Some(storage_version.0)
        && upload.storage_epoch.unwrap_or_default() == storage_epoch.0
        && upload.block_index == Some(catalog.block_index().0)
        && upload.latest_processed_msg_id == catalog.as_proto().latest_processed_msg_id
        && upload.external_source_token.as_deref() == catalog.external_source_token()
}

pub(crate) fn matches_catalog_boundary(catalog: &BlockCatalog, record: &ReplicaRecord) -> bool {
    let ReplicaBody::BlockBoundary(boundary) = &record.envelope.message else { return false; };
    catalog.boundary_replica_position() == Some(record.metadata.position)
        && catalog.boundary_term() == record.envelope.term
        && catalog.boundary_term_sequence() == record.envelope.term_sequence
        && boundary.block_index == Some(catalog.block_index().0)
        && boundary.latest_processed_msg_id == catalog.as_proto().latest_processed_msg_id
        && boundary.external_source_token.as_deref() == catalog.external_source_token()
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
        let (block, table_blocks) = match self.load_latest_checkpoint().await? {
            Some((catalog, tables)) => (Some(catalog), tables),
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
        Ok(RecoveredPartition { partition, block, progress, fence, table_blocks }.with_source_floor(source_floor))
    }
    async fn latest_checkpoint(&self, directory: &ObjectKey) -> Result<Option<BlockIndex>> {
        Ok(self.store.list(Some(directory)).await?.into_iter()
            .filter_map(|entry| block_index_from_key(entry.key.as_str()).ok())
            .max())
    }

    async fn load_latest_checkpoint(&self) -> Result<Option<(BlockCatalog, BTreeMap<rtdb_types::TableRef, TableBlock>)>> {
        let directory = ObjectKey::new("blocks")?;
        let Some(mut index) = self.latest_checkpoint(&directory).await? else { return Ok(None); };
        loop {
            match self.load(index).await {
                Err(error) if error.code == "rtdb.storage/missing-object" => {
                    // Collection can overtake a listing or a read of one of the table blocks.
                    let listed = self.latest_checkpoint(&directory).await?.map(|index| index.0);
                    let Some(next) = crate::checkpoint_discovery::newer_checkpoint(index.0, listed) else {
                        return Err(error);
                    };
                    index = BlockIndex(next);
                }
                result => return result.map(Some),
            }
        }
    }

    async fn load_catalog(&self, index: BlockIndex) -> Result<BlockCatalog> {
        let lease = self.cache.get(&ObjectKey::new(block_key(index)?)?).await?;
        let reservation = self.reserve_decode(lease.as_slice().len())?;
        let block = BlockCatalog::decode_reserved(lease.as_slice(), reservation)?;
        if block.block_index() != index { return Err(invalid("Block filename and payload index differ")); }
        Ok(block)
    }

    pub(crate) async fn retained_catalogs(&self, selected: &BlockCatalog, replica_epoch: StorageEpoch, cancellation: &CancellationToken) -> Result<Vec<HistoricalCatalog>> {
        let Some(mut next_boundary) = selected.boundary_replica_position() else { return Ok(Vec::new()); };
        let mut retained = Vec::new();
        for index in (0..selected.block_index().0).rev().map(BlockIndex) {
            check_cancelled(cancellation)?;
            let key = ObjectKey::new(block_key(index)?)?;
            match self.store.head_fresh(&key).await {
                Ok(_) => (),
                Err(rtdb_storage::Error::MissingObject(_)) => break,
                Err(error) => return Err(error.into()),
            }
            let catalog = self.load_catalog(index).await?;
            let Some(boundary) = catalog.boundary_replica_position().filter(|boundary| {
                boundary.after_offset(replica_epoch) >= 0 && *boundary < next_boundary
            }) else { break; };
            let reservation = self.memory.reserve(1024)?;
            retained.push(HistoricalCatalog { catalog, boundary, confirmed: false, _reservation: reservation });
            next_boundary = boundary;
        }
        retained.reverse();
        Ok(retained)
    }

    pub(crate) async fn historical_confirmation(&self, expected: &BlockCatalog, storage_version: StorageVersion, storage_epoch: StorageEpoch, cancellation: &CancellationToken) -> Result<RecoveredBlockConfirmation> {
        let (catalog, tables) = self.load(expected.block_index()).await?;
        if catalog.as_proto() != expected.as_proto() { return Err(invalid("Historical block differs from its observed boundary checkpoint")); }
        let confirmation = RecoveredBlockConfirmation::new(catalog, &tables, storage_version, storage_epoch, &self.memory)?;
        self.require_confirmation_files(&confirmation, cancellation).await?;
        Ok(confirmation)
    }

    pub(crate) async fn require_confirmation_files(&self, confirmation: &RecoveredBlockConfirmation, cancellation: &CancellationToken) -> Result<()> {
        let index = confirmation.catalog.block_index();
        let key = TrieKey::l0(index)?;
        let ReplicaBody::BlockUploaded(upload) = &confirmation.body else { return Err(invalid("Recovered confirmation has no upload body")); };
        let written = upload.tries.iter().map(|detail| legacy_table_ref(detail.table_name.as_deref().unwrap_or_default())).collect::<Result<BTreeSet<_>>>()?;
        check_cancelled(cancellation)?;
        self.store.head_fresh(&ObjectKey::new(block_key(index)?)?).await?;
        for entry in confirmation.catalog.table_entries()? {
            check_cancelled(cancellation)?;
            self.store.head_fresh(&ObjectKey::new(entry.slug.block_key(index)?)?).await?;
            if written.contains(&entry.table) {
                self.store.head_fresh(&ObjectKey::new(entry.slug.data_key(&key))?).await?;
                self.store.head_fresh(&ObjectKey::new(entry.slug.metadata_key(&key))?).await?;
            }
        }
        Ok(())
    }

    pub async fn load(&self, index: BlockIndex) -> Result<(BlockCatalog, BTreeMap<rtdb_types::TableRef, TableBlock>)> {
        let block = self.load_catalog(index).await?;
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
            let mut metadata = MetadataBuilder::new(false, false);
            let mut reservations = Vec::new();
            let temporary = tempfile::NamedTempFile::new().map_err(io_error)?;
            let mut writer = None;
            let pages = live.block_pages(&self.memory)?;
            write_node(live.trie().root(), &pages, &self.memory, &temporary, &mut writer, &mut metadata, &mut reservations, cancellation).await?;
            let file = writer.ok_or_else(|| invalid("Nonempty live table produced no pages"))?.finish()?;
            rtdb_storage::sync_file(&file).map_err(io_error)?;
            let size = i64::try_from(file.metadata().map_err(io_error)?.len()).map_err(io_error)?;
            self.store.put_file(&ObjectKey::new(entry.slug.data_key(&trie))?, temporary.path()).await?;
            check_cancelled(cancellation)?;
            let _metadata_serialization = self.memory.reserve(metadata.serialization_bound()?)?;
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

fn write_node<'a>(node: &'a TrieNode, pages: &'a BlockPages<'a>, memory: &'a Arc<MemoryBudget>, temporary: &'a tempfile::NamedTempFile, writer: &'a mut Option<ArrowFileWriter>, metadata: &'a mut MetadataBuilder, retained: &'a mut Vec<Arc<MemoryReservation>>, cancellation: &'a CancellationToken) -> BoxFuture<'a, Result<usize>> {
    Box::pin(async move {
    check_cancelled(cancellation)?;
    match node {
        TrieNode::Branch { children, .. } => {
            let mut offsets = [None; 4];
            for (slot, child) in offsets.iter_mut().zip(children) {
                if let Some(child) = child { *slot = Some(write_node(child, pages, memory, temporary, writer, metadata, retained, cancellation).await?); }
            }
            metadata.write_iid_branch(offsets)
        }
        TrieNode::Leaf(leaf) => {
            let rows = leaf.sorted_rows(pages.live_table())?;
            let batch = pages.copy(&rows)?;
            if writer.is_none() { *writer = Some(ArrowFileWriter::new_reserved(temporary.reopen().map_err(io_error)?, batch.batch().schema().as_ref(), memory)?); }
            writer.as_mut().ok_or_else(|| invalid("Missing page writer"))?.write(batch.batch())?;
            let (node, metadata_charge) = metadata.write_leaf_reserved(batch.batch(), memory)?;
            retained.push(metadata_charge);
            drop(batch);
            tokio::task::yield_now().await;
            Ok(node)
        }
    }
    })
}

fn block_tries(block: &TableBlock) -> impl Iterator<Item = &proto::TrieDetails> {
    let wire = block.as_proto();
    wire.partitions.iter().flat_map(|partition| &partition.tries)
        .chain(wire.tries.iter().filter(|_| wire.partitions.is_empty()))
}

fn l0_details<'a>(block: &'a TableBlock, key: &'a str) -> impl Iterator<Item = &'a proto::TrieDetails> {
    block_tries(block).filter(move |detail| detail.trie_key.as_deref().is_some_and(|recorded| recorded.eq_ignore_ascii_case(key)))
}

pub(crate) fn l0_tries(index: BlockIndex, tables: &BTreeMap<rtdb_types::TableRef, TableBlock>) -> Result<BTreeMap<rtdb_types::TableRef, Vec<CatalogTrie>>> {
    let key = TrieKey::l0(index)?;
    let wire_key = key.to_string();
    let mut result = BTreeMap::new();
    for (table, block) in tables {
        let mut notification = None;
        for detail in l0_details(block, &wire_key) {
            let recorded: TrieKey = detail.trie_key.as_deref().unwrap_or_default().parse()?;
            if recorded != key { return Err(invalid("Recovered L0 key does not describe this block")); }
            let summary = detail.trie_metadata.as_ref().map(|summary| proto::TrieMetadata {
                temporal_metadata: summary.temporal_metadata.clone(), row_count: summary.row_count, iid_bloom: None,
            });
            let trie = CatalogTrie::notification(key.clone(), detail.data_file_size.unwrap_or_default(), summary)?;
            if notification.as_ref().is_some_and(|existing| existing != &trie) {
                return Err(invalid("Conflicting L0 notifications for one table and block"));
            }
            notification = Some(trie);
        }
        if let Some(notification) = notification { result.insert(table.clone(), vec![notification]); }
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
