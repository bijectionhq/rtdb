use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{block::proto as block_pb, catalog::{BlockCatalog, TableBlock, legacy_table_ref}, database::proto::DatabaseConfig, envelope::ReplicaBody, ipc::ArrowFileWriter, keys::{block_index_from_key, block_key, TrieKey}, log::proto, metadata::MetadataBuilder};
use rtdb_index::{BlockPages, BlockPreparation, CatalogTrie, PartitionState, PartitionValue, TableCatalog, TrieCatalog};
use rtdb_storage::{ObjectKey, ObjectStore, cache::BufferPool, log::LogTail};
use rtdb_trie::{TrieConfig, TrieNode};
use rtdb_types::{Anomaly, BlockIndex, ReplicaPosition, Result, SourcePosition, StorageEpoch, StorageVersion, Timestamp};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{AppliedProgress, PartitionLogs, ReplicaRecord, TermFence};

pub struct RecoveredPartition {
    pub partition: Arc<PartitionState>,
    pub block: Option<BlockCatalog>,
    pub progress: AppliedProgress,
    pub fence: TermFence,
    pub(crate) compaction_replay_cutoff: Option<ReplicaPosition>,
    refresh_allowed: bool,
    published_checkpoint: Option<PublishedCheckpoint>,
    replay_tail: Option<PreparedReplay>,
    table_blocks: BTreeMap<rtdb_types::TableRef, TableBlock>,
}
struct PreparedReplay {
    logs: Arc<PartitionLogs>,
    after: ReplicaPosition,
    tail: Box<dyn LogTail>,
}

impl RecoveredPartition {
    /// Applies an authenticated inline resolved transaction to this private
    /// recovered partition without running live database or storage effects.
    pub fn apply_archived_transaction(
        &mut self, message: &proto::ResolvedTx, memory: &Arc<MemoryBudget>,
    ) -> Result<()> {
        let source = message.src_msg_id.map(SourcePosition).ok_or_else(|| invalid("Archived transaction has no source position"))?;
        if message.db_op.is_some() || message.external_source_token.is_some() || source <= self.progress.source_position {
            return Err(invalid("Archived transaction has unsupported effects or source order"));
        }
        let value = self.partition.value();
        let latest = value.live().latest_completed_tx();
        let rtdb_tx::ReplayedTransaction { writes, result, .. } =
            rtdb_tx::decode_replica(message, latest, value.live().block_index(), memory)?;
        if latest.is_some_and(|previous| result.tx_key().tx_id <= previous.tx_id
            || result.tx_key().system_time <= previous.system_time)
        {
            return Err(invalid("Archived transaction regressed its basis"));
        }
        self.partition.apply(&writes)?;
        self.progress.source_position = source;
        self.progress.tx_key = Some(result.tx_key());
        self.progress.latest_result = None;
        Ok(())
    }

    pub(crate) fn take_replay_tail(&mut self, logs: &Arc<PartitionLogs>, after: ReplicaPosition) -> Result<Option<Box<dyn LogTail>>> {
        let Some(prepared) = self.replay_tail.take() else { return Ok(None); };
        if !Arc::ptr_eq(&prepared.logs, logs) || prepared.after != after {
            return Err(invalid("The prepared replica cursor belongs to a different log or checkpoint boundary"));
        }
        Ok(Some(prepared.tail))
    }

    /// Selects initial source progress for the configured log epoch while retaining persisted identities.
    pub fn with_source_floor(mut self, source_floor: SourcePosition) -> Self {
        self.progress.source_position = self.progress.source_position.max(source_floor);
        self
    }

    pub(crate) fn replica_start(&self, logs: &PartitionLogs) -> Result<ReplicaPosition> {
        Ok(self.block.as_ref().and_then(BlockCatalog::boundary_replica_position).unwrap_or(logs.replica_floor()?))
    }

    pub(crate) fn allows_catalog_cut_tracking(&self) -> bool { self.refresh_allowed }

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
            let names = table.schema.len().checked_add(table.table.len())?.checked_add(1024)?;
            let bytes = block.as_proto().search_runs.iter().filter(|run| run.blocks == [catalog.block_index().0])
                .try_fold(bytes, |bytes, run| bytes.checked_add(run.encoded_size())?.checked_add(1024))?;
            l0_details(block, &key).try_fold(bytes, |bytes, detail| {
                bytes.checked_add(names)?.checked_add(detail.encoded_size())
            })
        }).and_then(|bytes| bytes.checked_mul(4)).and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(|| invalid("Recovered block notification exceeds addressable memory"))?;
        let reservation = memory.reserve(bytes)?;
        let tries = l0_tries(catalog.block_index(), table_blocks)?;
        let upload = proto::BlockUploaded {
            storage_version: Some(storage_version.0), storage_epoch: Some(storage_epoch.0), block_index: Some(catalog.block_index().0),
            latest_processed_msg_id: catalog.as_proto().latest_processed_msg_id,
            external_source_token: catalog.external_source_token().map(<[u8]>::to_vec),
            catalog_sha256: Some(checkpoint_digest(&catalog)?.to_vec()),
            tries: tries.iter().flat_map(|(table, tries)| tries.iter().map(|trie| trie.to_proto(table))).collect(),
        };
        Ok(Self { catalog, body: ReplicaBody::BlockUploaded(upload), _reservation: reservation })
    }

    pub(crate) fn matches(&self, record: &ReplicaRecord, required: bool) -> Result<bool> {
        let (ReplicaBody::BlockUploaded(expected), ReplicaBody::BlockUploaded(observed)) = (&self.body, &record.envelope.message) else { return Ok(false); };
        let follows_boundary = self.catalog.boundary_replica_position().is_some_and(|boundary| record.metadata.position > boundary);
        if !follows_boundary || !matches_confirmation(&self.catalog, observed,
            StorageVersion(expected.storage_version.unwrap_or_default()), StorageEpoch(expected.storage_epoch.unwrap_or_default())) {
            return Ok(false);
        }
        validate_uploaded_root(&self.catalog, observed, required)?;
        Ok(true)
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

pub(crate) fn checkpoint_digest(catalog: &BlockCatalog) -> Result<[u8; 32]> {
    catalog.publication_sha256().ok_or_else(|| invalid("The block catalog has no exact published encoding"))
}

pub(crate) fn validate_uploaded_root(catalog: &BlockCatalog, upload: &proto::BlockUploaded, required: bool) -> Result<()> {
    match upload.catalog_sha256.as_deref() {
        Some(chosen) if chosen == checkpoint_digest(catalog)?.as_slice() => Ok(()),
        None if !required => Ok(()),
        _ => Err(Anomaly::fault("rtdb.replication/checkpoint-digest", "The accepted upload does not certify the exact stored catalog")),
    }
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

/// The block and replica position admitted together by the serving directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublishedCheckpoint {
    pub block: BlockIndex,
    pub boundary: ReplicaPosition,
    pub root_sha256: Option<[u8; 32]>,
}

impl PublishedCheckpoint {
    fn accepts(self, block: &BlockCatalog) -> bool {
        let loaded = block.boundary_replica_position()
            .map(|position| position.after_offset(self.boundary.epoch()))
            .filter(|offset| *offset >= 0);
        crate::checkpoint_discovery::accepts_published_checkpoint(
            self.block.0, self.boundary.after_offset(self.boundary.epoch()), block.block_index().0, loaded,
        )
    }
}

/// How recovery finds a partition's latest block.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BlockDiscovery {
    /// Lists the block catalogs.
    #[default]
    List,
    /// Probes forward from a known block, one head per block.
    /// Listing handles a missing hint or refreshes discovery after a missing metadata read.
    From(BlockIndex),
    /// A published and admitted lower bound in one replica journal.
    FromPublished(PublishedCheckpoint),
    /// A restored serving generation requires its archived checkpoint and suppresses
    /// physical compaction effects from the sealed history preceding restoration.
    FromRestored { checkpoint: BlockIndex, compaction_cutoff: ReplicaPosition, root_sha256: Option<[u8; 32]> },
    /// Recovers exactly this block, for a reader of the state as of it.
    At(BlockIndex),
    /// Recovers exactly the block and journal boundary admitted by the directory.
    AtPublished(PublishedCheckpoint),
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
    pub async fn recover(&self, config: TrieConfig, source_floor: SourcePosition, now: Timestamp, discovery: BlockDiscovery) -> Result<RecoveredPartition> {
        let published = match discovery {
            BlockDiscovery::FromPublished(checkpoint) | BlockDiscovery::AtPublished(checkpoint) => Some(checkpoint),
            _ => None,
        };
        let selected = self.load_selected_checkpoint(discovery, None, published).await?;
        self.restore(config, source_floor, now, discovery, published, selected)
    }

    /// Admits an ordinary latest checkpoint with the same cursor the driver will consume.
    /// An exact or restored checkpoint specifies its own recovery anchor.
    pub async fn recover_for_replay(&self, mut recovered: RecoveredPartition, logs: &Arc<PartitionLogs>,
        config: TrieConfig, now: Timestamp, cancellation: &CancellationToken) -> Result<RecoveredPartition> {
        if recovered.published_checkpoint.is_some_and(|checkpoint| checkpoint.boundary.epoch() != logs.replica_epoch) {
            return Err(invalid("The admitted checkpoint belongs to a different replica journal than the opened log"));
        }
        let source_floor = logs.source_floor()?;
        if !recovered.refresh_allowed { return Ok(recovered.with_source_floor(source_floor)); }
        let work = async {
            loop {
                let after = recovered.replica_start(logs)?;
                let gap = match logs.recovery_tail(after).await {
                    Ok(tail) => {
                        recovered.replay_tail = tail.map(|tail| PreparedReplay { logs: Arc::clone(logs), after, tail });
                        return Ok(recovered.with_source_floor(source_floor));
                    }
                    Err(error) if logs.is_recovery_prefix_gap(after, &error)
                        && recovered.published_checkpoint.is_none_or(|checkpoint| checkpoint.root_sha256.is_none()) => error,
                    Err(error) => return Err(error.into()),
                };
                let previous = recovered.block.as_ref().map(BlockCatalog::block_index);
                let published = recovered.published_checkpoint;
                drop(recovered);
                let Some(replacement) = self.load_selected_checkpoint(BlockDiscovery::List, previous, published).await? else {
                    return Err(gap.into());
                };
                recovered = self.restore(config, source_floor, now, BlockDiscovery::List, published, Some(replacement))?;
            }
        };
        cancellation.run_until_cancelled(work).await.ok_or_else(crate::progress::cancelled)?
    }

    fn restore(&self, config: TrieConfig, source_floor: SourcePosition, now: Timestamp, discovery: BlockDiscovery,
        published: Option<PublishedCheckpoint>,
        selected: Option<(BlockCatalog, BTreeMap<rtdb_types::TableRef, TableBlock>)>) -> Result<RecoveredPartition> {
        let compaction_replay_cutoff = match discovery {
            BlockDiscovery::FromRestored { compaction_cutoff, .. } => Some(compaction_cutoff),
            _ => None,
        };
        let (block, table_blocks) = match selected {
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
        let refresh_allowed = matches!(discovery, BlockDiscovery::List | BlockDiscovery::From(_) | BlockDiscovery::FromPublished(_));
        let published_checkpoint = published.zip(block.as_ref()).and_then(|(admitted, block)| {
            block.boundary_replica_position().map(|boundary| PublishedCheckpoint {
                block: block.block_index(), boundary, root_sha256: admitted.root_sha256,
            })
        });
        Ok(RecoveredPartition { partition, block, progress, fence, table_blocks, compaction_replay_cutoff, refresh_allowed, published_checkpoint, replay_tail: None }.with_source_floor(source_floor))
    }
    async fn load_selected_checkpoint(&self, discovery: BlockDiscovery, previous: Option<BlockIndex>,
        published: Option<PublishedCheckpoint>) -> Result<Option<(BlockCatalog, BTreeMap<rtdb_types::TableRef, TableBlock>)>> {
        let latest = self.latest_block(discovery).await?;
        let selected = match previous {
            Some(previous) => crate::checkpoint_discovery::newer_checkpoint(previous.0, latest.map(|index| index.0)).map(BlockIndex),
            None => latest,
        };
        let Some(mut index) = selected else { return Ok(None); };
        let root_sha256 = match discovery {
            BlockDiscovery::FromRestored { root_sha256, .. } => root_sha256,
            _ => published.and_then(|checkpoint| checkpoint.root_sha256),
        };
        let refresh = root_sha256.is_none() && matches!(discovery, BlockDiscovery::List | BlockDiscovery::From(_) | BlockDiscovery::FromPublished(_));
        loop {
            match self.load_checkpoint(index, published, root_sha256).await {
                Err(error) if refresh && error.code == "rtdb.storage/missing-object" => {
                    let listed = self.latest_block(BlockDiscovery::List).await?.map(|index| index.0);
                    let Some(next) = crate::checkpoint_discovery::newer_checkpoint(index.0, listed) else {
                        return Err(error);
                    };
                    index = BlockIndex(next);
                }
                result => return result.map(Some),
            }
        }
    }

    async fn latest_block(&self, discovery: BlockDiscovery) -> Result<Option<BlockIndex>> {
        let exists = |index: BlockIndex| async move {
            match self.store.head_fresh(&ObjectKey::new(block_key(index)?)?).await {
                Ok(_) => Ok(true),
                Err(rtdb_storage::Error::MissingObject(_)) => Ok(false),
                Err(error) => Err(rtdb_types::Anomaly::from(error)),
            }
        };
        let discovery = match discovery {
            BlockDiscovery::AtPublished(published) => BlockDiscovery::At(published.block),
            BlockDiscovery::FromPublished(published) if published.root_sha256.is_some() => BlockDiscovery::At(published.block),
            BlockDiscovery::FromRestored { checkpoint, root_sha256: Some(_), .. } => BlockDiscovery::At(checkpoint),
            other => other,
        };
        let mut latest = match discovery {
            BlockDiscovery::At(block) if exists(block).await? => return Ok(Some(block)),
            BlockDiscovery::At(block) => return Err(rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.replication/block-gone", format!("Block {} is no longer stored", block.0))),
            BlockDiscovery::From(hint) if exists(hint).await? => hint,
            BlockDiscovery::FromPublished(checkpoint) => {
                if !exists(checkpoint.block).await? {
                    return self.latest_listed_block().await?.filter(|index| *index >= checkpoint.block)
                        .map(Some).ok_or_else(|| rtdb_types::Anomaly::new(
                            rtdb_types::AnomalyCategory::Unavailable, "rtdb.replication/block-gone",
                            format!("Published checkpoint {} and its successors could not be found", checkpoint.block.0),
                        ));
                }
                checkpoint.block
            }
            BlockDiscovery::FromRestored { checkpoint, .. } if exists(checkpoint).await? => checkpoint,
            BlockDiscovery::FromRestored { checkpoint, .. } => return Err(rtdb_types::Anomaly::new(
                rtdb_types::AnomalyCategory::Unavailable, "rtdb.replication/block-gone",
                format!("Restored checkpoint {} is no longer stored", checkpoint.0),
            )),
            _ => return self.latest_listed_block().await,
        };
        while let Some(next) = latest.0.checked_add(1).map(BlockIndex) {
            if !exists(next).await? { break; }
            latest = next;
        }
        Ok(Some(latest))
    }

    pub(crate) async fn latest_listed_block(&self) -> Result<Option<BlockIndex>> {
        let directory = ObjectKey::new("blocks")?;
        Ok(self.store.list(Some(&directory)).await?.into_iter().filter_map(|entry| block_index_from_key(entry.key.as_str()).ok()).max())
    }

    async fn load_catalog(&self, index: BlockIndex) -> Result<BlockCatalog> {
        let lease = self.cache.get(&ObjectKey::new(block_key(index)?)?).await?;
        self.decode_catalog(index, lease.as_slice())
    }

    pub(crate) async fn load_fresh_catalog(&self, index: BlockIndex) -> Result<BlockCatalog> {
        let key = ObjectKey::new(block_key(index)?)?;
        let size = usize::try_from(self.store.head_fresh(&key).await?.size)
            .map_err(|_| invalid("Checkpoint root exceeds addressable memory"))?;
        let read = self.memory.reserve(size)?;
        let bytes = rtdb_storage::IoRegion::retaining(read.clone(), self.store.get_fresh_exact(&key, size)).await?;
        self.decode_catalog(index, &bytes)
    }

    fn decode_catalog(&self, index: BlockIndex, bytes: &[u8]) -> Result<BlockCatalog> {
        let block = BlockCatalog::decode_reserved(bytes, self.reserve_decode(bytes.len())?)?;
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
        let stored = self.load_fresh_catalog(index).await?;
        validate_uploaded_root(&stored, upload, true)?;
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
        let block = self.load_fresh_catalog(index).await?;
        let tables = self.load_tables(&block).await?;
        Ok((block, tables))
    }

    async fn load_checkpoint(&self, index: BlockIndex, published: Option<PublishedCheckpoint>, root_sha256: Option<[u8; 32]>) -> Result<(BlockCatalog, BTreeMap<rtdb_types::TableRef, TableBlock>)> {
        let block = if root_sha256.is_some() { self.load_fresh_catalog(index).await? } else { self.load_catalog(index).await? };
        if root_sha256.is_some_and(|expected| block.publication_sha256() != Some(expected)) {
            return Err(invalid("The stored checkpoint root does not match the directory receipt"));
        }
        if published.is_some_and(|expected| !expected.accepts(&block)) {
            return Err(invalid("The stored checkpoint does not match or advance the admitted checkpoint in its replica journal"));
        }
        let tables = self.load_tables(&block).await?;
        Ok((block, tables))
    }

    async fn load_tables(&self, block: &BlockCatalog) -> Result<BTreeMap<rtdb_types::TableRef, TableBlock>> {
        let index = block.block_index();
        let mut tables = BTreeMap::new();
        for entry in block.table_entries()? {
            let lease = self.cache.get(&ObjectKey::new(entry.slug.block_key(index)?)?).await?;
            if !block.matches_table_block(entry.oid, &rtdb_format::catalog::publication_digest(lease.as_slice())) {
                return Err(invalid(format!("The stored table block for {} does not match its checkpoint manifest", entry.table.schema_and_table())));
            }
            let reservation = self.reserve_decode(lease.as_slice().len())?;
            tables.insert(entry.table, TableBlock::decode_reserved(lease.as_slice(), reservation)?);
        }
        Ok(tables)
    }
    pub async fn produce(&self, prepared: &BlockPreparation, boundary: &ReplicaRecord, databases: BTreeMap<String, DatabaseConfig>) -> Result<ProducedBlock> {
        self.produce_cancellable(prepared, boundary, databases, &CancellationToken::new()).await
    }
    /// Cancellation is observed between pages and completed storage operations; an upload already started is awaited.
    pub async fn produce_cancellable(&self, prepared: &BlockPreparation, boundary: &ReplicaRecord, databases: BTreeMap<String, DatabaseConfig>, cancellation: &CancellationToken) -> Result<ProducedBlock> {
        self.produce_with_search(prepared, boundary, databases, cancellation, None).await
    }

    pub(crate) async fn produce_with_search(&self, prepared: &BlockPreparation, boundary: &ReplicaRecord,
        databases: BTreeMap<String, DatabaseConfig>, cancellation: &CancellationToken,
        search: Option<&dyn crate::ServingSearchBuilder>) -> Result<ProducedBlock> {
        check_cancelled(cancellation)?;
        let ReplicaBody::BlockBoundary(message) = &boundary.envelope.message else { return Err(invalid("Block production requires a boundary")); };
        let index = BlockIndex(message.block_index.unwrap_or_default());
        if index != prepared.block_index() { return Err(invalid("Boundary does not match the current live block")); }
        let key = ObjectKey::new(block_key(index)?)?;
        let exists = match self.store.head_fresh(&key).await {
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
            let artefacts = match search {
                Some(search) => search.seal(prepared.value(), table, &trie, temporary.path(), size as u64, cancellation).await?,
                None => Vec::new(),
            };
            let mut notification = CatalogTrie::notification(trie, size, Some(summary))?;
            notification.artefacts = artefacts;
            tries.insert(table.clone(), vec![notification]);
            tokio::task::yield_now().await;
        }
        let table_blocks = prepared.table_blocks(&tries, boundary.metadata.timestamp)?;
        let mut sha256_by_oid = BTreeMap::new();
        for entry in table_catalog.entries() {
            check_cancelled(cancellation)?;
            let block = table_blocks.get(&entry.table).ok_or_else(|| invalid("Recorded table has no block metadata"))?;
            let bytes = Bytes::from(block.encode());
            sha256_by_oid.insert(entry.oid, Sha256::digest(&bytes).to_vec());
            self.store.put(&ObjectKey::new(entry.slug.block_key(index)?)?, bytes).await?;
        }
        let tx_key = prepared.value().live().latest_completed_tx();
        check_cancelled(cancellation)?;
        let mut catalog = BlockCatalog::from_proto(block_pb::Block {
            block_index: Some(index.0), latest_completed_tx: tx_key.map(|key| block_pb::TxKey { tx_id: Some(key.tx_id.0), system_time: Some(key.system_time.0) }),
            latest_processed_msg_id: message.latest_processed_msg_id,
            table_names: table_catalog.entries().map(|entry| entry.table.schema_and_table()).collect(),
            tables: table_catalog.entries().map(|entry| entry.to_proto()).collect(), secondary_databases: databases,
            boundary_replica_msg_id: Some(boundary.metadata.position.0), external_source_token: message.external_source_token.clone(),
            term_id: Some(boundary.envelope.term.0), boundary_term_seq: boundary.envelope.term_sequence.map(|sequence| sequence.0),
            naming_epoch: Some(i64::try_from(prepared.value().trie_catalog().naming_epoch()).map_err(|_| invalid("Naming epoch exceeds the block format"))?),
            table_block_manifest: Some(block_pb::TableBlockManifest { sha256_by_oid }),
        })?;
        self.store.put(&key, Bytes::from(catalog.encode_for_publication())).await?;
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

fn l0_details<'a>(block: &'a TableBlock, key: &'a str) -> impl Iterator<Item = &'a proto::TrieDetails> {
    block.trie_details().filter(move |detail| detail.trie_key.as_deref().is_some_and(|recorded| recorded.eq_ignore_ascii_case(key)))
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
            let mut trie = CatalogTrie::notification(key.clone(), detail.data_file_size.unwrap_or_default(), summary)?;
            trie.artefacts = detail.artefacts.clone();
            for run in block.as_proto().search_runs.iter().filter(|run| run.blocks == [index.0]) {
                trie.artefacts.push(proto::SearchRunChange { run: Some(run.clone()), ..Default::default() }.to_artefact()?);
            }
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
