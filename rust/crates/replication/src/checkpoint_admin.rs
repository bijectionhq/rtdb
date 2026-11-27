use std::{fs, io::Write, path::{Path, PathBuf}, sync::Arc};

use bytes::Bytes;
use fs2::FileExt;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{catalog::BlockCatalog, keys::{block_index_from_key, block_key}, log::proto};
use rtdb_storage::{HeldView, IoRegion, LocalStore, ObjectKey, ObjectStore, cache::BufferPool, object_store::ReadReservation};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, CatalogCheckpoint, CatalogCut, ReplicaPosition, Result, SourcePosition, StorageEpoch, StorageVersion, Term, Timestamp, rtdb_v1::Basis};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{BlockDiscovery, BlockStore, NativeServingAuthority, NativeServingHandle, PublishedCheckpoint, checkpoint_dependencies, serving::decode_checkpoint_image};

pub(crate) const PIN_FRAME: u8 = 7;
pub(crate) const RELEASE_FRAME: u8 = 8;
const MAX_RECEIPT: usize = 2 << 10;
const MAX_MANIFEST: usize = 4 << 20;
const COPY_PAGE: u64 = 256 << 10;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Selection {
    pin: Uuid,
    basis: Basis,
    until: Option<Timestamp>,
    block: BlockIndex,
    boundary: ReplicaPosition,
    source: SourcePosition,
    storage_epoch: StorageEpoch,
    storage_version: StorageVersion,
    root: [u8; 32],
}

impl Selection {
    fn published(&self) -> PublishedCheckpoint {
        PublishedCheckpoint { block: self.block, boundary: self.boundary, root_sha256: Some(self.root) }
    }

    fn namespace(&self) -> Result<String> {
        let commitment = hash(&encode(self)?);
        Ok(format!("db/{}/checkpoint-exports/{}/{}", Uuid::from_bytes(self.basis.incarnation()), self.pin, hex(&commitment)))
    }

    fn alive(&self, now: Timestamp) -> Result<()> {
        if self.until.is_some_and(|until| now >= until) {
            return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/checkpoint-pin-expired", "The checkpoint pin has expired"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PinControl {
    selection: Selection,
    manifest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PinReceipt {
    control: PinControl,
    replica: ReplicaPosition,
    term: Term,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReleaseControl(PinReceipt);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseReceipt { pin: PinReceipt, replica: ReplicaPosition, term: Term }

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchivedFile { key: String, length: u64, sha256: [u8; 32] }

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest { selection: Selection, files: Vec<ArchivedFile> }

/// An independently materialized checkpoint. Its basis and journal boundary belong to the source.
#[derive(Debug)]
pub struct DetachedDefaultCheckpoint {
    pub path: PathBuf,
    pub basis: Basis,
    pub checkpoint: PublishedCheckpoint,
    pub source: SourcePosition,
    receipt: PinReceipt,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalDefaultRestoreSeed {
    pub basis: Basis,
    pub original_basis: Basis,
    pub original_replica: ReplicaPosition,
    pub original_source: SourcePosition,
    pub original_root: [u8; 32],
    pub original_receipt: [u8; 32],
    pub block: BlockIndex,
    pub root: [u8; 32],
    pub log_epoch: StorageEpoch,
    pub source_floor: SourcePosition,
    pub replica_floor: ReplicaPosition,
}

impl LocalDefaultRestoreSeed {
    pub fn validate_original_receipt(&self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > MAX_RECEIPT || hash(bytes) != self.original_receipt {
            return Err(invalid("The Local install has a different original receipt"));
        }
        let receipt: PinReceipt = decode(bytes)?;
        let cut = &receipt.control.selection;
        if encode(&receipt)?.as_slice() != bytes || cut.basis != self.original_basis || cut.root != self.original_root
            || cut.boundary != self.original_replica || cut.source != self.original_source
            || cut.block != self.block || cut.pin.is_nil() || receipt.replica.epoch() != cut.boundary.epoch()
            || receipt.replica <= cut.boundary || receipt.term.0 <= 0
        { return Err(invalid("The Local install has different selected source coordinates")); }
        Ok(())
    }

    pub fn validate_cursor(&self, block: &BlockCatalog, source_epoch: StorageEpoch, replica_epoch: StorageEpoch) -> Result<()> {
        if self.log_epoch != source_epoch || self.log_epoch != replica_epoch
            || self.source_floor != SourcePosition::from_epoch_offset(source_epoch, -1)?
            || self.replica_floor != ReplicaPosition::from_epoch_offset(replica_epoch, -1)?
            || block.block_index() < self.block
        { return Err(invalid("The Local restore journal does not match its installed floor")); }
        if block.block_index() == self.block {
            if block.publication_sha256() != Some(self.root) || block.boundary_replica_position().is_some()
                || block.boundary_term() != Term(0) || block.latest_source_position() != Some(self.source_floor)
                || block.latest_completed_tx().is_some_and(|key| key.tx_id.0 >= self.source_floor.0)
            { return Err(invalid("The Local restore seed differs from the selected installation")); }
        } else if block.boundary_replica_position().is_none_or(|position| position.epoch() != replica_epoch || position <= self.replica_floor)
            || block.latest_source_position().is_none_or(|position| position < self.source_floor)
        { return Err(invalid("The Local restore checkpoint regressed outside its journal")); }
        Ok(())
    }

    pub fn validate_journal_heads(&self, block: &BlockCatalog, source_head: SourcePosition, replica_head: ReplicaPosition) -> Result<()> {
        if source_head < self.source_floor || replica_head < self.replica_floor
            || block.latest_source_position().is_none_or(|position| position > source_head)
            || block.boundary_replica_position().is_some_and(|position| position > replica_head)
        { return Err(invalid("The Local restore cursor is outside the durable journals")); }
        Ok(())
    }
}

impl DetachedDefaultCheckpoint {
    /// Installs a fresh-incarnation checkpoint in an empty, independently provisioned Local storage root.
    pub async fn seed_local(&self, destination: Arc<LocalStore>, incarnation: [u8; 16], memory: &Arc<MemoryBudget>,
        cancellation: &CancellationToken,
    ) -> Result<LocalDefaultRestoreSeed> {
        let selection = &self.receipt.control.selection;
        if selection.basis.incarnation() == incarnation || incarnation == [0; 16] { return Err(invalid("The restore destination needs a fresh incarnation")); }
        let source = Arc::new(LocalStore::open(&self.path)?);
        let store: Arc<dyn ObjectStore> = source.clone();
        let storage = Arc::new(BufferPool::new(Arc::clone(&store), 32 << 20));
        let blocks = BlockStore::new(store, storage, Arc::clone(memory), rtdb_index::TrieCatalog::default().file_size_target())?;
        let authority = NativeServingAuthority { incarnation: selection.basis.incarnation(), checkpoints: None };
        let (block, tables) = blocks.load(selection.block).await?;
        verify_plain_image(source.as_ref(), &authority, &block, selection).await?;
        let log_epoch = StorageEpoch(selection.boundary.epoch().0.max(selection.source.epoch().0).checked_add(1)
            .ok_or_else(|| invalid("The restored Local journal epoch is exhausted"))?);
        let source_floor = SourcePosition::from_epoch_offset(log_epoch, -1)?;
        let replica_floor = ReplicaPosition::from_epoch_offset(log_epoch, -1)?;
        if block.latest_completed_tx().is_some_and(|key| key.tx_id.0 >= source_floor.0) {
            return Err(invalid("The restored Local journal would reuse a source transaction coordinate"));
        }
        let root_key = ObjectKey::new(block_key(selection.block)?)?;
        let manifest_key = ObjectKey::new("checkpoint-restore-manifest")?;
        let (bytes, _charge) = read_small(source.as_ref(), &manifest_key, MAX_MANIFEST, memory).await?;
        if hash(&bytes) != self.receipt.control.manifest { return Err(invalid("The detached source manifest differs from its receipt")); }
        let manifest: Manifest = decode(&bytes)?;
        let expected = checkpoint_dependencies(&block, &tables)?;
        if &manifest.selection != selection || expected.iter().map(ObjectKey::as_str).ne(manifest.files.iter().map(|file| file.key.as_str())) {
            return Err(invalid("The detached source does not match the restore receipt"));
        }
        for file in &manifest.files {
            let key = ObjectKey::new(file.key.clone())?;
            if key != root_key { copy_verified(source.as_ref(), &key, destination.as_ref(), &key, Some(file), memory, cancellation).await?; }
        }
        let mut wire = block.as_proto().clone();
        let original = wire.serving_checkpoint.as_deref().ok_or_else(protected)?;
        wire.serving_checkpoint = Some(crate::serving::restore_plain_checkpoint_image(original, selection.basis.incarnation(), incarnation)?.to_vec());
        wire.latest_processed_msg_id = Some(source_floor.0);
        wire.boundary_replica_msg_id = None;
        wire.boundary_term_seq = None;
        wire.term_id = Some(0);
        wire.secondary_databases.clear();
        let mut root = BlockCatalog::from_proto(wire)?;
        let encoded = root.encode_for_publication();
        let seed = LocalDefaultRestoreSeed { basis: Basis::new(incarnation, selection.basis.position()), original_basis: selection.basis,
            original_replica: selection.boundary, original_source: selection.source, original_root: selection.root,
            original_receipt: hash(&encode(&self.receipt)?), block: selection.block, root: hash(&encoded), log_epoch, source_floor, replica_floor };
        destination.put(&root_key, encoded.into()).await?;
        let store: Arc<dyn ObjectStore> = destination.clone();
        let storage = Arc::new(BufferPool::new(Arc::clone(&store), 32 << 20));
        let blocks = BlockStore::new(store, storage, Arc::clone(memory), rtdb_index::TrieCatalog::default().file_size_target())?;
        let (reopened, tables) = blocks.load(seed.block).await?;
        seed.validate_cursor(&reopened, log_epoch, log_epoch)?;
        blocks.verify_primary_inventory(&reopened, &tables).await?;
        let (fold, _, shared) = decode_checkpoint_image(reopened.as_proto().serving_checkpoint.as_deref().ok_or_else(unavailable)?, incarnation)?;
        if !shared || fold.index().head().basis != seed.basis { return Err(invalid("The Local restore did not reopen its new incarnation")); }
        source.close_and_wait().await;
        Ok(seed)
    }
}

impl PinControl {
    pub(crate) fn encode_record(&self) -> Result<proto::NoOp> {
        let mut frame = vec![PIN_FRAME];
        frame.extend(encode(self)?);
        if frame.len() > MAX_RECEIPT { return Err(invalid("The checkpoint pin is too large")); }
        Ok(proto::NoOp { serving_record: Some(frame.into()), ..Default::default() })
    }

    pub(crate) fn decode(message: &proto::NoOp) -> Result<Option<Self>> {
        let Some(bytes) = message.serving_record.as_deref().and_then(|bytes| bytes.strip_prefix(&[PIN_FRAME])) else { return Ok(None); };
        if bytes.len() >= MAX_RECEIPT || message.src_msg_id.is_some() || message.placed_slot_reservation.is_some() {
            return Err(invalid("The checkpoint pin has an invalid replica frame"));
        }
        let control: Self = decode(bytes)?;
        if control.selection.pin.is_nil() || control.selection.boundary.0 < 0 || control.selection.block.0 < 0 {
            return Err(invalid("The checkpoint pin has invalid coordinates"));
        }
        Ok(Some(control))
    }

    pub(crate) fn check_predecessor(&self, incarnation: [u8; 16], logs: &crate::PartitionLogs, replica: ReplicaPosition, source: SourcePosition) -> Result<()> {
        let cut = &self.selection;
        if cut.basis.incarnation() != incarnation || cut.boundary.epoch() != replica.epoch() || cut.boundary > replica || cut.source > source
            || cut.storage_epoch != logs.storage_epoch || cut.storage_version != logs.storage_version
        {
            return Err(invalid("The checkpoint pin exceeds the installed database prefix"));
        }
        Ok(())
    }

    pub(crate) fn check_lifetime(&self, now: Timestamp) -> Result<()> { self.selection.alive(now) }

    pub(crate) async fn apply(self, store: &dyn ObjectStore, replica: ReplicaPosition, term: Term, memory: &Arc<MemoryBudget>) -> Result<()> {
        let cut = &self.selection;
        if replica <= cut.boundary { return Err(invalid("The checkpoint pin must follow the published boundary")); }
        if read_receipt(store, cut.basis.incarnation(), cut.pin).await?.is_some() { return Ok(()); }
        let (manifest, _charge) = read_small(store, &manifest_key(&cut.namespace()?)?, MAX_MANIFEST, memory).await?;
        if hash(&manifest) != self.manifest { return Err(invalid("The ordered pin has no exact durable manifest")); }
        let key = receipt_key(cut.basis.incarnation(), cut.pin)?;
        let receipt = PinReceipt { control: self, replica, term };
        store.put(&key, Bytes::from(encode(&receipt)?)).await?;
        Ok(())
    }
}

impl ReleaseControl {
    pub(crate) fn encode_record(&self) -> Result<proto::NoOp> {
        let mut frame = vec![RELEASE_FRAME];
        frame.extend(encode(self)?);
        if frame.len() > MAX_RECEIPT { return Err(invalid("The checkpoint release is too large")); }
        Ok(proto::NoOp { serving_record: Some(frame.into()), ..Default::default() })
    }

    pub(crate) fn decode(message: &proto::NoOp) -> Result<Option<Self>> {
        let Some(bytes) = message.serving_record.as_deref().and_then(|bytes| bytes.strip_prefix(&[RELEASE_FRAME])) else { return Ok(None); };
        if bytes.len() >= MAX_RECEIPT || message.src_msg_id.is_some() || message.placed_slot_reservation.is_some() {
            return Err(invalid("The checkpoint release has an invalid replica frame"));
        }
        Ok(Some(decode(bytes)?))
    }

    pub(crate) fn check_predecessor(&self, incarnation: [u8; 16], replica: ReplicaPosition) -> Result<()> {
        if self.0.control.selection.basis.incarnation() != incarnation || self.0.replica.epoch() != replica.epoch() || self.0.replica > replica {
            return Err(invalid("The checkpoint release does not name an installed pin"));
        }
        Ok(())
    }

    pub(crate) async fn apply(self, store: &dyn ObjectStore, replica: ReplicaPosition, term: Term) -> Result<()> {
        let selected = &self.0.control.selection;
        let pin = read_receipt(store, selected.basis.incarnation(), selected.pin).await?.ok_or_else(unavailable)?;
        if pin != self.0 || replica <= pin.replica || term < pin.term { return Err(invalid("The checkpoint release changed its ordered pin")); }
        if read_release(store, &pin).await?.is_some() { return Ok(()); }
        let key = release_key(selected.basis.incarnation(), selected.pin)?;
        store.put(&key, Bytes::from(encode(&ReleaseReceipt { pin, replica, term })?)).await?;
        Ok(())
    }
}

impl NativeServingHandle {
    /// Orders a retained export for the installed source. The caller owns deployment authorization.
    pub async fn pin_checkpoint_export(&self, pin: Uuid, basis: Basis, until: Option<Timestamp>, cancellation: &CancellationToken) -> Result<Vec<u8>> {
        self.pin_export(pin, basis, until, None, cancellation).await
    }

    /// Selects an older publication authenticated by the trusted catalog caller.
    pub async fn pin_retained_checkpoint_export(&self, pin: Uuid, basis: Basis, until: Option<Timestamp>,
        publication: PublishedCheckpoint, cancellation: &CancellationToken,
    ) -> Result<Vec<u8>> {
        self.pin_export(pin, basis, until, Some(publication), cancellation).await
    }

    async fn pin_export(&self, pin: Uuid, basis: Basis, until: Option<Timestamp>, requested: Option<PublishedCheckpoint>,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>> {
        let logs = self.replication.partition_logs();
        let store = logs.store().as_ref();
        if basis.incarnation() != self.authority.incarnation || pin.is_nil() { return Err(invalid("The checkpoint pin does not name this database")); }
        require_export_store(logs)?;
        let _hold = export_lock(store, pin, false)?;
        if self.replication.status().first_erasure.is_some() { return Err(protected()); }
        if let Some(receipt) = read_receipt(store, self.authority.incarnation, pin).await? {
            if read_release(store, &receipt).await?.is_some() { return Err(released()); }
            return self.pin_answer(receipt, basis, until, requested);
        }
        self.replication.serving_barrier(cancellation).await?;
        let term = self.replication.term().ok_or_else(unavailable)?;
        let status = self.replication.status();
        if status.first_erasure.is_some() { return Err(protected()); }
        let snapshot = self.replication.serving_partition().ok_or_else(unavailable)?;
        let blocks = logs.recovery_block_store(snapshot.trie_catalog().file_size_target())?;
        let block = if let Some(publication) = requested {
            let root = blocks.load_fresh_catalog(publication.block).await?;
            if publication.root_sha256.is_none() || root.publication_sha256() != publication.root_sha256
                || root.boundary_replica_position() != Some(publication.boundary)
            { return Err(unavailable()); }
            root
        } else {
            let latest = snapshot.table_catalog().block().ok_or_else(unavailable)?;
            find_retained_publication(&blocks, &self.authority, latest, basis, cancellation).await?
        };
        let selection = Selection {
            pin, basis, until, block: block.block_index(), boundary: block.boundary_replica_position().ok_or_else(unavailable)?,
            source: block.latest_source_position().ok_or_else(unavailable)?, storage_epoch: logs.storage_epoch,
            storage_version: logs.storage_version, root: block.publication_sha256().ok_or_else(unavailable)?,
        };
        selection.alive(self.replication.serving_clock())?;
        let installed = status.installed.ok_or_else(unavailable)?;
        if selection.boundary.epoch() != logs.replica_epoch || selection.boundary > installed.replica
            || selection.source > installed.source || !logs.confirms_recovery_checkpoint(&block).await?
        { return Err(unavailable()); }
        let cut = CatalogCut { storage_version: logs.storage_version, storage_epoch: logs.storage_epoch,
            replica_epoch: logs.replica_epoch, through: selection.boundary };
        let checkpoint = CatalogCheckpoint { boundary: selection.boundary, root: selection.root };
        let hold = HeldView::catalog_at(store, Arc::new(snapshot.clone()), Some(cut), Some(checkpoint)).await?;
        let control = IoRegion::reading(hold, stage_export(&blocks, &self.authority, selection, logs.memory(), cancellation)).await?;
        term.pin_checkpoint(control.clone(), cancellation).await?;
        let receipt = read_receipt(store, self.authority.incarnation, pin).await?.ok_or_else(unavailable)?;
        if receipt.control != control { return Err(conflict()); }
        if read_release(store, &receipt).await?.is_some() { return Err(released()); }
        self.pin_answer(receipt, basis, until, requested)
    }

    fn pin_answer(&self, receipt: PinReceipt, basis: Basis, until: Option<Timestamp>, requested: Option<PublishedCheckpoint>) -> Result<Vec<u8>> {
        let cut = &receipt.control.selection;
        if cut.basis != basis || cut.until != until || requested.is_some_and(|requested| requested != cut.published()) { return Err(conflict()); }
        cut.alive(self.replication.serving_clock())?;
        encode(&receipt)
    }

    /// Builds and reopens a detached store in a host-provisioned output directory.
    pub async fn materialize_checkpoint_export(&self, ticket: &[u8], output_parent: &Path, cancellation: &CancellationToken) -> Result<DetachedDefaultCheckpoint> {
        if ticket.len() > MAX_RECEIPT { return Err(invalid("The checkpoint ticket is too large")); }
        let receipt: PinReceipt = decode(ticket)?;
        if encode(&receipt)?.as_slice() != ticket { return Err(invalid("The checkpoint ticket has no canonical receipt encoding")); }
        let control = &receipt.control;
        let cut = &control.selection;
        if cut.basis.incarnation() != self.authority.incarnation { return Err(invalid("The checkpoint ticket belongs to another source")); }
        let logs = self.replication.partition_logs();
        let source = logs.store().as_ref();
        require_export_store(logs)?;
        let _hold = export_lock(source, cut.pin, false)?;
        let admitted = read_receipt(source, self.authority.incarnation, cut.pin).await?.ok_or_else(unavailable)?;
        if admitted != receipt || self.replication.status().first_erasure.is_some() { return Err(unavailable()); }
        if read_release(source, &receipt).await?.is_some() { return Err(released()); }
        cut.alive(self.replication.serving_clock())?;
        let output_parent = fs::canonicalize(output_parent).map_err(io)?;
        if let Some(local) = source.local_maintenance_store() {
            let source_root = fs::canonicalize(local.root()).map_err(io)?;
            if output_parent.starts_with(source_root) { return Err(invalid("The restore destination must be outside the source storage")); }
        }
        let memory = logs.memory();
        let namespace = cut.namespace()?;
        let (manifest_bytes, _manifest_charge) = read_small(source, &manifest_key(&namespace)?, MAX_MANIFEST, memory).await?;
        if hash(&manifest_bytes) != control.manifest { return Err(invalid("The archived manifest differs from the ordered pin")); }
        let manifest: Manifest = decode(&manifest_bytes)?;
        if &manifest.selection != cut { return Err(invalid("The archive selection differs from the ordered pin")); }
        let apple_sync = source.apple_sync();
        let ready = output_parent.join(format!("checkpoint-{}-{}", Uuid::from_bytes(cut.basis.incarnation()), cut.pin));
        if ready.try_exists().map_err(io)? {
            reopen_detached(&ready, &self.authority, ticket, control, &manifest, memory, cancellation).await?;
        } else {
            let directory = tempfile::Builder::new().prefix(".checkpoint-restore-").tempdir_in(&output_parent).map_err(io)?;
            let staged = directory.path().join("storage");
            let destination = Arc::new(LocalStore::open(&staged)?.with_apple_sync(apple_sync));
            for file in &manifest.files {
                let key = ObjectKey::new(file.key.clone())?;
                let archived = archive_key(&namespace, &key)?;
                copy_verified(source, &archived, destination.as_ref(), &key, Some(file), memory, cancellation).await?;
            }
            destination.put(&ObjectKey::new("checkpoint-restore-manifest")?, manifest_bytes.clone()).await?;
            destination.put(&ObjectKey::new("checkpoint-restore-receipt")?, Bytes::copy_from_slice(ticket)).await?;
            verify_detached(&destination, &self.authority, ticket, control, &manifest, memory).await?;
            destination.close_and_wait().await;
            drop(destination);
            self.check_restore_delivery(cut, cancellation)?;
            if let Err(error) = fs::rename(staged, &ready) {
                if !ready.try_exists().map_err(io)? { return Err(io(error)); }
                reopen_detached(&ready, &self.authority, ticket, control, &manifest, memory, cancellation).await?;
            }
            rtdb_storage::sync_directory(directory.path(), apple_sync).map_err(io)?;
        }
        rtdb_storage::sync_directory(&output_parent, apple_sync).map_err(io)?;
        self.check_restore_delivery(cut, cancellation)?;
        Ok(DetachedDefaultCheckpoint { path: ready, basis: cut.basis, checkpoint: cut.published(), source: cut.source, receipt })
    }

    /// Orders release of the exact receipt and reclaims its Local archive after admitted uses drain.
    pub async fn release_checkpoint_export(&self, ticket: &[u8], cancellation: &CancellationToken) -> Result<()> {
        if ticket.len() > MAX_RECEIPT { return Err(invalid("The checkpoint ticket is too large")); }
        let receipt: PinReceipt = decode(ticket)?;
        if encode(&receipt)?.as_slice() != ticket { return Err(invalid("The checkpoint ticket has no canonical receipt encoding")); }
        let cut = &receipt.control.selection;
        if cut.basis.incarnation() != self.authority.incarnation { return Err(unavailable()); }
        let logs = self.replication.partition_logs();
        require_export_store(logs)?;
        let store = logs.store().as_ref();
        let original = read_receipt(store, self.authority.incarnation, cut.pin).await?.ok_or_else(unavailable)?;
        if original != receipt { return Err(conflict()); }
        if read_release(store, &receipt).await?.is_none() {
            self.replication.serving_barrier(cancellation).await?;
            let term = self.replication.term().ok_or_else(unavailable)?;
            term.release_checkpoint(ReleaseControl(receipt.clone()), cancellation).await?;
            if read_release(store, &receipt).await?.is_none() { return Err(unavailable()); }
        }
        let _drained = export_lock(store, cut.pin, true)?;
        let namespace = cut.namespace()?;
        let manifest = manifest_key(&namespace)?;
        match store.head_fresh(&manifest).await {
            Ok(_) => (),
            Err(rtdb_storage::Error::MissingObject(_)) => return Ok(()),
            Err(error) => return Err(error.into()),
        }
        let (body, _charge) = read_small(store, &manifest, MAX_MANIFEST, logs.memory()).await?;
        let inventory: Manifest = decode(&body)?;
        if hash(&body) != receipt.control.manifest || &inventory.selection != cut { return Err(invalid("The released archive changed its manifest")); }
        for entry in inventory.files {
            cancelled(cancellation)?;
            let key = archive_key(&namespace, &ObjectKey::new(entry.key)?)?;
            store.delete(&key).await?;
        }
        store.delete(&manifest).await?;
        Ok(())
    }

    fn check_restore_delivery(&self, cut: &Selection, cancellation: &CancellationToken) -> Result<()> {
        cancelled(cancellation)?;
        if self.replication.status().first_erasure.is_some() { return Err(protected()); }
        cut.alive(self.replication.serving_clock())
    }
}

async fn reopen_detached(path: &Path, authority: &NativeServingAuthority, ticket: &[u8], control: &PinControl, manifest: &Manifest,
    memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
) -> Result<()> {
    if !fs::symlink_metadata(path).map_err(io)?.file_type().is_dir() {
        return Err(invalid("The detached restore backing must be its own directory"));
    }
    let destination = Arc::new(LocalStore::open(path)?);
    verify_detached(&destination, authority, ticket, control, manifest, memory).await?;
    for file in &manifest.files {
        let key = ObjectKey::new(file.key.clone())?;
        if destination.head_fresh(&key).await?.size != file.length
            || hash_object(destination.as_ref(), &key, file.length, &mut std::io::sink(), memory, cancellation).await? != file.sha256
        { return Err(invalid("The detached backing has a different archived object")); }
    }
    destination.close_and_wait().await;
    Ok(())
}

async fn verify_detached(destination: &Arc<LocalStore>, authority: &NativeServingAuthority, ticket: &[u8], control: &PinControl,
    manifest: &Manifest, memory: &Arc<MemoryBudget>,
) -> Result<()> {
    let receipt_key = ObjectKey::new("checkpoint-restore-receipt")?;
    let manifest_key = ObjectKey::new("checkpoint-restore-manifest")?;
    let (backing_manifest, _charge) = read_small(destination.as_ref(), &manifest_key, MAX_MANIFEST, memory).await?;
    if destination.get_fresh_exact(&receipt_key, ticket.len()).await?.as_ref() != ticket || hash(&backing_manifest) != control.manifest {
        return Err(invalid("The detached store did not retain the selected restore evidence"));
    }
    let cut = &control.selection;
    let store: Arc<dyn ObjectStore> = destination.clone();
    let pool = Arc::new(BufferPool::new(Arc::clone(&store), 32 << 20));
    let blocks = BlockStore::new(store, pool, Arc::clone(memory), rtdb_index::TrieCatalog::default().file_size_target())?;
    let (root, tables) = blocks.load(cut.block).await?;
    verify_plain_image(blocks.object_store(), authority, &root, cut).await?;
    blocks.verify_primary_inventory(&root, &tables).await?;
    let expected = checkpoint_dependencies(&root, &tables)?;
    if expected.iter().map(ObjectKey::as_str).ne(manifest.files.iter().map(|file| file.key.as_str())) {
        return Err(invalid("The archive does not contain the exact checkpoint dependency set"));
    }
    let recovered = blocks.recover(Default::default(), cut.source, Timestamp(0), BlockDiscovery::AtPublished(cut.published())).await?;
    if recovered.progress.source_position != cut.source { return Err(invalid("Restored source progress disagrees with the pin")); }
    Ok(())
}

async fn stage_export(blocks: &BlockStore, authority: &NativeServingAuthority, selection: Selection,
    memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
) -> Result<PinControl> {
    let store = blocks.object_store();
    let (block, tables) = blocks.load(selection.block).await?;
    verify_plain_image(store, authority, &block, &selection).await?;
    blocks.verify_primary_inventory(&block, &tables).await?;
    let namespace = selection.namespace()?;
    let mut bound_objects = std::collections::BTreeMap::from([(ObjectKey::new(block_key(selection.block)?)?, selection.root)]);
    for entry in block.table_entries()? {
        let digest = block.table_block_digest(entry.oid).and_then(|digest| digest.try_into().ok())
            .ok_or_else(|| invalid("The selected root does not bind the table manifest"))?;
        bound_objects.insert(ObjectKey::new(entry.slug.block_key(selection.block)?)?, digest);
    }
    let dependencies = checkpoint_dependencies(&block, &tables)?;
    let metadata_bound = dependencies.iter().try_fold(2048usize, |bound, key| {
        bound.checked_add(key.as_str().len().saturating_mul(2).saturating_add(384))
            .filter(|bound| *bound <= MAX_MANIFEST).ok_or_else(|| invalid("The checkpoint manifest is too large"))
    })?;
    let _manifest_charge = memory.reserve(metadata_bound.saturating_mul(2))?;
    let mut files = Vec::new();
    for key in dependencies {
        let destination = archive_key(&namespace, &key)?;
        let archived = copy_verified(store, &key, store, &destination, None, memory, cancellation).await?;
        if bound_objects.get(&key).is_some_and(|expected| *expected != archived.sha256) {
            return Err(invalid("The staged checkpoint metadata differs from the published root"));
        }
        files.push(archived);
    }
    let body = encode(&Manifest { selection: selection.clone(), files })?;
    if body.len() > MAX_MANIFEST { return Err(invalid("The checkpoint manifest is too large")); }
    let manifest = hash(&body);
    store.put(&manifest_key(&namespace)?, Bytes::from(body)).await?;
    Ok(PinControl { selection, manifest })
}

async fn find_retained_publication(
    blocks: &BlockStore, authority: &NativeServingAuthority, latest: &BlockCatalog, basis: Basis,
    cancellation: &CancellationToken,
) -> Result<BlockCatalog> {
    let store = blocks.object_store();
    let has_basis = async |block: &BlockCatalog| -> Result<bool> {
        let wire = block.as_proto();
        let Some(image) = authority.read_checkpoint(
            store, wire.serving_checkpoint.as_deref(), wire.serving_checkpoint_object.as_deref(),
        ).await? else { return Ok(false); };
        let (fold, _, shared) = decode_checkpoint_image(image.bytes(), authority.incarnation)?;
        Ok(shared && fold.index().head().basis == basis)
    };
    if has_basis(latest).await? {
        return Ok(latest.clone());
    }
    let directory = ObjectKey::new("blocks")?;
    let mut retained = store.list(Some(&directory)).await?.into_iter()
        .filter_map(|object| block_index_from_key(object.key.as_str()).ok())
        .filter(|index| *index < latest.block_index())
        .collect::<Vec<_>>();
    retained.sort_unstable_by(|left, right| right.cmp(left));
    for index in retained {
        if cancellation.is_cancelled() {
            return Err(Anomaly::new(AnomalyCategory::Interrupted, "rtdb/checkpoint-export", "The export was interrupted"));
        }
        let block = match blocks.load_fresh_catalog(index).await {
            Ok(block) => block,
            Err(error) if error.category == AnomalyCategory::NotFound => continue,
            Err(error) => return Err(error),
        };
        if has_basis(&block).await? {
            return Ok(block);
        }
    }
    Err(unavailable())
}

async fn verify_plain_image(store: &dyn ObjectStore, authority: &NativeServingAuthority, block: &BlockCatalog, selection: &Selection) -> Result<()> {
    if block.block_index() != selection.block || block.publication_sha256() != Some(selection.root)
        || block.boundary_replica_position() != Some(selection.boundary) || block.latest_source_position() != Some(selection.source)
        || block.external_source_token().is_some()
    { return Err(invalid("The checkpoint differs from the requested exact source")); }
    let wire = block.as_proto();
    let image = authority.read_checkpoint(store, wire.serving_checkpoint.as_deref(), wire.serving_checkpoint_object.as_deref()).await?
        .ok_or_else(unavailable)?;
    let (fold, _, shared) = decode_checkpoint_image(image.bytes(), authority.incarnation)?;
    if image.key_generation().is_some() || fold.index().has_custody_groups() || fold.index().has_visibility_history() || fold.pending_erasure().is_some()
        || block.table_entries()?.iter().any(|entry| rtdb_types::TableRef::owned_schema(&entry.table.schema))
    { return Err(protected()); }
    if !shared || fold.index().head().basis != selection.basis { return Err(invalid("The requested Basis is not the exact published shared cut")); }
    fold.validate_installed_receipts(Some(selection.source), Some(selection.boundary), block.boundary_term())
}

async fn copy_verified(source: &dyn ObjectStore, from: &ObjectKey, target: &dyn ObjectStore, to: &ObjectKey,
    expected: Option<&ArchivedFile>, memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
) -> Result<ArchivedFile> {
    let length = source.head_fresh(from).await?.size;
    let mut file = tempfile::NamedTempFile::new().map_err(io)?;
    let sha256 = hash_object(source, from, length, file.as_file_mut(), memory, cancellation).await?;
    if expected.is_some_and(|expected| expected.length != length || expected.sha256 != sha256) {
        return Err(invalid("An archived object differs from the ordered manifest"));
    }
    cancelled(cancellation)?;
    target.put_file(to, file.path()).await?;
    if target.head_fresh(to).await?.size != length
        || hash_object(target, to, length, &mut std::io::sink(), memory, cancellation).await? != sha256
    { return Err(invalid("The destination did not retain the exact object bytes")); }
    Ok(ArchivedFile { key: from.as_str().into(), length, sha256 })
}

async fn hash_object(store: &dyn ObjectStore, key: &ObjectKey, length: u64, sink: &mut (dyn Write + Send),
    memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
) -> Result<[u8; 32]> {
    let mut digest = Sha256::new();
    let ranged = store.supports_scan_ranges(key);
    let mut offset = 0;
    while offset < length {
        cancelled(cancellation)?;
        let end = if ranged { offset.saturating_add(COPY_PAGE).min(length) } else { length };
        let size = usize::try_from(end - offset).map_err(|_| invalid("An archived object exceeds addressable memory"))?;
        let allocation = if ranged { store.scan_range_allocation(key, size)? } else { store.whole_object_allocation(key, size)? };
        let held = memory.reserve(allocation.retained_bytes)?;
        let scratch = (allocation.scratch_bytes != 0).then(|| memory.reserve(allocation.scratch_bytes)).transpose()?;
        let reservation = ReadReservation::funded(size, held, scratch);
        let bytes = if ranged { store.get_reserved_scan_range(key, offset..end, reservation).await? }
            else { store.get_reserved_object(key, reservation).await? };
        if bytes.as_slice().len() != size { return Err(invalid("An archived object was truncated")); }
        digest.update(bytes.as_slice());
        sink.write_all(bytes.as_slice()).map_err(io)?;
        offset = end;
    }
    Ok(digest.finalize().into())
}

async fn read_small(store: &dyn ObjectStore, key: &ObjectKey, limit: usize, memory: &Arc<MemoryBudget>) -> Result<(Bytes, Arc<MemoryReservation>)> {
    let len = usize::try_from(store.head_fresh(key).await?.size).ok().filter(|len| *len <= limit)
        .ok_or_else(|| invalid("Archive metadata exceeds its supported size"))?;
    let charge = memory.reserve(len)?;
    Ok((store.get_fresh_exact(key, len).await?, charge))
}

async fn read_receipt(store: &dyn ObjectStore, incarnation: [u8; 16], pin: Uuid) -> Result<Option<PinReceipt>> {
    let key = receipt_key(incarnation, pin)?;
    let size = match store.head_fresh(&key).await {
        Ok(metadata) => usize::try_from(metadata.size).ok().filter(|size| *size <= MAX_RECEIPT).ok_or_else(|| invalid("The stored checkpoint receipt is too large"))?,
        Err(rtdb_storage::Error::MissingObject(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let receipt: PinReceipt = decode(&store.get_fresh_exact(&key, size).await?)?;
    if receipt.control.selection.pin != pin || receipt.control.selection.basis.incarnation() != incarnation
        || receipt.replica <= receipt.control.selection.boundary || receipt.term.0 <= 0
    { return Err(invalid("The stored receipt has a different ordered pin identity")); }
    Ok(Some(receipt))
}

fn require_export_store(logs: &crate::PartitionLogs) -> Result<()> {
    let store = logs.store();
    if store.local_classic_key_slots().is_some() || logs.checkpoint_log_keys().is_some() { return Err(protected()); }
    if !store.syncs_checkpoints() || store.local_maintenance_store().is_none() { return Err(unavailable()); }
    Ok(())
}

fn export_lock(store: &dyn ObjectStore, pin: Uuid, exclusive: bool) -> Result<fs::File> {
    let local = store.local_maintenance_store().ok_or_else(unavailable)?;
    let directory = local.root().join(".checkpoint-export-locks");
    fs::create_dir_all(&directory).map_err(io)?;
    let file = fs::OpenOptions::new().create(true).truncate(false).read(true).write(true).open(directory.join(pin.to_string())).map_err(io)?;
    let lock = if exclusive { FileExt::try_lock_exclusive(&file) } else { FileExt::try_lock_shared(&file) };
    match lock {
        Ok(()) => Ok(file),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/checkpoint-pin-draining", "The Local checkpoint archive is draining admitted restores")),
        Err(error) => Err(io(error)),
    }
}

async fn read_release(store: &dyn ObjectStore, pin: &PinReceipt) -> Result<Option<ReleaseReceipt>> {
    let selected = &pin.control.selection;
    let key = release_key(selected.basis.incarnation(), selected.pin)?;
    let size = match store.head_fresh(&key).await {
        Ok(metadata) => usize::try_from(metadata.size).ok().filter(|size| *size <= MAX_RECEIPT).ok_or_else(|| invalid("The checkpoint release is too large"))?,
        Err(rtdb_storage::Error::MissingObject(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let release: ReleaseReceipt = decode(&store.get_fresh_exact(&key, size).await?)?;
    if &release.pin != pin || release.replica <= pin.replica || release.term < pin.term { return Err(invalid("The receipt has a different ordered release")); }
    Ok(Some(release))
}

fn archive_key(namespace: &str, key: &ObjectKey) -> Result<ObjectKey> { Ok(ObjectKey::new(format!("{namespace}/objects/{key}"))?) }
fn manifest_key(namespace: &str) -> Result<ObjectKey> { Ok(ObjectKey::new(format!("{namespace}/manifest"))?) }
fn receipt_key(incarnation: [u8; 16], pin: Uuid) -> Result<ObjectKey> { Ok(ObjectKey::new(format!("db/{}/checkpoint-pins/{pin}", Uuid::from_bytes(incarnation)))?) }
fn release_key(incarnation: [u8; 16], pin: Uuid) -> Result<ObjectKey> { Ok(ObjectKey::new(format!("db/{}/checkpoint-releases/{pin}", Uuid::from_bytes(incarnation)))?) }
fn hash(bytes: &[u8]) -> [u8; 32] { Sha256::digest(bytes).into() }
fn hex(bytes: &[u8; 32]) -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() }
fn encode(value: &impl Serialize) -> Result<Vec<u8>> { serde_json::to_vec(value).map_err(|error| invalid(error.to_string())) }
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> { serde_json::from_slice(bytes).map_err(|error| invalid(error.to_string())) }
fn io(error: std::io::Error) -> Anomaly { invalid(error.to_string()) }
fn cancelled(token: &CancellationToken) -> Result<()> { if token.is_cancelled() { Err(crate::progress::cancelled()) } else { Ok(()) } }
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("rtdb/checkpoint-export", message) }
fn conflict() -> Anomaly { Anomaly::new(AnomalyCategory::Conflict, "rtdb/checkpoint-pin-conflict", "The pin already retains a different checkpoint selection") }
fn unavailable() -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb/checkpoint-unretained", "The installed checkpoint export is unavailable") }
fn protected() -> Anomaly { Anomaly::unsupported("rtdb/checkpoint-custody-binding", "Checkpoint export requires a plain history or an explicit destination custody binding") }
fn released() -> Anomaly { Anomaly::new(AnomalyCategory::NotFound, "rtdb/checkpoint-pin-released", "The ordered checkpoint pin has been released") }

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn first_admitted_pin_receipt_and_expiry_survive_reopening_the_store() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalStore::open(directory.path()).unwrap();
        let memory = MemoryBudget::new(1 << 24);
        let pin = Uuid::from_bytes([3; 16]);
        let selection = Selection { pin, basis: Basis::new([7; 16], 4), until: Some(Timestamp(30)), block: BlockIndex(1),
            boundary: ReplicaPosition(11), source: SourcePosition(9), storage_epoch: StorageEpoch(0),
            storage_version: rtdb_format::keys::CURRENT_STORAGE_VERSION, root: [5; 32] };
        let body = encode(&Manifest { selection: selection.clone(), files: Vec::new() }).unwrap();
        let control = PinControl { selection, manifest: hash(&body) };
        let key = manifest_key(&control.selection.namespace().unwrap()).unwrap();
        store.put(&key, body.into()).await.unwrap();
        let frame = control.encode_record().unwrap();
        let first = PinControl::decode(&frame).unwrap().unwrap();
        first.apply(&store, ReplicaPosition(12), Term(2), &memory).await.unwrap();
        store.close_and_wait().await;
        drop(store);

        let store = LocalStore::open(directory.path()).unwrap();
        PinControl::decode(&frame).unwrap().unwrap().apply(&store, ReplicaPosition(17), Term(3), &memory).await.unwrap();
        let receipt = read_receipt(&store, [7; 16], pin).await.unwrap().unwrap();
        assert_eq!(receipt, PinReceipt { control, replica: ReplicaPosition(12), term: Term(2) });
        assert!(receipt.control.selection.alive(Timestamp(29)).is_ok());
        assert_eq!(receipt.control.selection.alive(Timestamp(30)).unwrap_err().code, "rtdb/checkpoint-pin-expired");
        let reader = export_lock(&store, pin, false).unwrap();
        let release = ReleaseControl(receipt.clone()).encode_record().unwrap();
        ReleaseControl::decode(&release).unwrap().unwrap().apply(&store, ReplicaPosition(18), Term(3)).await.unwrap();
        assert_eq!(export_lock(&store, pin, true).unwrap_err().code, "rtdb/checkpoint-pin-draining");
        drop(reader);
        let reclaimer = export_lock(&store, pin, true).unwrap();
        store.delete(&key).await.unwrap();
        drop(reclaimer);
        store.close_and_wait().await;
        drop(store);

        let store = LocalStore::open(directory.path()).unwrap();
        PinControl::decode(&frame).unwrap().unwrap().apply(&store, ReplicaPosition(12), Term(2), &memory).await.unwrap();
        ReleaseControl::decode(&release).unwrap().unwrap().apply(&store, ReplicaPosition(18), Term(3)).await.unwrap();
        assert_eq!(read_release(&store, &receipt).await.unwrap().unwrap().replica, ReplicaPosition(18));
    }

    #[test]
    fn restore_rebinds_the_migrated_classic_exact_read_floor() {
        use rtdb_index::branches::{BranchCut, BranchIndex};
        use rtdb_types::rtdb_v1::At;

        let old = [7; 16];
        let fresh = [6; 16];
        let mut index = BranchIndex::new(old);
        index.advance_classic(BranchCut { basis: Basis::new(old, 4), system_time: Timestamp(5) }).unwrap();
        index.seal_classic_checkpoint();
        index.restore_in(fresh).unwrap();
        assert_eq!(index.cut(At::AsOf(Timestamp(5))).unwrap().basis, Basis::new(fresh, 4));
        assert_eq!(index.exact(Basis::new(fresh, 3)).unwrap_err().code, "rtdb/cursor-expired");
    }
}
