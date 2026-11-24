use std::collections::BTreeSet;

use bytes::Bytes;
use rtdb_format::{
    catalog::BlockCatalog,
    envelope::ReplicaBody,
    journal::{ObjectId, ObjectKind},
    keys::{block_index_from_key, block_key, oversized_block_index, skipped_tx_position},
};
use rtdb_storage::{
    Error, ObjectKey, ReadReservation,
    log::{CheckpointReplicaReference, LogOffset},
};
use rtdb_types::{Anomaly, BlockIndex, ReplicaPosition, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{BlockStore, PartitionLogs, blocks::checkpoint_digest, checkpoint_dependencies};

use super::{ErasureCut, RewriteEvidence, check_cancelled, invalid};

const MAX_INVENTORY_BYTES: usize = 16 << 20;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetirementInventory {
    block: BlockIndex,
    cut: ErasureCut,
    boundary: ReplicaPosition,
    survivor_members: [u8; 32],
    old_roots: Vec<OldRoot>,
    old_native_images: BTreeSet<String>,
    replica_payloads: BTreeSet<String>,
    objects: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OldRoot {
    block: BlockIndex,
    sha256: [u8; 32],
    inline: bool,
    sealed: Option<String>,
}

impl RetirementInventory {
    pub(super) async fn capture(
        blocks: &BlockStore,
        logs: &PartitionLogs,
        base: Option<&BlockCatalog>,
        block: BlockIndex,
        evidence: RewriteEvidence,
        survivor: &BTreeSet<ObjectKey>,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        let store = blocks.object_store();
        let mut roots = BTreeSet::new();
        let mut objects = BTreeSet::new();
        for item in store.list(Some(&ObjectKey::new("blocks")?)).await? {
            let index = block_index_from_key(item.key.as_str())?;
            if item.key.as_str() != block_key(index)? || index >= block {
                return Err(invalid());
            }
            roots.insert(index);
        }
        if let Some(base) = base {
            if !roots.contains(&base.block_index()) {
                return Err(invalid());
            }
        }
        let mut old_roots = Vec::new();
        let mut old_native_images = BTreeSet::new();
        for index in roots {
            check_cancelled(cancellation)?;
            let (catalog, tables) = blocks.load(index).await?;
            if catalog.latest_source_position().is_none_or(|source| {
                source.epoch() != logs.source_epoch || source > evidence.cut.source
            }) || catalog.boundary_replica_position().is_none_or(|boundary| {
                boundary.epoch() != logs.replica_epoch || boundary >= evidence.boundary
            }) || catalog.external_source_token().is_some()
                || !catalog.secondary_databases().is_empty()
            {
                return Err(incomplete());
            }
            let sha256 = checkpoint_digest(&catalog)?;
            if base.is_some_and(|base| {
                base.block_index() == index && base.publication_sha256() != Some(sha256)
            }) {
                return Err(invalid());
            }
            let wire = catalog.as_proto();
            let inline = wire.serving_checkpoint.is_some();
            let sealed = wire.serving_checkpoint_object.clone();
            if let Some(path) = &sealed {
                let id: ObjectId = path.parse()?;
                if inline
                    || id.kind != ObjectKind::File
                    || id.database.as_bytes() != &evidence.cut.basis.incarnation()
                {
                    return Err(invalid());
                }
                old_native_images.insert(path.clone());
            }
            let dependencies = checkpoint_dependencies(&catalog, &tables)?;
            if !dependencies.is_disjoint(survivor) {
                return Err(invalid());
            }
            objects.extend(dependencies);
            old_roots.push(OldRoot {
                block: index,
                sha256,
                inline,
                sealed,
            });
        }
        let database = uuid::Uuid::from_bytes(evidence.cut.basis.incarnation());
        let directory = ObjectKey::new(format!("db/{database}/file"))?;
        let erase_term = u64::try_from(evidence.cut.term.0).map_err(|_| invalid())?;
        for item in store.list(Some(&directory)).await? {
            check_cancelled(cancellation)?;
            let id: ObjectId = item.key.as_str().parse()?;
            if id.database != database || id.kind != ObjectKind::File {
                return Err(invalid());
            }
            if id.epoch < erase_term {
                old_native_images.insert(item.key.to_string());
                objects.insert(item.key);
            }
        }
        let replica_payloads =
            Self::replica_payloads(logs, block, evidence.boundary, cancellation).await?;
        for path in &replica_payloads {
            objects.insert(ObjectKey::new(path)?);
        }
        for item in store.list(Some(&ObjectKey::new("tables")?)).await? {
            check_cancelled(cancellation)?;
            if !survivor.contains(&item.key) {
                objects.insert(item.key);
            }
        }
        for item in store.list(Some(&ObjectKey::new("oversized")?)).await? {
            check_cancelled(cancellation)?;
            if oversized_block_index(item.key.as_str()).is_some_and(|index| index < block) {
                objects.insert(item.key);
            }
        }
        for item in store.list(Some(&ObjectKey::new("skipped-txs")?)).await? {
            check_cancelled(cancellation)?;
            if skipped_tx_position(item.key.as_str()).is_some_and(|source| {
                source.epoch() == logs.source_epoch && source <= evidence.cut.source
            }) {
                objects.insert(item.key);
            }
        }
        let inventory = Self {
            block,
            cut: evidence.cut,
            boundary: evidence.boundary,
            survivor_members: evidence.members_sha256,
            old_roots,
            old_native_images,
            replica_payloads,
            objects: objects.into_iter().map(|key| key.to_string()).collect(),
        };
        inventory.validate(block, evidence, survivor)?;
        Ok(inventory)
    }

    async fn replica_payloads(
        logs: &PartitionLogs,
        block: BlockIndex,
        boundary: ReplicaPosition,
        cancellation: &CancellationToken,
    ) -> Result<BTreeSet<String>> {
        let through = boundary.after_offset(logs.replica_epoch);
        let before = through
            .checked_add(1)
            .filter(|before| *before > 0)
            .ok_or_else(invalid)?;
        let end = ReplicaPosition(boundary.0.checked_add(1).ok_or_else(invalid)?);
        let (_, custody) = logs.checkpoint_log_keys().ok_or_else(incomplete)?;
        let dense = custody.checkpoint_offsets_are_dense();
        let mut objects = BTreeSet::new();
        let mut first_live = 0;
        if let Some(prior) = custody
            .prior_checkpoint_references(LogOffset(through))
            .await?
        {
            first_live = prior.first_live.0;
            if !(0..=before).contains(&first_live) {
                return Err(incomplete());
            }
            for reference in prior.detached {
                let CheckpointReplicaReference::Detached {
                    storage_version,
                    storage_epoch,
                    key,
                } = reference
                else {
                    return Err(incomplete());
                };
                if storage_version != logs.storage_version
                    || storage_epoch != logs.storage_epoch
                    || oversized_block_index(&key).is_none_or(|index| index > block)
                {
                    return Err(incomplete());
                }
                objects.insert(key);
            }
        }
        if first_live == before {
            return Ok(objects);
        }
        let first = ReplicaPosition::from_epoch_offset(logs.replica_epoch, first_live)?;
        let mut range = logs.replica_range(first, end).await?;
        let bound = range.allocation_bound();
        let mut previous: Option<ReplicaPosition> = None;
        while let Some(raw) = cancellation
            .run_until_cancelled(
                range.next(ReadReservation::new(bound, logs.memory().reserve(bound)?)),
            )
            .await
            .ok_or_else(crate::progress::cancelled)??
        {
            let record = logs.decode_replica(raw)?.ok_or_else(incomplete)?;
            let position = record.metadata.position;
            if previous.map_or(position != first, |previous| {
                position <= previous || dense && previous.0.checked_add(1) != Some(position.0)
            }) || position > boundary
            {
                return Err(incomplete());
            }
            previous = Some(position);
            if let Some(key) = record
                .envelope
                .oversized_payload(logs.storage_version, logs.storage_epoch)?
            {
                if record.envelope.offload_publication()?.is_some()
                    || oversized_block_index(&key).is_none_or(|index| index > block)
                {
                    return Err(incomplete());
                }
                objects.insert(key);
            }
            if position == boundary
                && !matches!(&record.envelope.message, ReplicaBody::BlockBoundary(_))
            {
                return Err(invalid());
            }
        }
        if previous != Some(boundary) {
            return Err(incomplete());
        }
        Ok(objects)
    }

    pub(super) async fn store(&self, blocks: &BlockStore) -> Result<[u8; 32]> {
        let bytes = serde_json::to_vec(self).map_err(|_| invalid())?;
        if bytes.len() > MAX_INVENTORY_BYTES {
            return Err(incomplete());
        }
        let _charge = blocks.memory().reserve(bytes.len())?;
        let digest = digest(&bytes);
        blocks
            .object_store()
            .put(&inventory_key(self.block)?, Bytes::from(bytes))
            .await?;
        Ok(digest)
    }

    pub(super) async fn read(
        blocks: &BlockStore,
        block: BlockIndex,
        evidence: RewriteEvidence,
        survivor: &BTreeSet<ObjectKey>,
    ) -> Result<Option<Self>> {
        let key = inventory_key(block)?;
        let store = blocks.object_store();
        let size = match store.head_fresh(&key).await {
            Ok(object) => usize::try_from(object.size).map_err(|_| invalid())?,
            Err(Error::MissingObject(_)) if evidence.inventory_sha256.is_none() => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if size == 0 || size > MAX_INVENTORY_BYTES {
            return Err(incomplete());
        }
        let charge = blocks.memory().reserve(size)?;
        let bytes =
            rtdb_storage::IoRegion::retaining(charge, store.get_fresh_exact(&key, size)).await?;
        if evidence
            .inventory_sha256
            .is_some_and(|expected| expected != digest(&bytes))
        {
            return Err(invalid());
        }
        let inventory: Self = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        inventory.validate(block, evidence, survivor)?;
        Ok(Some(inventory))
    }

    pub(super) fn objects(&self) -> Result<Vec<ObjectKey>> {
        self.objects
            .iter()
            .map(|path| Ok(ObjectKey::new(path)?))
            .collect()
    }

    fn validate(
        &self,
        block: BlockIndex,
        evidence: RewriteEvidence,
        survivor: &BTreeSet<ObjectKey>,
    ) -> Result<()> {
        if self.block != block
            || self.cut != evidence.cut
            || self.boundary != evidence.boundary
            || self.survivor_members != evidence.members_sha256
            || !self.objects.windows(2).all(|pair| pair[0] < pair[1])
            || !self
                .old_roots
                .windows(2)
                .all(|pair| pair[0].block < pair[1].block)
        {
            return Err(invalid());
        }
        let keys = self.objects()?.into_iter().collect::<BTreeSet<_>>();
        if !keys.is_disjoint(survivor) || keys.contains(&inventory_key(block)?) {
            return Err(invalid());
        }
        let holds = |path: &str| {
            self.objects
                .binary_search_by(|name| name.as_str().cmp(path))
                .is_ok()
        };
        for root in &self.old_roots {
            if root.block >= block
                || !holds(&block_key(root.block)?)
                || root.inline && root.sealed.is_some()
                || root
                    .sealed
                    .as_ref()
                    .is_some_and(|path| !self.old_native_images.contains(path))
            {
                return Err(invalid());
            }
        }
        let roots = self
            .old_roots
            .iter()
            .map(|root| block_key(root.block))
            .collect::<Result<BTreeSet<_>>>()?;
        for path in &self.old_native_images {
            let id: ObjectId = path.parse()?;
            let named = self
                .old_roots
                .iter()
                .any(|root| root.sealed.as_ref() == Some(path));
            if !holds(path)
                || id.kind != ObjectKind::File
                || id.database.as_bytes() != &self.cut.basis.incarnation()
                || !named && u64::try_from(self.cut.term.0).map_or(true, |term| id.epoch >= term)
            {
                return Err(invalid());
            }
        }
        for path in &self.replica_payloads {
            if !holds(path)
                || !path.starts_with("oversized/")
                || oversized_block_index(path).is_none_or(|index| index > block)
            {
                return Err(invalid());
            }
        }
        if self.objects.iter().any(|path| {
            !(path.starts_with("tables/")
                || roots.contains(path)
                || self.old_native_images.contains(path)
                || self.replica_payloads.contains(path)
                || path.starts_with("oversized/")
                    && oversized_block_index(path).is_some_and(|index| index < block)
                || skipped_tx_position(path).is_some_and(|source| {
                    source.epoch() == self.cut.source.epoch() && source <= self.cut.source
                }))
        }) {
            return Err(invalid());
        }
        Ok(())
    }
}

fn inventory_key(block: BlockIndex) -> Result<ObjectKey> {
    let name = block_key(block)?;
    let name = name.strip_prefix("blocks/").ok_or_else(invalid)?;
    Ok(ObjectKey::new(format!("default-erasure-inventory/{name}"))?)
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"rtdb.default.retirement-inventory/v1\0");
    hasher.update(bytes);
    hasher.finalize().into()
}

pub(super) fn incomplete() -> Anomaly {
    Anomaly::unsupported(
        "rtdb/default-erasure-inventory",
        "The provider cannot establish the complete retained payload inventory for this cut",
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rtdb_columnar::MemoryBudget;
    use rtdb_storage::{MemoryStore, ObjectStore, cache::BufferPool};
    use rtdb_types::{SourcePosition, Term, rtdb_v1::Basis};

    use super::*;

    #[tokio::test]
    async fn restart_accepts_only_the_bound_inventory_disjoint_from_the_survivor() -> Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
        let memory = MemoryBudget::new(1 << 20);
        let cache = Arc::new(BufferPool::new(Arc::clone(&store), 1 << 16));
        let blocks = BlockStore::new(store, cache, memory, 1 << 16)?;
        let block = BlockIndex(3);
        let cut = ErasureCut {
            basis: Basis::new([5; 16], 7),
            source: SourcePosition(8),
            replica: ReplicaPosition(9),
            term: Term(1),
        };
        let mut evidence = RewriteEvidence {
            cut,
            boundary: ReplicaPosition(11),
            members_sha256: [6; 32],
            inventory_sha256: None,
            prior_generation: None,
        };
        let root = block_key(BlockIndex(2))?;
        let offload = rtdb_format::keys::oversized_key(BlockIndex(2), "body")?;
        let old_trie = "tables/items/data/old.arrow";
        let orphan = ObjectId {
            database: uuid::Uuid::from_bytes(cut.basis.incarnation()),
            kind: ObjectKind::File,
            epoch: 0,
            seq: 6,
            digest: [7; 32],
        }
        .to_string();
        let inventory = RetirementInventory {
            block,
            cut,
            boundary: evidence.boundary,
            survivor_members: evidence.members_sha256,
            old_roots: vec![OldRoot {
                block: BlockIndex(2),
                sha256: [7; 32],
                inline: true,
                sealed: None,
            }],
            old_native_images: BTreeSet::from([orphan.clone()]),
            replica_payloads: BTreeSet::from([offload.clone()]),
            objects: vec![root, orphan, offload, old_trie.into()],
        };
        evidence.inventory_sha256 = Some(inventory.store(&blocks).await?);
        let survivor = BTreeSet::from([ObjectKey::new("tables/items/data/clean.arrow")?]);
        assert_eq!(
            RetirementInventory::read(&blocks, block, evidence, &survivor).await?,
            Some(inventory)
        );
        let collision = BTreeSet::from([ObjectKey::new(old_trie)?]);
        assert!(
            RetirementInventory::read(&blocks, block, evidence, &collision)
                .await
                .is_err()
        );
        evidence.inventory_sha256 = Some([9; 32]);
        assert!(
            RetirementInventory::read(&blocks, block, evidence, &survivor)
                .await
                .is_err()
        );
        Ok(())
    }
}
