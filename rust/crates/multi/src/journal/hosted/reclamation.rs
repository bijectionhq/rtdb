use bytes::Bytes;
use rtdb_format::{keys::{is_compacted_primary_file, is_receipt_trie_file}, trie_retirement::{InstalledRetirement, MAXIMUM_RECEIPT_BYTES, RetirementFile}};
use rtdb_storage::{Error, IoRegion, ObjectKey, ObjectStore, Result};
use rtdb_types::{CatalogCut, PhysicalObjectVersion};
use std::sync::Arc;
use serde::{Deserialize, Serialize};

use super::{Placement, genesis, prefix_retirement::GrantDescriptor};
use crate::deployment::CellStorage;
use crate::keys::KeyService;
use crate::store::{EncryptingStore, exact_retirement::{InstalledPublication, InstalledPublicationDeletionGrant}};
use crate::reference::{Bucket, PrefixedStore};

pub(super) fn queue_prefix(grant_prefix: &str) -> String {
    format!("{grant_prefix}/~reclaim/v1")
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Candidate {
    grant: GrantDescriptor,
    provider: [u8; 32],
    key: String,
    version: Option<PhysicalObjectVersion>,
}

const RECEIPTS: &str = "receipts";
const MAXIMUM_CANDIDATE_BYTES: u64 = 16 << 10;

pub(super) fn can_fence_trie_file(key: &str) -> bool {
    is_receipt_trie_file(key) || is_compacted_primary_file(key)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReceiptCandidate {
    grant: GrantDescriptor,
    provider: [u8; 32],
    key: String,
    receipt: String,
    cut: CatalogCut,
    publication: InstalledPublication,
}

impl ReceiptCandidate {
    fn publication(file: &RetirementFile) -> Option<InstalledPublication> {
        match (&file.version, &file.terminal_publication) {
            (None, Some(terminal)) => Some(InstalledPublication::Terminal { terminal: terminal.clone() }),
            (Some(version), None) if file.matches_publication => Some(InstalledPublication::Version { version: version.clone() }),
            _ => None,
        }
    }
}

pub(super) struct ReclamationQueue {
    records: PrefixedStore,
    grant: GrantDescriptor,
    provider: [u8; 32],
    source_prefix: String,
}

impl ReclamationQueue {
    pub(super) fn new(
        bucket: Bucket,
        writer: Option<Bucket>,
        queue_prefix: String,
        source_prefix: String,
        grant: GrantDescriptor,
        provider: [u8; 32],
    ) -> Self {
        let records = PrefixedStore::new(bucket, queue_prefix).with_writer(writer);
        Self {
            records,
            grant,
            provider,
            source_prefix,
        }
    }

    pub(super) async fn enqueue_receipt(
        &self, upper: &dyn ObjectStore, lower: Option<&dyn ObjectStore>,
        key: &ObjectKey, receipt: &ObjectKey, cut: CatalogCut, file: &RetirementFile,
    ) -> Result<()> {
        if key.as_str() != file.key || !can_fence_trie_file(key.as_str()) {
            return Err(Error::InvalidKey(key.to_string()));
        }
        let Some(publication) = ReceiptCandidate::publication(file) else { return Ok(()); };
        if let Some(lower) = lower {
            match lower.head_fresh(key).await {
                Ok(_) => return Ok(()),
                Err(Error::MissingObject(_)) => (),
                Err(error) => return Err(error),
            }
        }
        if !upper.retirement_publication(key).await?.is_some_and(|observed| publication.matches(&observed)) {
            return Ok(());
        }
        let bytes = serde_json::to_vec(&ReceiptCandidate {
            grant: self.grant.clone(),
            provider: self.provider,
            key: key.to_string(),
            receipt: receipt.to_string(),
            cut,
            publication,
        }).map_err(|error| Error::Task(error.to_string()))?;
        self.put_candidate(receipt_candidate_key(&bytes)?, bytes).await
    }

    pub(super) async fn enqueue(
        &self,
        upper: &dyn ObjectStore,
        lower: Option<&dyn ObjectStore>,
        key: &ObjectKey,
    ) -> Result<()> {
        if matches!(
            key.as_str(),
            super::prefix_retirement::DESCRIPTOR | crate::store::ROOT_MARKER
        ) {
            return Err(Error::InvalidKey(key.to_string()));
        }
        // An inherited object wins even if a loose object with the same key exists above it.
        if let Some(lower) = lower {
            match lower.head_fresh(key).await {
                Ok(_) => return Ok(()),
                Err(Error::MissingObject(_)) => (),
                Err(error) => return Err(error),
            }
        }
        let observed = match upper.observe_head(key).await {
            Ok(observed) => observed,
            Err(Error::MissingObject(_)) => return Ok(()),
            Err(error) => return Err(error),
        };
        let candidate = Candidate {
            grant: self.grant.clone(),
            provider: self.provider,
            key: format!("{}/{key}", self.source_prefix),
            version: observed.version,
        };
        let bytes =
            serde_json::to_vec(&candidate).map_err(|error| Error::Task(error.to_string()))?;
        self.put_candidate(ObjectKey::new(candidate_digest(&bytes))?, bytes).await
    }

    async fn put_candidate(&self, id: ObjectKey, bytes: Vec<u8>) -> Result<()> {
        match self.records.get_fresh_exact(&id, bytes.len()).await {
            Ok(existing) if existing.as_ref() == bytes.as_slice() => Ok(()),
            Ok(_) => Err(Error::ConflictingObject(id.to_string())),
            Err(Error::MissingObject(_)) => self.records.put(&id, Bytes::from(bytes)).await,
            Err(error) => Err(error),
        }
    }
}

fn candidate_digest(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
    let mut hex = String::with_capacity(64);
    for byte in digest.as_ref() {
        write!(hex, "{byte:02x}").expect("write to String");
    }
    hex
}

fn receipt_candidate_key(bytes: &[u8]) -> Result<ObjectKey> {
    ObjectKey::new(format!("{RECEIPTS}/{}", candidate_digest(bytes)))
}

pub(super) async fn settle_receipts(
    placement: &Placement, storage: &CellStorage, keys: &Arc<dyn KeyService>,
) -> rtdb_types::Result<()> {
    let (Some(custodian), CellStorage::S3 { config }) = (placement.custodian.as_ref(), storage) else {
        return Ok(());
    };
    let route = custodian.route().await?;
    let Some(active) = route.writer.grant.filter(|_| !genesis(route.checkpoint)) else { return Ok(()); };
    let root = ObjectKey::new(crate::archive::checkpoint_storage_root()?)?;
    let residence = placement.record.storage_prefix();
    let grant_root = super::storage::grant_prefix(&residence, route.placement.generation, active.epoch);
    let source = super::storage::source_prefix(&residence, route.placement.generation, active.epoch, &root);
    let io = IoRegion::new();
    let bucket = storage.bucket(Arc::clone(&io)).await?;
    let records = PrefixedStore::new(Arc::clone(&bucket), queue_prefix(&grant_root));
    let entries = records.list(Some(&ObjectKey::new(RECEIPTS)?)).await?;
    if entries.is_empty() { return Ok(()); }

    let inventory = CellStorage::s3_inventory(config).await?;
    if config.terminal_scope.is_none() { inventory.require_versioned().await?; }
    let checkpoint = placement.checkpoint_custody(Arc::clone(&io), Arc::clone(keys)).await?;
    checkpoint.open(route.checkpoint).await?;
    let provider = storage.custody_namespace()?;
    let ledger = Arc::new(super::storage::serving_ledger(Arc::clone(keys), route.placement, provider, &source, io));
    let raw = Arc::new(PrefixedStore::new(Arc::clone(&bucket), source.clone()));
    let sealed = EncryptingStore::with_units(raw.clone(), ledger.clone());

    for entry in entries {
        if entry.size > MAXIMUM_CANDIDATE_BYTES { continue; }
        let bytes = records.get_fresh_exact(&entry.key, entry.size as usize).await?;
        if receipt_candidate_key(&bytes)? != entry.key { continue; }
        let Ok(candidate) = serde_json::from_slice::<ReceiptCandidate>(&bytes) else { continue; };
        if candidate.provider != provider || candidate.grant != GrantDescriptor::new(placement, active) {
            continue;
        }
        let through = candidate.cut.through.after_offset(candidate.cut.replica_epoch);
        if candidate.cut.replica_epoch != custodian.log_epoch()
            || rtdb_format::keys::StorageRoot::new(candidate.cut.storage_version, candidate.cut.storage_epoch)?.key() != root.as_str()
            || u64::try_from(through).ok().is_none_or(|through| through >= route.floor.next_lsn)
            || route.floor.next_lsn > route.checkpoint.cut.next_lsn
            || !can_fence_trie_file(&candidate.key)
        {
            continue;
        }
        let key = ObjectKey::new(candidate.key.as_str())?;
        let target = ObjectKey::new(format!("{source}/{key}"))?;
        if !candidate.publication.available(&target, &inventory).await? { continue; }
        let receipt = ObjectKey::new(candidate.receipt.as_str())?;
        let size = match sealed.head_fresh(&receipt).await {
            Ok(head) if head.size <= MAXIMUM_RECEIPT_BYTES as u64 => head.size as usize,
            Ok(_) | Err(Error::MissingObject(_)) => continue,
            Err(error) => return Err(error.into()),
        };
        let body = sealed.get_fresh_exact(&receipt, size).await?;
        let installed = InstalledRetirement::decode(receipt.as_str(), &body)?;
        if installed.cut() != candidate.cut || !installed.files().iter().any(|file| {
            file.key == candidate.key && ReceiptCandidate::publication(file).is_some_and(|publication| publication == candidate.publication)
        }) {
            continue;
        }
        match raw.retirement_publication(&key).await? {
            Some(observed) if candidate.publication.matches(&observed) => (),
            Some(_) => continue,
            None => match raw.head_fresh(&key).await {
                Err(Error::MissingObject(_)) => (),
                Ok(_) => continue,
                Err(error) => return Err(error.into()),
            },
        }
        let Some(fence) = custodian.withdraw_object(&route, candidate.publication.withdrawal()?).await? else { continue; };
        let grant = InstalledPublicationDeletionGrant::from_journal(&target, &candidate.publication, fence, route.placement)?;
        ledger.withdraw(&key).await?;
        grant.settle(&inventory).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use rtdb_format::{
        block::proto::Block,
        catalog::{BlockCatalog, TableBlock},
        envelope::{ReplicaBody, ReplicaEnvelope},
        keys::{block_key, oversized_key},
    };
    use rtdb_replication::{
        ReplayBase, ReplayDependencySources, checkpoint_dependencies, tail_dependencies,
    };
    use rtdb_storage::{LocalStore, MemoryStore};
    use rtdb_types::{
        Anomaly, BlockIndex, StorageEpoch, StorageVersion, TableRef, Term, TermSequence,
    };
    use uuid::Uuid;

    use super::*;

    const GRANT_PREFIX: &str = "tenant/lineage/database/1/~ordered/2/7";
    const SOURCE_PREFIX: &str = "tenant/lineage/database/1/~ordered/2/7/v06";

    fn key(name: impl Into<String>) -> ObjectKey {
        ObjectKey::new(name).unwrap()
    }

    fn queue(bucket: Bucket) -> ReclamationQueue {
        let grant = GrantDescriptor {
            deployment: "database".into(),
            incarnation: 1,
            placement: Uuid::from_u128(1),
            placement_incarnation: Uuid::from_u128(2),
            group: Uuid::from_u128(3),
            generation: 2,
            worker: Uuid::from_u128(4),
            serial: 1,
            epoch: 7,
        };
        ReclamationQueue::new(
            bucket,
            None,
            queue_prefix(GRANT_PREFIX),
            SOURCE_PREFIX.into(),
            grant,
            [9; 32],
        )
    }

    async fn candidates(bucket: &dyn ObjectStore) -> Vec<Candidate> {
        let mut candidates = Vec::new();
        for entry in bucket
            .list(Some(&key(queue_prefix(GRANT_PREFIX))))
            .await
            .unwrap()
        {
            let bytes = bucket.get_fresh(&entry.key).await.unwrap();
            candidates.push(serde_json::from_slice(&bytes).unwrap());
        }
        candidates
    }

    #[test]
    fn installed_trie_grants_cover_primary_versions_and_leave_other_files_with_their_owners() {
        assert!(can_fence_trie_file("tables/public$orders/data/l01-rc-b00.arrow"));
        assert!(can_fence_trie_file("tables/public$orders/meta/l01-rc-b00.arrow"));
        assert!(!can_fence_trie_file("tables/public$orders/data/l00-rc-b00.arrow"));
        assert!(!can_fence_trie_file("~packs/b00.pack"));
        assert!(!can_fence_trie_file("blocks/b00.binpb"));
    }

    #[tokio::test]
    async fn selectable_checkpoint_object_is_queued_but_never_deleted() {
        let directory = tempfile::tempdir().unwrap();
        let bucket: Bucket = Arc::new(LocalStore::open(directory.path()).unwrap());
        let queue = queue(Arc::clone(&bucket));
        let upper = PrefixedStore::new(bucket, SOURCE_PREFIX);
        let catalog = BlockCatalog::from_proto(Block {
            block_index: Some(0),
            ..Default::default()
        })
        .unwrap();
        let root = key(block_key(catalog.block_index()).unwrap());
        let bytes = Bytes::from(catalog.encode());
        upper.put(&root, bytes.clone()).await.unwrap();
        assert!(
            checkpoint_dependencies(&catalog, &BTreeMap::new())
                .unwrap()
                .contains(&root)
        );

        let (first, retry) = tokio::join!(
            queue.enqueue(&upper, None, &root),
            queue.enqueue(&upper, None, &root)
        );
        first.unwrap();
        retry.unwrap();
        drop((queue, upper));

        let reopened: Bucket = Arc::new(LocalStore::open(directory.path()).unwrap());
        assert_eq!(
            candidates(reopened.as_ref()).await,
            [Candidate {
                grant: self::queue(Arc::clone(&reopened)).grant,
                provider: [9; 32],
                key: format!("{SOURCE_PREFIX}/{root}"),
                version: None,
            }]
        );
        let physical_root = key(format!("{SOURCE_PREFIX}/{root}"));
        assert_eq!(
            reopened.get_fresh(&physical_root).await.ok(),
            Some(bytes),
            "physical deletion must be refused while the checkpoint is selectable"
        );
    }

    #[tokio::test]
    async fn inherited_missing_and_control_objects_do_not_become_candidates() {
        let bucket: Bucket = Arc::new(MemoryStore::default());
        let queue = queue(Arc::clone(&bucket));
        let upper = PrefixedStore::new(Arc::clone(&bucket), SOURCE_PREFIX);
        let lower = MemoryStore::default();
        let inherited = key("blocks/b00.binpb");
        lower
            .put(&inherited, Bytes::from_static(b"inherited"))
            .await
            .unwrap();
        upper
            .put(&inherited, Bytes::from_static(b"shadowed"))
            .await
            .unwrap();
        queue
            .enqueue(&upper, Some(&lower), &inherited)
            .await
            .unwrap();
        queue
            .enqueue(&upper, Some(&lower), &key("blocks/missing"))
            .await
            .unwrap();
        for marker in [
            super::super::prefix_retirement::DESCRIPTOR,
            crate::store::ROOT_MARKER,
        ] {
            assert!(matches!(
                queue.enqueue(&upper, None, &key(marker)).await,
                Err(Error::InvalidKey(_))
            ));
        }
        assert!(candidates(bucket.as_ref()).await.is_empty());
    }

    struct PayloadSource<'a>(&'a dyn ObjectStore);

    #[async_trait]
    impl ReplayDependencySources for PayloadSource<'_> {
        async fn oversized_payload(&self, key: &ObjectKey) -> rtdb_types::Result<Bytes> {
            Ok(self.0.get_fresh(key).await?)
        }

        async fn adopted_checkpoint(
            &self,
            _: BlockIndex,
        ) -> rtdb_types::Result<(BlockCatalog, BTreeMap<TableRef, TableBlock>)> {
            Err(Anomaly::fault(
                "rtdb.multi/test",
                "The test suffix has no adopted checkpoint",
            ))
        }
    }

    #[tokio::test]
    async fn a_retained_offloaded_payload_remains_replayable_after_its_delete_request() {
        let bucket: Bucket = Arc::new(MemoryStore::default());
        let queue = queue(Arc::clone(&bucket));
        let upper = PrefixedStore::new(Arc::clone(&bucket), SOURCE_PREFIX);
        let envelope = ReplicaEnvelope {
            term: Term(7),
            term_sequence: Some(TermSequence(1)),
            message: ReplicaBody::NoOp(Default::default()),
        };
        let storage_version = StorageVersion(6);
        let storage_epoch = StorageEpoch(0);
        let block = BlockIndex(1);
        let payload = envelope.encode();
        let proxy = envelope.offloaded(
            storage_version,
            storage_epoch,
            block,
            "retained".into(),
            &payload,
        );
        let name = key(oversized_key(block, "retained").unwrap());
        upper.put(&name, Bytes::from(payload)).await.unwrap();

        queue.enqueue(&upper, None, &name).await.unwrap();

        let base = ReplayBase {
            storage_version,
            storage_epoch,
            checkpoint: None,
        };
        assert!(
            tail_dependencies(base, [proxy], &PayloadSource(&upper))
                .await
                .unwrap()
                .contains(&name)
        );
        assert_eq!(
            candidates(bucket.as_ref()).await[0].key,
            format!("{SOURCE_PREFIX}/{name}")
        );
    }
}
