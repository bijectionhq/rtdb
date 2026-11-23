use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use rtdb_format::{keys::TrieKey, log::proto::PublishedObjectVersion, trie_retirement::{InstalledRetirement, MAXIMUM_INTENT_BYTES, ResetRetirementCut, RetirementFile, RetirementIntent, intent_digest, journal_directory}};
use rtdb_index::PartitionValue;
use rtdb_storage::{Error, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, CatalogCut, ReplicaPosition, Result, TableRef};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::TrieDeletionPublisher;

const PENDING: &str = "pending";
const RESET_PENDING: &str = "reset-pending";

pub(crate) async fn observe_file(store: &dyn ObjectStore, key: &ObjectKey, published: Option<PublishedObjectVersion>) -> Result<RetirementFile> {
    let terminal_publication = published.as_ref().and_then(PublishedObjectVersion::terminal_publication);
    if terminal_publication.is_some() {
        return Ok(RetirementFile { key: key.to_string(), version: None, terminal_publication, matches_publication: false });
    }
    let version = store.retirement_version(key).await?;
    let matches_publication = version.as_ref().is_some_and(|observed| {
        published.as_ref().is_some_and(|expected| *expected == PublishedObjectVersion::from(observed.clone()))
    });
    Ok(RetirementFile { key: key.to_string(), version, terminal_publication: None, matches_publication })
}

#[derive(Clone)]
pub(crate) struct FileRelease {
    pub slots: Arc<Semaphore>,
    pub cancellation: CancellationToken,
}

impl FileRelease {
    async fn queue(&self, store: &dyn ObjectStore, receipt: &ObjectKey, contents: Bytes) -> Result<()> {
        let _permit = self.slots.acquire().await.map_err(crate::invalid)?;
        if self.cancellation.is_cancelled() { return Err(crate::closed()); }
        store.release_trie_files(receipt, contents).await
    }
}

pub(crate) struct PendingRetirement {
    intent: ObjectKey,
    receipt: ObjectKey,
    encoded: Bytes,
    plan: RetirementIntent,
}

pub struct ResetRetirements(Vec<PendingRetirement>);

impl ResetRetirements {
    /// Stages the compacted pairs selected by a reset before its replacement checkpoint is published.
    pub async fn stage(store: &dyn ObjectStore, view: &PartitionValue, cancellation: &CancellationToken) -> Result<Self> {
        let catalog = view.trie_catalog();
        let naming_epoch = catalog.naming_epoch().checked_add(1).filter(|epoch| i64::try_from(*epoch).is_ok())
            .ok_or_else(|| crate::invalid("The next trie naming epoch exceeds the block format"))?;
        let required = ResetRetirementCut { block: view.live().block_index(), naming_epoch };
        let cut = view.snapshot().catalog_cut();
        let mut pending = Vec::new();
        for table in catalog.tables() {
            let entry = view.table_catalog().entry(table).ok_or_else(|| crate::invalid("The reset table has no registered storage identity"))?;
            let tries = catalog.iter_tries(table).filter(|trie| trie.key.level() > 0).collect::<Vec<_>>();
            for chunk in tries.chunks(64) {
                let cut = cut.ok_or_else(|| crate::invalid("The reset selection has no installed catalog cut"))?;
                let mut keys = Vec::with_capacity(chunk.len());
                let mut files = Vec::with_capacity(2 * chunk.len());
                for trie in chunk {
                    if cancellation.is_cancelled() { return Err(crate::closed()); }
                    keys.push(trie.key.to_string());
                    let data = ObjectKey::new(entry.slug.data_key(&trie.key))?;
                    let metadata = ObjectKey::new(entry.slug.metadata_key(&trie.key))?;
                    let versions = trie.published_files.as_ref();
                    files.push(observe_file(store, &data, versions.and_then(|held| held.data.clone())).await?);
                    files.push(observe_file(store, &metadata, versions.and_then(|held| held.metadata.clone())).await?);
                }
                let plan = RetirementIntent { selected: cut, table: table.clone(), tries: keys, files, after_reset: Some(required) };
                pending.push(PendingRetirement::stage_plan(store, plan).await?);
            }
        }
        Ok(Self(pending))
    }

    /// Submits reset retirements through the normal receipt and catalog floor after the reset is adopted.
    /// A held reader can defer physical release until a later collection cycle.
    pub async fn publish(self, store: &dyn ObjectStore, publisher: &dyn TrieDeletionPublisher,
        view: &PartitionValue, block: BlockIndex, cancellation: &CancellationToken,
    ) -> Result<()> {
        if self.0.is_empty() { return Ok(()); }
        let cut = view.snapshot().catalog_cut().ok_or_else(|| crate::invalid("The adopted reset has no installed catalog cut"))?;
        let release = FileRelease { slots: Arc::new(Semaphore::new(1)), cancellation: cancellation.clone() };
        for pending in self.0 {
            let keys = pending.keys()?;
            if !pending.plan.after_reset.is_some_and(|required| required.block == block)
                || !pending.admits_reset(view, cut, &keys)
            {
                return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.maintenance/reset-checkpoint-unavailable", "The reset retirement has no adopted replacement checkpoint"));
            }
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let receipt = publisher.publish(pending.plan.table.clone(), keys).await?;
            pending.finish(store, receipt, &release).await?;
        }
        Ok(())
    }
}

impl PendingRetirement {
    fn for_bytes(encoded: Bytes) -> Result<Self> {
        let plan: RetirementIntent = serde_json::from_slice(&encoded).map_err(crate::invalid)?;
        let prefix = journal_directory(plan.selected);
        let directory = if plan.after_reset.is_some() { RESET_PENDING } else { PENDING };
        let id = intent_digest(&encoded);
        Ok(Self {
            intent: ObjectKey::new(format!("{prefix}/{directory}/{id}"))?,
            receipt: ObjectKey::new(format!("{prefix}/installed/{id}"))?,
            encoded,
            plan,
        })
    }

    pub async fn stage(store: &dyn ObjectStore, selected: CatalogCut, table: &TableRef, keys: &[TrieKey], files: Vec<RetirementFile>) -> Result<Self> {
        let intent = RetirementIntent { selected, table: table.clone(), tries: keys.iter().map(TrieKey::to_string).collect(), files, after_reset: None };
        Self::stage_plan(store, intent).await
    }

    async fn stage_plan(store: &dyn ObjectStore, intent: RetirementIntent) -> Result<Self> {
        let bytes = Bytes::from(serde_json::to_vec(&intent).map_err(crate::invalid)?);
        if bytes.len() > MAXIMUM_INTENT_BYTES { return Err(crate::invalid("A trie retirement intent exceeds the metadata size limit")); }
        let pending = Self::for_bytes(bytes)?;
        store.put(&pending.intent, pending.encoded.clone()).await?;
        Ok(pending)
    }

    fn keys(&self) -> Result<Vec<TrieKey>> {
        let keys = self.plan.tries.iter().map(|key| key.parse()).collect::<Result<Vec<TrieKey>>>()?;
        if keys.is_empty() || keys.iter().any(|key| key.level() == 0) {
            return Err(crate::invalid("A trie retirement intent must name at least one non-L0 trie and no L0 trie"));
        }
        Ok(keys)
    }

    fn admits_reset(&self, view: &PartitionValue, cut: CatalogCut, keys: &[TrieKey]) -> bool {
        let Some(required) = self.plan.after_reset else { return false; };
        let Some(block) = view.table_catalog().block() else { return false; };
        let selected = self.plan.selected;
        selected.through <= cut.through && CatalogCut { through: cut.through, ..selected } == cut
            && block.publication_sha256().is_some()
            && block.boundary_replica_position().is_some_and(|boundary| boundary.0 >= 0 && boundary > selected.through
                && boundary.epoch() == cut.replica_epoch && boundary <= cut.through)
            && required.admits(block, keys.iter().any(|key| view.retains_trie(&self.plan.table, key)))
    }

    fn validate_receipt(&self, position: ReplicaPosition) -> Result<()> {
        self.plan.installed_cut(position).map(|_| ()).map_err(crate::invalid)
    }

    async fn installed(&self, store: &dyn ObjectStore) -> Result<Option<ReplicaPosition>> {
        match store.get_fresh_exact(&self.receipt, 8 + self.encoded.len()).await {
            Ok(receipt) => {
                let (_, selected) = receipt.split_at_checked(8)
                    .ok_or_else(|| crate::invalid("A trie removal receipt has no installed position"))?;
                if selected != self.encoded.as_ref() { return Err(crate::invalid("A trie removal receipt names a different intent")); }
                let installed = InstalledRetirement::decode(self.receipt.as_str(), &receipt).map_err(crate::invalid)?;
                Ok(Some(installed.cut().through))
            }
            Err(Error::MissingObject(_)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn receipt_bytes(&self, position: ReplicaPosition) -> Bytes {
        let mut receipt = BytesMut::with_capacity(8 + self.encoded.len());
        receipt.put_i64(position.0);
        receipt.extend_from_slice(&self.encoded);
        receipt.freeze()
    }

    pub async fn finish(self, store: &dyn ObjectStore, position: ReplicaPosition, release: &FileRelease) -> Result<()> {
        self.validate_receipt(position)?;
        let installed = match store.put(&self.receipt, self.receipt_bytes(position)).await {
            Ok(()) => position,
            Err(error) => self.installed(store).await?.ok_or(error)?,
        };
        self.release_intent(store, installed, release).await
    }

    async fn release_intent(&self, store: &dyn ObjectStore, installed: ReplicaPosition, release: &FileRelease) -> Result<()> {
        let cut = CatalogCut { through: installed, ..self.plan.selected };
        if store.advance_catalog_floor(cut).await? {
            if store.defers_trie_releases() {
                release.queue(store, &self.receipt, self.receipt_bytes(installed)).await?;
            }
            store.delete(&self.intent).await?;
        }
        Ok(())
    }
}

pub(crate) async fn resume(
    store: &dyn ObjectStore, publisher: &dyn TrieDeletionPublisher, view: &PartitionValue,
    cut: CatalogCut, release: &FileRelease,
) -> Result<()> {
    let journal = journal_directory(cut);
    for directory in [RESET_PENDING, PENDING] {
        let prefix = ObjectKey::new(format!("{journal}/{directory}"))?;
        resume_directory(store, publisher, view, cut, release, &prefix).await?;
    }
    Ok(())
}

async fn resume_directory(store: &dyn ObjectStore, publisher: &dyn TrieDeletionPublisher, view: &PartitionValue,
    cut: CatalogCut, release: &FileRelease, prefix: &ObjectKey,
) -> Result<()> {
    for object in store.list(Some(prefix)).await? {
        if release.cancellation.is_cancelled() { return Err(crate::closed()); }
        let size = usize::try_from(object.size).ok().filter(|size| *size <= MAXIMUM_INTENT_BYTES)
            .ok_or_else(|| crate::invalid("A trie retirement intent exceeds the metadata size limit"))?;
        let bytes = match store.get_fresh_exact(&object.key, size).await {
            Ok(bytes) => bytes,
            Err(Error::MissingObject(_)) => continue,
            Err(error) => return Err(error.into()),
        };
        let pending = PendingRetirement::for_bytes(bytes)?;
        if pending.intent != object.key || !object.key.is_under(prefix) {
            return Err(crate::invalid("A trie retirement intent does not match its journal address"));
        }
        if pending.plan.selected.through > cut.through { continue; }
        let keys = pending.keys()?;
        if pending.plan.after_reset.is_some() && !pending.admits_reset(view, cut, &keys) { continue; }
        if let Some(installed) = pending.installed(store).await? {
            if installed <= cut.through { pending.release_intent(store, installed, release).await?; }
            continue;
        }
        if keys.iter().all(|key| !view.retains_trie(&pending.plan.table, key)) {
            let receipt = publisher.publish(pending.plan.table.clone(), keys).await?;
            pending.finish(store, receipt, release).await?;
        }
    }
    Ok(())
}
