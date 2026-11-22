use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use rtdb_format::{keys::TrieKey, log::proto::PublishedObjectVersion, trie_retirement::{InstalledRetirement, MAXIMUM_INTENT_BYTES, RetirementFile, RetirementIntent, intent_digest, journal_directory}};
use rtdb_index::PartitionValue;
use rtdb_storage::{Error, ObjectKey, ObjectStore};
use rtdb_types::{CatalogCut, ReplicaPosition, Result, TableRef};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::TrieDeletionPublisher;

pub(crate) async fn observe_file(store: &dyn ObjectStore, key: &ObjectKey, published: Option<PublishedObjectVersion>) -> Result<RetirementFile> {
    let version = store.retirement_version(key).await?;
    let matches_publication = version.as_ref().is_some_and(|observed| {
        published.as_ref().is_some_and(|expected| *expected == PublishedObjectVersion::from(observed.clone()))
    });
    Ok(RetirementFile { key: key.to_string(), version, matches_publication })
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

impl PendingRetirement {
    fn for_bytes(encoded: Bytes) -> Result<Self> {
        let plan: RetirementIntent = serde_json::from_slice(&encoded).map_err(crate::invalid)?;
        let prefix = journal_directory(plan.selected);
        let id = intent_digest(&encoded);
        Ok(Self {
            intent: ObjectKey::new(format!("{prefix}/pending/{id}"))?,
            receipt: ObjectKey::new(format!("{prefix}/installed/{id}"))?,
            encoded,
            plan,
        })
    }

    pub async fn stage(store: &dyn ObjectStore, selected: CatalogCut, table: &TableRef, keys: &[TrieKey], files: Vec<RetirementFile>) -> Result<Self> {
        let intent = RetirementIntent { selected, table: table.clone(), tries: keys.iter().map(TrieKey::to_string).collect(), files };
        let bytes = Bytes::from(serde_json::to_vec(&intent).map_err(crate::invalid)?);
        if bytes.len() > MAXIMUM_INTENT_BYTES { return Err(crate::invalid("A trie retirement intent exceeds the metadata size limit")); }
        let pending = Self::for_bytes(bytes)?;
        store.put(&pending.intent, pending.encoded.clone()).await?;
        Ok(pending)
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
    let prefix = ObjectKey::new(format!("{}/pending", journal_directory(cut)))?;
    for object in store.list(Some(&prefix)).await? {
        if release.cancellation.is_cancelled() { return Err(crate::closed()); }
        let size = usize::try_from(object.size).ok().filter(|size| *size <= MAXIMUM_INTENT_BYTES)
            .ok_or_else(|| crate::invalid("A trie retirement intent exceeds the metadata size limit"))?;
        let bytes = match store.get_fresh_exact(&object.key, size).await {
            Ok(bytes) => bytes,
            Err(Error::MissingObject(_)) => continue,
            Err(error) => return Err(error.into()),
        };
        let pending = PendingRetirement::for_bytes(bytes)?;
        if pending.intent != object.key || !object.key.is_under(&prefix) {
            return Err(crate::invalid("A trie retirement intent does not match its journal address"));
        }
        if pending.plan.selected.through > cut.through { continue; }
        if let Some(installed) = pending.installed(store).await? {
            if installed <= cut.through { pending.release_intent(store, installed, release).await?; }
            continue;
        }
        let keys = pending.plan.tries.iter().map(|key| key.parse()).collect::<Result<Vec<TrieKey>>>()?;
        if keys.is_empty() || keys.iter().any(|key| key.level() == 0) {
            return Err(crate::invalid("A trie retirement intent must name at least one non-L0 trie and no L0 trie"));
        }
        if keys.iter().all(|key| !view.retains_trie(&pending.plan.table, key)) {
            let receipt = publisher.publish(pending.plan.table.clone(), keys).await?;
            pending.finish(store, receipt, release).await?;
        }
    }
    Ok(())
}
