use bytes::{BufMut, Bytes, BytesMut};
use rtdb_format::keys::{TRIE_RETIREMENT_DIRECTORY, TrieKey};
use rtdb_index::PartitionValue;
use rtdb_storage::{Error, ObjectKey, ObjectStore};
use rtdb_types::{CatalogCut, ReplicaPosition, Result, TableRef};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::TrieDeletionPublisher;

const MAXIMUM_INTENT_BYTES: usize = 1 << 20;

#[derive(Serialize, Deserialize)]
struct Intent {
    selected: CatalogCut,
    table: TableRef,
    tries: Vec<String>,
}

pub(crate) struct PendingRetirement {
    intent: ObjectKey,
    receipt: ObjectKey,
    selected: CatalogCut,
    encoded: Bytes,
}

fn journal_prefix(cut: CatalogCut) -> String {
    format!("{TRIE_RETIREMENT_DIRECTORY}/{}-{}-{}", cut.storage_version.0, cut.storage_epoch.0, cut.replica_epoch.0)
}

impl PendingRetirement {
    fn for_bytes(selected: CatalogCut, encoded: Bytes) -> Result<Self> {
        let prefix = journal_prefix(selected);
        let id = Sha256::digest(&encoded);
        Ok(Self {
            intent: ObjectKey::new(format!("{prefix}/pending/{id:x}"))?,
            receipt: ObjectKey::new(format!("{prefix}/installed/{id:x}"))?,
            selected,
            encoded,
        })
    }

    pub async fn stage(store: &dyn ObjectStore, selected: CatalogCut, table: &TableRef, keys: &[TrieKey]) -> Result<Self> {
        let intent = Intent { selected, table: table.clone(), tries: keys.iter().map(TrieKey::to_string).collect() };
        let bytes = Bytes::from(serde_json::to_vec(&intent).map_err(crate::invalid)?);
        if bytes.len() > MAXIMUM_INTENT_BYTES { return Err(crate::invalid("A trie retirement intent exceeds the metadata size limit")); }
        let pending = Self::for_bytes(selected, bytes)?;
        store.put(&pending.intent, pending.encoded.clone()).await?;
        Ok(pending)
    }

    fn validate_receipt(&self, position: ReplicaPosition) -> Result<()> {
        if !receipt_follows_selection(self.selected.through.0, self.selected.replica_epoch.0, position.0, position.epoch().0) {
            return Err(crate::invalid("The trie removal receipt is outside its selection's journal or does not advance it"));
        }
        Ok(())
    }

    async fn installed(&self, store: &dyn ObjectStore) -> Result<bool> {
        match store.get_fresh_exact(&self.receipt, 8 + self.encoded.len()).await {
            Ok(receipt) => {
                let (position, selected) = receipt.split_at_checked(8)
                    .ok_or_else(|| crate::invalid("A trie removal receipt has no installed position"))?;
                if selected != self.encoded.as_ref() { return Err(crate::invalid("A trie removal receipt names a different intent")); }
                let position = position.try_into().map_err(|_| crate::invalid("A trie removal receipt position must be eight bytes"))?;
                self.validate_receipt(ReplicaPosition(i64::from_be_bytes(position)))?;
                Ok(true)
            }
            Err(Error::MissingObject(_)) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn finish(self, store: &dyn ObjectStore, position: ReplicaPosition) -> Result<()> {
        self.validate_receipt(position)?;
        let mut receipt = BytesMut::with_capacity(8 + self.encoded.len());
        receipt.put_i64(position.0);
        receipt.extend_from_slice(&self.encoded);
        if let Err(error) = store.put(&self.receipt, receipt.freeze()).await {
            if !self.installed(store).await? { return Err(error.into()); }
        }
        store.delete(&self.intent).await?;
        Ok(())
    }
}

pub(crate) async fn resume(
    store: &dyn ObjectStore, publisher: &dyn TrieDeletionPublisher, view: &PartitionValue,
    cut: CatalogCut, cancellation: &CancellationToken,
) -> Result<()> {
    let prefix = ObjectKey::new(format!("{}/pending", journal_prefix(cut)))?;
    for object in store.list(Some(&prefix)).await? {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let size = usize::try_from(object.size).ok().filter(|size| *size <= MAXIMUM_INTENT_BYTES)
            .ok_or_else(|| crate::invalid("A trie retirement intent exceeds the metadata size limit"))?;
        let bytes = match store.get_fresh_exact(&object.key, size).await {
            Ok(bytes) => bytes,
            Err(Error::MissingObject(_)) => continue,
            Err(error) => return Err(error.into()),
        };
        let intent: Intent = serde_json::from_slice(&bytes).map_err(crate::invalid)?;
        let pending = PendingRetirement::for_bytes(intent.selected, bytes)?;
        if pending.intent != object.key || !object.key.is_under(&prefix) {
            return Err(crate::invalid("A trie retirement intent does not match its journal address"));
        }
        if intent.selected.through > cut.through { continue; }
        if pending.installed(store).await? {
            store.delete(&pending.intent).await?;
            continue;
        }
        let keys = intent.tries.iter().map(|key| key.parse()).collect::<Result<Vec<TrieKey>>>()?;
        if keys.is_empty() || keys.iter().any(|key| key.level() == 0) {
            return Err(crate::invalid("A trie retirement intent must name at least one non-L0 trie and no L0 trie"));
        }
        if keys.iter().all(|key| !view.retains_trie(&intent.table, key)) {
            let receipt = publisher.publish(intent.table, keys).await?;
            pending.finish(store, receipt).await?;
        }
    }
    Ok(())
}

#[path = "retirement_intent_step.rs"]
mod step;
use step::receipt_follows_selection;
