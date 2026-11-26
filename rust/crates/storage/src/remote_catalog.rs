use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use object_store::{Error as ProviderError, ObjectStore as ProviderStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion, path::Path};
use rtdb_types::{Anomaly, AnomalyCategory, CatalogCut, Result};

use crate::{IoRegion, ObjectKey, local_catalog::{covers, same_storage, well_formed}};

const MAX_LEDGER_BYTES: usize = 1024 * 1024;

#[derive(Default)]
struct Ledger {
    sequence: u64,
    floor: Option<CatalogCut>,
}

/// The retirement floor of a shared object-store root.
/// Readers register nothing here: a superseded file stays readable for the garbage-collection window, so a retirement
/// is released as soon as it is recorded. The floor only refuses a retirement from another storage root or an older
/// journal than one already recorded.
pub(super) struct CatalogLedger {
    client: Arc<dyn ProviderStore>,
    key: ObjectKey,
    path: Path,
    reads: Arc<IoRegion>,
}

impl CatalogLedger {
    pub fn new(client: Arc<dyn ProviderStore>, key: ObjectKey, path: Path) -> Arc<Self> {
        Arc::new(Self { client, key, path, reads: IoRegion::new() })
    }

    pub async fn advance(self: &Arc<Self>, cut: CatalogCut) -> Result<bool> {
        Ok(self.close_admission(cut, &[None]).await?[0])
    }

    /// Records the installed retirement and releases every candidate it names.
    pub async fn close_admission(self: &Arc<Self>, cut: CatalogCut, paths: &[Option<&str>]) -> Result<Vec<bool>> {
        if !well_formed(cut) || cut.through.after_offset(cut.replica_epoch) < 0 {
            return Err(unavailable("Invalid remote catalog floor"));
        }
        self.update(|state| {
            let closed = state.floor.is_some_and(|prior| covers(prior, cut));
            if !closed && state.floor.is_some_and(|prior| {
                !covers(cut, prior) && !(same_storage(cut, prior) && cut.replica_epoch > prior.replica_epoch)
            }) {
                return Err(unavailable("The remote catalog floor belongs to another storage root or a newer journal"));
            }
            if !closed { state.floor = Some(cut); }
            Ok((!closed, vec![true; paths.len()]))
        }).await
    }

    async fn read(self: &Arc<Self>) -> Result<(Ledger, PutMode)> {
        self.reads.read_async(self.clone(), self.key.clone(), |ledger, key| async move {
            Ok(ledger.read_ledger(&key).await)
        }).await?
    }

    async fn read_ledger(&self, key: &ObjectKey) -> Result<(Ledger, PutMode)> {
        let response = match self.client.get(&self.path).await {
            Err(ProviderError::NotFound { .. }) => return Ok((Ledger::default(), PutMode::Create)),
            result => result.map_err(|error| super::provider_error(key, error))?,
        };
        if response.meta.size > MAX_LEDGER_BYTES as u64 {
            return Err(unavailable("The remote catalog ledger exceeds its size limit"));
        }
        let version = UpdateVersion { e_tag: response.meta.e_tag.clone(), version: response.meta.version.clone() };
        if version.e_tag.is_none() && version.version.is_none() {
            return Err(unavailable("The remote catalog ledger has no conditional update identity"));
        }
        let bytes = response.bytes().await.map_err(|error| super::provider_error(key, error))?;
        // A ledger written before reader registration was removed may still list reader holds; nothing reads them.
        let (sequence, floor, _holds): (u64, Option<CatalogCut>, serde::de::IgnoredAny) =
            serde_json::from_slice(&bytes).map_err(unavailable)?;
        if sequence == 0 || floor.is_some_and(|floor| !well_formed(floor)) {
            return Err(unavailable("Malformed remote catalog ledger"));
        }
        Ok((Ledger { sequence, floor }, PutMode::Update(version)))
    }

    async fn update<T>(self: &Arc<Self>, mut change: impl FnMut(&mut Ledger) -> Result<(bool, T)>) -> Result<T> {
        for _ in 0..8 {
            let (mut state, mode) = self.read().await?;
            let (changed, answer) = change(&mut state)?;
            if !changed { return Ok(answer); }
            let sequence = state.sequence.checked_add(1).ok_or_else(|| unavailable("Remote catalog ledger sequence exhausted"))?;
            // The empty hold map keeps the ledger readable by a node that still registers readers.
            let holds: BTreeMap<String, ()> = BTreeMap::new();
            let bytes = serde_json::to_vec(&(sequence, state.floor, holds)).map_err(unavailable)?;
            let options = PutOptions { mode, ..Default::default() };
            match self.client.put_opts(&self.path, Bytes::from(bytes).into(), options).await {
                Ok(_) => return Ok(answer),
                Err(ProviderError::Precondition { .. } | ProviderError::AlreadyExists { .. }) => continue,
                Err(error) => return Err(super::provider_error(&self.key, error).into()),
            }
        }
        Err(unavailable("The remote catalog ledger kept changing"))
    }
}

fn unavailable(message: impl std::fmt::Display) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.storage/remote-catalog-ledger", message.to_string())
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use rtdb_types::{ReplicaPosition, StorageEpoch, StorageVersion};

    use super::*;

    fn cut(through: i64) -> CatalogCut {
        CatalogCut { storage_version: StorageVersion(6), storage_epoch: StorageEpoch(0), replica_epoch: StorageEpoch(0),
            through: ReplicaPosition(through) }
    }

    #[tokio::test]
    async fn legacy_reader_holds_never_block_a_retirement() {
        let client: Arc<dyn ProviderStore> = Arc::new(InMemory::new());
        let path = Path::from("catalog-ledger");
        let legacy = serde_json::json!([3, null, {
            "6f9619ff-8b86-d011-b42d-00c04fc964ff": { "catalog": { "cut": cut(1), "catalog_file_paths": null }, "owner": null }
        }]);
        client.put_opts(&path, Bytes::from(serde_json::to_vec(&legacy).unwrap()).into(), PutOptions::default()).await.unwrap();
        let ledger = CatalogLedger::new(Arc::clone(&client), ObjectKey::new("catalog-ledger").unwrap(), path);
        assert_eq!(ledger.close_admission(cut(5), &[None, Some("tables/x/data/l01-rc-b00.arrow")]).await.unwrap(), [true, true]);
        let (state, _) = ledger.read().await.unwrap();
        assert_eq!((state.sequence, state.floor), (4, Some(cut(5))));
        assert!(ledger.advance(cut(4)).await.unwrap(), "an older retirement is already covered");
        assert!(ledger.close_admission(CatalogCut { storage_epoch: StorageEpoch(1), ..cut(9) }, &[None]).await.is_err());
    }
}
