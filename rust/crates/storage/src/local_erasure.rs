use std::{fs, io::ErrorKind, path::Path};

use rtdb_types::{
    Anomaly, BlockIndex, CatalogCut, ReplicaPosition, Result, SourcePosition, Timestamp,
};
use serde::{Deserialize, Serialize};

use crate::{Error, IoRegion, LocalStore, ObjectKey, ObjectStore};

const MARKER: &str = ".tmp/catalog-ledger/erasure-recovery";
const OBJECT_BATCH: usize = 128;

#[derive(Clone, Copy, Serialize, Deserialize)]
struct ObjectProgress {
    fence: LocalErasureRecovery,
    count: usize,
    names_sha256: [u8; 32],
    settled: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalErasureRecovery {
    pub block: BlockIndex,
    pub boundary: ReplicaPosition,
    pub source: SourcePosition,
    pub root_sha256: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inventory_sha256: Option<[u8; 32]>,
    pub unreadable_by: Timestamp,
}

impl LocalStore {
    /// Reads the durable lower bound for recovery on this local residence.
    pub async fn erasure_recovery_fence(&self) -> Result<Option<LocalErasureRecovery>> {
        let marker = self.root().join(MARKER);
        self.io_region()
            .run_blocking(move || read(&marker))
            .await
            .map_err(Into::into)
    }

    /// Installs the exact root authorized by an ordered erasure before log key admission changes.
    /// Returns false while another operation holds the local recovery gate.
    pub async fn close_erasure_recovery(&self, next: LocalErasureRecovery) -> Result<bool> {
        if !valid(next) || !self.defers_trie_releases() {
            return Err(invalid().into());
        }
        let marker = self.root().join(MARKER);
        let Some(gate) = self.try_lock_recovery_deletion().await? else {
            return Ok(false);
        };
        IoRegion::retaining(
            gate,
            self.io_region().run_blocking(move || {
                let dir = marker.parent().ok_or_else(invalid)?;
                if let Some(prior) = read(&marker)? {
                    if prior == next {
                        crate::sync_directory(dir).map_err(|error| Error::io(dir, error))?;
                        return Ok(true);
                    }
                    if prior.block >= next.block
                        || prior.boundary.epoch() != next.boundary.epoch()
                        || prior.boundary >= next.boundary
                        || prior.source.epoch() != next.source.epoch()
                        || prior.source >= next.source
                    {
                        return Err(invalid());
                    }
                }
                write_synced(&marker, &next)?;
                Ok(true)
            }),
        )
        .await
        .map_err(Into::into)
    }

    pub async fn erasure_objects_complete(
        &self,
        fence: LocalErasureRecovery,
        objects: &[ObjectKey],
    ) -> Result<bool> {
        if fence.inventory_sha256.is_none() || self.erasure_recovery_fence().await? != Some(fence) {
            return Ok(false);
        }
        let path = self.object_progress_path(fence);
        let count = objects.len();
        let names_sha256 = object_names_sha256(objects);
        Ok(self
            .io_region()
            .run_blocking(move || {
                Ok(read_object_progress(&path, fence, count, names_sha256)?
                    .is_some_and(|progress| progress.settled == count))
            })
            .await?)
    }

    fn object_progress_path(&self, fence: LocalErasureRecovery) -> std::path::PathBuf {
        self.root().join(format!(
            ".tmp/catalog-ledger/erasure-objects-{}",
            fence.block.0
        ))
    }

    /// Settles one batch from the inventory authenticated by the exact recovery floor.
    /// The caller has already settled both log prefixes and supplied the original reader deadline.
    pub async fn settle_erasure_objects(
        &self,
        store: &dyn ObjectStore,
        fence: LocalErasureRecovery,
        cut: CatalogCut,
        objects: &[ObjectKey],
        now_lower: Timestamp,
    ) -> Result<bool> {
        let keys = store.local_classic_key_slots().ok_or_else(invalid)?;
        if fence.inventory_sha256.is_none()
            || cut.through != fence.boundary
            || cut.replica_epoch != fence.boundary.epoch()
            || store
                .local_maintenance_store()
                .is_none_or(|local| !std::ptr::eq(local, self))
            || !objects.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(invalid().into());
        }
        let Some(gate) = self.try_lock_recovery_deletion().await? else {
            return Ok(false);
        };
        IoRegion::retaining(gate, async {
            if self.erasure_recovery_fence().await? != Some(fence) {
                return Err(invalid().into());
            }
            let drained = self.close_erasure_catalog(cut).await?;
            if !drained || now_lower < fence.unreadable_by {
                return Ok(false);
            }
            let path = self.object_progress_path(fence);
            let count = objects.len();
            let names_sha256 = object_names_sha256(objects);
            let mut progress = self
                .io_region()
                .run_blocking({
                    let path = path.clone();
                    move || open_object_progress(&path, fence, count, names_sha256)
                })
                .await?;
            let end = progress.settled.saturating_add(OBJECT_BATCH).min(count);
            let batch = &objects[progress.settled..end];
            keys.withdraw_all(batch).await?;
            for key in batch {
                store.delete(key).await?;
                store.forget(key).await?;
            }
            progress.settled = end;
            self.io_region()
                .run_blocking(move || write_synced(&path, &progress))
                .await?;
            Ok(end == count)
        })
        .await
    }
}

impl LocalErasureRecovery {
    /// Admits a recovery catalog at the exact fenced block or a successor in the same logs.
    pub fn require(
        &self,
        block: BlockIndex,
        source: Option<SourcePosition>,
        boundary: Option<ReplicaPosition>,
        digest: Option<[u8; 32]>,
    ) -> Result<()> {
        let covered = block >= self.block
            && source.is_some_and(|source| {
                source.epoch() == self.source.epoch() && source >= self.source
            })
            && (if block == self.block {
                boundary == Some(self.boundary)
                    && digest == Some(self.root_sha256)
                    && source == Some(self.source)
            } else {
                boundary.is_some_and(|boundary| {
                    boundary.epoch() == self.boundary.epoch() && boundary > self.boundary
                })
            });
        if covered { Ok(()) } else { Err(erased()) }
    }
}

pub(crate) fn erased() -> Anomaly {
    Anomaly::new(
        rtdb_types::AnomalyCategory::NotFound,
        "rtdb/erased-under-view",
        "The selected recovery block precedes the local erasure fence",
    )
}

fn read(path: &Path) -> crate::Result<Option<LocalErasureRecovery>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::io(path, error)),
    };
    let fence = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if !valid(fence) {
        return Err(invalid());
    }
    Ok(Some(fence))
}

fn object_names_sha256(objects: &[ObjectKey]) -> [u8; 32] {
    use ring::digest::{Context, SHA256};
    let mut digest = Context::new(&SHA256);
    digest.update(b"rtdb.default.retired-objects/v1\0");
    for key in objects {
        digest.update(&(key.as_str().len() as u64).to_be_bytes());
        digest.update(key.as_str().as_bytes());
    }
    digest.finish().as_ref().try_into().expect("SHA-256 digest")
}

fn open_object_progress(
    path: &Path,
    fence: LocalErasureRecovery,
    count: usize,
    names_sha256: [u8; 32],
) -> crate::Result<ObjectProgress> {
    if let Some(progress) = read_object_progress(path, fence, count, names_sha256)? {
        return Ok(progress);
    }
    let progress = ObjectProgress {
        fence,
        count,
        names_sha256,
        settled: 0,
    };
    write_synced(path, &progress)?;
    Ok(progress)
}

fn read_object_progress(
    path: &Path,
    fence: LocalErasureRecovery,
    count: usize,
    names_sha256: [u8; 32],
) -> crate::Result<Option<ObjectProgress>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::io(path, error)),
    };
    let progress: ObjectProgress = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if progress.fence != fence
        || progress.count != count
        || progress.names_sha256 != names_sha256
        || progress.settled > count
    {
        return Err(invalid());
    }
    Ok(Some(progress))
}

fn write_synced(path: &Path, value: &impl Serialize) -> crate::Result<()> {
    let dir = path.parent().ok_or_else(invalid)?;
    let mut file = tempfile::NamedTempFile::new_in(dir).map_err(|error| Error::io(dir, error))?;
    serde_json::to_writer(&mut file, value).map_err(|_| invalid())?;
    crate::sync_file(file.as_file()).map_err(|error| Error::io(path, error))?;
    file.persist(path)
        .map_err(|error| Error::io(path, error.error))?;
    crate::sync_directory(dir).map_err(|error| Error::io(dir, error))
}

fn valid(fence: LocalErasureRecovery) -> bool {
    fence.block.0 >= 0 && fence.boundary.0 >= 0 && fence.source.0 >= 0
}

fn invalid() -> Error {
    Error::Task("Invalid local erasure recovery fence".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reopening_the_local_store_preserves_the_exact_checkpoint_fence() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let open = || {
            Ok::<_, Anomaly>(
                LocalStore::open(directory.path())?
                    .with_catalog_ledger(rtdb_types::StorageEpoch(0)),
            )
        };
        let store = open()?;
        let fence = LocalErasureRecovery {
            block: BlockIndex(3),
            boundary: ReplicaPosition(30),
            source: SourcePosition(20),
            root_sha256: [4; 32],
            inventory_sha256: Some([5; 32]),
            unreadable_by: Timestamp(55),
        };
        let hold = store.hold_recovery_open().await?;
        assert!(!store.close_erasure_recovery(fence).await?);
        drop(hold);
        assert!(store.close_erasure_recovery(fence).await?);
        let reopened = open()?;
        assert_eq!(reopened.erasure_recovery_fence().await?, Some(fence));
        fence.require(
            fence.block,
            Some(fence.source),
            Some(fence.boundary),
            Some(fence.root_sha256),
        )?;
        assert!(
            fence
                .require(
                    fence.block,
                    Some(fence.source),
                    Some(fence.boundary),
                    Some([5; 32])
                )
                .is_err()
        );
        assert!(
            reopened
                .close_erasure_recovery(LocalErasureRecovery {
                    unreadable_by: Timestamp(56),
                    ..fence
                })
                .await
                .is_err()
        );
        Ok(())
    }
    #[tokio::test]
    async fn object_retirement_drains_old_catalogs_and_resumes_the_exact_batch() -> Result<()> {
        use crate::sealing::ClassicKeyVault;
        use bytes::Bytes;
        use rtdb_types::{StorageEpoch, StorageVersion};
        use std::sync::Arc;

        let files = tempfile::tempdir().unwrap();
        let vault_root = tempfile::tempdir().unwrap();
        let vault = ClassicKeyVault::open(vault_root.path())?;
        let epoch = StorageEpoch(0);
        let open = || {
            LocalStore::open(files.path()).map(|store| Arc::new(store.with_catalog_ledger(epoch)))
        };
        let raw = open()?;
        let keys = vault
            .units(raw.as_ref(), Arc::clone(raw.io_region()), true)
            .await?;
        let store = ClassicKeyVault::wrap(raw.clone(), keys)?;
        let fence = LocalErasureRecovery {
            block: BlockIndex(3),
            source: SourcePosition(20),
            boundary: ReplicaPosition(30),
            root_sha256: [4; 32],
            inventory_sha256: Some([5; 32]),
            unreadable_by: Timestamp(55),
        };
        let cut = CatalogCut {
            storage_version: StorageVersion(1),
            storage_epoch: epoch,
            replica_epoch: epoch,
            through: fence.boundary,
        };
        let prior = CatalogCut {
            through: ReplicaPosition(12),
            ..cut
        };
        let objects = (0..=OBJECT_BATCH)
            .map(|index| ObjectKey::new(format!("tables/items/old-{index:04}")))
            .collect::<crate::Result<Vec<_>>>()?;
        let survivor = ObjectKey::new("tables/items/clean")?;
        let bytes = Bytes::from_static(b"sealed data");
        for key in objects.iter().chain([&survivor]) {
            store.put(key, bytes.clone()).await?;
        }
        let held = store.retain_catalog(Some(prior)).await?.unwrap();
        assert!(raw.close_erasure_recovery(fence).await?);
        assert!(
            !raw.settle_erasure_objects(&store, fence, cut, &objects, Timestamp(55))
                .await?
        );
        assert!(store.retain_catalog(Some(prior)).await.is_err());
        assert!(store.retain_catalog(Some(cut)).await?.is_some());
        assert_eq!(store.get_fresh(&objects[0]).await?, bytes);
        drop(held);
        assert!(
            !raw.settle_erasure_objects(&store, fence, cut, &objects, Timestamp(54))
                .await?
        );
        assert!(
            !raw.settle_erasure_objects(&store, fence, cut, &objects, Timestamp(55))
                .await?
        );
        assert!(raw.head_fresh(&objects[0]).await.is_err());
        assert_eq!(store.get_fresh(objects.last().unwrap()).await?, bytes);

        drop(store);
        drop(raw);
        let reopened = open()?;
        let keys = vault
            .units(reopened.as_ref(), Arc::clone(reopened.io_region()), false)
            .await?;
        let store = ClassicKeyVault::wrap(reopened.clone(), keys)?;
        let mut different = objects.clone();
        different[0] = ObjectKey::new("tables/items/foreign")?;
        assert!(
            reopened
                .settle_erasure_objects(&store, fence, cut, &different, Timestamp(55))
                .await
                .is_err()
        );
        assert!(
            reopened
                .settle_erasure_objects(&store, fence, cut, &objects, Timestamp(55))
                .await?
        );
        assert!(
            store
                .put(objects.last().unwrap(), bytes.clone())
                .await
                .is_err()
        );
        assert_eq!(store.get_fresh(&survivor).await?, bytes);
        Ok(())
    }
}
