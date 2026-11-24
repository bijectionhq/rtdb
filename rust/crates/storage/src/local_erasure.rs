use std::{fs, io::ErrorKind, path::Path};

use rtdb_types::{Anomaly, BlockIndex, ReplicaPosition, Result, SourcePosition, Timestamp};
use serde::{Deserialize, Serialize};

use crate::{Error, IoRegion, LocalStore, ObjectStore};

const MARKER: &str = ".tmp/catalog-ledger/erasure-recovery";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalErasureRecovery {
    pub block: BlockIndex,
    pub boundary: ReplicaPosition,
    pub source: SourcePosition,
    pub root_sha256: [u8; 32],
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
                let mut file =
                    tempfile::NamedTempFile::new_in(dir).map_err(|error| Error::io(dir, error))?;
                serde_json::to_writer(&mut file, &next).map_err(|_| invalid())?;
                crate::sync_file(file.as_file()).map_err(|error| Error::io(&marker, error))?;
                file.persist(&marker)
                    .map_err(|error| Error::io(&marker, error.error))?;
                crate::sync_directory(dir).map_err(|error| Error::io(dir, error))?;
                Ok(true)
            }),
        )
        .await
        .map_err(Into::into)
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
}
