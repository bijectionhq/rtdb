use std::{ops::AsyncFnOnce, sync::Arc};

use rtdb_crypt::{Kms, Plaintext};
use rtdb_format::journal::{Manifest, ObjectId};
use rtdb_store::{ObjectStore, VerifiedManifest, read_file};
use rtdb_types::{Anomaly, Result};
use uuid::Uuid;

use super::MultiDatabase;

pub struct PinnedExport {
    image: VerifiedManifest,
    store: Arc<dyn ObjectStore>,
    kms: Arc<dyn Kms>,
}

impl PinnedExport {
    pub fn manifest(&self) -> &Manifest {
        self.image.contents()
    }

    pub async fn file(&self, id: ObjectId) -> Result<Plaintext> {
        if !self.image.contents().files.contains(&id) {
            return Err(Anomaly::incorrect(
                "rtdb/unpinned-file",
                "The file is outside the pinned checkpoint",
            ));
        }
        read_file(self.store.as_ref(), self.kms.as_ref(), id).await
    }

    pub async fn check_readable(&self) -> Result<()> {
        self.kms.check_readable(self.image.generation).await
    }
}

impl MultiDatabase {
    pub async fn open_export(&mut self, id: Uuid) -> Result<PinnedExport> {
        let image = self.open_pin(id).await?;
        Ok(PinnedExport {
            image,
            store: Arc::clone(&self.store),
            kms: Arc::clone(&self.stream.kms),
        })
    }

    /// The destination must durably publish its independent backing before returning success.
    pub async fn transfer_fork<T>(
        &mut self,
        id: Uuid,
        persist_destination: impl for<'a> AsyncFnOnce(&'a PinnedExport) -> Result<T>,
    ) -> Result<T> {
        let export = self.open_export(id).await?;
        let destination = persist_destination(&export).await?;
        self.release_pin(id).await?;
        Ok(destination)
    }
}
