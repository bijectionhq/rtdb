use bytes::Bytes;
use rtdb_crypt::{Context, Generation, Kms, Plaintext};
use rtdb_format::journal::{Manifest, ObjectId, ObjectKind};
use rtdb_types::{Anomaly, Result};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{ObjectKey, ObjectStore};

/// Owns allocation for one newly acquired database writer epoch.
/// The writer creates this once after acquiring the epoch and acquires a higher epoch on restart.
pub struct WriterObjects {
    database: Uuid,
    epoch: u64,
    next: Option<u64>,
}

impl WriterObjects {
    pub fn new(database: Uuid, epoch: u64) -> Self {
        Self {
            database,
            epoch,
            next: Some(0),
        }
    }

    pub async fn file(
        &mut self,
        kms: &dyn Kms,
        generation: u64,
        data: &[u8],
    ) -> Result<PreparedObject> {
        self.prepare(kms, generation, ObjectKind::File, data).await
    }

    pub async fn manifest(
        &mut self,
        kms: &dyn Kms,
        generation: u64,
        manifest: &Manifest,
    ) -> Result<PreparedObject> {
        if manifest
            .files
            .iter()
            .chain(manifest.pins.iter().map(|pin| &pin.manifest))
            .any(|id| id.database != self.database || id.epoch > self.epoch)
        {
            return Err(Anomaly::incorrect(
                "rtdb.store/manifest-scope",
                "Manifest references an object outside the writer scope",
            ));
        }
        self.prepare(kms, generation, ObjectKind::Manifest, &manifest.encode()?)
            .await
    }

    pub(super) fn database(&self) -> Uuid {
        self.database
    }

    async fn prepare(
        &mut self,
        kms: &dyn Kms,
        generation: u64,
        kind: ObjectKind,
        data: &[u8],
    ) -> Result<PreparedObject> {
        let seq = self.next.ok_or_else(|| {
            Anomaly::fault(
                "rtdb.store/sequence-exhausted",
                "Writer object sequence exhausted",
            )
        })?;
        self.next = seq.checked_add(1);
        let mut id = ObjectId {
            database: self.database,
            kind,
            epoch: self.epoch,
            seq,
            digest: [0; 32],
        };
        let generation = Generation {
            database: self.database,
            number: generation,
        };
        let bytes = rtdb_crypt::seal(kms, generation, context(id), data).await?;
        id.digest = Sha256::digest(&bytes).into();
        Ok(PreparedObject {
            id,
            bytes: Bytes::from(bytes),
        })
    }
}

/// An allocation and its immutable bytes, reusable for a retry of the same put.
pub struct PreparedObject {
    id: ObjectId,
    bytes: Bytes,
}

impl PreparedObject {
    pub fn id(&self) -> ObjectId {
        self.id
    }

    pub async fn put(&self, store: &dyn ObjectStore) -> Result<()> {
        let key = key(self.id)?;
        store
            .put(&key, self.bytes.clone())
            .await
            .map_err(Anomaly::from)
    }
}

/// A manifest whose ciphertext digest and context were verified on read.
pub struct VerifiedManifest {
    pub(super) id: ObjectId,
    pub(super) manifest: Manifest,
    pub generation: Generation,
}

impl VerifiedManifest {
    pub fn id(&self) -> ObjectId {
        self.id
    }

    pub fn contents(&self) -> &Manifest {
        &self.manifest
    }
}

pub async fn read_file(store: &dyn ObjectStore, kms: &dyn Kms, id: ObjectId) -> Result<Plaintext> {
    require_kind(id, ObjectKind::File)?;
    read(store, kms, id).await
}

pub async fn read_manifest(
    store: &dyn ObjectStore,
    kms: &dyn Kms,
    id: ObjectId,
) -> Result<VerifiedManifest> {
    require_kind(id, ObjectKind::Manifest)?;
    let plaintext = read(store, kms, id).await?;
    Ok(VerifiedManifest {
        id,
        generation: plaintext.generation,
        manifest: Manifest::decode(&plaintext.bytes)?,
    })
}

async fn read(store: &dyn ObjectStore, kms: &dyn Kms, id: ObjectId) -> Result<Plaintext> {
    let bytes = store.get(&key(id)?).await.map_err(Anomaly::from)?;
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    if id.digest != digest {
        return Err(Anomaly::fault(
            "rtdb.store/object-digest",
            format!("Digest mismatch for object {id}"),
        ));
    }
    rtdb_crypt::open(kms, id.database, context(id), &bytes).await
}

pub(super) fn key(id: ObjectId) -> Result<ObjectKey> {
    ObjectKey::new(id.to_string()).map_err(Anomaly::from)
}

pub(super) fn require_kind(id: ObjectId, kind: ObjectKind) -> Result<()> {
    if id.kind != kind {
        return Err(Anomaly::fault(
            "rtdb.store/object-kind",
            format!("Unexpected object kind for {id}"),
        ));
    }
    Ok(())
}

fn context(id: ObjectId) -> Context {
    let ObjectId {
        kind, epoch, seq, ..
    } = id;
    match kind {
        ObjectKind::File => Context::File { epoch, seq },
        ObjectKind::Manifest => Context::Manifest { epoch, seq },
    }
}

#[cfg(test)]
mod tests {
    use rtdb_crypt::LocalKms;

    use super::*;
    use crate::MemoryStore;

    #[tokio::test]
    async fn objects_allocate_fresh_slots_and_verify_ciphertext_before_opening() {
        let store = MemoryStore::default();
        let kms = LocalKms::new(|| 0);
        let database = Uuid::from_u128(5);
        let mut writer = WriterObjects::new(database, 1);
        kms.create(Generation {
            database,
            number: 0,
        })
        .await
        .unwrap();

        let first = writer.file(&kms, 0, b"data").await.unwrap();
        let second = writer.file(&kms, 0, b"data").await.unwrap();
        assert_eq!((first.id().epoch, first.id().seq), (1, 0));
        assert_eq!((second.id().epoch, second.id().seq), (1, 1));
        first.put(&store).await.unwrap();
        first.put(&store).await.unwrap();
        second.put(&store).await.unwrap();
        assert_eq!(
            &*read_file(&store, &kms, first.id()).await.unwrap().bytes,
            b"data"
        );

        let mut successor = WriterObjects::new(database, 2);
        let third = successor.file(&kms, 0, b"data").await.unwrap();
        assert_eq!((third.id().epoch, third.id().seq), (2, 0));
        assert_ne!(third.id(), first.id());

        let key = key(first.id()).unwrap();
        store.delete(&key).await.unwrap();
        store.put(&key, second.bytes).await.unwrap();
        assert!(matches!(
            read_file(&store, &kms, first.id()).await,
            Err(error) if error.code == "rtdb.store/object-digest"
        ));
    }

    #[tokio::test]
    async fn objects_never_wrap_the_sequence_counter() {
        let database = Uuid::from_u128(5);
        let mut writer = WriterObjects {
            database,
            epoch: 1,
            next: Some(u64::MAX),
        };
        let kms = LocalKms::new(|| 0);
        kms.create(Generation {
            database,
            number: 0,
        })
        .await
        .unwrap();

        assert_eq!(writer.file(&kms, 0, b"a").await.unwrap().id().seq, u64::MAX);
        assert!(matches!(
            writer.file(&kms, 0, b"b").await,
            Err(error) if error.code == "rtdb.store/sequence-exhausted"
        ));
    }
}
