//! Independent key slots for each directory-managed journal publication.

use std::sync::Arc;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use ring::{
    digest::{Context, SHA256},
    rand::{SecureRandom, SystemRandom},
};
use rtdb_storage::{Error, IoRegion, Result, log::LogRecord};
use rtdb_types::Result as DomainResult;

use crate::keys::{ArchiveKey, KeyService};

type Publication = [u8; 32];
const PUBLICATION_BYTES: usize = size_of::<Publication>();
pub(super) const OVERHEAD: usize = PUBLICATION_BYTES + crate::cipher::SEALED_OVERHEAD;

pub(crate) struct JournalUnits {
    scope: Arc<str>,
    keys: Arc<dyn ArchiveKey>,
}

impl JournalUnits {
    pub(crate) async fn open(
        keys: &dyn KeyService,
        residence: &str,
        scope: &str,
        io: Arc<IoRegion>,
    ) -> DomainResult<Self> {
        Ok(Self {
            scope: Arc::from(scope),
            keys: keys.scoped_unit_key(residence, io).await?,
        })
    }

    fn associated(&self, publication: &Publication) -> String {
        format!(
            "rtdb.multi/journal-unit/v1/{}/{}",
            self.scope,
            URL_SAFE_NO_PAD.encode(publication)
        )
    }

    pub(super) async fn seal(&self, plaintext: Bytes) -> Result<Bytes> {
        let mut publication = [0; PUBLICATION_BYTES];
        SystemRandom::new()
            .fill(&mut publication)
            .map_err(|_| Error::Task("Cannot allocate a journal publication identity".into()))?;
        self.seal_unit(publication, plaintext).await
    }

    /// Replaying an interrupted certified copy reproduces the same publication.
    pub(crate) async fn seed(&self, offset: i64, plaintext: Bytes) -> Result<Bytes> {
        let mut hash = Context::new(&SHA256);
        hash.update(b"rtdb.multi/journal-seed/v1\0");
        hash.update(&offset.to_be_bytes());
        hash.update(&plaintext);
        let publication = hash.finish().as_ref().try_into().expect("SHA-256 width");
        self.seal_unit(publication, plaintext).await
    }

    async fn seal_unit(&self, publication: Publication, plaintext: Bytes) -> Result<Bytes> {
        let ciphertext = self
            .keys
            .seal(&self.associated(&publication), plaintext)
            .await?;
        let mut frame = Vec::with_capacity(PUBLICATION_BYTES + ciphertext.len());
        frame.extend_from_slice(&publication);
        frame.extend_from_slice(&ciphertext);
        Ok(frame.into())
    }

    pub(crate) async fn decrypt(&self, sealed: Bytes) -> Result<Bytes> {
        let publication = sealed
            .get(..PUBLICATION_BYTES)
            .and_then(|prefix| prefix.try_into().ok())
            .ok_or_else(|| {
                Error::Task("A sealed journal frame has no publication identity".into())
            })?;
        self.keys
            .open(
                &self.associated(&publication),
                sealed.slice(PUBLICATION_BYTES..),
            )
            .await
    }

    pub(super) async fn open_record(&self, record: LogRecord) -> Result<LogRecord> {
        struct Opened {
            plaintext: Bytes,
            _charged_frame: Bytes,
        }
        impl AsRef<[u8]> for Opened {
            fn as_ref(&self) -> &[u8] {
                &self.plaintext
            }
        }

        let plaintext = self.decrypt(record.payload.clone()).await?;
        let payload = Bytes::from_owner(Opened {
            plaintext,
            _charged_frame: record.payload,
        });
        Ok(LogRecord { payload, ..record })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::LocalKeyService;

    #[tokio::test]
    async fn a_reseeded_frame_uses_only_its_destination_slot() -> DomainResult<()> {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path())?;
        keys.key("predecessor").await?;
        keys.key("successor").await?;
        let io = IoRegion::new();
        let source = JournalUnits::open(&keys, "predecessor", "replica", Arc::clone(&io)).await?;
        let target = JournalUnits::open(&keys, "successor", "replica", io).await?;
        let body = Bytes::from_static(b"retained journal record");
        let original = source.seal(body.clone()).await?;
        let copy = target
            .seed(7, source.decrypt(original.clone()).await?)
            .await?;
        assert_eq!(target.seed(7, body.clone()).await?, copy);
        assert_ne!(target.seed(8, body.clone()).await?, copy);
        assert!(target.decrypt(original).await.is_err());
        keys.destroy("predecessor").await?;
        assert_eq!(target.decrypt(copy).await?, body);
        Ok(())
    }
}
