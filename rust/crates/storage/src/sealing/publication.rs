use std::sync::Arc;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::{Bytes, BytesMut};
use rtdb_types::ServingPublication;
use uuid::Uuid;

use super::{FramedObject, ObjectCipher, PlaintextView, UnitOperation, UnitOutput};
use crate::{Error, ObjectKey, Result, RetainedBytes};

pub const PUBLICATION_ID_BYTES: usize = ServingPublication::BYTES;

/// Selects the original key slot from an immutable object's serving envelope.
/// The origin travels with the bytes when a pack or fork reference moves.
#[derive(Clone)]
pub struct PublicationSealing {
    ledger: Arc<dyn ObjectCipher>,
    origin: [u8; 32],
    archive: Option<Arc<dyn ObjectCipher>>,
    applies: fn(&ObjectKey) -> bool,
    #[cfg(feature = "remote")]
    attempts: Option<(Arc<dyn crate::remote::PublicationJournal>, String)>,
}

impl PublicationSealing {
    pub fn new(
        ledger: Arc<dyn ObjectCipher>,
        origin: [u8; 32],
        applies: fn(&ObjectKey) -> bool,
    ) -> Self {
        assert_ne!(origin, [0; 32]);
        Self {
            ledger,
            origin,
            archive: None,
            applies,
            #[cfg(feature = "remote")]
            attempts: None,
        }
    }

    #[cfg(feature = "remote")]
    pub fn with_attempts(
        mut self,
        journal: Arc<dyn crate::remote::PublicationJournal>,
        prefix: String,
    ) -> Self {
        self.attempts = Some((journal, prefix));
        self
    }

    pub fn with_archive(mut self, archive: Arc<dyn ObjectCipher>) -> Self {
        self.archive = Some(archive);
        self
    }

    pub fn applies(&self, name: &ObjectKey) -> bool {
        (self.applies)(name)
    }

    pub(super) fn ledger(&self) -> Arc<dyn ObjectCipher> {
        Arc::clone(&self.ledger)
    }

    fn envelope(publication: ServingPublication, sealed: Bytes) -> Bytes {
        let mut frame = BytesMut::with_capacity(PUBLICATION_ID_BYTES + sealed.len());
        frame.extend_from_slice(&publication.prefix());
        frame.extend_from_slice(&sealed);
        frame.freeze()
    }

    /// An admitted read-only overlay exposes the original archive ciphertext.
    pub fn archived(sealed: Bytes) -> Bytes {
        Self::envelope(ServingPublication::ARCHIVE, sealed)
    }

    fn identity(name: &ObjectKey, bytes: &[u8]) -> Result<ServingPublication> {
        ServingPublication::from_prefix(bytes)
            .ok_or_else(|| Error::ConflictingObject(name.to_string()))
    }

    pub(super) async fn operate(
        &self,
        name: &ObjectKey,
        publication: ServingPublication,
        operation: UnitOperation,
    ) -> Result<UnitOutput> {
        let read = !operation.provisions();
        let (authority, slot, operation) = if publication == ServingPublication::ARCHIVE {
            let authority = self
                .archive
                .as_ref()
                .ok_or_else(|| Error::Closed("The original archive is not admitted".into()))?;
            (authority, name.clone(), operation)
        } else {
            let slot = ObjectKey::new(format!(
                "publications/{}/{}/{name}",
                URL_SAFE_NO_PAD.encode(publication.origin),
                publication.attempt
            ))?;
            (
                &self.ledger,
                slot,
                UnitOperation::AtLogicalName(name.clone(), Box::new(operation)),
            )
        };
        let output = authority.operate(&slot, operation).await?;
        if read {
            PlaintextView::track(authority, &slot)?;
        }
        Ok(output)
    }

    pub async fn seal(&self, name: &ObjectKey, plaintext: Bytes) -> Result<Bytes> {
        let publication = ServingPublication {
            origin: self.origin,
            attempt: Uuid::new_v4(),
        };
        #[cfg(feature = "remote")]
        if let Some((journal, prefix)) = &self.attempts {
            journal
                .allocate_unit(
                    &ObjectKey::new(format!("{prefix}/{name}"))?,
                    name,
                    publication,
                )
                .await?;
        }
        let sealed = self
            .operate(name, publication, UnitOperation::Seal(plaintext))
            .await?
            .bytes()?;
        Ok(Self::envelope(publication, sealed))
    }

    pub async fn open(&self, name: &ObjectKey, sealed: Bytes) -> Result<Bytes> {
        let publication = Self::identity(name, &sealed)?;
        self.operate(
            name,
            publication,
            UnitOperation::Open(sealed.slice(PUBLICATION_ID_BYTES..)),
        )
        .await?
        .bytes()
    }

    pub(super) async fn open_retained(
        &self,
        name: &ObjectKey,
        sealed: RetainedBytes,
    ) -> Result<Bytes> {
        let publication = Self::identity(name, sealed.as_slice())?;
        self.operate(
            name,
            publication,
            UnitOperation::OpenRetained(sealed.discard_prefix(PUBLICATION_ID_BYTES)),
        )
        .await?
        .bytes()
    }

    pub async fn header(
        &self,
        name: &ObjectKey,
        header: Bytes,
    ) -> Result<(FramedObject, ServingPublication)> {
        let publication = Self::identity(name, &header)?;
        let layout = self
            .operate(
                name,
                publication,
                UnitOperation::Header(header.slice(PUBLICATION_ID_BYTES..)),
            )
            .await?
            .header()?;
        Ok((layout, publication))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_identity_and_serving_attempts_use_disjoint_frames() {
        let original = ServingPublication {
            origin: [5; 32],
            attempt: Uuid::new_v4(),
        };
        let retry = ServingPublication {
            attempt: Uuid::new_v4(),
            ..original
        };
        assert_eq!(
            ServingPublication::from_prefix(&original.prefix()),
            Some(original)
        );
        assert_eq!(
            ServingPublication::from_prefix(&retry.prefix()),
            Some(retry)
        );
        assert_ne!(original.prefix(), retry.prefix());
        assert_eq!(
            ServingPublication::from_prefix(&ServingPublication::ARCHIVE.prefix()),
            Some(ServingPublication::ARCHIVE)
        );
        assert!(ServingPublication::from_prefix(&original.origin).is_none());
    }
}
