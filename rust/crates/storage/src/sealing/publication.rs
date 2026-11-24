use std::sync::Arc;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::{Bytes, BytesMut};
use rtdb_types::ServingPublication;
use uuid::Uuid;

use super::{
    FramedObject, ObjectCipher, PlaintextView, UnitOperation, UnitOutput, object::FramedFile,
};
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

    fn bind(
        &self,
        name: &ObjectKey,
        publication: ServingPublication,
        operation: UnitOperation,
    ) -> Result<(&Arc<dyn ObjectCipher>, ObjectKey, UnitOperation)> {
        Ok(if publication == ServingPublication::ARCHIVE {
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
        })
    }

    pub(super) async fn operate(
        &self,
        name: &ObjectKey,
        publication: ServingPublication,
        operation: UnitOperation,
    ) -> Result<UnitOutput> {
        let read = !operation.provisions();
        let (authority, slot, operation) = self.bind(name, publication, operation)?;
        let output = authority.operate(&slot, operation).await?;
        if read {
            PlaintextView::track(authority, &slot)?;
        }
        Ok(output)
    }

    async fn allocate(&self, name: &ObjectKey) -> Result<ServingPublication> {
        let publication = ServingPublication {
            origin: self.origin,
            attempt: Uuid::new_v4(),
        };
        #[cfg(not(feature = "remote"))]
        let _ = name;
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
        Ok(publication)
    }

    pub async fn seal(&self, name: &ObjectKey, plaintext: Bytes) -> Result<Bytes> {
        let publication = self.allocate(name).await?;
        self.operate(name, publication, UnitOperation::SealPrefixedObject {
            prefix: Bytes::copy_from_slice(&publication.prefix()),
            body: plaintext,
        }).await?.bytes()
    }

    pub(super) async fn seal_file(&self, name: &ObjectKey, file: FramedFile) -> Result<Bytes> {
        let publication = self.allocate(name).await?;
        self.operate(name, publication, UnitOperation::SealFile {
            prefix: Bytes::copy_from_slice(&publication.prefix()),
            file,
        }).await?.bytes()
    }

    pub(super) async fn matches_sealed(
        &self, name: &ObjectKey, existing: Bytes, expected: Bytes,
    ) -> Result<bool> {
        let existing_publication = Self::identity(name, &existing)?;
        let expected_publication = Self::identity(name, &expected)?;
        let expected = expected.slice(PUBLICATION_ID_BYTES..);
        if existing_publication == expected_publication {
            return self.operate(name, existing_publication, UnitOperation::CompareSealed {
                existing: existing.slice(PUBLICATION_ID_BYTES..),
                expected,
            }).await?.matches();
        }
        let existing = self.open(name, existing).await?;
        let (authority, slot, operation) = self.bind(name, expected_publication, UnitOperation::Open(expected))?;
        let expected = authority.operate(&slot, operation).await?.bytes()?;
        Ok(existing == expected)
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
