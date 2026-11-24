use std::sync::Arc;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::{Bytes, BytesMut};

use super::{FramedObject, ObjectCipher, PlaintextView, UnitOperation, UnitOutput};
use crate::{Error, ObjectKey, Result, RetainedBytes};

pub const PUBLICATION_ID_BYTES: usize = 32;
type Origin = [u8; PUBLICATION_ID_BYTES];
const ARCHIVE: Origin = [0; PUBLICATION_ID_BYTES];

/// Selects the original key slot from an immutable object's serving envelope.
/// The origin travels with the bytes when a pack or fork reference moves.
#[derive(Clone)]
pub struct PublicationSealing {
    ledger: Arc<dyn ObjectCipher>,
    origin: Origin,
    archive: Option<Arc<dyn ObjectCipher>>,
    applies: fn(&ObjectKey) -> bool,
}

impl PublicationSealing {
    pub fn new(
        ledger: Arc<dyn ObjectCipher>,
        origin: Origin,
        applies: fn(&ObjectKey) -> bool,
    ) -> Self {
        assert_ne!(origin, ARCHIVE);
        Self {
            ledger,
            origin,
            archive: None,
            applies,
        }
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

    fn envelope(origin: Origin, sealed: Bytes) -> Bytes {
        let mut frame = BytesMut::with_capacity(PUBLICATION_ID_BYTES + sealed.len());
        frame.extend_from_slice(&origin);
        frame.extend_from_slice(&sealed);
        frame.freeze()
    }

    /// An admitted read-only overlay exposes the original archive ciphertext.
    pub fn archived(sealed: Bytes) -> Bytes {
        Self::envelope(ARCHIVE, sealed)
    }

    fn origin(name: &ObjectKey, bytes: &[u8]) -> Result<Origin> {
        bytes
            .get(..PUBLICATION_ID_BYTES)
            .and_then(|prefix| prefix.try_into().ok())
            .ok_or_else(|| Error::ConflictingObject(name.to_string()))
    }

    pub(super) async fn operate(
        &self,
        name: &ObjectKey,
        origin: Origin,
        operation: UnitOperation,
    ) -> Result<UnitOutput> {
        let read = !operation.provisions();
        let (authority, slot, operation) = if origin == ARCHIVE {
            let authority = self
                .archive
                .as_ref()
                .ok_or_else(|| Error::Closed("The original archive is not admitted".into()))?;
            (authority, name.clone(), operation)
        } else {
            let slot = ObjectKey::new(format!(
                "publications/{}/{name}",
                URL_SAFE_NO_PAD.encode(origin)
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
        let sealed = self
            .operate(name, self.origin, UnitOperation::Seal(plaintext))
            .await?
            .bytes()?;
        Ok(Self::envelope(self.origin, sealed))
    }

    pub async fn open(&self, name: &ObjectKey, sealed: Bytes) -> Result<Bytes> {
        let origin = Self::origin(name, &sealed)?;
        self.operate(
            name,
            origin,
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
        let origin = Self::origin(name, sealed.as_slice())?;
        self.operate(
            name,
            origin,
            UnitOperation::OpenRetained(sealed.discard_prefix(PUBLICATION_ID_BYTES)),
        )
        .await?
        .bytes()
    }

    pub async fn header(
        &self,
        name: &ObjectKey,
        header: Bytes,
    ) -> Result<(FramedObject, [u8; PUBLICATION_ID_BYTES])> {
        let origin = Self::origin(name, &header)?;
        let layout = self
            .operate(
                name,
                origin,
                UnitOperation::Header(header.slice(PUBLICATION_ID_BYTES..)),
            )
            .await?
            .header()?;
        Ok((layout, origin))
    }
}
