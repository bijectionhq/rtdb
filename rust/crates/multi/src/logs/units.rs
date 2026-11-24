//! Independent key slots for each directory-managed journal publication.

use std::sync::Arc;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use ring::{
    digest::{Context, SHA256, digest},
    rand::{SecureRandom, SystemRandom},
};
use rtdb_format::envelope::ReplicaFrame;
use rtdb_storage::{Error, IoRegion, Result, log::LogRecord};
use rtdb_types::Result as DomainResult;

use crate::keys::{ArchiveKey, KeyService};

type Publication = [u8; 32];
const PUBLICATION_BYTES: usize = size_of::<Publication>();
const HEADER_BYTES: usize = PUBLICATION_BYTES * 2;
pub(super) const OVERHEAD: usize = HEADER_BYTES + crate::cipher::SEALED_OVERHEAD;

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

    fn associated(&self, publication: &[u8], reference: &[u8]) -> String {
        format!(
            "rtdb.multi/journal-unit/v2/{}/{}/{}",
            self.scope,
            URL_SAFE_NO_PAD.encode(publication),
            URL_SAFE_NO_PAD.encode(reference)
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
        let reference = reference_digest(&plaintext);
        let mut header = [0; HEADER_BYTES];
        header[..PUBLICATION_BYTES].copy_from_slice(&publication);
        header[PUBLICATION_BYTES..].copy_from_slice(&reference);
        self.keys.seal_prefixed(
            &self.associated(&publication, &reference),
            Bytes::copy_from_slice(&header),
            plaintext,
        ).await
    }

    pub(crate) fn retirement_label(&self, sealed: &[u8]) -> Result<String> {
        let (publication, reference) = split_header(sealed)?;
        Ok(self.associated(publication, reference))
    }

    pub(crate) async fn decrypt(&self, sealed: Bytes) -> Result<Bytes> {
        let (publication, reference) = split_header(&sealed)?;
        let plaintext = self.keys
            .open(
                &self.associated(publication, reference),
                sealed.slice(HEADER_BYTES..),
            )
            .await?;
        if reference_digest(&plaintext).as_slice() != reference {
            return Err(Error::Task("The sealed journal frame has a different detached reference".into()));
        }
        Ok(plaintext)
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

fn split_header(sealed: &[u8]) -> Result<(&[u8], &[u8])> {
    let header = sealed.get(..HEADER_BYTES)
        .ok_or_else(|| Error::Task("A sealed journal frame has no publication header".into()))?;
    Ok(header.split_at(PUBLICATION_BYTES))
}

/// A separate payload owner can match a delivered pointer to a certified
/// frame without opening the journal frame or borrowing its key.
pub(crate) fn commits_payload_reference(sealed: &[u8], reference: &[u8]) -> bool {
    let supplied = reference_digest(reference);
    supplied != [0; 32] && payload_reference_digest(sealed) == Some(supplied)
}

pub(crate) fn payload_reference_digest(sealed: &[u8]) -> Option<[u8; 32]> {
    if sealed.len() < OVERHEAD { return None; }
    sealed.get(PUBLICATION_BYTES..HEADER_BYTES)?.try_into().ok()
}

fn reference_digest(plaintext: &[u8]) -> [u8; 32] {
    if !matches!(ReplicaFrame::has_external_payload(plaintext), Ok(true)) {
        return [0; 32];
    }
    digest(&SHA256, plaintext).as_ref().try_into().expect("SHA-256 width")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::LocalKeyService;
    use rtdb_format::{envelope::{ReplicaBody, ReplicaEnvelope}, keys::CURRENT_STORAGE_VERSION, log::proto::NoOp};
    use rtdb_types::{BlockIndex, StorageEpoch, Term, TermSequence};

    #[tokio::test]
    async fn sealing_byte_path_commits_only_its_exact_external_reference() -> DomainResult<()> {
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path())?;
        keys.key("replica").await?;
        let units = JournalUnits::open(&keys, "replica", "replica", IoRegion::new()).await?;
        let inline = ReplicaEnvelope {
            term: Term(1),
            term_sequence: Some(TermSequence(0)),
            message: ReplicaBody::NoOp(NoOp::default()),
        };
        let body = inline.encode();
        let reference = |name| inline.offloaded(CURRENT_STORAGE_VERSION, StorageEpoch(0), BlockIndex(1), name, &body).encode();
        let chosen = reference("chosen".into());
        let other = reference("other".into());
        let sealed = units.seal(chosen.clone().into()).await?;
        let publication: Publication = sealed[..PUBLICATION_BYTES].try_into().unwrap();
        let digest = reference_digest(&chosen);
        let old_ciphertext = units.keys.seal(
            &units.associated(&publication, &digest),
            chosen.clone().into(),
        ).await?;
        let mut old_frame = publication.to_vec();
        old_frame.extend_from_slice(&digest);
        old_frame.extend_from_slice(&old_ciphertext);
        assert_eq!(sealed.as_ref(), old_frame);

        assert!(commits_payload_reference(&sealed, &chosen));
        assert!(!commits_payload_reference(&sealed, &other));
        assert_eq!(units.decrypt(sealed.clone()).await?.as_ref(), chosen.as_slice());

        let mut substituted = sealed.to_vec();
        substituted[PUBLICATION_BYTES..HEADER_BYTES].copy_from_slice(&reference_digest(&other));
        assert!(units.decrypt(substituted.into()).await.is_err());
        assert!(!commits_payload_reference(&units.seal(body.clone().into()).await?, &body));
        Ok(())
    }

    #[tokio::test]
    async fn sealing_byte_path_reseed_uses_only_its_destination_slot() -> DomainResult<()> {
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
