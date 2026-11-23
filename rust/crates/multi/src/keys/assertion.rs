use std::{fs, num::NonZeroU64, sync::Arc};

use bytes::{Bytes, BytesMut};
use rtdb_storage::IoRegion;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use uuid::Uuid;

use crate::cipher::{Cipher, SEALED_OVERHEAD};

use super::{DESTROYED, KEY_BYTES, LocalKeyService, archive, io, is_destroyed, new_key};

/// The immutable pre-ordering identity. The receiver must bind it to the original
/// assertion before an accepted-history reader can use the envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssertionStage {
    deployment: Uuid,
    incarnation: NonZeroU64,
    stage: Uuid,
}

impl AssertionStage {
    pub(super) const KEY_PREFIX: &str = "assertion-stage-";

    pub fn new(deployment: Uuid, incarnation: NonZeroU64, stage: Uuid) -> Result<Self> {
        if deployment.is_nil() || stage.is_nil() {
            return Err(Anomaly::incorrect(
                "rtdb.multi/assertion-stage", "An assertion stage requires a deployment and a staging identity",
            ));
        }
        Ok(Self { deployment, incarnation, stage })
    }

    pub(super) fn key_id(self) -> String {
        format!("{}{}-{}-{}", Self::KEY_PREFIX, self.deployment, self.incarnation, self.stage)
    }

    fn associated(self, purpose: &str) -> String {
        format!("rtdb.multi/assertion-envelope/v1/{purpose}/{}", self.key_id())
    }
}

/// A passive, relocatable ciphertext frame. The stage comes from authenticated
/// placement evidence and is never inferred from the frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssertionEnvelope(Bytes);

const HEADER: &[u8] = b"RTAS\x01";
const WRAPPED_KEY_BYTES: usize = KEY_BYTES + SEALED_OVERHEAD;
const BODY_OFFSET: usize = HEADER.len() + WRAPPED_KEY_BYTES;

impl TryFrom<Bytes> for AssertionEnvelope {
    type Error = Anomaly;

    fn try_from(bytes: Bytes) -> Result<Self> {
        if !bytes.starts_with(HEADER) || bytes.len() < BODY_OFFSET + SEALED_OVERHEAD {
            return Err(Anomaly::incorrect(
                "rtdb.multi/assertion-envelope", "Invalid assertion envelope framing",
            ));
        }
        Ok(Self(bytes))
    }
}

impl From<AssertionEnvelope> for Bytes {
    fn from(envelope: AssertionEnvelope) -> Self {
        envelope.0
    }
}

impl AssertionEnvelope {
    pub(super) fn seal(stage: AssertionStage, wrapping_key: &[u8; KEY_BYTES], plaintext: &[u8]) -> Result<Self> {
        let data_key = new_key()?;
        let wrapped = Cipher::new(wrapping_key)?.seal(&stage.associated("key"), &data_key)?;
        let body = Cipher::new(&data_key)?.seal(&stage.associated("body"), plaintext)?;
        let mut frame = BytesMut::with_capacity(HEADER.len() + wrapped.len() + body.len());
        frame.extend_from_slice(HEADER);
        frame.extend_from_slice(&wrapped);
        frame.extend_from_slice(&body);
        Ok(Self(frame.freeze()))
    }

    pub(super) fn open(&self, stage: AssertionStage, wrapping_key: &[u8; KEY_BYTES]) -> Result<Bytes> {
        let data_key = Cipher::new(wrapping_key)?
            .open(&stage.associated("key"), &self.0[HEADER.len()..BODY_OFFSET])?;
        Ok(Cipher::new(&data_key)?.open(&stage.associated("body"), &self.0[BODY_OFFSET..])?)
    }
}

#[async_trait::async_trait]
pub trait AssertionKeyService: Send + Sync {
    /// The caller retains the original frame for retry and reconciles unused
    /// stages after ambiguous publication.
    async fn seal_assertion_stage(
        &self, stage: AssertionStage, region: &Arc<IoRegion>, plaintext: Bytes,
    ) -> Result<AssertionEnvelope>;

    /// The caller owns current disclosure admission and the lifetime of
    /// returned plaintext.
    async fn open_assertion_stage(
        &self, stage: AssertionStage, region: &Arc<IoRegion>, envelope: AssertionEnvelope,
    ) -> Result<Bytes>;

    /// Records vault withdrawal, including for a stage that never acquired a
    /// key. The ordered completion barrier owns the remaining custody.
    async fn retire_assertion_stage(
        &self, stage: AssertionStage, region: &Arc<IoRegion>,
    ) -> Result<()>;
}

#[async_trait::async_trait]
impl AssertionKeyService for LocalKeyService {
    async fn seal_assertion_stage(
        &self, stage: AssertionStage, region: &Arc<IoRegion>, plaintext: Bytes,
    ) -> Result<AssertionEnvelope> {
        if !self.provision_archive_keys {
            return Err(Anomaly::unsupported(
                "rtdb.multi/assertion-vault", "Assertion staging requires the local operation broker",
            ));
        }
        let authority = self.clone();
        region.run_blocking(move || {
            let seal = || {
                let vault = authority.archive_vault()?;
                let path = vault.path(&stage.key_id())?;
                let _lock = vault.lock_key(&path)?;
                if path.try_exists().map_err(io)? {
                    return Err(Anomaly::new(
                        AnomalyCategory::Conflict, "rtdb.multi/assertion-stage-used",
                        "This assertion stage already has a custody entry",
                    ));
                }
                let key = new_key()?;
                let envelope = AssertionEnvelope::seal(stage, &key, &plaintext)?;
                vault.publish(&path, &key)?;
                Ok(envelope)
            };
            Ok(seal())
        }).await?
    }

    async fn open_assertion_stage(
        &self, stage: AssertionStage, region: &Arc<IoRegion>, envelope: AssertionEnvelope,
    ) -> Result<Bytes> {
        let authority = self.clone();
        let vault = region.run_blocking(move || Ok(authority.archive_vault())).await??;
        let operation = archive::LocalArchiveKey {
            unit_vault: vault.clone(),
            vault,
            id: stage.key_id(),
            region: Arc::clone(region),
        };
        Ok(operation.operate_with(move |key| envelope.open(stage, key), |error| {
            match error.code.as_ref() {
                "rtdb.multi/key-destroyed" => rtdb_storage::Error::ErasedUnderView,
                _ => rtdb_storage::Error::Remote {
                    key: "assertion wrapping capability".into(),
                    message: error.to_string(),
                },
            }
        }).await?)
    }

    async fn retire_assertion_stage(
        &self, stage: AssertionStage, region: &Arc<IoRegion>,
    ) -> Result<()> {
        let authority = self.clone();
        region.run_blocking(move || {
            let retire = || {
                let vault = authority.archive_vault()?;
                let path = vault.path(&stage.key_id())?;
                let _lock = vault.lock_key(&path)?;
                match fs::read(&path) {
                    Ok(bytes) if is_destroyed(&bytes) => vault.sync_directory(),
                    Ok(_) => vault.publish(&path, DESTROYED),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        vault.publish(&path, DESTROYED)
                    }
                    Err(error) => Err(io(error)),
                }
            };
            Ok(retire())
        }).await?
    }
}

pub(super) fn require_other_key_namespace(id: &str) -> Result<()> {
    if id.starts_with(AssertionStage::KEY_PREFIX) {
        return Err(Anomaly::new(
            AnomalyCategory::Forbidden, "rtdb.multi/key-non-exportable",
            "An assertion stage belongs to its operation broker",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::KeyService;

    #[tokio::test]
    async fn retained_frames_require_their_own_live_stage() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let region = IoRegion::new();
        let deployment = Uuid::new_v4();
        let incarnation = NonZeroU64::MIN;
        let a = AssertionStage::new(deployment, incarnation, Uuid::new_v4())?;
        let b = AssertionStage::new(deployment, incarnation, Uuid::new_v4())?;
        let missing = AssertionStage::new(deployment, incarnation, Uuid::new_v4())?;
        let secret = Bytes::from_static(b"first body");
        let neighbor = Bytes::from_static(b"neighbor body");

        let first = vault.seal_assertion_stage(a, &region, secret.clone()).await?;
        let second = vault.seal_assertion_stage(b, &region, neighbor.clone()).await?;
        assert_eq!(vault.open_assertion_stage(a, &region, first.clone()).await?, secret);
        assert!(vault.open_assertion_stage(b, &region, first.clone()).await.is_err());
        assert!(vault.open_assertion_stage(missing, &region, first.clone()).await.is_err());
        assert!(vault.seal_assertion_stage(a, &region, neighbor.clone()).await.is_err());
        assert!(vault.key(&a.key_id()).await.is_err());
        assert!(vault.archive_key(&a.key_id(), false, Arc::clone(&region)).await.is_err());

        let mut swapped = first.0[..BODY_OFFSET].to_vec();
        swapped.extend_from_slice(&second.0[BODY_OFFSET..]);
        let swapped = AssertionEnvelope::try_from(Bytes::from(swapped))?;
        assert!(vault.open_assertion_stage(a, &region, swapped).await.is_err());

        let retained = Bytes::from(first);
        assert!(AssertionEnvelope::try_from(retained.slice(..BODY_OFFSET)).is_err());
        vault.retire_assertion_stage(a, &region).await?;
        vault.retire_assertion_stage(missing, &region).await?;
        let reopened = LocalKeyService::with_archive_broker(directory.path())?;
        reopened.retire_assertion_stage(a, &region).await?;
        let erased = reopened.open_assertion_stage(a, &region, retained.try_into()?).await.unwrap_err();
        assert_eq!(erased.code, "rtdb/erased-under-view");
        assert!(reopened.seal_assertion_stage(a, &region, secret.clone()).await.is_err());
        assert!(reopened.seal_assertion_stage(missing, &region, secret).await.is_err());
        assert_eq!(reopened.open_assertion_stage(b, &region, second).await?, neighbor);
        Ok(())
    }
}
