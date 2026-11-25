use std::{fs, num::NonZeroU64, sync::Arc};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::{Bytes, BytesMut};
use ring::{digest, hmac};
use rtdb_storage::IoRegion;
use rtdb_types::{Anomaly, AnomalyCategory, AssertionId, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::cipher::{Cipher, SEALED_OVERHEAD};

use super::{DESTROYED, KEY_BYTES, LocalKeyService, io, is_destroyed, new_key};

pub(super) mod family;
pub(super) mod issuer;

/// The immutable pre-ordering identity. The receiver must bind it to the original
/// assertion before an accepted-history reader can use the envelope.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "StageCoordinates", into = "StageCoordinates")]
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

    pub fn deployment(self) -> Uuid {
        self.deployment
    }

    pub fn incarnation(self) -> NonZeroU64 {
        self.incarnation
    }

    pub fn identity(self) -> Uuid {
        self.stage
    }

    pub(super) fn key_id(self) -> String {
        format!("{}{}-{}-{}", Self::KEY_PREFIX, self.deployment, self.incarnation, self.stage)
    }
}

type StageCoordinates = (Uuid, NonZeroU64, Uuid);

impl TryFrom<StageCoordinates> for AssertionStage {
    type Error = Anomaly;

    fn try_from((deployment, incarnation, stage): StageCoordinates) -> Result<Self> {
        Self::new(deployment, incarnation, stage)
    }
}

impl From<AssertionStage> for StageCoordinates {
    fn from(stage: AssertionStage) -> Self {
        (stage.deployment, stage.incarnation, stage.stage)
    }
}

/// Canonical pre-order bytes supplied by the admitted receiver. The scope
/// commitment covers the original attempt and disclosure domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssertionAuthoring {
    pub assertion_id: AssertionId,
    pub scope_commitment: [u8; 32],
    pub codec: u32,
}

impl AssertionAuthoring {
    fn binding(self, stage: AssertionStage) -> String {
        let mut input = digest::Context::new(&digest::SHA256);
        input.update(b"rtdb.multi/assertion-authoring/v2\0");
        input.update(stage.deployment.as_bytes());
        input.update(&stage.incarnation.get().to_be_bytes());
        input.update(stage.stage.as_bytes());
        input.update(&self.assertion_id.to_bytes());
        input.update(&self.scope_commitment);
        input.update(&self.codec.to_be_bytes());
        URL_SAFE_NO_PAD.encode(input.finish())
    }
}

/// A passive, relocatable ciphertext frame. The stage and authoring context
/// come from admission or certified placement evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssertionEnvelope(Bytes);

const HEADER: &[u8] = b"RTAS\x02";
const WRAPPED_KEY_BYTES: usize = KEY_BYTES + SEALED_OVERHEAD;
const BODY_OFFSET: usize = HEADER.len() + WRAPPED_KEY_BYTES;
const MAX_BODY_BYTES: usize = crate::process::MAXIMUM_LOG_MESSAGE_BYTES;
const MAX_ENVELOPE_BYTES: usize = BODY_OFFSET + SEALED_OVERHEAD + MAX_BODY_BYTES;
const COMMITMENT_BYTES: usize = 32;
const CAPABILITY_HEADER: &[u8] = b"RTAC\x01";
const CAPABILITY_ENVELOPE_OFFSET: usize = CAPABILITY_HEADER.len() + KEY_BYTES + COMMITMENT_BYTES;
pub(super) const MAX_CAPABILITY_BYTES: usize = CAPABILITY_ENVELOPE_OFFSET + MAX_ENVELOPE_BYTES;

impl TryFrom<Bytes> for AssertionEnvelope {
    type Error = Anomaly;

    fn try_from(bytes: Bytes) -> Result<Self> {
        if !bytes.starts_with(HEADER)
            || !(BODY_OFFSET + SEALED_OVERHEAD..=MAX_ENVELOPE_BYTES).contains(&bytes.len())
        {
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
    fn seal(binding: &str, wrapping_key: &[u8; KEY_BYTES], plaintext: &[u8]) -> Result<Self> {
        let data_key = new_key()?;
        let wrapped = Cipher::new(wrapping_key)?
            .seal(&format!("rtdb.multi/assertion-envelope/v2/key/{binding}"), &data_key)?;
        let body = Cipher::new(&data_key)?
            .seal(&format!("rtdb.multi/assertion-envelope/v2/body/{binding}"), plaintext)?;
        let mut frame = BytesMut::with_capacity(HEADER.len() + wrapped.len() + body.len());
        frame.extend_from_slice(HEADER);
        frame.extend_from_slice(&wrapped);
        frame.extend_from_slice(&body);
        Ok(Self(frame.freeze()))
    }

    fn open(&self, binding: &str, wrapping_key: &[u8; KEY_BYTES]) -> Result<Bytes> {
        let data_key = Cipher::new(wrapping_key)?
            .open(
                &format!("rtdb.multi/assertion-envelope/v2/key/{binding}"),
                &self.0[HEADER.len()..BODY_OFFSET],
            ).map_err(|_| stage_mismatch())?;
        Cipher::new(&data_key)?
            .open(
                &format!("rtdb.multi/assertion-envelope/v2/body/{binding}"),
                &self.0[BODY_OFFSET..],
            ).map_err(|_| stage_mismatch())
    }

    pub fn digest(&self) -> [u8; 32] {
        let mut result = [0; 32];
        result.copy_from_slice(digest::digest(&digest::SHA256, &self.0).as_ref());
        result
    }
}

/// The current vault record keeps the original frame for exact retries.
/// The vault replaces the entire record with a tombstone on retirement.
pub(super) struct StagedCapability {
    record: Bytes,
    envelope: AssertionEnvelope,
}

impl StagedCapability {
    pub(super) fn create(
        stage: AssertionStage, authoring: AssertionAuthoring, plaintext: &[u8],
    ) -> Result<Self> {
        require_body_limit(plaintext)?;
        let key = new_key()?;
        let binding = authoring.binding(stage);
        let envelope = AssertionEnvelope::seal(&binding, &key, plaintext)?;
        let (commitment_key, candidate) = candidate_commitment(&key, &binding, plaintext);
        let commitment = hmac::sign(&commitment_key, &candidate);
        let mut record = BytesMut::with_capacity(CAPABILITY_ENVELOPE_OFFSET + envelope.0.len());
        record.extend_from_slice(CAPABILITY_HEADER);
        record.extend_from_slice(&key);
        record.extend_from_slice(commitment.as_ref());
        record.extend_from_slice(&envelope.0);
        Ok(Self { record: record.freeze(), envelope })
    }

    pub(super) fn read(record: Bytes) -> Result<Self> {
        if !record.starts_with(CAPABILITY_HEADER) || record.len() < CAPABILITY_ENVELOPE_OFFSET {
            return Err(Anomaly::fault("rtdb.multi/assertion-capability", "Invalid assertion capability record"));
        }
        let envelope = Bytes::copy_from_slice(&record[CAPABILITY_ENVELOPE_OFFSET..]).try_into()?;
        Ok(Self { record, envelope })
    }

    fn key(&self) -> &[u8; KEY_BYTES] {
        self.record[CAPABILITY_HEADER.len()..CAPABILITY_HEADER.len() + KEY_BYTES]
            .try_into().expect("validated capability header")
    }

    pub(super) fn bytes(&self) -> &[u8] {
        &self.record
    }

    pub(super) fn envelope(self) -> AssertionEnvelope {
        self.envelope
    }

    pub(super) fn retry(
        self, stage: AssertionStage, authoring: AssertionAuthoring, plaintext: &[u8],
    ) -> Result<AssertionEnvelope> {
        require_body_limit(plaintext)?;
        let binding = authoring.binding(stage);
        let (key, candidate) = candidate_commitment(self.key(), &binding, plaintext);
        let expected = &self.record[CAPABILITY_HEADER.len() + KEY_BYTES..CAPABILITY_ENVELOPE_OFFSET];
        hmac::verify(&key, &candidate, expected).map_err(|_| stage_mismatch())?;
        Ok(self.envelope)
    }

    pub(super) fn open(
        &self, stage: AssertionStage, authoring: AssertionAuthoring, envelope: AssertionEnvelope,
    ) -> Result<Bytes> {
        if self.envelope != envelope {
            return Err(stage_mismatch());
        }
        envelope.open(&authoring.binding(stage), self.key())
    }

    pub(super) fn verify(&self, stage: AssertionStage, authoring: AssertionAuthoring, envelope: AssertionEnvelope) -> Result<()> {
        if self.envelope != envelope { return Err(stage_mismatch()); }
        let binding = authoring.binding(stage);
        let opened = Cipher::new(self.key())?
            .open(&format!("rtdb.multi/assertion-envelope/v2/key/{binding}"), &envelope.0[HEADER.len()..BODY_OFFSET])
            .map_err(|_| stage_mismatch())?;
        let mut opened = opened.try_into_mut().map_err(|_| stage_mismatch())?;
        zeroize::Zeroize::zeroize(opened.as_mut());
        Ok(())
    }
}

fn candidate_commitment(
    wrapping_key: &[u8; KEY_BYTES], binding: &str, plaintext: &[u8],
) -> (hmac::Key, [u8; 32]) {
    let wrapping_key = hmac::Key::new(hmac::HMAC_SHA256, wrapping_key);
    let mut context = hmac::Context::with_key(&wrapping_key);
    context.update(b"rtdb.multi/assertion-retry/v1\0");
    context.update(binding.as_bytes());
    let key = hmac::Key::new(hmac::HMAC_SHA256, context.sign().as_ref());
    let mut candidate = [0; 32];
    candidate.copy_from_slice(digest::digest(&digest::SHA256, plaintext).as_ref());
    (key, candidate)
}

fn require_body_limit(plaintext: &[u8]) -> Result<()> {
    if plaintext.len() > MAX_BODY_BYTES {
        return Err(Anomaly::incorrect(
            "rtdb.multi/assertion-size", "The assertion exceeds the source message limit",
        ));
    }
    Ok(())
}

fn stage_mismatch() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict, "rtdb.multi/assertion-stage-mismatch",
        "The assertion does not match the admitted stage",
    )
}

pub(super) fn stage_used() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict, "rtdb.multi/assertion-stage-used",
        "This assertion stage has been retired",
    )
}

/// Passive backup record for an assertion envelope. The record contains no
/// capability or tombstone. Restore must obtain the expected stage from the
/// original ordered effect and use the live issuer for any subsequent open.
pub struct AssertionBackup {
    stage: AssertionStage,
    envelope: AssertionEnvelope,
}

impl AssertionBackup {
    const HEADER: &[u8; 5] = b"RTAB\x01";
    const ENVELOPE_OFFSET: usize = Self::HEADER.len() + 16 + 8 + 16;

    pub fn new(stage: AssertionStage, envelope: AssertionEnvelope) -> Self {
        Self { stage, envelope }
    }

    pub fn restore(self, expected: AssertionStage) -> Result<AssertionEnvelope> {
        if self.stage != expected {
            return Err(Self::invalid());
        }
        Ok(self.envelope)
    }

    fn invalid() -> Anomaly {
        Anomaly::incorrect("rtdb.multi/assertion-backup", "Invalid assertion backup or original stage")
    }
}

impl From<AssertionBackup> for Bytes {
    fn from(backup: AssertionBackup) -> Self {
        let envelope = Bytes::from(backup.envelope);
        let mut frame = BytesMut::with_capacity(AssertionBackup::ENVELOPE_OFFSET + envelope.len());
        frame.extend_from_slice(AssertionBackup::HEADER);
        frame.extend_from_slice(backup.stage.deployment.as_bytes());
        frame.extend_from_slice(&backup.stage.incarnation.get().to_be_bytes());
        frame.extend_from_slice(backup.stage.stage.as_bytes());
        frame.extend_from_slice(&envelope);
        frame.freeze()
    }
}

impl TryFrom<Bytes> for AssertionBackup {
    type Error = Anomaly;

    fn try_from(bytes: Bytes) -> Result<Self> {
        if !bytes.starts_with(Self::HEADER) || bytes.len() < Self::ENVELOPE_OFFSET {
            return Err(Self::invalid());
        }
        let offset = Self::HEADER.len();
        let deployment = Uuid::from_slice(&bytes[offset..offset + 16]).map_err(|_| Self::invalid())?;
        let incarnation = u64::from_be_bytes(bytes[offset + 16..offset + 24].try_into().map_err(|_| Self::invalid())?);
        let incarnation = NonZeroU64::new(incarnation).ok_or_else(Self::invalid)?;
        let stage = Uuid::from_slice(&bytes[offset + 24..Self::ENVELOPE_OFFSET]).map_err(|_| Self::invalid())?;
        Ok(Self {
            stage: AssertionStage::new(deployment, incarnation, stage)?,
            envelope: bytes.slice(Self::ENVELOPE_OFFSET..).try_into()?,
        })
    }
}

#[async_trait::async_trait]
pub trait AssertionKeyService: Send + Sync {
    async fn verify_assertion_envelope(&self, _stage: AssertionStage, _authoring: AssertionAuthoring,
        _region: &Arc<IoRegion>, _envelope: AssertionEnvelope) -> Result<()> {
        Err(Anomaly::unsupported("rtdb.multi/assertion-envelope-proof", "The key vault cannot authenticate a retained envelope"))
    }
    /// A retry of the admitted candidate returns the original frame.
    async fn seal_assertion_stage(
        &self, stage: AssertionStage, authoring: AssertionAuthoring, region: &Arc<IoRegion>,
        plaintext: Bytes,
    ) -> Result<AssertionEnvelope>;

    /// The trusted resolver owns the one-use admission and lifetime of this
    /// transient plaintext. This operation cannot authorize a history read.
    async fn open_assertion_for_resolution(
        &self, stage: AssertionStage, authoring: AssertionAuthoring, region: &Arc<IoRegion>,
        envelope: AssertionEnvelope,
    ) -> Result<Bytes>;

    /// Records vault withdrawal, including for a stage that never acquired a
    /// key. The ordered completion barrier owns the remaining custody.
    async fn retire_assertion_stage(
        &self, stage: AssertionStage, region: &Arc<IoRegion>,
    ) -> Result<()>;
}

#[async_trait::async_trait]
impl AssertionKeyService for LocalKeyService {
    async fn verify_assertion_envelope(&self, stage: AssertionStage, authoring: AssertionAuthoring,
        region: &Arc<IoRegion>, envelope: AssertionEnvelope) -> Result<()> {
        self.require_assertion_broker()?;
        let authority = self.clone();
        region.run_blocking(move || {
            let verify = || {
                let vault = authority.archive_vault()?;
                let path = vault.path(&stage.key_id())?;
                let _lock = vault.lock_key_shared(&path)?;
                let bytes = fs::read(&path).map_err(io)?;
                if is_destroyed(&bytes) { return Err(rtdb_storage::Error::ErasedUnderView.into()); }
                StagedCapability::read(bytes.into())?.verify(stage, authoring, envelope)
            };
            Ok(verify())
        }).await?
    }
    async fn seal_assertion_stage(
        &self, stage: AssertionStage, authoring: AssertionAuthoring, region: &Arc<IoRegion>,
        plaintext: Bytes,
    ) -> Result<AssertionEnvelope> {
        self.require_assertion_broker()?;
        let authority = self.clone();
        region.run_blocking(move || {
            let seal = || {
                let vault = authority.archive_vault()?;
                let path = vault.path(&stage.key_id())?;
                let _lock = vault.lock_key(&path)?;
                match fs::read(&path) {
                    Ok(bytes) if is_destroyed(&bytes) => Err(stage_used()),
                    Ok(bytes) => {
                        let frame = StagedCapability::read(bytes.into())?.retry(stage, authoring, &plaintext)?;
                        vault.sync_directory()?;
                        Ok(frame)
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        let capability = StagedCapability::create(stage, authoring, &plaintext)?;
                        vault.publish(&path, capability.bytes())?;
                        Ok(capability.envelope())
                    }
                    Err(error) => Err(io(error)),
                }
            };
            Ok(seal())
        }).await?
    }

    async fn open_assertion_for_resolution(
        &self, stage: AssertionStage, authoring: AssertionAuthoring, region: &Arc<IoRegion>,
        envelope: AssertionEnvelope,
    ) -> Result<Bytes> {
        self.require_assertion_broker()?;
        let authority = self.clone();
        region.run_blocking(move || {
            let open = || {
                let vault = authority.archive_vault()?;
                let path = vault.path(&stage.key_id())?;
                let _lock = vault.lock_key_shared(&path)?;
                let bytes = fs::read(&path).map_err(io)?;
                if is_destroyed(&bytes) {
                    return Err(rtdb_storage::Error::ErasedUnderView.into());
                }
                StagedCapability::read(bytes.into())?.open(stage, authoring, envelope)
            };
            Ok(open())
        }).await?
    }

    async fn retire_assertion_stage(
        &self, stage: AssertionStage, region: &Arc<IoRegion>,
    ) -> Result<()> {
        self.require_assertion_broker()?;
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

impl LocalKeyService {
    fn require_assertion_broker(&self) -> Result<()> {
        if !self.provision_archive_keys {
            return Err(Anomaly::unsupported(
                "rtdb.multi/assertion-vault", "Assertion custody requires the local operation broker",
            ));
        }
        self.require_service_root()
    }
}

pub(super) fn require_other_key_namespace(id: &str) -> Result<()> {
    if id.starts_with(AssertionStage::KEY_PREFIX) || id.starts_with(family::LOCAL_PREFIX)
        || id.starts_with(family::LOCAL_ISSUER_PREFIX)
    {
        return Err(Anomaly::new(
            AnomalyCategory::Forbidden, "rtdb.multi/key-non-exportable",
            "Assertion authority belongs to its operation broker",
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
        let a_form = AssertionAuthoring { assertion_id: AssertionId::from_bytes([1; 32]), scope_commitment: [2; 32], codec: 3 };
        let b_form = AssertionAuthoring { assertion_id: AssertionId::from_bytes([4; 32]), ..a_form };
        let secret = Bytes::from_static(b"first body");
        let neighbor = Bytes::from_static(b"neighbor body");

        let first = vault.seal_assertion_stage(a, a_form, &region, secret.clone()).await?;
        let second = vault.seal_assertion_stage(b, b_form, &region, neighbor.clone()).await?;
        assert_ne!(first.digest(), second.digest());
        let retry_vault = LocalKeyService::with_archive_broker(directory.path())?;
        assert_eq!(retry_vault.seal_assertion_stage(a, a_form, &region, secret.clone()).await?, first);
        retry_vault.verify_assertion_envelope(a, a_form, &region, first.clone()).await?;
        for substitute in [
            b_form,
            AssertionAuthoring { scope_commitment: [5; 32], ..a_form },
            AssertionAuthoring { codec: 6, ..a_form },
        ] {
            assert!(retry_vault.seal_assertion_stage(a, substitute, &region, secret.clone()).await.is_err());
            assert!(retry_vault.open_assertion_for_resolution(a, substitute, &region, first.clone()).await.is_err());
            assert!(retry_vault.verify_assertion_envelope(a, substitute, &region, first.clone()).await.is_err());
        }
        assert_eq!(vault.open_assertion_for_resolution(a, a_form, &region, first.clone()).await?, secret);
        assert!(vault.open_assertion_for_resolution(b, b_form, &region, first.clone()).await.is_err());
        assert!(vault.open_assertion_for_resolution(missing, a_form, &region, first.clone()).await.is_err());
        assert!(vault.seal_assertion_stage(a, a_form, &region, neighbor.clone()).await.is_err());
        assert!(vault.key(&a.key_id()).await.is_err());
        assert!(vault.archive_key(&a.key_id(), false, Arc::clone(&region)).await.is_err());

        let mut swapped = first.0[..BODY_OFFSET].to_vec();
        swapped.extend_from_slice(&second.0[BODY_OFFSET..]);
        let swapped = AssertionEnvelope::try_from(Bytes::from(swapped))?;
        assert!(vault.verify_assertion_envelope(a, a_form, &region, swapped.clone()).await.is_err());
        assert!(vault.open_assertion_for_resolution(a, a_form, &region, swapped).await.is_err());

        let retained = Bytes::from(first);
        assert!(AssertionEnvelope::try_from(retained.slice(..BODY_OFFSET)).is_err());
        let backup = Bytes::from(AssertionBackup::new(a, retained.try_into()?));
        assert!(AssertionBackup::try_from(backup.clone())?.restore(b).is_err());
        let raw_service = LocalKeyService::new(directory.path())?;
        assert!(raw_service.retire_assertion_stage(a, &region).await.is_err());
        assert!(raw_service.open_assertion_for_resolution(a, a_form, &region,
            AssertionBackup::try_from(backup.clone())?.restore(a)?).await.is_err());

        vault.retire_assertion_stage(a, &region).await?;
        vault.retire_assertion_stage(missing, &region).await?;
        let reopened = LocalKeyService::with_archive_broker(directory.path())?;
        reopened.retire_assertion_stage(a, &region).await?;
        let restored = AssertionBackup::try_from(backup)?.restore(a)?;
        let erased = reopened.open_assertion_for_resolution(a, a_form, &region, restored).await.unwrap_err();
        assert_eq!(erased.code, "rtdb/erased-under-view");
        assert!(reopened.seal_assertion_stage(a, a_form, &region, secret.clone()).await.is_err());
        assert!(reopened.seal_assertion_stage(missing, a_form, &region, secret).await.is_err());
        assert_eq!(reopened.open_assertion_for_resolution(b, b_form, &region, second).await?, neighbor);
        Ok(())
    }
}
