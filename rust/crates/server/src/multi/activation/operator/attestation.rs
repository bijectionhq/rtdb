use std::collections::BTreeMap;

#[cfg(unix)]
use std::{
    fs::OpenOptions,
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

#[cfg(unix)]
use ring::signature::Ed25519KeyPair;
use ring::signature::{ED25519, UnparsedPublicKey};
use rtdb_crypt::{Generation, RecoveryKeyHold};
use rtdb_db::ActivationScope;
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::super::{Binding, VerifiedSuccessor, recovery::SourceCut, successor_incarnation};

const VERSION: u8 = 1;
const HELD_KEY_VERSION: u8 = 2;

pub(super) type Attestors = BTreeMap<Uuid, [u8; 32]>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    pub authority_id: Uuid,
    pub database: String,
    pub expected: Binding,
    pub operation_id: Uuid,
    pub cell: Option<Uuid>,
}

impl Intent {
    pub(super) fn scope(&self) -> Result<ActivationScope> {
        ActivationScope::for_cell(self.authority_id, &self.database, self.cell)
    }

    pub(super) fn candidate(&self) -> Result<Uuid> {
        successor_incarnation(
            self.authority_id,
            &self.database,
            self.expected,
            self.operation_id,
            self.cell,
        )
    }
}

/// Versioned receipt for the exact staged incarnation and its committed restore origin.
/// The source cut is (expected incarnation, source position, source system time).
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Evidence {
    version: u8,
    intent: Intent,
    candidate: Uuid,
    source_position: u64,
    source_system_time: i64,
    source_lsn: u64,
    manifest_sha256: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key: Option<KeyEvidence>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct KeyEvidence {
    pub generation: u64,
    pub authority_sha256: [u8; 32],
}

impl Evidence {
    fn message(&self) -> Result<Vec<u8>> {
        super::super::super::validate_deployment_name(&self.intent.database)?;
        let mut bytes = b"rtdb.activation.installation-attestation\0".to_vec();
        bytes.push(self.version);
        bytes.extend_from_slice(self.intent.authority_id.as_bytes());
        bytes.extend_from_slice(&(self.intent.database.len() as u16).to_be_bytes());
        bytes.extend_from_slice(self.intent.database.as_bytes());
        bytes.extend_from_slice(self.intent.expected.incarnation.as_bytes());
        bytes.extend_from_slice(&self.intent.expected.revision.to_be_bytes());
        bytes.extend_from_slice(self.intent.operation_id.as_bytes());
        bytes.extend_from_slice(self.intent.cell.ok_or_else(invalid)?.as_bytes());
        bytes.extend_from_slice(self.candidate.as_bytes());
        bytes.extend_from_slice(&self.source_position.to_be_bytes());
        bytes.extend_from_slice(&self.source_system_time.to_be_bytes());
        bytes.extend_from_slice(&self.source_lsn.to_be_bytes());
        bytes.extend_from_slice(&self.manifest_sha256);
        match (self.version, self.key) {
            (VERSION, None) => (),
            (HELD_KEY_VERSION, Some(key)) if key.authority_sha256 != [0; 32] => {
                bytes.extend_from_slice(&key.generation.to_be_bytes());
                bytes.extend_from_slice(&key.authority_sha256);
            }
            _ => return Err(invalid()),
        }
        Ok(bytes)
    }

    fn key_hold(&self) -> Result<Option<RecoveryKeyHold>> {
        let Some(key) = self.key else { return Ok(None); };
        let digest = ring::digest::digest(&ring::digest::SHA256, &self.message()?);
        Ok(Some(RecoveryKeyHold {
            authority: self.intent.authority_id,
            authority_sha256: key.authority_sha256,
            operation: self.intent.operation_id,
            generation: Generation { database: self.candidate, number: key.generation },
            installation_sha256: digest.as_ref().try_into().expect("SHA-256 width"),
        }))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Attestation {
    evidence: Evidence,
    signature: Vec<u8>,
}

impl Attestation {
    #[cfg(unix)]
    pub(super) fn sign(
        intent: Intent,
        manifest_sha256: [u8; 32],
        installed: &rtdb_db::InstalledBackup,
        key: &Ed25519KeyPair,
    ) -> Result<Self> {
        Self::sign_installation(intent, manifest_sha256, installed, None, key)
    }

    #[cfg(unix)]
    pub(super) fn sign_installation(
        intent: Intent,
        manifest_sha256: [u8; 32],
        installed: &rtdb_db::InstalledBackup,
        key_custody: Option<KeyEvidence>,
        signing_key: &Ed25519KeyPair,
    ) -> Result<Self> {
        let candidate = intent.candidate()?;
        if intent.cell.is_none()
            || installed.activation_scope() != Some(intent.scope()?)
            || installed.source_incarnation() != intent.expected.incarnation
            || installed.destination_basis().incarnation() != *candidate.as_bytes()
            || installed.destination_basis().position() != installed.source_basis().position()
            || installed.manifest_sha256() != manifest_sha256
        {
            return Err(invalid());
        }
        let evidence = Evidence {
            version: if key_custody.is_some() { HELD_KEY_VERSION } else { VERSION },
            intent,
            candidate,
            source_position: installed.source_basis().position(),
            source_system_time: installed.source_system_time().0,
            source_lsn: installed.source_lsn().0,
            manifest_sha256,
            key: key_custody,
        };
        Self::signed(evidence, signing_key)
    }

    #[cfg(unix)]
    fn signed(evidence: Evidence, key: &Ed25519KeyPair) -> Result<Self> {
        let signature = key.sign(&evidence.message()?).as_ref().to_vec();
        Ok(Self {
            evidence,
            signature,
        })
    }

    #[cfg(unix)]
    pub(super) fn read(path: &Path) -> Result<Self> {
        serde_json::from_slice(&read_file(path, 4096, false)?).map_err(|_| invalid())
    }

    #[cfg(unix)]
    pub(super) fn held_key(&self) -> Result<(&Intent, RecoveryKeyHold)> {
        if self.evidence.candidate != self.evidence.intent.candidate()? {
            return Err(invalid());
        }
        let hold = self.evidence.key_hold()?.ok_or_else(invalid)?;
        Ok((&self.evidence.intent, hold))
    }

    pub(super) fn verify(
        &self,
        intent: &Intent,
        manifest_sha256: [u8; 32],
        attestors: &Attestors,
    ) -> Result<VerifiedSuccessor> {
        let evidence = &self.evidence;
        let cell = intent.cell.ok_or_else(invalid)?;
        let key = attestors.get(&cell).ok_or_else(invalid)?;
        if &evidence.intent != intent
            || evidence.candidate != intent.candidate()?
            || evidence.manifest_sha256 != manifest_sha256
            || evidence.source_lsn == 0
        {
            return Err(invalid());
        }
        UnparsedPublicKey::new(&ED25519, key)
            .verify(&evidence.message()?, &self.signature)
            .map_err(|_| invalid())?;
        Ok(VerifiedSuccessor {
            predecessor: intent.expected.incarnation,
            successor: evidence.candidate,
            manifest_digest: evidence.manifest_sha256,
            activation_scope: Some(intent.scope()?),
            destination_cell: Some(cell),
            key_hold: evidence.key_hold()?,
            source_cut: SourceCut {
                lsn: evidence.source_lsn,
                position: evidence.source_position,
                system_time: evidence.source_system_time,
            },
        })
    }
}

#[cfg(unix)]
pub(super) fn signing_key(path: &Path) -> Result<Ed25519KeyPair> {
    let bytes = zeroize::Zeroizing::new(read_file(path, 256, true)?);
    Ed25519KeyPair::from_pkcs8_maybe_unchecked(&bytes).map_err(|_| invalid())
}

#[cfg(unix)]
fn read_file(path: &Path, limit: u64, private: bool) -> Result<Vec<u8>> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| invalid())?;
    let metadata = file.metadata().map_err(|_| invalid())?;
    if !metadata.is_file()
        || (private
            && (metadata.mode() & 0o077 != 0 || metadata.uid() != super::broker::effective_uid()))
    {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid())?;
    if bytes.len() as u64 > limit {
        return Err(invalid());
    }
    Ok(bytes)
}

fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/activation-installation-attestation",
        "Invalid destination installation attestation or unregistered destination identity",
    )
}

#[cfg(all(test, unix))]
mod tests {
    use ring::signature::KeyPair;

    use super::*;

    fn intent() -> Intent {
        Intent {
            authority_id: Uuid::new_v4(),
            database: "xtdb".to_owned(),
            expected: Binding {
                incarnation: Uuid::new_v4(),
                revision: 7,
            },
            operation_id: Uuid::new_v4(),
            cell: Some(Uuid::new_v4()),
        }
    }

    fn key(seed: u8) -> Ed25519KeyPair {
        Ed25519KeyPair::from_seed_unchecked(&[seed; 32]).unwrap()
    }

    fn public_key(key: &Ed25519KeyPair) -> [u8; 32] {
        key.public_key().as_ref().try_into().unwrap()
    }

    #[tokio::test]
    async fn recovery_key_custody_releases_only_after_exact_authority_settlement() {
        use crate::multi::activation::{
            ActivationAuthority,
            recovery::{Intent as RecoveryIntent, LossPolicy, Operation, Status},
        };
        use rtdb_crypt::{FileKms, Kms};
        use std::{sync::{Arc, atomic::{AtomicU64, Ordering}}, time::Duration};

        let directory = tempfile::tempdir().unwrap();
        let authority_path = directory.path().join("authority");
        let identity = Uuid::new_v4();
        let fingerprint = [4; 32];
        let open_authority = || {
            let mut authority = ActivationAuthority::open_local(&authority_path, identity).unwrap();
            let state = Arc::get_mut(&mut authority).unwrap();
            let ticks = AtomicU64::new(0);
            state.clock = Arc::new(move || Some(Duration::from_secs(ticks.fetch_add(6, Ordering::SeqCst))));
            state.connection_sha256 = Some(fingerprint);
            authority
        };
        let authority = open_authority();
        let predecessor = authority.bootstrap("xtdb", Uuid::new_v4()).await.unwrap();
        let cell = Uuid::new_v4();
        let path = directory.path().join("vault");
        FileKms::create_directory(&path).unwrap();
        let kms = FileKms::open(&path, || 0).unwrap();
        let signer = key(7);
        let trusted = Attestors::from([(cell, public_key(&signer))]);
        let make = |operation_id| {
            let intent = Intent {
                authority_id: identity, database: "xtdb".to_owned(), expected: predecessor,
                operation_id, cell: Some(cell),
            };
            Attestation::signed(Evidence {
                version: HELD_KEY_VERSION,
                candidate: intent.candidate().unwrap(),
                intent,
                source_position: 11, source_system_time: 13, source_lsn: 17,
                manifest_sha256: [5; 32],
                key: Some(KeyEvidence { generation: 1, authority_sha256: fingerprint }),
            }, &signer).unwrap()
        };
        let recovery = |operation_id| RecoveryIntent {
            operation: Operation { predecessor, operation_id },
            destination_cell: Some(cell),
            loss_policy: LossPolicy::AcceptUnknownTail,
        };
        let first = recovery(Uuid::new_v4());
        authority.begin_recovery("xtdb", first, None).await.unwrap();
        let old_proof = make(first.operation.operation_id);
        let (_, old_hold) = old_proof.held_key().unwrap();
        kms.create(old_hold.generation).await.unwrap();
        kms.retain_recovery_key(old_hold).await.unwrap();
        assert!(super::super::release_recovery_key(&authority, "xtdb", &old_proof, &kms).await.is_err());
        kms.require_recovery_key(old_hold).await.unwrap();

        let selected = recovery(Uuid::new_v4());
        authority.begin_recovery("xtdb", selected, Some(first.operation.operation_id)).await.unwrap();
        assert_eq!(super::super::release_recovery_key(&authority, "xtdb", &old_proof, &kms).await.unwrap(), old_hold);
        let proof = make(selected.operation.operation_id);
        let (intent, hold) = proof.held_key().unwrap();
        kms.create(hold.generation).await.unwrap();
        kms.retain_recovery_key(hold).await.unwrap();
        let installation = proof.verify(intent, [5; 32], &trusted).unwrap();
        authority.record_recovery_installation("xtdb", selected.operation, installation).await.unwrap();
        authority.freeze_recovery("xtdb", selected.operation).await.unwrap();
        assert!(super::super::release_recovery_key(&authority, "xtdb", &proof, &kms).await.is_err());
        assert!(kms.retire(hold.generation, 1).await.is_err());
        authority.publish_recovery("xtdb", selected.operation).await.unwrap();
        drop(authority);
        drop(kms);

        let authority = open_authority();
        let kms = FileKms::open(&path, || 0).unwrap();
        let published = authority.recovery_status("xtdb", selected.operation).await.unwrap();
        assert!(matches!(published, Status::Published { installation, .. } if installation.key_hold == Some(hold)));
        let mut substituted = proof.clone();
        substituted.evidence.key.as_mut().unwrap().authority_sha256 = [22; 32];
        assert!(super::super::release_recovery_key(&authority, "xtdb", &substituted, &kms).await.is_err());
        kms.require_recovery_key(hold).await.unwrap();
        assert_eq!(super::super::release_recovery_key(&authority, "xtdb", &proof, &kms).await.unwrap(), hold);
        assert_eq!(super::super::release_recovery_key(&authority, "xtdb", &proof, &kms).await.unwrap(), hold);
        kms.retire(hold.generation, 1).await.unwrap();
    }

    #[test]
    fn exact_intent_archive_cut_and_destination_are_authenticated() {
        let intent = intent();
        let cell = intent.cell.unwrap();
        let key = key(7);
        let manifest_sha256 = [1; 32];
        let proof = Attestation::signed(
            Evidence {
                version: VERSION,
                candidate: intent.candidate().unwrap(),
                intent: intent.clone(),
                source_position: 71,
                source_system_time: 364100,
                source_lsn: 605,
                manifest_sha256,
                key: None,
            },
            &key,
        )
        .unwrap();
        let trusted = Attestors::from([(cell, public_key(&key))]);
        let json = serde_json::to_vec_pretty(&proof).unwrap();
        let transported: Attestation = serde_json::from_slice(&json).unwrap();
        assert_eq!(
            transported
                .verify(&intent, manifest_sha256, &trusted)
                .unwrap()
                .successor,
            intent.candidate().unwrap(),
        );
        assert!(proof.verify(&intent, [2; 32], &trusted).is_err());
        assert!(
            proof
                .verify(&intent, manifest_sha256, &Attestors::new())
                .is_err()
        );
        let wrong_cell = Uuid::new_v4();
        assert!(
            proof
                .verify(
                    &intent,
                    manifest_sha256,
                    &Attestors::from([(wrong_cell, public_key(&key))])
                )
                .is_err()
        );
        let different_key = Ed25519KeyPair::from_seed_unchecked(&[8; 32]).unwrap();
        assert!(
            proof
                .verify(
                    &intent,
                    manifest_sha256,
                    &Attestors::from([(cell, public_key(&different_key))])
                )
                .is_err()
        );

        let mut wrong_intent = intent.clone();
        wrong_intent.operation_id = Uuid::new_v4();
        assert!(
            proof
                .verify(&wrong_intent, manifest_sha256, &trusted)
                .is_err()
        );
        wrong_intent = intent.clone();
        wrong_intent.expected.revision += 1;
        assert!(
            proof
                .verify(&wrong_intent, manifest_sha256, &trusted)
                .is_err()
        );
        wrong_intent = intent.clone();
        wrong_intent.authority_id = Uuid::new_v4();
        assert!(
            proof
                .verify(&wrong_intent, manifest_sha256, &trusted)
                .is_err()
        );

        let mut tampered = proof.clone();
        tampered.evidence.source_position += 1;
        assert!(tampered.verify(&intent, manifest_sha256, &trusted).is_err());
        tampered = proof.clone();
        tampered.evidence.source_system_time += 1;
        assert!(tampered.verify(&intent, manifest_sha256, &trusted).is_err());
        tampered = proof.clone();
        tampered.evidence.source_lsn += 1;
        assert!(tampered.verify(&intent, manifest_sha256, &trusted).is_err());
        tampered = proof;
        tampered.evidence.manifest_sha256 = [2; 32];
        assert!(tampered.verify(&intent, [2; 32], &trusted).is_err());
    }
}
