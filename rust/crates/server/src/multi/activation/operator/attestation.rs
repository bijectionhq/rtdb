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
use super::super::journal_custody::{JournalAttestation, JournalEvidence, RetainedJournal};

const VERSION: u8 = 1;
const HELD_KEY_VERSION: u8 = 2;
const HOSTED_CUSTODY_VERSION: u8 = 3;

pub(super) type Attestors = BTreeMap<Uuid, [u8; 32]>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::multi) struct Intent {
    pub authority_id: Uuid,
    pub database: String,
    pub expected: Binding,
    pub operation_id: Uuid,
    pub cell: Option<Uuid>,
}

impl Intent {
    pub(in crate::multi) fn scope(&self) -> Result<ActivationScope> {
        ActivationScope::for_cell(self.authority_id, &self.database, self.cell)
    }

    pub(in crate::multi) fn candidate(&self) -> Result<Uuid> {
        successor_incarnation(
            self.authority_id,
            &self.database,
            self.expected,
            self.operation_id,
            self.cell,
        )
    }

    fn evidence(&self, cut: SourceCut, manifest_sha256: [u8; 32], key: Option<KeyEvidence>) -> Result<Evidence> {
        Ok(Evidence {
            version: if key.is_some() { HELD_KEY_VERSION } else { VERSION },
            intent: self.clone(),
            candidate: self.candidate()?,
            source_position: cut.position,
            source_system_time: cut.system_time,
            source_lsn: cut.lsn,
            manifest_sha256,
            key,
            objects_sha256: None,
        })
    }

    #[cfg(feature = "backup-object-holds")]
    pub(in crate::multi) fn archive_key_hold(&self, archive: &rtdb_db::VerifiedBackup, key: KeyEvidence) -> Result<RecoveryKeyHold> {
        if archive.source_basis().incarnation() != *self.expected.incarnation.as_bytes() {
            return Err(invalid());
        }
        let cut = SourceCut {
            lsn: archive.source_lsn().0,
            position: archive.source_basis().position(),
            system_time: archive.source_system_time().0,
        };
        self.evidence(cut, archive.manifest_sha256(), Some(key))?.key_hold()?.ok_or_else(invalid)
    }

    #[cfg(feature = "backup-object-holds")]
    pub(in crate::multi) fn installed_key_hold(&self, installed: &rtdb_db::InstalledBackup, key: KeyEvidence) -> Result<RecoveryKeyHold> {
        self.installed_evidence(installed.manifest_sha256(), installed, Some(key))?.key_hold()?.ok_or_else(invalid)
    }

    fn installed_evidence(
        &self, manifest_sha256: [u8; 32], installed: &rtdb_db::InstalledBackup, key: Option<KeyEvidence>,
    ) -> Result<Evidence> {
        let candidate = self.candidate()?;
        if self.cell.is_none()
            || installed.activation_scope() != Some(self.scope()?)
            || installed.source_incarnation() != self.expected.incarnation
            || installed.destination_basis().incarnation() != *candidate.as_bytes()
            || installed.destination_basis().position() != installed.source_basis().position()
            || installed.manifest_sha256() != manifest_sha256
        {
            return Err(invalid());
        }
        let cut = SourceCut {
            lsn: installed.source_lsn().0,
            position: installed.source_basis().position(),
            system_time: installed.source_system_time().0,
        };
        self.evidence(cut, manifest_sha256, key)
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    objects_sha256: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::multi) struct KeyEvidence {
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
        match (self.version, self.key, self.objects_sha256) {
            (VERSION, None, None) => (),
            (HELD_KEY_VERSION, Some(key), None) if key.authority_sha256 != [0; 32] => {
                bytes.extend_from_slice(&key.generation.to_be_bytes());
                bytes.extend_from_slice(&key.authority_sha256);
            }
            (HOSTED_CUSTODY_VERSION, Some(key), Some(objects)) if key.authority_sha256 != [0; 32] && objects != [0; 32] => {
                bytes.extend_from_slice(&key.generation.to_be_bytes());
                bytes.extend_from_slice(&key.authority_sha256);
                bytes.extend_from_slice(&objects);
            }
            _ => return Err(invalid()),
        }
        Ok(bytes)
    }

    fn key_hold(&self) -> Result<Option<RecoveryKeyHold>> {
        let Some(key) = self.key else { return Ok(None); };
        let message = self.message()?;
        let key_message = if self.version == HOSTED_CUSTODY_VERSION {
            Self { version: HELD_KEY_VERSION, objects_sha256: None, ..self.clone() }.message()?
        } else { message };
        let digest = ring::digest::digest(&ring::digest::SHA256, &key_message);
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
pub(in crate::multi) struct Attestation {
    evidence: Evidence,
    signature: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    journal: Option<JournalAttestation>,
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
    pub(in crate::multi) fn sign_installation(
        intent: Intent,
        manifest_sha256: [u8; 32],
        installed: &rtdb_db::InstalledBackup,
        key_custody: Option<KeyEvidence>,
        signing_key: &Ed25519KeyPair,
    ) -> Result<Self> {
        Self::signed(intent.installed_evidence(manifest_sha256, installed, key_custody)?, signing_key)
    }

    #[cfg(all(unix, feature = "backup-object-holds"))]
    pub(in crate::multi) async fn sign_hosted_installation(
        authority: &super::super::ActivationAuthority,
        intent: Intent,
        installed: &rtdb_db::InstalledBackup,
        hold: RecoveryKeyHold,
        objects_sha256: [u8; 32],
        signing_key: &Ed25519KeyPair,
    ) -> Result<Self> {
        use journal_contract::Lsn;
        use super::super::recovery::{LossPolicy, Operation, Status};

        let key = KeyEvidence {
            generation: hold.generation.number,
            authority_sha256: authority.connection_digest()?,
        };
        let mut evidence = intent.installed_evidence(installed.manifest_sha256(), installed, Some(key))?;
        if evidence.key_hold()? != Some(hold) || intent.authority_id != authority.identity() {
            return Err(invalid());
        }
        let operation = Operation { predecessor: intent.expected, operation_id: intent.operation_id };
        match authority.recovery_status(&intent.database, operation).await? {
            Status::Pending { intent: selected, candidate, installation, final_source_lsn, .. }
                if selected.destination_cell == intent.cell && candidate == evidence.candidate
                    && (selected.loss_policy != LossPolicy::Lossless || final_source_lsn == Some(Lsn(evidence.source_lsn)))
                    && installation.is_none_or(|prior| {
                        prior.manifest_digest == evidence.manifest_sha256 && prior.key_hold == Some(hold)
                            && prior.source_cut == SourceCut {
                                lsn: evidence.source_lsn,
                                position: evidence.source_position,
                                system_time: evidence.source_system_time,
                            }
                    }) => (),
            _ => return Err(invalid()),
        }
        evidence.version = HOSTED_CUSTODY_VERSION;
        evidence.objects_sha256 = Some(objects_sha256);
        Self::signed(evidence, signing_key)
    }

    #[cfg(unix)]
    fn signed(evidence: Evidence, key: &Ed25519KeyPair) -> Result<Self> {
        let signature = key.sign(&evidence.message()?).as_ref().to_vec();
        Ok(Self {
            evidence,
            signature,
            journal: None,
        })
    }

    #[cfg(unix)]
    pub(super) fn read(path: &Path) -> Result<Self> {
        serde_json::from_slice(&read_file(path, 4096, false)?).map_err(|_| invalid())
    }

    #[cfg(unix)]
    pub(in crate::multi) fn held_key(&self) -> Result<(&Intent, RecoveryKeyHold)> {
        if self.evidence.candidate != self.evidence.intent.candidate()? {
            return Err(invalid());
        }
        let hold = self.evidence.key_hold()?.ok_or_else(invalid)?;
        Ok((&self.evidence.intent, hold))
    }

    #[cfg(unix)]
    pub(super) fn bind_journal(&mut self, evidence: JournalEvidence, signer: &Ed25519KeyPair) -> Result<RetainedJournal> {
        let (_, hold) = self.held_key()?;
        let journal = JournalAttestation::sign(evidence, hold, signer);
        let retained = journal.retained(hold);
        self.journal = Some(journal);
        Ok(retained)
    }

    #[cfg(unix)]
    pub(super) fn held_journal(&self) -> Result<(&Intent, RecoveryKeyHold, RetainedJournal)> {
        let (intent, key) = self.held_key()?;
        let journal = self.journal.as_ref().ok_or_else(invalid)?;
        Ok((intent, key, journal.retained(key)))
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
        let key_hold = evidence.key_hold()?;
        let journal_hold = self.journal.as_ref().map(|journal| {
            journal.verify(key_hold.ok_or_else(invalid)?, key)
        }).transpose()?;
        Ok(VerifiedSuccessor {
            predecessor: intent.expected.incarnation,
            successor: evidence.candidate,
            manifest_digest: evidence.manifest_sha256,
            activation_scope: Some(intent.scope()?),
            destination_cell: Some(cell),
            key_hold,
            journal_hold,
            source_cut: SourceCut {
                lsn: evidence.source_lsn,
                position: evidence.source_position,
                system_time: evidence.source_system_time,
            },
        })
    }
}

#[cfg(unix)]
pub(in crate::multi) fn signing_key(path: &Path) -> Result<Ed25519KeyPair> {
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
                objects_sha256: None,
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
                objects_sha256: None,
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
