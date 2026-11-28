use std::{path::Path, sync::Arc, time::Duration};

use journal_contract::{
    CheckpointAnchor, CheckpointCustody, CheckpointHold, Journal, JournalError, Lsn, StreamId,
};
use ring::signature::{ED25519, UnparsedPublicKey};
use rtdb_crypt::RecoveryKeyHold;
use rtdb_types::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{configuration, conflict, journal_error};
use crate::multi::{CONSENSUS_SCHEME, Credentials, JournalConfig, io_error};

pub(in crate::multi) struct KeeperJournal {
    pub journal: Arc<rtdb_stream_journal::ConsensusJournal>,
    pub connection_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::multi) struct JournalEvidence {
    pub connection_sha256: [u8; 32],
    pub anchor: CheckpointAnchor,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::multi) struct RetainedJournal {
    pub connection_sha256: [u8; 32],
    #[serde(with = "hold_serde")]
    pub hold: CheckpointHold,
}

mod hold_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    #[derive(Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    struct Order {
        operation_id: Uuid,
        installation_sha256: [u8; 32],
        anchor: CheckpointAnchor,
    }

    pub(super) fn serialize<S: Serializer>(
        hold: &CheckpointHold,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Order {
            operation_id: Uuid::from_u128(hold.operation_id),
            installation_sha256: hold.installation_sha256,
            anchor: hold.anchor,
        }
        .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<CheckpointHold, D::Error> {
        let order = Order::deserialize(deserializer)?;
        Ok(CheckpointHold {
            operation_id: order.operation_id.as_u128(),
            installation_sha256: order.installation_sha256,
            anchor: order.anchor,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct JournalAttestation {
    evidence: JournalEvidence,
    signature: Vec<u8>,
}

impl JournalAttestation {
    #[cfg(unix)]
    pub(super) fn sign(
        evidence: JournalEvidence,
        key: RecoveryKeyHold,
        signer: &ring::signature::Ed25519KeyPair,
    ) -> Self {
        Self {
            evidence,
            signature: signer.sign(&evidence.message(key)).as_ref().to_vec(),
        }
    }

    pub(super) fn verify(
        &self,
        key: RecoveryKeyHold,
        public_key: &[u8; 32],
    ) -> Result<RetainedJournal> {
        UnparsedPublicKey::new(&ED25519, public_key)
            .verify(&self.evidence.message(key), &self.signature)
            .map_err(|_| conflict())?;
        Ok(self.retained(key))
    }

    pub(super) fn retained(&self, key: RecoveryKeyHold) -> RetainedJournal {
        self.evidence.retained(key)
    }
}

impl JournalEvidence {
    pub(super) fn retained(self, key: RecoveryKeyHold) -> RetainedJournal {
        RetainedJournal {
            connection_sha256: self.connection_sha256,
            hold: CheckpointHold {
                operation_id: key.operation.as_u128(),
                installation_sha256: Sha256::digest(self.message(key)).into(),
                anchor: self.anchor,
            },
        }
    }

    fn message(self, key: RecoveryKeyHold) -> Vec<u8> {
        let mut message = b"rtdb.activation.journal-custody/v1\0".to_vec();
        message.extend_from_slice(&key.installation_sha256);
        message.extend_from_slice(&self.connection_sha256);
        message.extend_from_slice(&self.anchor.announced_at.0.to_be_bytes());
        message.extend_from_slice(&self.anchor.covered_through.0.to_be_bytes());
        message.extend_from_slice(&self.anchor.record_sha256);
        message
    }
}

impl KeeperJournal {
    pub(in crate::multi) fn connect(url: &str, credentials: &Credentials) -> Result<Self> {
        let path = Path::new(
            url.strip_prefix(CONSENSUS_SCHEME)
                .ok_or_else(configuration)?,
        );
        if !path.is_absolute() {
            return Err(configuration());
        }
        let config = JournalConfig::read(path).map_err(io_error)?;
        let ca = std::fs::read(&credentials.ca).map_err(io_error)?;
        let peers = serde_json::to_vec(&(config.catalog, config.catalog_voters, &config.keepers))
            .map_err(|_| configuration())?;
        let mut binding = Sha256::new();
        binding.update(b"rtdb.activation.connection/v1\0");
        binding.update(Sha256::digest(peers));
        binding.update(Sha256::digest(ca));
        let journal =
            rtdb_stream_journal::ConsensusJournal::new(credentials, &config).map_err(io_error)?;
        Ok(Self {
            journal: Arc::new(journal),
            connection_sha256: binding.finalize().into(),
        })
    }

    pub(in crate::multi) async fn selected(&self, candidate: Uuid) -> Result<JournalEvidence> {
        Ok(self.observe_selection(candidate).await?.0)
    }

    pub(in crate::multi) async fn selected_for_readback(
        &self,
        candidate: Uuid,
        head: Lsn,
        anchor: CheckpointAnchor,
    ) -> Result<JournalEvidence> {
        let (selected, observed_head) = self.observe_selection(candidate).await?;
        if selected.anchor != anchor || observed_head != head {
            return Err(conflict());
        }
        Ok(selected)
    }

    #[cfg(all(unix, feature = "backup-object-holds"))]
    pub(super) async fn selected_for_abandonment(
        &self, candidate: Uuid, head: Lsn, anchor: Option<CheckpointAnchor>,
    ) -> Result<JournalEvidence> {
        let (selected, observed_head) = self.observe_selection(candidate).await?;
        if observed_head < head || selected.anchor.announced_at > head
            || anchor.is_some_and(|anchor| selected.anchor != anchor)
        {
            return Err(conflict());
        }
        Ok(selected)
    }

    async fn observe_selection(&self, candidate: Uuid) -> Result<(JournalEvidence, Lsn)> {
        let stream = StreamId(candidate.as_u128());
        let receipt = self
            .journal
            .selected_checkpoint(stream)
            .await
            .map_err(journal_error)?
            .ok_or_else(|| journal_error(JournalError::NotFound))?;
        let at = receipt.checkpoint_lsn;
        let page = self
            .journal
            .read_page(stream, at, at, 1, Duration::ZERO)
            .await
            .map_err(journal_error)?;
        let [(found, bytes)] = page.records.as_slice() else {
            return Err(conflict());
        };
        if *found != at || page.floor != receipt.logical_floor_lsn || page.head < at {
            return Err(conflict());
        }
        Ok((JournalEvidence {
            connection_sha256: self.connection_sha256,
            anchor: CheckpointAnchor {
                announced_at: at,
                covered_through: receipt.covered_through_lsn,
                record_sha256: Sha256::digest(bytes).into(),
            },
        }, page.head))
    }

    pub(in crate::multi) async fn retain(
        &self,
        candidate: Uuid,
        retained: RetainedJournal,
    ) -> Result<()> {
        let hold = self.require_connection(retained)?;
        let observed = self
            .journal
            .retain_checkpoint(StreamId(candidate.as_u128()), hold)
            .await
            .map_err(journal_error)?;
        if observed
            != (CheckpointCustody {
                hold,
                released: false,
            })
        {
            return Err(conflict());
        }
        Ok(())
    }

    #[cfg(all(unix, feature = "backup-object-holds"))]
    pub(in crate::multi) async fn retire_unacquired(&self, candidate: Uuid) -> Result<()> {
        let stream = StreamId(candidate.as_u128());
        match self.journal.head(stream).await {
            Ok(_) => (),
            Err(JournalError::Deleted) => return Ok(()),
            Err(JournalError::NotFound) => match self.journal.create_immutable(stream).await {
                Ok(()) | Err(JournalError::Conflict { .. }) => (),
                Err(JournalError::Deleted) => return Ok(()),
                Err(error) => return Err(journal_error(error)),
            },
            Err(error) => return Err(journal_error(error)),
        }
        match self.journal.require_immutable(stream).await {
            Ok(()) => (),
            Err(JournalError::Deleted) => return Ok(()),
            Err(error) => return Err(journal_error(error)),
        }
        match self.journal.delete(stream).await {
            Ok(()) | Err(JournalError::Deleted) => (),
            Err(error) => return Err(journal_error(error)),
        }
        match self.journal.head(stream).await {
            Err(JournalError::Deleted) => Ok(()),
            Ok(_) => Err(conflict()),
            Err(error) => Err(journal_error(error)),
        }
    }

    pub(super) async fn require_retained(
        &self,
        candidate: Uuid,
        retained: RetainedJournal,
    ) -> Result<()> {
        let hold = self.require_connection(retained)?;
        let observed = self
            .journal
            .checkpoint_custody(StreamId(candidate.as_u128()))
            .await
            .map_err(journal_error)?;
        if observed
            != Some(CheckpointCustody {
                hold,
                released: false,
            })
        {
            return Err(conflict());
        }
        Ok(())
    }

    pub(super) async fn release(&self, candidate: Uuid, retained: RetainedJournal) -> Result<()> {
        let hold = self.require_connection(retained)?;
        let observed = self
            .journal
            .release_checkpoint(StreamId(candidate.as_u128()), hold)
            .await
            .map_err(journal_error)?;
        if observed
            != (CheckpointCustody {
                hold,
                released: true,
            })
        {
            return Err(conflict());
        }
        Ok(())
    }

    #[cfg(all(unix, feature = "backup-object-holds"))]
    pub(in crate::multi) async fn abandon(&self, candidate: Uuid, retained: RetainedJournal) -> Result<()> {
        let hold = self.require_connection(retained)?;
        let observed = self.journal.checkpoint_custody(StreamId(candidate.as_u128())).await.map_err(journal_error)?;
        if observed.is_none() {
            // Record a terminal disposition even if the attestor never reached the keeper.
            if let Err(error) = self.retain(candidate, retained).await {
                let observed = self.journal.checkpoint_custody(StreamId(candidate.as_u128())).await.map_err(journal_error)?;
                if observed.is_none_or(|state| state.hold != hold) {
                    return Err(error);
                }
            }
        }
        self.release(candidate, retained).await
    }

    #[cfg(unix)]
    pub(super) async fn require_replacement(
        &self,
        candidate: Uuid,
        retained: RetainedJournal,
    ) -> Result<()> {
        let hold = self.require_connection(retained)?;
        let observed = self
            .journal
            .checkpoint_custody(StreamId(candidate.as_u128()))
            .await
            .map_err(journal_error)?;
        if observed
            != Some(CheckpointCustody {
                hold,
                released: true,
            })
        {
            return Err(replacement_pending());
        }
        let selected = self.selected(candidate).await?;
        if selected.anchor.announced_at <= hold.anchor.announced_at
            || selected.anchor.covered_through < hold.anchor.announced_at
        {
            return Err(replacement_pending());
        }
        Ok(())
    }

    fn require_connection(&self, retained: RetainedJournal) -> Result<CheckpointHold> {
        if retained.connection_sha256 == self.connection_sha256 {
            return Ok(retained.hold);
        }
        Err(conflict())
    }
}

#[cfg(unix)]
fn replacement_pending() -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::new(
        rtdb_types::AnomalyCategory::Conflict,
        "rtdb/activation-custody-replacement",
        "The published successor has not selected a replacement checkpoint",
    )
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use bytes::Bytes;
    use journal_contract::{CheckpointInstallation, Epoch, Lsn, WriterSessionId};
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use rtdb_crypt::Generation;
    use stream_logkeeper::stream::consensus::{
        CLUSTER_VERSION_CHECKPOINT_CUSTODY, testing::LocalCluster,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_journal_custody_binds_the_staged_key_and_live_keeper_hold() {
        const PRINCIPAL: &str = "urn:logkeeper:client:journal-custody";
        let keepers = LocalCluster::start_replicated_catalog(&[PRINCIPAL])
            .await
            .unwrap();
        keepers
            .finalize_version(PRINCIPAL, CLUSTER_VERSION_CHECKPOINT_CUSTODY)
            .await
            .unwrap();
        let config = keepers.journal_config(64 << 20, 4 << 20);
        let journal = KeeperJournal {
            journal: Arc::new(keepers.journal(PRINCIPAL, &config).unwrap()),
            connection_sha256: [7; 32],
        };
        let candidate = Uuid::new_v4();
        let stream = StreamId(candidate.as_u128());
        journal.journal.create_immutable(stream).await.unwrap();
        let sealed = journal
            .journal
            .seal_session(stream, Epoch(0), WriterSessionId(1))
            .await
            .unwrap();
        journal
            .journal
            .install_checkpoint(
                sealed.fence,
                CheckpointInstallation {
                    operation_id: 8,
                    checkpoint_lsn: Lsn(1),
                    covered_through_lsn: Lsn(0),
                    record: Bytes::from_static(b"exact restored checkpoint"),
                },
            )
            .await
            .unwrap();
        let key = RecoveryKeyHold {
            authority: Uuid::new_v4(),
            authority_sha256: [10; 32],
            operation: Uuid::new_v4(),
            generation: Generation {
                database: candidate,
                number: 1,
            },
            installation_sha256: [11; 32],
        };
        let signer = Ed25519KeyPair::from_seed_unchecked(&[12; 32]).unwrap();
        let public_key = signer.public_key().as_ref().try_into().unwrap();
        let evidence = journal.selected(candidate).await.unwrap();
        let attestation = JournalAttestation::sign(evidence, key, &signer);
        let hold = attestation.verify(key, &public_key).unwrap();
        let encoded = serde_json::to_value(hold).unwrap();
        assert_eq!(encoded["hold"]["operation_id"], key.operation.to_string());
        assert_eq!(
            serde_json::from_value::<RetainedJournal>(encoded).unwrap(),
            hold
        );
        let foreign = RecoveryKeyHold {
            installation_sha256: [13; 32],
            ..key
        };
        assert!(attestation.verify(foreign, &public_key).is_err());
        assert!(journal.require_retained(candidate, hold).await.is_err());
        journal.retain(candidate, hold).await.unwrap();
        journal.require_retained(candidate, hold).await.unwrap();
        let substitute = RetainedJournal {
            connection_sha256: [14; 32],
            ..hold
        };
        assert!(
            journal
                .require_retained(candidate, substitute)
                .await
                .is_err()
        );
        #[cfg(feature = "backup-object-holds")]
        assert!(journal.require_replacement(candidate, hold).await.is_err());
        journal.release(candidate, hold).await.unwrap();
        assert!(journal.require_retained(candidate, hold).await.is_err());
        #[cfg(feature = "backup-object-holds")]
        {
            assert!(journal.require_replacement(candidate, hold).await.is_err());
            journal
                .journal
                .install_checkpoint(
                    sealed.fence,
                    CheckpointInstallation {
                        operation_id: 9,
                        checkpoint_lsn: Lsn(2),
                        covered_through_lsn: Lsn(1),
                        record: Bytes::from_static(b"serving checkpoint"),
                    },
                )
                .await
                .unwrap();
            journal.require_replacement(candidate, hold).await.unwrap();
        }
        journal.release(candidate, hold).await.unwrap();
    }
}
