use rtdb_format::envelope::{ReplicaBody, ReplicaFrame, ReplicaPeek};
use rtdb_types::{StorageEpoch, StorageVersion};
use serde::{Deserialize, Serialize};

use super::*;
use crate::log::CheckpointReplicaReference;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReferenceEvidence {
    offset: i64,
    publication: [u8; 16],
    sealed_sha256: [u8; 32],
    reference: CheckpointReplicaReference,
}

impl KafkaFrameKeys {
    pub fn replica_reference(bytes: &[u8]) -> Option<CheckpointReplicaReference> {
        if ReplicaPeek::read(bytes).is_some() {
            return Some(CheckpointReplicaReference::Inline);
        }
        let envelope = ReplicaFrame::decode(bytes).ok()?.into_envelope()?;
        let ReplicaBody::OversizedMessage(message) = &envelope.message else {
            return Some(CheckpointReplicaReference::Inline);
        };
        if envelope.offload_publication().ok()?.is_some() {
            return None;
        }
        let storage_version = StorageVersion(message.storage_version.unwrap_or_default());
        let storage_epoch = StorageEpoch(message.storage_epoch.unwrap_or_default());
        let key = envelope
            .oversized_payload(storage_version, storage_epoch)
            .ok()??;
        if key.len() > 1024 {
            return None;
        }
        Some(CheckpointReplicaReference::Detached {
            storage_version,
            storage_epoch,
            key,
        })
    }

    pub(super) fn record_reference_locked(
        &self,
        offset: i64,
        frame: &[u8],
        reference: &CheckpointReplicaReference,
    ) -> crate::Result<()> {
        let directory = self.0.path.join("replica-references");
        match fs::create_dir(&directory) {
            Ok(()) => sync_directory(&self.0.path).map_err(io(&directory))?,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => (),
            Err(error) => return Err(io(&directory)(error)),
        }
        let evidence = ReferenceEvidence {
            offset,
            publication: kafka_publication(frame)?.into_bytes(),
            sealed_sha256: digest(&SHA256, frame)
                .as_ref()
                .try_into()
                .expect("SHA-256 digest"),
            reference: reference.clone(),
        };
        let bytes = serde_json::to_vec(&evidence).map_err(invalid_reference)?;
        let path = directory.join(offset.to_string());
        match fs::read(&path) {
            Ok(prior) if prior == bytes => sync_directory(&directory).map_err(io(&directory)),
            Ok(_) => Err(invalid_reference(
                "the sealed offset changed its object reference",
            )),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                write_kafka_marker(&path, &bytes, false)
            }
            Err(error) => Err(io(&path)(error)),
        }
    }

    pub async fn prior_replica_references(
        &self,
        first_live: i64,
    ) -> crate::Result<Vec<CheckpointReplicaReference>> {
        if first_live < 0 {
            return Err(invalid_reference("negative broker floor"));
        }
        let path = Arc::clone(&self.0.path);
        self.0
            .region
            .run_blocking(move || {
                let floor = path.join("retired-before");
                let _gate = lock(&floor)?;
                let classic_floor = kafka_retired_before(&floor)?;
                let references = path.join("replica-references");
                let positions = path.join("positions");
                let mut detached = std::collections::BTreeSet::new();
                for offset in 0..first_live {
                    let body = fs::read(references.join(offset.to_string())).map_err(|error| {
                        if error.kind() == ErrorKind::NotFound {
                            Error::UnavailableLogRange {
                                requested: offset,
                                first_retained: first_live,
                            }
                        } else {
                            io(&references)(error)
                        }
                    })?;
                    let evidence: ReferenceEvidence =
                        serde_json::from_slice(&body).map_err(invalid_reference)?;
                    if evidence.offset != offset || evidence.publication == [0; 16] {
                        return Err(invalid_reference("foreign offset or publication"));
                    }
                    if offset >= classic_floor {
                        let mut expected = [0; 48];
                        expected[..16].copy_from_slice(&evidence.publication);
                        expected[16..].copy_from_slice(&evidence.sealed_sha256);
                        let marker = positions.join(offset.to_string());
                        if fs::read(&marker).map_err(io(&marker))? != expected {
                            return Err(invalid_reference(
                                "reference identity differs from its sealed offset",
                            ));
                        }
                    }
                    if matches!(
                        &evidence.reference,
                        CheckpointReplicaReference::Detached { .. }
                    ) {
                        detached.insert(evidence.reference);
                    }
                }
                Ok(detached.into_iter().collect())
            })
            .await
    }
}

fn invalid_reference(error: impl std::fmt::Display) -> Error {
    Error::Task(format!("Invalid Kafka replica reference evidence: {error}"))
}

#[cfg(test)]
mod tests {
    use rtdb_format::envelope::{ReplicaBody, ReplicaEnvelope};
    use rtdb_types::{BlockIndex, Term};

    use super::*;

    #[tokio::test]
    async fn references_survive_the_classic_floor_and_require_every_retired_position() -> Result<()>
    {
        let directory = crate::sealing::private_tempdir();
        let vault = ClassicKeyVault::open(directory.path())?;
        let topic = Uuid::new_v4().into_bytes();
        let keys = vault.kafka_log_keys(topic, true, IoRegion::new())?;
        let oversized = ReplicaEnvelope::offloaded_body(
            Term(1),
            None,
            StorageVersion(1),
            StorageEpoch(2),
            BlockIndex(3),
            "body".into(),
            b"detached",
        );
        let reference = KafkaFrameKeys::replica_reference(&oversized.encode()).unwrap();
        let frame = keys
            .seal_kafka_record(Bytes::from(oversized.encode()))
            .await?;
        keys.record_kafka_publication_and_reference(0, &frame, Some(&reference))?;
        let inline = ReplicaEnvelope {
            term: Term(1),
            term_sequence: None,
            message: ReplicaBody::NoOp(Default::default()),
        };
        let frame = keys.seal_kafka_record(Bytes::from(inline.encode())).await?;
        keys.record_kafka_publication(1, &frame)?;
        keys.open_kafka_publication_with_references(1, frame, true)
            .await?;
        let missing = keys
            .seal_kafka_record(Bytes::from_static(b"legacy"))
            .await?;
        keys.record_kafka_publication(2, &missing)?;
        assert!(keys.retire_kafka_before(3).await?);
        let reopened = vault.kafka_log_keys(topic, false, IoRegion::new())?;
        assert_eq!(reopened.prior_replica_references(2).await?, vec![reference]);
        assert!(reopened.prior_replica_references(3).await.is_err());
        Ok(())
    }
}
