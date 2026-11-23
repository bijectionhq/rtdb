use std::{iter::Peekable, sync::Arc, vec::IntoIter};

use bytes::Bytes;
use prost::Message;
use ring::digest::{Context, SHA256};
use rtdb_directory::{CheckpointTransferTerminal, DeploymentRecord};
use rtdb_logkeeper::{ArchivedReplicaEntry, CertifiedRangeReader};
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore};
use rtdb_types::{MemoryBudget, MemoryReservation, Result};

use crate::{
    archive::{
        checkpoint_transfer_prefix, encode, inconsistent, replica_suffix, sha256, unavailable,
    },
    cipher::SEALED_OVERHEAD,
    keys::{ArchiveKey, KeyService, legacy_archive_key},
    reference::key,
};

const MAX_RECEIPT_BYTES: usize = 16 << 10;

pub(super) struct PayloadBytes {
    pub(super) bytes: Bytes,
    _reservation: Arc<MemoryReservation>,
}

impl PayloadBytes {
    pub(super) async fn read(
        store: &dyn ObjectStore,
        key: &ObjectKey,
        memory: &Arc<MemoryBudget>,
    ) -> Result<Self> {
        let size = usize::try_from(store.head_fresh(key).await?.size).map_err(|_| bad_segment())?;
        let reserved = size
            .checked_mul(6)
            .and_then(|size| size.checked_add(4096))
            .ok_or_else(bad_segment)?;
        let reservation = memory.reserve(reserved)?;
        let bytes = IoRegion::retaining(reservation.clone(), store.observe_fresh_exact(key, size)).await?.value;
        Ok(Self {
            bytes,
            _reservation: reservation,
        })
    }
}

pub(super) struct ReplicaArtifact<'a> {
    store: &'a dyn ObjectStore,
    cipher: Arc<dyn ArchiveKey>,
    prefix: String,
    receipt: Bytes,
    chain_domain: &'static [u8],
}

pub(super) enum OrderedTransfer {
    Segments([u8; 32]),
    Legacy { entries: Vec<ArchivedReplicaEntry>, _reservation: Arc<MemoryReservation> },
}

impl OrderedTransfer {
    pub(super) async fn read_page(
        &self, artifact: &ReplicaArtifact<'_>, first: i64, through: i64, chain: &mut Context,
    ) -> Result<Vec<ArchivedReplicaEntry>> {
        match self {
            Self::Segments(_) => artifact.read_segment(first, through, chain).await,
            Self::Legacy { entries, .. } => {
                if require_dense(entries, first, through)? != through { return Err(bad_segment()); }
                Ok(entries.clone())
            }
        }
    }

    pub(super) fn finish(&self, chain: Context) -> Result<()> {
        match self {
            Self::Segments(expected) => require_chain(chain, *expected),
            Self::Legacy { .. } => Ok(()),
        }
    }
}

impl<'a> ReplicaArtifact<'a> {
    pub(super) async fn open_transfer(
        store: &'a dyn ObjectStore,
        keys: &dyn KeyService,
        record: &DeploymentRecord,
        terminal: &CheckpointTransferTerminal,
    ) -> Result<Self> {
        let receipt = encode(terminal)?;
        let material = keys
            .existing_key(&record.key_id())
            .await?
            .ok_or_else(|| unavailable("The original target media key is unavailable"))?;
        Self::new(
            store,
            legacy_archive_key(&material)?,
            checkpoint_transfer_prefix(record, terminal.fence.destination.intent.operation),
            receipt,
            b"RTDB checkpoint transfer segments v2\0",
        )
    }

    pub(super) fn new(
        store: &'a dyn ObjectStore,
        cipher: Arc<dyn ArchiveKey>,
        prefix: String,
        receipt: Bytes,
        chain_domain: &'static [u8],
    ) -> Result<Self> {
        if receipt.len() > MAX_RECEIPT_BYTES {
            return Err(inconsistent(
                "The replica archive receipt exceeds its bound",
            ));
        }
        Ok(Self {
            store,
            cipher,
            prefix,
            receipt,
            chain_domain,
        })
    }

    pub(super) async fn read_ordered_transfer(
        &self, record: &DeploymentRecord, terminal: &CheckpointTransferTerminal, expected: [u8; 32],
        memory: &Arc<MemoryBudget>,
    ) -> Result<OrderedTransfer> {
        let manifest = self.manifest()?;
        let manifest_len = 32 + self.receipt.len() + SEALED_OVERHEAD;
        match self.store.head_fresh(&manifest).await {
            Ok(head) if head.size == manifest_len as u64 => {
                let sealed = self.store.observe_fresh_exact(&manifest, manifest_len).await?.value;
                if sha256(&sealed) == expected {
                    return Ok(OrderedTransfer::Segments(self.open_manifest(&manifest, sealed).await?));
                }
            }
            Ok(_) | Err(rtdb_storage::Error::MissingObject(_)) => (),
            Err(error) => return Err(error.into()),
        }
        let operation = terminal.fence.destination.intent.operation;
        let legacy = key(&format!("{}/checkpoint-transfers/{operation}.v1", record.storage_prefix()))?;
        let maximum = 4 + self.receipt.len() + replica_suffix::MAX_SEGMENT_BYTES + SEALED_OVERHEAD;
        let size = usize::try_from(self.store.head_fresh(&legacy).await?.size)
            .ok().filter(|size| *size <= maximum).ok_or_else(bad_segment)?;
        let reservation = memory.reserve(size.checked_mul(6).ok_or_else(bad_segment)?)?;
        let sealed = self.store.observe_fresh_exact(&legacy, size).await?.value;
        if sha256(&sealed) != expected { return Err(bad_segment()); }
        let opened = self.cipher.open(legacy.as_str(), sealed).await?;
        let receipt_len = self.receipt.len();
        if opened.get(..4) != Some((receipt_len as u32).to_be_bytes().as_slice())
            || opened.get(4..4 + receipt_len) != Some(self.receipt.as_ref())
        {
            return Err(bad_segment());
        }
        let entries = replica_suffix::decode(opened.slice(4 + receipt_len..))?;
        let boundary = terminal.fence.destination.checkpoint.boundary;
        if entries.is_empty() {
            if boundary != terminal.replica_through { return Err(bad_segment()); }
        } else if require_dense(&entries, boundary.checked_add(1).ok_or_else(bad_segment)?, terminal.replica_through)? != terminal.replica_through {
            return Err(bad_segment());
        }
        Ok(OrderedTransfer::Legacy { entries, _reservation: reservation })
    }

    pub(super) fn chain(&self) -> Context {
        let mut digest = Context::new(&SHA256);
        digest.update(self.chain_domain);
        digest.update(&sha256(&self.receipt));
        digest
    }

    pub(super) async fn put_segment(
        &self,
        entries: &[ArchivedReplicaEntry],
        chain: &mut Context,
    ) -> Result<()> {
        let first = entries.first().ok_or_else(bad_segment)?.lsn;
        let through = entries.last().ok_or_else(bad_segment)?.lsn;
        require_dense(entries, first, through)?;
        let object = self.segment(first)?;
        let plaintext = replica_suffix::encode_records(entries)?;
        let digest = self.put(&object, &plaintext).await?;
        extend_chain(chain, first, through, &digest);
        Ok(())
    }

    pub(super) async fn read_segment(
        &self,
        first: i64,
        terminal: i64,
        chain: &mut Context,
    ) -> Result<Vec<ArchivedReplicaEntry>> {
        let object = self.segment(first)?;
        let sealed = self
            .read(&object, replica_suffix::MAX_SEGMENT_BYTES + SEALED_OVERHEAD)
            .await?;
        let entries = replica_suffix::decode(self.cipher.open(object.as_str(), sealed.clone()).await?)?;
        let through = require_dense(&entries, first, terminal)?;
        extend_chain(chain, first, through, &sha256(&sealed));
        Ok(entries)
    }

    /// The payload object belongs to its exact replica offset. The segment
    /// continues to carry the certified outer envelope.
    pub(super) async fn put_payload(
        &self,
        offset: i64,
        body: &[u8],
        chain: &mut Context,
    ) -> Result<()> {
        let object = self.payload(offset)?;
        let digest = self.put(&object, body).await?;
        extend_payload(chain, offset, &digest);
        Ok(())
    }

    pub(super) async fn read_payload(
        &self,
        offset: i64,
        chain: &mut Context,
        memory: &Arc<MemoryBudget>,
    ) -> Result<PayloadBytes> {
        let object = self.payload(offset)?;
        let mut body = PayloadBytes::read(self.store, &object, memory).await?;
        extend_payload(chain, offset, &sha256(&body.bytes));
        body.bytes = IoRegion::retaining(
            body._reservation.clone(), self.cipher.open(object.as_str(), body.bytes),
        ).await?;
        Ok(body)
    }

    pub(super) async fn put_manifest(&self, chain: Context) -> Result<[u8; 32]> {
        let object = self.manifest()?;
        let mut body = Vec::with_capacity(32 + self.receipt.len());
        body.extend_from_slice(chain.finish().as_ref());
        body.extend_from_slice(&self.receipt);
        self.put(&object, &body).await
    }

    pub(super) async fn read_manifest(&self, expected_digest: [u8; 32]) -> Result<[u8; 32]> {
        let object = self.manifest()?;
        let sealed = self
            .read(&object, 32 + self.receipt.len() + SEALED_OVERHEAD)
            .await?;
        if sha256(&sealed) != expected_digest {
            return Err(inconsistent(
                "The replica archive differs from its ordered digest",
            ));
        }
        self.open_manifest(&object, sealed).await
    }

    async fn open_manifest(&self, object: &ObjectKey, sealed: Bytes) -> Result<[u8; 32]> {
        let opened = self.cipher.open(object.as_str(), sealed).await?;
        if opened.get(32..) != Some(self.receipt.as_ref()) {
            return Err(inconsistent(
                "The replica archive names another exact receipt",
            ));
        }
        opened[..32].try_into().map_err(|_| bad_segment())
    }

    pub(super) async fn reseal(&self, scope: &str, entries: &mut [ArchivedReplicaEntry]) -> Result<()> {
        for entry in entries {
            entry.payload = self.cipher.seal(scope, entry.payload.clone()).await?;
        }
        Ok(())
    }

    fn manifest(&self) -> Result<ObjectKey> {
        key(&format!("{}/manifest", self.prefix)).map_err(Into::into)
    }

    fn segment(&self, first: i64) -> Result<ObjectKey> {
        key(&format!("{}/segments/{first:020}", self.prefix)).map_err(Into::into)
    }

    fn payload(&self, offset: i64) -> Result<ObjectKey> {
        key(&format!("{}/payloads/{offset:020}", self.prefix)).map_err(Into::into)
    }

    async fn put(&self, object: &ObjectKey, plaintext: &[u8]) -> Result<[u8; 32]> {
        let sealed = self.cipher.seal(object.as_str(), Bytes::copy_from_slice(plaintext)).await?;
        match self.store.head_fresh(object).await {
            Ok(prior) if prior.size == sealed.len() as u64 => (),
            Ok(_) => return Err(bad_segment()),
            Err(rtdb_storage::Error::MissingObject(_)) => {
                self.store.put(object, sealed.clone()).await?
            }
            Err(error) => return Err(error.into()),
        }
        let current = self
            .store
            .observe_fresh_exact(object, sealed.len())
            .await?
            .value;
        if current != sealed {
            return Err(inconsistent(
                "The replay artifact changed before receipt ordering",
            ));
        }
        Ok(sha256(&sealed))
    }

    async fn read(&self, object: &ObjectKey, maximum: usize) -> Result<Bytes> {
        let size = usize::try_from(self.store.head_fresh(object).await?.size)
            .ok()
            .filter(|size| *size <= maximum)
            .ok_or_else(|| {
                inconsistent("The ordered bootstrap object exceeds its storage bound")
            })?;
        Ok(self.store.observe_fresh_exact(object, size).await?.value)
    }
}

pub(super) struct CertifiedSegments {
    reader: CertifiedRangeReader,
    page: Peekable<IntoIter<ArchivedReplicaEntry>>,
}

impl CertifiedSegments {
    pub(super) fn new(reader: CertifiedRangeReader) -> Self {
        Self {
            reader,
            page: Vec::new().into_iter().peekable(),
        }
    }

    /// Regroup the certified raw records by size and offset, independent of
    /// the source server's page boundaries. An exact retry produces the same objects.
    pub(super) async fn next(&mut self) -> Result<Option<Vec<ArchivedReplicaEntry>>> {
        let mut entries = Vec::new();
        let mut encoded_bytes = 128;
        while entries.len() < replica_suffix::MAX_SEGMENT_ENTRIES {
            if self.page.peek().is_none() {
                let Some(page) = self.reader.next_page().await? else {
                    break;
                };
                self.page = page.into_iter().peekable();
            }
            let Some(entry) = self.page.peek() else {
                return Err(bad_segment());
            };
            // Reserve the length delimiter and existing segment format header.
            let next_size = encoded_bytes + entry.encoded_len() + 10;
            if next_size > replica_suffix::MAX_SEGMENT_BYTES {
                if entries.is_empty() {
                    return Err(bad_segment());
                }
                break;
            }
            entries.extend(self.page.next());
            encoded_bytes = next_size;
        }
        Ok((!entries.is_empty()).then_some(entries))
    }
}

pub(super) fn require_chain(chain: Context, expected: [u8; 32]) -> Result<()> {
    if chain.finish().as_ref() != expected.as_slice() {
        return Err(inconsistent(
            "The replica segments differ from the ordered archive manifest",
        ));
    }
    Ok(())
}

fn require_dense(entries: &[ArchivedReplicaEntry], first: i64, terminal: i64) -> Result<i64> {
    let through = entries.last().ok_or_else(bad_segment)?.lsn;
    if through > terminal
        || through == i64::MAX
        || entries.len() > replica_suffix::MAX_SEGMENT_ENTRIES
        || entries
            .iter()
            .enumerate()
            .any(|(index, entry)| first.checked_add(index as i64) != Some(entry.lsn))
    {
        return Err(bad_segment());
    }
    Ok(through)
}

fn extend_chain(chain: &mut Context, first: i64, through: i64, digest: &[u8; 32]) {
    chain.update(&first.to_be_bytes());
    chain.update(&through.to_be_bytes());
    chain.update(digest);
}

fn extend_payload(chain: &mut Context, offset: i64, digest: &[u8; 32]) {
    chain.update(b"payload\0");
    chain.update(&offset.to_be_bytes());
    chain.update(digest);
}

fn bad_segment() -> rtdb_types::Anomaly {
    inconsistent("The replica segment has another interval or exceeds its bound")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_directory::{CellMedia, Checkpoint, CheckpointStage, CheckpointStageIntent, CheckpointTransferFence};
    use rtdb_types::{Timestamp, TransactionKey, TxId};
    use uuid::Uuid;

    #[tokio::test]
    async fn an_ordered_inline_bootstrap_reopens_without_trusting_a_provisional_segmented_manifest() -> Result<()> {
        let bucket = rtdb_storage::MemoryStore::default();
        let target = DeploymentRecord::new("target", "acme", Vec::new(), 0);
        let operation = Uuid::new_v4();
        let terminal = CheckpointTransferTerminal {
            fence: CheckpointTransferFence {
                destination: CheckpointStage {
                    intent: CheckpointStageIntent {
                        operation, source: Uuid::new_v4(), capture: Uuid::new_v4(), source_manifest_sha256: "source".into(),
                        destination: target.id.parse().unwrap(), name: target.name.clone(), org: target.org.clone(),
                    },
                    source_incarnation: 1, source_log_epoch: 0,
                    checkpoint: Checkpoint { block: 1, boundary: 7, source: 1, system_time: 10, root_sha256: Some([1; 32]) },
                    media: CellMedia { objects: [4; 32], keys: "keys".into() },
                    transfer_only: true, staged_manifest_sha256: Some("staged".into()),
                },
                source_writer_epoch: 1, fenced_writer_epoch: 2,
            },
            replica_through: 8, source_through: 2, basis: TransactionKey { tx_id: TxId(2), system_time: Timestamp(11) },
            replica_sha256: Some([2; 32]), lifetime_floors_sha256: [3; 32],
        };
        let key_bytes = [4; crate::keys::KEY_BYTES];
        let receipt = encode(&terminal)?;
        let artifact = ReplicaArtifact::new(&bucket, legacy_archive_key(&key_bytes)?, checkpoint_transfer_prefix(&target, operation), receipt.clone(), b"test\0")?;
        let entry = ArchivedReplicaEntry { lsn: 8, epoch: 1, timestamp: 11, payload: Bytes::from_static(b"inline") };
        let object = key(&format!("{}/checkpoint-transfers/{operation}.v1", target.storage_prefix()))?;
        let mut plaintext = (receipt.len() as u32).to_be_bytes().to_vec();
        plaintext.extend_from_slice(&receipt);
        plaintext.extend_from_slice(&replica_suffix::encode_records(std::slice::from_ref(&entry))?);
        let sealed = crate::cipher::Cipher::new(&key_bytes)?.seal_deterministic(object.as_str(), &plaintext)?;
        let digest = sha256(&sealed);
        bucket.put(&object, sealed).await?;
        artifact.put_manifest(artifact.chain()).await?;
        let memory = MemoryBudget::new(1 << 20);
        let ordered = artifact.read_ordered_transfer(&target, &terminal, digest, &memory).await?;
        let mut chain = artifact.chain();
        assert!(ordered.read_page(&artifact, 8, 8, &mut chain).await? == [entry]);
        ordered.finish(chain)?;
        assert!(artifact.read_ordered_transfer(&target, &terminal, [0; 32], &memory).await.is_err());
        Ok(())
    }
}
