use std::{iter::Peekable, vec::IntoIter};

use bytes::Bytes;
use prost::Message;
use ring::digest::{Context, SHA256};
use rtdb_directory::{CheckpointTransferTerminal, DeploymentRecord};
use rtdb_logkeeper::{ArchivedReplicaEntry, CertifiedRangeReader};
use rtdb_storage::{ObjectKey, ObjectStore};
use rtdb_types::Result;

use crate::{
    archive::{
        checkpoint_transfer_prefix, encode, inconsistent, replica_suffix, sha256, unavailable,
    },
    cipher::{Cipher, SEALED_OVERHEAD},
    keys::KeyService,
    reference::key,
};

const MAX_RECEIPT_BYTES: usize = 16 << 10;

pub(super) struct ReplicaArtifact<'a> {
    store: &'a dyn ObjectStore,
    cipher: Cipher,
    prefix: String,
    receipt: Bytes,
    chain_domain: &'static [u8],
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
            &material,
            checkpoint_transfer_prefix(record, terminal.fence.destination.intent.operation),
            receipt,
            b"RTDB checkpoint transfer segments v2\0",
        )
    }

    pub(super) fn new(
        store: &'a dyn ObjectStore,
        key: &[u8],
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
            cipher: Cipher::new(key)?,
            prefix,
            receipt,
            chain_domain,
        })
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
        let entries = replica_suffix::decode(self.cipher.open(object.as_str(), &sealed)?)?;
        let through = require_dense(&entries, first, terminal)?;
        extend_chain(chain, first, through, &sha256(&sealed));
        Ok(entries)
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
        let opened = self.cipher.open(object.as_str(), &sealed)?;
        if opened.get(32..) != Some(self.receipt.as_ref()) {
            return Err(inconsistent(
                "The replica archive names another exact receipt",
            ));
        }
        opened[..32].try_into().map_err(|_| bad_segment())
    }

    pub(super) fn reseal(&self, scope: &str, entries: &mut [ArchivedReplicaEntry]) -> Result<()> {
        for entry in entries {
            entry.payload = self.cipher.seal_deterministic(scope, &entry.payload)?;
        }
        Ok(())
    }

    fn manifest(&self) -> Result<ObjectKey> {
        key(&format!("{}/manifest", self.prefix))
    }

    fn segment(&self, first: i64) -> Result<ObjectKey> {
        key(&format!("{}/segments/{first:020}", self.prefix))
    }

    async fn put(&self, object: &ObjectKey, plaintext: &[u8]) -> Result<[u8; 32]> {
        let sealed = self.cipher.seal_deterministic(object.as_str(), plaintext)?;
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

fn bad_segment() -> rtdb_types::Anomaly {
    inconsistent("The replica segment has another interval or exceeds its bound")
}
