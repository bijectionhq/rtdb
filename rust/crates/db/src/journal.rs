use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use journal_contract::{BoxFuture, Epoch, Journal, JournalError, Lsn, Page, StreamId};
use rtdb_crypt::{Context, Generation, Kms};
use rtdb_format::journal::Record;
use rtdb_tx::transact::TransactionRecord;
use rtdb_types::{Anomaly, AnomalyCategory, Result, TimedJournalCutoff, Timestamp};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const ENVELOPE_VERSION: u8 = 1;
const PAGE_SIZE: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimedOriginal {
    pub stream: StreamId,
    pub epoch: Epoch,
    pub at: Lsn,
    pub payload_sha256: [u8; 32],
    pub cutoff: TimedJournalCutoff,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimedOriginalProof {
    pub original: TimedOriginal,
    pub quorum_sync_at: Timestamp,
    pub evidence_sha256: [u8; 32],
}

impl TimedOriginalProof {
    pub(crate) fn verify_exact(self, original: TimedOriginal) -> Result<()> {
        if self.original != original || self.evidence_sha256 == [0; 32]
            || self.quorum_sync_at.0 <= 0 || self.quorum_sync_at >= original.cutoff.before()
        {
            return Err(Anomaly::fault("rtdb/timed-original-proof", "The provider returned a different timed original quorum proof"));
        }
        Ok(())
    }
}

/// Commits a contiguous append group through its returned head.
/// An error may leave a committed prefix of the group.
pub trait JournalBatchAppender: Send + Sync {
    fn append_batch(
        &self,
        stream: StreamId,
        epoch: Epoch,
        records: Vec<(Lsn, Bytes)>,
    ) -> BoxFuture<'_, journal_contract::Result<Lsn>>;

    fn maximum_timed_original_bytes(&self) -> Option<usize> { None }

    /// Returns verified original quorum evidence for the exact sealed bytes.
    fn append_timed_original(
        &self,
        _original: TimedOriginal,
        _payload: Bytes,
    ) -> BoxFuture<'_, journal_contract::Result<TimedOriginalProof>> {
        Box::pin(async { Err(JournalError::Unavailable) })
    }

    /// Reopens retained proofs under the original keeper identities after writer replacement.
    fn prove_timed_original(
        &self,
        _original: TimedOriginal,
    ) -> BoxFuture<'_, journal_contract::Result<TimedOriginalProof>> {
        Box::pin(async { Err(JournalError::Unavailable) })
    }
}

#[derive(Clone)]
pub(crate) struct Stream {
    pub(crate) database: Uuid,
    journal: Arc<dyn Journal>,
    pub(crate) batch: Option<Arc<dyn JournalBatchAppender>>,
    pub(crate) kms: Arc<dyn Kms>,
    pub(crate) manifests: Arc<crate::replay::ManifestCache>,
}

pub(crate) struct Entry {
    pub(crate) epoch: Epoch,
    pub(crate) generation: u64,
    pub(crate) record: Record,
}

impl Stream {
    pub(crate) fn new(database: Uuid, journal: Arc<dyn Journal>, kms: Arc<dyn Kms>) -> Self {
        Self {
            database,
            journal,
            batch: None,
            kms,
            manifests: Arc::default(),
        }
    }

    fn id(&self) -> StreamId {
        StreamId(u128::from_be_bytes(*self.database.as_bytes()))
    }

    pub(crate) fn timed_original(&self, at: Lsn, epoch: Epoch, payload: &[u8], cutoff: TimedJournalCutoff) -> TimedOriginal {
        TimedOriginal { stream: self.id(), epoch, at, payload_sha256: Sha256::digest(payload).into(), cutoff }
    }

    pub(crate) async fn create(&self) -> Result<()> {
        self.kms
            .create(Generation {
                database: self.database,
                number: 1,
            })
            .await?;
        self.journal.create(self.id()).await.map_err(journal_error)
    }

    pub(crate) async fn head(&self) -> Result<Lsn> {
        self.journal.head(self.id()).await.map_err(journal_error)
    }

    pub(crate) async fn seal(&self, epoch: Epoch) -> journal_contract::Result<Lsn> {
        self.journal.seal(self.id(), epoch).await
    }

    pub(crate) async fn read(&self, from: Lsn) -> journal_contract::Result<Page> {
        self.journal
            .read(self.id(), from, PAGE_SIZE, Duration::ZERO)
            .await
    }

    /// Waits up to `wait` for a committed record after `applied`.
    /// A reader below the retained floor, or behind an operator recovery, also has a record to apply.
    pub(crate) async fn holds_after(&self, applied: Lsn, wait: Duration) -> Result<bool> {
        match self.journal.read(self.id(), Lsn(applied.0 + 1), 1, wait).await {
            Ok(page) => Ok(!page.records.is_empty()),
            Err(JournalError::Trimmed { .. } | JournalError::Recovered { .. }) => Ok(true),
            Err(error) => Err(journal_error(error)),
        }
    }

    pub(crate) async fn trim(&self, epoch: Epoch, through: Lsn) -> journal_contract::Result<()> {
        self.journal.trim(self.id(), epoch, through).await
    }

    pub(crate) async fn encode(&self, lsn: Lsn, entry: &Entry) -> Result<Bytes> {
        let record = entry.record.encode()?;
        let mut plaintext = Vec::with_capacity(9 + record.len());
        plaintext.push(ENVELOPE_VERSION);
        plaintext.extend_from_slice(&entry.epoch.0.to_be_bytes());
        plaintext.extend_from_slice(&record);
        let generation = Generation {
            database: self.database,
            number: entry.generation,
        };
        let context = Context::Journal { lsn: lsn.0 };
        Ok(
            rtdb_crypt::seal(self.kms.as_ref(), generation, context, &plaintext)
                .await?
                .into(),
        )
    }

    pub(crate) async fn decode(&self, lsn: Lsn, bytes: &[u8]) -> Result<Entry> {
        let context = Context::Journal { lsn: lsn.0 };
        let plaintext = rtdb_crypt::open(self.kms.as_ref(), self.database, context, bytes).await?;
        let (header, record) = plaintext
            .bytes
            .split_first_chunk::<9>()
            .ok_or_else(invalid_entry)?;
        if header[0] != ENVELOPE_VERSION {
            return Err(invalid_entry());
        }
        let epoch = Epoch(u64::from_be_bytes(header[1..].try_into().unwrap()));
        if epoch == Epoch(0) {
            return Err(invalid_entry());
        }
        let record = Record::decode(record)?;
        if let Record::Tx { payload, .. } = &record
            && let Some(cutoff) = TransactionRecord::funded_journal_cutoff(payload)?
        {
            let batch = self.batch.as_ref().ok_or_else(rtdb_tx::transact::funding_unsupported)?;
            let original = self.timed_original(lsn, epoch, bytes, cutoff);
            batch.prove_timed_original(original).await.map_err(journal_error)?.verify_exact(original)?;
        }
        Ok(Entry { epoch, generation: plaintext.generation.number, record })
    }

    pub(crate) async fn append(
        &self,
        epoch: Epoch,
        at: Lsn,
        payload: Bytes,
    ) -> journal_contract::Result<Lsn> {
        self.journal.append(self.id(), epoch, at, payload).await
    }

    pub(crate) async fn append_group(
        &self,
        epoch: Epoch,
        mut records: Vec<(Lsn, Bytes)>,
    ) -> journal_contract::Result<Lsn> {
        if let Some(batch) = &self.batch {
            return batch.append_batch(self.id(), epoch, records).await;
        }
        if records.len() != 1 {
            return Err(JournalError::Unavailable);
        }
        let (at, payload) = records.pop().expect("one record");
        self.append(epoch, at, payload).await
    }
}

pub(crate) fn next(lsn: Lsn) -> Result<Lsn> {
    lsn.0
        .checked_add(1)
        .map(Lsn)
        .ok_or_else(|| Anomaly::fault("rtdb/journal-exhausted", "The journal has no next position"))
}

fn invalid_entry() -> Anomaly {
    Anomaly::fault(
        "rtdb/multi-journal-envelope",
        "Invalid Multi journal record envelope",
    )
}

/// The journal head is behind what this process already replayed: the stream
/// was recovered after losing two of its three voters, and the records after
/// `head` through `replayed` are lost.
pub(crate) fn journal_shortened(head: Lsn, replayed: Lsn) -> Anomaly {
    Anomaly::fault(
        "rtdb/journal-shortened",
        format!(
            "The journal ends at {} but this process replayed through {}; journal records {} through {} were lost in a recovery",
            head.0,
            replayed.0,
            head.0.saturating_add(1),
            replayed.0
        ),
    )
}

pub(crate) fn journal_error(error: JournalError) -> Anomaly {
    if let JournalError::Recovered { head } = error {
        return Anomaly::fault(
            "rtdb/journal-recovered",
            format!(
                "The journal was recovered after losing two of its three voters and ends at {}; commits acknowledged after it may be lost. An administrator must acknowledge the recovery before writes resume",
                head.0
            ),
        );
    }
    let (category, code) = match error {
        JournalError::Fenced { .. } => (AnomalyCategory::Unavailable, "rtdb/writer-fenced"),
        JournalError::Conflict { .. } => (AnomalyCategory::Conflict, "rtdb/journal-conflict"),
        JournalError::Trimmed { .. } => (AnomalyCategory::NotFound, "rtdb/cursor-expired"),
        JournalError::TooLarge { .. } => {
            (AnomalyCategory::Incorrect, "rtdb/journal-record-too-large")
        }
        JournalError::Full { .. } => (AnomalyCategory::Busy, "rtdb/journal-full"),
        JournalError::NotFound | JournalError::Deleted => {
            (AnomalyCategory::NotFound, "rtdb/unknown-database")
        }
        JournalError::Unavailable => (AnomalyCategory::Unavailable, "rtdb/journal-unavailable"),
        JournalError::Recovered { .. } => unreachable!("handled above"),
    };
    Anomaly::new(category, code, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovered_and_shortened_journals_name_the_loss() {
        let recovered = journal_error(JournalError::Recovered { head: Lsn(41) });
        assert_eq!(recovered.code, "rtdb/journal-recovered");
        assert!(recovered.message.unwrap().contains("ends at 41"));
        let shortened = journal_shortened(Lsn(41), Lsn(50));
        assert_eq!(shortened.code, "rtdb/journal-shortened");
        assert!(shortened.message.unwrap().contains("42 through 50"));
    }

    #[test]
    fn timed_original_proof_requires_exact_sealed_bytes_and_an_exclusive_quorum_sample() {
        let original = TimedOriginal {
            stream: StreamId(1), epoch: Epoch(2), at: Lsn(3), payload_sha256: [4; 32],
            cutoff: TimedJournalCutoff::new(Timestamp(20), [5; 32]).unwrap(),
        };
        let proof = TimedOriginalProof { original, quorum_sync_at: Timestamp(19), evidence_sha256: [6; 32] };
        assert!(proof.verify_exact(original).is_ok());
        assert!(proof.verify_exact(TimedOriginal { payload_sha256: [7; 32], ..original }).is_err());
        assert!(proof.verify_exact(TimedOriginal { epoch: Epoch(8), ..original }).is_err());
        assert!(TimedOriginalProof { quorum_sync_at: Timestamp(20), ..proof }.verify_exact(original).is_err());
    }
}
