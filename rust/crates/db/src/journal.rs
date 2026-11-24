use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use journal_contract::{BoxFuture, Epoch, Journal, JournalError, Lsn, Page, StreamId};
use rtdb_crypt::{Context, Generation, Kms};
use rtdb_format::journal::Record;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use uuid::Uuid;

const ENVELOPE_VERSION: u8 = 1;
const PAGE_SIZE: usize = 128;

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

    /// A committed entry attests the exact payload, writer epoch and certificate
    /// under the original quorum, before the exclusive journal cutoff.
    fn append_timed_original(
        &self,
        _stream: StreamId,
        _epoch: Epoch,
        _at: Lsn,
        _payload: Bytes,
        _cutoff: rtdb_types::TimedJournalCutoff,
    ) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        Box::pin(async { Err(JournalError::Unavailable) })
    }
}

#[derive(Clone)]
pub(crate) struct Stream {
    pub(crate) database: Uuid,
    journal: Arc<dyn Journal>,
    pub(crate) batch: Option<Arc<dyn JournalBatchAppender>>,
    pub(crate) kms: Arc<dyn Kms>,
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
        }
    }

    pub(crate) fn id(&self) -> StreamId {
        StreamId(u128::from_be_bytes(*self.database.as_bytes()))
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
        Ok(Entry {
            epoch,
            generation: plaintext.generation.number,
            record: Record::decode(record)?,
        })
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

pub(crate) fn journal_error(error: JournalError) -> Anomaly {
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
    };
    Anomaly::new(category, code, error.to_string())
}
