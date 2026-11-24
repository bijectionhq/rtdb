use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use journal_contract::{Epoch, Journal, JournalError, Lsn, StreamId};
use rtdb_crypt::{Context, Generation, Kms};
use rtdb_format::journal::Record;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use uuid::Uuid;

const ENVELOPE_VERSION: u8 = 1;
const PAGE_SIZE: usize = 128;

#[derive(Clone)]
pub(crate) struct Stream {
    pub(crate) database: Uuid,
    journal: Arc<dyn Journal>,
    pub(crate) kms: Arc<dyn Kms>,
}

pub(crate) struct CommittedPage {
    pub(crate) floor: Lsn,
    pub(crate) records: Vec<(Lsn, Bytes)>,
    pub(crate) head: Lsn,
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
            kms,
        }
    }

    fn id(&self) -> StreamId {
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

    pub(crate) async fn read(&self, from: Lsn) -> journal_contract::Result<CommittedPage> {
        let page = self
            .journal
            .read(self.id(), from, PAGE_SIZE, Duration::ZERO)
            .await?;
        Ok(CommittedPage {
            floor: page.floor,
            records: page.records,
            head: page.end,
        })
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
