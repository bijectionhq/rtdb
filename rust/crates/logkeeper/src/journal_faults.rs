use std::{collections::VecDeque, sync::Arc, time::Duration};

use bytes::Bytes;
use journal_contract::{BoxFuture, Epoch, Journal, JournalError, Lsn, Page, Result, StreamId};
use parking_lot::Mutex;

/// A one-shot fault in the next journal request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Fault {
    /// Wait before issuing the request to the journal.
    Delay(Duration),
    /// Replace the next append's success with `Unavailable` after the journal commits it.
    DropAppendReply,
    /// Return `Unavailable` without issuing the request.
    Unavailable,
}

/// Scripts one-shot faults on journal requests.
pub struct FaultJournal<J: ?Sized> {
    journal: Arc<J>,
    faults: Mutex<VecDeque<Fault>>,
}

impl<J: Journal + ?Sized> FaultJournal<J> {
    pub fn new(journal: Arc<J>) -> Self {
        Self {
            journal,
            faults: Mutex::new(VecDeque::new()),
        }
    }

    /// A dropped reply applies to the next append attempt and leaves intervening non-append requests alone.
    pub fn inject(&self, fault: Fault) {
        self.faults.lock().push_back(fault);
    }

    async fn before(&self, append: bool) -> Result<Option<Fault>> {
        let fault = {
            let mut faults = self.faults.lock();
            match faults.front() {
                Some(Fault::DropAppendReply) if !append => None,
                _ => faults.pop_front(),
            }
        };
        match fault {
            Some(Fault::Delay(duration)) => tokio::time::sleep(duration).await,
            Some(Fault::Unavailable) => return Err(JournalError::Unavailable),
            _ => (),
        }
        Ok(fault)
    }
}

impl<J: Journal + ?Sized> Journal for FaultJournal<J> {
    fn create(&self, stream: StreamId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.before(false).await?;
            self.journal.create(stream).await
        })
    }

    fn seal(&self, stream: StreamId, epoch: Epoch) -> BoxFuture<'_, Result<Lsn>> {
        Box::pin(async move {
            self.before(false).await?;
            self.journal.seal(stream, epoch).await
        })
    }

    fn append(
        &self,
        stream: StreamId,
        epoch: Epoch,
        at: Lsn,
        payload: Bytes,
    ) -> BoxFuture<'_, Result<Lsn>> {
        Box::pin(async move {
            let fault = self.before(true).await?;
            let lsn = self.journal.append(stream, epoch, at, payload).await?;
            if fault == Some(Fault::DropAppendReply) {
                Err(JournalError::Unavailable)
            } else {
                Ok(lsn)
            }
        })
    }

    fn read(
        &self,
        stream: StreamId,
        from: Lsn,
        max: usize,
        wait: Duration,
    ) -> BoxFuture<'_, Result<Page>> {
        Box::pin(async move {
            self.before(false).await?;
            self.journal.read(stream, from, max, wait).await
        })
    }

    fn head(&self, stream: StreamId) -> BoxFuture<'_, Result<Lsn>> {
        Box::pin(async move {
            self.before(false).await?;
            self.journal.head(stream).await
        })
    }

    fn trim(&self, stream: StreamId, epoch: Epoch, through: Lsn) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.before(false).await?;
            self.journal.trim(stream, epoch, through).await
        })
    }

    fn delete(&self, stream: StreamId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.before(false).await?;
            self.journal.delete(stream).await
        })
    }

    fn acknowledge_recovery(&self, stream: StreamId) -> BoxFuture<'_, Result<Lsn>> {
        Box::pin(async move {
            self.before(false).await?;
            self.journal.acknowledge_recovery(stream).await
        })
    }
}
