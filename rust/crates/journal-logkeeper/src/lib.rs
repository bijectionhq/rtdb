use std::{future::poll_fn, io, sync::Arc, task::Poll, time::Duration};

use bytes::Bytes;
use journal_contract::{BoxFuture, Epoch, Journal, JournalError, Lsn, Page, Result, StreamId};
use stream_logkeeper::stream::{transport::TcpTransport, writer::Writer};

pub use stream_logkeeper::stream::transport::Credentials;
pub use stream_logkeeper::stream::{
    timed::{TimedOriginal, TimedOriginalProof, TimedOriginalVerifier},
    wire::KeeperId,
};

#[derive(Clone)]
pub struct StreamJournal(Writer);

impl StreamJournal {
    pub async fn connect(
        url: &str,
        credentials: &Credentials,
        deadline: Duration,
    ) -> io::Result<Self> {
        let (transport, ensemble) = TcpTransport::discover(url, credentials, deadline).await?;
        let writer = Writer::new(Arc::new(transport), ensemble).map_err(io::Error::other)?;
        Ok(Self(writer))
    }

    pub async fn connect_with_timed_original(
        url: &str,
        credentials: &Credentials,
        deadline: Duration,
        verifier: Arc<dyn TimedOriginalVerifier>,
        maximum_payload_bytes: Option<usize>,
    ) -> io::Result<Self> {
        let Self(writer) = Self::connect(url, credentials, deadline).await?;
        Ok(Self(writer.with_timed_original(verifier, maximum_payload_bytes)))
    }

    pub fn maximum_timed_original_bytes(&self) -> Option<usize> {
        self.0.maximum_timed_original_bytes()
    }

    pub async fn append_timed_original(
        &self,
        original: TimedOriginal,
        payload: Bytes,
    ) -> Result<TimedOriginalProof> {
        self.0.append_timed_original(original, payload).await
    }

    pub async fn prove_timed_original(&self, original: TimedOriginal) -> Result<TimedOriginalProof> {
        self.0.prove_timed_original(original).await
    }

    /// Submit a contiguous group to the writer's shared stream queue.
    pub async fn append_batch(
        &self,
        stream: StreamId,
        epoch: Epoch,
        records: Vec<(Lsn, Bytes)>,
    ) -> Result<Lsn> {
        let (head, _) = records.last().ok_or(JournalError::Unavailable)?;
        let head = *head;
        if records
            .windows(2)
            .any(|pair| pair[0].0.0.checked_add(1) != Some(pair[1].0.0))
        {
            return Err(JournalError::Unavailable);
        }
        let mut appends = records
            .into_iter()
            .map(|(at, payload)| (at, Some(self.append(stream, epoch, at, payload))))
            .collect::<Vec<_>>();
        // Writer::append joins the stream queue on its first poll. Stage in
        // journal order so the writer can collect contiguous pending records.
        poll_fn(|context| {
            let mut waiting = false;
            for (at, append) in &mut appends {
                let Some(future) = append.as_mut() else {
                    continue;
                };
                match future.as_mut().poll(context) {
                    Poll::Ready(Ok(committed)) if committed == *at => *append = None,
                    Poll::Ready(Ok(_)) => return Poll::Ready(Err(JournalError::Unavailable)),
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => waiting = true,
                }
            }
            if waiting {
                Poll::Pending
            } else {
                Poll::Ready(Ok(head))
            }
        })
        .await
    }
}

impl Journal for StreamJournal {
    fn create(&self, stream: StreamId) -> BoxFuture<'_, Result<()>> {
        self.0.create(stream)
    }

    fn seal(&self, stream: StreamId, epoch: Epoch) -> BoxFuture<'_, Result<Lsn>> {
        self.0.seal(stream, epoch)
    }

    fn append(
        &self,
        stream: StreamId,
        epoch: Epoch,
        at: Lsn,
        payload: Bytes,
    ) -> BoxFuture<'_, Result<Lsn>> {
        self.0.append(stream, epoch, at, payload)
    }

    fn read(
        &self,
        stream: StreamId,
        from: Lsn,
        max: usize,
        wait: Duration,
    ) -> BoxFuture<'_, Result<Page>> {
        self.0.read(stream, from, max, wait)
    }

    fn head(&self, stream: StreamId) -> BoxFuture<'_, Result<Lsn>> {
        self.0.head(stream)
    }

    fn trim(&self, stream: StreamId, epoch: Epoch, through: Lsn) -> BoxFuture<'_, Result<()>> {
        self.0.trim(stream, epoch, through)
    }

    fn delete(&self, stream: StreamId) -> BoxFuture<'_, Result<()>> {
        self.0.delete(stream)
    }
}

#[cfg(test)]
mod tests;
