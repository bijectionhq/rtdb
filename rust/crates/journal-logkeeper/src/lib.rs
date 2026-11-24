use std::{future::poll_fn, io, sync::Arc, task::Poll, time::Duration};

use bytes::Bytes;
use journal_contract::{BoxFuture, Epoch, Journal, JournalError, Lsn, Page, Result, StreamId};
use stream_contract::Journal as _;
use stream_logkeeper::stream::{transport::TcpTransport, writer::Writer};

pub use stream_logkeeper::stream::transport::Credentials;

#[derive(Clone)]
pub struct LogkeeperJournal(Writer);

impl LogkeeperJournal {
    pub async fn connect(
        url: &str,
        credentials: &Credentials,
        deadline: Duration,
    ) -> io::Result<Self> {
        let (transport, ensemble) = TcpTransport::discover(url, credentials, deadline).await?;
        let writer = Writer::new(Arc::new(transport), ensemble).map_err(io::Error::other)?;
        Ok(Self(writer))
    }

    /// Submit a contiguous group to the writer's shared stream queue.
    pub async fn append_batch(
        &self, stream: StreamId, epoch: Epoch, records: Vec<(Lsn, Bytes)>,
    ) -> Result<Lsn> {
        let (head, _) = records.last().ok_or(JournalError::Unavailable)?;
        let head = *head;
        if records.windows(2).any(|pair| pair[0].0.0.checked_add(1) != Some(pair[1].0.0)) {
            return Err(JournalError::Unavailable);
        }
        let mut appends = records.into_iter().map(|(at, payload)| {
            (at, Some(self.append(stream, epoch, at, payload)))
        }).collect::<Vec<_>>();
        // Writer::append joins the stream queue on its first poll. Stage in
        // journal order so the writer can collect contiguous pending records.
        poll_fn(|context| {
            let mut waiting = false;
            for (at, append) in &mut appends {
                let Some(future) = append.as_mut() else { continue; };
                match future.as_mut().poll(context) {
                    Poll::Ready(Ok(committed)) if committed == *at => *append = None,
                    Poll::Ready(Ok(_)) => return Poll::Ready(Err(JournalError::Unavailable)),
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => waiting = true,
                }
            }
            if waiting { Poll::Pending } else { Poll::Ready(Ok(head)) }
        }).await
    }
}

impl Journal for LogkeeperJournal {
    fn create(&self, stream: StreamId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.0
                .create(stream_contract::StreamId(stream.0))
                .await
                .map_err(error)
        })
    }

    fn seal(&self, stream: StreamId, epoch: Epoch) -> BoxFuture<'_, Result<Lsn>> {
        Box::pin(async move {
            self.0
                .seal(
                    stream_contract::StreamId(stream.0),
                    stream_contract::Epoch(epoch.0),
                )
                .await
                .map(|lsn| Lsn(lsn.0))
                .map_err(error)
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
            self.0
                .append(
                    stream_contract::StreamId(stream.0),
                    stream_contract::Epoch(epoch.0),
                    stream_contract::Lsn(at.0),
                    payload,
                )
                .await
                .map(|lsn| Lsn(lsn.0))
                .map_err(error)
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
            let page = self
                .0
                .read(
                    stream_contract::StreamId(stream.0),
                    stream_contract::Lsn(from.0),
                    max,
                    wait,
                )
                .await
                .map_err(error)?;
            Ok(Page {
                floor: Lsn(page.floor.0),
                records: page
                    .records
                    .into_iter()
                    .map(|(lsn, payload)| (Lsn(lsn.0), payload))
                    .collect(),
                end: Lsn(page.head.0),
            })
        })
    }

    fn head(&self, stream: StreamId) -> BoxFuture<'_, Result<Lsn>> {
        Box::pin(async move {
            self.0
                .head(stream_contract::StreamId(stream.0))
                .await
                .map(|lsn| Lsn(lsn.0))
                .map_err(error)
        })
    }

    fn trim(&self, stream: StreamId, epoch: Epoch, through: Lsn) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.0
                .trim(
                    stream_contract::StreamId(stream.0),
                    stream_contract::Epoch(epoch.0),
                    stream_contract::Lsn(through.0),
                )
                .await
                .map_err(error)
        })
    }

    fn delete(&self, stream: StreamId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.0
                .delete(stream_contract::StreamId(stream.0))
                .await
                .map_err(error)
        })
    }
}

fn error(error: stream_contract::JournalError) -> JournalError {
    use stream_contract::JournalError as Remote;
    match error {
        Remote::Fenced { current } => JournalError::Fenced {
            current: Epoch(current.0),
        },
        Remote::Conflict { head } => JournalError::Conflict {
            committed_end: Lsn(head.0),
        },
        Remote::Trimmed { floor } => JournalError::Trimmed {
            floor: Lsn(floor.0),
        },
        Remote::TooLarge { max } => JournalError::TooLarge { max },
        Remote::Full { floor } => JournalError::Full {
            floor: Lsn(floor.0),
        },
        Remote::NotFound => JournalError::NotFound,
        Remote::Deleted => JournalError::Deleted,
        Remote::Unavailable => JournalError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use stream_contract::Identity;
    use stream_logkeeper::stream::{
        keeper::{Keeper, Limits},
        wire::{KeeperId, Reply, Request, Transport, TransportError},
    };

    use super::*;

    struct LocalTransport {
        keepers: Arc<BTreeMap<KeeperId, Keeper>>,
        identity: Identity,
    }

    impl Transport for LocalTransport {
        fn identity(&self) -> &Identity {
            &self.identity
        }

        fn call(
            &self,
            keeper: KeeperId,
            request: Request,
        ) -> stream_contract::BoxFuture<'_, std::result::Result<Reply, TransportError>> {
            Box::pin(async move { self.keepers[&keeper].call(&self.identity, request).await })
        }
    }

    #[tokio::test]
    async fn logkeeper_journal_maps_positions_pages_and_fences() {
        let dir = tempfile::tempdir().unwrap();
        let ids = [KeeperId(1), KeeperId(2), KeeperId(3)];
        let keepers = Arc::new(
            ids.into_iter()
                .map(|id| {
                    let keeper =
                        Keeper::open(&dir.path().join(id.0.to_string()), id, Limits::default())
                            .unwrap();
                    (id, keeper)
                })
                .collect::<BTreeMap<_, _>>(),
        );
        let client = |identity| {
            let transport = Arc::new(LocalTransport {
                keepers: keepers.clone(),
                identity: Identity::from_static(identity),
            });
            LogkeeperJournal(Writer::new(transport, ids).unwrap())
        };
        let first = client("urn:logkeeper:client:first");
        let second = client("urn:logkeeper:client:second");
        let stream = StreamId(100);
        let data = Bytes::from_static(b"record");
        first.create(stream).await.unwrap();
        assert_eq!(first.head(stream).await.unwrap(), Lsn(0));
        assert_eq!(first.seal(stream, Epoch(1)).await.unwrap(), Lsn(0));
        assert_eq!(
            first
                .append(stream, Epoch(1), Lsn(1), data.clone())
                .await
                .unwrap(),
            Lsn(1)
        );
        assert_eq!(second.seal(stream, Epoch(2)).await.unwrap(), Lsn(1));
        assert_eq!(
            first.append(stream, Epoch(1), Lsn(2), data.clone()).await,
            Err(JournalError::Fenced { current: Epoch(2) })
        );
        let page = second
            .read(stream, Lsn(1), 1, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(page.records, [(Lsn(1), data)]);
        assert_eq!(page.end, Lsn(1));
        let next = Bytes::from_static(b"next");
        let last = Bytes::from_static(b"last");
        assert_eq!(second.append_batch(stream, Epoch(2), vec![
            (Lsn(2), next.clone()), (Lsn(3), last.clone()),
        ]).await.unwrap(), Lsn(3));
        assert_eq!(second.read(stream, Lsn(2), 2, Duration::ZERO).await.unwrap().records,
            [(Lsn(2), next), (Lsn(3), last)]);
    }
}
