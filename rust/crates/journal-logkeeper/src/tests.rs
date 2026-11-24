use std::{
    collections::BTreeMap,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

use journal_contract::{Identity, conformance};
use stream_logkeeper::stream::{
    keeper::{Keeper, Limits},
    wire::{KeeperId, Reply, Request, Transport, TransportError},
};

use super::*;

const KEEPERS: [KeeperId; 3] = [KeeperId(1), KeeperId(2), KeeperId(3)];
const STREAM: StreamId = StreamId(100);
const EPOCH: Epoch = Epoch(1);

struct LocalTransport {
    keepers: Arc<BTreeMap<KeeperId, Keeper>>,
    identity: Identity,
    drop_publication_replies: Option<Arc<AtomicBool>>,
}

impl Transport for LocalTransport {
    fn identity(&self) -> &Identity {
        &self.identity
    }

    fn call(
        &self,
        keeper: KeeperId,
        request: Request,
    ) -> BoxFuture<'_, std::result::Result<Reply, TransportError>> {
        Box::pin(async move {
            let drop_reply = self
                .drop_publication_replies
                .as_ref()
                .is_some_and(|enabled| enabled.load(Ordering::Relaxed))
                && matches!(&request, Request::Append { records, .. } if records.is_empty());
            let reply = self.keepers[&keeper].call(&self.identity, request).await?;
            if drop_reply && matches!(reply, Reply::Appended { .. }) {
                return Err(TransportError::TimedOut);
            }
            Ok(reply)
        })
    }
}

fn keepers(path: &Path, limits: Limits) -> Arc<BTreeMap<KeeperId, Keeper>> {
    Arc::new(
        KEEPERS
            .into_iter()
            .map(|id| {
                let keeper = Keeper::open(&path.join(id.0.to_string()), id, limits).unwrap();
                (id, keeper)
            })
            .collect(),
    )
}

fn client(
    keepers: &Arc<BTreeMap<KeeperId, Keeper>>,
    identity: Identity,
    drop_publication_replies: Option<Arc<AtomicBool>>,
) -> StreamJournal {
    let transport = Arc::new(LocalTransport {
        keepers: Arc::clone(keepers),
        identity,
        drop_publication_replies,
    });
    StreamJournal(Writer::new(transport, KEEPERS).unwrap())
}

#[tokio::test]
async fn stream_journal_conforms_to_the_contract() {
    let dir = tempfile::tempdir().unwrap();
    let keepers = keepers(
        dir.path(),
        Limits {
            max_record_bytes: conformance::LIMITS.max_record_bytes.try_into().unwrap(),
            stream_quota_bytes: conformance::LIMITS.stream_quota_bytes,
            ..Limits::default()
        },
    );
    conformance::run(|identity| Arc::new(client(&keepers, identity, None))).await;
}

fn records() -> Vec<(Lsn, Bytes)> {
    vec![
        (Lsn(1), Bytes::from_static(b"first")),
        (Lsn(2), Bytes::from_static(b"second")),
    ]
}

async fn assert_committed_batch(journal: &dyn Journal) {
    let page = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = journal
                .read(STREAM, Lsn(1), 2, Duration::ZERO)
                .await
                .unwrap();
            if page.head >= Lsn(2) {
                break page;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("no keeper returned the committed batch");
    assert_eq!(page.head, Lsn(2));
    assert_eq!(page.records, records());
}

#[tokio::test]
async fn stream_journal_appends_a_batch_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let keepers = keepers(dir.path(), Limits::default());
    let writer = client(
        &keepers,
        Identity::from_static("urn:logkeeper:client:batch"),
        None,
    );
    writer.create(STREAM).await.unwrap();
    assert_eq!(writer.seal(STREAM, EPOCH).await, Ok(Lsn(0)));
    assert_eq!(
        writer.append_batch(STREAM, EPOCH, records()).await,
        Ok(Lsn(2))
    );
    assert_committed_batch(&writer).await;
}

#[tokio::test]
async fn stream_journal_recovers_a_batch_after_lost_publication_replies() {
    let dir = tempfile::tempdir().unwrap();
    let keepers = keepers(dir.path(), Limits::default());
    let drop_publication_replies = Arc::new(AtomicBool::new(true));
    let writer = client(
        &keepers,
        Identity::from_static("urn:logkeeper:client:writer"),
        Some(Arc::clone(&drop_publication_replies)),
    );
    let successor = client(
        &keepers,
        Identity::from_static("urn:logkeeper:client:successor"),
        None,
    );
    writer.create(STREAM).await.unwrap();
    assert_eq!(writer.seal(STREAM, EPOCH).await, Ok(Lsn(0)));
    assert_eq!(
        writer.append_batch(STREAM, EPOCH, records()).await,
        Err(JournalError::Unavailable),
    );
    drop_publication_replies.store(false, Ordering::Relaxed);
    assert_eq!(successor.head(STREAM).await, Ok(Lsn(2)));
    assert_eq!(writer.seal(STREAM, EPOCH).await, Ok(Lsn(2)));
    assert_committed_batch(&successor).await;
    let next = Bytes::from_static(b"third");
    assert_eq!(
        writer
            .append_batch(STREAM, EPOCH, vec![(Lsn(3), next.clone())])
            .await,
        Ok(Lsn(3))
    );
    assert_eq!(successor.seal(STREAM, Epoch(2)).await, Ok(Lsn(3)));
    let mut expected = records();
    expected.push((Lsn(3), next));
    assert_eq!(
        successor
            .read(STREAM, Lsn(1), 3, Duration::ZERO)
            .await
            .unwrap()
            .records,
        expected
    );
}
