#![cfg(feature = "simulation")]

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use journal_contract::{
    Epoch, FileJournal, Identity, Journal, JournalError, Lsn, MemoryJournal, StreamId, conformance,
};
use rtdb_logkeeper::simulation::{Fault, FaultJournal};

const STREAM: StreamId = StreamId(1);
const EPOCH: Epoch = Epoch(3);
const PAYLOAD: Bytes = Bytes::from_static(b"record");

async fn opened() -> FaultJournal<dyn Journal> {
    let inner = MemoryJournal::default().handle(Identity(1));
    let journal = FaultJournal::new(Arc::new(inner) as Arc<dyn Journal>);
    journal.create(STREAM).await.unwrap();
    assert_eq!(journal.seal(STREAM, EPOCH).await, Ok(Lsn(0)));
    journal
}

#[tokio::test]
async fn fault_journal_distinguishes_unissued_and_unacknowledged_appends() {
    let journal = opened().await;

    journal.inject(Fault::Unavailable);
    assert_eq!(
        journal.append(STREAM, EPOCH, Lsn(1), PAYLOAD).await,
        Err(JournalError::Unavailable)
    );
    assert_eq!(journal.head(STREAM).await, Ok(Lsn(0)));

    journal.inject(Fault::DropAppendReply);
    assert_eq!(journal.head(STREAM).await, Ok(Lsn(0)));
    assert_eq!(
        journal.append(STREAM, EPOCH, Lsn(1), PAYLOAD).await,
        Err(JournalError::Unavailable)
    );
    assert_eq!(journal.head(STREAM).await, Ok(Lsn(1)));
    assert_eq!(
        journal.append(STREAM, EPOCH, Lsn(1), PAYLOAD).await,
        Ok(Lsn(1))
    );
    let page = journal
        .read(STREAM, Lsn(1), 2, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(page.records, [(Lsn(1), PAYLOAD)]);
}

#[tokio::test]
async fn fault_journal_leaves_a_rejected_append_error_visible() {
    let journal = opened().await;
    journal.inject(Fault::DropAppendReply);
    assert_eq!(
        journal.append(STREAM, EPOCH, Lsn(2), PAYLOAD).await,
        Err(JournalError::Conflict {
            committed_end: Lsn(0)
        })
    );
    assert_eq!(
        journal.append(STREAM, EPOCH, Lsn(1), PAYLOAD).await,
        Ok(Lsn(1))
    );
}

#[tokio::test]
async fn fault_journal_delays_dispatch_and_releases_the_fault_after_one_call() {
    let journal = opened().await;
    journal.inject(Fault::Delay(Duration::from_secs(1)));
    let mut append = journal.append(STREAM, EPOCH, Lsn(1), PAYLOAD);
    tokio::select! {
        biased;
        result = &mut append => panic!("delayed request completed: {result:?}"),
        _ = tokio::task::yield_now() => (),
    }
    assert_eq!(journal.head(STREAM).await, Ok(Lsn(0)));
    drop(append);

    assert_eq!(
        journal.append(STREAM, EPOCH, Lsn(1), PAYLOAD).await,
        Ok(Lsn(1))
    );
}

#[tokio::test]
async fn fault_journal_conforms_over_the_memory_reference() {
    let journal = MemoryJournal::new(conformance::LIMITS);
    conformance::run(|identity| Arc::new(FaultJournal::new(Arc::new(journal.handle(identity)))))
        .await;
}

#[tokio::test]
async fn fault_journal_conforms_over_the_file_reference() {
    let directory = tempfile::tempdir().unwrap();
    let journal = FileJournal::open(directory.path().join("journal"), conformance::LIMITS).unwrap();
    conformance::run(|identity| Arc::new(FaultJournal::new(Arc::new(journal.handle(identity)))))
        .await;
}
