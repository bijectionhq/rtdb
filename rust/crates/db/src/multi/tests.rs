use std::{
    sync::atomic::{AtomicI64, Ordering},
    time::Duration,
};

use journal_contract::{FileJournal, Identity, Limits, MemoryJournal, StreamId};
use rtdb_crypt::LocalKms;
use rtdb_logkeeper::simulation::{Fault, FaultJournal};
use rtdb_store::{MemoryStore, read_file};
use rtdb_tx::transact::{Cell, Outcome, Row, Window};
use rtdb_types::{
    EntityId, StructFields, TableRef, Value,
    rtdb_v1::{At, Layer, MAIN_BRANCH},
};

use super::*;

const DATABASE: Uuid = Uuid::from_u128(0x123456789abcdef0123456789abcdef0);
const LEASE: i64 = 100;
const DRIFT: i64 = 5;

struct Cluster {
    time: Arc<AtomicI64>,
    kms: Arc<LocalKms>,
    store: Arc<MemoryStore>,
}

impl Cluster {
    fn new() -> Self {
        let time = Arc::new(AtomicI64::new(10));
        let kms = Arc::new(LocalKms::new({
            let time = Arc::clone(&time);
            move || time.load(Ordering::SeqCst)
        }));
        Self {
            time,
            kms,
            store: Arc::new(MemoryStore::default()),
        }
    }

    fn at(&self, now: i64) {
        assert!(self.time.fetch_max(now, Ordering::SeqCst) <= now);
    }

    fn node(&self, n: u8, journal: Arc<dyn Journal>) -> MultiDatabase {
        let time = Arc::clone(&self.time);
        MultiDatabase::new(
            MultiOptions {
                database: DATABASE,
                node: ip(n),
                lease_micros: LEASE,
                drift_micros: DRIFT,
            },
            journal,
            self.store.clone(),
            self.kms.clone(),
            Arc::new(move || Timestamp(time.load(Ordering::SeqCst))),
            Arc::new(ClaimSealer::new(*DATABASE.as_bytes(), [42; 32])),
        )
        .unwrap()
    }
}

fn ip(n: u8) -> IpAddr {
    IpAddr::from([192, 0, 2, n])
}

fn live(n: u8) -> BTreeSet<IpAddr> {
    BTreeSet::from([ip(n)])
}

fn table() -> TableRef {
    TableRef::new("public", "docs")
}

fn request(key: u8, op: Op) -> Transact {
    Transact {
        key: vec![key; 16],
        require: vec![],
        window: Window::default(),
        guards: vec![],
        ops: vec![op],
        otherwise: vec![],
    }
}

fn put(key: u8, id: i64, text: &str) -> Transact {
    request(
        key,
        Op::Put {
            branch: MAIN_BRANCH.into(),
            table: table(),
            rows: vec![Row {
                id: EntityId::Integer(id),
                valid: None,
                cells: StructFields::from([("text".into(), Cell::Value(Value::Utf8(text.into())))]),
            }],
            if_absent: false,
        },
    )
}

async fn text_at(node: &mut MultiDatabase, branch: &str, id: i64) -> Option<Value> {
    let time = (node.clock)();
    let index = node.fresh_index().await.unwrap();
    let view = index.view(branch, At::Latest, Layer::Merged, time).unwrap();
    view.row(&table(), &EntityId::Integer(id), time)
        .and_then(|row| row.image_at(time))
        .and_then(|image| image.cells.get("text").cloned())
}

#[tokio::test]
async fn multi_driver_replays_genesis_settlement_and_a_file_checkpoint() {
    let files = tempfile::tempdir().unwrap();
    let path = files.path().join("journal");
    let journal = FileJournal::open(&path, Limits::default()).unwrap();
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(Identity(1))));
    writer.create().await.unwrap();
    assert_eq!(writer.tick(&live(1)).await.unwrap(), Role::Writer(Epoch(1)));
    let mut timed = put(1, 1, "published");
    timed.window.before = Some(Timestamp(30));
    let receipt = writer.transact(&timed).await.unwrap();
    assert_eq!(receipt.outcome, Outcome::Then);
    assert_eq!(receipt.basis.position(), 3);
    writer.checkpoint(vec![]).await.unwrap();
    assert_eq!(writer.retained_floor(), Some(Lsn(4)));
    drop(writer);
    drop(journal);

    let journal = FileJournal::open(path, Limits::default()).unwrap();
    let mut successor = cluster.node(2, Arc::new(journal.handle(Identity(2))));
    assert_eq!(successor.tick(&live(2)).await.unwrap(), Role::Follower);
    assert_eq!(
        successor.tx_status(&timed.key).await.unwrap().basis,
        receipt.basis
    );
    assert_eq!(
        text_at(&mut successor, MAIN_BRANCH, 1).await,
        Some(Value::Utf8("published".into()))
    );
    cluster.at(116);
    assert_eq!(
        successor.tick(&live(2)).await.unwrap(),
        Role::Writer(Epoch(2))
    );
    assert!(successor.transact(&timed).await.unwrap().replayed);
}

#[tokio::test]
async fn multi_driver_replacement_refuses_a_timed_tx_after_its_reply_is_lost() {
    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let faults = Arc::new(FaultJournal::new(Arc::new(journal.handle(Identity(1)))));
    let mut writer = cluster.node(1, faults.clone());
    let mut successor = cluster.node(2, Arc::new(journal.handle(Identity(2))));
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();

    let mut timed = put(1, 1, "pending");
    timed.window.before = Some(Timestamp(90));
    faults.inject(Fault::DropAppendReply);
    assert_eq!(
        writer.transact(&timed).await.unwrap_err().code,
        "rtdb/journal-unavailable"
    );
    assert_eq!(writer.role(), Role::Follower);
    assert_eq!(
        successor.tx_status(&timed.key).await.unwrap_err().code,
        "rtdb/transaction-pending"
    );
    assert_eq!(text_at(&mut successor, MAIN_BRANCH, 1).await, None);
    cluster.at(115);
    assert_eq!(successor.tick(&live(2)).await.unwrap(), Role::Follower);
    cluster.at(116);
    assert_eq!(
        successor.tick(&live(2)).await.unwrap(),
        Role::Writer(Epoch(2))
    );
    let receipt = successor.tx_status(&timed.key).await.unwrap();
    assert!(matches!(receipt.outcome, Outcome::Refused { .. }));
    assert_eq!(text_at(&mut successor, MAIN_BRANCH, 1).await, None);
}

#[tokio::test]
async fn multi_driver_dueling_dns_views_keep_one_epoch_until_the_winner_lapses() {
    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let first = Arc::new(FaultJournal::new(Arc::new(journal.handle(Identity(1)))));
    let mut left = cluster.node(1, first.clone());
    let mut right = cluster.node(2, Arc::new(journal.handle(Identity(2))));
    left.create().await.unwrap();
    left.refresh().await.unwrap();
    right.refresh().await.unwrap();
    first.inject(Fault::Unavailable);
    assert_eq!(
        left.promote().await.unwrap_err().code,
        "rtdb/journal-unavailable"
    );
    right.promote().await.unwrap();
    left.promote().await.unwrap();
    assert_eq!(left.role(), Role::Follower);
    assert_eq!(right.role(), Role::Writer(Epoch(1)));
    for now in [30, 60, 100, 140, 180] {
        cluster.at(now);
        assert_eq!(left.tick(&live(1)).await.unwrap(), Role::Follower);
        assert_eq!(right.tick(&live(2)).await.unwrap(), Role::Writer(Epoch(1)));
    }
    cluster.at(276);
    assert_eq!(right.role(), Role::Follower);
    assert_eq!(left.tick(&live(1)).await.unwrap(), Role::Writer(Epoch(2)));
    assert_eq!(right.tick(&live(2)).await.unwrap(), Role::Follower);
}

#[tokio::test]
async fn multi_driver_abandoned_seal_allows_one_replacement_after_its_grace_period() {
    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let abandoned = journal.handle(Identity(1));
    let mut successor = cluster.node(2, Arc::new(journal.handle(Identity(2))));
    successor.create().await.unwrap();
    let stream = StreamId(DATABASE.as_u128());
    assert_eq!(abandoned.seal(stream, Epoch(1)).await.unwrap(), Lsn(0));
    for now in [10, 30, 60, 110, 115] {
        cluster.at(now);
        assert_eq!(successor.tick(&live(2)).await.unwrap(), Role::Follower);
        assert_eq!(abandoned.head(stream).await.unwrap(), Lsn(0));
    }
    cluster.at(116);
    assert_eq!(
        successor.tick(&live(2)).await.unwrap(),
        Role::Writer(Epoch(2))
    );
    let page = abandoned
        .read(stream, Lsn(1), 1, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(page.end, Lsn(1));
    assert!(!page.records[0].1.starts_with(b"RTDBR"));
}

#[tokio::test]
async fn multi_driver_storage_reclaims_only_unreachable_epochs_and_keeps_old_branch_bases() {
    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let mut first = cluster.node(1, Arc::new(journal.handle(Identity(1))));
    let mut second = cluster.node(2, Arc::new(journal.handle(Identity(2))));
    first.create().await.unwrap();
    first.tick(&live(1)).await.unwrap();
    let original = first.transact(&put(1, 1, "old")).await.unwrap();
    let pinned_file = first.write_file(b"export").await.unwrap();
    let orphan = first.write_file(b"orphan").await.unwrap();
    let saved = first.checkpoint(vec![pinned_file]).await.unwrap();
    let pin = first
        .pin_checkpoint(saved, Some(Timestamp(150)))
        .await
        .unwrap();
    first.transact(&put(2, 1, "new")).await.unwrap();
    first.checkpoint(vec![]).await.unwrap();

    cluster.at(116);
    assert_eq!(second.tick(&live(2)).await.unwrap(), Role::Writer(Epoch(2)));
    assert_eq!(first.collect().await.unwrap_err().code, "rtdb/no-writer");
    second.collect().await.unwrap();
    assert!(
        read_file(cluster.store.as_ref(), cluster.kms.as_ref(), orphan)
            .await
            .is_err()
    );
    assert_eq!(
        second.open_pin(pin.id).await.unwrap().contents().files,
        [pinned_file]
    );
    assert!(second.checkpoint(vec![orphan]).await.is_err());
    second
        .transact(&request(
            3,
            Op::Fork {
                child: "branch".into(),
                from: (MAIN_BRANCH.into(), original.basis),
                writable: false,
                expires_at: None,
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        text_at(&mut second, "branch", 1).await,
        Some(Value::Utf8("old".into()))
    );

    cluster.at(152);
    assert_eq!(
        second.open_pin(pin.id).await.err().unwrap().code,
        "rtdb/unknown-pin"
    );
    second.collect().await.unwrap();
    assert!(
        read_file(cluster.store.as_ref(), cluster.kms.as_ref(), pinned_file)
            .await
            .is_ok()
    );
    second.release_pin(pin.id).await.unwrap();
    second.checkpoint(vec![]).await.unwrap();
    second.collect().await.unwrap();
    assert!(
        read_file(cluster.store.as_ref(), cluster.kms.as_ref(), pinned_file)
            .await
            .is_err()
    );
    assert_eq!(
        text_at(&mut second, "branch", 1).await,
        Some(Value::Utf8("old".into()))
    );
}

#[tokio::test]
async fn multi_driver_fences_an_append_delayed_until_after_the_next_seal() {
    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let faults = Arc::new(FaultJournal::new(Arc::new(journal.handle(Identity(1)))));
    let mut first = cluster.node(1, faults.clone());
    let mut successor = cluster.node(2, Arc::new(journal.handle(Identity(2))));
    first.create().await.unwrap();
    first.tick(&live(1)).await.unwrap();

    let request = put(1, 1, "deposed");
    let at = next(first.state().end).unwrap();
    let Resolution::Append(prepared) = first
        .state()
        .transactions
        .resolve(&request, at.0, (first.clock)(), &[], &first.claims)
        .unwrap()
    else {
        panic!("expected a new write");
    };
    let record = Record::Tx {
        payload: prepared.record().encode().unwrap().to_vec(),
        objects: vec![],
    };
    let authority = first.authority().unwrap();
    faults.inject(Fault::Delay(Duration::from_millis(20)));
    {
        let append = first.append(authority, record);
        tokio::pin!(append);
        tokio::select! {
            biased;
            result = &mut append => panic!("append completed before the fence: {result:?}"),
            _ = tokio::task::yield_now() => (),
        }
        cluster.at(116);
        assert_eq!(
            successor.tick(&live(2)).await.unwrap(),
            Role::Writer(Epoch(2))
        );
        assert_eq!(append.await.unwrap_err().code, "rtdb/writer-fenced");
    }
    assert_eq!(first.role(), Role::Follower);
    assert!(first.writer.is_none());
    assert_eq!(text_at(&mut successor, MAIN_BRANCH, 1).await, None);
    assert_eq!(
        successor.transact(&request).await.unwrap().outcome,
        Outcome::Then
    );
}

#[tokio::test]
async fn multi_driver_never_retargets_a_recovery_cut_overtaken_by_trim() {
    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(Identity(1))));
    let mut reader = cluster.node(2, Arc::new(journal.handle(Identity(2))));
    writer.create().await.unwrap();
    assert_eq!(writer.tick(&live(1)).await.unwrap(), Role::Writer(Epoch(1)));
    writer.transact(&put(1, 1, "first")).await.unwrap();
    let requested = reader.stream.head().await.unwrap();
    cluster.at(11);
    writer.transact(&put(2, 1, "second")).await.unwrap();
    writer.checkpoint(vec![]).await.unwrap();

    assert_eq!(
        reader.catch_up(requested).await.unwrap_err().code,
        "rtdb/cursor-expired"
    );
    assert_eq!(
        text_at(&mut reader, MAIN_BRANCH, 1).await,
        Some(Value::Utf8("second".into()))
    );
}

#[tokio::test]
async fn multi_driver_late_upload_cannot_reenter_reachability() {
    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let mut first = cluster.node(1, Arc::new(journal.handle(Identity(1))));
    let mut second = cluster.node(2, Arc::new(journal.handle(Identity(2))));
    first.create().await.unwrap();
    first.tick(&live(1)).await.unwrap();
    let delayed = first
        .writer
        .as_mut()
        .unwrap()
        .objects
        .file(cluster.kms.as_ref(), 1, b"unpublished bytes")
        .await
        .unwrap();

    cluster.at(116);
    assert_eq!(second.tick(&live(2)).await.unwrap(), Role::Writer(Epoch(2)));
    assert_eq!(second.collect().await.unwrap(), 0);
    delayed.put(cluster.store.as_ref()).await.unwrap();
    assert_eq!(
        second
            .checkpoint(vec![delayed.id()])
            .await
            .unwrap_err()
            .code,
        "rtdb.store/unreachable-publication"
    );
    assert_eq!(second.collect().await.unwrap(), 1);
    assert!(
        read_file(cluster.store.as_ref(), cluster.kms.as_ref(), delayed.id())
            .await
            .is_err()
    );
}
