use std::{
    sync::atomic::{AtomicBool, AtomicI64, Ordering},
    time::Duration,
};

use bytes::Bytes;
use journal_contract::{BoxFuture, FileJournal, Identity, Limits, MemoryJournal, Page, StreamId};
use rtdb_crypt::LocalKms;
use rtdb_logkeeper::simulation::{Fault, FaultJournal};
use rtdb_store::{MemoryStore, read_file};
use rtdb_tx::transact::{
    Cell, Declaration, Guard, Outcome, REQUIREMENT_FAILED, Resolution, Row, Version, Visibility, Window,
};
use rtdb_types::{
    EntityId, StructFields, TableRef, Value,
    rtdb_v1::{At, LabelScope, Layer, MAIN_BRANCH},
};

use super::*;
use tokio::sync::{Mutex, Notify};

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

#[tokio::test]
async fn multi_driver_tokens_reroute_and_verify_the_row_footprint() {
    use rtdb_crypt::tokens::PageToken;
    use rtdb_types::rtdb_v1::{ClaimEvidence, ClaimFootprint, ClaimRegion, ClaimScope};

    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(Identity(1))));
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    let receipt = writer.transact(&put(1, 1, "original")).await.unwrap();
    let evidence = ClaimEvidence {
        scope: ClaimScope { branch: MAIN_BRANCH.into(), basis: receipt.basis, layer: Layer::Merged },
        footprint: ClaimFootprint::from_regions([ClaimRegion::Row {
            table: table(), iid: EntityId::Integer(1).iid(),
        }]),
    };
    let page = PageToken { basis: receipt.basis, clock: receipt.system_time, offset: 1, request: [5; 32] };
    let issuer = writer.token_authority();
    let claim = issuer.seal_claim(&evidence).await.unwrap();
    let after = issuer.seal_page(&page).await.unwrap();

    let follower = cluster.node(2, Arc::new(journal.handle(Identity(2))));
    let rerouted = follower.token_authority();
    assert_eq!(rerouted.seal_claim(&evidence).await.unwrap(), claim);
    assert_eq!(rerouted.open_page(&after).await.unwrap(), page);

    writer.transact(&put(2, 2, "unrelated")).await.unwrap();
    let mut guarded = put(3, 3, "accepted");
    guarded.guards.push(Guard::Unchanged(claim));
    assert_eq!(writer.transact(&guarded).await.unwrap().outcome, Outcome::Then);
    writer.transact(&put(4, 1, "changed")).await.unwrap();
    guarded.key = vec![5; 16];
    assert_eq!(writer.transact(&guarded).await.unwrap().outcome, Outcome::Otherwise { failed: vec![0] });
}

fn table() -> TableRef {
    TableRef::new("public", "docs")
}

fn request(key: u8, op: Op) -> Transact {
    Transact {
        custody_request_digest: None,
        key: vec![key; 16],
        require: vec![],
        window: Window::default(),
        guards: vec![],
        ops: vec![op],
        otherwise: vec![],
        requires: vec![],
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
                custody_group: None,
                protected_columns: Vec::new(),
                custody_payload: None,
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
async fn multi_driver_requires_refuses_a_displaced_owner_and_recovers_the_receipt() {
    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(Identity(1))));
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    let writes = WritePipeline::start(Arc::new(Mutex::new(writer)));
    let original = writes.transact(put(1, 1, "original owner")).await.unwrap();
    let successor = writes.transact(put(2, 1, "new owner")).await.unwrap();

    let version = |id, expect| Guard::Version {
        branch: MAIN_BRANCH.into(), table: table(), id: EntityId::Integer(id), valid_at: None, expect,
    };
    let mut stale = put(3, 2, "then");
    stale.requires = vec![version(1, Version::Written(original.basis))];
    stale.guards = vec![version(2, Version::Present)];
    stale.otherwise = put(3, 3, "otherwise").ops;
    let refused = writes.transact(stale.clone()).await.unwrap();
    assert_eq!(refused.outcome, Outcome::Refused {
        error: Anomaly::new(AnomalyCategory::Conflict, REQUIREMENT_FAILED, "Requirement 0 failed"),
    });

    cluster.at(30);
    let mut follower = cluster.node(2, Arc::new(journal.handle(Identity(2))));
    let replayed = Receipt { replayed: true, ..refused };
    assert_eq!(follower.tx_status(&stale.key).await.unwrap(), replayed);
    assert_eq!(writes.transact(stale.clone()).await.unwrap(), replayed);
    assert_eq!(text_at(&mut follower, MAIN_BRANCH, 1).await, Some(Value::Utf8("new owner".into())));
    assert_eq!(text_at(&mut follower, MAIN_BRANCH, 2).await, None);
    assert_eq!(text_at(&mut follower, MAIN_BRANCH, 3).await, None);

    stale.key = vec![4; 16];
    stale.requires = vec![version(1, Version::Written(successor.basis))];
    let receipt = writes.transact(stale).await.unwrap();
    assert_eq!(receipt.outcome, Outcome::Otherwise { failed: vec![0] });
    assert_eq!(text_at(&mut follower, MAIN_BRANCH, 2).await, None);
    assert_eq!(text_at(&mut follower, MAIN_BRANCH, 3).await, Some(Value::Utf8("otherwise".into())));
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
    let gate = Arc::new(GateJournal::new(faults.clone()));
    let mut writer = cluster.node(1, gate.clone());
    let mut successor = cluster.node(2, Arc::new(journal.handle(Identity(2))));
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();

    let mut timed = put(1, 1, "pending");
    timed.window.before = Some(Timestamp(90));
    gate.block_seal.store(true, Ordering::SeqCst);
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
    let claims = first.tokens.verify_claims(request.claims()).await;
    let at = next(first.state().head).unwrap();
    let Resolution::Append(prepared) = first
        .state()
        .transactions
        .resolve(&request, at.0, (first.clock)(), &[], &claims)
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

struct GateJournal {
    inner: Arc<dyn Journal>,
    arm: AtomicBool,
    block_seal: AtomicBool,
    block_trim: AtomicBool,
    entered: Notify,
    release: Notify,
}

impl GateJournal {
    fn new(inner: Arc<dyn Journal>) -> Self {
        Self {
            inner,
            arm: AtomicBool::new(false),
            block_seal: AtomicBool::new(false),
            block_trim: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
        }
    }
}

impl Journal for GateJournal {
    fn create(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<()>> {
        self.inner.create(stream)
    }

    fn seal(&self, stream: StreamId, epoch: Epoch) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        if self.block_seal.load(Ordering::SeqCst) {
            Box::pin(async { Err(JournalError::Unavailable) })
        } else {
            self.inner.seal(stream, epoch)
        }
    }

    fn append(
        &self,
        stream: StreamId,
        epoch: Epoch,
        at: Lsn,
        payload: Bytes,
    ) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        Box::pin(async move {
            if self.arm.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.inner.append(stream, epoch, at, payload).await
        })
    }

    fn read(
        &self,
        stream: StreamId,
        from: Lsn,
        max: usize,
        wait: Duration,
    ) -> BoxFuture<'_, journal_contract::Result<Page>> {
        self.inner.read(stream, from, max, wait)
    }

    fn head(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        self.inner.head(stream)
    }

    fn trim(
        &self,
        stream: StreamId,
        epoch: Epoch,
        through: Lsn,
    ) -> BoxFuture<'_, journal_contract::Result<()>> {
        if self.block_trim.load(Ordering::SeqCst) {
            Box::pin(async { Err(JournalError::Unavailable) })
        } else {
            self.inner.trim(stream, epoch, through)
        }
    }

    fn delete(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<()>> {
        self.inner.delete(stream)
    }
}

#[tokio::test]
async fn multi_driver_resolves_a_dependent_write_before_its_predecessor_is_durable() {
    exercise_pipeline(PipelineOutcome::Commit).await;
}

#[tokio::test]
async fn multi_driver_withdraws_the_staged_suffix_on_append_failure() {
    exercise_pipeline(PipelineOutcome::Unavailable).await;
}

#[tokio::test]
async fn multi_driver_withdraws_the_staged_suffix_when_fenced() {
    exercise_pipeline(PipelineOutcome::Fenced).await;
}

#[tokio::test]
async fn multi_driver_recovers_a_committed_append_before_resuming_after_a_lost_reply() {
    exercise_pipeline(PipelineOutcome::LostReply).await;
}

#[derive(Clone, Copy)]
enum PipelineOutcome {
    Commit,
    Unavailable,
    LostReply,
    Fenced,
}

async fn exercise_pipeline(outcome: PipelineOutcome) {
    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let faults = Arc::new(FaultJournal::new(Arc::new(journal.handle(Identity(1)))));
    let gate = Arc::new(GateJournal::new(faults.clone()));
    let mut writer = cluster.node(1, gate.clone());
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();

    let preparing = Arc::new(Notify::new());
    writer.clock = Arc::new({
        let preparing = Arc::clone(&preparing);
        move || {
            preparing.notify_one();
            Timestamp(10)
        }
    });
    let database = Arc::new(Mutex::new(writer));
    let pipeline = WritePipeline::start(database.clone());
    gate.arm.store(true, Ordering::SeqCst);
    let first = pipeline.transact(put(1, 1, "first"));
    tokio::pin!(first);
    tokio::select! {
        result = &mut first => panic!("write returned before durability: {result:?}"),
        reached = tokio::time::timeout(Duration::from_secs(2), gate.entered.notified()) => reached.unwrap(),
    }
    tokio::time::timeout(Duration::from_secs(2), preparing.notified())
        .await
        .unwrap();

    let mut dependent = put(2, 2, "second");
    dependent.guards.push(Guard::Version {
        branch: MAIN_BRANCH.into(),
        table: table(),
        id: EntityId::Integer(1),
        valid_at: None,
        expect: Version::Present,
    });
    let second = pipeline.transact(dependent);
    tokio::pin!(second);
    tokio::select! {
        result = &mut second => panic!("dependent write returned before durability: {result:?}"),
        observed = tokio::time::timeout(Duration::from_secs(2), preparing.notified()) => observed.unwrap(),
    }
    let observer = journal.handle(Identity(2));
    let stream = StreamId(DATABASE.as_u128());
    assert_eq!(observer.head(stream).await.unwrap(), Lsn(1));
    assert_eq!(
        database
            .lock()
            .await
            .tx_status(&[2; 16])
            .await
            .unwrap_err()
            .code,
        "rtdb/transaction-pending"
    );
    let expected_error = match outcome {
        PipelineOutcome::Commit => None,
        PipelineOutcome::Unavailable => {
            faults.inject(Fault::Unavailable);
            Some(("rtdb/journal-unavailable", Lsn(1)))
        }
        PipelineOutcome::LostReply => {
            faults.inject(Fault::DropAppendReply);
            Some(("rtdb/journal-unavailable", Lsn(2)))
        }
        PipelineOutcome::Fenced => {
            cluster.at(116);
            let mut successor = cluster.node(2, Arc::new(journal.handle(Identity(2))));
            assert_eq!(
                successor.tick(&live(2)).await.unwrap(),
                Role::Writer(Epoch(2))
            );
            Some(("rtdb/writer-fenced", Lsn(2)))
        }
    };
    gate.release.notify_one();
    let (first, second) = tokio::join!(first, second);
    if let Some((error, head)) = expected_error {
        let first = first.unwrap_err();
        assert_eq!(first.code, error);
        assert_eq!(first.category, AnomalyCategory::Interrupted);
        assert_eq!(second.unwrap_err().code, "rtdb/speculative-write-withdrawn");
        assert_eq!(observer.head(stream).await.unwrap(), head);
        if matches!(outcome, PipelineOutcome::Fenced) {
            assert_eq!(database.lock().await.role(), Role::Follower);
        } else {
            let next = pipeline.transact(put(3, 3, "resumed")).await.unwrap();
            assert_eq!(next.basis.position(), head.0 + 1);
            let mut database = database.lock().await;
            assert_eq!(database.role(), Role::Writer(Epoch(1)));
            if matches!(outcome, PipelineOutcome::LostReply) {
                assert_eq!(
                    database.tx_status(&[1; 16]).await.unwrap().basis.position(),
                    2
                );
            }
            assert_eq!(
                database.tx_status(&[2; 16]).await.unwrap_err().code,
                "rtdb/unknown-transaction"
            );
        }
    } else {
        assert_eq!(first.unwrap().basis.position(), 2);
        let second = second.unwrap();
        assert_eq!(second.basis.position(), 3);
        assert_eq!(second.outcome, Outcome::Then);
        assert_eq!(observer.head(stream).await.unwrap(), Lsn(3));
    }
}

#[tokio::test]
async fn multi_driver_admits_a_dependent_write_behind_a_timed_terminal_slot() {
    exercise_timed_suffix(false).await;
}

#[tokio::test]
async fn multi_driver_redecides_the_unsent_suffix_after_timed_refusal() {
    exercise_timed_suffix(true).await;
}

async fn exercise_timed_suffix(refuse: bool) {
    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let gate = Arc::new(GateJournal::new(Arc::new(journal.handle(Identity(1)))));
    let mut writer = cluster.node(1, gate.clone());
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    let preparing = Arc::new(Notify::new());
    writer.clock = Arc::new({
        let preparing = preparing.clone();
        let time = cluster.time.clone();
        move || {
            preparing.notify_one();
            Timestamp(time.load(Ordering::SeqCst))
        }
    });
    let database = Arc::new(Mutex::new(writer));
    let pipeline = WritePipeline::start(database.clone());
    gate.arm.store(true, Ordering::SeqCst);
    let mut timed = put(1, 1, "tentative");
    timed.window.before = Some(Timestamp(25));
    let first = pipeline.transact(timed);
    tokio::pin!(first);
    tokio::select! {
        result = &mut first => panic!("timed write returned before durability: {result:?}"),
        reached = tokio::time::timeout(Duration::from_secs(2), gate.entered.notified()) => reached.unwrap(),
    }
    preparing.notified().await;
    let mut later = put(2, 2, "later");
    later.guards.push(Guard::Version {
        branch: MAIN_BRANCH.into(),
        table: table(),
        id: EntityId::Integer(1),
        valid_at: None,
        expect: Version::Absent,
    });
    let dependent = pipeline.transact(later);
    tokio::pin!(dependent);
    tokio::select! {
        result = &mut dependent => panic!("the dependent write passed the terminal slot: {result:?}"),
        ready = tokio::time::timeout(Duration::from_secs(2), preparing.notified()) => ready.unwrap(),
    }
    assert_eq!(
        database
            .lock()
            .await
            .tx_status(&[2; 16])
            .await
            .unwrap_err()
            .code,
        "rtdb/transaction-pending"
    );
    assert_eq!(
        journal
            .handle(Identity(2))
            .head(StreamId(DATABASE.as_u128()))
            .await
            .unwrap(),
        Lsn(1)
    );
    if refuse {
        cluster.at(30);
    }
    gate.release.notify_one();
    let (first, dependent) = tokio::join!(first, dependent);
    let first = first.unwrap();
    let dependent = dependent.unwrap();
    assert_eq!((first.basis.position(), dependent.basis.position()), (3, 4));
    if refuse {
        assert!(matches!(first.outcome, Outcome::Refused { .. }));
        assert_eq!(dependent.outcome, Outcome::Then);
    } else {
        assert_eq!(first.outcome, Outcome::Then);
        assert_eq!(dependent.outcome, Outcome::Otherwise { failed: vec![0] });
    }
}

struct RecordingBatch {
    journal: Arc<dyn Journal>,
    groups: Mutex<Vec<Vec<Lsn>>>,
}

impl JournalBatchAppender for RecordingBatch {
    fn append_batch(
        &self,
        stream: StreamId,
        epoch: Epoch,
        records: Vec<(Lsn, Bytes)>,
    ) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        Box::pin(async move {
            self.groups
                .lock()
                .await
                .push(records.iter().map(|(at, _)| *at).collect());
            let mut head = Lsn(0);
            for (at, payload) in records {
                head = self.journal.append(stream, epoch, at, payload).await?;
            }
            Ok(head)
        })
    }
}

#[tokio::test]
async fn multi_driver_dispatches_the_decided_batch_in_one_journal_request() {
    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let journal: Arc<dyn Journal> = Arc::new(journal.handle(Identity(1)));
    let batch = Arc::new(RecordingBatch {
        journal: journal.clone(),
        groups: Mutex::new(Vec::new()),
    });
    let mut database = cluster.node(1, journal).with_batch_appender(batch.clone());
    database.create().await.unwrap();
    database.tick(&live(1)).await.unwrap();
    let pipeline = WritePipeline::start(Arc::new(Mutex::new(database)));
    let (first, second) = tokio::join!(
        pipeline.transact(put(1, 1, "first")),
        pipeline.transact(put(2, 2, "second")),
    );
    assert_eq!(
        (
            first.unwrap().basis.position(),
            second.unwrap().basis.position()
        ),
        (2, 3)
    );
    assert_eq!(
        *batch.groups.lock().await,
        vec![vec![Lsn(1)], vec![Lsn(2), Lsn(3)]]
    );
}

#[tokio::test]
async fn multi_driver_routes_the_same_idempotency_key_in_two_receipt_namespaces() {
    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let mut database = cluster.node(1, Arc::new(journal.handle(Identity(1))));
    database.create().await.unwrap();
    database.tick(&live(1)).await.unwrap();
    let declaration = request(
        1,
        Op::Declare {
            table: table(),
            change: Declaration::Visibility(Visibility {
                column: "label".into(),
            }),
        },
    );
    assert_eq!(
        database.transact(&declaration).await.unwrap().outcome,
        Outcome::Then
    );
    let labelled = |id, value| {
        let mut request = put(2, id, value);
        let Op::Put { rows, .. } = &mut request.ops[0] else {
            unreachable!()
        };
        rows[0].valid = Some(rtdb_types::Interval::ALL.into());
        rows[0]
            .cells
            .insert("label".into(), Cell::Value(Value::Binary(vec![1])));
        request
    };
    let scope = LabelScope {
        read: BTreeSet::from([vec![1]]),
        write: BTreeSet::from([vec![1]]),
    };
    let database = Arc::new(Mutex::new(database));
    let pipeline = WritePipeline::start(database.clone());
    let (left, right) = tokio::join!(
        pipeline.transact_scoped(labelled(1, "left"), scope.clone(), [7; 32]),
        pipeline.transact_scoped(labelled(2, "right"), scope.clone(), [8; 32]),
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.outcome, Outcome::Then);
    assert_eq!(right.outcome, Outcome::Then);
    assert_ne!(left.basis, right.basis);
    let mut database = database.lock().await;
    for (namespace, receipt) in [([7; 32], left), ([8; 32], right)] {
        let authority = ScopedTransact {
            receipt_namespace: namespace,
            labels: &scope,
        };
        let status = database
            .tx_status_scoped(&[2; 16], authority)
            .await
            .unwrap();
        assert_eq!(status.basis, receipt.basis);
        assert!(status.replayed);
    }
    assert_eq!(
        database.tx_status(&[2; 16]).await.unwrap_err().code,
        "rtdb/unknown-transaction"
    );
}

struct FixedRotationTime(Lsn, Timestamp);

impl RotationTiming for FixedRotationTime {
    fn committed_at(&self, database: Uuid, rotation: Lsn) -> BoxFuture<'_, Result<Timestamp>> {
        Box::pin(async move {
            assert_eq!(database, DATABASE);
            assert_eq!(rotation, self.0);
            Ok(self.1)
        })
    }
}

#[tokio::test]
async fn multi_driver_recovers_rotation_after_key_retirement_overtakes_trim() {
    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let gate = Arc::new(GateJournal::new(Arc::new(journal.handle(Identity(1)))));
    let timing = Arc::new(FixedRotationTime(Lsn(5), Timestamp(10)));
    let mut writer = cluster
        .node(1, gate.clone())
        .with_rotation_timing(timing.clone());
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    let receipt = writer.transact(&put(1, 1, "retained")).await.unwrap();
    let old = writer.write_file(b"old ciphertext").await.unwrap();
    writer.checkpoint(vec![old]).await.unwrap();
    gate.block_trim.store(true, Ordering::SeqCst);
    assert!(
        writer
            .rotate_clean_checkpoint(&[Bytes::from_static(b"clean")])
            .await
            .is_err()
    );
    assert!(
        read_file(cluster.store.as_ref(), cluster.kms.as_ref(), old)
            .await
            .is_ok()
    );

    cluster.at(115);
    assert_eq!(
        read_file(cluster.store.as_ref(), cluster.kms.as_ref(), old)
            .await
            .err()
            .unwrap()
            .code,
        "rtdb/erased-under-view"
    );
    cluster.at(116);
    let mut successor = cluster
        .node(2, Arc::new(journal.handle(Identity(2))))
        .with_rotation_timing(timing);
    assert_eq!(
        successor.tick(&live(2)).await.unwrap(),
        Role::Writer(Epoch(2))
    );
    assert_eq!(successor.retained_floor(), Some(Lsn(4)));
    assert_eq!(successor.generation(), Some(2));
    assert_eq!(
        successor.tx_status(&receipt.key).await.unwrap().basis,
        receipt.basis
    );
    assert_eq!(
        text_at(&mut successor, MAIN_BRANCH, 1).await,
        Some(Value::Utf8("retained".into()))
    );
    assert_eq!(
        successor.check_generation(1).await.unwrap_err().code,
        "rtdb/erased-under-view"
    );
    assert_eq!(
        successor
            .rotate_clean_checkpoint(&[])
            .await
            .unwrap()
            .unreadable_by,
        Timestamp(115)
    );
}

#[tokio::test]
async fn multi_driver_releases_a_fork_pin_after_independent_destination_publication() {
    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let mut source = cluster.node(1, Arc::new(journal.handle(Identity(1))));
    source.create().await.unwrap();
    source.tick(&live(1)).await.unwrap();
    let source_file = source.write_file(b"fork data").await.unwrap();
    let image = source.checkpoint(vec![source_file]).await.unwrap();
    let pin = source.pin_checkpoint(image, None).await.unwrap();
    let failed = source
        .transfer_fork(pin.id, async |_| {
            Err::<(), _>(Anomaly::fault(
                "rtdb/test-publish",
                "Destination publication failed",
            ))
        })
        .await;
    assert!(failed.is_err());
    assert_eq!(
        source.open_export(pin.id).await.unwrap().manifest().files,
        [source_file]
    );

    let destination_id = Uuid::from_u128(2);
    let mut child = MultiDatabase::new(
        MultiOptions {
            database: destination_id,
            node: ip(2),
            lease_micros: LEASE,
            drift_micros: DRIFT,
        },
        Arc::new(journal.handle(Identity(2))),
        cluster.store.clone(),
        cluster.kms.clone(),
        Arc::new(|| Timestamp(10)),
    )
    .unwrap();
    child.create().await.unwrap();
    child.tick(&live(2)).await.unwrap();
    let destination = source
        .transfer_fork(pin.id, async |export| {
            let data = export.file(export.manifest().files[0]).await?;
            let file = child.write_file(&data.bytes).await?;
            child.checkpoint(vec![file]).await
        })
        .await
        .unwrap();
    assert_eq!(
        source.open_export(pin.id).await.err().unwrap().code,
        "rtdb/unknown-pin"
    );
    let destination = read_manifest(cluster.store.as_ref(), cluster.kms.as_ref(), destination)
        .await
        .unwrap();
    let file = destination.contents().files[0];
    assert_eq!(file.database, destination_id);
    let restored = read_file(cluster.store.as_ref(), cluster.kms.as_ref(), file)
        .await
        .unwrap();
    assert_eq!(restored.bytes.as_slice(), b"fork data");
}
