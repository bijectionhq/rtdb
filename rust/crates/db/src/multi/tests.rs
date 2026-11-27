use std::{
    sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
    time::Duration,
};

use bytes::Bytes;
use journal_contract::{
    BoxFuture, Identity, Page, StreamId,
    reference::{FileJournal, Limits, MemoryJournal},
};
use rtdb_crypt::LocalKms;
use rtdb_logkeeper::simulation::{Fault, FaultJournal};
use rtdb_store::{MemoryStore, ObjectStore, read_file};
use rtdb_tx::transact::{
    Cell, Declaration, Guard, Op, Outcome, REQUIREMENT_FAILED, Resolution, Row, Version, Visibility, Window,
};
use rtdb_types::{
    EntityId, StructFields, TableRef, Value,
    rtdb_v1::{At, Basis, LabelScope, Layer, MAIN_BRANCH, ManagedCheckpointReservation, ManagedCheckpointTarget},
};

use super::*;
use tokio::sync::{Mutex, Notify};

const DATABASE: Uuid = Uuid::from_u128(0x123456789abcdef0123456789abcdef0);
const LEASE: i64 = 100;
const DRIFT: i64 = 5;
const NODE_1_IDENTITY: Identity = Identity::from_static("urn:rtdb:test:1");
const NODE_2_IDENTITY: Identity = Identity::from_static("urn:rtdb:test:2");
const NODE_3_IDENTITY: Identity = Identity::from_static("urn:rtdb:test:3");

/// The journal behind a test. `RTDB_MULTI_JOURNAL=consensus` runs the test on
/// real keeper-hosted consensus streams: three voters and a catalog keeper on
/// loopback with enrolled media and mutual TLS. The default is the in-memory
/// reference journal.
enum Journals {
    Memory(MemoryJournal),
    Consensus {
        cluster: stream_logkeeper::stream::consensus::testing::LocalCluster,
        config: stream_logkeeper::stream::consensus::JournalConfig,
    },
}

/// Keepers accept only `urn:logkeeper:client:` principals. Multi treats a
/// journal identity as opaque, so each test identity maps to one principal.
fn keeper_principal(identity: &Identity) -> String {
    format!("urn:logkeeper:client:{}", identity.as_str().replace(':', "-"))
}

/// A journal handle for one node identity, from either source.
#[derive(Clone)]
struct TestJournal(Arc<dyn Journal>);

async fn journals() -> Journals {
    match std::env::var("RTDB_MULTI_JOURNAL").as_deref() {
        Ok("consensus") => {
            let principals = [NODE_1_IDENTITY, NODE_2_IDENTITY, NODE_3_IDENTITY].map(|identity| keeper_principal(&identity));
            let principals = principals.iter().map(String::as_str).collect::<Vec<_>>();
            let cluster = stream_logkeeper::stream::consensus::testing::LocalCluster::start(&principals).await.unwrap();
            let config = cluster.journal_config(8 << 30, 4 << 20);
            Journals::Consensus { cluster, config }
        }
        Ok("memory") | Err(_) => Journals::Memory(MemoryJournal::default()),
        Ok(other) => panic!("unknown RTDB_MULTI_JOURNAL {other}"),
    }
}

impl Journals {
    fn handle(&self, identity: Identity) -> TestJournal {
        TestJournal(match self {
            Self::Memory(journal) => Arc::new(journal.handle(identity)),
            Self::Consensus { cluster, config } => Arc::new(cluster.journal(&keeper_principal(&identity), config).unwrap()),
        })
    }

    /// Stops and restarts every keeper, keeping their media.
    async fn restart_keepers(&mut self) {
        if let Self::Consensus { cluster, .. } = self {
            cluster.shutdown().await.unwrap();
            for keeper in cluster.voters().into_iter().chain([cluster.catalog()]) {
                cluster.restart(keeper).await.unwrap();
            }
        }
    }
}

impl Journal for TestJournal {
    fn create(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<()>> {
        self.0.create(stream)
    }

    fn seal(&self, stream: StreamId, epoch: Epoch) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        self.0.seal(stream, epoch)
    }

    fn append(&self, stream: StreamId, epoch: Epoch, at: Lsn, payload: Bytes) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        self.0.append(stream, epoch, at, payload)
    }

    fn read(&self, stream: StreamId, from: Lsn, max: usize, wait: Duration) -> BoxFuture<'_, journal_contract::Result<Page>> {
        self.0.read(stream, from, max, wait)
    }

    fn head(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        self.0.head(stream)
    }

    fn trim(&self, stream: StreamId, epoch: Epoch, through: Lsn) -> BoxFuture<'_, journal_contract::Result<()>> {
        self.0.trim(stream, epoch, through)
    }

    fn delete(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<()>> {
        self.0.delete(stream)
    }

    fn acknowledge_recovery(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        self.0.acknowledge_recovery(stream)
    }
}

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
        self.node_for(DATABASE, n, journal)
    }

    fn node_for(&self, database: Uuid, n: u8, journal: Arc<dyn Journal>) -> MultiDatabase {
        let time = Arc::clone(&self.time);
        MultiDatabase::new(
            MultiOptions {
                database,
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
        .with_binary_checkpoints(true)
        .with_manifest_budget(rtdb_types::MemoryBudget::new(16 << 20))
    }
}

fn ip(n: u8) -> IpAddr {
    IpAddr::from([192, 0, 2, n])
}

fn live(n: u8) -> BTreeSet<IpAddr> {
    BTreeSet::from([ip(n)])
}

#[test]
fn an_ephemeral_key_vault_cannot_enable_multi_row_custody() {
    let journal = MemoryJournal::default();
    let database = Cluster::new().node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    assert!(!database.write_features().contains(&rtdb_tx::transact::ROW_CUSTODY));
    assert_eq!(database.with_row_custody().err().unwrap().code, "rtdb/row-custody");
}

#[tokio::test]
async fn ranked_catalog_replays_at_the_built_cut_after_later_writes() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    let first = writer.transact(&put(1, 1, "first")).await.unwrap();
    let first = rtdb_index::branches::BranchCut { basis: first.basis, system_time: first.system_time };
    let build = writer.ranked_build().unwrap().unwrap();
    let memory = rtdb_types::MemoryBudget::new(16 << 20);
    writer.publish_ranked_catalog(build, vec![1], vec![Bytes::from_static(b"trie")], &memory).await.unwrap();

    let mut follower = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    follower.fresh_index().await.unwrap();
    let publication = follower.ranked_publication(first).unwrap();
    let files = publication.open(&memory).await.unwrap();
    assert_eq!(files.cut, first);
    assert_eq!(files.inventory, [1]);
    assert_eq!(files.files[0].0.as_ref(), b"trie");
    assert!(writer.ranked_build().unwrap().is_none());

    let second = writer.transact(&put(2, 2, "second")).await.unwrap();
    let second = rtdb_index::branches::BranchCut { basis: second.basis, system_time: second.system_time };
    let build = writer.ranked_build().unwrap().unwrap();
    let third = writer.transact(&put(3, 3, "third")).await.unwrap();
    let third = rtdb_index::branches::BranchCut { basis: third.basis, system_time: third.system_time };
    writer.publish_ranked_catalog(build, vec![2], vec![Bytes::from_static(b"second trie")], &memory).await.unwrap();
    follower.fresh_index().await.unwrap();
    assert_eq!(follower.ranked_publication(second).unwrap().open(&memory).await.unwrap().cut, second);
    assert!(follower.ranked_publication(third).is_none());
    assert!(follower.ranked_publication(first).is_some());
    assert!(follower.ranked_publication(rtdb_index::branches::BranchCut {
        system_time: Timestamp(first.system_time.0 + 1), ..first
    }).is_none());
}

#[tokio::test]
async fn assigned_follower_observes_settled_writer_progress_without_admitting_writes() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    let mut follower = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    writer.create().await.unwrap();
    assert!(matches!(writer.tick(&live(1)).await.unwrap(), Role::Writer(_)));

    let owner = Arc::new(());
    assert_eq!(writer.assign_follower(&owner).await.unwrap_err().code, "rtdb/follower-unavailable");
    let first = writer.transact(&put(1, 1, "first")).await.unwrap();
    follower.assign_follower(&owner).await.unwrap();
    let replacement = Arc::new(());
    assert_eq!(follower.assign_follower(&replacement).await.unwrap_err().code, "rtdb/follower-unavailable");
    assert_eq!(follower.follower_state(&replacement).err().unwrap().code, "rtdb/follower-unavailable");

    cluster.at(20);
    let second = writer.transact(&put(2, 2, "second")).await.unwrap();
    cluster.at(30);
    writer.renew().await.unwrap();
    let (local, _) = follower.follower_view(&owner).unwrap();
    assert_eq!(local.head().basis, first.basis);
    assert_eq!(local.cut(At::AtLeast(second.basis)).unwrap_err().code, "rtdb/basis-ahead");
    let (head, position) = follower.follower_writer_head(&owner).await.unwrap();
    assert_eq!(head, second.basis);
    assert_eq!(position + 1, writer.stream.head().await.unwrap().0);
    assert_eq!(follower.tick(&live(2)).await.unwrap(), Role::Follower);
    assert_eq!(follower.transact(&put(3, 3, "denied")).await.unwrap_err().code, "rtdb/no-writer");

    cluster.at(200);
    assert_eq!(follower.tick(&live(2)).await.unwrap(), Role::Follower);
    assert_eq!(follower.follower_writer_head(&owner).await.unwrap_err().code, "rtdb/follower-progress-unavailable");
    let (local, generation) = follower.follower_view(&owner).unwrap();
    assert_eq!(local.head().basis, second.basis);
    let retained = local.cut(At::Exactly(first.basis)).unwrap();
    assert_eq!(retained.basis, first.basis);
    assert_eq!(local.applied_position(retained).unwrap(), first.basis.position());
    follower.check_generation(generation).await.unwrap();
    drop(owner);
    assert!(matches!(follower.tick(&live(2)).await.unwrap(), Role::Writer(_)));
}

#[tokio::test]
async fn multi_driver_tokens_reroute_and_verify_the_row_footprint() {
    use rtdb_crypt::tokens::PageToken;
    use rtdb_types::rtdb_v1::{ClaimEvidence, ClaimFootprint, ClaimRegion, ClaimScope};

    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
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

    let follower = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
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
        request_memory: None,
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
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
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
    let mut follower = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
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
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    writer.create().await.unwrap();
    assert_eq!(writer.tick(&live(1)).await.unwrap(), Role::Writer(Epoch(1)));
    let before = writer.transact(&put(2, 2, "earlier transaction")).await.unwrap();
    let mut timed = put(1, 1, "published");
    timed.window.before = Some(Timestamp(30));
    let receipt = writer.transact(&timed).await.unwrap();
    assert_eq!(receipt.outcome, Outcome::Then);
    assert_eq!(receipt.basis.position(), 4);
    assert!(receipt.system_time > before.system_time);
    let original = Lsn(receipt.basis.position() - 1);
    let page = writer.stream.read(original).await.unwrap();
    let (position, bytes) = &page.records[0];
    assert_eq!(*position, original);
    let entry = writer.stream.decode(*position, bytes).await.unwrap();
    let Record::Tx { payload, .. } = entry.record else { panic!("expected the original Tx"); };
    let TransactionRecord::Tx(record) = TransactionRecord::decode(&payload).unwrap()
        else { panic!("expected a resolved Tx"); };
    assert_eq!(record.admission(), (timed.key.as_slice(), Timestamp(10), timed.logical_sha256().unwrap()));
    assert_eq!(record.canonical_request().unwrap().as_bytes(), timed.canonical_request().unwrap().as_bytes());
    assert!(record.admission().1 < receipt.system_time);
    assert_eq!(record.effects().len(), 1);
    writer.checkpoint(vec![]).await.unwrap();
    assert_eq!(writer.retained_floor(), Some(Lsn(5)));
    drop(writer);
    drop(journal);

    let journal = FileJournal::open(path, Limits::default()).unwrap();
    let mut successor = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    assert_eq!(successor.tick(&live(2)).await.unwrap(), Role::Follower);
    assert_eq!(
        successor.tx_status(&timed.key).await.unwrap().basis,
        receipt.basis
    );
    cluster.at(receipt.system_time.0);
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
    timed.window.before = Some(Timestamp(31));
    assert_eq!(successor.transact(&timed).await.unwrap_err().code, "rtdb/idempotency-key");
}

#[tokio::test]
async fn managed_checkpoint_pin_is_exclusive_and_restores_after_a_lost_reply() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut source = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    source.create().await.unwrap();
    source.tick(&live(1)).await.unwrap();
    let selected = source.transact(&put(1, 1, "at the reserved cut")).await.unwrap().basis;
    let pin = Uuid::from_u128(3);
    let destination = Uuid::from_u128(7);
    let target = ManagedCheckpointTarget { incarnation: *destination.as_bytes(), request: [8; 32] };
    let source_identity = source.pin_managed_checkpoint(pin, selected, target).await.unwrap();
    let reservation = ManagedCheckpointReservation { source: source_identity, target, reserved_at: Basis::new([6; 16], 4) };

    assert_eq!(source.pin_managed_checkpoint(pin, selected, target).await.unwrap(), source_identity);
    assert_eq!(source.pin_checkpoint_at(pin, selected, None).await.unwrap_err().code, "rtdb/pin-id");
    assert_eq!(source.open_pin(pin).await.err().unwrap().code, "rtdb/managed-checkpoint-custody");
    assert_eq!(source.open_export(pin).await.err().unwrap().code, "rtdb/managed-checkpoint-custody");
    assert_eq!(source.release_pin(pin).await.unwrap_err().code, "rtdb/managed-checkpoint-custody");
    let changed = ManagedCheckpointReservation { target: ManagedCheckpointTarget { request: [9; 32], ..target }, ..reservation };
    assert!(source.open_managed_checkpoint(changed).await.is_err());
    source.checkpoint(Vec::new()).await.unwrap();
    let mut follower = cluster.node(3, Arc::new(journal.handle(NODE_3_IDENTITY)));
    let export = follower.open_managed_checkpoint(reservation).await.unwrap();

    let faults = Arc::new(FaultJournal::new(Arc::new(journal.handle(NODE_2_IDENTITY))));
    let mut child = cluster.node_for(destination, 2, faults.clone());
    child.create().await.unwrap();
    child.tick(&live(2)).await.unwrap();
    child.require_managed_checkpoint_destination(reservation).await.unwrap();
    assert!(child.restore_export(&export).await.is_err());
    assert!(child.restore_managed_checkpoint(&export, changed).await.is_err());
    assert!(child.require_managed_checkpoint_restore(reservation).await.is_err());

    faults.inject(Fault::DropAppendReply);
    assert_eq!(child.restore_managed_checkpoint(&export, reservation).await.unwrap_err().category, AnomalyCategory::Interrupted);
    let mut reopened = cluster.node_for(destination, 3, Arc::new(journal.handle(NODE_3_IDENTITY)));
    let (basis, view, _) = reopened.require_managed_checkpoint_restore(reservation).await.unwrap();
    assert_eq!(basis, Basis::new(target.incarnation, selected.position()));
    assert_eq!(view.head().basis, basis);
    assert_eq!(text_at(&mut reopened, MAIN_BRANCH, 1).await, Some(Value::Utf8("at the reserved cut".into())));
    assert!(source.release_managed_checkpoint(changed).await.is_err());
    source.release_managed_checkpoint(reservation).await.unwrap();
    source.release_managed_checkpoint(reservation).await.unwrap();
    assert_eq!(follower.open_managed_checkpoint(reservation).await.err().unwrap().code, "rtdb/unknown-pin");
    assert_eq!(reopened.require_managed_checkpoint_restore(reservation).await.unwrap().0, basis);
}

#[tokio::test]
async fn native_checkpoint_keeps_original_occurrences_and_fences_reads_until_the_reciprocal_drain() {
    use rtdb_index::branches::{BranchCut, BranchEvent, WrittenVersion};
    use rtdb_tx::transact::{CustodyPayload, CustodyPlacement, CustodyPlacementCommitments, CustodyWriterFence, DerivedCustodyReservation};
    use rtdb_types::Interval;
    use rtdb_types::rtdb_v1::{
        DependentApplicationKey, DependentCustodyReceipt, DependentKeyCustody, DependentKeyDisposition,
        DependentKeyTerminal, NativeCustodyFence, NativeDependentCustodyDisposition, NativeDependentCustodyEnrollment,
        NativeDependentCustodyMember, NativeDependentCustodyOrder, NativeDependentCustodyScope, NativeDependentPoint, NativeDependentRead,
    };
    use sha2::{Digest, Sha256};

    let journal = journals().await;
    let cluster = Cluster::new();
    let mut source = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    source.create().await.unwrap();
    source.tick(&live(1)).await.unwrap();
    let fork_at = source.fresh_index().await.unwrap().head().basis;
    let fork = request(50, Op::Fork {
        child: "copied-point".into(), from: (MAIN_BRANCH.into(), fork_at), writable: true, expires_at: None,
    });
    let forked = append_custody_fixture(&mut source, &fork).await;
    assert_eq!(forked.outcome, Outcome::Then);
    let linked_valid = Interval::from_micros(0, 10_000).unwrap();
    let registry = TableRef::new("public", "custody_registry");
    let mut placements = Vec::new();
    for n in [1, 2] {
        let group = [n; 16];
        let binding = [n; 32];
        let carrier = vec![n; 12];
        let vault = source.token_authority().custody_vault();
        vault.create(group).await.unwrap();
        let clear = serde_json::to_vec(&StructFields::from([("carrier".into(), Value::Binary(carrier.clone()))])).unwrap();
        let envelope = vault.seal(group, binding, &clear).await.unwrap();
        let occurrence = Op::PlaceCustodyRow {
            branch: MAIN_BRANCH.into(), table: table(),
            row: Row {
                id: EntityId::Integer(i64::from(n)), valid: None, cells: StructFields::new(),
                custody_group: Some(group), protected_columns: vec!["carrier".into()],
                custody_payload: Some(CustodyPayload { binding, envelope: envelope.clone() }),
            },
            placement: CustodyPlacement {
                placement_id: group, reservation: None, derived: None, carrier_column: "carrier".into(), link_column: None,
                commitments: Some(CustodyPlacementCommitments { carrier_sha256: Sha256::digest(carrier).into(), link_sha256: None }),
            },
        };
        let mut write = request(n, Op::CreateCustodyGroup { group });
        write.ops.push(occurrence);
        if n == 1 {
            let carrier = vec![3; 12];
            let link = vec![4; 12];
            let carrier_sha256 = Sha256::digest(&carrier).into();
            let link_sha256 = Sha256::digest(&link).into();
            let clear = serde_json::to_vec(&StructFields::from([
                ("carrier".into(), Value::Binary(carrier)), ("link".into(), Value::Binary(link)),
            ])).unwrap();
            let binding = [3; 32];
            let payload = vault.seal(group, binding, &clear).await.unwrap();
            write.ops.push(Op::PlaceCustodyRow {
                branch: "copied-point".into(), table: table(),
                row: Row {
                    id: EntityId::Integer(3), valid: Some(linked_valid.into()), cells: StructFields::new(),
                    custody_group: Some(group), protected_columns: vec!["carrier".into(), "link".into()],
                    custody_payload: Some(CustodyPayload { binding, envelope: payload }),
                },
                placement: CustodyPlacement {
                    placement_id: [3; 16], reservation: None, carrier_column: "carrier".into(), link_column: Some("link".into()),
                    commitments: Some(CustodyPlacementCommitments { carrier_sha256, link_sha256: Some(link_sha256) }),
                    derived: Some(DerivedCustodyReservation {
                        original_placement_id: group, link: [4; 16], review_sha256: [5; 32], carrier_sha256, link_sha256,
                    }),
                },
            });
        }
        write.ops.push(Op::Put {
            branch: MAIN_BRANCH.into(), table: registry.clone(), if_absent: true,
            rows: vec![Row {
                id: EntityId::String(format!("{:032x}:member", u128::from_be_bytes(group))),
                valid: None, cells: StructFields::from([("body".into(), Cell::Value(Value::Binary(br#"{"member":true}"#.to_vec())))]),
                custody_group: None, protected_columns: Vec::new(), custody_payload: None,
            }],
        });
        let receipt = append_custody_fixture(&mut source, &write).await;
        assert_eq!(receipt.outcome, Outcome::Then);
        placements.push((group, receipt, envelope));
    }
    let (group, original, envelope) = &placements[0];
    let (other_group, selected, _) = &placements[1];
    let other_inventory = source.fresh_index().await.unwrap().custody_inventory(*other_group).unwrap().sha256;
    let target_id = Uuid::from_u128(7);
    let target = ManagedCheckpointTarget { incarnation: *target_id.as_bytes(), request: [8; 32] };
    let pin = Uuid::from_u128(3);
    assert_eq!(source.pin_checkpoint_at(pin, selected.basis, None).await.unwrap_err().code, "rtdb/export-custody");
    assert_eq!(source.pin_managed_checkpoint(pin, original.basis, target).await.unwrap_err().code, "rtdb/native-checkpoint-cut");
    let identity = source.pin_managed_checkpoint(pin, selected.basis, target).await.unwrap();
    let reservation = ManagedCheckpointReservation { source: identity, target, reserved_at: Basis::new([6; 16], 4) };
    assert_eq!(source.release_managed_checkpoint(reservation).await.unwrap_err().code, "rtdb/native-checkpoint-terminal");
    let claim = source.claim_native_checkpoint_copy(reservation).await.unwrap();
    assert_eq!(source.claim_native_checkpoint_copy(reservation).await.unwrap_err().code, "rtdb/native-checkpoint-copy-claimed");
    assert_ne!(source.fresh_index().await.unwrap().custody_inventory(*other_group).unwrap().sha256, other_inventory);
    let mut revoked_while_copying = source.fresh_index().await.unwrap().clone();
    let head = revoked_while_copying.head();
    revoked_while_copying.advance(rtdb_index::branches::BranchCut {
        basis: Basis::new(head.basis.incarnation(), head.basis.position() + 1),
        system_time: head.system_time.next().unwrap(),
    }).unwrap();
    revoked_while_copying.apply(BranchEffect::RevokeCustodyGroup { group: *other_group }).unwrap();
    assert!(!revoked_while_copying.custody_releases_drained(*other_group));

    let export = source.open_managed_checkpoint(reservation).await.unwrap();
    let faults = Arc::new(FaultJournal::new(Arc::new(journal.handle(NODE_2_IDENTITY))));
    let mut target = cluster.node_for(target_id, 2, faults.clone());
    target.create().await.unwrap();
    target.tick(&live(2)).await.unwrap();
    assert_eq!(target.restore_managed_checkpoint(&export, reservation).await.unwrap_err().code, "rtdb/restore-custody");
    faults.inject(Fault::DropAppendReply);
    assert_eq!(target.restore_native_checkpoint(&export, &claim).await.unwrap_err().category, AnomalyCategory::Interrupted);
    drop(export);

    let mut follower = cluster.node_for(target_id, 3, Arc::new(journal.handle(NODE_3_IDENTITY)));
    let installation = follower.native_checkpoint_installation(reservation).await.unwrap();
    let installed = installation.basis();
    assert_eq!(installation.custody(), &claim);
    let (_, view, destination_generation) = follower.require_managed_checkpoint_restore(reservation).await.unwrap();
    assert_eq!(view.require_local_custody().unwrap_err().code, "rtdb/inherited-custody");
    assert!(view.custody_inventory(*group).is_none());
    let scope = NativeDependentCustodyScope {
        reservation, destination: installed, source_enrolled_at: claim.enrolled_at, group: *group,
        checkpoint_group_sha256: claim.groups[0].1,
    };
    let metadata = view.native_dependent_checkpoint_metadata(scope, &registry, "body", 1024).unwrap();
    assert_eq!(metadata.scope, scope);
    assert_eq!(metadata.members.len(), 2);
    assert_eq!(metadata.members[0].source_basis, original.basis);
    assert_eq!(metadata.members[0].enrolled, original.basis);
    assert_eq!(metadata.members[0].target, EntityId::Integer(1));
    assert_eq!(metadata.rows.len(), 1);
    assert_eq!(metadata.rows[0].body.as_slice(), br#"{"member":true}"#);
    assert_eq!(metadata.rows[0].written, Basis::new(reservation.target.incarnation, original.basis.position()));
    assert!(view.native_dependent_checkpoint_metadata(scope, &registry, "body", 1).is_err());
    let mut wrong = scope;
    wrong.checkpoint_group_sha256 = [0; 32];
    assert!(view.native_dependent_checkpoint_metadata(wrong, &registry, "body", 1024).is_err());
    let (inherited, occurrence) = view.native_inherited_carrier(MAIN_BRANCH, &table(), &EntityId::Integer(1), envelope).unwrap();
    assert_eq!(inherited, *group);
    assert_eq!(view.native_inherited_carrier(MAIN_BRANCH, &table(), &EntityId::Integer(2), envelope).unwrap_err().code, "rtdb/native-checkpoint-binding");
    assert_eq!(source.fresh_index().await.unwrap().check_native_export_read(&claim, installed, inherited, occurrence).unwrap_err().code, "rtdb/native-checkpoint-binding");

    let read = NativeDependentRead {
        scope,
        destination_generation,
        branch: MAIN_BRANCH.into(),
        branch_created: view.lineage_at(MAIN_BRANCH, installed).unwrap().created,
        table: table(),
        id: EntityId::Integer(1),
        valid_at: original.system_time,
        placement: *group,
        occurrence,
        application_key: [14; 32],
    };
    let (copy, binding) = view.native_dependent_destination(&read, original.system_time).unwrap();
    assert_eq!(copy, &claim);
    let derived = NativeDependentRead {
        placement: [3; 16], occurrence: metadata.members[1].native.occurrence.unwrap(),
        branch: "copied-point".into(), branch_created: Basis::new(installed.incarnation(), forked.basis.position()),
        id: EntityId::Integer(3), ..read.clone()
    };
    let (_, derived_binding) = view.native_dependent_destination(&derived, original.system_time).unwrap();
    let point = NativeDependentPoint {
        scope, branch: derived.branch.clone(), table: derived.table.clone(), id: derived.id.clone(),
        valid_at: derived.valid_at, placement: derived.placement,
    };
    let (created, valid, member) = view.native_dependent_point_selector(&point, original.system_time).unwrap();
    assert_eq!((created, valid), (derived.branch_created, linked_valid));
    assert_eq!(member, metadata.members[1]);
    let outside = NativeDependentPoint { valid_at: linked_valid.upper(), ..point.clone() };
    assert!(view.native_dependent_point_selector(&outside, original.system_time).is_err());
    let other_id = NativeDependentPoint { id: EntityId::String("3".into()), ..point.clone() };
    assert!(view.native_dependent_point_selector(&other_id, original.system_time).is_err());
    let other_placement = NativeDependentPoint { placement: *group, ..point.clone() };
    assert!(view.native_dependent_point_selector(&other_placement, original.system_time).is_err());
    let mut shadowed = view.clone();
    let head = shadowed.head();
    let cut = BranchCut { basis: Basis::new(head.basis.incarnation(), head.basis.position() + 1), system_time: head.system_time.next().unwrap() };
    shadowed.advance(cut).unwrap();
    shadowed.apply(BranchEffect::Write {
        branch: point.branch.clone(), table: point.table.clone(),
        event: BranchEvent { id: point.id.clone(), valid: linked_valid, cut, image: None, written: WrittenVersion::Branch },
    }).unwrap();
    assert!(shadowed.native_dependent_point_selector(&point, cut.system_time).is_err());
    let mut substituted = read.clone();
    substituted.placement = *other_group;
    assert!(view.check_native_dependent_destination(&substituted, original.system_time).is_err());
    substituted = read.clone();
    substituted.branch_created = installed;
    assert!(view.check_native_dependent_destination(&substituted, original.system_time).is_err());
    let writer = CustodyWriterFence {
        table: table(), id: read.id.clone(), valid_at: read.valid_at, written: original.basis,
    };

    source.record_native_checkpoint_drain(installation).await.unwrap();
    assert!(source.fresh_index().await.unwrap().require_native_dependent_publication(reservation, installed).is_err());
    assert!(source.native_dependent_original(&claim, &read, &writer).await.is_err());
    let witness = source.native_dependent_source_generation(read.scope).await.unwrap();
    source.release_managed_checkpoint(reservation).await.unwrap();
    let mut enrollments = Vec::new();
    for &(group, checkpoint_group_sha256) in &claim.groups {
        let scope = NativeDependentCustodyScope {
            reservation, destination: installed, source_enrolled_at: claim.enrolled_at, group, checkpoint_group_sha256,
        };
        let roster = source.fresh_index().await.unwrap().native_dependent_custody_roster(scope).unwrap();
        assert_eq!(roster.len(), if group == *other_group { 1 } else { 2 });
        let mut enrollment = NativeDependentCustodyEnrollment {
            scope, owner: [11; 32], application_scope: [12; 32],
            receipt: DependentCustodyReceipt { key: checkpoint_group_sha256, evidence_sha256: [13; 32] },
            members: Vec::new(),
        };
        assert!(source.record_native_dependent_custody(NativeDependentCustodyOrder::Enroll(enrollment.clone())).await.is_err());
        enrollment.members = roster.into_iter().map(|native| {
            let mut carriers = vec![native.carrier_sha256];
            carriers.extend(native.link_sha256);
            carriers.sort_unstable();
            NativeDependentCustodyMember {
                native,
                application_keys: vec![DependentApplicationKey {
                    key: [14; 32], custody: DependentKeyCustody::OriginalReference, carriers,
                }],
            }
        }).collect();
        let generation = source.native_dependent_source_generation(scope).await.unwrap();
        let mut stale_generation = generation;
        stale_generation.writer_epoch += 1;
        let order = NativeDependentCustodyOrder::Enroll(enrollment.clone());
        assert!(source.record_native_dependent_in_generation(order.clone(), Some(stale_generation)).await.is_err());
        source.record_native_dependent_in_generation(order, Some(generation)).await.unwrap();
        enrollments.push(enrollment);
    }
    source.fresh_index().await.unwrap().require_native_dependent_publication(reservation, installed).unwrap();
    assert_eq!(source.pin_managed_checkpoint(pin, selected.basis, reservation.target).await.unwrap_err().code, "rtdb/native-checkpoint-pin-used");
    source.checkpoint(Vec::new()).await.unwrap();
    let mut original_follower = cluster.node(3, Arc::new(journal.handle(NODE_3_IDENTITY)));
    let original_view = original_follower.fresh_index().await.unwrap();
    original_view.check_native_export_read(&claim, installed, inherited, occurrence).unwrap();
    let (_, retained) = original_view.custody_read_occurrence(selected.basis, inherited, occurrence).unwrap();
    assert_eq!(retained.written, original.basis);
    assert_eq!(retained.receipt_key, original.key);
    let checked = source.native_dependent_original(&claim, &read, &writer).await.unwrap();
    assert_eq!(checked.receipt.key, original.key);
    assert_eq!(checked.source_generation, witness);
    let evidence = source.native_dependent_carrier(&claim, &read, &writer, binding).await.unwrap();
    assert_eq!(evidence.payload.envelope, *envelope);
    let [(contributor, receipt)] = evidence.contributors.as_slice() else { panic!("one original contributor"); };
    assert_eq!(contributor.placement_id, *group);
    assert_eq!(contributor.commitment(*group), occurrence);
    assert_eq!(receipt.basis, original.basis);
    assert!(source.native_dependent_carrier(&claim, &read, &writer, [0; 32]).await.is_err());
    let linked = source.native_dependent_carrier(&claim, &derived, &writer, derived_binding).await.unwrap();
    let mut expected = vec![read.occurrence, derived.occurrence];
    expected.sort_unstable();
    assert_eq!(linked.contributors.iter().map(|(member, _)| member.commitment(*group)).collect::<Vec<_>>(), expected);
    assert!(linked.contributors.iter().all(|(_, receipt)| receipt.basis == original.basis));
    let mut wrong_reader = read.clone();
    wrong_reader.application_key = [20; 32];
    assert!(source.native_dependent_original(&claim, &wrong_reader, &writer).await.is_err());
    assert!(original_follower.native_dependent_original(&claim, &read, &writer).await.is_err());
    assert_eq!(source.tick(&live(3)).await.unwrap(), Role::Follower);
    assert_eq!(original_follower.tick(&live(3)).await.unwrap(), Role::Writer(Epoch(2)));
    assert!(source.native_dependent_original(&claim, &read, &writer).await.is_err());
    let mut source = original_follower;
    let successor = source.native_dependent_original(&claim, &read, &writer).await.unwrap();
    assert_eq!(successor.receipt.basis, checked.receipt.basis);
    assert_ne!(successor.source_generation, checked.source_generation);

    let stale = request(3, Op::RevokeCustodyGroup { group: *other_group, inventory_sha256: other_inventory });
    let stale = append_custody_fixture(&mut source, &stale).await;
    assert!(matches!(stale.outcome, Outcome::Refused { error } if error.code == "rtdb/custody-inventory-changed"));
    let inventory = source.fresh_index().await.unwrap().custody_inventory(*other_group).unwrap().sha256;
    let revoke = request(4, Op::RevokeCustodyGroup { group: *other_group, inventory_sha256: inventory });
    let revoked = append_custody_fixture(&mut source, &revoke).await;
    assert_eq!(revoked.outcome, Outcome::Then);
    assert_eq!(source.fresh_index().await.unwrap().check_native_export_read(&claim, installed, inherited, occurrence).unwrap_err().code, "rtdb/erased-under-view");
    assert!(source.native_dependent_original(&claim, &read, &writer).await.is_err());
    assert!(source.native_dependent_carrier(&claim, &read, &writer, binding).await.is_err());
    assert!(!source.fresh_index().await.unwrap().custody_releases_drained(*other_group));

    let enrollment = enrollments.iter().find(|enrollment| enrollment.scope.group == *other_group).unwrap();
    let mut disposition = NativeDependentCustodyDisposition {
        scope: enrollment.scope,
        fence: NativeCustodyFence { revoke_key: revoked.key, revoked_at: revoked.basis, inventory_sha256: inventory },
        enrollment: enrollment.receipt,
        receipt: DependentCustodyReceipt { key: [15; 32], evidence_sha256: [16; 32] },
        keys: Vec::new(),
    };
    assert!(source.native_dependent_custody_status(disposition.scope, &disposition.fence).await.unwrap().is_none());
    assert!(source.record_native_dependent_custody(NativeDependentCustodyOrder::Dispose(disposition.clone())).await.is_err());
    disposition.keys.push(DependentKeyDisposition {
        key: [14; 32], reader_drain_sha256: [17; 32], part_disposition_sha256: [18; 32],
        terminal: DependentKeyTerminal::Destroyed([19; 32]),
    });
    assert!(source.record_native_dependent_custody(NativeDependentCustodyOrder::Dispose(disposition.clone())).await.is_err());
    disposition.keys[0].terminal = DependentKeyTerminal::ReleasedOriginalReference([19; 32]);
    let mut wrong_fence = disposition.clone();
    wrong_fence.fence.revoke_key = original.key.clone();
    assert!(source.record_native_dependent_custody(NativeDependentCustodyOrder::Dispose(wrong_fence)).await.is_err());
    let order = NativeDependentCustodyOrder::Dispose(disposition);
    let terminal = source.record_native_dependent_custody(order.clone()).await.unwrap();
    assert!(source.fresh_index().await.unwrap().custody_releases_drained(*other_group));
    source.checkpoint(Vec::new()).await.unwrap();
    let NativeDependentCustodyOrder::Dispose(disposition) = &order else { unreachable!() };
    let status = source.native_dependent_custody_status(disposition.scope, &disposition.fence).await.unwrap().unwrap();
    assert_eq!(status.key, terminal.key);
    assert_eq!(status.basis, terminal.basis);
    let recovered = source.record_native_dependent_custody(order).await.unwrap();
    assert_eq!(recovered.key, terminal.key);
    assert_eq!(recovered.basis, terminal.basis);
    assert!(recovered.replayed);
}

async fn append_custody_fixture(database: &mut MultiDatabase, request: &Transact) -> Receipt {
    use rtdb_tx::transact::ROW_CUSTODY;
    let lsn = next(database.state().head).unwrap();
    let position = database.state().coordinates.position(lsn).unwrap();
    let claims = database.tokens.verify_claims(request.claims()).await;
    let Resolution::Append(prepared) = database.state().transactions
        .resolve(request, position, (database.clock)(), &[ROW_CUSTODY], &claims).unwrap()
    else { panic!("expected an ordered custody fixture"); };
    let lease = database.authority().unwrap();
    let record = Record::Tx { payload: prepared.record().encode().unwrap().to_vec(), objects: vec![] };
    let receipt = database.append(lease, record).await.unwrap().unwrap();
    let pipeline = database.new_pipeline().unwrap();
    let writer = database.writer.as_mut().unwrap();
    writer.pipeline = pipeline;
    writer.acknowledged = lsn;
    receipt
}

#[tokio::test]
async fn portable_backup_restores_a_fresh_stream_without_source_data() {
    use rtdb_crypt::FileKms;

    let source_vault = tempfile::tempdir().unwrap();
    let destination_vault = tempfile::tempdir().unwrap();
    let source_path = source_vault.path().join("keys");
    let destination_path = destination_vault.path().join("keys");
    FileKms::create_directory(&source_path).unwrap();
    FileKms::create_directory(&destination_path).unwrap();
    let source_keys = Arc::new(FileKms::open(&source_path, || 10).unwrap());
    let destination_keys = Arc::new(FileKms::open(&destination_path, || 10).unwrap());
    let options = |database, node| MultiOptions {
        database,
        node: ip(node),
        lease_micros: LEASE,
        drift_micros: DRIFT,
    };
    let request = put(1, 1, "portable row");
    let (archive, receipt, branch_cut, source_lsn) = {
        let source = journals().await;
        let mut writer = MultiDatabase::new(options(DATABASE, 1), Arc::new(source.handle(NODE_1_IDENTITY)),
            Arc::new(MemoryStore::default()), source_keys.clone(), Arc::new(|| Timestamp(10))).unwrap();
        writer.create().await.unwrap();
        writer.tick(&live(1)).await.unwrap();
        let receipt = writer.transact(&request).await.unwrap();
        let branch_cut = writer.transact(&self::request(2, Op::Fork {
            child: "backup-report".into(), from: (MAIN_BRANCH.into(), receipt.basis), writable: true, expires_at: None,
        })).await.unwrap();
        let source_lsn = writer.stream.head().await.unwrap();
        let archive = writer.portable_backup().await.unwrap().into_bytes();
        writer.transact(&put(3, 1, "after backup")).await.unwrap();
        (archive, receipt, branch_cut, source_lsn)
    };
    assert!(crate::PortableBackup::verify(&archive, destination_keys.clone()).await.is_err());
    for byte in [8, archive.len() - 1] {
        let mut changed = archive.clone();
        changed[byte] ^= 1;
        assert!(crate::PortableBackup::verify(&changed, source_keys.clone()).await.is_err());
    }
    let backup = crate::PortableBackup::verify(&archive, source_keys.clone()).await.unwrap();
    assert_eq!(backup.source_receipt(&request.key).await.unwrap(), Receipt { replayed: true, ..receipt.clone() });
    assert_eq!(backup.source_basis(), branch_cut.basis);
    assert_eq!(backup.source_lsn(), source_lsn);
    assert_eq!(backup.source_receipt(&[3; 16]).await.unwrap_err().code, "rtdb/unknown-transaction");

    let destination = Uuid::from_u128(999);
    let destination_time = Arc::new(AtomicI64::new(10));
    let journal = journals().await;
    let store = Arc::new(MemoryStore::default());
    let open = |node, handle| {
        let clock = Arc::clone(&destination_time);
        MultiDatabase::new(options(destination, node), Arc::new(journal.handle(handle)),
            store.clone(), destination_keys.clone(), Arc::new(move || Timestamp(clock.load(Ordering::SeqCst)))).unwrap()
    };
    let mut successor = open(2, NODE_2_IDENTITY);
    successor.create().await.unwrap();
    successor.tick(&live(2)).await.unwrap();
    let install = successor.install_portable_backup(backup).await.unwrap();
    assert_eq!(install.source_incarnation(), DATABASE);
    assert_eq!(install.source_lsn(), source_lsn);
    let rebase = |basis: Basis| Basis::new(*destination.as_bytes(), basis.position());
    assert_eq!(install.destination_basis(), rebase(branch_cut.basis));
    let restored = Receipt { replayed: true, basis: rebase(receipt.basis), ..receipt };
    assert_eq!(successor.transact(&request).await.unwrap(), restored);
    assert_eq!(text_at(&mut successor, "backup-report", 1).await, Some(Value::Utf8("portable row".into())));
    let retry = crate::PortableBackup::verify(&archive, source_keys.clone()).await.unwrap();
    assert_eq!(successor.install_portable_backup(retry).await.unwrap(), install);
    let new = successor.transact(&put(4, 2, "new history")).await.unwrap();
    assert!(new.basis > install.destination_basis());
    destination_time.store(new.system_time.0, Ordering::SeqCst);
    successor.checkpoint(Vec::new()).await.unwrap();
    drop(successor);
    drop(source_keys);
    drop(source_vault);
    let mut reopened = open(3, NODE_3_IDENTITY);
    assert_eq!(reopened.installed_backup().await.unwrap(), Some(install));
    assert_eq!(reopened.tx_status(&request.key).await.unwrap(), restored);
    assert_eq!(text_at(&mut reopened, MAIN_BRANCH, 2).await, Some(Value::Utf8("new history".into())));
}

#[tokio::test]
async fn portable_backup_refuses_external_objects_and_revoked_keys() {
    use rtdb_crypt::FileKms;

    let vault = tempfile::tempdir().unwrap();
    let path = vault.path().join("keys");
    FileKms::create_directory(&path).unwrap();
    let keys = Arc::new(FileKms::open(&path, || 10).unwrap());
    let journal = journals().await;
    let mut writer = MultiDatabase::new(MultiOptions { database: DATABASE, node: ip(1), lease_micros: LEASE, drift_micros: DRIFT },
        Arc::new(journal.handle(NODE_1_IDENTITY)), Arc::new(MemoryStore::default()), keys.clone(), Arc::new(|| Timestamp(10))).unwrap();
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    let archive = writer.portable_backup().await.unwrap().into_bytes();
    let file = writer.write_file(b"external checkpoint dependency").await.unwrap();
    writer.checkpoint(vec![file]).await.unwrap();
    assert_eq!(writer.portable_backup().await.err().unwrap().code, "rtdb/backup-dependencies");
    let verified = crate::PortableBackup::verify(&archive, keys.clone()).await.unwrap();
    let destination = Uuid::from_u128(999);
    let store = Arc::new(MemoryStore::default());
    let mut successor = MultiDatabase::new(MultiOptions { database: destination, node: ip(2), lease_micros: LEASE, drift_micros: DRIFT },
        Arc::new(journal.handle(NODE_2_IDENTITY)), store.clone(), keys.clone(), Arc::new(|| Timestamp(10))).unwrap();
    successor.create().await.unwrap();
    successor.tick(&live(2)).await.unwrap();
    let objects_before_restore = store.list(None).await.unwrap();
    let head_before_restore = successor.stream.head().await.unwrap();

    let generation = Generation { database: DATABASE, number: 1 };
    keys.retire(generation, 10).await.unwrap();
    assert_eq!(crate::PortableBackup::verify(&archive, keys).await.err().unwrap().code, "rtdb/erased-under-view");
    assert_eq!(successor.install_portable_backup(verified).await.unwrap_err().code, "rtdb/erased-under-view");
    assert_eq!(successor.stream.head().await.unwrap(), head_before_restore);
    assert_eq!(store.list(None).await.unwrap(), objects_before_restore);
    assert_eq!(successor.installed_backup().await.unwrap(), None);
}

#[tokio::test]
async fn exact_export_pins_the_selected_branch_ancestry_after_trim() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    let original = writer.transact(&put(1, 1, "parent row")).await.unwrap();
    let selected = writer.transact(&request(2, Op::Fork {
        child: "report".into(), from: (MAIN_BRANCH.into(), original.basis), writable: true, expires_at: None,
    })).await.unwrap();
    let future = writer.transact(&put(3, 1, "later row")).await.unwrap();
    writer.transact(&request(4, Op::DropBranch { branch: "report".into() })).await.unwrap();
    writer.checkpoint(Vec::new()).await.unwrap();

    let id = Uuid::from_u128(7);
    let pin = writer.pin_checkpoint_at(id, selected.basis, None).await.unwrap();
    assert_eq!(writer.pin_checkpoint_at(id, selected.basis, None).await.unwrap(), pin);
    assert_eq!(writer.pin_checkpoint_at(id, future.basis, None).await.unwrap_err().code, "rtdb/pin-id");
    writer.checkpoint(Vec::new()).await.unwrap();

    let ticket = writer.export_ticket(id, selected.basis, None).await.unwrap();
    assert_eq!(writer.export_ticket(id, selected.basis, None).await.unwrap(), ticket);
    let mut changed = ticket.clone();
    *changed.last_mut().unwrap() ^= 1;
    assert_eq!(writer.open_export_ticket(&changed).await.err().unwrap().code, "rtdb/invalid-export");
    let export = writer.open_export_ticket(&ticket).await.unwrap();
    let identity = export.checkpoint_identity().unwrap();
    let mut source_follower = cluster.node(3, Arc::new(journal.handle(NODE_3_IDENTITY)));
    assert_eq!(source_follower.role(), Role::Follower);
    assert_eq!(
        source_follower.open_export_ticket(&ticket).await.unwrap().checkpoint_identity().unwrap(),
        identity,
    );
    assert_eq!(identity.pin, *id.as_bytes());
    assert_eq!(identity.basis, selected.basis);
    assert_eq!(identity.until, None);
    assert_eq!(writer.open_export_ticket(&ticket).await.unwrap().checkpoint_identity().unwrap(), identity);
    let (cut, checkpoint) = export.exact_checkpoint().await.unwrap();
    assert_eq!(cut.basis, selected.basis);
    let view = checkpoint.index().view("report", At::Exactly(cut.basis), Layer::Merged, cut.system_time).unwrap();
    assert_eq!(view.base(), Some(&(MAIN_BRANCH.into(), original.basis)));
    assert_eq!(view.row(&table(), &EntityId::Integer(1), cut.system_time)
        .and_then(|row| row.image_at(cut.system_time)).and_then(|image| image.cells.get("text")),
        Some(&Value::Utf8("parent row".into())));
    assert_eq!(checkpoint.tx_status(&[1; 16]).unwrap().basis, original.basis);
    assert_eq!(checkpoint.tx_status(&[3; 16]).unwrap_err().code, "rtdb/unknown-transaction");
    assert_eq!(checkpoint.index().exact(future.basis).unwrap_err().code, "rtdb/unknown-basis");

    let destination = Uuid::from_u128(7);
    let faults = Arc::new(FaultJournal::new(Arc::new(journal.handle(NODE_2_IDENTITY))));
    let mut child = cluster.node_for(destination, 2, faults.clone());
    child.create().await.unwrap();
    child.tick(&live(2)).await.unwrap();
    faults.inject(Fault::DropAppendReply);
    assert_eq!(child.restore_export(&export).await.unwrap_err().category, AnomalyCategory::Interrupted);
    child.tick(&live(2)).await.unwrap();
    let restored = child.restore_export(&export).await.unwrap();
    let local = |basis: Basis| Basis::new(*destination.as_bytes(), basis.position());
    assert_eq!(restored.basis, local(selected.basis));
    let source = Some(crate::CheckpointSource { cut, pin: id });
    assert_eq!(child.checkpoint_source().await.unwrap(), source);
    assert_eq!(child.tx_status(&original.key).await.unwrap().basis, local(original.basis));
    let index = child.fresh_index().await.unwrap();
    assert_eq!(index.exact(selected.basis).unwrap_err().code, "rtdb/foreign-basis");
    assert_eq!(index.view("report", At::Latest, Layer::Merged, restored.system_time).unwrap().base(),
        Some(&(MAIN_BRANCH.into(), local(original.basis))));
    assert_eq!(text_at(&mut child, "report", 1).await, Some(Value::Utf8("parent row".into())));
    assert_eq!(child.fresh_history().await.unwrap().after_floor, restored.basis.position());

    let mut timed = put(5, 2, "new deployment");
    timed.window.before = Some(Timestamp(50));
    let written = child.transact(&timed).await.unwrap();
    assert!(written.basis > restored.basis);
    assert_eq!(written.outcome, Outcome::Then);
    writer.release_export_ticket(&ticket).await.unwrap();
    writer.release_export_ticket(&ticket).await.unwrap();
    assert_eq!(writer.open_export_ticket(&ticket).await.err().unwrap().code, "rtdb/unknown-pin");
    assert_eq!(source_follower.open_export_ticket(&ticket).await.err().unwrap().code, "rtdb/unknown-pin");
    let replacement = writer.export_ticket(id, selected.basis, None).await.unwrap();
    assert_ne!(replacement, ticket);
    assert_eq!(writer.open_export_ticket(&ticket).await.err().unwrap().code, "rtdb/invalid-export");
    assert_eq!(writer.release_export_ticket(&ticket).await.unwrap_err().code, "rtdb/invalid-export");
    writer.release_export_ticket(&replacement).await.unwrap();
    child.checkpoint(Vec::new()).await.unwrap();
    let mut follower = cluster.node_for(destination, 3, Arc::new(journal.handle(NODE_3_IDENTITY)));
    assert_eq!(follower.tx_status(&timed.key).await.unwrap().basis, written.basis);
    assert_eq!(follower.checkpoint_source().await.unwrap(), source);
    assert_eq!(text_at(&mut follower, "report", 1).await, Some(Value::Utf8("parent row".into())));
}

/// A reference journal that an operator recovery can cut back, as
/// `logkeeper keeper recover-stream` does to a consensus stream that lost two
/// of its three voters: the stream keeps its first `head` records under a
/// recovery fence, and every seal returns `Recovered` until someone
/// acknowledges the recovery. Keeper-hosted recovery needs two replacement
/// keepers in distinct failure domains, which the local consensus cluster
/// does not have.
#[derive(Default)]
struct Recoverable {
    current: std::sync::Mutex<MemoryJournal>,
    /// The recovered head while the recovery awaits acknowledgement.
    pending: std::sync::Mutex<Option<Lsn>>,
    acknowledgements: AtomicUsize,
    /// Holds each acknowledgement until this many nodes are acknowledging.
    rendezvous: std::sync::Mutex<Option<Arc<tokio::sync::Barrier>>>,
}

const RECOVERY_IDENTITY: Identity = Identity::from_static("urn:rtdb:test:recovery");
const RECOVERY_FENCE: Epoch = Epoch(5);

impl Recoverable {
    fn handle(self: &Arc<Self>, identity: Identity) -> Arc<dyn Journal> {
        Arc::new(RecoverableHandle { shared: Arc::clone(self), identity })
    }

    fn journal(&self, identity: &Identity) -> impl Journal + use<> {
        self.current.lock().unwrap().handle(identity.clone())
    }

    /// Replaces the stream with its first `head` records under the recovery
    /// fence, as the survivor's replica would hold them.
    async fn recover(&self, stream: StreamId, head: Lsn) {
        let old = self.journal(&RECOVERY_IDENTITY);
        let survivor = MemoryJournal::default();
        let copy = survivor.handle(RECOVERY_IDENTITY);
        copy.create(stream).await.unwrap();
        copy.seal(stream, Epoch(RECOVERY_FENCE.0 - 1)).await.unwrap();
        let mut at = Lsn(1);
        while at <= head {
            let page = old.read(stream, at, 128, Duration::ZERO).await.unwrap();
            for (lsn, payload) in page.records.into_iter().take_while(|(lsn, _)| *lsn <= head) {
                copy.append(stream, Epoch(RECOVERY_FENCE.0 - 1), lsn, payload).await.unwrap();
                at = Lsn(lsn.0 + 1);
            }
        }
        survivor.handle(Identity::from_static("urn:rtdb:test:operator")).seal(stream, RECOVERY_FENCE).await.unwrap();
        *self.current.lock().unwrap() = survivor;
        *self.pending.lock().unwrap() = Some(head);
    }

    async fn acknowledge(&self, stream: StreamId) -> journal_contract::Result<Lsn> {
        let rendezvous = self.rendezvous.lock().unwrap().clone();
        if let Some(rendezvous) = rendezvous {
            rendezvous.wait().await;
        }
        self.acknowledgements.fetch_add(1, Ordering::SeqCst);
        self.pending.lock().unwrap().take();
        self.journal(&RECOVERY_IDENTITY).head(stream).await
    }
}

struct RecoverableHandle {
    shared: Arc<Recoverable>,
    identity: Identity,
}

impl Journal for RecoverableHandle {
    fn create(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<()>> {
        let journal = self.shared.journal(&self.identity);
        Box::pin(async move { journal.create(stream).await })
    }

    fn seal(&self, stream: StreamId, epoch: Epoch) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        let pending = *self.shared.pending.lock().unwrap();
        let journal = self.shared.journal(&self.identity);
        Box::pin(async move {
            match pending {
                Some(head) => Err(JournalError::Recovered { head }),
                None => journal.seal(stream, epoch).await,
            }
        })
    }

    fn append(&self, stream: StreamId, epoch: Epoch, at: Lsn, payload: Bytes) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        let journal = self.shared.journal(&self.identity);
        Box::pin(async move { journal.append(stream, epoch, at, payload).await })
    }

    fn read(&self, stream: StreamId, from: Lsn, max: usize, wait: Duration) -> BoxFuture<'_, journal_contract::Result<Page>> {
        let journal = self.shared.journal(&self.identity);
        Box::pin(async move { journal.read(stream, from, max, wait).await })
    }

    fn head(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        let journal = self.shared.journal(&self.identity);
        Box::pin(async move { journal.head(stream).await })
    }

    fn trim(&self, stream: StreamId, epoch: Epoch, through: Lsn) -> BoxFuture<'_, journal_contract::Result<()>> {
        let journal = self.shared.journal(&self.identity);
        Box::pin(async move { journal.trim(stream, epoch, through).await })
    }

    fn delete(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<()>> {
        let journal = self.shared.journal(&self.identity);
        Box::pin(async move { journal.delete(stream).await })
    }

    fn acknowledge_recovery(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        Box::pin(self.shared.acknowledge(stream))
    }
}

/// The recovery records a node's retained journal holds.
fn recovery_records(node: &MultiDatabase) -> Vec<Record> {
    node.committed.as_ref().unwrap().records.iter().filter(|record| matches!(record, Record::Recovered { .. })).cloned().collect()
}

/// A writer that committed `kept` and then two lost commits, a follower that
/// applied all three, and the journal head after `kept`.
async fn recoverable_cluster(cluster: &Cluster, journal: &Arc<Recoverable>) -> (MultiDatabase, MultiDatabase, Lsn) {
    let (writer, follower, _, kept) = recoverable_cluster_with_lagging(cluster, journal).await;
    (writer, follower, kept)
}

/// As [`recoverable_cluster`], with a follower that applied only `kept`.
async fn recoverable_cluster_with_lagging(cluster: &Cluster, journal: &Arc<Recoverable>) -> (MultiDatabase, MultiDatabase, MultiDatabase, Lsn) {
    let mut writer = cluster.node(1, journal.handle(NODE_1_IDENTITY));
    let mut follower = cluster.node(2, journal.handle(NODE_2_IDENTITY));
    let mut lagging = cluster.node(4, journal.handle(Identity::from_static("urn:rtdb:test:4")));
    writer.create().await.unwrap();
    assert_eq!(writer.tick(&live(1)).await.unwrap(), Role::Writer(Epoch(1)));
    writer.transact(&put(1, 1, "kept")).await.unwrap();
    let kept = writer.stream.head().await.unwrap();
    assert_eq!(text_at(&mut lagging, MAIN_BRANCH, 1).await, Some(Value::Utf8("kept".into())));
    writer.transact(&put(2, 2, "lost")).await.unwrap();
    let lost = writer.transact(&put(3, 3, "lost")).await.unwrap();
    cluster.at(lost.system_time.0);
    assert_eq!(text_at(&mut follower, MAIN_BRANCH, 3).await, Some(Value::Utf8("lost".into())));
    (writer, follower, lagging, kept)
}

#[tokio::test]
async fn multi_driver_acknowledges_a_journal_recovery_records_it_and_resumes_writing() {
    let cluster = Cluster::new();
    let journal = Arc::new(Recoverable::default());
    let (writer, mut follower, kept) = recoverable_cluster(&cluster, &journal).await;
    let stream = writer.journal_stream();
    drop(writer);
    journal.recover(stream, kept).await;
    let recoveries = journal_recoveries_acknowledged();

    // The follower outlives the lost writer's lease, notices the recovery when
    // it seals, acknowledges it, seals above the recovery's fence and records
    // the loss before it writes.
    cluster.at(10 + LEASE + DRIFT + 1);
    assert_eq!(follower.tick(&live(2)).await.unwrap(), Role::Writer(Epoch(RECOVERY_FENCE.0 + 1)));
    assert_eq!(journal.acknowledgements.load(Ordering::SeqCst), 1);
    assert!(journal_recoveries_acknowledged() > recoveries);
    let recovery = follower.journal_recovery().unwrap();
    assert_eq!((recovery.head, recovery.epoch, recovery.time), (kept, Epoch(RECOVERY_FENCE.0 + 1), Timestamp(10 + LEASE + DRIFT + 1)));
    assert!(recovery.at > kept);
    assert_eq!(text_at(&mut follower, MAIN_BRANCH, 1).await, Some(Value::Utf8("kept".into())));
    assert_eq!(text_at(&mut follower, MAIN_BRANCH, 2).await, None);
    follower.transact(&put(4, 2, "after recovery")).await.unwrap();
    assert_eq!(text_at(&mut follower, MAIN_BRANCH, 2).await, Some(Value::Utf8("after recovery".into())));

    // A reader opened after the recovery replays the same history.
    let mut reader = cluster.node(3, journal.handle(NODE_3_IDENTITY));
    assert_eq!(text_at(&mut reader, MAIN_BRANCH, 2).await, Some(Value::Utf8("after recovery".into())));
    assert_eq!(text_at(&mut reader, MAIN_BRANCH, 3).await, None);
    assert_eq!(reader.journal_recovery(), Some(recovery));
}

#[tokio::test]
async fn multi_driver_holds_writes_after_a_journal_recovery_when_configured() {
    let cluster = Cluster::new();
    let journal = Arc::new(Recoverable::default());
    let (writer, follower, kept) = recoverable_cluster(&cluster, &journal).await;
    let stream = writer.journal_stream();
    drop((writer, follower));
    journal.recover(stream, kept).await;

    let mut held = cluster.node(2, journal.handle(NODE_2_IDENTITY)).with_hold_writes_after_recovery(true);
    cluster.at(10 + LEASE + DRIFT + 1);
    let refused = held.tick(&live(2)).await.unwrap_err();
    assert_eq!(refused.code, "rtdb/journal-recovered");
    assert!(refused.message.as_deref().unwrap_or("").contains("acknowledges the recovery"));
    assert_eq!(held.role(), Role::Follower);
    assert_eq!(journal.acknowledgements.load(Ordering::SeqCst), 0);
    // An administrator acknowledges it; the held writer then resumes without
    // recording a recovery of its own.
    journal.acknowledge(stream).await.unwrap();
    assert_eq!(held.tick(&live(2)).await.unwrap(), Role::Follower);
    cluster.at(10 + 2 * (LEASE + DRIFT + 1));
    assert_eq!(held.tick(&live(2)).await.unwrap(), Role::Writer(Epoch(RECOVERY_FENCE.0 + 1)));
    assert_eq!(held.journal_recovery(), None);
    assert_eq!(text_at(&mut held, MAIN_BRANCH, 2).await, None);
    held.transact(&put(4, 2, "after recovery")).await.unwrap();
}

#[tokio::test]
async fn multi_followers_rebuild_their_state_across_a_journal_recovery() {
    let cluster = Cluster::new();
    let journal = Arc::new(Recoverable::default());
    let (mut writer, mut ahead, mut behind, kept) = recoverable_cluster_with_lagging(&cluster, &journal).await;
    let stream = writer.journal_stream();
    let lost = writer.stream.head().await.unwrap();
    journal.recover(stream, kept).await;

    // A new writer resumes and commits past the positions the lost commits
    // held, so the head alone no longer shows that the history changed.
    let mut successor = cluster.node(3, journal.handle(NODE_3_IDENTITY));
    cluster.at(10 + LEASE + DRIFT + 1);
    assert_eq!(successor.tick(&live(3)).await.unwrap(), Role::Writer(Epoch(RECOVERY_FENCE.0 + 1)));
    for id in 10..16 {
        let receipt = successor.transact(&put(id as u8, id, "new")).await.unwrap();
        cluster.at(receipt.system_time.0);
    }
    assert!(successor.stream.head().await.unwrap() > lost);
    let recovery = successor.journal_recovery();

    for node in [&mut ahead, &mut behind, &mut writer] {
        assert_eq!(text_at(node, MAIN_BRANCH, 1).await, Some(Value::Utf8("kept".into())));
        assert_eq!(text_at(node, MAIN_BRANCH, 2).await, None);
        assert_eq!(text_at(node, MAIN_BRANCH, 3).await, None);
        assert_eq!(text_at(node, MAIN_BRANCH, 15).await, Some(Value::Utf8("new".into())));
        assert_eq!(node.journal_recovery(), recovery);
        assert_eq!(node.role(), Role::Follower);
    }
    assert_eq!(journal.acknowledgements.load(Ordering::SeqCst), 1);
}

/// The end-to-end operator recovery on real keepers: five keepers in three
/// domains plus the catalog keeper. Voter 1 stops after the first commit, the
/// other two voters acknowledge two more and are then lost, and the operator
/// runs `recover-stream` on voter 1. The next writer acknowledges the
/// recovery and records it; the old writer and both followers rebuild; none
/// of them mixes the lost commits with the commits written after recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_resumes_after_a_real_keeper_recovery_and_followers_rebuild() {
    use stream_logkeeper::stream::consensus::testing::LocalCluster;
    const NODE_4_IDENTITY: Identity = Identity::from_static("urn:rtdb:test:4");
    let principals = [NODE_1_IDENTITY, NODE_2_IDENTITY, NODE_3_IDENTITY, NODE_4_IDENTITY].map(|identity| keeper_principal(&identity));
    let principals = principals.iter().map(String::as_str).collect::<Vec<_>>();
    let mut keepers = LocalCluster::start_spared(&principals).await.unwrap();
    let config = keepers.journal_config(8 << 30, 4 << 20);
    let handle = |keepers: &LocalCluster, identity: Identity| -> Arc<dyn Journal> {
        Arc::new(keepers.journal(&keeper_principal(&identity), &config).unwrap())
    };
    let [survivor, lost_b, lost_c] = keepers.voters();

    let cluster = Cluster::new();
    let mut writer = cluster.node(1, handle(&keepers, NODE_1_IDENTITY));
    let mut ahead = cluster.node(2, handle(&keepers, NODE_2_IDENTITY));
    let mut behind = cluster.node(4, handle(&keepers, NODE_4_IDENTITY));
    writer.create().await.unwrap();
    assert_eq!(writer.tick(&live(1)).await.unwrap(), Role::Writer(Epoch(1)));
    writer.transact(&put(1, 1, "kept")).await.unwrap();
    let kept = writer.stream.head().await.unwrap();
    assert_eq!(text_at(&mut behind, MAIN_BRANCH, 1).await, Some(Value::Utf8("kept".into())));
    let stream = writer.journal_stream();
    // Voter 1 holds every record through `kept` before it stops: a follower
    // keeper applies a committed entry within a heartbeat.
    tokio::time::sleep(Duration::from_secs(1)).await;
    keepers.stop(survivor).await.unwrap();

    // The other two voters acknowledge two more commits, then both are lost.
    writer.transact(&put(2, 2, "lost")).await.unwrap();
    let receipt = writer.transact(&put(3, 3, "lost")).await.unwrap();
    cluster.at(receipt.system_time.0);
    let lost = writer.stream.head().await.unwrap();
    assert!(lost > kept);
    assert_eq!(text_at(&mut ahead, MAIN_BRANCH, 3).await, Some(Value::Utf8("lost".into())));
    keepers.stop(lost_b).await.unwrap();
    keepers.stop(lost_c).await.unwrap();

    let report = keepers.recover_stream(survivor, stream, [lost_b, lost_c]).await.unwrap();
    assert!(report.written);
    assert_eq!(report.head, kept);
    assert!(report.voters.contains(&survivor) && !report.voters.contains(&lost_b) && !report.voters.contains(&lost_c));
    keepers.restart(survivor).await.unwrap();

    // A new writer meets the recovery, acknowledges it, seals above its fence
    // and records the loss before it commits past the lost positions.
    let recoveries = journal_recoveries_acknowledged();
    let faults = crate::journal::journal_recovery_faults();
    let mut successor = cluster.node(3, handle(&keepers, NODE_3_IDENTITY));
    cluster.at(10 + LEASE + DRIFT + 1);
    let role = successor.tick(&live(3)).await.unwrap();
    assert_eq!(role, Role::Writer(Epoch(report.epoch.0 + 1)));
    assert!(journal_recoveries_acknowledged() > recoveries);
    let recovery = successor.journal_recovery().unwrap();
    assert_eq!((recovery.head, recovery.epoch), (kept, Epoch(report.epoch.0 + 1)));
    for id in 10..16 {
        let receipt = successor.transact(&put(id as u8, id, "new")).await.unwrap();
        cluster.at(receipt.system_time.0);
    }
    assert!(successor.stream.head().await.unwrap() > lost);

    // The old writer and both followers rebuild from the journal: the kept
    // commit and the new ones, never the lost ones.
    for node in [&mut ahead, &mut behind, &mut writer, &mut successor] {
        assert_eq!(text_at(node, MAIN_BRANCH, 1).await, Some(Value::Utf8("kept".into())));
        assert_eq!(text_at(node, MAIN_BRANCH, 2).await, None);
        assert_eq!(text_at(node, MAIN_BRANCH, 3).await, None);
        assert_eq!(text_at(node, MAIN_BRANCH, 15).await, Some(Value::Utf8("new".into())));
        assert_eq!(node.journal_recovery(), Some(recovery));
    }
    for node in [&ahead, &behind, &writer] {
        assert_eq!(node.role(), Role::Follower);
    }
    let rebuilt = crate::journal::journal_recovery_faults();
    assert!(rebuilt[1].1 + rebuilt[2].1 > faults[1].1 + faults[2].1, "no follower reported the replaced history");

    // A reader opened after the recovery replays the same history.
    let mut reader = cluster.node(5, handle(&keepers, NODE_4_IDENTITY));
    assert_eq!(text_at(&mut reader, MAIN_BRANCH, 2).await, None);
    assert_eq!(text_at(&mut reader, MAIN_BRANCH, 15).await, Some(Value::Utf8("new".into())));
    assert_eq!(reader.journal_recovery(), Some(recovery));
    keepers.shutdown().await.unwrap();
}

#[tokio::test]
async fn multi_writers_racing_to_acknowledge_a_journal_recovery_record_it_once() {
    let cluster = Cluster::new();
    let journal = Arc::new(Recoverable::default());
    let (writer, first, kept) = recoverable_cluster(&cluster, &journal).await;
    let stream = writer.journal_stream();
    drop(writer);
    let mut first = first;
    let mut second = cluster.node(3, journal.handle(NODE_3_IDENTITY));
    text_at(&mut second, MAIN_BRANCH, 1).await;
    journal.recover(stream, kept).await;

    // Both nodes take themselves for the preferred writer and see the
    // recovery before either acknowledges it.
    *journal.rendezvous.lock().unwrap() = Some(Arc::new(tokio::sync::Barrier::new(2)));
    cluster.at(10 + LEASE + DRIFT + 1);
    let (two, three) = (live(2), live(3));
    let (a, b) = tokio::join!(first.tick(&two), second.tick(&three));
    *journal.rendezvous.lock().unwrap() = None;
    assert_eq!(journal.acknowledgements.load(Ordering::SeqCst), 2);
    let roles = [a.unwrap(), b.unwrap()];
    assert_eq!(roles.iter().filter(|role| matches!(role, Role::Writer(_))).count(), 1, "{roles:?}");
    let (mut winner, mut loser) = if matches!(roles[0], Role::Writer(_)) { (first, second) } else { (second, first) };
    winner.transact(&put(4, 4, "winner")).await.unwrap();
    assert_eq!(recovery_records(&winner).len(), 1);

    // The loser takes over once the winner releases; the journal already
    // records the recovery, so it does not record it again.
    let successor = BTreeSet::from([loser.options.node]);
    assert_eq!(winner.tick(&successor).await.unwrap(), Role::Follower);
    cluster.at(10 + 2 * (LEASE + DRIFT + 1));
    assert!(matches!(loser.tick(&successor).await.unwrap(), Role::Writer(_)));
    loser.transact(&put(5, 5, "loser")).await.unwrap();
    assert_eq!(recovery_records(&loser).len(), 1);
    assert_eq!(loser.journal_recovery().unwrap().head, kept);
    assert_eq!(text_at(&mut winner, MAIN_BRANCH, 5).await, Some(Value::Utf8("loser".into())));
    assert_eq!(recovery_records(&winner).len(), 1);
}

#[tokio::test]
async fn multi_driver_replacement_refuses_a_timed_tx_after_its_reply_is_lost() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let faults = Arc::new(FaultJournal::new(Arc::new(journal.handle(NODE_1_IDENTITY))));
    let gate = Arc::new(GateJournal::new(faults.clone()));
    let mut writer = cluster.node(1, gate.clone());
    let mut successor = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
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
    let journal = journals().await;
    let cluster = Cluster::new();
    let first = Arc::new(FaultJournal::new(Arc::new(journal.handle(NODE_1_IDENTITY))));
    let mut left = cluster.node(1, first.clone());
    let mut right = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
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
async fn multi_driver_hands_off_a_settled_writer_after_a_lost_release_reply() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let faults = Arc::new(FaultJournal::new(Arc::new(journal.handle(NODE_1_IDENTITY))));
    let mut first = cluster.node(1, faults.clone());
    let mut successor = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    first.create().await.unwrap();
    first.tick(&live(1)).await.unwrap();
    let prior = first.transact(&put(1, 1, "released writer")).await.unwrap();
    assert_eq!(successor.tick(&live(2)).await.unwrap(), Role::Follower);

    faults.inject(Fault::DropAppendReply);
    assert_eq!(first.tick(&live(2)).await.unwrap_err().category, AnomalyCategory::Interrupted);
    assert_eq!(successor.tick(&live(2)).await.unwrap(), Role::Writer(Epoch(2)));
    assert_eq!(successor.tx_status(&prior.key).await.unwrap().basis, prior.basis);
    assert_eq!(first.transact(&put(2, 2, "no prior authority")).await.unwrap_err().code, "rtdb/no-writer");
}

#[tokio::test]
async fn multi_driver_shutdown_releases_the_writer_and_closes_the_pipeline() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    writer.create().await.unwrap();
    assert_eq!(writer.tick(&live(1)).await.unwrap(), Role::Writer(Epoch(1)));
    let writes = WritePipeline::start(Arc::new(Mutex::new(writer)));
    let before_shutdown = writes.transact(put(1, 1, "before shutdown")).await.unwrap();

    let mut successor = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    assert_eq!(successor.tick(&live(2)).await.unwrap(), Role::Follower);
    writes.shutdown().await.unwrap();
    assert_eq!(
        writes.tick(live(1)).await.unwrap_err().code,
        "rtdb/write-pipeline-closed"
    );
    assert_eq!(successor.tick(&live(2)).await.unwrap(), Role::Writer(Epoch(2)));
    assert_eq!(
        successor.tx_status(&before_shutdown.key).await.unwrap().basis,
        before_shutdown.basis
    );
}

#[tokio::test]
async fn deployment_retirement_closes_admission_after_a_lost_reply() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let faults = Arc::new(FaultJournal::new(Arc::new(journal.handle(NODE_1_IDENTITY))));
    let mut writer = cluster.node(1, faults.clone());
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    writer.transact(&put(1, 1, "before retirement")).await.unwrap();
    faults.inject(Fault::DropAppendReply);
    assert_eq!(writer.retire_deployment().await.unwrap_err().category, AnomalyCategory::Interrupted);

    let mut follower = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    let terminal = follower.deployment_retirement().await.unwrap().unwrap();
    assert_eq!(terminal.incarnation, DATABASE);
    assert_eq!(terminal.generation, 1);
    assert_eq!(follower.retire_deployment().await.unwrap(), terminal);
    assert_eq!(follower.tick(&live(2)).await.unwrap(), Role::Follower);
    assert_eq!(follower.fresh_view().await.unwrap_err().code, "rtdb/deployment-retired");
    assert_eq!(writer.transact(&put(2, 2, "after retirement")).await.unwrap_err().code, "rtdb/deployment-retired");
}

#[tokio::test]
async fn multi_driver_abandoned_seal_allows_one_replacement_after_its_grace_period() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let abandoned = journal.handle(NODE_1_IDENTITY);
    let mut successor = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
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
    assert_eq!(page.head, Lsn(1));
    assert!(!page.records[0].1.starts_with(b"RTDBR"));
}

#[tokio::test]
async fn multi_driver_storage_reclaims_only_unreachable_epochs_and_keeps_old_branch_bases() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut first = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    let mut second = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
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
    let journal = journals().await;
    let cluster = Cluster::new();
    let faults = Arc::new(FaultJournal::new(Arc::new(journal.handle(NODE_1_IDENTITY))));
    let mut first = cluster.node(1, faults.clone());
    let mut successor = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
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
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    let mut reader = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
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
async fn failed_journal_read_keeps_the_replayed_prefix() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    let faults = Arc::new(FaultJournal::new(Arc::new(journal.handle(NODE_2_IDENTITY))));
    let mut reader = cluster.node(2, faults.clone());
    writer.create().await.unwrap();
    assert_eq!(writer.tick(&live(1)).await.unwrap(), Role::Writer(Epoch(1)));
    writer.transact(&put(1, 1, "first")).await.unwrap();
    reader.refresh().await.unwrap();
    let replayed = reader.state().head;
    cluster.at(11);
    writer.transact(&put(2, 1, "second")).await.unwrap();
    let end = reader.stream.head().await.unwrap();
    assert!(end > replayed);

    faults.inject(Fault::Unavailable);
    assert_eq!(reader.catch_up(end).await.unwrap_err().code, "rtdb/journal-unavailable");
    assert_eq!(reader.committed.as_ref().map(|state| state.head), Some(replayed));

    reader.catch_up(end).await.unwrap();
    assert_eq!(reader.state().head, end);
    assert_eq!(text_at(&mut reader, MAIN_BRANCH, 1).await, Some(Value::Utf8("second".into())));
}

#[tokio::test]
async fn multi_driver_late_upload_cannot_reenter_reachability() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut first = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    let mut second = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
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

    fn acknowledge_recovery(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        self.inner.acknowledge_recovery(stream)
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
    let journal = journals().await;
    let cluster = Cluster::new();
    let faults = Arc::new(FaultJournal::new(Arc::new(journal.handle(NODE_1_IDENTITY))));
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
    let observer = journal.handle(NODE_2_IDENTITY);
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
            let mut successor = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
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
async fn managed_workload_expired_while_waiting_for_the_original_acknowledgement() {
    use rtdb_tx::transact::workload::{Applied, Authority, Family, Fence, Holder, Operation, Placement, Point, Previous, Reply, Request};

    let journal = journals().await;
    let cluster = Cluster::new();
    let gate = Arc::new(GateJournal::new(Arc::new(journal.handle(NODE_1_IDENTITY))));
    let mut writer = cluster.node(1, gate.clone());
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    let database = Arc::new(Mutex::new(writer));
    let writes = WritePipeline::start(database.clone());

    let point = Point::new("public.installation_authority", "authority".into(), Timestamp(0)).unwrap();
    let command = |namespace, expires_at, operation| Request { point: point.clone(), namespace, expires_at, operation };
    let submit = |key, authority, transaction| Operation::Submit {
        key: vec![key; 16], authority, transaction, digest: [key; 32],
    };
    let applied = |reply: Reply, namespace| {
        let Reply::Receipt(receipt) = reply else { panic!("expected receipt") };
        assert_eq!(receipt.outcome, Outcome::Then);
        Applied { namespace, key: receipt.key, basis: receipt.basis.to_bytes(), system_time_micros: receipt.system_time.0 }
    };
    let placement = Placement {
        epoch: 1, assignment: Some(Holder { assignment_id: [7; 16], holder: "conductor".into() }),
    };
    let advance = Authority::AdvancePlacement {
        installation_intent: [6; 16], previous: Previous::Empty, next: placement.clone(),
    };
    let fence = Fence { placement, applied: applied(
        writes.workload(command([1; 32], Timestamp(80), submit(1, advance, None))).await.unwrap(), [1; 32],
    ) };
    let claim = Authority::ClaimWriter { fence: fence.clone() };
    let writer = applied(
        writes.workload(command([2; 32], Timestamp(80), submit(2, claim, None))).await.unwrap(), [2; 32],
    );

    let write = Authority::Write { fence, writer };
    gate.arm.store(true, Ordering::SeqCst);
    let pending = writes.workload(command([3; 32], Timestamp(25), submit(3, write, Some(put(3, 1, "expired")))));
    tokio::pin!(pending);
    tokio::select! {
        _ = &mut pending => panic!("the workload returned before the original acknowledgement"),
        reached = tokio::time::timeout(Duration::from_secs(2), gate.entered.notified()) => reached.unwrap(),
    }
    cluster.at(30);
    gate.release.notify_one();
    let Reply::Receipt(refused) = pending.await.unwrap() else { panic!("expected refusal") };
    assert!(matches!(refused.outcome, Outcome::Refused { .. }));
    let key = vec![3; 16];
    let status = command([3; 32], Timestamp(80), Operation::Status { key: key.clone(), family: Family::Write });
    let Reply::Receipt(recovered) = writes.workload(status).await.unwrap() else { panic!("expected retained refusal") };
    assert_eq!(recovered, Receipt { replayed: true, ..refused });
    let mut database = database.lock().await;
    assert_eq!(database.tx_status(&key).await.unwrap_err().code, "rtdb/unknown-transaction");
    assert_eq!(text_at(&mut database, MAIN_BRANCH, 1).await, None);
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
    let journal = journals().await;
    let cluster = Cluster::new();
    let gate = Arc::new(GateJournal::new(Arc::new(journal.handle(NODE_1_IDENTITY))));
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
            .handle(NODE_2_IDENTITY)
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
    let journal = journals().await;
    let cluster = Cluster::new();
    let journal: Arc<dyn Journal> = Arc::new(journal.handle(NODE_1_IDENTITY));
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
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut database = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
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

#[tokio::test]
async fn interrupted_erase_recovers_the_retirement_proof_and_atomic_floor() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let faults = Arc::new(FaultJournal::new(Arc::new(journal.handle(NODE_1_IDENTITY))));
    let mut writer = cluster.node(1, faults.clone());
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    let original = writer.transact(&put(1, 1, "old row")).await.unwrap();

    let mut erase = put(2, 2, "retained floor");
    erase.ops.insert(0, Op::Erase { branch: None, table: table(), ids: vec![EntityId::Integer(1)] });
    let pin = Uuid::from_u128(42);
    writer.pin_checkpoint_at(pin, original.basis, None).await.unwrap();
    assert_eq!(writer.transact(&erase).await.unwrap_err().code, "rtdb/key-rotation-pending");
    writer.release_pin(pin).await.unwrap();

    faults.inject(Fault::DropAppendReply);
    assert_eq!(writer.transact(&erase).await.unwrap_err().category, AnomalyCategory::Interrupted);
    assert_eq!(writer.tx_status(&erase.key).await.unwrap_err().code, "rtdb/erasure-pending");
    writer.tick(&live(1)).await.unwrap();
    let receipt = writer.tx_status(&erase.key).await.unwrap();
    assert_eq!(receipt.erasure, Some(Erasure::Armed { unreadable_by: Timestamp(120) }));
    assert_eq!(writer.transact(&erase).await.unwrap(), receipt);

    cluster.at(120);
    assert_eq!(writer.check_generation(1).await.unwrap_err().code, "rtdb/erased-under-view");
    let mut successor = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    successor.tick(&live(2)).await.unwrap();
    assert_eq!(successor.tx_status(&erase.key).await.unwrap(), receipt);
    assert_eq!(text_at(&mut successor, MAIN_BRANCH, 1).await, None);
    assert_eq!(text_at(&mut successor, MAIN_BRANCH, 2).await, Some(Value::Utf8("retained floor".into())));
    successor.checkpoint(Vec::new()).await.unwrap();
    let mut follower = cluster.node(3, Arc::new(journal.handle(NODE_3_IDENTITY)));
    assert_eq!(follower.tx_status(&erase.key).await.unwrap(), receipt);
}

#[tokio::test]
async fn multi_driver_recovers_rotation_after_key_retirement_overtakes_trim() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let gate = Arc::new(GateJournal::new(Arc::new(journal.handle(NODE_1_IDENTITY))));
    let mut writer = cluster.node(1, gate.clone());
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

    cluster.at(120);
    assert_eq!(
        read_file(cluster.store.as_ref(), cluster.kms.as_ref(), old)
            .await
            .err()
            .unwrap()
            .code,
        "rtdb/erased-under-view"
    );
    cluster.at(121);
    let mut successor = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
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
        Timestamp(120)
    );
}

#[tokio::test]
async fn multi_driver_releases_a_fork_pin_after_independent_destination_publication() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut source = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
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
        Arc::new(journal.handle(NODE_2_IDENTITY)),
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

#[tokio::test]
async fn checked_workload_custody_retains_its_namespaced_receipt_across_checkpoint() {
    use rtdb_tx::transact::{CustodyPlacement, workload::{Applied, Authority, Family, Fence, Holder, Operation, Placement, Point, Previous, Reply, Request}};

    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    let database = Arc::new(Mutex::new(writer));
    let writes = WritePipeline::start(database.clone());

    let point = Point::new("public.installation_authority", "authority".into(), Timestamp(0)).unwrap();
    let command = |namespace, operation| Request { point: point.clone(), namespace, expires_at: Timestamp(200), operation };
    let submit = |key, authority, transaction| Operation::Submit {
        key: vec![key; 16], authority, transaction, digest: [key; 32],
    };
    let receipt = |answer| {
        let Reply::Receipt(receipt) = answer else { panic!("expected receipt") };
        assert_eq!(receipt.outcome, Outcome::Then);
        receipt
    };
    let applied = |receipt: Receipt, namespace| Applied {
        namespace, key: receipt.key, basis: receipt.basis.to_bytes(), system_time_micros: receipt.system_time.0,
    };
    let placement = Placement { epoch: 1, assignment: Some(Holder { assignment_id: [7; 16], holder: "conductor".into() }) };
    let advance = Authority::AdvancePlacement { installation_intent: [6; 16], previous: Previous::Empty, next: placement.clone() };
    let placed = receipt(writes.workload(command([1; 32], submit(1, advance, None))).await.unwrap());
    let fence = Fence { placement, applied: applied(placed, [1; 32]) };
    let claim = Authority::ClaimWriter { fence: fence.clone() };
    let original = receipt(writes.workload(command([2; 32], submit(2, claim, None))).await.unwrap());
    let writer_key = original.key.clone();
    let writer_basis = original.basis;
    let writer = applied(original, [2; 32]);
    let group = [4; 16];
    let mut transaction = request(3, Op::CreateCustodyGroup { group });
    transaction.ops.push(Op::PlaceCustodyRow {
        branch: MAIN_BRANCH.into(), table: table(),
        row: Row {
            id: EntityId::Integer(1), valid: Some(rtdb_types::Interval::ALL.into()),
            cells: StructFields::from([("carrier".into(), Cell::Value(Value::Binary(vec![5])))]),
            custody_group: Some(group), protected_columns: vec!["carrier".into()], custody_payload: None,
        },
        placement: CustodyPlacement {
            placement_id: [6; 16], reservation: None, derived: None, carrier_column: "carrier".into(),
            link_column: None, commitments: None,
        },
    });
    let ordinary = Authority::Write { fence: fence.clone(), writer: writer.clone() };
    assert_eq!(writes.workload(command([3; 32], submit(3, ordinary, Some(transaction.clone()))))
        .await.err().unwrap().code, "rtdb/managed-workload-authority");
    let custody = Authority::CustodyWrite { fence: fence.clone(), writer: writer.clone(), request_digest: [3; 32] };
    let retained = receipt(writes.workload(command([3; 32], submit(3, custody, Some(transaction)))).await.unwrap());
    let overwrite = submit(4, Authority::Write { fence, writer }, Some(put(4, 1, "unprotected overwrite")));
    assert_eq!(writes.workload(command([3; 32], overwrite)).await.err().unwrap().code, "rtdb/managed-workload-authority");

    database.lock().await.checkpoint(Vec::new()).await.unwrap();
    drop(writes);
    cluster.at(116);
    let mut successor = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    assert!(matches!(successor.tick(&live(2)).await.unwrap(), Role::Writer(_)));
    let recovered = successor.original_custody_receipt(&retained.key, retained.basis).await.unwrap();
    assert_eq!(recovered, Receipt { replayed: true, ..retained.clone() });
    assert!(successor.original_custody_receipt(&writer_key, writer_basis).await.is_err());
    assert_eq!(successor.original_writer_receipt(&writer_key, writer_basis).await.unwrap().basis, writer_basis);
    assert!(successor.original_writer_receipt(&writer_key, retained.basis).await.is_err());
    let writes = WritePipeline::start(Arc::new(Mutex::new(successor)));
    let status = |family| Operation::Status { key: retained.key.clone(), family };
    assert_eq!(writes.workload(command([3; 32], status(Family::Write))).await.err().unwrap().code, "rtdb/unknown-transaction");
    let Reply::Receipt(scoped) = writes.workload(command([3; 32], status(Family::CustodyWrite))).await.unwrap()
        else { panic!("expected recovered custody receipt") };
    assert_eq!(scoped, recovered);
}

#[tokio::test]
async fn classic_candidate_status_recovers_a_lost_checkpoint_append_after_reopen() {
    use rtdb_format::data::{StoredEvent, StoredOperation};
    use rtdb_index::branches::BranchCut;
    use rtdb_types::{EventPosition, Interval, TransactionKey, TxId};

    let journal = journals().await;
    let cluster = Cluster::new();
    let faults = Arc::new(FaultJournal::new(Arc::new(journal.handle(NODE_1_IDENTITY))));
    let mut writer = cluster.node(1, faults.clone());
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    let first = TransactionKey { tx_id: TxId(123), system_time: Timestamp(5) };
    let terminal = TransactionKey { tx_id: TxId(124), system_time: Timestamp(7) };
    let cut = |tx: TransactionKey| BranchCut {
        basis: classic_import_basis(writer.incarnation(), tx.tx_id).unwrap(), system_time: tx.system_time,
    };
    let (first_cut, terminal_cut) = (cut(first), cut(terminal));
    let mut index = BranchIndex::new(writer.incarnation());
    let id = EntityId::Integer(1);
    let valid = Interval::new(Timestamp(40), Timestamp(48)).unwrap();
    index.advance_classic(first_cut).unwrap();
    index.apply_classic_event(&table(), StoredEvent {
        position: Some(EventPosition { transaction: first.tx_id, ordinal: 0 }),
        origin: None, entity_key: None, iid: id.iid(), system_from: first.system_time,
        commit_position: 0, valid_time: valid,
        operation: StoredOperation::Put(StructFields::from([("_id".into(), id.as_value())])), recency: None,
    }, Some(first.tx_id)).unwrap();
    index.advance_classic(terminal_cut).unwrap();
    let mut folded = TransactState::new(writer.incarnation());
    folded.replace_applied_index(index).unwrap();

    let projection = [83; 32];
    faults.inject(Fault::DropAppendReply);
    assert_eq!(writer.install_classic_checkpoint(projection, &folded).await.unwrap_err().category, AnomalyCategory::Interrupted);
    drop(writer);

    let mut reopened = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    let proof = reopened.classic_checkpoint_status(projection).await.unwrap().unwrap();
    assert_eq!(proof.basis, terminal_cut.basis);
    assert_eq!(proof.system_time, terminal_cut.system_time);
    assert_eq!(reopened.install_classic_checkpoint(projection, &folded).await.unwrap(), proof);
    assert!(reopened.classic_checkpoint_status([84; 32]).await.is_err());
    assert_eq!(reopened.classic_checkpoint_cut(projection, first).await.unwrap(), first_cut);
    assert!(reopened.classic_checkpoint_cut(projection, TransactionKey { system_time: Timestamp(6), ..first }).await.is_err());
    let (index, _) = reopened.fresh_view().await.unwrap();
    let view = index.view(MAIN_BRANCH, At::Exactly(first_cut.basis), Layer::Merged, first_cut.system_time).unwrap();
    assert_eq!(view.row(&table(), &id, Timestamp(44)).unwrap().valid, valid);
    assert!(view.row(&table(), &id, Timestamp(48)).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn immutable_history_moves_the_rtdb_writer_without_changing_committed_receipts() {
    use stream_logkeeper::stream::consensus::{CLUSTER_VERSION_CURRENT, testing::LocalCluster};

    const PRINCIPAL: &str = "urn:logkeeper:client:immutable-rtdb";
    let mut keepers = LocalCluster::start_replicated_catalog(&[PRINCIPAL]).await.unwrap();
    let config = keepers.journal_config(8 << 30, 4 << 20);
    let journal = Arc::new(keepers.journal(PRINCIPAL, &config).unwrap());
    assert_eq!(journal.client().finalize_version(CLUSTER_VERSION_CURRENT).await.unwrap(),
        CLUSTER_VERSION_CURRENT);
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, journal.clone()).with_immutable_journal();
    writer.create().await.unwrap();
    assert_eq!(writer.tick(&live(1)).await.unwrap(), Role::Writer(Epoch(1)));
    let before = put(1, 1, "first writer");
    let receipt = writer.transact(&before).await.unwrap();

    let mut successor = cluster.node(2, journal.clone()).with_immutable_journal();
    assert_eq!(successor.tick(&live(2)).await.unwrap(), Role::Follower);
    assert_eq!(successor.tx_status(&before.key).await.unwrap().basis, receipt.basis);
    cluster.at(125);
    assert_eq!(successor.tick(&live(2)).await.unwrap(), Role::Writer(Epoch(2)));
    assert!(writer.transact(&put(2, 2, "expired writer")).await.is_err());
    let after = put(3, 3, "second writer");
    let after_receipt = successor.transact(&after).await.unwrap();

    let mut reopened = cluster.node(3, journal).with_immutable_journal();
    assert_eq!(reopened.tick(&live(2)).await.unwrap(), Role::Follower);
    assert_eq!(reopened.tx_status(&before.key).await.unwrap().basis, receipt.basis);
    assert_eq!(reopened.tx_status(&after.key).await.unwrap().basis, after_receipt.basis);

    keepers.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_driver_resumes_after_every_keeper_restarts() {
    let mut journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    writer.create().await.unwrap();
    assert_eq!(writer.tick(&live(1)).await.unwrap(), Role::Writer(Epoch(1)));
    let (first_request, second_request) = (put(1, 1, "before restart"), put(2, 2, "second write"));
    let first = writer.transact(&first_request).await.unwrap();
    let second = writer.transact(&second_request).await.unwrap();

    // Reads run at the test clock, so move it past both system times.
    cluster.at(50);
    // Every keeper stops, keeps its media, and starts again.
    journal.restart_keepers().await;
    drop(writer);

    let mut follower = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    assert_eq!(follower.tick(&live(1)).await.unwrap(), Role::Follower);
    assert_eq!(follower.tx_status(&first_request.key).await.unwrap().basis, first.basis);
    assert_eq!(text_at(&mut follower, MAIN_BRANCH, 1).await, Some(Value::Utf8("before restart".into())));
    assert_eq!(text_at(&mut follower, MAIN_BRANCH, 2).await, Some(Value::Utf8("second write".into())));

    // A reopened node waits for the earlier lease to lapse, then takes over
    // at a later epoch.
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    let mut role = writer.tick(&live(1)).await.unwrap();
    for now in [120, 130, 140, 150] {
        if matches!(role, Role::Writer(_)) {
            break;
        }
        cluster.at(now);
        role = writer.tick(&live(1)).await.unwrap();
    }
    assert!(matches!(role, Role::Writer(epoch) if epoch > Epoch(1)), "{role:?}");
    assert_eq!(writer.tx_status(&second_request.key).await.unwrap().basis, second.basis);
    let third = writer.transact(&put(3, 3, "after restart")).await.unwrap();
    assert!(third.basis.position() > second.basis.position());
    cluster.at(160);
    follower.fresh_index().await.unwrap();
    assert_eq!(text_at(&mut follower, MAIN_BRANCH, 3).await, Some(Value::Utf8("after restart".into())));
}

#[tokio::test]
async fn multi_driver_checkpoints_once_the_retained_journal_suffix_reaches_its_bound() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    writer.create().await.unwrap();
    assert_eq!(writer.tick(&live(1)).await.unwrap(), Role::Writer(Epoch(1)));
    for id in 1..=4 {
        writer.transact(&put(id as u8, id, "bounded")).await.unwrap();
    }
    let floor = writer.retained_floor().unwrap();
    // Below the bound nothing is checkpointed.
    assert_eq!(writer.checkpoint_retained_suffix(1_000).await.unwrap(), None);
    assert_eq!(writer.retained_floor(), Some(floor));
    // At the bound the writer checkpoints and trims its journal.
    assert!(writer.checkpoint_retained_suffix(4).await.unwrap().is_some());
    assert!(writer.retained_floor().unwrap() > floor);
    // A follower never checkpoints.
    let mut follower = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    assert_eq!(follower.tick(&live(1)).await.unwrap(), Role::Follower);
    assert_eq!(follower.checkpoint_retained_suffix(1).await.unwrap(), None);
    cluster.at(50);
    assert_eq!(text_at(&mut follower, MAIN_BRANCH, 4).await, Some(Value::Utf8("bounded".into())));
}

/// A Multi journal trace for `verification/multi/check_journal_trace.lean`,
/// written to `RTDB_JOURNAL_TRACE_DIR` when that is set.
struct JournalTrace {
    name: &'static str,
    events: Vec<String>,
    traced: Lsn,
    captured: BTreeSet<u64>,
    floor: Lsn,
}

impl JournalTrace {
    fn new(name: &'static str) -> Self {
        Self { name, events: Vec::new(), traced: Lsn(0), captured: BTreeSet::new(), floor: Lsn(1) }
    }

    /// Records every record the writer applied since the last call, then its trim.
    fn sync(&mut self, writer: &MultiDatabase) {
        let state = writer.state();
        for lsn in self.traced.0 + 1..=state.head.0 {
            let record = &state.records[(lsn - state.floor.0) as usize];
            let event = match record {
                Record::Checkpoint(checkpoint) => {
                    if self.captured.insert(checkpoint.through) {
                        self.events.push(format!("capture {}", checkpoint.through));
                    }
                    format!("announce {lsn} {}", checkpoint.through)
                }
                Record::Tx { .. } => format!("commit {lsn}"),
                _ => format!("control {lsn}"),
            };
            self.events.push(event);
        }
        self.traced = state.head;
        if state.floor != self.floor {
            self.floor = state.floor;
            self.events.push(format!("trim {}", state.floor.0 - 1));
        }
    }

    fn capture(&mut self, writer: &MultiDatabase, cut: Lsn) {
        self.sync(writer);
        assert_eq!(cut, self.traced);
        self.captured.insert(cut.0);
        self.events.push(format!("capture {}", cut.0));
    }

    fn replay(&mut self, writer: &MultiDatabase, fresh: &MultiDatabase) {
        self.sync(writer);
        let state = fresh.state();
        let base = state.base.as_ref().map_or(0, |base| base.announced.0);
        self.events.push(format!("replay {} {base} {}", state.floor.0, state.head.0));
    }

    fn write(self) {
        if let Some(directory) = std::env::var_os("RTDB_JOURNAL_TRACE_DIR") {
            let path = std::path::Path::new(&directory).join(format!("{}.trace", self.name));
            std::fs::write(path, self.events.join("\n") + "\n").unwrap();
        }
    }
}

/// The replayed state a writer keeps must equal what a fresh process replays.
fn assert_same_replay(kept: &MultiDatabase, fresh: &MultiDatabase) {
    let (kept, fresh) = (kept.state(), fresh.state());
    assert_eq!((kept.floor, kept.head, kept.epoch, kept.generation), (fresh.floor, fresh.head, fresh.epoch, fresh.generation));
    assert_eq!((kept.lease, kept.released, kept.records.len()), (fresh.lease, fresh.released, fresh.records.len()));
    assert_eq!(kept.base.as_ref().map(|base| (base.announced, base.checkpoint.clone())),
        fresh.base.as_ref().map(|base| (base.announced, base.checkpoint.clone())));
    assert_eq!(kept.effects.len(), fresh.effects.len());
    assert_eq!(kept.transactions.encode_checkpoint().unwrap(), fresh.transactions.encode_checkpoint().unwrap());
}

async fn first_record(node: &MultiDatabase, at: Lsn) -> Record {
    let page = node.stream.read(at).await.unwrap();
    let (lsn, bytes) = &page.records[0];
    assert_eq!(*lsn, at);
    node.stream.decode(*lsn, bytes).await.unwrap().record
}

async fn writer_with_deferred_checkpoints(cluster: &Cluster, journal: Arc<dyn Journal>) -> MultiDatabase {
    let mut writer = cluster.node(1, journal).with_deferred_checkpoints(true);
    writer.create().await.unwrap();
    assert_eq!(writer.tick(&live(1)).await.unwrap(), Role::Writer(Epoch(1)));
    writer
}

async fn commit_range(writer: &mut MultiDatabase, ids: std::ops::RangeInclusive<i64>, text: &str) {
    for id in ids {
        writer.transact(&put(id as u8, id, text)).await.unwrap();
    }
}

async fn assert_texts(node: &mut MultiDatabase, expected: &[(i64, &str)]) {
    for (id, text) in expected {
        assert_eq!(text_at(node, MAIN_BRANCH, *id).await, Some(Value::Utf8((*text).into())), "row {id}");
    }
}

#[tokio::test]
async fn deferred_checkpoint_keeps_the_commits_between_its_cut_and_its_announcement() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = writer_with_deferred_checkpoints(&cluster, Arc::new(journal.handle(NODE_1_IDENTITY))).await;
    let mut trace = JournalTrace::new("deferred-checkpoint");
    commit_range(&mut writer, 1..=3, "before the cut").await;
    // A follower that replays from LSN 1 and keeps its prefix across the trim.
    let mut early = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    assert_eq!(early.tick(&live(1)).await.unwrap(), Role::Follower);
    assert_eq!(early.retained_floor(), Some(Lsn(1)));

    let capture = writer.capture_checkpoint(1).await.unwrap().unwrap();
    let cut = capture.through();
    trace.capture(&writer, cut);
    commit_range(&mut writer, 4..=6, "while building").await;
    let prepared = writer.checkpoint_builder().build(capture).await.unwrap();
    commit_range(&mut writer, 7..=8, "after the upload").await;
    let manifest = writer.announce_checkpoint(prepared).await.unwrap().manifest().unwrap();

    // The floor is the first record after the cut, an ordinary commit. The
    // checkpoint record that announces the base comes after the retained
    // commits.
    let floor = Lsn(cut.0 + 1);
    assert_eq!(writer.retained_floor(), Some(floor));
    assert!(matches!(first_record(&writer, floor).await, Record::Tx { .. }));
    let announced = writer.state().head;
    assert!(matches!(first_record(&writer, announced).await,
        Record::Checkpoint(Checkpoint { manifest: id, through, .. }) if id == manifest && through == cut.0));
    assert!(announced.0 > floor.0 + 4);

    cluster.at(50);
    let expected = [(1, "before the cut"), (3, "before the cut"), (4, "while building"), (6, "while building"),
        (7, "after the upload"), (8, "after the upload")];
    let mut fresh = cluster.node(3, Arc::new(journal.handle(NODE_3_IDENTITY)));
    assert_eq!(fresh.tick(&live(1)).await.unwrap(), Role::Follower);
    assert_eq!(fresh.retained_floor(), Some(floor));
    assert_texts(&mut fresh, &expected).await;
    assert_same_replay(&writer, &fresh);
    trace.replay(&writer, &fresh);
    assert_texts(&mut early, &expected).await;
    assert_eq!(early.retained_floor(), Some(floor));

    // Later commits replay over the same base.
    commit_range(&mut writer, 9..=9, "after the announcement").await;
    cluster.at(60);
    assert_texts(&mut fresh, &[(9, "after the announcement")]).await;
    let mut late = cluster.node(3, Arc::new(journal.handle(NODE_3_IDENTITY)));
    assert_eq!(late.tick(&live(1)).await.unwrap(), Role::Follower);
    assert_texts(&mut late, &[(2, "before the cut"), (5, "while building"), (9, "after the announcement")]).await;
    assert_same_replay(&writer, &late);
    trace.replay(&writer, &late);
    trace.write();
}

#[tokio::test]
async fn replay_installs_a_classic_floor_then_two_deferred_floors_in_a_row() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = writer_with_deferred_checkpoints(&cluster, Arc::new(journal.handle(NODE_1_IDENTITY))).await;
    let mut trace = JournalTrace::new("classic-then-two-deferred");
    commit_range(&mut writer, 1..=2, "first").await;
    // A classic checkpoint: the floor names the checkpoint itself.
    trace.sync(&writer);
    writer.checkpoint(Vec::new()).await.unwrap();
    let classic = writer.retained_floor().unwrap();
    assert!(matches!(first_record(&writer, classic).await, Record::Checkpoint(_)));
    let mut fresh = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    assert_eq!(fresh.tick(&live(1)).await.unwrap(), Role::Follower);
    assert_same_replay(&writer, &fresh);
    trace.replay(&writer, &fresh);

    commit_range(&mut writer, 3..=4, "second").await;
    let first = writer.capture_checkpoint(1).await.unwrap().unwrap();
    let first_cut = first.through();
    trace.capture(&writer, first_cut);
    commit_range(&mut writer, 5..=6, "between the first cut and its announcement").await;
    let first = writer.checkpoint_builder().build(first).await.unwrap();
    writer.announce_checkpoint(first).await.unwrap().manifest().unwrap();
    assert_eq!(writer.retained_floor(), Some(Lsn(first_cut.0 + 1)));

    // The second cut follows the first announcement.
    commit_range(&mut writer, 7..=8, "third").await;
    trace.sync(&writer);
    let second = writer.capture_checkpoint(1).await.unwrap().unwrap();
    let second_cut = second.through();
    trace.capture(&writer, second_cut);
    assert!(second_cut > writer.state().base.as_ref().unwrap().announced);
    commit_range(&mut writer, 9..=10, "between the second cut and its announcement").await;
    let second = writer.checkpoint_builder().build(second).await.unwrap();
    writer.announce_checkpoint(second).await.unwrap().manifest().unwrap();
    let floor = Lsn(second_cut.0 + 1);
    assert_eq!(writer.retained_floor(), Some(floor));
    // Every retained record is applied once: from the floor through the head.
    assert_eq!(writer.state().records.len() as u64, writer.state().head.0 + 1 - floor.0);
    assert_eq!(writer.state().base.as_ref().unwrap().checkpoint.through, second_cut.0);

    cluster.at(50);
    let mut fresh = cluster.node(3, Arc::new(journal.handle(NODE_3_IDENTITY)));
    assert_eq!(fresh.tick(&live(1)).await.unwrap(), Role::Follower);
    assert_same_replay(&writer, &fresh);
    trace.replay(&writer, &fresh);
    trace.write();
    let mut expected = vec![(1, "first"), (2, "first"), (3, "second"), (4, "second"), (7, "third"), (8, "third")];
    expected.extend([(5, "between the first cut and its announcement"), (6, "between the first cut and its announcement")]);
    expected.extend([(9, "between the second cut and its announcement"), (10, "between the second cut and its announcement")]);
    assert_texts(&mut fresh, &expected).await;
    // The follower that installed the classic floor follows both trims.
    assert_texts(&mut cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY))), &expected).await;
}

#[tokio::test]
async fn deferred_announcement_is_refused_after_a_key_rotation_or_a_new_writer() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = writer_with_deferred_checkpoints(&cluster, Arc::new(journal.handle(NODE_1_IDENTITY))).await;
    commit_range(&mut writer, 1..=2, "kept").await;
    let capture = writer.capture_checkpoint(1).await.unwrap().unwrap();
    let prepared = writer.checkpoint_builder().build(capture).await.unwrap();
    writer.rotate_clean_checkpoint(&[]).await.unwrap();
    let floor = writer.retained_floor();
    let head = writer.state().head;
    assert_eq!(writer.announce_checkpoint(prepared).await.unwrap(), deferred::Announcement::Invalidated);
    assert_eq!((writer.retained_floor(), writer.state().head), (floor, head));
    // While the rotation awaits its retirement no capture is taken.
    assert!(writer.capture_checkpoint(1).await.unwrap().is_none());

    // A capture under one writer epoch is never announced by another.
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = writer_with_deferred_checkpoints(&cluster, Arc::new(journal.handle(NODE_1_IDENTITY))).await;
    commit_range(&mut writer, 1..=2, "kept").await;
    let capture = writer.capture_checkpoint(1).await.unwrap().unwrap();
    let prepared = writer.checkpoint_builder().build(capture).await.unwrap();
    cluster.at(120);
    let mut successor = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY))).with_deferred_checkpoints(true);
    assert_eq!(successor.tick(&live(2)).await.unwrap(), Role::Writer(Epoch(2)));
    assert!(!matches!(writer.announce_checkpoint(prepared).await, Ok(deferred::Announcement::Announced(_))));
    assert_eq!(successor.retained_floor(), Some(Lsn(1)));
    commit_range(&mut successor, 3..=3, "successor").await;
    assert!(successor.checkpoint_retained_suffix(1).await.unwrap().is_some());
    assert_texts(&mut successor, &[(1, "kept"), (2, "kept"), (3, "successor")]).await;
}

#[tokio::test]
async fn an_unannounced_deferred_manifest_leaves_replay_unchanged_and_is_reclaimed() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = writer_with_deferred_checkpoints(&cluster, Arc::new(journal.handle(NODE_1_IDENTITY))).await;
    commit_range(&mut writer, 1..=3, "committed").await;
    let capture = writer.capture_checkpoint(1).await.unwrap().unwrap();
    let prepared = writer.checkpoint_builder().build(capture).await.unwrap();
    let orphan = prepared.manifest();
    assert!(read_manifest(cluster.store.as_ref(), cluster.kms.as_ref(), orphan).await.is_ok());
    // The writer stops after the upload and before the announcement.
    drop(writer);

    cluster.at(120);
    let mut successor = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    assert_eq!(successor.tick(&live(2)).await.unwrap(), Role::Writer(Epoch(2)));
    assert_eq!(successor.retained_floor(), Some(Lsn(1)));
    assert!(!successor.state().records.iter().any(|record| matches!(record, Record::Checkpoint(_))));
    assert_texts(&mut successor, &[(1, "committed"), (3, "committed")]).await;
    assert!(successor.collect().await.unwrap() >= 1);
    assert!(read_manifest(cluster.store.as_ref(), cluster.kms.as_ref(), orphan).await.is_err());
}

#[tokio::test]
async fn an_announced_deferred_checkpoint_replays_from_the_old_floor_until_the_next_trim() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let gate = Arc::new(GateJournal::new(Arc::new(journal.handle(NODE_1_IDENTITY))));
    let mut writer = writer_with_deferred_checkpoints(&cluster, gate.clone()).await;
    commit_range(&mut writer, 1..=2, "before").await;
    assert!(writer.checkpoint_retained_suffix(1).await.unwrap().is_some());
    let old_floor = writer.retained_floor().unwrap();
    commit_range(&mut writer, 3..=4, "cut").await;
    let capture = writer.capture_checkpoint(1).await.unwrap().unwrap();
    let cut = capture.through();
    commit_range(&mut writer, 5..=6, "between").await;
    let prepared = writer.checkpoint_builder().build(capture).await.unwrap();
    // The announcement commits and the trim does not.
    gate.block_trim.store(true, Ordering::SeqCst);
    assert!(writer.announce_checkpoint(prepared).await.is_err());
    gate.block_trim.store(false, Ordering::SeqCst);
    drop(writer);

    cluster.at(50);
    let expected = [(1, "before"), (2, "before"), (3, "cut"), (4, "cut"), (5, "between"), (6, "between")];
    let mut fresh = cluster.node(3, Arc::new(journal.handle(NODE_3_IDENTITY)));
    assert_eq!(fresh.tick(&live(1)).await.unwrap(), Role::Follower);
    assert_eq!(fresh.retained_floor(), Some(old_floor));
    assert!(fresh.state().records.iter().any(|record| matches!(record,
        Record::Checkpoint(checkpoint) if checkpoint.through == cut.0)));
    assert_texts(&mut fresh, &expected).await;

    // The next writer checkpoints again and trims past the old floor.
    cluster.at(120);
    let mut successor = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY))).with_deferred_checkpoints(true);
    assert_eq!(successor.tick(&live(2)).await.unwrap(), Role::Writer(Epoch(2)));
    assert!(successor.checkpoint_retained_suffix(1).await.unwrap().is_some());
    assert!(successor.retained_floor().unwrap() > Lsn(cut.0));
    assert_texts(&mut successor, &expected).await;
    let mut reopened = cluster.node(3, Arc::new(journal.handle(NODE_3_IDENTITY)));
    assert_eq!(reopened.tick(&live(2)).await.unwrap(), Role::Follower);
    assert_same_replay(&successor, &reopened);
}

#[tokio::test]
async fn the_pipeline_commits_while_a_deferred_checkpoint_builds() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let writer = writer_with_deferred_checkpoints(&cluster, Arc::new(journal.handle(NODE_1_IDENTITY))).await;
    let writes = Arc::new(WritePipeline::start(Arc::new(Mutex::new(writer))));
    for id in 1..=4 {
        writes.transact(put(id as u8, id, "before")).await.unwrap();
    }
    let commits = {
        let writes = Arc::clone(&writes);
        tokio::spawn(async move {
            for id in 5..=16 {
                writes.transact(put(id as u8, id, "concurrent")).await.unwrap();
            }
        })
    };
    let manifest = writes.bound_journal(1).await.unwrap();
    commits.await.unwrap();
    assert!(manifest.is_some());
    assert!(writes.bound_journal(1_000).await.unwrap().is_none());
    cluster.at(50);
    let mut fresh = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    assert_eq!(fresh.tick(&live(1)).await.unwrap(), Role::Follower);
    assert!(fresh.retained_floor().unwrap() > Lsn(1));
    for id in 1..=16 {
        assert!(text_at(&mut fresh, MAIN_BRANCH, id).await.is_some(), "row {id}");
    }
}

#[tokio::test]
async fn binary_checkpoints_replay_over_json_checkpoints_and_the_reverse() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY))).with_binary_checkpoints(false);
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    commit_range(&mut writer, 1..=2, "json").await;
    let json = writer.checkpoint(Vec::new()).await.unwrap();
    let image = read_manifest(cluster.store.as_ref(), cluster.kms.as_ref(), json).await.unwrap();
    assert_eq!(image.contents().payload[24] & 8, 0);
    cluster.at(50);
    let mut reader = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY))).with_binary_checkpoints(true);
    assert_eq!(reader.tick(&live(1)).await.unwrap(), Role::Follower);
    assert_texts(&mut reader, &[(1, "json"), (2, "json")]).await;
    assert_same_replay(&writer, &reader);

    let mut writer = writer.with_binary_checkpoints(true);
    commit_range(&mut writer, 3..=3, "binary").await;
    let binary = writer.checkpoint(Vec::new()).await.unwrap();
    let image = read_manifest(cluster.store.as_ref(), cluster.kms.as_ref(), binary).await.unwrap();
    assert_eq!(image.contents().payload[24] & 8, 8);
    assert!(image.contents().payload.len() < 25 + writer.state().transactions.encode_checkpoint().unwrap().len());
    cluster.at(60);
    let mut reader = cluster.node(3, Arc::new(journal.handle(NODE_3_IDENTITY))).with_binary_checkpoints(false);
    assert_eq!(reader.tick(&live(1)).await.unwrap(), Role::Follower);
    assert_texts(&mut reader, &[(1, "json"), (3, "binary")]).await;
    assert_same_replay(&writer, &reader);
}

#[tokio::test]
async fn reachability_reads_each_checkpoint_manifest_once() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    commit_range(&mut writer, 1..=2, "pinned").await;
    let first = writer.checkpoint(Vec::new()).await.unwrap();
    let pin = writer.pin_checkpoint(first, None).await.unwrap();
    commit_range(&mut writer, 3..=3, "later").await;
    writer.checkpoint(Vec::new()).await.unwrap();
    // The writer resolves the closure from its own summaries: the objects
    // can disappear from the store without changing it.
    let manifests: Vec<_> = writer.state().records.iter().filter_map(|record| match record {
        Record::Checkpoint(checkpoint) => Some(checkpoint.manifest),
        _ => None,
    }).chain(writer.state().base.as_ref().map(|base| base.checkpoint.manifest)).collect();
    let saved = futures_copy(&cluster, &manifests).await;
    for id in &manifests {
        cluster.store.delete(&ObjectKey::new(id.to_string()).unwrap()).await.unwrap();
    }
    let lease = writer.authority().unwrap();
    let closure = writer.reachability(lease).await.unwrap();
    assert!(closure.pins().any(|held| *held == pin));
    assert!(manifests.iter().all(|id| closure.contains(*id)));
    for (id, bytes) in saved {
        cluster.store.put(&ObjectKey::new(id.to_string()).unwrap(), bytes).await.unwrap();
    }
}

async fn futures_copy(cluster: &Cluster, ids: &[ObjectId]) -> Vec<(ObjectId, Bytes)> {
    let mut saved = Vec::new();
    for id in ids {
        saved.push((*id, cluster.store.get(&ObjectKey::new(id.to_string()).unwrap()).await.unwrap()));
    }
    saved
}

#[tokio::test]
async fn an_entry_another_writer_appends_wakes_an_applied_watch_before_any_refresh() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    let mut follower = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    writer.transact(&put(1, 1, "first")).await.unwrap();
    follower.fresh_index().await.unwrap();

    let mut watch = follower.watch_applied();
    watch.mark_seen();
    let mut waiting = Box::pin(watch.changed(tokio::time::Instant::now() + Duration::from_secs(60)));
    assert!(tokio::time::timeout(Duration::from_millis(50), &mut waiting).await.is_err(), "nothing is left to apply");
    writer.transact(&put(2, 2, "second")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), waiting).await.expect("the other writer's entry wakes the watch").unwrap();
}

/// Counts the reads that wait for new records.
struct LongPolls {
    inner: Arc<dyn Journal>,
    count: AtomicUsize,
}

impl Journal for LongPolls {
    fn create(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<()>> {
        self.inner.create(stream)
    }

    fn seal(&self, stream: StreamId, epoch: Epoch) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        self.inner.seal(stream, epoch)
    }

    fn append(&self, stream: StreamId, epoch: Epoch, at: Lsn, payload: Bytes) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        self.inner.append(stream, epoch, at, payload)
    }

    fn read(&self, stream: StreamId, from: Lsn, max: usize, wait: Duration) -> BoxFuture<'_, journal_contract::Result<Page>> {
        if !wait.is_zero() {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.read(stream, from, max, wait)
    }

    fn head(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        self.inner.head(stream)
    }

    fn trim(&self, stream: StreamId, epoch: Epoch, through: Lsn) -> BoxFuture<'_, journal_contract::Result<()>> {
        self.inner.trim(stream, epoch, through)
    }

    fn delete(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<()>> {
        self.inner.delete(stream)
    }

    fn acknowledge_recovery(&self, stream: StreamId) -> BoxFuture<'_, journal_contract::Result<Lsn>> {
        self.inner.acknowledge_recovery(stream)
    }
}

#[tokio::test]
async fn applied_watch_waiters_share_one_journal_long_poll() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    let polls = Arc::new(LongPolls { inner: Arc::new(journal.handle(NODE_2_IDENTITY)), count: AtomicUsize::new(0) });
    let mut follower = cluster.node(2, polls.clone());
    writer.create().await.unwrap();
    writer.tick(&live(1)).await.unwrap();
    writer.transact(&put(1, 1, "first")).await.unwrap();
    follower.fresh_index().await.unwrap();

    let until = tokio::time::Instant::now() + Duration::from_secs(60);
    let waiters: Vec<_> = (0..4).map(|_| {
        let mut watch = follower.watch_applied();
        watch.mark_seen();
        tokio::spawn(async move { watch.changed(until).await })
    }).collect();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(waiters.iter().all(|waiter| !waiter.is_finished()), "nothing is left to apply");
    writer.transact(&put(2, 2, "second")).await.unwrap();
    for waiter in waiters {
        tokio::time::timeout(Duration::from_secs(10), waiter).await.expect("the entry wakes every waiter").unwrap().unwrap();
    }
    // A woken waiter that re-checks before its refresh lands may poll once more, but never one poll per waiter.
    assert!(polls.count.load(Ordering::SeqCst) < 4, "the waiters shared their long polls");
}

#[tokio::test]
async fn next_deadline_tracks_writer_renewal_and_follower_lapse() {
    let journal = journals().await;
    let cluster = Cluster::new();
    let mut writer = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    let mut follower = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    writer.create().await.unwrap();
    assert_eq!(writer.next_deadline(&live(1)), None);

    writer.tick(&live(1)).await.unwrap();
    assert_eq!(
        writer.next_deadline(&live(1)),
        Some(Duration::from_micros((LEASE / 2 - DRIFT) as u64)),
    );
    assert!(writer.next_checkpoint_lsn(10).is_some());

    follower.fresh_index().await.unwrap();
    assert_eq!(follower.next_deadline(&live(1)), None);
    assert_eq!(
        follower.next_deadline(&live(2)),
        Some(Duration::from_micros((LEASE + DRIFT + 1) as u64)),
    );
    assert_eq!(follower.next_checkpoint_lsn(10), None);

    cluster.at(10 + LEASE + DRIFT + 1);
    assert_eq!(writer.next_deadline(&live(1)), Some(Duration::ZERO));
    assert_eq!(follower.next_deadline(&live(2)), Some(Duration::ZERO));
    assert_eq!(follower.next_deadline(&live(1)), None);
}
