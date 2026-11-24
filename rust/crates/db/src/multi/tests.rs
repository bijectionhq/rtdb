use std::{
    sync::atomic::{AtomicBool, AtomicI64, Ordering},
    time::Duration,
};

use bytes::Bytes;
use journal_contract::{
    BoxFuture, Identity, Page, StreamId,
    reference::{FileJournal, Limits, MemoryJournal},
};
use rtdb_crypt::LocalKms;
use rtdb_logkeeper::simulation::{Fault, FaultJournal};
use rtdb_store::{MemoryStore, read_file};
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
    }
}

fn ip(n: u8) -> IpAddr {
    IpAddr::from([192, 0, 2, n])
}

fn live(n: u8) -> BTreeSet<IpAddr> {
    BTreeSet::from([ip(n)])
}

#[tokio::test]
async fn ranked_catalog_replays_at_the_built_cut_after_later_writes() {
    let journal = MemoryJournal::default();
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
    let journal = MemoryJournal::default();
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
    drop(owner);
    assert!(matches!(follower.tick(&live(2)).await.unwrap(), Role::Writer(_)));
}

#[tokio::test]
async fn multi_driver_tokens_reroute_and_verify_the_row_footprint() {
    use rtdb_crypt::tokens::PageToken;
    use rtdb_types::rtdb_v1::{ClaimEvidence, ClaimFootprint, ClaimRegion, ClaimScope};

    let journal = MemoryJournal::default();
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
    let journal = MemoryJournal::default();
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
    assert_eq!(record.canonical_request().unwrap().get(), timed.canonical_request().unwrap().get());
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
    let journal = MemoryJournal::default();
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
    use rtdb_tx::transact::{CustodyPayload, CustodyPlacement, CustodyPlacementCommitments};
    use sha2::{Digest, Sha256};

    let journal = MemoryJournal::default();
    let cluster = Cluster::new();
    let mut source = cluster.node(1, Arc::new(journal.handle(NODE_1_IDENTITY)));
    source.create().await.unwrap();
    source.tick(&live(1)).await.unwrap();
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
                placement_id: group, reservation: None, carrier_column: "carrier".into(), link_column: None,
                commitments: Some(CustodyPlacementCommitments { carrier_sha256: Sha256::digest(carrier).into(), link_sha256: None }),
            },
        };
        let mut write = request(n, Op::CreateCustodyGroup { group });
        write.ops.push(occurrence);
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
    let (_, view, _) = follower.require_managed_checkpoint_restore(reservation).await.unwrap();
    assert_eq!(view.require_local_custody().unwrap_err().code, "rtdb/inherited-custody");
    assert!(view.custody_inventory(*group).is_none());
    let (inherited, occurrence) = view.native_inherited_carrier(MAIN_BRANCH, &table(), &EntityId::Integer(1), envelope).unwrap();
    assert_eq!(inherited, *group);
    assert_eq!(view.native_inherited_carrier(MAIN_BRANCH, &table(), &EntityId::Integer(2), envelope).unwrap_err().code, "rtdb/native-checkpoint-binding");
    assert_eq!(source.fresh_index().await.unwrap().check_native_export_read(&claim, installed, inherited, occurrence).unwrap_err().code, "rtdb/native-checkpoint-binding");

    source.record_native_checkpoint_drain(installation).await.unwrap();
    source.release_managed_checkpoint(reservation).await.unwrap();
    assert_eq!(source.pin_managed_checkpoint(pin, selected.basis, reservation.target).await.unwrap_err().code, "rtdb/native-checkpoint-pin-used");
    source.checkpoint(Vec::new()).await.unwrap();
    let mut original_follower = cluster.node(3, Arc::new(journal.handle(NODE_3_IDENTITY)));
    let original_view = original_follower.fresh_index().await.unwrap();
    original_view.check_native_export_read(&claim, installed, inherited, occurrence).unwrap();
    let (_, retained) = original_view.custody_read_occurrence(selected.basis, inherited, occurrence).unwrap();
    assert_eq!(retained.written, original.basis);
    assert_eq!(retained.receipt_key, original.key);

    let stale = request(3, Op::RevokeCustodyGroup { group: *other_group, inventory_sha256: other_inventory });
    let stale = append_custody_fixture(&mut source, &stale).await;
    assert!(matches!(stale.outcome, Outcome::Refused { error } if error.code == "rtdb/custody-inventory-changed"));
    let inventory = source.fresh_index().await.unwrap().custody_inventory(*other_group).unwrap().sha256;
    let revoke = request(4, Op::RevokeCustodyGroup { group: *other_group, inventory_sha256: inventory });
    assert_eq!(append_custody_fixture(&mut source, &revoke).await.outcome, Outcome::Then);
    assert_eq!(source.fresh_index().await.unwrap().check_native_export_read(&claim, installed, inherited, occurrence).unwrap_err().code, "rtdb/erased-under-view");
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
async fn exact_export_pins_the_selected_branch_ancestry_after_trim() {
    let journal = MemoryJournal::default();
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

#[tokio::test]
async fn multi_driver_replacement_refuses_a_timed_tx_after_its_reply_is_lost() {
    let journal = MemoryJournal::default();
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
    let journal = MemoryJournal::default();
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
    let journal = MemoryJournal::default();
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
async fn deployment_retirement_closes_admission_after_a_lost_reply() {
    let journal = MemoryJournal::default();
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
    let journal = MemoryJournal::default();
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
    let journal = MemoryJournal::default();
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
    let journal = MemoryJournal::default();
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
    let journal = MemoryJournal::default();
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
async fn multi_driver_late_upload_cannot_reenter_reachability() {
    let journal = MemoryJournal::default();
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

    let journal = MemoryJournal::default();
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
    let journal = MemoryJournal::default();
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
    let journal = MemoryJournal::default();
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
    let journal = MemoryJournal::default();
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
    let journal = MemoryJournal::default();
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
    let journal = MemoryJournal::default();
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
    let journal = MemoryJournal::default();
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

    let journal = MemoryJournal::default();
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
    let custody = Authority::CustodyWrite { fence, writer, request_digest: [3; 32] };
    let retained = receipt(writes.workload(command([3; 32], submit(3, custody, Some(transaction)))).await.unwrap());

    database.lock().await.checkpoint(Vec::new()).await.unwrap();
    drop(writes);
    cluster.at(116);
    let mut successor = cluster.node(2, Arc::new(journal.handle(NODE_2_IDENTITY)));
    assert!(matches!(successor.tick(&live(2)).await.unwrap(), Role::Writer(_)));
    let recovered = successor.original_custody_receipt(&retained.key, retained.basis).await.unwrap();
    assert_eq!(recovered, Receipt { replayed: true, ..retained.clone() });
    let writes = WritePipeline::start(Arc::new(Mutex::new(successor)));
    let status = |family| Operation::Status { key: retained.key.clone(), family };
    assert_eq!(writes.workload(command([3; 32], status(Family::Write))).await.err().unwrap().code, "rtdb/unknown-transaction");
    let Reply::Receipt(scoped) = writes.workload(command([3; 32], status(Family::CustodyWrite))).await.unwrap()
        else { panic!("expected recovered custody receipt") };
    assert_eq!(scoped, recovered);
}
