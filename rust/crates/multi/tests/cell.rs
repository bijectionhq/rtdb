//! A whole cell in one process: a directory, three logkeepers, nodes and a gateway, driven through a real Postgres client.

mod common;

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use common::*;
use rtdb_directory::{DeploymentRecord, Directory, Lifecycle, MemberId, MemberPool, MemoryDirectory};
use rtdb_multi::admin;
use rtdb_multi::control::{Request, Response};
use rtdb_multi::keys::KeyService;
use rtdb_multi::cells::{CellMap, MemoryCellMap};
use rtdb_multi::deployment::CellStorage;
use rtdb_multi::keys::LocalKeyService;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio_postgres::NoTls;

async fn await_resident_idle(cell: &Cell, name: &str) -> rtdb_directory::DeploymentRecord {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let record = cell.directory.deployment_by_name(name).await.unwrap().unwrap();
            if record.is_resident_idle() { return record; }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("the holding reached resident idle")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resident_idle_releases_the_writer_and_wakes_with_a_fresh_epoch() {
    let cell = Cell::start_with_resident_idle(2, Duration::from_millis(300), None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = connect_eventually(&cell, "shop").await;
    client.simple_query("INSERT INTO docs (_id, value) VALUES (1, 71)").await.unwrap();
    let source = cell.holder("shop").await.unwrap();
    let serving = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    drop(client);

    let resident = await_resident_idle(&cell, "shop").await;
    assert_eq!(resident.holder, serving.holder);
    assert_eq!(resident.epoch, serving.epoch + 1);
    assert!(resident.checkpoint.is_some());
    let status = admin::cell_status(cell.directory.as_ref(), 1).await.unwrap();
    assert_eq!((status.active, status.resident_idle, status.parked), (0, 1, 0));
    let placement = admin::placement_status(cell.directory.as_ref(), 1).await.unwrap();
    assert_eq!((placement.general.holdings, placement.general.resident_idle), (1, 1));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let node = cell.node(source);
            if node.agent.held().is_empty() && node.server.node().catalog().database("shop").is_err()
                && !node.server.node().catalog().is_draining("shop")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("resident idle closed the database driver");

    let client = connect_eventually(&cell, "shop").await;
    let woke = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert!(!woke.is_resident_idle());
    assert_eq!(woke.holder, serving.holder);
    assert_eq!(woke.epoch, resident.epoch + 1);
    assert_eq!(values(&client, "SELECT value FROM docs").await, ["71"]);
    drop(client);

    let idle_again = await_resident_idle(&cell, "shop").await;
    let target = 1 - source;
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(target).lease.id(), &control()).await.unwrap();
    let moved = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert!(!moved.is_resident_idle());
    assert_eq!(moved.epoch, idle_again.epoch + 2);
    assert_eq!(moved.holder.as_ref(), Some(&cell.node(target).lease.id()));
    let client = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&client, "SELECT value FROM docs").await, ["71"]);
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resident_idle_holder_can_later_park_from_the_idle_deadline() {
    let directory = ObservedDirectory::new();
    let cell = Cell::start_with_resident_deadline(directory.clone(), Duration::from_millis(300), Duration::from_secs(2)).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = connect_eventually(&cell, "shop").await;
    client.simple_query("INSERT INTO docs (_id, value) VALUES (1, 35)").await.unwrap();
    drop(client);
    let resident = await_resident_idle(&cell, "shop").await;
    let parked = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let record = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
            if record.holder.is_none() { break record; }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("the resident idled through its park deadline");
    assert!(!parked.is_resident_idle());
    assert_eq!(parked.epoch, resident.epoch + 1);
    assert_eq!(directory.holder_queries.load(Ordering::Relaxed), 0);
    let client = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&client, "SELECT value FROM docs").await, ["35"]);
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopping_a_node_releases_its_agent_and_its_standby_storage_witness() {
    let mut cell = Cell::start(1, None).await;
    let agent = Arc::downgrade(&cell.node(0).agent);
    let keys = cell.data.path().join("keys");
    let has_storage_witness = || {
        keys.read_dir().unwrap().any(|entry| entry.unwrap().path().extension().is_some_and(|extension| extension == "storage-region"))
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !has_storage_witness() { tokio::time::sleep(Duration::from_millis(10)).await; }
    }).await.unwrap();
    cell.nodes[0].take().unwrap().kill().await.unwrap();
    assert!(agent.upgrade().is_none());
    assert!(!has_storage_witness());
    cell.stop().await;
}

#[derive(Default)]
struct RecoveryGate {
    registered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

struct ObservedDirectory {
    inner: Arc<MemoryDirectory>,
    recovery_gate: std::sync::Mutex<Option<Arc<RecoveryGate>>>,
    opening_gate: std::sync::Mutex<Option<(String, Arc<RecoveryGate>)>>,
    lost_anchor_reply: std::sync::Mutex<Option<(u64, Arc<RecoveryGate>)>>,
    restore_interruption: std::sync::atomic::AtomicUsize,
    reads: std::sync::atomic::AtomicUsize,
    holder_queries: std::sync::atomic::AtomicUsize,
    membership_scans: std::sync::atomic::AtomicUsize,
    local_retirement_completed: std::sync::Mutex<Option<(String, Arc<tokio::sync::Notify>)>>,
}

impl ObservedDirectory {
    fn new() -> Arc<Self> {
        Arc::new(Self { inner: MemoryDirectory::new(), recovery_gate: Default::default(), opening_gate: Default::default(), lost_anchor_reply: Default::default(), restore_interruption: Default::default(), reads: Default::default(), holder_queries: Default::default(), membership_scans: Default::default(), local_retirement_completed: Default::default() })
    }
}

#[async_trait::async_trait]
impl Directory for ObservedDirectory {
    async fn reserve_installed_journal(&self, id: uuid::Uuid) -> Result<()> { self.inner.reserve_installed_journal(id).await }
    async fn create_deployment(&self, record: DeploymentRecord) -> Result<DeploymentRecord> { self.inner.create_deployment(record).await }
    async fn stage_fork(&self, child: DeploymentRecord, preparation: rtdb_directory::ForkPreparation) -> Result<DeploymentRecord> {
        self.inner.stage_fork(child, preparation).await
    }
    async fn publish_fork(&self, child: DeploymentRecord, expected: rtdb_directory::Version, preparation: Option<rtdb_directory::ForkPublication>) -> Result<DeploymentRecord> { self.inner.publish_fork(child, expected, preparation).await }
    async fn lineage_state(&self, lineage: &str) -> Result<rtdb_directory::LineageState> { self.inner.lineage_state(lineage).await }
    async fn change_lineage(&self, lineage: &str, change: rtdb_directory::LineageChange) -> Result<rtdb_directory::LineageState> {
        let completed = match &change {
            rtdb_directory::LineageChange::SettleRetirement { retirement } => self.local_retirement_completed.lock().unwrap()
                .as_ref().filter(|(source, _)| source == &retirement.source).map(|(_, completed)| Arc::clone(completed)),
            _ => None,
        };
        let result = self.inner.change_lineage(lineage, change).await?;
        if let Some(completed) = completed { completed.notify_one(); }
        Ok(result)
    }
    async fn authorize_owned_retirement(&self, id: &str, expected: rtdb_directory::Version, retirement: rtdb_directory::LineageRetirement) -> Result<rtdb_directory::LineageState> {
        self.inner.authorize_owned_retirement(id, expected, retirement).await
    }
    async fn settle_owned_version(&self, id: &str, expected: rtdb_directory::Version, retirement: rtdb_directory::LineageRetirement) -> Result<rtdb_directory::Cas<DeploymentRecord>> { self.inner.settle_owned_version(id, expected, retirement).await }
    async fn begin_reference_disposal(&self, id: &str, expected: rtdb_directory::Version) -> Result<rtdb_directory::ReferenceDisposal> { self.inner.begin_reference_disposal(id, expected).await }
    async fn settle_reference_disposal(&self, permit: &rtdb_directory::ReferenceDisposal, record: DeploymentRecord, expected: rtdb_directory::Version) -> Result<rtdb_directory::Cas<DeploymentRecord>> { self.inner.settle_reference_disposal(permit, record, expected).await }
    async fn deployment(&self, id: &str) -> Result<Option<DeploymentRecord>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.deployment(id).await
    }
    async fn deployment_by_name(&self, name: &str) -> Result<Option<DeploymentRecord>> { self.inner.deployment_by_name(name).await }
    async fn deployments(&self) -> Result<Vec<DeploymentRecord>> { self.inner.deployments().await }
    async fn deployments_held_by(&self, id: &MemberId) -> Result<Vec<DeploymentRecord>> {
        self.holder_queries.fetch_add(1, Ordering::Relaxed);
        self.inner.deployments_held_by(id).await
    }
    async fn replace_deployment(&self, record: DeploymentRecord, expected: rtdb_directory::Version) -> Result<rtdb_directory::Cas<DeploymentRecord>> {
        let anchor_reply = self.lost_anchor_reply.lock().unwrap().clone();
        let anchor_update = if let Some((epoch, gate)) = anchor_reply
            && record.epoch == epoch && record.replica_anchor.is_some()
            && self.inner.deployment(&record.id).await?.is_some_and(|before| before.replica_anchor != record.replica_anchor)
        { Some(gate) } else { None };
        let interrupted = || Anomaly::new(AnomalyCategory::Unavailable, "test/restore-interrupted", "Restore lost its directory response");
        if record.checkpoint_recovery.as_ref().is_some_and(|recovery| recovery.completed_at_ms.is_some())
            && self.restore_interruption.compare_exchange(2, 3, Ordering::Relaxed, Ordering::Relaxed).is_ok()
        {
            return Err(interrupted());
        }
        let result = self.inner.replace_deployment(record, expected).await?;
        if matches!(&result, rtdb_directory::Cas::Applied(record) if record.lifecycle == Lifecycle::Restoring)
            && self.restore_interruption.compare_exchange(1, 2, Ordering::Relaxed, Ordering::Relaxed).is_ok()
        {
            return Err(interrupted());
        }
        if let Some(gate) = anchor_update && matches!(&result, rtdb_directory::Cas::Applied(_)) {
            gate.registered.notify_one();
            gate.resume.notified().await;
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "test/anchor-reply-lost", "The anchor update reply was lost"));
        }
        let opening = match &result {
            rtdb_directory::Cas::Applied(record) if !record.storage_regions.is_empty() => {
                let mut gate = self.opening_gate.lock().unwrap();
                if gate.as_ref().is_some_and(|(deployment, _)| deployment == &record.id) {
                    gate.take().map(|(_, gate)| gate)
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some(gate) = opening {
            gate.registered.notify_one();
            gate.resume.notified().await;
        }
        let gate = match &result {
            rtdb_directory::Cas::Applied(record) if record.storage_regions.values().any(|region| matches!(region, rtdb_directory::StorageRegion::ReplicaReplay { .. })) => {
                self.recovery_gate.lock().unwrap().take()
            }
            _ => None,
        };
        if let Some(gate) = gate {
            gate.registered.notify_one();
            gate.resume.notified().await;
        }
        Ok(result)
    }
    async fn register_member(&self, member: rtdb_directory::MemberRecord) -> Result<rtdb_directory::MemberRecord> { self.inner.register_member(member).await }
    async fn member(&self, id: &MemberId) -> Result<Option<rtdb_directory::MemberRecord>> { self.inner.member(id).await }
    async fn members(&self) -> Result<Vec<rtdb_directory::MemberRecord>> {
        self.membership_scans.fetch_add(1, Ordering::Relaxed);
        self.inner.members().await
    }
    async fn replace_member(&self, member: rtdb_directory::MemberRecord, expected: rtdb_directory::Version) -> Result<rtdb_directory::Cas<rtdb_directory::MemberRecord>> { self.inner.replace_member(member, expected).await }
    fn change_feed(&self) -> Option<&rtdb_directory::ChangeFeed> { self.inner.change_feed() }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unrelated_cold_start_finishes_while_the_first_waits_for_storage() {
    let directory = ObservedDirectory::new();
    let cell = Cell::start_with_directory(directory.clone()).await;
    let waiting = admin::create_deployment(directory.as_ref(), "waiting", "acme", 3).await.unwrap();
    admin::create_deployment(directory.as_ref(), "independent", "acme", 3).await.unwrap();
    let gate = Arc::new(RecoveryGate::default());
    *directory.opening_gate.lock().unwrap() = Some((waiting.id, gate.clone()));
    let agent = cell.node(0).agent.clone();
    let first = tokio::spawn(async move { agent.activate("waiting").await });
    tokio::time::timeout(Duration::from_secs(5), gate.registered.notified()).await.unwrap();
    assert!(cell.node(0).server.node().catalog().database("waiting").is_err());
    let second = tokio::time::timeout(Duration::from_secs(4), cell.connect("independent")).await.unwrap();
    second.simple_query("INSERT INTO orders (_id, total) VALUES (1, 7)").await.unwrap();
    assert_eq!(values(&second, "SELECT total FROM orders").await, ["7"]);
    assert!(!first.is_finished());
    gate.resume.notify_one();
    assert!(matches!(tokio::time::timeout(Duration::from_secs(5), first).await.unwrap().unwrap().unwrap(), Response::Held { .. }));
    drop(second);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_holdings_follow_the_shared_feed_without_reconcile_point_reads() {
    let directory = ObservedDirectory::new();
    let cell = Cell::start_with_directory(directory.clone()).await;
    let deployment = admin::create_deployment(directory.as_ref(), "shop", "acme", 3).await.unwrap();
    assert!(matches!(cell.node(0).agent.activate("shop").await.unwrap(), Response::Held { .. }));
    let node = cell.node(0);
    tokio::time::sleep(Duration::from_millis(350)).await;
    let reads = directory.reads.load(Ordering::Relaxed);
    for _ in 0..3 { node.agent.reconcile().await; }
    assert_eq!(directory.reads.load(Ordering::Relaxed), reads);

    let mut record = directory.inner.deployment(&deployment.id).await.unwrap().unwrap();
    let version = record.version;
    record.holder = None;
    record.epoch += 1;
    directory.inner.replace_deployment(record, version).await.unwrap().applied().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while node.agent.held().contains_key("shop") { tokio::time::sleep(Duration::from_millis(20)).await; }
    }).await.expect("the holder observes the newer placement through the feed");
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routing_to_a_current_writer_does_not_scan_the_member_inventory() {
    let directory = ObservedDirectory::new();
    let cell = Cell::start_with_directory(directory.clone()).await;
    admin::create_deployment(directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let node = cell.node(0);
    let Response::Held { epoch, .. } = node.agent.activate("shop").await.unwrap() else { panic!("expected the writer") };
    let scans = directory.membership_scans.load(Ordering::Relaxed);
    for _ in 0..4 {
        assert!(matches!(node.agent.activate("shop").await.unwrap(), Response::Held { epoch: held, .. } if held == epoch));
        let client = connect_eventually(&cell, "shop").await;
        assert_eq!(values(&client, "SELECT 13").await, ["13"]);
        drop(client);
    }
    assert_eq!(directory.membership_scans.load(Ordering::Relaxed), scans);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resource_rebalancing_moves_the_largest_tail_toward_an_eligible_node() {
    let cell = Cell::start(2, None).await;
    let directory = cell.directory.as_ref();
    for (name, bytes) in [("heavy", 8192), ("light", 16)] {
        admin::create_deployment(directory, name, "acme", 3).await.unwrap();
        assert!(matches!(cell.node(0).agent.activate(name).await.unwrap(), Response::Held { .. }));
        let client = connect_eventually(&cell, name).await;
        client.simple_query(&format!("INSERT INTO docs (_id, payload) VALUES (1, '{}')", "x".repeat(bytes))).await.unwrap();
    }
    let snapshot = rtdb_multi::control::call(&cell.node(0).control_address.to_string(), &Request::HoldingLoads { measure_query_cpu: true }, Duration::from_secs(2), &control()).await.unwrap();
    let Response::HoldingLoads { holdings } = snapshot else { panic!("expected the writer's active loads") };
    assert_eq!(holdings.len(), 2);
    let reader = connect_eventually(&cell, "heavy").await;
    assert_eq!(values(&reader, "SELECT CHAR_LENGTH(payload) FROM docs").await, ["8192"]);
    drop(reader);
    let profile = rtdb_multi::control::call(&cell.node(0).control_address.to_string(), &Request::HoldingLoads { measure_query_cpu: true }, Duration::from_secs(2), &control()).await.unwrap();
    let Response::HoldingLoads { holdings: after_probe } = profile else { panic!("expected the writer's CPU sample") };
    if cfg!(any(target_os = "linux", target_os = "macos")) {
        let selected = |name: &str| after_probe.iter().find(|holding| holding.name == name).unwrap().query_cpu_nanos;
        assert!(selected("heavy").unwrap() > 0);
        assert_eq!(selected("light"), Some(0));
    }
    let submitted: BTreeMap<_, _> = holdings.iter().map(|holding| (holding.name.as_str(), holding.submitted_bytes.unwrap())).collect();
    assert!(submitted["heavy"] > 8192 && submitted["heavy"] > submitted["light"] && submitted["light"] > 0);
    let by_name: BTreeMap<_, _> = holdings.into_iter().map(|holding| (holding.name, holding.load.tail_bytes)).collect();
    assert!(by_name["heavy"] > by_name["light"] && by_name["light"] > 0);
    let limit = std::num::NonZeroU64::new(by_name["heavy"] + by_name["light"] / 2).unwrap();
    let limits = rtdb_multi::load::LoadLimits { tail_bytes: Some(limit), ..Default::default() };
    let mut samples = rtdb_multi::load::LoadSamples::default();

    assert_eq!(rtdb_multi::load::rebalance(directory, 4, limits, &control(), &mut samples).await.unwrap(), 1);
    assert_eq!(cell.holder("heavy").await, Some(1));
    assert_eq!(cell.holder("light").await, Some(0));
    let client = connect_eventually(&cell, "heavy").await;
    assert_eq!(values(&client, "SELECT CHAR_LENGTH(payload) FROM docs").await, ["8192"]);
    drop(client);
    assert_eq!(rtdb_multi::load::rebalance(directory, 4, limits, &control(), &mut samples).await.unwrap(), 0);
    cell.stop().await;
}

#[tokio::test]
async fn the_local_directory_reopens_the_same_installation_and_fence() {
    let state = tempfile::tempdir().unwrap();
    assert!(MemoryDirectory::open(state.path()).is_err());
    MemoryDirectory::initialize(state.path()).unwrap();
    let directory = MemoryDirectory::open(state.path()).unwrap();
    assert!(MemoryDirectory::open(state.path()).is_err());
    let initial = directory.create_deployment(
        DeploymentRecord::new("shop", "acme", vec![], 0),
    ).await.unwrap();
    let mut fenced = initial.clone();
    fenced.epoch += 1;
    fenced.lifecycle = Lifecycle::Deleting;
    let fenced = directory.replace_deployment(fenced, initial.version).await.unwrap().applied().unwrap();
    drop(directory);

    assert!(MemoryDirectory::initialize(state.path()).is_err());
    let directory = MemoryDirectory::open(state.path()).unwrap();
    assert_eq!(directory.deployment_by_name("shop").await.unwrap(), Some(fenced.clone()));
    assert_eq!(directory.replace_deployment(initial.clone(), initial.version).await.unwrap(),
        rtdb_directory::Cas::Conflict(Some(fenced)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hosted_install_retries_recover_the_same_authority() {
    let cell = Cell::start(2, None).await;
    let id = uuid::Uuid::new_v4();
    let directory = cell.directory.as_ref();
    let (first, second) = tokio::join!(
        admin::install_deployment(directory, id, "shop", "acme", 3),
        admin::install_deployment(directory, id, "shop", "acme", 3),
    );
    let installed = first.unwrap();
    assert_eq!(second.unwrap(), installed);
    assert_eq!(installed.id, id.to_string());
    assert!(installed.linearizable_reads);
    assert!(admin::install_deployment(directory, uuid::Uuid::new_v4(), "shop", "acme", 3).await.is_err());
    assert!(admin::install_deployment(directory, id, "other", "acme", 3).await.is_err());
    let mut client = tokio_postgres::Config::new();
    client.host("127.0.0.1").port(cell.gateway.port()).user("xtdb").password("cell-test");
    client.dbname(&format!("shop@{}", uuid::Uuid::new_v4().simple()));
    assert!(client.connect(NoTls).await.is_err());
    assert!(cell.holder("shop").await.is_none());
    let authority = id.simple().to_string();
    client.dbname(&format!("shop@{authority}"));
    let (client, connection) = client.connect(NoTls).await.unwrap();
    assert_eq!(connection.parameter("rtdb_installation_id"), Some(authority.as_str()));
    tokio::spawn(connection);
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    let holder = cell.holder("shop").await.unwrap();
    admin::move_deployment(directory, "shop", &cell.node(1 - holder).lease.id(), &control()).await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    drop(client);
    assert_eq!(admin::install_deployment(directory, id, "shop", "acme", 3).await.unwrap().id, installed.id);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pgwire_holder_refuses_a_gateway_with_a_stale_installation_route() {
    let cell = Cell::start(1, None).await;
    let serving = uuid::Uuid::new_v4();
    admin::install_deployment(cell.directory.as_ref(), serving, "shop", "acme", 3).await.unwrap();
    assert!(matches!(cell.node(0).agent.activate("shop").await.unwrap(), Response::Held { .. }));

    let stale = uuid::Uuid::new_v4();
    let cached: Arc<dyn Directory> = MemoryDirectory::new();
    for member in cell.directory.members().await.unwrap() { cached.register_member(member).await.unwrap(); }
    let mut stale_record = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    stale_record.id = stale.to_string();
    stale_record.lineage = stale.to_string();
    cached.create_deployment(stale_record).await.unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let options = rtdb_multi::gateway::GatewayOptions { capabilities: Some(secret()), ..Default::default() };
    let gateway = tokio::spawn(rtdb_multi::gateway::serve(listener, cached, options, cancel.clone()));

    let mut client = tokio_postgres::Config::new();
    client.host("127.0.0.1").port(address.port()).user("xtdb").password("cell-test");
    client.dbname(&format!("shop@{}", stale.simple()));
    let Err(error) = client.connect(NoTls).await else { panic!("the stale route accepted a different installation"); };
    assert_eq!(error.code(), Some(&tokio_postgres::error::SqlState::INVALID_CATALOG_NAME));

    cancel.cancel();
    gateway.await.unwrap().unwrap();
    cell.stop().await;
}

#[tokio::test]
async fn failed_disposal_reuses_the_fence_on_retry() {
    let directory = MemoryDirectory::new();
    let data = tempfile::tempdir().unwrap();
    let storage = CellStorage::Local { root: data.path().join("objects") };
    let keys = LocalKeyService::new(data.path().join("keys")).unwrap();
    let installation = directory.create_deployment(
        DeploymentRecord::new("shop", "acme", vec![MemberId::fresh()], 0),
    ).await.unwrap();

    let error = admin::finish_disposal(directory.as_ref(), &storage, &keys, &installation.id, 4 << 20).await.unwrap_err();
    assert_eq!(error.code, "rtdb.multi/disposal-unaccepted");
    let accepted = admin::begin_disposal(directory.as_ref(), &installation.id).await.unwrap();
    assert_eq!(accepted.epoch, installation.epoch + 1);

    for _ in 0..2 {
        let error = admin::finish_disposal(directory.as_ref(), &storage, &keys, &installation.id, 4 << 20).await.unwrap_err();
        assert_eq!(error.code, "rtdb.multi/administration-ensemble");
        let pending = directory.deployment(&installation.id).await.unwrap().unwrap();
        assert_eq!(pending.lifecycle, Lifecycle::Deleting);
        assert_eq!(pending.epoch, installation.epoch + 1);
        assert_eq!(pending.version, installation.version + 1);
        assert!(pending.disposal.is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn placement_status_counts_general_and_isolation_demand_separately() {
    let cell = Cell::start_with(vec![MemberPool::General, MemberPool::Isolation], None).await;
    let directory = cell.directory.as_ref();
    for name in ["shop", "isolated", "parked"] { admin::create_deployment(directory, name, "acme", 3).await.unwrap(); }
    let mut isolated = directory.deployment_by_name("isolated").await.unwrap().unwrap();
    isolated.lifecycle = rtdb_directory::Lifecycle::Quarantined;
    let version = isolated.version;
    assert!(directory.replace_deployment(isolated, version).await.unwrap().applied().is_some());
    cell.node(0).agent.activate("shop").await.unwrap();
    cell.node(1).agent.activate("isolated").await.unwrap();

    let report = admin::placement_status(directory, 1).await.unwrap();
    assert_eq!((report.general.eligible, report.general.holdings, report.general.recommended_nodes), (1, 1, 1));
    assert_eq!((report.isolation.eligible, report.isolation.holdings, report.isolation.recommended_nodes), (1, 1, 1));
    assert_eq!(report.logkeepers, 3);
    assert_eq!(report.compaction_workers, 0);
    assert_eq!(report.nodes.values().sum::<usize>(), 2);
    assert_eq!(cell.holder("parked").await, None);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auditing_a_pristine_deployment_does_not_allocate_its_encryption_key() {
    let cell = Cell::start(0, None).await;
    let deployment = admin::create_deployment(cell.directory.as_ref(), "pristine", "acme", 3).await.unwrap();
    let keys = cell.keys();
    assert!(keys.existing(&deployment.id).await.is_err());
    let report = admin::audit(cell.directory.as_ref(), &keys, "pristine", 4 << 20).await.unwrap();
    assert!(report.violations.is_empty());
    assert!(report.replica_digests.is_empty());
    assert_eq!(report.required_replica_quorum, 2);
    assert_eq!(report.replica_windows.len(), 3);
    assert!(keys.existing(&deployment.id).await.is_err());
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acceptance_audit_matches_one_exact_entry_after_quorum_recovery() {
    use rtdb_format::{envelope::{ReplicaBody, ReplicaEnvelope}, log::proto};
    use rtdb_storage::{Error, log::{GuardedAppend, Log, LogOffset}};
    use rtdb_types::{JournalEndpoint, PlacedAttemptScope, SourcePosition, Term, TermSequence, Timestamp};

    let cell = Cell::start(0, None).await;
    let directory = cell.directory.as_ref();
    let deployment = admin::create_deployment(directory, "strict", "acme", 3).await.unwrap();
    let authority = uuid::Uuid::parse_str(&deployment.id).unwrap();
    let scope = deployment.stream("replica");
    let stream = rtdb_logkeeper::StreamConfig {
        servers: cell.logkeepers.iter().filter(|keeper| deployment.ensemble.contains(&keeper.lease.id()))
            .map(|keeper| JournalEndpoint::Member { address: keeper.address.to_string(), journal: keeper.lease.id().0 })
            .collect(),
        stream: scope.clone(), write_quorum: 2, maximum_message_bytes: 4 << 20,
    };
    let keys = cell.keys();
    keys.key(&deployment.key_id()).await.unwrap();
    let placed = PlacedAttemptScope::new(&scope, 1).unwrap().issue()
        .identify(SourcePosition(3), Some("owner")).unwrap();
    let body = ReplicaEnvelope { term: Term(1), term_sequence: Some(TermSequence(1)),
        message: ReplicaBody::ResolvedTx(proto::ResolvedTx {
            tx_id: Some(3), src_msg_id: Some(3), system_time_micros: Some(100), committed: Some(true),
            placed_attempt: Some(placed.encode().to_vec().into()),
            acceptance_stage: Some(proto::AcceptanceStage {
                clock_upper_micros: Some(120), cutoff_micros: Some(150),
                journal_cutoff_micros: Some(i64::MAX), clock_certificate_sha256: Some(vec![7; 32]),
            }),
            ..Default::default()
        }),
    };
    let broadcast = rtdb_logkeeper::open_writer(stream.clone(), 1, Default::default()).await.unwrap();
    let writer = rtdb_multi::logs::SealedLog::new(broadcast, &keys, &deployment.key_id(), &deployment.replica_seal_scope(), rtdb_storage::IoRegion::new()).await.unwrap();
    let cutoff = Timestamp(150);
    let staging = Timestamp(120);
    assert!(writer.maximum_guarded_append_bytes().is_some());
    assert_eq!(
        writer.maximum_timed_original_append_bytes(),
        writer.maximum_guarded_append_bytes()
    );
    for late in [118, 149, 200] {
        let clock = || Timestamp(late);
        let result = writer.enqueue_guarded(body.encode().into(), GuardedAppend::new(&clock, staging, cutoff, None)).await;
        assert!(matches!(result, Err(Error::GuardedAppendCutoff)));
        assert_eq!(writer.latest().unwrap(), LogOffset(-1));
    }
    let clock = || Timestamp(124);
    let journal_cutoff = Timestamp(i64::MAX);
    let physical = rtdb_types::TimedJournalCutoff::new(journal_cutoff, [7; 32]).unwrap();
    let durable = writer.enqueue_guarded(body.encode().into(), GuardedAppend::new(&clock, staging, cutoff, Some(physical)))
        .await.unwrap().durable().await.unwrap();
    assert_eq!(durable.offset, LogOffset(0));
    drop(writer);
    assert_eq!(rtdb_logkeeper::seal_recover_committed(&stream, 2).await.unwrap(), LogOffset(0));

    let report = admin::audit_acceptance_quorum(directory, &keys, authority, deployment.incarnation,
        deployment.log_epoch, 0, 4 << 20).await.unwrap();
    assert_eq!(report.authority, authority);
    assert_eq!(report.replica_stream, scope);
    assert_eq!(report.required_replica_quorum, 2);
    assert_eq!(report.required_original_quorum, Some(2));
    assert_eq!(report.clock_certificate_sha256, Some("07".repeat(32)));
    assert_eq!(report.original_replica_stream.as_deref(), Some(scope.as_str()));
    let mut roster: Vec<_> = stream.servers.iter().map(|server| server.journal().unwrap().to_owned()).collect();
    roster.sort_unstable();
    assert_eq!(report.original_journals, roster);
    assert_eq!(report.journal_cutoff_micros, Some(journal_cutoff.0));
    assert_eq!(report.acceptance.replica_offset, 0);
    assert_eq!(report.acceptance.source_position, 3);
    assert_eq!(report.acceptance.staging_clock_upper_micros, 120);
    assert_eq!(report.replica_append_sample_micros, 124);
    assert!(report.quorum_observed_clock_upper_micros > 124);
    assert!(report.journal_sync_observations.len() >= report.required_replica_quorum);
    let mut observed = report.journal_sync_observations.values().copied().collect::<Vec<_>>();
    observed.sort_unstable();
    assert_eq!(report.quorum_sync_clock_upper_micros, Some(observed[1]));
    assert!(observed[1] > 124);
    assert!(admin::audit_acceptance_quorum(directory, &keys, authority, deployment.incarnation,
        deployment.log_epoch + 1, 0, 4 << 20).await.is_err());
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_holding_flushes_then_keeps_serving_under_the_same_epoch() {
    let cell = Cell::start_with_idle_flush(Duration::from_millis(100)).await;
    let directory = cell.directory.as_ref();
    let deployment = admin::create_deployment(directory, "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 23)").await.unwrap();
    let original = directory.deployment(&deployment.id).await.unwrap().unwrap();
    drop(client);

    let flushed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = directory.deployment(&deployment.id).await.unwrap().unwrap();
            if current.checkpoint.is_some() { return current; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert_eq!((flushed.epoch, flushed.holder), (original.epoch, original.holder));
    let client = cell.connect("shop").await;
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["23"]);
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saturated_holder_declines_gateway_startup_without_stalling_existing_sessions() {
    use rtdb_multi::gateway::{GatewayOptions, serve};
    use tokio_util::sync::CancellationToken;

    let cell = Cell::start(1, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let mut clients = Vec::new();
    for _ in 0..pgwire_connections() { clients.push(cell.connect("shop").await); }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let shutdown = CancellationToken::new();
    let options = GatewayOptions { startup_timeout: Duration::from_millis(250), capabilities: Some(secret()), ..Default::default() };
    let gateway = tokio::spawn(serve(listener, cell.directory.clone(), options, shutdown.clone()));
    let connection_string = format!("host=127.0.0.1 port={port} user=xtdb password=cell-test dbname=shop");
    let Ok(Err(error)) = tokio::time::timeout(Duration::from_secs(3), tokio_postgres::connect(&connection_string, NoTls)).await
        else { panic!("the saturated holder did not report its startup deadline") };
    assert_eq!(error.code(), Some(&tokio_postgres::error::SqlState::CANNOT_CONNECT_NOW));
    assert_eq!(values(&clients[0], "SELECT 23").await, ["23"]);

    drop(clients.pop());
    let (client, connection) = tokio::time::timeout(Duration::from_secs(3), tokio_postgres::connect(&connection_string, NoTls)).await.unwrap().unwrap();
    let connection = tokio::spawn(connection);
    assert_eq!(values(&client, "SELECT 24").await, ["24"]);
    drop((client, clients));
    connection.await.unwrap().unwrap();
    shutdown.cancel();
    gateway.await.unwrap().unwrap();
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_connection_limits_leave_room_for_another_deployment_and_release_disconnected_sessions() {
    use rtdb_multi::gateway::{GatewayOptions, serve};
    use tokio_util::sync::CancellationToken;

    let cell = Cell::start(1, None).await;
    for name in ["shop", "market"] {
        admin::create_deployment(cell.directory.as_ref(), name, "acme", 3).await.unwrap();
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let stop = CancellationToken::new();
    let options = GatewayOptions {
        maximum_connections: 2, maximum_connections_per_deployment: 1, capabilities: Some(secret()),
        ..Default::default()
    };
    let gateway = tokio::spawn(serve(listener, cell.directory.clone(), options, stop.clone()));
    let connect = |name| {
        let uri = format!("host=127.0.0.1 port={port} user=xtdb password=cell-test dbname={name}");
        async move { tokio_postgres::connect(&uri, NoTls).await }
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        use tokio_postgres::error::SqlState;

        let (shop, shop_driver) = connect("shop").await.unwrap();
        let shop_driver = tokio::spawn(shop_driver);
        let Err(full) = connect("shop").await else { panic!("a second connection to shop was admitted"); };
        assert_eq!(full.code(), Some(&SqlState::TOO_MANY_CONNECTIONS));
        let (market, market_driver) = loop {
            match connect("market").await {
                Ok(connected) => break connected,
                Err(error) if error.code().is_none() => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(error) => panic!("market cannot use the gateway's other connection: {error}"),
            }
        };
        let market_driver = tokio::spawn(market_driver);
        let Err(full) = connect("shop").await else { panic!("a third connection to the gateway was admitted"); };
        assert_eq!(full.code(), None);
        assert_eq!(values(&market, "SELECT 23").await, ["23"]);

        drop(shop);
        shop_driver.await.unwrap().unwrap();
        let (shop, driver) = loop {
            match connect("shop").await {
                Ok(connected) => break connected,
                Err(error) if error.code().is_none() || error.code() == Some(&SqlState::TOO_MANY_CONNECTIONS) => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(error) => panic!("shop cannot reconnect: {error}"),
            }
        };
        let driver = tokio::spawn(driver);
        assert_eq!(values(&shop, "SELECT 24").await, ["24"]);
        drop((shop, market));
        driver.await.unwrap().unwrap();
        market_driver.await.unwrap().unwrap();
    }).await.expect("another deployment and a subsequent connection can use available capacity");
    stop.cancel();
    gateway.await.unwrap().unwrap();
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_engine_pin_constrains_claims_and_moves_until_explicitly_cleared() {
    let mut cell = Cell::start(1, None).await;
    let newer = cell.add_node_with_engine(Some("next-image".into())).await;
    let new_member = cell.node(newer).lease.id();
    let old_engine = cell.node(0).lease.record().engine_version;
    let directory = cell.directory.as_ref();
    admin::create_deployment(directory, "shop", "acme", 3).await.unwrap();
    admin::pin_engine(directory, "shop", Some(&old_engine)).await.unwrap();
    assert!(cell.node(newer).agent.activate("shop").await.is_err());
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 23)").await.unwrap();
    assert_eq!(cell.holder("shop").await, Some(0));
    assert_eq!(admin::move_deployment(directory, "shop", &new_member, &control()).await.unwrap_err().code, "rtdb.multi/not-placeable");
    assert_eq!(admin::pin_engine(directory, "shop", Some("next-image")).await.unwrap_err().code, "rtdb.multi/engine-pin-in-use");

    for name in ["pinned-b", "pinned-c"] {
        admin::create_deployment(directory, name, "acme", 3).await.unwrap();
        admin::pin_engine(directory, name, Some(&old_engine)).await.unwrap();
        cell.node(0).agent.activate(name).await.unwrap();
    }
    assert_eq!(admin::rebalance(directory, 10, &control()).await.unwrap(), 0);
    assert!(admin::roll_engine(directory, "next-image").await.unwrap().is_empty());
    admin::pin_engine(directory, "shop", None).await.unwrap();
    assert_eq!(admin::rebalance(directory, 10, &control()).await.unwrap(), 1);
    assert_eq!(cell.holder("shop").await, Some(newer));
    admin::pin_engine(directory, "shop", Some("next-image")).await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["23"]);
    let old_member = cell.node(0).lease.id();
    assert!(admin::roll_engine(directory, "next-image").await.unwrap().is_empty());
    for name in ["pinned-b", "pinned-c"] { admin::pin_engine(directory, name, None).await.unwrap(); }
    admin::create_deployment(directory, "parked-old", "acme", 3).await.unwrap();
    admin::pin_engine(directory, "parked-old", Some(&old_engine)).await.unwrap();
    assert_eq!(cell.holder("parked-old").await, None);
    assert!(admin::roll_engine(directory, "next-image").await.unwrap().is_empty());
    admin::pin_engine(directory, "parked-old", None).await.unwrap();
    assert_eq!(admin::roll_engine(directory, "next-image").await.unwrap(), [old_member.clone()]);
    let draining = directory.member(&old_member).await.unwrap().unwrap();
    assert_eq!(draining.state, rtdb_directory::MemberState::Draining);
    assert_eq!(draining.drain_target_engine.as_deref(), Some("next-image"));
    tokio::time::timeout(Duration::from_secs(5), async {
        while cell.holder("pinned-b").await != Some(newer) || cell.holder("pinned-c").await != Some(newer) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_version_rollout_keeps_a_target_for_a_pinned_holder_already_draining() {
    let mut cell = Cell::start(2, None).await;
    cell.add_node_with_engine(Some("next-image".into())).await;
    let old = cell.node(0).lease.record();
    let spare = cell.node(1).lease.id();
    let directory = cell.directory.as_ref();
    admin::create_deployment(directory, "shop", "acme", 3).await.unwrap();
    admin::pin_engine(directory, "shop", Some(&old.engine_version)).await.unwrap();
    cell.node(0).agent.activate("shop").await.unwrap();
    admin::drain(directory, &old.id).await.unwrap();
    assert!(admin::roll_engine(directory, "next-image").await.unwrap().is_empty());
    assert_eq!(directory.member(&spare).await.unwrap().unwrap().state, rtdb_directory::MemberState::Eligible);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_engine_pin_can_advance_after_its_old_holder_dies() {
    let mut cell = Cell::start(1, None).await;
    let newer = cell.add_node_with_engine(Some("next-image".into())).await;
    let old = cell.node(0).lease.record();
    let directory = std::sync::Arc::clone(&cell.directory);
    admin::create_deployment(directory.as_ref(), "shop", "acme", 3).await.unwrap();
    admin::pin_engine(directory.as_ref(), "shop", Some(&old.engine_version)).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 23)").await.unwrap();
    cell.nodes[0].take().unwrap().kill().await.unwrap();
    let mut dead = directory.member(&old.id).await.unwrap().unwrap();
    dead.state = rtdb_directory::MemberState::Dead;
    let version = dead.version;
    assert!(directory.replace_member(dead, version).await.unwrap().applied().is_some());
    admin::pin_engine(directory.as_ref(), "shop", Some("next-image")).await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["23"]);
    assert_eq!(cell.holder("shop").await, Some(newer));
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_engine_pin_without_a_compatible_destination_prevents_cell_move_fencing() {
    use rtdb_multi::cells::{CellMap, MemoryCellMap};

    let map = MemoryCellMap::new();
    let east = Cell::start_in("east", map.clone(), 1).await;
    let mut west = Cell::start_in("west", map.clone(), 0).await;
    west.add_node_with_engine(Some("next-image".into())).await;
    let (east_storage, west_storage, east_keys, west_keys) = (east.storage(), west.storage(), east.keys(), west.keys());
    let from = admin::CellHandle { name: "east", directory: &east.directory, storage: &east_storage, keys: &east_keys, broker: None, maximum_deployments: 10 };
    let to = admin::CellHandle { name: "west", directory: &west.directory, storage: &west_storage, keys: &west_keys, broker: None, maximum_deployments: 10 };
    admin::create_in_cells(map.as_ref(), std::slice::from_ref(&from), "shop", "acme", 3).await.unwrap();
    let client = east.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 23)").await.unwrap();
    let engine = east.node(0).lease.record().engine_version;
    let before = admin::pin_engine(east.directory.as_ref(), "shop", Some(&engine)).await.unwrap();
    assert_eq!(admin::move_cell(map.as_ref(), &from, &to, "shop", 4 << 20, None).await.unwrap_err().code, "rtdb.multi/engine-pin-no-target");
    let after = east.directory.deployment(&before.id).await.unwrap().unwrap();
    assert_eq!((after.epoch, after.holder, after.lifecycle), (before.epoch, before.holder, before.lifecycle));
    assert_eq!(map.cell_of("shop").await.unwrap().as_deref(), Some("east"));
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["23"]);
    drop(client);
    east.stop().await;
    west.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deployment_is_served_through_the_gateway_and_survives_its_holder_dying() {
    let mut cell = Cell::start(2, None).await;
    let history = uuid::Uuid::new_v4();
    admin::install_deployment(cell.directory.as_ref(), history, "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let basis = values(&client, "SELECT _system_from FROM orders WHERE _id = 1").await.remove(0);
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    let submitted = values(&client, "SHOW latest_submitted_tx").await.remove(0);
    let barrier = client.simple_query("SHOW rtdb_hosted_source_barrier").await.unwrap();
    let receipt = barrier.iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
    }).unwrap();
    assert_eq!(receipt.len(), 3);
    assert_eq!(receipt.get("rtdb_hosted_source_barrier"), Some(history.to_string().as_str()));
    assert_eq!(receipt.get("tx_id"), Some(submitted.as_str()));
    let ordered_time = values(&client, "SELECT _system_from FROM orders WHERE _id = 2").await.remove(0);
    assert_eq!(receipt.get("system_time"), Some(ordered_time.as_str()));
    drop(client);

    let first = cell.holder("shop").await.expect("the first connection placed the deployment");
    let epoch = cell.directory.deployment_by_name("shop").await.unwrap().unwrap().epoch;
    cell.nodes[first].take().unwrap().kill().await.unwrap();

    let client = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&client, "SHOW rtdb_hosted_source_barrier").await, [history.to_string()]);
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10", "20"], "nothing acknowledged is lost when the holder dies");
    let exact = format!("SELECT total FROM orders FOR SYSTEM_TIME AS OF TIMESTAMP '{basis}'");
    assert_eq!(values(&client, &exact).await, ["10"]);
    client.simple_query("INSERT INTO orders (_id, total) VALUES (3, 30)").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10", "20", "30"]);
    let record = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert!(record.epoch > epoch, "the survivor holds it under a later epoch");
    assert_ne!(cell.holder("shop").await, Some(first));
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hosted_source_barrier_refuses_a_missing_directory_checkpoint() {
    let cell = Cell::start(1, None).await;
    let history = uuid::Uuid::new_v4();
    let directory = cell.directory.as_ref();
    admin::install_deployment(directory, history, "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    assert_eq!(values(&client, "SHOW rtdb_hosted_source_barrier").await, [history.to_string()]);
    assert_eq!(values(&client, "SHOW rtdb_hosted_source_owner").await, [history.to_string()]);
    client.batch_execute("BEGIN READ ONLY").await.unwrap();
    assert_eq!(values(&client, "SHOW rtdb_hosted_source_owner").await, [history.to_string()]);
    client.batch_execute("COMMIT").await.unwrap();

    let mut record = directory.deployment(&history.to_string()).await.unwrap().unwrap();
    let expected_version = record.version;
    record.checkpoint = Some(rtdb_directory::Checkpoint { block: 7, boundary: 11, source: 11, system_time: 100, root_sha256: None });
    assert!(matches!(directory.replace_deployment(record, expected_version).await.unwrap(), rtdb_directory::Cas::Applied(_)));

    let refusal = client.simple_query("SHOW rtdb_hosted_source_barrier").await.unwrap_err();
    assert!(refusal.as_db_error().unwrap().message().contains("has not recovered the directory's durable checkpoint"));
    let refusal = client.simple_query("SHOW rtdb_hosted_source_owner").await.unwrap_err();
    assert!(refusal.as_db_error().unwrap().message().contains("has not recovered the directory's durable checkpoint"));
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partitioned_holder_cannot_commit_once_another_member_takes_over() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);

    let zombie_index = cell.holder("shop").await.unwrap();
    let zombie = cell.nodes[zombie_index].as_ref().unwrap();
    let direct = connect(&zombie.pgwire_address.to_string(), "shop").await;
    let zombie_id = zombie.lease.id();
    zombie.stop_renewing();
    while cell.directory.member(&zombie_id).await.unwrap().unwrap().lease_expires_at_ms > rtdb_directory::now_ms() { tokio::time::sleep(Duration::from_millis(50)).await; }

    let client = connect_eventually(&cell, "shop").await;
    assert_ne!(cell.holder("shop").await, Some(zombie_index), "the other node took over");
    let refused = direct.simple_query("INSERT INTO orders (_id, total) VALUES (99, 990)").await;
    assert!(refused.is_err(), "a deposed holder's write does not commit: {refused:?}");
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10", "20"], "the deposed holder's write is visible nowhere");
    drop(client);
    drop(direct);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_activation_parks_a_cold_holding_under_execution_memory_pressure() {
    let cell = Cell::start_watching(vec![MemberPool::General], None, Some(Duration::from_secs(60))).await;
    let node = cell.node(0);
    let memory = node.server.node().memory();
    admin::create_deployment(cell.directory.as_ref(), "cold", "acme", 3).await.unwrap();
    node.agent.activate("cold").await.unwrap();
    let cold = common::connect(&node.pgwire_address.to_string(), "cold").await;
    cold.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(cold);
    assert_eq!(cell.holder("cold").await, Some(0));
    admin::create_deployment(cell.directory.as_ref(), "next", "acme", 3).await.unwrap();
    let high_water = memory.limit_bytes() - memory.limit_bytes() / 5;
    let pressure = memory.reserve(high_water).unwrap();

    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let refused = node.agent.activate("next").await.unwrap_err();
            assert_eq!(refused.code, "rtdb.multi/node-under-pressure");
            assert_eq!(cell.holder("next").await, None);
            if cell.holder("cold").await.is_none() { break; }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("the cold holding parks during activation retries");
    drop(pressure);
    let next = cell.connect("next").await;
    assert_eq!(cell.holder("next").await, Some(0));
    drop(next);
    let cold = cell.connect("cold").await;
    assert_eq!(values(&cold, "SELECT total FROM orders").await, ["10"]);
    drop(cold);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_reclamation_respects_a_holdings_minimum_residency() {
    let cell = Cell::start_with_minimum_residency(Duration::from_secs(60)).await;
    let node = cell.node(0);
    for name in ["recent", "next"] {
        admin::create_deployment(cell.directory.as_ref(), name, "acme", 3).await.unwrap();
    }
    assert!(matches!(node.agent.activate("recent").await.unwrap(), Response::Held { .. }));
    let memory = node.server.node().memory();
    let pressure = memory.reserve(memory.limit_bytes() - memory.limit_bytes() / 5).unwrap();
    let error = node.agent.activate("next").await.unwrap_err();
    assert_eq!(error.code, "rtdb.multi/node-under-pressure");
    assert_eq!(cell.holder("recent").await, Some(0));
    assert_eq!(cell.holder("next").await, None);
    node.agent.park("recent").await.unwrap();
    assert_eq!(cell.holder("recent").await, None);
    drop(pressure);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_claims_share_a_node_activation_limit_while_warm_access_continues() {
    let cell = Cell::start_rate_limited(std::num::NonZeroUsize::MIN).await;
    for name in ["first", "second"] {
        admin::create_deployment(cell.directory.as_ref(), name, "acme", 3).await.unwrap();
    }
    let agent = &cell.node(0).agent;
    let (first, second) = tokio::join!(agent.activate("first"), agent.activate("second"));
    let (warm, cold, refusal) = match (first, second) {
        (Ok(_), Err(error)) => ("first", "second", error),
        (Err(error), Ok(_)) => ("second", "first", error),
        results => panic!("expected one admitted activation: {results:?}"),
    };
    assert_eq!(refusal.code, "rtdb.multi/activation-rate-limited");
    assert!(matches!(agent.activate(warm).await.unwrap(), Response::Held { .. }));
    assert_eq!(cell.holder(cold).await, None);
    let client = tokio::time::timeout(Duration::from_secs(5), cell.connect(cold)).await.unwrap();
    assert_eq!(cell.holder(cold).await, Some(0));
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_deployment_parks_and_wakes_on_its_next_connection() {
    let cell = Cell::start(1, Some(Duration::from_millis(800))).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);

    let mut parked = false;
    for _ in 0..50 {
        let record = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
        if record.holder.is_none() && record.epoch > 0 { parked = true; break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(parked, "an untouched deployment is parked: no holder");
    assert!(cell.nodes[0].as_ref().unwrap().agent.held().is_empty(), "and its node runs nothing for it");

    let client = cell.connect("shop").await;
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"], "waking replays nothing lost");
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deployments_are_isolated_from_each_other() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    admin::create_deployment(cell.directory.as_ref(), "blog", "globex", 3).await.unwrap();
    let shop = cell.connect("shop").await;
    let blog = cell.connect("blog").await;
    shop.simple_query("INSERT INTO items (_id, label) VALUES (1, 'shop')").await.unwrap();
    blog.simple_query("INSERT INTO items (_id, label) VALUES (1, 'blog')").await.unwrap();
    assert_eq!(values(&shop, "SELECT label FROM items").await, ["shop"]);
    assert_eq!(values(&blog, "SELECT label FROM items").await, ["blog"]);
    let unknown = tokio_postgres::connect(&format!("host=127.0.0.1 port={} user=xtdb password=cell-test dbname=nowhere", cell.gateway.port()), NoTls).await;
    assert!(unknown.is_err(), "an unknown deployment is refused at the gateway");
    drop((shop, blog));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drained_node_hands_its_deployments_to_the_rest_of_the_cell() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);

    let drained = cell.holder("shop").await.unwrap();
    let initial = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    admin::drain(cell.directory.as_ref(), &cell.nodes[drained].as_ref().unwrap().lease.id()).await.unwrap();
    let target = 1 - drained;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = cell.directory.deployment(&initial.id).await.unwrap().unwrap();
            if current.holder.as_ref() == Some(&cell.node(target).lease.id()) && cell.node(target).agent.held().contains_key("shop") {
                assert!(current.epoch > initial.epoch);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert!(cell.node(drained).agent.held().is_empty());

    let client = cell.connect("shop").await;
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    let holder = cell.holder("shop").await.unwrap();
    assert_eq!(holder, target);
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_node_keeps_a_recently_used_deployment_until_a_target_is_available() {
    let cell = Cell::start(1, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let node = cell.node(0);
    admin::drain(cell.directory.as_ref(), &node.lease.id()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while node.lease.record().state != rtdb_directory::MemberState::Draining {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    node.agent.reconcile().await;
    client.simple_query("UPDATE orders SET total = total + 1 WHERE _id = 1").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["11"]);
    assert_eq!(cell.holder("shop").await, Some(0));
    drop(client);
    cell.stop().await;
}



async fn submitted_attempt(client: &tokio_postgres::Client) -> (i64, String) {
    client.simple_query("SHOW LATEST_SUBMITTED_TX").await.unwrap().into_iter().find_map(|message| {
        let tokio_postgres::SimpleQueryMessage::Row(row) = message else { return None; };
        assert_eq!(row.get("committed"), None);
        Some((row.get("tx_id").unwrap().parse().unwrap(), row.get("await_token").unwrap().to_owned()))
    }).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reused_intake_position_cannot_redeem_the_sealed_writers_receipt() {
    use rtdb_format::basis::encode_tx_basis;
    use rtdb_types::{TxBasis, TxId};

    struct AnotherPrincipal;
    #[async_trait::async_trait]
    impl rtdb_node::SessionIdentity for AnotherPrincipal {
        fn user(&self) -> Option<&str> { Some("another-user") }
        async fn revalidate(&self, _: rtdb_types::Timestamp) -> Result<()> { Ok(()) }
        fn authorize_database(&self, _: &str, _: rtdb_node::DatabaseAccess) -> Result<()> { Ok(()) }
    }

    let mut cell = Cell::start_durable(2).await;
    let shop = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let via_gateway = cell.connect("shop").await;
    via_gateway.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let holder = cell.holder("shop").await.unwrap();
    let writer = connect(&cell.node(holder).pgwire_address.to_string(), "shop").await;
    writer.simple_query("BEGIN READ WRITE; UPDATE orders SET total = 20 WHERE _id = 1").await.unwrap();

    let old = cell.directory.deployment(&shop.id).await.unwrap().unwrap();
    let config = rtdb_logkeeper::StreamConfig {
        servers: cell.logkeepers.iter().filter(|keeper| old.ensemble.contains(&keeper.lease.id())).map(|keeper| keeper.address.to_string().into()).collect(),
        stream: old.stream("replica"), write_quorum: 2, maximum_message_bytes: 4 << 20,
    };
    rtdb_logkeeper::seal(&config, old.epoch + 1).await.unwrap();
    writer.simple_query("COMMIT ASYNC").await.unwrap();
    let (lost_id, lost) = submitted_attempt(&writer).await;

    drop((writer, via_gateway));
    cell.nodes[holder].take().unwrap().kill().await.unwrap();
    let successor = connect_eventually(&cell, "shop").await;
    let new = cell.directory.deployment(&shop.id).await.unwrap().unwrap();
    assert!(new.epoch > old.epoch);
    successor.simple_query("BEGIN READ WRITE; UPDATE orders SET total = 30 WHERE _id = 1; COMMIT ASYNC").await.unwrap();
    let (new_id, pending) = submitted_attempt(&successor).await;
    assert_eq!(lost_id, new_id);
    assert_ne!(lost, pending);
    assert_eq!(values(&successor, "SELECT total FROM orders").await, ["30"]);
    let applied = values(&successor, "SHOW AWAIT_TOKEN").await.pop().unwrap();
    assert_ne!(applied, pending);
    assert_eq!(submitted_attempt(&successor).await, (new_id, pending));

    let reader = cell.connect("shop").await;
    reader.simple_query(&format!("SET AWAIT_TOKEN TO '{lost}'")).await.unwrap();
    let error = reader.simple_query("SELECT total FROM orders").await
        .expect_err("The sealed writer's receipt cannot return the successor's row");
    assert!(error.as_db_error().unwrap().message().contains("closed writer epoch has no outcome"));
    let numeric = encode_tx_basis(&TxBasis { databases: BTreeMap::from([("shop".into(), vec![TxId(new_id)])]) });
    reader.simple_query(&format!("SET AWAIT_TOKEN TO '{numeric}'")).await.unwrap();
    let error = reader.simple_query("SELECT total FROM orders").await.unwrap_err();
    assert!(error.as_db_error().unwrap().message().contains("Await token does not belong"));
    reader.simple_query(&format!("SET AWAIT_TOKEN TO '{applied}'")).await.unwrap();
    assert_eq!(values(&reader, "SELECT total FROM orders").await, ["30"]);

    let holder = cell.holder("shop").await.unwrap();
    let mut another = cell.node(holder).server.node().session("shop", Arc::new(AnotherPrincipal)).unwrap();
    another.state_mut().set_await_token(Some(applied.clone())).unwrap();
    assert_eq!(another.state_mut().read_basis().await.unwrap_err().code, "rtdb/invalid-await-token");

    let mut forged = applied.clone().into_bytes();
    let index = forged.len() / 2;
    forged[index] = if forged[index] == b'A' { b'B' } else { b'A' };
    let forged = String::from_utf8(forged).unwrap();
    reader.simple_query(&format!("BEGIN READ ONLY WITH (AWAIT_TOKEN = '{forged}')")).await.unwrap();
    let error = reader.simple_query("SELECT total FROM orders").await.unwrap_err();
    assert!(error.as_db_error().unwrap().message().contains("Await token is invalid"));
    reader.simple_query("ROLLBACK").await.unwrap();

    drop((reader, successor, another));
    cell.stop().await;

    let other_installation = Cell::start_durable(1).await;
    let other_shop = admin::create_deployment(other_installation.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    assert_ne!(other_shop.id, shop.id);
    let fresh = other_installation.connect("shop").await;
    fresh.simple_query(&format!("SET AWAIT_TOKEN TO '{applied}'")).await.unwrap();
    let error = fresh.simple_query("SELECT 1").await.unwrap_err();
    assert!(error.as_db_error().unwrap().message().contains("Await token is invalid"));
    drop(fresh);
    other_installation.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn placed_attempt_outcomes_keep_their_exact_identity_through_a_checkpoint() {
    use rtdb_format::envelope::{ReplicaBody, ReplicaEnvelope};
    use rtdb_storage::log::{Log, LogOffset};
    use rtdb_types::{PlacedAttempt, PlacedAttemptScope, SourcePosition};

    let cell = Cell::start_durable(2).await;
    let deployment = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let writer = cell.connect("shop").await;
    writer.simple_query("BEGIN READ WRITE; INSERT INTO orders (_id, total) VALUES (1, 10); COMMIT ASYNC").await.unwrap();
    let (_, provisional) = submitted_attempt(&writer).await;
    assert_eq!(values(&writer, "SELECT total FROM orders").await, ["10"]);
    assert!(writer.simple_query("ASSERT FALSE").await.is_err());

    let record = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
    let stream = record.stream("replica");
    let scope = PlacedAttemptScope::new(&stream, record.epoch).unwrap();
    let next_scope = PlacedAttemptScope::new(&stream, record.epoch + 1).unwrap();
    let config = rtdb_logkeeper::StreamConfig {
        servers: cell.logkeepers.iter().filter(|keeper| record.ensemble.contains(&keeper.lease.id())).map(|keeper| keeper.address.to_string().into()).collect(),
        stream, write_quorum: 2, maximum_message_bytes: 4 << 20,
    };
    let replica = rtdb_logkeeper::open_reader(config.clone(), Default::default()).await.unwrap();
    let replica = rtdb_multi::logs::SealedLog::new(replica, &cell.keys(), &record.key_id(), &record.replica_seal_scope(), rtdb_storage::IoRegion::new()).await.unwrap();
    let mut tail = replica.tail(LogOffset(-1)).await.unwrap();
    let outcomes = tokio::time::timeout(Duration::from_secs(5), async {
        let mut outcomes = Vec::new();
        while outcomes.len() < 2 {
            for record in tail.poll(Duration::from_millis(50)).await.unwrap() {
                let Some(ReplicaEnvelope { message: ReplicaBody::ResolvedTx(tx), .. }) = ReplicaEnvelope::decode(&record.payload).unwrap() else { continue; };
                let Some(encoded) = tx.placed_attempt.as_deref() else { continue; };
                let position = SourcePosition(tx.src_msg_id.unwrap());
                assert_eq!(tx.tx_id, Some(position.0));
                let attempt = PlacedAttempt::decode(encoded).unwrap();
                assert!(attempt.matches(scope, position, Some("xtdb")));
                assert!(!attempt.matches(next_scope, position, Some("xtdb")));
                assert!(tx.table_data.contains_key("xt/placed_attempt_outcomes"));
                outcomes.push((attempt.outcome_key(), tx.committed.unwrap()));
            }
        }
        outcomes
    }).await.unwrap();
    assert_eq!(outcomes.iter().map(|(_, committed)| *committed).collect::<Vec<_>>(), [true, false]);
    let mut expected: Vec<String> = outcomes.into_iter().map(|(key, _)| key).collect();
    expected.sort();
    let receipt_query = "SELECT _id FROM xt.placed_attempt_outcomes";
    assert_eq!(values(&writer, receipt_query).await, expected);

    let holder = cell.holder("shop").await.unwrap();
    cell.node(holder).agent.flush("shop").await.unwrap();
    drop((writer, tail, replica));
    cell.node(holder).agent.park("shop").await.unwrap();
    let parked = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
    let boundary = parked.checkpoint.unwrap().boundary;
    rtdb_logkeeper::trim(&config, LogOffset(boundary)).await.unwrap();
    let reopened = cell.connect("shop").await;
    let successor = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
    assert!(successor.epoch > record.epoch);
    assert!(successor.checkpoint.is_some());
    reopened.simple_query(&format!("SET AWAIT_TOKEN TO '{provisional}'")).await.unwrap();
    assert_eq!(values(&reopened, "SELECT total FROM orders").await, ["10"]);
    assert_ne!(values(&reopened, "SHOW AWAIT_TOKEN").await, [provisional]);
    assert_eq!(values(&reopened, receipt_query).await, expected);
    drop(reopened);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_linearizable_subscriber_refuses_to_pin_a_basis_after_its_ensemble_is_sealed() {
    let cell = Cell::start_durable(2).await;
    let deployment = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    admin::set_linearizable_reads(cell.directory.as_ref(), "shop", true).await.unwrap();
    admin::set_read_width(cell.directory.as_ref(), "shop", 2).await.unwrap();
    let writer = cell.connect("shop").await;
    writer.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let holder = cell.holder("shop").await.unwrap();
    let replica = cell.node(1 - holder);
    replica.agent.subscribe("shop").await.unwrap();
    let reader = connect(&replica.pgwire_address.to_string(), "shop").await;
    assert_eq!(values(&reader, "SELECT total FROM orders").await, ["10"]);

    let record = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
    let config = rtdb_logkeeper::StreamConfig {
        servers: cell.logkeepers.iter().filter(|keeper| record.ensemble.contains(&keeper.lease.id())).map(|keeper| keeper.address.to_string().into()).collect(),
        stream: record.stream("replica"), write_quorum: 2, maximum_message_bytes: 4 << 20,
    };
    rtdb_logkeeper::seal(&config, record.epoch + 1).await.unwrap();
    let error = reader.simple_query("SELECT total FROM orders").await.unwrap_err();
    assert!(error.as_db_error().unwrap().message().contains("could not establish a fresh snapshot"));
    drop((reader, writer));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_gateway_retries_an_implicit_read_on_the_holder_when_the_subscriber_cannot_confirm() {
    let cell = Cell::start_without_log_fsync(2).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    admin::set_linearizable_reads(cell.directory.as_ref(), "shop", true).await.unwrap();
    admin::set_read_width(cell.directory.as_ref(), "shop", 2).await.unwrap();
    let writer = cell.connect("shop").await;
    writer.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let holder = cell.holder("shop").await.unwrap();
    let reader_config = || {
        let mut config = tokio_postgres::Config::new();
        config.host("127.0.0.1").port(cell.gateway.port()).user("xtdb").password("cell-test").dbname("shop")
            .options("-c default_transaction_read_only=on");
        config
    };
    let (reader, connection) = reader_config().connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    assert_eq!(cell.node(1 - holder).agent.subscribed(), ["shop"]);
    assert_eq!(values(&reader, "SELECT total FROM orders").await, ["10"]);

    let (transaction, connection) = reader_config().connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    transaction.simple_query("BEGIN READ ONLY").await.unwrap();
    let error = transaction.simple_query("SELECT total FROM orders").await.unwrap_err();
    assert!(error.as_db_error().unwrap().message().contains("could not establish a fresh snapshot"));
    transaction.simple_query("ROLLBACK").await.unwrap();
    drop((reader, writer, transaction));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_only_sessions_spread_across_the_deployments_read_width() {
    let cell = Cell::start(3, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let writer = cell.connect("shop").await;
    writer.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let holder = cell.holder("shop").await.unwrap();
    cell.node(holder).agent.flush("shop").await.unwrap();
    admin::set_read_width(cell.directory.as_ref(), "shop", 3).await.unwrap();

    let mut readers = Vec::new();
    for _ in 0..2 {
        let (reader, connection) = tokio_postgres::Config::new().host("127.0.0.1").port(cell.gateway.port()).user("xtdb").password("cell-test").dbname("shop")
            .options("-c default_transaction_read_only=on").connect(NoTls).await.unwrap();
        tokio::spawn(connection);
        readers.push(reader);
    }
    for replica in (0..3).filter(|replica| *replica != holder) {
        assert_eq!(cell.node(replica).agent.subscribed(), ["shop"]);
    }
    for reader in &readers {
        assert_eq!(values(reader, "SELECT total FROM orders").await, ["10"]);
    }
    admin::set_read_width(cell.directory.as_ref(), "shop", 2).await.unwrap();
    let replicas = (0..3).filter(|replica| *replica != holder).collect::<Vec<_>>();
    for &replica in &replicas { cell.node(replica).agent.reconcile().await; }
    let (active, closed): (Vec<_>, Vec<_>) = replicas.into_iter()
        .partition(|&replica| cell.node(replica).agent.subscribed() == ["shop"]);
    assert_eq!((active.len(), closed.len()), (1, 1));
    assert_eq!(cell.node(closed[0]).agent.subscribe("shop").await.unwrap_err().code, "rtdb.multi/read-width");
    for reader in &readers { assert_eq!(values(reader, "SELECT total FROM orders").await, ["10"]); }
    drop((readers, writer));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_controller_widens_for_read_sessions_and_retires_replicas_when_they_leave() {
    let cell = Cell::start(3, None).await;
    let directory = cell.directory.clone();
    let deployment = admin::create_deployment(directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let writer = cell.connect("shop").await;
    writer.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let holder = cell.holder("shop").await.unwrap();
    cell.node(holder).agent.flush("shop").await.unwrap();

    let mut readers = Vec::new();
    for _ in 0..2 {
        let (reader, connection) = tokio_postgres::Config::new().host("127.0.0.1").port(cell.gateway.port())
            .user("xtdb").password("cell-test").dbname("shop").options("-c default_transaction_read_only=on")
            .connect(NoTls).await.unwrap();
        tokio::spawn(connection);
        readers.push(reader);
    }
    assert_eq!(directory.deployment(&deployment.id).await.unwrap().unwrap().read_width, 1);

    let stop = tokio_util::sync::CancellationToken::new();
    let options = admin::ControllerOptions {
        interval: Duration::from_millis(50),
        suspicion_probes: 0,
        control: control(),
        read_width: Some(rtdb_multi::read_width::ReadWidthOptions {
            sessions_per_node: std::num::NonZeroU64::new(1).unwrap(),
            max_width: std::num::NonZeroU32::new(3).unwrap(),
        }),
        ..Default::default()
    };
    let controller = admin::run_controller(directory.clone(), options, stop.clone());
    let wait_for_width = async |width| {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if directory.deployment(&deployment.id).await.unwrap().unwrap().read_width == width { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap_or_else(|_| panic!("read width did not reach {width}"));
    };
    let verify = async {
        wait_for_width(3).await;
        for _ in 0..2 {
            let (reader, connection) = tokio_postgres::Config::new().host("127.0.0.1").port(cell.gateway.port())
                .user("xtdb").password("cell-test").dbname("shop").options("-c default_transaction_read_only=on")
                .connect(NoTls).await.unwrap();
            tokio::spawn(connection);
            readers.push(reader);
        }
        for reader in &readers {
            assert_eq!(values(reader, "SELECT total FROM orders").await, ["10"]);
        }
        assert!((0..3).filter(|&node| node != holder).all(|node| cell.node(node).agent.subscribed() == ["shop"]));

        drop(readers);
        wait_for_width(1).await;
        for replica in (0..3).filter(|&node| node != holder) {
            cell.node(replica).agent.reconcile().await;
            assert!(cell.node(replica).agent.subscribed().is_empty());
        }
        assert_eq!(values(&writer, "SELECT total FROM orders").await, ["10"]);
        stop.cancel();
    };
    tokio::join!(controller, verify);
    drop(writer);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_only_session_is_served_by_a_log_fed_replica() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let writer = cell.connect("shop").await;
    writer.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    admin::set_read_width(cell.directory.as_ref(), "shop", 2).await.unwrap();
    let holder = cell.holder("shop").await.unwrap();

    let (reader, connection) = tokio_postgres::Config::new().host("127.0.0.1").port(cell.gateway.port()).user("xtdb").password("cell-test").dbname("shop")
        .options("-c default_transaction_read_only=on").connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    let replica = 1 - holder;
    assert_eq!(cell.node(replica).agent.subscribed(), ["shop"], "the read is routed to the node beside the holder");
    let mut seen = Vec::new();
    for _ in 0..100 {
        // Until the replica has replayed the table's creation, the table is unknown there.
        if reader.simple_query("SELECT total FROM orders").await.is_ok() { seen = values(&reader, "SELECT total FROM orders").await; }
        if seen == ["10"] { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(seen, ["10"], "the replica catches up from the log");
    assert!(reader.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.is_err(), "a replica refuses writes");
    drop((writer, reader));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_move_and_a_rebalance_hand_deployments_over_inside_their_pool() {
    let cell = Cell::start_with(vec![MemberPool::General, MemberPool::General, MemberPool::Isolation, MemberPool::Compaction], None).await;
    for name in ["a1", "a2", "a3", "a4"] {
        admin::create_deployment(cell.directory.as_ref(), name, "acme", 3).await.unwrap();
        let client = cell.connect(name).await;
        client.simple_query(&format!("INSERT INTO items (_id, label) VALUES (1, '{name}')")).await.unwrap();
    }
    let target = cell.node(0).lease.id();
    for name in ["a1", "a2", "a3", "a4"] { admin::move_deployment(cell.directory.as_ref(), name, &target, &control()).await.unwrap(); }
    assert_eq!(cell.node(0).agent.held().len(), 4, "every deployment moved to the target");
    let before = cell.directory.deployment_by_name("a1").await.unwrap().unwrap();
    for wrong_pool in [2, 3] {
        let error = admin::move_deployment(cell.directory.as_ref(), "a1", &cell.node(wrong_pool).lease.id(), &control()).await.unwrap_err();
        assert_eq!(error.code, "rtdb.multi/not-placeable");
        let current = cell.directory.deployment_by_name("a1").await.unwrap().unwrap();
        assert_eq!((current.epoch, current.holder.as_ref(), current.placement_move.as_ref()),
            (before.epoch, before.holder.as_ref(), before.placement_move.as_ref()));
    }
    let moved = admin::rebalance(cell.directory.as_ref(), 10, &control()).await.unwrap();
    assert_eq!(moved, 2);
    assert_eq!((cell.node(0).agent.held().len(), cell.node(1).agent.held().len()), (2, 2), "rebalancing evens the holdings out");
    for name in ["a1", "a2", "a3", "a4"] {
        let client = cell.connect(name).await;
        assert_eq!(values(&client, "SELECT label FROM items").await, [name], "nothing is lost in a move");
    }
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_rebalances_leave_the_first_reservation_in_charge_of_the_move() {
    let mut cell = Cell::start(1, None).await;
    for name in ["one", "two"] {
        admin::create_deployment(cell.directory.as_ref(), name, "acme", 3).await.unwrap();
        assert!(matches!(cell.node(0).agent.activate(name).await.unwrap(), Response::Held { .. }));
    }
    cell.add_node().await;
    let catalog = cell.node(0).server.node().catalog();
    let one = catalog.database("one").unwrap();
    let two = catalog.database("two").unwrap();
    let pauses = (one.pause_transactions().await.unwrap(), two.pause_transactions().await.unwrap());
    let directory = Arc::clone(&cell.directory);
    let first = tokio::spawn(async move { admin::rebalance(directory.as_ref(), 10, &control()).await });

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if cell.directory.deployment_by_name("one").await.unwrap().unwrap().placement_move.is_some()
                || cell.directory.deployment_by_name("two").await.unwrap().unwrap().placement_move.is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }).await.expect("the first rebalancer reserved the move");

    let competing = admin::rebalance(cell.directory.as_ref(), 10, &control()).await;
    drop(pauses);
    assert_eq!(first.await.unwrap().unwrap(), 1);
    assert_eq!(competing.unwrap(), 0);
    assert_eq!((cell.node(0).agent.held().len(), cell.node(1).agent.held().len()), (1, 1));
    drop((one, two));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unavailable_move_target_releases_the_reservation_and_late_parks_leave_the_new_holding() {
    use rtdb_multi::control::{self as rpc, MoveTicket, Request, Response};

    let cell = Cell::start(1, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let old = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    let old_member = old.holder.as_ref().unwrap();
    let mut target = cell.directory.member(old_member).await.unwrap().unwrap();
    target.id = rtdb_directory::MemberId::fresh();
    let unavailable = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    target.control_address = Some(unavailable.local_addr().unwrap().to_string());
    drop(unavailable);
    let target = cell.directory.register_member(target).await.unwrap();

    assert!(admin::move_deployment(cell.directory.as_ref(), "shop", &target.id, &control()).await.is_err());
    let parked = cell.directory.deployment(&old.id).await.unwrap().unwrap();
    assert!(parked.holder.is_none());
    assert!(parked.placement_move.is_none());
    let ticket = MoveTicket { deployment: old.id.clone(), request: uuid::Uuid::new_v4() };
    let late_activate = rpc::call(
        &cell.node(0).control_address.to_string(), &Request::ActivateForMove { name: "shop".into(), ticket: ticket.clone() },
        Duration::from_secs(5), &control(),
    ).await.unwrap();
    assert!(matches!(late_activate, Response::Refused { code, .. } if code == "rtdb.multi/stale-handoff"));
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    let recovered = cell.directory.deployment(&old.id).await.unwrap().unwrap();
    assert!(recovered.epoch > old.epoch);
    assert_eq!(recovered.holder.as_ref(), Some(old_member));

    let late = rpc::call(
        &cell.node(0).control_address.to_string(), &Request::ParkForMove { name: "shop".into(), epoch: old.epoch, ticket },
        Duration::from_secs(5), &control(),
    ).await.unwrap();
    assert!(matches!(late, Response::Refused { code, .. } if code == "rtdb.multi/stale-handoff"));
    let current = cell.directory.deployment(&old.id).await.unwrap().unwrap();
    assert_eq!((current.epoch, current.holder), (recovered.epoch, recovered.holder));
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deployment_whose_activations_crash_is_quarantined_to_the_isolation_pool() {
    let cell = Cell::start_with(vec![MemberPool::General, MemberPool::Isolation], None).await;
    let record = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let node = cell.node(0);
    assert!(matches!(node.agent.activate("shop").await.unwrap(), Response::Held { .. }));
    let (client, connection) = tokio_postgres::Config::new().host("127.0.0.1").port(node.pgwire_address.port())
        .user("xtdb").password("cell-test").dbname("shop").connect(NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);
    connection.await.unwrap().unwrap();
    node.agent.park("shop").await.unwrap();
    let original_ensemble = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = cell.directory.deployment(&record.id).await.unwrap().unwrap();
            let mut broken = current.clone();
            broken.ensemble = vec![MemberId("no-such-logkeeper".into())];
            if cell.directory.replace_deployment(broken, current.version).await.unwrap().applied().is_some() {
                break current.ensemble;
            }
            tokio::task::yield_now().await;
        }
    }).await.expect("the directory accepted the bad ensemble");

    for _ in 0..2 {
        assert!(tokio_postgres::connect(&format!("host=127.0.0.1 port={} user=xtdb password=cell-test dbname=shop", cell.gateway.port()), NoTls).await.is_err());
    }
    let quarantined = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert_eq!(quarantined.lifecycle, rtdb_directory::Lifecycle::Quarantined);
    assert!(cell.node(0).agent.held().is_empty(), "a general node no longer takes it");

    let mut repaired = quarantined.clone();
    repaired.ensemble = original_ensemble;
    cell.directory.replace_deployment(repaired, quarantined.version).await.unwrap().applied().unwrap();
    let isolated = cell.connect("shop").await;
    assert_eq!(cell.holder("shop").await, Some(1));
    let released = admin::unquarantine(cell.directory.as_ref(), "shop").await.unwrap();
    assert!(released.holder.is_none());
    assert_eq!(released.activation_crashes, 0);
    let isolated_catalog = cell.node(1).server.node().catalog();
    tokio::time::timeout(Duration::from_secs(5), async {
        while isolated_catalog.database("shop").is_ok() || isolated_catalog.is_draining("shop") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("the isolation node closed the released placement");
    assert_eq!(values(&isolated, "SELECT total FROM orders").await, ["10"]);
    assert_eq!(cell.holder("shop").await, Some(0));
    drop(isolated);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_starts_from_its_parents_data_and_diverges_and_a_deletion_leaves_nothing_readable() {
    let cell = Cell::start(2, None).await;
    let parent = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    drop(client);

    let child = admin::fork(&cell.directory, &cell.storage(), &cell.keys(), "shop", "shop-preview", Some(&cell.fork_broker()), &control()).await.unwrap();
    assert_eq!((child.lineage.as_str(), child.parent.as_deref()), (parent.lineage.as_str(), Some(parent.id.as_str())));
    let root = cell.data.path().join("objects");
    let own = |prefix: &str| files_under(&root.join(prefix)).into_iter().filter(|path| !path.components().any(|part| part.as_os_str() == rtdb_multi::reference::BASE_MANIFEST)).count();
    assert_eq!(own(&child.storage_prefix()), 0, "a fork copies nothing: it references its parent's objects");
    let preview = cell.connect("shop-preview").await;
    assert_eq!(values(&preview, "SELECT total FROM orders").await, ["10", "20"], "the fork starts from its parent's state");
    preview.simple_query("INSERT INTO orders (_id, total) VALUES (3, 30)").await.unwrap();
    let shop = cell.connect("shop").await;
    assert_eq!(values(&shop, "SELECT total FROM orders").await, ["10", "20"], "the fork's writes stay in the fork");
    shop.simple_query("INSERT INTO orders (_id, total) VALUES (4, 40)").await.unwrap();
    assert_eq!(values(&preview, "SELECT total FROM orders").await, ["10", "20", "30"], "and the parent's in the parent");
    drop(preview);
    let holder = cell.holder("shop-preview").await.unwrap();
    cell.node(holder).agent.park("shop-preview").await.unwrap();
    let written = own(&child.storage_prefix());

    let keys = cell.keys();
    let before = cell.directory.deployment(&parent.id).await.unwrap().unwrap();
    let wrong_root = rtdb_multi::deployment::CellStorage::Local { root: cell.data.path().join("wrong-objects") };
    let wrong_keys = rtdb_multi::keys::LocalKeyService::new(cell.data.path().join("wrong-keys")).unwrap();
    for (storage, keys) in [(&wrong_root, &keys), (&cell.storage(), &wrong_keys)] {
        let error = admin::delete_deployment(cell.directory.as_ref(), storage, keys, "shop", 4 << 20).await.unwrap_err();
        assert_eq!(error.code, "rtdb.multi/storage-binding-mismatch");
        let current = cell.directory.deployment(&parent.id).await.unwrap().unwrap();
        assert_eq!((current.epoch, current.lifecycle), (before.epoch, before.lifecycle));
        assert!(current.deletion_record.is_none());
        assert!(!files_under(&root.join(parent.storage_prefix())).is_empty());
    }
    assert!(keys.existing(&parent.id).await.is_ok());
    let pending = admin::begin_disposal(cell.directory.as_ref(), &parent.id).await.unwrap();
    assert_eq!(pending.lifecycle, Lifecycle::Deleting);
    let stop = tokio_util::sync::CancellationToken::new();
    let reaper = tokio::spawn(rtdb_multi::store::run_reaper(
        cell.directory.clone(), cell.storage(), Some(Arc::new(cell.keys())),
        None,
        Duration::from_millis(10), stop.clone(),
    ));
    let deletion = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(receipt) = cell.directory.deployment(&parent.id).await.unwrap().unwrap().disposal {
                break *receipt;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("the reaper recovers the scheduled disposal");
    stop.cancel();
    reaper.await.unwrap();
    assert_eq!(deletion.epoch, pending.epoch);
    assert_eq!(deletion.deployment, parent.id);
    assert!(files_under(&root.join(parent.storage_prefix())).is_empty(), "the parent's objects are erased");
    assert!(own(&child.storage_prefix()) > written, "after the fork's shared objects were copied into it");
    assert!(rtdb_multi::keys::KeyService::key(&cell.keys(), &parent.key_id()).await.is_err(), "its key is withdrawn");
    let tombstone = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert_eq!(tombstone.lifecycle, rtdb_directory::Lifecycle::Deleted);
    assert_eq!(tombstone.disposal.as_deref(), Some(&deletion));
    let recovered = admin::delete_installation(cell.directory.as_ref(), &cell.storage(), &cell.keys(), &parent.id, 4 << 20).await.unwrap();
    assert_eq!(recovered, deletion);
    assert_eq!(cell.directory.deployment(&parent.id).await.unwrap().unwrap().version, tombstone.version);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !shop.is_closed() { tokio::time::sleep(Duration::from_millis(10)).await; }
    }).await.expect("the gateway closes an existing session after observing disposal");
    let signed = tombstone.deletion_record.as_deref().unwrap();
    assert_eq!(signed.facts.deployment, tombstone.id);
    assert_eq!(signed.facts.epoch, tombstone.epoch);
    assert_eq!(signed.facts.storage_prefix, parent.storage_prefix());
    assert_eq!(Some(signed.facts.namespaces), tombstone.storage_namespaces);
    let retry = admin::delete_deployment(cell.directory.as_ref(), &cell.storage(), &cell.keys(), "shop", 4 << 20).await.unwrap();
    assert_eq!(&retry, signed);
    rtdb_multi::keys::verify_deletion(&retry, &cell.keys().deletion_verification_key().await.unwrap()).unwrap();
    assert!(tokio_postgres::connect(&format!("host=127.0.0.1 port={} user=xtdb password=cell-test dbname=shop", cell.gateway.port()), NoTls).await.is_err(), "a deleted deployment is refused");
    let preview = connect_eventually(&cell, "shop-preview").await;
    assert_eq!(values(&preview, "SELECT total FROM orders").await, ["10", "20", "30"], "a fork outlives its parent's deletion");
    drop(preview);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unopened_root_can_be_deleted_and_a_legacy_fenced_deletion_can_resume() {
    use rtdb_directory::Lifecycle;
    let cell = Cell::start(1, None).await;
    let (storage, keys) = (cell.storage(), cell.keys());
    let unopened = admin::create_deployment(cell.directory.as_ref(), "unopened", "acme", 3).await.unwrap();
    let receipt = admin::delete_deployment(cell.directory.as_ref(), &storage, &keys, "unopened", 4 << 20).await.unwrap();
    assert_eq!((receipt.facts.deployment, receipt.facts.epoch), (unopened.id, 1));

    admin::create_deployment(cell.directory.as_ref(), "legacy", "acme", 3).await.unwrap();
    let client = cell.connect("legacy").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);
    cell.node(0).agent.park("legacy").await.unwrap();
    let legacy = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut legacy = cell.directory.deployment_by_name("legacy").await.unwrap().unwrap();
            let version = legacy.version;
            legacy.epoch += 1;
            legacy.lifecycle = Lifecycle::Deleting;
            legacy.storage_namespaces = None;
            if let Some(fenced) = cell.directory.replace_deployment(legacy, version).await.unwrap().applied() {
                break fenced;
            }
            tokio::task::yield_now().await;
        }
    }).await.expect("the legacy deletion fixture acquired the directory fence");
    let wrong_root = rtdb_multi::deployment::CellStorage::Local { root: cell.data.path().join("wrong-objects") };
    let error = admin::delete_deployment(cell.directory.as_ref(), &wrong_root, &keys, "legacy", 4 << 20).await.unwrap_err();
    assert_eq!(error.code, "rtdb.multi/storage-binding-mismatch");
    let unchanged = cell.directory.deployment_by_name("legacy").await.unwrap().unwrap();
    assert_eq!(unchanged.lifecycle, legacy.lifecycle);
    assert_eq!(unchanged.epoch, legacy.epoch);
    assert_eq!(unchanged.storage_namespaces, legacy.storage_namespaces);
    assert!(unchanged.disposal.is_none());
    assert!(unchanged.deletion_record.is_none());

    let receipt = admin::delete_deployment(cell.directory.as_ref(), &storage, &keys, "legacy", 4 << 20).await.unwrap();
    assert_eq!((receipt.facts.deployment, receipt.facts.epoch), (legacy.id, legacy.epoch));
    assert!(files_under(&cell.data.path().join("objects").join(receipt.facts.storage_prefix)).is_empty());
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_a_deployment_stores_is_readable_at_rest() {
    let cell = Cell::start(1, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, note) VALUES (1, 'plaintext-marker-zq81')").await.unwrap();
    drop(client);
    cell.node(0).agent.park("shop").await.unwrap();
    let data = cell.stop().await;
    let files = files_under(data.path());
    assert!(files.iter().any(|file| file.to_string_lossy().contains("/blocks/")), "the park flushed a block");
    for file in files {
        let bytes = std::fs::read(&file).unwrap();
        assert!(!bytes.windows(21).any(|window| window == b"plaintext-marker-zq81"), "{} holds the row in the clear", file.display());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_connection_survives_a_planned_move_and_its_holders_death() {
    let mut cell = Cell::start(3, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let statement = client.prepare("SELECT total FROM orders WHERE _id = 2").await.unwrap();

    let first = cell.holder("shop").await.unwrap();
    let target = (first + 1) % 3;
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(target).lease.id(), &control()).await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"], "the same connection reads on the new holder");
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    let row = client.query_one(&statement, &[]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 20, "a statement prepared before the move still runs after it");

    let holder = cell.holder("shop").await.unwrap();
    assert_eq!(holder, target);
    cell.nodes[holder].take().unwrap().kill().await.unwrap();
    tokio::time::sleep(Duration::from_millis(1800)).await;
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10", "20"], "and survives its holder's death while idle");
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_gateway_session_keeps_its_new_holder_from_parking() {
    let idle = Duration::from_millis(800);
    let cell = Cell::start(2, Some(idle)).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();

    let target = 1 - cell.holder("shop").await.unwrap();
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(target).lease.id(), &control()).await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    tokio::time::sleep(idle * 2).await;
    assert_eq!(cell.holder("shop").await, Some(target));

    drop(client);
    tokio::time::timeout(Duration::from_secs(5), async {
        while cell.holder("shop").await.is_some() { tokio::time::sleep(Duration::from_millis(50)).await; }
    }).await.unwrap();
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_move_replays_a_short_tail_and_flushes_a_large_one() {
    let cell = Cell::start_durable(2).await;
    let record = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (0, 123)").await.unwrap();
    let source = cell.holder("shop").await.unwrap();
    let target = 1 - source;
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(target).lease.id(), &control()).await.unwrap();

    let path = cell.data.path().join("objects").join(record.storage_prefix());
    assert!(!files_under(&path).iter().any(|file| file.to_string_lossy().contains("/~packs/")));
    assert_eq!(cell.holder("shop").await, Some(target));
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["123"]);

    let rows = (1..=65).map(|id| format!("({id}, {id})")).collect::<Vec<_>>().join(", ");
    client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES {rows}")).await.unwrap();
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(source).lease.id(), &control()).await.unwrap();
    assert!(files_under(&path).iter().any(|file| file.to_string_lossy().contains("/~packs/")));
    assert_eq!(cell.holder("shop").await, Some(source));
    assert_eq!(values(&client, "SELECT COUNT(*) FROM orders").await, ["66"]);
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_admission_holds_new_transactions_outside_the_intake_log() {
    let cell = Cell::start_durable(1).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 0)").await.unwrap();
    let database = cell.node(0).server.node().catalog().database("shop").unwrap();
    let paused = database.pause_transactions().await.unwrap();
    let previously_admitted = database.latest_submitted_position().unwrap();
    {
        let write = client.simple_query("UPDATE orders SET total = total + 1 WHERE _id = 1");
        tokio::pin!(write);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut write).await.is_err());
        assert_eq!(database.latest_submitted_position().unwrap(), previously_admitted);
        drop(paused);
        tokio::time::timeout(Duration::from_secs(5), write).await.unwrap().unwrap();
    }
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["1"]);
    drop((database, client));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_checkpoint_capture_keeps_its_cut_and_archives_only_sealed_objects() {
    let cell = Cell::start(1, None).await;
    let id = uuid::Uuid::new_v4();
    admin::install_deployment(cell.directory.as_ref(), id, "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, detail) VALUES (1, 'sample-payload-for-archive')").await.unwrap();
    let holder = cell.holder("shop").await.unwrap();
    let advance = |previous: Option<i64>| {
        let cell = &cell;
        async move {
            cell.node(holder).agent.flush("shop").await.unwrap();
            tokio::time::timeout(Duration::from_secs(8), async {
                loop {
                    if let Some(checkpoint) = cell.directory.deployment(&id.to_string()).await.unwrap().unwrap().checkpoint
                        && previous.is_none_or(|block| checkpoint.block > block)
                    {
                        return checkpoint;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }).await.unwrap()
        }
    };
    let first = advance(None).await;
    let selected = cell.directory.deployment(&id.to_string()).await.unwrap().unwrap();
    assert_eq!(selected.checkpoint, Some(first));
    let destination = tempfile::tempdir().unwrap();
    let archive = CellStorage::Local { root: destination.path().to_owned() };
    let operation = uuid::Uuid::new_v4();
    let capture = rtdb_multi::archive::capture(cell.directory.clone(), &cell.storage(), &archive, &cell.keys(), id, operation, None, Some(&selected)).await.unwrap();
    assert_eq!(capture.point.checkpoint, first);
    assert!(capture.objects.iter().any(|object| object.name.ends_with(".arrow")));
    assert!(capture.objects.iter().any(|object| object.name.starts_with("blocks/")));
    const SAMPLE: &[u8] = b"sample-payload-for-archive";
    for object in &capture.objects {
        let path = destination.path().join(format!("checkpoint-captures/{id}/{operation}/objects/{}", object.name));
        let bytes = std::fs::read(path).unwrap();
        assert_eq!(bytes.len() as u64, object.bytes);
        assert!(!bytes.windows(SAMPLE.len()).any(|window| window == SAMPLE));
    }
    client.simple_query("INSERT INTO orders (_id, detail) VALUES (2, 'later')").await.unwrap();
    assert!(advance(Some(first.block)).await.block > first.block);
    let stale_operation = uuid::Uuid::new_v4();
    let raced = rtdb_multi::archive::capture(
        cell.directory.clone(), &cell.storage(), &archive, &cell.keys(),
        id, stale_operation, None, Some(&selected),
    ).await.unwrap_err();
    assert_eq!(raced.message.as_deref(), Some("The selected source cut changed before archive admission"));
    let newer = cell.directory.deployment(&id.to_string()).await.unwrap().unwrap();
    assert!(!newer.checkpoint_captures.contains_key(&stale_operation));
    let mismatched = rtdb_multi::archive::capture(
        cell.directory.clone(), &cell.storage(), &archive, &cell.keys(),
        id, operation, None, Some(&newer),
    ).await.unwrap_err();
    assert_eq!(mismatched.message.as_deref(), Some("The capture operation already names another checkpoint"));
    let replay = rtdb_multi::archive::capture(cell.directory.clone(), &cell.storage(), &archive, &cell.keys(), id, operation, None, Some(&selected)).await.unwrap();
    assert_eq!(replay, capture);
    let other_destination = tempfile::tempdir().unwrap();
    let other_archive = CellStorage::Local { root: other_destination.path().to_owned() };
    assert!(rtdb_multi::archive::capture(cell.directory.clone(), &cell.storage(), &other_archive, &cell.keys(), id, operation, None, None).await.is_err());
    assert!(rtdb_multi::archive::begin_retirement(cell.directory.as_ref(), id).await.is_err());
    assert!(rtdb_multi::archive::restore(cell.directory.as_ref(), &cell.storage(), &archive, &cell.keys(), id, operation, 4 << 20, None).await.is_err());
    drop(client);
    admin::delete_installation(cell.directory.as_ref(), &cell.storage(), &cell.keys(), &id.to_string(), 4 << 20).await.unwrap();
    let stage = rtdb_directory::CheckpointStageIntent {
        operation: uuid::Uuid::new_v4(),
        source: id,
        capture: operation,
        source_manifest_sha256: capture.manifest_sha256().unwrap(),
        destination: uuid::Uuid::new_v4(),
        name: "restored-shop".into(),
        org: "acme".into(),
    };
    let staged = rtdb_multi::archive::stage_checkpoint(
        cell.directory.clone(), &archive, &cell.storage(), &cell.keys(), stage.clone(), None,
    ).await.unwrap();
    assert_eq!(staged.checkpoint, first);
    assert!(staged.staged_manifest_sha256.is_some());
    let held = cell.directory.deployment(&stage.destination.to_string()).await.unwrap().unwrap();
    assert_eq!(held.lifecycle, Lifecycle::Restoring);
    assert!(held.checkpoint.is_none() && held.holder.is_none() && held.ensemble.is_empty());
    assert!(held.storage_namespaces.is_some());
    assert_eq!(held.checkpoint_stage.as_deref(), Some(&staged));
    let mut different = stage.clone();
    different.operation = uuid::Uuid::new_v4();
    assert!(rtdb_multi::archive::stage_checkpoint(
        cell.directory.clone(), &archive, &cell.storage(), &cell.keys(), different, None,
    ).await.is_err());
    let scheduled = rtdb_multi::archive::begin_retirement(cell.directory.as_ref(), id).await.unwrap();
    assert!(scheduled.observed_empty_at_ms.is_none());
    let retired = rtdb_multi::archive::finish_retirement(cell.directory.as_ref(), &cell.keys(), id)
        .await
        .unwrap();
    assert!(retired.observed_empty_at_ms.is_some());
    assert_eq!(retired.authorized_at_ms, scheduled.authorized_at_ms);
    assert!(files_under(&destination.path().join(format!("checkpoint-captures/{id}/{operation}"))).is_empty());
    assert_eq!(rtdb_multi::archive::begin_retirement(cell.directory.as_ref(), id).await.unwrap(), retired);
    assert_eq!(rtdb_multi::archive::stage_checkpoint(
        cell.directory.clone(), &archive, &cell.storage(), &cell.keys(), stage.clone(), None,
    ).await.unwrap(), staged);
    let receipt = admin::delete_installation(
        cell.directory.as_ref(), &cell.storage(), &cell.keys(), &stage.destination.to_string(), 4 << 20,
    ).await.unwrap();
    assert_eq!(receipt.deployment, stage.destination.to_string());
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parked_checkpoint_keeps_serving_and_recapturing_after_original_packs_go_away() {
    let cell = Cell::start(1, None).await;
    let id = uuid::Uuid::new_v4();
    admin::install_deployment(cell.directory.as_ref(), id, "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let holder = cell.holder("shop").await.unwrap();
    drop(client);
    cell.node(holder).agent.park("shop").await.unwrap();
    let parked = cell.directory.deployment(&id.to_string()).await.unwrap().unwrap();
    let checkpoint = parked.checkpoint.unwrap();
    assert!(parked.holder.is_none() && parked.recovery_manifest_sha256.is_some());

    let storage = cell.storage();
    let keys = cell.keys();
    let media = tempfile::tempdir().unwrap();
    let archive = CellStorage::Local { root: media.path().to_owned() };
    let operation = uuid::Uuid::new_v4();
    let captured = rtdb_multi::archive::capture(
        cell.directory.clone(), &storage, &archive, &keys, id, operation, None, Some(&parked),
    ).await.unwrap();
    let manifest = captured.manifest_sha256().unwrap();
    let registered = cell.directory.deployment(&id.to_string()).await.unwrap().unwrap();
    let input = captured.objects.iter().find(|object| object.name.contains("/data/")).unwrap();
    let object = media.path().join(format!("checkpoint-captures/{id}/{operation}/objects/{}", input.name));
    let original = std::fs::read(&object).unwrap();
    let mut corrupt = original.clone();
    corrupt[0] ^= 1;
    std::fs::write(&object, corrupt).unwrap();
    let retain = || rtdb_multi::archive::retain_serving_replay(
        cell.directory.as_ref(), &storage, &archive, &keys, &registered, operation, &manifest, None,
    );
    assert_eq!(retain().await.unwrap_err().code, "rtdb.storage/immutable-object-conflict");
    assert!(cell.directory.deployment(&id.to_string()).await.unwrap().unwrap().serving_replay_capture.is_none());
    std::fs::write(object, original).unwrap();

    assert_eq!(retain().await.unwrap(), captured.point);
    let attached = cell.directory.deployment(&id.to_string()).await.unwrap().unwrap();
    assert_eq!(attached.serving_replay_capture, Some(operation));
    assert_eq!(attached.checkpoint, Some(checkpoint));
    assert_eq!(attached.epoch, parked.epoch);
    assert!(attached.checkpoint_recovery.is_none());

    let CellStorage::Local { root } = &storage else { panic!("local cell fixture") };
    let version = rtdb_format::keys::StorageRoot::new(
        rtdb_format::keys::CURRENT_STORAGE_VERSION, rtdb_types::StorageEpoch(0),
    ).unwrap().key();
    let packs = root.join(parked.storage_prefix()).join(version).join("~packs");
    assert!(std::fs::read_dir(&packs).unwrap().next().is_some());
    let isolated = tempfile::tempdir().unwrap();
    std::fs::rename(&packs, isolated.path().join("removed-original-packs")).unwrap();

    let client = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&client, "SELECT total FROM orders ORDER BY _id").await, ["10"]);
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders ORDER BY _id").await, ["10", "20"]);
    let holder = cell.holder("shop").await.unwrap();
    drop(client);
    cell.node(holder).agent.park("shop").await.unwrap();
    let successor = cell.directory.deployment(&id.to_string()).await.unwrap().unwrap();
    assert!(successor.checkpoint.unwrap().block > checkpoint.block);
    let next = uuid::Uuid::new_v4();
    let recaptured = rtdb_multi::archive::capture(
        cell.directory.clone(), &storage, &archive, &keys, id, next, None, Some(&successor),
    ).await.unwrap();
    assert!(recaptured.objects.iter().any(|object| object.name == input.name));
    let next_manifest = recaptured.manifest_sha256().unwrap();
    let registered_successor = cell.directory.deployment(&id.to_string()).await.unwrap().unwrap();
    assert_eq!(rtdb_multi::archive::retain_serving_replay(
        cell.directory.as_ref(), &storage, &archive, &keys, &registered_successor, next, &next_manifest, None,
    ).await.unwrap(), recaptured.point);
    assert!(retain().await.is_err());
    let reopened = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&reopened, "SELECT total FROM orders ORDER BY _id").await, ["10", "20"]);
    drop(reopened);
    let destination = Cell::start(1, None).await;
    let from = admin::CellHandle {
        name: "source",
        directory: &cell.directory,
        storage: &storage,
        keys: &keys,
        broker: None,
        maximum_deployments: 1,
    };
    let to = admin::CellHandle {
        name: "destination",
        directory: &destination.directory,
        storage: &destination.storage(),
        keys: &destination.keys(),
        broker: None,
        maximum_deployments: 1,
    };
    let intent = rtdb_directory::CellTransferIntent {
        operation: Some(uuid::Uuid::new_v4()),
        authority: id,
        name: "shop".into(),
        source: rtdb_directory::CellResidence { cell: "source".into(), org: "acme".into() },
        destination: rtdb_directory::CellResidence { cell: "destination".into(), org: "acme-destination".into() },
    };
    let transfer = admin::prepare_managed_cell_transfer(&from, &to, &intent, 4 << 20, None).await.unwrap();
    let arrived = admin::activate_managed_cell_transfer(&from, &to, &transfer).await.unwrap();
    assert!(arrived.serving_replay_capture.is_none());
    let moved = connect_eventually(&destination, "shop").await;
    assert_eq!(values(&moved, "SELECT total FROM orders ORDER BY _id").await, ["10", "20"]);
    drop(moved);
    destination.stop().await;
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_local_reaper_retires_an_original_l0_pack_under_verified_archive_custody() {
    let directory = ObservedDirectory::new();
    let cell = Cell::start_with_directory(directory.clone()).await;
    let id = uuid::Uuid::new_v4();
    let deployment = id.to_string();
    admin::install_deployment(cell.directory.as_ref(), id, "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let holder = cell.holder("shop").await.unwrap();
    let agent = &cell.node(holder).agent;
    agent.flush("shop").await.unwrap();
    let first = cell.directory.deployment(&deployment).await.unwrap().unwrap().checkpoint.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    agent.flush("shop").await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let record = cell.directory.deployment(&deployment).await.unwrap().unwrap();
            if record.replica_anchor.is_some_and(|anchor| anchor.interval[1] > first.boundary) { break; }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("a later checkpoint certified the earlier replica boundary");
    drop(client);
    agent.park("shop").await.unwrap();
    let storage = cell.storage();
    let keys = cell.keys();
    let child = admin::fork_at_block(
        &cell.directory, &storage, &keys, "shop", "preview", Some(&cell.fork_broker()), first.block,
    ).await.unwrap();
    let parked = cell.directory.deployment(&deployment).await.unwrap().unwrap();
    let current = parked.checkpoint.unwrap();
    assert!(current.block > first.block);
    let media = tempfile::tempdir().unwrap();
    let archive = CellStorage::Local { root: media.path().to_owned() };
    let operation = uuid::Uuid::new_v4();
    let captured = rtdb_multi::archive::capture(
        cell.directory.clone(), &storage, &archive, &keys, id, operation, None, Some(&parked),
    ).await.unwrap();
    let manifest = captured.manifest_sha256().unwrap();
    let registered = cell.directory.deployment(&deployment).await.unwrap().unwrap();
    rtdb_multi::archive::retain_serving_replay(
        cell.directory.as_ref(), &storage, &archive, &keys, &registered, operation, &manifest, None,
    ).await.unwrap();

    let CellStorage::Local { root } = &storage else { panic!("local cell fixture") };
    let version = rtdb_format::keys::StorageRoot::new(
        rtdb_format::keys::CURRENT_STORAGE_VERSION, rtdb_types::StorageEpoch(0),
    ).unwrap().key();
    let store = root.join(parked.storage_prefix()).join(version);
    let pack_name = |block| rtdb_multi::packing::block_pack(&rtdb_format::keys::block_key(rtdb_types::BlockIndex(block)).unwrap());
    let original_pack = store.join(pack_name(first.block));
    let published_pack = store.join(pack_name(current.block));
    assert!(original_pack.is_file() && published_pack.is_file());
    let l0 = rtdb_format::keys::TrieKey::l0(rtdb_types::BlockIndex(first.block)).unwrap();
    let input = captured.objects.iter().find(|object| object.name.ends_with(&format!("/data/{l0}.arrow"))).unwrap();
    let object = media.path().join(format!("checkpoint-captures/{id}/{operation}/objects/{}", input.name));
    let original_archive = std::fs::read(&object).unwrap();
    let mut corrupt = original_archive.clone();
    corrupt[0] ^= 1;
    let completed = Arc::new(tokio::sync::Notify::new());
    *directory.local_retirement_completed.lock().unwrap() = Some((parked.storage_prefix(), completed.clone()));
    let stop = tokio_util::sync::CancellationToken::new();
    let reaper = tokio::spawn(rtdb_multi::store::run_reaper(
        cell.directory.clone(), storage.clone(), Some(Arc::new(cell.keys())),
        None,
        Duration::from_millis(15), stop.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(10), completed.notified()).await
        .expect("the local reaper completed the parent's sweep with a descendant");
    assert!(original_pack.is_file());
    std::fs::write(&object, corrupt).unwrap();
    let preview = cell.connect("preview").await;
    assert_eq!(values(&preview, "SELECT total FROM orders ORDER BY _id").await, ["10"]);
    let preview_holder = cell.holder("preview").await.unwrap();
    drop(preview);
    cell.node(preview_holder).agent.park("preview").await.unwrap();
    admin::begin_disposal(cell.directory.as_ref(), &child.id).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let record = cell.directory.deployment(&child.id).await.unwrap().unwrap();
            if record.disposal.is_some() { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("the child relinquished its original pack references");
    let attached = cell.directory.deployment(&deployment).await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let record = cell.directory.deployment(&deployment).await.unwrap().unwrap();
            if record.version > attached.version && record.storage_regions.is_empty() { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("the reaper checked the damaged archive and closed its read region");
    assert!(original_pack.is_file());
    std::fs::write(&object, original_archive).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while original_pack.is_file() { tokio::time::sleep(Duration::from_millis(10)).await; }
    }).await.expect("the reaper physically removed the covered pack");
    stop.cancel();
    reaper.await.unwrap();
    assert!(published_pack.is_file());

    let reopened = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&reopened, "SELECT total FROM orders ORDER BY _id").await, ["10", "20"]);
    let holder = cell.holder("shop").await.unwrap();
    drop(reopened);
    cell.node(holder).agent.park("shop").await.unwrap();
    let successor = cell.directory.deployment(&deployment).await.unwrap().unwrap();
    let repair = uuid::Uuid::new_v4();
    let recaptured = rtdb_multi::archive::capture(
        cell.directory.clone(), &storage, &archive, &keys, id, repair, None, Some(&successor),
    ).await.unwrap();
    assert!(recaptured.objects.iter().any(|object| object.name == input.name));
    rtdb_multi::archive::restore(
        cell.directory.as_ref(), &storage, &archive, &keys, id, repair, 4 << 20, None,
    ).await.unwrap();
    let restored = cell.directory.deployment(&deployment).await.unwrap().unwrap();
    assert_eq!(restored.serving_replay_capture, Some(repair));
    let reopened = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&reopened, "SELECT total FROM orders ORDER BY _id").await, ["10", "20"]);
    reopened.simple_query("INSERT INTO orders (_id, total) VALUES (3, 30)").await.unwrap();
    assert_eq!(values(&reopened, "SELECT total FROM orders ORDER BY _id").await, ["10", "20", "30"]);
    let holder = cell.holder("shop").await.unwrap();
    drop(reopened);
    cell.node(holder).agent.park("shop").await.unwrap();
    let after_restore = cell.directory.deployment(&deployment).await.unwrap().unwrap();
    let next = uuid::Uuid::new_v4();
    let advanced = rtdb_multi::archive::capture(
        cell.directory.clone(), &storage, &archive, &keys, id, next, None, Some(&after_restore),
    ).await.unwrap();
    assert!(advanced.objects.iter().any(|object| object.name == input.name));
    let registered = cell.directory.deployment(&deployment).await.unwrap().unwrap();
    rtdb_multi::archive::retain_serving_replay(
        cell.directory.as_ref(), &storage, &archive, &keys, &registered, next, &advanced.manifest_sha256().unwrap(), None,
    ).await.unwrap();
    let published = cell.directory.deployment(&deployment).await.unwrap().unwrap();
    assert_eq!(published.serving_replay_capture, Some(next));
    assert!(published.checkpoint_repair_receipts.contains_key(&repair));
    let reopened = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&reopened, "SELECT total FROM orders ORDER BY _id").await, ["10", "20", "30"]);
    drop(reopened);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restoring_the_current_archive_preserves_the_ordered_tail_and_authority() {
    let mut cell = Cell::start(2, None).await;
    let id = uuid::Uuid::new_v4();
    admin::install_deployment(cell.directory.as_ref(), id, "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let holder = cell.holder("shop").await.unwrap();
    cell.node(holder).agent.flush("shop").await.unwrap();
    let record = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let record = cell.directory.deployment(&id.to_string()).await.unwrap().unwrap();
            if record.checkpoint.is_some() { return record; }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.unwrap();
    let archive_dir = tempfile::tempdir().unwrap();
    let archive = CellStorage::Local { root: archive_dir.path().to_owned() };
    let operation = uuid::Uuid::new_v4();
    let storage = cell.storage();
    let capture = rtdb_multi::archive::capture(cell.directory.clone(), &storage, &archive, &cell.keys(), id, operation, None, None).await.unwrap();
    let accepted = cell.directory.deployment(&id.to_string()).await.unwrap().unwrap();
    let manifest = capture.manifest_sha256().unwrap();
    assert_eq!(accepted.checkpoint_captures[&operation].manifest_sha256.as_deref(), Some(manifest.as_str()));
    let successor_operation = uuid::Uuid::new_v4();
    rtdb_multi::archive::capture(cell.directory.clone(), &storage, &archive, &cell.keys(), id, successor_operation, None, None).await.unwrap();
    let object = capture.objects.iter().find(|object| object.name.ends_with(".arrow")).unwrap();
    let path = archive_dir.path().join(format!("checkpoint-captures/{id}/{operation}/objects/{}", object.name));
    let ciphertext = std::fs::read(&path).unwrap();
    let mut corrupt = ciphertext.clone();
    corrupt[0] ^= 1;
    std::fs::write(&path, corrupt).unwrap();
    assert!(rtdb_multi::archive::capture(cell.directory.clone(), &storage, &archive, &cell.keys(), id, operation, None, None).await.is_err());
    assert!(rtdb_multi::archive::restore(cell.directory.as_ref(), &storage, &archive, &cell.keys(), id, operation, 4 << 20, None).await.is_err());
    let still_live = cell.directory.deployment(&id.to_string()).await.unwrap().unwrap();
    assert_eq!(still_live.lifecycle, Lifecycle::Live);
    assert_eq!(still_live.epoch, record.epoch);
    std::fs::write(path, ciphertext).unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    drop(client);
    cell.nodes[holder].take().unwrap().kill().await.unwrap();
    let CellStorage::Local { root } = &storage else { panic!("a disposable local fixture") };
    std::fs::remove_dir_all(root.join(record.storage_prefix())).unwrap();

    let restored = rtdb_multi::archive::restore(cell.directory.as_ref(), &storage, &archive, &cell.keys(), id, operation, 4 << 20, None).await.unwrap();
    assert!(restored.completed_at_ms.is_some());
    assert_eq!(restored.epoch, record.epoch + 1);
    let successor = rtdb_multi::archive::restore(cell.directory.as_ref(), &storage, &archive, &cell.keys(), id, successor_operation, 4 << 20, None).await.unwrap();
    assert!(successor.epoch > restored.epoch);
    let reopened = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&reopened, "SHOW rtdb_hosted_source_barrier").await, [id.to_string()]);
    assert_eq!(values(&reopened, "SELECT total FROM orders ORDER BY _id").await, ["10", "20"]);
    let recaptured = rtdb_multi::archive::capture(
        cell.directory.clone(), &storage, &archive, &cell.keys(), id, uuid::Uuid::new_v4(), None, None,
    ).await.expect("recapturing a repaired checkpoint must open the admitted source archive");
    assert_eq!(recaptured.point.checkpoint, successor.checkpoint);
    let retried = rtdb_multi::archive::restore(cell.directory.as_ref(), &storage, &archive, &cell.keys(), id, operation, 4 << 20, None).await.unwrap();
    assert_eq!(retried, restored);
    let other_dir = tempfile::tempdir().unwrap();
    let other_archive = CellStorage::Local { root: other_dir.path().to_owned() };
    assert!(rtdb_multi::archive::restore(cell.directory.as_ref(), &storage, &other_archive, &cell.keys(), id, operation, 4 << 20, None).await.is_err());
    drop(reopened);

    let destination = Cell::start(1, None).await;
    let from = admin::CellHandle {
        name: "source",
        directory: &cell.directory,
        storage: &storage,
        keys: &cell.keys(),
        broker: None,
        maximum_deployments: 1,
    };
    let to = admin::CellHandle {
        name: "destination",
        directory: &destination.directory,
        storage: &destination.storage(),
        keys: &destination.keys(),
        broker: None,
        maximum_deployments: 1,
    };
    let intent = rtdb_directory::CellTransferIntent {
        operation: Some(uuid::Uuid::new_v4()),
        authority: id,
        name: "shop".into(),
        source: rtdb_directory::CellResidence { cell: "source".into(), org: "acme".into() },
        destination: rtdb_directory::CellResidence { cell: "destination".into(), org: "acme-destination".into() },
    };
    let prepared = admin::prepare_managed_cell_transfer(&from, &to, &intent, 4 << 20, None)
        .await.expect("a cross-cell move uses its original repair archive admission");
    let pending = destination.directory.deployment(&id.to_string()).await.unwrap().unwrap();
    assert_eq!(pending.lifecycle, Lifecycle::Moving);
    assert_eq!(pending.org, intent.destination.org);
    assert_eq!(prepared.source_incarnation, record.incarnation);
    assert_eq!(prepared.destination_incarnation, pending.incarnation);
    assert_ne!(record.storage_prefix(), pending.storage_prefix());
    assert_ne!(record.stream("replica"), pending.stream("replica"));
    assert_ne!(record.key_id(), pending.key_id());
    assert!(pending.move_from.is_none());
    let mut foreign = prepared.clone();
    foreign.intent.operation = Some(uuid::Uuid::new_v4());
    assert!(admin::activate_managed_cell_transfer(&from, &to, &foreign).await.is_err());

    let CellStorage::Local { root } = to.storage else { panic!("a disposable local fixture") };
    let block = rtdb_types::BlockIndex(pending.checkpoint.unwrap().block);
    let catalog = rtdb_format::keys::block_key(block).unwrap();
    let root_key = rtdb_format::keys::StorageRoot::new(rtdb_format::keys::CURRENT_STORAGE_VERSION, rtdb_types::StorageEpoch(0)).unwrap().key();
    let storage_root = root.join(pending.storage_prefix()).join(root_key);
    let pack = storage_root.join(rtdb_multi::packing::block_pack(&catalog));
    let missing = tempfile::tempdir_in(root).unwrap();
    let displaced = missing.path().join("checkpoint.pack");
    std::fs::rename(&pack, &displaced).unwrap();
    assert!(admin::activate_managed_cell_transfer(&from, &to, &prepared).await.is_err());
    assert_eq!(cell.directory.deployment(&id.to_string()).await.unwrap().unwrap().lifecycle, Lifecycle::Moving);
    assert_eq!(destination.directory.deployment(&id.to_string()).await.unwrap().unwrap().lifecycle, Lifecycle::Moving);
    std::fs::rename(&displaced, &pack).unwrap();

    let marker = storage_root.join(rtdb_multi::store::ROOT_MARKER);
    std::fs::OpenOptions::new().write(true).create_new(true).open(&marker).unwrap();
    assert_eq!(admin::activate_managed_cell_transfer(&from, &to, &prepared).await.unwrap_err().code, "rtdb.multi/foreign-root");
    assert_eq!(cell.directory.deployment(&id.to_string()).await.unwrap().unwrap().lifecycle, Lifecycle::Moving);
    std::fs::remove_file(marker).unwrap();

    let arrived = admin::activate_managed_cell_transfer(&from, &to, &prepared).await.unwrap();
    assert_eq!(arrived.lifecycle, Lifecycle::Live);
    assert_eq!(admin::prepare_managed_cell_transfer(&from, &to, &intent, 4 << 20, None).await.unwrap(), prepared);
    assert_eq!(admin::activate_managed_cell_transfer(&from, &to, &prepared).await.unwrap(), arrived);
    assert!(arrived.checkpoint_recovery.is_none());
    let moved = connect_eventually(&destination, "shop").await;
    assert_eq!(values(&moved, "SHOW rtdb_hosted_source_barrier").await, [id.to_string()]);
    assert_eq!(values(&moved, "SELECT total FROM orders ORDER BY _id").await, ["10", "20"]);
    drop(moved);

    let back = rtdb_directory::CellTransferIntent {
        operation: Some(uuid::Uuid::new_v4()),
        source: intent.destination.clone(),
        destination: intent.source.clone(),
        ..intent
    };
    let waiting = admin::prepare_managed_cell_transfer(&to, &from, &back, 4 << 20, None).await.unwrap_err();
    assert_eq!(waiting.code, "rtdb.multi/previous-residence");
    assert_eq!(destination.directory.deployment(&id.to_string()).await.unwrap().unwrap().lifecycle, Lifecycle::Live);

    let retired = admin::finish_disposal(from.directory.as_ref(), from.storage, from.keys, &id.to_string(), 4 << 20).await.unwrap();
    let returned = admin::prepare_managed_cell_transfer(&to, &from, &back, 4 << 20, None).await.unwrap();
    assert_eq!(returned.destination_incarnation, returned.source_incarnation + 1);
    let active = admin::activate_managed_cell_transfer(&to, &from, &returned).await.unwrap();
    assert_eq!(active.retired_residences[&record.incarnation].serving, retired);
    assert_eq!(active.checkpoint_captures[&operation].incarnation, record.incarnation);
    let retried_after_return = rtdb_multi::archive::restore(
        from.directory.as_ref(), from.storage, &archive, from.keys, id, operation, 4 << 20, None,
    ).await.unwrap();
    assert_eq!(retried_after_return, restored);
    assert!(from.directory.deployment(&id.to_string()).await.unwrap().unwrap().checkpoint_recovery.is_none());
    assert!(from.keys.existing_key(&record.key_id()).await.is_err());
    assert_eq!(from.keys.existing_key(&active.key_id()).await.unwrap(), to.keys.existing_key(&arrived.key_id()).await.unwrap());
    assert!(rtdb_multi::archive::begin_retirement(from.directory.as_ref(), id).await.is_err());

    let moved = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&moved, "SHOW rtdb_hosted_source_barrier").await, [id.to_string()]);
    assert_eq!(values(&moved, "SELECT total FROM orders ORDER BY _id").await, ["10", "20"]);
    drop(moved);
    admin::delete_installation(from.directory.as_ref(), from.storage, from.keys, &id.to_string(), 4 << 20).await.unwrap();
    rtdb_multi::archive::begin_retirement(from.directory.as_ref(), id).await.unwrap();
    let retired_archives = rtdb_multi::archive::finish_retirement(from.directory.as_ref(), from.keys, id).await.unwrap();
    assert!(retired_archives.observed_empty_at_ms.is_some());
    assert!(files_under(&archive_dir.path().join(format!("checkpoint-captures/{id}/{operation}"))).is_empty());
    archive_dir.close().unwrap();
    destination.stop().await;
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_a_checkpoint_restore_resumes_after_fencing_and_sealing() {
    use rtdb_storage::{IoRegion, ObjectKey};

    let (Some(storage), Some(archive)) = (s3_storage(), s3_archive_storage()) else {
        eprintln!("RTDB_S3_ENDPOINT is not set: skipped");
        return;
    };
    tokio::time::timeout(Duration::from_secs(240), async {
        let directory = ObservedDirectory::new();
        let cell = Cell::start_configured_with_directory(directory.clone(), Some(storage.clone()),
            vec![MemberPool::General; 2], None, None, None, true, |_| {}).await;
        let deployment = admin::create_deployment(directory.as_ref(), "shop", "acme", 3).await.unwrap();
        let id = uuid::Uuid::parse_str(&deployment.id).unwrap();
        let client = cell.connect("shop").await;
        client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10), (2, 20)").await.unwrap();
        let holder = cell.holder("shop").await.unwrap();
        cell.node(holder).agent.flush("shop").await.unwrap();
        let checkpoint = directory.deployment(&deployment.id).await.unwrap().unwrap().checkpoint;
        let operation = uuid::Uuid::new_v4();
        let keys = cell.keys();
        let capture = rtdb_multi::archive::capture(directory.clone(), &storage, &archive, &keys, id, operation, Some(hosted_test_owner()), None).await.unwrap();
        assert!(!capture.objects.is_empty());
        client.simple_query("UPDATE orders SET total = 11 WHERE _id = 1").await.unwrap();
        client.simple_query("DELETE FROM orders WHERE _id = 2").await.unwrap();
        let history = order_history(&client).await;
        drop(client);

        let owner = hosted_test_owner();
        let restore = || rtdb_multi::archive::restore(directory.as_ref(), &storage, &archive, &keys, id, operation, 4 << 20, Some(&owner));
        directory.restore_interruption.store(1, Ordering::Relaxed);
        assert_eq!(restore().await.unwrap_err().code, "test/restore-interrupted");
        let fenced = directory.deployment(&deployment.id).await.unwrap().unwrap();
        assert_eq!(fenced.lifecycle, Lifecycle::Restoring);
        assert!(fenced.holder.is_none());
        assert_eq!(fenced.checkpoint, checkpoint);

        let CellStorage::S3 { config } = &storage else { unreachable!() };
        let bucket = config.open(&config.cell_root().unwrap(), &BTreeMap::new(), IoRegion::new()).await.unwrap();
        let prefix = ObjectKey::new(fenced.storage_prefix()).unwrap();
        let objects = bucket.list(Some(&prefix)).await.unwrap();
        assert!(!objects.is_empty());
        for object in objects { bucket.delete(&object.key).await.unwrap(); }
        assert!(bucket.list(Some(&prefix)).await.unwrap().is_empty());

        assert_eq!(restore().await.unwrap_err().code, "test/restore-interrupted");
        let prepared = directory.deployment(&deployment.id).await.unwrap().unwrap();
        assert_eq!(prepared.lifecycle, Lifecycle::Restoring);
        assert_eq!(prepared.epoch, fenced.epoch);
        assert!(bucket.list(Some(&prefix)).await.unwrap().is_empty());

        let finished = restore().await.unwrap();
        assert_eq!(finished.epoch, fenced.epoch);
        assert_eq!(Some(finished.checkpoint), checkpoint);
        assert!(finished.completed_at_ms.is_some());
        assert_eq!(restore().await.unwrap(), finished);
        let recovered = connect_eventually(&cell, "shop").await;
        assert_eq!(order_history(&recovered).await, history);
        assert_eq!(values(&recovered, "SELECT total FROM orders").await, ["11"]);
        drop(recovered);
        cell.stop().await;
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_flush_publishes_a_checkpoint_before_replying() {
    let cell = Cell::start_watching(vec![MemberPool::General], None, Some(Duration::from_secs(60))).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 9)").await.unwrap();
    let database = cell.node(0).server.node().catalog().database("shop").unwrap();
    let position = database.latest_submitted_position().unwrap();
    assert!(cell.directory.deployment_by_name("shop").await.unwrap().unwrap().checkpoint.is_none());

    cell.node(0).agent.flush("shop").await.unwrap();

    let held = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    let checkpoint = held.checkpoint.expect("the flush has a publicly visible checkpoint");
    let key = rtdb_storage::ObjectKey::new(rtdb_format::keys::block_key(rtdb_types::BlockIndex(checkpoint.block)).unwrap()).unwrap();
    let root = database.storage().get(&key).await.unwrap();
    assert_eq!(checkpoint.root_sha256, Some(rtdb_format::catalog::publication_digest(root.as_slice())),
        "a reported flush must publish a receipt for its stored root");
    assert!(held.recovery_manifest_sha256.is_some());
    assert_eq!(held.holder, Some(cell.node(0).lease.id()));
    assert!(checkpoint.source >= position.after_offset(rtdb_types::StorageEpoch(held.log_epoch)));
    drop((database, client));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_snapshot_refuses_a_mismatched_recovery_manifest() {
    let cell = Cell::start_durable(1).await;
    let deployment = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 9)").await.unwrap();
    drop(client);
    let agent = &cell.node(0).agent;
    agent.park("shop").await.unwrap();
    let parked = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
    let expected = parked.recovery_manifest_sha256.unwrap();
    let set_manifest = async |digest| {
        for _ in 0..8 {
            let mut current = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
            let version = current.version;
            current.recovery_manifest_sha256 = Some(digest);
            if cell.directory.replace_deployment(current, version).await.unwrap().applied().is_some() {
                return;
            }
        }
        panic!("the parked record kept changing while setting its recovery manifest");
    };
    let mut wrong = expected;
    wrong[0] ^= 1;
    set_manifest(wrong).await;
    let error = agent.snapshot("shop").await.unwrap_err();
    assert_eq!(error.code, "rtdb.multi/storage-region");

    set_manifest(expected).await;
    agent.snapshot("shop").await.unwrap();
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn park_drains_admission_before_flushing_and_publishes_its_checkpoint() {
    let cell = Cell::start_durable(1).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 9)").await.unwrap();
    drop(client);
    let database = cell.node(0).server.node().catalog().database("shop").unwrap();
    let position = database.latest_submitted_position().unwrap();
    {
        let admission = database.pause_transactions().await.unwrap();
        let park = cell.node(0).agent.park("shop");
        tokio::pin!(park);
        assert!(tokio::time::timeout(Duration::from_millis(120), &mut park).await.is_err());
        assert!(database.status().adopted_block.is_none());
        drop(admission);
        park.await.unwrap();
    }

    let parked = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert!(parked.holder.is_none());
    assert!(parked.recovery_manifest_sha256.is_some());
    let checkpoint = parked.checkpoint.expect("a parked deployment publishes its final checkpoint");
    assert!(checkpoint.source >= position.after_offset(rtdb_types::StorageEpoch(parked.log_epoch)));
    drop(database);
    let client = cell.connect("shop").await;
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["9"]);
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_small_flush_writes_one_pack_and_its_catalog() {
    let cell = Cell::start(1, None).await;
    let record = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    for table in ["orders", "items", "users"] { client.simple_query(&format!("INSERT INTO {table} (_id, v) VALUES (1, 1)")).await.unwrap(); }
    drop(client);
    cell.node(0).agent.park("shop").await.unwrap();
    let root = cell.data.path().join("objects");
    let objects: Vec<String> = files_under(&root.join(record.storage_prefix())).into_iter().map(|path| path.to_string_lossy().into_owned()).collect();
    assert_eq!(objects.iter().filter(|path| path.contains("/~packs/")).count(), 1, "{objects:#?}");
    assert!(!objects.iter().any(|path| path.contains("/blocks/") || path.contains("/l00-")),
        "block metadata and L0 files belong to the pack: {objects:#?}");
    let client = cell.connect("shop").await;
    assert_eq!(values(&client, "SELECT v FROM items").await, ["1"], "a packed block reads back after the park");
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compaction_worker_compacts_a_deployment_its_holder_does_not() {
    let cell = Cell::start_with(vec![MemberPool::General, MemberPool::Compaction], None).await;
    let record = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    for round in 0..6 {
        client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({round}, {round})")).await.unwrap();
        cell.node(0).agent.flush("shop").await.unwrap();
    }
    let root = cell.data.path().join("objects");
    let compacted = || files_under(&root.join(record.storage_prefix())).iter().any(|path| { let path = path.to_string_lossy(); path.contains("/data/l") && !path.contains("/data/l00-") });
    for _ in 0..200 {
        if compacted() { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(compacted(), "the worker wrote compaction outputs");
    assert_eq!(cell.node(1).agent.subscribed(), ["shop"], "the worker holds a compacting replica");
    assert!(cell.node(1).agent.held().is_empty(), "and holds no deployment");
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["0", "1", "2", "3", "4", "5"]);
    drop(client);
    cell.stop().await;
}

#[tokio::test]
async fn controller_probes_can_progress_before_another_member_replies() {
    use rtdb_directory::{Directory, MemberId, MemberRecord, MemberRole, MemberState, MemoryDirectory};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let directory = MemoryDirectory::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let pgwire = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pgwire_address = pgwire.local_addr().unwrap().to_string();
    for id in ["first", "second"] {
        directory.register_member(MemberRecord {
            id: MemberId(id.into()), role: MemberRole::Node, address: pgwire_address.clone(), control_address: Some(address.clone()),
            availability_zone: "local".into(), engine_version: "test".into(), lease_expires_at_ms: rtdb_directory::now_ms() + 30_000,
            state: MemberState::Eligible, drain_target_engine: None, pool: MemberPool::General, storage_namespaces: None, version: 0,
        }).await.unwrap();
    }
    let answer = async {
        let mut peers = Vec::new();
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = BufReader::new(stream);
            let mut line = String::new();
            peer.read_line(&mut line).await.unwrap();
            assert!(line.contains("\"op\":\"ping\""));
            peers.push(peer.into_inner());
        }
        for mut peer in peers {
            let _ = peer.write_all(b"{\"outcome\":\"done\"}\n").await;
        }
    };
    let options = admin::ControllerOptions { suspicion_probes: 1, probe_timeout: Duration::from_millis(500), ..Default::default() };
    let mut failures = std::collections::BTreeMap::new();
    let (confirmed, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(admin::confirm_suspicions(directory.as_ref(), &mut failures, &options), answer)
    }).await.unwrap();
    assert_eq!(confirmed.unwrap(), 0);
    assert!(failures.is_empty());
}

#[tokio::test]
async fn a_missed_serving_probe_counts_only_without_inspection_overflow() {
    use rtdb_directory::{MemberRecord, MemberRole, MemberState};
    use rtdb_multi::control::{self, Handler};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    struct Answers;
    #[async_trait::async_trait]
    impl Handler for Answers {
        async fn handle(&self, request: Request) -> Response {
            assert_eq!(request, Request::Ping);
            Response::Done
        }
    }

    let directory = MemoryDirectory::new();
    let pgwire = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = pgwire.local_addr().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let control_address = listener.local_addr().unwrap();
    let (status, status_view) = tokio::sync::watch::channel(rtdb_pgwire::ServerStatus::default());
    let stop = tokio_util::sync::CancellationToken::new();
    let server = tokio::spawn(control::serve_with_pgwire(listener, Arc::new(Answers), control(), stop.clone(), address, None, status_view));
    let probe = tokio::spawn(async move {
        for count in 0..4 {
            let (mut peer, _) = pgwire.accept().await.unwrap();
            let mut preface = [0; rtdb_pgwire::SERVING_PROBE.len()];
            peer.read_exact(&mut preface).await.unwrap();
            assert_eq!(preface, rtdb_pgwire::SERVING_PROBE);
            if count < 2 { status.send_modify(|current| current.startup_probe_overflows += 1); }
        }
    });
    let member = directory.register_member(MemberRecord {
        id: MemberId("writer".into()), role: MemberRole::Node, address: address.to_string(),
        control_address: Some(control_address.to_string()), availability_zone: "local".into(),
        engine_version: "test".into(), lease_expires_at_ms: rtdb_directory::now_ms() + 30_000,
        state: MemberState::Eligible, drain_target_engine: None, pool: MemberPool::General, storage_namespaces: None, version: 0,
    }).await.unwrap();
    let options = admin::ControllerOptions { control: common::control(), ..Default::default() };
    let mut failures = BTreeMap::new();
    for _ in 0..2 {
        assert_eq!(admin::confirm_suspicions(directory.as_ref(), &mut failures, &options).await.unwrap(), 0);
        assert!(failures.is_empty());
    }
    assert_eq!(admin::confirm_suspicions(directory.as_ref(), &mut failures, &options).await.unwrap(), 0);
    assert_eq!(admin::confirm_suspicions(directory.as_ref(), &mut failures, &options).await.unwrap(), 1);
    assert_eq!(directory.member(&member.id).await.unwrap().unwrap().state, MemberState::Dead);
    probe.await.unwrap();
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn the_controller_cannot_confirm_an_endpoint_changed_while_its_probe_is_pending() {
    use rtdb_directory::{MemberRecord, MemberRole, MemberState};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    let directory = MemoryDirectory::new();
    let control = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unreachable = silent.local_addr().unwrap().to_string();
    drop(silent);
    let replacement = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let member = directory.register_member(MemberRecord {
        id: MemberId("node".into()), role: MemberRole::Node, address: unreachable,
        control_address: Some(control.local_addr().unwrap().to_string()), availability_zone: "local".into(),
        engine_version: "test".into(), lease_expires_at_ms: rtdb_directory::now_ms() + 30_000,
        state: MemberState::Eligible, drain_target_engine: None, pool: MemberPool::General, storage_namespaces: None, version: 0,
    }).await.unwrap();
    let id = member.id.clone();
    let replace_during_probe = async {
        let (peer, _) = control.accept().await.unwrap();
        let mut peer = BufReader::new(peer);
        let mut request = String::new();
        peer.read_line(&mut request).await.unwrap();
        let mut replacement_member = member.clone();
        replacement_member.address = replacement.local_addr().unwrap().to_string();
        assert!(directory.replace_member(replacement_member, member.version).await.unwrap().applied().is_some());
        peer.get_mut().write_all(b"{\"outcome\":\"done\"}\n").await.unwrap();
    };
    let options = admin::ControllerOptions { suspicion_probes: 1, probe_timeout: Duration::from_millis(200), ..Default::default() };
    let mut failures = BTreeMap::new();
    let (confirmed, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(admin::confirm_suspicions(directory.as_ref(), &mut failures, &options), replace_during_probe)
    }).await.unwrap();
    assert_eq!(confirmed.unwrap(), 0);
    assert_eq!(directory.member(&id).await.unwrap().unwrap().state, MemberState::Eligible);
}

#[tokio::test]
async fn the_controller_counts_failures_at_each_endpoint_independently() {
    use rtdb_directory::{Directory, MemberId, MemberRecord, MemberRole, MemberState, MemoryDirectory};

    let directory = MemoryDirectory::new();
    let original = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replacement = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = original.local_addr().unwrap().to_string();
    let mut member = directory.register_member(MemberRecord {
        id: MemberId("node".into()), role: MemberRole::Node, address: address.clone(), control_address: Some(address),
        availability_zone: "local".into(), engine_version: "test".into(), lease_expires_at_ms: rtdb_directory::now_ms() + 30_000,
        state: MemberState::Eligible, drain_target_engine: None, pool: MemberPool::General, storage_namespaces: None, version: 0,
    }).await.unwrap();
    let options = admin::ControllerOptions { suspicion_probes: 2, probe_timeout: Duration::from_millis(20), ..Default::default() };
    let mut failures = std::collections::BTreeMap::new();
    assert_eq!(admin::confirm_suspicions(directory.as_ref(), &mut failures, &options).await.unwrap(), 0);

    member.control_address = Some(replacement.local_addr().unwrap().to_string());
    let version = member.version;
    assert!(directory.replace_member(member, version).await.unwrap().applied().is_some());
    assert_eq!(admin::confirm_suspicions(directory.as_ref(), &mut failures, &options).await.unwrap(), 0);
    assert_eq!(admin::confirm_suspicions(directory.as_ref(), &mut failures, &options).await.unwrap(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_controller_confirms_a_silent_node_dead_before_its_lease_runs_out() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);
    let status = admin::cell_status(cell.directory.as_ref(), 1).await.unwrap();
    assert_eq!((status.active, status.parked, status.recommended_nodes), (1, 0, 1));

    let silent = cell.holder("shop").await.unwrap();
    let silent_id = cell.node(silent).lease.id();
    let options = admin::ControllerOptions { probe_timeout: Duration::from_millis(100), control: control(), ..Default::default() };
    let mut failures = std::collections::BTreeMap::new();
    assert_eq!(admin::confirm_suspicions(cell.directory.as_ref(), &mut failures, &options).await.unwrap(), 0, "a node that answers is not suspected");
    // Simulate a hung agent: its probes go unanswered while its lease is still being renewed.
    let hung = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut record = cell.directory.member(&silent_id).await.unwrap().unwrap();
    for _ in 0..8 {
        let mut next = record.clone();
        next.control_address = Some(hung.local_addr().unwrap().to_string());
        match cell.directory.replace_member(next, record.version).await.unwrap() { rtdb_directory::Cas::Applied(_) => break, rtdb_directory::Cas::Conflict(current) => record = current.unwrap() }
    }
    let mut confirmed = 0;
    for _ in 0..2 { confirmed += admin::confirm_suspicions(cell.directory.as_ref(), &mut failures, &options).await.unwrap(); }
    assert_eq!(confirmed, 1, "two unanswered probes confirm it");
    assert_eq!(cell.directory.member(&silent_id).await.unwrap().unwrap().state, rtdb_directory::MemberState::Dead);
    let client = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    assert_ne!(cell.holder("shop").await, Some(silent), "another node took over without waiting out the lease");
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saturated_pgwire_sessions_do_not_make_the_controller_depose_a_writer() {
    let cell = Cell::start(1, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();

    let address = cell.node(0).pgwire_address.to_string();
    let mut sessions = vec![client];
    tokio::time::timeout(Duration::from_secs(15), async {
        for _ in 1..common::pgwire_connections() { sessions.push(common::connect(&address, "shop").await); }
    }).await.unwrap();

    let options = admin::ControllerOptions { control: common::control(), ..Default::default() };
    let mut failures = BTreeMap::new();
    for _ in 0..options.suspicion_probes {
        assert_eq!(admin::confirm_suspicions(cell.directory.as_ref(), &mut failures, &options).await.unwrap(), 0);
    }
    assert!(failures.is_empty());
    assert_eq!(values(&sessions[0], "SELECT total FROM orders").await, ["10"]);
    drop(sessions);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_only_failure_promotes_a_writer_and_unblocks_gateway_startup() {
    exercise_pgwire_failover(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_that_accepts_tcp_without_serving_pgwire_is_replaced_during_startup() {
    exercise_pgwire_failover(true).await;
}

async fn exercise_pgwire_failover(stalls_after_connect: bool) {
    use rtdb_directory::{Cas, MemberState};
    use rtdb_multi::control::{self, Handler, Request, Response};
    use tokio::net::TcpListener;

    struct UnreachableWriter {
        address: String,
        epoch: u64,
        activated: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl Handler for UnreachableWriter {
        async fn handle(&self, request: Request) -> Response {
            match request {
                Request::Ping => Response::Done,
                Request::Activate { .. } => {
                    self.activated.notify_one();
                    Response::Held { address: self.address.clone(), epoch: self.epoch }
                }
                request => panic!("unexpected request: {request:?}"),
            }
        }
    }

    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);
    let old = cell.holder("shop").await.unwrap();
    let member = cell.node(old).lease.id();
    let epoch = cell.directory.deployment_by_name("shop").await.unwrap().unwrap().epoch;

    let data_endpoint = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let data_address = data_endpoint.local_addr().unwrap();
    let unreachable = data_address.to_string();
    let stalled_listener = stalls_after_connect.then_some(data_endpoint);
    let agent_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let agent_address = agent_listener.local_addr().unwrap().to_string();
    let agent = Arc::new(UnreachableWriter { address: unreachable.clone(), epoch, activated: Default::default() });
    let stop = tokio_util::sync::CancellationToken::new();
    let running = if stalls_after_connect {
        tokio::spawn(control::serve_with_pgwire(agent_listener, agent.clone(), common::control(), stop.clone(), data_address, None, tokio::sync::watch::channel(Default::default()).1))
    } else {
        tokio::spawn(control::serve(agent_listener, agent.clone(), common::control(), stop.clone()))
    };
    loop {
        let mut record = cell.directory.member(&member).await.unwrap().unwrap();
        record.address = unreachable.clone();
        record.control_address = Some(agent_address.clone());
        let version = record.version;
        if let Cas::Applied(_) = cell.directory.replace_member(record, version).await.unwrap() { break; }
    }

    let gateway = cell.gateway;
    let reconnect = tokio::spawn(async move {
        tokio_postgres::Config::new().host(&gateway.ip().to_string()).port(gateway.port())
            .user("xtdb").password("cell-test").dbname("shop").connect(NoTls).await
    });
    tokio::time::timeout(Duration::from_secs(5), agent.activated.notified()).await.unwrap();

    let options = admin::ControllerOptions { control: common::control(), ..Default::default() };
    let mut failures = std::collections::BTreeMap::new();
    assert_eq!(admin::confirm_suspicions(cell.directory.as_ref(), &mut failures, &options).await.unwrap(), 0);
    assert_eq!(admin::confirm_suspicions(cell.directory.as_ref(), &mut failures, &options).await.unwrap(), 1);
    assert_eq!(cell.directory.member(&member).await.unwrap().unwrap().state, MemberState::Dead);
    assert!(matches!(control::call(&agent_address, &Request::Ping, Duration::from_secs(1), &common::control()).await.unwrap(), Response::Done));
    let (client, driver) = tokio::time::timeout(Duration::from_secs(5), reconnect).await.unwrap().unwrap().unwrap();
    let driver = tokio::spawn(driver);
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    assert_ne!(cell.holder("shop").await, Some(old));
    drop(client);
    driver.await.unwrap().unwrap();
    drop(stalled_listener);
    stop.cancel();
    running.await.unwrap().unwrap();
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_waits_for_replacement_when_the_writer_control_port_closes() {
    use rtdb_directory::MemberState;
    use tokio::net::TcpListener;

    let cell = Cell::start(2, None).await;
    let deployment = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);
    let original = cell.holder("shop").await.unwrap();
    let member = cell.node(original).lease.id();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let closed = listener.local_addr().unwrap();
    drop(listener);
    loop {
        let mut record = cell.directory.member(&member).await.unwrap().unwrap();
        let previous = record.version;
        record.control_address = Some(closed.to_string());
        if cell.directory.replace_member(record, previous).await.unwrap().applied().is_some() { break; }
    }

    let gateway = cell.gateway;
    let mut startup = tokio::spawn(async move {
        tokio_postgres::Config::new().host("127.0.0.1").port(gateway.port())
            .user("xtdb").password("cell-test").dbname("shop").connect(NoTls).await
    });
    assert!(tokio::time::timeout(Duration::from_millis(250), &mut startup).await.is_err());
    let options = admin::ControllerOptions { control: control(), ..Default::default() };
    let mut failures = BTreeMap::new();
    for _ in 0..options.suspicion_probes {
        admin::confirm_suspicions(cell.directory.as_ref(), &mut failures, &options).await.unwrap();
    }
    assert_eq!(cell.directory.member(&member).await.unwrap().unwrap().state, MemberState::Dead);
    let (client, driver) = tokio::time::timeout(Duration::from_secs(5), startup).await.unwrap().unwrap().unwrap();
    let driver = tokio::spawn(driver);
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    assert_eq!(cell.directory.deployment(&deployment.id).await.unwrap().unwrap().holder, Some(cell.node(1 - original).lease.id()));
    drop(client);
    driver.await.unwrap().unwrap();
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_asymmetric_gateway_stall_uses_the_existing_writer_and_preserves_authentication() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};

    let cell = Cell::start(2, None).await;
    let original = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);
    let holder = cell.holder("shop").await.unwrap();
    let member = cell.node(holder).lease.id();
    let bound = cell.directory.deployment(&original.id).await.unwrap().unwrap();
    let actual_control = cell.node(holder).control_address;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = listener.local_addr().unwrap();
    let stalls = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stalled_address = stalls.local_addr().unwrap();
    let relays = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts = relays.clone();
    let proxy = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        while let Ok((mut gateway, _)) = listener.accept().await {
            let attempts = attempts.clone();
            tokio::spawn(async move {
                let mut request = String::new();
                BufReader::new(&mut gateway).read_line(&mut request).await.unwrap();
                let op: serde_json::Value = serde_json::from_str(&request).unwrap();
                let mut target = TcpStream::connect(actual_control).await.unwrap();
                target.write_all(request.as_bytes()).await.unwrap();
                if op["op"] == "activate" || op["op"] == "ping" {
                    let mut reply = String::new();
                    BufReader::new(&mut target).read_line(&mut reply).await.unwrap();
                    let mut reply: serde_json::Value = serde_json::from_str(&reply).unwrap();
                    if reply["outcome"] == "held" {
                        reply["address"] = stalled_address.to_string().into();
                    }
                    if op["op"] == "ping" {
                        let overflows = reply["startup_probe_overflows"].as_u64().unwrap();
                        reply["startup_probe_overflows"] = (overflows + 1).into();
                    }
                    let _ = gateway.write_all(format!("{}\n", serde_json::to_string(&reply).unwrap()).as_bytes()).await;
                } else {
                    if op["op"] == "open_pgwire" { attempts.fetch_add(1, Ordering::Relaxed); }
                    let _ = tokio::io::copy_bidirectional(&mut gateway, &mut target).await;
                }
            });
        }
    }));
    loop {
        let mut record = cell.directory.member(&member).await.unwrap().unwrap();
        let version = record.version;
        record.control_address = Some(proxy_address.to_string());
        if cell.directory.replace_member(record, version).await.unwrap().applied().is_some() { break; }
    }

    let options = admin::ControllerOptions { control: control(), ..Default::default() };
    let mut failures = BTreeMap::new();
    assert_eq!(admin::confirm_suspicions(cell.directory.as_ref(), &mut failures, &options).await.unwrap(), 0);
    assert!(failures.is_empty());
    let bad = tokio::time::timeout(Duration::from_secs(6), tokio_postgres::Config::new()
        .host("127.0.0.1").port(cell.gateway.port()).user("xtdb").dbname("shop").password("wrong").connect(NoTls)).await.unwrap();
    assert_eq!(bad.err().unwrap().as_db_error().unwrap().code().code(), "28P01");

    let client = tokio::time::timeout(Duration::from_secs(6), cell.connect("shop")).await.unwrap();
    client.simple_query("UPDATE orders SET total = total + 1").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["11"]);
    assert_eq!(relays.load(Ordering::Relaxed), 2);
    assert_eq!(cell.holder("shop").await, Some(holder));
    assert_eq!(cell.directory.deployment(&original.id).await.unwrap().unwrap().epoch, bound.epoch);
    assert_eq!(admin::confirm_suspicions(cell.directory.as_ref(), &mut failures, &options).await.unwrap(), 0);
    assert!(failures.is_empty());

    drop((client, stalls, proxy));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_holder_moves_a_degraded_stream_to_live_logkeepers() {
    let mut cell = Cell::start(2, None).await;
    cell.add_logkeeper().await;
    let record = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);

    fn position(cell: &Cell, id: &rtdb_directory::MemberId) -> Option<usize> { cell.logkeepers.iter().position(|logkeeper| logkeeper.lease.id() == *id) }
    let first_dead = position(&cell, &record.ensemble[0]).unwrap();
    let dead_id = record.ensemble[0].clone();
    let retired = rtdb_types::JournalEndpoint::Member { address: cell.logkeepers[first_dead].address.to_string(), journal: dead_id.0.clone() };
    cell.logkeepers.remove(first_dead).stop().await.unwrap();
    tokio::time::sleep(Duration::from_millis(1700)).await;
    let holder = cell.holder("shop").await.unwrap();
    cell.node(holder).agent.park("shop").await.unwrap();

    let client = cell.connect("shop").await;
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    let moved = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert!(!moved.ensemble.contains(&dead_id), "the dead logkeeper left the ensemble: {:?}", moved.ensemble);
    assert_eq!(moved.ensemble.len(), 3);
    assert!(moved.replica_journals.contains(&retired));
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();

    let second = record.ensemble.iter().skip(1).find_map(|id| position(&cell, id)).unwrap();
    cell.logkeepers.remove(second).stop().await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (3, 30)").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10", "20", "30"], "a second original logkeeper's death leaves a quorum of the new ensemble");
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn journal_replacement_keeps_acknowledged_history_after_interrupted_trim() {
    use rtdb_storage::log::Log;

    let directory = ObservedDirectory::new();
    let mut cell = Cell::start_with_directory(directory.clone()).await;
    cell.add_node().await;
    cell.add_logkeeper().await;
    let deployment = admin::create_deployment(directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10), (2, 20)").await.unwrap();
    let holder = cell.holder("shop").await.unwrap();
    let serving = directory.deployment(&deployment.id).await.unwrap().unwrap();
    let lost_reply = Arc::new(RecoveryGate::default());
    *directory.lost_anchor_reply.lock().unwrap() = Some((serving.epoch, lost_reply.clone()));
    cell.node(holder).agent.flush("shop").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), lost_reply.registered.notified()).await.unwrap();

    client.simple_query("UPDATE orders SET total = 11 WHERE _id = 1").await.unwrap();
    client.simple_query("DELETE FROM orders WHERE _id = 2").await.unwrap();
    let history = values(&client, "SELECT total FROM orders FOR ALL SYSTEM_TIME FOR ALL VALID_TIME").await;
    let latest = values(&client, "SHOW AWAIT_TOKEN").await.pop().unwrap();
    drop(client);

    let checkpointed = directory.deployment(&deployment.id).await.unwrap().unwrap();
    let checkpoint = checkpointed.checkpoint.unwrap();
    let anchor = checkpointed.replica_anchor.as_ref().unwrap();
    assert_eq!(anchor.interval[1], checkpoint.boundary - 1);
    let live = directory.members().await.unwrap().into_iter()
        .map(|member| (member.id, member.address)).collect::<BTreeMap<_, _>>();
    let reader = rtdb_logkeeper::open_reader(rtdb_logkeeper::StreamConfig {
        servers: checkpointed.ensemble.iter().map(|id| live[id].clone().into()).collect(),
        stream: checkpointed.stream("replica"),
        write_quorum: 2,
        maximum_message_bytes: 4 << 20,
    }, Default::default()).await.unwrap();
    let floor = reader.retained_floor().await.unwrap().unwrap().0;
    assert!(floor < checkpoint.boundary, "the published anchor reply has not authorized the trim call");

    let missing = &checkpointed.ensemble[0];
    let index = cell.logkeepers.iter().position(|keeper| keeper.lease.id() == *missing).unwrap();
    cell.logkeepers.remove(index).stop().await.unwrap();
    let writer = cell.nodes[holder].take().unwrap();
    writer.stop_renewing();
    let kill = tokio::spawn(async move { writer.kill().await });
    let member = serving.holder.as_ref().unwrap();
    loop {
        let mut writer = directory.member(member).await.unwrap().unwrap();
        let version = writer.version;
        writer.state = rtdb_directory::MemberState::Dead;
        if directory.replace_member(writer, version).await.unwrap().applied().is_some() { break; }
    }
    lost_reply.resume.notify_one();
    kill.await.unwrap().unwrap();

    let recovered = connect_eventually(&cell, "shop").await;
    let moved = directory.deployment(&deployment.id).await.unwrap().unwrap();
    assert!(moved.epoch > checkpointed.epoch);
    assert_eq!(moved.checkpoint, Some(checkpoint));
    assert_eq!(moved.replica_anchor.as_ref(), Some(anchor));
    assert!(!moved.ensemble.contains(missing));
    assert_eq!(values(&recovered, "SELECT total FROM orders").await, ["11"]);
    assert_eq!(values(&recovered, "SELECT total FROM orders FOR ALL SYSTEM_TIME FOR ALL VALID_TIME").await, history);
    recovered.simple_query(&format!("SET AWAIT_TOKEN TO '{latest}'")).await.unwrap();
    assert_eq!(values(&recovered, "SELECT total FROM orders").await, ["11"]);

    let second = cell.logkeepers.iter().position(|keeper| {
        keeper.lease.id() != *missing && checkpointed.ensemble.contains(&keeper.lease.id())
    }).unwrap();
    cell.logkeepers.remove(second).stop().await.unwrap();
    recovered.simple_query("INSERT INTO orders (_id, total) VALUES (3, 30)").await.unwrap();
    assert_eq!(values(&recovered, "SELECT total FROM orders").await, ["11", "30"]);
    drop(recovered);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletion_drops_a_recorded_copy_outside_the_current_ensemble() {
    use rtdb_multi::logs::{DirectoryRecorder, EnsembleRecorder};
    use rtdb_storage::log::{Log, LogOffset};

    let mut cell = Cell::start_durable(1).await;
    cell.add_logkeeper().await;
    let deployment = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);
    let held = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
    let extra = cell.logkeepers.iter().find(|keeper| !held.ensemble.contains(&keeper.lease.id())).unwrap();
    let extra = rtdb_types::JournalEndpoint::Member { address: extra.address.to_string(), journal: extra.lease.id().0 };
    let stream = rtdb_logkeeper::StreamConfig { servers: vec![extra], stream: held.stream("replica"), write_quorum: 1, maximum_message_bytes: 4 << 20 };
    let recorder = DirectoryRecorder { directory: cell.directory.clone() };
    recorder.reserve_copy(&held.id, held.epoch, &stream.servers).await.unwrap();
    let speculative = rtdb_logkeeper::open_writer(stream.clone(), held.epoch, Default::default()).await.unwrap();
    speculative.enqueue(bytes::Bytes::from_static(b"copied record"), rtdb_types::Timestamp(0)).await.unwrap().durable().await.unwrap();
    assert_eq!(speculative.read_range(LogOffset(0), LogOffset(1)).await.unwrap().len(), 1);
    drop(speculative);

    let receipt = admin::delete_deployment(cell.directory.as_ref(), &cell.storage(), &cell.keys(), "shop", 4 << 20).await.unwrap();
    assert!(rtdb_logkeeper::open_writer(stream, receipt.facts.epoch + 1, Default::default()).await.is_err());
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deposed_holder_refuses_a_linearizable_read_instead_of_serving_it_stale() {
    let cell = Cell::start_watching(vec![MemberPool::General; 2], None, Some(Duration::from_secs(3600))).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    admin::set_linearizable_reads(cell.directory.as_ref(), "shop", true).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"], "a linearizable read is served while the holder holds");
    drop(client);

    let zombie_index = cell.holder("shop").await.unwrap();
    let zombie = cell.node(zombie_index);
    let direct = connect(&zombie.pgwire_address.to_string(), "shop").await;
    let zombie_id = zombie.lease.id();
    zombie.stop_renewing();
    while cell.directory.member(&zombie_id).await.unwrap().unwrap().lease_expires_at_ms > rtdb_directory::now_ms() { tokio::time::sleep(Duration::from_millis(50)).await; }
    let client = connect_eventually(&cell, "shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    assert!(direct.simple_query("SELECT total FROM orders").await.is_err(), "the deposed holder does not answer from its stale state");
    drop((client, direct));
    cell.stop().await;
}

#[derive(Default)]
struct InterruptedCutover {
    map: rtdb_multi::cells::MemoryCellMap,
    attempts: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl rtdb_multi::cells::CellMap for InterruptedCutover {
    async fn register_cell(&self, cell: &str, gateway: &str) -> rtdb_types::Result<()> { self.map.register_cell(cell, gateway).await }
    async fn gateway(&self, cell: &str) -> rtdb_types::Result<Option<String>> { self.map.gateway(cell).await }
    async fn cells(&self) -> rtdb_types::Result<std::collections::BTreeMap<String, String>> { self.map.cells().await }
    async fn cell_of(&self, deployment: &str) -> rtdb_types::Result<Option<String>> { self.map.cell_of(deployment).await }

    async fn assign(&self, deployment: &str, cell: &str, expected: Option<&str>) -> rtdb_types::Result<bool> {
        if expected.is_none() { return self.map.assign(deployment, cell, expected).await; }
        let interrupted = || rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "test/cutover-interrupted", "The cell-map acknowledgement was lost");
        match self.attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed) {
            0 => Err(interrupted()),
            1 => {
                assert!(self.map.assign(deployment, cell, expected).await?);
                Err(interrupted())
            }
            _ => self.map.assign(deployment, cell, expected).await,
        }
    }
}

struct InterruptedInstall {
    keys: rtdb_multi::keys::LocalKeyService,
    failed: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl KeyService for InterruptedInstall {
    async fn withdraw_unit_scope(&self, scope: rtdb_multi::keys::UnitScope, io: &Arc<rtdb_storage::IoRegion>)
        -> rtdb_types::Result<()>
    {
        self.keys.withdraw_unit_scope(scope, io).await
    }
    fn retained_unit_ledger(&self, scope: rtdb_multi::keys::UnitScope, io: Arc<rtdb_storage::IoRegion>)
        -> rtdb_types::Result<Arc<dyn rtdb_storage::sealing::ObjectCipher>>
    {
        self.keys.retained_unit_ledger(scope, io)
    }
    fn custody_namespace(&self) -> rtdb_types::Result<String> { self.keys.custody_namespace() }
    fn hold_local_storage_region(&self, region: uuid::Uuid) -> rtdb_types::Result<Option<std::fs::File>> { self.keys.hold_local_storage_region(region) }
    fn local_storage_region_drained(&self, region: uuid::Uuid) -> rtdb_types::Result<bool> { self.keys.local_storage_region_drained(region) }
    fn forget_local_storage_region(&self, region: uuid::Uuid) -> rtdb_types::Result<()> { self.keys.forget_local_storage_region(region) }
    async fn namespace_id(&self) -> rtdb_types::Result<[u8; 32]> { self.keys.namespace_id().await }
    async fn key(&self, deployment: &str) -> rtdb_types::Result<[u8; 32]> { self.keys.key(deployment).await }
    async fn existing(&self, deployment: &str) -> rtdb_types::Result<[u8; 32]> { self.keys.existing(deployment).await }
    async fn existing_key(&self, deployment: &str) -> rtdb_types::Result<Option<[u8; 32]>> { self.keys.existing_key(deployment).await }
    async fn grant_deployment_key(&self, deployment: &str, binding: [u8; 32]) -> rtdb_storage::Result<rtdb_multi::keys::UnitOutput> {
        self.keys.grant_deployment_key(deployment, binding).await
    }
    async fn copy(&self, from: &str, to: &str) -> rtdb_types::Result<()> { self.keys.copy(from, to).await }
    async fn destroy(&self, deployment: &str) -> rtdb_types::Result<()> { self.keys.destroy(deployment).await }
    async fn sign_deletion(&self, facts: rtdb_directory::DeletionFacts) -> rtdb_types::Result<rtdb_directory::DeletionRecord> { self.keys.sign_deletion(facts).await }
    async fn deletion_verification_key(&self) -> rtdb_types::Result<String> { self.keys.deletion_verification_key().await }

    async fn install(&self, deployment: &str, key: [u8; 32]) -> rtdb_types::Result<()> {
        if !self.failed.swap(true, std::sync::atomic::Ordering::Relaxed) {
            return Err(rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "test/key-install-interrupted", "The destination key service was unavailable"));
        }
        self.keys.install(deployment, key).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_move_resumes_when_preparation_or_the_placement_switch_is_interrupted() {
    exercise_interrupted_cell_move(None, None, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_a_cell_move_resumes_after_key_install_and_cutover_interruptions() {
    let (Some(east), Some(west)) = (s3_storage(), s3_storage()) else {
        eprintln!("RTDB_S3_ENDPOINT is not set: skipped");
        return;
    };
    tokio::time::timeout(Duration::from_secs(240), exercise_interrupted_cell_move(Some(east), Some(west), Some(hosted_test_owner()))).await.unwrap();
}

async fn exercise_interrupted_cell_move(east: Option<CellStorage>, west: Option<CellStorage>, owner: Option<rtdb_directory::HostedTask>) {
    let map = Arc::new(InterruptedCutover::default());
    let east = Cell::start_on(east, vec![MemberPool::General], None, None, Some((map.clone(), "east".into()))).await;
    let west = Cell::start_on(west, vec![MemberPool::General; 2], None, None, Some((map.clone(), "west".into()))).await;
    let (east_storage, west_storage) = (east.storage(), west.storage());
    let east_keys = east.keys();
    let west_keys = InterruptedInstall { keys: west.keys(), failed: Default::default() };
    let from = admin::CellHandle { name: "east", directory: &east.directory, storage: &east_storage, keys: &east_keys, broker: None, maximum_deployments: 10 };
    let to = admin::CellHandle { name: "west", directory: &west.directory, storage: &west_storage, keys: &west_keys, broker: None, maximum_deployments: 10 };
    let (_, original) = admin::create_in_cells(map.as_ref(), std::slice::from_ref(&from), "shop", "acme", 3).await.unwrap();
    let client = east.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    east.node(east.holder("shop").await.unwrap()).agent.flush("shop").await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    let history = order_history(&client).await;
    drop(client);

    let move_shop = || admin::move_cell(map.as_ref(), &from, &to, "shop", 4 << 20, owner.clone());
    assert_eq!(move_shop().await.unwrap_err().code, "test/key-install-interrupted");
    let incomplete = east.directory.deployment(&original.id).await.unwrap().unwrap();
    assert_eq!(incomplete.lifecycle, Lifecycle::Moving);
    let partial = west.directory.deployment(&original.id).await.unwrap().unwrap();
    assert_eq!(partial.lifecycle, Lifecycle::Moving);
    assert!(partial.move_from.is_some());
    assert_eq!(partial.cell_transfer, incomplete.cell_transfer);

    assert_eq!(move_shop().await.unwrap_err().code, "test/cutover-interrupted");
    assert_eq!(map.cell_of("shop").await.unwrap().as_deref(), Some("east"));
    let source = east.directory.deployment(&original.id).await.unwrap().unwrap();
    assert_eq!(source.lifecycle, Lifecycle::Moving);
    assert_eq!(source.cell_transfer, incomplete.cell_transfer);
    assert_eq!(source.epoch, incomplete.epoch);
    let staged = west.directory.deployment(&original.id).await.unwrap().unwrap();
    assert_eq!(staged.lifecycle, Lifecycle::Moving);
    assert_eq!(staged.storage_generation, partial.storage_generation);
    assert!(staged.storage_generation.is_some());
    assert!(staged.move_from.is_none(), "the destination journal copy is fully prepared");

    assert_eq!(move_shop().await.unwrap_err().code, "test/cutover-interrupted");
    assert_eq!(map.cell_of("shop").await.unwrap().as_deref(), Some("west"));
    assert_eq!(west.directory.deployment(&original.id).await.unwrap().unwrap(), staged);

    let arrived = move_shop().await.unwrap();
    assert_eq!(arrived.lifecycle, Lifecycle::Live);
    assert_eq!(arrived.storage_generation, staged.storage_generation);
    let tombstone = east.directory.deployment(&original.id).await.unwrap().unwrap();
    assert_eq!(tombstone.lifecycle, Lifecycle::Deleted);
    move_shop().await.unwrap();
    assert_eq!(east.directory.deployment(&original.id).await.unwrap().unwrap(), tombstone);

    let reader = east.connect("shop").await;
    assert_eq!(values(&reader, "SELECT total FROM orders").await, ["10", "20"]);
    assert_eq!(order_history(&reader).await, history);
    let adopted = west.directory.deployment(&original.id).await.unwrap().unwrap();
    assert!(adopted.move_from.is_none());
    assert_eq!(adopted.ensemble.len(), staged.ensemble.len());
    assert_eq!(adopted.ensemble, staged.ensemble);
    let mut addresses = std::collections::BTreeSet::new();
    for member in &adopted.ensemble {
        assert!(addresses.insert(west.directory.member(member).await.unwrap().unwrap().address));
    }
    drop(reader);
    east.stop().await;
    west.stop().await;
}

struct LoseTransferReply {
    inner: Arc<MemoryCellMap>,
    lost: AtomicBool,
}

#[async_trait::async_trait]
impl CellMap for LoseTransferReply {
    async fn register_cell(&self, cell: &str, gateway: &str) -> Result<()> {
        self.inner.register_cell(cell, gateway).await
    }

    async fn gateway(&self, cell: &str) -> Result<Option<String>> {
        self.inner.gateway(cell).await
    }

    async fn cells(&self) -> Result<BTreeMap<String, String>> {
        self.inner.cells().await
    }

    async fn cell_of(&self, deployment: &str) -> Result<Option<String>> {
        self.inner.cell_of(deployment).await
    }

    async fn assign(&self, deployment: &str, cell: &str, expected: Option<&str>) -> Result<bool> {
        let assigned = self.inner.assign(deployment, cell, expected).await?;
        if assigned && expected.is_some() && !self.lost.swap(true, Ordering::SeqCst) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "test/lost-transfer-reply", "The assignment reply was lost"));
        }
        Ok(assigned)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_deployment_moves_cells_with_its_unflushed_tail_and_its_old_gateway_forwards() {
    let map: Arc<dyn CellMap> = Arc::new(LoseTransferReply { inner: MemoryCellMap::new(), lost: AtomicBool::new(false) });
    let east = Cell::start_in("east", map.clone(), 1).await;
    let west = Cell::start_in("west", map.clone(), 1).await;
    let (east_storage, west_storage, east_keys, west_keys) = (east.storage(), west.storage(), east.keys(), west.keys());
    fn handle<'a>(name: &'a str, cell: &'a Cell, storage: &'a rtdb_multi::deployment::CellStorage, keys: &'a rtdb_multi::keys::LocalKeyService, maximum_deployments: usize) -> admin::CellHandle<'a> {
        admin::CellHandle { name, directory: &cell.directory, storage, keys, broker: None, maximum_deployments }
    }
    let (placed, _) = admin::create_in_cells(map.as_ref(), &[handle("east", &east, &east_storage, &east_keys, 10), handle("west", &west, &west_storage, &west_keys, 0)], "shop", "acme", 3).await.unwrap();
    assert_eq!(placed, "east", "a deployment goes to a cell with room");
    let client = east.connect("shop").await;
    client.simple_query("INSERT INTO anchor_probe (_id) VALUES (1)").await.unwrap();
    east.node(0).agent.flush("shop").await.unwrap();
    let anchor = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let record = east.directory.deployment_by_name("shop").await.unwrap().unwrap();
            if let Some(anchor) = record.replica_anchor { break *anchor; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    drop(client);
    let (preceding, through) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let report = admin::audit(east.directory.as_ref(), &east_keys, "shop", 4 << 20).await.unwrap();
            if report.transactions >= 2
                && let (Some(digest), Some([_, through])) = (report.replica_continued_digest, report.replica_interval)
            {
                break (digest, through);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();

    let source = handle("east", &east, &east_storage, &east_keys, 10);
    let destination = handle("west", &west, &west_storage, &west_keys, 10);
    let error = admin::move_cell(map.as_ref(), &source, &destination, "shop", 4 << 20, None).await.unwrap_err();
    assert_eq!(error.code, "test/lost-transfer-reply");
    let fenced = east.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert_eq!(fenced.lifecycle, Lifecycle::Moving);
    let pending = west.directory.deployment(&fenced.id).await.unwrap().unwrap();
    assert_eq!(pending.lifecycle, Lifecycle::Moving);
    assert!(pending.move_from.is_none(), "the destination recovered the tail before the route changed");
    assert!(fenced.replica_journals.is_disjoint(&pending.replica_journals));
    let replay = admin::audit(west.directory.as_ref(), &west_keys, "shop", 4 << 20).await.unwrap();
    assert!(replay.transactions >= 2 && replay.violations.is_empty());
    assert_eq!(pending.replica_anchor.as_deref(), Some(&anchor));
    let stream = pending.stream("replica");
    assert_eq!(replay.replica_anchor.unwrap().stream, stream);
    let members: BTreeMap<_, _> = west.directory.members().await.unwrap().into_iter().map(|member| (member.id.clone(), member.address)).collect();
    let servers = pending.ensemble.iter().map(|id| members[id].clone().into()).collect::<Vec<_>>();
    let config = rtdb_logkeeper::StreamConfig {
        write_quorum: rtdb_multi::deployment::write_quorum(servers.len()), servers, stream: stream.clone(), maximum_message_bytes: 4 << 20,
    };
    let prefix = rtdb_logkeeper::JournalDigest::from_digest_bytes(stream, anchor.interval, anchor.digest);
    assert_eq!(
        rtdb_logkeeper::certify_committed_prefix(&config, through, Some(&prefix)).await.unwrap().digest,
        preceding,
    );

    let arrived = admin::move_cell(map.as_ref(), &source, &destination, "shop", 4 << 20, None).await.unwrap();
    assert_eq!(arrived.epoch, fenced.epoch);
    assert_eq!(map.cell_of("shop").await.unwrap().as_deref(), Some("west"));
    let through_east = connect_eventually(&east, "shop").await;
    assert_eq!(values(&through_east, "SELECT total FROM orders").await, ["10", "20"], "the unflushed tail moved with it, and the old cell's gateway forwards");
    through_east.simple_query("INSERT INTO orders (_id, total) VALUES (3, 30)").await.unwrap();
    let through_west = west.connect("shop").await;
    assert_eq!(values(&through_west, "SELECT total FROM orders").await, ["10", "20", "30"]);
    let settled = west.directory.deployment(&arrived.id).await.unwrap().unwrap();
    assert!(settled.move_from.is_none(), "activation never reopened the source journals");
    assert_eq!(east.directory.deployment_by_name("shop").await.unwrap().unwrap().lifecycle, rtdb_directory::Lifecycle::Deleted);

    let tombstone = east.directory.deployment_by_name("shop").await.unwrap().unwrap();
    let east_bucket = east.storage().bucket(rtdb_storage::IoRegion::new()).await.unwrap();
    let left_behind = || async { east_bucket.list(Some(&rtdb_storage::ObjectKey::new(tombstone.storage_prefix()).unwrap())).await.unwrap().len() };
    assert!(left_behind().await > 0, "the old cell still holds the deployment's objects right after the move");
    let keys = east.keys();
    assert!(rtdb_multi::store::move_cleanup_due(&tombstone));
    let wrong = admin::finish_disposal(east.directory.as_ref(), &west_storage, &keys, &tombstone.id, 4 << 20).await.unwrap_err();
    assert_eq!(wrong.code, "rtdb.multi/move-storage-custody");
    assert!(left_behind().await > 0);
    let receipt = admin::finish_disposal(east.directory.as_ref(), &east_storage, &keys, &tombstone.id, 4 << 20).await.unwrap();
    assert_eq!(receipt.journals, tombstone.replica_journals);
    assert_eq!(left_behind().await, 0, "the source disposal receipt follows removal of its serving objects");
    assert!(rtdb_multi::keys::KeyService::key(&keys, &tombstone.key_id()).await.is_err(), "the source key stays withdrawn");
    let recovered = admin::move_cell(map.as_ref(), &source, &destination, "shop", 4 << 20, None).await.unwrap();
    assert_eq!(recovered.id, arrived.id);
    assert_eq!(recovered.cell_transfer, arrived.cell_transfer);
    let retired = east.directory.deployment(&tombstone.id).await.unwrap().unwrap();
    assert_eq!(retired.disposal.as_deref(), Some(&receipt));
    assert!(!rtdb_multi::store::move_cleanup_due(&retired));
    assert_eq!(values(&through_west, "SELECT total FROM orders").await, ["10", "20", "30"], "while the deployment reads on in its new cell");
    drop((through_east, through_west));
    east.stop().await;
    west.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_over_its_tail_budget_flushes_its_largest_tail_first() {
    let cell = Cell::start(1, None).await;
    for name in ["big", "small"] { admin::create_deployment(cell.directory.as_ref(), name, "acme", 3).await.unwrap(); }
    let big = cell.connect("big").await;
    for id in 0..40 { big.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, {id})")).await.unwrap(); }
    let small = cell.connect("small").await;
    small.simple_query("INSERT INTO orders (_id, total) VALUES (1, 1)").await.unwrap();
    let node = cell.node(0);
    let rows = |name: &str| node.server.node().catalog().database(name).unwrap().live_rows();
    let small_tail = rows("small");
    assert!(rows("big") > small_tail && small_tail > 0);
    node.agent.bound_tails_for_test(small_tail + 1).await;
    assert_eq!((rows("big"), rows("small")), (0, small_tail), "the largest tail is flushed, and only as much as needed to fit");
    assert_eq!(values(&big, "SELECT COUNT(*) FROM orders").await, ["40"]);
    drop((big, small));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovering_subscriber_keeps_its_suffix_through_a_concurrent_trim() {
    use rtdb_directory::StorageRegion;
    use rtdb_storage::log::{Log, LogOffset};

    let directory = ObservedDirectory::new();
    let trace = std::env::var_os("RTDB_RECOVERY_TRACE_PATH")
        .map(|path| (path, directory.deployment_changes().unwrap()));
    let mut cell = Cell::start_with_directory(directory.clone()).await;
    let deployment = admin::create_deployment(directory.as_ref(), "shop", "acme", 3).await.unwrap();
    admin::set_linearizable_reads(directory.as_ref(), "shop", true).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    cell.node(0).agent.flush("shop").await.unwrap();
    let initial = directory.deployment(&deployment.id).await.unwrap().unwrap().checkpoint.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20), (3, 30)").await.unwrap();
    let subscriber = cell.add_node().await;
    admin::set_read_width(directory.as_ref(), "shop", 2).await.unwrap();
    let gate = Arc::new(RecoveryGate::default());
    *directory.recovery_gate.lock().unwrap() = Some(gate.clone());
    let agent = Arc::clone(&cell.node(subscriber).agent);
    let opening = tokio::spawn(async move { agent.subscribe("shop").await });
    tokio::time::timeout(Duration::from_secs(5), gate.registered.notified()).await.expect("subscriber registered a durable replay hold");

    let held = directory.deployment(&deployment.id).await.unwrap().unwrap();
    let first_required = held.storage_regions.values().find_map(|region| match region {
        StorageRegion::ReplicaReplay { first_required, .. } => Some(*first_required),
        _ => None,
    }).unwrap();
    assert_eq!(first_required, initial.boundary + 1);
    cell.node(0).agent.flush("shop").await.unwrap();
    let updated = directory.deployment(&deployment.id).await.unwrap().unwrap().checkpoint.unwrap();
    assert!(updated.boundary - 1 >= first_required);
    let members = directory.members().await.unwrap().into_iter().map(|member| (member.id.clone(), member.address)).collect::<BTreeMap<_, _>>();
    let servers = held.ensemble.iter().map(|id| members[id].clone().into()).collect();
    let reader = rtdb_logkeeper::open_reader(rtdb_logkeeper::StreamConfig {
        write_quorum: 2, servers, stream: held.stream("replica"), maximum_message_bytes: 4 << 20,
    }, Default::default()).await.unwrap();
    let probe_floor = async || {
        let before = directory.deployment(&deployment.id).await.unwrap().unwrap();
        let floor = reader.retained_floor().await.unwrap().unwrap();
        let after = directory.deployment(&deployment.id).await.unwrap().unwrap();
        (before.version == after.version).then_some((before, floor))
    };
    let (held_record, retained) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some((record, floor)) = probe_floor().await && floor.0 >= first_required { break (record, floor); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("holder trimmed up to the recovering subscriber");
    assert_eq!(retained, LogOffset(first_required), "trim must retain the first required replica record");
    assert_eq!(reader.read_range(LogOffset(first_required), LogOffset(first_required + 1)).await.unwrap().len(), 1);

    gate.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(5), opening).await.unwrap().unwrap().unwrap();
    let database = cell.node(subscriber).server.node().catalog().database("shop").unwrap();
    let mut status = database.watch_status();
    tokio::time::timeout(Duration::from_secs(5), status.wait_for(|status| {
        status.replica_position.offset() >= updated.boundary || matches!(status.role, rtdb_replication::Role::Stopped)
    })).await.unwrap().unwrap();
    assert!(database.status().replica_position.offset() >= updated.boundary, "subscriber applied the newer block boundary");
    let (follower, connection) = tokio_postgres::Config::new().host("127.0.0.1").port(cell.gateway.port())
        .user("xtdb").password("cell-test").dbname("shop").options("-c default_transaction_read_only=on")
        .connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    assert_eq!(cell.node(subscriber).agent.subscribed(), ["shop"]);
    assert_eq!(values(&follower, "SELECT total FROM orders").await, ["10", "20", "30"]);
    let drained_probe = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some((record, floor)) = probe_floor().await
                && floor.0 >= updated.boundary
                && !record.storage_regions.values().any(|region| matches!(region, StorageRegion::ReplicaReplay { .. }))
            {
                break (record.version, floor.0);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("closed replay hold lets the holder finish trimming");
    if let Some((path, mut changes)) = trace {
        let mut revisions = Vec::new();
        loop {
            match changes.try_recv() {
                Ok(record) if record.id == deployment.id => revisions.push(serde_json::json!({
                    "scope": (record.stream("replica"), record.log_epoch),
                    "version": record.version,
                    "checkpoint": record.checkpoint,
                    "manifest": record.recovery_manifest_sha256,
                    "authorized_through": record.replica_anchor.map(|anchor| anchor.interval[1]),
                    "first_required": record.storage_regions.values().filter_map(StorageRegion::first_required_replica).collect::<Vec<_>>()
                })),
                Ok(_) => (),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                Err(error) => panic!("recovery trace lost directory revisions: {error}"),
            }
        }
        let held_probe = (held_record.version, retained.0);
        let trace = serde_json::json!({ "revisions": revisions, "retained_floors": [held_probe, drained_probe] });
        std::fs::write(path, serde_json::to_vec(&trace).unwrap()).unwrap();
    }
    drop((follower, client, database));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hosted_search_rebuilds_after_trim_and_loss_of_postings_and_all_source_tries() {
    use rtdb_format::keys::{CURRENT_STORAGE_VERSION, StorageRoot};
    use rtdb_multi::packing::{PACK_DIRECTORY, encode_pack, read_index};
    use rtdb_storage::{LocalStore, ObjectKey, ObjectStore, log::Log};
    use rtdb_types::{BlockIndex, StorageEpoch, TableRef};

    let mut cell = Cell::start_durable(0).await;
    cell.add_logical_search_node().await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("CREATE SEARCH INDEX docs_body ON docs (body)").await.unwrap();
    for (id, body) in [(1, "audit report report"), (2, "audit report"), (3, "audit memo"), (4, "unrelated")] {
        client.simple_query(&format!("INSERT INTO docs RECORDS {{_id: {id}, body: '{body}'}}")).await.unwrap();
        cell.node(0).agent.flush("shop").await.unwrap();
    }
    let database = cell.node(0).server.node().catalog().database("shop").unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    client.simple_query("INSERT INTO checkpoint_marker (_id) VALUES (1)").await.unwrap();
    cell.node(0).agent.flush("shop").await.unwrap();
    let record = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let record = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
            if record.replica_anchor.as_ref().is_some_and(|anchor| anchor.interval[1] == record.checkpoint.unwrap().boundary - 1) {
                break record;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.unwrap();
    let checkpoint = record.checkpoint.unwrap();
    let members = cell.directory.members().await.unwrap().into_iter()
        .map(|member| (member.id, member.address)).collect::<BTreeMap<_, _>>();
    let reader = rtdb_logkeeper::open_reader(rtdb_logkeeper::StreamConfig {
        servers: record.ensemble.iter().map(|id| members[id].clone().into()).collect(),
        stream: record.stream("replica"),
        write_quorum: 2,
        maximum_message_bytes: 4 << 20,
    }, Default::default()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if reader.retained_floor().await.unwrap().is_some_and(|floor| floor.0 == checkpoint.boundary) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("the logkeepers trimmed the replica prefix before restarting the holder");
    drop(reader);
    let table = TableRef::new("public", "docs");
    let snapshot = database.snapshot();
    let slug = snapshot.table_entry(&table).unwrap().slug.clone();
    let catalog = snapshot.trie_catalog();
    assert!(catalog.current_tries(&table).iter().all(|trie| trie.key.level() > 0));
    assert_eq!(catalog.l0_source(&table, BlockIndex(-1), BlockIndex(i64::MAX)).count(), 4);
    let extent = catalog.search_runs(&table).flat_map(|run| run.blocks.iter().copied()).collect::<std::collections::BTreeSet<_>>();
    assert_eq!(extent.len(), 4);
    drop((snapshot, database, client));
    cell.nodes[0].take().unwrap().kill().await.unwrap();

    let CellStorage::Local { root } = cell.storage() else { unreachable!() };
    let storage_root = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0)).unwrap().key();
    let raw = LocalStore::open(root.join(record.storage_prefix()).join(storage_root)).unwrap();
    let mut removed = 0;
    for pack in raw.list(Some(&ObjectKey::new(PACK_DIRECTORY).unwrap())).await.unwrap() {
        let index = read_index(&raw, &pack.key).await.unwrap();
        let mut objects = BTreeMap::new();
        for (name, entry) in index.entries {
            if name.starts_with(&format!("{}/data/l00-", slug.table_prefix()))
                || name.starts_with(&format!("{}/meta/l00-", slug.table_prefix()))
            {
                removed += 1;
                continue;
            }
            objects.insert(name, raw.get_range(&pack.key, entry.offset..entry.offset + entry.length).await.unwrap());
        }
        let (bytes, _) = encode_pack(&objects, index.previous, index.written_at_ms.unwrap());
        raw.delete(&pack.key).await.unwrap();
        raw.put(&pack.key, bytes).await.unwrap();
    }
    assert_eq!(removed, 8);
    for prefix in [rtdb_format::search_manifest::run_prefix(&slug), rtdb_format::term_directory::page_prefix(&slug)] {
        let objects = raw.list(Some(&ObjectKey::new(prefix).unwrap())).await.unwrap();
        assert!(!objects.is_empty());
        for object in objects { raw.delete(&object.key).await.unwrap(); }
    }

    cell.add_logical_search_node().await;
    let client = connect_eventually(&cell, "shop").await;
    let query = "SELECT _id FROM docs WHERE MATCH(body, 'audit report') ORDER BY _id";
    assert_eq!(values(&client, query).await, ["1", "2"]);
    assert_eq!(values(&client, "SELECT _id FROM docs WHERE MATCH(body, 'audit memo')").await, ["3"]);
    assert_eq!(values(&client, "SELECT _id FROM docs WHERE MATCH(body, 'unrelated')").await, ["4"]);
    let holder = cell.holder("shop").await.unwrap();
    let database = cell.node(holder).server.node().catalog().database("shop").unwrap();
    database.build_artefacts().await.unwrap();
    let rebuilt = database.trie_catalog();
    let runs = rebuilt.search_runs(&table).collect::<Vec<_>>();
    assert_eq!(runs.iter().flat_map(|run| run.blocks.iter().copied()).collect::<std::collections::BTreeSet<_>>(), extent);
    for run in runs {
        let receipt = run.source_receipt.as_ref().unwrap();
        assert_eq!(receipt.checkpoint_boundary(), checkpoint.boundary);
        assert_eq!(receipt.blocks, run.blocks);
        let key = ObjectKey::new(run.object_key(&slug).unwrap()).unwrap();
        assert!(database.object_store().head_fresh(&key).await.is_ok());
    }
    let run = rebuilt.search_runs(&table).next().unwrap();
    let term_directory = rebuilt.term_directory(&table, run.definition_id(), run.definition_version()).unwrap();
    let root = term_directory.page_root().unwrap().unwrap();
    let prefix = rtdb_format::term_directory::page_prefix(&slug);
    let page = ObjectKey::new(rtdb_format::cow_tree::page_key(&prefix, root.digest)).unwrap();
    assert!(database.object_store().head_fresh(&page).await.is_ok());
    assert_eq!(values(&client, query).await, ["1", "2"]);
    drop((client, database));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_is_trimmed_below_its_last_published_block_and_still_recovers() {
    let mut cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    for round in 0..3 {
        for id in 0..5 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({}, 1)", round * 10 + id)).await.unwrap(); }
        let holder = cell.holder("shop").await.unwrap();
        cell.node(holder).agent.flush("shop").await.unwrap();
    }
    client.simple_query("INSERT INTO orders (_id, total) VALUES (99, 1)").await.unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    let record = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    let members: std::collections::BTreeMap<_, _> = cell.directory.members().await.unwrap().into_iter().map(|member| (member.id.clone(), member)).collect();
    let servers = record.ensemble.iter().map(|id| members[id].address.clone().into()).collect();
    let reader = rtdb_logkeeper::open_reader(rtdb_logkeeper::StreamConfig { write_quorum: 2, servers, stream: record.stream("replica"), maximum_message_bytes: 4 << 20 }, Default::default()).await.unwrap();
    use rtdb_storage::log::Log;
    assert!(matches!(reader.read_range(rtdb_storage::log::LogOffset(0), rtdb_storage::log::LogOffset(1)).await, Err(rtdb_storage::Error::UnavailableLogRange { .. })), "the stream is trimmed below the published blocks");
    assert_eq!(record.checkpoint.map(|checkpoint| checkpoint.block), Some(2), "the directory names the last published block, which a cold start starts from");
    let anchor = *record.replica_anchor.unwrap();
    assert_eq!(anchor.interval, [0, record.checkpoint.unwrap().boundary - 1]);
    assert_eq!(anchor.root_incarnation, record.incarnation);
    drop(client);

    let holder = cell.holder("shop").await.unwrap();
    cell.nodes[holder].take().unwrap().kill().await.unwrap();
    let client = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&client, "SELECT COUNT(*) FROM orders").await, ["16"], "a new holder recovers from the block and the trimmed tail");
    client.simple_query("INSERT INTO orders (_id, total) VALUES (100, 1)").await.unwrap();
    let report = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let report = admin::audit(cell.directory.as_ref(), &cell.keys(), "shop", 4 << 20).await.unwrap();
            if report.replica_digests.len() >= 2 { break report; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    assert!(report.replica_interval.unwrap()[0] > 0);
    assert_eq!(report.replica_anchor.unwrap().interval, anchor.interval);
    assert!(report.replica_continued_digest.is_some());
    drop(client);
    cell.add_node().await;
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gateway_session_keeps_its_await_token_across_moves_and_replays_a_clear() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let token = values(&client, "SHOW AWAIT_TOKEN").await;
    assert_eq!(token.len(), 1);
    assert!(!token[0].is_empty());

    let source = cell.holder("shop").await.unwrap();
    let target = 1 - source;
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(target).agent.member(), &control()).await.unwrap();
    assert_eq!(values(&client, "SHOW AWAIT_TOKEN").await, token);
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);

    client.simple_query("SET AWAIT_TOKEN = NULL").await.unwrap();
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(source).agent.member(), &control()).await.unwrap();
    let result = client.simple_query("SHOW AWAIT_TOKEN").await.unwrap();
    let row = result.iter().find_map(|message| match message { tokio_postgres::SimpleQueryMessage::Row(row) => Some(row), _ => None }).unwrap();
    assert_eq!(row.get(0), None);
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gateway_session_restores_settings_and_discard_all_resets_them_across_moves() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    let default_zone = values(&client, "SHOW timezone").await;
    let default_search_path = values(&client, "SHOW search_path").await;
    client.simple_query("SET TIME ZONE 'Europe/Paris'; SET search_path TO 'reports,public'; SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY").await.unwrap();

    let source = cell.holder("shop").await.unwrap();
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(1 - source).agent.member(), &control()).await.unwrap();
    assert_eq!(values(&client, "SHOW timezone").await, ["Europe/Paris"]);
    assert_eq!(values(&client, "SHOW search_path").await, ["reports,public"]);
    client.simple_query("BEGIN").await.unwrap();
    assert!(client.simple_query("INSERT INTO items (_id) VALUES (1)").await.is_err());
    client.simple_query("ROLLBACK").await.unwrap();
    client.simple_query("DISCARD ALL").await.unwrap();

    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(source).agent.member(), &control()).await.unwrap();
    assert_eq!(values(&client, "SHOW timezone").await, default_zone);
    assert_eq!(values(&client, "SHOW search_path").await, default_search_path);
    client.simple_query("BEGIN; INSERT INTO items (_id) VALUES (2); COMMIT").await.unwrap();
    assert_eq!(values(&client, "SELECT _id FROM items").await, ["2"]);
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_statements_keep_their_bound_search_path_across_a_move() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO north.inventory (_id, label) VALUES (1, 'north')").await.unwrap();
    client.simple_query("INSERT INTO south.inventory (_id, label) VALUES (1, 'south')").await.unwrap();
    client.simple_query("SET search_path TO 'north'; PREPARE chosen AS SELECT label FROM inventory").await.unwrap();
    let prepared = client.prepare_typed("SELECT label FROM inventory WHERE _id = $1", &[tokio_postgres::types::Type::INT8]).await.unwrap();
    client.simple_query("SET search_path TO 'south'").await.unwrap();
    assert_eq!(values(&client, "EXECUTE chosen").await, ["north"]);
    assert_eq!(client.query_one(&prepared, &[&1i64]).await.unwrap().get::<_, String>(0), "north");
    assert_eq!(values(&client, "SELECT label FROM inventory").await, ["south"]);

    let source = cell.holder("shop").await.unwrap();
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(1 - source).agent.member(), &control()).await.unwrap();
    assert_eq!(values(&client, "EXECUTE chosen").await, ["north"]);
    assert_eq!(client.query_one(&prepared, &[&1i64]).await.unwrap().get::<_, String>(0), "north");
    assert_eq!(values(&client, "SELECT label FROM inventory").await, ["south"]);
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_and_a_later_holder_reuse_the_same_deployment_disk_cache() {
    let cell = Cell::start(1, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let node = cell.node(0);
    let first = cell.connect("shop").await;
    first.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    node.agent.flush("shop").await.unwrap();
    assert_eq!(values(&first, "SELECT total FROM orders").await, ["10"]);
    drop(first);

    let cache = cell.data.path().join("cache-0");
    let namespaces = || {
        files_under(&cache).iter().map(|path| path.strip_prefix(&cache).unwrap().components().next().unwrap().as_os_str().to_owned())
            .filter(|component| component.to_string_lossy().starts_with("~rtdb-store-")).collect::<std::collections::BTreeSet<_>>()
    };
    let original = namespaces();
    assert_eq!(original.len(), 1);
    node.agent.park("shop").await.unwrap();
    assert!(matches!(node.agent.snapshot("shop").await.unwrap(), Response::Subscribed { .. }));
    assert_eq!(namespaces(), original);
    let next = cell.connect("shop").await;
    assert_eq!(values(&next, "SELECT total FROM orders").await, ["10"]);
    assert_eq!(namespaces(), original);
    drop(next);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_command_that_parses_a_statement_is_resent_whole_after_a_move() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let first = cell.holder("shop").await.unwrap();
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(1 - first).lease.id(), &control()).await.unwrap();
    let statement = client.prepare("SELECT total FROM orders WHERE _id = 1").await.expect("a named statement parsed as the first command after a move");
    assert_eq!(client.query_one(&statement, &[]).await.unwrap().get::<_, i64>(0), 10);
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_commit_reply_replays_on_the_replacement_with_one_effect() {
    use rtdb_directory::Cas;
    use rtdb_multi::control::{self, Handler};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::sync::CancellationToken;

    struct ProxyRoute { control: String, pgwire: String }

    #[async_trait::async_trait]
    impl Handler for ProxyRoute {
        async fn handle(&self, request: Request) -> Response {
            match control::call(&self.control, &request, Duration::from_secs(5), &common::control()).await {
                Ok(Response::Held { epoch, .. }) => Response::Held { epoch, address: self.pgwire.clone() },
                Ok(response) => response,
                Err(error) => Response::refused(&error),
            }
        }
    }

    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let setup = cell.connect("shop").await;
    setup.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(setup);
    let source = cell.holder("shop").await.unwrap();
    let source_member = cell.node(source).lease.id();
    let target = cell.node(1 - source).lease.id();

    let pgwire_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pgwire = pgwire_listener.local_addr().unwrap().to_string();
    let writer = cell.node(source).pgwire_address;
    let committed = Arc::new(tokio::sync::Notify::new());
    let cut = CancellationToken::new();
    let connection_proxy = {
        let committed = Arc::clone(&committed);
        let cut = cut.clone();
        tokio::spawn(async move {
            let (gateway, _) = pgwire_listener.accept().await.unwrap();
            let backend = TcpStream::connect(writer).await.unwrap();
            let (mut gateway_in, mut gateway_out) = gateway.into_split();
            let (mut backend_in, mut backend_out) = backend.into_split();
            let forward_replies = async {
                let mut greeted = false;
                loop {
                    let tag = backend_in.read_u8().await.unwrap();
                    let length = backend_in.read_i32().await.unwrap();
                    assert!((4..1_048_576).contains(&length));
                    let mut body = vec![0; (length - 4) as usize];
                    backend_in.read_exact(&mut body).await.unwrap();
                    if !greeted {
                        gateway_out.write_u8(tag).await.unwrap();
                        gateway_out.write_i32(length).await.unwrap();
                        gateway_out.write_all(&body).await.unwrap();
                        greeted = tag == b'Z';
                    } else if tag == b'C' {
                        committed.notify_one();
                        cut.cancelled().await;
                        return;
                    } else {
                        assert_ne!(tag, b'E', "{}", String::from_utf8_lossy(&body));
                    }
                }
            };
            tokio::select! {
                () = forward_replies => {}
                result = tokio::io::copy(&mut gateway_in, &mut backend_out) => panic!("gateway closed before the reply was cut: {result:?}"),
            }
        })
    };

    let control_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let control_address = control_listener.local_addr().unwrap().to_string();
    let handler = Arc::new(ProxyRoute { control: cell.node(source).control_address.to_string(), pgwire });
    let stop = CancellationToken::new();
    let control_proxy = tokio::spawn(control::serve(control_listener, handler, common::control(), stop.clone()));
    loop {
        let mut member = cell.directory.member(&source_member).await.unwrap().unwrap();
        member.control_address = Some(control_address.clone());
        let version = member.version;
        if let Cas::Applied(_) = cell.directory.replace_member(member, version).await.unwrap() { break; }
    }

    let client = cell.connect("shop").await;
    let pending = tokio::spawn(async move {
        let reply = client.simple_query("UPDATE orders SET total = total + 1 WHERE _id = 1").await;
        (client, reply)
    });
    tokio::time::timeout(Duration::from_secs(10), committed.notified()).await.unwrap();
    admin::move_deployment(cell.directory.as_ref(), "shop", &target, &common::control()).await.unwrap();
    cut.cancel();
    connection_proxy.await.unwrap();

    let (client, reply) = tokio::time::timeout(Duration::from_secs(5), pending).await.unwrap().unwrap();
    assert!(matches!(reply.unwrap().as_slice(), [tokio_postgres::SimpleQueryMessage::CommandComplete(_)]));
    assert_eq!(cell.holder("shop").await, Some(1 - source));
    assert_eq!(values(&client, "SELECT total FROM orders WHERE _id = 1").await, ["11"]);
    drop(client);
    stop.cancel();
    control_proxy.await.unwrap().unwrap();
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_retried_with_its_idempotency_key_commits_once() {
    let mut cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let keyed = |key: &str, id: i64| format!("BEGIN READ WRITE WITH (METADATA = {{idempotency_key: '{key}'}}); INSERT INTO orders (_id, total) VALUES ({id}, 1); COMMIT");
    let client = cell.connect("shop").await;
    client.batch_execute(&keyed("order-1", 1)).await.unwrap();
    client.batch_execute(&keyed("order-1", 1)).await.expect("a retry with a committed key succeeds");
    client.batch_execute(&keyed("order-1", 2)).await.expect_err("a different transaction under the same key is refused");
    assert_eq!(values(&client, "SELECT _id FROM orders").await, ["1"], "and does not run again");
    drop(client);

    let holder = cell.holder("shop").await.unwrap();
    cell.nodes[holder].take().unwrap().kill().await.unwrap();
    let client = connect_eventually(&cell, "shop").await;
    client.batch_execute(&keyed("order-1", 1)).await.expect("the key survives a change of holder");
    client.batch_execute(&keyed("order-2", 4)).await.unwrap();
    assert_eq!(values(&client, "SELECT _id FROM orders").await, ["1", "4"]);
    drop(client);
    cell.add_node().await;
    cell.stop().await;
}

/// The type of the first message a server answers `parameters`' startup with, and its first four body bytes.
async fn greeting(address: &str, parameters: &[(&str, &str)]) -> (u8, [u8; 4]) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut body = 196608i32.to_be_bytes().to_vec();
    for (key, value) in parameters { for field in [key, value] { body.extend_from_slice(field.as_bytes()); body.push(0); } }
    body.push(0);
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(&((body.len() + 4) as i32).to_be_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    let tag = stream.read_u8().await.unwrap();
    let _length = stream.read_i32().await.unwrap();
    let mut first = [0; 4];
    stream.read_exact(&mut first).await.unwrap();
    (tag, first)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_admits_a_capability_only_for_its_deployment_and_the_gateway_never_forwards_a_clients() {
    let cell = Cell::start(1, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    admin::create_deployment(cell.directory.as_ref(), "blog", "acme", 3).await.unwrap();
    drop(cell.connect("shop").await);
    let node = cell.node(0).pgwire_address.to_string();
    let genuine = common::secret().mint("shop", "xtdb", false, Duration::from_secs(30));
    let ok = (b'R', [0, 0, 0, 0]);
    assert_eq!(greeting(&node, &[("user", "xtdb"), ("database", "shop"), ("rtdb.capability", &genuine)]).await, ok, "a genuine capability stands in for the password");
    assert_eq!(greeting(&node, &[("user", "xtdb"), ("database", "blog"), ("rtdb.capability", &genuine)]).await.0, b'E', "another deployment's capability is refused");
    let foreign = rtdb_multi::capability::CellSecret::new(&[9; 32]).unwrap().mint("shop", "xtdb", false, Duration::from_secs(30));
    assert_eq!(greeting(&node, &[("user", "xtdb"), ("database", "shop"), ("rtdb.capability", &foreign)]).await.0, b'E', "another cell's capability is refused");
    assert_eq!(greeting(&cell.gateway.to_string(), &[("user", "xtdb"), ("database", "shop"), ("rtdb.capability", &genuine)]).await, (b'R', [0, 0, 0, 3]), "through the gateway, a client's capability is dropped and the password asked for");
    let control = cell.node(0).control_address.to_string();
    let ping = |key: rtdb_multi::control::ControlKey| { let control = control.clone(); async move { rtdb_multi::control::call(&control, &rtdb_multi::control::Request::Ping, Duration::from_secs(5), &key).await.unwrap() } };
    assert!(matches!(ping(common::control()).await, rtdb_multi::control::Response::Done), "a request signed with the cell's secret is served");
    assert!(matches!(ping(rtdb_multi::control::ControlKey::default()).await, rtdb_multi::control::Response::Refused { .. }), "an unsigned one is refused");
    assert!(matches!(ping(rtdb_multi::control::ControlKey(Some(std::sync::Arc::new(rtdb_multi::capability::CellSecret::new(&[9; 32]).unwrap())))).await, rtdb_multi::control::Response::Refused { .. }), "and so is one signed by another cell");
    for grant in [genuine.clone(), "forged".to_owned(), rtdb_multi::capability::CellSecret::new(&[9; 32]).unwrap().node_grant("shop")] {
        let answer = rtdb_multi::control::call(&control, &rtdb_multi::control::Request::PublishCompaction { name: "shop".into(), notification: Vec::new(), grant }, Duration::from_secs(5), &common::control()).await.unwrap();
        assert!(matches!(answer, rtdb_multi::control::Response::Refused { .. }), "only another of the cell's nodes may ask for a deployment's data or publish for it: {answer:?}");
    }
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_frozen_checkpoint_confirms_only_await_tokens_it_covers() {
    let mut cell = Cell::start_durable(3).await;
    let shop = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let writer = cell.connect("shop").await;
    writer.simple_query("BEGIN READ WRITE; INSERT INTO orders (_id, total) VALUES (1, 10); COMMIT ASYNC").await.unwrap();
    let (_, retained_receipt) = submitted_attempt(&writer).await;
    assert_eq!(values(&writer, "SELECT total FROM orders").await, ["10"]);
    let retained_basis = values(&writer, "SHOW AWAIT_TOKEN").await.pop().unwrap();

    let holder = cell.holder("shop").await.unwrap();
    cell.node(holder).agent.flush("shop").await.unwrap();
    let published = cell.directory.deployment(&shop.id).await.unwrap().unwrap();
    assert!(published.checkpoint.unwrap().root_sha256.is_some());

    writer.simple_query("BEGIN READ WRITE; UPDATE orders SET total = 20 WHERE _id = 1; COMMIT ASYNC").await.unwrap();
    let (_, later_receipt) = submitted_attempt(&writer).await;
    assert_eq!(values(&writer, "SELECT total FROM orders").await, ["20"]);
    let later_basis = values(&writer, "SHOW AWAIT_TOKEN").await.pop().unwrap();
    drop(writer);
    cell.nodes[holder].take().unwrap().kill().await.unwrap();

    let successor = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&successor, "SELECT total FROM orders").await, ["20"]);
    let current = cell.directory.deployment(&shop.id).await.unwrap().unwrap();
    assert!(current.epoch > published.epoch);
    assert_eq!(current.checkpoint, published.checkpoint);

    let (snapshot, connection) = tokio_postgres::Config::new().host("127.0.0.1").port(cell.gateway.port())
        .user("xtdb").password("cell-test").dbname("shop").options("-c rtdb.read=snapshot")
        .connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    assert_eq!(values(&snapshot, "SELECT total FROM orders").await, ["10"]);
    let successor_node = cell.holder("shop").await.unwrap();
    cell.node(successor_node).agent.flush("shop").await.unwrap();
    let advanced = cell.directory.deployment(&shop.id).await.unwrap().unwrap();
    assert!(advanced.checkpoint.unwrap().boundary > current.checkpoint.unwrap().boundary);
    for logkeeper in cell.logkeepers.drain(..) { logkeeper.stop().await.unwrap(); }

    snapshot.simple_query(&format!("SET AWAIT_TOKEN TO '{retained_basis}'")).await.unwrap();
    assert_eq!(values(&snapshot, "SELECT total FROM orders").await, ["10"]);

    snapshot.simple_query(&format!("SET AWAIT_TOKEN TO '{retained_receipt}'")).await.unwrap();
    assert_eq!(values(&snapshot, "SELECT total FROM orders").await, ["10"]);
    assert_ne!(values(&snapshot, "SHOW AWAIT_TOKEN").await, [retained_receipt]);
    assert_eq!(values(&snapshot, "SELECT total FROM orders").await, ["10"]);

    for token in [later_basis, later_receipt] {
        snapshot.simple_query(&format!("SET AWAIT_TOKEN TO '{token}'")).await.unwrap();
        let error = tokio::time::timeout(Duration::from_secs(3), snapshot.simple_query("SELECT total FROM orders"))
            .await.unwrap().expect_err("A token beyond the frozen checkpoint cannot authorize a read");
        assert_eq!(error.as_db_error().unwrap().message(), "This checkpoint of shop does not cover the await token");
    }
    snapshot.simple_query("SET AWAIT_TOKEN = NULL").await.unwrap();
    assert_eq!(values(&snapshot, "SELECT total FROM orders").await, ["10"]);
    drop((snapshot, successor));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_session_reads_the_checkpoint_from_object_storage_on_another_node() {
    let mut cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    for id in 0..5 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, 1)")).await.unwrap(); }
    let holder = cell.holder("shop").await.unwrap();
    cell.node(holder).agent.flush("shop").await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (99, 1)").await.unwrap();
    for _ in 0..50 {
        if cell.directory.deployment_by_name("shop").await.unwrap().unwrap().checkpoint.is_some() { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for logkeeper in cell.logkeepers.drain(..) { logkeeper.stop().await.unwrap(); }

    let (snapshot, connection) = tokio_postgres::Config::new().host("127.0.0.1").port(cell.gateway.port()).user("xtdb").password("cell-test").dbname("shop")
        .options("-c rtdb.read=snapshot").connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    assert_eq!(values(&snapshot, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4"], "a snapshot reads the deployment as of its checkpoint, with no logkeeper up");
    let second = values(&snapshot, "SELECT _system_from FROM orders WHERE _id = 1").await.remove(0);
    assert_eq!(values(&snapshot, &format!("SELECT _id FROM orders FOR SYSTEM_TIME AS OF TIMESTAMP '{second}'")).await, ["0", "1"], "and at any earlier basis");
    assert!(cell.node(1 - holder).agent.subscribed().contains(&"shop".to_owned()), "on a node that does not hold it");
    assert!(snapshot.simple_query("INSERT INTO orders (_id, total) VALUES (7, 1)").await.is_err(), "and it never writes");
    let reader = cell.node(1 - holder);
    assert_eq!(reader.agent.subscribe("shop").await.unwrap_err().code, "rtdb.multi/read-width");
    assert!(reader.agent.subscribed_snapshots().contains(&"shop".to_owned()));
    admin::set_read_width(cell.directory.as_ref(), "shop", 2).await.unwrap();
    assert!(matches!(reader.agent.subscribe("shop").await, Err(_) | Ok(rtdb_multi::control::Response::Subscribed { .. })));
    assert!(!reader.agent.subscribed_snapshots().contains(&"shop".to_owned()), "asked for a replica, a node replaces its snapshot rather than serve it as one");
    drop((snapshot, client));
    cell.gateway_cancellation.cancel();
    for node in cell.nodes.iter_mut().filter_map(Option::take) { node.kill().await.unwrap(); }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn updates_sent_back_to_back_across_a_holders_death_are_neither_lost_nor_doubled() {
    let mut cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO counters (_id, n) VALUES (1, 0)").await.unwrap();
    let holder = cell.holder("shop").await.unwrap();
    let doomed = cell.nodes[holder].take().unwrap();
    let killer = tokio::spawn(async move { tokio::time::sleep(Duration::from_millis(300)).await; doomed.kill().await.unwrap(); });
    // Commands run back to back from before the holder dies until well after, so it dies while one is in flight.
    let mut commands = 0;
    let mut after = 0;
    while after < 20 {
        client.simple_query("UPDATE counters SET n = n + 1 WHERE _id = 1").await.unwrap_or_else(|error| panic!("command {commands} failed across the holder's death: {error:?}"));
        commands += 1;
        if killer.is_finished() { after += 1; }
    }
    killer.await.unwrap();
    assert_eq!(values(&client, "SELECT n FROM counters").await, [commands.to_string()], "every command ran exactly once");
    drop(client);
    cell.add_node().await;
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelined_prepared_writes_commit_individually_before_and_after_a_move() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO counters (_id, n) VALUES (1, 0)").await.unwrap();
    let update = client.prepare_typed("UPDATE counters SET n = n + $1 WHERE _id = 1", &[tokio_postgres::types::Type::INT8]).await.unwrap();
    tokio::try_join!(client.execute(&update, &[&1_i64]), client.execute(&update, &[&2_i64])).unwrap();
    assert_eq!(values(&client, "SELECT n FROM counters").await, ["3"]);

    let holder = cell.holder("shop").await.unwrap();
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(1 - holder).lease.id(), &common::control()).await.unwrap();
    tokio::try_join!(client.execute(&update, &[&4_i64]), client.execute(&update, &[&8_i64])).unwrap();
    assert_eq!(values(&client, "SELECT n FROM counters").await, ["15"]);
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_copy_keeps_the_same_gateway_session_available_for_later_writes() {
    use bytes::Bytes;
    use futures::SinkExt;

    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    tokio::time::timeout(Duration::from_secs(5), async {
        let input = client.copy_in("COPY counters FROM STDIN WITH (FORMAT 'transit-json')").await.unwrap();
        let mut input = std::pin::pin!(input);
        input.as_mut().send(Bytes::from_static(br#"["^ ","_id",1,"n",10]"#)).await.unwrap();
        assert_eq!(input.as_mut().finish().await.unwrap(), 1);
        {
            let abandoned = client.copy_in("COPY counters FROM STDIN WITH (FORMAT 'transit-json')").await.unwrap();
            let mut abandoned = std::pin::pin!(abandoned);
            abandoned.as_mut().send(Bytes::from_static(br#"["^ ","_id",2,"n",80]"#)).await.unwrap();
        }
        client.simple_query("UPDATE counters SET n = n + 2 WHERE _id = 1").await.unwrap();
        assert_eq!(values(&client, "SELECT n FROM counters").await, ["12"]);
    }).await.expect("the extended copy and its next write finish");
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_move_does_not_restore_statements_cleared_by_discard_all() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    let stale = client.prepare("SELECT 42 AS n").await.unwrap();
    assert!(client.query_one(&stale, &[]).await.is_ok());
    client.batch_execute("DISCARD ALL").await.unwrap();

    let holder = cell.holder("shop").await.unwrap();
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(1 - holder).lease.id(), &common::control()).await.unwrap();
    let error = client.query_one(&stale, &[]).await.unwrap_err();
    assert_eq!(error.as_db_error().unwrap().message(), "no prepared statement");
    assert_eq!(values(&client, "SELECT 7 AS n").await, ["7"]);
    drop(client);
    cell.stop().await;
}

/// Every column of every row `sql` returns, sorted.
async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let mut rows: Vec<String> = client.simple_query(sql).await.unwrap().into_iter().filter_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => Some((0..row.len()).map(|column| row.get(column).unwrap_or("NULL").to_owned()).collect::<Vec<_>>().join("|")),
        _ => None,
    }).collect();
    rows.sort();
    rows
}

const FRAGMENT_QUERIES: [&str; 6] = [
    "SELECT * FROM orders",
    "SELECT _id, total FROM orders WHERE total > 2500",
    "SELECT count(*) AS n, sum(total) AS total, max(note) AS note FROM orders",
    "SELECT i._id, i.qty, o.note FROM items AS i JOIN orders AS o ON i.order_id = o._id",
    "SELECT _id, _system_from, _system_to, note FROM orders FOR ALL SYSTEM_TIME WHERE _id < 400",
    "SELECT _id, note FROM orders WHERE note = 'tail' AND _system_from > '2000-01-01T00:00:00Z'",
];

/// A deployment with three tables spanning many trie leaves, flushed to a block and then written past it.
async fn large_deployment(cell: &Cell, name: &str) -> tokio_postgres::Client {
    admin::create_deployment(cell.directory.as_ref(), name, "acme", 3).await.unwrap();
    let client = cell.connect(name).await;
    for chunk in 0..150 {
        let rows: Vec<String> = (chunk * 20..(chunk + 1) * 20).map(|id| format!("({id}, {id}, 'order {id}')")).collect();
        client.simple_query(&format!("INSERT INTO orders (_id, total, note) VALUES {}", rows.join(", "))).await.unwrap();
    }
    for chunk in 0..75 {
        let items: Vec<String> = (chunk * 20..(chunk + 1) * 20).map(|id| format!("('item-{id}', {}, {})", id * 2, id % 9)).collect();
        client.simple_query(&format!("INSERT INTO items (_id, order_id, qty) VALUES {}", items.join(", "))).await.unwrap();
    }
    client.simple_query("INSERT INTO users (_id, name) VALUES (1, 'ada'), (2, 'grace')").await.unwrap();
    client.simple_query("UPDATE orders SET total = total + 0.5 WHERE _id % 7 = 0").await.unwrap();
    let holder = cell.holder(name).await.unwrap();
    cell.node(holder).agent.flush(name).await.unwrap();
    client.simple_query("UPDATE orders SET note = 'tail' WHERE _id BETWEEN 100 AND 300").await.unwrap();
    client.simple_query("DELETE FROM orders WHERE _id BETWEEN 2000 AND 2100").await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total, note) VALUES (5000, 1, 'late'), (5001, 2, 'late')").await.unwrap();
    client
}

async fn fragment_answers(client: &tokio_postgres::Client) -> Vec<Vec<String>> {
    let mut answers = Vec::new();
    for sql in FRAGMENT_QUERIES { answers.push(rows(client, sql).await); }
    answers
}

fn split_above(cell: &Cell, bytes: u64) { for node in cell.nodes.iter().flatten() { node.fragmenter.split_above(bytes); } }

/// The fragments each node read for another node's scans, by node index.
fn fragments_read(cell: &Cell) -> Vec<std::collections::BTreeMap<(String, String, String), u64>> {
    cell.nodes.iter().map(|node| node.as_ref().map(|node| node.agent.fragments_read()).unwrap_or_default()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_reader_keeps_the_cut_of_its_captured_catalog() {
    struct Reader;
    #[async_trait::async_trait]
    impl rtdb_node::SessionIdentity for Reader {
        fn user(&self) -> Option<&str> { Some("reader") }
        async fn revalidate(&self, _: rtdb_types::Timestamp) -> Result<()> { Ok(()) }
        fn authorize_database(&self, _: &str, _: rtdb_node::DatabaseAccess) -> Result<()> { Ok(()) }
    }

    let cell = Cell::start(2, None).await;
    let deployment = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO docs (_id, v) VALUES (1, 17)").await.unwrap();
    let holder = cell.holder("shop").await.unwrap();
    let node = cell.node(holder).server.node();
    let basis = rtdb_node::QueryBasis { snapshot: node.snapshot_basis().unwrap(), replica_capture: None, resume_token: None, has_await_floor: false, snapshot_time: None,
        current_time: node.query_instant(), timezone: "UTC".into() };
    let cancellation = tokio_util::sync::CancellationToken::new();
    let capture = node.query_context("shop", &basis, &Reader, &cancellation).await.unwrap();
    let cut = capture.catalog.database("shop").unwrap().partitions[0].catalog_cut().unwrap();
    let record = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
    let (&reader, pin) = record.catalog_readers.iter().find(|(_, pin)| pin.cut == Some(cut)).unwrap();
    assert!(record.storage_regions.contains_key(&pin.owner));
    assert_eq!(pin.replica_epoch, record.log_epoch);
    assert_eq!(pin.storage_root, rtdb_format::keys::StorageRoot::new(cut.storage_version, cut.storage_epoch).unwrap().key());

    client.simple_query("UPDATE docs SET v = 25 WHERE _id = 1").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let database = node.catalog().database("shop").unwrap();
        while !database.snapshot().catalog_cut().is_some_and(|current| current.through > cut.through) {
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    let next_basis = rtdb_node::QueryBasis { snapshot: node.snapshot_basis().unwrap(), ..basis };
    let next = node.query_context("shop", &next_basis, &Reader, &cancellation).await.unwrap();
    let next_cut = next.catalog.database("shop").unwrap().partitions[0].catalog_cut().unwrap();
    assert!(cut.through < next_cut.through);
    let record = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
    assert_eq!(record.catalog_readers[&reader].cut, Some(cut));
    assert!(record.catalog_readers.values().any(|pin| pin.cut == Some(next_cut)));
    drop(next);
    drop(capture);

    tokio::time::timeout(Duration::from_secs(5), async {
        while cell.directory.deployment(&deployment.id).await.unwrap().unwrap().catalog_readers.contains_key(&reader) {
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fragment_requires_the_opened_residence_and_replica_epoch() {
    use rtdb_plan::{ColumnId, Field, QualifiedTable, Scan, TableBinding, TemporalFilter};
    use rtdb_query::{ScanFragment, TriePrefix};
    use rtdb_types::{Interval, LogicalType, ScalarType, TableRef, TemporalBounds};

    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO docs (_id, v) VALUES (1, 17)").await.unwrap();
    assert_eq!(values(&client, "SELECT v FROM docs").await, ["17"]);
    let holder = cell.holder("shop").await.unwrap();
    let running = cell.node(holder);
    let (residence, basis) = {
        let database = running.server.node().catalog().database("shop").unwrap();
        (database.fragment_residence().unwrap().clone(), database.progress().current().tx_key.unwrap())
    };
    let data_type = LogicalType::Scalar(ScalarType::I64);
    let mut fragment = ScanFragment {
        scan: Scan {
            table: TableBinding { name: QualifiedTable { database: "shop".into(), table: TableRef::new("public", "docs") },
                oid: None, columns: rtdb_types::StructFields::from([("v".into(), data_type.clone())]) },
            columns: vec![Field { id: ColumnId(0), name: "v".into(), qualifier: None, qualified_only: false, unnamed: false, data_type }],
            valid_time: TemporalFilter::All, system_time: TemporalFilter::All, clamp_valid_time: false,
            iids: None, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new(),
        },
        prefix: TriePrefix::new(Vec::new()).unwrap(),
        basis,
        residence: Some(residence.clone()),
        bounds: TemporalBounds { valid_time: Interval::ALL,
            system_time: Interval::from_micros(i64::MIN, basis.system_time.0 + 1).unwrap() },
        timezone: "UTC".into(),
    };
    let matching = running.agent.scan_fragment("shop", &fragment).await.unwrap();
    assert!(!matching.as_slice().is_empty());

    let mut different_storage = residence.clone();
    different_storage.storage = Arc::from("another physical residence");
    let mut different_epoch = residence.clone();
    different_epoch.replica_epoch += 1;
    for (scope, assertion) in [
        (Some(different_storage), "the worker must refuse another storage residence"),
        (Some(different_epoch), "the worker must refuse another replica epoch"),
        (None, "the worker must refuse an unscoped fragment"),
    ] {
        fragment.residence = scope;
        let result = running.agent.scan_fragment("shop", &fragment).await;
        assert_eq!(result.err().map(|error| error.code).as_deref(), Some("rtdb.node/fragment-residence"), "{assertion}");
    }
    fragment.residence = Some(residence);
    let mut wire = serde_json::to_value(Request::ScanFragment { name: "shop".into(), fragment, grant: String::new() }).unwrap();
    assert_eq!(wire["op"], "scan_scoped_fragment");
    wire["op"] = "scan_fragment".into();
    assert!(serde_json::from_value::<Request>(wire).is_err());
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_deployments_scans_read_as_fragments_on_the_nodes_their_slices_rank_first_on() {
    let cell = Cell::start(3, None).await;
    let client = large_deployment(&cell, "big").await;
    let whole = fragment_answers(&client).await;
    assert!(whole[0].len() > 2900 && !whole[3].is_empty() && whole[4].len() > 400 && !whole[5].is_empty());
    assert!(fragments_read(&cell).iter().all(|read| read.is_empty()), "a deployment within one node's cache is scanned whole");

    split_above(&cell, 0);
    let holder = cell.holder("big").await.unwrap();
    let warming = fragment_answers(&client).await;
    for ((sql, whole), warming) in FRAGMENT_QUERIES.iter().zip(&whole).zip(&warming) { assert_eq!(warming, whole, "{sql}"); }
    // A reader without the deployment open refuses and opens a replica on its next pass.
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let refused = cell.node(holder).fragmenter.failures();
    let before: Vec<u64> = fragments_read(&cell).iter().map(|read| read.values().sum()).collect();
    let fragmented = fragment_answers(&client).await;
    for ((sql, whole), fragmented) in FRAGMENT_QUERIES.iter().zip(&whole).zip(&fragmented) { assert_eq!(fragmented, whole, "{sql}"); }
    let first = fragments_read(&cell);
    assert!(first[holder].is_empty(), "the coordinating holder reads its own fragments without sending them to itself");
    assert_eq!(first.iter().zip(&before).filter(|(read, before)| read.values().sum::<u64>() > **before).count(), 2, "both other nodes read the slices that rank them first: {first:?}");
    assert_eq!(cell.node(holder).fragmenter.failures(), refused, "once warm, every fragment sent is read, at or past the holder's basis");

    let record = cell.directory.deployment_by_name("big").await.unwrap().unwrap();
    let members = cell.directory.members().await.unwrap();
    let readers = rtdb_multi::fragments::readers(&members);
    for (index, read) in first.iter().enumerate() {
        for (_, table, prefix) in read.keys() {
            let (schema, table) = table.split_once('.').unwrap();
            let prefix = rtdb_query::TriePrefix::new(prefix.bytes().map(|digit| digit - b'0').collect()).unwrap();
            let key = rtdb_multi::fragments::fragment_key(&record.storage_identity(), &rtdb_types::TableRef::new(schema, table), &prefix);
            let ranked = rtdb_affinity::rank(&key, &readers, |member| member.id.as_str());
            assert_eq!(ranked[0].id, cell.node(index).lease.id(), "{key} is read on the node it ranks first on");
        }
    }

    let again = fragment_answers(&client).await;
    assert_eq!(again, whole);
    let second = fragments_read(&cell);
    for (first, second) in first.iter().zip(&second) {
        assert_eq!(first.keys().collect::<Vec<_>>(), second.keys().collect::<Vec<_>>(), "each slice is read on the same node again");
        assert!(first.iter().all(|(key, count)| second[key] > *count));
    }
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fragment_whose_reader_does_not_answer_is_read_by_the_coordinator() {
    let cell = Cell::start(2, None).await;
    let client = large_deployment(&cell, "big").await;
    let whole = fragment_answers(&client).await;

    // A reader the directory still lists, whose agent no longer listens, ranks first for some of the orders table's slices.
    let closed = { let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(); listener.local_addr().unwrap().to_string() };
    let record = cell.directory.deployment_by_name("big").await.unwrap().unwrap();
    let members = cell.directory.members().await.unwrap();
    let template = members.iter().find(|member| member.role == rtdb_directory::MemberRole::Node).unwrap().clone();
    let orders = rtdb_types::TableRef::new("public", "orders");
    let ranks_first = |fake: &rtdb_directory::MemberRecord| {
        let mut all = members.clone();
        all.push(fake.clone());
        let readers = rtdb_multi::fragments::readers(&all);
        rtdb_query::FragmentDepth::new(2).unwrap().prefixes().iter().any(|prefix| {
            rtdb_affinity::rank(&rtdb_multi::fragments::fragment_key(&record.storage_identity(), &orders, prefix), &readers, |member| member.id.as_str())[0].id == fake.id
        })
    };
    let fake = (0..100).map(|index| rtdb_directory::MemberRecord { id: rtdb_directory::MemberId(format!("vanished-{index}")), control_address: Some(closed.clone()),
        lease_expires_at_ms: rtdb_directory::now_ms() + 3_600_000, ..template.clone() }).find(|fake| ranks_first(fake)).unwrap();
    cell.directory.register_member(fake).await.unwrap();

    split_above(&cell, 0);
    let fragmented = fragment_answers(&client).await;
    for ((sql, whole), fragmented) in FRAGMENT_QUERIES.iter().zip(&whole).zip(&fragmented) { assert_eq!(fragmented, whole, "{sql}"); }
    let holder = cell.holder("big").await.unwrap();
    assert!(cell.node(holder).fragmenter.failures() > 0, "the vanished reader's fragments failed and were read by the holder");
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_at_a_past_block_holds_that_blocks_state_and_diverges_from_it() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    for id in 0..3 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, 1)")).await.unwrap(); }
    let holder = cell.holder("shop").await.unwrap();
    cell.node(holder).agent.flush("shop").await.unwrap();
    for id in 3..6 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, 1)")).await.unwrap(); }
    cell.node(holder).agent.flush("shop").await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (99, 1)").await.unwrap();

    admin::fork_at_block(&cell.directory, &cell.storage(), &cell.keys(), "shop", "shop-then", Some(&cell.fork_broker()), 0).await.unwrap();
    let then = cell.connect("shop-then").await;
    assert_eq!(values(&then, "SELECT _id FROM orders").await, ["0", "1", "2"], "the fork holds the first block's state, and none of what came after");
    then.simple_query("INSERT INTO orders (_id, total) VALUES (50, 1)").await.unwrap();
    let fork_holder = cell.holder("shop-then").await.unwrap();
    cell.node(fork_holder).agent.flush("shop-then").await.unwrap();
    then.simple_query("INSERT INTO orders (_id, total) VALUES (51, 1)").await.unwrap();
    assert_eq!(values(&then, "SELECT _id FROM orders").await, ["0", "1", "2", "50", "51"], "and writes and flushes its own blocks");
    assert_eq!(values(&client, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "5", "99"], "while the parent is untouched");
    drop((then, client));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_at_a_time_after_the_checkpoint_holds_the_tail_up_to_it() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    for id in 0..3 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, 1)")).await.unwrap(); }
    let holder = cell.holder("shop").await.unwrap();
    cell.node(holder).agent.flush("shop").await.unwrap();
    for _ in 0..50 {
        if cell.directory.deployment_by_name("shop").await.unwrap().unwrap().checkpoint.is_some() { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for id in 3..5 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, 1)")).await.unwrap(); }
    let written = values(&client, "SELECT _system_from FROM orders WHERE _id = 4").await.remove(0);
    let then = rtdb_types::Timestamp(chrono::DateTime::parse_from_str(&written, "%Y-%m-%d %H:%M:%S%.f%:z").unwrap().timestamp_micros());
    for id in 5..7 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, 1)")).await.unwrap(); }

    assert!(admin::fork_at_time(&cell.directory, &cell.storage(), &cell.keys(), "shop", "too-early", None, rtdb_types::Timestamp(0), 4 << 20).await.is_err(),
        "a time before the checkpoint is a past block's");
    admin::fork_at_time(&cell.directory, &cell.storage(), &cell.keys(), "shop", "shop-then", Some(&cell.fork_broker()), then, 4 << 20).await.unwrap();
    admin::fork_time_into(
        &cell.directory, &cell.storage(), &cell.keys(), "shop",
        admin::ForkDestination { name: "shop-external-then", org: Some("recipient-org"), hosted_task: None, broker: None },
        None, then, 4 << 20,
    ).await.unwrap();
    let external = connect_eventually(&cell, "shop-external-then").await;
    assert_eq!(values(&external, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4"]);
    drop(external);
    let fork = connect_eventually(&cell, "shop-then").await;
    assert_eq!(values(&fork, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4"], "the fork holds the block and the tail up to the time, and nothing after");
    fork.simple_query("INSERT INTO orders (_id, total) VALUES (50, 1)").await.unwrap();
    assert_eq!(values(&fork, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "50"], "and takes writes of its own");
    assert_eq!(values(&client, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "5", "6"], "while the parent is untouched");
    drop((fork, client));
    let mut cell = cell;
    let fork_holder = cell.holder("shop-then").await.unwrap();
    cell.nodes[fork_holder].take().unwrap().kill().await.unwrap();
    cell.add_node().await;
    let fork = connect_eventually(&cell, "shop-then").await;
    assert_eq!(values(&fork, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "50"], "the copied tail is the fork's history, recovered by its next holder");
    drop(fork);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_organisation_fork_owns_its_checkpoint_and_survives_erasure_of_its_source() {
    use rtdb_format::keys::{CURRENT_STORAGE_VERSION, StorageRoot, block_key};
    use rtdb_multi::{cipher::Cipher, packing::PackDiscovery, reference::Base, store};
    use rtdb_storage::ObjectStore;
    use rtdb_types::BlockIndex;

    let cell = Cell::start(2, None).await;
    let directory = cell.directory.as_ref();
    let storage = cell.storage();
    let keys = cell.keys();
    let parent = admin::create_deployment(directory, "shop", "source-org", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let first = admin::flush_checkpoint(directory, &parent, &control()).await.unwrap().checkpoint.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    drop(client);

    let current = admin::fork_into(
        &cell.directory, &storage, &keys, "shop",
        admin::ForkDestination { name: "shop-current", org: Some("recipient-org"), hosted_task: None, broker: None }, &control(),
    ).await.unwrap();
    let historical = admin::fork_block_into(
        &cell.directory, &storage, &keys, "shop",
        admin::ForkDestination { name: "shop-historical", org: Some("recipient-org"), hosted_task: None, broker: None },
        first.block,
    ).await.unwrap();

    let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await.unwrap();
    let version = StorageRoot::new(CURRENT_STORAGE_VERSION, rtdb_types::StorageEpoch(0)).unwrap().key();
    let root = |record: &DeploymentRecord| format!("{}/{version}", record.storage_prefix());
    let parent_key = keys.existing_key(&parent.key_id()).await.unwrap().unwrap();
    let child_key = keys.existing_key(&historical.key_id()).await.unwrap().unwrap();
    assert_ne!(parent_key, child_key);
    let parent_cipher = Cipher::new(&parent_key).unwrap();
    let child_cipher = Cipher::new(&child_key).unwrap();
    for child in [&current, &historical] {
        assert_eq!(child.parent.as_deref(), Some(parent.id.as_str()));
        assert_eq!((child.org.as_str(), child.lineage.as_str()), ("recipient-org", child.id.as_str()));
        assert!(Base::load(bucket.as_ref(), &root(child)).await.unwrap().objects.is_empty());
    }
    let catalog = rtdb_storage::ObjectKey::new(block_key(BlockIndex(first.block)).unwrap()).unwrap();
    let parent_store = store::open_deployment_store(bucket.clone(), &root(&parent), PackDiscovery::List, store::wall_clock()).await.unwrap();
    let child_store = store::open_deployment_store(bucket.clone(), &root(&historical), PackDiscovery::List, store::wall_clock()).await.unwrap();
    let source = parent_store.get(&catalog).await.unwrap();
    let copied = child_store.get(&catalog).await.unwrap();
    assert_eq!(parent_cipher.open(catalog.as_str(), &source).unwrap(), child_cipher.open(catalog.as_str(), &copied).unwrap());
    assert!(parent_cipher.open(catalog.as_str(), &copied).is_err());

    admin::delete_deployment(directory, &storage, &keys, "shop", 4 << 20).await.unwrap();
    assert_eq!(keys.existing_key(&parent.key_id()).await.unwrap_err().code, "rtdb.multi/key-destroyed");
    let preview = connect_eventually(&cell, "shop-current").await;
    assert_eq!(values(&preview, "SELECT total FROM orders").await, ["10", "20"]);
    let past = connect_eventually(&cell, "shop-historical").await;
    assert_eq!(values(&past, "SELECT total FROM orders").await, ["10"]);
    past.simple_query("INSERT INTO orders (_id, total) VALUES (3, 30)").await.unwrap();
    assert_eq!(values(&past, "SELECT total FROM orders").await, ["10", "30"]);
    drop((preview, past));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_publishes_and_trims_only_after_adopting_a_block_from_its_own_log() {
    use rtdb_storage::log::{Log, LogOffset};

    let cell = Cell::start_watching(vec![rtdb_directory::MemberPool::General], None, Some(Duration::from_secs(60))).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let parent = cell.connect("shop").await;
    for id in 0..8 {
        parent.simple_query(&format!("INSERT INTO orders (_id) VALUES ({id})")).await.unwrap();
    }
    let child = admin::fork(&cell.directory, &cell.storage(), &cell.keys(), "shop", "shop-preview", Some(&cell.fork_broker()), &control()).await.unwrap();
    let preview = cell.connect("shop-preview").await;
    preview.simple_query("INSERT INTO orders (_id) VALUES (8)").await.unwrap();
    let holder = cell.holder("shop-preview").await.unwrap();
    let agent = &cell.node(holder).agent;
    agent.reconcile().await;
    assert!(cell.directory.deployment(&child.id).await.unwrap().unwrap().checkpoint.is_none());
    let members: std::collections::BTreeMap<_, _> = cell.directory.members().await.unwrap().into_iter().map(|member| (member.id.clone(), member)).collect();
    let servers = child.ensemble.iter().map(|id| members[id].address.clone().into()).collect();
    let reader = rtdb_logkeeper::open_reader(rtdb_logkeeper::StreamConfig {
        write_quorum: 2, servers, stream: child.stream("replica"), maximum_message_bytes: 4 << 20,
    }, Default::default()).await.unwrap();
    assert_eq!(reader.read_range(LogOffset(0), LogOffset(1)).await.unwrap().len(), 1);

    agent.flush("shop-preview").await.unwrap();
    agent.reconcile().await;
    let checkpoint = cell.directory.deployment(&child.id).await.unwrap().unwrap().checkpoint.unwrap();
    assert!(checkpoint.boundary > 0 && checkpoint.source >= 0);
    assert!(matches!(reader.read_range(LogOffset(0), LogOffset(1)).await, Err(rtdb_storage::Error::UnavailableLogRange { .. })));
    assert_eq!(reader.read_range(LogOffset(checkpoint.boundary), LogOffset(checkpoint.boundary + 1)).await.unwrap().len(), 1);
    assert_eq!(values(&preview, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "5", "6", "7", "8"]);
    drop((parent, preview));
    cell.stop().await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unsafe_cell_move_leaves_the_source_live() {
    use rtdb_multi::{deployment::CellStorage, keys::{KeyService, LocalKeyService}};

    let map = rtdb_multi::cells::MemoryCellMap::new();
    let east = Cell::start_in("east", map.clone(), 1).await;
    let west = Cell::start_in("west", map.clone(), 0).await;
    let (east_storage, west_storage, east_keys, west_keys) = (east.storage(), west.storage(), east.keys(), west.keys());
    let from = admin::CellHandle { name: "east", directory: &east.directory, storage: &east_storage, keys: &east_keys, broker: None, maximum_deployments: 10 };
    let deployment = admin::create_in_cells(map.as_ref(), std::slice::from_ref(&from), "shop", "acme", 3).await.unwrap().1;
    let client = east.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let before = east.directory.deployment(&deployment.id).await.unwrap().unwrap();

    let object_alias = west.data.path().join("east-objects");
    std::os::unix::fs::symlink(east.data.path().join("objects"), &object_alias).unwrap();
    let aliased_storage = CellStorage::Local { root: object_alias };
    let pack_prefix = format!("{}/v06/~packs", deployment.storage_prefix());
    let nested_storage = CellStorage::Local { root: east.data.path().join("objects").join(&pack_prefix) };
    let object_store = east_storage.bucket(rtdb_storage::IoRegion::new()).await.unwrap();
    let packs = rtdb_storage::ObjectKey::new(pack_prefix).unwrap();
    let old_packs = object_store.list(Some(&packs)).await.unwrap();
    let key_alias = west.data.path().join("east-keys");
    std::os::unix::fs::symlink(east.data.path().join("keys"), &key_alias).unwrap();
    let aliased_keys = LocalKeyService::new(key_alias).unwrap();

    let mut conflicting_key = east_keys.key(&deployment.id).await.unwrap();
    conflicting_key[0] ^= 1;
    let mut destination_residence = deployment.clone();
    destination_residence.incarnation += 1;
    west_keys.install(&destination_residence.key_id(), conflicting_key).await.unwrap();
    for (storage, keys, expected) in [
        (&aliased_storage, &west_keys, "rtdb.multi/shared-cell-storage"),
        (&nested_storage, &west_keys, "rtdb.multi/shared-cell-storage"),
        (&west_storage, &aliased_keys, "rtdb.multi/shared-cell-keys"),
        (&west_storage, &west_keys, "rtdb.multi/key-conflict"),
    ] {
        let to = admin::CellHandle { name: "west", directory: &west.directory, storage, keys, broker: None, maximum_deployments: 10 };
        let error = admin::move_cell(map.as_ref(), &from, &to, "shop", 4 << 20, None).await.unwrap_err();
        assert_eq!(error.code, expected);
        let after = east.directory.deployment(&deployment.id).await.unwrap().unwrap();
        assert_eq!(after.epoch, before.epoch);
        assert_eq!(after.lifecycle, rtdb_directory::Lifecycle::Live);
        assert_eq!(after.holder, before.holder);
        assert!(west.directory.deployment_by_name("shop").await.unwrap().is_none());
    }
    assert_eq!(object_store.list(Some(&packs)).await.unwrap(), old_packs);
    assert_eq!(rtdb_multi::cells::CellMap::cell_of(map.as_ref(), "shop").await.unwrap().as_deref(), Some("east"));
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    drop(client);
    east.stop().await;
    west.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflicting_cell_copy_moves_through_a_fresh_destination_generation() {
    exercise_conflicting_copy(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_managed_transfer_selects_only_the_fresh_attempts_receipt() {
    exercise_conflicting_copy(true).await;
}

async fn exercise_conflicting_copy(managed: bool) {
    let map = Arc::new(MemoryCellMap::default());
    let east = Cell::start_on(None, vec![MemberPool::General], None, None, Some((map.clone(), "east".into()))).await;
    let west = Cell::start_on(None, vec![MemberPool::General; 2], None, None, Some((map.clone(), "west".into()))).await;
    let (east_storage, west_storage) = (east.storage(), west.storage());
    let east_keys = east.keys();
    let west_keys = InterruptedInstall { keys: west.keys(), failed: Default::default() };
    let from = admin::CellHandle { name: "east", directory: &east.directory, storage: &east_storage, keys: &east_keys, broker: None, maximum_deployments: 10 };
    let to = admin::CellHandle { name: "west", directory: &west.directory, storage: &west_storage, keys: &west_keys, broker: None, maximum_deployments: 10 };
    let (_, original) = admin::create_in_cells(map.as_ref(), std::slice::from_ref(&from), "shop", "acme", 3).await.unwrap();
    if managed {
        admin::set_linearizable_reads(east.directory.as_ref(), "shop", true).await.unwrap();
    }
    let client = east.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 30), (2, 10)").await.unwrap();
    client.simple_query("CREATE INDEX order_totals ON orders (total, _id)").await.unwrap();
    east.node(east.holder("shop").await.unwrap()).agent.flush("shop").await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (3, 20)").await.unwrap();
    let history = order_history(&client).await;
    let ordered = "SELECT _id FROM orders WHERE total >= 10 ORDER BY total";
    let by_total = in_order(&client, ordered).await;
    assert_eq!(by_total, ["2", "3", "1"]);
    drop(client);

    let intent = rtdb_directory::CellTransferIntent {
        operation: Some(uuid::Uuid::new_v4()),
        authority: original.id.parse().unwrap(),
        name: "shop".into(),
        source: rtdb_directory::CellResidence { cell: "east".into(), org: "acme".into() },
        destination: rtdb_directory::CellResidence { cell: "west".into(), org: "acme".into() },
    };
    let interrupted = if managed {
        admin::prepare_managed_cell_transfer(&from, &to, &intent, 4 << 20, None).await.map(drop)
    } else {
        admin::move_cell(map.as_ref(), &from, &to, "shop", 4 << 20, None).await.map(drop)
    };
    assert_eq!(interrupted.unwrap_err().code, "test/key-install-interrupted");
    let partial = west.directory.deployment(&original.id).await.unwrap().unwrap();
    let first = partial.cell_transfer.as_ref().unwrap().attempt.clone().unwrap();
    assert_eq!(first.ordinal, 1);
    assert_eq!(partial.storage_generation, Some(first.generation));
    assert!(partial.move_from.is_some());

    let CellStorage::Local { root } = &west_storage else { panic!("a disposable local fixture") };
    let staged_prefix = root.join(partial.storage_prefix());
    let copied = files_under(&staged_prefix).into_iter()
        .find(|path| path.extension().is_some_and(|extension| extension == "pack"))
        .expect("the interrupted attempt staged the checkpoint pack");
    std::fs::write(&copied, b"conflicting immutable bytes").unwrap();

    let stale = rtdb_directory::PreparedCellTransfer {
        intent: intent.clone(),
        source_incarnation: original.incarnation,
        destination_incarnation: partial.incarnation,
        epoch: partial.epoch,
        attempt: Some(first.clone()),
    };
    let arrived = if managed {
        let prepared = admin::prepare_managed_cell_transfer(&from, &to, &intent, 4 << 20, None).await.unwrap();
        assert_eq!(prepared.attempt.as_ref().map(|attempt| attempt.ordinal), Some(2));
        let refused = admin::activate_managed_cell_transfer(&from, &to, &stale).await.unwrap_err();
        assert_eq!(refused.code, "rtdb.multi/cell-transfer", "the closed attempt's receipt cannot activate");
        let mut legacy = prepared.clone();
        legacy.attempt = None;
        assert!(admin::activate_managed_cell_transfer(&from, &to, &legacy).await.is_err());
        assert_eq!(admin::prepare_managed_cell_transfer(&from, &to, &intent, 4 << 20, None).await.unwrap(), prepared);
        let arrived = admin::activate_managed_cell_transfer(&from, &to, &prepared).await.unwrap();
        admin::dispose_abandoned(&to, &original.id).await.unwrap().unwrap()
            .abandoned_generations.get(&first.generation).unwrap().disposition.as_ref().unwrap();
        assert_eq!(admin::activate_managed_cell_transfer(&from, &to, &stale).await.unwrap_err().code, "rtdb.multi/cell-transfer");
        let _ = arrived;
        west.directory.deployment(&original.id).await.unwrap().unwrap()
    } else {
        admin::move_cell(map.as_ref(), &from, &to, "shop", 4 << 20, None).await.unwrap()
    };
    assert_eq!(arrived.lifecycle, Lifecycle::Live);
    let selected = arrived.cell_transfer.as_ref().unwrap().attempt.clone().unwrap();
    assert_eq!(selected.ordinal, 2);
    assert_ne!(selected.generation, first.generation);
    assert_eq!(arrived.storage_generation, Some(selected.generation));
    assert_eq!(selected.checkpoint, first.checkpoint);
    assert_eq!(selected.sealed_journals, first.sealed_journals);
    let closed = &arrived.abandoned_generations[&first.generation];
    assert_eq!(closed.ordinal, 1);
    assert!(closed.disposition.is_some(), "the closed generation is emptied once the destination is selected");
    assert!(files_under(&staged_prefix).is_empty());
    assert!(!files_under(&root.join(arrived.storage_prefix())).is_empty());
    let tombstone = east.directory.deployment(&original.id).await.unwrap().unwrap();
    assert_eq!(tombstone.lifecycle, Lifecycle::Deleted);
    assert_eq!(tombstone.cell_transfer, arrived.cell_transfer);

    let reader = connect_eventually(&west, "shop").await;
    assert_eq!(values(&reader, "SELECT total FROM orders").await, ["10", "20", "30"]);
    assert_eq!(in_order(&reader, ordered).await, by_total);
    assert_eq!(order_history(&reader).await, history);
    drop(reader);

    let holder = west.holder("shop").await.unwrap();
    let lost = west.node(holder);
    let lost_id = lost.lease.id();
    lost.stop_renewing();
    while west.directory.member(&lost_id).await.unwrap().unwrap().lease_expires_at_ms > rtdb_directory::now_ms() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let recovered = connect_eventually(&west, "shop").await;
    recovered.simple_query("INSERT INTO orders (_id, total) VALUES (4, 40)").await.unwrap();
    assert_eq!(in_order(&recovered, ordered).await, ["2", "3", "1", "4"]);
    assert_eq!(values(&recovered, "SELECT total FROM orders").await, ["10", "20", "30", "40"]);
    drop(recovered);
    east.stop().await;
    west.stop().await;
}

async fn in_order(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    client.simple_query(sql).await.unwrap().into_iter().filter_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        _ => None,
    }).collect()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Replacement { ClosedBeforeSelection, SelectedBeforeInstall, LegacyStaged, LegacyPrepared, LegacyPreparedIntact }

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupted_generation_replacement_resumes_from_either_record() {
    for case in [Replacement::ClosedBeforeSelection, Replacement::SelectedBeforeInstall] {
        exercise_interrupted_replacement(case).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_transfers_without_attempts_recover_or_activate() {
    for case in [Replacement::LegacyStaged, Replacement::LegacyPrepared, Replacement::LegacyPreparedIntact] {
        exercise_interrupted_replacement(case).await;
    }
}

async fn edit(directory: &Arc<dyn Directory>, id: &str, change: impl FnOnce(&mut DeploymentRecord)) {
    let mut record = directory.deployment(id).await.unwrap().unwrap();
    let version = record.version;
    change(&mut record);
    assert!(directory.replace_deployment(record, version).await.unwrap().applied().is_some());
}

async fn exercise_interrupted_replacement(case: Replacement) {
    let map = Arc::new(InterruptedCutover::default());
    let east = Cell::start_on(None, vec![MemberPool::General], None, None, Some((map.clone(), "east".into()))).await;
    let west = Cell::start_on(None, vec![MemberPool::General], None, None, Some((map.clone(), "west".into()))).await;
    let (east_storage, west_storage) = (east.storage(), west.storage());
    let east_keys = east.keys();
    let west_keys = InterruptedInstall { keys: west.keys(), failed: Default::default() };
    let from = admin::CellHandle { name: "east", directory: &east.directory, storage: &east_storage, keys: &east_keys, broker: None, maximum_deployments: 10 };
    let to = admin::CellHandle { name: "west", directory: &west.directory, storage: &west_storage, keys: &west_keys, broker: None, maximum_deployments: 10 };
    let (_, original) = admin::create_in_cells(map.as_ref(), std::slice::from_ref(&from), "shop", "acme", 3).await.unwrap();
    let client = east.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    east.node(east.holder("shop").await.unwrap()).agent.flush("shop").await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    drop(client);
    let id = original.id.clone();
    let move_shop = || admin::move_cell(map.as_ref(), &from, &to, "shop", 4 << 20, None);

    assert_eq!(move_shop().await.unwrap_err().code, "test/key-install-interrupted");
    let partial = west.directory.deployment(&id).await.unwrap().unwrap();
    let first = partial.storage_generation.unwrap();
    let CellStorage::Local { root } = &west_storage else { panic!("a disposable local fixture") };
    let staged_prefix = root.join(partial.storage_prefix());
    let pack = files_under(&staged_prefix).into_iter()
        .find(|path| path.extension().is_some_and(|extension| extension == "pack")).unwrap();
    let closed = rtdb_directory::AbandonedCellGeneration {
        ordinal: 1, namespaces: partial.storage_namespaces.unwrap(), prefix: partial.storage_prefix(),
        closed_at_ms: rtdb_directory::now_ms(), first_empty_at_ms: None, disposition: None, detached_replay: None,
    };
    let strip = |record: &mut DeploymentRecord| { record.cell_transfer.as_mut().unwrap().attempt = None; };
    let expected_ordinal = match case {
        Replacement::ClosedBeforeSelection => {
            edit(&west.directory, &id, |record| { record.abandoned_generations.insert(first, closed.clone()); }).await;
            Some(2)
        }
        Replacement::SelectedBeforeInstall => {
            edit(&west.directory, &id, |record| { record.abandoned_generations.insert(first, closed.clone()); }).await;
            edit(&east.directory, &id, |record| {
                let attempt = record.cell_transfer.as_mut().unwrap().attempt.as_mut().unwrap();
                attempt.ordinal = 2;
                attempt.generation = uuid::Uuid::new_v4();
            }).await;
            Some(2)
        }
        Replacement::LegacyStaged => {
            std::fs::write(&pack, b"conflicting immutable bytes").unwrap();
            edit(&west.directory, &id, strip).await;
            edit(&east.directory, &id, strip).await;
            Some(1)
        }
        Replacement::LegacyPrepared => {
            assert_eq!(move_shop().await.unwrap_err().code, "test/cutover-interrupted");
            let prepared = west.directory.deployment(&id).await.unwrap().unwrap();
            assert!(prepared.move_from.is_none());
            std::fs::write(&pack, b"conflicting immutable bytes").unwrap();
            edit(&west.directory, &id, strip).await;
            edit(&east.directory, &id, strip).await;
            None
        }
        Replacement::LegacyPreparedIntact => {
            assert_eq!(move_shop().await.unwrap_err().code, "test/cutover-interrupted");
            edit(&west.directory, &id, strip).await;
            edit(&east.directory, &id, strip).await;
            let arrived = loop {
                match move_shop().await {
                    Ok(arrived) => break arrived,
                    Err(error) if error.code == "test/cutover-interrupted" => continue,
                    Err(error) => panic!("{error:?}"),
                }
            };
            assert_eq!(arrived.storage_generation, Some(first));
            assert!(arrived.cell_transfer.as_ref().unwrap().attempt.is_none());
            assert!(arrived.abandoned_generations.is_empty());
            let reader = connect_eventually(&west, "shop").await;
            assert_eq!(values(&reader, "SELECT total FROM orders").await, ["10", "20"]);
            drop(reader);
            east.stop().await;
            west.stop().await;
            return;
        }
    };
    let arrived = loop {
        match move_shop().await {
            Ok(arrived) => break arrived,
            Err(error) if error.code == "test/cutover-interrupted" => continue,
            Err(_) if case == Replacement::LegacyPrepared => {
                // Prepared bytes are verified again before activation, and a
                // prepared attempt is never replaced by a fresh generation.
                assert_eq!(map.cell_of("shop").await.unwrap().as_deref(), Some("east"));
                assert_eq!(east.directory.deployment(&id).await.unwrap().unwrap().lifecycle, Lifecycle::Moving);
                assert_eq!(west.directory.deployment(&id).await.unwrap().unwrap().storage_generation, Some(first));
                assert!(west.directory.deployment(&id).await.unwrap().unwrap().abandoned_generations.is_empty());
                east.stop().await;
                west.stop().await;
                return;
            }
            Err(error) => panic!("{error:?}"),
        }
    };
    let attempt = arrived.cell_transfer.as_ref().unwrap().attempt.as_ref().map(|attempt| attempt.ordinal);
    assert_eq!(attempt, expected_ordinal);
    assert_ne!(arrived.storage_generation, Some(first));
    assert!(arrived.abandoned_generations[&first].disposition.is_some());
    assert!(files_under(&staged_prefix).is_empty());
    let reader = connect_eventually(&west, "shop").await;
    assert_eq!(values(&reader, "SELECT total FROM orders").await, ["10", "20"]);
    drop(reader);
    east.stop().await;
    west.stop().await;
}
