//! A node's agent: the member that claims, opens, watches, parks and gives up deployments on one node (`Claim`, `OpenHolding`, `Park`, `DeposeOnRefusedAppend`, `ParkUnderPressure`, `CountActivationCrash`), and serves log-fed read replicas (`Subscription`).
//!
//! The agent never decides safety.
//! The directory's version check decides who holds an epoch, and the logkeepers refuse every append from an earlier one; the agent only keeps what this node runs in step with what the directory says.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_directory::{Cas, Checkpoint, CheckpointSuffixCustody, CheckpointSuffixPin, DeploymentRecord, Directory, Lifecycle, MAX_RETAINED_WORLDS, MemberId, MemberRecord, MemberState, Version, claim, effective_holder, now_ms, park, suspend_holding};
use rtdb_format::catalog::BlockCatalog;
use rtdb_node::{Database, Node};
use rtdb_replication::Role as DriverRole;
use rtdb_types::{Anomaly, AnomalyCategory, Result, StorageEpoch, TransactionKey};
use tokio_util::sync::CancellationToken;

use crate::control::{ACTIVATION_RATE_LIMITED, CLAIM_CONTENDED, Handler, MoveTicket, NODE_UNDER_PRESSURE, NOT_CLAIMABLE, Request, Response};
use crate::deployment::{CellStorage, Role, database_config, ensemble_servers, journal_endpoint, select_logkeepers};
use crate::lease::MemberLease;
use crate::hosted_recovery::{ReplicaTrimPlan, replay_start};

enum SuffixCut<'a> {
    Tip(TransactionKey),
    HeldPublication(
        &'a rtdb_node::OwnedForkCut, &'a dyn crate::keys::KeyService,
        &'a BlockCatalog, Arc<rtdb_storage::IoRegion>,
    ),
}

impl SuffixCut<'_> {
    fn time(&self) -> rtdb_types::Timestamp {
        match self {
            Self::Tip(key) => key.system_time,
            Self::HeldPublication(cut, ..) => cut.basis(),
        }
    }

    fn matches(&self, key: TransactionKey) -> bool {
        match self {
            Self::Tip(expected) => *expected == key,
            Self::HeldPublication(cut, ..) => cut.matches_basis(key),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AgentOptions {
    pub audience_journal_port: Option<u16>,
    pub storage: CellStorage,
    pub world_archive: Option<CellStorage>,
    pub s3_write_role: Option<S3WriteRole>,
    pub replica_payload_reclamation: bool,
    /// How long an activation may take to reach a confirmed leadership before it is abandoned.
    pub activation_timeout: Duration,
    /// How many new deployment opens may start within one second.
    pub activations_per_second: NonZeroUsize,
    /// How often held deployments are checked against the directory.
    pub watch_interval: Duration,
    /// How long a holding's unflushed rows may sit without requests.
    pub clean_after_idle: Duration,
    /// Park a deployment nobody has touched for this long; never, if absent.
    pub park_after: Option<Duration>,
    pub resident_idle_after: Option<Duration>,
    pub min_residency: Duration,
    /// Drop a read replica nobody has touched for this long.
    pub unsubscribe_after: Duration,
    /// The most deployments this node keeps active; beyond it, the least recently used are parked.
    pub max_active: Option<usize>,
    /// Activation failures after which a deployment is quarantined to the isolation pool.
    pub quarantine_after: u32,
    /// The most unflushed rows this node's holdings may keep in their live indexes together; beyond it, the largest are flushed first (`BoundTailMemory`).
    pub max_tail_rows: Option<u64>,
    /// Whether this node is a compaction worker: it holds nothing, and compacts every held deployment that ranks it first among the cell's workers.
    pub compaction_worker: bool,
    /// How long a snapshot stays open; it must be shorter than the reaper's grace (`max_snapshot_lifetime`).
    pub snapshot_lifetime: Duration,
    /// When this node's sessions split a scan into fragments, and how it reads the fragments other nodes send it.
    pub fragments: crate::fragments::FragmentOptions,
}

#[derive(Clone, Debug)]
pub struct S3WriteRole { pub arn: String, pub sts_endpoint: Option<String>, pub require_conditional_put: bool }

impl AgentOptions {
    pub fn new(storage: CellStorage) -> Self {
        Self { audience_journal_port: None, storage, world_archive: None, s3_write_role: None, replica_payload_reclamation: false, activation_timeout: Duration::from_secs(30), activations_per_second: NonZeroUsize::new(200).unwrap(),
            watch_interval: Duration::from_millis(250), clean_after_idle: Duration::from_secs(60), park_after: None, resident_idle_after: None, min_residency: Duration::ZERO,
            unsubscribe_after: Duration::from_secs(60), max_active: None, quarantine_after: 2, compaction_worker: false, max_tail_rows: None, snapshot_lifetime: SNAPSHOT_LIFETIME,
            fragments: crate::fragments::FragmentOptions::default() }
    }
}

/// How long a snapshot stays open by default, which a reaper's grace must exceed (`max_snapshot_lifetime`).
pub const SNAPSHOT_LIFETIME: Duration = Duration::from_secs(30 * 60);

const CONFIRM_WITHIN: Duration = Duration::from_secs(2);
const PRESSURE_WAIT: Duration = Duration::from_millis(500);
const PRESSURE_PARKS_PER_PASS: usize = 8;
const IDLE_FLUSHES_PER_PASS: usize = 2;
const RESIDENT_IDLE_PER_PASS: usize = 8;
const HANDOFF_REPLAY_ROWS: u64 = 64;
const HANDOFF_REPLAY_RECORDS: i64 = 16;
const DRAIN_HANDOFFS_PER_PASS: usize = 8;
const DRAIN_CONCURRENT_HANDOFFS: usize = 1;
const ACTIVE_DRAIN_WINDOW: Duration = Duration::from_secs(60);

fn require_recovered_checkpoint(record: &DeploymentRecord, database: &Database) -> Result<()> {
    let Some(required) = record.checkpoint else { return Ok(()); };
    let block = database.status().adopted_block;
    let recovered = block.as_ref().is_some_and(|block| {
        let epoch = rtdb_types::StorageEpoch(record.log_epoch);
        let index = block.block_index().0;
        let boundary = block.boundary_replica_position().map_or(-1, |pos| pos.after_offset(epoch));
        let source = block.latest_source_position().map_or(-1, |pos| pos.after_offset(epoch));
        let system_time = block.latest_completed_tx().map_or(i64::MIN, |tx| tx.system_time.0);
        (index == required.block && boundary == required.boundary && source == required.source && system_time == required.system_time)
            || (index > required.block && boundary >= required.boundary && source >= required.source && system_time >= required.system_time)
    });
    if recovered {
        Ok(())
    } else {
        Err(Anomaly::new(
            AnomalyCategory::Unavailable, "rtdb.multi/checkpoint-not-recovered",
            format!("{} has not recovered the directory's durable checkpoint", record.name),
        ))
    }
}

#[derive(Clone, Copy)]
enum HostedReadCheck {
    Ordinary,
    AudienceRoute,
    AudienceCapture,
}

struct LocalHolding {
    id: String,
    epoch: u64,
    active_calls: usize,
    last_touch: Instant,
    opened: Instant,
    linearizable: bool,
    trimmed_through: Option<i64>,
    database: std::sync::Weak<Database>,
    claimed_version: Version,
    placement: tokio::sync::watch::Receiver<Option<DeploymentRecord>>,
}

struct HoldingCall {
    agent: Weak<NodeAgent>,
    name: String,
    attachment: Weak<Database>,
}

impl Drop for HoldingCall {
    fn drop(&mut self) {
        let Some(agent) = self.agent.upgrade() else { return };
        let mut holdings = agent.holdings.lock();
        if let Some(holding) = holdings.get_mut(&self.name).filter(|holding| holding.database.ptr_eq(&self.attachment)) {
            holding.active_calls -= 1;
            holding.last_touch = Instant::now();
        }
    }
}

type Turns = Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>;

#[derive(Default)]
struct ActivationWindow { starts: VecDeque<Instant> }

impl ActivationWindow {
    fn admit(&mut self, now: Instant, limit: NonZeroUsize) -> bool {
        while self.starts.front().is_some_and(|start| now.duration_since(*start) >= Duration::from_secs(1)) {
            self.starts.pop_front();
        }
        if self.starts.len() >= limit.get() { return false; }
        self.starts.push_back(now);
        true
    }
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct ResidentDeadline {
    at_ms: i64,
    name: String,
    deployment: String,
    epoch: u64,
}

struct DeploymentTurn<'a> {
    turns: &'a Turns,
    name: &'a str,
    gate: Option<Arc<tokio::sync::Mutex<()>>>,
}

impl<'a> DeploymentTurn<'a> {
    fn new(turns: &'a Turns, name: &'a str) -> Self {
        let mut entries = turns.lock();
        let gate = match entries.get(name) {
            Some(gate) => gate.clone(),
            None => entries.entry(name.to_owned()).or_default().clone(),
        };
        Self { turns, name, gate: Some(gate) }
    }

    async fn lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.gate.as_ref().expect("the turn still holds its gate").lock().await
    }

    fn try_lock(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        self.gate.as_ref().expect("the turn still holds its gate").try_lock().ok()
    }
}

impl Drop for DeploymentTurn<'_> {
    fn drop(&mut self) {
        let mut turns = self.turns.lock();
        // Drop this reference under the registry lock so simultaneous departures can see the last one.
        drop(self.gate.take());
        if turns.get(self.name).is_some_and(|gate| Arc::strong_count(gate) == 1) {
            turns.remove(self.name);
        }
    }
}
/// `fragments` keeps a replica open for the fragments other nodes send it, whatever the deployment's read width.
struct LocalSubscription { id: String, database: Weak<Database>, last_touch: Instant, opened: Instant, role: Role, fragments: bool }

#[derive(Clone, Copy, PartialEq, Eq)]
enum SubscriptionUse { Query, FreshQuery, Fragment }

pub struct NodeAgent {
    storage_namespaces: rtdb_directory::StorageNamespaces,
    directory: Arc<dyn Directory>,
    keys: Arc<dyn crate::keys::KeyService>,
    node: Node,
    lease: Arc<MemberLease>,
    address: String,
    flight_endpoints: FlightEndpoints,
    options: AgentOptions,
    holdings: Mutex<BTreeMap<String, LocalHolding>>,
    /// Deployments a fragment was asked of before this node had them open, whose replicas its next pass opens.
    warming: Mutex<BTreeSet<String>>,
    /// The cell's secret, which the requests that read a deployment's data or publish for it must carry a grant under.
    secret: Option<Arc<crate::capability::CellSecret>>,
    writers: crate::logs::Writers,
    subscriptions: Mutex<BTreeMap<String, LocalSubscription>>,
    /// One activation, subscription or park at a time per deployment name.
    turns: Turns,
    activations: Mutex<ActivationWindow>,
    resident_deadlines: Mutex<BTreeSet<ResidentDeadline>>,
    memory_relief: tokio::sync::Mutex<()>,
    /// How many fragments this node has read for other nodes' scans, by deployment, table and prefix.
    fragments_read: Mutex<BTreeMap<(String, String, String), u64>>,
}

pub(crate) struct FlightEndpoints {
    pub ordinary: Option<String>,
    pub audience: Option<String>,
}

impl FlightEndpoints {
    fn get(&self, audience: bool) -> &Option<String> {
        if audience { &self.audience } else { &self.ordinary }
    }
}

impl NodeAgent {
    pub(crate) fn new(directory: Arc<dyn Directory>, keys: Arc<dyn crate::keys::KeyService>, node: Node, lease: Arc<MemberLease>, address: String, flight_endpoints: FlightEndpoints, options: AgentOptions, storage_namespaces: rtdb_directory::StorageNamespaces, writers: crate::logs::Writers, secret: Option<Arc<crate::capability::CellSecret>>) -> Arc<Self> {
        Arc::new(Self { directory, keys, node, lease, address, flight_endpoints, options, storage_namespaces, secret, warming: Mutex::new(BTreeSet::new()), holdings: Mutex::new(BTreeMap::new()), writers, subscriptions: Mutex::new(BTreeMap::new()), turns: Mutex::new(HashMap::new()), activations: Default::default(), resident_deadlines: Mutex::default(), memory_relief: Default::default(), fragments_read: Mutex::new(BTreeMap::new()) })
    }

    pub fn member(&self) -> MemberId { self.lease.id() }
    pub fn held(&self) -> BTreeMap<String, u64> { self.holdings.lock().iter().map(|(name, holding)| (name.clone(), holding.epoch)).collect() }

    fn holding_loads(&self, measure_query_cpu: bool) -> Vec<crate::load::HoldingLoad> {
        self.holdings.lock().iter().filter_map(|(name, holding)| {
            let database = holding.database.upgrade()?;
            Some(crate::load::HoldingLoad {
                name: name.clone(),
                deployment: holding.id.clone(),
                epoch: holding.epoch,
                load: crate::load::LoadVector { tail_bytes: database.live_batch_bytes(), cache_bytes: database.cache_bytes(), ..Default::default() },
                submitted_bytes: Some(database.submitted_bytes()),
                query_cpu_nanos: if measure_query_cpu { database.observe_query_cpu() } else { None },
            })
        }).collect()
    }
    fn read_loads(&self) -> Vec<crate::read_width::ReadLoad> {
        use crate::read_width::{ReadLoad, ReadPlacement};
        let mut loads: Vec<_> = self.holdings.lock().values().filter_map(|holding| {
            let database = holding.database.upgrade()?;
            let placement = holding.placement.borrow();
            let record = placement.as_ref().filter(|record| record.id == holding.id && record.epoch == holding.epoch)?;
            Some(ReadLoad {
                deployment: holding.id.clone(),
                sessions: database.replica_read_sessions() as u64,
                placement: ReadPlacement::Writer { epoch: holding.epoch, width: record.read_width },
            })
        }).collect();
        let writer_ids: BTreeSet<_> = loads.iter().map(|load| load.deployment.clone()).collect();
        loads.extend(self.subscriptions.lock().values()
            .filter(|subscription| subscription.role == Role::Subscriber && !writer_ids.contains(&subscription.id))
            .filter_map(|subscription| {
                let database = subscription.database.upgrade()?;
                Some(ReadLoad {
                    deployment: subscription.id.clone(), sessions: database.replica_read_sessions() as u64,
                    placement: ReadPlacement::Replica,
                })
            }));
        loads
    }

    /// The deployments this node has open as snapshots.
    pub fn subscribed_snapshots(&self) -> Vec<String> { self.subscriptions.lock().iter().filter(|(_, subscription)| matches!(subscription.role, Role::Snapshot(_))).map(|(name, _)| name.clone()).collect() }
    pub fn subscribed(&self) -> Vec<String> { self.subscriptions.lock().keys().cloned().collect() }
    /// The fragments this node has read for other nodes, as (deployment, `schema.table`, prefix) with how often.
    pub fn fragments_read(&self) -> BTreeMap<(String, String, String), u64> { self.fragments_read.lock().clone() }

    fn turn<'a>(&'a self, name: &'a str) -> DeploymentTurn<'a> {
        DeploymentTurn::new(&self.turns, name)
    }

    async fn members(&self) -> Result<BTreeMap<MemberId, MemberRecord>> {
        Ok(self.directory.members().await?.into_iter().map(|member| (member.id.clone(), member)).collect())
    }

    async fn record(&self, name: &str) -> Result<DeploymentRecord> {
        self.directory.deployment_by_name(name).await?.ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-deployment", format!("No deployment is named {name}")))
    }

    pub async fn activate(&self, name: &str) -> Result<Response> { self.activate_holding(name, None).await }

    async fn activate_holding(&self, name: &str, moving: Option<&MoveTicket>) -> Result<Response> {
        let turn = self.turn(name);
        let me = self.member();
        let mut attempted_relief = false;
        let mut admitted = false;
        for _ in 0..4 {
            let active_turn = turn.lock().await;
            let record = self.record(name).await?;
            if moving.is_some_and(|ticket| ticket.deployment != record.id) {
                return Err(stale_handoff(name));
            }
            let locally_open = |holding: &LocalHolding| {
                holding.id == record.id && holding.epoch == record.epoch
                    && self.node.catalog().database(name).is_ok_and(|database| std::ptr::eq(holding.database.as_ptr(), Arc::as_ptr(&database)))
            };
            let reuse_local = || {
                let mut holdings = self.holdings.lock();
                let holding = holdings.get_mut(name).filter(|holding| locally_open(holding))?;
                holding.last_touch = Instant::now();
                holding.linearizable = record.linearizable_reads;
                Some(Response::Held { epoch: record.epoch, address: self.address.clone() })
            };
            let can_reuse = record.holder.as_ref() == Some(&me) && self.holdings.lock().get(name).is_some_and(locally_open);
            if can_reuse
                && self.directory.member(&me).await.ok().flatten().is_some_and(|member| record.may_hold_on(&member, now_ms()))
                && let Some(held) = reuse_local()
            {
                return Ok(held);
            }
            let members = self.members().await?;
            let now = now_ms();
            let holder = effective_holder(&record, &members, now);
            if holder == Some(&me) && let Some(held) = reuse_local() { return Ok(held); }
            if moving.is_some_and(|ticket| !record.active_move(now).is_some_and(|intent| intent.request == ticket.request && intent.target == me)) {
                return Err(stale_handoff(name));
            }
            if let Some(holder) = holder {
                if *holder != me {
                    let address = members.get(holder).map(|member| member.address.clone()).unwrap_or_default();
                    return Ok(Response::Redirect { holder: holder.to_string(), address });
                }
            }
            let Some(mut next) = claim(&record, &me, &members, now) else {
                return Err(Anomaly::new(AnomalyCategory::Conflict, NOT_CLAIMABLE, format!("{name} cannot be claimed by this member now")));
            };
            if record.storage_namespaces.is_some() {
                crate::deployment::require_storage_namespaces(&record, self.storage_namespaces)?;
            }
            if self.execution_memory_pressured() {
                if attempted_relief {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, NODE_UNDER_PRESSURE, "The node has no execution-memory headroom for another deployment"));
                }
                drop(active_turn);
                self.relieve_execution_memory(Some(name)).await;
                attempted_relief = true;
                continue;
            }
            let reserved_here = record.active_move(now).is_some_and(|intent| intent.target == me);
            if !admitted && !reserved_here {
                let mut activations = self.activations.lock();
                if !activations.admit(Instant::now(), self.options.activations_per_second) {
                    return Err(Anomaly::new(AnomalyCategory::Busy, ACTIVATION_RATE_LIMITED, "The node has reached its deployment activation rate"));
                }
                admitted = true;
            }
            next.storage_namespaces = Some(self.storage_namespaces);
            match self.directory.replace_deployment(next, record.version).await? {
                Cas::Applied(held) => {
                    self.cancel_resident_deadline(&record);
                    return self.open(held, &members).await;
                }
                Cas::Conflict(_) => continue,
            }
        }
        Err(Anomaly::new(AnomalyCategory::Conflict, CLAIM_CONTENDED, format!("{name} kept changing while being claimed")))
    }

    async fn open(&self, record: DeploymentRecord, members: &BTreeMap<MemberId, MemberRecord>) -> Result<Response> {
        self.stop_running(&record.name).await;
        let opened = async {
            let configuration = self.holder_config(&record, members)?;
            let database = self.node.database_or_attach(record.name.clone(), configuration).await?
                .ok_or_else(|| Anomaly::fault("rtdb.multi/not-opened", "The node declined to open the deployment"))?;
            self.await_leadership(&record.name, &database).await?;
            if record.linearizable_reads {
                require_recovered_checkpoint(&record, &database)?;
            }
            Ok::<_, Anomaly>(database)
        }.await;
        match opened {
            Ok(database) => {
                let placement = rtdb_directory::watch_deployment(Arc::clone(&self.directory), record.id.clone(), self.options.watch_interval);
                let opened = Instant::now();
                self.holdings.lock().insert(record.name.clone(), LocalHolding {
                    id: record.id.clone(), epoch: record.epoch, active_calls: 0, last_touch: opened, opened,
                    linearizable: record.linearizable_reads, trimmed_through: None, database: Arc::downgrade(&database),
                    claimed_version: record.version, placement,
                });
                Ok(Response::Held { epoch: record.epoch, address: self.address.clone() })
            }
            Err(error) => {
                self.stop_running(&record.name).await;
                let crashed = !matches!(error.category, AnomalyCategory::Unavailable | AnomalyCategory::Busy | AnomalyCategory::Interrupted | AnomalyCategory::Conflict);
                self.release_failed_holding(&record.id, record.epoch, crashed).await;
                Err(error)
            }
        }
    }

    /// The configuration a holder opens `record` with: on its ensemble while a quorum of it is live and every member is, otherwise moving the stream to an ensemble of live logkeepers that keeps the live members (`FlushOnDegradedEnsemble`).
    fn holder_config(&self, record: &DeploymentRecord, members: &BTreeMap<MemberId, MemberRecord>) -> Result<rtdb_format::database::proto::DatabaseConfig> {
        let now = now_ms();
        let live = |id: &MemberId| members.get(id).is_some_and(|member| member.is_live(now));
        let servers = ensemble_servers(record, members)?;
        let alive: Vec<MemberId> = record.ensemble.iter().filter(|id| live(id)).cloned().collect();
        if alive.len() == record.ensemble.len() || alive.len() < crate::deployment::write_quorum(record.ensemble.len()) {
            return Ok(database_config(record, servers, &self.options.storage, Role::Holder));
        }
        let candidates: Vec<&MemberRecord> = members.values().filter(|member| member.role == rtdb_directory::MemberRole::Logkeeper && member.is_live(now) && !record.ensemble.contains(&member.id)).collect();
        let live_members = alive.iter().map(|id| &members[id]).collect();
        let selected = select_logkeepers(&record.storage_identity(), record.ensemble.len(), live_members, &candidates);
        if selected.len() < record.ensemble.len() { return Ok(database_config(record, servers, &self.options.storage, Role::Holder)); }
        let next = selected.into_iter().map(journal_endpoint).collect();
        Ok(crate::deployment::moving_config(record, next, Some(servers), &self.options.storage, Role::Holder))
    }

    /// Before a query on a deployment held with linearizable reads, confirms with a quorum of its logkeepers that no later holder has sealed its stream (`ServeLinearizableReadOnWriter`).
    pub async fn confirm_read(&self, name: &str) -> Result<()> {
        let Some(id) = self.holdings.lock().get(name).filter(|holding| holding.linearizable).map(|holding| holding.id.clone()) else { return Ok(()) };
        self.confirm_holder(name, &id, None).await
    }

    pub async fn confirm_hosted_read(&self, database: &Database) -> Result<String> {
        self.confirm_placed_read(database, HostedReadCheck::Ordinary).await
    }

    pub(crate) async fn confirm_audience_read(&self, database: &Database, require_quorum: bool) -> Result<String> {
        let check = if require_quorum { HostedReadCheck::AudienceCapture } else { HostedReadCheck::AudienceRoute };
        self.confirm_placed_read(database, check).await
    }

    async fn confirm_placed_read(&self, database: &Database, check: HostedReadCheck) -> Result<String> {
        let name = database.name();
        let unavailable = || Anomaly::new(
            AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, format!("This node does not hold a linearizable database for {name}"),
        );
        let (id, epoch) = self.holdings.lock().get(name)
            .filter(|holding| holding.linearizable && std::ptr::eq(holding.database.as_ptr(), database))
            .map(|holding| (holding.id.clone(), holding.epoch))
            .ok_or_else(unavailable)?;
        let member_id = self.member();
        let record = match check {
            HostedReadCheck::Ordinary => self.directory.deployment(&id).await?,
            HostedReadCheck::AudienceRoute | HostedReadCheck::AudienceCapture => {
                self.directory.audience_authority(&id, &member_id).await?.and_then(|(record, member)| {
                    (member.id == member_id && record.may_hold_on(&member, now_ms())).then_some(record)
                })
            }
        }.filter(|record| record.id == id && record.name == name && record.linearizable_reads && record.epoch == epoch
            && record.holder.as_ref() == Some(&member_id)
            && matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined))
            .ok_or_else(unavailable)?;
        if !matches!(check, HostedReadCheck::Ordinary) {
            let scope = rtdb_types::PlacedAttemptScope::new(&record.stream("replica"), record.epoch)?;
            if database.placed_writer_scope() != Some(scope) {
                return Err(unavailable());
            }
        }
        require_recovered_checkpoint(&record, database)?;
        match check {
            HostedReadCheck::Ordinary => self.confirm_holder(name, &id, None).await?,
            HostedReadCheck::AudienceRoute => {}
            HostedReadCheck::AudienceCapture => {
                let port = self.options.audience_journal_port.ok_or_else(unavailable)?;
                self.confirm_holder(name, &id, Some(port)).await?;
            }
        }
        Ok(id)
    }

    async fn confirm_holder(&self, name: &str, id: &str, authority_port: Option<u16>) -> Result<()> {
        let writer = self.writers.lock().get(id).and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, format!("This node no longer holds {name}")))?;
        let confirmation = async {
            match authority_port {
                Some(port) => writer.confirm_authority(port).await,
                None => writer.confirm_leadership().await,
            }
        };
        match tokio::time::timeout(CONFIRM_WITHIN, confirmation).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(Anomaly::new(AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, format!("This node no longer holds {name}"))),
            Err(_) => Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/leadership-unconfirmed", format!("A quorum of {name}'s logkeepers did not confirm its holder in time"))),
        }
    }

    async fn await_leadership(&self, name: &str, database: &Arc<Database>) -> Result<()> {
        let activation_deadline = tokio::time::sleep(self.options.activation_timeout);
        tokio::pin!(activation_deadline);
        let mut changes = database.watch_status();
        let stopped = || database.progress().fault().unwrap_or_else(|| {
            Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/open-failed", "The deployment's driver stopped while it was being opened")
        });
        loop {
            let status = changes.borrow_and_update().clone();
            if matches!(status.role, DriverRole::Leading(_)) { return Ok(()); }
            if let Some(failure) = status.promotion_failure { return Err(failure); }
            if matches!(status.role, DriverRole::Stopped) { return Err(stopped()); }
            if self.node.catalog().database(name).is_err() { return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/open-failed", "The deployment stopped while it was being opened")); }
            tokio::select! {
                _ = &mut activation_deadline => return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/activation-timeout", format!(
                    "The holder did not confirm its leadership in time (role {:?}, highest term {:?}, replica position {:?}, open block {:?})",
                    status.role, status.highest_term, status.replica_position, status.open_block))),
                changed = changes.changed() => if changed.is_err() { return Err(stopped()); },
            }
        }
    }

    /// Counts an activation that failed or a holding whose database stopped, releasing the placement if this member still has it, and quarantines the deployment once failures reach the threshold.
    async fn release_failed_holding(&self, id: &str, epoch: u64, crashed: bool) {
        for _ in 0..4 {
            let Ok(Some(record)) = self.directory.deployment(id).await else { return };
            let Some(mut next) = park(&record, &self.member(), epoch) else { return };
            if crashed {
                next.activation_crashes += 1;
                if next.activation_crashes >= self.options.quarantine_after && next.lifecycle == Lifecycle::Live {
                    next.lifecycle = Lifecycle::Quarantined;
                }
            }
            if let Ok(Cas::Applied(_)) = self.directory.replace_deployment(next, record.version).await { return; }
        }
    }

    /// Stops running `name` on this node, waiting for its resources to drain so the name can be opened again.
    async fn stop_running(&self, name: &str) {
        self.stop_serving(name).await;
        self.wait_until_detached(name).await;
    }

    async fn wait_until_detached(&self, name: &str) {
        while self.node.catalog().is_draining(name) { tokio::time::sleep(Duration::from_millis(5)).await; }
    }

    async fn stop_serving(&self, name: &str) {
        self.holdings.lock().remove(name);
        self.subscriptions.lock().remove(name);
        if self.node.catalog().database(name).is_ok() { let _ = self.node.detach_local(name.to_owned()).await; }
    }

    /// Flushes through the source position observed when this request starts.
    async fn flush_and_adopt(&self, database: &Arc<Database>, id: &str, epoch: u64, wait: Duration) -> Result<Arc<BlockCatalog>> {
        let required = database.latest_submitted_position()?;
        let member = self.member();
        let mut watched = rtdb_directory::watch_deployment(self.directory.clone(), id.to_owned(), Duration::from_millis(100));
        let deposed = watched.wait_for(|snapshot| snapshot.as_ref().is_some_and(|record| {
            record.epoch > epoch || record.epoch == epoch && record.holder.as_ref() != Some(&member)
        }));
        let flush = async {
            let mut changes = database.watch_status();
            loop {
                let open = changes.borrow().open_block;
                database.flush().await?;
                let adopted = |status: &rtdb_replication::DriverStatus| {
                    status.adopted_block.as_ref().is_some_and(|block| block.block_index() >= open)
                };
                if let Ok(status) = changes.wait_for(|status| adopted(status) || matches!(status.role, DriverRole::Stopped)).await {
                    if let Some(block) = status.adopted_block.as_ref().filter(|_| adopted(&status))
                        && (required.0 < 0 || block.latest_source_position().is_some_and(|position| position >= required))
                    {
                        return Ok(Arc::clone(block));
                    }
                    if !matches!(status.role, DriverRole::Stopped) { continue; }
                }
                return Err(database.progress().fault().unwrap_or_else(|| Anomaly::new(
                    AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, "The holder stopped before adopting its flush",
                )));
            }
        };
        tokio::select! {
            _ = deposed => Err(Anomaly::new(AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, "The holder was deposed while waiting for its flush")),
            flushed = tokio::time::timeout(wait, flush) => flushed.unwrap_or_else(|_| {
                Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/flush-timeout", "The flush did not complete in time"))
            }),
        }
    }

    pub async fn flush(&self, name: &str) -> Result<Response> {
        let turn = self.turn(name);
        let _turn = turn.lock().await;
        let (id, epoch) = self.holdings.lock().get(name).map(|holding| (holding.id.clone(), holding.epoch))
            .ok_or_else(|| Anomaly::incorrect("rtdb.multi/not-held", format!("This node does not hold {name}")))?;
        let database = self.node.catalog().database(name)?;
        let deadline = tokio::time::Instant::now() + self.options.activation_timeout;
        let block = self.flush_and_adopt(&database, &id, epoch, self.options.activation_timeout).await?;
        let missing = || Anomaly::fault("rtdb.multi/flush-checkpoint", format!("{name} did not adopt a block in its current log epoch"));
        let (published, checkpoint) = tokio::time::timeout_at(deadline, self.publish_checkpoint(&id, epoch, &database, &block)).await
            .map_err(|_| checkpoint_timeout(name))??.ok_or_else(missing)?;
        if published.checkpoint != Some(checkpoint) || checkpoint.root_sha256.is_none() {
            return Err(Anomaly::new(
                AnomalyCategory::Unavailable, "rtdb.multi/capture-flush",
                "The adopted checkpoint is no longer the exact published cut",
            ));
        }
        Ok(Response::Flushed { checkpoint })
    }

    async fn pin_checkpoint_suffix(
        &self, name: &str, deployment: uuid::Uuid, lease: uuid::Uuid,
        capture: uuid::Uuid, basis: TransactionKey, expires_at_ms: i64,
    ) -> Result<Response> {
        self.pin_suffix_at(name, deployment, lease, capture, SuffixCut::Tip(basis), expires_at_ms)
            .await.map(|(_, pin)| Response::CheckpointSuffixPinned { pin })
    }

    pub(crate) async fn pin_held_fork_suffix(
        &self, name: &str, deployment: uuid::Uuid,
        capture: uuid::Uuid, cut: &rtdb_node::OwnedForkCut, keys: &dyn crate::keys::KeyService,
        base: &BlockCatalog, io: Arc<rtdb_storage::IoRegion>, expires_at_ms: i64,
    ) -> Result<(uuid::Uuid, CheckpointSuffixPin)> {
        self.pin_suffix_at(
            name, deployment, uuid::Uuid::new_v4(), capture,
            SuffixCut::HeldPublication(cut, keys, base, io), expires_at_ms,
        ).await
    }

    async fn pin_suffix_at(
        &self, name: &str, deployment: uuid::Uuid, lease: uuid::Uuid,
        capture: uuid::Uuid, cutoff: SuffixCut<'_>, expires_at_ms: i64,
    ) -> Result<(uuid::Uuid, CheckpointSuffixPin)> {
        const MAX_PIN_MS: i64 = 60_000;
        const MAX_PINS: usize = 32;
        let custody = match &cutoff {
            SuffixCut::Tip(_) => CheckpointSuffixCustody::Capture,
            SuffixCut::HeldPublication(..) => CheckpointSuffixCustody::World,
        };
        let unavailable = || Anomaly::new(
            AnomalyCategory::Unavailable, "rtdb.multi/checkpoint-suffix-unavailable",
            "The exact checkpoint suffix is no longer admissible on this writer",
        );
        let now = now_ms();
        if lease.is_nil() || capture.is_nil() || expires_at_ms <= now || expires_at_ms > now.saturating_add(MAX_PIN_MS) {
            return Err(unavailable());
        }
        let (id, epoch) = self.holdings.lock().get(name).map(|holding| (holding.id.clone(), holding.epoch))
            .ok_or_else(unavailable)?;
        if id != deployment.to_string() { return Err(unavailable()); }
        let member = self.member();
        let validate = |record: &DeploymentRecord| -> Result<(Checkpoint, String)> {
            if record.name != name || record.epoch != epoch || record.holder.as_ref() != Some(&member)
                || record.lifecycle != Lifecycle::Live || record.move_from.is_some()
                || record.checkpoint_recovery.is_some() || expires_at_ms <= now_ms()
            {
                return Err(unavailable());
            }
            if let SuffixCut::HeldPublication(cut, ..) = &cutoff {
                let expected = rtdb_types::PlacedAttemptScope::new(&record.stream("replica"), record.epoch)?;
                if (!cut.is_publication() && cut.overlay().is_none()) || cut.scope() != expected
                    || expires_at_ms > cut.until().0.div_euclid(1000)
                {
                    return Err(unavailable());
                }
            }
            crate::archive::require_capture_open(record, capture)?;
            let archived = record.checkpoint_captures.get(&capture).filter(|archived|
                archived.media_bound && archived.incarnation == record.incarnation
                && archived.log_epoch == record.log_epoch && archived.checkpoint.root_sha256.is_some()
            ).ok_or_else(unavailable)?;
            let checkpoint = archived.checkpoint;
            if checkpoint.boundary < 0 || cutoff.time().0 <= checkpoint.system_time
                || record.replica_anchor.as_ref().is_some_and(|anchor| anchor.root_incarnation != record.incarnation)
                || replay_start(record, checkpoint.boundary).is_err()
            {
                return Err(unavailable());
            }
            Ok((checkpoint, archived.manifest_sha256.clone().ok_or_else(unavailable)?))
        };
        let record = self.directory.deployment(&id).await?.ok_or_else(unavailable)?;
        let (checkpoint, manifest_sha256) = validate(&record)?;
        let reusable_world = |record: &DeploymentRecord| {
            record.checkpoint_suffix_pins.iter().find_map(|(id, prior)| {
                (prior.custody == CheckpointSuffixCustody::World && prior.capture == capture
                    && prior.manifest_sha256 == manifest_sha256 && prior.checkpoint == checkpoint
                    && prior.incarnation == record.incarnation && prior.log_epoch == record.log_epoch
                    && prior.writer_epoch == epoch && cutoff.matches(prior.basis)
                    && prior.expires_at_ms > now_ms() && prior.expires_at_ms <= expires_at_ms)
                    .then_some(*id)
            })
        };
        if custody == CheckpointSuffixCustody::World {
            if let Some(existing) = reusable_world(&record) {
                return Ok((existing, record.checkpoint_suffix_pins[&existing].clone()));
            }
        } else if let Some(prior) = record.checkpoint_suffix_pins.get(&lease) {
            if prior.capture == capture && prior.manifest_sha256 == manifest_sha256
                && prior.checkpoint == checkpoint && cutoff.matches(prior.basis)
                && prior.custody == custody && prior.expires_at_ms == expires_at_ms
                && prior.incarnation == record.incarnation && prior.log_epoch == record.log_epoch && prior.writer_epoch == epoch
            {
                return Ok((lease, prior.clone()));
            }
            return Err(unavailable());
        }
        let database = self.node.catalog().database(name)?;
        let before = database.status();
        let applied = database.progress().current();
        let after = database.status();
        let installed = after.installed.filter(|installed|
            matches!(after.role, DriverRole::Leading(_)) && before.role == after.role
            && before.installed == Some(*installed) && installed.source == applied.source_position
            && applied.tx_key.is_some_and(|head| {
                match &cutoff {
                    SuffixCut::Tip(basis) => head == *basis,
                    SuffixCut::HeldPublication(..) => head.system_time >= cutoff.time(),
                }
            })
        ).ok_or_else(unavailable)?;
        let log_epoch = StorageEpoch(record.log_epoch);
        let replica_through = installed.replica.after_offset(log_epoch);
        let source_through = installed.source.after_offset(log_epoch);
        if replica_through <= checkpoint.boundary || source_through <= checkpoint.source {
            return Err(unavailable());
        }

        let members = self.members().await?;
        let servers = ensemble_servers(&record, &members)?;
        let stream = rtdb_logkeeper::StreamConfig {
            write_quorum: crate::deployment::write_quorum(servers.len()), servers,
            stream: record.stream("replica"), maximum_message_bytes: 4 << 20,
        };
        let first_replica = checkpoint.boundary.checked_add(1).ok_or_else(unavailable)?;
        let remaining = expires_at_ms.saturating_sub(now_ms()).max(0) as u64;
        let certify = async {
            let proof = rtdb_logkeeper::certify_committed_range(&stream, first_replica, replica_through).await?;
            if let SuffixCut::Tip(basis) = &cutoff {
                return Ok::<_, Anomaly>((*basis, replica_through, source_through, proof));
            }
            // Discovery streams the quorum-certified applied interval. Its
            // target is the original held publication, not the driver tip.
            let _charge = self.node.memory().reserve(32 << 20)?;
            let SuffixCut::HeldPublication(cut, keys, base, io) = &cutoff else { return Err(unavailable()); };
            let (basis, replica, source) = crate::archive::find_held_publication(
                &stream, &proof, &record, &self.options.storage, *keys, cut,
                base, Arc::clone(io), self.node.memory(),
            ).await?;
            if source <= checkpoint.source || source > source_through || replica > replica_through {
                return Err(unavailable());
            }
            let prefix = rtdb_logkeeper::certify_committed_range(&stream, first_replica, replica).await?;
            Ok((basis, replica, source, prefix))
        };
        let (basis, replica_through, source_through, proof) = tokio::time::timeout(
            Duration::from_millis(remaining), certify,
        ).await.map_err(|_| unavailable())??;
        if proof.interval != [first_replica, replica_through] { return Err(unavailable()); }
        let pin = CheckpointSuffixPin {
            capture, custody, manifest_sha256: manifest_sha256.clone(), checkpoint, incarnation: record.incarnation,
            log_epoch: record.log_epoch, writer_epoch: epoch, basis, replica_through,
            source_through, journal_interval: proof.interval, journal_sha256: proof.digest_bytes().ok_or_else(unavailable)?,
            expires_at_ms, archive_sha256: None,
        };
        let installed = crate::admin::update_installation(self.directory.as_ref(), &id, |current| {
            let (selected, manifest) = validate(current)?;
            if selected != pin.checkpoint || manifest != pin.manifest_sha256
                || current.incarnation != record.incarnation || current.log_epoch != record.log_epoch
                || current.ensemble != record.ensemble
            {
                return Err(unavailable());
            }
            if custody == CheckpointSuffixCustody::World && reusable_world(current).is_some() {
                return Ok(());
            }
            if let Some(prior) = current.checkpoint_suffix_pins.get(&lease) {
                return if prior == &pin { Ok(()) } else { Err(unavailable()) };
            }
            let now = now_ms();
            if expires_at_ms <= now { return Err(unavailable()); }
            current.checkpoint_suffix_pins.retain(|_, hold| {
                hold.custody != CheckpointSuffixCustody::Capture
                    || hold.expires_at_ms > now || hold.archive_sha256.is_some()
            });
            let world = custody == CheckpointSuffixCustody::World;
            let count = current.checkpoint_suffix_pins.values().filter(|hold| {
                (hold.custody != CheckpointSuffixCustody::Capture) == world
            }).count();
            let limit = if world { MAX_RETAINED_WORLDS + MAX_PINS } else { MAX_PINS };
            if count >= limit {
                return Err(unavailable());
            }
            current.checkpoint_suffix_pins.insert(lease, pin.clone());
            Ok(())
        }).await?;
        let id = if custody == CheckpointSuffixCustody::World {
            reusable_world(&installed).ok_or_else(unavailable)?
        } else {
            lease
        };
        let pin = installed.checkpoint_suffix_pins.get(&id).cloned().ok_or_else(unavailable)?;
        Ok((id, pin))
    }

    pub async fn park(&self, name: &str) -> Result<Response> { self.park_holding(name, None).await }

    async fn park_holding(&self, name: &str, moving: Option<(u64, &MoveTicket)>) -> Result<Response> {
        let turn = self.turn(name);
        let _turn = turn.lock().await;
        self.park_with_turn(name, moving, self.options.activation_timeout).await
    }

    async fn park_with_turn(&self, name: &str, moving: Option<(u64, &MoveTicket)>, flush_wait: Duration) -> Result<Response> {
        let local = self.holdings.lock().get(name).map(|holding| (holding.id.clone(), holding.epoch));
        let Some((id, epoch)) = local else { return self.park_resident(name, moving, None).await; };
        let is_requested = |record: &DeploymentRecord| moving.is_none_or(|(epoch, ticket)| {
            record.id == ticket.deployment && record.epoch == epoch && record.holder.as_ref() == Some(&self.member())
                && record.active_move(now_ms()).is_some_and(|intent| intent.request == ticket.request)
        });
        let log_epoch = if let Some((move_epoch, ticket)) = moving {
            if id != ticket.deployment || epoch != move_epoch {
                return Err(stale_handoff(name));
            }
            let record = self.directory.deployment(&id).await?.filter(is_requested).ok_or_else(|| stale_handoff(name))?;
            Some(StorageEpoch(record.log_epoch))
        } else { None };

        let database = self.node.catalog().database(name).ok();
        let deadline = tokio::time::Instant::now() + flush_wait;
        let remaining = || deadline.saturating_duration_since(tokio::time::Instant::now());
        let write_pause = if let Some(database) = database.as_ref() {
            if log_epoch.is_some_and(|epoch| !short_handoff_tail(database, epoch)) {
                self.flush_and_adopt(database, &id, epoch, remaining()).await?;
            }
            let pause = tokio::time::timeout_at(deadline, database.pause_transactions()).await.map_err(|_| {
                let code = if moving.is_some() { "rtdb.multi/handoff-drain-timeout" } else { "rtdb.multi/park-drain-timeout" };
                Anomaly::new(AnomalyCategory::Unavailable, code, "The holder could not drain its admitted writes in time")
            })??;
            if log_epoch.is_none_or(|epoch| !short_handoff_tail(database, epoch)) {
                self.flush_and_adopt(database, &id, epoch, remaining()).await?;
            }
            Some(pause)
        } else { None };
        if let Some(database) = &database
            && let Some(block) = database.status().adopted_block
        {
            tokio::time::timeout_at(deadline, self.publish_checkpoint(&id, epoch, database, &block)).await
                .map_err(|_| checkpoint_timeout(name))??;
        }
        for _ in 0..8 {
            let Some(record) = self.directory.deployment(&id).await? else { break };
            if !is_requested(&record) { return Err(stale_handoff(name)); }
            let Some(parked) = park(&record, &self.member(), epoch) else { break };
            if let Cas::Applied(_) = self.directory.replace_deployment(parked, record.version).await? {
                self.stop_serving(name).await;
                drop(write_pause);
                if moving.is_none() { self.wait_until_detached(name).await; }
                return Ok(Response::Done);
            }
        }
        if self.directory.deployment(&id).await?.as_ref().is_some_and(|record| record.epoch == epoch && record.holder.as_ref() == Some(&self.member())) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/update-contended", format!("{name} kept changing while it was parked")));
        }
        self.stop_running(name).await;
        Ok(Response::Done)
    }

    fn resident_deadline(&self, record: &DeploymentRecord) -> Option<ResidentDeadline> {
        let period = self.options.park_after?;
        let idle = record.resident_idle.filter(|_| record.is_resident_idle())?;
        let at_ms = idle.last_activity_at_ms.saturating_add(period.as_millis().min(i64::MAX as u128) as i64);
        Some(ResidentDeadline { at_ms, name: record.name.clone(), deployment: record.id.clone(), epoch: idle.epoch })
    }

    fn cancel_resident_deadline(&self, record: &DeploymentRecord) {
        if let Some(deadline) = self.resident_deadline(record) {
            self.resident_deadlines.lock().remove(&deadline);
        }
    }

    async fn park_due_residents(&self) {
        let mut due = Vec::new();
        {
            let now = now_ms();
            let mut deadlines = self.resident_deadlines.lock();
            while due.len() < RESIDENT_IDLE_PER_PASS && deadlines.first().is_some_and(|deadline| deadline.at_ms <= now) {
                due.push(deadlines.pop_first().expect("a due deadline exists"));
            }
        }
        for deadline in due {
            let turn = self.turn(&deadline.name);
            let _turn = turn.lock().await;
            if self.park_resident(&deadline.name, None, Some((&deadline.deployment, deadline.epoch))).await.is_err() {
                self.resident_deadlines.lock().insert(deadline.clone());
            }
        }
    }

    pub(crate) async fn park_resident_holdings(&self) {
        if self.options.resident_idle_after.is_none() { return; }
        if let Ok(records) = self.directory.deployments_held_by(&self.member()).await {
            for record in records.into_iter().filter(DeploymentRecord::is_resident_idle) {
                let _ = self.park_if_resident(&record).await;
            }
        }
    }

    async fn park_if_resident(&self, expected: &DeploymentRecord) -> Result<Response> {
        let turn = self.turn(&expected.name);
        let _turn = turn.lock().await;
        self.park_resident(&expected.name, None, Some((&expected.id, expected.epoch))).await
    }

    async fn park_resident(&self, name: &str, moving: Option<(u64, &MoveTicket)>, expected: Option<(&str, u64)>) -> Result<Response> {
        let member = self.member();
        for _ in 0..8 {
            let Some(record) = self.directory.deployment_by_name(name).await? else { return Ok(Response::Done) };
            if !record.is_resident_idle() || record.holder.as_ref() != Some(&member) { return Ok(Response::Done); }
            if expected.is_some_and(|(id, epoch)| record.id != id || record.epoch != epoch) { return Ok(Response::Done); }
            if moving.is_some_and(|(epoch, ticket)| record.id != ticket.deployment || record.epoch != epoch
                || !record.active_move(now_ms()).is_some_and(|intent| intent.request == ticket.request))
            {
                return Err(stale_handoff(name));
            }
            let parked = park(&record, &member, record.epoch).expect("the resident holder matches");
            if let Cas::Applied(_) = self.directory.replace_deployment(parked, record.version).await? {
                self.cancel_resident_deadline(&record);
                return Ok(Response::Done);
            }
        }
        Err(Anomaly::new(AnomalyCategory::Conflict, CLAIM_CONTENDED, format!("{name} kept changing while it was parked")))
    }

    async fn suspend_if_idle(&self, name: &str, expected: &DeploymentRecord, last_touch: Instant) -> Result<()> {
        let turn = self.turn(name);
        let _turn = turn.lock().await;
        let unchanged = || self.holdings.lock().get(name).is_some_and(|holding| {
            holding.id == expected.id && holding.epoch == expected.epoch && holding.last_touch == last_touch && holding.active_calls == 0
        });
        if !unchanged() { return Ok(()); }
        let member = self.member();
        let database = self.node.catalog().database(name)?;
        let deadline = tokio::time::Instant::now() + self.options.activation_timeout;
        let pause = tokio::time::timeout_at(deadline, database.pause_transactions()).await
            .map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/suspend-drain-timeout", "The holder could not drain its admitted writes in time"))??;
        let block = self.flush_and_adopt(&database, &expected.id, expected.epoch,
            deadline.saturating_duration_since(tokio::time::Instant::now())).await?;
        tokio::time::timeout_at(deadline, self.publish_checkpoint(&expected.id, expected.epoch, &database, &block)).await
            .map_err(|_| checkpoint_timeout(name))??;
        for _ in 0..8 {
            if !unchanged() { return Ok(()); }
            let Some(record) = self.directory.deployment(&expected.id).await? else { return Ok(()) };
            let now = now_ms();
            let last_activity = now.saturating_sub(last_touch.elapsed().as_millis().min(i64::MAX as u128) as i64);
            let Some(suspended) = suspend_holding(&record, &member, expected.epoch, now, last_activity) else { return Ok(()) };
            if !unchanged() { return Ok(()); }
            if let Cas::Applied(suspended) = self.directory.replace_deployment(suspended, record.version).await? {
                if let Some(deadline) = self.resident_deadline(&suspended) {
                    self.resident_deadlines.lock().insert(deadline);
                }
                self.stop_serving(name).await;
                drop(pause);
                self.wait_until_detached(name).await;
                return Ok(());
            }
        }
        Err(Anomaly::new(AnomalyCategory::Conflict, CLAIM_CONTENDED, format!("{name} kept changing while it became idle")))
    }

    fn execution_memory_pressured(&self) -> bool {
        let budget = self.node.memory();
        budget.used_bytes() >= budget.limit_bytes() - budget.limit_bytes() / 5
    }

    async fn relieve_execution_memory(&self, protected: Option<&str>) {
        if !self.execution_memory_pressured() { return; }
        let member = self.member();
        let Ok(Some(local_member)) = self.directory.member(&member).await else { return };
        let draining = local_member.state == MemberState::Draining;
        if draining && local_member.drain_target_engine.is_some() { return; }
        let Ok(_relief) = tokio::time::timeout(PRESSURE_WAIT, self.memory_relief.lock()).await else { return };
        if !self.execution_memory_pressured() { return; }
        let budget = self.node.memory();
        let target = budget.limit_bytes() - budget.limit_bytes() / 4;
        let mut candidates: Vec<_> = self.holdings.lock().iter()
            .filter(|(name, holding)| Some(name.as_str()) != protected && holding.opened.elapsed() >= self.options.min_residency)
            .map(|(name, holding)| (holding.last_touch, name.clone(), holding.id.clone(), holding.epoch))
            .collect();
        candidates.sort_unstable();
        let mut attempted = 0;
        for (touched, name, id, epoch) in candidates {
            if budget.used_bytes() <= target || attempted == PRESSURE_PARKS_PER_PASS { break; }
            let turn = self.turn(&name);
            let Some(_turn) = turn.try_lock() else { continue };
            let same_activity = || self.holdings.lock().get(&name).is_some_and(|holding| {
                holding.id == id && holding.epoch == epoch && holding.last_touch == touched
            });
            if !same_activity() { continue; }
            attempted += 1;
            match self.directory.deployment(&id).await {
                Ok(Some(record)) if record.epoch == epoch && record.holder.as_ref() == Some(&member) => {
                    if draining && record.engine_pin.is_some() { continue; }
                    if same_activity() { let _ = self.park_with_turn(&name, None, PRESSURE_WAIT).await; }
                }
                Ok(_) => self.stop_running(&name).await,
                Err(_) => continue,
            }
        }
    }

    pub async fn subscribe(&self, name: &str) -> Result<Response> { self.subscribe_for(name, SubscriptionUse::Query).await }

    async fn subscribe_for(&self, name: &str, usage: SubscriptionUse) -> Result<Response> {
        let turn = self.turn(name);
        let _turn = turn.lock().await;
        let record = self.record(name).await?;
        if record.lifecycle != Lifecycle::Live { return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/not-live", format!("{name} is not live"))); }
        if usage == SubscriptionUse::FreshQuery && !record.linearizable_reads {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/read-policy-changed", format!("The read policy of {name} changed")));
        }
        if record.move_from.is_some() {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/log-move-pending", format!("{name}'s holder has yet to adopt its log")));
        }
        if let Some(holding) = self.holdings.lock().get_mut(name).filter(|holding| holding.id == record.id && holding.epoch == record.epoch) {
            holding.linearizable = record.linearizable_reads;
            return Ok(Response::Subscribed { address: self.address.clone() });
        }
        let fragments = usage == SubscriptionUse::Fragment;
        let members = self.members().await?;
        if !fragments && !crate::deployment::read_replicas(&record, &members, now_ms()).iter().any(|member| member.id == self.member()) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/read-width", format!("This node is outside the read width of {name}")));
        }
        let configuration = database_config(&record, ensemble_servers(&record, &members)?, &self.options.storage, Role::Subscriber);
        if let Some(subscription) = self.subscriptions.lock().get_mut(name) {
            if subscription.id == record.id && !matches!(subscription.role, Role::Snapshot(_))
                && self.node.catalog().database(name).is_ok_and(|database| {
                    std::ptr::eq(subscription.database.as_ptr(), Arc::as_ptr(&database)) && database.configuration().log == configuration.log
                })
            {
                subscription.last_touch = Instant::now();
                subscription.fragments |= fragments;
                return Ok(Response::Subscribed { address: self.address.clone() });
            }
        }
        self.stop_running(name).await;
        let database = self.node.database_or_attach(name.to_owned(), configuration).await?
            .ok_or_else(|| Anomaly::fault("rtdb.multi/not-opened", "The node declined to open the replica"))?;
        self.subscriptions.lock().insert(name.to_owned(), LocalSubscription { id: record.id, database: Arc::downgrade(&database), last_touch: Instant::now(), opened: Instant::now(), role: Role::Subscriber, fragments });
        Ok(Response::Subscribed { address: self.address.clone() })
    }

    fn check_node_grant(&self, grant: &str, name: &str) -> Result<()> {
        let secret = self.secret.as_ref().ok_or_else(|| Anomaly::new(AnomalyCategory::Forbidden, "rtdb.multi/no-cell-secret", "This node has no cell secret, so it serves no other node's request for data"))?;
        secret.verify_node_grant(grant, name)
    }

    /// Reads a fragment of another node's scan of `name`, from the holding or replica this node has of it, opening a replica if it has neither.
    pub async fn scan_fragment(&self, name: &str, fragment: &rtdb_query::ScanFragment) -> Result<Response> {
        use base64::Engine;
        if fragment.database() != name { return Err(Anomaly::incorrect("rtdb.multi/fragment-database", format!("A fragment of {} was sent as one of {name}", fragment.database()))); }
        // A reader without the deployment open refuses at once, so its coordinator reads the slice itself rather than wait on an open; it opens a replica on its next pass, for the scans after.
        let open = self.node.catalog().database(name).is_ok() && (self.holdings.lock().contains_key(name) || self.subscriptions.lock().get(name).is_some_and(|subscription| !matches!(subscription.role, Role::Snapshot(_))));
        if !open {
            self.warming.lock().insert(name.to_owned());
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/fragment-reader-cold", format!("This node is opening a replica of {name}")));
        }
        self.touch(name);
        let response = self.node.scan_fragment_retained(fragment, self.options.fragments.catch_up, &CancellationToken::new()).await?;
        let bytes = response.as_slice();
        // A slice too large to answer in one response is read by its coordinator instead.
        if bytes.len() as u64 > crate::control::MAXIMUM_RESPONSE_BYTES / 4 * 3 - 1024 {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/fragment-too-large", format!("A fragment of {name} is larger than one response carries")));
        }
        let table = fragment.table();
        *self.fragments_read.lock().entry((name.to_owned(), format!("{}.{}", table.schema, table.table), fragment.prefix.to_string())).or_default() += 1;
        Ok(Response::Fragment { batches: base64::engine::general_purpose::STANDARD.encode(bytes) })
    }

    /// Serves the deployment's reads as of its checkpoint, opening it from object storage alone unless this node already has it open, at that state or a later one.
    pub async fn snapshot(&self, name: &str) -> Result<Response> {
        let turn = self.turn(name);
        let _turn = turn.lock().await;
        let record = self.record(name).await?;
        if record.lifecycle != Lifecycle::Live { return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/not-live", format!("{name} is not live"))); }
        if self.holdings.lock().contains_key(name) && self.node.catalog().database(name).is_ok() { return Ok(Response::Subscribed { address: self.address.clone() }); }
        if let Some(subscription) = self.subscriptions.lock().get_mut(name) {
            if subscription.id == record.id && self.node.catalog().database(name).is_ok() {
                subscription.last_touch = Instant::now();
                return Ok(Response::Subscribed { address: self.address.clone() });
            }
        }
        self.stop_running(name).await;
        let mut record = record;
        for _ in 0..8 {
            let checkpoint = record.checkpoint.ok_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/no-checkpoint", format!("{name} has published no block to read")))?;
            let configuration = database_config(&record, Vec::new(), &self.options.storage, Role::Snapshot(checkpoint));
            match self.node.database_or_attach(name.to_owned(), configuration).await {
                Ok(Some(database)) => {
                    self.subscriptions.lock().insert(name.to_owned(), LocalSubscription { id: record.id, database: Arc::downgrade(&database), last_touch: Instant::now(), opened: Instant::now(), role: Role::Snapshot(checkpoint), fragments: false });
                    return Ok(Response::Subscribed { address: self.address.clone() });
                }
                Err(error) if error.code == crate::store::CHECKPOINT_ADVANCED => {
                    record = self.directory.deployment(&record.id).await?
                        .filter(|fresh| fresh.lifecycle == Lifecycle::Live && fresh.incarnation == record.incarnation
                            && fresh.checkpoint.is_some_and(|published| published.block > checkpoint.block))
                        .ok_or(error)?;
                }
                Err(error) => return Err(error),
                Ok(None) => return Err(Anomaly::fault("rtdb.multi/not-opened", "The node declined to open the snapshot")),
            }
        }
        Err(Anomaly::new(AnomalyCategory::Conflict, crate::store::CHECKPOINT_ADVANCED, "The published checkpoint kept changing while opening the snapshot"))
    }

    /// Publishes a worker's compaction as the deployment's holder.
    pub async fn publish_compaction(&self, name: &str, notification: &[u8]) -> Result<Response> {
        use prost::Message;
        if !self.holdings.lock().contains_key(name) { return Err(Anomaly::new(AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, format!("This node does not hold {name}"))); }
        let notification = rtdb_format::log::proto::TriesAdded::decode(notification).map_err(|error| Anomaly::incorrect("rtdb.multi/compaction", error.to_string()))?;
        let position = self.node.catalog().database(name)?.publish_compaction(notification, &CancellationToken::new()).await?;
        Ok(Response::Published { position: position.0 })
    }

    async fn publish_checkpoint(&self, id: &str, epoch: u64, database: &Database, block: &BlockCatalog) -> Result<Option<(DeploymentRecord, Checkpoint)>> {
        let member = self.member();
        let deposed = || Anomaly::new(AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, "The holder changed before publishing its checkpoint");
        let mut record = self.directory.deployment(id).await?.ok_or_else(&deposed)?;
        let log_epoch = StorageEpoch(record.log_epoch);
        let Some(checkpoint) = checkpoint_for_block(block, log_epoch) else { return Ok(None); };
        let mut manifest = None;
        for _ in 0..8 {
            if record.epoch != epoch || record.holder.as_ref() != Some(&member) || record.log_epoch != log_epoch.0 {
                return Err(deposed());
            }
            if let Some(published) = record.checkpoint {
                if published.block == checkpoint.block && published.root_sha256.is_some() && published != checkpoint {
                    return Err(Anomaly::fault("rtdb.multi/checkpoint-receipt", "The adopted checkpoint disagrees with the directory receipt"));
                }
                if published.block > checkpoint.block || (published.block == checkpoint.block && record.recovery_manifest_sha256.is_some()) {
                    return Ok(Some((record, checkpoint)));
                }
            }
            let digest = match manifest {
                Some(digest) => digest,
                None => {
                    let digest = crate::archive::recovery_manifest_sha256(database.object_store().as_ref(), checkpoint, log_epoch).await?;
                    manifest = Some(digest);
                    digest
                }
            };
            let mut next = record.clone();
            next.checkpoint = Some(checkpoint);
            next.recovery_manifest_sha256 = Some(digest);
            match self.directory.replace_deployment(next, record.version).await? {
                Cas::Applied(stored) => return Ok(Some((stored, checkpoint))),
                Cas::Conflict(Some(current)) => record = current,
                Cas::Conflict(None) => return Err(deposed()),
            }
        }
        Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/checkpoint-contended", "The deployment kept changing while publishing its checkpoint"))
    }

    // The retained boundary record supplies the source position where the next holder resumes intake.
    async fn publish_and_trim(&self, name: &str, current: &DeploymentRecord) {
        let epoch = current.epoch;
        let Ok(database) = self.node.catalog().database(name) else { return };
        let Some(block) = database.status().adopted_block else { return };
        let Some(checkpoint) = checkpoint_for_block(&block, StorageEpoch(current.log_epoch)) else { return };
        let trimmed_through = self.holdings.lock().get(name)
            .filter(|holding| holding.id == current.id && holding.epoch == epoch)
            .map(|holding| holding.trimmed_through.unwrap_or(-1));
        let Some(trimmed_through) = trimmed_through else { return; };
        let needs_trim = trimmed_through < checkpoint.boundary - 1;
        let needs_publication = current.checkpoint.is_none_or(|published| published.block < checkpoint.block
            || (published.block == checkpoint.block && current.recovery_manifest_sha256.is_none()));
        if !needs_publication && !needs_trim { return; }
        let Ok(Some((record, _))) = self.publish_checkpoint(&current.id, epoch, &database, &block).await else { return; };
        let Some(plan) = ReplicaTrimPlan::new(record, checkpoint.boundary, trimmed_through, now_ms()) else { return; };
        let Ok(members) = self.members().await else { return };
        let Ok(Some(grant)) = Box::pin(plan.authorize(self.directory.as_ref(), &members, &self.options.storage, self.keys.as_ref(), self.options.replica_payload_reclamation)).await else { return; };
        if let Ok(through) = grant.apply(self.directory.as_ref()).await
            && let Some(holding) = self.holdings.lock().get_mut(name).filter(|holding| holding.id == current.id && holding.epoch == epoch)
        {
            holding.trimmed_through = Some(through);
        }
    }

    #[doc(hidden)]
    pub async fn bound_tails_for_test(&self, limit: u64) { self.bound_tails(limit).await }

    /// Flushes the largest tails until the holdings' unflushed rows fit within `limit`.
    async fn bound_tails(&self, limit: u64) {
        let names: Vec<String> = self.holdings.lock().keys().cloned().collect();
        let mut tails: Vec<(u64, String)> = names.into_iter().filter_map(|name| self.node.catalog().database(&name).ok().map(|database| (database.live_rows(), name))).collect();
        let mut total: u64 = tails.iter().map(|(rows, _)| rows).sum();
        tails.sort_by(|left, right| right.cmp(left));
        for (rows, name) in tails {
            if total <= limit || rows == 0 { break; }
            if self.flush(&name).await.is_ok() { total -= rows; }
        }
    }

    /// A compaction worker opens, as a compacting replica, every held deployment that ranks it first among the cell's live workers, and closes the rest.
    async fn reconcile_compaction(&self) {
        let Ok(members) = self.members().await else { return };
        let Ok(deployments) = crate::deployment::live_holdings(self.directory.as_ref(), &members, now_ms()).await else { return };
        let now = now_ms();
        let workers: Vec<&MemberRecord> = members.values().filter(|member| member.pool == rtdb_directory::MemberPool::Compaction && member.is_live(now)).collect();
        let me = self.member();
        let assigned_here = |record: &DeploymentRecord| {
            if record.lifecycle != Lifecycle::Live || record.move_from.is_some() || effective_holder(record, &members, now).is_none() { return false; }
            let compatible: Vec<_> = workers.iter().copied().filter(|member| record.permits_engine(member)).collect();
            let ranked = rtdb_affinity::rank(&format!("{}/compaction", record.storage_identity()), &compatible, |member| member.id.as_str());
            ranked.first().is_some_and(|first| first.id == me)
        };
        let mut wanted: BTreeMap<_, _> = deployments.into_iter().filter(&assigned_here).map(|record| (record.name.clone(), record)).collect();
        let current: Vec<(String, String)> = self.subscriptions.lock().iter().filter(|(_, subscription)| subscription.role == Role::Compactor).map(|(name, subscription)| (name.clone(), subscription.id.clone())).collect();
        for (name, id) in current {
            // A handoff can become visible on the record before its new holder index entry appears.
            if !wanted.contains_key(&name) {
                match self.directory.deployment(&id).await {
                    Ok(Some(record)) if assigned_here(&record) => { wanted.insert(name.clone(), record); }
                    Ok(_) => {}
                    Err(_) => continue,
                }
            }
            if wanted.get(&name).is_none_or(|record| record.id != id) || self.node.catalog().database(&name).is_err() {
                let turn = self.turn(&name);
                let _turn = turn.lock().await;
                self.stop_running(&name).await;
            }
        }
        for (name, record) in wanted {
            if self.subscriptions.lock().contains_key(&name) { continue; }
            let turn = self.turn(&name);
            let _turn = turn.lock().await;
            let Ok(servers) = ensemble_servers(&record, &members) else { continue };
            let configuration = database_config(&record, servers, &self.options.storage, Role::Compactor);
            if let Ok(Some(database)) = self.node.database_or_attach(name.clone(), configuration).await {
                self.subscriptions.lock().insert(name.clone(), LocalSubscription { id: record.id, database: Arc::downgrade(&database), last_touch: Instant::now(), opened: Instant::now(), role: Role::Compactor, fragments: false });
            }
        }
    }

    pub(crate) fn hold_active_call(self: &Arc<Self>, database: &Database) -> Option<Box<dyn Send + Sync>> {
        let mut holdings = self.holdings.lock();
        let holding = holdings.get_mut(database.name()).filter(|holding| std::ptr::eq(holding.database.as_ptr(), database))?;
        holding.active_calls += 1;
        holding.last_touch = Instant::now();
        Some(Box::new(HoldingCall { agent: Arc::downgrade(self), name: database.name().into(), attachment: holding.database.clone() }))
    }

    pub fn touch(&self, name: &str) {
        if let Some(holding) = self.holdings.lock().get_mut(name) { holding.last_touch = Instant::now(); }
        if let Some(subscription) = self.subscriptions.lock().get_mut(name) { subscription.last_touch = Instant::now(); }
    }

    /// Reconciles holdings and replicas with the directory, handing off recently used holdings when draining.
    pub async fn reconcile(&self) {
        self.relieve_execution_memory(None).await;
        let local_member = self.lease.record();
        let draining = local_member.state == MemberState::Draining;
        let target_engine = local_member.drain_target_engine.as_deref();
        let warming = std::mem::take(&mut *self.warming.lock());
        if !draining { for name in warming { let _ = self.subscribe_for(&name, SubscriptionUse::Fragment).await; } }
        let me = self.member();
        let destinations = if draining { self.members().await.ok() } else { None };
        let active_within = self.options.park_after.map_or(ACTIVE_DRAIN_WINDOW, |after| after.min(ACTIVE_DRAIN_WINDOW));
        let choose_drain_target = |record: &DeploymentRecord| {
            let now = now_ms();
            let candidates: Vec<_> = destinations.as_ref()?.values().filter(|member| member.id != me
                && member.control_address.is_some() && record.may_place_on(member, now)
                && target_engine.is_none_or(|target| member.engine_version == target)
                && record.storage_namespaces.is_none_or(|namespaces| member.storage_namespaces == Some(namespaces))).collect();
            rtdb_affinity::rank(&record.storage_identity(), &candidates, |member| member.id.as_str())
                .first().map(|target| target.id.clone())
        };
        let mut handoffs = Vec::new();
        let mut idle_flushes = 0;
        let mut suspended = 0;
        let mut held: Vec<_> = self.holdings.lock().iter().map(|(name, holding)| {
            (name.clone(), holding.id.clone(), holding.epoch, holding.last_touch, holding.claimed_version, holding.placement.clone(), holding.opened, holding.active_calls)
        }).collect();
        held.sort_by_key(|(_, _, _, touched, _, _, _, _)| std::cmp::Reverse(*touched));
        for (rank, (name, id, epoch, last_touch, claimed_version, placement, opened, active_calls)) in held.into_iter().enumerate() {
            let current = placement.borrow().as_ref().filter(|record| record.version >= claimed_version).cloned();
            let placed_here = |record: &DeploymentRecord| record.epoch == epoch && record.holder.as_ref() == Some(&me);
            let deposed = current.as_ref().is_some_and(|record| !placed_here(record));
            let stopped = self.node.catalog().database(&name).map_or(true, |database| matches!(database.status().role, DriverRole::Stopped));
            if deposed || stopped {
                let turn = self.turn(&name);
                let _turn = turn.lock().await;
                // An activation or park may have replaced the holding while this pass waited for its turn; only the holding it looked at is given up.
                if !self.holdings.lock().get(&name).is_some_and(|holding| holding.id == id && holding.epoch == epoch) { continue; }
                let stopped = self.node.catalog().database(&name).map_or(true, |database| matches!(database.status().role, DriverRole::Stopped));
                self.stop_running(&name).await;
                if !deposed && stopped { self.release_failed_holding(&id, epoch, true).await; }
                continue;
            }
            let Some(record) = current.as_ref() else { continue };
            if draining {
                if target_engine.is_none() && record.engine_pin.is_none() && active_calls == 0 && last_touch.elapsed() >= active_within {
                    let _ = self.park(&name).await;
                } else if handoffs.len() < DRAIN_HANDOFFS_PER_PASS && let Some(target) = choose_drain_target(record) {
                    handoffs.push((name, epoch, target));
                }
                continue;
            }
            self.publish_and_trim(&name, record).await;
            let over = self.options.max_active.is_some_and(|limit| rank >= limit);
            if over || active_calls == 0 && self.options.park_after.is_some_and(|after| last_touch.elapsed() >= after) {
                let _ = self.park(&name).await;
            } else if active_calls == 0 && suspended < RESIDENT_IDLE_PER_PASS
                && self.options.resident_idle_after.is_some_and(|after| last_touch.elapsed() >= after)
                && opened.elapsed() >= self.options.min_residency
            {
                suspended += 1;
                let _ = self.suspend_if_idle(&name, record, last_touch).await;
            } else if active_calls == 0 && idle_flushes < IDLE_FLUSHES_PER_PASS && last_touch.elapsed() >= self.options.clean_after_idle
                && self.node.catalog().database(&name).is_ok_and(|database| database.live_rows() > 0)
            {
                idle_flushes += 1;
                let _ = self.flush(&name).await;
            }
        }
        if self.options.park_after.is_some() {
            self.park_due_residents().await;
        }
        if self.options.resident_idle_after.is_some() && draining
            && let Ok(residents) = self.directory.deployments_held_by(&me).await
        {
            for record in residents.into_iter().filter(DeploymentRecord::is_resident_idle) {
                if target_engine.is_none() && record.engine_pin.is_none() {
                    let _ = self.park_if_resident(&record).await;
                } else if handoffs.len() < DRAIN_HANDOFFS_PER_PASS && let Some(target) = choose_drain_target(&record) {
                    handoffs.push((record.name, record.epoch, target));
                }
            }
        }
        if !handoffs.is_empty() {
            use futures::StreamExt;

            let control = crate::control::ControlKey(self.secret.clone());
            let control = &control;
            let source = &me;
            futures::stream::iter(handoffs.into_iter().map(|(name, epoch, target)| async move {
                let _ = crate::admin::move_from_holding(self.directory.as_ref(), &name, &target, Some((source, epoch)), control).await;
            })).buffer_unordered(DRAIN_CONCURRENT_HANDOFFS).for_each(|()| async {}).await;
        }
        if self.options.compaction_worker { self.reconcile_compaction().await; }
        if let Some(limit) = self.options.max_tail_rows { self.bound_tails(limit).await; }
        let subscribed: Vec<(String, String, Instant, Instant, bool, bool)> = self.subscriptions.lock().iter().filter(|(_, subscription)| subscription.role != Role::Compactor)
            .map(|(name, subscription)| (name.clone(), subscription.id.clone(), subscription.last_touch, subscription.opened, matches!(subscription.role, Role::Snapshot(_)), subscription.fragments)).collect();
        let reader_members = if subscribed.iter().any(|(_, _, _, _, snapshot, fragments)| !snapshot && !fragments) {
            self.members().await.ok()
        } else { None };
        for (name, id, last_touch, opened, snapshot, fragments) in subscribed {
            let wanted = match self.directory.deployment(&id).await {
                Ok(Some(record)) if record.lifecycle == Lifecycle::Live => snapshot || fragments
                    || (record.read_width > 1 && reader_members.as_ref().is_none_or(|members| {
                        crate::deployment::read_replicas(&record, members, now_ms()).iter().any(|member| member.id == me)
                    })),
                Ok(_) => false,
                Err(_) => true,
            };
            // A snapshot closes before the reaper's grace could erase what it reads.
            let expired = snapshot && opened.elapsed() >= self.options.snapshot_lifetime;
            if draining || !wanted || expired || last_touch.elapsed() >= self.options.unsubscribe_after || self.node.catalog().database(&name).is_err() {
                let turn = self.turn(&name);
                let _turn = turn.lock().await;
                if self.subscriptions.lock().get(&name).is_some_and(|current| current.id == id && current.opened == opened) {
                    self.stop_running(&name).await;
                }
            }
        }
    }

    pub async fn run(self: Arc<Self>, cancellation: CancellationToken) {
        let lost = self.lease.lost();
        let mut ticker = tokio::time::interval(self.options.watch_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = lost.cancelled() => {
                    // Without a lease this node cannot know it still holds anything, so it stops serving all of it.
                    let names: Vec<String> = self.holdings.lock().keys().chain(self.subscriptions.lock().keys()).cloned().collect();
                    for name in names { self.stop_running(&name).await; }
                    return;
                }
                _ = ticker.tick() => self.reconcile().await,
            }
        }
    }
}

fn checkpoint_for_block(block: &BlockCatalog, log_epoch: StorageEpoch) -> Option<Checkpoint> {
    let boundary = block.boundary_replica_position()?.after_offset(log_epoch);
    if boundary < 0 { return None; }
    Some(Checkpoint {
        block: block.block_index().0,
        boundary,
        source: block.latest_source_position().map_or(-1, |position| position.after_offset(log_epoch)),
        system_time: block.latest_completed_tx().map_or(i64::MIN, |tx| tx.system_time.0),
        root_sha256: Some(block.publication_sha256()?),
    })
}

fn short_handoff_tail(database: &Database, log_epoch: StorageEpoch) -> bool {
    let status = database.status();
    let boundary = status.adopted_block.as_ref().and_then(|block| block.boundary_replica_position())
        .map_or(-1, |position| position.after_offset(log_epoch));
    matches!(status.role, DriverRole::Leading(_))
        && (0..=HANDOFF_REPLAY_RECORDS).contains(&(status.replica_position.after_offset(log_epoch) - boundary))
        && database.live_rows() <= HANDOFF_REPLAY_ROWS
}

fn stale_handoff(name: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/stale-handoff", format!("The requested holding of {name} has changed"))
}

fn checkpoint_timeout(name: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/checkpoint-timeout", format!("The checkpoint for {name} was not published in time"))
}

#[async_trait]
impl Handler for NodeAgent {
    async fn handle(&self, request: Request) -> Response {
        let result = match request {
            Request::Activate { name } => self.activate(&name).await,
            Request::ActivateForMove { name, ticket } => self.activate_holding(&name, Some(&ticket)).await,
            Request::Park { name } => self.park(&name).await,
            Request::ParkForMove { name, epoch, ticket } => self.park_holding(&name, Some((epoch, &ticket))).await,
            Request::Touch { name } => { self.touch(&name); Ok(Response::Done) }
            Request::Ping => Ok(Response::Done),
            Request::OpenPgwire => Err(Anomaly::unsupported("rtdb.multi/control-request", "This control endpoint does not provide Pgwire")),
            Request::HoldingLoads { measure_query_cpu } => Ok(Response::HoldingLoads { holdings: self.holding_loads(measure_query_cpu) }),
            Request::ReadLoads => Ok(Response::ReadLoads { loads: self.read_loads() }),
            Request::FlightEndpoint { audience } => self.flight_endpoints.get(audience).clone().map(|address| Response::FlightEndpoint { address })
                .ok_or_else(|| Anomaly::incorrect("rtdb.multi/no-flight-endpoint", "The holder does not have a Flight SQL endpoint")),
            Request::Subscribe { name } => self.subscribe(&name).await,
            Request::SubscribeFresh { name } => self.subscribe_for(&name, SubscriptionUse::FreshQuery).await,
            Request::Snapshot { name } => self.snapshot(&name).await,
            Request::Flush { name } => self.flush(&name).await,
            Request::PinCheckpointSuffix { name, deployment, lease, capture, basis, expires_at_ms } =>
                self.pin_checkpoint_suffix(&name, deployment, lease, capture, basis, expires_at_ms).await,
            Request::PublishCompaction { name, notification, grant } => match self.check_node_grant(&grant, &name) { Ok(()) => self.publish_compaction(&name, &notification).await, Err(error) => Err(error) }
            Request::ScanFragment { name, fragment, grant } => match self.check_node_grant(&grant, &name) { Ok(()) => self.scan_fragment(&name, &fragment).await, Err(error) => Err(error) }
        };
        result.unwrap_or_else(|error| Response::refused(&error))
    }
}


#[cfg(test)]
mod tests {
    use super::{ActivationWindow, DeploymentTurn, Duration, Instant, NonZeroUsize, Turns};

    #[test]
    fn the_activation_window_counts_the_last_second_across_clock_boundaries() {
        let mut window = ActivationWindow::default();
        let limit = NonZeroUsize::new(2).unwrap();
        let first = Instant::now();
        assert!(window.admit(first, limit));
        assert!(window.admit(first + Duration::from_millis(600), limit));
        assert!(!window.admit(first + Duration::from_millis(999), limit));
        assert!(window.admit(first + Duration::from_secs(1), limit));
        assert!(!window.admit(first + Duration::from_millis(1599), limit));
        assert!(window.admit(first + Duration::from_millis(1600), limit));
    }

    #[tokio::test]
    async fn a_deployment_turn_survives_while_another_operation_uses_it() {
        let turns = Turns::default();
        let first = DeploymentTurn::new(&turns, "shop");
        let held = first.lock().await;
        let second = DeploymentTurn::new(&turns, "shop");
        assert!(second.gate.as_ref().unwrap().try_lock().is_err());
        drop(held);
        drop(first);
        assert_eq!(turns.lock().len(), 1);
        drop(second.lock().await);
        drop(second);
        assert!(turns.lock().is_empty());
    }

    #[test]
    fn departing_operations_reclaim_their_turn_under_contention() {
        let turns = Turns::default();
        for index in 0..100 {
            let name = format!("deployment-{index}");
            let first = DeploymentTurn::new(&turns, &name);
            let second = DeploymentTurn::new(&turns, &name);
            std::thread::scope(|scope| {
                scope.spawn(move || drop(first));
                scope.spawn(move || drop(second));
            });
            assert!(turns.lock().is_empty());
        }
    }
}
