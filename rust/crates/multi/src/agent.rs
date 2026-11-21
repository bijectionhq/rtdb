//! A node's agent: the member that claims, opens, watches, parks and gives up deployments on one node (`Claim`, `OpenHolding`, `Park`, `DeposeOnRefusedAppend`, `ParkUnderPressure`, `CountActivationCrash`), and serves log-fed read replicas (`Subscription`).
//!
//! The agent never decides safety.
//! The directory's version check decides who holds an epoch, and the logkeepers refuse every append from an earlier one; the agent only keeps what this node runs in step with what the directory says.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_directory::{Cas, DeploymentRecord, Directory, Lifecycle, MemberId, MemberRecord, MemberState, claim, effective_holder, now_ms, park};
use rtdb_node::{Database, Node};
use rtdb_replication::Role as DriverRole;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio_util::sync::CancellationToken;

use crate::control::{Handler, MoveTicket, Request, Response};
use crate::deployment::{CellStorage, Role, database_config, ensemble_servers, select_logkeepers};
use crate::lease::MemberLease;

#[derive(Clone, Debug)]
pub struct AgentOptions {
    pub storage: CellStorage,
    /// How long an activation may take to reach a confirmed leadership before it is abandoned.
    pub activation_timeout: Duration,
    /// How often held deployments are checked against the directory.
    pub watch_interval: Duration,
    /// Park a deployment nobody has touched for this long; never, if absent.
    pub park_after: Option<Duration>,
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

impl AgentOptions {
    pub fn new(storage: CellStorage) -> Self {
        Self { storage, activation_timeout: Duration::from_secs(30), watch_interval: Duration::from_millis(250), park_after: None,
            unsubscribe_after: Duration::from_secs(60), max_active: None, quarantine_after: 2, compaction_worker: false, max_tail_rows: None, snapshot_lifetime: SNAPSHOT_LIFETIME,
            fragments: crate::fragments::FragmentOptions::default() }
    }
}

/// How long a snapshot stays open by default, which a reaper's grace must exceed (`max_snapshot_lifetime`).
pub const SNAPSHOT_LIFETIME: Duration = Duration::from_secs(30 * 60);

const CONFIRM_WITHIN: Duration = Duration::from_secs(2);

struct LocalHolding {
    id: String,
    epoch: u64,
    last_touch: Instant,
    linearizable: bool,
    trimmed_through: Option<i64>,
}

type Turns = Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>;

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
struct LocalSubscription { id: String, last_touch: Instant, opened: Instant, role: Role, fragments: bool }

pub struct NodeAgent {
    directory: Arc<dyn Directory>,
    node: Node,
    lease: Arc<MemberLease>,
    address: String,
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
    /// How many fragments this node has read for other nodes' scans, by deployment, table and prefix.
    fragments_read: Mutex<BTreeMap<(String, String, String), u64>>,
}

impl NodeAgent {
    pub fn new(directory: Arc<dyn Directory>, node: Node, lease: Arc<MemberLease>, address: String, options: AgentOptions, writers: crate::logs::Writers, secret: Option<Arc<crate::capability::CellSecret>>) -> Arc<Self> {
        Arc::new(Self { directory, node, lease, address, options, secret, warming: Mutex::new(BTreeSet::new()), holdings: Mutex::new(BTreeMap::new()), writers, subscriptions: Mutex::new(BTreeMap::new()), turns: Mutex::new(HashMap::new()), fragments_read: Mutex::new(BTreeMap::new()) })
    }

    pub fn member(&self) -> MemberId { self.lease.id() }
    pub fn held(&self) -> BTreeMap<String, u64> { self.holdings.lock().iter().map(|(name, holding)| (name.clone(), holding.epoch)).collect() }
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
        let _turn = turn.lock().await;
        let me = self.member();
        for _ in 0..4 {
            let record = self.record(name).await?;
            if moving.is_some_and(|ticket| ticket.deployment != record.id) {
                return Err(stale_handoff(name));
            }
            let members = self.members().await?;
            let now = now_ms();
            let holder = effective_holder(&record, &members, now);
            if let Some(holding) = self.holdings.lock().get_mut(name) {
                if holding.id == record.id && holding.epoch == record.epoch && holder == Some(&me) && self.node.catalog().database(name).is_ok() {
                    holding.last_touch = Instant::now();
                    return Ok(Response::Held { epoch: record.epoch, address: self.address.clone() });
                }
            }
            if moving.is_some_and(|ticket| !record.active_move(now).is_some_and(|intent| intent.request == ticket.request && intent.target == me)) {
                return Err(stale_handoff(name));
            }
            if let Some(holder) = holder {
                if *holder != me {
                    let address = members.get(holder).map(|member| member.address.clone()).unwrap_or_default();
                    return Ok(Response::Redirect { holder: holder.to_string(), address });
                }
            }
            let Some(next) = claim(&record, &me, &members, now) else {
                return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/not-claimable", format!("{name} cannot be claimed by this member now")));
            };
            match self.directory.replace_deployment(next, record.version).await? {
                Cas::Applied(held) => return self.open(held, &members).await,
                Cas::Conflict(_) => continue,
            }
        }
        Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/claim-contended", format!("{name} kept changing while being claimed")))
    }

    async fn open(&self, record: DeploymentRecord, members: &BTreeMap<MemberId, MemberRecord>) -> Result<Response> {
        self.stop_running(&record.name).await;
        let opened = async {
            let configuration = self.holder_config(&record, members)?;
            let database = self.node.database_or_attach(record.name.clone(), configuration).await?
                .ok_or_else(|| Anomaly::fault("rtdb.multi/not-opened", "The node declined to open the deployment"))?;
            self.await_leadership(&record.name, &database).await
        }.await;
        match opened {
            Ok(()) => {
                self.holdings.lock().insert(record.name.clone(), LocalHolding { id: record.id.clone(), epoch: record.epoch, last_touch: Instant::now(), linearizable: record.linearizable_reads, trimmed_through: None });
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
        if let Some(previous) = &record.move_from {
            let logkeepers: Vec<_> = members.values().filter(|member| member.role == rtdb_directory::MemberRole::Logkeeper && member.is_live(now)).collect();
            let count = record.ensemble.len();
            let mut selected = select_logkeepers(&record.storage_identity(), count, Vec::new(), &logkeepers);
            let mut addresses: BTreeSet<_> = selected.iter().map(|member| member.address.as_str()).collect();
            if selected.len() < crate::deployment::write_quorum(count) {
                return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/no-log-quorum", "The destination cannot form a write quorum for the moved log"));
            }
            for id in &record.ensemble {
                if selected.len() == count { break; }
                if let Some(member) = members.get(id).filter(|member| member.role == rtdb_directory::MemberRole::Logkeeper)
                    && addresses.insert(member.address.as_str()) {
                    selected.push(member);
                }
            }
            if selected.len() != count {
                return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/no-log-quorum", "The destination cannot resolve the moved log's full ensemble width"));
            }
            let servers = selected.iter().map(|member| member.address.clone()).collect();
            let ensemble = selected.iter().map(|member| member.id.clone()).collect();
            return Ok(crate::deployment::moving_config(record, servers, Some((previous.clone(), ensemble)), &self.options.storage, Role::Holder));
        }
        let servers = ensemble_servers(record, members)?;
        let alive: Vec<MemberId> = record.ensemble.iter().filter(|id| live(id)).cloned().collect();
        if alive.len() == record.ensemble.len() || alive.len() < crate::deployment::write_quorum(record.ensemble.len()) {
            return Ok(database_config(record, servers, &self.options.storage, Role::Holder));
        }
        let candidates: Vec<&MemberRecord> = members.values().filter(|member| member.role == rtdb_directory::MemberRole::Logkeeper && member.is_live(now) && !record.ensemble.contains(&member.id)).collect();
        let live_members = alive.iter().map(|id| &members[id]).collect();
        let selected = select_logkeepers(&record.storage_identity(), record.ensemble.len(), live_members, &candidates);
        if selected.len() < record.ensemble.len() { return Ok(database_config(record, servers, &self.options.storage, Role::Holder)); }
        let next = selected.iter().map(|member| member.address.clone()).collect();
        let ensemble = selected.iter().map(|member| member.id.clone()).collect();
        Ok(crate::deployment::moving_config(record, next, Some((servers, ensemble)), &self.options.storage, Role::Holder))
    }

    /// Before a query on a deployment held with linearizable reads, confirms with a quorum of its logkeepers that no later holder has sealed its stream (`ServeLinearizableReadOnWriter`).
    pub async fn confirm_read(&self, name: &str) -> Result<()> {
        let Some(id) = self.holdings.lock().get(name).filter(|holding| holding.linearizable).map(|holding| holding.id.clone()) else { return Ok(()) };
        let writer = self.writers.lock().get(&id).and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, format!("This node no longer holds {name}")))?;
        match tokio::time::timeout(CONFIRM_WITHIN, writer.confirm_leadership()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(Anomaly::new(AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, format!("This node no longer holds {name}"))),
            Err(_) => Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/leadership-unconfirmed", format!("A quorum of {name}'s logkeepers did not confirm its holder in time"))),
        }
    }

    async fn await_leadership(&self, name: &str, database: &Arc<Database>) -> Result<()> {
        let deadline = Instant::now() + self.options.activation_timeout;
        loop {
            let status = database.status();
            if matches!(status.role, DriverRole::Leading(_)) { return Ok(()); }
            if let Some(failure) = status.promotion_failure { return Err(failure); }
            if matches!(status.role, DriverRole::Stopped) {
                return Err(database.progress().fault().unwrap_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/open-failed", "The deployment's driver stopped while it was being opened")));
            }
            if self.node.catalog().database(name).is_err() { return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/open-failed", "The deployment stopped while it was being opened")); }
            if Instant::now() > deadline {
                return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/activation-timeout", format!(
                    "The holder did not confirm its leadership in time (role {:?}, highest term {:?}, replica position {:?}, open block {:?})",
                    status.role, status.highest_term, status.replica_position, status.open_block)));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
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
        self.holdings.lock().remove(name);
        self.subscriptions.lock().remove(name);
        if self.node.catalog().database(name).is_ok() { let _ = self.node.detach_local(name.to_owned()).await; }
        while self.node.catalog().is_draining(name) { tokio::time::sleep(Duration::from_millis(5)).await; }
    }

    /// Flushes `database` and waits until the block that was open is adopted.
    async fn flush_and_adopt(&self, database: &Arc<Database>, id: &str, epoch: u64) -> Result<()> {
        let member = self.member();
        let mut watched = rtdb_directory::watch_deployment(self.directory.clone(), id.to_owned(), Duration::from_millis(100));
        let deposed = watched.wait_for(|snapshot| snapshot.as_ref().is_some_and(|record| {
            record.epoch > epoch || record.epoch == epoch && record.holder.as_ref() != Some(&member)
        }));
        let flush = async {
            let open = database.status().open_block;
            database.flush().await?;
            while database.status().adopted_block.as_ref().is_none_or(|block| block.block_index() < open) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok(())
        };
        tokio::select! {
            _ = deposed => Err(Anomaly::new(AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, "The holder was deposed while waiting for its flush")),
            flushed = tokio::time::timeout(self.options.activation_timeout, flush) => flushed.unwrap_or_else(|_| {
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
        self.flush_and_adopt(&database, &id, epoch).await?;
        Ok(Response::Done)
    }

    pub async fn park(&self, name: &str) -> Result<Response> { self.park_holding(name, None).await }

    async fn park_holding(&self, name: &str, moving: Option<(u64, &MoveTicket)>) -> Result<Response> {
        let turn = self.turn(name);
        let _turn = turn.lock().await;
        let Some((id, epoch)) = self.holdings.lock().get(name).map(|holding| (holding.id.clone(), holding.epoch)) else { return Ok(Response::Done); };
        let is_requested = |record: &DeploymentRecord| moving.is_none_or(|(epoch, ticket)| {
            record.id == ticket.deployment && record.epoch == epoch && record.holder.as_ref() == Some(&self.member())
                && record.active_move(now_ms()).is_some_and(|intent| intent.request == ticket.request)
        });
        if let Some((move_epoch, ticket)) = moving {
            if id != ticket.deployment || epoch != move_epoch
                || !self.directory.deployment(&id).await?.as_ref().is_some_and(is_requested) {
                return Err(stale_handoff(name));
            }
        }

        // The next holder starts from a closed block once the flushed block has been adopted.
        if let Ok(database) = self.node.catalog().database(name) { self.flush_and_adopt(&database, &id, epoch).await?; }
        for _ in 0..8 {
            let Some(record) = self.directory.deployment(&id).await? else { break };
            if !is_requested(&record) { return Err(stale_handoff(name)); }
            let Some(parked) = park(&record, &self.member(), epoch) else { break };
            if let Cas::Applied(_) = self.directory.replace_deployment(parked, record.version).await? {
                self.stop_running(name).await;
                return Ok(Response::Done);
            }
        }
        if self.directory.deployment(&id).await?.as_ref().is_some_and(|record| record.epoch == epoch && record.holder.as_ref() == Some(&self.member())) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/update-contended", format!("{name} kept changing while it was parked")));
        }
        self.stop_running(name).await;
        Ok(Response::Done)
    }

    pub async fn subscribe(&self, name: &str) -> Result<Response> { self.subscribe_for(name, false).await }

    /// Opens a log-fed replica of `name` unless this node already serves it; `fragments` keeps it open for fragment reads.
    async fn subscribe_for(&self, name: &str, fragments: bool) -> Result<Response> {
        let turn = self.turn(name);
        let _turn = turn.lock().await;
        let record = self.record(name).await?;
        if record.lifecycle != Lifecycle::Live { return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/not-live", format!("{name} is not live"))); }
        if record.move_from.is_some() {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/log-move-pending", format!("{name}'s holder has yet to adopt its log")));
        }
        if let Some(subscription) = self.subscriptions.lock().get_mut(name) {
            // A snapshot never advances, so it is replaced by a replica rather than served as one.
            if subscription.id == record.id && !matches!(subscription.role, Role::Snapshot(_)) && self.node.catalog().database(name).is_ok() {
                subscription.last_touch = Instant::now();
                subscription.fragments |= fragments;
                return Ok(Response::Subscribed { address: self.address.clone() });
            }
        }
        if self.holdings.lock().contains_key(name) { return Ok(Response::Subscribed { address: self.address.clone() }); }
        let members = self.members().await?;
        let configuration = database_config(&record, ensemble_servers(&record, &members)?, &self.options.storage, Role::Subscriber);
        self.stop_running(name).await;
        self.node.database_or_attach(name.to_owned(), configuration).await?
            .ok_or_else(|| Anomaly::fault("rtdb.multi/not-opened", "The node declined to open the replica"))?;
        self.subscriptions.lock().insert(name.to_owned(), LocalSubscription { id: record.id, last_touch: Instant::now(), opened: Instant::now(), role: Role::Subscriber, fragments });
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
        let bytes = self.node.scan_fragment(fragment, self.options.fragments.catch_up, &CancellationToken::new()).await?;
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
        let checkpoint = record.checkpoint.ok_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/no-checkpoint", format!("{name} has published no block to read")))?;
        let configuration = database_config(&record, Vec::new(), &self.options.storage, Role::Snapshot(checkpoint));
        self.stop_running(name).await;
        self.node.database_or_attach(name.to_owned(), configuration).await?
            .ok_or_else(|| Anomaly::fault("rtdb.multi/not-opened", "The node declined to open the snapshot"))?;
        self.subscriptions.lock().insert(name.to_owned(), LocalSubscription { id: record.id, last_touch: Instant::now(), opened: Instant::now(), role: Role::Snapshot(checkpoint), fragments: false });
        Ok(Response::Subscribed { address: self.address.clone() })
    }

    /// Publishes a worker's compaction as the deployment's holder.
    pub async fn publish_compaction(&self, name: &str, notification: &[u8]) -> Result<Response> {
        use prost::Message;
        if !self.holdings.lock().contains_key(name) { return Err(Anomaly::new(AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, format!("This node does not hold {name}"))); }
        let notification = rtdb_format::log::proto::TriesAdded::decode(notification).map_err(|error| Anomaly::incorrect("rtdb.multi/compaction", error.to_string()))?;
        let position = self.node.catalog().database(name)?.publish_compaction(notification, &CancellationToken::new()).await?;
        Ok(Response::Published { position: position.0 })
    }

    /// Publishes the holding's last adopted block as the deployment's checkpoint, and trims its stream below that block's boundary, whose records the block now holds (`TrimFollowsPublication`).
    /// The boundary record itself is kept: it names the source position a later holder's intake starts past.
    async fn publish_and_trim(&self, name: &str, id: &str, epoch: u64) {
        let Ok(database) = self.node.catalog().database(name) else { return };
        let Some(block) = database.status().adopted_block else { return };
        let Some(position) = block.boundary_replica_position() else { return };
        let Ok(Some(record)) = self.directory.deployment(id).await else { return };
        if record.epoch != epoch || record.holder.as_ref() != Some(&self.member()) { return; }

        let log_epoch = rtdb_types::StorageEpoch(record.log_epoch);
        let boundary = position.after_offset(log_epoch);
        if boundary < 0 { return; }
        let block_index = block.block_index().0;
        if record.checkpoint.is_none_or(|checkpoint| checkpoint.block < block_index) {
            let mut next = record.clone();
            let source = block.latest_source_position().map_or(-1, |position| position.after_offset(log_epoch));
            let system_time = block.latest_completed_tx().map_or(i64::MIN, |tx| tx.system_time.0);
            next.checkpoint = Some(rtdb_directory::Checkpoint { block: block_index, boundary, source, system_time });
            if !matches!(self.directory.replace_deployment(next, record.version).await, Ok(Cas::Applied(_))) { return; }
        }

        if boundary == 0 || !self.holdings.lock().get(name).is_some_and(|holding| {
            holding.id == id && holding.epoch == epoch && holding.trimmed_through.is_none_or(|trimmed| trimmed < boundary - 1)
        }) {
            return;
        }
        let Ok(members) = self.members().await else { return };
        let Ok(servers) = ensemble_servers(&record, &members) else { return };
        let stream = rtdb_logkeeper::StreamConfig { write_quorum: crate::deployment::write_quorum(servers.len()), servers, stream: record.stream("replica"), maximum_message_bytes: 4 << 20 };
        if rtdb_logkeeper::trim(&stream, rtdb_storage::log::LogOffset(boundary - 1)).await.is_ok()
            && let Some(holding) = self.holdings.lock().get_mut(name).filter(|holding| holding.id == id && holding.epoch == epoch)
        {
            holding.trimmed_through = Some(boundary - 1);
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
        let (Ok(deployments), Ok(members)) = (self.directory.deployments().await, self.members().await) else { return };
        let now = now_ms();
        let workers: Vec<&MemberRecord> = members.values().filter(|member| member.pool == rtdb_directory::MemberPool::Compaction && member.is_live(now)).collect();
        let me = self.member();
        let mut wanted = BTreeMap::new();
        for record in deployments.iter().filter(|record| record.lifecycle == Lifecycle::Live && record.move_from.is_none() && effective_holder(record, &members, now).is_some()) {
            let ranked = rtdb_affinity::rank(&format!("{}/compaction", record.storage_identity()), &workers, |member| member.id.as_str());
            if ranked.first().is_some_and(|first| first.id == me) { wanted.insert(record.name.clone(), record.clone()); }
        }
        let current: Vec<(String, String)> = self.subscriptions.lock().iter().filter(|(_, subscription)| subscription.role == Role::Compactor).map(|(name, subscription)| (name.clone(), subscription.id.clone())).collect();
        for (name, id) in current {
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
            if let Ok(Some(_)) = self.node.database_or_attach(name.clone(), configuration).await {
                self.subscriptions.lock().insert(name.clone(), LocalSubscription { id: record.id, last_touch: Instant::now(), opened: Instant::now(), role: Role::Compactor, fragments: false });
            }
        }
    }

    pub fn touch(&self, name: &str) {
        if let Some(holding) = self.holdings.lock().get_mut(name) { holding.last_touch = Instant::now(); }
        if let Some(subscription) = self.subscriptions.lock().get_mut(name) { subscription.last_touch = Instant::now(); }
    }

    /// Gives up every deployment the directory no longer places here under the epoch this node opened it at, every one whose database stopped, every idle one, and the least recently used beyond `max_active`; drops idle or unwanted replicas.
    pub async fn reconcile(&self) {
        let draining = self.lease.record().state == MemberState::Draining;
        let warming = std::mem::take(&mut *self.warming.lock());
        if !draining { for name in warming { let _ = self.subscribe_for(&name, true).await; } }
        let me = self.member();
        let mut held: Vec<(String, String, u64, Instant)> = self.holdings.lock().iter().map(|(name, holding)| (name.clone(), holding.id.clone(), holding.epoch, holding.last_touch)).collect();
        held.sort_by_key(|(.., touched)| std::cmp::Reverse(*touched));
        for (rank, (name, id, epoch, last_touch)) in held.into_iter().enumerate() {
            let current = match self.directory.deployment(&id).await { Ok(current) => current, Err(_) => continue };
            let placed_here = current.as_ref().is_some_and(|record| record.epoch == epoch && record.holder.as_ref() == Some(&me));
            let stopped = self.node.catalog().database(&name).map_or(true, |database| matches!(database.status().role, DriverRole::Stopped));
            if !placed_here || stopped {
                let turn = self.turn(&name);
                let _turn = turn.lock().await;
                // An activation or park may have replaced the holding while this pass waited for its turn; only the holding it looked at is given up.
                if !self.holdings.lock().get(&name).is_some_and(|holding| holding.id == id && holding.epoch == epoch) { continue; }
                let stopped = self.node.catalog().database(&name).map_or(true, |database| matches!(database.status().role, DriverRole::Stopped));
                self.stop_running(&name).await;
                if placed_here && stopped { self.release_failed_holding(&id, epoch, true).await; }
                continue;
            }
            self.publish_and_trim(&name, &id, epoch).await;
            let over = self.options.max_active.is_some_and(|limit| rank >= limit);
            if draining || over || self.options.park_after.is_some_and(|after| last_touch.elapsed() >= after) { let _ = self.park(&name).await; }
        }
        if self.options.compaction_worker { self.reconcile_compaction().await; }
        if let Some(limit) = self.options.max_tail_rows { self.bound_tails(limit).await; }
        let subscribed: Vec<(String, String, Instant, Instant, bool, bool)> = self.subscriptions.lock().iter().filter(|(_, subscription)| subscription.role != Role::Compactor)
            .map(|(name, subscription)| (name.clone(), subscription.id.clone(), subscription.last_touch, subscription.opened, matches!(subscription.role, Role::Snapshot(_)), subscription.fragments)).collect();
        for (name, id, last_touch, opened, snapshot, fragments) in subscribed {
            let wanted = matches!(self.directory.deployment(&id).await, Ok(Some(record)) if record.lifecycle == Lifecycle::Live && (snapshot || fragments || record.read_width > 1));
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

fn stale_handoff(name: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/stale-handoff", format!("The requested holding of {name} has changed"))
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
            Request::Subscribe { name } => self.subscribe(&name).await,
            Request::Snapshot { name } => self.snapshot(&name).await,
            Request::Flush { name } => self.flush(&name).await,
            Request::PublishCompaction { name, notification, grant } => match self.check_node_grant(&grant, &name) { Ok(()) => self.publish_compaction(&name, &notification).await, Err(error) => Err(error) }
            Request::ScanFragment { name, fragment, grant } => match self.check_node_grant(&grant, &name) { Ok(()) => self.scan_fragment(&name, &fragment).await, Err(error) => Err(error) }
        };
        result.unwrap_or_else(|error| Response::refused(&error))
    }
}


#[cfg(test)]
mod tests {
    use super::{DeploymentTurn, Turns};

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
