//! A node's agent: the member that claims, opens, watches, parks and gives up deployments on one node (`Claim`, `OpenHolding`, `Park`, `DeposeOnRefusedAppend`).
//!
//! The agent never decides safety.
//! The directory's version check decides who holds an epoch, and the logkeepers refuse every append from an earlier one; the agent only keeps what this node runs in step with what the directory says.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_directory::{Cas, DeploymentRecord, Directory, MemberId, MemberRecord, claim, effective_holder, now_ms, park};
use rtdb_node::Node;
use rtdb_replication::Role as DriverRole;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio_util::sync::CancellationToken;

use crate::control::{Handler, Request, Response};
use crate::deployment::{CellStorage, Role, database_config};
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
}

impl AgentOptions {
    pub fn new(storage: CellStorage) -> Self {
        Self { storage, activation_timeout: Duration::from_secs(30), watch_interval: Duration::from_millis(250), park_after: None }
    }
}

struct LocalHolding { id: String, epoch: u64, last_touch: Instant }

pub struct NodeAgent {
    directory: Arc<dyn Directory>,
    node: Node,
    lease: Arc<MemberLease>,
    address: String,
    options: AgentOptions,
    holdings: Mutex<BTreeMap<String, LocalHolding>>,
    /// One activation or park at a time per deployment name.
    turns: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl NodeAgent {
    pub fn new(directory: Arc<dyn Directory>, node: Node, lease: Arc<MemberLease>, address: String, options: AgentOptions) -> Arc<Self> {
        Arc::new(Self { directory, node, lease, address, options, holdings: Mutex::new(BTreeMap::new()), turns: Mutex::new(HashMap::new()) })
    }

    pub fn member(&self) -> MemberId { self.lease.id() }
    pub fn held(&self) -> BTreeMap<String, u64> { self.holdings.lock().iter().map(|(name, holding)| (name.clone(), holding.epoch)).collect() }

    fn turn(&self, name: &str) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(self.turns.lock().entry(name.to_owned()).or_default())
    }

    async fn members(&self) -> Result<BTreeMap<MemberId, MemberRecord>> {
        Ok(self.directory.members().await?.into_iter().map(|member| (member.id.clone(), member)).collect())
    }

    pub async fn activate(&self, name: &str) -> Result<Response> {
        let turn = self.turn(name);
        let _turn = turn.lock().await;
        let me = self.member();
        for _ in 0..4 {
            let Some(record) = self.directory.deployment_by_name(name).await? else {
                return Err(Anomaly::incorrect("rtdb.multi/unknown-deployment", format!("No deployment is named {name}")));
            };
            let members = self.members().await?;
            if let Some(holding) = self.holdings.lock().get_mut(name) {
                if holding.id == record.id && holding.epoch == record.epoch && record.holder.as_ref() == Some(&me) && self.node.catalog().database(name).is_ok() {
                    holding.last_touch = Instant::now();
                    return Ok(Response::Held { epoch: record.epoch, address: self.address.clone() });
                }
            }
            if let Some(holder) = effective_holder(&record, &members, now_ms()) {
                if *holder != me {
                    let address = members.get(holder).map(|member| member.address.clone()).unwrap_or_default();
                    return Ok(Response::Redirect { holder: holder.to_string(), address });
                }
            }
            let Some(next) = claim(&record, &me, &members, now_ms()) else {
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
        let servers = record.ensemble.iter().map(|id| members.get(id).map(|member| member.address.clone())
            .ok_or_else(|| Anomaly::fault("rtdb.multi/unknown-logkeeper", format!("Logkeeper {id} is not a member")))).collect::<Result<Vec<_>>>()?;
        self.stop_running(&record.name).await;
        let configuration = database_config(&record, servers, &self.options.storage, Role::Holder);
        let opened = async {
            let database = self.node.database_or_attach(record.name.clone(), configuration).await?
                .ok_or_else(|| Anomaly::fault("rtdb.multi/not-opened", "The node declined to open the deployment"))?;
            let deadline = Instant::now() + self.options.activation_timeout;
            loop {
                let status = database.status();
                if matches!(status.role, DriverRole::Leading(_)) { return Ok(()); }
                if let Some(failure) = status.promotion_failure { return Err(failure); }
                if matches!(status.role, DriverRole::Stopped) {
                    return Err(database.progress().fault().unwrap_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/open-failed", "The deployment's driver stopped while it was being opened")));
                }
                if self.node.catalog().database(&record.name).is_err() { return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/open-failed", "The deployment stopped while it was being opened")); }
                if Instant::now() > deadline {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/activation-timeout", format!(
                        "The holder did not confirm its leadership in time (role {:?}, highest term {:?}, replica position {:?}, open block {:?})",
                        status.role, status.highest_term, status.replica_position, status.open_block)));
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }.await;
        match opened {
            Ok(()) => {
                self.holdings.lock().insert(record.name.clone(), LocalHolding { id: record.id.clone(), epoch: record.epoch, last_touch: Instant::now() });
                Ok(Response::Held { epoch: record.epoch, address: self.address.clone() })
            }
            Err(error) => {
                self.stop_running(&record.name).await;
                if let Some(parked) = park(&record, &self.member(), record.epoch) { let _ = self.directory.replace_deployment(parked, record.version).await; }
                Err(error)
            }
        }
    }

    /// Stops running `name` on this node, waiting for its resources to drain so the name can be opened again.
    async fn stop_running(&self, name: &str) {
        self.holdings.lock().remove(name);
        if self.node.catalog().database(name).is_ok() { let _ = self.node.detach_local(name.to_owned()).await; }
        while self.node.catalog().is_draining(name) { tokio::time::sleep(Duration::from_millis(5)).await; }
    }

    pub async fn park(&self, name: &str) -> Result<Response> {
        let turn = self.turn(name);
        let _turn = turn.lock().await;
        let Some((id, epoch)) = self.holdings.lock().get(name).map(|holding| (holding.id.clone(), holding.epoch)) else { return Ok(Response::Done); };
        if let Ok(database) = self.node.catalog().database(name) {
            // A park releases the placement only once the flushed block is adopted, so the next holder starts from a closed block rather than a dangling one.
            let open = database.status().open_block;
            database.flush().await?;
            let deadline = Instant::now() + self.options.activation_timeout;
            while database.status().adopted_block.as_ref().is_none_or(|block| block.block_index() < open) {
                if Instant::now() > deadline { return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/park-timeout", "The parking flush did not complete in time")); }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        if let Some(record) = self.directory.deployment(&id).await? {
            if let Some(parked) = park(&record, &self.member(), epoch) { self.directory.replace_deployment(parked, record.version).await?; }
        }
        self.stop_running(name).await;
        Ok(Response::Done)
    }

    pub fn touch(&self, name: &str) {
        if let Some(holding) = self.holdings.lock().get_mut(name) { holding.last_touch = Instant::now(); }
    }

    /// Gives up every deployment the directory no longer places here under the epoch this node opened it at, every one whose database stopped, and every idle one.
    pub async fn reconcile(&self) {
        let draining = self.lease.record().state == rtdb_directory::MemberState::Draining;
        let held: Vec<(String, String, u64, Instant)> = self.holdings.lock().iter().map(|(name, holding)| (name.clone(), holding.id.clone(), holding.epoch, holding.last_touch)).collect();
        let me = self.member();
        for (name, id, epoch, last_touch) in held {
            let current = match self.directory.deployment(&id).await { Ok(current) => current, Err(_) => continue };
            let placed_here = current.as_ref().is_some_and(|record| record.epoch == epoch && record.holder.as_ref() == Some(&me));
            if !placed_here || self.node.catalog().database(&name).is_err() {
                let turn = self.turn(&name);
                let _turn = turn.lock().await;
                self.stop_running(&name).await;
                continue;
            }
            if draining || self.options.park_after.is_some_and(|after| last_touch.elapsed() >= after) { let _ = self.park(&name).await; }
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
                    let names: Vec<String> = self.holdings.lock().keys().cloned().collect();
                    for name in names { self.stop_running(&name).await; }
                    return;
                }
                _ = ticker.tick() => self.reconcile().await,
            }
        }
    }
}

#[async_trait]
impl Handler for NodeAgent {
    async fn handle(&self, request: Request) -> Response {
        let result = match request {
            Request::Activate { name } => self.activate(&name).await,
            Request::Park { name } => self.park(&name).await,
            Request::Touch { name } => { self.touch(&name); Ok(Response::Done) }
        };
        result.unwrap_or_else(|error| Response::refused(&error))
    }
}
