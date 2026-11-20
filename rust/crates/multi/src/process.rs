//! Assembling the cell's processes: a node (server, agent, lease, control listener) and a logkeeper (server and lease).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rtdb_columnar::MemoryBudget;
use rtdb_directory::{Directory, MemberRole};
use rtdb_server::{PlacedOptions, ServerDriver, ServerHandle, config::NodeConfiguration};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::agent::{AgentOptions, NodeAgent};
use crate::deployment::LOGKEEPER_LOG_TYPE;
use crate::lease::{LeaseOptions, MemberLease, Registration};
use crate::logs::LogkeeperLogFactory;

const MAXIMUM_LOG_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

pub struct NodeSettings {
    pub configuration: NodeConfiguration,
    pub allocation_limit_bytes: usize,
    pub control: SocketAddr,
    pub availability_zone: String,
    pub agent: AgentOptions,
    pub lease: LeaseOptions,
    pub pool: rtdb_directory::MemberPool,
    pub claim_wait: Duration,
    pub keys: Arc<dyn crate::keys::KeyService>,
    /// Where this node keeps its cache of the cell's objects.
    pub cache_dir: PathBuf,
    /// Whether this node's holders compact their own deployments; off when the cell has compaction workers.
    pub local_compaction: bool,
    /// The cell's secret, so the node accepts the capabilities its gateways mint in place of a password.
    pub capabilities: Option<Arc<crate::capability::CellSecret>>,
}

/// A running node. Dropping it does not stop it; call `shutdown` or `kill`.
pub struct RunningNode {
    pub server: ServerHandle,
    pub agent: Arc<NodeAgent>,
    pub fragmenter: Arc<crate::fragments::CellFragmenter>,
    pub lease: Arc<MemberLease>,
    pub control_address: SocketAddr,
    pub pgwire_address: SocketAddr,
    cancellation: CancellationToken,
    lease_cancellation: CancellationToken,
    tasks: JoinSet<Result<()>>,
}

pub async fn start_node(directory: Arc<dyn Directory>, mut settings: NodeSettings) -> Result<RunningNode> {
    if settings.configuration.disk_cache.is_none() {
        settings.configuration.disk_cache = Some(rtdb_server::config::DiskCacheSettings { path: settings.cache_dir.clone(), maximum_bytes: Some(1 << 30), maximum_ratio: 0.5 });
    }
    if !settings.local_compaction && settings.pool != rtdb_directory::MemberPool::Compaction { settings.configuration.compactor.threads = 0; }
    settings.agent.compaction_worker = settings.pool == rtdb_directory::MemberPool::Compaction;
    let options = settings.configuration.node_options(settings.allocation_limit_bytes)?;
    let memory = MemoryBudget::new(options.execution_memory_bytes);
    let writers: crate::logs::Writers = Default::default();
    let fence = Arc::new(FenceSlot::default());
    let recorder = Arc::new(crate::logs::DirectoryRecorder { directory: Arc::clone(&directory) });
    let cache_bytes = settings.configuration.disk_cache.as_ref().and_then(|cache| cache.maximum_bytes).unwrap_or(u64::MAX);
    let fragmenter = crate::fragments::CellFragmenter::new(Arc::clone(&directory), &settings.agent.fragments, cache_bytes, settings.capabilities.clone());
    let storage = Arc::new(crate::store::CellStoreFactory { directory: Arc::clone(&directory), keys: Arc::clone(&settings.keys), fence_interval: Duration::from_millis(100) });
    let placed = PlacedOptions {
        log_factories: vec![(LOGKEEPER_LOG_TYPE.into(), LogkeeperLogFactory::new(MAXIMUM_LOG_MESSAGE_BYTES, Arc::clone(&settings.keys), Arc::clone(&writers), recorder))],
        storage_factories: vec![(crate::store::CELL_STORAGE_TYPE.into(), storage)],
        claim_wait: settings.claim_wait,
        capabilities: settings.capabilities.clone().map(|secret| secret as Arc<dyn rtdb_pgwire::CapabilityVerifier>),
        read_fence: Some(fence.clone() as Arc<dyn rtdb_node::ReadFence>),
        scan_fragmenter: Some(fragmenter.clone() as Arc<dyn rtdb_query::ScanFragmenter>),
        compaction_relay: match (settings.pool == rtdb_directory::MemberPool::Compaction, &settings.capabilities) {
            (true, Some(secret)) => Some(Arc::new(crate::relay::HolderRelay { directory: Arc::clone(&directory), secret: Arc::clone(secret) }) as Arc<dyn rtdb_node::CompactionRelay>),
            (true, None) => return Err(Anomaly::incorrect("rtdb.multi/no-cell-secret", "A compaction worker publishes through holders, which needs the cell's secret")),
            (false, _) => None,
        },
    };
    let (server, driver) = ServerDriver::open_placed(settings.configuration, settings.allocation_limit_bytes, memory, placed).await?;
    let pgwire_address = server.addresses().pgwire.ok_or_else(|| Anomaly::incorrect("rtdb.multi/no-pgwire", "A cell node needs a pgwire listener"))?;
    let control = TcpListener::bind(settings.control).await.map_err(io)?;
    let control_address = control.local_addr().map_err(io)?;
    let lease = MemberLease::register(Arc::clone(&directory), Registration {
        role: MemberRole::Node, address: pgwire_address.to_string(), control_address: Some(control_address.to_string()), availability_zone: settings.availability_zone, pool: settings.pool,
    }, settings.lease).await?;
    fragmenter.set_member(lease.id());
    let agent = NodeAgent::new(directory, server.node().clone(), Arc::clone(&lease), pgwire_address.to_string(), settings.agent, writers, settings.capabilities.clone());
    let _ = fence.0.set(Arc::clone(&agent));
    let cancellation = CancellationToken::new();
    let lease_cancellation = CancellationToken::new();
    let mut tasks = JoinSet::new();
    tasks.spawn(driver.run());
    { let lease = Arc::clone(&lease); let cancellation = lease_cancellation.clone(); tasks.spawn(async move { lease.run(cancellation).await; Ok(()) }); }
    { let agent = Arc::clone(&agent); let cancellation = cancellation.clone(); tasks.spawn(async move { agent.run(cancellation).await; Ok(()) }); }
    tasks.spawn(crate::control::serve(control, agent.clone(), crate::control::ControlKey(settings.capabilities.clone()), cancellation.clone()));
    Ok(RunningNode { server, agent, fragmenter, lease, control_address, pgwire_address, cancellation, lease_cancellation, tasks })
}

impl RunningNode {
    /// Parks every deployment it holds, leaves the cell and stops.
    pub async fn shutdown(mut self) -> Result<()> {
        for name in self.agent.held().into_keys() { let _ = self.agent.park(&name).await; }
        let _ = self.lease.retire().await;
        self.stop().await
    }

    /// Stops renewing the lease and stops serving, without parking or leaving: to the cell, this node has died.
    pub async fn kill(mut self) -> Result<()> {
        self.stop().await
    }

    /// Stops renewing the lease while the node keeps running: a partitioned member whose placements others will take over.
    pub fn stop_renewing(&self) { self.lease_cancellation.cancel(); }

    async fn stop(&mut self) -> Result<()> {
        self.lease_cancellation.cancel();
        self.cancellation.cancel();
        self.server.shutdown();
        while let Some(result) = self.tasks.join_next().await {
            if let Ok(Err(error)) = result { if error.category != AnomalyCategory::Interrupted { return Err(error); } }
        }
        Ok(())
    }
}

/// The read fence, filled once the agent that answers it exists.
#[derive(Default)]
struct FenceSlot(std::sync::OnceLock<Arc<NodeAgent>>);

#[async_trait::async_trait]
impl rtdb_node::ReadFence for FenceSlot {
    async fn confirm(&self, database: &str) -> Result<()> {
        match self.0.get() { Some(agent) => agent.confirm_read(database).await, None => Ok(()) }
    }
}

pub struct LogkeeperSettings { pub listen: SocketAddr, pub data_dir: PathBuf, pub fsync: bool, pub cache_bytes: usize, pub availability_zone: String, pub lease: LeaseOptions }

pub struct RunningLogkeeper {
    pub address: SocketAddr,
    pub lease: Arc<MemberLease>,
    cancellation: CancellationToken,
    tasks: JoinSet<Result<()>>,
}

pub async fn start_logkeeper(directory: Arc<dyn Directory>, settings: LogkeeperSettings) -> Result<RunningLogkeeper> {
    let listener = TcpListener::bind(settings.listen).await.map_err(io)?;
    let address = listener.local_addr().map_err(io)?;
    let lease = MemberLease::register(directory, Registration { role: MemberRole::Logkeeper, address: address.to_string(), control_address: None, availability_zone: settings.availability_zone, pool: Default::default() }, settings.lease).await?;
    let cancellation = CancellationToken::new();
    let mut tasks = JoinSet::new();
    tasks.spawn(rtdb_logkeeper::serve(listener, rtdb_logkeeper::ServerOptions { data_dir: settings.data_dir, fsync: settings.fsync, cache_bytes: settings.cache_bytes }, cancellation.clone()));
    { let lease = Arc::clone(&lease); let cancellation = cancellation.clone(); tasks.spawn(async move { lease.run(cancellation).await; Ok(()) }); }
    Ok(RunningLogkeeper { address, lease, cancellation, tasks })
}

impl RunningLogkeeper {
    pub async fn stop(mut self) -> Result<()> {
        self.cancellation.cancel();
        while let Some(result) = self.tasks.join_next().await { if let Ok(Err(error)) = result { return Err(error); } }
        Ok(())
    }
}

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/listener", error.to_string()) }
