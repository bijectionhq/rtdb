//! Assembling the cell's processes: a node (server, agent, lease, control listener) and a logkeeper (server and lease).

pub(crate) mod standalone;

pub use standalone::{STANDALONE_PROTOCOL, StandaloneSettings, initialize_standalone, serve_standalone};

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rtdb_columnar::MemoryBudget;
use rtdb_directory::{Directory, MemberId, MemberRole};
use rtdb_server::{PlacedOptions, ServerDriver, ServerHandle, config::NodeConfiguration};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::agent::{AgentOptions, NodeAgent};
use crate::deployment::LOGKEEPER_LOG_TYPE;
use crate::lease::{LeaseOptions, MemberLease, Registration};
use crate::logs::LogkeeperLogFactory;

const MAXIMUM_LOG_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

pub struct NodeSettings {
    pub configuration: NodeConfiguration,
    pub allocation_limit_bytes: usize,
    pub control: SocketAddr,
    /// The address peers use when the listeners bind on all interfaces.
    pub advertise_host: Option<String>,
    pub hosted_task: Option<rtdb_directory::HostedTask>,
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
    storage_regions: TaskTracker,
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
    let storage_regions = TaskTracker::new();
    let storage = Arc::new(crate::store::CellStoreFactory {
        directory: Arc::clone(&directory),
        keys: Arc::clone(&settings.keys),
        hosted_task: settings.hosted_task,
        storage_regions: storage_regions.clone(),
    });
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
    let pgwire_endpoint = advertised(pgwire_address, settings.advertise_host.as_deref())?;
    let control_endpoint = advertised(control_address, settings.advertise_host.as_deref())?;
    let lease = MemberLease::register(Arc::clone(&directory), Registration {
        role: MemberRole::Node, address: pgwire_endpoint.clone(), control_address: Some(control_endpoint), availability_zone: settings.availability_zone, pool: settings.pool,
    }, settings.lease).await?;
    fragmenter.set_member(lease.id());
    let agent = NodeAgent::new(directory, server.node().clone(), Arc::clone(&lease), pgwire_endpoint, settings.agent, writers, settings.capabilities.clone());
    let _ = fence.0.set(Arc::clone(&agent));
    let cancellation = CancellationToken::new();
    let lease_cancellation = CancellationToken::new();
    let mut tasks = JoinSet::new();
    tasks.spawn(driver.run());
    { let lease = Arc::clone(&lease); let cancellation = lease_cancellation.clone(); tasks.spawn(async move { lease.run(cancellation).await; Ok(()) }); }
    { let agent = Arc::clone(&agent); let cancellation = cancellation.clone(); tasks.spawn(async move { agent.run(cancellation).await; Ok(()) }); }
    tasks.spawn(crate::control::serve(control, agent.clone(), crate::control::ControlKey(settings.capabilities.clone()), cancellation.clone()));
    Ok(RunningNode { server, agent, fragmenter, lease, control_address, pgwire_address, cancellation, lease_cancellation, storage_regions, tasks })
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
        let mut failure = None;
        while let Some(result) = self.tasks.join_next().await {
            if let Ok(Err(error)) = result && error.category != AnomalyCategory::Interrupted {
                failure.get_or_insert(error);
            }
        }
        self.storage_regions.close();
        if tokio::time::timeout(Duration::from_secs(10), self.storage_regions.wait()).await.is_err() {
            failure.get_or_insert_with(|| Anomaly::new(
                AnomalyCategory::Unavailable,
                "rtdb.multi/storage-release-pending",
                "The directory has not confirmed all serving storage region releases",
            ));
        }
        failure.map_or(Ok(()), Err)
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

    async fn confirm_hosted(&self, database: &rtdb_node::Database) -> Result<String> {
        let agent = self.0.get().ok_or_else(|| Anomaly::new(
            AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, "This node is not holding a hosted database",
        ))?;
        agent.confirm_hosted_read(database).await
    }
}

pub struct LogkeeperSettings {
    pub listen: SocketAddr,
    /// Stable DNS name of the disk, so ensembles can still reach it after a process restart.
    pub advertise_host: Option<String>,
    pub data_dir: PathBuf,
    pub fsync: bool,
    pub cache_bytes: usize,
    pub availability_zone: String,
    pub lease: LeaseOptions,
}

pub struct RunningLogkeeper {
    pub address: SocketAddr,
    pub lease: Arc<MemberLease>,
    cancellation: CancellationToken,
    tasks: JoinSet<Result<()>>,
}

pub async fn start_logkeeper(directory: Arc<dyn Directory>, settings: LogkeeperSettings) -> Result<RunningLogkeeper> {
    let listener = TcpListener::bind(settings.listen).await.map_err(io)?;
    let address = listener.local_addr().map_err(io)?;
    let endpoint = advertised(address, settings.advertise_host.as_deref())?;
    let (journal, identity) = rtdb_logkeeper::PreparedJournal::open_for_member(rtdb_logkeeper::ServerOptions {
        data_dir: settings.data_dir,
        fsync: settings.fsync,
        cache_bytes: settings.cache_bytes,
    }, MemberId::fresh().0).await?;
    if !uuid::Uuid::parse_str(&identity).is_ok_and(|id| !id.is_nil() && id.to_string() == identity) {
        return Err(Anomaly::incorrect("rtdb.multi/logkeeper-identity", "The retained journal member ID is invalid"));
    }
    let lease = MemberLease::reclaim_logkeeper(directory, Registration {
        role: MemberRole::Logkeeper, address: endpoint, control_address: None,
        availability_zone: settings.availability_zone, pool: Default::default(),
    }, MemberId(identity), settings.advertise_host.is_some(), settings.lease).await?;
    let cancellation = CancellationToken::new();
    let mut tasks = JoinSet::new();
    tasks.spawn(journal.serve(listener, cancellation.clone()));
    { let lease = Arc::clone(&lease); let cancellation = cancellation.clone(); tasks.spawn(async move { lease.run(cancellation).await; Ok(()) }); }
    Ok(RunningLogkeeper { address, lease, cancellation, tasks })
}

impl RunningLogkeeper {
    pub async fn run_until(mut self, shutdown: CancellationToken) -> Result<()> {
        let failure = tokio::select! {
            _ = shutdown.cancelled() => None,
            exited = self.tasks.join_next() => Some(match exited {
                Some(Ok(Err(error))) => error,
                Some(Err(error)) => Anomaly::fault("rtdb.multi/logkeeper-task", error.to_string()),
                Some(Ok(Ok(()))) | None => Anomaly::fault("rtdb.multi/logkeeper-stopped", "Logkeeper terminated before shutdown"),
            }),
        };
        let stopped = self.stop().await;
        failure.map_or(stopped, Err)
    }

    pub async fn stop(mut self) -> Result<()> {
        self.cancellation.cancel();
        let mut failure = None;
        while let Some(result) = self.tasks.join_next().await {
            if let Ok(Err(error)) = result { failure.get_or_insert(error); }
        }
        let retired = self.lease.retire().await;
        failure.map_or(retired, Err)
    }
}

fn advertised(bound: SocketAddr, host: Option<&str>) -> Result<String> {
    let invalid = || Anomaly::incorrect("rtdb.multi/advertise", "A wildcard listener needs a routable advertised IP or DNS name");
    match host {
        None if !bound.ip().is_unspecified() => Ok(bound.to_string()),
        Some(host) => {
            if let Ok(address) = host.parse::<std::net::IpAddr>() {
                if address.is_unspecified() { return Err(invalid()); }
                return Ok(SocketAddr::new(address, bound.port()).to_string());
            }
            if !host.is_empty() && host.len() <= 253 && host.split('.').all(|part| {
                !part.is_empty() && part.len() <= 63 && !part.starts_with('-') && !part.ends_with('-')
                    && part.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            }) {
                return Ok(format!("{}:{}", host.to_ascii_lowercase(), bound.port()));
            }
            Err(invalid())
        }
        None => Err(invalid()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_locked_journal_is_never_advertised_as_a_second_logkeeper() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let data = tempfile::tempdir().unwrap();
        let settings = || LogkeeperSettings {
            listen: "127.0.0.1:0".parse().unwrap(),
            advertise_host: None,
            data_dir: data.path().to_owned(),
            fsync: true,
            cache_bytes: 1 << 20,
            availability_zone: "local".into(),
            lease: LeaseOptions::default(),
        };
        let original = start_logkeeper(directory.clone(), settings()).await.unwrap();
        let id = original.lease.id();
        let installed = crate::admin::install_deployment(directory.as_ref(), uuid::Uuid::new_v4(), "test", "local", 1).await.unwrap();
        assert_eq!(installed.ensemble, vec![id.clone()]);
        assert!(start_logkeeper(directory.clone(), settings()).await.is_err());
        assert_eq!(directory.members().await.unwrap().len(), 1);
        original.stop().await.unwrap();
        let replacement = start_logkeeper(directory.clone(), settings()).await.unwrap();
        assert_eq!(replacement.lease.id(), id);
        let record = directory.member(&id).await.unwrap().unwrap();
        assert!(record.is_live(rtdb_directory::now_ms()));
        assert_eq!(record.address, replacement.address.to_string());
        assert_eq!(directory.members().await.unwrap().len(), 1);
        replacement.stop().await.unwrap();
    }

    #[tokio::test]
    async fn a_different_journal_cannot_take_an_existing_logkeeper_address() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let data = tempfile::tempdir().unwrap();
        let empty = tempfile::tempdir().unwrap();
        let settings = |path: &std::path::Path, listen| LogkeeperSettings {
            listen,
            advertise_host: Some("keeper.test.internal".into()),
            data_dir: path.to_owned(),
            fsync: true,
            cache_bytes: 1 << 20,
            availability_zone: "test".into(),
            lease: LeaseOptions::default(),
        };
        let running = start_logkeeper(directory.clone(), settings(data.path(), "127.0.0.1:0".parse().unwrap())).await.unwrap();
        let listen = running.address;
        let id = running.lease.id();
        running.stop().await.unwrap();

        assert!(start_logkeeper(directory.clone(), settings(empty.path(), listen)).await.is_err());
        assert_eq!(directory.members().await.unwrap().len(), 1);
        let restarted = start_logkeeper(directory.clone(), settings(data.path(), listen)).await.unwrap();
        assert_eq!(restarted.lease.id(), id);
        restarted.stop().await.unwrap();
    }

    #[test]
    fn wildcard_listeners_advertise_a_reachable_address_and_bound_port() {
        let bound = "0.0.0.0:5432".parse().unwrap();
        assert!(advertised(bound, None).is_err());
        assert!(advertised(bound, Some("0.0.0.0")).is_err());
        assert_eq!(advertised(bound, Some("keeper.cell.internal")).unwrap(), "keeper.cell.internal:5432");
        assert_eq!(advertised(bound, Some("fd00::7")).unwrap(), "[fd00::7]:5432");
    }
}

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/listener", error.to_string()) }
