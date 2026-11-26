//! Assembling the cell's processes: a node (server, agent, lease, control listener) and a logkeeper (server and lease).

mod owned_worlds;
pub(crate) mod standalone;

pub use standalone::{STANDALONE_PROTOCOL, StandaloneSettings, initialize_standalone, serve_standalone};

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rtdb_columnar::MemoryBudget;
use rtdb_directory::{Directory, MemberId, MemberRole};
use rtdb_node::NodeOptions;
use rtdb_server::{PlacedOptions, ServerDriver, ServerHandle, config::NodeConfiguration};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::{CancellationToken, DropGuard};
use tokio_util::task::TaskTracker;

use crate::agent::{AgentOptions, NodeAgent};
use crate::deployment::LOGKEEPER_LOG_TYPE;
use crate::lease::{LeaseOptions, MemberLease, Registration};
use crate::logs::LogkeeperLogFactory;

pub(crate) const MAXIMUM_LOG_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
const PLACED_FLUSH_INTERVAL_LIMIT: Duration = Duration::from_secs(15 * 60);
const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");
const NODE_DRAIN_TIMEOUT: Duration = Duration::from_secs(45);
const NODE_RETIRE_TIMEOUT: Duration = Duration::from_secs(5);

pub struct NodeSettings {
    pub configuration: NodeConfiguration,
    pub allocation_limit_bytes: usize,
    pub control: SocketAddr,
    /// The address peers use when the listeners bind on all interfaces.
    pub advertise_host: Option<String>,
    pub hosted_task: Option<rtdb_directory::HostedTask>,
    pub availability_zone: String,
    pub engine_version: Option<String>,
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
    pub classic_retained_flight_key: Option<PathBuf>,
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
    // Dropping the JoinSet aborts the async tasks but cannot abort the receiver clock's blocking thread, which would then hold the caller's runtime shutdown forever.
    _stop_clock_on_drop: DropGuard,
}

pub async fn start_node(directory: Arc<dyn Directory>, mut settings: NodeSettings) -> Result<RunningNode> {
    if (settings.agent.installed_journal_manifest.is_some() || settings.agent.installed_cell.is_some()) && settings.capabilities.is_none() {
        return Err(Anomaly::incorrect("rtdb.multi/ordered-worker", "Installed journal workers require a cell route credential"));
    }
    let ordered = Arc::new(match (&settings.agent.installed_journal_manifest, &settings.agent.installed_cell) {
        (Some(_), Some(_)) => return Err(Anomaly::incorrect("rtdb.multi/installed-cell", "Choose one installed journal configuration")),
        (None, Some(cell)) => crate::journal::hosted::Routes::runtime(
            Arc::clone(cell), settings.agent.storage.clone(),
        )?,
        (Some(path), None) => crate::journal::hosted::Routes::read(path, &settings.agent.storage, settings.agent.replay_broker.as_ref())?,
        (None, None) => crate::journal::hosted::Routes::default(),
    });
    ordered.reserve_deployments(directory.as_ref()).await?;
    let audience = settings.configuration.flight_sql.as_ref().and_then(|flight| flight.audience);
    if settings.agent.audience_journal_port == Some(0)
        || audience.is_none() && settings.agent.audience_journal_port.is_some()
        || audience.is_some() && settings.agent.audience_journal_port.is_none() && settings.agent.installed_journal_manifest.is_none()
            && settings.agent.installed_cell.is_none()
    {
        return Err(Anomaly::incorrect("rtdb.multi/audience-authority",
            "A classic audience Flight listener requires its read-only journal port"));
    }
    if audience.is_some() {
        if settings.agent.audience_journal_port.is_some() {
            directory.require_audience_reads()?;
        } else {
            directory.require_installed_audience_seats()?;
        }
    }
    if let Some(archive) = &settings.agent.world_archive {
        crate::archive::require_independent_world_archive(&settings.agent.storage, archive)?;
    }
    let engine_version = settings.engine_version.take().unwrap_or_else(|| ENGINE_VERSION.into());
    crate::admin::validate_engine_version(&engine_version)?;
    let scoped_writes = match (&settings.agent.s3_write_role, &settings.agent.storage) {
        (Some(role), crate::deployment::CellStorage::S3 { config }) => {
            let issuer = Box::pin(rtdb_s3::ScopedS3Writes::new(&role.arn, config.region.as_deref(), role.sts_endpoint.as_deref(),
                role.require_conditional_put)).await?;
            Some(Arc::new(crate::store::ScopedCellWrites::new(issuer)))
        }
        (Some(_), _) => return Err(Anomaly::incorrect("rtdb.multi/scoped-storage", "A scoped S3 write role requires S3 cell storage")),
        (None, _) => None,
    };
    let bucket = Box::pin(settings.agent.storage.bucket(rtdb_storage::IoRegion::new())).await?;
    let storage_namespaces = if scoped_writes.is_some() {
        let objects = crate::deployment::existing_storage_namespace(bucket.as_ref()).await?
            .ok_or_else(|| Anomaly::incorrect("rtdb.multi/storage-namespace-missing", "Initialize the cell object namespace before starting a scoped writer"))?;
        rtdb_directory::StorageNamespaces { objects, keys: settings.keys.namespace_id().await? }
    } else {
        crate::deployment::storage_namespaces(bucket.as_ref(), settings.keys.as_ref()).await?
    };
    if settings.configuration.disk_cache.is_none() {
        settings.configuration.disk_cache = Some(rtdb_server::config::DiskCacheSettings { path: settings.cache_dir.clone(), maximum_bytes: Some(1 << 30), maximum_ratio: 0.5 });
    }
    if !settings.local_compaction && settings.pool != rtdb_directory::MemberPool::Compaction { settings.configuration.compactor.threads = 0; }
    settings.agent.compaction_worker = settings.pool == rtdb_directory::MemberPool::Compaction;
    settings.configuration.indexer.flush_duration = settings.configuration.indexer.flush_duration.min(PLACED_FLUSH_INTERVAL_LIMIT);
    let options = settings.configuration.node_options(settings.allocation_limit_bytes)?;
    let memory = MemoryBudget::new(options.execution_memory_bytes);
    let writers: crate::logs::Writers = Default::default();
    let fence = Arc::new(FenceSlot { agent: Default::default(), hosted_task: settings.hosted_task.clone() });
    let cache_bytes = settings.configuration.disk_cache.as_ref().and_then(|cache| cache.maximum_bytes).unwrap_or(u64::MAX);
    let fragmenter = crate::fragments::CellFragmenter::new(Arc::clone(&directory), storage_namespaces.objects, &settings.agent.fragments, cache_bytes, settings.capabilities.clone());
    let storage_regions = TaskTracker::new();
    let local_witnesses = matches!(&settings.agent.storage, crate::deployment::CellStorage::Local { .. })
        .then(|| crate::store::LocalWitnessPool::new(Arc::clone(&settings.keys), &storage_regions));
    let holds = Arc::new(crate::recovery_holds::RecoveryHolds {
        directory: Arc::clone(&directory),
        keys: Arc::clone(&settings.keys),
        broker: settings.agent.replay_broker.clone(),
        custody: if local_witnesses.is_some() { rtdb_directory::ReplayCustody::Local }
            else { rtdb_directory::ReplayCustody::Hosted { owner: settings.hosted_task.clone() } },
        tasks: storage_regions.clone(),
    });
    let owned_archive = crate::archive::WorldArchive {
        directory: Arc::clone(&directory),
        storage: settings.agent.world_archive.clone().unwrap_or_else(|| settings.agent.storage.clone()),
        keys: Arc::clone(&settings.keys),
        hosted_task: settings.hosted_task.clone(),
        options: NodeOptions { owned_relations: true, ..options.clone() },
    };
    let flight_owned_worlds = settings.configuration.flight_sql.as_ref()
        .filter(|flight| flight.port >= 0 && flight.owned_native_actions)
        .map(|_| {
            Arc::new(owned_worlds::PlacedWorldHost {
                directory: Arc::clone(&directory),
                agent: Arc::clone(&fence),
                capture_source: settings.agent.world_archive.as_ref().map(|_| settings.agent.storage.clone()),
                archive: owned_archive.storage.clone(),
                keys: Arc::clone(&settings.keys),
                hosted_task: settings.hosted_task.clone(),
                options: owned_archive.options.clone(),
                cell_secret: settings.capabilities.clone(),
                family_vault_namespace: storage_namespaces.keys,
            }) as Arc<dyn rtdb_flight_sql::OwnedWorldHost>
        });
    let storage = Arc::new(crate::store::CellStoreFactory {
        directory: Arc::clone(&directory),
        keys: Arc::clone(&settings.keys),
        storage_namespaces,
        scoped_writes: scoped_writes.clone(),
        hosted_task: settings.hosted_task,
        audience_source: audience.map(|(_, profile)| profile.source()),
        storage_regions: storage_regions.clone(),
        local_witnesses,
        replay_broker: settings.agent.replay_broker.clone(),
    });
    let receiver_clock = crate::receiver_clock::ReceiverClock::new(Arc::clone(&directory), storage_namespaces.objects);
    let decision_clock: Arc<dyn rtdb_node::DecisionClock> = receiver_clock.clone();
    let publish_clock = settings.pool == rtdb_directory::MemberPool::General;
    let placed = PlacedOptions {
        decision_clock: Some(decision_clock),
        log_factories: vec![
            (LOGKEEPER_LOG_TYPE.into(), LogkeeperLogFactory::new(MAXIMUM_LOG_MESSAGE_BYTES, Arc::clone(&writers), holds)),
            (crate::journal::hosted::LOG_TYPE.into(), Arc::new(crate::journal::hosted::Logs {
                routes: Arc::clone(&ordered), keys: Arc::clone(&settings.keys), releases: storage_regions.clone(),
            })),
        ],
        storage_factories: vec![
            (crate::store::CELL_STORAGE_TYPE.into(), storage),
            (crate::journal::hosted::STORE_TYPE.into(), Arc::new(crate::journal::hosted::StoreFactory {
                routes: Arc::clone(&ordered), storage: settings.agent.storage.clone(),
                keys: Arc::clone(&settings.keys), scoped_writes: scoped_writes.clone(),
                releases: storage_regions.clone(),
            })),
        ],
        claim_wait: settings.claim_wait,
        capabilities: settings.capabilities.clone().map(|secret| secret as Arc<dyn rtdb_pgwire::CapabilityVerifier>),
        flight_peer: settings.capabilities.clone().map(|secret| secret as Arc<dyn rtdb_flight_sql::FlightPeerVerifier>),
        classic_flight_exclusion: Some(Arc::new(crate::classic_flight::ReceiverExclusion::new(
            Arc::clone(&directory), settings.classic_retained_flight_key.as_deref(),
        )?)),
        flight_owned_worlds,
        world_constraints: Some(Arc::new(owned_archive)),
        read_fence: Some(fence.clone() as Arc<dyn rtdb_node::ReadFence>),
        scan_fragmenter: Some(fragmenter.clone() as Arc<dyn rtdb_query::ScanFragmenter>),
        compaction_relay: match (settings.pool == rtdb_directory::MemberPool::Compaction, &settings.capabilities) {
            (true, Some(secret)) => Some(Arc::new(crate::relay::HolderRelay { directory: Arc::clone(&directory), secret: Arc::clone(secret) }) as Arc<dyn rtdb_node::CompactionRelay>),
            (true, None) => return Err(Anomaly::incorrect("rtdb.multi/no-cell-secret", "A compaction worker publishes through holders, which needs the cell's secret")),
            (false, _) => None,
        },
    };
    let relay_limit = settings.configuration.server.as_ref()
        .filter(|_| settings.capabilities.is_some() && !settings.configuration.auth.uses_remote_address())
        .map(|server| server.connections);
    let (server, driver) = Box::pin(ServerDriver::open_placed(settings.configuration, settings.allocation_limit_bytes, memory, placed)).await?;
    let pgwire = server.primary_pgwire().ok_or_else(|| Anomaly::incorrect("rtdb.multi/no-pgwire", "A cell node needs a pgwire listener"))?;
    let pgwire_address = pgwire.address();
    let control = TcpListener::bind(settings.control).await.map_err(io)?;
    let control_address = control.local_addr().map_err(io)?;
    let pgwire_endpoint = advertised(pgwire_address, settings.advertise_host.as_deref())?;
    let control_endpoint = advertised(control_address, settings.advertise_host.as_deref())?;
    let flight_endpoint = server.addresses().flight_sql
        .map(|address| advertised(address, settings.advertise_host.as_deref())).transpose()?;
    let lease = MemberLease::register(Arc::clone(&directory), Registration {
        role: MemberRole::Node, address: pgwire_endpoint.clone(), control_address: Some(control_endpoint), availability_zone: settings.availability_zone, pool: settings.pool, engine_version, storage_namespaces: Some(storage_namespaces),
    }, settings.lease).await?;
    if let Some(writes) = scoped_writes { writes.set_member(lease.id()); }
    fragmenter.set_member(lease.id());
    let agent = NodeAgent::new(directory, Arc::clone(&settings.keys), server.node().clone(), Arc::clone(&lease), pgwire_endpoint, flight_endpoint, settings.agent, storage_namespaces, writers, ordered, storage_regions.clone(), settings.capabilities.clone());
    let _ = fence.agent.set(Arc::downgrade(&agent));
    let cancellation = CancellationToken::new();
    let lease_cancellation = CancellationToken::new();
    let mut tasks = JoinSet::new();
    tasks.spawn(driver.run());
    if publish_clock {
        let cancellation = lease_cancellation.clone();
        tasks.spawn_blocking(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().map_err(io)?;
            runtime.block_on(receiver_clock.publish(cancellation));
            Ok(())
        });
    }
    { let lease = Arc::clone(&lease); let cancellation = lease_cancellation.clone(); tasks.spawn(async move { lease.run(cancellation).await; Ok(()) }); }
    { let agent = Arc::clone(&agent); let cancellation = cancellation.clone(); tasks.spawn(async move { agent.run(cancellation).await; Ok(()) }); }
    tasks.spawn(crate::control::serve_with_pgwire(
        control, agent.clone(), crate::control::ControlKey(settings.capabilities.clone()),
        cancellation.clone(), pgwire_address, relay_limit, pgwire.watch_status(),
    ));
    let _stop_clock_on_drop = lease_cancellation.clone().drop_guard();
    Ok(RunningNode { server, agent, fragmenter, lease, control_address, pgwire_address, cancellation, lease_cancellation, storage_regions, tasks, _stop_clock_on_drop })
}

impl RunningNode {
    /// Withdraws from placement, parks its holdings within a drain budget, leaves the cell and stops.
    pub async fn shutdown(mut self) -> Result<()> {
        let _ = tokio::time::timeout(NODE_DRAIN_TIMEOUT, async {
            self.lease.begin_drain().await?;
            self.agent.park_for_shutdown().await;
            Ok::<(), Anomaly>(())
        }).await;
        self.lease_cancellation.cancel();
        let _ = tokio::time::timeout(NODE_RETIRE_TIMEOUT, self.lease.retire()).await;
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
        self.agent.drop_subscriptions();
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
struct FenceSlot {
    agent: std::sync::OnceLock<std::sync::Weak<NodeAgent>>,
    hosted_task: Option<rtdb_directory::HostedTask>,
}

#[async_trait::async_trait]
impl rtdb_node::ReadFence for FenceSlot {
    fn hold_active_call(&self, database: &rtdb_node::Database) -> Option<Box<dyn Send + Sync>> {
        self.agent.get()?.upgrade()?.hold_active_call(database)
    }

    async fn hold_transaction_custody(&self, database: &rtdb_node::Database) -> Result<Option<Arc<dyn Send + Sync>>> {
        let agent = self.agent.get().and_then(std::sync::Weak::upgrade).ok_or_else(stopped_agent)?;
        agent.hold_transaction_custody(database).await
    }

    async fn confirm(&self, database: &str) -> Result<()> {
        match self.agent.get() {
            Some(agent) => agent.upgrade().ok_or_else(stopped_agent)?.confirm_read(database).await,
            None => Ok(()),
        }
    }

    async fn confirm_hosted(&self, database: &rtdb_node::Database) -> Result<String> {
        let agent = self.agent.get().and_then(std::sync::Weak::upgrade).ok_or_else(stopped_agent)?;
        agent.confirm_hosted_read(database).await
    }

    async fn confirm_audience(&self, database: &rtdb_node::Database, require_quorum: bool) -> Result<String> {
        let agent = self.agent.get().and_then(std::sync::Weak::upgrade).ok_or_else(stopped_agent)?;
        agent.confirm_audience_read(database, require_quorum).await
    }

    async fn confirm_held_audience(&self, database: &rtdb_node::Database) -> Result<String> {
        let agent = self.agent.get().and_then(std::sync::Weak::upgrade).ok_or_else(stopped_agent)?;
        agent.confirm_held_audience_read(database).await
    }

    async fn hold_audience_seat(&self, database: std::sync::Weak<rtdb_node::Database>, index: usize,
        ready: tokio::sync::oneshot::Sender<Result<()>>, release: CancellationToken) {
        let Some(agent) = self.agent.get().and_then(std::sync::Weak::upgrade) else {
            let _ = ready.send(Err(stopped_agent()));
            return;
        };
        let prepared = agent.prepare_audience_seat(database, index, self.hosted_task.as_ref()).await;
        drop(agent);
        let claim = match prepared {
            Ok(Some(claim)) if !release.is_cancelled() => claim,
            Ok(Some(_)) => return,
            Ok(None) => {
                let _ = ready.send(Err(audience_seat_unavailable()));
                return;
            }
            Err(error) => {
                let _ = ready.send(Err(error));
                return;
            }
        };
        let acquired = claim.acquire().await.and_then(|owned| owned.then_some(()).ok_or_else(audience_seat_unavailable));
        let owns_seat = acquired.is_ok();
        if ready.send(acquired).is_ok() && owns_seat {
            release.cancelled().await;
        }
        // The write may have committed after losing its reply. Fence the
        // claim and its predecessor before returning the local permit.
        let claim = &claim;
        crate::hold_release::release_hold(format!("audience seat {index}"), move || claim.release()).await;
    }
}

fn audience_seat_unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/audience-seat-unavailable",
        "The audience seat is not available at this placement")
}

fn stopped_agent() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, rtdb_node::NOT_HELD, "The placement agent is unavailable")
}

pub struct LogkeeperSettings {
    pub listen: SocketAddr,
    pub authority: Option<(SocketAddr, rtdb_flight_sql::AudienceFlightConfig)>,
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
    let authority = if let Some((listen, profile)) = settings.authority {
        if !settings.fsync {
            return Err(Anomaly::incorrect("rtdb.multi/audience-authority", "The audience journal requires fsync"));
        }
        let socket = TcpListener::bind(listen).await.map_err(io)?;
        Some(rtdb_logkeeper::AuthorityListener { socket, source: profile.source(), connections: profile.permits() })
    } else { None };
    let (journal, identity) = rtdb_logkeeper::PreparedJournal::open_for_member(rtdb_logkeeper::ServerOptions::new(
        settings.data_dir, settings.fsync, settings.cache_bytes,
    )).await?;
    if !uuid::Uuid::parse_str(&identity).is_ok_and(|id| !id.is_nil() && id.to_string() == identity) {
        return Err(Anomaly::incorrect("rtdb.multi/logkeeper-identity", "The retained journal member ID is invalid"));
    }
    let lease = MemberLease::reclaim_logkeeper(directory, Registration {
        role: MemberRole::Logkeeper, address: endpoint, control_address: None,
        availability_zone: settings.availability_zone, engine_version: ENGINE_VERSION.into(), pool: Default::default(), storage_namespaces: None,
    }, MemberId(identity), settings.advertise_host.is_some(), settings.lease).await?;
    let cancellation = CancellationToken::new();
    let mut tasks = JoinSet::new();
    tasks.spawn(journal.serve_with_authority(listener, authority, cancellation.clone()));
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
            authority: None,
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
            authority: None,
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
