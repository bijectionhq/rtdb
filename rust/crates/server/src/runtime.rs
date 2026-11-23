use std::{net::SocketAddr, sync::Arc, time::Duration};
use std::io::Write;

use rtdb_auth::{Authenticator, NativeHttp};
use rtdb_columnar::MemoryBudget;
use rtdb_metrics::Registry;
use rtdb_node::{Node, NodeDriver, NodeServices, SystemClock, TransactionClock};
use rtdb_tx::TransactionAuthorization;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{config::NodeConfiguration, health::{HealthConfig, HealthDriver, HealthHandle}};

struct HostedTransactionAuthority {
    auth: Arc<Authenticator>,
    owner: String,
}

impl TransactionAuthorization for HostedTransactionAuthority {
    fn check_role_membership(&self, database: &str, principal: Option<&str>) -> Result<()> {
        self.auth.check_role_membership(database, principal)
    }

    fn is_hosted_control_owner(&self, principal: &str) -> bool {
        principal == self.owner
    }
}

/// What a node placed by an outside authority needs beyond its configuration.
pub struct PlacedOptions {
    /// Log factories for the placed databases' logs, by protobuf type name.
    pub log_factories: Vec<(String, Arc<dyn rtdb_node::LogFactory>)>,
    /// Storage factories for the placed databases' object stores, by protobuf type name.
    pub storage_factories: Vec<(String, Arc<dyn rtdb_node::RemoteStorageFactory>)>,
    /// Asked before every query whether the node may still serve its database.
    pub read_fence: Option<Arc<dyn rtdb_node::ReadFence>>,
    /// Splits the placed databases' large scans into fragments read by the nodes they belong to.
    pub scan_fragmenter: Option<Arc<dyn rtdb_query::ScanFragmenter>>,
    /// Publishes compactions this node's read-only databases produce through their writers elsewhere.
    pub compaction_relay: Option<Arc<dyn rtdb_node::CompactionRelay>>,
    /// How long a placed holder waits on an empty replica read before claiming.
    pub claim_wait: Duration,
    /// Accepts capabilities minted by the cell's gateways in place of a password.
    pub capabilities: Option<Arc<dyn rtdb_pgwire::CapabilityVerifier>>,
    pub flight_peer: Option<Arc<dyn rtdb_flight_sql::FlightPeerVerifier>>,
    pub flight_owned_worlds: Option<Arc<dyn rtdb_flight_sql::OwnedWorldHost>>,
    pub world_constraints: Option<Arc<dyn rtdb_node::WorldConstraintArchive>>,
}

#[derive(Clone, Debug, Default)]
pub struct ListenerAddresses { pub pgwire: Option<SocketAddr>, pub read_only_pgwire: Option<SocketAddr>, pub flight_sql: Option<SocketAddr>, pub health: Option<SocketAddr> }

#[derive(Clone)]
pub struct ServerHandle {
    node: Node,
    auth: Arc<Authenticator>,
    metrics: Arc<Registry>,
    addresses: ListenerAddresses,
    primary_pgwire: Option<rtdb_pgwire::ServerHandle>,
    cancellation: CancellationToken,
}
impl ServerHandle {
    pub fn node(&self) -> &Node { &self.node }
    pub fn authenticator(&self) -> &Arc<Authenticator> { &self.auth }
    pub fn metrics(&self) -> &Arc<Registry> { &self.metrics }
    pub fn addresses(&self) -> &ListenerAddresses { &self.addresses }
    pub fn primary_pgwire(&self) -> Option<&rtdb_pgwire::ServerHandle> { self.primary_pgwire.as_ref() }
    pub fn shutdown(&self) { self.cancellation.cancel(); }
}

pub struct ServerDriver {
    handle: ServerHandle,
    node: NodeDriver,
    pgwire: Vec<(rtdb_pgwire::ServerHandle, rtdb_pgwire::ServerDriver)>,
    flight: Option<(rtdb_flight_sql::ServerHandle, rtdb_flight_sql::ServerDriver)>,
    health: Option<(HealthHandle, HealthDriver)>,
    registrations: Vec<rtdb_metrics::MetricRegistration>,
    tracing: Option<(crate::tracing::TraceExportHandle, crate::tracing::TraceExporter)>,
    metric_export: Option<(crate::metric_export::MetricExportHandle, crate::metric_export::MetricsExporter)>,
}
impl ServerDriver {
    /// Opens catalogs and binds every listener before starting any driver.
    /// Run exactly once; request shutdown through the handle and join this run before releasing dependencies.
    pub async fn open(config: NodeConfiguration, allocation_limit_bytes: usize) -> Result<(ServerHandle, Self)> {
        let memory = MemoryBudget::new(config.node_options(allocation_limit_bytes)?.execution_memory_bytes);
        Self::open_mode(config, allocation_limit_bytes, memory, false, None, None).await
    }

    /// The supplied execution budget must match the cache/execution split of the process allocation ceiling.
    /// Startup allocations retained by the caller may already hold reservations from this budget.
    pub async fn open_with_memory(config: NodeConfiguration, allocation_limit_bytes: usize, memory: Arc<MemoryBudget>) -> Result<(ServerHandle, Self)> {
        Self::open_mode(config, allocation_limit_bytes, memory, false, None, None).await
    }

    /// Opens a node whose databases an outside placement authority attaches and detaches, with no primary database.
    /// Its holders claim as soon as they reach the tip and append nothing while idle.
    pub async fn open_placed(config: NodeConfiguration, allocation_limit_bytes: usize, memory: Arc<MemoryBudget>, placed: PlacedOptions) -> Result<(ServerHandle, Self)> {
        Self::open_mode(config, allocation_limit_bytes, memory, false, None, Some(placed)).await
    }

    /// Uses a host clock for transactions appended to in-memory and local source logs.
    pub async fn open_with_log_clock(config: NodeConfiguration, allocation_limit_bytes: usize, memory: Arc<MemoryBudget>, clock: Arc<TransactionClock>) -> Result<(ServerHandle, Self)> {
        Self::open_mode(config, allocation_limit_bytes, memory, false, Some(clock), None).await
    }

    /// Runs compaction while following the replica log, with no transaction listeners or leader election.
    /// A separate writer must process compaction notifications from the source log.
    pub async fn open_compactor(config: NodeConfiguration, allocation_limit_bytes: usize) -> Result<(ServerHandle, Self)> {
        let memory = MemoryBudget::new(config.node_options(allocation_limit_bytes)?.execution_memory_bytes);
        Self::open_compactor_with_memory(config, allocation_limit_bytes, memory).await
    }

    /// Applies compactor-only startup options while retaining the supplied execution budget.
    pub async fn open_compactor_with_memory(mut config: NodeConfiguration, allocation_limit_bytes: usize, memory: Arc<MemoryBudget>) -> Result<(ServerHandle, Self)> {
        config.server = None;
        config.flight_sql = None;
        config.indexer.enabled = true;
        config.garbage.enabled = false;
        Self::open_mode(config, allocation_limit_bytes, memory, true, None, None).await
    }

    async fn open_mode(config: NodeConfiguration, allocation_limit_bytes: usize, memory: Arc<MemoryBudget>, compactor_only: bool, transaction_clock: Option<Arc<TransactionClock>>, placed: Option<PlacedOptions>) -> Result<(ServerHandle, Self)> {
        let mut options = config.node_options(allocation_limit_bytes)?;
        options.owned_relations = config.flight_sql.as_ref().is_some_and(|flight| flight.port >= 0 && flight.owned_native_actions);
        if let Some(placed) = &placed {
            options.membership = rtdb_node::Membership::Placed;
            options.replication.claim_wait = Some(placed.claim_wait);
            options.replication.idle_heartbeat = false;
            // A placed deployment's tail lives on its logkeepers, which keep tails short: no record waits longer than this for its block.
            options.replication.flush_timeout = options.replication.flush_timeout.min(Duration::from_secs(15 * 60));
        }
        if memory.limit_bytes() != options.execution_memory_bytes { return Err(Anomaly::incorrect("rtdb.server/invalid-memory-budget", "Supplied execution budget differs from the configured allocation split")); }
        if compactor_only { options.replication.may_lead = false; }
        let metrics = Registry::new(&config.node_id, Arc::clone(&memory))?;
        let metric_export = crate::metric_export::MetricsExporter::open(&config.modules, metrics.clone(), Arc::clone(&memory)).await?;
        let tracing = if config.tracing.enabled {
            match crate::tracing::TraceExporter::open(&config.tracing, Arc::clone(&memory)) { Ok(tracing) => Some(tracing), Err(error) => { let _ = writeln!(std::io::stderr().lock(), "Failed to create tracer; tracing is disabled: {error}"); None } }
        } else { None };
        let tracer = tracing.as_ref().map(|(handle, _)| handle.recorder().clone() as Arc<dyn rtdb_plan::Tracer>);
        let http = Arc::new(NativeHttp::new(Arc::clone(&memory), Duration::from_secs(30), 4 * 1024 * 1024)?);
        let auth = Authenticator::open(config.auth.clone(), Arc::clone(&memory), Arc::new(SystemClock), http).await?;
        let authority: Arc<dyn TransactionAuthorization> = match config.flight_sql.as_ref()
            .filter(|flight| flight.port >= 0 && flight.owned_native_actions)
            .and_then(|flight| flight.hosted_controls.as_ref())
            .filter(|policy| policy.relations.contains("bj_installation"))
        {
            Some(policy) => Arc::new(HostedTransactionAuthority { auth: auth.clone(), owner: policy.owner.clone() }),
            None => auth.clone(),
        };
        let (resources, external_sources, mut registrations) = crate::resources::open(&config, &memory, &metrics)?;
        let mut resources = match transaction_clock {
            Some(clock) => resources.with_log_clock(clock),
            None => resources,
        };
        let compaction_relay = placed.as_ref().and_then(|placed| placed.compaction_relay.clone());
        let read_fence = placed.as_ref().and_then(|placed| placed.read_fence.clone());
        let scan_fragmenter = placed.as_ref().and_then(|placed| placed.scan_fragmenter.clone());
        let capabilities = placed.as_ref().and_then(|placed| placed.capabilities.clone());
        let flight_peer = placed.as_ref().and_then(|placed| placed.flight_peer.clone());
        let flight_owned_worlds = placed.as_ref().and_then(|placed| placed.flight_owned_worlds.clone());
        let world_constraints = placed.as_ref().and_then(|placed| placed.world_constraints.clone());
        let gateway_session_state = placed.is_some();
        if let Some(placed) = placed {
            for (protobuf_type, factory) in placed.log_factories { resources = resources.register_log(protobuf_type, factory)?; }
            for (protobuf_type, factory) in placed.storage_factories { resources = resources.register_storage(protobuf_type, factory)?; }
        }
        let mut services = NodeServices { resources: Arc::new(resources), external_sources, execution_observer: Some(metrics.clone()), tracer, compaction_relay, world_constraints, read_fence, scan_fragmenter,
            ..NodeServices::local(authority, metrics.clone(), auth.clone(), metrics.clone()) };
        if !config.search.enabled { services.scan_accelerators.clear(); }
        let (node, driver) = Node::open_with_memory(options, services, memory).await?;
        let cancellation = CancellationToken::new();
        let listeners = async {
        registrations.push(metrics.register_provider(node.metrics_provider()?)?);
        let mut addresses = ListenerAddresses::default();
        let mut pgwire = Vec::new();
        if let Some(settings) = &config.server {
            let tls = settings.ssl.as_ref().map(|settings| crate::tls::load(settings, node.memory())).transpose()?;
            for (port, read_only) in [(settings.port, false), (settings.read_only_port, true)] {
                if port < 0 { continue; }
                let mut configuration = rtdb_pgwire::ServerConfig { address: address(&settings.host, port).await?, maximum_connections: settings.connections,
                    pending_connections: settings.connections, read_only, playground: config.playground, tls: tls.clone(), capabilities: capabilities.clone(), gateway_session_state, ..Default::default() };
                if let Some(zone) = &config.default_timezone {
                    configuration.parameters.insert("timezone".into(), zone.clone());
                }
                let (handle, driver) = rtdb_pgwire::ServerDriver::bind(configuration, node.clone(), auth.clone()).await?;
                registrations.push(metrics.register_provider(handle.metrics_provider()?)?);
                if read_only { addresses.read_only_pgwire = Some(handle.address()); } else { addresses.pgwire = Some(handle.address()); }
                pgwire.push((handle, driver));
            }
        }
        let flight = match &config.flight_sql {
            Some(settings) if settings.port >= 0 => {
                let configuration = rtdb_flight_sql::FlightConfig { address: address(&settings.host, settings.port).await?,
                    transaction_idle_timeout: settings.transaction_idle_timeout, prepared_statement_idle_timeout: settings.prepared_statement_idle_timeout,
                    session_idle_timeout: settings.session_idle_timeout, session_unclaimed_timeout: settings.session_unclaimed_timeout, playground: config.playground,
                    forwarded_peer: flight_peer, owned_world_host: flight_owned_worlds, owned_native_actions: settings.owned_native_actions, hosted_controls: settings.hosted_controls.clone(), ..Default::default() };
                let (handle, driver) = rtdb_flight_sql::ServerDriver::bind(configuration, node.clone(), auth.clone()).await?;
                addresses.flight_sql = Some(handle.address());
                Some((handle, driver))
            }
            _ => None,
        };
        let health = match &config.health {
            Some(settings) if settings.port >= 0 => {
                let configuration = HealthConfig { address: address(&settings.host, settings.port).await?, ..Default::default() };
                let (handle, driver) = HealthDriver::bind(configuration, node.clone(), metrics.clone(), &cancellation).await?;
                addresses.health = Some(handle.address());
                Some((handle, driver))
            }
            _ => None,
        };
        Ok::<_, Anomaly>((addresses, pgwire, flight, health))
        }.await;
        let (addresses, pgwire, flight, health) = match listeners {
            Ok(listeners) => listeners,
            Err(error) => return match driver.close_without_run().await { Ok(()) => Err(error), Err(close) => Err(error.with_context("resource-close-error", close.to_string().into())) },
        };
        let primary_pgwire = pgwire.iter().find(|(listener, _)| Some(listener.address()) == addresses.pgwire)
            .map(|(listener, _)| listener.clone());
        let handle = ServerHandle { node, auth, metrics, addresses, primary_pgwire, cancellation };
        Ok((handle.clone(), Self { handle, node: driver, pgwire, flight, health, registrations, tracing, metric_export }))
    }

    /// Releases bound listeners and drains an unstarted node's resources without starting its workers.
    pub async fn close_without_run(self) -> Result<()> {
        self.handle.shutdown();
        drop(self.pgwire); drop(self.flight); drop(self.health);
        if let Some((handle, _)) = &self.tracing { handle.close(); }
        if let Some((handle, _)) = &self.metric_export { handle.close(); }
        self.node.close_without_run().await
    }

    pub async fn run(self) -> Result<()> {
        let guard = ShutdownGuard { node: self.handle.node.clone(), cancellation: self.handle.cancellation.clone() };
        let _registrations = self.registrations;
        let mut exporters = JoinSet::new();
        let tracing = self.tracing.map(|(handle, exporter)| { exporters.spawn(exporter.run()); handle });
        let metric_export = self.metric_export.map(|(handle, exporter)| { exporters.spawn(exporter.run()); handle });
        let mut node = tokio::spawn(self.node.run());
        let mut listeners = JoinSet::new();
        let mut pgwire = Vec::new();
        for (handle, driver) in self.pgwire { pgwire.push(handle); listeners.spawn(driver.run()); }
        let flight = self.flight.map(|(handle, driver)| { listeners.spawn(driver.run()); handle });
        let health = self.health.map(|(handle, driver)| { listeners.spawn(driver.run()); handle });
        enum Exit { Requested, Node(std::result::Result<Result<()>, JoinError>), Listener(Option<std::result::Result<Result<()>, JoinError>>) }
        let exit = tokio::select! {
            biased;
            _ = self.handle.cancellation.cancelled() => Exit::Requested,
            result = &mut node => Exit::Node(result),
            result = listeners.join_next(), if !listeners.is_empty() => Exit::Listener(result),
        };
        let node_finished = matches!(&exit, Exit::Node(_));
        let mut failure = match exit {
            Exit::Requested => None,
            Exit::Node(result) => joined(result).err().or_else(|| Some(unexpected("node"))),
            Exit::Listener(result) => result.and_then(|result| joined(result).err()).or_else(|| Some(unexpected("listener"))),
        };
        for listener in pgwire { listener.shutdown(); }
        if let Some(listener) = flight { listener.shutdown(); }
        if let Some(listener) = health { listener.shutdown(); }
        while let Some(result) = listeners.join_next().await { remember(&mut failure, joined(result)); }
        self.handle.node.shutdown();
        if !node_finished { remember(&mut failure, joined(node.await)); }
        if let Some(tracing) = tracing { tracing.close(); }
        if let Some(metric_export) = metric_export { metric_export.close(); }
        while let Some(result) = exporters.join_next().await { if let Err(error) = result { remember(&mut failure, Err(Anomaly::fault("rtdb.tracing/task", error.to_string()))); } }
        drop(guard);
        failure.map_or(Ok(()), Err)
    }
}

struct ShutdownGuard { node: Node, cancellation: CancellationToken }
impl Drop for ShutdownGuard { fn drop(&mut self) { self.cancellation.cancel(); self.node.shutdown(); } }

async fn address(host: &str, port: i32) -> Result<SocketAddr> {
    let port = u16::try_from(port).map_err(|_| Anomaly::incorrect("rtdb.server/port", "Listener port is outside the unsigned 16-bit range"))?;
    tokio::net::lookup_host((host, port)).await.map_err(|error| Anomaly::incorrect("rtdb.server/host", error.to_string()))?.next()
        .ok_or_else(|| Anomaly::incorrect("rtdb.server/host", "Listener hostname resolved to no addresses"))
}
fn joined(result: std::result::Result<Result<()>, JoinError>) -> Result<()> { result.map_err(|error| Anomaly::fault("rtdb.server/task", error.to_string()))? }
fn unexpected(component: &str) -> Anomaly { Anomaly::fault("rtdb.server/component-stopped", format!("Server {component} stopped before shutdown was requested")) }
fn remember(first: &mut Option<Anomaly>, result: Result<()>) { if let Err(error) = result { if first.is_none() && error.category != AnomalyCategory::Interrupted { *first = Some(error); } } }
