//! The gateway: terminates a client's pgwire startup, finds or activates the deployment's holder, and splices the connection through to it (`RouteRead`, `ActivateOnDemand`, `ReclaimOnDemand`).
//!
//! Routing never authorises: a node may refuse or redirect, and the gateway walks on.

use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use rtdb_directory::{DeploymentRecord, Directory, Lifecycle, MemberId, MemberRecord, MemberRole, effective_holder, now_ms};
use rtdb_types::{Anomaly, AnomalyCategory, MemoryBudget, Result};
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_rustls::{TlsAcceptor, rustls};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use tokio_util::task::{AbortOnDropHandle, TaskTracker};

use crate::control::{self, Request, Response};

const SSL_REQUEST: i32 = 80877103;
const WRITER_DIAL_TIMEOUT: Duration = Duration::from_secs(1);
const GSSENC_REQUEST: i32 = 80877104;
const CANCEL_REQUEST: i32 = 80877102;
const PROTOCOL_3: i32 = 196608;
const MAXIMUM_STARTUP_BYTES: usize = 10_000;
const SNAPSHOT_PARAMETER: &str = "rtdb.read";

pub(crate) fn database_selector(selector: &str) -> Result<(&str, Option<Uuid>)> {
    let Some((name, authority)) = selector.split_once('@') else {
        return Ok((selector, None));
    };
    let authority = Uuid::parse_str(authority).ok().filter(|id| {
        !id.is_nil() && !name.is_empty() && id.simple().to_string() == authority
    }).ok_or_else(|| Anomaly::incorrect("rtdb.multi/database-selector", "Expected a database name followed by its installation ID"))?;
    Ok((name, Some(authority)))
}

#[derive(Clone)]
pub struct GatewayOptions {
    /// The cell map and this gateway's cell, so a connection for a deployment living elsewhere is forwarded there.
    pub cells: Option<(Arc<dyn crate::cells::CellMap>, String)>,
    pub installed_journals: Option<Arc<crate::journal::hosted::GatewayRoutes>>,
    pub installed_cell: Option<Arc<crate::journal::cell::CellAccess>>,
    pub control_timeout: Duration,
    /// When set, every frontend must negotiate TLS before startup.
    pub tls: Option<Arc<rustls::ServerConfig>>,
    pub startup_timeout: Duration,
    /// How long established sessions may finish after the listener stops admitting connections.
    pub shutdown_grace: Duration,
    pub maximum_connections: usize,
    pub maximum_connections_per_deployment: usize,
    pub maximum_buffered_bytes: usize,
    /// How often an open connection tells its holder it is in use.
    pub touch_interval: Duration,
    /// The cell's secret, with which the gateway mints a capability for each reconnection of an authenticated session.
    pub capabilities: Option<Arc<crate::capability::CellSecret>>,
    pub classic_retained_flight_key: Option<std::path::PathBuf>,
}

impl Default for GatewayOptions {
    fn default() -> Self {
        Self {
            cells: None, installed_journals: None, installed_cell: None, tls: None, startup_timeout: crate::session::STARTUP_WITHIN, shutdown_grace: Duration::ZERO,
            control_timeout: Duration::from_secs(35), touch_interval: Duration::from_secs(10),
            maximum_connections: 4096, maximum_connections_per_deployment: 256,
            maximum_buffered_bytes: crate::session::DEFAULT_GATEWAY_FRAME_BYTES, capabilities: None,
            classic_retained_flight_key: None,
        }
    }
}

impl GatewayOptions {
    pub(crate) async fn ordered_placement(&self, name: &str)
        -> Result<Option<crate::journal::hosted::GatewayPlacement>>
    {
        if let Some(cell) = &self.installed_cell {
            return cell.gateway_placement(name).await;
        }
        Ok(self.installed_journals.as_ref().and_then(|routes| routes.by_name(name)).cloned())
    }
}

pub(crate) struct DeploymentConnections {
    limit: usize,
    counts: Mutex<BTreeMap<String, usize>>,
}

pub(crate) struct DeploymentConnection {
    registry: Arc<DeploymentConnections>,
    name: String,
}

impl DeploymentConnections {
    pub(crate) fn new(limit: usize) -> Self { Self { limit, counts: Mutex::default() } }
    pub(crate) fn admit(self: &Arc<Self>, name: &str) -> Option<DeploymentConnection> {
        let mut active = self.counts.lock();
        let count = active.entry(name.to_owned()).or_default();
        if *count >= self.limit { return None; }
        *count += 1;
        Some(DeploymentConnection { registry: Arc::clone(self), name: name.to_owned() })
    }
}

impl Drop for DeploymentConnection {
    fn drop(&mut self) {
        let mut active = self.registry.counts.lock();
        let count = active.get_mut(&self.name).expect("an admission owns a connection slot");
        *count -= 1;
        if *count == 0 { active.remove(&self.name); }
    }
}

pub async fn serve(listener: TcpListener, directory: Arc<dyn Directory>, options: GatewayOptions, cancellation: CancellationToken) -> Result<()> {
    if !(1..=Semaphore::MAX_PERMITS).contains(&options.maximum_connections) || options.maximum_connections_per_deployment == 0 {
        return Err(Anomaly::incorrect("rtdb.multi/gateway-connections", "Gateway connection limits must be positive and supported by the runtime"));
    }
    if options.maximum_buffered_bytes == 0 {
        return Err(Anomaly::incorrect("rtdb.multi/gateway-buffer", "The gateway's frame memory budget must be positive"));
    }
    let frame_memory = MemoryBudget::new(options.maximum_buffered_bytes);
    let connections = Arc::new(Semaphore::new(options.maximum_connections));
    let deployments = Arc::new(DeploymentConnections::new(options.maximum_connections_per_deployment));
    let heartbeats = Arc::new(Heartbeats::default());
    let mut connection_index = 0usize;
    let tasks = TaskTracker::new();
    let sessions = CancellationToken::new();
    let _cancel_sessions = sessions.clone().drop_guard();
    loop {
        let (client, _) = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            accepted = listener.accept() => accepted.map_err(io)?,
        };
        let Ok(permit) = Arc::clone(&connections).try_acquire_owned() else { continue; };
        let _ = client.set_nodelay(true);
        let deployments = Arc::clone(&deployments);
        let frame_memory = Arc::clone(&frame_memory);
        let directory = Arc::clone(&directory);
        let options = options.clone();
        let heartbeats = Arc::clone(&heartbeats);
        let cancellation = sessions.child_token();
        let index = connection_index;
        connection_index = connection_index.wrapping_add(1);
        tasks.spawn(async move {
            let _permit = permit;
            tokio::select! {
                _ = cancellation.cancelled() => {}
                _ = connection(client, directory, options, cancellation.clone(), heartbeats, deployments, frame_memory, index) => {}
            }
        });
    }
    drop(listener);
    tasks.close();
    if !options.shutdown_grace.is_zero() && tokio::time::timeout(options.shutdown_grace, tasks.wait()).await.is_ok() {
        return Ok(());
    }
    sessions.cancel();
    tasks.wait().await;
    Ok(())
}

const RELAY_ANSWER_DEADLINE_PARAMETER: &str = "rtdb.relay_answer_deadline";

struct Startup { bytes: Vec<u8>, parameters: BTreeMap<String, String> }

impl Startup {
    /// Whether the client asked to read the deployment as of its checkpoint (`rtdb.read=snapshot`, as a parameter or in `options`), which any node may serve from object storage.
    fn snapshot(&self) -> bool {
        self.parameters.get(SNAPSHOT_PARAMETER).is_some_and(|value| value == "snapshot")
            || self.parameters.get("options").is_some_and(|options| options.split_whitespace().any(|option| option.trim_start_matches("-c").split_once('=')
                .is_some_and(|(key, value)| matches!(key.trim(), SNAPSHOT_PARAMETER | "rtdb_read") && value == "snapshot")))
    }

    /// Whether the client asked for a read-only session, which a log-fed replica may serve.
    fn read_only(&self) -> bool {
        if self.snapshot() { return true; }
        let on = |value: &str| matches!(value.trim().to_ascii_lowercase().as_str(), "on" | "true" | "1" | "yes");
        self.parameters.get("default_transaction_read_only").is_some_and(|value| on(value))
            || self.parameters.get("target_session_attrs").is_some_and(|value| matches!(value.as_str(), "read-only" | "prefer-standby" | "standby"))
            || self.parameters.get("options").is_some_and(|options| options.split_whitespace().any(|option| option.trim_start_matches("-c").split_once('=')
                .is_some_and(|(key, value)| key.trim() == "default_transaction_read_only" && on(value))))
    }
}

trait Transport: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Transport for T {}
type Client = Box<dyn Transport>;

async fn connection(client: TcpStream, directory: Arc<dyn Directory>, options: GatewayOptions, cancellation: CancellationToken,
                    heartbeats: Arc<Heartbeats>, deployments: Arc<DeploymentConnections>, frame_memory: Arc<MemoryBudget>, connection_index: usize) -> Result<()> {
    let startup_deadline = tokio::time::Instant::now() + options.startup_timeout;
    let startup = tokio::time::timeout_at(startup_deadline, read_startup(Box::new(rtdb_storage::sealing::DeliveryIo::new(client)), options.tls.clone()));
    let Some(result) = cancellation.run_until_cancelled(startup).await else { return Ok(()); };
    let Some((mut client, mut startup)) = result.map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/startup-timeout", "PostgreSQL startup did not finish"))?? else { return Ok(()); };
    let forward_stamp = startup.parameters.remove(RELAY_ANSWER_DEADLINE_PARAMETER).as_deref() == Some("on");
    let selector = startup.parameters.get("database");
    let selection = match selector {
        Some(selector) => database_selector(selector),
        None => Ok((startup.parameters.get("user").map(String::as_str).unwrap_or_default(), None)),
    };
    let (name, expected) = match selection {
        Ok((name, expected)) if !name.is_empty() => (name.to_owned(), expected),
        Ok(_) => return refuse(&mut client, "3D000", "The startup message names no database").await,
        Err(error) => return refuse(&mut client, "3D000", error.message.as_deref().unwrap_or_default()).await,
    };
    let Some(_deployment_connection) = deployments.admit(&name) else {
        return refuse(&mut client, "53300", "This deployment has reached its gateway connection limit").await;
    };
    let ordered = options.installed_cell.is_some()
        || options.installed_journals.as_ref().and_then(|routes| routes.by_name(&name)).is_some();
    let Ok(remote_gateway) = tokio::time::timeout_at(startup_deadline, async {
        if ordered { None } else { elsewhere(&directory, &name, &options).await }
    }).await else {
        return refuse(&mut client, "57P03", "Timed out looking up the deployment's cell").await;
    };
    if let Some(gateway) = remote_gateway {
        startup.parameters.insert(RELAY_ANSWER_DEADLINE_PARAMETER.into(), "on".into());
        startup.bytes = encode_startup(&startup.parameters);
        let forward = tokio::time::timeout_at(startup_deadline, async {
            let mut forward = connect(&gateway).await?;
            forward.write_all(&startup.bytes).await.map_err(io)?;
            Ok::<_, Anomaly>(forward)
        }).await;
        let forward = match forward {
            Ok(Ok(forward)) => forward,
            Ok(Err(error)) => return refuse(&mut client, "08006", error.message.as_deref().unwrap_or_default()).await,
            Err(_) => return refuse(&mut client, "57P03", "Timed out forwarding startup to the deployment's cell").await,
        };
        return crate::session::relay_cell(client, forward, &frame_memory, forward_stamp).await;
    }
    if let Some(installation) = expected {
        startup.parameters.insert("database".into(), name.clone());
        startup.parameters.insert(rtdb_pgwire::REQUIRED_INSTALLATION_PARAMETER.into(), installation.to_string());
    }
    startup.parameters.insert(rtdb_pgwire::GATEWAY_AWAIT_TOKEN_PARAMETER.into(), "on".into());
    startup.parameters.insert(rtdb_pgwire::GATEWAY_SETTINGS_PARAMETER.into(), "on".into());
    let snapshot = startup.snapshot();
    let read_only = startup.read_only();
    startup.parameters.remove(rtdb_pgwire::GATEWAY_REPLICA_READ_PARAMETER);
    if read_only && !snapshot {
        startup.parameters.insert(rtdb_pgwire::GATEWAY_REPLICA_READ_PARAMETER.into(), "on".into());
    }
    startup.bytes = encode_startup(&startup.parameters);
    let mut router = GatewayRouter {
        directory: Arc::clone(&directory), name,
        deployment: expected.map(|id| id.hyphenated().to_string()),
        read_only, snapshot, options: options.clone(), heartbeats, connection_index,
    };
    let route_initial = tokio::time::timeout_at(startup_deadline, router.route_with_record(false)).await;
    let (server, control, epoch, deployment) = match route_initial {
        Ok(Ok(route)) => route,
        Err(_) => return refuse(&mut client, "57P03", "Timed out routing startup to the deployment's holder").await,
        Ok(Err(error)) => return refuse(&mut client, "08006", error.message.as_deref().unwrap_or(error.code.as_ref())).await,
    };
    router.deployment = Some(deployment.clone());
    let router = Arc::new(router);
    let grants = options.capabilities.clone().map(|secret| crate::session::Grants { secret, parameters: startup.parameters.clone(), read_only: startup.read_only() });
    let keyed = !startup.read_only();
    let session = if ordered {
        crate::session::Session::new_ordered(Bytes::from(startup.bytes), router, grants, keyed)
    } else {
        crate::session::Session::new(Bytes::from(startup.bytes), router, directory, deployment, grants, keyed)
    }.with_frame_memory(frame_memory).with_installation(expected);
    tokio::select! {
        _ = cancellation.cancelled() => Ok(()),
        outcome = session.run_before(client, server, control, epoch, startup_deadline, forward_stamp) => outcome,
    }
}

/// The gateway of the cell a deployment lives in, when that is not this gateway's cell.
async fn elsewhere(directory: &Arc<dyn Directory>, name: &str, options: &GatewayOptions) -> Option<String> {
    let (map, cell) = options.cells.as_ref()?;
    let here = directory.deployment_by_name(name).await.ok().flatten();
    if here.as_ref().is_some_and(|record| matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)) { return None; }
    let living = map.cell_of(name).await.ok().flatten()?;
    if &living == cell { return None; }
    map.gateway(&living).await.ok().flatten()
}

struct GatewayRouter {
    directory: Arc<dyn Directory>,
    name: String,
    deployment: Option<String>,
    read_only: bool,
    snapshot: bool,
    options: GatewayOptions,
    heartbeats: Arc<Heartbeats>,
    connection_index: usize,
}

type HeartbeatTarget = (String, String);

#[derive(Default)]
struct Heartbeats(Mutex<BTreeMap<HeartbeatTarget, Weak<Heartbeat>>>);

struct Heartbeat {
    registry: Weak<Heartbeats>,
    target: HeartbeatTarget,
    _task: AbortOnDropHandle<()>,
}

impl Heartbeats {
    fn sort_by_sessions(&self, deployment: &str, candidates: &mut [&MemberRecord]) {
        let active = self.0.lock();
        candidates.sort_by_cached_key(|candidate| candidate.control_address.as_ref()
            .and_then(|address| active.get(&(deployment.to_owned(), address.clone())))
            .map_or(0, Weak::strong_count));
    }

    fn register(self: &Arc<Self>, deployment: &str, address: String, options: &GatewayOptions) -> Arc<Heartbeat> {
        let target = (deployment.to_owned(), address);
        let mut active = self.0.lock();
        if let Some(heartbeat) = active.get(&target).and_then(Weak::upgrade) { return heartbeat; }
        let (name, address) = target.clone();
        let interval = options.touch_interval;
        let timeout = options.control_timeout;
        let key = control::ControlKey(options.capabilities.clone());
        let request = Request::Touch { name };
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let _ = control::call(&address, &request, timeout, &key).await;
            }
        });
        let heartbeat = Arc::new(Heartbeat { registry: Arc::downgrade(self), target: target.clone(), _task: AbortOnDropHandle::new(task) });
        active.insert(target, Arc::downgrade(&heartbeat));
        heartbeat
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            let mut active = registry.0.lock();
            if active.get(&self.target).is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self)) {
                active.remove(&self.target);
            }
        }
    }
}

impl GatewayRouter {
    async fn route_with_record(&self, prefer_holder: bool) -> Result<(TcpStream, Option<String>, u64, String)> {
        control::retry_activation(self.options.control_timeout, || route(self, prefer_holder)).await
    }
}

#[async_trait::async_trait]
impl crate::session::Router for GatewayRouter {
    async fn route(&self) -> Result<(TcpStream, Option<String>, u64)> {
        let (stream, control, epoch, _) = self.route_with_record(false).await?;
        Ok((stream, control, epoch))
    }

    async fn route_holder(&self) -> Result<(TcpStream, Option<String>, u64)> {
        let (stream, control, epoch, _) = self.route_with_record(true).await?;
        Ok((stream, control, epoch))
    }

    fn start_touching(&self, address: String) -> Option<Arc<dyn Send + Sync>> {
        Some(self.heartbeats.register(&self.name, address, &self.options))
    }
}

/// The open connection to the deployment's holder, and that holder's control address.
async fn route(router: &GatewayRouter, prefer_holder: bool) -> Result<(TcpStream, Option<String>, u64, String)> {
    let directory = &router.directory;
    let name = router.name.as_str();
    let options = &router.options;
    if let Some(installed) = options.ordered_placement(name).await? {
        if router.snapshot {
            return Err(Anomaly::unsupported("rtdb.multi/ordered-pgwire-checkpoint-unheld",
                "The installed PGWire checkpoint selector cannot bind a connection to an exact protected checkpoint"));
        }
        if router.deployment.as_deref().is_some_and(|id| Uuid::parse_str(id).ok() != Some(installed.deployment)) {
            return Err(Anomaly::incorrect("rtdb.multi/install-identity", format!("Database {name} has another installation ID")));
        }
        if router.read_only && !prefer_holder
            && let Ok(read) = route_ordered_reader(name, &installed, options, false).await
        {
            let server = connect_serving(&read.worker.pgwire, &read.worker.control, read.probe, options).await?;
            return Ok((server, Some(read.worker.control), read.epoch, installed.deployment.to_string()));
        }
        let (server, worker, epoch) = route_ordered(name, &installed, options, !router.read_only || prefer_holder, |worker, probe| async move {
            connect_serving(&worker.pgwire, &worker.control, probe, options).await
        }).await?;
        return Ok((server, Some(worker.control), epoch, installed.deployment.to_string()));
    }
    if options.installed_cell.is_some() {
        return Err(Anomaly::incorrect("rtdb.multi/unknown-deployment", format!("No installed deployment is named {name}")));
    }
    let record = directory.deployment_by_name(name).await?
        .ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-deployment", format!("No deployment is named {name}")))?;
    if router.deployment.as_deref().is_some_and(|id| id != record.id.as_str()) {
        return Err(Anomaly::incorrect("rtdb.multi/install-identity", format!("Database {name} has another installation ID")));
    }
    match record.lifecycle {
        Lifecycle::Live | Lifecycle::Quarantined => {}
        Lifecycle::Moving => return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/moving", format!("{name} is moving to another cell; retry shortly"))),
        Lifecycle::Restoring => return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/restoring", format!("{name}'s checkpoint is being restored; retry shortly"))),
        Lifecycle::Deleting | Lifecycle::Deleted => return Err(Anomaly::incorrect("rtdb.multi/deleted", format!("{name} has been deleted"))),
    }
    if (prefer_holder || !router.snapshot && (!router.read_only || record.read_width <= 1))
        && let Some(holder) = &record.holder
        && let Some(member) = directory.member(holder).await.ok().flatten()
        && record.may_hold_on(&member, now_ms())
        && let Some(control_address) = member.control_address
        && let Ok((Response::Held { address, epoch }, confirmed, probe)) = confirm_writer(directory.as_ref(), &record, &control_address, options).await
    {
        return Ok((connect_serving(&address, &confirmed, probe, options).await?, Some(confirmed), epoch, record.id));
    }
    let members: BTreeMap<MemberId, MemberRecord> = directory.members().await?.into_iter().map(|member| (member.id.clone(), member)).collect();
    if !prefer_holder && router.snapshot && record.checkpoint.is_some() && record.lifecycle == Lifecycle::Live {
        for reader in snapshot_readers(&record, &members) {
            let Some(control_address) = &reader.control_address else { continue; };
            if let Ok((Response::Subscribed { address }, probe)) = control::call_with_pgwire_probe(control_address,
                Request::Snapshot { name: name.to_owned() }, options.control_timeout, &control::ControlKey(options.capabilities.clone())).await
                && let Ok(stream) = connect_serving(&address, control_address, probe, options).await
            { return Ok((stream, Some(control_address.clone()), record.epoch, record.id.clone())); }
        }
    }
    if !prefer_holder && router.read_only && record.read_width > 1 && record.lifecycle == Lifecycle::Live {
        let mut readers = crate::deployment::read_replicas(&record, &members, now_ms());
        let count = readers.len();
        if count > 1 {
            readers.rotate_left(router.connection_index % count);
            router.heartbeats.sort_by_sessions(name, &mut readers);
        }
        for replica in readers {
            let Some(control_address) = &replica.control_address else { continue; };
            let request = if record.linearizable_reads { Request::SubscribeFresh { name: name.to_owned() } } else { Request::Subscribe { name: name.to_owned() } };
            if let Ok((Response::Subscribed { address }, probe)) = control::call_with_pgwire_probe(control_address,
                request, options.control_timeout, &control::ControlKey(options.capabilities.clone())).await
                && let Ok(stream) = connect_serving(&address, control_address, probe, options).await
            { return Ok((stream, Some(control_address.clone()), record.epoch, record.id.clone())); }
        }
    }
    let mut last = None;
    let mut retryable = None;
    let mut refused = None;
    for candidate in candidates(&record, &members) {
        let Some(control_address) = &candidate.control_address else { continue; };
        match confirm_writer(directory.as_ref(), &record, control_address, options).await {
            Ok((Response::Held { address, epoch }, confirmed, probe)) => return Ok((connect_serving(&address, &confirmed, probe, options).await?, Some(confirmed), epoch, record.id.clone())),
            Ok((Response::Refused { code, message }, _, _)) if control::retryable_activation(&code) => {
                retryable = Some(Anomaly::new(AnomalyCategory::Unavailable, code, message));
            }
            Ok((Response::Refused { code, message }, _, _)) if code == control::NOT_CLAIMABLE => {
                last = Some(Anomaly::new(AnomalyCategory::Unavailable, code, message));
            }
            Ok((Response::Refused { code, message }, _, _)) => refused = Some(Anomaly::new(AnomalyCategory::Unavailable, code, message)),
            Ok(_) => {}
            Err(error) if control::unreachable_control(&error.code) => {
                retryable.get_or_insert_with(|| Anomaly::new(AnomalyCategory::Unavailable, control::WRITER_CONTROL_UNREACHABLE,
                    error.message.as_deref().unwrap_or("The writer's control endpoint is unreachable")));
            }
            Err(error) => last = Some(error),
        }
    }
    Err(refused.or(retryable).or(last).unwrap_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/no-node", "No node can serve this deployment")))
}

pub(crate) struct OrderedReaderRoute {
    pub worker: crate::journal::hosted::WorkerEndpoint,
    pub placement: ordered_logkeeper::ordered::Placement,
    pub epoch: u64,
    pub flight: Option<String>,
    probe: Option<u64>,
}

pub(crate) async fn route_ordered_reader(
    name: &str, installed: &crate::journal::hosted::GatewayPlacement, options: &GatewayOptions, require_flight: bool,
) -> Result<OrderedReaderRoute> {
    let view = installed.journal.route().await?;
    let incumbent = view.writer.grant.map(|grant| grant.enrollment);
    let secret = options.capabilities.as_ref().ok_or_else(|| Anomaly::incorrect(
        "rtdb.multi/ordered-gateway", "An ordered reader needs its cell route credential"))?;
    let key = control::ControlKey(options.capabilities.clone());
    for worker in installed.endpoints.values().filter(|worker| Some(worker.enrollment) != incumbent) {
        let request = Request::SubscribeOrderedFresh {
            name: name.into(), placement: view.placement, enrollment: worker.enrollment, epoch: view.writer.epoch,
        };
        let Ok((Response::OrderedRead { address, flight, proof }, probe)) =
            control::call_with_pgwire_probe(&worker.control, request, options.control_timeout, &key).await else { continue; };
        if address == worker.pgwire && (!require_flight || flight.is_some()) && secret.verify_route(&control::ordered_read_binding(
            name, view.placement, worker.enrollment, view.writer.epoch, &address, flight.as_deref(),
        ), &proof) {
            return Ok(OrderedReaderRoute {
                worker: worker.clone(), placement: view.placement, epoch: view.writer.epoch, flight, probe,
            });
        }
    }
    Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/ordered-reader",
        "No enrolled worker could open the ordered read basis"))
}

pub(crate) async fn route_ordered<T, Open>(
    name: &str,
    installed: &crate::journal::hosted::GatewayPlacement,
    options: &GatewayOptions,
    allow_takeover: bool,
    open: impl Fn(crate::journal::hosted::WorkerEndpoint, Option<u64>) -> Open + Send,
) -> Result<(T, crate::journal::hosted::WorkerEndpoint, u64)>
where
    Open: Future<Output = Result<T>> + Send,
{
    let view = installed.journal.route().await?;
    let incumbent = view.writer.grant.map(|grant| grant.enrollment);
    if !allow_takeover {
        let grant = view.writer.grant.ok_or_else(writer_unreachable)?;
        let worker = installed.endpoints.get(&grant.enrollment).ok_or_else(writer_unreachable)?;
        let key = control::ControlKey(options.capabilities.clone());
        let probe = control::ping(&worker.control, options.control_timeout.min(WRITER_DIAL_TIMEOUT), &key).await?;
        let frontend = open(worker.clone(), probe).await?;
        if installed.journal.route().await?.writer.grant != Some(grant) {
            return Err(writer_unreachable());
        }
        return Ok((frontend, worker.clone(), grant.epoch));
    }
    let predecessor = control::OrderedPredecessor { epoch: view.writer.epoch, enrollment: incumbent };
    let secret = options.capabilities.as_ref().ok_or_else(|| Anomaly::incorrect(
        "rtdb.multi/ordered-gateway", "The installed gateway needs a cell route credential"))?;
    let key = control::ControlKey(options.capabilities.clone());
    let installed_incumbent = incumbent.and_then(|owner| installed.endpoints.get(&owner));
    let candidates = installed_incumbent.into_iter()
        .chain(installed.endpoints.values().filter(|worker| Some(worker.enrollment) != incumbent));
    let mut refused = None;
    for worker in candidates {
        let takeover = Some(worker.enrollment) != incumbent;
        let activation = control::OrderedActivation::issue(secret, name, view.placement, predecessor, worker.enrollment, takeover);
        let request = Request::ActivateOrdered { name: name.to_owned(), activation };
        let timeout = if takeover { options.control_timeout } else { options.control_timeout.min(WRITER_DIAL_TIMEOUT) };
        match control::call_with_pgwire_probe(&worker.control, request, timeout, &key).await {
            Ok((Response::OrderedHeld { address, epoch, proof }, probe)) if address == worker.pgwire
                && (takeover || epoch == view.writer.epoch)
                && secret.verify_route(&activation.reply_binding(view.placement, epoch, &address), &proof) => {
                let current = installed.journal.route().await?;
                if current.writer.grant.is_none_or(|grant| grant.epoch != epoch || grant.enrollment != worker.enrollment) {
                    return Err(Anomaly::new(AnomalyCategory::Conflict, control::CLAIM_CONTENDED,
                        "The installed journal selected a different writer"));
                }
                match open(worker.clone(), probe).await {
                    Ok(frontend) => return Ok((frontend, worker.clone(), epoch)),
                    Err(error) => refused = Some(error),
                }
            }
            Ok((Response::Refused { code, message }, _)) => {
                refused = Some(Anomaly::new(AnomalyCategory::Unavailable, code, message));
            }
            _ => {}
        }
    }
    Err(refused.unwrap_or_else(|| Anomaly::new(AnomalyCategory::Unavailable,
        control::WRITER_CONTROL_UNREACHABLE, "No exact enrolled worker could take the journal grant")))
}


async fn confirm_writer(directory: &dyn Directory, record: &DeploymentRecord, control_address: &str, options: &GatewayOptions) -> Result<(Response, String, Option<u64>)> {
    let key = control::ControlKey(options.capabilities.clone());
    let request = Request::Activate { name: record.name.clone() };
    let (response, probe) = control::call_with_pgwire_probe(control_address, request.clone(), options.control_timeout, &key).await?;
    if let Response::Redirect { holder, .. } = &response
        && let Some(member) = directory.member(&MemberId(holder.clone())).await?
        && record.may_hold_on(&member, now_ms())
        && let Some(target) = member.control_address.filter(|address| address != control_address)
    {
        let (response, probe) = control::call_with_pgwire_probe(&target, request, options.control_timeout, &key).await?;
        return Ok((response, target, probe));
    }
    Ok((response, control_address.to_owned(), probe))
}

/// The nodes that may serve a snapshot read: every node that may take a placement, in the deployment's affinity order, the holder last, so snapshots spare the writer.
fn snapshot_readers<'a>(record: &DeploymentRecord, members: &'a BTreeMap<MemberId, MemberRecord>) -> Vec<&'a MemberRecord> {
    let now = now_ms();
    let holder = effective_holder(record, members, now).cloned();
    let nodes: Vec<&MemberRecord> = members.values().filter(|member| member.role == MemberRole::Node && member.pool == rtdb_directory::MemberPool::General && member.may_receive_placements(now)).collect();
    let (mut others, holders): (Vec<&MemberRecord>, Vec<&MemberRecord>) = rtdb_affinity::rank(&record.storage_identity(), &nodes, |member| member.id.as_str()).into_iter().copied()
        .partition(|member| holder.as_ref() != Some(&member.id));
    others.extend(holders);
    others
}

/// The holder that counts, then every node that may take a placement, in the deployment's affinity order.
fn candidates<'a>(record: &DeploymentRecord, members: &'a BTreeMap<MemberId, MemberRecord>) -> Vec<&'a MemberRecord> {
    let now = now_ms();
    let holder = effective_holder(record, members, now).and_then(|holder| members.get(holder));
    let nodes: Vec<&MemberRecord> = members.values().filter(|member| record.may_place_on(member, now)).collect();
    let reserved = record.active_move(now).and_then(|intent| members.get(&intent.target))
        .filter(|member| record.may_place_on(member, now) && holder.is_none_or(|holder| holder.id != member.id));
    let ranked = rtdb_affinity::rank(&record.storage_identity(), &nodes, |member| member.id.as_str());
    holder.into_iter().chain(reserved).chain(ranked.into_iter().copied().filter(|member| {
        holder.is_none_or(|holder| holder.id != member.id) && reserved.is_none_or(|target| target.id != member.id)
    })).collect()
}

async fn connect(address: &str) -> Result<TcpStream> {
    let stream = TcpStream::connect(address).await.map_err(io)?;
    stream.set_nodelay(true).map_err(io)?;
    Ok(stream)
}

fn writer_unreachable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, control::WRITER_PGWIRE_UNREACHABLE, "The writer's pgwire endpoint is unreachable")
}

async fn connect_serving(address: &str, control: &str, probe: Option<u64>, options: &GatewayOptions) -> Result<TcpStream> {
    let timeout = options.control_timeout.min(WRITER_DIAL_TIMEOUT);
    let key = control::ControlKey(options.capabilities.clone());
    let direct = async {
        let mut stream = connect(address).await?;
        if probe.is_some() { check_pgwire_serving(&mut stream, rtdb_pgwire::SERVING_STARTUP).await?; }
        Ok::<_, Anomaly>(stream)
    };
    if let Ok(Ok(stream)) = tokio::time::timeout(timeout, direct).await { return Ok(stream); }
    let relay = async {
        let mut stream = control::open_pgwire(control, timeout, &key).await?;
        check_pgwire_serving(&mut stream, rtdb_pgwire::SERVING_STARTUP).await?;
        Ok::<_, Anomaly>(stream)
    };
    if let Ok(Ok(stream)) = tokio::time::timeout(timeout, relay).await { return Ok(stream); }
    if let Some(before) = probe
        && matches!(control::ping(control, timeout, &key).await, Ok(Some(after)) if after != before)
        && let Ok(Ok(stream)) = tokio::time::timeout(timeout, connect(address)).await
    {
        return Ok(stream);
    }
    Err(writer_unreachable())
}

async fn check_pgwire_serving(stream: &mut TcpStream, preface: [u8; 8]) -> Result<()> {
    stream.write_all(&preface).await.map_err(io)?;
    let mut reply = [0; rtdb_pgwire::SERVING_REPLY.len()];
    stream.read_exact(&mut reply).await.map_err(io)?;
    if reply == rtdb_pgwire::SERVING_REPLY { Ok(()) } else { Err(writer_unreachable()) }
}

pub(crate) async fn probe_pgwire(address: &str, timeout: Duration, serving_probe: bool) -> bool {
    let probe = async {
        let mut stream = connect(address).await?;
        if serving_probe { check_pgwire_serving(&mut stream, rtdb_pgwire::SERVING_PROBE).await?; }
        Ok::<_, Anomaly>(())
    };
    matches!(tokio::time::timeout(timeout, probe).await, Ok(Ok(())))
}

/// Returns None for a cancel request, which this gateway does not forward.
async fn read_startup(mut client: Client, tls: Option<Arc<rustls::ServerConfig>>) -> Result<Option<(Client, Startup)>> {
    let mut encrypted = false;
    loop {
        let length = client.read_i32().await.map_err(io)?;
        let length = usize::try_from(length).ok().filter(|length| (8..=MAXIMUM_STARTUP_BYTES).contains(length))
            .ok_or_else(|| Anomaly::incorrect("rtdb.multi/startup", "Startup message length is out of range"))?;
        let mut body = vec![0; length - 4];
        client.read_exact(&mut body).await.map_err(io)?;
        let code = i32::from_be_bytes(body[..4].try_into().expect("four bytes"));
        match code {
            SSL_REQUEST if length == 8 => {
                if let Some(tls) = tls.as_ref().filter(|_| !encrypted) {
                    client.write_all(b"S").await.map_err(io)?;
                    client = Box::new(TlsAcceptor::from(Arc::clone(tls)).accept(client).await.map_err(io)?);
                    encrypted = true;
                } else {
                    client.write_all(b"N").await.map_err(io)?;
                    client.flush().await.map_err(io)?;
                }
            }
            GSSENC_REQUEST if length == 8 => {
                client.write_all(b"N").await.map_err(io)?;
                client.flush().await.map_err(io)?;
            }
            CANCEL_REQUEST if length == 16 => return Ok(None),
            PROTOCOL_3 => {
                if tls.is_some() && !encrypted {
                    refuse(&mut client, "08004", "This gateway requires TLS").await?;
                    return Ok(None);
                }
                let mut parameters = BTreeMap::new();
                let mut fields = body[4..].split(|byte| *byte == 0).map(|field| String::from_utf8_lossy(field).into_owned());
                while let (Some(key), Some(value)) = (fields.next(), fields.next()) {
                    if key.is_empty() { break; }
                    parameters.insert(key, value);
                }
                // Only a gateway grants capabilities: one a client sends is dropped.
                parameters.remove(rtdb_pgwire::CAPABILITY_PARAMETER);
                parameters.retain(|key, _| !key.eq_ignore_ascii_case(rtdb_pgwire::REQUIRED_INSTALLATION_PARAMETER)
                    && !key.eq_ignore_ascii_case(rtdb_pgwire::VERIFIED_INSTALLATION_PARAMETER));
                return Ok(Some((client, Startup { bytes: encode_startup(&parameters), parameters })));
            }
            _ => return Err(Anomaly::incorrect("rtdb.multi/startup", format!("Unsupported startup code {code}"))),
        }
    }
}

/// A protocol-3 startup message carrying `parameters`.
pub(crate) fn encode_startup(parameters: &BTreeMap<String, String>) -> Vec<u8> {
    let mut body = PROTOCOL_3.to_be_bytes().to_vec();
    for (key, value) in parameters { for field in [key, value] { body.extend_from_slice(field.as_bytes()); body.push(0); } }
    body.push(0);
    let mut bytes = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    bytes.extend_from_slice(&body);
    bytes
}

async fn refuse(client: &mut (impl AsyncWrite + Unpin), sqlstate: &str, message: &str) -> Result<()> {
    let mut fields = Vec::new();
    for (tag, value) in [(b'S', "FATAL"), (b'V', "FATAL"), (b'C', sqlstate), (b'M', message)] {
        fields.push(tag);
        fields.extend_from_slice(value.as_bytes());
        fields.push(0);
    }
    fields.push(0);
    let mut frame = vec![b'E'];
    frame.extend_from_slice(&((fields.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(&fields);
    client.write_all(&frame).await.map_err(io)?;
    client.flush().await.map_err(io)
}

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/gateway-io", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_rustls::{TlsConnector, rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject}};

    // The repository's test certificate predates subject alternative names.
    // Pin it for this transport test and still verify the handshake signatures.
    #[derive(Debug)]
    struct PinnedCertificate(CertificateDer<'static>);

    impl rustls::client::danger::ServerCertVerifier for PinnedCertificate {
        fn verify_server_cert(&self, certificate: &CertificateDer<'_>, _: &[CertificateDer<'_>], _: &ServerName<'_>, _: &[u8], _: rustls::pki_types::UnixTime) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            if certificate.as_ref() != self.0.as_ref() {
                return Err(rustls::Error::General("Unexpected gateway test certificate".into()));
            }
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(&self, message: &[u8], certificate: &CertificateDer<'_>, signature: &rustls::DigitallySignedStruct) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls12_signature(message, certificate, signature, &rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms)
        }

        fn verify_tls13_signature(&self, message: &[u8], certificate: &CertificateDer<'_>, signature: &rustls::DigitallySignedStruct) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls13_signature(message, certificate, signature, &rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms)
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms.supported_schemes()
        }
    }

    #[tokio::test]
    async fn reconnect_refuses_a_different_deployment_with_the_same_name() {
        let directory = rtdb_directory::MemoryDirectory::new();
        directory.create_deployment(DeploymentRecord::new("shop", "acme", Vec::new(), 0)).await.unwrap();
        let router = GatewayRouter {
            directory, name: "shop".into(), deployment: Some("original".into()),
            read_only: false, snapshot: false, options: GatewayOptions::default(), heartbeats: Arc::new(Heartbeats::default()),
            connection_index: 0,
        };
        let result = crate::session::Router::route(&router).await;
        assert_eq!(result.unwrap_err().code, "rtdb.multi/install-identity");
    }

    #[tokio::test]
    async fn a_redirect_uses_the_new_holders_confirmed_endpoint_and_epoch() {
        use rtdb_directory::{MemberPool, MemberRole, MemberState};

        struct Answer(Response);

        #[async_trait::async_trait]
        impl control::Handler for Answer {
            async fn handle(&self, request: Request) -> Response {
                assert!(matches!(request, Request::Activate { name } if name == "shop"));
                self.0.clone()
            }
        }

        let directory = rtdb_directory::MemoryDirectory::new();
        let source_control = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_control = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hinted = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ready = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_address = target_control.local_addr().unwrap();
        let member = |id: &str, control: &TcpListener| MemberRecord {
            id: MemberId(id.into()),
            role: MemberRole::Node,
            address: hinted.local_addr().unwrap().to_string(),
            control_address: Some(control.local_addr().unwrap().to_string()),
            availability_zone: "a".into(),
            engine_version: "0".into(),
            lease_expires_at_ms: now_ms() + 60_000,
            state: MemberState::Eligible,
            drain_target_engine: None,
            pool: MemberPool::General,
            storage_namespaces: None,
            version: 0,
        };
        let source = directory.register_member(member("source", &source_control)).await.unwrap();
        let target = directory.register_member(member("target", &target_control)).await.unwrap();
        let mut deployment = DeploymentRecord::new("shop", "acme", Vec::new(), 0);
        deployment.holder = Some(source.id);
        deployment.epoch = 1;
        let deployment = directory.create_deployment(deployment).await.unwrap();

        let stop = CancellationToken::new();
        let source = tokio::spawn(control::serve(source_control, Arc::new(Answer(Response::Redirect {
            holder: target.id.to_string(), address: hinted.local_addr().unwrap().to_string(),
        })), control::ControlKey::default(), stop.clone()));
        let target = tokio::spawn(control::serve(target_control, Arc::new(Answer(Response::Held {
            address: ready.local_addr().unwrap().to_string(), epoch: 7,
        })), control::ControlKey::default(), stop.clone()));
        let router = GatewayRouter {
            directory, name: deployment.name.clone(), deployment: Some(deployment.id),
            read_only: false, snapshot: false, options: GatewayOptions::default(),
            heartbeats: Arc::new(Heartbeats::default()), connection_index: 0,
        };
        let (server, control, epoch, _) = route(&router, false).await.unwrap();
        assert_eq!(server.peer_addr().unwrap(), ready.local_addr().unwrap());
        assert_eq!(control.as_deref(), Some(target_address.to_string().as_str()));
        assert_eq!(epoch, 7);

        stop.cancel();
        source.await.unwrap().unwrap();
        target.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn gateway_requires_tls_before_routing_startup() {
        let certificate = CertificateDer::from_pem_slice(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.crt")).unwrap();
        let key = PrivateKeyDer::from_pem_slice(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.key")).unwrap();
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions().unwrap().with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], key).unwrap();
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions().unwrap().dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedCertificate(certificate)))
            .with_no_client_auth();
        let tls = Arc::new(server);
        let parameters = BTreeMap::from([("database".into(), "tenant".into())]);
        let message = encode_startup(&parameters);

        let (mut socket, server) = tokio::io::duplex(4096);
        socket.write_all(&message).await.unwrap();
        assert!(read_startup(Box::new(server), Some(Arc::clone(&tls))).await.unwrap().is_none());
        assert_eq!(socket.read_u8().await.unwrap(), b'E');

        let (mut socket, server) = tokio::io::duplex(4096);
        let read = tokio::spawn(async move { read_startup(Box::new(server), Some(tls)).await.unwrap().unwrap().1 });
        socket.write_i32(8).await.unwrap();
        socket.write_i32(SSL_REQUEST).await.unwrap();
        assert_eq!(socket.read_u8().await.unwrap(), b'S');
        let mut socket = TlsConnector::from(Arc::new(client))
            .connect(ServerName::try_from("xtdb.example.com").unwrap(), socket).await.unwrap();
        socket.write_all(&message).await.unwrap();
        socket.flush().await.unwrap();
        assert_eq!(read.await.unwrap().parameters, parameters);
    }

    #[tokio::test]
    async fn simultaneous_sessions_share_backend_heartbeats_until_the_last_disconnect() {
        let heartbeats = Arc::new(Heartbeats::default());
        let options = GatewayOptions::default();
        let first = heartbeats.register("shop", "old".to_owned(), &options);
        let second = heartbeats.register("shop", "old".to_owned(), &options);
        let moved = heartbeats.register("shop", "new".to_owned(), &options);
        let other = heartbeats.register("other-shop", "old".to_owned(), &options);
        assert!(Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(&first, &moved));
        assert!(!Arc::ptr_eq(&first, &other));
        assert_eq!(heartbeats.0.lock().len(), 3);

        drop(first);
        assert_eq!(heartbeats.0.lock().len(), 3);
        drop(second);
        let active = heartbeats.0.lock();
        assert!(!active.contains_key(&("shop".to_owned(), "old".to_owned())));
        assert_eq!(active.len(), 2);
        drop(active);
        drop((moved, other));
        assert!(heartbeats.0.lock().is_empty());
    }
}
