//! The gateway: terminates a client's pgwire startup, finds or activates the deployment's holder, and splices the connection through to it (`RouteRead`, `ActivateOnDemand`, `ReclaimOnDemand`).
//!
//! Routing never authorises: a node may refuse or redirect, and the gateway walks on.

use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use rtdb_directory::{DeploymentRecord, Directory, Lifecycle, MemberId, MemberRecord, MemberRole, effective_holder, now_ms};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use crate::control::{self, Request, Response};

const SSL_REQUEST: i32 = 80877103;
const GSSENC_REQUEST: i32 = 80877104;
const CANCEL_REQUEST: i32 = 80877102;
const PROTOCOL_3: i32 = 196608;
const MAXIMUM_STARTUP_BYTES: usize = 10_000;
const SNAPSHOT_PARAMETER: &str = "rtdb.read";

#[derive(Clone)]
pub struct GatewayOptions {
    /// The cell map and this gateway's cell, so a connection for a deployment living elsewhere is forwarded there.
    pub cells: Option<(Arc<dyn crate::cells::CellMap>, String)>,
    pub control_timeout: Duration,
    pub startup_timeout: Duration,
    /// How often an open connection tells its holder it is in use.
    pub touch_interval: Duration,
    /// The cell's secret, with which the gateway mints a capability for each reconnection of an authenticated session.
    pub capabilities: Option<Arc<crate::capability::CellSecret>>,
}

impl Default for GatewayOptions {
    fn default() -> Self { Self { cells: None, control_timeout: Duration::from_secs(35), startup_timeout: crate::session::STARTUP_WITHIN, touch_interval: Duration::from_secs(10), capabilities: None } }
}

pub async fn serve(listener: TcpListener, directory: Arc<dyn Directory>, options: GatewayOptions, cancellation: CancellationToken) -> Result<()> {
    let heartbeats = Arc::new(Heartbeats::default());
    loop {
        let (client, _) = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            accepted = listener.accept() => accepted.map_err(io)?,
        };
        let directory = Arc::clone(&directory);
        let options = options.clone();
        let heartbeats = Arc::clone(&heartbeats);
        let cancellation = cancellation.child_token();
        tokio::spawn(async move {
            tokio::select! {
                _ = cancellation.cancelled() => {}
                _ = connection(client, directory, options, heartbeats) => {}
            }
        });
    }
}

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

async fn connection(mut client: TcpStream, directory: Arc<dyn Directory>, options: GatewayOptions, heartbeats: Arc<Heartbeats>) -> Result<()> {
    let startup_deadline = tokio::time::Instant::now() + options.startup_timeout;
    let Ok(startup) = tokio::time::timeout_at(startup_deadline, read_startup(&mut client)).await else { return Ok(()); };
    let Some(startup) = startup? else { return Ok(()); };
    let Some(name) = startup.parameters.get("database").or_else(|| startup.parameters.get("user")).cloned() else {
        return refuse(&mut client, "3D000", "The startup message names no database").await;
    };
    let Ok(remote_gateway) = tokio::time::timeout_at(startup_deadline, elsewhere(&directory, &name, &options)).await else {
        return refuse(&mut client, "57P03", "Timed out looking up the deployment's cell").await;
    };
    if let Some(gateway) = remote_gateway {
        let forward = tokio::time::timeout_at(startup_deadline, async {
            let mut forward = connect(&gateway).await?;
            forward.write_all(&startup.bytes).await.map_err(io)?;
            Ok::<_, Anomaly>(forward)
        }).await;
        let mut forward = match forward {
            Ok(Ok(forward)) => forward,
            Ok(Err(error)) => return refuse(&mut client, "08006", error.message.as_deref().unwrap_or_default()).await,
            Err(_) => return refuse(&mut client, "57P03", "Timed out forwarding startup to the deployment's cell").await,
        };
        return tokio::io::copy_bidirectional(&mut client, &mut forward).await.map(|_| ()).map_err(io);
    }
    let router = Arc::new(GatewayRouter { directory: Arc::clone(&directory), name: name.clone(), read_only: startup.read_only(), snapshot: startup.snapshot(), options: options.clone(), heartbeats });
    let (server, control, epoch, deployment) = match tokio::time::timeout_at(startup_deadline, router.route_with_record()).await {
        Ok(Ok(route)) => route,
        Ok(Err(error)) => return refuse(&mut client, "08006", error.message.as_deref().unwrap_or(error.code.as_ref())).await,
        Err(_) => return refuse(&mut client, "57P03", "Timed out routing startup to the deployment's holder").await,
    };
    let grants = options.capabilities.clone().map(|secret| crate::session::Grants { secret, parameters: startup.parameters.clone(), read_only: startup.read_only() });
    let keyed = !startup.read_only();
    let session = crate::session::Session::new(Bytes::from(startup.bytes), router, directory, deployment, grants, keyed);
    session.run_before(client, server, control, epoch, startup_deadline).await
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

struct GatewayRouter { directory: Arc<dyn Directory>, name: String, read_only: bool, snapshot: bool, options: GatewayOptions, heartbeats: Arc<Heartbeats> }

type HeartbeatTarget = (String, String);

#[derive(Default)]
struct Heartbeats(Mutex<BTreeMap<HeartbeatTarget, Weak<Heartbeat>>>);

struct Heartbeat {
    registry: Weak<Heartbeats>,
    target: HeartbeatTarget,
    _task: AbortOnDropHandle<()>,
}

impl Heartbeats {
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
    async fn route_with_record(&self) -> Result<(TcpStream, Option<String>, u64, String)> {
        control::retry_activation(self.options.control_timeout, || {
            route(&self.directory, &self.name, self.read_only, self.snapshot, &self.options)
        }).await
    }
}

#[async_trait::async_trait]
impl crate::session::Router for GatewayRouter {
    async fn route(&self) -> Result<(TcpStream, Option<String>, u64)> {
        let (stream, control, epoch, _) = self.route_with_record().await?;
        Ok((stream, control, epoch))
    }

    fn start_touching(&self, address: String) -> Option<Arc<dyn Send + Sync>> {
        Some(self.heartbeats.register(&self.name, address, &self.options))
    }
}

/// The open connection to the deployment's holder, and that holder's control address.
async fn route(directory: &Arc<dyn Directory>, name: &str, read_only: bool, snapshot: bool, options: &GatewayOptions) -> Result<(TcpStream, Option<String>, u64, String)> {
    let record = directory.deployment_by_name(name).await?
        .ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-deployment", format!("No deployment is named {name}")))?;
    match record.lifecycle {
        Lifecycle::Live | Lifecycle::Quarantined => {}
        Lifecycle::Moving => return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/moving", format!("{name} is moving to another cell; retry shortly"))),
        Lifecycle::Deleting | Lifecycle::Deleted => return Err(Anomaly::incorrect("rtdb.multi/deleted", format!("{name} has been deleted"))),
    }
    let members: BTreeMap<MemberId, MemberRecord> = directory.members().await?.into_iter().map(|member| (member.id.clone(), member)).collect();
    if snapshot && record.checkpoint.is_some() && record.lifecycle == Lifecycle::Live {
        for reader in snapshot_readers(&record, &members) {
            let Some(control_address) = &reader.control_address else { continue; };
            if let Ok(Response::Subscribed { address }) = control::call(control_address, &Request::Snapshot { name: name.to_owned() }, options.control_timeout, &control::ControlKey(options.capabilities.clone())).await {
                if let Ok(stream) = connect(&address).await { return Ok((stream, Some(control_address.clone()), record.epoch, record.id.clone())); }
            }
        }
    }
    if read_only && record.read_width > 1 && record.lifecycle == Lifecycle::Live {
        for replica in replicas(&record, &members) {
            let Some(control_address) = &replica.control_address else { continue; };
            if let Ok(Response::Subscribed { address }) = control::call(control_address, &Request::Subscribe { name: name.to_owned() }, options.control_timeout, &control::ControlKey(options.capabilities.clone())).await {
                if let Ok(stream) = connect(&address).await { return Ok((stream, Some(control_address.clone()), record.epoch, record.id.clone())); }
            }
        }
    }
    let mut last = None;
    let mut retryable = None;
    let mut refused = None;
    for candidate in candidates(&record, &members) {
        let Some(control_address) = &candidate.control_address else { continue; };
        match control::call(control_address, &Request::Activate { name: name.to_owned() }, options.control_timeout, &control::ControlKey(options.capabilities.clone())).await {
            Ok(Response::Held { address, epoch }) => return Ok((connect(&address).await?, Some(control_address.clone()), epoch, record.id.clone())),
            Ok(Response::Redirect { holder, address }) => {
                let control = members.get(&MemberId(holder)).and_then(|member| member.control_address.clone());
                return Ok((connect(&address).await?, control, record.epoch, record.id.clone()));
            }
            Ok(Response::Refused { code, message }) if control::retryable_activation(&code) => {
                retryable = Some(Anomaly::new(AnomalyCategory::Unavailable, code, message));
            }
            Ok(Response::Refused { code, message }) if code == control::NOT_CLAIMABLE => {
                last = Some(Anomaly::new(AnomalyCategory::Unavailable, code, message));
            }
            Ok(Response::Refused { code, message }) => refused = Some(Anomaly::new(AnomalyCategory::Unavailable, code, message)),
            Ok(Response::Done | Response::Subscribed { .. } | Response::Published { .. } | Response::Fragment { .. }) => {}
            Err(error) => last = Some(error),
        }
    }
    Err(refused.or(retryable).or(last).unwrap_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/no-node", "No node can serve this deployment")))
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

/// The nodes that may serve the deployment's reads beside its holder: the ranks after the holder's, within its read width.
fn replicas<'a>(record: &DeploymentRecord, members: &'a BTreeMap<MemberId, MemberRecord>) -> Vec<&'a MemberRecord> {
    let now = now_ms();
    let holder = effective_holder(record, members, now).cloned();
    let nodes: Vec<&MemberRecord> = members.values().filter(|member| record.may_place_on(member, now)).collect();
    rtdb_affinity::rank(&record.storage_identity(), &nodes, |member| member.id.as_str()).into_iter().copied()
        .filter(|member| holder.as_ref() != Some(&member.id)).take(record.read_width.saturating_sub(1) as usize).collect()
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

/// Answers SSL and GSS encryption requests with "not supported" until the client sends a startup message.
/// Returns None for a cancel request, which this gateway does not forward.
async fn read_startup(client: &mut TcpStream) -> Result<Option<Startup>> {
    loop {
        let length = client.read_i32().await.map_err(io)?;
        let length = usize::try_from(length).ok().filter(|length| (8..=MAXIMUM_STARTUP_BYTES).contains(length))
            .ok_or_else(|| Anomaly::incorrect("rtdb.multi/startup", "Startup message length is out of range"))?;
        let mut body = vec![0; length - 4];
        client.read_exact(&mut body).await.map_err(io)?;
        let code = i32::from_be_bytes(body[..4].try_into().expect("four bytes"));
        match code {
            SSL_REQUEST | GSSENC_REQUEST => { client.write_all(b"N").await.map_err(io)?; }
            CANCEL_REQUEST => return Ok(None),
            PROTOCOL_3 => {
                let mut parameters = BTreeMap::new();
                let mut fields = body[4..].split(|byte| *byte == 0).map(|field| String::from_utf8_lossy(field).into_owned());
                while let (Some(key), Some(value)) = (fields.next(), fields.next()) {
                    if key.is_empty() { break; }
                    parameters.insert(key, value);
                }
                // Only a gateway grants capabilities: one a client sends is dropped.
                parameters.remove(rtdb_pgwire::CAPABILITY_PARAMETER);
                return Ok(Some(Startup { bytes: encode_startup(&parameters), parameters }));
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

async fn refuse(client: &mut TcpStream, sqlstate: &str, message: &str) -> Result<()> {
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
    client.write_all(&frame).await.map_err(io)
}

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/gateway-io", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::{Arc, GatewayOptions, Heartbeats};

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
