use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::time::Duration;
use std::sync::{Arc, Mutex, Weak};
use rtdb_auth::Authenticator;
use rtdb_node::Node;
use rtdb_types::{Anomaly, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use crate::{ServerConfig, error::network};

pub const SERVING_PROBE: [u8; 8] = *b"RTDB\0\0\0\x01";
pub const SERVING_STARTUP: [u8; 8] = *b"RTDB\0\0\0\x02";
pub const SERVING_REPLY: [u8; 4] = *b"RTOK";
/// Decides the protocol from the connection's first bytes. A serving probe or
/// startup is eight bytes and every PostgreSQL first message is at least eight,
/// so reading until eight bytes or the first byte matching neither prefix never
/// waits on bytes the client holds back. PostgreSQL bytes read here are
/// replayed to the connection. Inspection runs inside the startup deadline.
async fn inspect_startup(mut stream: TcpStream, address: SocketAddr, deadline: Duration) -> Option<(Rewound, SocketAddr)> {
    let inspect = async {
        let mut bytes = [0; SERVING_PROBE.len()];
        let mut filled = 0;
        while filled < bytes.len() {
            let read = stream.read(&mut bytes[filled..]).await?;
            if read == 0 { return Ok::<_, std::io::Error>(None); }
            filled += read;
            if bytes[..filled] != SERVING_PROBE[..filled] && bytes[..filled] != SERVING_STARTUP[..filled] {
                return Ok(Some(bytes[..filled].to_vec()));
            }
        }
        stream.set_nodelay(true)?;
        stream.write_all(&SERVING_REPLY).await?;
        Ok((bytes == SERVING_STARTUP).then(Vec::new))
    };
    match tokio::time::timeout(deadline, inspect).await {
        Ok(Ok(Some(prefix))) => Some((Rewound { prefix, offset: 0, stream }, address)),
        Ok(Ok(None) | Err(_)) | Err(_) => None,
    }
}

/// A TCP stream whose first bytes, already read by startup inspection, are
/// read again before the socket's own.
pub(crate) struct Rewound { prefix: Vec<u8>, offset: usize, stream: TcpStream }
impl Rewound {
    fn new(stream: TcpStream) -> Self { Self { prefix: Vec::new(), offset: 0, stream } }
}
impl tokio::io::AsyncRead for Rewound {
    fn poll_read(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &mut tokio::io::ReadBuf<'_>) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.offset < this.prefix.len() {
            let count = buf.remaining().min(this.prefix.len() - this.offset);
            buf.put_slice(&this.prefix[this.offset..this.offset + count]);
            this.offset += count;
            return std::task::Poll::Ready(Ok(()));
        }
        tokio::io::AsyncRead::poll_read(std::pin::Pin::new(&mut this.stream), cx, buf)
    }
}
impl tokio::io::AsyncWrite for Rewound {
    fn poll_write(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &[u8]) -> std::task::Poll<std::io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(std::pin::Pin::new(&mut self.get_mut().stream), cx, buf)
    }
    fn poll_write_vectored(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, bufs: &[std::io::IoSlice<'_>]) -> std::task::Poll<std::io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write_vectored(std::pin::Pin::new(&mut self.get_mut().stream), cx, bufs)
    }
    fn is_write_vectored(&self) -> bool { tokio::io::AsyncWrite::is_write_vectored(&self.stream) }
    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(std::pin::Pin::new(&mut self.get_mut().stream), cx)
    }
    fn poll_shutdown(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(std::pin::Pin::new(&mut self.get_mut().stream), cx)
    }
}

#[derive(Clone, Debug, Default)]
pub struct ServerStatus { pub active_connections: usize, pub pending_connections: usize, pub accepted_connections: u64, pub refused_connections: u64, pub startup_probe_overflows: u64, pub stopped: bool }

#[derive(Clone)]
pub struct ServerHandle { address: SocketAddr, shutdown: CancellationToken, status: watch::Receiver<ServerStatus>, memory: Arc<rtdb_columnar::MemoryBudget>, registry: Weak<CancellationRegistry> }
impl ServerHandle {
    pub fn address(&self) -> SocketAddr { self.address }
    pub fn shutdown(&self) { self.shutdown.cancel(); }
    pub fn status(&self) -> ServerStatus { self.status.borrow().clone() }
    pub fn watch_status(&self) -> watch::Receiver<ServerStatus> { self.status.clone() }
    /// The provider retains only published connection counts and their allocation ownership.
    pub fn metrics_provider(&self) -> Result<Arc<dyn rtdb_plan::CatalogMetrics>> {
        let reservation = self.memory.reserve(512)?;
        Ok(Arc::new(ConnectionMetrics { status: self.status.clone(), registry: self.registry.clone(), memory: Arc::clone(&self.memory), _reservation: reservation }))
    }
}
struct ConnectionMetrics { status: watch::Receiver<ServerStatus>, registry: Weak<CancellationRegistry>, memory: Arc<rtdb_columnar::MemoryBudget>, _reservation: Arc<rtdb_columnar::MemoryReservation> }
impl rtdb_plan::CatalogMetrics for ConnectionMetrics {
    fn metrics(&self) -> Result<rtdb_plan::CatalogRows<rtdb_plan::CatalogMetric>> {
        let reservation = self.memory.reserve(2048)?;
        let status = self.status.borrow();
        let active = self.registry.upgrade().map_or(0, |registry| registry.slots.lock().unwrap_or_else(|error| error.into_inner()).len());
        let rows = [("pgwire.active_connections", active), ("pgwire.pending_connections", status.pending_connections)].into_iter()
            .map(|(name, value)| rtdb_plan::CatalogMetric::Gauge { name: name.into(), tags: BTreeMap::new(), unit: None, value: value as f64 }).collect();
        Ok(rtdb_plan::CatalogRows::new(rows, reservation))
    }
}

pub struct ServerDriver { listener: TcpListener, config: Arc<ServerConfig>, node: Node, auth: Arc<Authenticator>, shutdown: CancellationToken, status: watch::Sender<ServerStatus>, registry: Arc<CancellationRegistry> }
impl ServerDriver {
    pub async fn bind(config: ServerConfig, node: Node, auth: Arc<Authenticator>) -> Result<(ServerHandle, Self)> {
        if config.maximum_connections == 0 || config.maximum_message_bytes < 4 || config.maximum_message_bytes > 0x3fff_ffff || config.authentication_timeout.is_zero() { return Err(Anomaly::incorrect("xtdb.pgwire/configuration", "PostgreSQL capacities and authentication timeout must be positive")); }
        if config.require_tls && config.tls.is_none() {
            return Err(Anomaly::incorrect("xtdb.pgwire/configuration", "This listener requires a TLS identity"));
        }
        let listener = TcpListener::bind(config.address).await.map_err(network)?;
        let address = listener.local_addr().map_err(network)?;
        let shutdown = node.cancellation().child_token();
        let (status, receiver) = watch::channel(ServerStatus::default());
        let registry = Arc::new(CancellationRegistry { slots: Mutex::new(BTreeMap::new()), observer: node.execution_observer() });
        let handle = ServerHandle { address, shutdown: shutdown.clone(), status: receiver, memory: Arc::clone(node.memory()), registry: Arc::downgrade(&registry) };
        Ok((handle, Self { listener, config: Arc::new(config), node, auth, shutdown, status, registry }))
    }
    /// Request shutdown through the handle, then await this future through every connection join.
    pub async fn run(self) -> Result<()> {
        let cancellation = CancellationToken::new();
        let draining = CancellationToken::new();
        let mut tasks = JoinSet::new(); let mut inspections = JoinSet::new(); let mut pending = VecDeque::new(); let mut next_process = 0i32;
        let mut state = ServerStatus::default(); let mut failure = None;
        'accept: loop {
            while tasks.len() < self.config.maximum_connections {
                let Some((stream, address)) = pending.pop_front() else { break; };
                let Some(process) = next_process.checked_add(1) else { failure = Some(Anomaly::fault("xtdb.pgwire/process-id", "PostgreSQL connection identifier exhausted")); break 'accept; }; next_process = process;
                let connection = crate::connection::ConnectionDriver::new(next_process, stream, address, false, self.node.clone(), Arc::clone(&self.auth), Arc::clone(&self.config), Arc::clone(&self.registry), cancellation.child_token(), draining.clone());
                tasks.spawn(connection.run());
            }
            state.active_connections = tasks.len(); state.pending_connections = pending.len(); self.status.send_replace(state.clone());
            let connection = tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => break,
                completion = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Err(error)) = completion { failure = Some(Anomaly::fault("xtdb.pgwire/connection-task", error.to_string())); break; }
                    None
                },
                inspection = inspections.join_next(), if !inspections.is_empty() => match inspection {
                    Some(Err(error)) => { failure = Some(Anomaly::fault("xtdb.pgwire/startup-inspection", error.to_string())); break; }
                    Some(Ok(inspected)) => inspected,
                    None => None,
                },
                accepted = self.listener.accept() => match accepted {
                    Ok((stream, address)) => {
                        state.accepted_connections = state.accepted_connections.saturating_add(1);
                        // An inspecting connection holds a pending place, so inspection
                        // is bounded by the same capacity that refuses connections.
                        let pending_places = pending.len() + inspections.len();
                        if !self.config.gateway_session_state {
                            Some((Rewound::new(stream), address))
                        } else if tasks.len() + pending_places < self.config.maximum_connections.saturating_add(self.config.pending_connections) {
                            inspections.spawn(inspect_startup(stream, address, self.config.authentication_timeout));
                            None
                        } else {
                            state.refused_connections = state.refused_connections.saturating_add(1);
                            if let Some(observer) = self.node.execution_observer() { observer.connection_event(rtdb_plan::ConnectionEvent::Refused); }
                            None
                        }
                    }
                    Err(error) => { failure = Some(network(error)); break; },
                },
            };
            if let Some((stream, address)) = connection {
                if tasks.len() < self.config.maximum_connections || pending.len() < self.config.pending_connections {
                    if let Err(error) = stream.stream.set_nodelay(true) { failure = Some(network(error)); break; }
                    pending.push_back((stream, address));
                } else {
                    state.refused_connections = state.refused_connections.saturating_add(1);
                    if let Some(observer) = self.node.execution_observer() { observer.connection_event(rtdb_plan::ConnectionEvent::Refused); }
                }
            }
        }
        drop(self.listener); pending.clear(); inspections.shutdown().await; draining.cancel();
        let deadline = tokio::time::sleep(self.config.drain_timeout); tokio::pin!(deadline);
        while !tasks.is_empty() {
            tokio::select! { _ = &mut deadline => break, completion = tasks.join_next() => if let Some(Err(error)) = completion { if failure.is_none() { failure = Some(Anomaly::fault("xtdb.pgwire/connection-task", error.to_string())); } } }
        }
        cancellation.cancel();
        while let Some(completion) = tasks.join_next().await { if let Err(error) = completion { if failure.is_none() { failure = Some(Anomaly::fault("xtdb.pgwire/connection-task", error.to_string())); } } }
        state.active_connections = 0; state.pending_connections = 0; state.stopped = true; self.status.send_replace(state);
        failure.map_or(Ok(()), Err)
    }
}

/// Serve one read-only session on an established TLS stream. The caller
/// authorizes entry into the stream. PgWire also applies its node authenticator.
pub async fn serve_read_only_tls(
    stream: tokio_rustls::server::TlsStream<rtdb_query::DeliveryIo<tokio::net::TcpStream>>,
    node: Node,
    auth: Arc<Authenticator>,
    parameters: BTreeMap<String, String>,
    cancellation: CancellationToken,
) -> Result<()> {
    let address = stream.get_ref().0.get_ref().peer_addr().map_err(network)?;
    let mut config = ServerConfig { read_only: true, require_tls: true, ..Default::default() };
    config.parameters.extend(parameters);
    let registry = Arc::new(CancellationRegistry {
        slots: Mutex::new(BTreeMap::new()),
        observer: node.execution_observer(),
    });
    crate::connection::ConnectionDriver::new(
        1, stream, address, true, node, auth, Arc::new(config),
        registry, cancellation, CancellationToken::new(),
    ).run().await
}

#[derive(Default)]
pub(crate) struct CancellationRegistry { slots: Mutex<BTreeMap<i32, Weak<CancellationSlot>>>, observer: Option<Arc<dyn rtdb_plan::ExecutionObserver>> }
impl CancellationRegistry {
    pub fn register(self: &Arc<Self>, process: i32) -> Registration { let slot = Arc::new(CancellationSlot::default()); self.slots.lock().unwrap_or_else(|error| error.into_inner()).insert(process, Arc::downgrade(&slot)); Registration { process, slot, registry: Arc::clone(self) } }
    pub fn cancel(&self, process: i32) { if let Some(slot) = self.slots.lock().unwrap_or_else(|error| error.into_inner()).get(&process).and_then(Weak::upgrade) { if let Some(observer) = &self.observer { observer.connection_event(rtdb_plan::ConnectionEvent::Cancelled); } slot.cancel(); } }
}
pub(crate) struct Registration { process: i32, pub slot: Arc<CancellationSlot>, registry: Arc<CancellationRegistry> }
impl Drop for Registration { fn drop(&mut self) { self.registry.slots.lock().unwrap_or_else(|error| error.into_inner()).remove(&self.process); } }
#[derive(Default)]
struct CancellationState { current: Option<CancellationToken>, peer_closed: bool }
#[derive(Default)]
pub(crate) struct CancellationSlot(Mutex<CancellationState>);
impl CancellationSlot {
    pub fn enter(self: &Arc<Self>, token: &CancellationToken) -> ActiveQuery { let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner()); if state.peer_closed { token.cancel(); } state.current = Some(token.clone()); ActiveQuery(Arc::clone(self)) }
    fn cancel(&self) { if let Some(token) = self.0.lock().unwrap_or_else(|error| error.into_inner()).current.as_ref() { token.cancel(); } }
    pub fn peer_closed(&self) { let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner()); state.peer_closed = true; if let Some(token) = &state.current { token.cancel(); } }
}
pub(crate) struct ActiveQuery(Arc<CancellationSlot>);
impl Drop for ActiveQuery { fn drop(&mut self) { self.0.0.lock().unwrap_or_else(|error| error.into_inner()).current = None; } }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_gauges_count_authenticated_registrations_and_retained_pending_status() {
        use rtdb_plan::CatalogMetrics;
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let registry = Arc::new(CancellationRegistry::default());
        let registration = registry.register(1);
        let (_, status) = watch::channel(ServerStatus { active_connections: 3, pending_connections: 4, ..Default::default() });
        let provider = ConnectionMetrics { status, registry: Arc::downgrade(&registry), _reservation: memory.reserve(512).unwrap(), memory };
        let rows = provider.metrics().unwrap();
        assert!(rows.rows().iter().any(|metric| matches!(metric, rtdb_plan::CatalogMetric::Gauge { name, value, .. } if name == "pgwire.active_connections" && *value == 1.0)));
        assert!(rows.rows().iter().any(|metric| matches!(metric, rtdb_plan::CatalogMetric::Gauge { name, value, .. } if name == "pgwire.pending_connections" && *value == 4.0)));
        drop(registration);
        assert!(provider.metrics().unwrap().rows().iter().any(|metric| matches!(metric, rtdb_plan::CatalogMetric::Gauge { name, value, .. } if name == "pgwire.active_connections" && *value == 0.0)));
    }

    #[test]
    fn cancel_targets_only_the_registered_active_query_and_does_not_latch() {
        let registry = Arc::new(CancellationRegistry::default());
        let registration = registry.register(7);
        let first = CancellationToken::new(); let active = registration.slot.enter(&first);
        registry.cancel(8); assert!(!first.is_cancelled());
        registry.cancel(7); assert!(first.is_cancelled()); drop(active);
        let second = CancellationToken::new(); let active = registration.slot.enter(&second);
        assert!(!second.is_cancelled()); drop(registration);
        registry.cancel(7); assert!(!second.is_cancelled()); drop(active);
    }

    #[test]
    fn peer_disconnect_cancels_current_and_later_queued_query_work() {
        let slot = Arc::new(CancellationSlot::default());
        let first = CancellationToken::new(); let active = slot.enter(&first);
        slot.peer_closed(); assert!(first.is_cancelled()); drop(active);
        let queued = CancellationToken::new(); let _active = slot.enter(&queued); assert!(queued.is_cancelled());
    }
}
