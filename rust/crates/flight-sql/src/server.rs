use std::{future::Future, io::{self, IoSlice}, net::SocketAddr, pin::Pin, sync::{Arc, Mutex}, task::{Context, Poll}};
use futures::{StreamExt, task::AtomicWaker};
use rtdb_auth::Authenticator;
use rtdb_columnar::MemoryReservation;
use rtdb_node::{Clock, Node, SystemClock};
use rtdb_storage::sealing::{DeliveryDeadline, PlaintextDelivery};
use rtdb_types::{Anomaly, Result};
use tokio::{io::{AsyncRead, AsyncWrite, ReadBuf}, net::{TcpListener, TcpStream}, sync::{OwnedSemaphorePermit, Semaphore, watch}};
use tokio_rustls::{TlsAcceptor, rustls, server::TlsStream};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::{Connected, TcpConnectInfo};
use crate::{FlightConfig, action_route::ActionRoute, service::Service};
use crate::config::{AUDIENCE_H2_CONNECTION_WINDOW, AUDIENCE_H2_STREAM_WINDOW, AUDIENCE_H2_STREAMS, AUDIENCE_HOLDER_TRANSPORT_BYTES, AUDIENCE_REPLY_BYTES};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ServerStatus { pub active_calls: usize, pub sessions: usize, pub transactions: usize, pub prepared_statements: usize, pub stopped: bool }
#[derive(Clone)]
pub struct ServerHandle { address: SocketAddr, cancellation: CancellationToken, status: watch::Receiver<ServerStatus> }
impl ServerHandle {
    pub fn address(&self) -> SocketAddr { self.address }
    pub fn shutdown(&self) { self.cancellation.cancel(); }
    pub fn status(&self) -> ServerStatus { self.status.borrow().clone() }
    pub fn watch_status(&self) -> watch::Receiver<ServerStatus> { self.status.clone() }
}
pub struct ServerDriver { listener: Option<TcpListener>, service: Service, tls: Option<TlsAcceptor> }

pub(crate) enum Binding {
    Ordinary,
    Standalone,
    #[cfg(feature = "multi")]
    Follower(crate::follower::FollowerRoute),
}

impl ServerDriver {
    /// Binds the insecure Flight endpoint without spawning a task.
    /// The owner runs this driver and cancels its handle, then joins `run`, before closing the node.
    pub async fn bind(config: FlightConfig, node: Node, authenticator: Arc<Authenticator>) -> Result<(ServerHandle, Self)> {
        Self::bind_with_clock(config, node, authenticator, Arc::new(SystemClock)).await
    }
    /// Binds the private TLS listener for the installed native database.
    pub async fn bind_standalone(config: FlightConfig, node: Node, authenticator: Arc<Authenticator>, tls: rustls::ServerConfig) -> Result<(ServerHandle, Self)> {
        if !config.address.ip().is_loopback() || config.owned_native_actions || config.hosted_controls.is_some()
            || config.audience.is_some() || config.forwarded_peer.is_some() || config.playground
        {
            return Err(Anomaly::incorrect("rtdb.flight-sql/standalone", "Installed Flight requires a private native listener"));
        }
        config.validate(Some("xtdb"))?;
        Self::bind_private_tls(config, node, authenticator, tls, Binding::Standalone).await
    }

    pub async fn bind_loopback_tls(config: FlightConfig, node: Node, authenticator: Arc<Authenticator>, tls: rustls::ServerConfig) -> Result<(ServerHandle, Self)> {
        if !config.address.ip().is_loopback() {
            return Err(Anomaly::incorrect("rtdb.flight-sql/development-tls", "Development Flight TLS requires a loopback address"));
        }
        config.validate(None)?;
        Self::bind_private_tls(config, node, authenticator, tls, Binding::Ordinary).await
    }

    async fn bind_private_tls(config: FlightConfig, node: Node, authenticator: Arc<Authenticator>, mut tls: rustls::ServerConfig, binding: Binding) -> Result<(ServerHandle, Self)> {
        tls.alpn_protocols = vec![b"h2".to_vec()];
        let listener = TcpListener::bind(config.address).await.map_err(network)?;
        Self::bind_listener(config, node, authenticator, Arc::new(SystemClock), Some(TlsAcceptor::from(Arc::new(tls))), listener, binding)
    }
    pub async fn bind_with_clock(config: FlightConfig, node: Node, authenticator: Arc<Authenticator>, clock: Arc<dyn Clock>) -> Result<(ServerHandle, Self)> {
        config.validate(None)?;
        let listener = TcpListener::bind(config.address).await.map_err(network)?;
        Self::bind_listener(config, node, authenticator, clock, None, listener, Binding::Ordinary)
    }
    /// Binds TLS Flight for one positively assigned Multi follower.
    /// Run the driver once and join it after shutting down its handle.
    #[cfg(feature = "multi")]
    pub async fn bind_follower(
        address: SocketAddr, node: Node, authenticator: Arc<Authenticator>, mut tls: rustls::ServerConfig,
        assignment: crate::NativeFollowerAssignment, attachment: &Arc<rtdb_node::Database>, reader: rtdb_node::MultiFollower,
    ) -> Result<(ServerHandle, Self)> {
        if !address.ip().is_loopback() {
            return Err(Anomaly::incorrect("rtdb/follower-assignment", "The follower listener requires a loopback address"));
        }
        let config = FlightConfig { address, ..FlightConfig::default() };
        config.validate(None)?;
        let listener = TcpListener::bind(address).await.map_err(network)?;
        let endpoint = listener.local_addr().map_err(network)?;
        let route = crate::follower::FollowerRoute::new(assignment, endpoint, attachment, reader)?;
        route.current(&node)?;
        tls.alpn_protocols = vec![b"h2".to_vec()];
        Self::bind_listener(config, node, authenticator, Arc::new(SystemClock), Some(TlsAcceptor::from(Arc::new(tls))), listener, Binding::Follower(route))
    }
    fn bind_listener(config: FlightConfig, node: Node, authenticator: Arc<Authenticator>, clock: Arc<dyn Clock>, tls: Option<TlsAcceptor>, listener: TcpListener, binding: Binding) -> Result<(ServerHandle, Self)> {
        let address = listener.local_addr().map_err(network)?;
        let cancellation = node.cancellation().child_token();
        let (sender, receiver) = watch::channel(ServerStatus::default());
        let service = Service::new(node, authenticator, config, clock, cancellation.clone(), sender, binding)?;
        Ok((ServerHandle { address, cancellation, status: receiver }, Self { listener: Some(listener), service, tls }))
    }
    pub async fn run(mut self) -> Result<()> {
        let listener = self.listener.take().expect("unstarted Flight driver");
        let cancellation = self.service.0.cancellation.clone();
        let incoming_cancel = cancellation.clone();
        let audience = self.service.0.config.audience;
        let connections = audience.map(|audience| Arc::new(Semaphore::new(audience.permits())));
        let transport = self.service.0.audience.as_ref().map(|admission| Arc::clone(&admission.transport));
        let incoming = TcpListenerStream::new(listener).filter_map(move |result| {
            let admission = connections.as_ref().map(|permits| Arc::clone(permits).try_acquire_owned()).transpose();
            let result = match (result, admission) {
                (Err(error), _) => Some(Err(error)),
                (Ok(_), Err(_)) => None,
                (Ok(stream), Ok(admission)) => transport.as_ref().map(|transport| transport.reserve(AUDIENCE_HOLDER_TRANSPORT_BYTES))
                    .transpose().ok().map(|reservation| stream.set_nodelay(true).map(|()| ConnectionIo {
                        peer: stream.connect_info(),
                        stream: FlightEgressIo::new(stream, Arc::new(FlightEgress::default())),
                        _admission: admission, _transport: reservation,
                        cancellation: Box::pin(incoming_cancel.clone().cancelled_owned()),
                    })),
            };
            std::future::ready(result)
        });
        let maximum = self.service.0.config.maximum_message_bytes;
        let encoding = if audience.is_some() { maximum.min(AUDIENCE_REPLY_BYTES) } else { maximum };
        let server = ActionRoute::new(self.service.clone(), self.service.request_bytes(), encoding);
        let mut http = tonic::transport::Server::builder().http2_max_header_list_size(8192);
        if audience.is_some() {
            http = http.max_concurrent_streams(AUDIENCE_H2_STREAMS)
                .initial_stream_window_size(AUDIENCE_H2_STREAM_WINDOW)
                .initial_connection_window_size(AUDIENCE_H2_CONNECTION_WINDOW)
                .http2_adaptive_window(Some(false));
        }
        let server = http.add_service(crate::trace_scope::TraceScope(server));
        let stop = cancellation.clone().cancelled_owned();
        let tls = self.tls.take();
        let serving = async {
            match tls {
                Some(tls) => {
                    let incoming = incoming.map(|connection| {
                        let tls = tls.clone();
                        async move {
                            match connection {
                                Ok(connection) => TlsConnection::accept(tls, connection).await.map(Ok),
                                Err(error) => Some(Err(error)),
                            }
                        }
                    }).buffer_unordered(32).filter_map(std::future::ready);
                    server.serve_with_incoming_shutdown(incoming, stop).await
                }
                None => server.serve_with_incoming_shutdown(incoming, stop).await,
            }
        };
        tokio::pin!(serving);
        let mut sweep = tokio::time::interval(self.service.0.config.sweep_interval);
        let result = loop { tokio::select! { result = &mut serving => break result.map_err(network), _ = sweep.tick() => self.service.sweep() } };
        cancellation.cancel(); self.service.close();
        let mut status = self.service.0.status.subscribe();
        while status.borrow_and_update().active_calls != 0 { if status.changed().await.is_err() { break; } }
        self.service.0.status.send_modify(|status| status.stopped = true); result
    }
}
impl Drop for ServerDriver { fn drop(&mut self) { self.service.0.cancellation.cancel(); self.service.close(); } }
#[derive(Clone)]
pub(crate) struct FlightConnectInfo {
    pub(crate) peer: TcpConnectInfo,
    pub(crate) egress: Arc<FlightEgress>,
}

#[derive(Default)]
pub struct FlightEgress {
    state: Mutex<EgressState>,
    socket: AtomicWaker,
}

#[derive(Default)]
struct EgressState {
    cutoff: Option<DeliveryDeadline>,
    closed: bool,
    grants: Vec<PlaintextDelivery>,
}

impl FlightEgress {
    pub(crate) fn admit(&self, grant: PlaintextDelivery, relay: Option<DeliveryDeadline>) -> Result<()> {
        grant.check()?;
        if let (Some(grant), Some(relay)) = (grant.transport_deadline(), relay) && !grant.covers(relay) {
            return Err(network(expired()));
        }
        let Some(cutoff) = relay.or_else(|| grant.transport_deadline()) else {
            grant.observe_answer();
            return Ok(());
        };
        let mut state = self.state.lock().expect("Flight egress state");
        state.restrict(cutoff).map_err(network)?;
        grant.check()?;
        grant.observe_answer();
        state.grants.push(grant);
        drop(state);
        self.socket.wake();
        Ok(())
    }

    /// Retains the issuer's exclusive cutoff for downstream socket writes.
    pub fn restrict(&self, cutoff: DeliveryDeadline) -> io::Result<()> {
        self.state.lock().expect("Flight egress state").restrict(cutoff)?;
        self.socket.wake();
        Ok(())
    }

    fn cutoff(&self, cx: &mut Context<'_>) -> io::Result<Option<DeliveryDeadline>> {
        self.socket.register(cx.waker());
        let state = self.state.lock().expect("Flight egress state");
        if state.closed || state.cutoff.is_some_and(|cutoff| !cutoff.is_live())
            || state.grants.iter().any(|grant| grant.check().is_err()) { return Err(expired()); }
        Ok(state.cutoff)
    }

    fn close(&self) {
        let mut state = self.state.lock().expect("Flight egress state");
        state.closed = true;
        let grants = std::mem::take(&mut state.grants);
        drop(state);
        drop(grants);
    }
}

impl EgressState {
    fn restrict(&mut self, cutoff: DeliveryDeadline) -> io::Result<()> {
        if self.closed || !cutoff.is_live() || self.cutoff.is_some_and(|prior| !prior.is_live()) {
            return Err(expired());
        }
        self.cutoff = Some(self.cutoff.map_or(cutoff, |prior| prior.earlier(cutoff)));
        Ok(())
    }
}

fn expired() -> io::Error { io::Error::new(io::ErrorKind::TimedOut, "The Flight answer delivery deadline expired") }

/// Guards the connection after HTTP/2 accepts a protected reply into its own queues.
/// A multiplexed connection carries its earliest cutoff until it closes.
pub struct FlightEgressIo<W> {
    stream: W,
    egress: Arc<FlightEgress>,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<W> FlightEgressIo<W> {
    pub fn new(stream: W, egress: Arc<FlightEgress>) -> Self { Self { stream, egress, timer: None } }

    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if let Some(cutoff) = self.egress.cutoff(cx)? {
            let cutoff = cutoff.instant().into();
            let timer = self.timer.get_or_insert_with(|| Box::pin(tokio::time::sleep_until(cutoff)));
            if timer.deadline() != cutoff { timer.as_mut().reset(cutoff); }
            if timer.as_mut().poll(cx).is_ready() { return Err(expired()); }
        }
        Ok(())
    }
}

impl<W> Drop for FlightEgressIo<W> { fn drop(&mut self) { self.egress.close(); } }

impl<W: AsyncRead + Unpin> AsyncRead for FlightEgressIo<W> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for FlightEgressIo<W> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        self.check(cx)?;
        Pin::new(&mut self.stream).poll_write(cx, bytes)
    }
    fn is_write_vectored(&self) -> bool { self.stream.is_write_vectored() }
    fn poll_write_vectored(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
        self.check(cx)?;
        Pin::new(&mut self.stream).poll_write_vectored(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

struct ConnectionIo {
    stream: FlightEgressIo<TcpStream>,
    peer: TcpConnectInfo,
    _admission: Option<OwnedSemaphorePermit>,
    _transport: Option<Arc<MemoryReservation>>,
    cancellation: Pin<Box<dyn Future<Output = ()> + Send>>,
}

struct TlsConnection {
    stream: TlsStream<ConnectionIo>,
    info: FlightConnectInfo,
}

impl TlsConnection {
    async fn accept(tls: TlsAcceptor, connection: ConnectionIo) -> Option<Self> {
        let info = connection.connect_info();
        let stream = tokio::time::timeout(std::time::Duration::from_secs(5), tls.accept(connection))
            .await.ok()?.ok()?;
        (stream.get_ref().1.alpn_protocol() == Some(b"h2".as_slice())).then_some(Self { stream, info })
    }
}

impl Connected for TlsConnection {
    type ConnectInfo = FlightConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo { self.info.clone() }
}

impl AsyncRead for TlsConnection {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for TlsConnection {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl Connected for ConnectionIo {
    type ConnectInfo = FlightConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        FlightConnectInfo { peer: self.peer.clone(), egress: self.stream.egress.clone() }
    }
}

impl ConnectionIo {
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancellation.as_mut().poll(cx).is_ready() {
            return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "Flight server stopped"));
        }
        Ok(())
    }
}
impl AsyncRead for ConnectionIo {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> { self.check(cx)?; Pin::new(&mut self.stream).poll_read(cx, buffer) }
}
impl AsyncWrite for ConnectionIo {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> { self.check(cx)?; Pin::new(&mut self.stream).poll_write(cx, bytes) }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> { self.check(cx)?; Pin::new(&mut self.stream).poll_flush(cx) }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> { Pin::new(&mut self.stream).poll_shutdown(cx) }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};
    use rtdb_storage::{HeldView, ReadLease, sealing::PlaintextView};
    use rtdb_types::MemoryBudget;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn queued_flight_bytes_require_the_original_grant_at_the_socket() {
        let time = Arc::new(AtomicI64::new(0));
        let clock = {
            let time = time.clone();
            Arc::new(move || time.load(Ordering::Relaxed))
        };
        let view = PlaintextView::new(MemoryBudget::new(64 << 10));
        let grant = view.grant().await.unwrap().holding(
            HeldView::recovery(Arc::new(())), Some(Arc::new(ReadLease::new(Some(2_000_000), clock))),
        );
        let egress = Arc::new(FlightEgress::default());
        egress.admit(grant, None).unwrap();
        let mut output = Vec::new();

        time.store(2_000_000, Ordering::Relaxed);
        let error = FlightEgressIo::new(&mut output, egress).write_all(b"queued HTTP/2 body").await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(output.is_empty());
    }
}

fn network(error: impl std::fmt::Display) -> Anomaly { Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.flight-sql/transport", error.to_string()) }
