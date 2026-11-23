use std::{future::Future, io, net::SocketAddr, pin::Pin, sync::Arc, task::{Context, Poll}};
use arrow_flight::flight_service_server::FlightServiceServer;
use futures::StreamExt;
use rtdb_auth::Authenticator;
use rtdb_node::{Clock, Node, SystemClock};
use rtdb_types::{Anomaly, Result};
use tokio::{io::{AsyncRead, AsyncWrite, ReadBuf}, net::{TcpListener, TcpStream}, sync::watch};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::{Connected, TcpConnectInfo};
use crate::{FlightConfig, service::Service};

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
pub struct ServerDriver { listener: Option<TcpListener>, service: Service }
impl ServerDriver {
    /// Binds the insecure Flight endpoint without spawning a task.
    /// The owner runs this driver and cancels its handle, then joins `run`, before closing the node.
    pub async fn bind(config: FlightConfig, node: Node, authenticator: Arc<Authenticator>) -> Result<(ServerHandle, Self)> {
        Self::bind_with_clock(config, node, authenticator, Arc::new(SystemClock)).await
    }
    pub async fn bind_with_clock(config: FlightConfig, node: Node, authenticator: Arc<Authenticator>, clock: Arc<dyn Clock>) -> Result<(ServerHandle, Self)> {
        config.validate()?;
        let listener = TcpListener::bind(config.address).await.map_err(network)?;
        let address = listener.local_addr().map_err(network)?;
        let cancellation = node.cancellation().child_token();
        let (sender, receiver) = watch::channel(ServerStatus::default());
        let service = Service::new(node, authenticator, config, clock, cancellation.clone(), sender)?;
        Ok((ServerHandle { address, cancellation, status: receiver }, Self { listener: Some(listener), service }))
    }
    pub async fn run(mut self) -> Result<()> {
        let listener = self.listener.take().expect("unstarted Flight driver");
        let cancellation = self.service.0.cancellation.clone();
        let incoming_cancel = cancellation.clone();
        let incoming = TcpListenerStream::new(listener).map(move |result| result.and_then(|stream| { stream.set_nodelay(true)?; Ok(ConnectionIo { stream, cancellation: Box::pin(incoming_cancel.clone().cancelled_owned()) }) }));
        let server = FlightServiceServer::new(self.service.clone()).max_decoding_message_size(self.service.0.config.maximum_message_bytes).max_encoding_message_size(self.service.0.config.maximum_message_bytes);
        let serving = tonic::transport::Server::builder().http2_max_header_list_size(8192).add_service(server).serve_with_incoming_shutdown(incoming, cancellation.clone().cancelled_owned());
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
struct ConnectionIo { stream: TcpStream, cancellation: Pin<Box<dyn Future<Output = ()> + Send>> }
impl Connected for ConnectionIo { type ConnectInfo = TcpConnectInfo; fn connect_info(&self) -> Self::ConnectInfo { self.stream.connect_info() } }
impl ConnectionIo { fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> { if self.cancellation.as_mut().poll(cx).is_ready() { Err(io::Error::new(io::ErrorKind::ConnectionAborted, "Flight server stopped")) } else { Ok(()) } } }
impl AsyncRead for ConnectionIo {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> { if let Err(error) = self.check(cx) { return Poll::Ready(Err(error)); } Pin::new(&mut self.stream).poll_read(cx, buffer) }
}
impl AsyncWrite for ConnectionIo {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> { if let Err(error) = self.check(cx) { return Poll::Ready(Err(error)); } Pin::new(&mut self.stream).poll_write(cx, bytes) }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> { if let Err(error) = self.check(cx) { return Poll::Ready(Err(error)); } Pin::new(&mut self.stream).poll_flush(cx) }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> { Pin::new(&mut self.stream).poll_shutdown(cx) }
}
fn network(error: impl std::fmt::Display) -> Anomaly { Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.flight-sql/transport", error.to_string()) }
