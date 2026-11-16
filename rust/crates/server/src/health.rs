use std::{collections::BTreeMap, convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_node::{HealthChecks, HealthResponse, Node};
use rtdb_plan::CatalogMetrics;
use rtdb_types::{Anomaly, Result};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct HealthConfig {
    pub address: SocketAddr,
    pub maximum_connections: usize,
    pub header_timeout: Duration,
    pub drain_timeout: Duration,
}
impl Default for HealthConfig {
    fn default() -> Self { Self { address: ([127, 0, 0, 1], 0).into(), maximum_connections: 64, header_timeout: Duration::from_secs(30), drain_timeout: Duration::from_secs(5) } }
}

#[derive(Clone)]
pub struct HealthHandle { address: SocketAddr, cancellation: CancellationToken }
impl HealthHandle {
    pub fn address(&self) -> SocketAddr { self.address }
    pub fn shutdown(&self) { self.cancellation.cancel(); }
}

pub struct HealthDriver { listener: TcpListener, application: Arc<HealthApplication>, config: HealthConfig, cancellation: CancellationToken }
impl HealthDriver {
    /// Binds without spawning; the caller runs and joins the returned driver before releasing the node.
    pub async fn bind(config: HealthConfig, node: Node, metrics: Arc<dyn CatalogMetrics>, parent: &CancellationToken) -> Result<(HealthHandle, Self)> {
        if config.maximum_connections == 0 || config.header_timeout.is_zero() { return Err(Anomaly::incorrect("rtdb.health/invalid-config", "Health connection capacity and header timeout must be positive")); }
        let memory = Arc::clone(node.memory());
        let checks = HealthChecks::new(node)?;
        let listener = TcpListener::bind(config.address).await.map_err(io_error)?;
        let address = listener.local_addr().map_err(io_error)?;
        let cancellation = parent.child_token();
        let handle = HealthHandle { address, cancellation: cancellation.clone() };
        Ok((handle, Self { listener, application: Arc::new(HealthApplication { checks, metrics, memory }), config, cancellation }))
    }
    pub async fn run(self) -> Result<()> {
        let mut connections = JoinSet::new();
        let mut failure = None;
        loop {
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => break,
                result = connections.join_next(), if !connections.is_empty() => if let Some(Err(error)) = result { failure = Some(Anomaly::fault("rtdb.health/connection-task", error.to_string())); break; },
                accepted = self.listener.accept() => {
                    let (socket, _) = match accepted { Ok(accepted) => accepted, Err(error) => { failure = Some(io_error(error)); break; } };
                    if connections.len() >= self.config.maximum_connections { drop(socket); continue; }
                    let reservation = match self.application.memory.reserve(64 * 1024) { Ok(reservation) => reservation, Err(_) => { drop(socket); continue; } };
                    let application = Arc::clone(&self.application);
                    let cancellation = self.cancellation.child_token();
                    let config = self.config.clone();
                    connections.spawn(async move {
                        let _reservation = reservation;
                        let mut http = http1::Builder::new();
                        http.timer(TokioTimer::new()).header_read_timeout(config.header_timeout).max_buf_size(16 * 1024).max_headers(64);
                        let connection = http.serve_connection(TokioIo::new(socket), service_fn(move |request| Arc::clone(&application).respond(request)));
                        tokio::pin!(connection);
                        tokio::select! {
                            _ = &mut connection => (),
                            _ = cancellation.cancelled() => {
                                connection.as_mut().graceful_shutdown();
                                let _ = tokio::time::timeout(config.drain_timeout, &mut connection).await;
                            }
                        }
                    });
                }
            }
        }
        drop(self.listener);
        self.cancellation.cancel();
        while let Some(result) = connections.join_next().await { if let Err(error) = result { if failure.is_none() { failure = Some(Anomaly::fault("rtdb.health/connection-task", error.to_string())); } } }
        failure.map_or(Ok(()), Err)
    }
}

struct HealthApplication { checks: HealthChecks, metrics: Arc<dyn CatalogMetrics>, memory: Arc<MemoryBudget> }
impl HealthApplication {
    async fn respond(self: Arc<Self>, request: Request<Incoming>) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
        let response = match self.route(request.method(), request.uri().path(), request.uri().query()).await {
            Ok(response) => response,
            Err(error) => Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR).body(Full::new(Bytes::from(format!("Exception when calling endpoint - {error}")))).unwrap_or_else(|_| Response::new(Full::new(Bytes::new()))),
        };
        Ok(response)
    }
    async fn route(&self, method: &Method, path: &str, query: Option<&str>) -> Result<Response<Full<Bytes>>> {
        let known = matches!(path, "/" | "/metrics" | "/healthz/started" | "/healthz/alive" | "/healthz/ready" | "/system/finish-block");
        if known && ((path == "/system/finish-block" && method != Method::POST) || (path != "/system/finish-block" && method != Method::GET)) {
            return response(HealthResponse { status: 405, headers: BTreeMap::new(), body: "Method not allowed".into() }, &self.memory);
        }
        let check = match path {
            "/" => HealthResponse { status: 200, headers: BTreeMap::from([("Content-Type".into(), "text/html; charset=utf-8".into())]), body: INDEX.into() },
            "/metrics" => {
                let rows = self.metrics.sample().await?;
                let bytes = crate::prometheus::render(rows.rows(), &self.memory)?;
                return Response::builder().status(200).header("Content-Type", "text/plain; version=0.0.4").body(Full::new(bytes)).map_err(io_error);
            }
            "/healthz/started" => self.checks.started(),
            "/healthz/alive" => self.checks.alive().await?,
            "/healthz/ready" => self.checks.ready(),
            "/system/finish-block" => self.checks.finish_block(query.and_then(raw_database)).await,
            _ => HealthResponse { status: 404, headers: BTreeMap::new(), body: "Not found".into() },
        };
        response(check, &self.memory)
    }
}

fn raw_database(query: &str) -> Option<&str> { query.split('&').filter_map(|entry| entry.strip_prefix("db=")).find(|value| !value.is_empty()) }
struct ResponseText { text: String, _reservation: Arc<MemoryReservation> }
impl AsRef<[u8]> for ResponseText { fn as_ref(&self) -> &[u8] { self.text.as_bytes() } }
fn response(response: HealthResponse, memory: &Arc<MemoryBudget>) -> Result<Response<Full<Bytes>>> {
    let reservation = memory.reserve(response.body.capacity().checked_add(1024).ok_or_else(|| io_error("Health response size overflow"))?)?;
    let mut builder = Response::builder().status(response.status);
    for (key, value) in response.headers { builder = builder.header(key, value); }
    builder.body(Full::new(Bytes::from_owner(ResponseText { text: response.body, _reservation: reservation }))).map_err(io_error)
}
fn io_error(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.health/http", error.to_string()) }

const INDEX: &str = "<!DOCTYPE html>\n<html>\n<head><title>XTDB Healthz API</title></head>\n<body>\n  <h1>XTDB Healthz API</h1>\n  <p>See <a href=\"https://docs.xtdb.com/ops/config/monitoring.html\">Monitoring</a> and <a href=\"https://docs.xtdb.com/ops/maintenance.html\">Maintenance</a> documentation for details on using this API endpoint.</p>\n</body>\n</html>\n";
