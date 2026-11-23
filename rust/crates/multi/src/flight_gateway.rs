use std::{convert::Infallible, net::IpAddr, pin::Pin, sync::Arc, task::{Context, Poll}, time::Duration};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Request, Response, Uri, header};
use http_body_util::{Either, Empty};
use hyper::{body::{Body, Frame, Incoming, SizeHint}, server::conn::http2, service::service_fn};
use hyper_util::{client::legacy::{Client, connect::HttpConnector}, rt::{TokioExecutor, TokioIo, TokioTimer}};
use rtdb_directory::{DeploymentRecord, Directory, MemberId, MemberRecord, now_ms};
use rtdb_flight_sql::{AUDIENCE_FLIGHT_HEADER, FORWARDED_PEER_HEADER, AudienceFlightConfig};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};
use tokio::{io::{AsyncRead, AsyncWrite}, net::TcpListener, sync::{OwnedSemaphorePermit, Semaphore}};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use uuid::Uuid;

use crate::{admin, capability::CellSecret, cipher::Cipher, control, gateway::{DeploymentConnection, DeploymentConnections, GatewayOptions, database_selector}};

const DATABASE_HEADER: &str = "x-xtdb-database";
const COOKIE: &str = "arrow_flight_session_id";
const ROUTE_CONTEXT: &str = "rtdb.multi/flight-session/v3";
const HANDSHAKE: &str = "/arrow.flight.protocol.FlightService/Handshake";
const DO_ACTION: &str = "/arrow.flight.protocol.FlightService/DoAction";
const MAXIMUM_HEADER_BYTES: u32 = 8 * 1024;
const MAXIMUM_ROUTE_BYTES: usize = 4096;

#[derive(Serialize, Deserialize)]
struct Target {
    name: String,
    deployment: String,
    holder: MemberId,
    epoch: u64,
    address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enrollment: Option<ordered_logkeeper::ordered::Enrollment>,
}

#[derive(Serialize, Deserialize)]
struct SessionRoute {
    target: Target,
    session: Uuid,
    audience: Option<String>,
}

struct Router {
    directory: Arc<dyn Directory>,
    control: control::ControlKey,
    secret: Arc<CellSecret>,
    routes: Cipher,
    transport: Client<HttpConnector, Incoming>,
    calls: Arc<Semaphore>,
    deployments: Option<Arc<DeploymentConnections>>,
    audience: Option<AudienceFlightConfig>,
    options: GatewayOptions,
    timeout: Duration,
}

struct RoutedBody {
    body: Incoming,
    _call: OwnedSemaphorePermit,
    _deployment: Option<DeploymentConnection>,
}

impl Body for RoutedBody {
    type Data = Bytes;
    type Error = hyper::Error;
    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<std::result::Result<Frame<Bytes>, hyper::Error>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool { self.body.is_end_stream() }
    fn size_hint(&self) -> SizeHint { self.body.size_hint() }
}

type ProxyBody = Either<Empty<Bytes>, RoutedBody>;

pub async fn serve(listener: TcpListener, directory: Arc<dyn Directory>, options: GatewayOptions, cancellation: CancellationToken) -> Result<()> {
    serve_listener(listener, directory, options, None, cancellation).await
}

pub async fn serve_audience(
    listener: TcpListener, directory: Arc<dyn Directory>, options: GatewayOptions,
    audience: AudienceFlightConfig, cancellation: CancellationToken,
) -> Result<()> {
    serve_listener(listener, directory, options, Some(audience), cancellation).await
}

async fn serve_listener(
    listener: TcpListener, directory: Arc<dyn Directory>, options: GatewayOptions,
    audience: Option<AudienceFlightConfig>, cancellation: CancellationToken,
) -> Result<()> {
    let secret = options.capabilities.as_ref().ok_or_else(|| Anomaly::incorrect("rtdb.multi/flight-secret", "Flight SQL routing requires the cell secret"))?;
    let address = listener.local_addr().map_err(network)?;
    let tls = options.tls.as_ref().map(|config| {
        let mut config = (**config).clone();
        config.alpn_protocols = vec![b"h2".to_vec()];
        TlsAcceptor::from(Arc::new(config))
    });
    if !address.ip().is_loopback() && tls.is_none() {
        return Err(Anomaly::incorrect("rtdb.multi/flight-tls", "A non-loopback Flight SQL gateway requires TLS"));
    }
    if !(1..=Semaphore::MAX_PERMITS).contains(&options.maximum_connections) || options.maximum_connections_per_deployment == 0 {
        return Err(Anomaly::incorrect("rtdb.multi/flight-admission", "Flight SQL gateway admission limits must be positive and supported by the runtime"));
    }

    let mut connector = HttpConnector::new();
    connector.set_connect_timeout(Some(options.control_timeout));
    connector.set_nodelay(true);
    let transport = Client::builder(TokioExecutor::new()).http2_only(true)
        .retry_canceled_requests(false).pool_timer(TokioTimer::new()).pool_max_idle_per_host(1)
        .pool_idle_timeout(Duration::from_secs(30)).build(connector);
    let limit = audience.map_or(options.maximum_connections, AudienceFlightConfig::permits);
    let router = Arc::new(Router { directory, control: control::ControlKey(Some(Arc::clone(secret))),
        secret: Arc::clone(secret), routes: secret.flight_session_cipher()?, transport, audience,
        calls: Arc::new(Semaphore::new(limit)),
        deployments: audience.is_none().then(|| Arc::new(DeploymentConnections::new(options.maximum_connections_per_deployment))),
        timeout: options.startup_timeout, options });
    let connections = Arc::new(Semaphore::new(limit));
    loop {
        let (stream, remote) = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            result = listener.accept() => result.map_err(network)?,
        };
        let Ok(connection) = Arc::clone(&connections).try_acquire_owned() else { continue };
        let (tls, router, cancellation) = (tls.clone(), Arc::clone(&router), cancellation.clone());
        tokio::spawn(async move {
            let _connection = connection;
            let io = async {
                match tls {
                    Some(tls) => {
                        if let Ok(Ok(stream)) = tokio::time::timeout(router.timeout, tls.accept(stream)).await {
                            serving(stream, remote.ip(), router).await;
                        }
                    }
                    None => serving(stream, remote.ip(), router).await,
                }
            };
            let _ = cancellation.run_until_cancelled(io).await;
        });
    }
}

async fn serving(stream: impl AsyncRead + AsyncWrite + Send + Unpin + 'static, remote: IpAddr, router: Arc<Router>) {
    let service = service_fn(move |request| {
        let router = Arc::clone(&router);
        async move {
            let response = match router.proxy(request, remote).await {
                Ok(response) => response,
                Err(status) => status.into_http::<Empty<Bytes>>().map(Either::Left),
            };
            Ok::<_, Infallible>(response)
        }
    });
    let _ = http2::Builder::new(TokioExecutor::new()).max_header_list_size(MAXIMUM_HEADER_BYTES)
        .max_concurrent_streams(256).serve_connection(TokioIo::new(stream), service).await;
}

impl Router {
    fn decode(&self, token: &str) -> std::result::Result<SessionRoute, Status> {
        let invalid = || Status::unauthenticated("Invalid Flight SQL session route");
        if token.len() > MAXIMUM_ROUTE_BYTES { return Err(invalid()); }
        let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?;
        let opened = self.routes.open(ROUTE_CONTEXT, &bytes).map_err(|_| invalid())?;
        serde_json::from_slice(&opened).map_err(|_| invalid())
    }

    fn encode(&self, route: &SessionRoute) -> std::result::Result<String, Status> {
        let bytes = serde_json::to_vec(route).map_err(|_| Status::internal("Invalid Flight SQL route"))?;
        let sealed = self.routes.seal(ROUTE_CONTEXT, &bytes).map_err(|_| Status::internal("Could not seal the Flight SQL route"))?;
        let token = URL_SAFE_NO_PAD.encode(sealed);
        if token.len() > MAXIMUM_ROUTE_BYTES { return Err(Status::internal("The Flight SQL route exceeds its size limit")); }
        Ok(token)
    }

    async fn first_target(&self, selector: &str) -> std::result::Result<Target, Status> {
        let (name, installation) = database_selector(selector).map_err(|_| Status::invalid_argument("Invalid Flight SQL deployment selector"))?;
        if let Some(installed) = self.options.installed_journals.as_ref().and_then(|routes| routes.by_name(name)) {
            if self.audience.is_some() || installation.is_some_and(|id| id != installed.deployment) {
                return Err(Status::not_found("No matching installed Flight SQL deployment"));
            }
            let (address, worker, epoch) = crate::gateway::route_ordered(name, installed, &self.options, async |worker, _| {
                let reply = control::call(&worker.control, &control::Request::FlightEndpoint { audience: false },
                    self.timeout, &self.control).await?;
                match reply {
                    control::Response::FlightEndpoint { address } => Ok(address),
                    _ => Err(Anomaly::incorrect("rtdb.multi/flight-route", "The installed writer has no Flight SQL endpoint")),
                }
            }).await.map_err(|_| moved())?;
            let target = Target { name: name.to_owned(), deployment: installed.deployment.to_string(),
                holder: MemberId(worker.enrollment.worker.to_string()), epoch, address, enrollment: Some(worker.enrollment) };
            self.validate(&target).await?;
            return Ok(target);
        }
        let record = self.directory.deployment_by_name(name).await.map_err(directory_error)?
            .filter(|record| installation.is_none_or(|expected| expected.to_string() == record.id)
                && self.audience.is_none_or(|profile| Uuid::parse_str(&record.id).ok() == Some(profile.source())))
            .ok_or_else(|| Status::not_found("No matching local Flight SQL deployment"))?;
        if record.writer_pool().is_none() { return Err(moved()); }
        let holder = admin::ensure_held(self.directory.as_ref(), name, &self.control).await
            .map_err(|_| Status::unavailable("The Flight SQL deployment has no available writer"))?;
        let (current, member) = tokio::try_join!(self.directory.deployment(&record.id), self.directory.member(&holder)).map_err(directory_error)?;
        let current = current.filter(|record| record.writer_pool().is_some() && record.holder.as_ref() == Some(&holder)).ok_or_else(moved)?;
        let member = member.filter(|member| holder_is_current(&current, member)).ok_or_else(moved)?;
        let control = member.control_address.as_deref().ok_or_else(moved)?;
        let reply = control::call(control, &control::Request::FlightEndpoint { audience: self.audience.is_some() }, self.timeout, &self.control).await
            .map_err(|_| Status::unavailable("The holder did not report a Flight SQL endpoint"))?;
        let control::Response::FlightEndpoint { address } = reply else { return Err(Status::unavailable("The holder has no Flight SQL endpoint")); };
        Ok(Target { name: current.name, deployment: current.id, holder, epoch: current.epoch, address, enrollment: None })
    }

    async fn validate(&self, target: &Target) -> std::result::Result<(), Status> {
        if let Some(installed) = self.options.installed_journals.as_ref().and_then(|routes| routes.by_name(&target.name)) {
            let Some(enrollment) = target.enrollment.filter(|id| installed.endpoints.contains_key(id)) else { return Err(moved()); };
            let route = installed.journal.route().await.map_err(|_| moved())?;
            return if target.deployment == installed.deployment.to_string() && route.writer.grant
                .is_some_and(|grant| grant.epoch == target.epoch && grant.enrollment == enrollment) { Ok(()) } else { Err(moved()) };
        }
        if target.enrollment.is_some() { return Err(moved()); }
        let (record, member) = tokio::try_join!(self.directory.deployment(&target.deployment), self.directory.member(&target.holder)).map_err(directory_error)?;
        let current = record.zip(member).is_some_and(|(record, member)| {
            record.name == target.name && record.epoch == target.epoch && holder_is_current(&record, &member)
        });
        if current { Ok(()) } else { Err(moved()) }
    }

    async fn proxy(&self, mut request: Request<Incoming>, remote: IpAddr) -> std::result::Result<Response<ProxyBody>, Status> {
        if request.method() != http::Method::POST || !request.uri().path().starts_with("/arrow.flight.protocol.FlightService/") {
            return Err(Status::unimplemented("This listener serves Arrow Flight SQL"));
        }
        let authorization = single_header(request.headers(), header::AUTHORIZATION.as_str())?;
        let bearer = authorization.and_then(|value| value.trim().split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer")).map(|(_, token)| token);
        let cookie = flight_cookie(request.headers())?;
        if let (Some(cookie), Some(bearer)) = (cookie, bearer)
            && cookie != bearer { return Err(Status::unauthenticated("Flight SQL cookie and bearer route disagree")); }
        let selector = single_header(request.headers(), DATABASE_HEADER)?;
        let route = bearer.or(cookie).map(|token| self.decode(token)).transpose()?;
        let parse_audience = |value: &str| AudienceFlightConfig::parse(value)
            .map_err(|_| Status::invalid_argument("Invalid Flight audience reservation"));
        let requested_audience = single_header(request.headers(), AUDIENCE_FLIGHT_HEADER)?
            .map(parse_audience).transpose()?;
        let routed_audience = route.as_ref().and_then(|route| route.audience.as_deref())
            .map(parse_audience).transpose()?;
        let audience = match (route.as_ref(), requested_audience) {
            (None, Some(source)) if request.uri().path() == HANDSHAKE => Some(source),
            (None, None) => None,
            (Some(_), None) => routed_audience,
            _ => return Err(Status::permission_denied("The Flight audience is fixed at its handshake")),
        };
        if audience != self.audience {
            return Err(Status::permission_denied("The Flight route belongs to another listener"));
        }
        if routed_audience.is_some() && request.uri().path() != DO_ACTION {
            return Err(Status::permission_denied("The Flight audience only routes observation actions"));
        }
        let call = Arc::clone(&self.calls).try_acquire_owned().map_err(|_| Status::resource_exhausted("Flight SQL gateway call limit reached"))?;
        let name = match (&route, selector) {
            (None, None) => return Err(Status::invalid_argument("Flight SQL startup requires x-xtdb-database")),
            (None, Some(selector)) => database_selector(selector).map_err(|_| Status::invalid_argument("Invalid Flight SQL deployment selector"))?.0,
            (Some(route), _) => route.target.name.as_str(),
        };
        let deployment = self.deployments.as_ref().map(|deployments| {
            deployments.admit(name).ok_or_else(|| Status::resource_exhausted("This deployment has reached its Flight SQL call limit"))
        }).transpose()?;
        let routing = async {
            match route {
                Some(route) => {
                    if let Some(selector) = selector {
                        let (name, installation) = database_selector(selector).map_err(|_| Status::invalid_argument("Invalid Flight SQL deployment selector"))?;
                        if name != route.target.name || installation.is_some_and(|id| id.to_string() != route.target.deployment) {
                            return Err(Status::permission_denied("This Flight SQL session belongs to another deployment"));
                        }
                    }
                    // An audience route locates a socket. Serving authority is checked on the holder.
                    if self.audience.is_none() { self.validate(&route.target).await?; }
                    Ok((route.target, Some(route.session)))
                }
                None => self.first_target(selector.expect("a new session has a selector")).await.map(|target| (target, None)),
            }
        };
        let (target, session) = tokio::time::timeout(self.timeout, routing).await
            .map_err(|_| Status::deadline_exceeded("Timed out resolving the Flight SQL route"))??;
        if audience.is_some_and(|profile| Uuid::parse_str(&target.deployment).ok() != Some(profile.source())) {
            return Err(Status::permission_denied("The Flight route belongs to another audience source"));
        }

        let has_bearer = bearer.is_some();
        let headers = request.headers_mut();
        headers.remove(header::HOST);
        headers.remove(header::COOKIE);
        headers.remove(FORWARDED_PEER_HEADER);
        headers.remove(AUDIENCE_FLIGHT_HEADER);
        headers.insert(DATABASE_HEADER, header_value(&target.name)?);
        if let Some(source) = audience {
            headers.insert(AUDIENCE_FLIGHT_HEADER, header_value(&source.to_string())?);
        }
        if let Some(session) = session {
            headers.insert(header::COOKIE, header_value(&format!("{COOKIE}={session}"))?);
            if has_bearer { headers.insert(header::AUTHORIZATION, header_value(&format!("Bearer {session}"))?); }
        } else {
            headers.insert(FORWARDED_PEER_HEADER, header_value(&self.secret.flight_peer(&target.deployment, target.epoch, remote))?);
        }
        let path = request.uri().path_and_query().ok_or_else(|| Status::invalid_argument("Invalid Flight SQL request path"))?.clone();
        *request.uri_mut() = Uri::builder().scheme("http").authority(target.address.as_str()).path_and_query(path)
            .build().map_err(|_| Status::internal("Invalid private Flight SQL endpoint"))?;
        let mut response = self.transport.request(request).await.map_err(|_| Status::unavailable("The Flight SQL writer disconnected"))?;
        self.rewrite_response(response.headers_mut(), target, audience)?;
        Ok(response.map(|body| Either::Right(RoutedBody { body, _call: call, _deployment: deployment })))
    }

    fn rewrite_response(&self, headers: &mut HeaderMap, target: Target, audience: Option<AudienceFlightConfig>) -> std::result::Result<(), Status> {
        let Some(cookie) = headers.remove(header::SET_COOKIE) else { return Ok(()); };
        let cookie = cookie.to_str().map_err(|_| Status::internal("Invalid private Flight SQL cookie"))?;
        if cookie.contains("Max-Age=0") {
            headers.insert(header::SET_COOKIE, HeaderValue::from_static("arrow_flight_session_id=; Max-Age=0"));
            headers.remove(header::AUTHORIZATION);
            return Ok(());
        }
        let session = cookie.strip_prefix("arrow_flight_session_id=").and_then(|id| Uuid::parse_str(id).ok())
            .ok_or_else(|| Status::internal("Invalid private Flight SQL session"))?;
        let audience = audience.map(|profile| profile.to_string());
        if audience.as_deref() != single_header(headers, AUDIENCE_FLIGHT_HEADER)? {
            return Err(Status::unavailable("The holder did not acknowledge the exact Flight audience"));
        }
        let route = self.encode(&SessionRoute { target, session, audience })?;
        headers.insert(header::SET_COOKIE, header_value(&format!("{COOKIE}={route}"))?);
        if headers.contains_key(header::AUTHORIZATION) {
            headers.insert(header::AUTHORIZATION, header_value(&format!("Bearer {route}"))?);
        }
        Ok(())
    }
}

fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> std::result::Result<Option<&'a str>, Status> {
    let mut values = headers.get_all(name).iter();
    let first = values.next().map(|value| value.to_str().map_err(|_| Status::invalid_argument("Invalid Flight SQL metadata"))).transpose()?;
    if values.next().is_some() { return Err(Status::invalid_argument("Duplicate Flight SQL metadata")); }
    Ok(first)
}

fn flight_cookie(headers: &HeaderMap) -> std::result::Result<Option<&str>, Status> {
    let mut named = headers.get_all(header::COOKIE).iter().filter_map(|header| header.to_str().ok())
        .flat_map(|header| header.split(';')).filter_map(|field| field.trim().split_once('='))
        .filter_map(|(name, value)| (name == COOKIE && !value.is_empty()).then_some(value));
    let first = named.next();
    if named.next().is_some() { return Err(Status::unauthenticated("Duplicate Flight SQL session cookies")); }
    Ok(first)
}

fn holder_is_current(record: &DeploymentRecord, member: &MemberRecord) -> bool {
    record.holder.as_ref() == Some(&member.id) && record.may_hold_on(member, now_ms())
}

fn header_value(value: &str) -> std::result::Result<HeaderValue, Status> {
    HeaderValue::from_str(value).map_err(|_| Status::internal("Invalid private Flight SQL metadata"))
}

fn moved() -> Status { Status::unavailable("The Flight SQL placement changed. Establish a new session.") }
fn directory_error(_: Anomaly) -> Status { Status::unavailable("The Flight SQL directory is unavailable") }
fn network(error: std::io::Error) -> Anomaly { Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.multi/flight-gateway", error.to_string()) }
