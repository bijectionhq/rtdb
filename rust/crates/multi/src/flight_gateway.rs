use std::{collections::BTreeMap, convert::Infallible, io, net::IpAddr, pin::Pin, sync::Arc, task::{Context, Poll}, time::Duration};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::{Bytes, BytesMut};
use http::{HeaderMap, HeaderValue, Request, Response, Uri, header};
use http_body_util::{BodyExt, Either, Empty, Full, combinators::UnsyncBoxBody};
use hyper::{body::{Body, Frame, Incoming, SizeHint}, server::conn::http2, service::service_fn};
use hyper_util::{client::legacy::{Client, connect::HttpConnector}, rt::{TokioExecutor, TokioIo, TokioTimer}};
use prost::Message;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_directory::{DeploymentRecord, Directory, Lifecycle, MemberId, MemberRecord, now_ms};
use rtdb_flight_sql::{FlightEgress, FlightEgressIo, GATEWAY_ANSWER_DEADLINE_HEADER, FORWARDED_PEER_HEADER};
use rtdb_storage::sealing::DeliveryDeadline;
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};
use tokio::{io::{AsyncRead, AsyncWrite}, net::TcpListener, sync::{OwnedSemaphorePermit, Semaphore}};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use uuid::Uuid;

use crate::{admin, capability::CellSecret, cipher::Cipher, classic_flight, control, gateway::{DeploymentConnection, DeploymentConnections, GatewayOptions, database_selector}};

const DATABASE_HEADER: &str = "x-xtdb-database";
const COOKIE: &str = "arrow_flight_session_id";
const ROUTE_CONTEXT: &str = "rtdb.multi/flight-session/v6";
const FRESH_HEADER: &str = "x-rtdb-fresh-read";
const HANDSHAKE: &str = "/arrow.flight.protocol.FlightService/Handshake";
const DO_ACTION: &str = "/arrow.flight.protocol.FlightService/DoAction";
const MAXIMUM_HEADER_BYTES: u32 = 8 * 1024;
const MAXIMUM_ROUTE_BYTES: usize = 4096;
const MAXIMUM_ACTION_FRAME_BYTES: usize = (64 << 20) + 5;

#[derive(Serialize, Deserialize)]
struct Target {
    name: String,
    deployment: String,
    holder: MemberId,
    epoch: u64,
    address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enrollment: Option<ordered_logkeeper::ordered::Enrollment>,
    replica: Option<FreshReplica>,
    ordered_reader: Option<ordered_logkeeper::ordered::Placement>,
    fresh: bool,
}

#[derive(Serialize, Deserialize)]
struct FreshReplica {
    stream: String,
    log_epoch: i32,
    storage_prefix: String,
}

#[derive(Serialize, Deserialize)]
struct SessionRoute {
    target: Target,
    session: Uuid,
}

struct Router {
    directory: Arc<dyn Directory>,
    control: control::ControlKey,
    secret: Arc<CellSecret>,
    routes: Cipher,
    transport: Client<HttpConnector, RequestBody>,
    calls: Arc<Semaphore>,
    deployments: Arc<DeploymentConnections>,
    options: GatewayOptions,
    timeout: Duration,
    retained_key: Option<[u8; 32]>,
    inspected_actions: Arc<MemoryBudget>,
}

struct RoutedBody {
    body: Incoming,
    _call: OwnedSemaphorePermit,
    _deployment: DeploymentConnection,
    deadline: Option<DeliveryDeadline>,
}

impl Body for RoutedBody {
    type Data = Bytes;
    type Error = io::Error;
    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<std::result::Result<Frame<Bytes>, io::Error>>> {
        if self.deadline.is_some_and(|deadline| !deadline.is_live()) {
            return Poll::Ready(Some(Err(io::Error::new(io::ErrorKind::TimedOut, "The routed Flight answer expired"))));
        }
        Pin::new(&mut self.body).poll_frame(cx).map(|frame| frame.map(|result| result.map_err(io::Error::other)))
    }
    fn is_end_stream(&self) -> bool { self.body.is_end_stream() }
    fn size_hint(&self) -> SizeHint { self.body.size_hint() }
}

type ProxyBody = Either<Empty<Bytes>, RoutedBody>;
type RequestBody = UnsyncBoxBody<Bytes, io::Error>;

struct InspectedAction {
    wire: Bytes,
    _memory: Arc<MemoryReservation>,
}

impl AsRef<[u8]> for InspectedAction {
    fn as_ref(&self) -> &[u8] { &self.wire }
}

pub async fn serve(listener: TcpListener, directory: Arc<dyn Directory>, options: GatewayOptions, cancellation: CancellationToken) -> Result<()> {
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
    let limit = options.maximum_connections;
    let mut client = Client::builder(TokioExecutor::new());
    client.http2_only(true).retry_canceled_requests(false).pool_timer(TokioTimer::new())
        .pool_max_idle_per_host(1).pool_idle_timeout(Duration::from_secs(30));
    let transport = client.build(connector);
    let retained_key = options.classic_retained_flight_key.as_deref().map(classic_flight::read_retained_key).transpose()?;
    let router = Arc::new(Router { directory, control: control::ControlKey(Some(Arc::clone(secret))),
        secret: Arc::clone(secret), routes: secret.flight_session_cipher()?, transport,
        calls: Arc::new(Semaphore::new(limit)),
        deployments: Arc::new(DeploymentConnections::new(options.maximum_connections_per_deployment)),
        timeout: options.startup_timeout, options, retained_key, inspected_actions: MemoryBudget::new(8 * MAXIMUM_ACTION_FRAME_BYTES) });
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
            let egress = Arc::new(FlightEgress::default());
            let stream = FlightEgressIo::new(stream, egress.clone());
            let io = async {
                match tls {
                    Some(tls) => {
                        if let Ok(Ok(stream)) = tokio::time::timeout(router.timeout, tls.accept(stream)).await {
                            serving(stream, remote.ip(), router, egress).await;
                        }
                    }
                    None => serving(stream, remote.ip(), router, egress).await,
                }
            };
            let _ = cancellation.run_until_cancelled(io).await;
        });
    }
}

async fn serving(stream: impl AsyncRead + AsyncWrite + Send + Unpin + 'static, remote: IpAddr, router: Arc<Router>, egress: Arc<FlightEgress>) {
    let service = service_fn(move |request| {
        let router = Arc::clone(&router);
        let egress = Arc::clone(&egress);
        async move {
            let response = match router.proxy(request, remote, &egress).await {
                Ok(response) => response,
                Err(status) => status.into_http::<Empty<Bytes>>().map(Either::Left),
            };
            Ok::<_, Infallible>(response)
        }
    });
    let mut http = http2::Builder::new(TokioExecutor::new());
    http.max_header_list_size(MAXIMUM_HEADER_BYTES).max_concurrent_streams(256);
    let _ = http.serve_connection(TokioIo::new(stream), service).await;
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

    async fn first_target(&self, selector: &str, fresh: bool) -> std::result::Result<(Target, Option<DeploymentRecord>), Status> {
        let (name, installation) = database_selector(selector).map_err(|_| Status::invalid_argument("Invalid Flight SQL deployment selector"))?;
        if fresh {
            return self.fresh_target(name, installation).await.map(|target| (target, None));
        }
        if let Some(installed) = self.options.ordered_placement(name).await.map_err(directory_error)? {
            if installation.is_some_and(|id| id != installed.deployment) {
                return Err(Status::not_found("No matching installed Flight SQL deployment"));
            }
            let (address, worker, epoch) = crate::gateway::route_ordered(name, &installed, &self.options, true, |worker, _| async move {
                let reply = control::call(&worker.control, &control::Request::FlightEndpoint,
                    self.timeout, &self.control).await?;
                match reply {
                    control::Response::FlightEndpoint { address } => Ok(address),
                    _ => Err(Anomaly::incorrect("rtdb.multi/flight-route", "The installed writer has no Flight SQL endpoint")),
                }
            }).await.map_err(|_| moved())?;
            let target = Target { name: name.to_owned(), deployment: installed.deployment.to_string(),
                holder: MemberId(worker.enrollment.worker.to_string()), epoch, address, enrollment: Some(worker.enrollment), replica: None, ordered_reader: None, fresh: false };
            let classic = self.validate(&target).await?;
            return Ok((target, classic));
        }
        if self.options.installed_cell.is_some() {
            return Err(Status::not_found("No matching installed Flight SQL deployment"));
        }
        let record = self.directory.deployment_by_name(name).await.map_err(directory_error)?
            .filter(|record| installation.is_none_or(|expected| expected.to_string() == record.id))
            .ok_or_else(|| Status::not_found("No matching local Flight SQL deployment"))?;
        if record.writer_pool().is_none() { return Err(moved()); }
        let holder = admin::ensure_held(self.directory.as_ref(), name, &self.control).await
            .map_err(|_| Status::unavailable("The Flight SQL deployment has no available writer"))?;
        let (current, member) = tokio::try_join!(self.directory.deployment(&record.id), self.directory.member(&holder)).map_err(directory_error)?;
        let current = current.filter(|record| record.writer_pool().is_some() && record.holder.as_ref() == Some(&holder)).ok_or_else(moved)?;
        let member = member.filter(|member| holder_is_current(&current, member)).ok_or_else(moved)?;
        let control = member.control_address.as_deref().ok_or_else(moved)?;
        let reply = control::call(control, &control::Request::FlightEndpoint, self.timeout, &self.control).await
            .map_err(|_| Status::unavailable("The holder did not report a Flight SQL endpoint"))?;
        let control::Response::FlightEndpoint { address } = reply else { return Err(Status::unavailable("The holder has no Flight SQL endpoint")); };
        let target = Target { name: current.name.clone(), deployment: current.id.clone(), holder, epoch: current.epoch,
            address, enrollment: None, replica: None, ordered_reader: None, fresh: false };
        Ok((target, rtdb_directory::classic_flight::excludes_unclassified(&current).then_some(current)))
    }

    /// Resolve an eligible subscriber and let the receiver establish its own cut.
    async fn fresh_target(&self, name: &str, installation: Option<Uuid>) -> std::result::Result<Target, Status> {
        if let Some(installed) = self.options.ordered_placement(name).await.map_err(directory_error)? {
            if installation.is_some_and(|id| id != installed.deployment) {
                return Err(Status::not_found("No matching installed Flight SQL deployment"));
            }
            let target = if let Ok(read) = crate::gateway::route_ordered_reader(name, &installed, &self.options, true).await {
                Target {
                    name: name.into(), deployment: installed.deployment.to_string(),
                    holder: MemberId(read.worker.enrollment.worker.to_string()),
                    address: read.flight.ok_or_else(moved)?, epoch: read.epoch,
                    enrollment: Some(read.worker.enrollment), replica: None, ordered_reader: Some(read.placement), fresh: true,
                }
            } else {
                let route = installed.journal.route().await.map_err(|_| moved())?;
                let grant = route.writer.grant.ok_or_else(moved)?;
                let worker = installed.endpoints.get(&grant.enrollment).ok_or_else(moved)?;
                let address = self.reported_flight_endpoint(&worker.control).await?;
                Target {
                    name: name.into(), deployment: installed.deployment.to_string(),
                    holder: MemberId(grant.enrollment.worker.to_string()), epoch: grant.epoch,
                    address, enrollment: Some(grant.enrollment), replica: None, ordered_reader: None, fresh: true,
                }
            };
            let _ = self.validate(&target).await?;
            return Ok(target);
        }
        if self.options.installed_cell.is_some() {
            return Err(Status::not_found("No matching installed Flight SQL deployment"));
        }
        let record = self.directory.deployment_by_name(name).await.map_err(directory_error)?
            .filter(|record| installation.is_none_or(|expected| expected.to_string() == record.id))
            .ok_or_else(|| Status::not_found("No matching local Flight SQL deployment"))?;
        if rtdb_directory::classic_flight::excludes_unclassified(&record) { return Err(classic_flight_refusal()); }
        if !record.linearizable_reads || record.lifecycle != Lifecycle::Live || record.move_from.is_some() {
            return Err(moved());
        }
        let members: BTreeMap<_, _> = self.directory.members().await.map_err(directory_error)?
            .into_iter().map(|member| (member.id.clone(), member)).collect();
        for replica in crate::deployment::read_replicas(&record, &members, now_ms()) {
            let Some(control) = replica.control_address.as_deref() else { continue; };
            let subscription = control::Request::SubscribeFresh { name: name.to_owned() };
            if !matches!(control::call(control, &subscription, self.timeout, &self.control).await,
                Ok(control::Response::Subscribed { .. }))
            {
                continue;
            }
            let Ok(control::Response::FlightEndpoint { address }) = control::call(
                control, &control::Request::FlightEndpoint, self.timeout, &self.control,
            ).await else { continue; };
            let target = Target {
                name: record.name.clone(), deployment: record.id.clone(),
                holder: replica.id.clone(), epoch: record.epoch, address, enrollment: None,
                ordered_reader: None, fresh: true,
                replica: Some(FreshReplica {
                    stream: record.stream("replica"), log_epoch: record.log_epoch,
                    storage_prefix: record.storage_prefix(),
                }),
            };
            if self.validate(&target).await.is_ok() { return Ok(target); }
        }
        let holder = record.holder.as_ref().and_then(|holder| members.get(holder))
            .filter(|holder| holder_is_current(&record, holder)).ok_or_else(moved)?;
        let control = holder.control_address.as_deref().ok_or_else(moved)?;
        let target = Target {
            name: record.name, deployment: record.id, holder: holder.id.clone(), epoch: record.epoch,
            address: self.reported_flight_endpoint(control).await?, enrollment: None,
            replica: None, ordered_reader: None, fresh: true,
        };
        let _ = self.validate(&target).await?;
        Ok(target)
    }

    async fn reported_flight_endpoint(&self, endpoint: &str) -> std::result::Result<String, Status> {
        match control::call(endpoint, &control::Request::FlightEndpoint, self.timeout, &self.control).await {
            Ok(control::Response::FlightEndpoint { address }) => Ok(address),
            _ => Err(moved()),
        }
    }

    async fn validate(&self, target: &Target) -> std::result::Result<Option<DeploymentRecord>, Status> {
        if let Some(installed) = self.options.ordered_placement(&target.name).await.map_err(directory_error)? {
            let Some(enrollment) = target.enrollment.filter(|id| installed.endpoints.contains_key(id)) else { return Err(moved()); };
            let route = installed.journal.route().await.map_err(|_| moved())?;
            if target.deployment != installed.deployment.to_string() || route.writer.epoch != target.epoch {
                return Err(moved());
            }
            return if target.ordered_reader.is_some_and(|scope| scope == route.placement)
                || target.ordered_reader.is_none() && route.writer.grant.is_some_and(|grant| grant.enrollment == enrollment)
            { Ok(None) } else { Err(moved()) };
        }
        if self.options.installed_cell.is_some() { return Err(moved()); }
        if let Some(replica) = &target.replica {
            let Some(record) = self.directory.deployment(&target.deployment).await.map_err(directory_error)? else { return Err(moved()); };
            if rtdb_directory::classic_flight::excludes_unclassified(&record) { return Err(classic_flight_refusal()); }
            if record.name != target.name || record.epoch != target.epoch || record.log_epoch != replica.log_epoch
                || record.stream("replica") != replica.stream || record.storage_prefix() != replica.storage_prefix
                || !record.linearizable_reads || record.lifecycle != Lifecycle::Live || record.move_from.is_some()
            {
                return Err(moved());
            }
            let members: BTreeMap<_, _> = self.directory.members().await.map_err(directory_error)?
                .into_iter().map(|member| (member.id.clone(), member)).collect();
            return if crate::deployment::read_replicas(&record, &members, now_ms())
                .iter().any(|member| member.id == target.holder) { Ok(None) } else { Err(moved()) };
        }
        if target.enrollment.is_some() || target.ordered_reader.is_some() { return Err(moved()); }
        let (record, member) = tokio::try_join!(self.directory.deployment(&target.deployment), self.directory.member(&target.holder)).map_err(directory_error)?;
        if target.fresh && record.as_ref().is_some_and(rtdb_directory::classic_flight::excludes_unclassified) {
            return Err(classic_flight_refusal());
        }
        let (record, _) = record.zip(member).filter(|(record, member)| {
            record.name == target.name && record.epoch == target.epoch && holder_is_current(record, member)
        }).ok_or_else(moved)?;
        Ok(rtdb_directory::classic_flight::excludes_unclassified(&record).then_some(record))
    }

    async fn proxy(&self, mut request: Request<Incoming>, remote: IpAddr, egress: &FlightEgress) -> std::result::Result<Response<ProxyBody>, Status> {
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
        let fresh = match single_header(request.headers(), FRESH_HEADER)? {
            Some("1") if route.is_none() && request.uri().path() == HANDSHAKE => true,
            Some(_) => return Err(Status::permission_denied("A fresh Flight route is fixed at its handshake")),
            None => false,
        };
        if route.as_ref().is_some_and(|route| route.target.fresh) && request.uri().path() != DO_ACTION {
            return Err(Status::permission_denied("The fresh Flight route carries owned reads"));
        }
        let call = Arc::clone(&self.calls).try_acquire_owned().map_err(|_| Status::resource_exhausted("Flight SQL gateway call limit reached"))?;
        let name = match (&route, selector) {
            (None, None) => return Err(Status::invalid_argument("Flight SQL startup requires x-xtdb-database")),
            (None, Some(selector)) => database_selector(selector).map_err(|_| Status::invalid_argument("Invalid Flight SQL deployment selector"))?.0,
            (Some(route), _) => route.target.name.as_str(),
        };
        let deployment = self.deployments.admit(name)
            .ok_or_else(|| Status::resource_exhausted("This deployment has reached its Flight SQL call limit"))?;
        let routing = async move {
            match route {
                Some(route) => {
                    if let Some(selector) = selector {
                        let (name, installation) = database_selector(selector).map_err(|_| Status::invalid_argument("Invalid Flight SQL deployment selector"))?;
                        if name != route.target.name || installation.is_some_and(|id| id.to_string() != route.target.deployment) {
                            return Err(Status::permission_denied("This Flight SQL session belongs to another deployment"));
                        }
                    }
                    let classic = self.validate(&route.target).await?;
                    Ok((route.target, Some(route.session), classic))
                }
                None => self.first_target(selector.expect("a new session has a selector"), fresh).await
                    .map(|(target, classic)| (target, None, classic)),
            }
        };
        let (target, session, classic) = tokio::time::timeout(self.timeout, routing).await
            .map_err(|_| Status::deadline_exceeded("Timed out resolving the Flight SQL route"))??;

        let has_bearer = bearer.is_some();
        let headers = request.headers_mut();
        headers.remove(header::HOST);
        headers.remove(header::COOKIE);
        headers.remove(FORWARDED_PEER_HEADER);
        headers.remove(FRESH_HEADER);
        headers.insert(GATEWAY_ANSWER_DEADLINE_HEADER, HeaderValue::from_static("forward"));
        headers.insert(DATABASE_HEADER, header_value(&target.name)?);
        if let Some(session) = session {
            headers.insert(header::COOKIE, header_value(&format!("{COOKIE}={session}"))?);
            if has_bearer { headers.insert(header::AUTHORIZATION, header_value(&format!("Bearer {session}"))?); }
        } else {
            let peer = match (&target.replica, target.ordered_reader) {
                (Some(replica), None) => self.secret.fresh_flight_peer(
                    &target.deployment, target.epoch, replica.log_epoch, &replica.stream, &replica.storage_prefix, remote,
                ),
                (None, Some(placement)) => self.secret.ordered_reader_peer(
                    placement, target.epoch, target.enrollment.ok_or_else(moved)?, remote,
                ),
                (None, None) => self.secret.flight_peer(&target.deployment, target.epoch, target.fresh, remote),
                (Some(_), Some(_)) => return Err(moved()),
            };
            headers.insert(FORWARDED_PEER_HEADER, header_value(&peer)?);
        }
        let path = request.uri().path_and_query().ok_or_else(|| Status::invalid_argument("Invalid Flight SQL request path"))?.clone();
        *request.uri_mut() = Uri::builder().scheme("http").authority(target.address.as_str()).path_and_query(path)
            .build().map_err(|_| Status::internal("Invalid private Flight SQL endpoint"))?;
        let (parts, body) = request.into_parts();
        let body = match classic {
            Some(record) if parts.uri.path() == DO_ACTION =>
                self.inspect_classic_action(&record, &parts.headers, session, body).await?,
            _ => body.map_err(io::Error::other).boxed_unsync(),
        };
        let request = Request::from_parts(parts, body);
        let mut response = self.transport.request(request).await.map_err(|_| Status::unavailable("The Flight SQL attachment disconnected"))?;
        let deadline = single_header(response.headers(), GATEWAY_ANSWER_DEADLINE_HEADER)?
            .map(|header| header.parse::<u64>().ok().and_then(DeliveryDeadline::from_wire_nanos)
                .ok_or_else(|| Status::unavailable("The Flight holder's answer deadline expired"))).transpose()?;
        response.headers_mut().remove(GATEWAY_ANSWER_DEADLINE_HEADER);
        if let Some(deadline) = deadline {
            egress.restrict(deadline).map_err(|_| Status::unavailable("The Flight answer cannot reach this socket before expiry"))?;
        }
        self.rewrite_response(response.headers_mut(), target)?;
        Ok(response.map(|body| Either::Right(RoutedBody { body, _call: call, _deployment: deployment, deadline })))
    }

    async fn inspect_classic_action(
        &self, record: &DeploymentRecord, headers: &HeaderMap, session: Option<Uuid>, mut body: Incoming,
    ) -> std::result::Result<RequestBody, Status> {
        let proof = single_header(headers, classic_flight::RETAINED_PROOF_HEADER)?;
        let inspected = tokio::time::timeout(self.timeout, async {
            let mut wire = BytesMut::with_capacity(5);
            let mut message = None;
            while let Some(frame) = body.frame().await {
                let mut data = frame.map_err(|_| invalid_classic_action())?
                    .into_data().map_err(|_| invalid_classic_action())?;
                if message.is_none() {
                    let needed = (5 - wire.len()).min(data.len());
                    wire.extend_from_slice(&data.split_to(needed));
                    let Some(header) = wire.first_chunk::<5>() else { continue; };
                    let length = classic_action_length(header)?;
                    let memory = self.inspected_actions.reserve(4 * length.max(4096))
                        .map_err(|_| Status::resource_exhausted("Classic Flight action inspection capacity reached"))?;
                    wire.reserve(length - wire.len());
                    message = Some((length, memory));
                }
                let (length, _) = message.as_ref().ok_or_else(invalid_classic_action)?;
                if data.len() > *length - wire.len() { return Err(invalid_classic_action()); }
                wire.extend_from_slice(&data);
            }
            let (_, memory) = message.filter(|(length, _)| *length == wire.len()).ok_or_else(invalid_classic_action)?;
            Ok(InspectedAction { wire: wire.freeze(), _memory: memory })
        }).await.map_err(|_| Status::deadline_exceeded("Timed out reading the Classic Flight action"))??;
        let action = decode_classic_action(&inspected.wire)?;
        if let Some(proof) = proof {
            let key = self.retained_key.as_ref().filter(|_| {
                session.is_some() && rtdb_flight_sql::classic_retained_action(&action.r#type)
            }).ok_or_else(classic_retained_refusal)?;
            classic_flight::verify_forwarded_retained(key, record, proof, &action.r#type, &action.body)
                .map_err(|_| classic_retained_refusal())?;
        } else if action.r#type == "rtdb.owned.OpenFreshRead.v2" {
            return Err(classic_flight_refusal());
        }
        let wire = Bytes::from_owner(inspected);
        Ok(Full::new(wire).map_err(|never| -> io::Error { match never {} }).boxed_unsync())
    }

    fn rewrite_response(&self, headers: &mut HeaderMap, target: Target) -> std::result::Result<(), Status> {
        let Some(cookie) = headers.remove(header::SET_COOKIE) else { return Ok(()); };
        let cookie = cookie.to_str().map_err(|_| Status::internal("Invalid private Flight SQL cookie"))?;
        if cookie.contains("Max-Age=0") {
            headers.insert(header::SET_COOKIE, HeaderValue::from_static("arrow_flight_session_id=; Max-Age=0"));
            headers.remove(header::AUTHORIZATION);
            return Ok(());
        }
        let session = cookie.strip_prefix("arrow_flight_session_id=").and_then(|id| Uuid::parse_str(id).ok())
            .ok_or_else(|| Status::internal("Invalid private Flight SQL session"))?;
        if target.replica.is_some() || target.ordered_reader.is_some() {
            headers.insert(FRESH_HEADER, HeaderValue::from_static("1"));
        }
        let route = self.encode(&SessionRoute { target, session })?;
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
fn classic_flight_refusal() -> Status { Status::failed_precondition("The Classic Original excludes unclassified FreshFlight") }
fn classic_retained_refusal() -> Status { Status::permission_denied("The request lacks its exact Classic retained read authority") }
fn invalid_classic_action() -> Status { Status::invalid_argument("Expected one bounded uncompressed Classic Flight action") }

fn classic_action_length(header: &[u8; 5]) -> std::result::Result<usize, Status> {
    let [compressed, a, b, c, d] = *header;
    let length = u32::from_be_bytes([a, b, c, d]) as usize;
    if compressed != 0 || length > MAXIMUM_ACTION_FRAME_BYTES - 5 { return Err(invalid_classic_action()); }
    Ok(length + 5)
}

fn decode_classic_action(wire: &Bytes) -> std::result::Result<arrow_flight::Action, Status> {
    let header = wire.first_chunk::<5>().ok_or_else(invalid_classic_action)?;
    if classic_action_length(header)? != wire.len() { return Err(invalid_classic_action()); }
    arrow_flight::Action::decode(wire.slice(5..)).map_err(|_| invalid_classic_action())
}

#[cfg(test)]
#[test]
fn classic_action_inspection_matches_the_receiver_action_type() {
    let open = arrow_flight::Action { r#type: "rtdb.owned.OpenFreshRead.v2".to_owned(), body: Bytes::from_static(b"body") };
    let mut frame = vec![0];
    frame.extend_from_slice(&(open.encoded_len() as u32).to_be_bytes());
    open.encode(&mut frame).unwrap();
    assert_eq!(decode_classic_action(&Bytes::copy_from_slice(&frame)).unwrap(), open);
    frame.push(0);
    assert!(decode_classic_action(&Bytes::from(frame.clone())).is_err());
    frame.pop();
    frame[0] = 1;
    assert!(decode_classic_action(&Bytes::from(frame)).is_err());
}

fn directory_error(_: Anomaly) -> Status { Status::unavailable("The Flight SQL directory is unavailable") }
fn network(error: std::io::Error) -> Anomaly { Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.multi/flight-gateway", error.to_string()) }
