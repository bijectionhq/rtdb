use std::{pin::Pin, sync::Arc};
use arrow_flight::{self as flight, flight_service_server::FlightService, sql::{Any, Command, ProstMessageExt, TicketStatementQuery}};
use bytes::Bytes;
use futures::Stream;
use prost::Message;
use rtdb_auth::Authenticator;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_node::{Clock, DatabaseAccess, Node, SessionIdentity};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;
use crate::{AUDIENCE_FLIGHT_HEADER, FORWARDED_PEER_HEADER, AudienceFlightConfig, FlightConfig, auth::{self, Authentication, Credentials}, error::{arrow, cancelled, status, unsupported}, lease::{Leased, Lookup, Registry}, server::ServerStatus, state::{ConnectionResource, Prepared, PreparedScope, Session, Transaction}, wire};

pub(crate) type Output<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;
#[derive(Clone)]
pub(crate) struct Service(pub Arc<Inner>);
pub(crate) struct Inner {
    pub node: Node,
    pub config: FlightConfig,
    pub cancellation: CancellationToken,
    pub sessions: Registry<String, Session>,
    pub transactions: Registry<Bytes, Transaction>,
    pub prepared: Registry<Bytes, Prepared>,
    pub owned_reads: Registry<Bytes, crate::owned_actions::OwnedReadResource>,
    pub owned_world_reads: Registry<Bytes, crate::owned_actions::OwnedWorldReadResource>,
    pub owned_read_slots: Arc<Semaphore>,
    pub audience: Option<AudienceAdmission>,
    auth: Authentication,
    permits: Arc<Semaphore>,
    pub status: watch::Sender<ServerStatus>,
}

pub(crate) struct AudienceAdmission {
    calls: Arc<Semaphore>,
    pub holds: Arc<Semaphore>,
}

#[derive(Clone, Copy)]
enum CallKind { Ordinary, Handshake, Action }
pub(crate) struct Call {
    pub service: Service,
    pub session: Arc<Session>,
    pub minted: bool,
    pub database: String,
    pub cancellation: CancellationToken,
    _admission: Admission,
    _lease: Option<AcquiredSession>,
    _active_call: Option<Box<dyn Send + Sync>>,
    _reservation: Arc<MemoryReservation>,
}
struct Admission { service: Service, permit: Option<OwnedSemaphorePermit> }
impl Admission {
    fn new(service: Service, permit: OwnedSemaphorePermit) -> Self { service.0.status.send_modify(|status| status.active_calls += 1); Self { service, permit: Some(permit) } }
}
impl Drop for Admission { fn drop(&mut self) { self.permit.take(); self.service.0.status.send_modify(|status| status.active_calls -= 1); self.service.publish(); } }
struct AcquiredSession { service: Service, session: Arc<Session> }
impl Drop for AcquiredSession { fn drop(&mut self) { self.service.0.sessions.release(&self.session.id); self.service.publish(); } }
impl Drop for Call {
    fn drop(&mut self) { self.cancellation.cancel(); self._lease.take(); }
}
impl Call {
    pub fn response<T>(&self, value: T) -> Result<Response<T>, Status> {
        let mut response = Response::new(value);
        if self.session.closed() {
            if !self.minted { response.metadata_mut().insert("set-cookie", format!("{}={}; Max-Age=0", auth::COOKIE, self.session.id).parse().map_err(arrow)?); }
        } else if self.minted {
            response.metadata_mut().insert("set-cookie", format!("{}={}", auth::COOKIE, self.session.id).parse().map_err(arrow)?);
            if self.session.caller.mac.is_some() { response.metadata_mut().insert("authorization", format!("Bearer {}", self.session.id).parse().map_err(arrow)?); }
            if let Some(source) = self.session.audience_source {
                let audience = self.service.0.config.audience.filter(|profile| profile.source() == source)
                    .ok_or_else(unsupported)?;
                response.metadata_mut().insert(AUDIENCE_FLIGHT_HEADER, audience.to_string().parse().map_err(arrow)?);
            }
        }
        Ok(response)
    }
    pub fn reserve(&self, bytes: usize) -> Result<Arc<MemoryReservation>, Status> { self.service.0.node.memory().reserve(bytes).map_err(status) }
    pub fn error(&self, mut error: Status) -> Status {
        if let Ok(response) = self.response(()) { for key in ["set-cookie", "authorization"] { if let Some(value) = response.metadata().get(key) { error.metadata_mut().insert(key, value.clone()); } } }
        error
    }
}
impl Service {
    pub fn new(node: Node, auth: Arc<Authenticator>, config: FlightConfig, clock: Arc<dyn Clock>, cancellation: CancellationToken, status: watch::Sender<ServerStatus>) -> Self {
        let memory = Arc::clone(node.memory());
        let audience = config.audience.map(|audience| AudienceAdmission {
            calls: Arc::new(Semaphore::new(audience.permits())),
            holds: Arc::new(Semaphore::new(audience.permits())),
        });
        Self(Arc::new(Inner { sessions: Registry::new(clock.clone(), memory.clone()), transactions: Registry::new(clock.clone(), memory.clone()), prepared: Registry::new(clock.clone(), memory.clone()), owned_reads: Registry::new(clock.clone(), memory.clone()), owned_world_reads: Registry::new(clock, memory), owned_read_slots: Arc::new(Semaphore::new(64)), audience, auth: Authentication::new(auth), permits: Arc::new(Semaphore::new(config.maximum_concurrent_calls)), node, config, cancellation, status }))
    }
    pub fn memory(&self) -> &Arc<MemoryBudget> { self.0.node.memory() }
    pub fn sweep(&self) { self.0.sessions.sweep(); self.0.transactions.sweep(); self.0.prepared.sweep(); self.0.owned_reads.sweep(); self.0.owned_world_reads.sweep(); self.publish(); }
    pub fn close(&self) { self.0.owned_world_reads.close(); self.0.owned_reads.close(); self.0.prepared.close(); self.0.transactions.close(); self.0.sessions.close(); self.publish(); }
    pub fn publish(&self) {
        let counts = (self.0.sessions.len(), self.0.transactions.len(), self.0.prepared.len());
        self.0.status.send_if_modified(|status| {
            let changed = (status.sessions, status.transactions, status.prepared_statements) != counts;
            (status.sessions, status.transactions, status.prepared_statements) = counts;
            changed
        });
    }
    pub async fn call<T>(&self, request: &Request<T>, body_bytes: usize) -> Result<Call, Status> {
        self.call_with_kind(request, body_bytes, CallKind::Ordinary).await
    }
    async fn call_with_kind<T>(&self, request: &Request<T>, body_bytes: usize, kind: CallKind) -> Result<Call, Status> {
        if self.0.cancellation.is_cancelled() { return Err(cancelled()); }
        let source_headers = request.metadata().get_all(AUDIENCE_FLIGHT_HEADER);
        let mut sources = source_headers.iter();
        let audience_source = sources.next().map(|source| {
            source.to_str().ok().and_then(|source| AudienceFlightConfig::parse(source).ok())
                .filter(|profile| self.0.config.audience == Some(*profile))
                .map(AudienceFlightConfig::source)
                .ok_or_else(|| Status::permission_denied("Flight audience reservation is not installed"))
        }).transpose()?;
        if sources.next().is_some() || audience_source.is_some() && matches!(kind, CallKind::Ordinary) {
            return Err(Status::permission_denied("Flight audience reservation requires its observation transport"));
        }
        let permits = match (audience_source, kind) {
            (Some(_), CallKind::Action) => &self.0.audience.as_ref().ok_or_else(unsupported)?.calls,
            _ => &self.0.permits,
        };
        let permit = Arc::clone(permits).try_acquire_owned().map_err(|_| Status::resource_exhausted("Flight concurrent-call limit reached"))?;
        let admission = Admission::new(self.clone(), permit);
        let header_bytes = request.metadata().iter().try_fold(4096usize, |bytes, entry| {
            let size = match entry { tonic::metadata::KeyAndValueRef::Ascii(key, value) => wire::add(key.as_str().len(), value.as_encoded_bytes().len())?, tonic::metadata::KeyAndValueRef::Binary(key, value) => wire::add(key.as_str().len(), value.as_encoded_bytes().len())? };
            wire::add(bytes, wire::mul(size, 16)?)
        })?;
        let reservation = self.memory().reserve(wire::add(header_bytes, wire::mul(body_bytes, 32)?)?).map_err(status)?;
        let requested_database = request.metadata().get("x-xtdb-database").map(|value| value.to_str().map_err(arrow)).transpose()?;
        let forwarded_peer = request.metadata().get(FORWARDED_PEER_HEADER);
        let credentials = Credentials::parse(request)?;
        let cookie = auth::cookie(request); let bearer = credentials.bearer();
        if let (Some(cookie), Some(bearer)) = (cookie, bearer) { if !auth::equal(cookie.as_bytes(), bearer.as_bytes()) { return Err(auth::unauthenticated("the session cookie and bearer token name different sessions")); } }
        let (session, minted, lease) = if let Some(id) = bearer.or(cookie) {
            if forwarded_peer.is_some() {
                return Err(auth::unauthenticated("Forwarded peer proof is for a new session"));
            }
            let session = match self.0.sessions.lookup(&id.to_owned(), true) {
                Lookup::Found(session) => session,
                Lookup::Expired(after) => return Err(session_error(bearer.is_some(), format!("session expired after {} idle", crate::lease::duration(after)))),
                Lookup::Unknown => return Err(session_error(bearer.is_some(), "unknown session")),
            };
            let lease = AcquiredSession { service: self.clone(), session: Arc::clone(&session) };
            if session.audience_source != audience_source || audience_source.is_some() && !matches!(kind, CallKind::Action) {
                return Err(Status::permission_denied("Flight session belongs to another admission class"));
            }
            if let Some(raw) = credentials.raw() {
                if session.caller.mac.is_none_or(|mac| !auth::equal(&mac, &self.0.auth.mac(raw))) { return Err(auth::unauthenticated("these credentials aren't the ones the session was authenticated with")); }
            }
            let verified = self.0.cancellation.run_until_cancelled(session.caller.principal.revalidate(self.0.node.now())).await.ok_or_else(cancelled)?;
            if let Err(error) = verified { if let Lookup::Found(removed) = self.0.sessions.take(&session.id) { removed.close(); } return Err(auth::auth_error(error)); }
            (session, false, lease)
        } else {
            if audience_source.is_some() && (!matches!(kind, CallKind::Handshake) || forwarded_peer.is_none()) {
                return Err(Status::permission_denied("An audience session requires an authenticated placed handshake"));
            }
            let catalog = self.0.node.catalog();
            let placed_name = || requested_database.filter(|name| !name.is_empty())
                .ok_or_else(|| Status::invalid_argument("A placed Flight SQL session requires x-xtdb-database"));
            let (peer, verified_attachment) = if let Some(proof) = forwarded_peer {
                let verifier = self.0.config.forwarded_peer.as_ref()
                    .ok_or_else(|| auth::unauthenticated("This listener does not accept forwarded peers"))?;
                let proof = proof.to_str().map_err(|_| auth::unauthenticated("Invalid forwarded peer proof"))?;
                let attachment = catalog.database(placed_name()?).map_err(status)?;
                let peer = verifier.verify(proof, &attachment).map_err(auth::auth_error)?;
                (Some(peer.to_string()), Some(attachment))
            } else {
                (request.remote_addr().map(|address| address.ip().to_string()), None)
            };
            let caller = self.0.cancellation.run_until_cancelled(self.0.auth.authenticate(&credentials, peer.as_deref())).await.ok_or_else(cancelled)??;
            let placement = if catalog.is_placed() {
                let name = placed_name()?;
                caller.principal.authorize_database(name, DatabaseAccess::Read).map_err(status)?;
                match verified_attachment { Some(attachment) => Some(attachment), None => Some(catalog.database(name).map_err(status)?) }
            } else { None };
            if audience_source.is_some() && (placement.is_none() || self.0.config.hosted_controls.as_ref()
                .is_none_or(|controls| caller.principal.user_id() != Some(controls.owner.as_str())))
            {
                return Err(Status::permission_denied("An audience session requires the installed hosted principal"));
            }
            let capacity = 1024 + placement.as_ref().map_or(0, |database| database.name().len());
            let session = Arc::new(Session::new(Uuid::new_v4().to_string(), caller, placement.as_ref(), audience_source, self.memory().reserve(capacity).map_err(status)?));
            self.0.sessions.insert(session.id.clone(), Arc::clone(&session), self.0.config.session_idle_timeout, self.0.config.session_unclaimed_timeout, true, session.id.len()).map_err(status)?;
            let lease = AcquiredSession { service: self.clone(), session: Arc::clone(&session) };
            (session, true, lease)
        };
        let (database, attachment) = session.resolve_database(requested_database, &self.0.node)?;
        let active_call = attachment.as_deref().and_then(|attachment| self.0.node.hold_active_call(attachment));
        let call = Call { service: self.clone(), session, minted, database, cancellation: self.0.cancellation.child_token(),
            _admission: admission, _lease: Some(lease), _active_call: active_call, _reservation: reservation };
        self.publish(); Ok(call)
    }
    pub async fn admit_database(&self, database: &str) -> Result<bool, Status> {
        if self.0.config.playground { self.0.node.database_or_attach(database.into(), Default::default()).await.map(|database| database.is_some()).map_err(status) }
        else { Ok(self.0.node.catalog().database(database).is_ok()) }
    }
    pub async fn connection(&self, call: &Call, database: &str) -> Result<Arc<ConnectionResource>, Status> {
        if self.0.config.playground { self.admit_database(database).await?; }
        call.session.connection(database, &self.0.node)
    }
    pub fn transaction(&self, call: &Call, handle: &Bytes) -> Result<Arc<Transaction>, Status> {
        let value = found(self.0.transactions.lookup(handle, false), "transaction")?;
        if !call.session.owns(&value.owner) { return Err(Status::not_found("unknown transaction")); } Ok(value)
    }
    pub fn prepared(&self, call: &Call, handle: &Bytes) -> Result<Arc<Prepared>, Status> {
        let value = found(self.0.prepared.lookup(handle, false), "prepared statement")?;
        if !call.session.owns(&value.owner) { return Err(Status::not_found("unknown prepared statement")); } Ok(value)
    }
    pub async fn scoped(&self, call: &Call, scope: &PreparedScope) -> Result<Arc<ConnectionResource>, Status> {
        match scope { PreparedScope::Session(database) => self.connection(call, database).await, PreparedScope::Transaction(handle) => Ok(Arc::clone(&self.transaction(call, handle)?.connection)) }
    }
    pub async fn tx_or_session(&self, call: &Call, transaction: Option<&Bytes>) -> Result<Arc<ConnectionResource>, Status> {
        match transaction { Some(handle) => Ok(Arc::clone(&self.transaction(call, handle)?.connection)), None => self.connection(call, &call.database).await }
    }
}
pub(crate) fn found<V>(value: Lookup<V>, what: &str) -> Result<Arc<V>, Status> { match value { Lookup::Found(value) => Ok(value), Lookup::Expired(after) => Err(Status::not_found(format!("{what} expired after {} idle", crate::lease::duration(after)))), Lookup::Unknown => Err(Status::not_found(format!("unknown {what}"))) } }
fn session_error(bearer: bool, message: impl Into<String>) -> Status { if bearer { Status::unauthenticated(message) } else { Status::not_found(message) } }
pub(crate) fn command(bytes: &[u8]) -> Result<Command, Status> { Command::try_from(Any::decode(bytes).map_err(arrow)?).map_err(arrow) }
pub(crate) fn result_stream<T: Send + 'static>(call: Call, values: Vec<Result<T, Status>>) -> Result<Response<Output<T>>, Status> {
    let headers = call.response(())?.metadata().clone();
    let stream: Output<T> = Box::pin(async_stream::stream! { let _call = call; for value in values { if _call.cancellation.is_cancelled() { yield Err(cancelled()); break; } yield value; } });
    let mut response = Response::new(stream); *response.metadata_mut() = headers; Ok(response)
}

#[tonic::async_trait]
impl FlightService for Service {
    type HandshakeStream = Output<flight::HandshakeResponse>;
    type ListFlightsStream = Output<flight::FlightInfo>;
    type DoGetStream = Output<flight::FlightData>;
    type DoPutStream = Output<flight::PutResult>;
    type DoActionStream = Output<flight::Result>;
    type ListActionsStream = Output<flight::ActionType>;
    type DoExchangeStream = Output<flight::FlightData>;
    async fn handshake(&self, request: Request<Streaming<flight::HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> {
        let call = self.call_with_kind(&request, 0, CallKind::Handshake).await?;
        result_stream(call, Vec::new())
    }
    async fn list_flights(&self, request: Request<flight::Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> { let call = self.call(&request, request.get_ref().encoded_len()).await?; Err(call.error(unsupported())) }
    async fn poll_flight_info(&self, request: Request<flight::FlightDescriptor>) -> Result<Response<flight::PollInfo>, Status> { let call = self.call(&request, request.get_ref().encoded_len()).await?; Err(call.error(unsupported())) }
    async fn do_exchange(&self, request: Request<Streaming<flight::FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> { let call = self.call(&request, 0).await?; Err(call.error(unsupported())) }
    async fn list_actions(&self, request: Request<flight::Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        let call = self.call(&request, 0).await?;
        let actions = ["BeginSavepoint", "BeginTransaction", "CreatePreparedStatement", "ClosePreparedStatement", "CreatePreparedSubstraitPlan", "CancelQuery", "EndSavepoint", "EndTransaction"];
        result_stream(call, actions.into_iter().map(|name| Ok(flight::ActionType { r#type: name.into(), description: name.into() })).collect())
    }
    async fn get_flight_info(&self, request: Request<flight::FlightDescriptor>) -> Result<Response<flight::FlightInfo>, Status> {
        let call = self.call(&request, request.get_ref().encoded_len()).await?;
        let descriptor = request.into_inner(); let cmd = command(&descriptor.cmd).map_err(|error| call.error(error))?;
        let work = async {
            let (schema, ticket) = match cmd {
                Command::CommandStatementQuery(command) => { let connection = self.tx_or_session(&call, command.transaction_id.as_ref()).await?; self.query_info(connection, &command.query, None).await? }
                Command::CommandPreparedStatementQuery(command) => { let prepared = self.prepared(&call, &command.prepared_statement_handle)?; let params = prepared.params.lock().expect("parameters lock").clone(); let connection = self.scoped(&call, &prepared.scope).await?; self.query_info(connection, &prepared.sql, params).await? }
                command => (self.metadata_schema(&command)?, command.into_any().encode_to_vec().into()),
            };
            Ok::<_, Status>(flight::FlightInfo { schema: wire::schema_message(&schema, self.memory())?, flight_descriptor: Some(descriptor), endpoint: vec![flight::FlightEndpoint { ticket: Some(flight::Ticket { ticket }), ..Default::default() }], total_records: -1, total_bytes: -1, ordered: false, app_metadata: Bytes::new() })
        };
        let mut result = call.cancellation.run_until_cancelled(work).await.ok_or_else(cancelled).and_then(|result| result).map_err(|error| call.error(error))?;
        result.schema = wire::retain(result.schema, Arc::clone(&call._reservation));
        call.response(result)
    }
    async fn get_schema(&self, request: Request<flight::FlightDescriptor>) -> Result<Response<flight::SchemaResult>, Status> {
        let call = self.call(&request, request.get_ref().encoded_len()).await?;
        let command = command(&request.into_inner().cmd).map_err(|error| call.error(error))?;
        let work = async {
            let schema = match command {
                Command::CommandStatementQuery(command) => { let connection = self.connection(&call, &call.database).await?; self.describe(connection, &command.query).await?.0 }
                Command::CommandPreparedStatementQuery(command) => { let prepared = self.prepared(&call, &command.prepared_statement_handle)?; let connection = self.scoped(&call, &prepared.scope).await?; self.describe(connection, &prepared.sql).await?.0 }
                command => self.metadata_schema(&command)?,
            };
            Ok::<_, Status>(flight::SchemaResult { schema: wire::schema_message(&schema, self.memory())? })
        };
        let result = call.cancellation.run_until_cancelled(work).await.ok_or_else(cancelled).and_then(|result| result).map_err(|error| call.error(error))?; call.response(result)
    }
    async fn do_get(&self, request: Request<flight::Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let call = self.call(&request, request.get_ref().encoded_len()).await?;
        let command = command(&request.into_inner().ticket).map_err(|error| call.error(error))?;
        match command {
            Command::TicketStatementQuery(ticket) => self.query_stream(call, ticket.statement_handle).await,
            command => self.metadata_stream(call, command).await,
        }
    }
    async fn do_put(&self, request: Request<Streaming<flight::FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        let call = self.call(&request, 0).await?;
        self.upload(call, request.into_inner()).await
    }
    async fn do_action(&self, request: Request<flight::Action>) -> Result<Response<Self::DoActionStream>, Status> {
        let call = self.call_with_kind(&request, request.get_ref().encoded_len(), CallKind::Action).await?;
        self.action(call, request.into_inner()).await
    }
}

pub(crate) fn query_ticket(ticket: Bytes, memory: &Arc<MemoryBudget>) -> Result<Bytes, Status> {
    let charge = memory.reserve(wire::add(wire::mul(ticket.len(), 3)?, 1024)?).map_err(status)?;
    Ok(wire::retain(TicketStatementQuery { statement_handle: ticket }.as_any().encode_to_vec(), charge))
}
