use std::{pin::Pin, sync::{Arc, Weak}, time::Duration};
use arrow_flight::{self as flight, flight_service_server::FlightService, sql::{Any, Command, ProstMessageExt, TicketStatementQuery}};
use bytes::Bytes;
use futures::Stream;
use prost::Message;
use rtdb_auth::Authenticator;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_node::{Clock, DatabaseAccess, Node, SessionIdentity};
use rtdb_storage::cache::CacheBudget;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch};
use tokio_util::sync::{CancellationToken, DropGuard};
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;
use crate::config::accepts_seat_header;
use rtdb_storage::sealing::{DeliveryDeadline, PlaintextDelivery};
use crate::{AUDIENCE_FLIGHT_HEADER, AUDIENCE_SEAT_HEADER, FORWARDED_PEER_HEADER, AudienceFlightConfig, FlightConfig, auth::{self, Authentication, Credentials}, error::{arrow, cancelled, status, unsupported}, lease::{Leased, Lookup, Registry}, server::{Binding, ServerStatus}, state::{AudienceSession, ConnectionResource, Prepared, PreparedScope, Session, Transaction}, wire};
use crate::audience_seat::{OccupiedSeatAction, SeatAttachment, SeatDecision, SeatState, admit_audience_seat, occupied_seat_action};

#[macro_use]
mod audience_owner;

pub(crate) const CLOSE_SESSION: &str = "CloseSession";

pub(crate) type Output<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;
#[derive(Clone)]
pub(crate) struct Service(pub Arc<Inner>);
pub(crate) struct Inner {
    pub node: Node,
    pub(crate) v1: crate::v1::State,
    pub config: FlightConfig,
    binding: Binding,
    pub cancellation: CancellationToken,
    pub sessions: Registry<String, Session>,
    pub transactions: Registry<Bytes, Transaction>,
    pub prepared: Registry<Bytes, Prepared>,
    pub query_tickets: Registry<Uuid, crate::execute::QueryTicket>,
    pub query_ticket_slots: Arc<Semaphore>,
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
    seats: Vec<AudienceSessionSeat>,
    memory: Arc<MemoryBudget>,
    cache: Arc<CacheBudget>,
    pub(crate) transport: Arc<MemoryBudget>,
    sessions: Registry<String, Session>,
}

struct AudienceSessionSeat {
    permit: Arc<Semaphore>,
    original: tokio::sync::Mutex<Weak<Session>>,
}

#[derive(Clone, Copy)]
enum CallKind { Ordinary, Handshake, Action, AudienceCleanup, V1Query, V1Changes, V1Capabilities, V1Action, ManagedCreate, ManagedWorkload, TxStatus }

impl CallKind {
    fn allows_label_scope(self) -> bool {
        matches!(self, Self::Handshake | Self::V1Query | Self::V1Changes | Self::V1Capabilities | Self::V1Action | Self::TxStatus)
    }

    fn serves_assigned_follower(self) -> bool {
        matches!(self, Self::Handshake | Self::V1Query | Self::V1Capabilities)
    }

    fn allows_workload_grant(self) -> bool {
        matches!(self, Self::Handshake | Self::ManagedCreate | Self::ManagedWorkload | Self::TxStatus)
    }

    fn action(audience: bool, name: &str) -> Self {
        if audience && matches!(name, CLOSE_SESSION | crate::owned_actions::CLOSE_OWNED_READ_V1) {
            Self::AudienceCleanup
        } else {
            Self::Action
        }
    }
}
pub(crate) struct Call {
    service: Service,
    kind: CallKind,
    pub session: Arc<Session>,
    pub minted: bool,
    pub database: String,
    pub cancellation: CancellationToken,
    admission: Arc<Admission>,
    _lease: Option<AcquiredSession>,
    _active_call: Option<Arc<dyn Send + Sync>>,
    _reservation: Arc<MemoryReservation>,
    egress: Option<Arc<crate::server::FlightEgress>>,
    routed_delivery: bool,
}
struct Admission { service: Service, permit: Option<OwnedSemaphorePermit> }
impl Admission {
    fn new(service: Service, permit: OwnedSemaphorePermit) -> Self { service.0.status.send_modify(|status| status.active_calls += 1); Self { service, permit: Some(permit) } }
}
impl Drop for Admission { fn drop(&mut self) { self.permit.take(); self.service.0.status.send_modify(|status| status.active_calls -= 1); self.service.publish(); } }
struct AcquiredSession { service: Service, session: Arc<Session> }
impl Drop for AcquiredSession { fn drop(&mut self) { self.service.sessions(self.session.audience_source().is_some()).release(&self.session.id); self.service.publish(); } }
impl Drop for Call {
    fn drop(&mut self) { self.cancellation.cancel(); self._lease.take(); }
}
impl Call {
    pub(crate) fn audience_io_owner(&self) -> Option<Arc<dyn Send + Sync>> {
        capture_audience_io_owner!(self)
    }

    pub(crate) fn handoff(&self, grant: PlaintextDelivery, relay: Option<DeliveryDeadline>) -> Result<(), Status> {
        if let Some(egress) = &self.egress { return egress.admit(grant, relay).map_err(status); }
        grant.check().map_err(status)?;
        if grant.is_bounded() || relay.is_some() { return Err(Status::unavailable("Flight answer transport has no delivery fence")); }
        grant.observe_answer();
        Ok(())
    }

    pub(crate) fn handoff_action(&self, grant: PlaintextDelivery, relay: Option<DeliveryDeadline>) -> Result<(), Status> {
        let handoff = || self.handoff(grant, relay);
        if self.session.audience_source().is_some() {
            self.session.with_audience_handoff(matches!(self.kind, CallKind::AudienceCleanup), handoff)
        } else {
            handoff()
        }
    }

    pub(crate) fn delivery_headers(
        &self, grant: &PlaintextDelivery, streaming: bool,
    ) -> Result<(tonic::metadata::MetadataMap, Option<DeliveryDeadline>), Status> {
        let mut headers = self.response(())?.metadata().clone();
        let cutoff = if self.routed_delivery {
            grant.transport_deadline().or_else(|| streaming.then(DeliveryDeadline::answer_window))
        } else { None };
        if let Some(cutoff) = cutoff {
            let wire = cutoff.wire_nanos().ok_or_else(|| Status::unavailable("Flight answer deadline cannot be routed"))?;
            headers.insert(crate::GATEWAY_ANSWER_DEADLINE_HEADER, wire.to_string().parse().map_err(arrow)?);
        }
        Ok((headers, cutoff))
    }

    pub fn response<T>(&self, value: T) -> Result<Response<T>, Status> {
        let mut response = Response::new(value);
        if self.session.closed() {
            if !self.minted { response.metadata_mut().insert("set-cookie", format!("{}={}; Max-Age=0", auth::COOKIE, self.session.id).parse().map_err(arrow)?); }
        } else if self.minted {
            response.metadata_mut().insert("set-cookie", format!("{}={}", auth::COOKIE, self.session.id).parse().map_err(arrow)?);
            if self.session.caller.mac.is_some() { response.metadata_mut().insert("authorization", format!("Bearer {}", self.session.id).parse().map_err(arrow)?); }
            if let Some(source) = self.session.audience_source() {
                let audience = self.service.0.config.audience.filter(|profile| profile.source() == source)
                    .ok_or_else(unsupported)?;
                let seat = self.session.audience_seat().ok_or_else(unsupported)?;
                response.metadata_mut().insert(AUDIENCE_FLIGHT_HEADER, audience.to_string().parse().map_err(arrow)?);
                response.metadata_mut().insert(AUDIENCE_SEAT_HEADER, seat.to_string().parse().map_err(arrow)?);
            }
        }
        Ok(response)
    }
    pub fn memory(&self) -> &Arc<MemoryBudget> { self.session.memory() }
    pub fn reserve(&self, bytes: usize) -> Result<Arc<MemoryReservation>, Status> { self.memory().reserve(bytes).map_err(status) }
    pub(crate) fn request_reservation(&self) -> Arc<MemoryReservation> {
        Arc::clone(&self._reservation)
    }
    pub(crate) fn retain_request_bytes(&self, bytes: impl Into<Bytes>) -> Bytes {
        wire::retain(bytes, self.request_reservation())
    }
    pub fn close_session(&self) {
        self.service.retire_session(&self.session);
    }
    pub fn error(&self, mut error: Status) -> Status {
        if let Ok(response) = self.response(()) { for key in ["set-cookie", "authorization"] { if let Some(value) = response.metadata().get(key) { error.metadata_mut().insert(key, value.clone()); } } }
        error
    }
}
impl Service {
    pub fn new(node: Node, auth: Arc<Authenticator>, config: FlightConfig, clock: Arc<dyn Clock>, cancellation: CancellationToken, status: watch::Sender<ServerStatus>, binding: Binding) -> rtdb_types::Result<Self> {
        let memory = Arc::clone(node.memory());
        let audience = if let Some(profile) = config.audience {
            let cache = CacheBudget::with_memory(memory.prepay(profile.cache_bytes())?);
            let transport = memory.prepay(profile.transport_bytes())?;
            let memory = memory.prepay(profile.execution_bytes())?;
            Some(AudienceAdmission {
                calls: Arc::new(Semaphore::new(profile.permits())),
                seats: (0..profile.session_seats()).map(|_| AudienceSessionSeat {
                    permit: Arc::new(Semaphore::new(1)), original: tokio::sync::Mutex::new(Weak::new()),
                }).collect(),
                sessions: Registry::new(clock.clone(), memory.clone()),
                cache,
                memory,
                transport,
            })
        } else { None };
        Ok(Self(Arc::new(Inner { sessions: Registry::new(clock.clone(), memory.clone()), transactions: Registry::new(clock.clone(), memory.clone()), prepared: Registry::new(clock.clone(), memory.clone()), query_tickets: Registry::new(clock.clone(), memory.clone()), query_ticket_slots: Arc::new(Semaphore::new(config.maximum_concurrent_calls)), owned_reads: Registry::new(clock.clone(), memory.clone()), owned_world_reads: Registry::new(clock, memory), owned_read_slots: Arc::new(Semaphore::new(64)), audience, v1: Default::default(), auth: Authentication::new(auth), permits: Arc::new(Semaphore::new(config.maximum_concurrent_calls)), node, config, binding, cancellation, status })))
    }
    pub(crate) fn standalone_custody_owner(&self) -> Option<&'static str> {
        matches!(&self.0.binding, Binding::Standalone).then_some("xtdb")
    }
    #[cfg(feature = "multi")]
    pub(crate) fn follower(&self) -> Option<&crate::follower::FollowerRoute> {
        match &self.0.binding { Binding::Follower(route) => Some(route), _ => None }
    }
    pub(crate) fn follower_only(&self) -> bool {
        match &self.0.binding {
            Binding::Ordinary | Binding::Standalone => false,
            #[cfg(feature = "multi")]
            Binding::Follower(_) => true,
        }
    }
    pub fn memory(&self) -> &Arc<MemoryBudget> { self.0.node.memory() }
    fn sessions(&self, reserved: bool) -> &Registry<String, Session> {
        if reserved {
            &self.0.audience.as_ref().expect("reserved session has an installed audience").sessions
        } else {
            &self.0.sessions
        }
    }
    fn retire_session(&self, session: &Session) {
        if let Lookup::Found(registered) = self.sessions(session.audience_source().is_some()).take(&session.id) {
            registered.close();
        }
        if session.audience_source().is_none() {
            self.0.owned_reads.close_where(|read| read.session == session.id);
            self.0.owned_world_reads.close_where(|read| read.session == session.id);
        }
    }
    pub fn sweep(&self) {
        if let Some(audience) = &self.0.audience {
            audience.sessions.sweep();
        }
        self.0.sessions.sweep(); self.0.transactions.sweep(); self.0.prepared.sweep(); self.0.query_tickets.sweep(); self.0.owned_reads.sweep(); self.0.owned_world_reads.sweep();
        self.publish();
    }
    pub fn close(&self) {
        if let Some(audience) = &self.0.audience {
            audience.sessions.close();
        }
        self.0.query_tickets.close(); self.0.owned_world_reads.close(); self.0.owned_reads.close(); self.0.prepared.close(); self.0.transactions.close(); self.0.sessions.close();
        self.publish();
    }
    pub fn publish(&self) {
        let reserved = self.0.audience.as_ref().map_or(0, |audience| audience.sessions.len());
        let counts = (self.0.sessions.len() + reserved, self.0.transactions.len(), self.0.prepared.len());
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
        if self.follower_only() && (!kind.serves_assigned_follower()
            || request.metadata().contains_key(FORWARDED_PEER_HEADER)
            || request.metadata().contains_key(crate::GATEWAY_ANSWER_DEADLINE_HEADER))
        {
            return Err(follower_refusal());
        }
        let transport = request.extensions().get::<crate::server::FlightConnectInfo>();
        let source_headers = request.metadata().get_all(AUDIENCE_FLIGHT_HEADER);
        let mut sources = source_headers.iter();
        let audience_source = sources.next().map(|source| {
            source.to_str().ok().and_then(|source| AudienceFlightConfig::parse(source).ok())
                .filter(|profile| self.0.config.audience == Some(*profile))
                .map(AudienceFlightConfig::source)
                .ok_or_else(|| Status::permission_denied("Flight audience reservation is not installed"))
        }).transpose()?;
        if sources.next().is_some() || audience_source.is_some() != self.0.config.audience.is_some()
            || audience_source.is_some() && matches!(kind, CallKind::Ordinary | CallKind::V1Query | CallKind::V1Changes) {
            return Err(Status::permission_denied("Flight audience reservation requires its observation transport"));
        }
        let audience = if audience_source.is_some() { Some(self.0.audience.as_ref().ok_or_else(unsupported)?) } else { None };
        let seat_headers = request.metadata().get_all(AUDIENCE_SEAT_HEADER);
        let mut seats = seat_headers.iter();
        let (first, second) = (seats.next(), seats.next());
        let is_handshake = matches!(kind, CallKind::Handshake);
        let seat = match first {
            Some(value) if accepts_seat_header(audience.is_some(), is_handshake, true, second.is_some()) => {
                let profile = self.0.config.audience.ok_or_else(unsupported)?;
                let seat = value.to_str().ok().and_then(|value| profile.parse_seat(value).ok())
                    .ok_or_else(|| Status::permission_denied("Invalid audience reader seat"))?;
                Some(seat)
            }
            _ if first.is_some() || (audience.is_some() && is_handshake) => {
                return Err(Status::permission_denied("An audience seat is fixed at its authenticated handshake"));
            }
            _ => None,
        };
        let sessions = self.sessions(audience.is_some());
        let permits = audience.map_or(&self.0.permits, |audience| &audience.calls);
        let permit = Arc::clone(permits).try_acquire_owned().map_err(|_| Status::resource_exhausted("Flight concurrent-call limit reached"))?;
        let admission = Arc::new(Admission::new(self.clone(), permit));
        let header_bytes = request.metadata().iter().try_fold(4096usize, |bytes, entry| {
            let size = match entry { tonic::metadata::KeyAndValueRef::Ascii(key, value) => wire::add(key.as_str().len(), value.as_encoded_bytes().len())?, tonic::metadata::KeyAndValueRef::Binary(key, value) => wire::add(key.as_str().len(), value.as_encoded_bytes().len())? };
            wire::add(bytes, wire::mul(size, 16)?)
        })?;
        let request_memory = audience.map_or(self.memory(), |audience| &audience.memory);
        let reservation = request_memory.reserve(wire::add(header_bytes, wire::mul(body_bytes, 32)?)?).map_err(status)?;
        let requested_database = request.metadata().get("x-xtdb-database").map(|value| value.to_str().map_err(arrow)).transpose()?;
        let follower_attachment: Option<Arc<rtdb_node::Database>> = None;
        #[cfg(feature = "multi")]
        let (requested_database, follower_attachment) = if let Some(follower) = self.follower() {
            (Some(follower.select(request.metadata()).map_err(crate::v1::error::follower)?), Some(follower.current(&self.0.node).map_err(crate::v1::error::follower)?))
        } else { (requested_database, follower_attachment) };
        let forwarded_peer = request.metadata().get(FORWARDED_PEER_HEADER);
        let admit_workload = |principal: &rtdb_auth::Principal, fresh_replica| {
            if let Some(grant) = principal.workload_grant() {
                if !kind.allows_workload_grant()
                    || matches!(kind, CallKind::ManagedCreate) && grant.is_assignment()
                    || matches!(kind, CallKind::ManagedWorkload | CallKind::TxStatus) && !grant.is_assignment()
                    || audience.is_some()
                    || self.follower_only()
                    || fresh_replica
                    || forwarded_peer.is_some()
                    || requested_database.is_some_and(|name| name != grant.database())
                    || !self.0.node.catalog().is_managed()
                {
                    return Err(Status::permission_denied("The credential requires its exact managed workload route"));
                }
            }
            Ok(())
        };
        let credentials = Credentials::parse(request)?;
        let cookie = auth::cookie(request); let bearer = credentials.bearer();
        if let (Some(cookie), Some(bearer)) = (cookie, bearer) { if !auth::equal(cookie.as_bytes(), bearer.as_bytes()) { return Err(auth::unauthenticated("the session cookie and bearer token name different sessions")); } }
        let (session, minted, lease) = if let Some(id) = bearer.or(cookie) {
            if forwarded_peer.is_some() {
                return Err(auth::unauthenticated("Forwarded peer proof is for a new session"));
            }
            let session = match sessions.lookup(&id.to_owned(), true) {
                Lookup::Found(session) => session,
                Lookup::Expired(after) => {
                    let message = if audience_source.is_some() { "audience session expired".into() }
                        else { format!("session expired after {} idle", crate::lease::duration(after)) };
                    return Err(session_error(bearer.is_some(), message));
                }
                Lookup::Unknown => return Err(session_error(bearer.is_some(), "unknown session")),
            };
            let lease = AcquiredSession { service: self.clone(), session: Arc::clone(&session) };
            if session.fresh_replica && !matches!(kind, CallKind::Action | CallKind::V1Capabilities | CallKind::V1Action | CallKind::TxStatus) {
                return Err(Status::permission_denied("The fresh Flight session carries owned read actions"));
            }
            if session.audience_source() != audience_source
                || audience_source.is_some() && !matches!(kind, CallKind::Action | CallKind::V1Capabilities | CallKind::V1Action | CallKind::TxStatus | CallKind::AudienceCleanup)
            {
                return Err(Status::permission_denied("Flight session belongs to another admission class"));
            }
            if let Some(raw) = credentials.raw() {
                if session.caller.mac.is_none_or(|mac| !auth::equal(&mac, &self.0.auth.mac(raw))) { return Err(auth::unauthenticated("these credentials aren't the ones the session was authenticated with")); }
            }
            let verified = self.0.cancellation.run_until_cancelled(session.caller.principal.revalidate(self.0.node.now())).await.ok_or_else(cancelled)?;
            if let Err(error) = verified {
                self.retire_session(&session);
                return Err(auth::auth_error(error));
            }
            admit_workload(&session.caller.principal, session.fresh_replica)?;
            (session, false, lease)
        } else {
            if audience_source.is_some() && (!matches!(kind, CallKind::Handshake) || forwarded_peer.is_none()) {
                return Err(Status::permission_denied("An audience session requires an authenticated placed handshake"));
            }
            let catalog = self.0.node.catalog();
            let placed_name = || requested_database.filter(|name| !name.is_empty())
                .ok_or_else(|| Status::invalid_argument("A placed Flight SQL session requires x-xtdb-database"));
            let (peer, verified_attachment, fresh_replica) = if let Some(proof) = forwarded_peer {
                let verifier = self.0.config.forwarded_peer.as_ref()
                    .ok_or_else(|| auth::unauthenticated("This listener does not accept forwarded peers"))?;
                let proof = proof.to_str().map_err(|_| auth::unauthenticated("Invalid forwarded peer proof"))?;
                let attachment = catalog.database(placed_name()?).map_err(status)?;
                let (peer, fresh_replica) = verifier.verify(proof, &attachment).map_err(auth::auth_error)?;
                if fresh_replica && (!is_handshake || audience_source.is_some()) {
                    return Err(Status::permission_denied("A replica route requires its fresh Flight handshake"));
                }
                (Some(peer.to_string()), Some(attachment), fresh_replica)
            } else {
                (transport.and_then(|transport| transport.peer.remote_addr()).or_else(|| request.remote_addr()).map(|address| address.ip().to_string()), None, false)
            };
            let caller = self.0.cancellation.run_until_cancelled(self.0.auth.authenticate(&credentials, peer.as_deref())).await.ok_or_else(cancelled)??;
            if self.follower_only() && (!caller.verified_credential || caller.principal.label_scope().is_some()) {
                return Err(auth::unauthenticated("The assigned follower requires verified unscoped credentials"));
            }
            admit_workload(&caller.principal, fresh_replica)?;
            if let Some(scope) = caller.principal.label_scope() {
                if !kind.allows_label_scope() || audience.is_some() || fresh_replica {
                    return Err(Status::permission_denied("The credential requires the scoped RTDB v1 route"));
                }
                let database = requested_database.filter(|name| !name.is_empty())
                    .ok_or_else(|| Status::invalid_argument("A scoped credential requires x-xtdb-database"))?;
                scope.admit_database_name(database).map_err(status)?;
            }
            let placement = if let Some(attachment) = follower_attachment {
                caller.principal.authorize_database(attachment.name(), DatabaseAccess::Read).map_err(status)?;
                Some(attachment)
            } else if catalog.is_placed() {
                let name = placed_name()?;
                if caller.principal.label_scope().is_none() {
                    caller.principal.authorize_database(name, DatabaseAccess::Read).map_err(status)?;
                }
                match verified_attachment { Some(attachment) => Some(attachment), None => Some(catalog.database(name).map_err(status)?) }
            } else { None };
            if audience_source.is_some() && (placement.is_none() || caller.mac.is_none() || self.0.config.hosted_controls.as_ref()
                .is_none_or(|controls| caller.principal.user_id() != Some(controls.owner.as_str())))
            {
                return Err(Status::permission_denied("An audience session requires the installed hosted principal"));
            }
            if let Some(source) = audience_source {
                if placement.as_ref().is_none_or(|database| !database.serves_audience_storage(source)) {
                    return Err(Status::failed_precondition("The attachment has no eligible audience storage for this source"));
                }
            }
            let capacity = 1024 + placement.as_ref().map_or(0, |database| database.name().len());
            let memory = audience.map_or(self.memory(), |audience| &audience.memory);
            let mint = |audience_slot| -> Result<Arc<Session>, Status> {
                let session = Arc::new(Session::new(Uuid::new_v4().to_string(), Arc::clone(&caller),
                    placement.as_ref(), fresh_replica, audience_slot, memory.reserve(capacity).map_err(status)?));
                sessions.insert(session.id.clone(), Arc::clone(&session), self.0.config.session_idle_timeout,
                    self.0.config.session_unclaimed_timeout, true, session.id.len()).map_err(status)?;
                Ok(session)
            };
            let session = if let Some(((audience, source), requested)) = audience.zip(audience_source).zip(seat) {
                let slot = &audience.seats[requested.index()];
                let occupied = || Status::resource_exhausted("The audience reader seat still owns a session");
                let displaced = || Status::unavailable("The audience Flight writer attachment changed during handshake");
                let authenticated = placement.as_ref().ok_or_else(displaced)?;
                self.0.node.confirm_audience_route(authenticated, source).await.map_err(status)?;
                let mut original = slot.original.try_lock().map_err(|_| occupied())?;
                let current = catalog.database(authenticated.name()).map_err(|_| displaced())?;
                let candidate_is_current = Arc::ptr_eq(authenticated, &current);
                let existing = match original.upgrade() {
                    Some(session) => {
                        let attachment = if session.placed_database() != Some(current.name()) {
                            SeatAttachment::ForeignDeployment
                        } else if session.holds_attachment(&current) {
                            SeatAttachment::Current { unexpired: session.audience_remaining_micros().is_some() }
                        } else {
                            SeatAttachment::Displaced
                        };
                        match occupied_seat_action(candidate_is_current, attachment) {
                            OccupiedSeatAction::RejectStaleCandidate => return Err(displaced()),
                            OccupiedSeatAction::RejectForeignDeployment => return Err(occupied()),
                            OccupiedSeatAction::CheckRetry => Some(session),
                            OccupiedSeatAction::Retire => {
                                self.retire_session(&session);
                                None
                            }
                        }
                    }
                    None if candidate_is_current => None,
                    None => return Err(displaced()),
                };
                let seat_state = if let Some(session) = existing {
                    let same_credential = session.caller.mac.as_ref().zip(caller.mac.as_ref())
                        .is_some_and(|(left, right)| auth::equal(left, right));
                    SeatState::Live {
                        same_credential,
                        same_attempt: session.audience_seat() == Some(requested),
                        session,
                    }
                } else {
                    match Arc::clone(&slot.permit).try_acquire_owned() {
                        Ok(permit) => SeatState::Vacant(permit),
                        Err(_) => SeatState::Draining,
                    }
                };
                match admit_audience_seat(seat_state) {
                    SeatDecision::Resume(session) => match sessions.lookup(&session.id, true) {
                        Lookup::Found(registered) => registered,
                        _ => return Err(occupied()),
                    },
                    SeatDecision::Mint(permit) => {
                        let expires_at = tokio::time::Instant::now() + Duration::from_secs(180);
                        let claim = self.reserve_audience_seat(Arc::downgrade(authenticated), requested.index(), permit).await?;
                        if authenticated.requires_anchored_catalog() {
                            self.0.node.confirm_audience_route(authenticated, source).await.map_err(status)?;
                        }
                        if tokio::time::Instant::now() >= expires_at {
                            return Err(Status::deadline_exceeded("The audience session ceiling elapsed during handshake"));
                        }
                        let session = mint(Some(AudienceSession { source, seat: requested,
                            cache: Arc::clone(&audience.cache), expires_at, _claim: claim,
                            projection: std::sync::OnceLock::new(),
                            recovery_work: rtdb_storage::audience_work::TurnAllowance::new(
                                crate::config::AUDIENCE_RECOVERY_LIMITS, crate::config::AUDIENCE_RECOVERY_ACTIONS,
                            ),
                        }))?;
                        *original = Arc::downgrade(&session);
                        session
                    }
                    SeatDecision::Refuse => return Err(occupied()),
                }
            } else {
                mint(None)?
            };
            let lease = AcquiredSession { service: self.clone(), session: Arc::clone(&session) };
            (session, true, lease)
        };
        if session.caller.principal.label_scope().is_some() && !kind.allows_label_scope() {
            return Err(Status::permission_denied("The credential requires the scoped RTDB v1 route"));
        }
        let requested_database = requested_database
            .or_else(|| session.caller.principal.workload_grant().map(|grant| grant.database()));
        let (database, attachment) = if matches!(kind, CallKind::AudienceCleanup) {
            let original = session.placed_database().ok_or_else(unsupported)?;
            if requested_database.is_some_and(|requested| requested != original) {
                return Err(Status::permission_denied("The audience cleanup belongs to another deployment"));
            }
            // Close is scoped to the authenticated original carrier. It
            // opens no native connection and cannot prolong writer residency.
            (original.to_owned(), None)
        } else {
            match session.resolve_database(requested_database, &self.0.node) {
                Ok(resolved) => resolved,
                Err(error) => {
                    if minted && audience_source.is_some() { self.retire_session(&session); }
                    return Err(error);
                }
            }
        };
        if let Some(scope) = session.caller.principal.label_scope() {
            scope.admit_database_name(&database).map_err(status)?;
        }
        let active_call = attachment.as_deref().and_then(|attachment| self.0.node.hold_active_call(attachment)).map(Arc::from);
        let call = Call { service: self.clone(), kind, session, minted, database, cancellation: self.0.cancellation.child_token(),
            admission, _lease: Some(lease), _active_call: active_call, _reservation: reservation,
            egress: transport.map(|transport| transport.egress.clone()),
            routed_delivery: request.metadata().get(crate::GATEWAY_ANSWER_DEADLINE_HEADER).is_some_and(|header| header == "forward") };
        self.publish(); Ok(call)
    }
    async fn reserve_audience_seat(&self, database: Weak<rtdb_node::Database>, index: usize,
        permit: OwnedSemaphorePermit) -> Result<DropGuard, Status> {
        let (ready, acquired) = oneshot::channel();
        let release = CancellationToken::new();
        let guard = release.clone().drop_guard();
        let node = self.0.node.clone();
        tokio::spawn(async move {
            // An interrupted handshake still owns the permit until its directory write is resolved.
            let _permit = permit;
            node.hold_audience_seat(database, index, ready, release).await;
        });
        acquired.await.map_err(|_| cancelled())?.map_err(status)?;
        Ok(guard)
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

pub(crate) async fn action_result_stream(
    call: Call, values: Vec<Result<flight::Result, Status>>,
    plaintext: Arc<rtdb_storage::sealing::PlaintextView>,
) -> Result<Response<Output<flight::Result>>, Status> {
    if !values.iter().any(Result::is_ok) { return result_stream(call, values); }
    handoff_action_results(call, values, plaintext).await
}

pub(crate) async fn handoff_action_results<T, I>(
    call: Call, values: I, plaintext: Arc<rtdb_storage::sealing::PlaintextView>,
) -> Result<Response<Output<T>>, Status>
where
    T: Send + 'static,
    I: IntoIterator<Item = Result<T, Status>>,
    I::IntoIter: Send + 'static,
{
    let first = call.cancellation.run_until_cancelled(plaintext.grant()).await.ok_or_else(cancelled)?.map_err(status)?;
    let (headers, cutoff) = call.delivery_headers(&first, false)?;
    let stream: Output<T> = Box::pin(futures::stream::unfold(
        (call, values.into_iter(), plaintext, Some(first), false),
        move |(call, mut values, plaintext, mut first, stopped)| async move {
            if stopped { return None; }
            let value = values.next()?;
            let result = call.cancellation.run_until_cancelled(async {
                let value = value?;
                let grant = match first.take() { Some(grant) => grant, None => plaintext.grant().await.map_err(status)? };
                call.handoff_action(grant, cutoff)?;
                Ok(value)
            }).await.unwrap_or_else(|| Err(cancelled()));
            let stop = result.is_err();
            Some((result, (call, values, plaintext, first, stop)))
        },
    ));
    let mut response = Response::new(stream);
    *response.metadata_mut() = headers;
    Ok(response)
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
    async fn do_exchange(&self, request: Request<Streaming<flight::FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        use crate::owned_actions::{DECIDE_HOSTED_STREAM_V1, HOSTED_UPLOAD_HEADER};
        let call = self.call(&request, 0).await?;
        let headers = request.metadata().get_all(HOSTED_UPLOAD_HEADER);
        let mut versions = headers.iter();
        if !versions.next().is_some_and(|version| version == DECIDE_HOSTED_STREAM_V1)
            || versions.next().is_some() || !self.supports_hosted_upload(&call)
        {
            return Err(call.error(unsupported()));
        }
        self.hosted_upload(call, request.into_inner()).await
    }
    async fn list_actions(&self, request: Request<flight::Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        let call = self.call(&request, 0).await?;
        let actions = ["BeginSavepoint", "BeginTransaction", "CreatePreparedStatement", "ClosePreparedStatement", "CreatePreparedSubstraitPlan", "CancelQuery", "EndSavepoint", "EndTransaction"];
        let catalog_actions = [crate::catalog_actions::PUBLISH_CATALOG, crate::catalog_actions::WATCH_RELATION, crate::catalog_actions::SCAN_CATALOG];
        let catalog_enabled = self.0.config.owned_native_actions
            && !call.session.fresh_replica && call.session.audience_source().is_none();
        let hosted_upload = self.supports_hosted_upload(&call)
            .then_some(crate::owned_actions::DECIDE_HOSTED_STREAM_V1);
        let hosted_worlds = self.0.config.owned_world_host.is_some() && self.supports_hosted_upload(&call);
        let hosted_world_actions = [
            crate::owned_actions::DECIDE_HELD_WORLDS_V3,
            crate::owned_family::RESOLVE_FAMILY_MEMBER_V1,
            crate::owned_family::FAMILY_RESPONSE_RELEASE_V2,
            crate::owned_actions::HELD_WORLD_SHARED_COLUMN_CENSUS_V1,
            crate::owned_actions::HELD_WORLD_POINT_AT_V2,
        ];
        let root_rank = self.0.config.owned_native_actions && call.session.audience_source().is_none();
        #[cfg(feature = "multi")]
        let actions = actions.into_iter().chain(crate::admin::ACTIONS.into_iter().filter(|name| self.supports_admin_action(&call, name)))
            .collect::<Vec<_>>();
        result_stream(call, actions.into_iter().chain(crate::v1::ACTIONS.into_iter()
            .filter(|name| *name != crate::v1::MANAGED_WORKLOAD))
            .chain(hosted_world_actions.into_iter().filter(|_| hosted_worlds))
            .chain(crate::owned_actions::ROOT_RANK_ACTIONS.into_iter().filter(|_| root_rank))
            .chain(crate::owned_actions::WORLD_RANK_ACTIONS.into_iter().filter(|_| hosted_worlds))
            .chain(catalog_actions.into_iter().filter(|_| catalog_enabled))
            .chain(hosted_upload)
            .map(|name| Ok(flight::ActionType { r#type: name.into(), description: name.into() })).collect())
    }
    async fn get_flight_info(&self, request: Request<flight::FlightDescriptor>) -> Result<Response<flight::FlightInfo>, Status> {
        let call = self.call(&request, request.get_ref().encoded_len()).await?;
        let descriptor = request.into_inner(); let cmd = command(&descriptor.cmd).map_err(|error| call.error(error))?;
        let work = async {
            let (schema, ticket) = match cmd {
                Command::CommandStatementQuery(command) => { let connection = self.tx_or_session(&call, command.transaction_id.as_ref()).await?; self.query_info(&call, connection, &command.query, None, None).await? }
                Command::CommandPreparedStatementQuery(command) => { let prepared = self.prepared(&call, &command.prepared_statement_handle)?; let params = prepared.params.lock().expect("parameters lock").clone(); let connection = self.scoped(&call, &prepared.scope).await?; self.query_info(&call, connection, &prepared.sql, prepared.plan.as_ref(), params).await? }
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
        let ticket = &request.get_ref().ticket;
        if let Some(command) = crate::v1::ticket(ticket) {
            let kind = match &command {
                crate::v1::Ticket::Query(_) => CallKind::V1Query,
                crate::v1::Ticket::Changes(_) => CallKind::V1Changes,
            };
            let call = self.call_with_kind(&request, request.get_ref().encoded_len(), kind)
                .await.map_err(crate::v1::error::map_status)?;
            return self.v1_get(call, command, request.metadata()).await;
        }
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
        let name = request.get_ref().r#type.as_str();
        if crate::v1::is_action(name) {
            let kind = match name {
                crate::v1::CAPABILITIES => CallKind::V1Capabilities,
                crate::v1::MANAGED_CREATE => CallKind::ManagedCreate,
                crate::v1::MANAGED_WORKLOAD => CallKind::ManagedWorkload,
                crate::v1::TX_STATUS => CallKind::TxStatus,
                _ => CallKind::V1Action,
            };
            let call = self.call_with_kind(&request, request.get_ref().encoded_len(), kind)
                .await.map_err(crate::v1::error::map_status)?;
            return self.v1_action(call, request.into_inner()).await;
        }
        let kind = CallKind::action(self.0.audience.is_some(), name);
        let call = self.call_with_kind(&request, request.get_ref().encoded_len(), kind).await?;
        let ipc_stream = request.metadata().get(crate::actions::OWNED_IPC_HEADER)
            .is_some_and(|encoding| encoding == "stream-v1");
        self.action(call, request.into_inner(), ipc_stream).await
    }
}

fn follower_refusal() -> Status {
    Status::permission_denied("The assigned follower serves only native Query and Capabilities")
}

pub(crate) fn query_ticket(ticket: Bytes, memory: &Arc<MemoryBudget>) -> Result<Bytes, Status> {
    let charge = memory.reserve(wire::add(wire::mul(ticket.len(), 3)?, 1024)?).map_err(status)?;
    Ok(wire::retain(TicketStatementQuery { statement_handle: ticket }.as_any().encode_to_vec(), charge))
}

#[cfg(test)]
mod tests {
    use super::{CLOSE_SESSION, CallKind};
    use crate::owned_actions::CLOSE_OWNED_READ_V1;

    #[test]
    fn label_scope_is_only_admitted_to_v1_and_handshake() {
        for kind in [CallKind::Handshake, CallKind::V1Query, CallKind::V1Changes, CallKind::V1Capabilities, CallKind::V1Action, CallKind::TxStatus] {
            assert!(kind.allows_label_scope());
        }
        for kind in [CallKind::Ordinary, CallKind::Action, CallKind::AudienceCleanup, CallKind::ManagedCreate, CallKind::ManagedWorkload] {
            assert!(!kind.allows_label_scope());
        }
    }

    #[test]
    fn follower_admission_has_a_positive_query_allowlist() {
        for kind in [CallKind::Handshake, CallKind::V1Query, CallKind::V1Capabilities] {
            assert!(kind.serves_assigned_follower());
        }
        for kind in [CallKind::Ordinary, CallKind::Action, CallKind::AudienceCleanup, CallKind::V1Changes, CallKind::V1Action, CallKind::ManagedCreate, CallKind::ManagedWorkload, CallKind::TxStatus] {
            assert!(!kind.serves_assigned_follower());
        }
    }

    #[test]
    fn workload_scope_has_a_positive_managed_action_allowlist() {
        for kind in [CallKind::Handshake, CallKind::ManagedCreate, CallKind::ManagedWorkload, CallKind::TxStatus] {
            assert!(kind.allows_workload_grant());
        }
        for kind in [CallKind::Ordinary, CallKind::Action, CallKind::AudienceCleanup, CallKind::V1Query, CallKind::V1Changes, CallKind::V1Capabilities, CallKind::V1Action] {
            assert!(!kind.allows_workload_grant());
        }
    }

    #[test]
    fn only_audience_closes_can_dispatch_without_a_current_attachment() {
        for action in [CLOSE_SESSION, CLOSE_OWNED_READ_V1] {
            assert!(matches!(CallKind::action(true, action), CallKind::AudienceCleanup));
            assert!(matches!(CallKind::action(false, action), CallKind::Action));
        }
        for action in [
            crate::owned_actions::PREPARE_APPLIED_READ_V3,
            crate::owned_actions::OPEN_APPLIED_OWNED_READ_V5,
            crate::owned_actions::CONFIRM_APPLIED_HOSTED_V6,
            "closesession",
        ] {
            assert!(matches!(CallKind::action(true, action), CallKind::Action));
        }
    }
}
