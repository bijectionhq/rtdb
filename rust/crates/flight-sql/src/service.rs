use std::{pin::Pin, sync::Arc};
use arrow_flight::{self as flight, flight_service_server::FlightService, sql::{Any, Command, ProstMessageExt, TicketStatementQuery}};
use bytes::Bytes;
use futures::Stream;
use prost::Message;
use rtdb_auth::Authenticator;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_node::{Clock, DatabaseAccess, Node, SessionIdentity};
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;
use rtdb_storage::sealing::{DeliveryDeadline, PlaintextDelivery};
use crate::{FORWARDED_PEER_HEADER, FlightConfig, auth::{self, Authentication, Credentials}, error::{arrow, cancelled, status, unsupported}, lease::{Leased, Lookup, Registry}, server::{Binding, ServerStatus}, state::{ConnectionResource, Prepared, PreparedScope, Session, StoreReadLease, Transaction}, wire};
use crate::store_release::{self, StoreRelease};

pub(crate) const CLOSE_SESSION: &str = "CloseSession";
const CLASSIC_RETAINED_PROOF: &str = "x-rtdb-classic-retained-proof";
const MAX_CLASSIC_RETAINED_PROOF: usize = 4608;

fn routed_answer_deadline(grant: Option<DeliveryDeadline>, streaming: bool) -> Option<DeliveryDeadline> {
    if !streaming { return grant; }
    // Later batches may load encrypted objects whose delivery grants last only one answer window.
    let window = DeliveryDeadline::answer_window();
    Some(grant.map_or(window, |grant| grant.earlier(window)))
}

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
    pub owned_reads: Registry<Bytes, crate::owned_actions::OwnedReadResource>,
    pub owned_world_reads: Registry<Bytes, crate::owned_actions::OwnedWorldReadResource>,
    pub owned_read_slots: Arc<Semaphore>,
    pub exact_reads: Registry<Uuid, crate::v1::exact_read::ExactRead>,
    auth: Authentication,
    permits: Arc<Semaphore>,
    pub status: watch::Sender<ServerStatus>,
}

#[derive(Clone, Copy)]
enum CallKind { Ordinary, Handshake, Action, SessionClose, OwnedAction, OpenFresh, V1Query, V1Changes, V1Capabilities, V1ExactRead, V1Action, V1CustodyRelease, V1OriginalFamily, V1OriginalFamilyReadback, V1OriginalPrivateReplay, V1Appointment, ManagedCreate, ManagedWorkload, TxStatus, Store, StoreRead, StoreWriter,
    #[cfg(feature = "multi")]
    StoreTimedAppend,
}

impl CallKind {
    fn requires_store_release(self) -> bool {
        match self {
            Self::Store | Self::StoreRead | Self::StoreWriter => true,
            #[cfg(feature = "multi")]
            Self::StoreTimedAppend => true,
            _ => false,
        }
    }

    fn allows_label_scope(self) -> bool {
        matches!(self, Self::Handshake | Self::V1Query | Self::V1Changes | Self::V1Capabilities | Self::V1Action | Self::SessionClose | Self::Store | Self::StoreRead | Self::V1Appointment | Self::TxStatus)
    }

    fn serves_assigned_follower(self) -> bool {
        matches!(self, Self::Handshake | Self::SessionClose | Self::V1Query | Self::V1Capabilities | Self::V1ExactRead)
    }

    fn allows_workload_root_read(self) -> bool {
        matches!(self, Self::Handshake | Self::V1Query | Self::V1Changes | Self::V1Capabilities)
    }

    fn allows_original_custody_control(self) -> bool {
        matches!(self, Self::Handshake | Self::V1CustodyRelease)
    }

    fn allows_original_family(self) -> bool {
        matches!(self, Self::Handshake | Self::V1Capabilities | Self::V1Query | Self::V1OriginalFamily)
    }

    fn allows_original_family_readback(self) -> bool {
        matches!(self, Self::Handshake | Self::V1Capabilities | Self::V1OriginalFamilyReadback)
    }

    fn allows_workload_grant(self) -> bool {
        matches!(self, Self::Handshake | Self::ManagedCreate | Self::ManagedWorkload | Self::TxStatus)
    }

    /// A store grant (a placement fence or a provisioning grant) reaches only
    /// the store contract, whose actions keep it inside its scope.
    fn allows_store_grant(self) -> bool {
        matches!(self, Self::Handshake | Self::SessionClose | Self::Store | Self::StoreRead)
    }

    fn allows_funded_appointment(self) -> bool {
        matches!(self, Self::Handshake | Self::V1Capabilities | Self::V1Appointment | Self::TxStatus)
    }

    fn v1_admission_status(self, status: Status) -> Status {
        if matches!(self, Self::StoreRead) && status.code() == tonic::Code::Unauthenticated {
            return status;
        }
        crate::v1::error::map_status(status)
    }

    fn action(name: &str) -> Self {
        if name == CLOSE_SESSION {
            Self::SessionClose
        } else if name == crate::owned_actions::OPEN_FRESH_OWNED_READ_V2 {
            Self::OpenFresh
        } else if name.starts_with("rtdb.owned.") {
            Self::OwnedAction
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
    store_read: Option<Arc<StoreReadLease>>,
    _reservation: Arc<MemoryReservation>,
    #[cfg(feature = "multi")]
    store_reply: std::sync::Mutex<Option<crate::v1::store::StoreReplyGuard>>,
    egress: Option<Arc<crate::server::FlightEgress>>,
    routed_delivery: bool,
    pub(crate) classic_flight_excluded: bool,
    pub(crate) classic_retained: Option<crate::ClassicRetainedAdmission>,
}
struct StoreReadBuffer {
    bytes: Bytes,
    _reader: Arc<StoreReadLease>,
}

impl AsRef<[u8]> for StoreReadBuffer {
    fn as_ref(&self) -> &[u8] { &self.bytes }
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
    #[cfg(feature = "multi")]
    pub(crate) fn retain_store_reply(&self, guard: crate::v1::store::StoreReplyGuard) -> Result<(), Status> {
        let mut slot = self.store_reply.lock().map_err(|_| Status::internal("Store reply ownership is unavailable"))?;
        if slot.is_some() {
            return Err(Status::internal("An action owns one Store reply"));
        }
        *slot = Some(guard);
        Ok(())
    }

    #[cfg(feature = "multi")]
    pub(crate) fn take_store_reply(&mut self) -> Result<Option<crate::v1::store::StoreReplyGuard>, Status> {
        Ok(self.store_reply.get_mut().map_err(|_| Status::internal("Store reply ownership is unavailable"))?.take())
    }

    pub(crate) fn require_imported_workload(&self, database: &rtdb_node::Database) -> Result<(), Status> {
        let Some(driver) = database.serving_driver() else { return Ok(()); };
        let Some(fence) = driver.classic_workload_fence() else { return Ok(()); };
        let caller = &self.session.caller;
        let appointment = caller.verified_credential && self.kind.allows_funded_appointment()
            && caller.principal.workload_grant()
                .and_then(|grant| grant.funded_appointment(&self.database, driver.incarnation(), self.service.0.node.now()).ok())
                .is_some_and(|appointment| appointment.fence().at_or_after_import(fence));
        if appointment { return Ok(()); }
        let allowed = caller.verified_credential && caller.principal.workload_grant()
            .and_then(|grant| grant.assignment(&self.database, driver.incarnation(), self.service.0.node.now()).ok())
            .is_some_and(|(authority, _, _)| authority.permits_imported_workload(fence)
                || matches!(self.kind, CallKind::Handshake | CallKind::TxStatus)
                    && matches!(authority, rtdb_tx::transact::workload::Authority::ReceiptRecovery { .. })
                || matches!(self.kind, CallKind::Handshake | CallKind::ManagedWorkload)
                    && matches!(authority, rtdb_tx::transact::workload::Authority::AttemptSettlement { .. }));
        let allowed = allowed || (caller.verified_credential && self.kind.allows_original_family()
            && self.service.0.config.custody_providers.get(&self.database).is_some_and(|route| {
                caller.principal.workload_grant()
                    .and_then(|grant| grant.original_family(&self.database, route.authority(), self.service.0.node.now()).ok())
                    .is_some_and(|(scope, _, _)| rtdb_types::rtdb_v1::Basis::from_bytes(&scope.created)
                        .is_ok_and(|created| scope.fence.covers_imported_family_line(fence, &scope.writer, created)))
            }));
        let allowed = allowed || (caller.verified_credential && self.kind.allows_original_family_readback()
            && self.service.0.config.custody_providers.get(&self.database).is_some_and(|route| {
                caller.principal.workload_grant()
                    .and_then(|grant| grant.original_family_readback(&self.database, route.authority(), self.service.0.node.now()).ok())
                    .is_some_and(|(scope, _, _)| rtdb_types::rtdb_v1::Basis::from_bytes(&scope.line.created)
                        .is_ok_and(|created| scope.line.fence.covers_imported_family_line(fence, &scope.line.writer, created)))
            }));
        let allowed = allowed || (caller.verified_credential
            && matches!(self.kind, CallKind::Handshake | CallKind::V1OriginalPrivateReplay)
            && self.service.0.config.custody_providers.get(&self.database).is_some_and(|route| {
                route.private_replay_configured()
                    && route.owner().is_some() && caller.principal.user_id() == route.owner()
                    && caller.principal.workload_grant().is_some_and(|grant| grant.admit_original_custody(
                        &self.database, route.authority(), rtdb_auth::OriginalCustodyOperation::PrivateReplay,
                        self.service.0.node.now(),
                    ).is_ok())
            }));
        if allowed { Ok(()) } else { Err(Status::permission_denied("This deployment requires its exact imported workload assignment")) }
    }

    pub(crate) fn handoff(&self, grant: PlaintextDelivery, relay: Option<DeliveryDeadline>) -> Result<(), Status> {
        let relay = self.classic_deadline(relay);
        if let Some(egress) = &self.egress { return egress.admit(grant, relay).map_err(status); }
        grant.check().map_err(status)?;
        if grant.is_bounded() || relay.is_some() { return Err(Status::unavailable("Flight answer transport has no delivery fence")); }
        grant.observe_answer();
        Ok(())
    }

    pub(crate) fn delivery_headers(
        &self, grant: &PlaintextDelivery, streaming: bool,
    ) -> Result<(tonic::metadata::MetadataMap, Option<DeliveryDeadline>), Status> {
        let mut headers = self.response(())?.metadata().clone();
        let cutoff = if self.routed_delivery {
            self.classic_deadline(routed_answer_deadline(grant.transport_deadline(), streaming))
        } else { None };
        if let Some(cutoff) = cutoff {
            let wire = cutoff.wire_nanos().ok_or_else(|| Status::unavailable("Flight answer deadline cannot be routed"))?;
            headers.insert(crate::GATEWAY_ANSWER_DEADLINE_HEADER, wire.to_string().parse().map_err(arrow)?);
        }
        Ok((headers, cutoff))
    }

    fn classic_deadline(&self, cutoff: Option<DeliveryDeadline>) -> Option<DeliveryDeadline> {
        match self.classic_retained {
            Some(retained) => Some(cutoff.map_or(retained.deadline, |cutoff| cutoff.earlier(retained.deadline))),
            None => cutoff,
        }
    }

    pub fn response<T>(&self, value: T) -> Result<Response<T>, Status> {
        let mut response = Response::new(value);
        if self.session.closed() {
            if !self.minted { response.metadata_mut().insert("set-cookie", format!("{}={}; Max-Age=0", auth::COOKIE, self.session.id).parse().map_err(arrow)?); }
        } else if self.minted {
            response.metadata_mut().insert("set-cookie", format!("{}={}", auth::COOKIE, self.session.id).parse().map_err(arrow)?);
            if self.session.caller.mac.is_some() { response.metadata_mut().insert("authorization", format!("Bearer {}", self.session.id).parse().map_err(arrow)?); }
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
    pub(crate) fn retain_store_read_body(&self, bytes: Bytes) -> Bytes {
        match &self.store_read {
            Some(reader) => Bytes::from_owner(StoreReadBuffer { bytes, _reader: reader.clone() }),
            None => bytes,
        }
    }

    pub async fn close_session(&self) {
        self.service.fence_session(&self.session);
        // Keep the closed session's identity through the normal idle bound.
        // A retry can confirm its drain without reopening admission.
        if self.minted {
            let _ = self.service.0.sessions.take(&self.session.id);
        }
        self.session.wait_store_reads().await;
    }
    pub fn error(&self, mut error: Status) -> Status {
        if let Ok(response) = self.response(()) { for key in ["set-cookie", "authorization"] { if let Some(value) = response.metadata().get(key) { error.metadata_mut().insert(key, value.clone()); } } }
        error
    }
}
impl Service {
    pub fn new(node: Node, auth: Arc<Authenticator>, config: FlightConfig, clock: Arc<dyn Clock>, cancellation: CancellationToken, status: watch::Sender<ServerStatus>, binding: Binding) -> rtdb_types::Result<Self> {
        let memory = Arc::clone(node.memory());
        let exact_reads = Registry::new(clock.clone(), memory.clone());
        Ok(Self(Arc::new(Inner { sessions: Registry::new(clock.clone(), memory.clone()), transactions: Registry::new(clock.clone(), memory.clone()), prepared: Registry::new(clock.clone(), memory.clone()), owned_reads: Registry::new(clock.clone(), memory.clone()), owned_world_reads: Registry::new(clock, memory), owned_read_slots: Arc::new(Semaphore::new(64)), exact_reads, v1: Default::default(), auth: Authentication::new(auth), permits: Arc::new(Semaphore::new(config.maximum_concurrent_calls)), node, config, binding, cancellation, status })))
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
    /// The largest request message the transport decodes.
    pub(crate) fn request_bytes(&self) -> usize {
        self.0.config.maximum_message_bytes
    }
        pub(crate) fn ticket_binding<'a>(&self, call: &'a Call) -> crate::ticket::Binding<'a> {
        crate::ticket::Binding { mac: self.0.auth.ticket_mac(), session: &call.session.id, owner: call.session.caller.owner }
    }
    fn fence_session(&self, session: &Session) {
        session.close();
        self.0.owned_reads.close_where(|read| read.session == session.id);
        self.0.owned_world_reads.close_where(|read| read.session == session.id);
        self.0.exact_reads.close_where(|read| read.session == session.id);
    }
    fn retire_session(&self, session: &Session) {
        self.fence_session(session);
        self.0.sessions.retire(&session.id);
    }
    pub fn sweep(&self) {
        self.0.sessions.sweep(); self.0.transactions.sweep(); self.0.prepared.sweep(); self.0.owned_reads.sweep(); self.0.owned_world_reads.sweep();
        self.0.exact_reads.sweep();
        self.publish();
    }
    pub fn close(&self) {
        self.0.exact_reads.close();
        self.0.owned_world_reads.close(); self.0.owned_reads.close(); self.0.prepared.close(); self.0.transactions.close(); self.0.sessions.close();
        self.publish();
    }
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
    pub(crate) async fn classic_flight_excluded(&self, attachment: Option<&rtdb_node::Database>) -> Result<bool, Status> {
        match self.0.config.classic_flight_exclusion.as_ref().zip(attachment) {
            Some((policy, attachment)) => policy.excludes_unclassified(attachment).await.map_err(status),
            None => Ok(false),
        }
    }
    async fn call_with_kind<T>(&self, request: &Request<T>, body_bytes: usize, kind: CallKind) -> Result<Call, Status> {
        if self.0.cancellation.is_cancelled() { return Err(cancelled()); }
        let retained_proof = request.metadata().contains_key(CLASSIC_RETAINED_PROOF);
        if retained_proof && !matches!(kind, CallKind::OwnedAction | CallKind::OpenFresh) {
            return Err(classic_retained_refusal());
        }
        let store_read = matches!(kind, CallKind::StoreRead);
        if self.follower_only() && (!(kind.serves_assigned_follower() || store_read)
            || request.metadata().contains_key(FORWARDED_PEER_HEADER)
            || request.metadata().contains_key(crate::GATEWAY_ANSWER_DEADLINE_HEADER))
        {
            return Err(follower_refusal());
        }
        let transport = request.extensions().get::<crate::server::FlightConnectInfo>();
        let is_handshake = matches!(kind, CallKind::Handshake);
        #[cfg(feature = "multi")]
        let timed_store = crate::v1::store::timed_append::requested_incarnation(request.metadata())?;
        #[cfg(not(feature = "multi"))]
        let timed_store = None;
        if timed_store.is_some() && (!is_handshake || self.follower_only()
            || request.metadata().contains_key(FORWARDED_PEER_HEADER)
            || request.metadata().contains_key(crate::GATEWAY_ANSWER_DEADLINE_HEADER))
        {
            return Err(Status::invalid_argument("Timed Store enrollment requires a direct writer handshake"));
        }
        let store_access_probe = crate::v1::store_access::probe(request.metadata(), is_handshake)?;
        if timed_store.is_some() && store_access_probe.is_some() {
            return Err(Status::invalid_argument("A timed Store handshake cannot probe access"));
        }
        let store_action = kind.requires_store_release();
        let offers_store_release = request.metadata().contains_key(store_release::HEADER);
        let store_handshake = is_handshake && offers_store_release;
        if store_action && offers_store_release {
            return Err(Status::invalid_argument("A store release belongs on the Flight handshake"));
        }
        let sessions = &self.0.sessions;
        let permit = Arc::clone(&self.0.permits).try_acquire_owned().map_err(|_| Status::resource_exhausted("Flight concurrent-call limit reached"))?;
        let admission = Arc::new(Admission::new(self.clone(), permit));
        let header_bytes = request.metadata().iter().try_fold(4096usize, |bytes, entry| {
            let size = match entry { tonic::metadata::KeyAndValueRef::Ascii(key, value) => wire::add(key.as_str().len(), value.as_encoded_bytes().len())?, tonic::metadata::KeyAndValueRef::Binary(key, value) => wire::add(key.as_str().len(), value.as_encoded_bytes().len())? };
            wire::add(bytes, wire::mul(size, 16)?)
        })?;
        let reservation = self.memory().reserve(wire::add(header_bytes, wire::mul(body_bytes, 32)?)?).map_err(status)?;
        let requested_database = request.metadata().get("x-xtdb-database").map(|value| value.to_str().map_err(arrow)).transpose()?;
        let follower_attachment: Option<Arc<rtdb_node::Database>> = None;
        #[cfg(feature = "multi")]
        let (requested_database, follower_attachment) = if let Some(follower) = self.follower() {
            (Some(follower.select(request.metadata()).map_err(crate::v1::error::follower)?), Some(follower.current(&self.0.node).map_err(crate::v1::error::follower)?))
        } else { (requested_database, follower_attachment) };
        let forwarded_peer = request.metadata().get(FORWARDED_PEER_HEADER);
        let admit_workload = |principal: &rtdb_auth::Principal, fresh_replica| {
            if let Some(grant) = principal.workload_grant() {
                let allowed = if grant.is_placement_fence() {
                    matches!(kind, CallKind::Handshake | CallKind::StoreWriter)
                } else if grant.is_store_grant() {
                    kind.allows_store_grant()
                } else if grant.is_original_private_replay() {
                    matches!(kind, CallKind::Handshake | CallKind::V1OriginalPrivateReplay)
                } else if grant.is_original_custody_control() {
                    kind.allows_original_custody_control()
                } else if grant.is_original_family() {
                    kind.allows_original_family()
                } else if grant.is_original_family_readback() {
                    kind.allows_original_family_readback()
                } else if grant.is_funded_appointment() {
                    kind.allows_funded_appointment()
                } else if grant.is_custody_reader() {
                    matches!(kind, CallKind::Handshake | CallKind::V1Query | CallKind::V1Capabilities)
                } else if grant.is_root_reader() {
                    kind.allows_workload_root_read()
                } else {
                    kind.allows_workload_grant()
                };
                if !allowed
                    || matches!(kind, CallKind::ManagedCreate) && grant.is_assignment()
                    || matches!(kind, CallKind::ManagedWorkload | CallKind::TxStatus)
                        && !grant.is_assignment() && !grant.is_funded_appointment()
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
        #[cfg(feature = "multi")]
        if matches!(kind, CallKind::StoreTimedAppend) && bearer.is_none() {
            return Err(auth::unauthenticated("Timed Store requires its authenticated writer bearer"));
        }
        if store_access_probe.is_some() && (bearer.or(cookie).is_some() || self.follower_only() || store_handshake) {
            return Err(Status::invalid_argument("A Store access probe requires new credentials on a placed writer route"));
        }
        if retained_proof && bearer.or(cookie).is_none() { return Err(classic_retained_refusal()); }
        if let (Some(cookie), Some(bearer)) = (cookie, bearer) { if !auth::equal(cookie.as_bytes(), bearer.as_bytes()) { return Err(auth::unauthenticated("the session cookie and bearer token name different sessions")); } }
        let (session, minted, lease, mint_exclusion, store_close) = if let Some(id) = bearer.or(cookie) {
            if forwarded_peer.is_some() {
                return Err(auth::unauthenticated("Forwarded peer proof is for a new session"));
            }
            let id = id.to_owned();
            let found = if matches!(kind, CallKind::SessionClose) {
                sessions.lookup_for_close(&id)
            } else {
                sessions.lookup(&id, true)
            };
            let session = match found {
                Lookup::Found(session) => session,
                Lookup::Expired(after) => {
                    let message = format!("session expired after {} idle", crate::lease::duration(after));
                    return Err(session_error(bearer.is_some(), message));
                }
                Lookup::Unknown => return Err(session_error(bearer.is_some(), "unknown session")),
            };
            let lease = AcquiredSession { service: self.clone(), session: Arc::clone(&session) };
            if session.closed() && !matches!(kind, CallKind::SessionClose) {
                return Err(session_error(bearer.is_some(), "session closed"));
            }
            if store_handshake {
                return Err(Status::failed_precondition("A store release handshake opens a new Flight session"));
            }
            if timed_store.is_some() {
                return Err(Status::failed_precondition("A timed Store handshake opens a new Flight session"));
            }
            if session.fresh_replica && !store_read && !matches!(kind, CallKind::Action | CallKind::SessionClose | CallKind::OwnedAction | CallKind::OpenFresh | CallKind::V1Capabilities | CallKind::V1Action | CallKind::V1Appointment | CallKind::TxStatus) {
                return Err(Status::permission_denied("The fresh Flight session carries owned read actions"));
            }
            if let Some(raw) = credentials.raw() {
                if session.caller.mac.is_none_or(|mac| !auth::equal(&mac, &self.0.auth.mac(raw))) { return Err(auth::unauthenticated("these credentials aren't the ones the session was authenticated with")); }
            }
            // An enrolled Store bearer retains only the right to close its
            // own exact session after the original credential loses access.
            let store_close = matches!(kind, CallKind::SessionClose) && bearer.is_some() && session.store_release.is_some();
            if !store_close {
                let verified = self.0.cancellation.run_until_cancelled(session.caller.principal.revalidate(self.0.node.now())).await.ok_or_else(cancelled)?;
                if let Err(error) = verified {
                    self.retire_session(&session);
                    return Err(auth::auth_error(error));
                }
                admit_workload(&session.caller.principal, session.fresh_replica)?;
            }
            if store_action { StoreRelease::require(session.store_release)?; }
            (session, false, lease, None, store_close)
        } else {
            let catalog = self.0.node.catalog();
            let placed_name = || requested_database.filter(|name| !name.is_empty())
                .ok_or_else(|| Status::invalid_argument("A placed Flight SQL session requires x-xtdb-database"));
            let (peer, verified_attachment, fresh_replica) = if let Some(proof) = forwarded_peer {
                let verifier = self.0.config.forwarded_peer.as_ref()
                    .ok_or_else(|| auth::unauthenticated("This listener does not accept forwarded peers"))?;
                let proof = proof.to_str().map_err(|_| auth::unauthenticated("Invalid forwarded peer proof"))?;
                let attachment = catalog.database(placed_name()?).map_err(status)?;
                let (peer, fresh_replica) = verifier.verify(proof, &attachment).map_err(auth::auth_error)?;
                if fresh_replica && !is_handshake {
                    return Err(Status::permission_denied("A replica route requires its fresh Flight handshake"));
                }
                (Some(peer.to_string()), Some(attachment), fresh_replica)
            } else {
                (transport.and_then(|transport| transport.peer.remote_addr()).or_else(|| request.remote_addr()).map(|address| address.ip().to_string()), None, false)
            };
            let caller = self.0.cancellation.run_until_cancelled(self.0.auth.authenticate(&credentials, peer.as_deref())).await.ok_or_else(cancelled)??;
            if let Some(expected) = store_access_probe {
                if !catalog.is_placed() || fresh_replica {
                    return Err(crate::v1::error::unsupported("store.access-probe"));
                }
                let attachment = match verified_attachment {
                    Some(attachment) => attachment,
                    None => catalog.database(placed_name()?).map_err(status)?,
                };
                return Err(crate::v1::store_access::store_denial(&caller, &attachment, expected));
            }
            if store_action { StoreRelease::require(None)?; }
            if self.follower_only() && (!caller.verified_credential || caller.principal.label_scope().is_some()) {
                return Err(auth::unauthenticated("The assigned follower requires verified unscoped credentials"));
            }
            admit_workload(&caller.principal, fresh_replica)?;
            if let Some(scope) = caller.principal.label_scope() {
                if !kind.allows_label_scope() || fresh_replica {
                    return Err(Status::permission_denied("The credential requires the scoped RTDB v1 route"));
                }
                let database = requested_database.filter(|name| !name.is_empty())
                    .ok_or_else(|| Status::invalid_argument("A scoped credential requires x-xtdb-database"))?;
                scope.admit_database_name(database).map_err(status)?;
            }
            let placement = if let Some(attachment) = follower_attachment {
                caller.principal.authorize_database(attachment.name(), DatabaseAccess::Read).map_err(status)?;
                Some(attachment)
            } else if let Some(grant) = caller.principal.workload_grant()
                .filter(|grant| grant.is_original_family_readback() || grant.is_original_private_replay() || grant.is_placement_fence())
            {
                Some(catalog.database(grant.database()).map_err(status)?)
            } else if catalog.is_placed() {
                let name = placed_name()?;
                if caller.principal.label_scope().is_none() {
                    caller.principal.authorize_database(name, DatabaseAccess::Read).map_err(status)?;
                }
                match verified_attachment { Some(attachment) => Some(attachment), None => Some(catalog.database(name).map_err(status)?) }
            } else { None };
            #[cfg(feature = "multi")]
            let placement = match timed_store {
                Some(incarnation) => Some(self.admit_timed_store_handshake(&caller, placed_name()?, incarnation).await?),
                None => placement,
            };
            let excluded = if fresh_replica || matches!(kind, CallKind::OwnedAction | CallKind::OpenFresh) {
                self.classic_flight_excluded(placement.as_deref()).await?
            } else { false };
            if excluded && (fresh_replica || matches!(kind, CallKind::OpenFresh)) {
                return Err(classic_flight_refusal());
            }
            let store_release = if store_handshake {
                if caller.principal.label_scope().is_some()
                    || caller.principal.workload_grant().is_some_and(|grant| !grant.is_store_grant() && !grant.is_placement_fence())
                    || (self.follower_only() || fresh_replica) && placement.is_none()
                {
                    return Err(Status::permission_denied("This credential or route does not serve the store contract"));
                }
                Some(StoreRelease::negotiate(request.metadata(), caller.verified_credential)?)
            } else { None };
            if timed_store.is_some() { StoreRelease::require(store_release)?; }
            let capacity = 1024 + placement.as_ref().map_or(0, |database| database.name().len());
            let session = Arc::new(Session::new(Uuid::new_v4().to_string(), Arc::clone(&caller),
                placement.as_ref(), fresh_replica, store_release, timed_store, self.memory().reserve(capacity).map_err(status)?));
            sessions.insert(session.id.clone(), Arc::clone(&session), self.0.config.session_idle_timeout,
                self.0.config.session_unclaimed_timeout, true, session.id.len()).map_err(status)?;
            let lease = AcquiredSession { service: self.clone(), session: Arc::clone(&session) };
            (session, true, lease, Some(excluded), false)
        };
        if session.caller.principal.label_scope().is_some() && !kind.allows_label_scope() {
            return Err(Status::permission_denied("The credential requires the scoped RTDB v1 route"));
        }
        let requested_database = requested_database
            .or_else(|| session.caller.principal.workload_grant().map(|grant| grant.database()));
        let (database, attachment) = if matches!(kind, CallKind::SessionClose) {
            session.resolve_close_route(requested_database, &self.0.node)?
        } else {
            session.resolve_database(requested_database, &self.0.node)?
        };
        if let Some(scope) = session.caller.principal.label_scope() {
            scope.admit_database_name(&database).map_err(status)?;
        }
        let classic_flight_excluded = match mint_exclusion {
            Some(excluded) => excluded,
            None if !retained_proof && matches!(kind, CallKind::OwnedAction | CallKind::OpenFresh) =>
                self.classic_flight_excluded(attachment.as_deref()).await?,
            None => false,
        };
        if classic_flight_excluded && matches!(kind, CallKind::OpenFresh) && !retained_proof { return Err(classic_flight_refusal()); }
        let active_call = attachment.as_deref().filter(|_| !retained_proof)
            .and_then(|attachment| self.0.node.hold_active_call(attachment)).map(Arc::from);
        let store_read = store_read.then(|| session.enter_store_read()).transpose()?;
        let call = Call { service: self.clone(), kind, session, minted, database, cancellation: self.0.cancellation.child_token(),
            admission, _lease: Some(lease), _active_call: active_call, store_read, _reservation: reservation,
            #[cfg(feature = "multi")]
            store_reply: std::sync::Mutex::new(None),
            egress: transport.map(|transport| transport.egress.clone()),
            routed_delivery: request.metadata().get(crate::GATEWAY_ANSWER_DEADLINE_HEADER).is_some_and(|header| header == "forward"),
            classic_flight_excluded, classic_retained: None };
        if !store_close && self.0.node.catalog().is_managed()
            && let Ok(attached) = self.0.node.catalog().database(&call.database)
        {
            call.require_imported_workload(&attached)?;
        }
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

pub(crate) async fn action_result_stream(
    call: Call, values: Vec<Result<flight::Result, Status>>,
    plaintext: Arc<rtdb_storage::sealing::PlaintextView>,
) -> Result<Response<Output<flight::Result>>, Status> {
    if !values.iter().any(Result::is_ok) { return result_stream(call, values); }
    handoff_action_results(call, values, plaintext, false).await
}

pub(crate) async fn handoff_action_results<T, I>(
    call: Call, values: I, plaintext: Arc<rtdb_storage::sealing::PlaintextView>, expose_cutoff: bool,
) -> Result<Response<Output<T>>, Status>
where
    T: Send + 'static,
    I: IntoIterator<Item = Result<T, Status>>,
    I::IntoIter: Send + 'static,
{
    let first = call.cancellation.run_until_cancelled(plaintext.grant()).await.ok_or_else(cancelled)?.map_err(status)?;
    let (mut headers, cutoff) = call.delivery_headers(&first, false)?;
    if expose_cutoff && let Some(deadline) = first.transport_deadline() {
        let until = deadline.wire_nanos().ok_or_else(|| Status::unavailable("Owned reply cutoff cannot be routed"))?;
        headers.insert("x-rtdb-owned-reply-until", until.to_string().parse().map_err(arrow)?);
    }
    let stream: Output<T> = Box::pin(futures::stream::unfold(
        (call, values.into_iter(), plaintext, Some(first), false),
        move |(call, mut values, plaintext, mut first, stopped)| async move {
            if stopped { return None; }
            let value = values.next()?;
            let result = call.cancellation.run_until_cancelled(async {
                let value = value?;
                let grant = match first.take() { Some(grant) => grant, None => plaintext.grant().await.map_err(status)? };
                call.handoff(grant, cutoff)?;
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
        if crate::v1::original_family_readback::is_readback(&call) {
            self.check_family_readback_handshake(&call).await.map_err(|status| call.error(status))?;
        }
        let release = if call.minted { call.session.store_release } else { None };
        let retained_basis_append = call.minted && call.session.caller.verified_credential
            && call.session.caller.principal.label_scope().is_none()
            && call.session.caller.principal.workload_grant().is_none()
            && !call.session.fresh_replica && !self.follower_only();
        let fenced_append = call.minted && call.session.caller.verified_credential
            && call.session.caller.principal.label_scope().is_none()
            && call.session.caller.principal.workload_grant().is_none_or(|grant| grant.is_store_grant() || grant.is_placement_fence())
            && !call.session.fresh_replica && !self.follower_only();
        let fenced_purge = retained_basis_append && self.supports_store_purge_floor(&call).await;
        #[cfg(feature = "multi")]
        let timed_append = call.session.timed_store_incarnation;
        let mut reply = result_stream(call, Vec::new())?;
        #[cfg(feature = "multi")]
        if let Some(incarnation) = timed_append {
            reply.metadata_mut().insert(crate::v1::store::TIMED_APPEND_CAPABILITY, tonic::metadata::MetadataValue::from_static("1"));
            reply.metadata_mut().insert_bin(
                crate::v1::store::timed_append::INCARNATION_HEADER,
                tonic::metadata::MetadataValue::from_bytes(&incarnation),
            );
        }
        if retained_basis_append {
            reply.metadata_mut().insert("x-rtdb-store-retained-basis", tonic::metadata::MetadataValue::from_static("1"));
        }
        if fenced_append {
            reply.metadata_mut().insert("x-rtdb-claim-alternative", tonic::metadata::MetadataValue::from_static("1"));
            reply.metadata_mut().insert(crate::v1::WRITER_FENCE_HEADER, tonic::metadata::MetadataValue::from_static("2"));
        }
        if fenced_purge {
            reply.metadata_mut().insert(crate::v1::store::PURGE_WRITER_HEADER, tonic::metadata::MetadataValue::from_static("2"));
        }
        if let Some(release) = release {
            reply.metadata_mut().insert(store_release::HEADER, release.metadata());
        }
        Ok(reply)
    }
    async fn list_flights(&self, request: Request<flight::Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> { let call = self.call(&request, request.get_ref().encoded_len()).await?; Err(call.error(unsupported())) }
    async fn poll_flight_info(&self, request: Request<flight::FlightDescriptor>) -> Result<Response<flight::PollInfo>, Status> { let call = self.call(&request, request.get_ref().encoded_len()).await?; Err(call.error(unsupported())) }
    async fn do_exchange(&self, request: Request<Streaming<flight::FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        use crate::owned_actions::{DECIDE_HOSTED_STREAM_V1, HOSTED_UPLOAD_HEADER};
        use crate::v1::store::{STORE_EXCHANGE_HEADER, TIMED_APPEND_VERSION};
        if request.metadata().contains_key(STORE_EXCHANGE_HEADER) {
            let versions = request.metadata().get_all(STORE_EXCHANGE_HEADER);
            let mut versions = versions.iter();
            if !versions.next().is_some_and(|version| version == TIMED_APPEND_VERSION)
                || versions.next().is_some() || request.metadata().contains_key(HOSTED_UPLOAD_HEADER)
                || request.metadata().contains_key(FORWARDED_PEER_HEADER)
                || request.metadata().contains_key(crate::GATEWAY_ANSWER_DEADLINE_HEADER)
            {
                return Err(unsupported());
            }
            #[cfg(feature = "multi")]
            {
                let kind = CallKind::StoreTimedAppend;
                let call = self.call_with_kind(&request, 0, kind).await.map_err(|status| kind.v1_admission_status(status))?;
                return self.timed_store_exchange(call, request.into_inner()).await;
            }
            #[cfg(not(feature = "multi"))]
            return Err(unsupported());
        }
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
        let hosted_upload = self.supports_hosted_upload(&call)
            .then_some(crate::owned_actions::DECIDE_HOSTED_STREAM_V1);
        let hosted_worlds = self.0.config.owned_world_host.is_some() && self.supports_hosted_upload(&call);
        let retired_family = self.supports_retired_original_family(&call)
            .then_some([
                crate::owned_family::RETIRED_ORIGINAL_FAMILY_V1,
                crate::owned_family::RETIRED_ORIGINAL_WORLD_RANK_V1,
            ]);
        let hosted_world_actions = [
            crate::owned_actions::DECIDE_HELD_WORLDS_V3,
            crate::owned_family::RESOLVE_FAMILY_MEMBER_V1,
            crate::owned_family::FAMILY_RESPONSE_RELEASE_V2,
            crate::owned_actions::HELD_WORLD_SHARED_COLUMN_CENSUS_V2,
            crate::owned_actions::HELD_WORLD_POINT_AT_V2,
        ];
        let hosted_carrier_actions = [
            crate::owned_actions::HELD_PROTECTED_COVERAGE_V2,
            crate::owned_actions::HELD_PROTECTED_CARRIERS_V2,
            crate::owned_actions::HELD_SEALED_SEARCH_UNIT_ROWS_V2,
        ];
        let root_rank = self.0.config.owned_native_actions;
        #[cfg(feature = "multi")]
        let actions = actions.into_iter().chain(crate::admin::ACTIONS.into_iter().filter(|name| self.supports_admin_action(&call, name)))
            .collect::<Vec<_>>();
        let store_enabled = !call.session.fresh_replica
            && call.session.caller.principal.label_scope().is_none();
        let purge_enabled = store_enabled && self.supports_store_purge_floor(&call).await;
        result_stream(call, actions.into_iter().chain(crate::v1::ACTIONS.into_iter()
            .filter(|name| !matches!(*name, crate::v1::MANAGED_WORKLOAD | crate::v1::exact_read::OPEN | crate::v1::exact_read::CLOSE)))
            .chain(crate::v1::STORE_ACTIONS.into_iter().filter(|name| store_enabled
                && (*name != crate::v1::store::PURGE_FLOOR || purge_enabled)))
            .chain(hosted_world_actions.into_iter().filter(|_| hosted_worlds))
            .chain(hosted_carrier_actions.into_iter().filter(|_| hosted_upload.is_some()))
            .chain(crate::owned_actions::ROOT_RANK_ACTIONS.into_iter().filter(|_| root_rank))
            .chain(crate::owned_actions::WORLD_RANK_ACTIONS.into_iter().filter(|_| hosted_worlds))
            .chain(hosted_upload)
            .chain(retired_family.into_iter().flatten())
            .map(|name| Ok(flight::ActionType { r#type: name.into(), description: name.into() })).collect())
    }
    async fn get_flight_info(&self, request: Request<flight::FlightDescriptor>) -> Result<Response<flight::FlightInfo>, Status> {
        let call = self.call(&request, request.get_ref().encoded_len()).await?;
        let descriptor = request.into_inner(); let cmd = command(&descriptor.cmd).map_err(|error| call.error(error))?;
        let work = async {
            let (schema, ticket) = match cmd {
                Command::CommandStatementQuery(command) => { let connection = self.tx_or_session(&call, command.transaction_id.as_ref()).await?; rtdb_types::boxed(|| self.query_info(&call, connection, command.transaction_id.as_ref(), &command.query, None, None)).await? }
                Command::CommandPreparedStatementQuery(command) => { let prepared = self.prepared(&call, &command.prepared_statement_handle)?; let params = prepared.params.lock().expect("parameters lock").clone(); let connection = self.scoped(&call, &prepared.scope).await?; let transaction = match &prepared.scope { PreparedScope::Transaction(handle) => Some(handle), PreparedScope::Session(_) => None }; rtdb_types::boxed(|| self.query_info(&call, connection, transaction, &prepared.sql, prepared.plan.as_ref(), params)).await? }
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
                Command::CommandPreparedStatementQuery(command) => { let prepared = self.prepared(&call, &command.prepared_statement_handle)?; let connection = self.scoped(&call, &prepared.scope).await?; self.describe_prepared(connection, &prepared.sql).await? }
                command => self.metadata_schema(&command)?,
            };
            Ok::<_, Status>(flight::SchemaResult { schema: wire::schema_message(&schema, self.memory())? })
        };
        let result = call.cancellation.run_until_cancelled(work).await.ok_or_else(cancelled).and_then(|result| result).map_err(|error| call.error(error))?; call.response(result)
    }
    async fn do_get(&self, request: Request<flight::Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = &request.get_ref().ticket;
        if let Some(command) = crate::v1::ticket(ticket) {
            if self.0.config.store_only {
                return Err(Status::unimplemented("This listener serves rtdb.store.v1, not rtdb.v1 queries"));
            }
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
            Command::TicketStatementQuery(ticket) => rtdb_types::boxed(|| self.query_stream(call, ticket.statement_handle)).await,
            command => self.metadata_stream(call, command).await,
        }
    }
    async fn do_put(&self, request: Request<Streaming<flight::FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        let call = self.call(&request, 0).await?;
        rtdb_types::boxed(|| self.upload(call, request.into_inner())).await
    }
    async fn do_action(&self, request: Request<flight::Action>) -> Result<Response<Self::DoActionStream>, Status> {
        let name = request.get_ref().r#type.as_str();
        if crate::v1::requests_funded_offer(name) {
            return Err(crate::v1::error::funding_kernel_unavailable());
        }
        if crate::v1::is_action(name) {
            if self.0.config.store_only && name != crate::v1::TRANSACT && !crate::v1::store_only_admits(name) {
                return Err(Status::unimplemented(format!("This listener serves rtdb.store.v1, not {name}")));
            }
            let kind = match name {
                crate::v1::CAPABILITIES => CallKind::V1Capabilities,
                crate::v1::exact_read::OPEN | crate::v1::exact_read::CLOSE => CallKind::V1ExactRead,
                crate::v1::MANAGED_CREATE => CallKind::ManagedCreate,
                crate::v1::MANAGED_WORKLOAD => CallKind::ManagedWorkload,
                crate::v1::TX_STATUS => CallKind::TxStatus,
                crate::v1::custody::RELEASE => CallKind::V1CustodyRelease,
                crate::v1::original_family_landing::ACTION => CallKind::V1OriginalFamily,
                crate::v1::original_family_readback::ACTION => CallKind::V1OriginalFamilyReadback,
                crate::v1::original_family_landing::private_replay::ACTION => CallKind::V1OriginalPrivateReplay,
                crate::v1::TRANSACT => CallKind::V1Appointment,
                name if crate::v1::is_store_reader_action(name) => CallKind::StoreRead,
                name if crate::v1::is_store_writer_action(name) => CallKind::StoreWriter,
                name if crate::v1::is_store_action(name) => CallKind::Store,
                _ => CallKind::V1Action,
            };
            let call = self.call_with_kind(&request, request.get_ref().encoded_len(), kind)
                .await.map_err(|status| kind.v1_admission_status(status))?;
            return rtdb_types::boxed(|| self.v1_action(call, request.into_inner())).await;
        }
        let kind = CallKind::action(name);
        let mut call = rtdb_types::boxed(|| self.call_with_kind(&request, request.get_ref().encoded_len(), kind)).await?;
        let proof_headers = request.metadata().get_all(CLASSIC_RETAINED_PROOF);
        let mut proofs = proof_headers.iter();
        if let Some(proof) = proofs.next() {
            let proof = proof.to_str().ok().filter(|proof| proof.len() <= MAX_CLASSIC_RETAINED_PROOF)
                .ok_or_else(classic_retained_refusal)?;
            if proofs.next().is_some() || call.session.fresh_replica
                || !call.session.caller.verified_credential || call.egress.is_none()
                || !crate::actions::classic_retained_action(name)
            {
                return Err(classic_retained_refusal());
            }
            let principal = call.session.caller.principal.user_id().ok_or_else(classic_retained_refusal)?;
            let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
            let attachment = attachment.ok_or_else(classic_retained_refusal)?;
            let authority = self.0.config.classic_flight_exclusion.as_ref().ok_or_else(classic_retained_refusal)?;
            let admission = authority.verify_retained(&attachment, crate::ClassicRetainedRead {
                principal, action: name, body_sha256: Sha256::digest(&request.get_ref().body).into(), proof,
            }).await.map_err(status)?;
            call.classic_retained = Some(admission);
            call._active_call = self.0.node.hold_active_call(&attachment).map(Arc::from);
        }
        let ipc_stream = request.metadata().get(crate::actions::OWNED_IPC_HEADER)
            .is_some_and(|encoding| encoding == "stream-v1");
        rtdb_types::boxed(|| self.action(call, request.into_inner(), ipc_stream)).await
    }
}

pub(crate) fn classic_flight_refusal() -> Status {
    Status::failed_precondition("The Classic Original excludes unclassified FreshFlight")
}

fn classic_retained_refusal() -> Status {
    Status::permission_denied("The request lacks its exact Classic retained read authority")
}

fn follower_refusal() -> Status {
    Status::permission_denied("The assigned follower does not serve this call")
}

pub(crate) fn query_ticket(ticket: Bytes, memory: &Arc<MemoryBudget>) -> Result<Bytes, Status> {
    let charge = memory.reserve(wire::add(wire::mul(ticket.len(), 3)?, 1024)?).map_err(status)?;
    Ok(wire::retain(TicketStatementQuery { statement_handle: ticket }.as_any().encode_to_vec(), charge))
}

#[cfg(test)]
mod tests {
    use super::{CallKind, DeliveryDeadline, MemoryBudget, routed_answer_deadline, session_error};
    use std::sync::Arc;
    use tonic::{Code, Status};

    #[test]
    fn store_readers_distinguish_bearer_loss_from_refused_admission() {
        let map = |status| CallKind::StoreRead.v1_admission_status(status).code();
        for message in ["unknown session", "session expired"] {
            assert_eq!(map(session_error(true, message)), Code::Unauthenticated);
        }
        assert_eq!(map(Status::permission_denied("refused")), Code::PermissionDenied);
        assert_eq!(CallKind::Store.v1_admission_status(session_error(true, "unknown session")).code(),
            Code::PermissionDenied);
    }

    #[test]
    fn label_scope_is_only_admitted_to_v1_and_handshake() {
        for kind in [CallKind::Handshake, CallKind::V1Query, CallKind::V1Changes, CallKind::V1Capabilities, CallKind::V1Action, CallKind::TxStatus] {
            assert!(kind.allows_label_scope());
        }
        for kind in [CallKind::Ordinary, CallKind::Action, CallKind::ManagedCreate, CallKind::ManagedWorkload] {
            assert!(!kind.allows_label_scope());
        }
    }

    #[test]
    fn follower_admission_has_a_positive_query_allowlist() {
        for kind in [CallKind::Handshake, CallKind::V1Query, CallKind::V1Capabilities, CallKind::V1ExactRead] {
            assert!(kind.serves_assigned_follower());
        }
        for kind in [CallKind::Ordinary, CallKind::Action, CallKind::V1Changes, CallKind::V1Action, CallKind::ManagedCreate, CallKind::ManagedWorkload, CallKind::TxStatus] {
            assert!(!kind.serves_assigned_follower());
        }
    }

    #[test]
    fn workload_root_reader_has_a_read_only_allowlist() {
        for kind in [CallKind::Handshake, CallKind::V1Query, CallKind::V1Changes, CallKind::V1Capabilities] {
            assert!(kind.allows_workload_root_read());
        }
        for kind in [CallKind::Ordinary, CallKind::Action,
            CallKind::V1Action, CallKind::ManagedWorkload,
            CallKind::ManagedCreate, CallKind::TxStatus] {
            assert!(!kind.allows_workload_root_read());
        }
    }

    #[test]
    fn workload_scope_has_a_positive_managed_action_allowlist() {
        for kind in [CallKind::Handshake, CallKind::ManagedCreate, CallKind::ManagedWorkload, CallKind::TxStatus] {
            assert!(kind.allows_workload_grant());
        }
        for kind in [CallKind::Ordinary, CallKind::Action, CallKind::V1Query, CallKind::V1Changes, CallKind::V1Capabilities, CallKind::V1Action] {
            assert!(!kind.allows_workload_grant());
        }
    }

    #[test]
    fn funded_appointment_calls_have_a_separate_positive_allowlist() {
        for kind in [CallKind::Handshake, CallKind::V1Capabilities, CallKind::V1Appointment, CallKind::TxStatus] {
            assert!(kind.allows_funded_appointment());
        }
        for kind in [CallKind::Ordinary, CallKind::Action, CallKind::V1Query,
            CallKind::V1Changes, CallKind::V1Action, CallKind::V1CustodyRelease, CallKind::V1OriginalFamily,
            CallKind::ManagedCreate, CallKind::ManagedWorkload] {
            assert!(!kind.allows_funded_appointment());
        }
        assert!(!CallKind::V1Appointment.allows_workload_grant());
    }

    #[test]
    fn original_control_is_separate_from_workload_routes() {
        for kind in [CallKind::Handshake, CallKind::V1CustodyRelease] {
            assert!(kind.allows_original_custody_control());
        }
        for kind in [CallKind::Ordinary, CallKind::Action, CallKind::V1Query,
            CallKind::V1Changes, CallKind::V1Capabilities, CallKind::V1Action, CallKind::ManagedCreate,
            CallKind::ManagedWorkload, CallKind::TxStatus] {
            assert!(!kind.allows_original_custody_control());
        }
        let release = CallKind::V1CustodyRelease;
        assert!(!release.allows_label_scope());
        assert!(!release.allows_workload_grant());
        assert!(!release.allows_workload_root_read());
        assert!(!release.serves_assigned_follower());
    }

    #[test]
    fn original_family_admits_only_its_handshake_capabilities_review_and_action() {
        for kind in [CallKind::Handshake, CallKind::V1Capabilities, CallKind::V1Query, CallKind::V1OriginalFamily] {
            assert!(kind.allows_original_family());
        }
        for kind in [CallKind::Ordinary, CallKind::Action, CallKind::V1Changes,
            CallKind::V1Action, CallKind::V1CustodyRelease, CallKind::ManagedCreate, CallKind::ManagedWorkload,
            CallKind::TxStatus] {
            assert!(!kind.allows_original_family());
        }
        let family = CallKind::V1OriginalFamily;
        assert!(!family.allows_workload_grant());
        assert!(!family.allows_workload_root_read());
        assert!(!family.allows_original_custody_control());
        assert!(!family.allows_label_scope());
        assert!(!family.serves_assigned_follower());
    }

    #[test]
    fn original_family_readback_admits_only_its_handshake_capabilities_and_action() {
        for kind in [CallKind::Handshake, CallKind::V1Capabilities, CallKind::V1OriginalFamilyReadback] {
            assert!(kind.allows_original_family_readback());
        }
        for kind in [CallKind::Ordinary, CallKind::Action, CallKind::V1Query,
            CallKind::V1Changes, CallKind::V1Action, CallKind::V1CustodyRelease, CallKind::V1OriginalFamily,
            CallKind::V1Appointment, CallKind::ManagedCreate, CallKind::ManagedWorkload, CallKind::TxStatus] {
            assert!(!kind.allows_original_family_readback());
        }
        let readback = CallKind::V1OriginalFamilyReadback;
        assert!(!readback.allows_original_family());
        assert!(!readback.allows_workload_grant());
        assert!(!readback.allows_workload_root_read());
        assert!(!readback.allows_original_custody_control());
        assert!(!readback.allows_label_scope());
        assert!(!readback.serves_assigned_follower());
    }

    #[tokio::test]
    async fn routed_streaming_deadline_is_capped_before_loading_encrypted_batches() {
        use std::time::{Duration, Instant};
        use rtdb_storage::{HeldView, ReadLease, sealing::PlaintextView};

        let plaintext = PlaintextView::new(MemoryBudget::new(1 << 20));
        let long = plaintext.grant().await.unwrap().holding(HeldView::recovery(Arc::new(())),
            Some(Arc::new(ReadLease::until(Instant::now() + Duration::from_secs(60)))));
        let long = long.transport_deadline().unwrap();
        let selected = routed_answer_deadline(Some(long), true).unwrap();
        assert!(long.covers(selected));
        assert!(!selected.covers(long));
        assert!(DeliveryDeadline::answer_window().covers(selected));

        let short = plaintext.grant().await.unwrap().holding(HeldView::recovery(Arc::new(())),
            Some(Arc::new(ReadLease::until(Instant::now() + Duration::from_secs(5)))));
        let short = short.transport_deadline().unwrap();
        let selected = routed_answer_deadline(Some(short), true).unwrap();
        assert!(short.covers(selected) && selected.covers(short));
        assert!(routed_answer_deadline(Some(long), false).unwrap().covers(long));
    }
}
