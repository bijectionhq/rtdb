use std::{collections::BTreeMap, sync::{Arc, Mutex, Weak, atomic::{AtomicBool, Ordering}}};
use bytes::Bytes;
use rtdb_auth::Principal;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_node::{Database, NativeSession, Node};
use rtdb_storage::cache::CacheBudget;
use tokio_util::sync::{CancellationToken, DropGuard};
use tonic::Status;
use uuid::Uuid;
use crate::{error::status, lease::Leased, owned_actions::OwnedReadResource, wire::BoundParameters};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Owner { Anonymous, Authenticated(Uuid) }
pub(crate) struct ResourceOwner {
    principal: Owner,
    placement: Option<Weak<Database>>,
}
impl ResourceOwner {
    fn matches(&self, principal: Owner, placement: Option<&Weak<Database>>) -> bool {
        self.principal == principal && match (&self.placement, placement) {
            (Some(expected), Some(actual)) => expected.ptr_eq(actual),
            (None, None) => true,
            _ => false,
        }
    }
}
pub(crate) struct Caller { pub owner: Owner, pub principal: Arc<Principal>, pub mac: Option<[u8; 32]> }
pub(crate) struct ConnectionResource { pub native: tokio::sync::Mutex<NativeSession>, pub cancellation: CancellationToken, closed: AtomicBool }
impl ConnectionResource {
    pub fn new(native: NativeSession) -> Arc<Self> { let cancellation = native.state().cancellation().clone(); Arc::new(Self { native: tokio::sync::Mutex::new(native), cancellation, closed: AtomicBool::new(false) }) }
    pub async fn borrow(&self) -> Result<tokio::sync::MutexGuard<'_, NativeSession>, Status> {
        let guard = self.cancellation.run_until_cancelled(self.native.lock()).await.ok_or_else(|| Status::not_found("connection closed"))?;
        if self.closed.load(Ordering::Acquire) { return Err(Status::not_found("connection closed")); } Ok(guard)
    }
}
impl Leased for ConnectionResource {
    fn close(&self) { self.closed.store(true, Ordering::Release); self.cancellation.cancel(); if let Ok(mut native) = self.native.try_lock() { native.state_mut().close(); } }
}
impl Drop for ConnectionResource { fn drop(&mut self) { self.native.get_mut().state_mut().close(); } }
struct CatalogSelection { name: String, _reservation: Arc<MemoryReservation> }
enum SessionState {
    Open {
        connections: BTreeMap<String, (Arc<ConnectionResource>, Arc<MemoryReservation>)>,
        catalog: Option<CatalogSelection>,
        audience_generation: u64,
        audience_read: Option<(Bytes, Arc<OwnedReadResource>)>,
    },
    Closed,
}
struct Placement { name: String, attachment: Weak<Database> }

pub(crate) struct AudienceProjection {
    pub relation: rtdb_types::OwnedRelation,
    pub address_path: rtdb_types::OwnedRelation,
}

pub(crate) struct AudienceSession {
    pub source: Uuid,
    pub projection: std::sync::OnceLock<AudienceProjection>,
    pub recovery_work: rtdb_storage::audience_work::TurnAllowance,
    pub seat: crate::AudienceSeat,
    pub cache: Arc<CacheBudget>,
    pub expires_at: tokio::time::Instant,
    pub _claim: DropGuard,
}

pub(crate) struct Session {
    pub id: String,
    pub caller: Arc<Caller>,
    pub fresh_replica: bool,
    placement: Option<Placement>,
    state: Mutex<SessionState>,
    _reservation: Arc<MemoryReservation>,
    // Release the directory claim after the registered session state and its charge.
    audience: Option<AudienceSession>,
}
impl Session {
    pub fn new(id: String, caller: Arc<Caller>, local: Option<&Arc<Database>>, fresh_replica: bool, audience: Option<AudienceSession>, reservation: Arc<MemoryReservation>) -> Self {
        let placement = local.map(|database| Placement { name: database.name().into(), attachment: Arc::downgrade(database) });
        Self {
            id, caller, placement, fresh_replica,
            state: Mutex::new(SessionState::Open { connections: BTreeMap::new(), catalog: None, audience_generation: 0, audience_read: None }),
            _reservation: reservation,
            audience,
        }
    }
    pub fn audience_source(&self) -> Option<Uuid> { self.audience.as_ref().map(|audience| audience.source) }
    pub fn audience_seat(&self) -> Option<crate::AudienceSeat> { self.audience.as_ref().map(|audience| audience.seat) }

    pub(crate) fn with_audience_handoff<T>(
        &self,
        cleanup: bool,
        handoff: impl FnOnce() -> Result<T, Status>,
    ) -> Result<T, Status> {
        let state = self.state.lock().expect("session lock");
        let open = matches!(*state, SessionState::Open { .. });
        if !crate::audience_seat::audience_handoff_permitted(open, cleanup) {
            return Err(Status::not_found("session closed"));
        }
        let result = handoff();
        drop(state);
        result
    }

    pub fn install_audience_projection(&self, projection: AudienceProjection) -> Result<(), Status> {
        self.audience.as_ref().ok_or_else(crate::error::unsupported)?.projection.set(projection)
            .map_err(|_| Status::failed_precondition("The audience projection is already prepared"))
    }

    pub fn audience_projection(&self) -> Result<&AudienceProjection, Status> {
        self.audience.as_ref().and_then(|audience| audience.projection.get())
            .ok_or_else(|| Status::failed_precondition("The audience projection is not prepared"))
    }

    pub fn audience_recovery_work(&self) -> Option<rtdb_storage::audience_work::TurnAllowance> {
        self.audience.as_ref().map(|audience| audience.recovery_work.clone())
    }

    pub fn audience_remaining_micros(&self) -> Option<u64> {
        let remaining = self.audience.as_ref()?.expires_at.saturating_duration_since(tokio::time::Instant::now());
        u64::try_from(remaining.as_micros()).ok().filter(|remaining| *remaining > 0)
    }

    pub fn audience_read(&self, handle: &Bytes) -> Option<Arc<OwnedReadResource>> {
        let state = self.state.lock().expect("session lock");
        let SessionState::Open { audience_read: Some((id, read)), .. } = &*state else { return None; };
        (id == handle).then(|| Arc::clone(read))
    }

    pub fn begin_audience_read(&self, generation: u64) -> Result<(), Status> {
        let previous = {
            let mut state = self.state.lock().expect("session lock");
            let SessionState::Open { audience_generation, audience_read, .. } = &mut *state else {
                return Err(Status::not_found("session closed"));
            };
            if self.audience.is_none() || generation <= *audience_generation {
                return Err(Status::failed_precondition("The audience read generation is stale"));
            }
            *audience_generation = generation;
            audience_read.take()
        };
        if let Some((_, read)) = previous { read.close(); }
        Ok(())
    }

    pub fn take_audience_read(&self, handle: &Bytes) -> Option<Arc<OwnedReadResource>> {
        let mut state = self.state.lock().expect("session lock");
        let SessionState::Open { audience_read, .. } = &mut *state else { return None; };
        if audience_read.as_ref().is_none_or(|(id, _)| id != handle) { return None; }
        audience_read.take().map(|(_, read)| read)
    }

    pub fn install_audience_read(&self, generation: u64, handle: Bytes, read: Arc<OwnedReadResource>) -> Result<(), Status> {
        let mut state = self.state.lock().expect("session lock");
        let SessionState::Open { audience_generation, audience_read, .. } = &mut *state else { return Err(Status::not_found("session closed")); };
        if generation == 0 || generation != *audience_generation || audience_read.is_some() {
            return Err(Status::failed_precondition("The audience read generation changed"));
        }
        *audience_read = Some((handle, read));
        Ok(())
    }

    pub fn memory(&self) -> &Arc<MemoryBudget> { self._reservation.budget() }
    pub fn closed(&self) -> bool { matches!(*self.state.lock().expect("session lock"), SessionState::Closed) }
    pub fn placed_database(&self) -> Option<&str> { self.placement.as_ref().map(|placement| placement.name.as_str()) }
    pub fn holds_attachment(&self, database: &Database) -> bool {
        self.placement.as_ref().is_some_and(|placement| std::ptr::eq(placement.attachment.as_ptr(), database))
    }
    pub fn owner(&self) -> ResourceOwner {
        ResourceOwner { principal: self.caller.owner, placement: self.placement.as_ref().map(|placement| placement.attachment.clone()) }
    }
    pub fn owns(&self, owner: &ResourceOwner) -> bool {
        owner.matches(self.caller.owner, self.placement.as_ref().map(|placement| &placement.attachment))
    }
    fn require_database(&self, database: &str, node: &Node) -> Result<Option<Arc<Database>>, Status> {
        if let Some(placement) = &self.placement {
            if database != placement.name {
                return Err(Status::permission_denied("This Flight SQL session belongs to another deployment"));
            }
            let current = node.catalog().database(database).map_err(|_| placement_changed())?;
            if !self.holds_attachment(&current) { return Err(placement_changed()); }
            return Ok(Some(current));
        }
        Ok(None)
    }
    pub fn resolve_database(&self, header: Option<&str>, node: &Node) -> Result<(String, Option<Arc<Database>>), Status> {
        let state = self.state.lock().expect("session lock");
        let SessionState::Open { catalog, .. } = &*state else { return Err(Status::not_found("session closed")); };
        let database = header.or_else(|| self.placed_database()).or_else(|| catalog.as_ref().map(|selection| selection.name.as_str())).unwrap_or("xtdb");
        let attachment = self.require_database(database, node)?;
        Ok((database.into(), attachment))
    }
    pub fn select_catalog(&self, name: Option<String>) -> Result<(), Status> {
        let selection = name.map(|name| Ok::<_, Status>(CatalogSelection { _reservation: self.memory().reserve(name.capacity() + 128).map_err(status)?, name })).transpose()?;
        match &mut *self.state.lock().expect("session lock") { SessionState::Open { catalog, .. } => { *catalog = selection; Ok(()) }, SessionState::Closed => Err(Status::not_found("session closed")) }
    }
    pub fn open_native(&self, database: &str, node: &Node) -> Result<NativeSession, Status> {
        self.require_database(database, node)?;
        let identity = self.caller.principal.clone();
        let mut native = if let Some(audience) = &self.audience {
            node.audience_session(database, identity, audience.source, Arc::clone(self.memory()), Arc::clone(&audience.cache))
        } else {
            node.session(database, identity)
        }.map_err(status)?;
        native.state_mut().set_application_name("XTDB Flight SQL").map_err(status)?;
        self.require_database(database, node)?;
        Ok(native)
    }
    pub fn connection(&self, database: &str, node: &Node) -> Result<Arc<ConnectionResource>, Status> {
        self.require_database(database, node)?;
        let mut state = self.state.lock().expect("session lock");
        let SessionState::Open { connections, .. } = &mut *state else { return Err(Status::not_found("session closed")); };
        if let Some((connection, _)) = connections.get(database) { return Ok(Arc::clone(connection)); }
        let charge = self.memory().reserve(database.len() + 512).map_err(status)?;
        let connection = ConnectionResource::new(self.open_native(database, node)?);
        connections.insert(database.into(), (Arc::clone(&connection), charge)); Ok(connection)
    }
}

fn placement_changed() -> Status { Status::unavailable("This node no longer holds the Flight SQL session's deployment") }

impl Leased for Session {
    fn hard_deadline(&self) -> Option<tokio::time::Instant> { self.audience.as_ref().map(|audience| audience.expires_at) }

    fn close(&self) {
        let state = std::mem::replace(&mut *self.state.lock().expect("session lock"), SessionState::Closed);
        if let SessionState::Open { connections, audience_read, .. } = state {
            if let Some((_, read)) = audience_read { read.close(); }
            for (connection, _) in connections.into_values() { connection.close(); }
        }
    }
}
impl Drop for Session { fn drop(&mut self) { self.close(); } }
pub(crate) struct Transaction { pub owner: ResourceOwner, pub connection: Arc<ConnectionResource>, _reservation: Arc<MemoryReservation> }
impl Transaction { pub fn new(owner: ResourceOwner, connection: Arc<ConnectionResource>, reservation: Arc<MemoryReservation>) -> Self { Self { owner, connection, _reservation: reservation } } }
impl Leased for Transaction { fn close(&self) { self.connection.close(); } }
impl Drop for Transaction { fn drop(&mut self) { self.close(); } }
#[derive(Clone)]
pub(crate) enum PreparedScope { Session(String), Transaction(Bytes) }
pub(crate) struct Prepared { pub owner: ResourceOwner, pub sql: String, pub scope: PreparedScope, pub params: Mutex<Option<Arc<BoundParameters>>>, _reservation: Arc<MemoryReservation> }
impl Prepared { pub fn new(owner: ResourceOwner, sql: String, scope: PreparedScope, reservation: Arc<MemoryReservation>) -> Self { Self { owner, sql, scope, params: Mutex::new(None), _reservation: reservation } } }
impl Leased for Prepared { fn close(&self) { *self.params.lock().expect("prepared parameters lock") = None; } }

#[cfg(test)]
mod tests {
    use super::{Caller, Leased, Owner, Session};
    use rtdb_auth::{AuthConfig, Authenticator, NativeHttp};
    use rtdb_columnar::MemoryBudget;
    use std::{sync::Arc, time::Duration};
    use tonic::Code;

    #[tokio::test]
    async fn closed_session_refuses_data_handoff_and_acknowledges_cleanup() {
        let memory = MemoryBudget::new(4096);
        let http = NativeHttp::new(Arc::clone(&memory), Duration::from_secs(1), 1024).unwrap();
        let auth = Authenticator::open(
            AuthConfig::SingleRootUser { password: None },
            Arc::clone(&memory),
            Arc::new(rtdb_node::SystemClock),
            Arc::new(http),
        ).await.unwrap();
        let caller = Arc::new(Caller {
            owner: Owner::Anonymous,
            principal: auth.unauthenticated().unwrap(),
            mac: None,
        });
        let session = Session::new("session".into(), caller, None, false, None, memory.reserve(256).unwrap());

        session.with_audience_handoff(false, || Ok(())).unwrap();
        session.close();
        let refusal = session.with_audience_handoff::<()>(false, || panic!("closed egress ran"));
        assert_eq!(refusal.unwrap_err().code(), Code::NotFound);
        session.with_audience_handoff(true, || Ok(())).unwrap();
    }
}
