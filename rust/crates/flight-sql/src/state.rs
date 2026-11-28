use std::{collections::BTreeMap, sync::{Arc, Mutex, Weak, atomic::{AtomicBool, Ordering}}};
use bytes::Bytes;
use rtdb_auth::Principal;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_node::{Database, NativeSession, Node, PreparedSql};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use uuid::Uuid;
use crate::{error::status, lease::Leased, store_release::StoreRelease, wire::BoundParameters};

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
pub(crate) struct Caller { pub owner: Owner, pub principal: Arc<Principal>, pub mac: Option<[u8; 32]>, pub verified_credential: bool }
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
    },
    Closed,
}
struct Placement { name: String, attachment: Weak<Database> }

#[derive(Default)]
struct StoreReadDrain {
    active: std::sync::atomic::AtomicUsize,
    finished: tokio::sync::Notify,
}

/// One admitted call, including all Flight-owned copies of its reply buffer.
pub(crate) struct StoreReadLease(Arc<StoreReadDrain>);

impl Drop for StoreReadLease {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.finished.notify_waiters();
        }
    }
}

pub(crate) struct Session {
    pub id: String,
    pub caller: Arc<Caller>,
    pub fresh_replica: bool,
    pub(crate) store_release: Option<StoreRelease>,
    pub(crate) timed_store_incarnation: Option<[u8; 16]>,
    placement: Option<Placement>,
    state: Mutex<SessionState>,
    store_reads: Arc<StoreReadDrain>,
    _reservation: Arc<MemoryReservation>,
}
impl Session {
    pub fn new(id: String, caller: Arc<Caller>, local: Option<&Arc<Database>>, fresh_replica: bool,
        store_release: Option<StoreRelease>, timed_store_incarnation: Option<[u8; 16]>, reservation: Arc<MemoryReservation>) -> Self {
        let placement = local.map(|database| Placement { name: database.name().into(), attachment: Arc::downgrade(database) });
        Self {
            id, caller, placement, fresh_replica, store_release, timed_store_incarnation,
            state: Mutex::new(SessionState::Open { connections: BTreeMap::new(), catalog: None }),
            store_reads: Arc::default(),
            _reservation: reservation,
        }
    }

    pub(crate) fn register_read(&self, register: impl FnOnce() -> Result<(), Status>) -> Result<(), Status> {
        let state = self.state.lock().expect("session lock");
        if matches!(*state, SessionState::Closed) {
            return Err(Status::not_found("session closed"));
        }
        register()
    }

    pub(crate) fn enter_store_read(&self) -> Result<Arc<StoreReadLease>, Status> {
        let mut lease = None;
        self.register_read(|| {
            self.store_reads.active.fetch_add(1, Ordering::AcqRel);
            lease = Some(Arc::new(StoreReadLease(self.store_reads.clone())));
            Ok(())
        })?;
        Ok(lease.expect("Store reader registered under the session lock"))
    }

    pub(crate) async fn wait_store_reads(&self) {
        loop {
            let completion = self.store_reads.finished.notified();
            tokio::pin!(completion);
            completion.as_mut().enable();
            if self.store_reads.active.load(Ordering::Acquire) == 0 {
                return;
            }
            completion.await;
        }
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
        let mut native = node.session(database, identity).map_err(status)?;
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
    fn close(&self) {
        let state = std::mem::replace(&mut *self.state.lock().expect("session lock"), SessionState::Closed);
        if let SessionState::Open { connections, .. } = state {
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
pub(crate) struct Prepared { pub owner: ResourceOwner, pub sql: String, pub plan: Option<PreparedSql>, pub scope: PreparedScope, pub params: Mutex<Option<Arc<BoundParameters>>>, _reservation: Arc<MemoryReservation> }
impl Prepared { pub fn new(owner: ResourceOwner, sql: String, plan: Option<PreparedSql>, scope: PreparedScope, reservation: Arc<MemoryReservation>) -> Self { Self { owner, sql, plan, scope, params: Mutex::new(None), _reservation: reservation } } }
impl Leased for Prepared { fn close(&self) { *self.params.lock().expect("prepared parameters lock") = None; } }
