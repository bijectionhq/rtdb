use std::{collections::BTreeMap, sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}};
use bytes::Bytes;
use rtdb_auth::Principal;
use rtdb_columnar::MemoryReservation;
use rtdb_node::{NativeSession, Node};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use uuid::Uuid;
use crate::{error::status, lease::Leased};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Owner { Anonymous, Authenticated(Uuid) }
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
enum SessionState { Open { connections: BTreeMap<String, (Arc<ConnectionResource>, Arc<MemoryReservation>)>, catalog: Option<CatalogSelection> }, Closed }
pub(crate) struct Session { pub id: String, pub caller: Arc<Caller>, state: Mutex<SessionState>, _reservation: Arc<MemoryReservation> }
impl Session {
    pub fn new(id: String, caller: Arc<Caller>, reservation: Arc<MemoryReservation>) -> Self { Self { id, caller, state: Mutex::new(SessionState::Open { connections: BTreeMap::new(), catalog: None }), _reservation: reservation } }
    pub fn closed(&self) -> bool { matches!(*self.state.lock().expect("session lock"), SessionState::Closed) }
    pub fn database(&self, header: Option<&str>) -> Result<String, Status> {
        match &*self.state.lock().expect("session lock") { SessionState::Open { catalog, .. } => Ok(header.or_else(|| catalog.as_ref().map(|selection| selection.name.as_str())).unwrap_or("xtdb").into()), SessionState::Closed => Err(Status::not_found("session closed")) }
    }
    pub fn select_catalog(&self, name: Option<String>, node: &Node) -> Result<(), Status> {
        let selection = name.map(|name| Ok::<_, Status>(CatalogSelection { _reservation: node.memory().reserve(name.capacity() + 128).map_err(status)?, name })).transpose()?;
        match &mut *self.state.lock().expect("session lock") { SessionState::Open { catalog, .. } => { *catalog = selection; Ok(()) }, SessionState::Closed => Err(Status::not_found("session closed")) }
    }
    pub fn connection(&self, database: &str, node: &Node) -> Result<Arc<ConnectionResource>, Status> {
        let mut state = self.state.lock().expect("session lock");
        let SessionState::Open { connections, .. } = &mut *state else { return Err(Status::not_found("session closed")); };
        if let Some((connection, _)) = connections.get(database) { return Ok(Arc::clone(connection)); }
        let charge = node.memory().reserve(database.len() + 512).map_err(status)?;
        let connection = ConnectionResource::new(node.session(database, self.caller.principal.clone()).map_err(status)?);
        connections.insert(database.into(), (Arc::clone(&connection), charge)); Ok(connection)
    }
}
impl Leased for Session {
    fn close(&self) { if let SessionState::Open { connections, .. } = std::mem::replace(&mut *self.state.lock().expect("session lock"), SessionState::Closed) { for (connection, _) in connections.into_values() { connection.close(); } } }
}
impl Drop for Session { fn drop(&mut self) { self.close(); } }
pub(crate) struct Transaction { pub owner: Owner, pub connection: Arc<ConnectionResource>, _reservation: Arc<MemoryReservation> }
impl Transaction { pub fn new(owner: Owner, connection: Arc<ConnectionResource>, reservation: Arc<MemoryReservation>) -> Self { Self { owner, connection, _reservation: reservation } } }
impl Leased for Transaction { fn close(&self) { self.connection.close(); } }
impl Drop for Transaction { fn drop(&mut self) { self.close(); } }
#[derive(Clone)]
pub(crate) enum PreparedScope { Session(String), Transaction(Bytes) }
pub(crate) struct Prepared { pub owner: Owner, pub sql: String, pub scope: PreparedScope, pub params: Mutex<Option<Bytes>>, _reservation: Arc<MemoryReservation> }
impl Prepared { pub fn new(owner: Owner, sql: String, scope: PreparedScope, reservation: Arc<MemoryReservation>) -> Self { Self { owner, sql, scope, params: Mutex::new(None), _reservation: reservation } } }
impl Leased for Prepared { fn close(&self) { *self.params.lock().expect("prepared parameters lock") = None; } }
