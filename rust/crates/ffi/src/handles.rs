use std::{collections::BTreeMap, sync::{Arc, LazyLock, atomic::{AtomicU64, Ordering}}};
use parking_lot::Mutex;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_tx::{RetainedAnomaly, RetainedBytes};
use rtdb_types::{Anomaly, Result};
use tokio_util::sync::CancellationToken;
use crate::{auth::{DeviceHandle, PrincipalHandle}, region::DatabaseHandle, statement::{CursorHandle, SessionHandle, StatementHandle}};

pub(crate) struct Diagnostic { pub error: RetainedAnomaly, pub wire: RetainedBytes }
pub(crate) struct NativeBytes { pub value: RetainedBytes, pub memory: Arc<MemoryBudget> }
pub(crate) struct OperationToken { pub token: CancellationToken, pub session: Arc<SessionHandle>, pub _reservation: Arc<MemoryReservation> }
impl Drop for OperationToken { fn drop(&mut self) { self.token.cancel(); } }

#[derive(Clone)]
pub(crate) enum Handle {
    Database(Arc<DatabaseHandle>), Session(Arc<SessionHandle>), Statement(Arc<StatementHandle>),
    Cursor(Arc<CursorHandle>), Cancellation(Arc<OperationToken>), Error(Arc<Diagnostic>), Bytes(Arc<NativeBytes>),
    Principal(Arc<PrincipalHandle>), Device(Arc<DeviceHandle>),
}
impl Handle {
    pub fn memory(&self) -> &Arc<MemoryBudget> {
        match self {
            Self::Database(value) => value.0.memory(), Self::Session(value) => value.region.memory(),
            Self::Statement(value) => value.session.region.memory(), Self::Cursor(value) => value.region.memory(),
            Self::Cancellation(value) => value.session.region.memory(), Self::Error(_) => &ERROR_MEMORY, Self::Bytes(value) => &value.memory,
            Self::Principal(value) => &value.memory, Self::Device(value) => &value.memory,
        }
    }
}
struct Entry { value: Handle, _reservation: Arc<MemoryReservation> }
static REGISTRY: LazyLock<Mutex<BTreeMap<u64, Entry>>> = LazyLock::new(|| Mutex::new(BTreeMap::new()));
static NEXT: AtomicU64 = AtomicU64::new(2);
static ERROR_MEMORY: LazyLock<Arc<MemoryBudget>> = LazyLock::new(|| MemoryBudget::new(16 * 1024 * 1024));

pub(crate) fn insert(value: Handle) -> Result<u64> {
    let reservation = value.memory().reserve(std::mem::size_of::<Entry>() + 512)?;
    let id = NEXT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| next.checked_add(1)).map_err(|_| super::invalid("Opaque handle identity space is exhausted"))?;
    REGISTRY.lock().insert(id, Entry { value, _reservation: reservation });
    Ok(id)
}
pub(crate) fn get(id: u64) -> Result<Handle> {
    REGISTRY.lock().get(&id).map(|entry| entry.value.clone()).ok_or_else(|| super::invalid("Handle has been released or was never issued"))
}
pub(crate) fn release(id: u64) -> Result<()> {
    if id == 0 || id == 1 { return Ok(()); }
    let removed = REGISTRY.lock().remove(&id).ok_or_else(|| super::invalid("Handle has already been released"))?;
    drop(removed);
    Ok(())
}
pub(crate) fn diagnostic(error: Anomaly) -> u64 {
    let retained = RetainedAnomaly::new(error, &ERROR_MEMORY).and_then(|error| Ok(Diagnostic { wire: error.encode()?, error }));
    retained.and_then(|value| insert(Handle::Error(Arc::new(value)))).unwrap_or(1)
}
pub(crate) fn database(id: u64) -> Result<Arc<DatabaseHandle>> { match get(id)? { Handle::Database(value) => Ok(value), _ => Err(super::invalid("Expected a database handle")) } }
pub(crate) fn session(id: u64) -> Result<Arc<SessionHandle>> { match get(id)? { Handle::Session(value) => Ok(value), _ => Err(super::invalid("Expected a session handle")) } }
pub(crate) fn statement(id: u64) -> Result<Arc<StatementHandle>> { match get(id)? { Handle::Statement(value) => Ok(value), _ => Err(super::invalid("Expected a statement handle")) } }
pub(crate) fn cursor(id: u64) -> Result<Arc<CursorHandle>> { match get(id)? { Handle::Cursor(value) => Ok(value), _ => Err(super::invalid("Expected a cursor handle")) } }
pub(crate) fn cancellation(id: u64, session: &Arc<SessionHandle>) -> Result<CancellationToken> {
    match get(id)? {
        Handle::Cancellation(value) if Arc::ptr_eq(&value.session, session) => Ok(value.token.clone()),
        _ => Err(super::invalid("Operation token belongs to another session")),
    }
}
