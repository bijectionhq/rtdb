use parking_lot::Mutex;
use rtdb_auth::{DeviceAuthorization, Principal, Secret};
use rtdb_columnar::MemoryBudget;
use rtdb_tx::RetainedBytes;
use rtdb_types::Result;
use std::sync::{Arc, Weak};
use tokio_util::sync::CancellationToken;

use crate::{add, call, cancelled, closed, invalid, output, text};
use crate::handles::{self, Handle, NativeBytes};
use crate::region::{EmbeddedRegion, blocking_caller};
use crate::statement::SessionHandle;

pub(crate) struct PrincipalHandle {
    region: Weak<EmbeddedRegion>,
    principal: Arc<Principal>,
    pub(crate) memory: Arc<MemoryBudget>,
}

pub(crate) struct DeviceHandle {
    region: Weak<EmbeddedRegion>,
    pending: Mutex<Option<DeviceAuthorization>>,
    cancellation: CancellationToken,
    pub(crate) memory: Arc<MemoryBudget>,
}

impl DeviceHandle {
    pub(crate) fn close(&self) {
        self.cancellation.cancel();
        drop(self.pending.lock().take());
    }
}

impl Drop for DeviceHandle {
    fn drop(&mut self) { self.close(); }
}

fn retain_principal(region: &Arc<EmbeddedRegion>, principal: Arc<Principal>) -> Result<u64> {
    handles::insert(Handle::Principal(Arc::new(PrincipalHandle {
        region: Arc::downgrade(region), principal, memory: Arc::clone(region.memory()),
    })))
}

fn owned_text(value: &str, memory: &Arc<MemoryBudget>) -> Result<u64> {
    let reservation = memory.reserve(add(value.len(), 256)?)?;
    let value = RetainedBytes::with_reservation(value.as_bytes().to_vec(), reservation)?;
    handles::insert(Handle::Bytes(Arc::new(NativeBytes { value, memory: Arc::clone(memory) })))
}

pub(crate) unsafe fn authenticate(
    region: &EmbeddedRegion, mode: u32, user: *const u8, user_length: usize, secret: *const u8, secret_length: usize,
) -> Result<Arc<Principal>> {
    let authenticator = region.server.authenticator();
    match mode {
        0 => { blocking_caller()?; authenticator.unauthenticated() }
        1 => { blocking_caller()?; authenticator.authenticate_trusted(unsafe { text(user, user_length)? }) }
        2 | 3 => {
            let user = unsafe { text(user, user_length)? };
            let secret = unsafe { text(secret, secret_length)? };
            let _input = region.memory().reserve(add(256, add(user.len(), secret.len())?)?)?;
            let user = user.to_owned();
            let secret = Secret::new(secret);
            let authenticator = Arc::clone(authenticator);
            let cancellation = region.server.node().cancellation().clone();
            region.run(move || async move {
                cancellation.run_until_cancelled(async {
                    if mode == 2 { authenticator.verify_password(&user, secret.expose()).await }
                    else { authenticator.verify_client_credentials(&user, secret.expose()).await }
                }).await.ok_or_else(cancelled)?
            })
        }
        _ => Err(invalid("Unknown embedded authentication mode")),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_authenticate(
    database: u64, mode: u32, user: *const u8, user_length: usize, secret: *const u8, secret_length: usize,
    out: *mut u64, error: *mut u64,
) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?;
        *out = 0;
        if mode == 0 { return Err(invalid("An authentication mode is required for a principal")); }
        let database = handles::database(database)?;
        let _admitted = database.0.enter()?;
        let principal = authenticate(&database.0, mode, user, user_length, secret, secret_length)?;
        *out = retain_principal(&database.0, principal)?;
        Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_principal_user_id(handle: u64, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?;
        *out = 0;
        let Handle::Principal(principal) = handles::get(handle)? else { return Err(invalid("Expected a principal handle")); };
        if let Some(user) = principal.principal.user_id() { *out = owned_text(user, &principal.memory)?; }
        Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_open_as(
    database: u64, name: *const u8, name_length: usize, principal: u64, out: *mut u64, error: *mut u64,
) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?;
        *out = 0;
        blocking_caller()?;
        let database = handles::database(database)?;
        let _admitted = database.0.enter()?;
        let Handle::Principal(principal) = handles::get(principal)? else { return Err(invalid("Expected a principal handle")); };
        if !principal.region.ptr_eq(&Arc::downgrade(&database.0)) {
            return Err(invalid("Principal belongs to another embedded node"));
        }
        let identity = Arc::clone(&principal.principal);
        let session = database.0.server.node().session(text(name, name_length)?, identity)?;
        *out = handles::insert(Handle::Session(SessionHandle::new(Arc::clone(&database.0), session)?))?;
        Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_start_device_auth(
    database: u64, user: *const u8, user_length: usize, out: *mut u64, error: *mut u64,
) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?;
        *out = 0;
        let database = handles::database(database)?;
        let _admitted = database.0.enter()?;
        let user = text(user, user_length)?;
        let _input = database.0.memory().reserve(add(user.len(), 256)?)?;
        let user = user.to_owned();
        let token = database.0.server.node().cancellation().child_token();
        let cancellation = token.clone();
        let authenticator = Arc::clone(database.0.server.authenticator());
        let authorization = database.0.run(move || async move {
            cancellation.run_until_cancelled(authenticator.start_device_auth(&user)).await.ok_or_else(cancelled)?
        })?;
        *out = handles::insert(Handle::Device(Arc::new(DeviceHandle {
            region: Arc::downgrade(&database.0), pending: Mutex::new(Some(authorization)), cancellation: token,
            memory: Arc::clone(database.0.memory()),
        })))?;
        Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_device_auth_url(handle: u64, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?;
        *out = 0;
        let Handle::Device(device) = handles::get(handle)? else { return Err(invalid("Expected a device authorization handle")); };
        let region = device.region.upgrade().ok_or_else(closed)?;
        let _admitted = region.enter()?;
        let pending = device.pending.lock();
        let authorization = pending.as_ref().ok_or_else(closed)?;
        *out = owned_text(authorization.url().as_str(), &device.memory)?;
        Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_device_auth_wait(handle: u64, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?;
        *out = 0;
        blocking_caller()?;
        let Handle::Device(device) = handles::get(handle)? else { return Err(invalid("Expected a device authorization handle")); };
        let region = device.region.upgrade().ok_or_else(closed)?;
        let _admitted = region.enter()?;
        let authorization = device.pending.lock().take().ok_or_else(closed)?;
        let token = device.cancellation.clone();
        drop(device);
        let principal = region.run(move || async move { authorization.await_principal(&token).await })?;
        *out = retain_principal(&region, principal)?;
        Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_user_id(handle: u64, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?;
        *out = 0;
        blocking_caller()?;
        let session = handles::session(handle)?;
        let _admitted = session.region.enter()?;
        session.ensure_open()?;
        let state = session.state.blocking_lock();
        if let Some(user) = state.state().user() { *out = owned_text(user, session.region.memory())?; }
        Ok(())
    }) }
}
