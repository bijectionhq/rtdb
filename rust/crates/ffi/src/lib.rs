//! Versioned blocking C ABI; ownership and thread-safety requirements are in `include/rtdb.h`.
mod arrow;
mod auth;
mod handles;
mod region;
mod statement;
mod metadata;
mod transit;

use std::{panic::{AssertUnwindSafe, catch_unwind}, sync::Arc};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp};
use handles::Handle;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RtdbSlice { pub data: *const u8, pub length: usize }

fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("rtdb.ffi/invalid-argument", message) }
fn closed() -> Anomaly { Anomaly::incorrect("rtdb.ffi/closed", "The native resource is closed") }
fn cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/interrupted", "Operation was cancelled") }
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(|| invalid("Allocation size exceeds usize")) }
fn mul(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(|| invalid("Allocation size exceeds usize")) }

unsafe fn output<'a, T>(pointer: *mut T) -> Result<&'a mut T> { unsafe { pointer.as_mut() }.ok_or_else(|| invalid("Output pointer is null")) }
unsafe fn bytes<'a>(pointer: *const u8, length: usize) -> Result<&'a [u8]> {
    if length == 0 { return Ok(&[]); }
    if pointer.is_null() || length > isize::MAX as usize { return Err(invalid("Input pointer or length is invalid")); }
    Ok(unsafe { std::slice::from_raw_parts(pointer, length) })
}
unsafe fn text<'a>(pointer: *const u8, length: usize) -> Result<&'a str> {
    std::str::from_utf8(unsafe { bytes(pointer, length) }?).map_err(|_| invalid("Text input is not UTF-8"))
}
unsafe fn call(error: *mut u64, work: impl FnOnce() -> Result<()>) -> i32 {
    if error.is_null() { return 1; }
    unsafe { *error = 0; }
    let result = catch_unwind(AssertUnwindSafe(work)).unwrap_or_else(|_| Err(Anomaly::fault("rtdb.ffi/panic", "A native call panicked")));
    match result {
        Ok(()) => 0,
        Err(failure) => { unsafe { *error = catch_unwind(AssertUnwindSafe(|| handles::diagnostic(failure))).unwrap_or(1); } 1 }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rtdb_v1_abi_version() -> u32 { 1 }

fn open_database(out: &mut u64, yaml: &str, memory_bytes: usize, properties: &str, clock: Option<Arc<rtdb_node::TransactionClock>>) -> Result<()> {
    let region = region::EmbeddedRegion::open(yaml, memory_bytes, properties, clock)?;
    *out = handles::insert(Handle::Database(Arc::new(region::DatabaseHandle(region))))?;
    Ok(())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_open(yaml: *const u8, yaml_length: usize, memory_bytes: usize, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?;
        *out = 0;
        open_database(out, text(yaml, yaml_length)?, memory_bytes, "{}", None)
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_open_with_properties(yaml: *const u8, yaml_length: usize, memory_bytes: usize, properties: *const u8, properties_length: usize, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?;
        *out = 0;
        if properties_length > 4096 { return Err(invalid("Native system properties exceed 4096 bytes")); }
        open_database(out, text(yaml, yaml_length)?, memory_bytes, text(properties, properties_length)?, None)
    }) }
}

pub type RtdbInstantSource = unsafe extern "C" fn(context: usize, seconds: *mut i64, nanos: *mut u32) -> i32;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_open_with_log_clock(
    yaml: *const u8, yaml_length: usize, memory_bytes: usize, properties: *const u8, properties_length: usize,
    clock: Option<RtdbInstantSource>, context: usize, out: *mut u64, error: *mut u64,
) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?;
        *out = 0;
        if properties_length > 4096 { return Err(invalid("Native system properties exceed 4096 bytes")); }
        let clock = clock.ok_or_else(|| invalid("The log clock callback is null"))?;
        let clock = Arc::new(move || {
            let mut seconds = 0;
            let mut nanos = 0;
            if clock(context, &mut seconds, &mut nanos) != 0 {
                return Err(Anomaly::fault("rtdb.ffi/clock", "The host log clock failed"));
            }
            Timestamp::from_epoch_parts(seconds, nanos as i32)
        });
        open_database(out, text(yaml, yaml_length)?, memory_bytes, text(properties, properties_length)?, Some(clock))
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_flush_all(handle: u64, timeout_nanos: i64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        if timeout_nanos <= 0 { return Err(invalid("Flush timeout must be positive")); }
        let database = handles::database(handle)?;
        let _admitted = database.0.enter()?;
        let node = database.0.server.node().clone();
        let timeout = std::time::Duration::from_nanos(timeout_nanos as u64);
        database.0.run(move || async move { node.flush_all(timeout).await })
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_sync_all(handle: u64, timeout_nanos: i64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        if timeout_nanos <= 0 { return Err(invalid("Sync timeout must be positive")); }
        let database = handles::database(handle)?;
        let _admitted = database.0.enter()?;
        let node = database.0.server.node().clone();
        let timeout = std::time::Duration::from_nanos(timeout_nanos as u64);
        database.0.run(move || async move { node.sync_all(timeout, node.cancellation()).await })
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_compact(
    handle: u64, name: *const u8, name_length: usize, timeout_nanos: i64, error: *mut u64,
) -> i32 {
    unsafe { call(error, || {
        if timeout_nanos <= 0 { return Err(invalid("Compaction timeout must be positive")); }
        let database = handles::database(handle)?;
        let _admitted = database.0.enter()?;
        let node = database.0.server.node().clone();
        let target = node.catalog().database(text(name, name_length)?)?;
        let timeout = std::time::Duration::from_nanos(timeout_nanos as u64);
        database.0.run(move || async move {
            let compaction = target.compaction().ok_or_else(|| invalid("Compaction is disabled for this database"))?;
            tokio::time::timeout(timeout, node.cancellation().run_until_cancelled(compaction.compact_all()))
                .await.map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.ffi/compaction-timeout", "Timed out awaiting compaction"))?
                .ok_or_else(closed)?
        })
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_retain(handle: u64, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || { let out = output(out)?; *out = 0; *out = if handle == 1 { 1 } else { handles::insert(handles::get(handle)?)? }; Ok(()) }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_release(handle: u64, error: *mut u64) -> i32 { unsafe { call(error, || handles::release(handle)) } }
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_close(handle: u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        match handles::get(handle)? {
            Handle::Database(value) => value.0.close()?, Handle::Session(value) => value.close(), Handle::Statement(value) => value.close(),
            Handle::Cursor(value) => value.close(), Handle::Cancellation(value) => value.token.cancel(), Handle::Device(value) => value.close(),
            Handle::Error(_) | Handle::Bytes(_) | Handle::Principal(_) => return Err(invalid("Immutable values are released, not closed")),
        } Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_error_field(handle: u64, field: u32, out: *mut RtdbSlice, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?;
        let view = if handle == 1 {
            match field { 0 => b"unavailable".as_slice(), 1 => b"rtdb.ffi/diagnostic-budget".as_slice(), 2 => b"The bounded diagnostic budget is exhausted".as_slice(), 3 => b"".as_slice(), _ => return Err(invalid("Unknown diagnostic field")) }
        } else {
            let Handle::Error(diagnostic) = handles::get(handle)? else { return Err(invalid("Expected an error handle")); };
            let value = match field {
                0 => match diagnostic.error.category { AnomalyCategory::Incorrect => "incorrect", AnomalyCategory::Unsupported => "unsupported", AnomalyCategory::NotFound => "not-found", AnomalyCategory::Forbidden => "forbidden", AnomalyCategory::Conflict => "conflict", AnomalyCategory::Busy => "busy", AnomalyCategory::Unavailable => "unavailable", AnomalyCategory::Interrupted => "interrupted", AnomalyCategory::Fault => "fault" }.as_bytes(),
                1 => diagnostic.error.code.as_bytes(),
                2 => match &diagnostic.error.message {
                    Some(message) => message.as_bytes(),
                    None => { *out = RtdbSlice { data: std::ptr::null(), length: 0 }; return Ok(()); }
                },
                3 => diagnostic.wire.as_ref(),
                _ => return Err(invalid("Unknown diagnostic field")),
            };
            *out = RtdbSlice { data: value.as_ptr(), length: value.len() }; return Ok(());
        };
        *out = RtdbSlice { data: view.as_ptr(), length: view.len() }; Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_open(database: u64, name: *const u8, name_length: usize, authentication: u32, user: *const u8, user_length: usize, secret: *const u8, secret_length: usize, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?; *out = 0;
        let database = handles::database(database)?;
        let _admitted = database.0.enter()?;
        let principal = auth::authenticate(&database.0, authentication, user, user_length, secret, secret_length)?;
        let session = database.0.server.node().session(text(name, name_length)?, principal)?;
        *out = handles::insert(Handle::Session(statement::SessionHandle::new(Arc::clone(&database.0), session)?))?; Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_operation_new(session: u64, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?; *out = 0;
        let session = handles::session(session)?; let _admitted = session.region.enter()?;
        if session.cancellation.is_cancelled() { return Err(closed()); }
        let reservation = session.region.memory().reserve(std::mem::size_of::<handles::OperationToken>() + 256)?;
        let token = session.cancellation.child_token();
        *out = handles::insert(Handle::Cancellation(Arc::new(handles::OperationToken { token, session: Arc::clone(&session), _reservation: reservation })))?; Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_cancel(handle: u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        match handles::get(handle)? { Handle::Cancellation(value) => value.token.cancel(), Handle::Cursor(value) => value.token.cancel(), _ => return Err(invalid("Expected an operation or cursor handle")) } Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_action(handle: u64, operation: u64, action: u32, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let session = handles::session(handle)?; let _admitted = session.region.enter()?;
        let token = handles::cancellation(operation, &session)?;
        let session_handle = Arc::clone(&session);
        session.region.run(move || async move {
            let mut state = session_handle.state.lock().await;
            token.run_until_cancelled(async {
                state.state().revalidate().await?;
                match action {
                    0 => state.state_mut().set_auto_commit(false).await?, 1 => state.state_mut().set_auto_commit(true).await?,
                    2 => state.state_mut().begin()?,
                    3 => { state.state_mut().commit().await?; }, 4 => state.state_mut().rollback()?,
                    5 | 6 => { let result = state.state_mut().commit_tx(Some(if action == 5 { rtdb_node::CommitMode::Sync } else { rtdb_node::CommitMode::Async })).await?; if let rtdb_node::CommitResult::Executed(result) = result { if let Some(error) = result.error() { return Err(error.clone()); } } },
                    7 => state.state_mut().rollback_tx()?, _ => return Err(invalid("Unknown session action")),
                } Ok(())
            }).await.ok_or_else(cancelled)?
        })
    }) }
}

#[repr(C)]
#[derive(Default)]
pub struct RtdbCommitResult {
    pub tx_id: i64,
    pub system_time_micros: i64,
    pub diagnostic: u64,
    pub kind: u32,
    pub reserved: u32,
}

impl From<rtdb_node::CommitResult> for RtdbCommitResult {
    fn from(result: rtdb_node::CommitResult) -> Self {
        use rtdb_node::CommitResult;

        match result {
            CommitResult::NoTransaction => Self::default(),
            CommitResult::Submitted(tx) => Self { tx_id: tx.tx_id.0, kind: 1, ..Self::default() },
            CommitResult::Executed(tx) => {
                let key = tx.tx_key();
                Self {
                    tx_id: key.tx_id.0,
                    system_time_micros: key.system_time.0,
                    diagnostic: tx.error().map_or(0, |error| handles::diagnostic(error.clone())),
                    kind: if tx.committed() { 2 } else { 3 },
                    reserved: 0,
                }
            }
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_commit_result(
    handle: u64, operation: u64, asynchronous: u32, out: *mut RtdbCommitResult, error: *mut u64,
) -> i32 {
    use rtdb_node::CommitMode;

    unsafe { call(error, || {
        let out = output(out)?;
        *out = RtdbCommitResult::default();
        let mode = match asynchronous {
            0 => CommitMode::Sync,
            1 => CommitMode::Async,
            _ => return Err(invalid("Commit mode must be 0 (sync) or 1 (async)")),
        };
        let session = handles::session(handle)?;
        let _admitted = session.region.enter()?;
        let token = handles::cancellation(operation, &session)?;
        let session_handle = Arc::clone(&session);
        let result = session.region.run(move || async move {
            let mut state = session_handle.state.lock().await;
            token.run_until_cancelled(state.state_mut().commit_tx(Some(mode))).await.ok_or_else(cancelled)?
        })?;
        *out = result.into();
        Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_set_catalog(handle: u64, name: *const u8, length: usize, error: *mut u64) -> i32 {
    unsafe { call(error, || { let session = handles::session(handle)?; let _admitted = session.region.enter()?; session.state.blocking_lock().state_mut().set_database(text(name, length)?)?; Ok(()) }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_set_application_name(handle: u64, name: *const u8, length: usize, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let session = handles::session(handle)?;
        let _admitted = session.region.enter()?;
        session.state.blocking_lock().state_mut().set_application_name(text(name, length)?)?;
        Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_await_token(handle: u64, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?; *out = 0;
        let session = handles::session(handle)?; let _admitted = session.region.enter()?; session.ensure_open()?;
        let state = session.state.blocking_lock();
        if let Some(token) = state.state().effective_await_token() {
            let memory = session.region.memory(); let reservation = memory.reserve(add(token.len(), 256)?)?;
            let value = rtdb_tx::RetainedBytes::with_reservation(token.as_bytes().to_vec(), reservation)?;
            *out = handles::insert(Handle::Bytes(Arc::new(handles::NativeBytes { value, memory: Arc::clone(memory) })))?;
        } Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_set_await_token(handle: u64, operation: u64, token: *const u8, length: usize, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let session = handles::session(handle)?;
        let _admitted = session.region.enter()?;
        let cancellation = handles::cancellation(operation, &session)?;
        let token = optional_text(token, length)?;
        let _input = session.region.memory().reserve(add(4096, token.map_or(0, str::len))?)?;
        let token = token.map(str::to_owned);
        let session_handle = Arc::clone(&session);
        session.region.run(move || async move {
            let mut state = session_handle.state.lock().await;
            let _control = state.admit_frontend_prepare(&[], &cancellation).await?;
            state.state_mut().set_await_token(token)
        })
    }) }
}

#[repr(C)]
pub struct RtdbReadTime { pub text: RtdbSlice, pub seconds: i64, pub nanos: u32, pub kind: u32 }

unsafe fn optional_text<'a>(data: *const u8, length: usize) -> Result<Option<&'a str>> {
    if data.is_null() && length == 0 { Ok(None) } else { unsafe { text(data, length).map(Some) } }
}

enum ReadTime<'a> {
    Text(&'a str),
    Instant(rtdb_types::Instant),
    TemporalText { text: &'a str, oid: u32 },
}

impl<'a> ReadTime<'a> {
    unsafe fn parse(input: *const RtdbReadTime) -> Result<Option<Self>> {
        let Some(input) = (unsafe { input.as_ref() }) else { return Ok(None); };
        Ok(Some(match input.kind {
            1 => Self::Text(unsafe { text(input.text.data, input.text.length) }?),
            2 => Self::Instant(rtdb_types::Instant::new(input.seconds, input.nanos)?),
            3..=5 => Self::TemporalText {
                text: unsafe { text(input.text.data, input.text.length) }?,
                oid: match input.kind { 3 => 1082, 4 => 1114, _ => 1184 },
            },
            _ => return Err(invalid("Unknown read time kind")),
        }))
    }

    fn text_len(&self) -> usize {
        match self { Self::Text(value) | Self::TemporalText { text: value, .. } => value.len(), Self::Instant(_) => 0 }
    }

    fn into_owned(self) -> OwnedReadTime {
        match self {
            Self::Text(value) => OwnedReadTime::Text(value.to_owned()),
            Self::Instant(instant) => OwnedReadTime::Instant(instant),
            Self::TemporalText { text, oid } => OwnedReadTime::TemporalText { text: text.to_owned(), oid },
        }
    }
}

enum OwnedReadTime {
    Text(String),
    Instant(rtdb_types::Instant),
    TemporalText { text: String, oid: u32 },
}

impl OwnedReadTime {
    fn snapshot(&self, timezone: &str) -> Result<rtdb_types::Timestamp> {
        match self {
            Self::Text(value) => rtdb_expr::parse_timestamp(value, timezone),
            Self::Instant(instant) => instant.as_micros(),
            Self::TemporalText { text, oid } => rtdb_expr::timestamp_micros(&Self::temporal_value(*oid, text)?, timezone),
        }
    }
    fn clock(&self, timezone: &str) -> Result<rtdb_types::Instant> {
        match self {
            Self::Text(value) => rtdb_types::parse_query_clock(value, timezone),
            Self::Instant(instant) => Ok(*instant),
            Self::TemporalText { text, oid } => rtdb_expr::query_clock_instant(&Self::temporal_value(*oid, text)?, timezone),
        }
    }
    fn temporal_value(oid: u32, text: &str) -> Result<rtdb_types::Value> {
        let mut value = rtdb_pg_codec::read_text(oid, text)?;
        rtdb_columnar::admit_execution_value(&mut value)?;
        Ok(value)
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_set_read_settings(handle: u64, operation: u64, timezone: *const u8, timezone_length: usize,
    snapshot: *const u8, snapshot_length: usize, snapshot_time: *const RtdbReadTime, clock_time: *const RtdbReadTime, error: *mut u64,
) -> i32 {
    unsafe { change_read_settings(handle, operation, timezone, timezone_length, snapshot, snapshot_length,
        snapshot_time, clock_time, std::ptr::null(), 0, false, error) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_begin_read_only(handle: u64, operation: u64, timezone: *const u8, timezone_length: usize,
    snapshot: *const u8, snapshot_length: usize, snapshot_time: *const RtdbReadTime, clock_time: *const RtdbReadTime, error: *mut u64,
) -> i32 {
    unsafe { change_read_settings(handle, operation, timezone, timezone_length, snapshot, snapshot_length,
        snapshot_time, clock_time, std::ptr::null(), 0, true, error) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_set_transaction_read_only(
    handle: u64, operation: u64, snapshot: *const u8, snapshot_length: usize, snapshot_time: *const RtdbReadTime,
    clock_time: *const RtdbReadTime, await_token: *const u8, await_length: usize, error: *mut u64,
) -> i32 {
    unsafe { change_read_settings(handle, operation, std::ptr::null(), 0, snapshot, snapshot_length,
        snapshot_time, clock_time, await_token, await_length, false, error) }
}

unsafe fn change_read_settings(handle: u64, operation: u64, timezone: *const u8, timezone_length: usize,
    snapshot: *const u8, snapshot_length: usize, snapshot_time: *const RtdbReadTime, clock_time: *const RtdbReadTime,
    await_token: *const u8, await_length: usize, begin: bool, error: *mut u64,
) -> i32 {
    use rtdb_node::{AccessSettings, OptionUpdate, ReadOnlySettings, TransactionSettings};
    unsafe { call(error, || {
        let session = handles::session(handle)?;
        let _admitted = session.region.enter()?;
        let cancellation = handles::cancellation(operation, &session)?;
        let timezone = optional_text(timezone, timezone_length)?;
        let snapshot = optional_text(snapshot, snapshot_length)?;
        let await_token = optional_text(await_token, await_length)?;
        let snapshot_time = ReadTime::parse(snapshot_time)?;
        let clock_time = ReadTime::parse(clock_time)?;
        let bytes = [timezone.map_or(0, str::len), snapshot.map_or(0, str::len), await_token.map_or(0, str::len),
            snapshot_time.as_ref().map_or(0, ReadTime::text_len), clock_time.as_ref().map_or(0, ReadTime::text_len)]
            .into_iter().try_fold(0, add)?;
        let _input = session.region.memory().reserve(add(4096, mul(bytes, 4)?)?)?;
        let timezone = timezone.map(str::to_owned);
        let snapshot = snapshot.map(str::to_owned);
        let await_token = await_token.map(str::to_owned);
        let snapshot_time = snapshot_time.map(ReadTime::into_owned);
        let clock_time = clock_time.map(ReadTime::into_owned);
        let session_handle = Arc::clone(&session);
        session.region.run(move || async move {
            let mut state = session_handle.state.lock().await;
            let _control = state.admit_frontend_prepare(&[], &cancellation).await?;
            let effective_timezone = match timezone.as_deref() {
                Some(zone) => rtdb_expr::normalize_timezone(zone)?,
                None => state.state().effective_timezone().to_owned(),
            };
            let settings = ReadOnlySettings {
                snapshot_token: snapshot.map_or(OptionUpdate::Unchanged, OptionUpdate::Set),
                snapshot_time: snapshot_time.map(|time| time.snapshot(&effective_timezone)).transpose()?.map_or(OptionUpdate::Unchanged, OptionUpdate::Set),
                clock_time: clock_time.map(|time| time.clock(&effective_timezone)).transpose()?.map_or(OptionUpdate::Unchanged, OptionUpdate::Set),
                await_token: await_token.map_or(OptionUpdate::Unchanged, OptionUpdate::Set),
            };
            let settings = TransactionSettings {
                access: Some(AccessSettings::ReadOnly(settings)),
                timezone: timezone.map_or(OptionUpdate::Unchanged, |_| OptionUpdate::Set(effective_timezone)),
            };
            if begin { state.state_mut().begin_with(settings) }
            else { state.state_mut().set_transaction(settings) }
        })
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_begin_write_only(
    handle: u64, operation: u64, timezone: *const u8, timezone_length: usize, system_time: *const RtdbReadTime,
    metadata: *const u8, metadata_length: usize, asynchronous: u32, error: *mut u64,
) -> i32 {
    unsafe { change_write_settings(handle, operation, timezone, timezone_length, system_time,
        metadata, metadata_length, asynchronous, true, error) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_set_transaction_read_write(
    handle: u64, operation: u64, system_time: *const RtdbReadTime, metadata: *const u8, metadata_length: usize,
    asynchronous: u32, error: *mut u64,
) -> i32 {
    unsafe { change_write_settings(handle, operation, std::ptr::null(), 0, system_time,
        metadata, metadata_length, asynchronous, false, error) }
}

unsafe fn change_write_settings(
    handle: u64, operation: u64, timezone: *const u8, timezone_length: usize, system_time: *const RtdbReadTime,
    metadata: *const u8, metadata_length: usize, asynchronous: u32, begin: bool, error: *mut u64,
) -> i32 {
    use rtdb_node::{AccessSettings, OptionUpdate, ReadWriteSettings, TransactionSettings};

    unsafe { call(error, || {
        let asynchronous = match asynchronous {
            0 => OptionUpdate::Set(false),
            1 => OptionUpdate::Set(true),
            2 if !begin => OptionUpdate::Unchanged,
            _ => return Err(invalid("Unknown asynchronous transaction setting")),
        };
        let session = handles::session(handle)?;
        let _admitted = session.region.enter()?;
        let token = handles::cancellation(operation, &session)?;
        let timezone = optional_text(timezone, timezone_length)?;
        let system_time = ReadTime::parse(system_time)?;
        let text_bytes = add(timezone.map_or(0, str::len), system_time.as_ref().map_or(0, ReadTime::text_len))?;
        let _input = session.region.memory().reserve(add(4096, mul(text_bytes, 4)?)?)?;
        let timezone = timezone.map(str::to_owned);
        let system_time = system_time.map(ReadTime::into_owned);
        let metadata = if metadata.is_null() && metadata_length == 0 { None } else {
            let (value, _decoded) = rtdb_pg_codec::decode_transit_msgpack(bytes(metadata, metadata_length)?, session.region.memory())?.into_parts();
            Some(rtdb_tx::UserMetadata::new(value, session.region.memory())?)
        };
        let session_handle = Arc::clone(&session);
        session.region.run(move || async move {
            let mut state = session_handle.state.lock().await;
            token.run_until_cancelled(async {
                if begin { state.state().revalidate().await?; }
                else { state.admit_frontend_prepare(&[], &token).await?; }
                let effective_timezone = match &timezone {
                    Some(zone) => rtdb_expr::normalize_timezone(zone)?,
                    None => state.state().effective_timezone().to_owned(),
                };
                let system_time = system_time.map(|time| time.snapshot(&effective_timezone)).transpose()?;
                let settings = TransactionSettings {
                    access: Some(AccessSettings::ReadWrite(ReadWriteSettings {
                        system_time: system_time.map_or(OptionUpdate::Unchanged, OptionUpdate::Set),
                        user_metadata: metadata.map_or(OptionUpdate::Unchanged, OptionUpdate::Set),
                        asynchronous,
                    })),
                    timezone: timezone.map_or(OptionUpdate::Unchanged, |_| OptionUpdate::Set(effective_timezone)),
                };
                if begin { state.state_mut().begin_with(settings) }
                else { state.state_mut().set_transaction(settings) }
            }).await.ok_or_else(cancelled)?
        })
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_set_transaction_timezone(
    handle: u64, operation: u64, timezone: *const u8, length: usize, error: *mut u64,
) -> i32 {
    use rtdb_node::{OptionUpdate, TransactionSettings};

    unsafe { call(error, || {
        let session = handles::session(handle)?;
        let _admitted = session.region.enter()?;
        let token = handles::cancellation(operation, &session)?;
        let timezone = text(timezone, length)?;
        let _input = session.region.memory().reserve(add(4096, mul(length, 4)?)?)?;
        let timezone = timezone.to_owned();
        let session_handle = Arc::clone(&session);
        session.region.run(move || async move {
            let mut state = session_handle.state.lock().await;
            let _control = state.admit_frontend_prepare(&[], &token).await?;
            state.state_mut().set_transaction(TransactionSettings { access: None, timezone: OptionUpdate::Set(timezone) })
        })
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_statement_new(session: u64, source: *const u8, length: usize, bulk: u32, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?; *out = 0;
        if bulk > 1 { return Err(invalid("Only CREATE_APPEND bulk ingest is supported")); }
        let session = handles::session(session)?; let _admitted = session.region.enter()?;
        let statement = statement::StatementHandle::new(Arc::clone(&session), text(source, length)?, bulk == 1)?;
        *out = handles::insert(Handle::Statement(statement))?; Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_statement_set_sql(handle: u64, source: *const u8, length: usize, error: *mut u64) -> i32 {
    unsafe { call(error, || { let statement = handles::statement(handle)?; let _admitted = statement.session.region.enter()?; statement.replace_source(text(source, length)?, false) }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_statement_prepare(handle: u64, operation: u64, error: *mut u64) -> i32 {
    unsafe { call(error, || { let statement = handles::statement(handle)?; let _admitted = statement.session.region.enter()?; statement.prepare(&handles::cancellation(operation, &statement.session)?) }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_statement_bind(handle: u64, array: *mut FFI_ArrowArray, schema: *mut FFI_ArrowSchema, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let statement = handles::statement(handle)?; let _admitted = statement.session.region.enter()?;
        let rows = arrow::import_arguments(output(array)?, output(schema)?, statement.session.region.memory())?;
        statement.bind(rows)
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_statement_bind_retained(handle: u64, array: *mut FFI_ArrowArray, schema: *mut FFI_ArrowSchema, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let statement = handles::statement(handle)?;
        let _admitted = statement.session.region.enter()?;
        let arguments = arrow::import_retained_arguments(output(array)?, output(schema)?, statement.session.region.memory())?;
        statement.bind(arguments)
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_statement_bind_transit(handle: u64, input: *const u8, length: usize, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let statement = handles::statement(handle)?; let _admitted = statement.session.region.enter()?;
        let memory = statement.session.region.memory();
        let arguments = transit::bind_arguments(bytes(input, length)?, memory)?;
        let reservation = memory.reserve(add(mul(arguments.types().len(), 64)?, 4096)?)?;
        let names = (0..arguments.types().len()).map(|index| format!("${}", index + 1)).collect();
        statement.bind(arrow::BoundArguments { arguments, names, _reservation: reservation })
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_statement_schema(handle: u64, operation: u64, parameters: u32, out: *mut FFI_ArrowSchema, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?;
        if out.release.is_some() { return Err(invalid("Output Arrow schema is still owned")); }
        let statement = handles::statement(handle)?; let _admitted = statement.session.region.enter()?;
        let schema = statement.schema(parameters != 0, &handles::cancellation(operation, &statement.session)?)?;
        *out = arrow::export_schema(&schema.schema, statement.session.region.memory())?; Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_statement_schema_with_parameters(
    handle: u64, operation: u64, parameters: *const FFI_ArrowSchema, out: *mut FFI_ArrowSchema, error: *mut u64,
) -> i32 {
    unsafe { call(error, || {
        if std::ptr::eq(parameters, out) { return Err(invalid("Parameter and output schemas must be distinct")); }
        let out = output(out)?;
        if out.release.is_some() { return Err(invalid("Output Arrow schema is still owned")); }
        let parameters = parameters.as_ref().ok_or_else(|| invalid("Parameter Arrow schema is null"))?;
        let statement = handles::statement(handle)?;
        let _admitted = statement.session.region.enter()?;
        let token = handles::cancellation(operation, &statement.session)?;
        let memory = statement.session.region.memory();
        let types = arrow::read_parameter_types(parameters, memory)?;
        let schema = statement.schema_with_types(false, Some(types), &token)?;
        *out = arrow::export_schema(&schema.schema, memory)?;
        Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_statement_query(handle: u64, operation: u64, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?; *out = 0;
        let statement = handles::statement(handle)?; let _admitted = statement.session.region.enter()?;
        *out = handles::insert(Handle::Cursor(statement.query(&handles::cancellation(operation, &statement.session)?)?))?; Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_query_ra(session: u64, operation: u64, plan: *const u8, plan_length: usize, arguments: *const u8, arguments_length: usize, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?; *out = 0;
        let session = handles::session(session)?;
        let _admitted = session.region.enter()?;
        let operation = handles::cancellation(operation, &session)?;
        let memory = session.region.memory();
        let source = text(plan, plan_length)?;
        let _token_storage = memory.reserve(add(4096, source.len())?)?;
        let token = operation.child_token();
        let source = source.to_owned();
        let (arguments, _arguments) = rtdb_pg_codec::decode_transit_msgpack(bytes(arguments, arguments_length)?, memory)?.into_parts();
        let rtdb_types::Value::Struct(arguments) = arguments else { return Err(invalid("Logical relation arguments require a Transit map")); };
        let session_handle = Arc::clone(&session);
        let query_token = token.clone();
        let cursor = session.region.run(move || async move {
            session_handle.state.lock().await.query_ra(&source, arguments, &query_token).await
        })?;
        *out = handles::insert(Handle::Cursor(statement::CursorHandle::new(Arc::clone(&session.region), cursor, token)?))?;
        Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_query_ra_arrow(session: u64, operation: u64, plan: *const u8, plan_length: usize,
    array: *mut FFI_ArrowArray, schema: *mut FFI_ArrowSchema, out: *mut u64, error: *mut u64,
) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?; *out = 0;
        let session = handles::session(session)?;
        let _admitted = session.region.enter()?;
        let operation = handles::cancellation(operation, &session)?;
        let memory = session.region.memory();
        let source = text(plan, plan_length)?;
        let _token_storage = memory.reserve(add(4096, source.len())?)?;
        let token = operation.child_token();
        let source = source.to_owned();
        let bound = arrow::import_retained_arguments(output(array)?, output(schema)?, memory)?;
        let session_handle = Arc::clone(&session);
        let query_token = token.clone();
        let (cursor, _bound) = session.region.run(move || async move {
            let cursor = session_handle.state.lock().await.query_ra_arrow(&source, &bound.names, &bound.arguments, &query_token).await?;
            Ok((cursor, bound))
        })?;
        *out = handles::insert(Handle::Cursor(statement::CursorHandle::new(Arc::clone(&session.region), cursor, token)?))?;
        Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_statement_update(handle: u64, operation: u64, affected: *mut i64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(affected)?; *out = -1;
        let statement = handles::statement(handle)?; let _admitted = statement.session.region.enter()?;
        *out = statement.update(&handles::cancellation(operation, &statement.session)?)?; Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_cursor_schema(handle: u64, out: *mut FFI_ArrowSchema, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?; if out.release.is_some() { return Err(invalid("Output Arrow schema is still owned")); }
        let cursor = handles::cursor(handle)?; let _admitted = cursor.region.enter()?;
        *out = arrow::export_schema(cursor.schema(), cursor.region.memory())?; Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_cursor_next(handle: u64, out_array: *mut FFI_ArrowArray, out_schema: *mut FFI_ArrowSchema, has_batch: *mut u32, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let has_batch = output(has_batch)?; *has_batch = 0;
        let out_array = output(out_array)?; let out_schema = output(out_schema)?;
        if out_array.release.is_some() || out_schema.release.is_some() { return Err(invalid("Output Arrow structs are still owned")); }
        let cursor = handles::cursor(handle)?; let _admitted = cursor.region.enter()?;
        if let Some(batch) = cursor.next()? {
            let (array, schema) = arrow::export_declared_batch(batch, cursor.region.memory()).inspect_err(|_| cursor.consumer_error())?;
            *out_array = array; *out_schema = schema; *has_batch = 1;
        } Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_cursor_next_array(handle: u64, out_array: *mut FFI_ArrowArray, has_batch: *mut u32, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let has_batch = output(has_batch)?; *has_batch = 0;
        let out_array = output(out_array)?;
        if out_array.release.is_some() { return Err(invalid("Output Arrow structs are still owned")); }
        let cursor = handles::cursor(handle)?; let _admitted = cursor.region.enter()?;
        if let Some(batch) = cursor.next()? {
            *out_array = arrow::export_declared_array(batch, cursor.region.memory()).inspect_err(|_| cursor.consumer_error())?;
            *has_batch = 1;
        }
        Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_cursor_consumer_error(handle: u64, error: *mut u64) -> i32 { unsafe { call(error, || { handles::cursor(handle)?.consumer_error(); Ok(()) }) } }

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_cursor_next_transit(handle: u64, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?; *out = 0;
        if let Some((handle, _)) = next_row_bytes(handle)? { *out = handle; }
        Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_cursor_next_row_bytes(handle: u64, view: *mut RtdbSlice, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?; *out = 0;
        let view = output(view)?; *view = RtdbSlice { data: std::ptr::null(), length: 0 };
        if let Some((handle, slice)) = next_row_bytes(handle)? { *out = handle; *view = slice; }
        Ok(())
    }) }
}

fn next_row_bytes(handle: u64) -> Result<Option<(u64, RtdbSlice)>> {
    let cursor = handles::cursor(handle)?;
    let _admitted = cursor.region.enter()?;
    let Some(batch) = cursor.next()? else { return Ok(None); };
    (|| {
        let memory = cursor.region.memory();
        let batch = batch.batch();
        let bound = (0..batch.num_rows()).try_fold(8192usize, |bound, row| {
            batch.schema_ref().fields().iter().zip(batch.columns()).try_fold(add(bound, 1024)?, |bound, (field, column)| {
                let cell = rtdb_columnar::ArrowCell { field, array: column.as_ref(), row };
                add(bound, add(mul(cell.payload_bytes()?, 32)?, add(mul(field.name().len(), 4)?, 2048)?)?)
            })
        })?;
        let mut reservation = memory.reserve(bound)?;
        let mut bytes = Vec::new();
        rtdb_format::transit::write_arrow_batch_json(&mut bytes, batch, memory)?;
        if bytes.capacity() > reservation.bytes() { return Err(invalid("Transit result exceeded its allocation bound")); }
        Arc::get_mut(&mut reservation).expect("cursor encoding owns its reservation").shrink_to(bytes.capacity())?;
        let value = rtdb_tx::RetainedBytes::with_reservation(bytes, reservation)?;
        let slice = RtdbSlice { data: value.as_ref().as_ptr(), length: value.as_ref().len() };
        let handle = handles::insert(Handle::Bytes(Arc::new(handles::NativeBytes { value, memory: Arc::clone(memory) })))?;
        Ok(Some((handle, slice)))
    })().inspect_err(|_| cursor.consumer_error())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_bytes(handle: u64, out: *mut RtdbSlice, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let Handle::Bytes(bytes) = handles::get(handle)? else { return Err(invalid("Expected a byte value handle")); };
        let bytes = bytes.value.as_ref(); *output(out)? = RtdbSlice { data: bytes.as_ptr(), length: bytes.len() }; Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_metadata(session: u64, operation: u64, request: *const u8, length: usize, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?; *out = 0;
        let session = handles::session(session)?; let _admitted = session.region.enter()?;
        let token = handles::cancellation(operation, &session)?;
        let _request = session.region.memory().reserve(add(mul(length, 64)?, 8192)?)?;
        let request = serde_json::from_slice(bytes(request, length)?).map_err(|failure| invalid(failure.to_string()))?;
        *out = handles::insert(Handle::Cursor(metadata::open(&session, token, request)?))?; Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_memory_used(database: u64, out: *mut usize, error: *mut u64) -> i32 {
    unsafe { call(error, || { *output(out)? = handles::database(database)?.0.memory().used_bytes(); Ok(()) }) }
}
/// Writes the bound PostgreSQL wire port, or -1 when the configuration starts no pgwire listener.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_pgwire_port(database: u64, out: *mut i32, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let port = handles::database(database)?.0.server.addresses().pgwire.map_or(-1, |address| i32::from(address.port()));
        *output(out)? = port;
        Ok(())
    }) }
}
/// Writes the host a client reaches the pgwire listener on, as UTF-8 into `out`, and its length into `length`.
/// A wildcard bind is reachable on loopback, so it reports `localhost`; no listener reports a length of 0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_pgwire_client_host(database: u64, out: *mut u8, capacity: usize, length: *mut usize, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let address = handles::database(database)?.0.server.addresses().pgwire;
        write_client_host(address, out, capacity, length)
    }) }
}

unsafe fn write_client_host(address: Option<std::net::SocketAddr>, out: *mut u8, capacity: usize, length: *mut usize) -> Result<()> {
    let length = unsafe { output(length)? };
    let host = address
        .map(|address| if address.ip().is_unspecified() { "localhost".to_owned() } else { address.ip().to_string() })
        .unwrap_or_default();
    if host.len() > capacity { return Err(Anomaly::incorrect("rtdb.ffi/buffer-too-small", "Host buffer is too small")); }
    if !host.is_empty() {
        if out.is_null() { return Err(invalid("Host buffer is null")); }
        unsafe { std::ptr::copy_nonoverlapping(host.as_ptr(), out, host.len()) };
    }
    *length = host.len();
    Ok(())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_read_only_pgwire_client_host(database: u64, out: *mut u8, capacity: usize, length: *mut usize, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let database = handles::database(database)?;
        let _admitted = database.0.enter()?;
        write_client_host(database.0.server.addresses().read_only_pgwire, out, capacity, length)
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_read_only_pgwire_port(database: u64, out: *mut i32, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let database = handles::database(database)?;
        let _admitted = database.0.enter()?;
        *output(out)? = database.0.server.addresses().read_only_pgwire.map_or(-1, |address| i32::from(address.port()));
        Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_healthz_port(database: u64, out: *mut i32, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let database = handles::database(database)?;
        let _admitted = database.0.enter()?;
        *output(out)? = database.0.server.addresses().health.map_or(-1, |address| i32::from(address.port()));
        Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_catalog_status(database: u64, out: *mut u64, error: *mut u64) -> i32 {
    use rtdb_format::transit::{TransitValue as T, encode_json};

    unsafe { call(error, || {
        let out = output(out)?;
        *out = 0;
        let database = handles::database(database)?;
        let _admitted = database.0.enter()?;
        let memory = database.0.memory();
        let snapshot = database.0.server.node().catalog().open_database_map();
        let bound = snapshot.keys().try_fold(4096, |size, name| add(size, add(1024, mul(16, name.len())?)?))?;
        let reservation = memory.reserve(bound)?;
        let status = T::Map(snapshot.iter().map(|(name, database)| {
            let key = database.progress().current().tx_key.map_or(T::Null, |key| {
                T::Array(vec![T::Integer(key.tx_id.0), T::Integer(key.system_time.0)])
            });
            (T::String(name.clone()), T::Array(vec![key]))
        }).collect());
        let value = rtdb_tx::RetainedBytes::with_reservation(encode_json(&status)?, reservation)?;
        *out = handles::insert(Handle::Bytes(Arc::new(handles::NativeBytes { value, memory: Arc::clone(memory) })))?;
        Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_auth_method(
    database: u64, user: *const u8, user_length: usize, remote: *const u8, remote_length: usize,
    out: *mut u32, error: *mut u64,
) -> i32 {
    use rtdb_auth::Method;

    unsafe { call(error, || {
        let database = handles::database(database)?;
        let _admitted = database.0.enter()?;
        let method = database.0.server.authenticator().method_for(optional_text(user, user_length)?, optional_text(remote, remote_length)?);
        *output(out)? = match method {
            None => 0,
            Some(Method::Trust) => 1,
            Some(Method::Password) => 2,
            Some(Method::ClientCredentials) => 3,
            Some(Method::DeviceAuth) => 4,
        };
        Ok(())
    }) }
}
/// Writes the bound Flight SQL port, or -1 when the configuration starts no Flight SQL listener.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_flight_sql_port(database: u64, out: *mut i32, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let port = handles::database(database)?.0.server.addresses().flight_sql.map_or(-1, |address| i32::from(address.port()));
        *output(out)? = port;
        Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_transact(session: u64, operation: u64, request: *const u8, length: usize, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?; *out = 0;
        let session = handles::session(session)?; let _admitted = session.region.enter()?;
        let token = handles::cancellation(operation, &session)?;
        let memory = session.region.memory();
        let request = transit::TransactionInput::decode(bytes(request, length)?, memory)?;
        let value = transit::transaction_key(transit::transaction(&session, &token, request)?)?;
        let reservation = memory.reserve(add(mul(value.retained_bytes()?, 32)?, 8192)?)?;
        let value = rtdb_tx::RetainedBytes::with_reservation(transit::encode(&value)?, reservation)?;
        *out = handles::insert(Handle::Bytes(Arc::new(handles::NativeBytes { value, memory: Arc::clone(memory) })))?; Ok(())
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_transact_result(
    session: u64, operation: u64, request: *const u8, length: usize, out: *mut RtdbCommitResult, error: *mut u64,
) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?;
        *out = RtdbCommitResult::default();
        let session = handles::session(session)?;
        let _admitted = session.region.enter()?;
        let token = handles::cancellation(operation, &session)?;
        let request = transit::TransactionInput::decode(bytes(request, length)?, session.region.memory())?;
        *out = transit::transaction(&session, &token, request)?.into();
        Ok(())
    }) }
}

#[cfg(test)]
mod tests;
