//! Versioned blocking C ABI; ownership and thread-safety requirements are in `include/rtdb.h`.
mod arrow;
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
        let node = database.0.server.node();
        database.0.run(node.flush_all(std::time::Duration::from_nanos(timeout_nanos as u64)))
    }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_database_sync_all(handle: u64, timeout_nanos: i64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        if timeout_nanos <= 0 { return Err(invalid("Sync timeout must be positive")); }
        let database = handles::database(handle)?;
        let _admitted = database.0.enter()?;
        let node = database.0.server.node();
        database.0.run(node.sync_all(std::time::Duration::from_nanos(timeout_nanos as u64), node.cancellation()))
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
            Handle::Cursor(value) => value.close(), Handle::Cancellation(value) => value.token.cancel(), Handle::Error(_) | Handle::Bytes(_) => return Err(invalid("Immutable values are released, not closed")),
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
        let principal = database.0.run(async {
            let auth = database.0.server.authenticator();
            match authentication {
                0 => auth.unauthenticated(), 1 => auth.authenticate_trusted(text(user, user_length)?),
                2 => auth.verify_password(text(user, user_length)?, text(secret, secret_length)?).await,
                3 => auth.verify_client_credentials(text(user, user_length)?, text(secret, secret_length)?).await,
                _ => Err(invalid("Unknown embedded authentication mode")),
            }
        })?;
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
        session.region.run(async {
            let mut state = session.state.lock();
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_set_catalog(handle: u64, name: *const u8, length: usize, error: *mut u64) -> i32 {
    unsafe { call(error, || { let session = handles::session(handle)?; let _admitted = session.region.enter()?; session.state.lock().state_mut().set_database(text(name, length)?)?; Ok(()) }) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_set_application_name(handle: u64, name: *const u8, length: usize, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let session = handles::session(handle)?;
        let _admitted = session.region.enter()?;
        session.state.lock().state_mut().set_application_name(text(name, length)?)?;
        Ok(())
    }) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_await_token(handle: u64, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?; *out = 0;
        let session = handles::session(handle)?; let _admitted = session.region.enter()?; session.ensure_open()?;
        let state = session.state.lock();
        if let Some(token) = state.state().effective_await_token() {
            let memory = session.region.memory(); let reservation = memory.reserve(add(token.len(), 256)?)?;
            let value = rtdb_tx::RetainedBytes::with_reservation(token.as_bytes().to_vec(), reservation)?;
            *out = handles::insert(Handle::Bytes(Arc::new(handles::NativeBytes { value, memory: Arc::clone(memory) })))?;
        } Ok(())
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
pub unsafe extern "C" fn rtdb_v1_statement_bind_transit(handle: u64, input: *const u8, length: usize, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let statement = handles::statement(handle)?; let _admitted = statement.session.region.enter()?;
        let memory = statement.session.region.memory();
        let (value, _decode_reservation) = rtdb_pg_codec::decode_transit_msgpack(bytes(input, length)?, memory)?.into_parts();
        let rtdb_types::Value::List(rows) = value else { return Err(invalid("Transit bind requires an array of argument rows")); };
        let rows = rows.into_iter().map(|row| match row { rtdb_types::Value::List(row) => Ok(row), _ => Err(invalid("Each Transit argument row must be an array")) }).collect::<Result<Vec<_>>>()?;
        let arguments = rtdb_tx::SqlArguments::from_rows(rows, memory)?;
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
pub unsafe extern "C" fn rtdb_v1_statement_query(handle: u64, operation: u64, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || region::with_query_stack(|| {
        let out = output(out)?; *out = 0;
        let statement = handles::statement(handle)?; let _admitted = statement.session.region.enter()?;
        *out = handles::insert(Handle::Cursor(statement.query(&handles::cancellation(operation, &statement.session)?)?))?; Ok(())
    })) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_session_query_ra(session: u64, operation: u64, plan: *const u8, plan_length: usize, arguments: *const u8, arguments_length: usize, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || region::with_query_stack(|| {
        let out = output(out)?; *out = 0;
        let session = handles::session(session)?;
        let _admitted = session.region.enter()?;
        let operation = handles::cancellation(operation, &session)?;
        let memory = session.region.memory();
        let _token_storage = memory.reserve(4096)?;
        let token = operation.child_token();
        let source = text(plan, plan_length)?;
        let (arguments, _arguments) = rtdb_pg_codec::decode_transit_msgpack(bytes(arguments, arguments_length)?, memory)?.into_parts();
        let rtdb_types::Value::Struct(arguments) = arguments else { return Err(invalid("Logical relation arguments require a Transit map")); };
        let cursor = session.region.run(async { session.state.lock().query_ra(source, arguments, &token).await })?;
        *out = handles::insert(Handle::Cursor(statement::CursorHandle::new(Arc::clone(&session.region), cursor, token)?))?;
        Ok(())
    })) }
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
        *out = arrow::export_schema(&cursor.schema, cursor.region.memory())?; Ok(())
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
pub unsafe extern "C" fn rtdb_v1_cursor_consumer_error(handle: u64, error: *mut u64) -> i32 { unsafe { call(error, || { handles::cursor(handle)?.consumer_error(); Ok(()) }) } }

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rtdb_v1_cursor_next_transit(handle: u64, out: *mut u64, error: *mut u64) -> i32 {
    unsafe { call(error, || {
        let out = output(out)?; *out = 0;
        let cursor = handles::cursor(handle)?; let _admitted = cursor.region.enter()?;
        if let Some(batch) = cursor.next()? {
            let encoded = (|| {
                let memory = cursor.region.memory();
                let batch = batch.batch();
                let schema = batch.schema();
                let mut bound = add(8192, mul(batch.num_rows(), 1024)?)?;
                for row in 0..batch.num_rows() { for (field, column) in schema.fields().iter().zip(batch.columns()) {
                    bound = add(bound, add(rtdb_columnar::estimate_read_cell_bytes(field, column.as_ref(), row)?, add(mul(field.name().len(), 4)?, 2048)?)?)?;
                } }
                let _rows = memory.reserve(bound)?;
                let mut rows = Vec::with_capacity(batch.num_rows());
                let readers = schema.fields().iter().map(|field| rtdb_columnar::ColumnReader::new(field)).collect::<Result<Vec<_>>>()?;
                for row in 0..batch.num_rows() {
                    rows.push(rtdb_types::Value::Struct(schema.fields().iter().zip(&readers).zip(batch.columns()).map(|((field, reader), column)| Ok((field.name().clone(), reader.read(column.as_ref(), row)?))).collect::<Result<_>>()?));
                }
                // Rows encode one at a time into the same array, so only one row's transient encoding is charged at once.
                let mut reservation = memory.reserve(8192)?;
                let mut bytes = vec![b'['];
                for (index, row) in rows.iter().enumerate() {
                    let _transient = memory.reserve(add(mul(row.retained_bytes()?, 16)?, 8192)?)?;
                    let element = transit::encode_row(row)?;
                    let needed = add(add(bytes.len(), element.len())?, 2)?;
                    Arc::get_mut(&mut reservation).expect("cursor encoding owns its reservation").grow_to(add(needed, 8192)?)?;
                    bytes.reserve_exact(needed - bytes.len());
                    if index > 0 { bytes.push(b','); }
                    bytes.extend_from_slice(&element);
                }
                bytes.push(b']');
                let retained = bytes.capacity().max(reservation.bytes());
                Arc::get_mut(&mut reservation).expect("cursor encoding owns its reservation").grow_to(retained)?;
                drop(rows);
                let value = rtdb_tx::RetainedBytes::with_reservation(bytes, reservation)?;
                handles::insert(Handle::Bytes(Arc::new(handles::NativeBytes { value, memory: Arc::clone(memory) })))
            })().inspect_err(|_| cursor.consumer_error())?;
            *out = encoded;
        } Ok(())
    }) }
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
        let host = handles::database(database)?.0.server.addresses().pgwire
            .map(|address| if address.ip().is_unspecified() { "localhost".to_owned() } else { address.ip().to_string() })
            .unwrap_or_default();
        if host.len() > capacity { return Err(rtdb_types::Anomaly::incorrect("rtdb.ffi/buffer-too-small", "Host buffer is too small")); }
        if !host.is_empty() { std::ptr::copy_nonoverlapping(host.as_ptr(), out, host.len()); }
        *output(length)? = host.len();
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
        let value = transit::transaction(&session, &token, request)?;
        let reservation = memory.reserve(add(mul(value.retained_bytes()?, 32)?, 8192)?)?;
        let value = rtdb_tx::RetainedBytes::with_reservation(transit::encode(&value)?, reservation)?;
        *out = handles::insert(Handle::Bytes(Arc::new(handles::NativeBytes { value, memory: Arc::clone(memory) })))?; Ok(())
    }) }
}

#[cfg(test)]
mod tests;
