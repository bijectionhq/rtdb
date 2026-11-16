use std::sync::{Arc, Barrier};
use arrow_array::{Array, Int64Array, RecordBatch, StructArray, ffi::from_ffi};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::{MemoryBudget, OwnedBatch};
use super::*;

fn ok(status: i32, error: u64) {
    if status != 0 {
        let message = match handles::get(error) { Ok(Handle::Error(error)) => error.error.to_string(), _ => "diagnostic unavailable".into() };
        let _ = handles::release(error);
        panic!("{message}");
    }
    assert_eq!(error, 0);
}
struct Native { database: u64, session: u64, retained: Vec<u64> }
impl Native {
    fn open() -> Self {
        let yaml = b"server: null\nflightSql: null\nnodeId: ffi-test\n";
        let mut database = 0; let mut session = 0; let mut error = 0;
        ok(unsafe { rtdb_v1_database_open(yaml.as_ptr(), yaml.len(), 512 * 1024 * 1024, &mut database, &mut error) }, error);
        ok(unsafe { rtdb_v1_session_open(database, b"xtdb".as_ptr(), 4, 0, std::ptr::null(), 0, std::ptr::null(), 0, &mut session, &mut error) }, error);
        Self { database, session, retained: Vec::new() }
    }
    fn operation(&mut self) -> u64 {
        let mut out = 0; let mut error = 0;
        ok(unsafe { rtdb_v1_operation_new(self.session, &mut out, &mut error) }, error); self.retained.push(out); out
    }
    fn statement(&mut self, sql: &str) -> u64 {
        let mut out = 0; let mut error = 0;
        ok(unsafe { rtdb_v1_statement_new(self.session, sql.as_ptr(), sql.len(), 0, &mut out, &mut error) }, error); self.retained.push(out); out
    }
    fn query(&mut self, statement: u64, operation: u64) -> u64 {
        let mut out = 0; let mut error = 0;
        ok(unsafe { rtdb_v1_statement_query(statement, operation, &mut out, &mut error) }, error); self.retained.push(out); out
    }
}
impl Drop for Native {
    fn drop(&mut self) {
        for handle in self.retained.drain(..).rev() { let _ = handles::release(handle); }
        let _ = handles::release(self.session); let _ = handles::release(self.database);
    }
}

#[test]
fn statement_captures_creation_catalog_for_prepare_schema_and_reexecution() {
    let mut native = Native::open();
    let operation = native.operation();
    let session = handles::session(native.session).unwrap();
    let token = handles::cancellation(operation, &session).unwrap();
    let insert = native.statement("INSERT INTO docs (_id, label) VALUES (1, 'first')");
    handles::statement(insert).unwrap().update(&token).unwrap();
    let prepared = native.statement("SELECT label FROM docs");
    let prepared = handles::statement(prepared).unwrap();
    prepared.prepare(&token).unwrap();
    let deferred = native.statement("SELECT label FROM docs");
    let deferred = handles::statement(deferred).unwrap();
    let dml = native.statement("INSERT INTO docs (_id, label) VALUES (3, 'third')");
    session.region.run(async {
        session.state.lock().execute_control(&rtdb_plan::ControlStatement::AttachDatabase { name: "other".into(), configuration_yaml: None }, &[], &token).await?;
        session.state.lock().state_mut().set_database("other")
    }).unwrap();
    let insert = native.statement("INSERT INTO docs (_id, label) VALUES (2, 'second')");
    handles::statement(insert).unwrap().update(&token).unwrap();
    handles::statement(dml).unwrap().update(&token).unwrap();
    for statement in [&prepared, &deferred] {
        statement.prepare(&token).unwrap();
        assert_eq!(statement.schema(false, &token).unwrap().schema.field(0).name(), "label");
        for _ in 0..2 {
            let cursor = statement.query(&token).unwrap();
            let batch = cursor.next().unwrap().unwrap();
            assert_eq!(batch.batch().num_rows(), 1);
            assert_eq!(rtdb_columnar::read_value(batch.batch().schema().field(0), batch.batch().column(0).as_ref(), 0).unwrap(), rtdb_types::Value::Utf8("first".into()));
            assert!(cursor.next().unwrap().is_none());
        }
    }
    deferred.replace_source("SELECT label FROM docs", false).unwrap();
    let batch = deferred.query(&token).unwrap().next().unwrap().unwrap();
    assert_eq!(rtdb_columnar::read_value(batch.batch().schema().field(0), batch.batch().column(0).as_ref(), 0).unwrap(), rtdb_types::Value::Utf8("first".into()));
    let current = native.statement("SELECT label FROM docs");
    assert_eq!(handles::statement(current).unwrap().query(&token).unwrap().next().unwrap().unwrap().batch().num_rows(), 2);
    assert_eq!(session.state.lock().state().database(), "other");
}

#[test]
fn adbc_session_show_survives_failed_transaction_while_queries_and_pgwire_show_reject() {
    let mut native = Native::open();
    let operation = native.operation();
    let session = handles::session(native.session).unwrap();
    let token = handles::cancellation(operation, &session).unwrap();
    {
        let mut connection = session.state.lock();
        connection.state_mut().set_parameter("application_name".into(), Some("inspectable".into())).unwrap();
        connection.state_mut().begin().unwrap();
        connection.state_mut().fail_transaction(invalid("failed"));
    }
    let show = native.statement("SHOW application_name");
    let show = handles::statement(show).unwrap();
    show.prepare(&token).unwrap();
    let batch = show.query(&token).unwrap().next().unwrap().unwrap();
    assert_eq!(rtdb_columnar::read_value(batch.batch().schema().field(0), batch.batch().column(0).as_ref(), 0).unwrap(), rtdb_types::Value::Utf8("inspectable".into()));
    for sql in ["SELECT 1", "SHOW TIME ZONE", "SHOW SNAPSHOT_TOKEN"] {
        let statement = native.statement(sql);
        assert_eq!(handles::statement(statement).unwrap().query(&token).err().unwrap().code, "xtdb/tx-failed");
    }
    let error = session.region.run(async {
        session.state.lock().execute_control_with(&rtdb_plan::ControlStatement::Show { name: "application_name".into() }, &[], &token, rtdb_node::ControlExecutionOptions { frontend: rtdb_node::ControlFrontend::PostgreSql, implicit_transaction: false }).await.map(|_| ())
    }).unwrap_err();
    assert_eq!(error.code, "xtdb/tx-failed");
    assert_eq!(session.state.lock().state().status(), rtdb_node::TransactionStatus::Failed);
}

#[test]
fn released_ids_are_rejected_and_retained_ids_are_independent() {
    let first = handles::diagnostic(invalid("first"));
    assert_ne!(first, 1);
    let second = handles::insert(handles::get(first).unwrap()).unwrap();
    handles::release(first).unwrap();
    assert!(handles::get(first).is_err());
    assert!(handles::release(first).is_err());
    assert!(matches!(handles::get(second), Ok(Handle::Error(_))));
    handles::release(second).unwrap();
    let third = handles::diagnostic(invalid("third"));
    assert!(third > second); handles::release(third).unwrap();
}

#[test]
fn ffi_batch_survives_statement_cursor_and_database_close() {
    let mut native = Native::open();
    let statement = native.statement("SELECT 42 AS answer"); let operation = native.operation();
    let cursor = native.query(statement, operation);
    let memory = Arc::clone(handles::database(native.database).unwrap().0.memory());
    let mut array = FFI_ArrowArray::empty(); let mut schema = FFI_ArrowSchema::empty(); let mut present = 0; let mut error = 0;
    ok(unsafe { rtdb_v1_cursor_next(cursor, &mut array, &mut schema, &mut present, &mut error) }, error); assert_eq!(present, 1);
    ok(unsafe { rtdb_v1_close(statement, &mut error) }, error);
    ok(unsafe { rtdb_v1_close(cursor, &mut error) }, error);
    ok(unsafe { rtdb_v1_close(native.database, &mut error) }, error);
    drop(native);
    let imported = unsafe { from_ffi(array, &schema) }.unwrap();
    let rows = StructArray::from(imported);
    let value = rtdb_columnar::read_value(&Field::try_from(schema.child(0)).unwrap(), rows.column(0).as_ref(), 0).unwrap();
    assert!(matches!(value, rtdb_types::Value::I32(42) | rtdb_types::Value::I64(42)));
    assert!(memory.used_bytes() > 0);
    drop(rows); drop(schema); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn adbc_unnamed_parameters_are_bound_by_ordinal_and_copied() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("", DataType::Int64, false), Field::new("", DataType::Int64, false)])), vec![Arc::new(Int64Array::from(vec![3])), Arc::new(Int64Array::from(vec![7]))]).unwrap();
    let mut schema = FFI_ArrowSchema::try_from(batch.schema().as_ref()).unwrap();
    let mut array = FFI_ArrowArray::new(&StructArray::from(batch).to_data());
    let values = unsafe { arrow::import_rows(&mut array, &mut schema, &memory) }.unwrap();
    assert!(array.release.is_none()); assert!(schema.release.is_none());
    assert_eq!(values.arguments.rows().next().unwrap(), &[rtdb_types::Value::I64(3), rtdb_types::Value::I64(7)]);
    assert_eq!(values.names, vec!["", ""]);
    drop(values); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn arrow_release_is_idempotent_and_retains_charge_through_imported_children() {
    let memory = MemoryBudget::new(1_000_000);
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])), vec![Arc::new(Int64Array::from(vec![9]))]).unwrap();
    let batch = OwnedBatch::new(batch, &memory).unwrap();
    let (mut array, mut schema) = arrow::export_batch(batch, &memory).unwrap();
    let release_array = array.release.unwrap(); let release_schema = schema.release.unwrap();
    unsafe { release_array(&mut array); release_array(&mut array); release_schema(&mut schema); release_schema(&mut schema); }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn decimal_c_data_uses_signed_parameters_and_retains_detached_schema_children() {
    for width in [32, 64, 128, 256] {
        for (precision, scale) in [(3, i32::MIN), (3, -129), (3, 128), (3, i32::MAX), (i32::MIN, 0), (-1, i32::MIN), (0, i32::MAX), (256, 1), (i32::MAX, i32::MAX)] {
            let memory = MemoryBudget::new(16 * 1024 * 1024);
            let ty = rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Decimal { precision, scale, bit_width: width });
            let coefficient = arrow_buffer::i256::from_i128(-123).to_be_bytes();
            let value = rtdb_types::Value::Decimal(rtdb_types::DecimalValue::new(coefficient[32-usize::from(width/8)..].to_vec(), precision, scale, width).unwrap());
            let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![rtdb_columnar::to_field("value", &ty).unwrap()])), vec![rtdb_columnar::copy_array(&ty, std::slice::from_ref(&value)).unwrap()]).unwrap();
            let (mut array, mut schema) = arrow::export_batch(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
            let expected = if width == 128 { format!("d:{precision},{scale}") } else { format!("d:{precision},{scale},{width}") };
            assert_eq!(schema.child(0).format(), expected);
            assert!(!schema.child(0).metadata().unwrap().contains_key(rtdb_columnar::INTERNAL_DECIMAL_SCALE_KEY));
            assert!(!schema.child(0).metadata().unwrap().contains_key(rtdb_columnar::INTERNAL_DECIMAL_PRECISION_KEY));
            let imported = unsafe { arrow::import_rows(&mut array, &mut schema, &memory) }.unwrap();
            assert_eq!(imported.arguments.types(), &[ty.clone()]);
            assert_eq!(imported.arguments.rows().next().unwrap(), &[value]);
            drop(imported); assert_eq!(memory.used_bytes(), 0);
            let mut schema = arrow::export_schema(&Schema::new(vec![rtdb_columnar::to_field("value", &ty).unwrap()]), &memory).unwrap();
            let mut child = unsafe { std::mem::replace(&mut **schema.children, FFI_ArrowSchema::empty()) };
            drop(schema); assert!(memory.used_bytes() > 0); assert_eq!(child.format(), expected);
            let release = child.release.unwrap(); unsafe { release(&mut child); release(&mut child); }
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}

#[test]
fn foreign_decimal_scale_annotations_are_rejected_and_owned_inputs_released() {
    for key in [rtdb_columnar::INTERNAL_DECIMAL_SCALE_KEY, rtdb_columnar::INTERNAL_DECIMAL_PRECISION_KEY] {
    let memory = MemoryBudget::new(1 << 20);
    let field = Field::new("value", DataType::Decimal128(3, 1), true).with_metadata(std::collections::HashMap::from([(key.to_owned(), i32::MAX.to_string())]));
    let batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![field])));
    let mut schema = FFI_ArrowSchema::try_from(batch.schema().as_ref()).unwrap();
    let mut array = FFI_ArrowArray::new(&StructArray::from(batch).to_data());
    assert!(unsafe { arrow::import_rows(&mut array, &mut schema, &memory) }.is_err());
    assert!(array.release.is_none()); assert!(schema.release.is_none()); assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn nested_decimal_c_data_restores_each_union_child_scale() {
    use rtdb_types::{DecimalValue, LogicalType, ScalarType, Value};
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let decimal = |scale| LogicalType::Scalar(ScalarType::Decimal { precision: 3, scale, bit_width: 128 });
    let ty = LogicalType::list(LogicalType::from_legs([decimal(i32::MIN), decimal(i32::MAX)]));
    let value = Value::List([i32::MIN, i32::MAX].into_iter().map(|scale| Value::Decimal(DecimalValue::new((-123i128).to_be_bytes().to_vec(), 3, scale, 128).unwrap())).collect());
    let page = RecordBatch::try_new(Arc::new(Schema::new(vec![rtdb_columnar::to_field("values", &ty).unwrap()])), vec![rtdb_columnar::build_array(&ty, std::slice::from_ref(&value)).unwrap()]).unwrap();
    let (mut array, mut schema) = arrow::export_batch(OwnedBatch::new(page, &memory).unwrap(), &memory).unwrap();
    let imported = unsafe { arrow::import_rows(&mut array, &mut schema, &memory) }.unwrap();
    assert_eq!(imported.arguments.types(), &[ty]); assert_eq!(imported.arguments.rows().next().unwrap(), &[value]);
    drop(imported); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn cancelling_one_stream_preserves_the_session_and_sibling_stream() {
    let mut native = Native::open();
    let first = native.statement("SELECT 1 AS v"); let first_operation = native.operation(); let first = native.query(first, first_operation);
    let second = native.statement("SELECT 2 AS v"); let second_operation = native.operation(); let second = native.query(second, second_operation);
    let mut error = 0;
    ok(unsafe { rtdb_v1_cancel(first_operation, &mut error) }, error);
    assert!(handles::cursor(first).unwrap().next().is_err());
    assert_eq!(handles::cursor(second).unwrap().next().unwrap().unwrap().batch().num_rows(), 1);
}

#[test]
fn adbc_set_query_rejection_has_no_side_effect_and_show_schema_requires_prepare() {
    let mut native = Native::open();
    let operation = native.operation();
    let session = handles::session(native.session).unwrap();
    let token = handles::cancellation(operation, &session).unwrap();
    let set = native.statement("SET application_name = 'changed'");
    let set = handles::statement(set).unwrap();
    assert!(matches!(set.query(&token), Err(error) if error.code == "xtdb/not-a-query"));
    assert_eq!(session.state.lock().state().parameter("application_name"), None);
    assert!(set.schema(true, &token).unwrap().schema.fields().is_empty());
    let show = native.statement("SHOW application_name");
    let show = handles::statement(show).unwrap();
    assert!(matches!(show.schema(false, &token), Err(error) if error.code == "xtdb.adbc/not-prepared"));
    show.prepare(&token).unwrap();
    assert_eq!(show.schema(false, &token).unwrap().schema.fields()[0].name(), "application_name");
    assert!(matches!(show.update(&token), Err(error) if error.code == "xtdb.adbc/not-an-update"));
}

#[test]
fn show_latest_submitted_after_async_submit_does_not_await_a_future_token() {
    let mut native = Native::open();
    let session = handles::session(native.session).unwrap();
    session.region.run(async {
        let mut session = session.state.lock();
        session.state_mut().submit_tx(vec![rtdb_tx::TxOp::Sql { sql: "INSERT INTO docs (_id) VALUES (1)".into(), args: None }], Default::default()).await?;
        let future = rtdb_format::basis::encode_tx_basis(&rtdb_types::TxBasis { databases: std::collections::BTreeMap::from([("xtdb".into(), vec![rtdb_types::TxId(i64::MAX)])]) });
        session.state_mut().set_await_token(Some(future))
    }).unwrap();
    let statement = native.statement("SHOW latest_submitted_tx");
    let statement = handles::statement(statement).unwrap();
    let operation = native.operation();
    let token = handles::cancellation(operation, &session).unwrap();
    let cancellation = token.clone();
    let (sender, receiver) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || { let result = statement.query(&token).and_then(|cursor| cursor.next()).map(|batch| batch.unwrap().batch().num_rows()); let _ = sender.send(result); });
    let result = receiver.recv_timeout(std::time::Duration::from_secs(2));
    cancellation.cancel(); worker.join().unwrap();
    assert_eq!(result.unwrap().unwrap(), 1);
}

#[test]
fn concurrent_close_cancels_admitted_work_before_joining() {
    let native = Native::open();
    let region = Arc::clone(&handles::database(native.database).unwrap().0);
    let waiting = Arc::clone(&region); let barrier = Arc::new(Barrier::new(2)); let entered = Arc::clone(&barrier);
    let thread = std::thread::spawn(move || {
        let _admitted = waiting.enter().unwrap();
        let token = waiting.server.node().cancellation().child_token();
        entered.wait();
        waiting.run(async { token.cancelled().await; Ok(()) }).unwrap();
    });
    barrier.wait(); region.close().unwrap(); thread.join().unwrap(); assert!(region.enter().is_err());
}

#[test]
fn metadata_schema_keeps_adbc_field_order_and_nullability() {
    let schema = metadata::objects_schema();
    assert_eq!(schema.fields()[0].name(), "catalog_name"); assert!(!schema.fields()[0].is_nullable());
    let DataType::List(item) = schema.fields()[1].data_type() else { panic!() };
    let DataType::Struct(fields) = item.data_type() else { panic!() };
    assert_eq!(fields[0].name(), "db_schema_name"); assert_eq!(fields[1].name(), "db_schema_tables");
}

#[test]
fn clojure_result_transit_omits_null_struct_fields_and_keeps_narrow_numbers() {
    let value = rtdb_types::Value::Struct(std::collections::BTreeMap::from([("missing".into(), rtdb_types::Value::Null), ("small".into(), rtdb_types::Value::I8(5))]));
    let wire = transit::encode(&value).unwrap();
    let decoded = rtdb_pg_codec::from_transit(rtdb_format::transit::decode_json(&wire).unwrap()).unwrap();
    let rtdb_types::Value::Struct(fields) = decoded else { panic!() };
    assert!(!fields.contains_key("missing")); assert_eq!(fields["small"], rtdb_types::Value::I8(5));
}
