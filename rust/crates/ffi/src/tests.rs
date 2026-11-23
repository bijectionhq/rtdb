use rtdb_types::StructFields;
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

fn incorrect(status: i32, error: u64, code: &str) { incorrect_anomaly(status, error, code); }

fn incorrect_anomaly(status: i32, error: u64, code: &str) -> rtdb_types::Anomaly {
    assert_eq!(status, 1);
    let Handle::Error(diagnostic) = handles::get(error).unwrap() else { panic!("expected diagnostic") };
    handles::release(error).unwrap();
    assert_eq!(diagnostic.error.category, AnomalyCategory::Incorrect);
    assert_eq!(diagnostic.error.code, code);
    (*diagnostic.error).clone()
}

struct Native { database: u64, session: u64, retained: Vec<u64> }

#[test]
fn flush_all_validates_timeout_and_refuses_closed_handles() {
    let native = Native::open();
    let mut error = 0;
    for timeout in [0, -1] {
        let status = unsafe { rtdb_v1_database_flush_all(native.database, timeout, &mut error) };
        incorrect(status, error, "rtdb.ffi/invalid-argument");
    }
    ok(unsafe { rtdb_v1_close(native.database, &mut error) }, error);
    let status = unsafe { rtdb_v1_database_flush_all(native.database, 1_000_000, &mut error) };
    incorrect(status, error, "rtdb.ffi/closed");
}

#[test]
fn sync_all_validates_timeout_waits_on_an_open_node_and_refuses_closed_handles() {
    let native = Native::open();
    let mut error = 0;
    for timeout in [0, -1] {
        let status = unsafe { rtdb_v1_database_sync_all(native.database, timeout, &mut error) };
        incorrect(status, error, "rtdb.ffi/invalid-argument");
    }
    ok(unsafe { rtdb_v1_database_sync_all(native.database, 1_000_000_000, &mut error) }, error);
    ok(unsafe { rtdb_v1_close(native.database, &mut error) }, error);
    let status = unsafe { rtdb_v1_database_sync_all(native.database, 1_000_000, &mut error) };
    incorrect(status, error, "rtdb.ffi/closed");
}

#[test]
fn pgwire_port_reports_the_bound_listener_or_its_absence() {
    let port = |native: &Native| { let mut port = 0; let mut error = 0; ok(unsafe { rtdb_v1_database_pgwire_port(native.database, &mut port, &mut error) }, error); port };
    assert_eq!(port(&Native::open()), -1);
    let listening = Native::open_yaml(b"server:\n  port: 0\nflightSql: null\nnodeId: ffi-port-test\n");
    let bound = port(&listening);
    assert!(bound > 0);
    assert!(std::net::TcpStream::connect(("127.0.0.1", bound as u16)).is_ok());
}

#[test]
fn pgwire_client_host_is_the_bound_address_or_localhost_for_a_wildcard() {
    let host = |native: &Native| {
        let (mut buffer, mut length, mut error) = ([0u8; 64], 0usize, 0);
        ok(unsafe { rtdb_v1_database_pgwire_client_host(native.database, buffer.as_mut_ptr(), buffer.len(), &mut length, &mut error) }, error);
        String::from_utf8(buffer[..length].to_vec()).unwrap()
    };
    assert_eq!(host(&Native::open()), "");
    assert_eq!(host(&Native::open_yaml(b"server:\n  port: 0\n  host: 127.0.0.1\nflightSql: null\nnodeId: ffi-host-test\n")), "127.0.0.1");
    assert_eq!(host(&Native::open_yaml(b"server:\n  port: 0\n  host: '*'\nflightSql: null\nnodeId: ffi-wildcard-host-test\n")), "localhost");
}

#[test]
fn flight_sql_port_reports_the_bound_listener_or_its_absence() {
    let port = |native: &Native| { let mut port = 0; let mut error = 0; ok(unsafe { rtdb_v1_database_flight_sql_port(native.database, &mut port, &mut error) }, error); port };
    assert_eq!(port(&Native::open()), -1);
    let listening = Native::open_yaml(b"server: null\nflightSql:\n  port: 0\nnodeId: ffi-flight-port-test\n");
    let bound = port(&listening);
    assert!(bound > 0);
    assert!(std::net::TcpStream::connect(("127.0.0.1", bound as u16)).is_ok());
}

#[test]
fn diagnostic_slices_distinguish_absent_empty_and_present_messages_through_retain() {
    for message in [None, Some(String::new()), Some("échec".into())] {
        let original = handles::diagnostic(Anomaly::new_nullable(AnomalyCategory::Fault, "test/error", message.clone()));
        assert_ne!(original, 1);
        let mut retained = 0; let mut error = 0;
        ok(unsafe { rtdb_v1_retain(original, &mut retained, &mut error) }, error);
        handles::release(original).unwrap();
        let mut slice = RtdbSlice { data: std::ptr::null(), length: 0 };
        ok(unsafe { rtdb_v1_error_field(retained, 2, &mut slice, &mut error) }, error);
        assert_eq!(slice.data.is_null(), message.is_none());
        assert_eq!(slice.length, message.as_ref().map_or(0, String::len));
        if let Some(message) = &message { assert_eq!(unsafe { std::slice::from_raw_parts(slice.data, slice.length) }, message.as_bytes()); }
        for (field, expected) in [(0, b"fault".as_slice()), (1, b"test/error".as_slice())] {
            ok(unsafe { rtdb_v1_error_field(retained, field, &mut slice, &mut error) }, error);
            assert_eq!(unsafe { std::slice::from_raw_parts(slice.data, slice.length) }, expected);
        }
        ok(unsafe { rtdb_v1_error_field(retained, 3, &mut slice, &mut error) }, error);
        assert!(!slice.data.is_null());
        let decoded = rtdb_format::transit::decode_anomaly(unsafe { std::slice::from_raw_parts(slice.data, slice.length) }).unwrap();
        assert_eq!(decoded.message, message);
        handles::release(retained).unwrap();
    }
}

#[test]
fn database_properties_reject_unknown_and_oversized_inputs_before_node_configuration() {
    let yaml = b"not: [valid YAML";
    for properties in [r#"{"xtdb.vector.null-checks":"false"}"#,r#"{"java.util.Arrays.useLegacyMergeSort":true}"#] {
        let mut database = 42;let mut error = 0;
        assert_eq!(unsafe { rtdb_v1_database_open_with_properties(yaml.as_ptr(),yaml.len(),1 << 20,properties.as_ptr(),properties.len(),&mut database,&mut error) },1);
        assert_eq!(database,0);
        let Handle::Error(diagnostic) = handles::get(error).unwrap() else { panic!("expected diagnostic") };
        assert_eq!(diagnostic.error.code,"xtdb/invalid-node-config");
        assert!(diagnostic.error.message.as_deref().is_some_and(|message| message.contains("system propert")));
        handles::release(error).unwrap();
    }
    let mut database = 42;let mut error = 0;
    assert_eq!(unsafe { rtdb_v1_database_open_with_properties(yaml.as_ptr(),yaml.len(),1 << 20,std::ptr::null(),4097,&mut database,&mut error) },1);
    assert_eq!(database,0);
    let Handle::Error(diagnostic) = handles::get(error).unwrap() else { panic!("expected diagnostic") };
    assert!(diagnostic.error.message.as_deref().is_some_and(|message| message.contains("4096")));handles::release(error).unwrap();
}

impl Native {
    fn open() -> Self {
        Self::open_yaml(b"server: null\nflightSql: null\nnodeId: ffi-test\n")
    }
    fn open_yaml(yaml: &[u8]) -> Self {
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
    fn set_sql(&self, statement: u64, sql: &str) {
        let mut error = 0;
        ok(unsafe { rtdb_v1_statement_set_sql(statement, sql.as_ptr(), sql.len(), &mut error) }, error);
    }
    fn memory_used(&self) -> usize {
        let mut used = 0;
        let mut error = 0;
        ok(unsafe { rtdb_v1_memory_used(self.database, &mut used, &mut error) }, error);
        used
    }
    fn query(&mut self, statement: u64, operation: u64) -> u64 {
        let mut out = 0; let mut error = 0;
        ok(unsafe { rtdb_v1_statement_query(statement, operation, &mut out, &mut error) }, error); self.retained.push(out); out
    }
    fn query_value(&mut self, statement: u64, operation: u64) -> rtdb_types::Value {
        let cursor = self.query(statement, operation);
        let mut array = FFI_ArrowArray::empty();
        let mut schema = FFI_ArrowSchema::empty();
        let mut present = 0;
        let mut error = 0;
        ok(unsafe { rtdb_v1_cursor_next(cursor, &mut array, &mut schema, &mut present, &mut error) }, error);
        assert_eq!(present, 1);
        let rows = StructArray::from(unsafe { from_ffi(array, &schema) }.unwrap());
        assert_eq!(rows.len(), 1);
        let field = Field::try_from(schema.child(0)).unwrap();
        rtdb_columnar::read_value(&field, rows.column(0).as_ref(), 0).unwrap()
    }
}
impl Drop for Native {
    fn drop(&mut self) {
        for handle in self.retained.drain(..).rev() { let _ = handles::release(handle); }
        let _ = handles::release(self.session); let _ = handles::release(self.database);
    }
}

#[test]
fn adbc_virtual_endpoints_keep_declared_schema_and_conformed_null_buffers() {
    let mut native = Native::open();
    let operation = native.operation();
    let session = handles::session(native.session).unwrap();
    let token = handles::cancellation(operation, &session).unwrap();
    let statement = native.statement("SELECT _valid_from,_valid_time FROM pg_catalog.pg_namespace LIMIT 1");
    let statement = handles::statement(statement).unwrap();
    statement.prepare(&token).unwrap();
    let prepared = statement.schema(false, &token).unwrap();
    assert!(!prepared.schema.field(0).is_nullable());
    let cursor = statement.query(&token).unwrap();
    assert_eq!(cursor.schema, prepared.schema);
    let batch = cursor.next().unwrap().unwrap();
    assert_eq!(batch.declared_schema().as_ref(), &cursor.schema);
    assert_eq!(batch.batch().column(0).null_count(), 1);
    assert!(batch.batch().schema().field(0).is_nullable());
    let (array, schema) = arrow::export_declared_batch(batch, session.region.memory()).unwrap();
    let exported = Schema::try_from(&schema).unwrap();
    assert_eq!(exported, cursor.schema);
    assert_eq!(array.child(0).null_count(), 1);
    assert!(cursor.next().unwrap().is_none());
    drop(array); drop(schema);
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
        session.state.lock().execute_control(&rtdb_plan::ControlStatement::AttachDatabase { name: "other".into(), configuration_yaml: None, source: "ATTACH DATABASE other".into() }, &[], &token).await?;
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
            drop(cursor);
            assert!(!token.is_cancelled());
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
        connection.state_mut().set_parameter("application_name", Some("inspectable".into())).unwrap();
        connection.state_mut().begin().unwrap();
        connection.state_mut().fail_transaction();
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
    let producer = MemoryBudget::new(16 * 1024 * 1024);
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("", DataType::Int64, false), Field::new("", DataType::Int64, false)])), vec![Arc::new(Int64Array::from(vec![3])), Arc::new(Int64Array::from(vec![7]))]).unwrap();
    let original = batch.column(0).to_data().buffers()[0].as_ptr();
    let (mut array, mut schema) = arrow::export_batch(OwnedBatch::new(batch, &producer).unwrap(), &producer).unwrap();
    let values = unsafe { arrow::import_arguments(&mut array, &mut schema, &memory) }.unwrap();
    assert!(array.release.is_none()); assert!(schema.release.is_none());
    assert_eq!(producer.used_bytes(), 0);
    let page = values.arguments.single_arrow_page().unwrap();
    assert_ne!(page.batch().column(0).to_data().buffers()[0].as_ptr(), original);
    assert_eq!(&*values.arguments.rows().next().unwrap().unwrap(), &[rtdb_types::Value::I64(3), rtdb_types::Value::I64(7)]);
    assert_eq!(values.names, vec!["", ""]);
    drop(values); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn adbc_unsigned_parameter_buffers_execute_as_signed_and_preserve_typed_nulls() {
    let mut native = Native::open();
    let operation = native.operation();
    let handle = native.statement("SELECT ? AS value");
    let statement = handles::statement(handle).unwrap();
    let token = handles::cancellation(operation, &statement.session).unwrap();
    statement.prepare(&token).unwrap();
    for value in [Some(u64::MAX), None] {
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("", DataType::UInt64, true)])), vec![Arc::new(arrow_array::UInt64Array::from(vec![value]))]).unwrap();
        let mut schema = FFI_ArrowSchema::try_from(batch.schema().as_ref()).unwrap();
        let mut array = FFI_ArrowArray::new(&StructArray::from(batch).to_data());
        let imported = unsafe { arrow::import_arguments(&mut array, &mut schema, statement.session.region.memory()) }.unwrap();
        assert_eq!(imported.arguments.types(), &[rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::U64).maybe()]);
        statement.bind(imported).unwrap();
        let cursor = statement.query(&token).unwrap();
        assert_eq!(cursor.schema.field(0).data_type(), &DataType::Int64);
        let page = cursor.next().unwrap().unwrap();
        let expected = value.map(|_| rtdb_types::Value::I64(-1)).unwrap_or(rtdb_types::Value::Null);
        assert_eq!(rtdb_columnar::read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), expected);
    }
}

#[test]
fn adbc_unsigned_bulk_documents_admit_ids_before_submission() {
    let mut native = Native::open();
    let operation = native.operation();
    let handle = native.statement("SELECT 1");
    let statement = handles::statement(handle).unwrap();
    let token = handles::cancellation(operation, &statement.session).unwrap();
    statement.replace_source("unsigned_docs", true).unwrap();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("_id", DataType::UInt64, false), Field::new("v", DataType::UInt64, false)])), vec![Arc::new(arrow_array::UInt64Array::from(vec![u64::MAX])), Arc::new(arrow_array::UInt64Array::from(vec![u64::MAX]))]).unwrap();
    let mut schema = FFI_ArrowSchema::try_from(batch.schema().as_ref()).unwrap();
    let mut array = FFI_ArrowArray::new(&StructArray::from(batch).to_data());
    statement.bind(unsafe { arrow::import_arguments(&mut array, &mut schema, statement.session.region.memory()) }.unwrap()).unwrap();
    assert_eq!(statement.update(&token).unwrap(), 1);
    statement.replace_source("SELECT _id,v FROM unsigned_docs", false).unwrap();
    let cursor = statement.query(&token).unwrap();
    let page = cursor.next().unwrap().unwrap();
    for column in 0..2 { assert_eq!(rtdb_columnar::read_value(page.batch().schema().field(column), page.batch().column(column).as_ref(), 0).unwrap(), rtdb_types::Value::I64(-1)); }
}

#[test]
fn adbc_embedded_vectors_stay_typed_from_bulk_ingest_through_bound_top_k() {
    use arrow_array::{FixedSizeListArray, Float32Array, Float64Array, StringArray};
    let mut native = Native::open();
    let handle = native.statement("SELECT 1");
    let operation = native.operation();
    let statement = handles::statement(handle).unwrap();
    let token = handles::cancellation(operation, &statement.session).unwrap();
    let bind = |batch: RecordBatch| {
        let mut schema = FFI_ArrowSchema::try_from(batch.schema().as_ref()).unwrap();
        let mut array = FFI_ArrowArray::new(&StructArray::from(batch).to_data());
        let mut error = 0;
        ok(unsafe { rtdb_v1_statement_bind(handle, &mut array, &mut schema, &mut error) }, error);
        assert!(array.release.is_none() && schema.release.is_none());
    };
    statement.replace_source("vectors", true).unwrap();
    let item = Arc::new(Field::new("item", DataType::Float32, true));
    let nulls = arrow_buffer::NullBuffer::from(vec![true, true, false, true]);
    let values = Float32Array::from(vec![Some(0.0), Some(2.0), Some(3.0), Some(0.0), None, None, Some(4.0), Some(0.0)]);
    let vectors = FixedSizeListArray::try_new(Arc::clone(&item), 2, Arc::new(values), Some(nulls)).unwrap();
    let schema = Schema::new(vec![
        Field::new("_id", DataType::Int64, true),
        Field::new("embedding", vectors.data_type().clone(), true),
        Field::new("label", DataType::Utf8, true),
    ]);
    bind(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4])), Arc::new(vectors),
        Arc::new(StringArray::from(vec![Some("old"), None, None, Some("new")]))]).unwrap());
    assert_eq!(statement.update(&token).unwrap(), 4);

    let mut error = 0;
    ok(unsafe { rtdb_v1_database_flush_all(native.database, 5_000_000_000, &mut error) }, error);
    statement.replace_source("SELECT _id, COSINE_DISTANCE(embedding, ?) AS d, embedding, label FROM vectors ORDER BY d NULLS LAST, _id OFFSET 1 LIMIT 3", false).unwrap();
    statement.prepare(&token).unwrap();
    let query = FixedSizeListArray::try_new(item, 2, Arc::new(Float32Array::from(vec![1.0, 0.0])), None).unwrap();
    let schema = Schema::new(vec![Field::new("", query.data_type().clone(), false)]);
    bind(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(query)]).unwrap());
    let page = statement.query(&token).unwrap().next().unwrap().unwrap();
    let batch = page.batch();
    let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let distances = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
    let vectors = batch.column(2).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    let labels = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(ids.values().as_ref(), &[4, 1, 3]);
    assert_eq!(distances.iter().collect::<Vec<_>>(), [Some(0.0), Some(1.0), None]);
    assert_eq!(labels.iter().collect::<Vec<_>>(), [Some("new"), Some("old"), None]);
    let values = vectors.values().as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(&values.values()[..4], &[4.0, 0.0, 0.0, 2.0]);
    assert!(vectors.is_null(2));
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
            let imported = unsafe { arrow::import_arguments(&mut array, &mut schema, &memory) }.unwrap();
            assert_eq!(imported.arguments.types(), &[ty.clone()]);
            assert_eq!(&*imported.arguments.rows().next().unwrap().unwrap(), &[value]);
            drop(imported); assert_eq!(memory.used_bytes(), 0);
            let schema = arrow::export_schema(&Schema::new(vec![rtdb_columnar::to_field("value", &ty).unwrap()]), &memory).unwrap();
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
    assert!(unsafe { arrow::import_arguments(&mut array, &mut schema, &memory) }.is_err());
    assert!(array.release.is_none()); assert!(schema.release.is_none()); assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn nested_decimal_c_data_restores_each_union_child_scale() {
    use rtdb_types::{DecimalValue, LogicalType, ScalarType, Value};
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let decimal = |scale| LogicalType::Scalar(ScalarType::Decimal { precision: 3, scale, bit_width: 128 });
    let ty = LogicalType::list(LogicalType::from_legs([decimal(i32::MIN), decimal(i32::MAX)])).maybe();
    let value = Value::list([i32::MIN, i32::MAX].into_iter().map(|scale| Value::Decimal(DecimalValue::new((-123i128).to_be_bytes().to_vec(), 3, scale, 128).unwrap())).collect());
    let page = RecordBatch::try_new(Arc::new(Schema::new(vec![rtdb_columnar::to_field("values", &ty).unwrap()])), vec![rtdb_columnar::build_array(&ty, &[Value::Null, value.clone()]).unwrap()]).unwrap();
    let (mut array, mut schema) = arrow::export_batch(OwnedBatch::new(page, &memory).unwrap(), &memory).unwrap();
    let imported = unsafe { arrow::import_arguments(&mut array, &mut schema, &memory) }.unwrap();
    assert_eq!(imported.arguments.types(), &[ty]);
    let mut rows = imported.arguments.rows();
    assert_eq!(&*rows.next().unwrap().unwrap(), &[Value::Null]);
    assert_eq!(&*rows.next().unwrap().unwrap(), &[value]);
    assert!(rows.next().is_none());
    drop(rows);
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
fn cursor_cancel_and_close_interrupt_active_pulls_without_cancelling_the_operation() {
    use std::{sync::{Barrier, mpsc}, thread, time::Duration};
    let mut native = Native::open();
    let operation = native.operation();
    let session = handles::session(native.session).unwrap();
    let token = handles::cancellation(operation, &session).unwrap();
    for close in [false, true] {
        let statement = native.statement("SELECT pg_sleep(10.0)");
        let cursor = native.query(statement, operation);
        let value = handles::cursor(cursor).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let started = Arc::clone(&barrier);
        let reader = Arc::clone(&value);
        let (result, received) = mpsc::channel();
        let worker = thread::spawn(move || {
            started.wait();
            result.send(reader.next().map(|_| ())).unwrap();
        });
        barrier.wait();
        thread::sleep(Duration::from_millis(50));
        let (closed, joined) = mpsc::channel();
        let closer = thread::spawn(move || {
            if close { value.close(); } else {
                let mut error = 0;
                ok(unsafe { rtdb_v1_cancel(cursor, &mut error) }, error);
            }
            closed.send(()).unwrap();
        });
        joined.recv_timeout(Duration::from_secs(3)).unwrap();
        let observed = received.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(matches!(&observed, Err(error) if error.category == rtdb_types::AnomalyCategory::Interrupted), "{observed:?}");
        closer.join().unwrap(); worker.join().unwrap();
        assert!(!token.is_cancelled());
    }
    let statement = native.statement("SELECT 1");
    assert!(handles::statement(statement).unwrap().query(&token).unwrap().next().unwrap().is_some());
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
fn adbc_rejected_replacement_keeps_preparation_and_bind_until_valid_sql_is_installed() {
    let mut native = Native::open();
    let operation = native.operation();
    let statement = native.statement("SELECT ? AS value");
    let mut error = 0;
    ok(unsafe { rtdb_v1_statement_prepare(statement, operation, &mut error) }, error);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![17]))],
    ).unwrap();
    let mut schema = FFI_ArrowSchema::try_from(batch.schema().as_ref()).unwrap();
    let mut array = FFI_ArrowArray::new(&StructArray::from(batch).to_data());
    ok(unsafe { rtdb_v1_statement_bind(statement, &mut array, &mut schema, &mut error) }, error);

    for (sql, code) in [
        ("SELECT 1; SELECT 'unterminated", "xtdb/sql-error"),
        ("SHOW TIME ZONE; SELECT 1", "xtdb/sql-error"),
        ("DISCARD ALL", "xtdb/not-a-query"),
    ] {
        let status = unsafe { rtdb_v1_statement_set_sql(statement, sql.as_ptr(), sql.len(), &mut error) };
        incorrect(status, error, code);
    }
    for sql in ["BEGIN", "START TRANSACTION", "BEGIN READ ONLY", "COMMIT", "COMMIT SYNC", "ROLLBACK"] {
        let mut rejected = u64::MAX;
        let status = unsafe { rtdb_v1_statement_new(native.session, sql.as_ptr(), sql.len(), 0, &mut rejected, &mut error) };
        assert_eq!(rejected, 0);
        incorrect(status, error, "xtdb.adbc/tx-control-as-sql");
        let status = unsafe { rtdb_v1_statement_set_sql(statement, sql.as_ptr(), sql.len(), &mut error) };
        incorrect(status, error, "xtdb.adbc/tx-control-as-sql");
    }

    let mut parameters = FFI_ArrowSchema::empty();
    ok(unsafe { rtdb_v1_statement_schema(statement, operation, 1, &mut parameters, &mut error) }, error);
    assert_eq!(Schema::try_from(&parameters).unwrap().fields().len(), 1);
    assert_eq!(native.query_value(statement, operation), rtdb_types::Value::I64(17));

    for (sql, report) in [
        ("SELEC 1", "line 1:0 mismatched input 'SELEC' expecting {'(', 'ASSERT', "),
        ("SELECT 1 LIMIT ALL", "line 1:15 mismatched input 'ALL' expecting {UNSIGNED_INTEGER, POSTGRES_PARAMETER_SPECIFICATION, '?'}"),
        ("SELECT 1; SELEC 1", "line 1:10 mismatched input 'SELEC' expecting <EOF>"),
    ] {
        native.set_sql(statement, sql);
        let status = unsafe { rtdb_v1_statement_prepare(statement, operation, &mut error) };
        let anomaly = incorrect_anomaly(status, error, "xtdb/sql-error");
        let message = anomaly.message.as_deref().unwrap();
        assert!(message.starts_with(&format!("Errors parsing SQL statement:\n  - {report}")), "{message}");
        assert!(anomaly.context.is_empty());
    }

    native.set_sql(statement, "SELECT CAST(42 AS BIGINT) AS value");
    let mut schema = FFI_ArrowSchema::empty();
    let status = unsafe { rtdb_v1_statement_schema(statement, operation, 0, &mut schema, &mut error) };
    incorrect(status, error, "xtdb.adbc/not-prepared");
    for _ in 0..2 {
        ok(unsafe { rtdb_v1_statement_prepare(statement, operation, &mut error) }, error);
        assert_eq!(native.query_value(statement, operation), rtdb_types::Value::I64(42));
    }
}

#[test]
fn adbc_set_and_show_round_trip_after_the_caller_releases_the_sql() {
    let mut native = Native::open();
    let operation = native.operation();
    let statement = native.statement("");
    let mut error = 0;

    let sql = String::from("SET datestyle = 'ISO'");
    native.set_sql(statement, &sql);
    drop(sql);
    let mut schema = FFI_ArrowSchema::empty();
    ok(unsafe { rtdb_v1_statement_schema(statement, operation, 0, &mut schema, &mut error) }, error);
    assert!(Schema::try_from(&schema).unwrap().fields().is_empty());
    drop(schema);
    let mut affected = 0;
    ok(unsafe { rtdb_v1_statement_update(statement, operation, &mut affected, &mut error) }, error);
    assert_eq!(affected, -1);

    let sql = String::from("SHOW datestyle");
    native.set_sql(statement, &sql);
    drop(sql);
    let mut schema = FFI_ArrowSchema::empty();
    let status = unsafe { rtdb_v1_statement_schema(statement, operation, 0, &mut schema, &mut error) };
    incorrect(status, error, "xtdb.adbc/not-prepared");
    ok(unsafe { rtdb_v1_statement_prepare(statement, operation, &mut error) }, error);
    ok(unsafe { rtdb_v1_statement_schema(statement, operation, 0, &mut schema, &mut error) }, error);
    let schema = Schema::try_from(&schema).unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "datestyle");
    assert_eq!(native.query_value(statement, operation), rtdb_types::Value::Utf8("ISO".into()));
}

#[test]
fn adbc_prepared_schemas_admit_positional_fields_and_unknown_binding_types() {
    let mut native = Native::open_yaml(
        b"server: null\nflightSql: null\nnodeId: ffi-schema-budget\nindexer: {enabled: false}\ncompactor: {threads: 0}\ngarbageCollector: {enabled: false}\n",
    );
    let operation = native.operation();
    let statement = native.statement("SELECT COALESCE(?, CAST(42 AS BIGINT)) AS typed_value, ? AS unspecified_value");
    let mut error = 0;
    ok(unsafe { rtdb_v1_statement_prepare(statement, operation, &mut error) }, error);

    let session = handles::session(native.session).unwrap();
    let memory = session.region.memory();
    let baseline = native.memory_used();
    let headroom = memory.limit_bytes() - baseline;
    let type_bytes = 2 * std::mem::size_of::<rtdb_types::LogicalType>();
    let parameter_bytes = 4096 + 16 * type_bytes;
    let pressure = memory.reserve(headroom - parameter_bytes + 1).unwrap();
    let mut parameters = FFI_ArrowSchema::empty();
    let status = unsafe { rtdb_v1_statement_schema(statement, operation, 1, &mut parameters, &mut error) };
    incorrect(status, error, "rtdb/memory-budget-exceeded");
    assert!(parameters.release.is_none());
    assert_eq!(memory.limit_bytes() - native.memory_used(), parameter_bytes - 1);
    drop(pressure);

    ok(unsafe { rtdb_v1_statement_schema(statement, operation, 1, &mut parameters, &mut error) }, error);
    let fields = Schema::try_from(&parameters).unwrap();
    assert_eq!(fields.fields().len(), 2);
    for field in fields.fields() {
        assert_eq!(field.name(), "");
        assert_eq!(field.data_type(), &DataType::Null);
        assert!(field.is_nullable());
    }
    assert_eq!(native.memory_used() - baseline, arrow::schema_bound(&fields).unwrap());
    drop(parameters);
    assert_eq!(native.memory_used(), baseline);

    let token = handles::cancellation(operation, &session).unwrap();
    let context = session.region.run(async {
        let mut connection = session.state.lock();
        connection.prepare_context_for("xtdb", &token).await
    }).unwrap();
    let context_bytes = native.memory_used() - baseline;
    assert!(context_bytes > 0);
    drop(context);
    assert_eq!(native.memory_used(), baseline);

    let pressure = memory.reserve(headroom - context_bytes - type_bytes + 1).unwrap();
    let mut result = FFI_ArrowSchema::empty();
    let status = unsafe { rtdb_v1_statement_schema(statement, operation, 0, &mut result, &mut error) };
    let Handle::Error(diagnostic) = handles::get(error).unwrap() else { panic!("expected diagnostic") };
    let request = format!("Cannot reserve {type_bytes} bytes from budget");
    assert!(diagnostic.error.message.as_deref().is_some_and(|message| message.starts_with(&request)));
    incorrect(status, error, "rtdb/memory-budget-exceeded");
    assert!(result.release.is_none());
    assert_eq!(memory.limit_bytes() - native.memory_used(), context_bytes + type_bytes - 1);
    drop(pressure);

    ok(unsafe { rtdb_v1_statement_schema(statement, operation, 0, &mut result, &mut error) }, error);
    let fields = Schema::try_from(&result).unwrap();
    assert_eq!(fields.fields().len(), 2);
    assert_eq!(fields.field(0).name(), "typed_value");
    assert_eq!(fields.field(0).data_type(), &DataType::Int64);
    assert_eq!(fields.field(1).name(), "unspecified_value");
    assert_eq!(fields.field(1).data_type(), &DataType::Null);
    let kept = native.memory_used() - baseline - arrow::schema_bound(&fields).unwrap();
    assert!(kept > 0, "the statement keeps the plan it bound");
    drop(result);
    assert_eq!(native.memory_used(), baseline + kept);
}

#[test]
fn adbc_prepared_dml_schema_returns_no_result_fields_without_a_read_context() {
    let mut native = Native::open();
    let operation = native.operation();
    let statement = native.statement("INSERT INTO docs (_id) VALUES (?)");
    let mut error = 0;
    ok(unsafe { rtdb_v1_statement_prepare(statement, operation, &mut error) }, error);
    ok(unsafe { rtdb_v1_cancel(operation, &mut error) }, error);

    let mut result = FFI_ArrowSchema::empty();
    ok(unsafe { rtdb_v1_statement_schema(statement, operation, 0, &mut result, &mut error) }, error);
    assert!(Schema::try_from(&result).unwrap().fields().is_empty());
    let mut parameters = FFI_ArrowSchema::empty();
    ok(unsafe { rtdb_v1_statement_schema(statement, operation, 1, &mut parameters, &mut error) }, error);
    let schema = Schema::try_from(&parameters).unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "");
    assert_eq!(schema.field(0).data_type(), &DataType::Null);
    assert!(schema.field(0).is_nullable());
}

#[test]
fn adbc_statement_replacement_keeps_the_previous_lease_during_admission() {
    let mut native = Native::open_yaml(
        b"server: null\nflightSql: null\nnodeId: ffi-source-budget\nindexer: {enabled: false}\ncompactor: {threads: 0}\ngarbageCollector: {enabled: false}\n",
    );
    let operation = native.operation();
    let current_sql = "SHOW application_name";
    let replacement_sql = "SELECT 1";
    let statement = native.statement(current_sql);
    let memory = Arc::clone(handles::database(native.database).unwrap().0.memory());

    let calibration = MemoryBudget::new(1 << 20);
    assert!(rtdb_sql::parse_admitted_control_in(replacement_sql, &Default::default(), &calibration).unwrap().is_none());
    let lexical_peak = calibration.peak_bytes();
    assert_eq!(calibration.used_bytes(), 0);
    let control = rtdb_sql::parse_admitted_control_in(current_sql, &Default::default(), &calibration).unwrap().unwrap();
    let control_bytes = calibration.used_bytes();
    drop(control);
    assert_eq!(calibration.used_bytes(), 0);
    let sql_bytes = replacement_sql.len() + 256;
    assert!(lexical_peak > sql_bytes);
    assert!(control_bytes > lexical_peak);

    let with_control = native.memory_used();
    let headroom = memory.limit_bytes() - with_control;
    assert!(headroom >= lexical_peak);
    let pressure = memory.reserve(headroom - lexical_peak + 1).unwrap();
    let under_pressure = native.memory_used();
    assert_eq!(memory.limit_bytes() - under_pressure, lexical_peak - 1);
    let mut error = 0;
    let status = unsafe { rtdb_v1_statement_set_sql(statement, replacement_sql.as_ptr(), replacement_sql.len(), &mut error) };
    incorrect(status, error, "rtdb/memory-budget-exceeded");
    assert_eq!(native.memory_used(), under_pressure);
    drop(pressure);
    assert_eq!(native.memory_used(), with_control);

    ok(unsafe { rtdb_v1_statement_prepare(statement, operation, &mut error) }, error);
    let mut schema = FFI_ArrowSchema::empty();
    ok(unsafe { rtdb_v1_statement_schema(statement, operation, 0, &mut schema, &mut error) }, error);
    assert_eq!(Schema::try_from(&schema).unwrap().field(0).name(), "application_name");
    drop(schema);
    assert_eq!(native.memory_used(), with_control);

    let pressure = memory.reserve(headroom - lexical_peak).unwrap();
    assert_eq!(memory.limit_bytes() - native.memory_used(), lexical_peak);
    native.set_sql(statement, replacement_sql);
    assert_eq!(native.memory_used(), memory.limit_bytes() - lexical_peak - control_bytes + sql_bytes);
    drop(pressure);
    assert_eq!(native.memory_used(), with_control - control_bytes + sql_bytes);
    ok(unsafe { rtdb_v1_close(statement, &mut error) }, error);
    assert_eq!(native.memory_used(), with_control - control_bytes);

    let bulk_baseline = native.memory_used();
    let ignored_prefix = format!("SELECT '{}.public.items", "x".repeat(16_384));
    let mut bulk_bytes = None;
    for target in ["items", "public.items", ignored_prefix.as_str()] {
        let mut bulk = 0;
        ok(unsafe { rtdb_v1_statement_new(native.session, target.as_ptr(), target.len(), 1, &mut bulk, &mut error) }, error);
        let retained = native.memory_used() - bulk_baseline;
        assert_eq!(*bulk_bytes.get_or_insert(retained), retained);
        ok(unsafe { rtdb_v1_close(bulk, &mut error) }, error);
        assert_eq!(retained - (native.memory_used() - bulk_baseline), "public".len() + "items".len() + 256);
        ok(unsafe { rtdb_v1_release(bulk, &mut error) }, error);
        assert_eq!(native.memory_used(), bulk_baseline);
    }
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
    let value = rtdb_types::Value::Struct(StructFields::from([("missing".into(), rtdb_types::Value::Null), ("small".into(), rtdb_types::Value::I8(5))]));
    let wire = transit::encode(&value).unwrap();
    let decoded = rtdb_pg_codec::from_transit(rtdb_format::transit::decode_json(&wire).unwrap()).unwrap();
    let rtdb_types::Value::Struct(fields) = decoded else { panic!() };
    assert!(!fields.contains_key("missing")); assert_eq!(fields["small"], rtdb_types::Value::I8(5));
}

#[test]
fn clojure_result_transit_encodes_doubles_without_precision_loss() {
    use rtdb_format::transit::{TransitValue as T, decode_json};
    use rtdb_types::Value;
    let value = 0.19736842105263158_f64;
    let wire = [b"[".as_slice(), &transit::encode_row(&Value::F64(value)).unwrap(), b",", &transit::encode_row(&Value::F64(-0.0)).unwrap(), b"]"].concat();
    let T::Array(values) = decode_json(&wire).unwrap() else { panic!("Expected rows") };
    let [T::Float(first), T::Float(second)] = values.as_slice() else { panic!("Expected double values") };
    assert_eq!(first.to_bits(), value.to_bits());
    assert_eq!(second.to_bits(), (-0.0_f64).to_bits());
}
