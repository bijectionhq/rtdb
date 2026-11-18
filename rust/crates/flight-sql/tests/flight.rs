use std::{sync::{Arc, atomic::{AtomicI64, Ordering}}, time::Duration};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_flight::{Action, FlightDescriptor, FlightInfo, flight_service_client::FlightServiceClient, sql::{Any, ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, ActionEndTransactionRequest, CommandGetCatalogs, CommandGetSqlInfo, CommandGetTables, CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementIngest, CommandStatementQuery, CommandStatementUpdate, EndTransaction, ProstMessageExt}};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use prost::Message;
use rtdb_auth::{AuthConfig, Authenticator, HttpRequest, HttpResponse, HttpTransport, Secret};
use rtdb_columnar::MemoryBudget;
use rtdb_flight_sql::{FlightConfig, ServerDriver, ServerHandle};
use rtdb_maintenance::CollectionObserver;
use rtdb_node::{Clock, Node, NodeOptions, NodeServices};
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows};
use rtdb_types::{Anomaly, Result, TableRef, Timestamp};
use tonic::{Request, Status, metadata::MetadataMap, transport::Channel};

struct NoHttp;
#[async_trait]
impl HttpTransport for NoHttp { async fn request(&self, _: HttpRequest) -> Result<HttpResponse> { Err(Anomaly::fault("test/http", "Unexpected HTTP request")) } }
struct Observer;
impl CollectionObserver for Observer { fn table_failed(&self, _: Option<&TableRef>, error: &Anomaly) { panic!("Unexpected collection failure: {error}"); } }
struct Metrics;
impl CatalogMetrics for Metrics { fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) } }
struct TickClock(AtomicI64);
impl Clock for TickClock { fn now(&self) -> Timestamp { Timestamp(self.0.fetch_add(1000, Ordering::Relaxed)) } }
struct ManualClock(AtomicI64);
impl Clock for ManualClock { fn now(&self) -> Timestamp { Timestamp(self.0.load(Ordering::Relaxed)) } }
struct Running { node: Node, node_task: tokio::task::JoinHandle<Result<()>>, server: ServerHandle, server_task: tokio::task::JoinHandle<Result<()>>, clock: Arc<ManualClock> }
impl Running {
    async fn start(password: Option<&str>, mut config: FlightConfig) -> Self {
        let node_clock: Arc<dyn Clock> = Arc::new(TickClock(AtomicI64::new(1_000_000)));
        let auth = Authenticator::open(AuthConfig::SingleRootUser { password: password.map(Secret::new) }, MemoryBudget::new(64 << 20), node_clock.clone(), Arc::new(NoHttp)).await.unwrap();
        let services = NodeServices { clock: node_clock, ..NodeServices::local(auth.clone(), Arc::new(Observer), auth.clone(), Arc::new(Metrics)) };
        let mut options = NodeOptions::default(); options.automatic_compaction = false; options.automatic_gc = false; options.replication.assertion_interval = Duration::from_millis(1); options.replication.election_seed = 1;
        let (node, driver) = Node::open(options, services).await.unwrap(); let node_task = tokio::spawn(driver.run());
        config.address = "127.0.0.1:0".parse().unwrap(); let clock = Arc::new(ManualClock(AtomicI64::new(0)));
        let (server, driver) = ServerDriver::bind_with_clock(config, node.clone(), auth, clock.clone()).await.unwrap(); let server_task = tokio::spawn(driver.run());
        Self { node, node_task, server, server_task, clock }
    }
    async fn stop(self) {
        self.server.shutdown(); tokio::time::timeout(Duration::from_secs(10), self.server_task).await.unwrap().unwrap().unwrap();
        assert!(self.server.status().stopped); assert_eq!(self.server.status().active_calls, 0);
        self.node.shutdown(); tokio::time::timeout(Duration::from_secs(10), self.node_task).await.unwrap().unwrap().unwrap();
    }
}
struct Client { client: FlightServiceClient<Channel>, headers: MetadataMap }
impl Client {
    async fn connect(server: &ServerHandle, basic: Option<&str>) -> Self {
        let channel = Channel::from_shared(format!("http://{}", server.address())).unwrap().connect().await.unwrap();
        let client = FlightServiceClient::new(channel); let mut this = Self { client, headers: MetadataMap::new() };
        if let Some(basic) = basic { use base64::Engine; this.headers.insert("authorization", format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(basic)).parse().unwrap()); }
        let request = this.request(futures::stream::iter(vec![arrow_flight::HandshakeRequest::default()]));
        let response = this.client.handshake(request).await.unwrap();
        if let Some(cookie) = response.metadata().get("set-cookie") { this.headers.insert("cookie", cookie.clone()); }
        if let Some(auth) = response.metadata().get("authorization") { this.headers.insert("authorization", auth.clone()); }
        let _: Vec<_> = response.into_inner().try_collect().await.unwrap(); this
    }
    fn request<T>(&self, value: T) -> Request<T> { let mut request = Request::new(value); *request.metadata_mut() = self.headers.clone(); request }
    async fn action(&mut self, name: &str, body: Bytes) -> std::result::Result<Vec<arrow_flight::Result>, Status> {
        let request = self.request(Action { r#type: name.into(), body }); self.client.do_action(request).await?.into_inner().try_collect().await
    }
    async fn begin(&mut self) -> Bytes {
        let response = self.action("BeginTransaction", ActionBeginTransactionRequest {}.as_any().encode_to_vec().into()).await.unwrap();
        Any::decode(response[0].body.as_ref()).unwrap().unpack::<ActionBeginTransactionResult>().unwrap().unwrap().transaction_id
    }
    async fn end(&mut self, handle: Bytes, commit: bool) -> std::result::Result<(), Status> {
        self.action("EndTransaction", ActionEndTransactionRequest { transaction_id: handle, action: if commit { EndTransaction::Commit } else { EndTransaction::Rollback } as i32 }.as_any().encode_to_vec().into()).await.map(|_| ())
    }
    async fn info(&mut self, sql: &str, transaction_id: Option<Bytes>) -> FlightInfo {
        let descriptor = FlightDescriptor::new_cmd(CommandStatementQuery { query: sql.into(), transaction_id }.as_any().encode_to_vec());
        let request = self.request(descriptor); self.client.get_flight_info(request).await.unwrap().into_inner()
    }
    async fn redeem(&mut self, info: &FlightInfo) -> Vec<RecordBatch> {
        let request = self.request(info.endpoint[0].ticket.clone().unwrap()); let input = self.client.do_get(request).await.unwrap().into_inner();
        arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(input.map_err(Into::into)).try_collect().await.unwrap()
    }
    async fn query(&mut self, sql: &str) -> Vec<RecordBatch> { let info = self.info(sql, None).await; self.redeem(&info).await }
    async fn update(&mut self, sql: &str, tx: Option<Bytes>) {
        let descriptor = FlightDescriptor::new_cmd(CommandStatementUpdate { query: sql.into(), transaction_id: tx }.as_any().encode_to_vec());
        let data = arrow_flight::FlightData { flight_descriptor: Some(descriptor), ..Default::default() };
        let request = self.request(futures::stream::iter(vec![data])); let output: Vec<_> = self.client.do_put(request).await.unwrap().into_inner().try_collect().await.unwrap();
        assert_eq!(arrow_flight::sql::DoPutUpdateResult::decode(output[0].app_metadata.as_ref()).unwrap().record_count, -1);
    }
    async fn prepare(&mut self, sql: &str, transaction_id: Option<Bytes>) -> ActionCreatePreparedStatementResult {
        let response = self.action("CreatePreparedStatement", ActionCreatePreparedStatementRequest { query: sql.into(), transaction_id }.as_any().encode_to_vec().into()).await.unwrap();
        Any::decode(response[0].body.as_ref()).unwrap().unpack().unwrap().unwrap()
    }
    async fn upload<M: ProstMessageExt>(&mut self, command: M, batches: Vec<RecordBatch>) {
        let schema = batches[0].schema(); let mut data = arrow_flight::utils::batches_to_flight_data(schema.as_ref(), batches).unwrap();
        data[0].flight_descriptor = Some(FlightDescriptor::new_cmd(command.as_any().encode_to_vec()));
        let request = self.request(futures::stream::iter(data)); let _: Vec<_> = self.client.do_put(request).await.unwrap().into_inner().try_collect().await.unwrap();
    }
}
fn integer(batch: &RecordBatch, column: usize, row: usize) -> i64 { batch.column(column).as_any().downcast_ref::<Int64Array>().unwrap().value(row) }
fn arguments(values: Vec<i64>) -> RecordBatch { RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("any-client-name", DataType::Int64, false)])), vec![Arc::new(Int64Array::from(values))]).unwrap() }

#[tokio::test]
async fn unsigned_parameter_fields_and_bits_admit_signed_query_and_dml_values() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let unsigned = |value| RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("any-name", DataType::UInt64, true)])), vec![Arc::new(arrow_array::UInt64Array::from(vec![value]))]).unwrap();
    let query = client.prepare("SELECT ? AS n", None).await;
    for value in [Some(u64::MAX), None] {
        client.upload(CommandPreparedStatementQuery { prepared_statement_handle: query.prepared_statement_handle.clone() }, vec![unsigned(value)]).await;
        let descriptor = FlightDescriptor::new_cmd(CommandPreparedStatementQuery { prepared_statement_handle: query.prepared_statement_handle.clone() }.as_any().encode_to_vec());
        let request = client.request(descriptor);
        let info = client.client.get_flight_info(request).await.unwrap().into_inner();
        assert_eq!(info.clone().try_decode_schema().unwrap().field(0).data_type(), &DataType::UInt64);
        let rows = client.redeem(&info).await;
        assert_eq!(rows[0].schema().field(0).data_type(), &DataType::Int64);
        let expected = value.map(|_| rtdb_types::Value::I64(-1)).unwrap_or(rtdb_types::Value::Null);
        assert_eq!(rtdb_columnar::read_value(rows[0].schema().field(0), rows[0].column(0).as_ref(), 0).unwrap(), expected);
    }
    let update = client.prepare("INSERT INTO docs (_id,v) VALUES (1,?)", None).await;
    client.upload(CommandPreparedStatementUpdate { prepared_statement_handle: update.prepared_statement_handle }, vec![unsigned(Some(u64::MAX))]).await;
    assert_eq!(integer(&client.query("SELECT v FROM docs").await[0], 0, 0), -1);
    let bulk = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("_id", DataType::UInt64, false), Field::new("v", DataType::UInt64, false)])), vec![Arc::new(arrow_array::UInt64Array::from(vec![u64::MAX])), Arc::new(arrow_array::UInt64Array::from(vec![u64::MAX]))]).unwrap();
    client.upload(CommandStatementIngest { table: "bulk_unsigned".into(), ..Default::default() }, vec![bulk]).await;
    let rows = client.query("SELECT _id,v FROM bulk_unsigned").await;
    assert_eq!((integer(&rows[0], 0, 0), integer(&rows[0], 1, 0)), (-1, -1));
    running.stop().await;
}

#[tokio::test]
async fn ticket_basis_is_immutable_and_update_ticket_redemption_is_effect_free() {
    let running = Running::start(None, FlightConfig::default()).await; let mut client = Client::connect(&running.server, None).await;
    let update = client.info("INSERT INTO docs (_id,n) VALUES (1,10)", None).await; assert!(client.redeem(&update).await.is_empty()); assert!(client.redeem(&update).await.is_empty());
    let before = client.info("SELECT count(*) AS n FROM docs", None).await; client.update("INSERT INTO docs (_id,n) VALUES (2,20)", None).await;
    assert_eq!(integer(&client.redeem(&before).await[0], 0, 0), 1); assert_eq!(integer(&client.query("SELECT count(*) AS n FROM docs").await[0], 0, 0), 2);
    running.stop().await;
}

#[tokio::test]
async fn prepared_update_info_buffers_the_bound_operation_once() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    client.update("INSERT INTO docs (_id,n) VALUES (1,10)", None).await;
    let transaction = client.begin().await;
    let prepared = client.prepare("UPDATE docs SET n = n + ? WHERE _id = 1", Some(transaction.clone())).await;
    let command = CommandPreparedStatementQuery { prepared_statement_handle: prepared.prepared_statement_handle };
    client.upload(command.clone(), vec![arguments(vec![2])]).await;
    let descriptor = FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
    let request = client.request(descriptor);
    let info = client.client.get_flight_info(request).await.unwrap().into_inner();
    assert!(info.clone().try_decode_schema().unwrap().fields().is_empty());
    assert!(client.redeem(&info).await.is_empty());
    assert!(client.redeem(&info).await.is_empty());
    client.end(transaction, true).await.unwrap();
    assert_eq!(integer(&client.query("SELECT n FROM docs").await[0], 0, 0), 12);
    running.stop().await;
}

#[tokio::test]
async fn query_info_pins_read_only_and_announced_schema_matches_redeemed_rows() {
    let running = Running::start(None, FlightConfig::default()).await; let mut client = Client::connect(&running.server, None).await;
    client.update("INSERT INTO docs (_id,n) VALUES (1,10)", None).await;
    let transaction = client.begin().await; let info = client.info("SELECT * FROM docs", Some(transaction.clone())).await;
    let announced = info.clone().try_decode_schema().unwrap(); let rows = client.redeem(&info).await;
    assert_eq!(rows[0].schema().as_ref(), &announced);
    let descriptor = FlightDescriptor::new_cmd(CommandStatementUpdate { query: "INSERT INTO docs (_id,n) VALUES (2,20)".into(), transaction_id: Some(transaction.clone()) }.as_any().encode_to_vec());
    let request = client.request(futures::stream::iter(vec![arrow_flight::FlightData { flight_descriptor: Some(descriptor), ..Default::default() }]));
    let result = match client.client.do_put(request).await { Ok(response) => response.into_inner().try_collect::<Vec<_>>().await.map(|_| ()), Err(error) => Err(error) };
    assert!(result.is_err()); client.end(transaction, false).await.unwrap(); running.stop().await;
}

#[tokio::test]
async fn prepared_parameters_are_positional_all_batches_update_and_old_ticket_keeps_old_bind() {
    let running = Running::start(None, FlightConfig::default()).await; let mut client = Client::connect(&running.server, None).await;
    let update = client.prepare("INSERT INTO docs (_id) VALUES (?)", None).await;
    client.upload(CommandPreparedStatementUpdate { prepared_statement_handle: update.prepared_statement_handle }, vec![arguments(vec![1, 2]), arguments(vec![3])]).await;
    assert_eq!(integer(&client.query("SELECT count(*) AS n FROM docs").await[0], 0, 0), 3);
    let query = client.prepare("SELECT ? AS n", None).await;
    client.upload(CommandPreparedStatementQuery { prepared_statement_handle: query.prepared_statement_handle.clone() }, vec![arguments(vec![41])]).await;
    let descriptor = FlightDescriptor::new_cmd(CommandPreparedStatementQuery { prepared_statement_handle: query.prepared_statement_handle.clone() }.as_any().encode_to_vec());
    let request = client.request(descriptor); let old = client.client.get_flight_info(request).await.unwrap().into_inner();
    client.upload(CommandPreparedStatementQuery { prepared_statement_handle: query.prepared_statement_handle }, vec![arguments(vec![42])]).await;
    assert_eq!(integer(&client.redeem(&old).await[0], 0, 0), 41); running.stop().await;
}

#[tokio::test]
async fn authenticated_handles_are_session_scoped_but_query_tickets_can_be_redeemed_by_another_caller() {
    let running = Running::start(Some("correct"), FlightConfig::default()).await;
    let mut first = Client::connect(&running.server, Some("xtdb:correct")).await; let mut second = Client::connect(&running.server, Some("xtdb:correct")).await;
    let handle = first.begin().await; assert_eq!(second.end(handle.clone(), true).await.unwrap_err().code(), tonic::Code::NotFound);
    first.end(handle, false).await.unwrap(); let info = first.info("SELECT 1::bigint AS n", None).await;
    assert_eq!(integer(&second.redeem(&info).await[0], 0, 0), 1); running.stop().await;
}

#[tokio::test]
async fn anonymous_transaction_survives_issuer_session_close_and_can_be_claimed_once() {
    let running = Running::start(None, FlightConfig::default()).await; let mut first = Client::connect(&running.server, None).await; let mut second = Client::connect(&running.server, None).await;
    let handle = first.begin().await; first.update("INSERT INTO docs (_id) VALUES (1)", Some(handle.clone())).await;
    first.action("CloseSession", Bytes::new()).await.unwrap(); second.end(handle.clone(), true).await.unwrap();
    assert_eq!(second.end(handle, true).await.unwrap_err().code(), tonic::Code::NotFound); assert_eq!(integer(&second.query("SELECT count(*) AS n FROM docs").await[0], 0, 0), 1); running.stop().await;
}

#[tokio::test]
async fn handle_expiry_is_checked_during_lookup_before_sweeper_runs() {
    let config = FlightConfig { transaction_idle_timeout: Duration::from_millis(1), sweep_interval: Duration::from_secs(3600), ..Default::default() };
    let running = Running::start(None, config).await; let mut client = Client::connect(&running.server, None).await; let tx = client.begin().await;
    running.clock.0.store(1000, Ordering::Relaxed); let error = client.end(tx, true).await.unwrap_err(); assert_eq!(error.code(), tonic::Code::NotFound); assert!(error.message().contains("PT0.001S")); running.stop().await;
}

#[tokio::test]
async fn bulk_ingestion_uses_each_arrow_batch_and_catalog_metadata_uses_dense_union_offsets() {
    let running = Running::start(None, FlightConfig::default()).await; let mut client = Client::connect(&running.server, None).await;
    let schema = Arc::new(Schema::new(vec![Field::new("_id", DataType::Int64, false)]));
    let batches = [1, 2].into_iter().map(|id| RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![id]))]).unwrap()).collect();
    client.upload(CommandStatementIngest { table: "docs".into(), ..Default::default() }, batches).await;
    assert_eq!(integer(&client.query("SELECT count(*) AS n FROM docs").await[0], 0, 0), 2);
    let request = client.request(FlightDescriptor::new_cmd(CommandGetTables { catalog: Some("xtdb".into()), db_schema_filter_pattern: Some("public".into()), table_name_filter_pattern: Some("docs".into()), table_types: Vec::new(), include_schema: true }.as_any().encode_to_vec()));
    let info = client.client.get_flight_info(request).await.unwrap().into_inner(); let tables = client.redeem(&info).await; assert_eq!(tables[0].num_rows(), 1); assert_eq!(tables[0].num_columns(), 5);
    let request = client.request(FlightDescriptor::new_cmd(CommandGetSqlInfo { info: Vec::new() }.as_any().encode_to_vec()));
    let info = client.client.get_flight_info(request).await.unwrap().into_inner(); let batches = client.redeem(&info).await;
    let union = batches[0].column(1).as_any().downcast_ref::<arrow_array::UnionArray>().unwrap(); assert_eq!(union.type_ids().as_ref(), &[0, 0, 3, 3, 3]); assert_eq!(union.offsets().unwrap().as_ref(), &[0, 1, 0, 1, 2]); running.stop().await;
}

#[tokio::test]
async fn shutdown_joins_a_call_waiting_for_its_first_upload_message() {
    let running = Running::start(None, FlightConfig::default()).await; let mut client = Client::connect(&running.server, None).await;
    let request = client.request(futures::stream::pending::<arrow_flight::FlightData>()); let upload = tokio::spawn(async move { client.client.do_put(request).await });
    tokio::task::yield_now().await; running.stop().await;
    assert!(tokio::time::timeout(Duration::from_secs(2), upload).await.unwrap().unwrap().is_err());
}

#[tokio::test]
async fn bearer_cookie_disagreement_and_replayed_different_basic_header_are_rejected_without_leaking_calls() {
    let running = Running::start(Some("correct"), FlightConfig::default()).await;
    let mut first = Client::connect(&running.server, Some("xtdb:correct")).await; let second = Client::connect(&running.server, Some("xtdb:correct")).await;
    let descriptor = FlightDescriptor::new_cmd(CommandGetCatalogs {}.as_any().encode_to_vec());
    let mut request = first.request(descriptor.clone()); request.metadata_mut().insert("cookie", second.headers.get("cookie").unwrap().clone());
    assert_eq!(first.client.get_flight_info(request).await.unwrap_err().code(), tonic::Code::Unauthenticated);
    let mut request = first.request(descriptor); use base64::Engine;
    request.metadata_mut().insert("authorization", format!("basic {}", base64::engine::general_purpose::STANDARD.encode("xtdb:correct")).parse().unwrap());
    assert_eq!(first.client.get_flight_info(request).await.unwrap_err().code(), tonic::Code::Unauthenticated);
    assert_eq!(integer(&first.query("SELECT 42::bigint AS n").await[0], 0, 0), 42); running.stop().await;
}

fn assert_initial_sql_diagnostic(error: &Status, diagnostic: &str) {
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let prefix = format!("Errors parsing SQL statement:\n  - line 1:0 {diagnostic} expecting ");
    assert!(error.message().starts_with(&prefix), "{}", error.message());
    let wire = error.metadata().get_bin("xtdb-anomaly-bin").unwrap().to_bytes().unwrap();
    let detail: serde_json::Value = serde_json::from_slice(&wire).unwrap();
    assert_eq!(detail["category"], "incorrect");
    assert_eq!(detail["code"], "xtdb/sql-error");
    assert_eq!(detail["message"], error.message());
    assert!(detail.get("cause").is_none());
    assert!(detail.get("data").is_none());
}

#[tokio::test]
async fn initial_statement_reports_use_direct_parser_anomaly_metadata_on_remote_flight() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    for (sql, diagnostic) in [
        ("SELEC 1", "mismatched input 'SELEC'"),
        ("extra SELECT 1", "extraneous input 'extra'"),
    ] {
        let command = CommandStatementQuery { query: sql.into(), transaction_id: None };
        let descriptor = FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
        let request = client.request(descriptor);
        let error = client.client.get_flight_info(request).await.unwrap_err();
        assert_initial_sql_diagnostic(&error, diagnostic);
    }
    assert_eq!(integer(&client.query("SELECT 42::bigint AS answer").await[0], 0, 0), 42);
    running.stop().await;
}

#[tokio::test]
async fn schema_and_prepare_report_direct_parser_errors_and_keep_the_flight_session() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    for (sql, diagnostic) in [
        ("SELEC 1", "mismatched input 'SELEC'"),
        ("extra SELECT 1", "extraneous input 'extra'"),
    ] {
        let command = CommandStatementQuery { query: sql.into(), transaction_id: None };
        let request = client.request(FlightDescriptor::new_cmd(command.as_any().encode_to_vec()));
        let error = client.client.get_schema(request).await.unwrap_err();
        assert_initial_sql_diagnostic(&error, diagnostic);

        let request = ActionCreatePreparedStatementRequest { query: sql.into(), transaction_id: None };
        let error = client.action("CreatePreparedStatement", request.as_any().encode_to_vec().into()).await.unwrap_err();
        assert_initial_sql_diagnostic(&error, diagnostic);
    }

    let sql = "SELECT 42::bigint AS answer";
    let command = CommandStatementQuery { query: sql.into(), transaction_id: None };
    let request = client.request(FlightDescriptor::new_cmd(command.as_any().encode_to_vec()));
    let schema = Schema::try_from(client.client.get_schema(request).await.unwrap().into_inner()).unwrap();
    assert_eq!(schema.fields()[0].name(), "answer");
    assert_eq!(schema.fields()[0].data_type(), &DataType::Int64);
    assert!(!client.prepare(sql, None).await.prepared_statement_handle.is_empty());
    running.stop().await;
}

#[tokio::test]
async fn get_schema_keeps_the_existing_later_statement_error_and_span() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let command = CommandStatementQuery { query: "SELECT 1; SELEC 1".into(), transaction_id: None };
    let request = client.request(FlightDescriptor::new_cmd(command.as_any().encode_to_vec()));
    let error = client.client.get_schema(request).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().starts_with("sql parser error: Expected: an SQL statement, found: SELEC "));
    let wire = error.metadata().get_bin("xtdb-anomaly-bin").unwrap().to_bytes().unwrap();
    let detail: serde_json::Value = serde_json::from_slice(&wire).unwrap();
    assert_eq!(detail["category"], "incorrect");
    assert_eq!(detail["code"], "xtdb/sql-error");
    assert_eq!(detail["data"]["span"], serde_json::json!({"start": 10, "end": 15}));
    running.stop().await;
}

#[tokio::test]
async fn commented_controls_set_and_report_the_flight_session_timezone() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    client.update("/* outer /* nested */ comment */ SET TIME ZONE 'America/New_York';", None).await;

    let show = "SHOW TIME -- ;\n ZONE;";
    let command = CommandStatementQuery { query: show.into(), transaction_id: None };
    let request = client.request(FlightDescriptor::new_cmd(command.as_any().encode_to_vec()));
    let schema = Schema::try_from(client.client.get_schema(request).await.unwrap().into_inner()).unwrap();
    assert_eq!(schema.field(0).name(), "timezone");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    let info = client.info(show, None).await;
    assert_eq!(info.clone().try_decode_schema().unwrap(), schema);
    let timezone = |batches: &[RecordBatch]| {
        assert_eq!(batches[0].num_rows(), 1);
        let column = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        column.value(0).to_owned()
    };
    assert_eq!(timezone(&client.redeem(&info).await), "America/New_York");

    let update = client.info("SET TIME ZONE /* zone */ 'UTC';", None).await;
    assert!(client.redeem(&update).await.is_empty());
    let command = CommandStatementQuery { query: "SET TIME ZONE 'Asia/Tokyo' TRAILING".into(), transaction_id: None };
    let request = client.request(FlightDescriptor::new_cmd(command.as_any().encode_to_vec()));
    let error = client.client.get_flight_info(request).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let wire = error.metadata().get_bin("xtdb-anomaly-bin").unwrap().to_bytes().unwrap();
    let detail: serde_json::Value = serde_json::from_slice(&wire).unwrap();
    assert_eq!(detail["code"], "xtdb/sql-error");
    assert_eq!(timezone(&client.query(show).await), "UTC");
    running.stop().await;
}

#[tokio::test]
async fn write_transaction_rejects_queries_and_sql_transaction_control_names_the_api() {
    let running = Running::start(None, FlightConfig::default()).await; let mut client = Client::connect(&running.server, None).await;
    let tx = client.begin().await; client.update("INSERT INTO docs (_id) VALUES (1)", Some(tx.clone())).await;
    let request = client.request(FlightDescriptor::new_cmd(CommandStatementQuery { query: "SELECT 1".into(), transaction_id: Some(tx.clone()) }.as_any().encode_to_vec()));
    let error = client.client.get_flight_info(request).await.unwrap_err(); assert_eq!(error.code(), tonic::Code::InvalidArgument); assert!(error.metadata().get_bin("xtdb-anomaly-bin").is_some());
    client.end(tx, false).await.unwrap();
    let request = client.request(FlightDescriptor::new_cmd(CommandStatementQuery { query: "BEGIN".into(), transaction_id: None }.as_any().encode_to_vec()));
    let error = client.client.get_flight_info(request).await.unwrap_err(); assert!(error.message().contains("transaction API")); running.stop().await;
}

#[tokio::test]
async fn custom_commit_sends_result_with_await_token_and_durable_transaction_identity() {
    let running = Running::start(None, FlightConfig::default()).await; let mut client = Client::connect(&running.server, None).await;
    let tx = client.begin().await; client.update("INSERT INTO docs (_id) VALUES (1)", Some(tx.clone())).await;
    let results = client.action("xtdb.CommitTransaction", tx).await.unwrap(); assert_eq!(results.len(), 1);
    let value: serde_json::Value = serde_json::from_slice(&results[0].body).unwrap(); assert_eq!(value["committed"], true); assert!(value["txId"].is_i64()); assert!(value["systemTime"].is_string()); assert!(value["awaitToken"].is_string()); assert!(value["error"].is_null()); running.stop().await;
}
