use std::{sync::{Arc, atomic::{AtomicI64, Ordering}}, time::Duration};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_flight::{Action, FlightDescriptor, FlightInfo, flight_service_client::FlightServiceClient, sql::{Any, ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, ActionEndTransactionRequest, CommandGetCatalogs, CommandGetSqlInfo, CommandGetTables, CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementIngest, CommandStatementQuery, CommandStatementUpdate, EndTransaction, ProstMessageExt}};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use prost::Message;
use rtdb_auth::{AuthConfig, Authenticator, HttpRequest, HttpResponse, HttpTransport, Secret};
use rtdb_columnar::MemoryBudget;
use rtdb_flight_sql::{FlightConfig, ServerDriver, ServerHandle};
use rtdb_maintenance::CollectionObserver;
use rtdb_node::{Clock, Membership, Node, NodeOptions, NodeServices};
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
    async fn start(password: Option<&str>, config: FlightConfig) -> Self { Self::start_with_placements(password, config, &[]).await }
    async fn start_with_placements(password: Option<&str>, mut config: FlightConfig, placements: &[&str]) -> Self {
        let node_clock: Arc<dyn Clock> = Arc::new(TickClock(AtomicI64::new(1_000_000)));
        let auth = Authenticator::open(AuthConfig::SingleRootUser { password: password.map(Secret::new) }, MemoryBudget::new(64 << 20), node_clock.clone(), Arc::new(NoHttp)).await.unwrap();
        let services = NodeServices { clock: node_clock, ..NodeServices::local(auth.clone(), Arc::new(Observer), auth.clone(), Arc::new(Metrics)) };
        let mut options = NodeOptions::default(); options.automatic_compaction = false; options.automatic_gc = false; options.replication.assertion_interval = Duration::from_millis(1); options.replication.election_seed = 1;
        options.owned_relations = config.owned_native_actions;
        if !placements.is_empty() { options.membership = Membership::Placed; }
        let (node, driver) = Node::open(options, services).await.unwrap(); let node_task = tokio::spawn(driver.run());
        for &database in placements { node.database_or_attach(database.into(), Default::default()).await.unwrap().unwrap(); }
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
        Self::connect_with_database(server, basic, None).await
    }
    async fn connect_with_database(server: &ServerHandle, basic: Option<&str>, database: Option<&str>) -> Self {
        let channel = Channel::from_shared(format!("http://{}", server.address())).unwrap().connect().await.unwrap();
        let client = FlightServiceClient::new(channel); let mut this = Self { client, headers: MetadataMap::new() };
        if let Some(basic) = basic { use base64::Engine; this.headers.insert("authorization", format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(basic)).parse().unwrap()); }
        if let Some(database) = database { this.headers.insert("x-xtdb-database", database.parse().unwrap()); }
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
async fn placed_flight_sessions_scope_catalogs_tickets_and_handles_to_their_attachment() {
    let running = Running::start_with_placements(None, FlightConfig::default(), &["first", "second"]).await;
    let mut first = Client::connect_with_database(&running.server, None, Some("first")).await;
    let mut second = Client::connect_with_database(&running.server, None, Some("second")).await;
    first.headers.remove("x-xtdb-database");
    second.headers.remove("x-xtdb-database");
    let catalogs = FlightDescriptor::new_cmd(CommandGetCatalogs {}.as_any().encode_to_vec());
    let first_catalogs = first.client.get_flight_info(first.request(catalogs.clone())).await.unwrap().into_inner();
    let batches = first.redeem(&first_catalogs).await;
    let names = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(names.iter().collect::<Vec<_>>(), [Some("first")]);

    let mut switched = first.request(catalogs.clone());
    switched.metadata_mut().insert("x-xtdb-database", "second".parse().unwrap());
    assert_eq!(first.client.get_flight_info(switched).await.unwrap_err().code(), tonic::Code::PermissionDenied);

    let info = first.info("SELECT 42::BIGINT AS n", None).await;
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    assert_eq!(second.client.do_get(second.request(ticket)).await.unwrap_err().code(), tonic::Code::PermissionDenied);
    assert_eq!(integer(&first.redeem(&info).await[0], 0, 0), 42);

    let transaction = first.begin().await;
    assert_eq!(second.end(transaction.clone(), false).await.unwrap_err().code(), tonic::Code::NotFound);
    first.end(transaction, false).await.unwrap();

    let prepared = first.prepare("SELECT 7::BIGINT AS n", None).await.prepared_statement_handle;
    let statement = FlightDescriptor::new_cmd(CommandPreparedStatementQuery { prepared_statement_handle: prepared.clone() }.as_any().encode_to_vec());
    assert_eq!(second.client.get_flight_info(second.request(statement.clone())).await.unwrap_err().code(), tonic::Code::NotFound);
    second.action("ClosePreparedStatement", ActionClosePreparedStatementRequest { prepared_statement_handle: prepared }.as_any().encode_to_vec().into()).await.unwrap();
    let result = first.client.get_flight_info(first.request(statement)).await.unwrap().into_inner();
    assert_eq!(integer(&first.redeem(&result).await[0], 0, 0), 7);

    running.node.detach_local("first".into()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while running.node.catalog().is_draining("first") { tokio::task::yield_now().await; }
    }).await.unwrap();
    running.node.database_or_attach("first".into(), Default::default()).await.unwrap().unwrap();
    assert_eq!(first.client.get_flight_info(first.request(catalogs)).await.unwrap_err().code(), tonic::Code::Unavailable);
    let mut replacement = Client::connect_with_database(&running.server, None, Some("first")).await;
    assert_eq!(integer(&replacement.query("SELECT 9::BIGINT AS n").await[0], 0, 0), 9);
    running.stop().await;
}

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
async fn flight_redeems_ordered_distinct_from_index_batches_at_the_ticket_basis() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    client.update("CREATE INDEX docs_candidates ON docs (world, key, label)", None).await;
    client.update("INSERT INTO docs RECORDS
        {_id: 1, world: 1, key: 'a', label: 'hidden'},
        {_id: 2, world: 1, key: 'b', label: 'public'},
        {_id: 3, world: 1, key: 'b', label: 'public'},
        {_id: 4, world: 1, key: 'c', label: 'public'}", None).await;
    let database = running.node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    running.node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.build_artefacts().await.unwrap();

    let table = TableRef::new("public", "docs");
    let snapshot = database.snapshot();
    let entry = snapshot.table_entry(&table).unwrap();
    let trie = rtdb_format::keys::TrieKey::l0(snapshot.trie_catalog().l0_max_block_index(&table)).unwrap();
    let primary = rtdb_storage::ObjectKey::new(entry.slug.data_key(&trie)).unwrap();
    let info = client.info("SELECT DISTINCT key FROM docs WHERE world = 1 AND label = 'public' ORDER BY key LIMIT 2", None).await;
    assert_eq!(info.clone().try_decode_schema().unwrap().field(0).data_type(), &DataType::Utf8);
    database.object_store().delete(&primary).await.unwrap();
    database.storage().invalidate(&primary).await.unwrap();

    let batches = client.redeem(&info).await;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_columns(), 1);
    let keys = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(keys.iter().collect::<Vec<_>>(), [Some("b"), Some("c")]);
    running.stop().await;
}

#[tokio::test]
async fn prepared_update_info_buffers_the_bound_operation_once() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    client.update("INSERT INTO docs (_id,n) VALUES (1,10), (2,20)", None).await;
    let transaction = client.begin().await;
    let prepared = client.prepare("UPDATE docs SET n = n + ? WHERE _id = ?", Some(transaction.clone())).await;
    let command = CommandPreparedStatementQuery { prepared_statement_handle: prepared.prepared_statement_handle };
    let schema = Arc::new(Schema::new(vec![Field::new("increment", DataType::Int64, false), Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![2, 4])), Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
    client.upload(command.clone(), vec![batch]).await;
    let descriptor = FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
    let request = client.request(descriptor);
    let info = client.client.get_flight_info(request).await.unwrap().into_inner();
    assert!(info.clone().try_decode_schema().unwrap().fields().is_empty());
    assert!(client.redeem(&info).await.is_empty());
    assert!(client.redeem(&info).await.is_empty());
    client.end(transaction, true).await.unwrap();
    let result = client.query("SELECT n FROM docs ORDER BY _id").await;
    let actual = result.iter().flat_map(|batch| (0..batch.num_rows()).map(|row| integer(batch, 0, row))).collect::<Vec<_>>();
    assert_eq!(actual, [12, 24]);
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
    client.upload(CommandPreparedStatementQuery { prepared_statement_handle: query.prepared_statement_handle.clone() }, vec![arguments(vec![41, 99])]).await;
    let descriptor = FlightDescriptor::new_cmd(CommandPreparedStatementQuery { prepared_statement_handle: query.prepared_statement_handle.clone() }.as_any().encode_to_vec());
    let request = client.request(descriptor.clone()); let old = client.client.get_flight_info(request).await.unwrap().into_inner();
    client.upload(CommandPreparedStatementQuery { prepared_statement_handle: query.prepared_statement_handle }, vec![arguments(vec![42])]).await;
    let request = client.request(descriptor); let current = client.client.get_flight_info(request).await.unwrap().into_inner();
    assert_eq!(integer(&client.redeem(&old).await[0], 0, 0), 41);
    let current_schema = current.clone().try_decode_schema().unwrap();
    let current = client.redeem(&current).await;
    assert_eq!(current[0].schema().as_ref(), &current_schema);
    assert_eq!(integer(&current[0], 0, 0), 42);
    running.stop().await;
}

#[tokio::test]
async fn prepared_query_projects_nullable_and_boolean_arrow_parameters() {
    use arrow_array::{Array, BooleanArray, cast::AsArray};

    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let query = client.prepare("SELECT $2 AS b, $1 AS n", None).await;
    let command = CommandPreparedStatementQuery { prepared_statement_handle: query.prepared_statement_handle };
    let schema = Arc::new(Schema::new(vec![
        Field::new("input_n", DataType::Int64, true),
        Field::new("input_b", DataType::Boolean, false),
    ]));
    let arguments = RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(vec![None])),
        Arc::new(BooleanArray::from(vec![true])),
    ]).unwrap();
    client.upload(command.clone(), vec![arguments]).await;
    let descriptor = FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
    let request = client.request(descriptor);
    let info = client.client.get_flight_info(request).await.unwrap().into_inner();
    let announced = info.clone().try_decode_schema().unwrap();
    let batches = client.redeem(&info).await;

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].schema().as_ref(), &announced);
    assert_eq!(batches[0].num_rows(), 1);
    assert!(batches[0].column(0).as_boolean().value(0));
    assert_eq!(batches[0].column(1).data_type(), &DataType::Int64);
    assert!(batches[0].column(1).is_null(0));
    running.stop().await;
}

#[tokio::test]
async fn prepared_query_computes_arrow_arguments_and_returns_a_nested_column() {
    use rtdb_columnar::{copy_array, read_value, to_field};
    use rtdb_types::Value;

    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let prepared = client.prepare("SELECT $1 + $2 AS total, $3 AS payload", None).await;
    let command = CommandPreparedStatementQuery { prepared_statement_handle: prepared.prepared_statement_handle };
    let payload = Value::list(vec![Value::Utf8("kept".into()), Value::I64(7)]);
    let ty = payload.logical_type();
    let schema = Arc::new(Schema::new(vec![
        Field::new("left", DataType::Int64, false),
        Field::new("right", DataType::Int64, false),
        to_field("doc", &ty).unwrap(),
    ]));
    let arguments = RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(vec![20])),
        Arc::new(Int64Array::from(vec![22])),
        copy_array(&ty, std::slice::from_ref(&payload)).unwrap(),
    ]).unwrap();
    client.upload(command.clone(), vec![arguments]).await;
    let descriptor = FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
    let request = client.request(descriptor);
    let info = client.client.get_flight_info(request).await.unwrap().into_inner();
    let batches = client.redeem(&info).await;

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].schema().as_ref(), &info.try_decode_schema().unwrap());
    assert_eq!(integer(&batches[0], 0, 0), 42);
    assert_eq!(read_value(batches[0].schema_ref().field(1), batches[0].column(1).as_ref(), 0).unwrap(), payload);
    running.stop().await;
}

#[tokio::test]
async fn prepared_updates_coalesce_compatible_parameter_layouts_across_uploads() {
    use rtdb_types::Value;

    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    client.update("INSERT INTO docs (_id,v) VALUES (1,0),(2,0),(3,0)", None).await;
    let transaction = client.begin().await;
    let prepared = client.prepare("UPDATE docs SET v = ? WHERE _id = ?", Some(transaction.clone())).await;
    let command = CommandPreparedStatementUpdate { prepared_statement_handle: prepared.prepared_statement_handle };
    let arguments = |label: &str, value: Value, id| {
        let ty = value.logical_type();
        let schema = Arc::new(Schema::new(vec![
            rtdb_columnar::to_field(label, &ty).unwrap(),
            Field::new(format!("{label}_id"), DataType::Int64, false),
        ]));
        RecordBatch::try_new(schema, vec![rtdb_columnar::build_array(&ty, &[value]).unwrap(), Arc::new(Int64Array::from(vec![id]))]).unwrap()
    };
    client.upload(command.clone(), vec![arguments("number", Value::U64(u64::MAX), 1)]).await;
    client.upload(command.clone(), vec![arguments("text", Value::Utf8("changed".into()), 2)]).await;
    client.upload(command, vec![arguments("absent", Value::Null, 3)]).await;
    client.end(transaction, true).await.unwrap();

    let mut rows = client.query("SELECT _id, v FROM docs").await.iter().flat_map(|batch| {
        let schema = batch.schema();
        (0..batch.num_rows()).map(|row| {
            (integer(batch, 0, row), rtdb_columnar::read_value(schema.field(1), batch.column(1).as_ref(), row).unwrap())
        }).collect::<Vec<_>>()
    }).collect::<Vec<_>>();
    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(rows, [(1, Value::I64(-1)), (2, Value::Utf8("changed".into())), (3, Value::Null)]);
    running.stop().await;
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
async fn bulk_ingest_preserves_nested_arrow_lists_with_signed_execution_values() {
    use arrow_array::{Array, ListArray, types::UInt64Type};

    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let items = ListArray::from_iter_primitive::<UInt64Type, _, _>([
        Some(vec![Some(u64::MAX), None]),
        Some(vec![Some(9)]),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Int64, false),
        Field::new("items", items.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(vec![1, 2])), Arc::new(items),
    ]).unwrap();
    client.upload(CommandStatementIngest { table: "docs".into(), ..Default::default() }, vec![batch]).await;

    let actual = client.query("SELECT items FROM docs ORDER BY _id").await;

    let lists: Vec<_> = actual.iter().flat_map(|batch| {
        let column = batch.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        column.iter().map(|value| value.unwrap().as_any().downcast_ref::<Int64Array>().unwrap().iter().collect::<Vec<_>>())
    }).collect();
    assert_eq!(lists, [vec![Some(-1), None], vec![Some(9)]]);
    running.stop().await;
}

#[tokio::test]
async fn mixed_nested_ingest_returns_arrow_unions_from_cold_persisted_pages() {
    use arrow_array::{Array, UInt64Array, UnionArray};
    use arrow_schema::UnionFields;
    use rtdb_types::{LogicalType, StructFields, Value};

    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let record = Value::Struct(StructFields::from([("a".into(), Value::I64(7))]));
    let list = Value::list(vec![Value::Utf8("list".into())]);
    let fixed = Value::FixedSizeList(vec![Value::I64(13), Value::I64(21)].into());
    let uuid = Value::Uuid([3; 16]);
    let upload = |ids: Vec<i64>, logical: LogicalType, values: &[Value]| {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Int64, false),
            rtdb_columnar::to_field("v", &logical).unwrap(),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(ids)),
            rtdb_columnar::build_array(&logical, values).unwrap(),
        ]).unwrap()
    };
    let tx = client.begin().await;
    let command = CommandStatementIngest { table: "docs".into(), transaction_id: Some(tx.clone()), ..Default::default() };
    client.upload(command.clone(), vec![upload(vec![1, 2], record.logical_type().maybe(), &[record.clone(), Value::Null])]).await;
    client.upload(command.clone(), vec![upload(vec![3], list.logical_type(), std::slice::from_ref(&list))]).await;
    let aliases = UnionFields::try_new([3, 8], [Field::new("u", DataType::UInt64, false), Field::new("s", DataType::Int64, false)]).unwrap();
    let aliases = UnionArray::try_new(aliases, vec![3i8, 8].into(), Some(vec![0i32, 0].into()),
        vec![Arc::new(UInt64Array::from(vec![u64::MAX])), Arc::new(Int64Array::from(vec![9]))]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("_id", DataType::Int64, false), Field::new("v", aliases.data_type().clone(), false)]));
    let aliases = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![4, 5])), Arc::new(aliases)]).unwrap();
    client.upload(command.clone(), vec![aliases]).await;
    client.upload(command.clone(), vec![upload(vec![6], fixed.logical_type(), std::slice::from_ref(&fixed))]).await;
    client.upload(command, vec![upload(vec![7], uuid.logical_type(), std::slice::from_ref(&uuid))]).await;
    client.end(tx, true).await.unwrap();

    let database = running.node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    running.node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    let snapshot = database.snapshot();
    let table = TableRef::new("public", "docs");
    let entry = snapshot.table_entry(&table).unwrap();
    let trie = rtdb_format::keys::TrieKey::l0(snapshot.trie_catalog().l0_max_block_index(&table)).unwrap();
    let key = entry.slug.data_key(&trie);
    let persisted = database.object_store().list(None).await.unwrap().into_iter()
        .find(|object| object.key.as_str() == key).unwrap();
    database.storage().invalidate(&persisted.key).await.unwrap();

    let mut rows: Vec<_> = client.query("SELECT _id, v FROM docs").await.iter().flat_map(|batch| {
        let schema = batch.schema();
        let field = schema.field(1);
        assert!(matches!(rtdb_columnar::from_field(field).unwrap(), LogicalType::Union(_)));
        let array = batch.column(1).as_any().downcast_ref::<UnionArray>().unwrap();
        (0..batch.num_rows()).map(|row| (integer(batch, 0, row), rtdb_columnar::read_value(field, array, row).unwrap())).collect::<Vec<_>>()
    }).collect();
    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(rows, [(1, record), (2, Value::Null), (3, list), (4, Value::I64(-1)), (5, Value::I64(9)), (6, fixed), (7, uuid)]);
    running.stop().await;
}

#[tokio::test]
async fn arrow_ingest_batches_and_sql_deletes_keep_transaction_order() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Int64, false),
        Field::new("score", DataType::UInt64, true),
    ]));
    let batch = |ids, scores| RecordBatch::try_new(Arc::clone(&schema), vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(arrow_array::UInt64Array::from(scores)),
    ]).unwrap();
    let tx = client.begin().await;
    let put = CommandStatementIngest { table: "docs".into(), transaction_id: Some(tx.clone()), ..Default::default() };
    client.upload(put.clone(), vec![batch(vec![1i64, 2], vec![Some(99u64), None])]).await;
    client.update("DELETE FROM docs WHERE _id = 1", Some(tx.clone())).await;
    client.upload(put, vec![batch(vec![1i64], vec![Some(u64::MAX)])]).await;
    client.end(tx, true).await.unwrap();

    let rows = client.query("SELECT _id, score FROM docs ORDER BY _id").await;
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let first = &rows[0];
    assert_eq!(integer(first, 0, 0), 1);
    assert_eq!(integer(first, 1, 0), -1);
    let (last, row) = if first.num_rows() == 2 { (first, 1) } else { (&rows[1], 0) };
    assert_eq!(integer(last, 0, row), 2);
    assert!(last.column(1).is_null(row));
    running.stop().await;
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
async fn uploaded_statement_errors_keep_direct_diagnostics_and_allow_the_next_update() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    for (sql, diagnostic) in [
        ("SELEC 1", "mismatched input 'SELEC'"),
        ("extra SELECT 1", "extraneous input 'extra'"),
    ] {
        let command = CommandStatementUpdate { query: sql.into(), transaction_id: None };
        let descriptor = FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
        let data = arrow_flight::FlightData { flight_descriptor: Some(descriptor), ..Default::default() };
        let request = client.request(futures::stream::iter([data]));
        let error = client.client.do_put(request).await.unwrap_err();
        assert_initial_sql_diagnostic(&error, diagnostic);
    }
    client.update("INSERT INTO docs (_id,n) VALUES (1,42)", None).await;
    assert_eq!(integer(&client.query("SELECT n FROM docs WHERE _id = 1").await[0], 0, 0), 42);
    running.stop().await;
}

#[tokio::test]
async fn get_schema_reports_a_later_statement_under_the_direct_grammar() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let command = CommandStatementQuery { query: "SELECT 1; SELEC 1".into(), transaction_id: None };
    let request = client.request(FlightDescriptor::new_cmd(command.as_any().encode_to_vec()));
    let error = client.client.get_schema(request).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(error.message(), "Errors parsing SQL statement:\n  - line 1:10 mismatched input 'SELEC' expecting <EOF>");
    let wire = error.metadata().get_bin("xtdb-anomaly-bin").unwrap().to_bytes().unwrap();
    let detail: serde_json::Value = serde_json::from_slice(&wire).unwrap();
    assert_eq!(detail, serde_json::json!({"category": "incorrect", "code": "xtdb/sql-error", "message": error.message()}));
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

#[tokio::test]
async fn uuid_and_fixed_size_binary_results_keep_their_arrow_storage_and_extension() {
    use arrow_array::{Array, FixedSizeBinaryArray};
    use std::collections::HashMap;
    let running = Running::start(None, FlightConfig::default()).await; let mut client = Client::connect(&running.server, None).await;
    let uuid_metadata = HashMap::from([("ARROW:extension:name".to_owned(), "uuid".to_owned()), ("ARROW:extension:metadata".to_owned(), String::new())]);
    let uuids: [Option<[u8; 16]>; 2] = [Some(std::array::from_fn(|index| 0xf0 | index as u8)), None];
    let fixed: [Option<[u8; 4]>; 2] = [None, Some([0xde, 0xad, 0xbe, 0xef])];
    let schema = Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Int64, false),
        Field::new("u", DataType::FixedSizeBinary(16), true).with_metadata(uuid_metadata.clone()),
        Field::new("b", DataType::FixedSizeBinary(4), true),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(uuids.iter().map(|value| value.as_ref().map(|bytes| bytes.as_slice())), 16).unwrap()),
        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(fixed.iter().map(|value| value.as_ref().map(|bytes| bytes.as_slice())), 4).unwrap()),
    ]).unwrap();
    client.upload(CommandStatementIngest { table: "docs".into(), ..Default::default() }, vec![batch]).await;
    let info = client.info("SELECT u, b, UUID 'f0f1f2f3-f4f5-f6f7-f8f9-fafbfcfdfeff' AS literal FROM docs ORDER BY _id", None).await;
    let announced = info.clone().try_decode_schema().unwrap();
    let rows = client.redeem(&info).await;
    assert_eq!(rows[0].schema().as_ref(), &announced);
    let field = |name: &str| announced.field_with_name(name).unwrap().clone();
    for name in ["u", "literal"] {
        assert_eq!(field(name).data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(field(name).metadata(), &uuid_metadata);
    }
    assert_eq!(field("b").data_type(), &DataType::FixedSizeBinary(4));
    assert!(field("b").metadata().get("ARROW:extension:name").is_none());
    let column = |name: &str| rows.iter().flat_map(|page| {
        let array = page.column_by_name(name).unwrap().as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap().clone();
        (0..array.len()).map(move |row| array.is_valid(row).then(|| array.value(row).to_vec()))
    }).collect::<Vec<_>>();
    assert_eq!(column("u"), uuids.iter().map(|value| value.map(|bytes| bytes.to_vec())).collect::<Vec<_>>());
    assert_eq!(column("b"), fixed.iter().map(|value| value.map(|bytes| bytes.to_vec())).collect::<Vec<_>>());
    assert_eq!(column("literal"), vec![uuids[0].map(|bytes| bytes.to_vec()); 2]);
    running.stop().await;
}

#[tokio::test]
async fn transactional_record_inserts_with_many_parameter_rows_release_their_memory() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let baseline = running.node.memory().used_bytes();
    for batch in 0..2i64 {
        let transaction = client.begin().await;
        let prepared = client.prepare(r#"INSERT INTO docs RECORDS {"_id": ?, "num": ?}"#, Some(transaction.clone())).await;
        let rows = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false), Field::new("b", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from_iter_values((0..1000).map(|i| batch * 1000 + i))), Arc::new(Int64Array::from_iter_values(0..1000))]).unwrap();
        client.upload(CommandPreparedStatementUpdate { prepared_statement_handle: prepared.prepared_statement_handle }, vec![rows]).await;
        client.end(transaction, true).await.unwrap();
    }
    assert_eq!(integer(&client.query("SELECT COUNT(*) FROM docs").await[0], 0, 0), 2000);
    assert!(running.node.memory().used_bytes() - baseline < 16 << 20);
    running.stop().await;
}

#[tokio::test]
async fn grouped_owned_commit_checks_every_relation_before_writing() {
    use arrow_ipc::{reader::StreamReader, writer::IpcWriteOptions};
    use rtdb_format::ipc::write_arrow_stream;

    fn ipc(batch: RecordBatch) -> Vec<u8> {
        write_arrow_stream(batch.schema_ref(), std::slice::from_ref(&batch), IpcWriteOptions::default()).unwrap()
    }

    fn row(operation: &str, value: Option<i64>, observed_at: Option<i64>, expected: Option<i64>) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Int64, false),
            Field::new("_op", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
            Field::new("_read_valid_micros", DataType::Int64, true),
            Field::new("_read_system_micros", DataType::Int64, true),
        ]));
        ipc(RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![operation])),
            Arc::new(Int64Array::from(vec![value])),
            Arc::new(Int64Array::from(vec![observed_at])),
            Arc::new(Int64Array::from(vec![expected])),
        ]).unwrap())
    }

    fn grouped(groups: &[(&str, Vec<u8>)]) -> Bytes {
        let mut body = i64::MAX.to_be_bytes().to_vec();
        body.extend_from_slice(&(groups.len() as u16).to_be_bytes());
        for (relation, stream) in groups {
            body.extend_from_slice(&(relation.len() as u16).to_be_bytes());
            body.extend_from_slice(relation.as_bytes());
            body.extend_from_slice(&(stream.len() as u32).to_be_bytes());
            body.extend_from_slice(stream);
        }
        body.into()
    }

    async fn point(client: &mut Client, relation: &str) -> (i64, i64) {
        let mut body = (relation.len() as u16).to_be_bytes().to_vec();
        body.extend_from_slice(relation.as_bytes());
        body.extend_from_slice(&ipc(RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("_id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        ).unwrap()));
        let output = client.action("rtdb.owned.ReadPoint.v1", body.into()).await.unwrap();
        let mut stream = StreamReader::try_new(output[0].body.as_ref(), None).unwrap();
        let at = stream.schema().metadata()["rtdb.owned.valid_micros"].parse().unwrap();
        let batch = stream.next().unwrap().unwrap();
        let value = integer(&batch, batch.schema().index_of("value").unwrap(), 0);
        (at, value)
    }

    let running = Running::start(Some("secret"), FlightConfig { owned_native_actions: true, ..FlightConfig::default() }).await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    let commit = "rtdb.owned.Commit.v2";
    let created = client.action(commit, grouped(&[
        ("items", row("create", Some(10), None, None)),
        ("orders", row("create", Some(20), None, None)),
    ])).await.unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(point(&mut client, "items").await.1, 10);
    let (at, value) = point(&mut client, "orders").await;
    assert_eq!(value, 20);

    let refused = client.action(commit, grouped(&[
        ("items", row("replace", Some(11), None, None)),
        ("orders", row("check", None, Some(at), Some(i64::MIN))),
    ])).await;
    assert_eq!(refused.unwrap_err().message(), "The owned point observation changed before ordering");
    assert_eq!(point(&mut client, "items").await.1, 10);
    assert_eq!(point(&mut client, "orders").await.1, 20);
    running.stop().await;
}


#[tokio::test]
async fn world_flight_actions_require_a_placed_world_host() {
    let running = Running::start(Some("secret"), FlightConfig { owned_native_actions: true, ..FlightConfig::default() }).await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    for action in [
        "rtdb.owned.PublishWorldCut.v1", "rtdb.owned.PublishWorldCut.v2",
        "rtdb.owned.CommitHeldWorlds.v4", "rtdb.owned.CommitWorld.v4",
        "rtdb.owned.OpenWorldRead.v4", "rtdb.owned.ReadHeldWorldPoint.v2", "rtdb.owned.SearchHeldWorld.v2",
        "rtdb.owned.ReadHeldWorldValidDuring.v2",
        "rtdb.owned.SelectHeldWorldParentAsOf.v1",
        "rtdb.owned.SelectHeldWorldAsOf.v1",
        "rtdb.owned.ReadHeldWorldPointAt.v1",
        "rtdb.owned.ReadHeldWorldParentVersionsAt.v1",
        "rtdb.owned.ReadHeldWorldParentChangesAt.v1",
        "rtdb.owned.ReadHeldWorldAncestorChanges.v1",
        "rtdb.owned.CloseWorldRead.v1",
    ] {
        let refused = client.action(action, Bytes::from_static(&[0; 16])).await.unwrap_err();
        assert_eq!(refused.code(), tonic::Code::Unimplemented, "{action}");
    }
    let refused = client.action("rtdb.owned.SearchHeldWorld.v2", Bytes::new()).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unimplemented);
    running.stop().await;
}

#[tokio::test]
async fn bulk_ingest_and_insert_select_preserve_fixed_float_embeddings() {
    use arrow_array::{Array, FixedSizeListArray, Float32Array, Float64Array};
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let vectors = FixedSizeListArray::try_new(item, 2, Arc::new(Float32Array::from(vec![2.0, 0.0, 0.0, 3.0])), None).unwrap();
    let schema = Schema::new(vec![Field::new("_id", DataType::Int64, false), Field::new("embedding", vectors.data_type().clone(), false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(vectors)]).unwrap();
    client.upload(CommandStatementIngest { table: "vectors".into(), ..Default::default() }, vec![batch]).await;
    client.update("INSERT INTO vectors_copy (_id, embedding) SELECT _id, embedding FROM vectors", None).await;
    for table in ["vectors", "vectors_copy"] {
        let sql = format!("SELECT embedding, COSINE_DISTANCE(embedding, ARRAY[1,0]) AS d FROM {table} ORDER BY _id");
        let results = client.query(&sql).await;
        let vectors = results[0].column(0).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let values = vectors.values().as_any().downcast_ref::<Float32Array>().unwrap();
        let distances = results[0].column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values.values().as_ref(), &[2.0, 0.0, 0.0, 3.0]);
        assert_eq!(distances.values().as_ref(), &[0.0, 1.0]);
    }
    let query = client.prepare("SELECT _id, embedding, COSINE_DISTANCE(embedding, ?) AS d FROM vectors_copy ORDER BY d, _id LIMIT 1", None).await;
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let value = FixedSizeListArray::try_new(item, 2, Arc::new(Float32Array::from(vec![0.0, 1.0])), None).unwrap();
    let schema = Schema::new(vec![Field::new("", value.data_type().clone(), false)]);
    let args = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(value)]).unwrap();
    let command = CommandPreparedStatementQuery { prepared_statement_handle: query.prepared_statement_handle };
    let descriptor = CommandPreparedStatementQuery { prepared_statement_handle: command.prepared_statement_handle.clone() };
    client.upload(command, vec![args]).await;
    let request = client.request(FlightDescriptor::new_cmd(descriptor.as_any().encode_to_vec()));
    let info = client.client.get_flight_info(request).await.unwrap().into_inner();
    let result = client.redeem(&info).await;
    assert_eq!(integer(&result[0], 0, 0), 2);
    let output = result[0].column(1).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    assert_eq!(output.values().as_any().downcast_ref::<Float32Array>().unwrap().values().as_ref(), &[0.0, 3.0]);
    running.stop().await;
}
