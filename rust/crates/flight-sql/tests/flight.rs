use std::{sync::{Arc, atomic::{AtomicI64, Ordering}}, time::Duration};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_flight::{Action, FlightDescriptor, FlightInfo, flight_service_client::FlightServiceClient, sql::{Any, ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, ActionEndTransactionRequest, CommandGetCatalogs, CommandGetSqlInfo, CommandGetTables, CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementIngest, CommandStatementQuery, CommandStatementUpdate, EndTransaction, ProstMessageExt}};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
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
struct HostedFixtureResources(rtdb_node::NativeResources);
struct HostedFixtureReceipts(rtdb_types::PlacedAttemptScope);
struct FixtureOwner;

const FIXTURE_SOURCE_HISTORY: uuid::Uuid = uuid::Uuid::from_u128(0x16ac_65ba_78eb_4d4f_9871_7157_bf8a_7023);

struct FixtureSourceFence;

#[async_trait]
impl rtdb_node::ReadFence for FixtureSourceFence {
    async fn confirm(&self, _: &str) -> Result<()> { Ok(()) }
    async fn confirm_hosted(&self, _: &rtdb_node::Database) -> Result<String> { Ok(FIXTURE_SOURCE_HISTORY.to_string()) }
}

#[async_trait]
impl rtdb_node::SessionIdentity for FixtureOwner {
    fn user(&self) -> Option<&str> { Some("xtdb") }
    async fn revalidate(&self, _: Timestamp) -> Result<()> { Ok(()) }
    fn authorize_database(&self, _: &str, _: rtdb_node::DatabaseAccess) -> Result<()> { Ok(()) }
}

impl rtdb_node::PlacedReceiptCodec for HostedFixtureReceipts {
    fn scope(&self) -> rtdb_types::PlacedAttemptScope { self.0 }

    fn seal(&self, value: &[u8]) -> Result<String> {
        use base64::Engine;
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value))
    }

    fn open(&self, token: &str) -> Result<Vec<u8>> {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(token)
            .map_err(|_| Anomaly::incorrect("test/placed-receipt", "Invalid fixture receipt"))
    }

    fn seal_snapshot(&self, value: &[u8]) -> Result<String> {
        Ok(format!("snapshot-{}", self.seal(value)?))
    }

    fn open_snapshot(&self, token: &str) -> Result<Vec<u8>> {
        let token = token.strip_prefix("snapshot-")
            .ok_or_else(|| Anomaly::incorrect("test/placed-snapshot", "Invalid fixture snapshot"))?;
        self.open(token)
    }
}

#[async_trait]
impl rtdb_node::ResourceFactory for HostedFixtureResources {
    fn validate(&self, name: &str, config: &rtdb_format::database::proto::DatabaseConfig) -> Result<()> {
        self.0.validate(name, config)
    }

    async fn open_storage(&self, name: &str, config: &rtdb_format::database::proto::DatabaseConfig) -> Result<rtdb_node::DatabaseStorage> {
        self.0.open_storage(name, config).await
    }

    async fn open_logs(
        &self, name: &str, config: &rtdb_format::database::proto::DatabaseConfig,
        access: rtdb_node::LogAccess, cancellation: tokio_util::sync::CancellationToken, storage: &rtdb_node::DatabaseStorage,
    ) -> Result<rtdb_node::OpenedLogs> {
        let mut logs = self.0.open_logs(name, config, access, cancellation, storage).await?;
        logs.await_authority = rtdb_node::AwaitAuthority::Live {
            codec: Box::new(HostedFixtureReceipts(rtdb_types::PlacedAttemptScope::new(name, 1)?)),
            recovery: rtdb_node::ClosedEpochRecovery::Opened(rtdb_storage::log::LogOffset(-1)),
            placed_receipt_retirement: false,
        };
        Ok(logs)
    }

    async fn close(&self) -> Result<()> { self.0.close().await }
}

type FixtureResources = Box<dyn FnOnce(Arc<dyn Clock>) -> Arc<dyn rtdb_node::ResourceFactory> + Send>;

struct Running { node: Node, node_task: tokio::task::JoinHandle<Result<()>>, server: ServerHandle, server_task: tokio::task::JoinHandle<Result<()>>, clock: Arc<ManualClock> }
impl Running {
    async fn start(password: Option<&str>, config: FlightConfig) -> Self { Self::start_with_placements(password, config, &[]).await }
    async fn start_with_placements(password: Option<&str>, config: FlightConfig, placements: &[&str]) -> Self {
        Self::start_with_tracer(password, config, placements, None).await
    }
    async fn start_with_tracer(password: Option<&str>, config: FlightConfig, placements: &[&str], tracer: Option<Arc<dyn rtdb_plan::Tracer>>) -> Self {
        Self::start_with_options(password, config, placements, tracer, NodeOptions::default()).await
    }
    async fn start_with_options(password: Option<&str>, config: FlightConfig, placements: &[&str], tracer: Option<Arc<dyn rtdb_plan::Tracer>>, options: NodeOptions) -> Self {
        Self::start_with_fence(password, config, placements, tracer, options, None).await
    }
    async fn start_with_fence(password: Option<&str>, config: FlightConfig, placements: &[&str], tracer: Option<Arc<dyn rtdb_plan::Tracer>>, options: NodeOptions, read_fence: Option<Arc<dyn rtdb_node::ReadFence>>) -> Self {
        Self::start_with_resources(password, config, placements, tracer, options, read_fence, None).await
    }
    async fn start_with_resources(password: Option<&str>, config: FlightConfig, placements: &[&str], tracer: Option<Arc<dyn rtdb_plan::Tracer>>, options: NodeOptions, read_fence: Option<Arc<dyn rtdb_node::ReadFence>>, resources: Option<Arc<dyn rtdb_node::ResourceFactory>>) -> Self {
        let resources = resources.map(|resources| Box::new(move |_: Arc<dyn Clock>| resources) as FixtureResources);
        Self::start_with_clocked_resources(password, config, placements, tracer, options, read_fence, resources).await
    }
    /// `resources` receives the node clock so that a local log stamps transactions on the same clock the node reads at.
    async fn start_with_clocked_resources(password: Option<&str>, mut config: FlightConfig, placements: &[&str], tracer: Option<Arc<dyn rtdb_plan::Tracer>>, mut options: NodeOptions, read_fence: Option<Arc<dyn rtdb_node::ReadFence>>, resources: Option<FixtureResources>) -> Self {
        let node_clock: Arc<dyn Clock> = Arc::new(TickClock(AtomicI64::new(1_000_000)));
        let auth = Authenticator::open(AuthConfig::SingleRootUser { password: password.map(Secret::new) }, MemoryBudget::new(64 << 20), node_clock.clone(), Arc::new(NoHttp)).await.unwrap();
        let mut services = NodeServices { clock: node_clock.clone(), tracer, read_fence, ..NodeServices::local(auth.clone(), Arc::new(Observer), auth.clone(), Arc::new(Metrics)) };
        if let Some(resources) = resources {
            services.resources = resources(node_clock.clone());
        } else if config.hosted_controls.is_some() {
            services.resources = Arc::new(HostedFixtureResources(rtdb_node::NativeResources::default()));
        } else {
            services.resources = Arc::new(rtdb_node::NativeResources::default().with_local_log_clock(node_clock.clone(), false));
        }
        options.automatic_compaction = false; options.automatic_gc = false; options.replication.assertion_interval = Duration::from_millis(1); options.replication.election_seed = 1;
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
        Self::connect_with_peer(server, basic, database, None).await
    }

    async fn connect_with_peer(server: &ServerHandle, basic: Option<&str>, database: Option<&str>, proof: Option<&str>) -> Self {
        let channel = Channel::from_shared(format!("http://{}", server.address())).unwrap().connect().await.unwrap();
        let client = FlightServiceClient::new(channel); let mut this = Self { client, headers: MetadataMap::new() };
        if let Some(basic) = basic { use base64::Engine; this.headers.insert("authorization", format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(basic)).parse().unwrap()); }
        if let Some(database) = database { this.headers.insert("x-xtdb-database", database.parse().unwrap()); }
        let mut request = this.request(futures::stream::iter(vec![arrow_flight::HandshakeRequest::default()]));
        if let Some(proof) = proof {
            request.metadata_mut().insert(rtdb_flight_sql::FORWARDED_PEER_HEADER, proof.parse().unwrap());
        }
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
async fn flight_sql_query_spans_follow_the_session_application_name() {
    let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(MemoryBudget::new(1 << 20), 32, true, false).unwrap();
    let running = Running::start_with_tracer(None, FlightConfig::default(), &[], Some(tracer.clone())).await;
    let mut client = Client::connect(&running.server, None).await;
    let default_query = "SELECT 42::BIGINT AS flight_default";
    let named_query = "SELECT 43::BIGINT AS flight_named";
    assert_eq!(integer(&client.query(default_query).await[0], 0, 0), 42);
    client.update("SET application_name = 'flight-importer'", None).await;
    assert_eq!(integer(&client.query(named_query).await[0], 0, 0), 43);
    drop(client);
    running.stop().await;
    tracer.close();
    let mut names = std::collections::BTreeMap::new();
    while let Some(span) = receiver.next().await {
        if span.name == "xtdb.query" {
            let attrs: std::collections::BTreeMap<_, _> = span.attributes().collect();
            if let Some(text) = attrs.get("query.text") {
                names.insert((*text).to_owned(), attrs["application.name"].to_owned());
            }
        }
    }
    assert_eq!(names[default_query], "XTDB Flight SQL");
    assert_eq!(names[named_query], "flight-importer");
}

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
    assert_eq!(second.client.do_get(second.request(ticket)).await.unwrap_err().code(), tonic::Code::NotFound);
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
async fn query_ticket_keeps_an_empty_cut_and_the_original_clock() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let clock = client.info("SELECT CURRENT_TIMESTAMP(6) AS held_clock", None).await;
    let before = client.redeem(&clock).await;
    let query = "SELECT count(*) AS n FROM xt.txs FOR ALL VALID_TIME";
    let empty = client.info(query, None).await;
    client.update("INSERT INTO docs (_id,n) VALUES (1,10)", None).await;
    let replayed = client.redeem(&clock).await;
    assert_eq!(before, replayed);
    assert_eq!(integer(&client.redeem(&empty).await[0], 0, 0), 0);
    assert_eq!(integer(&client.query(query).await[0], 0, 0), 1);
    running.stop().await;
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
async fn retained_ticket_pins_arrow_parameters_after_rebind() {
    use arrow_flight::sql::TicketStatementQuery;

    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let query = client.prepare("SELECT ? AS n", None).await;
    let command = CommandPreparedStatementQuery { prepared_statement_handle: query.prepared_statement_handle };
    client.upload(command.clone(), vec![arguments(vec![41, 99])]).await;
    let descriptor = FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
    let request = client.request(descriptor.clone());
    let info = client.client.get_flight_info(request).await.unwrap().into_inner();
    let ticket = Any::decode(info.endpoint[0].ticket.as_ref().unwrap().ticket.as_ref()).unwrap();
    let handle = TicketStatementQuery::decode(ticket.value).unwrap().statement_handle;
    assert!(handle.starts_with(b"RTDBFT\x02"));
    assert_eq!(handle.len(), 7 + 16);

    let text = RecordBatch::try_from_iter([("label", Arc::new(StringArray::from(vec!["new"])) as arrow_array::ArrayRef)]).unwrap();
    client.upload(command, vec![text]).await;
    let request = client.request(descriptor);
    let current = client.client.get_flight_info(request).await.unwrap().into_inner();
    assert_eq!(info.clone().try_decode_schema().unwrap().field(0).data_type(), &DataType::Int64);
    assert_eq!(current.clone().try_decode_schema().unwrap().field(0).data_type(), &DataType::Utf8);
    assert_eq!(integer(&client.redeem(&info).await[0], 0, 0), 41);
    let rows = client.redeem(&current).await;
    assert_eq!(rows[0].column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "new");
    let mut other = Client::connect(&running.server, None).await;
    let request = other.request(info.endpoint[0].ticket.clone().unwrap());
    assert_eq!(other.client.do_get(request).await.unwrap_err().code(), tonic::Code::NotFound);
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
async fn authenticated_handles_and_query_tickets_require_the_original_session() {
    let running = Running::start(Some("correct"), FlightConfig::default()).await;
    let mut first = Client::connect(&running.server, Some("xtdb:correct")).await; let mut second = Client::connect(&running.server, Some("xtdb:correct")).await;
    let handle = first.begin().await; assert_eq!(second.end(handle.clone(), true).await.unwrap_err().code(), tonic::Code::NotFound);
    first.end(handle, false).await.unwrap(); let info = first.info("SELECT 1::bigint AS n", None).await;
    let request = second.request(info.endpoint[0].ticket.clone().unwrap());
    assert_eq!(second.client.do_get(request).await.unwrap_err().code(), tonic::Code::NotFound);
    assert_eq!(integer(&first.redeem(&info).await[0], 0, 0), 1);
    running.stop().await;
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
async fn table_metadata_preserves_flight_filters_order_and_selected_schemas() {
    use arrow_array::cast::AsArray;
    use arrow_flight::sql::CommandGetDbSchemas;

    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    client.update("INSERT INTO doc_b (_id, label) VALUES (1, 'b')", None).await;
    client.update("INSERT INTO doc_a (_id, amount) VALUES (1, 42)", None).await;
    client.update("INSERT INTO docxa (_id) VALUES (1)", None).await;
    client.update("INSERT INTO doc_more (_id) VALUES (1)", None).await;

    let filter = CommandGetTables {
        catalog: Some("xtdb".into()),
        db_schema_filter_pattern: Some("p_bl%".into()),
        table_name_filter_pattern: Some(r"doc\__".into()),
        table_types: vec!["VIEW".into(), "TABLE".into()],
        include_schema: true,
    };
    for (command, matches) in [
        (filter.clone(), true),
        (CommandGetTables { include_schema: false, ..filter.clone() }, true),
        (CommandGetTables { catalog: Some("XTDB".into()), ..filter.clone() }, false),
        (CommandGetTables { catalog: Some(String::new()), ..filter.clone() }, false),
        (CommandGetTables { table_types: vec!["table".into()], ..filter }, false),
    ] {
        let include_schema = command.include_schema;
        let request = client.request(FlightDescriptor::new_cmd(command.as_any().encode_to_vec()));
        let info = client.client.get_flight_info(request).await.unwrap().into_inner();
        let batch = &client.redeem(&info).await[0];
        assert_eq!(batch.schema().as_ref(), &info.try_decode_schema().unwrap());
        assert_eq!(batch.num_columns(), if include_schema { 5 } else { 4 });
        if matches {
            assert_eq!(batch.column(0).as_string::<i32>().iter().collect::<Vec<_>>(), [Some("xtdb"), Some("xtdb")]);
            assert_eq!(batch.column(1).as_string::<i32>().iter().collect::<Vec<_>>(), [Some("public"), Some("public")]);
            assert_eq!(batch.column(2).as_string::<i32>().iter().collect::<Vec<_>>(), [Some("doc_a"), Some("doc_b")]);
            assert_eq!(batch.column(3).as_string::<i32>().iter().collect::<Vec<_>>(), [Some("TABLE"), Some("TABLE")]);
            if include_schema {
                let encoded = batch.column(4).as_binary::<i32>();
                let first = rtdb_format::ipc::read_schema_message(encoded.value(0)).unwrap();
                let second = rtdb_format::ipc::read_schema_message(encoded.value(1)).unwrap();
                assert!(first.field_with_name("amount").is_ok());
                assert!(first.field_with_name("label").is_err());
                assert!(second.field_with_name("label").is_ok());
                assert!(second.field_with_name("amount").is_err());
            }
        } else {
            assert_eq!(batch.num_rows(), 0);
        }
    }

    let command = CommandGetDbSchemas {
        catalog: Some("xtdb".into()),
        db_schema_filter_pattern: Some("p%".into()),
    };
    let request = client.request(FlightDescriptor::new_cmd(command.as_any().encode_to_vec()));
    let info = client.client.get_flight_info(request).await.unwrap().into_inner();
    let batch = &client.redeem(&info).await[0];
    assert_eq!(batch.column(0).as_string::<i32>().iter().collect::<Vec<_>>(), [Some("xtdb"), Some("xtdb")]);
    assert_eq!(batch.column(1).as_string::<i32>().iter().collect::<Vec<_>>(), [Some("pg_catalog"), Some("public")]);
    running.stop().await;
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
async fn owned_import_page_uses_the_held_cut_and_commits_atomically() {
    use arrow_array::{ArrayRef, TimestampMicrosecondArray};
    use arrow_ipc::writer::IpcWriteOptions;
    use rtdb_format::ipc::write_arrow_stream;

    fn ipc(columns: impl IntoIterator<Item = (&'static str, ArrayRef)>) -> Vec<u8> {
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        write_arrow_stream(batch.schema_ref(), std::slice::from_ref(&batch), IpcWriteOptions::default()).unwrap()
    }

    fn point_request(hold: &[u8], keys: &[&str], times: Vec<Option<i64>>) -> Bytes {
        let mut body = hold.to_vec();
        body.extend_from_slice(b"\0\x05items");
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false), Field::new("valid_micros", DataType::Int64, true),
        ])), vec![Arc::new(StringArray::from(keys.to_vec())), Arc::new(Int64Array::from(times))]).unwrap();
        body.extend(write_arrow_stream(batch.schema_ref(), std::slice::from_ref(&batch), IpcWriteOptions::default()).unwrap());
        body.into()
    }

    fn page(keys: &[&str], valid: Option<i64>) -> Bytes {
        let batch = ipc([
            ("_id", Arc::new(StringArray::from(keys.to_vec())) as ArrayRef),
            ("_op", Arc::new(StringArray::from(vec!["create"; keys.len()]))),
            ("value", Arc::new(Int64Array::from_iter_values(0..keys.len() as i64))),
            ("_valid_from", Arc::new(TimestampMicrosecondArray::from_value(0, keys.len()).with_timezone("UTC"))),
            ("_read_valid_micros", Arc::new(Int64Array::from(vec![valid; keys.len()]))),
        ]);
        let mut body = vec![0, 0];
        body.extend_from_slice(&i64::MAX.to_be_bytes());
        body.extend_from_slice(b"\0\x01\0\x05items");
        body.extend_from_slice(&(batch.len() as u32).to_be_bytes());
        body.extend(batch);
        body.into()
    }

    let running = Running::start(Some("secret"), FlightConfig { owned_native_actions: true, ..FlightConfig::default() }).await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    let commit = "rtdb.owned.CommitOrdered.v1";
    let probe = "rtdb.owned.ReadHeldAbsentKeys.v1";
    let point = "rtdb.owned.ReadHeldPoint.v2";
    client.action(commit, page(&["present"], None)).await.unwrap();
    let opened = client.action("rtdb.owned.OpenRead.v3", Bytes::new()).await.unwrap();
    let hold = opened[0].body.slice(..16);
    let system = i64::from_be_bytes(opened[0].body[16..24].try_into().unwrap());
    let valid = system - 1;
    client.action(commit, page(&["later"], None)).await.unwrap();

    let keys = (0..64).map(|row| format!("row_{row}")).collect::<Vec<_>>();
    let keys = keys.iter().map(String::as_str).collect::<Vec<_>>();
    for (ids, absent) in [(&keys[..], 64), (&["missing", "later", "present", "trailing"][..], 2), (&["present", "missing"][..], 0)] {
        let reply = client.action(probe, point_request(&hold, ids, vec![Some(valid); ids.len()])).await.unwrap();
        let mut expected = vec![1];
        expected.extend_from_slice(&system.to_be_bytes());
        expected.extend_from_slice(&valid.to_be_bytes());
        expected.push(absent);
        assert_eq!(reply[0].body.as_ref(), expected.as_slice());
    }
    let observed = client.action(point, point_request(&hold, &["later"], vec![Some(valid)])).await.unwrap();
    let mut observed = StreamReader::try_new(observed[0].body.as_ref(), None).unwrap();
    assert!(observed.next().is_none());
    assert_eq!(observed.schema().metadata()["rtdb.owned.present_micros"], system.to_string());
    assert_eq!(observed.schema().metadata()["rtdb.owned.valid_micros"], valid.to_string());

    for times in [vec![], vec![Some(valid); 65], vec![Some(valid), None], vec![Some(valid), Some(valid - 1)]] {
        let ids = vec!["missing"; times.len()];
        let error = client.action(probe, point_request(&hold, &ids, times)).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
    let error = client.action(commit, page(&[keys[0], "later"], Some(valid))).await.unwrap_err();
    assert_eq!(error.message(), "The owned point observation changed before ordering");
    let committed = client.action(commit, page(&keys, Some(valid))).await.unwrap();
    assert_eq!(committed.len(), 1);

    let opened = client.action("rtdb.owned.OpenRead.v3", Bytes::new()).await.unwrap();
    let next = opened[0].body.slice(..16);
    for (row, id) in keys.iter().enumerate() {
        let reply = client.action(point, point_request(&next, &[*id], vec![Some(valid)])).await.unwrap();
        let mut rows = StreamReader::try_new(reply[0].body.as_ref(), None).unwrap();
        let batch = rows.next().unwrap().unwrap();
        assert_eq!(integer(&batch, batch.schema().index_of("value").unwrap(), 0), row as i64);
        assert!(rows.next().is_none());
    }
    client.action("rtdb.owned.CloseRead.v1", hold).await.unwrap();
    client.action("rtdb.owned.CloseRead.v1", next).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn owned_v2_layout_reads_both_legs_after_redeclaration() {
    use arrow_array::ArrayRef;
    use arrow_ipc::writer::IpcWriteOptions;
    use rtdb_format::ipc::write_arrow_stream;
    use rtdb_types::Value;

    let running = Running::start(Some("secret"), FlightConfig { owned_native_actions: true, ..FlightConfig::default() }).await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    let capability = client.action("rtdb.owned.LayoutCapabilities.v1", Bytes::new()).await.unwrap();
    assert_eq!(capability[0].body.as_ref(), &[2]);
    let relation = b"\0\x0ctyped-layout";
    let declaration = |kind| {
        let mut body = relation.to_vec();
        body.extend_from_slice(format!(r#"{{"version":2,"fields":[{{"name":"value","type":{{"kind":"{kind}"}},"optional":false,"nullable":false}}]}}"#).as_bytes());
        Bytes::from(body)
    };
    let request = |id, value: Option<ArrayRef>| {
        let mut columns = vec![("_id", Arc::new(Int64Array::from(vec![id])) as ArrayRef)];
        columns.extend(value.map(|value| ("value", value)));
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        let mut body = relation.to_vec();
        body.extend(write_arrow_stream(batch.schema_ref(), &[batch.clone()], IpcWriteOptions::default()).unwrap());
        Bytes::from(body)
    };

    let legacy = client.action("rtdb.owned.DeclareRelation.v1", declaration("i64")).await.unwrap_err();
    assert_eq!(legacy.code(), tonic::Code::InvalidArgument);
    let layout = "rtdb.owned.RelationLayout.v1";
    let selected = client.action(layout, Bytes::from_static(relation)).await.unwrap();
    assert_eq!(selected[0].body.as_ref(), &[0]);
    client.action("rtdb.owned.DeclareRelation.v2", declaration("i64")).await.unwrap();
    let selected = client.action(layout, Bytes::from_static(relation)).await.unwrap();
    assert_eq!(selected[0].body[0], 2);
    assert_eq!(&selected[0].body[1..], &declaration("i64")[relation.len()..]);
    client.action("rtdb.owned.Create.v1", request(1, Some(Arc::new(Int64Array::from(vec![7]))))).await.unwrap();
    client.action("rtdb.owned.DeclareRelation.v2", declaration("utf8")).await.unwrap();
    let selected = client.action(layout, Bytes::from_static(relation)).await.unwrap();
    assert_eq!(selected[0].body[0], 2);
    let installed = rtdb_types::OwnedSchema::from_json(&selected[0].body[1..]).unwrap();
    assert!(installed.validate_row(&rtdb_types::StructFields::from([("value".into(), Value::I64(7))])).is_ok());
    assert!(installed.validate_row(&rtdb_types::StructFields::from([("value".into(), Value::Utf8("seven".into()))])).is_ok());
    let text = Arc::new(StringArray::from(vec!["seven"]));
    client.action("rtdb.owned.Create.v1", request(2, Some(text))).await.unwrap();

    for (id, expected) in [(1, Value::I64(7)), (2, Value::Utf8("seven".into()))] {
        let output = client.action("rtdb.owned.ReadPoint.v1", request(id, None)).await.unwrap();
        let batch = StreamReader::try_new(output[0].body.as_ref(), None).unwrap().next().unwrap().unwrap();
        let schema = batch.schema();
        let (index, field) = schema.column_with_name("value").unwrap();
        assert!(matches!(field.data_type(), DataType::Union(_, arrow_schema::UnionMode::Dense)));
        assert_eq!(rtdb_columnar::read_value(field, batch.column(index).as_ref(), 0).unwrap(), expected);
    }
    running.stop().await;
}

#[tokio::test]
async fn owned_v2_layout_keeps_wrapped_null_distinct_from_an_omitted_field() {
    use arrow_array::{Array, ArrayRef, BinaryArray, Float64Array, NullArray, StructArray, UnionArray};
    use arrow_ipc::writer::IpcWriteOptions;
    use rtdb_format::ipc::write_arrow_stream;
    use rtdb_types::Value;

    let running = Running::start(Some("secret"), FlightConfig { owned_native_actions: true, ..FlightConfig::default() }).await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    let relation = b"\0\x05facts";
    let mut declaration = relation.to_vec();
    declaration.extend_from_slice(br#"{"version":2,"fields":[{"name":"payload","type":{"kind":"union","legs":[{"kind":"binary"},{"kind":"struct","fields":[{"name":"v2_value","type":{"kind":"union","legs":[{"kind":"i64"},{"kind":"f64"}]},"optional":false,"nullable":true}]}]},"optional":true,"nullable":false}]}"#);
    client.action("rtdb.owned.DeclareRelation.v2", declaration.into()).await.unwrap();
    let request = |id, value: Option<ArrayRef>| {
        let mut columns = vec![("_id", Arc::new(Int64Array::from(vec![id])) as ArrayRef)];
        columns.extend(value.map(|value| ("payload", value)));
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        let mut body = relation.to_vec();
        body.extend(write_arrow_stream(batch.schema_ref(), std::slice::from_ref(&batch), IpcWriteOptions::default()).unwrap());
        Bytes::from(body)
    };
    let wrapped = |values: ArrayRef| Arc::new(StructArray::from(vec![
        (Arc::new(Field::new("v2_value", values.data_type().clone(), true)), values),
    ])) as ArrayRef;
    let mixed = UnionArray::try_new(
        arrow_schema::UnionFields::try_new([0, 1, 2], [
            Field::new("null", DataType::Null, true),
            Field::new("i64", DataType::Int64, false),
            Field::new("f64", DataType::Float64, false),
        ]).unwrap(),
        vec![2_i8].into(), Some(vec![0_i32].into()),
        vec![Arc::new(NullArray::new(0)), Arc::new(Int64Array::from(Vec::<i64>::new())), Arc::new(Float64Array::from(vec![-0.0]))],
    ).unwrap();
    for (id, payload) in [
        (1, Some(Arc::new(BinaryArray::from_iter_values([b"legacy"])) as ArrayRef)),
        (2, Some(wrapped(Arc::new(Int64Array::from(vec![11]))))),
        (3, Some(Arc::new(BinaryArray::from_iter_values([[2_u8]])) as ArrayRef)),
        (4, None),
        (5, Some(wrapped(Arc::new(mixed)))),
    ] {
        client.action("rtdb.owned.Create.v1", request(id, payload)).await.unwrap();
    }
    for (id, expected) in [
        (1, Value::Binary(b"legacy".to_vec())),
        (2, Value::Struct(rtdb_types::StructFields::from([("v2_value".into(), Value::I64(11))]))),
        (3, Value::Binary(vec![2])),
        (4, Value::Null),
        (5, Value::Struct(rtdb_types::StructFields::from([("v2_value".into(), Value::F64(-0.0))]))),
    ] {
        let output = client.action("rtdb.owned.ReadPoint.v1", request(id, None)).await.unwrap();
        let batch = StreamReader::try_new(output[0].body.as_ref(), None).unwrap().next().unwrap().unwrap();
        let schema = batch.schema();
        let (index, field) = schema.column_with_name("payload").unwrap();
        let actual = rtdb_columnar::read_value(field, batch.column(index).as_ref(), 0).unwrap();
        assert_eq!(actual, expected);
        if id == 5 {
            let Value::Struct(fields) = actual else { unreachable!() };
            let Some(Value::F64(value)) = fields.get("v2_value") else { unreachable!() };
            assert_eq!(value.to_bits(), (-0.0_f64).to_bits());
        }
    }
    running.stop().await;
}

#[tokio::test]
async fn owned_v2_layout_checks_nested_decimal_precision() {
    use arrow_array::{ArrayRef, Decimal128Array, StructArray};
    use arrow_ipc::writer::IpcWriteOptions;
    use rtdb_format::ipc::write_arrow_stream;

    let running = Running::start(Some("secret"), FlightConfig { owned_native_actions: true, ..FlightConfig::default() }).await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    let relation = b"\0\x08balances";
    let mut declaration = relation.to_vec();
    declaration.extend_from_slice(br#"{"version":2,"fields":[{"name":"balance","type":{"kind":"struct","fields":[{"name":"amount","type":{"kind":"decimal","precision":2,"scale":1,"bitWidth":128},"optional":false,"nullable":false}]},"optional":false,"nullable":false}]}"#);
    client.action("rtdb.owned.DeclareRelation.v2", declaration.into()).await.unwrap();
    let request = |coefficient| {
        let amount = Decimal128Array::from(vec![coefficient]).with_precision_and_scale(2, 1).unwrap();
        let balance = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("amount", DataType::Decimal128(2, 1), false)), Arc::new(amount) as ArrayRef),
        ])) as ArrayRef;
        let batch = RecordBatch::try_from_iter([
            ("_id", Arc::new(Int64Array::from(vec![1])) as ArrayRef), ("balance", balance),
        ]).unwrap();
        let mut body = relation.to_vec();
        body.extend(write_arrow_stream(batch.schema_ref(), std::slice::from_ref(&batch), IpcWriteOptions::default()).unwrap());
        Bytes::from(body)
    };
    let refused = client.action("rtdb.owned.Create.v1", request(100)).await.unwrap_err();
    assert_eq!(refused.message(), "Decimal coefficient exceeds its declared precision");
    client.action("rtdb.owned.Create.v1", request(99)).await.unwrap();
    running.stop().await;
}

#[test]
fn owned_spatial_flight_pages_share_the_original_cut() {
    std::thread::Builder::new().stack_size(rtdb_node::WORKER_STACK_BYTES).spawn(|| {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(check_spatial_pages_at_the_original_cut());
    }).unwrap().join().unwrap();
}

#[test]
fn owned_import_page_preserves_hosted_effect_positions() {
    std::thread::Builder::new().stack_size(rtdb_node::WORKER_STACK_BYTES).spawn(|| {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(check_hosted_effect_positions());
    }).unwrap().join().unwrap();
}

async fn check_hosted_effect_positions() {
    use arrow_array::{ArrayRef, BooleanArray, TimestampMicrosecondArray};
    use arrow_ipc::writer::IpcWriteOptions;
    use rtdb_format::ipc::write_arrow_stream;

    fn ipc(columns: impl IntoIterator<Item = (&'static str, ArrayRef)>) -> Vec<u8> {
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        write_arrow_stream(batch.schema_ref(), std::slice::from_ref(&batch), IpcWriteOptions::default()).unwrap()
    }

    fn group(body: &mut Vec<u8>, relation: &str, arrow: &[u8]) {
        body.extend_from_slice(&(relation.len() as u16).to_be_bytes());
        body.extend_from_slice(relation.as_bytes());
        body.extend_from_slice(&(arrow.len() as u32).to_be_bytes());
        body.extend_from_slice(arrow);
    }

    fn creates(ids: &[&str]) -> Vec<u8> {
        ipc([
            ("_id", Arc::new(StringArray::from(ids.to_vec())) as ArrayRef),
            ("_op", Arc::new(StringArray::from(vec!["create"; ids.len()]))),
        ])
    }

    fn exchange_frames(body: Bytes, prefix_len: usize) -> Vec<arrow_flight::FlightData> {
        let frame = |data_body| arrow_flight::FlightData { data_body, ..Default::default() };
        let size = u32::from_be_bytes(body[prefix_len..][..4].try_into().unwrap()) as usize;
        let end = prefix_len + 4 + size;
        let mut packed = body.slice(prefix_len + 4..end);
        let runs = u16::from_be_bytes(packed[..2].try_into().unwrap()) as usize;
        let stream_count = u16::from_be_bytes(packed[2..4].try_into().unwrap()) as usize;
        let mut frames = vec![frame(body.slice(..prefix_len)), frame(body.slice(end..)), frame(packed.split_to(4 + runs * 2))];
        for _ in 0..stream_count {
            let relation_len = u16::from_be_bytes(packed[..2].try_into().unwrap()) as usize;
            let header = packed.split_to(2 + relation_len + 4);
            let stream_len = u32::from_be_bytes(header[2 + relation_len..].try_into().unwrap()) as usize;
            frames.push(frame(header));
            let mut ipc = packed.split_to(stream_len);
            while !ipc.is_empty() {
                assert_eq!(&ipc[..4], &[255; 4]);
                let len = u32::from_le_bytes(ipc[4..8].try_into().unwrap()) as usize;
                let metadata_end = len + 8;
                let body_len = if len == 0 { 0 } else {
                    arrow_ipc::root_as_message(&ipc[8..metadata_end]).unwrap().bodyLength() as usize
                };
                frames.push(frame(ipc.split_to(metadata_end + body_len)));
            }
        }
        assert!(packed.is_empty());
        frames.push(arrow_flight::FlightData::default());
        frames
    }

    const UPLOAD: &str = "rtdb.owned.DecideHostedStream.v1";
    async fn exchange(client: &mut Client, version: &'static str, frames: Vec<arrow_flight::FlightData>)
        -> std::result::Result<Vec<arrow_flight::FlightData>, Status>
    {
        let mut request = client.request(futures::stream::iter(frames));
        request.metadata_mut().insert("x-rtdb-owned-write", version.parse().unwrap());
        client.client.do_exchange(request).await?.into_inner().try_collect().await
    }

    async fn ordinal(client: &mut Client, relation: &str, id: &str) -> u64 {
        let mut body = (relation.len() as u16).to_be_bytes().to_vec();
        body.extend_from_slice(relation.as_bytes());
        body.extend(ipc([("_id", Arc::new(StringArray::from(vec![id])) as ArrayRef)]));
        let reply = client.action("rtdb.owned.ReadPoint.v1", body.into()).await.unwrap();
        let reader = StreamReader::try_new(reply[0].body.as_ref(), None).unwrap();
        let schema = reader.schema();
        let position = &schema.metadata()["rtdb.owned.event_position"];
        assert_eq!(position.len(), 32);
        u64::from_str_radix(&position[16..], 16).unwrap()
    }

    let running = Running::start_with_placements(Some("secret"), FlightConfig {
        owned_native_actions: true,
        hosted_controls: Some(rtdb_flight_sql::HostedControlConfig {
            owner: "xtdb".into(),
            relations: ["bj_installation", "bj_fact_root_owner"].map(str::to_owned).into(),
        }),
        ..FlightConfig::default()
    }, &["application"]).await;
    let mut client = Client::connect_with_database(&running.server, Some("xtdb:secret"), Some("application")).await;
    client.update("INSERT INTO bj_installation RECORDS {_id: 'authority', kind: 'hosted', incarnation: X'07070707070707070707070707070707', writer: '11111111111111111111111111111111', _valid_from: TIMESTAMP '1970-01-01Z'}", None).await;
    client.update("INSERT INTO bj_fact_root_owner RECORDS {_id: 'root', revision: 1, _valid_from: TIMESTAMP '1970-01-01Z'}", None).await;
    let clock = |batches: &[RecordBatch]| batches[0].column(0).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(0);
    let claim = clock(&client.query("SELECT _system_from FROM bj_installation WHERE _id = 'authority'").await);
    let mut control_revision = clock(&client.query("SELECT _system_from FROM bj_fact_root_owner WHERE _id = 'root'").await);

    let request = |facts: Vec<u8>, expected: i64, revision: i64| {
        let mut body = claim.to_be_bytes().to_vec();
        body.extend([7; 16]);
        body.extend([0x11; 16]);
        body.extend([0, 0, 0]);
        body.extend(i64::MAX.to_be_bytes());
        body.extend((facts.len() as u32).to_be_bytes());
        body.extend(facts);
        body.extend(2u16.to_be_bytes());
        group(&mut body, "bj_installation", &ipc([
            ("_id", Arc::new(StringArray::from(vec!["authority"])) as ArrayRef),
            ("_expected_micros", Arc::new(Int64Array::from(vec![claim]))),
            ("_check_only", Arc::new(BooleanArray::from(vec![true]))),
        ]));
        group(&mut body, "bj_fact_root_owner", &ipc([
            ("_id", Arc::new(StringArray::from(vec!["root"])) as ArrayRef),
            ("_expected_micros", Arc::new(Int64Array::from(vec![expected]))),
            ("revision", Arc::new(Int64Array::from(vec![revision]))),
        ]));
        Bytes::from(body)
    };
    let mut legacy = 3u16.to_be_bytes().to_vec();
    group(&mut legacy, "old_documents", &creates(&["a"]));
    group(&mut legacy, "old_index", &creates(&["posting"]));
    group(&mut legacy, "old_documents", &creates(&["b"]));
    client.action("rtdb.owned.DecideHosted.v4", request(legacy, control_revision, 2)).await.unwrap();
    control_revision = clock(&client.query("SELECT _system_from FROM bj_fact_root_owner WHERE _id = 'root'").await);

    let packed = |last_count: u16| {
        let mut facts = [2u16, 0, 1, 1, last_count].into_iter().flat_map(u16::to_be_bytes).collect::<Vec<_>>();
        group(&mut facts, "new_documents", &creates(&["a", "b"]));
        facts.extend(1u16.to_be_bytes());
        group(&mut facts, "new_index", &creates(&["posting"]));
        facts
    };
    let action = "rtdb.owned.DecideHostedImportPage.v1";
    let error = client.action(action, request(packed(2), control_revision, 3)).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    client.action(action, request(packed(1), control_revision, 3)).await.unwrap();
    for (suffix, id) in [("documents", "a"), ("index", "posting"), ("documents", "b")] {
        let original = ordinal(&mut client, &format!("old_{suffix}"), id).await;
        assert_eq!(ordinal(&mut client, &format!("new_{suffix}"), id).await, original);
    }
    assert_eq!(integer(&client.query("SELECT revision FROM bj_fact_root_owner WHERE _id = 'root'").await[0], 0, 0), 3);
    let batch = |id| RecordBatch::try_from_iter([
        ("_id", Arc::new(StringArray::from(vec![id])) as ArrayRef),
        ("_op", Arc::new(StringArray::from(vec!["create"])) as ArrayRef),
    ]).unwrap();
    let documents = [batch("a"), batch("b")];
    let document_stream = write_arrow_stream(documents[0].schema_ref(), &documents, IpcWriteOptions::default()).unwrap();
    let ordinary_packed = |schedule: [u16; 3], documents_relation: &str, index_relation: &str| {
        let mut facts = [3u16, 2].into_iter().chain(schedule).flat_map(u16::to_be_bytes).collect::<Vec<_>>();
        group(&mut facts, documents_relation, &document_stream);
        group(&mut facts, index_relation, &creates(&["posting"]));
        facts
    };
    control_revision = clock(&client.query("SELECT _system_from FROM bj_fact_root_owner WHERE _id = 'root'").await);
    let action = "rtdb.owned.DecideHostedPacked.v1";
    let error = client.action(action, request(ordinary_packed([0, 1, 1], "packed_documents", "packed_index"), control_revision, 4)).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    client.action(action, request(ordinary_packed([0, 1, 0], "packed_documents", "packed_index"), control_revision, 4)).await.unwrap();
    for (suffix, id) in [("documents", "a"), ("index", "posting"), ("documents", "b")] {
        let original = ordinal(&mut client, &format!("old_{suffix}"), id).await;
        assert_eq!(ordinal(&mut client, &format!("packed_{suffix}"), id).await, original);
    }
    assert_eq!(integer(&client.query("SELECT revision FROM bj_fact_root_owner WHERE _id = 'root'").await[0], 0, 0), 4);

    let list = client.request(arrow_flight::Empty {});
    let advertised: Vec<_> = client.client.list_actions(list).await.unwrap().into_inner().try_collect().await.unwrap();
    assert!(advertised.iter().any(|action| action.r#type == UPLOAD));
    control_revision = clock(&client.query("SELECT _system_from FROM bj_fact_root_owner WHERE _id = 'root'").await);
    let frames = |schedule| exchange_frames(
        request(ordinary_packed(schedule, "streamed_documents", "streamed_index"), control_revision, 5),
        rtdb_tx::HostedAuthority::WIDTH + 11,
    );
    let full = frames([0, 1, 0]);
    assert_eq!(exchange(&mut client, "rtdb.owned.DecideHostedStream.v2", full.clone()).await.unwrap_err().code(), tonic::Code::Unimplemented);
    assert_eq!(exchange(&mut client, UPLOAD, frames([0, 1, 1])).await.unwrap_err().code(), tonic::Code::InvalidArgument);
    let mut truncated = full.clone();
    truncated.pop();
    assert_eq!(exchange(&mut client, UPLOAD, truncated.clone()).await.unwrap_err().code(), tonic::Code::InvalidArgument);
    let mut trailing = full.clone();
    trailing.push(arrow_flight::FlightData::default());
    assert_eq!(exchange(&mut client, UPLOAD, trailing).await.unwrap_err().code(), tonic::Code::InvalidArgument);

    let (entered_pending, pending) = tokio::sync::oneshot::channel();
    let stalled = futures::stream::iter(truncated).chain(futures::stream::once(async move {
        let _ = entered_pending.send(());
        std::future::pending::<arrow_flight::FlightData>().await
    }));
    let mut interrupted = client.request(stalled);
    interrupted.metadata_mut().insert("x-rtdb-owned-write", UPLOAD.parse().unwrap());
    let mut upload_client = client.client.clone();
    let task = tokio::spawn(async move { upload_client.do_exchange(interrupted).await });
    tokio::time::timeout(Duration::from_secs(5), pending).await.unwrap().unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    let mut stale = full.clone();
    let mut prefix = stale[0].data_body.to_vec();
    prefix[24] = 0x22;
    stale[0].data_body = prefix.into();
    let error = exchange(&mut client, UPLOAD, stale).await.unwrap_err();
    assert!(error.message().contains("authority changed before ordering"), "{error}");

    let original_controls = &full[1].data_body[2..];
    let length_at = 2 + "bj_installation".len();
    let size = u32::from_be_bytes(original_controls[length_at..length_at + 4].try_into().unwrap()) as usize;
    let owner_group = &original_controls[length_at + 4 + size..];
    let mut alternative = Vec::new();
    let mut facts = 2u16.to_be_bytes().to_vec();
    group(&mut facts, "streamed_documents", &creates(&["refusal"]));
    group(&mut facts, "streamed_index", &creates(&["refusal"]));
    alternative.extend_from_slice(&(facts.len() as u32).to_be_bytes());
    alternative.extend_from_slice(&facts);
    alternative.extend_from_slice(&1u16.to_be_bytes());
    alternative.extend_from_slice(owner_group);
    let mut body = request(ordinary_packed([0, 1, 0], "streamed_documents", "streamed_index"), control_revision, 5).to_vec();
    let flags = rtdb_tx::HostedAuthority::WIDTH;
    body[flags] = 4;
    body.splice(flags + 1..flags + 1, (alternative.len() as u32).to_be_bytes().into_iter().chain(alternative.iter().copied()));
    let invalid_alternative = exchange_frames(body.clone().into(), flags + 15 + alternative.len());
    let unary = client.action(action, body.into()).await.unwrap_err();
    let streamed = exchange(&mut client, UPLOAD, invalid_alternative).await.unwrap_err();
    assert!(unary.message().contains("must settle the exact original registered dispatch"), "{unary}");
    assert_eq!(streamed.code(), unary.code());
    assert_eq!(streamed.message(), unary.message());
    assert_eq!(integer(&client.query("SELECT revision FROM bj_fact_root_owner WHERE _id = 'root'").await[0], 0, 0), 4);

    let receipt = exchange(&mut client, UPLOAD, full).await.unwrap();
    assert_eq!(receipt.len(), 1);
    assert_eq!(StreamReader::try_new(receipt[0].data_body.as_ref(), None).unwrap().next().unwrap().unwrap().num_rows(), 1);
    for (suffix, id) in [("documents", "a"), ("index", "posting"), ("documents", "b")] {
        let original = ordinal(&mut client, &format!("old_{suffix}"), id).await;
        assert_eq!(ordinal(&mut client, &format!("streamed_{suffix}"), id).await, original);
    }
    assert_eq!(integer(&client.query("SELECT revision FROM bj_fact_root_owner WHERE _id = 'root'").await[0], 0, 0), 5);

    running.stop().await;
}

async fn check_spatial_pages_at_the_original_cut() {
    use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, ListArray, StructArray, UnionArray, builder::{BinaryBuilder, Int64Builder, ListBuilder, StringBuilder}};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_ipc::{reader::StreamReader, writer::IpcWriteOptions};
    use rtdb_format::ipc::write_arrow_stream;

    fn frame(body: &mut Vec<u8>, component: &str) {
        body.extend_from_slice(&u16::try_from(component.len()).unwrap().to_be_bytes());
        body.extend_from_slice(component.as_bytes());
    }

    fn ipc(body: &mut Vec<u8>, columns: impl IntoIterator<Item = (&'static str, ArrayRef)>) {
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        body.extend(write_arrow_stream(batch.schema_ref(), std::slice::from_ref(&batch), IpcWriteOptions::default()).unwrap());
    }

    fn point(longitude: f64) -> Vec<u8> {
        let mut bytes = vec![0, 1];
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&longitude.to_le_bytes());
        bytes.extend_from_slice(&0.0f64.to_le_bytes());
        bytes
    }

    fn canonical_point(longitude: f64) -> Vec<u8> {
        canonical_location(longitude, false)
    }

    fn canonical_location(longitude: f64, multi: bool) -> Vec<u8> {
        let mut image = vec![13];
        image.extend_from_slice(b"coordinates\0\x01");
        image.push(12);
        if multi { image.push(12); }
        for coordinate in [longitude, 0.0] {
            image.push(5);
            image.extend_from_slice(&(coordinate.to_bits() ^ (1 << 63)).to_be_bytes());
        }
        if multi { image.extend_from_slice(b"\0\0"); }
        image.extend_from_slice(b"\0\0type\0\x01\x09");
        image.extend_from_slice(if multi { b"MultiPoint" } else { b"Point" });
        image.extend_from_slice(b"\0\x01\0\0");
        image
    }

    fn select(names: &[&str]) -> ArrayRef {
        let mut selected = ListBuilder::new(StringBuilder::new());
        for name in names { selected.values().append_value(name); }
        selected.append(true);
        Arc::new(selected.finish())
    }

    fn query(
        hold: &[u8], path: &str, valid: i64, after: Option<&str>, region: &[u8],
        predicate: &str, document_columns: Option<ArrayRef>,
    ) -> Bytes {
        query_window(hold, path, valid, after, region, predicate, document_columns, None, None)
    }

    fn query_window(
        hold: &[u8], path: &str, valid: i64, after: Option<&str>, region: &[u8],
        predicate: &str, document_columns: Option<ArrayRef>, window: Option<(f64, Option<f64>)>,
        probe: Option<(&[&str], Option<&str>, i64)>,
    ) -> Bytes {
        let mut body = hold.to_vec();
        frame(&mut body, "places");
        frame(&mut body, path);
        let mut partition = ListBuilder::new(BinaryBuilder::new());
        partition.values().append_value(b"montrouge");
        partition.append(true);
        let mut columns = vec![
            ("partition", Arc::new(partition.finish()) as ArrayRef),
            ("region", Arc::new(BinaryArray::from(vec![region]))),
            ("predicate", Arc::new(StringArray::from(vec![predicate]))),
            ("valid_micros", Arc::new(Int64Array::from(vec![valid]))),
            ("limit", Arc::new(Int64Array::from(vec![probe.map_or(1, |(_, _, limit)| limit)]))),
            ("observe", Arc::new(BooleanArray::from(vec![false]))),
            ("reported_center", Arc::new(BooleanArray::from(vec![false]))),
            ("radius_m", Arc::new(Float64Array::from(vec![window.and_then(|(_, radius)| radius)]))),
            ("after", Arc::new(StringArray::from(vec![after]))),
        ];
        if let Some(selected) = document_columns {
            columns.push(("document_columns", selected));
        }
        if let Some((after_radius, _)) = window {
            columns.push(("after_radius_m", Arc::new(Float64Array::from(vec![after_radius]))));
        }
        if let Some((omittable, point_column, _)) = probe {
            columns.push(("omittable_columns", select(omittable)));
            columns.push(("point_column", Arc::new(StringArray::from(vec![point_column]))));
        }
        ipc(&mut body, columns);
        body.into()
    }

    let running = Running::start(Some("secret"), FlightConfig { owned_native_actions: true, ..FlightConfig::default() }).await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    let mut relation = Vec::new();
    frame(&mut relation, "places");
    let mut schema = relation.clone();
    schema.extend_from_slice(br#"{"version":2,"fields":[
        {"name":"location","type":{"kind":"binary"},"optional":false,"nullable":false},
        {"name":"never_written","type":{"kind":"binary"},"optional":true,"nullable":false},
        {"name":"borough","type":{"kind":"binary"},"optional":false,"nullable":false},
        {"name":"c_location","type":{"kind":"binary"},"optional":true,"nullable":false},
        {"name":"c_mixed","type":{"kind":"binary"},"optional":true,"nullable":false},
        {"name":"c_payload","type":{"kind":"binary"},"optional":true,"nullable":false},
        {"name":"c_compound","type":{"kind":"struct","fields":[
            {"name":"v2_value","type":{"kind":"list","element":{"kind":"struct","fields":[
                {"name":"is_present_note","type":{"kind":"bool"},"optional":false,"nullable":false},
                {"name":"value_note","type":{"kind":"utf8"},"optional":false,"nullable":true}
            ]}},"optional":false,"nullable":true}
        ]},"optional":true,"nullable":false}
    ]}"#);
    client.action("rtdb.owned.DeclareRelation.v2", schema.into()).await.unwrap();

    let path = "by_location";
    for (path, geometry) in [(path, "location"), ("by_unwritten", "never_written")] {
        let mut declaration = relation.clone();
        ipc(&mut declaration, [
            ("_id", Arc::new(StringArray::from(vec![path])) as ArrayRef),
            ("columns", select(&[geometry, "borough"])),
        ]);
        let declaration = Bytes::from(declaration);
        client.action("rtdb.owned.DeclareSpatialPath.v1", declaration.clone()).await.unwrap();
        client.action("rtdb.owned.DeclareSpatialPath.v1", declaration).await.unwrap();
    }

    let canonical_path = "by_canonical_location";
    let mixed_path = "by_mixed_location";
    for (path, geometry) in [(canonical_path, "c_location"), (mixed_path, "c_mixed")] {
        let mut declaration = relation.clone();
        ipc(&mut declaration, [
            ("_id", Arc::new(StringArray::from(vec![path])) as ArrayRef),
            ("columns", select(&[geometry, "borough"])),
            ("geometry_encoding", Arc::new(StringArray::from(vec!["bijection-geometry-v1"]))),
        ]);
        client.action("rtdb.owned.DeclareSpatialPath.v2", declaration.into()).await.unwrap();
    }

    let items = StructArray::from(vec![
        (Arc::new(Field::new("is_present_note", DataType::Boolean, false)), Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef),
        (Arc::new(Field::new("value_note", DataType::Utf8, true)), Arc::new(StringArray::from(vec!["é", "hidden"])) as ArrayRef),
    ]);
    let lists = ListArray::new(
        Arc::new(Field::new("item", items.data_type().clone(), false)),
        OffsetBuffer::new(vec![0, 1, 2, 2, 2].into()),
        Arc::new(items),
        Some(NullBuffer::from(vec![true, true, false, false])),
    );
    let compound = StructArray::new(
        vec![Field::new("v2_value", lists.data_type().clone(), true)].into(),
        vec![Arc::new(lists)],
        Some(NullBuffer::from(vec![true, true, false, true])),
    );
    let mut absent = ListBuilder::new(StringBuilder::new());
    for row in 0..4 {
        if row == 2 {
            absent.values().append_value("c_payload");
            absent.values().append_value("c_compound");
        }
        absent.append(row == 2);
    }
    let mut insert = relation.clone();
    ipc(&mut insert, [
        ("_id", Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as ArrayRef),
        ("_absent", Arc::new(absent.finish())),
        ("location", Arc::new(BinaryArray::from_iter_values([point(1.0), point(2.0), point(1.0), point(1.0)]))),
        ("borough", Arc::new(BinaryArray::from_vec(vec![b"montrouge", b"montrouge", b"elsewhere", b"montrouge"]))),
        ("c_location", Arc::new(BinaryArray::from_iter_values(std::array::from_fn::<_, 4, _>(|_| canonical_point(1.0))))),
        ("c_mixed", Arc::new(BinaryArray::from_iter_values(
            (0..4).map(|row| canonical_location(1.0, row == 1)),
        ))),
        ("c_payload", Arc::new(BinaryArray::from(vec![
            Some(&b"\x09first\0\x01"[..]), Some(&b"\x09old\0\x01"[..]), None, Some(&[2_u8][..]),
        ]))),
        ("c_compound", Arc::new(compound)),
    ]);
    client.action("rtdb.owned.Create.v1", insert.into()).await.unwrap();

    let reply = client.action("rtdb.owned.OpenRead.v3", Bytes::new()).await.unwrap();
    let hold = &reply[0].body[..16];
    let valid = i64::from_be_bytes(reply[0].body[16..24].try_into().unwrap());
    let mut replace = relation.clone();
    replace.extend_from_slice(&i64::MAX.to_be_bytes());
    ipc(&mut replace, [
        ("_id", Arc::new(StringArray::from(vec!["b"])) as ArrayRef),
        ("_op", Arc::new(StringArray::from(vec!["replace"]))),
        ("location", Arc::new(BinaryArray::from_iter_values([point(9.0)]))),
        ("borough", Arc::new(BinaryArray::from_iter_values([b"montrouge"]))),
        ("never_written", Arc::new(BinaryArray::from(vec![None::<&[u8]>]))),
        ("c_location", Arc::new(BinaryArray::from_iter_values([canonical_point(9.0)]))),
        ("c_mixed", Arc::new(BinaryArray::from_iter_values([canonical_point(9.0)]))),
        ("c_payload", Arc::new(BinaryArray::from_iter_values([b"\x09new\0\x01"]))),
        ("_absent", select(&["never_written"])),
    ]);
    client.action("rtdb.owned.Commit.v1", replace.into()).await.unwrap();
    let mut drop = Vec::new();
    ipc(&mut drop, [
        ("relation", Arc::new(StringArray::from(vec!["places"])) as ArrayRef),
        ("name", Arc::new(StringArray::from(vec![path]))),
    ]);
    client.action("rtdb.owned.DropSpatialPath.v2", drop.into()).await.unwrap();
    frame(&mut relation, path);
    client.action("rtdb.owned.DropSpatialPath.v1", relation.into()).await.unwrap();

    let legacy = query_window(hold, path, valid, None, &point(2.0), "intersects",
        Some(select(&["c_location"])), None, Some((&["c_location"], Some("c_location"), 1)));
    let reply = client.action("rtdb.owned.SpatialProbe.v1", legacy).await.unwrap();
    let mut stream = StreamReader::try_new(reply[0].body.as_ref(), None).unwrap();
    assert!(!stream.schema().metadata().contains_key("rtdb.owned.spatial_exact"));
    let first = stream.next().unwrap().unwrap();
    let document = first.column_by_name("document").unwrap().as_any()
        .downcast_ref::<arrow_array::StructArray>().unwrap();
    assert!(document.column_by_name("c_location").is_some());

    let request = query_window(hold, canonical_path, valid, None, &point(1.0), "intersects",
        Some(select(&["c_location", "c_payload", "c_compound", "borough"])), None,
        Some((&["c_location", "c_payload", "c_compound"], Some("c_location"), 1)));
    let reply = client.action("rtdb.owned.SpatialProbe.v1", request).await.unwrap();
    let mut stream = StreamReader::try_new(reply[0].body.as_ref(), None).unwrap();
    let schema = stream.schema();
    assert_eq!(schema.metadata()["rtdb.owned.spatial_exact"], "point-intersects-v1");
    let token = uuid::Uuid::parse_str(&schema.metadata()["rtdb.owned.spatial_filter"]).unwrap();
    let first = stream.next().unwrap().unwrap();
    let document = first.column_by_name("document").unwrap().as_any()
        .downcast_ref::<arrow_array::StructArray>().unwrap();
    assert_eq!(document.column_names(), ["borough", "_system_from"]);
    let sizes = first.column_by_name("omitted_sizes").unwrap().as_any()
        .downcast_ref::<arrow_array::StructArray>().unwrap();
    for (name, value) in [("c_location", 46), ("c_payload", 7), ("c_compound", 13)] {
        let size = sizes.column_by_name(name).unwrap().as_any().downcast_ref::<arrow_array::UInt32Array>().unwrap();
        assert_eq!(size.value(0), value);
    }

    let mut capability = hold.to_vec();
    capability.extend_from_slice(token.as_bytes());
    let mut next = capability.clone();
    next.extend_from_slice(&[1, 0, 0, b'a']);
    let reply = client.action("rtdb.owned.SpatialNext.v1", next.into()).await.unwrap();
    let mut stream = StreamReader::try_new(reply[0].body.as_ref(), None).unwrap();
    assert_eq!(stream.schema().metadata()["rtdb.owned.spatial_exact"], "point-intersects-v1");
    let second = stream.next().unwrap().unwrap();
    assert_eq!(rtdb_columnar::read_entity_id(second.schema_ref().field(0), second.column(0).as_ref(), 0).unwrap(),
        rtdb_columnar::EntityIdRef::String("b"));
    let document = second.column_by_name("document").unwrap().as_any()
        .downcast_ref::<arrow_array::StructArray>().unwrap();
    let compound = document.column_by_name("c_compound").unwrap().as_any().downcast_ref::<StructArray>().unwrap();
    let list = compound.column(0).as_any().downcast_ref::<ListArray>().unwrap().value(0);
    let item = list.as_any().downcast_ref::<StructArray>().unwrap();
    assert!(!item.column(0).as_any().downcast_ref::<BooleanArray>().unwrap().value(0));
    assert_eq!(item.column(1).as_any().downcast_ref::<StringArray>().unwrap().value(0), "hidden");
    let sizes = second.column_by_name("omitted_sizes").unwrap().as_any().downcast_ref::<StructArray>().unwrap();
    assert!(sizes.column_by_name("c_compound").is_none());
    let revision = document.column_by_name("_system_from").unwrap().as_any()
        .downcast_ref::<arrow_array::TimestampMicrosecondArray>().unwrap().value(0);

    let gather = |revision, columns: &[&str]| {
        let mut request = hold.to_vec();
        frame(&mut request, "places");
        ipc(&mut request, [
            ("_id", Arc::new(StringArray::from(vec!["b"])) as ArrayRef),
            ("revision_micros", Arc::new(Int64Array::from(vec![revision]))),
            ("document_columns", select(columns)),
        ]);
        Bytes::from(request)
    };
    for columns in [&["c_location", "c_payload", "borough"][..], &["c_payload"][..]] {
        let reply = client.action("rtdb.owned.SpatialGather.v1", gather(revision, columns)).await.unwrap();
        let mut stream = StreamReader::try_new(reply[0].body.as_ref(), None).unwrap();
        assert_eq!(stream.schema().metadata()["rtdb.owned.valid_micros"], valid.to_string());
        let winner = stream.next().unwrap().unwrap();
        let document = winner.column_by_name("document").unwrap().as_any()
            .downcast_ref::<arrow_array::StructArray>().unwrap();
        if columns.len() == 1 {
            assert_eq!(document.column_names(), ["c_payload", "_system_from"]);
        }
        let payload = document.column_by_name("c_payload").unwrap().as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(payload.value(0), b"\x09old\0\x01");
        assert!(winner.column_by_name("omitted_sizes").is_none());
    }
    assert_eq!(client.action("rtdb.owned.SpatialGather.v1", gather(revision + 1, &["c_payload"])).await.unwrap_err().code(),
        tonic::Code::Internal);
    client.action("rtdb.owned.SpatialClose.v1", capability.into()).await.unwrap();

    let request = query_window(hold, canonical_path, valid, None, &point(1.0), "intersects",
        Some(select(&["c_compound"])), None, Some((&["c_compound"], None, 2)));
    let reply = client.action("rtdb.owned.SpatialProbe.v1", request).await.unwrap();
    let mut stream = StreamReader::try_new(reply[0].body.as_ref(), None).unwrap();
    let page = stream.next().unwrap().unwrap();
    assert!(page.column_by_name("omitted_sizes").is_none());
    let document = page.column_by_name("document").unwrap().as_any().downcast_ref::<StructArray>().unwrap();
    let compound = document.column_by_name("c_compound").unwrap().as_any().downcast_ref::<StructArray>().unwrap();
    let lists = compound.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    for (row, expected) in ["é", "hidden"].into_iter().enumerate() {
        let list = lists.value(row);
        let item = list.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(item.column(1).as_any().downcast_ref::<StringArray>().unwrap().value(0), expected);
    }
    let token = uuid::Uuid::parse_str(&stream.schema().metadata()["rtdb.owned.spatial_filter"]).unwrap();
    let mut capability = hold.to_vec();
    capability.extend_from_slice(token.as_bytes());
    client.action("rtdb.owned.SpatialClose.v1", capability.into()).await.unwrap();

    let request = query_window(hold, mixed_path, valid, None, &point(1.0), "intersects",
        Some(select(&["c_mixed", "c_payload", "c_compound", "borough"])), None,
        Some((&["c_mixed", "c_payload", "c_compound"], Some("c_mixed"), 1)));
    let reply = client.action("rtdb.owned.SpatialProbe.v1", request).await.unwrap();
    let mut stream = StreamReader::try_new(reply[0].body.as_ref(), None).unwrap();
    assert!(!stream.schema().metadata().contains_key("rtdb.owned.spatial_exact"));
    let token = uuid::Uuid::parse_str(&stream.schema().metadata()["rtdb.owned.spatial_filter"]).unwrap();
    let first = stream.next().unwrap().unwrap();
    assert_eq!(rtdb_columnar::read_entity_id(first.schema_ref().field(0), first.column(0).as_ref(), 0).unwrap(),
        rtdb_columnar::EntityIdRef::String("a"));
    let document = first.column_by_name("document").unwrap().as_any()
        .downcast_ref::<arrow_array::StructArray>().unwrap();
    assert!(document.column_by_name("c_mixed").is_some());

    let mut mixed_capability = hold.to_vec();
    mixed_capability.extend_from_slice(token.as_bytes());
    for after in [b'b', b'b', b'z'] {
        let mut next = mixed_capability.clone();
        next.extend_from_slice(&[1, 0, 0, after]);
        let reply = client.action("rtdb.owned.SpatialNext.v1", next.into()).await.unwrap();
        let mut stream = StreamReader::try_new(reply[0].body.as_ref(), None).unwrap();
        let schema = stream.schema();
        assert_eq!(schema.metadata()["rtdb.owned.spatial_more"], "0");
        assert!(!schema.metadata().contains_key("rtdb.owned.spatial_exact"));
        if after == b'z' {
            assert!(stream.next().is_none());
        } else {
            let last = stream.next().unwrap().unwrap();
            assert_eq!(rtdb_columnar::read_entity_id(last.schema_ref().field(0), last.column(0).as_ref(), 0).unwrap(),
                rtdb_columnar::EntityIdRef::String("d"));
            let document = last.column_by_name("document").unwrap().as_any()
                .downcast_ref::<arrow_array::StructArray>().unwrap();
            assert!(document.column_by_name("c_mixed").is_some());
            let sizes = last.column_by_name("omitted_sizes").unwrap().as_any()
                .downcast_ref::<arrow_array::StructArray>().unwrap();
            for field in ["c_payload", "c_compound"] {
                let size = sizes.column_by_name(field).unwrap().as_any().downcast_ref::<arrow_array::UInt32Array>().unwrap();
                assert_eq!(size.value(0), 1);
            }
        }
    }
    client.action("rtdb.owned.SpatialClose.v1", mixed_capability.into()).await.unwrap();

    let projected = "rtdb.owned.SpatialHeld.v3";
    for (action, documents, selected) in [
        ("rtdb.owned.SpatialHeld.v1", false, None),
        ("rtdb.owned.SpatialHeld.v2", true, None),
        (projected, true, Some(select(&["borough", "never_written"]))),
    ] {
        for (after, expected, more, longitude) in [(None, "a", "1", 1.0), (Some("a"), "b", "1", 2.0)] {
            let request = query(hold, path, valid, after, &[], "all", selected.clone());
            let replies = client.action(action, request).await.unwrap();
            let mut stream = StreamReader::try_new(replies[0].body.as_ref(), None).unwrap();
            assert_eq!(stream.schema().metadata()["rtdb.owned.spatial_more"], more);
            assert_eq!(stream.schema().metadata()["rtdb.owned.present_micros"], valid.to_string());
            let batch = stream.next().unwrap().unwrap();
            let ids = batch.column(0).as_any().downcast_ref::<UnionArray>().unwrap();
            assert_eq!(ids.value(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), expected);
            assert_eq!(batch.num_columns(), if documents { 3 } else { 2 });
            if documents {
                let document = batch.column(2).as_any().downcast_ref::<arrow_array::StructArray>().unwrap();
                let revision = document.column_by_name("_system_from").unwrap().as_any()
                    .downcast_ref::<arrow_array::TimestampMicrosecondArray>().unwrap();
                let borough = document.column_by_name("borough").unwrap().as_any().downcast_ref::<BinaryArray>().unwrap();
                assert_eq!(borough.value(0), b"montrouge");
                if selected.is_some() {
                    assert_eq!(document.column_names(), ["borough", "_system_from"]);
                    assert!(document.column_by_name("location").is_none());
                    assert!(document.column_by_name("never_written").is_none());
                } else {
                    let field = &document.fields()[0];
                    assert_eq!(field.name(), "_id");
                    assert_eq!(rtdb_columnar::read_owned_entity_id(field, document.column(0).as_ref(), 0).unwrap(),
                        rtdb_columnar::EntityIdRef::String(expected));
                    let location = document.column_by_name("location").unwrap().as_any().downcast_ref::<BinaryArray>().unwrap();
                    assert_eq!(location.value(0), point(longitude));
                }
                assert!(revision.is_valid(0) && revision.value(0) <= valid);
            }
            assert!(stream.next().is_none());
        }

        let request = query(hold, path, valid, None, &point(20.0), "intersects", selected);
        let replies = client.action(action, request).await.unwrap();
        let mut empty = StreamReader::try_new(replies[0].body.as_ref(), None).unwrap();
        let schema = empty.schema();
        assert_eq!(schema.metadata()["rtdb.owned.spatial_more"], "0");
        if action == projected {
            let document = schema.field_with_name("document").unwrap();
            let DataType::Struct(fields) = document.data_type() else { panic!("Expected a document projection"); };
            assert_eq!(
                fields.iter().map(|field| field.name().as_str()).collect::<Vec<_>>(),
                ["borough", "_system_from"],
            );
        }
        assert!(empty.next().is_none());
    }
    let request = query(hold, path, valid, None, &point(2.0), "intersects", Some(select(&[])));
    let mut input = client.request(Action { r#type: projected.into(), body: request });
    input.metadata_mut().insert("x-rtdb-owned-ipc", "stream-v1".parse().unwrap());
    let reply = client.client.do_action(input).await.unwrap();
    assert_eq!(reply.metadata().get("x-rtdb-owned-ipc").unwrap(), "stream-v1");
    let frames: Vec<_> = reply.into_inner().try_collect().await.unwrap();
    assert_eq!(frames.len(), 3);
    assert_eq!(frames.last().unwrap().body.as_ref(), &[255, 255, 255, 255, 0, 0, 0, 0]);
    let encoded = frames.iter().flat_map(|frame| frame.body.iter().copied()).collect::<Vec<_>>();
    let mut streamed = StreamReader::try_new(encoded.as_slice(), None).unwrap();
    assert_eq!(streamed.schema().metadata()["rtdb.owned.present_micros"], valid.to_string());
    let batch = streamed.next().unwrap().unwrap();
    assert!(streamed.next().is_none());
    let document = batch.column(2).as_any().downcast_ref::<arrow_array::StructArray>().unwrap();
    assert_eq!(document.column_names(), ["_system_from"]);
    let ids = batch.column(0).as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(ids.value(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "b");

    for (path, geometry, expected) in [(path, "location", true), ("by_unwritten", "never_written", false)] {
        let request = query(hold, path, valid, None, &[], "all", Some(select(&[geometry])));
        let replies = client.action(projected, request).await.unwrap();
        let batch = StreamReader::try_new(replies[0].body.as_ref(), None).unwrap().next().unwrap().unwrap();
        let document = batch.column(2).as_any().downcast_ref::<arrow_array::StructArray>().unwrap();
        assert_eq!(document.column_by_name(geometry).is_some(), expected);
    }

    let mut region = vec![0, 1];
    region.extend_from_slice(&3u32.to_le_bytes());
    region.extend_from_slice(&1u32.to_le_bytes());
    region.extend_from_slice(&5u32.to_le_bytes());
    for (longitude, latitude) in [(0.0f64, -1.0f64), (3.0, -1.0), (3.0, 1.0), (0.0, 1.0), (0.0, -1.0)] {
        region.extend_from_slice(&longitude.to_le_bytes());
        region.extend_from_slice(&latitude.to_le_bytes());
    }
    let request = query(hold, path, valid, None, &region, "intersects", Some(select(&["borough"])));
    let first = client.action("rtdb.owned.SpatialOpen.v1", request).await.unwrap();
    let mut first = StreamReader::try_new(first[0].body.as_ref(), None).unwrap();
    let first_schema = first.schema();
    let token = uuid::Uuid::parse_str(&first_schema.metadata()["rtdb.owned.spatial_filter"]).unwrap();
    assert_eq!(first_schema.metadata()["rtdb.owned.spatial_more"], "1");
    let first = first.next().unwrap().unwrap();
    assert_eq!(rtdb_columnar::read_entity_id(first.schema_ref().field(0), first.column(0).as_ref(), 0).unwrap(),
        rtdb_columnar::EntityIdRef::String("a"));

    let other = client.action("rtdb.owned.OpenRead.v3", Bytes::new()).await.unwrap();
    let mut capability = hold.to_vec();
    capability.extend_from_slice(token.as_bytes());
    let mut next = capability.clone();
    next.extend_from_slice(&[1, 0, 0, b'a']);
    let mut foreign = next.clone();
    foreign[..16].copy_from_slice(&other[0].body[..16]);
    assert_eq!(client.action("rtdb.owned.SpatialNext.v1", foreign.into()).await.unwrap_err().code(), tonic::Code::NotFound);
    client.action("rtdb.owned.CloseRead.v1", other[0].body.slice(..16)).await.unwrap();

    for _ in 0..2 {
        let resumed = client.action("rtdb.owned.SpatialNext.v1", Bytes::copy_from_slice(&next)).await.unwrap();
        let mut resumed = StreamReader::try_new(resumed[0].body.as_ref(), None).unwrap();
        let schema = resumed.schema();
        assert_eq!(schema.metadata()["rtdb.owned.present_micros"], valid.to_string());
        assert_eq!(schema.metadata()["rtdb.owned.spatial_more"], "1");
        assert_eq!(schema.metadata()["rtdb.owned.spatial_filter"], token.simple().to_string());
        let resumed = resumed.next().unwrap().unwrap();
        assert_eq!(rtdb_columnar::read_entity_id(resumed.schema_ref().field(0), resumed.column(0).as_ref(), 0).unwrap(),
            rtdb_columnar::EntityIdRef::String("b"));
    }
    let request = query(hold, path, valid, Some("a"), &region, "intersects", Some(select(&["borough"])));
    let anchored = client.action("rtdb.owned.SpatialOpen.v1", request).await.unwrap();
    let anchored = StreamReader::try_new(anchored[0].body.as_ref(), None).unwrap();
    let bounded_token = uuid::Uuid::parse_str(&anchored.schema().metadata()["rtdb.owned.spatial_filter"]).unwrap();
    let mut earlier = hold.to_vec();
    earlier.extend_from_slice(bounded_token.as_bytes());
    earlier.extend_from_slice(&[1, 0, 0, b'0']);
    let reply = client.action("rtdb.owned.SpatialNext.v1", earlier.into()).await.unwrap();
    let earlier = StreamReader::try_new(reply[0].body.as_ref(), None).unwrap().next().unwrap().unwrap();
    assert_eq!(rtdb_columnar::read_entity_id(earlier.schema_ref().field(0), earlier.column(0).as_ref(), 0).unwrap(),
        rtdb_columnar::EntityIdRef::String("a"));

    let mut last = capability.clone();
    last.extend_from_slice(&[1, 0, 0, b'b']);
    let last = client.action("rtdb.owned.SpatialNext.v1", last.into()).await.unwrap();
    let mut last = StreamReader::try_new(last[0].body.as_ref(), None).unwrap();
    let schema = last.schema();
    assert_eq!(schema.metadata()["rtdb.owned.spatial_more"], "0");
    assert!(!schema.metadata().contains_key("rtdb.owned.spatial_filter"));
    let last = last.next().unwrap().unwrap();
    assert_eq!(rtdb_columnar::read_entity_id(last.schema_ref().field(0), last.column(0).as_ref(), 0).unwrap(),
        rtdb_columnar::EntityIdRef::String("d"));
    client.action("rtdb.owned.SpatialClose.v1", capability.into()).await.unwrap();
    assert_eq!(client.action("rtdb.owned.SpatialNext.v1", next.into()).await.unwrap_err().code(), tonic::Code::NotFound);
    let mut rank_capability = hold.to_vec();
    let mut continuation: Option<Bytes> = None;
    let mut previous_distance = None;
    let mut tied_distance = None;
    for (expected, more) in [("a", true), ("d", true), ("b", false)] {
        let replies = if let Some(next) = &continuation {
            client.action("rtdb.owned.SpatialRankNext.v1", next.clone()).await.unwrap()
        } else {
            let open = query(hold, path, valid, None, &point(0.0), "nearest", Some(select(&["borough"])));
            client.action("rtdb.owned.SpatialRankOpen.v1", open).await.unwrap()
        };
        let mut stream = StreamReader::try_new(replies[0].body.as_ref(), None).unwrap();
        let schema = stream.schema();
        assert_eq!(schema.metadata()["rtdb.owned.present_micros"], valid.to_string());
        assert_eq!(schema.metadata()["rtdb.owned.spatial_more"], if more { "1" } else { "0" });
        let batch = stream.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let ids = batch.column(0).as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(ids.value(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), expected);
        let document = batch.column(2).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(document.column_names(), ["borough", "_system_from"]);
        let borough = document.column_by_name("borough").unwrap().as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(borough.value(0), b"montrouge");
        let distance = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
        if expected == "a" { tied_distance = Some(distance); }
        if let Some(bound) = schema.metadata().get("rtdb.owned.spatial_next_distance_m") {
            assert!(bound.parse::<f64>().unwrap() >= distance);
            assert!(more);
        } else {
            assert!(!more);
        }
        if expected == "d" { assert_eq!(Some(distance), previous_distance); }
        if more {
            let token = uuid::Uuid::parse_str(&schema.metadata()["rtdb.owned.spatial_filter"]).unwrap();
            if continuation.is_none() {
                rank_capability.extend_from_slice(token.as_bytes());
            } else {
                assert_eq!(&rank_capability[16..], token.as_bytes());
            }
            let mut next = rank_capability.clone();
            next.extend_from_slice(&[1, 0]);
            next.extend_from_slice(&distance.to_be_bytes());
            next.extend_from_slice(&[0]);
            next.extend_from_slice(expected.as_bytes());
            if expected == "a" {
                let mut malformed = next.clone();
                malformed[34..42].copy_from_slice(&0.0_f64.to_be_bytes());
                assert_eq!(client.action("rtdb.owned.SpatialRankNext.v1", malformed.into()).await.unwrap_err().code(), tonic::Code::InvalidArgument);
            }
            continuation = Some(next.into());
        } else {
            assert!(!schema.metadata().contains_key("rtdb.owned.spatial_filter"));
        }
        previous_distance = Some(distance);
    }
    let last = continuation.unwrap();
    let retried = client.action("rtdb.owned.SpatialRankNext.v1", last.clone()).await.unwrap();
    let batch = StreamReader::try_new(retried[0].body.as_ref(), None).unwrap().next().unwrap().unwrap();
    assert_eq!(rtdb_columnar::read_entity_id(batch.schema_ref().field(0), batch.column(0).as_ref(), 0).unwrap(),
        rtdb_columnar::EntityIdRef::String("b"));
    client.action("rtdb.owned.SpatialClose.v1", rank_capability.into()).await.unwrap();
    assert_eq!(client.action("rtdb.owned.SpatialRankNext.v1", last).await.unwrap_err().code(), tonic::Code::NotFound);

    let complete_radius = tied_distance.unwrap();
    let outside_radius = previous_distance.unwrap();
    let window = |lower, upper| query_window(hold, path, valid, None, &point(0.0), "nearest",
        Some(select(&["location"])), Some((lower, upper)), None);
    let expanded = client.action("rtdb.owned.SpatialRankOpen.v2", window(complete_radius, Some(outside_radius))).await.unwrap();
    let mut stream = StreamReader::try_new(expanded[0].body.as_ref(), None).unwrap();
    assert_eq!(stream.schema().metadata()["rtdb.owned.present_micros"], valid.to_string());
    assert_eq!(stream.schema().metadata()["rtdb.owned.spatial_more"], "0");
    let batch = stream.next().unwrap().unwrap();
    assert_eq!(rtdb_columnar::read_entity_id(batch.schema_ref().field(0), batch.column(0).as_ref(), 0).unwrap(),
        rtdb_columnar::EntityIdRef::String("b"));
    assert_eq!(batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap().value(0), outside_radius);
    let geometry = batch.column(2).as_any().downcast_ref::<arrow_array::StructArray>().unwrap()
        .column_by_name("location").unwrap().as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(geometry.value(0), point(2.0));
    assert!(stream.next().is_none());
    let empty = client.action("rtdb.owned.SpatialRankOpen.v2", window(outside_radius, Some(outside_radius))).await.unwrap();
    let mut stream = StreamReader::try_new(empty[0].body.as_ref(), None).unwrap();
    assert_eq!(stream.schema().metadata()["rtdb.owned.spatial_more"], "0");
    assert!(stream.next().is_none());
    for lower in [f64::NAN, f64::INFINITY, -1.0, outside_radius + 1.0] {
        assert_eq!(client.action("rtdb.owned.SpatialRankOpen.v2", window(lower, Some(outside_radius))).await.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    let mut wrong_type = ListBuilder::new(Int64Builder::new());
    wrong_type.append(true);
    for columns in [select(&["borough", "borough"]), Arc::new(wrong_type.finish())] {
        let request = query(hold, path, valid, None, &[], "all", Some(columns));
        assert_eq!(client.action(projected, request).await.unwrap_err().code(), tonic::Code::InvalidArgument);
    }
    client.action("rtdb.owned.CloseRead.v1", Bytes::copy_from_slice(hold)).await.unwrap();
    running.stop().await;
}

#[test]
fn held_root_rank_flight_preserves_typed_ties_and_the_empty_window_schema() {
    std::thread::Builder::new().stack_size(rtdb_node::WORKER_STACK_BYTES).spawn(|| {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(check_held_root_rank_flight());
    }).unwrap().join().unwrap();
}

async fn check_held_root_rank_flight() {
    use arrow_array::{Float64Array, UInt64Array};
    use arrow_ipc::writer::IpcWriteOptions;
    use rtdb_columnar::{EntityIdRef, entity_id_array, read_entity_id};
    use rtdb_format::ipc::write_arrow_stream;
    use sha2::{Digest, Sha256};

    fn frame(body: &mut Vec<u8>, component: &str) {
        body.extend_from_slice(&(component.len() as u16).to_be_bytes());
        body.extend_from_slice(component.as_bytes());
    }

    let running = Running::start(Some("secret"), FlightConfig {
        owned_native_actions: true, ..FlightConfig::default()
    }).await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    for (relation, path, ids) in [
        ("rank_alpha", "alpha_body", vec![EntityIdRef::Integer(1), EntityIdRef::String("a")]),
        ("rank_beta", "beta_body", vec![EntityIdRef::String("a")]),
    ] {
        let mut prefix = Vec::new();
        frame(&mut prefix, relation);
        let mut declaration = prefix.clone();
        declaration.extend_from_slice(br#"{"fields":[{"name":"body","type":{"kind":"utf8"},"optional":false,"nullable":false}]}"#);
        client.action("rtdb.owned.DeclareRelation.v1", declaration.into()).await.unwrap();

        let path = RecordBatch::try_from_iter([
            ("_id", Arc::new(StringArray::from(vec![path])) as arrow_array::ArrayRef),
            ("column", Arc::new(StringArray::from(vec!["body"]))),
            ("analyzer", Arc::new(StringArray::from(vec!["simple"]))),
        ]).unwrap();
        let mut declaration = prefix.clone();
        declaration.extend(write_arrow_stream(path.schema_ref(), std::slice::from_ref(&path), IpcWriteOptions::default()).unwrap());
        client.action("rtdb.owned.DeclareSearchPath.v2", declaration.into()).await.unwrap();

        let text = Arc::new(StringArray::from(vec!["invoice"; ids.len()]));
        let batch = RecordBatch::try_from_iter([("_id", entity_id_array(ids).unwrap()), ("body", text)]).unwrap();
        let mut insert = prefix;
        insert.extend(write_arrow_stream(batch.schema_ref(), std::slice::from_ref(&batch), IpcWriteOptions::default()).unwrap());
        client.action("rtdb.owned.Create.v1", insert.into()).await.unwrap();
    }

    let opened = client.action("rtdb.owned.OpenRead.v3", Bytes::new()).await.unwrap();
    let handle = opened[0].body.slice(..16);
    let present = i64::from_be_bytes(opened[0].body[16..24].try_into().unwrap());
    let mut frame_bytes = handle.to_vec();
    frame_bytes.extend(i64::MIN.to_be_bytes());
    frame_bytes.extend(0u32.to_be_bytes());
    frame_bytes.extend(2u16.to_be_bytes());
    for (relation, path) in [("rank_beta", "beta_body"), ("rank_alpha", "alpha_body")] {
        frame(&mut frame_bytes, relation);
        frame(&mut frame_bytes, path);
    }
    let mut digest = Sha256::new();
    digest.update(b"rtdb.owned.search.held.v5\0");
    digest.update(&frame_bytes);
    digest.update(b"invoice");
    let scope = format!("{:x}", digest.finalize());
    let search = "rtdb.owned.SearchHeld.v7";
    let list = client.request(arrow_flight::Empty {});
    let advertised: Vec<_> = client.client.list_actions(list).await.unwrap()
        .into_inner().try_collect().await.unwrap();
    for action in [search, "rtdb.owned.NearestHeld.v4", "rtdb.owned.HybridHeld.v4"] {
        assert!(advertised.iter().any(|entry| entry.r#type == action));
    }
    assert!(advertised.iter().all(|entry| entry.r#type != "rtdb.owned.SearchHeld.v6"));
    let old = client.action("rtdb.owned.SearchHeld.v6", Bytes::new()).await.unwrap_err();
    assert_eq!(old.code(), tonic::Code::Unimplemented);
    let request = |offset: u32, limit: u16| {
        let mut body = frame_bytes.clone();
        body.extend(offset.to_be_bytes());
        body.extend(limit.to_be_bytes());
        body.push(0);
        body.extend(b"invoice");
        Bytes::from(body)
    };
    let mut first_schema: Option<Arc<Schema>> = None;
    for (offset, limit, expected) in [
        (0, 3, vec![(EntityIdRef::String("a"), 0), (EntityIdRef::String("a"), 1), (EntityIdRef::Integer(1), 1)]),
        (1, 1, vec![(EntityIdRef::String("a"), 1)]),
        (3, 1, vec![]),
    ] {
        let mut input = client.request(Action { r#type: search.into(), body: request(offset, limit) });
        input.metadata_mut().insert("x-rtdb-owned-ipc", "stream-v1".parse().unwrap());
        let streamed = client.client.do_action(input).await.unwrap();
        assert_eq!(streamed.metadata().get("x-rtdb-owned-ipc").unwrap(), "stream-v1");
        let frames: Vec<_> = streamed.into_inner().try_collect().await.unwrap();
        assert_eq!(frames.len(), if expected.is_empty() { 3 } else { 4 });
        assert_eq!(frames[0].body.as_ref(), &[0, 0]);
        assert_eq!(frames.last().unwrap().body.as_ref(), &[255, 255, 255, 255, 0, 0, 0, 0]);
        let encoded = frames[1..].iter().flat_map(|frame| frame.body.iter().copied()).collect::<Vec<_>>();

        let mut stream = StreamReader::try_new(encoded.as_slice(), None).unwrap();
        let schema = stream.schema();
        let metadata = schema.metadata();
        assert_eq!(metadata["rtdb.owned.search_total"], "3");
        assert_eq!(metadata["rtdb.owned.search_scope"], scope);
        assert_eq!(metadata["rtdb.owned.present_micros"], present.to_string());
        assert_eq!(metadata["rtdb.owned.valid_micros"], present.to_string());
        assert!(!metadata.contains_key("rtdb.owned.search_witnesses"));
        assert!(!metadata.contains_key("rtdb.owned.rank_families"));
        assert_eq!(schema.field(0), &rtdb_columnar::to_field("_id", &rtdb_columnar::entity_id_type()).unwrap());
        assert_eq!(schema.field(1), &Field::new("score", DataType::Float64, false));
        assert_eq!(schema.field(2), &Field::new("partition", DataType::UInt64, false));
        assert_eq!(schema.as_ref(), first_schema.get_or_insert_with(|| Arc::clone(&schema)).as_ref());
        if !expected.is_empty() {
            let batch = stream.next().unwrap().unwrap();
            let scores = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            let partitions = batch.column(2).as_any().downcast_ref::<UInt64Array>().unwrap();
            assert_eq!(batch.num_rows(), expected.len());
            assert!(scores.value(0) > 0.0);
            for (row, (id, partition)) in expected.into_iter().enumerate() {
                assert_eq!(read_entity_id(schema.field(0), batch.column(0).as_ref(), row).unwrap(), id);
                assert_eq!(partitions.value(row), partition);
                assert_eq!(scores.value(row).to_bits(), scores.value(0).to_bits());
            }
        }
        assert!(stream.next().is_none());
    }
    client.action("rtdb.owned.CloseRead.v1", handle).await.unwrap();
    let unary = client.action(search, request(0, 1)).await.unwrap_err();
    assert_eq!(unary.code(), tonic::Code::NotFound);
    let mut input = client.request(Action { r#type: search.into(), body: request(0, 1) });
    input.metadata_mut().insert("x-rtdb-owned-ipc", "stream-v1".parse().unwrap());
    let streamed = client.client.do_action(input).await.unwrap_err();
    assert_eq!((streamed.code(), streamed.message()), (unary.code(), unary.message()));
    running.stop().await;
}

#[tokio::test]
async fn hosted_source_confirmation_checks_the_exact_prior_positions() {
    let running = Running::start_with_fence(Some("secret"), FlightConfig {
        owned_native_actions: true,
        hosted_controls: Some(rtdb_flight_sql::HostedControlConfig {
            owner: "xtdb".into(),
            relations: ["bj_installation".to_owned()].into(),
        }),
        ..FlightConfig::default()
    }, &["application"], None, NodeOptions::default(), Some(Arc::new(FixtureSourceFence))).await;
    let mut owner = Client::connect_with_database(&running.server, Some("xtdb:secret"), Some("application")).await;
    let action = "rtdb.owned.ConfirmHostedSource.v1";
    let request = |history: uuid::Uuid, positions: &[(i64, i64)]| {
        let mut body = history.as_bytes().to_vec();
        body.push(positions.len() as u8);
        for (id, time) in positions {
            body.extend_from_slice(&id.to_be_bytes());
            body.extend_from_slice(&time.to_be_bytes());
        }
        Bytes::from(body)
    };
    let applied = |reply: &[arrow_flight::Result]| {
        let mut stream = StreamReader::try_new(reply[0].body.as_ref(), None).unwrap();
        let rows = stream.next().unwrap().unwrap();
        assert_eq!(rows.num_rows(), 1);
        assert!(stream.next().is_none());
        (integer(&rows, 0, 0), integer(&rows, 1, 0))
    };
    owner.update("INSERT INTO data RECORDS {_id: 'first'}", None).await;
    let first = applied(&owner.action(action, request(FIXTURE_SOURCE_HISTORY, &[])).await.unwrap());
    owner.update("INSERT INTO data RECORDS {_id: 'second'}", None).await;
    let second = applied(&owner.action(action, request(FIXTURE_SOURCE_HISTORY, &[first])).await.unwrap());
    assert!(second.0 > first.0 && second.1 >= first.1);
    assert!(owner.action(action, request(FIXTURE_SOURCE_HISTORY, &[(first.0, first.1 - 1)])).await.is_err());
    assert!(owner.action(action, request(uuid::Uuid::from_u128(1), &[])).await.is_err());
    running.stop().await;
}

#[tokio::test]
async fn held_root_catalog_keeps_its_cut_and_excludes_retired_declarations() {
    #[derive(Debug)]
    struct FreshPeer;

    impl rtdb_flight_sql::FlightPeerVerifier for FreshPeer {
        fn verify(&self, proof: &str, _: &rtdb_node::Database) -> Result<(std::net::IpAddr, bool)> {
            assert_eq!(proof, "fresh-fixture");
            Ok((std::net::Ipv4Addr::LOCALHOST.into(), true))
        }
    }

    let running = Running::start_with_placements(Some("secret"), FlightConfig {
        owned_native_actions: true,
        forwarded_peer: Some(Arc::new(FreshPeer)),
        hosted_controls: Some(rtdb_flight_sql::HostedControlConfig {
            owner: "xtdb".into(),
            relations: ["bj_relation_declarations".to_owned()].into(),
        }),
        ..FlightConfig::default()
    }, &["application"]).await;
    let mut owner = Client::connect_with_database(&running.server, Some("xtdb:secret"), Some("application")).await;
    let mut other = Client::connect_with_database(&running.server, Some("xtdb:secret"), Some("application")).await;
    for sql in [
        "INSERT INTO bj_relation_declarations RECORDS {_id: 'original', table_name: 'original', world: CAST(0 AS BIGINT), declaration: X'01', names: ['location'], _valid_from: TIMESTAMP '1970-01-01Z'}",
        "INSERT INTO bj_relation_declarations RECORDS {_id: 'retired', table_name: 'retired', world: CAST(-1 AS BIGINT), declaration: X'02', names: [], _valid_from: TIMESTAMP '1970-01-01Z'}",
    ] {
        owner.update(sql, None).await;
    }
    let open = owner.action("rtdb.owned.OpenRead.v3", Bytes::new()).await.unwrap();
    let hold = &open[0].body[..16];
    let request = |selector| Bytes::from([hold, &[selector]].concat());
    let action = "rtdb.owned.ReadHeldRootCatalog.v1";
    assert_eq!(other.action(action, request(7)).await.unwrap_err().code(), tonic::Code::NotFound);
    let mut replica = Client::connect_with_peer(
        &running.server, Some("xtdb:secret"), Some("application"), Some("fresh-fixture"),
    ).await;
    assert_eq!(replica.action(action, request(7)).await.unwrap_err().code(), tonic::Code::NotFound);
    assert_eq!(replica.action("rtdb.owned.OpenRead.v3", Bytes::new()).await.unwrap_err().code(), tonic::Code::PermissionDenied);
    assert_eq!(owner.action(action, request(0)).await.unwrap_err().code(), tonic::Code::PermissionDenied);
    owner.update("INSERT INTO bj_relation_declarations RECORDS {_id: 'original', table_name: 'original', world: CAST(-1 AS BIGINT), declaration: X'01', names: ['location'], _valid_from: TIMESTAMP '1970-01-01Z'}", None).await;

    let reply = owner.action(action, request(7)).await.unwrap();
    let mut stream = StreamReader::try_new(reply[0].body.as_ref(), None).unwrap();
    let held_micros = i64::from_be_bytes(open[0].body[16..24].try_into().unwrap());
    assert_eq!(stream.schema().metadata()["rtdb.owned.present_micros"], held_micros.to_string());
    let batch = stream.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column_by_name("_id").unwrap().as_any().downcast_ref::<StringArray>().unwrap().value(0), "original");
    assert_eq!(batch.column_by_name("declaration").unwrap().as_any().downcast_ref::<arrow_array::BinaryArray>().unwrap().value(0), &[1]);
    assert!(stream.next().is_none());

    let next = owner.action("rtdb.owned.OpenRead.v3", Bytes::new()).await.unwrap();
    let reply = owner.action(action, Bytes::from([&next[0].body[..16], &[7]].concat())).await.unwrap();
    let mut empty = StreamReader::try_new(reply[0].body.as_ref(), None).unwrap();
    assert!(empty.schema().field_with_name("declaration").is_ok());
    assert!(empty.next().is_none());
    owner.action("rtdb.owned.CloseRead.v1", Bytes::copy_from_slice(hold)).await.unwrap();
    owner.action("rtdb.owned.CloseRead.v1", Bytes::copy_from_slice(&next[0].body[..16])).await.unwrap();

    let records = (0..129).map(|row| {
        format!("{{_id: 'row_{row}', table_name: 'table_{row}', world: CAST(0 AS BIGINT), declaration: X'01', names: ['location'], _valid_from: TIMESTAMP '1970-01-01Z'}}")
    }).collect::<Vec<_>>().join(", ");
    owner.update(&format!("INSERT INTO bj_relation_declarations RECORDS {records}"), None).await;
    let overflow = owner.action("rtdb.owned.OpenRead.v3", Bytes::new()).await.unwrap();
    let request = Bytes::from([&overflow[0].body[..16], &[7]].concat());
    assert_eq!(owner.action(action, request).await.unwrap_err().code(), tonic::Code::ResourceExhausted);
    owner.action("rtdb.owned.CloseRead.v1", Bytes::copy_from_slice(&overflow[0].body[..16])).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn hosted_flight_publishes_spatial_readiness_with_release_writes_and_a_count() {
    use arrow_array::{ArrayRef, BinaryArray, BooleanArray, TimestampMicrosecondArray, builder::{ListBuilder, StringBuilder}};
    use arrow_ipc::writer::IpcWriteOptions;
    use rtdb_format::ipc::write_arrow_stream;

    fn frame(body: &mut Vec<u8>, bytes: &[u8]) {
        body.extend_from_slice(&u16::try_from(bytes.len()).unwrap().to_be_bytes());
        body.extend_from_slice(bytes);
    }

    fn ipc(columns: impl IntoIterator<Item = (&'static str, ArrayRef)>) -> Vec<u8> {
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        write_arrow_stream(batch.schema_ref(), std::slice::from_ref(&batch), IpcWriteOptions::default()).unwrap()
    }

    fn groups(groups: &[(&str, &[u8])]) -> Vec<u8> {
        let mut body = u16::try_from(groups.len()).unwrap().to_be_bytes().to_vec();
        for (relation, bytes) in groups {
            frame(&mut body, relation.as_bytes());
            body.extend_from_slice(&u32::try_from(bytes.len()).unwrap().to_be_bytes());
            body.extend_from_slice(bytes);
        }
        body
    }

    let running = Running::start_with_placements(Some("secret"), FlightConfig {
        owned_native_actions: true,
        hosted_controls: Some(rtdb_flight_sql::HostedControlConfig {
            owner: "xtdb".into(),
            relations: ["bj_installation", "bj_fact_root_owner", "bj_fact_root_program", "bj_relation_declarations"].map(str::to_owned).into(),
        }),
        ..FlightConfig::default()
    }, &["application"]).await;
    let mut client = Client::connect_with_database(&running.server, Some("xtdb:secret"), Some("application")).await;
    client.update("INSERT INTO bj_fact_root_owner RECORDS {_id: 'root', revision: 1, _valid_from: TIMESTAMP '1970-01-01 00:00:00+00:00'}", None).await;
    client.update("INSERT INTO bj_fact_root_program RECORDS {_id: 'active', application_schema: 'old', _valid_from: TIMESTAMP '1970-01-01 00:00:00+00:00'}", None).await;
    client.update("INSERT INTO bj_relation_declarations RECORDS {_id: 'original', declaration: X'01', history_days: 7, _valid_from: TIMESTAMP '1970-01-01 00:00:00+00:00'}", None).await;
    let authority_claim = |writer| format!("INSERT INTO bj_installation RECORDS {{_id: 'authority', kind: 'hosted', incarnation: X'07070707070707070707070707070707', writer: '{writer}', _valid_from: TIMESTAMP '1970-01-01 00:00:00+00:00'}}");
    client.update(&authority_claim("11111111111111111111111111111111"), None).await;
    let clock = |batches: &[RecordBatch]| batches[0].column(0).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(0);
    let first_claim = clock(&client.query("SELECT _system_from FROM bj_installation WHERE _id = 'authority'").await);
    let expected = clock(&client.query("SELECT _system_from FROM bj_fact_root_owner WHERE _id = 'root'").await);
    let expected_program = clock(&client.query("SELECT _system_from FROM bj_fact_root_program WHERE _id = 'active'").await);

    let mut relation = Vec::new();
    frame(&mut relation, b"places");
    let mut schema = relation.clone();
    schema.extend_from_slice(br#"{"fields":[{"name":"location","type":{"kind":"binary"},"optional":false,"nullable":false}]}"#);
    client.action("rtdb.owned.DeclareRelation.v1", schema.into()).await.unwrap();
    let mut installation = running.node.session("application", Arc::new(FixtureOwner)).unwrap();
    let capacity = installation.state_mut().execute_tx(vec![rtdb_tx::TxOp::OwnedDeclareCapacity {
        relation: rtdb_types::OwnedRelation::new("places").unwrap(),
        column: "location".into(),
        prefix_bytes: 0,
        cap: 2,
        when: Vec::new(),
    }], Default::default()).await.unwrap();
    assert!(capacity.committed(), "{:?}", capacity.error());
    drop(installation);

    let mut geometry = vec![0, 1];
    geometry.extend(1u32.to_le_bytes());
    geometry.extend(0.0f64.to_le_bytes());
    geometry.extend(0.0f64.to_le_bytes());
    let mut columns = ListBuilder::new(StringBuilder::new());
    columns.values().append_value("location");
    columns.append(true);
    let columns: ArrayRef = Arc::new(columns.finish());
    let declaration = ipc([
        ("_id", Arc::new(StringArray::from(vec!["nearby"])) as ArrayRef),
        ("columns", Arc::clone(&columns)),
    ]);
    let requirement = ipc([
        ("_id", Arc::new(StringArray::from(vec!["nearby"])) as ArrayRef),
        ("_op", Arc::new(StringArray::from(vec!["require-spatial-path"]))),
        ("columns", columns),
        ("geometry_encoding", Arc::new(StringArray::from(vec!["tagged-wkb-v1"]))),
    ]);
    let hook = ipc([
        ("_id", Arc::new(StringArray::from(vec!["hook"])) as ArrayRef),
        ("_op", Arc::new(StringArray::from(vec!["create"]))),
        ("location", Arc::new(BinaryArray::from_vec(vec![geometry.as_slice()]))),
    ]);
    let owner = ipc([
        ("_id", Arc::new(StringArray::from(vec!["root"])) as ArrayRef),
        ("_expected_micros", Arc::new(Int64Array::from(vec![expected]))),
        ("revision", Arc::new(Int64Array::from(vec![2]))),
    ]);
    let program = ipc([
        ("_id", Arc::new(StringArray::from(vec!["active"])) as ArrayRef),
        ("_expected_micros", Arc::new(Int64Array::from(vec![expected_program]))),
        ("application_schema", Arc::new(StringArray::from(vec!["new"]))),
    ]);
    let mut absent = ListBuilder::new(StringBuilder::new());
    absent.values().append_value("history_days");
    absent.append(true);
    absent.append(true);
    let declarations = ipc([
        ("_id", Arc::new(StringArray::from(vec!["places", "later"])) as ArrayRef),
        ("_expected_micros", Arc::new(Int64Array::from(vec![None, None]))),
        ("_absent", Arc::new(absent.finish())),
        ("declaration", Arc::new(BinaryArray::from_iter_values([b"p".as_slice(), b"l".as_slice()]))),
        ("history_days", Arc::new(Int64Array::from(vec![None, Some(30)]))),
    ]);

    let facts = groups(&[("places", &hook), ("places", &requirement)]);
    let mut body = vec![1];
    body.extend_from_slice(&1u16.to_be_bytes());
    frame(&mut body, b"places");
    body.push(0);
    frame(&mut body, b"location");
    frame(&mut body, &geometry);
    body.extend_from_slice(&1i64.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&i64::MAX.to_be_bytes());
    body.extend_from_slice(&u32::try_from(facts.len()).unwrap().to_be_bytes());
    body.extend(facts);
    let request = |stamp: i64, grant: u8, include_authority: bool| {
        let authority = ipc([
            ("_id", Arc::new(StringArray::from(vec!["authority"])) as ArrayRef),
            ("_expected_micros", Arc::new(Int64Array::from(vec![stamp]))),
            ("_check_only", Arc::new(BooleanArray::from(vec![true]))),
        ]);
        let mut controls: Vec<(&str, &[u8])> = vec![
            ("bj_fact_root_owner", &owner),
            ("bj_fact_root_program", &program),
            ("bj_relation_declarations", &declarations),
        ];
        if include_authority { controls.push(("bj_installation", &authority)); }
        let mut request = stamp.to_be_bytes().to_vec();
        request.extend([7; 16]);
        request.extend([grant; 16]);
        request.extend(&body);
        request.extend(groups(&controls));
        Bytes::from(request)
    };
    let action = "rtdb.owned.DecideHosted.v4";
    let omitted = client.action(action, request(first_claim, 0x11, false)).await.unwrap_err();
    assert!(omitted.message().contains("requires the installed authority check"), "{omitted}");

    let refused = client.action(action, request(first_claim, 0x11, true)).await.unwrap_err();
    assert!(refused.message().contains("lacks coverage"), "{refused}");

    client.update(&authority_claim("22222222222222222222222222222222"), None).await;
    let second_claim = clock(&client.query("SELECT _system_from FROM bj_installation WHERE _id = 'authority'").await);
    assert!(second_claim > first_claim);
    let stale = client.action(action, request(first_claim, 0x11, true)).await.unwrap_err();
    assert!(stale.message().contains("authority changed before ordering"), "{stale}");
    let wrong_grant = client.action(action, request(second_claim, 0x11, true)).await.unwrap_err();
    assert!(wrong_grant.message().contains("authority changed before ordering"), "{wrong_grant}");
    assert_eq!(integer(&client.query("SELECT revision FROM bj_fact_root_owner").await[0], 0, 0), 1);
    assert_eq!(integer(&client.query("SELECT count(*) FROM bj_relation_declarations").await[0], 0, 0), 1);

    relation.extend(declaration);
    client.action("rtdb.owned.DeclareSpatialPath.v1", relation.into()).await.unwrap();
    client.action(action, request(second_claim, 0x22, true)).await.unwrap();
    assert_eq!(integer(&client.query("SELECT revision FROM bj_fact_root_owner").await[0], 0, 0), 2);
    let ordered = clock(&client.query("SELECT _system_from FROM bj_fact_root_owner").await);
    assert_eq!(clock(&client.query("SELECT _system_from FROM bj_fact_root_program WHERE application_schema = 'new'").await), ordered);
    assert_eq!(clock(&client.query("SELECT _system_from FROM bj_relation_declarations WHERE _id = 'places' AND history_days IS NULL").await), ordered);
    assert_eq!(clock(&client.query("SELECT _system_from FROM bj_relation_declarations WHERE _id = 'later' AND history_days = 30").await), ordered);
    running.stop().await;
}

#[tokio::test]
#[ignore = "serves an isolated database for the paired Bijection Flight tests"]
async fn serve_spatial_pair() {
    use rtdb_types::{OwnedRelation, OwnedSchema};
    use std::path::Path;

    async fn wait_for(directory: &Path, marker: &str) {
        tokio::time::timeout(Duration::from_secs(40), async {
            while !directory.join(marker).is_file() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.unwrap_or_else(|_| panic!("Bijection did not reach {marker}"));
    }

    let directory = std::path::PathBuf::from(std::env::var_os("RTDB_SPATIAL_PAIR_DIR").unwrap());
    let password = std::env::var("RTDB_SPATIAL_FLIGHT_PASSWORD").unwrap();
    let hosted_controls = match std::env::var("RTDB_SPATIAL_PAIR_MODE").unwrap().as_str() {
        "standalone" => None,
        "placed" => Some(rtdb_flight_sql::HostedControlConfig {
            owner: "xtdb".into(),
            relations: ["bj_installation", "bj_fact_root_owner", "bj_fact_root_program", "bj_relation_declarations"].map(str::to_owned).into(),
        }),
        mode => panic!("Unknown spatial fixture mode {mode}"),
    };
    let standalone = hosted_controls.is_none();
    let options = NodeOptions { file_size_target: 1, automatic_artefacts: false, ..NodeOptions::default() };
    let fence = (!standalone).then(|| Arc::new(FixtureSourceFence) as Arc<dyn rtdb_node::ReadFence>);
    let running = Running::start_with_fence(Some(&password), FlightConfig {
        owned_native_actions: true,
        hosted_controls,
        ..FlightConfig::default()
    }, &["application"], None, options, fence).await;
    if standalone {
        std::fs::write(directory.join("listen"), running.server.address().to_string()).unwrap();
        wait_for(&directory, "standalone-verified").await;
        running.stop().await;
        return;
    }
    let mut client = Client::connect_with_database(&running.server, Some(&format!("xtdb:{password}")), Some("application")).await;
    for statement in [
        "CREATE TABLE bj_fact_root_program (_id TEXT, generation BIGINT, artifact TEXT, closure_digest BYTEA, closure TEXT, application_schema TEXT, source_reference TEXT, environment_contract TEXT)",
        "INSERT INTO bj_installation RECORDS {_id: 'authority', _valid_from: TIMESTAMP '1970-01-01Z'}",
        "INSERT INTO bj_fact_root_owner RECORDS {_id: 'root', layout_digest: X'01', _valid_from: TIMESTAMP '1970-01-01Z'}",
        "INSERT INTO bj_fact_root_program RECORDS {_id: 'active', generation: 1, application_schema: 'original', _valid_from: TIMESTAMP '1970-01-01Z'}",
        "INSERT INTO bj_relation_declarations RECORDS {_id: 'paired', table_name: 'paired_places', world: CAST(0 AS BIGINT), declaration: X'01', names: ['location'], _valid_from: TIMESTAMP '1970-01-01Z'}",
    ] {
        client.update(statement, None).await;
    }
    let relation = OwnedRelation::new("paired_places").unwrap();
    let geometry = "c_01010101010101010101010101010101";
    let schema = OwnedSchema::from_json(format!(r#"{{"fields":[{{"name":"{geometry}","type":{{"kind":"binary"}},"optional":false,"nullable":false}}]}}"#).as_bytes()).unwrap();
    let mut installation = running.node.session("application", Arc::new(FixtureOwner)).unwrap();
    let installed = installation.state_mut().execute_tx(vec![
        rtdb_tx::TxOp::OwnedDeclareRelation { relation: relation.clone(), schema },
        rtdb_tx::TxOp::OwnedDeclareCapacity { relation: relation.clone(), column: geometry.into(), prefix_bytes: 0, cap: 2, when: Vec::new() },
    ], Default::default()).await.unwrap();
    assert!(installed.committed(), "{:?}", installed.error());
    drop(installation);

    std::fs::write(directory.join("listen"), running.server.address().to_string()).unwrap();
    wait_for(&directory, "original-written").await;
    let database = running.node.catalog().database("application").unwrap();
    database.flush().await.unwrap();
    running.node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    let table = relation.table("xtdb");
    let catalog = database.trie_catalog();
    assert!(catalog.current_tries(&table).iter().any(|trie| trie.key.level() > 0));
    std::fs::write(directory.join("original-durable"), []).unwrap();

    wait_for(&directory, "backfill-requested").await;
    database.build_artefacts().await.unwrap();
    assert!(database.trie_catalog().current_tries(&table).iter().any(|trie| {
        trie.artefacts.iter().any(|record| record.kind() == "spatial")
    }));
    std::fs::write(directory.join("backfill-published"), []).unwrap();

    wait_for(&directory, "release-committed").await;
    let program = client.query("SELECT generation FROM bj_fact_root_program WHERE application_schema = 'successor'").await;
    assert_eq!(integer(&program[0], 0, 0), 2);
    running.stop().await;
}

#[tokio::test]
async fn owned_arrow_writes_refuse_a_decimal_coefficient_outside_declared_precision() {
    use arrow_array::{ArrayRef, BinaryArray, Decimal128Array};
    use arrow_ipc::writer::IpcWriteOptions;
    use rtdb_format::ipc::write_arrow_stream;

    let running = Running::start(Some("secret"), FlightConfig { owned_native_actions: true, ..FlightConfig::default() }).await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    let relation = b"\0\x06places";
    let mut schema = relation.to_vec();
    schema.extend_from_slice(br#"{"fields":[{"name":"location","type":{"kind":"binary"},"optional":false,"nullable":false},{"name":"score","type":{"kind":"decimal","precision":2,"scale":0,"bitWidth":128},"optional":false,"nullable":false}]}"#);
    client.action("rtdb.owned.DeclareRelation.v1", schema.into()).await.unwrap();

    let request = |coefficient, commit| {
        let mut columns = vec![
            ("_id", Arc::new(StringArray::from(vec!["place"])) as ArrayRef),
            ("location", Arc::new(BinaryArray::from_iter_values([b"wkb"]))),
            ("score", Arc::new(Decimal128Array::from(vec![coefficient]).with_precision_and_scale(2, 0).unwrap())),
        ];
        let mut body = relation.to_vec();
        if commit {
            body.extend_from_slice(&i64::MAX.to_be_bytes());
            columns.push(("_op", Arc::new(StringArray::from(vec!["replace"]))));
        }
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        body.extend(write_arrow_stream(batch.schema_ref(), std::slice::from_ref(&batch), IpcWriteOptions::default()).unwrap());
        Bytes::from(body)
    };
    let refused = client.action("rtdb.owned.Create.v1", request(100, false)).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    assert_eq!(refused.message(), "Decimal coefficient exceeds its declared precision");

    client.action("rtdb.owned.Create.v1", request(99, false)).await.unwrap();

    let refused = client.action("rtdb.owned.Commit.v1", request(-100, true)).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    assert_eq!(refused.message(), "Decimal coefficient exceeds its declared precision");
    running.stop().await;
}

#[tokio::test]
async fn binary_owned_flight_retains_identity_order_and_the_scalar_batch_refusal() {
    use arrow_array::{Array, ArrayRef, BinaryArray, TimestampMicrosecondArray,
        builder::{ListBuilder, StringBuilder}};
    use arrow_ipc::writer::IpcWriteOptions;
    use rtdb_format::ipc::write_arrow_stream;

    fn request(prefix: &[u8], columns: Vec<(&str, ArrayRef)>) -> Bytes {
        nullable_request(prefix, columns.into_iter().map(|(name, column)| { let nullable = column.null_count() > 0; (name, column, nullable) }).collect())
    }
    fn nullable_request(prefix: &[u8], columns: Vec<(&str, ArrayRef, bool)>) -> Bytes {
        let batch = RecordBatch::try_from_iter_with_nullable(columns).unwrap();
        let mut body = prefix.to_vec();
        body.extend(write_arrow_stream(batch.schema_ref(), std::slice::from_ref(&batch), IpcWriteOptions::default()).unwrap());
        body.into()
    }
    fn entries(rows: &[(u8, &[u8])]) -> Bytes {
        let mut body = (rows.len() as u16).to_be_bytes().to_vec();
        for &(selector, input) in rows {
            body.push(selector);
            body.extend_from_slice(&(input.len() as u32).to_be_bytes());
            body.extend_from_slice(input);
        }
        body.into()
    }

    let running = Running::start(Some("secret"), FlightConfig { owned_native_actions: true, ..FlightConfig::default() }).await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    let relation = b" items";
    let low: &[u8] = &[0, 255];
    let high: &[u8] = &[0, 255, 0];
    let mut declaration = relation.to_vec();
    declaration.extend_from_slice(br#"{"fields":[{"name":"value","type":{"kind":"i64"},"optional":false,"nullable":false}]}"#);
    client.action("rtdb.owned.DeclareRelation.v1", declaration.into()).await.unwrap();
    let mut columns = ListBuilder::new(StringBuilder::new());
    columns.values().append_value("value");
    columns.values().append_value("_id");
    columns.append(true);
    client.action("rtdb.owned.DeclarePath.v1", request(relation, vec![
        ("_id", Arc::new(StringArray::from(vec!["by_value"]))),
        ("columns", Arc::new(columns.finish())),
    ])).await.unwrap();

    let commit = request(&[relation.as_slice(), &i64::MAX.to_be_bytes()].concat(), vec![
        ("_id", Arc::new(BinaryArray::from_iter_values([high, low]))),
        ("_op", Arc::new(StringArray::from(vec!["create", "create"]))),
        ("value", Arc::new(Int64Array::from(vec![9, 9]))),
        ("_valid_from", Arc::new(TimestampMicrosecondArray::from_value(0, 2).with_timezone("UTC"))),
    ]);
    client.action("rtdb.owned.Commit.v1", commit.clone()).await.unwrap();
    assert_eq!(client.action("rtdb.owned.Commit.v1", commit).await.unwrap_err().message(), "Insert overlaps an existing document");

    let opened = client.action("rtdb.owned.OpenRead.v3", Bytes::new()).await.unwrap().remove(0).body;
    let hold = opened.slice(..16);
    let system = i64::from_be_bytes(opened[16..24].try_into().unwrap());
    let prefix = [hold.as_ref(), relation.as_slice()].concat();
    let point = |id: ArrayRef| vec![("_id", id, false), ("valid_micros", Arc::new(Int64Array::from(vec![Some(system)])) as ArrayRef, true)];
    let scalar_point = "rtdb.owned.ReadHeldPoint.v2";
    let binary_point = nullable_request(&prefix, point(Arc::new(BinaryArray::from_iter_values([low]))));
    let reply = client.action(scalar_point, binary_point.clone()).await.unwrap().remove(0).body;
    let mut stream = StreamReader::try_new(reply.as_ref(), None).unwrap();
    let original_position = stream.schema().metadata()["rtdb.owned.event_position"].clone();
    let row = stream.next().unwrap().unwrap();
    let ids = row.column_by_name("_id").unwrap().as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(ids.value(0), low);

    let text_point = nullable_request(&prefix, point(Arc::new(StringArray::from(vec!["00ff"]))));
    let text = client.action(scalar_point, text_point).await.unwrap().remove(0).body;
    assert!(StreamReader::try_new(text.as_ref(), None).unwrap().next().is_none());
    let historical = nullable_request(&[hold.as_ref(), &system.to_be_bytes(), relation.as_slice()].concat(),
        point(Arc::new(BinaryArray::from_iter_values([low]))));
    let historical = client.action("rtdb.owned.ReadHeldHistoricalPoint.v1", historical).await.unwrap().remove(0).body;
    let history = StreamReader::try_new(historical.as_ref(), None).unwrap();
    assert_eq!(history.schema().metadata()["rtdb.owned.event_position"], original_position);

    let unknown = [0; 16];
    let failure = client.action(scalar_point, Bytes::copy_from_slice(&unknown)).await.unwrap_err();
    let input = client.request(Action {
        r#type: "rtdb.owned.ReadPoints.v1".into(),
        body: entries(&[(0, &binary_point), (0, &unknown), (0, &binary_point)]),
    });
    let mut results = client.client.do_action(input).await.unwrap().into_inner();
    assert_eq!(results.message().await.unwrap().unwrap().body, reply);
    let refused = results.message().await.unwrap_err();
    assert_eq!((refused.code(), refused.message()), (failure.code(), failure.message()));

    let compact = [prefix.as_slice(), &system.to_be_bytes(), low].concat();
    let input = client.request(Action {
        r#type: "rtdb.owned.ReadSelection.v1".into(),
        body: entries(&[(9, &compact), (9, &compact)]),
    });
    let mut results = client.client.do_action(input).await.unwrap().into_inner();
    assert_eq!(&results.message().await.unwrap().unwrap().body[..2], &[b'P', 1]);
    assert_eq!(&results.message().await.unwrap().unwrap().body[..2], &[b'P', 0]);
    assert!(results.message().await.unwrap().is_none());

    let scan = |after| request(&prefix, vec![
        ("system_micros", Arc::new(Int64Array::from(vec![system]))),
        ("valid_micros", Arc::new(Int64Array::from(vec![system]))),
        ("limit", Arc::new(Int64Array::from(vec![1]))),
        ("after", Arc::new(BinaryArray::from(vec![after]))),
    ]);
    for (after, key, more) in [(None, low, "1"), (Some(low), high, "0")] {
        let page = client.action("rtdb.owned.ReadHeldIdentityPage.v1", scan(after)).await.unwrap().remove(0).body;
        let mut rows = StreamReader::try_new(page.as_ref(), None).unwrap();
        assert_eq!(rows.schema().metadata()["rtdb.owned.identity_more"], more);
        let batch = rows.next().unwrap().unwrap();
        let ids = batch.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(ids.value(0), key);
    }
    let range = request(&prefix, vec![
        ("path", Arc::new(StringArray::from(vec!["by_value"]))),
        ("lower", Arc::new(Int64Array::from(vec![9]))),
        ("upper", Arc::new(Int64Array::from(vec![None]))),
        ("limit", Arc::new(Int64Array::from(vec![2]))),
    ]);
    let first = client.action("rtdb.owned.ReadHeldRange.v1", range).await.unwrap().remove(0).body;
    let mut rows = StreamReader::try_new(first.as_ref(), None).unwrap();
    let batch = rows.next().unwrap().unwrap();
    let ids = batch.column_by_name("_id").unwrap().as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(ids.iter().collect::<Vec<_>>(), [Some(low), Some(high)]);

    client.action("rtdb.owned.CloseRead.v1", hold).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn held_sealed_search_unit_rows_preserve_binary_order_and_original_completion() {
    use arrow_array::{ArrayRef, BinaryArray, BooleanArray, TimestampMicrosecondArray};
    use rtdb_types::SEALED_SEARCH_UNIT_ROWS as INVENTORY;
    use arrow_ipc::writer::IpcWriteOptions;
    use rtdb_format::ipc::write_arrow_stream;

    const ACTION: &str = "rtdb.owned.ReadHeldSealedSearchUnitRows.v2";
    const MORE: &str = "rtdb.owned.identity_more";
    const WITNESS: &str = "rtdb.owned.range_witness";

    fn ipc(columns: Vec<(&str, ArrayRef)>) -> Vec<u8> {
        nullable_ipc(columns.into_iter().map(|(name, column)| { let nullable = column.null_count() > 0; (name, column, nullable) }).collect())
    }
    fn nullable_ipc(columns: Vec<(&str, ArrayRef, bool)>) -> Vec<u8> {
        let batch = RecordBatch::try_from_iter_with_nullable(columns).unwrap();
        write_arrow_stream(batch.schema_ref(), &[batch.clone()], IpcWriteOptions::default()).unwrap()
    }
    fn relation_frame(relation: &str) -> Vec<u8> {
        [&(relation.len() as u16).to_be_bytes()[..], relation.as_bytes()].concat()
    }
    fn group(body: &mut Vec<u8>, relation: &str, ipc: &[u8]) {
        body.extend(relation_frame(relation));
        body.extend((ipc.len() as u32).to_be_bytes());
        body.extend(ipc);
    }

    let running = Running::start_with_fence(Some("secret"), FlightConfig {
        owned_native_actions: true,
        hosted_controls: Some(rtdb_flight_sql::HostedControlConfig {
            owner: "xtdb".into(),
            relations: ["bj_installation"].map(str::to_owned).into(),
        }),
        ..FlightConfig::default()
    }, &["application"], None, NodeOptions::default(), Some(Arc::new(FixtureSourceFence))).await;
    let mut client = Client::connect_with_database(&running.server, Some("xtdb:secret"), Some("application")).await;
    client.update("INSERT INTO bj_installation RECORDS {_id: 'authority', kind: 'hosted', incarnation: X'07070707070707070707070707070707', writer: '11111111111111111111111111111111', _valid_from: TIMESTAMP '1970-01-01Z'}", None).await;
    client.update("CREATE TABLE bj_rtdb_restore_hold (_id TEXT, binding BYTEA)", None).await;
    let claim = client.query("SELECT _system_from FROM bj_installation WHERE _id = 'authority'").await[0]
        .column(0).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(0);
    let prefix = vec![0xbc; 32];
    let derived = format!("fact_{}_search", "01".repeat(32));
    let key = |alias: &[u8], payload: &[u8]| {
        let address = [&[11][..], &[2; 16], payload, &[0, 1]].concat();
        [alias, &[derived.len() as u8], derived.as_bytes(), &address].concat()
    };
    let first = key(&prefix, &[3, 0, 255]);
    let last = key(&prefix, &[3, 1]);
    let other = key(&[0xbd; 32], &[3, 1]);
    let request = |hold: &[u8], relation, prefix: &[u8], after: ArrayRef, limit| {
        let mut body = [hold, &relation_frame(relation)].concat();
        body.extend(nullable_ipc(vec![
            ("prefix", Arc::new(BinaryArray::from_iter_values([prefix])), false),
            ("after", after, true),
            ("limit", Arc::new(Int64Array::from(vec![limit])), false),
        ]));
        Bytes::from(body)
    };
    let after = |value: Option<&[u8]>| Arc::new(BinaryArray::from(vec![value])) as ArrayRef;
    let undeclared = client.action("rtdb.owned.OpenRead.v3", Bytes::new()).await.unwrap().remove(0).body.slice(..16);
    assert!(client.action(ACTION, request(&undeclared, INVENTORY, &prefix, after(None), 1)).await.is_err());
    client.action("rtdb.owned.CloseRead.v1", undeclared).await.unwrap();

    let mut declaration = relation_frame(INVENTORY);
    declaration.extend(br#"{"fields":[]}"#);
    client.action("rtdb.owned.DeclareRelation.v1", declaration.into()).await.unwrap();
    let stored = [last.as_slice(), other.as_slice(), first.as_slice()];
    let mut facts = 1u16.to_be_bytes().to_vec();
    group(&mut facts, INVENTORY, &ipc(vec![
        ("_id", Arc::new(BinaryArray::from_iter_values(stored))),
        ("_op", Arc::new(StringArray::from(vec!["replace"; stored.len()]))),
        ("_valid_from", Arc::new(TimestampMicrosecondArray::from_value(i64::MIN, stored.len()).with_timezone("UTC"))),
    ]));
    let mut commit = claim.to_be_bytes().to_vec();
    commit.extend([7; 16]);
    commit.extend([0x11; 16]);
    commit.extend([0, 0, 0]);
    commit.extend(i64::MAX.to_be_bytes());
    commit.extend((facts.len() as u32).to_be_bytes());
    commit.extend(facts);
    commit.extend(1u16.to_be_bytes());
    group(&mut commit, "bj_installation", &ipc(vec![
        ("_id", Arc::new(StringArray::from(vec!["authority"]))),
        ("_expected_micros", Arc::new(Int64Array::from(vec![claim]))),
        ("_check_only", Arc::new(BooleanArray::from(vec![true]))),
    ]));
    client.action("rtdb.owned.DecideHosted.v4", commit.into()).await.unwrap();

    let hold = client.action("rtdb.owned.OpenRead.v3", Bytes::new()).await.unwrap().remove(0).body.slice(..16);
    for (bound, expected, more) in [(None, Some(first.as_slice()), true), (Some(first.as_slice()), Some(last.as_slice()), false),
        (Some(last.as_slice()), None, false)]
    {
        let reply = client.action(ACTION, request(&hold, INVENTORY, &prefix, after(bound), 1)).await.unwrap().remove(0).body;
        let stream = StreamReader::try_new(reply.as_ref(), None).unwrap();
        let schema = stream.schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Binary);
        assert_eq!(schema.metadata()[MORE], if more { "1" } else { "0" });
        assert_eq!(schema.metadata().contains_key(WITNESS), !more);
        if let Some(nonce) = schema.metadata().get(WITNESS) {
            assert_eq!(uuid::Uuid::parse_str(nonce).unwrap().simple().to_string(), *nonce);
        }
        let mut result = Vec::new();
        for batch in stream {
            let batch = batch.unwrap();
            let ids = batch.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
            result.extend(ids.iter().map(|id| id.unwrap().to_vec()));
        }
        assert_eq!(result.iter().map(Vec::as_slice).collect::<Vec<_>>(), expected.into_iter().collect::<Vec<_>>());
    }
    for refused in [
        request(&hold, INVENTORY, &prefix[..31], after(None), 1),
        request(&hold, INVENTORY, &prefix, after(Some(other.as_slice())), 1),
        request(&hold, INVENTORY, &prefix, after(Some(prefix.as_slice())), 1),
        request(&hold, INVENTORY, &prefix, Arc::new(StringArray::from(vec![None::<&str>])), 1),
        request(&hold, INVENTORY, &prefix, after(None), 65),
        request(&hold, rtdb_types::SHARED_COLUMN_CARRIERS, &prefix, after(None), 1),
    ] {
        assert!(client.action(ACTION, refused).await.is_err());
    }
    let mut binary = vec![0; rtdb_types::SharedColumnCarrierPrefix::WIDTH];
    binary[..5].copy_from_slice(b"BFCP2");
    let mut body = [hold.as_ref(), &relation_frame(INVENTORY)].concat();
    body.extend(ipc(vec![
        ("prefix", Arc::new(BinaryArray::from_iter_values([binary.as_slice()]))),
        ("after", Arc::new(BinaryArray::from(vec![None::<&[u8]>]))),
        ("limit", Arc::new(Int64Array::from(vec![1]))),
    ]));
    assert!(client.action("rtdb.owned.ReadHeldProtectedCarrierPage.v2", body.into()).await.is_err());

    let mut another = Client::connect_with_database(&running.server, Some("xtdb:secret"), Some("application")).await;
    assert!(another.action(ACTION, request(&hold, INVENTORY, &prefix, after(None), 1)).await.is_err());
    client.update("INSERT INTO bj_rtdb_restore_hold RECORDS {_id: 'archive-source-fence.v1', binding: X'01', _valid_from: TIMESTAMP '1970-01-01Z'}", None).await;
    assert!(client.action(ACTION, request(&hold, INVENTORY, &prefix, after(None), 1)).await.is_err());
    client.action("rtdb.owned.CloseRead.v1", hold).await.unwrap();
    let fenced = client.action("rtdb.owned.OpenRead.v3", Bytes::new()).await.unwrap().remove(0).body.slice(..16);
    assert!(client.action(ACTION, request(&fenced, INVENTORY, &prefix, after(None), 1)).await.is_err());
    client.action("rtdb.owned.CloseRead.v1", fenced).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn held_read_batch_reuses_scalar_results_authentication_and_failure_order() {
    use arrow_array::{ArrayRef, BinaryArray, BooleanArray, ListArray, NullArray, StructArray, UInt64Array,
        builder::{ListBuilder, StringBuilder}};
    use arrow_ipc::writer::IpcWriteOptions;
    use rtdb_format::ipc::write_arrow_stream;

    fn request(prefix: &[u8], columns: Vec<(&str, ArrayRef)>) -> Bytes {
        // A non-nullable Null column is the bottom type, so a null control is declared nullable.
        let columns = columns.into_iter().map(|(name, column)| {
            let nullable = column.null_count() > 0 || column.data_type() == &DataType::Null;
            (name, column, nullable)
        });
        let batch = RecordBatch::try_from_iter_with_nullable(columns).unwrap();
        let mut body = prefix.to_vec();
        body.extend(write_arrow_stream(batch.schema_ref(), &[batch.clone()], IpcWriteOptions::default()).unwrap());
        body.into()
    }

    fn batch(entries: &[(u8, &Bytes)]) -> Bytes {
        let mut frame = (entries.len() as u16).to_be_bytes().to_vec();
        for (selector, body) in entries {
            frame.push(*selector);
            frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
            frame.extend_from_slice(body);
        }
        frame.into()
    }

    let running = Running::start(Some("secret"), FlightConfig { owned_native_actions: true, ..FlightConfig::default() }).await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    let relation = b"\0\x05items";
    let mut schema = relation.to_vec();
    schema.extend_from_slice(br#"{"fields":[{"name":"value","type":{"kind":"i64"},"optional":false,"nullable":false}]}"#);
    client.action("rtdb.owned.DeclareRelation.v1", schema.into()).await.unwrap();
    let mut columns = ListBuilder::new(StringBuilder::new());
    columns.values().append_value("value");
    columns.values().append_value("_id");
    columns.append(true);
    let columns: ListArray = columns.finish();
    client.action("rtdb.owned.DeclarePath.v1", request(relation, vec![
        ("_id", Arc::new(StringArray::from(vec!["by_value"]))),
        ("columns", Arc::new(columns)),
    ])).await.unwrap();

    client.action("rtdb.owned.Create.v1", request(relation, vec![
        ("_id", Arc::new(Int64Array::from(vec![1]))),
        ("value", Arc::new(Int64Array::from(vec![9]))),
    ])).await.unwrap();

    let database = running.node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    running.node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.build_artefacts().await.unwrap();

    let opened = client.action("rtdb.owned.OpenRead.v3", Bytes::new()).await.unwrap().remove(0).body;
    let hold = opened.slice(..16);
    let point_prefix = [hold.as_ref(), relation].concat();
    let point = |id| request(&point_prefix, vec![
        ("_id", Arc::new(Int64Array::from(vec![id]))),
        ("valid_micros", Arc::new(Int64Array::from(vec![None]))),
    ]);
    let (present, missing) = (point(1), point(2));
    let scalar_point = "rtdb.owned.ReadHeldPoint.v2";
    let points = "rtdb.owned.ReadPoints.v1";
    let scalar_missing = client.action(scalar_point, missing.clone()).await.unwrap().remove(0).body;
    let scalar_present = client.action(scalar_point, present.clone()).await.unwrap().remove(0).body;
    assert_ne!(scalar_present, scalar_missing);

    let input = client.request(Action { r#type: points.into(), body: batch(&[(0, &missing), (0, &present)]) });
    let reply = client.client.do_action(input).await.unwrap();
    assert_eq!(reply.metadata().get("x-rtdb-owned-points").unwrap(), "v1");
    let mut stream = reply.into_inner();
    assert_eq!(stream.message().await.unwrap().unwrap().body, scalar_missing);
    assert_eq!(stream.message().await.unwrap().unwrap().body, scalar_present);
    assert!(stream.message().await.unwrap().is_none());

    let unknown = Bytes::from(vec![0; 16]);
    let scalar_failure = client.action(scalar_point, unknown.clone()).await.unwrap_err();
    let input = client.request(Action { r#type: points.into(), body: batch(&[(0, &present), (0, &unknown), (0, &missing)]) });
    let mut stream = client.client.do_action(input).await.unwrap().into_inner();
    assert_eq!(stream.message().await.unwrap().unwrap().body, scalar_present);
    let failure = stream.message().await.unwrap_err();
    assert_eq!((failure.code(), failure.message()), (scalar_failure.code(), scalar_failure.message()));

    let missing_key = Bytes::from([point_prefix.as_slice(), &opened[16..24], b"absent"].concat());
    let selection = "rtdb.owned.ReadSelection.v1";
    let input = client.request(Action {
        r#type: selection.into(),
        body: batch(&[(0, &missing_key), (0, &unknown), (0, &missing_key)]),
    });
    let reply = client.client.do_action(input).await.unwrap();
    assert_eq!(reply.metadata().get("x-rtdb-owned-selection").unwrap(), "v1");
    let mut stream = reply.into_inner();
    assert!(!stream.message().await.unwrap().unwrap().body.is_empty());
    let failure = stream.message().await.unwrap_err();
    assert_eq!((failure.code(), failure.message()), (scalar_failure.code(), scalar_failure.message()));

    let refused = client.action("rtdb.owned.ReadAudiencePoints.v1", batch(&[(0, &present), (0, &missing)])).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);

    let mut foreign = Client::connect(&running.server, Some("xtdb:secret")).await;
    let failure = foreign.action(points, batch(&[(0, &present), (0, &missing)])).await.unwrap_err();
    assert_eq!(failure.code(), tonic::Code::NotFound);
    drop(foreign);

    let range_prefix = [hold.as_ref(), relation].concat();
    let range = |lower| request(&range_prefix, vec![
        ("path", Arc::new(StringArray::from(vec!["by_value"]))),
        ("lower", Arc::new(Int64Array::from(vec![lower]))),
        ("upper", Arc::new(Int64Array::from(vec![None]))),
        ("limit", Arc::new(Int64Array::from(vec![2]))),
    ]);
    let scalar_range = "rtdb.owned.ReadHeldRange.v1";
    let scalar = client.action(scalar_range, range(8)).await.unwrap().remove(0).body;
    let input = client.request(Action { r#type: "rtdb.owned.ReadRanges.v1".into(), body: batch(&[(0, &range(8)), (0, &range(10))]) });
    let reply = client.client.do_action(input).await.unwrap();
    assert_eq!(reply.metadata().get("x-rtdb-owned-ranges").unwrap(), "v1");
    let replies: Vec<_> = reply.into_inner().try_collect().await.unwrap();
    assert_eq!(replies.len(), 2);
    assert_ne!(replies[0].body, replies[1].body);
    assert_eq!(replies[0].body, scalar);
    assert_eq!(replies[1].body, client.action(scalar_range, range(10)).await.unwrap()[0].body);

    let selector = |value: ArrayRef| Arc::new(StructArray::try_from(vec![
        ("value", value),
        ("_id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
    ]).unwrap()) as ArrayRef;
    let full_range = |after: ArrayRef| request(&range_prefix, vec![
        ("path", Arc::new(StringArray::from(vec!["by_value"]))),
        ("lower", selector(Arc::new(Int64Array::from(vec![9])))),
        ("upper", Arc::new(NullArray::new(1))),
        ("after", after),
        ("descending", Arc::new(BooleanArray::from(vec![false]))),
        ("limit", Arc::new(Int64Array::from(vec![2]))),
    ]);
    let full = "rtdb.owned.ReadHeldRange.v2";
    let first = client.action(full, full_range(Arc::new(NullArray::new(1)))).await.unwrap().remove(0).body;
    let page = StreamReader::try_new(first.as_ref(), None).unwrap().next().unwrap().unwrap();
    let ids = page.column_by_name("_id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ids.values().as_ref(), &[1]);
    let empty = client.action(full, full_range(selector(Arc::new(Int64Array::from(vec![9])))))
        .await.unwrap().remove(0).body;
    assert!(StreamReader::try_new(empty.as_ref(), None).unwrap().next().is_none());

    for (after, unary, frame_count) in [
        (Arc::new(NullArray::new(1)) as ArrayRef, first, 3),
        (selector(Arc::new(Int64Array::from(vec![9]))), empty, 2),
    ] {
        let mut input = client.request(Action { r#type: full.into(), body: full_range(after) });
        input.metadata_mut().insert("x-rtdb-owned-ipc", "stream-v1".parse().unwrap());
        let reply = client.client.do_action(input).await.unwrap();
        assert_eq!(reply.metadata().get("x-rtdb-owned-ipc").unwrap(), "stream-v1");
        let frames: Vec<_> = reply.into_inner().try_collect().await.unwrap();
        assert_eq!(frames.len(), frame_count);
        assert_eq!(frames.last().unwrap().body.as_ref(), &[255, 255, 255, 255, 0, 0, 0, 0]);
        let encoded = frames.iter().flat_map(|frame| frame.body.iter().copied()).collect::<Vec<_>>();
        let mut original = StreamReader::try_new(unary.as_ref(), None).unwrap();
        let mut streamed = StreamReader::try_new(encoded.as_slice(), None).unwrap();
        assert_eq!(original.schema().fields(), streamed.schema().fields());
        let coordinates = ["rtdb.owned.present_micros", "rtdb.owned.valid_micros"];
        for key in coordinates {
            assert_eq!(original.schema().metadata().get(key), streamed.schema().metadata().get(key));
        }
        match (original.next(), streamed.next()) {
            (Some(original), Some(streamed)) => assert_eq!(original.unwrap().columns(), streamed.unwrap().columns()),
            (None, None) => {},
            _ => panic!("Negotiated range changed the number of record batches"),
        }
        assert!(original.next().is_none() && streamed.next().is_none());
    }

    let valid = i64::from_be_bytes(opened[16..24].try_into().unwrap());
    let aggregate = |lower: ArrayRef| request(&range_prefix, vec![
        ("path", Arc::new(StringArray::from(vec!["by_value"]))),
        ("lower", lower),
        ("upper", Arc::new(NullArray::new(1))),
        ("valid_micros", Arc::new(Int64Array::from(vec![valid]))),
    ]);
    let lower = selector(Arc::new(Int64Array::from(vec![9])));
    let body = client.action("rtdb.owned.CountHeldRange.v1", aggregate(Arc::clone(&lower)))
        .await.unwrap().remove(0).body;
    let counted = StreamReader::try_new(body.as_ref(), None).unwrap().next().unwrap().unwrap();
    assert_eq!(counted.column(0).as_any().downcast_ref::<UInt64Array>().unwrap().value(0), 1);
    let body = client.action("rtdb.owned.MeasureHeldRange.v1", aggregate(lower))
        .await.unwrap().remove(0).body;
    let measured = StreamReader::try_new(body.as_ref(), None).unwrap().next().unwrap().unwrap();
    assert_eq!(measured.column(1).as_any().downcast_ref::<BinaryArray>().unwrap().value(0), &9_i128.to_be_bytes());
    let wrong_width = selector(Arc::new(UInt64Array::from(vec![9])));
    let refused = client.action("rtdb.owned.CountHeldRange.v1", aggregate(wrong_width)).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);

    let mut input = client.request(Action { r#type: scalar_point.into(), body: present });
    input.metadata_mut().insert("x-rtdb-owned-ipc", "stream-v1".parse().unwrap());
    let reply = client.client.do_action(input).await.unwrap();
    assert_eq!(reply.metadata().get("x-rtdb-owned-ipc").unwrap(), "stream-v1");
    let frames: Vec<_> = reply.into_inner().try_collect().await.unwrap();
    assert!(frames.len() > 2);
    assert_eq!(frames.iter().flat_map(|frame| frame.body.iter().copied()).collect::<Vec<_>>(), scalar_present.as_ref());

    client.action("rtdb.owned.CloseRead.v1", hold).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn owned_flight_fallback_preserves_fixed_vector_bits_on_create_and_replace() {
    use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float32Array};
    use arrow_ipc::{reader::StreamReader, writer::IpcWriteOptions};
    use rtdb_columnar::ArrowCell;
    use rtdb_format::ipc::write_arrow_stream;
    use rtdb_types::ListView;

    let running = Running::start(Some("secret"), FlightConfig { owned_native_actions: true, ..FlightConfig::default() }).await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    let encode = |batch: &RecordBatch| {
        write_arrow_stream(batch.schema_ref(), std::slice::from_ref(batch), IpcWriteOptions::default()).unwrap()
    };
    for (relation, action, bits) in [
        ("vectors", "rtdb.owned.Create.v1", 0x7fc0_4321),
        ("vectors", "rtdb.owned.Commit.v1", 0x7fc0_1234),
        (rtdb_types::OWNED_ROOT_PUBLICATION_RELATION, "rtdb.owned.Create.v1", 0x7fc0_1111),
        (rtdb_types::OWNED_ROOT_PUBLICATION_RELATION, "rtdb.owned.Commit.v1", 0x7fc0_2222),
    ] {
        let item = Arc::new(Field::new("$data$", DataType::Float32, true));
        let data = Float32Array::from(vec![Some(f32::from_bits(bits)), Some(-0.0), None]);
        let vector = Arc::new(FixedSizeListArray::new(item, 3, Arc::new(data), None)) as ArrayRef;
        let mut fields = vec![Field::new("_id", DataType::Int64, false),
            Field::new("v", vector.data_type().clone(), false).with_metadata([("description".into(), "embedding".into())].into())];
        let mut columns = vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef, vector];
        let relation = [(relation.len() as u16).to_be_bytes().as_slice(), relation.as_bytes()].concat();
        let mut request = relation.clone();
        if action.ends_with("Commit.v1") {
            request.extend_from_slice(&i64::MAX.to_be_bytes());
            fields.push(Field::new("_op", DataType::Utf8, false));
            columns.push(Arc::new(StringArray::from(vec!["replace"])));
        }
        request.extend(encode(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()));
        client.action(action, request.into()).await.unwrap();

        let mut read = relation;
        read.extend(encode(&RecordBatch::try_from_iter([("_id", Arc::new(Int64Array::from(vec![1])) as ArrayRef)]).unwrap()));
        let output = client.action("rtdb.owned.ReadPoint.v1", read.into()).await.unwrap();
        let batch = StreamReader::try_new(output[0].body.as_ref(), None).unwrap().next().unwrap().unwrap();
        let (view, span) = ArrowCell::named(&batch, "v", 0).unwrap().float_list_span().unwrap();
        let ListView::F32(values) = view else { panic!("expected Float32 components"); };
        assert_eq!(values.value(span.start).to_bits(), bits);
        assert_eq!(values.value(span.start + 1).to_bits(), (-0.0f32).to_bits());
        assert!(values.is_null(span.start + 2));
    }
    running.stop().await;
}

#[tokio::test]
async fn world_flight_actions_require_a_placed_world_host() {
    let running = Running::start(Some("secret"), FlightConfig { owned_native_actions: true, ..FlightConfig::default() }).await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    for action in [
        "rtdb.owned.PublishWorldCut.v1", "rtdb.owned.DecideWorldCut.v1",
        "rtdb.owned.DecideHeldWorlds.v3",
        "rtdb.owned.OpenWorldRead.v5", "rtdb.owned.ReadHeldWorldPoint.v2", "rtdb.owned.SearchHeldWorld.v4",
        "rtdb.owned.NearestHeldWorld.v3", "rtdb.owned.NearestHeldWorldAt.v3", "rtdb.owned.NearestHeldWorldParentAt.v3",
        "rtdb.owned.ReadHeldWorldValidDuring.v2",
        "rtdb.owned.ReadHeldWorldHead.v1",
        "rtdb.owned.SelectHeldWorldParentAsOf.v1",
        "rtdb.owned.SelectHeldWorldAsOf.v1",
        "rtdb.owned.ReadHeldWorldPointAt.v2",
        "rtdb.owned.ReadHeldWorldParentVersionsAt.v1",
        "rtdb.owned.ReadHeldWorldParentChangesAt.v2",
        "rtdb.owned.ReadHeldWorldAncestorChanges.v2",
        "rtdb.owned.CloseWorldRead.v1",
        "rtdb.owned.AssertionFamilyScope.v1", "rtdb.owned.EnrollAssertionFamily.v1",
        "rtdb.owned.EnrollFamilyWorldRow.v1", "rtdb.owned.RegisterAssertionFamily.v1", "rtdb.owned.ReserveFamilyLanding.v1",
        "rtdb.owned.ActivateFamilyLanding.v2", "rtdb.owned.SealFamilyLandingLink.v2",
        "rtdb.owned.SealFamilyLandingPayload.v1", "rtdb.owned.OpenFamilyLandingPayload.v1",
        "rtdb.owned.ResolveFamilyMember.v1", "rtdb.owned.RequireLiveFamilyOccurrence.v1", "rtdb.owned.RequireFamilyPositionOpen.v1",
        "rtdb.owned.RegisterFamilyDerived.v1", "rtdb.owned.SealWorldOverlayRow.v1",
        "rtdb.owned.OpenWorldOverlayRow.v1",
        "rtdb.owned.PlanAssertionFamilyFence.v1", "rtdb.owned.SettleAssertionFamily.v1",
    ] {
        let refused = client.action(action, Bytes::from_static(&[0; 16])).await.unwrap_err();
        assert_eq!(refused.code(), tonic::Code::Unimplemented, "{action}");
    }
    let refused = client.action("rtdb.owned.SearchHeldWorld.v4", Bytes::new()).await.unwrap_err();
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

#[tokio::test]
async fn deleted_catalog_actions_are_not_served() {
    for config in [FlightConfig::default(), FlightConfig { owned_native_actions: true, ..Default::default() }] {
        let running = Running::start(None, config).await;
        let mut client = Client::connect(&running.server, None).await;
        let request = client.request(arrow_flight::Empty {});
        let actions: Vec<_> = client.client.list_actions(request).await.unwrap().into_inner().try_collect().await.unwrap();
        for name in ["rtdb.owned.PublishCatalog.v1", "rtdb.owned.WatchRelation.v1", "rtdb.owned.ScanCatalog.v1"] {
            assert!(actions.iter().all(|action| action.r#type != name));
            assert_eq!(client.action(name, Bytes::new()).await.unwrap_err().code(), tonic::Code::Unimplemented);
        }
        running.stop().await;
    }
}

#[tokio::test]
async fn prepared_control_uses_the_first_uploaded_arrow_parameter_row() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let control = client.prepare("SET TIME ZONE CASE WHEN CARDINALITY(?) = 3 THEN 'Europe/Paris' ELSE 'UTC' END", None).await;
    let list = arrow_array::ListArray::from_iter_primitive::<arrow_array::types::Float64Type, _, _>([
        Some([Some(-0.0), None, Some(1.25)]),
        None,
    ]);
    let batch = RecordBatch::try_from_iter([("vector", Arc::new(list) as arrow_array::ArrayRef)]).unwrap();
    client.upload(CommandPreparedStatementUpdate { prepared_statement_handle: control.prepared_statement_handle }, vec![batch]).await;
    let rows = client.query("SHOW TIME ZONE").await;
    let timezone = rows[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(timezone.value(0), "Europe/Paris");
    running.stop().await;
}

#[path = "flight/v1_contract.rs"]
mod v1_contract;
