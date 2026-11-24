use super::*;
use arrow_array::ArrayRef;
use arrow_flight::{FlightData, Ticket};
use prost::Name;
use rtdb_flight_sql::rtdb::v1::{self as v1, at::Selection};
use rtdb_types::EntityId;

const CHANGE_IMAGE_HEADER: &str = "x-rtdb-changes-images";

#[cfg(feature = "multi")]
#[tokio::test]
async fn checkpoint_administration_requires_a_verified_unscoped_credential() {
    use rtdb_flight_sql::{
        DeploymentAdminAction, DeploymentAdminAuthority, rtdb::admin::v1::ExportCheckpointRequest,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Default)]
    struct Authority(AtomicUsize);

    impl DeploymentAdminAuthority for Authority {
        fn authorize(&self, principal: &rtdb_auth::Principal, action: DeploymentAdminAction<'_>) -> rtdb_types::Result<()> {
            assert_eq!(principal.user_id(), Some("xtdb"));
            assert_eq!(action, DeploymentAdminAction::Export { deployment: "xtdb" });
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    let authority = Arc::new(Authority::default());
    let config = FlightConfig { deployment_admin: Some(authority.clone()), ..Default::default() };
    let body: Bytes = ExportCheckpointRequest { deployment: "xtdb".into(), ..Default::default() }
        .encode_to_vec().into();
    let action = "rtdb.admin.v1.ExportCheckpoint";

    let running = Running::start(None, config.clone()).await;
    let mut trust = Client::connect(&running.server, Some("xtdb:unverified")).await;
    assert_eq!(trust.action(action, body.clone()).await.unwrap_err().code(), tonic::Code::PermissionDenied);
    assert_eq!(authority.0.load(Ordering::Relaxed), 0);
    running.stop().await;

    let running = Running::start(Some("secret"), config).await;
    let mut verified = Client::connect(&running.server, Some("xtdb:secret")).await;
    error(verified.action(action, body).await.unwrap_err(), v1::AnomalyCategory::Incorrect, "rtdb/invalid-request");
    assert_eq!(authority.0.load(Ordering::Relaxed), 1);
    running.stop().await;
}

#[cfg(feature = "multi")]
#[tokio::test]
async fn managed_workload_can_only_create_and_reopen_its_exact_deployment() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use rtdb_auth::{Method, MethodRule, OidcConfig, WorkloadGrantConfig};
    use rtdb_flight_sql::rtdb::admin::v1::{CreateDeploymentRequest, Deployment};
    use serde_json::json;
    use tokio::sync::Mutex;

    struct Issuer(Arc<MemoryBudget>);

    #[async_trait]
    impl HttpTransport for Issuer {
        async fn request(&self, request: HttpRequest) -> Result<HttpResponse> {
            let body = match request {
                HttpRequest::Get { bearer: None, .. } => json!({
                    "issuer": "https://issuer.example",
                    "token_endpoint": "https://issuer.example/token",
                    "userinfo_endpoint": "https://issuer.example/userinfo",
                }),
                HttpRequest::Form { fields, .. } => {
                    assert_eq!(fields["client_id"].expose(), "workload-create");
                    json!({"access_token": "scoped", "expires_in": 60})
                }
                HttpRequest::Get { bearer: Some(_), .. } => {
                    let grant_id = [17; 16];
                    json!({"sub": "provisioner", "rtdb_workload_grant": {
                        "version": 1, "audience": "cell", "database": "reports",
                        "incarnation": FIXTURE_INCARNATION, "grant_id": grant_id,
                        "expires_at_micros": 60_000_000, "operations": ["create-deployment"],
                    }})
                }
            };
            HttpResponse::new(200, body, &self.0)
        }
    }

    let memory = MemoryBudget::new(64 << 20);
    let clock = Arc::new(ManualClock(AtomicI64::new(1_000_000)));
    let auth = Authenticator::open(AuthConfig::OpenIdConnect(OidcConfig {
        issuer_url: "https://issuer.example".parse().unwrap(),
        issuer_ca_pem: None,
        client_id: "rtdb".into(),
        client_secret: Secret::new("configured"),
        rules: vec![MethodRule::any(Method::ClientCredentials)],
        workload_grants: Some(WorkloadGrantConfig::new("cell".into(), "workload-".into()).unwrap()),
    }), memory.clone(), clock.clone(), Arc::new(Issuer(memory.clone()))).await.unwrap();
    let uuid = uuid::Uuid::from_bytes(FIXTURE_INCARNATION);
    let kms: Arc<dyn rtdb_crypt::Kms> = Arc::new(rtdb_crypt::LocalKms::new(|| 0));
    let key = rtdb_crypt::tokens::TokenKey::create(kms.as_ref(), rtdb_crypt::Generation { database: uuid, number: 0 }).await.unwrap();
    let tokens = rtdb_crypt::tokens::TokenAuthority::new(uuid, kms, Arc::new(FixtureTokenKeys(key)));
    let provisioned = Arc::new(Mutex::new(Vec::new()));
    let resources = Arc::new(AppliedFixtureResources {
        native: Default::default(),
        driver: Arc::new(AppliedFixture {
            state: std::sync::Mutex::new(rtdb_tx::transact::TransactState::new(FIXTURE_INCARNATION)),
            tokens,
            memory,
            admission: Default::default(),
            hold_first: Default::default(),
            waiting: Default::default(),
            release: Default::default(),
        }),
        provisioned: Some(provisioned.clone()),
    });
    let options = NodeOptions {
        membership: Membership::Managed(Default::default()),
        automatic_compaction: false,
        automatic_gc: false,
        ..Default::default()
    };
    let services = NodeServices {
        clock: clock.clone(),
        resources,
        ..NodeServices::local(auth.clone(), Arc::new(Observer), auth.clone(), Arc::new(Metrics))
    };
    let (node, driver) = Node::open(options, services).await.unwrap();
    let node_task = tokio::spawn(driver.run());
    let config = FlightConfig { address: "127.0.0.1:0".parse().unwrap(), ..Default::default() };
    let (server, driver) = ServerDriver::bind_with_clock(config, node.clone(), auth, clock.clone()).await.unwrap();
    let server_task = tokio::spawn(driver.run());
    let running = Running { node, node_task, server, server_task, clock };
    let channel = Channel::from_shared(format!("http://{}", running.server.address())).unwrap().connect().await.unwrap();
    let mut client = Client { client: FlightServiceClient::new(channel), headers: MetadataMap::new() };
    client.headers.insert("authorization", format!("Client {}", STANDARD.encode("workload-create:secret")).parse().unwrap());
    let create = "rtdb.admin.v1.CreateDeployment";
    let request = |name: &str, incarnation| Bytes::from(CreateDeploymentRequest {
        deployment: name.into(),
        incarnation,
    }.encode_to_vec());
    for (action, body) in [
        (create, request("reports", None)),
        (create, request("other", Some(FIXTURE_INCARNATION.to_vec()))),
        (create, request("reports", Some(vec![7; 16]))),
        ("rtdb.admin.v1.DeleteDeployment", Bytes::new()),
        ("rtdb.v1.Transact", Bytes::new()),
        ("rtdb.v1.ManagedWorkload", Bytes::new()),
        ("rtdb.v1.TxStatus", Bytes::new()),
        ("BeginTransaction", Bytes::new()),
    ] {
        assert!(client.action(action, body).await.is_err(), "{action}");
    }
    assert!(provisioned.lock().await.is_empty());

    let body = request("reports", Some(FIXTURE_INCARNATION.to_vec()));
    let first = client.action(create, body.clone()).await.unwrap();
    let handshake = client.request(futures::stream::iter([arrow_flight::HandshakeRequest::default()]));
    let response = client.client.handshake(handshake).await.unwrap();
    client.headers.insert("authorization", response.metadata().get("authorization").unwrap().clone());
    let _: Vec<_> = response.into_inner().try_collect().await.unwrap();
    assert!(client.action("rtdb.v1.Capabilities", Bytes::new()).await.is_err());
    let replay = client.action(create, body.clone()).await.unwrap();
    let deployment = Deployment::decode(first[0].body.clone()).unwrap();
    assert_eq!(deployment.deployment, "reports");
    assert_eq!(deployment.incarnation, FIXTURE_INCARNATION);
    assert_eq!(replay[0].body, first[0].body);
    assert_eq!(*provisioned.lock().await, [(String::from("reports"), uuid), (String::from("reports"), uuid)]);

    running.clock.0.store(60_000_000, Ordering::Relaxed);
    assert!(client.action(create, body).await.is_err());
    assert_eq!(provisioned.lock().await.len(), 2);
    running.stop().await;
}

fn input(sql: &str) -> v1::QueryRequest {
    v1::QueryRequest {
        source: Some(v1::Source {
            branch: "main".into(),
            at: None,
            layer: v1::Layer::Merged as i32,
        }),
        sql: sql.into(),
        ..Default::default()
    }
}

async fn get<M: Message + Name, R: Message + Default>(
    client: &mut Client,
    message: M,
) -> std::result::Result<(Vec<RecordBatch>, R), Status> {
    let (batches, trailer, _) = get_response(client, message).await?;
    Ok((batches, trailer))
}

async fn get_response<M: Message + Name, R: Message + Default>(
    client: &mut Client,
    message: M,
) -> std::result::Result<(Vec<RecordBatch>, R, MetadataMap), Status> {
    let command = Any {
        type_url: M::type_url(),
        value: message.encode_to_vec().into(),
    };
    let request = client.request(Ticket {
        ticket: command.encode_to_vec().into(),
    });
    let response = client.client.do_get(request).await?;
    let metadata = response.metadata().clone();
    let mut frames: Vec<FlightData> = response.into_inner().try_collect().await?;
    let last = frames.pop().expect("typed trailer");
    assert!(last.data_header.is_empty() && last.data_body.is_empty());
    let trailer = R::decode(last.app_metadata).expect("protobuf stream trailer");
    let schema =
        arrow_ipc::root_as_message(&frames.first().expect("Arrow schema").data_header).unwrap();
    assert!(schema.header_as_schema().is_some());
    let stream = futures::stream::iter(frames.into_iter().map(Ok));
    let batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(stream)
        .try_collect()
        .await
        .unwrap();
    Ok((batches, trailer, metadata))
}

fn error(status: Status, expected: v1::AnomalyCategory, code: &str) -> v1::Error {
    let metadata = status
        .metadata()
        .get_bin("rtdb-error-bin")
        .expect("typed error");
    let detail = v1::Error::decode(metadata.to_bytes().unwrap()).unwrap();
    assert_eq!(detail.category, expected as i32);
    assert_eq!(detail.code, code);
    detail
}

async fn query(
    client: &mut Client,
    query: v1::QueryRequest,
) -> (Vec<RecordBatch>, v1::QueryTrailer) {
    get(client, query).await.unwrap()
}

fn ints(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(move |row| integer(batch, 0, row)))
        .collect()
}

#[tokio::test]
async fn query_parameters_require_one_row_and_continuations_bind_the_exact_ipc() {
    fn batch(name: &str, values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_from_iter([(name, Arc::new(Int64Array::from(values)) as ArrayRef)]).unwrap()
    }
    fn stream(batch: &RecordBatch, count: usize) -> Vec<u8> {
        rtdb_format::ipc::write_stream(batch.schema_ref(), &vec![batch.clone(); count]).unwrap()
    }

    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    client.update("INSERT INTO wire_params (_id) VALUES (1), (2), (3)", None).await;

    let mut request = input("SELECT _id FROM wire_params WHERE _id >= $1 ORDER BY _id");
    request.page = Some(v1::Page { limit: 1, after: vec![] });
    let one = batch("p", vec![2]);
    request.params_ipc = Some(stream(&one, 1));
    let (rows, first) = query(&mut client, request.clone()).await;
    assert_eq!(ints(&rows), [2]);
    let mut continuation = request.clone();
    continuation.page.as_mut().unwrap().after = first.next.unwrap();
    assert_eq!(ints(&query(&mut client, continuation.clone()).await.0), [3]);

    continuation.params_ipc = Some(stream(&batch("renamed", vec![2]), 1));
    let status = get::<_, v1::QueryTrailer>(&mut client, continuation).await.unwrap_err();
    let detail = error(status, v1::AnomalyCategory::Incorrect, "rtdb/invalid-request");
    assert_eq!(detail.message, "Continuation belongs to a different query");

    for (bytes, message) in [
        (stream(&one, 0), "Parameters require exactly one Arrow batch"),
        (stream(&one, 2), "Parameters require exactly one Arrow batch"),
        (stream(&batch("p", vec![]), 1), "Parameters require exactly one row"),
        (stream(&batch("p", vec![2, 3]), 1), "Parameters require exactly one row"),
    ] {
        request.params_ipc = Some(bytes);
        let status = get::<_, v1::QueryTrailer>(&mut client, request.clone()).await.unwrap_err();
        let detail = error(status, v1::AnomalyCategory::Incorrect, "rtdb/invalid-request");
        assert_eq!(detail.message, message);
    }
    running.stop().await;
}

#[tokio::test]
async fn query_parameter_binding_preserves_result_types_and_errors() {
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
        Float64Array, Int32Array, ListArray, NullArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array, cast::AsArray, types::Float32Type,
    };
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use rtdb_types::Value;

    fn parameter(values: ArrayRef) -> Vec<u8> {
        let nullable = values.is_nullable();
        let batch =
            RecordBatch::try_from_iter_with_nullable([("argument", values, nullable)]).unwrap();
        rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch)).unwrap()
    }

    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let single_nan = f32::from_bits(0x7fc0_0042);
    let double_nan = f64::from_bits(0x7ff8_0000_0000_0042);
    for (values, data_type, expected) in [
        (
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            DataType::Boolean,
            Value::Bool(true),
        ),
        (
            Arc::new(Int32Array::from(vec![i32::MIN])) as ArrayRef,
            DataType::Int32,
            Value::I32(i32::MIN),
        ),
        (
            Arc::new(Float32Array::from(vec![single_nan])) as ArrayRef,
            DataType::Float32,
            Value::F32(single_nan),
        ),
        (
            Arc::new(Float32Array::from(vec![-0.0])) as ArrayRef,
            DataType::Float32,
            Value::F32(-0.0),
        ),
        (
            Arc::new(Float64Array::from(vec![double_nan])) as ArrayRef,
            DataType::Float64,
            Value::F64(double_nan),
        ),
        (
            Arc::new(Float64Array::from(vec![-0.0])) as ArrayRef,
            DataType::Float64,
            Value::F64(-0.0),
        ),
        (
            Arc::new(Float32Array::from(vec![None])) as ArrayRef,
            DataType::Float32,
            Value::Null,
        ),
        (
            Arc::new(Int64Array::from(vec![None])) as ArrayRef,
            DataType::Int64,
            Value::Null,
        ),
        (
            Arc::new(NullArray::new(1)) as ArrayRef,
            DataType::Null,
            Value::Null,
        ),
        (
            Arc::new(StringArray::from(vec!["café"])) as ArrayRef,
            DataType::Utf8,
            Value::Utf8("café".into()),
        ),
        (
            Arc::new(BinaryArray::from(vec![&b"\x00\xff"[..]])) as ArrayRef,
            DataType::Binary,
            Value::Binary(vec![0, 255]),
        ),
        (
            Arc::new(Date32Array::from(vec![-42])) as ArrayRef,
            DataType::Date32,
            Value::DateDay(-42),
        ),
        (
            Arc::new(
                Decimal128Array::from(vec![-123])
                    .with_precision_and_scale(8, 2)
                    .unwrap(),
            ) as ArrayRef,
            DataType::Decimal128(8, 2),
            Value::Decimal(
                rtdb_types::DecimalValue::new((-123_i128).to_be_bytes().to_vec(), 8, 2, 128)
                    .unwrap(),
            ),
        ),
        (
            Arc::new(UInt8Array::from(vec![u8::MAX])) as ArrayRef,
            DataType::Int8,
            Value::I8(-1),
        ),
        (
            Arc::new(UInt16Array::from(vec![u16::MAX])) as ArrayRef,
            DataType::Int16,
            Value::I16(-1),
        ),
        (
            Arc::new(UInt32Array::from(vec![u32::MAX])) as ArrayRef,
            DataType::Int32,
            Value::I32(-1),
        ),
        (
            Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef,
            DataType::Int64,
            Value::I64(-1),
        ),
        (
            Arc::new(UInt64Array::from(vec![None])) as ArrayRef,
            DataType::Int64,
            Value::Null,
        ),
    ] {
        let mut request = input("SELECT $1 AS n");
        request.params_ipc = Some(parameter(values));
        let (batches, _) = query(&mut client, request).await;
        let batch = &batches[0];
        let field = batch.schema_ref().field(0);
        assert_eq!(field.data_type(), &data_type);
        let actual = rtdb_columnar::read_value(field, batch.column(0).as_ref(), 0).unwrap();
        match (actual, expected) {
            (Value::F32(actual), Value::F32(expected)) => {
                assert_eq!(actual.to_bits(), expected.to_bits())
            }
            (Value::F64(actual), Value::F64(expected)) => {
                assert_eq!(actual.to_bits(), expected.to_bits())
            }
            (actual, expected) => assert_eq!(actual, expected),
        }
    }

    let item = Arc::new(Field::new("$data$", DataType::Float32, false));
    let list = ListArray::new(
        Arc::clone(&item),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
        Arc::new(Float32Array::from(vec![-0.0, single_nan])),
        None,
    );
    let mut request = input("SELECT $1 AS n");
    request.params_ipc = Some(parameter(Arc::new(list)));
    let (batches, _) = query(&mut client, request).await;
    let batch = &batches[0];
    assert_eq!(
        batch.schema_ref().field(0).data_type(),
        &DataType::List(item)
    );
    let values = batch.column(0).as_list::<i32>().value(0);
    let values = values.as_primitive::<Float32Type>();
    assert_eq!(values.value(0).to_bits(), (-0.0_f32).to_bits());
    assert_eq!(values.value(1).to_bits(), single_nan.to_bits());

    let mut request = input("SELECT 10::BIGINT / $1 AS n");
    request.params_ipc = Some(parameter(Arc::new(UInt64Array::from(vec![u64::MAX]))));
    assert_eq!(ints(&query(&mut client, request).await.0), [-10]);

    for values in [
        Arc::new(Int64Array::from(vec![0])) as ArrayRef,
        Arc::new(Float64Array::from(vec![-0.0])) as ArrayRef,
    ] {
        let mut request = input("SELECT 10::BIGINT / $1 AS n");
        request.params_ipc = Some(parameter(values));
        let status = get::<_, v1::QueryTrailer>(&mut client, request)
            .await
            .unwrap_err();
        let detail = error(
            status,
            v1::AnomalyCategory::Incorrect,
            "rtdb/invalid-request",
        );
        assert_eq!(detail.message, "data exception - division by zero");
    }

    let uri = rtdb_columnar::to_field(
        "address",
        &rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Uri),
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("divisor", DataType::Int64, false),
        uri,
    ]));
    let invalid_uri = "1:bad-scheme";
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(StringArray::from(vec![invalid_uri])),
        ],
    )
    .unwrap();
    let mut request = input("SELECT 10::BIGINT / $1 AS n, $2 AS address");
    request.params_ipc = Some(
        rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch)).unwrap(),
    );
    let status = get::<_, v1::QueryTrailer>(&mut client, request)
        .await
        .unwrap_err();
    let detail = error(
        status,
        v1::AnomalyCategory::Incorrect,
        "rtdb/invalid-request",
    );
    assert_eq!(detail.message, "Illegal scheme name");
    running.stop().await;
}

#[tokio::test]
async fn rtdb_v1_capabilities_paging_and_stable_errors() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    let reply = client
        .action("rtdb.v1.Capabilities", Bytes::new())
        .await
        .unwrap();
    let caps = v1::CapabilitiesResponse::decode(reply[0].body.clone()).unwrap();
    assert_eq!(caps.api, "rtdb.v1");
    assert!(caps.features.iter().any(|feature| feature == "query.track"));
    assert!(caps.features.iter().any(|feature| feature == "changes.progress-only"));
    assert!(caps.features.iter().any(|feature| feature == "changes.images.stream-v1"));
    assert_eq!(caps.limits.unwrap().min_key_bytes, 16);
    client
        .update(
            "INSERT INTO wire_rows (_id, n) VALUES (1, 10), (2, 20), (3, 30)",
            None,
        )
        .await;
    let mut request = input("SELECT n FROM wire_rows ORDER BY _id");
    request.track = true;
    request.page = Some(v1::Page {
        limit: 2,
        after: vec![],
    });
    request.source.as_mut().unwrap().at = Some(v1::At {
        selection: Some(Selection::Linearizable(())),
    });
    let (rows, first) = query(&mut client, request.clone()).await;
    assert_eq!(ints(&rows), [10, 20]);
    assert!(first.claim.is_some());
    client
        .update("UPDATE wire_rows SET n = 300 WHERE _id = 3", None)
        .await;
    request.page.as_mut().unwrap().after = first.next.unwrap();
    let mut other_session = Client::connect(&running.server, None).await;
    let (rows, second) = query(&mut other_session, request.clone()).await;
    assert_eq!(ints(&rows), [30]);
    assert_eq!(first.basis, second.basis);
    assert_eq!(first.claim, second.claim);
    assert!(second.next.is_none());

    let mut exact = input("SELECT n FROM wire_rows WHERE _id = 3");
    exact.source.as_mut().unwrap().at = Some(v1::At {
        selection: Some(Selection::Exactly(first.basis.clone().unwrap())),
    });
    assert_eq!(ints(&query(&mut client, exact.clone()).await.0), [30]);
    exact.source.as_mut().unwrap().at = Some(v1::At {
        selection: Some(Selection::AtLeast(first.basis.unwrap())),
    });
    assert_eq!(ints(&query(&mut client, exact.clone()).await.0), [300]);
    exact.source.as_mut().unwrap().at = Some(v1::At {
        selection: Some(Selection::AsOfMicros(first.system_time_micros)),
    });
    assert_eq!(ints(&query(&mut client, exact).await.0), [30]);

    request.page.as_mut().unwrap().after[8] ^= 1;
    let status = get::<_, v1::QueryTrailer>(&mut client, request)
        .await
        .unwrap_err();
    error(status, v1::AnomalyCategory::NotFound, "rtdb/cursor-expired");

    let mut branch = input("SELECT 1");
    branch.source.as_mut().unwrap().branch = "feature".into();
    let status = get::<_, v1::QueryTrailer>(&mut client, branch)
        .await
        .unwrap_err();
    assert_eq!(
        error(status, v1::AnomalyCategory::Unsupported, "rtdb/unsupported")
            .feature
            .as_deref(),
        Some("branches")
    );
    let mut layer = input("SELECT 1");
    layer.source.as_mut().unwrap().layer = v1::Layer::Local as i32;
    let status = get::<_, v1::QueryTrailer>(&mut client, layer)
        .await
        .unwrap_err();
    assert_eq!(
        error(status, v1::AnomalyCategory::Unsupported, "rtdb/unsupported")
            .feature
            .as_deref(),
        Some("branch.local-layer")
    );
    let status = get::<_, v1::QueryTrailer>(&mut client, input("DELETE FROM wire_rows"))
        .await
        .unwrap_err();
    error(
        status,
        v1::AnomalyCategory::Incorrect,
        "rtdb/invalid-request",
    );
    let status = client
        .action("rtdb.v1.TxStatus", Bytes::new())
        .await
        .unwrap_err();
    error(
        status,
        v1::AnomalyCategory::Incorrect,
        "rtdb/invalid-request",
    );
    running.stop().await;
}

#[tokio::test]
async fn rtdb_v1_tokens_survive_database_and_flight_restart() {
    use rtdb_format::database::proto::{DatabaseConfig, LocalLog, LocalStorage, database_config};
    use rtdb_types::{EntityId, TableRef, rtdb_v1::{Claim, ClaimRegion}};

    let root = tempfile::tempdir().unwrap();
    let options = || NodeOptions {
        membership: Membership::Durable {
            primary: DatabaseConfig {
                log: Some(database_config::Log::LocalLog(LocalLog {
                    path: Some(root.path().join("log").to_string_lossy().into()), ..Default::default()
                })),
                storage: Some(database_config::Storage::LocalStorage(LocalStorage {
                    path: Some(root.path().join("objects").to_string_lossy().into()), ..Default::default()
                })),
                ..Default::default()
            },
            skipped: Default::default(),
        },
        ..Default::default()
    };
    let mut request = input("SELECT n FROM wire_rows WHERE _id = 1 OR _id = 2 ORDER BY _id");
    // The Flight SQL update acknowledgement does not select an applied v1 cut.
    request.source.as_mut().unwrap().at = Some(v1::At { selection: Some(Selection::Linearizable(())) });
    request.track = true;
    request.page = Some(v1::Page { limit: 1, after: Vec::new() });

    let first = {
        let running = Running::start_with_options(None, FlightConfig::default(), &[], None, options()).await;
        let mut client = Client::connect(&running.server, None).await;
        client.update("INSERT INTO wire_rows (_id, n) VALUES (1, 10), (2, 20), (3, 30)", None).await;
        let (rows, first) = query(&mut client, request.clone()).await;
        assert_eq!(ints(&rows), [10]);
        let database = running.node.catalog().database("xtdb").unwrap();
        let authority = database.token_authority().await.unwrap();
        let claim = Claim::from_bytes(first.claim.clone().unwrap().value);
        let opened = authority.open_claim(&claim).await.unwrap();
        let table = TableRef::new("public", "wire_rows");
        assert_eq!(opened.footprint.regions().unwrap(), &[
            ClaimRegion::Row { table: table.clone(), iid: EntityId::Integer(1).iid() },
            ClaimRegion::Row { table, iid: EntityId::Integer(2).iid() },
        ].into_iter().collect());
        client.update("UPDATE wire_rows SET n = 200 WHERE _id = 2", None).await;
        let latest = v1::QueryRequest { page: None, track: false, ..request.clone() };
        assert_eq!(ints(&query(&mut client, latest).await.0), [10, 200]);
        database.apply_token_key_rotation(1, i64::MAX).await.unwrap();
        running.stop().await;
        first
    };

    request.page.as_mut().unwrap().after = first.next.clone().unwrap();
    let rerouted = Running::start_with_options(None, FlightConfig::default(), &[], None, options()).await;
    let mut client = Client::connect(&rerouted.server, None).await;
    let (rows, second) = query(&mut client, request.clone()).await;
    assert_eq!(ints(&rows), [20]);
    assert_eq!(first.basis, second.basis);
    assert!(second.next.is_none());
    let database = rerouted.node.catalog().database("xtdb").unwrap();
    let authority = database.token_authority().await.unwrap();
    let before = Claim::from_bytes(first.claim.unwrap().value);
    let after = Claim::from_bytes(second.claim.unwrap().value);
    assert_eq!(before, after);
    assert_eq!(authority.open_claim(&before).await.unwrap().scope.branch, "main");
    rerouted.stop().await;

    let foreign = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&foreign.server, None).await;
    let status = get::<_, v1::QueryTrailer>(&mut client, request).await.unwrap_err();
    error(status, v1::AnomalyCategory::NotFound, "rtdb/cursor-expired");
    foreign.stop().await;
}

#[tokio::test]
async fn rtdb_v1_sql_parameters_search_nearest_and_history() {
    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    client.update("INSERT INTO wire_docs (_id, body, embedding) VALUES (1, 'red invoice', ARRAY[1.0, 0.0]), (2, 'blue receipts', ARRAY[0.0, 1.0])", None).await;
    client
        .update(
            "UPDATE wire_docs SET body = 'paid invoice' WHERE _id = 1",
            None,
        )
        .await;
    let mut search = input(
        "SELECT _id FROM wire_docs WHERE MATCH(body, $1) ORDER BY SEARCH_SCORE(body, $1) DESC, _id",
    );
    let parameters = RecordBatch::try_from_iter([(
        "query",
        Arc::new(StringArray::from(vec!["invoice"])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    search.params_ipc = Some(
        rtdb_format::ipc::write_stream(parameters.schema().as_ref(), &[parameters.clone()])
            .unwrap(),
    );
    assert_eq!(ints(&query(&mut client, search.clone()).await.0), [1]);
    search.sql = "SELECT _id FROM RTDB_SEARCH(wire_docs, body, $1)".into();
    assert_eq!(ints(&query(&mut client, search).await.0), [1]);
    let nearest = input(
        "SELECT _id FROM wire_docs ORDER BY COSINE_DISTANCE(embedding, ARRAY[0.0, 1.0]), _id LIMIT 1",
    );
    assert_eq!(ints(&query(&mut client, nearest).await.0), [2]);
    let nearest = input("SELECT _id FROM RTDB_NEAREST(wire_docs, embedding, ARRAY[0.0, 1.0], 1)");
    assert_eq!(ints(&query(&mut client, nearest).await.0), [2]);
    let stemmed = input("SELECT _id FROM wire_docs WHERE MATCH(body, 'receipt', 'english') ORDER BY _id");
    assert_eq!(ints(&query(&mut client, stemmed).await.0), [2]);
    let unstemmed = input("SELECT _id FROM wire_docs WHERE MATCH(body, 'receipt') ORDER BY _id");
    assert!(ints(&query(&mut client, unstemmed).await.0).is_empty());
    let stemmed = input("SELECT _id FROM RTDB_SEARCH(wire_docs, body, 'receipt', 'english')");
    assert_eq!(ints(&query(&mut client, stemmed).await.0), [2]);
    let hybrid = input("SELECT _id FROM RTDB_HYBRID(wire_docs, body, 'invoice', embedding, ARRAY[0.0, 1.0], 2, 2)");
    assert_eq!(ints(&query(&mut client, hybrid).await.0), [1, 2]);
    let history =
        input("SELECT _id FROM wire_docs FOR ALL SYSTEM_TIME WHERE _id = 1 ORDER BY _system_from");
    assert_eq!(ints(&query(&mut client, history).await.0), [1, 1]);
    running.stop().await;
}

#[tokio::test]
async fn rtdb_v1_changes_whole_groups_images_wait_and_expiry() {
    use arrow_array::BinaryArray;
    let running = Running::start(None, FlightConfig::default()).await;
    let mut reader = Client::connect(&running.server, None).await;
    let mut writer = Client::connect(&running.server, None).await;
    writer
        .update("INSERT INTO wire_changes (_id, n) VALUES (1, 10)", None)
        .await;
    let start = query(&mut reader, input("SELECT n FROM wire_changes"))
        .await
        .1;
    let request = v1::ChangesRequest {
        branch: "main".into(),
        tables: vec!["wire_changes".into()],
        after: start.basis.clone(),
        images: v1::Images::Both as i32,
        limit: 1,
        wait_millis: Some(2_000),
        ..Default::default()
    };
    let wait = async {
        get::<_, v1::ChangesTrailer>(&mut reader, request.clone())
            .await
            .unwrap()
    };
    let write = async {
        tokio::time::sleep(Duration::from_millis(25)).await;
        writer
            .update("UPDATE wire_changes SET n = 20 WHERE _id = 1", None)
            .await;
    };
    let ((rows, page), ()) = tokio::join!(wait, write);
    assert!(page.complete);
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    for column in [3, 4, 5] {
        let payloads = rows
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap();
                column.iter().flatten()
            })
            .collect::<Vec<_>>();
        assert_eq!(payloads.len(), if column == 3 { 2 } else { 1 });
        for payload in payloads {
            assert!(
                StreamReader::try_new(payload, None)
                    .unwrap()
                    .next()
                    .unwrap()
                    .is_ok()
            );
        }
    }
    let mut shared_request = request.clone();
    shared_request.until = page.through.clone();
    shared_request.wait_millis = None;
    reader.headers.insert(CHANGE_IMAGE_HEADER, "stream-v1".parse().unwrap());
    let (shared, shared_page, headers) =
        get_response::<_, v1::ChangesTrailer>(&mut reader, shared_request).await.unwrap();
    assert_eq!(headers.get(CHANGE_IMAGE_HEADER).unwrap(), "stream-v1");
    assert_eq!(shared, rows);
    assert_eq!(shared_page, page);
    reader.headers.remove(CHANGE_IMAGE_HEADER);

    let mut request = request;
    request.after = page.through;
    request.after.as_mut().unwrap().value[0] ^= 1;
    let status = get::<_, v1::ChangesTrailer>(&mut reader, request.clone())
        .await
        .unwrap_err();
    error(status, v1::AnomalyCategory::NotFound, "rtdb/cursor-expired");
    let database = running.node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    running
        .node
        .sync_all(
            Duration::from_secs(10),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
    request.after = start.basis;
    let status = get::<_, v1::ChangesTrailer>(&mut reader, request)
        .await
        .unwrap_err();
    error(status, v1::AnomalyCategory::NotFound, "rtdb/cursor-expired");
    running.stop().await;
}

#[tokio::test]
async fn rtdb_v1_changes_packs_rows_and_progress_covers_the_same_groups() {
    use arrow_array::{Array, cast::AsArray, types::TimestampMicrosecondType};
    use rtdb_types::rtdb_v1::Basis;

    let running = Running::start(None, FlightConfig::default()).await;
    let mut client = Client::connect(&running.server, None).await;
    client
        .update("INSERT INTO wire_a (_id, n) VALUES (0, 0)", None)
        .await;
    client
        .update("INSERT INTO wire_z (_id, n) VALUES (0, 0)", None)
        .await;
    let (_, current) = query(&mut client, input("SELECT 1")).await;
    let (empty, start) = query(&mut client, input("SELECT 1 LIMIT 0")).await;
    assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    assert_eq!(current.basis, start.basis);

    let tx = client.begin().await;
    client
        .update(
            "INSERT INTO wire_a (_id, n) VALUES (1, 10), (2, 20)",
            Some(tx.clone()),
        )
        .await;
    client
        .update(
            "INSERT INTO wire_z (_id, n) VALUES (1, 30)",
            Some(tx.clone()),
        )
        .await;
    client.end(tx, true).await.unwrap();
    let (_, earlier) = query(&mut client, input("SELECT 1 LIMIT 0")).await;
    client
        .update("INSERT INTO wire_a (_id, n) VALUES (3, 40), (4, 50)", None)
        .await;
    let (_, later) = query(&mut client, input("SELECT 1 LIMIT 0")).await;

    for (order, expected_time, row_counts, table_names) in [
        (
            v1::Order::Ascending,
            earlier.system_time_micros,
            &[2, 1][..],
            &["public.wire_a", "public.wire_z"][..],
        ),
        (
            v1::Order::Descending,
            later.system_time_micros,
            &[2][..],
            &["public.wire_a"][..],
        ),
    ] {
        let mut request = v1::ChangesRequest {
            branch: "main".into(),
            tables: vec!["wire_z".into(), "wire_a".into()],
            after: start.basis.clone(),
            until: later.basis.clone(),
            order: order as i32,
            images: v1::Images::Both as i32,
            limit: 1,
            ..Default::default()
        };
        let (rows, images, headers) =
            get_response::<_, v1::ChangesTrailer>(&mut client, request.clone()).await.unwrap();
        assert!(headers.get(CHANGE_IMAGE_HEADER).is_none());
        assert_eq!(
            rows.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            row_counts,
        );
        for (batch, table) in rows.iter().zip(table_names) {
            let times = batch.column(1).as_primitive::<TimestampMicrosecondType>();
            let keys = batch.column(3).as_binary::<i32>();
            let before = batch.column(4).as_binary::<i32>();
            let after = batch.column(5).as_binary::<i32>();
            for row in 0..batch.num_rows() {
                assert_eq!(batch.column(0).as_string::<i32>().value(row), *table);
                assert_eq!(times.value(row), expected_time);
                assert!(batch.column(2).as_boolean().value(row));
                assert!(keys.is_valid(row) && after.is_valid(row) && before.is_null(row));
                let key = StreamReader::try_new(keys.value(row), None)
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap();
                let after = StreamReader::try_new(after.value(row), None)
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap();
                assert_eq!(key.num_rows(), 1);
                assert_eq!(after.num_rows(), 1);
                assert_eq!(
                    key.column(0).as_ref(),
                    after.column_by_name("_id").unwrap().as_ref()
                );
            }
        }
        assert!(!images.complete);
        let expected_through = match order {
            v1::Order::Ascending => earlier.basis.clone().unwrap(),
            v1::Order::Descending => {
                let basis = Basis::from_bytes(&later.basis.as_ref().unwrap().value).unwrap();
                v1::Basis {
                    value: Basis::new(basis.incarnation(), basis.position() - 1)
                        .to_bytes()
                        .to_vec(),
                }
            }
        };
        assert_eq!(images.through, Some(expected_through));

        client.headers.insert(CHANGE_IMAGE_HEADER, "stream-v1".parse().unwrap());
        let (shared, shared_trailer, headers) =
            get_response::<_, v1::ChangesTrailer>(&mut client, request.clone()).await.unwrap();
        assert_eq!(headers.get(CHANGE_IMAGE_HEADER).unwrap(), "stream-v1");
        assert_eq!(shared_trailer, images);
        assert_eq!(shared.len(), rows.len());
        for (ordinary, shared) in rows.iter().zip(&shared) {
            assert_eq!(shared.schema(), ordinary.schema());
            assert_eq!(&shared.columns()[..3], &ordinary.columns()[..3]);
            for column in [3, 4, 5] {
                let ordinary = ordinary.column(column).as_binary::<i32>();
                let shared = shared.column(column).as_binary::<i32>();
                assert!(shared.iter().skip(1).all(|cell| cell.is_none()));
                let decoded = match shared.iter().next().flatten() {
                    Some(stream) => StreamReader::try_new(stream, None)
                        .unwrap()
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .unwrap(),
                    None => Vec::new(),
                };
                let expected = ordinary
                    .iter()
                    .flatten()
                    .map(|cell| StreamReader::try_new(cell, None).unwrap().next().unwrap().unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(decoded, expected);
            }
        }

        client.headers.insert(CHANGE_IMAGE_HEADER, "future".parse().unwrap());
        let (unknown, unknown_trailer, headers) =
            get_response::<_, v1::ChangesTrailer>(&mut client, request.clone()).await.unwrap();
        assert!(headers.get(CHANGE_IMAGE_HEADER).is_none());
        assert_eq!(unknown, rows);
        assert_eq!(unknown_trailer, images);

        request.images = v1::Images::None as i32;
        client.headers.insert(CHANGE_IMAGE_HEADER, "stream-v1".parse().unwrap());
        let (_, ordinary, headers) =
            get_response::<_, v1::ChangesTrailer>(&mut client, request.clone()).await.unwrap();
        assert!(headers.get(CHANGE_IMAGE_HEADER).is_none());
        assert_eq!(ordinary, images);
        request.progress_only = true;
        let (rows, progress, headers) =
            get_response::<_, v1::ChangesTrailer>(&mut client, request.clone()).await.unwrap();
        assert!(headers.get(CHANGE_IMAGE_HEADER).is_none());
        assert!(rows.is_empty());
        assert_eq!(progress, ordinary);
        client.headers.remove(CHANGE_IMAGE_HEADER);

        request.images = v1::Images::Keys as i32;
        let status = get::<_, v1::ChangesTrailer>(&mut client, request)
            .await
            .unwrap_err();
        error(
            status,
            v1::AnomalyCategory::Incorrect,
            "rtdb/invalid-request",
        );
    }

    let mut request = v1::ChangesRequest {
        branch: "main".into(),
        tables: vec!["wire_a".into()],
        after: start.basis,
        limit: 1,
        wait_millis: Some(30_000),
        progress_only: true,
        ..Default::default()
    };
    let (rows, observed) = tokio::time::timeout(
        Duration::from_secs(10),
        get::<_, v1::ChangesTrailer>(&mut client, request.clone()),
    )
    .await
    .expect("a matching group completes the wait")
    .unwrap();
    assert!(rows.is_empty());
    assert_eq!(observed.through, earlier.basis);
    assert!(!observed.complete);

    request.after = later.basis.clone();
    request.wait_millis = Some(10);
    let (rows, timeout) = get::<_, v1::ChangesTrailer>(&mut client, request)
        .await
        .unwrap();
    assert!(rows.is_empty());
    assert_eq!(timeout.through, later.basis);
    assert!(timeout.complete);
    running.stop().await;
}

#[tokio::test]
#[ignore = "requires a running Flight endpoint"]
async fn rtdb_v1_remote_consumer() {
    let endpoint = std::env::var("RTDB_CONFORMANCE_ENDPOINT").expect("conformance endpoint");
    let transport = Channel::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = Client {
        client: FlightServiceClient::new(transport),
        headers: MetadataMap::new(),
    };
    if let Ok(database) = std::env::var("RTDB_CONFORMANCE_DATABASE") {
        client
            .headers
            .insert("x-xtdb-database", database.parse().unwrap());
    }
    if let Ok(token) = std::env::var("RTDB_CONFORMANCE_AUTHORIZATION") {
        client
            .headers
            .insert("authorization", token.parse().unwrap());
    }
    let reply = client
        .action("rtdb.v1.Capabilities", Bytes::new())
        .await
        .unwrap();
    assert_eq!(
        v1::CapabilitiesResponse::decode(reply[0].body.clone())
            .unwrap()
            .api,
        "rtdb.v1"
    );
    let (rows, trailer) = query(&mut client, input("SELECT 42::BIGINT AS value")).await;
    assert_eq!(ints(&rows), [42]);
    assert!(trailer.basis.is_some());
}

struct IndexedFixtureResources {
    native: rtdb_node::NativeResources,
    confirmations: Arc<AtomicI64>,
}

struct FixtureReadIndex(Arc<AtomicI64>);

#[async_trait]
impl rtdb_node::ReplicaReadIndex for FixtureReadIndex {
    async fn confirm(&self) -> Result<rtdb_storage::log::LogOffset> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(rtdb_storage::log::LogOffset(-1))
    }
}

#[async_trait]
impl rtdb_node::ResourceFactory for IndexedFixtureResources {
    fn validate(
        &self,
        name: &str,
        config: &rtdb_format::database::proto::DatabaseConfig,
    ) -> Result<()> {
        self.native.validate(name, config)
    }
    async fn open_storage(
        &self,
        name: &str,
        config: &rtdb_format::database::proto::DatabaseConfig,
    ) -> Result<rtdb_node::DatabaseStorage> {
        self.native.open_storage(name, config).await
    }
    async fn open_logs(
        &self,
        name: &str,
        config: &rtdb_format::database::proto::DatabaseConfig,
        access: rtdb_node::LogAccess,
        cancellation: tokio_util::sync::CancellationToken,
        storage: &rtdb_node::DatabaseStorage,
    ) -> Result<rtdb_node::OpenedLogs> {
        let mut logs = self
            .native
            .open_logs(name, config, access, cancellation, storage)
            .await?;
        logs.replica_read_index = Some(Box::new(FixtureReadIndex(self.confirmations.clone())));
        Ok(logs)
    }
    async fn close(&self) -> Result<()> {
        self.native.close().await
    }
}

#[tokio::test]
async fn rtdb_v1_placed_attachment_uses_the_replica_read_index() {
    let confirmations = Arc::new(AtomicI64::new(0));
    let resources = IndexedFixtureResources {
        native: Default::default(),
        confirmations: confirmations.clone(),
    };
    let options = NodeOptions {
        read_only: true,
        ..Default::default()
    };
    let running = Running::start_with_resources(
        None,
        FlightConfig::default(),
        &["wire-tenant"],
        None,
        options,
        None,
        Some(Arc::new(resources)),
    )
    .await;
    let mut client =
        Client::connect_with_database(&running.server, None, Some("wire-tenant")).await;
    let mut request = input("SELECT 42::BIGINT");
    request.source.as_mut().unwrap().at = Some(v1::At {
        selection: Some(Selection::Linearizable(())),
    });
    let (rows, selected) = query(&mut client, request).await;
    assert_eq!(ints(&rows), [42]);
    assert!(selected.basis.is_some());
    assert_eq!(confirmations.load(Ordering::Relaxed), 1);
    running.stop().await;
}

#[tokio::test]
#[ignore = "manual latency benchmark"]
async fn rtdb_v1_bench_point_and_range_against_held_reads() {
    use arrow_array::{
        ArrayRef,
        builder::{ListBuilder, StringBuilder},
    };
    use std::time::Instant;

    fn ipc(prefix: &[u8], fields: Vec<(&str, ArrayRef)>) -> Bytes {
        let batch = RecordBatch::try_from_iter(fields).unwrap();
        let mut bytes = prefix.to_vec();
        bytes.extend(
            rtdb_format::ipc::write_stream(batch.schema().as_ref(), &[batch.clone()]).unwrap(),
        );
        bytes.into()
    }

    let running = Running::start(
        Some("secret"),
        FlightConfig {
            owned_native_actions: true,
            ..Default::default()
        },
    )
    .await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    let relation = b"\0\x05items";
    let mut declaration = relation.to_vec();
    declaration.extend(
        br#"{"fields":[{"name":"value","type":{"kind":"i64"},"optional":false,"nullable":false}]}"#,
    );
    client
        .action("rtdb.owned.DeclareRelation.v1", declaration.into())
        .await
        .unwrap();
    let mut columns = ListBuilder::new(StringBuilder::new());
    columns.values().append_value("value");
    columns.values().append_value("_id");
    columns.append(true);
    client
        .action(
            "rtdb.owned.DeclarePath.v1",
            ipc(
                relation,
                vec![
                    ("_id", Arc::new(StringArray::from(vec!["by_value"]))),
                    ("columns", Arc::new(columns.finish())),
                ],
            ),
        )
        .await
        .unwrap();
    let rows = 1024_i64;
    let mut sql_values = Vec::new();
    for id in 1..=rows {
        client
            .action(
                "rtdb.owned.Create.v1",
                ipc(
                    relation,
                    vec![
                        ("_id", Arc::new(Int64Array::from(vec![id]))),
                        ("value", Arc::new(Int64Array::from(vec![id]))),
                    ],
                ),
            )
            .await
            .unwrap();
        sql_values.push(format!("({id}, {id})"));
    }
    client
        .update(
            &format!(
                "INSERT INTO wire_bench (_id, value) VALUES {}",
                sql_values.join(",")
            ),
            None,
        )
        .await;
    let database = running.node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    running
        .node
        .sync_all(
            Duration::from_secs(10),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
    database.build_artefacts().await.unwrap();
    let held = client
        .action("rtdb.owned.OpenRead.v3", Bytes::new())
        .await
        .unwrap()[0]
        .body
        .slice(..16);
    let prefix = [held.as_ref(), relation].concat();
    for (sql, action, is_range) in [
        (
            "SELECT value FROM wire_bench WHERE _id = $1",
            "rtdb.owned.ReadHeldPoint.v2",
            false,
        ),
        (
            "SELECT value FROM wire_bench WHERE value >= $1 AND value < $2 ORDER BY value, _id",
            "rtdb.owned.ReadHeldRange.v1",
            true,
        ),
    ] {
        let mut query_samples = Vec::new();
        let mut held_samples = Vec::new();
        for id in 1..=256_i64 {
            let mut request = input(sql);
            let lower = Arc::new(Int64Array::from(vec![id]));
            let upper = Arc::new(Int64Array::from(vec![id + 16]));
            let mut params: Vec<(&str, ArrayRef)> = vec![("lower", lower.clone())];
            if is_range {
                params.push(("upper", upper.clone()));
            }
            request.params_ipc = Some(ipc(&[], params).to_vec());
            let held_request = if is_range {
                ipc(
                    &prefix,
                    vec![
                        ("path", Arc::new(StringArray::from(vec!["by_value"]))),
                        ("lower", lower),
                        ("upper", upper),
                        ("limit", Arc::new(Int64Array::from(vec![16]))),
                    ],
                )
            } else {
                ipc(
                    &prefix,
                    vec![
                        ("_id", lower),
                        ("valid_micros", Arc::new(Int64Array::from(vec![None]))),
                    ],
                )
            };
            let started = Instant::now();
            let (batches, _) = query(&mut client, request).await;
            let query_elapsed = started.elapsed();
            let started = Instant::now();
            let response = client.action(action, held_request).await.unwrap();
            let held = StreamReader::try_new(response[0].body.as_ref(), None)
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            let held_elapsed = started.elapsed();
            let held_values = held
                .iter()
                .flat_map(|batch| {
                    let column = batch.schema().index_of("value").unwrap();
                    (0..batch.num_rows()).map(move |row| integer(batch, column, row))
                })
                .collect::<Vec<_>>();
            assert_eq!(ints(&batches), held_values);
            if id > 16 {
                query_samples.push(query_elapsed);
                held_samples.push(held_elapsed);
            }
        }
        for (transport, mut samples) in [("Query", query_samples), (action, held_samples)] {
            samples.sort_unstable();
            let p50 = samples[samples.len() / 2].as_micros();
            let p95 = samples[samples.len() * 95 / 100].as_micros();
            println!(
                "{transport} range={is_range} count={} p50={p50}us p95={p95}us",
                samples.len()
            );
        }
    }
    client
        .action("rtdb.owned.CloseRead.v1", held)
        .await
        .unwrap();
    running.stop().await;
}

const FIXTURE_INCARNATION: [u8; 16] = [41; 16];

struct AppliedFixture {
    state: std::sync::Mutex<rtdb_tx::transact::TransactState>,
    tokens: rtdb_crypt::tokens::TokenAuthority,
    memory: Arc<MemoryBudget>,
    admission: std::sync::Mutex<Option<rtdb_tx::transact::ResolvedTransact>>,
    hold_first: std::sync::atomic::AtomicBool,
    waiting: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

struct FixtureTokenKeys(rtdb_crypt::tokens::TokenKey);

#[async_trait]
impl rtdb_crypt::tokens::TokenKeys for FixtureTokenKeys {
    async fn current(&self) -> Result<rtdb_crypt::tokens::TokenKey> { Ok(self.0.clone()) }

    async fn get(&self, number: u64) -> Result<Option<rtdb_crypt::tokens::TokenKey>> {
        Ok((number == self.0.generation().number).then(|| self.0.clone()))
    }
}

#[async_trait]
impl rtdb_node::serving::ServingDriver for AppliedFixture {
    fn incarnation(&self) -> [u8; 16] {
        FIXTURE_INCARNATION
    }

    fn token_authority(&self) -> Option<rtdb_crypt::tokens::TokenAuthority> { Some(self.tokens.clone()) }

    fn features(&self) -> &[&str] {
        &[
            "query",
            "query.track",
            "query.page",
            "linearizable",
            "transact",
            "tx-status",
            "branches",
        ]
    }

    async fn query(
        &self,
        source: rtdb_node::serving::QuerySource,
        _track: bool,
        now: Timestamp,
        _: &tokio_util::sync::CancellationToken,
    ) -> Result<rtdb_node::serving::QueryView> {
        use rtdb_types::rtdb_v1::At;
        let state = self.state.lock().unwrap();
        let at = if source.at == At::Linearizable {
            At::Latest
        } else {
            source.at
        };
        let view = state.index().view(&source.branch, at, source.layer, now)?;
        Ok(rtdb_node::serving::QueryView {
            custody_groups: Default::default(),
            custody_contributors: None,
            key_generation: None,
            snapshot: rtdb_query::branch_snapshot(&view, &self.memory)?,
            cut: view.cut(),
            claim: None,
        })
    }

    async fn transact(
        &self,
        request: rtdb_tx::transact::Transact,
        _: &tokio_util::sync::CancellationToken,
    ) -> Result<rtdb_tx::transact::Receipt> {
        use rtdb_tx::transact::{Resolution, TransactionRecord};
        if request.key == [1; 16] && self.hold_first.swap(false, std::sync::atomic::Ordering::AcqRel) {
            self.waiting.notify_one();
            self.release.notified().await;
        }
        let claims = self.tokens.verify_claims(request.claims()).await;
        let mut state = self.state.lock().unwrap();
        let cut = state.index().head();
        let now = if cut.basis.position() == 0 {
            Timestamp(1)
        } else {
            cut.system_time.next()?
        };
        match state.resolve(
            &request,
            cut.basis.position() + 1,
            now,
            self.features(),
            &claims,
        )? {
            Resolution::Answer(receipt) => Ok(receipt),
            Resolution::Append(append) => {
                let record = TransactionRecord::decode(&append.record().encode()?)?;
                if request.key == [1; 16] {
                    let TransactionRecord::Tx(admitted) = &record else { unreachable!("an admission produces a Tx record"); };
                    *self.admission.lock().unwrap() = Some(admitted.clone());
                }
                Ok(state.apply(append.position(), record)?.unwrap())
            }
        }
    }

    async fn tx_status(
        &self,
        key: &[u8],
        _: &tokio_util::sync::CancellationToken,
    ) -> Result<rtdb_tx::transact::Receipt> {
        self.state.lock().unwrap().tx_status(key)
    }

    async fn changes(
        &self,
        request: rtdb_node::serving::Changes,
        _: &tokio_util::sync::CancellationToken,
    ) -> Result<rtdb_node::serving::ChangePage> {
        use rtdb_node::serving::{ChangeGroup, ChangeKind, ChangePage, ChangeRow};
        use rtdb_types::{AnomalyCategory, EntityId, TableRef};
        let cut = self.state.lock().unwrap().index().head();
        if request.after.is_none() {
            return Err(Anomaly::new(
                AnomalyCategory::NotFound,
                "rtdb/cursor-expired",
                "History was trimmed",
            ));
        }
        Ok(ChangePage {
            read_custody: None,
            groups: vec![ChangeGroup {
                cut,
                rows: vec![ChangeRow {
                    table: TableRef::new("public", "wire_applied"),
                    id: EntityId::Integer(2),
                    valid: None,
                    kind: ChangeKind::Erased,
                    erased_written_basis: None,
                    before: None,
                    after: None,
                }],
            }],
            through: cut.basis,
            complete: true,
            custody_groups: Default::default(),
            key_generation: None,
        })
    }
}

struct AppliedFixtureResources {
    native: rtdb_node::NativeResources,
    driver: Arc<AppliedFixture>,
    provisioned: Option<Arc<tokio::sync::Mutex<Vec<(String, uuid::Uuid)>>>>,
}

#[async_trait]
impl rtdb_node::ResourceFactory for AppliedFixtureResources {
    async fn provision_managed_database(
        &self,
        name: &str,
        expected: Option<uuid::Uuid>,
    ) -> Result<(rtdb_format::database::proto::DatabaseConfig, uuid::Uuid)> {
        let provisioned = self.provisioned.as_ref().unwrap();
        let incarnation = expected.unwrap();
        provisioned.lock().await.push((name.into(), incarnation));
        Ok((Default::default(), incarnation))
    }

    fn serving_driver(
        &self,
        _: &str,
        _: &rtdb_format::database::proto::DatabaseConfig,
        _: &tokio_util::sync::CancellationToken,
    ) -> Result<Option<Arc<dyn rtdb_node::serving::ServingDriver>>> {
        Ok(Some(self.driver.clone()))
    }

    fn validate(
        &self,
        name: &str,
        config: &rtdb_format::database::proto::DatabaseConfig,
    ) -> Result<()> {
        self.native.validate(name, config)
    }

    async fn open_storage(
        &self,
        name: &str,
        config: &rtdb_format::database::proto::DatabaseConfig,
    ) -> Result<rtdb_node::DatabaseStorage> {
        self.native.open_storage(name, config).await
    }

    async fn open_logs(
        &self,
        name: &str,
        config: &rtdb_format::database::proto::DatabaseConfig,
        access: rtdb_node::LogAccess,
        cancellation: tokio_util::sync::CancellationToken,
        storage: &rtdb_node::DatabaseStorage,
    ) -> Result<rtdb_node::OpenedLogs> {
        self.native
            .open_logs(name, config, access, cancellation, storage)
            .await
    }

    async fn close(&self) -> Result<()> {
        self.native.close().await
    }
}

fn integer_cell(value: i64) -> v1::Cell {
    scalar_cell(Arc::new(Int64Array::from(vec![value])))
}

fn scalar_cell(value: arrow_array::ArrayRef) -> v1::Cell {
    let batch = RecordBatch::try_from_iter([("value", value)]).unwrap();
    v1::Cell {
        value: Some(v1::cell::Value::ArrowIpc(
            rtdb_format::ipc::write_stream(batch.schema_ref(), &[batch.clone()]).unwrap(),
        )),
    }
}

#[tokio::test]
async fn rtdb_v1_ranks_rows_at_the_native_receipt_basis() {
    let resources = rtdb_node::NativeResources::default().with_native_serving_authority(
        "xtdb", rtdb_node::serving::NativeServingAuthority { incarnation: [32; 16], checkpoints: None },
    );
    let running = Running::start_with_resources(
        None, FlightConfig::default(), &[], None, Default::default(), None, Some(Arc::new(resources)),
    ).await;
    let database = running.node.catalog().database("xtdb").unwrap();
    let mut status = database.watch_status();
    tokio::time::timeout(Duration::from_secs(5), status.wait_for(|status| {
        matches!(status.role, rtdb_replication::Role::Leading(_))
    })).await.unwrap().unwrap();
    let mut client = Client::connect(&running.server, None).await;
    let geometry = [0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let row = |id, text, vector: [f32; 2]| {
        let embedding = arrow_array::ListArray::from_iter_primitive::<arrow_array::types::Float32Type, _, _>([
            Some(vector.map(Some)),
        ]);
        v1::Row {
            cells: [("_id".into(), integer_cell(id)),
                ("body".into(), scalar_cell(Arc::new(StringArray::from(vec![text])))),
                ("geometry".into(), scalar_cell(Arc::new(arrow_array::BinaryArray::from_vec(vec![geometry.as_slice()])))),
                ("embedding".into(), scalar_cell(Arc::new(embedding)))].into_iter().collect(),
            ..Default::default()
        }
    };
    let put = |key: u8, rows| v1::TransactRequest {
        key: vec![key; 16],
        ops: vec![v1::Op { op: Some(v1::op::Op::Put(v1::Put {
            branch: "main".into(), table: "public.ranked_rows".into(), rows, if_absent: false,
        })) }],
        ..Default::default()
    };
    let original = transact_wire(&mut client, put(1, vec![row(1, "receipt", [1.0, 0.0]), row(2, "invoice", [0.0, 1.0])])).await.unwrap();
    let later = transact_wire(&mut client, put(2, vec![row(1, "invoice", [0.0, 1.0])])).await.unwrap();
    assert!(matches!(later.outcome, Some(v1::receipt::Outcome::Then(()))));
    for (sql, expected) in [
        ("SELECT _id FROM RTDB_SEARCH(ranked_rows, body, 'receipt')", 1),
        ("SELECT _id FROM RTDB_NEAREST(ranked_rows, embedding, ARRAY[0.0, 1.0], 1)", 2),
    ] {
        let mut request = input(sql);
        request.source.as_mut().unwrap().at = Some(v1::At { selection: original.basis.clone().map(Selection::Exactly) });
        request.observe_receiver_clock = true;
        let (rows, trailer) = query(&mut client, request).await;
        assert_eq!(ints(&rows), [expected]);
        assert_eq!(trailer.basis, original.basis);
        assert_eq!(trailer.system_time_micros, original.system_time_micros);
        assert!(!trailer.receiver_clock.unwrap().proof.is_empty());
    }
    for (text, expected) in [("receipt", vec![1]), ("absent", vec![])] {
        let mut request = input(&format!(
            "SELECT SEARCH_TOKEN_COUNT(body) FROM ranked_rows WHERE MATCH(body, '{text}')"
        ));
        request.source.as_mut().unwrap().at = Some(v1::At { selection: original.basis.clone().map(Selection::Exactly) });
        request.track = true;
        request.require_clear_rank_input = true;
        let (rows, trailer) = query(&mut client, request).await;
        assert_eq!(ints(&rows), expected);
        assert_eq!(trailer.basis, original.basis);
        assert!(!trailer.claim.unwrap().value.is_empty());
    }
    let mut request = input(
        "WITH staged(_id, embedding) AS (
             VALUES (2, ARRAY[CAST(0 AS REAL), CAST(0 AS REAL)]),
                    (3, ARRAY[CAST(1 AS REAL), CAST(0 AS REAL)])
         ), MATERIALIZED candidates AS (
             SELECT _id, embedding FROM ranked_rows
             WHERE _id NOT IN (SELECT _id FROM staged)
             UNION ALL SELECT _id, embedding FROM staged LIMIT 4
         ), MATERIALIZED scores AS (
             SELECT _id, COSINE_DISTANCE_F32(embedding, $1) AS distance FROM candidates
         ), totals AS (
             SELECT RANK_ADMITTED_COUNT(COUNT(*), 3) AS admitted, COUNT(distance) AS total FROM scores
         ), hits AS (
             SELECT _id, distance FROM scores WHERE distance IS NOT NULL
             ORDER BY distance, ENTITY_SORT_KEY(_id) LIMIT 2
         )
         SELECT hits._id, totals.total, totals.admitted FROM totals LEFT JOIN hits ON TRUE
         ORDER BY hits.distance, ENTITY_SORT_KEY(hits._id)"
    );
    let probe = arrow_array::ListArray::from_iter_primitive::<arrow_array::types::Float32Type, _, _>([Some([Some(1.0), Some(0.0)])]);
    let params = RecordBatch::try_from_iter([("probe", Arc::new(probe) as arrow_array::ArrayRef)]).unwrap();
    request.params_ipc = Some(rtdb_format::ipc::write_stream(params.schema_ref(), &[params.clone()]).unwrap());
    request.source.as_mut().unwrap().at = Some(v1::At { selection: original.basis.clone().map(Selection::Exactly) });
    request.track = true;
    request.require_clear_rank_input = true;
    let (rows, trailer) = query(&mut client, request).await;
    assert_eq!(ints(&rows), [1, 3]);
    assert!(rows.iter().all(|batch| (0..batch.num_rows()).all(|row| integer(batch, 1, row) == 2)));
    assert_eq!(trailer.basis, original.basis);
    assert!(!trailer.claim.unwrap().value.is_empty());
    let mut request = input("SELECT _id FROM ranked_rows WHERE FALSE");
    request.track = true;
    request.require_clear_rank_input = true;
    let status = get::<_, v1::QueryTrailer>(&mut client, request).await.unwrap_err();
    error(status, v1::AnomalyCategory::Unsupported, "rtdb/unsupported");

    for (anchor, expected) in [(0, vec![1, 2]), (1, vec![2]), (2, vec![])] {
        let point = "X'00010100000000000000000000000000000000000000'";
        let mut request = input(&format!(
            "WITH MATERIALIZED distances AS (
                 SELECT _id, _rtdb_written_basis, body,
                        SPATIAL_DISTANCE(geometry, {point}, 'tagged-wkb-v1', 'wgs84_geometry_m', 0) AS distance
                 FROM ranked_rows
                 WHERE SPATIAL_MATCH(geometry, {point}, 'tagged-wkb-v1', 'equals')
             )
             SELECT _id, _rtdb_written_basis, body, distance FROM distances
             WHERE distance IS NOT NULL AND (distance > 0 OR ENTITY_SORT_KEY(_id) > ENTITY_SORT_KEY({anchor}))
             ORDER BY distance, ENTITY_SORT_KEY(_id) LIMIT 2"
        ));
        request.source.as_mut().unwrap().at = Some(v1::At { selection: original.basis.clone().map(Selection::Exactly) });
        request.track = true;
        request.require_clear_rank_input = true;
        let (rows, trailer) = query(&mut client, request).await;
        assert_eq!(ints(&rows), expected);
        for batch in &rows {
            for row in 0..batch.num_rows() {
                assert!(!batch.column(1).is_null(row));
                assert!(!batch.column(2).is_null(row));
            }
        }
        assert_eq!(trailer.basis, original.basis);
        assert!(!trailer.claim.unwrap().value.is_empty());
    }
    running.stop().await;
}

async fn transact_wire(
    client: &mut Client,
    request: v1::TransactRequest,
) -> std::result::Result<v1::Receipt, Status> {
    let result = client
        .action("rtdb.v1.Transact", request.encode_to_vec().into())
        .await?;
    Ok(v1::Receipt::decode(result[0].body.clone()).unwrap())
}

#[tokio::test]
async fn native_ivf_proves_the_generation_and_ranks_the_resolved_filtered_overlay() {
    use arrow_array::{BinaryArray, Float64Array, ListArray, types::Float32Type};
    use rtdb_types::rtdb_v1::{Basis, RANKED_IVF};
    use tokio_util::sync::CancellationToken;

    let resources = rtdb_node::NativeResources::default().with_native_serving_authority(
        "xtdb", rtdb_node::serving::NativeServingAuthority { incarnation: [33; 16], checkpoints: None },
    );
    let options = NodeOptions { file_size_target: 1, automatic_artefacts: false, ..Default::default() };
    let running = Running::start_with_resources(
        None, FlightConfig::default(), &[], None, options, None, Some(Arc::new(resources)),
    ).await;
    let database = running.node.catalog().database("xtdb").unwrap();
    let mut status = database.watch_status();
    tokio::time::timeout(Duration::from_secs(5), status.wait_for(|status| {
        matches!(status.role, rtdb_replication::Role::Leading(_))
    })).await.unwrap().unwrap();
    let mut client = Client::connect(&running.server, None).await;
    let declaration = transact_wire(&mut client, v1::TransactRequest {
        key: vec![1; 16],
        ops: vec![v1::Op { op: Some(v1::op::Op::Declare(v1::Declare {
            table: "public.vectors".into(),
            change: Some(v1::declare::Change::Index(v1::Index {
                name: "vectors_ivf".into(), kind: v1::IndexKind::Vector as i32,
                columns: vec!["embedding".into(), "category".into()],
                options: Some(v1::index::Options::Vector(v1::VectorIndexOptions {
                    method: "ivf".into(), dimensions: 2, recall: 0.9,
                })),
            })),
        })) }],
        ..Default::default()
    }).await.unwrap();
    let generation = Basis::from_bytes(&declaration.basis.unwrap().value).unwrap().position();

    let row = |id, vector: [f32; 2], category: &[u8]| v1::Row {
        cells: [
            ("_id".into(), id),
            ("embedding".into(), scalar_cell(Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>([Some(vector.map(Some))])))),
            ("category".into(), scalar_cell(Arc::new(BinaryArray::from_vec(vec![category])))),
        ].into_iter().collect(),
        ..Default::default()
    };
    let mut rows = vec![
        row(integer_cell(2), [1.0, 0.0], b"allow"),
        row(integer_cell(3), [3.0, 4.0], b"allow"),
        row(scalar_cell(Arc::new(StringArray::from(vec!["3"]))), [3.0, 4.0], b"allow"),
        row(integer_cell(5), [1.0, 0.0], b"allow"),
        row(integer_cell(6), [0.0, 0.0], b"allow"),
    ];
    rows.extend((100..170).map(|id| row(integer_cell(id), [1.0, 0.0], b"deny")));
    let written = transact_wire(&mut client, v1::TransactRequest {
        key: vec![2; 16],
        ops: vec![v1::Op { op: Some(v1::op::Op::Put(v1::Put {
            table: "public.vectors".into(), branch: "main".into(), rows, if_absent: false,
        })) }],
        ..Default::default()
    }).await.unwrap();

    let mut request = input("");
    request.source.as_mut().unwrap().at = Some(v1::At { selection: written.basis.clone().map(Selection::Exactly) });
    request.track = true;
    request.require_clear_rank_input = true;
    request.ranked_ivf = Some(v1::RankedIvf {
        schema: "public".into(), table: "vectors".into(), index: "vectors_ivf".into(), generation,
        valid_at_micros: Some(written.system_time_micros), k: 8, probe: vec![1.0, 0.0],
        filters: vec![v1::RankedIvfFilter { column: "category".into(), values: vec![b"allow".to_vec(), b"other".to_vec()] }],
        staged: [(2, [0.0, 0.0], Some(b"allow".to_vec())), (4, [1.0, 1.0], Some(b"allow".to_vec())), (5, [1.0, 0.0], None)]
            .into_iter().map(|(id, embedding, value)| v1::RankedIvfStaged {
                identity_key: EntityId::Integer(id).handle_bytes(),
                replacement: Some(v1::RankedIvfReplacement { embedding: embedding.to_vec(), filter_values: vec![v1::RankedIvfFilterCell { value }] }),
            }).collect(),
    });
    assert!(get::<_, v1::QueryTrailer>(&mut client, request.clone()).await.is_err());

    database.flush().await.unwrap();
    running.node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();
    running.node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
    let caps = client.action("rtdb.v1.Capabilities", Bytes::new()).await.unwrap();
    assert!(v1::CapabilitiesResponse::decode(caps[0].body.clone()).unwrap().features.iter().any(|feature| feature == RANKED_IVF));

    let mut readiness = input("SELECT COUNT(*) FROM xt.ranked_indexes WHERE _id = 'vectors_ivf' AND artefacts_ready AND serving_mode = 'exact-sql' AND NOT approximate");
    readiness.source = request.source.clone();
    readiness.track = true;
    readiness.require_clear_rank_input = true;
    readiness.verify_ranked_artefacts = true;
    let (rows, trailer) = query(&mut client, readiness).await;
    assert_eq!(ints(&rows), [1]);
    assert!(trailer.ranked_ivf.is_none());

    let mut wrong_generation = request.clone();
    wrong_generation.ranked_ivf.as_mut().unwrap().generation += 1;
    assert!(get::<_, v1::QueryTrailer>(&mut client, wrong_generation).await.is_err());
    let (batches, trailer) = query(&mut client, request.clone()).await;
    assert_eq!(trailer.basis, written.basis);
    assert!(trailer.claim.is_some());
    assert_eq!(trailer.ranked_ivf, Some(v1::RankedIvfResult { generation, approximate: false, candidate_count: 3 }));
    let hits: Vec<_> = batches.iter().flat_map(|batch| {
        let keys = batch.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        let scores = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        (0..batch.num_rows()).map(move |row| (EntityId::from_handle_bytes(keys.value(row)).unwrap(), scores.value(row)))
    }).collect();
    assert_eq!(hits, [
        (EntityId::Integer(4), 1.0 - 1.0 / 2.0_f64.sqrt()),
        (EntityId::String("3".into()), 1.0 - 3.0 / 5.0),
        (EntityId::Integer(3), 1.0 - 3.0 / 5.0),
    ]);

    let mut empty = request.clone();
    empty.ranked_ivf.as_mut().unwrap().filters[0].values = vec![b"absent".to_vec()];
    let (batches, trailer) = query(&mut client, empty).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    assert!(trailer.claim.is_some());
    assert_eq!(trailer.ranked_ivf, Some(v1::RankedIvfResult { generation, approximate: false, candidate_count: 0 }));

    let ranked = request.ranked_ivf.as_mut().unwrap();
    ranked.k = 1;
    ranked.filters.clear();
    ranked.staged.clear();
    let (batches, trailer) = query(&mut client, request).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(trailer.ranked_ivf, Some(v1::RankedIvfResult { generation, approximate: true, candidate_count: 64 }));
    running.stop().await;
}

#[tokio::test]
async fn rtdb_v1_reads_compound_index_catalog_at_the_declaration_receipt() {
    use rtdb_node::serving::NativeServingAuthority;

    let incarnation = [29; 16];
    let resources = rtdb_node::NativeResources::default().with_native_serving_authority(
        "xtdb",
        NativeServingAuthority {
            incarnation,
            checkpoints: None,
        },
    );
    let running = Running::start_with_resources(
        None, FlightConfig::default(), &[], None, Default::default(), None, Some(Arc::new(resources)),
    ).await;
    let database = running.node.catalog().database("xtdb").unwrap();
    let mut status = database.watch_status();
    tokio::time::timeout(Duration::from_secs(5), status.wait_for(|status| {
        matches!(status.role, rtdb_replication::Role::Leading(_))
    })).await.unwrap().unwrap();
    let mut client = Client::connect(&running.server, None).await;
    let declare_index = |name: &str, columns: &[&str]| v1::Op {
        op: Some(v1::op::Op::Declare(v1::Declare {
            table: "public.events".into(),
            change: Some(v1::declare::Change::Index(v1::Index {
                name: name.into(),
                kind: v1::IndexKind::Ordered as i32,
                columns: columns.iter().map(|column| (*column).into()).collect(),
                options: None,
            })),
        })),
    };
    let first = transact_wire(&mut client, v1::TransactRequest {
        key: vec![1; 16],
        ops: vec![declare_index("events_by_partition", &["partition_key", "sequence_no"])],
        ..Default::default()
    }).await.unwrap();
    let mut read = input("SELECT COUNT(*) FROM xt.ordered_indexes WHERE _id = 'events_by_partition' \
        AND schema_name = 'public' AND table_name = 'events' AND columns = ARRAY['partition_key', 'sequence_no']");
    read.source.as_mut().unwrap().at = Some(v1::At { selection: first.basis.clone().map(Selection::AtLeast) });
    let (rows, trailer) = query(&mut client, read).await;
    assert_eq!(ints(&rows), [1]);
    assert_eq!(trailer.basis, first.basis);

    use v1::{IndexKind, index::Options};
    let ranked_index = |name: &str, kind: IndexKind, columns: &[&str], options| v1::Op {
        op: Some(v1::op::Op::Declare(v1::Declare {
            table: "public.events".into(),
            change: Some(v1::declare::Change::Index(v1::Index {
                name: name.into(), kind: kind as i32,
                columns: columns.iter().map(|column| (*column).into()).collect(),
                options: Some(options),
            })),
        })),
    };
    let declarations = vec![
        ranked_index("events_text", IndexKind::Search, &["body"], Options::Search(v1::SearchIndexOptions {
            analyzer: "english_stop".into(), history: true,
        })),
        ranked_index("events_places", IndexKind::Spatial, &["geometry", "partition_key"], Options::Spatial(v1::SpatialIndexOptions {
            geometry_encoding: "tagged-wkb-v1".into(),
        })),
        ranked_index("events_vectors", IndexKind::Vector, &["embedding"], Options::Vector(v1::VectorIndexOptions {
            method: "ivf".into(), dimensions: 2, recall: 0.9,
        })),
    ];
    let ranked = transact_wire(&mut client, v1::TransactRequest {
        key: vec![20; 16], require: vec!["declare.ranked-index".into()], ops: declarations,
        ..Default::default()
    }).await.unwrap();
    assert!(matches!(ranked.outcome, Some(v1::receipt::Outcome::Then(()))));
    let mut read = input("SELECT COUNT(*) FROM xt.ranked_indexes WHERE table_name = 'events'
        AND serving_mode = 'exact-sql' AND NOT artefacts_ready AND NOT approximate AND (
        (kind = 'search' AND analyzer = 'english_stop' AND history)
        OR (kind = 'spatial' AND geometry_encoding = 'tagged-wkb-v1' AND columns = ARRAY['geometry', 'partition_key'])
        OR (kind = 'vector' AND method = 'ivf' AND dimensions = 2 AND recall > 0.8))");
    read.source.as_mut().unwrap().at = Some(v1::At { selection: ranked.basis.map(Selection::Exactly) });
    read.track = true;
    read.require_clear_rank_input = true;
    read.verify_ranked_artefacts = true;
    assert_eq!(ints(&query(&mut client, read.clone()).await.0), [3]);
    read.source.as_mut().unwrap().at = Some(v1::At { selection: first.basis.clone().map(Selection::Exactly) });
    assert_eq!(ints(&query(&mut client, read).await.0), [0]);

    let second = transact_wire(&mut client, v1::TransactRequest {
        key: vec![2; 16],
        ops: vec![declare_index("events_by_sequence", &["sequence_no", "partition_key"])],
        ..Default::default()
    }).await.unwrap();
    let mut read = input("SELECT COUNT(*) FROM xt.ordered_indexes WHERE table_name = 'events'");
    read.source.as_mut().unwrap().at = Some(v1::At { selection: second.basis.map(Selection::AtLeast) });
    assert_eq!(ints(&query(&mut client, read.clone()).await.0), [2]);
    read.source.as_mut().unwrap().at = Some(v1::At { selection: first.basis.map(Selection::Exactly) });
    assert_eq!(ints(&query(&mut client, read).await.0), [1]);

    let constraint = |change| v1::Op {
        op: Some(v1::op::Op::Declare(v1::Declare {
            table: "public.events".into(),
            change: Some(v1::declare::Change::Constraint(v1::Constraint {
                name: "event_capacity".into(), expression: String::new(), change: Some(change),
            })),
        })),
    };
    let installed = transact_wire(&mut client, v1::TransactRequest {
        key: vec![3; 16],
        require: vec!["declare.constraint.count-range".into()],
        ops: vec![constraint(v1::constraint::Change::CountRange(v1::CountRangeConstraint {
            index: "events_by_partition".into(), n: 2, ..Default::default()
        }))],
        ..Default::default()
    }).await.unwrap();
    assert!(matches!(installed.outcome, Some(v1::receipt::Outcome::Then(()))));
    let mut read = input("SELECT COUNT(*) FROM xt.count_constraints WHERE constraint_name = 'event_capacity'");
    read.source.as_mut().unwrap().at = Some(v1::At { selection: installed.basis.clone().map(Selection::AtLeast) });
    assert_eq!(ints(&query(&mut client, read.clone()).await.0), [1]);

    let put = transact_wire(&mut client, v1::TransactRequest {
        key: vec![4; 16],
        ops: vec![v1::Op { op: Some(v1::op::Op::Put(v1::Put {
            branch: "main".into(), table: "public.events".into(),
            rows: (1..=3).map(|id| v1::Row {
                cells: [("_id".into(), integer_cell(id)), ("partition_key".into(), integer_cell(1)),
                    ("sequence_no".into(), integer_cell(id))].into_iter().collect(),
                ..Default::default()
            }).collect(),
            if_absent: false,
        })) }],
        ..Default::default()
    }).await.unwrap();
    assert!(matches!(put.outcome, Some(v1::receipt::Outcome::Refused(error)) if error.code == "rtdb/count-constraint"));

    let dropped = transact_wire(&mut client, v1::TransactRequest {
        key: vec![5; 16],
        ops: vec![constraint(v1::constraint::Change::Drop(()))],
        ..Default::default()
    }).await.unwrap();
    read.source.as_mut().unwrap().at = Some(v1::At { selection: dropped.basis.map(Selection::AtLeast) });
    assert_eq!(ints(&query(&mut client, read.clone()).await.0), [0]);
    read.source.as_mut().unwrap().at = Some(v1::At { selection: installed.basis.map(Selection::Exactly) });
    assert_eq!(ints(&query(&mut client, read).await.0), [1]);
    running.stop().await;
}

#[tokio::test]
async fn rtdb_v1_serving_driver_uses_one_applied_basis_for_query_claim_write_and_changes() {
    let database = uuid::Uuid::from_bytes(FIXTURE_INCARNATION);
    let kms: Arc<dyn rtdb_crypt::Kms> = Arc::new(rtdb_crypt::LocalKms::new(|| 0));
    let key = rtdb_crypt::tokens::TokenKey::create(kms.as_ref(), rtdb_crypt::Generation { database, number: 0 }).await.unwrap();
    let tokens = rtdb_crypt::tokens::TokenAuthority::new(database, kms, Arc::new(FixtureTokenKeys(key)));
    let driver = Arc::new(AppliedFixture {
        state: std::sync::Mutex::new(rtdb_tx::transact::TransactState::new(FIXTURE_INCARNATION)),
        tokens,
        memory: MemoryBudget::new(1 << 27),
        admission: Default::default(),
        hold_first: std::sync::atomic::AtomicBool::new(true),
        waiting: Default::default(),
        release: Default::default(),
    });
    let resources = Arc::new(AppliedFixtureResources {
        native: Default::default(),
        driver: driver.clone(),
        provisioned: None,
    });
    let running = Running::start_with_resources(
        None,
        FlightConfig::default(),
        &[],
        None,
        Default::default(),
        None,
        Some(resources),
    )
    .await;
    let mut client = Client::connect(&running.server, None).await;
    let put = |branch: &str, id, reversed| {
        let mut cells = vec![
            ("_id".to_owned(), integer_cell(id)),
            ("n".to_owned(), integer_cell(id * 10)),
            ("m".to_owned(), integer_cell(id * 100)),
        ];
        if reversed {
            cells.reverse();
        }
        v1::Op {
            op: Some(v1::op::Op::Put(v1::Put {
                branch: branch.into(),
                table: "wire_applied".into(),
                if_absent: true,
                rows: vec![v1::Row {
                    cells: cells.into_iter().collect(),
                    custody_group: Vec::new(),
                    protected_columns: Vec::new(),
                }],
            })),
        }
    };
    let key = vec![1; 16];
    let mut original = v1::TransactRequest {
        key: key.clone(),
        ops: vec![put("main", 1, false)],
        ..Default::default()
    }
    .encode_to_vec();
    original.extend([0xf8, 0x07, 0x01]);
    let original: Bytes = original.into();
    let mut same_session = Client {
        client: client.client.clone(),
        headers: client.headers.clone(),
    };
    let raw = original.clone();
    let held =
        tokio::spawn(async move { same_session.action("rtdb.v1.Transact", raw).await.unwrap() });
    tokio::time::timeout(Duration::from_secs(3), driver.waiting.notified())
        .await
        .unwrap();
    assert!(driver.admission.lock().unwrap().is_none());
    let independent = tokio::time::timeout(
        Duration::from_secs(3),
        transact_wire(
            &mut client,
            v1::TransactRequest {
                key: vec![9; 16],
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(independent.outcome, Some(v1::receipt::Outcome::Then(())));
    driver.release.notify_one();
    let first = v1::Receipt::decode(held.await.unwrap()[0].body.clone()).unwrap();
    {
        let admission = driver.admission.lock().unwrap();
        let record = admission.as_ref().unwrap();
        assert_eq!(record.admission().0, key.as_slice());
        assert_eq!(record.admission().1, Timestamp(1));
        let request: serde_json::Value = serde_json::from_str(record.canonical_request().unwrap().get()).unwrap();
        assert_eq!(request["key"], serde_json::json!(key));
        assert_eq!(request["ops"][0]["Put"]["table"]["table"], "wire_applied");
        assert_eq!(record.effects().len(), 1);
    }
    let replay = transact_wire(
        &mut client,
        v1::TransactRequest {
            key: key.clone(),
            ops: vec![put("main", 1, true)],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(first.basis, replay.basis);
    assert!(replay.replayed);
    let status = client
        .action(
            "rtdb.v1.TxStatus",
            v1::TxStatusRequest { key }.encode_to_vec().into(),
        )
        .await
        .unwrap();
    assert_eq!(
        v1::Receipt::decode(status[0].body.clone()).unwrap().basis,
        first.basis
    );

    let mut read = input("SELECT n FROM wire_applied ORDER BY n");
    read.track = true;
    read.source.as_mut().unwrap().at = Some(v1::At {
        selection: first.basis.clone().map(Selection::AtLeast),
    });
    let (rows, main) = query(&mut client, read.clone()).await;
    assert_eq!(ints(&rows), [10]);
    assert_eq!(main.basis, first.basis);
    let basis =
        rtdb_types::rtdb_v1::Basis::from_bytes(&main.basis.as_ref().unwrap().value).unwrap();
    assert_eq!(basis.position(), 1);
    assert_eq!(basis.incarnation(), FIXTURE_INCARNATION);

    let validate_key = vec![2; 16];
    let validated = transact_wire(
        &mut client,
        v1::TransactRequest {
            key: validate_key.clone(),
            guards: vec![v1::Guard {
                guard: Some(v1::guard::Guard::Unchanged(main.claim.unwrap())),
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(validated.outcome, Some(v1::receipt::Outcome::Then(())));
    assert_eq!(validated.basis, main.basis);
    let absent = client
        .action(
            "rtdb.v1.TxStatus",
            v1::TxStatusRequest { key: validate_key }
                .encode_to_vec()
                .into(),
        )
        .await
        .unwrap_err();
    assert_eq!(absent.code(), tonic::Code::NotFound);

    let fork = v1::Op {
        op: Some(v1::op::Op::Fork(v1::Fork {
            child: "trial".into(),
            writable: true,
            expires_at_micros: None,
            from: Some(v1::BranchBasis {
                branch: "main".into(),
                basis: main.basis.clone(),
            }),
        })),
    };
    let second = transact_wire(
        &mut client,
        v1::TransactRequest {
            key: vec![3; 16],
            ops: vec![fork, put("trial", 2, false)],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    read.source.as_mut().unwrap().branch = "trial".into();
    assert_eq!(ints(&query(&mut client, read.clone()).await.0), [10, 20]);
    read.source.as_mut().unwrap().layer = v1::Layer::Local as i32;
    let (rows, trial) = query(&mut client, read).await;
    assert_eq!(ints(&rows), [20]);
    assert_eq!(trial.basis, second.basis);

    let mut changes = v1::ChangesRequest {
        branch: "trial".into(),
        tables: vec!["wire_applied".into()],
        limit: 1,
        images: v1::Images::Both as i32,
        ..Default::default()
    };
    let expired = get::<_, v1::ChangesTrailer>(&mut client, changes.clone())
        .await
        .unwrap_err();
    error(
        expired,
        v1::AnomalyCategory::NotFound,
        "rtdb/cursor-expired",
    );
    changes.after = first.basis;
    let (rows, trailer) = get::<_, v1::ChangesTrailer>(&mut client, changes)
        .await
        .unwrap();
    assert_eq!(trailer.through, second.basis);
    let erased = rows[0]
        .column_by_name("erased")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::BooleanArray>()
        .unwrap();
    assert!(erased.value(0));
    running.stop().await;
}

#[tokio::test]
async fn rtdb_v1_native_driver_applies_branches_and_keeps_their_receipts_at_the_checkpoint() {
    use rtdb_node::serving::NativeServingAuthority;
    use rtdb_types::rtdb_v1::Basis;

    let incarnation = [27; 16];
    let resources = rtdb_node::NativeResources::default().with_native_serving_authority("xtdb", NativeServingAuthority {
        incarnation, checkpoints: None,
    });
    let mut options = rtdb_node::NodeOptions::default();
    options.replication.rows_per_block = 2;
    let running = Running::start_with_resources(Some("secret"), FlightConfig::default(), &[], None, options, None, Some(Arc::new(resources))).await;
    let database = running.node.catalog().database("xtdb").unwrap();
    let mut status = database.watch_status();
    tokio::time::timeout(Duration::from_secs(5), status.wait_for(|status| {
        matches!(status.role, rtdb_replication::Role::Leading(_))
    })).await.unwrap().unwrap();
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    let put = |branch: &str, id| v1::Op { op: Some(v1::op::Op::Put(v1::Put {
        branch: branch.into(), table: "public.native_branch".into(), if_absent: false,
        rows: vec![v1::Row { cells: [("_id".into(), integer_cell(id)), ("n".into(), integer_cell(id * 10))].into_iter().collect(), custody_group: Vec::new(), protected_columns: Vec::new() }],
    })) };
    let first = transact_wire(&mut client, v1::TransactRequest {
        key: vec![1; 16], ops: vec![put("main", 1)], ..Default::default()
    }).await.unwrap();
    let mut read = input("SELECT n FROM native_branch ORDER BY n");
    read.track = true;
    read.observe_applied_position = true;
    read.source.as_mut().unwrap().at = Some(v1::At { selection: first.basis.clone().map(Selection::AtLeast) });
    let (rows, main) = query(&mut client, read.clone()).await;
    assert_eq!(ints(&rows), [10]);
    assert_eq!(main.basis, first.basis);
    assert_eq!(main.applied_position, Some(Basis::from_bytes(&first.basis.as_ref().unwrap().value).unwrap().position()));
    let validated = transact_wire(&mut client, v1::TransactRequest {
        key: vec![2; 16],
        guards: vec![v1::Guard { guard: Some(v1::guard::Guard::Unchanged(main.claim.unwrap())) }],
        ..Default::default()
    }).await.unwrap();
    assert_eq!(validated.basis, first.basis);
    let second_request = v1::TransactRequest {
        key: vec![3; 16],
        require: vec![rtdb_tx::transact::TIMED_TRANSACT.into()],
        window: Some(v1::Window {
            not_before_micros: None,
            before_micros: Some(running.node.now().0.saturating_add(30_000_000)),
        }),
        ops: vec![v1::Op { op: Some(v1::op::Op::Fork(v1::Fork {
            child: "trial".into(), writable: true, expires_at_micros: None,
            from: Some(v1::BranchBasis { branch: "main".into(), basis: first.basis.clone() }),
        })) }, put("trial", 2)],
        ..Default::default()
    };
    let second = transact_wire(&mut client, second_request.clone()).await.unwrap();
    assert_eq!(Basis::from_bytes(&second.basis.as_ref().unwrap().value).unwrap(), Basis::new(incarnation, 3));
    read.source.as_mut().unwrap().at = Some(v1::At { selection: second.basis.clone().map(Selection::AtLeast) });
    read.source.as_mut().unwrap().branch = "trial".into();
    assert_eq!(ints(&query(&mut client, read.clone()).await.0), [10, 20]);
    read.source.as_mut().unwrap().layer = v1::Layer::Local as i32;
    assert_eq!(ints(&query(&mut client, read).await.0), [20]);
    let (changes, coverage) = get::<_, v1::ChangesTrailer>(&mut client, v1::ChangesRequest {
        branch: "trial".into(), tables: vec!["public.native_branch".into()], limit: 1,
        after: first.basis, images: v1::Images::Both as i32, observe_applied_positions: true, ..Default::default()
    }).await.unwrap();
    assert_eq!(changes.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let change = changes.iter().find(|batch| batch.num_rows() == 1).unwrap();
    let position = change.column_by_name("applied_position").unwrap()
        .as_any().downcast_ref::<arrow_array::UInt64Array>().unwrap();
    assert_eq!(position.value(0), 3);
    assert_eq!(coverage.through, second.basis);

    tokio::time::timeout(Duration::from_secs(5), status.wait_for(|status| {
        status.adopted_block.as_ref().is_some_and(|block| block.as_proto().serving_checkpoint.is_some())
    })).await.unwrap().unwrap();
    let status_reply = client.action("rtdb.v1.TxStatus", v1::TxStatusRequest { key: vec![3; 16] }.encode_to_vec().into()).await.unwrap();
    assert_eq!(v1::Receipt::decode(status_reply[0].body.clone()).unwrap().basis, second.basis);
    let replay = transact_wire(&mut client, second_request).await.unwrap();
    assert_eq!(replay.basis, second.basis);
    assert!(replay.replayed);
    running.stop().await;
}

#[tokio::test]
async fn rtdb_v1_default_startup_fences_both_arms_and_recovers_receipts() {
    use rtdb_format::database::proto::{DatabaseConfig, LocalLog, LocalStorage, database_config::{Log, Storage}};
    use v1::{receipt::Outcome, version::Expect};

    let root = tempfile::tempdir().unwrap();
    let options = || {
        let path = |name| Some(root.path().join(name).to_string_lossy().into_owned());
        let primary = DatabaseConfig {
            log: Some(Log::LocalLog(LocalLog { path: path("log"), epoch: Some(0) })),
            storage: Some(Storage::LocalStorage(LocalStorage { path: path("storage"), epoch: Some(0) })),
            ..Default::default()
        };
        let mut options = NodeOptions {
            membership: Membership::Durable { primary, skipped: Default::default() },
            default_v1_serving: true,
            ..Default::default()
        };
        options.replication.rows_per_block = 4;
        options
    };
    let start = || {
        let options = options();
        async move {
            let running = Running::start_with_options(None, FlightConfig::default(), &[], None, options).await;
            let mut status = running.node.catalog().database("xtdb").unwrap().watch_status();
            tokio::time::timeout(Duration::from_secs(5), status.wait_for(|status| {
                matches!(status.role, rtdb_replication::Role::Leading(_))
            })).await.unwrap().unwrap();
            (running, status)
        }
    };

    let (running, mut status) = start().await;
    let mut client = Client::connect(&running.server, None).await;
    let caps = client.action("rtdb.v1.Capabilities", Bytes::new()).await.unwrap();
    let caps = v1::CapabilitiesResponse::decode(caps[0].body.clone()).unwrap();
    assert!(caps.features.iter().any(|feature| feature == "transact.requires"));

    let put = |id, n| v1::Op { op: Some(v1::op::Op::Put(v1::Put {
        branch: "main".into(), table: "public.wire_owner".into(), if_absent: false,
        rows: vec![v1::Row { cells: [("_id".into(), integer_cell(id)), ("n".into(), integer_cell(n))].into_iter().collect(), custody_group: Vec::new(), protected_columns: Vec::new() }],
    })) };
    let version = |id, expect| v1::Guard { guard: Some(v1::guard::Guard::Version(v1::Version {
        branch: "main".into(), table: "public.wire_owner".into(), id: Some(integer_cell(id)),
        valid_at_micros: None, expect: Some(expect),
    })) };
    let owner = transact_wire(&mut client, v1::TransactRequest {
        key: vec![1; 16], ops: vec![put(1, 10)], ..Default::default()
    }).await.unwrap();
    let successor = transact_wire(&mut client, v1::TransactRequest {
        key: vec![2; 16], ops: vec![put(1, 20)], ..Default::default()
    }).await.unwrap();
    assert!(caps.features.iter().any(|feature| feature == "query.written-basis"));
    let read_version = input("SELECT _rtdb_written_basis FROM wire_owner WHERE _id = 1");
    let (versions, _) = query(&mut client, read_version).await;
    let written = versions[0].column(0).as_any().downcast_ref::<arrow_array::BinaryArray>().unwrap();
    let written = v1::Basis { value: written.value(0).to_vec() };
    assert_eq!(Some(&written), successor.basis.as_ref());
    let (all, _) = query(&mut client, input("SELECT * FROM wire_owner")).await;
    assert!(all[0].schema().field_with_name("_rtdb_written_basis").is_err());

    let stale = v1::TransactRequest {
        key: vec![3; 16], require: vec!["transact.requires".into()],
        requires: vec![version(1, Expect::Written(owner.basis.unwrap()))],
        guards: vec![version(2, Expect::Present(()))],
        ops: vec![put(2, 30)], otherwise: vec![put(3, 40)], ..Default::default()
    };
    let refused = transact_wire(&mut client, stale.clone()).await.unwrap();
    let Some(Outcome::Refused(error)) = &refused.outcome else { panic!("expected refusal"); };
    assert_eq!(error.category(), v1::AnomalyCategory::Conflict);
    assert_eq!(error.code, "rtdb/requirement-failed");
    assert_eq!(error.message, "Requirement 0 failed");
    assert!(!refused.replayed);
    let read = input("SELECT n FROM wire_owner ORDER BY n");
    assert_eq!(ints(&query(&mut client, read.clone()).await.0), [20]);
    let replayed = v1::Receipt { replayed: true, ..refused };
    assert_eq!(transact_wire(&mut client, stale.clone()).await.unwrap(), replayed);
    let status = client.action(
        "rtdb.v1.TxStatus", v1::TxStatusRequest { key: stale.key.clone() }.encode_to_vec().into(),
    ).await.unwrap();
    assert_eq!(v1::Receipt::decode(status[0].body.clone()).unwrap(), replayed);

    let selected = transact_wire(&mut client, v1::TransactRequest {
        key: vec![4; 16], requires: vec![version(1, Expect::Written(written))], ..stale.clone()
    }).await.unwrap();
    assert_eq!(selected.outcome, Some(Outcome::Otherwise(v1::Otherwise { failed: vec![0] })));
    assert_eq!(ints(&query(&mut client, read.clone()).await.0), [20, 40]);
    tokio::time::timeout(Duration::from_secs(5), status.wait_for(|status| {
        status.adopted_block.as_ref().is_some_and(|block| block.as_proto().serving_checkpoint.is_some())
    })).await.unwrap().unwrap();
    running.stop().await;

    let (running, _) = start().await;
    let mut client = Client::connect(&running.server, None).await;
    let status_request = v1::TxStatusRequest { key: stale.key.clone() }.encode_to_vec();
    let status = client.action("rtdb.v1.TxStatus", status_request.clone().into()).await.unwrap();
    assert_eq!(v1::Receipt::decode(status[0].body.clone()).unwrap(), replayed);
    assert_eq!(transact_wire(&mut client, stale).await.unwrap(), replayed);
    let mut read = read;
    read.source.as_mut().unwrap().at = Some(v1::At { selection: selected.basis.clone().map(Selection::Exactly) });
    let (rows, reopened) = query(&mut client, read).await;
    assert_eq!(ints(&rows), [20, 40]);
    assert_eq!(reopened.basis, selected.basis);

    client.update("INSERT INTO classic_only (_id, n) VALUES (1, 50)", None).await;
    assert_eq!(ints(&client.query("SELECT n FROM classic_only FOR ALL VALID_TIME").await), [50]);
    let caps = client.action("rtdb.v1.Capabilities", Bytes::new()).await.unwrap();
    assert!(v1::CapabilitiesResponse::decode(caps[0].body.clone()).unwrap().features.is_empty());
    let status = client.action("rtdb.v1.TxStatus", status_request.into()).await.unwrap_err();
    error(status, v1::AnomalyCategory::Unsupported, "rtdb/default-serving-classic-history");
    running.stop().await;
}

struct CustodyMountObservations(std::sync::OnceLock<rtdb_node::serving::custody_release::OriginalReceipt>);

#[async_trait]
impl rtdb_node::serving::custody_release::OriginalCarrierObservations for CustodyMountObservations {
    async fn confirmed_writer(
        &self, _: rtdb_node::serving::custody_release::CustodyCarrierAuthority, _: &tokio_util::sync::CancellationToken,
    ) -> Result<rtdb_node::serving::custody_release::OriginalReceipt> {
        self.0.get().cloned().ok_or_else(|| rtdb_types::Anomaly::unsupported("test/no-writer", "The fixture writer is not confirmed"))
    }

    async fn registered_response(
        &self, _: rtdb_node::serving::custody_release::CustodyCarrierAuthority, _: rtdb_node::serving::custody_release::CustodyResponsePart,
        _: &tokio_util::sync::CancellationToken,
    ) -> Result<rtdb_node::serving::custody_release::RegisteredCustodyResponse> {
        Err(rtdb_types::Anomaly::unsupported("test/no-response", "This fixture only mounts the original writer"))
    }

    async fn registered_carrier(
        &self, _: rtdb_node::serving::custody_release::CustodyReleaseRegistration, _: &tokio_util::sync::CancellationToken,
    ) -> Result<rtdb_node::serving::custody_release::OriginalCarrierReceipt> {
        Err(rtdb_types::Anomaly::unsupported("test/no-carrier", "This fixture only mounts the original writer"))
    }
}

#[tokio::test]
async fn native_original_custody_mount_rechecks_the_writer_and_withdraws_old_owners() {
    use rtdb_crypt::{Generation, Kms, LocalKms};
    use rtdb_node::serving::{NativeServingAuthority, QuerySource, custody_release::{
        CustodyCarrierAuthority, CustodyProviderInstallation, OriginalReceipt, OriginalWriterPoint, original_writer_commitment,
    }};
    use rtdb_types::rtdb_v1::{At, Basis, Layer};
    use tokio_util::sync::CancellationToken;

    let incarnation = [74; 16];
    let kms_time = Arc::new(AtomicI64::new(0));
    let kms: Arc<dyn Kms> = Arc::new(LocalKms::new({
        let time = Arc::clone(&kms_time);
        move || time.load(Ordering::SeqCst)
    }));
    let selected = Generation { database: uuid::Uuid::from_bytes(incarnation), number: 1 };
    let successor = Generation { number: 2, ..selected };
    kms.create(selected).await.unwrap();
    kms.create(successor).await.unwrap();
    let resources = rtdb_node::NativeResources::default().with_native_serving_authority(
        "xtdb", NativeServingAuthority { incarnation, checkpoints: Some(rtdb_replication::NativeCheckpointAuthority {
            kms: Arc::clone(&kms), generation: successor.number,
        }) },
    );
    let running = Running::start_with_resources(
        None, FlightConfig::default(), &[], None, Default::default(), None, Some(Arc::new(resources)),
    ).await;
    let catalog = running.node.catalog();
    let database = catalog.database("xtdb").unwrap();
    let mut status = database.watch_status();
    tokio::time::timeout(Duration::from_secs(5), status.wait_for(|status| {
        matches!(status.role, rtdb_replication::Role::Leading(_))
    })).await.unwrap().unwrap();
    let mut client = Client::connect(&running.server, None).await;
    let receipt = transact_wire(&mut client, v1::TransactRequest {
        key: vec![76; 16],
        ops: vec![v1::Op { op: Some(v1::op::Op::Put(v1::Put {
            branch: "main".into(), table: "public.installation".into(), if_absent: true,
            rows: vec![v1::Row { cells: [("_id".into(), integer_cell(1)), ("writer".into(), integer_cell(1))].into(), ..Default::default() }],
        })) }],
        ..Default::default()
    }).await.unwrap();
    let observed = OriginalReceipt {
        key: receipt.key,
        basis: Basis::from_bytes(&receipt.basis.unwrap().value).unwrap(),
        system_time: Timestamp(receipt.system_time_micros),
    };
    let installation = |proof: OriginalReceipt| CustodyProviderInstallation {
        authority: CustodyCarrierAuthority {
            incarnation, source_history: [77; 32], installation: [78; 16], registry: [79; 32],
            original_writer: original_writer_commitment([78; 16], &observed.key),
        },
        writer: OriginalWriterPoint { table: TableRef::new("public", "installation"), id: EntityId::Integer(1), valid_at: observed.system_time },
        observations: Arc::new(CustodyMountObservations(proof.into())),
    };
    let cancellation = CancellationToken::new();
    let wrong = OriginalReceipt { system_time: observed.system_time.next().unwrap(), ..observed.clone() };
    assert!(catalog.mount_original_custody_provider("xtdb", installation(wrong), &cancellation).await.is_err());
    assert!(catalog.original_custody_response_owner("xtdb").is_err());
    let mount = catalog.mount_original_custody_provider("xtdb", installation(observed.clone()), &cancellation).await.unwrap();
    assert!(catalog.mount_original_custody_provider("xtdb", installation(observed.clone()), &cancellation).await.is_err());
    let driver = database.serving_driver().unwrap();
    assert!(!driver.features().contains(&rtdb_tx::transact::ROW_CUSTODY));
    assert!(driver.check_read_delivery(None, &[[1; 16]].into(), &cancellation).await.is_err());
    let writer = rtdb_tx::transact::CustodyWriterFence {
        table: TableRef::new("public", "installation"), id: EntityId::Integer(1), valid_at: observed.system_time, written: observed.basis,
    };
    driver.check_custody_release_delivery(&Default::default(), &writer, Some(selected.number), &cancellation).await.unwrap();
    kms.retire(selected, 10).await.unwrap();
    kms_time.store(10, Ordering::SeqCst);
    assert_eq!(driver.check_custody_release_delivery(&Default::default(), &writer, Some(selected.number), &cancellation)
        .await.unwrap_err().code, "rtdb/erased-under-view");
    driver.check_custody_release_delivery(&Default::default(), &writer, Some(successor.number), &cancellation).await.unwrap();
    let owner = catalog.original_custody_response_owner("xtdb").unwrap();
    let release = owner.response_release(&rtdb_node::SystemClock, &cancellation).unwrap();
    let source = || QuerySource { branch: "main".into(), at: At::Linearizable, layer: Layer::Merged };
    let captured = release.capture_ranked(source()).await.unwrap();
    assert!(captured.view().custody_contributors.as_ref().unwrap().is_empty());
    drop(mount);
    assert!(release.capture_ranked(source()).await.is_err());
    assert!(catalog.original_custody_response_owner("xtdb").is_err());
    let replacement = catalog.mount_original_custody_provider("xtdb", installation(observed.clone()), &cancellation).await.unwrap();
    assert!(owner.response_release(&rtdb_node::SystemClock, &cancellation).is_err());
    assert!(catalog.original_custody_response_owner("xtdb").is_ok());
    drop(replacement);
    running.stop().await;
}

#[tokio::test]
async fn configured_custody_query_mounts_only_for_the_credentialed_owner() {
    use rtdb_node::serving::{NativeServingAuthority, custody_release::{
        CustodyCarrierAuthority, CustodyProviderInstallation, OriginalReceipt, OriginalWriterPoint, original_writer_commitment,
    }};
    use rtdb_types::rtdb_v1::Basis;

    let incarnation = [84; 16];
    let key = vec![85; 16];
    let observations = Arc::new(CustodyMountObservations(Default::default()));
    let provider = Arc::new(rtdb_flight_sql::CustodyProviderRoute::new(CustodyProviderInstallation {
        authority: CustodyCarrierAuthority {
            incarnation, source_history: [86; 32], installation: [87; 16], registry: [88; 32],
            original_writer: original_writer_commitment([87; 16], &key),
        },
        writer: OriginalWriterPoint { table: TableRef::new("public", "installation"), id: EntityId::Integer(1), valid_at: Timestamp(0) },
        observations: observations.clone(),
    }).with_owner("xtdb".into()));
    let config = || FlightConfig {
        custody_providers: [("xtdb".into(), provider.clone())].into(),
        ..Default::default()
    };
    let resources = || Arc::new(rtdb_node::NativeResources::default().with_native_serving_authority(
        "xtdb", NativeServingAuthority { incarnation, checkpoints: None },
    ));
    let mut evidence_query = input("SELECT 42 AS n");
    evidence_query.custody_release_evidence = true;

    let trusted = Running::start_with_resources(None, config(), &[], None, Default::default(), None, Some(resources())).await;
    let mut client = Client::connect(&trusted.server, Some("xtdb:secret")).await;
    let denied = get::<_, v1::QueryTrailer>(&mut client, evidence_query.clone()).await.unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    assert!(trusted.node.catalog().original_custody_response_owner("xtdb").is_err());
    trusted.stop().await;

    let running = Running::start_with_resources(Some("secret"), config(), &[], None, Default::default(), None, Some(resources())).await;
    let catalog = running.node.catalog();
    let mut status = catalog.database("xtdb").unwrap().watch_status();
    tokio::time::timeout(Duration::from_secs(5), status.wait_for(|status| {
        matches!(status.role, rtdb_replication::Role::Leading(_))
    })).await.unwrap().unwrap();
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    let receipt = transact_wire(&mut client, v1::TransactRequest {
        key,
        ops: vec![v1::Op { op: Some(v1::op::Op::Put(v1::Put {
            branch: "main".into(), table: "public.installation".into(), if_absent: true,
            rows: vec![v1::Row { cells: [
                ("_id".into(), integer_cell(1)), ("_valid_from".into(), integer_cell(0)),
                ("writer".into(), integer_cell(1)),
            ].into(), ..Default::default() }],
        })) }],
        ..Default::default()
    }).await.unwrap();
    assert!(observations.0.set(OriginalReceipt {
        key: receipt.key,
        basis: Basis::from_bytes(&receipt.basis.unwrap().value).unwrap(),
        system_time: Timestamp(receipt.system_time_micros),
    }).is_ok());
    let (_, ordinary) = query(&mut client, input("SELECT 42 AS n")).await;
    assert!(ordinary.custody_release_proof.is_none());
    assert!(catalog.original_custody_response_owner("xtdb").is_err());
    let (_, protected) = query(&mut client, evidence_query).await;
    assert!(protected.custody_release_proof.is_some_and(|proof| !proof.is_empty()));
    assert!(catalog.original_custody_response_owner("xtdb").is_ok());
    let capabilities = client.action("rtdb.v1.Capabilities", Bytes::new()).await.unwrap();
    let capabilities = v1::CapabilitiesResponse::decode(capabilities[0].body.clone()).unwrap();
    assert!(!capabilities.features.iter().any(|feature| feature == rtdb_tx::transact::ROW_CUSTODY));
    running.stop().await;
}
