use super::*;
use arrow_flight::{FlightData, Ticket};
use prost::Name;
use rtdb_flight_sql::rtdb::v1::{self as v1, at::Selection};

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
    let command = Any {
        type_url: M::type_url(),
        value: message.encode_to_vec().into(),
    };
    let request = client.request(Ticket {
        ticket: command.encode_to_vec().into(),
    });
    let mut frames: Vec<FlightData> = client
        .client
        .do_get(request)
        .await?
        .into_inner()
        .try_collect()
        .await?;
    let last = frames.pop().expect("typed trailer");
    assert!(last.data_header.is_empty() && last.data_body.is_empty());
    let trailer = R::decode(last.app_metadata).expect("protobuf stream trailer");
    let schema = arrow_ipc::root_as_message(&frames.first().expect("Arrow schema").data_header)
        .unwrap();
    assert!(schema.header_as_schema().is_some());
    let stream = futures::stream::iter(frames.into_iter().map(Ok));
    let batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(stream)
        .try_collect()
        .await
        .unwrap();
    Ok((batches, trailer))
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
    assert!(second.claim.is_some());
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
        .update("INSERT INTO wire_a (_id, n) VALUES (1, 10), (2, 20)", Some(tx.clone()))
        .await;
    client
        .update("INSERT INTO wire_z (_id, n) VALUES (1, 30)", Some(tx.clone()))
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
        let (rows, images) = get::<_, v1::ChangesTrailer>(&mut client, request.clone())
            .await
            .unwrap();
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
                let key = StreamReader::try_new(keys.value(row), None).unwrap().next().unwrap().unwrap();
                let after = StreamReader::try_new(after.value(row), None).unwrap().next().unwrap().unwrap();
                assert_eq!(key.num_rows(), 1);
                assert_eq!(after.num_rows(), 1);
                assert_eq!(key.column(0).as_ref(), after.column_by_name("_id").unwrap().as_ref());
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

        request.images = v1::Images::None as i32;
        let (_, ordinary) = get::<_, v1::ChangesTrailer>(&mut client, request.clone())
            .await
            .unwrap();
        assert_eq!(ordinary, images);
        request.progress_only = true;
        let (rows, progress) = get::<_, v1::ChangesTrailer>(&mut client, request.clone())
            .await
            .unwrap();
        assert!(rows.is_empty());
        assert_eq!(progress, ordinary);

        request.images = v1::Images::Keys as i32;
        let status = get::<_, v1::ChangesTrailer>(&mut client, request).await.unwrap_err();
        error(status, v1::AnomalyCategory::Incorrect, "rtdb/invalid-request");
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
    let (rows, timeout) = get::<_, v1::ChangesTrailer>(&mut client, request).await.unwrap();
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
