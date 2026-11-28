#![allow(dead_code)]
mod common;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use arrow_flight::{Action, FlightDescriptor, HandshakeRequest, Ticket, flight_service_client::FlightServiceClient,
    sql::{CommandStatementQuery, ProstMessageExt, client::FlightSqlServiceClient}};
use base64::Engine as _;
use futures::TryStreamExt;
use prost::Message;
use rtdb_multi::{admin, flight_gateway, gateway::GatewayOptions};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, transport::Channel};

use common::{Cell, control, secret};

async fn channel(address: SocketAddr) -> Channel {
    Channel::from_shared(format!("http://{address}")).unwrap().connect().await.unwrap()
}

// Owned reads plan SQL on a runtime worker, so the test gives its workers the stack the server binaries configure.
#[test]
fn applied_owned_read_pins_the_catalog_without_reserving_or_delegating_a_present() {
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).thread_stack_size(rtdb_node::WORKER_STACK_BYTES)
        .enable_all().build().unwrap().block_on(check_applied_owned_read_pins_the_catalog());
}

async fn check_applied_owned_read_pins_the_catalog() {
    let cell = Cell::start(1, None).await;
    let id = uuid::Uuid::new_v4();
    let deployment = admin::install_deployment(cell.directory.as_ref(), id, "shop", "acme", 3).await.unwrap();
    let pgwire = cell.connect("shop").await;
    pgwire.simple_query("INSERT INTO orders (_id, total) VALUES (1, 77)").await.unwrap();
    pgwire.simple_query("SELECT total FROM orders").await.unwrap();
    let node = cell.node(cell.holder("shop").await.unwrap()).server.node();
    let prior = node.latest_completed_transactions().unwrap()["shop"][0].unwrap();
    pgwire.simple_query("INSERT INTO orders (_id, total) VALUES (2, 89)").await.unwrap();
    pgwire.simple_query("SELECT total FROM orders").await.unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let cancellation = CancellationToken::new();
    let options = GatewayOptions { capabilities: Some(secret()), ..Default::default() };
    let driver = tokio::spawn(flight_gateway::serve(listener, Arc::clone(&cell.directory), options, cancellation.clone()));
    let mut flight = FlightSqlServiceClient::new(channel(address).await);
    let selector = format!("shop@{}", id.simple());
    flight.set_header("x-xtdb-database", &selector);
    flight.handshake("xtdb", "cell-test").await.unwrap();
    let route = flight.token().unwrap();
    let action = |name: &str, body: Vec<u8>| {
        let mut request = Request::new(Action { r#type: name.to_owned(), body: body.into() });
        request.metadata_mut().insert("authorization", format!("Bearer {route}").parse().unwrap());
        request.metadata_mut().insert("x-xtdb-database", selector.parse().unwrap());
        request
    };

    let floor = node.latest_completed_transactions().unwrap()["shop"][0].unwrap();
    let request = |history: uuid::Uuid, tx_offset: i64, ordered_at: i64, prior_at: i64| {
        let mut body = history.as_bytes().to_vec();
        body.push(2);
        for (offset, time) in [(tx_offset, ordered_at), (prior.tx_id.0, prior_at)] {
            body.extend_from_slice(&offset.to_be_bytes());
            body.extend_from_slice(&time.to_be_bytes());
        }
        body.extend_from_slice(&0u64.to_be_bytes());
        body
    };
    let mut raw = FlightServiceClient::new(channel(address).await);
    for _ in 0..2 {
        let before = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
        let body = request(id, floor.tx_id.0, floor.system_time.0, prior.system_time.0);
        let mut results = raw.do_action(action("rtdb.owned.OpenAppliedRead.v5", body))
            .await.unwrap().into_inner();
        let result = results.message().await.unwrap().unwrap();
        assert!(results.message().await.unwrap().is_none());
        assert_eq!(result.body.len(), 40);
        assert_eq!(&result.body[16..24], &floor.system_time.0.to_be_bytes());
        assert_eq!(&result.body[24..32], &floor.tx_id.0.to_be_bytes());
        let pinned = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
        assert_eq!(pinned.catalog_readers, before.catalog_readers, "an applied read registers nothing in the directory");

        let mut scoped = result.body[..16].to_vec();
        scoped.extend_from_slice(&1u16.to_be_bytes());
        scoped.extend_from_slice(&6u16.to_be_bytes());
        scoped.extend_from_slice(b"orders");
        let error = match raw.do_action(action("rtdb.owned.OpenOwnedScopedRead.v2", scoped)).await {
            Ok(response) => response.into_inner().message().await.unwrap_err(),
            Err(error) => error,
        };
        assert_eq!(error.code(), Code::PermissionDenied);

        let mut closed = raw.do_action(action("rtdb.owned.CloseRead.v1", result.body[..16].to_vec()))
            .await.unwrap().into_inner();
        assert!(closed.message().await.unwrap().is_none());
    }
    assert_eq!(node.latest_completed_transactions().unwrap()["shop"][0], Some(floor));

    for body in [
        request(id, floor.tx_id.0 + 1, floor.system_time.0 + 1, prior.system_time.0),
        request(id, floor.tx_id.0, floor.system_time.0, prior.system_time.0 - 1),
        request(uuid::Uuid::new_v4(), floor.tx_id.0, floor.system_time.0, prior.system_time.0),
    ] {
        let error = match raw.do_action(action("rtdb.owned.OpenAppliedRead.v5", body)).await {
            Ok(response) => response.into_inner().message().await.unwrap_err(),
            Err(error) => error,
        };
        assert_eq!(error.code(), Code::Unavailable);
    }

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), driver).await.unwrap().unwrap().unwrap();
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flight_gateway_routes_authenticated_streams_and_refuses_a_displaced_session() {
    let cell = Cell::start(2, None).await;
    let deployment = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let installation = uuid::Uuid::parse_str(&deployment.id).unwrap().simple();
    let selector = format!("shop@{installation}");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let cancellation = CancellationToken::new();
    let options = GatewayOptions { capabilities: Some(secret()), ..Default::default() };
    let driver = tokio::spawn(flight_gateway::serve(listener, Arc::clone(&cell.directory), options, cancellation.clone()));

    let mut flight = FlightSqlServiceClient::new(channel(address).await);
    flight.set_header("x-xtdb-database", &selector);
    flight.handshake("xtdb", "cell-test").await.unwrap();
    let route = flight.token().unwrap().clone();

    flight.execute_update("INSERT INTO orders (_id, total) VALUES (1, 77)".into(), None).await.unwrap();
    let info = flight.execute("SELECT CAST(total AS BIGINT) AS total FROM orders".into(), None).await.unwrap();
    let ticket = info.endpoint.into_iter().next().unwrap().ticket.unwrap();
    let rows: Vec<_> = flight.do_get(ticket.clone()).await.unwrap().try_collect().await.unwrap();
    assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
    let batch = &rows[0];
    assert_eq!(rtdb_columnar::read_value(batch.schema().field(0), batch.column(0).as_ref(), 0).unwrap(), rtdb_types::Value::I64(77));

    let request = |ticket: Ticket, token: &str, selector: Option<&str>| {
        let mut request = Request::new(ticket);
        request.metadata_mut().insert("authorization", format!("Bearer {token}").parse().unwrap());
        if let Some(selector) = selector { request.metadata_mut().insert("x-xtdb-database", selector.parse().unwrap()); }
        request
    };
    let mut raw = FlightServiceClient::new(channel(address).await);
    assert!(raw.do_get(request(ticket.clone(), &route, None)).await.is_ok());
    assert_eq!(raw.do_get(request(ticket.clone(), "invalid", None)).await.unwrap_err().code(), Code::Unauthenticated);
    assert_eq!(raw.do_get(request(ticket.clone(), &route, Some("another"))).await.unwrap_err().code(), Code::PermissionDenied);

    let startup = |store: bool| {
        let mut request = Request::new(futures::stream::iter([HandshakeRequest::default()]));
        let credentials = base64::engine::general_purpose::STANDARD.encode("xtdb:cell-test");
        request.metadata_mut().insert("authorization", format!("Basic {credentials}").parse().unwrap());
        request.metadata_mut().insert("x-xtdb-database", selector.parse().unwrap());
        request.metadata_mut().insert("x-rtdb-request-store-drain", "1".parse().unwrap());
        if store {
            request.metadata_mut().insert(rtdb_flight_sql::STORE_RELEASE_HEADER, rtdb_flight_sql::LINKED_STORE_RELEASE.parse().unwrap());
        }
        request
    };
    let ordinary = raw.handshake(startup(false)).await.unwrap();
    assert!(!ordinary.metadata().contains_key("x-rtdb-store-drain-ticket"));
    let _: Vec<_> = ordinary.into_inner().try_collect().await.unwrap();
    let enrolled = raw.handshake(startup(true)).await.unwrap();
    assert!(!enrolled.metadata().contains_key(rtdb_flight_sql::STORE_DRAIN_PROOF_HEADER));
    let drain = enrolled.metadata().get("x-rtdb-store-drain-ticket").unwrap().clone();
    let _: Vec<_> = enrolled.into_inner().try_collect().await.unwrap();

    let holder = cell.holder("shop").await.unwrap();
    let target = 1 - holder;
    let holder_server = cell.node(holder).server.addresses().flight_sql.unwrap();
    let mut private = FlightServiceClient::new(channel(holder_server).await);
    let mut proof = Request::new(futures::stream::iter([HandshakeRequest::default()]));
    proof.metadata_mut().insert("x-rtdb-flight-peer", "untrusted".parse().unwrap());
    proof.metadata_mut().insert("x-xtdb-database", "shop".parse().unwrap());
    assert_eq!(private.handshake(proof).await.unwrap_err().code(), Code::Unauthenticated);

    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(target).lease.id(), &control()).await.unwrap();
    assert_eq!(raw.do_get(request(ticket, &route, None)).await.unwrap_err().code(), Code::Unavailable);

    let mut close = Request::new(Action { r#type: "CloseSession".to_owned(), body: Default::default() });
    close.metadata_mut().insert("x-rtdb-store-drain-ticket", drain);
    let confirmation = raw.do_action(close).await.unwrap();
    assert!(!confirmation.metadata().contains_key("authorization"));
    assert!(!confirmation.metadata().contains_key("set-cookie"));
    assert!(!confirmation.metadata().contains_key(rtdb_flight_sql::STORE_DRAIN_PROOF_HEADER));
    let mut results = confirmation.into_inner();
    let result = results.message().await.unwrap().unwrap();
    #[derive(Message)]
    struct CloseResult {
        #[prost(int32, tag = "1")]
        status: i32,
        #[prost(bool, tag = "2")]
        store_reads_drained: bool,
        #[prost(bytes, tag = "3")]
        store_watch_drain: Vec<u8>,
    }
    let closed = CloseResult::decode(result.body).unwrap();
    assert_eq!(closed.status, 1);
    assert!(closed.store_reads_drained);
    assert_eq!(closed.store_watch_drain.len(), 16 + 64);
    assert!(results.message().await.unwrap().is_none());

    let mut replacement = FlightSqlServiceClient::new(channel(address).await);
    replacement.set_header("x-xtdb-database", &selector);
    replacement.handshake("xtdb", "cell-test").await.unwrap();
    let info = replacement.execute("SELECT CAST(total AS BIGINT) AS total FROM orders".into(), None).await.unwrap();
    let all: Vec<_> = replacement.do_get(info.endpoint.into_iter().next().unwrap().ticket.unwrap()).await.unwrap().try_collect().await.unwrap();
    assert_eq!(all.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
    let batch = &all[0];
    assert_eq!(rtdb_columnar::read_value(batch.schema().field(0), batch.column(0).as_ref(), 0).unwrap(), rtdb_types::Value::I64(77));

    let mut wrong_installation = Request::new(FlightDescriptor::new_cmd(CommandStatementQuery {
        query: "SELECT 1".into(), transaction_id: None,
    }.as_any().encode_to_vec()));
    wrong_installation.metadata_mut().insert("x-xtdb-database", format!("shop@{}", uuid::Uuid::new_v4().simple()).parse().unwrap());
    assert_eq!(raw.get_flight_info(wrong_installation).await.unwrap_err().code(), Code::NotFound);

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), driver).await.unwrap().unwrap().unwrap();
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_streaming_flight_call_keeps_the_attachment_active_until_cancelled() {
    let idle_after = Duration::from_secs(1);
    let cell = Cell::start_with_resident_idle(1, idle_after, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let cancellation = CancellationToken::new();
    let options = GatewayOptions { capabilities: Some(secret()), ..Default::default() };
    let driver = tokio::spawn(flight_gateway::serve(listener, Arc::clone(&cell.directory), options, cancellation.clone()));
    let mut flight = FlightSqlServiceClient::new(channel(address).await);
    flight.set_header("x-xtdb-database", "shop");
    flight.handshake("xtdb", "cell-test").await.unwrap();
    let before = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();

    let (started, sent) = tokio::sync::oneshot::channel();
    let mut started = Some(started);
    let stream = futures::stream::poll_fn(move |_| {
        if let Some(started) = started.take() { let _ = started.send(()); }
        std::task::Poll::<Option<arrow_flight::FlightData>>::Pending
    });
    let mut request = Request::new(stream);
    request.metadata_mut().insert("authorization", format!("Bearer {}", flight.token().unwrap()).parse().unwrap());
    let mut client = FlightServiceClient::new(channel(address).await);
    let upload = tokio::spawn(async move { client.do_put(request).await });
    tokio::time::timeout(Duration::from_secs(2), sent).await.unwrap().unwrap();
    tokio::time::sleep(idle_after + Duration::from_millis(600)).await;
    assert!(!upload.is_finished());
    let still_serving = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert_eq!(still_serving.epoch, before.epoch);
    assert!(!still_serving.is_resident_idle());
    upload.abort();
    assert!(upload.await.unwrap_err().is_cancelled());

    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            if cell.directory.deployment_by_name("shop").await.unwrap().unwrap().is_resident_idle() { break; }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("cancelled Flight streaming released the activity lease");
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), driver).await.unwrap().unwrap().unwrap();
    cell.stop().await;
}
