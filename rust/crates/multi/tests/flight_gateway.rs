#![allow(dead_code)]
mod common;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use arrow_flight::{FlightDescriptor, HandshakeRequest, Ticket, flight_service_client::FlightServiceClient,
    sql::{CommandStatementQuery, ProstMessageExt, client::FlightSqlServiceClient}};
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
