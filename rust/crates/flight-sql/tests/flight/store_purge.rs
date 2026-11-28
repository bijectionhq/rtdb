use super::*;
use rtdb_flight_sql::rtdb::store::v1::{KeyRange, PurgeFloorRequest, WriterFence};
use rtdb_node::serving::NativeServingAuthority;
use tonic::Code;

const ACTION: &str = "rtdb.store.v1.PurgeFloor";
const INCARNATION: [u8; 16] = [51; 16];

fn resources() -> FixtureResources {
    Box::new(|clock| Arc::new(fixture_resources(clock).with_native_serving_authority("xtdb",
        NativeServingAuthority { incarnation: INCARNATION, checkpoints: None })))
}

fn request(incarnation: [u8; 16]) -> Bytes {
    PurgeFloorRequest {
        incarnation: incarnation.to_vec(), since: Some(0), keep_from: Some(0),
        reads: vec![KeyRange { start: vec![1], end: None }],
        writer_fence: Some(WriterFence { epoch: 1, writer: vec![2; 16], generation: 3, claim: vec![4; 16] }),
        retention_micros: Some(0),
    }.encode_to_vec().into()
}

#[tokio::test]
async fn trust_only_flight_cannot_purge_store_history() {
    let running = Running::start_with_clocked_resources(
        None, FlightConfig::default(), &[], None, Default::default(), None, Some(resources()),
    ).await;
    let mut client = Client::connect(&running.server, Some("xtdb:unverified")).await;
    let refusal = client.action(ACTION, request(INCARNATION)).await.unwrap_err();
    assert_eq!(refusal.code(), Code::PermissionDenied);
    running.stop().await;
}

#[tokio::test]
async fn the_default_store_refuses_a_purge_and_a_foreign_incarnation() {
    let config = FlightConfig { store_only: true, ..Default::default() };
    let running = Running::start_with_clocked_resources(
        Some("secret"), config, &[], None, Default::default(), None, Some(resources()),
    ).await;
    let mut client = Client::connect(&running.server, Some("xtdb:secret")).await;
    let refusal = client.action(ACTION, request([0; 16])).await.unwrap_err();
    assert_eq!(refusal.code(), Code::FailedPrecondition);
    let refusal = client.action(ACTION, request(INCARNATION)).await.unwrap_err();
    assert_eq!(refusal.code(), Code::Unimplemented);

    let request = client.request(arrow_flight::Empty {});
    let actions: Vec<_> = client.client.list_actions(request).await.unwrap().into_inner().try_collect().await.unwrap();
    assert!(!actions.iter().any(|action| action.r#type == ACTION));
    running.stop().await;
}
