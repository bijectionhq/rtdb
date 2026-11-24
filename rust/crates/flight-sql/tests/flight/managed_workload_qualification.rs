use std::net::SocketAddrV4;
use std::path::{Path, PathBuf};

use base64::{Engine, engine::general_purpose::STANDARD};
use rtdb_flight_sql::rtdb::admin::v1::{CreateDeploymentRequest, Deployment};
use rtdb_types::rtdb_v1::Basis;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tonic::{
    Code,
    metadata::MetadataValue,
    transport::{Certificate, ClientTlsConfig},
};
use zeroize::Zeroizing;

use super::*;

const CREATE: &str = "rtdb.admin.v1.CreateDeployment";
const ADVANCE: &str = "rtdb.v1.ManagedWorkload";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    flight_address: SocketAddrV4,
    flight_ca: PathBuf,
    create_credential: PathBuf,
    assignment_credential: PathBuf,
    frozen_advance_body: PathBuf,
}

#[derive(Deserialize)]
struct Credential {
    client_id: String,
    client_secret: String,
    sub: String,
    issuer: Option<String>,
    rtdb_workload_grant: Grant,
}

#[derive(Deserialize)]
struct Grant {
    version: u32,
    audience: String,
    database: String,
    incarnation: [u8; 16],
    grant_id: [u8; 16],
}

impl Credential {
    fn read(path: &Path) -> Self {
        let bytes = Zeroizing::new(std::fs::read(path).expect("read private credential"));
        serde_json::from_slice(&bytes).unwrap_or_else(|_| panic!("invalid private credential"))
    }

    async fn handshake(&self, channel: Channel) -> Client {
        let raw = Zeroizing::new(format!("{}:{}", self.client_id, self.client_secret));
        let header = Zeroizing::new(format!("Client {}", STANDARD.encode(raw.as_bytes())));
        let mut authorization: MetadataValue<_> = header.parse().expect("Client authorization");
        authorization.set_sensitive(true);
        let mut client = Client {
            client: FlightServiceClient::new(channel),
            headers: MetadataMap::new(),
        };
        client.headers.insert("authorization", authorization);
        client.headers.insert(
            "x-xtdb-database",
            self.rtdb_workload_grant
                .database
                .parse()
                .expect("grant database"),
        );
        let request = client.request(futures::stream::iter([
            arrow_flight::HandshakeRequest::default(),
        ]));
        let response = client
            .client
            .handshake(request)
            .await
            .expect("exact workload Client handshake");
        let mut authorization = response
            .metadata()
            .get("authorization")
            .expect("Flight session authorization")
            .clone();
        authorization.set_sensitive(true);
        client.headers.insert("authorization", authorization);
        let _: Vec<_> = response
            .into_inner()
            .try_collect()
            .await
            .expect("complete Flight handshake");
        client
    }

    fn create_body(&self) -> Bytes {
        CreateDeploymentRequest {
            deployment: self.rtdb_workload_grant.database.clone(),
            incarnation: Some(self.rtdb_workload_grant.incarnation.to_vec()),
        }
        .encode_to_vec()
        .into()
    }

    fn receipt_namespace(&self) -> [u8; 32] {
        let grant = &self.rtdb_workload_grant;
        let mut hash = Sha256::new();
        hash.update(b"rtdb.v1/workload-receipt\0");
        hash.update(grant.incarnation);
        hash.update(grant.grant_id);
        for identity in [
            self.issuer.as_deref().expect("assignment issuer"),
            &grant.audience,
            &self.sub,
            &grant.database,
        ] {
            hash.update((identity.len() as u64).to_be_bytes());
            hash.update(identity.as_bytes());
        }
        hash.finalize().into()
    }
}

async fn expect_role_refusal(client: &mut Client, action: &str, body: Bytes) {
    let status = client
        .action(action, body)
        .await
        .err()
        .expect("cross-role action must refuse");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(
        status.message(),
        "The credential requires its exact managed workload route"
    );
}

#[tokio::test]
#[ignore = "requires an explicitly prepared loopback Multi fixture and live exact issuer credentials"]
async fn combined_local_workload_oidc_exact_roles() {
    let manifest = PathBuf::from(
        std::env::var_os("RTDB_COMBINED_WORKLOAD_FIXTURE").expect("qualification fixture manifest"),
    );
    let root = manifest.parent().expect("fixture directory");
    let fixture: Fixture =
        serde_json::from_slice(&std::fs::read(&manifest).expect("read fixture manifest"))
            .expect("fixture manifest");
    assert!(fixture.flight_address.ip().is_loopback() && fixture.flight_address.port() > 0);
    let create = Credential::read(&root.join(fixture.create_credential));
    let assignment = Credential::read(&root.join(fixture.assignment_credential));
    assert_eq!(create.rtdb_workload_grant.version, 1);
    assert_eq!(assignment.rtdb_workload_grant.version, 2);

    let body = Bytes::from(
        std::fs::read(root.join(fixture.frozen_advance_body)).expect("frozen first Advance body"),
    );
    let request = v1::ManagedWorkloadRequest::decode(body.clone()).expect("first Advance request");
    let Some(v1::managed_workload_request::Operation::AdvancePlacement(advance)) =
        request.operation
    else {
        panic!("expected AdvancePlacement");
    };
    assert!(matches!(
        advance.previous,
        Some(v1::workload_advance_placement::Previous::Empty(_))
    ));
    let next = advance.next.expect("first placement target");
    assert_eq!(next.epoch, 1);
    assert!(next.assignment.is_some());
    assert_eq!(request.key.len(), 16);
    assert!(request.key.iter().any(|&byte| byte != 0));
    assert_eq!(request.key, advance.installation_intent);

    let ca = std::fs::read(root.join(fixture.flight_ca)).expect("read private Flight CA");
    let channel = Channel::from_shared(format!("https://{}", fixture.flight_address))
        .unwrap()
        .tls_config(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca)))
        .unwrap()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .connect()
        .await
        .expect("connect to the exact local Flight listener");

    let mut create_client = create.handshake(channel.clone()).await;
    let response = create_client
        .action(CREATE, create.create_body())
        .await
        .expect("exact Create");
    assert_eq!(response.len(), 1);
    let deployment = Deployment::decode(response[0].body.clone()).expect("Create receipt");
    assert_eq!(deployment.deployment, create.rtdb_workload_grant.database);
    assert_eq!(
        deployment.incarnation.as_slice(),
        &create.rtdb_workload_grant.incarnation
    );
    expect_role_refusal(&mut create_client, ADVANCE, body.clone()).await;

    let mut assignment_client = assignment.handshake(channel).await;
    expect_role_refusal(&mut assignment_client, CREATE, assignment.create_body()).await;
    let response = assignment_client
        .action(ADVANCE, body)
        .await
        .expect("exact first Advance");
    assert_eq!(response.len(), 1);
    let response = v1::ManagedWorkloadResponse::decode(response[0].body.clone())
        .expect("first Advance response");
    assert_eq!(
        response.receipt_namespace.as_slice(),
        &assignment.receipt_namespace()
    );
    let Some(v1::managed_workload_response::Result::Receipt(receipt)) = response.result else {
        panic!("expected ordered first Advance receipt");
    };
    assert_eq!(receipt.key, request.key);
    assert!(matches!(
        receipt.outcome,
        Some(v1::receipt::Outcome::Then(_))
    ));
    assert!(
        !receipt.replayed,
        "qualify against a fresh retained attempt"
    );
    let basis = Basis::from_bytes(&receipt.basis.expect("ordered receipt basis").value)
        .expect("valid ordered receipt basis");
    assert_eq!(
        basis.incarnation(),
        assignment.rtdb_workload_grant.incarnation
    );
    assert!(basis.position() > 0);
}
