use std::sync::atomic::{AtomicU64, Ordering};

use arrow_flight::{
    Action, FlightData, HandshakeRequest, HandshakeResponse,
    flight_service_client::FlightServiceClient,
};
use journal_contract::Lsn;
use prost::Message;
use ring::signature::{Ed25519KeyPair, KeyPair};
use rtdb_crypt::receiver_clock::{ReceiverCertificateBinding, VerifiedReceiverCertificate};
use rtdb_flight_sql::rtdb::{store::v1 as store, v1::Error};
use rtdb_node::CertifiedDecisionClock;
use rtdb_tx::{DecisionClock, DecisionObservation};
use serde_json::json;
use stream_logkeeper::stream::consensus::{CLUSTER_VERSION_CURRENT, testing::LocalCluster};
use tokio::sync::mpsc;
use tonic::{
    Code, Request, Response, Status, Streaming,
    metadata::{Ascii, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};

use super::super::*;

const PRINCIPAL: &str = "urn:logkeeper:client:rtdb-timed-flight";
const TIMED_CAPABILITY: &str = "x-rtdb-store-timed-append";
const TIMED_INCARNATION: &str = "x-rtdb-store-timed-incarnation-bin";
const AUTHORIZATION_KEY: &[u8] = b"\0\0authorization";
const EFFECT_KEY: &[u8] = b"\0\0effect";

#[test]
fn timed_store_round_trips_over_exact_tls_flight() {
    std::thread::Builder::new()
        .stack_size(rtdb_node::WORKER_STACK_BYTES)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(rtdb_node::WORKER_STACK_BYTES)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(90), tls_round_trip())
                        .await
                        .expect("local timed Store fixture did not finish");
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn tls_round_trip() {
    let incarnation = Uuid::new_v4();
    let journal_stream = StreamId(incarnation.as_u128());
    let mut keepers = LocalCluster::start_authenticated(&[PRINCIPAL])
        .await
        .unwrap();
    let placement = keepers.journal_config(64 << 20, 256 << 10);
    let application = Arc::new(
        keepers
            .journal(PRINCIPAL, &placement)
            .unwrap()
            .with_timed_original(keepers.timed_verifier(), Some(64 << 10)),
    );
    let mut control_keepers = LocalCluster::start_authenticated(&[PRINCIPAL])
        .await
        .unwrap();
    let control = Arc::new(
        control_keepers
            .journal(
                PRINCIPAL,
                &control_keepers.journal_config(64 << 20, 256 << 10),
            )
            .unwrap(),
    );
    for journal in [&application, &control] {
        journal
            .finalize_version(CLUSTER_VERSION_CURRENT)
            .await
            .unwrap();
    }
    let history = application.catalog_history().await.unwrap();
    let application_root = history.genesis.root();
    let application_catalog = history.genesis.catalog;
    let control_root = control.catalog_history().await.unwrap().genesis.root();
    assert_ne!(control_root, application_root);
    let mut authority = ActivationAuthority::new(control.clone(), Uuid::new_v4()).unwrap();
    Arc::get_mut(&mut authority).unwrap().authenticated_journal = Some(control);
    authority
        .bootstrap(DEVELOPMENT_DATABASE, incarnation)
        .await
        .unwrap();

    let data = tempfile::tempdir().unwrap();
    let (config, limit, memory, runtime) = open_runtime(
        data.path(),
        incarnation,
        &placement,
        application.clone(),
        authority,
    )
    .await;
    let certificate = receiver_certificate(incarnation);
    let receiver_digest = certificate.digest();
    let clock = Arc::new(
        CertifiedDecisionClock::new(
            Arc::new(LiveReceiver(AtomicU64::new(1))),
            certificate,
            [9; 16],
        )
        .unwrap(),
    );
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate_pem = identity.cert.pem();
    let tls = crate::tls::from_pem_identity(
        certificate_pem.as_bytes(),
        identity.signing_key.serialize_pem().as_bytes(),
    )
    .unwrap();
    let (handle, server) = crate::ServerDriver::open_with_multi_certified_clock(
        config,
        limit,
        memory,
        &runtime,
        Some(tls),
        clock,
    )
    .await
    .unwrap();
    let server = tokio::spawn(server.run());
    let address = handle.addresses().flight_sql.unwrap();
    let mut client = FlightServiceClient::new(
        Endpoint::from_shared(format!("https://{address}"))
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .domain_name("localhost")
                    .ca_certificate(Certificate::from_pem(certificate_pem)),
            )
            .unwrap()
            .connect()
            .await
            .unwrap(),
    );
    // Drive readiness, then tick explicitly before each exchange so unrelated
    // lease renewals cannot change the journal head during a challenged turn.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            runtime.tick(handle.node()).await.unwrap();
            if runtime
                .ready_driver(DEVELOPMENT_DATABASE, incarnation)
                .await
                .is_ok_and(|driver| driver.incarnation() == *incarnation.as_bytes())
            {
                let attachment = handle
                    .node()
                    .catalog()
                    .database(DEVELOPMENT_DATABASE)
                    .unwrap();
                if attachment
                    .serving_driver()
                    .unwrap()
                    .supports_timed_store_append()
                    .await
                {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();

    assert_eq!(
        handshake(&mut client, Uuid::new_v4())
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let accepted = handshake(&mut client, incarnation).await.unwrap();
    assert_eq!(accepted.metadata().get(TIMED_CAPABILITY).unwrap(), "1");
    assert_eq!(
        accepted
            .metadata()
            .get_bin(TIMED_INCARNATION)
            .unwrap()
            .to_bytes()
            .unwrap()
            .as_ref(),
        incarnation.as_bytes()
    );
    assert_eq!(
        accepted
            .metadata()
            .get("x-rtdb-store-writer-fence")
            .unwrap(),
        "2"
    );
    let bearer = accepted.metadata().get("authorization").unwrap().clone();
    assert!(bearer.to_str().unwrap().starts_with("Bearer "));
    let mut handshake_body = accepted.into_inner();
    assert!(handshake_body.message().await.unwrap().is_none());

    let writer_fence = claim_writer(&mut client, &bearer, incarnation).await;
    let read = read_store(&mut client, &bearer, incarnation).await;
    assert_eq!(read.incarnation.as_slice(), incarnation.as_bytes());
    assert!(read.pages[0].entries.is_empty());

    runtime.tick(handle.node()).await.unwrap();
    let before = application.head(journal_stream).await.unwrap();
    let begin = append_pair(
        incarnation,
        read.at,
        application_root.0,
        receiver_digest,
        &writer_fence,
    );
    let (send, mut output) = exchange(&mut client, &bearer, begin.clone()).await;
    let challenge = prepared(&mut output).await;
    assert_eq!(challenge.lsn, before.0 + 2);
    assert_eq!(application.head(journal_stream).await.unwrap(), before);
    let mut changed = begin.preview.unwrap();
    changed.writes[0].key = b"\0\0unexpected-authorization".to_vec();
    send.send(frame(store::timed_append_input::Message::Confirm(changed)))
        .await
        .unwrap();
    drop(send);
    assert_status(
        output.message().await.unwrap_err(),
        Code::InvalidArgument,
        "rtdb.store/timed-append-protocol",
    );
    assert_eq!(application.head(journal_stream).await.unwrap(), before);

    runtime.tick(handle.node()).await.unwrap();
    let before = application.head(journal_stream).await.unwrap();
    let begin = append_pair(
        incarnation,
        read.at,
        application_root.0,
        receiver_digest,
        &writer_fence,
    );
    let (send, mut output) = exchange(&mut client, &bearer, begin.clone()).await;
    let challenge = prepared(&mut output).await;
    assert_eq!(challenge.catalog.as_slice(), application_catalog.as_bytes());
    assert_eq!(challenge.stream.as_slice(), incarnation.as_bytes());
    assert!(challenge.writer_epoch > 0);
    assert_eq!(challenge.lsn, before.0 + 2);
    assert_eq!(challenge.journal_before_micros, begin.journal_before_micros);
    assert_eq!(
        challenge.receiver_certificate_sha256.as_slice(),
        receiver_digest
    );
    assert_eq!(application.head(journal_stream).await.unwrap(), before);
    let mut confirmed = begin.preview.clone().unwrap();
    confirmed.writes[0].value = Some(challenge.payload_sha256.clone());
    send.send(frame(store::timed_append_input::Message::Confirm(
        confirmed,
    )))
    .await
    .unwrap();
    drop(send);
    let store::timed_append_output::Message::Committed(receipt) = reply(&mut output).await else {
        panic!("expected the committed timed Store receipt");
    };
    assert_eq!(receipt.original.as_ref(), Some(&challenge));
    assert!(matches!(
        receipt.reply.unwrap().outcome,
        Some(store::append_reply::Outcome::Committed(_))
    ));
    assert!(receipt.quorum_sync_at_micros < challenge.journal_before_micros);
    assert_eq!(receipt.evidence_sha256.len(), 32);
    assert_ne!(receipt.evidence_sha256, [0; 32]);
    assert!(output.message().await.unwrap().is_none());

    let at = Lsn(challenge.lsn);
    let (send, mut replay) = exchange(&mut client, &bearer, begin).await;
    assert_status(
        replay.message().await.unwrap_err(),
        Code::Aborted,
        "rtdb.store/timed-append-ordered",
    );
    drop(send);
    assert_eq!(application.head(journal_stream).await.unwrap(), at);
    let archive = application
        .authenticated_archive(journal_stream, Some(at.0))
        .await
        .unwrap();
    let proof = archive.inclusion(at).unwrap();
    let leaf = proof.verify(application_root).unwrap();
    assert_eq!(
        leaf.payload_sha256.as_slice(),
        challenge.payload_sha256.as_slice()
    );
    assert_eq!(
        leaf.timing.unwrap().exclusive_cutoff_micros,
        challenge.journal_before_micros
    );
    let read = read_store(&mut client, &bearer, incarnation).await;
    let entries = &read.pages[0].entries;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].key, AUTHORIZATION_KEY);
    assert_eq!(
        entries[0].value.as_deref(),
        Some(challenge.payload_sha256.as_slice())
    );
    assert_eq!(entries[1].key, EFFECT_KEY);
    assert_eq!(entries[1].value.as_deref(), Some(b"value".as_slice()));
    assert_eq!(entries[0].written + 1, entries[1].written);

    drop(client);
    handle.shutdown();
    server.await.unwrap().unwrap();
    drop(handle);
    runtime.close().await;
    control_keepers.shutdown().await.unwrap();
    keepers.shutdown().await.unwrap();
}

async fn open_runtime(
    data: &Path,
    incarnation: Uuid,
    placement: &JournalConfig,
    application: Arc<ConsensusJournal>,
    authority: Arc<ActivationAuthority>,
) -> (NodeConfiguration, usize, Arc<MemoryBudget>, MultiRuntime) {
    let placement_file = data.join("journal.json");
    fs::write(&placement_file, json!({
        "keepers": placement.keepers, "catalog": placement.catalog, "catalog_voters": placement.catalog_voters,
        "voters": placement.voters, "quota_bytes": placement.quota_bytes, "max_record_bytes": placement.max_record_bytes,
    }).to_string()).unwrap();
    let mut config = NodeConfiguration::from_yaml_with_environment(
        "multiJournal: {immutableStreams: true}\nflightSql:\n  kernelTimedStore: true\n  ownedNativeActions: true\n  hostedControls: {owner: xtdb, relations: [bj_installation]}\n",
        &|name| (name == "XTDB_PASSWORD").then(|| "secret".into()), false,
    ).unwrap();
    config.primary.storage = Some(Storage::LocalStorage(LocalStorage {
        path: Some(data.join("catalog").to_str().unwrap().to_owned()),
        epoch: Some(0),
    }));
    let limit = 256 << 20;
    let memory = MemoryBudget::new(config.node_options(limit).unwrap().execution_memory_bytes);
    let io = IoRegion::new();
    let backends = Backends {
        journal: application.clone(),
        batch: Some(Arc::new(ConsensusJournalBatch(application.clone()))),
        placement: Some(PlacementSource {
            journal: application,
            config: placement_file,
        }),
        store: Arc::new(rtdb_storage::MemoryStore::default()),
        io,
        vault: data.join("keys"),
        node: Ipv4Addr::LOCALHOST.into(),
        peers: None,
        checkpoints: config.multi_checkpoints,
        journal_settings: config.multi_journal,
        activation: Some(authority),
        #[cfg(all(feature = "s3", feature = "backup-object-holds"))]
        ranked_provider: None,
        offline_scope: None,
        _journal_lock: None,
    };
    let runtime = MultiRuntime::open(
        &mut config,
        memory.clone(),
        BTreeMap::from([(DEVELOPMENT_DATABASE.into(), incarnation)]),
        backends,
        None,
    )
    .await
    .unwrap();
    (config, limit, memory, runtime)
}

struct LiveReceiver(AtomicU64);

#[async_trait::async_trait]
impl DecisionClock for LiveReceiver {
    async fn observe(&self) -> Result<DecisionObservation> {
        Ok(DecisionObservation {
            domain: [9; 16],
            epoch: 1,
            sequence: self.0.fetch_add(1, Ordering::Relaxed),
            instant: SystemClock.now(),
        })
    }
}

fn receiver_certificate(source: Uuid) -> VerifiedReceiverCertificate {
    let signer = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
    let binding = ReceiverCertificateBinding {
        issuer: signer.public_key().as_ref().try_into().unwrap(),
        purpose: "bijection.rtdb.durable-acceptance-clock.v4",
        deployment: Uuid::new_v4(),
        source,
        cell: "local-test",
        image: "local-test",
        service_scope: &"ab".repeat(32),
    };
    let statement = json!({
        "purpose": binding.purpose, "deployment": binding.deployment, "source": binding.source,
        "cell": binding.cell, "rtdbImage": binding.image, "serviceScope": binding.service_scope,
        "maxStageToDurableLagNs": 5_001_000, "maxJournalClockErrorNs": 1_000,
        "expiresAtNs": (SystemClock.now().0 + 120_000_000) as u64 * 1_000,
        "evidenceSha256": "cd".repeat(32),
    })
    .to_string();
    let signature: String = signer
        .sign(statement.as_bytes())
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let envelope = json!({ "statement": statement, "signature": signature }).to_string();
    VerifiedReceiverCertificate::verify(envelope.as_bytes(), binding).unwrap()
}

async fn handshake(
    client: &mut FlightServiceClient<Channel>,
    incarnation: Uuid,
) -> std::result::Result<Response<Streaming<HandshakeResponse>>, Status> {
    let mut request = Request::new(futures::stream::iter([HandshakeRequest::default()]));
    request.metadata_mut().insert(
        "x-xtdb-database",
        MetadataValue::from_static(DEVELOPMENT_DATABASE),
    );
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::from_static("Basic eHRkYjpzZWNyZXQ="),
    );
    request
        .metadata_mut()
        .insert(TIMED_CAPABILITY, MetadataValue::from_static("1"));
    request.metadata_mut().insert_bin(
        TIMED_INCARNATION,
        MetadataValue::from_bytes(incarnation.as_bytes()),
    );
    request.metadata_mut().insert(
        "x-rtdb-store-release",
        MetadataValue::from_static(::store::CONTRACT_VERSION),
    );
    client.handshake(request).await
}

async fn claim_writer(
    client: &mut FlightServiceClient<Channel>,
    bearer: &MetadataValue<Ascii>,
    incarnation: Uuid,
) -> store::WriterFence {
    let claim = store::ClaimWriterRequest {
        incarnation: incarnation.as_bytes().to_vec(),
        epoch: 1,
        assignment: Uuid::new_v4().as_bytes().to_vec(),
        attempt: Uuid::new_v4().as_bytes().to_vec(),
    };
    let request = writer_request(
        Action {
            r#type: "rtdb.store.v1.ClaimWriter".into(),
            body: claim.encode_to_vec().into(),
        },
        bearer,
    );
    let mut output = client.do_action(request).await.unwrap().into_inner();
    let reply = store::ClaimWriterReply::decode(output.message().await.unwrap().unwrap().body)
        .unwrap();
    assert_eq!(reply.claim.as_ref(), Some(&claim));
    assert!(reply.generation > 0);
    assert!(output.message().await.unwrap().is_none());
    store::WriterFence {
        epoch: claim.epoch,
        writer: claim.assignment,
        generation: reply.generation,
        claim: claim.attempt,
    }
}

async fn read_store(
    client: &mut FlightServiceClient<Channel>,
    bearer: &MetadataValue<Ascii>,
    incarnation: Uuid,
) -> store::ReadReply {
    let read = store::ReadRequest {
        incarnation: incarnation.as_bytes().to_vec(),
        limit: 8,
        ranges: vec![store::KeyRange {
            start: vec![0, 0],
            end: Some(vec![0, 1]),
        }],
        ..Default::default()
    };
    let request = writer_request(
        Action {
            r#type: "rtdb.store.v1.Read".into(),
            body: read.encode_to_vec().into(),
        },
        bearer,
    );
    let mut output = client.do_action(request).await.unwrap().into_inner();
    let read = store::ReadReply::decode(output.message().await.unwrap().unwrap().body).unwrap();
    assert!(output.message().await.unwrap().is_none());
    read
}

fn writer_request<T>(input: T, bearer: &MetadataValue<Ascii>) -> Request<T> {
    let mut request = Request::new(input);
    request.metadata_mut().insert(
        "x-xtdb-database",
        MetadataValue::from_static(DEVELOPMENT_DATABASE),
    );
    request
        .metadata_mut()
        .insert("authorization", bearer.clone());
    request
}

fn append_pair(
    incarnation: Uuid,
    since: u64,
    application_root: [u8; 32],
    certificate: [u8; 32],
    writer_fence: &store::WriterFence,
) -> store::TimedAppendBegin {
    let now = SystemClock.now().0;
    let decision_before = now + 1_750_000;
    let journal_before = now + 2_000_000;
    let append = |key, address: &[u8], value, before| store::AppendRequest {
        key: vec![key; 16],
        incarnation: incarnation.as_bytes().to_vec(),
        since,
        writes: vec![store::Write {
            key: address.to_vec(),
            value: Some(value),
            ..Default::default()
        }],
        window: Some(store::Window {
            not_before: Some(now),
            before: Some(before),
        }),
        writer_fence: Some(writer_fence.clone()),
        ..Default::default()
    };
    store::TimedAppendBegin {
        preview: Some(append(1, AUTHORIZATION_KEY, vec![0; 32], decision_before)),
        effect: Some(append(2, EFFECT_KEY, b"value".to_vec(), journal_before)),
        application_root: application_root.to_vec(),
        decision_before_micros: decision_before,
        journal_before_micros: journal_before,
        receiver_certificate_sha256: certificate.to_vec(),
    }
}

async fn exchange(
    client: &mut FlightServiceClient<Channel>,
    bearer: &MetadataValue<Ascii>,
    begin: store::TimedAppendBegin,
) -> (mpsc::Sender<FlightData>, Streaming<FlightData>) {
    let (send, receive) = mpsc::channel(1);
    send.send(frame(store::timed_append_input::Message::Begin(begin)))
        .await
        .unwrap();
    let input = futures::stream::unfold(receive, |mut receiver| async {
        receiver.recv().await.map(|frame| (frame, receiver))
    });
    let mut request = writer_request(input, bearer);
    request.metadata_mut().insert(
        "x-rtdb-store-exchange",
        MetadataValue::from_static("timed-append-v1"),
    );
    let output = client.do_exchange(request).await.unwrap().into_inner();
    (send, output)
}

fn frame(message: store::timed_append_input::Message) -> FlightData {
    FlightData {
        data_body: store::TimedAppendInput {
            message: Some(message),
        }
        .encode_to_vec()
        .into(),
        ..Default::default()
    }
}

async fn reply(output: &mut Streaming<FlightData>) -> store::timed_append_output::Message {
    let frame = output.message().await.unwrap().unwrap();
    assert!(
        frame.flight_descriptor.is_none()
            && frame.data_header.is_empty()
            && frame.app_metadata.is_empty()
    );
    store::TimedAppendOutput::decode(frame.data_body)
        .unwrap()
        .message
        .unwrap()
}

async fn prepared(output: &mut Streaming<FlightData>) -> store::TimedAppendCommitment {
    let store::timed_append_output::Message::Prepared(challenge) = reply(output).await else {
        panic!("expected the timed Store preparation challenge");
    };
    challenge
}

fn assert_status(status: Status, code: Code, error: &str) {
    assert_eq!(status.code(), code, "{status}");
    let detail = status
        .metadata()
        .get_bin("rtdb-error-bin")
        .unwrap()
        .to_bytes()
        .unwrap();
    assert_eq!(Error::decode(detail).unwrap().code, error);
}
