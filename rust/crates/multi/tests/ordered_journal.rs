//! Acceptance through the installed journal daemon and RTDB's ordinary clients.

mod common;
#[allow(dead_code)]
#[path = "../../kafka/tests/support/kafka_tls.rs"]
mod tls_fixture;

use std::fs::{self, OpenOptions};
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use ordered_logkeeper::ordered::daemon::{
    ApplicationRoute, Client, Credentials, Manifest, SuccessorRoute,
};
use ordered_logkeeper::ordered::service::{
    Route,
    wire::{self, Decision, Operation, Reply, WireError},
};
use ordered_logkeeper::ordered::{
    APPEND_RECORD_CHARGE_BYTES, AppendClass, AppendResult, Checkpoint, CheckpointPreparation,
    Command, Custody, Enrollment, Fingerprint, Grant, Hold, Outcome, Placement, Principal, Refusal,
    Request, RequestId, TrimResult, Window,
};
use ring::digest::{SHA256, digest};
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};
use rtdb_directory::{DeploymentRecord, Directory, MemberPool, MemoryDirectory, now_ms};
use rtdb_multi::agent::AgentOptions;
use rtdb_multi::deployment::CellStorage;
use rtdb_multi::gateway::{self, GatewayOptions};
use rtdb_multi::journal::hosted::{GatewayRoutes, archive_identity};
use rtdb_multi::keys::{KeyService, LocalKeyService};
use rtdb_multi::{admin, flight_gateway, process};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_rustls::{
    TlsConnector,
    rustls::{
        ClientConfig, RootCertStore,
        crypto::ring as tls_ring,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
        version::TLS13,
    },
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(20);
const FRAME: usize = 5 << 20;

async fn eventually<T>(mut probe: impl AsyncFnMut() -> Option<T>) -> T {
    tokio::time::timeout(DEADLINE, async {
        loop {
            if let Some(found) = probe().await {
                return found;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("condition never became observable")
}

fn reserve_port() -> std::net::TcpListener {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap()
}

fn write_json(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
}

struct Identity {
    credentials: Value,
    pin: Fingerprint,
}

impl Identity {
    fn new(directory: &Path, name: &str, issuer: &tls_fixture::Credential) -> Self {
        let leaf = tls_fixture::Credential::new(name, true, Some(issuer));
        let certificate = directory.join(format!("{name}.crt"));
        let key = directory.join(format!("{name}.key"));
        fs::write(&certificate, leaf.certificate.to_pem().unwrap()).unwrap();
        fs::write(&key, leaf.key.private_key_to_pem_pkcs8().unwrap()).unwrap();
        let pin = digest(&SHA256, &leaf.certificate.to_der().unwrap())
            .as_ref()
            .try_into()
            .unwrap();
        Self {
            credentials: json!({ "certificate": certificate, "key": key, "roots": directory.join("ca.crt") }),
            pin,
        }
    }

    fn client(&self, route: &Value) -> Client {
        Client::new(
            serde_json::from_value(route.clone()).unwrap(),
            &serde_json::from_value(self.credentials.clone()).unwrap(),
        )
        .unwrap()
    }

    fn pin_for(&self, role: Value) -> Value {
        json!({ "certificate": self.pin, "role": role })
    }
}

type DaemonJob = JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>;

struct Voter {
    address: SocketAddr,
    port: Option<std::net::TcpListener>,
    manifest: PathBuf,
    stop: CancellationToken,
    job: Option<DaemonJob>,
}

impl Voter {
    fn manifest(&self) -> Manifest {
        Manifest::read(&self.manifest).unwrap()
    }

    fn start(&mut self) {
        let manifest = self.manifest();
        drop(self.port.take());
        self.job = Some(tokio::spawn(manifest.run(self.stop.clone())));
    }

    async fn stop(&mut self) {
        self.stop.cancel();
        if let Some(job) = self.job.take() {
            tokio::time::timeout(DEADLINE, job)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }
}

impl Drop for Voter {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(job) = &self.job {
            job.abort();
        }
    }
}

struct Cohort {
    voters: Vec<Voter>,
    group: PathBuf,
    route: Value,
}

struct Authority {
    data: tempfile::TempDir,
    issuer: tls_fixture::Credential,
    group_spec: Value,
    placement: Placement,
    administrator: Identity,
    custodian: Identity,
    router: Identity,
    workers: [Identity; 2],
    enrollments: [Enrollment; 2],
}

impl Authority {
    fn new(deployment: Uuid, ordinary: u64, maintenance: u64, replay_holds: u64) -> Self {
        let data = tempfile::tempdir().unwrap();
        let path = data.path();
        let issuer = tls_fixture::Credential::new("ordered-acceptance-ca", true, None);
        fs::write(path.join("ca.crt"), issuer.certificate.to_pem().unwrap()).unwrap();
        let placement = Placement {
            deployment,
            incarnation: Uuid::new_v4(),
            group: Uuid::new_v4(),
            generation: 1,
        };
        // Fixed ordering makes worker 0 the first gateway candidate.
        let enrollments = [0, 1].map(|index| Enrollment {
            worker: Uuid::from_u128(index as u128 + 1),
            serial: index as u64 + 1,
        });
        let administrator = Identity::new(path, "administrator", &issuer);
        let custodian = Identity::new(path, "custodian", &issuer);
        let router = Identity::new(path, "router", &issuer);
        let workers = [0, 1].map(|index| Identity::new(path, &format!("worker-{index}"), &issuer));
        let initial_history = [7_u8; 32];
        let empty_digest = [0_u8; 32];
        let group_spec = json!({
            "placement": placement,
            "limits": {
                "enrolled_workers": 2,
                "replay_holds": replay_holds,
                "retry_positions": 128,
                "retained_data_bytes": ordinary + maintenance,
                "maintenance_bytes": maintenance,
            },
            "initial_checkpoint": {
                "cut": {"next_lsn": 0, "history": initial_history},
                "root": empty_digest,
                "manifest": empty_digest,
                "custody": {"allocation": Uuid::new_v4(), "revision": 1, "provider": empty_digest},
            },
            "initial_window": {"allocation": Uuid::new_v4(), "serial": 1, "bytes": ordinary},
            "maximum_prefix_bytes": ordinary + maintenance + (1 << 20),
            "maximum_application_frame": FRAME,
            "clients": [
                administrator.pin_for(json!({"kind": "administrator"})),
                custodian.pin_for(json!({"kind": "custodian"})),
                router.pin_for(json!({"kind": "route_reader"})),
                workers[0].pin_for(json!({"kind": "worker", "enrollment": enrollments[0]})),
                workers[1].pin_for(json!({"kind": "worker", "enrollment": enrollments[1]})),
            ],
        });
        Self {
            data,
            issuer,
            group_spec,
            placement,
            administrator,
            custodian,
            router,
            workers,
            enrollments,
        }
    }

    fn cohort(&self, name: &str, predecessor: Option<&ApplicationRoute>) -> Cohort {
        let mut peers = Vec::new();
        let mut manifests = Vec::new();
        for index in 0..3 {
            let label = format!("{name}-voter-{index}");
            let voter = Uuid::new_v4();
            let port = reserve_port();
            let address = port.local_addr().unwrap();
            let identity = Identity::new(self.data.path(), &label, &self.issuer);
            let barrier_key = self.data.path().join(format!("{label}.ed25519"));
            let pair = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
            fs::write(&barrier_key, pair.as_ref()).unwrap();
            let key = Ed25519KeyPair::from_pkcs8(pair.as_ref()).unwrap();
            let receipt_key: [u8; 32] = key.public_key().as_ref().try_into().unwrap();
            peers.push(json!({
                "voter": voter,
                "zone": format!("az-{index}"),
                "endpoint": {"address": address, "server_name": "localhost", "certificate": identity.pin},
                "receipt_key": receipt_key,
            }));
            let manifest = json!({
                "voter": voter,
                "listen": address,
                "directory": self.data.path().join(&label),
                "credentials": identity.credentials,
                "barrier_key": barrier_key,
                "maximum_connections": 64,
                "exchange_ms": 2_500,
            });
            manifests.push((label, port, address, manifest));
        }
        let mut group = self.group_spec.clone();
        group["voters"] = json!(peers);
        if let Some(source) = predecessor {
            group["predecessor"] = json!(source.group);
            group["predecessor_keys"] = json!(source.verification_keys);
        }
        let group_path = self.data.path().join(format!("{name}-group.json"));
        write_json(&group_path, &group);
        let voters: Vec<_> = manifests
            .into_iter()
            .map(|(label, port, address, mut manifest)| {
                manifest["groups"] = json!([group]);
                let path = self.data.path().join(format!("{label}.json"));
                write_json(&path, &manifest);
                Voter {
                    address,
                    port: Some(port),
                    manifest: path,
                    stop: CancellationToken::new(),
                    job: None,
                }
            })
            .collect();
        let route = voters[0]
            .manifest()
            .application_route(self.placement.deployment)
            .unwrap();
        Cohort {
            voters,
            group: group_path,
            route: serde_json::to_value(route).unwrap(),
        }
    }

    async fn start(&self, name: &str) -> Cohort {
        let mut cohort = self.cohort(name, None);
        for voter in &mut cohort.voters {
            voter.manifest().initialize().unwrap();
            voter.start();
        }
        let control = self.administrator.client(&cohort.route);
        eventually(async || match control.call(Operation::Route).await {
            Ok(Reply::Route(route)) => Some(route),
            _ => None,
        })
        .await;
        for expected in self.enrollments {
            let observed = route(&control).await;
            assert_eq!(
                execute(
                    &control,
                    request(
                        observed,
                        Principal::Administrator,
                        Command::Enroll {
                            worker: expected.worker,
                            expected: None
                        }
                    )
                )
                .await
                .receipt
                .outcome,
                Ok(Outcome::Enrolled(expected)),
            );
        }
        cohort
    }
}

async fn route(client: &Client) -> Route {
    let Reply::Route(view) = client.call(Operation::Route).await.unwrap() else {
        panic!("the installed journal did not report its route");
    };
    view
}

fn request(view: Route, principal: Principal, command: Command) -> Request {
    Request {
        placement: view.placement,
        principal,
        id: RequestId::fresh(view.position, view.retry_positions).unwrap(),
        command,
    }
}

async fn execute(client: &Client, request: Request) -> Decision {
    let Reply::Decision(decision) = client.call(Operation::Execute(request)).await.unwrap() else {
        panic!("the journal did not return a decision");
    };
    decision
}

async fn grant(client: &Client, who: Enrollment) -> (Grant, Hold) {
    let view = route(client).await;
    let decision = execute(
        client,
        request(
            view,
            Principal::Worker(who),
            Command::Grant {
                expected: view.writer,
                candidate: who,
            },
        ),
    )
    .await;
    let Outcome::Granted { grant, recovery } = decision.receipt.outcome.unwrap() else {
        panic!("the worker did not obtain its recovery grant");
    };
    (grant, recovery)
}

async fn release(client: &Client, hold: Hold) {
    assert!(
        matches!(client.call(Operation::Release { hold }).await.unwrap(),
        Reply::Released(released) if released == hold)
    );
}

// This stands at the caller's transport boundary, after authentication. It
// proves the daemon started sending the reply, then makes delivery fail.
struct DropReply<S>(S);

impl<S: AsyncRead + Unpin> AsyncRead for DropReply<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut byte = [0_u8];
        let mut observed = ReadBuf::new(&mut byte);
        match Pin::new(&mut self.0).poll_read(cx, &mut observed) {
            Poll::Ready(Ok(())) if !observed.filled().is_empty() => Poll::Ready(Err(
                io::Error::new(io::ErrorKind::ConnectionAborted, "discarded ordered reply"),
            )),
            result => result,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for DropReply<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, data)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

async fn discard_reply(identity: &Identity, route: &ApplicationRoute, original: Request) {
    let credentials: Credentials = serde_json::from_value(identity.credentials.clone()).unwrap();
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_file(credentials.roots).unwrap())
        .unwrap();
    let mut tls = ClientConfig::builder_with_provider(Arc::new(tls_ring::default_provider()))
        .with_protocol_versions(&[&TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_client_auth_cert(
            vec![CertificateDer::from_pem_file(credentials.certificate).unwrap()],
            PrivateKeyDer::from_pem_file(credentials.key).unwrap(),
        )
        .unwrap();
    tls.alpn_protocols = vec![b"logkeeper-application/1".to_vec()];
    let endpoint = &route.endpoints[0];
    let stream = TcpStream::connect(endpoint.address).await.unwrap();
    let mut channel = TlsConnector::from(Arc::new(tls))
        .connect(endpoint.server_name.clone().try_into().unwrap(), stream)
        .await
        .unwrap();
    let (_, peer) = channel.get_ref();
    let leaf = peer.peer_certificates().unwrap().first().unwrap();
    assert_eq!(
        digest(&SHA256, leaf.as_ref()).as_ref(),
        endpoint.certificate
    );
    channel.write_all(&route.group).await.unwrap();
    let outcome = wire::exchange(
        DropReply(channel),
        route.placement,
        Operation::Execute(original),
        route.maximum_frame,
        DEADLINE,
    )
    .await;
    assert!(
        matches!(outcome, Err(WireError::Io(error)) if error.kind() == io::ErrorKind::ConnectionAborted)
    );
}

fn append(view: Route, grant: Grant, class: AppendClass, payload: Bytes) -> Request {
    request(
        view,
        Principal::Worker(grant.enrollment),
        Command::Append {
            grant,
            class,
            expected_next: view.head.next_lsn,
            timestamp: 1,
            payload,
        },
    )
}

async fn reconcile(client: &Client, grant: Grant, original: Request) -> AppendResult {
    let Reply::Reconciled(appended) = client
        .call(Operation::Reconcile { grant, original })
        .await
        .unwrap()
    else {
        panic!("the surviving journal did not return the exact original append");
    };
    appended
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn funded_control_reconciles_lost_append_and_read_replies_through_trim_and_repair() {
    const ORDINARY: u64 = 4096;
    let authority = Authority::new(Uuid::new_v4(), ORDINARY, 1024, 1);
    let mut cohort = authority.start("initial").await;
    let installed: ApplicationRoute = serde_json::from_value(cohort.route.clone()).unwrap();
    let old = authority.workers[0].client(&cohort.route);
    let next = authority.workers[1].client(&cohort.route);
    let custody = authority.custodian.client(&cohort.route);
    let (incumbent, first_recovery) = grant(&old, authority.enrollments[0]).await;
    let before = route(&old).await;
    let payload = Bytes::from(vec![9; (ORDINARY - APPEND_RECORD_CHARGE_BYTES) as usize]);
    let original = append(before, incumbent, AppendClass::Data, payload.clone());
    discard_reply(&authority.workers[0], &installed, original.clone()).await;
    let appended = reconcile(&old, incumbent, original.clone()).await;
    assert_eq!(appended.lsn, before.head.next_lsn);
    assert_eq!(appended.byte_end, ORDINARY);
    assert_eq!(route(&old).await.head, appended.cut);

    let nonce = NonZeroU64::new(1).unwrap();
    let read_request = request(
        route(&old).await,
        Principal::Worker(incumbent.enrollment),
        Command::Barrier {
            grant: incumbent,
            nonce,
        },
    );
    discard_reply(&authority.workers[0], &installed, read_request.clone()).await;
    let recovered = execute(&old, read_request.clone()).await;
    let Outcome::Barrier { read, .. } = recovered.receipt.outcome.unwrap() else {
        panic!("the exact read remained unadmitted");
    };
    assert_eq!(read.id, read_request.id);
    assert_eq!(read.through, appended.cut);
    assert!(recovered.barrier.is_some());
    let fresh_request = request(
        route(&old).await,
        Principal::Worker(incumbent.enrollment),
        Command::Barrier {
            grant: incumbent,
            nonce,
        },
    );
    assert_eq!(
        execute(&old, fresh_request).await.receipt.outcome,
        Err(Refusal::Stale)
    );

    let failed_voter = installed.endpoints[0].address;
    cohort
        .voters
        .iter_mut()
        .find(|voter| voter.address == failed_voter)
        .unwrap()
        .stop()
        .await;
    let (replacement, second_recovery) = grant(&next, authority.enrollments[1]).await;
    assert_eq!(replacement.epoch, incumbent.epoch + 1);
    assert_eq!(second_recovery.through, appended.cut);
    let view = route(&next).await;
    assert_eq!(view.window_spent, ORDINARY);
    assert_eq!(
        execute(
            &next,
            append(
                view,
                replacement,
                AppendClass::Data,
                Bytes::from_static(b"ordinary")
            )
        )
        .await
        .receipt
        .outcome,
        Err(Refusal::Capacity),
    );
    assert_eq!(
        execute(
            &old,
            append(
                view,
                incumbent,
                AppendClass::Maintenance,
                Bytes::from_static(b"late")
            )
        )
        .await
        .receipt
        .outcome,
        Err(Refusal::Stale),
    );
    let maintained = execute(
        &next,
        append(
            route(&next).await,
            replacement,
            AppendClass::Maintenance,
            Bytes::from_static(b"bounded maintenance"),
        ),
    )
    .await;
    let Outcome::Appended(maintained) = maintained.receipt.outcome.unwrap() else {
        panic!("maintenance could not use the standing reservation");
    };
    assert_eq!(maintained.class, AppendClass::Maintenance);
    let pressure = route(&next).await;
    assert_eq!(pressure.window_spent, ORDINARY);
    assert_eq!(pressure.maintenance_retained, maintained.byte_end);
    assert_eq!(
        reconcile(&next, replacement, original.clone()).await,
        appended
    );

    let exact = Checkpoint {
        cut: maintained.cut,
        root: [19; 32],
        manifest: [23; 32],
        custody: Custody {
            allocation: Uuid::new_v4(),
            revision: 2,
            provider: [29; 32],
        },
    };
    // This case drives the journal authority in isolation. The separate RTDB
    // case below supplies the real provider validation and recoverable copy.
    let view = route(&custody).await;
    let preparation = CheckpointPreparation {
        checkpoint: exact.cut,
        root: exact.root,
        source: replacement,
        base: view.checkpoint,
        custody: exact.custody,
        usable_bytes: 64 << 20,
        descriptor: [31; 32],
    };
    assert_eq!(
        execute(
            &custody,
            request(
                view,
                Principal::Custodian,
                Command::PrepareCheckpoint { preparation }
            )
        )
        .await
        .receipt
        .outcome,
        Ok(Outcome::CheckpointPrepared(preparation))
    );
    let checkpoint_replay = |from| Operation::CheckpointReplay {
        preparation,
        base: view.checkpoint,
        from,
        maximum_bytes: ORDINARY + 1024,
    };
    assert!(matches!(
        next.call(checkpoint_replay(view.checkpoint.cut))
            .await
            .unwrap(),
        Reply::Refused(Refusal::Unauthorized)
    ));
    let mut alien = view.checkpoint.cut;
    alien.history[0] ^= 1;
    assert!(matches!(
        custody.call(checkpoint_replay(alien)).await.unwrap(),
        Reply::Refused(Refusal::Invalid)
    ));
    let Reply::Replay(prepared) = custody
        .call(checkpoint_replay(view.checkpoint.cut))
        .await
        .unwrap()
    else {
        panic!("the custodian lost its exact chosen input under ordinary pressure");
    };
    assert_eq!(prepared.through, maintained.cut);
    assert_eq!(prepared.records.len(), 2);
    assert_eq!(prepared.records[0].result, appended);
    assert_eq!(prepared.records[1].result, maintained);

    assert_eq!(
        execute(
            &custody,
            request(
                route(&custody).await,
                Principal::Custodian,
                Command::Publish { checkpoint: exact }
            )
        )
        .await
        .receipt
        .outcome,
        Ok(Outcome::Published(exact))
    );
    assert!(matches!(
        custody
            .call(checkpoint_replay(view.checkpoint.cut))
            .await
            .unwrap(),
        Reply::Refused(Refusal::Stale)
    ));

    release(&old, first_recovery).await;
    release(&next, second_recovery).await;
    let trim = Command::Trim {
        selected: exact,
        through: exact.cut,
    };
    assert_eq!(
        execute(
            &custody,
            request(route(&custody).await, Principal::Custodian, trim.clone())
        )
        .await
        .receipt
        .outcome,
        Err(Refusal::Unavailable),
        "the read that overlapped handoff still owns its replay interval"
    );
    let Reply::Replay(page) = old
        .call(Operation::Replay {
            hold: read,
            from: read.start,
            maximum_bytes: ORDINARY,
        })
        .await
        .unwrap()
    else {
        panic!("the displaced worker lost the live read hold");
    };
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].payload, payload);
    release(&old, read).await;
    assert!(
        execute(&old, read_request).await.barrier.is_none(),
        "a released marker cannot acquire a new proof"
    );
    assert_eq!(
        execute(
            &custody,
            request(route(&custody).await, Principal::Custodian, trim)
        )
        .await
        .receipt
        .outcome,
        Ok(Outcome::Trimmed(TrimResult {
            from: view.floor,
            through: exact.cut,
            ordinary_bytes: appended.byte_end - appended.byte_start,
            maintenance_bytes: maintained.byte_end - maintained.byte_start,
        }))
    );
    assert_eq!(route(&next).await.maintenance_retained, 0);
    assert!(matches!(
        old.call(Operation::Replay {
            hold: read,
            from: read.start,
            maximum_bytes: ORDINARY
        })
        .await
        .unwrap(),
        Reply::Refused(Refusal::Unavailable)
    ));
    assert_eq!(
        reconcile(&next, replacement, original.clone()).await,
        appended
    );
    let mut conflicting = original.clone();
    let Command::Append { payload, .. } = &mut conflicting.command else {
        unreachable!()
    };
    *payload = Bytes::from_static(b"different bytes");
    assert!(matches!(
        next.call(Operation::Reconcile {
            grant: replacement,
            original: conflicting
        })
        .await
        .unwrap(),
        Reply::Refused(Refusal::Conflict)
    ));

    let view = route(&custody).await;
    let funded = Window {
        allocation: Uuid::new_v4(),
        serial: view.window.serial + 1,
        bytes: ORDINARY,
    };
    assert_eq!(
        execute(
            &custody,
            request(
                view,
                Principal::Custodian,
                Command::FundWindow { window: funded }
            )
        )
        .await
        .receipt
        .outcome,
        Ok(Outcome::WindowFunded(funded))
    );
    let continued = execute(
        &next,
        append(
            route(&next).await,
            replacement,
            AppendClass::Data,
            Bytes::from_static(b"next"),
        ),
    )
    .await;
    assert!(matches!(
        continued.receipt.outcome,
        Ok(Outcome::Appended(_))
    ));
    let selected = route(&next).await;

    let mut successor = authority.cohort("successor", Some(&installed));
    let tickets: Vec<_> = successor
        .voters
        .iter()
        .enumerate()
        .map(|(index, voter)| {
            let ticket = authority.data.path().join(format!("reservation-{index}"));
            voter
                .manifest()
                .reserve_repair(authority.placement.deployment, &ticket)
                .unwrap();
            ticket
        })
        .collect();
    let source = cohort
        .voters
        .iter()
        .find(|voter| voter.address != failed_voter)
        .unwrap();
    let package = authority.data.path().join("sealed-cohort");
    assert!(
        source
            .manifest()
            .seal_repair(&successor.group, &[tickets[0].as_path()], &package)
            .await
            .is_err()
    );
    assert_eq!(route(&next).await.head, selected.head);
    let tickets: Vec<_> = tickets.iter().map(PathBuf::as_path).collect();
    source
        .manifest()
        .seal_repair(&successor.group, &tickets, &package)
        .await
        .unwrap();
    assert!(matches!(
        next.call(Operation::Route).await.unwrap(),
        Reply::Refused(Refusal::CohortSealed)
    ));
    for voter in &mut successor.voters {
        voter
            .manifest()
            .install_repair(authority.placement.deployment, &package)
            .unwrap();
        voter.start();
    }
    let mut ordered_route: ApplicationRoute = serde_json::from_value(cohort.route.clone()).unwrap();
    let repaired_route: ApplicationRoute = serde_json::from_value(successor.route.clone()).unwrap();
    ordered_route.successors.push(SuccessorRoute {
        predecessor: ordered_route.group,
        group: repaired_route.group,
        endpoints: repaired_route.endpoints,
    });
    ordered_route
        .verification_keys
        .extend(repaired_route.verification_keys);
    let repaired = authority.workers[1].client(&serde_json::to_value(ordered_route).unwrap());
    let inherited = eventually(async || match repaired.call(Operation::Route).await {
        Ok(Reply::Route(view)) => Some(view),
        _ => None,
    })
    .await;
    assert_eq!(inherited.head, selected.head);
    assert_eq!(inherited.floor, exact.cut);
    assert_eq!(inherited.checkpoint, exact);
    assert_eq!(inherited.writer, selected.writer);
    assert_eq!(inherited.window, funded);
    assert_eq!(reconcile(&repaired, replacement, original).await, appended);
    assert!(matches!(
        execute(
            &repaired,
            append(
                inherited,
                replacement,
                AppendClass::Data,
                Bytes::from_static(b"after repair")
            )
        )
        .await
        .receipt
        .outcome,
        Ok(Outcome::Appended(_))
    ));

    for voter in &mut successor.voters {
        voter.stop().await;
    }
    for voter in &mut cohort.voters {
        voter.stop().await;
    }
}

async fn lose_commit(listener: TcpListener, gateway: SocketAddr) {
    let (client, _) = listener.accept().await.unwrap();
    let backend = TcpStream::connect(gateway).await.unwrap();
    let (mut incoming, mut outgoing) = client.into_split();
    let (mut replies, mut requests) = backend.into_split();
    let reply = async {
        loop {
            let tag = replies.read_u8().await.unwrap();
            let frame_len = replies.read_i32().await.unwrap();
            assert!((4..1_048_576).contains(&frame_len));
            let mut payload = vec![0; (frame_len - 4) as usize];
            replies.read_exact(&mut payload).await.unwrap();
            if tag == b'C' && payload == b"COMMIT\0" {
                // A real CommandComplete was generated, but the SQL client
                // will never receive it or a ReadyForQuery for the command.
                return;
            }
            assert_ne!(tag, b'E', "{}", String::from_utf8_lossy(&payload));
            outgoing.write_u8(tag).await.unwrap();
            outgoing.write_i32(frame_len).await.unwrap();
            outgoing.write_all(&payload).await.unwrap();
        }
    };
    tokio::select! {
        () = reply => {}
        early = tokio::io::copy(&mut incoming, &mut requests) => {
            panic!("client closed before the committed result could be hidden: {early:?}");
        }
    }
}

async fn flight_client(
    address: SocketAddr,
    deployment: Uuid,
) -> arrow_flight::sql::client::FlightSqlServiceClient<tonic::transport::Channel> {
    let channel = tonic::transport::Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);
    client.set_header("x-xtdb-database", format!("shop@{}", deployment.simple()));
    client.handshake("xtdb", "cell-test").await.unwrap();
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_replaces_a_writer_and_resumes_its_prepared_provider_copy() {
    use futures::TryStreamExt;

    let record = DeploymentRecord::new("shop", "acceptance", Vec::new(), now_ms());
    let deployment: Uuid = record.id.parse().unwrap();
    let authority = Authority::new(deployment, 1 << 20, 24 << 20, 16);
    let mut cohort = authority.start("rtdb").await;
    let routing = authority.router.client(&cohort.route);
    let installed: ApplicationRoute = serde_json::from_value(cohort.route.clone()).unwrap();
    let directory: Arc<dyn Directory> = MemoryDirectory::new();
    let keys = Arc::new(LocalKeyService::new(authority.data.path().join("keys")).unwrap());
    keys.key(&record.key_id()).await.unwrap();
    let storage = CellStorage::Local {
        root: authority.data.path().join("serving"),
    };
    let archive_root = authority.data.path().join("archive");
    let archive = CellStorage::Local {
        root: archive_root.clone(),
    };
    admin::initialize_storage(&storage, keys.as_ref())
        .await
        .unwrap();
    admin::initialize_storage(&archive, keys.as_ref())
        .await
        .unwrap();
    let provider = archive_identity(&archive, "checkpoints").await.unwrap();
    let allocation = Uuid::new_v4();
    let reservations = json!([
        { "custody": {"allocation": allocation, "revision": 2, "provider": provider}, "usable_bytes": 64 << 20 },
        { "custody": {"allocation": Uuid::new_v4(), "revision": 3, "provider": provider}, "usable_bytes": 64 << 20 },
    ]);

    let ports = [0, 1].map(|_| (reserve_port(), reserve_port()));
    let endpoints: Vec<_> = ports
        .iter()
        .zip(authority.enrollments)
        .map(|((pgwire, control), enrollment)| {
            json!({
                "enrollment": enrollment,
                "control": control.local_addr().unwrap(),
                "pgwire": pgwire.local_addr().unwrap(),
            })
        })
        .collect();
    let installed_client = |identity: &Identity, role: Value| {
        json!({
            "incarnation": record.incarnation,
            "log_epoch": record.log_epoch,
            "authority": role,
            "route": cohort.route,
            "credentials": identity.credentials,
        })
    };
    let mut nodes = Vec::new();
    for (index, (pgwire, control)) in ports.into_iter().enumerate() {
        let path = authority
            .data
            .path()
            .join(format!("worker-route-{index}.json"));
        write_json(
            &path,
            &json!([{
                "deployment": record,
                "worker": installed_client(&authority.workers[index], json!({"kind": "worker", "enrollment": authority.enrollments[index]})),
                "custody": installed_client(&authority.custodian, json!({"kind": "custody"})),
                "endpoints": endpoints,
                "archive": archive,
                "archive_prefix": "checkpoints",
                "checkpoint_reservations": reservations,
            }]),
        );
        let control_address = control.local_addr().unwrap();
        let pgwire_port = pgwire.local_addr().unwrap().port();
        let configuration = rtdb_server::config::NodeConfiguration::from_yaml(&format!(
            "authn: !SingleRootUser\n  password: cell-test\nserver:\n  port: {pgwire_port}\n  numThreads: 4\nflightSql: {{port: 0}}\n",
        )).unwrap();
        let mut agent = AgentOptions::new(storage.clone());
        agent.installed_journal_manifest = Some(path);
        agent.watch_interval = Duration::from_millis(50);
        drop(control);
        drop(pgwire);
        let node = process::start_node(
            MemoryDirectory::new(),
            process::NodeSettings {
                configuration,
                allocation_limit_bytes: 1 << 30,
                control: control_address,
                advertise_host: Some("127.0.0.1".into()),
                hosted_task: None,
                availability_zone: format!("worker-zone-{index}"),
                engine_version: None,
                agent,
                lease: common::lease(),
                pool: MemberPool::General,
                claim_wait: Duration::from_millis(2),
                keys: keys.clone(),
                cache_dir: authority.data.path().join(format!("cache-{index}")),
                local_compaction: false,
                capabilities: Some(common::secret()),
            },
        )
        .await
        .unwrap();
        nodes.push(Some(node));
    }

    let gateway_manifest = authority.data.path().join("gateway.json");
    write_json(
        &gateway_manifest,
        &json!([{
            "name": "shop",
            "deployment": deployment,
            "journal": installed_client(&authority.router, json!({"kind": "routing"})),
            "endpoints": endpoints,
        }]),
    );
    let options = GatewayOptions {
        capabilities: Some(common::secret()),
        installed_journals: Some(Arc::new(GatewayRoutes::read(&gateway_manifest).unwrap())),
        ..Default::default()
    };
    let serving = CancellationToken::new();
    let sql_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sql_address = sql_listener.local_addr().unwrap();
    let sql = tokio::spawn(gateway::serve(
        sql_listener,
        Arc::clone(&directory),
        options.clone(),
        serving.clone(),
    ));
    let flight_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let flight_address = flight_listener.local_addr().unwrap();
    let flight = tokio::spawn(flight_gateway::serve(
        flight_listener,
        Arc::clone(&directory),
        options,
        serving.clone(),
    ));

    let initial = common::connect(&sql_address.to_string(), "shop").await;
    initial
        .simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)")
        .await
        .unwrap();
    drop(initial);
    let before = route(&routing).await;
    let incumbent = before.writer.grant.unwrap();
    let incumbent_index = authority
        .enrollments
        .iter()
        .position(|&worker| worker == incumbent.enrollment)
        .unwrap();

    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy.local_addr().unwrap();
    let discarded = tokio::spawn(lose_commit(proxy, sql_address));
    let faulted = common::connect(&proxy_address.to_string(), "shop").await;
    let update = "BEGIN READ WRITE WITH (METADATA = {idempotency_key: 'hidden-order-update'});                   UPDATE orders SET total = total + 1 WHERE _id = 1; COMMIT";
    assert!(
        tokio::time::timeout(DEADLINE, faulted.batch_execute(update))
            .await
            .unwrap()
            .is_err()
    );
    tokio::time::timeout(DEADLINE, discarded)
        .await
        .unwrap()
        .unwrap();
    assert!(route(&routing).await.head.next_lsn > before.head.next_lsn);

    let mut old_flight = flight_client(flight_address, deployment).await;
    let ticket = old_flight
        .execute("SELECT CAST(total AS BIGINT) FROM orders".into(), None)
        .await
        .unwrap()
        .endpoint
        .into_iter()
        .next()
        .unwrap()
        .ticket
        .unwrap();

    // Hold the real provider allocation in use, so staging fails after
    // the journal has selected the original exact checkpoint intent.
    let allocation_parent = archive_root
        .join("checkpoints")
        .join(deployment.to_string())
        .join(authority.placement.incarnation.to_string())
        .join("2");
    let allocation_name = allocation.to_string();
    let lifecycle = allocation_parent.join(format!("{allocation_name}.lifecycle"));
    fs::create_dir_all(&lifecycle).unwrap();
    let blocker = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(lifecycle.join("lock"))
        .unwrap();
    blocker.try_lock().unwrap();
    let original_node = nodes[incumbent_index].as_ref().unwrap();
    assert!(original_node.agent.flush("shop").await.is_err());
    let interrupted = route(&routing).await;
    let pending = interrupted
        .preparing
        .expect("the exact provider intent was ordered before the fault");
    assert_eq!(pending.source, incumbent);
    assert_eq!(pending.custody.allocation, allocation);
    assert_eq!(interrupted.checkpoint, before.checkpoint);
    assert_eq!(interrupted.floor, before.floor);
    assert!(
        !allocation_parent
            .join(&allocation_name)
            .join("manifest")
            .exists()
    );

    let unavailable = installed.endpoints[0].address;
    cohort
        .voters
        .iter_mut()
        .find(|voter| voter.address == unavailable)
        .unwrap()
        .stop()
        .await;
    nodes[incumbent_index].take().unwrap().kill().await.unwrap();
    let view = route(&routing).await;
    if view.writer.grant == Some(incumbent) {
        let old_writer = authority.workers[incumbent_index].client(&cohort.route);
        assert_eq!(
            execute(
                &old_writer,
                request(
                    view,
                    Principal::Worker(incumbent.enrollment),
                    Command::Vacate { grant: incumbent }
                )
            )
            .await
            .receipt
            .outcome,
            Ok(Outcome::Vacated)
        );
    }
    assert!(route(&routing).await.writer.grant.is_none());
    drop(blocker);

    // The second node's custodian resumes the chosen copy before any client
    // can promote it as a writer.
    let published = eventually(async || {
        let view = route(&routing).await;
        (view.checkpoint.custody == pending.custody && view.floor == pending.checkpoint)
            .then_some(view)
    })
    .await;
    assert!(published.writer.grant.is_none());
    let replacement = common::connect(&sql_address.to_string(), "shop").await;
    assert_eq!(
        common::values(&replacement, "SELECT total FROM orders WHERE _id = 1").await,
        ["11"]
    );
    let elected = route(&routing).await.writer.grant.unwrap();
    assert_eq!(
        elected.enrollment,
        authority.enrollments[1 - incumbent_index]
    );
    assert!(elected.epoch > incumbent.epoch);
    assert!(directory.deployments().await.unwrap().is_empty());

    assert_eq!(published.checkpoint.cut, pending.checkpoint);
    assert_eq!(published.checkpoint.root, pending.root);
    assert!(published.preparing.is_none());
    assert!(
        allocation_parent
            .join(&allocation_name)
            .join("manifest")
            .is_file()
    );

    replacement.batch_execute(update).await.unwrap();
    assert_eq!(
        common::values(&replacement, "SELECT total FROM orders WHERE _id = 1").await,
        ["11"]
    );
    let conflicting = update.replace("total + 1", "total + 2");
    assert!(replacement.batch_execute(&conflicting).await.is_err());
    assert!(
        old_flight.do_get(ticket).await.is_err(),
        "a ticket from the displaced Flight session cannot be served"
    );
    let mut new_flight = flight_client(flight_address, deployment).await;
    let ticket = new_flight
        .execute("SELECT CAST(total AS BIGINT) FROM orders".into(), None)
        .await
        .unwrap()
        .endpoint
        .into_iter()
        .next()
        .unwrap()
        .ticket
        .unwrap();
    let batches: Vec<_> = new_flight
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    let batch = &batches[0];
    assert_eq!(
        rtdb_columnar::read_value(batch.schema().field(0), batch.column(0).as_ref(), 0).unwrap(),
        rtdb_types::Value::I64(11)
    );

    // Closing the journal root and disposing of its local bytes are distinct
    // effects. An overlapping provider borrow delays the physical action.
    let provider_reader = OpenOptions::new()
        .read(true)
        .open(lifecycle.join("lock"))
        .unwrap();
    provider_reader.try_lock_shared().unwrap();
    replacement
        .simple_query("INSERT INTO orders (_id, total) VALUES (2, 70)")
        .await
        .unwrap();
    nodes[1 - incumbent_index]
        .as_ref()
        .unwrap()
        .agent
        .flush("shop")
        .await
        .unwrap();
    // The restored writer itself holds the old root until its attachment
    // closes. Keep the separate provider reader alive across that release.
    drop(new_flight);
    drop(replacement);
    nodes[1 - incumbent_index]
        .as_ref()
        .unwrap()
        .agent
        .park("shop")
        .await
        .unwrap();
    let custodian = authority.custodian.client(&cohort.route);
    eventually(async || {
        let view = route(&custodian).await;
        view.retirement
            .filter(|old| old.checkpoint == published.checkpoint && old.closed)
    })
    .await;
    assert!(
        allocation_parent
            .join(&allocation_name)
            .join("manifest")
            .is_file()
    );
    drop(provider_reader);
    eventually(async || {
        let marked = fs::read(lifecycle.join("closed"))
            .ok()
            .is_some_and(|bytes| bytes.as_slice() == published.checkpoint.manifest);
        let removed = !allocation_parent.join(&allocation_name).exists();
        let settled = route(&custodian)
            .await
            .retirement
            .is_none_or(|old| old.checkpoint != published.checkpoint);
        (marked && removed && settled).then_some(())
    })
    .await;

    serving.cancel();
    tokio::time::timeout(DEADLINE, sql)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(DEADLINE, flight)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for node in nodes.into_iter().flatten() {
        node.kill().await.unwrap();
    }
    for voter in &mut cohort.voters {
        voter.stop().await;
    }
}
