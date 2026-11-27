//! Acceptance through the installed journal daemon and RTDB's ordinary clients.

mod common;
#[allow(dead_code)]
#[path = "../../kafka/tests/support/kafka_tls.rs"]
mod tls_fixture;

use std::fs::{self, OpenOptions};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use ordered_logkeeper::ordered::daemon::{
    ApplicationRoute, Client, Credentials, EndpointAddress, Manifest, SuccessorRoute,
};
use ordered_logkeeper::ordered::service::{
    Route, WithdrawnWorker,
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
use rtdb_multi::{admin, control, flight_gateway, process};
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

fn ordered_cli(command: &str, manifest: &Path, args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_rtdb-multi"))
        .arg(command)
        .arg("--manifest")
        .arg(manifest)
        .args(args)
        .output()
        .unwrap()
}

fn ordered_cli_ok(command: &str, manifest: &Path, args: &[&str]) -> Vec<u8> {
    let output = ordered_cli(command, manifest, args);
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    output.stdout
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
                "serving_lease": 60_000,
                "clock_drift": 1_000,
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
    let stream = endpoint.connect().await.unwrap();
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
            references: None,
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
async fn custodian_disposes_expired_withdrawn_worker_before_trim() {
    withdrawn_worker_disposal_and_trim(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn withdrawn_worker_hold_blocks_trim_past_the_deadline_without_disposal() {
    withdrawn_worker_disposal_and_trim(false).await;
}

async fn withdrawn_worker_disposal_and_trim(automatic_disposal: bool) {
    let deployment = DeploymentRecord::new("shop", "acceptance", Vec::new(), now_ms());
    let mut authority = Authority::new(deployment.id.parse().unwrap(), 4096, 1024, 1);
    authority.group_spec["limits"]["serving_lease"] = json!(3_000);
    authority.group_spec["limits"]["clock_drift"] = json!(5_000);
    let cohort = authority.start("disposal").await;
    let administrator = authority.administrator.client(&cohort.route);
    let custodian = authority.custodian.client(&cohort.route);
    let worker = authority.workers[0].client(&cohort.route);
    let enrollment = authority.enrollments[0];
    let principal = Principal::Worker(enrollment);
    let (grant, recovery) = grant(&worker, enrollment).await;
    release(&worker, recovery).await;

    let payload = Bytes::from_static(b"pinned by a withdrawn reader");
    let record = execute(
        &worker,
        append(route(&worker).await, grant, AppendClass::Data, payload),
    )
    .await;
    let Outcome::Appended(record) = record.receipt.outcome.unwrap() else {
        panic!("the test record was not appended");
    };
    let read = execute(
        &worker,
        request(route(&worker).await, principal, Command::Barrier),
    )
    .await;
    let Outcome::Barrier { read, .. } = read.receipt.outcome.unwrap() else {
        panic!("the worker did not acquire a read hold");
    };
    assert_eq!(read.owner, enrollment);
    assert_eq!(read.through, record.cut);

    let initial = route(&custodian).await;
    let selected = Checkpoint {
        cut: record.cut,
        root: [19; 32],
        manifest: [23; 32],
        custody: Custody {
            allocation: Uuid::new_v4(),
            revision: 2,
            provider: [29; 32],
        },
    };
    let preparation = CheckpointPreparation {
        erasure: false,
        checkpoint: selected.cut,
        root: selected.root,
        source: grant,
        base: initial.checkpoint,
        custody: selected.custody,
        usable_bytes: 64 << 20,
        descriptor: [31; 32],
    };
    let custody = async |command| {
        execute(
            &custodian,
            request(route(&custodian).await, Principal::Custodian, command),
        )
        .await
        .receipt
        .outcome
    };
    let lease = execute(
        &worker,
        request(route(&worker).await, principal, Command::Lease { grant }),
    )
    .await;
    let Outcome::Leased { lease_end, .. } = lease.receipt.outcome.unwrap() else {
        panic!("the worker did not acquire its serving lease");
    };
    assert_eq!(
        execute(
            &administrator,
            request(
                route(&administrator).await,
                Principal::Administrator,
                Command::Withdraw { enrollment },
            ),
        )
        .await
        .receipt
        .outcome,
        Ok(Outcome::Withdrawn),
    );
    let deadline = lease_end + initial.clock_drift;
    let withdrawn = route(&custodian).await;
    assert!(withdrawn.writer.grant.is_none());
    assert!(withdrawn.time < lease_end);
    let service = if automatic_disposal {
        Some(start_disposal_custodian(&authority, &cohort, deployment).await)
    } else {
        None
    };
    let pending = async || {
        let Reply::WithdrawnWorkers(workers) = custodian
            .call(Operation::WithdrawnWorkers { after: None })
            .await
            .unwrap()
        else {
            panic!("the journal did not report withdrawn hold owners");
        };
        workers
    };
    let expected_worker = WithdrawnWorker {
        enrollment,
        eligible_at: Some(deadline),
    };
    let wait_until = async |time: u64| {
        tokio::time::sleep(Duration::from_millis(
            time.saturating_sub(now_ms().try_into().unwrap()),
        ))
        .await;
    };
    wait_until(lease_end).await;
    assert_eq!(pending().await, [expected_worker]);
    let idle = route(&custodian).await;
    assert_eq!(idle.position, withdrawn.position);
    assert_eq!(idle.time, withdrawn.time);
    assert_eq!(idle.floor, initial.floor);

    if automatic_disposal {
        tokio::time::timeout(DEADLINE, async {
            while !pending().await.is_empty() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("the custodian did not dispose the expired enrollment on the idle journal");
        let disposed = route(&custodian).await;
        assert!(disposed.time >= deadline);
        assert_eq!(disposed.position, idle.position + 1);
        assert_eq!(disposed.checkpoint, initial.checkpoint);
        assert_eq!(disposed.floor, initial.floor);
    } else {
        wait_until(deadline + 250).await;
        assert_eq!(pending().await, [expected_worker]);
        assert_eq!(route(&custodian).await.time, idle.time);
    }

    assert_eq!(
        custody(Command::PrepareCheckpoint { preparation, references: None }).await,
        Ok(Outcome::CheckpointPrepared(preparation)),
    );
    assert_eq!(
        custody(Command::Publish {
            checkpoint: selected
        })
        .await,
        Ok(Outcome::Published(selected)),
    );
    if automatic_disposal {
        eventually(async || (route(&custodian).await.floor == selected.cut).then_some(())).await;
    } else {
        let trim = Command::Trim {
            selected,
            through: selected.cut,
        };
        assert_eq!(custody(trim).await, Err(Refusal::Unavailable));
        let blocked = route(&custodian).await;
        assert!(blocked.time > deadline);
        assert_eq!(blocked.floor, initial.floor);
        assert_eq!(pending().await, [expected_worker]);
    }
    if let Some((cancel, service)) = service {
        cancel.cancel();
        tokio::time::timeout(DEADLINE, service).await.unwrap().unwrap().unwrap();
    }
}

async fn start_disposal_custodian(
    authority: &Authority,
    cohort: &Cohort,
    deployment: DeploymentRecord,
) -> (CancellationToken, JoinHandle<rtdb_types::Result<()>>) {
    use rtdb_multi::journal::hosted::custodian;

    let root = authority.data.path();
    let keys = Arc::new(LocalKeyService::new(root.join("keys")).unwrap());
    let storage = CellStorage::Local { root: root.join("serving") };
    let archive = CellStorage::Local { root: root.join("archive") };
    admin::initialize_storage(&storage, keys.as_ref()).await.unwrap();
    admin::initialize_storage(&archive, keys.as_ref()).await.unwrap();
    let installed = |identity: &Identity, role: Value| {
        json!({
            "incarnation": deployment.incarnation,
            "log_epoch": deployment.log_epoch,
            "authority": role,
            "route": cohort.route,
            "credentials": identity.credentials,
        })
    };
    let standby = authority.enrollments[1];
    let manifest = root.join("custodian.json");
    write_json(&manifest, &json!([{
        "deployment": deployment,
        "worker": installed(&authority.workers[1], json!({"kind": "worker", "enrollment": standby})),
        "custody": installed(&authority.custodian, json!({"kind": "custody"})),
        "endpoints": [{ "enrollment": standby, "control": "127.0.0.1:1", "pgwire": "127.0.0.1:2" }],
        "archive": archive,
        "archive_prefix": "checkpoints",
    }]));
    let cancel = CancellationToken::new();
    let service = tokio::spawn(custodian::run(custodian::Settings {
        placements: Some(manifest),
        discovery: None,
        serving: storage,
        archive,
        keys,
        broker: None,
        memory: rtdb_columnar::MemoryBudget::new(1 << 30),
    }, cancel.clone()));
    (cancel, service)
}

async fn start_disposal_node(
    authority: &Authority,
    cohort: &Cohort,
    deployment: DeploymentRecord,
    automatic_disposal: bool,
) -> process::RunningNode {
    let root = authority.data.path();
    let keys = Arc::new(LocalKeyService::new(root.join("keys")).unwrap());
    let storage = CellStorage::Local {
        root: root.join("serving"),
    };
    let archive = CellStorage::Local {
        root: root.join("archive"),
    };
    admin::initialize_storage(&storage, keys.as_ref())
        .await
        .unwrap();
    admin::initialize_storage(&archive, keys.as_ref())
        .await
        .unwrap();
    let pgwire = reserve_port();
    let control = reserve_port();
    let pgwire_address = pgwire.local_addr().unwrap();
    let control_address = control.local_addr().unwrap();
    let installed = |identity: &Identity, role: Value| {
        json!({
            "incarnation": deployment.incarnation,
            "log_epoch": deployment.log_epoch,
            "authority": role,
            "route": cohort.route,
            "credentials": identity.credentials,
        })
    };
    let standby = authority.enrollments[1];
    let manifest = root.join("disposal-node.json");
    write_json(
        &manifest,
        &json!([{
            "deployment": deployment,
            "worker": installed(&authority.workers[1], json!({"kind": "worker", "enrollment": standby})),
            "custody": automatic_disposal.then(|| installed(&authority.custodian, json!({"kind": "custody"}))),
            "endpoints": [{ "enrollment": standby, "control": control_address, "pgwire": pgwire_address }],
            "archive": archive,
            "archive_prefix": "checkpoints",
        }]),
    );
    let pgwire_port = pgwire_address.port();
    let configuration = rtdb_server::config::NodeConfiguration::from_yaml(&format!(
        "authn: !SingleRootUser\n  password: cell-test\nserver:\n  port: {pgwire_port}\n  numThreads: 4\nflightSql: {{port: 0, ownedNativeActions: true}}\n",
    )).unwrap();
    let mut agent = AgentOptions::new(storage);
    agent.installed_journal_manifest = Some(manifest);
    agent.watch_interval = Duration::from_millis(50);
    let settings = process::NodeSettings {
        configuration,
        allocation_limit_bytes: 1 << 30,
        control: control_address,
        advertise_host: Some("127.0.0.1".into()),
        hosted_task: None,
        availability_zone: "custody-zone".into(),
        engine_version: None,
        agent,
        lease: common::lease(),
        pool: MemberPool::General,
        claim_wait: Duration::from_millis(2),
        keys,
        cache_dir: root.join("cache"),
        local_compaction: false,
        capabilities: Some(common::secret()), classic_retained_flight_key: None,
    };
    drop(pgwire);
    drop(control);
    process::start_node(MemoryDirectory::new(), settings)
        .await
        .unwrap()
}

#[test]
fn installed_owned_reads_hold_their_cut_and_retry_a_discarded_reply() {
    std::thread::Builder::new().stack_size(rtdb_node::WORKER_STACK_BYTES).spawn(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_stack_size(rtdb_node::WORKER_STACK_BYTES)
            .enable_all()
            .build()
            .unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(async {
                tokio::time::timeout(DEADLINE, exercise_installed_owned_reads()).await.unwrap();
            });
        }));
        runtime.shutdown_timeout(DEADLINE);
        if let Err(error) = result { std::panic::resume_unwind(error); }
    }).unwrap().join().unwrap();
}

async fn exercise_installed_owned_reads() {
    use arrow_flight::{Action, flight_service_client::FlightServiceClient};
    use tonic::{Code, transport::Channel};

    struct Reader;
    #[async_trait::async_trait]
    impl rtdb_node::SessionIdentity for Reader {
        fn user(&self) -> Option<&str> { Some("xtdb") }
        async fn revalidate(&self, _: rtdb_types::Timestamp) -> rtdb_types::Result<()> { Ok(()) }
        fn authorize_database(&self, _: &str, _: rtdb_node::DatabaseAccess) -> rtdb_types::Result<()> { Ok(()) }
    }

    let record = DeploymentRecord::new("shop", "acceptance", Vec::new(), now_ms());
    let deployment = record.id.parse().unwrap();
    let authority = Authority::new(deployment, 1 << 20, 24 << 20, 16);
    let mut cohort = authority.start("owned-reads").await;
    let keys = LocalKeyService::new(authority.data.path().join("keys")).unwrap();
    keys.key(&record.key_id()).await.unwrap();
    keys.provision_journal_key(rtdb_multi::keys::JournalKey::new(authority.placement, 1)).await.unwrap();
    let node = start_disposal_node(&authority, &cohort, record.clone(), false).await;
    let manifest = authority.data.path().join("owned-gateway.json");
    write_json(&manifest, &json!([{
        "name": "shop",
        "deployment": deployment,
        "journal": {
            "incarnation": record.incarnation, "log_epoch": record.log_epoch,
            "authority": {"kind": "routing"}, "route": cohort.route,
            "credentials": authority.router.credentials,
        },
        "endpoints": [{
            "enrollment": authority.enrollments[1],
            "control": node.control_address, "pgwire": node.pgwire_address,
        }],
    }]));
    let options = GatewayOptions {
        capabilities: Some(common::secret()),
        installed_journals: Some(Arc::new(GatewayRoutes::read(&manifest).unwrap())),
        ..Default::default()
    };
    let directory: Arc<dyn Directory> = MemoryDirectory::new();
    let stop = CancellationToken::new();
    let flight = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let flight_address = flight.local_addr().unwrap();
    let flight = tokio::spawn(flight_gateway::serve(flight, directory, options, stop.clone()));

    let session = flight_client(flight_address, deployment).await;
    let channel = Channel::from_shared(format!("http://{flight_address}")).unwrap().connect().await.unwrap();
    let mut client = FlightServiceClient::new(channel);
    let action = |name: &str, body: Vec<u8>| {
        let mut request = tonic::Request::new(Action { r#type: name.to_owned(), body: body.into() });
        request.metadata_mut().insert("authorization", format!("Bearer {}", session.token().unwrap()).parse().unwrap());
        request
    };
    let open = client.do_action(action("rtdb.owned.OpenRead.v3", vec![])).await.unwrap()
        .into_inner().message().await.unwrap().unwrap().body;
    assert_eq!(open.len(), 40);
    let mut fresh = deployment.as_bytes().to_vec();
    fresh.push(0);
    let lost = client.do_action(action("rtdb.owned.OpenFreshRead.v2", fresh.clone())).await.unwrap();
    drop(lost);
    let retained = client.do_action(action("rtdb.owned.OpenFreshRead.v2", fresh)).await.unwrap()
        .into_inner().message().await.unwrap().unwrap().body;
    assert_ne!(&open[..16], &retained[..16]);
    assert_eq!(&open[16..32], &retained[16..32]);

    let mut floor = deployment.as_bytes().to_vec();
    floor.push(1);
    floor.extend_from_slice(&retained[24..32]);
    floor.extend_from_slice(&retained[16..24]);
    floor.extend_from_slice(&0_u64.to_be_bytes());
    let applied = client.do_action(action("rtdb.owned.OpenAppliedRead.v5", floor)).await.unwrap()
        .into_inner().message().await.unwrap().unwrap().body;
    assert_eq!(&applied[16..32], &retained[16..32]);
    let mut usage = retained[..16].to_vec();
    usage.extend_from_slice(&1_u16.to_be_bytes());
    usage.extend_from_slice(&6_u16.to_be_bytes());
    usage.extend_from_slice(b"orders");
    let usage = client.do_action(action("rtdb.owned.ReadHeldStorageUsage.v1", usage)).await.unwrap()
        .into_inner().message().await.unwrap().unwrap();
    assert!(!usage.body.is_empty());
    let mut closed = client.do_action(action("rtdb.owned.CloseRead.v1", open[..16].to_vec())).await.unwrap().into_inner();
    assert!(closed.message().await.unwrap().is_none());
    let error = match client.do_action(action("rtdb.owned.OpenWorldRead.v6", Uuid::new_v4().as_bytes().to_vec())).await {
        Ok(reply) => reply.into_inner().message().await.unwrap_err(),
        Err(error) => error,
    };
    assert_eq!(error.code(), Code::Unimplemented);
    assert!(String::from_utf8_lossy(&error.metadata().get_bin("xtdb-anomaly-bin").unwrap().to_bytes().unwrap())
        .contains("rtdb.owned/ordered-world-unheld"));
    let mut closed = client.do_action(action("CloseSession", vec![])).await.unwrap().into_inner();
    assert!(closed.message().await.unwrap().is_some());

    let local = node.server.node();
    let tx = local.latest_completed_transactions().unwrap()["shop"][0].unwrap();
    let floor = rtdb_node::AppliedReadFloor::new(deployment, vec![tx]).unwrap();
    let mut session = local.session("shop", Arc::new(Reader)).unwrap();
    let original = session.assertion_family_reader().unwrap();
    let cancellation = session.state().cancellation();
    let read = original.current_covering(&floor, cancellation).await.unwrap();
    assert!(tx.at_or_before(read.applied_source().unwrap()));
    read.require_unretired_family_source().await.unwrap();
    let other_revision = rtdb_types::TransactionKey {
        system_time: rtdb_types::Timestamp(tx.system_time.0 - 1), ..tx
    };
    let wrong_cut = rtdb_node::AppliedReadFloor::new(deployment, vec![other_revision]).unwrap();
    assert!(original.current_covering(&wrong_cut, cancellation).await.is_err());
    let wrong_history = rtdb_node::AppliedReadFloor::new(Uuid::new_v4(), vec![tx]).unwrap();
    assert!(original.current_covering(&wrong_history, cancellation).await.is_err());
    drop(read);
    let sql = |sql: &str| rtdb_tx::TxOp::Sql { sql: sql.into(), args: None };
    let declaration = session.state_mut().execute_tx(vec![
        sql("CREATE TABLE bj_rtdb_restore_hold (_id TEXT)"),
    ], Default::default()).await.unwrap();
    assert!(declaration.committed(), "{:?}", declaration.error());
    original.current(deployment, session.state().cancellation()).await.unwrap()
        .require_unretired_family_source().await.unwrap();
    let fence = session.state_mut().execute_tx(vec![sql(
        "INSERT INTO bj_rtdb_restore_hold (_id, _valid_from) VALUES \
         ('archive-source-fence.v1', TIMESTAMP WITH TIME ZONE '1970-01-01 00:00:00+00:00')",
    )], Default::default()).await.unwrap();
    assert!(fence.committed(), "{:?}", fence.error());
    assert!(original.current(deployment, session.state().cancellation()).await.unwrap()
        .require_unretired_family_source().await.is_err());
    drop(session);

    stop.cancel();
    flight.await.unwrap().unwrap();
    node.shutdown().await.unwrap();
    for voter in &mut cohort.voters { voter.stop().await; }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_cli_reuses_worker_roster_capacity_after_exact_withdrawal() {
    let authority = Authority::new(Uuid::new_v4(), 4096, 1024, 1);
    let mut cohort = authority.start("churn").await;
    let control = authority.data.path().join("worker-control.json");
    write_json(&control, &json!({
        "credentials": authority.administrator.credentials,
        "route": cohort.route,
    }));

    let withdraw = |enrollment: Enrollment| {
        ordered_cli_ok("ordered-withdraw", &control, &[
            "--worker", &enrollment.worker.to_string(), "--serial", &enrollment.serial.to_string(),
        ]);
    };
    let worker = Uuid::new_v4();
    let blocked = ordered_cli("ordered-enroll", &control, &["--worker", &worker.to_string()]);
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("Capacity"));
    withdraw(authority.enrollments[0]);

    let mut previous = authority.enrollments[1].serial;
    for _ in 0..8 {
        let worker = Uuid::new_v4();
        let enrollment: Enrollment = serde_json::from_slice(&ordered_cli_ok(
            "ordered-enroll", &control, &["--worker", &worker.to_string()],
        )).unwrap();
        assert_eq!(enrollment.worker, worker);
        assert!(enrollment.serial > previous);
        previous = enrollment.serial;
        withdraw(enrollment);
        let disposed: Enrollment = serde_json::from_slice(&ordered_cli_ok(
            "ordered-dispose-worker", &control, &[
                "--worker", &worker.to_string(), "--serial", &enrollment.serial.to_string(),
                "--disposition", &"01".repeat(32),
            ],
        )).unwrap();
        assert_eq!(disposed, enrollment);
    }
    for voter in &mut cohort.voters {
        voter.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_cli_repairs_the_cohort_after_reconciling_lost_appends_and_reads() {
    const ORDINARY: u64 = 4096;
    let authority = Authority::new(Uuid::new_v4(), ORDINARY, 1024, 1);
    let mut cohort = authority.start("initial").await;
    let deployment = authority.placement.deployment.to_string();
    let installed: ApplicationRoute = serde_json::from_slice(&ordered_cli_ok(
        "ordered-route", &cohort.voters[0].manifest, &["--deployment", &deployment],
    )).unwrap();
    assert_eq!(installed.placement, authority.placement);
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

    // A read whose reply is lost is never replayed: its exact resend conflicts,
    // the reader releases it by identity, and a fresh read admits its own hold.
    let lost_request = request(
        route(&old).await,
        Principal::Worker(incumbent.enrollment),
        Command::Barrier,
    );
    discard_reply(&authority.workers[0], &installed, lost_request.clone()).await;
    let resent = execute(&old, lost_request.clone()).await;
    assert_eq!(resent.receipt.outcome, Err(Refusal::Conflict));
    assert!(resent.barrier.is_none());
    let released = execute(
        &old,
        request(
            route(&old).await,
            Principal::Worker(incumbent.enrollment),
            Command::ReleaseRead {
                read: lost_request.id,
            },
        ),
    )
    .await;
    assert_eq!(released.receipt.outcome, Ok(Outcome::Released));
    let read_request = request(
        route(&old).await,
        Principal::Worker(incumbent.enrollment),
        Command::Barrier,
    );
    let admitted = execute(&old, read_request.clone()).await;
    let Outcome::Barrier { read, .. } = admitted.receipt.outcome.unwrap() else {
        panic!("the fresh read was not admitted after releasing the lost one");
    };
    assert_eq!(read.id, read_request.id);
    assert_eq!(read.through, appended.cut);
    assert!(admitted.barrier.is_some());

    let EndpointAddress::Socket(failed_voter) = installed.endpoints[0].address else { panic!("test voter must use a socket address") };
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
    assert_eq!(pressure.last_maintenance, Some(maintained.cut));
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
    let inspect = Operation::InspectCheckpoint {
        base: view.checkpoint,
        from: view.checkpoint.cut,
        through: maintained.cut,
        maximum_bytes: ORDINARY + 1024,
    };
    assert!(matches!(
        next.call(inspect.clone()).await.unwrap(),
        Reply::Refused(Refusal::Unauthorized)
    ));
    let Reply::Replay(candidate) = custody.call(inspect).await.unwrap() else {
        panic!("the custodian cannot discover the retained chosen input");
    };
    assert_eq!(candidate.through, maintained.cut);
    assert_eq!(
        candidate
            .records
            .iter()
            .map(|row| row.result)
            .collect::<Vec<_>>(),
        [appended, maintained]
    );
    let preparation = CheckpointPreparation {
        erasure: false,
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
                Command::PrepareCheckpoint { preparation, references: None }
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
            ordered_cli_ok("ordered-reserve-repair", &voter.manifest,
                &["--deployment", &deployment, "--output", ticket.to_str().unwrap()]);
            ticket
        })
        .collect();
    let source = cohort
        .voters
        .iter()
        .find(|voter| voter.address != failed_voter)
        .unwrap();
    let package = authority.data.path().join("sealed-cohort");
    let seal = |tickets| ordered_cli("ordered-seal-repair", &source.manifest, &[
        "--destination", successor.group.to_str().unwrap(),
        "--output", package.to_str().unwrap(), "--tickets", tickets,
    ]);
    assert!(!seal(tickets[0].to_str().unwrap()).status.success());
    assert_eq!(route(&next).await.head, selected.head);
    let tickets = tickets.iter().map(|ticket| ticket.to_str().unwrap()).collect::<Vec<_>>().join(",");
    let sealed = seal(&tickets);
    assert!(sealed.status.success(), "{}", String::from_utf8_lossy(&sealed.stderr));
    assert!(matches!(
        next.call(Operation::Route).await.unwrap(),
        Reply::Refused(Refusal::CohortSealed)
    ));
    for voter in &mut successor.voters {
        ordered_cli_ok("ordered-install-repair", &voter.manifest,
            &["--deployment", &deployment, "--package", package.to_str().unwrap()]);
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
    recover_after_writer_loss(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_custodian_discovers_the_chosen_upload_after_writer_loss() {
    recover_after_writer_loss(false).await;
}

async fn recover_after_writer_loss(prepare_before_loss: bool) {
    use futures::TryStreamExt;

    let record = DeploymentRecord::new("shop", "acceptance", Vec::new(), now_ms());
    let deployment: Uuid = record.id.parse().unwrap();
    let authority = Authority::new(deployment, 1 << 20, 24 << 20, 16);
    let mut cohort = authority.start("rtdb").await;
    let routing = authority.router.client(&cohort.route);
    let installed: ApplicationRoute = serde_json::from_value(cohort.route.clone()).unwrap();
    let directory: Arc<dyn Directory> = MemoryDirectory::new();
    if prepare_before_loss {
        directory.create_deployment(DeploymentRecord::new("legacy", "acceptance", Vec::new(), now_ms())).await.unwrap();
    }
    let keys = Arc::new(LocalKeyService::new(authority.data.path().join("keys")).unwrap());
    keys.key(&record.key_id()).await.unwrap();
    keys.provision_journal_key(rtdb_multi::keys::JournalKey::new(installed.placement, 1))
        .await
        .unwrap();
    let serving_root = authority.data.path().join("serving");
    let storage = CellStorage::Local {
        root: serving_root.clone(),
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
    let mut standby = None;
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
                "custody": (prepare_before_loss || index == 1)
                    .then(|| installed_client(&authority.custodian, json!({"kind": "custody"}))),
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
        if prepare_before_loss {
            agent.min_residency = Duration::ZERO;
        }
        let settings = process::NodeSettings {
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
            capabilities: Some(common::secret()), classic_retained_flight_key: None,
        };
        if !prepare_before_loss && index == 1 {
            standby = Some((settings, pgwire, control));
            nodes.push(None);
        } else {
            drop(control);
            drop(pgwire);
            let node = process::start_node(Arc::clone(&directory), settings)
                .await
                .unwrap();
            nodes.push(Some(node));
        }
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

    if prepare_before_loss {
        assert_eq!(admin::set_read_width(directory.as_ref(), "legacy", 2).await.unwrap().read_width, 2);
        assert_eq!(directory.create_deployment(record.clone()).await.unwrap_err().code, "rtdb.directory/exists");
        let node = nodes[incumbent_index].as_ref().unwrap();
        let name = || "shop".to_owned();
        let key = control::ControlKey(Some(common::secret()));
        for request in [
            control::Request::Activate { name: name() },
            control::Request::Subscribe { name: name() },
            control::Request::SubscribeFresh { name: name() },
            control::Request::Snapshot { name: name() },
            control::Request::Flush { name: name() },
            control::Request::Park { name: name() },
            control::Request::PinCheckpointSuffix {
                name: name(), deployment, lease: Uuid::new_v4(), capture: Uuid::new_v4(),
                basis: rtdb_types::TransactionKey { tx_id: rtdb_types::TxId(1), system_time: rtdb_types::Timestamp(1) },
                expires_at_ms: now_ms() + 30_000,
            },
            control::Request::PublishCompaction { name: name(), notification: Vec::new(), grant: common::secret().node_grant("shop") },
        ] {
            let response = control::call(&node.control_address.to_string(), &request, DEADLINE, &key).await.unwrap();
            assert!(matches!(response, control::Response::Refused { ref code, .. } if code == "rtdb.multi/ordered-placement"), "{request:?}: {response:?}");
        }
        let memory = node.server.node().memory();
        let target = memory.limit_bytes() - memory.limit_bytes() / 10;
        let pressure = memory.reserve(target.saturating_sub(memory.used_bytes())).unwrap();
        node.agent.reconcile().await;
        assert_eq!(node.agent.held().get("shop"), Some(&incumbent.epoch));
        drop(pressure);
        assert_eq!(route(&routing).await.writer.grant, Some(incumbent));
    }

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
    let pending = interrupted.preparing;
    if prepare_before_loss {
        let pending = pending.expect("the exact provider intent was ordered before the fault");
        assert_eq!(pending.source, incumbent);
        assert_eq!(pending.custody.allocation, allocation);
    } else {
        assert_eq!(incumbent_index, 0);
        assert!(pending.is_none());
        assert!(interrupted.head.next_lsn > before.head.next_lsn);
    }
    assert_eq!(interrupted.checkpoint, before.checkpoint);
    assert_eq!(interrupted.floor, before.floor);
    assert!(
        !allocation_parent
            .join(&allocation_name)
            .join("manifest")
            .exists()
    );

    let suffix = common::connect(&sql_address.to_string(), "shop").await;
    let large = "x".repeat(1_310_720);
    suffix
        .simple_query(&format!(
            "INSERT INTO suffix_values (_id, body) VALUES (1, '{large}')"
        ))
        .await
        .unwrap();
    drop(suffix);
    let storage_root = rtdb_format::keys::StorageRoot::new(
        rtdb_format::keys::CURRENT_STORAGE_VERSION,
        rtdb_types::StorageEpoch(0),
    )
    .unwrap()
    .key();
    let original_objects = serving_root
        .join(record.storage_prefix())
        .join("~ordered")
        .join(authority.placement.generation.to_string())
        .join(incumbent.epoch.to_string())
        .join(&storage_root)
        .join("oversized");
    assert!(
        fs::read_dir(original_objects)
            .unwrap()
            .any(|entry| entry.unwrap().path().is_file())
    );
    assert!(route(&routing).await.head.next_lsn > interrupted.head.next_lsn);
    let generation_root = serving_root
        .join(record.storage_prefix())
        .join("~ordered")
        .join(authority.placement.generation.to_string());
    let retired_prefix = generation_root.join(incumbent.epoch.to_string());
    assert!(retired_prefix.join(&storage_root).join("~grant").is_file());
    {
        let database = original_node.server.node().catalog().database("shop").unwrap();
        let block = database.status().adopted_block.as_ref().unwrap().block_index();
        let root = rtdb_storage::ObjectKey::new(rtdb_format::keys::block_key(block).unwrap()).unwrap();
        let payload = database.object_store().list(Some(&rtdb_storage::ObjectKey::new("oversized").unwrap()))
            .await.unwrap().into_iter().next().unwrap().key;
        for key in [root, payload] {
            database.object_store().delete(&key).await.unwrap();
            database.object_store().delete(&key).await.unwrap();
            let physical = retired_prefix.join(&storage_root).join(key.as_str());
            assert!(physical.is_file(), "delete must preserve the selectable closure object {key}");
            let name = physical.strip_prefix(&serving_root).unwrap().to_str().unwrap();
            let matching: Vec<Value> = fs::read_dir(retired_prefix.join("~reclaim/v1")).unwrap()
                .map(|entry| serde_json::from_slice(&fs::read(entry.unwrap().path()).unwrap()).unwrap())
                .filter(|candidate: &Value| candidate["key"] == name)
                .collect();
            assert_eq!(matching.len(), 1);
            assert_eq!(matching[0]["grant"]["epoch"], json!(incumbent.epoch));
        }
    }
    // An overlapping reader of the incumbent's prefix, as a zombie process
    // would hold it, keeps the prefix through every other piece of evidence.
    let zombie = OpenOptions::new()
        .read(true)
        .open(generation_root.join(format!("{}.lifecycle", incumbent.epoch)).join("lock"))
        .unwrap();
    zombie.try_lock_shared().unwrap();

    let EndpointAddress::Socket(unavailable) = installed.endpoints[0].address else { panic!("test voter must use a socket address") };
    cohort
        .voters
        .iter_mut()
        .find(|voter| voter.address == unavailable)
        .unwrap()
        .stop()
        .await;
    nodes[incumbent_index].take().unwrap().kill().await.unwrap();
    let view = route(&routing).await;
    let old_writer = authority.workers[incumbent_index].client(&cohort.route);
    let stranded = if prepare_before_loss {
        if view.writer.grant == Some(incumbent) {
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
        None
    } else {
        let strand = match view.writer.grant {
            Some(strand) => strand,
            None => {
                let (strand, held) = grant(&old_writer, incumbent.enrollment).await;
                release(&old_writer, held).await;
                strand
            }
        };
        let (settings, pgwire, control) = standby.take().unwrap();
        drop(pgwire);
        drop(control);
        nodes[1] = Some(
            process::start_node(Arc::clone(&directory), settings)
                .await
                .unwrap(),
        );
        Some(strand)
    };
    assert_eq!(route(&routing).await.writer.grant, stranded);
    drop(blocker);

    let published = eventually(async || {
        let view = route(&routing).await;
        (view.checkpoint.custody.allocation == allocation
            && view.floor == view.checkpoint.cut
            && view.floor != before.floor)
            .then_some(view)
    })
    .await;
    assert_eq!(published.writer.grant, stranded);
    let replacement = common::connect(&sql_address.to_string(), "shop").await;
    assert_eq!(
        common::values(&replacement, "SELECT total FROM orders WHERE _id = 1").await,
        ["11"]
    );
    assert_eq!(
        common::values(
            &replacement,
            "SELECT octet_length(body) FROM suffix_values WHERE _id = 1"
        )
        .await,
        ["1310720"]
    );
    let elected = route(&routing).await.writer.grant.unwrap();
    assert_eq!(
        elected.enrollment,
        authority.enrollments[1 - incumbent_index]
    );
    assert!(elected.epoch > incumbent.epoch);
    let remaining = directory.deployments().await.unwrap();
    assert_eq!(remaining.len(), prepare_before_loss as usize);
    assert!(remaining.iter().all(|record| record.name == "legacy"));

    if let Some(pending) = pending {
        assert_eq!(published.checkpoint.cut, pending.checkpoint);
        assert_eq!(published.checkpoint.root, pending.root);
    } else {
        assert!(published.checkpoint.cut.next_lsn < interrupted.head.next_lsn);
        assert_ne!(published.checkpoint.root, [0; 32]);
    }
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

    let covered = eventually(async || {
        let view = route(&custodian).await;
        (view.floor == view.checkpoint.cut && view.checkpoint.cut.next_lsn >= published.head.next_lsn)
            .then_some(view)
    })
    .await;
    assert!(covered.writer.epoch > incumbent.epoch);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        retired_prefix.join(&storage_root).join("~grant").is_file(),
        "a held prefix is retained after the journal no longer needs it"
    );
    drop(zombie);
    let lifecycle = generation_root.join(format!("{}.lifecycle", incumbent.epoch));
    let receipt = generation_root.join("~retired").join(format!("{}.settled", incumbent.epoch));
    eventually(async || (!retired_prefix.exists() && lifecycle.join("closed").is_file() && receipt.is_file()).then_some(())).await;
    let settled: Value = serde_json::from_slice(&fs::read(&receipt).unwrap()).unwrap();
    assert_eq!(settled["intent"]["grant"]["epoch"], json!(incumbent.epoch));
    assert_eq!(settled["intent"]["grant"]["worker"], json!(incumbent.enrollment.worker));
    assert_eq!(settled["intent"]["inventory"], json!("local_directory"));
    let successor = generation_root.join(elected.epoch.to_string());
    assert!(successor.join(&storage_root).join("~grant").is_file(), "the current grant's prefix survives");
    let late = OpenOptions::new().read(true).open(lifecycle.join("lock")).unwrap();
    late.try_lock_shared().unwrap();
    assert!(lifecycle.join("closed").is_file(), "a late reader finds the permanent closure");
    drop(late);

    let reopened = common::connect(&sql_address.to_string(), "shop").await;
    assert_eq!(
        common::values(&reopened, "SELECT total FROM orders").await,
        ["11", "70"]
    );
    assert_eq!(
        common::values(&reopened, "SELECT octet_length(body) FROM suffix_values WHERE _id = 1").await,
        ["1310720"]
    );
    drop(reopened);

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

#[test]
fn installed_cell_leaves_a_worker_slot_and_resolves_the_custody_manifest() {
    use rtdb_multi::journal::cell::InstalledCell;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("cell.json");
    let voters: Vec<_> = (1_u8..=3).map(|index| json!({
        "voter": Uuid::from_u128(u128::from(index)),
        "zone": format!("zone-{index}"),
        "endpoint": format!("voter-{index}:5448"),
        "server_name": format!("voter-{index}"),
        "certificate": vec![index; 32],
        "receipt_key": format!("{index:064x}"),
    })).collect();
    let mut definition = json!({
        "cell": "custody-test", "ca": "ca.pem", "archive_root": "archive", "keys": "keys",
        "custody_manifest": "custody.json", "voters": voters,
        "limits": {
            "enrolled_workers": 1, "replay_holds": 8, "retry_positions": 128,
            "retained_data_bytes": 1_048_576, "maintenance_bytes": 262_144,
            "serving_lease": 60_000, "clock_drift": 500,
        },
        "maximum_prefix_bytes": 2_097_152, "maximum_application_frame": FRAME,
        "administrator_pin": vec![4; 32], "route_reader_pin": vec![5; 32],
        "workers": [{
            "worker": Uuid::from_u128(7), "certificate": vec![6; 32],
            "control": "127.0.0.1:5444", "pgwire": "127.0.0.1:5432",
        }],
    });

    write_json(&path, &definition);
    assert!(matches!(InstalledCell::read(&path), Err(error) if error.message.as_deref()
        .is_some_and(|message| message.contains("leave room for one more enrollment"))));

    definition["limits"]["enrolled_workers"] = json!(2);
    write_json(&path, &definition);
    let cell = InstalledCell::read(&path).unwrap();
    assert_eq!(cell.custody_manifest, Some(root.path().join("custody.json")));
}

fn start_runtime_voters(authority: &Authority) -> (Cohort, Value) {
    let mut cohort = authority.cohort("runtime", None);
    let management = &authority.group_spec["clients"];
    for voter in &mut cohort.voters {
        let mut definition: Value = serde_json::from_slice(&fs::read(&voter.manifest).unwrap()).unwrap();
        definition["groups"] = json!([]);
        definition["management"] = management.clone();
        write_json(&voter.manifest, &definition);
        voter.manifest().initialize().unwrap();
        voter.start();
    }
    let group: Value = serde_json::from_slice(&fs::read(&cohort.group).unwrap()).unwrap();
    (cohort, group)
}

fn write_runtime_cell(
    authority: &Authority,
    group: &Value,
    archive_root: &Path,
    keys_dir: &Path,
    control: SocketAddr,
    pgwire: SocketAddr,
) -> PathBuf {
    let root = authority.data.path();
    let voters: Vec<_> = group["voters"].as_array().unwrap().iter().map(|voter| {
        let receipt: Vec<u8> = serde_json::from_value(voter["receipt_key"].clone()).unwrap();
        json!({
            "voter": voter["voter"], "zone": voter["zone"],
            "endpoint": voter["endpoint"]["address"], "server_name": voter["endpoint"]["server_name"],
            "certificate": voter["endpoint"]["certificate"],
            "receipt_key": receipt.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        })
    }).collect();

    let cell_file = root.join("runtime-cell.json");
    let pem = |identity: &Identity| json!({
        "certificate": identity.credentials["certificate"], "key": identity.credentials["key"],
    });
    write_json(&cell_file, &json!({
        "cell": "runtime", "ca": root.join("ca.crt"), "archive_root": archive_root, "keys": keys_dir,
        "limits": group["limits"], "maximum_prefix_bytes": group["maximum_prefix_bytes"],
        "maximum_application_frame": FRAME, "exchange_ms": 2_500, "voters": voters,
        "administrator_pin": authority.administrator.pin, "route_reader_pin": authority.router.pin,
        "custodian_pin": authority.custodian.pin,
        "workers": [{
            "worker": authority.enrollments[0].worker, "certificate": authority.workers[0].pin,
            "control": control, "pgwire": pgwire,
        }],
        "credentials": {
            "administrator": pem(&authority.administrator), "route_reader": pem(&authority.router),
            "worker": pem(&authority.workers[0]), "custodian": pem(&authority.custodian),
        },
    }));
    cell_file
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custodian_discovers_a_published_runtime_journal_and_trims_it() {
    use rtdb_multi::journal::{cell::{InstalledCell, Role}, hosted::custodian};

    let authority = Authority::new(Uuid::new_v4(), 2 << 20, 24 << 20, 8);
    let root = authority.data.path();
    let (mut cohort, group) = start_runtime_voters(&authority);
    let archive_root = root.join("archive");
    let keys_dir = root.join("keys");
    let keys: Arc<dyn KeyService> = Arc::new(LocalKeyService::new(keys_dir.clone()).unwrap());
    let cell_file = write_runtime_cell(&authority, &group, &archive_root, &keys_dir,
        "127.0.0.1:1".parse().unwrap(), "127.0.0.1:2".parse().unwrap());
    let cell = InstalledCell::read(&cell_file).unwrap();
    let directory: Arc<dyn Directory> = MemoryDirectory::new();
    let unpublished = Uuid::new_v4();
    directory.reserve_installed_journal(unpublished).await.unwrap();
    let admin = cell.access(Role::Administrator).unwrap();
    eventually(async || admin.by_deployment(unpublished).await.ok()).await;
    let deployment = authority.placement.deployment;
    admin.install(directory.as_ref(), keys.as_ref(), deployment, "runtime", "test").await.unwrap();
    let selected_route = admin.by_deployment(deployment).await.unwrap().unwrap().route;
    let route_json = serde_json::to_value(selected_route).unwrap();
    let worker = authority.workers[0].client(&route_json);
    let custody = authority.custodian.client(&route_json);
    let (grant, recovery) = grant(&worker, authority.enrollments[0]).await;
    release(&worker, recovery).await;
    let initial = route(&custody).await;
    let appended = execute(&worker, append(initial, grant, AppendClass::Data,
        Bytes::from_static(b"retained by runtime custody"))).await;
    let Outcome::Appended(record) = appended.receipt.outcome.unwrap() else {
        panic!("the runtime journal did not append");
    };
    let chosen = Checkpoint {
        cut: record.cut, root: [19; 32], manifest: [23; 32],
        custody: Custody { allocation: Uuid::new_v4(), revision: 2, provider: [29; 32] },
    };
    let prepare = CheckpointPreparation {
        erasure: false, checkpoint: chosen.cut, root: chosen.root, source: grant,
        base: initial.checkpoint, custody: chosen.custody, usable_bytes: 64 << 20,
        descriptor: [31; 32],
    };
    let mutate = async |command| execute(&custody, request(route(&custody).await,
        Principal::Custodian, command)).await.receipt.outcome;
    assert_eq!(mutate(Command::PrepareCheckpoint { preparation: prepare, references: None }).await,
        Ok(Outcome::CheckpointPrepared(prepare)));
    assert_eq!(mutate(Command::Publish { checkpoint: chosen }).await, Ok(Outcome::Published(chosen)));
    assert_eq!(route(&custody).await.floor, initial.floor);

    let cancel = CancellationToken::new();
    let service = tokio::spawn(custodian::run(custodian::Settings {
        placements: None,
        discovery: Some(custodian::Discovery::new(directory, &cell).unwrap()),
        serving: CellStorage::Local { root: root.join("serving") },
        archive: CellStorage::Local { root: archive_root },
        keys,
        broker: None,
        memory: rtdb_columnar::MemoryBudget::new(1 << 30),
    }, cancel.clone()));
    eventually(async || (route(&custody).await.floor == chosen.cut).then_some(())).await;
    cancel.cancel();
    tokio::time::timeout(DEADLINE, service).await.unwrap().unwrap().unwrap();
    for voter in &mut cohort.voters { voter.stop().await; }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_install_serves_two_tenants_after_voter_and_worker_restart() {
    use rtdb_multi::journal::cell::{InstalledCell, Role};
    use tokio::io::{AsyncBufReadExt, BufReader};

    struct Worker(Option<process::RunningNode>);
    impl Drop for Worker {
        fn drop(&mut self) {
            if let Some(node) = &self.0 {
                node.stop_renewing();
            }
        }
    }

    let authority = Authority::new(Uuid::new_v4(), 2 << 20, 24 << 20, 32);
    let root = authority.data.path();
    let (mut cohort, group) = start_runtime_voters(&authority);

    let storage = CellStorage::Local { root: root.join("runtime-serving") };
    let archive_root = root.join("runtime-archive");
    let archive = CellStorage::Local { root: archive_root.clone() };
    let keys_dir = root.join("runtime-keys");
    let keys: Arc<dyn KeyService> = Arc::new(LocalKeyService::new(keys_dir.clone()).unwrap());
    admin::initialize_storage(&storage, keys.as_ref()).await.unwrap();
    admin::initialize_storage(&archive, keys.as_ref()).await.unwrap();

    let control_port = reserve_port();
    let pgwire_port = reserve_port();
    let control = control_port.local_addr().unwrap();
    let pgwire = pgwire_port.local_addr().unwrap();
    let cell_file = write_runtime_cell(&authority, &group, &archive_root, &keys_dir, control, pgwire);

    let directory: Arc<dyn Directory> = MemoryDirectory::new();
    let cell = InstalledCell::read(&cell_file).unwrap();
    let discovery = Arc::new(cell.access(Role::RouteReader).unwrap());
    let deployments = [Uuid::new_v4(), Uuid::new_v4()];
    eventually(async || discovery.by_deployment(deployments[0]).await.ok()).await;

    let node_settings = || {
        let mut agent = AgentOptions::new(storage.clone());
        agent.installed_cell = Some(cell.clone());
        agent.watch_interval = Duration::from_millis(50);
        process::NodeSettings {
            configuration: rtdb_server::config::NodeConfiguration::from_yaml(&format!(
                "authn: !SingleRootUser\n  password: cell-test\nserver:\n  port: {}\n  numThreads: 4\n",
                pgwire.port(),
            )).unwrap(),
            allocation_limit_bytes: 1 << 30,
            control,
            advertise_host: Some("127.0.0.1".into()),
            hosted_task: Some(rtdb_directory::HostedTask::new(
                "arn:aws:ecs:us-east-1:123456789012:cluster/runtime-test",
                "arn:aws:ecs:us-east-1:123456789012:task/runtime-test/0123456789abcdef0123456789abcdef",
            ).unwrap()),
            availability_zone: "test-az".into(),
            engine_version: None,
            agent,
            lease: common::lease(),
            pool: MemberPool::General,
            claim_wait: Duration::from_millis(2),
            keys: keys.clone(),
            cache_dir: root.join("runtime-cache"),
            local_compaction: false,
            capabilities: Some(common::secret()), classic_retained_flight_key: None,
        }
    };
    drop(control_port);
    drop(pgwire_port);
    let mut node = Worker(Some(process::start_node(directory.clone(), node_settings()).await.unwrap()));

    let lifecycle = Identity::new(root, "lifecycle", &authority.issuer);
    let credentials: Credentials = serde_json::from_value(lifecycle.credentials.clone()).unwrap();
    let provider = Arc::new(tls_ring::default_provider());
    let server = tokio_rustls::rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&TLS13]).unwrap().with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from_pem_file(&credentials.certificate).unwrap()],
            PrivateKeyDer::from_pem_file(&credentials.key).unwrap(),
        ).unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from_pem_file(&credentials.roots).unwrap()).unwrap();
    let connector = TlsConnector::from(Arc::new(ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&TLS13]).unwrap().with_root_certificates(roots).with_no_client_auth()));

    let token = "1".repeat(64);
    let token_file = root.join("installation-token");
    fs::write(&token_file, &token).unwrap();
    let disposal_token = "2".repeat(64);
    let disposal_file = root.join("disposal-token");
    fs::write(&disposal_file, &disposal_token).unwrap();
    let installer = rtdb_multi::install::Installer::from_file(&token_file, Some(&disposal_file), "test".into(), 3).unwrap()
        .with_installed_cell(Arc::new(cell.access(Role::Administrator).unwrap()), keys.clone());
    let installs = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let installs_address = installs.local_addr().unwrap();
    let stop = CancellationToken::new();
    let installation = tokio::spawn(rtdb_multi::install::serve(
        installs, Arc::new(server), directory.clone(), Arc::new(installer), stop.clone(),
    ));
    let lifecycle = async |operation: &str, token: &str, name: &str, deployment: Uuid, receive: bool| {
        let socket = TcpStream::connect(installs_address).await.unwrap();
        let mut stream = connector.connect("localhost".try_into().unwrap(), socket).await.unwrap();
        let mut request = serde_json::to_vec(&json!({
            "operation": operation, "token": token, "name": name, "authority": deployment, "incarnation": 1,
        })).unwrap();
        request.push(b'\n');
        stream.write_all(&request).await.unwrap();
        if !receive { return Value::Null; }
        let mut reply = String::new();
        BufReader::new(stream).read_line(&mut reply).await.unwrap();
        serde_json::from_str::<Value>(&reply).unwrap()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let gateway = tokio::spawn(gateway::serve(listener, directory.clone(), GatewayOptions {
        capabilities: Some(common::secret()),
        installed_cell: Some(discovery.clone()),
        ..Default::default()
    }, stop.clone()));

    lifecycle("install", &token, "alpha", deployments[0], false).await;
    let before = eventually(async || discovery.by_deployment(deployments[0]).await.ok().flatten()).await;
    for (name, deployment) in ["alpha", "bravo"].into_iter().zip(deployments) {
        assert_eq!(lifecycle("install", &token, name, deployment, true).await,
            json!({ "outcome": "installed", "name": name, "authority": deployment }));
    }
    assert_eq!(discovery.by_deployment(deployments[0]).await.unwrap(), Some(before));
    assert!(directory.deployments().await.unwrap().is_empty());
    let empty = Uuid::new_v4();
    assert_eq!(lifecycle("install", &token, "empty", empty, true).await["outcome"], "installed");
    assert_eq!(lifecycle("schedule_disposal", &disposal_token, "empty", empty, true).await["outcome"], "scheduled");
    assert!(discovery.by_deployment(empty).await.unwrap().is_none());
    assert_eq!(lifecycle("schedule_disposal", &disposal_token, "empty", empty, true).await["outcome"], "scheduled");
    assert_eq!(lifecycle("install", &token, "empty", empty, true).await["outcome"], "refused");

    for (name, total) in [("alpha", 12), ("bravo", 34)] {
        let client = common::connect(&address, name).await;
        assert_eq!(common::values(&client, "SELECT 1").await, vec!["1"]);
        client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES (1, {total})"))
            .await.unwrap();
    }
    assert_eq!(lifecycle("schedule_disposal", &disposal_token, "alpha", deployments[0], true).await["outcome"], "deferred");
    node.0.take().unwrap().shutdown().await.unwrap();
    cohort.voters[0].stop().await;
    cohort.voters[0].stop = CancellationToken::new();
    cohort.voters[0].start();
    let voter = cohort.voters[0].address;
    eventually(async || TcpStream::connect(voter).await.ok()).await;
    cohort.voters[1].stop().await;
    let mut replacement = Worker(Some(process::start_node(directory.clone(), node_settings()).await.unwrap()));
    for (name, total) in [("alpha", "12"), ("bravo", "34")] {
        let result = common::connect(&address, name).await.simple_query("SELECT total FROM orders").await.unwrap();
        let values: Vec<_> = result.iter().filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
            _ => None,
        }).collect();
        assert_eq!(values, vec![total]);
    }

    {
        let client = common::connect(&address, "bravo").await;
        client.simple_query("CREATE INDEX orders_total ON orders (total, _id)").await.unwrap();
        let holder = replacement.0.as_ref().unwrap().server.node();
        let database = holder.catalog().database("bravo").unwrap();
        database.flush().await.unwrap();
        holder.sync_all(DEADLINE, &CancellationToken::new()).await.unwrap();
        database.build_artefacts().await.unwrap();
        let prefix = rtdb_storage::ObjectKey::new(rtdb_format::keys::ORDERED_INDEX_ROOT).unwrap();
        let store = database.object_store();
        let manifest = store.list(Some(&prefix)).await.unwrap().into_iter()
            .find(|object| object.key.as_str().contains("/manifests/")).unwrap();
        assert!(store.get_fresh(&manifest.key).await.unwrap().starts_with(&[0xff; 4]));
    }

    replacement.0.take().unwrap().shutdown().await.unwrap();
    stop.cancel();
    gateway.await.unwrap().unwrap();
    installation.await.unwrap().unwrap();
    for voter in &mut cohort.voters { voter.stop().await; }
}
