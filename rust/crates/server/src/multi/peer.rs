use std::{
    collections::BTreeSet,
    net::{IpAddr, SocketAddr},
    sync::{Arc, RwLock},
    time::Duration,
};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_crypt::tokens::{MAX_PEER_MESSAGE_BYTES, TokenAuthority};
use rtdb_node::{
    Clock, MultiWriteAdmission, MultiWriteRoute, Node, SystemClock,
    serving::{ServingDriver, ServingScope},
};
use rtdb_tx::transact::{Receipt, Transact};
use rtdb_types::{
    Anomaly, AnomalyCategory, Result, Timestamp,
    rtdb_v1::{Basis, LabelScope},
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream},
    sync::Semaphore,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{CLOCK_DRIFT_MICROS, MultiRuntime};

pub(super) const PORT: u16 = 3001;
const MAX_FRAME_BYTES: usize = MAX_PEER_MESSAGE_BYTES + 128;
const IN_FLIGHT: usize = 128;
const ADMISSION_MICROS: i64 = 5_000_000;
const TIMEOUT: Duration = Duration::from_secs(5);

pub(super) struct Peer {
    address: IpAddr,
    live: RwLock<BTreeSet<IpAddr>>,
    tokens: TokenAuthority,
    memory: Arc<MemoryBudget>,
    outgoing: Semaphore,
}

struct DatabaseRoute {
    peer: Arc<Peer>,
    database: String,
}

#[derive(Serialize, Deserialize)]
struct Request {
    nonce: Uuid,
    from: IpAddr,
    to: IpAddr,
    expires: Timestamp,
    database: String,
    incarnation: [u8; 16],
    admission: Admission,
    request: Option<Box<RawValue>>,
}

#[derive(Serialize, Deserialize)]
enum Admission {
    Ordinary(Option<Scope>),
    CustodyResponse,
    Catalog,
    Export {
        pin: Uuid,
        basis: Basis,
        until: Option<Timestamp>,
    },
    Restore {
        request: [u8; 32],
    },
    Retire,
}

#[derive(Serialize, Deserialize)]
struct Scope {
    labels: LabelScope,
    namespace: [u8; 32],
}

#[derive(Serialize, Deserialize)]
struct Reply {
    nonce: Uuid,
    result: Result<Answer>,
}

#[derive(Serialize, Deserialize)]
enum Answer {
    Receipt(Receipt),
    Ticket(Vec<u8>),
    Installed(Basis),
    Retired {
        terminal: u64,
        generation: u64,
        lease_until: i64,
    },
}

impl Peer {
    pub(super) fn new(
        address: IpAddr,
        tokens: TokenAuthority,
        memory: Arc<MemoryBudget>,
    ) -> Arc<Self> {
        Arc::new(Self {
            address,
            live: RwLock::new(BTreeSet::new()),
            tokens,
            memory,
            outgoing: Semaphore::new(IN_FLIGHT),
        })
    }

    pub(super) fn observe(&self, live: &BTreeSet<IpAddr>) {
        self.live
            .write()
            .expect("Multi peer discovery")
            .clone_from(live);
    }

    pub(super) fn database(self: &Arc<Self>, name: &str) -> Arc<dyn MultiWriteRoute> {
        Arc::new(DatabaseRoute {
            peer: Arc::clone(self),
            database: name.to_owned(),
        })
    }

    pub(super) async fn serve(
        self: &Arc<Self>,
        runtime: &MultiRuntime,
        node: &Node,
        stop: &CancellationToken,
    ) -> Result<()> {
        let listener = TcpListener::bind(SocketAddr::new(self.address, PORT))
            .await
            .map_err(network)?;
        let mut calls = JoinSet::new();
        stop.run_until_cancelled(async {
            loop {
                tokio::select! {
                    received = listener.accept(), if calls.len() < IN_FLIGHT => {
                        let (stream, remote) = received.map_err(network)?;
                        if !self.live.read().expect("Multi peer discovery").contains(&remote.ip()) {
                            continue;
                        }
                        let runtime = runtime.clone();
                        let peer = Arc::clone(self);
                        let node = node.clone();
                        calls.spawn(async move {
                            let _ = tokio::time::timeout(TIMEOUT, peer.receive(&runtime, &node, stream, remote.ip())).await;
                        });
                    }
                    _ = calls.join_next(), if !calls.is_empty() => (),
                }
            }
        }).await.unwrap_or(Ok(()))
    }

    async fn receive(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        mut stream: TcpStream,
        from: IpAddr,
    ) -> Result<()> {
        let (token, reservation) = read_frame(&mut stream, &self.memory).await?;
        let bytes = self.tokens.open_peer_request(&token).await?;
        let request: Request = serde_json::from_slice(&bytes).map_err(encoding)?;
        let latest = SystemClock.now().0.saturating_add(CLOCK_DRIFT_MICROS);
        if request.from != from
            || request.to != self.address
            || request.nonce.is_nil()
            || request.expires.0 <= latest
            || request.expires.0 > latest.saturating_add(ADMISSION_MICROS)
        {
            return Err(Anomaly::new(
                AnomalyCategory::Forbidden,
                "rtdb/peer-admission",
                "Peer write admission has expired or names a different route",
            ));
        }
        let nonce = request.nonce;
        let result = self.apply(runtime, node, request, reservation).await;
        let reply = serde_json::to_vec(&Reply { nonce, result }).map_err(encoding)?;
        let token = self.tokens.seal_peer_reply(&reply).await?;
        write_frame(&mut stream, &token).await
    }

    async fn apply(
        &self,
        runtime: &MultiRuntime,
        node: &Node,
        frame: Request,
        reservation: MemoryReservation,
    ) -> Result<Answer> {
        let target = self.selected(Uuid::from_bytes(frame.incarnation));
        if target != Some(self.address) {
            return Err(unavailable());
        }
        let incarnation = Uuid::from_bytes(frame.incarnation);
        let name = &frame.database;
        let admitted = match frame.admission {
            Admission::Catalog if name.is_empty() => {
                let request = transaction(frame.request, reservation)?;
                return Ok(Answer::Receipt(
                    runtime
                        .catalog
                        .apply_peer_write(frame.incarnation, &request)
                        .await?,
                ));
            }
            Admission::Export { pin, basis, until } if frame.request.is_none() => {
                let writer = runtime.ready_driver(name, incarnation).await?;
                let ticket = writer
                    .export_checkpoint(pin, basis, until, &CancellationToken::new())
                    .await?;
                return Ok(Answer::Ticket(ticket));
            }
            Admission::Restore { request } if frame.request.is_none() => {
                let basis = runtime
                    .install_peer_transfer(name, incarnation, request)
                    .await?;
                return Ok(Answer::Installed(basis));
            }
            Admission::Retire if frame.request.is_none() => {
                let writer = runtime.ready_driver(name, incarnation).await?;
                let event = writer.retire_deployment().await?;
                return Ok(Answer::Retired {
                    terminal: event.terminal.0,
                    generation: event.generation,
                    lease_until: event.lease_until,
                });
            }
            admitted => admitted,
        };
        let attached = node.catalog().database(&frame.database)?;
        let serving = attached.serving_driver().ok_or_else(unavailable)?;
        if serving.incarnation() != frame.incarnation {
            return Err(unavailable());
        }
        let writer = runtime
            .databases
            .read()
            .expect("Multi serving catalog")
            .get(&frame.database)
            .cloned()
            .ok_or_else(unavailable)?;
        if writer.incarnation() != frame.incarnation {
            return Err(unavailable());
        }
        let request = transaction(frame.request, reservation)?;
        let receipt = match admitted {
            Admission::Ordinary(scope) => {
                writer
                    .transact_local(
                        request,
                        scope.map(|scope| ServingScope {
                            labels: scope.labels,
                            receipt_namespace: scope.namespace,
                        }),
                    )
                    .await
            }
            Admission::CustodyResponse => writer.submit_response_local(request).await,
            _ => Err(unavailable()),
        }?;
        Ok(Answer::Receipt(receipt))
    }

    fn selected(&self, incarnation: Uuid) -> Option<IpAddr> {
        rtdb_db::preferred(
            incarnation,
            &self.live.read().expect("Multi peer discovery"),
        )
    }

    pub(super) async fn forward_catalog(
        &self,
        incarnation: Uuid,
        request: &Transact,
    ) -> Result<Option<Receipt>> {
        self.forward_transaction(
            String::new(),
            *incarnation.as_bytes(),
            request,
            Admission::Catalog,
        )
        .await
    }

    pub(super) async fn forward_export(
        &self,
        name: &str,
        incarnation: Uuid,
        pin: Uuid,
        basis: Basis,
        until: Option<Timestamp>,
    ) -> Result<Option<Vec<u8>>> {
        match self
            .forward(
                name.to_owned(),
                *incarnation.as_bytes(),
                None,
                Admission::Export { pin, basis, until },
            )
            .await?
        {
            Some(Answer::Ticket(ticket)) => Ok(Some(ticket)),
            None => Ok(None),
            _ => Err(unavailable()),
        }
    }

    pub(super) async fn forward_restore(
        &self,
        name: &str,
        incarnation: Uuid,
        request: [u8; 32],
    ) -> Result<Option<Basis>> {
        match self
            .forward(
                name.to_owned(),
                *incarnation.as_bytes(),
                None,
                Admission::Restore { request },
            )
            .await?
        {
            Some(Answer::Installed(basis)) if basis.incarnation() == *incarnation.as_bytes() => {
                Ok(Some(basis))
            }
            None => Ok(None),
            _ => Err(unavailable()),
        }
    }

    pub(super) async fn forward_retirement(
        &self,
        name: &str,
        incarnation: Uuid,
    ) -> Result<Option<rtdb_db::DeploymentRetirement>> {
        match self
            .forward(
                name.to_owned(),
                *incarnation.as_bytes(),
                None,
                Admission::Retire,
            )
            .await?
        {
            Some(Answer::Retired {
                terminal,
                generation,
                lease_until,
            }) => Ok(Some(rtdb_db::DeploymentRetirement {
                incarnation,
                terminal: journal_contract::Lsn(terminal),
                generation,
                lease_until,
            })),
            None => Ok(None),
            _ => Err(unavailable()),
        }
    }

    async fn forward_transaction(
        &self,
        database: String,
        incarnation: [u8; 16],
        request: &Transact,
        admission: Admission,
    ) -> Result<Option<Receipt>> {
        let target = self.selected(Uuid::from_bytes(incarnation));
        if target.is_none_or(|to| to == self.address) {
            return Ok(None);
        }
        match self
            .forward(
                database,
                incarnation,
                Some(request.canonical_request()?),
                admission,
            )
            .await?
        {
            Some(Answer::Receipt(receipt)) => Ok(Some(receipt)),
            None => Ok(None),
            _ => Err(unavailable()),
        }
    }

    async fn forward(
        &self,
        database: String,
        incarnation: [u8; 16],
        request: Option<Box<RawValue>>,
        admission: Admission,
    ) -> Result<Option<Answer>> {
        let Some(to) = self
            .selected(Uuid::from_bytes(incarnation))
            .filter(|to| *to != self.address)
        else {
            return Ok(None);
        };
        let frame = Request {
            nonce: Uuid::new_v4(),
            from: self.address,
            to,
            expires: Timestamp(SystemClock.now().0.saturating_add(ADMISSION_MICROS)),
            database,
            incarnation,
            admission,
            request,
        };
        tokio::time::timeout(TIMEOUT, self.send(frame))
            .await
            .map_err(|_| unavailable())?
            .map(Some)
    }

    async fn send(&self, request: Request) -> Result<Answer> {
        let _permit = self.outgoing.try_acquire().map_err(|_| overloaded())?;
        let encoded = serde_json::to_vec(&request).map_err(encoding)?;
        if encoded.len() > MAX_PEER_MESSAGE_BYTES {
            return Err(overloaded());
        }
        let token = self.tokens.seal_peer_request(&encoded).await?;
        let target = SocketAddr::new(request.to, PORT);
        let socket = if request.to.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        }
        .map_err(network)?;
        socket
            .bind(SocketAddr::new(self.address, 0))
            .map_err(network)?;
        let mut stream = socket.connect(target).await.map_err(network)?;
        write_frame(&mut stream, &token).await?;
        let (reply, _reservation) = read_frame(&mut stream, &self.memory).await?;
        let bytes = self.tokens.open_peer_reply(&reply).await?;
        let reply: Reply = serde_json::from_slice(&bytes).map_err(encoding)?;
        if reply.nonce != request.nonce {
            return Err(unavailable());
        }
        reply.result
    }
}

#[async_trait::async_trait]
impl MultiWriteRoute for DatabaseRoute {
    async fn forward(
        &self,
        incarnation: [u8; 16],
        request: &Transact,
        admitted: MultiWriteAdmission<'_>,
    ) -> Result<Option<Receipt>> {
        let admission = match admitted {
            MultiWriteAdmission::Ordinary(scope) => Admission::Ordinary(scope.map(|scope| Scope {
                labels: scope.labels.clone(),
                namespace: scope.receipt_namespace,
            })),
            MultiWriteAdmission::CustodyResponse => Admission::CustodyResponse,
        };
        self.peer
            .forward_transaction(self.database.clone(), incarnation, request, admission)
            .await
    }
}

fn transaction(raw: Option<Box<RawValue>>, reservation: MemoryReservation) -> Result<Transact> {
    let raw = raw.ok_or_else(unavailable)?;
    let mut request = Transact::decode_canonical_request(raw.get().as_bytes())?;
    request.request_memory = Some(Arc::new(reservation));
    Ok(request)
}

async fn read_frame(
    stream: &mut TcpStream,
    memory: &Arc<MemoryBudget>,
) -> Result<(Vec<u8>, MemoryReservation)> {
    let size = stream.read_u32().await.map_err(network)? as usize;
    if !(1..=MAX_FRAME_BYTES).contains(&size) {
        return Err(overloaded());
    }
    let reservation = memory.reserve(size * 32 + 4096)?;
    let mut token = vec![0; size];
    stream.read_exact(&mut token).await.map_err(network)?;
    Ok((token, reservation))
}

async fn write_frame(stream: &mut TcpStream, token: &[u8]) -> Result<()> {
    if !(1..=MAX_FRAME_BYTES).contains(&token.len()) {
        return Err(overloaded());
    }
    stream
        .write_u32(token.len() as u32)
        .await
        .map_err(network)?;
    stream.write_all(token).await.map_err(network)
}

fn encoding(error: serde_json::Error) -> Anomaly {
    Anomaly::incorrect("rtdb/peer-encoding", error.to_string())
}

fn network(_: std::io::Error) -> Anomaly {
    unavailable()
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/peer-unavailable",
        "The selected writer peer is unavailable",
    )
}

fn overloaded() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Busy,
        "rtdb/peer-admission",
        "The peer write is outside the supported admission budget",
    )
}
