#![allow(dead_code)]

use bytes::Bytes;
use prost::Message;
use rtdb_logkeeper::{JournalEndpoint, PreparedJournal, ServerOptions, StreamConfig};
use rtdb_storage::log::{Log, LogMetadata};
use rtdb_types::Timestamp;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct Server {
    pub address: SocketAddr,
    pub data_dir: PathBuf,
    cancellation: CancellationToken,
    task: JoinHandle<rtdb_types::Result<()>>,
}

impl Server {
    pub async fn start(data_dir: &Path) -> Self { Self::start_at(data_dir, "127.0.0.1:0".parse().unwrap()).await }

    pub async fn start_at(data_dir: &Path, address: SocketAddr) -> Self { Self::start_with(data_dir, address, ServerOptions::DEFAULT_CACHE_BYTES).await }

    pub async fn start_with(data_dir: &Path, address: SocketAddr, cache_bytes: usize) -> Self {
        Self::start_with_durability(data_dir, address, cache_bytes, true).await
    }

    pub async fn start_with_durability(data_dir: &Path, address: SocketAddr, cache_bytes: usize, fsync: bool) -> Self {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_reuseaddr(true).unwrap();
        socket.bind(address).unwrap();
        let listener = socket.listen(1024).unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let options = ServerOptions { data_dir: data_dir.to_path_buf(), fsync, cache_bytes };
        // Opening before returning means the journal's JOURNAL_ID exists by the time a test reads it.
        let task = match PreparedJournal::open(options).await {
            Ok(journal) => tokio::spawn(journal.serve(listener, cancellation.clone())),
            Err(error) => tokio::spawn(async move { Err(error) }),
        };
        Self { address, data_dir: data_dir.to_path_buf(), cancellation, task }
    }

    pub async fn stop(self) -> (PathBuf, SocketAddr) {
        self.cancellation.cancel();
        self.task.await.unwrap().unwrap();
        (self.data_dir, self.address)
    }
}

pub struct Ensemble {
    pub directories: Vec<tempfile::TempDir>,
    pub servers: Vec<Option<Server>>,
    pub addresses: Vec<SocketAddr>,
}

impl Ensemble {
    pub async fn start(count: usize) -> Self {
        let directories: Vec<_> = (0..count).map(|_| tempfile::tempdir().unwrap()).collect();
        let mut servers = Vec::new();
        for directory in &directories { servers.push(Some(Server::start(directory.path()).await)); }
        let addresses = servers.iter().map(|server| server.as_ref().unwrap().address).collect();
        Self { directories, servers, addresses }
    }

    pub fn config(&self, stream: &str) -> StreamConfig {
        StreamConfig { servers: self.addresses.iter().map(|address| address.to_string().into()).collect(), stream: stream.into(), write_quorum: 2, maximum_message_bytes: 1024 }
    }

    pub fn config_of(&self, servers: &[usize], stream: &str) -> StreamConfig {
        StreamConfig { servers: servers.iter().map(|&server| self.addresses[server].to_string().into()).collect(), stream: stream.into(), write_quorum: 1, maximum_message_bytes: 1024 }
    }

    pub fn journal(&self, server: usize) -> String {
        std::fs::read_to_string(self.directories[server].path().join("JOURNAL_ID")).unwrap()
    }

    /// Names each journal by its identity, so the ensemble stays the same one whichever route reaches it.
    pub fn members(&self, routes: &[(usize, SocketAddr)], stream: &str) -> StreamConfig {
        let servers = routes.iter().map(|&(server, address)| JournalEndpoint::Member { address: address.to_string(), journal: self.journal(server) }).collect();
        StreamConfig { servers, stream: stream.into(), write_quorum: 2, maximum_message_bytes: 1024 }
    }

    pub async fn stop(&mut self, server: usize) { self.servers[server].take().unwrap().stop().await; }

    pub async fn restart(&mut self, server: usize) {
        self.servers[server] = Some(Server::start_at(self.directories[server].path(), self.addresses[server]).await);
    }
}

pub async fn append(log: &dyn Log, payload: &str, timestamp: i64) -> LogMetadata {
    log.enqueue(Bytes::from(payload.to_owned()), Timestamp(timestamp)).await.unwrap().durable().await.unwrap()
}

/// Forwards connections to `target`, counting the request frames clients send through it; while held, it forwards none.
pub struct CountingProxy {
    pub address: SocketAddr,
    pub requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    appends: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    commits: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    drop_commits: std::sync::Arc<std::sync::atomic::AtomicBool>,
    drop_appends: std::sync::Arc<std::sync::atomic::AtomicBool>,
    accepted_appends: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    accepted_commits: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    held_overwrites: tokio::sync::watch::Sender<bool>,
    held_overwrite_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    append_refusal_epoch: std::sync::Arc<std::sync::atomic::AtomicU64>,
    held: tokio::sync::watch::Sender<bool>,
}

#[derive(prost::Message)]
struct CountedRequest {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(bytes = "bytes", optional, tag = "2")]
    append: Option<Bytes>,
    #[prost(bytes = "bytes", optional, tag = "7")]
    commit: Option<Bytes>,
    #[prost(bytes = "bytes", optional, tag = "35")]
    install_decision: Option<Bytes>,
    #[prost(bytes = "bytes", optional, tag = "6")]
    overwrite: Option<Bytes>,
}

#[derive(prost::Message)]
struct ResponseWithRefusal {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(message, optional, tag = "3")]
    refusal: Option<Refusal>,
    #[prost(bytes = "bytes", optional, tag = "2")]
    accepted: Option<Bytes>,
    #[prost(bytes = "bytes", optional, tag = "12")]
    installed_decision: Option<Bytes>,
}

#[derive(prost::Message)]
struct Refusal {
    #[prost(uint64, tag = "1")]
    promised: u64,
}

impl CountingProxy {
    pub async fn start(target: SocketAddr) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let appends = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let commits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let drop_commits = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let append_refusal_epoch = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter = std::sync::Arc::clone(&requests);
        let append_counter = std::sync::Arc::clone(&appends);
        let commit_counter = std::sync::Arc::clone(&commits);
        let dropping_commits = std::sync::Arc::clone(&drop_commits);
        let drop_appends = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dropping_appends = std::sync::Arc::clone(&drop_appends);
        let accepted_appends = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepted = std::sync::Arc::clone(&accepted_appends);
        let accepted_commits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let commit_accepts = std::sync::Arc::clone(&accepted_commits);
        let refusals = std::sync::Arc::clone(&append_refusal_epoch);
        let held = tokio::sync::watch::channel(false).0;
        let holding = held.clone();
        let held_overwrites = tokio::sync::watch::channel(false).0;
        let mutating = held_overwrites.clone();
        let held_overwrite_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let overwrite_counter = std::sync::Arc::clone(&held_overwrite_count);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = tokio::net::TcpStream::connect(target).await else { continue };
                let (mut client_reader, mut client_writer) = client.into_split();
                let (mut server_reader, mut server_writer) = server.into_split();
                let counter = std::sync::Arc::clone(&counter);
                let append_counter = std::sync::Arc::clone(&append_counter);
                let commit_counter = std::sync::Arc::clone(&commit_counter);
                let dropping_commits = std::sync::Arc::clone(&dropping_commits);
                let dropping_appends = std::sync::Arc::clone(&dropping_appends);
                let accepted = std::sync::Arc::clone(&accepted);
                let refusals = std::sync::Arc::clone(&refusals);
                let pending_appends = std::sync::Arc::new(parking_lot::Mutex::new(std::collections::BTreeSet::new()));
                let append_ids = std::sync::Arc::clone(&pending_appends);
                let pending_commits = std::sync::Arc::new(parking_lot::Mutex::new(std::collections::BTreeSet::new()));
                let commit_ids = std::sync::Arc::clone(&pending_commits);
                let commit_accepts = std::sync::Arc::clone(&commit_accepts);
                let mut holding = holding.subscribe();
                let mut held_overwrites = mutating.subscribe();
                let held_overwrite_count = std::sync::Arc::clone(&overwrite_counter);
                tokio::spawn(async move {
                    loop {
                        let Ok(length) = client_reader.read_u32().await else { break };
                        let mut body = vec![0; length as usize];
                        if client_reader.read_exact(&mut body).await.is_err() { break; }
                        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        if let Ok(request) = CountedRequest::decode(body.as_slice()) {
                            if request.append.is_some() {
                                append_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                if dropping_appends.load(std::sync::atomic::Ordering::SeqCst) { continue; }
                                append_ids.lock().insert(request.id);
                            }
                            if request.overwrite.is_some() && *held_overwrites.borrow() {
                                held_overwrite_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                if held_overwrites.wait_for(|held| !held).await.is_err() { break; }
                            }
                            if request.commit.is_some() || request.install_decision.is_some() {
                                commit_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                if dropping_commits.load(std::sync::atomic::Ordering::SeqCst) { continue; }
                                commit_ids.lock().insert(request.id);
                            }
                        }
                        if holding.wait_for(|held| !held).await.is_err() { break; }
                        if server_writer.write_all(&length.to_be_bytes()).await.is_err() || server_writer.write_all(&body).await.is_err() { break; }
                    }
                });
                tokio::spawn(async move {
                    while let Ok(length) = server_reader.read_u32().await {
                        let mut body = vec![0; length as usize];
                        if server_reader.read_exact(&mut body).await.is_err() { break; }
                        if let Ok(response) = ResponseWithRefusal::decode(body.as_slice()) {
                            if pending_commits.lock().remove(&response.id)
                                && (response.accepted.is_some() || response.installed_decision.is_some()) {
                                commit_accepts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            }
                            if pending_appends.lock().remove(&response.id) {
                                if let Some(refusal) = response.refusal {
                                    refusals.fetch_max(refusal.promised, std::sync::atomic::Ordering::SeqCst);
                                }
                                if response.accepted.is_some() {
                                    accepted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                }
                            }
                        }
                        if client_writer.write_all(&length.to_be_bytes()).await.is_err() || client_writer.write_all(&body).await.is_err() { break; }
                    }
                });
            }
        });
        Self { address, requests, appends, commits, drop_commits, drop_appends, accepted_appends, accepted_commits, held_overwrites, held_overwrite_count, append_refusal_epoch, held }
    }

    pub fn hold(&self, held: bool) { self.held.send_replace(held); }

    pub fn hold_overwrites(&self, held: bool) { self.held_overwrites.send_replace(held); }
    pub fn held_overwrites(&self) -> usize { self.held_overwrite_count.load(std::sync::atomic::Ordering::SeqCst) }

    pub fn requests(&self) -> usize { self.requests.load(std::sync::atomic::Ordering::SeqCst) }
    pub fn appends(&self) -> usize { self.appends.load(std::sync::atomic::Ordering::SeqCst) }
    pub fn commits(&self) -> usize { self.commits.load(std::sync::atomic::Ordering::SeqCst) }
    pub fn drop_commits(&self) { self.drop_commits.store(true, std::sync::atomic::Ordering::SeqCst); }
    pub fn pass_commits(&self) { self.drop_commits.store(false, std::sync::atomic::Ordering::SeqCst); }
    pub fn drop_appends(&self) { self.drop_appends.store(true, std::sync::atomic::Ordering::SeqCst); }
    pub fn accepted_commits(&self) -> usize { self.accepted_commits.load(std::sync::atomic::Ordering::SeqCst) }
    pub fn accepted_appends(&self) -> usize { self.accepted_appends.load(std::sync::atomic::Ordering::SeqCst) }
    pub fn append_refusal_epoch(&self) -> u64 { self.append_refusal_epoch.load(std::sync::atomic::Ordering::SeqCst) }
}
