#![allow(dead_code)]

use bytes::Bytes;
use rtdb_logkeeper::{ServerOptions, StreamConfig, serve};
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

    pub async fn start_at(data_dir: &Path, address: SocketAddr) -> Self {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_reuseaddr(true).unwrap();
        socket.bind(address).unwrap();
        let listener = socket.listen(1024).unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let options = ServerOptions { data_dir: data_dir.to_path_buf(), fsync: true };
        let task = tokio::spawn(serve(listener, options, cancellation.clone()));
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
        StreamConfig { servers: self.addresses.iter().map(ToString::to_string).collect(), stream: stream.into(), write_quorum: 2, maximum_message_bytes: 1024 }
    }

    pub fn config_of(&self, servers: &[usize], stream: &str) -> StreamConfig {
        StreamConfig { servers: servers.iter().map(|&server| self.addresses[server].to_string()).collect(), stream: stream.into(), write_quorum: 1, maximum_message_bytes: 1024 }
    }

    pub async fn stop(&mut self, server: usize) { self.servers[server].take().unwrap().stop().await; }

    pub async fn restart(&mut self, server: usize) {
        self.servers[server] = Some(Server::start_at(self.directories[server].path(), self.addresses[server]).await);
    }
}

pub async fn append(log: &dyn Log, payload: &str, timestamp: i64) -> LogMetadata {
    log.enqueue(Bytes::from(payload.to_owned()), Timestamp(timestamp)).await.unwrap().durable().await.unwrap()
}
