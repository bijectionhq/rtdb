use std::{io, net::{IpAddr, SocketAddr, ToSocketAddrs}, sync::Arc};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use tokio::task::JoinHandle;

use super::Error;

const LOOKUP_ALLOCATION_BOUND: usize = 2 * 1024 * 1024;

pub(super) async fn resolve(host: &str, port: u16, budget: &Arc<MemoryBudget>) -> Result<SocketAddr, Error> {
    resolve_with(host, port, budget, |host, port| {
        (host, port).to_socket_addrs()?.next().ok_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, "HTTP hostname has no address"))
    }).await
}

struct Lookup { host: String, port: u16, _charge: Arc<MemoryReservation> }

impl Lookup {
    fn run(self, resolver: impl FnOnce(&str, u16) -> io::Result<SocketAddr>) -> Result<SocketAddr, Error> {
        resolver(&self.host, self.port).map_err(|_| Error("HTTP name resolution failed"))
    }
}

struct PendingLookup { task: JoinHandle<Result<SocketAddr, Error>> }

impl Drop for PendingLookup {
    fn drop(&mut self) {
        // Started system lookups cannot be interrupted; their Lookup retains admission until completion.
        self.task.abort();
    }
}

async fn resolve_with<F>(host: &str, port: u16, budget: &Arc<MemoryBudget>, resolver: F) -> Result<SocketAddr, Error>
where F: FnOnce(&str, u16) -> io::Result<SocketAddr> + Send + 'static {
    if let Ok(address) = host.parse::<IpAddr>() { return Ok(SocketAddr::new(address, port)); }
    let bound = host.len().checked_mul(4).and_then(|bytes| bytes.checked_add(LOOKUP_ALLOCATION_BOUND)).ok_or(Error("HTTP name resolution allocation overflow"))?;
    let charge = budget.reserve(bound).map_err(|_| Error("HTTP name resolution memory limit exceeded"))?;
    let lookup = Lookup { host: host.to_owned(), port, _charge: charge };
    let mut pending = PendingLookup { task: tokio::task::spawn_blocking(move || lookup.run(resolver)) };
    (&mut pending.task).await.map_err(|_| Error("HTTP name resolution task failed"))?
}

#[cfg(test)]
mod tests;
