//! Typed requests to the servers of one stream's ensemble.

use super::connection::{Binding, Callback, Connection, connection, remote};
use super::{JournalEndpoint, StreamConfig};
use crate::protocol::{Accepted, Check, Commit, DigestPage, DropStream, Entries, Entry, Fetch, LogletCheck, Overwrite, PageDigest, Read, Refused, RequestBody, ResponseBody, Retire, Seal, Sealed, Trim};
use bytes::Bytes;
use parking_lot::Mutex;
use rtdb_storage::{Error, Result};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;

pub(crate) const MAXIMUM_BATCH_ENTRIES: u32 = 1024;
pub(crate) const MAXIMUM_BATCH_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ack {
    Accepted(Accepted),
    Refused(Refused),
}

pub(crate) struct Ensemble {
    pub config: StreamConfig,
    pub links: Vec<Peer>,
    identity: Bytes,
}

pub(crate) struct Peer {
    connection: Arc<Connection>,
    binding: Binding,
}

impl Peer {
    fn address(&self) -> &str { self.connection.address() }

    pub fn send(&self, body: RequestBody, callback: Callback) {
        self.connection.send(self.binding.clone(), body, callback)
    }

    async fn call(&self, body: RequestBody) -> Result<ResponseBody> {
        self.connection.call(self.binding.clone(), body).await
    }
}

impl Ensemble {
    pub fn new(config: StreamConfig) -> Self {
        let journals = Arc::new(Mutex::new(config.servers.iter().map(|server| server.journal().map(Arc::from)).collect()));
        let links = config.servers.iter().enumerate().map(|(slot, server)| Peer {
            connection: connection(server.address()),
            binding: match server.journal() {
                Some(expected) => Binding::Expected(Arc::from(expected)),
                None => Binding::Discover { slot, journals: Arc::clone(&journals) },
            },
        }).collect();
        let mut journals: Vec<_> = config.servers.iter().filter_map(JournalEndpoint::journal).collect();
        let mut identity = Sha256::new();
        if journals.len() == config.servers.len() {
            identity.update(b"rtdb.logkeeper/ensemble/v2");
        } else {
            identity.update(b"rtdb.logkeeper/ensemble/v1");
            journals = config.servers.iter().map(JournalEndpoint::address).collect();
        }
        journals.sort_unstable();
        identity.update((config.write_quorum as u64).to_be_bytes());
        for journal in journals {
            identity.update((journal.len() as u64).to_be_bytes());
            identity.update(journal.as_bytes());
        }
        Self { config, links, identity: Bytes::copy_from_slice(&identity.finalize()) }
    }

    pub fn len(&self) -> usize { self.links.len() }

    pub fn stream(&self) -> String { self.config.stream.clone() }

    fn unexpected(&self, server: usize, reply: ResponseBody) -> Error {
        let address = self.links[server].address();
        match reply {
            ResponseBody::Failed(failed) => remote(address, failed.message),
            other => remote(address, format!("unexpected reply {other:?}")),
        }
    }

    pub fn ack(&self, server: usize, reply: Result<ResponseBody>) -> Result<Ack> {
        match reply? {
            ResponseBody::Accepted(accepted) => Ok(Ack::Accepted(accepted)),
            ResponseBody::Refused(refused) => Ok(Ack::Refused(refused)),
            other => Err(self.unexpected(server, other)),
        }
    }

    fn entries(&self, server: usize, reply: Result<ResponseBody>) -> Result<Entries> {
        match reply? {
            ResponseBody::Entries(entries) => Ok(entries),
            other => Err(self.unexpected(server, other)),
        }
    }

    pub async fn seal(&self, server: usize, epoch: u64) -> Result<Sealed> {
        match self.links[server].call(RequestBody::Seal(Seal { stream: self.stream(), epoch })).await? {
            ResponseBody::Sealed(sealed) => Ok(sealed),
            other => Err(self.unexpected(server, other)),
        }
    }

    pub async fn trim(&self, server: usize, through: i64) -> Result<Ack> {
        let reply = self.links[server].call(RequestBody::Trim(Trim { stream: self.stream(), through_lsn: through })).await;
        self.ack(server, reply)
    }

    pub async fn trim_durable(&self, server: usize, through: i64) -> Result<Ack> {
        let reply = self.links[server].call(RequestBody::TrimDurable(Trim { stream: self.stream(), through_lsn: through })).await;
        self.ack(server, reply)
    }

    pub async fn drop_stream(&self, server: usize, epoch: u64) -> Result<Ack> {
        let reply = self.links[server].call(RequestBody::DropStream(DropStream { stream: self.stream(), epoch })).await;
        self.ack(server, reply)
    }

    pub async fn retire(&self, server: usize, epoch: u64) -> Result<Ack> {
        let reply = self.links[server].call(RequestBody::Retire(Retire { stream: self.stream(), epoch })).await;
        self.ack(server, reply)
    }

    pub async fn commit(&self, server: usize, epoch: u64, commit: i64) -> Result<Ack> {
        let reply = self.links[server].call(RequestBody::Commit(Commit { stream: self.stream(), epoch, commit_lsn: commit })).await;
        self.ack(server, reply)
    }

    pub fn check_synced_request(&self, epoch: u64) -> RequestBody { RequestBody::CheckSynced(Check { stream: self.stream(), epoch }) }

    fn loglet_check(&self, epoch: u64) -> LogletCheck {
        LogletCheck { stream: self.stream(), epoch, ensemble: self.identity.clone() }
    }

    pub async fn activate(&self, server: usize, epoch: u64) -> Result<Ack> {
        let reply = self.links[server].call(RequestBody::ActivateLoglet(self.loglet_check(epoch))).await;
        self.ack(server, reply)
    }

    pub async fn read_index(&self, server: usize, epoch: u64) -> Result<Ack> {
        let reply = self.links[server].call(RequestBody::ReadIndex(self.loglet_check(epoch))).await;
        self.ack(server, reply)
    }

    pub fn repair_request(&self, epoch: u64, from: i64, entries: Vec<Entry>, commit: i64) -> RequestBody {
        RequestBody::Overwrite(Overwrite {
            stream: self.stream(), epoch, from_lsn: from, entries, complete: true, commit_lsn: commit, activate_ensemble: self.identity.clone(),
        })
    }

    pub async fn overwrite(&self, server: usize, epoch: u64, from: i64, entries: Vec<Entry>, complete: bool) -> Result<Ack> {
        let body = RequestBody::Overwrite(Overwrite {
            stream: self.stream(), epoch, from_lsn: from, entries, complete, commit_lsn: -1, activate_ensemble: Bytes::new(),
        });
        let reply = self.links[server].call(body).await;
        self.ack(server, reply)
    }

    /// Held entries in `[from, to)`, committed or not.
    pub async fn fetch(&self, server: usize, from: i64, to: i64, max_entries: u32) -> Result<Entries> {
        let body = RequestBody::Fetch(Fetch { stream: self.stream(), from_lsn: from, to_lsn: to, max_entries, max_bytes: MAXIMUM_BATCH_BYTES });
        let reply = self.links[server].call(body).await;
        self.entries(server, reply)
    }

    pub async fn digest_page(&self, server: usize, from_lsn: i64, through_lsn: i64, preceding_digest: Option<[u8; 32]>) -> Result<PageDigest> {
        let preceding_digest = preceding_digest.map_or_else(Bytes::new, |digest| Bytes::copy_from_slice(&digest));
        let request = RequestBody::DigestPage(DigestPage { stream: self.stream(), from_lsn, through_lsn, preceding_digest });
        match self.links[server].call(request).await? {
            ResponseBody::PageDigest(page) => Ok(page),
            other => Err(self.unexpected(server, other)),
        }
    }

    pub async fn read(&self, server: usize, from: i64, max_entries: u32, wait: Duration) -> Result<Entries> {
        let wait_ms = wait.as_millis().min(super::connection::CALL_TIMEOUT.as_millis() / 2) as u32;
        let body = RequestBody::Read(Read { stream: self.stream(), from_lsn: from, max_entries, max_bytes: MAXIMUM_BATCH_BYTES, wait_ms });
        let reply = self.links[server].call(body).await;
        self.entries(server, reply)
    }
}

/// A read starting at or below the server's trim point.
pub(crate) fn check_retained(from: i64, entries: &Entries) -> Result<()> {
    if from <= entries.trim_lsn { return Err(Error::UnavailableLogRange { requested: from, first_retained: entries.trim_lsn + 1 }); }
    Ok(())
}
