//! Typed requests to the servers of one stream's ensemble.

use super::connection::{Connection, connection, remote};
use super::StreamConfig;
use crate::protocol::{Accepted, Check, Commit, Entries, Entry, Fetch, Overwrite, Read, Refused, RequestBody, ResponseBody, Seal, Sealed, Trim};
use rtdb_storage::{Error, Result};
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
    pub links: Vec<Arc<Connection>>,
}

impl Ensemble {
    pub fn new(config: StreamConfig) -> Self {
        let links = config.servers.iter().map(|address| connection(address)).collect();
        Self { config, links }
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

    pub async fn commit(&self, server: usize, epoch: u64, commit: i64) -> Result<Ack> {
        let reply = self.links[server].call(RequestBody::Commit(Commit { stream: self.stream(), epoch, commit_lsn: commit })).await;
        self.ack(server, reply)
    }

    pub fn check_request(&self, epoch: u64) -> RequestBody { RequestBody::Check(Check { stream: self.stream(), epoch }) }

    pub fn overwrite_request(&self, epoch: u64, from: i64, entries: Vec<Entry>, complete: bool, commit: i64) -> RequestBody {
        RequestBody::Overwrite(Overwrite { stream: self.stream(), epoch, from_lsn: from, entries, complete, commit_lsn: commit })
    }

    pub async fn overwrite(&self, server: usize, epoch: u64, from: i64, entries: Vec<Entry>, complete: bool) -> Result<Ack> {
        let reply = self.links[server].call(self.overwrite_request(epoch, from, entries, complete, -1)).await;
        self.ack(server, reply)
    }

    /// Held entries in `[from, to)`, committed or not.
    pub async fn fetch(&self, server: usize, from: i64, to: i64, max_entries: u32) -> Result<Entries> {
        let body = RequestBody::Fetch(Fetch { stream: self.stream(), from_lsn: from, to_lsn: to, max_entries, max_bytes: MAXIMUM_BATCH_BYTES });
        let reply = self.links[server].call(body).await;
        self.entries(server, reply)
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
