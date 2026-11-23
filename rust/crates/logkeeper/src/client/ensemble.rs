//! Typed requests to the servers of one stream's ensemble.

use super::connection::{Binding, Callback, Connection, connection, remote};
use super::{JournalEndpoint, StreamConfig, read_index_evidence::valid_reply};
use crate::protocol::{AUTHORITY_FRAME_BYTES, Accepted, Append, Check, Commit, DigestPage, DropStream, Entries, Entry, Fetch, LogletCheck, Overwrite, PageDigest, Read, ReadIndexReply, ReadIndexRequest, Refused, Request, RequestBody, Response, ResponseBody, Retire, Seal, Sealed, Trim, encode_frame, read_bounded_frame};
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use rtdb_storage::{Error, Result};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::{io::AsyncWriteExt, net::TcpStream};

const AUTHORITY_IDLE: Duration = Duration::from_secs(120);

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
    authority: tokio::sync::Mutex<Option<AuthoritySession>>,
}

struct AuthoritySession {
    port: u16,
    socket: TcpStream,
    last_used: Instant,
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
            authority: tokio::sync::Mutex::new(None),
            binding: match server.journal() {
                Some(expected) => Binding::Expected(Arc::from(expected)),
                None => Binding::Discover { slot, journals: Arc::clone(&journals) },
            },
        }).collect();
        let mut journals: Vec<_> = config.servers.iter().filter_map(JournalEndpoint::journal).collect();
        let identified = journals.len() == config.servers.len();
        if !identified {
            journals = config.servers.iter().map(JournalEndpoint::address).collect();
        }
        journals.sort_unstable();
        let identity = crate::protocol::ensemble_identity(identified, config.write_quorum as u64, journals);
        Self { config, links, identity: Bytes::copy_from_slice(&identity) }
    }

    pub fn len(&self) -> usize { self.links.len() }

    pub fn stream(&self) -> String { self.config.stream.clone() }

    pub(super) fn identity(&self) -> &Bytes { &self.identity }

    pub fn supports_guarded(&self) -> bool {
        self.config.write_quorum > self.len() - self.config.write_quorum
            && self.config.servers.iter().all(|server| server.journal().is_some())
    }

    pub fn timed_origin(&self, certificate: [u8; 32]) -> crate::protocol::TimedJournalOrigin {
        let mut journals: Vec<_> = self.config.servers.iter().map(|server| {
            server.journal().expect("guarded ensemble identity").to_owned()
        }).collect();
        journals.sort_unstable();
        crate::protocol::TimedJournalOrigin {
            journals, write_quorum: self.config.write_quorum as u64, stream: self.stream(),
            clock_certificate_sha256: Bytes::copy_from_slice(&certificate),
            original_entry_sha256: Bytes::new(),
        }
    }

    pub fn matches_timed_origin(&self, identity: &[u8]) -> bool {
        crate::protocol::timed_origin_fingerprint(&self.config.stream, &self.identity).as_slice() == identity
    }

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

    fn decision_request(&self, epoch: u64, through: i64) -> Commit {
        Commit { stream: self.stream(), epoch, commit_lsn: through, ensemble: self.identity.clone() }
    }

    pub fn commit_request(&self, epoch: u64, through: i64) -> RequestBody {
        RequestBody::Commit(self.decision_request(epoch, through))
    }

    pub async fn commit(&self, server: usize, epoch: u64, commit: i64) -> Result<Ack> {
        let reply = self.links[server].call(self.commit_request(epoch, commit)).await;
        self.ack(server, reply)
    }

    pub fn publish_request(&self, epoch: u64, through: i64) -> RequestBody {
        RequestBody::Publish(self.decision_request(epoch, through))
    }

    pub async fn publish(&self, server: usize, epoch: u64, through: i64) -> Result<Ack> {
        let reply = self.links[server].call(self.publish_request(epoch, through)).await;
        self.ack(server, reply)
    }

    pub fn check_synced_request(&self, epoch: u64) -> RequestBody { RequestBody::CheckSynced(Check { stream: self.stream(), epoch }) }

    fn loglet_check(&self, epoch: u64) -> LogletCheck {
        LogletCheck { stream: self.stream(), epoch, ensemble: self.identity.clone() }
    }

    pub async fn confirm_authority(&self, server: usize, epoch: u64, port: u16) -> Result<Option<u64>> {
        let endpoint = &self.config.servers[server];
        let journal = endpoint.journal().ok_or_else(|| remote(endpoint.address(), "writer authority requires an installed journal identity"))?;
        let (host, _) = endpoint.address().rsplit_once(':')
            .ok_or_else(|| remote(endpoint.address(), "invalid journal authority route"))?;
        let address = format!("{host}:{port}");
        let mut peer = self.links[server].authority.lock().await;

        // A cancelled call drops its socket, so a delayed reply cannot answer another round.
        let mut socket = match peer.take() {
            Some(session) if session.port == port && session.last_used.elapsed() < AUTHORITY_IDLE => session.socket,
            _ => {
                let socket = TcpStream::connect(&address).await.map_err(|error| remote(&address, error.to_string()))?;
                socket.set_nodelay(true).map_err(|error| remote(&address, error.to_string()))?;
                socket
            }
        };
        let request = Request {
            id: 1,
            body: Some(RequestBody::ConfirmAuthority(self.loglet_check(epoch))),
            target_journal: journal.to_owned(),
        };
        let mut frame = BytesMut::new();
        encode_frame(&request, &mut frame);
        socket.write_all(&frame).await.map_err(|error| remote(&address, error.to_string()))?;
        let response = read_bounded_frame::<Response>(&mut socket, AUTHORITY_FRAME_BYTES).await
            .map_err(|error| remote(&address, error.to_string()))?;
        let outcome = match response {
            Some((Response { id: 1, body: Some(ResponseBody::AuthorityConfirmed(_)) }, _)) => None,
            Some((Response { id: 1, body: Some(ResponseBody::Refused(refusal)) }, _)) => Some(refusal.promised),
            _ => return Err(remote(&address, "the journal did not confirm writer authority")),
        };
        *peer = Some(AuthoritySession { port, socket, last_used: Instant::now() });
        Ok(outcome)
    }

    pub async fn activate(&self, server: usize, epoch: u64) -> Result<Ack> {
        let reply = self.links[server].call(RequestBody::ActivateLoglet(self.loglet_check(epoch))).await;
        self.ack(server, reply)
    }

    pub async fn read_index(&self, server: usize, epoch: u64, nonce: [u8; 16]) -> Result<ReadIndexReply> {
        let request = ReadIndexRequest { loglet: Some(self.loglet_check(epoch)), nonce: Bytes::copy_from_slice(&nonce) };
        match self.links[server].call(RequestBody::ReadIndex(request)).await? {
            ResponseBody::ReadIndex(reply) if <[u8; 16]>::try_from(reply.nonce.as_ref()).is_ok_and(|observed| {
                valid_reply(u128::from_be_bytes(nonce), u128::from_be_bytes(observed), reply.decided, reply.visible)
            }) => Ok(reply),
            ResponseBody::Refused(refused) if refused.promised > epoch =>
                Err(Error::Deposed { epoch, promised: refused.promised }),
            other => Err(self.unexpected(server, other)),
        }
    }

    pub fn append_request(&self, epoch: u64, entry: Entry, commit_lsn: i64) -> RequestBody {
        let guarded = entry.guarded;
        let append = Append {
            stream: self.stream(), epoch, first_lsn: entry.lsn, entries: vec![entry], commit_lsn,
            guarded_ensemble: if guarded { self.identity.clone() } else { Bytes::new() },
        };
        if guarded { RequestBody::AppendObserved(append) } else { RequestBody::Append(append) }
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
        self.read_digest(server, DigestPage { stream: self.stream(), from_lsn, through_lsn, preceding_digest, sealed_epoch: None, certified_entry_digest: Bytes::new() }).await
    }

    pub async fn guarded_digest(&self, server: usize, lsn: i64, epoch: u64) -> Result<PageDigest> {
        self.read_digest(server, DigestPage {
            stream: self.stream(), from_lsn: lsn, through_lsn: lsn, preceding_digest: Bytes::new(), sealed_epoch: Some(epoch),
            certified_entry_digest: Bytes::new(),
        }).await
    }

    pub async fn original_digest(&self, server: usize, lsn: i64, digest: &[u8; 32]) -> Result<PageDigest> {
        self.read_digest(server, DigestPage {
            stream: self.stream(), from_lsn: lsn, through_lsn: lsn, preceding_digest: Bytes::new(), sealed_epoch: None,
            certified_entry_digest: Bytes::copy_from_slice(digest),
        }).await
    }

    async fn read_digest(&self, server: usize, page: DigestPage) -> Result<PageDigest> {
        let request = RequestBody::DigestPage(page);
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
