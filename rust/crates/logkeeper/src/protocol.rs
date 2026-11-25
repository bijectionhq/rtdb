//! The wire protocol: a stream of frames, each a big-endian u32 length followed by one prost message.
//! A client sends `Request`s and receives `Response`s carrying the same id, in any order.
//! One connection multiplexes every stream; a server decides a connection's mutations in arrival order.

use crate::decision::{DecisionCertificate, DecisionInstallation, DecisionVote, DecisionVoteRequest, Handoff, HandoffCertificate, StableDecision};
use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};

mod frame_parts;
pub(crate) use frame_parts::{FrameParts, WIRE_BATCH_FRAMES};

pub(crate) const MAXIMUM_FRAME_BYTES: usize = 256 * 1024 * 1024;
pub(crate) const AUTHORITY_FRAME_BYTES: usize = 4096;

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Entry {
    #[prost(sint64, tag = "1")]
    pub lsn: i64,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
    #[prost(sint64, tag = "3")]
    pub timestamp: i64,
    #[prost(bytes = "bytes", tag = "4")]
    pub payload: Bytes,
    /// A guarded entry retains its admission kind through repair and replay.
    #[prost(bool, tag = "5")]
    pub guarded: bool,
    /// Exclusive bound on a qualifying original journal's rounded UTC upper sample.
    #[prost(sfixed64, optional, tag = "6")]
    pub guarded_before: Option<i64>,
    /// The exact voter roster needed to interpret a physical cutoff after a move.
    #[prost(message, optional, tag = "7")]
    pub timed_origin: Option<TimedJournalOrigin>,
}

impl Entry {
    pub(crate) fn valid_guard(&self) -> bool {
        match (self.guarded_before, &self.timed_origin) {
            (None, None) => true,
            (Some(before), Some(origin)) => self.guarded && before > 0 && origin.valid(),
            _ => false,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct TimedJournalOrigin {
    #[prost(string, repeated, tag = "1")]
    pub journals: Vec<String>,
    #[prost(uint64, tag = "2")]
    pub write_quorum: u64,
    #[prost(string, tag = "3")]
    pub stream: String,
    #[prost(bytes = "bytes", tag = "4")]
    pub clock_certificate_sha256: Bytes,
    /// Empty on the original append. A copy whose payload is resealed keeps the original
    /// stream's digest of the exact appended entry, so its original voters remain findable.
    #[prost(bytes = "bytes", tag = "5")]
    pub original_entry_sha256: Bytes,
}

impl TimedJournalOrigin {
    pub(crate) fn valid(&self) -> bool {
        let count = self.journals.len() as u64;
        self.clock_certificate_sha256.len() == 32 && self.clock_certificate_sha256.as_ref() != &[0; 32]
            && (self.original_entry_sha256.is_empty()
                || self.original_entry_sha256.len() == 32 && self.original_entry_sha256.as_ref() != &[0; 32])
            && !self.stream.is_empty() && count / 2 < self.write_quorum && self.write_quorum <= count
            && self.journals.windows(2).all(|pair| pair[0] < pair[1])
            && self.journals.iter().all(|id| rtdb_types::valid_journal_identity(id))
    }

    pub(crate) fn ensemble_identity(&self) -> [u8; 32] {
        ensemble_identity(true, self.write_quorum, self.journals.iter().map(String::as_str))
    }

    pub(crate) fn fingerprint(&self) -> [u8; 32] {
        timed_origin_fingerprint(&self.stream, &self.ensemble_identity())
    }
}

pub(crate) fn timed_origin_fingerprint(stream: &str, ensemble: &[u8]) -> [u8; 32] {
    let mut origin = Sha256::new();
    origin.update(b"rtdb.logkeeper/timed-original/v1");
    origin.update((stream.len() as u64).to_be_bytes());
    origin.update(stream.as_bytes());
    origin.update(ensemble);
    origin.finalize().into()
}

pub(crate) fn ensemble_identity<'a>(identified: bool, quorum: u64, members: impl IntoIterator<Item = &'a str>) -> [u8; 32] {
    let mut identity = Sha256::new();
    if identified { identity.update(b"rtdb.logkeeper/ensemble/v2"); }
    else { identity.update(b"rtdb.logkeeper/ensemble/v1"); }
    identity.update(quorum.to_be_bytes());
    for member in members {
        identity.update((member.len() as u64).to_be_bytes());
        identity.update(member.as_bytes());
    }
    identity.finalize().into()
}

pub(crate) fn before_guarded_cutoff(upper: i64, before: Option<i64>) -> bool {
    upper > 0 && before.is_none_or(|before| upper < before)
}

/// Entries from `start_lsn` up to the next run's start (or the holder's last lsn) were written at `epoch`.
#[derive(Clone, Copy, PartialEq, Eq, Message)]
pub struct EpochRun {
    #[prost(sint64, tag = "1")]
    pub start_lsn: i64,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Append {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
    #[prost(sint64, tag = "3")]
    pub first_lsn: i64,
    #[prost(message, repeated, tag = "4")]
    pub entries: Vec<Entry>,
    #[prost(sint64, tag = "5")]
    pub commit_lsn: i64,
    /// The exact ensemble whose activation authorizes an observed append.
    #[prost(bytes = "bytes", tag = "6")]
    pub guarded_ensemble: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Seal {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
    #[prost(bytes = "bytes", tag = "3")]
    pub nonce: Bytes,
}

/// Committed entries only; `wait_ms` long-polls when none are available at `from_lsn`.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct Read {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(sint64, tag = "2")]
    pub from_lsn: i64,
    #[prost(uint32, tag = "3")]
    pub max_entries: u32,
    #[prost(uint64, tag = "4")]
    pub max_bytes: u64,
    #[prost(uint32, tag = "5")]
    pub wait_ms: u32,
}

/// Every held entry in `[from_lsn, to_lsn)`, committed or not: for a writer copying its own history between servers.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct Fetch {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(sint64, tag = "2")]
    pub from_lsn: i64,
    #[prost(sint64, tag = "3")]
    pub to_lsn: i64,
    #[prost(uint32, tag = "4")]
    pub max_entries: u32,
    #[prost(uint64, tag = "5")]
    pub max_bytes: u64,
}

/// Truncates at `from_lsn` and appends `entries`.
/// `complete` claims the server now holds the writer's whole history up to its last entry, marking it synced at `epoch`; until then it stays synced at whatever epoch it was.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct Overwrite {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
    #[prost(sint64, tag = "3")]
    pub from_lsn: i64,
    #[prost(message, repeated, tag = "4")]
    pub entries: Vec<Entry>,
    #[prost(bool, tag = "5")]
    pub complete: bool,
    #[prost(sint64, tag = "6")]
    pub commit_lsn: i64,
    #[prost(bytes = "bytes", tag = "7")]
    pub activate_ensemble: Bytes,
    #[prost(bool, tag = "8")]
    pub certified: bool,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Commit {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
    #[prost(sint64, tag = "3")]
    pub commit_lsn: i64,
    #[prost(bytes = "bytes", tag = "4")]
    pub ensemble: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Trim {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(sint64, tag = "2")]
    pub through_lsn: i64,
}

/// Fences a disposed installation and awaits removal of its journal payloads.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct Retire {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
}

/// Asks whether the stream is still unsealed beyond `epoch`, for a writer confirming its leadership: `Accepted` when the promise is at most `epoch`, `Refused` carrying the higher promise otherwise.
/// It changes nothing, so it is answered without a journal round.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct Check {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct LogletCheck {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
    #[prost(bytes = "bytes", tag = "3")]
    pub ensemble: Bytes,
    #[prost(bool, tag = "4")]
    pub certified: bool,
    #[prost(message, optional, tag = "5")]
    pub handoff: Option<HandoffCertificate>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct ReadIndexRequest {
    #[prost(message, optional, tag = "1")]
    pub loglet: Option<LogletCheck>,
    #[prost(bytes = "bytes", tag = "2")]
    pub nonce: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub struct ReadIndexReply {
    #[prost(bytes = "bytes", tag = "1")]
    pub nonce: Bytes,
    #[prost(sint64, tag = "2")]
    pub decided: i64,
    #[prost(sint64, tag = "3")]
    pub visible: i64,
    #[prost(message, optional, tag = "4")]
    pub certificate: Option<DecisionCertificate>,
    #[prost(message, optional, tag = "5")]
    pub stability: Option<StableDecision>,
    #[prost(bytes = "bytes", tag = "6")]
    pub key: Bytes,
    #[prost(bytes = "bytes", tag = "7")]
    pub signature: Bytes,
    #[prost(message, optional, tag = "8")]
    pub handoff: Option<HandoffCertificate>,
}

impl ReadIndexReply {
    fn signing_bytes(&self, check: &LogletCheck) -> Vec<u8> {
        let mut reply = self.clone();
        reply.signature = Bytes::new();
        let mut bytes = b"rtdb.logkeeper/read-index/v1".to_vec();
        check.encode_length_delimited(&mut bytes).expect("Vec has capacity");
        reply.encode_length_delimited(&mut bytes).expect("Vec has capacity");
        bytes
    }

    pub(crate) fn sign(&mut self, check: &LogletCheck, signer: &rtdb_storage::attestation::SigningKey) {
        self.key = Bytes::copy_from_slice(&signer.public_key());
        self.signature = Bytes::copy_from_slice(&signer.sign(&self.signing_bytes(check)));
    }

    pub(crate) fn verifies(&self, check: &LogletCheck, journal: &str) -> bool {
        let Ok(key) = <&[u8; 32]>::try_from(self.key.as_ref()) else { return false; };
        let Ok(signature) = <&[u8; 64]>::try_from(self.signature.as_ref()) else { return false; };
        crate::attestation::journal_id(key).to_string() == journal
            && rtdb_storage::attestation::verifies(key, &self.signing_bytes(check), signature)
    }
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct DropStream {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct DigestPage {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(sint64, tag = "2")]
    pub from_lsn: i64,
    #[prost(sint64, tag = "3")]
    pub through_lsn: i64,
    #[prost(bytes = "bytes", tag = "4")]
    pub preceding_digest: Bytes,
    /// An exact guarded entry may be inspected before commit only at this seal.
    #[prost(uint64, optional, tag = "5")]
    pub sealed_epoch: Option<u64>,
    /// Retrieve a retained original sample for an exact, independently certified entry.
    #[prost(bytes = "bytes", tag = "6")]
    pub certified_entry_digest: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Identify {
    #[prost(bytes = "bytes", tag = "1")]
    pub nonce: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub struct JournalIdentity {
    #[prost(string, tag = "1")]
    pub journal: String,
    #[prost(bytes = "bytes", tag = "2")]
    pub public_key: Bytes,
    #[prost(bytes = "bytes", tag = "3")]
    pub signature: Bytes,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub(crate) enum RequestBody {
    #[prost(message, tag = "2")]
    Append(Append),
    #[prost(message, tag = "3")]
    Seal(Seal),
    #[prost(message, tag = "4")]
    Read(Read),
    #[prost(message, tag = "5")]
    Fetch(Fetch),
    #[prost(message, tag = "6")]
    Overwrite(Overwrite),
    #[prost(message, tag = "7")]
    Commit(Commit),
    #[prost(message, tag = "33")]
    Publish(Commit),
    #[prost(message, tag = "8")]
    Trim(Trim),
    #[prost(message, tag = "9")]
    Check(Check),
    #[prost(message, tag = "10")]
    DropStream(DropStream),
    #[prost(message, tag = "11")]
    Retire(Retire),
    #[prost(message, tag = "12")]
    DigestPage(DigestPage),
    #[prost(message, tag = "13")]
    CheckSynced(Check),
    #[prost(message, tag = "14")]
    ActivateLoglet(LogletCheck),
    #[prost(message, tag = "32")]
    ReadIndex(ReadIndexRequest),
    #[prost(message, tag = "16")]
    Identify(Identify),
    #[prost(message, tag = "18")]
    ConfirmAuthority(LogletCheck),
    // Providers that do not enforce the journal cutoff cannot acknowledge this request.
    #[prost(message, tag = "31")]
    AppendObserved(Append),
    #[prost(message, tag = "30")]
    TrimDurable(Trim),
    #[prost(message, tag = "34")]
    VoteDecision(DecisionVoteRequest),
    #[prost(message, tag = "35")]
    InstallDecision(DecisionCertificate),
    #[prost(message, tag = "36")]
    PublishDecision(StableDecision),
    #[prost(message, tag = "37")]
    ExportHandoff(Handoff),
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Request {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(oneof = "RequestBody", tags = "2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 18, 30, 31, 32, 33, 34, 35, 36, 37")]
    pub body: Option<RequestBody>,
    #[prost(string, tag = "17")]
    pub target_journal: String,
}

/// An `Accepted` reply to an Append, Overwrite, Commit or Check announces that the stream's promise on this server was still at most the request's epoch when the server decided it, which was after the request was sent.
#[derive(Clone, Copy, PartialEq, Eq, Message)]
pub struct Accepted {
    #[prost(sint64, tag = "1")]
    pub last_lsn: i64,
    #[prost(sint64, tag = "2")]
    pub commit_lsn: i64,
}

/// `promised > epoch` means the writer is deposed; otherwise the server needs repair before it accepts this writer's appends.
#[derive(Clone, Copy, PartialEq, Eq, Message)]
pub struct Refused {
    #[prost(uint64, tag = "1")]
    pub promised: u64,
    #[prost(sint64, tag = "2")]
    pub last_lsn: i64,
}

/// `synced` is the epoch whose writer last brought this server up to its whole history, 0 when none has.
#[derive(Clone, PartialEq, Message)]
pub struct Sealed {
    #[prost(uint64, tag = "1")]
    pub promised: u64,
    #[prost(uint64, tag = "2")]
    pub synced: u64,
    #[prost(sint64, tag = "3")]
    pub trim_lsn: i64,
    #[prost(sint64, tag = "4")]
    pub last_lsn: i64,
    #[prost(sint64, tag = "5")]
    pub commit_lsn: i64,
    #[prost(message, repeated, tag = "6")]
    pub runs: Vec<EpochRun>,
    #[prost(bool, tag = "7")]
    pub guarded_tail: bool,
    #[prost(message, optional, tag = "8")]
    pub certificate: Option<DecisionCertificate>,
    #[prost(message, optional, tag = "9")]
    pub stability: Option<StableDecision>,
    #[prost(bytes = "bytes", tag = "10")]
    pub key: Bytes,
    #[prost(bytes = "bytes", tag = "11")]
    pub signature: Bytes,
}

impl Sealed {
    fn signing_bytes(&self, request: &Seal) -> Vec<u8> {
        let mut reply = self.clone();
        reply.signature = Bytes::new();
        let mut bytes = b"rtdb.logkeeper/sealed-prefix/v1".to_vec();
        request.encode_length_delimited(&mut bytes).expect("Vec has capacity");
        reply.encode_length_delimited(&mut bytes).expect("Vec has capacity");
        bytes
    }

    pub(crate) fn sign(&mut self, request: &Seal, signer: &rtdb_storage::attestation::SigningKey) {
        self.key = Bytes::copy_from_slice(&signer.public_key());
        self.signature = Bytes::copy_from_slice(&signer.sign(&self.signing_bytes(request)));
    }

    pub(crate) fn verifies(&self, request: &Seal, journal: &str) -> bool {
        let Ok(key) = <&[u8; 32]>::try_from(self.key.as_ref()) else { return false; };
        let Ok(signature) = <&[u8; 64]>::try_from(self.signature.as_ref()) else { return false; };
        crate::attestation::journal_id(key).to_string() == journal
            && rtdb_storage::attestation::verifies(key, &self.signing_bytes(request), signature)
    }
}

/// A read at or below `trim_lsn` returns no entries; the client reports the trimmed range.
#[derive(Clone, PartialEq, Message)]
pub struct Entries {
    #[prost(message, repeated, tag = "1")]
    pub entries: Vec<Entry>,
    #[prost(sint64, tag = "2")]
    pub commit_lsn: i64,
    #[prost(sint64, tag = "3")]
    pub trim_lsn: i64,
    #[prost(sint64, tag = "4")]
    pub last_lsn: i64,
}

#[derive(Clone, PartialEq, Message)]
pub struct Failed {
    #[prost(string, tag = "1")]
    pub message: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct PageDigest {
    #[prost(sint64, tag = "1")]
    pub through_lsn: i64,
    #[prost(bytes = "bytes", tag = "2")]
    pub digest: Bytes,
    #[prost(sfixed64, optional, tag = "3")]
    pub synced_clock_upper_micros: Option<i64>,
    /// For an exact guarded probe, this journal's local append provenance.
    #[prost(bool, optional, tag = "4")]
    pub original_append: Option<bool>,
    #[prost(sfixed64, optional, tag = "5")]
    pub guarded_before: Option<i64>,
    #[prost(bytes = "bytes", tag = "6")]
    pub timed_origin_identity: Bytes,
}

/// Confirms only the promised writer. It conveys no append or commit position.
#[derive(Clone, PartialEq, Message)]
pub struct AuthorityConfirmed {}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum ResponseBody {
    #[prost(message, tag = "2")]
    Accepted(Accepted),
    #[prost(message, tag = "3")]
    Refused(Refused),
    #[prost(message, tag = "4")]
    Sealed(Sealed),
    #[prost(message, tag = "5")]
    Entries(Entries),
    #[prost(message, tag = "6")]
    Failed(Failed),
    #[prost(message, tag = "7")]
    PageDigest(PageDigest),
    #[prost(message, tag = "8")]
    JournalIdentity(JournalIdentity),
    #[prost(message, tag = "9")]
    AuthorityConfirmed(AuthorityConfirmed),
    #[prost(message, tag = "10")]
    ReadIndex(ReadIndexReply),
    #[prost(message, tag = "11")]
    DecisionVote(DecisionVote),
    #[prost(message, tag = "12")]
    DecisionInstallation(DecisionInstallation),
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Response {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(oneof = "ResponseBody", tags = "2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12")]
    pub body: Option<ResponseBody>,
}

pub(crate) fn encode_frame(message: &impl Message, buffer: &mut BytesMut) {
    let length = message.encoded_len();
    buffer.reserve(4 + length);
    buffer.put_u32(length as u32);
    message.encode(buffer).expect("buffer reserved for the whole message");
}

/// `None` is a clean end of stream between frames.
pub(crate) async fn read_frame<M: Message + Default>(reader: &mut (impl AsyncRead + Unpin)) -> std::io::Result<Option<M>> {
    Ok(read_sized_frame(reader).await?.map(|(message, _)| message))
}

pub(crate) async fn read_sized_frame<M: Message + Default>(reader: &mut (impl AsyncRead + Unpin)) -> std::io::Result<Option<(M, usize)>> {
    read_bounded_frame(reader, MAXIMUM_FRAME_BYTES).await
}

pub(crate) async fn read_bounded_frame<M: Message + Default>(
    reader: &mut (impl AsyncRead + Unpin), limit: usize,
) -> std::io::Result<Option<(M, usize)>> {
    let length = match reader.read_u32().await {
        Ok(length) => length as usize,
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    };
    if length > limit {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("frame of {length} bytes exceeds {limit}")));
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).await?;
    M::decode(Bytes::from(body)).map(|message| Some((message, length))).map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

#[cfg(test)]
#[tokio::test]
async fn a_frame_is_charged_for_its_wire_bytes_even_if_decoding_discards_unknown_fields() {
    let mut frame = [0, 0, 0, 3, 0xf8, 0x07, 1].as_slice();
    let (request, bytes) = read_sized_frame::<Request>(&mut frame).await.unwrap().unwrap();
    assert_eq!(bytes, 3);
    assert_eq!(request.encoded_len(), 0);
    assert!(frame.is_empty());
}

#[cfg(test)]
#[test]
fn a_legacy_peer_cannot_decode_a_durable_trim_as_an_ordinary_trim() {
    #[derive(Message)]
    struct LegacyTrimRequest {
        #[prost(message, optional, tag = "8")]
        trim: Option<Trim>,
        #[prost(string, tag = "17")]
        target_journal: String,
    }

    let trim = Trim { stream: "scope".into(), through_lsn: 4 };
    let request = |body| Request { id: 1, body: Some(body), target_journal: "journal".into() }.encode_to_vec();
    let ordinary = LegacyTrimRequest::decode(request(RequestBody::Trim(trim.clone())).as_slice()).unwrap();
    assert!(ordinary.trim == Some(trim.clone()));
    let durable = LegacyTrimRequest::decode(request(RequestBody::TrimDurable(trim)).as_slice()).unwrap();
    assert!(durable.trim.is_none());
    assert_eq!(durable.target_journal, "journal");
}

#[cfg(test)]
#[test]
fn authority_confirmation_and_durable_trim_use_disjoint_requests() {
    #[derive(Message)]
    struct AuthorityRequest {
        #[prost(message, optional, tag = "18")]
        confirm_authority: Option<LogletCheck>,
    }

    let confirm = AuthorityRequest {
        confirm_authority: Some(LogletCheck {
            stream: "scope".into(),
            epoch: 8,
            ensemble: Bytes::from_static(&[42; 32]),
            certified: false,
            handoff: None,
        }),
    };
    let decoded = Request::decode(confirm.encode_to_vec().as_slice()).unwrap();
    assert!(matches!(decoded.body, Some(RequestBody::ConfirmAuthority(check))
        if check.stream == "scope" && check.epoch == 8 && check.ensemble == &[42; 32][..]));

    let trim = Request {
        id: 1,
        body: Some(RequestBody::TrimDurable(Trim { stream: "scope".into(), through_lsn: 4 })),
        target_journal: "journal".into(),
    };
    let encoded = trim.encode_to_vec();
    let legacy = AuthorityRequest::decode(encoded.as_slice()).unwrap();
    assert!(legacy.confirm_authority.is_none());
    let decoded = Request::decode(encoded.as_slice()).unwrap();
    assert!(matches!(decoded.body, Some(RequestBody::TrimDurable(trim))
        if trim.stream == "scope" && trim.through_lsn == 4));
}

/// Groups consecutive lsns written at the same epoch.
#[cfg(test)]
pub(crate) fn runs_of(entries: impl IntoIterator<Item = (i64, u64)>) -> Vec<EpochRun> {
    let mut runs: Vec<EpochRun> = Vec::new();
    for (lsn, epoch) in entries {
        if runs.last().is_none_or(|run| run.epoch != epoch) { runs.push(EpochRun { start_lsn: lsn, epoch }); }
    }
    runs
}
