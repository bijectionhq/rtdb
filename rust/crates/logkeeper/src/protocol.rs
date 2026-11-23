//! The wire protocol: a stream of frames, each a big-endian u32 length followed by one prost message.
//! A client sends `Request`s and receives `Response`s carrying the same id, in any order.
//! One connection multiplexes every stream; a server decides a connection's mutations in arrival order.

use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt};

pub(crate) const MAXIMUM_FRAME_BYTES: usize = 256 * 1024 * 1024;

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
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Seal {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
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
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Commit {
    #[prost(string, tag = "1")]
    pub stream: String,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
    #[prost(sint64, tag = "3")]
    pub commit_lsn: i64,
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
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Identify {}

#[derive(Clone, PartialEq, Message)]
pub struct JournalIdentity {
    #[prost(string, tag = "1")]
    pub journal: String,
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
    #[prost(message, tag = "15")]
    ReadIndex(LogletCheck),
    #[prost(message, tag = "16")]
    Identify(Identify),
    #[prost(message, tag = "18")]
    TrimDurable(Trim),
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Request {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(oneof = "RequestBody", tags = "2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18")]
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
}

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
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Response {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(oneof = "ResponseBody", tags = "2, 3, 4, 5, 6, 7, 8")]
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
    let length = match reader.read_u32().await {
        Ok(length) => length as usize,
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    };
    if length > MAXIMUM_FRAME_BYTES {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("frame of {length} bytes exceeds {MAXIMUM_FRAME_BYTES}")));
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

/// Groups consecutive lsns written at the same epoch.
#[cfg(test)]
pub(crate) fn runs_of(entries: impl IntoIterator<Item = (i64, u64)>) -> Vec<EpochRun> {
    let mut runs: Vec<EpochRun> = Vec::new();
    for (lsn, epoch) in entries {
        if runs.last().is_none_or(|run| run.epoch != epoch) { runs.push(EpochRun { start_lsn: lsn, epoch }); }
    }
    runs
}
