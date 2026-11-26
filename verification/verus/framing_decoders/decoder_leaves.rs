use crate::{PROTOBUF_LOG_HEADER, PlacedAttempt, RTDB_EXTENSION_LOG_HEADER, ReplicaBody, SourceBody, XTDB_REFUSAL_PREFIX};
use vstd::prelude::*;

pub type SourceBytes = Vec<u8>;

verus! {

pub type Result<T> = core::result::Result<T, &'static str>;

pub uninterp spec fn source_bytes(body: SourceBody) -> Seq<u8>;
pub uninterp spec fn source_from_bytes(bytes: Seq<u8>) -> Option<Option<SourceBody>>;
pub uninterp spec fn replica_from_bytes(bytes: Seq<u8>) -> Option<(Option<i64>, Option<i64>, Option<ReplicaBody>)>;
pub uninterp spec fn valid_stage(body: Option<ReplicaBody>) -> bool;
pub uninterp spec fn attempt_bytes(attempt: PlacedAttempt) -> Seq<u8>;
pub uninterp spec fn attempt_from_bytes(bytes: Seq<u8>) -> Option<PlacedAttempt>;
pub uninterp spec fn replica_bytes(term: i64, sequence: Option<i64>, body: ReplicaBody) -> Seq<u8>;
pub uninterp spec fn replica_extension(body: ReplicaBody) -> bool;

/// The protobuf body of a framed replica record: after the plain header, or after the extension header and its refusal prefix.
pub open spec fn replica_wire_body(bytes: Seq<u8>) -> Option<Seq<u8>> {
    if bytes.len() > 0 && bytes[0] == PROTOBUF_LOG_HEADER {
        Some(bytes.skip(1))
    } else if bytes.len() >= 1 + XTDB_REFUSAL_PREFIX@.len() && bytes[0] == RTDB_EXTENSION_LOG_HEADER
        && bytes.subrange(1, 1 + XTDB_REFUSAL_PREFIX@.len()) == XTDB_REFUSAL_PREFIX@
    {
        Some(bytes.skip(1 + XTDB_REFUSAL_PREFIX@.len()))
    } else {
        None
    }
}

/// How a replica record is framed: a body that uses an RTDB extension takes the extension header and the refusal prefix.
pub open spec fn replica_framed(term: i64, sequence: Option<i64>, body: ReplicaBody) -> Seq<u8> {
    if replica_extension(body) {
        seq![RTDB_EXTENSION_LOG_HEADER] + XTDB_REFUSAL_PREFIX@ + replica_bytes(term, sequence, body)
    } else {
        seq![PROTOBUF_LOG_HEADER] + replica_bytes(term, sequence, body)
    }
}

/// Every framed replica record yields its own protobuf body back.
pub proof fn framed_replica_body(term: i64, sequence: Option<i64>, body: ReplicaBody)
    ensures replica_wire_body(replica_framed(term, sequence, body)) == Some(replica_bytes(term, sequence, body)),
{
    let bytes = replica_framed(term, sequence, body);
    let inner = replica_bytes(term, sequence, body);
    if replica_extension(body) {
        assert(bytes.subrange(1, 1 + XTDB_REFUSAL_PREFIX@.len()) =~= XTDB_REFUSAL_PREFIX@);
        assert(bytes.skip(1 + XTDB_REFUSAL_PREFIX@.len()) =~= inner);
    } else {
        assert(bytes.skip(1) =~= inner);
    }
}

#[verifier::external_body]
pub fn split_header(bytes: &[u8]) -> (out: Option<(u8, &[u8])>)
    ensures (out is Some) == (bytes@.len() > 0),
        out matches Some((header, body)) ==> header == bytes@[0] && body@ == bytes@.skip(1),
{ unimplemented!() }

#[verifier::external_body]
pub fn unknown_header(header: u8, kind: &str) -> &'static str { unimplemented!() }

#[verifier::external_body]
pub fn decode_source(bytes: &[u8]) -> (out: Result<Option<SourceBody>>)
    ensures match out {
        Ok(value) => source_from_bytes(bytes@) == Some(value),
        Err(_) => source_from_bytes(bytes@).is_none(),
    },
{ unimplemented!() }

#[verifier::external_body]
pub fn encode_source(body: &SourceBody) -> (out: Vec<u8>)
    ensures out@ == seq![PROTOBUF_LOG_HEADER] + source_bytes(*body),
{ unimplemented!() }

#[verifier::external_body]
pub proof fn prost_source_round_trips(body: SourceBody)
    ensures source_from_bytes(source_bytes(body)) == Some(Some(body)),
{ unimplemented!() }

#[verifier::external_body]
pub fn decode_replica(bytes: &[u8]) -> (out: Result<(Option<i64>, Option<i64>, Option<ReplicaBody>)>)
    ensures match out {
        Ok(value) => replica_from_bytes(bytes@) == Some(value),
        Err(_) => replica_from_bytes(bytes@).is_none(),
    },
{ unimplemented!() }

#[verifier::external_body]
pub fn encode_replica(term: i64, sequence: Option<i64>, message: &ReplicaBody) -> (out: Vec<u8>)
    ensures out@ == replica_framed(term, sequence, *message),
{ unimplemented!() }

#[verifier::external_body]
pub fn check_stage(body: &Option<ReplicaBody>) -> (out: Result<()>)
    ensures (out is Ok) == valid_stage(*body),
{ unimplemented!() }

#[verifier::external_body]
pub fn decode_attempt(bytes: &[u8]) -> (out: Result<PlacedAttempt>)
    ensures match out {
        Ok(value) => attempt_from_bytes(bytes@) == Some(value) && bytes@ == attempt_bytes(value) && bytes@.len() == 96,
        Err(_) => attempt_from_bytes(bytes@).is_none(),
    },
{ unimplemented!() }

#[verifier::external_body]
pub fn encode_attempt(attempt: PlacedAttempt) -> (out: [u8; 96])
    ensures out@ == attempt_bytes(attempt), attempt_from_bytes(out@) == Some(attempt), out@.len() == 96,
{ unimplemented!() }

#[verifier::external_body]
pub fn strip_prefix<'a>(bytes: &'a [u8], prefix: &[u8]) -> (out: Option<&'a [u8]>)
    ensures (out is Some) == (prefix@.len() <= bytes@.len() && bytes@.take(prefix@.len() as int) == prefix@),
        out matches Some(body) ==> bytes@ == prefix@ + body@ && body@ == bytes@.skip(prefix@.len() as int),
{ unimplemented!() }

#[verifier::external_body]
pub fn legacy_source(bytes: &[u8]) -> (out: SourceBytes)
    ensures out@ == bytes@,
{ unimplemented!() }

#[verifier::external_body]
pub fn join_bytes(prefix: &[u8], body: &[u8]) -> (out: Vec<u8>)
    ensures out@ == prefix@ + body@,
{ unimplemented!() }

#[verifier::external_body]
pub proof fn prost_replica_round_trips(term: i64, sequence: Option<i64>, body: ReplicaBody)
    ensures replica_from_bytes(replica_bytes(term, sequence, body)) == Some((Some(term), sequence, Some(body))),
{ unimplemented!() }

}
