#![allow(non_snake_case)]

use crate::inputs::{ReplicaBody, Result};
use crate::log_headers::{PROTOBUF_LOG_HEADER, RTDB_EXTENSION_LOG_HEADER, XTDB_REFUSAL_PREFIX};
use vstd::prelude::*;

verus! {

pub struct ReplicaEnvelope {
    pub message: ReplicaBody,
    pub term: i64,
    pub sequence: Option<i64>,
}

pub uninterp spec fn replica_from_bytes(bytes: Seq<u8>) -> Option<(i64, Option<i64>, ReplicaBody)>;

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

pub open spec fn payloadKey(envelope: &ReplicaEnvelope) -> Option<u64> {
    match envelope.message { ReplicaBody::OversizedMessage(key) => Some(key), _ => None }
}

impl ReplicaEnvelope {
    #[verifier::external_body]
    pub fn oversized_payload(&self, version: i32, epoch: i32) -> (out: Result<Option<u64>>)
        ensures out matches Ok(path) ==> path == payloadKey(self),
    { unimplemented!() }

    #[verifier::external_body]
    pub fn unwrap_payload(&self, bytes: &Vec<u8>) -> (out: Result<ReplicaEnvelope>)
        ensures out matches Ok(decoded) ==> decoded.term == self.term
            && decoded.sequence == self.sequence
            && !(decoded.message is OversizedMessage)
            && replica_wire_body(bytes@) matches Some(wire)
            && replica_from_bytes(wire) == Some((decoded.term, decoded.sequence, decoded.message)),
    { unimplemented!() }
}

pub struct PayloadSource { pub objects: Ghost<Map<u64, Seq<u8>>> }
impl PayloadSource {
    #[verifier::external_body]
    pub fn read(&self, key: &u64) -> (out: Result<Vec<u8>>)
        ensures out matches Ok(bytes) ==> self.objects@.dom().contains(*key)
            && bytes@ == self.objects@[*key],
    { unimplemented!() }
}

}
