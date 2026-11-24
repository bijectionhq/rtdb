#![allow(unexpected_cfgs)]

use super::{Anomaly, ReplicaBody, decoder::{ReplicaEnvelope, decode_replica_envelope}, invalid};
#[cfg(not(verus_keep_ghost))]
use rtdb_types::Result;
#[cfg(verus_keep_ghost)]
use super::decoder_leaves::Result;
#[cfg(verus_keep_ghost)]
use super::digest_leaf::{digest_matches, sha256};
#[cfg(verus_keep_ghost)]
use vstd::prelude::*;
#[cfg(not(verus_keep_ghost))]
use prost::bytes::Bytes;
#[cfg(not(verus_keep_ghost))]
use sha2::{Digest, Sha256};

#[cfg(not(verus_keep_ghost))]
fn digest_matches(expected: &[u8], bytes: &[u8]) -> bool {
    expected == Sha256::digest(bytes).as_slice()
}

#[cfg(verus_keep_ghost)]
verus! {

pub open spec fn accepts_detached_frame(proxy: ReplicaEnvelope, payload: ReplicaEnvelope, bytes: Seq<u8>) -> bool {
    proxy.message matches ReplicaBody::OversizedMessage(reference)
        && (reference.payload_sha256 matches Some(expected) ==> expected@ == sha256(bytes))
        && payload.term == proxy.term
        && payload.term_sequence == proxy.term_sequence
        && !(payload.message is OversizedMessage)
        && (payload.message matches ReplicaBody::ResolvedTx(tx) ==> tx.acceptance_stage is None)
}

}

impl ReplicaEnvelope {
    /// Checks the fetched record against the already-admitted envelope without admitting it a second time.
    #[cfg_attr(verus_keep_ghost, verus_spec(out =>
        ensures out matches Ok(payload) ==> accepts_detached_frame(*self, payload, bytes@)
            && bytes@.len() > 0 && bytes@[0] == super::PROTOBUF_LOG_HEADER
            && (super::decoder_leaves::replica_from_bytes(bytes@.skip(1)) matches Some((term, seq, body))
                && payload.term.0 == term.unwrap_or(0)
                && payload.term_sequence == match seq { Some(value) => Some(super::TermSequence(value)), None => None }
                && body == Some(payload.message)),
    ))]
    pub fn unwrap_payload(&self, bytes: &[u8]) -> Result<Self> {
        self.check_payload_digest(bytes)?;
        let Some(payload) = decode_replica_envelope(bytes)? else {
            return Err(invalid("Oversized payload has no replica message"));
        };
        self.check_payload_envelope(&payload)?;
        Ok(payload)
    }

    /// Decoded protobuf byte fields can retain the fetched allocation.
    #[cfg(not(verus_keep_ghost))]
    pub fn unwrap_payload_bytes(&self, bytes: Bytes) -> Result<Self> {
        self.check_payload_digest(&bytes)?;
        let Some(payload) = super::ReplicaFrame::decode_bytes(bytes)?.into_envelope() else {
            return Err(invalid("Oversized payload has no replica message"));
        };
        self.check_payload_envelope(&payload)?;
        Ok(payload)
    }

    #[cfg_attr(verus_keep_ghost, verus_spec(out =>
        ensures out is Ok ==> (self.message matches ReplicaBody::OversizedMessage(reference)
            && (reference.payload_sha256 matches Some(expected) ==> expected@ == sha256(bytes@))),
    ))]
    fn check_payload_digest(&self, bytes: &[u8]) -> Result<()> {
        let ReplicaBody::OversizedMessage(reference) = &self.message else {
            return Err(invalid("Only an oversized envelope has an external payload"));
        };
        if let Some(expected) = &reference.payload_sha256 {
            if !digest_matches(expected.as_slice(), bytes) {
                return Err(Anomaly::fault("rtdb.format/oversized-payload-mismatch", "Oversized payload bytes differ from their replica reference"));
            }
        }
        Ok(())
    }

    #[cfg_attr(verus_keep_ghost, verus_spec(out =>
        ensures out is Ok ==> payload.term == self.term && payload.term_sequence == self.term_sequence
            && !(payload.message is OversizedMessage)
            && (payload.message matches ReplicaBody::ResolvedTx(tx) ==> tx.acceptance_stage is None),
    ))]
    fn check_payload_envelope(&self, payload: &Self) -> Result<()> {
        let same_sequence = match (payload.term_sequence, self.term_sequence) {
            (Some(observed), Some(expected)) => observed.0 == expected.0,
            (None, None) => true,
            _ => false,
        };
        if payload.term.0 != self.term.0 || !same_sequence {
            return Err(Anomaly::fault("rtdb.format/oversized-term-mismatch", "Oversized payload term or sequence differs from its envelope"));
        }
        if matches!(payload.message, ReplicaBody::OversizedMessage(_)) {
            return Err(invalid("Nested oversized replica messages are invalid"));
        }
        if matches!(&payload.message, ReplicaBody::ResolvedTx(tx) if tx.acceptance_stage.is_some()) {
            return Err(invalid("Strict acceptance requires an inline replica record"));
        }
        Ok(())
    }
}
