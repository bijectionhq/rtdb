use super::{Anomaly, ReplicaBody, decoder::{ReplicaEnvelope, decode_replica_envelope}, invalid};
use rtdb_types::Result;
use sha2::{Digest, Sha256};

fn digest_matches(expected: &[u8], bytes: &[u8]) -> bool {
    expected == Sha256::digest(bytes).as_slice()
}

impl ReplicaEnvelope {
    /// Checks the fetched record against the already-admitted envelope without admitting it a second time.
    pub fn unwrap_payload(&self, bytes: &[u8]) -> Result<Self> {
        let ReplicaBody::OversizedMessage(reference) = &self.message else {
            return Err(invalid("Only an oversized envelope has an external payload"));
        };
        if let Some(expected) = &reference.payload_sha256 {
            if !digest_matches(expected.as_slice(), bytes) {
                return Err(Anomaly::fault("rtdb.format/oversized-payload-mismatch", "Oversized payload bytes differ from their replica reference"));
            }
        }
        let Some(payload) = decode_replica_envelope(bytes)? else {
            return Err(invalid("Oversized payload has no replica message"));
        };
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
        Ok(payload)
    }
}
