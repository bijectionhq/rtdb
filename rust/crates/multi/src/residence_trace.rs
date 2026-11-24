//! Opt-in observations of residence authority decisions.

use rtdb_format::envelope::{ReplicaBody, ReplicaEnvelope, decode_replica_outcome_attempt};
use rtdb_storage::log::LogMetadata;
use rtdb_types::PlacedAttempt;

pub(crate) use rtdb_types::residence_trace::{enabled, record};

pub(crate) struct AttemptOutcome {
    attempt: PlacedAttempt,
    committed: bool,
    system_time: i64,
}

impl AttemptOutcome {
    pub(crate) fn from_replica(bytes: &[u8]) -> Option<Self> {
        if !enabled() { return None; }
        let ReplicaBody::ResolvedTx(tx) = ReplicaEnvelope::decode(bytes).ok()??.message else { return None; };
        let (attempt, _) = decode_replica_outcome_attempt(tx.placed_attempt.as_deref()?).ok()?;
        Some(Self { attempt, committed: tx.committed?, system_time: tx.system_time_micros? })
    }

    pub(crate) fn observe(self, metadata: LogMetadata) {
        record("attempt-outcome", || serde_json::json!({
            "history": self.attempt.scope().history_fingerprint(), "epoch": self.attempt.scope().writer_epoch(),
            "attempt": self.attempt.outcome_key(), "through": metadata.offset.0, "append_time": metadata.timestamp.0,
            "system_time": self.system_time, "committed": self.committed,
        }));
    }
}
