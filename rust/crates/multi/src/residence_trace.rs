//! Opt-in observations of residence authority decisions.

use std::{fs::{File, OpenOptions}, io::Write, sync::OnceLock};

use parking_lot::Mutex;
use rtdb_format::envelope::{ReplicaBody, ReplicaEnvelope, decode_replica_outcome_attempt};
use rtdb_storage::log::LogMetadata;
use rtdb_types::PlacedAttempt;
use serde::Serialize;
use serde_json::Value;

static TRACE: OnceLock<Option<Mutex<Option<File>>>> = OnceLock::new();

#[derive(Serialize)]
struct Observation {
    event: &'static str,
    #[serde(flatten)]
    fields: Value,
}

fn sink() -> Option<&'static Mutex<Option<File>>> {
    TRACE.get_or_init(|| {
        let path = std::env::var_os("RTDB_RESIDENCE_TRACE_PATH")?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(path) {
            Ok(file) => Some(Mutex::new(Some(file))),
            Err(error) => {
                eprintln!("Could not start the residence trace: {error}");
                None
            }
        }
    }).as_ref()
}

pub(crate) fn record(event: &'static str, fields: impl FnOnce() -> Value) {
    let Some(trace) = sink() else { return; };
    let mut trace = trace.lock();
    let Some(file) = trace.as_mut() else { return; };
    let mut bytes = serde_json::to_vec(&Observation { event, fields: fields() }).expect("residence trace fields are JSON");
    bytes.push(b'\n');
    if let Err(error) = file.write_all(&bytes) {
        *trace = None;
        eprintln!("Residence trace stopped after a write error: {error}");
    }
}

pub(crate) struct AttemptOutcome {
    attempt: PlacedAttempt,
    committed: bool,
    system_time: i64,
}

impl AttemptOutcome {
    pub(crate) fn from_replica(bytes: &[u8]) -> Option<Self> {
        sink()?;
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
