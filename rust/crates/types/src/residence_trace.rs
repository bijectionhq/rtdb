//! Opt-in observations of runtime authority decisions.
//!
//! Callers install the output path explicitly via [`install`] (the server and Multi binaries do so
//! from `RTDB_RESIDENCE_TRACE_PATH` at startup); this library reads no environment.

use std::{fs::{File, OpenOptions}, io::Write, path::PathBuf, sync::{Mutex, OnceLock}};

use serde::Serialize;
use serde_json::Value;

struct TraceSink {
    path: PathBuf,
    file: Mutex<Option<File>>,
}

static TRACE: OnceLock<TraceSink> = OnceLock::new();

#[derive(Serialize)]
struct Observation {
    event: &'static str,
    #[serde(flatten)]
    fields: Value,
}

/// Installs the process's residence trace file.
///
/// Passing `None` leaves tracing disabled (and fails if a trace path was already installed).
/// Passing `Some(path)` creates a new JSONL file (`0o600` on Unix) and enables recording into it;
/// re-installing the same path succeeds, while installing a different path fails.
pub fn install(path: Option<PathBuf>) -> crate::Result<()> {
    let Some(path) = path else {
        if TRACE.get().is_some() {
            return Err(crate::Anomaly::fault(
                "rtdb.residence-trace/already-installed",
                "A residence trace path was already installed",
            ));
        }
        return Ok(());
    };
    if let Some(existing) = TRACE.get() {
        return if existing.path == path {
            Ok(())
        } else {
            Err(crate::Anomaly::fault(
                "rtdb.residence-trace/already-installed",
                "A different residence trace path was already installed",
            ))
        };
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(&path).map_err(|error| {
        crate::Anomaly::fault(
            "rtdb.residence-trace/open-failed",
            format!("Could not start the residence trace at {}: {error}", path.display()),
        )
    })?;
    match TRACE.set(TraceSink { path: path.clone(), file: Mutex::new(Some(file)) }) {
        Ok(()) => Ok(()),
        Err(_) => {
            if TRACE.get().is_some_and(|existing| existing.path == path) {
                Ok(())
            } else {
                Err(crate::Anomaly::fault(
                    "rtdb.residence-trace/already-installed",
                    "A different residence trace path was already installed",
                ))
            }
        }
    }
}

fn sink() -> Option<&'static Mutex<Option<File>>> {
    TRACE.get().map(|sink| &sink.file)
}

pub fn record(event: &'static str, fields: impl FnOnce() -> Value) {
    let Some(trace) = sink() else { return; };
    let Ok(mut trace) = trace.lock() else { return; };
    let Some(file) = trace.as_mut() else { return; };
    let mut bytes = serde_json::to_vec(&Observation { event, fields: fields() }).expect("residence trace fields are JSON");
    bytes.push(b'\n');
    if let Err(error) = file.write_all(&bytes) {
        *trace = None;
        tracing::warn!("Residence trace stopped after a write error: {error}");
    }
}

pub fn enabled() -> bool { sink().is_some() }

pub fn logkeeper_publication(
    stream: &str, epoch: u64, ensemble: &[u8], through: i64, quorum: usize,
    installed: impl FnOnce() -> Vec<usize>, decision: Option<[u8; 32]>,
) {
    record("quorum-publication", || serde_json::json!({
        "stream": stream, "epoch": epoch, "ensemble": ensemble, "through": through,
        "quorum": quorum, "installed": installed(), "decision": decision,
    }));
}

pub fn logkeeper_receipt(
    stream: &str, epoch: u64, ensemble: &[u8], through: i64, decision_through: i64, decision: Option<[u8; 32]>,
) {
    record("quorum-receipt", || serde_json::json!({
        "stream": stream, "epoch": epoch, "ensemble": ensemble, "through": through,
        "decision_through": decision_through, "decision": decision,
    }));
}


/// Correlates one authenticated loglet decision with the catalog selected for its read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CertifiedRead {
    pub invocation: [u8; 16],
    pub stream: String,
    pub epoch: u64,
    pub ensemble: [u8; 32],
    pub through: i64,
    pub decision: [u8; 32],
}

impl CertifiedRead {
    pub fn observe(&self, event: &'static str) {
        record(event, || serde_json::to_value(self).expect("read identity encodes"));
    }
}

#[derive(Serialize)]
pub struct CertifiedDecision<'a> {
    pub stream: &'a str,
    pub epoch: u64,
    pub ensemble: &'a [u8],
    pub through: i64,
    pub predecessor: i64,
    pub decision: [u8; 32],
    pub statement: Vec<u8>,
    pub prefix_digest: &'a [u8],
    pub predecessor_digest: &'a [u8],
    pub roster: &'a [String],
    pub quorum: u32,
    pub voters: Vec<String>,
    pub installers: Vec<String>,
}

pub fn logkeeper_decision<'a>(decision: impl FnOnce() -> CertifiedDecision<'a>) {
    record("certified-decision", || serde_json::to_value(decision()).expect("decision identity encodes"));
}


#[derive(Serialize)]
pub struct RepairInventory<'a> {
    pub residence: &'a str,
    pub anchor_block: i64,
    pub anchor_boundary: i64,
    pub anchor_root: [u8; 32],
    pub inventory: [u8; 32],
    pub observed_inventory: [u8; 32],
    pub target: Option<RepairInventoryTarget>,
}

#[derive(Serialize)]
pub struct RepairInventoryTarget {
    pub invocation: uuid::Uuid,
    pub block: i64,
    pub boundary: i64,
    pub root: [u8; 32],
}

impl RepairInventory<'_> {
    pub fn observe(&self, event: &'static str) {
        record(event, || serde_json::to_value(self).expect("repair inventory encodes"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn residence_trace_is_disabled_until_installed_explicitly() {
        assert!(!enabled());
        install(None).unwrap();
        let path = std::env::temp_dir().join(format!("rtdb-residence-trace-{}.jsonl", uuid::Uuid::new_v4()));
        install(Some(path.clone())).unwrap();
        assert!(enabled());
        // Re-installing the same path is idempotent; a different path or disabling fails.
        install(Some(path.clone())).unwrap();
        assert!(install(Some(path.with_extension("other"))).is_err());
        assert!(install(None).is_err());
        record("test-event", || serde_json::json!({ "ok": true }));
        let line = std::fs::read_to_string(&path).unwrap();
        let parsed: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(parsed["event"], "test-event");
        assert_eq!(parsed["ok"], true);
        let _ = std::fs::remove_file(&path);
    }
}
