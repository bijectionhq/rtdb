//! Opt-in observations of runtime authority decisions.

use std::{fs::{File, OpenOptions}, io::Write, sync::{Mutex, OnceLock}};

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
                tracing::warn!("Could not start the residence trace: {error}");
                None
            }
        }
    }).as_ref()
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
