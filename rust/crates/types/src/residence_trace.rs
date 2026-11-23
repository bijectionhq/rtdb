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
                eprintln!("Could not start the residence trace: {error}");
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
        eprintln!("Residence trace stopped after a write error: {error}");
    }
}

pub fn enabled() -> bool { sink().is_some() }

pub fn logkeeper_publication(
    stream: &str, epoch: u64, ensemble: &[u8], through: i64, quorum: usize,
    installed: impl FnOnce() -> Vec<usize>,
) {
    record("quorum-publication", || serde_json::json!({
        "stream": stream, "epoch": epoch, "ensemble": ensemble, "through": through,
        "quorum": quorum, "installed": installed(),
    }));
}
