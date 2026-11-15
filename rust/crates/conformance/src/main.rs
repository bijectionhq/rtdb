use rtdb_conformance::{CaseReceipt, Ledger, compare};
use serde::de::DeserializeOwned;
use std::{env, fs, path::Path, process::ExitCode};

fn read<T: DeserializeOwned>(path: &str) -> Result<T, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(&fs::read(Path::new(path))?)?)
}

fn run() -> Result<bool, Box<dyn std::error::Error>> {
    let arguments: Vec<_> = env::args().skip(1).collect();
    if arguments.len() != 4 || arguments[0] != "compare" {
        return Err("usage: rtdb-conformance compare LEDGER.json REFERENCE.json RUST.json".into());
    }
    let ledger: Ledger = read(&arguments[1])?;
    let reference: Vec<CaseReceipt> = read(&arguments[2])?;
    let native: Vec<CaseReceipt> = read(&arguments[3])?;
    let report = compare(&ledger, &reference, &native);
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(report.complete_parity)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => { eprintln!("{error}"); ExitCode::from(2) }
    }
}
