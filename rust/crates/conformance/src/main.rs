use rtdb_conformance::{CaseReceipt, Ledger, compare};
use serde::de::DeserializeOwned;
use std::{env, fs, path::Path, process::ExitCode};

fn read<T: DeserializeOwned>(path: &str) -> Result<T, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(&fs::read(Path::new(path))?)?)
}

fn run() -> Result<bool, Box<dyn std::error::Error>> {
    let arguments: Vec<_> = env::args().skip(1).collect();
    if arguments.len() == 3 && arguments[0] == "backend" {
        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
        let execution = runtime.block_on(rtdb_conformance::backend::execute(Path::new(&arguments[1]), Path::new(&arguments[2])))
            .map_err(|error| -> Box<dyn std::error::Error> { error })?;
        println!("{}", serde_json::to_string(&execution)?);
        return Ok(execution["execution_complete"] == true);
    }
    if arguments.len() == 3 && arguments[0] == "recovery" {
        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
        let execution = runtime.block_on(rtdb_conformance::history::recovery::execute(Path::new(&arguments[1]), Path::new(&arguments[2])))?;
        println!("{}", serde_json::to_string(&execution)?);
        return Ok(execution["execution_complete"] == true);
    }
    if arguments.len() == 2 && arguments[0] == "observe" {
        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
        let execution = runtime.block_on(rtdb_conformance::controls::execute(Path::new(&arguments[1])))?;
        println!("{}", serde_json::to_string(&execution)?);
        return Ok(true);
    }
    if arguments.len() == 3 && arguments[0] == "history" {
        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
        let execution = runtime.block_on(rtdb_conformance::history::execute(Path::new(&arguments[1]), Path::new(&arguments[2])))?;
        println!("{}", serde_json::to_string(&execution)?);
        return Ok(execution["execution_complete"] == true);
    }
    if arguments.len() == 3 && arguments[0] == "sql-logic" {
        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
        let execution = runtime.block_on(rtdb_conformance::sql_logic::execute(Path::new(&arguments[1]), Path::new(&arguments[2])))?;
        println!("{}", serde_json::to_string(&execution)?);
        return Ok(execution["execution_complete"] == true);
    }
    if arguments.len() != 4 || arguments[0] != "compare" {
        return Err("usage: rtdb-conformance backend REQUEST.json OUTPUT | observe FIXTURE.json | history FIXTURE.json OUTPUT | recovery REQUEST.json OUTPUT | sql-logic PLAN.json OUTPUT | compare LEDGER.json REFERENCE.json RUST.json".into());
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
