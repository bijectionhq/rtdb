use rtdb_conformance::{CaseReceipt, Ledger, compare};
use serde::de::DeserializeOwned;
use std::{env, fs, path::Path, process::ExitCode};

fn read<T: DeserializeOwned>(path: &str) -> Result<T, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(&fs::read(Path::new(path))?)?)
}

fn run() -> Result<bool, Box<dyn std::error::Error>> {
    let arguments: Vec<_> = env::args().skip(1).collect();
    if arguments.len() == 3 && arguments[0] == "backend" {
        let runtime = tokio::runtime::Builder::new_multi_thread().thread_stack_size(rtdb_node::WORKER_STACK_BYTES).enable_all().build()?;
        let execution = runtime.block_on(rtdb_conformance::backend::execute(Path::new(&arguments[1]), Path::new(&arguments[2])))
            .map_err(|error| -> Box<dyn std::error::Error> { error })?;
        println!("{}", serde_json::to_string(&execution)?);
        return Ok(execution["execution_complete"] == true);
    }
    if arguments.len() == 3 && arguments[0] == "recovery" {
        let runtime = tokio::runtime::Builder::new_multi_thread().thread_stack_size(rtdb_node::WORKER_STACK_BYTES).enable_all().build()?;
        let execution = runtime.block_on(rtdb_conformance::history::recovery::execute(Path::new(&arguments[1]), Path::new(&arguments[2])))?;
        println!("{}", serde_json::to_string(&execution)?);
        return Ok(execution["execution_complete"] == true);
    }
    if arguments.len() == 2 && arguments[0] == "observe" {
        let runtime = tokio::runtime::Builder::new_multi_thread().thread_stack_size(rtdb_node::WORKER_STACK_BYTES).enable_all().build()?;
        let execution = runtime.block_on(rtdb_conformance::controls::execute(Path::new(&arguments[1])))?;
        println!("{}", serde_json::to_string(&execution)?);
        return Ok(true);
    }
    if arguments.len() == 3 && arguments[0] == "history" {
        let runtime = tokio::runtime::Builder::new_multi_thread().thread_stack_size(rtdb_node::WORKER_STACK_BYTES).enable_all().build()?;
        let execution = runtime.block_on(rtdb_conformance::history::execute(Path::new(&arguments[1]), Path::new(&arguments[2])))?;
        println!("{}", serde_json::to_string(&execution)?);
        return Ok(execution["execution_complete"] == true);
    }
    if arguments.len() == 2 && matches!(arguments[0].as_str(), "mixed-iid" | "mixed-iid-capture") {
        let runtime = tokio::runtime::Builder::new_multi_thread().thread_stack_size(rtdb_node::WORKER_STACK_BYTES).enable_all().build()?;
        let output = Path::new(&arguments[1]);
        if arguments[0] == "mixed-iid-capture" {
            let execution = runtime.block_on(rtdb_conformance::history::mixed_iid::capture_checkpoint(output))?;
            println!("{}", serde_json::to_string_pretty(&execution)?);
        } else {
            let execution = runtime.block_on(rtdb_conformance::history::mixed_iid::execute(output))?;
            println!("{}", serde_json::to_string(&execution)?);
        }
        return Ok(true);
    }
    if arguments.len() == 3 && arguments[0] == "mixed-iid-replay" {
        let runtime = tokio::runtime::Builder::new_multi_thread().thread_stack_size(rtdb_node::WORKER_STACK_BYTES).enable_all().build()?;
        let execution = runtime.block_on(rtdb_conformance::history::mixed_iid::replay_checkpoint(
            Path::new(&arguments[1]), Path::new(&arguments[2]),
        ))?;
        println!("{}", serde_json::to_string(&execution)?);
        return Ok(true);
    }
    if arguments.len() == 3 && arguments[0] == "parser-wire" {
        let runtime = tokio::runtime::Builder::new_multi_thread().thread_stack_size(rtdb_node::WORKER_STACK_BYTES).enable_all().build()?;
        let execution = runtime.block_on(rtdb_conformance::history::parser_wire::execute(
            Path::new(&arguments[1]), Path::new(&arguments[2]),
        ))?;
        println!("{}", serde_json::to_string(&execution)?);
        return Ok(true);
    }
    if arguments.len() == 3 && arguments[0] == "sql-logic" {
        let runtime = tokio::runtime::Builder::new_multi_thread().thread_stack_size(rtdb_node::WORKER_STACK_BYTES).enable_all().build()?;
        let execution = runtime.block_on(rtdb_conformance::sql_logic::execute(Path::new(&arguments[1]), Path::new(&arguments[2])))?;
        println!("{}", serde_json::to_string(&execution)?);
        return Ok(execution["execution_complete"] == true);
    }
    if arguments.len() == 3 && arguments[0] == "simulation-refused-claim" {
        let runtime = tokio::runtime::Builder::new_multi_thread().thread_stack_size(rtdb_node::WORKER_STACK_BYTES).enable_all().build()?;
        let execution = runtime.block_on(rtdb_conformance::simulation::execute(Path::new(&arguments[1]), Path::new(&arguments[2])))?;
        println!("{}", serde_json::to_string(&execution)?);
        return Ok(execution["execution_complete"] == true);
    }
    if arguments.len() != 4 || arguments[0] != "compare" {
        return Err("usage: rtdb-conformance backend REQUEST.json OUTPUT | observe FIXTURE.json | history FIXTURE.json OUTPUT | mixed-iid OUTPUT | mixed-iid-capture OUTPUT | mixed-iid-replay REQUEST.json OUTPUT | parser-wire FIXTURE.json OUTPUT | recovery REQUEST.json OUTPUT | sql-logic PLAN.json OUTPUT | simulation-refused-claim PROJECTED.json OUTPUT | compare LEDGER.json REFERENCE.json RUST.json".into());
    }
    let ledger: Ledger = read(&arguments[1])?;
    let reference: Vec<CaseReceipt> = read(&arguments[2])?;
    let native: Vec<CaseReceipt> = read(&arguments[3])?;
    let report = compare(&ledger, &reference, &native);
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(report.complete_parity)
}

fn main() -> ExitCode {
    let worker = std::thread::Builder::new()
        .name("rtdb-conformance".into())
        .stack_size(rtdb_node::WORKER_STACK_BYTES)
        .spawn(|| match run() {
            Ok(true) => ExitCode::SUCCESS,
            Ok(false) => ExitCode::FAILURE,
            Err(error) => { eprintln!("{error}"); ExitCode::from(2) }
        });
    match worker {
        Ok(handle) => handle.join().unwrap_or_else(|_| {
            eprintln!("rtdb-conformance worker thread panicked");
            ExitCode::from(2)
        }),
        Err(error) => {
            eprintln!("failed to spawn rtdb-conformance worker thread: {error}");
            ExitCode::from(2)
        }
    }
}
