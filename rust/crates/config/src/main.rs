use std::{io::{Read, Write}, path::Path, process::ExitCode, sync::Arc};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_config::{InputFormat, MAX_INPUT_BYTES, qualify, qualify_with_classic_s3_inventory};
use zeroize::Zeroizing;

const USAGE: &str = "Usage: rtdb-config <yaml|ingest-yaml|edn> CONFIG ENVIRONMENT.json PROVENANCE.json [--classic-s3-key-inventory INVENTORY.json]";

struct Input { bytes: Zeroizing<Vec<u8>>, _reservation: Arc<MemoryReservation> }
impl Input {
    fn read(path: &Path, memory: &Arc<MemoryBudget>) -> Result<Self, &'static str> {
        let reservation = memory.reserve((MAX_INPUT_BYTES + 1) * 2).map_err(|_| "Input allocation limit exceeded")?;
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        { use std::os::unix::fs::OpenOptionsExt; options.custom_flags(libc::O_NONBLOCK); }
        let file = options.open(path).map_err(|_| "Cannot open a declared input file")?;
        if !file.metadata().map_err(|_| "Cannot inspect a declared input file")?.is_file() { return Err("Declared inputs must be regular files"); }
        let mut bytes = Zeroizing::new(Vec::new());
        file.take((MAX_INPUT_BYTES + 1) as u64).read_to_end(&mut bytes).map_err(|_| "Cannot read a declared input file")?;
        if bytes.len() > MAX_INPUT_BYTES { return Err("A declared input exceeds one MiB"); }
        Ok(Self { bytes, _reservation: reservation })
    }
}
fn run() -> Result<ExitCode, &'static str> {
    let mut args = std::env::args_os().skip(1);
    let Some(format) = args.next() else { return Err(USAGE); };
    if format == "--build-info" {
        if args.next().is_some() { return Err("--build-info accepts no arguments"); }
        let mut output = std::io::stdout().lock();
        serde_json::to_writer(&mut output, &rtdb_types::compiled_build_info()).map_err(|_| "Cannot write build information")?;
        writeln!(output).map_err(|_| "Cannot write build information")?;
        output.flush().map_err(|_| "Cannot flush build information")?;
        return Ok(ExitCode::SUCCESS);
    }
    if format == "--help" {
        writeln!(std::io::stdout().lock(), "{USAGE}\n       rtdb-config --build-info\nReads only explicit inputs and prints a redacted JSON qualification report.\nExit 2 means ineligible for cutover. Exit 1 means input or output failure.").map_err(|_| "Cannot write help")?;
        return Ok(ExitCode::SUCCESS);
    }
    let format = match format.to_str() { Some("yaml") => InputFormat::Yaml, Some("ingest-yaml") => InputFormat::IngestYaml, Some("edn") => InputFormat::Edn, _ => return Err("Unknown input format") };
    let config = args.next().ok_or("Missing configuration path")?;
    let environment = args.next().ok_or("Missing explicit environment JSON path")?;
    let provenance = args.next().ok_or("Missing provenance JSON path")?;
    let s3_inventory = match args.next() {
        Some(flag) if flag == "--classic-s3-key-inventory" => Some(args.next().ok_or("Missing Classic S3 inventory JSON path")?),
        Some(_) => return Err("Unexpected argument"),
        None => None,
    };
    if args.next().is_some() { return Err("Unexpected argument"); }
    let memory = MemoryBudget::new(1024 * 1024 * 1024);
    let config = Input::read(Path::new(&config), &memory)?;
    let environment = Input::read(Path::new(&environment), &memory)?;
    let provenance = Input::read(Path::new(&provenance), &memory)?;
    let text = std::str::from_utf8(&config.bytes).map_err(|_| "Configuration must contain UTF-8 text")?;
    let report = match s3_inventory {
        Some(path) => {
            let inventory = Input::read(Path::new(&path), &memory)?;
            qualify_with_classic_s3_inventory(text, format, &environment.bytes, &provenance.bytes, &inventory.bytes, &memory)
        }
        None => qualify(text, format, &environment.bytes, &provenance.bytes, &memory),
    }.map_err(|_| "Qualification input or allocation limit exceeded")?;
    let mut output = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut output, &report).map_err(|_| "Cannot write qualification report")?;
    writeln!(output).map_err(|_| "Cannot write qualification report")?;
    output.flush().map_err(|_| "Cannot flush qualification report")?;
    Ok(if report.cutover_eligible { ExitCode::SUCCESS } else { ExitCode::from(2) })
}
fn main() -> ExitCode {
    match run() { Ok(code) => code, Err(error) => { let _ = writeln!(std::io::stderr().lock(), "rtdb-config: {error}"); ExitCode::FAILURE } }
}
