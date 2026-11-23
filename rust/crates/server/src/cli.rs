use std::{fs::File, io::{BufRead, Read}, path::{Path, PathBuf}};
use rtdb_auth::{PasswordHash, Secret};
use rtdb_columnar::MemoryBudget;
use rtdb_types::{Anomaly, Result};
use zeroize::Zeroizing;

use crate::config::{NodeConfiguration, ParsedConfiguration, ProcessEnvironment, PgwireSettings, FlightSettings};

pub const HELP: &str = "RTDB\n\nUsage: rtdb --build-info\n       rtdb [node|ingest|playground|compactor] [-f FILE] [--memory-limit BYTES]\n       rtdb [export-snapshot|reset-compactor] DATABASE [-f FILE] [--dry-run] [--memory-limit BYTES]\n       rtdb reset-compactor DATABASE --retained-base PATH [-f FILE] [--dry-run]\n       rtdb repair-checkpoint STORAGE REPLICA_LOG OUTPUT_PARENT BASE@SHA256 THROUGH@SHA256 [--base-storage PATH] [--memory-limit BYTES]\n       rtdb repair-checkpoint STORAGE REPLICA_LOG OUTPUT_PARENT THROUGH@SHA256 --retained-base --base-storage PATH [--memory-limit BYTES]\n       rtdb retain-repair-base STORAGE REPLICA_LOG EXPORTED_STORAGE BASE@SHA256 [--memory-limit BYTES]\n       rtdb hash-password [--argon2id|--bcrypt] [PASSWORD]\n\nNode defaults to xtdb.yaml, xtdb.yml or xtdb.edn in the working directory.\nIngest configuration uses YAML.\nPlayground accepts --port PORT and --flight-sql-port PORT.\nStartup accepts -Djava.util.Arrays.useLegacyMergeSort=true before or after the command.\nPasswords may be read from standard input.\nFile inspection: read-arrow-file, read-arrow-stream-file, read-hash-trie-file,\n                 read-table-block-file, read-block-file PATH [--memory-limit BYTES].\nInspection output is typed JSON.\nMemory defaults to RTDB_MEMORY_LIMIT_BYTES or 1073741824 bytes.\n";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeMode { Node, Ingest, Playground, Compactor }
#[derive(Clone, Debug)]
pub struct StartOptions { pub mode: NodeMode, pub file: Option<PathBuf>, pub allocation_limit_bytes: usize, pub pgwire_port: i32, pub flight_sql_port: i32, pub object_sort: rtdb_types::ObjectSort }
#[derive(Clone, Debug)]
pub enum Command { Help, Version, BuildInfo, Start(StartOptions), Inspect { kind: crate::inspect::FileKind, path: PathBuf, allocation_limit_bytes: usize }, SnapshotExport { options: StartOptions, database: String, dry_run: bool }, CompactorReset { options: StartOptions, database: String, dry_run: bool, retained_base: Option<PathBuf> }, RepairCheckpoint { request: rtdb_node::LocalRepair, memory_bytes: usize },
    RepairRetainedCheckpoint { request: rtdb_node::RetainedLocalRepair, memory_bytes: usize },
    RetainRepairBase { request: rtdb_node::RetainRepairBase, memory_bytes: usize }, HashPassword { bcrypt: bool, password: Option<Secret> } }

pub fn parse(arguments: impl IntoIterator<Item = String>, environment: &impl Fn(&str) -> Option<String>) -> Result<Command> {
    let mut arguments = arguments.into_iter().peekable();
    let mut object_sort = rtdb_types::ObjectSort::default();
    while arguments.peek().is_some_and(|argument|argument.starts_with("-D")) {
        object_sort = crate::config::properties::command_property(&arguments.next().expect("peeked property"))?;
    }
    if matches!(arguments.peek().map(String::as_str), Some("help" | "--help" | "-h")) { return Ok(Command::Help); }
    if matches!(arguments.peek().map(String::as_str), Some("--version" | "-V")) { return Ok(Command::Version); }
    if arguments.peek().is_some_and(|argument| argument == "--build-info") {
        arguments.next();
        if arguments.next().is_some() { return Err(invalid("--build-info accepts no arguments")); }
        return Ok(Command::BuildInfo);
    }
    if matches!(arguments.peek().map(String::as_str), Some("repair-checkpoint" | "retain-repair-base")) {
        let command = arguments.next().expect("peeked repair command");
        let retaining = command == "retain-repair-base";
        if matches!(arguments.peek().map(String::as_str), Some("-h" | "--help")) { return Ok(Command::Help); }
        let mut positional = Vec::new();
        let mut base_storage = None;
        let mut retained_base = false;
        let mut memory_bytes = environment("RTDB_MEMORY_LIMIT_BYTES").map(|value| bytes(&value)).transpose()?.unwrap_or(1024 * 1024 * 1024);
        while let Some(argument) = arguments.next() {
            if argument == "--memory-limit" {
                memory_bytes = bytes(&arguments.next().ok_or_else(|| invalid("--memory-limit requires bytes"))?)?;
            } else if argument == "--retained-base" && !retaining {
                retained_base = true;
            } else if argument == "--base-storage" && !retaining {
                base_storage = Some(PathBuf::from(arguments.next().ok_or_else(|| invalid("--base-storage requires a path"))?));
            } else if argument.starts_with('-') {
                return Err(invalid(format!("Unknown repair option '{argument}'")));
            } else {
                positional.push(argument);
            }
        }
        if retaining {
            let [source, replica, exported, base]: [String; 4] = positional.try_into()
                .map_err(|_| invalid("retain-repair-base requires storage, a replica log, an exported store, and one checkpoint receipt"))?;
            let request = rtdb_node::RetainRepairBase {
                source_storage: source.into(), replica_log: replica.into(), base_storage: exported.into(), base: repair_checkpoint(&base)?,
            };
            return Ok(Command::RetainRepairBase { request, memory_bytes });
        }
        if retained_base {
            let [source, replica, output, through]: [String; 4] = positional.try_into()
                .map_err(|_| invalid("Repair from the retained base requires storage, a replica log, an output parent, and one target receipt"))?;
            let request = rtdb_node::RetainedLocalRepair {
                source_storage: source.into(), replica_log: replica.into(), output_parent: output.into(),
                base_storage: base_storage.ok_or_else(|| invalid("--retained-base requires --base-storage"))?,
                through: repair_checkpoint(&through)?,
            };
            return Ok(Command::RepairRetainedCheckpoint { request, memory_bytes });
        }
        let [source, replica, output, base, through]: [String; 5] = positional.try_into()
            .map_err(|_| invalid("repair-checkpoint requires storage, a replica log, an output parent, and two checkpoint receipts"))?;
        let request = rtdb_node::LocalRepair {
            source_storage: source.into(), base_storage, replica_log: replica.into(), output_parent: output.into(),
            base: repair_checkpoint(&base)?, through: repair_checkpoint(&through)?,
        };
        return Ok(Command::RepairCheckpoint { request, memory_bytes });
    }
    let inspection = match arguments.peek().map(String::as_str) {
        Some("read-arrow-file") => Some(crate::inspect::FileKind::ArrowFile),
        Some("read-arrow-stream-file") => Some(crate::inspect::FileKind::ArrowStream),
        Some("read-hash-trie-file") => Some(crate::inspect::FileKind::HashTrie),
        Some("read-table-block-file") => Some(crate::inspect::FileKind::TableBlock),
        Some("read-block-file") => Some(crate::inspect::FileKind::Block),
        _ => None,
    };
    if let Some(kind) = inspection {
        arguments.next();
        let mut path = None;
        let mut allocation_limit_bytes = environment("RTDB_MEMORY_LIMIT_BYTES").map(|value| bytes(&value)).transpose()?.unwrap_or(1024 * 1024 * 1024);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "-h" | "--help" => return Ok(Command::Help),
                "--memory-limit" => allocation_limit_bytes = bytes(&arguments.next().ok_or_else(|| invalid("--memory-limit requires bytes"))?)?,
                name if !name.starts_with('-') && path.is_none() => path = Some(PathBuf::from(argument)),
                _ => return Err(invalid("File inspection accepts one path and --memory-limit")),
            }
        }
        return Ok(Command::Inspect { kind, path: path.ok_or_else(|| invalid("File inspection requires a path"))?, allocation_limit_bytes });
    }
    if arguments.peek().is_some_and(|argument| matches!(argument.as_str(), "export-snapshot" | "reset-compactor")) {
        let command = arguments.next().expect("matched maintenance command");
        let mut options = StartOptions { mode: NodeMode::Node, file: None, allocation_limit_bytes: environment("RTDB_MEMORY_LIMIT_BYTES").map(|value| bytes(&value)).transpose()?.unwrap_or(1024 * 1024 * 1024), pgwire_port: 5432, flight_sql_port: 3000, object_sort };
        let mut database = None;
        let mut dry_run = false;
        let mut retained_base = None;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "-h" | "--help" => return Ok(Command::Help),
                "-f" | "--file" => options.file = Some(PathBuf::from(arguments.next().ok_or_else(|| invalid("--file requires a path"))?)),
                "--memory-limit" => options.allocation_limit_bytes = bytes(&arguments.next().ok_or_else(|| invalid("--memory-limit requires bytes"))?)?,
                "--dry-run" => dry_run = true,
                "--retained-base" if command == "reset-compactor" && retained_base.is_none() => {
                    let path = arguments.next().filter(|path| !path.starts_with('-'))
                        .ok_or_else(|| invalid("--retained-base requires the detached export path"))?;
                    retained_base = Some(PathBuf::from(path));
                }
                name if name.starts_with("-D") => options.object_sort = crate::config::properties::command_property(name)?,
                name if !name.starts_with('-') && database.is_none() => database = Some(argument),
                _ => return Err(invalid(format!("{command} received an unknown or repeated option"))),
            }
        }
        let database = database.ok_or_else(|| invalid(format!("{command} requires a database name")))?;
        return Ok(if command == "export-snapshot" { Command::SnapshotExport { options, database, dry_run } } else { Command::CompactorReset { options, database, dry_run, retained_base } });
    }
    if arguments.peek().is_some_and(|argument| argument == "hash-password") {
        arguments.next();
        let mut algorithm = None;
        let mut password = None;
        let mut positional = false;
        for argument in arguments {
            match argument.as_str() {
                "--" if !positional => positional = true,
                "-h" | "--help" if !positional => return Ok(Command::Help),
                "--bcrypt" | "--argon2id" if !positional => {
                    let value = argument == "--bcrypt";
                    if algorithm.is_some_and(|previous| previous != value) { return Err(invalid("Choose one of --argon2id or --bcrypt")); }
                    algorithm = Some(value);
                }
                value if !positional && value.starts_with('-') => return Err(invalid("Unknown hash-password option")),
                _ if password.is_none() => password = Some(Secret::new(argument)),
                _ => return Err(invalid("hash-password accepts one password")),
            }
        }
        return Ok(Command::HashPassword { bcrypt: algorithm.unwrap_or(false), password });
    }
    let mode = match arguments.peek().map(String::as_str) {
        Some("node") => { arguments.next(); NodeMode::Node }
        Some("ingest") => { arguments.next(); NodeMode::Ingest }
        Some("compactor") => { arguments.next(); NodeMode::Compactor }
        Some("playground") => { arguments.next(); NodeMode::Playground }
        None => NodeMode::Node,
        Some(option) if option.starts_with('-') => NodeMode::Node,
        Some(_) => return Err(invalid("Unknown RTDB command")),
    };
    let allocation_limit_bytes = environment("RTDB_MEMORY_LIMIT_BYTES").map(|value| bytes(&value)).transpose()?.unwrap_or(1024 * 1024 * 1024);
    let mut options = StartOptions { mode, file: None, allocation_limit_bytes, pgwire_port: 5432, flight_sql_port: 3000, object_sort };
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "-f" | "--file" if mode != NodeMode::Playground => options.file = Some(PathBuf::from(arguments.next().ok_or_else(|| invalid("--file requires a path"))?)),
            "--memory-limit" => options.allocation_limit_bytes = bytes(&arguments.next().ok_or_else(|| invalid("--memory-limit requires bytes"))?)?,
            "-p" | "--port" if mode == NodeMode::Playground => options.pgwire_port = port(arguments.next())?,
            "--flight-sql-port" if mode == NodeMode::Playground => options.flight_sql_port = port(arguments.next())?,
            name if name.starts_with("-D") => options.object_sort = crate::config::properties::command_property(name)?,
            _ => return Err(invalid(format!("Unknown option '{argument}'"))),
        }
    }
    Ok(Command::Start(options))
}

fn repair_checkpoint(value: &str) -> Result<rtdb_node::RepairCheckpoint> {
    let (index, sha256) = value.split_once('@').ok_or_else(|| invalid("A repair checkpoint has the form BLOCK@SHA256"))?;
    let block = index.parse::<i64>().ok().filter(|index| *index >= 0).ok_or_else(|| invalid("Checkpoint block must be nonnegative"))?;
    if sha256.len() != 64 || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid("Checkpoint SHA-256 must have 64 hexadecimal digits"));
    }
    let mut digest = [0; 32];
    for (offset, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&sha256[2 * offset..2 * offset + 2], 16).map_err(|_| invalid("Invalid checkpoint SHA-256"))?;
    }
    Ok(rtdb_node::RepairCheckpoint { block: rtdb_types::BlockIndex(block), sha256: digest })
}

impl StartOptions {
    /// Reserves parsing and environment expansion before constructing startup configuration.
    pub fn load_reserved(&self) -> Result<ParsedConfiguration> {
        let memory = MemoryBudget::new(self.allocation_limit_bytes);
        if self.mode == NodeMode::Playground {
            let mut parsed = ParsedConfiguration::from_yaml("", &ProcessEnvironment, &memory, false)?;
            let config = parsed.configuration_mut();
            config.playground = true;
            config.auth = rtdb_auth::AuthConfig::SingleRootUser { password: None };
            config.server = Some(PgwireSettings { host: "0.0.0.0".into(), port: self.pgwire_port, ..Default::default() });
            config.flight_sql = Some(FlightSettings { host: "0.0.0.0".into(), port: self.flight_sql_port, ..Default::default() });
            config.object_sort = self.object_sort;
            return Ok(parsed);
        }
        let path = self.file.clone().or_else(|| ["xtdb.yaml", "xtdb.yml", "xtdb.edn"].into_iter().map(PathBuf::from).find(|path| path.is_file()));
        if self.mode == NodeMode::Ingest && path.is_none() { return Err(invalid("ingest requires a YAML configuration file")); }
        let _input = path.as_ref().map(|_| memory.reserve(8 * 1024 * 1024 + 4096)).transpose()?;
        let text = path.as_deref().map(read_configuration).transpose()?.unwrap_or_default();
        let parsed = match (self.mode, path.as_deref().and_then(Path::extension).and_then(|extension| extension.to_str())) {
            (NodeMode::Ingest, Some("edn")) => return Err(invalid("Ingest configuration uses YAML")),
            (NodeMode::Ingest, _) => ParsedConfiguration::from_yaml(&text, &ProcessEnvironment, &memory, true)?,
            (_, Some("edn")) => ParsedConfiguration::from_edn(&text, &ProcessEnvironment, &memory)?,
            _ => ParsedConfiguration::from_yaml(&text, &ProcessEnvironment, &memory, false)?,
        };
        for warning in parsed.warnings() { eprintln!("{warning}"); }
        Ok(parsed.with_object_sort(self.object_sort))
    }

    pub fn load(&self) -> Result<NodeConfiguration> {
        if self.mode == NodeMode::Playground {
            let mut config = NodeConfiguration::from_yaml("")?;
            config.playground = true;
            config.auth = rtdb_auth::AuthConfig::SingleRootUser { password: None };
            config.server = Some(PgwireSettings { host: "0.0.0.0".into(), port: self.pgwire_port, ..Default::default() });
            config.flight_sql = Some(FlightSettings { host: "0.0.0.0".into(), port: self.flight_sql_port, ..Default::default() });
            config.object_sort = self.object_sort;
            return Ok(config);
        }
        let path = self.file.clone().or_else(|| ["xtdb.yaml", "xtdb.yml", "xtdb.edn"].into_iter().map(PathBuf::from).find(|path| path.is_file()));
        if self.mode == NodeMode::Ingest && path.is_none() { return Err(invalid("ingest requires a YAML configuration file")); }
        let text = path.as_deref().map(read_configuration).transpose()?.unwrap_or_default();
        let mut configuration = match (self.mode, path.as_deref().and_then(Path::extension).and_then(|extension| extension.to_str())) {
            (NodeMode::Ingest, Some("edn")) => Err(invalid("Ingest configuration uses YAML")),
            (NodeMode::Ingest, _) => NodeConfiguration::from_ingest_yaml(&text),
            (_, Some("edn")) => {
                let (configuration, warnings) = NodeConfiguration::from_edn_with_environment(&text, &|name| std::env::var(name).ok())?;
                for warning in warnings { eprintln!("{warning}"); }
                Ok(configuration)
            }
            _ => NodeConfiguration::from_yaml(&text),
        }?;
        configuration.object_sort = self.object_sort;
        Ok(configuration)
    }
}

fn read_configuration(path: &Path) -> Result<Zeroizing<String>> {
    if !matches!(path.extension().and_then(|extension| extension.to_str()), Some("yaml" | "yml" | "edn")) { return Err(invalid("Node configuration must be a YAML or EDN file")); }
    let mut text = Zeroizing::new(String::new());
    File::open(path).map_err(|error| Anomaly::incorrect("opts-file-not-found", error.to_string()))?.take(4 * 1024 * 1024 + 1).read_to_string(&mut text).map_err(invalid)?;
    if text.len() > 4 * 1024 * 1024 { return Err(invalid("Configuration exceeds 4 MiB")); }
    Ok(text)
}

pub fn hash_password(bcrypt: bool, password: Option<Secret>, input: &mut impl BufRead) -> Result<String> {
    let password = match password {
        Some(password) => password,
        None => {
            let mut line = Zeroizing::new(String::new());
            input.take(1024 * 1024 + 1).read_line(&mut line).map_err(invalid)?;
            if line.len() > 1024 * 1024 { return Err(invalid("Password input exceeds 1 MiB")); }
            if line.ends_with('\n') { line.pop(); if line.ends_with('\r') { line.pop(); } }
            if line.is_empty() { return Err(invalid("Missing password")); }
            Secret::new(line.as_str())
        }
    };
    let hash = if bcrypt { PasswordHash::bcrypt(password.expose())? } else { PasswordHash::argon2id(password.expose(), &MemoryBudget::new(128 * 1024 * 1024))? };
    Ok(hash.encoded().into())
}

fn bytes(value: &str) -> Result<usize> { value.parse::<usize>().ok().filter(|value| *value > 1).ok_or_else(|| invalid("Memory limit must be a positive byte count greater than one")) }
fn port(value: Option<String>) -> Result<i32> { value.and_then(|value| value.parse::<i32>().ok()).filter(|value| (-1..=65535).contains(value)).ok_or_else(|| invalid("Port must be between -1 and 65535")) }
fn invalid(message: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.cli/arguments", message.to_string()) }
