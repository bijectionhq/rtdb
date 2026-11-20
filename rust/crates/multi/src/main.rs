//! The RTDB multi cell processes.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rtdb_directory::{Directory, MemoryDirectory, RemoteDirectory};
use rtdb_multi::{admin, agent::AgentOptions, deployment::CellStorage, gateway, lease::LeaseOptions, process};
use rtdb_server::config::NodeConfiguration;
use rtdb_types::{Anomaly, Result};
use tokio_util::sync::CancellationToken;

const HELP: &str = "RTDB multi\n\n\
Usage: rtdb-multi directory --listen ADDR\n\
       rtdb-multi logkeeper --directory DIR --listen ADDR --data PATH [--zone ZONE] [--no-fsync]\n\
       rtdb-multi node --directory DIR --storage-root PATH --keys PATH --cache PATH [--config FILE] [--control ADDR] [--zone ZONE] [--park-after SECONDS] [--max-active N] [--max-tail-rows N] [--isolation | --compaction-worker] [--no-local-compaction] [--memory-limit BYTES]\n\
       rtdb-multi gateway --directory DIR --listen ADDR [--cell NAME --cell-map ADDR]\n\
       rtdb-multi cell-map --listen ADDR\n\
       rtdb-multi move-cell --cell-map ADDR --name NAME --from CELL --from-directory DIR --from-storage-root PATH --from-keys PATH --to CELL --to-directory DIR --to-storage-root PATH --to-keys PATH\n\
       rtdb-multi controller --directory DIR [--rebalance MOVES]\n\
       rtdb-multi reaper --directory DIR --storage-root PATH [--grace SECONDS]\n\
       rtdb-multi create --directory DIR --name NAME [--org ORG] [--replicas N]\n\
       rtdb-multi list --directory DIR\n\
       rtdb-multi drain --directory DIR --member ID\n\
       rtdb-multi status --directory DIR [--target-per-node N]\n\
       rtdb-multi audit --directory DIR --keys PATH --name NAME\n\
       rtdb-multi move --directory DIR --name NAME --to MEMBER\n\
       rtdb-multi read-width --directory DIR --name NAME --width N\n\
       rtdb-multi unquarantine --directory DIR --name NAME\n\
       rtdb-multi fork --directory DIR --storage-root PATH --keys PATH --from NAME --name NAME\n\
       rtdb-multi delete --directory DIR --storage-root PATH --keys PATH --name NAME\n\n\
DIR is HOST:PORT of a directory server, or dynamodb:PREFIX for DynamoDB tables named PREFIX-deployments, PREFIX-deployment-names and PREFIX-members\n\
(RTDB_DYNAMODB_ENDPOINT points at DynamoDB Local; requires the `dynamodb` feature).\n";

#[tokio::main]
async fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if let Err(error) = run(arguments).await {
        eprintln!("{}: {}", error.code, error.message.as_deref().unwrap_or(""));
        std::process::exit(1);
    }
}

struct Arguments { flags: BTreeMap<String, Option<String>> }

impl Arguments {
    fn parse(arguments: &[String]) -> Result<Self> {
        let mut flags = BTreeMap::new();
        let mut iterator = arguments.iter().peekable();
        while let Some(flag) = iterator.next() {
            let name = flag.strip_prefix("--").ok_or_else(|| usage(format!("Unexpected argument {flag}")))?;
            let value = if iterator.peek().is_some_and(|next| !next.starts_with("--")) { iterator.next().cloned() } else { None };
            flags.insert(name.to_owned(), value);
        }
        Ok(Self { flags })
    }
    fn optional(&self, name: &str) -> Option<&str> { self.flags.get(name).and_then(|value| value.as_deref()) }
    fn required(&self, name: &str) -> Result<&str> { self.optional(name).ok_or_else(|| usage(format!("--{name} is required"))) }
    fn present(&self, name: &str) -> bool { self.flags.contains_key(name) }
    fn address(&self, name: &str, default: &str) -> Result<SocketAddr> {
        self.optional(name).unwrap_or(default).parse().map_err(|_| usage(format!("--{name} must be HOST:PORT")))
    }
    fn number<T: std::str::FromStr>(&self, name: &str, default: T) -> Result<T> {
        self.optional(name).map(|value| value.parse().map_err(|_| usage(format!("--{name} must be a number")))).transpose().map(|value| value.unwrap_or(default))
    }
}

async fn run(arguments: Vec<String>) -> Result<()> {
    let Some((command, rest)) = arguments.split_first() else { print!("{HELP}"); return Ok(()); };
    let arguments = Arguments::parse(rest)?;
    let cancellation = CancellationToken::new();
    { let cancellation = cancellation.clone(); tokio::spawn(async move { let _ = tokio::signal::ctrl_c().await; cancellation.cancel(); }); }
    match command.as_str() {
        "help" | "--help" | "-h" => { print!("{HELP}"); Ok(()) }
        "directory" => {
            let listener = tokio::net::TcpListener::bind(arguments.address("listen", "127.0.0.1:7001")?).await.map_err(io)?;
            eprintln!("directory listening on {}", listener.local_addr().map_err(io)?);
            rtdb_directory::serve(listener, MemoryDirectory::new(), cancellation).await
        }
        "logkeeper" => {
            let directory = directory(arguments.required("directory")?).await?;
            let logkeeper = process::start_logkeeper(directory, process::LogkeeperSettings {
                listen: arguments.address("listen", "127.0.0.1:0")?, data_dir: PathBuf::from(arguments.required("data")?), fsync: !arguments.present("no-fsync"),
                availability_zone: arguments.optional("zone").unwrap_or("local").into(), lease: LeaseOptions::default(),
            }).await?;
            eprintln!("logkeeper {} listening on {}", logkeeper.lease.id(), logkeeper.address);
            cancellation.cancelled().await;
            logkeeper.stop().await
        }
        "node" => {
            let directory = directory(arguments.required("directory")?).await?;
            let configuration = match arguments.optional("config") {
                Some(path) => NodeConfiguration::from_yaml(&std::fs::read_to_string(path).map_err(io)?)?,
                None => NodeConfiguration::from_yaml("server:\n  port: 0\n")?,
            };
            let mut agent = AgentOptions::new(CellStorage::Local { root: PathBuf::from(arguments.required("storage-root")?) });
            agent.max_tail_rows = arguments.optional("max-tail-rows").map(|count| count.parse()).transpose().map_err(|_| usage("--max-tail-rows must be a number".into()))?;
            agent.max_active = arguments.optional("max-active").map(|count| count.parse()).transpose().map_err(|_| usage("--max-active must be a number".into()))?;
            agent.park_after = arguments.optional("park-after").map(|seconds| seconds.parse().map(Duration::from_secs)).transpose().map_err(|_| usage("--park-after must be a number of seconds".into()))?;
            let node = process::start_node(directory, process::NodeSettings {
                configuration, allocation_limit_bytes: arguments.number("memory-limit", 1usize << 30)?, control: arguments.address("control", "127.0.0.1:0")?,
                availability_zone: arguments.optional("zone").unwrap_or("local").into(), agent, lease: LeaseOptions::default(), claim_wait: Duration::from_millis(20),
                pool: if arguments.present("isolation") { rtdb_directory::MemberPool::Isolation } else if arguments.present("compaction-worker") { rtdb_directory::MemberPool::Compaction } else { rtdb_directory::MemberPool::General },
                local_compaction: !arguments.present("no-local-compaction"),
                keys: keys(&arguments)?, cache_dir: PathBuf::from(arguments.required("cache")?),
            }).await?;
            eprintln!("node {} serving pgwire on {}, control on {}", node.lease.id(), node.pgwire_address, node.control_address);
            cancellation.cancelled().await;
            node.shutdown().await
        }
        "gateway" => {
            let directory = directory(arguments.required("directory")?).await?;
            let listener = tokio::net::TcpListener::bind(arguments.address("listen", "127.0.0.1:5432")?).await.map_err(io)?;
            eprintln!("gateway listening on {}", listener.local_addr().map_err(io)?);
            let mut options = gateway::GatewayOptions::default();
            if let (Some(cell), Some(map)) = (arguments.optional("cell"), arguments.optional("cell-map")) {
                let map: Arc<dyn rtdb_multi::cells::CellMap> = rtdb_multi::cells::RemoteCellMap::new(map);
                let advertised = arguments.optional("advertise").map(str::to_owned).unwrap_or(listener.local_addr().map_err(io)?.to_string());
                map.register_cell(cell, &advertised).await?;
                options.cells = Some((map, cell.to_owned()));
            }
            gateway::serve(listener, directory, options, cancellation).await
        }
        "controller" => {
            let directory = directory(arguments.required("directory")?).await?;
            let options = admin::ControllerOptions { rebalance_moves: arguments.number("rebalance", 0usize)?, ..Default::default() };
            admin::run_controller(directory, options, cancellation).await;
            Ok(())
        }
        "cell-map" => {
            let listener = tokio::net::TcpListener::bind(arguments.address("listen", "127.0.0.1:7000")?).await.map_err(io)?;
            eprintln!("cell map listening on {}", listener.local_addr().map_err(io)?);
            rtdb_multi::cells::serve(listener, rtdb_multi::cells::MemoryCellMap::new(), cancellation).await
        }
        "move-cell" => {
            let map = rtdb_multi::cells::RemoteCellMap::new(arguments.required("cell-map")?);
            let (from_directory, to_directory) = (directory(arguments.required("from-directory")?).await?, directory(arguments.required("to-directory")?).await?);
            let from_storage = CellStorage::Local { root: PathBuf::from(arguments.required("from-storage-root")?) };
            let to_storage = CellStorage::Local { root: PathBuf::from(arguments.required("to-storage-root")?) };
            let from_keys = rtdb_multi::keys::LocalKeyService::new(arguments.required("from-keys")?)?;
            let to_keys = rtdb_multi::keys::LocalKeyService::new(arguments.required("to-keys")?)?;
            let from = admin::CellHandle { name: arguments.required("from")?, directory: from_directory.as_ref(), storage: &from_storage, keys: &from_keys, maximum_deployments: usize::MAX };
            let to = admin::CellHandle { name: arguments.required("to")?, directory: to_directory.as_ref(), storage: &to_storage, keys: &to_keys, maximum_deployments: usize::MAX };
            let record = admin::move_cell(map.as_ref(), &from, &to, arguments.required("name")?, 4 << 20).await?;
            println!("{}", serde_json::to_string_pretty(&record).expect("a record serializes"));
            Ok(())
        }
        "reaper" => {
            let directory = directory(arguments.required("directory")?).await?;
            let grace = Duration::from_secs(arguments.number("grace", 3600u64)?);
            rtdb_multi::store::run_reaper(directory, PathBuf::from(arguments.required("storage-root")?), grace, Duration::from_secs(10), cancellation).await;
            Ok(())
        }
        "move" => {
            let directory = directory(arguments.required("directory")?).await?;
            admin::move_deployment(directory.as_ref(), arguments.required("name")?, &rtdb_directory::MemberId(arguments.required("to")?.into())).await
        }
        "read-width" => {
            let directory = directory(arguments.required("directory")?).await?;
            admin::set_read_width(directory.as_ref(), arguments.required("name")?, arguments.number("width", 1u32)?).await.map(|_| ())
        }
        "unquarantine" => {
            let directory = directory(arguments.required("directory")?).await?;
            admin::unquarantine(directory.as_ref(), arguments.required("name")?).await.map(|_| ())
        }
        "fork" => {
            let directory = directory(arguments.required("directory")?).await?;
            let storage = CellStorage::Local { root: PathBuf::from(arguments.required("storage-root")?) };
            let record = admin::fork(directory.as_ref(), &storage, keys(&arguments)?.as_ref(), arguments.required("from")?, arguments.required("name")?).await?;
            println!("{}", serde_json::to_string_pretty(&record).expect("a record serializes"));
            Ok(())
        }
        "delete" => {
            let directory = directory(arguments.required("directory")?).await?;
            let storage = CellStorage::Local { root: PathBuf::from(arguments.required("storage-root")?) };
            let deletion = admin::delete_deployment(directory.as_ref(), &storage, keys(&arguments)?.as_ref(), arguments.required("name")?, 4 << 20).await?;
            println!("{}", serde_json::to_string_pretty(&deletion).expect("a record serializes"));
            Ok(())
        }
        "create" => {
            let directory = directory(arguments.required("directory")?).await?;
            let record = admin::create_deployment(directory.as_ref(), arguments.required("name")?, arguments.optional("org").unwrap_or("default"), arguments.number("replicas", 3usize)?).await?;
            println!("{}", serde_json::to_string_pretty(&record).expect("a record serializes"));
            Ok(())
        }
        "status" => {
            let directory = directory(arguments.required("directory")?).await?;
            let status = admin::cell_status(directory.as_ref(), arguments.number("target-per-node", 1000usize)?).await?;
            println!("{}", serde_json::to_string_pretty(&status).expect("a status serializes"));
            Ok(())
        }
        "audit" => {
            let directory = directory(arguments.required("directory")?).await?;
            let report = admin::audit(directory.as_ref(), keys(&arguments)?.as_ref(), arguments.required("name")?, 4 << 20).await?;
            println!("{}", serde_json::to_string_pretty(&report).expect("a report serializes"));
            if report.violations.is_empty() { Ok(()) } else { Err(Anomaly::fault("rtdb.multi/audit", format!("{} violations", report.violations.len()))) }
        }
        "drain" => {
            let directory = directory(arguments.required("directory")?).await?;
            admin::drain(directory.as_ref(), &rtdb_directory::MemberId(arguments.required("member")?.into())).await?;
            Ok(())
        }
        "list" => {
            let directory = directory(arguments.required("directory")?).await?;
            let listing = serde_json::json!({ "deployments": directory.deployments().await?, "members": directory.members().await? });
            println!("{}", serde_json::to_string_pretty(&listing).expect("a listing serializes"));
            Ok(())
        }
        other => Err(usage(format!("Unknown command {other}"))),
    }
}

fn keys(arguments: &Arguments) -> Result<Arc<dyn rtdb_multi::keys::KeyService>> {
    Ok(Arc::new(rtdb_multi::keys::LocalKeyService::new(arguments.required("keys")?)?))
}

async fn directory(spec: &str) -> Result<Arc<dyn Directory>> {
    if let Some(prefix) = spec.strip_prefix("dynamodb:") { return dynamodb(prefix).await; }
    let mut attempts = 0;
    loop {
        match RemoteDirectory::connect(spec).await {
            Ok(directory) => return Ok(directory),
            Err(error) if attempts < 50 => { attempts += 1; let _ = error; tokio::time::sleep(Duration::from_millis(200)).await; }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(feature = "dynamodb")]
async fn dynamodb(prefix: &str) -> Result<Arc<dyn Directory>> {
    let tables = rtdb_directory::DynamoTables::with_prefix(prefix);
    let directory = match std::env::var("RTDB_DYNAMODB_ENDPOINT") {
        Ok(endpoint) => rtdb_directory::DynamoDirectory::new(rtdb_directory::DynamoDirectory::local_client(&endpoint), tables),
        Err(_) => rtdb_directory::DynamoDirectory::from_sdk_config(&aws_config::load_from_env().await, tables),
    };
    directory.create_tables().await?;
    Ok(Arc::new(directory))
}

#[cfg(not(feature = "dynamodb"))]
async fn dynamodb(_: &str) -> Result<Arc<dyn Directory>> {
    Err(Anomaly::unsupported("rtdb.multi/dynamodb", "This binary was built without the dynamodb feature"))
}

fn usage(message: String) -> Anomaly { Anomaly::incorrect("rtdb.multi/usage", format!("{message}\n\n{HELP}")) }
fn io(error: std::io::Error) -> Anomaly { Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.multi/io", error.to_string()) }
