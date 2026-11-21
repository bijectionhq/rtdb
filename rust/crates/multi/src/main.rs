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
Usage: rtdb-multi initialize-directory --state PATH\n\
       rtdb-multi directory --state PATH --listen ADDR\n\
       rtdb-multi logkeeper --directory DIR --listen ADDR --data PATH [--advertise-host HOST] [--zone ZONE] [--cache-bytes BYTES] [--no-fsync]\n\
       rtdb-multi node --directory DIR --storage-root PATH --keys PATH --cache PATH [--config FILE] [--control ADDR] [--advertise-host HOST] [--zone ZONE] [--park-after SECONDS] [--max-active N] [--max-tail-rows N] [--isolation | --compaction-worker] [--no-local-compaction] [--memory-limit BYTES] [--cell-secret FILE]\n\
       rtdb-multi gateway --directory DIR --listen ADDR [--config FILE] [--cell NAME --cell-map ADDR] [--cell-secret FILE]\n\
                          [--install-listen ADDR --install-token-file FILE --install-org ORG --install-replicas N]\n\
       rtdb-multi cell-map --listen ADDR\n\
       rtdb-multi move-cell --cell-map ADDR --name NAME --from CELL --from-directory DIR --from-storage-root PATH --from-keys PATH --to CELL --to-directory DIR --to-storage-root PATH --to-keys PATH\n\
       rtdb-multi controller --directory DIR [--rebalance MOVES] [--cell-secret FILE]\n\
       rtdb-multi reaper --directory DIR --storage-root PATH [--keys PATH] [--grace SECONDS]\n\
       rtdb-multi create --directory DIR --name NAME [--org ORG] [--replicas N]\n\
       rtdb-multi install --directory DIR --id UUID --name NAME --org ORG [--replicas N]\n\
       rtdb-multi list --directory DIR\n\
       rtdb-multi drain --directory DIR --member ID\n\
       rtdb-multi status --directory DIR [--target-per-node N]\n\
       rtdb-multi audit --directory DIR --keys PATH --name NAME\n\
       rtdb-multi move --directory DIR --name NAME --to MEMBER [--cell-secret FILE]\n\
       rtdb-multi read-width --directory DIR --name NAME --width N\n\
       rtdb-multi unquarantine --directory DIR --name NAME\n\
       rtdb-multi fork --directory DIR --storage-root PATH --keys PATH --from NAME --name NAME [--at-block N | --at-time RFC3339] [--grace SECONDS] [--cell-secret FILE]\n\
       rtdb-multi delete --directory DIR --storage-root PATH --keys PATH --id UUID\n\
       rtdb-multi schedule-disposal --directory DIR --id UUID\n\
       rtdb-multi disposal --directory DIR --id UUID\n\
       rtdb-multi capture --directory DIR --storage-root PATH --archive-root PATH --keys PATH --id UUID --operation UUID\n\
       rtdb-multi restore --directory DIR --storage-root PATH --archive-root PATH --keys PATH --id UUID --operation UUID\n\n\
DIR is HOST:PORT of a directory server, or dynamodb:PREFIX for DynamoDB tables named PREFIX-deployments, PREFIX-deployment-names and PREFIX-members\n\
(RTDB_DYNAMODB_ENDPOINT points at DynamoDB Local; requires the `dynamodb` feature).\n\
For `--keys` use a private local directory or dynamodb:TABLE with an independently installed key table.\n";

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

fn installation_id(arguments: &Arguments) -> Result<uuid::Uuid> {
    required_uuid(arguments, "id")
}

fn required_uuid(arguments: &Arguments, name: &str) -> Result<uuid::Uuid> {
    let raw_id = arguments.required(name)?;
    uuid::Uuid::parse_str(raw_id).ok().filter(|id| !id.is_nil() && id.to_string() == raw_id)
        .ok_or_else(|| usage(format!("--{name} must be a canonical, nonzero UUID")))
}

async fn run(arguments: Vec<String>) -> Result<()> {
    let Some((command, rest)) = arguments.split_first() else { print!("{HELP}"); return Ok(()); };
    let arguments = Arguments::parse(rest)?;
    let cancellation = CancellationToken::new();
    { let cancellation = cancellation.clone(); tokio::spawn(async move { shutdown_signal().await; cancellation.cancel(); }); }
    match command.as_str() {
        "help" | "--help" | "-h" => { print!("{HELP}"); Ok(()) }
        "initialize-directory" => MemoryDirectory::initialize(std::path::Path::new(arguments.required("state")?)),
        "directory" => {
            let directory = MemoryDirectory::open(std::path::Path::new(arguments.required("state")?))?;
            let listener = tokio::net::TcpListener::bind(arguments.address("listen", "127.0.0.1:7001")?).await.map_err(io)?;
            eprintln!("directory listening on {}", listener.local_addr().map_err(io)?);
            rtdb_directory::serve(listener, directory, cancellation).await
        }
        "logkeeper" => {
            let directory = directory(arguments.required("directory")?).await?;
            let logkeeper = process::start_logkeeper(directory, process::LogkeeperSettings {
                advertise_host: arguments.optional("advertise-host").map(str::to_owned),
                listen: arguments.address("listen", "127.0.0.1:0")?, data_dir: PathBuf::from(arguments.required("data")?), fsync: !arguments.present("no-fsync"),
                cache_bytes: arguments.number("cache-bytes", rtdb_logkeeper::ServerOptions::DEFAULT_CACHE_BYTES)?,
                availability_zone: arguments.optional("zone").unwrap_or("local").into(), lease: LeaseOptions::default(),
            }).await?;
            eprintln!("logkeeper {} listening on {}", logkeeper.lease.id(), logkeeper.address);
            logkeeper.run_until(cancellation).await
        }
        "node" => {
            let directory = directory(arguments.required("directory")?).await?;
            let configuration = match arguments.optional("config") {
                Some(path) => NodeConfiguration::from_yaml(&std::fs::read_to_string(path).map_err(io)?)?,
                None => NodeConfiguration::from_yaml("server:\n  port: 0\n")?,
            };
            let mut agent = AgentOptions::new(CellStorage::parse(arguments.required("storage-root")?)?);
            agent.max_tail_rows = arguments.optional("max-tail-rows").map(|count| count.parse()).transpose().map_err(|_| usage("--max-tail-rows must be a number".into()))?;
            agent.max_active = arguments.optional("max-active").map(|count| count.parse()).transpose().map_err(|_| usage("--max-active must be a number".into()))?;
            agent.park_after = arguments.optional("park-after").map(|seconds| seconds.parse().map(Duration::from_secs)).transpose().map_err(|_| usage("--park-after must be a number of seconds".into()))?;
            let node = process::start_node(directory, process::NodeSettings {
                advertise_host: arguments.optional("advertise-host").map(str::to_owned),
                configuration, allocation_limit_bytes: arguments.number("memory-limit", 1usize << 30)?, control: arguments.address("control", "127.0.0.1:0")?,
                availability_zone: arguments.optional("zone").unwrap_or("local").into(), agent, lease: LeaseOptions::default(), claim_wait: Duration::from_millis(20),
                pool: if arguments.present("isolation") { rtdb_directory::MemberPool::Isolation } else if arguments.present("compaction-worker") { rtdb_directory::MemberPool::Compaction } else { rtdb_directory::MemberPool::General },
                local_compaction: !arguments.present("no-local-compaction"), capabilities: cell_secret(&arguments)?,
                keys: key_service(arguments.required("keys")?).await?, cache_dir: PathBuf::from(arguments.required("cache")?),
            }).await?;
            eprintln!("node {} serving pgwire on {}, control on {}", node.lease.id(), node.pgwire_address, node.control_address);
            cancellation.cancelled().await;
            node.shutdown().await
        }
        "gateway" => {
            let directory = directory(arguments.required("directory")?).await?;
            let listener = tokio::net::TcpListener::bind(arguments.address("listen", "127.0.0.1:5432")?).await.map_err(io)?;
            eprintln!("gateway listening on {}", listener.local_addr().map_err(io)?);
            let tls = if let Some(path) = arguments.optional("config") {
                let configuration = NodeConfiguration::from_yaml(&std::fs::read_to_string(path).map_err(io)?)?;
                let settings = configuration.server.and_then(|server| server.ssl)
                    .ok_or_else(|| usage("The gateway configuration must provide server.ssl".into()))?;
                Some(rtdb_server::tls::load(&settings, &rtdb_columnar::MemoryBudget::new(16 << 20))?)
            } else {
                None
            };
            let mut options = gateway::GatewayOptions { capabilities: cell_secret(&arguments)?, tls: tls.clone(), ..Default::default() };
            if let (Some(cell), Some(map)) = (arguments.optional("cell"), arguments.optional("cell-map")) {
                let map: Arc<dyn rtdb_multi::cells::CellMap> = rtdb_multi::cells::RemoteCellMap::new(map);
                let advertised = arguments.optional("advertise").map(str::to_owned).unwrap_or(listener.local_addr().map_err(io)?.to_string());
                map.register_cell(cell, &advertised).await?;
                options.cells = Some((map, cell.to_owned()));
            }
            if arguments.present("install-listen") {
                let tls = tls.ok_or_else(|| usage("The installation listener requires a TLS configuration".into()))?;
                let installer = rtdb_multi::install::Installer::from_file(
                    std::path::Path::new(arguments.required("install-token-file")?),
                    arguments.required("install-org")?.to_owned(),
                    arguments.number("install-replicas", 3usize)?,
                )?;
                let installs = tokio::net::TcpListener::bind(arguments.address("install-listen", "127.0.0.1:0")?).await.map_err(io)?;
                tokio::select! {
                    gateway = gateway::serve(listener, directory.clone(), options, cancellation.clone()) => gateway,
                    installation = rtdb_multi::install::serve(installs, tls, directory, Arc::new(installer), cancellation) => installation,
                }
            } else {
                gateway::serve(listener, directory, options, cancellation).await
            }
        }
        "controller" => {
            let directory = directory(arguments.required("directory")?).await?;
            let options = admin::ControllerOptions { rebalance_moves: arguments.number("rebalance", 0usize)?, control: rtdb_multi::control::ControlKey(cell_secret(&arguments)?), ..Default::default() };
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
            let from_storage = CellStorage::parse(arguments.required("from-storage-root")?)?;
            let to_storage = CellStorage::parse(arguments.required("to-storage-root")?)?;
            let from_keys = key_service(arguments.required("from-keys")?).await?;
            let to_keys = key_service(arguments.required("to-keys")?).await?;
            let from = admin::CellHandle { name: arguments.required("from")?, directory: from_directory.as_ref(), storage: &from_storage, keys: from_keys.as_ref(), maximum_deployments: usize::MAX };
            let to = admin::CellHandle { name: arguments.required("to")?, directory: to_directory.as_ref(), storage: &to_storage, keys: to_keys.as_ref(), maximum_deployments: usize::MAX };
            let record = admin::move_cell(map.as_ref(), &from, &to, arguments.required("name")?, 4 << 20).await?;
            println!("{}", serde_json::to_string_pretty(&record).expect("a record serializes"));
            Ok(())
        }
        "reaper" => {
            let directory = directory(arguments.required("directory")?).await?;
            let grace = Duration::from_secs(arguments.number("grace", 3600u64)?);
            // An open snapshot reads objects its deployment may have released since; the reaper's grace has to outlast it.
            if grace <= rtdb_multi::agent::SNAPSHOT_LIFETIME { return Err(usage("--grace must exceed a snapshot's lifetime of 30 minutes".into())); }
            let keys = match arguments.optional("keys") {
                Some(specification) => Some(key_service(specification).await?),
                None => None,
            };
            rtdb_multi::store::run_reaper(directory, CellStorage::parse(arguments.required("storage-root")?)?, keys, grace, Duration::from_secs(10), cancellation).await;
            Ok(())
        }
        "move" => {
            let directory = directory(arguments.required("directory")?).await?;
            admin::move_deployment(directory.as_ref(), arguments.required("name")?, &rtdb_directory::MemberId(arguments.required("to")?.into()), &rtdb_multi::control::ControlKey(cell_secret(&arguments)?)).await
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
            let storage = CellStorage::parse(arguments.required("storage-root")?)?;
            let (from, name) = (arguments.required("from")?, arguments.required("name")?);
            let record = match (arguments.optional("at-block"), arguments.optional("at-time")) {
                (None, Some(time)) => {
                    let time = time.parse::<chrono::DateTime<chrono::Utc>>().map_err(|_| usage("--at-time must be an RFC 3339 timestamp".into()))?;
                    admin::fork_at_time(directory.as_ref(), &storage, key_service(arguments.required("keys")?).await?.as_ref(), from, name, rtdb_types::Timestamp(time.timestamp_micros()),
                        Duration::from_secs(arguments.number("grace", 3600u64)?), 4 << 20).await?
                }
                (at_block, _) => match at_block {
                Some(block) => {
                    let block = block.parse().map_err(|_| usage("--at-block must be a block index".into()))?;
                    admin::fork_at_block(directory.as_ref(), &storage, key_service(arguments.required("keys")?).await?.as_ref(), from, name, block, Duration::from_secs(arguments.number("grace", 3600u64)?)).await?
                }
                None => admin::fork(directory.as_ref(), &storage, key_service(arguments.required("keys")?).await?.as_ref(), from, name, &rtdb_multi::control::ControlKey(cell_secret(&arguments)?)).await?,
                },
            };
            println!("{}", serde_json::to_string_pretty(&record).expect("a record serializes"));
            Ok(())
        }
        "delete" => {
            let directory = directory(arguments.required("directory")?).await?;
            let storage = CellStorage::parse(arguments.required("storage-root")?)?;
            let id = installation_id(&arguments)?;
            let keys = key_service(arguments.required("keys")?).await?;
            let deletion = admin::delete_deployment(directory.as_ref(), &storage, keys.as_ref(), &id.to_string(), 4 << 20).await?;
            println!("{}", serde_json::to_string_pretty(&deletion).expect("a record serializes"));
            Ok(())
        }
        "disposal" => {
            let directory = directory(arguments.required("directory")?).await?;
            let id = installation_id(&arguments)?;
            let receipt = directory.deployment(&id.to_string()).await?.and_then(|record| record.disposal);
            println!("{}", serde_json::to_string_pretty(&receipt).expect("a receipt serializes"));
            Ok(())
        }
        "capture" => {
            let directory = directory(arguments.required("directory")?).await?;
            let storage = CellStorage::parse(arguments.required("storage-root")?)?;
            let archive = CellStorage::parse(arguments.required("archive-root")?)?;
            let keys = key_service(arguments.required("keys")?).await?;
            let capture = rtdb_multi::archive::capture(
                directory.as_ref(), &storage, &archive, keys.as_ref(),
                installation_id(&arguments)?, required_uuid(&arguments, "operation")?,
            ).await?;
            println!("{}", serde_json::to_string_pretty(&capture).expect("a capture serializes"));
            Ok(())
        }
        "restore" => {
            let directory = directory(arguments.required("directory")?).await?;
            let storage = CellStorage::parse(arguments.required("storage-root")?)?;
            let archive = CellStorage::parse(arguments.required("archive-root")?)?;
            let keys = key_service(arguments.required("keys")?).await?;
            let recovery = rtdb_multi::archive::restore(
                directory.as_ref(), &storage, &archive, keys.as_ref(),
                installation_id(&arguments)?, required_uuid(&arguments, "operation")?, 4 << 20,
            ).await?;
            println!("{}", serde_json::to_string_pretty(&recovery).expect("a restore receipt serializes"));
            Ok(())
        }
        "schedule-disposal" => {
            let directory = directory(arguments.required("directory")?).await?;
            let id = installation_id(&arguments)?;
            let scheduled = admin::begin_disposal(directory.as_ref(), &id.to_string()).await?;
            println!("{}", serde_json::json!({
                "deployment": scheduled.id,
                "incarnation": scheduled.incarnation,
                "epoch": scheduled.epoch,
                "lifecycle": scheduled.lifecycle,
            }));
            Ok(())
        }
        "create" => {
            let directory = directory(arguments.required("directory")?).await?;
            let record = admin::create_deployment(directory.as_ref(), arguments.required("name")?, arguments.optional("org").unwrap_or("default"), arguments.number("replicas", 3usize)?).await?;
            println!("{}", serde_json::to_string_pretty(&record).expect("a record serializes"));
            Ok(())
        }
        "install" => {
            let directory = directory(arguments.required("directory")?).await?;
            let id = installation_id(&arguments)?;
            let record = admin::install_deployment(
                directory.as_ref(), id, arguments.required("name")?, arguments.required("org")?,
                arguments.number("replicas", 3usize)?,
            ).await?;
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
            let report = admin::audit(directory.as_ref(), key_service(arguments.required("keys")?).await?.as_ref(), arguments.required("name")?, 4 << 20).await?;
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

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut terminate) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = terminate.recv() => {},
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

/// The cell secret from `--cell-secret FILE`, shared by a cell's gateways and nodes; without one, reconnection needs nodes that trust the gateway.
fn cell_secret(arguments: &Arguments) -> Result<Option<Arc<rtdb_multi::capability::CellSecret>>> {
    arguments.optional("cell-secret").map(|path| rtdb_multi::capability::CellSecret::from_file(std::path::Path::new(path)).map(Arc::new)).transpose()
}

async fn key_service(specification: &str) -> Result<Arc<dyn rtdb_multi::keys::KeyService>> {
    if let Some(table) = specification.strip_prefix("dynamodb:") {
        return dynamo_keys(table).await;
    }
    Ok(Arc::new(rtdb_multi::keys::LocalKeyService::new(specification)?))
}

#[cfg(feature = "dynamodb")]
async fn dynamo_keys(table: &str) -> Result<Arc<dyn rtdb_multi::keys::KeyService>> {
    let client = match std::env::var("RTDB_DYNAMODB_ENDPOINT") {
        Ok(endpoint) => rtdb_directory::DynamoDirectory::local_client(&endpoint),
        Err(_) => aws_sdk_dynamodb::Client::new(&aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await),
    };
    Ok(Arc::new(rtdb_multi::keys::dynamo::DynamoKeyService::new(client, table.to_owned())?))
}

#[cfg(not(feature = "dynamodb"))]
async fn dynamo_keys(_: &str) -> Result<Arc<dyn rtdb_multi::keys::KeyService>> {
    Err(Anomaly::unsupported("rtdb.multi/dynamodb", "This binary was built without the dynamodb feature"))
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
        Ok(endpoint) => {
            let directory = rtdb_directory::DynamoDirectory::local(&endpoint, tables);
            directory.create_tables().await?;
            directory
        }
        Err(_) => rtdb_directory::DynamoDirectory::from_sdk_config(
            &aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await, tables,
        ),
    };
    Ok(Arc::new(directory))
}

#[cfg(not(feature = "dynamodb"))]
async fn dynamodb(_: &str) -> Result<Arc<dyn Directory>> {
    Err(Anomaly::unsupported("rtdb.multi/dynamodb", "This binary was built without the dynamodb feature"))
}

fn usage(message: String) -> Anomaly { Anomaly::incorrect("rtdb.multi/usage", format!("{message}\n\n{HELP}")) }
fn io(error: std::io::Error) -> Anomaly { Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.multi/io", error.to_string()) }
