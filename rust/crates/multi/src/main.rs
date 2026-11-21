//! The RTDB multi cell processes.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rtdb_directory::{Directory, MemoryDirectory, RemoteDirectory};
use rtdb_multi::cells::{CellMap, MemoryCellMap, RemoteCellMap};
use rtdb_multi::{admin, agent::AgentOptions, deployment::CellStorage, gateway, lease::LeaseOptions, process};
use rtdb_server::config::NodeConfiguration;
use rtdb_types::{Anomaly, Result};
use tokio_util::sync::CancellationToken;

const HELP: &str = "RTDB multi\n\n\
Usage: rtdb-multi directory --listen ADDR\n\
       rtdb-multi logkeeper --directory DIR --listen ADDR --data PATH [--zone ZONE] [--cache-bytes BYTES] [--no-fsync]\n\
       rtdb-multi node --directory DIR --storage-root PATH --keys PATH --cache PATH [--config FILE] [--control ADDR] [--zone ZONE] [--engine-version VERSION] [--park-after SECONDS] [--max-active N] [--activations-per-second N] [--s3-write-role ARN] [--sts-endpoint URL] [--s3-compat-write-policy] [--max-tail-rows N] [--isolation | --compaction-worker] [--no-local-compaction] [--memory-limit BYTES] [--cell-secret FILE]\n\
       rtdb-multi init-storage --storage-root PATH --keys PATH\n\
       rtdb-multi gateway --directory DIR --listen ADDR [--cell NAME --cell-map MAP] [--cell-secret FILE]\n\
       rtdb-multi cell-map --listen ADDR [--dynamodb-prefix PREFIX]\n\
       rtdb-multi move-cell --cell-map MAP --name NAME --from CELL --from-directory DIR --from-storage-root PATH --from-keys PATH --to CELL --to-directory DIR --to-storage-root PATH --to-keys PATH\n\
       rtdb-multi controller --directory DIR [--rebalance MOVES] [--cell-secret FILE]\n\
       rtdb-multi reaper --directory DIR --storage-root PATH [--keys PATH] [--grace SECONDS]\n\
       rtdb-multi create --directory DIR --name NAME [--org ORG] [--replicas N]\n\
       rtdb-multi list --directory DIR\n\
       rtdb-multi drain --directory DIR --member ID\n\
       rtdb-multi roll-engine --directory DIR --version VERSION\n\
       rtdb-multi status --directory DIR [--target-per-node N]\n\
       rtdb-multi audit --directory DIR --keys PATH --name NAME\n\
       rtdb-multi audit-log --data PATH --stream STREAM [--from LSN --through LSN] [--expect DIGEST]\n\
       rtdb-multi move --directory DIR --name NAME --to MEMBER [--cell-secret FILE]\n\
       rtdb-multi pin-engine --directory DIR --name NAME (--version VERSION | --clear)\n\
       rtdb-multi read-width --directory DIR --name NAME --width N\n\
       rtdb-multi unquarantine --directory DIR --name NAME\n\
       rtdb-multi fork --directory DIR --storage-root PATH --keys PATH --from NAME --name NAME [--at-block N | --at-time RFC3339] [--grace SECONDS] [--cell-secret FILE]\n\
       rtdb-multi delete --directory DIR --storage-root PATH --keys PATH --name NAME\n\n\
       rtdb-multi deletion-key --keys PATH\n\n\
       rtdb-multi verify-deletion --record PATH --public-key KEY\n\n\
DIR is HOST:PORT of a directory server, or dynamodb:PREFIX for DynamoDB tables named PREFIX-deployments, PREFIX-deployment-names and PREFIX-members\n\
Use dynamodb+packed:PREFIX to write packed parked rows after all readers have upgraded.\n\
MAP is HOST:PORT of a cell-map server, or dynamodb:PREFIX for tables named PREFIX-cells and PREFIX-placements.\n\
DynamoDB requires the `dynamodb` feature. RTDB_DYNAMODB_ENDPOINT points at DynamoDB Local.\n";

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
                cache_bytes: arguments.number("cache-bytes", rtdb_logkeeper::ServerOptions::DEFAULT_CACHE_BYTES)?,
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
            let mut agent = AgentOptions::new(CellStorage::parse(arguments.required("storage-root")?)?);
            if arguments.present("s3-write-role") {
                let sts_endpoint = if arguments.present("sts-endpoint") { Some(arguments.required("sts-endpoint")?.to_owned()) } else { None };
                agent.s3_write_role = Some(rtdb_multi::agent::S3WriteRole { arn: arguments.required("s3-write-role")?.to_owned(),
                    sts_endpoint, require_conditional_put: !arguments.present("s3-compat-write-policy") });
            } else if arguments.present("sts-endpoint") || arguments.present("s3-compat-write-policy") {
                return Err(usage("STS endpoint and compatibility options require --s3-write-role".into()));
            }
            agent.activations_per_second = arguments.number("activations-per-second", agent.activations_per_second)?;
            agent.max_tail_rows = arguments.optional("max-tail-rows").map(|count| count.parse()).transpose().map_err(|_| usage("--max-tail-rows must be a number".into()))?;
            agent.max_active = arguments.optional("max-active").map(|count| count.parse()).transpose().map_err(|_| usage("--max-active must be a number".into()))?;
            agent.park_after = arguments.optional("park-after").map(|seconds| seconds.parse().map(Duration::from_secs)).transpose().map_err(|_| usage("--park-after must be a number of seconds".into()))?;
            let node = process::start_node(directory, process::NodeSettings {
                configuration, allocation_limit_bytes: arguments.number("memory-limit", 1usize << 30)?, control: arguments.address("control", "127.0.0.1:0")?,
                availability_zone: arguments.optional("zone").unwrap_or("local").into(),
                engine_version: if arguments.present("engine-version") { Some(arguments.required("engine-version")?.to_owned()) } else { None },
                agent, lease: LeaseOptions::default(), claim_wait: Duration::from_millis(2),
                pool: if arguments.present("isolation") { rtdb_directory::MemberPool::Isolation } else if arguments.present("compaction-worker") { rtdb_directory::MemberPool::Compaction } else { rtdb_directory::MemberPool::General },
                local_compaction: !arguments.present("no-local-compaction"), capabilities: cell_secret(&arguments)?,
                keys: keys(&arguments)?, cache_dir: PathBuf::from(arguments.required("cache")?),
            }).await?;
            eprintln!("node {} serving pgwire on {}, control on {}", node.lease.id(), node.pgwire_address, node.control_address);
            cancellation.cancelled().await;
            node.shutdown().await
        }
        "init-storage" => {
            let storage = CellStorage::parse(arguments.required("storage-root")?)?;
            let namespaces = admin::initialize_storage(&storage, keys(&arguments)?.as_ref()).await?;
            println!("{}", serde_json::to_string(&namespaces).expect("storage namespaces serialize"));
            Ok(())
        }
        "gateway" => {
            let directory = directory(arguments.required("directory")?).await?;
            let listener = tokio::net::TcpListener::bind(arguments.address("listen", "127.0.0.1:5432")?).await.map_err(io)?;
            eprintln!("gateway listening on {}", listener.local_addr().map_err(io)?);
            let mut options = gateway::GatewayOptions { capabilities: cell_secret(&arguments)?, ..Default::default() };
            if let (Some(cell), Some(map)) = (arguments.optional("cell"), arguments.optional("cell-map")) {
                let map = cell_map(map).await?;
                let advertised = arguments.optional("advertise").map(str::to_owned).unwrap_or(listener.local_addr().map_err(io)?.to_string());
                map.register_cell(cell, &advertised).await?;
                options.cells = Some((map, cell.to_owned()));
            }
            gateway::serve(listener, directory, options, cancellation).await
        }
        "controller" => {
            let directory = directory(arguments.required("directory")?).await?;
            let options = admin::ControllerOptions { rebalance_moves: arguments.number("rebalance", 0usize)?, control: rtdb_multi::control::ControlKey(cell_secret(&arguments)?), ..Default::default() };
            admin::run_controller(directory, options, cancellation).await;
            Ok(())
        }
        "cell-map" => {
            let listener = tokio::net::TcpListener::bind(arguments.address("listen", "127.0.0.1:7000")?).await.map_err(io)?;
            let map: Arc<dyn CellMap> = match arguments.optional("dynamodb-prefix") {
                Some(prefix) => dynamo_cell_map(prefix).await?,
                None => MemoryCellMap::new(),
            };
            eprintln!("cell map listening on {}", listener.local_addr().map_err(io)?);
            rtdb_multi::cells::serve(listener, map, cancellation).await
        }
        "move-cell" => {
            let map = cell_map(arguments.required("cell-map")?).await?;
            let (from_directory, to_directory) = (directory(arguments.required("from-directory")?).await?, directory(arguments.required("to-directory")?).await?);
            let from_storage = CellStorage::parse(arguments.required("from-storage-root")?)?;
            let to_storage = CellStorage::parse(arguments.required("to-storage-root")?)?;
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
            // An open snapshot reads objects its deployment may have released since; the reaper's grace has to outlast it.
            if grace <= rtdb_multi::agent::SNAPSHOT_LIFETIME { return Err(usage("--grace must exceed a snapshot's lifetime of 30 minutes".into())); }
            let keys = arguments.optional("keys").map(|path| rtdb_multi::keys::LocalKeyService::new(path).map(|keys| Arc::new(keys) as Arc<dyn rtdb_multi::keys::KeyService>)).transpose()?;
            rtdb_multi::store::run_reaper(directory, CellStorage::parse(arguments.required("storage-root")?)?, keys, grace, Duration::from_secs(10), cancellation).await;
            Ok(())
        }
        "move" => {
            let directory = directory(arguments.required("directory")?).await?;
            admin::move_deployment(directory.as_ref(), arguments.required("name")?, &rtdb_directory::MemberId(arguments.required("to")?.into()), &rtdb_multi::control::ControlKey(cell_secret(&arguments)?)).await
        }
        "pin-engine" => {
            let version = match (arguments.present("clear"), arguments.present("version")) {
                (false, true) => Some(arguments.required("version")?),
                (true, false) if arguments.optional("clear").is_none() => None,
                _ => return Err(usage("Specify exactly one of --version VERSION or --clear".into())),
            };
            let directory = directory(arguments.required("directory")?).await?;
            admin::pin_engine(directory.as_ref(), arguments.required("name")?, version).await.map(|_| ())
        }
        "roll-engine" => {
            let directory = directory(arguments.required("directory")?).await?;
            for member in admin::roll_engine(directory.as_ref(), arguments.required("version")?).await? { println!("{member}"); }
            Ok(())
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
                    admin::fork_at_time(directory.as_ref(), &storage, keys(&arguments)?.as_ref(), from, name, rtdb_types::Timestamp(time.timestamp_micros()),
                        Duration::from_secs(arguments.number("grace", 3600u64)?), 4 << 20).await?
                }
                (at_block, _) => match at_block {
                Some(block) => {
                    let block = block.parse().map_err(|_| usage("--at-block must be a block index".into()))?;
                    admin::fork_at_block(directory.as_ref(), &storage, keys(&arguments)?.as_ref(), from, name, block, Duration::from_secs(arguments.number("grace", 3600u64)?)).await?
                }
                None => admin::fork(directory.as_ref(), &storage, keys(&arguments)?.as_ref(), from, name, &rtdb_multi::control::ControlKey(cell_secret(&arguments)?)).await?,
                },
            };
            println!("{}", serde_json::to_string_pretty(&record).expect("a record serializes"));
            Ok(())
        }
        "delete" => {
            let directory = directory(arguments.required("directory")?).await?;
            let storage = CellStorage::parse(arguments.required("storage-root")?)?;
            let deletion = admin::delete_deployment(directory.as_ref(), &storage, keys(&arguments)?.as_ref(), arguments.required("name")?, 4 << 20).await?;
            println!("{}", serde_json::to_string_pretty(&deletion).expect("a record serializes"));
            Ok(())
        }
        "deletion-key" => {
            println!("{}", keys(&arguments)?.deletion_verification_key()?);
            Ok(())
        }
        "verify-deletion" => {
            let contents = std::fs::read(arguments.required("record")?).map_err(io)?;
            let receipt = serde_json::from_slice(&contents).map_err(|error| usage(format!("Invalid deletion receipt: {error}")))?;
            rtdb_multi::keys::verify_deletion(&receipt, arguments.required("public-key")?)?;
            println!("Deletion receipt signature verified");
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
        "audit-log" => {
            let data = PathBuf::from(arguments.required("data")?);
            let stream = arguments.required("stream")?.to_owned();
            let interval = match (arguments.present("from"), arguments.present("through")) {
                (true, true) => Some([
                    arguments.required("from")?.parse::<i64>().map_err(|_| usage("--from must be a log position".into()))?,
                    arguments.required("through")?.parse::<i64>().map_err(|_| usage("--through must be a log position".into()))?,
                ]),
                (false, false) => None,
                _ => return Err(usage("--from and --through must be supplied together".into())),
            };
            let journal_stream = stream.clone();
            let proof = tokio::task::spawn_blocking(move || rtdb_logkeeper::inspect_retained(&data, &journal_stream, interval)).await
                .map_err(|error| Anomaly::fault("rtdb.multi/audit-log", error.to_string()))??;
            println!("{}", serde_json::json!({"stream": stream, "interval": proof.as_ref().map(|proof| proof.interval),
                "digest": proof.as_ref().map(|proof| &proof.digest)}));
            if arguments.present("expect") {
                let expected = arguments.required("expect")?;
                if proof.is_none_or(|proof| proof.digest != expected) {
                    return Err(Anomaly::fault("rtdb.multi/audit-log", "The retained journal digest differs from the expected value"));
                }
            }
            Ok(())
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

/// The cell secret from `--cell-secret FILE`, shared by a cell's gateways and nodes; without one, reconnection needs nodes that trust the gateway.
fn cell_secret(arguments: &Arguments) -> Result<Option<Arc<rtdb_multi::capability::CellSecret>>> {
    arguments.optional("cell-secret").map(|path| rtdb_multi::capability::CellSecret::from_file(std::path::Path::new(path)).map(Arc::new)).transpose()
}

fn keys(arguments: &Arguments) -> Result<Arc<dyn rtdb_multi::keys::KeyService>> {
    Ok(Arc::new(rtdb_multi::keys::LocalKeyService::new(arguments.required("keys")?)?))
}

async fn cell_map(spec: &str) -> Result<Arc<dyn CellMap>> {
    if let Some(prefix) = spec.strip_prefix("dynamodb:") { return dynamo_cell_map(prefix).await; }
    Ok(RemoteCellMap::new(spec))
}

#[cfg(feature = "dynamodb")]
async fn dynamo_cell_map(prefix: &str) -> Result<Arc<dyn CellMap>> {
    use rtdb_multi::cells::DynamoCellMap;
    let map = match std::env::var("RTDB_DYNAMODB_ENDPOINT") {
        Ok(endpoint) => {
            let map = DynamoCellMap::new(rtdb_directory::DynamoDirectory::local_client(&endpoint), prefix);
            map.create_tables().await?;
            map
        }
        Err(_) => DynamoCellMap::new(aws_sdk_dynamodb::Client::new(&aws_config::load_from_env().await), prefix),
    };
    Ok(Arc::new(map))
}

#[cfg(not(feature = "dynamodb"))]
async fn dynamo_cell_map(_: &str) -> Result<Arc<dyn CellMap>> {
    Err(Anomaly::unsupported("rtdb.multi/dynamodb", "This binary was built without the dynamodb feature"))
}

async fn directory(spec: &str) -> Result<Arc<dyn Directory>> {
    if let Some(prefix) = spec.strip_prefix("dynamodb:") { return dynamodb(prefix, false).await; }
    if let Some(prefix) = spec.strip_prefix("dynamodb+packed:") { return dynamodb(prefix, true).await; }
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
async fn dynamodb(prefix: &str, pack_parked: bool) -> Result<Arc<dyn Directory>> {
    let tables = rtdb_directory::DynamoTables::with_prefix(prefix);
    let directory = match std::env::var("RTDB_DYNAMODB_ENDPOINT") {
        Ok(endpoint) => rtdb_directory::DynamoDirectory::local(&endpoint, tables),
        Err(_) => rtdb_directory::DynamoDirectory::from_sdk_config(&aws_config::load_from_env().await, tables),
    };
    directory.create_tables().await?;
    let directory = if pack_parked { directory.with_packed_parked_records() } else { directory };
    Ok(Arc::new(directory))
}

#[cfg(not(feature = "dynamodb"))]
async fn dynamodb(_: &str, _: bool) -> Result<Arc<dyn Directory>> {
    Err(Anomaly::unsupported("rtdb.multi/dynamodb", "This binary was built without the dynamodb feature"))
}

fn usage(message: String) -> Anomaly { Anomaly::incorrect("rtdb.multi/usage", format!("{message}\n\n{HELP}")) }
fn io(error: std::io::Error) -> Anomaly { Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.multi/io", error.to_string()) }
