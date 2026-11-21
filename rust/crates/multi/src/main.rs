//! The RTDB multi cell processes.

use std::collections::BTreeMap;
use std::io::{Read, Write};
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
Usage: rtdb-multi --build-info\n\
       rtdb-multi initialize-directory --state PATH\n\
       rtdb-multi initialize-standalone --state PATH --name NAME --id UUID\n\
       rtdb-multi standalone --state PATH --name NAME --id UUID --certificate FILE --private-key FILE --password-file FILE --ready-file FILE [--parent-stdin]\n\
       rtdb-multi directory --state PATH --listen ADDR\n\
       rtdb-multi logkeeper --directory DIR --listen ADDR --data PATH [--advertise-host HOST] [--zone ZONE] [--cache-bytes BYTES] [--no-fsync]\n\
       rtdb-multi node --directory DIR --storage-root PATH --keys PATH --cache PATH [--config FILE] [--control ADDR] [--advertise-host HOST] [--zone ZONE] [--ecs-cluster ARN] [--park-after SECONDS] [--max-active N] [--max-tail-rows N] [--isolation | --compaction-worker] [--no-local-compaction] [--memory-limit BYTES] [--cell-secret FILE]\n\
       rtdb-multi gateway --directory DIR --listen ADDR [--config FILE] [--cell NAME --cell-map ADDR] [--cell-secret FILE]\n\
                          [--install-listen ADDR --install-token-file FILE --install-org ORG --install-replicas N]\n\
                          [--disposal-token-file FILE]\n\
       rtdb-multi cell-map --listen ADDR\n\
       rtdb-multi move-cell --cell-map ADDR --name NAME --from CELL --from-directory DIR --from-storage-root PATH --from-keys PATH --to CELL --to-directory DIR --to-storage-root PATH --to-keys PATH [--ecs-cluster ARN]\n\
       rtdb-multi prepare-cell-transfer [--transfer FILE | --transfer-bucket BUCKET --transfer-digest SHA256] --from CELL --from-directory DIR --from-storage-root PATH --from-keys PATH --to CELL --to-directory DIR --to-storage-root PATH --to-keys PATH [--ecs-cluster ARN]\n\
       rtdb-multi activate-cell-transfer [--transfer FILE | --transfer-bucket BUCKET --transfer-digest SHA256] --from CELL --from-directory DIR --from-storage-root PATH --from-keys PATH --to CELL --to-directory DIR --to-storage-root PATH --to-keys PATH\n\
       rtdb-multi controller --directory DIR [--rebalance MOVES] [--cell-secret FILE]\n\
       rtdb-multi reaper --directory DIR --storage-root PATH [--keys PATH] [--ecs-cluster ARN] [--grace SECONDS] [--recovery-listen ADDR --recovery-config FILE --recovery-token-file FILE --recovery-org ORG --archive-root PATH]\n\
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
       rtdb-multi schedule-recovery-disposal --directory DIR --id UUID\n\
       rtdb-multi recovery-disposal --directory DIR --id UUID\n\
       rtdb-multi capture --directory DIR --storage-root PATH --archive-root PATH --keys PATH --id UUID --operation UUID\n\
       rtdb-multi stage-checkpoint --directory DIR --archive-root PATH --storage-root PATH --keys PATH --stage-request FILE [--ecs-cluster ARN]\n\
       rtdb-multi read-checkpoint-stage --directory DIR --storage-root PATH --keys PATH --id UUID --operation UUID --manifest-sha256 SHA256 --certificate FILE --private-key FILE --password-file FILE --ready-file FILE [--ecs-cluster ARN] [--parent-stdin]\n\
       rtdb-multi restore --directory DIR --storage-root PATH --archive-root PATH --keys PATH --id UUID --operation UUID\n\n\
DIR is HOST:PORT of a directory server, or dynamodb:PREFIX for the deployments, deployment-names, members and stopped-tasks tables\n\
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

fn hosted_task(arguments: &Arguments) -> Result<Option<rtdb_directory::HostedTask>> {
    let Some(cluster) = arguments.optional("ecs-cluster") else {
        if arguments.present("ecs-cluster") { return Err(usage("--ecs-cluster needs the installed cluster ARN".into())); }
        return Ok(None);
    };
    let task = std::env::var("RTDB_ECS_TASK_ARN")
        .map_err(|_| usage("The hosted entrypoint must supply the current ECS task ARN".into()))?;
    Ok(Some(rtdb_directory::HostedTask::new(cluster, &task)?))
}

const TRANSFER_DOCUMENT_BYTES: u64 = 8192;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HostedTransferBinding {
    cell: String,
    cluster: String,
    directory: String,
    storage: String,
    keys: String,
    exchange: String,
    attempt_gate: String,
}

async fn admit_hosted_transfer(command: &str, arguments: &Arguments) -> Result<()> {
    let encoded = match std::env::var("RTDB_TRANSFER_BINDING") {
        Ok(encoded) => encoded,
        Err(std::env::VarError::NotPresent) if !arguments.present("transfer-attempt") => return Ok(()),
        Err(_) => return Err(usage("Invalid installed RTDB transfer binding".into())),
    };
    let binding: HostedTransferBinding = serde_json::from_str(&encoded)
        .map_err(|_| usage("Invalid installed RTDB transfer binding".into()))?;
    for (flag, installed) in [
        ("to", binding.cell),
        ("ecs-cluster", binding.cluster),
        ("to-directory", binding.directory),
        ("to-storage-root", binding.storage),
        ("to-keys", binding.keys),
        ("transfer-bucket", binding.exchange),
    ] {
        if arguments.optional(flag) != Some(installed.as_str()) {
            return Err(usage(format!("Hosted transfer --{flag} differs from its installed destination")));
        }
    }
    if command == "move-cell" || arguments.present("transfer") {
        return Err(usage("The hosted task requires a catalog-managed transfer exchange".into()));
    }
    let expected_gate = arguments.required("to-directory")?.strip_prefix("dynamodb:")
        .map(|prefix| format!("{prefix}-stopped-tasks"));
    if expected_gate.as_deref() != Some(binding.attempt_gate.as_str()) {
        return Err(usage("The hosted transfer attempt gate differs from its installed directory".into()));
    }
    let attempt = required_uuid(arguments, "transfer-attempt")?;
    let task = hosted_task(arguments)?
        .ok_or_else(|| usage("The hosted transfer requires its exact ECS task ARN".into()))?;
    claim_transfer_attempt(&binding.attempt_gate, attempt, &task).await
}

#[cfg(feature = "dynamodb")]
async fn claim_transfer_attempt(table: &str, attempt: uuid::Uuid, task: &rtdb_directory::HostedTask) -> Result<()> {
    use aws_sdk_dynamodb::types::{AttributeValue, ReturnValuesOnConditionCheckFailure};

    let client = aws_sdk_dynamodb::Client::new(&aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await);
    let id = format!("transfer-attempt-{attempt}");
    let result = client.put_item()
        .table_name(table)
        .item("id", AttributeValue::S(id))
        .item("task_arn", AttributeValue::S(task.task_arn().to_owned()))
        .condition_expression("attribute_not_exists(id)")
        .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
        .send().await;
    match result {
        Ok(_) => Ok(()),
        Err(error) => {
            let Some(aws_sdk_dynamodb::operation::put_item::PutItemError::ConditionalCheckFailedException(occupied)) = error.as_service_error() else {
                return Err(Anomaly::new(
                    rtdb_types::AnomalyCategory::Unavailable,
                    "rtdb.multi/transfer-attempt",
                    format!("Could not admit the hosted transfer attempt: {error}"),
                ));
            };
            // An SDK retry after a lost reply can recover its own claim.
            let owner = occupied.item().and_then(|item| item.get("task_arn")).and_then(|value| value.as_s().ok());
            if owner.is_some_and(|arn| arn == task.task_arn()) {
                return Ok(());
            }
            Err(usage("The hosted transfer attempt is closed or claimed by another task".into()))
        }
    }
}

#[cfg(not(feature = "dynamodb"))]
async fn claim_transfer_attempt(_: &str, _: uuid::Uuid, _: &rtdb_directory::HostedTask) -> Result<()> {
    Err(Anomaly::unsupported("rtdb.multi/dynamodb", "Hosted transfers require the dynamodb feature"))
}

struct TransferExchange {
    store: Arc<dyn rtdb_storage::ObjectStore>,
    io: Arc<rtdb_storage::IoRegion>,
    digest: String,
}

impl TransferExchange {
    async fn open(arguments: &Arguments) -> Result<Option<Self>> {
        let remote = arguments.present("transfer-bucket") || arguments.present("transfer-digest");
        if remote == arguments.present("transfer") {
            return Err(usage("A transfer needs one local file or a private bucket and request digest".into()));
        }
        if !remote {
            return Ok(None);
        }
        let bucket = arguments.required("transfer-bucket")?;
        let valid_bucket = (3..=63).contains(&bucket.len())
            && bucket.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'.'))
            && bucket.as_bytes()[0].is_ascii_alphanumeric()
            && bucket.as_bytes()[bucket.len() - 1].is_ascii_alphanumeric();
        let digest = arguments.required("transfer-digest")?;
        if !valid_bucket || digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) {
            return Err(usage("Invalid transfer exchange namespace or request digest".into()));
        }
        let io = rtdb_storage::IoRegion::new();
        let root = rtdb_storage::ObjectKey::new(format!("rtdb-transfers/{digest}"))?;
        let store = rtdb_s3::S3Config::new(bucket).open(&root, &BTreeMap::new(), io.clone()).await?;
        Ok(Some(Self { store, io, digest: digest.to_owned() }))
    }

    async fn read_request(&self) -> Result<Vec<u8>> {
        let key = rtdb_storage::ObjectKey::new("request.json")?;
        let size = self.store.head(&key).await?.size;
        if size == 0 || size > TRANSFER_DOCUMENT_BYTES {
            return Err(usage("The exchanged transfer request exceeds its size bound".into()));
        }
        let bytes = self.store.get_range(&key, 0..size).await?;
        let digest = ring::digest::digest(&ring::digest::SHA256, &bytes);
        let actual: String = digest.as_ref().iter().map(|byte| format!("{byte:02x}")).collect();
        if bytes.len() as u64 != size || actual != self.digest {
            return Err(usage("The exchanged transfer request has another digest".into()));
        }
        Ok(bytes.to_vec())
    }
}

async fn transfer_document<T: serde::de::DeserializeOwned>(
    arguments: &Arguments,
    exchange: Option<&TransferExchange>,
) -> Result<T> {
    let bytes = match exchange {
        Some(exchange) => exchange.read_request().await?,
        None => {
            let file = std::fs::File::open(arguments.required("transfer")?).map_err(io)?;
            let mut bytes = Vec::new();
            file.take(TRANSFER_DOCUMENT_BYTES + 1).read_to_end(&mut bytes).map_err(io)?;
            if bytes.len() as u64 > TRANSFER_DOCUMENT_BYTES {
                return Err(usage("The cell transfer document exceeds its size limit".into()));
            }
            bytes
        },
    };
    serde_json::from_slice(&bytes).map_err(|_| usage("Invalid cell transfer document".into()))
}

async fn execute_managed_transfer(
    command: &str,
    arguments: &Arguments,
    from: &admin::CellHandle<'_>,
    to: &admin::CellHandle<'_>,
) -> Result<()> {
    let exchange = TransferExchange::open(arguments).await?;
    let result = async {
        let receipt = if command == "prepare-cell-transfer" {
            let intent = transfer_document(arguments, exchange.as_ref()).await?;
            admin::prepare_managed_cell_transfer(from, to, &intent, 4 << 20, hosted_task(arguments)?).await?
        } else {
            let prepared = transfer_document(arguments, exchange.as_ref()).await?;
            admin::activate_managed_cell_transfer(from, to, &prepared).await?;
            prepared
        };
        if exchange.is_none() {
            println!("{}", serde_json::to_string(&receipt).expect("a transfer receipt serializes"));
        }
        Ok(())
    }.await;
    if let Some(exchange) = exchange {
        exchange.io.close_and_wait().await;
    }
    result
}

fn installation_id(arguments: &Arguments) -> Result<uuid::Uuid> {
    required_uuid(arguments, "id")
}

fn required_uuid(arguments: &Arguments, name: &str) -> Result<uuid::Uuid> {
    let raw_id = arguments.required(name)?;
    uuid::Uuid::parse_str(raw_id).ok().filter(|id| !id.is_nil() && id.to_string() == raw_id)
        .ok_or_else(|| usage(format!("--{name} must be a canonical, nonzero UUID")))
}

fn watch_parent(arguments: &Arguments, cancellation: &CancellationToken) {
    if arguments.present("parent-stdin") {
        let stop = cancellation.clone();
        std::thread::spawn(move || {
            let _ = std::io::copy(&mut std::io::stdin().lock(), &mut std::io::sink());
            stop.cancel();
        });
    }
}

async fn run(arguments: Vec<String>) -> Result<()> {
    let Some((command, rest)) = arguments.split_first() else { print!("{HELP}"); return Ok(()); };
    if command == "--build-info" {
        if !rest.is_empty() {
            return Err(usage("--build-info accepts no arguments".into()));
        }
        #[derive(serde::Serialize)]
        struct MultiBuildInfo {
            #[serde(flatten)]
            provenance: rtdb_types::BuildInfo,
            standalone_protocol: u32,
        }
        let report = MultiBuildInfo {
            provenance: rtdb_types::compiled_build_info(),
            standalone_protocol: process::STANDALONE_PROTOCOL,
        };
        let mut output = std::io::stdout().lock();
        serde_json::to_writer(&mut output, &report).map_err(|error| io(std::io::Error::other(error)))?;
        use std::io::Write as _;
        return writeln!(output).map_err(io);
    }
    let arguments = Arguments::parse(rest)?;
    let cancellation = CancellationToken::new();
    { let cancellation = cancellation.clone(); tokio::spawn(async move { shutdown_signal().await; cancellation.cancel(); }); }
    match command.as_str() {
        "help" | "--help" | "-h" => { print!("{HELP}"); Ok(()) }
        "initialize-standalone" => {
            process::initialize_standalone(std::path::Path::new(arguments.required("state")?), arguments.required("name")?, installation_id(&arguments)?).await
        }
        "standalone" => {
            watch_parent(&arguments, &cancellation);
            process::serve_standalone(process::StandaloneSettings {
                root: arguments.required("state")?.into(),
                name: arguments.required("name")?.into(),
                id: installation_id(&arguments)?,
                certificate: arguments.required("certificate")?.into(),
                private_key: arguments.required("private-key")?.into(),
                password_file: arguments.required("password-file")?.into(),
                ready_file: arguments.required("ready-file")?.into(),
            }, cancellation).await
        }
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
                hosted_task: hosted_task(&arguments)?,
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
                    arguments.optional("disposal-token-file").map(std::path::Path::new),
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
        "move-cell" | "prepare-cell-transfer" | "activate-cell-transfer" => {
            admit_hosted_transfer(&command, &arguments).await?;
            let (from_directory, to_directory) = (directory(arguments.required("from-directory")?).await?, directory(arguments.required("to-directory")?).await?);
            let from_storage = CellStorage::parse(arguments.required("from-storage-root")?)?;
            let to_storage = CellStorage::parse(arguments.required("to-storage-root")?)?;
            let from_keys = key_service(arguments.required("from-keys")?).await?;
            let to_keys = key_service(arguments.required("to-keys")?).await?;
            let from = admin::CellHandle { name: arguments.required("from")?, directory: &from_directory, storage: &from_storage, keys: from_keys.as_ref(), maximum_deployments: usize::MAX };
            let to = admin::CellHandle { name: arguments.required("to")?, directory: &to_directory, storage: &to_storage, keys: to_keys.as_ref(), maximum_deployments: usize::MAX };
            match command.as_str() {
                "move-cell" => {
                    let map = rtdb_multi::cells::RemoteCellMap::new(arguments.required("cell-map")?);
                    let record = admin::move_cell(map.as_ref(), &from, &to, arguments.required("name")?, 4 << 20, hosted_task(&arguments)?).await?;
                    println!("{}", serde_json::to_string_pretty(&record).expect("a record serializes"));
                },
                "prepare-cell-transfer" | "activate-cell-transfer" => {
                    execute_managed_transfer(&command, &arguments, &from, &to).await?;
                },
                _ => unreachable!(),
            }
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
            let storage = CellStorage::parse(arguments.required("storage-root")?)?;
            let recovery = if arguments.present("recovery-listen") {
                let configuration = NodeConfiguration::from_yaml(
                    &std::fs::read_to_string(arguments.required("recovery-config")?).map_err(io)?,
                )?;
                let tls_settings = configuration.server.and_then(|server| server.ssl)
                    .ok_or_else(|| usage("The recovery configuration must provide server.ssl".into()))?;
                let tls = rtdb_server::tls::load(&tls_settings, &rtdb_columnar::MemoryBudget::new(16 << 20))?;
                let service = rtdb_multi::recovery::RecoveryService::from_file(
                    std::path::Path::new(arguments.required("recovery-token-file")?),
                    arguments.required("recovery-org")?.to_owned(),
                    storage.clone(),
                    CellStorage::parse(arguments.required("archive-root")?)?,
                    keys.clone().ok_or_else(|| usage("The recovery endpoint requires --keys".into()))?,
                    hosted_task(&arguments)?,
                )?;
                let listener = tokio::net::TcpListener::bind(
                    arguments.address("recovery-listen", "127.0.0.1:0")?,
                ).await.map_err(io)?;
                Some((listener, tls, Arc::new(service)))
            } else {
                None
            };
            let reaper = rtdb_multi::store::run_reaper(
                directory.clone(), storage, keys, grace, Duration::from_secs(10), cancellation.clone(),
            );
            if let Some((listener, tls, service)) = recovery {
                let (_, result) = tokio::join!(reaper, async {
                    let result = rtdb_multi::recovery::serve(listener, tls, directory, service, cancellation.clone()).await;
                    cancellation.cancel();
                    result
                });
                result
            } else {
                reaper.await;
                Ok(())
            }
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
        "schedule-recovery-disposal" => {
            let directory = directory(arguments.required("directory")?).await?;
            let scheduled = rtdb_multi::archive::begin_retirement(
                directory.as_ref(), installation_id(&arguments)?,
            ).await?;
            println!("{}", serde_json::to_string_pretty(&scheduled).expect("archive retirement serializes"));
            Ok(())
        }
        "recovery-disposal" => {
            let directory = directory(arguments.required("directory")?).await?;
            let id = installation_id(&arguments)?;
            let receipt = directory.deployment(&id.to_string()).await?.and_then(|record| record.archive_retirement);
            println!("{}", serde_json::to_string_pretty(&receipt).expect("archive retirement serializes"));
            Ok(())
        }
        "capture" => {
            let directory = directory(arguments.required("directory")?).await?;
            let storage = CellStorage::parse(arguments.required("storage-root")?)?;
            let archive = CellStorage::parse(arguments.required("archive-root")?)?;
            let keys = key_service(arguments.required("keys")?).await?;
            let capture = rtdb_multi::archive::capture(
                directory, &storage, &archive, keys.as_ref(),
                installation_id(&arguments)?, required_uuid(&arguments, "operation")?, None,
            ).await?;
            println!("{}", serde_json::to_string_pretty(&capture).expect("a capture serializes"));
            Ok(())
        }
        "stage-checkpoint" => {
            if std::env::var_os("RTDB_TRANSFER_BINDING").is_some() {
                return Err(usage("Checkpoint staging requires the original archive operator".into()));
            }
            let file = std::fs::File::open(arguments.required("stage-request")?).map_err(io)?;
            let mut body = Vec::new();
            file.take(TRANSFER_DOCUMENT_BYTES + 1).read_to_end(&mut body).map_err(io)?;
            if body.len() as u64 > TRANSFER_DOCUMENT_BYTES {
                return Err(usage("The checkpoint stage request exceeds its size limit".into()));
            }
            let intent = serde_json::from_slice(&body)
                .map_err(|_| usage("Invalid checkpoint stage request".into()))?;
            let directory = directory(arguments.required("directory")?).await?;
            let archive = CellStorage::parse(arguments.required("archive-root")?)?;
            let storage = CellStorage::parse(arguments.required("storage-root")?)?;
            let keys = key_service(arguments.required("keys")?).await?;
            let receipt = rtdb_multi::archive::stage_checkpoint(
                directory, &archive, &storage, keys.as_ref(), intent, hosted_task(&arguments)?,
            ).await?;
            println!("{}", serde_json::to_string(&receipt).expect("a stage receipt serializes"));
        }
        "read-checkpoint-stage" => {
            if std::env::var_os("RTDB_TRANSFER_BINDING").is_some() {
                return Err(usage("Checkpoint inspection requires a recovery operator".into()));
            }
            let directory = directory(arguments.required("directory")?).await?;
            let storage = CellStorage::parse(arguments.required("storage-root")?)?;
            let keys = key_service(arguments.required("keys")?).await?;
            let settings = rtdb_multi::archive::StagedReaderSettings {
                destination: installation_id(&arguments)?,
                operation: required_uuid(&arguments, "operation")?,
                manifest_sha256: arguments.required("manifest-sha256")?.to_owned(),
                certificate: arguments.required("certificate")?.into(),
                private_key: arguments.required("private-key")?.into(),
                password_file: arguments.required("password-file")?.into(),
                ready_file: arguments.required("ready-file")?.into(),
            };
            watch_parent(&arguments, &cancellation);
            rtdb_multi::archive::serve_staged_checkpoint(
                directory, &storage, keys.as_ref(), settings, hosted_task(&arguments)?, cancellation,
            ).await
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
