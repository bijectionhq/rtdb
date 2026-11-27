mod authentication;
mod remotes;
mod node;
mod edn;
mod bounded;
mod receiver_clock;
mod host_zone;
mod multi_checkpoints;
mod multi_journal;
pub(crate) mod properties;

use std::{collections::{BTreeMap, BTreeSet}, path::PathBuf, time::Duration};
use rtdb_auth::{AuthConfig, Secret};
use rtdb_format::{config::{Yaml, YamlFields, default_database_config, decode_database_value, decode_log, decode_storage, parse_yaml, resolve_env_vars, scalar_string}, database::proto::DatabaseConfig};
use rtdb_types::{Anomaly, Result, SourcePosition};
use rtdb_node::NodeOptions;
use rtdb_replication::ReplicationConfig;

pub use remotes::RemoteConfig;
pub use crate::custody_provider::Settings as CustodyProviderSettings;
pub use bounded::{ConfigurationEnvironment, EnvironmentValue, ParsedConfiguration, ParsedYamlDocument, ProcessEnvironment};
pub use properties::object_sort_properties;
pub use host_zone::host_timezone_from;
pub use multi_checkpoints::MultiCheckpointSettings;
pub use multi_journal::MultiJournalSettings;

#[derive(Clone, Debug)]
pub struct PgwireSettings { pub host: String, pub port: i32, pub read_only_port: i32, pub connections: usize, pub ssl: Option<KeyStoreSettings> }
impl Default for PgwireSettings { fn default() -> Self { Self { host: "127.0.0.1".into(), port: 0, read_only_port: -1, connections: 42, ssl: None } } }
#[derive(Clone, Debug)]
pub struct KeyStoreSettings { pub path: PathBuf, pub password: Secret }

#[derive(Clone, Debug)]
pub struct FlightSettings {
    pub host: String, pub port: i32, pub transaction_idle_timeout: Duration, pub prepared_statement_idle_timeout: Duration,
    pub session_idle_timeout: Duration, pub session_unclaimed_timeout: Duration,
    pub owned_native_actions: bool,
    pub store_only: bool,
    pub hosted_controls: Option<rtdb_flight_sql::HostedControlConfig>,
    pub custody_providers: Vec<CustodyProviderSettings>,
}
impl Default for FlightSettings {
    fn default() -> Self { Self { host: "127.0.0.1".into(), port: 0, transaction_idle_timeout: Duration::from_secs(1800), prepared_statement_idle_timeout: Duration::from_secs(1800), session_idle_timeout: Duration::from_secs(1800), session_unclaimed_timeout: Duration::from_secs(60), owned_native_actions: false, store_only: false, hosted_controls: None, custody_providers: Vec::new() } }
}
#[derive(Clone, Debug)]
pub struct HealthSettings { pub host: String, pub port: i32 }
#[derive(Clone, Debug)]
pub struct MemoryCacheSettings { pub maximum_bytes: Option<usize>, pub maximum_ratio: f64 }
impl Default for MemoryCacheSettings { fn default() -> Self { Self { maximum_bytes: None, maximum_ratio: 0.5 } } }
#[derive(Clone, Debug)]
pub struct DiskCacheSettings { pub path: PathBuf, pub maximum_bytes: Option<u64>, pub maximum_ratio: f64 }
#[derive(Clone, Debug)]
/// `optimistic_conflict_limit` (`indexer.optimisticConflictLimit`, an RTDB extension) is the credits a work key's durable
/// footprint grant debit starts from when its operation names none.
pub struct IndexerSettings { pub log_limit: usize, pub page_limit: usize, pub rows_per_block: u64, pub flush_duration: Duration, pub skip_transactions: BTreeSet<SourcePosition>, pub enabled: bool, pub optimistic_conflict_limit: std::num::NonZeroU8 }
#[derive(Clone, Debug)]
pub struct CompactorSettings {
    pub threads: usize,
    pub run_end_encoded_iids: bool,
    pub within_file_recency_branching: bool,
    pub scan_tie_order_from_naming_epoch: Option<std::num::NonZeroU64>,
}
/// Full-text search's index lane; with it off, `MATCH` is still answered, by scanning.
#[derive(Clone, Debug)]
pub struct SearchSettings { pub enabled: bool, pub threads: usize, pub write_format: rtdb_node::SearchFileFormat }
#[derive(Clone, Debug)]
/// `garbage_lifetime` is signed, as XTDB's `java.time.Duration`: a negative lifetime is accepted as upstream does.
pub struct GarbageSettings { pub enabled: bool, pub blocks_to_keep: usize, pub garbage_lifetime: chrono::TimeDelta }
#[derive(Clone, Debug)]
pub struct TracingSettings { pub enabled: bool, pub endpoint: String, pub service_name: String, pub query_tracing: bool, pub transaction_tracing: bool }
/// Physical erasure enrolment for fresh Default primaries; both are off unless configured.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DefaultErasureSettings {
    /// Enrols a fresh native Local primary for public v1 Erase; applies only with `defaultV1Serving`.
    pub local: bool,
    /// Enrols a fresh Default primary with Kafka logs and a Local object store; requires native Kafka and `defaultV1Serving`.
    pub kafka: bool,
}
/// The original key vaults, captured from the startup environment.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyVaultSettings {
    /// `RTDB_CLASSIC_KEY_VAULT`: seals classic logs and objects.
    pub classic: Option<PathBuf>,
    /// `RTDB_V1_TOKEN_KEY_VAULT`: generates v1 serving tokens.
    pub v1_token: Option<PathBuf>,
    /// `RTDB_V1_CHECKPOINT_KEY_VAULT`: generates v1 checkpoint keys.
    pub v1_checkpoint: Option<PathBuf>,
}
impl KeyVaultSettings {
    fn decode(environment: &impl Fn(&str) -> Option<String>) -> Self {
        let path = |name: &str| environment(name).map(PathBuf::from);
        Self { classic: path("RTDB_CLASSIC_KEY_VAULT"), v1_token: path("RTDB_V1_TOKEN_KEY_VAULT"), v1_checkpoint: path("RTDB_V1_CHECKPOINT_KEY_VAULT") }
    }
}
#[derive(Clone, Debug)]
pub enum ModuleSettings { CloudWatch { namespace: String }, AzureMonitor { connection_string: Secret } }

#[derive(Clone)]
pub struct NodeConfiguration {
    pub playground: bool,
    pub server: Option<PgwireSettings>,
    pub flight_sql: Option<FlightSettings>,
    pub(crate) funded_receiver_clock: Option<receiver_clock::Installation>,
    #[cfg(feature = "multi")]
    pub(crate) deployment_admin: Option<std::sync::Arc<dyn rtdb_flight_sql::DeploymentAdminAuthority>>,
    #[cfg(feature = "multi")]
    pub(crate) default_checkpoint_restore_directory: Option<PathBuf>,
    pub health: Option<HealthSettings>,
    pub remotes: BTreeMap<String, RemoteConfig>,
    pub primary: DatabaseConfig,
    pub fixed_databases: Option<BTreeMap<String, DatabaseConfig>>,
    pub memory_cache: MemoryCacheSettings,
    pub disk_cache: Option<DiskCacheSettings>,
    /// The configured node time zone; PostgreSQL sessions do not start in it.
    pub default_timezone: Option<String>,
    /// The host's zone, from the startup environment's `TZ` or `/etc/localtime`; PostgreSQL sessions start in it.
    pub host_timezone: String,
    pub indexer: IndexerSettings,
    pub compactor: CompactorSettings,
    pub garbage: GarbageSettings,
    pub search: SearchSettings,
    pub auth: AuthConfig,
    pub tracing: TracingSettings,
    pub modules: Vec<ModuleSettings>,
    pub read_only_databases: bool,
    pub default_v1_serving: bool,
    pub default_erasure: DefaultErasureSettings,
    pub key_vaults: KeyVaultSettings,
    pub ordered_index_arrow_references: bool,
    pub ordered_index_arrow_references_on_private_storage: bool,
    pub skipped_databases: BTreeSet<String>,
    pub node_id: String,
    pub local_log_uses_clock: bool,
    pub kafka_auto_create_topic: bool,
    pub join_build_rows: i64,
    /// Overrides the per-query spill limit: `query.spillLimitBytes`, else `RTDB_QUERY_SPILL_LIMIT_BYTES`.
    pub query_spill_limit_bytes: Option<u64>,
    pub generate_series_end_exclusive: bool,
    pub object_sort: rtdb_types::ObjectSort,
    pub apple_sync: rtdb_storage::AppleSync,
    pub multi_checkpoints: MultiCheckpointSettings,
    pub multi_journal: MultiJournalSettings,
}
impl std::fmt::Debug for NodeConfiguration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NodeConfiguration").field("node_id", &self.node_id).field("remote_aliases", &self.remotes.keys()).field("ingest", &self.fixed_databases.is_some()).finish_non_exhaustive()
    }
}

impl NodeConfiguration {
    /// Mounts the host's deployment authority on the ordinary Flight listener.
    #[cfg(feature = "multi")]
    pub fn with_deployment_admin(mut self, authority: std::sync::Arc<dyn rtdb_flight_sql::DeploymentAdminAuthority>) -> Self {
        self.deployment_admin = Some(authority);
        self
    }

    pub fn from_yaml(text: &str) -> Result<Self> { Self::from_yaml_with_environment(text, &|name| std::env::var(name).ok(), false) }
    pub fn from_ingest_yaml(text: &str) -> Result<Self> { Self::from_yaml_with_environment(text, &|name| std::env::var(name).ok(), true) }
    pub fn from_yaml_with_environment(text: &str, environment: &impl Fn(&str) -> Option<String>, ingest: bool) -> Result<Self> {
        let mut value = if text.trim().is_empty() { Yaml::Mapping(Default::default()) } else { parse_yaml(text)? };
        resolve_env_vars(&mut value, environment)?;
        Self::from_value(value, environment, ingest)
    }
    fn from_value(value: Yaml, environment: &impl Fn(&str) -> Option<String>, ingest: bool) -> Result<Self> {
        let mut fields = YamlFields::new(value, "node")?;
        let mut primary = default_database_config();
        let mut local_log_uses_clock = false;
        let mut kafka_auto_create_topic = true;
        if let Some(log) = fields.take("log") {
            if ingest { return Err(invalid("Ingest node configuration uses 'databases', not a primary 'log'")); }
            if let Yaml::Tagged(tagged) = &log {
                if let Yaml::Mapping(values) = &tagged.value {
                    if let Some(value) = values.get(Yaml::String("useInstantSourceForNonTx".into())) { local_log_uses_clock = boolean(value, "useInstantSourceForNonTx")?; }
                    if let Some(value) = values.get(Yaml::String("autoCreateTopic".into())) { kafka_auto_create_topic = boolean(value, "autoCreateTopic")?; }
                }
            }
            primary.log = Some(decode_log(log)?);
        }
        if let Some(storage) = fields.take("storage") {
            if ingest { return Err(invalid("Ingest node configuration uses 'databases', not primary 'storage'")); }
            primary.storage = Some(decode_storage(storage)?);
        }
        let remotes = remotes::decode_maps(fields.take("logClusters"), fields.take("remotes"))?;
        let memory_cache = fields.take("memoryCache").map(memory_cache).transpose()?.unwrap_or_default();
        let disk_cache = fields.take_nullable("diskCache").map(disk_cache).transpose()?;
        let health = fields.take_nullable("healthz").map(health).transpose()?;
        let indexer = indexer(fields.take("indexer"), environment)?;
        let node_id = fields.string("nodeId")?.or_else(|| environment("XTDB_NODE_ID")).unwrap_or_else(|| uuid::Uuid::new_v4().to_string().split('-').next().unwrap_or("rtdb").into());
        // As XTDB's `parse-long`, a value that is not a signed 64-bit integer keeps the default.
        let join_build_rows = environment("XTDB_JOIN_SPILL_THRESHOLD").and_then(|text| join_threshold(&text)).unwrap_or(100_000);
        let query_spill_limit_bytes = query_spill_limit(fields.take("query"), environment)?;
        let host_timezone = host_zone::host_timezone(environment)?;
        let key_vaults = KeyVaultSettings::decode(environment);
        let generate_series_end_exclusive = environment("XTDB_GENERATE_SERIES_END_EXCLUSIVE").is_some_and(|value|value.eq_ignore_ascii_case("true"));
        let default_erasure = DefaultErasureSettings {
            local: erasure_flag(&mut fields, "defaultLocalErasure", "RTDB_DEFAULT_LOCAL_ERASURE", environment)?,
            kafka: erasure_flag(&mut fields, "defaultKafkaErasure", "RTDB_DEFAULT_KAFKA_ERASURE", environment)?,
        };
        let references = fields.boolean("orderedIndexArrowReferences")?;
        let ordered_index_arrow_references = references.unwrap_or(false);
        let ordered_index_arrow_references_on_private_storage = references.is_none();
        let multi_checkpoints = MultiCheckpointSettings::decode(fields.take("multiCheckpoints"), environment)?;
        let multi_journal = MultiJournalSettings::decode(fields.take("multiJournal"))?;
        let apple_sync = apple_sync(environment)?;
        let configuration = if ingest {
            let fixed_databases = match fields.take("databases") {
                Some(value) => YamlFields::new(value, "databases")?.into_entries().into_iter().map(|(name, value)| Ok((name, decode_database_value(value)?))).collect::<Result<_>>()?,
                None => BTreeMap::new(),
            };
            Self { playground: false, server: None, flight_sql: None, funded_receiver_clock: None,
                #[cfg(feature = "multi")]
                deployment_admin: None,
                #[cfg(feature = "multi")]
                default_checkpoint_restore_directory: None,
                health, remotes, primary, fixed_databases: Some(fixed_databases), memory_cache, disk_cache,
                default_timezone: None, host_timezone, indexer, compactor: compactor(None, environment)?, garbage: garbage(None)?, search: search(None)?, auth: authentication::default_auth(environment),
                tracing: tracing(None)?, modules: Vec::new(), read_only_databases: false, default_v1_serving: false, default_erasure, key_vaults, query_spill_limit_bytes, ordered_index_arrow_references, ordered_index_arrow_references_on_private_storage, skipped_databases: BTreeSet::new(), node_id, local_log_uses_clock, kafka_auto_create_topic, join_build_rows, generate_series_end_exclusive, object_sort: Default::default(), apple_sync, multi_checkpoints, multi_journal }
        } else {
            let server = match fields.take("server") { None => Some(PgwireSettings::default()), Some(Yaml::Null) => None, Some(value) => Some(pgwire(value)?) };
            let flight_sql = match fields.take("flightSql") { None => Some(FlightSettings::default()), Some(Yaml::Null) => None, Some(value) => Some(flight(value)?) };
            let funded_receiver_clock = fields.take("fundedReceiverClock").map(receiver_clock::Installation::decode).transpose()?;
            let default_timezone = fields.string("defaultTz")?.as_deref().map(rtdb_expr::normalize_timezone).transpose()?;
            let compactor = compactor(fields.take("compactor"), environment)?;
            let garbage = garbage(fields.take("garbageCollector"))?;
            let search = search(fields.take("search"))?;
            let auth = fields.take("authn").map(|value| authentication::decode(value, environment)).transpose()?.unwrap_or_else(|| authentication::default_auth(environment));
            #[cfg(feature = "multi")]
            let deployment_admin = authentication::deployment_administrators(
                fields.sequence("deploymentAdminPrincipals")?, fields.sequence("deploymentCreatePrincipals")?,
            )?;
            #[cfg(feature = "multi")]
            let default_checkpoint_restore_directory = fields.string("defaultCheckpointRestoreDirectory")?.map(PathBuf::from);
            #[cfg(feature = "multi")]
            if default_checkpoint_restore_directory.as_ref().is_some_and(|directory| !directory.is_absolute()) {
                return Err(invalid("defaultCheckpointRestoreDirectory requires an absolute path"));
            }
            #[cfg(feature = "multi")]
            if deployment_admin.is_some() && !flight_sql.as_ref().is_some_and(|flight| flight.port >= 0) {
                return Err(invalid("Deployment administration grants require an ordinary Flight SQL listener"));
            }
            let tracing = tracing(fields.take("tracer"))?;
            let modules = fields.sequence("modules")?.unwrap_or_default().into_iter().map(module).collect::<Result<_>>()?;
            let read_only_databases = fields.boolean("readOnlyDatabases")?.unwrap_or(false);
            let default_v1_serving = fields.boolean("defaultV1Serving")?.unwrap_or(false);
            let skipped_databases = match fields.sequence("skipDbs")? {
                Some(values) => values.iter().map(|value| scalar_string(value, "skipDbs")).collect::<Result<_>>()?,
                None => environment("XTDB_SKIP_DBS").into_iter().flat_map(|value| value.split(',').map(str::trim).filter(|value| !value.is_empty()).map(str::to_owned).collect::<Vec<_>>()).collect(),
            };
            Self { playground: false, server, flight_sql, funded_receiver_clock,
                #[cfg(feature = "multi")]
                deployment_admin,
                #[cfg(feature = "multi")]
                default_checkpoint_restore_directory,
                health, remotes, primary, fixed_databases: None, memory_cache, disk_cache, default_timezone, host_timezone, indexer, compactor, garbage, search, auth, tracing, modules, read_only_databases, default_v1_serving, default_erasure, key_vaults, query_spill_limit_bytes, ordered_index_arrow_references, ordered_index_arrow_references_on_private_storage, skipped_databases, node_id, local_log_uses_clock, kafka_auto_create_topic, join_build_rows, generate_series_end_exclusive, object_sort: Default::default(), apple_sync, multi_checkpoints, multi_journal }
        };
        fields.finish()?;
        Ok(configuration)
    }
}

/// Reads an erasure switch from its YAML key, else from its environment variable.
/// The variable keeps its historical spelling: exactly `1` enables, and `0` or empty disables.
/// Any other value is rejected rather than read as off, since it once silently meant off and may have been meant as on.
fn erasure_flag(fields: &mut YamlFields, key: &str, variable: &str, environment: &impl Fn(&str) -> Option<String>) -> Result<bool> {
    if let Some(value) = fields.boolean(key)? { return Ok(value); }
    match environment(variable).as_deref() {
        None | Some("" | "0") => Ok(false),
        Some("1") => Ok(true),
        Some(_) => Err(invalid(format!("{variable} must be 1 or 0"))),
    }
}

/// The process setting that selects the Apple durable-write policy for every command of a binary.
pub const APPLE_SYNC_SETTING: &str = "RTDB_APPLE_SYNC";

/// Validates `RTDB_APPLE_SYNC` from the startup environment.
/// - Errors: a value other than blank, `full` or `fsync`.
pub fn apple_sync(environment: &impl Fn(&str) -> Option<String>) -> Result<rtdb_storage::AppleSync> {
    rtdb_storage::AppleSync::from_setting(environment(APPLE_SYNC_SETTING).as_deref())
        .map_err(|error| error.with_context("setting", APPLE_SYNC_SETTING.into()))
}

/// Installs the process-wide settings the libraries read no environment for: the `XTDB_VERSION` and `GIT_SHA` version
/// overrides, and the `RTDB_TZDB_FILE` replacement for the bundled time-zone rules.
/// - Errors: a setting already installed or used with a different value.
pub fn install_process_settings(environment: &impl Fn(&str) -> Option<String>) -> Result<()> {
    rtdb_types::install_runtime_version(environment("XTDB_VERSION").as_deref(), environment("GIT_SHA").as_deref())?;
    rtdb_types::install_tzdb_file(environment("RTDB_TZDB_FILE").filter(|path| !path.is_empty()).map(std::path::PathBuf::from))?;
    rtdb_types::residence_trace::install(environment("RTDB_RESIDENCE_TRACE_PATH").filter(|path| !path.is_empty()).map(std::path::PathBuf::from))
}

/// The process setting that caps one query's spill files when the YAML sets no `query.spillLimitBytes`.
pub const QUERY_SPILL_LIMIT_SETTING: &str = "RTDB_QUERY_SPILL_LIMIT_BYTES";

/// Reads `query.spillLimitBytes`, else [`QUERY_SPILL_LIMIT_SETTING`].
/// - Errors: a value that is not a non-negative byte count, or an unknown key in the `query` section.
fn query_spill_limit(value: Option<Yaml>, environment: &impl Fn(&str) -> Option<String>) -> Result<Option<u64>> {
    let mut fields = YamlFields::new(value.unwrap_or_else(empty), "query")?;
    let configured = fields.string("spillLimitBytes")?.map(|text| (text, "query.spillLimitBytes"));
    fields.finish()?;
    configured.or_else(|| environment(QUERY_SPILL_LIMIT_SETTING).map(|text| (text, QUERY_SPILL_LIMIT_SETTING)))
        .map(|(text, source)| text.trim().parse::<u64>().map_err(|_| invalid(format!("{source} must be a non-negative byte count"))))
        .transpose()
}

fn join_threshold(value: &str) -> Option<i64> {
    let (negative, digits) = match value.as_bytes().first() { Some(b'-') => (true, &value[1..]), Some(b'+') => (false, &value[1..]), _ => (false, value) };
    if digits.is_empty() { return None; }
    let magnitude = digits.chars().try_fold(0i64, |value, digit| value.checked_mul(10)?.checked_sub(i64::from(rtdb_types::decimal_digit(digit)?)))?;
    if negative { Some(magnitude) } else { magnitude.checked_neg() }
}

fn pgwire(value: Yaml) -> Result<PgwireSettings> {
    let mut fields = YamlFields::new(value, "server")?;
    let host = bind_host(&mut fields)?;
    let port = decode_port(&mut fields, "port", 0)?;
    let read_only_port = decode_port(&mut fields, "readOnlyPort", -1)?;
    let connections = positive(fields.integer("numThreads")?.unwrap_or(42) as i64, "numThreads")?;
    let ssl = fields.take_nullable("ssl").map(|value| {
        let mut fields = YamlFields::new(value, "ssl")?;
        let ssl = KeyStoreSettings { path: fields.required_string("keyStore")?.into(), password: Secret::new(fields.required_string("keyStorePassword")?) };
        fields.finish()?;
        Ok::<_, Anomaly>(ssl)
    }).transpose()?;
    fields.finish()?;
    Ok(PgwireSettings { host, port, read_only_port, connections, ssl })
}
fn flight(value: Yaml) -> Result<FlightSettings> {
    let mut fields = YamlFields::new(value, "flightSql")?;
    let hosted_controls = fields.take("hostedControls").map(|value| {
        let mut controls = YamlFields::new(value, "flightSql.hostedControls")?;
        let owner = controls.required_string("owner")?;
        let relations = controls.sequence("relations")?.unwrap_or_default().iter()
            .map(|value| scalar_string(value, "hosted control relation")).collect::<Result<BTreeSet<_>>>()?;
        controls.finish()?;
        Ok::<_, Anomaly>(rtdb_flight_sql::HostedControlConfig { owner, relations })
    }).transpose()?;
    let custody_providers = custody_providers(&mut fields)?;
    if hosted_controls.is_none() && custody_providers.iter().any(|route| route.owner.is_none()) {
        return Err(Anomaly::incorrect("rtdb/custody-provider-configuration", "Each custody provider route requires a configured owner"));
    }
    let config = FlightSettings { host: fields.string("host")?.map(normalize_bind_host).unwrap_or_else(|| "127.0.0.1".into()), port: decode_port(&mut fields, "port", 0)?,
        transaction_idle_timeout: duration(&mut fields, "transactionIdleTimeout", 1800)?, prepared_statement_idle_timeout: duration(&mut fields, "preparedStatementIdleTimeout", 1800)?,
        session_idle_timeout: duration(&mut fields, "sessionIdleTimeout", 1800)?, session_unclaimed_timeout: duration(&mut fields, "sessionUnclaimedTimeout", 60)?, owned_native_actions: fields.boolean("ownedNativeActions")?.unwrap_or(false), store_only: false, hosted_controls, custody_providers };
    fields.finish()?;
    Ok(config)
}

fn custody_providers(fields: &mut YamlFields) -> Result<Vec<CustodyProviderSettings>> {
    let providers = fields.sequence("custodyProviders")?.unwrap_or_default().into_iter()
        .map(crate::custody_provider::read_settings).collect::<Result<Vec<_>>>()?;
    if providers.iter().map(|provider| &provider.database).collect::<BTreeSet<_>>().len() != providers.len() {
        return Err(Anomaly::incorrect("rtdb/custody-provider-configuration", "A database accepts one configured original custody provider"));
    }
    Ok(providers)
}

pub fn parse_custody_provider_config(text: &str) -> Result<Vec<CustodyProviderSettings>> {
    let mut value = parse_yaml(text)?;
    resolve_env_vars(&mut value, &|name| std::env::var(name).ok())?;
    let mut fields = YamlFields::new(value, "custody provider configuration")?;
    let providers = custody_providers(&mut fields)?;
    fields.finish()?;
    Ok(providers)
}
fn health(value: Yaml) -> Result<HealthSettings> {
    let mut fields = YamlFields::new(value, "healthz")?;
    let settings = HealthSettings { host: bind_host(&mut fields)?, port: decode_port(&mut fields, "port", 0)? };
    fields.finish()?;
    Ok(settings)
}
fn memory_cache(value: Yaml) -> Result<MemoryCacheSettings> {
    let mut fields = YamlFields::new(value, "memoryCache")?;
    let maximum_bytes = nullable_i64(&mut fields, "maxSizeBytes")?.map(|bytes| positive(bytes, "memoryCache.maxSizeBytes")).transpose()?;
    let maximum_ratio = ratio(&mut fields, "maxSizeRatio", 0.5)?;
    fields.finish()?;
    Ok(MemoryCacheSettings { maximum_bytes, maximum_ratio })
}
fn disk_cache(value: Yaml) -> Result<DiskCacheSettings> {
    let mut fields = YamlFields::new(value, "diskCache")?;
    let path = fields.required_string("path")?.into();
    let maximum_bytes = nullable_i64(&mut fields, "maxSizeBytes")?.map(|bytes| positive(bytes, "diskCache.maxSizeBytes").map(|bytes| bytes as u64)).transpose()?;
    let maximum_ratio = ratio(&mut fields, "maxSizeRatio", 0.75)?;
    fields.finish()?;
    Ok(DiskCacheSettings { path, maximum_bytes, maximum_ratio })
}
fn indexer(value: Option<Yaml>, environment: &impl Fn(&str) -> Option<String>) -> Result<IndexerSettings> {
    let mut fields = YamlFields::new(value.unwrap_or_else(empty), "indexer")?;
    let log_limit = positive(integer(&mut fields, "logLimit", 64)?, "logLimit")?;
    let page_limit = positive(integer(&mut fields, "pageLimit", 1024)?, "pageLimit")?;
    let rows_per_block = positive(integer(&mut fields, "rowsPerBlock", ReplicationConfig::DEFAULT_ROWS_PER_BLOCK as i64)?, "rowsPerBlock")? as u64;
    let flush_duration = duration(&mut fields, "flushDuration", ReplicationConfig::DEFAULT_FLUSH_TIMEOUT.as_secs())?;
    let skip_transactions = match fields.sequence("skipTxs")? {
        Some(values) => values.iter().map(|value| scalar_string(value, "skipTxs")?.parse::<i64>().map(SourcePosition).map_err(|_| invalid("skipTxs must contain message IDs"))).collect::<Result<_>>()?,
        None => environment("XTDB_SKIP_TXS").into_iter().flat_map(|value| value.split(',').map(str::trim).map(str::to_owned).collect::<Vec<_>>()).map(|value| value.parse::<i64>().map(SourcePosition).map_err(|_| invalid("XTDB_SKIP_TXS must contain message IDs"))).collect::<Result<_>>()?,
    };
    let enabled = fields.boolean("enabled")?.unwrap_or(true);
    let optimistic_conflict_limit = fields.integer("optimisticConflictLimit")?
        .map(|limit| u8::try_from(limit).ok().and_then(std::num::NonZeroU8::new)
            .ok_or_else(|| invalid("indexer.optimisticConflictLimit must be between 1 and 255"))).transpose()?
        .unwrap_or(rtdb_tx::FootprintGrantOptions::DEFAULT_OPTIMISTIC_CONFLICT_LIMIT);
    fields.finish()?;
    Ok(IndexerSettings { log_limit, page_limit, rows_per_block, flush_duration, skip_transactions, enabled, optimistic_conflict_limit })
}
fn compactor(value: Option<Yaml>, environment: &impl Fn(&str) -> Option<String>) -> Result<CompactorSettings> {
    let mut fields = YamlFields::new(value.unwrap_or_else(empty), "compactor")?;
    let threads = fields.integer("threads")?.map_or_else(|| NodeOptions::default_compaction_threads(environment), |threads| threads.max(0) as usize);
    let opt_in_221 = fields.boolean("optIn221")?.unwrap_or_else(|| env_boolean(environment, "XTDB_COMPACTOR_221_OPT_IN"));
    let scan_tie_order_from_naming_epoch = fields.integer("scanTieOrderFromNamingEpoch")?
        .map(|epoch| u64::try_from(epoch).ok().and_then(std::num::NonZeroU64::new)
            .ok_or_else(|| invalid("compactor.scanTieOrderFromNamingEpoch must be positive"))).transpose()?;
    fields.finish()?;
    Ok(CompactorSettings { threads, run_end_encoded_iids: opt_in_221, within_file_recency_branching: opt_in_221, scan_tie_order_from_naming_epoch })
}
fn garbage(value: Option<Yaml>) -> Result<GarbageSettings> {
    let mut fields = YamlFields::new(value.unwrap_or_else(empty), "garbageCollector")?;
    let blocks_to_keep = fields.string("blocksToKeep")?.map(|text| yaml_int(&text).ok_or_else(|| invalid("'blocksToKeep' must be a 32-bit integer"))).transpose()?;
    let result = GarbageSettings { enabled: fields.boolean("enabled")?.unwrap_or(true), blocks_to_keep: positive(i64::from(blocks_to_keep.unwrap_or(10)), "blocksToKeep")?,
        garbage_lifetime: fields.string("garbageLifetime")?.map(|value| parse_signed_duration(&value)).transpose()?.unwrap_or_else(|| chrono::TimeDelta::hours(24)) };
    fields.finish()?;
    Ok(result)
}
fn search(value: Option<Yaml>) -> Result<SearchSettings> {
    let mut fields = YamlFields::new(value.unwrap_or_else(empty), "search")?;
    let write_format = match fields.integer("writeFormat")? {
        None => rtdb_node::SearchFileFormat::default(),
        Some(11) => rtdb_node::SearchFileFormat::Tantivy,
        Some(12) => rtdb_node::SearchFileFormat::Arrow,
        Some(13) => rtdb_node::SearchFileFormat::Paged,
        Some(14) => rtdb_node::SearchFileFormat::Tree,
        Some(15) => rtdb_node::SearchFileFormat::Packed,
        Some(16) => rtdb_node::SearchFileFormat::Logical,
        _ => return Err(rtdb_types::Anomaly::incorrect("rtdb.config/search-format", "search.writeFormat must be between 11 and 16")),
    };
    let result = SearchSettings { enabled: fields.boolean("enabled")?.unwrap_or(true),
        threads: positive(fields.integer("threads")?.unwrap_or(1) as i64, "threads")?, write_format };
    fields.finish()?;
    Ok(result)
}
fn tracing(value: Option<Yaml>) -> Result<TracingSettings> {
    let mut fields = YamlFields::new(value.unwrap_or_else(empty), "tracer")?;
    let result = TracingSettings { enabled: fields.boolean("enabled")?.unwrap_or(false), endpoint: fields.string("endpoint")?.unwrap_or_else(|| "http://localhost:4318/v1/traces".into()),
        service_name: fields.string("serviceName")?.unwrap_or_else(|| "xtdb".into()), query_tracing: fields.boolean("queryTracing")?.unwrap_or(true), transaction_tracing: fields.boolean("transactionTracing")?.unwrap_or(true) };
    fields.finish()?;
    Ok(result)
}
fn module(value: Yaml) -> Result<ModuleSettings> {
    let (tag, mut fields) = YamlFields::tagged(value, "module")?;
    let result = match tag.as_str() {
        "!CloudWatch" => ModuleSettings::CloudWatch { namespace: fields.string("namespace")?.unwrap_or_else(|| "xtdb.metrics".into()) },
        "!AzureMonitor" => ModuleSettings::AzureMonitor { connection_string: Secret::new(fields.required_string("connectionString")?) },
        _ => return Err(invalid(format!("Unknown module configuration tag '{tag}'"))),
    };
    fields.finish()?;
    Ok(result)
}
fn empty() -> Yaml { Yaml::Mapping(Default::default()) }
fn bind_host(fields: &mut YamlFields) -> Result<String> {
    match fields.take("host") {
        None => Ok("127.0.0.1".into()),
        Some(Yaml::Null) => Ok("0.0.0.0".into()),
        Some(value) => scalar_string(&value, "host").map(normalize_bind_host),
    }
}
fn normalize_bind_host(host: String) -> String {
    if host == "*" { "0.0.0.0".into() } else { host }
}
fn env_boolean(environment: &impl Fn(&str) -> Option<String>, key: &str) -> bool { environment(key).is_some_and(|value| value.eq_ignore_ascii_case("true")) }
fn decode_port(fields: &mut YamlFields, key: &str, default: i32) -> Result<i32> {
    let port = fields.integer(key)?.unwrap_or(default);
    if !(-1..=65535).contains(&port) { return Err(Anomaly::incorrect("xtdb.error/illegal-arg", format!("Port value out of range: {port}")).with_context("key", key.into())); }
    Ok(port)
}
/// A 32-bit integer as XTDB's YAML decoder (kaml) reads one: decimal, or `0x` hexadecimal or `0o` octal, each optionally negated.
fn yaml_int(text: &str) -> Option<i32> {
    let (negative, body) = text.strip_prefix('-').map_or((false, text), |rest| (true, rest));
    let (radix, digits) = if let Some(hex) = body.strip_prefix("0x") { (16, hex) } else if let Some(octal) = body.strip_prefix("0o") { (8, octal) } else { return join_threshold(text).and_then(|value| i32::try_from(value).ok()); };
    if digits.starts_with(['+', '-']) && negative { return None; }
    i32::from_str_radix(&if negative { format!("-{digits}") } else { digits.to_owned() }, radix).ok()
}
fn positive(value: i64, key: &str) -> Result<usize> { usize::try_from(value).ok().filter(|value| *value > 0).ok_or_else(|| invalid(format!("{key} must be positive"))) }
fn integer(fields: &mut YamlFields, key: &str, default: i64) -> Result<i64> { fields.string(key)?.map(|value| value.parse().map_err(|_| invalid(format!("{key} must be an integer")))).transpose().map(|value| value.unwrap_or(default)) }
fn nullable_i64(fields: &mut YamlFields, key: &str) -> Result<Option<i64>> { fields.nullable_string(key)?.map(|value| value.parse().map_err(|_| invalid(format!("{key} must be an integer")))).transpose() }
fn ratio(fields: &mut YamlFields, key: &str, default: f64) -> Result<f64> {
    let value = fields.string(key)?.map(|value| value.parse::<f64>().map_err(|_| invalid(format!("{key} must be a ratio")))).transpose()?.unwrap_or(default);
    if !value.is_finite() || !(0.0..=1.0).contains(&value) || value == 0.0 { return Err(invalid(format!("{key} must be greater than zero and at most one"))); }
    Ok(value)
}
fn boolean(value: &Yaml, key: &str) -> Result<bool> { match scalar_string(value, key)?.as_str() { "true" => Ok(true), "false" => Ok(false), _ => Err(invalid(format!("{key} must be a boolean"))) } }
fn duration(fields: &mut YamlFields, key: &str, seconds: u64) -> Result<Duration> { fields.string(key)?.map(|value| parse_duration(&value)).transpose().map(|value| value.unwrap_or(Duration::from_secs(seconds))) }
fn signed_duration(fields: &mut YamlFields, key: &str, default: chrono::TimeDelta) -> Result<chrono::TimeDelta> {
    fields.string(key)?.map(|value| parse_signed_duration(&value)).transpose().map(|value| value.unwrap_or(default))
}

/// Parses a non-negative ISO-8601 duration of days, hours, minutes and seconds.
pub fn parse_duration(value: &str) -> Result<Duration> {
    let nanos = duration_nanos(value)?;
    let seconds = u64::try_from(nanos / 1_000_000_000).map_err(|_| invalid("Duration must be nonnegative"))?;
    let fraction = u32::try_from(nanos % 1_000_000_000).map_err(|_| invalid("Duration must be nonnegative"))?;
    Ok(Duration::new(seconds, fraction))
}

/// Parses an ISO-8601 duration of days, hours, minutes and seconds that may be negative, as `java.time.Duration.parse`.
pub fn parse_signed_duration(value: &str) -> Result<chrono::TimeDelta> {
    let nanos = duration_nanos(value)?;
    let seconds = i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| invalid("Duration exceeds range"))?;
    chrono::TimeDelta::new(seconds, nanos.rem_euclid(1_000_000_000) as u32).ok_or_else(|| invalid("Duration exceeds range"))
}

fn duration_nanos(value: &str) -> Result<i128> {
    let regex = regex::Regex::new(r"(?i)^([+-]?)P(?:([+-]?\d+)D)?(?:T(?:([+-]?\d+)H)?(?:([+-]?\d+)M)?(?:([+-]?\d+)(?:[.,](\d{0,9}))?S)?)?$").map_err(|error| invalid(error.to_string()))?;
    let captures = regex.captures(value).ok_or_else(|| invalid("Duration must use ISO-8601 days, hours, minutes and seconds"))?;
    if (2..=5).all(|index| captures.get(index).is_none()) || (value.to_ascii_uppercase().contains('T') && (3..=5).all(|index| captures.get(index).is_none())) { return Err(invalid("Duration has no components")); }
    let mut nanos = 0i128;
    for (index, factor) in [(2, 86400i128), (3, 3600), (4, 60), (5, 1)] {
        if let Some(value) = captures.get(index) { nanos = nanos.checked_add(value.as_str().parse::<i128>().map_err(|_| invalid("Duration exceeds range"))?.checked_mul(factor * 1_000_000_000).ok_or_else(|| invalid("Duration exceeds range"))?).ok_or_else(|| invalid("Duration exceeds range"))?; }
    }
    if let Some(fraction) = captures.get(6) {
        if !fraction.is_empty() {
            let fraction_value = fraction.as_str().parse::<i128>().map_err(|_| invalid("Invalid duration fraction"))? * 10i128.pow((9 - fraction.len()) as u32);
            let sign = if captures.get(5).is_some_and(|seconds| seconds.as_str().starts_with('-')) { -1 } else { 1 };
            nanos = nanos.checked_add(fraction_value * sign).ok_or_else(|| invalid("Duration exceeds range"))?;
        }
    }
    if captures.get(1).is_some_and(|sign| sign.as_str() == "-") { nanos = nanos.checked_neg().ok_or_else(|| invalid("Duration exceeds range"))?; }
    Ok(nanos)
}

fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb/invalid-node-config", message) }
