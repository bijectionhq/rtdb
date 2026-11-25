use rtdb_format::config::Yaml;
use rtdb_types::Result;
use rtdb_xtql::{Edn, EdnKind};
use serde_yaml::{Mapping, value::{Tag, TaggedValue}};

use super::{NodeConfiguration, invalid};

impl NodeConfiguration {
    /// Decodes the data-only Clojure configuration surface and reports ignored top-level keys.
    /// Only `#env` is added to the EDN reader; JVM object constructors and reader evaluation are unavailable.
    pub fn from_edn_with_environment(text: &str, environment: &impl Fn(&str) -> Option<String>) -> Result<(Self, Vec<String>)> {
        let mut root = rtdb_xtql::read(text)?;
        expand_environment(&mut root, environment)?;
        let mut output = Mapping::new();
        let mut warnings = Vec::new();
        let mut modules = Vec::new();
        for (key, value) in entries(&root)? {
            let key = name(key)?;
            let (target, converted) = match key.as_str() {
                "default-tz" => ("defaultTz", scalar(value)?),
                "log" | "xtdb/log" => ("log", backend(value, Kind::Log)?),
                "storage" | "xtdb.buffer-pool/storage" => ("storage", backend(value, Kind::Storage)?),
                "xtdb.buffer-pool/in-memory" => ("storage", tagged("!InMemory", fields(value, &[("epoch", "epoch")])?)),
                "xtdb.buffer-pool/local" => ("storage", tagged("!Local", fields(value, &[("epoch", "epoch"), ("path", "path")])?)),
                "xtdb.buffer-pool/remote" => ("storage", storage_remote(value)?),
                "log-clusters" | "xtdb.log/clusters" => ("logClusters", named_backends(value, Kind::Cluster)?),
                "remotes" | "xtdb.remote/remotes" => ("remotes", named_backends(value, Kind::Remote)?),
                "memory-cache" | "xtdb.cache/memory" => ("memoryCache", cache(value, false)?),
                "disk-cache" | "xtdb.cache/disk" => ("diskCache", cache(value, true)?),
                "indexer" => ("indexer", durations(fields(value, &[("rows-per-block", "rowsPerBlock"), ("page-limit", "pageLimit"), ("log-limit", "logLimit"), ("skip-txs", "skipTxs"), ("enabled?", "enabled"), ("flush-duration", "flushDuration")])?, &["flushDuration"])?),
                "compactor" => ("compactor", fields(value, &[("threads", "threads"), ("opt-in-221?", "optIn221")])?),
                "search" | "xtdb/search" => ("search", fields(value, &[("enabled?", "enabled"), ("threads", "threads")])?),
                "skip-dbs" => ("skipDbs", scalar(value)?),
                "authn" | "xtdb/authn" => ("authn", backend(value, Kind::Authentication)?),
                "xtdb.authn/single-root-user-authn" => ("authn", authentication("single-root-user", value)?),
                "xtdb.authn/user-list-authn" => ("authn", authentication("user-list", value)?),
                "xtdb.authn/openid-connect-authn" => ("authn", authentication("openid-connect", value)?),
                "server" | "xtdb.pgwire/server" => ("server", server(value)?),
                "flight-sql" | "xtdb.flight-sql/server" => ("flightSql", durations(fields(value, &[("host", "host"), ("port", "port"), ("transaction-idle-timeout", "transactionIdleTimeout"), ("prepared-statement-idle-timeout", "preparedStatementIdleTimeout"), ("session-idle-timeout", "sessionIdleTimeout"), ("session-unclaimed-timeout", "sessionUnclaimedTimeout")])?, &["transactionIdleTimeout", "preparedStatementIdleTimeout", "sessionIdleTimeout", "sessionUnclaimedTimeout"])?),
                "funded-receiver-clock" => ("fundedReceiverClock", funded_receiver_clock(value)?),
                "healthz" | "xtdb/healthz" => ("healthz", host(fields(value, &[("host", "host"), ("port", "port")])?, value)?),
                "garbage-collector" | "xtdb/garbage-collector" => ("garbageCollector", fields(value, &[("enabled?", "enabled"), ("blocks-to-keep", "blocksToKeep"), ("garbage-lifetime", "garbageLifetime"), ("reader-lease", "readerLease"), ("reader-lease-margin", "readerLeaseMargin")])?),
                "tracer" | "xtdb/tracer" => ("tracer", fields(value, &[("enabled?", "enabled"), ("endpoint", "endpoint"), ("service-name", "serviceName"), ("query-tracing?", "queryTracing"), ("transaction-tracing?", "transactionTracing")])?),
                "xtdb.aws.cloudwatch/metrics" => { modules.push(tagged("!CloudWatch", fields(value, &[("namespace", "namespace")])?)); continue; }
                "xtdb.azure.monitor/metrics" => { modules.push(tagged("!AzureMonitor", fields(value, &[("connection-string", "connectionString")])?)); continue; }
                _ => { warnings.push(format!("Unknown configuration key: :{key}")); continue; }
            };
            if converted != Yaml::Null || target == "server" { output.insert(Yaml::String(target.into()), converted); }
        }
        if !modules.is_empty() { output.insert("modules".into(), Yaml::Sequence(modules)); }
        Ok((Self::from_value(Yaml::Mapping(output), environment, false)?, warnings))
    }
}

fn funded_receiver_clock(value: &Edn) -> Result<Yaml> {
    let mut profile = fields(value, &[("installation-file", "installationFile"), ("domain", "domain"), ("placement-epoch", "placementEpoch"), ("purpose", "purpose")])?;
    if let Some(transport) = value.option("transport").filter(|value| !matches!(value.kind, EdnKind::Nil)) {
        insert(&mut profile, "transport", fields(transport, &[
            ("socket", "socket"), ("verification-key", "verificationKey"),
            ("evidence-sha256", "evidenceSha256"), ("timeout-millis", "timeoutMillis"),
        ])?)?;
    }
    Ok(profile)
}

#[derive(Clone, Copy)]
enum Kind { Log, Storage, ObjectStore, Cluster, Remote, Authentication }

fn backend(value: &Edn, kind: Kind) -> Result<Yaml> {
    let sequence = sequence(value)?;
    let tag = name(sequence.first().ok_or_else(|| invalid("Backend configuration requires a type"))?)?;
    let empty = Edn { span: value.span, kind: EdnKind::Nil };
    let options = sequence.get(1).unwrap_or(&empty);
    if sequence.len() > 2 { return Err(invalid("Backend configuration has more than a type and options")); }
    match (kind, tag.as_str()) {
        (Kind::Log, "in-memory" | "xtdb.log/in-memory") => Ok(tagged("!InMemory", fields(options, &[("epoch", "epoch"), ("replay-buffer-size", "replayBufferSize")])?)),
        (Kind::Log, "local" | "xtdb.log/local") => Ok(tagged("!Local", fields(options, &[("epoch", "epoch"), ("path", "path"), ("instant-source-for-non-tx-msgs?", "useInstantSourceForNonTx")])?)),
        (Kind::Log, "kafka" | "xtdb.kafka/kafka") => Ok(tagged("!Kafka", fields(options, &[("cluster", "cluster"), ("topic", "topic"), ("replica-cluster", "replicaCluster"), ("replica-topic", "replicaTopic"), ("epoch", "epoch"), ("create-topic?", "autoCreateTopic")])?)),
        (Kind::Storage, "in-memory") => Ok(tagged("!InMemory", fields(options, &[("epoch", "epoch")])?)),
        (Kind::Storage, "local") => Ok(tagged("!Local", fields(options, &[("path", "path"), ("epoch", "epoch")])?)),
        (Kind::Storage, "remote") => storage_remote(options),
        (Kind::Cluster, "kafka" | "xtdb.kafka/cluster") => Ok(tagged("!Kafka", fields(options, &[("bootstrap-servers", "bootstrapServers"), ("properties-map", "propertiesMap"), ("properties-file", "propertiesFile")])?)),
        (Kind::Remote, "postgres" | "xtdb.postgres/remote") => Ok(tagged("!Postgres", fields(options, &[("host", "hostname"), ("port", "port"), ("database", "database"), ("username", "username"), ("password", "password")])?)),
        (Kind::ObjectStore, "s3" | "xtdb.aws/s3") => {
            let mut result = fields(options, &[("bucket", "bucket"), ("prefix", "prefix"), ("endpoint", "endpoint"), ("path-style-access-enabled?", "pathStyleAccessEnabled")])?;
            if let Some(credentials) = options.option("credentials") { insert(&mut result, "credentials", fields(credentials, &[("access-key", "accessKey"), ("secret-key", "secretKey")])?)?; }
            Ok(tagged("!S3", result))
        }
        (Kind::ObjectStore, "azure" | "xtdb.azure/object-store") => Ok(tagged("!Azure", fields(options, &[("storage-account", "storageAccount"), ("container", "container"), ("prefix", "prefix"), ("user-managed-identity-client-id", "userManagedIdentityClientId"), ("storage-account-endpoint", "storageAccountEndpoint")])?)),
        (Kind::ObjectStore, "google-cloud" | "xtdb.gcp/object-store") => Ok(tagged("!GoogleCloud", fields(options, &[("project-id", "projectId"), ("bucket", "bucket"), ("prefix", "prefix")])?)),
        (Kind::Authentication, tag) => authentication(tag, options),
        _ => Err(invalid(format!("Unknown native EDN backend type :{tag}"))),
    }
}

fn storage_remote(value: &Edn) -> Result<Yaml> {
    let mut result = fields(value, &[("epoch", "epoch")])?;
    let store = value.option("object-store").ok_or_else(|| invalid("Remote storage requires an object store"))?;
    insert(&mut result, "objectStore", backend(store, Kind::ObjectStore)?)?;
    Ok(tagged("!Remote", result))
}
fn named_backends(value: &Edn, kind: Kind) -> Result<Yaml> {
    entries(value)?.iter().map(|(key, value)| Ok((Yaml::String(name(key)?), backend(value, kind)?))).collect::<Result<Mapping>>().map(Yaml::Mapping)
}
fn cache(value: &Edn, disk: bool) -> Result<Yaml> {
    let mut result = fields(value, &[("max-cache-bytes", "maxSizeBytes"), ("max-cache-ratio", "maxSizeRatio")])?;
    if disk { if let Some(path) = value.option("path") { insert(&mut result, "path", scalar(path)?)?; } }
    Ok(result)
}
fn server(value: &Edn) -> Result<Yaml> {
    if matches!(value.kind, EdnKind::Nil) { return Ok(Yaml::Null); }
    let mut result = host(fields(value, &[("host", "host"), ("port", "port"), ("read-only-port", "readOnlyPort"), ("num-threads", "numThreads")])?, value)?;
    if let Some(ssl) = value.option("ssl").filter(|value| !matches!(value.kind, EdnKind::Nil)) { insert(&mut result, "ssl", fields(ssl, &[("keystore", "keyStore"), ("keystore-password", "keyStorePassword")])?)?; }
    Ok(result)
}
fn host(mut output: Yaml, input: &Edn) -> Result<Yaml> {
    if input.option("host").is_some_and(|value| matches!(value.kind, EdnKind::Nil)) { insert(&mut output, "host", Yaml::Null)?; }
    Ok(output)
}
fn authentication(tag: &str, options: &Edn) -> Result<Yaml> {
    match tag {
        "single-root-user" | "xtdb.authn/single-root-user-authn" => {
            let mut result = fields(options, &[("password", "password")])?;
            if options.option("password").is_none_or(|value| matches!(value.kind, EdnKind::Nil)) { insert(&mut result, "password", Yaml::Null)?; }
            Ok(tagged("!SingleRootUser", result))
        }
        "user-list" | "xtdb.authn/user-list-authn" => {
            let users = options.option("users").ok_or_else(|| invalid("User-list authentication requires users"))?;
            let mut result = Yaml::Mapping(Mapping::new());
            let mut hashes = Mapping::new();
            for (user, hash) in entries(users)? {
                let pair = sequence(hash)?;
                if pair.len() != 2 { return Err(invalid("Password hash requires an algorithm and encoded hash")); }
                let tag = match name(&pair[0])?.as_str() { "argon2id" => "!Argon2id", "bcrypt" => "!BCrypt", _ => return Err(invalid("Unknown password hash algorithm")) };
                hashes.insert(Yaml::String(name(user)?), tagged(tag, scalar(&pair[1])?));
            }
            insert(&mut result, "users", Yaml::Mapping(hashes))?;
            if let Some(rules) = options.option("rules").filter(|value| !matches!(value.kind, EdnKind::Nil)) { insert(&mut result, "rules", rules_value(rules)?)?; }
            Ok(tagged("!UserList", result))
        }
        "openid-connect" | "xtdb.authn/openid-connect-authn" => {
            let mut result = fields(options, &[("issuer-url", "issuerUrl"), ("client-id", "clientId"), ("client-secret", "clientSecret")])?;
            insert(&mut result, "rules", options.option("rules").map(rules_value).transpose()?.unwrap_or_else(|| Yaml::Sequence(Vec::new())))?;
            Ok(tagged("!OpenIdConnect", result))
        }
        _ => Err(invalid("Unknown EDN authentication type")),
    }
}
fn rules_value(value: &Edn) -> Result<Yaml> {
    sequence(value)?.iter().map(|rule| {
        let mut result = fields(rule, &[("user", "user"), ("remote-addr", "remoteAddress")])?;
        let method = name(rule.option("method").ok_or_else(|| invalid("Authentication rule requires method"))?)?.replace('-', "_").to_ascii_uppercase();
        insert(&mut result, "method", Yaml::String(method))?;
        Ok(result)
    }).collect::<Result<Vec<_>>>().map(Yaml::Sequence)
}
fn durations(mut value: Yaml, fields: &[&str]) -> Result<Yaml> {
    let map = value.as_mapping_mut().ok_or_else(|| invalid("Expected duration settings map"))?;
    for field in fields {
        if let Some(value) = map.get_mut(Yaml::String((*field).into())) {
            if let Some(milliseconds) = value.as_i64() {
                if milliseconds < 0 { return Err(invalid("Duration milliseconds must be nonnegative")); }
                *value = Yaml::String(format!("PT{}.{:03}S", milliseconds / 1000, milliseconds % 1000));
            }
        }
    }
    Ok(value)
}
fn fields(value: &Edn, fields: &[(&str, &str)]) -> Result<Yaml> {
    let mut result = Mapping::new();
    entries(value)?;
    for (source, target) in fields {
        if let Some(value) = value.option(source).filter(|value| !matches!(value.kind, EdnKind::Nil)) { result.insert(Yaml::String((*target).into()), scalar(value)?); }
    }
    Ok(Yaml::Mapping(result))
}
fn insert(map: &mut Yaml, key: &str, value: Yaml) -> Result<()> { map.as_mapping_mut().ok_or_else(|| invalid("Expected configuration map"))?.insert(Yaml::String(key.into()), value); Ok(()) }
fn tagged(tag: &str, value: Yaml) -> Yaml { Yaml::Tagged(Box::new(TaggedValue { tag: Tag::new(tag), value })) }
fn entries(value: &Edn) -> Result<&[(Edn, Edn)]> { match &value.kind { EdnKind::Map(entries) => Ok(entries), EdnKind::Nil => Ok(&[]), _ => Err(invalid("Configuration requires an EDN map")) } }
fn sequence(value: &Edn) -> Result<&[Edn]> { match &value.kind { EdnKind::List(values) | EdnKind::Vector(values) | EdnKind::Set(values) => Ok(values), EdnKind::Nil => Ok(&[]), _ => Err(invalid("Configuration requires an EDN sequence")) } }
fn name(value: &Edn) -> Result<String> { match &value.kind { EdnKind::String(value) | EdnKind::Keyword(value) | EdnKind::Symbol(value) => Ok(value.clone()), _ => Err(invalid("Configuration key requires a string, keyword or symbol")) } }
fn scalar(value: &Edn) -> Result<Yaml> {
    match &value.kind {
        EdnKind::Nil => Ok(Yaml::Null), EdnKind::Bool(value) => Ok(Yaml::Bool(*value)), EdnKind::Integer(value) => Ok(Yaml::Number((*value).into())),
        EdnKind::Float(value) => serde_yaml::to_value(value).map_err(|error| invalid(error.to_string())),
        EdnKind::String(value) | EdnKind::Keyword(value) | EdnKind::Symbol(value) => Ok(Yaml::String(value.clone())),
        EdnKind::List(values) | EdnKind::Vector(values) | EdnKind::Set(values) => values.iter().map(scalar).collect::<Result<_>>().map(Yaml::Sequence),
        EdnKind::Map(values) => values.iter().map(|(key, value)| Ok((Yaml::String(name(key)?), scalar(value)?))).collect::<Result<Mapping>>().map(Yaml::Mapping),
        _ => Err(invalid("EDN configuration contains an unsupported data form")),
    }
}
fn expand_environment(value: &mut Edn, environment: &impl Fn(&str) -> Option<String>) -> Result<()> {
    match &mut value.kind {
        EdnKind::Tagged { tag, value: inner } if tag == "env" => {
            let variable = match &inner.kind { EdnKind::Keyword(value) => format!(":{value}"), _ => name(inner)? };
            value.kind = environment(&variable).map_or(EdnKind::Nil, EdnKind::String);
        }
        EdnKind::List(values) | EdnKind::Vector(values) | EdnKind::Set(values) => { for value in values { expand_environment(value, environment)?; } }
        EdnKind::Map(values) => { for (key, value) in values { expand_environment(key, environment)?; expand_environment(value, environment)?; } }
        _ => (),
    }
    Ok(())
}
