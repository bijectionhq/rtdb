use rtdb_format::config::Yaml;
use crate::QualificationReport;

#[derive(Clone, Copy)]
enum Shape { Node, Ingest, Database, Log, Storage, Store, Remote, Auth, Module, Source, Indexer, Fields(&'static [&'static str]), NamedDatabase, NamedRemote, Rules, Users, Opaque }

pub(crate) fn inspect(value: &Yaml, ingest: bool, report: &mut QualificationReport) {
    walk(value, if ingest { Shape::Ingest } else { Shape::Node }, "$", report);
}
fn walk(value: &Yaml, shape: Shape, path: &str, report: &mut QualificationReport) {
    if path.len() > 1024 { report.gap("inventory-path-limit", "$", "A configuration path exceeds the offline inventory limit."); return; }
    if matches!(value, Yaml::Null) { return; }
    if let Yaml::Sequence(values) = value {
        for (index, value) in values.iter().enumerate() { walk(value, shape, &format!("{path}[{index}]"), report); }
        return;
    }
    let (value, shape) = if let Yaml::Tagged(tagged) = value {
        let tag = tagged.tag.to_string();
        let fields: Option<&'static [&'static str]> = match (shape, tag.as_str()) {
            (Shape::Log, "!InMemory") => Some(&["epoch", "replayBufferSize"]),
            (Shape::Log, "!Local") => Some(&["epoch", "path", "useInstantSourceForNonTx"]),
            (Shape::Log, "!Kafka") => Some(&["epoch", "cluster", "topic", "replicaCluster", "replicaTopic", "autoCreateTopic"]),
            (Shape::Storage, "!InMemory") => Some(&["epoch"]),
            (Shape::Storage, "!Local") => Some(&["epoch", "path"]),
            (Shape::Storage, "!Remote") => Some(&["epoch", "objectStore"]),
            (Shape::Store, "!S3") => Some(&["bucket", "region", "prefix", "endpoint", "pathStyleAccessEnabled", "remote", "credentials"]),
            (Shape::Store, "!Azure") => Some(&["storageAccount", "container", "prefix", "storageAccountKey", "userManagedIdentityClientId", "storageAccountEndpoint", "remote"]),
            (Shape::Store, "!GoogleCloud") => Some(&["projectId", "bucket", "prefix"]),
            (Shape::Remote, "!Kafka") => Some(&["bootstrapServers", "propertiesMap", "propertiesFile", "schemaRegistryUrl"]),
            (Shape::Remote, "!S3") => Some(&["accessKey", "secretKey"]),
            (Shape::Remote, "!AzureBlob") => Some(&["connectionString", "storageAccountKey"]),
            (Shape::Remote, "!Postgres") => Some(&["hostname", "port", "database", "username", "password"]),
            (Shape::Auth, "!SingleRootUser") => Some(&["password"]),
            (Shape::Auth, "!UserList") => Some(&["users", "rules"]),
            (Shape::Auth, "!OpenIdConnect") => Some(&["issuerUrl", "clientId", "clientSecret", "rules"]),
            (Shape::Module, "!CloudWatch") => Some(&["namespace"]),
            (Shape::Module, "!AzureMonitor") => Some(&["connectionString"]),
            (Shape::Source, "!Postgres") => Some(&["remote", "slotName", "publicationName", "indexer", "ignoreTables"]),
            (Shape::Source, "!KafkaConnect") => Some(&["remote", "topic", "connectConfig", "indexer"]),
            (Shape::Indexer, "!DirectMirror") => Some(&[]),
            (Shape::Indexer, "!Docs") => Some(&["table"]),
            (Shape::Users, "!Argon2id" | "!BCrypt") => return,
            _ => None,
        };
        match fields {
            Some(fields) => (&tagged.value, Shape::Fields(fields)),
            None => { report.gap("unsupported-tag-or-object-hook", path, "This configuration tag or JVM object hook has no native data-only adapter at this path."); return; }
        }
    } else { (value, shape) };
    let allowed: &[&str] = match shape {
        Shape::Node => &["log", "storage", "logClusters", "remotes", "memoryCache", "diskCache", "healthz", "indexer", "nodeId", "server", "flightSql", "defaultTz", "compactor", "garbageCollector", "search", "authn", "tracer", "modules", "readOnlyDatabases", "skipDbs"],
        Shape::Ingest => &["databases", "logClusters", "remotes", "memoryCache", "diskCache", "healthz", "indexer", "nodeId"],
        Shape::Database => &["log", "storage", "mode", "externalSource", "critical", "partitions"],
        Shape::Fields(fields) => fields,
        Shape::NamedDatabase | Shape::NamedRemote | Shape::Users | Shape::Opaque => &[],
        Shape::Rules => &["method", "user", "remoteAddress"],
        _ => { report.gap("missing-provider-tag", path, "This setting requires an explicit supported native provider tag."); return; }
    };
    if let Yaml::Mapping(values) = value {
        for (key, value) in values {
            let Some(key) = key.as_str() else { report.gap("invalid-option-key", path, "Configuration option keys must be strings."); continue; };
            let next = format!("{path}.{key}");
            let child = match shape {
                Shape::NamedDatabase => Shape::Database,
                Shape::NamedRemote => Shape::Remote,
                Shape::Users => Shape::Users,
                Shape::Opaque => Shape::Opaque,
                _ if !allowed.contains(&key) => { report.gap("unsupported-option", &next, "This option is not accepted by the native production configuration surface."); continue; }
                _ => match key {
                    "databases" => Shape::NamedDatabase,
                    "logClusters" | "remotes" => Shape::NamedRemote,
                    "log" => Shape::Log,
                    "storage" => Shape::Storage,
                    "objectStore" => Shape::Store,
                    "authn" => Shape::Auth,
                    "modules" => Shape::Module,
                    "externalSource" => Shape::Source,
                    "indexer" if matches!(shape, Shape::Node | Shape::Ingest) => Shape::Fields(&["rowsPerBlock", "pageLimit", "logLimit", "flushDuration", "skipTxs", "enabled"]),
                    "indexer" => Shape::Indexer,
                    "credentials" => Shape::Fields(&["accessKey", "secretKey"]),
                    "server" => Shape::Fields(&["host", "port", "readOnlyPort", "numThreads", "ssl"]),
                    "ssl" => Shape::Fields(&["keyStore", "keyStorePassword"]),
                    "flightSql" => Shape::Fields(&["host", "port", "transactionIdleTimeout", "preparedStatementIdleTimeout", "sessionIdleTimeout", "sessionUnclaimedTimeout"]),
                    "healthz" => Shape::Fields(&["host", "port"]),
                    "memoryCache" => Shape::Fields(&["maxSizeBytes", "maxSizeRatio"]),
                    "diskCache" => Shape::Fields(&["path", "maxSizeBytes", "maxSizeRatio"]),
                    "compactor" => Shape::Fields(&["threads", "reeIids", "withinFileRecencyBranching"]),
                    "garbageCollector" => Shape::Fields(&["enabled", "blocksToKeep", "garbageLifetime", "readerLease", "readerLeaseMargin"]),
                    "search" => Shape::Fields(&["enabled", "threads"]),
                    "tracer" => Shape::Fields(&["enabled", "endpoint", "serviceName", "queryTracing", "transactionTracing"]),
                    "users" => Shape::Users,
                    "rules" => Shape::Rules,
                    _ => Shape::Opaque,
                },
            };
            if matches!(key, "propertiesMap" | "connectConfig") { property_keys(value, key == "connectConfig", &next, report); }
            walk(value, child, &next, report);
        }
    } else if let Yaml::Sequence(values) = value {
        for (index, value) in values.iter().enumerate() { walk(value, shape, &format!("{path}[{index}]"), report); }
    }
}
fn property_keys(value: &Yaml, connect: bool, path: &str, report: &mut QualificationReport) {
    let Some(values) = value.as_mapping() else { return; };
    for (key, value) in values {
        let Some(key) = key.as_str() else { continue; };
        let path = format!("{path}.{key}");
        if key.ends_with(".class") || key.contains("callback.handler") || key == "plugin.path" || key == "interceptor.classes" {
            report.gap("jvm-property-hook", path, "This JVM class or plugin property requires a native adapter.");
        } else if connect && matches!(key, "key.converter" | "value.converter") {
            let supported = matches!(value.as_str(), Some("org.apache.kafka.connect.json.JsonConverter" | "org.apache.kafka.connect.storage.StringConverter" | "org.apache.kafka.connect.converters.ByteArrayConverter" | "io.confluent.connect.avro.AvroConverter" | "io.confluent.connect.json.JsonSchemaConverter" | "org.apache.kafka.connect.converters.BooleanConverter" | "org.apache.kafka.connect.converters.ShortConverter" | "org.apache.kafka.connect.converters.IntegerConverter" | "org.apache.kafka.connect.converters.LongConverter" | "org.apache.kafka.connect.converters.FloatConverter" | "org.apache.kafka.connect.converters.DoubleConverter"));
            if !supported { report.gap("unsupported-connect-converter", path, "This converter has no native Kafka Connect implementation."); }
        } else if connect {
            report.gap("connect-plugin-options-unqualified", path, "This converter, transform or predicate option requires native factory validation; offline qualification does not construct plugin clients.");
        }
    }
}
