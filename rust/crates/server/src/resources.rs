use std::sync::Arc;
use rtdb_columnar::MemoryBudget;
use rtdb_metrics::Registry;
use rtdb_node::{ExternalSources, NativeResources};
use rtdb_types::Result;
use rtdb_storage::disk_cache::DiskCache;

use crate::config::{NodeConfiguration, RemoteConfig};

/// Constructs backend factories without opening a node or starting any driver.
/// - Inputs: validated node configuration, shared allocation budget, and metric registry.
/// - Ownership: returned factories own their configuration and memory-budget references.
///   Retain returned metric registrations while their samples are required.
///   Close the resource factory after its consumers and drivers join, including failed startup.
/// - Errors: credential resolution and local disk-cache initialization can fail with a provider or configuration anomaly.
pub fn open_backend_resources(config: &NodeConfiguration, memory: &Arc<MemoryBudget>, metrics: &Arc<Registry>) -> Result<(NativeResources, ExternalSources, Vec<rtdb_metrics::MetricRegistration>)> {
    let mut resources = NativeResources::default().with_local_log_clock(Arc::new(rtdb_node::SystemClock), config.local_log_uses_clock);
    let classic_vault = std::env::var_os("RTDB_CLASSIC_KEY_VAULT")
        .map(|path| rtdb_storage::sealing::ClassicKeyVault::open(std::path::Path::new(&path))).transpose()?;
    if let Some(vault) = &classic_vault {
        resources = resources.with_classic_key_vault(vault.clone());
    }
    let mut sources = ExternalSources::new();
    let mut registrations = Vec::new();
    if let Some(settings) = &config.disk_cache {
        std::fs::create_dir_all(&settings.path).map_err(disk_error)?;
        if let Some(vault) = &classic_vault {
            vault.require_separate_path(&settings.path)?;
        }
        let capacity = match settings.maximum_bytes {
            Some(bytes) => bytes,
            None => (fs2::total_space(&settings.path).map_err(disk_error)? as f64 * settings.maximum_ratio) as u64,
        };
        let cache = DiskCache::open(&settings.path, capacity, Arc::clone(memory))?;
        registrations.push(metrics.register_disk_cache(Arc::clone(&cache))?);
        resources = resources.with_disk_cache(cache);
    }
    #[cfg(feature = "kafka")]
    {
        let clusters = config.remotes.iter().filter_map(|(name, remote)| match remote {
            RemoteConfig::Kafka { bootstrap_servers, properties, properties_file, schema_registry_url } => Some((name.clone(), rtdb_kafka::KafkaClusterConfig {
                bootstrap_servers: bootstrap_servers.clone(), properties_map: properties.iter().map(|(name, secret)| (name.clone(), secret.expose().to_owned())).collect(),
                properties_file: properties_file.clone(), schema_registry_url: schema_registry_url.clone(),
            })),
            _ => None,
        }).collect::<std::collections::BTreeMap<_, _>>();
        resources = resources.with_kafka_topic_creation(clusters.clone(), metrics.clone(), config.kafka_auto_create_topic, Arc::clone(memory))?;
        #[cfg(feature = "kafka-connect")]
        sources.insert("xtdb.kafka.connectsrc.proto.KafkaConnectSourceConfig".into(), Arc::new(rtdb_node::KafkaConnectSourceFactory::new(clusters, Arc::clone(memory), Arc::new(rtdb_node::SystemClock)).with_observer(metrics.clone())));
    }
    #[cfg(feature = "s3")]
    {
        let remotes = config.remotes.iter().filter_map(|(name, remote)| match remote {
            RemoteConfig::S3 { access_key, secret_key } => Some((name.clone(), rtdb_s3::S3Credentials { access_key: access_key.expose().into(), secret_key: secret_key.expose().into() })),
            _ => None,
        }).collect::<std::collections::BTreeMap<_, _>>();
        resources = resources.with_s3(Arc::new(remotes))?;
    }
    #[cfg(feature = "azure")]
    {
        let remotes = config.remotes.iter().filter_map(|(name, remote)| match remote {
            RemoteConfig::Azure { connection_string, storage_account_key } => Some((name.clone(), rtdb_azure::AzureCredentials {
                connection_string: connection_string.as_ref().map(|secret| secret.expose().into()), storage_account_key: storage_account_key.as_ref().map(|secret| secret.expose().into()),
            })),
            _ => None,
        }).collect::<std::collections::BTreeMap<_, _>>();
        resources = resources.with_azure(Arc::new(remotes))?;
    }
    #[cfg(feature = "gcs")]
    { resources = resources.with_gcs()?; }
    #[cfg(feature = "pg-source")]
    {
        let remotes = config.remotes.iter().filter_map(|(name, remote)| match remote {
            RemoteConfig::Postgres { hostname, port, database, username, password } => {
                let mut remote = rtdb_pg_source::Remote::new(hostname.clone(), database.clone(), username.clone(), password.expose().into());
                remote.port = *port;
                Some((name.clone(), remote))
            }
            _ => None,
        }).collect();
        sources.insert("xtdb.postgres.proto.PostgresSourceConfig".into(), Arc::new(rtdb_node::PostgresSourceFactory::new(remotes, Arc::clone(memory))?.with_metrics(metrics.clone())));
    }
    Ok((resources, sources, registrations))
}

pub(crate) use open_backend_resources as open;

fn disk_error(error: std::io::Error) -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.server/disk-cache", error.to_string())
}
