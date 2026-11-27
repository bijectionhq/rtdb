#[cfg(any(feature = "kafka", feature = "s3", feature = "azure", feature = "gcs"))]
use std::sync::Arc;
#[cfg(any(feature = "kafka", feature = "s3", feature = "azure", feature = "gcs"))]
use async_trait::async_trait;
#[cfg(any(feature = "kafka", feature = "s3", feature = "azure", feature = "gcs"))]
use prost::Message;
#[cfg(any(feature = "kafka", feature = "s3", feature = "azure", feature = "gcs"))]
use prost_types::Any;
#[cfg(any(feature = "kafka", feature = "s3", feature = "azure", feature = "gcs"))]
use rtdb_types::{Anomaly, Result};
#[cfg(any(feature = "s3", feature = "azure", feature = "gcs"))]
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore};
#[cfg(any(feature = "s3", feature = "azure", feature = "gcs"))]
use crate::RemoteStorageFactory;
use crate::NativeResources;

impl NativeResources {
    #[cfg(feature = "kafka")]
    /// Registers Kafka log resources with automatic topic creation and the node's TLS allocation budget.
    /// Opens no clients and returns an error if the Kafka log configuration type is already registered.
    pub fn with_kafka(self, clusters: std::collections::BTreeMap<String, rtdb_kafka::KafkaClusterConfig>, observer: Arc<dyn rtdb_kafka::KafkaObserver>, memory: Arc<rtdb_columnar::MemoryBudget>) -> Result<Self> {
        self.with_kafka_topic_creation(clusters, observer, true, memory)
    }
    #[cfg(feature = "kafka")]
    /// Registers Kafka resources with the supplied topic-creation policy and node allocation budget.
    /// Opens no clients and returns an error if the Kafka log configuration type is already registered.
    pub fn with_kafka_topic_creation(self, clusters: std::collections::BTreeMap<String, rtdb_kafka::KafkaClusterConfig>, observer: Arc<dyn rtdb_kafka::KafkaObserver>, auto_create_topic: bool, memory: Arc<rtdb_columnar::MemoryBudget>) -> Result<Self> {
        let vault = self.classic_vault.clone();
        self.register_log("xtdb.kafka.proto.KafkaLogConfig", Arc::new(KafkaFactory { clusters, observer, auto_create_topic, memory, vault }))
    }
    #[cfg(feature = "s3")]
    pub fn with_s3(self, remotes: Arc<dyn rtdb_s3::S3CredentialResolver>) -> Result<Self> {
        self.register_storage("xtdb.aws.proto.S3ObjectStoreConfig", Arc::new(S3Factory { remotes }))
    }
    #[cfg(feature = "azure")]
    pub fn with_azure(self, remotes: Arc<dyn rtdb_azure::AzureCredentialResolver>) -> Result<Self> {
        self.register_storage("xtdb.azure.proto.AzureBlobStorageConfig", Arc::new(AzureFactory { remotes }))
    }
    #[cfg(feature = "gcs")]
    pub fn with_gcs(self) -> Result<Self> { self.register_storage("xtdb.gcp.proto.GcsObjectStoreConfig", Arc::new(GcsFactory)) }
}

#[cfg(feature = "s3")]
struct S3Factory { remotes: Arc<dyn rtdb_s3::S3CredentialResolver> }
#[cfg(feature = "s3")]
#[async_trait]
impl RemoteStorageFactory for S3Factory {
    fn uses_classic_custody(&self) -> bool { true }

    fn storage_location(&self, configuration: &Any, storage_root: &ObjectKey) -> Result<Option<String>> {
        let wire = rtdb_format::generated::xtdb::aws::proto::S3ObjectStoreConfig::decode(configuration.value.as_slice()).map_err(invalid)?;
        rtdb_s3::S3Config::from_proto(&wire).storage_location(storage_root).map(Some)
    }

    async fn open(&self, configuration: &Any, storage_root: &ObjectKey, io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        let wire = rtdb_format::generated::xtdb::aws::proto::S3ObjectStoreConfig::decode(configuration.value.as_slice()).map_err(invalid)?;
        rtdb_s3::S3Config::from_proto(&wire).open(storage_root, self.remotes.as_ref(), io).await
    }
}

#[cfg(feature = "azure")]
struct AzureFactory { remotes: Arc<dyn rtdb_azure::AzureCredentialResolver> }
#[cfg(feature = "azure")]
#[async_trait]
impl RemoteStorageFactory for AzureFactory {
    fn uses_classic_custody(&self) -> bool { true }

    fn storage_location(&self, configuration: &Any, storage_root: &ObjectKey) -> Result<Option<String>> {
        let wire = rtdb_format::generated::xtdb::azure::proto::AzureBlobStorageConfig::decode(configuration.value.as_slice()).map_err(invalid)?;
        rtdb_azure::AzureConfig::from_proto(&wire).storage_location(storage_root, self.remotes.as_ref()).map(Some)
    }

    async fn open(&self, configuration: &Any, storage_root: &ObjectKey, io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        let wire = rtdb_format::generated::xtdb::azure::proto::AzureBlobStorageConfig::decode(configuration.value.as_slice()).map_err(invalid)?;
        rtdb_azure::AzureConfig::from_proto(&wire).open(storage_root, self.remotes.as_ref(), io).await
    }
}

#[cfg(feature = "gcs")]
struct GcsFactory;
#[cfg(feature = "gcs")]
#[async_trait]
impl RemoteStorageFactory for GcsFactory {
    fn uses_classic_custody(&self) -> bool { true }

    fn storage_location(&self, configuration: &Any, storage_root: &ObjectKey) -> Result<Option<String>> {
        let wire = rtdb_format::generated::xtdb::gcp::proto::GcsObjectStoreConfig::decode(configuration.value.as_slice()).map_err(invalid)?;
        rtdb_gcs::GcsConfig::from_proto(&wire).storage_location(storage_root).map(Some)
    }

    async fn open(&self, configuration: &Any, storage_root: &ObjectKey, io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        let wire = rtdb_format::generated::xtdb::gcp::proto::GcsObjectStoreConfig::decode(configuration.value.as_slice()).map_err(invalid)?;
        rtdb_gcs::GcsConfig::from_proto(&wire).open(storage_root, io).await
    }
}

#[cfg(feature = "kafka")]
struct KafkaFactory { clusters: std::collections::BTreeMap<String, rtdb_kafka::KafkaClusterConfig>, observer: Arc<dyn rtdb_kafka::KafkaObserver>, auto_create_topic: bool, memory: Arc<rtdb_columnar::MemoryBudget>, vault: Option<rtdb_storage::sealing::ClassicKeyVault> }
#[cfg(feature = "kafka")]
#[async_trait]
impl crate::LogFactory for KafkaFactory {
    async fn open(&self, configuration: &Any, access: crate::LogAccess, cancellation: tokio_util::sync::CancellationToken) -> Result<crate::OpenedLogs> {
        self.open_pair(configuration, access, cancellation, None).await
    }

    async fn open_for_default_erasure(&self, configuration: &Any, access: crate::LogAccess, cancellation: tokio_util::sync::CancellationToken,
        owner: uuid::Uuid, topics: Option<[uuid::Uuid; 2]>,
    ) -> Result<crate::OpenedLogs> {
        if access.source_read_only() || access.replica_read_only() {
            return Err(invalid("Default Kafka erasure requires the original writable attachment"));
        }
        self.open_pair(configuration, access, cancellation, Some((owner, topics))).await
    }
}

#[cfg(feature = "kafka")]
impl KafkaFactory {
    async fn open_pair(&self, configuration: &Any, access: crate::LogAccess, cancellation: tokio_util::sync::CancellationToken,
        erasure: Option<(uuid::Uuid, Option<[uuid::Uuid; 2]>)>,
    ) -> Result<crate::OpenedLogs> {
        use futures::FutureExt;
        use rtdb_kafka::{KafkaLog, KafkaLogConfig, KafkaLogOptions};
        let wire = rtdb_format::generated::xtdb::kafka::proto::KafkaLogConfig::decode(configuration.value.as_slice()).map_err(invalid)?;
        let configuration = KafkaLogConfig::from_proto(&wire);
        let source_cluster = self.clusters.get(&configuration.cluster).ok_or_else(|| invalid(format!("No Kafka cluster configured with alias '{}'", configuration.cluster)))?;
        let replica_cluster = self.clusters.get(configuration.replica_cluster()).ok_or_else(|| invalid(format!("No Kafka cluster configured with alias '{}'", configuration.replica_cluster())))?;
        if configuration.cluster == configuration.replica_cluster() && configuration.topic == configuration.replica_topic() {
            return Err(invalid("Source and replica Kafka logs must use distinct topics or clusters"));
        }
        let mut source_options = KafkaLogOptions::new(&configuration.topic);
        source_options.read_only = access.source_read_only();
        source_options.auto_create_topic = self.auto_create_topic;
        let mut replica_options = KafkaLogOptions::new(configuration.replica_topic());
        replica_options.read_only = access.replica_read_only();
        replica_options.auto_create_topic = self.auto_create_topic;
        replica_options.pipelined = true;
        let vault = self.vault.clone().ok_or_else(|| invalid("Classic Kafka logs require RTDB_CLASSIC_KEY_VAULT"))?;
        let (source, replica) = if let Some((owner, topics)) = erasure {
            let source = KafkaLog::open_classic_erasure(source_cluster, source_options, Arc::clone(&self.observer), Arc::clone(&self.memory),
                vault.clone(), false, owner.into_bytes(), topics.map(|topics| topics[0].into_bytes())).await?;
            let replica = KafkaLog::open_classic_erasure(replica_cluster, replica_options, Arc::clone(&self.observer), Arc::clone(&self.memory),
                vault, true, owner.into_bytes(), topics.map(|topics| topics[1].into_bytes())).await?;
            (source, replica)
        } else {
            let source = KafkaLog::open_classic(source_cluster, source_options, Arc::clone(&self.observer), Arc::clone(&self.memory), vault.clone()).await?;
            let replica = KafkaLog::open_classic_replica(replica_cluster, replica_options, Arc::clone(&self.observer), Arc::clone(&self.memory), vault).await?;
            (source, replica)
        };
        if source.log.topic_id() == replica.log.topic_id() {
            return Err(invalid("Source and replica Kafka logs resolve to the same physical topic"));
        }

        let mut drivers = Vec::new();
        for (log, writable) in [(&source.log, !access.source_read_only()), (&replica.log, !access.replica_read_only())] {
            if !writable || erasure.is_some() { continue; }
            let log = Arc::clone(log);
            let token = cancellation.clone();
            drivers.push(async move {
                loop {
                    if token.is_cancelled() { return Ok(()); }
                    let delay = match log.settle_classic_prefix().await {
                        Ok(true) => 30,
                        Ok(false) => 1,
                        Err(error) => {
                            tracing::warn!("Kafka prefix custody settlement failed: {}", error.code);
                            30
                        }
                    };
                    tokio::select! {
                        _ = token.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(std::time::Duration::from_secs(delay)) => (),
                    }
                }
            }.boxed());
        }
        for driver in source.driver.into_iter().chain(replica.driver) {
            let token = cancellation.clone();
            drivers.push(async move { driver.run(token).await.map_err(Into::into) }.boxed());
        }
        let epoch = rtdb_types::StorageEpoch(configuration.epoch);
        Ok(crate::OpenedLogs { source: source.log, replica: replica.log, source_epoch: epoch, replica_epoch: epoch,
            replica_read_index: None, placed_transfer: None, await_authority: crate::AwaitAuthority::SourcePosition, drivers })
    }
}

#[cfg(any(feature = "kafka", feature = "s3", feature = "azure", feature = "gcs"))]
fn invalid(message: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("xtdb.db-catalog/invalid-db-config", message.to_string()) }

#[cfg(all(test, feature = "kafka"))]
mod kafka_tests {
    use super::*;
    use crate::LogFactory;
    struct Observer;
    impl rtdb_kafka::KafkaObserver for Observer { fn warning(&self, _: rtdb_kafka::KafkaWarning) {} }

    #[tokio::test]
    async fn kafka_log_factory_admits_tls_against_the_supplied_node_budget() {
        let memory = rtdb_columnar::MemoryBudget::new(0);
        let mut cluster = rtdb_kafka::KafkaClusterConfig::new("unused.invalid:9092");
        cluster.properties_map.insert("security.protocol".into(), "SSL".into());
        let directory = tempfile::tempdir().unwrap();
        let vault = rtdb_storage::sealing::ClassicKeyVault::open(directory.path()).unwrap();
        let factory = KafkaFactory { clusters: std::collections::BTreeMap::from([("remote".into(), cluster)]), observer: Arc::new(Observer), auto_create_topic: false, memory: Arc::clone(&memory), vault: Some(vault) };
        let configuration = Any { type_url: "xtdb.kafka.proto.KafkaLogConfig".into(), value: rtdb_kafka::KafkaLogConfig::new("remote", "source").to_proto().encode_to_vec() };
        let result = factory.open(&configuration, crate::LogAccess::ReadWrite, tokio_util::sync::CancellationToken::new()).await;
        assert_eq!(result.err().unwrap().code, "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), 0);
    }
}
