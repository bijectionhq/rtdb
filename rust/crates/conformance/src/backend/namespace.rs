use prost::Message;
use rtdb_format::{database::proto::database_config, generated::xtdb};
use rtdb_server::config::NodeConfiguration;
use serde_json::{Value, json};

use super::Result;

fn scoped_path(path: &str, namespace: &str) -> Result<()> {
    if path.contains('\\') || path.split('/').any(|part|matches!(part,"."|".."))
        || path.split('/').filter(|part|*part==namespace).count()!=1 {
        return Err("backend root lacks its exact isolated namespace component".into());
    }
    Ok(())
}
fn scoped_topic(topic: &str, namespace: &str) -> Result<()> {
    if !topic.strip_prefix(&format!("{namespace}-")).is_some_and(|suffix|!suffix.is_empty() && suffix.bytes().all(|byte|byte.is_ascii_alphanumeric()||byte==b'-'||byte==b'_')) {
        return Err("Kafka topic lacks its exact isolated namespace prefix".into());
    }
    Ok(())
}

pub(super) fn validate(config: &NodeConfiguration, namespace: &str, capability: &str) -> Result<Value> {
    if !namespace.starts_with("rtdb-recovery-") || !(20..=80).contains(&namespace.len())
        || !namespace.bytes().all(|byte|byte.is_ascii_lowercase()||byte.is_ascii_digit()||byte==b'-') {
        return Err("backend helper namespace must be an owned recovery invocation".into());
    }
    if config.fixed_databases.is_some() || config.primary.partitions.unwrap_or(1).max(1)!=1 {
        return Err("backend helper requires one explicit database partition".into());
    }
    let storage = match &config.primary.storage {
        Some(database_config::Storage::LocalStorage(value)) => {
            let path = value.path.as_deref().ok_or("local storage path absent")?;
            scoped_path(path,namespace)?;
            json!({"kind":"local","path":path,"epoch":value.epoch.unwrap_or_default()})
        }
        Some(database_config::Storage::RemoteStorage(value)) => {
            let provider = value.object_store.as_ref().ok_or("remote object-store configuration absent")?;
            let name = provider.type_url.rsplit('/').next().ok_or("object-store protobuf type absent")?;
            let mut identity = match name {
                "xtdb.aws.proto.S3ObjectStoreConfig" => {
                    let value = xtdb::aws::proto::S3ObjectStoreConfig::decode(provider.value.as_slice())?;
                    if value.credentials.is_some() { return Err("recovery storage credentials must use a node-local alias or credential chain".into()); }
                    json!({"kind":"s3","bucket":value.bucket,"prefix":value.prefix,"region":value.region,"endpoint":value.endpoint,"path_style":value.path_style_access_enabled,"remote":value.remote})
                }
                "xtdb.azure.proto.AzureBlobStorageConfig" => {
                    let value = xtdb::azure::proto::AzureBlobStorageConfig::decode(provider.value.as_slice())?;
                    if value.storage_account_key.is_some() { return Err("recovery Azure keys must use a node-local alias".into()); }
                    json!({"kind":"azure","account":value.storage_account,"container":value.container,"prefix":value.prefix,"endpoint":value.storage_account_endpoint,"remote":value.remote})
                }
                "xtdb.gcp.proto.GcsObjectStoreConfig" => {
                    let value = xtdb::gcp::proto::GcsObjectStoreConfig::decode(provider.value.as_slice())?;
                    json!({"kind":"gcs","project":value.project_id,"bucket":value.bucket,"prefix":value.prefix})
                }
                _=>return Err("recovery helper does not recognize this storage provider".into()),
            };
            scoped_path(identity["prefix"].as_str().ok_or("remote prefix absent")?,namespace)?;
            identity["epoch"] = json!(value.epoch.unwrap_or_default());
            identity
        }
        _=>return Err("recovery helper requires durable explicit storage".into()),
    };
    let log = match &config.primary.log {
        Some(database_config::Log::LocalLog(value))=> {
            let path = value.path.as_deref().ok_or("local log path absent")?;
            scoped_path(path,namespace)?;
            json!({"kind":"local","path":path,"epoch":value.epoch.unwrap_or_default()})
        }
        Some(database_config::Log::OtherLog(value)) if value.type_url.rsplit('/').next()==Some("xtdb.kafka.proto.KafkaLogConfig")=> {
            let value = xtdb::kafka::proto::KafkaLogConfig::decode(value.value.as_slice())?;
            let topic = value.topic.as_deref().ok_or("Kafka source topic absent")?;
            let replica = value.replica_topic.clone().unwrap_or_else(||format!("{topic}-replica"));
            scoped_topic(topic,namespace)?;
            scoped_topic(&replica,namespace)?;
            json!({"kind":"kafka","topic":topic,"replica_topic":replica,"cluster":value.log_cluster_alias,
                "replica_cluster":value.replica_cluster_alias,"epoch":value.epoch.unwrap_or_default()})
        }
        _=>return Err("recovery helper requires explicit durable logs".into()),
    };
    match capability {
        "s3"|"azure"|"gcs" if storage["kind"]==capability=>(),
        "kafka"|"kafka-connect" if log["kind"]=="kafka"=>(),
        "local"|"pg-source" if storage["kind"]=="local" && log["kind"]=="local"=>(),
        _=>return Err("resolved configuration does not exercise the selected backend capability".into()),
    }
    Ok(json!({"storage":storage,"log":log,"capability":capability,"partitions":1}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn namespace_components_do_not_admit_neighbors_or_dot_segments() {
        let namespace="rtdb-recovery-0123456789abcdef";
        assert!(scoped_path(&format!("/data/{namespace}/objects"),namespace).is_ok());
        assert!(scoped_path(&format!("/data/{namespace}-other/objects"),namespace).is_err());
        assert!(scoped_path(&format!("/data/{namespace}/../objects"),namespace).is_err());
        assert!(scoped_topic(&format!("{namespace}-source"),namespace).is_ok());
        assert!(scoped_topic(&format!("{namespace}other-source"),namespace).is_err());
    }
}
