use std::{collections::BTreeMap, path::{Component, Path}};
use rtdb_columnar::MemoryBudget;
use rtdb_format::database::proto::database_config::{Log, Storage};
use rtdb_server::config::{ConfigurationEnvironment, EnvironmentValue, ParsedConfiguration};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

struct Environment(BTreeMap<String, String>);
impl ConfigurationEnvironment for Environment {
    fn lookup(&self, name: &str, memory: &std::sync::Arc<MemoryBudget>) -> rtdb_types::Result<Option<EnvironmentValue>> {
        self.0.get(name).map(|value|EnvironmentValue::copy(value,memory)).transpose()
    }
}
fn clean_absolute(value: &str) -> bool {
    value.starts_with('/') && !value.contains("//") && value.split('/').all(|part|part != "." && part != "..")
        && Path::new(value).components().all(|part|matches!(part,Component::RootDir | Component::Normal(_)))
}

/// Resolves local storage and log paths through the production bounded YAML decoder.
/// Inputs are retained configuration bytes and the exact explicit container environment.
/// The caller must keep runtime mounts outside `/measured-objects` and reject overlapping binds.
/// Returns digest-bound lexical paths; mount routing is verified separately in the running container.
/// Returns an error for a nonlocal database, an unmeasured storage root, or a log/cache path inside the mount.
/// No database, credential provider, or task is opened.
pub fn validate_configuration(text: &str, environment: BTreeMap<String,String>) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    if text.len()>1024*1024 || environment.len()>1024 || environment.iter().any(|(k,v)|k.len()>4096 || v.len()>65536) { return Err("configuration validation input limit exceeded".into()); }
    let memory=MemoryBudget::new(256*1024*1024);
    let environment_bytes=serde_json::to_vec(&environment)?;
    let parsed=ParsedConfiguration::from_yaml(text,&Environment(environment),&memory,false)?;
    let config=parsed.configuration();
    if config.fixed_databases.is_some() || config.playground || config.disk_cache.is_some() || !config.remotes.is_empty() {
        return Err("filesystem performance requires ordinary local-only node configuration without remote aliases or disk cache".into());
    }
    let root=Path::new("/measured-objects");
    let mut databases=Vec::new();
    for (name,database) in [("xtdb",&config.primary)] {
        let Some(Storage::LocalStorage(storage))=database.storage.as_ref() else { return Err("every measured database must use local storage".into()); };
        let Some(Log::LocalLog(log))=database.log.as_ref() else { return Err("filesystem performance requires local logs outside object storage".into()); };
        let storage_path=storage.path.as_deref().unwrap_or_default();
        let log_path=log.path.as_deref().unwrap_or_default();
        if !clean_absolute(storage_path) || !Path::new(storage_path).starts_with(root)
            || !clean_absolute(log_path) || Path::new(log_path).starts_with(root) || root.starts_with(log_path)
            || database.external_source.is_some() {
            return Err("database storage/log/source configuration escapes the measured local boundary".into());
        }
        databases.push(json!({"name":name,"storage_path":storage_path,"storage_epoch":storage.epoch.unwrap_or(0),"log_path":log_path}));
    }
    Ok(json!({"schema_version":1,"configuration_sha256":format!("{:x}",Sha256::digest(text.as_bytes())),
        "environment_sha256":format!("{:x}",Sha256::digest(environment_bytes)),"mount_target":"/measured-objects","databases":databases,
        "attached_databases_allowed":false,"runtime_configuration_changes_allowed":false}))
}
