//! What a node opens for a deployment: the `DatabaseConfig` derived from its directory record.

use std::path::PathBuf;

use rtdb_directory::DeploymentRecord;
use rtdb_format::database::proto::{DatabaseConfig, RemoteStorage, database_config};
use serde::{Deserialize, Serialize};

/// The protobuf type name the logkeeper log factory is registered under.
pub const LOGKEEPER_LOG_TYPE: &str = "rtdb.multi.LogkeeperLog";

/// Where a cell keeps its deployments' objects.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CellStorage {
    /// A directory every node of the cell can reach, such as a shared volume.
    Local { root: PathBuf },
}

/// How a node holds a deployment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// The writer, under the record's epoch.
    Holder,
    /// A log-fed reader that never writes.
    Subscriber,
    /// A log-fed reader that compacts, writing only compaction outputs and publishing them through the holder.
    Compactor,
}

/// The logkeeper streams a deployment's database reads and writes, carried in its `DatabaseConfig`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogkeeperLogConfig {
    pub servers: Vec<String>,
    /// The deployment whose key seals the stream's payloads.
    #[serde(default)]
    pub deployment: String,
    pub replica_stream: String,
    /// The ensemble the stream moves from, when the holder opening it moves it to `servers`.
    #[serde(default)]
    pub previous_servers: Option<Vec<String>>,
    /// The members `servers` are, recorded as the deployment's ensemble once a move completes.
    #[serde(default)]
    pub ensemble: Vec<String>,
    /// The writer epoch a holder opens its streams at; ignored by a subscriber.
    pub epoch: u64,
    /// The namespace of the deployment's log positions.
    #[serde(default)]
    pub log_epoch: i32,
    pub write_quorum: usize,
}

pub fn write_quorum(ensemble: usize) -> usize { ensemble / 2 + 1 }

/// The configuration a node opens `record` with, given its ensemble's addresses.
pub fn database_config(record: &DeploymentRecord, servers: Vec<String>, storage: &CellStorage, role: Role) -> DatabaseConfig {
    moving_config(record, servers, None, storage, role)
}

/// The configuration a holder opens `record` with when it moves the stream from `previous` to `servers`, the ensemble `ensemble`.
pub fn moving_config(record: &DeploymentRecord, servers: Vec<String>, moving: Option<(Vec<String>, Vec<rtdb_directory::MemberId>)>, storage: &CellStorage, role: Role) -> DatabaseConfig {
    let mut log = LogkeeperLogConfig { write_quorum: write_quorum(servers.len()), servers, deployment: record.id.clone(), replica_stream: record.stream("replica"), epoch: record.epoch, log_epoch: record.log_epoch, previous_servers: None, ensemble: record.ensemble.iter().map(|id| id.0.clone()).collect() };
    if let Some((previous, ensemble)) = moving { log.previous_servers = Some(previous); log.ensemble = ensemble.into_iter().map(|id| id.0).collect(); }
    let storage = match storage {
        CellStorage::Local { root } => {
            let store = crate::store::CellStoreConfig { root: root.clone(), prefix: record.storage_prefix(), deployment: record.id.clone(),
                writer_epoch: (role == Role::Holder).then_some(record.epoch), compactor: role == Role::Compactor };
            database_config::Storage::RemoteStorage(RemoteStorage {
                object_store: Some(prost_types::Any { type_url: format!("type.googleapis.com/{}", crate::store::CELL_STORAGE_TYPE), value: serde_json::to_vec(&store).expect("a store configuration serializes") }),
                epoch: Some(0),
            })
        }
    };
    DatabaseConfig {
        log: Some(database_config::Log::OtherLog(prost_types::Any {
            type_url: format!("type.googleapis.com/{LOGKEEPER_LOG_TYPE}"),
            value: serde_json::to_vec(&log).expect("a log configuration serializes"),
        })),
        storage: Some(storage),
        mode: match role { Role::Holder => None, Role::Subscriber | Role::Compactor => Some(1) },
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_holder_opens_its_streams_at_the_record_epoch_under_its_own_prefix() {
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.epoch = 4;
        let config = database_config(&record, vec!["a".into(), "b".into(), "c".into()], &CellStorage::Local { root: "/cell".into() }, Role::Holder);
        let Some(database_config::Log::OtherLog(any)) = &config.log else { panic!("a logkeeper log") };
        let log: LogkeeperLogConfig = serde_json::from_slice(&any.value).unwrap();
        assert_eq!((log.epoch, log.write_quorum, log.replica_stream.as_str()), (4, 2, format!("{}/1/replica", record.id).as_str()));
        let Some(database_config::Storage::RemoteStorage(remote)) = &config.storage else { panic!("cell storage") };
        let store: crate::store::CellStoreConfig = serde_json::from_slice(&remote.object_store.as_ref().unwrap().value).unwrap();
        assert!(store.prefix.ends_with(&format!("{}/1", record.id)) && store.writer_epoch == Some(4));
        assert_eq!(config.mode, None);
        assert_eq!(database_config(&record, vec![], &CellStorage::Local { root: "/cell".into() }, Role::Subscriber).mode, Some(1), "a subscriber is read-only");
    }
}
