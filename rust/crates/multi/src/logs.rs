//! The log factory a node opens a deployment's logkeeper streams through.
//!
//! A holder opens both streams as their writer at its epoch, which seals them: from then on no earlier holder's append is accepted.
//! A subscriber opens them as a reader.

use std::sync::Arc;

use async_trait::async_trait;
use prost_types::Any;
use rtdb_logkeeper::StreamConfig;
use rtdb_node::{LogAccess, LogFactory, OpenedLogs};
use rtdb_types::{Anomaly, Result, StorageEpoch};
use tokio_util::sync::CancellationToken;

use crate::deployment::LogkeeperLogConfig;

pub struct LogkeeperLogFactory { pub maximum_message_bytes: usize }

impl LogkeeperLogFactory {
    pub fn new(maximum_message_bytes: usize) -> Arc<Self> { Arc::new(Self { maximum_message_bytes }) }

    fn stream(&self, config: &LogkeeperLogConfig, stream: &str) -> StreamConfig {
        StreamConfig { servers: config.servers.clone(), stream: stream.to_owned(), write_quorum: config.write_quorum, maximum_message_bytes: self.maximum_message_bytes }
    }
}

#[async_trait]
impl LogFactory for LogkeeperLogFactory {
    async fn open(&self, configuration: &Any, access: LogAccess, cancellation: CancellationToken) -> Result<OpenedLogs> {
        let config: LogkeeperLogConfig = serde_json::from_slice(&configuration.value)
            .map_err(|error| Anomaly::incorrect("rtdb.multi/log-config", format!("Logkeeper log configuration is malformed: {error}")))?;
        let source = self.stream(&config, &config.source_stream);
        let replica = self.stream(&config, &config.replica_stream);
        let (source, replica): (Arc<dyn rtdb_storage::log::Log>, Arc<dyn rtdb_storage::log::Log>) = match access {
            LogAccess::ReadWrite => (
                rtdb_logkeeper::open_writer(source, config.epoch, cancellation.clone()).await?,
                rtdb_logkeeper::open_writer(replica, config.epoch, cancellation).await?,
            ),
            LogAccess::ReadOnly => (rtdb_logkeeper::open_reader(source, cancellation.clone()).await?, rtdb_logkeeper::open_reader(replica, cancellation).await?),
            LogAccess::SourceWriter => return Err(Anomaly::unsupported("rtdb.multi/source-writer",
                "A deployment's streams have one writer, its holder; submissions reach it through the gateway")),
        };
        Ok(OpenedLogs { source, replica, source_epoch: StorageEpoch(0), replica_epoch: StorageEpoch(0), drivers: Vec::new() })
    }
}
