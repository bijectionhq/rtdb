use std::sync::Arc;
use async_trait::async_trait;
use rtdb_format::{keys::TrieKey, log::proto::TriesAdded};
use rtdb_index::{PartitionState, PartitionValue};
use rtdb_types::{ReplicaPosition, Result, SourcePosition, TableRef};

pub trait SnapshotSource: Send + Sync {
    fn snapshot(&self) -> Arc<PartitionValue>;
}

impl SnapshotSource for PartitionState {
    fn snapshot(&self) -> Arc<PartitionValue> { self.value() }
}

#[async_trait]
pub trait CompactionPublisher: Send + Sync {
    /// Completes after the source record is durably appended and admitted processing has applied it.
    async fn publish(&self, notification: TriesAdded) -> Result<SourcePosition>;
}

#[async_trait]
pub trait TrieDeletionPublisher: Send + Sync {
    /// Confirms the removal from this term's installed replica prefix and checkpoint view.
    /// Term teardown must fail both queued and appended requests that have not completed.
    async fn publish(&self, table: TableRef, keys: Vec<TrieKey>) -> Result<ReplicaPosition>;
}
