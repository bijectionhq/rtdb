use std::sync::Arc;
use async_trait::async_trait;
use rtdb_format::{keys::TrieKey, log::proto::TriesAdded};
use rtdb_index::{PartitionState, PartitionValue};
use rtdb_types::{ReplicaPosition, Result, SourcePosition, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;

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

/// Selects the tables of retired relations that collection may now supersede, and publishes their retirement (gc.allium: RetireDueTables).
#[async_trait]
pub trait TableRetirements: Send + Sync {
    /// The tables whose relations' retention has passed and which nothing still reads, with the time they retire at; none when there are no such tables.
    async fn due(&self, cancellation: &CancellationToken) -> Result<Option<(Vec<TableRef>, Timestamp)>>;
    /// Completes once this term's installed replica prefix applies the retirement.
    async fn retire(&self, tables: Vec<TableRef>, as_of: Timestamp) -> Result<()>;
}
