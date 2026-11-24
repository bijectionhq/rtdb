use async_trait::async_trait;
use rtdb_format::{data::EventBatch, log::proto::EntityDirectoryRoot};
use rtdb_index::Snapshot;
use rtdb_storage::cache::BufferPool;
use rtdb_types::{
    BlockIndex, EventHeader, EventOrigin, MemoryBudget, ReplicaPosition, Result, TableRef,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct HistoryRead<'a> {
    pub snapshot: &'a Snapshot,
    pub storage: &'a Arc<BufferPool>,
    pub memory: &'a Arc<MemoryBudget>,
    pub cancellation: &'a CancellationToken,
    pub table: &'a TableRef,
    pub blocks: &'a [i64],
    pub put_columns: &'a [String],
}

#[derive(Clone, Copy)]
pub enum HistoryRow<'a> {
    Stored {
        batch: &'a EventBatch,
        row: usize,
    },
    Header {
        header: EventHeader,
        origin: Option<EventOrigin>,
        page_row: usize,
    },
}

pub type HistoryVisitor<'a> = dyn FnMut(BlockIndex, HistoryRow<'_>) -> Result<()> + Send + 'a;

pub enum HistoryCoverage {
    Native {
        lineage: String,
        through: ReplicaPosition,
    },
    Semantic {
        lineage: String,
        checkpoint_boundary: ReplicaPosition,
        checkpoint_catalog_sha256: [u8; 32],
        through: ReplicaPosition,
        directory: EntityDirectoryRoot,
        suffix_rows: i64,
    },
}

#[async_trait]
pub trait TableHistorySource: Send + Sync {
    async fn visit_table(
        &self,
        read: HistoryRead<'_>,
        visit: &mut HistoryVisitor<'_>,
    ) -> Result<Option<HistoryCoverage>>;
}
