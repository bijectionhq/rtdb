mod erasure_ancestry;
pub use erasure_ancestry::{EventCoordinate, ForkCoordinates, ForkEvents, ForkSide, InheritedEvents, decode_fork_events, encode_fork_events, erasure_subject, project_erasure_origins};
mod admission;
mod admission_step;
mod appender;
mod blocks;
mod checkpoint_admin;
mod catalog_cut;
mod checkpoint_discovery;
mod crash_log;
mod replay_floor;
mod driver;
mod default_erasure;
pub use default_erasure::{DefaultErasureReadCut, DefaultErasureReadFence, LocalDefaultErasureReadFence, LocalErasureRead};
mod fact_history;
mod applied_prefix;
mod logs;
mod maintenance_class;
mod promotion;
mod progress;
mod readback_identity;
mod recovery_promotion;
mod replay_progress;
mod replay_closure;
mod repair;
mod repair_decision;
mod retirement_receipt;
mod snapshot_replay;
mod sql_survivor;
mod state;
mod serving;
mod table_replay;

pub use rtdb_tx::project_held_replica;

pub fn verify_archived_transaction(
    message: &rtdb_format::log::proto::ResolvedTx,
    predecessor: rtdb_types::TransactionKey,
    block: rtdb_types::BlockIndex,
    memory: &std::sync::Arc<rtdb_columnar::MemoryBudget>,
) -> rtdb_types::Result<()> {
    rtdb_tx::decode_replica(message, Some(predecessor), block, memory).map(|_| ())
}
pub use admission_step::{Admission, TermFence, TermPosition, Verdict};
pub use blocks::{BlockDiscovery, BlockStore, ProducedBlock, PublishedCheckpoint, RecoveredPartition, latest_stored_block, stage_live_primary};
pub use checkpoint_admin::{DetachedDefaultCheckpoint, LocalDefaultRestoreSeed};
pub use driver::{CommittedErasure, ExternalReceipt, DatabaseEffects, DriverStatus, InstalledPrefix, RecoverableExternalToken, ReplicationClock, ReplicationConfig, ReplicationDriver, ReplicationHandle, Role, ServingSearchBuilder, TermHandle};
pub use logs::{PartitionLogs, ReplicaMetadata, ReplicaReceipt, ReplicaRecord};
pub use snapshot_replay::{ErasureTarget, SnapshotReplay, SqlErasureSource};
pub use sql_survivor::SqlSurvivor;
pub use replay_closure::{ReplayBase, ReplayDependencySources, checkpoint_dependencies, tail_dependencies};
pub use repair::{LocalRepair, PrimaryReset, PublishedPrimaryReset, RepairCheckpoint, RepairedStore, RetainRepairBase, RetainedLocalRepair};
pub use progress::{AppliedProgress, AppliedTransaction, ProgressMonitor};
pub use serving::{classic_inline_receipts, NATIVE_READ_FEATURES, NATIVE_WRITE_FEATURES, NativeCheckpointAuthority, NativeServingAuthority, NativeServingHandle, NativeServingScope, NativeServingView};
pub use state::{BlockCycle, FlushTimer, PendingBlock};
pub use table_replay::{RetainedHistory, RetainedTableReplay};
