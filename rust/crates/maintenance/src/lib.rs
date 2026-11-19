mod artefact;
mod driver;
mod failure;
mod gc;
mod job;
mod merge;
mod publication;

pub use artefact::{ArtefactBuilder, ArtefactDefinition, ArtefactDriver, ArtefactHandle, ArtefactJob, ArtefactPublisher, available_artefact_jobs};
pub use driver::{CompactionDriver, CompactionHandle, CompactionExecutor, CycleDriver, CycleHandle, CycleWorker};
pub use failure::{CompactionFailure, CompactionObserver, StderrCompactionObserver};
pub use gc::{BlockCollector, CollectionObserver, GarbageConfig, TrieCollector, block_is_garbage};
pub use job::{CompactionJob, RecencyMode, available_jobs};
pub use merge::{MergeConfig, RecencyPartition, SegmentCompactor, recency_partition};
pub use publication::{CompactionPublisher, SnapshotSource, TrieDeletionPublisher};

fn invalid(message: impl std::fmt::Display) -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect("rtdb.maintenance/invalid-state", message.to_string())
}

fn closed() -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::fault("rtdb.maintenance/closed", "Maintenance scope ended")
}
