mod artefact;
mod driver;
mod entity_history;
mod failure;
mod gc;
mod job;
mod merge;
mod publication;
mod retention;
mod retirement_intent;

pub use artefact::{ArtefactBuilder, ArtefactSource, wants, ArtefactDatabase, ReplayVisitor, RetainedArtefactSource, ArtefactDefinition, ArtefactKind, ArtefactDriver, ArtefactHandle, ArtefactJob, ArtefactPublisher, available_artefact_jobs, retired_artefacts, stray_artefacts};
pub use driver::{CompactionDriver, CompactionHandle, CompactionExecutor, CycleDriver, CycleHandle, CycleWorker};
pub use entity_history::EntityHistoryArtefacts;
pub use failure::{CompactionFailure, CompactionObserver, StderrCompactionObserver};
pub use gc::{BlockCollector, CollectionObserver, CollectorClock, GarbageConfig, TrieCollector, block_is_garbage};
pub use job::{CompactionJob, RecencyMode, available_jobs};
pub use merge::{MergeConfig, RecencyPartition, SegmentCompactor, recency_partition};
pub use publication::{CompactionPublisher, SnapshotSource, TableRetirements, TrieDeletionPublisher};
pub use retirement_intent::ResetRetirements;

/// Reading a stored file within the memory budget, as compaction reads its inputs.
pub mod input { pub use crate::merge::{download, open_input, read_page}; }

fn invalid(message: impl std::fmt::Display) -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect("rtdb.maintenance/invalid-state", message.to_string())
}

fn closed() -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::fault("rtdb.maintenance/closed", "Maintenance scope ended")
}
