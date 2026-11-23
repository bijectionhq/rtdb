mod ordered_index;
pub use ordered_index::path_columns_at_cut;
pub use ordered_index::{OwnedCountRequest, OwnedDecimalSum, OwnedRangeCount, OwnedRangePage, OwnedRangeRequest, owned_count as count_owned_cut, measure_owned_cut, range_owned_cut};
mod session;
mod resources;
mod catalog;
mod config;
mod database;
mod read_currency;
mod node;
mod backends;
mod runtime;
mod metrics;
mod health;
pub use health::{HealthChecks, HealthResponse};
#[cfg(feature = "pg-source")]
mod postgres;
#[cfg(feature = "pg-source")]
pub use postgres::{PostgresIndexerFactory, PostgresSourceFactory};
#[cfg(feature = "kafka-connect")]
mod kafka_connect;
#[cfg(feature = "kafka-connect")]
pub use kafka_connect::{KafkaConnectSourceFactory, KafkaIndexerFactory};

pub use session::*;
pub use resources::*;
mod placed_receipts;
pub use placed_receipts::{AwaitAuthority, ClosedEpochRecovery, PlacedReceiptCodec};
pub use catalog::{DatabaseCatalog, Membership};
pub use config::*;
pub use database::{CompactionRelay, Database};
pub use rtdb_tx::HostedControlWrite;
pub use rtdb_replication::{LocalRepair, RepairCheckpoint, RepairedStore, RetainRepairBase, RetainedLocalRepair};
pub use node::{OwnedStorageUsage, ExpiryCandidate, ExpiryCandidatePage, OwnedIdentityPage, OwnedReplayWorld, OwnedValidHistory, OwnedVersion, OwnedVersionOrder, OwnedVersionPage, OwnedVersionRequest};
#[cfg(feature = "search")]
pub use rtdb_search::{HeldTextCorpus, OwnedSearchPage, owned_ranked as rank_owned_cut, owned_ranked_with_overlay as rank_owned_overlay_cut, owned_text_column};
pub use node::{IDEMPOTENCY_KEY_PARAMETER, NOT_HELD, REPLICA_BEHIND, NativeSession, Node, NodeDriver, NodeServices, OwnedChangeCommit, OwnedChangePage, OwnedForkCut, OwnedMergedChanges, OwnedRelationChanges, OWNED_MERGED_CHANGE_RELATIONS, OwnedRangeWitness, OwnedReadSession, OwnedRootPublication, OwnedRootSection, OwnedWorldEffect, OwnedWorldFork, OwnedWorldWitness, OwnedWorldSection, OwnedWorldShadowRead, OwnedWorldVersionRead, OwnedWorldWrite, ReadFence, WorldClockRequest};

mod statement;
pub use statement::{PreparedSql, ResultSignaturePolicy};
mod control;
mod inspection;
mod inspection_render;
pub use control::{ControlExecutionOptions, ControlFrontend, ControlResult, control_schema, transaction_settings};
mod operations;
pub use operations::{CompactorReset, SnapshotExport};

/// The stack each runtime worker gets: a query's operators and expressions nest with the depth of the statement,
/// and stack overflow aborts the process rather than failing the query. It is reserved, not committed, up front.
pub const WORKER_STACK_BYTES: usize = 16 << 20;
