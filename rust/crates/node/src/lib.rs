mod ordered_index;
pub use ordered_index::{path_columns_at_cut, stable_owned_path_columns};
pub use ordered_index::{OwnedCountRequest, OwnedDecimalSum, OwnedKeyBound, OwnedRangeCount, OwnedRangePage, OwnedRangeRequest, RetainedKeyBound, owned_count as count_owned_cut, measure_owned_cut, range_owned_cut};
mod session;
mod sql_snapshots;
mod resources;
mod catalog;
mod config;
mod database;
mod read_currency;
mod read_selection;
mod table_retirement;
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
pub mod serving;
pub use rtdb_tx::{ArchivedWorldConstraints, AssertionFenceClaim, AssertionFenceWorldWinner, HostedControlWrite, WorldConstraintArchive};
pub use rtdb_replication::{LocalRepair, RepairCheckpoint, RepairedStore, RetainRepairBase, RetainedLocalRepair};
pub use node::{DurableRange, FundedMutationClock, MAX_DURABLE_RANGE_BYTES, MAX_FUNDED_MUTATION_CLOCK_TOKEN_BYTES};
pub use node::{OwnedStorageUsage, ExpiryCandidate, ExpiryCandidatePage, OwnedIdentityPage, OwnedReplayWorld, OwnedValidHistory, OwnedVersion, OwnedVersionOrder, OwnedVersionPage, OwnedVersionRequest};
pub use node::{AppliedOwnedPoint, AppliedReadConfirmation, AppliedReadFloor};
pub use node::{AssertionFenceStatus, CurrentAssertionFamilyRead, OriginalAssertionFamilyReader, RootCatalogRead};
pub use node::SearchFileFormat;
#[cfg(feature = "search")]
pub use rtdb_search::{
    HeldTextCorpus, TextShadows, OwnedSearchBatch, OwnedSearchPage, owned_ranked as rank_owned_cut,
    owned_ranked_overlay_batch as rank_owned_overlay_batch, owned_ranked_with_overlay as rank_owned_overlay_cut,
    owned_search_analyzer, owned_text_column,
};
#[cfg(feature = "search")]
pub use rtdb_search::{hybrid, nearest};
pub use node::{ChangeEdgeOrder, OwnedChangeEdge, archived_changes_at_cut, archived_path_touch_at_cut, archived_versions_at_cut, HeldSpatialNearest, HeldSpatialProbe, HeldSpatialRange, IDEMPOTENCY_KEY_PARAMETER, NOT_HELD, REPLICA_BEHIND, NativeSession, Node, NodeDriver, NodeServices, OwnedChangeCommit, OwnedChangePage, OwnedForkCut, OwnedMergedChanges, OwnedPublicationLookup, OwnedRelationChanges, OWNED_MERGED_CHANGE_RELATIONS, OwnedRangeWitness, OwnedReadSession, OwnedRootPublication, OwnedRootSection, OwnedWorldAcceptedCut, OwnedWorldEffect, OwnedWorldFork, OwnedWorldObservations, OwnedWorldRegistrationOutcome, OwnedWorldWitness, OwnedWorldSection, OwnedWorldShadowRead, OwnedWorldVersionRead, OwnedWorldWrite, ReadFence, WorldClockRequest};

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

pub use rtdb_tx::{DecisionClock, DecisionObservation};
