mod accelerate;
#[doc(hidden)]
pub mod rank_cutoff;
mod ranking;
#[doc(hidden)]
pub use ranking::{QueryRanking, QueryWinners, RankOutcome, RankSource, RankWork, VerifiedRankRow, run_rank_partition};
mod aggregate;
mod context;
mod branch_snapshot;
pub use branch_snapshot::branch_snapshot;
mod parameters;
pub use parameters::BoundParameters;
mod cursor;
mod runtime_schema;
mod operators;
mod set_keys;
mod scalar_key;
mod limit_bounds;
mod list;
mod column_table;
#[cfg(test)]
mod column_table_tests;
#[cfg(test)]
mod list_tests;
mod join;
mod join_key;
mod pushdown;
mod sort;
mod window;
mod scan;
#[doc(hidden)]
pub use scan::{trie_row_count_up_to, visit_trie_iids};
#[doc(hidden)]
pub use scan::visit_trie_events;
mod fragment;
mod columnar;
mod vector;
mod cosine;
mod page_metadata;
mod scan_tasks;
mod spill;
mod patch;
mod system_schema;
mod system;
mod inspection;
mod trace;
mod settings;
pub use settings::{SnapshotAdmission, SnapshotTokenIssuer, SqlSnapshotView};
mod pages;
mod analysis;
mod batch_join;
mod explain;
mod optimize;
mod materialized;
mod cross;
mod query_cpu;
mod join_storage;
mod join_spill;
mod mark;

pub use accelerate::{CandidateIids, CandidateScores, CandidateVerifier, ConstantEvaluator, FileMatches, IidScope, LiveSearchTerms, MatchAnswers, PreparedSearchQuery, OrderedBatches, OrderedCandidates, OrderedScan, ResolvedSearchRow, ScanAccelerator, ScanRequest, ScanSelection, SearchRowLocation, SnapshotRow};
pub use accelerate::{LocatedHistoryEvent, resolve_located_point};
mod cow_pages;
pub use cow_pages::ObjectPageStore;
pub mod entity_directory;
mod entity_history;
pub use entity_history::{HistoryObject, table_point_rows, uncovered_history_iids};
pub use context::{CatalogSnapshot, DatabaseSnapshot, OrderedIndexInventory, OrderedIndexStorage, QueryContext, QueryOptions, retain_catalog_snapshot};
pub use fragment::{FragmentBytes, FragmentDepth, FragmentReply, FragmentResidence, ScanFragment, ScanFragmenter, TriePrefix, evaluate_fragment, evaluate_fragment_retained, evaluate_fragment_with_arguments};
pub use cursor::{PreparedQuery, QueryCursor, encoding_ordered_batch};
pub use rtdb_storage::sealing::{DeliveryIo, current_delivery_deadline};
pub use query_cpu::QueryCpu;
pub use settings::unindexed_tx;
pub use runtime_schema::RuntimeSchema;

fn invalid(code: &str, message: impl std::fmt::Display) -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect(format!("rtdb.query/{code}"), message.to_string())
}

fn cardinality_violation() -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect("xtdb.single-join/cardinality-violation", "cardinality violation")
}

mod hosted_control;
pub use hosted_control::{HostedControlPoint, read_hosted_control_point};
mod owned_purge;
pub use owned_purge::{OwnedPurgeBucket, owned_purge_floor, read_owned_purge_bucket, purge_coverage_document};
mod owned_relation;
mod owned_unique;
mod owned_range;
mod owned_range_order;
pub use owned_range::{verify_owned_entity_region, verify_owned_fixed_range};
pub use owned_relation::OwnedSpatialScan;
pub use owned_unique::{OwnedUniqueHit, OwnedUniquePolicy, OWNED_UNIQUE_KEY_MAX, OWNED_UNIQUE_PATHS_MAX, OWNED_UNIQUE_POLICY_ID, lookup_owned_unique, owned_unique_key_id, owned_unique_path_key, read_owned_unique_policy};
pub use owned_relation::{
    ArchivedHeldClock, OwnedChangeEntry, OwnedIdentityCandidates, OwnedSpatialProjection, open_owned_artefact_candidate, open_owned_artefact_candidates, open_owned_assertion_point, open_owned_capacity_point, open_owned_identity_page, open_owned_identity_page_all_valid, open_owned_identity_prefix, open_owned_path_catalog, open_owned_point, open_owned_point_at_system, open_owned_root_prefix_projection, open_owned_root_projection, open_owned_root_projection_of,
    open_owned_spatial_scan, open_owned_key_timeline, open_owned_unique_point, open_owned_unique_timeline, open_owned_unique_window, open_archived_world_unique_window, open_staged_unique_window, open_owned_valid_boundaries, open_owned_valid_during_at, owned_identity_candidates, owned_micros, owned_row_revision, owned_row_revision_for, read_owned_parent_schemas, read_owned_root_publication,
    read_archived_held_clock, read_held_world_publication, read_owned_change_entry, read_owned_fixed_range_changes, read_owned_fixed_range_rule, read_owned_lifetime_floor, read_owned_lifetime_floor_table, read_owned_relation_schema, read_owned_retirement, read_owned_retirements, read_owned_schema_catalog, read_owned_world, read_owned_world_at, read_owned_world_with_revision, read_owned_world_clock, read_owned_worlds, read_owned_world_clock_publication, select_owned_world_clock,
};
pub use owned_relation::{
    open_owned_lifetime_point, open_owned_point_with_position_mask, open_owned_valid_during_with_position_mask, open_owned_world_constraint_projection,
    owned_receipt_transaction, owned_row_event_position, read_owned_point_version, owned_row_assertion_ordinal, owned_row_lifetime, read_owned_point_revision,
    try_owned_lifetime_range_expiry,
};

mod owned_journal;
pub use owned_journal::{OwnedJournalHeader, read_owned_journal_header};
