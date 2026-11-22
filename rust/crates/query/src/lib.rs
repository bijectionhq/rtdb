mod accelerate;
mod aggregate;
mod context;
mod parameters;
mod cursor;
mod runtime_schema;
mod operators;
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
mod fragment;
mod columnar;
mod vector;
mod page_metadata;
mod scan_tasks;
mod spill;
mod patch;
mod system_schema;
mod system;
mod inspection;
mod trace;
mod settings;
mod pages;
mod analysis;
mod batch_join;
mod explain;
mod optimize;
mod materialized;
mod cross;
mod query_cpu;
mod join_storage;
mod mark;

pub use accelerate::{CandidateVerifier, ConstantEvaluator, FileMatches, MatchAnswers, OrderedBatches, OrderedCandidates, OrderedScan, ScanAccelerator, ScanRequest};
pub use context::{CatalogSnapshot, DatabaseSnapshot, OrderedIndexInventory, OrderedIndexStorage, QueryContext, QueryOptions, retain_catalog_snapshot};
pub use fragment::{FragmentBytes, FragmentDepth, FragmentResidence, ScanFragment, ScanFragmenter, TriePrefix, evaluate_fragment, evaluate_fragment_retained};
pub use cursor::{PreparedQuery, QueryCursor, encoding_ordered_batch};
pub use query_cpu::QueryCpu;
pub use settings::unindexed_tx;
pub use runtime_schema::RuntimeSchema;

fn invalid(code: &str, message: impl std::fmt::Display) -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect(format!("rtdb.query/{code}"), message.to_string())
}

fn cardinality_violation() -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect("xtdb.single-join/cardinality-violation", "cardinality violation")
}

mod owned_relation;
pub use owned_relation::{open_owned_path_catalog, open_owned_point, open_owned_relation_schema, owned_row_revision};
