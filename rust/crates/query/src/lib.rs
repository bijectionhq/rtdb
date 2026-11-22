mod accelerate;
mod aggregate;
mod context;
mod parameters;
mod cursor;
mod runtime_schema;
mod operators;
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
mod pages;
mod analysis;
mod batch_join;
mod explain;
mod optimize;
mod materialized;
mod cross;
mod join_storage;
mod mark;

pub use accelerate::{CandidateVerifier, ConstantEvaluator, CorpusRegistry, CorpusSource, FileMatches, MatchAnswers, ScanAccelerator, ScanRequest};
pub use context::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions};
pub use fragment::{FragmentDepth, ScanFragment, ScanFragmenter, TriePrefix, evaluate_fragment};
pub use cursor::{PreparedQuery, QueryCursor};
pub use settings::unindexed_tx;
pub use runtime_schema::RuntimeSchema;

fn invalid(code: &str, message: impl std::fmt::Display) -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect(format!("rtdb.query/{code}"), message.to_string())
}

fn cardinality_violation() -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect("xtdb.single-join/cardinality-violation", "cardinality violation")
}
