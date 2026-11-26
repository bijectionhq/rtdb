//! Bitemporal full-text search: search files over a trie or a partition group, and the plumbing that builds and reads them.
//! Behavioural spec: dev/doc/fts.allium; plan: dev/doc/fts-implementation.adoc.

pub mod accelerate;
pub mod definitions;
mod readiness;
pub mod file;
pub mod lane;
mod held_corpus;
mod rank_family;
pub mod hybrid;
pub mod nearest;
mod member_bounds;
use rtdb_query::rank_cutoff;
mod rank_windows;
mod rectangle;
mod term_directory;
pub mod runtime;
mod delta;

pub use file::{DocMatch, FORMAT_VERSION, MemberMatches, SearchFile, SearchRow, Source, build_search_file, write_group_search_file};
pub use definitions::{SearchDefinition, definitions_in};
pub use readiness::committed_native_ready;
pub use lane::{GROUP_BYTES, KIND, SearchArtefacts, SearchBuilder, search_pages};
pub use accelerate::{OwnedSearchBatch, OwnedSearchHit, OwnedSearchPage, SearchAccelerator, owned_corpus, owned_ranked, owned_ranked_with_overlay, owned_ranked_overlay_batch};
pub use held_corpus::{HeldTextCorpus, TextShadows, owned_search_analyzer, owned_text_column};
pub use nearest::{HeldVectorOverlay, NearestPartition, NearestQuery, owned_nearest_batch};
pub use runtime::{SearchRuntime, Suspect};

/// Whether a held owned read's one snapshot is capped exactly at its own transaction basis, no later than the read's clock.
/// The read's clock is a receiver-clock reading taken after the basis was reserved, so it may trail it by any amount.
pub(crate) fn held_at_or_before(snapshot: &rtdb_index::Snapshot, present: rtdb_types::Timestamp) -> bool {
    snapshot.tx_basis().is_some_and(|basis| {
        snapshot.system_time_cap() == Some(basis.system_time) && basis.system_time <= present
    })
}
