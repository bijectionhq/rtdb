//! Bitemporal full-text search: search files over a trie or a partition group, and the plumbing that builds and reads them.
//! Behavioural spec: dev/doc/fts.allium; plan: dev/doc/fts-implementation.adoc.

pub mod accelerate;
pub mod definitions;
pub mod file;
pub mod lane;
mod held_corpus;
mod member_bounds;
mod rank_cutoff;
mod rank_windows;
mod rectangle;
mod term_directory;
pub mod runtime;
mod delta;

pub use file::{DocMatch, FORMAT_VERSION, MemberMatches, SearchFile, SearchRow, Source, build_search_file, write_group_search_file};
pub use definitions::{SearchDefinition, definitions_in};
pub use lane::{GROUP_BYTES, KIND, SearchArtefacts, SearchBuilder, search_pages};
pub use accelerate::{OwnedSearchBatch, OwnedSearchHit, OwnedSearchPage, SearchAccelerator, owned_corpus, owned_ranked, owned_ranked_with_overlay, owned_ranked_overlay_batch};
pub use held_corpus::{HeldTextCorpus, owned_text_column};
pub use runtime::{SearchRuntime, Suspect};
