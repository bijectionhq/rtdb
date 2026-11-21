//! Bitemporal full-text search: search files over a trie or a partition group, and the plumbing that builds and reads them.
//! Behavioural spec: dev/doc/fts.allium; plan: dev/doc/fts-implementation.adoc.

pub mod accelerate;
pub mod corpus;
pub mod definitions;
pub mod file;
pub mod lane;
mod rank_cutoff;
mod rectangle;
pub mod runtime;

pub use file::{DocMatch, FORMAT_VERSION, MemberMatches, SearchFile, SearchRow, Source, build_search_file, write_group_search_file};
pub use definitions::{SearchDefinition, definitions_in};
pub use lane::{GROUP_BYTES, KIND, SearchArtefacts, SearchBuilder, search_pages};
pub use accelerate::SearchAccelerator;
pub use runtime::{SearchRuntime, Suspect};
