//! Bitemporal full-text search: per-trie search files and the plumbing that builds and reads them.
//! Behavioural spec: dev/doc/fts.allium; plan: dev/doc/fts-implementation.adoc.

pub mod accelerate;
pub mod definitions;
pub mod file;
pub mod lane;
pub mod runtime;

pub use file::{DocMatch, FORMAT_VERSION, SearchFile, SearchRow, Source, build_search_file};
pub use definitions::{SearchDefinition, definitions_in};
pub use lane::{KIND, SearchArtefacts, SearchBuilder, search_pages};
pub use accelerate::SearchAccelerator;
pub use runtime::{SearchRuntime, Suspect};
