//! Bitemporal full-text search: per-trie search files and the plumbing that builds and reads them.
//! Behavioural spec: dev/doc/fts.allium; plan: dev/doc/fts-implementation.adoc.

pub mod file;

pub use file::{DocMatch, SearchFile, SearchRow, build_search_file};
