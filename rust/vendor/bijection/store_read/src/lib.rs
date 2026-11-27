//! The store's pure read core (crate-graph D51, store-cut-rank).
//!
//! Everything a read of owned relations decides from stored bytes, and
//! nothing that writes: the store primitives ([`store`]), the key layout
//! ([`partition`], [`segment`], [`layout`], [`rows`], [`keys`]), owned events
//! and their bitemporal resolution ([`event`]), and the spatial cell keys
//! ([`spatial_key`], [`spatial_index`]). It has no I/O and builds for
//! `wasm32-wasip1`; RTDB vendors it with a pinned digest so ranked reads run
//! next to the data with Bijection's exact code.
pub mod catalog;
pub mod event;
pub mod keys;
pub mod layout;
pub mod partition;
pub mod rank;
pub mod rows;
pub mod segment;
pub mod source;
pub mod spatial_index;
pub mod spatial_key;
pub mod store;
