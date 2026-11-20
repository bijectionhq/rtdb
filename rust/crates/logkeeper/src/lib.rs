//! The logkeeper: a log server holding many named streams, and a quorum client presenting one stream as an `rtdb_storage::log::Log`.
//!
//! A stream's writer holds an epoch.
//! It seals a write quorum of the stream's servers at that epoch, recovers the committed history onto them, and only then appends; a server accepts an append only from the writer it is synced to at its promised epoch, so a deposed writer cannot commit.
//! An entry is committed once a write quorum holds it durably; readers see committed entries only.

mod client;
mod protocol;
mod server;

pub use client::{QuorumLog, StreamConfig, open_reader, open_writer, seal, trim};
pub use server::{ServerOptions, serve};
