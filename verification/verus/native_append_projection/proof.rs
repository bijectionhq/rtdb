#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/format/src/envelope/decoder.rs"]
mod production;
#[path = "../../../rust/crates/format/src/envelope/wire.rs"]
mod wire;
#[path = "../../../rust/crates/format/src/envelope/log_headers.rs"]
mod log_headers;
#[path = "../framing_decoders/decoder_leaves.rs"]
mod decoder_leaves;
mod laws;

use log_headers::{LEGACY_FLUSH_HEADER, LEGACY_TX_HEADER, PROTOBUF_LOG_HEADER, RTDB_EXTENSION_LOG_HEADER, XTDB_REFUSAL_PREFIX};

#[derive(Clone, Copy)]
pub struct Term(i64);
#[derive(Clone, Copy)]
pub struct TermSequence(i64);
pub struct ReplicaBody { variant: u8, value: u64 }
pub struct SourceBody { variant: u8, value: u64 }
#[derive(Clone, Copy)]
pub struct PlacedAttempt { id: u64 }

fn invalid(message: &'static str) -> &'static str { message }

fn main() {}
