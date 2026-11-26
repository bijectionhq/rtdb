#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/multi/src/journal/append_selection.rs"]
mod production;
#[path = "../../../rust/crates/multi/src/keys/journal_identity.rs"]
mod journal_identity;
#[path = "../../../rust/crates/multi/src/journal/replay_cursor.rs"]
mod replay_cursor;
#[path = "../../../rust/crates/format/src/envelope/detached.rs"]
mod detached;
#[path = "../../../rust/crates/format/src/envelope/decoder.rs"]
mod decoder;
#[path = "../../../rust/crates/format/src/envelope/wire.rs"]
mod wire;
#[path = "../../../rust/crates/format/src/envelope/log_headers.rs"]
mod log_headers;
#[path = "../framing_decoders/decoder_leaves.rs"]
mod decoder_leaves;
mod digest_leaf;
mod laws;

use log_headers::{LEGACY_FLUSH_HEADER, LEGACY_TX_HEADER, PROTOBUF_LOG_HEADER, RTDB_EXTENSION_LOG_HEADER, XTDB_REFUSAL_PREFIX};

#[derive(Clone, Copy)]
pub enum AppendClass { Data, Maintenance }

pub type Uuid = u128;
#[derive(Clone, Copy)]
pub struct Placement { deployment: Uuid, incarnation: Uuid, group: Uuid, generation: u64 }
#[derive(Clone, Copy)]
pub struct Grant { epoch: u64, key_generation: u64 }
#[derive(Clone, Copy, PartialEq)]
pub struct Term(pub i64);
#[derive(Clone, Copy, PartialEq)]
pub struct TermSequence(pub i64);
pub struct OversizedMessage { pub payload_sha256: Option<Vec<u8>> }
pub struct ResolvedTx { pub acceptance_stage: Option<u64>, pub body_identity: u64 }
pub enum ReplicaBody { OversizedMessage(OversizedMessage), ResolvedTx(ResolvedTx), Control(u64) }
pub struct SourceBody { variant: u8, value: u64 }
#[derive(Clone, Copy)]
pub struct PlacedAttempt { id: u64 }

fn invalid(message: &'static str) -> &'static str { message }
struct Anomaly;
impl Anomaly {
    fn fault(code: &'static str, _message: &'static str) -> &'static str { code }
}

pub struct RetainedRecord {
    next: u64,
    history: u128,
    generation: u64,
    ciphertext_identity: u128,
    timestamp: i64,
    sealed_len: usize,
}

fn main() {}
