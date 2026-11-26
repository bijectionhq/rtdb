#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/flight-sql/src/audience_seat.rs"]
mod audience_seat;
#[path = "../../../rust/crates/flight-sql/src/config/seat_wire.rs"]
mod seat_wire;
mod seat_uuid;
#[path = "../../../rust/crates/format/src/envelope/wire.rs"]
mod wire;
#[path = "../../../rust/crates/logkeeper/src/server/journal/framing.rs"]
mod framing;

#[path = "journal_leaves.rs"]
mod journal_codec_leaves;
mod laws;

pub struct Kind { variant: u8, data: u64 }
pub struct Record {
    stream: u64,
    kind: Option<Kind>,
    active_ensemble: journal_codec_leaves::Bytes,
    original_append: Option<bool>,
    certified: bool,
}

fn main() {}

#[path = "../../../rust/crates/format/src/envelope/decoder.rs"]
mod decoder;
mod decoder_leaves;

#[path = "../../../rust/crates/format/src/envelope/log_headers.rs"]
mod log_headers;
use log_headers::{LEGACY_FLUSH_HEADER, LEGACY_TX_HEADER, PROTOBUF_LOG_HEADER, RTDB_EXTENSION_LOG_HEADER, XTDB_REFUSAL_SUFFIX};

#[derive(Clone, Copy)]
pub struct Term(i64);
#[derive(Clone, Copy)]
pub struct TermSequence(i64);
pub struct ReplicaBody { variant: u8, value: u64 }
pub struct SourceBody { variant: u8, value: u64 }
#[derive(Clone, Copy)]
pub struct PlacedAttempt { id: u64 }

fn invalid(message: &'static str) -> &'static str { message }
