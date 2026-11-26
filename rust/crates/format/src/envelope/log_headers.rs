pub const PROTOBUF_LOG_HEADER: u8 = 3;
pub const LEGACY_TX_HEADER: u8 = 0xff;
pub const LEGACY_FLUSH_HEADER: u8 = 2;

/// Frames a log record that uses a feature XTDB does not implement.
/// XTDB rejects a source record with this header; a replica record also starts its body with [XTDB_REFUSAL_PREFIX].
/// Behavioural spec: dev/doc/db.allium (XtdbRefusesRtdbExtensionRecords).
pub const RTDB_EXTENSION_LOG_HEADER: u8 = 4;

/// A protobuf tag of field number 0, which every protobuf parser rejects before reading any field.
/// XTDB ignores the replica header byte and parses from the next one, so it fails on an extension replica record with
/// `InvalidProtocolBufferException: Protocol message contained an invalid tag (zero).`; the same prefix on a block file
/// fails XTDB's `Block.parseFrom` and `TableBlock.parseFrom`. No encoded protobuf message starts with this byte.
pub const XTDB_REFUSAL_PREFIX: [u8; 1] = [0];

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {
pub(super) proof fn log_headers_keep_their_protocol_values()
    ensures PROTOBUF_LOG_HEADER == 3 && LEGACY_TX_HEADER == 255 && LEGACY_FLUSH_HEADER == 2,
{}

pub(super) proof fn the_extension_header_is_not_a_protocol_header()
    ensures RTDB_EXTENSION_LOG_HEADER == 4,
        RTDB_EXTENSION_LOG_HEADER != PROTOBUF_LOG_HEADER,
        RTDB_EXTENSION_LOG_HEADER != LEGACY_TX_HEADER,
        RTDB_EXTENSION_LOG_HEADER != LEGACY_FLUSH_HEADER,
{}
}
