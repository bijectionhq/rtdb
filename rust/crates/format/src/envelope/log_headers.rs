pub const PROTOBUF_LOG_HEADER: u8 = 3;
pub const LEGACY_TX_HEADER: u8 = 0xff;
pub const LEGACY_FLUSH_HEADER: u8 = 2;

/// Frames a log record that uses a feature XTDB does not implement.
/// XTDB rejects a source record with this header; a replica record also ends with [XTDB_REFUSAL_SUFFIX].
/// Behavioural spec: dev/doc/db.allium (XtdbRefusesRtdbExtensionRecords).
pub const RTDB_EXTENSION_LOG_HEADER: u8 = 4;

/// The storage version named by [XTDB_REFUSAL_SUFFIX]: ASCII "RTDB", which no XTDB release writes.
pub const RTDB_EXTENSION_STORAGE_VERSION: i32 = 0x5254_4442;

/// `oversized_message { storage_version: RTDB_EXTENSION_STORAGE_VERSION }`, field 8 of `ReplicaLogMessage`.
/// XTDB ignores the replica header byte, and the last oneof member on the wire wins, so XTDB reads an extension replica
/// record as an oversized message of a storage version it does not have, which it refuses rather than skips.
pub const XTDB_REFUSAL_SUFFIX: [u8; 8] = [0x42, 0x06, 0x08, 0xc2, 0x88, 0xd1, 0x92, 0x05];

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
