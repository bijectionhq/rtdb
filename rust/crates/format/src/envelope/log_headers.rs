pub const PROTOBUF_LOG_HEADER: u8 = 3;
pub const LEGACY_TX_HEADER: u8 = 0xff;
pub const LEGACY_FLUSH_HEADER: u8 = 2;

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {
pub(super) proof fn log_headers_keep_their_protocol_values()
    ensures PROTOBUF_LOG_HEADER == 3 && LEGACY_TX_HEADER == 255 && LEGACY_FLUSH_HEADER == 2,
{}
}
