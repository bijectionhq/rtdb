use rtdb_storage::audience_work::Limits;

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(ensures
    AUDIENCE_READ_LIMITS.provider_requests == 256,
    AUDIENCE_READ_LIMITS.provider_bytes == 33_554_432,
    AUDIENCE_READ_LIMITS.provider_metadata_bytes == 4_194_304,
    AUDIENCE_READ_LIMITS.scan_items == 65_536,
    AUDIENCE_READ_LIMITS.decode_bytes == 67_108_864,
))]
pub(crate) const AUDIENCE_READ_LIMITS: Limits = Limits {
    provider_requests: 256,
    provider_bytes: 33_554_432,
    provider_metadata_bytes: 4_194_304,
    scan_items: 65_536,
    decode_bytes: 67_108_864,
};
