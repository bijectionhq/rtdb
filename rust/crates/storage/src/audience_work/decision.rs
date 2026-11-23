#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[derive(Clone, Copy)]
pub struct Limits {
    pub provider_requests: usize,
    pub provider_bytes: usize,
    pub provider_metadata_bytes: usize,
    pub scan_items: usize,
    pub decode_bytes: usize,
}

#[cfg_attr(verus_keep_ghost, verus_spec(fresh => ensures fresh == if active { None } else { limits }))]
pub(super) fn new_action(active: bool, limits: Option<Limits>) -> Option<Limits> {
    if active { None } else { limits }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures out == if actions > 0 {
    Some((actions - 1) as usize)
} else { None }))]
pub(super) fn admit_action(actions: usize) -> Option<usize> {
    actions.checked_sub(1)
}

#[cfg_attr(verus_keep_ghost, verus_spec(next => ensures
    (next.is_some() == (remaining.is_some() && fits(remaining.unwrap(), cost)))
        && (next.is_some() ==> exact_debit(remaining.unwrap(), cost, next.unwrap())),
))]
pub(super) fn debit(remaining: Option<Limits>, cost: Limits) -> Option<Limits> {
    let remaining = remaining?;
    Some(Limits {
        provider_requests: remaining.provider_requests.checked_sub(cost.provider_requests)?,
        provider_bytes: remaining.provider_bytes.checked_sub(cost.provider_bytes)?,
        provider_metadata_bytes: remaining.provider_metadata_bytes.checked_sub(cost.provider_metadata_bytes)?,
        scan_items: remaining.scan_items.checked_sub(cost.scan_items)?,
        decode_bytes: remaining.decode_bytes.checked_sub(cost.decode_bytes)?,
    })
}

pub(super) enum Work {
    ProviderRead(usize),
    Scan(usize),
    Decode(usize),
}

#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures
    (out.provider_requests, out.provider_bytes, out.provider_metadata_bytes, out.scan_items, out.decode_bytes)
        == match work {
            Work::ProviderRead(bytes) => (1usize, bytes, 16_384usize, 0usize, 0usize),
            Work::Scan(items) => (0usize, 0usize, 0usize, items, 0usize),
            Work::Decode(bytes) => (0usize, 0usize, 0usize, 0usize, bytes),
        },
))]
pub(super) fn cost(work: Work) -> Limits {
    let (provider_requests, provider_bytes, provider_metadata_bytes, scan_items, decode_bytes) =
        match work {
            Work::ProviderRead(bytes) => (1, bytes, PROVIDER_METADATA_BYTES, 0, 0),
            Work::Scan(items) => (0, 0, 0, items, 0),
            Work::Decode(bytes) => (0, 0, 0, 0, bytes),
        };
    Limits { provider_requests, provider_bytes, provider_metadata_bytes, scan_items, decode_bytes }
}

#[cfg_attr(verus_keep_ghost, verus_spec(ensures PROVIDER_METADATA_BYTES == 16_384))]
pub const PROVIDER_METADATA_BYTES: usize = 16_384;

#[cfg(verus_keep_ghost)]
verus! {
    pub(super) open spec fn fits(remaining: Limits, cost: Limits) -> bool {
        remaining.provider_requests >= cost.provider_requests
            && remaining.provider_bytes >= cost.provider_bytes
            && remaining.provider_metadata_bytes >= cost.provider_metadata_bytes
            && remaining.scan_items >= cost.scan_items
            && remaining.decode_bytes >= cost.decode_bytes
    }

    pub(super) open spec fn exact_debit(remaining: Limits, cost: Limits, next: Limits) -> bool {
        remaining.provider_requests == cost.provider_requests as int + next.provider_requests as int
            && remaining.provider_bytes == cost.provider_bytes as int + next.provider_bytes as int
            && remaining.provider_metadata_bytes == cost.provider_metadata_bytes as int + next.provider_metadata_bytes as int
            && remaining.scan_items == cost.scan_items as int + next.scan_items as int
            && remaining.decode_bytes == cost.decode_bytes as int + next.decode_bytes as int
    }
}
