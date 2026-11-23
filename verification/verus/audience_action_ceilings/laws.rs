use vstd::prelude::*;

use crate::{
    audience_limits::AUDIENCE_READ_LIMITS,
    decision::{admit_action, cost, debit, new_action, Limits, Work},
    drain::retain_until_drained,
};

verus! {
    fn provider_precharge(available: Limits, bytes: usize) -> (next: Option<Limits>)
        ensures
            next.is_some() == (available.provider_requests > 0 && available.provider_bytes >= bytes
                && available.provider_metadata_bytes >= 16_384),
            next.is_some() ==> next.unwrap().provider_requests as int + 1 == available.provider_requests
                && next.unwrap().provider_bytes as int + bytes as int == available.provider_bytes
                && next.unwrap().provider_metadata_bytes as int + 16_384 == available.provider_metadata_bytes
                && next.unwrap().scan_items == available.scan_items
                && next.unwrap().decode_bytes == available.decode_bytes,
    {
        debit(Some(available), cost(Work::ProviderRead(bytes)))
    }

    fn exhausted_is_closed(cost: Limits) -> (next: Option<Limits>)
        ensures next.is_none(),
    {
        debit(None, cost)
    }

    fn action_retry_and_next_action(remaining_actions: usize) -> (out: (Option<Limits>, Option<usize>))
        ensures
            out.0.is_none(),
            out.1 == if remaining_actions > 0 { Some((remaining_actions - 1) as usize) } else { None },
    {
        (new_action(true, Some(AUDIENCE_READ_LIMITS)), admit_action(remaining_actions))
    }

    fn action_boundaries() -> (out: Limits)
        ensures
            out.provider_requests == 256 && out.provider_bytes == 33_554_432
                && out.provider_metadata_bytes == 4_194_304 && out.scan_items == 65_536
                && out.decode_bytes == 67_108_864,
    {
        AUDIENCE_READ_LIMITS
    }

    fn action_and_permits_stay_with_provider<T, U>(permit: T, allowance: U, drained: bool)
        -> (out: Option<(T, U)>)
        ensures out == if drained { None } else { Some((permit, allowance)) },
    {
        retain_until_drained(Some((permit, allowance)), drained)
    }
}
