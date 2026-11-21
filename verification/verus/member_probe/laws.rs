use crate::production::failed_rounds;
use vstd::prelude::*;

#[verus_spec(out =>
    requires previous < u32::MAX,
    ensures out == previous + 1,
)]
fn healthy_control_does_not_mask_pgwire_failure(previous: u32) -> u32 {
    failed_rounds(previous, true, true, false)
}

#[verus_spec(out => ensures out == 1,)]
fn a_new_pgwire_address_starts_its_own_streak(previous: u32) -> u32 {
    failed_rounds(previous, false, true, false)
}
