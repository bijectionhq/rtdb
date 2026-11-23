use crate::production::entry_fits;
use vstd::prelude::*;

verus! {

// LAW R5-PACK-RANGE.
pub fn an_extent_past_the_payload_is_refused(offset: u64, length: u64, limit: u64)
    requires (offset as int) + (length as int) > (limit as int),
{
    let fits = entry_fits(offset, length, limit);
    assert(!fits);
}

// LAW R5-PACK-OVERFLOW.
pub fn an_extent_that_overflows_u64_is_refused(offset: u64, length: u64, limit: u64)
    requires (offset as int) + (length as int) > u64::MAX as int,
{
    let fits = entry_fits(offset, length, limit);
    assert(!fits);
}

// LAW R5-PACK-END.
pub fn an_entry_can_end_at_the_payload_boundary(offset: u64, length: u64, limit: u64)
    requires (offset as int) + (length as int) == (limit as int),
{
    let fits = entry_fits(offset, length, limit);
    assert(fits);
}

}
