use crate::production::remaining_after_sealing;
use vstd::prelude::*;

verus! {

// LAW DUR-2-ARCHIVE-CHARGE.
pub fn a_funded_object_charges_its_entire_ciphertext(remaining: u64, plaintext: u64, overhead: u64)
    requires (plaintext as int) + (overhead as int) <= remaining as int,
{
    let result = remaining_after_sealing(remaining, plaintext, overhead);
    assert(result.is_some() && (result.unwrap() as int) == (remaining as int) - plaintext - overhead);
}

// LAW DUR-2-ARCHIVE-LIMIT.
pub fn ciphertext_beyond_the_remaining_budget_is_refused(remaining: u64, plaintext: u64, overhead: u64)
    requires (plaintext as int) + (overhead as int) > remaining as int,
{
    let result = remaining_after_sealing(remaining, plaintext, overhead);
    assert(result.is_none());
}

}
