//! Content-complete applied image digest chain for ordered recovery, promotion, and serving.
//!
//! A matching replica cut, source watermark, and transaction key can still
//! coexist with an omitted control row, authority row, or transaction receipt
//! if apply drops an effect without checking content completeness. This module
//! tracks the chained digest over the certified checkpoint base and every
//! authenticated replica record's complete effects, control rows, and receipt,
//! requiring the applied image digest to match the authenticated expectation
//! before promotion, writer activation, or `InstalledPrefix` publication.
#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[derive(Clone, Copy)]
pub struct AppliedImageDigest {
    pub expected_position: i64,
    pub applied_position: i64,
    pub expected_digest: [u8; 32],
    pub applied_digest: [u8; 32],
    pub complete: bool,
}

#[cfg_attr(verus_keep_ghost, verus_spec(same => ensures same == (left@ == right@)))]
pub fn same_digest(left: &[u8; 32], right: &[u8; 32]) -> bool {
    let mut i: usize = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        i <= 32,
        forall|k: int| 0 <= k < i ==> #[trigger] left@[k] == right@[k],
        decreases 32 - i,
    ))]
    while i < 32 {
        if left[i] != right[i] {
            return false;
        }
        i += 1;
    }
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(left@ =~= right@);
    }
    true
}

/// Start an applied-image digest chain at the certified checkpoint base
/// (or genesis `[0; 32]` at the initial replica floor).
#[cfg_attr(verus_keep_ghost, verus_spec(state => ensures
    state.expected_position == base_position,
    state.applied_position == base_position,
    state.expected_digest == base_digest,
    state.applied_digest == base_digest,
    state.complete,
))]
pub fn applied_image_from_base(base_position: i64, base_digest: [u8; 32]) -> AppliedImageDigest {
    AppliedImageDigest {
        expected_position: base_position,
        applied_position: base_position,
        expected_digest: base_digest,
        applied_digest: base_digest,
        complete: true,
    }
}

impl AppliedImageDigest {

    /// Record the authenticated expectation for the next replica record before
    /// or independently of applying its effects, control rows, and receipt.
    /// Marks the image incomplete until `record_applied` confirms the exact
    /// same chained digest at `position`.
    #[cfg_attr(verus_keep_ghost, verus_spec(accepted => ensures
        accepted == (old(self).complete && position > old(self).expected_position),
        accepted ==> {
            &&& final(self).expected_position == position
            &&& final(self).expected_digest == next_expected_digest
            &&& final(self).applied_position == old(self).applied_position
            &&& final(self).applied_digest == old(self).applied_digest
            &&& !final(self).complete
        },
        !accepted ==> {
            &&& final(self).expected_position == old(self).expected_position
            &&& final(self).expected_digest == old(self).expected_digest
            &&& final(self).applied_position == old(self).applied_position
            &&& final(self).applied_digest == old(self).applied_digest
            &&& !final(self).complete
        },
    ))]
    pub fn observe_authenticated(&mut self, position: i64, next_expected_digest: [u8; 32]) -> bool {
        if !self.complete || position <= self.expected_position {
            self.complete = false;
            return false;
        }
        self.expected_position = position;
        self.expected_digest = next_expected_digest;
        self.complete = false;
        true
    }

    /// Confirm that applying the record at `position` installed the complete
    /// image (all data writes, control rows, authority rows, and receipt)
    /// producing `next_applied_digest == expected_digest`.
    #[cfg_attr(verus_keep_ghost, verus_spec(matched => ensures
        matched == (!old(self).complete
            && position == old(self).expected_position
            && next_applied_digest@ == old(self).expected_digest@),
        matched ==> {
            &&& final(self).applied_position == position
            &&& final(self).applied_digest == next_applied_digest
            &&& final(self).expected_position == old(self).expected_position
            &&& final(self).expected_digest == old(self).expected_digest
            &&& final(self).applied_digest@ == final(self).expected_digest@
            &&& final(self).complete
        },
        !matched ==> {
            &&& final(self).applied_position == old(self).applied_position
            &&& final(self).applied_digest == old(self).applied_digest
            &&& final(self).expected_position == old(self).expected_position
            &&& final(self).expected_digest == old(self).expected_digest
            &&& !final(self).complete
        },
    ))]
    pub fn record_applied(&mut self, position: i64, next_applied_digest: [u8; 32]) -> bool {
        if self.complete || position != self.expected_position
            || !same_digest(&next_applied_digest, &self.expected_digest)
        {
            self.complete = false;
            return false;
        }
        self.applied_position = position;
        self.applied_digest = next_applied_digest;
        self.complete = true;
        true
    }

    /// True iff the complete authenticated image through `through` (including
    /// every control row and receipt) has been applied and matches the
    /// authenticated expectation digest.
    #[cfg_attr(verus_keep_ghost, verus_spec(ready => ensures
        ready == (self.complete
            && self.applied_position == through
            && self.expected_position == through
            && self.applied_digest@ == self.expected_digest@),
    ))]
    pub fn is_complete_at(&self, through: i64) -> bool {
        self.complete
            && self.applied_position == through
            && self.expected_position == through
            && same_digest(&self.applied_digest, &self.expected_digest)
    }
}
