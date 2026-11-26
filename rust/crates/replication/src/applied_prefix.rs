//! Whether every replica record the driver has read has also been applied.
//!
//! Promotion, writer activation and installed-prefix publication need the
//! replica prefix this node read to be applied, not only read: a record held
//! behind a pending block or an in-flight resolution is applied after records
//! that follow it in the log.
//!
//! This checks that apply was complete. It does not check what was applied.
//! XTDB trusts its term-fenced replica log in the same way. A leader's own
//! read-back is matched to what it appended by fingerprint
//! (`readback_identity`), and a follower applies the record it read.

use std::collections::VecDeque;

pub(crate) struct AppliedPrefix {
    /// Every admitted position up to and including this one has been applied.
    applied_through: i64,
    /// Admitted positions after `applied_through` in replica order, each marked once applied.
    pending: VecDeque<(i64, bool)>,
}

impl AppliedPrefix {
    pub(crate) fn at(base: i64) -> Self {
        Self { applied_through: base, pending: VecDeque::new() }
    }

    /// Admits the next position the reader advanced over. Returns false unless it follows every admitted position.
    pub(crate) fn admit(&mut self, position: i64) -> bool {
        let last = self.pending.back().map_or(self.applied_through, |(last, _)| *last);
        if position <= last { return false; }
        self.pending.push_back((position, false));
        true
    }

    /// Marks an admitted position applied. Returns false if it was never admitted or was already applied.
    pub(crate) fn applied(&mut self, position: i64) -> bool {
        let Ok(index) = self.pending.binary_search_by_key(&position, |(at, _)| *at) else { return false; };
        let applied = &mut self.pending[index].1;
        if *applied { return false; }
        *applied = true;
        while let Some(&(at, true)) = self.pending.front() {
            self.applied_through = at;
            self.pending.pop_front();
        }
        true
    }

    /// True once every admitted position has been applied and the last of them is `through`.
    pub(crate) fn is_complete_at(&self, through: i64) -> bool {
        self.pending.is_empty() && self.applied_through == through
    }
}

#[cfg(test)]
mod tests {
    use super::AppliedPrefix;

    #[test]
    fn a_held_record_keeps_the_prefix_incomplete_until_it_is_applied() {
        let mut prefix = AppliedPrefix::at(10);
        assert!(prefix.is_complete_at(10));
        assert!(prefix.admit(11) && prefix.admit(13) && prefix.admit(14));
        // 11 is held behind a pending block; the upload at 14 and the no-op at 13 apply first.
        assert!(prefix.applied(14) && prefix.applied(13));
        assert!(!prefix.is_complete_at(14));
        assert!(prefix.applied(11));
        assert!(prefix.is_complete_at(14));
    }

    #[test]
    fn positions_are_admitted_in_order_and_applied_once() {
        let mut prefix = AppliedPrefix::at(10);
        assert!(!prefix.admit(10), "the base is already applied");
        assert!(prefix.admit(12));
        assert!(!prefix.admit(11), "the reader cannot go back");
        assert!(!prefix.applied(11), "a position that was never admitted cannot be applied");
        assert!(prefix.applied(12));
        assert!(!prefix.applied(12), "a position is applied once");
        assert!(prefix.is_complete_at(12));
        assert!(!prefix.is_complete_at(13));
    }
}
