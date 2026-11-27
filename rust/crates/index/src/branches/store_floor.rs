use rtdb_types::{Anomaly, AnomalyCategory, Result, rtdb_v1::{At, Basis, Layer, MAIN_BRANCH}};

use super::{BranchIndex, store::KeyRange};

impl BranchIndex {
    pub fn check_store_purge_guards(&self, since: Basis, keep_from: u64, reads: &[KeyRange]) -> Result<()> {
        if keep_from > since.position() {
            return Err(Anomaly::incorrect("rtdb.store/purge-floor-ahead", "A purge floor cannot exceed its basis"));
        }
        for range in reads {
            range.check()?;
        }
        self.require_local_custody()?;
        if self.has_custody_groups() || self.has_visibility_history() {
            return Err(Anomaly::unsupported("rtdb.store/unsupported", "The ordered-bytes store serves only an unlabelled database"));
        }
        let now = self.head.system_time;
        self.view(MAIN_BRANCH, At::Exactly(since), Layer::Merged, now)?;
        let view = self.view(MAIN_BRANCH, At::Latest, Layer::Merged, now)?;
        for (read, range) in reads.iter().enumerate() {
            if view.store_written_since(range, since)?.is_some() {
                return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.store/purge-guard",
                    "A write followed the purge plan's observation")
                    .with_context("read", (read as u64).into()));
            }
        }
        Ok(())
    }

    pub fn check_store_purge_branch_retention(&self, keep_from: u64) -> Result<()> {
        let retained_branch = self.branches.values().any(|branch| {
            branch.base.as_ref().is_some_and(|(_, basis)| basis.position() < keep_from)
                && branch.dropped.is_none_or(|basis| basis.position() > keep_from)
        });
        // Scoped holds use projected bases with a different version line.
        if retained_branch || !self.scoped_holds.is_empty() {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.store/purge-retention",
                "A native branch or scoped hold still retains history"));
        }
        Ok(())
    }
}
