use std::sync::Arc;

use rtdb_types::{Anomaly, AnomalyCategory, EntityId, Result, Timestamp, Value, rtdb_v1::{At, Basis, Layer, MAIN_BRANCH}};

use super::{BranchEvent, BranchIndex, WrittenVersion, store::{KeyRange, VALUE_COLUMN, store_table}};

impl BranchIndex {
    pub fn store_history_floor(&self) -> u64 {
        self.store_history_floor.unwrap_or(0)
    }

    pub(super) fn require_store_history(&self, basis: Basis) -> Result<()> {
        if self.store_history_floor.is_some() {
            self.require_incarnation(basis)?;
            if basis.position() < self.store_history_floor() {
                return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/cursor-expired",
                    "The requested basis precedes the retained store history"));
            }
        }
        Ok(())
    }

    /// Check an exact dependency at the same index that orders byte-store writes.
    pub fn check_retained_store_basis(&self, basis: Basis) -> Result<()> {
        self.require_store_history(basis)?;
        self.exact(basis).map(|_| ())
    }

    pub fn reclaim_store_history(&mut self, keep_from: u64) {
        if keep_from <= self.store_history_floor() {
            return;
        }
        self.store_history_floor = Some(keep_from);
        let main = Arc::make_mut(self.branches.get_mut(MAIN_BRANCH).expect("main branch"));
        let Some(rows) = main.tables.get_mut(&store_table()) else { return; };
        let rows = Arc::make_mut(rows);
        let mut events = rows.events.clone();
        for (id, history) in &rows.events {
            if !matches!(id, EntityId::Binary(_)) {
                continue;
            }
            let boundary = history.partition_point(|(_, event)| event.cut.basis.position() <= keep_from);
            if let Some((generation, _)) = boundary.checked_sub(1).and_then(|anchor| history.get(anchor))
                && boundary > 1
                && history[..boundary].iter().all(|(g, event)| g == generation && is_plain_store_event(event))
            {
                events.insert(id.clone(), Arc::new(history[boundary - 1..].to_vec()));
            }
        }
        rows.events = events;
    }

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

fn is_plain_store_event(event: &BranchEvent) -> bool {
    event.written == WrittenVersion::Branch
        && event.valid.lower() == event.cut.system_time && event.valid.upper() == Timestamp::MAX
        && event.image.as_ref().is_none_or(|image| {
            image.expires_at.is_none() && image.custody_group.is_none() && image.custody_payload.is_none()
                && image.cells.len() == 2 && matches!(image.cells.get(VALUE_COLUMN), Some(Value::Binary(_)))
        })
}
