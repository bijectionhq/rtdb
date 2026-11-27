use rtdb_format::journal::Record;
use rtdb_index::branches::store::KeyRange;
use rtdb_store::ObjectKey;
use rtdb_types::{Anomaly, AnomalyCategory, Result, rtdb_v1::Basis};

use super::{Lease, MultiDatabase, next, no_writer};

impl MultiDatabase {
    pub(super) async fn reclaim_store_checkpoint_manifests(&mut self) -> Result<()> {
        if self.authority().is_err() || !self.committed.as_ref().is_some_and(|state| state.store_purge_floor != 0) {
            return Ok(());
        }
        self.refresh().await?;
        let lease = self.authority()?;
        let state = self.state();
        let floor = state.floor;
        if state.store_purge_floor == 0 || state.checkpointed_store_history_floor < state.store_purge_floor
            || self.writer.as_ref().and_then(|writer| writer.reclaimed_store_history_through) == Some(floor)
        {
            return Ok(());
        }
        self.require_destroyed_generation()?;
        self.stream.require_serving().await?;
        let closure = self.reachability(lease).await?;
        let prefix = ObjectKey::new(format!("db/{}", self.options.database)).map_err(Anomaly::from)?;
        let listing = self.store.list(Some(&prefix)).await.map_err(Anomaly::from)?;
        let writer = self.writer.as_ref().ok_or_else(no_writer)?;
        let plan = closure.plan_store_history_gc(listing.iter().map(|object| &object.key), &writer.retired_store_checkpoints)?;
        self.require_authority(lease)?;
        plan.execute(self.store.as_ref()).await?;
        let writer = self.writer.as_mut().ok_or_else(no_writer)?;
        writer.retired_store_checkpoints.retain(|&id| closure.contains(id));
        writer.reclaimed_store_history_through = Some(floor);
        Ok(())
    }

    /// Advisory preflight at the settled Multi writer prefix. Durable
    /// publication must repeat admission under its fenced append authority.
    pub async fn check_store_purge_floor(&mut self, since: Basis, keep_from: u64, reads: &[KeyRange]) -> Result<()> {
        self.admit_store_purge_floor(since, keep_from, reads).await.map(|_| ())
    }

    /// Orders the exclusive upper bound on versions eligible for reclamation.
    /// Recovery and reader obligations can still retain versions below it.
    pub async fn publish_store_purge_floor(&mut self, since: Basis, keep_from: u64, reads: &[KeyRange]) -> Result<()> {
        if let Some(lease) = self.admit_store_purge_floor(since, keep_from, reads).await? {
            self.append_control(lease, Record::StorePurgeReclaim { since, keep_from }).await?;
        }
        Ok(())
    }

    async fn admit_store_purge_floor(&mut self, since: Basis, keep_from: u64, reads: &[KeyRange]) -> Result<Option<Lease>> {
        self.stream.require_serving().await?;
        self.refresh().await?;
        let lease = self.authority()?;
        self.require_armed_rotation()?;
        let state = self.state();
        let head = state.head;
        let journal_floor = state.floor;
        let writer = self.writer.as_ref().ok_or_else(no_writer)?;
        if writer.acknowledged != head
            || writer.pipeline.next_position() != state.coordinates.position(next(head)?)?
            || state.lease.map(|(epoch, _)| epoch) != Some(lease.epoch)
            || state.transactions.recover_pending().is_some()
        {
            return Err(prefix_unsettled());
        }

        let index = state.transactions.index();
        index.check_store_purge_guards(since, keep_from, reads)?;
        if keep_from <= state.store_purge_floor && keep_from <= index.store_history_floor() {
            self.require_authority(lease)?;
            return Ok(None);
        }
        index.check_store_purge_branch_retention(keep_from)?;
        self.require_destroyed_generation()?;
        if self.reachability(lease).await?.pins().next().is_some() {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.store/purge-pin",
                "An ordered checkpoint pin still retains history"));
        }
        self.refresh().await?;
        if self.authority()?.epoch != lease.epoch || self.state().head != head || self.state().floor != journal_floor {
            return Err(prefix_unsettled());
        }
        // The journal compares the next slot and writer fence when the caller appends.
        Ok(Some(lease))
    }
}

fn prefix_unsettled() -> Anomaly {
    Anomaly::new(AnomalyCategory::Busy, "rtdb.store/purge-prefix-unsettled",
        "The writer must check the purge floor against a settled journal prefix")
}
