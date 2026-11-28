use rtdb_format::journal::{Record, StorePurgeClock};
use rtdb_index::branches::store::{KeyRange, StoreWriterClaim};
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
        if plan.execute(self.store.as_ref()).await? != plan.keys().len() {
            return Ok(());
        }
        let writer = self.writer.as_mut().ok_or_else(no_writer)?;
        writer.retired_store_checkpoints.retain(|&id| closure.contains(id));
        writer.reclaimed_store_history_through = Some(floor);
        Ok(())
    }

    /// Advisory preflight at the settled Multi writer prefix. Durable
    /// publication must repeat admission under its fenced append authority.
    pub async fn check_store_purge_floor(
        &mut self, since: Basis, keep_from: u64, retention_micros: i64, reads: &[KeyRange], writer: Option<StoreWriterClaim>,
    ) -> Result<()> {
        self.admit_store_purge_floor(since, keep_from, retention_micros, reads, writer).await.map(|_| ())
    }

    /// Orders the exclusive upper bound on versions eligible for reclamation.
    /// Recovery and reader obligations can still retain versions below it.
    pub async fn publish_store_purge_floor(
        &mut self, since: Basis, keep_from: u64, retention_micros: i64, reads: &[KeyRange], writer: Option<StoreWriterClaim>,
    ) -> Result<()> {
        if let Some((lease, clock)) = self.admit_store_purge_floor(since, keep_from, retention_micros, reads, writer).await? {
            self.append_control(lease, Record::StorePurgeReclaim { since, keep_from, clock }).await?;
        }
        Ok(())
    }

    async fn admit_store_purge_floor(
        &mut self, since: Basis, keep_from: u64, retention_micros: i64, reads: &[KeyRange], writer: Option<StoreWriterClaim>,
    ) -> Result<Option<(Lease, StorePurgeClock)>> {
        self.stream.require_serving().await?;
        self.refresh().await?;
        let lease = self.settled_store_writer()?;
        let state = self.state();
        let head = state.head;
        let journal_floor = state.floor;

        let index = state.transactions.index();
        index.require_store_writer(writer.map(|claim| (since.incarnation(), claim.fence, claim.generation, claim.claim)))?;
        index.check_store_purge_guards(since, keep_from, reads)?;
        if since.position() < state.store_purge_floor {
            return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/cursor-expired",
                "The purge basis precedes the effective Store floor"));
        }
        let clock = StorePurgeClock { census_micros: retention_micros, lower_bound_micros: self.lower() };
        if !clock.covers_census() {
            return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb.store/purge-clock",
                "The writer's lower clock bound has not certified the retention census"));
        }
        if keep_from <= state.store_purge_floor && keep_from <= index.store_history_floor() {
            self.require_authority(lease)?;
            return Ok(None);
        }
        index.check_store_purge_branch_retention(keep_from)?;
        self.require_destroyed_generation()?;
        let reachability = self.reachability(lease).await?;
        for pin in reachability.pins() {
            if !self.pin_has_independent_store_history(pin).await? {
                return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.store/purge-pin",
                    "An ordered checkpoint pin still retains history"));
            }
        }
        self.refresh().await?;
        if self.authority()?.epoch != lease.epoch || self.state().head != head || self.state().floor != journal_floor {
            return Err(prefix_unsettled());
        }
        let generation = self.state().generation;
        self.store_reads.check_floor(keep_from)?;
        self.writer.as_mut().ok_or_else(no_writer)?.store_holds.check_floor(keep_from, clock.lower_bound_micros, generation)?;
        // The journal compares the next slot and writer fence when the caller appends.
        Ok(Some((lease, clock)))
    }

    pub(super) fn settled_store_writer(&self) -> Result<Lease> {
        let lease = self.authority()?;
        self.require_armed_rotation()?;
        let state = self.state();
        let writer = self.writer.as_ref().ok_or_else(no_writer)?;
        if writer.acknowledged != state.head
            || writer.pipeline.next_position() != state.coordinates.position(next(state.head)?)?
            || state.lease.map(|(epoch, _)| epoch) != Some(lease.epoch)
            || state.transactions.recover_pending().is_some()
        {
            return Err(prefix_unsettled());
        }
        Ok(lease)
    }
}

pub(super) fn prefix_unsettled() -> Anomaly {
    Anomaly::new(AnomalyCategory::Busy, "rtdb.store/purge-prefix-unsettled",
        "The writer must check the purge floor against a settled journal prefix")
}
