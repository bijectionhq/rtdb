use rtdb_types::{BlockIndex, CatalogCut, OwnedWorld};

use super::prepared_landing_basis::{LandingBasis, generation_matches};
use super::*;

const MAX_GENERATION_BYTES: usize = 16 << 20;

pub(crate) struct PreparedLanding {
    catalog: CatalogCut,
    writes: StagedWrite,
    slot: crate::OwnedClockSlot,
    refused: bool,
}

pub(crate) fn is_landing(operations: &[Operation]) -> bool {
    operations.iter().any(|operation| matches!(operation,
        Operation::Public(TxOp::OwnedPublishWorld { land_root: Some(_), .. })
    )) && operations.iter().any(|operation| matches!(operation,
        Operation::Public(TxOp::HostedControl(control)) if control.is_landing_receipt_insert()
    ))
}

fn invalid() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/invalid-prepared-generation", "Invalid prepared Proposal generation")
}

fn too_large() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/prepared-generation-bound", "Prepared Proposal generation exceeds its retained byte bound")
}

fn stale() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/stale-generation", "The prepared Proposal generation's predecessor changed")
}

impl SourceTransaction {
    /// Reads the authored clock target from the bounded command envelope.
    pub fn proposal_generation_reading(&self) -> Result<Option<Timestamp>> {
        if !is_landing(&self.operations) { return Ok(None); }
        let mut ticks = self.operations.iter().filter_map(|operation| match operation {
            Operation::Public(TxOp::OwnedClockSlot { slot }) => Some(*slot),
            _ => None,
        });
        let slot = ticks.next().ok_or_else(invalid)?;
        if ticks.next().is_some() { return Err(invalid()); }
        Ok(Some(slot.reading))
    }

    /// Lowers both the approved effects and the chosen constraint outcome on one
    /// private snapshot at the actual source position. The offered reading
    /// is used for acceptance terms and never addresses the staged events.
    pub async fn prepare_proposal_generation(
        &mut self, snapshot: impl FnOnce() -> Snapshot, key: TransactionKey, appended: Timestamp,
        context: &ResolveContext,
    ) -> Result<()> {
        if !is_landing(&self.operations) {
            return Ok(());
        }
        if self.guard.is_some() || self.options.user_metadata.is_some()
            || self.options.system_time.is_some() || self.prepared_landing.is_some()
        {
            return Err(invalid());
        }
        let footprint = self.encode_landing_footprint()?;
        let mut operations = self.decode_landing_footprint(footprint.as_slice())?;
        let base = snapshot();
        let catalog = base.catalog_cut().ok_or_else(stale)?;
        let predecessor = base.tx_basis().ok_or_else(stale)?;
        let scope = self.placed_attempt(SourcePosition(key.tx_id.0))?.map(PlacedAttempt::scope)
            .filter(|scope| Some(*scope) == context.owned_witness_scope).ok_or_else(invalid)?;

        let time = self.proposal_generation_reading()?.ok_or_else(invalid)?;
        let slot = operations.operations.iter().find_map(|operation| match operation {
            Operation::Public(TxOp::OwnedClockSlot { slot }) => Some(*slot),
            _ => None,
        }).ok_or_else(invalid)?;
        crate::system_time::clock_slot_time(slot, Some(predecessor), appended, time).ok_or_else(|| {
            Anomaly::incorrect("rtdb.owned/clock-slot-missed", "The offered transaction clock is unavailable")
        })?;
        operations.operations.retain(|operation| !matches!(operation, Operation::Public(TxOp::OwnedClockSlot { .. })));
        operations.place_owned_periods(time, true)?;
        operations.place_proposal_keys(time, true)?;
        let (primary, refusal) = constraint_receipt::split(&operations.operations, true)?;
        require_landing(primary, refusal)?;

        let base = rtdb_query::retain_catalog_snapshot(base, &context.storage).await?;
        let mut tx = OpenTransaction::new(base.clone(), key, &self.options, context)?;
        tx.terms_time = time;
        tx.check_owned_preconditions(primary, Some(scope), true).await?;
        for operation in primary {
            context.check_cancelled()?;
            validate_document_operation(operation, self.options.user.as_deref())?;
            tx.apply(operation).await?;
        }
        let refused = match tx.finish_owned_effects(primary).await {
            Ok(()) => {
                tx.write_owned_changes().await?;
                false
            }
            Err(error) if constraint_receipt::may_settle(&error) => {
                tx = OpenTransaction::new(base, key, &self.options, context)?;
                tx.terms_time = time;
                tx.apply_selected_refusal(primary, refusal, Some(scope)).await?;
                true
            }
            Err(error) => return Err(error),
        };
        let writes = tx.buffer.stage()?;
        if writes.tables().keys().any(|table| table.schema == "xt") {
            return Err(invalid());
        }
        let mut remaining = MAX_GENERATION_BYTES;
        for write in writes.tables().values() {
            let batch = write.batch().ok_or_else(invalid)?;
            remaining = remaining.checked_sub(batch.arrow().get_array_memory_size()).ok_or_else(too_large)?;
        }
        self.prepared_landing = Some(PreparedLanding { catalog, writes, slot, refused });
        Ok(())
    }
}

fn require_landing(primary: &[Operation], refusal: &[Operation]) -> Result<()> {
    use crate::hosted_control::ProposalLandingEffect;
    if refusal.is_empty() || !is_landing(primary)
        || crate::hosted_control::hosted_proposal_land(primary)
            .is_none_or(|landing| landing.effect == ProposalLandingEffect::Close)
        || primary.iter().any(|operation| matches!(operation,
            Operation::Public(TxOp::OwnedDeadline { recheck_at_staging: true, .. })))
    {
        return Err(invalid());
    }
    Ok(())
}

fn landing_basis(predecessor: TransactionKey, block: BlockIndex, catalog: CatalogCut) -> LandingBasis {
    LandingBasis {
        predecessor_id: predecessor.tx_id.0,
        predecessor_time: predecessor.system_time.0,
        block: block.0,
        storage_version: catalog.storage_version.0,
        storage_epoch: catalog.storage_epoch.0,
        replica_epoch: catalog.replica_epoch.0,
        replica_through: catalog.through.0,
    }
}

impl PreparedLanding {
    pub(super) fn choose(
        self, base: &Snapshot, assigned: TransactionKey, slot: Option<crate::OwnedClockSlot>,
        primary: &[Operation], refusal: &[Operation],
        scope: Option<rtdb_types::PlacedAttemptScope>, context: &ResolveContext,
    ) -> Result<(StagedWrite, bool)> {
        require_landing(primary, refusal)?;
        if slot != Some(self.slot) || scope.is_none() || scope != context.owned_witness_scope {
            return Err(OwnedWorld::unavailable());
        }
        let prepared = landing_basis(
            self.writes.predecessor().ok_or_else(stale)?, self.writes.block_index(), self.catalog,
        );
        let installed = landing_basis(
            base.tx_basis().ok_or_else(stale)?, base.block_index(), base.catalog_cut().ok_or_else(stale)?,
        );
        let staged_key = self.writes.tx_key();
        if !generation_matches(
            prepared, installed,
            (staged_key.tx_id.0, staged_key.system_time.0),
            (assigned.tx_id.0, assigned.system_time.0),
        ) {
            return Err(stale());
        }
        Ok((self.writes, self.refused))
    }
}
