use rtdb_format::data::StoredOperation;
use rtdb_types::{BlockIndex, CatalogCut, OWNED_ATTEMPT_RELATION, OwnedRelation, OwnedWorld, PlacedAttemptScope};

use super::control_order::{LandingMarker, landing_marker_matches};
use super::prepared_landing_basis::{LandingBasis, prepared_authority_matches, select_generation};
use super::*;

const MAX_GENERATION_BYTES: usize = 16 << 20;

pub(crate) struct PreparedLanding {
    catalog: CatalogCut,
    writes: StagedWrite,
    slot: crate::OwnedClockSlot,
    scope: PlacedAttemptScope,
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
        let mut operations = self.landing_footprint()?;
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
        if crate::system_time::clock_slot_reading(slot.issued.instant) != Some(time)
            || !crate::system_time::clock_slot_admits(slot.through.0, predecessor.tx_id.0, time.0, appended.0, time.0)
        {
            return Err(Anomaly::incorrect("rtdb.owned/clock-slot-missed", "The offered transaction clock is unavailable"));
        }
        operations.operations.retain(|operation| !matches!(operation, Operation::Public(TxOp::OwnedClockSlot { .. })));
        operations.place_owned_periods(time, true)?;
        operations.place_proposal_keys(time, true)?;
        let (primary, refusal) = constraint_receipt::split(&operations.operations, true)?;
        require_landing(primary, refusal)?;

        let mut tx = OpenTransaction::new(base.clone(), key, &self.options, context)?;
        tx.terms_time = time;
        tx.check_owned_preconditions(primary, Some(scope), true).await?;
        for write in primary.iter().filter_map(crate::hosted_control::owned_document_write) {
            if !write.relation.is_root_data() || !write.relation.name().ends_with("_search") { continue; }
            for row in write.documents.rows() {
                let Some(key) = row.binary_id() else { continue; };
                let Some(selector) = rtdb_types::RootFamilyDerivativeId::parse(key).map_err(|_| invalid())?
                    else { continue; };
                let original = context.world_constraints.as_ref().ok_or_else(OwnedWorld::unavailable)?;
                original.require_root_landing_derivative(PreparedFamilyDerivative {
                    database: &context.database, placement: scope, selector, relation: write.relation.name(), key,
                }).await?;
            }
        }
        for operation in primary {
            context.check_cancelled()?;
            validate_document_operation(operation, self.options.user.as_deref())?;
            tx.apply(operation).await?;
        }
        let staged = match tx.finish_owned_effects(primary).await {
            Ok(()) => PrimaryResult::Staged(()),
            Err(error) if constraint_receipt::may_settle(&error) => PrimaryResult::Constraint(error),
            Err(error) => PrimaryResult::Failed(error),
        };
        let refused = match select_publication(staged, Some(refusal)) {
            Publish::Primary(()) => {
                tx.write_owned_changes().await?;
                false
            }
            Publish::StageRefusal(refusal) => {
                tx = OpenTransaction::new(base, key, &self.options, context)?;
                tx.terms_time = time;
                tx.apply_selected_refusal(primary, refusal, Some(scope)).await?;
                true
            }
            Publish::Abort(error) => return Err(error),
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
        self.prepared_landing = Some(PreparedLanding { catalog, writes, slot, scope, refused });
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
        scope: Option<PlacedAttemptScope>, owner: Option<&str>, context: &ResolveContext,
    ) -> Result<(StagedWrite, bool)> {
        require_landing(primary, refusal)?;
        if !prepared_authority_matches(self.slot, self.scope, slot, scope, context.owned_witness_scope) {
            return Err(OwnedWorld::unavailable());
        }
        let prepared = landing_basis(
            self.writes.predecessor().ok_or_else(stale)?, self.writes.block_index(), self.catalog,
        );
        let installed = landing_basis(
            base.tx_basis().ok_or_else(stale)?, base.block_index(), base.catalog_cut().ok_or_else(stale)?,
        );
        let staged_key = self.writes.tx_key();
        let (writes, refused) = select_generation(
            prepared, installed,
            (staged_key.tx_id.0, staged_key.system_time.0),
            (assigned.tx_id.0, assigned.system_time.0),
            self.writes, self.refused,
        ).ok_or_else(stale)?;
        Self::require_original_marker(&writes, primary, assigned, owner.ok_or_else(no_owned_principal)?, context)?;
        Ok((writes, refused))
    }

    fn require_original_marker(
        writes: &StagedWrite, primary: &[Operation], assigned: TransactionKey, owner: &str,
        context: &ResolveContext,
    ) -> Result<()> {
        let (attempt, submitted) = primary.iter().find_map(|operation| match operation {
            Operation::Public(TxOp::OwnedPublishWorld { attempt, fingerprint, land_root: Some(_), .. }) =>
                Some((attempt, fingerprint)),
            _ => None,
        }).ok_or_else(invalid)?;
        let table = OwnedRelation::new(OWNED_ATTEMPT_RELATION)?.table(owner);
        let batch = writes.tables().get(&table)
            .and_then(|write| write.batch()).filter(|batch| batch.len() == 1).ok_or_else(invalid)?;
        let bound = batch.arrow().get_array_memory_size().checked_mul(4)
            .and_then(|bytes| bytes.checked_add(8192)).ok_or_else(too_large)?;
        let _decoded = context.memory.reserve(bound)?;
        let event = batch.view().read_event(0)?;
        let StoredOperation::Put(row) = event.operation else { return Err(invalid()); };
        let (Some(Value::Utf8(id)), Some(Value::Binary(fingerprint))) = (row.get("_id"), row.get("fingerprint"))
            else { return Err(invalid()); };
        let marker = LandingMarker {
            context: id,
            fingerprint,
            source: event.position.map(|position| position.transaction.0),
            revision: event.system_from.0,
        };
        if row.len() != 2 || event.valid_time != Interval::ALL
            || !landing_marker_matches(marker, attempt, submitted, assigned.tx_id.0, assigned.system_time.0)
        {
            return Err(invalid());
        }
        Ok(())
    }
}
