use rtdb_format::log::proto;
use rtdb_index::{IndexBatch, TableWrite};
use rtdb_types::{BlockIndex, CatalogCut, OwnedWorld, ReplicaPosition, StorageEpoch, StorageVersion};
use sha2::{Digest, Sha256};

use super::*;

// The generation travels in the same retained source frame as its footprint.
// Before enqueue, its batches and encoded bytes are owned by the submitting call.
const MAX_GENERATION_BYTES: usize = 16 << 20;

pub(crate) struct PreparedLanding {
    catalog: CatalogCut,
    writes: StagedWrite,
    refused: bool,
}

pub(super) fn is_landing(operations: &[Operation]) -> bool {
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
            Operation::Public(TxOp::OwnedClockSlot { predecessor }) => Some(*predecessor),
            _ => None,
        });
        let tick = ticks.next().ok_or_else(invalid)?;
        let time = crate::system_time::clock_slot_reading(tick.system_time).ok_or_else(invalid)?;
        if ticks.next().is_some() { return Err(invalid()); }
        Ok(Some(time))
    }

    /// Lowers both the approved effects and the chosen constraint outcome on one
    /// private snapshot. Its provisional transaction ID is used only by the
    /// buffer. The admitted landing operations use the offered system reading.
    pub async fn prepare_proposal_generation(
        &mut self, snapshot: impl FnOnce() -> Snapshot, context: &ResolveContext,
    ) -> Result<Option<Timestamp>> {
        if !is_landing(&self.operations) {
            return Ok(None);
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
        let provisional_id = predecessor.tx_id.0.checked_add(1).ok_or_else(overflow)?;
        let scope = self.placed_attempt(SourcePosition(provisional_id))?.map(PlacedAttempt::scope)
            .filter(|scope| Some(*scope) == context.owned_witness_scope).ok_or_else(invalid)?;

        let time = self.proposal_generation_reading()?.ok_or_else(invalid)?;
        let tick = operations.operations.iter().find_map(|operation| match operation {
            Operation::Public(TxOp::OwnedClockSlot { predecessor }) => Some(*predecessor),
            _ => None,
        }).ok_or_else(invalid)?;
        crate::system_time::clock_slot_time(tick, Some(predecessor), time).ok_or_else(|| {
            Anomaly::incorrect("rtdb.owned/clock-slot-missed", "The offered transaction clock is unavailable")
        })?;
        operations.operations.retain(|operation| !matches!(operation, Operation::Public(TxOp::OwnedClockSlot { .. })));
        operations.place_owned_periods(time, true)?;
        let (primary, refusal) = constraint_receipt::split(&operations.operations, true)?;
        require_landing(primary, refusal)?;

        let base = rtdb_query::retain_catalog_snapshot(base, &context.storage).await?;
        let key = TransactionKey { tx_id: TxId(provisional_id), system_time: time };
        let mut tx = OpenTransaction::new(base.clone(), key, &self.options, context)?;
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
                tx.apply_selected_refusal(primary, refusal, Some(scope)).await?;
                true
            }
            Err(error) => return Err(error),
        };
        let writes = tx.buffer.stage()?;
        if writes.tables().keys().any(|table| table.schema == "xt") {
            return Err(invalid());
        }
        self.prepared_landing = Some(PreparedLanding { catalog, writes, refused });
        self.landing_footprint = Some(footprint);
        Ok(Some(time))
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

impl PreparedLanding {
    pub(super) fn choose(
        self, base: &Snapshot, assigned: TransactionKey, slot: Option<TransactionKey>,
        primary: &[Operation], refusal: &[Operation],
        scope: Option<rtdb_types::PlacedAttemptScope>, context: &ResolveContext,
    ) -> Result<(StagedWrite, bool)> {
        require_landing(primary, refusal)?;
        if slot.is_none() || scope.is_none() || scope != context.owned_witness_scope {
            return Err(OwnedWorld::unavailable());
        }
        // All data and control predicates were evaluated at this exact cut,
        // with this assigned clock. Any intervening publication, catalog or
        // epoch change invalidates the entire read and inheritance proof.
        if self.writes.predecessor() != base.tx_basis()
            || self.writes.block_index() != base.block_index()
            || Some(self.catalog) != base.catalog_cut()
            || self.writes.tx_key().system_time != assigned.system_time
        {
            return Err(stale());
        }
        Ok((self.writes, self.refused))
    }

    pub(crate) fn encode(&self, footprint: &[u8], memory: &Arc<MemoryBudget>) -> Result<(proto::PreparedLanding, Vec<Arc<MemoryReservation>>)> {
        let predecessor = self.writes.predecessor().ok_or_else(invalid)?;
        let mut frame = proto::PreparedLanding {
            predecessor_id: Some(predecessor.tx_id.0),
            predecessor_micros: Some(predecessor.system_time.0),
            block_index: Some(self.writes.block_index().0),
            storage_version: Some(self.catalog.storage_version.0),
            storage_epoch: Some(self.catalog.storage_epoch.0),
            replica_epoch: Some(self.catalog.replica_epoch.0),
            catalog_through: Some(self.catalog.through.0),
            clock_micros: Some(self.writes.tx_key().system_time.0),
            footprint_sha256: Some(Sha256::digest(footprint).to_vec()),
            constraint_refused: Some(self.refused),
            ..Default::default()
        };
        let mut bytes_left = MAX_GENERATION_BYTES;
        let mut charges = Vec::new();
        for (table, write) in self.writes.tables() {
            let batch = write.batch().ok_or_else(invalid)?.arrow();
            let encoded = crate::ipc::encode(batch.schema().as_ref(), [batch], memory)?;
            let name = format!("{}/{}", table.schema, table.table);
            bytes_left = bytes_left.checked_sub(encoded.as_slice().len() + name.len() + 64).ok_or_else(too_large)?;
            let (bytes, charge) = encoded.into_parts();
            frame.table_data.insert(name, bytes);
            charges.push(charge);
        }
        Ok((frame, charges))
    }

    pub(crate) fn decode(
        frame: &proto::PreparedLanding, footprint: Option<&[u8]>, memory: &Arc<MemoryBudget>,
    ) -> Result<Self> {
        let digest = Sha256::digest(footprint.ok_or_else(invalid)?);
        if frame.footprint_sha256.as_deref() != Some(&digest[..]) || frame.table_data.is_empty() {
            return Err(invalid());
        }
        let catalog = CatalogCut {
            storage_version: StorageVersion(frame.storage_version.ok_or_else(invalid)?),
            storage_epoch: StorageEpoch(frame.storage_epoch.ok_or_else(invalid)?),
            replica_epoch: StorageEpoch(frame.replica_epoch.ok_or_else(invalid)?),
            through: ReplicaPosition(frame.catalog_through.ok_or_else(invalid)?),
        };
        let predecessor = TransactionKey {
            tx_id: TxId(frame.predecessor_id.ok_or_else(invalid)?),
            system_time: Timestamp(frame.predecessor_micros.ok_or_else(invalid)?),
        };
        let key = TransactionKey {
            tx_id: TxId(predecessor.tx_id.0.checked_add(1).ok_or_else(invalid)?),
            system_time: Timestamp(frame.clock_micros.ok_or_else(invalid)?),
        };
        let mut bytes_left = MAX_GENERATION_BYTES;
        let mut tables = BTreeMap::new();
        for (name, bytes) in &frame.table_data {
            bytes_left = bytes_left.checked_sub(bytes.len() + name.len() + 64).ok_or_else(too_large)?;
            let (schema, name) = name.split_once('/').ok_or_else(invalid)?;
            if schema.is_empty() || schema == "xt" || name.is_empty() || name.contains('/') {
                return Err(invalid());
            }
            let mut pages = crate::ipc::decode(bytes, memory)?;
            if pages.len() != 1 {
                return Err(invalid());
            }
            let batch = IndexBatch::from_owned(pages.pop().ok_or_else(invalid)?)?;
            tables.insert(TableRef::new(schema, name), TableWrite::new(StructFields::new(), Some(batch)));
        }
        let writes = StagedWrite::new(key, Some(predecessor), BlockIndex(frame.block_index.ok_or_else(invalid)?), tables)?;
        Ok(Self { catalog, writes, refused: frame.constraint_refused.ok_or_else(invalid)? })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_seals_its_footprint_and_requires_the_original_predecessor() -> Result<()> {
        let memory = MemoryBudget::new(1 << 24);
        let state = rtdb_index::PartitionState::empty(Default::default())?;
        let key = |id, micros| TransactionKey { tx_id: TxId(id), system_time: Timestamp(micros) };
        state.apply(&TransactionBuffer::new(state.snapshot(), key(1, 10), Arc::clone(&memory))?.stage()?)?;

        let application_table = TableRef::new("public", "landing");
        let mut buffer = TransactionBuffer::new(state.snapshot(), key(2, 20), Arc::clone(&memory))?;
        buffer.put(application_table.clone(), StructFields::from([("_id".into(), Value::I64(1))]), Interval::ALL)?;
        let catalog = CatalogCut {
            storage_version: StorageVersion(1), storage_epoch: StorageEpoch(1),
            replica_epoch: StorageEpoch(1), through: ReplicaPosition(1),
        };
        let generation = PreparedLanding { catalog, writes: buffer.stage()?, refused: false };
        let (frame, _charge) = generation.encode(b"original footprint", &memory)?;
        assert!(PreparedLanding::decode(&frame, Some(b"foreign footprint"), &memory).is_err());
        let generation = PreparedLanding::decode(&frame, Some(b"original footprint"), &memory)?;
        assert_eq!(generation.catalog, catalog);
        assert!(!state.snapshot().contains_table(&application_table));

        let mut metadata = TransactionBuffer::new(state.snapshot(), key(5, 20), Arc::clone(&memory))?.stage()?;
        metadata.install_prepared_generation(generation.writes)?;
        assert!(!state.snapshot().contains_table(&application_table));
        state.apply(&metadata)?;
        assert!(state.snapshot().contains_table(&application_table));

        let old = PreparedLanding::decode(&frame, Some(b"original footprint"), &memory)?;
        let mut later = TransactionBuffer::new(state.snapshot(), key(6, 30), memory)?.stage()?;
        assert!(later.install_prepared_generation(old.writes).is_err());
        Ok(())
    }
}
