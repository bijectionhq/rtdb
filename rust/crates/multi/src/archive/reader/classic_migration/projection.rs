use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_db::classic_import_basis;
use rtdb_directory::CheckpointTransferBootstrap;
use rtdb_format::data::{StoredEvent, StoredOperation};
use rtdb_index::{
    Snapshot,
    branches::{BranchCut, BranchEffect, BranchIndex, BranchTable, TableSchema},
};
use rtdb_replication::RecoveredPartition;
use rtdb_tx::transact::TransactState;
use rtdb_types::{
    EntityId, Iid, Interval, Result, TableRef, TimeUnit, Timestamp, TransactionKey, TxId, Value,
    rtdb_v1::Basis,
};
use serde::Serialize;
use uuid::Uuid;

use super::{
    RelationInventory, RelationKind, RetainedInventory,
    application::{Application, ApplicationEvidence},
    inconsistent, is_transactions,
};
use crate::archive::{encode, sha256};

pub(crate) struct ClassicPrepared {
    pub(crate) checkpoint: Option<bytes::Bytes>,
    pub(crate) proof: ClassicProjection,
    pub(crate) application: Option<ApplicationEvidence>,
    _charges: Vec<Arc<MemoryReservation>>,
}

#[derive(Serialize)]
pub(crate) struct ClassicProjection {
    pub(crate) sha256: [u8; 32],
    pub(crate) terminal: TransactionKey,
    pub(crate) native_basis: Basis,
    cuts: usize,
    cut_mapping_sha256: [u8; 32],
    retained: RetainedInventory,
    pub(crate) checkpoint_sha256: Option<[u8; 32]>,
    canonical_receipts: Option<usize>,
    relations: Vec<ProjectedRelation>,
    outstanding: Vec<&'static str>,
}

#[derive(Serialize)]
struct ProjectedRelation {
    source: RelationInventory,
    coverage: RelationCoverage,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum RelationCoverage {
    EmptyLiteral,
    PositionedLiteral,
    ClockAnchoredLiteral,
    ErasureRequiresOriginal,
    ProtectedRequiresOriginal,
    ClassicOutcomeRetained,
    OwnedRequiresOriginal,
    SystemRequiresDisposition,
    LiteralRequiresEventTranslation,
}

pub(super) struct Projection {
    destination: Uuid,
    request: [u8; 32],
    terminal: TransactionKey,
    memory: Arc<MemoryBudget>,
    cuts: BTreeMap<TxId, Timestamp>,
    events: BTreeMap<TableRef, Vec<(StoredEvent, [u8; 32])>>,
    charges: Vec<Arc<MemoryReservation>>,
    application: Option<Application>,
}

impl Projection {
    pub(super) fn new(
        destination: Uuid,
        request: [u8; 32],
        terminal: TransactionKey,
        memory: Arc<MemoryBudget>,
    ) -> Self {
        Self {
            destination,
            request,
            terminal,
            memory,
            cuts: BTreeMap::new(),
            events: BTreeMap::new(),
            charges: Vec::new(),
            application: None,
        }
    }

    pub(super) fn with_application(mut self, application: Application) -> Self {
        self.application = Some(application);
        self
    }

    pub(super) fn observe(
        &mut self,
        table: &TableRef,
        event: StoredEvent,
        fingerprint: [u8; 32],
        charge: Arc<MemoryReservation>,
    ) -> Result<()> {
        if let Some(position) = event.position {
            self.cut(position.transaction, event.system_from)?;
        }
        if is_transactions(table) {
            if let Some(application) = &mut self.application {
                application.observe_transaction(&event, fingerprint)?;
            }
            if let StoredOperation::Put(document) = &event.operation
                && let (
                    Some(Value::I64(id)),
                    Some(Value::Timestamp {
                        value,
                        unit: TimeUnit::Microsecond,
                        ..
                    }),
                ) = (document.get("_id"), document.get("system_time"))
            {
                self.cut(TxId(*id), Timestamp(*value))?;
            }
        } else {
            self.events
                .entry(table.clone())
                .or_default()
                .push((event, fingerprint));
            self.charges.push(charge);
        }
        Ok(())
    }

    fn cut(&mut self, transaction: TxId, time: Timestamp) -> Result<()> {
        let key = TransactionKey {
            tx_id: transaction,
            system_time: time,
        };
        if !key.at_or_before(self.terminal)
            || self
                .cuts
                .insert(transaction, time)
                .is_some_and(|prior| prior != time)
        {
            return Err(inconsistent(
                "The retained Classic transaction coordinates disagree",
            ));
        }
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        snapshot: &Snapshot,
        inventory: &[RelationInventory],
        retained: &RetainedInventory,
        recovered: &RecoveredPartition,
        bootstrap: &CheckpointTransferBootstrap,
        serving_suffix: bool,
    ) -> Result<ClassicPrepared> {
        self.cut(self.terminal.tx_id, self.terminal.system_time)?;
        let block = recovered
            .block
            .as_ref()
            .ok_or_else(|| inconsistent("The Classic replay has no checkpoint"))?;
        if let Some(checkpoint) = block.latest_completed_tx() {
            self.cut(checkpoint.tx_id, checkpoint.system_time)?;
        }
        let mut by_time = BTreeMap::new();
        for (&tx, &time) in &self.cuts {
            by_time
                .entry(time)
                .and_modify(|value| *value = None)
                .or_insert(Some(tx));
        }
        let incarnation = *self.destination.as_bytes();
        let native_basis = classic_import_basis(incarnation, self.terminal.tx_id)?;
        let cut_mapping_sha256 = sha256(&encode(&(incarnation, &self.cuts))?);
        let mut effects = BTreeMap::<TxId, Vec<(TableRef, StoredEvent)>>::new();
        let mut declarations = BTreeMap::new();
        let mut relations = Vec::new();
        for relation in inventory {
            let table = &relation.table;
            let coverage = match relation.kind {
                RelationKind::Literal => {
                    let events = self.events.remove(table).unwrap_or_default();
                    if relation.erasures > 0 || relation.redacted > 0 {
                        RelationCoverage::ErasureRequiresOriginal
                    } else if events.iter().any(|(event, _)| matches!(&event.operation,
                        StoredOperation::Put(cells) if BranchIndex::has_unowned_checkpoint_carrier(table, cells)))
                    {
                        RelationCoverage::ProtectedRequiresOriginal
                    } else if let Some(events) = literal_history(events, &by_time) {
                        for (transaction, event) in events {
                            effects.entry(transaction).or_default().push((table.clone(), event));
                        }
                        declarations.insert(table.clone(), snapshot.declared_columns(table));
                        if relation.events == 0 {
                            RelationCoverage::EmptyLiteral
                        } else if relation.unpositioned == 0 {
                            RelationCoverage::PositionedLiteral
                        } else {
                            RelationCoverage::ClockAnchoredLiteral
                        }
                    } else {
                        RelationCoverage::LiteralRequiresEventTranslation
                    }
                }
                RelationKind::ClassicOutcome => RelationCoverage::ClassicOutcomeRetained,
                RelationKind::Owned => RelationCoverage::OwnedRequiresOriginal,
                RelationKind::System => RelationCoverage::SystemRequiresDisposition,
            };
            relations.push(ProjectedRelation {
                source: relation.clone(),
                coverage,
            });
        }
        let mut index = BranchIndex::new(incarnation);
        for (&tx, &time) in &self.cuts {
            index.advance_classic(BranchCut {
                basis: classic_import_basis(incarnation, tx)?,
                system_time: time,
            })?;
            for (table, event) in effects.remove(&tx).into_iter().flatten() {
                if let Some(application) = &mut self.application {
                    application.observe_event(&table, tx, &event, &self.memory)?;
                }
                let written = event.position.map(|position| position.transaction);
                index.apply_classic_event(&table, event, written)?;
            }
        }
        for (table, columns) in declarations {
            if !columns.is_empty() || index.definition(&table, native_basis).is_none() {
                index.apply(BranchEffect::Declare {
                    table,
                    definition: BranchTable {
                        schema: TableSchema::Open(columns),
                        ..Default::default()
                    },
                })?;
            }
        }
        let application = self.application.take()
            .map(|application| application.finish(&index, retained, inventory, self.terminal, &self.memory))
            .transpose()?;
        let mut transactions = TransactState::new(incarnation);
        transactions.replace_applied_index(index)?;
        let mut outstanding = Vec::new();
        let mut blocked = false;
        if bootstrap.terminal.placed_debt.is_some() {
            outstanding.push("original_placed_reservation");
            blocked = true;
        }
        if serving_suffix {
            outstanding.push("canonical_replica_suffix");
            blocked = true;
        }
        let mut canonical_receipts = None;
        if block.as_proto().serving_checkpoint_object.is_some() {
            outstanding.push("sealed_canonical_checkpoint");
            blocked = true;
        } else {
            match rtdb_replication::classic_inline_receipts(
                block,
                *bootstrap
                    .terminal
                    .fence
                    .destination
                    .intent
                    .source
                    .as_bytes(),
            ) {
                Ok(Some(original)) if original.index().has_classic_migration_controls() => {
                    outstanding.push("canonical_branch_or_custody_controls");
                    blocked = true;
                }
                Ok(Some(original)) => {
                    match transactions.import_classic_receipts(&original, |position| {
                        let tx = TxId(position.source.0);
                        self.cuts.get(&tx)?;
                        classic_import_basis(incarnation, tx).ok()
                    }) {
                        Ok(count) => canonical_receipts = Some(count),
                        Err(_) => {
                            outstanding.push("canonical_receipt_coordinates");
                            blocked = true;
                        }
                    }
                }
                Ok(None) => canonical_receipts = Some(0),
                Err(_) => {
                    outstanding.push("canonical_checkpoint_authentication");
                    blocked = true;
                }
            }
        }
        let checkpoint = if blocked {
            None
        } else {
            let bytes = transactions.encode_checkpoint()?;
            self.charges.push(
                self.memory
                    .reserve(bytes.len().checked_mul(4).ok_or_else(|| {
                        inconsistent("Classic checkpoint exceeds the memory bound")
                    })?)?,
            );
            Some(bytes)
        };
        let checkpoint_sha256 = checkpoint.as_deref().map(sha256);
        let sha256 = sha256(&encode(&(
            "rtdb.multi/classic-destination-projection/v1",
            self.request,
            bootstrap,
            inventory,
            &relations,
            cut_mapping_sha256,
            checkpoint_sha256,
            canonical_receipts,
            &outstanding,
        ))?);
        if retained.worlds != 0
            || retained.suffix_pins != 0
            || retained.other_captures != 0
            || retained.pending_capture_releases != 0
        {
            outstanding.push("retained_original_custody");
        }
        Ok(ClassicPrepared {
            checkpoint,
            application,
            proof: ClassicProjection {
                sha256,
                terminal: self.terminal,
                native_basis,
                cuts: self.cuts.len(),
                cut_mapping_sha256,
                retained: retained.clone(),
                checkpoint_sha256,
                canonical_receipts,
                relations,
                outstanding,
            },
            _charges: self.charges,
        })
    }
}

fn literal_history(
    mut rows: Vec<(StoredEvent, [u8; 32])>,
    cuts: &BTreeMap<Timestamp, Option<TxId>>,
) -> Option<Vec<(TxId, StoredEvent)>> {
    rows.sort_by_key(|(event, _)| event.header().precedence());
    let mut addresses = BTreeMap::<Iid, EntityId>::new();
    let mut identities = BTreeMap::<EntityId, Iid>::new();
    let mut versions = BTreeMap::new();
    let mut unpositioned = BTreeMap::<(TxId, Iid), Vec<Interval>>::new();
    let mut events = Vec::with_capacity(rows.len());
    let mut seen = BTreeSet::new();
    for (event, digest) in rows {
        if event.origin.is_some() || event.commit_position != 0 || event.entity_key.is_some() {
            return None;
        }
        let transaction = match event.position {
            Some(position) => {
                if versions
                    .insert(position, digest)
                    .is_some_and(|prior| prior != digest)
                {
                    return None;
                }
                position.transaction
            }
            None => (*cuts.get(&event.system_from)?)?,
        };
        if !seen.insert(digest) {
            continue;
        }
        if event.position.is_none() {
            let intervals = unpositioned.entry((transaction, event.iid)).or_default();
            if intervals
                .iter()
                .any(|interval| interval.intersects(event.valid_time))
            {
                return None;
            }
            intervals.push(event.valid_time);
        }
        match &event.operation {
            StoredOperation::Put(cells) => {
                let id = cells.get("_id")?.entity_id().ok()?;
                if addresses
                    .insert(event.iid, id.clone())
                    .is_some_and(|prior| prior != id)
                    || identities
                        .insert(id, event.iid)
                        .is_some_and(|prior| prior != event.iid)
                {
                    return None;
                }
            }
            StoredOperation::Delete if addresses.contains_key(&event.iid) => (),
            StoredOperation::Delete | StoredOperation::Erase | StoredOperation::Redacted => {
                return None;
            }
        }
        events.push((transaction, event));
    }
    Some(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::{EventPosition, StructFields};

    fn put(id: &EntityId, valid: Interval, position: Option<EventPosition>) -> StoredEvent {
        StoredEvent {
            position,
            origin: None,
            entity_key: None,
            iid: id.iid(),
            system_from: Timestamp(12),
            commit_position: 0,
            valid_time: valid,
            recency: None,
            operation: StoredOperation::Put(StructFields::from([("_id".into(), id.as_value())])),
        }
    }

    #[test]
    fn positioned_history_orders_events_and_legacy_history_needs_one_unambiguous_cut() {
        let id = EntityId::Integer(1);
        let tx = TxId(81);
        let first = put(
            &id,
            Interval::ALL,
            Some(EventPosition {
                transaction: tx,
                ordinal: 0,
            }),
        );
        let next = put(
            &id,
            Interval::ALL,
            Some(EventPosition {
                transaction: tx,
                ordinal: 1,
            }),
        );
        let rows = vec![(next.clone(), [2; 32]), (first.clone(), [1; 32])];
        assert_eq!(
            literal_history(rows, &BTreeMap::new()).unwrap(),
            [(tx, first), (tx, next)]
        );

        let legacy = put(&id, Interval::ALL, None);
        let cuts = BTreeMap::from([(Timestamp(12), Some(tx))]);
        assert_eq!(
            literal_history(vec![(legacy.clone(), [1; 32])], &cuts).unwrap(),
            [(tx, legacy.clone())]
        );
        assert!(
            literal_history(
                vec![(legacy.clone(), [1; 32]), (legacy.clone(), [2; 32])],
                &cuts
            )
            .is_none()
        );
        assert!(
            literal_history(
                vec![(legacy, [1; 32])],
                &BTreeMap::from([(Timestamp(12), None)])
            )
            .is_none()
        );
    }
}
