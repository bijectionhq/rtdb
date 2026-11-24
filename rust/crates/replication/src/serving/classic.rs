use std::{cmp::Reverse, sync::Arc};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::data::{EventBatch, StoredEvent, StoredOperation};
use rtdb_index::{Snapshot, StagedWrite, TransactionBuffer, branches::{BranchCut, BranchEffect, BranchIndex, BranchTable}};
use rtdb_storage::{HeldView, IoRegion, cache::BufferPool};
use rtdb_types::{Anomaly, EntityId, Interval, Iid, LogicalType, Result, StructFields, TableRef, Timestamp, TransactionKey,
    rtdb_v1::{At, Basis, Layer, MAIN_BRANCH}};
use tokio_util::sync::CancellationToken;

pub(super) fn data_table(table: &TableRef) -> bool {
    !matches!(table.schema.as_str(), "xt" | "information_schema" | "pg_catalog") && !TableRef::owned_schema(&table.schema)
}

pub(super) fn touches_data(write: &StagedWrite) -> bool {
    write.tables().iter().any(|(table, write)| {
        data_table(table) && (write.batch().is_some_and(|batch| !batch.is_empty()) || !write.declared_columns().is_empty())
    })
}

fn declare(index: &mut BranchIndex, table: &TableRef, fields: &StructFields<LogicalType>) -> Result<()> {
    let mut definition = index.definition(table, index.head().basis).cloned().unwrap_or_default();
    if definition.dropped {
        definition = BranchTable { generation: definition.generation, ..Default::default() };
    }
    for (column, ty) in fields {
        let ty = definition.columns.get(column).map_or_else(|| ty.clone(), |prior| prior.join(ty));
        definition.columns.insert(column.clone(), ty);
    }
    index.apply(BranchEffect::Declare { table: table.clone(), definition })
}

pub(super) fn apply_live(index: &mut BranchIndex, position: u64, write: &StagedWrite,
    memory: &Arc<MemoryBudget>, charges: &mut Vec<Arc<MemoryReservation>>) -> Result<()> {
    index.advance_classic(BranchCut {
        basis: Basis::new(index.head().basis.incarnation(), position), system_time: write.tx_key().system_time,
    })?;
    let transaction = Some(write.tx_key().tx_id);
    for (table, write) in write.tables().iter().filter(|(table, _)| data_table(table)) {
        if !write.declared_columns().is_empty() { declare(index, table, write.declared_columns())?; }
        if let Some(batch) = write.batch() {
            for row in 0..batch.len() {
                let (event, charge) = read_event(batch.view(), row, memory)?;
                index.apply_classic_event(table, event, transaction)?;
                charges.push(charge);
            }
        }
    }
    Ok(())
}

fn read_event(batch: &EventBatch, row: usize, memory: &Arc<MemoryBudget>) -> Result<(StoredEvent, Arc<MemoryReservation>)> {
    let bytes = batch.borrow_event(row)?.document_allocation_bound()?.checked_add(512)
        .ok_or_else(|| Anomaly::incorrect("rtdb/classic-projection-size", "Classic effect exceeds addressable memory"))?;
    let charge = memory.reserve(bytes)?;
    Ok((batch.read_event(row)?, charge))
}

pub(super) async fn recover(index: &mut BranchIndex, snapshot: Snapshot, storage: &Arc<BufferPool>,
    memory: &Arc<MemoryBudget>, cancellation: &CancellationToken) -> Result<Vec<Arc<MemoryReservation>>> {
    let snapshot = Arc::new(snapshot);
    let hold = HeldView::catalog_at(storage.object_store().as_ref(), snapshot.clone(), snapshot.catalog_cut(),
        snapshot.catalog_checkpoint()?, snapshot.catalog_file_paths()).await?;
    IoRegion::reading(hold, async {
        let mut events = Vec::new();
        let mut charges = Vec::new();
        for table in snapshot.tables().into_iter().filter(data_table) {
            let Some(entry) = snapshot.table_entry(&table) else { continue; };
            for trie in snapshot.trie_entries(&table) {
                rtdb_query::visit_trie_events(storage, memory, cancellation, &entry.slug, &trie.key, |batch| {
                    for row in 0..batch.len() {
                        let (event, charge) = read_event(batch, row, memory)?;
                        events.push((table.clone(), event, events.len()));
                        charges.push(charge);
                    }
                    Ok(())
                }).await?;
            }
        }
        events.sort_unstable_by_key(|(_, event, encountered)| (event.header().precedence(), Reverse(*encountered)));
        let mut previous = None;
        for (table, event, _) in events {
            let written = event.position.map(|position| position.transaction);
            let transaction = (event.system_from, written);
            if previous != Some(transaction) {
                advance(index, event.system_from)?;
                previous = Some(transaction);
            }
            index.apply_classic_event(&table, event, written)?;
        }
        if let Some(key) = snapshot.tx_basis() {
            advance(index, key.system_time)?;
            for table in snapshot.tables().into_iter().filter(data_table) {
                let fields = snapshot.declared_columns(&table);
                if !fields.is_empty() || index.definition(&table, index.head().basis).is_none() {
                    declare(index, &table, &fields)?;
                }
            }
        }
        Ok(charges)
    }).await
}

fn advance(index: &mut BranchIndex, system_time: Timestamp) -> Result<()> {
    let head = index.head().basis;
    let position = head.position().checked_add(1).ok_or_else(super::invalid)?;
    index.advance_classic(BranchCut { basis: Basis::new(head.incarnation(), position), system_time })
}

pub(crate) fn stage_main(snapshot: Snapshot, prior: &BranchIndex, key: TransactionKey, effects: &[BranchEffect],
    memory: &Arc<MemoryBudget>) -> Result<StagedWrite> {
    let mut buffer = TransactionBuffer::new(snapshot, key, Arc::clone(memory))?;
    let view = prior.view(MAIN_BRANCH, At::Latest, Layer::Merged, prior.head().system_time)?;
    for effect in effects {
        match effect {
            BranchEffect::Write { branch, table, event } if branch == MAIN_BRANCH => {
                let iid = prior.physical_iid(table, &event.id);
                let put_until = event.image.as_ref().map_or(event.valid.lower(), |image| {
                    image.expires_at.unwrap_or(event.valid.upper()).clamp(event.valid.lower(), event.valid.upper())
                });
                if let Some(image) = &event.image && put_until > event.valid.lower() {
                    if image.custody_payload.is_some() { return Err(super::invalid()); }
                    append(&mut buffer, table, iid, Interval::new(event.valid.lower(), put_until)?, StoredOperation::Put(image.cells.clone()))?;
                }
                if put_until < event.valid.upper() {
                    append(&mut buffer, table, iid, Interval::new(put_until, event.valid.upper())?, StoredOperation::Delete)?;
                }
            }
            BranchEffect::Erase { branch, table, id } if branch.as_deref().is_none_or(|branch| branch == MAIN_BRANCH) => {
                append(&mut buffer, table, prior.physical_iid(table, id), Interval::ALL, StoredOperation::Erase)?;
            }
            BranchEffect::Declare { table, definition } if definition.dropped => {
                let mut ids = view.ids(table).into_iter().cloned().collect::<std::collections::BTreeSet<EntityId>>();
                ids.extend(effects.iter().filter_map(|effect| match effect {
                    BranchEffect::Write { branch, table: written, event } if branch == MAIN_BRANCH && written == table => Some(event.id.clone()),
                    _ => None,
                }));
                for id in ids { append(&mut buffer, table, prior.physical_iid(table, &id), Interval::ALL, StoredOperation::Delete)?; }
            }
            BranchEffect::Declare { table, definition } => {
                buffer.declare_table(table.clone(), definition.columns.keys().cloned())?;
                let fields = definition.columns.iter().map(|(name, ty)| (name.clone(), ty.clone())).collect::<Vec<_>>();
                buffer.record_sql_declaration(table, &fields)?;
            }
            _ => (),
        }
    }
    buffer.stage()
}

fn append(buffer: &mut TransactionBuffer, table: &TableRef, iid: Iid, valid_time: Interval, operation: StoredOperation) -> Result<()> {
    buffer.append(table.clone(), StoredEvent {
        position: None, origin: None, commit_position: 0, entity_key: None, iid, system_from: buffer.tx_key().system_time,
        valid_time, operation, recency: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::{TxId, Value};

    #[test]
    fn legacy_classic_effect_needs_provenance_for_a_written_basis() {
        let table = TableRef::new("public", "items");
        let id = EntityId::Integer(1);
        let event = || StoredEvent {
            position: None, origin: None, commit_position: 0, entity_key: None, iid: id.iid(), system_from: Timestamp(1),
            valid_time: Interval::ALL, operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(1))])), recency: None,
        };
        let mut index = BranchIndex::new([7; 16]);
        advance(&mut index, Timestamp(1)).unwrap();
        index.apply_classic_event(&table, event(), None).unwrap();
        let root = index.view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(1)).unwrap();
        assert!(root.row(&table, &id, Timestamp(0)).unwrap().written_basis().is_none());
        let mut versions = Vec::new();
        for _ in 0..2 {
            advance(&mut index, Timestamp(1)).unwrap();
            index.apply_classic_event(&table, event(), Some(TxId(7))).unwrap();
            let root = index.view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(1)).unwrap();
            versions.push(root.row(&table, &id, Timestamp(0)).unwrap().written_basis().unwrap());
        }
        assert_eq!(versions[0], versions[1]);
    }

    #[test]
    fn iid_only_masks_use_the_stored_classic_identity() {
        let mut index = BranchIndex::new([7; 16]);
        let table = TableRef::new("public", "items");
        let id = EntityId::Integer(1);
        let iid = Iid([0xa1; 16]);
        let event = |operation, system_from| StoredEvent {
            position: None, origin: None, commit_position: 0, entity_key: None, iid, system_from,
            valid_time: Interval::ALL, operation, recency: None,
        };
        advance(&mut index, Timestamp(1)).unwrap();
        index.apply_classic_event(&table, event(StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(1))])), Timestamp(1)), Some(TxId(19))).unwrap();
        assert_eq!(index.physical_iid(&table, &id), iid);
        let cut = index.head();
        advance(&mut index, Timestamp(1)).unwrap();
        index.apply_classic_event(&table, event(StoredOperation::Delete, Timestamp(1)), Some(TxId(20))).unwrap();
        let root = index.view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(1)).unwrap();
        assert!(root.row(&table, &id, Timestamp(0)).is_none());
        assert_eq!(root.history(&table, &id).len(), 2);
        advance(&mut index, Timestamp(2)).unwrap();
        index.apply_classic_event(&table, event(StoredOperation::Erase, Timestamp(2)), Some(TxId(21))).unwrap();
        let root = index.view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(2)).unwrap();
        assert!(root.history(&table, &id).is_empty());
        assert!(matches!(index.view(MAIN_BRANCH, At::Exactly(cut.basis), Layer::Merged, Timestamp(2)),
            Err(error) if error.code == "rtdb/erased-under-view"));
    }
}
