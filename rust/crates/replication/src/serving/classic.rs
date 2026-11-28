use std::{cmp::Reverse, collections::{BTreeMap, BTreeSet}, sync::Arc};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::data::{EncodedEvent, EventBatch, StoredEvent, StoredOperation};
use rtdb_index::{Snapshot, StagedWrite, TransactionBuffer, branches::{BranchCut, BranchEffect, BranchIndex, BranchTable, BranchView, RankedIndexKind, RowImage}};
use rtdb_storage::{HeldView, IoRegion, cache::BufferPool};
use rtdb_tx::transact::Receipt;
use rtdb_types::{Anomaly, EntityId, EventHeader, EventOp, Interval, Iid, LogicalType, Result, SourcePosition, StructFields, TableRef, Timestamp, TransactionKey, TxId, Value,
    rtdb_v1::{At, Basis, Layer, MAIN_BRANCH, SEARCH_BUILD_CATALOG, SPATIAL_BUILD_CATALOG, VECTOR_BUILD_CATALOG, ranked_build_id}};
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
    definition.schema.widen(fields)?;
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
                event.apply(index, table, transaction)?;
                charges.push(charge);
            }
        }
    }
    Ok(())
}

pub(super) struct PreparedEffects {
    tables: Vec<Vec<ClassicEvent>>,
    charges: Vec<Arc<MemoryReservation>>,
    _inventory: Arc<MemoryReservation>,
}

impl PreparedEffects {
    pub(super) fn prepare(write: &StagedWrite, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let overflow = || Anomaly::incorrect("rtdb/classic-projection-size", "Classic effect exceeds addressable memory")
            .with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded);
        let rows = usize::try_from(write.row_count()?).map_err(|_| overflow())?;
        let bytes = rows.checked_mul(std::mem::size_of::<ClassicEvent>() + std::mem::size_of::<Arc<MemoryReservation>>())
            .and_then(|bytes| bytes.checked_add(write.tables().len().checked_mul(128)?))
            .ok_or_else(overflow)?;
        let _inventory = memory.reserve(bytes)?;
        let mut tables = Vec::with_capacity(write.tables().len());
        let mut charges = Vec::with_capacity(rows);
        for (table, write) in write.tables() {
            if !data_table(table) { continue; }
            let mut events = Vec::with_capacity(write.row_count());
            if let Some(batch) = write.batch() {
                for row in 0..batch.len() {
                    let (event, charge) = read_event(batch.view(), row, memory)?;
                    events.push(event);
                    charges.push(charge);
                }
            }
            tables.push(events);
        }
        Ok(Self { tables, charges, _inventory })
    }

    pub(super) fn apply(self, index: &mut BranchIndex, position: u64, write: &StagedWrite) -> Result<Vec<Arc<MemoryReservation>>> {
        index.advance_classic(BranchCut {
            basis: Basis::new(index.head().basis.incarnation(), position), system_time: write.tx_key().system_time,
        })?;
        let transaction = Some(write.tx_key().tx_id);
        for ((table, write), events) in write.tables().iter().filter(|(table, _)| data_table(table)).zip(self.tables) {
            if !write.declared_columns().is_empty() { declare(index, table, write.declared_columns())?; }
            for event in events { event.apply(index, table, transaction)?; }
        }
        Ok(self.charges)
    }
}

struct ClassicEvent {
    header: EventHeader,
    operation: StoredOperation,
}

impl ClassicEvent {
    fn apply(self, index: &mut BranchIndex, table: &TableRef, transaction: Option<TxId>) -> Result<()> {
        index.apply_classic_operation(table, self.header.iid, self.header.valid_time, self.operation, transaction)
    }
}

fn read_event(batch: &EventBatch, row: usize, memory: &Arc<MemoryBudget>) -> Result<(ClassicEvent, Arc<MemoryReservation>)> {
    let event = batch.borrow_event(row)?;
    let bytes = event.document_allocation_bound()?.checked_add(512)
        .ok_or_else(|| Anomaly::incorrect("rtdb/classic-projection-size", "Classic effect exceeds addressable memory").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded))?;
    let charge = memory.reserve(bytes)?;
    let header = event.header();
    let operation = match header.op {
        EventOp::Put => StoredOperation::Put(batch.read_put_document(row)?),
        EventOp::Delete => StoredOperation::Delete,
        EventOp::Erase => StoredOperation::Erase,
        EventOp::Redacted => StoredOperation::Redacted,
    };
    Ok((ClassicEvent { header, operation }, charge))
}

pub(super) async fn recover(index: &mut BranchIndex, snapshot: Snapshot, storage: &Arc<BufferPool>,
    memory: &Arc<MemoryBudget>, cancellation: &CancellationToken) -> Result<Vec<Arc<MemoryReservation>>> {
    let snapshot = Arc::new(snapshot);
    let hold = HeldView::catalog_at(storage.object_store().as_ref(), snapshot.clone(), snapshot.catalog_cut(),
        snapshot.catalog_checkpoint()?).await?;
    IoRegion::reading(hold, async {
        let mut events = Vec::new();
        let mut charges = Vec::new();
        for table in snapshot.tables().into_iter().filter(data_table) {
            let Some(entry) = snapshot.table_entry(&table) else { continue; };
            for trie in snapshot.trie_entries(&table) {
                rtdb_query::visit_full_trie_events(storage, memory, cancellation, &entry.slug, &trie.key, |batch| {
                    for row in 0..batch.len() {
                        let (event, charge) = read_event(batch, row, memory)?;
                        events.push((table.clone(), event, events.len()));
                        charges.push(charge);
                    }
                    Ok(())
                }).await?;
            }
        }
        events.sort_unstable_by_key(|(_, event, encountered)| (event.header.precedence(), Reverse(*encountered)));
        let mut previous = None;
        for (table, event, _) in events {
            let written = event.header.position.map(|position| position.transaction);
            let transaction = (event.header.system_from, written);
            if previous != Some(transaction) {
                advance(index, event.header.system_from)?;
                previous = Some(transaction);
            }
            event.apply(index, &table, written)?;
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

pub(crate) fn stage_source(snapshot: Snapshot, prior: &BranchIndex, source: SourcePosition, receipt: &Receipt, effects: &[BranchEffect],
    memory: &Arc<MemoryBudget>) -> Result<StagedWrite> {
    let key = TransactionKey { tx_id: TxId(source.0), system_time: receipt.system_time };
    let mut buffer = TransactionBuffer::new(snapshot, key, Arc::clone(memory))?;
    let view = prior.view(MAIN_BRANCH, At::Latest, Layer::Merged, prior.head().system_time)?;
    let protected: BTreeSet<_> = effects.iter().filter_map(|effect| match effect {
        BranchEffect::Write { branch, table, event } if branch == MAIN_BRANCH && event.image.as_deref().is_some_and(is_protected) => {
            Some((table, &event.id))
        }
        _ => None,
    }).collect();
    for (ordinal, effect) in effects.iter().enumerate() {
        let preceding = &effects[..ordinal];
        match effect {
            BranchEffect::Write { branch, table, event } if branch == MAIN_BRANCH => {
                let iid = prior.physical_iid(table, &event.id);
                if event.image.as_deref().is_some_and(is_protected) {
                    withdraw_public(&mut buffer, &view, table, &event.id, iid, event.valid, preceding)?;
                    continue;
                }
                if event.image.is_none() && (protected.contains(&(table, &event.id)) || prior.has_custody_address(table, &event.id)) {
                    withdraw_public(&mut buffer, &view, table, &event.id, iid, event.valid, preceding)?;
                    continue;
                }
                let put_until = event.image.as_ref().map_or(event.valid.lower(), |image| {
                    image.expires_at.unwrap_or(event.valid.upper()).clamp(event.valid.lower(), event.valid.upper())
                });
                if let Some(image) = &event.image && put_until > event.valid.lower() {
                    append(&mut buffer, table, iid, Interval::new(event.valid.lower(), put_until)?, StoredOperation::Put(image.cells.clone()))?;
                }
                if put_until < event.valid.upper() {
                    append(&mut buffer, table, iid, Interval::new(put_until, event.valid.upper())?, StoredOperation::Delete)?;
                }
            }
            BranchEffect::Erase { branch, table, id } if branch.as_deref().is_none_or(|branch| branch == MAIN_BRANCH) => {
                let public = view.history(table, id).into_iter().any(|event| event.image.as_deref().is_some_and(|image| !is_protected(image)))
                    || preceding.iter().any(|effect| matches!(effect, BranchEffect::Write { branch, table: written, event }
                        if branch == MAIN_BRANCH && written == table && &event.id == id && event.image.as_deref().is_some_and(|image| !is_protected(image))));
                if public || (!protected.contains(&(table, id)) && !prior.has_custody_address(table, id)) {
                    append(&mut buffer, table, prior.physical_iid(table, id), Interval::ALL, StoredOperation::Erase)?;
                }
            }
            BranchEffect::Declare { table, definition } if definition.dropped => {
                let mut ids = view.ids(table).into_iter().cloned().collect::<BTreeSet<EntityId>>();
                ids.extend(effects.iter().filter_map(|effect| match effect {
                    BranchEffect::Write { branch, table: written, event } if branch == MAIN_BRANCH && written == table => Some(event.id.clone()),
                    _ => None,
                }));
                for id in ids {
                    let iid = prior.physical_iid(table, &id);
                    if protected.contains(&(table, &id)) || prior.has_custody_address(table, &id) {
                        withdraw_public(&mut buffer, &view, table, &id, iid, Interval::ALL, preceding)?;
                    } else {
                        append(&mut buffer, table, iid, Interval::ALL, StoredOperation::Delete)?;
                    }
                }
            }
            BranchEffect::Declare { table, definition } => {
                let columns = definition.schema.columns();
                buffer.declare_table(table.clone(), columns.keys().cloned())?;
                let fields = columns.iter().map(|(name, ty)| (name.clone(), ty.clone())).collect::<Vec<_>>();
                buffer.record_sql_declaration(table, &fields)?;
            }
            _ => (),
        }
    }
    stage_ranked_builds(prior, effects, &mut buffer, memory)?;
    buffer.stage()
}

fn stage_ranked_builds(prior: &BranchIndex, effects: &[BranchEffect], buffer: &mut TransactionBuffer, memory: &Arc<MemoryBudget>) -> Result<()> {
    let declarations = effects.iter().filter_map(|effect| match effect {
        BranchEffect::Declare { table, definition } if data_table(table) => Some((table, definition)),
        _ => None,
    });
    let _scratch = memory.reserve(declarations.clone().count().saturating_mul(128))?;
    let final_tables: BTreeMap<_, _> = declarations.collect();
    let catalog = |kind: &RankedIndexKind| match kind {
        RankedIndexKind::Search { .. } => SEARCH_BUILD_CATALOG,
        RankedIndexKind::Spatial { .. } => SPATIAL_BUILD_CATALOG,
        RankedIndexKind::Ivf { .. } => VECTOR_BUILD_CATALOG,
    };
    for (table, final_definition) in final_tables {
        let old = prior.definition(table, prior.head().basis).filter(|definition| !definition.dropped);
        let final_indexes = &final_definition.ranked_indexes;
        let id = |name: &str| ranked_build_id(table, name);
        if let Some(old) = old {
            for (name, declaration) in &old.ranked_indexes {
                let catalog_name = catalog(&declaration.definition.kind);
                if final_indexes.get(name).map(|index| catalog(&index.definition.kind)) != Some(catalog_name)
                {
                    buffer.delete(TableRef::new("xt", catalog_name), &EntityId::String(id(name)), Interval::ALL)?;
                }
            }
        }
        for (name, declaration) in final_indexes {
            let catalog = catalog(&declaration.definition.kind);
            if old.and_then(|old| old.ranked_indexes.get(name)) == Some(declaration) { continue; }
            let Ok(generation) = i64::try_from(declaration.position) else { continue; };
            let mut row = StructFields::from([
                ("_id".into(), Value::Utf8(id(name))),
                ("index_name".into(), Value::Utf8(name.clone())),
                ("schema_name".into(), Value::Utf8(table.schema.clone())),
                ("table_name".into(), Value::Utf8(table.table.clone())),
                ("version".into(), Value::I64(generation)),
            ]);
            let columns = &declaration.definition.columns;
            match &declaration.definition.kind {
                RankedIndexKind::Spatial { geometry_encoding } => {
                    row.insert("columns".into(), Value::list(columns.iter().cloned().map(Value::Utf8).collect()));
                    row.insert("geometry_encoding".into(), Value::Utf8(geometry_encoding.clone()));
                }
                RankedIndexKind::Ivf { dimensions, recall_bits } => {
                    let Some((column, partition)) = columns.split_first() else { continue; };
                    row.insert("column_name".into(), Value::Utf8(column.clone()));
                    row.insert("partition_columns".into(), Value::list(partition.iter().cloned().map(Value::Utf8).collect()));
                    row.insert("dimensions".into(), Value::I64(i64::from(*dimensions)));
                    row.insert("method".into(), Value::Utf8("ivf".into()));
                    row.insert("recall".into(), Value::F64(f64::from_bits(*recall_bits)));
                }
                RankedIndexKind::Search { analyzer, history } => {
                    row.insert("columns".into(), Value::list(columns.iter().cloned().map(Value::Utf8).collect()));
                    row.insert("analyzer".into(), Value::Utf8(analyzer.clone()));
                    row.insert("history".into(), Value::Bool(*history));
                }
            }
            buffer.put(TableRef::new("xt", catalog), row, Interval::ALL)?;
        }
    }
    Ok(())
}

fn is_protected(image: &RowImage) -> bool { image.custody_group.is_some() || image.custody_payload.is_some() }

fn withdraw_public(buffer: &mut TransactionBuffer, view: &BranchView<'_>, table: &TableRef, id: &EntityId, iid: Iid,
    valid: Interval, preceding: &[BranchEffect],
) -> Result<()> {
    let mut visible = Vec::new();
    for event in view.history(table, id) { update_public(&mut visible, event.valid, event.image.as_deref())?; }
    for effect in preceding {
        match effect {
            BranchEffect::Write { branch, table: written, event } if branch == MAIN_BRANCH && written == table && &event.id == id => {
                update_public(&mut visible, event.valid, event.image.as_deref())?;
            }
            BranchEffect::Erase { branch, table: written, id: erased }
                if branch.as_deref().is_none_or(|branch| branch == MAIN_BRANCH) && written == table && erased == id => visible.clear(),
            BranchEffect::Declare { table: written, definition } if written == table && definition.dropped => visible.clear(),
            _ => (),
        }
    }
    for overlap in visible.into_iter().filter_map(|range| range.intersection(valid)) {
        append(buffer, table, iid, overlap, StoredOperation::Delete)?;
    }
    Ok(())
}

fn update_public(visible: &mut Vec<Interval>, valid: Interval, image: Option<&RowImage>) -> Result<()> {
    let old = std::mem::take(visible);
    for range in old {
        let Some(overlap) = range.intersection(valid) else { visible.push(range); continue; };
        if range.lower() < overlap.lower() { visible.push(Interval::new(range.lower(), overlap.lower())?); }
        if overlap.upper() < range.upper() { visible.push(Interval::new(overlap.upper(), range.upper())?); }
    }
    if let Some(image) = image.filter(|image| !is_protected(image)) {
        let end = image.expires_at.unwrap_or(valid.upper()).min(valid.upper());
        if valid.lower() < end { visible.push(Interval::new(valid.lower(), end)?); }
    }
    Ok(())
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
    use rtdb_types::{EventOp, Value};

    #[test]
    fn ordered_native_ranked_definitions_stage_their_generations_and_withdraw_on_drop() {
        use rtdb_index::branches::{RankedIndexDeclaration, RankedIndexDefinition};
        use rtdb_tx::transact::Outcome;

        let memory = MemoryBudget::new(1 << 24);
        let partition = rtdb_index::PartitionState::empty(Default::default()).unwrap();
        let table = TableRef::new("public", "places");
        let catalog = TableRef::new("xt", SPATIAL_BUILD_CATALOG);
        let mut index = BranchIndex::new([7; 16]);
        let basis = Basis::new([7; 16], 20);
        let define = BranchEffect::Declare { table: table.clone(), definition: BranchTable {
            ranked_indexes: [
                ("by_geometry", "geometry", RankedIndexKind::Spatial { geometry_encoding: "tagged-wkb-v1".into() }),
                ("by_text", "body", RankedIndexKind::Search { analyzer: "english".into(), history: true }),
                ("by_vector", "embedding", RankedIndexKind::Ivf { dimensions: 3, recall_bits: 0.75f64.to_bits() }),
            ].map(|(name, column, kind)| (name.into(), RankedIndexDeclaration {
                definition: RankedIndexDefinition { columns: vec![column.into(), "region".into()], kind },
                position: basis.position(),
            })).into(),
            ..Default::default()
        }};
        let receipt = |basis, time| Receipt { basis, system_time: Timestamp(time), key: vec![1; 16], outcome: Outcome::Then,
            replayed: false, erasure: None, decision_clock: None, store_reading: None };
        let write = stage_source(partition.snapshot(), &index, SourcePosition(30), &receipt(basis, 10), std::slice::from_ref(&define), &memory).unwrap();
        let batch = write.tables()[&catalog].batch().unwrap();
        let event = batch.view().read_event(0).unwrap();
        let StoredOperation::Put(row) = event.operation else { panic!("expected a build definition"); };
        assert_eq!(row.get("version"), Some(&Value::I64(20)));
        assert_eq!(row.get("columns"), Some(&Value::list(vec![Value::Utf8("geometry".into()), Value::Utf8("region".into())])));
        let iid = event.iid;
        for (catalog, option, expected) in [
            (SEARCH_BUILD_CATALOG, "analyzer", Value::Utf8("english".into())),
            (VECTOR_BUILD_CATALOG, "recall", Value::F64(0.75)),
        ] {
            let event = write.tables()[&TableRef::new("xt", catalog)].batch().unwrap().view().read_event(0).unwrap();
            let StoredOperation::Put(row) = event.operation else { panic!("expected a build definition"); };
            assert_eq!(row.get("version"), Some(&Value::I64(20)));
            assert_eq!(row.get(option), Some(&expected));
        }
        partition.apply(&write).unwrap();
        index.advance(BranchCut { basis, system_time: Timestamp(10) }).unwrap();
        index.apply(define).unwrap();
        let drop = BranchEffect::Declare { table, definition: BranchTable { dropped: true, ..Default::default() } };
        let write = stage_source(partition.snapshot(), &index, SourcePosition(31), &receipt(Basis::new([7; 16], 21), 11), &[drop], &memory).unwrap();
        let event = write.tables()[&catalog].batch().unwrap().view().read_event(0).unwrap();
        assert!(matches!(event.operation, StoredOperation::Delete));
        assert_eq!(event.iid, iid);
        for catalog in [SEARCH_BUILD_CATALOG, VECTOR_BUILD_CATALOG] {
            let event = write.tables()[&TableRef::new("xt", catalog)].batch().unwrap().view().read_event(0).unwrap();
            assert!(matches!(event.operation, StoredOperation::Delete));
        }
    }

    #[test]
    fn a_protected_assertion_only_masks_the_previously_clear_overlap_at_its_stored_iid() {
        use rtdb_index::branches::{BranchEvent, CustodyPayload, WrittenVersion};
        use rtdb_tx::transact::Outcome;

        let memory = MemoryBudget::new(1 << 24);
        let partition = rtdb_index::PartitionState::empty(Default::default()).unwrap();
        let table = TableRef::new("public", "items");
        let mut index = BranchIndex::new([7; 16]);
        advance(&mut index, Timestamp(1)).unwrap();
        let iid = Iid([0x31; 16]);
        let clear = Interval::new(Timestamp(4), Timestamp(8)).unwrap();
        index.apply_classic_event(&table, StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None,
            iid, system_from: Timestamp(1), valid_time: clear, recency: None,
            operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(1))])), }, Some(TxId(1))).unwrap();

        let cut = BranchCut { basis: Basis::new([7; 16], 2), system_time: Timestamp(2) };
        let receipt = Receipt { key: vec![3; 16], basis: cut.basis, system_time: cut.system_time, outcome: Outcome::Then,
            replayed: false, erasure: None, decision_clock: None, store_reading: None };
        let protected = Interval::new(Timestamp(6), Timestamp(10)).unwrap();
        let protect = |id| BranchEffect::Write { branch: MAIN_BRANCH.into(), table: table.clone(), event: BranchEvent {
            id: EntityId::Integer(id), valid: protected, cut, written: WrittenVersion::Branch,
            image: Some(Arc::new(RowImage { cells: StructFields::from([("_id".into(), Value::I64(id))]), expires_at: None,
                custody_group: Some([3; 16]), custody_payload: Some(CustodyPayload { binding: [4; 32], envelope: vec![5] }) })),
        } };
        let write = stage_source(partition.snapshot(), &index, SourcePosition(5), &receipt, &[protect(1), protect(2)], &memory).unwrap();
        let batch = write.tables()[&table].batch().unwrap();
        assert_eq!(batch.len(), 1);
        let header = batch.header(0).unwrap();
        assert_eq!(header.iid, iid);
        assert_eq!(header.op, EventOp::Delete);
        assert_eq!(header.valid_time, clear.intersection(protected).unwrap());
    }

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
        let memory = MemoryBudget::new(1 << 20);
        let types = StructFields::from([("_id".into(), LogicalType::Scalar(rtdb_types::ScalarType::I64))]);
        let arrow = rtdb_format::data::build_event_batch(&[
            event(StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(1))])), Timestamp(1)),
            event(StoredOperation::Delete, Timestamp(1)),
            event(StoredOperation::Erase, Timestamp(2)),
        ], Some(types), None).unwrap();
        let batch = EventBatch::new(arrow).unwrap();
        let mut apply = |row, time, tx| {
            advance(&mut index, Timestamp(time)).unwrap();
            let (event, _charge) = read_event(&batch, row, &memory).unwrap();
            event.apply(&mut index, &table, Some(TxId(tx))).unwrap();
            index.clone()
        };
        let projected = apply(0, 1, 19);
        assert_eq!(projected.physical_iid(&table, &id), iid);
        let cut = projected.head();
        let index = apply(1, 1, 20);
        let root = index.view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(1)).unwrap();
        assert!(root.row(&table, &id, Timestamp(0)).is_none());
        assert_eq!(root.history(&table, &id).len(), 2);
        let index = apply(2, 2, 21);
        let root = index.view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(2)).unwrap();
        assert!(root.history(&table, &id).is_empty());
        assert!(matches!(index.view(MAIN_BRANCH, At::Exactly(cut.basis), Layer::Merged, Timestamp(2)),
            Err(error) if error.code == "rtdb/erased-under-view"));
    }
}
