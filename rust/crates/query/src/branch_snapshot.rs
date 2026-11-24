use std::{collections::BTreeMap, sync::Arc};

use rtdb_columnar::MemoryBudget;
use rtdb_index::{
    LiveIndex, Snapshot, StoredEvent, StoredOperation, TableCatalog, TransactionBuffer, TrieCatalog,
    branches::{BranchCut, BranchView},
};
use rtdb_trie::TrieConfig;
use rtdb_types::{
    Anomaly, BlockIndex, EntityId, Interval, Result, StructFields, TableRef, Timestamp,
    TransactionKey, TxId, Value,
    rtdb_v1::{Basis, Claim, ClaimScope, ClaimSealer},
};

pub fn branch_snapshot(view: &BranchView<'_>, memory: &Arc<MemoryBudget>) -> Result<Snapshot> {
    let tables = Arc::new(TableCatalog::default());
    let tries = Arc::new(TrieCatalog::default());
    let mut live = Arc::new(LiveIndex::new(BlockIndex(0), None, TrieConfig::default()));
    let mut events = BTreeMap::new();
    for (table, _) in view.tables() {
        for id in view.ids(table) {
            for event in view.history(table, id) {
                let target: &mut Vec<_> = events.entry(event.cut.basis).or_default();
                let put_until = event.image.as_ref().map(|image| {
                    image.expires_at.unwrap_or(event.valid.upper()).clamp(event.valid.lower(), event.valid.upper())
                }).unwrap_or(event.valid.lower());
                if let Some(image) = &event.image
                    && put_until > event.valid.lower()
                {
                    target.push((table, id, Interval::new(event.valid.lower(), put_until)?, event.cut, Some(&image.cells)));
                }
                if put_until < event.valid.upper() {
                    target.push((table, id, Interval::new(put_until, event.valid.upper())?, event.cut, None));
                }
            }
        }
    }

    let head = view.cut();
    events.entry(head.basis).or_default();
    for (basis, events) in events {
        let before = Snapshot::new(Arc::clone(&tries), Arc::clone(&tables), Arc::clone(&live));
        let system_time = events.first().map_or(head.system_time, |(_, _, _, cut, _)| cut.system_time);
        let mut buffer = TransactionBuffer::new(before, tx_key(BranchCut { basis, system_time })?, Arc::clone(memory))?;
        for (table, id, valid_time, _, image) in events {
            let operation = image.map_or(StoredOperation::Delete, |cells| StoredOperation::Put(cells.clone()));
            buffer.append(table.clone(), StoredEvent {
                position: None, origin: None, commit_position: 0, entity_key: None, iid: id.iid(),
                system_from: system_time, valid_time, operation, recency: None,
            })?;
        }
        if basis == head.basis {
            for (table, definition) in view.tables() {
                buffer.declare_table(table.clone(), definition.columns.keys().cloned())?;
                buffer.declare_document_types(table, &definition.columns)?;
            }
            append_lineage(view, &mut buffer)?;
        }
        live = Arc::new(live.apply(&buffer.stage()?)?);
    }
    Ok(Snapshot::new(tries, tables, live))
}

pub fn seal_branch_claim(view: &BranchView<'_>, claims: &ClaimSealer) -> Result<Claim> {
    claims.seal(&ClaimScope { branch: view.branch().into(), basis: view.cut().basis, layer: view.layer() })
}

fn tx_key(cut: BranchCut) -> Result<TransactionKey> {
    let position = i64::try_from(cut.basis.position())
        .map_err(|_| Anomaly::unsupported("rtdb/query-basis", "SQL does not support this journal position"))?;
    Ok(TransactionKey { tx_id: TxId(position), system_time: cut.system_time })
}

fn append_lineage(view: &BranchView<'_>, buffer: &mut TransactionBuffer) -> Result<()> {
    let table = TableRef::new("xt", "branch_lineage");
    for row in view.lineage() {
        let (parent, base) = row.base.map_or((Value::Null, Value::Null), |(branch, base)| {
            (Value::Utf8(branch), basis_value(base))
        });
        let id = EntityId::String(row.branch.clone());
        let cells = StructFields::from([
            ("_id".into(), id.as_value()),
            ("branch".into(), Value::Utf8(row.branch)),
            ("base_branch".into(), parent),
            ("base_basis".into(), base),
            ("created_basis".into(), basis_value(row.created)),
            ("writable".into(), Value::Bool(row.writable)),
            ("expires_at".into(), row.expires_at.map_or(Value::Null, |at: Timestamp| Value::I64(at.0))),
            ("dropped_basis".into(), row.dropped.map_or(Value::Null, basis_value)),
        ]);
        buffer.put(table.clone(), cells, Interval::ALL)?;
    }
    Ok(())
}

fn basis_value(basis: Basis) -> Value { Value::Binary(basis.to_bytes().to_vec()) }
