use std::{collections::BTreeMap, sync::Arc};

use rtdb_columnar::MemoryBudget;
use rtdb_index::{
    LiveIndex, Snapshot, StoredEvent, StoredOperation, TableCatalog, TransactionBuffer, TrieCatalog,
    branches::{BranchCut, BranchView, RankedIndexKind, RowImage},
};
use rtdb_trie::TrieConfig;
use rtdb_types::{
    Anomaly, BlockIndex, EntityId, Interval, LogicalType, Result, ScalarType, StructFields, TableRef, Timestamp,
    TransactionKey, TxId, Value,
    rtdb_v1::{Basis, WRITTEN_BASIS_COLUMN},
};

pub fn branch_snapshot(view: &BranchView<'_>, memory: &Arc<MemoryBudget>) -> Result<Snapshot> {
    branch_snapshot_with(view, memory, |image| {
        if image.custody_payload.is_some() {
            return Err(Anomaly::unsupported("rtdb/row-custody", "The query route has no protected value reader"));
        }
        Ok(image.cells.clone())
    })
}

pub fn branch_snapshot_with(
    view: &BranchView<'_>, memory: &Arc<MemoryBudget>,
    mut open_image: impl FnMut(&RowImage) -> Result<StructFields<Value>>,
) -> Result<Snapshot> {
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
                    target.push((table, id, Interval::new(event.valid.lower(), put_until)?, event.cut, Some((image.as_ref(), event.written_basis()))));
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
            let operation = if let Some((image, written)) = image {
                let mut cells = open_image(image)?;
                cells.insert(WRITTEN_BASIS_COLUMN.into(), written.map_or(Value::Null, basis_value));
                StoredOperation::Put(cells)
            } else { StoredOperation::Delete };
            buffer.append(table.clone(), StoredEvent {
                position: None, origin: None, commit_position: 0, entity_key: None, iid: view.physical_iid(table, id),
                system_from: system_time, valid_time, operation, recency: None,
            })?;
        }
        if basis == head.basis {
            for (table, definition) in view.tables() {
                let mut columns = definition.columns.clone();
                columns.insert(WRITTEN_BASIS_COLUMN.into(), LogicalType::Scalar(ScalarType::Binary));
                buffer.declare_table(table.clone(), columns.keys().cloned())?;
                buffer.declare_document_types(table, &columns)?;
            }
            append_lineage(view, &mut buffer)?;
            append_ordered_indexes(view, &mut buffer)?;
            append_ranked_indexes(view, &mut buffer)?;
            append_count_constraints(view, &mut buffer)?;
            append_custody_inventory(view, &mut buffer)?;
            append_custody_placements(view, &mut buffer)?;
        }
        live = Arc::new(live.apply(&buffer.stage()?)?);
    }
    Ok(Snapshot::new(tries, tables, live))
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

fn append_ordered_indexes(view: &BranchView<'_>, buffer: &mut TransactionBuffer) -> Result<()> {
    let catalog = TableRef::new("xt", "ordered_indexes");
    let text = LogicalType::Scalar(ScalarType::Utf8);
    let columns = StructFields::from([
        ("_id".into(), text.clone()),
        ("schema_name".into(), text.clone()),
        ("table_name".into(), text.clone()),
        ("columns".into(), LogicalType::list(text)),
        ("version".into(), LogicalType::Scalar(ScalarType::I64)),
    ]);
    buffer.declare_table(catalog.clone(), columns.keys().cloned())?;
    buffer.declare_document_types(&catalog, &columns)?;
    for (table, definition) in view.tables() {
        for index in definition.indexes.values() {
            let basis = view.index_version(table, index).ok_or_else(|| {
                Anomaly::fault("rtdb/index-version", "The index has no declaration at the selected cut")
            })?;
            let cells = StructFields::from([
                ("_id".into(), Value::Utf8(index.name.clone())),
                ("schema_name".into(), Value::Utf8(table.schema.clone())),
                ("table_name".into(), Value::Utf8(table.table.clone())),
                ("columns".into(), Value::list(index.columns().map(|name| Value::Utf8(name.into())).collect())),
                ("version".into(), Value::I64(tx_key(BranchCut { basis, system_time: view.cut().system_time })?.tx_id.0)),
            ]);
            buffer.put(catalog.clone(), cells, Interval::ALL)?;
        }
    }
    Ok(())
}

fn append_count_constraints(view: &BranchView<'_>, buffer: &mut TransactionBuffer) -> Result<()> {
    let catalog = TableRef::new("xt", "count_constraints");
    let text = LogicalType::Scalar(ScalarType::Utf8);
    let binary = LogicalType::Scalar(ScalarType::Binary);
    let columns = StructFields::from([
        ("_id".into(), text.clone()),
        ("schema_name".into(), text.clone()),
        ("table_name".into(), text.clone()),
        ("constraint_name".into(), text.clone()),
        ("index_name".into(), text.clone()),
        ("group_column".into(), text.clone()),
        ("predicate_columns".into(), LogicalType::list(text)),
        ("predicate_values".into(), LogicalType::list(binary.clone())),
        ("prefix_bytes".into(), LogicalType::Scalar(ScalarType::U64)),
        ("lower_key".into(), binary.clone()),
        ("upper_key".into(), binary),
        ("valid_at_micros".into(), LogicalType::Scalar(ScalarType::I64)),
        ("maximum".into(), LogicalType::Scalar(ScalarType::U64)),
    ]);
    buffer.declare_table(catalog.clone(), columns.keys().cloned())?;
    buffer.declare_document_types(&catalog, &columns)?;
    for (table, name, range) in view.count_constraints() {
        let mut cells = count_constraint_row(table, name, range.valid_at, range.n);
        cells.insert("index_name".into(), Value::Utf8(range.index.clone()));
        cells.insert("lower_key".into(), range.lower.clone().map_or(Value::Null, Value::Binary));
        cells.insert("upper_key".into(), range.upper.clone().map_or(Value::Null, Value::Binary));
        buffer.put(catalog.clone(), cells, Interval::ALL)?;
    }
    for (table, name, rule) in view.grouped_constraints() {
        let mut cells = count_constraint_row(table, name, rule.valid_at, rule.n);
        cells.insert("group_column".into(), Value::Utf8(rule.column.clone()));
        cells.insert("prefix_bytes".into(), rule.prefix_bytes.map_or(Value::Null, |width| Value::U64(width.into())));
        cells.insert("predicate_columns".into(), Value::list(rule.equals.keys().cloned().map(Value::Utf8).collect()));
        cells.insert("predicate_values".into(), Value::list(rule.equals.values().cloned().map(Value::Binary).collect()));
        buffer.put(catalog.clone(), cells, Interval::ALL)?;
    }
    Ok(())
}

fn append_ranked_indexes(view: &BranchView<'_>, buffer: &mut TransactionBuffer) -> Result<()> {
    let catalog = TableRef::new("xt", "ranked_indexes");
    let search = TableRef::new("xt", "search_indexes");
    let text = LogicalType::Scalar(ScalarType::Utf8);
    let integer = LogicalType::Scalar(ScalarType::I64);
    let boolean = LogicalType::Scalar(ScalarType::Bool);
    let shared = StructFields::from([
        ("_id".into(), text.clone()),
        ("schema_name".into(), text.clone()),
        ("table_name".into(), text.clone()),
        ("columns".into(), LogicalType::list(text.clone())),
        ("version".into(), integer.clone()),
    ]);
    let mut search_columns = shared.clone();
    search_columns.insert("analyzer".into(), text.clone());
    search_columns.insert("history".into(), boolean.clone());
    let mut catalog_columns = search_columns.clone();
    for field in ["kind", "geometry_encoding", "method", "serving_mode"] {
        catalog_columns.insert(field.into(), text.clone());
    }
    catalog_columns.insert("dimensions".into(), integer);
    catalog_columns.insert("recall".into(), LogicalType::Scalar(ScalarType::F64));
    catalog_columns.insert("artefacts_ready".into(), boolean);
    buffer.declare_table(catalog.clone(), catalog_columns.keys().cloned())?;
    buffer.declare_document_types(&catalog, &catalog_columns)?;
    for (table, definition) in view.tables() {
        for (name, declaration) in &definition.ranked_indexes {
            let ranked = &declaration.definition;
            let version = i64::try_from(declaration.position)
                .map_err(|_| Anomaly::unsupported("rtdb/query-basis", "SQL does not support this declaration position"))?;
            let mut row = StructFields::from([
                ("_id".into(), Value::Utf8(name.clone())),
                ("schema_name".into(), Value::Utf8(table.schema.clone())),
                ("table_name".into(), Value::Utf8(table.table.clone())),
                ("columns".into(), Value::list(ranked.columns.iter().cloned().map(Value::Utf8).collect())),
                ("version".into(), Value::I64(version)),
            ]);
            let kind = match &ranked.kind {
                RankedIndexKind::Search { analyzer, history } => {
                    row.insert("analyzer".into(), Value::Utf8(analyzer.clone()));
                    row.insert("history".into(), Value::Bool(*history));
                    buffer.put(search.clone(), row.clone(), Interval::ALL)?;
                    "search"
                }
                RankedIndexKind::Spatial { geometry_encoding } => {
                    row.insert("geometry_encoding".into(), Value::Utf8(geometry_encoding.clone()));
                    "spatial"
                }
                RankedIndexKind::Ivf { dimensions, recall_bits } => {
                    row.insert("method".into(), Value::Utf8("ivf".into()));
                    row.insert("dimensions".into(), Value::I64(i64::from(*dimensions)));
                    row.insert("recall".into(), Value::F64(f64::from_bits(*recall_bits)));
                    "vector"
                }
            };
            row.insert("kind".into(), Value::Utf8(kind.into()));
            row.insert("serving_mode".into(), Value::Utf8("exact-sql".into()));
            row.insert("artefacts_ready".into(), Value::Bool(false));
            buffer.put(catalog.clone(), row, Interval::ALL)?;
        }
    }
    Ok(())
}

fn count_constraint_row(table: &TableRef, name: &str, valid_at: Option<Timestamp>, maximum: u64) -> StructFields<Value> {
    let id = format!("{}:{}{}:{}{name}", table.schema.len(), table.schema, table.table.len(), table.table);
    StructFields::from([
        ("_id".into(), Value::Utf8(id)),
        ("schema_name".into(), Value::Utf8(table.schema.clone())),
        ("table_name".into(), Value::Utf8(table.table.clone())),
        ("constraint_name".into(), Value::Utf8(name.into())),
        ("valid_at_micros".into(), valid_at.map_or(Value::Null, |at| Value::I64(at.0))),
        ("maximum".into(), Value::U64(maximum)),
    ])
}

fn append_custody_inventory(view: &BranchView<'_>, buffer: &mut TransactionBuffer) -> Result<()> {
    let catalog = TableRef::new("xt", "custody_groups");
    let binary = LogicalType::Scalar(ScalarType::Binary);
    let columns = StructFields::from([
        ("_id".into(), LogicalType::Scalar(ScalarType::Utf8)),
        ("inventory_sha256".into(), binary.clone()),
        ("placements".into(), LogicalType::Scalar(ScalarType::U64)),
        ("revoked_at".into(), binary),
    ]);
    buffer.declare_table(catalog.clone(), columns.keys().cloned())?;
    buffer.declare_document_types(&catalog, &columns)?;
    for entry in view.custody_inventories() {
        let cells = StructFields::from([
            ("_id".into(), Value::Utf8(format!("{:032x}", u128::from_be_bytes(entry.group)))),
            ("inventory_sha256".into(), Value::Binary(entry.sha256.to_vec())),
            ("placements".into(), Value::U64(entry.placements)),
            ("revoked_at".into(), entry.revoked_at.map_or(Value::Null, basis_value)),
        ]);
        buffer.put(catalog.clone(), cells, Interval::ALL)?;
    }
    Ok(())
}

fn append_custody_placements(view: &BranchView<'_>, buffer: &mut TransactionBuffer) -> Result<()> {
    let catalog = TableRef::new("xt", "custody_placements");
    let binary = LogicalType::Scalar(ScalarType::Binary);
    let columns = StructFields::from([
        ("_id".into(), LogicalType::Scalar(ScalarType::Utf8)),
        ("group_id".into(), binary.clone()),
        ("enrolled_basis".into(), binary.clone()),
        ("reservation_sha256".into(), binary.clone()),
        ("occurrence_sha256".into(), binary.clone()),
        ("carrier_sha256".into(), binary.clone()),
        ("written_basis".into(), binary),
        ("ordinal".into(), LogicalType::Scalar(ScalarType::U64)),
    ]);
    buffer.declare_table(catalog.clone(), columns.keys().cloned())?;
    buffer.declare_document_types(&catalog, &columns)?;
    let binary = |bytes: [u8; 32]| Value::Binary(bytes.to_vec());
    for entry in view.custody_placements() {
        let cells = StructFields::from([
            ("_id".into(), Value::Utf8(format!("{:032x}", u128::from_be_bytes(entry.placement_id)))),
            ("group_id".into(), Value::Binary(entry.group.to_vec())),
            ("enrolled_basis".into(), basis_value(entry.enrolled)),
            ("reservation_sha256".into(), entry.reservation.map_or(Value::Null, binary)),
            ("occurrence_sha256".into(), entry.occurrence.map_or(Value::Null, |occurrence| binary(occurrence.commitment(entry.group)))),
            ("carrier_sha256".into(), entry.occurrence.map_or(Value::Null, |occurrence| binary(occurrence.carrier_sha256))),
            ("written_basis".into(), entry.occurrence.map_or(Value::Null, |occurrence| basis_value(occurrence.written))),
            ("ordinal".into(), entry.occurrence.map_or(Value::Null, |occurrence| Value::U64(occurrence.ordinal))),
        ]);
        buffer.put(catalog.clone(), cells, Interval::ALL)?;
    }
    Ok(())
}

fn basis_value(basis: Basis) -> Value { Value::Binary(basis.to_bytes().to_vec()) }
