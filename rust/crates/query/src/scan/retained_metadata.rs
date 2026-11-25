use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::StructArray;
use rtdb_columnar::{MemoryBudget, MemoryReservation, estimate_read_cell_bytes, read_execution_value, read_execution_value_as, validate_readable_cell};
use rtdb_index::{IndexBatch, Snapshot};
use rtdb_plan::{Catalog, ColumnId, Expr, Field, QualifiedTable, Scan, TemporalFilter};
use rtdb_temporal::ResolvedRow;
use rtdb_types::{Iid, LogicalType, Result, TableRef, TimeUnit, Value};

use crate::columnar::{Column, FundedArrowCell, SelectedColumn, stable_read_layout};
use super::{PartitionScan, ScanCursor, invalid};

fn sidecar() -> TableRef { TableRef::new("xt", "idempotency_receipts") }

pub(super) fn is_projected(scan: &Scan) -> bool {
    scan.table.name.table == TableRef::new("xt", "txs")
        && scan.columns.iter().any(|field| field.name == "user_metadata")
}

pub(super) fn has_sidecar(snapshot: &Snapshot) -> bool {
    snapshot.column_names(&sidecar()).contains("user_metadata")
}

impl PartitionScan {
    pub(super) async fn retained_transaction_metadata(
        &mut self, rows: &[ResolvedRow<(u32, u32)>], batches: &[Arc<IndexBatch>],
    ) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
        if !is_projected(&self.scan) || !has_sidecar(&self.snapshot) { return Ok(None); }
        let Some(head) = self.snapshot.tx_basis() else { return Ok(None); };
        let memory = &self.execution.context.memory;
        let mut inventory = reserve_rows(memory, rows.len(), 256)?;
        let snapshot = self.snapshot.clone().with_system_time_cap(Some(head.system_time))?;
        let time = if let Some(time) = &self.metadata_time {
            time.clone()
        } else {
            let time = self.transaction_metadata_time(&snapshot, head).await?;
            self.metadata_time = Some(time.clone());
            time
        };
        let Some(time) = time else { return Ok(None); };

        let iids = rows.iter().map(|row| batches[row.source.0 as usize].header(row.source.1 as usize).map(|header| header.iid))
            .collect::<Result<Vec<_>>>()?;
        let Some(mut retained) = self.retention_scan(
            &snapshot, sidecar(), Some(iids.iter().copied().collect()), &["_id", "user_metadata"],
            TemporalFilter::At(Expr::Literal(time)),
        ).await? else { return Ok(None); };

        let mut payloads = BTreeMap::new();
        let mut receipts = Vec::new();
        while let Some(page) = Box::pin(retained.next_batch()).await? {
            let source = receipts.len();
            let mut keep = false;
            for row in 0..page.len {
                let Value::I64(id) = page.columns[0].value(row)? else { continue; };
                page.columns[1].validate_readable_cell(row, memory)?;
                reserve_more(&mut inventory, 128)?;
                if payloads.insert(id, (source, row)).is_some() {
                    return Err(invalid("transaction-metadata", "Multiple retained values for one transaction"));
                }
                keep = true;
            }
            if keep {
                reserve_more(&mut inventory, 256)?;
                let owner = Arc::clone(page.column_reservation(1));
                let column = page.columns.into_iter().nth(1).expect("The receipt scan projects metadata");
                receipts.push((column, owner));
            }
        }
        drop(retained);

        let mut cells = Vec::with_capacity(rows.len());
        for row in rows {
            let batch = &batches[row.source.0 as usize];
            let offset = row.source.1 as usize;
            let Value::I64(id) = read_event_cell(event_cell(batch, offset, "_id")?)? else {
                return Err(invalid("transaction-metadata", "The transaction history has no transaction id"));
            };
            if let Some(&(source, row)) = payloads.get(&id) {
                let (column, owner) = &receipts[source];
                cells.push(MetadataCell::Receipt { column, owner, row });
            } else {
                cells.push(MetadataCell::Original(event_cell(batch, offset, "user_metadata")?));
            }
        }
        project_metadata(&cells, memory).map(Some)
    }

    async fn transaction_metadata_time(
        &self, snapshot: &Snapshot, head: rtdb_types::TransactionKey,
    ) -> Result<Option<Value>> {
        let clock_id = rtdb_types::EntityId::Integer(head.tx_id.0).iid();
        for iids in [Some([clock_id].into()), None] {
            let complete_history = iids.is_none();
            let Some(mut clock) = self.retention_scan(
                snapshot, TableRef::new("xt", "txs"), iids, &["system_time", "receipt_time"], TemporalFilter::All,
            ).await? else { return Ok(None); };
            let mut exact = None;
            let mut prior = None;
            while let Some(page) = Box::pin(clock.next_batch()).await? {
                for row in 0..page.len {
                    let time = page.columns[0].value(row)?;
                    let Value::Timestamp { value, unit: TimeUnit::Microsecond, .. } = time else {
                        return Err(invalid("transaction-metadata", "A transaction has no system time"));
                    };
                    if value > head.system_time.0 || (!complete_history && value != head.system_time.0) { continue; }
                    let observed = match page.columns[1].value(row)? {
                        Value::Timestamp { value, unit: TimeUnit::Microsecond, .. } => Some(value),
                        Value::Null | Value::Undefined => None,
                        _ => return Err(invalid("transaction-metadata", "The retained clock has no timestamp")),
                    };
                    if value == head.system_time.0 {
                        if exact.replace(observed).is_some() {
                            return Err(invalid("transaction-metadata", "Multiple clocks for the captured transaction"));
                        }
                    } else if let Some(observed) = observed {
                        prior = Some(prior.map_or(observed, |prior: i64| prior.max(observed)));
                    }
                }
            }
            let observed = exact.flatten();
            if observed.is_some() || complete_history {
                return Ok(observed.or(prior).map(|value| Value::Timestamp {
                    value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
                }));
            }
        }
        Err(invalid("transaction-metadata", "The retained clock cannot be matched to the captured transaction"))
    }

    async fn retention_scan(
        &self, snapshot: &Snapshot, table: TableRef, iids: Option<BTreeSet<Iid>>,
        names: &[&str], valid_time: TemporalFilter,
    ) -> Result<Option<ScanCursor>> {
        let qualified = QualifiedTable { database: self.scan.table.name.database.clone(), table };
        let Some(table) = self.execution.context.catalog.table(&qualified)? else { return Ok(None); };
        let columns = names.iter().enumerate().map(|(index, name)| Field {
            id: ColumnId(index as u32), name: (*name).to_owned(),
            qualifier: None, qualified_only: false, unnamed: false, data_type: table.scan_column_type(name),
        }).collect();
        let scan = Scan {
            table, columns, valid_time, system_time: TemporalFilter::Default, clamp_valid_time: false,
            iids, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new(),
        };
        ScanCursor::over(scan, self.execution.clone(), None, std::slice::from_ref(snapshot), &self.storage, false, None).await.map(Some)
    }
}

#[derive(Clone, Copy)]
enum MetadataCell<'a> {
    Receipt { column: &'a Column, owner: &'a Arc<MemoryReservation>, row: usize },
    Original(Option<FundedArrowCell<'a>>),
}

fn project_metadata(cells: &[MetadataCell<'_>], memory: &Arc<MemoryBudget>) -> Result<(Column, Arc<MemoryReservation>)> {
    if let Some(projected) = borrow_metadata(cells, memory)? { return Ok(projected); }

    let mut charge = reserve_rows(memory, cells.len(), size_of::<Value>())?;
    let mut values = Vec::with_capacity(cells.len());
    for cell in cells.iter().copied() {
        let bound = match cell {
            MetadataCell::Receipt { column, row, .. } => column.bound(row)?,
            MetadataCell::Original(cell) => cell.map_or(Ok(0), |cell| match cell.logical {
                Some(_) => estimate_read_cell_bytes(cell.field, cell.array.as_ref(), cell.row),
                None => rtdb_columnar::estimate_read_value_bytes(cell.field, cell.array.as_ref(), cell.row),
            })?,
        };
        reserve_more(&mut charge, bound)?;
        values.push(match cell {
            MetadataCell::Receipt { column, row, .. } => column.value(row)?,
            MetadataCell::Original(cell) => read_event_cell(cell)?,
        });
    }
    settle_metadata(Column::Values(values), charge)
}

fn borrow_metadata(cells: &[MetadataCell<'_>], memory: &Arc<MemoryBudget>)
    -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let mut charge = reserve_rows(memory, cells.len(), size_of::<(usize, usize)>())?;
    let mut positions = Vec::with_capacity(cells.len());
    let mut sources = vec![None];
    let mut inputs = HashMap::new();
    for cell in cells {
        let position = match *cell {
            MetadataCell::Receipt { column, owner, row } => column.funded_arrow_cell(row, owner),
            MetadataCell::Original(cell) => Some(cell),
        };
        let (array, field, logical, owner, row) = match position {
            Some(Some(FundedArrowCell { array, field, logical: Some(logical), owner, row })) => (array, field, logical, owner, row),
            Some(None) => {
                positions.push((0, 0));
                continue;
            }
            _ => return Ok(None),
        };
        let key = (Arc::as_ptr(array).cast::<()>(), Arc::as_ptr(field), std::ptr::from_ref(logical), Arc::as_ptr(owner));
        let source = if let Some(&source) = inputs.get(&key) {
            source
        } else {
            let schema = field.size().checked_mul(8).ok_or_else(projection_size)?;
            let _layout = memory.reserve(schema)?;
            if *logical == LogicalType::Nothing || !stable_read_layout(logical, field) { return Ok(None); }
            reserve_more(&mut charge, logical.retained_bytes()?.checked_add(256).ok_or_else(projection_size)?)?;
            let source = sources.len();
            sources.push(Some((Arc::clone(array), Arc::clone(field), Some(logical.clone()), Some(Arc::clone(owner)))));
            inputs.insert(key, source);
            source
        };
        if matches!(cell, MetadataCell::Original(_)) {
            validate_readable_cell(field, array.as_ref(), logical, row, memory)?;
        }
        positions.push((source, row));
    }
    let column = Column::Selected(SelectedColumn::new(sources, Arc::new(positions)));
    settle_metadata(column, charge).map(Some)
}

fn settle_metadata(column: Column, mut charge: Arc<MemoryReservation>) -> Result<(Column, Arc<MemoryReservation>)> {
    let bytes = column.retained_bytes()?;
    let lease = Arc::get_mut(&mut charge).expect("The metadata projection owns its reservation");
    if bytes <= lease.bytes() { lease.shrink_to(bytes)?; } else { lease.grow_to(bytes)?; }
    Ok((column, charge))
}

fn event_cell<'a>(batch: &'a IndexBatch, row: usize, name: &str) -> Result<Option<FundedArrowCell<'a>>> {
    let op = batch.view().operations()?;
    let put = op.child(op.type_id(row)).as_any().downcast_ref::<StructArray>()
        .ok_or_else(|| invalid("event-schema", "Visible event is not a put struct"))?;
    Ok(put.fields().iter().enumerate().find(|(_, field)| field.name() == name).map(|(index, field)| FundedArrowCell {
        array: put.column(index), field, logical: batch.put_field_type(index), owner: batch.arrow_reservation(), row: op.value_offset(row),
    }))
}

fn read_event_cell(cell: Option<FundedArrowCell<'_>>) -> Result<Value> {
    let Some(FundedArrowCell { array, field, logical, row, .. }) = cell else { return Ok(Value::Null); };
    match logical {
        Some(logical) => read_execution_value_as(logical, field, array.as_ref(), row),
        None => read_execution_value(field, array.as_ref(), row),
    }
}

fn projection_size() -> rtdb_types::Anomaly { invalid("transaction-metadata", "The projection exceeds addressable memory") }

fn reserve_rows(memory: &Arc<MemoryBudget>, len: usize, cell_bytes: usize) -> Result<Arc<MemoryReservation>> {
    let bound = len.checked_mul(cell_bytes).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(projection_size)?;
    memory.reserve(bound)
}

fn reserve_more(charge: &mut Arc<MemoryReservation>, bytes: usize) -> Result<()> {
    let lease = Arc::get_mut(charge).ok_or_else(|| invalid("transaction-metadata", "The projection budget is shared"))?;
    let bound = lease.bytes().checked_add(bytes).ok_or_else(projection_size)?;
    lease.grow_to(bound)
}

#[test]
fn metadata_value_fallback_coerces_legacy_unsigned_cells_and_owns_its_charge() {
    use arrow_array::{ArrayRef, UInt64Array};
    use rtdb_types::ScalarType;

    let memory = MemoryBudget::new(1 << 20);
    let logical = LogicalType::Scalar(ScalarType::U64);
    let field = Arc::new(rtdb_columnar::to_field("user_metadata", &logical).unwrap());
    let array: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX]));
    let owner = memory.reserve(array.get_array_memory_size()).unwrap();
    let input = FundedArrowCell { array: &array, field: &field, logical: Some(&logical), owner: &owner, row: 0 };

    let (column, charge) = project_metadata(&[MetadataCell::Original(Some(input)), MetadataCell::Original(None)], &memory).unwrap();
    drop((array, owner));
    assert!(matches!(&column, Column::Values(values) if values.as_slice() == [Value::I64(-1), Value::Null]));
    assert_eq!(memory.used_bytes(), charge.bytes());
    assert_eq!(charge.bytes(), column.retained_bytes().unwrap());
}
