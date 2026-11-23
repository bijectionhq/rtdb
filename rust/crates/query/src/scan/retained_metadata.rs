use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rtdb_columnar::MemoryReservation;
use rtdb_index::{IndexBatch, Snapshot};
use rtdb_plan::{Catalog, ColumnId, Expr, Field, QualifiedTable, Scan, TemporalFilter};
use rtdb_temporal::ResolvedRow;
use rtdb_types::{Iid, Result, TableRef, TimeUnit, Value};

use crate::columnar::Column;
use super::{PartitionScan, ScanCursor, invalid, read_match_input};

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
        let mut charge = memory.reserve(rows.len().saturating_mul(256).saturating_add(4096))?;
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
        while let Some(page) = Box::pin(retained.next_batch()).await? {
            for row in 0..page.len {
                let Value::I64(id) = page.columns[0].value(row)? else { continue; };
                reserve_value(&mut charge, page.columns[1].bound(row)?)?;
                if payloads.insert(id, page.columns[1].value(row)?).is_some() {
                    return Err(invalid("transaction-metadata", "Multiple retained values for one transaction"));
                }
            }
        }
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            let batch = &batches[row.source.0 as usize];
            let offset = row.source.1 as usize;
            let Value::I64(id) = read_match_input(batch, offset, "_id")? else {
                return Err(invalid("transaction-metadata", "The transaction history has no transaction id"));
            };
            if let Some(value) = payloads.get(&id) {
                reserve_value(&mut charge, value.retained_bytes()?)?;
                values.push(value.clone());
            } else {
                reserve_value(&mut charge, super::read_match_input_bound(batch, offset, "user_metadata")?)?;
                values.push(read_match_input(batch, offset, "user_metadata")?);
            }
        }
        Ok(Some((Column::Values(values), charge)))
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

fn reserve_value(charge: &mut Arc<MemoryReservation>, bytes: usize) -> Result<()> {
    let lease = Arc::get_mut(charge).ok_or_else(|| invalid("transaction-metadata", "The projection budget is shared"))?;
    let bound = lease.bytes().checked_add(bytes).ok_or_else(|| invalid("transaction-metadata", "The projection exceeds addressable memory"))?;
    lease.grow_to(bound)
}
