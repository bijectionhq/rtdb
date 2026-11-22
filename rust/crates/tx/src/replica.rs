use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_format::{data::StoredOperation, generated::xtdb::log::proto::ResolvedTx};
use rtdb_index::{IndexBatch, StagedWrite, TableWrite};
use rtdb_types::{Anomaly, BlockIndex, EventOp, Interval, PLACED_ATTEMPT_OUTCOME_TABLE, PlacedAttempt, Result, TableRef, Timestamp, TransactionKey, TxId, Value};

use crate::{RetainedAnomaly, TransactionResult};

pub struct ReplayedTransaction {
    pub writes: StagedWrite,
    pub result: TransactionResult,
    pub placed_attempt: Option<PlacedAttempt>,
}

/// Decodes the complete transaction for atomic application by the caller.
pub fn decode_replica(message: &ResolvedTx, predecessor: Option<TransactionKey>, block: BlockIndex, memory: &Arc<MemoryBudget>) -> Result<ReplayedTransaction> {
    let placed_attempt = if let Some(placed) = message.placed_attempt.as_deref() {
        let attempt = PlacedAttempt::decode(placed).map_err(|_| invalid("Invalid placed attempt identity"))?;
        let position = Some(attempt.position().0);
        if message.src_msg_id != position || message.tx_id != position
            || !message.table_data.contains_key(&format!("xt/{PLACED_ATTEMPT_OUTCOME_TABLE}"))
        {
            return Err(invalid("Placed attempt and transaction outcome disagree"));
        }
        Some(attempt)
    } else { None };
    let key = TransactionKey { tx_id: TxId(message.tx_id.unwrap_or_default()), system_time: Timestamp(message.system_time_micros.unwrap_or_default()) };
    let result = if message.committed.unwrap_or_default() {
        TransactionResult::Committed { tx_key: key }
    } else {
        let error = message.error.as_deref().filter(|error| !error.is_empty()).map(|error| decode_error(error, memory)).transpose()?;
        TransactionResult::Aborted { tx_key: key, error }
    };
    let mut tables = BTreeMap::new();
    let mut owned_namespace = None;
    for (name, bytes) in &message.table_data {
        let (schema, physical_name) = name.split_once('/').ok_or_else(|| invalid("Replica table must use schema/table"))?;
        if schema.is_empty() || physical_name.is_empty() || physical_name.contains('/') { return Err(invalid("Invalid replica table name")); }
        let table = TableRef::new(schema, physical_name);
        if TableRef::owned_schema(schema) {
            if !table.supported_owned_name() {
                return Err(invalid("Unknown owned relation policy or invalid owned relation name"));
            }
            let namespace = (&physical_name[..64], table.owned_world_scope());
            if owned_namespace.is_some_and(|seen| seen != namespace) {
                return Err(invalid("Resolved transaction writes more than one owner or world scope"));
            }
            owned_namespace = Some(namespace);
        }
        let pages = crate::ipc::decode(bytes, memory)?;
        if pages.len() != 1 { return Err(invalid("Resolved transaction table must contain exactly one record batch")); }
        let batch = IndexBatch::from_owned(pages.into_iter().next().ok_or_else(|| invalid("Replica table has no batch"))?)?;
        tables.insert(table, TableWrite::new(StructFields::new(), Some(batch)));
    }
    if let Some((_, owned_scope)) = owned_namespace {
        if !result.committed() || message.db_op.is_some() {
            return Err(invalid("Resolved owned transaction mixes storage scopes"));
        }
        let controls: Vec<_> = tables
            .iter()
            .filter(|(table, _)| {
                !TableRef::owned_schema(&table.schema)
                    && !(table.schema == "xt"
                        && matches!(
                            table.table.as_str(),
                            "txs" | "idempotency_receipts" | PLACED_ATTEMPT_OUTCOME_TABLE
                        ))
            })
            .collect();
        if !controls.is_empty() {
            let Some(attempt) = placed_attempt.filter(|_| owned_scope == (false, None)) else {
                return Err(invalid(
                    "Resolved hosted controls require one placed owned root",
                ));
            };
            verify_hosted_controls(&tables, &controls, attempt, key, memory)?;
        }
    }
    Ok(ReplayedTransaction { writes: StagedWrite::new(key, predecessor, block, tables)?, result, placed_attempt })
}

fn verify_hosted_controls(
    tables: &BTreeMap<TableRef, TableWrite>,
    controls: &[(&TableRef, &TableWrite)],
    attempt: PlacedAttempt,
    key: TransactionKey,
    memory: &Arc<MemoryBudget>,
) -> Result<()> {
    use crate::hosted_control::{CONTROL_TABLES, admits_name};

    let mismatch =
        || invalid("Resolved hosted controls differ from their ordered attempt inventory");
    let outcome = tables
        .get(&TableRef::new("xt", PLACED_ATTEMPT_OUTCOME_TABLE))
        .and_then(TableWrite::batch)
        .filter(|batch| batch.len() == 1)
        .ok_or_else(mismatch)?;
    let _decode = memory.reserve(
        outcome
            .arrow()
            .get_array_memory_size()
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(mismatch)?,
    )?;
    let event = outcome.view().read_event(0)?;
    let StoredOperation::Put(proof) = event.operation else {
        return Err(mismatch());
    };
    if event.system_from != key.system_time
        || !matches!(proof.get("_id"), Some(Value::Utf8(id)) if id == &attempt.outcome_key())
        || proof.get("tx_id") != Some(&Value::I64(key.tx_id.0))
    {
        return Err(mismatch());
    }
    let Some(Value::List(names)) = proof.get(CONTROL_TABLES) else {
        return Err(mismatch());
    };
    if names.len() != controls.len() || names.len() > 32 {
        return Err(mismatch());
    }
    let mut expected = BTreeSet::new();
    for name in names {
        let Value::Utf8(name) = name else {
            return Err(mismatch());
        };
        if !admits_name(name) || !expected.insert(name.as_str()) {
            return Err(mismatch());
        }
    }
    let valid = Interval::new(Timestamp(0), Timestamp::MAX)?;
    for (table, rows) in controls {
        if table.schema != "public" || !expected.remove(table.table.as_str()) {
            return Err(mismatch());
        }
        let batch = rows
            .batch()
            .filter(|batch| !batch.is_empty())
            .ok_or_else(mismatch)?;
        for index in 0..batch.len() {
            let row = batch.header(index)?;
            if row.op != EventOp::Put
                || row.system_from != key.system_time
                || row.valid_time != valid
            {
                return Err(mismatch());
            }
        }
    }
    if !expected.is_empty() {
        return Err(mismatch());
    }
    Ok(())
}

fn decode_error(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<RetainedAnomaly> {
    let _reservation = memory.reserve(bytes.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("Transit error allocation overflow"))?)?;
    RetainedAnomaly::from_transit(bytes, memory)
}
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::fault("rtdb.tx/invalid-replica-transaction", message) }
