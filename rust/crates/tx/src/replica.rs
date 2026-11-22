use rtdb_types::StructFields;
use std::collections::BTreeMap;
use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_format::generated::xtdb::log::proto::ResolvedTx;
use rtdb_index::{IndexBatch, StagedWrite, TableWrite};
use rtdb_types::{Anomaly, BlockIndex, PLACED_ATTEMPT_OUTCOME_TABLE, PlacedAttempt, Result, TableRef, Timestamp, TransactionKey, TxId};

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
    if owned_namespace.is_some() && (!result.committed() || message.db_op.is_some() || tables.keys().any(|table| {
        !TableRef::owned_schema(&table.schema)
            && !(table.schema == "xt" && matches!(table.table.as_str(), "txs" | "idempotency_receipts" | PLACED_ATTEMPT_OUTCOME_TABLE))
    })) {
        return Err(invalid("Resolved owned transaction mixes storage scopes"));
    }
    Ok(ReplayedTransaction { writes: StagedWrite::new(key, predecessor, block, tables)?, result, placed_attempt })
}

fn decode_error(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<RetainedAnomaly> {
    let _reservation = memory.reserve(bytes.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("Transit error allocation overflow"))?)?;
    RetainedAnomaly::from_transit(bytes, memory)
}
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::fault("rtdb.tx/invalid-replica-transaction", message) }
