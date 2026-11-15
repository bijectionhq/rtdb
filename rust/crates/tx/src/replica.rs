use std::collections::BTreeMap;
use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_format::generated::xtdb::log::proto::ResolvedTx;
use rtdb_index::{IndexBatch, StagedWrite, TableWrite};
use rtdb_types::{Anomaly, BlockIndex, Result, TableRef, Timestamp, TransactionKey, TxId};

use crate::{RetainedAnomaly, TransactionResult};

/// Decodes every table before returning an applyable transaction; caller publishes it atomically.
pub fn decode_replica(message: &ResolvedTx, predecessor: Option<TransactionKey>, block: BlockIndex, memory: &Arc<MemoryBudget>) -> Result<(StagedWrite, TransactionResult)> {
    let key = TransactionKey { tx_id: TxId(message.tx_id.unwrap_or_default()), system_time: Timestamp(message.system_time_micros.unwrap_or_default()) };
    let result = if message.committed.unwrap_or_default() {
        TransactionResult::Committed { tx_key: key }
    } else {
        let error = message.error.as_deref().filter(|error| !error.is_empty()).map(|error| decode_error(error, memory)).transpose()?;
        TransactionResult::Aborted { tx_key: key, error }
    };
    let mut tables = BTreeMap::new();
    for (name, bytes) in &message.table_data {
        let (schema, table) = name.split_once('/').ok_or_else(|| invalid("Replica table must use schema/table"))?;
        if schema.is_empty() || table.is_empty() || table.contains('/') { return Err(invalid("Invalid replica table name")); }
        let pages = crate::ipc::decode(bytes, memory)?;
        if pages.len() != 1 { return Err(invalid("Resolved transaction table must contain exactly one record batch")); }
        let batch = IndexBatch::from_owned(pages.into_iter().next().ok_or_else(|| invalid("Replica table has no batch"))?)?;
        tables.insert(TableRef::new(schema, table), TableWrite::new(BTreeMap::new(), Some(batch)));
    }
    Ok((StagedWrite::new(key, predecessor, block, tables)?, result))
}

fn decode_error(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<RetainedAnomaly> {
    let _reservation = memory.reserve(bytes.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("Transit error allocation overflow"))?)?;
    RetainedAnomaly::from_transit(bytes, memory)
}
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::fault("rtdb.tx/invalid-replica-transaction", message) }
