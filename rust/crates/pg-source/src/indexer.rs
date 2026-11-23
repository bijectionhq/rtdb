use async_trait::async_trait;
use rtdb_format::data::{StoredEvent, StoredOperation};
use rtdb_tx::{OpenTransaction, TransactionWriter, WriterResult};
use rtdb_types::{Anomaly, Interval, Result, Timestamp, Value};
use std::sync::Arc;
use crate::{PgTransaction, RowKind};

#[async_trait]
pub trait PgIndexer: Send + Sync {
    /// The returned future owns all indexing work and must release its resources when the leader term drops it.
    async fn index_transaction(&self, source: &PgTransaction, target: &mut OpenTransaction<'_>) -> Result<()>;
}
pub struct DirectMirror;
#[async_trait]
impl PgIndexer for DirectMirror {
    async fn index_transaction(&self, source: &PgTransaction, target: &mut OpenTransaction<'_>) -> Result<()> {
        for (index, operation) in source.operations.iter().enumerate() {
            let id = operation.row.get("_id").ok_or_else(|| Anomaly::incorrect("xtdb.postgres/missing-id", "Source row has no _id column"))?.entity_id()?;
            let system_from = target.tx_key().system_time;
            let (valid_time, operation_value) = match operation.kind {
                RowKind::Put => {
                    let mut document = operation.row.clone();
                    let lower = valid_time(document.remove("_valid_from"), "_valid_from")?;
                    let upper = valid_time(document.remove("_valid_to"), "_valid_to")?;
                    if upper.is_some() && lower.is_none() { return Err(Anomaly::incorrect("xtdb.postgres/invalid-explicit-valid-time", "_valid_to requires _valid_from")); }
                    (Interval::new(lower.unwrap_or(system_from), upper.unwrap_or(Timestamp::MAX))?, StoredOperation::Put(document))
                }
                RowKind::Delete => (Interval::new(system_from, Timestamp::MAX)?, StoredOperation::Delete),
            };
            target.append(operation.table.clone(), StoredEvent { commit_position: 0, entity_key: None, iid: id.iid(), system_from, valid_time, operation: operation_value, recency: None })?;
            if index % 64 == 63 { tokio::task::yield_now().await; }
        }
        Ok(())
    }
}
fn valid_time(value: Option<Value>, column: &str) -> Result<Option<Timestamp>> {
    match value { None | Some(Value::Null) => Ok(None), Some(value @ Value::Timestamp { timezone: Some(_), .. }) => Ok(Some(rtdb_expr::timestamp_micros(&value, "UTC")?)), _ => Err(Anomaly::incorrect("xtdb.postgres/invalid-explicit-valid-time", format!("{column} must be a TIMESTAMPTZ column"))) }
}
pub(crate) struct ImportWriter { pub transaction: PgTransaction, pub indexer: Arc<dyn PgIndexer> }
#[async_trait]
impl TransactionWriter for ImportWriter {
    async fn write(&mut self, target: &mut OpenTransaction<'_>) -> Result<WriterResult> {
        if self.transaction.commit_time.is_none() { self.transaction.commit_time = Some(target.tx_key().system_time); }
        self.indexer.index_transaction(&self.transaction, target).await?;
        Ok(WriterResult::Committed { user_metadata: None })
    }
}
