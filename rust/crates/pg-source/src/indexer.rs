use async_trait::async_trait;
use arrow_array::RecordBatch;
use rtdb_columnar::MemoryBudget;
use rtdb_format::data::{StoredEvent, StoredOperation};
use rtdb_tx::{OpenTransaction, TransactionWriter, WriterResult};
use rtdb_types::{Anomaly, EventHeader, EventOp, Interval, Result, Timestamp, Value};
use std::sync::Arc;
use crate::{ArrowPgTransaction, PgTransaction, RowKind};

#[async_trait]
pub trait PgIndexer: Send + Sync {
    /// The returned future owns all indexing work and must release its resources when the leader term drops it.
    async fn index_transaction(&self, source: &PgTransaction, target: &mut OpenTransaction<'_>) -> Result<()>;

    /// Indexes the PostgreSQL wire decoder's batches.
    /// Existing indexers that accept row maps receive them here at the extension boundary.
    async fn index_arrow_transaction(
        &self, source: &ArrowPgTransaction, target: &mut OpenTransaction<'_>, memory: &Arc<MemoryBudget>,
    ) -> Result<()> {
        self.index_transaction(&source.to_rows(memory)?, target).await
    }
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
            target.append(operation.table.clone(), StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid: id.iid(), system_from, valid_time, operation: operation_value, recency: None })?;
            if index % 64 == 63 { tokio::task::yield_now().await; }
        }
        Ok(())
    }

    async fn index_arrow_transaction(
        &self, source: &ArrowPgTransaction, target: &mut OpenTransaction<'_>, memory: &Arc<MemoryBudget>,
    ) -> Result<()> {
        let system_from = target.tx_key().system_time;
        for (index, operation) in source.batches.iter().enumerate() {
            let batch = operation.rows.batch();
            let missing_id = || Anomaly::incorrect("xtdb.postgres/missing-id", "Source row has no _id column");
            let id = batch.schema_ref().index_of("_id").map_err(|_| missing_id())?;
            let from = batch.schema_ref().index_of("_valid_from").ok();
            let to = batch.schema_ref().index_of("_valid_to").ok();
            match operation.kind {
                RowKind::Put => {
                    let bytes = batch.num_rows().checked_mul(std::mem::size_of::<EventHeader>())
                        .ok_or_else(|| crate::wire::protocol("PostgreSQL event header allocation overflow"))?;
                    let _headers = memory.reserve(bytes)?;
                    let mut headers = Vec::with_capacity(batch.num_rows());
                    for row in 0..batch.num_rows() {
                        let iid = read_cell(batch, id, row, memory, |value| Ok(value.entity_id()?.iid()))?;
                        let lower = from.map(|column| read_cell(batch, column, row, memory, |value| valid_time(Some(value), "_valid_from"))).transpose()?.flatten();
                        let upper = to.map(|column| read_cell(batch, column, row, memory, |value| valid_time(Some(value), "_valid_to"))).transpose()?.flatten();
                        if upper.is_some() && lower.is_none() {
                            return Err(Anomaly::incorrect("xtdb.postgres/invalid-explicit-valid-time", "_valid_to requires _valid_from"));
                        }
                        let valid_time = Interval::new(lower.unwrap_or(system_from), upper.unwrap_or(Timestamp::MAX))?;
                        headers.push(EventHeader { position: None, commit_position: 0, iid, system_from, valid_time, op: EventOp::Put });
                        if row % 64 == 63 { tokio::task::yield_now().await; }
                    }
                    let (fields, columns): (Vec<_>, Vec<_>) = batch.schema_ref().fields().iter().zip(batch.columns())
                        .filter(|(field, _)| !matches!(field.name().as_str(), "_valid_from" | "_valid_to"))
                        .map(|(field, column)| (Arc::clone(field), Arc::clone(column)))
                        .unzip();
                    let docs = rtdb_columnar::build_struct_array(fields.into(), &columns, batch.num_rows(), None)?;
                    target.append_arrow_puts(&operation.table, docs, &headers)?;
                }
                RowKind::Delete => {
                    for row in 0..batch.num_rows() {
                        let iid = read_cell(batch, id, row, memory, |value| Ok(value.entity_id()?.iid()))?;
                        let valid_time = Interval::new(system_from, Timestamp::MAX)?;
                        target.append(operation.table.clone(), StoredEvent { position: None,
                            origin: None,
                            commit_position: 0,
                            entity_key: None, iid, system_from, valid_time, operation: StoredOperation::Delete, recency: None,
                        })?;
                    }
                }
            }
            if index % 64 == 63 { tokio::task::yield_now().await; }
        }
        Ok(())
    }
}
fn read_cell<T>(batch: &RecordBatch, column: usize, row: usize, memory: &Arc<MemoryBudget>,
    consume: impl FnOnce(Value) -> Result<T>,
) -> Result<T> {
    let field = batch.schema_ref().field(column);
    let array = batch.column(column).as_ref();
    let bytes = rtdb_columnar::estimate_read_value_bytes(field, array, row)?.checked_mul(3)
        .ok_or_else(|| crate::wire::protocol("PostgreSQL header cell allocation overflow"))?;
    let _scratch = memory.reserve(bytes)?;
    consume(rtdb_columnar::read_value(field, array, row)?)
}

fn valid_time(value: Option<Value>, column: &str) -> Result<Option<Timestamp>> {
    match value { None | Some(Value::Null) => Ok(None), Some(value @ Value::Timestamp { timezone: Some(_), .. }) => Ok(Some(rtdb_expr::timestamp_micros(&value, "UTC")?)), _ => Err(Anomaly::incorrect("xtdb.postgres/invalid-explicit-valid-time", format!("{column} must be a TIMESTAMPTZ column"))) }
}
pub(crate) struct ImportWriter {
    pub transaction: ArrowPgTransaction,
    pub indexer: Arc<dyn PgIndexer>,
    pub memory: Arc<MemoryBudget>,
}
#[async_trait]
impl TransactionWriter for ImportWriter {
    async fn write(&mut self, target: &mut OpenTransaction<'_>) -> Result<WriterResult> {
        if self.transaction.commit_time.is_none() { self.transaction.commit_time = Some(target.tx_key().system_time); }
        self.indexer.index_arrow_transaction(&self.transaction, target, &self.memory).await?;
        Ok(WriterResult::Committed { user_metadata: None })
    }
}
