use async_trait::async_trait;
use arrow_array::{RecordBatch, cast::AsArray, types::{
    TimestampSecondType, TimestampMillisecondType, TimestampMicrosecondType, TimestampNanosecondType,
}};
use arrow_schema::DataType;
use rtdb_columnar::{ArrowCell, MemoryBudget, read_entity_id};
use rtdb_format::data::{StoredEvent, StoredOperation};
use rtdb_tx::{OpenTransaction, TransactionWriter, WriterResult};
use rtdb_types::{Anomaly, EventHeader, EventOp, Iid, Interval, Result, Timestamp, Value};
use std::sync::Arc;
use crate::{ArrowChanges, ArrowPgTransaction, PgTransaction, RowKind, decoder::DecodedTransaction};

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
        index_arrow_runs(source.ordered_runs(), target, memory).await
    }
}

async fn index_arrow_runs<'a>(
    runs: impl Iterator<Item = (&'a ArrowChanges, std::ops::Range<usize>)> + Send,
    target: &mut OpenTransaction<'_>, memory: &Arc<MemoryBudget>,
) -> Result<()> {
    let system_from = target.tx_key().system_time;
    for (index, (operation, rows)) in runs.enumerate() {
        let batch = operation.rows.batch();
        let missing_id = || Anomaly::incorrect("xtdb.postgres/missing-id", "Source row has no _id column");
        let id = batch.schema_ref().index_of("_id").map_err(|_| missing_id())?;
        let from = batch.schema_ref().index_of("_valid_from").ok();
        let to = batch.schema_ref().index_of("_valid_to").ok();
        match operation.kind {
            RowKind::Put => {
                let bytes = rows.len().checked_mul(std::mem::size_of::<EventHeader>())
                    .ok_or_else(|| crate::wire::protocol("PostgreSQL event header allocation overflow"))?;
                let _headers = memory.reserve(bytes)?;
                let mut headers = Vec::with_capacity(rows.len());
                let mut failure = None;
                for row in rows.clone() {
                    let header = put_header(batch, row, id, from, to, system_from);
                    match header {
                        Ok(header) => headers.push(header),
                        Err(error) => { failure = Some(error); break; }
                    }
                    if headers.len() % 64 == 0 { tokio::task::yield_now().await; }
                }
                // Staging the valid prefix lets its failure surface before a later header error.
                if !headers.is_empty() {
                    let (fields, columns): (Vec<_>, Vec<_>) = batch.schema_ref().fields().iter().zip(batch.columns())
                        .filter(|(field, _)| !matches!(field.name().as_str(), "_valid_from" | "_valid_to"))
                        .map(|(field, column)| (Arc::clone(field), column.slice(rows.start, headers.len())))
                        .unzip();
                    let docs = rtdb_columnar::build_struct_array(fields.into(), &columns, headers.len(), None)?;
                    target.append_arrow_puts(&operation.table, docs, &headers)?;
                }
                if let Some(error) = failure { return Err(error); }
            }
            RowKind::Delete => {
                for start in rows.clone().step_by(64) {
                    let len = (rows.end - start).min(64);
                    let iids = (start..start + len).map(|row| cell_iid(cell(batch, id, row)));
                    target.append_arrow_deletes(&operation.table, iids)?;
                    if len == 64 { tokio::task::yield_now().await; }
                }
            }
        }
        if index % 64 == 63 { tokio::task::yield_now().await; }
    }
    Ok(())
}
fn cell(batch: &RecordBatch, column: usize, row: usize) -> ArrowCell<'_> {
    ArrowCell { field: batch.schema_ref().field(column), array: batch.column(column).as_ref(), row }
}

fn cell_iid(cell: ArrowCell<'_>) -> Result<Iid> {
    let cell = cell.physical()?;
    if !cell.is_null()? && matches!(cell.field.data_type(), DataType::Date32 | DataType::Date64) {
        return Err(Anomaly::invalid_entity_id("Invalid ID type: java.time.LocalDate")
            .with_context("type", "java.time.LocalDate".into()));
    }
    Ok(read_entity_id(cell.field, cell.array, cell.row)?.iid())
}

fn put_header(
    batch: &RecordBatch, row: usize, id: usize, from: Option<usize>, to: Option<usize>, system_from: Timestamp,
) -> Result<EventHeader> {
    let iid = cell_iid(cell(batch, id, row))?;
    let lower = from.map(|column| valid_time_cell(cell(batch, column, row), "_valid_from")).transpose()?.flatten();
    let upper = to.map(|column| valid_time_cell(cell(batch, column, row), "_valid_to")).transpose()?.flatten();
    if upper.is_some() && lower.is_none() {
        return Err(Anomaly::incorrect("xtdb.postgres/invalid-explicit-valid-time", "_valid_to requires _valid_from"));
    }
    let valid_time = Interval::new(lower.unwrap_or(system_from), upper.unwrap_or(Timestamp::MAX))?;
    Ok(EventHeader { position: None, commit_position: 0, iid, system_from, valid_time, op: EventOp::Put })
}

fn valid_time_cell(cell: ArrowCell<'_>, name: &str) -> Result<Option<Timestamp>> {
    use arrow_schema::TimeUnit;
    let cell = cell.physical()?;
    if cell.is_null()? { return Ok(None); }
    let DataType::Timestamp(unit, Some(_)) = cell.field.data_type() else { return Err(invalid_valid_time(name)); };
    let (raw, units) = match unit {
        TimeUnit::Second => (cell.array.as_primitive::<TimestampSecondType>().value(cell.row), 1),
        TimeUnit::Millisecond => (cell.array.as_primitive::<TimestampMillisecondType>().value(cell.row), 1_000),
        TimeUnit::Microsecond => (cell.array.as_primitive::<TimestampMicrosecondType>().value(cell.row), 1_000_000),
        TimeUnit::Nanosecond => (cell.array.as_primitive::<TimestampNanosecondType>().value(cell.row), 1_000_000_000),
    };
    let micros = (i128::from(raw) * 1_000_000).div_euclid(units);
    Ok(Some(Timestamp(i64::try_from(micros).map_err(|_| {
        Anomaly::incorrect("xtdb.expression/overflow-error", "data exception - overflow error")
    })?)))
}

fn invalid_valid_time(name: &str) -> Anomaly {
    Anomaly::incorrect("xtdb.postgres/invalid-explicit-valid-time", format!("{name} must be a TIMESTAMPTZ column"))
}

fn valid_time(value: Option<Value>, column: &str) -> Result<Option<Timestamp>> {
    match value { None | Some(Value::Null) => Ok(None), Some(value @ Value::Timestamp { timezone: Some(_), .. }) => Ok(Some(rtdb_expr::timestamp_micros(&value, "UTC")?)), _ => Err(invalid_valid_time(column)) }
}
pub(crate) struct ImportWriter {
    pub transaction: DecodedTransaction,
    pub indexer: Option<Arc<dyn PgIndexer>>,
    pub memory: Arc<MemoryBudget>,
}
#[async_trait]
impl TransactionWriter for ImportWriter {
    async fn write(&mut self, target: &mut OpenTransaction<'_>) -> Result<WriterResult> {
        if self.transaction.commit_time.is_none() { self.transaction.commit_time = Some(target.tx_key().system_time); }
        if let Some(indexer) = &self.indexer {
            let (transaction, _charge) = self.transaction.compatibility_batches(&self.memory)?;
            indexer.index_arrow_transaction(&transaction, target, &self.memory).await?;
        } else {
            index_arrow_runs(self.transaction.ordered_runs(), target, &self.memory).await?;
        }
        Ok(WriterResult::Committed { user_metadata: None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::TimeUnit;

    #[test]
    fn pg_source_arrow_headers_match_the_row_adapter() {
        let values = [
            Value::I16(-7), Value::I32(0), Value::I64(i64::MIN), Value::Utf8("a/id".into()),
            Value::Keyword(rtdb_types::Keyword::from_name("a/id")), Value::Uuid([4; 16]),
            Value::Null, Value::I64(1), Value::DateDay(0),
        ];
        for value in values {
            let logical = value.logical_type();
            let field = rtdb_columnar::to_field("_id", &logical).unwrap();
            let array = rtdb_columnar::copy_array(&logical, std::slice::from_ref(&value)).unwrap();
            let cell = ArrowCell { field: &field, array: array.as_ref(), row: 0 };
            assert_eq!(cell_iid(cell), value.entity_id().map(|id| id.iid()));
        }
        let values = [
            Value::Timestamp { value: -1, unit: TimeUnit::Nanosecond, timezone: Some("UTC".into()) },
            Value::Timestamp { value: i64::MAX, unit: TimeUnit::Second, timezone: Some("UTC".into()) },
            Value::Timestamp { value: 1, unit: TimeUnit::Microsecond, timezone: None },
            Value::Null,
        ];
        for value in values {
            let logical = value.logical_type();
            let field = rtdb_columnar::to_field("_valid_from", &logical).unwrap();
            let array = rtdb_columnar::copy_array(&logical, std::slice::from_ref(&value)).unwrap();
            let cell = ArrowCell { field: &field, array: array.as_ref(), row: 0 };
            assert_eq!(valid_time_cell(cell, "_valid_from"), valid_time(Some(value), "_valid_from"));
        }
    }
}
