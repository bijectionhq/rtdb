//! A source transaction that failed for a reason other than the caller's, with what the reference's `CrashLogger.withCrashLog`
//! is given when the failure came from a put, delete or erase.

use std::sync::Arc;

use rtdb_index::{LiveTable, TransactionBuffer};
use rtdb_types::{Anomaly, Result, TableRef};

use crate::TxOp;
use crate::source::Operation;
use crate::values::ArrowDocumentWrite;

/// The error that stopped resolution, and the document write it stopped in.
#[derive(Debug)]
pub struct SourceFailure {
    pub error: Anomaly,
    /// Present only for a put, delete or erase that failed for a reason other than the caller's.
    pub operation: Option<FailedOperation>,
}

/// The reference's `withCrashLog` arguments for the failed operation.
#[derive(Debug)]
pub struct FailedOperation {
    pub table: TableRef,
    /// `error putting documents`, `error deleting documents` or `error erasing documents`.
    pub message: &'static str,
    /// The failed transaction's own segment of the table, the reference's `OpenTx.Table`, or why it could not be read.
    pub open_tx: Result<Arc<LiveTable>>,
}

impl From<Anomaly> for SourceFailure {
    fn from(error: Anomaly) -> Self { Self { error, operation: None } }
}

impl From<SourceFailure> for Anomaly {
    fn from(failure: SourceFailure) -> Self { failure.error }
}

impl std::fmt::Display for SourceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.error.fmt(f) }
}

/// The table a put, delete or erase writes, and the message the reference's crash-log wrapper around it gives.
/// Its patch path has no such wrapper.
pub(super) fn document_write(operation: &Operation) -> Option<(&TableRef, &'static str)> {
    match operation {
        Operation::Public(TxOp::DeleteDocs { table, .. }) => Some((table, "error deleting documents")),
        Operation::Public(TxOp::EraseDocs { table, .. }) => Some((table, "error erasing documents")),
        Operation::Public(TxOp::InsertDocs { table, .. } | TxOp::PutDocs { table, .. } | TxOp::PutArrowDocs { table, .. } | TxOp::WriteArrowDocs { table, write: ArrowDocumentWrite::Put, .. })
        | Operation::InsertIids { table, .. } | Operation::InsertArrowIids { table, .. } | Operation::PutIids { table, .. } | Operation::PutReplayArrow { table, .. } => Some((table, "error putting documents")),
        _ => None,
    }
}

/// The failed put, delete or erase, read from the transaction as the operation left it; a caller's error is rethrown by the reference
/// without a crash log, so it names no operation.
pub(super) fn failed_operation(buffer: &TransactionBuffer, operation: &Operation, error: &Anomaly) -> Option<FailedOperation> {
    if error.is_caller_error() { return None; }
    let (table, message) = document_write(operation)?;
    Some(FailedOperation { table: table.clone(), message, open_tx: buffer.transaction_segment(table) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_columnar::MemoryBudget;
    use rtdb_types::{AnomalyCategory, EntityId, Interval, StructFields, Timestamp, TransactionKey, TxId, Value};

    #[test]
    fn a_failed_document_write_names_its_table_message_and_the_transactions_own_rows() {
        let memory = MemoryBudget::new(1 << 24);
        let state = rtdb_index::PartitionState::empty(Default::default()).unwrap();
        let key = TransactionKey { tx_id: TxId(1), system_time: Timestamp(10) };
        let table = TableRef::new("public", "docs");
        let mut buffer = TransactionBuffer::new(state.snapshot(), key, Arc::clone(&memory)).unwrap();
        for id in 0..2 {
            buffer.put(table.clone(), StructFields::from([("_id".into(), Value::I64(id))]), Interval::ALL).unwrap();
        }
        let ids = || crate::EntityIds::new(vec![EntityId::Integer(0)], &memory).unwrap();
        let fault = Anomaly::fault("rtdb.live/append", "Live append failed");

        let delete = Operation::Public(TxOp::DeleteDocs { table: table.clone(), ids: ids(), valid_from: None, valid_to: None });
        let failed = failed_operation(&buffer, &delete, &fault).expect("a delete that faults names its operation");
        assert_eq!((&failed.table, failed.message), (&table, "error deleting documents"));
        let open_tx = failed.open_tx.unwrap();
        assert_eq!((open_tx.table(), open_tx.row_count()), (&table, 2));

        let erase = Operation::Public(TxOp::EraseDocs { table: table.clone(), ids: ids() });
        assert_eq!(failed_operation(&buffer, &erase, &fault).unwrap().message, "error erasing documents");

        let other = TableRef::new("public", "other");
        let documents = crate::Documents::new(vec![StructFields::from([("_id".into(), Value::I64(5))])], &memory).unwrap();
        let put = Operation::Public(TxOp::PutDocs { table: other.clone(), documents, valid_from: None, valid_to: None });
        let failed = failed_operation(&buffer, &put, &Anomaly::new(AnomalyCategory::Interrupted, "rtdb/interrupted", "Interrupted")).unwrap();
        assert_eq!((&failed.table, failed.message), (&other, "error putting documents"));
        assert_eq!(failed.open_tx.unwrap().row_count(), 0, "a table the transaction has not written has an empty segment");

        assert!(failed_operation(&buffer, &delete, &Anomaly::incorrect("xtdb/invalid-id", "Invalid ID")).is_none(),
            "a caller's error is not crash-logged");
        let sql = Operation::Public(TxOp::Sql { sql: "INSERT INTO docs (_id) VALUES (3)".into(), args: None });
        assert!(failed_operation(&buffer, &sql, &fault).is_none(), "only a put, delete or erase is wrapped");
    }

    #[test]
    fn a_source_failure_is_its_error_to_callers_that_take_an_anomaly() {
        let error = Anomaly::fault("rtdb.live/append", "Live append failed");
        let failure = SourceFailure::from(error.clone());
        assert!(failure.operation.is_none());
        assert_eq!(Anomaly::from(failure), error);
    }
}
