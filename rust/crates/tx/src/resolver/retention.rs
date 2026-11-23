use rtdb_columnar::read_value;
use rtdb_types::{Anomaly, Result, TimeUnit, Timestamp, Value};

use super::OpenTransaction;
use crate::idempotency_window::advance_retry_clock;

impl OpenTransaction<'_> {
    /// The predecessor belongs to this installed history, including any staged transactions.
    pub(super) async fn observe_retention_clock(&self, appended: Timestamp) -> Result<Timestamp> {
        let snapshot = self.buffer.base_snapshot();
        let Some(predecessor) = snapshot.tx_basis() else { return Ok(appended); };
        if !snapshot.column_names(&rtdb_types::TableRef::new("xt", "txs")).contains("receipt_time") {
            return Ok(appended);
        }
        let mut cursor = self.open_query(
            "SELECT receipt_time FROM xt.txs FOR ALL VALID_TIME WHERE _id = ?",
            vec![Value::I64(predecessor.tx_id.0)],
        ).await?;
        while let Some(page) = cursor.next_batch().await? {
            let batch = page.batch();
            if batch.num_rows() == 0 { continue; }
            let value = read_value(batch.schema().field(0), batch.column(0).as_ref(), 0)?;
            if batch.num_rows() != 1 {
                return Err(Anomaly::fault("rtdb.tx/retention-clock", "The durable receipt clock has multiple predecessors"));
            }
            return match value {
                Value::Timestamp { value, unit: TimeUnit::Microsecond, .. } => Ok(Timestamp(advance_retry_clock(value, appended.0))),
                Value::Null | Value::Undefined => Ok(appended),
                _ => Err(Anomaly::fault("rtdb.tx/retention-clock", "The durable receipt clock has no timestamp")),
            };
        }
        Err(Anomaly::fault("rtdb.tx/retention-clock", "The ordered transaction predecessor is missing"))
    }
}
