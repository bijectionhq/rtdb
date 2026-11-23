use rtdb_columnar::read_value;
use rtdb_types::{Anomaly, AnomalyCategory, PlacedAttemptScope, Result, StructFields, TimeUnit, Timestamp, TransactionKey, TxId, Value};
use sha2::{Digest, Sha256};

use crate::bounded_retry::{BoundedRetry, bounded_receipts_table, duplicate};
use crate::idempotency_window::{BoundedWindow, bounded_window};
use crate::{SourceTransaction, resolver::{OpenTransaction, interval, time_value}};

pub(super) struct RetryScope(String);

impl RetryScope {
    pub(super) fn new(placed: Option<PlacedAttemptScope>) -> Self {
        let scope = placed.map_or_else(|| "classic".into(), |placed| {
            placed.history_fingerprint().iter().map(|byte| format!("{byte:02x}")).collect()
        });
        Self(scope)
    }

    fn receipt_id(&self, principal: Option<&str>, retry: BoundedRetry) -> String {
        let mut digest = Sha256::new();
        digest.update(b"rtdb/bounded-retry-id/v1\0");
        digest.update(self.0.as_bytes());
        digest.update(retry.nonce().as_bytes());
        digest.update(retry.cutoff().0.to_be_bytes());
        if let Some(principal) = principal {
            digest.update([1]);
            digest.update(principal.as_bytes());
        } else {
            digest.update([0]);
        }
        format!("receipt/v1/{:x}", digest.finalize())
    }
}

pub(super) struct BoundedReceipt {
    id: String,
    digest: String,
    cutoff: Timestamp,
}

impl OpenTransaction<'_> {
    pub(super) async fn admit_bounded_retry(
        &self, scope: &RetryScope, request: &SourceTransaction, retry: BoundedRetry, observed: Timestamp,
    ) -> Result<Result<Option<BoundedReceipt>>> {
        let cutoff = retry.cutoff();
        match bounded_window(observed.0, cutoff.0) {
            BoundedWindow::Expired => return Ok(Err(Anomaly::incorrect("rtdb/idempotency-expired", "The bounded retry identity reached its exclusive cutoff"))),
            BoundedWindow::TooLong => return Ok(Err(Anomaly::incorrect("rtdb/idempotency-window", "The bounded retry deadline is more than an hour after admission"))),
            BoundedWindow::Open => {}
        }
        let id = scope.receipt_id(request.options.user.as_deref(), retry);
        let digest = match request.body_digest() {
            Ok(digest) => digest,
            Err(error) if error.is_caller_error() => return Ok(Err(error)),
            Err(error) => return Err(error),
        };
        if self.snapshot()?.contains_table(&bounded_receipts_table()) {
            let mut query = self.open_query(
                "SELECT digest, tx_id, tx_system_time FROM xt.bounded_retry_receipts FOR ALL VALID_TIME WHERE _id = ?",
                vec![Value::Utf8(id.clone())],
            ).await?;
            while let Some(batch) = query.next_batch().await? {
                let batch = batch.batch();
                if batch.num_rows() == 0 { continue; }
                let cell = |index: usize| read_value(batch.schema().field(index), batch.column(index).as_ref(), 0);
                let (Value::Utf8(stored), Value::I64(tx_id), Value::Timestamp { value: system_time, unit: TimeUnit::Microsecond, .. }) =
                    (cell(0)?, cell(1)?, cell(2)?) else {
                        return Err(Anomaly::fault("rtdb.tx/bounded-retry-receipt", "A bounded retry receipt has no digest or original transaction key"));
                    };
                let original = TransactionKey { tx_id: TxId(tx_id), system_time: Timestamp(system_time) };
                let error = if stored == digest { duplicate(original) } else {
                    Anomaly::new(AnomalyCategory::Conflict, "rtdb/idempotency-conflict", "This bounded retry identity already names another transaction")
                };
                return Ok(Err(error));
            }
        }
        Ok(Ok(Some(BoundedReceipt { id, digest, cutoff })))
    }

    pub(super) fn write_bounded_receipt(&mut self, receipt: BoundedReceipt, observed: Timestamp) -> Result<()> {
        let key = self.tx_key();
        let row = StructFields::from([
            ("_id".into(), Value::Utf8(receipt.id)),
            ("digest".into(), Value::Utf8(receipt.digest)),
            ("tx_id".into(), Value::I64(key.tx_id.0)),
            ("tx_system_time".into(), time_value(key.system_time)),
        ]);
        self.buffer.put(bounded_receipts_table(), row, interval(observed, receipt.cutoff)?)
    }

}
