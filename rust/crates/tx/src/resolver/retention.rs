use rtdb_columnar::read_value;
use rtdb_types::{Anomaly, Result, TimeUnit, TxId, Timestamp, Value};

use super::OpenTransaction;
use crate::idempotency_window::advance_retry_clock;

#[derive(Clone, Copy)]
pub(super) struct RetentionClock {
    pub(super) time: Timestamp,
    previous: Option<Timestamp>,
}

impl RetentionClock {
    fn observe(previous: Option<Timestamp>, appended: Timestamp) -> Self {
        Self { time: Timestamp(advance_retry_clock(previous.unwrap_or(appended).0, appended.0)), previous }
    }

    fn collect_at(self, tx: TxId) -> bool {
        const SECOND: i64 = 1_000_000;
        tx.0.rem_euclid(64) == 0
            || self.previous.is_some_and(|previous| previous.0.div_euclid(SECOND) < self.time.0.div_euclid(SECOND))
    }
}

impl OpenTransaction<'_> {
    /// The predecessor belongs to this installed history, including any staged transactions.
    pub(super) async fn observe_retention_clock(&self, appended: Timestamp) -> Result<RetentionClock> {
        let snapshot = self.buffer.base_snapshot();
        let Some(predecessor) = snapshot.tx_basis() else { return Ok(RetentionClock::observe(None, appended)); };
        if !snapshot.column_names(&rtdb_types::TableRef::new("xt", "txs")).contains("receipt_time") {
            return Ok(RetentionClock::observe(None, appended));
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
                Value::Timestamp { value, unit: TimeUnit::Microsecond, .. } => Ok(RetentionClock::observe(Some(Timestamp(value)), appended)),
                Value::Null | Value::Undefined => Ok(RetentionClock::observe(None, appended)),
                _ => Err(Anomaly::fault("rtdb.tx/retention-clock", "The durable receipt clock has no timestamp")),
            };
        }
        Err(Anomaly::fault("rtdb.tx/retention-clock", "The ordered transaction predecessor is missing"))
    }
}

impl OpenTransaction<'_> {
    pub(super) async fn historical_guard_original(&self, key: &str, submitted_at: Timestamp) -> Result<Option<rtdb_types::TransactionKey>> {
        let markers = rtdb_types::TableRef::new("public", "rtdb_idempotency_keys");
        if !self.snapshot()?.contains_table(&markers) { return Ok(None); }
        let mut cursor = self.open_query(
            super::HISTORICAL_KEYED_TRANSACTION_SQL,
            vec![super::time_value(self.tx_key().system_time), super::time_value(submitted_at), Value::Utf8(key.into())],
        ).await?;
        while let Some(page) = cursor.next_batch().await? {
            let page = page.batch();
            if page.num_rows() == 0 { continue; }
            let cell = |index| read_value(page.schema().field(index), page.column(index).as_ref(), 0);
            let (Value::I64(id), Value::Timestamp { value, unit: TimeUnit::Microsecond, .. }) = (cell(0)?, cell(1)?) else {
                return Err(Anomaly::fault("rtdb.tx/legacy-guard-original", "The duplicate guard has no matching original transaction key"));
            };
            return Ok(Some(rtdb_types::TransactionKey { tx_id: TxId(id), system_time: Timestamp(value) }));
        }
        Ok(None)
    }

    /// Progressively replaces expired payloads with committed erase events.
    /// Reused legacy keys are grouped by their newest interval before erasure.
    pub(super) async fn retire_expired_receipts(&mut self, clock: RetentionClock) -> Result<()> {
        if !clock.collect_at(self.tx_key().tx_id) { return Ok(()); }
        let observed = clock.time;
        let receipts = super::receipts_table();
        if self.snapshot()?.contains_table(&receipts) {
            self.retire_receipt_rows(
                receipts,
                "SELECT _id FROM xt.idempotency_receipts FOR ALL VALID_TIME GROUP BY _id HAVING MAX(_valid_to) <= ? ORDER BY _id LIMIT 128",
                observed,
            ).await?;
        }
        let bounded = crate::bounded_retry::bounded_receipts_table();
        if self.snapshot()?.contains_table(&bounded) {
            self.retire_receipt_rows(
                bounded,
                "SELECT _id FROM xt.bounded_retry_receipts FOR ALL VALID_TIME WHERE _valid_to <= ? ORDER BY _id LIMIT 128",
                observed,
            ).await?;
        }
        let private = crate::replay_submission::replay_table();
        if self.snapshot()?.contains_table(&private) {
            self.retire_receipt_rows(
                private,
                "SELECT _id FROM xt.rtdb_replay_keys FOR ALL VALID_TIME GROUP BY _id HAVING MAX(_valid_to) <= ? ORDER BY _id LIMIT 128",
                observed,
            ).await?;
        }
        Ok(())
    }

    async fn retire_receipt_rows(&mut self, table: rtdb_types::TableRef, sql: &str, observed: Timestamp) -> Result<()> {
        let mut cursor = self.open_query(sql, vec![super::time_value(observed)]).await?;
        while let Some(page) = cursor.next_batch().await? {
            let page = page.batch();
            for row in 0..page.num_rows() {
                let id = read_value(page.schema().field(0), page.column(0).as_ref(), row)?;
                self.buffer.erase(table.clone(), &id.entity_id()?)?;
            }
        }
        Ok(())
    }
}
