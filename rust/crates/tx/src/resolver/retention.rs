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
        let context = self.context.query_context(snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
        let previous = installed_receipt_time(context, predecessor).await?;
        Ok(RetentionClock::observe(previous, appended))
    }
}

/// The installed receipt clock belongs to an exact transaction position.
/// A fork can give a copied replica frame a new source ID while preserving its stored history row.
pub async fn installed_receipt_time(
    context: std::sync::Arc<rtdb_query::QueryContext>, head: rtdb_types::TransactionKey,
) -> Result<Option<Timestamp>> {
    use rtdb_types::TableRef;

    let database = rtdb_plan::Catalog::default_database(&context.catalog);
    let [snapshot] = context.catalog.database(database)?.partitions.as_slice() else {
        return Err(clock_error("The installed receipt clock requires one partition"));
    };
    if !snapshot.column_names(&TableRef::new("xt", "txs")).contains("receipt_time") { return Ok(None); }
    let system_time = || super::time_value(head.system_time);
    let point = read_receipt_clock_row(&context,
        "SELECT receipt_time FROM xt.txs FOR ALL VALID_TIME WHERE _id = ? AND system_time = ?",
        vec![Value::I64(head.tx_id.0), system_time()],
    ).await?;
    let exact = match point {
        Some(clock) => Some(clock),
        None => read_receipt_clock_row(&context,
            "SELECT receipt_time FROM xt.txs FOR ALL VALID_TIME WHERE system_time = ?",
            vec![system_time()],
        ).await?,
    };
    if let Some(Some(exact)) = exact { return Ok(Some(exact)); }

    // Legacy replay can advance the installed head without a transaction row carrying the clock.
    Ok(read_receipt_clock_row(&context,
        "SELECT MAX(receipt_time) FROM xt.txs FOR ALL VALID_TIME WHERE system_time <= ?",
        vec![system_time()],
    ).await?.flatten())
}

async fn read_receipt_clock_row(
    context: &std::sync::Arc<rtdb_query::QueryContext>, sql: &str, arguments: Vec<Value>,
) -> Result<Option<Option<Timestamp>>> {
    use rtdb_query::PreparedQuery;

    let options = rtdb_sql::PrepareOptions {
        parameter_types: arguments.iter().enumerate().map(|(slot, value)| (slot, value.logical_type())).collect(),
        default_timezone: context.timezone.clone(),
        generate_series_end_exclusive: context.options.generate_series_end_exclusive,
        ..Default::default()
    };
    let prepared = rtdb_sql::prepare_in(sql, &context.catalog, &options, &context.memory)?;
    let mut cursor = PreparedQuery::from_prepared(prepared, &context.memory)?.execute(context.clone(), arguments).await?;
    let mut observed = None;
    while let Some(page) = cursor.next_batch().await? {
        let batch = page.batch();
        if batch.num_rows() == 0 { continue; }
        if batch.num_rows() != 1 || observed.is_some() {
            return Err(clock_error("The durable receipt clock has multiple predecessors"));
        }
        let value = read_value(batch.schema().field(0), batch.column(0).as_ref(), 0)?;
        observed = Some(match value {
            Value::Timestamp { value, unit: TimeUnit::Microsecond, .. } => Some(Timestamp(value)),
            Value::Null | Value::Undefined => None,
            _ => return Err(clock_error("The durable receipt clock has no timestamp")),
        });
    }
    Ok(observed)
}

fn clock_error(message: &str) -> Anomaly { Anomaly::fault("rtdb.tx/retention-clock", message) }

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
    /// Legacy retries admitted in different windows have independent stored identities.
    pub(super) async fn retire_expired_receipts(&mut self, clock: RetentionClock) -> Result<()> {
        if !clock.collect_at(self.tx_key().tx_id) { return Ok(()); }
        let observed = clock.time;
        let receipts = super::receipts_table();
        if self.snapshot()?.contains_table(&receipts) {
            self.retire_receipt_rows(
                receipts,
                "SELECT _id FROM xt.idempotency_receipts FOR ALL VALID_TIME GROUP BY _id HAVING MAX(_valid_to) <= ? ORDER BY CAST(_id AS TEXT) LIMIT 128",
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
                "SELECT _id FROM xt.rtdb_replay_keys FOR ALL VALID_TIME GROUP BY _id HAVING MAX(_valid_to) <= ? ORDER BY CAST(_id AS TEXT) LIMIT 128",
                observed,
            ).await?;
        }
        Ok(())
    }

    pub(super) async fn retire_placed_outcomes(&mut self, clock: RetentionClock) -> Result<()> {
        let outcomes = rtdb_types::TableRef::new("xt", rtdb_types::PLACED_ATTEMPT_OUTCOME_TABLE);
        if !self.context.placed_receipt_retirement || !clock.collect_at(self.tx_key().tx_id)
            || !self.snapshot()?.contains_table(&outcomes)
        {
            return Ok(());
        }
        self.retire_receipt_rows(
            outcomes,
            "SELECT _id FROM xt.placed_attempt_outcomes FOR ALL VALID_TIME WHERE _valid_to <= ? ORDER BY _id LIMIT 128",
            clock.time,
        ).await
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
