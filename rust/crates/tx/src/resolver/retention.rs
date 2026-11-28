use arrow_array::{Array, RecordBatch};
use rtdb_columnar::{ColumnReader, read_value};
use rtdb_types::{Anomaly, Iid, Result, TableRef, TimeUnit, TxId, Timestamp, Value};
use sha2::{Digest, Sha256};

use super::OpenTransaction;
use crate::idempotency_window::advance_retry_clock;

#[derive(Clone, Copy)]
pub(super) struct RetentionClock {
    pub(super) time: Timestamp,
    previous: Option<Timestamp>,
}

impl RetentionClock {
    pub(super) fn observe(previous: Option<Timestamp>, appended: Timestamp) -> Self {
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
    pub(super) async fn observe_retention_clock(&self, appended: Timestamp, resolved_receipt_time: Option<Timestamp>) -> Result<RetentionClock> {
        let previous = if let Some(time) = resolved_receipt_time {
            Some(time)
        } else {
            let snapshot = self.buffer.base_snapshot();
            if let Some(predecessor) = snapshot.tx_basis() {
                let context = self.context.query_context(snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
                installed_receipt_time(context, predecessor).await?
            } else {
                None
            }
        };
        Ok(match self.decision {
            Some(sample) => RetentionClock { time: sample.instant, previous },
            None => RetentionClock::observe(previous, appended),
        })
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
    if let Some(Some(exact)) = point { return Ok(Some(exact)); }

    // A copied prefix can preserve its transaction row under the original source ID.
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

pub(super) const METADATA_ERASURE_UNIT: &str = "metadata_erasure_unit";

pub(super) fn metadata_erasure_key(table: &TableRef, iid: Iid, key: Option<&[u8]>) -> String {
    metadata_erasure_digest(table, [key.unwrap_or(&iid.0), &[]])
}

pub(super) fn metadata_erasure_key_parts(table: &TableRef, iid: Iid, key: Option<(u8, &[u8])>) -> String {
    match key {
        Some((tag, payload)) => metadata_erasure_digest(table, [&[tag], payload]),
        None => metadata_erasure_key(table, iid, None),
    }
}

fn metadata_erasure_digest(table: &TableRef, key: [&[u8]; 2]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"rtdb/transaction-metadata-erasure/v1");
    for field in [table.schema.as_bytes(), table.table.as_bytes()] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    digest.update((key[0].len() as u64 + key[1].len() as u64).to_be_bytes());
    for part in key { digest.update(part); }
    format!("{:x}", digest.finalize())
}

#[test]
fn metadata_erasure_keys_preserve_v1_framing() {
    let table = TableRef::new("app", "records");
    let iid = Iid([0x2a; 16]);
    assert_eq!(
        metadata_erasure_key_parts(&table, iid, None),
        "5a49faa7d23cfb781f0d6d1c49b9199a1dda17dde03f1a783defd539d6da450d",
    );
    assert_eq!(
        metadata_erasure_key_parts(&table, iid, Some((4, &[0, 0xfe, 0xff]))),
        "0b9b2c38ba74b586491e2b0c38c6aeec03d66cd397744f115c0668b1e59b1689",
    );
}

impl OpenTransaction<'_> {
    pub(super) async fn retire_unit_metadata(&mut self, table: &TableRef, iid: Iid, key: Option<&[u8]>) -> Result<()> {
        let receipts = super::receipts_table();
        if !self.buffer.base_snapshot().column_names(&receipts).contains(METADATA_ERASURE_UNIT) { return Ok(()); }
        let value = Value::Utf8(metadata_erasure_key(table, iid, key));
        let mut cursor = self.open_query(
            "SELECT _id FROM xt.idempotency_receipts FOR ALL VALID_TIME WHERE metadata_erasure_unit = ?",
            vec![value],
        ).await?;
        while let Some(page) = cursor.next_batch().await? {
            self.stage_receipt_erasures(&receipts, page.batch())?;
        }
        Ok(())
    }

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
        if self.buffer.contains_table(&receipts) {
            self.retire_receipt_rows(
                receipts,
                "SELECT _id FROM xt.idempotency_receipts FOR ALL VALID_TIME GROUP BY _id HAVING MAX(_valid_to) <= ? ORDER BY CAST(_id AS TEXT) LIMIT 128",
                observed,
            ).await?;
        }
        let bounded = crate::bounded_retry::bounded_receipts_table();
        if self.buffer.contains_table(&bounded) {
            self.retire_receipt_rows(
                bounded,
                "SELECT _id FROM xt.bounded_retry_receipts FOR ALL VALID_TIME WHERE _valid_to <= ? ORDER BY _id LIMIT 128",
                observed,
            ).await?;
        }
        let private = crate::replay_submission::replay_table();
        if self.buffer.contains_table(&private) {
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
            || !self.buffer.contains_table(&outcomes)
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
            self.stage_receipt_erasures(&table, page.batch())?;
        }
        Ok(())
    }

    fn stage_receipt_erasures(&mut self, table: &TableRef, page: &RecordBatch) -> Result<()> {
        if page.num_rows() == 0 { return Ok(()); }
        let ids = page.column(0).as_ref();
        let reader = ColumnReader::new(page.schema_ref().field(0))?;
        for row in 0..page.num_rows() {
            self.buffer.erase_iid(table, receipt_erasure_iid(&reader, ids, row)?)?;
        }
        Ok(())
    }
}

fn receipt_erasure_iid(reader: &ColumnReader<'_>, ids: &dyn Array, row: usize) -> Result<Iid> {
    match reader.read_entity_id(ids, row) {
        Ok(Some(id)) => Ok(id.iid()),
        _ => Ok(reader.read(ids, row)?.entity_id()?.iid()),
    }
}

#[cfg(test)]
#[test]
fn receipt_erasure_iids_match_raw_value_id_semantics() -> Result<()> {
    use rtdb_columnar::{build_array, to_field};
    use rtdb_types::{Keyword, LogicalType};

    let valid = [
        Value::Utf8("reçu/東京".into()), Value::Keyword(Keyword::from_name(":legacy/receipt")),
        Value::I8(-7), Value::I16(-123), Value::I32(-400), Value::I64(i64::MIN), Value::Uuid([0x12; 16]),
    ];
    let invalid = [Value::Null, Value::DateDay(42), Value::U64(u64::MAX), Value::Binary(vec![1, 2])];
    let values: Vec<_> = valid.into_iter().chain(invalid).collect();
    let ty = LogicalType::from_legs(values.iter().map(Value::logical_type));
    let field = to_field("_id", &ty)?;
    let array = build_array(&ty, &values)?;
    let reader = ColumnReader::new(&field)?;
    for row in 0..values.len() {
        let expected = read_value(&field, array.as_ref(), row).and_then(|value| value.entity_id()).map(|id| id.iid());
        assert_eq!(receipt_erasure_iid(&reader, array.as_ref(), row), expected);
    }
    Ok(())
}
