use rtdb_types::{Anomaly, Result, TableRef, Timestamp, TransactionKey, TxId, Value};

use crate::UserMetadata;

pub const BOUNDED_RETRY_METADATA_KEY: &str = "rtdb_retry_v1";
const RECEIPT_TABLE: &str = "bounded_retry_receipts";
const DUPLICATE: &str = "rtdb/idempotency-bounded-duplicate";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BoundedRetry {
    nonce: [u8; 32],
    cutoff: Timestamp,
}

impl BoundedRetry {
    pub(crate) fn from_metadata(metadata: Option<&UserMetadata>) -> Result<Option<Self>> {
        let Some(Value::Struct(fields)) = metadata.map(UserMetadata::value) else { return Ok(None); };
        let Some(value) = fields.get(BOUNDED_RETRY_METADATA_KEY) else { return Ok(None); };
        let Value::Struct(identity) = value else { return Err(invalid()); };
        let (Some(Value::Utf8(nonce)), Some(Value::I64(not_after))) =
            (identity.get("nonce"), identity.get("not_after_micros")) else { return Err(invalid()); };
        if identity.len() != 2 || fields.contains_key("idempotency_key") {
            return Err(invalid());
        }
        Self::new(nonce.as_bytes(), *not_after).map(Some)
    }

    pub(crate) fn new(nonce: &[u8], cutoff: i64) -> Result<Self> {
        let nonce: [u8; 32] = nonce.try_into().map_err(|_| invalid())?;
        if !nonce.iter().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
            || cutoff == i64::MAX
        {
            return Err(invalid());
        }
        Ok(Self { nonce, cutoff: Timestamp(cutoff) })
    }

    pub(crate) fn nonce(&self) -> &str { std::str::from_utf8(&self.nonce).expect("validated ASCII nonce") }
    pub(crate) fn cutoff(&self) -> Timestamp { self.cutoff }
}

pub(crate) fn bounded_receipts_table() -> TableRef { TableRef::new("xt", RECEIPT_TABLE) }
pub(crate) fn is_bounded_receipts_table(table: &TableRef) -> bool {
    table.schema == "xt" && table.table == RECEIPT_TABLE
}

pub(crate) fn duplicate(original: TransactionKey) -> Anomaly {
    Anomaly::incorrect(DUPLICATE, "The bounded retry identity already has a terminal outcome")
        .with_context("original-tx-id", original.tx_id.0.into())
        .with_context("original-system-time-micros", original.system_time.0.into())
}

pub fn bounded_retry_original(error: &Anomaly) -> Result<Option<TransactionKey>> {
    if error.code != DUPLICATE { return Ok(None); }
    let field = |name| error.context.get(name).and_then(serde_json::Value::as_i64)
        .ok_or_else(|| Anomaly::fault("rtdb.tx/bounded-retry-outcome", "Stored retry has no original outcome key"));
    Ok(Some(TransactionKey {
        tx_id: TxId(field("original-tx-id")?),
        system_time: Timestamp(field("original-system-time-micros")?),
    }))
}

fn invalid() -> Anomaly {
    Anomaly::incorrect("rtdb/invalid-bounded-retry", "rtdb_retry_v1 requires a 32-digit lowercase hex nonce and integer not_after_micros, without an idempotency_key")
}
