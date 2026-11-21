use rtdb_types::{Anomaly, Result, TableRef, Timestamp, TransactionKey, TxId};

use crate::idempotency_window::{ReplayWindow, replay_window};

const REPLAY_TABLE: &str = "rtdb_replay_keys";
const DUPLICATE_REPLAY: &str = "rtdb.tx/private-replay-duplicate";

pub struct ReplaySubmission {
    key: [u8; 34],
    submitted_at: Timestamp,
    not_after: Timestamp,
    expires_at: Timestamp,
}

impl ReplaySubmission {
    pub fn admit(key: &str, submitted_at: Timestamp, not_after: Timestamp) -> Result<Self> {
        let key: [u8; 34] = key.as_bytes().try_into().map_err(|_| invalid_key())?;
        if !key[..32].iter().all(u8::is_ascii_hexdigit) || key[32..] != *b"/0" {
            return Err(invalid_key());
        }
        let expires_at = match replay_window(submitted_at.0, not_after.0) {
            ReplayWindow::Open(expiry) => Timestamp(expiry),
            ReplayWindow::Elapsed => return Err(Anomaly::incorrect("rtdb.node/replay-window-elapsed", "Replay submission reached its exclusive cutoff")),
            ReplayWindow::Unrepresentable => return Err(Anomaly::incorrect("xtdb/time-overflow", "Replay marker has no finite validity interval")),
        };
        Ok(Self { key, submitted_at, not_after, expires_at })
    }

    pub(crate) fn key(&self) -> &str { std::str::from_utf8(&self.key).expect("replay key is ASCII") }
    pub(crate) fn submitted_at(&self) -> Timestamp { self.submitted_at }
    pub(crate) fn not_after(&self) -> Timestamp { self.not_after }
    pub(crate) fn expires_at(&self) -> Timestamp { self.expires_at }
}

pub(crate) fn replay_table() -> TableRef { TableRef::new("xt", REPLAY_TABLE) }
pub(crate) fn is_replay_table(table: &TableRef) -> bool { table.schema == "xt" && table.table == REPLAY_TABLE }

pub(crate) fn duplicate_replay(original: TransactionKey) -> Anomaly {
    Anomaly::incorrect(DUPLICATE_REPLAY, "The replay command already committed")
        .with_context("original-tx-id", original.tx_id.0.into())
        .with_context("original-system-time-micros", original.system_time.0.into())
}

pub fn private_replay_original(error: &Anomaly) -> Result<Option<TransactionKey>> {
    if error.code != DUPLICATE_REPLAY { return Ok(None); }
    let original_field = |name| error.context.get(name).and_then(serde_json::Value::as_i64)
        .ok_or_else(|| Anomaly::fault("rtdb.tx/private-replay-outcome", "Stored private replay outcome has no original transaction key"));
    Ok(Some(TransactionKey {
        tx_id: TxId(original_field("original-tx-id")?),
        system_time: Timestamp(original_field("original-system-time-micros")?),
    }))
}

fn invalid_key() -> Anomaly {
    Anomaly::incorrect("rtdb.node/invalid-replay-key", "Private replay key must be a 32-digit hexadecimal nonce followed by /0")
}
