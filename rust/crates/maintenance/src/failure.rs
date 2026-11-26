use std::io::Write;
use rtdb_format::keys::TrieKey;
use rtdb_storage::ObjectKey;
use rtdb_types::{Anomaly, AnomalyCategory, TableRef};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct CompactionFailure {
    pub event: &'static str,
    /// Qualified table name in `schema/table` form.
    pub table: String,
    pub output_trie_key: String,
    pub anomaly_code: String,
    pub anomaly_category: AnomalyCategory,
    pub operation: Option<String>,
    /// Relative requested storage key, excluding provider endpoints and local root paths.
    pub object_key: Option<String>,
    /// True only for the `rtdb.storage/permission-denied` anomaly code.
    pub permission_denied: bool,
    /// Lowercase SHA-256 of a present message's UTF-8 bytes; absent messages serialize as JSON null.
    pub message_sha256: Option<String>,
}

impl CompactionFailure {
    /// Captures the job identity and typed error classification without retaining the message or arbitrary context.
    /// Only nonempty operation names containing lowercase ASCII letters, underscores or hyphens and validated object keys are retained.
    pub fn new(table: &TableRef, output: &TrieKey, error: &Anomaly) -> Self {
        let operation = error.context.get("operation").and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_lowercase() || byte == b'_' || byte == b'-'))
            .map(str::to_owned);
        let object_key = error.context.get("key").and_then(serde_json::Value::as_str)
            .and_then(|key| ObjectKey::new(key).ok()).map(|key| key.to_string());
        let message_sha256 = error.message.as_ref().map(|message| {
            let digest = Sha256::digest(message.as_bytes());
            let mut hex = String::with_capacity(64);
            const HEX: &[u8; 16] = b"0123456789abcdef";
            for byte in digest { hex.push(HEX[usize::from(byte >> 4)] as char); hex.push(HEX[usize::from(byte & 15)] as char); }
            hex
        });
        Self { event: "compaction_failed", table: table.schema_and_table(), output_trie_key: output.to_string(),
            anomaly_code: error.code.clone(), anomaly_category: error.category, operation, object_key,
            permission_denied: error.code == "rtdb.storage/permission-denied", message_sha256 }
    }

    /// Writes one JSON object followed by a newline; write failures are returned to the caller.
    pub fn write_json_line(&self, mut output: impl Write) -> std::io::Result<()> {
        serde_json::to_writer(&mut output, self).map_err(std::io::Error::other)?;
        output.write_all(b"\n")
    }
}

pub trait CompactionObserver: Send + Sync {
    /// Called synchronously once for each non-cancellation worker failure, including workers joined during shutdown.
    /// Maintenance's closed code and the Interrupted category are excluded.
    /// Implementations must return promptly and must not panic; observation does not change the job's result or retry policy.
    fn failed(&self, failure: &CompactionFailure);
}

/// Logs each failure as one ERROR event whose fields are the failure's own.
#[derive(Default)]
pub struct LoggingCompactionObserver;

impl CompactionObserver for LoggingCompactionObserver {
    fn failed(&self, failure: &CompactionFailure) {
        tracing::error!(
            event = failure.event, table = %failure.table, output_trie_key = %failure.output_trie_key,
            anomaly_code = %failure.anomaly_code, anomaly_category = ?failure.anomaly_category,
            operation = failure.operation.as_deref(), object_key = failure.object_key.as_deref(),
            permission_denied = failure.permission_denied, message_sha256 = failure.message_sha256.as_deref(),
            "Compaction job failed"
        );
    }
}
