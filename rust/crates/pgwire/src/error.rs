use rtdb_types::{Anomaly, AnomalyCategory};

pub(crate) fn protocol(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb.pgwire/protocol-violation", message).with_context("sql-state", "08P01".into()) }
pub(crate) fn network(error: impl std::fmt::Display) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "xtdb.pgwire/connection-error", error.to_string()) }
pub(crate) fn cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb.pgwire/query-cancelled", "query cancelled during execution").with_context("sql-state", "57014".into()) }
pub(crate) fn state(error: &Anomaly) -> &str {
    if let Some(state) = error.context.get("sql-state").and_then(serde_json::Value::as_str) { return state; }
    match error.category {
        AnomalyCategory::Conflict => match error.code.as_str() { "xtdb/assert-failed" => "P0004", "xtdb/prepared-query-out-of-date" | "prepared-query-out-of-date" => "0A000", _ => "XX000" },
        AnomalyCategory::Incorrect => match error.code.as_str() { "xtdb/unindexed-tx" => "0B000", "xtdb.pgwire/invalid-arg-representation" => if error.context.get("arg-format").and_then(serde_json::Value::as_str) == Some("binary") { "22P03" } else { "22P02" }, "xtdb.authn/malformed-client-credentials" => "28P01", "xtdb/discard-in-tx" => "25001", _ => "08P01" },
        AnomalyCategory::Forbidden => match error.code.as_str() { "xtdb.authn/failed" | "xtdb.authn/refresh-failed" => "28P01", "xtdb.authn/no-matching-rule" => "28000", _ => "42501" },
        AnomalyCategory::Unsupported => "0A000", AnomalyCategory::Interrupted => "57014", _ => "XX000",
    }
}
