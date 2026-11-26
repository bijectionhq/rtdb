use rtdb_types::{Anomaly, AnomalyCategory, AnomalyKind};

pub(crate) fn protocol(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb.pgwire/protocol-violation", message).with_context("sql-state", "08P01".into()) }
pub(crate) fn network(error: impl std::fmt::Display) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "xtdb.pgwire/connection-error", error.to_string()) }
pub(crate) fn cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb.pgwire/query-cancelled", "query cancelled during execution").with_context("sql-state", "57014".into()) }

/// The SQLSTATE an error reports: an explicit state first, then its kind, then upstream's `ex->pgw-err` table by category and code.
pub(crate) fn state(error: &Anomaly) -> &str {
    if let Some(state) = error.context.get("sql-state").and_then(serde_json::Value::as_str) { return state; }
    if let Some(kind) = error.kind { return kind_state(kind); }
    match error.category {
        AnomalyCategory::Conflict => match error.code.as_str() { "xtdb/assert-failed" => "P0004", "xtdb/prepared-query-out-of-date" | "prepared-query-out-of-date" => "0A000", _ => "XX000" },
        AnomalyCategory::Incorrect => match error.code.as_str() { "xtdb/unindexed-tx" => "0B000", "xtdb.pgwire/invalid-arg-representation" => if error.context.get("arg-format").and_then(serde_json::Value::as_str) == Some("binary") { "22P03" } else { "22P02" }, "xtdb.authn/malformed-client-credentials" => "28P01", "xtdb/discard-in-tx" => "25001", _ => "08P01" },
        AnomalyCategory::Forbidden => match error.code.as_str() { "xtdb.authn/failed" | "xtdb.authn/refresh-failed" => "28P01", "xtdb.authn/no-matching-rule" => "28000", _ => "42501" },
        AnomalyCategory::Unsupported => "0A000", AnomalyCategory::Interrupted => "57014", _ => "XX000",
    }
}

/// PostgreSQL's SQLSTATE for each condition RTDB raises that upstream XTDB has no error for.
fn kind_state(kind: AnomalyKind) -> &'static str {
    match kind {
        AnomalyKind::ResourceExhausted => "53200",
        AnomalyKind::ConfigurationLimitExceeded => "53400",
        AnomalyKind::ProgramLimitExceeded => "54000",
        AnomalyKind::NumericValueOutOfRange => "22003",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_exhaustion_reports_out_of_memory_by_its_kind_not_its_code() {
        let budget = rtdb_columnar::MemoryBudget::new(16);
        let exhausted = budget.reserve(17).err().expect("the budget refuses the reservation");
        assert_eq!(state(&exhausted), "53200");
        assert_eq!(state(&Anomaly::new(AnomalyCategory::Busy, "test/cache-full", "cache full").with_kind(AnomalyKind::ResourceExhausted)), "53200");
        assert_eq!(state(&Anomaly::incorrect("rtdb/memory-budget-exceeded", "a code alone is not exhaustion")), "08P01");
        assert_eq!(state(&Anomaly::incorrect("xtdb/sql-error", "bad")), "08P01");
    }

    #[test]
    fn each_kind_reports_its_postgres_state_rather_than_a_protocol_violation() {
        for (kind, expected) in [(AnomalyKind::ResourceExhausted, "53200"), (AnomalyKind::ConfigurationLimitExceeded, "53400"),
            (AnomalyKind::ProgramLimitExceeded, "54000"), (AnomalyKind::NumericValueOutOfRange, "22003")] {
            assert_eq!(state(&Anomaly::incorrect("test/limit", "detail").with_kind(kind)), expected, "{kind:?}");
        }
    }

    #[test]
    fn an_explicit_state_wins_over_a_kind() {
        let error = Anomaly::incorrect("test/limit", "detail").with_kind(AnomalyKind::ProgramLimitExceeded).with_context("sql-state", "08P01".into());
        assert_eq!(state(&error), "08P01");
    }

    #[test]
    fn upstream_codes_keep_the_upstream_states() {
        assert_eq!(state(&Anomaly::incorrect("xtdb.expression/overflow-error", "data exception - overflow error")), "08P01");
        assert_eq!(state(&Anomaly::incorrect("xtdb/unindexed-tx", "later")), "0B000");
        assert_eq!(state(&Anomaly::fault("test/fault", "broken")), "XX000");
        assert_eq!(state(&Anomaly::unsupported("test/unsupported", "no")), "0A000");
    }

    #[test]
    fn an_out_of_range_integer_cell_reports_numeric_value_out_of_range() {
        let error = rtdb_pg_codec::integer_in_range(rtdb_types::PgType::Int2, i128::from(i16::MAX) + 1).unwrap_err();
        assert_eq!(error.code, "rtdb.pgwire/integer-out-of-range");
        assert_eq!(state(&error), "22003");
    }
}
