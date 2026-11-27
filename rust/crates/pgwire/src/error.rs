use rtdb_types::{Anomaly, AnomalyCategory, AnomalyKind};

pub(crate) fn protocol(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb.pgwire/protocol-violation", message).with_context("sql-state", "08P01".into()) }
pub(crate) fn network(error: impl std::fmt::Display) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "xtdb.pgwire/connection-error", error.to_string()) }
pub(crate) fn cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb.pgwire/query-cancelled", "query cancelled during execution").with_context("sql-state", "57014".into()) }

/// The SQLSTATE an error reports: an explicit state first, then its kind, then upstream's `ex->pgw-err` table by category and code.
pub(crate) fn state(error: &Anomaly) -> &str { classify(error).0 }

/// The SQLSTATE of an error frame about to be sent, logging the errors upstream's `ex->pgw-err` logs as uncaught.
pub(crate) fn frame_state(error: &Anomaly) -> &str {
    let (state, uncaught) = classify(error);
    if uncaught { tracing::error!(code = %error.code, category = ?error.category, "Uncaught exception processing message: {error}"); }
    state
}

/// The state, and whether upstream reaches it through its fall-through `XX000` branch, which logs the error.
fn classify(error: &Anomaly) -> (&str, bool) {
    if let Some(state) = error.context.get("sql-state").and_then(serde_json::Value::as_str) { return (state, false); }
    if let Some(state) = error.kind.and_then(kind_state) { return (state, state == "XX000"); }
    let state = match error.category {
        AnomalyCategory::Conflict => match error.code.as_str() { "xtdb/assert-failed" => "P0004", "xtdb/prepared-query-out-of-date" | "prepared-query-out-of-date" => "0A000", _ => "XX000" },
        AnomalyCategory::Incorrect => match error.code.as_str() { "xtdb/unindexed-tx" => "0B000", "xtdb.pgwire/invalid-arg-representation" => if error.context.get("arg-format").and_then(serde_json::Value::as_str) == Some("binary") { "22P03" } else { "22P02" }, "xtdb.authn/malformed-client-credentials" => "28P01", "xtdb/discard-in-tx" => "25001", _ => "08P01" },
        AnomalyCategory::Forbidden => match error.code.as_str() { "xtdb.authn/failed" | "xtdb.authn/refresh-failed" => "28P01", "xtdb.authn/no-matching-rule" => "28000", _ => "42501" },
        AnomalyCategory::Unsupported => "0A000", AnomalyCategory::Interrupted => "57014",
        AnomalyCategory::NotFound | AnomalyCategory::Busy | AnomalyCategory::Unavailable | AnomalyCategory::Fault => return ("XX000", true),
    };
    (state, false)
}

/// The SQLSTATE a kind reports whatever the error's category, or `None` for a kind that keeps upstream's state.
/// Memory exhaustion is `XX000`, as upstream reports an Arrow out-of-memory error, not PostgreSQL's `53200`.
fn kind_state(kind: AnomalyKind) -> Option<&'static str> {
    match kind {
        AnomalyKind::ResourceExhausted => Some("XX000"),
        AnomalyKind::ConfigurationLimitExceeded => Some("53400"),
        AnomalyKind::ProgramLimitExceeded => Some("54000"),
        AnomalyKind::NumericValueOutOfRange => Some("22003"),
        AnomalyKind::TableNotFound | AnomalyKind::OptimisticConflict | AnomalyKind::MissingObject | AnomalyKind::CorruptObject
        | AnomalyKind::InputOutOfOrder | AnomalyKind::GrammarRejected | AnomalyKind::BasisAhead | AnomalyKind::UnknownBasis
        | AnomalyKind::NoWriter => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_exhaustion_reports_upstreams_uncaught_state_by_its_kind_not_its_code() {
        let budget = rtdb_columnar::MemoryBudget::new(16);
        let exhausted = budget.reserve(17).err().expect("the budget refuses the reservation");
        assert_eq!(classify(&exhausted), ("XX000", true));
        assert_eq!(classify(&Anomaly::new(AnomalyCategory::Busy, "test/cache-full", "cache full").with_kind(AnomalyKind::ResourceExhausted)), ("XX000", true));
        assert_eq!(state(&Anomaly::incorrect("rtdb/memory-budget-exceeded", "a code alone is not exhaustion")), "08P01");
        assert_eq!(state(&Anomaly::incorrect("xtdb/sql-error", "bad")), "08P01");
    }

    #[test]
    fn each_kind_reports_its_postgres_state_rather_than_a_protocol_violation() {
        for (kind, expected) in [(AnomalyKind::ResourceExhausted, "XX000"), (AnomalyKind::ConfigurationLimitExceeded, "53400"),
            (AnomalyKind::ProgramLimitExceeded, "54000"), (AnomalyKind::NumericValueOutOfRange, "22003")] {
            assert_eq!(state(&Anomaly::incorrect("test/limit", "detail").with_kind(kind)), expected, "{kind:?}");
        }
    }

    #[test]
    fn a_missing_table_keeps_upstreams_state_despite_its_kind() {
        assert_eq!(state(&rtdb_plan::table_not_found("Table not found: t")), "08P01");
    }

    #[test]
    fn only_upstreams_fall_through_errors_count_as_uncaught() {
        assert_eq!(classify(&Anomaly::fault("test/fault", "broken")), ("XX000", true));
        assert_eq!(classify(&Anomaly::new(AnomalyCategory::Unavailable, "test/down", "down")), ("XX000", true));
        assert_eq!(classify(&Anomaly::new(AnomalyCategory::NotFound, "test/gone", "gone")), ("XX000", true));
        assert_eq!(classify(&Anomaly::new(AnomalyCategory::Conflict, "test/conflict", "conflict")), ("XX000", false));
        assert_eq!(classify(&Anomaly::incorrect("xtdb/sql-error", "bad")), ("08P01", false));
        assert_eq!(classify(&Anomaly::fault("test/fault", "broken").with_context("sql-state", "XX000".into())), ("XX000", false));
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

    /// RTDB `incorrect` codes named like a limit that rightly keep upstream's `08P01`: each refuses a malformed argument,
    /// configuration or schema, as XTDB reports an `IllegalArgumentException`, rather than work that outgrew a limit.
    /// A limit raised as `incorrect` must carry its kind instead of joining this list.
    const LIMIT_SHAPED_PROTOCOL_VIOLATIONS: &[(&str, &str)] = &[
        ("rtdb.auth/http-limit", "a configured OIDC response limit of zero"),
        ("rtdb.node/invalid-memory-budget", "node options disagree with the supplied execution budget"),
        ("rtdb.server/invalid-memory-budget", "the supplied execution budget disagrees with the configured split"),
        ("rtdb.store/invalid-bound", "a store bound whose min exceeds its max"),
        ("rtdb.owned/count-bound", "a count bound on a relation that cannot carry one"),
        ("rtdb.owned/range-bound", "a range bound that does not match its path key"),
        ("rtdb.owned/range-limit", "a requested range limit outside 1..=128"),
        ("rtdb.owned/nearest-limit", "a nearest request with a malformed or out-of-range probe, filter or k"),
        ("rtdb.owned/search-limit", "a search request with an out-of-range offset, limit, partition count or text"),
        ("rtdb/binary-width", "a negative fixed-size binary width in a schema"),
        ("rtdb/list-size", "a negative fixed-size list length in a schema"),
        ("rtdb/iid-run-width", "IID run ends of a type other than i16, i32 or i64"),
        ("rtdb/run-end-width", "run ends of a type other than i16, i32 or i64"),
        ("rtdb/time-width", "a time width that disagrees with its unit"),
        ("rtdb/change-limit", "a changes request with a zero group limit"),
        ("rtdb/rank-bound", "a rank admission with negative counts (its limit variant carries a kind)"),
        ("rtdb/reservation-budget", "absorbing a reservation from another budget, a misuse of the reservation API"),
    ];

    const LIMIT_SUFFIXES: [&str; 9] = ["-size", "-overflow", "-limit", "-budget", "-bound", "-width", "-exceeded", "-too-large", "-capacity"];

    /// The `rtdb*` codes a source file raises as `incorrect`, read from the literal that follows the constructor.
    fn incorrect_codes(source: &str) -> Vec<&str> {
        let mut codes = Vec::new();
        for opener in ["Anomaly::incorrect(", "Category::Incorrect,"] {
            for (start, _) in source.match_indices(opener) {
                let rest = source[start + opener.len()..].trim_start();
                let Some(literal) = rest.strip_prefix('"') else { continue };
                let Some(end) = literal.find('"') else { continue };
                if literal[..end].starts_with("rtdb") { codes.push(&literal[..end]); }
            }
        }
        codes
    }

    fn rust_sources(directory: &std::path::Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() { rust_sources(&path, files); } else if path.extension().is_some_and(|extension| extension == "rs") { files.push(path); }
        }
    }

    #[test]
    fn no_rtdb_limit_code_reports_a_protocol_violation_unless_listed() {
        let crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let mut files = Vec::new();
        for entry in std::fs::read_dir(crates).unwrap() {
            let source = entry.unwrap().path().join("src");
            if source.is_dir() { rust_sources(&source, &mut files); }
        }
        let mut limit_codes = std::collections::BTreeSet::new();
        for file in files {
            let text = std::fs::read_to_string(&file).unwrap();
            limit_codes.extend(incorrect_codes(&text).into_iter().filter(|code| LIMIT_SUFFIXES.iter().any(|suffix| code.ends_with(suffix))).map(str::to_owned));
        }
        let reported = |code: &str| {
            let error = Anomaly::incorrect(code, "detail");
            let error = match AnomalyKind::from_wire_code(code) { Some(kind) => error.with_kind(kind), None => error };
            state(&error).to_owned()
        };
        let listed = |code: &str| LIMIT_SHAPED_PROTOCOL_VIOLATIONS.iter().any(|(listed, _)| *listed == code);
        let unlisted = limit_codes.iter().filter(|code| reported(code) == "08P01" && !listed(code)).collect::<Vec<_>>();
        assert!(unlisted.is_empty(), "limit codes reporting 08P01 without a kind: {unlisted:?}");
        let stale = LIMIT_SHAPED_PROTOCOL_VIOLATIONS.iter().filter(|(code, _)| !limit_codes.contains(*code) || reported(code) != "08P01").collect::<Vec<_>>();
        assert!(stale.is_empty(), "listed codes no longer raised, or now given a kind: {stale:?}");
        for code in ["rtdb/list-offset-overflow", "rtdb.sql/batch-size", "rtdb.query/shared-plan-size", "rtdb/decimal-width", "rtdb.query/expression-size",
            "rtdb/value-size-overflow", "rtdb.plan/catalog-size", "rtdb/journal-record-too-large"] {
            assert_eq!(reported(code), "54000", "{code}");
        }
        for (code, expected) in [("rtdb.storage/message-too-large", "53400"), ("rtdb.auth/response-too-large", "53400"), ("rtdb/time-overflow", "22003")] {
            assert_eq!(reported(code), expected, "{code}");
        }
    }
}
