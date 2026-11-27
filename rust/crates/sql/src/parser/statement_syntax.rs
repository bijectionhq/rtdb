use std::{convert::Infallible, sync::Arc};
use rtdb_types::{AllocationScope, Anomaly, AnomalyCategory, MemoryBudget, Result};
use super::prediction::{CapturedClassPolicy, ParserEnvironment, PredictionError, SourceAssertions, StatementSyntaxDiagnostic};

#[derive(Clone, Copy)]
pub(super) enum StatementRoot { Direct, Batch }

/// Reparses a source the vendor SQL parser rejected with `xtdb/sql-error` under the original `directSqlStatement` grammar.
/// The original listener throws on its first syntax error, so a report is the complete diagnostic that source produces.
/// `Ok(None)` leaves the vendor error in place: the grammar accepted the input, reached a rule this port does not yet recognise, or the lexer stopped first.
/// Parser admission failures are returned to the caller.
pub fn statement_syntax_diagnostic(
    source: &str, error: &Anomaly, memory: &Arc<MemoryBudget>,
) -> Result<Option<StatementSyntaxDiagnostic>> {
    diagnose(source, error, memory, StatementRoot::Direct)
}

/// As `statement_syntax_diagnostic`, under the `multiSqlStatement` grammar for a complete input batch.
pub fn batch_syntax_diagnostic(
    source: &str, error: &Anomaly, memory: &Arc<MemoryBudget>,
) -> Result<Option<StatementSyntaxDiagnostic>> {
    diagnose(source, error, memory, StatementRoot::Batch)
}

fn diagnose(
    source: &str, error: &Anomaly, memory: &Arc<MemoryBudget>, root: StatementRoot,
) -> Result<Option<StatementSyntaxDiagnostic>> {
    if error.category != AnomalyCategory::Incorrect || error.code != "xtdb/sql-error" { return Ok(None); }
    let policy = CapturedClassPolicy::capture_parser_atn(SourceAssertions::Disabled, |_| Ok::<_, Infallible>(None)).unwrap();
    let mut parser = match ParserEnvironment::open_in(policy, memory) {
        Ok(parser) => parser,
        Err(PredictionError::Admission(error)) => return Err(error),
        Err(_) => return Ok(None),
    };
    let work = AllocationScope::new(memory)?;
    parser.statement_syntax_diagnostic(source, root, &work)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_retain_only_the_listener_lease() {
        let memory = MemoryBudget::new(64 << 20);
        for (source, message) in [
            ("SELEC 1", "line 1:0 mismatched input 'SELEC'"),
            ("-- note\n extra SELECT 1", "line 2:1 extraneous input 'extra'"),
            ("/* ★ */\n  SELEC 1", "line 2:2 mismatched input 'SELEC'"),
        ] {
            let diagnostic = {
                let source = source.to_owned();
                let error = crate::parse_in(&source, &memory).err().unwrap();
                assert_eq!(memory.used_bytes(), 0);
                statement_syntax_diagnostic(&source, &error, &memory).unwrap().unwrap()
            };
            let anomaly = diagnostic.anomaly();
            assert_eq!(anomaly.category, AnomalyCategory::Incorrect);
            assert_eq!(anomaly.code, "xtdb/sql-error");
            assert!(anomaly.context.is_empty());
            let expected = format!("Errors parsing SQL statement:\n  - {message} expecting ");
            let message = anomaly.message.as_deref().unwrap();
            assert!(message.starts_with(&expected));
            assert_eq!(memory.used_bytes(), message.len() + anomaly.code.len() + 256);
            drop(diagnostic);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn multi_report_owns_original_sql_and_releases_both_reservations() {
        let memory = MemoryBudget::new(64 << 20);
        let source = "SELEC 1; SELECT 2";
        let legacy = crate::parse_statement_batch(source, &memory).unwrap_err();
        assert_eq!(memory.used_bytes(), 0);
        let parse_report = || batch_syntax_diagnostic(source, &legacy, &memory).unwrap().unwrap();
        let report = parse_report();
        let listener_bytes = memory.used_bytes();
        assert!(listener_bytes > 0);
        let diagnostic = {
            let original = format!("  {source}; \n");
            let bytes = original.len();
            let diagnostic = report.with_sql_context(&original).unwrap();
            assert_eq!(memory.used_bytes(), listener_bytes + bytes + 1024);
            diagnostic
        };
        let anomaly = diagnostic.anomaly();
        assert_eq!(anomaly.context["sql"], "  SELEC 1; SELECT 2; \n");
        assert!(anomaly.message.as_deref().unwrap().starts_with("Errors parsing SQL statement:\n  - line 1:0 mismatched input 'SELEC' expecting "));
        drop(diagnostic);
        assert_eq!(memory.used_bytes(), 0);

        let report = parse_report();
        let occupied = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        assert_eq!(report.with_sql_context(source).unwrap_err().code, "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), occupied.bytes());
        drop(occupied);
        assert_eq!(memory.used_bytes(), 0);
    }

    const STATEMENT_HEAD: &str = "{'(', 'ASSERT', 'ATTACH', 'BEGIN', 'COMMIT', 'COPY', 'CREATE', 'DELETE', 'DETACH', 'DISCARD', 'ERASE', 'EXECUTE', 'EXPLAIN', 'FROM', 'GRANT', 'INSERT', 'PATCH', 'PREPARE', 'RECORDS', 'REVOKE', 'ROLLBACK', 'SELECT', 'SET', 'SETTING', 'SHOW', 'START', 'UPDATE', 'VALUES', 'WITH', 'XTQL'}";
    const ROW_COUNT: &str = "{UNSIGNED_INTEGER, POSTGRES_PARAMETER_SPECIFICATION, '?'}";

    #[test]
    fn reports_match_the_reference_listener_under_both_roots() {
        let memory = MemoryBudget::new(64 << 20);
        let same = |report: String| (report.clone(), report);
        for (source, (direct, multi)) in [
            ("SELECT 1; SELEC 1", ("line 1:10 mismatched input 'SELEC' expecting <EOF>".into(), "line 1:10 no viable alternative at input ';SELEC'".into())),
            ("SELECT 1; SELEC 1;", ("line 1:10 mismatched input 'SELEC' expecting <EOF>".into(), "line 1:10 no viable alternative at input ';SELEC'".into())),
            ("\"SELEC\" 1", same(format!("line 1:0 mismatched input '\"SELEC\"' expecting {STATEMENT_HEAD}"))),
            ("SELECT 1 +", same("line 1:9 extraneous input '+' expecting {<EOF>, ';'}".into())),
            ("PG_SLEEP 1", same(format!("line 1:0 mismatched input 'PG_SLEEP' expecting {STATEMENT_HEAD}"))),
            ("ADD 1", same(format!("line 1:0 mismatched input 'ADD' expecting {STATEMENT_HEAD}"))),
            ("SELEC 1; SELECT 2", same(format!("line 1:0 mismatched input 'SELEC' expecting {STATEMENT_HEAD}"))),
            ("SELECT 1 LIMIT ALL", same(format!("line 1:15 mismatched input 'ALL' expecting {ROW_COUNT}"))),
            ("SELECT 1 LIMIT ALL; SELEC 7", same(format!("line 1:15 mismatched input 'ALL' expecting {ROW_COUNT}"))),
            ("SELECT 1 OFFSET ALL", same(format!("line 1:16 mismatched input 'ALL' expecting {ROW_COUNT}"))),
            ("SELECT 1 LIMIT 2 OFFSET", same(format!("line 1:23 mismatched input '<EOF>' expecting {ROW_COUNT}"))),
            ("SELECT 1 FETCH FIRST 2 ONLY", same("line 1:23 missing {'ROW', 'ROWS'} at 'ONLY'".into())),
            ("SELECT 1 LIMIT 2 LIMIT 3", same("line 1:17 mismatched input 'LIMIT' expecting {<EOF>, ';'}".into())),
        ] {
            let error = Anomaly::incorrect("xtdb/sql-error", "vendor");
            for (diagnose, expected) in [(statement_syntax_diagnostic as fn(_, _, _) -> _, direct), (batch_syntax_diagnostic, multi)] {
                let report = diagnose(source, &error, &memory).unwrap().unwrap();
                assert_eq!(report.anomaly().message.as_deref().unwrap(), format!("Errors parsing SQL statement:\n  - {expected}"), "{source}");
            }
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn insert_values_returning_reports_the_first_reached_syntax_error() {
        let memory = MemoryBudget::new(64 << 20);
        for (source, expected) in [
            (
                "INSERT INTO docs (_id) VALUES (1) RETURNING CAST(a AS BLOB)",
                "line 1:54 no viable alternative at input 'CAST(aASBLOB'",
            ),
            (
                "INSERT INTO \"🙂\".\"select\" (\"_id\") VALUES (1), (2) RETURNING CAST(a AS BLOB)",
                "line 1:69 no viable alternative at input 'CAST(aASBLOB'",
            ),
            (
                "INSERT docs (_id) VALUES (1) RETURNING CAST(a AS BLOB)",
                "line 1:7 missing 'INTO' at 'docs'",
            ),
        ] {
            let legacy = crate::parse_in(source, &memory).unwrap_err();
            for diagnose in [statement_syntax_diagnostic as fn(_, _, _) -> _, batch_syntax_diagnostic] {
                let report = diagnose(source, &legacy, &memory).unwrap().unwrap();
                assert_eq!(report.anomaly().category, AnomalyCategory::Incorrect);
                assert_eq!(report.anomaly().code, "xtdb/sql-error");
                assert_eq!(report.anomaly().message.as_deref().unwrap(),
                    format!("Errors parsing SQL statement:\n  - {expected}"), "{source}");
                assert!(report.anomaly().context.is_empty());
            }
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn an_unported_insert_source_keeps_the_original_error() {
        let memory = MemoryBudget::new(64 << 20);
        for source in [
            "INSERT INTO docs RECORDS {_id: 1} RETURNING CAST(a AS BLOB)",
            "INSERT INTO docs (_id) VALUES (abs(1)) RETURNING CAST(a AS BLOB)",
        ] {
            let legacy = crate::parse_in(source, &memory).unwrap_err();
            assert!(statement_syntax_diagnostic(source, &legacy, &memory).unwrap().is_none(), "{source}");
            assert!(batch_syntax_diagnostic(source, &legacy, &memory).unwrap().is_none(), "{source}");
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn unrecognised_failures_keep_the_vendor_anomaly() {
        let memory = MemoryBudget::new(64 << 20);
        for source in ["SELECT 1", "extra @", "PREPARE q AS SELECT 1 LIMIT ALL"] {
            let error = Anomaly::incorrect("xtdb/sql-error", "vendor");
            assert!(statement_syntax_diagnostic(source, &error, &memory).unwrap().is_none(), "{source}");
            assert!(batch_syntax_diagnostic(source, &error, &memory).unwrap().is_none(), "{source}");
        }
        let other = Anomaly::incorrect("xtdb/other", "vendor");
        assert!(statement_syntax_diagnostic("SELEC 1", &other, &memory).unwrap().is_none());
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn parser_budget_denial_is_returned() {
        let original = MemoryBudget::new(64 << 20);
        for limit in [0, 4096] {
            let memory = MemoryBudget::new(limit);
            for source in ["SELEC 1", "SELECT 1; SELEC 1", "/* comment */ SELEC 1", "SELECT 1 LIMIT ALL"] {
                let error = crate::parse_in(source, &original).err().unwrap();
                let denied = statement_syntax_diagnostic(source, &error, &memory).unwrap_err();
                assert_eq!(denied.code, "rtdb/memory-budget-exceeded", "{source}");
                assert_eq!(memory.used_bytes(), 0);
            }
        }
    }
}
