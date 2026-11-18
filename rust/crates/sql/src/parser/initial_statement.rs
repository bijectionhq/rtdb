use std::{convert::Infallible, ops::Range, sync::Arc};
use rtdb_types::{AllocationScope, Anomaly, AnomalyCategory, MemoryBudget, Result};
use super::prediction::{CapturedClassPolicy, InitialStatementDiagnostic, ParserEnvironment, PredictionError, SourceAssertions};

const LEGACY_STATEMENT: &str = "sql parser error: Expected: an SQL statement, found: ";

#[derive(Clone, Copy)]
pub(super) enum InitialRoot { Direct, Batch }

/// Looks up a Direct diagnostic for a vendor SQL parser error from this same source.
/// The first token must be an unquoted ASCII identifier absent from both keyword vocabularies.
/// Returns a retained unwanted-token or statement-head mismatch diagnostic when its offending byte range matches.
/// `Ok(None)` leaves the original error in place, including when leading block trivia cannot be verified.
/// Admission failures after eligibility are returned to the caller.
pub fn initial_statement_diagnostic(
    source: &str, error: &Anomaly, memory: &Arc<MemoryBudget>,
) -> Result<Option<InitialStatementDiagnostic>> {
    project_initial(source, error, memory, InitialRoot::Direct)
}

/// Uses the multi-statement grammar for an initial error from the same complete input batch.
/// Eligibility, admission and listener ownership follow `initial_statement_diagnostic`.
pub fn initial_batch_diagnostic(
    source: &str, error: &Anomaly, memory: &Arc<MemoryBudget>,
) -> Result<Option<InitialStatementDiagnostic>> {
    project_initial(source, error, memory, InitialRoot::Batch)
}

fn project_initial(
    source: &str, error: &Anomaly, memory: &Arc<MemoryBudget>, root: InitialRoot,
) -> Result<Option<InitialStatementDiagnostic>> {
    let Some(span) = legacy_statement_span(source, error) else { return Ok(None); };
    if !crate::lexer::is_first_identifier(source, &span, memory) { return Ok(None); }

    let policy = CapturedClassPolicy::capture_parser_atn(SourceAssertions::Disabled, |_| Ok::<_, Infallible>(None)).unwrap();
    let mut parser = match ParserEnvironment::open_in(policy, memory) {
        Ok(parser) => parser,
        Err(PredictionError::Admission(error)) => return Err(error),
        Err(_) => return Ok(None),
    };
    let work = AllocationScope::new(memory)?;
    parser.initial_statement_diagnostic(source, &span, root, &work)
}

fn legacy_statement_span(source: &str, error: &Anomaly) -> Option<Range<usize>> {
    if error.category != AnomalyCategory::Incorrect || error.code != "xtdb/sql-error" { return None; }
    let found = error.message.as_deref()?.strip_prefix(LEGACY_STATEMENT)?;
    let span = error.context.get("span")?;
    let start = usize::try_from(span.get("start")?.as_u64()?).ok()?;
    let end = usize::try_from(span.get("end")?.as_u64()?).ok()?;
    let identifier = source.get(start..end)?;
    let mut chars = identifier.bytes();
    if !chars.next().is_some_and(|ch| ch.is_ascii_alphabetic() || ch == b'_')
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, b'_' | b'$'))
        || !found.strip_prefix(identifier)?.starts_with(" at Line: ")
        || sqlparser::keywords::ALL_KEYWORDS.iter().any(|keyword| identifier.eq_ignore_ascii_case(keyword))
    {
        return None;
    }
    Some(start..end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_first_token_reports_retain_only_the_listener_lease() {
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
                initial_statement_diagnostic(&source, &error, &memory).unwrap().unwrap()
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
    fn other_failures_keep_the_vendor_anomaly() {
        let memory = MemoryBudget::new(64 << 20);
        for source in ["SELECT 1; SELEC 1", "\"SELEC\" 1", "SELECT 1 +", "extra @", "PG_SLEEP 1", "ADD 1"] {
            let error = crate::parse_in(source, &memory).err().unwrap();
            assert!(initial_statement_diagnostic(source, &error, &memory).unwrap().is_none(), "{source}");
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn parser_budget_denial_follows_first_token_eligibility() {
        let original = MemoryBudget::new(64 << 20);
        for limit in [0, 4096] {
            let memory = MemoryBudget::new(limit);
            for unchecked in ["SELECT 1; SELEC 1", "/* comment */ SELEC 1", "PG_SLEEP 1", "ADD 1"] {
                let error = crate::parse_in(unchecked, &original).err().unwrap();
                assert!(initial_statement_diagnostic(unchecked, &error, &memory).unwrap().is_none(), "{unchecked}");
            }
            let initial = "SELEC 1";
            let error = crate::parse_in(initial, &original).err().unwrap();
            let denied = initial_statement_diagnostic(initial, &error, &memory).unwrap_err();
            assert_eq!(denied.code, "rtdb/memory-budget-exceeded");
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}
