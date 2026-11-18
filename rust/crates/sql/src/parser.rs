mod grammar;
mod initial_statement;
mod prediction;

pub use initial_statement::initial_statement_diagnostic;
pub use prediction::InitialStatementDiagnostic;

pub(crate) fn is_unquoted_identifier(text: &str) -> bool {
    crate::lexer::keyword(text).is_none_or(|keyword| grammar::pinned_symbol(keyword.name())
        .is_some_and(prediction::is_identifier_token))
}

#[cfg(test)]
mod tests {
    use rtdb_plan::ControlStatement;
    use rtdb_types::{AllocationScope, MemoryBudget};
    use super::prediction::{CapturedClassPolicy, DirectControlError, DirectControlRecognition, ParserEnvironment, SourceAssertions};

    #[test]
    fn direct_control_boundary_exposes_owned_mismatch_and_cause_free_reports() {
        let retained = MemoryBudget::new(64 << 20);
        let requests = MemoryBudget::new(64 << 20);
        let (mismatch, unwanted) = {
            let policy = CapturedClassPolicy::capture_parser_atn(
                SourceAssertions::Enabled, |_| Ok::<_, std::convert::Infallible>(None),
            ).unwrap();
            let mut parser = ParserEnvironment::open_in(policy, &retained).unwrap();
            let mut parse = |source: &str| -> Result<ControlStatement, DirectControlError> {
                let source = source.to_owned();
                let work = AllocationScope::new(&requests).unwrap();
                parser.parse_direct_control(&source, &work)
            };
            let mismatch = parse("SELEC 1").unwrap_err();
            let unwanted = parse("extra SELECT 1").unwrap_err();
            assert_eq!(parse("ROLLBACK").unwrap(), ControlStatement::Rollback);
            (mismatch, unwanted)
        };

        let mismatch_fact = mismatch.recognition().unwrap();
        let unwanted_fact = unwanted.recognition().unwrap();
        assert!(matches!(mismatch_fact, DirectControlRecognition::InputMismatch { offending, .. }
            if offending.index == 0 && offending.span.end.byte == 5));
        assert_eq!(mismatch_fact.offending_state(), Some(574));
        assert!(matches!(unwanted_fact, DirectControlRecognition::CauseFreeToken { offending }
            if offending.index == 0 && offending.span.end.byte == 5));
        assert_eq!(unwanted_fact.offending_state(), None);
        assert_eq!(mismatch.anomaly().unwrap().code, "xtdb/sql-error");
        assert_eq!(unwanted.anomaly().unwrap().code, "xtdb/sql-error");
        assert_eq!(retained.used_bytes(), 0);
        assert!(requests.used_bytes() > 0);
        drop((mismatch, unwanted));
        assert_eq!(requests.used_bytes(), 0);
    }
}
