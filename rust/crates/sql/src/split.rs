use std::{alloc::Layout, borrow::Borrow, sync::Arc};
use rtdb_plan::SourceSpan;
use rtdb_types::{Anomaly, MemoryBudget, MemoryReservation, Result};
use sqlparser::tokenizer::{Span, Token, TokenWithSpan};

/// Statement byte ranges retaining their storage reservation until dropped.
pub struct SplitStatements { storage: SplitStorage }

enum SplitStorage {
    Empty,
    Allocated { spans: Vec<SourceSpan>, _reservation: Arc<MemoryReservation> },
}

impl SplitStatements {
    /// Returns byte offsets in the caller's original source.
    pub fn as_slice(&self) -> &[SourceSpan] {
        match &self.storage { SplitStorage::Empty => &[], SplitStorage::Allocated { spans, .. } => spans }
    }

    /// Returns whether the source contained no nonempty statement groups.
    pub fn is_empty(&self) -> bool { self.as_slice().is_empty() }
}

struct SpanBoundaryCursor<I> { tokens: I }
impl<I> SpanBoundaryCursor<I> { fn new(tokens: I) -> Self { Self { tokens } } }
impl<I: Iterator> Iterator for SpanBoundaryCursor<I> where I::Item: Borrow<TokenWithSpan> {
    type Item = Span;
    fn next(&mut self) -> Option<Span> {
        let mut start = None;
        let mut end = None;
        for token in self.tokens.by_ref() {
            let token: &TokenWithSpan = token.borrow();
            match &token.token {
                Token::Whitespace(_) | Token::EOF => (),
                Token::SemiColon => {
                    if let (Some(start), Some(end)) = (start.take(), end.take()) { return Some(Span::new(start, end)); }
                }
                _ => { start.get_or_insert(token.span.start); end = Some(token.span.end); }
            }
        }
        start.zip(end).map(|(start, end)| Span::new(start, end))
    }
}

pub fn split_statements(source: &str) -> Result<Vec<SourceSpan>> {
    let tokens = crate::parse::tokenize(source)?;
    Ok(SpanBoundaryCursor::new(tokens.into_iter()).map(|span| crate::parse::source_span(source, span)).collect())
}

pub(super) struct SplitTokens {
    pub(super) values: Vec<TokenWithSpan>,
    reservation: Option<Arc<MemoryReservation>>,
}

impl SplitTokens {
    pub(super) fn tokenize_in(source: &str, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let mut tokens = Self { values: Vec::new(), reservation: None };
        let reservation = &mut tokens.reservation;
        crate::parse::tokenize_into(source, &mut tokens.values, |values, token| {
            if values.len() == values.capacity() {
                let capacity = values.capacity().checked_mul(2).ok_or_else(token_array_size)?.max(4);
                let layout = Layout::array::<TokenWithSpan>(capacity).map_err(|_| token_array_size())?;
                let bytes = layout.size().checked_add(256).ok_or_else(token_array_size)?;
                let replacement_reservation = memory.reserve(bytes)?;
                let mut replacement = Vec::new();
                replacement.try_reserve_exact(capacity).map_err(|error| Anomaly::incorrect("rtdb/allocation-failed", error.to_string()))?;
                replacement.append(values);
                drop(std::mem::replace(values, replacement));
                *reservation = Some(replacement_reservation);
            }
            values.push(token);
            Ok(())
        })?;
        Ok(tokens)
    }
}

fn token_array_size() -> Anomaly { Anomaly::incorrect("rtdb.sql/token-array-size", "SQL token array exceeds addressable memory") }

/// Admits located-token array growth during lexical validation, then admits the returned byte ranges.
/// The token array is released before return. Tokenizer payloads and scratch need separate admission.
/// Empty statement groups acquire no span charge. Size, allocation and budget errors propagate as native errors.
pub fn split_statements_in(source: &str, memory: &Arc<MemoryBudget>) -> Result<SplitStatements> {
    let mut tokens = SplitTokens::tokenize_in(source, memory)?;
    collect_split_ranges(source, &mut tokens.values, memory)
}

fn collect_split_ranges(source: &str, tokens: &mut Vec<TokenWithSpan>, memory: &Arc<MemoryBudget>) -> Result<SplitStatements> {
    let count = SpanBoundaryCursor::new(tokens.iter()).try_fold(0usize, |count, _| count.checked_add(1).ok_or_else(split_size))?;
    if count == 0 { return Ok(SplitStatements { storage: SplitStorage::Empty }); }
    let layout = Layout::array::<SourceSpan>(count).map_err(|_| split_size())?;
    let bytes = layout.size().checked_add(256).ok_or_else(split_size)?;
    let reservation = memory.reserve(bytes)?;
    let mut spans = Vec::new();
    spans.try_reserve_exact(count).map_err(|error| Anomaly::incorrect("rtdb/allocation-failed", error.to_string()))?;
    for span in SpanBoundaryCursor::new(tokens.drain(..)) {
        spans.push(crate::parse::source_span(source, span));
    }
    Ok(SplitStatements { storage: SplitStorage::Allocated { spans, _reservation: reservation } })
}

fn split_size() -> Anomaly { Anomaly::incorrect("rtdb.sql/split-size", "SQL split span storage exceeds addressable memory") }

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rtdb_types::{MemoryBudget, Result};

    fn split_ranges_fixture(source: &str, memory: &Arc<MemoryBudget>) -> Result<super::SplitStatements> {
        let mut tokens = crate::parse::tokenize(source)?;
        super::collect_split_ranges(source, &mut tokens, memory)
    }

    #[test]
    fn protocol_split_preserves_quoted_semicolons_and_custom_control_syntax() {
        let source = "-- setup\nBEGIN READ WRITE WITH (ASYNC = true); SELECT ';', $$a;b$$; /* ; */ COPY foo FROM STDIN WITH (FORMAT 'arrow-stream');";
        let statements = super::split_statements(source).unwrap();
        let text: Vec<_> = statements.iter().map(|span| &source[span.start..span.end]).collect();
        assert_eq!(text, ["BEGIN READ WRITE WITH (ASYNC = true)", "SELECT ';', $$a;b$$", "COPY foo FROM STDIN WITH (FORMAT 'arrow-stream')"]);
    }
    #[test]
    fn comment_only_sql_has_no_statement() { assert!(super::split_statements(" -- idle\n /* also idle */ ; ").unwrap().is_empty()); }

    #[test]
    fn charged_split_preserves_source_bytes_across_quoted_and_empty_groups() {
        for (source, expected) in [
            ("; -- setup\n SELECT 'é;😀', $$a;b$$; /* outer /* ; */ end */ ; SELECT \"a;é\"; ;", vec!["SELECT 'é;😀', $$a;b$$", "SELECT \"a;é\""]),
            ("BEGIN READ WRITE WITH (ASYNC = true); COPY foo FROM STDIN WITH (FORMAT 'arrow-stream');", vec!["BEGIN READ WRITE WITH (ASYNC = true)", "COPY foo FROM STDIN WITH (FORMAT 'arrow-stream')"]),
            ("SELECT 1 -- trailing\n; SELECT 2 /* trailing */", vec!["SELECT 1", "SELECT 2"]),
        ] {
            let memory = rtdb_types::MemoryBudget::new(4096);
            let charged = split_ranges_fixture(source, &memory).unwrap();
            assert_eq!(charged.as_slice(), super::split_statements(source).unwrap().as_slice());
            let text: Vec<_> = charged.as_slice().iter().map(|span| &source[span.start..span.end]).collect();
            assert_eq!(text, expected);
            assert_eq!(memory.used_bytes(), expected.len() * std::mem::size_of::<rtdb_plan::SourceSpan>() + 256);
            drop(charged);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn empty_split_groups_need_no_span_reservation() {
        let memory = rtdb_types::MemoryBudget::new(0);
        for source in ["", "; ; ;", " -- idle\n /* outer /* ; */ end */ ; "] {
            let charged = split_ranges_fixture(source, &memory).unwrap();
            assert!(charged.is_empty());
            assert_eq!(charged.as_slice(), &[]);
            assert_eq!(memory.used_bytes(), 0);
            assert_eq!(memory.peak_bytes(), 0);
        }
    }

    #[test]
    fn lexical_errors_precede_span_admission_and_later_tokenizer_errors() {
        let memory = rtdb_types::MemoryBudget::new(0);
        for (source, first) in [
            ("SELECT 1 AS \"bad/name\"; SELECT 'unterminated", '"'),
            ("SELECT a <@ b; SELECT 'unterminated", '@'),
            ("SELECT a ^@ b; SELECT 'unterminated", '^'),
        ] {
            let Err(error) = split_ranges_fixture(source, &memory) else { panic!("expected a lexical error"); };
            assert_eq!(error, super::split_statements(source).unwrap_err());
            assert_eq!(error.code, "xtdb/sql-error");
            assert_eq!(error.context["position"], serde_json::json!(source.find(first).unwrap() + 1));
            assert_eq!(memory.used_bytes(), 0);
            assert_eq!(memory.peak_bytes(), 0);
        }
    }

    #[test]
    fn span_storage_admits_nonempty_groups_at_the_exact_budget() {
        let source = "; ; SELECT 1; -- ignored\n ; /* ; */ SELECT 2; ; SELECT 3; ;";
        let bytes = 3 * std::mem::size_of::<rtdb_plan::SourceSpan>() + 256;
        let short = rtdb_types::MemoryBudget::new(bytes - 1);
        let Err(error) = split_ranges_fixture(source, &short) else { panic!("expected span admission to fail"); };
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(short.used_bytes(), 0);
        assert_eq!(short.peak_bytes(), 0);
        let exact = rtdb_types::MemoryBudget::new(bytes);
        let charged = split_ranges_fixture(source, &exact).unwrap();
        assert_eq!(charged.as_slice().len(), 3);
        assert_eq!(exact.used_bytes(), bytes);
        assert_eq!(exact.peak_bytes(), bytes);
        drop(charged);
        assert_eq!(exact.used_bytes(), 0);
    }

    #[test]
    fn cancelling_a_suspended_owner_releases_span_storage() {
        use std::{future::Future, task::{Context, Waker}};
        let bytes = 2 * std::mem::size_of::<rtdb_plan::SourceSpan>() + 256;
        let memory = rtdb_types::MemoryBudget::new(bytes);
        let charged = split_ranges_fixture("SELECT 1; SELECT 2", &memory).unwrap();
        let mut suspended = Box::pin(async move {
            std::future::pending::<()>().await;
            charged.as_slice().len()
        });
        let mut context = Context::from_waker(Waker::noop());
        assert!(suspended.as_mut().poll(&mut context).is_pending());
        assert_eq!(memory.used_bytes(), bytes);
        drop(suspended);
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(memory.peak_bytes(), bytes);
    }

    fn token_array_bytes(capacity: usize) -> usize {
        capacity * std::mem::size_of::<sqlparser::tokenizer::TokenWithSpan>() + 256
    }

    #[test]
    fn combined_split_holds_the_token_array_during_exact_range_admission() {
        let array = token_array_bytes(4);
        let spans = 2 * std::mem::size_of::<rtdb_plan::SourceSpan>() + 256;
        let short = MemoryBudget::new(array + spans - 1);
        let Err(error) = super::split_statements_in("x;y", &short) else { panic!("range admission must fail"); };
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(short.peak_bytes(), array);
        assert_eq!(short.used_bytes(), 0);

        let exact = MemoryBudget::new(array + spans);
        let split = super::split_statements_in("x;y", &exact).unwrap();
        assert_eq!(split.as_slice(), &[
            rtdb_plan::SourceSpan { start: 0, end: 1 },
            rtdb_plan::SourceSpan { start: 2, end: 3 },
        ]);
        assert_eq!(exact.peak_bytes(), array + spans);
        assert_eq!(exact.used_bytes(), spans);
        drop(split);
        assert_eq!(exact.used_bytes(), 0);
    }

    #[test]
    fn whitespace_token_array_growth_overlaps_and_releases_the_existing_lease() {
        let zero = MemoryBudget::new(0);
        assert!(super::split_statements_in("", &zero).unwrap().is_empty());
        assert_eq!(zero.peak_bytes(), 0);
        assert_eq!(zero.used_bytes(), 0);

        let array = token_array_bytes(4);
        let next = token_array_bytes(8);
        let short = MemoryBudget::new(array + next - 1);
        let Err(error) = super::split_statements_in("     ", &short) else { panic!("replacement array admission must fail"); };
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(short.peak_bytes(), array);
        assert_eq!(short.used_bytes(), 0);

        let exact = MemoryBudget::new(array + next);
        assert!(super::split_statements_in("     ", &exact).unwrap().is_empty());
        assert_eq!(exact.peak_bytes(), array + next);
        assert_eq!(exact.used_bytes(), 0);
    }

    #[test]
    fn token_array_growth_observes_current_lexical_errors_before_reserving() {
        let first = token_array_bytes(4);
        for source in ["x x 'unterminated", "x x @ 'unterminated", "@ x y 'unterminated"] {
            let memory = MemoryBudget::new(first);
            let Err(error) = super::split_statements_in(source, &memory) else { panic!("expected lexical error"); };
            assert_eq!(error, super::split_statements(source).unwrap_err());
            assert_eq!(error.code, "xtdb/sql-error");
            assert_eq!(memory.peak_bytes(), first);
            assert_eq!(memory.used_bytes(), 0);
        }

        let zero = MemoryBudget::new(0);
        for source in ["'unterminated", "@"] {
            let Err(error) = super::split_statements_in(source, &zero) else { panic!("expected lexical error"); };
            assert_eq!(error, super::split_statements(source).unwrap_err());
        }
        assert_eq!(zero.peak_bytes(), 0);
        assert_eq!(zero.used_bytes(), 0);
    }

    #[test]
    fn denied_token_array_growth_precedes_unreached_lexical_failure() {
        let first = token_array_bytes(4);
        let memory = MemoryBudget::new(first);
        let source = "x x y 'unterminated";
        let Err(error) = super::split_statements_in(source, &memory) else { panic!("expected token array admission error"); };
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(super::split_statements(source).unwrap_err().code, "xtdb/sql-error");
        assert_eq!(memory.peak_bytes(), first);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn combined_split_keeps_lexical_diagnostics_and_quoted_spans_with_capacity() {
        let memory = MemoryBudget::new(1 << 20);
        let source = r#"; SELECT 'é;😀', $$a;b$$; /* outer /* ; */ end */ ; SELECT "a;é"; ;"#;
        let spans = super::split_statements_in(source, &memory).unwrap();
        assert_eq!(spans.as_slice(), super::split_statements(source).unwrap().as_slice());
        assert_eq!(spans.as_slice().iter().map(|span| &source[span.start..span.end]).collect::<Vec<_>>(),
            ["SELECT 'é;😀', $$a;b$$", r#"SELECT "a;é""#]);
        drop(spans);
        for source in [
            r#"SELECT 1 AS "bad/name"; SELECT 'unterminated"#,
            "SELECT a <@ b; SELECT 'unterminated",
            "SELECT a ^@ b; SELECT 'unterminated",
        ] {
            let Err(error) = super::split_statements_in(source, &memory) else { panic!("expected lexical error"); };
            assert_eq!(error, super::split_statements(source).unwrap_err());
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}
