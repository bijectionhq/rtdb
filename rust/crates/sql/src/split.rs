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

/// Validates the whole source lexically, then admits the returned byte ranges.
/// The reservation covers the returned ranges; tokenizer scratch needs separate admission.
/// Empty input groups acquire no span charge; size, allocation and budget errors propagate as native errors.
pub fn split_statements_in(source: &str, memory: &Arc<MemoryBudget>) -> Result<SplitStatements> {
    let tokens = crate::parse::tokenize(source)?;
    let count = SpanBoundaryCursor::new(tokens.iter()).try_fold(0usize, |count, _| count.checked_add(1).ok_or_else(split_size))?;
    if count == 0 { return Ok(SplitStatements { storage: SplitStorage::Empty }); }
    let layout = Layout::array::<SourceSpan>(count).map_err(|_| split_size())?;
    let bytes = layout.size().checked_add(256).ok_or_else(split_size)?;
    let reservation = memory.reserve(bytes)?;
    let mut spans = Vec::new();
    spans.try_reserve_exact(count).map_err(|error| Anomaly::incorrect("rtdb/allocation-failed", error.to_string()))?;
    for span in SpanBoundaryCursor::new(tokens.into_iter()) {
        spans.push(crate::parse::source_span(source, span));
    }
    Ok(SplitStatements { storage: SplitStorage::Allocated { spans, _reservation: reservation } })
}

fn split_size() -> Anomaly { Anomaly::incorrect("rtdb.sql/split-size", "SQL split span storage exceeds addressable memory") }

#[cfg(test)]
mod tests {
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
            let charged = super::split_statements_in(source, &memory).unwrap();
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
            let charged = super::split_statements_in(source, &memory).unwrap();
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
            let Err(error) = super::split_statements_in(source, &memory) else { panic!("expected a lexical error"); };
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
        let Err(error) = super::split_statements_in(source, &short) else { panic!("expected span admission to fail"); };
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(short.used_bytes(), 0);
        assert_eq!(short.peak_bytes(), 0);
        let exact = rtdb_types::MemoryBudget::new(bytes);
        let charged = super::split_statements_in(source, &exact).unwrap();
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
        let charged = super::split_statements_in("SELECT 1; SELECT 2", &memory).unwrap();
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
}
