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
    let tokens = crate::parse::tokenize(source, None)?;
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
        crate::parse::tokenize_boundaries_into(source, Some(memory), &mut tokens.values, |values, token| {
            if values.len() == values.capacity() {
                let capacity = values.capacity().checked_mul(2).ok_or_else(token_array_size)?.max(4);
                let layout = Layout::array::<TokenWithSpan>(capacity).map_err(|_| token_array_size())?;
                let bytes = layout.size().checked_add(256).ok_or_else(token_array_size)?;
                let replacement_reservation = memory.reserve(bytes)?;
                let mut replacement = Vec::new();
                replacement.try_reserve_exact(capacity).map_err(|error| Anomaly::incorrect("rtdb/allocation-failed", error.to_string()).with_kind(rtdb_types::AnomalyKind::ResourceExhausted))?;
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

pub(super) fn boundary_lexical_bytes(source: &str) -> Option<usize> {
    source.len().checked_mul(8).and_then(|bytes| bytes.checked_add(1024))
}

/// Admits located-token array growth during lexical validation, then admits the returned byte ranges.
/// Source line and block comments occupy located slots with empty text fields.
/// The token array is released before return.
/// Other tokenizer payloads and scratch need separate admission.
/// Empty statement groups acquire no span charge.
/// Size, allocation and budget errors propagate as native errors.
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
    spans.try_reserve_exact(count).map_err(|error| Anomaly::incorrect("rtdb/allocation-failed", error.to_string()).with_kind(rtdb_types::AnomalyKind::ResourceExhausted))?;
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
        let mut tokens = crate::parse::tokenize(source, None)?;
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

#[cfg(test)]
mod boundary_line_comment_tests {
    use std::alloc::Layout;

    use rtdb_plan::SourceSpan;
    use rtdb_types::MemoryBudget;
    use sqlparser::tokenizer::{Location, Span, Token, TokenWithSpan, Whitespace};

    use super::{SplitTokens, split_statements, split_statements_in};
    use crate::{ExecutableSyntax, parse_statement_batch};

    fn array_charge(capacity: usize) -> usize {
        Layout::array::<TokenWithSpan>(capacity).unwrap().size() + 256
    }

    #[test]
    fn admitted_token_array_keeps_long_comment_locations_without_allocated_payloads() {
        const REPEATS: usize = 32768;
        let body = format!("{}\rβ\r", "é😀;".repeat(REPEATS));
        let source = format!("x;--{body}\nz");
        let first = array_charge(4);
        let next = array_charge(8);
        let memory = MemoryBudget::new(first + next);
        let boundary = SplitTokens::tokenize_in(&source, &memory).unwrap();
        assert_eq!(boundary.values.len(), 5);
        assert_eq!(memory.used_bytes(), next);
        assert_eq!(memory.peak_bytes(), first + next);

        let end = Location::new(1, (3 * REPEATS + 8) as u64);
        let token = &boundary.values[2];
        assert_eq!(token.span, Span::new(Location::new(1, 3), end));
        assert_eq!(crate::parse::source_span(&source, token.span), SourceSpan { start: 2, end: 7 * REPEATS + 8 });
        assert!(matches!(&token.token, Token::Whitespace(Whitespace::SingleLineComment { prefix, comment })
            if prefix.capacity() == 0 && comment.capacity() == 0));
        assert_eq!(boundary.values[3], Token::Whitespace(Whitespace::Newline));
        assert_eq!(boundary.values[3].span, Span::new(end, Location::new(2, 1)));
        assert_eq!(boundary.values[4].span, Span::new(Location::new(2, 1), Location::new(2, 2)));

        let full = crate::parse::tokenize(&source, None).unwrap();
        assert_eq!((&boundary.values[..2], &boundary.values[3..]), (&full[..2], &full[3..]));
        assert_eq!(token.span, full[2].span);
        assert!(matches!(&full[2].token, Token::Whitespace(Whitespace::SingleLineComment { prefix, comment })
            if prefix == "--" && comment == &body));
        drop(boundary);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn admitted_ranges_keep_original_bytes_across_comment_endings_and_quoted_semicolons() {
        let first = "SELECT 'é' -- 😀; inside\rignored\r\n, $$a;b$$";
        let source = format!("{first}; /* outer /* ; */ end */ SELECT 2; -- EOF é");
        let memory = MemoryBudget::new(1 << 20);
        let ranges = split_statements_in(&source, &memory).unwrap();
        let full = split_statements(&source).unwrap();
        assert_eq!(ranges.as_slice(), full.as_slice());
        assert_eq!(ranges.as_slice().iter().map(|span| &source[span.start..span.end]).collect::<Vec<_>>(),
            [first, "SELECT 2"]);
        drop(ranges);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn line_comment_slots_preserve_admission_and_lexical_error_order() {
        let first = array_charge(4);
        for (source, expected_span) in [
            ("x;--😀\r\n@", serde_json::json!({"start": 10, "end": 11})),
            ("x;--😀\r\n._x", serde_json::json!({"start": 10, "end": 10})),
        ] {
            let memory = MemoryBudget::new(first);
            let error = split_statements_in(source, &memory).err().expect("reached lexical error");
            assert_eq!(error, split_statements(source).unwrap_err());
            assert_eq!(error.code, "xtdb/sql-error");
            assert_eq!(error.context["position"], serde_json::json!(11));
            assert_eq!(error.context["span"], expected_span);
            assert_eq!(memory.peak_bytes(), first);
            assert_eq!(memory.used_bytes(), 0);
        }
        let memory = MemoryBudget::new(first);
        let source = "x;--😀\r\ny 'unterminated";
        let error = split_statements_in(source, &memory).err().expect("fifth token must reserve more space");
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(split_statements(source).unwrap_err().code, "xtdb/sql-error");
        assert_eq!(memory.peak_bytes(), first);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn admitted_sql_batch_keeps_comments_in_original_command_and_prepare_ranges() {
        let first = "SELECT 'é' -- 😀; still comment\rignored\r\n, 2";
        let command = "SELECT 3 -- é; inside\n, 4";
        let inner = format!("PREPARE inner_q AS\n {command}");
        let outer = format!("PREPARE outer_q AS {inner}");
        let source = format!("{first}; {outer};");
        let memory = MemoryBudget::new(1 << 20);
        let mut batch = parse_statement_batch(&source, &memory).unwrap();
        let first_statement = batch.next().unwrap();
        assert!(matches!(&first_statement.command, ExecutableSyntax::Sql(_)));
        assert!(first_statement.preparations.is_empty());
        assert_eq!(first_statement.command.source(), first);
        let wrapped = batch.next().unwrap();
        assert!(matches!(&wrapped.command, ExecutableSyntax::Sql(_)));
        assert_eq!(wrapped.command.source(), command);
        assert_eq!(wrapped.preparations.len(), 2);
        assert_eq!((wrapped.preparations[0].name.as_str(), wrapped.preparations[0].source),
            ("outer_q", outer.as_str()));
        assert_eq!((wrapped.preparations[1].name.as_str(), wrapped.preparations[1].source),
            ("inner_q", inner.as_str()));
        assert!(batch.next().is_none());
        drop(first_statement);
        drop(wrapped);
        drop(batch);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn later_sql_batch_grammar_error_after_unicode_comments_keeps_original_byte_span() {
        let source = "SELECT 1 -- 😀; ignored\r\n; SELECT 2;; SELECT 3";
        let memory = MemoryBudget::new(1 << 20);
        let error = parse_statement_batch(source, &memory).unwrap_err();
        assert_eq!(error.code, "xtdb/sql-error");
        assert_eq!(error.message.as_deref(), Some("Expected an SQL statement between semicolons"));
        assert_eq!(error.context["position"], serde_json::json!(39));
        assert_eq!(error.context["span"], serde_json::json!({"start": 38, "end": 39}));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[cfg(test)]
mod boundary_block_comment_tests {
    use std::alloc::Layout;

    use rtdb_plan::SourceSpan;
    use rtdb_types::MemoryBudget;
    use sqlparser::tokenizer::{Location, Span, Token, TokenWithSpan, Whitespace};

    use super::{SplitTokens, split_statements, split_statements_in};
    use crate::{ExecutableSyntax, parse_statement_batch};

    fn array_charge(capacity: usize) -> usize {
        Layout::array::<TokenWithSpan>(capacity).unwrap().size() + 256
    }

    #[test]
    fn admitted_nested_block_comments_keep_one_located_slot_without_a_body_allocation() {
        const REPEATS: usize = 32768;
        let body = format!("{}/* inner; */\r\nβ", "é😀;".repeat(REPEATS));
        let source = format!("x;/*{body}*/ z");
        let first = array_charge(4);
        let next = array_charge(8);
        let memory = MemoryBudget::new(first + next);
        let boundary = SplitTokens::tokenize_in(&source, &memory).unwrap();
        assert_eq!(boundary.values.len(), 5);
        assert_eq!(memory.used_bytes(), next);
        assert_eq!(memory.peak_bytes(), first + next);

        let token = &boundary.values[2];
        assert!(matches!(&token.token, Token::Whitespace(Whitespace::MultiLineComment(text))
            if text.capacity() == 0));
        assert_eq!(token.span, Span::new(Location::new(1, 3), Location::new(2, 4)));
        assert_eq!(crate::parse::source_span(&source, token.span), SourceSpan { start: 2, end: body.len() + 6 });
        assert_eq!(boundary.values[3], Token::Whitespace(Whitespace::Space));
        assert_eq!(boundary.values[3].span, Span::new(Location::new(2, 4), Location::new(2, 5)));
        assert_eq!(boundary.values[4].span, Span::new(Location::new(2, 5), Location::new(2, 6)));

        let full = crate::parse::tokenize(&source, None).unwrap();
        assert_eq!((&boundary.values[..2], &boundary.values[3..]), (&full[..2], &full[3..]));
        assert_eq!(token.span, full[2].span);
        assert!(matches!(&full[2].token, Token::Whitespace(Whitespace::MultiLineComment(text))
            if text == &body));
        drop(boundary);
        assert_eq!(memory.used_bytes(), 0);

        let memory = MemoryBudget::new(first);
        assert!(split_statements_in("/**/", &memory).unwrap().is_empty());
        assert_eq!(memory.peak_bytes(), first);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn nested_block_comment_errors_keep_token_array_and_lexical_precedence() {
        let capacity = array_charge(4);
        let source = "x;--😀\r\n/* outer\r\n/* inner */β";
        let memory = MemoryBudget::new(capacity);
        let error = split_statements_in(source, &memory).err().expect("unterminated block comment");
        assert_eq!(error, split_statements(source).unwrap_err());
        assert_eq!(error.code, "xtdb/sql-error");
        assert!(error.message.as_deref().unwrap().contains("Unexpected EOF while in a multi-line comment"));
        assert_eq!(error.context["position"], serde_json::json!(source.len() + 1));
        assert_eq!(error.context["span"], serde_json::json!({"start": source.len(), "end": source.len()}));
        assert_eq!(memory.peak_bytes(), capacity);
        assert_eq!(memory.used_bytes(), 0);

        let prefix = "x;/* é/* inner */\r\n😀 */ ";
        let source = format!("{prefix}@");
        let memory = MemoryBudget::new(capacity);
        let error = split_statements_in(&source, &memory).err().expect("fifth token validation");
        assert_eq!(error, split_statements(&source).unwrap_err());
        assert_eq!(error.code, "xtdb/sql-error");
        assert_eq!(error.context["position"], serde_json::json!(prefix.len() + 1));
        assert_eq!(error.context["span"], serde_json::json!({"start": prefix.len(), "end": prefix.len() + 1}));
        assert_eq!(memory.peak_bytes(), capacity);
        assert_eq!(memory.used_bytes(), 0);

        let source = format!("{prefix}y 'unterminated");
        let memory = MemoryBudget::new(capacity);
        let error = split_statements_in(&source, &memory).err().expect("fifth token admission");
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(split_statements(&source).unwrap_err().code, "xtdb/sql-error");
        assert_eq!(memory.peak_bytes(), capacity);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn admitted_statement_ranges_and_simple_query_syntax_keep_original_block_comment_bytes() {
        let first = "SELECT 'é' /* outer;\r\n /* 😀; */ β */, 2";
        let second = "SELECT 3 /*!50110 ; SELECT 99; */ , 4";
        let source = format!("{first}; {second};");
        let memory = MemoryBudget::new(1 << 20);
        let ranges = split_statements_in(&source, &memory).unwrap();
        let full = split_statements(&source).unwrap();
        assert_eq!(ranges.as_slice(), full.as_slice());
        assert_eq!(ranges.as_slice(), [
            SourceSpan { start: 0, end: first.len() },
            SourceSpan { start: first.len() + 2, end: source.len() - 1 },
        ]);
        drop(ranges);
        assert_eq!(memory.used_bytes(), 0);

        let mut batch = parse_statement_batch(&source, &memory).unwrap();
        for text in [first, second] {
            let statement = batch.next().unwrap();
            assert!(matches!(&statement.command, ExecutableSyntax::Sql(_)));
            assert_eq!(statement.command.source(), text);
        }
        assert!(batch.next().is_none());
        drop(batch);
        assert_eq!(memory.used_bytes(), 0);
    }
}
