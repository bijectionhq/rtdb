mod comment;
mod stream;
mod vocabulary;
#[cfg(test)]
mod tests;

use std::{fmt::Write, sync::Arc};
use rtdb_types::{AllocationScope, Anomaly, MemoryBudget, Result};
pub(crate) use vocabulary::{Keyword, Punctuation, keyword};
#[cfg(test)]
pub(crate) use vocabulary::spellings;
pub(crate) use stream::{IndexedLexeme, StreamError, TokenStream};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Position {
    pub(crate) byte: usize,
    pub(crate) code_point: usize,
    pub(crate) line: usize,
    pub(crate) column: usize,
}

impl Position {
    const START: Self = Self { byte: 0, code_point: 0, line: 1, column: 0 };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Span {
    pub(crate) start: Position,
    pub(crate) end: Position,
}

#[derive(Clone, Copy, Debug)]
struct Source<'sql>(&'sql str);

#[derive(Clone, Copy, Debug)]
struct Cursor<'sql> {
    source: Source<'sql>,
    position: Position,
}

impl<'sql> Cursor<'sql> {
    fn remaining(self) -> &'sql str { &self.source.0[self.position.byte..] }
    fn peek(self) -> Option<char> { self.remaining().chars().next() }
    fn take(&mut self) -> Option<char> {
        let value = self.peek()?;
        self.position.byte += value.len_utf8();
        self.position.code_point += 1;
        if value == '\n' { self.position.line += 1; self.position.column = 0; }
        else { self.position.column += 1; }
        Some(value)
    }
    fn bump(&mut self) { let _ = self.take(); }
    fn advance(mut self, characters: usize) -> Self {
        for _ in 0..characters { self.bump(); }
        self
    }
    fn while_matches(mut self, predicate: impl Fn(char) -> bool) -> Self {
        while self.peek().is_some_and(&predicate) { self.bump(); }
        self
    }
    fn lexeme(self, kind: Kind, end: Self) -> Lexeme<'sql> {
        Lexeme { source: self.source, kind, span: Span { start: self.position, end: end.position } }
    }
    fn error(self, failure: Self) -> RecognitionError<'sql> {
        RecognitionError { source: self.source, span: Span { start: self.position, end: failure.advance(1).position } }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Delimiter { DoubleQuote, Backtick }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Eof,
    Keyword(Keyword),
    Punctuation(Punctuation),
    Identifier,
    DelimitedIdentifier(Delimiter),
    Integer,
    Float,
    CharacterString,
    EscapedString,
    BinaryString,
    Parameter,
    DollarStart,
    DollarText,
    DollarEnd,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Lexeme<'sql> {
    source: Source<'sql>,
    kind: Kind,
    span: Span,
}

impl<'sql> Lexeme<'sql> {
    pub(crate) fn kind(self) -> Kind { self.kind }
    pub(crate) fn span(self) -> Span { self.span }
    pub(crate) fn raw(self) -> &'sql str { &self.source.0[self.span.start.byte..self.span.end.byte] }
    #[cfg(test)]
    pub(crate) fn body(self) -> Option<&'sql str> {
        let raw = self.raw();
        match self.kind {
            Kind::CharacterString | Kind::DelimitedIdentifier(_) => Some(&raw[1..raw.len() - 1]),
            Kind::EscapedString => Some(&raw[2..raw.len() - 1]),
            Kind::BinaryString => Some(&raw[3..raw.len() - 2]),
            Kind::DollarText => Some(raw),
            _ => None,
        }
    }
    #[cfg(test)]
    pub(crate) fn tag(self) -> Option<&'sql str> {
        matches!(self.kind, Kind::DollarStart | Kind::DollarEnd).then(|| {
            let raw = self.raw();
            &raw[1..raw.len() - 1]
        })
    }
}

#[derive(Debug)]
pub(crate) enum ScanError<'sql> {
    Admission(Anomaly),
    Recognition(RecognitionError<'sql>),
}

impl From<Anomaly> for ScanError<'_> {
    fn from(value: Anomaly) -> Self { Self::Admission(value) }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RecognitionError<'sql> {
    source: Source<'sql>,
    span: Span,
}

impl RecognitionError<'_> {
    #[cfg(test)]
    pub(crate) fn span(self) -> Span { self.span }
    pub(crate) fn for_delimited_identifier(source: &str, start: usize, end: usize) -> Option<RecognitionError<'_>> {
        if !source[start..].starts_with(['"', '`']) { return None; }
        let prefix = &source[..start];
        let line_prefix = prefix.rsplit_once('\n').map_or(prefix, |(_, line)| line);
        let position = Position {
            byte: start,
            code_point: prefix.chars().count(),
            line: prefix.bytes().filter(|byte| *byte == b'\n').count() + 1,
            column: line_prefix.chars().count(),
        };
        let mut cursor = Cursor { source: Source(source), position };
        while cursor.position.byte < end {
            let delimiter = match cursor.peek()? {
                '"' => Delimiter::DoubleQuote,
                '`' => Delimiter::Backtick,
                _ => return None,
            };
            let candidate = quoted(cursor, QuotedRule::Identifier(delimiter));
            let Some(accepted) = candidate.accepted else { return Some(cursor.error(candidate.failure)); };
            cursor = accepted;
        }
        None
    }

    #[cfg(test)]
    pub(crate) fn render(self, allocation: &AllocationScope) -> Result<Anomaly> {
        self.render_anomaly(|bytes| allocation.reserve(bytes))
    }

    pub(crate) fn compatibility_anomaly(self, memory: Option<&Arc<MemoryBudget>>) -> Anomaly {
        let span = self.span;
        let render = self.render_anomaly(|bytes| {
            let bytes = bytes.checked_add(256).ok_or_else(size_error)?;
            memory.map(|memory| memory.reserve(bytes)).transpose()
        });
        match render {
            Ok(anomaly) => anomaly.with_context("position", serde_json::json!(span.start.byte + 1))
                .with_context("span", serde_json::json!({"start": span.start.byte, "end": span.end.byte})),
            Err(error) => error,
        }
    }

    fn render_anomaly<T>(self, admit: impl FnOnce(usize) -> Result<T>) -> Result<Anomaly> {
        const CODE: &str = "xtdb/sql-error";
        const PREFIX: &str = "Errors parsing SQL statement:\n  - line ";
        const RECOGNITION: &str = " token recognition error at: '";
        let text = &self.source.0[self.span.start.byte..self.span.end.byte];
        let digits = |value: usize| if value == 0 { 1 } else { value.ilog10() as usize + 1 };
        let escaped = text.chars().try_fold(0usize, |size, value| {
            size.checked_add(if matches!(value, '\n' | '\r' | '\t') { 2 } else { value.len_utf8() }).ok_or_else(size_error)
        })?;
        let capacity = PREFIX.len().checked_add(digits(self.span.start.line))
            .and_then(|size| size.checked_add(1 + digits(self.span.start.column)))
            .and_then(|size| size.checked_add(RECOGNITION.len()))
            .and_then(|size| size.checked_add(escaped))
            .and_then(|size| size.checked_add(1)).ok_or_else(size_error)?;
        let _reservation = admit(capacity.checked_add(CODE.len()).ok_or_else(size_error)?)?;
        let mut message = String::with_capacity(capacity);
        write!(&mut message, "{PREFIX}{}:{}{RECOGNITION}", self.span.start.line, self.span.start.column).expect("String writing is infallible");
        for value in text.chars() {
            match value {
                '\n' => message.push_str("\\n"),
                '\r' => message.push_str("\\r"),
                '\t' => message.push_str("\\t"),
                value => message.push(value),
            }
        }
        message.push('\'');
        Ok(Anomaly::incorrect(CODE, message))
    }
}

#[derive(Clone, Copy)]
enum Mode { Default, Dollar { opening: Span } }

#[derive(Clone, Copy)]
struct LexerState<'sql> { cursor: Cursor<'sql>, mode: Mode }

pub(crate) struct Lexer<'sql, 'scope> {
    state: LexerState<'sql>,
    allocation: &'scope AllocationScope,
}

impl<'sql, 'scope> Lexer<'sql, 'scope> {
    pub(crate) fn new(source: &'sql str, allocation: &'scope AllocationScope) -> Self {
        Self { state: LexerState { cursor: Cursor { source: Source(source), position: Position::START }, mode: Mode::Default }, allocation }
    }

    pub(crate) fn next(&mut self) -> std::result::Result<Lexeme<'sql>, ScanError<'sql>> {
        loop {
            let start = self.state.cursor;
            let Some(first) = start.peek() else { return Ok(start.lexeme(Kind::Eof, start)); };
            if let Mode::Dollar { opening } = self.state.mode { return Ok(self.dollar(opening)); }
            if let Some(end) = simple_trivia_end(start, first) {
                self.state = LexerState { cursor: end, mode: Mode::Default };
                continue;
            }
            if start.remaining().starts_with("/*") {
                if let Some(end) = comment::end(start, self.allocation)? { self.state = LexerState { cursor: end, mode: Mode::Default }; continue; }
            }
            let mut mode = Mode::Default;
            let (kind, end) = if identifier_start(first) {
                word_or_literal(start, first)
            } else if first.is_ascii_digit() {
                number(start)
            } else if matches!(first, '\'' | '"' | '`') {
                let rule = match first { '\'' => QuotedRule::Standard, '"' => QuotedRule::Identifier(Delimiter::DoubleQuote), _ => QuotedRule::Identifier(Delimiter::Backtick) };
                let candidate = quoted(start, rule);
                let Some(end) = candidate.accepted else { return Err(ScanError::Recognition(start.error(candidate.failure))); };
                (match rule { QuotedRule::Standard => Kind::CharacterString, QuotedRule::Escaped => Kind::EscapedString, QuotedRule::Identifier(delimiter) => Kind::DelimitedIdentifier(delimiter) }, end)
            } else if first == '$' {
                let after = start.advance(1);
                if after.peek().is_some_and(|value| value.is_ascii_digit()) {
                    (Kind::Parameter, after.while_matches(|value| value.is_ascii_digit()))
                } else {
                    let mut end = after;
                    if end.peek().is_some_and(identifier_start) { end = end.while_matches(tag_part); }
                    if end.peek() != Some('$') { return Err(ScanError::Recognition(start.error(end))); }
                    end.bump();
                    mode = Mode::Dollar { opening: Span { start: start.position, end: end.position } };
                    (Kind::DollarStart, end)
                }
            } else if let Some((punctuation, width)) = vocabulary::punctuation(start.remaining()) {
                (Kind::Punctuation(punctuation), start.advance(width))
            } else {
                let failure = if first == '!' { start.advance(1) } else { start };
                return Err(ScanError::Recognition(start.error(failure)));
            };
            self.state = LexerState { cursor: end, mode };
            return Ok(start.lexeme(kind, end));
        }
    }

    fn dollar(&mut self, opening: Span) -> Lexeme<'sql> {
        let start = self.state.cursor;
        let tag = &start.source.0[opening.start.byte..opening.end.byte];
        let (kind, end, mode) = if start.remaining().starts_with(tag) {
            (Kind::DollarEnd, start.advance(tag.len()), Mode::Default)
        } else if start.peek() == Some('$') {
            (Kind::DollarText, start.advance(1), Mode::Dollar { opening })
        } else {
            (Kind::DollarText, start.while_matches(|value| value != '$'), Mode::Dollar { opening })
        };
        self.state = LexerState { cursor: end, mode };
        start.lexeme(kind, end)
    }
}

struct Candidate<'sql> { accepted: Option<Cursor<'sql>>, failure: Cursor<'sql> }

#[derive(Clone, Copy)]
enum QuotedRule { Standard, Escaped, Identifier(Delimiter) }

fn quoted(mut cursor: Cursor<'_>, rule: QuotedRule) -> Candidate<'_> {
    let quote = match rule { QuotedRule::Standard | QuotedRule::Escaped => '\'', QuotedRule::Identifier(Delimiter::DoubleQuote) => '"', QuotedRule::Identifier(Delimiter::Backtick) => '`' };
    cursor.bump();
    let mut accepted = None;
    while let Some(value) = cursor.peek() {
        if value == quote {
            cursor.bump();
            accepted = Some(cursor);
            if cursor.peek() != Some(quote) { break; }
            cursor.bump();
        } else if matches!((rule, value), (QuotedRule::Standard, '\n') | (QuotedRule::Identifier(_), '/')) {
            break;
        } else if matches!(rule, QuotedRule::Escaped) && value == '\\' {
            cursor.bump();
            let Some(escape) = cursor.peek() else { break; };
            if matches!(escape, '0'..='7') {
                for _ in 0..3 { if cursor.peek().is_some_and(|value| matches!(value, '0'..='7')) { cursor.bump(); } else { break; } }
            } else if matches!(escape.to_ascii_lowercase(), 'x' | 'u') {
                cursor.bump();
                let width = if escape.eq_ignore_ascii_case(&'x') { 2 } else { 4 };
                for _ in 0..width {
                    if !cursor.peek().is_some_and(|value| value.is_ascii_hexdigit()) { return Candidate { accepted, failure: cursor }; }
                    cursor.bump();
                }
            } else if matches!(escape.to_ascii_lowercase(), 'b' | 'f' | 'n' | 'r' | 't' | '\\' | '\'' | '"') {
                cursor.bump();
            } else { break; }
        } else { cursor.bump(); }
    }
    Candidate { accepted, failure: cursor }
}

fn binary(start: Cursor<'_>) -> Option<Cursor<'_>> {
    let cursor = start.advance(3).while_matches(|value| value.is_ascii_hexdigit());
    cursor.remaining().starts_with("')").then(|| cursor.advance(2))
}

fn number(start: Cursor<'_>) -> (Kind, Cursor<'_>) {
    let mut end = start.while_matches(|value| value.is_ascii_digit());
    let mut kind = Kind::Integer;
    if end.peek() == Some('.') && end.advance(1).peek().is_some_and(|value| value.is_ascii_digit()) {
        end = end.advance(1).while_matches(|value| value.is_ascii_digit());
        kind = Kind::Float;
    }
    if end.peek().is_some_and(|value| value.eq_ignore_ascii_case(&'e')) {
        let mut exponent = end.advance(1);
        if exponent.peek().is_some_and(|value| matches!(value, '+' | '-')) { exponent.bump(); }
        if exponent.peek().is_some_and(|value| value.is_ascii_digit()) {
            end = exponent.while_matches(|value| value.is_ascii_digit());
            kind = Kind::Float;
        }
    }
    (kind, end)
}

fn simple_trivia_end(start: Cursor<'_>, first: char) -> Option<Cursor<'_>> {
    if whitespace(first) { return Some(start.while_matches(whitespace)); }
    if start.remaining().starts_with("--") {
        return Some(start.while_matches(|value| !matches!(value, '\r' | '\n')));
    }
    None
}

fn word_or_literal(start: Cursor<'_>, first: char) -> (Kind, Cursor<'_>) {
    let identifier = start.while_matches(identifier_part);
    let raw = &start.source.0[start.position.byte..identifier.position.byte];
    let mut accepted = (vocabulary::keyword(raw).map_or(Kind::Identifier, Kind::Keyword), identifier);
    if first.eq_ignore_ascii_case(&'e') && start.advance(1).peek() == Some('\'') {
        if let Some(end) = quoted(start.advance(1), QuotedRule::Escaped).accepted { accepted = (Kind::EscapedString, end); }
    } else if first.eq_ignore_ascii_case(&'x') && start.advance(1).remaining().starts_with("('") {
        if let Some(end) = binary(start) { accepted = (Kind::BinaryString, end); }
    }
    accepted
}

fn whitespace(value: char) -> bool { matches!(value, ' ' | '\n' | '\r' | '\t') }
fn identifier_start(value: char) -> bool { value.is_ascii_alphabetic() || value == '_' }
fn tag_part(value: char) -> bool { identifier_start(value) || value.is_ascii_digit() }
fn identifier_part(value: char) -> bool { tag_part(value) || value == '$' }
fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.sql/allocation-size", "SQL lexer allocation exceeds addressable memory").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded) }
