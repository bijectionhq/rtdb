use std::collections::BTreeSet;
use rtdb_types::{AllocationScope, AnomalyCategory, MemoryBudget};
use super::{Delimiter, Kind, Lexer, Lexeme, Position, Punctuation as P, RenderedDiagnostic, ScanError, vocabulary};

fn tokens<'sql>(source: &'sql str, allocation: &AllocationScope) -> Vec<Lexeme<'sql>> {
    let mut lexer = Lexer::new(source, allocation);
    let mut output = Vec::new();
    loop {
        let token = lexer.next().unwrap();
        if token.kind == Kind::Eof { break; }
        output.push(token);
    }
    output
}

fn raw<'sql>(source: &'sql str, allocation: &AllocationScope) -> Vec<&'sql str> {
    tokens(source, allocation).into_iter().map(Lexeme::raw).collect()
}

#[test]
fn vocabulary_matches_every_pinned_keyword_alias_and_punctuation_rule() {
    let grammar = include_str!("../../../../../core/src/main/antlr/xtdb/antlr/SqlLexer.g4");
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut expected = BTreeSet::new();
    let mut selected = false;
    for line in grammar.lines() {
        if line.starts_with("COMMA :") { selected = true; }
        if line.starts_with("fragment DOLLAR_TAG_NAME") { break; }
        if !selected || line.trim_start().starts_with("//") { continue; }
        let Some((name, body)) = line.split_once(':') else { continue; };
        let name = name.trim();
        for spelling in body.split('\'').skip(1).step_by(2) {
            expected.insert((spelling, name));
            let found = tokens(spelling, &allocation);
            assert_eq!(found.len(), 1, "{spelling}");
            let actual = match found[0].kind {
                Kind::Keyword(value) => value.name(),
                Kind::Punctuation(value) => value.name(),
                other => panic!("{spelling}: {other:?}"),
            };
            assert_eq!(actual, name, "{spelling}");
            if spelling.starts_with(|value: char| value.is_ascii_alphabetic() || value == '_') {
                let lowercase = spelling.to_ascii_lowercase();
                assert_eq!(tokens(&lowercase, &allocation)[0].kind, found[0].kind);
                let suffixed = format!("{spelling}x");
                assert_eq!(tokens(&suffixed, &allocation)[0].kind, Kind::Identifier);
            }
        }
    }
    assert_eq!(vocabulary::spellings().collect::<BTreeSet<_>>(), expected);
    assert_eq!(tokens("PG_EXPANDARRAY", &allocation)[0].kind, Kind::Identifier);
}

#[test]
fn comments_whitespace_and_hash_minus_follow_pinned_boundaries() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    assert_eq!(raw("#-1 #--ignored\r2 --ignored\n3\t /*! SELECT nope */ 4", &allocation), ["#", "-", "1", "#", "2", "3", "4"]);
    for source in ["\u{000b}", "\u{000c}", "\u{00a0}", "é", "@", "^", "\\"] {
        assert!(matches!(Lexer::new(source, &allocation).next(), Err(ScanError::Recognition(_))), "{source:?}");
    }
    assert_eq!(raw("$1 _a1$a A$2 !~* ~* != <> #>> ->>", &allocation), ["$1", "_a1$a", "A$2", "!~*", "~*", "!=", "<>", "#>>", "->>"]);
}

#[test]
fn recursive_comments_retain_earlier_outer_acceptance_during_nested_continuations() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    assert_eq!(raw("/* outer /* inner */ tail */ after", &allocation), ["after"]);
    assert_eq!(raw("/* outer /* inner */ tail", &allocation), ["tail"]);
    assert!(raw("/* /* */", &allocation).is_empty());
    assert!(raw("/*/*/", &allocation).is_empty());
    assert_eq!(raw("/* a /* b */ c */ d */", &allocation), ["d", "*", "/"]);
    assert_eq!(raw("/* unclosed", &allocation), ["/", "*", "unclosed"]);
    assert_eq!(raw("/**/ /*/", &allocation), ["/", "*", "/"]);
}

#[test]
fn malformed_quoted_candidates_restore_their_last_accepted_endpoint() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut lexer = Lexer::new("'a''b", &allocation);
    assert_eq!(lexer.next().unwrap().raw(), "'a'");
    let Err(ScanError::Recognition(error)) = lexer.next() else { panic!() };
    assert_eq!(error.span.start.byte, 3);
    assert_eq!(error.span.end.byte, 5);
    assert_eq!(raw(r"E'\q' E'\s' E'a''\q'", &allocation), ["E", r"'\q'", "E", r"'\s'", "E'a'", r"'\q'"]);
    let mut lexer = Lexer::new("\"a\"\"b", &allocation);
    assert_eq!(lexer.next().unwrap().raw(), "\"a\"");
    assert!(matches!(lexer.next(), Err(ScanError::Recognition(_))));
}

#[test]
fn raw_literal_bodies_keep_query_and_control_decode_choices_open() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    for body in [r"\400", r"\777", r"\uD800", r"\U1234", r"\U00110000", r"\UFFFFFFFF", r"\N", r"\X41", r"\u00000041", "a''b", "a\nb"] {
        let source = format!("E'{body}'");
        let found = tokens(&source, &allocation);
        assert_eq!(found.len(), 1, "{source}");
        assert_eq!(found[0].kind, Kind::EscapedString);
        assert_eq!(found[0].body(), Some(body));
    }
    let found = tokens("'a''b' 'a\rb' \"a\"\"b\" `a``b`", &allocation);
    assert_eq!(found.iter().copied().map(Lexeme::body).collect::<Vec<_>>(), [Some("a''b"), Some("a\rb"), Some("a\"\"b"), Some("a``b")]);
    assert_eq!(found[2].kind, Kind::DelimitedIdentifier(Delimiter::DoubleQuote));
    assert_eq!(found[3].kind, Kind::DelimitedIdentifier(Delimiter::Backtick));
    assert!(matches!(Lexer::new("'a\nb'", &allocation).next(), Err(ScanError::Recognition(_))));
    for source in ["\"a/b\"", "`a/b`"] { assert!(matches!(Lexer::new(source, &allocation).next(), Err(ScanError::Recognition(_)))); }
}

#[test]
fn binary_literal_shape_is_contiguous_and_defers_odd_hex_validation() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    for (source, body) in [("X('')", ""), ("x('aF01')", "aF01"), ("X('0')", "0")] {
        let found = tokens(source, &allocation);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, Kind::BinaryString);
        assert_eq!(found[0].body(), Some(body));
    }
    assert_eq!(raw("X('GG') X ('00') X'00'", &allocation), ["X", "(", "'GG'", ")", "X", "(", "'00'", ")", "X", "'00'"]);
}

#[test]
fn numbers_keep_unsigned_tokens_and_fall_back_before_incomplete_exponents() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let found = tokens("1. .1 1e 1e+ 1e-2 1.0E+2 1_2 0x20 -9223372036854775808", &allocation);
    assert_eq!(found.iter().copied().map(Lexeme::raw).collect::<Vec<_>>(), ["1", ".", ".", "1", "1", "e", "1", "e", "+", "1e-2", "1.0E+2", "1", "_2", "0", "x20", "-", "9223372036854775808"]);
    assert_eq!(found[9].kind, Kind::Float);
    assert_eq!(found[10].kind, Kind::Float);
    assert_eq!(found[16].kind, Kind::Integer);
    assert_eq!(raw("1.2e+", &allocation), ["1.2", "e", "+"]);
}

#[test]
fn dollar_mode_keeps_text_tokens_and_matches_the_raw_tag_case() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let found = tokens("$A_1$a;$a_1$b$A_1$ $$x$$", &allocation);
    assert_eq!(found.iter().copied().map(Lexeme::raw).collect::<Vec<_>>(), ["$A_1$", "a;", "$", "a_1", "$", "b", "$A_1$", "$$", "x", "$$"]);
    assert_eq!(found[0].tag(), Some("A_1"));
    assert_eq!(found[6].kind, Kind::DollarEnd);
    assert_eq!(found[7].tag(), Some(""));
    let mut lexer = Lexer::new("$a$unterminated", &allocation);
    assert_eq!(lexer.next().unwrap().kind, Kind::DollarStart);
    assert_eq!(lexer.next().unwrap().kind, Kind::DollarText);
    assert_eq!(lexer.next().unwrap().kind, Kind::Eof);
    assert_eq!(lexer.next().unwrap().kind, Kind::Eof);
    let Err(ScanError::Recognition(error)) = Lexer::new("$tag!", &allocation).next() else { panic!() };
    assert_eq!(error.span.end.byte, 5);
}

#[test]
fn positions_distinguish_utf8_bytes_code_points_and_lf_only_line_changes() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut lexer = Lexer::new("--x\rE'a😀\r\nb'\t@", &allocation);
    let token = lexer.next().unwrap();
    assert_eq!(token.span.start, Position { byte: 4, code_point: 4, line: 1, column: 4 });
    assert_eq!(token.span.end, Position { byte: 15, code_point: 12, line: 2, column: 2 });
    let Err(ScanError::Recognition(error)) = lexer.next() else { panic!() };
    assert_eq!(error.span.start, Position { byte: 16, code_point: 13, line: 2, column: 3 });
    let diagnostic = error.render(&allocation).unwrap();
    let anomaly = diagnostic.anomaly();
    assert_eq!(anomaly.category, AnomalyCategory::Incorrect);
    assert_eq!(anomaly.code, "xtdb/sql-error");
    assert_eq!(anomaly.message.as_deref(), Some("Errors parsing SQL statement:\n  - line 2:3 token recognition error at: '@'"));
}

#[test]
fn lexer_errors_render_the_attempt_through_the_offending_character() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    for (source, displayed) in [("'a\nrest", "'a\\n"), ("\"a/b\"", "\"a/"), ("!a", "!a"), ("$tag!", "$tag!"), ("'a\t", "'a\\t")] {
        let Err(ScanError::Recognition(error)) = Lexer::new(source, &allocation).next() else { panic!("{source}") };
        let diagnostic = error.render(&allocation).unwrap();
        assert_eq!(diagnostic.anomaly().message, Some(format!("Errors parsing SQL statement:\n  - line 1:0 token recognition error at: '{displayed}'")));
    }
}

#[test]
fn scanner_does_not_request_a_later_error_or_comment_allocation_early() {
    let memory = MemoryBudget::new(4096);
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut lexer = Lexer::new("SELECT 1 @", &allocation);
    assert!(matches!(lexer.next().unwrap().kind, Kind::Keyword(_)));
    assert_eq!(lexer.next().unwrap().kind, Kind::Integer);
    assert_eq!(memory.used_bytes(), 4096);
    let Err(ScanError::Recognition(error)) = lexer.next() else { panic!() };
    assert_eq!(memory.used_bytes(), 4096);
    assert_eq!(error.render(&allocation).unwrap_err().code, "rtdb/memory-budget-exceeded");
    let mut lexer = Lexer::new("1 /* nested /* more */ */", &allocation);
    assert_eq!(lexer.next().unwrap().kind, Kind::Integer);
    let Err(ScanError::Admission(error)) = lexer.next() else { panic!() };
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn comment_frontiers_are_admitted_before_growth_and_release_with_the_scope() {
    let source = format!("{}{} SELECT", "/* ".repeat(64), " */".repeat(64));
    let memory = MemoryBudget::new(1 << 20);
    {
        let allocation = AllocationScope::new(&memory).unwrap();
        assert_eq!(raw(&source, &allocation), ["SELECT"]);
        assert!(memory.used_bytes() > 4096);
    }
    assert_eq!(memory.used_bytes(), 0);
    let limited = MemoryBudget::new(4096 + 256 + 8);
    {
        let allocation = AllocationScope::new(&limited).unwrap();
        assert!(matches!(Lexer::new("/* a */", &allocation).next(), Err(ScanError::Admission(_))));
    }
    assert_eq!(limited.used_bytes(), 0);
    let limited = MemoryBudget::new(8192);
    {
        let allocation = AllocationScope::new(&limited).unwrap();
        assert!(matches!(Lexer::new(&source, &allocation).next(), Err(ScanError::Admission(_))));
        assert!(limited.used_bytes() <= limited.limit_bytes());
    }
    assert_eq!(limited.used_bytes(), 0);
}

#[test]
fn recognition_error_rendering_reserves_its_complete_message_before_allocating() {
    const EXPECTED: &str = "Errors parsing SQL statement:\n  - line 1:0 token recognition error at: '@'";
    let required = 4096 + 256 + EXPECTED.len() + "xtdb/sql-error".len();
    for limit in [required - 1, required] {
        let memory = MemoryBudget::new(limit);
        {
            let allocation = AllocationScope::new(&memory).unwrap();
            let Err(ScanError::Recognition(error)) = Lexer::new("@", &allocation).next() else { panic!() };
            let result = error.render(&allocation);
            if limit == required { assert_eq!(result.unwrap().anomaly().message.as_deref(), Some(EXPECTED)); }
            else { assert_eq!(result.unwrap_err().code, "rtdb/memory-budget-exceeded"); }
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn rendered_diagnostic_owns_text_and_borrows_its_allocation_until_drop() {
    fn render(allocation: &AllocationScope) -> RenderedDiagnostic<'_> {
        let source = String::from("'unclosed");
        let Err(ScanError::Recognition(error)) = Lexer::new(&source, allocation).next() else { panic!() };
        error.render(allocation).unwrap()
    }
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let diagnostic = render(&allocation);
    let retained = memory.used_bytes();
    assert!(retained > 4096);
    assert_eq!(diagnostic.anomaly().message.as_deref(), Some("Errors parsing SQL statement:\n  - line 1:0 token recognition error at: ''unclosed'"));
    drop(diagnostic);
    assert_eq!(memory.used_bytes(), retained);
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn punctuation_does_not_absorb_a_following_comment_or_numeric_sign() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    assert_eq!(tokens("#- --rest", &allocation).into_iter().map(|token| token.kind).collect::<Vec<_>>(), [Kind::Punctuation(P::BitwiseXor), Kind::Punctuation(P::Minus)]);
}
