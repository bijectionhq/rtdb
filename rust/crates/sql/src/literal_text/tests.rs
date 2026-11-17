use super::*;
use crate::lexer::Lexer;
use rtdb_types::MemoryBudget;

fn tokens<'sql>(source: &'sql str, allocation: &AllocationScope) -> Vec<Lexeme<'sql>> {
    let mut lexer = Lexer::new(source, allocation);
    let mut tokens = Vec::new();
    loop {
        let token = lexer.next().unwrap();
        if token.kind() == Kind::Eof { return tokens; }
        tokens.push(token);
    }
}

fn assert_c(source: &str, expected: &str) { assert_c_units(JavaText::from_utf8(source), &expected.encode_utf16().collect::<Vec<_>>()); }

fn assert_c_units(source: JavaText<'_>, expected: &[u16]) {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let output = decode_c_body(source, &allocation).unwrap();
    assert_eq!(output.view().units().collect::<Vec<_>>(), expected);
    assert!(output.view().len_units() <= source.len_units());
    drop(allocation);
    assert_eq!(memory.used_bytes(), output.retained_bytes());
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

fn java_failure(source: JavaText<'_>) -> LiteralJavaFailure {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    match decode_c_body(source, &allocation).unwrap_err() {
        LiteralDecodeFailure::Java(error) => error,
        other => panic!("{other:?}"),
    }
}

#[test]
fn pinned_string_util_escape_cases_retain_their_exact_results() {
    for (source, expected) in [
        (r"\n", "\n"), (r"\r", "\r"), (r"\t", "\t"), (r"\40", " "), (r"\x20", " "),
        (r"\u0020", " "), (r"\U0001F601", "😁"), (r"\\", "\\"), (r"\'", "'"),
    ] { assert_c(source, expected); }
}

#[test]
fn utility_fallback_escapes_and_unmatched_text_keep_literal_transform_output() {
    for (source, expected) in [
        ("", ""), ("abc😀\0", "abc😀\0"), (r"\b\f\n\r\s\t", "\u{8}\u{c}\n\r \t"),
        (r#"\'\"\\"#, "'\"\\"), (r"\u0024\u005c", "$\\"), ("a\nb", "a\nb"),
    ] { assert_c(source, expected); }
}

#[test]
fn octal_matches_keep_a_third_digit_when_translate_escapes_consumes_only_two() {
    for (source, expected) in [
        (r"\0", "\0"), (r"\00", "\0"), (r"\0000", "\x000"), (r"\7", "\u{7}"),
        (r"\77", "?"), (r"\777", "?7"), (r"\400", " 0"), (r"\377", "ÿ"),
        (r"\378", "\u{1f}8"), (r"\39", "\u{3}9"), (r"\1234", "S4"), (r"\477", "'7"),
    ] { assert_c(source, expected); }
}

#[test]
fn utility_hexadecimal_widths_are_greedy_upper_bounds() {
    for (source, expected) in [
        (r"\x1", "\u{1}"), (r"\x12", "\u{12}"), (r"\x123", "\u{12}3"), (r"\xFf", "ÿ"),
        (r"\u1", "\u{1}"), (r"\u123", "\u{123}"), (r"\u12345", "\u{1234}5"),
        (r"\U1", "\u{1}"), (r"\U0001F6010", "😁0"), (r"\U000000001", "\x001"),
    ] { assert_c(source, expected); }
}

#[test]
fn unicode_conversion_preserves_bmp_units_and_builds_supplementary_pairs() {
    for (source, expected) in [
        (r"\uD800", &[0xd800][..]), (r"\uDFFF", &[0xdfff]),
        (r"\uD83D\uDE00", &[0xd83d, 0xde00]), (r"\uDE00\uD83D", &[0xde00, 0xd83d]),
        (r"\uFFFF", &[0xffff]), (r"\U00010000", &[0xd800, 0xdc00]), (r"\U0010FFFF", &[0xdbff, 0xdfff]),
    ] { assert_c_units(JavaText::from_utf8(source), expected); }
}

#[test]
fn raw_units_and_split_utf8_views_survive_unmatched_and_matched_bodies() {
    let raw = [0xd800, 0xdc00, 0xd800, 0x61, 0xdc00, 0, 0xffff];
    assert_c_units(JavaText::from_utf16(&raw), &raw);
    let matched = [0xd800, BACKSLASH, 0x6e, 0xdc00, 0xd83d, 0xde00];
    assert_c_units(JavaText::from_utf16(&matched), &[0xd800, 10, 0xdc00, 0xd83d, 0xde00]);
    let pair = JavaText::from_utf8("😀");
    assert_c_units(pair.slice_units(0..1).unwrap(), &[0xd83d]);
    assert_c_units(pair.slice_units(1..2).unwrap(), &[0xde00]);
}

#[test]
fn unmatched_trailing_slash_and_missing_hexadecimal_digits_stay_literal() {
    for source in [r"\", r"\x", r"\u", r"\U", r"\xG", r"\uZ", r"\U_"] { assert_c(source, source); }
    assert_c(r"\u\x41", r"\uA");
    assert_c(r"\x\n", "\\x\n");
}

#[test]
fn emitted_backslashes_are_not_scanned_again() {
    assert_c(r"\u005cn", r"\n");
    assert_c(r"\x5c777", r"\777");
    assert_c(r"\\uD800", r"\uD800");
    assert_c(r"\u005c\u006e", r"\n");
}

#[test]
fn line_continuations_translate_only_the_matched_fragment() {
    for (source, expected) in [("a\\\nb", "ab"), ("a\\\rb", "ab"), ("a\\\r\nb", "a\nb"), ("\\\r\n", "\n")] {
        assert_c(source, expected);
    }
}

#[test]
fn fallback_failures_retain_the_actual_first_unit() {
    for unit in [0, 0x38, 0x39, 0x71, 0x42, 0x46, 0x4e, 0x52, 0x54, 0x58, 0xd800, 0xdc00] {
        assert_eq!(java_failure(JavaText::from_utf16(&[BACKSLASH, unit])), LiteralJavaFailure::InvalidEscape(unit));
    }
    assert_eq!(java_failure(JavaText::from_utf8("\\😀")), LiteralJavaFailure::InvalidEscape(0xd83d));
}

#[test]
fn invalid_code_points_retain_the_signed_uint_cast() {
    for (source, value) in [(r"\U00110000", 0x110000), (r"\U7FFFFFFF", i32::MAX), (r"\U80000000", i32::MIN), (r"\UFFFFFFFF", -1)] {
        assert_eq!(java_failure(JavaText::from_utf8(source)), LiteralJavaFailure::InvalidCodePoint(value));
    }
}

#[test]
fn scanner_match_boundaries_use_units_and_combine_fallback_pairs() {
    let mut scanner = CScanner::new(JavaText::from_utf8("😀\\xG\\😀\\n"));
    for unit in [0xd83d, 0xde00, BACKSLASH, 0x78, 0x47] {
        assert!(matches!(scanner.next_fragment(), Some(Fragment::Unit(found)) if found == unit));
    }
    let Some(Fragment::Escape(escape)) = scanner.next_fragment() else { panic!() };
    assert_eq!(escape.start, 5);
    assert_eq!(&escape.units[..escape.length], &[BACKSLASH, 0xd83d, 0xde00]);
    assert_eq!(scanner.offset, 8);
    let Some(Fragment::Escape(escape)) = scanner.next_fragment() else { panic!() };
    assert_eq!(escape.start, 8);
    assert_eq!(&escape.units[..escape.length], &[BACKSLASH, 0x6e]);
    assert!(scanner.next_fragment().is_none());
}

#[test]
fn the_first_transform_failure_precedes_later_escape_conversion() {
    for (source, expected) in [
        (r"ok\q\UFFFFFFFF", LiteralJavaFailure::InvalidEscape(u16::from(b'q'))),
        (r"ok\UFFFFFFFF\q", LiteralJavaFailure::InvalidCodePoint(-1)),
    ] {
        let memory = MemoryBudget::new(1 << 20);
        let allocation = AllocationScope::new(&memory).unwrap();
        let error = decode_c_body(JavaText::from_utf8(source), &allocation).unwrap_err();
        assert!(matches!(error, LiteralDecodeFailure::Java(found) if found == expected));
        assert_eq!(memory.used_bytes(), 4096 + source.encode_utf16().count() * 2 + 256);
        assert_eq!(memory.peak_bytes(), memory.used_bytes());
        drop(allocation);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn java_messages_preserve_raw_units_hexadecimal_case_and_exception_class() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    for unit in [0, 0x71, 0xd800, 0xdc00, 0xffff] {
        let failure = LiteralJavaFailure::InvalidEscape(unit);
        assert_eq!(failure.exception_class(), "java.lang.IllegalArgumentException");
        let mut expected = "Invalid escape sequence: \\".encode_utf16().collect::<Vec<_>>();
        expected.push(unit);
        expected.extend(format!(" \\\\u{unit:04X}").encode_utf16());
        assert_eq!(failure.message(&allocation).unwrap().view().units().collect::<Vec<_>>(), expected);
    }
    for (value, suffix) in [(0x110000, "110000"), (i32::MAX, "7FFFFFFF"), (i32::MIN, "80000000"), (-1, "FFFFFFFF")] {
        let failure = LiteralJavaFailure::InvalidCodePoint(value);
        assert_eq!(failure.exception_class(), "java.lang.IllegalArgumentException");
        assert_eq!(failure.message(&allocation).unwrap().view().units().collect::<Vec<_>>(), format!("Not a valid Unicode code point: 0x{suffix}").encode_utf16().collect::<Vec<_>>());
    }
}

#[test]
fn standard_and_c_literal_views_keep_distinct_doubled_quote_policies() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    for (source, expected) in [("''", ""), ("'a''b'", "a'b"), ("''''", "'"), ("E'a''b'", "a''b"), (r"E'\uD83D\uDE00'", "😀"), ("'😀''é'", "😀'é")] {
        let found = tokens(source, &allocation);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].raw(), source);
        let output = Literal::single(found[0]).unwrap().decode(&allocation).unwrap();
        assert_eq!(output.view().units().collect::<Vec<_>>(), expected.encode_utf16().collect::<Vec<_>>());
    }
    for token in tokens("1 name \"identifier\" $1", &allocation) { assert!(Literal::single(token).is_none()); }
}

#[test]
fn lexically_admitted_uppercase_escapes_fail_in_the_case_sensitive_converter() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    for escape in ['B', 'F', 'N', 'R', 'T', 'X'] {
        let source = if escape == 'X' { "E'\\X20'".to_owned() } else { format!("E'\\{escape}'") };
        let found = tokens(&source, &allocation);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind(), Kind::EscapedString);
        assert_eq!(found[0].raw(), source);
        assert!(matches!(Literal::single(found[0]).unwrap().decode(&allocation), Err(LiteralDecodeFailure::Java(LiteralJavaFailure::InvalidEscape(unit))) if unit == escape as u16));
    }
    for source in [r"E'\s'", r"E'\x1'", "E'\\\r\n'"] {
        assert_ne!(Lexer::new(source, &allocation).next().unwrap().kind(), Kind::EscapedString);
    }
    for source in [r"E'\U0001F601'", r"e'\U0001F601'"] {
        let found = tokens(source, &allocation);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind(), Kind::EscapedString);
        assert_eq!(Literal::single(found[0]).unwrap().decode(&allocation).unwrap().view().units().collect::<Vec<_>>(), [0xd83d, 0xde01]);
    }
}

#[test]
fn dollar_views_join_all_text_tokens_without_decoding_or_separators() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    for (source, expected) in [("$$$$", ""), ("$a$$a$", ""), (r"$$a\n'b$$", r"a\n'b"), ("$tag$a$X$b😀$tag$", "a$X$b😀")] {
        let found = tokens(source, &allocation);
        let output = Literal::dollar(&found).unwrap().decode(&allocation).unwrap();
        assert_eq!(output.view().units().collect::<Vec<_>>(), expected.encode_utf16().collect::<Vec<_>>());
    }
    let found = tokens("$tag$a$X$b$tag$", &allocation);
    assert!(found.len() > 3);
    assert!(Literal::dollar(&[]).is_none());
    assert!(Literal::dollar(&found[..1]).is_none());
    assert!(Literal::dollar(&found[..found.len() - 1]).is_none());
    let different = tokens("$other$x$other$", &allocation);
    assert!(Literal::dollar(&[found[0], *different.last().unwrap()]).is_none());
    let number = tokens("1", &allocation);
    assert!(Literal::dollar(&[found[0], number[0], *found.last().unwrap()]).is_none());
    assert!(Literal::dollar(&[number[0], *found.last().unwrap()]).is_none());
}

#[test]
fn exact_scratch_and_retained_copy_budget_admits_without_unaccounted_growth() {
    let source = JavaText::from_utf8(r"\uD800x");
    let retained = JavaString::allocation_bound_utf16(&[0xd800, 0x78]).unwrap();
    let scratch = scratch_bytes(source.len_units()).unwrap() + 256;
    let bound = 4096 + scratch + retained;
    let memory = MemoryBudget::new(bound);
    let allocation = AllocationScope::new(&memory).unwrap();
    let output = decode_c_body(source, &allocation).unwrap();
    assert_eq!(memory.used_bytes(), bound);
    assert_eq!(memory.peak_bytes(), bound);
    drop(allocation);
    assert_eq!(memory.used_bytes(), retained);
    assert_eq!(output.view().units().collect::<Vec<_>>(), [0xd800, 0x78]);
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
    let memory = MemoryBudget::new(bound - 1);
    let allocation = AllocationScope::new(&memory).unwrap();
    assert!(matches!(decode_c_body(source, &allocation), Err(LiteralDecodeFailure::Resource(error)) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(memory.used_bytes(), 4096 + scratch);
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn unchanged_utf8_copy_needs_no_scratch_but_empty_output_still_owns_storage() {
    for source in ["", "plain😀", r"\x"] {
        let bound = JavaString::allocation_bound_utf8(source).unwrap();
        let memory = MemoryBudget::new(4096 + bound);
        let allocation = AllocationScope::new(&memory).unwrap();
        let output = decode_c_body(JavaText::from_utf8(source), &allocation).unwrap();
        assert_eq!(output.view().utf8_source(), Some(source));
        assert_eq!(memory.used_bytes(), 4096 + bound);
        assert_eq!(memory.peak_bytes(), 4096 + bound);
        drop(allocation);
        assert_eq!(memory.used_bytes(), bound);
        drop(output);
        assert_eq!(memory.used_bytes(), 0);
        let memory = MemoryBudget::new(4096 + bound - 1);
        let allocation = AllocationScope::new(&memory).unwrap();
        assert!(matches!(decode_c_body(JavaText::from_utf8(source), &allocation), Err(LiteralDecodeFailure::Resource(_))));
        assert_eq!(memory.used_bytes(), 4096);
    }
}

#[test]
fn scratch_admission_precedes_the_first_invalid_escape_transform() {
    let memory = MemoryBudget::new(4096 + 2 * 2 + 256 - 1);
    let allocation = AllocationScope::new(&memory).unwrap();
    assert!(matches!(decode_c_body(JavaText::from_utf8(r"\q"), &allocation), Err(LiteralDecodeFailure::Resource(error)) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(memory.used_bytes(), 4096);
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn retained_units_and_clones_outlive_the_source_scope_and_child_budget_handle() {
    let memory = MemoryBudget::new(1 << 20);
    let output = {
        let child = memory.scope();
        let allocation = AllocationScope::new(&child).unwrap();
        let source = String::from(r"\uD800x");
        decode_c_body(JavaText::from_utf8(&source), &allocation).unwrap()
    };
    let retained = output.retained_bytes();
    assert_eq!(memory.used_bytes(), retained);
    let cloned = output.clone();
    assert_eq!(memory.used_bytes(), retained);
    drop(output);
    assert_eq!(cloned.view().units().collect::<Vec<_>>(), [0xd800, 0x78]);
    assert_eq!(memory.used_bytes(), retained);
    drop(cloned);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn failed_admission_preserves_a_live_sibling_and_raw_failure_message_owns_its_lease() {
    let sibling_units = [0xd800];
    let sibling_bound = JavaString::allocation_bound_utf16(&sibling_units).unwrap();
    let memory = MemoryBudget::new(4096 + sibling_bound);
    let sibling = JavaString::copy_utf16(&sibling_units, &memory).unwrap();
    let allocation = AllocationScope::new(&memory).unwrap();
    assert!(matches!(decode_c_body(JavaText::from_utf8(r"\q"), &allocation), Err(LiteralDecodeFailure::Resource(_))));
    assert_eq!(memory.used_bytes(), 4096 + sibling_bound);
    drop(allocation);
    assert_eq!(memory.used_bytes(), sibling_bound);
    assert_eq!(sibling.view().units().next(), Some(0xd800));
    drop(sibling);
    assert_eq!(memory.used_bytes(), 0);
    let memory = MemoryBudget::new(1 << 20);
    let message = {
        let allocation = AllocationScope::new(&memory).unwrap();
        LiteralJavaFailure::InvalidEscape(0xd800).message(&allocation).unwrap()
    };
    assert_eq!(memory.used_bytes(), message.retained_bytes());
    assert!(message.view().units().any(|unit| unit == 0xd800));
    drop(message);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn scratch_limits_are_checked_and_never_extend_the_logical_capacity() {
    assert_eq!(scratch_bytes(0).unwrap(), 0);
    assert_eq!(scratch_bytes(3).unwrap(), 6);
    assert!(scratch_bytes(i32::MAX as usize + 1).is_err());
    assert!(scratch_bytes(usize::MAX).is_err());
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut output = DecodedUnits::new(1, &allocation).unwrap();
    let capacity = output.units.capacity();
    output.push(0xd800).unwrap();
    assert!(matches!(output.push(0xdc00), Err(LiteralDecodeFailure::Resource(error)) if error.code == "rtdb.sql/literal-text-size"));
    assert_eq!(output.units, [0xd800]);
    assert_eq!(output.units.capacity(), capacity);
    assert_eq!(memory.used_bytes(), 4096 + 2 + 256);
}

#[test]
fn standard_and_multi_token_dollar_copies_admit_their_full_scratch_overlap() {
    for (source, source_units, expected) in [("'a''😀'", 5, "a'😀"), ("$t$a$x$b$t$", 5, "a$x$b")] {
        let output_units = expected.encode_utf16().collect::<Vec<_>>();
        let retained = JavaString::allocation_bound_utf16(&output_units).unwrap();
        let scratch = scratch_bytes(source_units).unwrap() + 256;
        let memory = MemoryBudget::new(4096 + scratch + retained);
        let allocation = AllocationScope::new(&memory).unwrap();
        let found = tokens(source, &allocation);
        let literal = if found.len() == 1 { Literal::single(found[0]).unwrap() } else { Literal::dollar(&found).unwrap() };
        let output = literal.decode(&allocation).unwrap();
        assert_eq!(output.view().units().collect::<Vec<_>>(), output_units);
        assert_eq!(memory.used_bytes(), 4096 + scratch + retained);
        drop(allocation);
        assert_eq!(memory.used_bytes(), retained);
        drop(output);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn java_message_copy_admits_before_allocation_and_keeps_raw_failure_reusable() {
    let failure = LiteralJavaFailure::InvalidEscape(0xd800);
    let mut expected = "Invalid escape sequence: \\".encode_utf16().collect::<Vec<_>>();
    expected.push(0xd800);
    expected.extend(" \\\\uD800".encode_utf16());
    let retained = JavaString::allocation_bound_utf16(&expected).unwrap();
    let memory = MemoryBudget::new(4096 + retained - 1);
    let allocation = AllocationScope::new(&memory).unwrap();
    assert_eq!(failure.message(&allocation).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), 4096);
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
    let memory = MemoryBudget::new(4096 + retained);
    let allocation = AllocationScope::new(&memory).unwrap();
    let message = failure.message(&allocation).unwrap();
    assert_eq!(message.view().units().collect::<Vec<_>>(), expected);
    assert_eq!(memory.used_bytes(), 4096 + retained);
    drop(allocation);
    assert_eq!(memory.used_bytes(), retained);
    drop(message);
    assert_eq!(memory.used_bytes(), 0);
}
