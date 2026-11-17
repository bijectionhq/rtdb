use super::*;
use crate::json::{BoundedSink, CountingSink, JsonDocument};
use rtdb_types::MemoryBudget;

fn encode(text: JavaText<'_>, allocation: &AllocationScope) -> JsonDocument {
    let mut count = CountingSink::default();
    string(text, &mut count).unwrap();
    let mut output = BoundedSink::new(count.length, allocation).unwrap();
    string(text, &mut output).unwrap();
    output.finish().unwrap()
}

fn decoded(source: &str, expected: &[u16]) {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let (token, end) = JsonStringToken::parse(source, 0).unwrap();
    assert_eq!(end, source.len());
    let result = token.decode(&allocation).unwrap();
    assert_eq!(result.view().units().collect::<Vec<_>>(), expected);
    drop(allocation);
    assert_eq!(memory.used_bytes(), result.retained_bytes());
    drop(result);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn valid_utf8_string_spelling_matches_the_existing_json_serializer() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let controls = (0..32).map(|unit| char::from_u32(unit).unwrap()).collect::<String>();
    for text in ["", "a\"b\\c/d", "\u{8}\t\n\u{c}\r", "é漢😀", "\u{7f}\u{85}\u{2028}", &controls] {
        let output = encode(JavaText::from_utf8(text), &allocation);
        assert_eq!(output.as_str(), serde_json::to_string(text).unwrap());
    }
    assert_eq!(encode(JavaText::from_utf8("\0\u{1}\u{1f}"), &allocation).as_str(), r#""\u0000\u0001\u001f""#);
}

#[test]
fn isolated_units_use_json_escapes_while_pairs_keep_utf8_spelling() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    for (units, expected) in [
        (&[0xd800][..], r#""\ud800""#), (&[0xdfff], r#""\udfff""#),
        (&[0xd83d, 0xde00], "\"😀\""), (&[0xde00, 0xd83d], r#""\ude00\ud83d""#),
        (&[0xd800, 0x61, 0xdc00, 0], r#""\ud800a\udc00\u0000""#),
    ] { assert_eq!(encode(JavaText::from_utf16(units), &allocation).as_str(), expected); }
    let pair = JavaText::from_utf8("😀");
    assert_eq!(encode(pair.slice_units(0..1).unwrap(), &allocation).as_str(), r#""\ud83d""#);
    assert_eq!(encode(pair.slice_units(1..2).unwrap(), &allocation).as_str(), r#""\ude00""#);
}

#[test]
fn string_token_borrows_its_source_and_stops_at_the_closing_quote() {
    let source = "xx\"a😀\" trailing";
    let (token, end) = JsonStringToken::parse(source, 2).unwrap();
    assert_eq!(token.body, "a😀");
    assert_eq!(token.body.as_ptr(), source[3..].as_ptr());
    assert_eq!(token.units, 3);
    assert!(!token.escaped);
    assert_eq!(&source[end..], " trailing");
}

#[test]
fn escaped_hex_units_preserve_isolated_reversed_and_paired_surrogates() {
    decoded(r#""\ud800\udc00\ud800X\udfff\u0000""#, &[0xd800, 0xdc00, 0xd800, 0x58, 0xdfff, 0]);
    decoded(r#""\uDE00\uD83d\uFFFF\uffff""#, &[0xde00, 0xd83d, 0xffff, 0xffff]);
    decoded("\"😀\\ud800é\"", &[0xd83d, 0xde00, 0xd800, 0xe9]);
}

#[test]
fn json_short_escapes_decode_once_without_sql_escape_rules() {
    decoded(r#""\"\\\/\b\f\n\r\t""#, &[34, 92, 47, 8, 12, 10, 13, 9]);
    decoded(r#""\u005cn\u0024""#, &[92, 0x6e, 0x24]);
    decoded(r#""\\uD800""#, &[92, 0x75, 0x44, 0x38, 0x30, 0x30]);
    decoded(r#""\u00001""#, &[0, 0x31]);
}

#[test]
fn malformed_json_strings_report_the_first_actual_byte_position() {
    for (source, start, byte_offset, issue) in [
        ("", 0, 0, JsonStringIssue::ExpectedQuote), ("x", 0, 0, JsonStringIssue::ExpectedQuote),
        ("😀", 1, 1, JsonStringIssue::ExpectedQuote), ("\"a\n\"", 0, 2, JsonStringIssue::RawControl),
        (r#""\x""#, 0, 2, JsonStringIssue::InvalidEscape), (r#""\s""#, 0, 2, JsonStringIssue::InvalidEscape),
        (r#""\u0z00""#, 0, 4, JsonStringIssue::InvalidHex), (r#""\u12""#, 0, 5, JsonStringIssue::InvalidHex),
        ("\"\\", 0, 2, JsonStringIssue::InvalidEscape), ("\"abc", 0, 4, JsonStringIssue::MissingQuote),
        ("xx\"é\\q\"", 2, 6, JsonStringIssue::InvalidEscape),
    ] {
        assert!(matches!(JsonStringToken::parse(source, start), Err(JsonReadFailure::Syntax { byte_offset: found, issue: found_issue }) if found == byte_offset && found_issue == issue));
    }
    assert!(matches!(JsonStringToken::parse(r#""\x\uD800""#, 0), Err(JsonReadFailure::Syntax { byte_offset: 2, issue: JsonStringIssue::InvalidEscape })));
}

#[test]
fn unchanged_token_copy_has_no_scratch_and_retains_empty_storage() {
    for source in [r#""""#, "\"plain😀\""] {
        let (token, _) = JsonStringToken::parse(source, 0).unwrap();
        let retained = JavaString::allocation_bound_utf8(token.body).unwrap();
        let memory = MemoryBudget::new(4096 + retained);
        let allocation = AllocationScope::new(&memory).unwrap();
        let output = token.decode(&allocation).unwrap();
        assert_eq!(memory.used_bytes(), 4096 + retained);
        assert_eq!(output.view().utf8_source(), Some(token.body));
        drop(allocation);
        assert_eq!(memory.used_bytes(), retained);
        drop(output);
        assert_eq!(memory.used_bytes(), 0);
        let memory = MemoryBudget::new(4096 + retained - 1);
        let allocation = AllocationScope::new(&memory).unwrap();
        assert!(matches!(token.decode(&allocation), Err(JsonReadFailure::Admission(error)) if error.code == "rtdb/memory-budget-exceeded"));
        assert_eq!(memory.used_bytes(), 4096);
    }
}

#[test]
fn escaped_token_scratch_and_retained_copy_overlap_at_the_exact_bound() {
    let (token, _) = JsonStringToken::parse(r#""\ud800a\u0000""#, 0).unwrap();
    let retained = JavaString::allocation_bound_utf16(&[0xd800, 0x61, 0]).unwrap();
    let scratch = token.units * 2 + 256;
    let bound = 4096 + scratch + retained;
    let memory = MemoryBudget::new(bound);
    let allocation = AllocationScope::new(&memory).unwrap();
    let output = token.decode(&allocation).unwrap();
    assert_eq!(memory.used_bytes(), bound);
    assert_eq!(memory.peak_bytes(), bound);
    drop(allocation);
    assert_eq!(memory.used_bytes(), retained);
    assert_eq!(output.view().units().collect::<Vec<_>>(), [0xd800, 0x61, 0]);
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
    let memory = MemoryBudget::new(bound - 1);
    let allocation = AllocationScope::new(&memory).unwrap();
    assert!(matches!(token.decode(&allocation), Err(JsonReadFailure::Admission(error)) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(memory.used_bytes(), 4096 + scratch);
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn scratch_failure_preserves_a_sibling_and_unit_limits_precede_allocation() {
    let memory = MemoryBudget::new(1 << 20);
    let sibling = JavaString::copy_utf16(&[0xd800], &memory).unwrap();
    let allocation = AllocationScope::new(&memory).unwrap();
    let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
    let (token, _) = JsonStringToken::parse(r#""\n""#, 0).unwrap();
    assert!(matches!(token.decode(&allocation), Err(JsonReadFailure::Admission(_))));
    assert_eq!(sibling.view().units().next(), Some(0xd800));
    drop(blocker);
    assert!(matches!(DecodedText::new(i32::MAX as usize + 1, &allocation), Err(JsonReadFailure::Admission(error)) if error.code == "rtdb.plan/json-text-size"));
    let mut output = DecodedText::new(1, &allocation).unwrap();
    output.push(0xd800).unwrap();
    assert!(output.push(0xdc00).is_err());
    assert_eq!(output.units, [0xd800]);
    drop(output);
    drop(allocation);
    assert_eq!(memory.used_bytes(), sibling.retained_bytes());
    drop(sibling);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn json_document_clones_outlive_creator_source_scope_and_child_budget() {
    let memory = MemoryBudget::new(1 << 20);
    let output = {
        let child = memory.scope();
        let allocation = AllocationScope::new(&child).unwrap();
        let units = [0xd800, 0, 0xd83d, 0xde00];
        encode(JavaText::from_utf16(&units), &allocation)
    };
    let retained = output.retained_bytes();
    assert_eq!(memory.used_bytes(), retained);
    let cloned = output.clone();
    drop(output);
    assert_eq!(cloned.as_str(), "\"\\ud800\\u0000😀\"");
    assert_eq!(memory.used_bytes(), retained);
    drop(cloned);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn byte_sink_limits_and_utf8_validation_precede_document_publication() {
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut output = BoundedSink::new(1, &allocation).unwrap();
    assert!(matches!(output.append(b"ab"), Err(JsonWriteFailure::Internal)));
    assert!(output.bytes.is_empty());
    assert!(matches!(output.finish(), Err(JsonWriteFailure::Internal)));
    let mut output = BoundedSink::new(1, &allocation).unwrap();
    output.append(&[0xff]).unwrap();
    assert!(matches!(output.finish(), Err(JsonWriteFailure::Internal)));
    let mut count = CountingSink { length: usize::MAX - 1 };
    assert!(matches!(count.append(b"xx"), Err(JsonWriteFailure::Capacity)));
    assert_eq!(count.length, usize::MAX - 1);
    assert_eq!(memory.used_bytes(), 4096 + 2 * (1 + 256));
    assert!(matches!(JsonDocument::allocation_bound(usize::MAX), Err(JsonWriteFailure::Capacity)));
}
