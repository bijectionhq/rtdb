use super::*;
use rtdb_types::{MemoryBudget, java_text::JavaString};
use crate::EvaluationScope;

fn raw(pattern: &[u16]) -> Pattern {
    compile_text(JavaText::from_utf16(pattern), JavaText::from_utf8(""), &EvalContext::default()).unwrap()
}

fn translation(pattern: JavaText<'_>) -> Translation {
    let context = EvalContext::default();
    let source = PatternInput::new(pattern, &context).unwrap();
    Translator { source: &source, position: 0, flags: Flags::default(), scopes: Vec::new(), output: String::new(), names: Vec::new(), literals: Vec::new(), supplementary: source.points.iter().copied().any(supplementary), predicate_bmp: true, syntax_prefix_verified: true, syntax: None }.translate(&context).unwrap()
}

#[test]
fn escaped_surrogate_slice_keeps_bmp_search_starts() {
    let source = r"a\uD800|(?!\A).\b{g}b";
    let translated = translation(JavaText::from_utf8(source));
    assert!(!translated.supplementary, "{}", translated.pattern);
    let pattern = compile(source, "", &EvalContext::default()).unwrap();
    assert!(pattern.search("😀b", &EvalContext::default()).unwrap(), "{:?} {:?}", pattern.regex, Expr::parse_java_tree(&translated.pattern));
}

#[test]
fn whole_matches_reuse_the_pattern_and_release_each_subject_charge() {
    let memory = MemoryBudget::new(1 << 28);
    let mut matcher = JavaFullMatcher::compile(JavaText::from_utf8(r"a|(?=a)ab|\uD800"), &memory).unwrap();
    let retained = memory.used_bytes();
    assert!(retained > 0);
    for (source, expected) in [
        (JavaText::from_utf8("ab"), true),
        (JavaText::from_utf8("ba"), false),
        (JavaText::from_utf8("ab\n"), false),
        (JavaText::from_utf16(&[0xd800]), true),
    ] {
        assert_eq!(matcher.matches(source).unwrap(), expected);
        assert_eq!(memory.used_bytes(), retained);
    }
    drop(matcher);
    assert_eq!(memory.used_bytes(), 0);
}

fn failure(pattern: &[u16], context: &EvalContext<'_>) -> failure::PatternFailure {
    match compile_text(JavaText::from_utf16(pattern), JavaText::from_utf8(""), context).err().unwrap() {
        CompileFailure::Pattern(failure) => failure,
        CompileFailure::Resource(error) => panic!("{error}"),
    }
}

#[test]
fn utf8_and_unit_views_have_identical_grammar_names_and_matches() {
    for (pattern, source, expected) in [
        (r"(?<word>a+)\k<word>", "aaaa", true),
        (r"\Q1.*\E", "1.*", true),
        ("(?x)a # ignored\nb", "ab", true),
        (r"(?<=😀)x", "😀x", true),
        (r"[a-z&&[^b]]", "b", false),
        (r"\p{Lu}", "A", true),
        (r"\uD83D\uDE00", "😀", true),
        (r"\b{g}|", "😀", true),
    ] {
        let units = pattern.encode_utf16().collect::<Vec<_>>();
        let from_utf8 = translation(JavaText::from_utf8(pattern));
        let from_units = translation(JavaText::from_utf16(&units));
        assert_eq!(from_utf8.pattern, from_units.pattern);
        assert_eq!(from_utf8.names, from_units.names);
        assert_eq!(from_utf8.supplementary, from_units.supplementary);
        let input = source.encode_utf16().collect::<Vec<_>>();
        assert_eq!(raw(&units).search_text(JavaText::from_utf16(&input), &EvalContext::default()).unwrap(), expected);
        assert_eq!(compile(pattern, "", &EvalContext::default()).unwrap().search(source, &EvalContext::default()).unwrap(), expected);
    }
}

#[test]
fn literal_escaped_quoted_and_ranged_surrogates_reach_the_unit_matcher() {
    for unit in [0xd800, 0xdc00, 0xdfff] {
        for pattern in [vec![unit], vec![0x5c, unit], vec![0x5c, 0x51, unit, 0x5c, 0x45], vec![0x5b, 0xd800, 0x2d, 0xdfff, 0x5d]] {
            let pattern = raw(&pattern);
            assert!(pattern.search_text(JavaText::from_utf16(&[unit]), &EvalContext::default()).unwrap());
            assert!(!pattern.search("?", &EvalContext::default()).unwrap());
            assert!(!pattern.search("�", &EvalContext::default()).unwrap());
        }
    }
    let pattern = raw(&[0x5c, 0x51, 0xd800, 0x2e, 0x2a, 0x5c, 0x45]);
    assert!(pattern.search_text(JavaText::from_utf16(&[0xd800, 0x2e, 0x2a]), &EvalContext::default()).unwrap());
    assert!(!pattern.search_text(JavaText::from_utf16(&[0xd800, 0x61]), &EvalContext::default()).unwrap());
}

#[test]
fn split_utf8_ranges_keep_their_unit_without_reencoding() {
    let pair = JavaText::from_utf8("😀");
    for range in [0..1, 1..2] {
        let half = pair.slice_units(range).unwrap();
        let pattern = compile_text(half, JavaText::from_utf8(""), &EvalContext::default()).unwrap();
        assert!(pattern.search_text(half, &EvalContext::default()).unwrap());
        assert!(!pattern.search("?", &EvalContext::default()).unwrap());
    }
    assert!(raw(&[0xd83d, 0xde00]).search("😀", &EvalContext::default()).unwrap());
}

#[test]
fn comments_consume_raw_units_until_the_selected_java_line_ending() {
    for ending in [10, 13, 0x85, 0x2028, 0x2029] {
        let mut pattern = "(?x)#".encode_utf16().collect::<Vec<_>>();
        pattern.extend([0xd800, 0x2a, ending, 0x61]);
        let source = if ending == 10 || ending == 13 { vec![0x61] } else { vec![ending, 0x61] };
        assert!(raw(&pattern).search_text(JavaText::from_utf16(&source), &EvalContext::default()).unwrap());
    }
    let mut pattern = "(?dx)#".encode_utf16().collect::<Vec<_>>();
    pattern.extend([0xd800, 13, 0x2a, 10, 0x61]);
    assert!(raw(&pattern).search("a", &EvalContext::default()).unwrap());
    let mut pattern = "(?x)#".encode_utf16().collect::<Vec<_>>();
    pattern.extend([0xd800, 0, 0x61]);
    assert!(raw(&pattern).search("\0a", &EvalContext::default()).unwrap());
}

#[test]
fn raw_units_select_supplementary_lookbehind_from_the_remaining_source() {
    let mut pattern = "(?<=a)b|".encode_utf16().collect::<Vec<_>>();
    pattern.push(0xd800);
    let translated = translation(JavaText::from_utf16(&pattern));
    assert!(translated.supplementary);
    assert!(translated.pattern.starts_with("(?j<="));
    assert!(translation(JavaText::from_utf8("(?<=a)b")).pattern.starts_with("(?<="));
    let pattern = [0x28, 0x3f, 0x3c, 0x3d, 0xd800, 0x29, 0x61];
    assert!(raw(&pattern).search_text(JavaText::from_utf16(&[0xd800, 0x61]), &EvalContext::default()).unwrap());
}

#[test]
fn quote_preprocessing_preserves_points_and_its_checked_capacity() {
    for (input, expected) in [
        (vec![0x5c, 0x51, 0x31, 0x2e, 0x5c, 0x45], vec![0x5c, 0x78, 0x33, 0x31, 0x5c, 0x2e]),
        (vec![0x5c, 0x51, 0xd800, 0x2a], vec![0xd800, 0x5c, 0x2a]),
        (vec![0x5c, 0x5c, 0x51, 0xd800], vec![0x5c, 0x5c, 0x51, 0xd800]),
        (vec![0x5c, 0x51, 0x5c, 0x45, 0x5c, 0x51, 0x32, 0x5c, 0x45], vec![0x5c, 0x78, 0x33, 0x32]),
    ] {
        let source = PatternInput::new(JavaText::from_utf16(&input), &EvalContext::default()).unwrap();
        assert_eq!(source.points, expected);
        assert!(source.points.len() <= input.len() * 4 + 2);
        assert_eq!(source.original.units().collect::<Vec<_>>(), input);
    }
}

#[test]
fn work_accounting_equals_utf8_bytes_and_does_not_price_surrogates_as_replacement() {
    for value in ["", "a", "é", "中", "😀", "aé中😀"] {
        assert_eq!(work_bytes(JavaText::from_utf8(value)).unwrap(), value.len());
        assert_eq!(work_bytes(JavaText::from_utf16(&value.encode_utf16().collect::<Vec<_>>())).unwrap(), value.len());
    }
    assert_eq!(work_bytes(JavaText::from_utf16(&[0xd800])).unwrap(), 3);
    assert_eq!(work_bytes(JavaText::from_utf16(&[0xd800, 0xdc00])).unwrap(), 4);
    assert_eq!(work_bytes(JavaText::from_utf16(&[0xd800, 0xd800])).unwrap(), 6);
    let input = vec![0xd800; PATTERN_BYTES / 3 + 1];
    let error = compile_text(JavaText::from_utf16(&input), JavaText::from_utf8(""), &EvalContext::default()).err().unwrap();
    assert!(matches!(error, CompileFailure::Resource(error) if error.code == "rtdb.expr/regex-limit" && error.is(rtdb_types::AnomalyKind::ProgramLimitExceeded)));
}

#[test]
fn syntax_indices_use_quoted_code_points_while_patterns_keep_original_units() {
    for (pattern, index, description) in [
        (vec![0x29], -1, "Unmatched closing ')'"),
        (vec![0xd83d, 0xde00, 0x28], 2, "Unclosed group"),
        ("\t\\Q.\\E(".encode_utf16().collect(), 4, "Unclosed group"),
        (vec![0xd800, 0x28], 2, "Unclosed group"),
    ] {
        let failure = failure(&pattern, &EvalContext::default());
        assert_eq!(failure.pattern.view().units().collect::<Vec<_>>(), pattern);
        let syntax = failure.syntax.unwrap();
        assert_eq!(syntax.index, index);
        assert_eq!(syntax.description.view().units().collect::<Vec<_>>(), description.encode_utf16().collect::<Vec<_>>());
    }
}

#[test]
fn unknown_property_description_retains_raw_name_units_and_original_pattern() {
    let input = [0xd800, 0x5c, 0x70, 0x7b, 0xdc00, 0x7d];
    let failure = failure(&input, &EvalContext::default());
    assert_eq!(failure.pattern.view().units().collect::<Vec<_>>(), input);
    assert_eq!(failure.adapter_point, Some(6));
    let syntax = failure.syntax.unwrap();
    assert_eq!(syntax.index, 5);
    let mut expected = "Unknown character property name {".encode_utf16().collect::<Vec<_>>();
    expected.extend([0xdc00, 0x7d]);
    assert_eq!(syntax.description.view().units().collect::<Vec<_>>(), expected);
}

#[test]
fn syntax_and_unmapped_dynamic_failures_keep_their_charges_after_scope_drop() {
    for input in [vec![0xd800, 0x28], format!("\\p{{x={}}}", "a".repeat(8192)).encode_utf16().collect()] {
        let memory = MemoryBudget::new(128 * 1024 * 1024);
        let failure = {
            let scope = EvaluationScope::new(&memory).unwrap();
            failure(&input, &EvalContext { allocation: Some(&scope), ..Default::default() })
        };
        let texts = failure.pattern.retained_bytes() + failure.syntax.as_ref().map_or(0, |syntax| syntax.description.retained_bytes());
        assert!(memory.used_bytes() >= texts + failure.error().code.capacity() + failure.error().message.as_ref().map_or(0, String::capacity));
        assert_eq!(failure.pattern.view().units().collect::<Vec<_>>(), input);
        if input.len() > 100 { assert!(failure.syntax.is_none()); assert!(failure.error().message.as_ref().unwrap().len() > 8192); }
        drop(failure);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn backend_failures_retain_source_without_claiming_a_java_index() {
    let memory = MemoryBudget::new(64 * 1024 * 1024);
    let failure = {
        let scope = EvaluationScope::new(&memory).unwrap();
        failure(&[0x2a], &EvalContext { allocation: Some(&scope), ..Default::default() })
    };
    assert!(failure.syntax.is_none());
    assert_eq!(failure.adapter_point, None);
    assert_eq!(failure.pattern.view().units().collect::<Vec<_>>(), [0x2a]);
    assert!(memory.used_bytes() > failure.pattern.retained_bytes());
    drop(failure);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn deferred_grammar_failures_do_not_claim_later_native_errors_are_java_errors() {
    for input in ["(*)", "(*", "*)", "(a{3,2}", "(?x)\\p{unknown}", "(?x:a)(", "[\\p{unknown}]"] {
        let input = input.encode_utf16().collect::<Vec<_>>();
        let failure = failure(&input, &EvalContext::default());
        assert!(failure.syntax.is_none());
        assert_eq!(failure.pattern.view().units().collect::<Vec<_>>(), input);
    }
    for input in [vec![0xd800, 0x28, 0x2a], vec![0xdc00, 0x28, 0x2a, 0x29]] {
        let failure = failure(&input, &EvalContext::default());
        assert!(failure.syntax.is_none());
        assert_eq!(failure.pattern.view().units().collect::<Vec<_>>(), input);
    }
}

#[test]
fn failed_retained_copy_drops_the_partial_error_lease() {
    for include_pattern in [false, true] {
        let error = illegal("Unclosed group");
        let retained = std::mem::size_of::<Anomaly>() + error.code.capacity() + error.message.as_ref().unwrap().capacity() + 256;
        let pattern = JavaString::allocation_bound_utf8("(").unwrap();
        let description = JavaString::allocation_bound_utf8("Unclosed group").unwrap();
        let texts = pattern + if include_pattern { description } else { 0 };
        let memory = MemoryBudget::new(4096 + retained + texts - 1);
        let scope = EvaluationScope::new(&memory).unwrap();
        let syntax = Some(Syntax { description: Description::Static("Unclosed group"), index: 1 });
        let error = CompileFailure::retain(error, JavaText::from_utf8("("), Some(1), syntax, &EvalContext { allocation: Some(&scope), ..Default::default() });
        assert!(matches!(error, CompileFailure::Resource(error) if error.code == "rtdb/memory-budget-exceeded"));
        assert_eq!(memory.used_bytes(), 4096);
        drop(scope);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn preprocessing_admits_both_point_arrays_before_construction() {
    let points = 7usize;
    let bytes = (points + points * 4 + 2) * std::mem::size_of::<u32>() + 256;
    let memory = MemoryBudget::new(4096 + bytes - 1);
    let scope = EvaluationScope::new(&memory).unwrap();
    assert!(PatternInput::new(JavaText::from_utf8("\\Q1.*\\E"), &EvalContext { allocation: Some(&scope), ..Default::default() }).is_err());
    assert_eq!(memory.used_bytes(), 4096);
    drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}
