use super::*;
use rtdb_types::{AnomalyCategory, MemoryBudget};
use crate::EvaluationScope;

fn units(value: &str) -> Vec<u16> { value.encode_utf16().collect() }
fn raw(pattern: &str, replacement: &[u16]) -> CompiledReplacement {
    compile_replacement_text(JavaText::from_utf8(pattern), JavaText::from_utf16(replacement), JavaText::from_utf8(""), &EvalContext::default()).unwrap()
}
fn output(pattern: &str, replacement: &[u16], source: &[u16]) -> Vec<u16> {
    raw(pattern, replacement).replace_text(JavaText::from_utf16(source), &EvalContext::default()).unwrap().view().units().collect()
}

#[test]
fn postgres_normalization_is_one_length_preserving_unit_pass() {
    for (source, expected) in [
        (units(r"\12"), units("$12")),
        (units(r"\\12"), units(r"\\12")),
        (units(r"\\\12"), units(r"\\$12")),
        (units(r"\١\１"), units(r"\١\１")),
        (vec![0x5c, 0xd800, 0x5c, 0x31, 0xdc00], vec![0x5c, 0xd800, 0x24, 0x31, 0xdc00]),
        (vec![0xd83d, 0xde00, 0x5c, 0x5c, 0xdfff], vec![0xd83d, 0xde00, 0x5c, 0x5c, 0xdfff]),
    ] {
        let normalized = postgres_backrefs(JavaText::from_utf16(&source), &EvalContext::default()).unwrap();
        assert_eq!(normalized.view().len_units(), source.len());
        assert_eq!(normalized.view().units().collect::<Vec<_>>(), expected);
    }
}

#[test]
fn utf8_and_raw_replacements_share_groups_escapes_and_empty_match_rules() {
    for (source, pattern, replacement, expected) in [
        ("ab", "(a)(b)", "$2$1$0", "baab"),
        ("a", "(a)", "$12", "a2"),
        ("a", "(a)", "$0000001", "a"),
        ("b", "(a)?b", "$1", ""),
        ("ab", "(?<first>a)(b)", "${first}$2", "ab"),
        ("a", "(a)", r"\1", "a"),
        ("a", "(a)", r"\\1", r"\1"),
        ("a", "a", r"\$", "$"),
        ("😀", "", "", "😀"),
        ("😀", "", "x", "x?x?x"),
        ("😀", r"\G", "x", "x😀"),
    ] {
        let compiled = compile_replacement(pattern, replacement, "", &EvalContext::default()).unwrap();
        assert_eq!(compiled.replace(source, &EvalContext::default()).unwrap(), Value::Utf8(expected.into()));
        let result = raw(pattern, &units(replacement)).replace_text(JavaText::from_utf16(&units(source)), &EvalContext::default()).unwrap();
        assert_eq!(utf8(result.view(), &EvalContext::default()).unwrap(), expected);
    }
}

#[test]
fn escaped_and_fragmented_surrogate_units_join_only_in_the_completed_result() {
    let pair = vec![0xd83d, 0xde00];
    for (pattern, replacement, source) in [
        ("x", vec![0x5c, 0xd83d, 0xde00], vec![0x78]),
        ("x", vec![0xde00], vec![0xd83d, 0x78]),
        ("x", vec![0xd83d], vec![0x78, 0xde00]),
        ("x", vec![], vec![0xd83d, 0x78, 0xde00]),
        (r"(\uD83D)x", vec![0x24, 0x31, 0xde00], vec![0xd83d, 0x78]),
        (r"x(\uDE00)", vec![0xd83d, 0x24, 0x31], vec![0x78, 0xde00]),
    ] {
        let result = raw(pattern, &replacement).replace_text(JavaText::from_utf16(&source), &EvalContext::default()).unwrap();
        assert_eq!(result.view().units().collect::<Vec<_>>(), pair);
        assert_eq!(utf8(result.view(), &EvalContext::default()).unwrap(), "😀");
    }
    assert_eq!(output("x", &[0xd800], &[0x78]), [0xd800]);
    assert_eq!(output("x", &[0xdc00], &[0x78]), [0xdc00]);
    assert_eq!(output("", &[], &pair), pair);
    assert_eq!(output("", &[0x78], &pair), [0x78, 0xd83d, 0x78, 0xde00, 0x78]);
}

#[test]
fn no_match_never_parses_invalid_replacement_and_keeps_raw_source_units() {
    let source = [0xd800, 0x7a, 0xdc00];
    for replacement in ["$", "\\", "${missing}", "$9", "${1}"] {
        assert_eq!(output("x", &units(replacement), &source), source);
    }
    assert_eq!(output("x", &[0x24, 0xd800], &source), source);
}

#[test]
fn replacement_failures_preserve_first_exception_class_and_exact_message() {
    for (replacement, kind, message) in [
        ("\\", ReplacementExceptionKind::IllegalArgument, "character to be escaped is missing"),
        ("$", ReplacementExceptionKind::IllegalArgument, "Illegal group reference: group index is missing"),
        ("$x$9", ReplacementExceptionKind::IllegalArgument, "Illegal group reference"),
        ("${}", ReplacementExceptionKind::IllegalArgument, "named capturing group has 0 length name"),
        ("${", ReplacementExceptionKind::IllegalArgument, "named capturing group has 0 length name"),
        ("${a", ReplacementExceptionKind::IllegalArgument, "named capturing group is missing trailing '}'"),
        ("${1", ReplacementExceptionKind::IllegalArgument, "named capturing group is missing trailing '}'"),
        ("${1}", ReplacementExceptionKind::IllegalArgument, "capturing group name {1} starts with digit character"),
        ("${missing}", ReplacementExceptionKind::IllegalArgument, "No group with name {missing}"),
        ("$9$", ReplacementExceptionKind::IndexOutOfBounds, "No group 9"),
        ("$10", ReplacementExceptionKind::IndexOutOfBounds, "No group 1"),
    ] {
        let replacement = units(replacement);
        let compiled = raw("x", &replacement);
        let failure = compiled.replace_text(JavaText::from_utf8("xx"), &EvalContext::default()).unwrap_err();
        let ReplacementFailure::Java(failure) = failure else { panic!("Java replacement failure") };
        assert_eq!(failure.kind, kind);
        assert_eq!(failure.message.view().units().collect::<Vec<_>>(), units(message));
        assert_eq!(failure.replacement.view().units().collect::<Vec<_>>(), replacement);
    }
    for replacement in [vec![0xd800, 0x24, 0xdfff], vec![0x24, 0x7b, 0xd800, 0x7d]] {
        let compiled = raw("x", &replacement);
        let ReplacementFailure::Java(failure) = compiled.replace_text(JavaText::from_utf8("x"), &EvalContext::default()).unwrap_err() else { panic!("Java replacement failure") };
        assert_eq!(failure.kind, ReplacementExceptionKind::IllegalArgument);
        assert_eq!(failure.replacement.view().units().collect::<Vec<_>>(), replacement);
    }
}

#[test]
fn replacement_error_stops_after_the_first_successful_match_prefix() {
    let compiled = raw("x", &units("$9$"));
    let mut emitted = Vec::new();
    let error = compiled.pass(&units("axbx"), |part| { emitted.push(part.to_vec()); Ok(()) }).err().unwrap();
    assert!(matches!(error, ExpansionFailure::Java(ReplacementExceptionKind::IndexOutOfBounds, Message::Group(9))));
    assert_eq!(emitted, [units("a")]);
}

#[test]
fn normalized_replacement_keeps_its_lease_after_metadata_and_preparation_drop() {
    let memory = MemoryBudget::new(128 * 1024 * 1024);
    let replacement = {
        let scope = EvaluationScope::new(&memory).unwrap();
        let input = [0xd800, 0x5c, 0x31, 0xdc00];
        let compiled = compile_replacement_text(JavaText::from_utf8("(x)"), JavaText::from_utf16(&input), JavaText::from_utf8(""), &EvalContext { allocation: Some(&scope), ..Default::default() }).unwrap();
        compiled.replacement.clone()
    };
    assert_eq!(replacement.view().units().collect::<Vec<_>>(), [0xd800, 0x24, 0x31, 0xdc00]);
    assert_eq!(memory.used_bytes(), replacement.retained_bytes());
    drop(replacement);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn raw_output_and_its_clones_retain_storage_after_the_row_scope_ends() {
    let compiled = raw("x", &[0xd800]);
    for (source, expected) in [([0x78, 0x78], [0xd800, 0xd800]), ([0xd800, 0x7a], [0xd800, 0x7a])] {
        let memory = MemoryBudget::new(64 * 1024);
        let output = {
            let scope = EvaluationScope::new(&memory).unwrap();
            compiled.replace_text(JavaText::from_utf16(&source), &EvalContext { allocation: Some(&scope), ..Default::default() }).unwrap()
        };
        assert_eq!(output.view().units().collect::<Vec<_>>(), expected);
        assert_eq!(memory.used_bytes(), output.retained_bytes());
        let copy = output.clone();
        drop(output);
        assert_eq!(memory.used_bytes(), copy.retained_bytes());
        drop(copy);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn java_failure_retains_message_and_replacement_after_both_scopes_end() {
    let memory = MemoryBudget::new(128 * 1024 * 1024);
    let failure = {
        let preparation = EvaluationScope::new(&memory).unwrap();
        let compiled = compile_replacement_text(JavaText::from_utf8("x"), JavaText::from_utf16(&[0xd800, 0x24, 0x39]), JavaText::from_utf8(""), &EvalContext { allocation: Some(&preparation), ..Default::default() }).unwrap();
        let row = EvaluationScope::new(&memory).unwrap();
        compiled.replace_text(JavaText::from_utf8("x"), &EvalContext { allocation: Some(&row), ..Default::default() }).unwrap_err()
    };
    let ReplacementFailure::Java(failure) = failure else { panic!("Java replacement failure") };
    assert_eq!(failure.kind, ReplacementExceptionKind::IndexOutOfBounds);
    assert_eq!(failure.message.view().units().collect::<Vec<_>>(), units("No group 9"));
    assert_eq!(memory.used_bytes(), failure.message.retained_bytes() + failure.replacement.retained_bytes());
    drop(failure);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_matcher_failure_retains_its_diagnostic_after_the_row_scope_ends() {
    let compiled = raw(r"\A(?:(?=ab)c|a\b{g}b)\z", &units("$9"));
    let memory = MemoryBudget::new(64 * 1024);
    let failure = {
        let scope = EvaluationScope::new(&memory).unwrap();
        compiled.replace_text(JavaText::from_utf8("ab"), &EvalContext { allocation: Some(&scope), ..Default::default() }).unwrap_err()
    };
    let ReplacementFailure::Native(failure) = failure else { panic!("native matcher failure") };
    assert_eq!(failure.error.error().category, AnomalyCategory::Fault);
    assert_eq!(failure.error.error().code, "xtdb.error/unknown");
    assert_eq!(failure.replacement.view().units().collect::<Vec<_>>(), units("$9"));
    assert!(memory.used_bytes() >= failure.error.error().code.capacity() + failure.error.error().message.as_ref().unwrap().capacity());
    drop(failure);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn normalization_admits_scratch_and_retained_copy_without_untracked_fallback() {
    let source = [0xd800; 32];
    let scratch = source.len() * 2 + 256;
    let retained = JavaString::allocation_bound_utf16(&source).unwrap();
    for (extra, expected) in [(scratch - 1, 4096), (scratch + retained - 1, 4096 + scratch)] {
        let memory = MemoryBudget::new(4096 + extra);
        let scope = EvaluationScope::new(&memory).unwrap();
        let error = postgres_backrefs(JavaText::from_utf16(&source), &EvalContext { allocation: Some(&scope), ..Default::default() }).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), expected);
        drop(scope);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn source_admission_fails_before_match_or_replacement_parsing() {
    let compiled = raw("x", &units("$9"));
    let memory = MemoryBudget::new(4096 + 32 + 8192 + 256 - 1);
    let scope = EvaluationScope::new(&memory).unwrap();
    let failure = compiled.replace_text(JavaText::from_utf8("x"), &EvalContext { allocation: Some(&scope), ..Default::default() }).unwrap_err();
    assert!(matches!(failure, ReplacementFailure::Resource(error) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(memory.used_bytes(), 4096);
    drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn retained_output_copy_failure_keeps_only_the_live_scope_charges() {
    let compiled = raw("x", &units("abc"));
    let scratch = 32 + 8192 + 256 + 6 + 256;
    let retained = JavaString::allocation_bound_utf16(&units("abc")).unwrap();
    let memory = MemoryBudget::new(4096 + scratch + retained - 1);
    let scope = EvaluationScope::new(&memory).unwrap();
    let failure = compiled.replace_text(JavaText::from_utf8("x"), &EvalContext { allocation: Some(&scope), ..Default::default() }).unwrap_err();
    assert!(matches!(failure, ReplacementFailure::Resource(error) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(memory.used_bytes(), 4096 + scratch);
    drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn wrapper_byte_admission_counts_the_retained_unit_result_overlap() {
    let compiled = raw("x", &units("中"));
    let scratch = 32 + 8192 + 256 + 2 + 256;
    let retained = JavaString::allocation_bound_utf16(&units("中")).unwrap();
    let memory = MemoryBudget::new(4096 + scratch + retained + 3 + 256 - 1);
    let scope = EvaluationScope::new(&memory).unwrap();
    let error = compiled.replace("x", &EvalContext { allocation: Some(&scope), ..Default::default() }).unwrap_err();
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), 4096 + scratch);
    drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn failed_java_failure_message_admission_releases_partial_storage() {
    let compiled = raw("x", &units("$"));
    let scratch = 32 + 8192 + 256;
    let description = JavaString::allocation_bound_utf8("Illegal group reference: group index is missing").unwrap();
    let memory = MemoryBudget::new(4096 + scratch + description - 1);
    let scope = EvaluationScope::new(&memory).unwrap();
    let failure = compiled.replace_text(JavaText::from_utf8("x"), &EvalContext { allocation: Some(&scope), ..Default::default() }).unwrap_err();
    assert!(matches!(failure, ReplacementFailure::Resource(error) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(memory.used_bytes(), 4096 + scratch);
    drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn failed_named_failure_copy_leaves_no_retained_message_allocation() {
    let compiled = raw("x", &units("${missing}"));
    let scratch = 32 + 8192 + 256 + "No group with name {missing}".len() * 2 + 256;
    let description = JavaString::allocation_bound_utf16(&units("No group with name {missing}")).unwrap();
    let memory = MemoryBudget::new(4096 + scratch + description - 1);
    let scope = EvaluationScope::new(&memory).unwrap();
    let failure = compiled.replace_text(JavaText::from_utf8("x"), &EvalContext { allocation: Some(&scope), ..Default::default() }).unwrap_err();
    assert!(matches!(failure, ReplacementFailure::Resource(error) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(memory.used_bytes(), 4096 + scratch);
    drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn failed_native_diagnostic_retention_drops_the_temporary_under_its_scope() {
    let compiled = raw("x", &units("replacement"));
    let memory = MemoryBudget::new(4096 + 8192 + 256);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope), ..Default::default() };
    context.reserve(8192).unwrap();
    let failure = compiled.retain(ExpansionFailure::Native(Anomaly::fault("test/native", "diagnostic")), &context);
    assert!(matches!(failure, ReplacementFailure::Resource(error) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(memory.used_bytes(), 4096 + 8192 + 256);
    drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn output_and_unit_capacity_reject_overflow_without_large_allocations() {
    assert!(unit_bytes(i32::MAX as usize + 1).is_err());
    assert!(unit_bytes(usize::MAX).is_err());
    assert!(add(i32::MAX as usize, 1).is_err());
    assert_eq!(unit_bytes(32).unwrap(), 64);
}
