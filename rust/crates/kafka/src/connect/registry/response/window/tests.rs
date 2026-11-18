use super::{ConversionContext, Failure, Profile, Result, input_window};
use std::sync::Arc;
use rtdb_columnar::MemoryBudget;
use crate::connect::registry::input_window::{bootstrap, Call, DetectedInput, Observation, ReadMethod, ReadRequest, ReadTape, Returned, ReaderWindow, Utf16Reader, Utf16Window};
use super::super::{FailureKind, LexicalObservations, NameObservation, ObservedDecode, WindowInput, schema_from_window, schema_string_from_window};

const PROFILES: [Profile; 2] = [Profile::Jackson216, Profile::Jackson221];
fn slice(length: usize, returned: Returned<'_>) -> Observation<'_> { Observation { call: Call::Read(ReadRequest { method: ReadMethod::Slice, offset: 0, length }), returned } }
fn available(value: i32) -> Observation<'static> { Observation { call: Call::Available, returned: Returned::Available(value) } }
fn close(returned: Returned<'_>) -> Observation<'_> { Observation { call: Call::Close, returned } }
fn source_profile(profile: Profile) -> input_window::Profile { match profile { Profile::Jackson216 => input_window::Profile::Jackson216, Profile::Jackson221 => input_window::Profile::Jackson221 } }
fn utf8<'a>(capacity: usize, tape: &'a [Observation<'a>], profile: Profile, context: &mut ConversionContext) -> WindowInput<'a> {
    match bootstrap(capacity, ReadTape::new(tape), source_profile(profile), context).unwrap() { DetectedInput::Utf8(value) => WindowInput::Utf8(value), _ => panic!("Expected UTF8") }
}
fn utf32<'a>(capacity: usize, chars: usize, tape: &'a [Observation<'a>], profile: Profile, context: &mut ConversionContext) -> WindowInput<'a> {
    match bootstrap(capacity, ReadTape::new(tape), source_profile(profile), context).unwrap() { DetectedInput::Utf32(value) => WindowInput::Utf32(ReaderWindow::new(value, chars, context).unwrap()), _ => panic!("Expected UTF32") }
}
fn utf16<'a>(capacity: usize, chars: usize, tape: &'a [Observation<'a>], profile: Profile, context: &mut ConversionContext) -> WindowInput<'a> {
    match bootstrap(capacity, ReadTape::new(tape), source_profile(profile), context).unwrap() {
        DetectedInput::Utf16 { input, little } => WindowInput::Utf16(Utf16Window::new(Utf16Reader::new(input, little, context).unwrap(), chars, context).unwrap()),
        _ => panic!("Expected UTF16"),
    }
}
fn name_units(entries: &[(usize, &str)]) -> Vec<(usize, Vec<u16>)> { entries.iter().map(|(start, text)| (*start, text.encode_utf16().collect())).collect() }
fn names(entries: &[(usize, Vec<u16>)]) -> Vec<NameObservation<'_>> { entries.iter().map(|(start, units)| NameObservation { start: *start, units }).collect() }
fn qualified<'a>(names: &'a [NameObservation<'a>]) -> LexicalObservations<'a> { LexicalObservations { names, text_buffers: &[] } }
fn text(value: &Option<rtdb_types::java_text::JavaString>, expected: &str) { assert!(value.as_ref().unwrap().view().units().eq(expected.encode_utf16())); }
fn native<T>(result: Result<T>, expected: &str) {
    match result {
        Err(Failure::Native(cause)) => {
            assert!(cause.code.ends_with(expected), "{cause}");
            if expected == "/unsupported-source-profile" { assert_eq!(cause.category, rtdb_types::AnomalyCategory::Unsupported); }
        }
        _ => panic!("Expected native {expected}"),
    }
}
fn decode_failure<T>(result: Result<T>, expected: FailureKind) { match result { Err(Failure::Decode { kind, .. }) => assert_eq!(kind, expected), _ => panic!("Expected decode {expected:?}") } }
fn run(inspect: impl FnOnce(&mut ConversionContext)) { let budget = MemoryBudget::new(1 << 26); let mut context = ConversionContext::new(Arc::clone(&budget)); inspect(&mut context); drop(context); assert_eq!(budget.used_bytes(), 0); }

#[test]
fn typed_beans_consume_split_utf8_once_and_keep_field_comparisons_and_copies_off_the_tape() {
    for profile in PROFILES { run(|context| {
        let tape = [slice(32, Returned::Bytes(b"{\"schema\":\"he")), slice(32, Returned::Bytes(b"llo\",\"maxId\":7}")), close(Returned::Closed)];
        let entries = name_units(&[(1, "schema"), (18, "maxId")]); let names = names(&entries);
        let result: ObservedDecode<_> = schema_string_from_window(utf8(32, &tape, profile, context), profile, qualified(&names), context);
        assert_eq!(result.observations_consumed, 3); assert!(result.suppressed_close.is_none());
        let value = result.result.unwrap().unwrap(); text(&value.schema, "hello"); assert_eq!(value.max_id, Some(7));
    }); }
}

#[test]
fn nullable_nested_beans_and_duplicate_setters_use_the_existing_ordered_machine() {
    let bytes = br#"{"references":[null,{"name":"one","version":2}],"schemaType":null,"schemaType":"JSON"}"#;
    for profile in PROFILES { run(|context| {
        let tape = [slice(128, Returned::Bytes(bytes)), close(Returned::Closed)];
        let entries = name_units(&[(1, "references"), (21, "name"), (34, "version"), (48, "schemaType"), (66, "schemaType")]); let names = names(&entries);
        let result = schema_string_from_window(utf8(128, &tape, profile, context), profile, qualified(&names), context);
        assert_eq!(result.observations_consumed, 2);
        let value = result.result.unwrap().unwrap(); text(&value.schema_type, "JSON");
        let references = value.references.unwrap(); assert_eq!(references.len(), 2); assert!(references.as_slice()[0].is_none());
        text(&references.as_slice()[1].as_ref().unwrap().name, "one"); assert_eq!(references.as_slice()[1].as_ref().unwrap().version, Some(2));
    }); }
}

#[test]
fn unknown_strings_stay_lazy_until_the_following_field_step_and_skip_raw_surrogate_units() {
    for profile in PROFILES { run(|context| {
        let tape = [slice(32, Returned::Bytes(b"{\"ignored\":\"\xed\xa0")), slice(32, Returned::Bytes(b"\x80\",\"schema\":\"ok\"}")), close(Returned::Closed)];
        let entries = name_units(&[(1, "ignored"), (17, "schema")]); let names = names(&entries);
        let result = schema_string_from_window(utf8(32, &tape, profile, context), profile, qualified(&names), context);
        assert_eq!(result.observations_consumed, 3); text(&result.result.unwrap().unwrap().schema, "ok");
    }); }
}

#[test]
fn demanded_string_read_failures_survive_a_failing_close_and_keep_the_suppressed_cause() {
    for profile in PROFILES { run(|context| {
        let tape = [slice(32, Returned::Bytes(b"{\"schema\":\"ab")), slice(32, Returned::Io("value read")), close(Returned::Io("close read"))];
        let entries = name_units(&[(1, "schema")]); let names = names(&entries);
        let result = schema_string_from_window(utf8(32, &tape, profile, context), profile, qualified(&names), context);
        assert_eq!(result.observations_consumed, 3); native(result.result, "/registry-input-io");
        match result.suppressed_close { Some(Failure::Native(cause)) => assert!(cause.to_string().contains("close read")), _ => panic!("Expected suppressed close") }
    }); }
}

#[test]
fn a_successful_bean_is_not_published_when_its_source_close_fails() {
    for profile in PROFILES { run(|context| {
        let tape = [slice(8, Returned::Bytes(b"{}  ")), close(Returned::Io("close failed"))];
        let result = schema_string_from_window(utf8(8, &tape, profile, context), profile, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 2); assert!(result.suppressed_close.is_none()); native(result.result, "/registry-input-io");
    }); }
}

#[test]
fn creator_completion_prefetches_a_literal_before_construction_but_leaves_a_quoted_value_lazy() {
    for profile in PROFILES { run(|context| {
        let entries = name_units(&[(1, "schemaTags"), (16, "schemaEntity"), (32, "entityPath"), (50, "entityType"), (65, "next")]); let names = names(&entries);
        let prefix = b"{\"schemaTags\":[{\"schemaEntity\":{\"entityPath\":null,\"entityType\":0,\"next\":true";
        let tape = [slice(128, Returned::Bytes(prefix)), slice(128, Returned::Io("literal suffix")), close(Returned::Closed)];
        let result = schema_string_from_window(utf8(128, &tape, profile, context), profile, qualified(&names), context);
        assert_eq!(result.observations_consumed, 3); native(result.result, "/registry-input-io");
        let prefix = b"{\"schemaTags\":[{\"schemaEntity\":{\"entityPath\":null,\"entityType\":0,\"next\":\"";
        let tape = [slice(128, Returned::Bytes(prefix)), close(Returned::Closed)];
        let result = schema_string_from_window(utf8(128, &tape, profile, context), profile, qualified(&names), context);
        assert_eq!(result.observations_consumed, 2); decode_failure(result.result, FailureKind::Constructor);
    }); }
}

#[test]
fn creator_only_duplicate_setter_failure_precedes_the_unread_string_body() {
    let prefix = b"{\"subject\":null,\"version\":null,\"id\":null,\"schemaType\":null,\"references\":null,\"metadata\":null,\"ruleset\":null,\"schema\":null,\"schemaTags\":null,\"ruleset\":\"";
    for profile in PROFILES { run(|context| {
        let tape = [slice(256, Returned::Bytes(prefix)), close(Returned::Closed)];
        let entries = name_units(&[(1, "subject"), (16, "version"), (31, "id"), (41, "schemaType"), (59, "references"), (77, "metadata"), (93, "ruleset"), (108, "schema"), (122, "schemaTags"), (140, "ruleset")]); let names = names(&entries);
        let result = schema_from_window(utf8(256, &tape, profile, context), profile, qualified(&names), context);
        assert_eq!(result.observations_consumed, 2); decode_failure(result.result, FailureKind::MissingSetter);
    }); }
}

#[test]
fn unqualified_or_mismatched_symbol_results_stop_before_colon_and_value_refills() {
    for profile in PROFILES { run(|context| {
        for entries in [name_units(&[]), name_units(&[(2, "schema")]), name_units(&[(1, "other")])] {
            let names = names(&entries); let tape = [slice(16, Returned::Bytes(b"{\"schema\"")), close(Returned::Closed)];
            let result = schema_string_from_window(utf8(16, &tape, profile, context), profile, qualified(&names), context);
            assert_eq!(result.observations_consumed, 2); native(result.result, "/unsupported-source-profile");
        }
    }); }
}

#[test]
fn reader_refill_errors_can_precede_the_root_bean_while_an_unread_suffix_is_not_forced() {
    let root: Vec<u8> = [0xfeffu32, 0x7b, 0x7d].into_iter().flat_map(u32::to_be_bytes).collect();
    let mut coalesced = root.clone(); coalesced.extend_from_slice(&0x110000u32.to_be_bytes());
    for profile in PROFILES { run(|context| {
        let tape = [slice(32, Returned::Bytes(&coalesced)), close(Returned::Closed)];
        let result = schema_string_from_window(utf32(32, 32, &tape, profile, context), profile, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 2); native(result.result, "/registry-input-encoding");
        let tape = [slice(32, Returned::Bytes(&root)), close(Returned::Closed), slice(32, Returned::Bytes(&coalesced[root.len()..]))];
        let result = schema_string_from_window(utf32(32, 32, &tape, profile, context), profile, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 2); assert!(result.result.unwrap().is_some());
    }); }
}

#[test]
fn actual_utf32_pending_units_are_materialized_without_raw_byte_redecoding() {
    for profile in PROFILES { run(|context| {
        for point in [0x10000u32, 0x1f600] {
            let bytes: Vec<u8> = [0xfeff, 0x22, point, 0x22].into_iter().flat_map(u32::to_be_bytes).collect();
            let tape = [slice(32, Returned::Bytes(&bytes)), close(Returned::Closed)];
            let result = schema_string_from_window(utf32(32, 1, &tape, profile, context), profile, qualified(&[]), context);
            assert_eq!(result.observations_consumed, 2); let value = result.result.unwrap().unwrap().schema.unwrap();
            let expected = match (profile, point) { (Profile::Jackson216, 0x10000) => vec![0xd800], (Profile::Jackson216, _) => vec![0xd83d, 0xf600], (_, 0x10000) => vec![0xd800, 0xdc00], _ => vec![0xd83d, 0xde00] };
            assert!(value.view().units().eq(expected));
        }
    }); }
}

#[test]
fn utf16_replacement_consumes_the_actual_decoder_group_before_string_materialization() {
    let bytes: Vec<u8> = [0xfeffu16, 0x22, 0xd800, 0x41, 0x22].into_iter().flat_map(u16::to_be_bytes).collect();
    for profile in PROFILES { run(|context| {
        let tape = [slice(32, Returned::Bytes(&bytes)), close(Returned::Closed)];
        let result = schema_string_from_window(utf16(32, 3, &tape, profile, context), profile, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 2); text(&result.result.unwrap().unwrap().schema, "\u{fffd}");
    }); }
}

#[test]
fn utf16_ready_observations_decide_whether_a_later_io_failure_preempts_root_publication() {
    let bytes: Vec<u8> = [0xfeffu16, 0x7b, 0x7d].into_iter().flat_map(u16::to_be_bytes).collect();
    for profile in PROFILES { run(|context| {
        let tape = [slice(32, Returned::Bytes(&bytes)), available(0), close(Returned::Closed)];
        let result = schema_string_from_window(utf16(32, 8, &tape, profile, context), profile, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 3); assert!(result.result.unwrap().is_some());
        let tape = [slice(32, Returned::Bytes(&bytes)), available(1), slice(8192, Returned::Io("ready suffix")), close(Returned::Closed)];
        let result = schema_string_from_window(utf16(32, 8, &tape, profile, context), profile, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 4); native(result.result, "/registry-input-io");
    }); }
}

#[test]
fn numeric_and_literal_lookahead_observes_the_next_window_once_before_typed_coercion() {
    for profile in PROFILES { run(|context| {
        let entries = name_units(&[(1, "maxId")]); let names = names(&entries);
        let tape = [slice(16, Returned::Bytes(b"{\"maxId\":12")), slice(16, Returned::Bytes(b"}")), close(Returned::Closed)];
        let result = schema_string_from_window(utf8(16, &tape, profile, context), profile, qualified(&names), context);
        assert_eq!(result.observations_consumed, 3); assert_eq!(result.result.unwrap().unwrap().max_id, Some(12));
        let tape = [slice(8, Returned::Bytes(b"null")), slice(8, Returned::Eof), close(Returned::Closed)];
        let result = schema_string_from_window(utf8(8, &tape, profile, context), profile, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 3); assert!(result.result.unwrap().is_none());
    }); }
}

#[test]
fn escape_error_character_decoding_demands_its_utf8_tail_before_reporting_the_invalid_escape() {
    for profile in PROFILES { run(|context| {
        let tape = [slice(8, Returned::Bytes(b"\"ab\\\xc2")), slice(8, Returned::Io("escape continuation")), close(Returned::Closed)];
        let result = schema_string_from_window(utf8(8, &tape, profile, context), profile, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 3); native(result.result, "/registry-input-io");
        let tape = [slice(8, Returned::Bytes(b"\"ab\\q")), close(Returned::Closed)];
        let result = schema_string_from_window(utf8(8, &tape, profile, context), profile, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 2); decode_failure(result.result, FailureKind::Syntax);
    }); }
}

#[test]
fn source_tape_mismatch_and_native_admission_remain_outer_and_cleanup_closes_the_owner() {
    let budget = MemoryBudget::new(1 << 20); let mut context = ConversionContext::new(Arc::clone(&budget));
    let tape = [slice(32, Returned::Bytes(b"{\"schema\":\"ab")), slice(16, Returned::Bytes(b"c\"}")), close(Returned::Closed)];
    let entries = name_units(&[(1, "schema")]); let names = names(&entries);
    let result = schema_string_from_window(utf8(32, &tape, Profile::Jackson216, &mut context), Profile::Jackson216, qualified(&names), &mut context);
    assert_eq!(result.observations_consumed, 3); native(result.result, "/registry-input-observation");
    drop(context); assert_eq!(budget.used_bytes(), 0);
    let budget = MemoryBudget::new(1 << 20); let mut context = ConversionContext::new(Arc::clone(&budget));
    let tape = [slice(8, Returned::Bytes(b"{}  ")), close(Returned::Closed)];
    let input = utf8(8, &tape, Profile::Jackson216, &mut context);
    let blocker = budget.reserve(budget.limit_bytes() - budget.used_bytes()).unwrap();
    let result = schema_string_from_window(input, Profile::Jackson216, qualified(&[]), &mut context);
    assert_eq!(result.observations_consumed, 2); native(result.result, "/memory-budget-exceeded");
    drop(blocker); drop(context); assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn published_text_retains_its_own_admission_after_cursor_and_context_release() {
    let budget = MemoryBudget::new(1 << 20); let mut context = ConversionContext::new(Arc::clone(&budget));
    let tape = [slice(16, Returned::Bytes(b"\"hello\"")), close(Returned::Closed)];
    let result = schema_string_from_window(utf8(16, &tape, Profile::Jackson221, &mut context), Profile::Jackson221, qualified(&[]), &mut context);
    assert_eq!(result.observations_consumed, 2); let value = result.result.unwrap().unwrap();
    drop(context); assert!(budget.used_bytes() > 0); text(&value.schema, "hello"); drop(value); assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn unmodeled_error_token_and_newer_utf8_literal_rewind_paths_are_explicit_capability_failures() {
    for profile in PROFILES { run(|context| {
        let tape = [slice(16, Returned::Bytes(b"truX")), close(Returned::Closed)];
        let result = schema_string_from_window(utf8(16, &tape, profile, context), profile, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 2); native(result.result, "/unsupported-source-profile");
    }); }
    run(|context| {
        let tape = [slice(16, Returned::Bytes(b"null\xc2\xa9")), close(Returned::Closed)];
        let result = schema_string_from_window(utf8(16, &tape, Profile::Jackson221, context), Profile::Jackson221, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 2); native(result.result, "/unsupported-source-profile");
    });
}

#[test]
fn observed_utf8_values_keep_or_high_units_while_retained_names_keep_addition() {
    for profile in PROFILES { run(|context| {
        let tape = [slice(16, Returned::Bytes(b"\"\xf0\x80\x80\x80\"")), close(Returned::Closed)];
        let result = schema_string_from_window(utf8(16, &tape, profile, context), profile, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 2);
        assert!(result.result.unwrap().unwrap().schema.unwrap().view().units().eq([0xffc0, 0xdc00]));
        let tape = [slice(32, Returned::Bytes(b"{\"\xf0\x80\x80\x80\":null,\"schema\":\"ok\"}")), close(Returned::Closed)];
        let units = name_units(&[(13, "schema")]);
        let names = [NameObservation { start: 1, units: &[0xd7c0, 0xdc00] }, NameObservation { start: 13, units: &units[0].1 }];
        let result = schema_string_from_window(utf8(32, &tape, profile, context), profile, qualified(&names), context);
        assert_eq!(result.observations_consumed, 2); text(&result.result.unwrap().unwrap().schema, "ok");
    }); }
}

#[test]
fn a_transferred_utf32_window_rejects_a_different_lexical_profile_before_reading_its_first_character() {
    let bytes: Vec<u8> = [0xfeffu32, 0x7b, 0x7d].into_iter().flat_map(u32::to_be_bytes).collect();
    run(|context| {
        let tape = [slice(32, Returned::Bytes(&bytes)), close(Returned::Closed)];
        let input = utf32(32, 8, &tape, Profile::Jackson216, context);
        let result = schema_string_from_window(input, Profile::Jackson221, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 2); native(result.result, "/unsupported-source-profile");
    });
}

#[test]
fn reader_leading_zero_floats_keep_the_slow_paths_absent_exponent_length() {
    let number = format!("0.{}", "0".repeat(1000)); let body = format!("{{\"schema\":{number}}}");
    let bytes: Vec<u8> = std::iter::once(0xfeffu32).chain(body.chars().map(u32::from)).flat_map(u32::to_be_bytes).collect();
    for profile in PROFILES { run(|context| {
        let entries = name_units(&[(1, "schema")]); let names = names(&entries);
        let tape = [slice(8192, Returned::Bytes(&bytes)), close(Returned::Closed)];
        let result = schema_string_from_window(utf32(8192, 4096, &tape, profile, context), profile, qualified(&names), context);
        assert_eq!(result.observations_consumed, 2); text(&result.result.unwrap().unwrap().schema, &number);
        let tape = [slice(2048, Returned::Bytes(body.as_bytes())), close(Returned::Closed)];
        let result = schema_string_from_window(utf8(2048, &tape, profile, context), profile, qualified(&names), context);
        assert_eq!(result.observations_consumed, 2); decode_failure(result.result, FailureKind::Constraint);
    }); }
}

#[test]
fn reader_float_length_uses_the_observed_fast_or_slow_window_for_each_missing_component() {
    for number in [format!("1.{}", "0".repeat(1000)), format!("1e{}", "0".repeat(1000))] {
        let body = format!("{{\"schema\":{number}}}");
        let bytes32: Vec<u8> = std::iter::once(0xfeffu32).chain(body.chars().map(u32::from)).flat_map(u32::to_be_bytes).collect();
        let bytes16: Vec<u8> = std::iter::once(0xfeffu16).chain(body.encode_utf16()).flat_map(u16::to_be_bytes).collect();
        for profile in PROFILES { run(|context| {
            let entries = name_units(&[(1, "schema")]); let names = names(&entries);
            for chars in [4096, 64] {
                let tape = [slice(8192, Returned::Bytes(&bytes32)), close(Returned::Closed)];
                let result = schema_string_from_window(utf32(8192, chars, &tape, profile, context), profile, qualified(&names), context);
                assert_eq!(result.observations_consumed, 2);
                if chars == 4096 { decode_failure(result.result, FailureKind::Constraint); } else { text(&result.result.unwrap().unwrap().schema, &number); }
                let tape = [slice(8192, Returned::Bytes(&bytes16)), available(0), close(Returned::Closed)];
                let result = schema_string_from_window(utf16(8192, chars, &tape, profile, context), profile, qualified(&names), context);
                assert_eq!(result.observations_consumed, 3);
                if chars == 4096 { decode_failure(result.result, FailureKind::Constraint); } else { text(&result.result.unwrap().unwrap().schema, &number); }
            }
        }); }
    }
}

#[test]
fn the_minus_lookahead_refill_counts_even_when_the_rest_of_the_number_fits_the_next_window() {
    let number = format!("-1.{}", "0".repeat(1000)); let body = format!("{{\"schema\":{}{number}}}", " ".repeat(2037));
    let bytes: Vec<u8> = std::iter::once(0xfeffu32).chain(body.chars().map(u32::from)).flat_map(u32::to_be_bytes).collect();
    for profile in PROFILES { run(|context| {
        let entries = name_units(&[(1, "schema")]); let names = names(&entries);
        let tape = [slice(16384, Returned::Bytes(&bytes)), close(Returned::Closed)];
        let result = schema_string_from_window(utf32(16384, 2048, &tape, profile, context), profile, qualified(&names), context);
        assert_eq!(result.observations_consumed, 2); text(&result.result.unwrap().unwrap().schema, &number);
    }); }
}

#[test]
fn reader_slow_length_does_not_relax_integer_both_component_or_two_over_limit_cases() {
    let numbers = [format!("1{}", "0".repeat(1000)), format!("1.{}e0", "0".repeat(999)), format!("1.{}", "0".repeat(1001))];
    for number in numbers {
        let body = format!("{{\"schema\":{number}}}");
        let bytes: Vec<u8> = std::iter::once(0xfeffu32).chain(body.chars().map(u32::from)).flat_map(u32::to_be_bytes).collect();
        for profile in PROFILES { run(|context| {
            let entries = name_units(&[(1, "schema")]); let names = names(&entries);
            for chars in [4096, 64] {
                let tape = [slice(8192, Returned::Bytes(&bytes)), close(Returned::Closed)];
                let result = schema_string_from_window(utf32(8192, chars, &tape, profile, context), profile, qualified(&names), context);
                assert_eq!(result.observations_consumed, 2); decode_failure(result.result, FailureKind::Constraint);
            }
        }); }
    }
}

#[test]
fn eof_selects_the_reader_slow_guard_before_root_bean_coercion() {
    let number = format!("1.{}", "0".repeat(1000));
    let bytes: Vec<u8> = std::iter::once(0xfeffu32).chain(number.chars().map(u32::from)).flat_map(u32::to_be_bytes).collect();
    for profile in PROFILES { run(|context| {
        let tape = [slice(8192, Returned::Bytes(&bytes)), Observation { call: Call::Read(ReadRequest { method: ReadMethod::WholeBuffer, offset: 0, length: 8192 }), returned: Returned::Eof }, close(Returned::Closed)];
        let result = schema_string_from_window(utf32(8192, 4096, &tape, profile, context), profile, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 3); decode_failure(result.result, FailureKind::Coercion);
        let tape = [slice(2048, Returned::Bytes(number.as_bytes())), slice(2048, Returned::Eof), close(Returned::Closed)];
        let result = schema_string_from_window(utf8(2048, &tape, profile, context), profile, qualified(&[]), context);
        assert_eq!(result.observations_consumed, 3); decode_failure(result.result, FailureKind::Constraint);
    }); }
}
