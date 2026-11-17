use super::*;
use rtdb_columnar::MemoryBudget;

const PROFILES: [Profile; 2] = [Profile::Jackson216, Profile::Jackson221];
fn slice(offset: usize, length: usize, returned: Returned<'_>) -> Observation<'_> { Observation { call: Call::Read(ReadRequest { method: ReadMethod::Slice, offset, length }), returned } }
fn whole(length: usize, returned: Returned<'_>) -> Observation<'_> { Observation { call: Call::Read(ReadRequest { method: ReadMethod::WholeBuffer, offset: 0, length }), returned } }
fn close(returned: Returned<'_>) -> Observation<'_> { Observation { call: Call::Close, returned } }
fn bytes(points: &[u32], little: bool) -> Vec<u8> { points.iter().flat_map(|point| if little { point.to_le_bytes() } else { point.to_be_bytes() }).collect() }
fn run(inspect: impl FnOnce(&mut ConversionContext)) {
    let budget = MemoryBudget::new(1 << 24); let mut context = ConversionContext::new(Arc::clone(&budget));
    inspect(&mut context); drop(context); assert_eq!(budget.used_bytes(), 0);
}
fn reader<'a>(capacity: usize, observations: &'a [Observation<'a>], profile: Profile, context: &mut ConversionContext) -> Utf32Reader<'a> {
    match bootstrap(capacity, ReadTape::new(observations), profile, context).unwrap() { DetectedInput::Utf32(reader) => reader, _ => panic!("Expected UTF32") }
}
fn utf8<'a>(capacity: usize, observations: &'a [Observation<'a>], profile: Profile, context: &mut ConversionContext) -> Utf8Window<'a> {
    match bootstrap(capacity, ReadTape::new(observations), profile, context).unwrap() { DetectedInput::Utf8(window) => window, _ => panic!("Expected UTF8") }
}

#[test]
fn bootstrap_requests_remaining_capacity_and_keeps_excess_unparsed_bytes() {
    for profile in PROFILES { run(|context| {
        let observations = [slice(0, 16, Returned::Bytes(b"{")), slice(1, 15, Returned::Bytes(b"}not json")), close(Returned::Closed)];
        let mut window = utf8(16, &observations, profile, context);
        assert_eq!(window.observations_consumed(), 2);
        assert_eq!(window.next().unwrap(), Some(b'{')); assert_eq!(window.next().unwrap(), Some(b'}'));
        assert_eq!(window.observations_consumed(), 2);
        window.close().unwrap(); assert_eq!(window.observations_consumed(), 3);
    }); }
}

#[test]
fn failed_four_byte_bootstrap_can_read_eof_again_for_the_two_byte_probe() {
    for profile in PROFILES { run(|context| {
        let observations = [slice(0, 16, Returned::Bytes(b"x")), slice(1, 15, Returned::Eof), slice(1, 15, Returned::Eof), close(Returned::Closed)];
        let mut window = utf8(16, &observations, profile, context);
        assert_eq!(window.observations_consumed(), 3); assert_eq!(window.next().unwrap(), Some(b'x'));
        window.close().unwrap();
        let observations = [slice(0, 16, Returned::Bytes(&[0, 0])), slice(2, 14, Returned::Zero)];
        match bootstrap(16, ReadTape::new(&observations), profile, context).unwrap() {
            DetectedInput::Utf16 { input, little } => { assert!(!little); assert_eq!(input.buffered(), &[0, 0]); assert_eq!(input.observations_consumed(), 2); }
            _ => panic!("Expected the two-byte UTF16 fallback"),
        }
    }); }
}

#[test]
fn bom_and_zero_byte_detection_preserve_endianness_and_unsupported_orders() {
    for profile in PROFILES { run(|context| {
        for little in [false, true] {
            for bom in [false, true] {
                let data = bytes(if bom { &[0xfeff, 0x7b] } else { &[0x7b] }, little);
                let observations = [slice(0, 16, Returned::Bytes(&data)), close(Returned::Closed)];
                let mut value = reader(16, &observations, profile, context); let mut output = [0];
                assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], 0x7b); value.close().unwrap();
            }
        }
        for (data, expected) in [([0, 0, 0xff, 0xfe], "2143"), ([0xfe, 0xff, 0, 0], "3412"), ([0, 0x7b, 0, 0], "3412"), ([0, 0, 0x7b, 0], "2143")] {
            let observations = [slice(0, 16, Returned::Bytes(&data))];
            assert!(matches!(bootstrap(16, ReadTape::new(&observations), profile, context), Err(Failure::UnsupportedOrder(order)) if order == expected));
        }
    }); }
}

#[test]
fn utf8_bom_changes_buffer_pointer_without_changing_the_initial_processed_position() {
    for profile in PROFILES { run(|context| {
        let observations = [slice(0, 8, Returned::Bytes(b"\xef\xbb\xbf{}")), slice(0, 8, Returned::Eof), close(Returned::Closed)];
        let mut value = utf8(8, &observations, profile, context);
        assert_eq!(value.position(), 3); assert_eq!(value.processed(), 0);
        assert_eq!(value.next().unwrap(), Some(b'{')); assert_eq!(value.next().unwrap(), Some(b'}'));
        assert_eq!(value.next().unwrap(), None); assert_eq!(value.processed(), 5);
        value.close().unwrap(); assert_eq!(value.observations_consumed(), 3);
    }); }
}

#[test]
fn a_coalesced_utf32_suffix_fails_before_the_first_parser_character_is_published() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0x7b, 0x7d, 0x110000], false);
        let observations = [slice(0, 64, Returned::Bytes(&data)), close(Returned::Closed)];
        let mut window = ReaderWindow::new(reader(64, &observations, profile, context), 4000, context).unwrap();
        assert!(matches!(window.next(), Err(Failure::InvalidUtf32 { value: 0x100000, byte_position: 15, char_position: 2 })));
        assert_eq!(window.position(), 0); assert_eq!(window.reader.counters(), (0, 0));
        assert_eq!(&window.buffer.as_ref().unwrap().values[..2], &[0x7b, 0x7d]);
        window.close().unwrap();
    }); }
}

#[test]
fn a_split_utf32_suffix_is_not_read_if_the_caller_completes_and_closes_the_root() {
    for profile in PROFILES { run(|context| {
        let prefix = bytes(&[0xfeff, 0x7b, 0x7d], false);
        let observations = [slice(0, 64, Returned::Bytes(&prefix)), close(Returned::Closed)];
        let mut window = ReaderWindow::new(reader(64, &observations, profile, context), 4000, context).unwrap();
        assert_eq!(window.next().unwrap(), Some(0x7b)); assert_eq!(window.next().unwrap(), Some(0x7d));
        assert_eq!(window.observations_consumed(), 1); window.close().unwrap(); assert_eq!(window.observations_consumed(), 2);
        let suffix = bytes(&[0x110000], false);
        let observations = [slice(0, 64, Returned::Bytes(&prefix)), whole(64, Returned::Bytes(&suffix)), close(Returned::Closed)];
        let mut window = ReaderWindow::new(reader(64, &observations, profile, context), 4000, context).unwrap();
        window.next().unwrap(); window.next().unwrap();
        assert!(matches!(window.next(), Err(Failure::InvalidUtf32 { char_position: 2, byte_position: 15, .. })));
        assert_eq!(window.processed(), 2); window.close().unwrap();
    }); }
}

#[test]
fn utf32_refills_a_whole_buffer_then_only_the_missing_partial_word_tail() {
    for profile in PROFILES { run(|context| {
        let observations = [slice(0, 16, Returned::Bytes(&[0, 0, 0xfe, 0xff])), whole(16, Returned::Bytes(&[0])), slice(1, 15, Returned::Bytes(&[0, 0, 0x41])), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context); let mut output = [0; 8];
        assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], 0x41);
        assert_eq!(value.counters(), (4, 1)); assert_eq!(value.observations_consumed(), 3); value.close().unwrap();
    }); }
}

#[test]
fn utf32_returns_a_valid_prefix_before_compacting_an_incomplete_following_word() {
    for profile in PROFILES { run(|context| {
        let prefix = [0, 0, 0xfe, 0xff, 0, 0, 0, 0x41, 0, 0];
        let observations = [slice(0, 16, Returned::Bytes(&prefix)), slice(2, 14, Returned::Bytes(&[0, 0x42])), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context); let mut output = [0; 8];
        assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], 0x41);
        assert_eq!(value.buffered_bytes(), 2); assert_eq!(value.observations_consumed(), 1);
        assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], 0x42);
        assert_eq!(value.counters(), (8, 2)); value.close().unwrap();
    }); }
}

#[test]
fn partial_word_eof_releases_the_byte_buffer_before_reporting_source_positions() {
    for profile in PROFILES { run(|context| {
        let prefix = [0, 0, 0xfe, 0xff, 0, 0, 0, 0x41, 0, 0];
        let observations = [slice(0, 16, Returned::Bytes(&prefix)), slice(2, 14, Returned::Eof), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context); let mut output = [0; 8]; value.read_into(&mut output).unwrap();
        assert!(matches!(value.read_into(&mut output), Err(Failure::TruncatedUtf32 { got: 2, byte_position: 10, char_position: 1 })));
        assert!(!value.buffer_live()); assert_eq!(value.read_into(&mut output).unwrap(), None);
        assert_eq!(value.observations_consumed(), 2); value.close().unwrap();
    }); }
}

#[test]
fn a_clean_utf32_eof_is_not_reobserved_after_buffer_release_and_closes_once() {
    for profile in PROFILES { run(|context| {
        let observations = [slice(0, 16, Returned::Bytes(&[0, 0, 0xfe, 0xff])), whole(16, Returned::Eof), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context); let mut output = [0];
        assert_eq!(value.read_into(&mut output).unwrap(), None); assert!(!value.buffer_live());
        assert_eq!(value.read_into(&mut output).unwrap(), None); assert_eq!(value.observations_consumed(), 2);
        value.close().unwrap(); value.close().unwrap(); assert_eq!(value.observations_consumed(), 3);
    }); }
}

#[test]
fn source_zero_reads_have_distinct_bootstrap_utf8_and_utf32_failure_and_close_order() {
    for profile in PROFILES { run(|context| {
        let observations = [slice(0, 8, Returned::Bytes(b"null")), slice(0, 8, Returned::Zero), close(Returned::Io("close failure"))];
        let mut value = utf8(8, &observations, profile, context);
        for _ in 0..4 { value.next().unwrap(); }
        assert!(matches!(value.next(), Err(Failure::Io { stage: Stage::SourceClose, detail: "close failure" })));
        assert_eq!(value.observations_consumed(), 3);
        let observations = [slice(0, 8, Returned::Bytes(&[0, 0, 0xfe, 0xff])), whole(8, Returned::Zero), close(Returned::Io("close failure"))];
        let mut value = reader(8, &observations, profile, context); let mut output = [0];
        assert!(matches!(value.read_into(&mut output), Err(Failure::ZeroRead(Stage::Utf32Refill))));
        assert_eq!(value.observations_consumed(), 2); assert!(value.buffer_live());
        assert!(matches!(value.close(), Err(Failure::Io { stage: Stage::SourceClose, .. }))); assert!(!value.buffer_live());
    }); }
}

#[test]
fn profile_specific_pending_units_are_returned_without_a_new_stream_read() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0x1f600, 0x5a], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context); let mut output = [0];
        assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], 0xd83d);
        assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], if profile == Profile::Jackson216 { 0xf600 } else { 0xde00 });
        assert_eq!(value.observations_consumed(), 1); assert_eq!(value.counters(), (0, 2));
        assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], 0x5a); value.close().unwrap();
    }); }
}

#[test]
fn the_older_zero_pending_sentinel_loses_the_low_unit_at_a_pair_boundary() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0x10000], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), whole(16, Returned::Eof), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context); let mut output = [0];
        assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], 0xd800);
        if profile == Profile::Jackson216 { assert_eq!(value.read_into(&mut output).unwrap(), None); assert_eq!(value.counters(), (8, 1)); }
        else { assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], 0xdc00); assert_eq!(value.observations_consumed(), 1); assert_eq!(value.read_into(&mut output).unwrap(), None); assert_eq!(value.counters(), (8, 2)); }
        value.close().unwrap();
    }); }
}

#[test]
fn an_observed_larger_byte_buffer_exposes_the_pending_difference_at_the_four_thousand_unit_parser_boundary() {
    for profile in PROFILES { run(|context| {
        let mut points = vec![0xfeff]; points.extend(std::iter::repeat_n(0x20, 3999)); points.push(0x1f600);
        let data = bytes(&points, false);
        let observations = [slice(0, data.len(), Returned::Bytes(&data)), close(Returned::Closed)];
        let mut window = ReaderWindow::new(reader(data.len(), &observations, profile, context), 4000, context).unwrap();
        for _ in 0..3999 { assert_eq!(window.next().unwrap(), Some(0x20)); }
        assert_eq!(window.next().unwrap(), Some(0xd83d)); assert_eq!(window.processed(), 0);
        assert_eq!(window.next().unwrap(), Some(if profile == Profile::Jackson216 { 0xf600 } else { 0xde00 }));
        assert_eq!(window.processed(), 4000); assert_eq!(window.observations_consumed(), 1); window.close().unwrap();
    }); }
}

#[test]
fn utf32_bmp_surrogate_values_are_units_and_failed_output_does_not_advance_char_count() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0xd800, 0x110000], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context); let mut output = [0x33; 4];
        assert!(matches!(value.read_into(&mut output), Err(Failure::InvalidUtf32 { char_position: 1, byte_position: 11, .. })));
        assert_eq!(output[0], 0xd800); assert_eq!(output[1], 0x33); assert_eq!(value.counters(), (0, 0)); value.close().unwrap();
    }); }
}

#[test]
fn observed_io_errors_tape_mismatches_and_native_admission_failures_stay_distinct() {
    run(|context| {
        let observations = [slice(0, 8, Returned::Io("bootstrap failure"))];
        assert!(matches!(bootstrap(8, ReadTape::new(&observations), Profile::Jackson216, context), Err(Failure::Io { stage: Stage::Bootstrap, detail: "bootstrap failure" })));
        let observations = [whole(8, Returned::Bytes(b"null"))];
        assert!(matches!(bootstrap(8, ReadTape::new(&observations), Profile::Jackson216, context), Err(Failure::Observation { index: 0, .. })));
        let observations = [slice(0, 8, Returned::Bytes(b"null")), whole(8, Returned::Bytes(b"wrong call"))];
        let mut value = utf8(8, &observations, Profile::Jackson216, context);
        for _ in 0..4 { value.next().unwrap(); }
        assert_eq!(value.observations_consumed(), 1);
        assert!(matches!(value.next(), Err(Failure::Observation { index: 1, .. })));
    });
    let budget = MemoryBudget::new(0); let mut context = ConversionContext::new(Arc::clone(&budget));
    match bootstrap(8, ReadTape::new(&[]), Profile::Jackson216, &mut context) {
        Err(Failure::Native(cause)) => assert!(cause.code.ends_with("/batch-too-large")),
        _ => panic!("Admission must fail before the missing first read observation"),
    }
    drop(context); assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn explicit_parser_close_releases_both_payloads_even_when_source_close_throws() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0x7b], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), close(Returned::Io("close failure"))];
        let mut window = ReaderWindow::new(reader(16, &observations, profile, context), 4000, context).unwrap();
        assert!(matches!(window.close(), Err(Failure::Io { stage: Stage::SourceClose, .. })));
        assert!(window.buffer.is_none() && !window.reader.buffer_live());
        window.close().unwrap(); assert_eq!(window.next().unwrap(), None); assert_eq!(window.observations_consumed(), 2);
    }); }
}

#[test]
fn reader_counters_use_java_wrapping_arithmetic_and_storage_outlives_the_caller_context() {
    let budget = MemoryBudget::new(1 << 20); let mut context = ConversionContext::new(Arc::clone(&budget));
    let data = bytes(&[0xfeff, 0x41, 0x42, 0x43], false);
    let observations = [slice(0, 16, Returned::Bytes(&data)), close(Returned::Closed)];
    let mut value = reader(16, &observations, Profile::Jackson216, &mut context);
    value.char_count = i32::MAX - 1;
    let mut output = [0; 3]; assert_eq!(value.read_into(&mut output).unwrap(), Some(3)); assert_eq!(value.counters().1, i32::MIN + 1);
    drop(context); assert!(budget.used_bytes() > 0); value.close().unwrap(); assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn each_payload_is_admitted_at_its_exact_boundary_before_the_next_source_call() {
    let required = 8 + 256 + 4000 * 2 + 256;
    for extra in [0, 1] {
        let budget = MemoryBudget::new(required - extra); let mut context = ConversionContext::new(Arc::clone(&budget));
        let observations = [slice(0, 8, Returned::Bytes(&[0, 0, 0xfe, 0xff])), close(Returned::Closed)];
        let value = reader(8, &observations, Profile::Jackson216, &mut context);
        let result = ReaderWindow::new(value, 4000, &mut context);
        if extra == 0 {
            let mut window = result.unwrap(); assert_eq!(window.observations_consumed(), 1); assert_eq!(budget.used_bytes(), required); window.close().unwrap();
        } else { assert!(matches!(result, Err(Failure::Native(_)))); }
        drop(context); assert_eq!(budget.used_bytes(), 0);
    }
    run(|context| {
        assert!(matches!(bootstrap(usize::MAX, ReadTape::new(&[]), Profile::Jackson216, context), Err(Failure::Native(cause)) if cause.code.ends_with("/allocation-overflow")));
    });
}
