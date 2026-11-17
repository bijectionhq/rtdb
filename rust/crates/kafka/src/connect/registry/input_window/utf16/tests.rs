use super::*;
use crate::connect::registry::input_window::{bootstrap, DetectedInput, Observation, Profile, ReadTape};
use std::sync::Arc;
use rtdb_columnar::MemoryBudget;

const PROFILES: [Profile; 2] = [Profile::Jackson216, Profile::Jackson221];
fn slice(offset: usize, length: usize, returned: Returned<'_>) -> Observation<'_> { Observation { call: Call::Read(ReadRequest { method: ReadMethod::Slice, offset, length }), returned } }
fn available(returned: Returned<'_>) -> Observation<'_> { Observation { call: Call::Available, returned } }
fn close(returned: Returned<'_>) -> Observation<'_> { Observation { call: Call::Close, returned } }
fn bytes(units: &[u16], little: bool) -> Vec<u8> { units.iter().flat_map(|unit| if little { unit.to_le_bytes() } else { unit.to_be_bytes() }).collect() }
fn run(inspect: impl FnOnce(&mut ConversionContext)) {
    let budget = MemoryBudget::new(1 << 24); let mut context = ConversionContext::new(Arc::clone(&budget));
    inspect(&mut context); drop(context); assert_eq!(budget.used_bytes(), 0);
}
fn reader<'a>(capacity: usize, observations: &'a [Observation<'a>], profile: Profile, context: &mut ConversionContext) -> Utf16Reader<'a> {
    match bootstrap(capacity, ReadTape::new(observations), profile, context).unwrap() {
        DetectedInput::Utf16 { input, little } => Utf16Reader::new(input, little, context).unwrap(),
        _ => panic!("Expected UTF16 input"),
    }
}

#[test]
fn consumed_merged_prefix_is_freed_before_source_availability_is_demanded() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0x41, 0x42], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), available(Returned::Available(0)), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context); let mut output = [0; 8];
        assert_eq!(value.observations_consumed(), 1); assert!(value.source.input.buffer.is_some());
        assert_eq!(value.read_into(&mut output).unwrap(), Some(2)); assert_eq!(&output[..2], &[0x41, 0x42]);
        assert!(value.source.input.buffer.is_none()); assert_eq!(value.observations_consumed(), 2); value.close().unwrap();
    }); }
}

#[test]
fn remaining_merged_prefix_and_decoder_bytes_short_circuit_underlying_available_calls() {
    for profile in PROFILES { run(|context| {
        let mut units = vec![0xfeff]; units.extend(std::iter::repeat_n(0x41, 4500)); let data = bytes(&units, false);
        let observations = [slice(0, data.len(), Returned::Bytes(&data)), available(Returned::Available(0)), close(Returned::Closed)];
        let mut value = reader(data.len(), &observations, profile, context);
        assert_eq!(value.source.available().unwrap(), 9000); assert_eq!(value.observations_consumed(), 1);
        let mut output = [0; 8192]; assert_eq!(value.read_into(&mut output).unwrap(), Some(4500));
        assert!(output[..4500].iter().all(|unit| *unit == 0x41)); assert_eq!(value.observations_consumed(), 2); value.close().unwrap();
        let observations = [slice(0, data.len(), Returned::Bytes(&data)), close(Returned::Closed)];
        let mut value = reader(data.len(), &observations, profile, context); let mut output = [0; 4];
        assert_eq!(value.read_into(&mut output).unwrap(), Some(4)); assert!(value.ready().unwrap());
        assert_eq!(value.observations_consumed(), 1); value.close().unwrap();
    }); }
}

#[test]
fn available_io_is_suppressed_but_a_wrong_available_observation_is_not() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0x41, 0x42], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), available(Returned::Io("available failed")), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context); let mut output = [0; 8];
        assert_eq!(value.read_into(&mut output).unwrap(), Some(2)); assert_eq!(&output[..2], &[0x41, 0x42]); value.close().unwrap();
        let observations = [slice(0, 16, Returned::Bytes(&data)), available(Returned::Closed), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context);
        assert!(matches!(value.read_into(&mut output), Err(Failure::Observation { index: 1, .. })));
        assert_eq!(&output[..2], &[0x41, 0x42]); value.close().unwrap();
    }); }
}

#[test]
fn no_character_prefix_blocks_without_speculatively_asking_available() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0xd800], false); let low = bytes(&[0xdc00], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), slice(2, 8190, Returned::Bytes(&low)), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context); let mut output = [0; 2];
        assert_eq!(value.read_into(&mut output).unwrap(), Some(2)); assert_eq!(output, [0xd800, 0xdc00]);
        assert_eq!(value.observations_consumed(), 2); value.close().unwrap();
    }); }
}

#[test]
fn an_odd_buffered_byte_is_ready_and_is_compacted_before_the_next_blocking_read() {
    for profile in PROFILES { run(|context| {
        let data = [0xfe, 0xff, 0, 0x41, 0];
        let observations = [slice(0, 16, Returned::Bytes(&data)), available(Returned::Available(0)), slice(1, 8191, Returned::Bytes(&[0x42])), available(Returned::Available(0)), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context); let mut output = [0; 2];
        assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], 0x41);
        assert!(value.ready().unwrap()); assert_eq!(value.observations_consumed(), 2);
        assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], 0x42);
        assert_eq!(value.observations_consumed(), 4); value.close().unwrap();
    }); }
}

#[test]
fn replace_consumes_a_complete_malformed_high_pair_and_each_isolated_low_group() {
    for profile in PROFILES { run(|context| {
        for little in [false, true] {
            let data = bytes(&[0xfeff, 0xd800, 0x78, 0xdc00, 0x7a], little);
            let observations = [slice(0, 16, Returned::Bytes(&data)), available(Returned::Available(0)), close(Returned::Closed)];
            let mut value = reader(16, &observations, profile, context); let mut output = [0; 8];
            assert_eq!(value.read_into(&mut output).unwrap(), Some(3)); assert_eq!(&output[..3], &[0xfffd, 0xfffd, 0x7a]); value.close().unwrap();
        }
    }); }
}

#[test]
fn eof_replaces_each_incomplete_remaining_group_once_and_is_not_a_persistent_latch() {
    for profile in PROFILES { run(|context| {
        for tail in [&[0u8][..], &[0xd8, 0], &[0xd8, 0, 0]] {
            let mut data = bytes(&[0xfeff, 0x41], false); data.extend_from_slice(tail);
            let observations = [slice(0, 16, Returned::Bytes(&data)), available(Returned::Available(1)), slice(tail.len(), 8192 - tail.len(), Returned::Eof), slice(0, 8192, Returned::Eof), close(Returned::Closed)];
            let mut value = reader(16, &observations, profile, context); let mut output = [0; 8];
            assert_eq!(value.read_into(&mut output).unwrap(), Some(2)); assert_eq!(&output[..2], &[0x41, 0xfffd]);
            assert_eq!(value.read_into(&mut output).unwrap(), None); assert_eq!(value.observations_consumed(), 4); value.close().unwrap();
        }
    }); }
}

#[test]
fn valid_surrogate_pairs_remain_buffered_when_only_one_output_slot_is_left() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0x41, 0xd83d, 0xde00, 0x42], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), available(Returned::Available(0)), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context); let mut output = [0; 2];
        assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], 0x41); assert_eq!(value.ptr, 2);
        assert_eq!(value.read_into(&mut output).unwrap(), Some(2)); assert_eq!(output, [0xd83d, 0xde00]);
        assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], 0x42); value.close().unwrap();
    }); }
}

#[test]
fn scalar_read_retains_a_second_unit_and_bulk_leftover_delivery_checks_readiness_after_copy() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0x41, 0x42], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), available(Returned::Available(0)), available(Returned::Io("not ready")), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context);
        assert_eq!(value.read_one().unwrap(), Some(0x41)); assert!(value.ready().unwrap()); assert_eq!(value.observations_consumed(), 1);
        let mut output = [0; 4]; assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], 0x42);
        assert!(!value.ready().unwrap()); assert_eq!(value.observations_consumed(), 3); value.close().unwrap();
    }); }
}

#[test]
fn a_retained_scalar_leftover_can_be_read_after_close_while_bulk_and_ready_check_closed_first() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0x41, 0x42], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context);
        assert_eq!(value.read_one().unwrap(), Some(0x41)); value.close().unwrap();
        assert!(matches!(value.read_into(&mut []), Err(Failure::Io { detail: "Stream closed", .. })));
        assert!(matches!(value.ready(), Err(Failure::Io { detail: "Stream closed", .. })));
        assert_eq!(value.read_one().unwrap(), Some(0x42)); assert!(matches!(value.read_one(), Err(Failure::Io { detail: "Stream closed", .. })));
        value.close().unwrap(); assert_eq!(value.observations_consumed(), 2);
    }); }
}

#[test]
fn read_bytes_flips_the_compacted_tail_even_when_the_source_read_throws_or_returns_zero() {
    for profile in PROFILES { run(|context| {
        let data = [0xfe, 0xff, 0, 0x41, 0];
        let observations = [slice(0, 16, Returned::Bytes(&data)), available(Returned::Available(0)), slice(1, 8191, Returned::Io("read failed")), slice(1, 8191, Returned::Bytes(&[0x42])), available(Returned::Available(0)), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context); let mut output = [0; 2]; value.read_into(&mut output).unwrap();
        assert!(matches!(value.read_into(&mut output), Err(Failure::Io { stage: Stage::Utf16Read, detail: "read failed" })));
        assert_eq!((value.ptr, value.end, value.bytes.values[0]), (0, 1, 0));
        assert_eq!(value.read_into(&mut output).unwrap(), Some(1)); assert_eq!(output[0], 0x42); value.close().unwrap();
        let data = bytes(&[0xfeff, 0xd800], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), slice(2, 8190, Returned::Zero), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context);
        assert!(matches!(value.read_into(&mut output), Err(Failure::ZeroRead(Stage::Utf16Read))));
        assert_eq!((value.ptr, value.end), (0, 2)); assert_eq!(value.observations_consumed(), 2); value.close().unwrap();
    }); }
}

#[test]
fn available_controls_whether_a_later_read_failure_precedes_root_window_publication() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0x7b, 0x7d], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), available(Returned::Available(0)), close(Returned::Closed)];
        let mut window = Utf16Window::new(reader(16, &observations, profile, context), 4000, context).unwrap();
        assert_eq!(window.next().unwrap(), Some(0x7b)); assert_eq!(window.next().unwrap(), Some(0x7d));
        assert_eq!(window.observations_consumed(), 2); window.close().unwrap();
        let observations = [slice(0, 16, Returned::Bytes(&data)), available(Returned::Available(1)), slice(0, 8192, Returned::Io("later read failed")), close(Returned::Closed)];
        let mut window = Utf16Window::new(reader(16, &observations, profile, context), 4000, context).unwrap();
        assert!(matches!(window.next(), Err(Failure::Io { stage: Stage::Utf16Read, .. })));
        assert_eq!(window.position(), 0); assert_eq!(&window.buffer.as_ref().unwrap().values[..2], &[0x7b, 0x7d]); window.close().unwrap();
    }); }
}

#[test]
fn close_failure_closes_the_decoder_even_when_the_parser_still_has_an_attached_reader() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0x41], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), available(Returned::Available(0)), slice(0, 8192, Returned::Eof), close(Returned::Io("close failed"))];
        let mut window = Utf16Window::new(reader(16, &observations, profile, context), 4000, context).unwrap();
        assert_eq!(window.next().unwrap(), Some(0x41));
        assert!(matches!(window.next(), Err(Failure::Io { stage: Stage::SourceClose, .. })));
        assert!(window.reader.closed && window.attached && !window.closed);
        assert!(matches!(window.next(), Err(Failure::Io { stage: Stage::Utf16Read, detail: "Stream closed" })));
        assert_eq!(window.observations_consumed(), 4); window.close().unwrap(); assert_eq!(window.next().unwrap(), None);
    }); }
}

#[test]
fn fixed_endian_decoders_preserve_later_bom_units_and_negative_availability_is_not_ready() {
    for profile in PROFILES { run(|context| {
        for little in [false, true] {
            let data = bytes(&[0xfeff, 0x41, 0xfeff, 0x42], little);
            let observations = [slice(0, 16, Returned::Bytes(&data)), available(Returned::Available(-1)), close(Returned::Closed)];
            let mut value = reader(16, &observations, profile, context); let mut output = [0; 8];
            assert_eq!(value.read_into(&mut output).unwrap(), Some(3)); assert_eq!(&output[..3], &[0x41, 0xfeff, 0x42]); value.close().unwrap();
        }
    }); }
}

#[test]
fn decoder_and_parser_storage_are_admitted_before_demand_and_have_distinct_close_lifetimes() {
    let byte_charge = 16 + 256; let decoder_charge = 8192 + 256; let char_charge = 8000 + 256;
    for limit in [byte_charge, byte_charge + decoder_charge, byte_charge + decoder_charge + char_charge] {
        let budget = MemoryBudget::new(limit); let mut context = ConversionContext::new(Arc::clone(&budget));
        let data = bytes(&[0xfeff, 0x41], false); let observations = [slice(0, 16, Returned::Bytes(&data)), close(Returned::Closed)];
        let DetectedInput::Utf16 { input, little } = bootstrap(16, ReadTape::new(&observations), Profile::Jackson216, &mut context).unwrap() else { panic!("Expected UTF16") };
        let result = Utf16Reader::new(input, little, &mut context);
        if limit == byte_charge { assert!(matches!(result, Err(Failure::Native(_)))); drop(context); assert_eq!(budget.used_bytes(), 0); continue; }
        let result = Utf16Window::new(result.unwrap(), 4000, &mut context);
        if limit < byte_charge + decoder_charge + char_charge { assert!(matches!(result, Err(Failure::Native(_)))); drop(context); assert_eq!(budget.used_bytes(), 0); continue; }
        let mut window = result.unwrap(); assert_eq!(window.observations_consumed(), 1); assert_eq!(budget.used_bytes(), limit);
        window.close().unwrap(); assert!(window.buffer.is_none() && window.reader.closed);
        drop(context); assert_eq!(budget.used_bytes(), decoder_charge);
        drop(window); assert_eq!(budget.used_bytes(), 0);
    }
}

#[test]
fn zero_leftovers_and_nested_single_slot_reads_keep_their_source_generation() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0x41, 0], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context);
        assert_eq!(value.read_one().unwrap(), Some(0x41)); assert!(value.ready().unwrap()); value.close().unwrap();
        assert_eq!(value.read_one().unwrap(), Some(0)); assert_eq!(value.observations_consumed(), 2);
        let data = bytes(&[0xfeff, 0x41, 0x42, 0x43, 0x44], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), close(Returned::Closed)];
        let mut value = reader(16, &observations, profile, context);
        assert_eq!(value.read_one().unwrap(), Some(0x41)); let mut output = [0; 2];
        assert_eq!(value.read_into(&mut output).unwrap(), Some(2)); assert_eq!(output, [0x42, 0x43]);
        assert_eq!(value.read_into(&mut []).unwrap(), Some(0)); assert_eq!(value.observations_consumed(), 1);
        value.close().unwrap(); assert_eq!(value.read_one().unwrap(), Some(0x44));
    }); }
}

#[test]
fn a_failed_close_releases_the_merged_prefix_but_retains_the_closed_decoder_payload() {
    for profile in PROFILES { run(|context| {
        let data = bytes(&[0xfeff, 0x41], false);
        let observations = [slice(0, 16, Returned::Bytes(&data)), close(Returned::Io("close failed"))];
        let mut value = reader(16, &observations, profile, context);
        assert!(matches!(value.close(), Err(Failure::Io { stage: Stage::SourceClose, .. })));
        assert!(value.closed && value.source.input.buffer.is_none()); assert_eq!(value.bytes.values.len(), 8192);
        assert!(matches!(value.read_into(&mut [0; 2]), Err(Failure::Io { detail: "Stream closed", .. })));
        value.close().unwrap(); assert_eq!(value.observations_consumed(), 2);
    }); }
}
