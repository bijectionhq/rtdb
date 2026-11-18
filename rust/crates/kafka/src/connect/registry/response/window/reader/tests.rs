use super::*;
use rtdb_types::{Anomaly, MemoryBudget};
use crate::connect::registry::reader_symbols::{FactoryProfile, Profile, SymbolName};
use super::super::{Failure, FailureKind};
use input_window::{Call, DetectedInput, Observation, ReadMethod, ReadRequest, ReadTape, Returned};

const PROFILES: [Profile; 2] = [Profile::Jackson216, Profile::Jackson221];
const BYTE_CAPACITY: usize = 1024;
const CHAR_CAPACITY: usize = 64;

#[derive(Default)]
struct Intern { names: Vec<Vec<u16>>, denied: bool }
impl InternAuthority for Intern {
    fn intern(&mut self, candidate: Arc<SymbolName>, _: &mut ConversionContext) -> rtdb_types::Result<Arc<SymbolName>> {
        self.names.push(candidate.text().view().units().collect());
        if self.denied { return Err(Anomaly::incorrect("fixture/intern", "Intern demand denied")); }
        Ok(candidate)
    }
}
fn source_profile(profile: Profile) -> input_window::Profile {
    match profile { Profile::Jackson216 => input_window::Profile::Jackson216, Profile::Jackson221 => input_window::Profile::Jackson221 }
}
fn slice(returned: Returned<'_>) -> Observation<'_> {
    Observation { call: Call::Read(ReadRequest { method: ReadMethod::Slice, offset: 0, length: BYTE_CAPACITY }), returned }
}
fn whole(returned: Returned<'_>) -> Observation<'_> {
    Observation { call: Call::Read(ReadRequest { method: ReadMethod::WholeBuffer, offset: 0, length: BYTE_CAPACITY }), returned }
}
fn close(returned: Returned<'_>) -> Observation<'_> { Observation { call: Call::Close, returned } }
fn wire(text: &str, utf32: bool) -> Vec<u8> {
    let padded = format!("{text}{}", " ".repeat(CHAR_CAPACITY));
    if utf32 { [0, 0, 0xfe, 0xff].into_iter().chain(padded.chars().flat_map(|ch| u32::from(ch).to_be_bytes())).collect() }
    else { [0xfe, 0xff].into_iter().chain(padded.encode_utf16().flat_map(u16::to_be_bytes)).collect() }
}
fn reader<'a>(tape: &'a [Observation<'a>], profile: Profile, context: &mut ConversionContext) -> ReaderInput<'a> {
    match input_window::bootstrap(BYTE_CAPACITY, ReadTape::new(tape), source_profile(profile), context).unwrap() {
        DetectedInput::Utf16 { input, little } => ReaderInput::Utf16(input_window::Utf16Reader::new(input, little, context).unwrap()),
        DetectedInput::Utf32(reader) => ReaderInput::Utf32(reader),
        _ => panic!("expected an actual detected Reader branch"),
    }
}
fn text(value: &Option<rtdb_types::java_text::JavaString>, expected: &str) { assert!(value.as_ref().unwrap().view().units().eq(expected.encode_utf16())); }
fn native<T>(result: Result<T>, code: &str) {
    match result { Err(Failure::Native(cause)) => assert!(cause.code.ends_with(code), "{cause}"), _ => panic!("expected native {code}") }
}
fn decode_failure<T>(result: Result<T>, expected: FailureKind) {
    match result { Err(Failure::Decode { kind, .. }) => assert_eq!(kind, expected), _ => panic!("expected decode {expected:?}") }
}
fn run(mut inspect: impl FnMut(Profile, &mut ConversionContext)) {
    for profile in PROFILES {
        let memory = MemoryBudget::new(1 << 27);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        inspect(profile, &mut context);
        drop(context);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn actual_reader_tables_decode_both_reader_encodings_without_accepted_name_observations() {
    run(|profile, context| {
        let root = ReaderSymbols::new(FactoryProfile { profile, identity_hash_seed: 7 }, context).unwrap();
        assert_eq!(root.factory().profile, profile);
        let mut intern = Intern::default();
        for utf32 in [false, true] {
            let bytes = wire(r#"{"schema":"first","schema":"last","schemaType":null,"references":[null]}"#, utf32);
            let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Closed)];
            let result = schema_string_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut intern, context);
            assert_eq!(result.observations_consumed, 2);
            assert!(result.suppressed_close.is_none());
            let bean = result.result.unwrap().unwrap();
            text(&bean.schema, "last");
            assert!(bean.schema_type.is_none());
            assert!(bean.references.unwrap().as_slice()[0].is_none());
        }
        assert_eq!(intern.names, ["schema", "schemaType", "references"].map(|text| text.encode_utf16().collect::<Vec<_>>()));
    });
}

#[test]
fn reader_names_and_values_preserve_unmatched_escaped_utf16_units() {
    run(|profile, context| {
        let root = ReaderSymbols::new(FactoryProfile { profile, identity_hash_seed: 7 }, context).unwrap();
        let mut intern = Intern::default();
        for utf32 in [false, true] {
            let bytes = wire(r#"{"metadata":{"properties":{"\uD800":"\uDFFF"}},"schema":"\uD800"}"#, utf32);
            let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Closed)];
            let bean = schema_string_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut intern, context).result.unwrap().unwrap();
            assert_eq!(bean.schema.unwrap().view().units().collect::<Vec<_>>(), [0xd800]);
            let metadata = bean.metadata.unwrap();
            let properties = metadata.properties.unwrap();
            let property = &properties.values.as_slice()[0];
            assert_eq!(property.0.view().units().collect::<Vec<_>>(), [0xd800]);
            assert_eq!(property.1.as_ref().unwrap().view().units().collect::<Vec<_>>(), [0xdfff]);
        }
    });
}

#[test]
fn interning_is_demanded_before_colon_validation_and_a_syntax_failure_can_publish_accepted_names() {
    run(|profile, context| {
        let root = ReaderSymbols::new(FactoryProfile { profile, identity_hash_seed: 7 }, context).unwrap();
        for utf32 in [false, true] {
            let bytes = wire(r#"{"schema" true}"#, utf32);
            let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Closed)];
            let mut denied = Intern { denied: true, ..Intern::default() };
            if utf32 {
                let fresh = ReaderSymbols::new(FactoryProfile { profile, identity_hash_seed: 7 }, context).unwrap();
                native(schema_string_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &fresh, &mut denied, context).result, "fixture/intern");
            } else {
                native(schema_string_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut denied, context).result, "fixture/intern");
            }
            assert_eq!(denied.names, ["schema".encode_utf16().collect::<Vec<_>>()]);
        }
        let bytes = wire(r#"{"schema" true}"#, true);
        let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Closed)];
        let mut accepted = Intern::default();
        decode_failure(schema_string_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut accepted, context).result, FailureKind::Syntax);
        assert_eq!(accepted.names.len(), 1);
        let bytes = wire(r#"{"schema":"ok"}"#, false);
        let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Closed)];
        text(&schema_string_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut accepted, context).result.unwrap().unwrap().schema, "ok");
        assert_eq!(accepted.names.len(), 1);
    });
}

#[test]
fn reader_schema_creator_keeps_first_invalid_duplicate_demand_and_nullable_state() {
    run(|profile, context| {
        let root = ReaderSymbols::new(FactoryProfile { profile, identity_hash_seed: 7 }, context).unwrap();
        let mut intern = Intern::default();
        let bytes = wire(r#"{"id":"bad","id":1}"#, true);
        let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Closed)];
        decode_failure(schema_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut intern, context).result, FailureKind::Coercion);
        assert_eq!(intern.names, ["id".encode_utf16().collect::<Vec<_>>()]);
        let bytes = wire(r#"{"id":1,"schemaType":null,"schema":null}"#, false);
        let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Closed)];
        let bean = schema_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut intern, context).result.unwrap().unwrap();
        assert_eq!(bean.id, Some(1));
        text(&bean.schema_type, "AVRO");
        assert!(bean.schema.is_none());
        assert_eq!(intern.names.len(), 3);
    });
}

#[test]
fn a_source_close_failure_keeps_accepted_symbol_publication_without_publishing_the_bean() {
    run(|profile, context| {
        let root = ReaderSymbols::new(FactoryProfile { profile, identity_hash_seed: 7 }, context).unwrap();
        let mut intern = Intern::default();
        let bytes = wire(r#"{"schema":"ok"}"#, true);
        let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Io("close source"))];
        let result = schema_string_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut intern, context);
        assert_eq!(result.observations_consumed, 2);
        assert!(result.suppressed_close.is_none());
        native(result.result, "/registry-input-io");
        assert_eq!(intern.names.len(), 1);
        let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Closed)];
        text(&schema_string_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut intern, context).result.unwrap().unwrap().schema, "ok");
        assert_eq!(intern.names.len(), 1);
    });
}

#[test]
fn native_intern_failure_keeps_a_later_source_close_failure_suppressed_and_does_not_publish_names() {
    run(|profile, context| {
        let root = ReaderSymbols::new(FactoryProfile { profile, identity_hash_seed: 7 }, context).unwrap();
        let bytes = wire(r#"{"schema":"ok"}"#, true);
        let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Io("close source"))];
        let mut denied = Intern { denied: true, ..Intern::default() };
        let result = schema_string_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut denied, context);
        native(result.result, "fixture/intern");
        match result.suppressed_close { Some(Failure::Native(cause)) => assert!(cause.code.ends_with("/registry-input-io")), _ => panic!("expected suppressed source close") }
        let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Closed)];
        let mut accepted = Intern::default();
        schema_string_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut accepted, context).result.unwrap();
        assert_eq!(accepted.names.len(), 1);
    });
}

#[test]
fn source_read_failure_releases_the_accepted_prefix_and_closes_once() {
    run(|profile, context| {
        let root = ReaderSymbols::new(FactoryProfile { profile, identity_hash_seed: 7 }, context).unwrap();
        let mut bytes = vec![0, 0, 0xfe, 0xff];
        bytes.extend(r#"{"schema":""#.chars().flat_map(|ch| u32::from(ch).to_be_bytes()));
        let tape = [slice(Returned::Bytes(&bytes)), whole(Returned::Io("value read")), close(Returned::Closed)];
        let mut intern = Intern::default();
        let result = schema_string_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut intern, context);
        assert_eq!(result.observations_consumed, 3);
        native(result.result, "/registry-input-io");
        assert!(result.suppressed_close.is_none());
        let bytes = wire(r#"{"schema":"ok"}"#, true);
        let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Closed)];
        text(&schema_string_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut intern, context).result.unwrap().unwrap().schema, "ok");
        assert_eq!(intern.names.len(), 1);
    });
}

#[test]
fn utf32_profile_mismatch_is_rejected_before_its_first_character_but_still_closes_the_parser() {
    let memory = MemoryBudget::new(1 << 26);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    let root = ReaderSymbols::new(FactoryProfile { profile: Profile::Jackson221, identity_hash_seed: 7 }, &mut context).unwrap();
    let bytes = wire(r#"{"schema":"ok"}"#, true);
    let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Closed)];
    let input = reader(&tape, Profile::Jackson216, &mut context);
    let mut intern = Intern::default();
    let result = schema_string_from_reader(input, CHAR_CAPACITY, &root, &mut intern, &mut context);
    assert_eq!(result.observations_consumed, 2);
    match result.result { Err(Failure::Native(cause)) => assert_eq!(cause.category, rtdb_types::AnomalyCategory::Unsupported), _ => panic!("expected unsupported source profile") }
    assert!(intern.names.is_empty());
    drop(root); drop(context);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn child_or_parser_buffer_preparation_denial_does_not_invent_a_parser_close_call() {
    for profile in PROFILES { for child_allowed in [false, true] {
        let memory = MemoryBudget::new(1 << 26);
        let mut factory_context = ConversionContext::new(Arc::clone(&memory));
        let root = ReaderSymbols::new(FactoryProfile { profile, identity_hash_seed: 7 }, &mut factory_context).unwrap();
        drop(factory_context);
        let root_bytes = memory.used_bytes();
        let limit = BYTE_CAPACITY + 256 + if child_allowed { root_bytes } else { 0 };
        let mut context = ConversionContext::with_limit(Arc::clone(&memory), limit);
        let bytes = wire(r#"{"schema":"ok"}"#, true);
        let tape = [slice(Returned::Bytes(&bytes))];
        let input = reader(&tape, profile, &mut context);
        let mut intern = Intern::default();
        let result = schema_string_from_reader(input, CHAR_CAPACITY, &root, &mut intern, &mut context);
        assert_eq!(result.observations_consumed, 1);
        assert!(result.suppressed_close.is_none());
        native(result.result, "/batch-too-large");
        assert!(intern.names.is_empty());
        drop(context); drop(root);
        assert_eq!(memory.used_bytes(), 0);
    } }
}

#[test]
fn decoded_text_and_persistent_symbols_keep_their_original_reservations_after_context_drop() {
    let memory = MemoryBudget::new(1 << 26);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    let profile = Profile::Jackson216;
    let root = ReaderSymbols::new(FactoryProfile { profile, identity_hash_seed: 7 }, &mut context).unwrap();
    let bytes = wire(r#"{"schema":"owned"}"#, true);
    let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Closed)];
    let mut intern = Intern::default();
    let bean = schema_string_from_reader(reader(&tape, profile, &mut context), CHAR_CAPACITY, &root, &mut intern, &mut context).result.unwrap().unwrap();
    drop(context);
    assert!(memory.used_bytes() > 0);
    drop(root);
    assert_eq!(memory.used_bytes(), bean.schema.as_ref().unwrap().retained_bytes() + bean.schema_type.as_ref().unwrap().retained_bytes());
    text(&bean.schema, "owned");
    drop(bean);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn symbol_collision_constraints_and_later_source_consistency_failures_keep_distinct_decode_kinds() {
    run(|profile, context| {
        let root = ReaderSymbols::new(FactoryProfile { profile, identity_hash_seed: 7 }, context).unwrap();
        let mut prefix = String::from("{");
        for index in 0..303 {
            if index > 0 { prefix.push(','); }
            let name: String = (0..9).map(|bit| if index & (1 << bit) == 0 { "Aa" } else { "B@" }).collect();
            prefix.push('"'); prefix.push_str(&name); prefix.push('"');
            if index != 302 { prefix.push_str(":null"); }
        }
        let bytes: Vec<_> = [0, 0, 0xfe, 0xff].into_iter().chain(prefix.chars().flat_map(|ch| u32::from(ch).to_be_bytes())).collect();
        let mut tape: Vec<_> = bytes.chunks(BYTE_CAPACITY).enumerate().map(|(index, bytes)| if index == 0 { slice(Returned::Bytes(bytes)) } else { whole(Returned::Bytes(bytes)) }).collect();
        tape.push(close(Returned::Closed));
        let mut intern = Intern::default();
        let result = schema_string_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut intern, context);
        assert_eq!(result.observations_consumed, tape.len());
        decode_failure(result.result, FailureKind::Constraint);
        assert_eq!(intern.names.len(), 303);

        let mut prefix = String::from("{");
        let mut names = 0;
        for index in 0..42 {
            if index == 39 { continue; }
            if names > 0 { prefix.push(','); }
            prefix.push_str(&format!("\"other{index}\""));
            if names != 40 { prefix.push_str(":null"); }
            names += 1;
        }
        assert_eq!(names, 41);
        let bytes: Vec<_> = [0, 0, 0xfe, 0xff].into_iter().chain(prefix.chars().flat_map(|ch| u32::from(ch).to_be_bytes())).collect();
        let mut tape: Vec<_> = bytes.chunks(BYTE_CAPACITY).enumerate().map(|(index, bytes)| if index == 0 { slice(Returned::Bytes(bytes)) } else { whole(Returned::Bytes(bytes)) }).collect();
        tape.push(close(Returned::Closed));
        let result = schema_string_from_reader(reader(&tape, profile, context), CHAR_CAPACITY, &root, &mut intern, context);
        assert_eq!(result.observations_consumed, tape.len());
        decode_failure(result.result, FailureKind::Unchecked);
        assert_eq!(intern.names.len(), 343);
    });
}

struct PublishingIntern { root: Arc<ReaderSymbols>, memory: Arc<MemoryBudget>, calls: usize }
impl InternAuthority for PublishingIntern {
    fn intern(&mut self, candidate: Arc<SymbolName>, context: &mut ConversionContext) -> rtdb_types::Result<Arc<SymbolName>> {
        self.calls += 1;
        if candidate.text().view().units().eq("second".encode_utf16()) {
            let mut other_context = ConversionContext::new(Arc::clone(&self.memory));
            let mut child = self.root.child(&mut other_context).unwrap();
            child.find(&"external".encode_utf16().collect::<Vec<_>>(), &mut Intern::default(), &mut other_context).unwrap();
            child.release(&mut other_context).unwrap();
            drop(other_context);
            context.reserve(context.allocation_remaining())?;
        }
        Ok(candidate)
    }
}

#[test]
fn symbol_release_finally_failure_replaces_an_earlier_source_close_failure() {
    for profile in PROFILES {
        let memory = MemoryBudget::new(1 << 27);
        let mut context = ConversionContext::with_limit(Arc::clone(&memory), 1 << 16);
        let root = ReaderSymbols::new(FactoryProfile { profile, identity_hash_seed: 7 }, &mut context).unwrap();
        let bytes = wire(r#"{"first":true,"second":false}"#, true);
        let tape = [slice(Returned::Bytes(&bytes)), close(Returned::Io("close source"))];
        let input = reader(&tape, profile, &mut context);
        let mut intern = PublishingIntern { root: Arc::clone(&root), memory: Arc::clone(&memory), calls: 0 };
        let result = schema_string_from_reader(input, CHAR_CAPACITY, &root, &mut intern, &mut context);
        assert_eq!(intern.calls, 2);
        assert_eq!(result.observations_consumed, 2);
        native(result.result, "/batch-too-large");
        assert!(result.suppressed_close.is_none());
        drop(intern); drop(root); drop(context);
        assert_eq!(memory.used_bytes(), 0);
    }
}
