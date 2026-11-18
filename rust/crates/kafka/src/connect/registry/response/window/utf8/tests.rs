use super::*;
use rtdb_types::{Anomaly, MemoryBudget, java_text::JavaString};
use super::super::{Failure, FailureKind, Profile};
use super::super::super::symbols::{SymbolName, bytes::{self, FactoryProfile}};
use input_window::{Call, DetectedInput, Observation, ReadMethod, ReadRequest, ReadTape, Returned};

const PROFILES: [Profile; 2] = [Profile::Jackson216, Profile::Jackson221];
#[derive(Default)]
struct Intern { names: Vec<Vec<u16>>, denied: bool }
impl InternAuthority for Intern {
    fn intern(&mut self, candidate: Arc<SymbolName>, _: &mut ConversionContext) -> rtdb_types::Result<Arc<SymbolName>> {
        self.names.push(candidate.text().view().units().collect());
        if self.denied { return Err(Anomaly::incorrect("fixture/utf8-intern", "UTF8 interning denied")); }
        Ok(candidate)
    }
}
fn root(profile: Profile, context: &mut ConversionContext) -> Arc<ByteSymbols> { ByteSymbols::new(FactoryProfile { profile, seed: 7 }, context).unwrap() }
fn read(returned: Returned<'_>, capacity: usize) -> Observation<'_> {
    Observation { call: Call::Read(ReadRequest { method: ReadMethod::Slice, offset: 0, length: capacity }), returned }
}
fn observations<'a>(bytes: &'a [u8], capacity: usize, ending: Returned<'a>) -> Vec<Observation<'a>> {
    let mut tape: Vec<_> = bytes.chunks(capacity).map(|bytes| read(Returned::Bytes(bytes), capacity)).collect();
    tape.push(Observation { call: Call::Close, returned: ending }); tape
}
fn input<'a>(tape: &'a [Observation<'a>], capacity: usize, profile: Profile, context: &mut ConversionContext) -> input_window::Utf8Window<'a> {
    let profile = match profile { Profile::Jackson216 => input_window::Profile::Jackson216, Profile::Jackson221 => input_window::Profile::Jackson221 };
    match input_window::bootstrap(capacity, ReadTape::new(tape), profile, context).unwrap() { DetectedInput::Utf8(window) => window, _ => panic!("expected actual UTF8 detection") }
}
fn text(value: &Option<JavaString>, expected: &str) { assert!(value.as_ref().unwrap().view().units().eq(expected.encode_utf16())); }
fn native<T>(result: Result<T>, code: &str) {
    match result { Err(Failure::Native(cause)) => assert!(cause.code.ends_with(code), "{cause}"), _ => panic!("expected native {code}") }
}
fn kind<T>(result: Result<T>, expected: FailureKind) {
    match result { Err(Failure::Decode { kind, .. }) => assert_eq!(kind, expected), _ => panic!("expected decode {expected:?}") }
}
fn seed(root: &Arc<ByteSymbols>, quads: &[u32], text: &str, context: &mut ConversionContext) {
    let mut child = root.child(context).unwrap();
    let candidate = bytes::prepare_name(&text.encode_utf16().collect::<Vec<_>>(), context).unwrap();
    child.add(bytes::Quads::new(quads).unwrap(), candidate, &mut Intern::default(), context).unwrap();
    child.release(context).unwrap();
}
fn run(mut inspect: impl FnMut(Profile, &mut ConversionContext)) {
    for profile in PROFILES {
        let memory = MemoryBudget::new(1 << 29);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        inspect(profile, &mut context);
        drop(context); assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn actual_utf8_tables_decode_duplicate_fields_and_null_entries_without_name_tapes() {
    run(|profile, context| {
        let root = root(profile, context);
        let mut intern = Intern::default();
        let body = br#"{"schema":"first","schema":"last","schemaType":null,"references":[null]}"#;
        for capacity in [4, 16, 64] {
            let tape = observations(body, capacity, Returned::Closed);
            let result = schema_string_from_utf8(input(&tape, capacity, profile, context), &root, &mut intern, &[], context);
            assert_eq!(result.observations_consumed, tape.len()); assert!(result.suppressed_close.is_none());
            let bean = result.result.unwrap().unwrap();
            text(&bean.schema, "last"); assert!(bean.schema_type.is_none());
            assert!(bean.references.unwrap().as_slice()[0].is_none());
        }
        assert_eq!(intern.names, ["schema", "schemaType", "references"].map(|name| name.encode_utf16().collect::<Vec<_>>()));
    });
}

#[test]
fn utf8_creator_retains_first_invalid_duplicate_and_publishes_accepted_names() {
    run(|profile, context| {
        let root = root(profile, context);
        let mut intern = Intern::default();
        let first = observations(br#"{"id":"bad","id":1}"#, 64, Returned::Closed);
        kind(schema_from_utf8(input(&first, 64, profile, context), &root, &mut intern, &[], context).result, FailureKind::Coercion);
        assert_eq!(intern.names, ["id".encode_utf16().collect::<Vec<_>>()]);
        let second = observations(br#"{"id":1,"schemaType":null,"schema":null}"#, 64, Returned::Closed);
        let bean = schema_from_utf8(input(&second, 64, profile, context), &root, &mut intern, &[], context).result.unwrap().unwrap();
        assert_eq!(bean.id, Some(1)); text(&bean.schema_type, "AVRO"); assert!(bean.schema.is_none());
        assert_eq!(intern.names.len(), 3);
    });
}

#[test]
fn raw_and_escaped_ascii_names_share_the_same_quad_lookup() {
    run(|profile, context| {
        let root = root(profile, context); let mut intern = Intern::default();
        let bytes = br#"{"\u0073chema":"first","schema":"last"}"#;
        for capacity in [5, 64] {
            let tape = observations(bytes, capacity, Returned::Closed);
            text(&schema_string_from_utf8(input(&tape, capacity, profile, context), &root, &mut intern, &[], context).result.unwrap().unwrap().schema, "last");
        }
        assert_eq!(intern.names, ["schema".encode_utf16().collect::<Vec<_>>()]);
    });
}

#[test]
fn supplementary_escape_packing_preserves_the_two_profile_cache_difference() {
    run(|profile, context| {
        let root = root(profile, context); let mut intern = Intern::default();
        let body = "{\"😀\":null,\"\\uD83D\\uDE00\":null,\"schema\":\"ok\"}";
        let tape = observations(body.as_bytes(), 5, Returned::Closed);
        text(&schema_string_from_utf8(input(&tape, 5, profile, context), &root, &mut intern, &[], context).result.unwrap().unwrap().schema, "ok");
        assert_eq!(intern.names[0], [0xd83d, 0xde00]);
        assert_eq!(intern.names.len(), if profile == Profile::Jackson216 { 3 } else { 2 });
        if profile == Profile::Jackson216 { assert_eq!(intern.names[1], [0xd83d, 0xde00]); }
    });
}

#[test]
fn escaped_surrogate_validation_precedes_lookup_while_raw_surrogate_hits_skip_miss_decoding() {
    run(|profile, context| {
        let root = root(profile, context); seed(&root, &[0xffed_a080], "schema", context);
        let mut intern = Intern::default();
        let escaped = observations(br#"{"\uD800":"value"}"#, 64, Returned::Closed);
        let result = schema_string_from_utf8(input(&escaped, 64, profile, context), &root, &mut intern, &[], context);
        if profile == Profile::Jackson216 { text(&result.result.unwrap().unwrap().schema, "value"); }
        else { kind(result.result, FailureKind::Syntax); }
        let raw = observations(b"{\"\xed\xa0\x80\":\"value\"}", 64, Returned::Closed);
        text(&schema_string_from_utf8(input(&raw, 64, profile, context), &root, &mut intern, &[], context).result.unwrap().unwrap().schema, "value");
        assert!(intern.names.is_empty());
    });
}

#[test]
fn an_actual_malformed_byte_cache_hit_precedes_the_miss_decoder() {
    run(|profile, context| {
        let cached = root(profile, context); seed(&cached, &[u32::MAX], "schema", context);
        let bytes = b"{\"\xff\":\"ok\"}";
        let mut intern = Intern::default();
        let hit = observations(bytes, 64, Returned::Closed);
        text(&schema_string_from_utf8(input(&hit, 64, profile, context), &cached, &mut intern, &[], context).result.unwrap().unwrap().schema, "ok");
        let fresh = root(profile, context);
        let miss = observations(bytes, 64, Returned::Closed);
        kind(schema_string_from_utf8(input(&miss, 64, profile, context), &fresh, &mut intern, &[], context).result, FailureKind::Syntax);
        assert!(intern.names.is_empty());
    });
}

#[test]
fn completed_name_lookup_precedes_miss_length_and_utf8_validation() {
    run(|profile, context| {
        let mut body = b"{\"".to_vec(); body.extend(std::iter::repeat_n(0xff, 50_001)); body.extend_from_slice(b"\":\"ok\"}");
        let quads = vec![u32::MAX; 12_501];
        let cached = root(profile, context); seed(&cached, &quads, "schema", context);
        let mut intern = Intern::default();
        let hit = observations(&body, 1024, Returned::Closed);
        text(&schema_string_from_utf8(input(&hit, 1024, profile, context), &cached, &mut intern, &[], context).result.unwrap().unwrap().schema, "ok");
        let fresh = root(profile, context);
        let miss = observations(&body, 1024, Returned::Closed);
        match schema_string_from_utf8(input(&miss, 1024, profile, context), &fresh, &mut intern, &[], context).result {
            Err(Failure::Decode { kind, detail, .. }) => { assert_eq!(kind, FailureKind::Constraint); assert_eq!(detail, "JSON field name length exceeds 50000"); }
            _ => panic!("expected miss-only name length failure"),
        }
        assert!(intern.names.is_empty());
    });
}

#[test]
fn quad_growth_guard_follows_the_final_quote_before_any_colon_or_miss_decode() {
    run(|profile, context| {
        let root = root(profile, context); let mut intern = Intern::default();
        let mut body = b"{\"".to_vec(); body.extend(std::iter::repeat_n(b'a', 65_537)); body.push(b'"');
        let tape = observations(&body, 1024, Returned::Closed);
        let result = schema_string_from_utf8(input(&tape, 1024, profile, context), &root, &mut intern, &[], context);
        assert_eq!(result.observations_consumed, tape.len());
        match result.result {
            Err(Failure::Decode { kind, detail, .. }) => { assert_eq!(kind, FailureKind::Constraint); assert_eq!(detail, "JSON field name decode buffer exceeds allowed length"); }
            _ => panic!("expected quad-array growth constraint"),
        }
        assert!(intern.names.is_empty());
    });
}

#[test]
fn raw_control_check_cannot_be_bypassed_by_a_cached_escaped_name() {
    run(|profile, context| {
        let root = root(profile, context); seed(&root, &[0xffff_ff00], "schema", context);
        let mut intern = Intern::default();
        let raw = observations(b"{\"\0xxxx", 64, Returned::Closed);
        kind(schema_string_from_utf8(input(&raw, 64, profile, context), &root, &mut intern, &[], context).result, FailureKind::Syntax);
        let escaped = observations(br#"{"\u0000":"ok"}"#, 64, Returned::Closed);
        text(&schema_string_from_utf8(input(&escaped, 64, profile, context), &root, &mut intern, &[], context).result.unwrap().unwrap().schema, "ok");
        assert!(intern.names.is_empty());
    });
}

#[test]
fn four_byte_miss_decoder_uses_addition_and_preserves_source_out_of_range_units() {
    run(|profile, context| {
        let root = root(profile, context); let mut intern = Intern::default();
        let body = b"{\"\xf0\x80\x80\x80\":null,\"\xf7\xbf\xbf\xbf\":null}";
        let tape = observations(body, 5, Returned::Closed);
        schema_string_from_utf8(input(&tape, 5, profile, context), &root, &mut intern, &[], context).result.unwrap();
        assert_eq!(intern.names, [vec![0xd7c0, 0xdc00], vec![0xdfbf, 0xdfff]]);
    });
}

#[test]
fn source_refill_failure_precedes_lookup_and_interning_demand() {
    run(|profile, context| {
        let root = root(profile, context); let mut intern = Intern::default();
        let tape = [read(Returned::Bytes(b"{\"sc"), 8), read(Returned::Io("name read"), 8), Observation { call: Call::Close, returned: Returned::Closed }];
        let result = schema_string_from_utf8(input(&tape, 8, profile, context), &root, &mut intern, &[], context);
        assert_eq!(result.observations_consumed, 3); assert!(result.suppressed_close.is_none());
        native(result.result, "/registry-input-io"); assert!(intern.names.is_empty());
    });
}

#[test]
fn interning_failure_precedes_colon_and_keeps_source_close_failure_suppressed() {
    run(|profile, context| {
        let root = root(profile, context);
        let tape = observations(br#"{"schema" true}"#, 64, Returned::Io("close"));
        let mut intern = Intern { denied: true, ..Intern::default() };
        let result = schema_string_from_utf8(input(&tape, 64, profile, context), &root, &mut intern, &[], context);
        native(result.result, "fixture/utf8-intern");
        match result.suppressed_close { Some(Failure::Native(cause)) => assert!(cause.code.ends_with("/registry-input-io")), _ => panic!("expected suppressed source close") }
        assert_eq!(intern.names.len(), 1);
        let tape = observations(br#"{"schema":"ok"}"#, 64, Returned::Closed);
        let mut accepted = Intern::default();
        schema_string_from_utf8(input(&tape, 64, profile, context), &root, &mut accepted, &[], context).result.unwrap();
        assert_eq!(accepted.names.len(), 1);
    });
}

#[test]
fn colon_syntax_and_source_close_failures_still_publish_accepted_byte_names() {
    run(|profile, context| {
        let root = root(profile, context); let mut intern = Intern::default();
        let bad = observations(br#"{"schema" true}"#, 64, Returned::Closed);
        kind(schema_string_from_utf8(input(&bad, 64, profile, context), &root, &mut intern, &[], context).result, FailureKind::Syntax);
        let closing = observations(br#"{"schema":"ok"}"#, 64, Returned::Io("close"));
        native(schema_string_from_utf8(input(&closing, 64, profile, context), &root, &mut intern, &[], context).result, "/registry-input-io");
        let good = observations(br#"{"schema":"ok"}"#, 64, Returned::Closed);
        text(&schema_string_from_utf8(input(&good, 64, profile, context), &root, &mut intern, &[], context).result.unwrap().unwrap().schema, "ok");
        assert_eq!(intern.names.len(), 1);
    });
}

#[test]
fn child_and_fixed_quad_preparation_denial_do_not_invent_parser_close_calls() {
    for profile in PROFILES { for allow_child in [false, true] {
        let memory = MemoryBudget::new(1 << 24);
        let mut factory_context = ConversionContext::new(Arc::clone(&memory));
        let root = root(profile, &mut factory_context); drop(factory_context);
        let limit = 64 + 256 + if allow_child { memory.used_bytes() } else { 0 };
        let mut context = ConversionContext::with_limit(Arc::clone(&memory), limit);
        let tape = [read(Returned::Bytes(br#"{"schema":"ok"}"#), 64)];
        let mut intern = Intern::default();
        let result = schema_string_from_utf8(input(&tape, 64, profile, &mut context), &root, &mut intern, &[], &mut context);
        assert_eq!(result.observations_consumed, 1); assert!(result.suppressed_close.is_none());
        native(result.result, "/batch-too-large"); assert!(intern.names.is_empty());
        drop(root); drop(context); assert_eq!(memory.used_bytes(), 0);
    } }
}

#[test]
fn returned_text_and_byte_root_hold_original_reservations_after_context_drop() {
    let memory = MemoryBudget::new(1 << 24);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    let profile = Profile::Jackson221;
    let root = root(profile, &mut context); let mut intern = Intern::default();
    let tape = observations(br#"{"schema":"owned"}"#, 64, Returned::Closed);
    let bean = schema_string_from_utf8(input(&tape, 64, profile, &mut context), &root, &mut intern, &[], &mut context).result.unwrap().unwrap();
    drop(context); assert!(memory.used_bytes() > 0); drop(root);
    assert_eq!(memory.used_bytes(), bean.schema.as_ref().unwrap().retained_bytes() + bean.schema_type.as_ref().unwrap().retained_bytes());
    text(&bean.schema, "owned"); drop(bean); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn raw_three_byte_surrogate_misses_retain_the_selected_decoder_profile() {
    run(|profile, context| {
        let root = root(profile, context); let mut intern = Intern::default();
        let tape = observations(b"{\"\xed\xa0\x80\":null}", 64, Returned::Closed);
        let result = schema_string_from_utf8(input(&tape, 64, profile, context), &root, &mut intern, &[], context);
        if profile == Profile::Jackson216 {
            result.result.unwrap(); assert_eq!(intern.names, [vec![0xd800]]);
        } else {
            kind(result.result, FailureKind::Syntax); assert!(intern.names.is_empty());
        }
    });
}

#[test]
fn bom_and_split_escape_refills_need_no_external_name_positions() {
    run(|profile, context| {
        let root = root(profile, context); let mut intern = Intern::default();
        let body = [b"\xef\xbb\xbf".as_slice(), br#"{"\u0073chema":"ok"}"#].concat();
        let tape = observations(&body, 4, Returned::Closed);
        let result = schema_string_from_utf8(input(&tape, 4, profile, context), &root, &mut intern, &[], context);
        assert_eq!(result.observations_consumed, tape.len());
        text(&result.result.unwrap().unwrap().schema, "ok");
        assert_eq!(intern.names, ["schema".encode_utf16().collect::<Vec<_>>()]);
    });
}

#[test]
fn miss_continuation_failure_preserves_earlier_accepted_names_for_close() {
    run(|profile, context| {
        let root = root(profile, context); let mut intern = Intern::default();
        let broken = observations(b"{\"schema\":\"first\",\"\xc2A\":null}", 64, Returned::Closed);
        kind(schema_string_from_utf8(input(&broken, 64, profile, context), &root, &mut intern, &[], context).result, FailureKind::Syntax);
        assert_eq!(intern.names, ["schema".encode_utf16().collect::<Vec<_>>()]);
        let good = observations(br#"{"schema":"ok"}"#, 64, Returned::Closed);
        text(&schema_string_from_utf8(input(&good, 64, profile, context), &root, &mut intern, &[], context).result.unwrap().unwrap().schema, "ok");
        assert_eq!(intern.names.len(), 1);
    });
}

struct PublishingIntern { root: Arc<ByteSymbols>, memory: Arc<MemoryBudget>, calls: usize }
impl InternAuthority for PublishingIntern {
    fn intern(&mut self, candidate: Arc<SymbolName>, context: &mut ConversionContext) -> rtdb_types::Result<Arc<SymbolName>> {
        self.calls += 1;
        if candidate.text().view().units().eq("second".encode_utf16()) {
            let mut other_context = ConversionContext::new(Arc::clone(&self.memory));
            seed(&self.root, &[0x6578_7465, 0x726e_616c], "external", &mut other_context);
            drop(other_context);
            let remaining = context.allocation_remaining(); context.reserve(remaining)?;
        }
        Ok(candidate)
    }
}

#[test]
fn byte_symbol_finally_failure_replaces_a_preceding_input_close_failure() {
    for profile in PROFILES {
        let memory = MemoryBudget::new(1 << 27);
        let mut context = ConversionContext::with_limit(Arc::clone(&memory), 1 << 16);
        let root = root(profile, &mut context);
        let tape = observations(br#"{"first":true,"second":false}"#, 64, Returned::Io("close source"));
        let mut intern = PublishingIntern { root: Arc::clone(&root), memory: Arc::clone(&memory), calls: 0 };
        let result = schema_string_from_utf8(input(&tape, 64, profile, &mut context), &root, &mut intern, &[], &mut context);
        assert_eq!(intern.calls, 2); assert_eq!(result.observations_consumed, 2);
        native(result.result, "/batch-too-large"); assert!(result.suppressed_close.is_none());
        drop(intern); drop(root); drop(context); assert_eq!(memory.used_bytes(), 0);
    }
}
