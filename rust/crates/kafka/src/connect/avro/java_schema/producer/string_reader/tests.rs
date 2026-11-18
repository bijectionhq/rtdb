use super::*;
use super::super::super::test_support as support;
use crate::connect::registry::reader_symbols::SymbolName;
use std::{cell::{Cell, RefCell}, rc::Rc};

#[derive(Clone)]
struct Intern(Rc<RefCell<InternState>>);
struct InternState {
    names: Vec<Arc<SymbolName>>,
    calls: usize,
    charges: Vec<Arc<MemoryReservation>>,
    _control: Arc<MemoryReservation>,
}
impl Intern {
    fn new(context: &mut ConversionContext) -> Self {
        let control = context.reserve(4096).unwrap();
        Self(Rc::new(RefCell::new(InternState { names: Vec::new(), calls: 0, charges: Vec::new(), _control: control })))
    }
    fn calls(&self) -> usize { self.0.borrow().calls }
}
impl InternAuthority for Intern {
    fn intern(&mut self, candidate: Arc<SymbolName>, context: &mut ConversionContext) -> Result<Arc<SymbolName>> {
        let mut state = self.0.borrow_mut();
        state.calls += 1;
        if let Some(existing) = state.names.iter().find(|name| name.text().view().units().eq(candidate.text().view().units())) {
            return Ok(Arc::clone(existing));
        }
        state.charges.push(context.reserve(1024)?);
        state.names.push(Arc::clone(&candidate));
        Ok(candidate)
    }
}
fn profile(profile: Profile) -> FactoryProfile { FactoryProfile { profile, identity_hash_seed: 37 } }
fn policy() -> SourcePolicy { SourcePolicy { validate_defaults: false, fixed_bytes: FixedBytesLimit::DEFAULT } }

struct Inject {
    pool: TokenRecycler,
    root: Arc<ReaderSymbols>,
    intern: Intern,
    injected: bool,
}
impl InputBuffers for Inject {
    fn profile(&self) -> Profile { self.pool.profile() }
    fn take(&mut self, minimum: usize, context: &mut ConversionContext) -> Result<TokenBuffer> {
        if !self.injected {
            self.injected = true;
            let mut other = self.root.child(context).map_err(symbol_error)?;
            other.find(&[119, 97, 114, 109], &mut self.intern, context).map_err(symbol_error)?;
            other.release(context).map_err(symbol_error)?;
        }
        self.pool.take(minimum, context)
    }
    fn give(&mut self, buffer: TokenBuffer, context: &mut ConversionContext) -> Result<()> { self.pool.give(buffer, context) }
}

#[test]
fn direct_buffer_precedes_child_snapshot_and_reader_buffer_follows_it() {
    for version in [Profile::Jackson216, Profile::Jackson221] {
        for (fallback, calls) in [(false, 1), (true, 2)] {
            let mut context = support::context();
            let factory = SchemaStringFactory::new_default(profile(version), &mut context).unwrap();
            let mut intern = Intern::new(&mut context);
            let mut buffers = Inject { pool: TokenRecycler::empty(version), root: Arc::clone(&factory.symbols), intern: intern.clone(), injected: false };
            let mut input = r#"{"warm":1}"#.to_owned();
            if fallback { input.extend(std::iter::repeat_n(' ', DIRECT_STRING_LIMIT + 1 - input.len())); }
            factory.with_tree(JavaText::from_utf8(&input), &mut buffers, &mut intern, &mut context, |tree, _| {
                assert!(matches!(tree.unwrap().get("warm").unwrap().value(), JsonValue::I32(1)));
                Ok(())
            }).unwrap();
            assert_eq!(intern.calls(), calls);
        }
    }
}

#[test]
fn schema_failure_precedes_buffered_dangling_content_and_success_still_checks_it() {
    let mut context = support::context();
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson216), &mut context).unwrap();
    let mut buffers = TokenRecycler::empty(Profile::Jackson216);
    let mut intern = Intern::new(&mut context);
    let error = factory.parse_canonical(JavaText::from_utf8("\"Missing\" garbage"), &[], &mut buffers, &mut intern, policy(), &mut logical::Unregistered, &mut context).err().unwrap();
    assert_eq!(error.message.as_deref(), Some("Undefined canonical schema reference"));
    let error = factory.parse_canonical(JavaText::from_utf8("\"int\" garbage"), &[], &mut buffers, &mut intern, policy(), &mut logical::Unregistered, &mut context).err().unwrap();
    assert!(error.code.ends_with("avro-schema-dangling"));
}

#[test]
fn source_buffered_check_does_not_scan_string_reader_unread_tail() {
    for version in [Profile::Jackson216, Profile::Jackson221] {
        let mut context = support::context();
        let factory = SchemaStringFactory::new_default(profile(version), &mut context).unwrap();
        let mut intern = Intern::new(&mut context);
        let direct = format!("\"int\"{}X", " ".repeat(DIRECT_STRING_LIMIT - 6));
        assert_eq!(direct.len(), DIRECT_STRING_LIMIT);
        assert!(factory.parse_canonical(JavaText::from_utf8(&direct), &[], &mut TokenRecycler::empty(version), &mut intern, policy(), &mut logical::Unregistered, &mut context).is_err());
        let reader = format!("\"int\"{}X", " ".repeat(DIRECT_STRING_LIMIT - 4));
        assert_eq!(reader.len(), DIRECT_STRING_LIMIT + 2);
        let accepted = factory.parse_canonical(JavaText::from_utf8(&reader), &[], &mut TokenRecycler::empty(version), &mut intern, policy(), &mut logical::Unregistered, &mut context).unwrap();
        assert!(matches!(accepted.view().node().kind, Kind::Primitive(Type::Int)));
    }
}

#[test]
fn comments_are_accepted_inside_the_tree_but_released_trailing_comments_are_dangling() {
    let mut context = support::context();
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson221), &mut context).unwrap();
    let mut buffers = TokenRecycler::empty(Profile::Jackson221);
    let mut intern = Intern::new(&mut context);
    let parsed = factory.parse_canonical(JavaText::from_utf8("/*first*/ {\"type\": /*middle*/ \"int\"} "), &[], &mut buffers, &mut intern, policy(), &mut logical::Unregistered, &mut context).unwrap();
    assert!(matches!(parsed.view().node().kind, Kind::Primitive(Type::Int)));
    assert!(factory.parse_canonical(JavaText::from_utf8("\"int\" //tail"), &[], &mut buffers, &mut intern, policy(), &mut logical::Unregistered, &mut context).err().unwrap().code.ends_with("avro-schema-dangling"));
    assert!(factory.parse_canonical(JavaText::from_utf8("\"int\"\0"), &[], &mut buffers, &mut intern, policy(), &mut logical::Unregistered, &mut context).is_ok());
}

#[test]
fn comments_reject_unquoted_controls_but_keep_the_source_tab_and_newline_rules() {
    let mut context = support::context();
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson221), &mut context).unwrap();
    let mut buffers = TokenRecycler::empty(Profile::Jackson221);
    let mut intern = Intern::new(&mut context);
    for input in ["//bad\0\n\"int\"", "/*bad\u{b}*/\"int\"", "/*open"] {
        assert!(factory.with_tree(JavaText::from_utf8(input), &mut buffers, &mut intern, &mut context, |_, _| -> Result<()> { panic!("invalid comment precedes schema assembly") }).is_err());
    }
    assert!(factory.parse_canonical(JavaText::from_utf8("//a\t\r\n/*b\t\n*/\"int\""), &[], &mut buffers, &mut intern, policy(), &mut logical::Unregistered, &mut context).is_ok());
}

#[test]
fn raw_and_escaped_unmatched_units_are_preserved_in_tree_keys_and_values() {
    let mut context = support::context();
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson216), &mut context).unwrap();
    let mut buffers = TokenRecycler::empty(Profile::Jackson216);
    let mut intern = Intern::new(&mut context);
    let mut input = vec![123, 34, 0xd800, 34, 58, 34, 0xdc00, 34, 44];
    input.extend(r#""escaped":"\ud800\udc00\ud800"}"#.encode_utf16());
    let tree = factory.with_tree(JavaText::from_utf16(&input), &mut buffers, &mut intern, &mut context, |tree, _| Ok(tree.unwrap().clone())).unwrap();
    let JsonValue::Object(fields) = tree.value() else { panic!("expected object") };
    let (key, value) = fields.iter().next().unwrap();
    assert!(key.view().units().eq([0xd800]));
    assert!(value.as_text().unwrap().view().units().eq([0xdc00]));
    assert!(tree.get("escaped").unwrap().as_text().unwrap().view().units().eq([0xd800, 0xdc00, 0xd800]));
}

#[test]
fn unmatched_namespace_and_recursive_reference_share_the_actual_canonical_object() {
    let mut context = support::context();
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson221), &mut context).unwrap();
    let mut buffers = TokenRecycler::empty(Profile::Jackson221);
    let mut intern = Intern::new(&mut context);
    let source = factory.parse_canonical(JavaText::from_utf8(r#"{"type":"record","name":"R","namespace":"\ud800","doc":"\udc00","fields":[{"name":"next","type":["null","R"]}]}"#), &[], &mut buffers, &mut intern, policy(), &mut logical::Unregistered, &mut context).unwrap();
    let Kind::Record { name, fields: Some(fields), .. } = &source.view().node().kind else { panic!("expected record") };
    assert!(name.full().unwrap().view().units().eq([0xd800, 46, 82]));
    assert!(source.view().node().doc.as_ref().unwrap().view().units().eq([0xdc00]));
    let Kind::Union(variants) = &source.view().child(fields[0].schema).unwrap().node().kind else { panic!("expected union") };
    assert_eq!(variants[1], source.id());
}

#[test]
fn json_number_classes_and_duplicate_members_keep_source_values() {
    let mut context = support::context();
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson216), &mut context).unwrap();
    let mut buffers = TokenRecycler::empty(Profile::Jackson216);
    let mut intern = Intern::new(&mut context);
    let tree = factory.with_tree(JavaText::from_utf8(r#"{"x":0,"x":1,"long":2147483648,"integer":9223372036854775808,"float":1.0,"minus":-0.0}"#), &mut buffers, &mut intern, &mut context, |tree, _| Ok(tree.unwrap().clone())).unwrap();
    assert!(matches!(tree.get("x").unwrap().value(), JsonValue::I32(1)));
    assert!(matches!(tree.get("long").unwrap().value(), JsonValue::I64(2147483648)));
    assert!(matches!(tree.get("integer").unwrap().value(), JsonValue::Integer(_)));
    assert!(matches!(tree.get("float").unwrap().value(), JsonValue::F64(value) if *value==1.0));
    assert!(matches!(tree.get("minus").unwrap().value(), JsonValue::F64(value) if value.to_bits()==(-0.0f64).to_bits()));
    let JsonValue::Object(fields) = tree.value() else { panic!("expected object") };
    assert!(fields.iter().next().unwrap().0.view().units().eq([120]));
}

#[test]
fn literal_identifier_boundaries_and_root_number_whitespace_match_the_pinned_reader() {
    for selected in [Profile::Jackson216, Profile::Jackson221] {
        let mut context = support::context();
        let factory = SchemaStringFactory::new_default(profile(selected), &mut context).unwrap();
        let mut buffers = TokenRecycler::empty(selected);
        let mut intern = Intern::new(&mut context);
        for input in ["trueA", "false0", "null\u{301}", "false\u{20ac}", "1X"] {
            assert!(factory.with_tree(JavaText::from_utf8(input), &mut buffers, &mut intern, &mut context, |_, _| -> Result<()> {
                panic!("invalid token {input:?} reaches schema assembly")
            }).is_err());
        }
        let failure = factory.with_tree(JavaText::from_utf8("true$"), &mut buffers, &mut intern, &mut context, |tree, _| {
            assert!(matches!(tree.unwrap().value(), JsonValue::Bool(true)));
            Ok(())
        }).unwrap_err();
        assert!(failure.code.ends_with("avro-schema-dangling"));
        let failure = factory.with_tree(JavaText::from_utf8("1\rX"), &mut buffers, &mut intern, &mut context, |_, _| {
            Err::<(), _>(error("semantic", "Schema assembly"))
        }).unwrap_err();
        assert!(failure.code.ends_with("semantic"));
    }
    assert!(identifier::part(48));
    assert!(identifier::part(65));
    assert!(identifier::part(0x0301));
    assert!(!identifier::part(0xd800));
}

struct CountGive { pool: TokenRecycler, count: Rc<Cell<usize>>, fail: bool }
impl InputBuffers for CountGive {
    fn profile(&self) -> Profile { self.pool.profile() }
    fn take(&mut self, minimum: usize, context: &mut ConversionContext) -> Result<TokenBuffer> { self.pool.take(minimum, context) }
    fn give(&mut self, buffer: TokenBuffer, context: &mut ConversionContext) -> Result<()> {
        self.count.set(self.count.get()+1);
        if self.fail { return Err(error("close-fixture", "Source close failure")); }
        self.pool.give(buffer, context)
    }
}

#[test]
fn empty_tree_closes_before_callback_while_null_is_an_explicit_node() {
    let mut context = support::context();
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson221), &mut context).unwrap();
    let count = Rc::new(Cell::new(0));
    let mut buffers = CountGive { pool: TokenRecycler::empty(Profile::Jackson221), count: Rc::clone(&count), fail: false };
    let mut intern = Intern::new(&mut context);
    factory.with_tree(JavaText::from_utf8(" /*empty*/ "), &mut buffers, &mut intern, &mut context, |tree, _| {
        assert!(tree.is_none());
        assert_eq!(count.get(), 1);
        Ok(())
    }).unwrap();
    factory.with_tree(JavaText::from_utf8("null"), &mut buffers, &mut intern, &mut context, |tree, _| {
        assert!(matches!(tree.unwrap().value(), JsonValue::Null));
        assert_eq!(count.get(), 1);
        Ok(())
    }).unwrap();
    assert_eq!(count.get(), 2);
}

#[test]
fn finally_close_overrides_semantic_failure_and_is_not_replayed_after_empty_eof() {
    let mut context = support::context();
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson216), &mut context).unwrap();
    let count = Rc::new(Cell::new(0));
    let mut buffers = CountGive { pool: TokenRecycler::empty(Profile::Jackson216), count: Rc::clone(&count), fail: true };
    let mut intern = Intern::new(&mut context);
    let failure = factory.with_tree(JavaText::from_utf8("\"int\""), &mut buffers, &mut intern, &mut context, |_, _| Err::<(), _>(error("semantic", "Schema failure"))).unwrap_err();
    assert!(failure.code.ends_with("close-fixture"));
    assert_eq!(count.get(), 1);
    assert!(factory.with_tree(JavaText::from_utf8(""), &mut buffers, &mut intern, &mut context, |_, _| -> Result<()> { panic!("close failure precedes absent-tree callback") }).unwrap_err().code.ends_with("close-fixture"));
    assert_eq!(count.get(), 2);
}

#[test]
fn invalid_json_still_releases_its_successful_symbols_into_the_factory() {
    let mut context = support::context();
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson216), &mut context).unwrap();
    let mut buffers = TokenRecycler::empty(Profile::Jackson216);
    let mut intern = Intern::new(&mut context);
    assert!(factory.with_tree(JavaText::from_utf8(r#"{"warm" ? 1}"#), &mut buffers, &mut intern, &mut context, |_, _| Ok(())).is_err());
    assert_eq!(intern.calls(), 1);
    factory.with_tree(JavaText::from_utf8(r#"{"warm":1}"#), &mut buffers, &mut intern, &mut context, |_, _| Ok(())).unwrap();
    assert_eq!(intern.calls(), 1);
}

#[test]
fn recycler_release_keeps_the_source_version_specific_slot() {
    for (version, expected) in [(Profile::Jackson216, 5000), (Profile::Jackson221, 6000)] {
        let mut context = support::context();
        let mut recycler = TokenRecycler::empty(version);
        recycler.give(crate::connect::avro::TokenBuffer::new(6000, &mut context).unwrap(), &mut context).unwrap();
        recycler.give(TokenBuffer::new(5000, &mut context).unwrap(), &mut context).unwrap();
        assert_eq!(recycler.take(0, &mut context).unwrap().len(), expected);
    }
}

#[test]
fn retained_tree_owns_final_container_and_text_leases_after_parser_context_and_factory_drop() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson221), &mut context).unwrap();
    let mut buffers = TokenRecycler::empty(Profile::Jackson221);
    let mut intern = Intern::new(&mut context);
    let tree = factory.with_tree(JavaText::from_utf8(r#"{"a":["\ud800",1,2,3,4,5]}"#), &mut buffers, &mut intern, &mut context, |tree, _| Ok(tree.unwrap().clone())).unwrap();
    drop(factory);
    drop(buffers);
    drop(intern);
    drop(context);
    assert!(budget.used_bytes()>0);
    let JsonValue::Array(values) = tree.get("a").unwrap().value() else { panic!("expected array") };
    assert_eq!(values.len(), 6);
    assert!(values[0].as_text().unwrap().view().units().eq([0xd800]));
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), 0);
    assert!(tree.retain(&mut denied, &mut json::JsonSeen::default(), 0).is_err());
    drop(denied);
    let mut fresh = ConversionContext::new(Arc::clone(&budget));
    tree.retain(&mut fresh, &mut json::JsonSeen::default(), 0).unwrap();
    drop(tree);
    assert!(budget.used_bytes()>0);
    drop(fresh);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn fallback_refill_preserves_paired_and_unmatched_units_at_the_token_buffer_boundary() {
    for suffix in [&[0xd800, 0xdc00][..], &[0xd800][..]] {
        let mut context = support::context();
        let factory = SchemaStringFactory::new_default(profile(Profile::Jackson221), &mut context).unwrap();
        let mut buffers = TokenRecycler::empty(Profile::Jackson221);
        let mut intern = Intern::new(&mut context);
        let mut input = vec![34];
        input.extend(std::iter::repeat_n(65, 3998));
        input.extend_from_slice(suffix);
        input.push(34);
        input.resize(DIRECT_STRING_LIMIT + 1, 32);
        let tree = factory.with_tree(JavaText::from_utf16(&input), &mut buffers, &mut intern, &mut context, |tree, _| Ok(tree.unwrap().clone())).unwrap();
        let text = tree.as_text().unwrap().view();
        assert_eq!(text.len_units(), 3998 + suffix.len());
        assert!(text.slice_units(3998..text.len_units()).unwrap().units().eq(suffix.iter().copied()));
    }
}

#[test]
fn string_input_does_not_apply_the_stream_bootstrap_bom_rule() {
    let mut context = support::context();
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson216), &mut context).unwrap();
    let mut buffers = TokenRecycler::empty(Profile::Jackson216);
    let mut intern = Intern::new(&mut context);
    assert!(factory.with_tree(JavaText::from_utf8("\u{feff}\"int\""), &mut buffers, &mut intern, &mut context, |_, _| -> Result<()> { panic!("String BOM is a token error") }).is_err());
}

#[test]
fn reader_pairing_uses_comment_normalized_same_owner_reference_and_recursive_definitions() {
    let mut context = support::context();
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson221), &mut context).unwrap();
    let mut buffers = TokenRecycler::empty(Profile::Jackson221);
    let mut intern = Intern::new(&mut context);
    let references = [JavaText::from_utf8(r#"/*ref*/{"type":"record","name":"ns.R","fields":[{"name":"next","type":["null",/*self*/"R"]}]}"#)];
    let pair = ParsedPair::parse_java_text_with_reader(JavaText::from_utf8("/*root*/\"ns.R\"\0"), &references, &BTreeMap::new(), policy(), &factory, &mut buffers, &mut intern, &mut logical::Unregistered, &mut context).unwrap();
    let root = pair.root().canonical().unwrap();
    assert!(pair.definition("ns.R").unwrap().canonical().unwrap().same_object(&root));
    assert!(pair.root().field(0).unwrap().variant(1).unwrap().canonical().unwrap().same_object(&root));
    assert!(matches!(pair.root().native(), apache_avro::Schema::Ref { .. }));
}

#[test]
fn backend_projection_preserves_large_integer_double_signed_zero_and_text_values() {
    let mut context = support::context();
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson216), &mut context).unwrap();
    let mut buffers = TokenRecycler::empty(Profile::Jackson216);
    let mut intern = Intern::new(&mut context);
    let parsed = factory.parse_input(JavaText::from_utf8(r#"{"type":"int","i":9223372036854775808,"n":-18446744073709551617,"d":-0.0,"q":1e30,"a":"x\ny"}"#), &[], &mut buffers, &mut intern, policy(), &mut logical::Unregistered, &mut context).unwrap();
    assert!(parsed.inputs.root().contains("9223372036854775808"));
    assert!(parsed.inputs.root().contains("-18446744073709551617"));
    assert!(parsed.inputs.root().contains("-0.0"));
    let roundtrip = input::parse(parsed.inputs.root(), &mut context).unwrap();
    for key in ["i", "n", "d", "q", "a"] {
        assert!(parsed.canonical.root.view().node().properties.get(key).unwrap().equal(roundtrip.get(key).unwrap(), &mut context, 0).unwrap());
    }
    assert!(matches!(roundtrip.get("d").unwrap().value(), JsonValue::F64(value) if value.to_bits()==(-0.0f64).to_bits()));
}

#[test]
fn integer_projection_formats_zero_sign_and_more_decimal_chunks_than_binary_words() {
    let mut context = support::context();
    for (source, expected) in [
        ("-000", "0"),
        ("4294967295", "4294967295"),
        ("18446744073709551615", "18446744073709551615"),
        ("-9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999", "-9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999"),
    ] {
        let node = JsonNode::integer_decimal(JavaText::from_utf8(source), &mut context).unwrap();
        let JsonValue::Integer(integer) = node.value() else { panic!("expected BigInteger") };
        integer.with_decimal(&mut context, |text| {
            assert_eq!(text, expected);
            Ok(())
        }).unwrap();
    }
}

#[test]
fn native_projection_is_after_source_dangling_and_close_and_rejects_unrepresentable_values() {
    let mut context = support::context();
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson221), &mut context).unwrap();
    let count = Rc::new(Cell::new(0));
    let mut buffers = CountGive { pool: TokenRecycler::empty(Profile::Jackson221), count: Rc::clone(&count), fail: false };
    let mut intern = Intern::new(&mut context);
    let failure = factory.parse_input(JavaText::from_utf8(r#"{"type":"int","note":"\ud800"}"#), &[], &mut buffers, &mut intern, policy(), &mut logical::Unregistered, &mut context).err().unwrap();
    assert!(failure.code.ends_with("avro-reader-native-projection"));
    assert_eq!(count.get(), 1);
    let failure = factory.parse_input(JavaText::from_utf8(r#"{"type":"int","note":"\ud800"} X"#), &[], &mut buffers, &mut intern, policy(), &mut logical::Unregistered, &mut context).err().unwrap();
    assert!(failure.code.ends_with("avro-schema-dangling"));
    assert_eq!(count.get(), 2);
    let failure = factory.parse_input(JavaText::from_utf8(r#"{"type":"int","x":1e400}"#), &[], &mut buffers, &mut intern, policy(), &mut logical::Unregistered, &mut context).err().unwrap();
    assert!(failure.code.ends_with("avro-reader-native-projection"));
    let source = factory.parse_canonical(JavaText::from_utf8(r#"{"type":"int","x":1e400}"#), &[], &mut buffers, &mut intern, policy(), &mut logical::Unregistered, &mut context).unwrap();
    assert!(matches!(source.view().node().properties.get("x").unwrap().value(), JsonValue::F64(value) if value.is_infinite()));
}

#[test]
fn reader_datum_entry_decodes_after_input_factory_recycler_and_context_drop() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let factory = crate::connect::avro::SchemaStringFactory::new_default(profile(Profile::Jackson221), &mut context).unwrap();
    let mut buffers = crate::connect::avro::TokenRecycler::empty(Profile::Jackson221);
    let mut intern = Intern::new(&mut context);
    let input = Text::copy(JavaText::from_utf8(r#"/*schema*/{"type":"record","name":"R","doc":"kept","fields":[{"name":"x","type":"int"}]}"#), &mut context).unwrap();
    let datum = Arc::new(crate::connect::avro::AvroDatumPlan::parse_admitted_java_text_with_reader(input.view(), &[], &BTreeMap::new(), policy(), &factory, &mut buffers, &mut intern, &mut context).unwrap());
    let connect = datum.connect_plan(None, None, &mut 0, &mut context).unwrap();
    drop(input);
    drop(factory);
    drop(buffers);
    drop(intern);
    drop(context);
    assert!(budget.used_bytes()>0);
    let mut next = ConversionContext::new(Arc::clone(&budget));
    let converted = connect.decode(&[14], &mut next).unwrap();
    assert_eq!(converted.value, rtdb_types::Value::Struct(BTreeMap::from([("x".into(), rtdb_types::Value::I32(7))])));
    drop(converted);
    drop(datum);
    drop(connect);
    drop(next);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn all_source_schemas_and_closes_precede_transport_of_an_earlier_unrepresentable_reference() {
    let unrepresentable = JavaText::from_utf8(r#"{"type":"record","name":"R","doc":"\ud800","fields":[]}"#);
    for (references, root, expected) in [
        (vec![unrepresentable, JavaText::from_utf8("\"Missing\"")], "\"R\"", "avro-schema"),
        (vec![unrepresentable], "\"Missing\"", "avro-schema"),
        (vec![unrepresentable], "\"R\" X", "avro-schema-dangling"),
        (vec![unrepresentable], "\"R\"", "avro-reader-native-projection"),
    ] {
        let mut context = support::context();
        let factory = SchemaStringFactory::new_default(profile(Profile::Jackson221), &mut context).unwrap();
        let closes = Rc::new(Cell::new(0));
        let mut buffers = CountGive { pool: TokenRecycler::empty(Profile::Jackson221), count: Rc::clone(&closes), fail: false };
        let mut intern = Intern::new(&mut context);
        let failure = factory.parse_input(JavaText::from_utf8(root), &references, &mut buffers, &mut intern, policy(), &mut logical::Unregistered, &mut context).err().unwrap();
        assert!(failure.code.ends_with(expected), "{}", failure.code);
        assert_eq!(closes.get(), 2);
    }
    struct FailSecondClose(CountGive);
    impl InputBuffers for FailSecondClose {
        fn profile(&self) -> Profile { self.0.profile() }
        fn take(&mut self, minimum: usize, context: &mut ConversionContext) -> Result<TokenBuffer> {
            self.0.take(minimum, context)
        }
        fn give(&mut self, buffer: TokenBuffer, context: &mut ConversionContext) -> Result<()> {
            self.0.fail = self.0.count.get() == 1;
            self.0.give(buffer, context)
        }
    }
    let mut context = support::context();
    let factory = SchemaStringFactory::new_default(profile(Profile::Jackson221), &mut context).unwrap();
    let closes = Rc::new(Cell::new(0));
    let mut buffers = FailSecondClose(CountGive { pool: TokenRecycler::empty(Profile::Jackson221), count: Rc::clone(&closes), fail: false });
    let mut intern = Intern::new(&mut context);
    let failure = factory.parse_input(JavaText::from_utf8("\"R\""), &[unrepresentable], &mut buffers, &mut intern, policy(), &mut logical::Unregistered, &mut context).err().unwrap();
    assert!(failure.code.ends_with("close-fixture"));
    assert_eq!(closes.get(), 2);
}

#[derive(Clone)]
struct Trace { events: Rc<RefCell<Vec<&'static str>>>, _charge: Arc<MemoryReservation> }
impl Trace {
    fn new(context: &mut ConversionContext) -> Self {
        let charge = context.reserve(4096).unwrap();
        let mut events = Vec::new();
        events.try_reserve_exact(8).unwrap();
        Self { events: Rc::new(RefCell::new(events)), _charge: charge }
    }
    fn push(&self, event: &'static str) {
        let mut events = self.events.borrow_mut();
        assert!(events.len() < events.capacity());
        events.push(event);
    }
}
struct AcquireInject { pool: TokenRecycler, root: Arc<ReaderSymbols>, intern: Intern, trace: Trace }
impl InputBuffers for AcquireInject {
    fn profile(&self) -> Profile { self.pool.profile() }
    fn acquire(&mut self, context: &mut ConversionContext) -> Result<()> {
        self.trace.push("acquire");
        let mut child = self.root.child(context).map_err(symbol_error)?;
        child.find(&[119, 97, 114, 109], &mut self.intern, context).map_err(symbol_error)?;
        child.release(context).map_err(symbol_error)?;
        Ok(())
    }
    fn take(&mut self, minimum: usize, context: &mut ConversionContext) -> Result<TokenBuffer> {
        self.trace.push("take");
        self.pool.take(minimum, context)
    }
    fn give(&mut self, buffer: TokenBuffer, context: &mut ConversionContext) -> Result<()> {
        self.trace.push("give");
        self.pool.give(buffer, context)
    }
    fn close(&mut self, _: &mut ConversionContext) -> Result<()> { self.trace.push("close"); Ok(()) }
}

#[test]
fn source_recycler_acquisition_precedes_both_child_snapshot_paths() {
    for selected in [Profile::Jackson216, Profile::Jackson221] {
        for fallback in [false, true] {
            let mut context = support::context();
            let factory = SchemaStringFactory::new_default(profile(selected), &mut context).unwrap();
            let mut intern = Intern::new(&mut context);
            let trace = Trace::new(&mut context);
            let mut buffers = AcquireInject { pool: TokenRecycler::empty(selected), root: Arc::clone(&factory.symbols), intern: intern.clone(), trace: trace.clone() };
            context.reserve(70_000).unwrap();
            let mut input = r#"{"warm":1}"#.to_owned();
            if fallback { input.extend(std::iter::repeat_n(' ', DIRECT_STRING_LIMIT + 1 - input.len())); }
            factory.with_tree(JavaText::from_utf8(&input), &mut buffers, &mut intern, &mut context, |tree, _| {
                trace.push("schema");
                assert!(matches!(tree.unwrap().get("warm").unwrap().value(), JsonValue::I32(1)));
                Ok(())
            }).unwrap();
            assert_eq!(intern.calls(), 1);
            assert_eq!(&*trace.events.borrow(), &["acquire", "take", "schema", "give", "close"]);
        }
    }
}

struct CloseHooks { pool: TokenRecycler, trace: Trace, fail_acquire: bool, fail_give: bool, fail_close: bool }
impl InputBuffers for CloseHooks {
    fn profile(&self) -> Profile { self.pool.profile() }
    fn acquire(&mut self, _: &mut ConversionContext) -> Result<()> {
        self.trace.push("acquire");
        if self.fail_acquire { Err(error("acquire-fixture", "Source acquire failure")) } else { Ok(()) }
    }
    fn take(&mut self, minimum: usize, context: &mut ConversionContext) -> Result<TokenBuffer> {
        self.trace.push("take");
        self.pool.take(minimum, context)
    }
    fn give(&mut self, buffer: TokenBuffer, context: &mut ConversionContext) -> Result<()> {
        self.trace.push("give");
        if self.fail_give { Err(error("give-fixture", "Source token return failure")) } else { self.pool.give(buffer, context) }
    }
    fn close(&mut self, _: &mut ConversionContext) -> Result<()> {
        self.trace.push("close");
        if self.fail_close { Err(error("pool-close-fixture", "Source IOContext close failure")) } else { Ok(()) }
    }
}

#[test]
fn io_context_close_follows_token_return_is_skipped_on_failure_and_runs_once_at_empty_eof() {
    for (input, acquire, give, close, expected, events) in [
        ("\"int\"", true, false, false, "acquire-fixture", &["acquire"][..]),
        ("\"int\"", false, true, false, "give-fixture", &["acquire", "take", "schema", "give"][..]),
        ("\"int\"", false, false, true, "pool-close-fixture", &["acquire", "take", "schema", "give", "close"][..]),
        ("", false, false, true, "pool-close-fixture", &["acquire", "take", "give", "close"][..]),
    ] {
        let mut context = support::context();
        let factory = SchemaStringFactory::new_default(profile(Profile::Jackson221), &mut context).unwrap();
        let mut intern = Intern::new(&mut context);
        let trace = Trace::new(&mut context);
        let mut buffers = CloseHooks { pool: TokenRecycler::empty(Profile::Jackson221), trace: trace.clone(), fail_acquire: acquire, fail_give: give, fail_close: close };
        let failure = factory.with_tree(JavaText::from_utf8(input), &mut buffers, &mut intern, &mut context, |_, _| {
            trace.push("schema");
            Err::<(), _>(error("semantic", "Schema failure"))
        }).unwrap_err();
        assert!(failure.code.ends_with(expected));
        assert_eq!(&*trace.events.borrow(), events);
    }
}
