use super::*;
use std::{collections::BTreeMap, mem::size_of};
use crate::RegexForm;
use rtdb_types::{CalendarDate, CivilDate, DateUnit, DecimalValue, Keyword, MemoryBudget, TimeUnit, Value};

fn recipe(pattern: RegexForm) -> RegexReplacement { RegexReplacement { pattern, ..Default::default() } }
fn expected(pattern: &str) -> String {
    format!(r#"{{"pattern":{pattern},"replacement":{{"Literal":"Null"}},"start":{{"Literal":"Null"}},"occurrence":{{"Literal":"Null"}},"flags":{{"Literal":"Null"}}}}"#)
}
fn assert_form(form: RegexForm, pattern: &str) {
    let source = recipe(form);
    let legacy = serde_json::to_string(&source).unwrap();
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let output = encode(&source, &allocation).unwrap();
    assert_eq!(output.as_str(), expected(pattern));
    assert_eq!(output.as_str(), legacy);
    drop(allocation);
    assert_eq!(memory.used_bytes(), output.retained_bytes());
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}
fn frame_charge(capacity: usize) -> usize { capacity * size_of::<Frame<'_>>() + 256 }
fn measures(source: &RegexReplacement) -> (usize, usize) {
    let memory = MemoryBudget::new(32 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut traversal = Traversal::new(&allocation);
    let mut count = CountingSink::default();
    traversal.write(source, &mut count, Growth::Admit).unwrap();
    let frames = memory.used_bytes() - 4096;
    (count.length, frames)
}

#[test]
fn every_raw_form_variant_keeps_its_existing_tagged_shape() {
    for (form, pattern) in [
        (RegexForm::Literal(Value::Null), r#"{"Literal":"Null"}"#),
        (RegexForm::Character('😀'), r#"{"Character":"😀"}"#),
        (RegexForm::Symbol("a/b".into()), r#"{"Symbol":"a/b"}"#),
        (RegexForm::BigInteger("999999999999999999999".into()), r#"{"BigInteger":"999999999999999999999"}"#),
        (RegexForm::Decimal("1.00M".into()), r#"{"Decimal":"1.00M"}"#),
        (RegexForm::List(vec![]), r#"{"List":[]}"#),
        (RegexForm::Vector(vec![RegexForm::Character('x')]), r#"{"Vector":[{"Character":"x"}]}"#),
        (RegexForm::Set(vec![RegexForm::Literal(Value::I64(2)), RegexForm::Literal(Value::I64(1))]), r#"{"Set":[{"Literal":{"I64":2}},{"Literal":{"I64":1}}]}"#),
        (RegexForm::Map(vec![(RegexForm::Symbol("k".into()), RegexForm::Character('v'))]), r#"{"Map":[[{"Symbol":"k"},{"Character":"v"}]]}"#),
        (RegexForm::Tagged { tag: "tag".into(), value: Box::new(RegexForm::Literal(Value::Null)) }, r#"{"Tagged":{"tag":"tag","value":{"Literal":"Null"}}}"#),
        (RegexForm::AnonymousFunction(Box::new(RegexForm::List(vec![]))), r#"{"AnonymousFunction":{"List":[]}}"#),
    ] { assert_form(form, pattern); }
}

#[test]
fn all_forty_one_physical_value_variants_keep_their_wire_shapes() {
    let cases = vec![
        (Value::Undefined, r#""Undefined""#), (Value::Null, r#""Null""#), (Value::Bool(true), r#"{"Bool":true}"#),
        (Value::I8(-8), r#"{"I8":-8}"#), (Value::I16(-16), r#"{"I16":-16}"#), (Value::I32(-32), r#"{"I32":-32}"#), (Value::I64(-64), r#"{"I64":-64}"#),
        (Value::U8(8), r#"{"U8":8}"#), (Value::U16(16), r#"{"U16":16}"#), (Value::U32(32), r#"{"U32":32}"#), (Value::U64(u64::MAX), r#"{"U64":18446744073709551615}"#),
        (Value::F16(0x8000), r#"{"F16":32768}"#),
        (Value::F32(f32::from_bits(0x7fc00001)), r#"{"F32":2143289345}"#),
        (Value::F64(f64::from_bits(0x7ff8000000000001)), r#"{"F64":9221120237041090561}"#),
        (Value::Utf8("é😀\n".into()), r#"{"Utf8":"é😀\n"}"#),
        (Value::Binary(vec![0, 127, 255]), r#"{"Binary":[0,127,255]}"#),
        (Value::FixedSizeBinary(vec![9]), r#"{"FixedSizeBinary":[9]}"#),
        (Value::Decimal(DecimalValue { unscaled: vec![255, 0], precision: 4, scale: -2, bit_width: 16 }), r#"{"Decimal":{"unscaled":[255,0],"precision":4,"scale":-2,"bit_width":16}}"#),
        (Value::Date(CalendarDate::new(CivilDate::from_ymd(2000, 1, 2).unwrap(), DateUnit::Day)), r#"{"Date":{"date":"2000-01-02","unit":"Day"}}"#),
        (Value::DateDay(-1), r#"{"DateDay":-1}"#), (Value::DateMilli(-1000), r#"{"DateMilli":-1000}"#),
        (Value::Time { value: 7, unit: TimeUnit::Microsecond, bit_width: 64 }, r#"{"Time":{"value":7,"unit":"Microsecond","bit_width":64}}"#),
        (Value::Timestamp { value: -7, unit: TimeUnit::Nanosecond, timezone: Some("UTC".into()) }, r#"{"Timestamp":{"value":-7,"unit":"Nanosecond","timezone":"UTC"}}"#),
        (Value::Duration { value: 8, unit: TimeUnit::Second }, r#"{"Duration":{"value":8,"unit":"Second"}}"#),
        (Value::IntervalYearMonth(-12), r#"{"IntervalYearMonth":-12}"#),
        (Value::IntervalDayTime { days: -1, milliseconds: 3 }, r#"{"IntervalDayTime":{"days":-1,"milliseconds":3}}"#),
        (Value::IntervalMonthDayNano { months: 1, days: 2, nanoseconds: 3 }, r#"{"IntervalMonthDayNano":{"months":1,"days":2,"nanoseconds":3}}"#),
        (Value::IntervalMonthDayMicro { months: 4, days: 5, nanoseconds: 6 }, r#"{"IntervalMonthDayMicro":{"months":4,"days":5,"nanoseconds":6}}"#),
        (Value::Keyword(Keyword::from_name("a/b")), r#"{"Keyword":"a/b"}"#),
        (Value::Uuid([0; 16]), r#"{"Uuid":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}"#),
        (Value::Uri("https://example.test/é".into()), r#"{"Uri":"https://example.test/é"}"#),
        (Value::Transit(vec![0, 255]), r#"{"Transit":[0,255]}"#),
        (Value::Oid(-1), r#"{"Oid":-1}"#), (Value::RegClass(2), r#"{"RegClass":2}"#), (Value::RegProc(3), r#"{"RegProc":3}"#),
        (Value::List(vec![Value::I64(1)]), r#"{"List":[{"I64":1}]}"#),
        (Value::Set(vec![Value::I64(2), Value::Null]), r#"{"Set":[{"I64":2},"Null"]}"#),
        (Value::Map { entries: vec![(Value::I64(1), Value::Utf8("v".into()))], sorted: true }, r#"{"Map":{"entries":[[{"I64":1},{"Utf8":"v"}]],"sorted":true}}"#),
        (Value::FixedSizeList(vec![Value::Bool(false)]), r#"{"FixedSizeList":[{"Bool":false}]}"#),
        (Value::TimestampTzRange { lower: -1, upper: 2 }, r#"{"TimestampTzRange":{"lower":-1,"upper":2}}"#),
        (Value::Struct(BTreeMap::from([("v".into(), Value::I64(3))])), r#"{"Struct":{"v":{"I64":3}}}"#),
    ];
    assert_eq!(cases.len(), 41);
    for (value, wire) in cases { assert_form(RegexForm::Literal(value), &format!(r#"{{"Literal":{wire}}}"#)); }
}

#[test]
fn float_payload_bits_and_optional_timezone_are_not_normalized() {
    for bits in [0u32, 0x80000000, 0x7f800000, 0xff800000, 0x7fc00001, 0x7fa00002] {
        assert_form(RegexForm::Literal(Value::F32(f32::from_bits(bits))), &format!(r#"{{"Literal":{{"F32":{bits}}}}}"#));
    }
    for bits in [0u64, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000001, 0x7ff0000000000002] {
        assert_form(RegexForm::Literal(Value::F64(f64::from_bits(bits))), &format!(r#"{{"Literal":{{"F64":{bits}}}}}"#));
    }
    assert_form(RegexForm::Literal(Value::Timestamp { value: 0, unit: TimeUnit::Millisecond, timezone: None }), r#"{"Literal":{"Timestamp":{"value":0,"unit":"Millisecond","timezone":null}}}"#);
}

#[test]
fn recipe_fields_and_recursive_map_keys_keep_source_order_without_metadata_admission() {
    let source = RegexReplacement {
        pattern: RegexForm::Symbol("pattern-effect".into()),
        replacement: RegexForm::Literal(Value::I64(9)),
        start: RegexForm::List(vec![RegexForm::Symbol("start-effect".into())]),
        occurrence: RegexForm::AnonymousFunction(Box::new(RegexForm::Symbol("n-effect".into()))),
        flags: RegexForm::Literal(Value::Map { entries: vec![(Value::List(vec![Value::I64(2)]), Value::Struct(BTreeMap::from([("z".into(), Value::Null), ("a".into(), Value::Bool(true))])))], sorted: false }),
    };
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let output = encode(&source, &allocation).unwrap();
    assert_eq!(output.as_str(), serde_json::to_string(&source).unwrap());
    assert!(output.as_str().starts_with(r#"{"pattern":{"Symbol":"pattern-effect"},"replacement":{"Literal":{"I64":9}},"start":{"List":[{"Symbol":"start-effect"}]}"#));
    assert!(output.as_str().contains(r#""entries":[[{"List":[{"I64":2}]},{"Struct":{"a":{"Bool":true},"z":"Null"}}]],"sorted":false"#));
    assert!(source.replacement_text().is_err());
}

#[test]
fn shared_count_and_fill_reuse_the_entire_admitted_frame_history() {
    let source = recipe(RegexForm::List(vec![RegexForm::Literal(Value::List(vec![Value::I64(1), Value::Map { entries: vec![], sorted: false }]))]));
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut traversal = Traversal::new(&allocation);
    let mut count = CountingSink::default();
    traversal.write(&source, &mut count, Growth::Admit).unwrap();
    let before = memory.used_bytes();
    let limit = traversal.limit;
    let mut output = BoundedSink::new(count.length, &allocation).unwrap();
    let scratch = count.length + 256;
    traversal.write(&source, &mut output, Growth::Reuse).unwrap();
    assert_eq!(traversal.limit, limit);
    assert_eq!(memory.used_bytes(), before + scratch);
    let output = output.finish().unwrap();
    assert_eq!(output.as_str(), serde_json::to_string(&source).unwrap());
    assert_eq!(memory.used_bytes(), before + scratch + output.retained_bytes());
}

#[test]
fn exact_and_one_byte_short_combined_budgets_target_retained_publication() {
    let source = recipe(RegexForm::Literal(Value::Keyword(Keyword::from_name("plain/name"))));
    let (length, frames) = measures(&source);
    let retained = JsonDocument::allocation_bound(length).unwrap();
    let scratch = length + 256;
    let bound = 4096 + frames + scratch + retained;
    let memory = MemoryBudget::new(bound);
    let allocation = AllocationScope::new(&memory).unwrap();
    let output = encode(&source, &allocation).unwrap();
    assert_eq!(output.as_str(), serde_json::to_string(&source).unwrap());
    assert!(output.as_str().contains(r#"{"Keyword":"plain/name"}"#));
    assert!(!output.as_str().contains(":plain/name"));
    assert_eq!(memory.used_bytes(), bound);
    assert_eq!(memory.peak_bytes(), bound);
    drop(allocation);
    assert_eq!(memory.used_bytes(), retained);
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
    let memory = MemoryBudget::new(bound - 1);
    let allocation = AllocationScope::new(&memory).unwrap();
    assert!(matches!(encode(&source, &allocation), Err(JsonWriteFailure::Admission(error)) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(memory.used_bytes(), 4096 + frames + scratch);
    assert_eq!(source.pattern, RegexForm::Literal(Value::Keyword(Keyword::from_name("plain/name"))));
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
}

struct DeepFixture { recipe: Option<RegexReplacement>, forms: Vec<RegexForm>, values: Vec<Value> }
impl DeepFixture {
    fn new(pattern: RegexForm, nodes: usize) -> Self {
        Self { recipe: Some(recipe(pattern)), forms: Vec::with_capacity(nodes + 5), values: Vec::with_capacity(nodes + 5) }
    }
    fn source(&self) -> &RegexReplacement { self.recipe.as_ref().unwrap() }
}
impl Drop for DeepFixture {
    fn drop(&mut self) {
        if let Some(RegexReplacement { pattern, replacement, start, occurrence, flags }) = self.recipe.take() {
            self.forms.extend([pattern, replacement, start, occurrence, flags]);
        }
        while let Some(form) = self.forms.pop() {
            match form {
                RegexForm::Literal(value) => self.values.push(value),
                RegexForm::List(values) | RegexForm::Vector(values) | RegexForm::Set(values) => self.forms.extend(values),
                RegexForm::Map(entries) => for (key, value) in entries { self.forms.extend([key, value]); },
                RegexForm::Tagged { value, .. } | RegexForm::AnonymousFunction(value) => self.forms.push(*value),
                _ => (),
            }
        }
        while let Some(value) = self.values.pop() {
            match value {
                Value::List(values) | Value::Set(values) | Value::FixedSizeList(values) => self.values.extend(values),
                Value::Map { entries, .. } => for (key, value) in entries { self.values.extend([key, value]); },
                Value::Struct(fields) => self.values.extend(fields.into_values()),
                _ => (),
            }
        }
    }
}

#[test]
fn twenty_thousand_deep_physical_values_inside_a_literal_use_flat_work() {
    const DEPTH: usize = 20_000;
    let mut value = Value::I64(7);
    for _ in 0..DEPTH { value = Value::List(vec![value]); }
    let fixture = DeepFixture::new(RegexForm::Literal(value), DEPTH + 1);
    let memory = MemoryBudget::new(64 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let output = encode(fixture.source(), &allocation).unwrap();
    assert_eq!(output.as_str().matches(r#"{"List":["#).count(), DEPTH);
    assert_eq!(output.as_str().matches(r#"{"I64":7}"#).count(), 1);
    let shallow = serde_json::to_string(&recipe(RegexForm::Literal(Value::I64(7)))).unwrap();
    assert_eq!(output.as_str().len(), shallow.len() + DEPTH * 11);
    drop(fixture);
    drop(allocation);
    assert_eq!(memory.used_bytes(), output.retained_bytes());
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn twenty_thousand_nested_raw_forms_use_the_same_flat_traversal() {
    const DEPTH: usize = 20_000;
    let mut form = RegexForm::Literal(Value::Null);
    for _ in 0..DEPTH { form = RegexForm::List(vec![form]); }
    let fixture = DeepFixture::new(form, DEPTH + 1);
    let memory = MemoryBudget::new(64 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let output = encode(fixture.source(), &allocation).unwrap();
    assert_eq!(output.as_str().matches(r#"{"List":["#).count(), DEPTH);
    assert_eq!(output.as_str().len(), expected(r#"{"Literal":"Null"}"#).len() + DEPTH * 11);
    drop(fixture);
    drop(allocation);
    assert_eq!(memory.used_bytes(), output.retained_bytes());
}

#[test]
fn deep_mixed_containers_include_recursive_map_keys_and_struct_values() {
    const DEPTH: usize = 5_000;
    let mut value = Value::Utf8("leaf".into());
    for level in 0..DEPTH {
        value = match level % 5 {
            0 => Value::List(vec![value]), 1 => Value::Set(vec![value]), 2 => Value::FixedSizeList(vec![value]),
            3 => Value::Map { entries: vec![(value, Value::Null)], sorted: false },
            _ => Value::Struct(BTreeMap::from([("nested".into(), value)])),
        };
    }
    let fixture = DeepFixture::new(RegexForm::Vector(vec![RegexForm::Literal(value)]), DEPTH * 2 + 2);
    let memory = MemoryBudget::new(64 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let output = encode(fixture.source(), &allocation).unwrap();
    for prefix in [r#"{"List":["#, r#"{"Set":["#, r#"{"FixedSizeList":["#, r#"{"Map":{"entries":[["#, r#"{"Struct":{"nested":"#] {
        assert_eq!(output.as_str().matches(prefix).count(), DEPTH / 5);
    }
    assert_eq!(output.as_str().matches(r#"{"Utf8":"leaf"}"#).count(), 1);
    assert_eq!(output.as_str().matches(r#""sorted":false"#).count(), DEPTH / 5);
}

#[test]
fn wide_physical_values_inside_raw_maps_keep_order_without_a_width_sized_stack() {
    let values = (0..1024).map(|index| Value::Map { entries: vec![(Value::I64(index), Value::List(vec![Value::Null]))], sorted: index % 2 == 0 }).collect();
    let fixture = DeepFixture::new(RegexForm::Map(vec![(RegexForm::Symbol("rows".into()), RegexForm::Literal(Value::List(values)))]), 6000);
    let (length, frames) = measures(fixture.source());
    assert_eq!(frames, frame_charge(16));
    let memory = MemoryBudget::new(4 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let output = encode(fixture.source(), &allocation).unwrap();
    assert_eq!(output.as_str().len(), length);
    assert_eq!(output.as_str().matches(r#"{"Map":{"entries":"#).count(), 1024);
    assert!(output.as_str().find(r#"{"I64":0}"#).unwrap() < output.as_str().find(r#"{"I64":1023}"#).unwrap());
}

#[test]
fn first_frame_and_full_replacement_failures_preserve_unpublished_input() {
    let source = recipe(RegexForm::Literal(Value::I64(3)));
    let memory = MemoryBudget::new(4096 + frame_charge(16) - 1);
    let allocation = AllocationScope::new(&memory).unwrap();
    assert!(matches!(encode(&source, &allocation), Err(JsonWriteFailure::Admission(_))));
    assert_eq!(memory.used_bytes(), 4096);
    let mut value = Value::I64(8);
    for _ in 0..64 { value = Value::List(vec![value]); }
    let fixture = DeepFixture::new(RegexForm::Literal(value), 65);
    let memory = MemoryBudget::new(4096 + frame_charge(16) + frame_charge(32) - 1);
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut traversal = Traversal::new(&allocation);
    let mut count = CountingSink::default();
    assert!(matches!(traversal.write(fixture.source(), &mut count, Growth::Admit), Err(JsonWriteFailure::Admission(_))));
    assert_eq!(traversal.limit, 16);
    assert_eq!(memory.used_bytes(), 4096 + frame_charge(16));
    assert!(matches!(fixture.source().pattern, RegexForm::Literal(Value::List(_))));
}

struct FailingSink { bytes: Vec<u8>, limit: usize }
impl JsonSink for FailingSink {
    fn append(&mut self, bytes: &[u8]) -> WriteResult<()> {
        if self.bytes.len() + bytes.len() > self.limit { return Err(JsonWriteFailure::Capacity); }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

#[test]
fn sink_and_leaf_serializer_failures_return_the_original_prefix_error() {
    let source = recipe(RegexForm::Literal(Value::Utf8("é\n😀".into())));
    let expected = serde_json::to_string(&source).unwrap();
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    for limit in 0..expected.len() {
        let mut sink = FailingSink { bytes: Vec::new(), limit };
        let mut traversal = Traversal::new(&allocation);
        assert!(matches!(traversal.write(&source, &mut sink, Growth::Admit), Err(JsonWriteFailure::Capacity)));
        assert!(expected.as_bytes().starts_with(&sink.bytes));
        assert!(sink.bytes.len() <= limit);
    }
    let mut sink = FailingSink { bytes: Vec::new(), limit: 2 };
    assert!(matches!(leaf::serialize(leaf::LeafValue::new(&Value::Utf8("leaf".into())).unwrap(), &mut sink), Err(JsonWriteFailure::Capacity)));
    assert_eq!(serde_json::to_string(&source).unwrap(), expected);
}

#[test]
fn delegated_leaf_admission_excludes_recursive_values_and_keyword_cloning() {
    for value in [Value::List(vec![]), Value::Set(vec![]), Value::Map { entries: vec![], sorted: false }, Value::FixedSizeList(vec![]), Value::Struct(BTreeMap::new()), Value::Keyword(Keyword::from_name("name"))] {
        assert!(leaf::LeafValue::new(&value).is_none());
    }
    assert!(leaf::LeafValue::new(&Value::Utf8("borrowed".into())).is_some());
}

#[test]
fn scratch_admission_failure_leaves_a_live_sibling_and_all_scope_charges_intact() {
    let source = recipe(RegexForm::Literal(Value::I64(5)));
    let (length, frames) = measures(&source);
    let sibling_bound = JsonDocument::allocation_bound(1).unwrap();
    let memory = MemoryBudget::new(4096 + sibling_bound + frames + length + 256 - 1);
    let sibling = JsonDocument::copy("0", &memory).unwrap();
    let allocation = AllocationScope::new(&memory).unwrap();
    assert!(matches!(encode(&source, &allocation), Err(JsonWriteFailure::Admission(_))));
    assert_eq!(sibling.as_str(), "0");
    assert_eq!(memory.used_bytes(), 4096 + sibling_bound + frames);
    drop(allocation);
    assert_eq!(memory.used_bytes(), sibling_bound);
    drop(sibling);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn recipe_document_and_its_clone_retain_storage_after_all_creators_drop() {
    let memory = MemoryBudget::new(1 << 20);
    let output = {
        let child = memory.scope();
        let allocation = AllocationScope::new(&child).unwrap();
        let source = recipe(RegexForm::Literal(Value::Utf8("é😀".into())));
        encode(&source, &allocation).unwrap()
    };
    let retained = output.retained_bytes();
    assert_eq!(memory.used_bytes(), retained);
    let cloned = output.clone();
    drop(output);
    assert_eq!(cloned.as_str(), expected(r#"{"Literal":{"Utf8":"é😀"}}"#));
    assert_eq!(memory.used_bytes(), retained);
    drop(cloned);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn reuse_mode_rejects_unadmitted_frames_instead_of_growing_silently() {
    let source = RegexReplacement::default();
    let memory = MemoryBudget::new(4096);
    let allocation = AllocationScope::new(&memory).unwrap();
    let mut traversal = Traversal::new(&allocation);
    assert!(matches!(traversal.write(&source, &mut CountingSink::default(), Growth::Reuse), Err(JsonWriteFailure::Internal)));
    assert_eq!(memory.used_bytes(), 4096);
    assert_eq!(traversal.limit, 0);
}

#[test]
fn literal_view_leaf_bridge_preserves_the_existing_utf8_guard_membership() {
    let source = Value::Utf8("borrowed".into());
    assert!(matches!(RegexLiteralView::new(&source).kind(), LiteralKind::Text("borrowed")));
    assert!(leaf::LeafValue::new(&source).is_some());
    let source = Value::I64(7);
    let LiteralKind::Leaf(proved) = RegexLiteralView::new(&source).kind() else { panic!("expected a physical leaf") };
    assert!(std::ptr::eq(proved.value(), &source));
    assert!(leaf::LeafValue::new(proved.value()).is_some());
    for value in [Value::List(vec![]), Value::Set(vec![]), Value::Map { entries: vec![], sorted: false }, Value::FixedSizeList(vec![]), Value::Struct(BTreeMap::new()), Value::Keyword(Keyword::from_name("name"))] {
        assert!(!matches!(RegexLiteralView::new(&value).kind(), LiteralKind::Leaf(_)));
        assert!(leaf::LeafValue::new(&value).is_none());
    }
}

#[test]
fn proved_physical_leaf_serializer_failures_keep_the_original_prefix_error() {
    let source = recipe(RegexForm::Literal(Value::I64(123456789)));
    let expected = serde_json::to_string(&source).unwrap();
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    for limit in 0..expected.len() {
        let mut sink = FailingSink { bytes: Vec::new(), limit };
        let mut traversal = Traversal::new(&allocation);
        assert!(matches!(traversal.write(&source, &mut sink, Growth::Admit), Err(JsonWriteFailure::Capacity)));
        assert!(expected.as_bytes().starts_with(&sink.bytes));
        assert!(sink.bytes.len() <= limit);
    }
    assert_eq!(serde_json::to_string(&source).unwrap(), expected);
}
