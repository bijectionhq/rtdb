use rtdb_types::StructFields;
use super::*;
use rtdb_types::{CalendarDate, CivilDate, DateUnit, DecimalValue, Keyword, TimeUnit};

fn literal(value: &Value) -> RegexForm { let mut builder = RegexFormBuilder::new_untracked(); builder.literal(value).unwrap(); builder.finish().unwrap() }

#[test]
fn inline_nil_builder_and_default_need_no_budget_or_reservation() {
    let memory = MemoryBudget::new(0);
    let mut builder = RegexFormBuilder::new_in(&memory);
    builder.begin_literal().unwrap(); builder.value_leaf(&Value::Null).unwrap(); builder.end().unwrap();
    let value = builder.finish().unwrap();
    assert_eq!(value, RegexForm::default());
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(memory.peak_bytes(), 0);
    assert!(matches!(value.0, Backing::InlineNull));
    let copy = RegexForm::copy_in(value.view(), &memory).unwrap();
    assert_eq!(copy, value);
    assert_eq!(memory.peak_bytes(), 0);
}

#[test]
fn paired_root_clones_keep_backing_and_ancestor_budget_alive_after_creator_exit() {
    let parent = MemoryBudget::new(1 << 20);
    let value = {
        let region = parent.scope();
        let mut builder = RegexFormBuilder::new_in(&region);
        builder.begin_list().unwrap(); builder.symbol("retained").unwrap(); builder.end().unwrap();
        builder.finish().unwrap()
    };
    let retained = parent.used_bytes();
    assert!(retained > 0);
    let clone = value.clone();
    match (&value.0, &clone.0) {
        (Backing::Tracked { storage: a, _reservation: ar }, Backing::Tracked { storage: b, _reservation: br }) => { assert!(Arc::ptr_eq(a, b)); assert!(Arc::ptr_eq(ar, br)); }
        _ => panic!("tracked roots"),
    }
    drop(value);
    assert_eq!(parent.used_bytes(), retained);
    let weak = Arc::downgrade(&parent);
    drop(parent);
    assert!(weak.upgrade().is_some());
    assert_eq!(format!("{clone:?}"), "List([Symbol(\"retained\")])");
    drop(clone);
    assert!(weak.upgrade().is_none());
}

#[test]
fn failed_copy_releases_destination_buffers_and_keeps_the_source_and_its_charge() {
    let source_budget = MemoryBudget::new(8 << 20);
    let mut builder = RegexFormBuilder::new_in(&source_budget);
    builder.symbol(&"x".repeat(1 << 20)).unwrap();
    let source = builder.finish().unwrap();
    let retained = source_budget.used_bytes();
    let destination = MemoryBudget::new(4096);
    let error = RegexForm::copy_in(source.view(), &destination).unwrap_err();
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(destination.used_bytes(), 0);
    assert_eq!(source_budget.used_bytes(), retained);
    assert!(matches!(source.view().kind(), RegexFormKind::Symbol(value) if value.len() == 1 << 20 && value.bytes().all(|byte| byte == b'x')));
    drop(source);
    assert_eq!(source_budget.used_bytes(), 0);
}

#[test]
fn invalid_events_poison_and_destroy_partial_buffers_before_returning() {
    let memory = MemoryBudget::new(1 << 20);
    let mut builder = RegexFormBuilder::new_in(&memory);
    builder.begin_map().unwrap(); builder.symbol("key").unwrap();
    assert!(memory.used_bytes() > 0);
    assert_eq!(builder.end().unwrap_err().code, "rtdb.plan/regex-form-build");
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(builder.symbol("later").unwrap_err().code, "rtdb.plan/regex-form-build");
    assert_eq!(builder.finish().unwrap_err().code, "rtdb.plan/regex-form-build");
    let mut builder = RegexFormBuilder::new_in(&memory);
    assert_eq!(builder.value_leaf(&Value::I64(1)).unwrap_err().code, "rtdb.plan/regex-form-build");
    assert_eq!(memory.used_bytes(), 0);
    let mut builder = RegexFormBuilder::new_untracked();
    builder.begin_literal().unwrap();
    assert_eq!(builder.value_leaf(&Value::list(vec![])).unwrap_err().code, "rtdb.plan/regex-form-build");
}

#[test]
fn fresh_copies_have_distinct_nonempty_payloads_while_shared_nan_stays_nonreflexive() {
    let source = literal(&Value::Utf8("owned".into()));
    let memory = MemoryBudget::new(1 << 20);
    let copy = RegexForm::copy_in(source.view(), &memory).unwrap();
    let text = |form: &RegexForm| match form.view().kind() { RegexFormKind::Literal(value) => match value.kind() { RegexLiteralKind::Text(value) => value.as_ptr(), _ => panic!() }, _ => panic!() };
    assert_eq!(copy, source);
    assert_ne!(text(&copy), text(&source));
    let nan = literal(&Value::F64(f64::from_bits(0x7ff8000000000001)));
    assert_ne!(nan, nan);
    assert_ne!(nan, nan.clone());
    assert_eq!(literal(&Value::F32(0.0)), literal(&Value::F32(-0.0)));
}

#[test]
fn ratio_metadata_roundtrips_and_copies_with_its_owned_text_charge() {
    let json = r#"{"List":[{"Symbol":"str"},{"Ratio":"-2/3"}]}"#;
    let source = serde_json::from_str::<RegexForm>(json).unwrap();
    assert_eq!(serde_json::to_string(&source).unwrap(), json);
    assert_eq!(format!("{source:?}"), r#"List([Symbol("str"), Ratio("-2/3")])"#);
    let symbol = serde_json::from_str::<RegexForm>(r#"{"List":[{"Symbol":"str"},{"Symbol":"-2/3"}]}"#).unwrap();
    assert_ne!(source, symbol);
    assert_eq!(source.work_bound().unwrap(), symbol.work_bound().unwrap());

    let memory = MemoryBudget::new(1 << 20);
    let copy = RegexForm::copy_in(source.view(), &memory).unwrap();
    assert_eq!(copy, source);
    assert_eq!(copy.work_bound().unwrap(), source.work_bound().unwrap());
    let RegexFormKind::List(values) = copy.view().kind() else { panic!("raw list") };
    assert!(matches!(values.at(1).kind(), RegexFormKind::Ratio("-2/3")));
    assert!(memory.used_bytes() > 0);
    drop(copy);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn struct_last_value_and_first_key_backing_survive_sorted_completion_with_charged_tombstones() {
    let mut builder = RegexFormBuilder::new_untracked();
    builder.begin_literal().unwrap(); builder.begin_value_struct().unwrap();
    builder.field("x").unwrap(); builder.begin_value_list().unwrap(); builder.value_text(&"d".repeat(1024)).unwrap(); builder.end().unwrap();
    builder.field("x").unwrap(); builder.value_leaf(&Value::I64(7)).unwrap();
    builder.field("a").unwrap(); builder.value_leaf(&Value::Bool(true)).unwrap();
    builder.end().unwrap(); builder.end().unwrap();
    let value = builder.finish().unwrap();
    let expected = literal(&Value::Struct(StructFields::from([("a".into(), Value::Bool(true)), ("x".into(), Value::I64(7))])));
    assert_eq!(value, expected);
    assert_eq!(value.work_bound().unwrap(), expected.work_bound().unwrap());
    assert!(value.retained_bytes().unwrap() > expected.retained_bytes().unwrap());
    let storage = value.storage().unwrap();
    let storage::RawKind::Literal(id) = storage.forms[0].kind else { panic!() };
    let storage::LiteralKind::Struct(span) = storage.literals[id.0].kind else { panic!() };
    assert_eq!(storage.fields[span.start + 1].name.0, 0);
    assert_eq!(serde_json::to_string(&value).unwrap(), r#"{"Literal":{"Struct":{"a":{"Bool":true},"x":{"I64":7}}}}"#);
}

#[test]
fn direct_serde_accepts_value_before_tag_and_entries_before_or_after_sorted() {
    for input in [r#"{"Tagged":{"value":{"Symbol":"x"},"tag":"reader"}}"#, r#"{"Tagged":{"tag":"reader","value":{"Symbol":"x"}}}"#] {
        assert_eq!(serde_json::to_string(&serde_json::from_str::<RegexForm>(input).unwrap()).unwrap(), r#"{"Tagged":{"tag":"reader","value":{"Symbol":"x"}}}"#);
    }
    for input in [r#"{"Literal":{"Map":{"sorted":true,"entries":[["Null",{"I64":1}]]}}}"#, r#"{"Literal":{"Map":{"entries":[["Null",{"I64":1}]],"sorted":true}}}"#] {
        assert_eq!(serde_json::to_string(&serde_json::from_str::<RegexForm>(input).unwrap()).unwrap(), r#"{"Literal":{"Map":{"entries":[["Null",{"I64":1}]],"sorted":true}}}"#);
    }
    let value = serde_json::from_str::<RegexForm>(r#"{"Literal":{"Struct":{"b":{"I64":2},"a":{"I64":1},"b":{"I64":3}}}}"#).unwrap();
    assert_eq!(format!("{value:?}"), r#"Literal(Struct({"a": I64(1), "b": I64(3)}))"#);
}

fn physical_values() -> Vec<Value> {
    vec![Value::Undefined, Value::Null, Value::Bool(true), Value::I8(-8), Value::I16(-16), Value::I32(-32), Value::I64(-64),
        Value::U8(8), Value::U16(16), Value::U32(32), Value::U64(64), Value::F16(0x8000), Value::F32(-0.0), Value::F64(1.25),
        Value::Utf8("é\n😀".into()), Value::Binary(vec![0,255]), Value::FixedSizeBinary(vec![3]),
        Value::Decimal(DecimalValue { unscaled: vec![255,0], precision: 3, scale: -2, bit_width: 16 }),
        Value::Date(CalendarDate::new(CivilDate::from_ymd(1970,1,1).unwrap(), DateUnit::Day)), Value::DateDay(-1), Value::DateMilli(2),
        Value::Time { value: 3, unit: TimeUnit::Second, bit_width: 64 }, Value::Timestamp { value: 4, unit: TimeUnit::Nanosecond, timezone: Some("UTC".into()) },
        Value::Duration { value: 5, unit: TimeUnit::Microsecond }, Value::IntervalYearMonth(6), Value::IntervalDayTime { days: 7, milliseconds: 8 },
        Value::IntervalMonthDayNano { months: 1, days: 2, nanoseconds: 3 }, Value::IntervalMonthDayMicro { months: 4, days: 5, nanoseconds: 6 },
        Value::Keyword(Keyword::from_name(":x")), Value::Uuid([17;16]), Value::Uri("unvalidated:%%".into()), Value::Transit(vec![0,128,255]),
        Value::Oid(1), Value::RegClass(2), Value::RegProc(3), Value::list(vec![Value::I64(1)]), Value::Set(vec![Value::Null,Value::Null]),
        Value::Map { entries: vec![(Value::I64(1),Value::Bool(true))], sorted: false }, Value::FixedSizeList(vec![Value::Bool(false)].into()),
        Value::TimestampTzRange { lower: -1, upper: 2 }, Value::Struct(StructFields::from([("a".into(), Value::I64(1))]))]
}
struct LegacyLiteral<'a>(&'a Value);
impl fmt::Debug for LegacyLiteral<'_> { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_tuple("Literal").field(self.0).finish() } }

#[test]
fn all_physical_payloads_copy_exactly_and_debug_atoms_keep_original_formatter_options() {
    let values = physical_values();
    assert_eq!(values.len(), 41);
    let memory = MemoryBudget::new(8 << 20);
    for value in &values {
        let source = literal(value);
        let copied = RegexForm::copy_in(source.view(), &memory).unwrap();
        assert_eq!(serde_json::to_string(&copied).unwrap(), format!(r#"{{"Literal":{}}}"#, serde_json::to_string(value).unwrap()));
        let expected = LegacyLiteral(value);
        assert_eq!(format!("{copied:?}"), format!("{expected:?}"));
        assert_eq!(format!("{copied:#?}"), format!("{expected:#?}"));
        assert_eq!(format!("{copied:>32.2?}"), format!("{expected:>32.2?}"));
        assert_eq!(format!("{copied:+08x?}"), format!("{expected:+08x?}"));
        assert_eq!(format!("{copied:#X?}"), format!("{expected:#X?}"));
        assert_eq!(format!("{copied:\n>#4?}"), format!("{expected:\n>#4?}"));
    }
    assert_eq!(memory.used_bytes(), 0);
    let value = literal(&Value::I64(7));
    assert_eq!(format!("{value:\n>#4?}"), "Literal(\n    I64(\n        \n        \n        \n        7,\n    ),\n)");
}

#[test]
fn twenty_thousand_mixed_flat_nodes_copy_compare_debug_and_drop_without_owner_recursion() {
    const DEPTH: usize = 20_000;
    let mut builder = RegexFormBuilder::new_untracked();
    for _ in 0..DEPTH { builder.begin_list().unwrap(); }
    builder.begin_literal().unwrap();
    for level in 0..DEPTH {
        match level % 5 {
            0 => builder.begin_value_list().unwrap(), 1 => builder.begin_value_set().unwrap(), 2 => builder.begin_value_fixed_size_list().unwrap(),
            3 => builder.begin_value_map(false).unwrap(), _ => { builder.begin_value_struct().unwrap(); builder.field("nested").unwrap(); }
        }
    }
    builder.value_leaf(&Value::I64(7)).unwrap();
    for level in (0..DEPTH).rev() { if level % 5 == 3 { builder.value_leaf(&Value::Null).unwrap(); } builder.end().unwrap(); }
    builder.end().unwrap();
    for _ in 0..DEPTH { builder.end().unwrap(); }
    let source = builder.finish().unwrap();
    let memory = MemoryBudget::new(128 << 20);
    let copied = RegexForm::copy_in(source.view(), &memory).unwrap();
    assert_eq!(source, copied);
    let text = format!("{copied:?}");
    assert_eq!(text.matches("I64(7)").count(), 1);
    assert_eq!(text.matches("nested").count(), DEPTH / 5);
    drop(source); drop(copied);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn deep_duplicate_struct_loser_drops_payloads_iteratively_before_flat_publication() {
    let memory = MemoryBudget::new(64 << 20);
    let mut builder = RegexFormBuilder::new_in(&memory);
    builder.begin_literal().unwrap(); builder.begin_value_struct().unwrap(); builder.field("same").unwrap();
    for _ in 0..20_000 { builder.begin_value_struct().unwrap(); builder.field("nested").unwrap(); }
    builder.value_text(&"x".repeat(1 << 20)).unwrap();
    for _ in 0..20_000 { builder.end().unwrap(); }
    builder.field("same").unwrap(); builder.value_leaf(&Value::I64(7)).unwrap();
    builder.end().unwrap(); builder.end().unwrap();
    let value = builder.finish().unwrap();
    assert_eq!(format!("{value:?}"), "Literal(Struct({\"same\": I64(7)}))");
    assert_eq!(value, literal(&Value::Struct(StructFields::from([("same".into(), Value::I64(7))]))));
    let texts = &value.storage().unwrap().texts;
    assert_eq!(texts.iter().filter(|text| !text.is_empty()).count(), 1);
    assert!(texts.iter().all(|text| text == "same" || text.capacity() == 0));
    assert!(value.retained_bytes().unwrap() > value.work_bound().unwrap());
    drop(value);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn ordinary_alternate_debug_stops_on_bounded_writer_failure_at_twenty_thousand_depth() {
    use std::fmt::Write;
    struct Prefix { text: String, remaining: usize }
    impl fmt::Write for Prefix {
        fn write_str(&mut self, value: &str) -> fmt::Result {
            if value.len() > self.remaining { return Err(fmt::Error); }
            self.text.push_str(value); self.remaining -= value.len(); Ok(())
        }
    }
    let mut builder = RegexFormBuilder::new_untracked();
    for _ in 0..20_000 { builder.begin_list().unwrap(); }
    builder.literal(&Value::Null).unwrap();
    for _ in 0..20_000 { builder.end().unwrap(); }
    let value = builder.finish().unwrap();
    let mut writer = Prefix { text: String::new(), remaining: 96 };
    assert!(write!(&mut writer, "{value:#?}").is_err());
    assert!(writer.text.starts_with("List(\n    [\n        List(\n"));
    assert!(writer.text.len() <= 96);
}
