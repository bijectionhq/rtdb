use rtdb_types::StructFields;
use super::*;
use crate::RegexForm;
use rtdb_types::{CalendarDate, CivilDate, DateUnit, DecimalValue, Keyword, TimeUnit};

fn form_label(form: RegexFormView<'_>) -> &'static str {
    match form.kind() {
        RegexFormKind::Literal(_) => "literal", RegexFormKind::Character(_) => "character", RegexFormKind::Symbol(_) => "symbol",
        RegexFormKind::BigInteger(_) => "big-integer", RegexFormKind::Decimal(_) => "decimal", RegexFormKind::List(_) => "list",
        RegexFormKind::Vector(_) => "vector", RegexFormKind::Set(_) => "set", RegexFormKind::Map(_) => "map",
        RegexFormKind::Tagged { .. } => "tagged", RegexFormKind::AnonymousFunction(_) => "anonymous",
    }
}

fn literal_label(value: RegexLiteralView<'_>) -> &'static str {
    match value.kind() {
        RegexLiteralKind::Text(_) => "text", RegexLiteralKind::Keyword(_) => "keyword", RegexLiteralKind::Leaf(_) => "leaf",
        RegexLiteralKind::List(_) => "list", RegexLiteralKind::Set(_) => "set", RegexLiteralKind::Map { .. } => "map",
        RegexLiteralKind::FixedSizeList(_) => "fixed-size-list", RegexLiteralKind::Struct(_) => "struct",
    }
}

fn symbol<'a>(form: RegexFormView<'a>) -> &'a str {
    match form.kind() { RegexFormKind::Symbol(value) => value, _ => panic!("expected a symbol") }
}

fn text<'a>(value: RegexLiteralView<'a>) -> &'a str {
    match value.kind() { RegexLiteralKind::Text(value) => value, _ => panic!("expected text") }
}

fn first_form<'a>(source: &'a RegexForm) -> Option<RegexFormView<'a>> {
    match source.view().kind() { RegexFormKind::List(values) => values.get(0), _ => None }
}

fn first_literal<'a>(source: &'a Value) -> Option<RegexLiteralView<'a>> {
    match RegexLiteralView::new(source).kind() { RegexLiteralKind::List(values) => values.get(0), _ => None }
}

fn first_field<'a>(source: &'a Value) -> Option<(&'a str, RegexLiteralView<'a>)> {
    match RegexLiteralView::new(source).kind() { RegexLiteralKind::Struct(fields) => fields.iter().next(), _ => None }
}

fn cursor_traits<I: Iterator + DoubleEndedIterator + ExactSizeIterator + FusedIterator + Clone>(_: &I) {}

#[test]
fn all_eleven_raw_kinds_keep_their_source_distinctions() {
    let cases = [
        (raw_literal(Value::Null), "literal"), (raw_character('😀'), "character"),
        (raw_symbol("ns/name".into()), "symbol"), (raw_big_integer("00099".into()), "big-integer"),
        (raw_decimal("1.00M".into()), "decimal"), (raw_list(vec![]), "list"),
        (raw_vector(vec![]), "vector"), (raw_set(vec![]), "set"), (raw_map(vec![]), "map"),
        (raw_tagged("reader/tag".into(), raw_symbol("body".into())), "tagged"),
        (raw_anonymous(raw_symbol("%".into())), "anonymous"),
    ];
    assert_eq!(cases.len(), 11);
    for (source, label) in &cases { assert_eq!(form_label(source.view()), *label); }
    assert!(matches!(cases[1].0.view().kind(), RegexFormKind::Character('😀')));
    assert!(matches!(cases[3].0.view().kind(), RegexFormKind::BigInteger("00099")));
    assert!(matches!(cases[4].0.view().kind(), RegexFormKind::Decimal("1.00M")));
    assert!(matches!(cases[9].0.view().kind(), RegexFormKind::Tagged { tag: "reader/tag", value } if symbol(value) == "body"));
    assert!(matches!(cases[10].0.view().kind(), RegexFormKind::AnonymousFunction(value) if symbol(value) == "%"));
}

#[test]
fn all_forty_one_physical_kinds_have_exactly_thirty_four_borrowed_leaves() {
    let cases = vec![
        (Value::Undefined, "leaf"), (Value::Null, "leaf"), (Value::Bool(true), "leaf"),
        (Value::I8(-8), "leaf"), (Value::I16(-16), "leaf"), (Value::I32(-32), "leaf"), (Value::I64(-64), "leaf"),
        (Value::U8(8), "leaf"), (Value::U16(16), "leaf"), (Value::U32(32), "leaf"), (Value::U64(u64::MAX), "leaf"),
        (Value::F16(0x8000), "leaf"), (Value::F32(f32::from_bits(0x7fc00001)), "leaf"), (Value::F64(f64::from_bits(0x7ff8000000000001)), "leaf"),
        (Value::Utf8("é😀\n".into()), "text"), (Value::Binary(vec![0, 255]), "leaf"), (Value::FixedSizeBinary(vec![9]), "leaf"),
        (Value::Decimal(DecimalValue { unscaled: vec![255, 0], precision: 4, scale: -2, bit_width: 16 }), "leaf"),
        (Value::Date(CalendarDate::new(CivilDate::from_ymd(2000, 1, 2).unwrap(), DateUnit::Day)), "leaf"),
        (Value::DateDay(-1), "leaf"), (Value::DateMilli(-1000), "leaf"),
        (Value::Time { value: 7, unit: TimeUnit::Microsecond, bit_width: 64 }, "leaf"),
        (Value::Timestamp { value: -7, unit: TimeUnit::Nanosecond, timezone: None }, "leaf"),
        (Value::Duration { value: 8, unit: TimeUnit::Second }, "leaf"), (Value::IntervalYearMonth(-12), "leaf"),
        (Value::IntervalDayTime { days: -1, milliseconds: 3 }, "leaf"),
        (Value::IntervalMonthDayNano { months: 1, days: 2, nanoseconds: 3 }, "leaf"),
        (Value::IntervalMonthDayMicro { months: 4, days: 5, nanoseconds: 6 }, "leaf"),
        (Value::Keyword(Keyword::from_name("plain/name")), "keyword"), (Value::Uuid([0; 16]), "leaf"),
        (Value::Uri("https://example.test/é".into()), "leaf"), (Value::Transit(vec![0, 255]), "leaf"),
        (Value::Oid(-1), "leaf"), (Value::RegClass(2), "leaf"), (Value::RegProc(3), "leaf"),
        (Value::list(vec![Value::I64(1)]), "list"), (Value::Set(vec![Value::Null]), "set"),
        (Value::Map { entries: vec![(Value::Null, Value::Bool(true))], sorted: true }, "map"),
        (Value::FixedSizeList(vec![Value::Bool(false)].into()), "fixed-size-list"),
        (Value::TimestampTzRange { lower: -1, upper: 2 }, "leaf"), (Value::Struct(StructFields::new()), "struct"),
    ];
    assert_eq!(cases.len(), 41);
    let mut leaves = 0;
    for (source, label) in &cases {
        let view = RegexLiteralView::new(source);
        assert_eq!(literal_label(view), *label);
        if let RegexLiteralKind::Leaf(leaf) = view.kind() { assert!(std::ptr::eq(leaf.value().as_ref(), source)); leaves += 1; }
    }
    assert_eq!(leaves, 34);
    assert!(matches!(RegexLiteralView::new(&cases[12].0).kind(), RegexLiteralKind::Leaf(leaf) if matches!(leaf.value().as_ref(), Value::F32(value) if value.to_bits() == 0x7fc00001)));
    assert!(matches!(RegexLiteralView::new(&cases[13].0).kind(), RegexLiteralKind::Leaf(leaf) if matches!(leaf.value().as_ref(), Value::F64(value) if value.to_bits() == 0x7ff8000000000001)));
    assert!(matches!(RegexLiteralView::new(&cases[28].0).kind(), RegexLiteralKind::Keyword("plain/name")));
}

#[test]
fn child_and_text_borrows_outlive_temporary_views_and_cursors() {
    let raw = raw_list(vec![raw_symbol("borrowed".into())]);
    let child = first_form(&raw).unwrap();
    let storage = match &raw.0 {
        super::super::Backing::Tracked { storage, .. } | super::super::Backing::Untracked(storage) => storage.as_ref(),
        super::super::Backing::InlineNull => panic!("expected list"),
    };
    let super::super::storage::RawKind::List(children) = &storage.forms[0].kind else { panic!("expected list") };
    let child_id = storage.raw_children[children.start];
    let super::super::storage::RawKind::Symbol(text_id) = &storage.forms[child_id.0].kind else { panic!("expected symbol") };
    let source = &storage.texts[text_id.0];
    assert_eq!(symbol(child).as_ptr(), source.as_ptr());
    let values = Value::list(vec![Value::Utf8("physical".into())]);
    let value = first_literal(&values).unwrap();
    let Value::List(children) = &values else { panic!("expected list") };
    let child = children.get(0).unwrap();
    let Value::Utf8(source) = child.as_ref() else { panic!("expected text") };
    assert_eq!(text(value).as_ptr(), source.as_ptr());
    let fields = Value::Struct(StructFields::from([("field".into(), Value::I64(1))]));
    let (name, value) = first_field(&fields).unwrap();
    let Value::Struct(source) = &fields else { panic!("expected struct") };
    let (source_name, source_value) = source.first_key_value().unwrap();
    assert_eq!(name.as_ptr(), source_name.as_ptr());
    assert!(matches!(value.kind(), RegexLiteralKind::Leaf(leaf) if std::ptr::eq(leaf.value().as_ref(), source_value)));
}

#[test]
fn raw_sequences_keep_bounds_ranges_and_independent_exact_cursors() {
    let source = raw_vector(vec![raw_symbol("a".into()), raw_symbol("b".into()), raw_symbol("c".into())]);
    let RegexFormKind::Vector(values) = source.view().kind() else { panic!("expected vector") };
    assert_eq!(values.len(), 3);
    assert!(!values.is_empty());
    assert!(values.get(3).is_none());
    assert!(values.get(usize::MAX).is_none());
    assert_eq!(symbol(values.at(1)), "b");
    assert_eq!(symbol(values.first().unwrap()), "a");
    let (first, rest) = values.split_first().unwrap();
    assert_eq!(symbol(first), "a");
    assert_eq!(rest.len(), 2);
    assert_eq!(symbol(rest.at(0)), "b");
    assert_eq!(symbol(values.slice(1..3).at(1)), "c");
    assert!(values.slice(0..0).is_empty());
    assert!(values.slice(3..3).is_empty());
    let mut cursor = values.iter();
    cursor_traits(&cursor);
    assert_eq!(cursor.size_hint(), (3, Some(3)));
    assert_eq!(cursor.next().map(symbol), Some("a"));
    let mut cloned = cursor.clone();
    assert_eq!(cursor.len(), 2);
    assert_eq!(cursor.next_back().map(symbol), Some("c"));
    assert_eq!(cursor.size_hint(), (1, Some(1)));
    assert_eq!(cloned.next().map(symbol), Some("b"));
    assert_eq!(cloned.next_back().map(symbol), Some("c"));
    assert_eq!(cursor.next().map(symbol), Some("b"));
    assert_eq!(cursor.len(), 0);
    assert!(cursor.next().is_none());
    assert!(cursor.next_back().is_none());
    assert!(cursor.next().is_none());
}

#[test]
fn empty_raw_sequence_has_no_first_or_split_child() {
    let source = raw_list(vec![]);
    let RegexFormKind::List(values) = source.view().kind() else { panic!("expected list") };
    assert!(values.is_empty());
    assert!(values.first().is_none());
    assert!(values.split_first().is_none());
    assert!(values.get(0).is_none());
    assert!(values.slice(0..0).is_empty());
    let mut cursor = values.iter();
    assert_eq!(cursor.size_hint(), (0, Some(0)));
    assert!(cursor.next().is_none());
    assert!(cursor.next_back().is_none());
    let source = raw_map(vec![]);
    let RegexFormKind::Map(entries) = source.view().kind() else { panic!("expected map") };
    assert!(entries.is_empty());
    assert_eq!(entries.len(), 0);
    assert!(entries.get(0).is_none());
    let mut cursor = entries.iter();
    assert_eq!(cursor.size_hint(), (0, Some(0)));
    assert!(cursor.next().is_none());
    assert!(cursor.next_back().is_none());
}

#[test]
#[should_panic]
fn raw_sequence_at_len_panics_like_slice_indexing() {
    let source = raw_list(vec![]);
    let RegexFormKind::List(values) = source.view().kind() else { panic!("expected list") };
    values.at(0);
}

#[test]
#[should_panic]
fn raw_sequence_reversed_range_panics_like_slice_indexing() {
    let source = raw_list(vec![RegexForm::default()]);
    let RegexFormKind::List(values) = source.view().kind() else { panic!("expected list") };
    let start = 1;
    let end = 0;
    values.slice(start..end);
}

#[test]
#[should_panic]
fn raw_sequence_range_past_end_panics_like_slice_indexing() {
    let source = raw_list(vec![]);
    let RegexFormKind::List(values) = source.view().kind() else { panic!("expected list") };
    values.slice(0..1);
}

#[test]
fn raw_pairs_preserve_entry_order_and_source_lifetimes() {
    let source = raw_map(vec![(raw_symbol("z".into()), raw_symbol("first".into())), (raw_symbol("a".into()), raw_symbol("last".into()))]);
    let RegexFormKind::Map(entries) = source.view().kind() else { panic!("expected map") };
    assert_eq!(entries.len(), 2);
    assert!(!entries.is_empty());
    assert!(entries.get(usize::MAX).is_none());
    let (key, value) = entries.get(0).unwrap();
    assert_eq!((symbol(key), symbol(value)), ("z", "first"));
    let mut cursor = entries.iter();
    cursor_traits(&cursor);
    assert_eq!(cursor.size_hint(), (2, Some(2)));
    assert_eq!(cursor.next_back().map(|(key, value)| (symbol(key), symbol(value))), Some(("a", "last")));
    assert_eq!(cursor.len(), 1);
    assert_eq!(cursor.clone().count(), 1);
    assert_eq!(cursor.next().map(|(key, value)| (symbol(key), symbol(value))), Some(("z", "first")));
    assert!(cursor.next().is_none());
    assert!(cursor.next_back().is_none());
}

#[test]
fn physical_sequence_and_pair_cursors_borrow_exact_children() {
    for source in [Value::list(vec![Value::Utf8("a".into()), Value::Utf8("b".into())]), Value::Set(vec![Value::Utf8("a".into()), Value::Utf8("b".into())]), Value::FixedSizeList(vec![Value::Utf8("a".into()), Value::Utf8("b".into())].into())] {
        let values = match RegexLiteralView::new(&source).kind() { RegexLiteralKind::List(values) | RegexLiteralKind::Set(values) | RegexLiteralKind::FixedSizeList(values) => values, _ => panic!("expected sequence") };
        assert_eq!(values.len(), 2);
        assert!(!values.is_empty());
        assert!(values.get(usize::MAX).is_none());
        assert_eq!(text(values.get(0).unwrap()), "a");
        let mut cursor = values.iter();
        cursor_traits(&cursor);
        assert_eq!(cursor.size_hint(), (2, Some(2)));
        assert_eq!(cursor.next_back().map(text), Some("b"));
        assert_eq!(cursor.len(), 1);
        assert_eq!(cursor.clone().next().map(text), Some("a"));
        assert_eq!(cursor.next().map(text), Some("a"));
        assert!(cursor.next().is_none());
        assert!(cursor.next_back().is_none());
    }
    let source = Value::Map { entries: vec![(Value::Utf8("z".into()), Value::Utf8("first".into())), (Value::Utf8("a".into()), Value::Utf8("last".into()))], sorted: true };
    let RegexLiteralKind::Map { entries, sorted } = RegexLiteralView::new(&source).kind() else { panic!("expected map") };
    assert!(sorted);
    assert_eq!(entries.len(), 2);
    assert!(!entries.is_empty());
    assert!(entries.get(usize::MAX).is_none());
    assert_eq!(entries.get(0).map(|(key, value)| (text(key), text(value))), Some(("z", "first")));
    let mut cursor = entries.iter();
    cursor_traits(&cursor);
    assert_eq!(cursor.size_hint(), (2, Some(2)));
    assert_eq!(cursor.next_back().map(|(key, value)| (text(key), text(value))), Some(("a", "last")));
    assert_eq!(cursor.len(), 1);
    assert_eq!(cursor.clone().count(), 1);
    assert_eq!(cursor.next().map(|(key, value)| (text(key), text(value))), Some(("z", "first")));
    assert!(cursor.next_back().is_none());
    assert!(cursor.next().is_none());
}

#[test]
fn field_cursors_preserve_insertion_order_and_borrow_names_and_values() {
    let source = Value::Struct(StructFields::from([("a".into(), Value::Utf8("first".into())), ("z".into(), Value::Utf8("last".into()))]));
    let RegexLiteralKind::Struct(fields) = RegexLiteralView::new(&source).kind() else { panic!("expected fields") };
    assert_eq!(fields.len(), 2);
    assert!(!fields.is_empty());
    let mut cursor = fields.iter();
    cursor_traits(&cursor);
    assert_eq!(cursor.size_hint(), (2, Some(2)));
    assert_eq!(cursor.next_back().map(|(name, value)| (name, text(value))), Some(("z", "last")));
    assert_eq!(cursor.len(), 1);
    assert_eq!(cursor.clone().next().map(|(name, value)| (name, text(value))), Some(("a", "first")));
    assert_eq!(cursor.next().map(|(name, value)| (name, text(value))), Some(("a", "first")));
    assert_eq!(cursor.size_hint(), (0, Some(0)));
    assert!(cursor.next().is_none());
    assert!(cursor.next_back().is_none());
}

#[test]
fn empty_physical_containers_have_exact_exhausted_cursors() {
    let sequence = Value::list(vec![]);
    let RegexLiteralKind::List(values) = RegexLiteralView::new(&sequence).kind() else { panic!("expected list") };
    assert!(values.is_empty());
    assert_eq!(values.len(), 0);
    assert!(values.get(0).is_none());
    let mut cursor = values.iter();
    assert_eq!(cursor.len(), 0);
    assert!(cursor.next().is_none());
    assert!(cursor.next_back().is_none());
    let map = Value::Map { entries: vec![], sorted: false };
    let RegexLiteralKind::Map { entries, sorted } = RegexLiteralView::new(&map).kind() else { panic!("expected map") };
    assert!(!sorted);
    assert!(entries.is_empty());
    assert_eq!(entries.len(), 0);
    assert!(entries.get(0).is_none());
    let mut cursor = entries.iter();
    assert_eq!(cursor.len(), 0);
    assert!(cursor.next().is_none());
    assert!(cursor.next_back().is_none());
    let fields = Value::Struct(StructFields::new());
    let RegexLiteralKind::Struct(fields) = RegexLiteralView::new(&fields).kind() else { panic!("expected fields") };
    assert!(fields.is_empty());
    assert_eq!(fields.len(), 0);
    let mut cursor = fields.iter();
    assert_eq!(cursor.len(), 0);
    assert!(cursor.next().is_none());
    assert!(cursor.next_back().is_none());
}

fn raw_build(emit: impl FnOnce(&mut crate::RegexFormBuilder<'_>) -> rtdb_types::Result<()>) -> crate::RegexForm {
    let mut builder = crate::RegexFormBuilder::new_untracked();
    emit(&mut builder).unwrap();
    builder.finish().unwrap()
}
fn raw_literal(value: rtdb_types::Value) -> crate::RegexForm { raw_build(|builder| builder.literal(&value)) }
fn raw_character(value: char) -> crate::RegexForm { raw_build(|builder| builder.character(value)) }
fn raw_symbol(value: String) -> crate::RegexForm { raw_build(|builder| builder.symbol(&value)) }
fn raw_big_integer(value: String) -> crate::RegexForm { raw_build(|builder| builder.big_integer(&value)) }
fn raw_decimal(value: String) -> crate::RegexForm { raw_build(|builder| builder.decimal(&value)) }
fn raw_list(values: Vec<crate::RegexForm>) -> crate::RegexForm { raw_build(|builder| { builder.begin_list()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
fn raw_vector(values: Vec<crate::RegexForm>) -> crate::RegexForm { raw_build(|builder| { builder.begin_vector()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
fn raw_set(values: Vec<crate::RegexForm>) -> crate::RegexForm { raw_build(|builder| { builder.begin_set()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
fn raw_map(values: Vec<(crate::RegexForm, crate::RegexForm)>) -> crate::RegexForm { raw_build(|builder| { builder.begin_map()?; for (key, value) in values { builder.append_view(key.view())?; builder.append_view(value.view())?; } builder.end() }) }
fn raw_tagged(tag: String, value: crate::RegexForm) -> crate::RegexForm { raw_build(|builder| { builder.begin_tagged(&tag)?; builder.append_view(value.view())?; builder.end() }) }
fn raw_anonymous(value: crate::RegexForm) -> crate::RegexForm { raw_build(|builder| { builder.begin_anonymous()?; builder.append_view(value.view())?; builder.end() }) }
