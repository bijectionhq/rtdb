use rtdb_types::StructFields;
use std::{cell::Cell, fmt};
use rtdb_plan::{RegexForm as F, RegexReplacement};
use rtdb_types::{CalendarDate, CivilDate, DateUnit, DecimalValue, Keyword, TimeUnit, Value as V};
use serde::{Serialize, Serializer, ser::{SerializeMap, SerializeSeq}};

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Bool(bool), I8(i8), I16(i16), I32(i32), I64(i64), I128(i128),
    U8(u8), U16(u16), U32(u32), U64(u64), U128(u128), F32(u32), F64(u64),
    Char(char), Str(String), Bytes(Vec<u8>), OptionNone, OptionSome, Unit,
    UnitStruct(&'static str), UnitVariant(&'static str, u32, &'static str),
    NewtypeStruct(&'static str), NewtypeVariant(&'static str, u32, &'static str),
    CollectSeq, Seq(Option<usize>), SeqElement, SeqEnd,
    Tuple(usize), TupleElement, TupleEnd,
    TupleStruct(&'static str, usize), TupleField, TupleStructEnd,
    TupleVariant(&'static str, u32, &'static str, usize), TupleVariantField, TupleVariantEnd,
    CollectMap, Map(Option<usize>), MapKey, MapValue, MapEntry, MapEnd,
    Struct(&'static str, usize), Field(&'static str), StructEnd,
    StructVariant(&'static str, u32, &'static str, usize), VariantField(&'static str), StructVariantEnd,
    CollectStr(String),
}
use Event::*;

#[derive(Debug)]
struct ProbeError(String);
impl fmt::Display for ProbeError { fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result { output.write_str(&self.0) } }
impl std::error::Error for ProbeError {}
impl serde::ser::Error for ProbeError { fn custom<T: fmt::Display>(message: T) -> Self { Self(message.to_string()) } }
type ProbeResult<T> = Result<T, ProbeError>;

struct SerializerProbe { events: Vec<Event>, human_readable: bool, readable_queries: Cell<usize> }

impl<'a> Serializer for &'a mut SerializerProbe {
    type Ok = ();
    type Error = ProbeError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, value: bool) -> ProbeResult<()> { self.events.push(Bool(value)); Ok(()) }
    fn serialize_i8(self, value: i8) -> ProbeResult<()> { self.events.push(I8(value)); Ok(()) }
    fn serialize_i16(self, value: i16) -> ProbeResult<()> { self.events.push(I16(value)); Ok(()) }
    fn serialize_i32(self, value: i32) -> ProbeResult<()> { self.events.push(I32(value)); Ok(()) }
    fn serialize_i64(self, value: i64) -> ProbeResult<()> { self.events.push(I64(value)); Ok(()) }
    fn serialize_i128(self, value: i128) -> ProbeResult<()> { self.events.push(I128(value)); Ok(()) }
    fn serialize_u8(self, value: u8) -> ProbeResult<()> { self.events.push(U8(value)); Ok(()) }
    fn serialize_u16(self, value: u16) -> ProbeResult<()> { self.events.push(U16(value)); Ok(()) }
    fn serialize_u32(self, value: u32) -> ProbeResult<()> { self.events.push(U32(value)); Ok(()) }
    fn serialize_u64(self, value: u64) -> ProbeResult<()> { self.events.push(U64(value)); Ok(()) }
    fn serialize_u128(self, value: u128) -> ProbeResult<()> { self.events.push(U128(value)); Ok(()) }
    fn serialize_f32(self, value: f32) -> ProbeResult<()> { self.events.push(F32(value.to_bits())); Ok(()) }
    fn serialize_f64(self, value: f64) -> ProbeResult<()> { self.events.push(F64(value.to_bits())); Ok(()) }
    fn serialize_char(self, value: char) -> ProbeResult<()> { self.events.push(Char(value)); Ok(()) }
    fn serialize_str(self, value: &str) -> ProbeResult<()> { self.events.push(Str(value.into())); Ok(()) }
    fn serialize_bytes(self, value: &[u8]) -> ProbeResult<()> { self.events.push(Bytes(value.into())); Ok(()) }
    fn serialize_none(self) -> ProbeResult<()> { self.events.push(OptionNone); Ok(()) }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> ProbeResult<()> { self.events.push(OptionSome); value.serialize(self) }
    fn serialize_unit(self) -> ProbeResult<()> { self.events.push(Unit); Ok(()) }
    fn serialize_unit_struct(self, name: &'static str) -> ProbeResult<()> { self.events.push(UnitStruct(name)); Ok(()) }
    fn serialize_unit_variant(self, name: &'static str, index: u32, variant: &'static str) -> ProbeResult<()> { self.events.push(UnitVariant(name, index, variant)); Ok(()) }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(self, name: &'static str, value: &T) -> ProbeResult<()> { self.events.push(NewtypeStruct(name)); value.serialize(self) }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(self, name: &'static str, index: u32, variant: &'static str, value: &T) -> ProbeResult<()> { self.events.push(NewtypeVariant(name, index, variant)); value.serialize(self) }
    fn serialize_seq(self, length: Option<usize>) -> ProbeResult<Self::SerializeSeq> { self.events.push(Seq(length)); Ok(self) }
    fn serialize_tuple(self, length: usize) -> ProbeResult<Self::SerializeTuple> { self.events.push(Tuple(length)); Ok(self) }
    fn serialize_tuple_struct(self, name: &'static str, length: usize) -> ProbeResult<Self::SerializeTupleStruct> { self.events.push(TupleStruct(name, length)); Ok(self) }
    fn serialize_tuple_variant(self, name: &'static str, index: u32, variant: &'static str, length: usize) -> ProbeResult<Self::SerializeTupleVariant> { self.events.push(TupleVariant(name, index, variant, length)); Ok(self) }
    fn serialize_map(self, length: Option<usize>) -> ProbeResult<Self::SerializeMap> { self.events.push(Map(length)); Ok(self) }
    fn serialize_struct(self, name: &'static str, length: usize) -> ProbeResult<Self::SerializeStruct> { self.events.push(Struct(name, length)); Ok(self) }
    fn serialize_struct_variant(self, name: &'static str, index: u32, variant: &'static str, length: usize) -> ProbeResult<Self::SerializeStructVariant> { self.events.push(StructVariant(name, index, variant, length)); Ok(self) }
    fn collect_seq<I: IntoIterator>(self, values: I) -> ProbeResult<()> where I::Item: Serialize {
        self.events.push(CollectSeq);
        let values = values.into_iter();
        let (lower, upper) = values.size_hint();
        let mut sequence = self.serialize_seq((upper == Some(lower)).then_some(lower))?;
        for value in values { SerializeSeq::serialize_element(&mut sequence, &value)?; }
        SerializeSeq::end(sequence)
    }
    fn collect_map<K: Serialize, V: Serialize, I: IntoIterator<Item = (K, V)>>(self, entries: I) -> ProbeResult<()> {
        self.events.push(CollectMap);
        let entries = entries.into_iter();
        let (lower, upper) = entries.size_hint();
        let mut map = self.serialize_map((upper == Some(lower)).then_some(lower))?;
        for (key, value) in entries { SerializeMap::serialize_entry(&mut map, &key, &value)?; }
        SerializeMap::end(map)
    }
    fn collect_str<T: ?Sized + fmt::Display>(self, value: &T) -> ProbeResult<()> { self.events.push(CollectStr(value.to_string())); Ok(()) }
    fn is_human_readable(&self) -> bool { self.readable_queries.set(self.readable_queries.get() + 1); self.human_readable }
}

impl serde::ser::SerializeSeq for &mut SerializerProbe {
    type Ok = (); type Error = ProbeError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> ProbeResult<()> { self.events.push(SeqElement); value.serialize(&mut **self) }
    fn end(self) -> ProbeResult<()> { self.events.push(SeqEnd); Ok(()) }
}
impl serde::ser::SerializeTuple for &mut SerializerProbe {
    type Ok = (); type Error = ProbeError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> ProbeResult<()> { self.events.push(TupleElement); value.serialize(&mut **self) }
    fn end(self) -> ProbeResult<()> { self.events.push(TupleEnd); Ok(()) }
}
impl serde::ser::SerializeTupleStruct for &mut SerializerProbe {
    type Ok = (); type Error = ProbeError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> ProbeResult<()> { self.events.push(TupleField); value.serialize(&mut **self) }
    fn end(self) -> ProbeResult<()> { self.events.push(TupleStructEnd); Ok(()) }
}
impl serde::ser::SerializeTupleVariant for &mut SerializerProbe {
    type Ok = (); type Error = ProbeError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> ProbeResult<()> { self.events.push(TupleVariantField); value.serialize(&mut **self) }
    fn end(self) -> ProbeResult<()> { self.events.push(TupleVariantEnd); Ok(()) }
}
impl serde::ser::SerializeMap for &mut SerializerProbe {
    type Ok = (); type Error = ProbeError;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, value: &T) -> ProbeResult<()> { self.events.push(MapKey); value.serialize(&mut **self) }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> ProbeResult<()> { self.events.push(MapValue); value.serialize(&mut **self) }
    fn serialize_entry<K: ?Sized + Serialize, V: ?Sized + Serialize>(&mut self, key: &K, value: &V) -> ProbeResult<()> { self.events.push(MapEntry); key.serialize(&mut **self)?; value.serialize(&mut **self) }
    fn end(self) -> ProbeResult<()> { self.events.push(MapEnd); Ok(()) }
}
impl serde::ser::SerializeStruct for &mut SerializerProbe {
    type Ok = (); type Error = ProbeError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, name: &'static str, value: &T) -> ProbeResult<()> { self.events.push(Field(name)); value.serialize(&mut **self) }
    fn end(self) -> ProbeResult<()> { self.events.push(StructEnd); Ok(()) }
}
impl serde::ser::SerializeStructVariant for &mut SerializerProbe {
    type Ok = (); type Error = ProbeError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, name: &'static str, value: &T) -> ProbeResult<()> { self.events.push(VariantField(name)); value.serialize(&mut **self) }
    fn end(self) -> ProbeResult<()> { self.events.push(StructVariantEnd); Ok(()) }
}

fn s(text: &str) -> Event { Str(text.into()) }
fn trace(value: &impl Serialize, human_readable: bool) -> Vec<Event> {
    let mut probe = SerializerProbe { events: Vec::new(), human_readable, readable_queries: Cell::new(0) };
    value.serialize(&mut probe).unwrap();
    assert_eq!(probe.readable_queries.get(), 0);
    probe.events
}
fn literal_events(body: Vec<Event>) -> Vec<Event> {
    let mut events = vec![NewtypeVariant("RegexForm", 0, "Literal")];
    events.extend(body);
    events
}

#[test]
fn raw_variants_keep_generic_serde_method_names_indices_and_payloads() {
    let cases = vec![
        (raw_literal(V::Null), vec![NewtypeVariant("RegexForm", 0, "Literal"), UnitVariant("Value", 1, "Null")]),
        (raw_character('😀'), vec![NewtypeVariant("RegexForm", 1, "Character"), Char('😀')]),
        (raw_symbol("ns/x".into()), vec![NewtypeVariant("RegexForm", 2, "Symbol"), s("ns/x")]),
        (raw_big_integer("00099".into()), vec![NewtypeVariant("RegexForm", 3, "BigInteger"), s("00099")]),
        (raw_decimal("1.00M".into()), vec![NewtypeVariant("RegexForm", 4, "Decimal"), s("1.00M")]),
        (raw_list(vec![raw_character('x')]), vec![NewtypeVariant("RegexForm", 5, "List"), CollectSeq, Seq(Some(1)), SeqElement, NewtypeVariant("RegexForm", 1, "Character"), Char('x'), SeqEnd]),
        (raw_vector(vec![]), vec![NewtypeVariant("RegexForm", 6, "Vector"), CollectSeq, Seq(Some(0)), SeqEnd]),
        (raw_set(vec![]), vec![NewtypeVariant("RegexForm", 7, "Set"), CollectSeq, Seq(Some(0)), SeqEnd]),
        (raw_map(vec![(raw_character('k'), raw_character('v'))]), vec![NewtypeVariant("RegexForm", 8, "Map"), CollectSeq, Seq(Some(1)), SeqElement, Tuple(2), TupleElement, NewtypeVariant("RegexForm", 1, "Character"), Char('k'), TupleElement, NewtypeVariant("RegexForm", 1, "Character"), Char('v'), TupleEnd, SeqEnd]),
        (raw_tagged("reader/tag".into(), raw_literal(V::Null)), vec![StructVariant("RegexForm", 9, "Tagged", 2), VariantField("tag"), s("reader/tag"), VariantField("value"), NewtypeVariant("RegexForm", 0, "Literal"), UnitVariant("Value", 1, "Null"), StructVariantEnd]),
        (raw_anonymous(raw_character('%')), vec![NewtypeVariant("RegexForm", 10, "AnonymousFunction"), NewtypeVariant("RegexForm", 1, "Character"), Char('%')]),
        (raw_build(|builder| builder.ratio("-2/3")), vec![NewtypeVariant("RegexForm", 11, "Ratio"), s("-2/3")]),
    ];
    assert_eq!(cases.len(), 12);
    for (value, expected) in cases {
        for human_readable in [true, false] { assert_eq!(trace(&value, human_readable), expected); }
    }
}

#[test]
fn all_forty_one_literal_variants_keep_generic_serde_shapes_widths_and_bit_payloads() {
    let cases = vec![
        (V::Undefined, vec![UnitVariant("Value", 0, "Undefined")]),
        (V::Null, vec![UnitVariant("Value", 1, "Null")]),
        (V::Bool(true), vec![NewtypeVariant("Value", 2, "Bool"), Bool(true)]),
        (V::I8(-8), vec![NewtypeVariant("Value", 3, "I8"), I8(-8)]),
        (V::I16(-16), vec![NewtypeVariant("Value", 4, "I16"), I16(-16)]),
        (V::I32(-32), vec![NewtypeVariant("Value", 5, "I32"), I32(-32)]),
        (V::I64(-64), vec![NewtypeVariant("Value", 6, "I64"), I64(-64)]),
        (V::U8(8), vec![NewtypeVariant("Value", 7, "U8"), U8(8)]),
        (V::U16(16), vec![NewtypeVariant("Value", 8, "U16"), U16(16)]),
        (V::U32(32), vec![NewtypeVariant("Value", 9, "U32"), U32(32)]),
        (V::U64(u64::MAX), vec![NewtypeVariant("Value", 10, "U64"), U64(u64::MAX)]),
        (V::F16(0x8000), vec![NewtypeVariant("Value", 11, "F16"), U16(0x8000)]),
        (V::F32(f32::from_bits(0x7fc00001)), vec![NewtypeVariant("Value", 12, "F32"), U32(0x7fc00001)]),
        (V::F64(f64::from_bits(0x7ff8000000000001)), vec![NewtypeVariant("Value", 13, "F64"), U64(0x7ff8000000000001)]),
        (V::Utf8("é😀\n".into()), vec![NewtypeVariant("Value", 14, "Utf8"), s("é😀\n")]),
        (V::Binary(vec![0, 255]), vec![NewtypeVariant("Value", 15, "Binary"), CollectSeq, Seq(Some(2)), SeqElement, U8(0), SeqElement, U8(255), SeqEnd]),
        (V::FixedSizeBinary(vec![9]), vec![NewtypeVariant("Value", 16, "FixedSizeBinary"), CollectSeq, Seq(Some(1)), SeqElement, U8(9), SeqEnd]),
        (V::Decimal(DecimalValue { unscaled: vec![255, 0], precision: 4, scale: -2, bit_width: 16 }), vec![NewtypeVariant("Value", 17, "Decimal"), Struct("DecimalValue", 4), Field("unscaled"), CollectSeq, Seq(Some(2)), SeqElement, U8(255), SeqElement, U8(0), SeqEnd, Field("precision"), I32(4), Field("scale"), I32(-2), Field("bit_width"), U16(16), StructEnd]),
        (V::Date(CalendarDate::new(CivilDate::from_ymd(2000, 1, 2).unwrap(), DateUnit::Day)), vec![NewtypeVariant("Value", 18, "Date"), Struct("CalendarDate", 2), Field("date"), CollectStr("2000-01-02".into()), Field("unit"), UnitVariant("DateUnit", 0, "Day"), StructEnd]),
        (V::DateDay(-1), vec![NewtypeVariant("Value", 19, "DateDay"), I32(-1)]),
        (V::DateMilli(-1000), vec![NewtypeVariant("Value", 20, "DateMilli"), I64(-1000)]),
        (V::Time { value: 7, unit: TimeUnit::Microsecond, bit_width: 64 }, vec![StructVariant("Value", 21, "Time", 3), VariantField("value"), I64(7), VariantField("unit"), UnitVariant("TimeUnit", 2, "Microsecond"), VariantField("bit_width"), U8(64), StructVariantEnd]),
        (V::Timestamp { value: -7, unit: TimeUnit::Nanosecond, timezone: None }, vec![StructVariant("Value", 22, "Timestamp", 3), VariantField("value"), I64(-7), VariantField("unit"), UnitVariant("TimeUnit", 3, "Nanosecond"), VariantField("timezone"), OptionNone, StructVariantEnd]),
        (V::Duration { value: 8, unit: TimeUnit::Second }, vec![StructVariant("Value", 23, "Duration", 2), VariantField("value"), I64(8), VariantField("unit"), UnitVariant("TimeUnit", 0, "Second"), StructVariantEnd]),
        (V::IntervalYearMonth(-12), vec![NewtypeVariant("Value", 24, "IntervalYearMonth"), I32(-12)]),
        (V::IntervalDayTime { days: -1, milliseconds: 3 }, vec![StructVariant("Value", 25, "IntervalDayTime", 2), VariantField("days"), I32(-1), VariantField("milliseconds"), I32(3), StructVariantEnd]),
        (V::IntervalMonthDayNano { months: 1, days: 2, nanoseconds: 3 }, vec![StructVariant("Value", 26, "IntervalMonthDayNano", 3), VariantField("months"), I32(1), VariantField("days"), I32(2), VariantField("nanoseconds"), I64(3), StructVariantEnd]),
        (V::IntervalMonthDayMicro { months: 4, days: 5, nanoseconds: 6 }, vec![StructVariant("Value", 27, "IntervalMonthDayMicro", 3), VariantField("months"), I32(4), VariantField("days"), I32(5), VariantField("nanoseconds"), I64(6), StructVariantEnd]),
        (V::Keyword(Keyword::from_name("plain/name")), vec![NewtypeVariant("Value", 28, "Keyword"), s("plain/name")]),
        (V::Uuid([0; 16]), vec![NewtypeVariant("Value", 29, "Uuid"), Tuple(16), TupleElement, U8(0), TupleElement, U8(0), TupleElement, U8(0), TupleElement, U8(0), TupleElement, U8(0), TupleElement, U8(0), TupleElement, U8(0), TupleElement, U8(0), TupleElement, U8(0), TupleElement, U8(0), TupleElement, U8(0), TupleElement, U8(0), TupleElement, U8(0), TupleElement, U8(0), TupleElement, U8(0), TupleElement, U8(0), TupleEnd]),
        (V::Uri("https://example.test/é".into()), vec![NewtypeVariant("Value", 30, "Uri"), s("https://example.test/é")]),
        (V::Transit(vec![0, 255]), vec![NewtypeVariant("Value", 31, "Transit"), CollectSeq, Seq(Some(2)), SeqElement, U8(0), SeqElement, U8(255), SeqEnd]),
        (V::Oid(-1), vec![NewtypeVariant("Value", 32, "Oid"), I32(-1)]),
        (V::RegClass(2), vec![NewtypeVariant("Value", 33, "RegClass"), I32(2)]),
        (V::RegProc(3), vec![NewtypeVariant("Value", 34, "RegProc"), I32(3)]),
        (V::list(vec![V::I64(1)]), vec![NewtypeVariant("Value", 35, "List"), CollectSeq, Seq(Some(1)), SeqElement, NewtypeVariant("Value", 6, "I64"), I64(1), SeqEnd]),
        (V::Set(vec![V::Null, V::Null]), vec![NewtypeVariant("Value", 36, "Set"), CollectSeq, Seq(Some(2)), SeqElement, UnitVariant("Value", 1, "Null"), SeqElement, UnitVariant("Value", 1, "Null"), SeqEnd]),
        (V::Map { entries: vec![(V::Null, V::Bool(true))], sorted: true }, vec![StructVariant("Value", 37, "Map", 2), VariantField("entries"), CollectSeq, Seq(Some(1)), SeqElement, Tuple(2), TupleElement, UnitVariant("Value", 1, "Null"), TupleElement, NewtypeVariant("Value", 2, "Bool"), Bool(true), TupleEnd, SeqEnd, VariantField("sorted"), Bool(true), StructVariantEnd]),
        (V::FixedSizeList(vec![V::Bool(false)].into()), vec![NewtypeVariant("Value", 38, "FixedSizeList"), CollectSeq, Seq(Some(1)), SeqElement, NewtypeVariant("Value", 2, "Bool"), Bool(false), SeqEnd]),
        (V::TimestampTzRange { lower: -1, upper: 2 }, vec![StructVariant("Value", 39, "TimestampTzRange", 2), VariantField("lower"), I64(-1), VariantField("upper"), I64(2), StructVariantEnd]),
        (V::Struct(StructFields::from([("z".into(), V::Null), ("a".into(), V::Bool(true))])), vec![NewtypeVariant("Value", 40, "Struct"), CollectMap, Map(Some(2)), MapEntry, s("a"), NewtypeVariant("Value", 2, "Bool"), Bool(true), MapEntry, s("z"), UnitVariant("Value", 1, "Null"), MapEnd]),
    ];
    assert_eq!(cases.len(), 41);
    for (value, body) in cases {
        let expected = literal_events(body);
        let form = raw_literal(value);
        for human_readable in [true, false] { assert_eq!(trace(&form, human_readable), expected); }
    }
}

#[test]
fn recipe_generic_serde_and_json_keep_all_five_fields_in_declaration_order() {
    let recipe = RegexReplacement {
        pattern: raw_symbol("p".into()), replacement: raw_literal(V::Utf8("r".into())),
        start: raw_literal(V::I64(1)), occurrence: raw_literal(V::Bool(false)), flags: raw_vector(vec![raw_character('m')]),
    };
    let expected = vec![Struct("RegexReplacement", 5),
        Field("pattern"), NewtypeVariant("RegexForm", 2, "Symbol"), s("p"),
        Field("replacement"), NewtypeVariant("RegexForm", 0, "Literal"), NewtypeVariant("Value", 14, "Utf8"), s("r"),
        Field("start"), NewtypeVariant("RegexForm", 0, "Literal"), NewtypeVariant("Value", 6, "I64"), I64(1),
        Field("occurrence"), NewtypeVariant("RegexForm", 0, "Literal"), NewtypeVariant("Value", 2, "Bool"), Bool(false),
        Field("flags"), NewtypeVariant("RegexForm", 6, "Vector"), CollectSeq, Seq(Some(1)), SeqElement, NewtypeVariant("RegexForm", 1, "Character"), Char('m'), SeqEnd,
        StructEnd];
    for human_readable in [true, false] { assert_eq!(trace(&recipe, human_readable), expected); }
    let json = r#"{"pattern":{"Symbol":"p"},"replacement":{"Literal":{"Utf8":"r"}},"start":{"Literal":{"I64":1}},"occurrence":{"Literal":{"Bool":false}},"flags":{"Vector":[{"Character":"m"}]}}"#;
    assert_eq!(serde_json::to_string(&recipe).unwrap(), json);
    assert_eq!(serde_json::from_str::<RegexReplacement>(json).unwrap(), recipe);
}

#[test]
fn optional_timezone_and_remaining_unit_discriminants_keep_their_serde_methods() {
    let timestamp = raw_literal(V::Timestamp { value: 1, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) });
    let expected = literal_events(vec![StructVariant("Value", 22, "Timestamp", 3), VariantField("value"), I64(1), VariantField("unit"), UnitVariant("TimeUnit", 1, "Millisecond"), VariantField("timezone"), OptionSome, s("UTC"), StructVariantEnd]);
    for human_readable in [true, false] { assert_eq!(trace(&timestamp, human_readable), expected); }
    let date = raw_literal(V::Date(CalendarDate::new(CivilDate::from_ymd(2000, 1, 2).unwrap(), DateUnit::Millisecond)));
    let expected = literal_events(vec![NewtypeVariant("Value", 18, "Date"), Struct("CalendarDate", 2), Field("date"), CollectStr("2000-01-02".into()), Field("unit"), UnitVariant("DateUnit", 1, "Millisecond"), StructEnd]);
    for human_readable in [true, false] { assert_eq!(trace(&date, human_readable), expected); }
}

#[test]
fn float_wire_bits_preserve_nan_payloads_infinities_and_signed_zero() {
    for bits in [0u32, 0x80000000, 0x7f800000, 0xff800000, 0x7fc00001, 0x7fa00002] {
        let form = raw_literal(V::F32(f32::from_bits(bits)));
        let expected = literal_events(vec![NewtypeVariant("Value", 12, "F32"), U32(bits)]);
        for human_readable in [true, false] { assert_eq!(trace(&form, human_readable), expected); }
        let json = format!(r#"{{"Literal":{{"F32":{bits}}}}}"#);
        assert_eq!(serde_json::to_string(&form).unwrap(), json);
        let decoded_form = serde_json::from_str::<F>(&json).unwrap();
        let rtdb_plan::RegexFormKind::Literal(literal) = decoded_form.view().kind() else { panic!("expected F32") };
        let rtdb_plan::RegexLiteralKind::Leaf(leaf) = literal.kind() else { panic!("expected F32") };
        let cell = leaf.value();
        let V::F32(decoded) = cell.as_ref() else { panic!("expected F32") };
        assert_eq!(decoded.to_bits(), bits);
    }
    for bits in [0u64, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000001, 0x7ff0000000000002] {
        let form = raw_literal(V::F64(f64::from_bits(bits)));
        let expected = literal_events(vec![NewtypeVariant("Value", 13, "F64"), U64(bits)]);
        for human_readable in [true, false] { assert_eq!(trace(&form, human_readable), expected); }
        let json = format!(r#"{{"Literal":{{"F64":{bits}}}}}"#);
        assert_eq!(serde_json::to_string(&form).unwrap(), json);
        let decoded_form = serde_json::from_str::<F>(&json).unwrap();
        let rtdb_plan::RegexFormKind::Literal(literal) = decoded_form.view().kind() else { panic!("expected F64") };
        let rtdb_plan::RegexLiteralKind::Leaf(leaf) = literal.kind() else { panic!("expected F64") };
        let cell = leaf.value();
        let V::F64(decoded) = cell.as_ref() else { panic!("expected F64") };
        assert_eq!(decoded.to_bits(), bits);
    }
}

fn nil_recipe() -> serde_json::Value {
    serde_json::json!({"pattern":{"Literal":"Null"},"replacement":{"Literal":"Null"},"start":{"Literal":"Null"},"occurrence":{"Literal":"Null"},"flags":{"Literal":"Null"}})
}

fn data_error<T: serde::de::DeserializeOwned>(value: serde_json::Value, expected: &str) {
    let error = match serde_json::from_value::<T>(value) { Err(error) => error, Ok(_) => panic!("expected a deserialization error") };
    assert_eq!(error.classify(), serde_json::error::Category::Data);
    assert_eq!(error.to_string(), expected);
}

#[test]
fn rust_recipe_default_is_nil_but_serde_requires_every_field() {
    let expected = RegexReplacement { pattern: raw_literal(V::Null), replacement: raw_literal(V::Null), start: raw_literal(V::Null), occurrence: raw_literal(V::Null), flags: raw_literal(V::Null) };
    assert_eq!(RegexReplacement::default(), expected);
    assert_eq!(serde_json::from_value::<RegexReplacement>(nil_recipe()).unwrap(), expected);
    data_error::<RegexReplacement>(serde_json::json!({}), "missing field `pattern`");
    for (field, message) in [("pattern", "missing field `pattern`"), ("replacement", "missing field `replacement`"), ("start", "missing field `start`"), ("occurrence", "missing field `occurrence`"), ("flags", "missing field `flags`")] {
        let mut input = nil_recipe();
        input.as_object_mut().unwrap().remove(field);
        data_error::<RegexReplacement>(input, message);
    }
}

#[test]
fn null_and_missing_payloads_retain_their_distinct_errors() {
    data_error::<F>(serde_json::Value::Null, "invalid type: null, expected string or map");
    data_error::<F>(serde_json::json!({"Literal":null}), "invalid type: null, expected string or map");
    let mut input = nil_recipe();
    input["pattern"] = serde_json::Value::Null;
    data_error::<RegexReplacement>(input, "invalid type: null, expected string or map");
    data_error::<F>(serde_json::json!({"Tagged":{"tag":"reader"}}), "missing field `value`");
    data_error::<F>(serde_json::json!({"Tagged":{"value":{"Literal":"Null"}}}), "missing field `tag`");
    data_error::<F>(serde_json::json!({"Literal":{"Map":{"entries":[]}}}), "missing field `sorted`");
    let decoded = serde_json::from_value::<F>(serde_json::json!({"Literal":{"Timestamp":{"value":0,"unit":"Second"}}})).unwrap();
    assert_eq!(decoded, raw_literal(V::Timestamp { value: 0, unit: TimeUnit::Second, timezone: None }));
    let decoded = serde_json::from_value::<F>(serde_json::json!({"Literal":{"Timestamp":{"value":0,"unit":"Second","timezone":null}}})).unwrap();
    assert_eq!(decoded, raw_literal(V::Timestamp { value: 0, unit: TimeUnit::Second, timezone: None }));
}

#[test]
fn known_duplicate_fields_error_while_unknown_fields_are_consumed() {
    use serde::Deserialize;
    let fields = vec![("pattern", serde_json::json!({"Literal":"Null"})), ("pattern", serde_json::json!({"Symbol":"second"}))];
    let decoder = serde::de::value::MapDeserializer::<_, serde_json::Error>::new(fields.into_iter());
    let error = RegexReplacement::deserialize(decoder).unwrap_err();
    assert_eq!(error.classify(), serde_json::error::Category::Data);
    assert_eq!(error.to_string(), "duplicate field `pattern`");
    for (input, message) in [
        (r#"{"Tagged":{"tag":"first","tag":"second","value":{"Literal":"Null"}}}"#, "duplicate field `tag`"),
        (r#"{"Tagged":{"tag":"reader","value":{"Literal":"Null"},"value":{"Symbol":"second"}}}"#, "duplicate field `value`"),
    ] {
        let error = serde_json::from_str::<F>(input).unwrap_err();
        assert_eq!(error.classify(), serde_json::error::Category::Data);
        assert!(error.to_string().starts_with(message));
    }
    let mut input = nil_recipe();
    input["unknown"] = serde_json::json!({"nested":[1,{"ignored":true}]});
    assert_eq!(serde_json::from_value::<RegexReplacement>(input).unwrap(), RegexReplacement::default());
    let input = r#"{"Tagged":{"unknown":[1,{"ignored":true}],"tag":"reader","value":{"Literal":"Null"},"unknown":false}}"#;
    assert_eq!(serde_json::from_str::<F>(input).unwrap(), raw_tagged("reader".into(), raw_literal(V::Null)));
}

#[test]
fn character_serde_accepts_one_supplementary_scalar_and_rejects_other_payload_shapes() {
    assert_eq!(serde_json::to_string(&raw_character('😀')).unwrap(), r#"{"Character":"😀"}"#);
    assert_eq!(serde_json::from_str::<F>(r#"{"Character":"\ud83d\ude00"}"#).unwrap(), raw_character('😀'));
    assert_eq!(serde_json::from_str::<F>(r#"{"Character":"😀"}"#).unwrap(), raw_character('😀'));
    data_error::<F>(serde_json::json!({"Character":""}), "invalid value: string \"\", expected a character");
    data_error::<F>(serde_json::json!({"Character":"ab"}), "invalid value: string \"ab\", expected a character");
    data_error::<F>(serde_json::json!({"Character":null}), "invalid type: null, expected a character");
}

#[test]
fn generic_json_strings_reject_isolated_reversed_and_mismatched_surrogate_escapes() {
    for (input, message) in [
        (r#"{"Character":"\ud800"}"#, "unexpected end of hex escape"),
        (r#"{"Character":"\udc00"}"#, "lone leading surrogate in hex escape"),
        (r#"{"Character":"\udc00\ud800"}"#, "lone leading surrogate in hex escape"),
        (r#"{"Character":"\ud800\u0061"}"#, "lone leading surrogate in hex escape"),
        (r#"{"Literal":{"Utf8":"\ud800"}}"#, "unexpected end of hex escape"),
        (r#"{"Symbol":"\udc00"}"#, "lone leading surrogate in hex escape"),
    ] {
        let error = serde_json::from_str::<F>(input).unwrap_err();
        assert_eq!(error.classify(), serde_json::error::Category::Syntax);
        assert_eq!(error.line(), 1);
        assert!(error.column() > 0);
        assert!(error.to_string().starts_with(message));
    }
}

#[test]
fn physical_structs_keep_the_last_duplicate_and_utf8_key_order() {
    let input = r#"{"Literal":{"Struct":{"z":{"I64":1},"a":{"I64":2},"z":{"I64":3},"😀":"Null","\ue000":"Undefined"}}}"#;
    let form = serde_json::from_str::<F>(input).unwrap();
    let expected = raw_literal(V::Struct(StructFields::from([("a".into(), V::I64(2)), ("z".into(), V::I64(3)), ("\u{e000}".into(), V::Undefined), ("😀".into(), V::Null)])));
    assert_eq!(form, expected);
    assert_eq!(serde_json::to_string(&form).unwrap(), "{\"Literal\":{\"Struct\":{\"a\":{\"I64\":2},\"z\":{\"I64\":3},\"\u{e000}\":\"Undefined\",\"😀\":\"Null\"}}}");
}

#[test]
fn raw_map_set_and_physical_map_set_keep_order_and_duplicates() {
    for input in [
        r#"{"Map":[[{"Symbol":"z"},{"Literal":{"I64":1}}],[{"Symbol":"a"},{"Literal":{"I64":2}}],[{"Symbol":"z"},{"Literal":{"I64":3}}]]}"#,
        r#"{"Set":[{"Symbol":"z"},{"Symbol":"a"},{"Symbol":"z"}]}"#,
        r#"{"Literal":{"Map":{"entries":[[{"Utf8":"z"},{"I64":1}],[{"Utf8":"a"},{"I64":2}],[{"Utf8":"z"},{"I64":3}]],"sorted":false}}}"#,
        r#"{"Literal":{"Set":[{"Utf8":"z"},{"Utf8":"a"},{"Utf8":"z"}]}}"#,
    ] {
        let form = serde_json::from_str::<F>(input).unwrap();
        assert_eq!(serde_json::to_string(&form).unwrap(), input);
    }
    assert_ne!(raw_set(vec![raw_symbol("a".into()), raw_symbol("b".into())]), raw_set(vec![raw_symbol("b".into()), raw_symbol("a".into())]));
    assert_ne!(raw_set(vec![raw_symbol("a".into())]), raw_set(vec![raw_symbol("a".into()), raw_symbol("a".into())]));
    assert_ne!(raw_literal(V::Set(vec![V::I64(1), V::I64(2)])), raw_literal(V::Set(vec![V::I64(2), V::I64(1)])));
}

#[test]
fn shallow_debug_retains_normal_pretty_escaping_and_formatter_options() {
    let form = raw_tagged("tag".into(), raw_literal(V::Null));
    assert_eq!(format!("{form:?}"), "Tagged { tag: \"tag\", value: Literal(Null) }");
    assert_eq!(format!("{form:#?}"), "Tagged {\n    tag: \"tag\",\n    value: Literal(\n        Null,\n    ),\n}");
    let form = raw_list(vec![raw_character('\n'), raw_literal(V::Utf8("a\"\\\t".into()))]);
    assert_eq!(format!("{form:?}"), r#"List([Character('\n'), Literal(Utf8("a\"\\\t"))])"#);
    assert_eq!(format!("{:?}", raw_literal(V::Keyword(Keyword::from_name(":x")))), r#"Literal(Keyword(Keyword(":x")))"#);
    let nil = raw_literal(V::Null);
    assert_eq!(format!("{:>32.2?}", nil), "Literal(Null)");
    let integer = raw_literal(V::I64(31));
    assert_eq!(format!("{:x?}", integer), "Literal(I64(1f))");
    assert_eq!(format!("{:X?}", integer), "Literal(I64(1F))");
    assert_eq!(format!("{:?}", RegexReplacement::default()), "RegexReplacement { pattern: Literal(Null), replacement: Literal(Null), start: Literal(Null), occurrence: Literal(Null), flags: Literal(Null) }");
}

#[test]
fn structural_equality_retains_nonreflexive_nan_and_signed_zero_rules() {
    for form in [raw_literal(V::F32(f32::from_bits(0x7fc00001))), raw_literal(V::F64(f64::from_bits(0x7ff8000000000001)))] {
        let same_form = &form;
        assert_ne!(&form, same_form);
        assert_ne!(form, form.clone());
        let recipe = RegexReplacement { pattern: form, ..Default::default() };
        let same_recipe = &recipe;
        assert_ne!(&recipe, same_recipe);
        assert_ne!(recipe, recipe.clone());
    }
    assert_eq!(raw_literal(V::F32(0.0)), raw_literal(V::F32(-0.0)));
    assert_eq!(raw_literal(V::F64(0.0)), raw_literal(V::F64(-0.0)));
    assert_ne!(raw_literal(V::F16(0)), raw_literal(V::F16(0x8000)));
    assert_ne!(serde_json::to_string(&raw_literal(V::F32(0.0))).unwrap(), serde_json::to_string(&raw_literal(V::F32(-0.0))).unwrap());
    assert_ne!(serde_json::to_string(&raw_literal(V::F64(0.0))).unwrap(), serde_json::to_string(&raw_literal(V::F64(-0.0))).unwrap());
}

fn raw_build(emit: impl FnOnce(&mut rtdb_plan::RegexFormBuilder<'_>) -> rtdb_types::Result<()>) -> rtdb_plan::RegexForm {
    let mut builder = rtdb_plan::RegexFormBuilder::new_untracked();
    emit(&mut builder).unwrap();
    builder.finish().unwrap()
}
fn raw_literal(value: rtdb_types::Value) -> rtdb_plan::RegexForm { raw_build(|builder| builder.literal(&value)) }
fn raw_character(value: char) -> rtdb_plan::RegexForm { raw_build(|builder| builder.character(value)) }
fn raw_symbol(value: String) -> rtdb_plan::RegexForm { raw_build(|builder| builder.symbol(&value)) }
fn raw_big_integer(value: String) -> rtdb_plan::RegexForm { raw_build(|builder| builder.big_integer(&value)) }
fn raw_decimal(value: String) -> rtdb_plan::RegexForm { raw_build(|builder| builder.decimal(&value)) }
fn raw_list(values: Vec<rtdb_plan::RegexForm>) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_list()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
fn raw_vector(values: Vec<rtdb_plan::RegexForm>) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_vector()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
fn raw_set(values: Vec<rtdb_plan::RegexForm>) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_set()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
fn raw_map(values: Vec<(rtdb_plan::RegexForm, rtdb_plan::RegexForm)>) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_map()?; for (key, value) in values { builder.append_view(key.view())?; builder.append_view(value.view())?; } builder.end() }) }
fn raw_tagged(tag: String, value: rtdb_plan::RegexForm) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_tagged(&tag)?; builder.append_view(value.view())?; builder.end() }) }
fn raw_anonymous(value: rtdb_plan::RegexForm) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_anonymous()?; builder.append_view(value.view())?; builder.end() }) }
