use std::{fmt, marker::PhantomData};
use ::serde::{Deserialize, Deserializer, Serialize, Serializer, de::{self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor}, ser::{SerializeStructVariant, SerializeTuple}};
use rtdb_types::{CalendarDate, DecimalValue, TimeUnit, Value};
use super::{RegexForm, RegexFormBuilder, RegexFormKind as F, RegexFormView, RegexLiteralKind as V, RegexLiteralView};

const FORMS: &[&str] = &["Literal", "Character", "Symbol", "BigInteger", "Decimal", "List", "Vector", "Set", "Map", "Tagged", "AnonymousFunction", "Ratio"];
const VALUES: &[&str] = &["Undefined", "Null", "Bool", "I8", "I16", "I32", "I64", "U8", "U16", "U32", "U64", "F16", "F32", "F64", "Utf8", "Binary", "FixedSizeBinary", "Decimal", "Date", "DateDay", "DateMilli", "Time", "Timestamp", "Duration", "IntervalYearMonth", "IntervalDayTime", "IntervalMonthDayNano", "IntervalMonthDayMicro", "Keyword", "Uuid", "Uri", "Transit", "Oid", "RegClass", "RegProc", "List", "Set", "Map", "FixedSizeList", "TimestampTzRange", "Struct"];

struct RawSer<'a>(RegexFormView<'a>);
struct ValueSer<'a>(RegexLiteralView<'a>);
struct RawSequence<'a>(super::RegexForms<'a>);
struct ValueSequence<'a>(super::RegexLiterals<'a>);
struct RawPairs<'a>(super::RegexFormPairs<'a>);
struct ValuePairs<'a>(super::RegexLiteralPairs<'a>);
struct Fields<'a>(super::RegexFields<'a>);
struct Pair<A, B>(A, B);

impl Serialize for RegexForm { fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> { RawSer(self.view()).serialize(serializer) } }
impl Serialize for RawSer<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0.kind() {
            F::Literal(value) => serializer.serialize_newtype_variant("RegexForm", 0, "Literal", &ValueSer(value)),
            F::Character(value) => serializer.serialize_newtype_variant("RegexForm", 1, "Character", &value),
            F::Symbol(value) => serializer.serialize_newtype_variant("RegexForm", 2, "Symbol", value),
            F::BigInteger(value) => serializer.serialize_newtype_variant("RegexForm", 3, "BigInteger", value),
            F::Decimal(value) => serializer.serialize_newtype_variant("RegexForm", 4, "Decimal", value),
            F::List(values) => serializer.serialize_newtype_variant("RegexForm", 5, "List", &RawSequence(values)),
            F::Vector(values) => serializer.serialize_newtype_variant("RegexForm", 6, "Vector", &RawSequence(values)),
            F::Set(values) => serializer.serialize_newtype_variant("RegexForm", 7, "Set", &RawSequence(values)),
            F::Map(values) => serializer.serialize_newtype_variant("RegexForm", 8, "Map", &RawPairs(values)),
            F::Tagged { tag, value } => { let mut object = serializer.serialize_struct_variant("RegexForm", 9, "Tagged", 2)?; object.serialize_field("tag", tag)?; object.serialize_field("value", &RawSer(value))?; object.end() }
            F::AnonymousFunction(value) => serializer.serialize_newtype_variant("RegexForm", 10, "AnonymousFunction", &RawSer(value)),
            F::Ratio(value) => serializer.serialize_newtype_variant("RegexForm", 11, "Ratio", value),
        }
    }
}
impl Serialize for ValueSer<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0.kind() {
            V::Text(value) => serializer.serialize_newtype_variant("Value", 14, "Utf8", value),
            V::Keyword(value) => serializer.serialize_newtype_variant("Value", 28, "Keyword", value),
            V::Leaf(value) => match value.packed_float() {
                Some(component) => component.serialize(serializer),
                None => value.value().serialize(serializer),
            },
            V::List(values) => serializer.serialize_newtype_variant("Value", 35, "List", &ValueSequence(values)),
            V::Set(values) => serializer.serialize_newtype_variant("Value", 36, "Set", &ValueSequence(values)),
            V::Map { entries, sorted } => { let mut object = serializer.serialize_struct_variant("Value", 37, "Map", 2)?; object.serialize_field("entries", &ValuePairs(entries))?; object.serialize_field("sorted", &sorted)?; object.end() }
            V::FixedSizeList(values) => serializer.serialize_newtype_variant("Value", 38, "FixedSizeList", &ValueSequence(values)),
            V::Struct(fields) => serializer.serialize_newtype_variant("Value", 40, "Struct", &Fields(fields)),
        }
    }
}
impl Serialize for RawSequence<'_> { fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> { s.collect_seq(self.0.iter().map(RawSer)) } }
impl Serialize for ValueSequence<'_> { fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> { s.collect_seq(self.0.iter().map(ValueSer)) } }
impl Serialize for RawPairs<'_> { fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> { s.collect_seq(self.0.iter().map(|(k, v)| Pair(RawSer(k), RawSer(v)))) } }
impl Serialize for ValuePairs<'_> { fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> { s.collect_seq(self.0.iter().map(|(k, v)| Pair(ValueSer(k), ValueSer(v)))) } }
impl Serialize for Fields<'_> { fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> { s.collect_map(self.0.iter().map(|(k, v)| (k, ValueSer(v)))) } }
impl<A: Serialize, B: Serialize> Serialize for Pair<A, B> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> { let mut pair = s.serialize_tuple(2)?; pair.serialize_element(&self.0)?; pair.serialize_element(&self.1)?; pair.end() }
}

struct Name(&'static [&'static str]);
struct VariantRange(usize);
impl de::Expected for VariantRange { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "variant index 0 <= i < {}", self.0) } }
impl<'de> DeserializeSeed<'de> for Name {
    type Value = usize;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<usize, D::Error> { d.deserialize_identifier(self) }
}
impl Visitor<'_> for Name {
    type Value = usize;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("variant identifier") }
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<usize, E> { usize::try_from(value).ok().filter(|value| *value < self.0.len()).ok_or_else(|| E::invalid_value(de::Unexpected::Unsigned(value), &VariantRange(self.0.len()))) }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<usize, E> { self.0.iter().position(|name| *name == value).ok_or_else(|| E::unknown_variant(value, self.0)) }
    fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<usize, E> { match std::str::from_utf8(value) { Ok(value) => self.visit_str(value), Err(_) => Err(E::unknown_variant(&String::from_utf8_lossy(value), self.0)) } }
}

struct Field(&'static [&'static str]);
impl<'de> DeserializeSeed<'de> for Field {
    type Value = usize;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<usize, D::Error> { d.deserialize_identifier(self) }
}
impl Visitor<'_> for Field {
    type Value = usize;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("field identifier") }
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<usize, E> { Ok(usize::try_from(value).ok().filter(|index| *index < self.0.len()).unwrap_or(usize::MAX)) }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<usize, E> { Ok(self.0.iter().position(|name| *name == value).unwrap_or(usize::MAX)) }
    fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<usize, E> { Ok(self.0.iter().position(|name| name.as_bytes() == value).unwrap_or(usize::MAX)) }
}

impl<'de> Deserialize<'de> for RegexForm {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let mut builder = RegexFormBuilder::new_untracked();
        RawSeed(&mut builder).deserialize(d)?;
        builder.finish().map_err(de::Error::custom)
    }
}
struct RawSeed<'a, 'budget>(&'a mut RegexFormBuilder<'budget>);
struct ValueSeed<'a, 'budget>(&'a mut RegexFormBuilder<'budget>);
impl<'de> DeserializeSeed<'de> for RawSeed<'_, '_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> { d.deserialize_enum("RegexForm", FORMS, self) }
}
impl<'de> DeserializeSeed<'de> for ValueSeed<'_, '_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> { d.deserialize_enum("Value", VALUES, self) }
}
impl<'de> Visitor<'de> for RawSeed<'_, '_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("enum RegexForm") }
    fn visit_enum<A: EnumAccess<'de>>(self, access: A) -> Result<(), A::Error> {
        let (variant, value) = access.variant_seed(Name(FORMS))?;
        let builder = self.0;
        match variant {
            0 => { builder.begin_literal().map_err(de::Error::custom)?; value.newtype_variant_seed(ValueSeed(builder))?; builder.end().map_err(de::Error::custom) }
            1 => builder.character(value.newtype_variant()?).map_err(de::Error::custom),
            2 => builder.symbol(&value.newtype_variant::<String>()?).map_err(de::Error::custom),
            3 => builder.big_integer(&value.newtype_variant::<String>()?).map_err(de::Error::custom),
            4 => builder.decimal(&value.newtype_variant::<String>()?).map_err(de::Error::custom),
            5..=8 => {
                match variant { 5 => builder.begin_list(), 6 => builder.begin_vector(), 7 => builder.begin_set(), _ => builder.begin_map() }.map_err(de::Error::custom)?;
                value.newtype_variant_seed(SequenceSeed { builder, physical: false, pairs: variant == 8 })?;
                builder.end().map_err(de::Error::custom)
            }
            9 => { builder.begin_tagged("").map_err(de::Error::custom)?; value.struct_variant(&["tag", "value"], Tagged(builder))?; builder.end().map_err(de::Error::custom) }
            10 => { builder.begin_anonymous().map_err(de::Error::custom)?; value.newtype_variant_seed(RawSeed(builder))?; builder.end().map_err(de::Error::custom) }
            11 => builder.ratio(&value.newtype_variant::<String>()?).map_err(de::Error::custom),
            _ => unreachable!("closed form names"),
        }
    }
}

struct SequenceSeed<'a, 'budget> { builder: &'a mut RegexFormBuilder<'budget>, physical: bool, pairs: bool }
impl<'de> DeserializeSeed<'de> for SequenceSeed<'_, '_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> { d.deserialize_seq(self) }
}
impl<'de> Visitor<'de> for SequenceSeed<'_, '_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("a sequence") }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        loop {
            let value = if self.pairs { sequence.next_element_seed(PairSeed { builder: self.builder, physical: self.physical })? }
                else if self.physical { sequence.next_element_seed(ValueSeed(self.builder))? }
                else { sequence.next_element_seed(RawSeed(self.builder))? };
            if value.is_none() { return Ok(()); }
        }
    }
}
struct PairSeed<'a, 'budget> { builder: &'a mut RegexFormBuilder<'budget>, physical: bool }
impl<'de> DeserializeSeed<'de> for PairSeed<'_, '_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> { d.deserialize_tuple(2, self) }
}
impl<'de> Visitor<'de> for PairSeed<'_, '_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("a tuple of size 2") }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        for at in 0..2 {
            let value = if self.physical { sequence.next_element_seed(ValueSeed(self.builder))? } else { sequence.next_element_seed(RawSeed(self.builder))? };
            if value.is_none() { return Err(de::Error::invalid_length(at, &"a tuple of size 2")); }
        }
        Ok(())
    }
}

struct Tagged<'a, 'budget>(&'a mut RegexFormBuilder<'budget>);
impl<'de> Visitor<'de> for Tagged<'_, '_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("struct variant RegexForm::Tagged") }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        let tag: String = sequence.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &"struct variant RegexForm::Tagged with 2 elements"))?;
        self.0.set_tag(&tag).map_err(de::Error::custom)?;
        sequence.next_element_seed(RawSeed(self.0))?.ok_or_else(|| de::Error::invalid_length(1, &"struct variant RegexForm::Tagged with 2 elements"))?;
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let (mut tag, mut value) = (false, false);
        while let Some(field) = map.next_key_seed(Field(&["tag", "value"]))? {
            match field {
                0 => { if tag { return Err(de::Error::duplicate_field("tag")); } tag = true; self.0.set_tag(&map.next_value::<String>()?).map_err(de::Error::custom)?; }
                1 => { if value { return Err(de::Error::duplicate_field("value")); } value = true; map.next_value_seed(RawSeed(self.0))?; }
                _ => { map.next_value::<de::IgnoredAny>()?; }
            }
        }
        if !tag { return Err(de::Error::missing_field("tag")); }
        if !value { return Err(de::Error::missing_field("value")); }
        Ok(())
    }
}
struct PhysicalMap<'a, 'budget>(&'a mut RegexFormBuilder<'budget>);
impl<'de> Visitor<'de> for PhysicalMap<'_, '_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("struct variant Value::Map") }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        sequence.next_element_seed(SequenceSeed { builder: self.0, physical: true, pairs: true })?.ok_or_else(|| de::Error::invalid_length(0, &"struct variant Value::Map with 2 elements"))?;
        let sorted = sequence.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &"struct variant Value::Map with 2 elements"))?;
        self.0.set_sorted(sorted).map_err(de::Error::custom)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let (mut entries, mut sorted) = (false, false);
        while let Some(field) = map.next_key_seed(Field(&["entries", "sorted"]))? {
            match field {
                0 => { if entries { return Err(de::Error::duplicate_field("entries")); } entries = true; map.next_value_seed(SequenceSeed { builder: self.0, physical: true, pairs: true })?; }
                1 => { if sorted { return Err(de::Error::duplicate_field("sorted")); } sorted = true; self.0.set_sorted(map.next_value()?).map_err(de::Error::custom)?; }
                _ => { map.next_value::<de::IgnoredAny>()?; }
            }
        }
        if !entries { return Err(de::Error::missing_field("entries")); }
        if !sorted { return Err(de::Error::missing_field("sorted")); }
        Ok(())
    }
}
struct StructSeed<'a, 'budget>(&'a mut RegexFormBuilder<'budget>);
impl<'de> DeserializeSeed<'de> for StructSeed<'_, '_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> { d.deserialize_map(self) }
}
impl<'de> Visitor<'de> for StructSeed<'_, '_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("a map") }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(name) = map.next_key::<String>()? { self.0.field(&name).map_err(de::Error::custom)?; map.next_value_seed(ValueSeed(self.0))?; }
        Ok(())
    }
}

struct Payload<T>(PhantomData<T>);
macro_rules! payload {
    ($type:ident, $variant:literal, $count:literal; $($field:ident : $ty:ty = $index:literal => $missing:expr),+ $(,)?) => {
        struct $type { $($field: $ty),+ }
        impl<'de> Visitor<'de> for Payload<$type> {
            type Value = $type;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(concat!("struct variant Value::", $variant)) }
            fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<$type, A::Error> {
                $(let $field = sequence.next_element()?.ok_or_else(|| de::Error::invalid_length($index, &concat!("struct variant Value::", $variant, " with ", stringify!($count), " elements")))?;)+
                Ok($type { $($field),+ })
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<$type, A::Error> {
                $(let mut $field: Option<$ty> = None;)+
                while let Some(field) = map.next_key_seed(Field(&[$(stringify!($field)),+]))? {
                    match field {
                        $($index => { if $field.is_some() { return Err(de::Error::duplicate_field(stringify!($field))); } $field = Some(map.next_value()?); })+
                        _ => { map.next_value::<de::IgnoredAny>()?; }
                    }
                }
                $(let $field = match $field { Some(value) => value, None => $missing };)+
                Ok($type { $($field),+ })
            }
        }
    };
}
payload!(Time, "Time", 3; value: i64 = 0 => return Err(de::Error::missing_field("value")), unit: TimeUnit = 1 => return Err(de::Error::missing_field("unit")), bit_width: u8 = 2 => return Err(de::Error::missing_field("bit_width")));
payload!(Timestamp, "Timestamp", 3; value: i64 = 0 => return Err(de::Error::missing_field("value")), unit: TimeUnit = 1 => return Err(de::Error::missing_field("unit")), timezone: Option<String> = 2 => None);
payload!(Duration, "Duration", 2; value: i64 = 0 => return Err(de::Error::missing_field("value")), unit: TimeUnit = 1 => return Err(de::Error::missing_field("unit")));
payload!(DayTime, "IntervalDayTime", 2; days: i32 = 0 => return Err(de::Error::missing_field("days")), milliseconds: i32 = 1 => return Err(de::Error::missing_field("milliseconds")));
payload!(Nano, "IntervalMonthDayNano", 3; months: i32 = 0 => return Err(de::Error::missing_field("months")), days: i32 = 1 => return Err(de::Error::missing_field("days")), nanoseconds: i64 = 2 => return Err(de::Error::missing_field("nanoseconds")));
payload!(Micro, "IntervalMonthDayMicro", 3; months: i32 = 0 => return Err(de::Error::missing_field("months")), days: i32 = 1 => return Err(de::Error::missing_field("days")), nanoseconds: i64 = 2 => return Err(de::Error::missing_field("nanoseconds")));
payload!(Range, "TimestampTzRange", 2; lower: i64 = 0 => return Err(de::Error::missing_field("lower")), upper: i64 = 1 => return Err(de::Error::missing_field("upper")));

impl<'de> Visitor<'de> for ValueSeed<'_, '_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("enum Value") }
    fn visit_enum<A: EnumAccess<'de>>(self, access: A) -> Result<(), A::Error> {
        let (variant, value) = access.variant_seed(Name(VALUES))?;
        let builder = self.0;
        let leaf = match variant {
            0 => { value.unit_variant()?; Value::Undefined } 1 => { value.unit_variant()?; Value::Null }
            2 => Value::Bool(value.newtype_variant()?), 3 => Value::I8(value.newtype_variant()?), 4 => Value::I16(value.newtype_variant()?),
            5 => Value::I32(value.newtype_variant()?), 6 => Value::I64(value.newtype_variant()?), 7 => Value::U8(value.newtype_variant()?),
            8 => Value::U16(value.newtype_variant()?), 9 => Value::U32(value.newtype_variant()?), 10 => Value::U64(value.newtype_variant()?),
            11 => Value::F16(value.newtype_variant()?), 12 => Value::F32(f32::from_bits(value.newtype_variant()?)), 13 => Value::F64(f64::from_bits(value.newtype_variant()?)),
            14 => return builder.value_text(&value.newtype_variant::<String>()?).map_err(de::Error::custom),
            15 => Value::Binary(value.newtype_variant()?), 16 => Value::FixedSizeBinary(value.newtype_variant()?), 17 => Value::Decimal(value.newtype_variant::<DecimalValue>()?),
            18 => Value::Date(value.newtype_variant::<CalendarDate>()?), 19 => Value::DateDay(value.newtype_variant()?), 20 => Value::DateMilli(value.newtype_variant()?),
            21 => { let v = value.struct_variant(&["value", "unit", "bit_width"], Payload::<Time>(PhantomData))?; Value::Time { value: v.value, unit: v.unit, bit_width: v.bit_width } }
            22 => { let v = value.struct_variant(&["value", "unit", "timezone"], Payload::<Timestamp>(PhantomData))?; Value::Timestamp { value: v.value, unit: v.unit, timezone: v.timezone } }
            23 => { let v = value.struct_variant(&["value", "unit"], Payload::<Duration>(PhantomData))?; Value::Duration { value: v.value, unit: v.unit } }
            24 => Value::IntervalYearMonth(value.newtype_variant()?),
            25 => { let v = value.struct_variant(&["days", "milliseconds"], Payload::<DayTime>(PhantomData))?; Value::IntervalDayTime { days: v.days, milliseconds: v.milliseconds } }
            26 => { let v = value.struct_variant(&["months", "days", "nanoseconds"], Payload::<Nano>(PhantomData))?; Value::IntervalMonthDayNano { months: v.months, days: v.days, nanoseconds: v.nanoseconds } }
            27 => { let v = value.struct_variant(&["months", "days", "nanoseconds"], Payload::<Micro>(PhantomData))?; Value::IntervalMonthDayMicro { months: v.months, days: v.days, nanoseconds: v.nanoseconds } }
            28 => return builder.value_keyword(&value.newtype_variant::<String>()?).map_err(de::Error::custom),
            29 => Value::Uuid(value.newtype_variant()?), 30 => Value::Uri(value.newtype_variant()?), 31 => Value::Transit(value.newtype_variant()?),
            32 => Value::Oid(value.newtype_variant()?), 33 => Value::RegClass(value.newtype_variant()?), 34 => Value::RegProc(value.newtype_variant()?),
            35 | 36 | 38 => {
                match variant { 35 => builder.begin_value_list(), 36 => builder.begin_value_set(), _ => builder.begin_value_fixed_size_list() }.map_err(de::Error::custom)?;
                value.newtype_variant_seed(SequenceSeed { builder, physical: true, pairs: false })?;
                return builder.end().map_err(de::Error::custom);
            }
            37 => { builder.begin_value_map(false).map_err(de::Error::custom)?; value.struct_variant(&["entries", "sorted"], PhysicalMap(builder))?; return builder.end().map_err(de::Error::custom); }
            39 => { let v = value.struct_variant(&["lower", "upper"], Payload::<Range>(PhantomData))?; Value::TimestampTzRange { lower: v.lower, upper: v.upper } }
            40 => { builder.begin_value_struct().map_err(de::Error::custom)?; value.newtype_variant_seed(StructSeed(builder))?; return builder.end().map_err(de::Error::custom); }
            _ => unreachable!("closed physical names"),
        };
        builder.value_leaf(&leaf).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::serde::de::value::{Error, MapDeserializer, U64Deserializer, BorrowedBytesDeserializer};

    #[test]
    fn field_identifier_maps_accept_numeric_indices_and_ignore_unknown_numeric_keys() {
        let mut builder = RegexFormBuilder::new_untracked();
        builder.begin_tagged("").unwrap();
        let fields = vec![(0u64, serde_json::json!("reader")), (91u64, serde_json::json!([1,2])), (1u64, serde_json::json!({"Literal":"Null"}))];
        Tagged(&mut builder).visit_map(MapDeserializer::<_, serde_json::Error>::new(fields.into_iter())).unwrap();
        builder.end().unwrap();
        assert_eq!(serde_json::to_string(&builder.finish().unwrap()).unwrap(), r#"{"Tagged":{"tag":"reader","value":{"Literal":"Null"}}}"#);

        let mut builder = RegexFormBuilder::new_untracked();
        builder.begin_literal().unwrap(); builder.begin_value_map(false).unwrap();
        let fields = vec![(1u64, serde_json::json!(true)), (0u64, serde_json::json!([["Null", {"I64":7}]]))];
        PhysicalMap(&mut builder).visit_map(MapDeserializer::<_, serde_json::Error>::new(fields.into_iter())).unwrap();
        builder.end().unwrap(); builder.end().unwrap();
        assert_eq!(serde_json::to_string(&builder.finish().unwrap()).unwrap(), r#"{"Literal":{"Map":{"entries":[["Null",{"I64":7}]],"sorted":true}}}"#);

        let fields = vec![(0u64, serde_json::json!(3)), (1u64, serde_json::json!("Second")), (2u64, serde_json::json!(64)), (99u64, serde_json::json!(null))];
        let value = Payload::<Time>(PhantomData).visit_map(MapDeserializer::<_, serde_json::Error>::new(fields.into_iter())).unwrap();
        assert_eq!((value.value, value.unit, value.bit_width), (3, TimeUnit::Second, 64));
    }

    #[test]
    fn field_and_variant_identifier_callbacks_preserve_derive_indices_bytes_and_error_expectations() {
        assert_eq!(Field(&["tag", "value"]).deserialize(U64Deserializer::<Error>::new(1)).unwrap(), 1);
        assert_eq!(Field(&["tag", "value"]).deserialize(U64Deserializer::<Error>::new(2)).unwrap(), usize::MAX);
        assert_eq!(Field(&["tag", "value"]).deserialize(BorrowedBytesDeserializer::<Error>::new(b"tag")).unwrap(), 0);
        assert_eq!(Field(&["tag", "value"]).deserialize(BorrowedBytesDeserializer::<Error>::new(&[255])).unwrap(), usize::MAX);
        assert_eq!(Name(FORMS).deserialize(U64Deserializer::<Error>::new(11)).unwrap(), 11);
        let error = Name(FORMS).deserialize(U64Deserializer::<Error>::new(12)).unwrap_err();
        assert_eq!(error.to_string(), "invalid value: integer `12`, expected variant index 0 <= i < 12");
        let error = Name(FORMS).deserialize(BorrowedBytesDeserializer::<Error>::new(&[255])).unwrap_err();
        assert!(error.to_string().starts_with("unknown variant `�`, expected one of"));
    }
}
