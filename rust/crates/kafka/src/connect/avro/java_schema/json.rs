use super::*;
use rtdb_types::java_text::JavaText;
use crate::connect::json_decimal::JsonDecimal;

/// Equality, hashing and retention may reject source-valid values beyond the native traversal depth limit of 128.
#[derive(Clone)]
pub(in crate::connect::avro) struct JsonNode { data: Arc<JsonData>, charge: Arc<MemoryReservation> }
struct JsonData { value: JsonValue }
pub(in crate::connect::avro) enum JsonValue {
    Null, Bool(bool), I32(i32), I64(i64), Integer(Integer), F32(f32), F64(f64), Decimal(JsonDecimal),
    Text(Text), Array(Vec<JsonNode>), Object(Members),
}
pub(in crate::connect::avro) struct Integer { negative: bool, words: Vec<u32> }
impl Integer {
    pub(super) fn with_decimal<T>(&self, context: &mut ConversionContext, use_text: impl FnOnce(&str) -> Result<T>) -> Result<T> {
        let slots = add(mul(self.words.len(), 2)?, 1)?;
        context.reserve(add(mul(mul(slots, size_of::<u32>())?, 16)?, 4096)?)?;
        let mut chunks = Vec::new();
        chunks.try_reserve_exact(slots).map_err(allocation)?;
        chunks.push(0u32);
        for word in self.words.iter().rev() {
            let mut carry = u64::from(*word);
            for chunk in &mut chunks {
                let value = u64::from(*chunk) * (1u64 << 32) + carry;
                *chunk = (value % 1_000_000_000) as u32;
                carry = value / 1_000_000_000;
            }
            while carry != 0 { chunks.push((carry % 1_000_000_000) as u32); carry /= 1_000_000_000; }
        }
        let bound = add(mul(chunks.len(), 9)?, 1)?;
        context.reserve(add(mul(bound, 16)?, 4096)?)?;
        let mut text = String::new();
        text.try_reserve_exact(bound).map_err(allocation)?;
        if self.negative && chunks.iter().any(|chunk| *chunk != 0) { text.push('-'); }
        use std::fmt::Write;
        write!(&mut text, "{}", chunks.last().unwrap()).map_err(|_| invalid("Integer decimal formatting failed"))?;
        for chunk in chunks[..chunks.len()-1].iter().rev() {
            write!(&mut text, "{chunk:09}").map_err(|_| invalid("Integer decimal formatting failed"))?;
        }
        use_text(&text)
    }
}
pub(in crate::connect::avro) struct Members {
    fields: Vec<(Text, JsonNode)>,
    lookup: Vec<usize>,
    charges: Vec<Arc<MemoryReservation>>,
}
#[derive(Default)]
pub(super) struct JsonSeen<'a> { values: Vec<&'a JsonData> }
fn step(hash: i32, value: i32) -> i32 { hash.wrapping_mul(31).wrapping_add(value) }
fn double_hash(value: f64) -> i32 {
    let bits = if value.is_nan() { 0x7ff8000000000000 } else { value.to_bits() };
    bits as i32 ^ (bits >> 32) as i32
}
fn float_bits(value: f32) -> u32 { if value.is_nan() { 0x7fc00000 } else { value.to_bits() } }
fn double_bits(value: f64) -> u64 { if value.is_nan() { 0x7ff8000000000000 } else { value.to_bits() } }
fn guard(depth: usize) -> Result<()> { guard_depth(depth, "JsonNode traversal exceeds the native traversal depth limit of 128") }

impl JsonNode {
    pub(super) fn value(&self) -> &JsonValue { &self.data.value }
    pub(super) fn get(&self, key: &str) -> Option<&JsonNode> {
        match &self.data.value { JsonValue::Object(values) => values.get_text(JavaText::from_utf8(key)), _ => None }
    }
    pub(super) fn integral_long(&self) -> Option<i64> {
        match &self.data.value {
            JsonValue::I32(value) => Some(i64::from(*value)),
            JsonValue::I64(value) => Some(*value),
            JsonValue::Integer(value) if value.words.len() <= 2 => {
                let magnitude = u64::from(value.words.first().copied().unwrap_or(0)) | (u64::from(value.words.get(1).copied().unwrap_or(0)) << 32);
                if value.negative { (magnitude <= 1u64 << 63).then(|| magnitude.wrapping_neg() as i64) }
                else { i64::try_from(magnitude).ok() }
            }
            _ => None,
        }
    }
    /// `bound` admits every allocation in `build` not separately retained by its returned child/text owners.
    pub(in crate::connect::avro) fn with(bound: usize, context: &mut ConversionContext, build: impl FnOnce(&mut ConversionContext) -> Result<JsonValue>) -> Result<Self> {
        let charge = context.reserve(add(add(bound, size_of::<JsonData>())?, 4096)?)?;
        Ok(Self { data: Arc::new(JsonData { value: build(context)? }), charge })
    }
    pub(in crate::connect::avro) fn integer_decimal(source: JavaText<'_>, context: &mut ConversionContext) -> Result<Self> {
        let mut units = source.units().peekable();
        let negative = units.peek() == Some(&45);
        if matches!(units.peek(), Some(43 | 45)) { units.next(); }
        let mut digits = 0usize;
        for unit in units {
            if !(48..=57).contains(&unit) { return Err(invalid("Invalid BigInteger node token")); }
            digits = add(digits, 1)?;
        }
        if digits == 0 { return Err(invalid("Empty BigInteger node token")); }
        let count = add(digits, 1)?;
        let bound = add(mul(mul(count, size_of::<u32>())?, 16)?, 256)?;
        Self::with(bound, context, |_| {
            let mut words = Vec::new();
            words.try_reserve_exact(count).map_err(allocation)?;
            words.push(0u32);
            for unit in source.units().skip(usize::from(matches!(source.units().next(), Some(43 | 45)))) {
                let mut carry = u64::from(unit - 48);
                for word in &mut words {
                    let next = u64::from(*word) * 10 + carry;
                    *word = next as u32;
                    carry = next >> 32;
                }
                if carry != 0 { words.push(carry as u32); }
            }
            while words.last() == Some(&0) { words.pop(); }
            Ok(JsonValue::Integer(Integer { negative: negative && !words.is_empty(), words }))
        })
    }
    pub(in crate::connect::avro) fn decimal(source: &str, context: &mut ConversionContext) -> Result<Self> {
        let bytes = source.as_bytes();
        let mut at = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
        let mut digits = 0;
        while bytes.get(at).is_some_and(u8::is_ascii_digit) { at += 1; digits += 1; }
        if bytes.get(at) == Some(&b'.') {
            at += 1;
            while bytes.get(at).is_some_and(u8::is_ascii_digit) { at += 1; digits += 1; }
        }
        if digits == 0 { return Err(invalid("Invalid DecimalNode token")); }
        if matches!(bytes.get(at), Some(b'e' | b'E')) {
            at += 1;
            if matches!(bytes.get(at), Some(b'+' | b'-')) { at += 1; }
            let start = at;
            while bytes.get(at).is_some_and(u8::is_ascii_digit) { at += 1; }
            if start == at { return Err(invalid("Invalid DecimalNode exponent")); }
        }
        if at != bytes.len() { return Err(invalid("Invalid DecimalNode token suffix")); }
        Self::with(add(mul(bytes.len(), 16)?, 4096)?, context, |_| Ok(JsonValue::Decimal(JsonDecimal::parse(source)?)))
    }
    pub(in crate::connect::avro) fn as_text(&self) -> Option<&Text> {
        if let JsonValue::Text(value) = &self.data.value { Some(value) } else { None }
    }
    fn decimal_double(value: &JsonDecimal, context: &mut ConversionContext) -> Result<f64> {
        context.reserve(add(mul(value.encoded_bound(), 16)?, 256)?)?;
        Ok(value.double_value())
    }
    pub(super) fn is_nan_double(&self, context: &mut ConversionContext) -> Result<bool> {
        Ok(match &self.data.value {
            JsonValue::F32(value) => value.is_nan(),
            JsonValue::F64(value) => value.is_nan(),
            JsonValue::Decimal(value) => Self::decimal_double(value, context)?.is_nan(),
            _ => false,
        })
    }
    pub(super) fn equal(&self, other: &Self, context: &mut ConversionContext, depth: usize) -> Result<bool> {
        if Arc::ptr_eq(&self.data, &other.data) { return Ok(true); }
        guard(depth)?;
        Ok(match (&self.data.value, &other.data.value) {
            (JsonValue::Null, JsonValue::Null) => true,
            (JsonValue::Bool(left), JsonValue::Bool(right)) => left == right,
            (JsonValue::I32(left), JsonValue::I32(right)) => left == right,
            (JsonValue::I64(left), JsonValue::I64(right)) => left == right,
            (JsonValue::Integer(left), JsonValue::Integer(right)) => left.negative == right.negative && left.words == right.words,
            (JsonValue::F32(left), JsonValue::F32(right)) => float_bits(*left) == float_bits(*right),
            (JsonValue::F64(left), JsonValue::F64(right)) => double_bits(*left) == double_bits(*right),
            (JsonValue::Decimal(left), JsonValue::Decimal(right)) => left == right,
            (JsonValue::Text(left), JsonValue::Text(right)) => left == right,
            (JsonValue::Array(left), JsonValue::Array(right)) => {
                if left.len() != right.len() { return Ok(false); }
                for (left, right) in left.iter().zip(right) {
                    if !left.equal(right, context, depth + 1)? { return Ok(false); }
                }
                true
            }
            (JsonValue::Object(left), JsonValue::Object(right)) => left.equal(right, context, depth + 1)?,
            _ => false,
        })
    }
    pub(super) fn hash(&self, context: &mut ConversionContext, depth: usize) -> Result<i32> {
        guard(depth)?;
        Ok(match &self.data.value {
            JsonValue::Null => 4,
            JsonValue::Bool(value) => if *value { 3 } else { 1 },
            JsonValue::I32(value) => *value,
            JsonValue::I64(value) => *value as i32 ^ (*value >> 32) as i32,
            JsonValue::Integer(value) => {
                let hash = value.words.iter().rev().fold(0i32, |hash, word| step(hash, *word as i32));
                if value.negative { hash.wrapping_neg() } else { hash }
            }
            JsonValue::F32(value) => float_bits(*value) as i32,
            JsonValue::F64(value) => double_hash(*value),
            JsonValue::Decimal(value) => double_hash(Self::decimal_double(value, context)?),
            JsonValue::Text(value) => value.view().java_hash_code(),
            JsonValue::Array(values) => {
                let mut hash = 1;
                for value in values { hash = step(hash, value.hash(context, depth + 1)?); }
                hash
            }
            JsonValue::Object(values) => values.hash(context, depth + 1)?,
        })
    }
    pub(super) fn retain<'a>(&'a self, context: &mut ConversionContext, seen: &mut JsonSeen<'a>, depth: usize) -> Result<()> {
        if seen.values.iter().any(|value| std::ptr::eq(*value, self.data.as_ref())) { return Ok(()); }
        guard(depth)?;
        let next = growth(&seen.values)?;
        if let Some((_, bytes)) = next { context.reserve(bytes)?; }
        grow(&mut seen.values, next)?;
        seen.values.push(self.data.as_ref());
        context.retain(Arc::clone(&self.charge))?;
        match &self.data.value {
            JsonValue::Text(value) => value.retain(context)?,
            JsonValue::Array(values) => for value in values { value.retain(context, seen, depth + 1)?; },
            JsonValue::Object(values) => values.retain(context, seen, depth + 1)?,
            _ => (),
        }
        Ok(())
    }
}

impl Members {
    pub(in crate::connect::avro) fn empty() -> Self { Self { fields: Vec::new(), lookup: Vec::new(), charges: Vec::new() } }
    pub(in crate::connect::avro) fn with(entry_vector_bytes: usize, context: &mut ConversionContext, build: impl FnOnce() -> Result<Vec<(Text, JsonNode)>>) -> Result<Self> {
        Self::with_context(entry_vector_bytes, context, |_| build())
    }
    pub(super) fn with_context(entry_vector_bytes: usize, context: &mut ConversionContext, build: impl FnOnce(&mut ConversionContext) -> Result<Vec<(Text, JsonNode)>>) -> Result<Self> {
        let payload = context.reserve(add(entry_vector_bytes, 4096)?)?;
        let mut fields = build(context)?;
        let index = context.reserve(add(mul(mul(fields.len(), size_of::<usize>())?, 16)?, 256)?)?;
        let mut lookup = Vec::new();
        lookup.try_reserve_exact(fields.len()).map_err(allocation)?;
        lookup.extend(0..fields.len());
        lookup.sort_unstable_by(|left, right| fields[*left].0.cmp(&fields[*right].0).then_with(|| left.cmp(right)));
        let mut start = 0;
        let mut kept = 0;
        while start < lookup.len() {
            let mut end = start + 1;
            while end < lookup.len() && fields[lookup[start]].0 == fields[lookup[end]].0 { end += 1; }
            let first = lookup[start];
            let last = lookup[end - 1];
            if first != last {
                let value = fields[last].1.clone();
                fields[first].1 = value;
            }
            lookup[kept] = first;
            kept += 1;
            start = end;
        }
        lookup.truncate(kept);
        lookup.sort_unstable();
        let mut ordinal = 0;
        let mut kept = 0;
        fields.retain(|_| {
            let keep = lookup.get(kept) == Some(&ordinal);
            ordinal += 1;
            if keep { kept += 1; }
            keep
        });
        lookup.clear();
        lookup.extend(0..fields.len());
        lookup.sort_unstable_by(|left, right| fields[*left].0.cmp(&fields[*right].0));
        Ok(Self { fields, lookup, charges: vec![payload, index] })
    }
    pub(in crate::connect::avro) fn iter(&self) -> impl ExactSizeIterator<Item = (&Text, &JsonNode)> {
        self.fields.iter().map(|(key, value)| (key, value))
    }
    pub(super) fn get(&self, key: &Text) -> Option<&JsonNode> {
        self.get_text(key.view())
    }
    pub(super) fn get_text(&self, key: JavaText<'_>) -> Option<&JsonNode> {
        self.lookup.binary_search_by(|index| self.fields[*index].0.view().units().cmp(key.units())).ok()
            .map(|index| &self.fields[self.lookup[index]].1)
    }
    pub(super) fn len(&self) -> usize { self.fields.len() }
    pub(super) fn entry(&self, index: usize) -> (&Text, &JsonNode) {
        let (key, value) = &self.fields[index];
        (key, value)
    }
    fn equal(&self, other: &Self, context: &mut ConversionContext, depth: usize) -> Result<bool> {
        if self.len() != other.len() { return Ok(false); }
        for (key, value) in self.iter() {
            let Some(other) = other.get(key) else { return Ok(false); };
            if !value.equal(other, context, depth + 1)? { return Ok(false); }
        }
        Ok(true)
    }
    fn hash(&self, context: &mut ConversionContext, depth: usize) -> Result<i32> {
        let mut hash = 0i32;
        for (key, value) in self.iter() { hash = hash.wrapping_add(key.view().java_hash_code() ^ value.hash(context, depth + 1)?); }
        Ok(hash)
    }
    pub(super) fn retain<'a>(&'a self, context: &mut ConversionContext, seen: &mut JsonSeen<'a>, depth: usize) -> Result<()> {
        for charge in &self.charges { context.retain(Arc::clone(charge))?; }
        for (key, value) in self.iter() {
            key.retain(context)?;
            value.retain(context, seen, depth + 1)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
