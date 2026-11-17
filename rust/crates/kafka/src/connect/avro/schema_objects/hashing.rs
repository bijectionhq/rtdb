use super::*;
use super::evaluation::{Evaluation, IdentityWitness, SchemaType};

fn step(hash: i32, value: i32) -> i32 { hash.wrapping_mul(31).wrapping_add(value) }
fn guard(depth: usize) -> Result<()> { if depth > 128 { Err(invalid("Typed Java hashing depth exceeded")) } else { Ok(()) } }
pub(super) fn text(value: &str) -> i32 { value.encode_utf16().fold(0i32, |hash, unit| step(hash, i32::from(unit))) }
pub(super) fn boolean(value: bool) -> i32 { if value { 1231 } else { 1237 } }
fn long(value: i64) -> i32 { value as i32 ^ (value >> 32) as i32 }
fn float(value: f32) -> i32 { if value.is_nan() { 0x7fc00000 } else { value.to_bits() as i32 } }
fn double_bits(value: f64) -> i64 { if value.is_nan() { 0x7ff8000000000000 } else { value.to_bits() as i64 } }
pub(super) fn schema_type(kind: &Kind) -> SchemaType {
    match kind {
        Kind::Scalar(Scalar::Bool) => SchemaType::Bool, Kind::Scalar(Scalar::I8) => SchemaType::I8,
        Kind::Scalar(Scalar::I16) => SchemaType::I16, Kind::Scalar(Scalar::I32) => SchemaType::I32,
        Kind::Scalar(Scalar::I64) => SchemaType::I64, Kind::Scalar(Scalar::F32) => SchemaType::F32,
        Kind::Scalar(Scalar::F64) => SchemaType::F64, Kind::Scalar(Scalar::String) => SchemaType::String,
        Kind::Scalar(Scalar::Bytes) => SchemaType::Bytes, Kind::Array(_) => SchemaType::Array,
        Kind::Map(_, _) => SchemaType::Map, Kind::Struct(_) => SchemaType::Struct,
    }
}

pub(super) fn schema(view: ObjectView<'_>, evaluation: &mut Evaluation<'_, impl IdentityWitness>, depth: usize) -> Result<i32> {
    let view = view.actual()?;
    if let Object::Built { hash, .. } = &view.arena.objects[view.object.index] { if let Some(hash) = hash.get() { return Ok(*hash); } }
    guard(depth)?;
    match &view.arena.objects[view.object.index] {
        Object::Builder { .. } => evaluation.witness.builder_hash(view, evaluation.context),
        Object::Wrapper { target, optional } => Ok(boolean(*optional).wrapping_add(schema(view.child(*target), evaluation, depth + 1)?)),
        Object::Built { builder, hash } => {
            if let Some(hash) = hash.get() { return Ok(*hash); }
            let body = view.child(*builder).body()?; let metadata = &body.body.metadata;
            let mut computed = step(1, evaluation.witness.type_hash(schema_type(&body.body.kind), evaluation.context)?);
            computed = step(computed, boolean(body.optional));
            computed = step(computed, match &metadata.default { Some(default) => value(default, body.arena, evaluation, depth + 1)?, None => 0 });
            let fields_hash = if let Kind::Struct(fields) = &body.body.kind {
                let mut fields_hash = 1;
                for (index, field) in fields.iter().enumerate() {
                    let index = i32::try_from(index).map_err(|_| super::super::resolution::overflow())?;
                    let field_hash = step(step(step(1, text(&field.name)), index), schema(ObjectView { arena: body.arena, object: field.schema }, evaluation, depth + 1)?);
                    fields_hash = step(fields_hash, field_hash);
                }
                fields_hash
            } else { 0 };
            computed = step(computed, fields_hash);
            computed = step(computed, if let Kind::Map(key, _) = &body.body.kind { schema(ObjectView { arena: body.arena, object: *key }, evaluation, depth + 1)? } else { 0 });
            computed = step(computed, match &body.body.kind {
                Kind::Map(_, element) | Kind::Array(element) => schema(ObjectView { arena: body.arena, object: *element }, evaluation, depth + 1)?, _ => 0,
            });
            computed = step(computed, metadata.name.as_deref().map_or(0, text));
            computed = step(computed, metadata.version.unwrap_or(0));
            computed = step(computed, metadata.doc.as_deref().map_or(0, text));
            computed = step(computed, metadata.parameters.as_ref().map_or(0, |parameters| parameters.iter().fold(0i32, |hash, (key, value)| hash.wrapping_add(text(key) ^ text(value)))));
            if hash.set(computed).is_err() && hash.get() != Some(&computed) { return Err(invalid("Conflicting source schema hash observations")); }
            Ok(computed)
        }
        Object::Imported(_) => unreachable!(),
    }
}

pub(super) fn value(source: &JavaValue, owner: &SchemaObjects, evaluation: &mut Evaluation<'_, impl IdentityWitness>, depth: usize) -> Result<i32> {
    guard(depth)?;
    Ok(match source {
        JavaValue::Null => 0, JavaValue::Bool(value) => boolean(*value),
        JavaValue::I8(value) => i32::from(*value), JavaValue::I16(value) => i32::from(*value), JavaValue::I32(value) => *value,
        JavaValue::I64(value) | JavaValue::Date(value) => long(*value), JavaValue::F32(value) => float(*value), JavaValue::F64(value) => long(double_bits(*value)),
        JavaValue::String(value) => text(value), JavaValue::ByteArray(storage) => evaluation.witness.byte_array_hash(storage, evaluation.context)?,
        JavaValue::ByteBuffer(buffer) => buffer.remaining().iter().rev().fold(1i32, |hash, byte| step(hash, i32::from(*byte as i8))),
        JavaValue::Decimal(decimal) => magnitude_hash(&decimal.coefficient).wrapping_mul(31).wrapping_add(decimal.scale),
        JavaValue::Array(values) => {
            let mut hash = 1; for child in values { hash = step(hash, value(child, owner, evaluation, depth + 1)?); } hash
        }
        JavaValue::Map(entries) => {
            let mut hash = 0i32;
            let map_owner = entries.owner(owner)?;
            for (key, child) in entries.entries() { let key_hash = value(key.value(), key.owner(map_owner)?, evaluation, depth + 1)?; hash = hash.wrapping_add(key_hash ^ value(child, map_owner, evaluation, depth + 1)?); }
            hash
        }
        JavaValue::Struct { schema: schema_id, values } => {
            let values_hash = deep_list(values, owner, evaluation, depth + 1)?;
            step(step(1, schema(owner.value_schema(*schema_id)?, evaluation, depth + 1)?), values_hash)
        }
    })
}
fn deep_list(values: &[JavaValue], owner: &SchemaObjects, evaluation: &mut Evaluation<'_, impl IdentityWitness>, depth: usize) -> Result<i32> {
    let mut hash = 1;
    for child in values {
        let child_hash = if let JavaValue::ByteArray(storage) = child { storage.bytes().iter().fold(1i32, |hash, byte| step(hash, i32::from(*byte as i8))) }
        else { value(child, owner, evaluation, depth + 1)? };
        hash = step(hash, child_hash);
    }
    Ok(hash)
}

#[derive(Clone, Copy)]
struct Magnitude<'a> { bytes: &'a [u8], negative: bool, last_nonzero: Option<usize>, start: usize }
impl<'a> Magnitude<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        let negative = bytes[0] & 128 != 0; let last_nonzero = bytes.iter().rposition(|byte| *byte != 0);
        let mut result = Self { bytes, negative, last_nonzero, start: 0 };
        while result.start < bytes.len() && result.byte(result.start) == 0 { result.start += 1; }
        result
    }
    fn byte(self, index: usize) -> u8 {
        if !self.negative { self.bytes[index] }
        else { match self.last_nonzero { Some(last) if index < last => !self.bytes[index], Some(last) if index == last => self.bytes[index].wrapping_neg(), _ => 0 } }
    }
    fn len(self) -> usize { self.bytes.len() - self.start }
    fn sign(self) -> i32 { if self.len() == 0 { 0 } else if self.negative { -1 } else { 1 } }
    fn compare(self, other: Self) -> std::cmp::Ordering {
        self.len().cmp(&other.len()).then_with(|| (self.start..self.bytes.len()).map(|index| self.byte(index)).cmp((other.start..other.bytes.len()).map(|index| other.byte(index))))
    }
}
fn magnitude_hash(bytes: &[u8]) -> i32 {
    let magnitude = Magnitude::new(bytes); let mut hash = 0i32; let mut index = magnitude.start;
    while index < bytes.len() {
        let width = if index == magnitude.start { let remainder = magnitude.len() % 4; if remainder == 0 { 4 } else { remainder } } else { 4 };
        let mut word = 0u32; for _ in 0..width { word = (word << 8) | u32::from(magnitude.byte(index)); index += 1; }
        hash = step(hash, word as i32);
    }
    hash.wrapping_mul(magnitude.sign())
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Comparable { Bool, I8, I16, I32, I64, F32, F64, String, Date, Decimal }
pub(super) fn comparable(value: &JavaValue) -> Option<Comparable> {
    Some(match value {
        JavaValue::Bool(_) => Comparable::Bool, JavaValue::I8(_) => Comparable::I8, JavaValue::I16(_) => Comparable::I16,
        JavaValue::I32(_) => Comparable::I32, JavaValue::I64(_) => Comparable::I64, JavaValue::F32(_) => Comparable::F32,
        JavaValue::F64(_) => Comparable::F64, JavaValue::String(_) => Comparable::String, JavaValue::Date(_) => Comparable::Date,
        JavaValue::Decimal(_) => Comparable::Decimal, _ => return None,
    })
}
fn order(order: std::cmp::Ordering) -> i32 { match order { std::cmp::Ordering::Less => -1, std::cmp::Ordering::Equal => 0, std::cmp::Ordering::Greater => 1 } }
pub(super) fn compare(class: Comparable, left: &JavaValue, right: &JavaValue, context: &mut ConversionContext) -> Result<i32> {
    if comparable(right) != Some(class) { return Ok(0); }
    Ok(match (left, right) {
        (JavaValue::Bool(left), JavaValue::Bool(right)) => order(left.cmp(right)),
        (JavaValue::I8(left), JavaValue::I8(right)) => i32::from(*left) - i32::from(*right),
        (JavaValue::I16(left), JavaValue::I16(right)) => i32::from(*left) - i32::from(*right),
        (JavaValue::I32(left), JavaValue::I32(right)) => order(left.cmp(right)),
        (JavaValue::I64(left), JavaValue::I64(right)) | (JavaValue::Date(left), JavaValue::Date(right)) => order(left.cmp(right)),
        (JavaValue::F32(left), JavaValue::F32(right)) => if left < right { -1 } else if left > right { 1 } else { order(float(*left).cmp(&float(*right))) },
        (JavaValue::F64(left), JavaValue::F64(right)) => if left < right { -1 } else if left > right { 1 } else { order(double_bits(*left).cmp(&double_bits(*right))) },
        (JavaValue::String(left), JavaValue::String(right)) => {
            for (left, right) in left.encode_utf16().zip(right.encode_utf16()) { if left != right { return Ok(i32::from(left) - i32::from(right)); } }
            let left = i32::try_from(left.encode_utf16().count()).map_err(|_| super::super::resolution::overflow())?;
            let right = i32::try_from(right.encode_utf16().count()).map_err(|_| super::super::resolution::overflow())?; left - right
        }
        (JavaValue::Decimal(left), JavaValue::Decimal(right)) => decimal_compare(left, right, context)?,
        _ => return Err(invalid("Screened Java Comparable class differs from its candidate")),
    })
}

struct DecimalMagnitude { chunks: Vec<u32>, precision: usize }
impl DecimalMagnitude {
    fn new(magnitude: Magnitude<'_>, context: &mut ConversionContext) -> Result<Self> {
        let count = add(magnitude.len(), 1)?; context.reserve(add(mul(mul(count, size_of::<u32>())?, 16)?, 256)?)?;
        let mut chunks = Vec::new(); chunks.try_reserve_exact(count).map_err(|cause| error("allocation-failed", cause))?; chunks.push(0u32);
        for index in magnitude.start..magnitude.bytes.len() {
            let mut carry = u64::from(magnitude.byte(index));
            for chunk in &mut chunks { let next = u64::from(*chunk) * 256 + carry; *chunk = (next % 1_000_000_000) as u32; carry = next / 1_000_000_000; }
            if carry != 0 { chunks.push(carry as u32); }
        }
        let high = *chunks.last().unwrap(); let precision = add(mul(chunks.len() - 1, 9)?, if high == 0 { 1 } else { high.ilog10() as usize + 1 })?;
        Ok(Self { chunks, precision })
    }
    fn digit(&self, from_left: usize) -> u32 {
        if from_left >= self.precision { return 0; }
        let from_right = self.precision - from_left - 1; self.chunks[from_right / 9] / 10u32.pow((from_right % 9) as u32) % 10
    }
}
fn decimal_compare(left: &BigDecimal, right: &BigDecimal, context: &mut ConversionContext) -> Result<i32> {
    let left_magnitude = Magnitude::new(&left.coefficient); let right_magnitude = Magnitude::new(&right.coefficient);
    let sign = left_magnitude.sign(); let right_sign = right_magnitude.sign();
    if sign != right_sign { return Ok(order(sign.cmp(&right_sign))); } if sign == 0 { return Ok(0); }
    if left.scale == right.scale { return Ok(order(left_magnitude.compare(right_magnitude)) * sign); }
    let left_digits = DecimalMagnitude::new(left_magnitude, context)?; let right_digits = DecimalMagnitude::new(right_magnitude, context)?;
    let left_exponent = i64::try_from(left_digits.precision).map_err(|_| super::super::resolution::overflow())? - i64::from(left.scale);
    let right_exponent = i64::try_from(right_digits.precision).map_err(|_| super::super::resolution::overflow())? - i64::from(right.scale);
    if left_exponent != right_exponent { return Ok(order(left_exponent.cmp(&right_exponent)) * sign); }
    for index in 0..left_digits.precision.max(right_digits.precision) {
        let left = left_digits.digit(index); let right = right_digits.digit(index); if left != right { return Ok(order(left.cmp(&right)) * sign); }
    }
    Ok(0)
}
