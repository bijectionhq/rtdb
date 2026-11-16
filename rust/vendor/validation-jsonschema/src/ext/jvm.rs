use std::collections::{BTreeMap, BTreeSet};
use num_bigint::{BigInt, BigUint, Sign};
use roaring::RoaringTreemap;
use serde_json::{Number, Value};
use crate::Draft;
use super::{cmp, numeric::{self, Numeric}};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Semantics { Exact, Everit, Skema }
impl Semantics {
    pub(crate) fn for_draft(enabled: bool, draft: Draft) -> Self {
        if !enabled { Self::Exact }
        else if matches!(draft, Draft::Draft4 | Draft::Draft6 | Draft::Draft7) { Self::Everit }
        else { Self::Skema }
    }
    pub(crate) fn integer(self, number: &Number) -> bool {
        match self {
            Self::Exact => numeric::is_integer(number),
            Self::Everit => numeric::is_literal_integer(number),
            Self::Skema => number.as_str().split_once('.').is_none_or(|(_, fraction)| fraction.bytes().all(|value| value == b'0')),
        }
    }
    pub(crate) fn number_equal(self, left: &Number, right: &Number) -> bool {
        numeric::equal(left, right) && (self != Self::Everit || numeric::is_literal_integer(left) || numeric::is_literal_integer(right) || scale(left) == scale(right))
    }
    pub(crate) fn equal(self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Number(left), Value::Number(right)) => self.number_equal(left, right),
            (Value::Array(left), Value::Array(right)) => left.len() == right.len() && left.iter().zip(right).all(|(left, right)| self.equal(left, right)),
            (Value::Object(left), Value::Object(right)) => left.len() == right.len() && left.iter().zip(right).all(|((lk, lv), (rk, rv))| lk == rk && self.equal(lv, rv)),
            _ => cmp::equal(left, right),
        }
    }
    pub(crate) fn unique(self, items: &[Value]) -> bool {
        match self {
            Self::Exact => items.iter().map(NormalizedValue::new).collect::<BTreeSet<_>>().len() == items.len(),
            Self::Skema => {
                let mut seen = BTreeSet::new();
                items.iter().all(|value| seen.insert((java_hash(value), NormalizedValue::new(value))))
            }
            Self::Everit => {
                let mut groups = BTreeMap::<NormalizedValue, CompatibilityGroup>::new();
                for (id, value) in items.iter().enumerate() {
                    let mut signature = Vec::new();
                    let mut position = 0;
                    decimal_positions(value, &mut position, &mut signature);
                    let group = groups.entry(NormalizedValue::new(value)).or_default();
                    if group.has_compatible(&signature) { return false; }
                    group.insert(id as u64, signature);
                }
                true
            }
        }
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
enum NormalizedValue {
    Null, Boolean(bool), Number(Numeric), String(String), Array(Vec<Self>), Object(Vec<(String, Self)>),
}
impl NormalizedValue {
    fn new(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Boolean(*value),
            Value::Number(value) => Self::Number(Numeric::new(value)),
            Value::String(value) => Self::String(value.clone()),
            Value::Array(values) => Self::Array(values.iter().map(Self::new).collect()),
            Value::Object(values) => Self::Object(values.iter().map(|(key, value)| (key.clone(), Self::new(value))).collect()),
        }
    }
}
#[derive(Default)]
struct CompatibilityGroup { members: RoaringTreemap, positions: BTreeMap<usize, Scales> }
#[derive(Default)]
struct Scales { decimal: RoaringTreemap, values: BTreeMap<BigInt, RoaringTreemap> }
impl CompatibilityGroup {
    fn has_compatible(&self, signature: &[(usize, BigInt)]) -> bool {
        let mut candidates = self.members.clone();
        for (position, scale) in signature {
            if let Some(scales) = self.positions.get(position) {
                let mut conflicts = scales.decimal.clone();
                if let Some(same) = scales.values.get(scale) { conflicts -= same; }
                candidates -= &conflicts;
                if candidates.is_empty() { return false; }
            }
        }
        !candidates.is_empty()
    }
    fn insert(&mut self, id: u64, signature: Vec<(usize, BigInt)>) {
        self.members.insert(id);
        for (position, scale) in signature {
            let scales = self.positions.entry(position).or_default();
            scales.decimal.insert(id);
            scales.values.entry(scale).or_default().insert(id);
        }
    }
}
fn decimal_positions(value: &Value, position: &mut usize, output: &mut Vec<(usize, BigInt)>) {
    match value {
        Value::Number(number) => {
            if !numeric::is_literal_integer(number) { output.push((*position, scale(number))); }
            *position += 1;
        }
        Value::Array(values) => for value in values { decimal_positions(value, position, output); },
        Value::Object(values) => for value in values.values() { decimal_positions(value, position, output); },
        _ => {}
    }
}
fn scale(number: &Number) -> BigInt {
    let (mantissa, exponent) = number.as_str().split_once(['e', 'E']).unwrap_or((number.as_str(), "0"));
    BigInt::from(mantissa.split_once('.').map_or(0, |(_, value)| value.len())) - exponent.parse::<BigInt>().expect("JSON number exponent")
}
fn string_hash(value: &str) -> i32 { value.encode_utf16().fold(0i32, |hash, unit| hash.wrapping_mul(31).wrapping_add(i32::from(unit))) }
fn coefficient_hash(number: &Number) -> i32 {
    let mantissa = number.as_str().split(['e', 'E']).next().expect("number mantissa");
    let digits: Vec<u8> = mantissa.bytes().filter(u8::is_ascii_digit).collect();
    let coefficient = BigUint::parse_bytes(&digits, 10).expect("decimal coefficient");
    let hash = coefficient.to_u32_digits().iter().rev().fold(0u32, |hash, word| hash.wrapping_mul(31).wrapping_add(*word));
    if mantissa.starts_with('-') { hash.wrapping_neg() as i32 } else { hash as i32 }
}
fn low_int(value: &BigInt) -> i32 {
    let (sign, digits) = value.to_u32_digits();
    let value = digits.first().copied().unwrap_or(0);
    if sign == Sign::Minus { value.wrapping_neg() as i32 } else { value as i32 }
}
fn java_hash(value: &Value) -> i32 {
    match value {
        Value::Null => 0,
        Value::Bool(value) => if *value { 1231 } else { 1237 },
        Value::String(value) => string_hash(value),
        Value::Number(number) if numeric::is_literal_integer(number) => {
            if let Ok(value) = number.as_str().parse::<i32>() { value }
            else if let Ok(value) = number.as_str().parse::<i64>() { (value as u64 ^ (value as u64 >> 32)) as i32 }
            else { coefficient_hash(number) }
        }
        Value::Number(number) => coefficient_hash(number).wrapping_mul(31).wrapping_add(low_int(&scale(number))),
        Value::Array(values) => values.iter().fold(1i32, |hash, value| hash.wrapping_mul(31).wrapping_add(java_hash(value))),
        Value::Object(values) => values.iter().fold(0i32, |hash, (key, value)| hash.wrapping_add(string_hash(key) ^ java_hash(value))),
    }
}

pub(crate) fn meta_schema_view(draft: Draft, schema: &Value) -> Value {
    fn collect(value: &Value, draft: Draft, schemas: &mut BTreeMap<usize, Draft>) {
        let draft = draft.detect(value).unwrap_or(draft);
        schemas.insert(value as *const Value as usize, draft);
        for child in draft.subresources_of(value) { collect(child, draft, schemas); }
    }
    fn copy(value: &Value, schemas: &BTreeMap<usize, Draft>) -> Value {
        match value {
            Value::Object(values) => {
                let draft = schemas.get(&(value as *const Value as usize));
                Value::Object(values.iter().map(|(key, value)| {
                    let count = draft.is_some() && value.is_number() && matches!(key.as_str(), "minItems" | "maxItems" | "minProperties" | "maxProperties" | "minLength" | "maxLength");
                    let multiple = draft.is_some() && key == "multipleOf" && value.is_number();
                    let contains = matches!(draft, Some(Draft::Draft201909 | Draft::Draft202012)) && matches!(key.as_str(), "minContains" | "maxContains");
                    (key.clone(), if multiple { Value::from(1) } else if count || contains { Value::from(0) } else { copy(value, schemas) })
                }).collect())
            }
            Value::Array(values) => Value::Array(values.iter().map(|value| copy(value, schemas)).collect()),
            _ => value.clone(),
        }
    }
    let mut schemas = BTreeMap::new();
    collect(schema, draft, &mut schemas);
    copy(schema, &schemas)
}

pub(crate) fn int_value(number: &Number) -> i32 {
    let (mantissa, exponent) = number.as_str().split_once(['e', 'E']).unwrap_or((number.as_str(), "0"));
    let fraction = mantissa.split_once('.').map_or(0, |(_, value)| value.len());
    let exponent = exponent.parse::<BigInt>().expect("JSON number exponent") - BigInt::from(fraction);
    let digits: Vec<u8> = mantissa.bytes().filter(u8::is_ascii_digit).collect();
    let end = if exponent.sign() == Sign::Minus {
        use num_traits::ToPrimitive;
        digits.len().saturating_sub(exponent.magnitude().to_usize().unwrap_or(usize::MAX))
    } else { digits.len() };
    let mut value = digits[..end].iter().fold(0u32, |value, digit| value.wrapping_mul(10).wrapping_add(u32::from(*digit - b'0')));
    if exponent.sign() != Sign::Minus {
        if exponent >= BigInt::from(32u8) { return 0; }
        use num_traits::ToPrimitive;
        for _ in 0..exponent.to_usize().expect("bounded nonnegative exponent") { value = value.wrapping_mul(10); }
    }
    if mantissa.starts_with('-') { value.wrapping_neg() as i32 } else { value as i32 }
}
pub(crate) fn count(number: &Number, semantics: Semantics, fractional: bool) -> Option<i128> {
    match semantics {
        Semantics::Exact => numeric::unsigned_limit(number, fractional).map(i128::from),
        Semantics::Everit if numeric::is_literal_integer(number) => number.as_str().parse::<i32>().ok().map(i128::from),
        Semantics::Everit => None,
        Semantics::Skema => Some(i128::from(int_value(number))),
    }
}
