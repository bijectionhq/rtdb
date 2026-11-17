use num_bigint::BigInt;
use num_traits::{ToPrimitive,Zero};
use super::{Number,Result,invalid};
fn clojure_long(value: i64) -> i32 { if value == 0 { 0 } else { crate::murmur3_32(&value.to_le_bytes(),0) as i32 } }

fn integer(value: &BigInt) -> i32 {
    let (sign,digits) = value.to_u32_digits();
    let hash = digits.iter().rev().fold(0i32,|hash,digit|hash.wrapping_mul(31).wrapping_add(*digit as i32));
    if sign == num_bigint::Sign::Minus { hash.wrapping_neg() } else { hash }
}
fn long(value: i64) -> i32 { (value ^ ((value as u64 >> 32) as i64)) as i32 }

impl Number {
    /// Computes Java numeric-wrapper hashing, preserving Decimal scale and canonical floating NaN bits.
    /// Arbitrary-precision digit buffers require caller-admitted scratch storage.
    pub fn java_hash_code(&self) -> i32 {
        match self {
            Self::Byte(value) => i32::from(*value),Self::Short(value) => i32::from(*value),Self::Int(value) => *value,Self::Long(value) => long(*value),
            Self::BigInt(value) => value.to_i64().map_or_else(||integer(value),long),
            Self::Float(value) => if value.is_nan() { 0x7fc0_0000 } else { value.to_bits() as i32 },
            Self::Double(value) => long(if value.is_nan() { 0x7ff8_0000_0000_0000u64 as i64 } else { value.to_bits() as i64 }),
            Self::Decimal { coefficient,scale } => integer(coefficient).wrapping_mul(31).wrapping_add(*scale),
            Self::Ratio { numerator,denominator } => integer(numerator) ^ integer(denominator),
        }
    }
    /// Computes Clojure numeric hashing, including normalized Decimal scale and zero hashes for floating signed zero.
    /// Decimal normalization may allocate coefficient scratch and may reject scale underflow; the caller admits that work.
    pub fn hasheq(&self) -> Result<i32> {
        Ok(match self {
            Self::Byte(value) => clojure_long(i64::from(*value)),Self::Short(value) => clojure_long(i64::from(*value)),Self::Int(value) => clojure_long(i64::from(*value)),Self::Long(value) => clojure_long(*value),
            Self::BigInt(value) => value.to_i64().map_or_else(||integer(value),clojure_long),
            Self::Float(value) if *value == 0.0 => 0,Self::Double(value) if *value == 0.0 => 0,
            Self::Decimal { coefficient,scale } => {
                if coefficient.is_zero() { return Ok(0); }
                let mut coefficient = coefficient.clone();let mut scale = *scale;
                while (&coefficient % 10u8).is_zero() { scale = scale.checked_sub(1).ok_or_else(invalid)?;coefficient /= 10u8; }
                integer(&coefficient).wrapping_mul(31).wrapping_add(scale)
            }
            _ => self.java_hash_code(),
        })
    }
}
