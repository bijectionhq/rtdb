//! JVM numeric categories and Clojure operations for caller-admitted values.
//! Callers admit input, scratch and output allocations and retain their charges through the values' lifetimes.
//! Operation-specific `*_bound` methods cover their paired operations; parsing, casts, hashing and text need caller admission too.
//! This module owns no allocation scope, identity, namespace or dynamic arithmetic binding.
//! Object identity checks, reader admission and adapter-specific diagnostics remain with the caller.

use num_bigint::BigInt;
use num_traits::{Signed,ToPrimitive,Zero};
use crate::Anomaly;
use self::text::decimal_parts_text as decimal_text;
mod text;
pub use text::{decimal_coefficient_display, decimal_parts_text, write_decimal_coefficient};
mod parse;
mod hash;
mod arithmetic;
pub use arithmetic::{Binary,Dispatch,Unary};
mod comparison;
pub use comparison::{Comparison,Predicate};
mod classification;
pub use classification::Classification;
mod integral;
pub use integral::Integral;
mod bit;
pub use bit::{Bit,bit_not,bit_test};
mod extrema;
pub use extrema::{Extremum,ExtremumCall,Selected};

#[derive(Debug)]
/// `Invalid` requests an adapter-specific numeric diagnostic; `External` retains an operation's selected anomaly.
pub enum Error { Invalid,External(Anomaly) }
impl From<Anomaly> for Error { fn from(error: Anomaly) -> Self { Self::External(error) } }
/// A numeric outcome whose invalid-input diagnostic is selected by its consuming adapter.
pub type Result<T> = std::result::Result<T,Error>;
fn invalid() -> Error { Error::Invalid }

#[derive(Clone,Debug,PartialEq)]
/// Numeric payload with its JVM wrapper or Clojure exact-number category.
/// Ratio denominators must be positive and nonzero; callers establish their source representation before construction.
/// Cloning copies arbitrary-precision payloads and requires caller admission.
/// Derived `PartialEq` compares the stored Rust representation; use the named equality methods for JVM semantics.
pub enum Number {
    Byte(i8),Short(i16),Int(i32),Long(i64),BigInt(BigInt),Float(f32),Double(f64),
    Decimal { coefficient: BigInt,scale: i32 },Ratio { numerator: BigInt,denominator: BigInt },
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
/// Target primitive category for checked JVM numeric conversion.
pub enum Cast { Byte,Short,Int,Long,Float,Double }

impl Number {
    /// Parses a signed Java decimal long, rejecting whitespace and overflow.
    pub fn parse_long(text: &str) -> Result<Self> { parse::long(text) }
    /// Parses Java Double text, including hexadecimal literals, suffixes and ASCII control-space trimming.
    pub fn parse_double(text: &str) -> Result<Self> { parse::float(text, false) }
    /// Parses Java Float text and rounds the input directly to binary32.
    pub fn parse_float(text: &str) -> Result<Self> { parse::float(text, true) }
    /// Tests floating payloads without coercing or copying other numeric categories.
    pub fn is_nan(&self) -> bool { matches!(self,Self::Float(value) if value.is_nan()) || matches!(self,Self::Double(value) if value.is_nan()) }
    /// Parses a signed decimal integer into the Clojure BigInt category, including values that fit a long.
    pub fn integer(text: &str) -> Result<Self> { Ok(Self::BigInt(integer(text)?)) }
    /// Parses Java BigDecimal syntax and retains its signed 32-bit scale.
    pub fn decimal(text: &str) -> Result<Self> {
        let spelling = crate::decimal_spelling(text)?;
        let mantissa = spelling.ascii().split(['e','E']).next().ok_or_else(invalid)?;
        Ok(Self::Decimal { coefficient: integer(&mantissa.replace('.',""))?,scale: spelling.scale() })
    }
    /// Applies the checked boxed RT conversion, retaining its NaN and truncation rules.
    pub fn cast(kind: Cast,source: &Self) -> Result<Self> {
        Ok(match kind {
            Cast::Byte => Self::Byte(i8::try_from(source.long()?).map_err(|_|invalid())?),
            Cast::Short => Self::Short(i16::try_from(source.long()?).map_err(|_|invalid())?),
            Cast::Int => Self::Int(i32::try_from(source.long()?).map_err(|_|invalid())?),
            Cast::Long => Self::Long(source.long()?),
            Cast::Float => {
                if let Self::Float(value) = source { return Ok(Self::Float(*value)); }
                let value = source.double()?;
                if value < -f64::from(f32::MAX) || value > f64::from(f32::MAX) { return Err(invalid()); }
                Self::Float(value as f32)
            }
            Cast::Double => Self::Double(source.double()?),
        })
    }
    /// Applies checked primitive RT overloads, including direct integer-to-float rounding.
    /// The caller selects this path from emitted primitive metadata rather than a boxed runtime category.
    pub fn cast_primitive(kind: Cast,source: &Self) -> Result<Self> {
        if kind == Cast::Float {
            let value = match source { Self::Byte(value) => Some(*value as f32),Self::Short(value) => Some(*value as f32),Self::Int(value) => Some(*value as f32),Self::Long(value) => Some(*value as f32),_ => None };
            if let Some(value) = value { return Ok(Self::Float(value)); }
        }
        let bounds = match kind { Cast::Byte => Some((i64::from(i8::MIN),i64::from(i8::MAX))),Cast::Short => Some((i64::from(i16::MIN),i64::from(i16::MAX))),Cast::Int => Some((i64::from(i32::MIN),i64::from(i32::MAX))),_ => None };
        if let Some((min,max)) = bounds {
            let valid = match source {
                Self::Float(value) if kind == Cast::Int => !(*value < min as f32 || *value > max as f32),
                Self::Double(value) if kind == Cast::Int => !(*value < min as f64 || *value > max as f64),
                Self::Float(value) => *value >= min as f32 && *value <= max as f32,
                Self::Double(value) => *value >= min as f64 && *value <= max as f64,
                _ => return Self::cast(kind,source),
            };
            if !valid { return Err(invalid()); }
            let value = source.double()?;
            return Ok(match kind { Cast::Byte => Self::Byte(value as i8),Cast::Short => Self::Short(value as i16),Cast::Int => Self::Int(value as i32),_ => unreachable!() });
        }
        Self::cast(kind,source)
    }
    /// Bounds bytes in `text` and `printed` outputs from the stored payload size; scratch admission remains with the caller.
    pub fn text_capacity(&self) -> Result<usize> {
        let bits = match self {
            Self::BigInt(value) | Self::Decimal { coefficient: value,.. } => value.bits(),
            Self::Ratio { numerator,denominator } => numerator.bits().checked_add(denominator.bits()).ok_or_else(invalid)?,
            _ => 0,
        };
        usize::try_from(bits).ok().and_then(|bits|bits.checked_add(64)).ok_or_else(invalid)
    }
    /// Returns Java numeric object text, preserving Decimal scale and Ratio numerator/denominator spelling.
    pub fn text(&self) -> String {
        match self {
            Self::Byte(value) => value.to_string(),Self::Short(value) => value.to_string(),Self::Int(value) => value.to_string(),Self::Long(value) => value.to_string(),Self::BigInt(value) => value.to_string(),
            Self::Float(value) => crate::java_f32_text(*value),Self::Double(value) => crate::java_f64_text(*value),
            Self::Decimal { coefficient,scale } => decimal_text(coefficient,*scale),Self::Ratio { numerator,denominator } => format!("{numerator}/{denominator}"),
        }
    }
    /// Returns readable Clojure numeric text, including exact-number suffixes and symbolic nonfinite values.
    pub fn printed(&self) -> String {
        match self {
            Self::BigInt(_) => format!("{}N",self.text()),Self::Decimal { .. } => format!("{}M",self.text()),
            Self::Float(value) if value.is_nan() => "##NaN".into(),Self::Double(value) if value.is_nan() => "##NaN".into(),
            Self::Float(value) if value.is_infinite() => if value.is_sign_negative() { "##-Inf" } else { "##Inf" }.into(),
            Self::Double(value) if value.is_infinite() => if value.is_sign_negative() { "##-Inf" } else { "##Inf" }.into(),
            _ => self.text(),
        }
    }
    fn long(&self) -> Result<i64> {
        Ok(match self {
            Self::Byte(value) => i64::from(*value),Self::Short(value) => i64::from(*value),Self::Int(value) => i64::from(*value),Self::Long(value) => *value,
            Self::BigInt(value) => value.to_i64().ok_or_else(invalid)?,
            Self::Ratio { numerator,denominator } => (numerator / denominator).to_i64().ok_or_else(invalid)?,
            _ => { let value = self.double()?;if value < i64::MIN as f64 || value > i64::MAX as f64 { return Err(invalid()); } value as i64 }
        })
    }
    /// Converts to JVM double semantics, including ratio rounding and decimal signed underflow.
    pub fn double(&self) -> Result<f64> {
        Ok(match self {
            Self::Float(value) => f64::from(*value),Self::Double(value) => *value,
            Self::BigInt(value) => float_integer(value),
            Self::Decimal { coefficient,scale } => decimal_parts_to_f64(coefficient.clone(), *scale),
            Self::Ratio { numerator,denominator } => {
                ratio_double(numerator,denominator)?
            }
            _ => self.long()? as f64,
        })
    }
    /// Divides numeric objects while preserving exact-number categories and NaN selection.
    /// After NaN selection, zero divisors and unrepresentable exact decimal results return `Error::Invalid` for the adapter to interpret.
    pub fn divide(left: &Self,right: &Self) -> Result<Self> {
        for value in [left,right] { if matches!(value,Self::Float(value) if value.is_nan()) || matches!(value,Self::Double(value) if value.is_nan()) { return Ok(value.clone()); } }
        if right.zero() { return Err(invalid()); }
        if matches!(left,Self::Float(_) | Self::Double(_)) || matches!(right,Self::Float(_) | Self::Double(_)) { return Ok(Self::Double(left.double()? / right.double()?)); }
        if matches!(left,Self::Decimal { .. }) || matches!(right,Self::Decimal { .. }) {
            let (left,ls) = left.as_decimal()?;let (right,rs) = right.as_decimal()?;
            let scale = i64::from(ls) - i64::from(rs);
            return decimal_divide(left,right,scale);
        }
        let (ln,ld) = left.rational()?;let (rn,rd) = right.rational()?;
        let (numerator,denominator) = reduce(ln * rd,ld * rn)?;
        if denominator == BigInt::from(1) {
            if !matches!(left,Self::BigInt(_) | Self::Ratio { .. } | Self::Long(i64::MIN)) && !matches!(right,Self::BigInt(_) | Self::Ratio { .. } | Self::Long(i64::MIN)) {
                if let Some(value) = numerator.to_i64() { return Ok(Self::Long(value)); }
            }
            Ok(Self::BigInt(numerator))
        } else { Ok(Self::Ratio { numerator,denominator }) }
    }
    fn rational(&self) -> Result<(BigInt,BigInt)> {
        Ok(match self {
            Self::BigInt(value) => (value.clone(),BigInt::from(1)),
            Self::Ratio { numerator,denominator } => (numerator.clone(),denominator.clone()),
            Self::Decimal { .. } | Self::Float(_) | Self::Double(_) => return Err(invalid()),
            _ => (BigInt::from(self.long()?),BigInt::from(1)),
        })
    }
    fn as_decimal(&self) -> Result<(BigInt,i32)> {
        match self {
            Self::Decimal { coefficient,scale } => Ok((coefficient.clone(),*scale)),
            Self::Ratio { numerator,denominator } => match decimal_divide(numerator.clone(),denominator.clone(),0)? { Self::Decimal { coefficient,scale } => Ok((coefficient,scale)),_ => unreachable!() },
            _ => Ok((self.rational()?.0,0)),
        }
    }
    fn zero(&self) -> bool {
        match self {
            Self::Byte(value) => *value == 0,Self::Short(value) => *value == 0,Self::Int(value) => *value == 0,Self::Long(value) => *value == 0,
            Self::BigInt(value) | Self::Decimal { coefficient: value,.. } | Self::Ratio { numerator: value,.. } => value.is_zero(),
            Self::Float(value) => *value == 0.0,Self::Double(value) => *value == 0.0,
        }
    }
    /// Compares numeric payloads using Clojure collection category rules, without an object-identity fast path.
    /// Callers preserve identity-sensitive cases such as a reader's shared NaN object before calling this method.
    pub fn equivalent(&self,other: &Self) -> bool {
        match (self,other) {
            (Self::Float(_) | Self::Double(_),Self::Float(_) | Self::Double(_)) => self.double().ok().zip(other.double().ok()).is_some_and(|(left,right)|left == right),
            (Self::Decimal { coefficient: left,scale: ls },Self::Decimal { coefficient: right,scale: rs }) => bigdecimal::BigDecimal::new(left.clone(),i64::from(*ls)) == bigdecimal::BigDecimal::new(right.clone(),i64::from(*rs)),
            (Self::Ratio { numerator: ln,denominator: ld },Self::Ratio { numerator: rn,denominator: rd }) => ln == rn && ld == rd,
            _ => self.rational().ok().zip(other.rational().ok()).is_some_and(|((ln,ld),(rn,rd))|ld == BigInt::from(1) && rd == BigInt::from(1) && ln == rn),
        }
    }
    /// Compares JVM numeric wrappers, preserving category, Decimal scale, signed zero and canonical NaN equality.
    pub fn java_equal(&self,other: &Self) -> bool {
        match (self,other) {
            (Self::Float(left),Self::Float(right)) => left.to_bits() == right.to_bits() || left.is_nan() && right.is_nan(),
            (Self::Double(left),Self::Double(right)) => left.to_bits() == right.to_bits() || left.is_nan() && right.is_nan(),
            _ => self == other,
        }
    }
}

fn integer(text: &str) -> Result<BigInt> {
    let (negative,digits) = match text.as_bytes().first() { Some(b'-') => (true,&text[1..]),Some(b'+') => (false,&text[1..]),_ => (false,text) };
    if digits.is_empty() { return Err(invalid()); }
    let digits = digits.chars().map(|ch|crate::decimal_digit(ch).map(|digit|char::from(b'0' + digit)).ok_or_else(invalid)).collect::<Result<String>>()?;
    let value = BigInt::parse_bytes(digits.as_bytes(),10).ok_or_else(invalid)?;
    Ok(if negative { -value } else { value })
}
/// Rounds a big-endian two's-complement coefficient times ten to the negated scale to binary64.
/// Underflow preserves the sign and overflow produces the matching infinity.
pub fn decimal_bytes_to_f64(coefficient: &[u8], scale: i32) -> f64 {
    decimal_parts_to_f64(BigInt::from_signed_bytes_be(coefficient), scale)
}

fn decimal_parts_to_f64(coefficient: BigInt, scale: i32) -> f64 {
    if coefficient.is_zero() {
        return 0.0;
    }
    let sign = if coefficient.is_negative() { -1.0 } else { 1.0 };
    if scale < -308 {
        return sign * f64::INFINITY;
    }
    if scale <= 0 {
        return float_integer(&(coefficient * BigInt::from(10).pow(scale.unsigned_abs())));
    }
    if u64::from(scale.unsigned_abs()) * 3 > coefficient.bits().saturating_add(1075) {
        return sign * 0.0;
    }
    let ratio = num_rational::BigRational::new_raw(coefficient, BigInt::from(10).pow(scale.unsigned_abs()));
    ratio.to_f64().expect("a finite nonzero denominator defines a numeric quotient")
}

fn float_integer(value: &BigInt) -> f64 { value.to_f64().unwrap_or_else(||if value.is_negative() { f64::NEG_INFINITY } else { f64::INFINITY }) }
fn ratio_double(numerator: &BigInt,denominator: &BigInt) -> Result<f64> {
    let mut exponent = i64::try_from(numerator.abs().to_string().len()).map_err(|_|invalid())? - i64::try_from(denominator.to_string().len()).map_err(|_|invalid())?;
    let pow = |exponent: i64| u32::try_from(exponent).map(|exponent|BigInt::from(10).pow(exponent)).map_err(|_|invalid());
    let below = if exponent >= 0 { numerator.abs() < denominator * pow(exponent)? } else { numerator.abs() * pow(-exponent)? < *denominator };
    if below { exponent -= 1; }
    let scale = 15 - exponent;
    let (numerator_abs,denominator) = if scale >= 0 { (numerator.abs() * pow(scale)?,denominator.clone()) } else { (numerator.abs(),denominator * pow(-scale)?) };
    let mut coefficient = &numerator_abs / &denominator;let remainder = numerator_abs % &denominator;
    let doubled = remainder * 2u8;
    if doubled > denominator || doubled == denominator && !(&coefficient % 2u8).is_zero() { coefficient += 1u8; }
    if numerator.is_negative() { coefficient = -coefficient; }
    format!("{coefficient}e{}",-scale).parse().map_err(|_|invalid())
}
fn reduce(mut numerator: BigInt,mut denominator: BigInt) -> Result<(BigInt,BigInt)> {
    if denominator.is_zero() { return Err(invalid()); }
    let (mut left,mut right) = (numerator.abs(),denominator.abs());
    while !right.is_zero() { let remainder = &left % &right;left = right;right = remainder; }
    numerator /= &left;denominator /= left;
    if denominator.is_negative() { numerator = -numerator;denominator = -denominator; }
    Ok((numerator,denominator))
}
fn decimal_divide(numerator: BigInt,denominator: BigInt,preferred: i64) -> Result<Number> {
    let (mut coefficient,mut denominator) = reduce(numerator,denominator)?;
    if coefficient.is_zero() { return Ok(Number::Decimal { coefficient,scale: preferred.clamp(i64::from(i32::MIN),i64::from(i32::MAX)) as i32 }); }
    let mut counts = [0u32;2];
    for (index,factor) in [2u8,5].into_iter().enumerate() { while (&denominator % factor).is_zero() { denominator /= factor;counts[index] = counts[index].checked_add(1).ok_or_else(invalid)?; } }
    if denominator != BigInt::from(1) { return Err(invalid()); }
    let digits = counts[0].max(counts[1]);
    coefficient *= BigInt::from(2).pow(digits - counts[0]);coefficient *= BigInt::from(5).pow(digits - counts[1]);
    let mut scale = preferred.checked_add(i64::from(digits)).ok_or_else(invalid)?;
    while scale > preferred && (&coefficient % 10u8).is_zero() { coefficient /= 10u8;scale -= 1; }
    Ok(Number::Decimal { coefficient,scale: i32::try_from(scale).map_err(|_|invalid())? })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn numeric_text_and_readable_printing_retain_jvm_categories() {
        for (value,text,printed) in [
            (Number::integer("9223372036854775808").unwrap(),"9223372036854775808","9223372036854775808N"),
            (Number::decimal("1.00").unwrap(),"1.00","1.00M"),
            (Number::decimal("1E+12").unwrap(),"1E+12","1E+12M"),
            (Number::divide(&Number::Long(1),&Number::Long(2)).unwrap(),"1/2","1/2"),
            (Number::Float(1.0),"1.0","1.0"),(Number::Double(-0.0),"-0.0","-0.0"),
            (Number::Double(f64::NAN),"NaN","##NaN"),(Number::Float(f32::NEG_INFINITY),"-Infinity","##-Inf"),
        ] {
            assert_eq!(value.text(),text);assert_eq!(value.printed(),printed);
            assert!(value.text_capacity().unwrap() >= printed.len());
        }
    }
    #[test]
    fn checked_rt_casts_keep_primitive_and_boxed_float_boundaries_distinct() {
        for (kind,value,expected) in [(Cast::Byte,127.9,Number::Byte(127)),(Cast::Short,32767.9,Number::Short(32767)),(Cast::Int,2147483647.9,Number::Int(i32::MAX))] {
            assert_eq!(Number::cast(kind,&Number::Double(value)).unwrap(),expected);
            assert!(Number::cast_primitive(kind,&Number::Double(value)).is_err());
        }
        assert_eq!(Number::cast(Cast::Byte,&Number::Double(f64::NAN)).unwrap(),Number::Byte(0));
        assert!(Number::cast_primitive(Cast::Byte,&Number::Double(f64::NAN)).is_err());
        assert_eq!(Number::cast_primitive(Cast::Int,&Number::Double(f64::NAN)).unwrap(),Number::Int(0));
        assert_eq!(Number::cast_primitive(Cast::Int,&Number::Float(2147483648.0)).unwrap(),Number::Int(i32::MAX));
        assert!(Number::cast(Cast::Int,&Number::Float(2147483648.0)).is_err());
        assert_eq!(Number::cast(Cast::Long,&Number::Double(9223372036854775808.0)).unwrap(),Number::Long(i64::MAX));
        assert!(Number::cast(Cast::Long,&Number::integer("9223372036854775808").unwrap()).is_err());
        let source = Number::Long(4_611_686_293_305_294_849);
        let Number::Float(direct) = Number::cast_primitive(Cast::Float,&source).unwrap() else { panic!() };
        let Number::Float(boxed) = Number::cast(Cast::Float,&source).unwrap() else { panic!() };
        assert_eq!(direct.to_bits(),0x5e80_0001);assert_eq!(boxed.to_bits(),0x5e80_0000);
    }
}


#[test]
fn decimal_rounding_matches_binary64_parsing_at_extreme_scales() {
    let coefficients = [
        BigInt::from(0),
        BigInt::from(-1),
        BigInt::from(1),
        BigInt::from(5),
        BigInt::from(9_007_199_254_740_993_i64),
        (BigInt::from(1) << 255usize) - 1,
        -(BigInt::from(1) << 255usize),
    ];
    for coefficient in coefficients {
        for scale in [i32::MIN, -309, -308, -292, -1, 0, 1, 17, 308, 324, 325, 400, i32::MAX] {
            let expected = format!("{coefficient}e{}", -i64::from(scale)).parse::<f64>().unwrap();
            assert_eq!(decimal_bytes_to_f64(&coefficient.to_signed_bytes_be(), scale).to_bits(), expected.to_bits(),
                "{coefficient} at scale {scale}");
        }
    }
}
