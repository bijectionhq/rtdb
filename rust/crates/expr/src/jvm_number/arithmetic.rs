use super::{Cast,Error,Number,Result,invalid,reduce};
use num_bigint::BigInt;
use num_traits::Zero;
use rtdb_types::Anomaly;

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) enum Binary { Add(bool),Subtract(bool),Multiply(bool),Divide }
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) enum Unary { Negate(bool),Increment(bool),Decrement(bool) }
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) enum Dispatch { Object,Long,Double }
impl Dispatch {
    pub(crate) fn unary(value: Option<Cast>) -> Self {
        match value { Some(Cast::Float | Cast::Double) => Self::Double,Some(_) => Self::Long,None => Self::Object }
    }
    pub(crate) fn binary(left: Option<Cast>,right: Option<Cast>) -> Self {
        match (Self::unary(left),Self::unary(right)) {
            (Self::Double,_) | (_,Self::Double) => Self::Double,
            (Self::Long,Self::Long) => Self::Long,
            _ => Self::Object,
        }
    }
    pub(crate) fn result(self,promoting: bool) -> Option<Cast> {
        match self { Self::Double => Some(Cast::Double),Self::Long if !promoting => Some(Cast::Long),_ => None }
    }
}
impl Binary {
    pub(crate) fn primitive(self,dispatch: Dispatch) -> Option<Cast> {
        dispatch.result(matches!(self,Self::Add(true) | Self::Subtract(true) | Self::Multiply(true) | Self::Divide))
    }
}
impl Unary {
    pub(crate) fn primitive(self,dispatch: Dispatch) -> Option<Cast> {
        dispatch.result(matches!(self,Self::Negate(true) | Self::Increment(true) | Self::Decrement(true)))
    }
}

impl Number {
    pub(crate) fn admit_divisor(&self) -> Result<()> { if self.zero() { Err(failure("Divide by zero")) } else { Ok(()) } }
    pub(crate) fn unary_bound(&self,operation: Unary) -> Result<usize> {
        match operation { Unary::Negate(_) => scratch(self.storage_bits()?.checked_add(64).ok_or_else(invalid)?),Unary::Increment(_) | Unary::Decrement(_) => self.binary_bound(Binary::Add(false),&Self::Long(1)) }
    }
    pub(crate) fn binary_bound(&self,operation: Binary,other: &Self) -> Result<usize> {
        let mut bits = self.storage_bits()?.checked_add(other.storage_bits()?).and_then(|bits|bits.checked_add(128)).ok_or_else(invalid)?;
        if !self.floating() && !other.floating() && (self.decimal_kind() || other.decimal_kind()) {
            for value in [self,other] {
                if let Self::Ratio { denominator,.. } = value { bits = bits.checked_add(denominator.bits().checked_mul(4).ok_or_else(invalid)?).ok_or_else(invalid)?; }
            }
            if matches!(operation,Binary::Add(_) | Binary::Subtract(_)) {
                let (left_low,left_high) = self.decimal_scale_range()?;
                let (right_low,right_high) = other.decimal_scale_range()?;
                let left = if self.zero() { 0 } else { (right_high - left_low).max(0) };
                let right = if other.zero() { 0 } else { (left_high - right_low).max(0) };
                bits = bits.checked_add(u64::try_from(left.max(right)).map_err(|_|invalid())?.checked_mul(4).ok_or_else(invalid)?).ok_or_else(invalid)?;
            }
        }
        scratch(bits)
    }
    /// Retain the unary_bound reservation through this call and the returned value's lifetime.
    pub(crate) fn unary(&self,operation: Unary,dispatch: Dispatch) -> Result<Self> {
        if dispatch == Dispatch::Double || self.floating() {
            let value = self.double()?;
            return Ok(Self::Double(match operation { Unary::Negate(_) => -value,Unary::Increment(_) => value + 1.0,Unary::Decrement(_) => value - 1.0 }));
        }
        match operation {
            Unary::Negate(promote) => Ok(match self {
                Self::BigInt(value) => Self::BigInt(-value),
                Self::Ratio { numerator,denominator } => Self::Ratio { numerator: -numerator,denominator: denominator.clone() },
                Self::Decimal { coefficient,scale } => Self::Decimal { coefficient: -coefficient,scale: *scale },
                _ => { let value = self.long()?;integral_result(value.checked_neg(),promote,||-BigInt::from(value))? },
            }),
            Unary::Increment(promote) => self.binary(Binary::Add(promote),&Self::Long(1),dispatch),
            Unary::Decrement(promote) => self.binary(Binary::Add(promote),&Self::Long(-1),dispatch),
        }
    }
    /// Retain the binary_bound reservation through this call and the returned value's lifetime.
    pub(crate) fn binary(&self,operation: Binary,other: &Self,dispatch: Dispatch) -> Result<Self> {
        if dispatch == Dispatch::Double {
            let (left,right) = (self.double()?,other.double()?);
            return Ok(Self::Double(match operation { Binary::Add(_) => left + right,Binary::Subtract(_) => left - right,Binary::Multiply(_) => left * right,Binary::Divide => left / right }));
        }
        if let Binary::Subtract(promote) = operation {
            if dispatch == Dispatch::Long {
                let (left,right) = (self.long()?,other.long()?);
                return integral_result(left.checked_sub(right),promote,||BigInt::from(left) - BigInt::from(right));
            }
            let negative = other.unary(Unary::Negate(promote),Dispatch::Object)?;
            return self.binary(Binary::Add(promote),&negative,Dispatch::Object);
        }
        if operation == Binary::Divide {
            if self.is_nan() { return Ok(self.clone()); }
            if other.is_nan() { return Ok(other.clone()); }
            other.admit_divisor()?;
            return Self::divide(self,other).map_err(|error|if self.decimal_kind() || other.decimal_kind() { decimal_error(error) } else { error });
        }
        if self.floating() || other.floating() {
            let (left,right) = (self.double()?,other.double()?);
            return Ok(Self::Double(match operation { Binary::Add(_) => left + right,Binary::Multiply(_) => left * right,_ => unreachable!() }));
        }
        if self.decimal_kind() || other.decimal_kind() {
            let (left,ls) = self.as_decimal().map_err(decimal_error)?;
            let (right,rs) = other.as_decimal().map_err(decimal_error)?;
            return match operation {
                Binary::Add(_) => {
                    let scale = ls.max(rs);
                    let left = align_decimal(left,i64::from(scale) - i64::from(ls))?;
                    let right = align_decimal(right,i64::from(scale) - i64::from(rs))?;
                    Ok(Self::Decimal { coefficient: left + right,scale })
                }
                Binary::Multiply(_) => {
                    let scale = i64::from(ls) + i64::from(rs);
                    let scale = if left.is_zero() { scale.clamp(i64::from(i32::MIN),i64::from(i32::MAX)) as i32 } else { i32::try_from(scale).map_err(|_|failure(if scale > 0 { "Underflow" } else { "Overflow" }))? };
                    Ok(Self::Decimal { coefficient: left * right,scale })
                }
                _ => unreachable!(),
            };
        }
        if matches!(self,Self::Ratio { .. }) || matches!(other,Self::Ratio { .. }) {
            let (ln,ld) = self.rational()?;let (rn,rd) = other.rational()?;
            let (numerator,denominator) = match operation { Binary::Add(_) => (rn * &ld + ln * &rd,rd * ld),Binary::Multiply(_) => (rn * ln,rd * ld),_ => unreachable!() };
            let (numerator,denominator) = reduce(numerator,denominator)?;
            return Ok(if denominator == BigInt::from(1) { Self::BigInt(numerator) } else { Self::Ratio { numerator,denominator } });
        }
        if matches!(self,Self::BigInt(_)) || matches!(other,Self::BigInt(_)) {
            let left = self.rational()?.0;let right = other.rational()?.0;
            return Ok(Self::BigInt(match operation { Binary::Add(_) => left + right,Binary::Multiply(_) => left * right,_ => unreachable!() }));
        }
        let (left,right) = (self.long()?,other.long()?);
        match operation {
            Binary::Add(promote) => integral_result(left.checked_add(right),promote,||BigInt::from(left) + BigInt::from(right)),
            Binary::Multiply(promote) => integral_result(left.checked_mul(right),promote,||BigInt::from(left) * BigInt::from(right)),
            _ => unreachable!(),
        }
    }
    fn floating(&self) -> bool { matches!(self,Self::Float(_) | Self::Double(_)) }
    fn decimal_kind(&self) -> bool { matches!(self,Self::Decimal { .. }) }
    fn storage_bits(&self) -> Result<u64> {
        match self { Self::BigInt(value) | Self::Decimal { coefficient: value,.. } => Ok(value.bits()),Self::Ratio { numerator,denominator } => numerator.bits().checked_add(denominator.bits()).ok_or_else(invalid),_ => Ok(64) }
    }
    fn decimal_scale_range(&self) -> Result<(i64,i64)> {
        Ok(match self { Self::Decimal { scale,.. } => (i64::from(*scale),i64::from(*scale)),Self::Ratio { denominator,.. } => (0,i64::try_from(denominator.bits()).map_err(|_|invalid())?),_ => (0,0) })
    }
}
fn scratch(bits: u64) -> Result<usize> { usize::try_from(bits).ok().and_then(|bits|bits.checked_mul(32)).and_then(|bytes|bytes.checked_add(1024)).ok_or_else(invalid) }
fn integral_result(value: Option<i64>,promote: bool,large: impl FnOnce()->BigInt) -> Result<Number> {
    match value { Some(value) => Ok(Number::Long(value)),None if promote => Ok(Number::BigInt(large())),None => Err(failure("long overflow")) }
}
fn align_decimal(value: BigInt,places: i64) -> Result<BigInt> {
    if value.is_zero() || places == 0 { Ok(value) } else { Ok(value * BigInt::from(10).pow(u32::try_from(places).map_err(|_|failure("BigDecimal scale overflow"))?)) }
}
fn failure(message: &str) -> Error { Error::External(Anomaly::fault("xtdb.error/unknown",message)) }
fn decimal_error(error: Error) -> Error {
    match error { Error::Invalid => failure("Non-terminating decimal expansion; no exact representable decimal result."),other => other }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checked_and_promoting_operations_keep_numeric_categories() {
        let max = Number::Long(i64::MAX);let min = Number::Long(i64::MIN);
        assert!(max.binary(Binary::Add(false),&Number::Long(1),Dispatch::Long).is_err());
        assert_eq!(max.binary(Binary::Add(true),&Number::Long(1),Dispatch::Long).unwrap(),Number::integer("9223372036854775808").unwrap());
        assert_eq!(min.binary(Binary::Subtract(false),&min,Dispatch::Long).unwrap(),Number::Long(0));
        assert!(min.binary(Binary::Subtract(false),&min,Dispatch::Object).is_err());
        assert_eq!(min.binary(Binary::Subtract(true),&min,Dispatch::Long).unwrap(),Number::Long(0));
        assert_eq!(min.binary(Binary::Subtract(true),&min,Dispatch::Object).unwrap(),Number::integer("0").unwrap());
        assert_eq!(min.binary(Binary::Multiply(true),&Number::Long(-1),Dispatch::Object).unwrap(),Number::integer("9223372036854775808").unwrap());
        assert_eq!(max.unary(Unary::Increment(true),Dispatch::Long).unwrap(),Number::integer("9223372036854775808").unwrap());
        assert_eq!(min.unary(Unary::Decrement(true),Dispatch::Long).unwrap(),Number::integer("-9223372036854775809").unwrap());
        assert_eq!(Number::integer("1").unwrap().binary(Binary::Add(false),&Number::Long(1),Dispatch::Object).unwrap(),Number::integer("2").unwrap());
        let half = Number::divide(&Number::Long(1),&Number::Long(2)).unwrap();
        assert_eq!(half.binary(Binary::Add(false),&half,Dispatch::Object).unwrap(),Number::integer("1").unwrap());
        assert_eq!(half.binary(Binary::Multiply(false),&half,Dispatch::Object).unwrap(),Number::divide(&Number::Long(1),&Number::Long(4)).unwrap());
        assert_eq!(half.unary(Unary::Negate(false),Dispatch::Object).unwrap().text(),"-1/2");
    }
    #[test]
    fn primitive_floating_division_and_boxed_nan_dispatch_remain_distinct() {
        let one = Number::Double(1.0);let zero = Number::Long(0);
        assert_eq!(one.binary(Binary::Divide,&zero,Dispatch::Double).unwrap(),Number::Double(f64::INFINITY));
        assert!(one.binary(Binary::Divide,&zero,Dispatch::Object).is_err());
        assert!(matches!(Number::Float(f32::NAN).binary(Binary::Divide,&zero,Dispatch::Object).unwrap(),Number::Float(value) if value.is_nan()));
        assert!(matches!(Number::Float(1.0).unary(Unary::Negate(false),Dispatch::Object).unwrap(),Number::Double(-1.0)));
        assert_eq!(Binary::Add(true).primitive(Dispatch::Long),None);
        assert_eq!(Binary::Divide.primitive(Dispatch::Long),None);
        assert_eq!(Binary::Add(false).primitive(Dispatch::Long),Some(Cast::Long));
        assert_eq!(Binary::Add(true).primitive(Dispatch::Double),Some(Cast::Double));
        assert_eq!(Dispatch::binary(Some(Cast::Float),None),Dispatch::Double);
    }
    #[test]
    fn decimal_operations_preserve_preferred_scale_and_left_operand_overflow() {
        let a = Number::decimal("1.20").unwrap();let b = Number::decimal("2.3").unwrap();
        assert_eq!(a.binary(Binary::Add(false),&b,Dispatch::Object).unwrap(),Number::decimal("3.50").unwrap());
        assert_eq!(a.binary(Binary::Subtract(false),&b,Dispatch::Object).unwrap(),Number::decimal("-1.10").unwrap());
        assert_eq!(a.binary(Binary::Multiply(false),&b,Dispatch::Object).unwrap(),Number::decimal("2.760").unwrap());
        assert_eq!(Number::decimal("2.00").unwrap().binary(Binary::Divide,&Number::Long(2),Dispatch::Object).unwrap(),Number::decimal("1.00").unwrap());
        let half = Number::divide(&Number::Long(1),&Number::Long(2)).unwrap();
        assert_eq!(a.binary(Binary::Add(false),&half,Dispatch::Object).unwrap(),Number::decimal("1.70").unwrap());
        let third = Number::divide(&Number::Long(1),&Number::Long(3)).unwrap();
        assert!(a.binary(Binary::Add(false),&third,Dispatch::Object).is_err());
        let zero = Number::Decimal { coefficient: BigInt::from(0),scale: i32::MAX };
        let one = Number::Decimal { coefficient: BigInt::from(1),scale: 1 };
        assert_eq!(zero.binary(Binary::Multiply(false),&one,Dispatch::Object).unwrap(),zero);
        assert!(one.binary(Binary::Multiply(false),&zero,Dispatch::Object).is_err());
    }
    #[test]
    fn arithmetic_bounds_include_decimal_alignment_before_any_power_allocation() {
        let tiny = Number::Decimal { coefficient: BigInt::from(1),scale: i32::MAX };
        assert!(tiny.binary_bound(Binary::Add(false),&Number::Long(1)).unwrap() > 16 * 1024 * 1024);
        assert!(tiny.unary_bound(Unary::Increment(false)).unwrap() > 16 * 1024 * 1024);
        let zeros = [Number::Decimal { coefficient: BigInt::from(0),scale: i32::MIN },Number::Decimal { coefficient: BigInt::from(0),scale: i32::MAX }];
        assert!(zeros[0].binary_bound(Binary::Add(false),&zeros[1]).unwrap() < 16 * 1024);
        assert_eq!(zeros[0].binary(Binary::Add(false),&zeros[1],Dispatch::Object).unwrap(),zeros[1]);
    }
}
