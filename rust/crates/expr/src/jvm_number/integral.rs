use num_bigint::BigInt;
use num_traits::{Signed,Zero};
use rtdb_types::Anomaly;
use super::{Binary,Cast,Dispatch,Error,Number,Result,arithmetic::{align_decimal,decimal_error},comparison::decimal_compare,invalid};

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) enum Integral { Quotient,Remainder }
impl Integral {
    pub(crate) fn primitive(self,dispatch: Dispatch) -> Option<Cast> { dispatch.result(false) }
}
impl Number {
    pub(crate) fn integral_bound(&self,other: &Self) -> Result<usize> {
        let base = self.binary_bound(Binary::Multiply(false),other)?;
        let mut bytes = base.checked_mul(4).ok_or_else(invalid)?;
        if !matches!(self,Self::Float(_) | Self::Double(_)) && !matches!(other,Self::Float(_) | Self::Double(_)) && (matches!(self,Self::Decimal { .. }) || matches!(other,Self::Decimal { .. })) {
            let (left_low,left_high) = self.decimal_scale_range()?;
            let (right_low,right_high) = other.decimal_scale_range()?;
            let low = left_low - right_high;let high = left_high - right_low;
            let coefficient_bits = match self {
                Self::Decimal { coefficient,.. } | Self::BigInt(coefficient) => coefficient.bits(),
                Self::Ratio { numerator,denominator } => denominator.bits().checked_mul(4).and_then(|bits|bits.checked_add(numerator.bits())).ok_or_else(invalid)?,
                _ => 64,
            };
            if !self.zero() && low <= i64::try_from(coefficient_bits).map_err(|_|invalid())? {
                let places = usize::try_from(low.unsigned_abs().max(high.unsigned_abs())).map_err(|_|invalid())?;
                bytes = bytes.checked_add(places.checked_mul(512).ok_or_else(invalid)?).ok_or_else(invalid)?;
            }
        }
        Ok(bytes)
    }
    /// Retain the integral_bound reservation through this call and the returned value's lifetime.
    pub(crate) fn integral(&self,operation: Integral,other: &Self,dispatch: Dispatch) -> Result<Self> {
        if dispatch == Dispatch::Long {
            let (left,right) = (self.long()?,other.long()?);
            if right == 0 { return Err(failure("/ by zero")); }
            return Ok(Self::Long(match operation { Integral::Quotient => left.wrapping_div(right),Integral::Remainder => left.wrapping_rem(right) }));
        }
        other.admit_divisor()?;
        if dispatch == Dispatch::Double || matches!(self,Self::Float(_) | Self::Double(_)) || matches!(other,Self::Float(_) | Self::Double(_)) {
            return floating(operation,self.double()?,other.double()?);
        }
        if matches!(self,Self::Decimal { .. }) || matches!(other,Self::Decimal { .. }) {
            let (left,ls) = self.as_decimal().map_err(decimal_error)?;
            let (right,rs) = other.as_decimal().map_err(decimal_error)?;
            let quotient = decimal_quotient(&left,ls,&right,rs)?;
            if operation == Integral::Quotient { return Ok(quotient); }
            let product = quotient.binary(Binary::Multiply(false),&Self::Decimal { coefficient: right,scale: rs },Dispatch::Object)?;
            return Self::Decimal { coefficient: left,scale: ls }.binary(Binary::Subtract(false),&product,Dispatch::Object);
        }
        if matches!(self,Self::Ratio { .. }) || matches!(other,Self::Ratio { .. }) {
            let (ln,ld) = self.rational()?;let (rn,rd) = other.rational()?;
            let quotient = Self::BigInt((ln * rd) / (ld * rn));
            if operation == Integral::Quotient { return Ok(quotient); }
            let product = quotient.binary(Binary::Multiply(false),other,Dispatch::Object)?;
            return self.binary(Binary::Subtract(false),&product,Dispatch::Object);
        }
        if matches!(self,Self::BigInt(_)) || matches!(other,Self::BigInt(_)) {
            let left = self.rational()?.0;let right = other.rational()?.0;
            return Ok(Self::BigInt(match operation { Integral::Quotient => left / right,Integral::Remainder => left % right }));
        }
        let (left,right) = (self.long()?,other.long()?);
        Ok(Self::Long(match operation { Integral::Quotient => left.wrapping_div(right),Integral::Remainder => left.wrapping_rem(right) }))
    }
}

fn floating(operation: Integral,left: f64,right: f64) -> Result<Number> {
    if right == 0.0 { return Err(failure("Divide by zero")); }
    let quotient = left / right;
    let truncated = if quotient <= i64::MAX as f64 && quotient >= i64::MIN as f64 { (quotient as i64) as f64 }
        else if quotient.is_finite() { quotient }
        else { return Err(failure("Infinite or NaN")); };
    Ok(Number::Double(match operation { Integral::Quotient => truncated,Integral::Remainder => left - truncated * right }))
}

fn decimal_quotient(left: &BigInt,ls: i32,right: &BigInt,rs: i32) -> Result<Number> {
    let preferred = (i64::from(ls) - i64::from(rs)).clamp(i64::from(i32::MIN),i64::from(i32::MAX)) as i32;
    if decimal_compare(left.abs(),ls,right.abs(),rs)?.is_lt() {
        return Ok(Number::Decimal { coefficient: BigInt::from(0),scale: preferred });
    }
    let shift = i64::from(rs) - i64::from(ls);
    let mut coefficient = if shift >= 0 { align_decimal(left.clone(),shift)? / right }
        else { left / align_decimal(right.clone(),-shift)? };
    let mut scale = 0;
    if preferred > 0 { coefficient = align_decimal(coefficient,i64::from(preferred))?;scale = preferred; }
    else { while scale > preferred && (&coefficient % 10u8).is_zero() { coefficient /= 10u8;scale -= 1; } }
    Ok(Number::Decimal { coefficient,scale })
}
fn failure(message: &str) -> Error { Error::External(Anomaly::fault("xtdb.error/unknown",message)) }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn integral_division_preserves_categories_and_wraps_only_fixed_integers() {
        let min = Number::Long(i64::MIN);let minus_one = Number::Long(-1);
        for dispatch in [Dispatch::Long,Dispatch::Object] {
            assert_eq!(min.integral(Integral::Quotient,&minus_one,dispatch).unwrap(),min);
            assert_eq!(min.integral(Integral::Remainder,&minus_one,dispatch).unwrap(),Number::Long(0));
        }
        assert_eq!(Number::integer("-9223372036854775808").unwrap().integral(Integral::Quotient,&minus_one,Dispatch::Object).unwrap(),Number::integer("9223372036854775808").unwrap());
        let ratio = Number::divide(&Number::Long(7),&Number::Long(2)).unwrap();
        assert_eq!(ratio.integral(Integral::Quotient,&Number::Long(1),Dispatch::Object).unwrap(),Number::integer("3").unwrap());
        assert_eq!(ratio.integral(Integral::Remainder,&Number::Long(1),Dispatch::Object).unwrap(),Number::divide(&Number::Long(1),&Number::Long(2)).unwrap());
        assert_eq!(Number::Long(7).integral(Integral::Remainder,&ratio,Dispatch::Object).unwrap(),Number::integer("0").unwrap());
        assert_eq!(Number::Byte(7).integral(Integral::Quotient,&Number::Short(3),Dispatch::Object).unwrap(),Number::Long(2));
        assert_eq!(Number::Float(7.5).integral(Integral::Quotient,&Number::Short(2),Dispatch::Object).unwrap(),Number::Double(3.0));
        for (left,right,quotient,remainder) in [(7,3,2,1),(-7,3,-2,-1),(7,-3,-2,1),(-7,-3,2,-1)] {
            assert_eq!(Number::Long(left).integral(Integral::Quotient,&Number::Long(right),Dispatch::Long).unwrap(),Number::Long(quotient));
            assert_eq!(Number::Long(left).integral(Integral::Remainder,&Number::Long(right),Dispatch::Long).unwrap(),Number::Long(remainder));
        }
    }
    #[test]
    fn floating_remainder_follows_truncated_division_and_preserves_zero_sign() {
        let result = |operation,left,right|match Number::Double(left).integral(operation,&Number::Double(right),Dispatch::Double).unwrap() { Number::Double(value) => value,_ => panic!() };
        assert_eq!(result(Integral::Quotient,-0.0,1.0).to_bits(),0.0f64.to_bits());
        assert_eq!(result(Integral::Remainder,-0.0,1.0).to_bits(),(-0.0f64).to_bits());
        assert_eq!(result(Integral::Remainder,-0.0,-1.0).to_bits(),0.0f64.to_bits());
        assert_eq!(result(Integral::Remainder,1.0,0.1),0.0);
        assert!(result(Integral::Remainder,1.0,f64::INFINITY).is_nan());
        assert_eq!(result(Integral::Quotient,1.0,f64::INFINITY),0.0);
        assert_eq!(result(Integral::Quotient,1e100,1.0),1e100);
        assert_eq!(result(Integral::Remainder,1e100,1.0),0.0);
        for value in [f64::NAN,f64::INFINITY,f64::NEG_INFINITY] {
            for operation in [Integral::Quotient,Integral::Remainder] { assert!(Number::Double(value).integral(operation,&Number::Double(1.0),Dispatch::Double).is_err()); }
        }
    }
    #[test]
    fn decimal_quotients_and_remainders_keep_preferred_scales() {
        for (left,right,quotient,remainder) in [("2.00","0.5","4.0","0.00"),("1.00","3","0.00","1.00"),("5.00","2","2.00","1.00"),("1E+3","3","333","1"),("1E+3","4","2.5E+2","0E+1"),("1E+3","2","5E+2","0E+2"),("-7.0","3.00","-2","-1.00")] {
            let left = Number::decimal(left).unwrap();let right = Number::decimal(right).unwrap();
            assert_eq!(left.integral(Integral::Quotient,&right,Dispatch::Object).unwrap(),Number::decimal(quotient).unwrap());
            assert_eq!(left.integral(Integral::Remainder,&right,Dispatch::Object).unwrap(),Number::decimal(remainder).unwrap());
        }
        let tiny = Number::Decimal { coefficient: BigInt::from(1),scale: i32::MAX };
        let huge = Number::Decimal { coefficient: BigInt::from(1),scale: i32::MIN };
        assert!(tiny.integral_bound(&huge).unwrap() < 64 * 1024);
        assert_eq!(tiny.integral(Integral::Quotient,&huge,Dispatch::Object).unwrap(),Number::Decimal { coefficient: BigInt::from(0),scale: i32::MAX });
        assert_eq!(tiny.integral(Integral::Remainder,&huge,Dispatch::Object).unwrap(),tiny);
        assert!(huge.integral_bound(&Number::Long(3)).unwrap() > 16 * 1024 * 1024);
    }
}
