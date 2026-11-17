use std::borrow::Cow;
use num_traits::Signed;
use super::{Cast,Comparison,Dispatch,Number,Result,Unary};

#[derive(Clone,Copy,Debug)]
pub(crate) enum Extremum { Min,Max }
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) enum Selected { Left,Right }
#[derive(Clone,Copy,Debug)]
pub(crate) struct ExtremumCall { pub(crate) left: Dispatch,pub(crate) right: Dispatch }
impl ExtremumCall {
    pub(crate) const BOXED: Self = Self { left: Dispatch::Object,right: Dispatch::Object };
    pub(crate) fn primitive(self) -> Option<Cast> {
        match (self.left,self.right) { (Dispatch::Long,Dispatch::Long) => Some(Cast::Long),(Dispatch::Double,Dispatch::Double) => Some(Cast::Double),_ => None }
    }
}
impl Selected {
    pub(crate) fn choose<T>(self,left: T,right: T) -> T { match self { Self::Left => left,Self::Right => right } }
}
impl Number {
    /// Retain the comparison_bound reservation through this call.
    pub(crate) fn select_extremum(&self,operation: Extremum,other: &Self,call: ExtremumCall) -> Result<Selected> {
        if self.is_nan() { return Ok(Selected::Left); }
        if other.is_nan() { return Ok(Selected::Right); }
        let left = match call.primitive() {
            Some(Cast::Double) => {
                let left = self.double()?;let right = other.double()?;
                if left == 0.0 && right == 0.0 {
                    match operation { Extremum::Min => !right.is_sign_negative(),Extremum::Max => !left.is_sign_negative() }
                } else { match operation { Extremum::Min => left <= right,Extremum::Max => left >= right } }
            }
            Some(Cast::Long) => {
                let left = self.long()?;let right = other.long()?;
                match operation { Extremum::Min => left <= right,Extremum::Max => left >= right }
            }
            _ => {
                let dispatch = if call.left == Dispatch::Double || call.right == Dispatch::Double { Dispatch::Double } else { Dispatch::Object };
                self.compare_numeric(match operation { Extremum::Min => Comparison::Less,Extremum::Max => Comparison::Greater },other,dispatch)?
            }
        };
        Ok(if left { Selected::Left } else { Selected::Right })
    }
    pub(crate) fn absolute_bound(&self,dispatch: Dispatch) -> Result<usize> {
        if dispatch == Dispatch::Object && matches!(self,Self::Decimal { coefficient,.. } if !coefficient.is_negative()) { return Ok(0); }
        self.unary_bound(Unary::Negate(false))
    }
    /// Retain the absolute_bound reservation through the returned owned value's lifetime.
    pub(crate) fn absolute(&self,dispatch: Dispatch) -> Result<Cow<'_,Self>> {
        if dispatch == Dispatch::Double || matches!(self,Self::Float(_) | Self::Double(_)) { return Ok(Cow::Owned(Self::Double(self.double()?.abs()))); }
        if dispatch == Dispatch::Long { return Ok(Cow::Owned(Self::Long(self.long()?.wrapping_abs()))); }
        Ok(match self {
            Self::Byte(value) => Cow::Owned(Self::Long(i64::from(*value).abs())),Self::Short(value) => Cow::Owned(Self::Long(i64::from(*value).abs())),Self::Int(value) => Cow::Owned(Self::Long(i64::from(*value).abs())),Self::Long(value) => Cow::Owned(Self::Long(value.wrapping_abs())),
            Self::BigInt(value) => Cow::Owned(Self::BigInt(value.abs())),
            Self::Ratio { numerator,denominator } => Cow::Owned(Self::Ratio { numerator: numerator.abs(),denominator: denominator.clone() }),
            Self::Decimal { coefficient,scale } if coefficient.is_negative() => Cow::Owned(Self::Decimal { coefficient: coefficient.abs(),scale: *scale }),
            Self::Decimal { .. } => Cow::Borrowed(self),
            Self::Float(_) | Self::Double(_) => unreachable!(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::BigInt;

    #[test]
    fn primitive_extrema_keep_jdk_zero_and_nan_selection() {
        let call = ExtremumCall { left: Dispatch::Double,right: Dispatch::Double };
        for (left,right) in [(0.0,-0.0),(-0.0,0.0),(-0.0,-0.0),(0.0,0.0)] {
            for operation in [Extremum::Min,Extremum::Max] {
                let selected = Number::Double(left).select_extremum(operation,&Number::Double(right),call).unwrap();
                let result = selected.choose(left,right);
                let negative = match operation { Extremum::Min => left.is_sign_negative() || right.is_sign_negative(),Extremum::Max => left.is_sign_negative() && right.is_sign_negative() };
                assert_eq!(result.is_sign_negative(),negative);
            }
        }
        for operation in [Extremum::Min,Extremum::Max] {
            assert_eq!(Number::Double(f64::NAN).select_extremum(operation,&Number::Double(f64::NAN),call).unwrap(),Selected::Left);
            assert_eq!(Number::Double(0.0).select_extremum(operation,&Number::Double(f64::NAN),call).unwrap(),Selected::Right);
            assert_eq!(Number::Long(1).select_extremum(operation,&Number::Long(1),ExtremumCall { left: Dispatch::Long,right: Dispatch::Long }).unwrap(),Selected::Left);
            assert_eq!(Number::Long(1).select_extremum(operation,&Number::Long(1),ExtremumCall::BOXED).unwrap(),Selected::Right);
        }
    }
    #[test]
    fn mixed_extrema_compare_with_the_selected_parameter_types() {
        let integer = Number::Long(9007199254740993);let double = Number::Double(9007199254740992.0);
        assert_eq!(double.select_extremum(Extremum::Max,&integer,ExtremumCall { left: Dispatch::Double,right: Dispatch::Long }).unwrap(),Selected::Right);
        let one = Number::decimal("1.0").unwrap();let scaled = Number::decimal("1.00").unwrap();
        assert_eq!(one.select_extremum(Extremum::Min,&scaled,ExtremumCall::BOXED).unwrap(),Selected::Right);
        let third = Number::divide(&Number::Long(1),&Number::Long(3)).unwrap();
        assert!(one.select_extremum(Extremum::Min,&third,ExtremumCall::BOXED).is_err());
    }
    #[test]
    fn absolute_value_preserves_numeric_category_scale_and_decimal_borrowing() {
        assert_eq!(*Number::Long(i64::MIN).absolute(Dispatch::Long).unwrap(),Number::Long(i64::MIN));
        assert_eq!(*Number::Byte(i8::MIN).absolute(Dispatch::Object).unwrap(),Number::Long(128));
        assert_eq!(*Number::Short(i16::MIN).absolute(Dispatch::Object).unwrap(),Number::Long(32768));
        assert_eq!(*Number::Int(i32::MIN).absolute(Dispatch::Object).unwrap(),Number::Long(2147483648));
        assert_eq!(*Number::integer("-9223372036854775808").unwrap().absolute(Dispatch::Object).unwrap(),Number::integer("9223372036854775808").unwrap());
        let ratio = Number::Ratio { numerator: BigInt::from(-6),denominator: BigInt::from(3) };
        assert_eq!(*ratio.absolute(Dispatch::Object).unwrap(),Number::Ratio { numerator: BigInt::from(6),denominator: BigInt::from(3) });
        let positive = Number::decimal("1.00").unwrap();let negative = Number::decimal("-1.00").unwrap();
        assert!(matches!(positive.absolute(Dispatch::Object).unwrap(),Cow::Borrowed(value) if std::ptr::eq(value,&positive)));
        assert_eq!(positive.absolute_bound(Dispatch::Object).unwrap(),0);
        let zero = Number::decimal("0.000").unwrap();
        assert!(matches!(zero.absolute(Dispatch::Object).unwrap(),Cow::Borrowed(value) if std::ptr::eq(value,&zero)));
        assert_eq!(*negative.absolute(Dispatch::Object).unwrap(),positive);
        assert!(negative.absolute_bound(Dispatch::Object).unwrap() > std::mem::size_of::<Number>());
        for value in [-0.0,f64::from_bits(0xfff8_0000_0000_0001)] {
            let result = Number::Double(value).absolute(Dispatch::Object).unwrap().into_owned();
            let Number::Double(result) = result else { panic!("double result expected"); };
            assert_eq!(result.to_bits(),value.to_bits() & 0x7fff_ffff_ffff_ffff);
        }
        assert_eq!(*Number::Float(-1.2).absolute(Dispatch::Object).unwrap(),Number::Double(f64::from(1.2f32)));
    }
}
