use std::cmp::Ordering;
use num_bigint::BigInt;
use num_traits::{Signed,Zero};
use super::{Binary,Dispatch,Number,Result,arithmetic::{align_decimal,decimal_error},invalid};

#[derive(Clone,Copy,Debug)]
pub(crate) enum Comparison { Less,LessEqual,Greater,GreaterEqual,Equivalent }
#[derive(Clone,Copy,Debug)]
pub(crate) enum Predicate { Zero,Positive,Negative }
impl Comparison {
    fn accepts(self,ordering: Option<Ordering>) -> bool {
        match (self,ordering) {
            (_,None) => false,
            (Self::Less,Some(value)) => value == Ordering::Less,
            (Self::LessEqual,Some(value)) => value != Ordering::Greater,
            (Self::Greater,Some(value)) => value == Ordering::Greater,
            (Self::GreaterEqual,Some(value)) => value != Ordering::Less,
            (Self::Equivalent,Some(value)) => value == Ordering::Equal,
        }
    }
}
impl Number {
    pub(crate) fn comparison_bound(&self,other: &Self) -> Result<usize> { self.binary_bound(Binary::Multiply(false),other) }
    /// Retain the comparison_bound reservation through this call.
    pub(crate) fn compare_numeric(&self,operation: Comparison,other: &Self,dispatch: Dispatch) -> Result<bool> {
        let ordering = if dispatch == Dispatch::Double || matches!(self,Self::Float(_) | Self::Double(_)) || matches!(other,Self::Float(_) | Self::Double(_)) {
            self.double()?.partial_cmp(&other.double()?)
        } else if matches!(self,Self::Decimal { .. }) || matches!(other,Self::Decimal { .. }) {
            let (left,ls) = self.as_decimal().map_err(decimal_error)?;
            let (right,rs) = other.as_decimal().map_err(decimal_error)?;
            Some(decimal_compare(left,ls,right,rs)?)
        } else {
            let (ln,ld) = self.rational()?;let (rn,rd) = other.rational()?;
            Some((ln * rd).cmp(&(rn * ld)))
        };
        Ok(operation.accepts(ordering))
    }
    pub(crate) fn predicate(&self,operation: Predicate) -> bool {
        let ordering = match self {
            Self::Byte(value) => Some(value.cmp(&0)),Self::Short(value) => Some(value.cmp(&0)),Self::Int(value) => Some(value.cmp(&0)),Self::Long(value) => Some(value.cmp(&0)),
            Self::Float(value) => value.partial_cmp(&0.0),Self::Double(value) => value.partial_cmp(&0.0),
            Self::BigInt(value) | Self::Decimal { coefficient: value,.. } | Self::Ratio { numerator: value,.. } => Some(value.sign().cmp(&num_bigint::Sign::NoSign)),
        };
        match operation { Predicate::Zero => ordering == Some(Ordering::Equal),Predicate::Positive => ordering == Some(Ordering::Greater),Predicate::Negative => ordering == Some(Ordering::Less) }
    }
}
pub(super) fn decimal_compare(left: BigInt,ls: i32,right: BigInt,rs: i32) -> Result<Ordering> {
    let signs = left.sign().cmp(&right.sign());
    if signs != Ordering::Equal { return Ok(signs); }
    if left.is_zero() { return Ok(Ordering::Equal); }
    if ls == rs { return Ok(left.cmp(&right)); }
    let digits = |value: &BigInt|i64::try_from(value.to_string().len() - usize::from(value.is_negative())).map_err(|_|invalid());
    let left_exponent = digits(&left)? - i64::from(ls);
    let right_exponent = digits(&right)? - i64::from(rs);
    let magnitude = left_exponent.cmp(&right_exponent);
    if magnitude != Ordering::Equal { return Ok(if left.is_negative() { magnitude.reverse() } else { magnitude }); }
    let scale = ls.max(rs);
    let left = align_decimal(left,i64::from(scale) - i64::from(ls))?;
    let right = align_decimal(right,i64::from(scale) - i64::from(rs))?;
    Ok(left.cmp(&right))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn numeric_equivalence_crosses_object_categories_and_exact_scales() {
        let one = [Number::Long(1),Number::Double(1.0),Number::decimal("1.00").unwrap(),Number::integer("1").unwrap()];
        for left in &one { for right in &one { assert!(left.compare_numeric(Comparison::Equivalent,right,Dispatch::Object).unwrap()); } }
        assert!(!one[0].equivalent(&one[1]));
        let half = Number::divide(&Number::Long(1),&Number::Long(2)).unwrap();
        assert!(half.compare_numeric(Comparison::Equivalent,&Number::decimal("0.5").unwrap(),Dispatch::Object).unwrap());
        let third = Number::divide(&Number::Long(1),&Number::Long(3)).unwrap();
        assert!(third.compare_numeric(Comparison::Equivalent,&Number::decimal("0.3").unwrap(),Dispatch::Object).is_err());
    }
    #[test]
    fn comparisons_keep_exact_integers_and_floating_unordered_results() {
        let low = Number::integer("9007199254740992").unwrap();let high = Number::integer("9007199254740993").unwrap();
        assert!(low.compare_numeric(Comparison::Less,&high,Dispatch::Object).unwrap());
        assert!(low.compare_numeric(Comparison::Equivalent,&Number::Double(9007199254740992.0),Dispatch::Object).unwrap());
        for operation in [Comparison::Less,Comparison::LessEqual,Comparison::Greater,Comparison::GreaterEqual,Comparison::Equivalent] {
            assert!(!Number::Double(f64::NAN).compare_numeric(operation,&Number::Long(0),Dispatch::Object).unwrap());
            assert!(!Number::Long(0).compare_numeric(operation,&Number::Float(f32::NAN),Dispatch::Double).unwrap());
        }
        assert!(Number::Double(-0.0).predicate(Predicate::Zero));
        for operation in [Predicate::Zero,Predicate::Positive,Predicate::Negative] { assert!(!Number::Double(f64::NAN).predicate(operation)); }
        assert!(Number::integer("-1").unwrap().predicate(Predicate::Negative));
        assert!(Number::decimal("1E-2147483647").unwrap().predicate(Predicate::Positive));
    }
    #[test]
    fn decimal_comparison_uses_adjusted_magnitude_before_scale_alignment() {
        let tiny = Number::Decimal { coefficient: BigInt::from(1),scale: i32::MAX };
        let huge = Number::Decimal { coefficient: BigInt::from(1),scale: i32::MIN };
        assert!(tiny.comparison_bound(&huge).unwrap() < 16 << 10);
        assert!(tiny.compare_numeric(Comparison::Less,&huge,Dispatch::Object).unwrap());
        assert!(tiny.compare_numeric(Comparison::Greater,&Number::Long(0),Dispatch::Object).unwrap());
        for (left,right) in [("1.200","1.21"),("-1.21","-1.200"),("-1E+10","-1E-10")] {
            assert!(Number::decimal(left).unwrap().compare_numeric(Comparison::Less,&Number::decimal(right).unwrap(),Dispatch::Object).unwrap());
        }
        assert!(Number::decimal("1.200").unwrap().compare_numeric(Comparison::Equivalent,&Number::decimal("1.2").unwrap(),Dispatch::Object).unwrap());
    }
}
