use rtdb_types::Anomaly;
use super::{Error,Number,Result};

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) enum Bit { And,Or,Xor,AndNot,Clear,Set,Flip,ShiftLeft,ShiftRight,UnsignedShiftRight }
impl Bit {
    pub(crate) fn apply(self,left: i64,right: i64) -> i64 {
        let count = (right as u64 & 63) as u32;
        match self {
            Self::And => left & right,Self::Or => left | right,Self::Xor => left ^ right,Self::AndNot => left & !right,
            Self::Clear => left & !1i64.wrapping_shl(count),Self::Set => left | 1i64.wrapping_shl(count),Self::Flip => left ^ 1i64.wrapping_shl(count),
            Self::ShiftLeft => left.wrapping_shl(count),Self::ShiftRight => left >> count,Self::UnsignedShiftRight => ((left as u64) >> count) as i64,
        }
    }
}
pub(crate) fn bit_not(value: i64) -> i64 { !value }
pub(crate) fn bit_test(value: i64,index: i64) -> bool { value & 1i64.wrapping_shl((index as u64 & 63) as u32) != 0 }

impl Number {
    pub(crate) fn bit_operand(&self) -> Result<i64> {
        let message = match self {
            Self::Byte(value) => return Ok(i64::from(*value)),Self::Short(value) => return Ok(i64::from(*value)),Self::Int(value) => return Ok(i64::from(*value)),Self::Long(value) => return Ok(*value),
            Self::BigInt(_) => "bit operation not supported for: class clojure.lang.BigInt",
            Self::Decimal { .. } => "bit operation not supported for: class java.math.BigDecimal",
            Self::Ratio { .. } => "bit operation not supported for: class clojure.lang.Ratio",
            Self::Float(_) => "bit operation not supported for: class java.lang.Float",
            Self::Double(_) => "bit operation not supported for: class java.lang.Double",
        };
        Err(Error::External(Anomaly::fault("xtdb.error/unknown",message)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bit_operands_accept_fixed_integer_categories_only() {
        for value in [Number::Byte(-1),Number::Short(-1),Number::Int(-1),Number::Long(-1)] { assert_eq!(value.bit_operand().unwrap(),-1); }
        for (value,class) in [
            (Number::integer("1").unwrap(),"clojure.lang.BigInt"),
            (Number::decimal("1").unwrap(),"java.math.BigDecimal"),
            (Number::divide(&Number::Long(1),&Number::Long(2)).unwrap(),"clojure.lang.Ratio"),
            (Number::Float(1.0),"java.lang.Float"),(Number::Double(1.0),"java.lang.Double"),
        ] {
            let Error::External(error) = value.bit_operand().unwrap_err() else { panic!() };
            assert_eq!(error.message,format!("bit operation not supported for: class {class}"));
        }
    }
    #[test]
    fn bit_operations_keep_long_bit_patterns_and_mask_counts() {
        assert_eq!(bit_not(0),-1);
        assert_eq!(Bit::And.apply(12,10),8);
        assert_eq!(Bit::Or.apply(12,10),14);
        assert_eq!(Bit::Xor.apply(12,10),6);
        assert_eq!(Bit::AndNot.apply(12,10),4);
        assert_eq!(Bit::ShiftLeft.apply(i64::MAX,1),-2);
        assert_eq!(Bit::ShiftRight.apply(-2,1),-1);
        assert_eq!(Bit::UnsignedShiftRight.apply(-2,1),i64::MAX);
        for count in [-1,63,127,i64::MAX] {
            assert_eq!(Bit::ShiftLeft.apply(1,count),i64::MIN);
            assert_eq!(Bit::ShiftRight.apply(-1,count),-1);
            assert_eq!(Bit::UnsignedShiftRight.apply(-1,count),1);
            assert_eq!(Bit::Clear.apply(-1,count),i64::MAX);
            assert_eq!(Bit::Set.apply(0,count),i64::MIN);
            assert_eq!(Bit::Flip.apply(i64::MIN,count),0);
            assert!(bit_test(i64::MIN,count));
            assert!(!bit_test(1,count));
        }
        for count in [0,64,4294967296,i64::MIN] {
            assert_eq!(Bit::ShiftLeft.apply(1,count),1);
            assert_eq!(Bit::ShiftRight.apply(-1,count),-1);
            assert_eq!(Bit::UnsignedShiftRight.apply(-1,count),-1);
            assert_eq!(Bit::Clear.apply(-1,count),-2);
            assert_eq!(Bit::Set.apply(0,count),1);
            assert_eq!(Bit::Flip.apply(1,count),0);
            assert!(bit_test(1,count));
            assert!(!bit_test(i64::MIN,count));
        }
    }
}
