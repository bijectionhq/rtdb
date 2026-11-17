use super::Number;

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) enum Classification { Numeric,Integer,FixedInteger,Double,Floating,Ratio,Decimal,Rational,PositiveInteger,NegativeInteger,NaturalInteger }

impl Number {
    pub(crate) fn classifies(&self,classification: Classification) -> bool {
        use Classification as C;
        match self {
            Self::Byte(value) => classification.fixed(i64::from(*value)),
            Self::Short(value) => classification.fixed(i64::from(*value)),
            Self::Int(value) => classification.fixed(i64::from(*value)),
            Self::Long(value) => classification.fixed(*value),
            Self::BigInt(_) => matches!(classification,C::Numeric | C::Integer | C::Rational),
            Self::Float(_) => matches!(classification,C::Numeric | C::Floating),
            Self::Double(_) => matches!(classification,C::Numeric | C::Double | C::Floating),
            Self::Decimal { .. } => matches!(classification,C::Numeric | C::Decimal | C::Rational),
            Self::Ratio { .. } => matches!(classification,C::Numeric | C::Ratio | C::Rational),
        }
    }
}

impl Classification {
    fn fixed(self,value: i64) -> bool {
        match self {
            Self::Numeric | Self::Integer | Self::FixedInteger | Self::Rational => true,
            Self::PositiveInteger => value > 0,Self::NegativeInteger => value < 0,Self::NaturalInteger => value >= 0,
            Self::Double | Self::Floating | Self::Ratio | Self::Decimal => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::BigInt;
    const ALL: [Classification;11] = [Classification::Numeric,Classification::Integer,Classification::FixedInteger,Classification::Double,Classification::Floating,Classification::Ratio,Classification::Decimal,Classification::Rational,Classification::PositiveInteger,Classification::NegativeInteger,Classification::NaturalInteger];

    #[test]
    fn classifications_follow_runtime_categories_instead_of_integral_values() {
        for (value,expected) in [
            (Number::Byte(1),[true,true,true,false,false,false,false,true,true,false,true]),
            (Number::Short(1),[true,true,true,false,false,false,false,true,true,false,true]),
            (Number::Int(1),[true,true,true,false,false,false,false,true,true,false,true]),
            (Number::Long(1),[true,true,true,false,false,false,false,true,true,false,true]),
            (Number::integer("1").unwrap(),[true,true,false,false,false,false,false,true,false,false,false]),
            (Number::Float(1.0),[true,false,false,false,true,false,false,false,false,false,false]),
            (Number::Double(1.0),[true,false,false,true,true,false,false,false,false,false,false]),
            (Number::decimal("1.00").unwrap(),[true,false,false,false,false,false,true,true,false,false,false]),
            (Number::Ratio { numerator: BigInt::from(1),denominator: BigInt::from(1) },[true,false,false,false,false,true,false,true,false,false,false]),
        ] {
            for (classification,expected) in ALL.into_iter().zip(expected) { assert_eq!(value.classifies(classification),expected,"{value:?}: {classification:?}"); }
        }
    }

    #[test]
    fn signed_fixed_classification_handles_endpoints_and_rejects_arbitrary_precision() {
        for value in [i64::MIN,-1,0,1,i64::MAX] {
            for (classification,expected) in [(Classification::PositiveInteger,value > 0),(Classification::NegativeInteger,value < 0),(Classification::NaturalInteger,value >= 0)] {
                assert_eq!(Number::Long(value).classifies(classification),expected);
                assert!(!Number::BigInt(BigInt::from(value)).classifies(classification));
            }
        }
        for value in [Number::Byte(i8::MIN),Number::Short(i16::MIN),Number::Int(i32::MIN)] {
            assert!(value.classifies(Classification::NegativeInteger));
            assert!(!value.classifies(Classification::NaturalInteger));
        }
    }

    #[test]
    fn classification_borrows_extreme_values_without_numeric_conversion() {
        for value in [f64::NAN,f64::INFINITY,f64::NEG_INFINITY,-0.0,0.0] {
            for number in [Number::Double(value),Number::Float(value as f32)] {
                assert!(number.classifies(Classification::Floating));
                assert!(!number.classifies(Classification::Integer));
                assert!(!number.classifies(Classification::Rational));
            }
        }
        let coefficient = BigInt::from(10).pow(8192);
        for value in [Number::BigInt(coefficient.clone()),Number::BigInt(-&coefficient),Number::Decimal { coefficient: coefficient.clone(),scale: i32::MIN },Number::Decimal { coefficient: coefficient.clone(),scale: i32::MAX },Number::Ratio { numerator: coefficient,denominator: BigInt::from(3) }] {
            assert!(value.classifies(Classification::Numeric));
            assert!(value.classifies(Classification::Rational));
            assert!(!value.classifies(Classification::FixedInteger));
        }
    }
}
