use std::sync::Arc;

use arrow_array::{ArrayRef, Decimal128Array, Decimal256Array};
use arrow_buffer::i256;
use arrow_schema::Field;
use num_bigint::{BigInt, Sign};
use num_traits::ToPrimitive;
use rtdb_types::{DecimalValue, LogicalType, Result, ScalarType, Value};

use crate::invalid;

pub(crate) struct Numeric {
    scale: i32,
    coefficient: Coefficient,
}

enum Coefficient {
    Narrow(i128),
    Wide(i256),
}

pub(crate) fn parse(text: &str) -> Result<Numeric> {
    let decimal = rtdb_expr::parse_decimal(text).map_err(invalid)?;
    let (coefficient, scale) = decimal.into_bigint_and_scale();
    Numeric::new(coefficient, i32::try_from(scale).map_err(invalid)?)
}

impl Numeric {
    pub(crate) fn new(coefficient: BigInt, scale: i32) -> Result<Self> {
        let narrow_limit = 10i128.pow(32);
        if let Some(value) = coefficient.to_i128() && value.unsigned_abs() < narrow_limit as u128 {
            return Ok(Self { scale, coefficient: Coefficient::Narrow(value) });
        }
        if coefficient.bits() >= 256 {
            return Err(invalid("Decimal exceeds the pinned value writer precision"));
        }
        let mut limbs = coefficient.iter_u64_digits().map(u128::from);
        let mut pair = || limbs.next().unwrap_or_default() | (limbs.next().unwrap_or_default() << 64);
        let magnitude = i256::from_parts(pair(), pair() as i128);
        let narrow_limit = i256::from_i128(narrow_limit);
        if magnitude >= narrow_limit.wrapping_mul(narrow_limit) {
            return Err(invalid("Decimal exceeds the pinned value writer precision"));
        }
        let value = if coefficient.sign() == Sign::Minus { magnitude.wrapping_neg() } else { magnitude };
        let coefficient = Coefficient::Wide(value);
        Ok(Self { scale, coefficient })
    }

    fn layout(&self) -> (i32, u16) {
        match self.coefficient {
            Coefficient::Narrow(_) => (32, 128),
            Coefficient::Wide(_) => (64, 256),
        }
    }

    pub(crate) fn into_value(self) -> Result<Value> {
        Ok(Value::Decimal(self.into_decimal()?))
    }

    pub(crate) fn into_decimal(self) -> Result<DecimalValue> {
        let (precision, width) = self.layout();
        let bytes = match self.coefficient {
            Coefficient::Narrow(value) => value.to_be_bytes().to_vec(),
            Coefficient::Wide(value) => value.to_be_bytes().to_vec(),
        };
        DecimalValue::new(bytes, precision, self.scale, width)
    }

    pub(crate) fn into_arrow(self, name: &str) -> Result<(Field, ArrayRef)> {
        let (precision, bit_width) = self.layout();
        let field = rtdb_columnar::to_field(name, &LogicalType::Scalar(ScalarType::Decimal { precision, scale: self.scale, bit_width }))?;
        let array: ArrayRef = match self.coefficient {
            Coefficient::Narrow(value) => Arc::new(Decimal128Array::new(vec![value].into(), None).with_data_type(field.data_type().clone())),
            Coefficient::Wide(value) => Arc::new(Decimal256Array::new(vec![value].into(), None).with_data_type(field.data_type().clone())),
        };
        Ok((field, array))
    }
}
