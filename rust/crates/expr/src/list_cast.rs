use std::str::FromStr;

use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use rtdb_plan::CastTarget;
use rtdb_types::{
    ListBuilder, ListValue, ListView, LogicalType, NumericCell, NumericSpan, Result, ScalarType,
    decimal_arrow::DecimalParameters,
};

use crate::numeric::overflow;

pub(crate) enum Input<'a> {
    Stored(ListView<'a>),
    Opened(NumericSpan<'a>),
}

impl Input<'_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Stored(view) => view.len(),
            Self::Opened(span) => span.len(),
        }
    }

    pub(crate) fn get(&self, index: usize) -> NumericCell {
        match self {
            Self::Stored(view) => view.numeric_component(index),
            Self::Opened(span) => span.get(index).expect("index in cast input").signed(),
        }
    }
}

pub(crate) fn numeric(
    source: Input<'_>,
    target: &CastTarget,
    projection: bool,
    reserve: impl FnOnce(usize) -> Result<()>,
) -> Result<Option<ListValue>> {
    if target.union_target().is_some() {
        return Ok(None);
    }
    let target_type = match scalar_target(target) {
        Some(ty) => ty,
        None => return Ok(None),
    };
    if matches!(&source, Input::Stored(view) if !view.is_packed_numeric()) {
        return Ok(None);
    }
    let len = source.len();
    if matches!(target_type, Target::Scalar(ScalarType::F32 | ScalarType::F64))
        && (0..len).all(|index| source.get(index) == NumericCell::Null)
    {
        return Ok(None);
    }
    let per_component = 2 * std::mem::size_of::<rtdb_types::Value>() + 32;
    reserve(len.checked_mul(per_component).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?)?;
    let mut output = ListBuilder::with_capacity(len);
    for index in 0..len {
        let input = source.get(index);
        let Some(ty) = input.scalar_type() else {
            output.push_numeric(NumericCell::Null);
            continue;
        };
        target.validate_source(&LogicalType::Scalar(ty.clone()))?;
        let cell = cast_number(input, &ty, target_type)?;
        if projection {
            validate_output(cell)?;
        }
        if !output.try_push_numeric(cell) {
            return Ok(None);
        }
    }
    Ok(Some(output.finish()))
}

#[derive(Clone, Copy)]
pub(crate) enum Target<'a> {
    Scalar(&'a ScalarType),
    DefaultDecimal,
}

pub(crate) fn scalar_target(target: &CastTarget) -> Option<Target<'_>> {
    let mut target = target;
    while let CastTarget::Checked { target: inner, .. } = target {
        if target.union_target().is_some() {
            return None;
        }
        target = inner;
    }
    match target {
        CastTarget::DecimalDefault => Some(Target::DefaultDecimal),
        CastTarget::Fixed(ty) => {
            let mut ty = ty;
            while let LogicalType::RunEndEncoded { value, .. } = ty {
                ty = value;
            }
            match ty {
                LogicalType::Scalar(
                    scalar @ (ScalarType::I8
                    | ScalarType::I16
                    | ScalarType::I32
                    | ScalarType::I64
                    | ScalarType::U8
                    | ScalarType::U16
                    | ScalarType::U32
                    | ScalarType::U64
                    | ScalarType::F16
                    | ScalarType::F32
                    | ScalarType::F64
                    | ScalarType::Decimal { .. }),
                ) => Some(Target::Scalar(scalar)),
                _ => None,
            }
        }
        _ => None,
    }
}

pub(crate) fn cast_number(input: NumericCell, ty: &ScalarType, target: Target<'_>) -> Result<NumericCell> {
    use NumericCell as N;
    use ScalarType as S;
    let target = match target {
        Target::DefaultDecimal if matches!(input, N::Decimal(..)) => return Ok(input),
        Target::DefaultDecimal => &S::Decimal { precision: 64, scale: 9, bit_width: 256 },
        Target::Scalar(target) => target,
    };
    if ty == target {
        return Ok(input);
    }
    Ok(match target {
        S::F16 => N::F16(half::f16::from_f64(input.as_f64().unwrap()).to_bits()),
        S::F32 => N::F32(input.as_f64().unwrap() as f32),
        S::F64 => N::F64(input.as_f64().unwrap()),
        S::Decimal { precision, scale, bit_width } => {
            let exact = match input {
                N::Decimal(bytes, params) => {
                    BigDecimal::new(BigInt::from_signed_bytes_be(&bytes), i64::from(params.scale))
                }
                value => match value.integer() {
                    Some(integer) => BigDecimal::new(BigInt::from(integer), 0),
                    None => BigDecimal::from_str(&value.as_f64().unwrap().to_string()).map_err(|_| overflow())?,
                },
            };
            let params = DecimalParameters { precision: *precision, scale: *scale, bit_width: *bit_width };
            N::Decimal(crate::decimal::cast_coefficient(exact, params)?, params)
        }
        target => {
            let integer = if let Some(integer) = input.integer() {
                integer
            } else {
                let number = input.as_f64().unwrap();
                if matches!(input, N::Decimal(..)) {
                    crate::cast::decimal_integer(number)?
                } else {
                    crate::cast::float_integer(number)?
                }
            };
            crate::numeric::integer_cell(integer, target)?
        }
    })
}

pub(crate) fn validate_output(cell: NumericCell) -> Result<()> {
    if let NumericCell::Decimal(bytes, params) = cell {
        let width = usize::from(params.bit_width / 8);
        crate::decimal::validate_coefficient(&bytes[32 - width..], params.precision, params.bit_width)?;
    }
    Ok(())
}

pub(crate) fn cast_text_number(text: &str, target: Target<'_>) -> Result<NumericCell> {
    use ScalarType as S;
    let target = match target {
        Target::DefaultDecimal => &S::Decimal { precision: 64, scale: 9, bit_width: 256 },
        Target::Scalar(ty) => ty,
    };
    let mismatch = |name| rtdb_types::function_type_mismatch(name, &[LogicalType::Scalar(S::Utf8)]);
    Ok(match target {
        S::Decimal { precision, scale, bit_width } => {
            let value = crate::parse_decimal(text).map_err(|_| mismatch("decimal"))?;
            let params = DecimalParameters { precision: *precision, scale: *scale, bit_width: *bit_width };
            NumericCell::Decimal(crate::decimal::cast_coefficient(value, params)?, params)
        }
        S::F16 | S::F32 | S::F64 => {
            let number = crate::cast::parse_java_float(text)?;
            cast_number(NumericCell::F64(number), &S::F64, Target::Scalar(target))?
        }
        S::I8 | S::I16 | S::I32 | S::I64 => {
            crate::numeric::integer_cell(crate::cast::parse_java_integer(text, target)?, target)?
        }
        _ => crate::numeric::integer_cell(text.parse::<i128>().map_err(|_| mismatch("integer"))?, target)?,
    })
}
