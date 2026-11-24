use arrow_schema::{DataType, Field};
use crate::{Anomaly, Result};

/// Reserved native field metadata; external Arrow schemas must reject this key on ingress.
pub const INTERNAL_DECIMAL_SCALE_KEY: &str = "rtdb:internal:decimal-scale-i32";
/// Reserved native field metadata; external Arrow schemas must reject this key on ingress.
pub const INTERNAL_DECIMAL_PRECISION_KEY: &str = "rtdb:internal:decimal-precision-i32";

/// Logical decimal metadata; signed precision and scale do not constrain the physical coefficient until an ordinary write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecimalParameters { pub precision: i32, pub scale: i32, pub bit_width: u16 }

/// Physical Arrow precision used with an independently retained full-precision field annotation.
pub fn decimal_storage_precision(precision: i32) -> u8 { u8::try_from(precision).unwrap_or(1) }

/// Physical Arrow scale used with an independently retained full-scale field annotation.
pub fn decimal_storage_scale(scale: i32) -> i8 { i8::try_from(scale).unwrap_or(1) }

/// Reads physical storage parameters, including dictionary values; logical annotations are read by `decimal_field_parameters`.
pub fn decimal_parameters(data_type: &DataType) -> Option<(u8, i8, u16)> {
    match data_type {
        DataType::Decimal32(p, s) => Some((*p, *s, 32)),
        DataType::Decimal64(p, s) => Some((*p, *s, 64)),
        DataType::Decimal128(p, s) => Some((*p, *s, 128)),
        DataType::Decimal256(p, s) => Some((*p, *s, 256)),
        DataType::Dictionary(_, value) => decimal_parameters(value),
        _ => None,
    }
}

/// Reads signed logical decimal parameters and checks their physical carriers and canonical annotations.
pub fn decimal_field_parameters(field: &Field) -> Result<Option<DecimalParameters>> {
    let precision = field.metadata().get(INTERNAL_DECIMAL_PRECISION_KEY);
    let scale = field.metadata().get(INTERNAL_DECIMAL_SCALE_KEY);
    let Some((stored_precision, stored_scale, bit_width)) = decimal_parameters(field.data_type()) else {
        return if precision.is_some() || scale.is_some() { Err(invalid("Decimal annotation requires decimal storage")) } else { Ok(None) };
    };
    fn annotated(value: Option<&String>, stored: i32, narrow: impl Fn(i32) -> bool) -> Result<i32> {
        let Some(value) = value else { return Ok(stored); };
        let parsed = value.parse::<i32>().map_err(|_| invalid("Invalid full-width decimal parameter"))?;
        let digits = value.strip_prefix('-').unwrap_or(value);
        let canonical = if parsed == 0 { value == "0" } else { matches!(digits.as_bytes().first(), Some(b'1'..=b'9')) };
        if narrow(parsed) || stored != 1 || !canonical { return Err(invalid("Decimal annotation disagrees with its carrier")); }
        Ok(parsed)
    }
    Ok(Some(DecimalParameters {
        precision: annotated(precision, i32::from(stored_precision), |value| u8::try_from(value).is_ok())?,
        scale: annotated(scale, i32::from(stored_scale), |value| i8::try_from(value).is_ok())?,
        bit_width,
    }))
}

/// Reads the true scale of a native field, validating both logical decimal parameters.
pub fn decimal_scale(field: &Field) -> Result<Option<i32>> { Ok(decimal_field_parameters(field)?.map(|parameters| parameters.scale)) }

/// Attaches logical parameters after the caller has established matching physical decimal storage.
pub fn with_decimal_parameters(mut field: Field, parameters: DecimalParameters) -> Result<Field> {
    if decimal_parameters(field.data_type()) != Some((decimal_storage_precision(parameters.precision), decimal_storage_scale(parameters.scale), parameters.bit_width)) {
        return Err(invalid("Decimal storage disagrees with its logical parameters"));
    }
    let mut metadata = field.metadata().clone();
    metadata.remove(INTERNAL_DECIMAL_SCALE_KEY);
    metadata.remove(INTERNAL_DECIMAL_PRECISION_KEY);
    if i8::try_from(parameters.scale).is_err() { metadata.insert(INTERNAL_DECIMAL_SCALE_KEY.into(), parameters.scale.to_string()); }
    if u8::try_from(parameters.precision).is_err() { metadata.insert(INTERNAL_DECIMAL_PRECISION_KEY.into(), parameters.precision.to_string()); }
    field.set_metadata(metadata);
    Ok(field)
}

/// Replaces the logical scale while preserving and validating the field's logical precision.
pub fn with_decimal_scale(field: Field, scale: i32) -> Result<Field> {
    let mut parameters = decimal_field_parameters(&field)?.ok_or_else(|| invalid("Decimal scale requires decimal storage"))?;
    parameters.scale = scale;
    with_decimal_parameters(field, parameters)
}

fn invalid(message: &str) -> Anomaly { Anomaly::incorrect("rtdb/decimal-metadata", message) }


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wave4_decimal_annotations_keep_their_canonical_spellings() {
        for (text, expected) in [("-200", Some(-200)), ("200", Some(200)), ("+200", None), ("0200", None),
            ("-0200", None), ("-2147483648", Some(i32::MIN)), ("2147483647", Some(i32::MAX))] {
            let field = Field::new("value", DataType::Decimal128(10, 1), false)
                .with_metadata([(INTERNAL_DECIMAL_SCALE_KEY.into(), text.into())].into());
            match expected {
                Some(scale) => assert_eq!(decimal_scale(&field).unwrap(), Some(scale)),
                None => assert_eq!(decimal_scale(&field).unwrap_err().code, "rtdb/decimal-metadata"),
            }
        }
    }
}
