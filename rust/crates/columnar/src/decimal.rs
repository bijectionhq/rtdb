use std::sync::Arc;
use arrow_array::Array;
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, Schema, UnionFields};
use rtdb_types::{Anomaly, Result};

use crate::ArrowCell;

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
        if narrow(parsed) || stored != 1 || value != &parsed.to_string() { return Err(invalid("Decimal annotation disagrees with its carrier")); }
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

/// Validates the decimal coefficients in a computed Arrow result, including reachable nested cells.
pub fn validate_projected_decimals(field: &Field, array: &dyn Array) -> Result<()> {
    fn has_decimal(data_type: &DataType) -> bool {
        match data_type {
            DataType::Struct(fields) => fields.iter().any(|field| has_decimal(field.data_type())),
            DataType::Union(fields, _) => fields.iter().any(|(_, field)| has_decimal(field.data_type())),
            DataType::List(field) | DataType::FixedSizeList(field, _) | DataType::Map(field, _)
            | DataType::RunEndEncoded(_, field) => has_decimal(field.data_type()),
            _ => decimal_parameters(data_type).is_some(),
        }
    }

    fn validate(cell: ArrowCell<'_>) -> Result<()> {
        if !has_decimal(cell.field.data_type()) { return Ok(()); }
        let cell = cell.physical()?;
        if cell.is_null()? { return Ok(()); }
        if let Some(mut children) = cell.structure() { return children.try_for_each(validate); }
        if let Some(mut children) = cell.sequence() { return children.try_for_each(validate); }
        let Some((parameters, mut coefficient)) = cell.decimal_coefficient()? else { return Ok(()); };
        let precision = parameters.precision;
        if precision >= 77 { return Ok(()); }
        if precision > 0 {
            let width = usize::from(parameters.bit_width / 8);
            let start = coefficient.len() - width;
            let sign = if coefficient[start] & 0x80 == 0 { 0 } else { 0xff };
            coefficient[..start].fill(sign);
            let coefficient = i256::from_be_bytes(coefficient);
            let limit = i256::from_i128(10).wrapping_pow(precision as u32);
            if coefficient > -limit && coefficient < limit { return Ok(()); }
        }
        Err(Anomaly::incorrect("rtdb/decimal-precision", "Decimal coefficient exceeds its declared precision"))
    }

    if !has_decimal(field.data_type()) { return Ok(()); }
    (0..array.len()).try_for_each(|row| validate(ArrowCell { field, array, row }))
}

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

/// Rebuilds a schema's fields bottom-up, preserving container layouts and schema metadata.
/// The caller reserves space for the cloned field tree before invoking this function.
pub fn map_schema_fields(schema: &Schema, mut map: impl FnMut(Field) -> Result<Field>) -> Result<Schema> {
    fn child(value: &Arc<Field>, map: &mut impl FnMut(Field) -> Result<Field>) -> Result<Arc<Field>> { field(value.as_ref(), map).map(Arc::new) }
    fn data_type(value: &DataType, map: &mut impl FnMut(Field) -> Result<Field>) -> Result<DataType> {
        Ok(match value {
            DataType::Struct(fields) => DataType::Struct(fields.iter().map(|value| child(value, map)).collect::<Result<Vec<_>>>()?.into()),
            DataType::Union(fields, mode) => {
                let (ids, fields): (Vec<_>, Vec<_>) = fields.iter().map(|(id, value)| Ok((id, child(value, map)?))).collect::<Result<Vec<_>>>()?.into_iter().unzip();
                DataType::Union(UnionFields::try_new(ids, fields).map_err(crate::arrow_error)?, *mode)
            }
            DataType::List(value) => DataType::List(child(value, map)?),
            DataType::LargeList(value) => DataType::LargeList(child(value, map)?),
            DataType::ListView(value) => DataType::ListView(child(value, map)?),
            DataType::LargeListView(value) => DataType::LargeListView(child(value, map)?),
            DataType::FixedSizeList(value, size) => DataType::FixedSizeList(child(value, map)?, *size),
            DataType::Map(value, sorted) => DataType::Map(child(value, map)?, *sorted),
            DataType::RunEndEncoded(ends, values) => DataType::RunEndEncoded(child(ends, map)?, child(values, map)?),
            DataType::Dictionary(key, value) => DataType::Dictionary(Box::new(data_type(key, map)?), Box::new(data_type(value, map)?)),
            value => value.clone(),
        })
    }
    fn field(value: &Field, map: &mut impl FnMut(Field) -> Result<Field>) -> Result<Field> {
        let data_type = data_type(value.data_type(), map)?;
        map(value.clone().with_data_type(data_type))
    }
    Ok(Schema::new_with_metadata(schema.fields().iter().map(|value| field(value, &mut map)).collect::<Result<Vec<_>>>()?, schema.metadata().clone()))
}

fn invalid(message: &str) -> Anomaly { Anomaly::incorrect("rtdb/decimal-metadata", message) }
