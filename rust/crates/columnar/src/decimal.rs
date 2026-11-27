use std::sync::Arc;
use arrow_array::Array;
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, Schema, UnionFields};
use rtdb_types::{Anomaly, Result};

use crate::ArrowCell;

pub use rtdb_types::decimal_arrow::{
    DecimalParameters, INTERNAL_DECIMAL_PRECISION_KEY, INTERNAL_DECIMAL_SCALE_KEY, decimal_field_parameters,
    decimal_parameters, decimal_scale, decimal_storage_precision, decimal_storage_scale, with_decimal_parameters,
    with_decimal_scale,
};

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
        cell.validate_decimal_precision()
    }

    if !has_decimal(field.data_type()) { return Ok(()); }
    (0..array.len()).try_for_each(|row| validate(ArrowCell { field, array, row }))
}

impl ArrowCell<'_> {
    /// Applies the computed-output precision check to the reached decimal cell.
    pub fn validate_decimal_precision(self) -> Result<()> {
        let Some((parameters, mut coefficient)) = self.decimal_coefficient()? else { return Ok(()); };
        let width = usize::from(parameters.bit_width / 8);
        let start = coefficient.len() - width;
        let sign = if coefficient[start] & 0x80 == 0 { 0 } else { 0xff };
        coefficient[..start].fill(sign);
        if fits_precision(i256::from_be_bytes(coefficient), precision_bound(parameters.precision)) {
            Ok(())
        } else {
            Err(precision_error())
        }
    }
}

pub(crate) fn precision_bound(precision: i32) -> Option<i256> {
    match precision {
        ..=0 => Some(i256::ZERO),
        1..=76 => Some(i256::from_i128(10).wrapping_pow(precision as u32)),
        _ => None,
    }
}

pub(crate) fn fits_precision(coefficient: i256, bound: Option<i256>) -> bool {
    bound.is_none_or(|limit| coefficient > -limit && coefficient < limit)
}

pub(crate) fn precision_error() -> Anomaly {
    Anomaly::incorrect("rtdb/decimal-precision", "Decimal coefficient exceeds its declared precision").with_kind(rtdb_types::AnomalyKind::NumericValueOutOfRange)
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
