use std::sync::Arc;

use arrow_schema::{DataType, FieldRef, Schema, SchemaRef, UnionFields, UnionMode};
use rtdb_types::Result;

use super::invalid;

#[derive(Clone, Copy)]
enum FieldPosition { TopLevel, Child, MapEntries, MapKey }

pub(super) fn adapt(schema: &SchemaRef) -> Result<SchemaRef> {
    let fields = schema.fields().iter().map(|field| adapt_field(field, 0, FieldPosition::TopLevel)).collect::<Result<Vec<_>>>()?;
    if fields.iter().zip(schema.fields()).all(|(decoded, declared)| Arc::ptr_eq(decoded, declared)) {
        return Ok(Arc::clone(schema));
    }
    Ok(Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())))
}

fn adapt_field(field: &FieldRef, depth: usize, position: FieldPosition) -> Result<FieldRef> {
    let data_type = adapt_type(field.data_type(), depth, position)?;
    let nullable = field.is_nullable() || (matches!(position, FieldPosition::Child) && dense_union_can_be_null(&data_type));
    if nullable == field.is_nullable() && data_type == *field.data_type() {
        return Ok(Arc::clone(field));
    }
    Ok(Arc::new(field.as_ref().clone().with_data_type(data_type).with_nullable(nullable)))
}

fn dense_union_can_be_null(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Union(fields, UnionMode::Dense)
        if fields.iter().any(|(_, leg)| leg.is_nullable() || matches!(leg.data_type(), DataType::Null)))
}

fn adapt_type(data_type: &DataType, depth: usize, position: FieldPosition) -> Result<DataType> {
    if depth > 128 { return Err(invalid("Arrow decoding schema exceeds 128 levels")); }
    let child = |field| adapt_field(field, depth + 1, FieldPosition::Child);
    Ok(match data_type {
        DataType::Struct(fields) => DataType::Struct(fields.iter().enumerate().map(|(index, field)| {
            let position = if index == 0 && matches!(position, FieldPosition::MapEntries) { FieldPosition::MapKey } else { FieldPosition::Child };
            adapt_field(field, depth + 1, position)
        }).collect::<Result<Vec<_>>>()?.into()),
        DataType::Union(fields, mode) => DataType::Union(UnionFields::try_new(
            fields.iter().map(|(id, _)| id),
            fields.iter().map(|(_, field)| child(field)).collect::<Result<Vec<_>>>()?,
        ).map_err(invalid)?, *mode),
        DataType::List(field) => DataType::List(child(field)?),
        DataType::LargeList(field) => DataType::LargeList(child(field)?),
        DataType::ListView(field) => DataType::ListView(child(field)?),
        DataType::LargeListView(field) => DataType::LargeListView(child(field)?),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(child(field)?, *size),
        DataType::Map(field, ordered) => DataType::Map(adapt_field(field, depth + 1, FieldPosition::MapEntries)?, *ordered),
        DataType::RunEndEncoded(ends, values) => DataType::RunEndEncoded(child(ends)?, child(values)?),
        DataType::Dictionary(key, value) => DataType::Dictionary(key.clone(), Box::new(adapt_type(value, depth + 1, position)?)),
        data_type => data_type.clone(),
    })
}
