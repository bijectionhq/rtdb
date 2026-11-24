use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions, make_array};
use arrow_data::ArrayData;
use arrow_schema::{DataType, FieldRef, Schema, SchemaRef, UnionFields, UnionMode};
use rtdb_types::Result;

use super::invalid;

pub(super) const MAX_DEPTH: usize = 128;

#[derive(Clone, Copy)]
enum FieldPosition { TopLevel, Child, MapEntries, MapKey }

pub(super) fn adapt(schema: &SchemaRef) -> Result<SchemaRef> {
    let fields = schema.fields().iter().map(|field| adapt_field(field, 0, FieldPosition::TopLevel)).collect::<Result<Vec<_>>>()?;
    if fields.iter().zip(schema.fields()).all(|(decoded, declared)| Arc::ptr_eq(decoded, declared)) {
        return Ok(Arc::clone(schema));
    }
    Ok(Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())))
}

pub(super) fn adapt_batch(batch: &RecordBatch) -> Result<Option<RecordBatch>> {
    let schema = adapt(batch.schema_ref())?;
    if Arc::ptr_eq(&schema, batch.schema_ref()) { return Ok(None); }
    with_schema(batch, schema).map(Some)
}

pub(super) fn with_schema(batch: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let columns = schema.fields().iter().zip(batch.columns()).map(|(field, array)| {
        retype(array.to_data(), field.data_type()).map(make_array)
    }).collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema, columns, &options).map_err(invalid)
}

fn retype(data: ArrayData, target: &DataType) -> Result<ArrayData> {
    if data.data_type() == target { return Ok(data); }
    let types = match target {
        DataType::Struct(fields) => fields.iter().map(|field| field.data_type()).collect::<Vec<_>>(),
        DataType::Union(fields, _) => fields.iter().map(|(_, field)| field.data_type()).collect(),
        DataType::List(field) | DataType::LargeList(field) | DataType::ListView(field) | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) | DataType::Map(field, _) => vec![field.data_type()],
        DataType::RunEndEncoded(ends, values) => vec![ends.data_type(), values.data_type()],
        DataType::Dictionary(_, values) => vec![values.as_ref()],
        _ => return Err(invalid("Arrow decoding schema changed a storage type")),
    };
    if data.child_data().len() != types.len() { return Err(invalid("Arrow decoding schema changed the number of children")); }
    let children = data.child_data().iter().zip(types).map(|(child, target)| retype(child.clone(), target))
        .collect::<Result<Vec<_>>>()?;
    data.into_builder().data_type(target.clone()).child_data(children).build().map_err(invalid)
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
    if depth > MAX_DEPTH { return Err(invalid("Arrow decoding schema exceeds 128 levels")); }
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
