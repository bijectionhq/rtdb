//! The plain Arrow layouts XTDB writes, for the layouts it never does: dictionaries, views, list views and run-end encoding.
//! A stream RTDB writes carries the same values in the plain layout, so a log record holding one stays XTDB-readable.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int16Type, Int32Type, Int64Type, RunEndIndexType};
use arrow_array::{
    Array, ArrayRef, BinaryArray, GenericListArray, GenericListViewArray, OffsetSizeTrait, RecordBatch, RecordBatchOptions,
    RunArray, StringArray, UInt64Array, make_array,
};
use arrow_buffer::{ArrowNativeType, OffsetBuffer};
use arrow_schema::{DataType, Field, FieldRef, Schema, UnionFields};
use arrow_select::take::take;
use rtdb_types::Result;

use super::invalid;

/// Whether `data_type` holds a layout only at its leaves and in nesting XTDB writes.
pub(super) fn is_plain(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(..) | DataType::Utf8View | DataType::BinaryView | DataType::RunEndEncoded(..)
        | DataType::ListView(_) | DataType::LargeListView(_) => false,
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) | DataType::Map(child, _) => {
            is_plain(child.data_type())
        }
        DataType::Struct(fields) => fields.iter().all(|child| is_plain(child.data_type())),
        DataType::Union(fields, _) => fields.iter().all(|(_, child)| is_plain(child.data_type())),
        _ => true,
    }
}

pub(super) fn plain_schema(schema: &Schema) -> Schema {
    Schema::new_with_metadata(schema.fields().iter().map(|field| plain_field(field)).collect::<Vec<_>>(), schema.metadata().clone())
}

/// `page` in `schema`'s plain layout; `schema` is the plain form of the page's own schema.
pub(super) fn plain_page(schema: &Arc<Schema>, page: &RecordBatch) -> Result<RecordBatch> {
    let columns = page.columns().iter().map(plain_array).collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(page.num_rows()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(invalid)
}

fn plain_field(field: &Field) -> Field {
    match field.data_type() {
        // A run-end field is never null itself, and its values field carries the nulls and the value metadata (decimal scales).
        DataType::RunEndEncoded(_, values) => plain_field(values).with_name(field.name()).with_nullable(field.is_nullable() || values.is_nullable()),
        data_type => field.clone().with_data_type(plain_type(data_type)),
    }
}

fn plain_child(field: &FieldRef) -> FieldRef { Arc::new(plain_field(field)) }

fn plain_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Dictionary(_, values) => plain_type(values),
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::RunEndEncoded(_, values) => plain_type(values.data_type()),
        DataType::List(child) | DataType::ListView(child) => DataType::List(plain_child(child)),
        DataType::LargeList(child) | DataType::LargeListView(child) => DataType::LargeList(plain_child(child)),
        DataType::FixedSizeList(child, width) => DataType::FixedSizeList(plain_child(child), *width),
        DataType::Map(entries, sorted) => DataType::Map(plain_child(entries), *sorted),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(plain_child).collect()),
        DataType::Union(fields, mode) => {
            DataType::Union(fields.iter().map(|(id, child)| (id, plain_child(child))).collect::<UnionFields>(), *mode)
        }
        other => other.clone(),
    }
}

/// The same values with the same length, in `plain_type` of the array's type.
fn plain_array(array: &ArrayRef) -> Result<ArrayRef> {
    if is_plain(array.data_type()) { return Ok(Arc::clone(array)); }
    let plain = match array.data_type() {
        DataType::Dictionary(..) => {
            let dictionary = array.as_any_dictionary();
            take(dictionary.values().as_ref(), dictionary.keys(), None).map_err(invalid)?
        }
        DataType::Utf8View => Arc::new(array.as_string_view().iter().collect::<StringArray>()) as ArrayRef,
        DataType::BinaryView => Arc::new(array.as_binary_view().iter().collect::<BinaryArray>()) as ArrayRef,
        DataType::RunEndEncoded(ends, _) => match ends.data_type() {
            DataType::Int16 => expand_runs(array.as_run::<Int16Type>())?,
            DataType::Int32 => expand_runs(array.as_run::<Int32Type>())?,
            DataType::Int64 => expand_runs(array.as_run::<Int64Type>())?,
            other => return Err(invalid(format!("Unsupported run-end type {other}"))),
        },
        DataType::ListView(child) => return list_from_view(array.as_list_view::<i32>(), child),
        DataType::LargeListView(child) => return list_from_view(array.as_list_view::<i64>(), child),
        nested => {
            // Each child keeps its length, so the parent's offsets, type ids and validity still index it.
            let data = array.to_data();
            let children = data.child_data().iter()
                .map(|child| plain_array(&make_array(child.clone())).map(|child| child.to_data()))
                .collect::<Result<Vec<_>>>()?;
            let data = data.into_builder().data_type(plain_type(nested)).child_data(children).build().map_err(invalid)?;
            return Ok(make_array(data));
        }
    };
    plain_array(&plain)
}

fn expand_runs<R: RunEndIndexType>(array: &RunArray<R>) -> Result<ArrayRef> {
    let indices = UInt64Array::from_iter_values((0..array.len()).map(|row| array.get_physical_index(row) as u64));
    take(array.values().as_ref(), &indices, None).map_err(invalid)
}

fn list_from_view<O: OffsetSizeTrait>(array: &GenericListViewArray<O>, child: &FieldRef) -> Result<ArrayRef> {
    let mut indices = Vec::new();
    let mut offsets = Vec::with_capacity(array.len() + 1);
    offsets.push(O::usize_as(0));
    for row in 0..array.len() {
        if array.is_valid(row) {
            let start = array.value_offsets()[row].as_usize();
            indices.extend((start..start + array.value_sizes()[row].as_usize()).map(|index| index as u64));
        }
        offsets.push(O::from_usize(indices.len()).ok_or_else(|| invalid("Arrow list offsets overflow"))?);
    }
    let values = plain_array(&take(array.values().as_ref(), &UInt64Array::from(indices), None).map_err(invalid)?)?;
    let list = GenericListArray::<O>::try_new(plain_child(child), OffsetBuffer::new(offsets.into()), values, array.nulls().cloned())
        .map_err(invalid)?;
    Ok(Arc::new(list))
}
