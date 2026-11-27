//! The plain Arrow layouts XTDB writes, for the layouts it never does: dictionaries, views, list views and run-end encoding.
//! A stream RTDB writes carries the same values in the plain layout, so a log record holding one stays XTDB-readable.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int16Type, Int32Type, Int64Type, RunEndIndexType};
use arrow_array::{
    Array, ArrayRef, BinaryArray, GenericListViewArray, ListArray, OffsetSizeTrait, RecordBatch, RecordBatchOptions,
    RunArray, StringArray, UInt64Array, make_array,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, FieldRef, Schema, UnionFields};
use arrow_select::take::take;
use rtdb_types::Result;

use super::{allocation_add, invalid};

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
        DataType::List(child) | DataType::ListView(child) | DataType::LargeListView(child) => DataType::List(plain_child(child)),
        DataType::LargeList(child) => DataType::LargeList(plain_child(child)),
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
        DataType::Utf8View => {
            let views = array.as_string_view();
            check_byte_offsets(views.iter().flatten().map(str::len))?;
            Arc::new(views.iter().collect::<StringArray>()) as ArrayRef
        }
        DataType::BinaryView => {
            let views = array.as_binary_view();
            check_byte_offsets(views.iter().flatten().map(<[u8]>::len))?;
            Arc::new(views.iter().collect::<BinaryArray>()) as ArrayRef
        }
        DataType::RunEndEncoded(..) => take_run_values(array, 0..array.len())?,
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

fn take_run_values(array: &ArrayRef, rows: impl Iterator<Item = usize>) -> Result<ArrayRef> {
    fn gather<R: RunEndIndexType>(array: &RunArray<R>, rows: impl Iterator<Item = usize>) -> Result<ArrayRef> {
        let indices = UInt64Array::from_iter_values(rows.map(|row| array.get_physical_index(row) as u64));
        take(array.values().as_ref(), &indices, None).map_err(invalid)
    }

    let DataType::RunEndEncoded(ends, _) = array.data_type() else {
        return Err(invalid("Arrow column has no run ends"));
    };
    match ends.data_type() {
        DataType::Int16 => gather(array.as_run::<Int16Type>(), rows),
        DataType::Int32 => gather(array.as_run::<Int32Type>(), rows),
        DataType::Int64 => gather(array.as_run::<Int64Type>(), rows),
        other => Err(invalid(format!("Unsupported run-end type {other}"))),
    }
}

fn check_byte_offsets(mut lengths: impl Iterator<Item = usize>) -> Result<()> {
    let length = lengths.try_fold(0, allocation_add)?;
    i32::try_from(length).map_err(|_| invalid("Arrow byte offsets overflow"))?;
    Ok(())
}

fn list_from_view<O: OffsetSizeTrait>(array: &GenericListViewArray<O>, child: &FieldRef) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(allocation_add(array.len(), 1)?);
    offsets.push(0i32);
    let mut length = 0;
    for row in 0..array.len() {
        if array.is_valid(row) {
            length = allocation_add(length, array.value_sizes()[row].as_usize())?;
        }
        offsets.push(i32::try_from(length).map_err(|_| invalid("Arrow list offsets overflow"))?);
    }
    let rows = (0..array.len()).filter(|&row| array.is_valid(row)).flat_map(|row| {
        let start = array.value_offsets()[row].as_usize();
        start..start + array.value_sizes()[row].as_usize()
    });
    let values = array.values();
    let selected = if matches!(values.data_type(), DataType::RunEndEncoded(..)) {
        take_run_values(values, rows)?
    } else {
        let indices = UInt64Array::from_iter_values(rows.map(|row| row as u64));
        take(values.as_ref(), &indices, None).map_err(invalid)?
    };
    let list = ListArray::try_new(plain_child(child), OffsetBuffer::new(offsets.into()), plain_array(&selected)?, array.nulls().cloned())
        .map_err(invalid)?;
    Ok(Arc::new(list))
}
