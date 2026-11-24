use std::{collections::HashMap, sync::Arc};

use arrow_array::ArrayRef;
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_ipc::{DictionaryBatch, MetadataVersion, reader::read_record_batch};
use arrow_schema::{DataType, Field, Schema, UnionMode};
use arrow_select::concat::concat;
use bytes::Bytes;
use rtdb_columnar::{BufferIdentity, MemoryBudget, RetainedArray};
use tonic::Status;

use super::{add, arrow, invalid, mul, status};

pub(super) fn decode(message: DictionaryBatch<'_>, schema: &Schema, dictionaries: &HashMap<i64, ArrayRef>,
    body: Bytes, version: &MetadataVersion,
) -> Result<ArrayRef, Status> {
    let id = message.id();
    #[allow(deprecated)]
    let fields = schema.fields_with_dict_id(id);
    let Some(DataType::Dictionary(_, values)) = fields.first().map(|field| field.data_type()) else {
        return Err(invalid(format!("dictionary id {id} not found in schema")));
    };
    let batch = message.data().ok_or_else(|| invalid("Dictionary message has no record batch"))?;
    let schema = Arc::new(Schema::new(vec![Field::new("", values.as_ref().clone(), true)]));
    let body = rtdb_format::ipc::align_ipc_body(Buffer::from(body));
    let batch = read_record_batch(&body, batch, schema, dictionaries, None, version).map_err(arrow)?;
    Ok(Arc::clone(batch.column(0)))
}

pub(super) fn append(current: &RetainedArray, incoming: &RetainedArray, memory: &Arc<MemoryBudget>,
    is_funded: impl Fn(BufferIdentity) -> bool,
) -> Result<RetainedArray, Status> {
    let descriptors = add(current.descriptor_bytes(), incoming.descriptor_bytes())?;
    let inspection = memory.reserve(add(4096, mul(descriptors, 2)?)?).map_err(status)?;
    let current_data = current.array().to_data();
    let incoming_data = incoming.array().to_data();
    let rows = add(current_data.len(), incoming_data.len())?;
    let bound = add(concat_bound(&current_data, rows, 1)?, concat_bound(&incoming_data, rows, 1)?)?;
    drop(current_data);
    drop(incoming_data);
    drop(inspection);

    let mut copy = memory.reserve(bound).map_err(status)?;
    let values = concat(&[current.array().as_ref(), incoming.array().as_ref()])
        .map_err(|err| invalid(format!("Failed to concat delta dictionary: {err}")))?;
    let grant = Arc::get_mut(&mut copy).expect("dictionary concatenation owns its grant");
    RetainedArray::new(values, grant, |buffer| is_funded(buffer) || incoming.funds(buffer)).map_err(status)
}

fn concat_bound(data: &ArrayData, inherited_rows: usize, copies: usize) -> Result<usize, Status> {
    let rows = inherited_rows.max(mul(data.len(), copies)?);
    let width = match data.data_type() {
        DataType::FixedSizeBinary(width) => usize::try_from(*width).map_err(arrow)?,
        data_type => data_type.primitive_width().unwrap_or(16),
    };
    let mut bytes = add(4096, mul(rows, add(width, 1)?)?)?;
    for buffer in data.buffers() {
        bytes = add(bytes, add(256, mul(mul(buffer.len(), copies)?, 4)?)?)?;
    }

    // Arrow's fallback preallocates each child from the parent's row count. Dense unions and
    // list views can also copy the same child values repeatedly when their offsets overlap.
    let (child_rows, child_copies) = match data.data_type() {
        DataType::FixedSizeList(_, width) => (mul(rows, usize::try_from(*width).map_err(arrow)?)?, copies),
        DataType::Dictionary(_, _) => {
            // Nested dictionary merges can build a hash table and key remapping per value.
            let values = mul(data.child_data()[0].len(), copies)?;
            bytes = add(bytes, mul(values, 256)?)?;
            (values, copies)
        }
        DataType::Union(_, UnionMode::Dense) if dense_offsets_repeat(data) => (rows, mul(copies, data.len())?),
        DataType::ListView(_) if list_view_offsets_overlap::<i32>(data) => (rows, mul(copies, data.len())?),
        DataType::LargeListView(_) if list_view_offsets_overlap::<i64>(data) => (rows, mul(copies, data.len())?),
        _ => (rows, copies),
    };
    for child in data.child_data() { bytes = add(bytes, concat_bound(child, child_rows, child_copies)?)?; }
    Ok(bytes)
}

fn dense_offsets_repeat(data: &ArrayData) -> bool {
    let mut last = [-1; 128];
    data.buffer::<i8>(0).iter().zip(data.buffer::<i32>(1)).take(data.len()).any(|(&leg, &offset)| {
        let Ok(leg) = usize::try_from(leg) else { return true; };
        let previous = &mut last[leg];
        let repeats = offset <= *previous;
        *previous = offset;
        repeats
    })
}

fn list_view_offsets_overlap<T: arrow_buffer::ArrowNativeType>(data: &ArrayData) -> bool {
    let mut end = 0;
    data.buffer::<T>(0).iter().zip(data.buffer::<T>(1)).take(data.len()).any(|(&offset, &size)| {
        let start = offset.as_usize();
        let overlaps = size.as_usize() != 0 && start < end;
        end = start.saturating_add(size.as_usize()).max(end);
        overlaps
    })
}
