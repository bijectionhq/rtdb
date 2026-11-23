use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StructArray};
use arrow_buffer::{Buffer, OffsetBuffer};
use arrow_ipc::{
    MessageHeader, MetadataVersion,
    reader::StreamDecoder,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::Field;
use bytes::Bytes;
use rtdb_types::Result;

use super::format_error;

const BUFFER_ALIGNMENT: usize = std::mem::align_of::<i64>();
pub(super) const CONTINUATION: [u8; 4] = [0xff; 4];
const END: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];

pub(super) fn encode(batch: &RecordBatch) -> Result<Vec<u8>> {
    let options = IpcWriteOptions::try_new(64, false, MetadataVersion::V5).map_err(format_error)?;
    let mut writer = StreamWriter::try_new_with_options(Vec::new(), batch.schema_ref(), options)
        .map_err(format_error)?;
    writer.write(batch).map_err(format_error)?;
    writer.into_inner().map_err(format_error)
}

pub(super) fn decode(bytes: Bytes, max_rows: usize) -> Result<RecordBatch> {
    let (schema, tail) = message(&bytes)?;
    if schema.header_type() != MessageHeader::Schema || schema.bodyLength() != 0 {
        return Err(invalid());
    }
    let (data, tail) = message(tail)?;
    let record = data.header_as_record_batch().ok_or_else(invalid)?;
    let rows = usize::try_from(record.length()).map_err(format_error)?;
    let body = usize::try_from(data.bodyLength()).map_err(format_error)?;
    if rows == 0
        || rows > max_rows
        || tail.get(body..) != Some(END.as_slice())
        || record.compression().is_some()
        || record
            .variadicBufferCounts()
            .is_some_and(|counts| !counts.is_empty())
        || record.nodes().is_none_or(|nodes| {
            nodes.iter().any(|node| {
                let Ok(length) = usize::try_from(node.length()) else {
                    return true;
                };
                length > bytes.len() || !(0..=node.length()).contains(&node.null_count())
            })
        })
        || record.buffers().is_none_or(|buffers| {
            buffers.iter().any(|buffer| {
                buffer.offset() < 0
                    || buffer.offset() % BUFFER_ALIGNMENT as i64 != 0
                    || buffer.length() < 0
                    || buffer
                        .offset()
                        .checked_add(buffer.length())
                        .and_then(|end| usize::try_from(end).ok())
                        .is_none_or(|end| end > body)
            })
        })
    {
        return Err(invalid());
    }
    let mut buffer = if bytes.as_ptr().align_offset(BUFFER_ALIGNMENT) == 0 {
        Buffer::from(bytes)
    } else {
        Buffer::from_slice_ref(bytes.as_ref())
    };
    let mut decoder = StreamDecoder::new();
    let batch = decoder
        .decode(&mut buffer)
        .map_err(format_error)?
        .ok_or_else(invalid)?;
    if decoder.decode(&mut buffer).map_err(format_error)?.is_some() || !buffer.is_empty() {
        return Err(invalid());
    }
    decoder.finish().map_err(format_error)?;
    Ok(batch)
}

fn message(bytes: &[u8]) -> Result<(arrow_ipc::Message<'_>, &[u8])> {
    if !bytes.starts_with(&CONTINUATION) {
        return Err(invalid());
    }
    let length = bytes.get(4..8).ok_or_else(invalid)?;
    let length = u32::from_le_bytes(length.try_into().map_err(format_error)?) as usize;
    let end = 8usize
        .checked_add(length)
        .filter(|end| *end <= bytes.len() && end.is_multiple_of(BUFFER_ALIGNMENT))
        .ok_or_else(invalid)?;
    let message = arrow_ipc::root_as_message(&bytes[8..end]).map_err(format_error)?;
    if message.version() != MetadataVersion::V5 {
        return Err(invalid());
    }
    Ok((message, &bytes[end..]))
}

pub(super) fn list(batch: RecordBatch) -> Result<ArrayRef> {
    let length = i32::try_from(batch.num_rows()).map_err(format_error)?;
    let values = StructArray::from(batch);
    let field = Arc::new(Field::new_list_field(values.data_type().clone(), false));
    let list = ListArray::try_new(
        field,
        OffsetBuffer::new(vec![0, length].into()),
        Arc::new(values),
        None,
    )
    .map_err(format_error)?;
    Ok(Arc::new(list))
}

pub(super) fn unlist(array: &dyn Array) -> Result<RecordBatch> {
    let list = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(invalid)?;
    if list.len() != 1 || list.is_null(0) || list.value_offsets() != [0, list.values().len() as i32]
    {
        return Err(invalid());
    }
    let rows = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(invalid)?;
    if rows.null_count() != 0 {
        return Err(invalid());
    }
    Ok(RecordBatch::from(rows.clone()))
}

fn invalid() -> rtdb_types::Anomaly {
    format_error("Invalid ordered index Arrow IPC object")
}
