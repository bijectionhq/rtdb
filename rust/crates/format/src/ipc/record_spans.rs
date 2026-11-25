use std::{io::Write, sync::Arc};

use arrow_array::{OffsetSizeTrait, RecordBatch};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, Buffer};
use arrow_data::ArrayData;
use arrow_ipc::writer::{DictionaryTracker, EncodedData, IpcDataGenerator, write_message};
use arrow_schema::{DataType, Schema};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::Result;

use super::{FirstSchemaWriter, STREAM_ALIGNMENT, allocation_add, allocation_mul, invalid, java_layout, schema_translation_bound, write_options};

/// One uncompressed IPC record whose value buffers still belong to the input pages.
pub struct RecordSpans {
    alignment: usize,
    rows: usize,
    nodes: Vec<arrow_ipc::FieldNode>,
    buffers: Vec<Vec<Buffer>>,
    charge: Arc<MemoryReservation>,
}

impl RecordSpans {
    /// Plans one record with the same logical buffers as concatenating the input pages.
    /// The caller retains the input and its allocation charges through emission.
    pub fn concat(
        pages: &[&RecordBatch], alignment: usize, memory: &Arc<MemoryBudget>,
    ) -> Result<Option<Self>> {
        let Some(first) = pages.first() else { return Ok(None) };
        if !matches!(alignment, 8 | 64) {
            return Err(invalid("Unsupported Arrow record span alignment"));
        }
        if pages.iter().any(|page| page.schema_ref() != first.schema_ref()) {
            return Ok(None);
        }
        if !first.schema_ref().fields().iter().all(|field| supported(field.data_type())) {
            return Ok(None);
        }
        let schema_bound = schema_translation_bound(first.schema_ref())?;
        let page_bound = allocation_mul(allocation_mul(pages.len(), 512)?, first.num_columns().max(1))?;
        let mut plan = Self {
            alignment,
            rows: pages.iter().try_fold(0usize, |rows, page| allocation_add(rows, page.num_rows()))?,
            nodes: Vec::new(),
            buffers: Vec::new(),
            charge: memory.reserve(allocation_add(schema_bound, page_bound)?)?,
        };
        for index in 0..first.num_columns() {
            let sources = pages.iter().map(|page| page.column(index).to_data()).collect::<Vec<_>>();
            if !plan.column(&sources)? {
                return Ok(None);
            }
        }
        Ok(Some(plan))
    }

    fn column(&mut self, sources: &[ArrayData]) -> Result<bool> {
        let data_type = sources[0].data_type();
        let rows = sources.iter().try_fold(0usize, |rows, source| allocation_add(rows, source.len()))?;
        let nulls = if matches!(data_type, DataType::Null) {
            rows
        } else {
            sources.iter().try_fold(0usize, |count, source| allocation_add(count, source.null_count()))?
        };
        let charge = Arc::get_mut(&mut self.charge).expect("Arrow span planning owns its reservation");
        let descriptors = allocation_add(4096, allocation_mul(sources.len(), 512)?)?;
        let scratch = match data_type {
            DataType::Utf8 | DataType::Binary | DataType::List(_) => allocation_mul(allocation_add(rows, 1)?, 4)?,
            DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_) => allocation_mul(allocation_add(rows, 1)?, 8)?,
            DataType::Boolean => rows.div_ceil(8),
            _ => 0,
        };
        let bound = allocation_add(descriptors, allocation_add(scratch, rows.div_ceil(8))?)?;
        charge.grow_to(allocation_add(charge.bytes(), bound)?)?;
        self.nodes.push(arrow_ipc::FieldNode::new(
            i64::try_from(rows).map_err(invalid)?, i64::try_from(nulls).map_err(invalid)?,
        ));
        if matches!(data_type, DataType::Null) {
            return Ok(true);
        }
        if nulls == 0 {
            self.buffers.push(Vec::new());
        } else {
            let mut validity = BooleanBufferBuilder::new(rows);
            for source in sources {
                match source.nulls() {
                    Some(nulls) => validity.append_buffer(nulls.inner()),
                    None => validity.append_n(source.len(), true),
                }
            }
            self.buffers.push(vec![validity.finish().into_inner()]);
        }
        match data_type {
            DataType::Boolean => {
                let mut bits = BooleanBufferBuilder::new(rows);
                for source in sources {
                    bits.append_buffer(&BooleanBuffer::new(
                        source.buffers()[0].clone(), source.offset(), source.len(),
                    ));
                }
                self.buffers.push(vec![bits.finish().into_inner()]);
            }
            DataType::Utf8 | DataType::Binary | DataType::List(_) =>
                return self.offset_column::<i32>(sources, rows),
            DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_) =>
                return self.offset_column::<i64>(sources, rows),
            DataType::Struct(fields) => {
                for index in 0..fields.len() {
                    let children = sources.iter().map(|source| source.child_data()[index].clone()).collect::<Vec<_>>();
                    if !self.column(&children)? { return Ok(false); }
                }
            }
            DataType::FixedSizeList(_, width) => {
                let width = usize::try_from(*width).map_err(invalid)?;
                let children = sources.iter().map(|source| {
                    Ok(source.child_data()[0].slice(allocation_mul(source.offset(), width)?, allocation_mul(source.len(), width)?))
                }).collect::<Result<Vec<_>>>()?;
                return self.column(&children);
            }
            _ => {
                let width = match data_type {
                    DataType::FixedSizeBinary(width) => usize::try_from(*width).map_err(invalid)?,
                    _ => data_type.primitive_width().expect("Arrow span planning checked the type"),
                };
                let values = sources.iter().map(|source| {
                    Ok(source.buffers()[0].slice_with_length(
                        allocation_mul(source.offset(), width)?, allocation_mul(source.len(), width)?,
                    ))
                }).collect::<Result<Vec<_>>>()?;
                self.buffers.push(values);
            }
        }
        Ok(true)
    }

    fn offset_column<O: OffsetSizeTrait>(&mut self, sources: &[ArrayData], rows: usize) -> Result<bool> {
        let mut offsets = Vec::<O>::with_capacity(allocation_add(rows, 1)?);
        offsets.push(O::usize_as(0));
        let mut length = 0;
        let list = matches!(sources[0].data_type(), DataType::List(_) | DataType::LargeList(_));
        let mut values = Vec::with_capacity(sources.len());
        let mut children = Vec::new();
        for source in sources {
            let source_offsets = &source.buffer::<O>(0)[..=source.len()];
            let start = source_offsets[0].as_usize();
            for offset in &source_offsets[1..] {
                let rebased = allocation_add(length, offset.as_usize() - start)?;
                let Some(rebased) = O::from_usize(rebased) else { return Ok(false) };
                offsets.push(rebased);
            }
            let size = source_offsets[source.len()].as_usize() - start;
            length = allocation_add(length, size)?;
            if list {
                children.push(source.child_data()[0].slice(start, size));
            } else {
                values.push(source.buffers()[1].slice_with_length(start, size));
            }
        }
        self.buffers.push(vec![Buffer::from_vec(offsets)]);
        if list {
            self.column(&children)
        } else {
            self.buffers.push(values);
            Ok(true)
        }
    }

    pub fn metadata(&self) -> Result<(Vec<u8>, usize)> {
        let mut length = 0;
        let buffers = self.buffers.iter().map(|spans| {
            let bytes = spans.iter().try_fold(0usize, |bytes, span| allocation_add(bytes, span.len()))?;
            let descriptor = arrow_ipc::Buffer::new(
                i64::try_from(length).map_err(invalid)?, i64::try_from(bytes).map_err(invalid)?,
            );
            length = allocation_add(length, pad(bytes, self.alignment)?)?;
            Ok(descriptor)
        }).collect::<Result<Vec<_>>>()?;
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let buffers = builder.create_vector(&buffers);
        // Use Arrow's FlatBuffer construction order for byte-exact spill and receipt framing.
        let nodes = builder.create_vector(&self.nodes);
        let mut batch = arrow_ipc::RecordBatchBuilder::new(&mut builder);
        batch.add_length(i64::try_from(self.rows).map_err(invalid)?);
        batch.add_nodes(nodes);
        batch.add_buffers(buffers);
        let batch = batch.finish();
        let mut message = arrow_ipc::MessageBuilder::new(&mut builder);
        message.add_version(arrow_ipc::MetadataVersion::V5);
        message.add_header_type(arrow_ipc::MessageHeader::RecordBatch);
        message.add_bodyLength(i64::try_from(length).map_err(invalid)?);
        message.add_header(batch.as_union_value());
        let message = message.finish();
        builder.finish(message, None);
        Ok((builder.finished_data().to_vec(), length))
    }

    pub fn write_body(&self, output: &mut impl Write) -> std::io::Result<()> {
        const PADDING: [u8; 64] = [0; 64];
        for spans in &self.buffers {
            let mut len = 0;
            for span in spans {
                output.write_all(span)?;
                len += span.len();
            }
            output.write_all(&PADDING[..len.next_multiple_of(self.alignment) - len])?;
        }
        Ok(())
    }

    /// Writes this record with the canonical transaction stream framing and schema translation.
    pub fn write_java_stream(&self, schema: &Schema, sink: impl Write) -> Result<()> {
        if self.alignment != STREAM_ALIGNMENT { return Err(invalid("Java Arrow stream alignment differs from its record")); }
        let options = write_options()?;
        let decimal_schema = Arc::new(crate::ipc_decimal::DecimalSchema::new(schema)?);
        let mut output = FirstSchemaWriter {
            inner: java_layout::JavaLayout::new(sink), schema: Arc::clone(&decimal_schema),
            pending: Vec::new(), expected: None, complete: false,
        };
        let header = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
            &decimal_schema.wire, &mut DictionaryTracker::new(false), &options,
        );
        write_message(&mut output, header, &options).map_err(invalid)?;
        let (ipc_message, _) = self.metadata()?;
        write_message(&mut output, EncodedData { ipc_message, arrow_data: Vec::new() }, &options).map_err(invalid)?;
        self.write_body(&mut output).map_err(invalid)?;
        output.inner.write_all(&[0; 4]).map_err(invalid)?;
        output.inner.flush().map_err(invalid)?;
        if !output.complete { return Err(invalid("Arrow stream ended before its schema")); }
        Ok(())
    }
}

fn supported(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null | DataType::Boolean | DataType::FixedSizeBinary(_)
        | DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => true,
        DataType::Struct(fields) => fields.iter().all(|field| supported(field.data_type())),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => supported(field.data_type()),
        _ => data_type.primitive_width().is_some(),
    }
}

fn pad(bytes: usize, alignment: usize) -> Result<usize> {
    bytes.checked_next_multiple_of(alignment).ok_or_else(|| invalid("Arrow IPC span length overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, BooleanArray, Int32Array, ListArray, StringArray, types::Int32Type};
    use arrow_ipc::writer::{CompressionContext, IpcWriteOptions};

    #[test]
    fn spans_match_one_concatenated_record_and_its_canonical_stream() {
        let batch = RecordBatch::try_from_iter([
            ("number", Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(4)])) as ArrayRef),
            ("flag", Arc::new(BooleanArray::from(vec![None, Some(true), Some(false), Some(true)])) as ArrayRef),
            ("name", Arc::new(StringArray::from(vec![Some("skip"), None, Some("longer"), Some("z")])) as ArrayRef),
            ("codes", Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
                Some(vec![Some(4)]), None, Some(vec![Some(8), None]), Some(vec![]),
            ])) as ArrayRef),
        ]).unwrap();
        let empty = batch.slice(0, 0);
        let left = batch.slice(1, 1);
        let right = batch.slice(2, 2);
        let pages = [&empty, &left, &right];
        let combined = arrow_select::concat::concat_batches(batch.schema_ref(), pages).unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let spans = RecordSpans::concat(&pages, 64, &memory).unwrap().unwrap();
        let (metadata, size) = spans.metadata().unwrap();
        let mut body = Vec::new();
        spans.write_body(&mut body).unwrap();
        let (_, expected) = IpcDataGenerator::default().encode(
            &combined, &mut DictionaryTracker::new(false), &IpcWriteOptions::default(),
            &mut CompressionContext::default(),
        ).unwrap();
        assert_eq!(metadata, expected.ipc_message);
        assert_eq!(size, expected.arrow_data.len());
        assert_eq!(body, expected.arrow_data);

        let spans = RecordSpans::concat(&pages, STREAM_ALIGNMENT, &memory).unwrap().unwrap();
        let mut stream = Vec::new();
        spans.write_java_stream(batch.schema_ref(), &mut stream).unwrap();
        let expected = super::super::write_stream(batch.schema_ref(), &[combined]).unwrap();
        assert_eq!(stream, expected);
    }
}
