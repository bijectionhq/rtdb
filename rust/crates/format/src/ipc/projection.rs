use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use arrow_array::RecordBatch;
use arrow_buffer::{Buffer, bit_util::round_upto_multiple_of_64};
use arrow_ipc::{FieldNode, MetadataVersion};
use arrow_schema::{DataType, FieldRef, Schema, SchemaRef, UnionFields, UnionMode};
use flatbuffers::FlatBufferBuilder;
use rtdb_types::{Anomaly, Result};

use super::{
    PageProjection, align_ipc_body, allocation_add, allocation_mul, arrow_descriptors, codec_workspace,
    compressed_buffer_length, decoded_buffer_bound, decoded_buffer_span, field_metadata_bound, invalid,
    ipc_body_aligned, nulls, schema_metadata_bound, validate_lz4_frame,
};

#[derive(Clone, Copy)]
pub(super) enum Selection<'a> {
    Keep,
    Skip,
    Operations(&'a [String]),
    PutFields(&'a [String]),
    Children(&'a [ConfiguredField]),
}

pub(super) enum ConfiguredField {
    Keep,
    Skip,
    Children(Vec<ConfiguredField>),
}

impl ConfiguredField {
    pub(super) fn selection(&self) -> Selection<'_> {
        match self {
            Self::Keep => Selection::Keep,
            Self::Skip => Selection::Skip,
            Self::Children(fields) => Selection::Children(fields),
        }
    }
}

impl<'a> Selection<'a> {
    fn keeps(self) -> bool {
        !matches!(self, Self::Skip)
    }

    fn child(self, index: usize, field: &FieldRef) -> Result<Self> {
        Ok(match self {
            Self::Operations(names) if is_put_struct(field) => Self::PutFields(names),
            Self::PutFields(names) if !names.contains(field.name()) => Self::Skip,
            Self::Children(fields) => fields
                .get(index)
                .ok_or_else(|| invalid("Arrow projection exceeds its configured field inventory"))?
                .selection(),
            Self::Skip => Self::Skip,
            _ => Self::Keep,
        })
    }
}

impl<'a> PageProjection<'a> {
    pub(super) fn field(self, field: &FieldRef) -> Selection<'a> {
        match self {
            Self::Iid if field.name() != "_iid" => Selection::Skip,
            Self::PutColumns(names) if field.name() == "op" && matches!(field.data_type(), DataType::Union(..)) => {
                Selection::Operations(names)
            }
            _ => Selection::Keep,
        }
    }

    pub(super) fn output_schema(self, schema: &SchemaRef, nested: bool) -> Result<SchemaRef> {
        let Self::PutColumns(names) = self else {
            let fields: Vec<_> = schema.fields().iter().filter(|field| field.name() == "_iid").cloned().collect();
            return Ok(Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())));
        };
        if !nested {
            return Ok(Arc::clone(schema));
        }
        let fields = schema
            .fields()
            .iter()
            .map(|field| {
                let DataType::Union(legs, mode) = field.data_type() else {
                    return Ok(Arc::clone(field));
                };
                if field.name() != "op" {
                    return Ok(Arc::clone(field));
                }
                let children = legs.iter().map(|(_, leg)| {
                    if !is_put_struct(leg) {
                        return Arc::clone(leg);
                    }
                    let DataType::Struct(fields) = leg.data_type() else { unreachable!("put struct") };
                    if fields.iter().all(|field| names.contains(field.name())) {
                        return Arc::clone(leg);
                    }
                    let fields = fields.iter().filter(|field| names.contains(field.name())).cloned().collect();
                    Arc::new(leg.as_ref().clone().with_data_type(DataType::Struct(fields)))
                });
                let legs = UnionFields::try_new(legs.iter().map(|(id, _)| id), children).map_err(invalid)?;
                Ok(Arc::new(field.as_ref().clone().with_data_type(DataType::Union(legs, *mode))))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())))
    }
}

fn is_put_struct(field: &FieldRef) -> bool {
    field.name() == "put" && matches!(field.data_type(), DataType::Struct(_))
}

#[derive(Clone, Copy)]
pub(super) enum Preparation {
    Measure,
    ReadPage,
    ReadPreparedFile,
}

pub(super) fn prepare<'a, 'selection, G>(
    body: &'a Buffer,
    batch: arrow_ipc::RecordBatch<'a>,
    schema: &Schema,
    version: MetadataVersion,
    mut select: impl FnMut(usize, &FieldRef) -> Selection<'selection>,
    preparation: Preparation,
    reserve_scratch: impl FnOnce(usize) -> Result<G>,
) -> Result<Option<Plan<'a, G>>> {
    let measure = matches!(preparation, Preparation::Measure);
    let workspace = codec_workspace(batch)?;
    let mut invalid_null_count = false;
    for node in batch.nodes().into_iter().flatten() {
        usize::try_from(node.length()).map_err(invalid)?;
        invalid_null_count |= node.null_count() < -1;
    }

    let lz4 = !matches!(preparation, Preparation::ReadPreparedFile)
        && batch.compression().is_some_and(|compression| compression.codec() == arrow_ipc::CompressionType::LZ4_FRAME);
    let mut reserve_scratch = Some(reserve_scratch);
    let mut scratch = None;
    let mut allow_growth = || {
        if let Some(reserve) = reserve_scratch.take() {
            let nodes = batch.nodes().map_or(0, |nodes| nodes.len());
            let frames = if lz4 { batch.buffers().map_or(0, |buffers| buffers.len()) } else { 0 };
            // A growing Vec can briefly own both allocations during reallocation.
            scratch = Some(
                allocation_mul(nodes, 3 * size_of::<DescriptorSpan>())
                    .and_then(|nodes| allocation_add(nodes, allocation_mul(frames, 3 * size_of::<Lz4Frame<'_>>())?))
                    .and_then(reserve),
            );
        }
        scratch.as_ref().is_some_and(|grant| grant.is_ok())
    };
    let mut layout = Layout::new(body, batch, version, measure, lz4, &mut allow_growth);
    let base = if measure {
        schema_metadata_bound(schema.metadata()).and_then(|bytes| allocation_add(8192, bytes))
    } else {
        Ok(0)
    };
    let selection = schema.fields().iter().enumerate().try_for_each(|(index, field)| {
        let selection = select(index, field);
        layout.retained.top_level += usize::from(selection.keeps());
        layout.field(field, selection)
    });
    let inventory = layout.inventory();
    // Descriptor errors precede layout fallback, including buffers beyond a failed layout walk.
    layout.check_buffers(inventory.buffer, false)?;
    if let Some(error) = layout.wire_error {
        return Err(error);
    }
    let base = base?;
    if selection.is_err() || layout.position != inventory || (layout.retained.unknown_null_counts && invalid_null_count)
        || layout.retained.requires_full_decoder {
        return Ok(None);
    }
    let inventory = layout.retained;
    let alignment_copy = if measure && inventory.dense_union && !ipc_body_aligned(body) {
        round_upto_multiple_of_64(body.len())
    } else {
        0
    };
    let Ok(bound) = allocation_add(base, inventory.bound).and_then(|bound| allocation_add(bound, alignment_copy)) else {
        return Ok(None);
    };
    let workspace = if layout.decompresses { workspace } else { 0 };
    Ok(Some(Plan { body, batch, version, inventory, bound, workspace, _scratch: scratch.transpose()? }))
}

const INLINE_SPANS: usize = 8;
const INLINE_FRAMES: usize = 16;

struct Entries<T, const N: usize> {
    inline: [T; N],
    len: usize,
    overflow: Vec<T>,
}

impl<T: Copy + Default, const N: usize> Default for Entries<T, N> {
    fn default() -> Self {
        Self { inline: [T::default(); N], len: 0, overflow: Vec::new() }
    }
}

impl<T, const N: usize> Entries<T, N> {
    fn push(&mut self, value: T, allow_growth: &mut dyn FnMut() -> bool) {
        if self.len < N {
            self.inline[self.len] = value;
            self.len += 1;
        } else if allow_growth() {
            self.overflow.push(value);
        }
    }

    fn last_mut(&mut self) -> Option<&mut T> {
        if self.overflow.is_empty() { self.inline[..self.len].last_mut() } else { self.overflow.last_mut() }
    }

    fn iter(&self) -> impl Iterator<Item = &T> {
        self.inline[..self.len].iter().chain(&self.overflow)
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct Position {
    node: usize,
    buffer: usize,
    variadic: usize,
}

#[derive(Clone, Copy, Default)]
struct DescriptorSpan {
    start: Position,
    end: Position,
}

#[derive(Clone, Copy, Default)]
struct Lz4Frame<'a> {
    bytes: &'a [u8],
    expected: usize,
}

pub(super) struct Plan<'a, G> {
    body: &'a Buffer,
    batch: arrow_ipc::RecordBatch<'a>,
    version: MetadataVersion,
    inventory: Inventory<'a>,
    pub(super) bound: usize,
    pub(super) workspace: usize,
    _scratch: Option<G>,
}

#[derive(Default)]
struct Inventory<'a> {
    spans: Entries<DescriptorSpan, INLINE_SPANS>,
    frames: Entries<Lz4Frame<'a>, INLINE_FRAMES>,
    selected: Position,
    top_level: usize,
    nested: bool,
    unknown_null_counts: bool,
    native_incompatible: bool,
    dense_union: bool,
    /// A selected dictionary, or a selected union that may hold the reference's undefined cells.
    requires_full_decoder: bool,
    bound: usize,
}

impl<G> Plan<'_, G> {
    /// Unknown null counts are normalized only in a page written under the footer's metadata version.
    pub(super) fn declines(&self, native_version: bool) -> bool {
        self.inventory.unknown_null_counts && !native_version
    }

    pub(super) fn decode(
        &self,
        native_version: bool,
        source: &SchemaRef,
        indices: Option<&[usize]>,
        output_schema: impl FnOnce(bool) -> Result<SchemaRef>,
        scratch: Option<&Mutex<Descriptors>>,
    ) -> Result<RecordBatch> {
        if self.inventory.unknown_null_counts && !native_version {
            return Err(invalid("Arrow page and footer metadata versions differ"));
        }
        for frame in self.inventory.frames.iter() {
            validate_lz4_frame(frame.bytes, frame.expected)?;
        }
        let aligned = self.inventory.dense_union.then(|| align_ipc_body(self.body.clone()));
        let body = aligned.as_ref().unwrap_or(self.body);
        let batch = self.batch;
        let columns = indices.map_or(source.fields().len(), <[usize]>::len);
        if native_version
            && columns == self.inventory.top_level
            && matches!(self.version, MetadataVersion::V4 | MetadataVersion::V5)
            && !self.inventory.nested
            && !self.inventory.unknown_null_counts
            && !self.inventory.native_incompatible
            && batch.nodes().is_some()
            && batch.buffers().is_some()
            && usize::try_from(batch.length()).is_ok()
        {
            return arrow_ipc::reader::read_record_batch(
                body,
                batch,
                Arc::clone(source),
                &HashMap::new(),
                indices,
                &self.version,
            )
            .map_err(invalid);
        }

        let schema = output_schema(self.inventory.nested)?;
        if let Some(mut scratch) = scratch.and_then(|scratch| scratch.try_lock().ok())
            && scratch.fits(self.inventory.selected)
        {
            return self.decode_rewritten(body, schema, &mut scratch);
        }
        let mut descriptors = Descriptors::with_capacity(self.inventory.selected);
        self.decode_rewritten(body, schema, &mut descriptors)
    }

    fn decode_rewritten(&self, body: &Buffer, schema: SchemaRef, descriptors: &mut Descriptors) -> Result<RecordBatch> {
        let batch = self.batch;
        descriptors.nodes.clear();
        descriptors.buffers.clear();
        descriptors.counts.clear();
        let nodes = batch.nodes();
        let buffers = batch.buffers();
        let counts = batch.variadicBufferCounts();
        for span in self.inventory.spans.iter() {
            let DescriptorSpan { start, end } = *span;
            descriptors.nodes.extend((start.node..end.node).map(|index| *nodes.unwrap().get(index)));
            descriptors.buffers.extend((start.buffer..end.buffer).map(|index| *buffers.unwrap().get(index)));
            descriptors.counts.extend((start.variadic..end.variadic).map(|index| counts.unwrap().get(index)));
        }

        let mut builder = FlatBufferBuilder::new();
        let nodes = builder.create_vector(&descriptors.nodes);
        let buffers = builder.create_vector(&descriptors.buffers);
        let counts = (!descriptors.counts.is_empty()).then(|| builder.create_vector(&descriptors.counts));
        let compression = batch.compression().map(|compression| {
            arrow_ipc::BodyCompression::create(
                &mut builder,
                &arrow_ipc::BodyCompressionArgs { codec: compression.codec(), method: compression.method() },
            )
        });
        let batch = arrow_ipc::RecordBatch::create(
            &mut builder,
            &arrow_ipc::RecordBatchArgs {
                length: batch.length(),
                nodes: Some(nodes),
                buffers: Some(buffers),
                compression,
                variadicBufferCounts: counts,
            },
        );
        let batch = if self.inventory.unknown_null_counts {
            let message = arrow_ipc::Message::create(&mut builder, &arrow_ipc::MessageArgs {
                version: self.version,
                header_type: arrow_ipc::MessageHeader::RecordBatch,
                header: Some(batch.as_union_value()),
                bodyLength: i64::try_from(body.len()).map_err(invalid)?,
                ..Default::default()
            });
            builder.finish(message, None);
            let (metadata, start) = builder.mut_finished_buffer();
            let metadata = &mut metadata[start..];
            nulls::normalize(metadata, body, &schema)?;
            arrow_ipc::root_as_message(metadata).map_err(invalid)?.header_as_record_batch()
                .ok_or_else(|| invalid("Projected Arrow message has no record batch"))?
        } else {
            builder.finish(batch, None);
            flatbuffers::root::<arrow_ipc::RecordBatch<'_>>(builder.finished_data()).map_err(invalid)?
        };
        arrow_ipc::reader::read_record_batch(body, batch, schema, &HashMap::new(), None, &self.version)
            .map_err(invalid)
    }
}

pub(super) struct Descriptors {
    nodes: Vec<FieldNode>,
    buffers: Vec<arrow_ipc::Buffer>,
    counts: Vec<i64>,
}

impl Descriptors {
    pub(super) fn reusable() -> Self {
        Self::with_capacity(Position { node: 32, buffer: 64, variadic: 8 })
    }

    fn with_capacity(size: Position) -> Self {
        Self {
            nodes: Vec::with_capacity(size.node),
            buffers: Vec::with_capacity(size.buffer),
            counts: Vec::with_capacity(size.variadic),
        }
    }

    fn fits(&self, size: Position) -> bool {
        size.node <= self.nodes.capacity()
            && size.buffer <= self.buffers.capacity()
            && size.variadic <= self.counts.capacity()
    }
}

struct Layout<'a, 'b> {
    body: &'a [u8],
    batch: arrow_ipc::RecordBatch<'a>,
    version: MetadataVersion,
    measure: bool,
    compressed: bool,
    lz4: bool,
    allow_growth: &'b mut dyn FnMut() -> bool,
    position: Position,
    checked_buffers: usize,
    wire_error: Option<Anomaly>,
    decompresses: bool,
    retained: Inventory<'a>,
}

impl<'a, 'b> Layout<'a, 'b> {
    fn new(
        body: &'a [u8],
        batch: arrow_ipc::RecordBatch<'a>,
        version: MetadataVersion,
        measure: bool,
        lz4: bool,
        allow_growth: &'b mut dyn FnMut() -> bool,
    ) -> Self {
        Self {
            body,
            batch,
            version,
            measure,
            compressed: batch.compression().is_some(),
            lz4,
            allow_growth,
            position: Position::default(),
            checked_buffers: 0,
            wire_error: None,
            decompresses: false,
            retained: Inventory::default(),
        }
    }

    fn inventory(&self) -> Position {
        Position {
            node: self.batch.nodes().map_or(0, |nodes| nodes.len()),
            buffer: self.batch.buffers().map_or(0, |buffers| buffers.len()),
            variadic: self.batch.variadicBufferCounts().map_or(0, |counts| counts.len()),
        }
    }

    fn check_buffers(&mut self, end: usize, keep: bool) -> Result<()> {
        if self.wire_error.is_some() {
            return Ok(());
        }
        let Some(buffers) = self.batch.buffers() else {
            return Ok(());
        };
        for index in self.checked_buffers..end {
            self.checked_buffers = index + 1;
            let checked = decoded_buffer_span(buffers.get(index), self.body.len(), self.compressed, |start| {
                compressed_buffer_length(self.body, start)
            });
            let (span, decoded, decompresses) = match checked {
                Ok(checked) => checked,
                Err(error) => {
                    self.wire_error = Some(error);
                    break;
                }
            };
            if keep {
                self.decompresses |= decompresses;
                if self.measure {
                    self.retained.bound = allocation_add(self.retained.bound, decoded_buffer_bound(decoded)?)?;
                }
                if self.lz4 && decompresses {
                    self.retained.frames.push(
                        Lz4Frame { bytes: &self.body[span.start + 8..span.end], expected: decoded },
                        self.allow_growth,
                    );
                }
            }
        }
        Ok(())
    }

    /// The reference marks a union cell its row lacks with type id -1, which only the full decoder repairs.
    /// Compressed type ids cannot be inspected before decode, so they may hold such cells.
    fn may_hold_undefined_cells(&self, type_ids: usize, rows: i64) -> Result<bool> {
        let Some(buffers) = self.batch.buffers() else { return Ok(false) };
        let rows = usize::try_from(rows).map_err(invalid)?;
        let (span, _, decompresses) = decoded_buffer_span(buffers.get(type_ids), self.body.len(), self.compressed, |start| {
            compressed_buffer_length(self.body, start)
        })?;
        if decompresses { return Ok(true); }
        let start = if self.compressed && !span.is_empty() { span.start + 8 } else { span.start };
        let ids = self.body.get(start..span.end).ok_or_else(|| invalid("Arrow union type ids exceed their buffer"))?;
        Ok(ids.iter().take(rows).any(|id| (*id as i8) < 0))
    }

    fn field(&mut self, field: &FieldRef, selection: Selection<'_>) -> Result<()> {
        if self.wire_error.is_some() {
            return Ok(());
        }
        let keep = selection.keeps();
        let start = self.position;
        let nodes = self
            .batch
            .nodes()
            .filter(|nodes| start.node < nodes.len())
            .ok_or_else(|| invalid("Arrow schema exceeds its field-node inventory"))?;
        let node = nodes.get(start.node);
        if keep {
            if node.null_count() < -1 {
                return Err(invalid("Arrow null count is below the unknown-count sentinel"));
            }
            self.retained.unknown_null_counts |= node.null_count() == -1;
        }
        self.position.node += 1;

        let physical = match field.data_type() {
            DataType::Dictionary(key, _) => {
                self.retained.native_incompatible |= !key.is_integer();
                key.as_ref()
            }
            data_type => data_type,
        };
        let buffers = match physical {
            DataType::Null | DataType::RunEndEncoded(_, _) => 0,
            DataType::Union(_, mode) => {
                usize::from(self.version < MetadataVersion::V5) + if *mode == UnionMode::Dense { 2 } else { 1 }
            }
            DataType::Struct(_) | DataType::FixedSizeList(_, _) => 1,
            DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) => 2,
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::ListView(_)
            | DataType::LargeListView(_) => 3,
            DataType::Utf8View | DataType::BinaryView => {
                let counts = self
                    .batch
                    .variadicBufferCounts()
                    .filter(|counts| start.variadic < counts.len())
                    .ok_or_else(|| invalid("Arrow view exceeds its variadic inventory"))?;
                self.position.variadic += 1;
                // Arrow queues the original counts even for a top-level field it skips.
                self.retained.native_incompatible |= !keep;
                usize::try_from(counts.get(start.variadic))
                    .ok()
                    .and_then(|count| count.checked_add(2))
                    .ok_or_else(|| invalid("Arrow view buffer-count overflow"))?
            }
            _ => 2,
        };
        let end = start.buffer.checked_add(buffers).ok_or_else(|| invalid("Arrow buffer-count overflow"))?;
        if self.batch.buffers().is_none_or(|buffers| end > buffers.len()) {
            return Err(invalid("Arrow schema exceeds its buffer inventory"));
        }
        self.position.buffer = end;
        self.check_buffers(end, keep)?;
        if self.wire_error.is_some() {
            return Ok(());
        }
        if keep {
            self.retained.dense_union |= matches!(physical, DataType::Union(_, UnionMode::Dense));
            self.retained.requires_full_decoder |= matches!(field.data_type(), DataType::Dictionary(..))
                || (matches!(physical, DataType::Union(..))
                    && self.may_hold_undefined_cells(start.buffer + usize::from(self.version < MetadataVersion::V5), node.length())?);
            if self.measure {
                let mut descriptor = field_metadata_bound(field)?;
                if let DataType::Timestamp(_, Some(zone)) = field.data_type() {
                    descriptor = allocation_add(descriptor, allocation_mul(zone.len(), 16)?)?;
                }
                let rows = node.length() as usize;
                self.retained.bound =
                    allocation_add(self.retained.bound, allocation_add(arrow_descriptors(rows)?, descriptor)?)?;
            }
            let end = self.position;
            if let Some(last) = self.retained.spans.last_mut().filter(|last| last.end == start) {
                last.end = end;
            } else {
                self.retained.spans.push(DescriptorSpan { start, end }, self.allow_growth);
            }
            self.retained.selected.node += 1;
            self.retained.selected.buffer += buffers;
            self.retained.selected.variadic += end.variadic - start.variadic;
        }

        let mut child = |index, field: &FieldRef| {
            let child = selection.child(index, field)?;
            self.retained.nested |= keep && !child.keeps();
            self.field(field, child)
        };
        match physical {
            DataType::Struct(fields) => {
                for (index, field) in fields.iter().enumerate() {
                    child(index, field)?;
                }
            }
            DataType::Union(fields, _) => {
                for (index, (_, field)) in fields.iter().enumerate() {
                    child(index, field)?;
                }
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::Map(field, _)
            | DataType::FixedSizeList(field, _)
            | DataType::ListView(field)
            | DataType::LargeListView(field) => child(0, field)?,
            DataType::RunEndEncoded(ends, values) => {
                child(0, ends)?;
                child(1, values)?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
#[test]
fn fts_arrow_projection_keeps_event_headers_and_only_selected_put_fields() {
    use crate::{data::{EventBatch, PutText, StoredEvent, StoredOperation, build_event_batch}, ipc::{PageDecoder, write_file}};
    use rtdb_types::{EntityId, Interval, LogicalType, RunEndWidth, ScalarType, StructFields, Timestamp, Value};

    let event = |id, operation| StoredEvent {
        position: None, origin: None, entity_key: None, iid: EntityId::Integer(id).iid(),
        system_from: Timestamp(id), commit_position: 0, valid_time: Interval::ALL, operation, recency: None,
    };
    let events = [
        event(8, StoredOperation::Put(StructFields::from([
            ("payload".into(), Value::Utf8("large value".repeat(8192))),
            ("body".into(), Value::Utf8("search text".into())),
        ]))),
        event(9, StoredOperation::Delete),
    ];
    let utf8 = LogicalType::Scalar(ScalarType::Utf8);
    let put_fields = StructFields::from([("body".into(), utf8.clone()), ("payload".into(), utf8)]);
    let batch = build_event_batch(&events, Some(put_fields), Some(RunEndWidth::I32)).unwrap();
    let file = write_file(&batch.schema(), &[batch]).unwrap();
    let tail = file.len() - 10;
    let footer = tail - arrow_ipc::reader::read_footer_length(file[tail..].try_into().unwrap()).unwrap();
    let decoder = PageDecoder::from_footer(&file[footer..tail], footer as u64, None).unwrap();
    let range = decoder.page_range(0).unwrap();
    let bytes = Buffer::from(file[range.start as usize..range.end as usize].to_vec());
    let full = EventBatch::new(decoder.decode_page(0, bytes.clone()).unwrap()).unwrap();
    let projected = EventBatch::new(decoder.decode_event_page(0, bytes.clone(), &["body".into()]).unwrap()).unwrap();
    let headers = EventBatch::new(decoder.decode_event_page(0, bytes, &[]).unwrap()).unwrap();
    for row in 0..events.len() {
        assert_eq!(full.header(row).unwrap(), projected.header(row).unwrap());
        assert_eq!(full.header(row).unwrap(), headers.header(row).unwrap());
    }
    let row = projected.borrow_event(0).unwrap();
    assert!(matches!(row.put_text("body").unwrap(), PutText::Utf8("search text")));
    assert!(matches!(row.put_text("payload").unwrap(), PutText::Missing));
    assert!(matches!(headers.borrow_event(0).unwrap().put_text("body").unwrap(), PutText::Missing));
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use arrow_array::Int64Array;
    use arrow_schema::Field;

    use super::*;

    fn metadata(nodes: &[FieldNode], buffers: &[arrow_ipc::Buffer], compressed: bool) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        let nodes = builder.create_vector(nodes);
        let buffers = builder.create_vector(buffers);
        let compression = compressed.then(|| {
            arrow_ipc::BodyCompression::create(
                &mut builder,
                &arrow_ipc::BodyCompressionArgs {
                    codec: arrow_ipc::CompressionType::LZ4_FRAME,
                    method: arrow_ipc::BodyCompressionMethod::BUFFER,
                },
            )
        });
        let batch = arrow_ipc::RecordBatch::create(
            &mut builder,
            &arrow_ipc::RecordBatchArgs {
                length: 1,
                nodes: Some(nodes),
                buffers: Some(buffers),
                compression,
                variadicBufferCounts: None,
            },
        );
        builder.finish(batch, None);
        builder.finished_data().to_vec()
    }

    #[test]
    fn native_iid_projection_skips_a_nested_column_and_keeps_the_body_allocation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("omitted", DataType::Struct(vec![Field::new("value", DataType::Int64, false)].into()), false),
            Field::new("_iid", DataType::Int64, false),
        ]));
        let descriptors = metadata(
            &[FieldNode::new(1, 0); 3],
            &[
                arrow_ipc::Buffer::new(0, 0),
                arrow_ipc::Buffer::new(0, 0),
                arrow_ipc::Buffer::new(0, 0),
                arrow_ipc::Buffer::new(0, 0),
                arrow_ipc::Buffer::new(0, 8),
            ],
            false,
        );
        let batch = flatbuffers::root::<arrow_ipc::RecordBatch<'_>>(&descriptors).unwrap();
        let body = Buffer::from_vec(vec![19_i64]);
        let source = body.as_ptr();
        let plan = prepare(
            &body,
            batch,
            &schema,
            MetadataVersion::V5,
            |_, field| PageProjection::Iid.field(field),
            Preparation::Measure,
            |_| Ok(()),
        )
        .unwrap()
        .unwrap();
        let page = plan.decode(true, &schema, Some(&[1]), |_| panic!("native projection"), None).unwrap();
        drop(plan);
        drop(body);
        assert_eq!(page.schema(), Arc::new(schema.project(&[1]).unwrap()));
        let selected = page.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(selected.value(0), 19);
        assert_eq!(selected.values().as_ptr().cast::<u8>(), source);
    }

    #[test]
    fn invalid_skipped_descriptors_precede_projected_layout_fallback() {
        let schema = Schema::new(vec![
            Field::new("_iid", DataType::Int64, false),
            Field::new("omitted", DataType::Int64, false),
        ]);
        let body = Buffer::from(Vec::from((-2_i64).to_le_bytes()));
        let cases = [
            (-1, 8, 8, false, invalid(usize::try_from(-1_i64).unwrap_err())),
            (1, -1, 1, false, invalid(usize::try_from(-1_i64).unwrap_err())),
            (1, 8, 1, false, invalid("Arrow buffer exceeds its message body")),
            (1, 0, 7, true, invalid("Compressed Arrow buffer has no length")),
            (1, 0, 8, true, invalid(usize::try_from(-2_i64).unwrap_err())),
        ];
        for (skipped_rows, offset, length, compressed, expected) in cases {
            let descriptors = metadata(
                &[FieldNode::new(1, -1), FieldNode::new(skipped_rows, 0)],
                &[
                    arrow_ipc::Buffer::new(0, 0),
                    arrow_ipc::Buffer::new(0, 0),
                    arrow_ipc::Buffer::new(0, 0),
                    arrow_ipc::Buffer::new(offset, length),
                ],
                compressed,
            );
            let batch = flatbuffers::root::<arrow_ipc::RecordBatch<'_>>(&descriptors).unwrap();
            let result = prepare::<()>(
                &body,
                batch,
                &schema,
                MetadataVersion::V5,
                |_, field| PageProjection::Iid.field(field),
                Preparation::Measure,
                |_| panic!("invalid descriptors must fail before admission"),
            );
            assert_eq!(result.err(), Some(expected));
        }
    }

    #[test]
    fn only_selected_compressed_frames_use_lz4_workspace_and_length_validation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("omitted", DataType::Int64, false),
            Field::new("_iid", DataType::Int64, false),
        ]));
        let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
        encoder.write_all(&19_i64.to_le_bytes()).unwrap();
        let frame = encoder.finish().unwrap();
        for declared in [-1_i64, 8, 7] {
            let mut bytes = Vec::from(1_i64.to_le_bytes());
            bytes.extend(declared.to_le_bytes());
            if declared == -1 {
                bytes.extend(19_i64.to_le_bytes());
            } else {
                bytes.extend(&frame);
            }
            let body = Buffer::from(bytes);
            let descriptors = metadata(
                &[FieldNode::new(1, 0); 2],
                &[
                    arrow_ipc::Buffer::new(0, 0),
                    arrow_ipc::Buffer::new(0, 8),
                    arrow_ipc::Buffer::new(8, 0),
                    arrow_ipc::Buffer::new(8, (body.len() - 8) as i64),
                ],
                true,
            );
            let batch = flatbuffers::root::<arrow_ipc::RecordBatch<'_>>(&descriptors).unwrap();
            let plan = prepare(
                &body,
                batch,
                &schema,
                MetadataVersion::V5,
                |_, field| PageProjection::Iid.field(field),
                Preparation::Measure,
                |_| Ok(()),
            )
            .unwrap()
            .unwrap();
            assert_eq!(plan.workspace, if declared == -1 { 0 } else { 64 << 20 });
            let page = plan.decode(true, &schema, Some(&[1]), |_| panic!("native projection"), None);
            if declared == 7 {
                assert_eq!(page.unwrap_err(), invalid("LZ4 data exceeds its declared Arrow buffer length"));
            } else {
                assert_eq!(page.unwrap().column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 19);
            }
        }
    }
}
