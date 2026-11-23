use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::{FieldNode, MetadataVersion};
use arrow_schema::{DataType, FieldRef, Schema, UnionFields, UnionMode};
use flatbuffers::FlatBufferBuilder;
use rtdb_types::Result;

use super::{invalid, validate_lz4_batch};

pub(super) enum Selection<'a> {
    Iid,
    PutColumns(&'a [String]),
}

pub(super) fn decode(
    body: &Buffer,
    batch: arrow_ipc::RecordBatch<'_>,
    schema: &Schema,
    version: MetadataVersion,
    selection: Selection<'_>,
) -> Result<RecordBatch> {
    let mut layout = Layout {
        batch, version, node: 0, buffer: 0, variadic: 0,
        nodes: Vec::new(), buffers: Vec::new(), counts: Vec::new(),
    };
    let mut fields = Vec::new();
    for field in schema.fields() {
        let selected = match selection {
            Selection::Iid if field.name() == "_iid" => FieldSelection::All,
            Selection::Iid => FieldSelection::Skip,
            Selection::PutColumns(names) if field.name() == "op" => FieldSelection::Operations(names),
            Selection::PutColumns(_) => FieldSelection::All,
        };
        if let Some(field) = layout.field(field, selected)? {
            fields.push(field);
        }
    }
    if layout.node != batch.nodes().map_or(0, |nodes| nodes.len())
        || layout.buffer != batch.buffers().map_or(0, |buffers| buffers.len())
        || layout.variadic != batch.variadicBufferCounts().map_or(0, |counts| counts.len())
    {
        return Err(invalid("Arrow schema and field-node inventory disagree"));
    }

    // The projected descriptors still address the original body.
    let mut builder = FlatBufferBuilder::new();
    let nodes = builder.create_vector(&layout.nodes);
    let buffers = builder.create_vector(&layout.buffers);
    let counts = (!layout.counts.is_empty()).then(|| builder.create_vector(&layout.counts));
    let compression = batch.compression().map(|compression| arrow_ipc::BodyCompression::create(
        &mut builder,
        &arrow_ipc::BodyCompressionArgs { codec: compression.codec(), method: compression.method() },
    ));
    let batch = arrow_ipc::RecordBatch::create(&mut builder, &arrow_ipc::RecordBatchArgs {
        length: batch.length(), nodes: Some(nodes), buffers: Some(buffers), compression,
        variadicBufferCounts: counts,
    });
    builder.finish(batch, None);
    let batch = flatbuffers::root::<arrow_ipc::RecordBatch<'_>>(builder.finished_data()).map_err(invalid)?;
    validate_lz4_batch(batch, body)?;
    let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    arrow_ipc::reader::read_record_batch(body, batch, schema, &HashMap::new(), None, &version).map_err(invalid)
}

#[derive(Clone, Copy)]
enum FieldSelection<'a> {
    All,
    Skip,
    Operations(&'a [String]),
    PutFields(&'a [String]),
}

struct Layout<'a> {
    batch: arrow_ipc::RecordBatch<'a>,
    version: MetadataVersion,
    node: usize,
    buffer: usize,
    variadic: usize,
    nodes: Vec<FieldNode>,
    buffers: Vec<arrow_ipc::Buffer>,
    counts: Vec<i64>,
}

impl Layout<'_> {
    fn field(&mut self, field: &FieldRef, selection: FieldSelection<'_>) -> Result<Option<FieldRef>> {
        let keep = !matches!(selection, FieldSelection::Skip);
        let nodes = self.batch.nodes().filter(|nodes| self.node < nodes.len())
            .ok_or_else(|| invalid("Arrow schema exceeds its field-node inventory"))?;
        if keep {
            let node = nodes.get(self.node);
            if node.null_count() < 0 {
                return Err(invalid("Projected Arrow fields require known null counts"));
            }
            self.nodes.push(*node);
        }
        self.node += 1;

        let physical = match field.data_type() {
            DataType::Dictionary(key, _) => key.as_ref(),
            data_type => data_type,
        };
        let buffers = match physical {
            DataType::Null | DataType::RunEndEncoded(_, _) => 0,
            DataType::Union(_, mode) =>
                usize::from(self.version < MetadataVersion::V5) + if *mode == UnionMode::Dense { 2 } else { 1 },
            DataType::Struct(_) | DataType::FixedSizeList(_, _) => 1,
            DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) => 2,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
            | DataType::ListView(_) | DataType::LargeListView(_) => 3,
            DataType::Utf8View | DataType::BinaryView => {
                let counts = self.batch.variadicBufferCounts().filter(|counts| self.variadic < counts.len())
                    .ok_or_else(|| invalid("Arrow view exceeds its variadic inventory"))?;
                let count = counts.get(self.variadic);
                self.variadic += 1;
                if keep { self.counts.push(count); }
                usize::try_from(count).ok().and_then(|count| count.checked_add(2))
                    .ok_or_else(|| invalid("Arrow view buffer-count overflow"))?
            }
            _ => 2,
        };
        let end = self.buffer.checked_add(buffers).ok_or_else(|| invalid("Arrow buffer-count overflow"))?;
        let inventory = self.batch.buffers().filter(|buffers| end <= buffers.len())
            .ok_or_else(|| invalid("Arrow schema exceeds its buffer inventory"))?;
        if keep {
            self.buffers.extend((self.buffer..end).map(|index| *inventory.get(index)));
        }
        self.buffer = end;

        let next = if keep { FieldSelection::All } else { FieldSelection::Skip };
        let replacement = match physical {
            DataType::Struct(fields) => {
                let mut selected = Vec::new();
                for child in fields {
                    let selected_child = match selection {
                        FieldSelection::PutFields(names) if !names.iter().any(|name| name == child.name()) =>
                            FieldSelection::Skip,
                        _ => next,
                    };
                    if let Some(child) = self.field(child, selected_child)? { selected.push(child); }
                }
                (keep && matches!(selection, FieldSelection::PutFields(_)))
                    .then(|| DataType::Struct(selected.into()))
            }
            DataType::Union(legs, mode) => {
                let mut selected = Vec::new();
                for (id, leg) in legs.iter() {
                    let selection = match selection {
                        FieldSelection::Operations(names) if leg.name() == "put" && matches!(leg.data_type(), DataType::Struct(_)) =>
                            FieldSelection::PutFields(names),
                        _ => next,
                    };
                    if let Some(leg) = self.field(leg, selection)? { selected.push((id, leg)); }
                }
                if matches!(selection, FieldSelection::Operations(_)) {
                    let (ids, fields): (Vec<_>, Vec<_>) = selected.into_iter().unzip();
                    Some(DataType::Union(UnionFields::try_new(ids, fields).map_err(invalid)?, *mode))
                } else {
                    None
                }
            }
            DataType::List(child) | DataType::LargeList(child) | DataType::Map(child, _)
            | DataType::FixedSizeList(child, _) | DataType::ListView(child) | DataType::LargeListView(child) => {
                self.field(child, next)?;
                None
            }
            DataType::RunEndEncoded(ends, child) => {
                self.field(ends, next)?;
                self.field(child, next)?;
                None
            }
            _ => None,
        };
        Ok(keep.then(|| match replacement {
            Some(data_type) => Arc::new(field.as_ref().clone().with_data_type(data_type)),
            None => Arc::clone(field),
        }))
    }
}
