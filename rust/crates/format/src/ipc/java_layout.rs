//! Re-encodes Arrow stream messages in the byte layout the reference's Java `MessageSerializer` writes.
//! Log offsets are message identities, so a message must occupy exactly the bytes the reference gives it,
//! and block catalogs record file sizes, so a persisted file must too.
//! Schema, record-batch and file-footer flatbuffers are rebuilt in the reference's call order; batch bodies keep a validity
//! bitmap only for nullable fields, always present there, write no offsets for an offset-bearing field with no values,
//! and pad every buffer to eight bytes.

use std::io::{self, Write};

use arrow_ipc::{Field, MessageHeader, Type, UnionMode};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

/// How the reference counts a field's nulls in a record batch: unions and nullable fields record -1 (uncounted).
#[derive(Clone, Copy)]
enum NullCount { Zero, Unknown, Length }

/// One field's place in a record batch: its node's null rule, its buffer count, whether the first buffer is validity,
/// and whether the second holds offsets - which the reference writes empty, rather than as one zero offset, for a field with no values.
#[derive(Clone, Copy)]
struct FieldLayout { nulls: NullCount, buffers: usize, validity: bool, nullable: bool, offsets: bool }

/// A `Write` adapter that rewrites each framed stream message as it passes.
/// The end marker and any message it doesn't recognise pass through as written.
pub(crate) struct JavaLayout<W> {
    pub(crate) inner: W, pending: Vec<u8>, state: State, layouts: Option<Vec<FieldLayout>>,
    footer: Option<FileFooter>,
}

struct FileFooter {
    position: u64,
    blocks: Vec<arrow_ipc::Block>,
    schema: Option<Vec<u8>>,
}

struct Output<'a, W> { inner: &'a mut W, footer: Option<&'a mut FileFooter> }
impl<W: Write> Write for Output<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.inner.write(bytes)?;
        if let Some(footer) = self.footer.as_deref_mut() {
            footer.position += count as u64;
        }
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> { self.inner.flush() }
}

enum State { Prefix, Metadata { length: usize, continuation: bool }, Body { remaining: usize }, Planned(BodyPlan), Ended }

/// Where each of a batch body's buffers comes from in the arrow-rs body and how it is written.
struct BodyPlan { segments: Vec<Segment>, index: usize, position: usize, length: usize }
struct Segment { start: usize, length: usize, emit: Emit, padding: usize }
enum Emit { Copy, Drop, Fill(Vec<u8>) }

impl<W> JavaLayout<W> {
    fn new(inner: W, footer: Option<FileFooter>) -> Self {
        Self { inner, pending: Vec::new(), state: State::Prefix, layouts: None, footer }
    }

    pub(super) fn new_stream(inner: W) -> Self { Self::new(inner, None) }

    pub(super) fn new_file(inner: W, start: u64) -> Self {
        Self::new(inner, Some(FileFooter { position: start, blocks: Vec::new(), schema: None }))
    }

    pub(super) fn file_footer(&self) -> Result<Vec<u8>, String> {
        let file = self.footer.as_ref().ok_or("Arrow writer has no file footer state")?;
        let schema = file.schema.as_deref().ok_or("Arrow file has no schema message")?;
        footer(schema, &file.blocks)
    }
}

impl<W: Write> Write for JavaLayout<W> {
    fn write(&mut self, mut bytes: &[u8]) -> io::Result<usize> {
        let written = bytes.len();
        let mut out = Output { inner: &mut self.inner, footer: self.footer.as_mut() };
        while !bytes.is_empty() {
            match &mut self.state {
                State::Ended => { out.write_all(bytes)?; break; }
                State::Body { remaining } => {
                    let remaining = *remaining;
                    let count = remaining.min(bytes.len());
                    out.write_all(&bytes[..count])?; bytes = &bytes[count..];
                    self.state = if remaining == count { State::Prefix } else { State::Body { remaining: remaining - count } };
                }
                State::Planned(plan) => {
                    let consumed = plan.write(bytes, &mut out)?;
                    bytes = &bytes[consumed..];
                    if plan.position == plan.length { plan.finish(&mut out)?; self.state = State::Prefix; }
                }
                State::Prefix => {
                    let target = if self.pending.len() >= 4 && self.pending[..4] == [0xff; 4] { 8 } else { 4 };
                    let count = (target - self.pending.len()).min(bytes.len());
                    self.pending.extend_from_slice(&bytes[..count]); bytes = &bytes[count..];
                    if self.pending.len() == 4 && self.pending[..4] == [0xff; 4] { continue; }
                    if self.pending.len() < target { continue; }
                    let length = i32::from_le_bytes(self.pending[target - 4..target].try_into().expect("four prefix bytes"));
                    let continuation = target == 8;
                    if length == 0 {
                        out.write_all(&self.pending)?; self.pending.clear(); self.state = State::Ended;
                    } else {
                        let length = usize::try_from(length).map_err(|_| io::Error::other("Negative Arrow metadata length"))?;
                        self.pending.clear(); self.state = State::Metadata { length, continuation };
                    }
                }
                State::Metadata { length, continuation } => {
                    let (length, continuation) = (*length, *continuation);
                    let count = (length - self.pending.len()).min(bytes.len());
                    self.pending.extend_from_slice(&bytes[..count]); bytes = &bytes[count..];
                    if self.pending.len() < length { continue; }
                    let metadata = std::mem::take(&mut self.pending);
                    let rewritten = rewrite(&mut self.layouts, &metadata).map_err(io::Error::other)?;
                    let encoded = rewritten.metadata.unwrap_or(metadata);
                    let offset = out.footer.as_ref().map_or(0, |footer| footer.position);
                    frame(&mut out, &encoded, continuation)?;
                    if let Some(footer) = out.footer.as_deref_mut() {
                        match rewritten.header {
                            MessageHeader::Schema => footer.schema = Some(encoded),
                            MessageHeader::RecordBatch => {
                                let framed = footer.position - offset;
                                let body = rewritten.plan.as_ref().map_or(rewritten.body as i64, BodyPlan::output_length);
                                footer.blocks.push(arrow_ipc::Block::new(offset as i64, framed as i32, body));
                            }
                            _ => {}
                        }
                    }
                    self.state = match rewritten.plan {
                        Some(mut plan) if plan.length == 0 => { plan.finish(&mut out)?; State::Prefix }
                        Some(plan) => State::Planned(plan),
                        None if rewritten.body == 0 => State::Prefix,
                        None => State::Body { remaining: rewritten.body },
                    };
                }
            }
        }
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> { self.inner.flush() }
}

/// A rewritten message: its Java-layout metadata (none passes it through), its body plan, its input body length and its kind.
struct Rewritten { metadata: Option<Vec<u8>>, plan: Option<BodyPlan>, body: usize, header: MessageHeader }

/// Rewrites one message's metadata and plans its body; a schema replaces the batch layouts.
fn rewrite(layouts: &mut Option<Vec<FieldLayout>>, metadata: &[u8]) -> Result<Rewritten, String> {
    let message = arrow_ipc::root_as_message(metadata).map_err(|error| error.to_string())?;
    let body = usize::try_from(message.bodyLength()).map_err(|_| "Negative Arrow body length".to_owned())?;
    let header = message.header_type();
    let (metadata, plan) = match header {
        MessageHeader::Schema => {
            let schema = message.header_as_schema().ok_or("Arrow schema message has no schema")?;
            let mut fields = Vec::new();
            *layouts = schema.fields().iter().flatten().try_for_each(|field| field_layouts(&field, &mut fields)).map(|()| fields);
            (Some(schema_message(message.version().0, &schema)), None)
        }
        MessageHeader::RecordBatch => {
            let batch = message.header_as_record_batch().ok_or("Arrow batch message has no batch")?;
            match layouts.as_deref().and_then(|layouts| BodyPlan::new(&batch, layouts, body)) {
                Some((plan, nodes, buffers)) => {
                    let length = plan.output_length();
                    (Some(record_batch_message(message.version().0, length, &batch, &nodes, &buffers)), Some(plan))
                }
                None => (None, None),
            }
        }
        _ => (None, None),
    };
    Ok(Rewritten { metadata, plan, body, header })
}

impl BodyPlan {
    /// Plans the reference's body for a batch, or none when its layout isn't one the plan can mirror (compressed, or a buffer count it doesn't model).
    fn new(batch: &arrow_ipc::RecordBatch<'_>, layouts: &[FieldLayout], length: usize) -> Option<(Self, Vec<arrow_ipc::FieldNode>, Vec<arrow_ipc::Buffer>)> {
        if batch.compression().is_some() || batch.variadicBufferCounts().is_some_and(|counts| !counts.is_empty()) { return None; }
        let input_nodes: Vec<_> = batch.nodes()?.iter().copied().collect();
        let input_buffers: Vec<_> = batch.buffers()?.iter().copied().collect();
        if input_nodes.len() != layouts.len() || input_buffers.len() != layouts.iter().map(|layout| layout.buffers).sum::<usize>() { return None; }
        let (mut segments, mut nodes, mut buffers) = (Vec::new(), Vec::new(), Vec::new());
        let mut next = 0usize;
        let mut output = 0i64;
        for (node, layout) in input_nodes.iter().zip(layouts) {
            nodes.push(arrow_ipc::FieldNode::new(node.length(), match layout.nulls { NullCount::Zero => 0, NullCount::Unknown => -1, NullCount::Length => node.length() }));
            for index in 0..layout.buffers {
                let buffer = input_buffers[next]; next += 1;
                let (start, input_length) = (usize::try_from(buffer.offset()).ok()?, usize::try_from(buffer.length()).ok()?);
                if start.checked_add(input_length)? > length { return None; }
                let emit = if layout.validity && index == 0 {
                    if !layout.nullable { Emit::Drop }
                    else if input_length == 0 && node.length() > 0 { Emit::Fill(all_valid(usize::try_from(node.length()).ok()?)) }
                    else { Emit::Copy }
                } else if layout.offsets && index == 1 && node.length() == 0 { Emit::Drop } else { Emit::Copy };
                let written = match &emit { Emit::Copy => input_length, Emit::Drop => 0, Emit::Fill(bytes) => bytes.len() };
                let padding = (8 - written % 8) % 8;
                buffers.push(arrow_ipc::Buffer::new(output, i64::try_from(written).ok()?));
                output = output.checked_add(i64::try_from(written + padding).ok()?)?;
                segments.push(Segment { start, length: input_length, emit, padding });
            }
        }
        if segments.windows(2).any(|pair| pair[1].start < pair[0].start + pair[0].length) { return None; }
        Some((Self { segments, index: 0, position: 0, length }, nodes, buffers))
    }
    fn output_length(&self) -> i64 {
        self.segments.iter().map(|segment| (match &segment.emit { Emit::Copy => segment.length, Emit::Drop => 0, Emit::Fill(bytes) => bytes.len() } + segment.padding) as i64).sum()
    }
    /// Consumes input body bytes, emitting each planned buffer once its source range has passed.
    fn write(&mut self, mut bytes: &[u8], out: &mut impl Write) -> io::Result<usize> {
        let consumed = bytes.len().min(self.length - self.position);
        bytes = &bytes[..consumed];
        while let Some(segment) = self.segments.get(self.index) {
            let end = segment.start + segment.length;
            if self.position < segment.start {
                let skip = (segment.start - self.position).min(bytes.len());
                self.position += skip; bytes = &bytes[skip..];
                if self.position < segment.start { break; }
            }
            if self.position < end {
                let count = (end - self.position).min(bytes.len());
                if matches!(segment.emit, Emit::Copy) { out.write_all(&bytes[..count])?; }
                self.position += count; bytes = &bytes[count..];
                if self.position < end { break; }
            }
            if let Emit::Fill(bitmap) = &segment.emit { out.write_all(bitmap)?; }
            out.write_all(&[0; 8][..segment.padding])?;
            self.index += 1;
        }
        self.position += bytes.len();
        Ok(consumed)
    }
    fn finish(&mut self, out: &mut impl Write) -> io::Result<()> {
        if self.index < self.segments.len() { self.write(&[], out)?; }
        if self.index == self.segments.len() { Ok(()) } else { Err(io::Error::other("Arrow batch body ended before its buffers")) }
    }
}

fn all_valid(rows: usize) -> Vec<u8> {
    let mut bitmap = vec![0xff; rows.div_ceil(8)];
    if rows % 8 != 0 { if let Some(last) = bitmap.last_mut() { *last = (1u8 << (rows % 8)) - 1; } }
    bitmap
}

/// `MessageSerializer.writeMessageBuffer`: the length is padded so the prefix and metadata end on an 8-byte boundary.
fn frame(out: &mut impl Write, metadata: &[u8], continuation: bool) -> io::Result<()> {
    let prefix = if continuation { 8 } else { 4 };
    let padding = (8 - (metadata.len() + prefix) % 8) % 8;
    if continuation { out.write_all(&(-1i32).to_le_bytes())?; }
    out.write_all(&i32::try_from(metadata.len() + padding).map_err(io::Error::other)?.to_le_bytes())?;
    out.write_all(metadata)?;
    out.write_all(&[0; 8][..padding])
}

/// Fields in batch node order; none when a field's buffers aren't modelled, which leaves that schema's batches as written.
fn field_layouts(field: &Field<'_>, output: &mut Vec<FieldLayout>) -> Option<()> {
    let nulls = match field.type_type() {
        Type::Union => NullCount::Unknown,
        Type::Null => NullCount::Length,
        _ if field.nullable() => NullCount::Unknown,
        _ => NullCount::Zero,
    };
    let (buffers, validity) = match field.type_type() {
        Type::Union => (if field.type_as_union()?.mode() == UnionMode::Dense { 2 } else { 1 }, false),
        Type::Null => (0, false),
        Type::Struct_ | Type::FixedSizeList => (1, true),
        Type::Utf8 | Type::Binary | Type::LargeUtf8 | Type::LargeBinary => (3, true),
        Type::List | Type::LargeList | Type::Map | Type::Int | Type::FloatingPoint | Type::Bool | Type::Decimal | Type::Date | Type::Time
        | Type::Timestamp | Type::Interval | Type::Duration | Type::FixedSizeBinary => (2, true),
        _ => return None,
    };
    let offsets = matches!(field.type_type(), Type::Utf8 | Type::Binary | Type::LargeUtf8 | Type::LargeBinary | Type::List | Type::LargeList | Type::Map);
    output.push(FieldLayout { nulls, buffers, validity, nullable: field.nullable(), offsets });
    for child in field.children().iter().flatten() { field_layouts(&child, output)?; }
    Some(())
}

/// `MessageSerializer.serializeMessage` over `Schema.getSchema`.
pub(crate) fn schema_message(version: i16, schema: &arrow_ipc::Schema<'_>) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let header = java_schema(&mut builder, schema);
    message(builder, version, MessageHeader::Schema, header.as_union_value(), 0)
}

/// `Schema.getSchema`: fields, then custom metadata, then the schema table.
fn java_schema<'b>(builder: &mut FlatBufferBuilder<'b>, schema: &arrow_ipc::Schema<'_>) -> WIPOffset<arrow_ipc::Schema<'b>> {
    let fields: Vec<_> = schema.fields().iter().flatten().map(|field| java_field(builder, &field)).collect();
    let fields = builder.create_vector(&fields);
    let metadata = key_values(builder, schema.custom_metadata());
    let mut table = arrow_ipc::SchemaBuilder::new(builder);
    table.add_endianness(schema.endianness());
    table.add_fields(fields);
    table.add_custom_metadata(metadata);
    table.finish()
}

/// `ArrowFooter.writeTo` without dictionaries or custom metadata, as `ArrowUnloader`'s file mode writes it,
/// over a Java-layout schema message and the record batches' blocks.
fn footer(schema_message: &[u8], blocks: &[arrow_ipc::Block]) -> Result<Vec<u8>, String> {
    let message = arrow_ipc::root_as_message(schema_message).map_err(|error| error.to_string())?;
    let schema = message.header_as_schema().ok_or("Arrow schema message has no schema")?;
    let mut builder = FlatBufferBuilder::new();
    let schema = java_schema(&mut builder, &schema);
    let dictionaries = builder.create_vector::<arrow_ipc::Block>(&[]);
    let batches = builder.create_vector(blocks);
    let mut table = arrow_ipc::FooterBuilder::new(&mut builder);
    table.add_schema(schema);
    table.add_dictionaries(dictionaries);
    table.add_recordBatches(batches);
    table.add_version(arrow_ipc::MetadataVersion::V5);
    let root = table.finish();
    builder.finish(root, None);
    Ok(builder.finished_data().to_vec())
}

fn message(mut builder: FlatBufferBuilder<'_>, version: i16, header_type: MessageHeader, header: WIPOffset<flatbuffers::UnionWIPOffset>, body: i64) -> Vec<u8> {
    let mut table = arrow_ipc::MessageBuilder::new(&mut builder);
    table.add_header_type(header_type);
    table.add_header(header);
    table.add_version(arrow_ipc::MetadataVersion(version));
    table.add_bodyLength(body);
    let root = table.finish();
    builder.finish(root, None);
    builder.finished_data().to_vec()
}

fn key_values<'b>(builder: &mut FlatBufferBuilder<'b>, values: Option<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<arrow_ipc::KeyValue<'_>>>>) -> WIPOffset<flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<arrow_ipc::KeyValue<'b>>>> {
    // The reference's extension fields name their type before its metadata; arrow-rs keeps field metadata unordered.
    let rank = |key: Option<&str>| match key { Some("ARROW:extension:name") => 0, Some("ARROW:extension:metadata") => 1, _ => 2 };
    let mut values: Vec<_> = values.iter().flatten().collect();
    values.sort_by_key(|entry| rank(entry.key()));
    let entries: Vec<_> = values.into_iter().map(|entry| {
        let key = entry.key().map(|key| builder.create_string(key));
        let value = entry.value().map(|value| builder.create_string(value));
        let mut table = arrow_ipc::KeyValueBuilder::new(builder);
        if let Some(key) = key { table.add_key(key); }
        if let Some(value) = value { table.add_value(value); }
        table.finish()
    }).collect();
    builder.create_vector(&entries)
}

/// `Field.getField`: name, type, dictionary, children and metadata are created before the field table, in that order.
fn java_field<'b>(builder: &mut FlatBufferBuilder<'b>, field: &Field<'_>) -> WIPOffset<Field<'b>> {
    let name = field.name().map(|name| builder.create_string(name));
    let ty = java_type(builder, field);
    let dictionary = field.dictionary().map(|dictionary| {
        let index = dictionary.indexType().map(|index| {
            let mut table = arrow_ipc::IntBuilder::new(builder);
            table.add_bitWidth(index.bitWidth());
            table.add_is_signed(index.is_signed());
            table.finish()
        });
        let mut table = arrow_ipc::DictionaryEncodingBuilder::new(builder);
        table.add_id(dictionary.id());
        table.add_isOrdered(dictionary.isOrdered());
        if let Some(index) = index { table.add_indexType(index); }
        table.finish()
    });
    let children: Vec<_> = field.children().iter().flatten().map(|child| java_field(builder, &child)).collect();
    let children = builder.create_vector(&children);
    let metadata = key_values(builder, field.custom_metadata());
    let mut table = arrow_ipc::FieldBuilder::new(builder);
    if let Some(name) = name { table.add_name(name); }
    table.add_nullable(field.nullable());
    table.add_type_type(field.type_type());
    if let Some(ty) = ty { table.add_type_(ty); }
    table.add_children(children);
    table.add_custom_metadata(metadata);
    if let Some(dictionary) = dictionary { table.add_dictionary(dictionary); }
    table.finish()
}

/// `ArrowType.getType` for each type, adding its fields in the reference's order.
fn java_type<'b>(builder: &mut FlatBufferBuilder<'b>, field: &Field<'_>) -> Option<WIPOffset<flatbuffers::UnionWIPOffset>> {
    macro_rules! empty { ($builder:ident) => { arrow_ipc::$builder::new(builder).finish().as_union_value() } }
    Some(match field.type_type() {
        Type::Int => { let ty = field.type_as_int()?; let mut t = arrow_ipc::IntBuilder::new(builder); t.add_bitWidth(ty.bitWidth()); t.add_is_signed(ty.is_signed()); t.finish().as_union_value() }
        Type::FloatingPoint => { let ty = field.type_as_floating_point()?; let mut t = arrow_ipc::FloatingPointBuilder::new(builder); t.add_precision(ty.precision()); t.finish().as_union_value() }
        Type::FixedSizeBinary => { let ty = field.type_as_fixed_size_binary()?; let mut t = arrow_ipc::FixedSizeBinaryBuilder::new(builder); t.add_byteWidth(ty.byteWidth()); t.finish().as_union_value() }
        Type::Timestamp => {
            let ty = field.type_as_timestamp()?;
            let zone = ty.timezone().map(|zone| builder.create_string(zone));
            let mut t = arrow_ipc::TimestampBuilder::new(builder); t.add_unit(ty.unit()); if let Some(zone) = zone { t.add_timezone(zone); } t.finish().as_union_value()
        }
        Type::Date => { let ty = field.type_as_date()?; let mut t = arrow_ipc::DateBuilder::new(builder); t.add_unit(ty.unit()); t.finish().as_union_value() }
        Type::Time => { let ty = field.type_as_time()?; let mut t = arrow_ipc::TimeBuilder::new(builder); t.add_unit(ty.unit()); t.add_bitWidth(ty.bitWidth()); t.finish().as_union_value() }
        Type::Duration => { let ty = field.type_as_duration()?; let mut t = arrow_ipc::DurationBuilder::new(builder); t.add_unit(ty.unit()); t.finish().as_union_value() }
        Type::Interval => { let ty = field.type_as_interval()?; let mut t = arrow_ipc::IntervalBuilder::new(builder); t.add_unit(ty.unit()); t.finish().as_union_value() }
        Type::Decimal => { let ty = field.type_as_decimal()?; let mut t = arrow_ipc::DecimalBuilder::new(builder); t.add_precision(ty.precision()); t.add_scale(ty.scale()); t.add_bitWidth(ty.bitWidth()); t.finish().as_union_value() }
        Type::Union => {
            let ty = field.type_as_union()?;
            // The reference's unions carry no type ids, which readers take as the identity; other ids are kept.
            let ids = ty.typeIds().filter(|ids| !ids.iter().enumerate().all(|(index, id)| usize::try_from(id) == Ok(index)));
            let ids = ids.map(|ids| builder.create_vector(&ids.iter().collect::<Vec<_>>()));
            let mut t = arrow_ipc::UnionBuilder::new(builder); t.add_mode(ty.mode()); if let Some(ids) = ids { t.add_typeIds(ids); } t.finish().as_union_value()
        }
        Type::Map => { let ty = field.type_as_map()?; let mut t = arrow_ipc::MapBuilder::new(builder); t.add_keysSorted(ty.keysSorted()); t.finish().as_union_value() }
        Type::FixedSizeList => { let ty = field.type_as_fixed_size_list()?; let mut t = arrow_ipc::FixedSizeListBuilder::new(builder); t.add_listSize(ty.listSize()); t.finish().as_union_value() }
        Type::Utf8 => empty!(Utf8Builder), Type::Binary => empty!(BinaryBuilder), Type::Bool => empty!(BoolBuilder),
        Type::List => empty!(ListBuilder), Type::Struct_ => empty!(Struct_Builder), Type::Null => empty!(NullBuilder),
        Type::LargeUtf8 => empty!(LargeUtf8Builder), Type::LargeBinary => empty!(LargeBinaryBuilder), Type::LargeList => empty!(LargeListBuilder),
        Type::RunEndEncoded => empty!(RunEndEncodedBuilder),
        _ => return None,
    })
}

/// `ArrowRecordBatch.writeTo` for an uncompressed batch: nodes, then buffers, then the batch table.
fn record_batch_message(version: i16, body: i64, batch: &arrow_ipc::RecordBatch<'_>, nodes: &[arrow_ipc::FieldNode], buffers: &[arrow_ipc::Buffer]) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let nodes = builder.create_vector(nodes);
    let buffers = builder.create_vector(buffers);
    let mut table = arrow_ipc::RecordBatchBuilder::new(&mut builder);
    table.add_length(batch.length());
    table.add_nodes(nodes);
    table.add_buffers(buffers);
    let header = table.finish();
    message(builder, version, MessageHeader::RecordBatch, header.as_union_value(), body)
}

#[cfg(test)]
#[path = "java_layout_tests.rs"]
mod tests;
