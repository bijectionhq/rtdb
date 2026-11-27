//! Writes Arrow streams and files directly in the reference's Java layout (`xtdb.arrow.ArrowUnloader` in `STREAM` and `FILE`
//! modes), without an arrow-rs stream to rewrite.
//! Every schema, record-batch and file-footer message is built in the reference's call order, and each body buffer is the slice
//! arrow-rs would have written, kept, dropped or replaced with an all-valid bitmap exactly as `JavaLayout` plans it.
//! Streams leave out dictionaries, views, list views and run-end encoding (`supports` sends those to `plain_layout`); files also
//! write run-end encoded columns natively as XTDB's `RunEndEncodedVector` does.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::io::{self, Write};

use arrow_array::{Array, ArrowNativeTypeOp, OffsetSizeTrait, RecordBatch, types::{Int16Type, Int32Type, Int64Type, RunEndIndexType}};
use arrow_buffer::{ArrowNativeType, Buffer};
use arrow_data::{ArrayData, BufferSpec};
use arrow_ipc::MessageHeader;
use arrow_schema::{ArrowError, DataType, Field, IntervalUnit, Schema, TimeUnit, UnionMode};
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, UnionWIPOffset, Vector, WIPOffset};
use rtdb_columnar::{INTERNAL_DECIMAL_PRECISION_KEY, INTERNAL_DECIMAL_SCALE_KEY, decimal_field_parameters};
use rtdb_types::{Anomaly, Result};

use super::invalid;
use super::java_layout::{frame, message, write_all_valid};

const VERSION: i16 = arrow_ipc::MetadataVersion::V5.0;
pub(super) const FILE_MAGIC: &[u8; 6] = b"ARROW1";

/// Whether every field of `schema` has a layout this writer reproduces.
pub(super) fn supports(schema: &Schema) -> bool {
    fn data_type(value: &DataType) -> bool {
        match value {
            DataType::Null | DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 | DataType::Float16 | DataType::Float32
            | DataType::Float64 | DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
            | DataType::FixedSizeBinary(_) | DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_)
            | DataType::Timestamp(..) | DataType::Duration(_) | DataType::Interval(_) | DataType::Decimal32(..)
            | DataType::Decimal64(..) | DataType::Decimal128(..) | DataType::Decimal256(..) => true,
            DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) | DataType::Map(child, _) => {
                data_type(child.data_type())
            }
            DataType::Struct(fields) => fields.iter().all(|child| data_type(child.data_type())),
            DataType::Union(fields, _) => fields.iter().all(|(_, child)| data_type(child.data_type())),
            _ => false,
        }
    }
    schema.fields().iter().all(|field| data_type(field.data_type()))
}

/// Writes the schema, each page and the reference's end marker to `sink`; the caller has checked `supports(schema)`.
/// A page whose schema differs from `schema` is rejected before any of its bytes are written.
pub(super) fn write_stream<'a, W: Write>(schema: &Schema, pages: impl IntoIterator<Item = &'a RecordBatch>, sink: W) -> Result<W> {
    write_pages(schema, pages.into_iter().map(Ok), sink)
}

pub(super) fn write_pages<W: Write, P: Borrow<RecordBatch>>(
    schema: &Schema, pages: impl IntoIterator<Item = Result<P>>, mut sink: W,
) -> Result<W> {
    let metadata = schema_message(schema)?;
    frame(&mut sink, &metadata, true).map_err(io_error)?;
    let mut body = Body::default();
    for page in pages {
        let page = page?;
        let page: &RecordBatch = page.borrow();
        if page.schema_ref().as_ref() != schema {
            return Err(Anomaly::incorrect("rtdb.format/arrow-schema", "Arrow page schema differs from its declared stream schema"));
        }
        for (field, column) in schema.fields().iter().zip(page.columns()) {
            body.column(field, &column.to_data(), column.null_count())?;
        }
        let metadata = body.message(page.num_rows())?;
        frame(&mut sink, &metadata, true).map_err(io_error)?;
        body.write(&mut sink).map_err(io_error)?;
        body.clear();
    }
    sink.write_all(&[0; 4]).map_err(invalid)?;
    sink.flush().map_err(invalid)?;
    Ok(sink)
}

/// Incremental Arrow file writer in the reference's Java layout (`xtdb.arrow.ArrowUnloader.Mode.FILE`):
/// the magic padded to eight bytes, the schema and batch frames, its single zero end marker, then the Java-layout footer,
/// its length and the magic.
pub(super) struct FileWriter<W> {
    sink: W,
    schema: Schema,
    position: u64,
    blocks: Vec<arrow_ipc::Block>,
    body: Body,
}

impl<W: Write> FileWriter<W> {
    pub(super) fn new(mut sink: W, schema: &Schema) -> Result<Self> {
        let metadata = schema_message(schema)?;
        sink.write_all(FILE_MAGIC).map_err(invalid)?;
        sink.write_all(&[0; 2]).map_err(invalid)?;
        let framed = frame(&mut sink, &metadata, true).map_err(io_error)?;
        let position = 8u64.checked_add(u64::try_from(framed).map_err(invalid)?)
            .ok_or_else(|| invalid("Arrow file length exceeds the format"))?;
        Ok(Self {
            sink,
            schema: schema.clone(),
            position,
            blocks: Vec::new(),
            body: Body::default(),
        })
    }

    pub(super) fn validate_batch(&self, page: &RecordBatch) -> Result<()> {
        if page.schema_ref().as_ref() != &self.schema {
            return Err(Anomaly::incorrect("rtdb.format/arrow-schema", "Arrow page schema differs from its declared stream schema"));
        }
        Ok(())
    }

    /// Writes one page whose schema matches this file's schema.
    pub(super) fn write_batch(&mut self, page: &RecordBatch) -> Result<()> {
        self.body.clear();
        for (field, column) in self.schema.fields().iter().zip(page.columns()) {
            self.body.column(field, &column.to_data(), column.null_count())?;
        }
        let metadata = self.body.message(page.num_rows())?;
        let offset = i64::try_from(self.position).map_err(invalid)?;
        let framed = frame(&mut self.sink, &metadata, true).map_err(io_error)?;
        let metadata_length = i32::try_from(framed).map_err(invalid)?;
        self.body.write(&mut self.sink).map_err(io_error)?;
        let body_length = self.body.length;
        let block_bytes = u64::try_from(framed).map_err(invalid)?
            .checked_add(u64::try_from(body_length).map_err(invalid)?)
            .ok_or_else(|| invalid("Arrow file length exceeds the format"))?;
        self.position = self.position.checked_add(block_bytes)
            .ok_or_else(|| invalid("Arrow file length exceeds the format"))?;
        self.blocks.push(arrow_ipc::Block::new(offset, metadata_length, body_length));
        Ok(())
    }

    pub(super) fn sink(&mut self) -> &mut W { &mut self.sink }

    pub(super) fn finish(mut self) -> Result<W> {
        self.sink.write_all(&[0; 4]).map_err(invalid)?;
        let footer = file_footer(&self.schema, &self.blocks)?;
        self.sink.write_all(&footer).map_err(invalid)?;
        self.sink.write_all(&i32::try_from(footer.len()).map_err(invalid)?.to_le_bytes()).map_err(invalid)?;
        self.sink.write_all(FILE_MAGIC).map_err(invalid)?;
        self.sink.flush().map_err(invalid)?;
        Ok(self.sink)
    }
}

fn io_error(error: io::Error) -> Anomaly { invalid(ArrowError::from(error)) }

/// The Java-layout schema message for `schema`, decimal parameters at their full signed width.
/// Annotation errors are those `DecimalSchema::new` reports, found in the same post-order.
pub(super) fn schema_message(schema: &Schema) -> Result<Vec<u8>> {
    validate_schema(schema)?;
    let mut builder = FlatBufferBuilder::new();
    let header = java_schema(&mut builder, schema)?;
    Ok(message(builder, VERSION, MessageHeader::Schema, header.as_union_value(), 0))
}

fn validate_schema(schema: &Schema) -> Result<()> {
    fn validate(field: &Field) -> Result<()> {
        for child in children(field.data_type()) { validate(child)?; }
        decimal_field_parameters(field).map(drop)
    }
    if schema.metadata().keys().any(|key| reserved(key)) {
        return Err(Anomaly::unsupported("rtdb.format/decimal-metadata", "Decimal annotation belongs to a field"));
    }
    for field in schema.fields() { validate(field)?; }
    Ok(())
}

fn java_schema<'b>(builder: &mut FlatBufferBuilder<'b>, schema: &Schema) -> Result<WIPOffset<arrow_ipc::Schema<'b>>> {
    let fields = schema.fields().iter().map(|field| java_field(builder, field)).collect::<Result<Vec<_>>>()?;
    let fields = builder.create_vector(&fields);
    let metadata = key_values(builder, schema.metadata(), false);
    let mut table = arrow_ipc::SchemaBuilder::new(builder);
    table.add_endianness(arrow_ipc::Endianness::Little);
    table.add_fields(fields);
    table.add_custom_metadata(metadata);
    Ok(table.finish())
}

/// `ArrowFooter.writeTo` without dictionaries or custom metadata, as `ArrowUnloader`'s file mode writes it.
fn file_footer(schema: &Schema, blocks: &[arrow_ipc::Block]) -> Result<Vec<u8>> {
    let mut builder = FlatBufferBuilder::new();
    let schema = java_schema(&mut builder, schema)?;
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

fn reserved(key: &str) -> bool { matches!(key, INTERNAL_DECIMAL_PRECISION_KEY | INTERNAL_DECIMAL_SCALE_KEY) }

fn children(data_type: &DataType) -> Vec<&Field> {
    match data_type {
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) | DataType::Map(child, _) => vec![child.as_ref()],
        DataType::Struct(fields) => fields.iter().map(|child| child.as_ref()).collect(),
        DataType::Union(fields, _) => fields.iter().map(|(_, child)| child.as_ref()).collect(),
        DataType::RunEndEncoded(ends, values) => vec![ends.as_ref(), values.as_ref()],
        DataType::ListView(child) | DataType::LargeListView(child) => vec![child.as_ref()],
        _ => Vec::new(),
    }
}

/// arrow-rs writes keys sorted, and the reference names an extension's type before its metadata, ahead of the rest.
fn key_values<'b>(
    builder: &mut FlatBufferBuilder<'b>, metadata: &HashMap<String, String>, field: bool,
) -> WIPOffset<Vector<'b, ForwardsUOffset<arrow_ipc::KeyValue<'b>>>> {
    fn rank(key: &str) -> (u8, &str) {
        (match key { "ARROW:extension:name" => 0, "ARROW:extension:metadata" => 1, _ => 2 }, key)
    }
    let mut values: Vec<(&String, &String)> = metadata.iter().filter(|(key, _)| !(field && reserved(key.as_str()))).collect();
    values.sort_by(|left, right| rank(left.0.as_str()).cmp(&rank(right.0.as_str())));
    let entries: Vec<_> = values.into_iter().map(|(key, value)| {
        let key = builder.create_string(key);
        let value = builder.create_string(value);
        let mut table = arrow_ipc::KeyValueBuilder::new(builder);
        table.add_key(key);
        table.add_value(value);
        table.finish()
    }).collect();
    builder.create_vector(&entries)
}

fn java_field<'b>(builder: &mut FlatBufferBuilder<'b>, field: &Field) -> Result<WIPOffset<arrow_ipc::Field<'b>>> {
    let name = builder.create_string(field.name());
    let (type_type, ty) = java_type(builder, field)?;
    let children = children(field.data_type()).into_iter().map(|child| java_field(builder, child)).collect::<Result<Vec<_>>>()?;
    let children = builder.create_vector(&children);
    let metadata = key_values(builder, field.metadata(), true);
    let mut table = arrow_ipc::FieldBuilder::new(builder);
    table.add_name(name);
    table.add_nullable(field.is_nullable());
    table.add_type_type(type_type);
    table.add_type_(ty);
    table.add_children(children);
    table.add_custom_metadata(metadata);
    Ok(table.finish())
}

/// The type table the reference rebuilds from the one arrow-rs writes for each data type.
fn java_type<'b>(builder: &mut FlatBufferBuilder<'b>, field: &Field) -> Result<(arrow_ipc::Type, WIPOffset<UnionWIPOffset>)> {
    use arrow_ipc::Type;
    macro_rules! empty { ($type:ident, $builder:ident) => { (Type::$type, arrow_ipc::$builder::new(builder).finish().as_union_value()) } }
    let int = |builder: &mut FlatBufferBuilder<'b>, width: i32, signed: bool| {
        let mut t = arrow_ipc::IntBuilder::new(builder); t.add_bitWidth(width); t.add_is_signed(signed); (Type::Int, t.finish().as_union_value())
    };
    let float = |builder: &mut FlatBufferBuilder<'b>, precision: arrow_ipc::Precision| {
        let mut t = arrow_ipc::FloatingPointBuilder::new(builder); t.add_precision(precision); (Type::FloatingPoint, t.finish().as_union_value())
    };
    let unit = |unit: &TimeUnit| match unit {
        TimeUnit::Second => arrow_ipc::TimeUnit::SECOND,
        TimeUnit::Millisecond => arrow_ipc::TimeUnit::MILLISECOND,
        TimeUnit::Microsecond => arrow_ipc::TimeUnit::MICROSECOND,
        TimeUnit::Nanosecond => arrow_ipc::TimeUnit::NANOSECOND,
    };
    Ok(match field.data_type() {
        DataType::Null => empty!(Null, NullBuilder),
        DataType::Boolean => empty!(Bool, BoolBuilder),
        DataType::Int8 => int(builder, 8, true),
        DataType::Int16 => int(builder, 16, true),
        DataType::Int32 => int(builder, 32, true),
        DataType::Int64 => int(builder, 64, true),
        DataType::UInt8 => int(builder, 8, false),
        DataType::UInt16 => int(builder, 16, false),
        DataType::UInt32 => int(builder, 32, false),
        DataType::UInt64 => int(builder, 64, false),
        DataType::Float16 => float(builder, arrow_ipc::Precision::HALF),
        DataType::Float32 => float(builder, arrow_ipc::Precision::SINGLE),
        DataType::Float64 => float(builder, arrow_ipc::Precision::DOUBLE),
        DataType::Utf8 => empty!(Utf8, Utf8Builder),
        DataType::LargeUtf8 => empty!(LargeUtf8, LargeUtf8Builder),
        DataType::Binary => empty!(Binary, BinaryBuilder),
        DataType::LargeBinary => empty!(LargeBinary, LargeBinaryBuilder),
        DataType::FixedSizeBinary(width) => {
            let mut t = arrow_ipc::FixedSizeBinaryBuilder::new(builder); t.add_byteWidth(*width); (Type::FixedSizeBinary, t.finish().as_union_value())
        }
        DataType::Date32 | DataType::Date64 => {
            let unit = if matches!(field.data_type(), DataType::Date32) { arrow_ipc::DateUnit::DAY } else { arrow_ipc::DateUnit::MILLISECOND };
            let mut t = arrow_ipc::DateBuilder::new(builder); t.add_unit(unit); (Type::Date, t.finish().as_union_value())
        }
        DataType::Time32(time) | DataType::Time64(time) => {
            // arrow-rs derives the width from the unit, not from the variant.
            let width = if matches!(time, TimeUnit::Second | TimeUnit::Millisecond) { 32 } else { 64 };
            let mut t = arrow_ipc::TimeBuilder::new(builder); t.add_unit(unit(time)); t.add_bitWidth(width); (Type::Time, t.finish().as_union_value())
        }
        DataType::Timestamp(time, zone) => {
            let zone = zone.as_deref().filter(|zone| !zone.is_empty()).map(|zone| builder.create_string(zone));
            let mut t = arrow_ipc::TimestampBuilder::new(builder);
            t.add_unit(unit(time));
            if let Some(zone) = zone { t.add_timezone(zone); }
            (Type::Timestamp, t.finish().as_union_value())
        }
        DataType::Duration(time) => {
            let mut t = arrow_ipc::DurationBuilder::new(builder); t.add_unit(unit(time)); (Type::Duration, t.finish().as_union_value())
        }
        DataType::Interval(interval) => {
            let interval = match interval {
                IntervalUnit::YearMonth => arrow_ipc::IntervalUnit::YEAR_MONTH,
                IntervalUnit::DayTime => arrow_ipc::IntervalUnit::DAY_TIME,
                IntervalUnit::MonthDayNano => arrow_ipc::IntervalUnit::MONTH_DAY_NANO,
            };
            let mut t = arrow_ipc::IntervalBuilder::new(builder); t.add_unit(interval); (Type::Interval, t.finish().as_union_value())
        }
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..) | DataType::Decimal256(..) => {
            let parameters = decimal_field_parameters(field)?.ok_or_else(|| invalid("Arrow decimal field has no parameters"))?;
            let mut t = arrow_ipc::DecimalBuilder::new(builder);
            t.add_precision(parameters.precision);
            t.add_scale(parameters.scale);
            t.add_bitWidth(i32::from(parameters.bit_width));
            (Type::Decimal, t.finish().as_union_value())
        }
        DataType::List(_) => empty!(List, ListBuilder),
        DataType::LargeList(_) => empty!(LargeList, LargeListBuilder),
        DataType::Struct(_) => empty!(Struct_, Struct_Builder),
        DataType::FixedSizeList(_, size) => {
            let mut t = arrow_ipc::FixedSizeListBuilder::new(builder); t.add_listSize(*size); (Type::FixedSizeList, t.finish().as_union_value())
        }
        DataType::Map(_, sorted) => {
            let mut t = arrow_ipc::MapBuilder::new(builder); t.add_keysSorted(*sorted); (Type::Map, t.finish().as_union_value())
        }
        DataType::Union(fields, mode) => {
            // The reference's unions carry no type ids when they are the identity.
            let ids: Vec<i32> = fields.iter().map(|(id, _)| i32::from(id)).collect();
            let identity = ids.iter().enumerate().all(|(index, id)| usize::try_from(*id) == Ok(index));
            let ids = (!identity).then(|| builder.create_vector(&ids));
            let mut t = arrow_ipc::UnionBuilder::new(builder);
            t.add_mode(match mode { UnionMode::Sparse => arrow_ipc::UnionMode::Sparse, UnionMode::Dense => arrow_ipc::UnionMode::Dense });
            if let Some(ids) = ids { t.add_typeIds(ids); }
            (Type::Union, t.finish().as_union_value())
        }
        DataType::RunEndEncoded(ends, _) if matches!(ends.data_type(), DataType::Int16 | DataType::Int32 | DataType::Int64) => {
            empty!(RunEndEncoded, RunEndEncodedBuilder)
        }
        other => return Err(invalid(format!("Arrow type {other} has no native Java-layout encoding"))),
    })
}

/// One body buffer as the reference writes it, before its padding to eight bytes.
enum Part { Bytes(Buffer), Empty, AllValid { length: usize, last_byte: u8 } }

impl Part {
    fn len(&self) -> usize {
        match self { Part::Bytes(bytes) => bytes.len(), Part::Empty => 0, Part::AllValid { length, .. } => *length }
    }
}

/// A batch's nodes and body buffers, reused across a stream's pages.
#[derive(Default)]
struct Body { nodes: Vec<arrow_ipc::FieldNode>, buffers: Vec<arrow_ipc::Buffer>, parts: Vec<Part>, length: i64 }

impl Body {
    fn clear(&mut self) {
        self.nodes.clear();
        self.buffers.clear();
        self.parts.clear();
        self.length = 0;
    }

    fn push(&mut self, part: Part) -> Result<()> {
        let written = part.len();
        let length = i64::try_from(written).map_err(invalid)?;
        let padded = i64::try_from(written.next_multiple_of(8)).map_err(invalid)?;
        self.buffers.push(arrow_ipc::Buffer::new(self.length, length));
        self.length = self.length.checked_add(padded).ok_or_else(|| invalid("Arrow body length exceeds the format"))?;
        self.parts.push(part);
        Ok(())
    }

    /// arrow-rs's `write_array_data` for `data`, with each buffer as `JavaLayout` re-emits it.
    fn column(&mut self, field: &Field, data: &ArrayData, null_count: usize) -> Result<()> {
        let rows = data.len();
        let length = i64::try_from(rows).map_err(invalid)?;
        let nulls = match data.data_type() {
            DataType::Union(..) => -1,
            DataType::Null => length,
            DataType::RunEndEncoded(..) => 0,
            _ if field.is_nullable() => -1,
            _ => 0,
        };
        self.nodes.push(arrow_ipc::FieldNode::new(length, nulls));
        if !matches!(data.data_type(), DataType::Null | DataType::Union(..) | DataType::RunEndEncoded(..)) {
            // arrow-rs writes a bitmap only for a column with nulls; the reference keeps one only for nullable fields, always present.
            let part = match data.nulls() {
                _ if !field.is_nullable() => Part::Empty,
                Some(validity) if null_count > 0 => Part::Bytes(validity.inner().sliced()),
                _ if rows > 0 => Part::AllValid { length: rows.div_ceil(8), last_byte: match rows % 8 { 0 => 0xff, tail => (1u8 << tail) - 1 } },
                _ => Part::Empty,
            };
            self.push(part)?;
        }
        let fields = children(field.data_type());
        match data.data_type() {
            DataType::Utf8 | DataType::Binary => self.bytes::<i32>(data),
            DataType::LargeUtf8 | DataType::LargeBinary => self.bytes::<i64>(data),
            DataType::Boolean => self.push(Part::Bytes(data.buffers()[0].bit_slice(data.offset(), rows))),
            DataType::List(_) | DataType::Map(..) => self.list::<i32>(&fields, data),
            DataType::LargeList(_) => self.list::<i64>(&fields, data),
            DataType::FixedSizeList(_, size) => {
                let size = usize::try_from(*size).map_err(invalid)?;
                let child = data.child_data()[0].slice(data.offset() * size, rows * size);
                let field = fields.first().ok_or_else(|| invalid("Arrow list field has no child"))?;
                self.column(field, &child, child.null_count())
            }
            DataType::RunEndEncoded(..) => {
                let [ends_field, values_field] = fields.as_slice() else {
                    return Err(invalid("Arrow run-end column differs from its field's children"));
                };
                let (run_ends, values) = unslice_run_end(data)?;
                self.column(ends_field, &run_ends, run_ends.null_count())?;
                self.column(values_field, &values, values.null_count())
            }
            DataType::Struct(_) | DataType::Union(..) | DataType::Null => {
                for buffer in data.buffers() { self.push(Part::Bytes(buffer.clone()))?; }
                if fields.len() != data.child_data().len() { return Err(invalid("Arrow column differs from its field's children")); }
                for (field, child) in fields.into_iter().zip(data.child_data()) { self.column(field, child, child.null_count())?; }
                Ok(())
            }
            data_type => {
                let BufferSpec::FixedWidth { byte_width, .. } = arrow_data::layout(data_type).buffers[0] else {
                    return Err(invalid(format!("Arrow type {data_type} has no native Java-layout encoding")));
                };
                let buffer = &data.buffers()[0];
                let wanted = rows * byte_width;
                let values = if data.offset() != 0 || wanted < buffer.len() {
                    let start = data.offset() * byte_width;
                    buffer.slice_with_length(start, wanted.min(buffer.len() - start))
                } else {
                    buffer.clone()
                };
                self.push(Part::Bytes(values))
            }
        }
    }

    /// The reference writes no offsets for an empty column, where arrow-rs writes one zero offset.
    fn bytes<O: OffsetSizeTrait>(&mut self, data: &ArrayData) -> Result<()> {
        if data.is_empty() {
            self.push(Part::Empty)?;
            return self.push(Part::Empty);
        }
        let (offsets, start, length) = rebase::<O>(data);
        self.push(Part::Bytes(offsets))?;
        self.push(Part::Bytes(data.buffers()[1].slice_with_length(start, length)))
    }

    fn list<O: OffsetSizeTrait>(&mut self, fields: &[&Field], data: &ArrayData) -> Result<()> {
        let field = fields.first().ok_or_else(|| invalid("Arrow list field has no child"))?;
        let child = if data.is_empty() {
            self.push(Part::Empty)?;
            data.child_data()[0].slice(0, 0)
        } else {
            let (offsets, start, length) = rebase::<O>(data);
            self.push(Part::Bytes(offsets))?;
            data.child_data()[0].slice(start, length)
        };
        self.column(field, &child, child.null_count())
    }

    /// `ArrowRecordBatch.writeTo` over this body.
    fn message(&self, rows: usize) -> Result<Vec<u8>> {
        let mut builder = FlatBufferBuilder::new();
        let nodes = builder.create_vector(&self.nodes);
        let buffers = builder.create_vector(&self.buffers);
        let mut table = arrow_ipc::RecordBatchBuilder::new(&mut builder);
        table.add_length(i64::try_from(rows).map_err(invalid)?);
        table.add_nodes(nodes);
        table.add_buffers(buffers);
        let header = table.finish();
        Ok(message(builder, VERSION, MessageHeader::RecordBatch, header.as_union_value(), self.length))
    }

    fn write(&self, sink: &mut impl Write) -> io::Result<()> {
        for part in &self.parts {
            match part {
                Part::Bytes(bytes) => sink.write_all(bytes)?,
                Part::Empty => {}
                Part::AllValid { length, last_byte } => write_all_valid(sink, *length, *last_byte)?,
            }
            let written = part.len();
            sink.write_all(&[0; 8][..written.next_multiple_of(8) - written])?;
        }
        Ok(())
    }
}

/// arrow-rs's `reencode_offsets`: the column's offsets from zero, with the start and length of the values they cover.
fn rebase<O: OffsetSizeTrait>(data: &ArrayData) -> (Buffer, usize, usize) {
    let offsets = &data.buffers()[0];
    let slice = &offsets.typed_data::<O>()[data.offset()..data.offset() + data.len() + 1];
    let (first, last) = (slice[0], slice[slice.len() - 1]);
    let rebased = if first.as_usize() == 0 {
        let width = size_of::<O>();
        offsets.slice_with_length(data.offset() * width, (data.len() + 1) * width)
    } else {
        slice.iter().map(|offset| *offset - first).collect()
    };
    (rebased, first.as_usize(), last.as_usize() - first.as_usize())
}

/// Returns zero-offset `(run_ends, values)` children covering `data`'s logical slice.
fn unslice_run_end(data: &ArrayData) -> Result<(ArrayData, ArrayData)> {
    let [run_ends, values] = data.child_data() else {
        return Err(invalid("Arrow run-end column differs from its field's children"));
    };
    if data.is_empty() {
        return Ok((run_ends.slice(0, 0), values.slice(0, 0)));
    }
    match run_ends.data_type() {
        DataType::Int16 => unslice_runs::<Int16Type>(data, run_ends, values),
        DataType::Int32 => unslice_runs::<Int32Type>(data, run_ends, values),
        DataType::Int64 => unslice_runs::<Int64Type>(data, run_ends, values),
        other => Err(invalid(format!("Arrow run-end index type {other} is unsupported"))),
    }
}

fn unslice_runs<R: RunEndIndexType>(data: &ArrayData, run_ends: &ArrayData, values: &ArrayData) -> Result<(ArrayData, ArrayData)>
where
    R::Native: Ord,
{
    let buffer = run_ends.buffers().first().ok_or_else(|| invalid("Arrow run-ends child has no buffer"))?;
    let all = buffer.typed_data::<R::Native>()
        .get(run_ends.offset()..run_ends.offset() + run_ends.len())
        .ok_or_else(|| invalid("Arrow run-ends buffer is shorter than its child"))?;
    let max_value = all.last().copied().ok_or_else(|| invalid("Non-empty run-end column has no runs"))?.to_usize();
    if data.offset() == 0 && max_value == Some(data.len()) {
        return Ok((run_ends.clone(), values.clone()));
    }
    let logical_offset = R::Native::from_usize(data.offset()).ok_or_else(|| invalid("Arrow run-end offset exceeds index width"))?;
    let logical_end = data.offset().checked_add(data.len())
        .and_then(R::Native::from_usize)
        .ok_or_else(|| invalid("Arrow run-end slice exceeds index width"))?;
    let start = all.partition_point(|&end| end <= logical_offset);
    let end = all.partition_point(|&end| end < logical_end);
    if start > end || end >= all.len() || end >= values.len() {
        return Err(invalid("Arrow run-end slice exceeds its physical runs"));
    }
    let physical = end - start + 1;
    let last = R::Native::from_usize(data.len()).ok_or_else(|| invalid("Arrow run-end length exceeds index width"))?;
    let mut rebased = Vec::with_capacity(physical);
    for &value in &all[start..end] {
        rebased.push(value.sub_wrapping(logical_offset));
    }
    rebased.push(last);
    let new_ends = ArrayData::builder(R::DATA_TYPE)
        .len(physical)
        .add_buffer(Buffer::from_vec(rebased))
        .build()
        .map_err(invalid)?;
    Ok((new_ends, values.slice(start, physical)))
}

#[cfg(test)]
#[path = "java_stream_tests.rs"]
mod tests;
