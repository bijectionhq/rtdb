use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
    ops::Range,
    sync::Arc,
    time::Duration,
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray,
    GenericByteArray, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray,
    NullArray, OffsetSizeTrait, PrimitiveArray, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray, UInt64Array, downcast_primitive,
    types::{
        ArrowPrimitiveType, BinaryType, ByteArrayType, LargeBinaryType, LargeUtf8Type, Utf8Type,
    },
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer, ToByteSlice};
use arrow_flight::FlightData;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit as ArrowTime};
use prost::Message;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::ipc::{PreparedStreamRow, RowStreamEncoder};
use rtdb_node::serving::{ChangeImages, ChangeKind, ChangeOrder, Changes};
use rtdb_node::{Database, PreparedSql, ResultSignaturePolicy};
use rtdb_query::{QueryContext, QueryCursor};
use rtdb_types::{StructFields, TableRef, TimeUnit, Timestamp, Value, rtdb_v1::Basis};
use tonic::{
    Response, Status,
    metadata::{MetadataMap, MetadataValue},
};

use crate::{
    service::{Call, Output, Service},
    wire,
};

use super::{
    MAX_CHANGE_GROUPS, MAX_WAIT, error, head, proto,
    tokens::{self, cursor_expired},
    trailer,
};

pub(super) const PROGRESS_ONLY: &str = "changes.progress-only";
pub(super) const SHARED_IMAGES: &str = "changes.images.stream-v1";
pub(super) const APPLIED_POSITIONS: &str = "changes.applied-positions";
const IMAGE_HEADER: &str = "x-rtdb-changes-images";
const IMAGE_STREAM: &str = "stream-v1";
const IMAGE_END: [u8; 4] = [0; 4];
const MAX_PACKED_BYTES: usize = 1 << 20;
const MAX_DEFERRED_IMAGE_FIELDS: usize = 4;
const MAX_DEFERRED_IMAGE_NODES: usize = 8;
const MAX_DEFERRED_IMAGE_BYTES: usize = 1024;
const MAX_DEFERRED_IMAGE_SCRATCH: usize = 24 << 10;
const MAX_DEFERRED_VARIABLE_BYTES: usize = 256;
const MAX_DEFERRED_CHILD_ROWS: usize = u8::BITS as usize;
const MAX_INLINE_PRIMITIVE_BYTES: usize = 32;
const MIN_DEFERRED_IMAGE_ROW_BOUND: usize =
    MAX_DEFERRED_IMAGE_SCRATCH + 8 * MAX_DEFERRED_IMAGE_BYTES;

struct Request {
    tables: BTreeSet<TableRef>,
    after: Timestamp,
    until: Option<Timestamp>,
    descending: bool,
    images: proto::Images,
    progress_only: bool,
    shared_images: bool,
    limit: usize,
    wait: Duration,
}

struct Page {
    frames: Vec<(usize, Vec<FlightData>)>,
    readers: Vec<QueryCursor>,
    has_changes: bool,
    through: Timestamp,
    complete: bool,
}

struct ReservedRow {
    table: Arc<str>,
    time: Timestamp,
    opened: bool,
    images: [Option<Image>; 3],
    charge: Arc<MemoryReservation>,
}

impl ReservedRow {
    fn finish_images(&mut self) -> Result<(), Status> {
        for image in &mut self.images {
            if let Some(Image::Deferred(deferred)) = image {
                *image = Some(deferred.encode()?);
            }
        }
        Ok(())
    }

    fn encode(
        self,
        schema: &SchemaRef,
        encoder: &mut wire::Encoder,
    ) -> Result<Vec<FlightData>, Status> {
        let Self {
            table,
            time,
            opened,
            images: [key, before, after],
            charge,
        } = self;
        let images = [
            singleton_image_array(key).map_err(batch_error)?,
            singleton_image_array(before).map_err(batch_error)?,
            singleton_image_array(after).map_err(batch_error)?,
        ];
        let (values_capacity, values_len) = images
            .iter()
            .try_fold((0, 0), |(capacity, length), image| {
                let values = image.values();
                Ok((
                    wire::add(capacity, values.capacity())?,
                    wire::add(length, values.len())?,
                ))
            })
            .map_err(error::map_status)?;
        let batch =
            outer_row(Arc::clone(schema), &table, time, opened, images).map_err(batch_error)?;
        let bound = (|| {
            // Arrow's uncompressed IPC writer grows its body Vec from the used value bytes.
            let arrays = wire::mul(batch.get_array_memory_size() - values_capacity, 4)?;
            let values = wire::add(values_capacity, wire::mul(values_len, 2)?)?;
            wire::add(wire::schema_bound(schema)?, wire::add(arrays, values)?)
        })()
        .map_err(error::map_status)?;
        encoder
            .reserved_singleton(batch, charge, bound)
            .map_err(error::map_status)
    }

    fn shares_streams_with(&self, other: &Self) -> bool {
        self.images
            .iter()
            .zip(&other.images)
            .all(|(left, right)| match (left, right) {
                (None, None) => true,
                (Some(left), Some(right)) => left.shares_schema_with(right),
                _ => false,
            })
    }
}

enum Image {
    Binary(Vec<u8>),
    Shared {
        stream: Arc<ImageStream>,
        row: PreparedStreamRow,
    },
    Deferred(Box<DeferredImage>),
}

impl Image {
    fn encode(batch: &RecordBatch, stream: Option<&Arc<ImageStream>>) -> Result<Self, Status> {
        match stream {
            Some(stream) => Ok(Self::Shared {
                stream: Arc::clone(stream),
                row: stream.encoder.prepare(batch).map_err(error::anomaly)?,
            }),
            None => rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(batch))
                .map(Self::Binary)
                .map_err(error::anomaly),
        }
    }

    fn legacy_len(&self) -> usize {
        match self {
            Self::Binary(bytes) => bytes.len(),
            Self::Shared { stream, row } => {
                stream.encoder.schema_frame().len() + row.framed_len() + IMAGE_END.len()
            }
            Self::Deferred(image) => image.legacy_len,
        }
    }

    fn shares_schema_with(&self, other: &Self) -> bool {
        let (Self::Shared { stream: left, .. }, Self::Shared { stream: right, .. }) = (self, other)
        else {
            return false;
        };
        !left.encoder.has_dictionaries() && !right.encoder.has_dictionaries()
            && (Arc::ptr_eq(left, right)
                || left.encoder.schema_frame() == right.encoder.schema_frame())
    }
}

type ImageOffsets = [u16; MAX_DEFERRED_CHILD_ROWS + 1];

#[derive(Clone, Copy)]
enum ImageValue<B> {
    InlinePrimitive([u8; MAX_INLINE_PRIMITIVE_BYTES]),
    PrimitiveBytes(B),
    Boolean(u8),
    Bytes(B, ImageOffsets),
    FixedBinary(B),
    Null,
    Struct,
    OffsetList(ImageOffsets),
    FixedList,
}

#[derive(Clone, Copy)]
struct ImageCell<B> {
    value: ImageValue<B>,
    len: usize,
    null_byte: Option<u8>,
}

struct ImageCandidate<'a> {
    cells: [ImageCell<&'a [u8]>; MAX_DEFERRED_IMAGE_NODES],
    len: usize,
    scratch: usize,
    body_bytes: usize,
    padded_value_bytes: usize,
}

impl<'a> ImageCandidate<'a> {
    fn new(batch: &'a RecordBatch, row: usize, schema_bound: usize) -> Option<Self> {
        let schema = batch.schema_ref();
        if !(1..=MAX_DEFERRED_IMAGE_FIELDS).contains(&batch.num_columns())
            || schema_bound > MAX_DEFERRED_IMAGE_SCRATCH
        {
            return None;
        }
        let mut candidate = Self {
            cells: [ImageCell {
                value: ImageValue::Null,
                len: 0,
                null_byte: None,
            }; MAX_DEFERRED_IMAGE_NODES],
            len: 0,
            scratch: schema_bound,
            body_bytes: 0,
            padded_value_bytes: 0,
        };
        for (field, array) in schema.fields().iter().zip(batch.columns()) {
            candidate.capture(field, array.as_ref(), row..row + 1, u8::MAX)?;
        }
        Some(candidate)
    }

    fn capture(
        &mut self,
        field: &Field,
        array: &'a dyn Array,
        rows: Range<usize>,
        parent_validity: u8,
    ) -> Option<()> {
        let len = rows.len();
        if self.len == self.cells.len()
            || len > MAX_DEFERRED_CHILD_ROWS
            || rows.end > array.len()
            || array.data_type() != field.data_type()
        {
            return None;
        }
        let mask = ((1_u16 << len) - 1) as u8;
        let null_byte = array
            .nulls()
            .map(|nulls| image_bit_byte(nulls.inner(), rows.clone()))
            .filter(|byte| byte & mask != mask);
        let validity = if matches!(field.data_type(), DataType::Null) {
            0
        } else {
            null_byte.unwrap_or(mask)
        };
        if !field.is_nullable() && parent_validity & !validity & mask != 0 {
            return None;
        }

        let index = self.len;
        self.len += 1;
        let (value, array_bytes, offset_width) = match field.data_type() {
            DataType::Null => {
                array.as_any().downcast_ref::<NullArray>()?;
                (ImageValue::Null, size_of::<NullArray>(), 0)
            }
            DataType::Boolean => {
                let array = array.as_any().downcast_ref::<BooleanArray>()?;
                (
                    ImageValue::Boolean(image_bit_byte(array.values(), rows)),
                    size_of::<BooleanArray>(),
                    0,
                )
            }
            DataType::Utf8 => Self::bytes::<Utf8Type>(array, rows)?,
            DataType::Binary => Self::bytes::<BinaryType>(array, rows)?,
            DataType::LargeUtf8 => Self::bytes::<LargeUtf8Type>(array, rows)?,
            DataType::LargeBinary => Self::bytes::<LargeBinaryType>(array, rows)?,
            DataType::FixedSizeBinary(width) if *width > 0 => {
                let width = *width as usize;
                if width > MAX_DEFERRED_VARIABLE_BYTES {
                    return None;
                }
                let array = array.as_any().downcast_ref::<FixedSizeBinaryArray>()?;
                let bytes = array
                    .value_data()
                    .get(rows.start.checked_mul(width)?..rows.end.checked_mul(width)?)?;
                (
                    ImageValue::FixedBinary(bytes),
                    size_of::<FixedSizeBinaryArray>(),
                    0,
                )
            }
            DataType::Struct(fields) => {
                let children = array.as_any().downcast_ref::<StructArray>()?.columns();
                if children.len() != fields.len() {
                    return None;
                }
                for (field, child) in fields.iter().zip(children) {
                    self.capture(field, child.as_ref(), rows.clone(), validity)?;
                }
                (
                    ImageValue::Struct,
                    size_of::<StructArray>() + children.len() * size_of::<ArrayRef>(),
                    0,
                )
            }
            DataType::List(field) => {
                let array = array.as_any().downcast_ref::<ListArray>()?;
                let (offsets, child_rows) = rebase_image_offsets(array.value_offsets(), rows)?;
                self.capture(field, array.values().as_ref(), child_rows, u8::MAX)?;
                (ImageValue::OffsetList(offsets), size_of::<ListArray>(), 4)
            }
            DataType::LargeList(field) => {
                let array = array.as_any().downcast_ref::<LargeListArray>()?;
                let (offsets, child_rows) = rebase_image_offsets(array.value_offsets(), rows)?;
                self.capture(field, array.values().as_ref(), child_rows, u8::MAX)?;
                (
                    ImageValue::OffsetList(offsets),
                    size_of::<LargeListArray>(),
                    8,
                )
            }
            DataType::FixedSizeList(field, width) => {
                let array = array.as_any().downcast_ref::<FixedSizeListArray>()?;
                let width = usize::try_from(*width).ok()?;
                let child_rows = rows.start.checked_mul(width)?..rows.end.checked_mul(width)?;
                if child_rows.len() > MAX_DEFERRED_CHILD_ROWS {
                    return None;
                }
                let child_validity = (0..child_rows.len()).fold(0, |mask, index| {
                    mask | ((validity >> (index / width) & 1) << index)
                });
                self.capture(field, array.values().as_ref(), child_rows, child_validity)?;
                (ImageValue::FixedList, size_of::<FixedSizeListArray>(), 0)
            }
            DataType::Map(field, _) if !field.is_nullable() => {
                let array = array.as_any().downcast_ref::<MapArray>()?;
                let entries = array.entries();
                if entries.num_columns() != 2 {
                    return None;
                }
                let (offsets, child_rows) = rebase_image_offsets(array.value_offsets(), rows)?;
                self.capture(field, entries, child_rows, u8::MAX)?;
                (ImageValue::OffsetList(offsets), size_of::<MapArray>(), 4)
            }
            data_type => {
                macro_rules! capture_primitive {
                    ($ty:ty) => {
                        Self::primitive::<$ty>(array, rows)?
                    };
                }
                downcast_primitive!(data_type => (capture_primitive), _ => return None)
            }
        };
        let (value_bytes, compact_value_bytes) = match value {
            ImageValue::InlinePrimitive(_) => {
                let bytes = len * field.data_type().primitive_width()?;
                (bytes, bytes.next_multiple_of(64))
            }
            ImageValue::PrimitiveBytes(bytes) => (bytes.len(), bytes.len().next_multiple_of(64)),
            ImageValue::Boolean(_) => (usize::from(len != 0), 1),
            ImageValue::Bytes(bytes, _) => {
                self.padded_value_bytes += bytes.len().next_multiple_of(8);
                (bytes.len(), bytes.len())
            }
            ImageValue::FixedBinary(bytes) => (bytes.len(), bytes.len()),
            _ => (0, 0),
        };
        let offset_bytes = offset_width * (len + 1);
        self.body_bytes += value_bytes.next_multiple_of(8)
            + offset_bytes.next_multiple_of(8)
            + 8 * usize::from(null_byte.is_some());
        self.scratch += 4
            * (array_bytes + compact_value_bytes + offset_bytes + usize::from(null_byte.is_some()));
        self.cells[index] = ImageCell {
            value,
            len,
            null_byte,
        };
        (self.scratch <= MAX_DEFERRED_IMAGE_SCRATCH
            && self.body_bytes <= MAX_DEFERRED_IMAGE_BYTES
            && self.padded_value_bytes <= MAX_DEFERRED_VARIABLE_BYTES)
            .then_some(())
    }

    fn bytes<T: ByteArrayType>(
        array: &'a dyn Array,
        rows: Range<usize>,
    ) -> Option<(ImageValue<&'a [u8]>, usize, usize)> {
        let array = array.as_any().downcast_ref::<GenericByteArray<T>>()?;
        let (offsets, values) = rebase_image_offsets(array.value_offsets(), rows)?;
        let bytes = array.value_data().get(values)?;
        Some((
            ImageValue::Bytes(bytes, offsets),
            size_of::<GenericByteArray<T>>(),
            size_of::<T::Offset>(),
        ))
    }

    fn primitive<T: ArrowPrimitiveType>(
        array: &'a dyn Array,
        rows: Range<usize>,
    ) -> Option<(ImageValue<&'a [u8]>, usize, usize)> {
        let array = array.as_any().downcast_ref::<PrimitiveArray<T>>()?;
        let bytes = array.values().get(rows)?.to_byte_slice();
        let mut inline = [0; MAX_INLINE_PRIMITIVE_BYTES];
        let value = if let Some(target) = inline.get_mut(..bytes.len()) {
            target.copy_from_slice(bytes);
            ImageValue::InlinePrimitive(inline)
        } else {
            ImageValue::PrimitiveBytes(bytes)
        };
        Some((value, size_of::<PrimitiveArray<T>>(), 0))
    }
}

fn rebase_image_offsets<O: OffsetSizeTrait>(
    offsets: &[O],
    rows: Range<usize>,
) -> Option<(ImageOffsets, Range<usize>)> {
    let mut rebased = [0; MAX_DEFERRED_CHILD_ROWS + 1];
    if rows.is_empty() {
        return Some((rebased, 0..0));
    }
    let offsets = offsets.get(rows.start..=rows.end)?;
    let start = offsets.first()?.as_usize();
    let end = offsets.last()?.as_usize();
    for (target, offset) in rebased.iter_mut().zip(offsets) {
        *target = u16::try_from(offset.as_usize().checked_sub(start)?).ok()?;
    }
    Some((rebased, start..end))
}

fn image_bit_byte(bits: &BooleanBuffer, rows: Range<usize>) -> u8 {
    if rows.is_empty() {
        return 0;
    }
    let offset = bits.offset() + rows.start;
    // Arrow writes the entire source byte for byte-aligned slices and shifts and masks other slices.
    if offset.is_multiple_of(8) {
        bits.inner()[offset / 8]
    } else {
        rows.enumerate()
            .fold(0, |byte, (i, row)| byte | (u8::from(bits.value(row)) << i))
    }
}

fn image_offsets<O: OffsetSizeTrait>(offsets: &ImageOffsets, len: usize) -> OffsetBuffer<O> {
    OffsetBuffer::new(
        offsets[..=len]
            .iter()
            .map(|&offset| O::usize_as(offset.into()))
            .collect::<Vec<_>>()
            .into(),
    )
}

struct DeferredImage {
    schema: SchemaRef,
    cells: [ImageCell<Buffer>; MAX_DEFERRED_IMAGE_NODES],
    stream: Option<Arc<ImageStream>>,
    legacy_len: usize,
}

impl DeferredImage {
    fn encode(&self) -> Result<Image, Status> {
        let mut cells = &self.cells[..];
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|field| Self::column(field, &mut cells))
            .collect::<Result<Vec<_>, _>>()
            .map_err(batch_error)?;
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(batch_error)?;
        Image::encode(&batch, self.stream.as_ref())
    }

    fn column(field: &Field, cells: &mut &[ImageCell<Buffer>]) -> Result<ArrayRef, ArrowError> {
        let (cell, remaining) = cells
            .split_first()
            .expect("one deferred image cell per field");
        *cells = remaining;
        let len = cell.len;
        let nulls = cell
            .null_byte
            .and_then(|byte| NullBuffer::from_unsliced_buffer(Buffer::from_vec(vec![byte]), len));
        let array: ArrayRef = match (field.data_type(), &cell.value) {
            (data_type, ImageValue::InlinePrimitive(_) | ImageValue::PrimitiveBytes(_)) => {
                let buffer = match &cell.value {
                    ImageValue::InlinePrimitive(bytes) => {
                        let width = data_type
                            .primitive_width()
                            .expect("deferred primitive width");
                        Buffer::from_slice_ref(&bytes[..len * width])
                    }
                    ImageValue::PrimitiveBytes(bytes) => bytes.clone(),
                    _ => unreachable!(),
                };
                macro_rules! primitive_column {
                    ($ty:ty) => {{
                        let column: ArrayRef = Arc::new(
                            PrimitiveArray::<$ty>::new(ScalarBuffer::new(buffer, 0, len), nulls)
                                .with_data_type(data_type.clone()),
                        );
                        column
                    }};
                }
                downcast_primitive!(data_type => (primitive_column), _ => unreachable!("deferred primitive type"))
            }
            (DataType::Boolean, ImageValue::Boolean(byte)) => {
                let values = BooleanBuffer::new(Buffer::from_vec(vec![*byte]), 0, len);
                Arc::new(BooleanArray::new(values, nulls))
            }
            (DataType::Utf8, ImageValue::Bytes(bytes, offsets)) => Arc::new(StringArray::try_new(
                image_offsets(offsets, len),
                bytes.clone(),
                nulls,
            )?),
            (DataType::Binary, ImageValue::Bytes(bytes, offsets)) => Arc::new(
                BinaryArray::try_new(image_offsets(offsets, len), bytes.clone(), nulls)?,
            ),
            (DataType::LargeUtf8, ImageValue::Bytes(bytes, offsets)) => Arc::new(
                LargeStringArray::try_new(image_offsets(offsets, len), bytes.clone(), nulls)?,
            ),
            (DataType::LargeBinary, ImageValue::Bytes(bytes, offsets)) => Arc::new(
                LargeBinaryArray::try_new(image_offsets(offsets, len), bytes.clone(), nulls)?,
            ),
            (DataType::FixedSizeBinary(width), ImageValue::FixedBinary(bytes)) => {
                Arc::new(FixedSizeBinaryArray::try_new(*width, bytes.clone(), nulls)?)
            }
            (DataType::Null, ImageValue::Null) => Arc::new(NullArray::new(len)),
            (DataType::Struct(fields), ImageValue::Struct) => {
                let children = fields
                    .iter()
                    .map(|field| Self::column(field, cells))
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(StructArray::try_new_with_length(
                    fields.clone(),
                    children,
                    nulls,
                    len,
                )?)
            }
            (DataType::List(child), ImageValue::OffsetList(offsets)) => {
                Arc::new(ListArray::try_new(
                    child.clone(),
                    image_offsets(offsets, len),
                    Self::column(child, cells)?,
                    nulls,
                )?)
            }
            (DataType::LargeList(child), ImageValue::OffsetList(offsets)) => {
                Arc::new(LargeListArray::try_new(
                    child.clone(),
                    image_offsets(offsets, len),
                    Self::column(child, cells)?,
                    nulls,
                )?)
            }
            (DataType::FixedSizeList(child, width), ImageValue::FixedList) => {
                Arc::new(FixedSizeListArray::try_new_with_length(
                    child.clone(),
                    *width,
                    Self::column(child, cells)?,
                    nulls,
                    len,
                )?)
            }
            (DataType::Map(child, sorted), ImageValue::OffsetList(offsets)) => {
                let entries = Self::column(child, cells)?;
                let entries = entries
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("deferred map entries");
                Arc::new(MapArray::try_new(
                    child.clone(),
                    image_offsets(offsets, len),
                    entries.clone(),
                    nulls,
                    *sorted,
                )?)
            }
            _ => unreachable!("deferred image cell differs from its field"),
        };
        Ok(array)
    }
}

struct ImageStream {
    encoder: RowStreamEncoder,
    _charge: Arc<MemoryReservation>,
}

enum ImageEncoding {
    Binary,
    Stream(Option<Arc<ImageStream>>),
}

#[derive(Default)]
struct ImageLayout {
    schema: Option<SchemaRef>,
    base_wire_lengths: [Option<usize>; 2],
    shared_stream: Option<Arc<ImageStream>>,
}

impl ImageLayout {
    fn base_wire_len(&mut self, schema: &SchemaRef, has_body: bool) -> &mut Option<usize> {
        if self.schema.as_ref() != Some(schema) {
            self.schema = Some(Arc::clone(schema));
            self.base_wire_lengths = [None; 2];
        }
        &mut self.base_wire_lengths[usize::from(has_body)]
    }

    fn stream(
        &mut self,
        schema: &SchemaRef,
        scratch: &mut Arc<MemoryReservation>,
    ) -> Result<Arc<ImageStream>, Status> {
        if let Some(stream) = &self.shared_stream
            && stream.encoder.schema_ref() == schema
        {
            return Ok(Arc::clone(stream));
        }

        let encoder = RowStreamEncoder::new(schema).map_err(error::anomaly)?;
        let retained = wire::add(
            encoder.retained_bytes().map_err(error::anomaly)?,
            2 * size_of::<usize>() + size_of::<Arc<MemoryReservation>>(),
        )
        .map_err(error::map_status)?;
        let charge = Arc::get_mut(scratch)
            .expect("image preparation owns its scratch reservation")
            .split_off(retained)
            .map_err(error::anomaly)?;
        let stream = Arc::new(ImageStream {
            encoder,
            _charge: charge,
        });
        // Arrow schema equality omits the dictionary ordering flag.
        self.shared_stream = (!stream.encoder.has_dictionaries()).then(|| Arc::clone(&stream));
        Ok(stream)
    }
}

struct ImageProjection<'a> {
    source: &'a RecordBatch,
    columns: &'a [usize],
    projected: Option<RecordBatch>,
    array_bytes: Option<usize>,
    schema_bound: Option<usize>,
    layout: &'a mut ImageLayout,
    encoding: ImageEncoding,
}

impl<'a> ImageProjection<'a> {
    fn new(
        source: &'a RecordBatch,
        columns: &'a [usize],
        shared_images: bool,
        layout: &'a mut ImageLayout,
    ) -> Self {
        Self {
            source,
            columns,
            projected: None,
            array_bytes: None,
            schema_bound: None,
            layout,
            encoding: if shared_images {
                ImageEncoding::Stream(None)
            } else {
                ImageEncoding::Binary
            },
        }
    }

    fn prepare(
        &mut self,
        row: usize,
        memory: &Arc<MemoryBudget>,
    ) -> Result<(Image, Arc<MemoryReservation>), Status> {
        let projected = match &mut self.projected {
            Some(projected) => projected,
            None => self
                .projected
                .insert(self.source.project(self.columns).map_err(batch_error)?),
        };
        let schema = projected.schema_ref();
        let schema_bound = match self.schema_bound {
            Some(bound) => bound,
            None => {
                let bound = wire::schema_bound(schema).map_err(error::map_status)?;
                self.schema_bound = Some(bound);
                bound
            }
        };
        let candidate = ImageCandidate::new(projected, row, schema_bound);
        let mut slice = None;
        let array_bytes = if candidate.is_some() {
            // Supported slices keep every backing capacity, even for a row with no nulls.
            *self
                .array_bytes
                .get_or_insert_with(|| projected.get_array_memory_size())
        } else {
            slice
                .insert(projected.slice(row, 1))
                .get_array_memory_size()
        };
        let arrays = wire::mul(array_bytes, 4).map_err(error::map_status)?;
        let size = wire::add(arrays, schema_bound).map_err(error::map_status)?;
        let mut charge = memory.reserve(size).map_err(error::anomaly)?;
        let stream = match &mut self.encoding {
            ImageEncoding::Binary => None,
            ImageEncoding::Stream(encoder) => {
                let encoder = match encoder {
                    Some(encoder) => encoder,
                    None => encoder.insert(self.layout.stream(schema, &mut charge)?),
                };
                Some(&*encoder)
            }
        };
        if let Some(candidate) = candidate {
            let body_bytes = candidate.body_bytes;
            // A zero-length IPC body omits the FlatBuffer body-length slot.
            let base = self.layout.base_wire_len(schema, body_bytes != 0);
            if let Some(legacy_len) = (*base).and_then(|len| len.checked_add(body_bytes))
                && legacy_len <= MAX_DEFERRED_IMAGE_BYTES
            {
                return Ok((
                    Image::Deferred(Box::new(DeferredImage {
                        schema: Arc::clone(schema),
                        cells: candidate.cells.map(|cell| ImageCell {
                            len: cell.len,
                            null_byte: cell.null_byte,
                            value: match cell.value {
                                ImageValue::InlinePrimitive(bytes) => {
                                    ImageValue::InlinePrimitive(bytes)
                                }
                                ImageValue::PrimitiveBytes(bytes) => {
                                    ImageValue::PrimitiveBytes(Buffer::from_slice_ref(bytes))
                                }
                                ImageValue::Boolean(byte) => ImageValue::Boolean(byte),
                                ImageValue::Bytes(bytes, offsets) => {
                                    ImageValue::Bytes(Buffer::from_vec(bytes.to_vec()), offsets)
                                }
                                ImageValue::FixedBinary(bytes) => {
                                    ImageValue::FixedBinary(Buffer::from_vec(bytes.to_vec()))
                                }
                                ImageValue::Null => ImageValue::Null,
                                ImageValue::Struct => ImageValue::Struct,
                                ImageValue::OffsetList(offsets) => ImageValue::OffsetList(offsets),
                                ImageValue::FixedList => ImageValue::FixedList,
                            },
                        }),
                        stream: stream.cloned(),
                        legacy_len,
                    })),
                    charge,
                ));
            }
            let image = Image::encode(&projected.slice(row, 1), stream)?;
            // Empty children still emit their field nodes and buffer slots.
            if base.is_none() {
                *base = image.legacy_len().checked_sub(body_bytes);
            }
            return Ok((image, charge));
        }
        let image = Image::encode(slice.as_ref().expect("eager image slice"), stream)?;
        Ok((image, charge))
    }
}

#[derive(Clone, Copy)]
enum Boundary {
    Opened,
    Closed,
}

impl Boundary {
    fn column(self) -> &'static str {
        match self {
            Self::Opened => "_system_from",
            Self::Closed => "_system_to",
        }
    }
}

impl Service {
    pub(super) async fn v1_changes(
        &self,
        call: Call,
        request: proto::ChangesRequest,
        metadata: &MetadataMap,
    ) -> Result<Response<Output<FlightData>>, Status> {
        let offered = metadata.get_all(IMAGE_HEADER);
        let mut offered = offered.iter();
        let shared_images = !request.progress_only
            && request.images != proto::Images::None as i32
            && offered.next().is_some_and(|value| value == IMAGE_STREAM)
            && offered.next().is_none();
        let database = self
            .wire_database(&call, &request.branch, proto::Layer::Merged)
            .await
            .map_err(|status| call.error(status))?;
        if database.serving_driver().is_some() {
            return self.applied_changes(call, database, request).await;
        }
        if request.observe_applied_positions {
            return Err(call.error(error::unsupported(APPLIED_POSITIONS)));
        }
        if request.custody_release_evidence {
            return Err(call.error(error::unsupported(super::custody::SCOPED_CUSTODY)));
        }
        let (database, page) = self
            .open_change_page(&call, database, request, shared_images)
            .await
            .map_err(|status| call.error(status))?;
        let scope = database.wire_incarnation().await.map_err(error::anomaly)?;
        let through = tokens::basis(scope, page.through).into();
        let service = self.clone();
        let schema = schema();
        let first = page.readers[0]
            .delivery_grant()
            .await
            .map_err(error::anomaly)?;
        let (mut headers, cutoff) = call
            .delivery_headers(&first, true)
            .map_err(error::map_status)?;
        if shared_images {
            headers.insert(IMAGE_HEADER, MetadataValue::from_static(IMAGE_STREAM));
        }
        let stream: Output<FlightData> = Box::pin(async_stream::try_stream! {
            service.check_attachment(&call, &database)?;
            call.handoff(first, cutoff).map_err(error::map_status)?;
            yield wire::schema_data(&schema, call.memory()).map_err(error::map_status)?;
            for (reader, frames) in page.frames {
                for frame in frames {
                    service.check_attachment(&call, &database)?;
                    call.handoff(page.readers[reader].delivery_grant().await.map_err(error::anomaly)?, cutoff).map_err(error::map_status)?;
                    yield frame;
                }
            }
            service.check_attachment(&call, &database)?;
            for reader in page.readers {
                call.handoff(reader.delivery_grant().await.map_err(error::anomaly)?, cutoff).map_err(error::map_status)?;
            }
            yield trailer(proto::ChangesTrailer { through: Some(through), complete: page.complete, custody_release_proof: None });
        });
        let mut response = Response::new(stream);
        *response.metadata_mut() = headers;
        Ok(response)
    }

    async fn open_change_page(
        &self,
        call: &Call,
        database: Arc<Database>,
        request: proto::ChangesRequest,
        shared_images: bool,
    ) -> Result<(Arc<Database>, Page), Status> {
        let scope = database.wire_incarnation().await.map_err(error::anomaly)?;
        let tables = tables(&request.tables)?;
        let read_basis = |basis| tokens::read_basis(scope, basis).map_err(|_| cursor_expired());
        let after = request
            .after
            .as_ref()
            .map(read_basis)
            .transpose()?
            .unwrap_or(Timestamp::MIN);
        let until = request.until.as_ref().map(read_basis).transpose()?;
        let wait = Duration::from_millis(request.wait_millis.unwrap_or_default());
        let order = proto::Order::try_from(request.order)
            .map_err(|_| error::incorrect("Unknown change order"))?;
        let images = proto::Images::try_from(request.images)
            .map_err(|_| error::incorrect("Unknown change images"))?;
        if wait > MAX_WAIT
            || !(1..=MAX_CHANGE_GROUPS).contains(&request.limit)
            || until.is_some_and(|until| until < after)
        {
            return Err(error::incorrect("Invalid Changes interval, limit or wait"));
        }
        if request.progress_only && images != proto::Images::None {
            return Err(error::incorrect("Progress-only Changes requires IMAGES_NONE"));
        }
        let request = Request {
            tables,
            after,
            until,
            wait,
            images,
            progress_only: request.progress_only,
            shared_images,
            descending: order == proto::Order::Descending,
            limit: request.limit as usize,
        };
        let pack_limit = self.0.config.maximum_message_bytes.min(MAX_PACKED_BYTES);
        let deadline = tokio::time::Instant::now() + request.wait;
        loop {
            let context = self.wire_context(call, &database, false).await?;
            let present = head(&context, &call.database)?;
            let catalog = context
                .catalog
                .database(&call.database)
                .map_err(error::anomaly)?;
            let snapshot = &catalog.partitions[0];
            let floor = snapshot
                .table_catalog()
                .block()
                .and_then(|block| block.latest_completed_tx())
                .map_or(Timestamp::MIN, |tx| tx.system_time);
            if request.after < floor {
                return Err(cursor_expired());
            }
            if request.after > present {
                return Err(error::unavailable(
                    "Database has not applied the Changes cursor",
                ));
            }
            let upper = request.until.unwrap_or(present).min(present);
            let page = change_page(context, &request, upper, call.memory(), pack_limit).await?;
            if page.has_changes
                || request.until.is_some_and(|until| until <= present)
                || tokio::time::Instant::now() >= deadline
            {
                return Ok((database, page));
            }
            let next = Timestamp(present.0.saturating_add(1));
            let update = tokio::time::timeout_at(
                deadline,
                database
                    .progress()
                    .await_system_time(next, &call.cancellation),
            )
            .await;
            match update {
                Ok(Ok(_)) => continue,
                Ok(Err(anomaly)) => return Err(error::anomaly(anomaly)),
                Err(_) => return Ok((database, page)),
            }
        }
    }

    async fn applied_changes(
        &self,
        call: Call,
        database: Arc<Database>,
        request: proto::ChangesRequest,
    ) -> Result<Response<Output<FlightData>>, Status> {
        let progress_only = request.progress_only;
        let observe_positions = request.observe_applied_positions;
        let (page, images, context, initial, headers, cutoff, custody_release_proof) = async {
            let driver = database.serving_driver().expect("checked serving driver");
            let mut visibility = self.serving_scope(&call, driver)?;
            if request.custody_release_evidence {
                self.authorize_scoped_custody(&call)?;
                visibility.as_mut().ok_or_else(|| error::unsupported(super::custody::SCOPED_CUSTODY))?
                    .custody_evidence = true;
            }
            if observe_positions && (
                !call.session.caller.verified_credential || visibility.is_some()
                    || call.session.fresh_replica
                    || !driver.supports_query_tokens() || !database.wire_tokens_supported()
            ) {
                return Err(error::unsupported(APPLIED_POSITIONS));
            }
            let incarnation = visibility.as_ref().map_or_else(|| driver.incarnation(), |scope| {
                scope.labels.main_read_incarnation(driver.incarnation())
            });
            let tables = tables(&request.tables)?;
            let read_basis = |basis: proto::Basis| {
                Basis::from_bytes(&basis.value)
                    .ok()
                    .filter(|basis| basis.incarnation() == incarnation)
                    .ok_or_else(cursor_expired)
            };
            let after = request.after.map(read_basis).transpose()?;
            let until = request.until.map(read_basis).transpose()?;
            let wait = Duration::from_millis(request.wait_millis.unwrap_or_default());
            if wait > MAX_WAIT
                || !(1..=MAX_CHANGE_GROUPS).contains(&request.limit)
                || after.zip(until).is_some_and(|(after, until)| until < after)
            {
                return Err(error::incorrect("Invalid Changes interval, limit or wait"));
            }
            let order = match proto::Order::try_from(request.order)
                .map_err(|_| error::incorrect("Unknown change order"))?
            {
                proto::Order::Ascending => ChangeOrder::Ascending,
                proto::Order::Descending => ChangeOrder::Descending,
            };
            let images = match proto::Images::try_from(request.images)
                .map_err(|_| error::incorrect("Unknown change images"))?
            {
                proto::Images::None => ChangeImages::None,
                proto::Images::Keys => ChangeImages::Keys,
                proto::Images::Before => ChangeImages::Before,
                proto::Images::After => ChangeImages::After,
                proto::Images::Both => ChangeImages::Both,
            };
            if progress_only && images != ChangeImages::None {
                return Err(error::incorrect("Progress-only Changes requires IMAGES_NONE"));
            }
            let change = Changes {
                        branch: request.branch,
                        tables,
                        after,
                        until,
                        order,
                        images,
                        limit: request.limit as usize,
                        wait,
            };
            let (page, context) = if call.session.caller.principal.workload_grant().is_some() {
                let started = tokio::time::Instant::now();
                let mut applied = driver.watch_applied().await;
                let (page, cut) = loop {
                    if let Some(applied) = &mut applied { applied.mark_seen(); }
                    let (admitted, cut) = self.workload_root_cut(&call).await?;
                    if !Arc::ptr_eq(&admitted, &database) || cut.custody_groups.is_some() {
                        return Err(error::unavailable("The Original Changes attachment is unavailable"));
                    }
                    let page = driver.changes_workload_root(change.clone(), cut.basis, &call.cancellation)
                        .await.map_err(error::anomaly)?;
                    let remaining = wait.saturating_sub(started.elapsed());
                    let answered = !page.groups.is_empty() || until.is_some() && page.complete || remaining.is_zero();
                    let Some(applied) = applied.as_mut().filter(|_| !answered) else {
                        break (page, cut);
                    };
                    let change = applied.changed(tokio::time::Instant::now() + remaining);
                    let changed = call.cancellation.run_until_cancelled(change)
                        .await.ok_or_else(crate::error::cancelled).map_err(error::map_status)?;
                    if changed.is_err() {
                        return Err(error::unavailable("The Original Changes attachment is unavailable"));
                    }
                };
                let now = self.0.node.now();
                let source = rtdb_node::serving::QuerySource {
                    branch: rtdb_types::rtdb_v1::MAIN_BRANCH.into(),
                    at: rtdb_types::rtdb_v1::At::Exactly(cut.basis), layer: rtdb_types::rtdb_v1::Layer::Merged,
                };
                let view = driver.query_workload_root(source, cut, now, &call.cancellation)
                    .await.map_err(error::anomaly)?;
                let snapshot = rtdb_query::DatabaseSnapshot { partitions: vec![view.snapshot], storage: Arc::clone(database.storage()) };
                (page, self.scoped_wire_context(&call, snapshot, now)?.with_plaintext())
            } else if let Some(scope) = &visibility {
                let now = self.0.node.now();
                let page = driver.changes_scoped(change, scope, &call.cancellation).await.map_err(error::anomaly)?;
                let snapshot = rtdb_query::DatabaseSnapshot { partitions: Vec::new(), storage: Arc::clone(database.storage()) };
                (page, self.scoped_wire_context(&call, snapshot, now)?.with_plaintext())
            } else {
                let context = self.wire_context(&call, &database, false).await?.with_plaintext();
                let page = context.reading(driver.changes(change, &call.cancellation)).await.map_err(error::anomaly)?;
                (page, context)
            };
            let mut page = page;
            let custody_release_proof = if request.custody_release_evidence {
                let (source, contributors) = page.scoped_custody.take()
                    .ok_or_else(|| error::unsupported("complete custody release inventory"))?;
                Some(self.scoped_custody_read(&call, &database, page.through, source, page.key_generation, contributors).await?)
            } else {
                None
            };
            self.check_attachment(&call, &database)?;
            let context = match page.read_custody.clone() {
                Some(custody) => {
                    let deadline = custody.deadline();
                    context.with_delivery_owner(custody, deadline)
                }
                None => context,
            };
            let initial = context.delivery_grant().await.map_err(error::anomaly)?;
            let (headers, cutoff) = call
                .delivery_headers(&initial, true)
                .map_err(error::map_status)?;
            Ok((page, images, context, initial, headers, cutoff, custody_release_proof))
        }
        .await
        .map_err(|status| call.error(status))?;
        let mut fields = schema().fields().to_vec();
        fields.push(Arc::new(Field::new("erased", DataType::Boolean, false)));
        if observe_positions {
            fields.push(Arc::new(Field::new("applied_position", DataType::UInt64, false)));
        }
        let schema = Arc::new(Schema::new(fields));
        let service = self.clone();
        let memory = Arc::clone(call.memory());
        let stream: Output<FlightData> = Box::pin(async_stream::try_stream! {
            let mut encoder = wire::Encoder::new(&schema, &memory).map_err(error::map_status)?;
            let schema_frame = wire::schema_data(&schema, &memory).map_err(error::map_status)?;
            service.check_query_delivery(&call, &database, page.through, page.key_generation, &page.custody_groups).await?;
            service.check_attachment(&call, &database)?;
            call.handoff(initial, cutoff).map_err(error::map_status)?;
            yield schema_frame;
            for group in page.groups.into_iter().filter(|_| !progress_only) {
                for row in group.rows {
                    service.check_attachment(&call, &database)?;
                    let erased = row.kind == ChangeKind::Erased;
                    let id = row.id.as_value();
                    let key = match (images, row.erased_written_basis, row.valid) {
                        (ChangeImages::None, _, _) => None,
                        (_, Some(basis), Some(valid)) => Some(write_values([
                            ("_id", &id),
                            ("_valid_from", &time_value(valid.lower())),
                            ("_valid_to", &time_value(valid.upper())),
                            (rtdb_types::rtdb_v1::WRITTEN_BASIS_COLUMN, &Value::Binary(basis.to_bytes().to_vec())),
                        ], &memory)?),
                        _ => Some(write_values([("_id", &id)], &memory)?),
                    };
                    let before = if erased || !matches!(images, ChangeImages::Before | ChangeImages::Both) { None }
                        else { row.before.as_ref().map(|image| write_change_image(&image.cells, &id, row.valid, &memory)).transpose()? };
                    let after = if erased || !matches!(images, ChangeImages::After | ChangeImages::Both) { None }
                        else { row.after.as_ref().map(|image| write_change_image(&image.cells, &id, row.valid, &memory)).transpose()? };
                    let key = applied_image_array(key).map_err(batch_error)?;
                    let before = applied_image_array(before).map_err(batch_error)?;
                    let after = applied_image_array(after).map_err(batch_error)?;
                    let mut columns: Vec<ArrayRef> = vec![
                        Arc::new(StringArray::from(vec![format!("{}.{}", row.table.schema, row.table.table)])),
                        Arc::new(TimestampMicrosecondArray::from(vec![group.cut.system_time.0]).with_timezone("UTC")),
                        Arc::new(BooleanArray::from(vec![row.kind == ChangeKind::Opened])),
                        Arc::new(key),
                        Arc::new(before),
                        Arc::new(after),
                        Arc::new(BooleanArray::from(vec![erased])),
                    ];
                    if observe_positions {
                        columns.push(Arc::new(UInt64Array::from(vec![group.cut.basis.position()])));
                    }
                    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
                        .map_err(|error| error::map_status(Status::internal(error.to_string())))?;
                    for frame in encoder.batch(&batch, &memory).map_err(error::map_status)? {
                        let grant = context.delivery_grant().await.map_err(error::anomaly)?;
                        service.check_query_delivery(&call, &database, page.through, page.key_generation, &page.custody_groups).await?;
                        service.check_attachment(&call, &database)?;
                        call.handoff(grant, cutoff).map_err(error::map_status)?;
                        yield frame;
                    }
                }
            }
            let grant = context.delivery_grant().await.map_err(error::anomaly)?;
            service.check_query_delivery(&call, &database, page.through, page.key_generation, &page.custody_groups).await?;
            service.check_attachment(&call, &database)?;
            call.handoff(grant, cutoff).map_err(error::map_status)?;
            yield trailer(proto::ChangesTrailer { through: Some(page.through.into()), complete: page.complete, custody_release_proof });
        });
        let mut response = Response::new(stream);
        *response.metadata_mut() = headers;
        Ok(response)
    }
}

fn applied_image_array(image: Option<bytes::Bytes>) -> Result<BinaryArray, ArrowError> {
    match image {
        Some(bytes) => singleton_binary_array(Buffer::from(bytes)),
        None => Ok(BinaryArray::from(vec![None::<&[u8]>])),
    }
}

fn write_change_image(
    cells: &StructFields<Value>,
    id: &Value,
    valid: Option<rtdb_types::Interval>,
    memory: &Arc<rtdb_columnar::MemoryBudget>,
) -> Result<bytes::Bytes, Status> {
    let from = valid.map(|valid| time_value(valid.lower()));
    let to = valid.map(|valid| time_value(valid.upper()));
    let values = [
        ("_id", Some(id)),
        ("_valid_from", from.as_ref()),
        ("_valid_to", to.as_ref()),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|value| (name, value)))
    .chain(cells.iter().filter_map(|(name, value)| {
        (!matches!(name.as_str(), "_id" | "_valid_from" | "_valid_to"))
            .then_some((name.as_str(), value))
    }));
    write_values(values, memory)
}

fn write_values<'a>(
    values: impl IntoIterator<Item = (&'a str, &'a Value)>,
    memory: &Arc<rtdb_columnar::MemoryBudget>,
) -> Result<bytes::Bytes, Status> {
    let values = values.into_iter().collect::<Vec<_>>();
    let workspace = values.iter().try_fold(0, |size, (name, value)| {
        let bytes = value.retained_bytes().map_err(error::anomaly)?;
        let field = wire::add(name.len(), bytes).map_err(error::map_status)?;
        wire::add(size, field).map_err(error::map_status)
    })?;
    let _workspace = memory
        .reserve(wire::mul(workspace, 4).map_err(error::map_status)?)
        .map_err(error::anomaly)?;
    let (fields, columns) = values
        .into_iter()
        .map(|(name, value)| {
            let ty = value.logical_type();
            Ok((
                rtdb_columnar::to_field(name, &ty).map_err(error::anomaly)?,
                rtdb_columnar::build_array_refs(&ty, &[value]).map_err(error::anomaly)?,
            ))
        })
        .collect::<Result<Vec<_>, Status>>()?
        .into_iter()
        .unzip::<_, _, Vec<_>, Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| error::map_status(Status::internal(error.to_string())))?;
    let size = wire::add(
        wire::mul(batch.get_array_memory_size(), 4).map_err(error::map_status)?,
        wire::schema_bound(batch.schema().as_ref()).map_err(error::map_status)?,
    )
    .map_err(error::map_status)?;
    let charge = memory.reserve(size).map_err(error::anomaly)?;
    let bytes = rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch))
        .map_err(error::anomaly)?;
    Ok(wire::retain(bytes, charge))
}

fn tables(names: &[String]) -> Result<BTreeSet<TableRef>, Status> {
    if !(1..=16).contains(&names.len()) {
        return Err(error::incorrect(
            "Changes requires one to sixteen distinct tables",
        ));
    }
    let tables = names
        .iter()
        .map(|name| {
            if name.len() > 512 {
                return Err(error::incorrect("Table name is too long"));
            }
            TableRef::parse(name).map_err(error::anomaly)
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if names.len() != tables.len() {
        return Err(error::incorrect(
            "Changes requires one to sixteen distinct tables",
        ));
    }
    Ok(tables)
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("table", DataType::Utf8, false),
        Field::new(
            "system_time",
            DataType::Timestamp(ArrowTime::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("opened", DataType::Boolean, false),
        Field::new("key_ipc", DataType::Binary, true),
        Field::new("before_ipc", DataType::Binary, true),
        Field::new("after_ipc", DataType::Binary, true),
    ])
}

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn time_value(time: Timestamp) -> Value {
    Value::Timestamp {
        value: time.0,
        unit: TimeUnit::Microsecond,
        timezone: Some("UTC".into()),
    }
}

async fn change_page(
    context: Arc<QueryContext>,
    request: &Request,
    upper: Timestamp,
    memory: &Arc<MemoryBudget>,
    pack_limit: usize,
) -> Result<Page, Status> {
    let schema = Arc::new(schema());
    let encoder = (!request.progress_only)
        .then(|| wire::Encoder::new(&schema, memory))
        .transpose()
        .map_err(error::map_status)?;
    let mut groups = BTreeMap::<Timestamp, Vec<_>>::new();
    let mut readers = Vec::new();
    let mut has_more = false;
    let direction = if request.descending { "DESC" } else { "ASC" };
    for table in &request.tables {
        let table_name: Arc<str> = format!("{}.{}", table.schema, table.table).into();
        let row_base = encoder
            .as_ref()
            .map(|_| singleton_base(&schema, &table_name))
            .transpose()?;
        for boundary in [Boundary::Opened, Boundary::Closed] {
            let name = format!("{}.{}", quote(&table.schema), quote(&table.table));
            let column = boundary.column();
            let image = match boundary {
                Boundary::Opened => {
                    matches!(request.images, proto::Images::After | proto::Images::Both)
                }
                Boundary::Closed => {
                    matches!(request.images, proto::Images::Before | proto::Images::Both)
                }
            };
            let projection = if image {
                ", _id, _valid_from, _valid_to, *"
            } else if request.images != proto::Images::None {
                ", _id"
            } else {
                ""
            };
            let sql = format!(
                "SELECT {column}{projection} FROM {name} FOR ALL VALID_TIME FOR ALL SYSTEM_TIME WHERE {column} > $1 AND {column} <= $2 ORDER BY {column} {direction}"
            );
            let prepared = PreparedSql::prepare(
                &sql,
                &context,
                Default::default(),
                ResultSignaturePolicy::Dynamic,
            )
            .map_err(error::anomaly)?;
            let mut cursor = prepared
                .open_query(
                    context.clone(),
                    vec![time_value(request.after), time_value(upper)],
                )
                .await
                .map_err(error::anomaly)?;
            let reader_index = readers.len();
            let mut reader_groups = 0;
            let mut last_time = None;
            let mut more = false;
            let mut key_layout = ImageLayout::default();
            let mut image_layout = ImageLayout::default();
            while !more
                && let Some(()) = cursor
                    .encode_next_batch(|batch| -> Result<(), Status> {
                        let batch = batch.batch();
                        let projection = batch.schema_ref();
                        let (time_index, time_field) =
                            projection.column_with_name(column).ok_or_else(|| {
                                error::map_status(Status::internal(
                                    "Changes projection omitted system time",
                                ))
                            })?;
                        let key_projection =
                            projection.column_with_name("_id").map(|(index, _)| [index]);
                        let image_projection = if image {
                            (0..batch.num_columns())
                                .filter(|index| *index != time_index)
                                .collect::<Vec<_>>()
                        } else {
                            Vec::new()
                        };
                        let mut key_writer = key_projection.as_ref().map(|columns| {
                            ImageProjection::new(
                                batch,
                                columns,
                                request.shared_images,
                                &mut key_layout,
                            )
                        });
                        let mut image_writer = image.then(|| {
                            ImageProjection::new(
                                batch,
                                &image_projection,
                                request.shared_images,
                                &mut image_layout,
                            )
                        });
                        for row in 0..batch.num_rows() {
                            let value = rtdb_columnar::read_value(
                                time_field,
                                batch.column(time_index).as_ref(),
                                row,
                            )
                            .map_err(error::anomaly)?;
                            let time = rtdb_expr::timestamp_micros(&value, &context.timezone)
                                .map_err(error::anomaly)?;
                            if last_time != Some(time) {
                                reader_groups += 1;
                                last_time = Some(time);
                            }
                            groups.entry(time).or_default();
                            if groups.len() > request.limit {
                                has_more = true;
                                if request.descending {
                                    groups.pop_first();
                                } else {
                                    groups.pop_last();
                                }
                            }
                            if reader_groups > request.limit {
                                more = true;
                                break;
                            }
                            let Some(row_base) = row_base else {
                                continue;
                            };
                            let (key, _key_scratch) = key_writer
                                .as_mut()
                                .map(|writer| writer.prepare(row, memory))
                                .transpose()?
                                .unzip();
                            let (image, _image_scratch) = image_writer
                                .as_mut()
                                .map(|writer| writer.prepare(row, memory))
                                .transpose()?
                                .unzip();
                            let opened = matches!(boundary, Boundary::Opened);
                            let (before, after) =
                                if opened { (None, image) } else { (image, None) };
                            let images = [key, before, after];
                            let lengths = images
                                .each_ref()
                                .map(|image| image.as_ref().map(Image::legacy_len));
                            let bound = singleton_bound(row_base, lengths)?;
                            let charge = memory
                                .reserve(bound)
                                .map_err(crate::error::status)
                                .map_err(error::map_status)?;
                            let mut row = ReservedRow {
                                table: Arc::clone(&table_name),
                                time,
                                opened,
                                images,
                                charge,
                            };
                            // The singleton charge covers the retained images and one image's
                            // encoding scratch. Image encoding finishes before outer encoding.
                            if bound < MIN_DEFERRED_IMAGE_ROW_BOUND
                                || row
                                    .images
                                    .iter()
                                    .flatten()
                                    .any(|image| !matches!(image, Image::Deferred(_)))
                            {
                                row.finish_images()?;
                            }
                            if let Some(rows) = groups.get_mut(&time) {
                                rows.push((reader_index, row));
                            }
                        }
                        Ok(())
                    })
                    .await
                    .map_err(error::anomaly)?
                    .transpose()?
            {}
            readers.push(cursor);
        }
    }
    let has_changes = !groups.is_empty();
    let through = if request.descending {
        if has_more {
            Timestamp(
                groups
                    .first_key_value()
                    .expect("nonempty change page")
                    .0
                    .0
                    .saturating_sub(1),
            )
        } else {
            request.after
        }
    } else if has_more {
        *groups.last_key_value().expect("nonempty change page").0
    } else {
        upper
    };
    let complete = !has_more && request.until.is_none_or(|until| until <= upper);
    let frames = match encoder {
        Some(encoder) => encode_page(
            groups,
            request.descending,
            request.shared_images,
            &schema,
            encoder,
            memory,
            pack_limit,
        )?,
        None => Vec::new(),
    };
    Ok(Page {
        frames,
        readers,
        has_changes,
        through,
        complete,
    })
}

fn batch_error(error: ArrowError) -> Status {
    error::map_status(Status::internal(error.to_string()))
}

fn singleton_base(schema: &SchemaRef, table: &str) -> Result<usize, Status> {
    let images = std::array::from_fn(|_| BinaryArray::from(vec![Some(&[][..])]));
    let batch = outer_row(Arc::clone(schema), table, Timestamp(0), false, images)
        .map_err(batch_error)?;
    wire::batch_bound(&batch).map_err(error::map_status)
}

fn singleton_bound(base: usize, image_lengths: [Option<usize>; 3]) -> Result<usize, Status> {
    // Arrow's one-element BinaryArray collect starts with 1024 value bytes.
    // A null adds one 64-byte bitmap, and a single append grows the value Vec at most once.
    const INITIAL_VALUES: usize = 1024;
    let extra = image_lengths.into_iter().try_fold(0, |extra, len| {
        let growth = match len {
            None => 64,
            Some(len) if len > INITIAL_VALUES => len.max(2 * INITIAL_VALUES) - INITIAL_VALUES,
            Some(_) => 0,
        };
        wire::add(extra, growth)
    });
    extra
        .and_then(|extra| wire::add(base, wire::mul(extra, 4)?))
        .map_err(error::map_status)
}

fn outer_row(
    schema: SchemaRef,
    table: &str,
    time: Timestamp,
    opened: bool,
    [key, before, after]: [BinaryArray; 3],
) -> Result<RecordBatch, ArrowError> {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![table])),
            Arc::new(TimestampMicrosecondArray::from(vec![time.0]).with_timezone("UTC")),
            Arc::new(BooleanArray::from(vec![opened])),
            Arc::new(key),
            Arc::new(before),
            Arc::new(after),
        ],
    )
}

fn encode_page(
    mut groups: BTreeMap<Timestamp, Vec<(usize, ReservedRow)>>,
    descending: bool,
    shared_images: bool,
    schema: &SchemaRef,
    mut encoder: wire::Encoder,
    memory: &Arc<MemoryBudget>,
    pack_limit: usize,
) -> Result<Vec<(usize, Vec<FlightData>)>, Status> {
    for (_, row) in groups.values_mut().flatten() {
        row.finish_images()?;
    }
    let mut groups = groups.into_values();
    let mut rows = std::iter::from_fn(|| {
        if descending {
            groups.next_back()
        } else {
            groups.next()
        }
    })
    .flatten()
    .peekable();
    let mut frames = Vec::new();
    while let Some((reader, first)) = rows.next() {
        let mut bound = first.charge.bytes();
        if bound > pack_limit {
            frames.push((reader, first.encode(schema, &mut encoder)?));
            continue;
        }
        let mut run = vec![first];
        while let Some((_, row)) = rows.next_if(|(next_reader, row)| {
            *next_reader == reader && row.charge.bytes() <= pack_limit - bound
                && (!shared_images || run[0].shares_streams_with(row))
        }) {
            bound += row.charge.bytes();
            run.push(row);
        }
        if run.len() > 1
            && let Some(data) = pack_rows(schema, &run, bound, &mut encoder, memory, pack_limit)
        {
            frames.push((reader, data));
        } else {
            for row in run {
                frames.push((reader, row.encode(schema, &mut encoder)?));
            }
        }
    }
    Ok(frames)
}

fn pack_rows(
    schema: &SchemaRef,
    rows: &[ReservedRow],
    bound: usize,
    encoder: &mut wire::Encoder,
    memory: &Arc<MemoryBudget>,
    pack_limit: usize,
) -> Option<Vec<FlightData>> {
    // Packing is optional under memory pressure. The single-row fallback already holds its charges.
    let _scratch = memory.reserve(bound).ok()?;
    let batch = pack_batch(Arc::clone(schema), rows).ok()?;
    let frames = encoder.batch(&batch, memory).ok()?;
    frames
        .iter()
        .all(|frame| frame.encoded_len() <= pack_limit)
        .then_some(frames)
}

fn pack_batch(schema: SchemaRef, rows: &[ReservedRow]) -> Result<RecordBatch, ArrowError> {
    let [key, before, after] = [image_array(rows, 0)?, image_array(rows, 1)?, image_array(rows, 2)?];
    let tables = rows.iter().map(|row| row.table.as_ref());
    let times = rows.iter().map(|row| row.time.0);
    let opened = rows.iter().map(|row| row.opened);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(tables)),
            Arc::new(TimestampMicrosecondArray::from_iter_values(times).with_timezone("UTC")),
            Arc::new(opened.collect::<BooleanArray>()),
            Arc::new(key),
            Arc::new(before),
            Arc::new(after),
        ],
    )
}

fn singleton_image_array(image: Option<Image>) -> Result<BinaryArray, ArrowError> {
    match image {
        Some(Image::Binary(bytes)) => singleton_binary_array(Buffer::from_vec(bytes)),
        Some(Image::Shared { stream, row }) => {
            shared_image_array(&stream, std::iter::once(&row), 1)
        }
        None => Ok(BinaryArray::new_null(1)),
        Some(Image::Deferred(_)) => unreachable!("singleton images have been encoded"),
    }
}

fn singleton_binary_array(values: Buffer) -> Result<BinaryArray, ArrowError> {
    let len = values.len();
    let end = i32::try_from(len).map_err(|_| ArrowError::OffsetOverflowError(len))?;
    BinaryArray::try_new(OffsetBuffer::new(vec![0, end].into()), values, None)
}

fn image_array(rows: &[ReservedRow], column: usize) -> Result<BinaryArray, ArrowError> {
    let images = rows.iter().map(|row| row.images[column].as_ref());
    let Some(Image::Shared { stream, .. }) = &rows[0].images[column] else {
        return Ok(images
            .map(|image| match image {
                Some(Image::Binary(bytes)) => Some(bytes.as_slice()),
                None => None,
                Some(_) => unreachable!("packed rows use the same encoded image carrier"),
            })
            .collect());
    };
    let messages = images.flatten().map(|image| match image {
        Image::Shared { row, .. } => row,
        _ => unreachable!("packed rows use the same encoded image carrier"),
    });
    shared_image_array(stream, messages, rows.len())
}

fn shared_image_array<'a>(
    stream: &ImageStream,
    messages: impl Iterator<Item = &'a PreparedStreamRow> + Clone,
    rows: usize,
) -> Result<BinaryArray, ArrowError> {
    let schema_frame = stream.encoder.schema_frame();
    let len = schema_frame.len()
        + messages.clone().map(PreparedStreamRow::framed_len).sum::<usize>()
        + IMAGE_END.len();
    let end = i32::try_from(len).map_err(|_| ArrowError::OffsetOverflowError(len))?;
    let mut bytes = Vec::with_capacity(len);
    bytes.extend_from_slice(schema_frame);
    for message in messages {
        message.append_to(&mut bytes);
    }
    bytes.extend_from_slice(&IMAGE_END);
    let mut offsets = vec![end; rows + 1];
    offsets[0] = 0;
    let nulls = (rows > 1).then(|| {
        let mut valid = vec![false; rows];
        valid[0] = true;
        NullBuffer::from(valid)
    });
    BinaryArray::try_new(OffsetBuffer::new(offsets.into()), bytes.into(), nulls)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Decimal128Array, Float32Array, Int64Array, builder::StringDictionaryBuilder,
        types::Int32Type,
    };

    fn image_stream(image: &Image) -> Option<&Arc<ImageStream>> {
        match image {
            Image::Shared { stream, .. } => Some(stream),
            Image::Deferred(deferred) => deferred.stream.as_ref(),
            Image::Binary(_) => None,
        }
    }

    fn admission_bytes(image: &Image, scratch: &MemoryReservation, first: bool) -> usize {
        let retained = image_stream(image)
            .filter(|_| first)
            .map_or(0, |stream| stream._charge.bytes());
        scratch.bytes() + retained
    }

    #[test]
    fn fixed_images_preserve_scratch_admission_and_singleton_bytes_when_deferred() {
        let batch = RecordBatch::try_from_iter([
            (
                "_id",
                Arc::new(Int64Array::from(vec![Some(1), Some(-42), None])) as ArrayRef,
            ),
            (
                "_valid_from",
                Arc::new(
                    TimestampMicrosecondArray::from(vec![2, 170, 191])
                        .with_timezone("America/New_York"),
                ) as ArrayRef,
            ),
        ])
        .unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let schema = Arc::new(schema());
        let table: Arc<str> = Arc::from("public.rows");
        let base = singleton_base(&schema, &table).unwrap();
        assert!(base >= MIN_DEFERRED_IMAGE_ROW_BOUND);
        let candidate = batch.slice(1, 1);
        let scratch_bound = wire::batch_bound(&candidate).unwrap();
        let expected =
            rtdb_format::ipc::write_stream(candidate.schema_ref(), std::slice::from_ref(&candidate))
                .unwrap();

        for shared in [false, true] {
            let mut layout = ImageLayout::default();
            let mut projection = ImageProjection::new(&batch, &[0, 1], shared, &mut layout);
            let (first, first_scratch) = projection.prepare(0, &memory).unwrap();
            assert!(!matches!(&first, Image::Deferred(_)));
            drop(first_scratch);

            let restricted = MemoryBudget::new(scratch_bound - 1);
            let expected_error = error::anomaly(restricted.reserve(scratch_bound).err().unwrap());
            let error = projection.prepare(1, &restricted).err().unwrap();
            assert_eq!(error.code(), expected_error.code());
            assert_eq!(error.details(), expected_error.details());

            let (image, scratch) = projection.prepare(1, &memory).unwrap();
            assert!(matches!(&image, Image::Deferred(_)));
            assert_eq!(scratch.bytes(), scratch_bound);
            assert_eq!(image.legacy_len(), expected.len());
            let charge = memory
                .reserve(singleton_bound(base, [Some(expected.len()), None, None]).unwrap())
                .unwrap();
            let mut row = ReservedRow {
                table: Arc::clone(&table),
                time: Timestamp(1),
                opened: true,
                images: [Some(image), None, None],
                charge,
            };
            drop(scratch);
            row.finish_images().unwrap();
            let materialized = row.images[0].as_ref().unwrap();
            assert_eq!(materialized.legacy_len(), expected.len());
            assert_eq!(first.shares_schema_with(materialized), shared);
            let singleton = image_array(std::slice::from_ref(&row), 0).unwrap();
            assert_eq!(singleton.value(0), expected.as_slice());

            let (null, _) = projection.prepare(2, &memory).unwrap();
            assert!(matches!(null, Image::Deferred(_)));
        }
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn variable_images_keep_legacy_admission_and_bytes_across_padding_boundaries() {
        let oversized = vec![0xab; MAX_DEFERRED_VARIABLE_BYTES + 1];
        let batch = RecordBatch::try_from_iter([
            (
                "_id",
                Arc::new(StringArray::from(vec![
                    Some("seed"),
                    Some("é"),
                    Some(""),
                    Some("more than eight bytes"),
                    Some("one"),
                    None,
                ])) as ArrayRef,
            ),
            (
                "payload",
                Arc::new(BinaryArray::from_iter_values([
                    &b"\x00"[..],
                    &b"123456789"[..],
                    &b""[..],
                    &b"\xff\x00"[..],
                    oversized.as_slice(),
                    &b"\x01"[..],
                ])) as ArrayRef,
            ),
            (
                "number",
                Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5])) as ArrayRef,
            ),
        ])
        .unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let table: Arc<str> = Arc::from("public.rows");
        let base = singleton_base(&Arc::new(schema()), &table).unwrap();
        for shared in [false, true] {
            let mut layout = ImageLayout::default();
            let mut projection = ImageProjection::new(&batch, &[0, 1, 2], shared, &mut layout);
            for (index, deferred) in [false, true, true, true, false, true]
                .into_iter()
                .enumerate()
            {
                let candidate = batch.slice(index, 1);
                let bound = wire::batch_bound(&candidate).unwrap();
                if deferred {
                    let restricted = MemoryBudget::new(bound - 1);
                    let expected = error::anomaly(restricted.reserve(bound).err().unwrap());
                    let rejection = projection.prepare(index, &restricted).err().unwrap();
                    assert_eq!(rejection.code(), expected.code());
                    assert_eq!(rejection.details(), expected.details());
                }

                let expected = rtdb_format::ipc::write_stream(
                    candidate.schema_ref(),
                    std::slice::from_ref(&candidate),
                )
                .unwrap();
                let (image, scratch) = projection.prepare(index, &memory).unwrap();
                assert_eq!(admission_bytes(&image, &scratch, index == 0), bound);
                assert_eq!(matches!(&image, Image::Deferred(_)), deferred);
                assert_eq!(image.legacy_len(), expected.len());
                let charge = memory
                    .reserve(singleton_bound(base, [None, None, Some(expected.len())]).unwrap())
                    .unwrap();
                let mut row = ReservedRow {
                    table: Arc::clone(&table),
                    time: Timestamp(1),
                    opened: true,
                    images: [None, None, Some(image)],
                    charge,
                };
                drop(scratch);
                row.finish_images().unwrap();
                let singleton = image_array(std::slice::from_ref(&row), 2).unwrap();
                assert_eq!(singleton.value(0), expected.as_slice());
            }
            drop(projection);
            drop(layout);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn nullable_images_preserve_physical_null_bytes_in_singleton_and_shared_streams() {
        let validity = |bytes: [u8; 2]| {
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from(bytes),
                0,
                10,
            )))
        };
        let numbers = Arc::new(Int64Array::new(
            (1_i64..=10).collect::<Vec<_>>().into(),
            validity([0b1111_1101, 0b1111_1110]),
        )) as ArrayRef;
        let (offsets, values, _) = StringArray::from(vec![
            "hidden",
            "é",
            "hidden across padding",
            "",
            "",
            "",
            "",
            "",
            "hidden",
            "",
        ])
        .into_parts();
        let text = Arc::new(StringArray::new(
            offsets,
            values,
            validity([0b1111_1010, 0b1100_0010]),
        )) as ArrayRef;
        let payload = Arc::new(BinaryArray::new(
            OffsetBuffer::new((0..=10).collect::<Vec<i32>>().into()),
            Buffer::from([0xab; 10]),
            validity([0b1111_1111, 0b0011_0000]),
        )) as ArrayRef;
        let document = Arc::new(StructArray::new(
            vec![
                Field::new("n", DataType::Int64, false),
                Field::new("s", DataType::Utf8, true),
            ]
            .into(),
            vec![Arc::clone(&numbers), Arc::clone(&text)],
            validity([0b1111_1100, 0b1010_0010]),
        )) as ArrayRef;
        let flat =
            RecordBatch::try_from_iter([("n", numbers), ("s", text), ("b", Arc::clone(&payload))])
                .unwrap();
        let nested = RecordBatch::try_from_iter([("doc", document), ("b", payload)]).unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let table: Arc<str> = Arc::from("public.rows");
        let base = singleton_base(&Arc::new(schema()), &table).unwrap();
        let indices = [0, 1, 2, 8, 9];

        for batch in [flat, nested] {
            let columns = (0..batch.num_columns()).collect::<Vec<_>>();
            let slices = indices.map(|index| batch.slice(index, 1));
            for shared in [false, true] {
                let mut layout = ImageLayout::default();
                let mut projection = ImageProjection::new(&batch, &columns, shared, &mut layout);
                let mut rows = Vec::new();
                for (position, (index, slice)) in indices.into_iter().zip(&slices).enumerate() {
                    let (image, scratch) = projection.prepare(index, &memory).unwrap();
                    assert_eq!(
                        admission_bytes(&image, &scratch, position == 0),
                        wire::batch_bound(slice).unwrap(),
                    );
                    assert_eq!(matches!(&image, Image::Deferred(_)), position != 0);
                    let expected = rtdb_format::ipc::write_stream(
                        slice.schema_ref(),
                        std::slice::from_ref(slice),
                    )
                    .unwrap();
                    assert_eq!(image.legacy_len(), expected.len());
                    let charge = memory
                        .reserve(singleton_bound(base, [None, Some(expected.len()), None]).unwrap())
                        .unwrap();
                    let mut row = ReservedRow {
                        table: Arc::clone(&table),
                        time: Timestamp(1),
                        opened: false,
                        images: [None, Some(image), None],
                        charge,
                    };
                    drop(scratch);
                    row.finish_images().unwrap();
                    let encoded = image_array(std::slice::from_ref(&row), 1).unwrap();
                    assert_eq!(encoded.value(0), expected.as_slice());
                    rows.push(row);
                }
                if shared {
                    assert!(rows.iter().all(|row| rows[0].shares_streams_with(row)));
                    let expected =
                        rtdb_format::ipc::write_stream(batch.schema_ref(), &slices).unwrap();
                    let encoded = image_array(&rows, 1).unwrap();
                    assert_eq!(encoded.value(0), expected.as_slice());
                    assert_eq!(encoded.null_count(), rows.len() - 1);
                }
            }
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn bounded_nested_images_preserve_admission_and_bytes_with_empty_and_null_children() {
        let parent_nulls = || {
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from([0b1111_1011]),
                0,
                6,
            )))
        };
        let child_nulls = || {
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from([0xb7, 0xee, 0x3f, 0xff]),
                0,
                25,
            )))
        };
        let flags = Arc::new(BooleanArray::new(
            BooleanBuffer::new(Buffer::from([0xe5, 0x9a, 0x5b, 0x01]), 0, 25),
            child_nulls(),
        ));
        let (offsets, bytes, _) = StringArray::from_iter_values(
            (0..25).map(|row| if row % 2 == 0 { "hidden" } else { "été" }),
        )
        .into_parts();
        let text = Arc::new(StringArray::new(offsets, bytes, child_nulls()));
        let fields = vec![
            Field::new("flag", DataType::Boolean, false),
            Field::new("text", DataType::Utf8, true),
        ];
        let nested = Arc::new(StructArray::new(
            fields.into(),
            vec![flags, text.clone()],
            child_nulls(),
        ));
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new("item", nested.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1, 1, 4, 8, 16, 25].into()),
            nested,
            parent_nulls(),
        ));
        let wrapper = Arc::new(StructArray::new(
            vec![Field::new("list", list.data_type().clone(), true)].into(),
            vec![list],
            None,
        ));
        let numbers = Arc::new(Int64Array::from_iter_values(0_i64..25));
        let large = Arc::new(LargeListArray::new(
            Arc::new(Field::new("item", DataType::Int64, false)),
            OffsetBuffer::new(vec![0, 1, 1, 4, 8, 16, 25].into()),
            numbers.clone(),
            parent_nulls(),
        ));
        let ragged = Arc::new(ListArray::new(
            Arc::new(Field::new("item", large.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1, 1, 3, 4, 5, 6].into()),
            large,
            parent_nulls(),
        ));
        let entries = StructArray::new(
            vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Utf8, true),
            ]
            .into(),
            vec![numbers, text],
            None,
        );
        let map = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 1, 1, 4, 8, 16, 25].into()),
            entries,
            parent_nulls(),
            true,
        ));
        let fixed = Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Int64, false)),
            2,
            Arc::new(Int64Array::from(
                (0_i64..12)
                    .map(|value| (value != 4).then_some(value))
                    .collect::<Vec<_>>(),
            )),
            parent_nulls(),
        ));

        for (array, last_deferred) in [
            (wrapper as ArrayRef, false),
            (ragged as ArrayRef, false),
            (map as ArrayRef, false),
            (fixed as ArrayRef, true),
        ] {
            let batch = RecordBatch::try_from_iter([("payload", array)]).unwrap();
            let memory = MemoryBudget::new(1 << 20);
            let slices = (0..batch.num_rows())
                .map(|row| batch.slice(row, 1))
                .collect::<Vec<_>>();
            for shared in [false, true] {
                let mut layout = ImageLayout::default();
                let mut rows = Vec::new();
                for (row, slice) in slices.iter().enumerate() {
                    let source = batch.slice(row, 1);
                    let mut projection = ImageProjection::new(&source, &[0], shared, &mut layout);
                    let bound = wire::batch_bound(slice).unwrap();
                    let restricted = MemoryBudget::new(bound - 1);
                    let expected = error::anomaly(restricted.reserve(bound).err().unwrap());
                    let rejection = projection.prepare(0, &restricted).err().unwrap();
                    assert_eq!(rejection.code(), expected.code());
                    assert_eq!(rejection.details(), expected.details());

                    let (image, charge) = projection.prepare(0, &memory).unwrap();
                    let deferred = row != 0 && (last_deferred || row + 1 != slices.len());
                    assert_eq!(matches!(&image, Image::Deferred(_)), deferred);
                    assert_eq!(admission_bytes(&image, &charge, row == 0), bound);
                    let expected = rtdb_format::ipc::write_stream(
                        slice.schema_ref(),
                        std::slice::from_ref(slice),
                    )
                    .unwrap();
                    assert_eq!(image.legacy_len(), expected.len());
                    let mut row = ReservedRow {
                        table: Arc::from("public.rows"),
                        time: Timestamp(1),
                        opened: true,
                        images: [None, None, Some(image)],
                        charge,
                    };
                    row.finish_images().unwrap();
                    let encoded = image_array(std::slice::from_ref(&row), 2).unwrap();
                    assert_eq!(encoded.value(0), expected.as_slice());
                    rows.push(row);
                }
                if shared {
                    let expected =
                        rtdb_format::ipc::write_stream(batch.schema_ref(), &slices).unwrap();
                    let encoded = image_array(&rows, 2).unwrap();
                    assert_eq!(encoded.value(0), expected.as_slice());
                    assert_eq!(encoded.null_count(), rows.len() - 1);
                }
            }
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn bounded_scalar_and_metadata_images_keep_sliced_wire_bytes() {
        let validity = || {
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from([0xff, 0xfe]),
                0,
                10,
            )))
        };
        let flags = Arc::new(BooleanArray::new(
            BooleanBuffer::new(Buffer::from([0x5a, 0xa5]), 0, 10),
            validity(),
        ));
        let floats = Arc::new(Float32Array::new(
            vec![f32::from_bits(0x7fc0_0042); 10].into(),
            validity(),
        ));
        let uuid = Arc::new(
            FixedSizeBinaryArray::try_new(16, Buffer::from_vec((0_u8..160).collect()), validity())
                .unwrap(),
        );
        let uuid_field = rtdb_columnar::to_field(
            "_id",
            &rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Uuid).maybe(),
        )
        .unwrap();
        let annotated = RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(
                vec![
                    uuid_field,
                    Field::new("flag", DataType::Boolean, true),
                    Field::new("float", DataType::Float32, true),
                ],
                [("image-metadata".into(), "preserved".into())].into(),
            )),
            vec![uuid, flags, floats],
        )
        .unwrap();

        let decimal_type = DataType::Decimal128(12, 1);
        let decimals = Arc::new(
            Decimal128Array::new((1_i128..=10).collect::<Vec<_>>().into(), validity())
                .with_data_type(decimal_type.clone()),
        );
        let decimal = rtdb_columnar::with_decimal_parameters(
            Field::new("decimal", decimal_type, true),
            rtdb_columnar::DecimalParameters {
                precision: 12,
                scale: -400,
                bit_width: 128,
            },
        )
        .unwrap();
        let text = Arc::new(LargeStringArray::from(vec![
            "",
            "",
            "",
            "a longer prefix",
            "",
            "",
            "",
            "seed",
            "été",
            "",
        ]));
        let payloads = Arc::new(LargeBinaryArray::from_iter_values(
            (0..10).map(|row| &b"123456789"[..row]),
        ));
        let wide = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                decimal,
                Field::new("large_text", DataType::LargeUtf8, false),
                Field::new("large_binary", DataType::LargeBinary, false),
            ])),
            vec![decimals, text, payloads],
        )
        .unwrap();

        let null_doc = Arc::new(
            StructArray::try_new(
                vec![Field::new("null", DataType::Null, true)].into(),
                vec![Arc::new(NullArray::new(10))],
                Some(NullBuffer::new(BooleanBuffer::new(
                    Buffer::from([0x7f, 0x01]),
                    0,
                    10,
                ))),
            )
            .unwrap(),
        ) as ArrayRef;
        let nulls = RecordBatch::try_from_iter([("doc", null_doc)]).unwrap();

        for shared in [false, true] {
            let memory = MemoryBudget::new(1 << 20);
            let mut layout = ImageLayout::default();
            for (batch, deferred) in [
                (&annotated, [false, true, true]),
                (&wide, [false, true, true]),
                (&nulls, [false, false, true]),
            ] {
                let batch = batch.slice(7, 3);
                let columns = (0..batch.num_columns()).collect::<Vec<_>>();
                let mut projection = ImageProjection::new(&batch, &columns, shared, &mut layout);
                for (index, deferred) in deferred.into_iter().enumerate() {
                    let slice = batch.slice(index, 1);
                    let expected = rtdb_format::ipc::write_stream(
                        slice.schema_ref(),
                        std::slice::from_ref(&slice),
                    ).unwrap();
                    let (image, charge) = projection.prepare(index, &memory).unwrap();
                    assert_eq!(
                        admission_bytes(&image, &charge, index == 0),
                        wire::batch_bound(&slice).unwrap(),
                    );
                    assert_eq!(matches!(&image, Image::Deferred(_)), deferred);
                    assert_eq!(image.legacy_len(), expected.len());
                    let mut row = ReservedRow {
                        table: Arc::from("public.rows"),
                        time: Timestamp(1),
                        opened: true,
                        images: [None, None, Some(image)],
                        charge,
                    };
                    row.finish_images().unwrap();
                    let encoded = image_array(std::slice::from_ref(&row), 2).unwrap();
                    assert_eq!(encoded.value(0), expected.as_slice());
                }
            }
            drop(layout);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn shared_images_keep_the_released_bytes_across_input_batches() {
        let long = "value".repeat(500);
        let batch = RecordBatch::try_from_iter([
            (
                "_id",
                Arc::new(StringArray::from(vec!["one", "two", "three"])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(StringArray::from(vec![
                    Some("short"),
                    None,
                    Some(long.as_str()),
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        let inputs = [batch.slice(0, 1), batch.slice(1, 2)];
        let memory = MemoryBudget::new(1 << 20);
        let outer_schema = Arc::new(schema());
        let table: Arc<str> = Arc::from("public.rows");
        let base = singleton_base(&outer_schema, &table).unwrap();
        let released = RowStreamEncoder::new(batch.schema_ref()).unwrap();
        let mut expected = released.schema_frame().to_vec();
        let mut rows: Vec<ReservedRow> = Vec::new();
        let mut shared_layout = ImageLayout::default();
        let mut ordinary_layout = ImageLayout::default();
        for input in &inputs {
            let mut shared = ImageProjection::new(input, &[0, 1], true, &mut shared_layout);
            let mut ordinary = ImageProjection::new(input, &[0, 1], false, &mut ordinary_layout);
            for index in 0..input.num_rows() {
                let candidate = input.slice(index, 1);
                let legacy = rtdb_format::ipc::write_stream(
                    candidate.schema_ref(),
                    std::slice::from_ref(&candidate),
                )
                .unwrap();
                let (image, scratch) = shared.prepare(index, &memory).unwrap();
                let (binary, binary_scratch) = ordinary.prepare(index, &memory).unwrap();
                let deferred = rows.len() == 1;
                assert_eq!(matches!(&image, Image::Deferred(_)), deferred);
                assert_eq!(matches!(&binary, Image::Deferred(_)), deferred);
                let image_bound = wire::batch_bound(&candidate).unwrap();
                assert_eq!(
                    admission_bytes(&image, &scratch, rows.is_empty()),
                    image_bound,
                );
                assert_eq!(binary_scratch.bytes(), image_bound);
                if let Some(first) = rows.first() {
                    assert!(Arc::ptr_eq(
                        image_stream(first.images[0].as_ref().unwrap()).unwrap(),
                        image_stream(&image).unwrap(),
                    ));
                }
                let binary = match binary {
                    Image::Deferred(deferred) => deferred.encode().unwrap(),
                    binary => binary,
                };
                let Image::Binary(bytes) = binary else {
                    panic!("expected an ordinary image");
                };
                assert_eq!(bytes, legacy);
                assert_eq!(image.legacy_len(), legacy.len());
                expected.extend_from_slice(&released.encode(&candidate).unwrap());
                let charge = memory
                    .reserve(singleton_bound(base, [Some(legacy.len()), None, None]).unwrap())
                    .unwrap();
                let mut row = ReservedRow {
                    table: Arc::clone(&table),
                    time: Timestamp(1),
                    opened: true,
                    images: [Some(image), None, None],
                    charge,
                };
                row.finish_images().unwrap();
                let singleton = image_array(std::slice::from_ref(&row), 0).unwrap();
                assert_eq!(singleton.value(0), legacy.as_slice());
                rows.push(row);
            }
        }
        expected.extend_from_slice(&IMAGE_END);
        assert!(rows[0].shares_streams_with(&rows[2]));
        let packed = image_array(&rows, 0).unwrap();
        assert_eq!(packed.value(0), expected.as_slice());
        assert!(packed.is_null(1));
        assert!(packed.is_null(2));
        drop((rows, shared_layout));
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn a_changed_schema_replaces_the_encoder_while_old_images_keep_its_charge() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let batch = RecordBatch::try_from_iter([("_id", values)]).unwrap();
        let mut changed = batch.clone();
        changed
            .schema_metadata_mut()
            .insert("layout".into(), "new".into());
        let memory = MemoryBudget::new(1 << 20);
        let mut layout = ImageLayout::default();
        let (first, deferred) = {
            let mut projection = ImageProjection::new(&batch, &[0], true, &mut layout);
            (
                projection.prepare(0, &memory).unwrap().0,
                projection.prepare(1, &memory).unwrap().0,
            )
        };
        let first_stream = image_stream(&first).unwrap();
        assert!(Arc::ptr_eq(first_stream, image_stream(&deferred).unwrap()));
        let old_charge = first_stream._charge.bytes();
        assert_eq!(memory.used_bytes(), old_charge);

        let changed = ImageProjection::new(&changed, &[0], true, &mut layout)
            .prepare(0, &memory)
            .unwrap()
            .0;
        assert!(!first.shares_schema_with(&changed));
        let new_charge = image_stream(&changed).unwrap()._charge.bytes();
        assert_eq!(memory.used_bytes(), old_charge + new_charge);
        drop(first);

        let Image::Deferred(deferred) = deferred else {
            panic!("expected a deferred image");
        };
        let materialized = deferred.encode().unwrap();
        drop(deferred);
        assert_eq!(memory.used_bytes(), old_charge + new_charge);
        let candidate = batch.slice(1, 1);
        let expected = rtdb_format::ipc::write_stream(
            candidate.schema_ref(),
            std::slice::from_ref(&candidate),
        )
        .unwrap();
        let row = ReservedRow {
            table: Arc::from("public.rows"),
            time: Timestamp(1),
            opened: true,
            images: [Some(materialized), None, None],
            charge: memory.reserve(0).unwrap(),
        };
        let array = image_array(std::slice::from_ref(&row), 0).unwrap();
        assert_eq!(array.value(0), expected.as_slice());
        drop(row);
        assert_eq!(memory.used_bytes(), new_charge);
        drop((changed, layout));
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn image_scratch_admission_precedes_a_schema_encoding_error() {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["one"]));
        let valid = RecordBatch::try_from_iter([("_id", values)]).unwrap();
        let mut batch = valid.clone();
        batch
            .schema_metadata_mut()
            .insert(rtdb_columnar::INTERNAL_DECIMAL_SCALE_KEY.into(), "0".into());
        let candidate = batch.slice(0, 1).project(&[0]).unwrap();
        let bound = wire::batch_bound(&candidate).unwrap();
        for shared in [false, true] {
            let mut layout = ImageLayout::default();
            let mut valid_images = ImageProjection::new(&valid, &[0], shared, &mut layout);
            valid_images.prepare(0, &MemoryBudget::new(bound)).unwrap();
            let mut images = ImageProjection::new(&batch, &[0], shared, &mut layout);
            let memory = MemoryBudget::new(bound - 1);
            let expected = error::anomaly(memory.reserve(bound).err().unwrap());
            let rejection = images.prepare(0, &memory).err().unwrap();
            assert_eq!(rejection.code(), expected.code());
            assert_eq!(rejection.details(), expected.details());
            assert_eq!(memory.used_bytes(), 0);

            let memory = MemoryBudget::new(bound);
            let expected = error::anomaly(
                rtdb_format::ipc::write_stream(
                    candidate.schema_ref(),
                    std::slice::from_ref(&candidate),
                )
                .unwrap_err(),
            );
            let rejection = images.prepare(0, &memory).err().unwrap();
            assert_eq!(rejection.code(), expected.code());
            assert_eq!(rejection.details(), expected.details());
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn descriptor_reservation_equals_the_legacy_singleton_charge() {
        let schema = Arc::new(schema());
        let table = "public.a_table_name_long_enough_to_cross_an_arrow_string_buffer_boundary";
        let base = singleton_base(&schema, table).unwrap();
        for len in [0, 1, 1023, 1024, 1025, 2048, 2049, MAX_PACKED_BYTES] {
            for [key, before, after] in [[true, false, false], [true, true, false], [false; 3]] {
                let bytes = vec![0; len];
                let lengths = [key, before, after].map(|present| present.then_some(len));
                let images = lengths.map(|len| BinaryArray::from(vec![len.map(|_| bytes.as_slice())]));
                let batch = outer_row(Arc::clone(&schema), table, Timestamp(7), true, images).unwrap();
                assert_eq!(
                    singleton_bound(base, lengths).unwrap(),
                    wire::batch_bound(&batch).unwrap(),
                );
            }
        }
    }

    #[test]
    fn retained_rows_fall_back_to_singletons_when_packing_has_no_scratch() {
        let memory = MemoryBudget::new(1 << 20);
        let schema = Arc::new(schema());
        let encoder = wire::Encoder::new(&schema, &memory).unwrap();
        let time = Timestamp(1);
        let table: Arc<str> = Arc::from("public.rows");
        let base = singleton_base(&schema, &table).unwrap();
        let bytes = vec![0xab; 2049];
        let images = [
            BinaryArray::from(vec![Some(bytes.as_slice())]),
            BinaryArray::from(vec![None::<&[u8]>]),
            BinaryArray::from(vec![Some(&[][..])]),
        ];
        let reference = outer_row(Arc::clone(&schema), &table, time, true, images).unwrap();
        let expected = wire::Encoder::new(&schema, &memory)
            .unwrap()
            .batch(&reference, &memory)
            .unwrap();
        let bound = singleton_bound(base, [Some(bytes.len()), None, Some(0)]).unwrap();
        assert_eq!(bound, wire::batch_bound(&reference).unwrap());
        let rows = (0..2)
            .map(|_| {
                let charge = memory.reserve(bound).unwrap();
                let mut key = Vec::with_capacity(2 * (bytes.len() + IMAGE_END.len()));
                key.extend_from_slice(&bytes);
                let row = ReservedRow {
                    table: Arc::clone(&table),
                    time,
                    opened: true,
                    images: [Some(Image::Binary(key)), None, Some(Image::Binary(Vec::new()))],
                    charge,
                };
                (0, row)
            })
            .collect();
        let filler = memory.reserve(memory.available_bytes()).unwrap();
        let frames = encode_page(
            BTreeMap::from([(time, rows)]),
            false,
            false,
            &schema,
            encoder,
            &memory,
            MAX_PACKED_BYTES,
        )
        .unwrap();
        assert_eq!(frames, vec![(0, expected.clone()), (0, expected)]);
        drop((frames, filler));
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn a_binary_singleton_uses_the_original_values_allocation() {
        let mut bytes = Vec::with_capacity(4096);
        bytes.extend_from_slice(b"image payload");
        let pointer = bytes.as_ptr();
        let capacity = bytes.capacity();
        let array = singleton_image_array(Some(Image::Binary(bytes))).unwrap();
        assert_eq!(array.values().as_ptr(), pointer);
        assert_eq!(array.values().capacity(), capacity);
        assert_eq!(array.value(0), b"image payload");
    }

    #[test]
    fn applied_image_singletons_keep_the_wire_bytes_and_the_image_reservation() {
        let image_memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let frame_memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let id = Value::I64(7);
        let cells = StructFields::from([("payload".into(), Value::Utf8("data".repeat(300)))]);
        let valid = rtdb_types::Interval::new(Timestamp(10), Timestamp(20)).unwrap();
        let image = write_change_image(&cells, &id, Some(valid), &image_memory).unwrap();
        let charge = image_memory.used_bytes();
        assert!(charge > 0);
        let pointer = image.as_ptr();
        let schema = Arc::new(Schema::new(vec![
            Field::new("present", DataType::Binary, true),
            Field::new("absent", DataType::Binary, true),
        ]));
        let legacy = RecordBatch::try_new(Arc::clone(&schema), vec![
            Arc::new(BinaryArray::from(vec![Some(image.as_ref())])),
            Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
        ]).unwrap();
        let present = applied_image_array(Some(image)).unwrap();
        assert_eq!(present.value(0).as_ptr(), pointer);
        let shared = RecordBatch::try_new(Arc::clone(&schema), vec![
            Arc::new(present),
            Arc::new(applied_image_array(None).unwrap()),
        ]).unwrap();
        let frame = |batch: &RecordBatch| wire::Encoder::new(&schema, &frame_memory)
            .unwrap().batch(batch, &frame_memory).unwrap();
        assert_eq!(frame(&shared), frame(&legacy));
        assert_eq!(image_memory.used_bytes(), charge);
        drop(shared);
        assert_eq!(image_memory.used_bytes(), 0);
    }

    #[test]
    fn selected_rows_above_the_packing_limit_keep_singleton_frames() {
        let memory = MemoryBudget::new(1 << 20);
        let schema = Arc::new(schema());
        let table: Arc<str> = Arc::from("public.rows");
        let time = Timestamp(1);
        let charge = singleton_bound(singleton_base(&schema, &table).unwrap(), [None; 3]).unwrap();
        let make_row = || ReservedRow {
            table: Arc::clone(&table),
            time,
            opened: true,
            images: [None, None, None],
            charge: memory.reserve(charge).unwrap(),
        };
        let mut encoder = wire::Encoder::new(&schema, &memory).unwrap();
        let expected = make_row().encode(&schema, &mut encoder).unwrap();
        let encoder = wire::Encoder::new(&schema, &memory).unwrap();
        let frames = encode_page(
            BTreeMap::from([(time, vec![(2, make_row()), (2, make_row())])]),
            false, false, &schema, encoder, &memory, charge - 1,
        ).unwrap();
        assert_eq!(frames, vec![(2, expected.clone()), (2, expected)]);
    }

    #[test]
    fn dictionary_images_keep_their_independent_streams() {
        let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
        dictionary.append("one").unwrap();
        dictionary.append("two").unwrap();
        let values: ArrayRef = Arc::new(dictionary.finish());
        let batch = RecordBatch::try_from_iter([("_id", values)]).unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let mut shared_layout = ImageLayout::default();
        let mut legacy_layout = ImageLayout::default();
        let mut make_row = |batch: &RecordBatch| {
            let mut shared = ImageProjection::new(batch, &[0], true, &mut shared_layout);
            let mut legacy = ImageProjection::new(batch, &[0], false, &mut legacy_layout);
            let (image, charge) = shared.prepare(0, &memory).unwrap();
            let (legacy, _) = legacy.prepare(0, &memory).unwrap();
            let Image::Binary(legacy) = legacy else {
                panic!("expected an ordinary image");
            };
            assert_eq!(image.legacy_len(), legacy.len());
            let row = ReservedRow {
                table: Arc::from("public.rows"),
                time: Timestamp(1),
                opened: true,
                images: [Some(image), None, None],
                charge,
            };
            let encoded = image_array(std::slice::from_ref(&row), 0).unwrap();
            assert_eq!(encoded.value(0), legacy.as_slice());
            row
        };
        let first = make_row(&batch.slice(0, 1));
        let second = make_row(&batch.slice(1, 1));
        assert!(!Arc::ptr_eq(
            image_stream(first.images[0].as_ref().unwrap()).unwrap(),
            image_stream(second.images[0].as_ref().unwrap()).unwrap(),
        ));
        assert!(!first.shares_streams_with(&second));
    }
}
