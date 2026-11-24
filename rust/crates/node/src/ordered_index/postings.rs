use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Int64Array,
    NullArray, RecordBatch, StringArray, TimestampMicrosecondArray, UInt8Array, UInt64Array,
    UnionArray,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder, StringBuilder},
};
use arrow_buffer::Buffer;
use arrow_ipc::{
    MetadataVersion,
    reader::read_record_batch,
    root_as_message,
    writer::{CompressionContext, DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
#[cfg(test)]
use bytes::Bytes;
use rtdb_columnar::MemoryReservation;
use rtdb_types::{Iid, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeSeq};

use super::{
    format_error,
    key::{Atom, AtomRef, Bucket, arrow},
    tree::AddressRef,
};

const INTEGER_PRECISION: u8 = 20;
const ALIGNMENT: usize = 16;

// Bit positions in the leaf layout and type IDs in mixed Arrow columns.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Bool = 0,
    Integer = 1,
    Text = 2,
    Binary = 3,
    Uuid = 4,
    Day = 5,
    Unordered = 6,
    Null = 7,
    InstantMicros = 8,
    FloatOrder = 9,
    Decimal128 = 10,
}

const KINDS: [Kind; 11] = [
    Kind::Bool,
    Kind::Integer,
    Kind::Text,
    Kind::Binary,
    Kind::Uuid,
    Kind::Day,
    Kind::Unordered,
    Kind::Null,
    Kind::InstantMicros,
    Kind::FloatOrder,
    Kind::Decimal128,
];
const VALID_LAYOUT_BITS: u16 = (1 << KINDS.len()) - 1;

impl Kind {
    fn of(value: AtomRef<'_>) -> Self {
        match value {
            AtomRef::Bool(_) => Self::Bool,
            AtomRef::Integer(_) => Self::Integer,
            AtomRef::Text(_) => Self::Text,
            AtomRef::Binary(_) => Self::Binary,
            AtomRef::Uuid(_) => Self::Uuid,
            AtomRef::Day(_) => Self::Day,
            AtomRef::InstantMicros(_) => Self::InstantMicros,
            AtomRef::FloatOrder(_) => Self::FloatOrder,
            AtomRef::Decimal128(..) => Self::Decimal128,
            AtomRef::Unordered => Self::Unordered,
            AtomRef::Null => Self::Null,
        }
    }

    fn bit(self) -> u16 {
        1 << self as u8
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::Integer => DataType::Decimal128(INTEGER_PRECISION, 0),
            Self::Text => DataType::Utf8,
            Self::Binary => DataType::Binary,
            Self::Uuid => DataType::FixedSizeBinary(16),
            Self::Decimal128 => DataType::FixedSizeBinary(18),
            Self::Day | Self::InstantMicros => DataType::Int64,
            Self::FloatOrder => DataType::UInt64,
            Self::Unordered | Self::Null => DataType::Null,
        }
    }

    fn buffer_count(self) -> usize {
        match self {
            Self::Text | Self::Binary => 3,
            Self::Unordered | Self::Null => 0,
            _ => 2,
        }
    }

    fn build<'a>(self, cells: impl ExactSizeIterator<Item = AtomRef<'a>>, bytes: usize) -> Result<ArrayRef> {
        macro_rules! primitive {
            ($array:ty, $variant:ident) => {
                Arc::new(
                    cells
                        .map(|cell| match cell {
                            AtomRef::$variant(value) => Some(value),
                            _ => None,
                        })
                        .collect::<$array>(),
                )
            };
        }
        Ok(match self {
            Self::Bool => primitive!(BooleanArray, Bool),
            Self::Day => primitive!(Int64Array, Day),
            Self::InstantMicros => primitive!(Int64Array, InstantMicros),
            Self::FloatOrder => primitive!(UInt64Array, FloatOrder),
            Self::Integer => {
                let values = cells
                    .map(|cell| match cell {
                        AtomRef::Integer(value) => Some(value),
                        _ => None,
                    })
                    .collect::<Decimal128Array>();
                Arc::new(
                    values
                        .with_precision_and_scale(INTEGER_PRECISION, 0)
                        .map_err(format_error)?,
                )
            }
            Self::Decimal128 => {
                let mut builder = FixedSizeBinaryBuilder::with_capacity(cells.len(), 18);
                for cell in cells {
                    if let AtomRef::Decimal128(precision, scale, coefficient) = cell {
                        let mut bytes = [0; 18];
                        bytes[0] = precision;
                        bytes[1] = scale as u8;
                        bytes[2..].copy_from_slice(&coefficient.to_be_bytes());
                        builder.append_value(bytes).map_err(format_error)?;
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            Self::Text => {
                let mut builder = StringBuilder::with_capacity(cells.len(), bytes);
                for cell in cells {
                    match cell {
                        AtomRef::Text(value) => builder.append_value(value),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            Self::Binary => {
                let mut builder = BinaryBuilder::with_capacity(cells.len(), bytes);
                for cell in cells {
                    match cell {
                        AtomRef::Binary(value) => builder.append_value(value),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            Self::Uuid => {
                let mut builder = FixedSizeBinaryBuilder::with_capacity(cells.len(), 16);
                for cell in cells {
                    match cell {
                        AtomRef::Uuid(value) => {
                            builder.append_value(value).map_err(format_error)?
                        }
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            Self::Unordered | Self::Null => Arc::new(NullArray::new(cells.len())),
        })
    }

    fn cell(self, array: &dyn Array, row: usize) -> AtomRef<'_> {
        macro_rules! cell {
            ($array:ty, $variant:ident) => {
                AtomRef::$variant(
                    arrow::<$array>(array)
                        .expect("validated posting column")
                        .value(row),
                )
            };
        }
        if array.is_null(row) {
            return AtomRef::Null;
        }
        match self {
            Self::Bool => cell!(BooleanArray, Bool),
            Self::Integer => cell!(Decimal128Array, Integer),
            Self::Decimal128 => {
                let bytes = arrow::<FixedSizeBinaryArray>(array).expect("validated decimal posting").value(row);
                AtomRef::Decimal128(bytes[0], bytes[1] as i8,
                    i128::from_be_bytes(bytes[2..].try_into().expect("validated decimal coefficient")))
            },
            Self::Text => cell!(StringArray, Text),
            Self::Binary => cell!(BinaryArray, Binary),
            Self::Uuid => AtomRef::Uuid(fixed(array, row)),
            Self::Day => cell!(Int64Array, Day),
            Self::InstantMicros => cell!(Int64Array, InstantMicros),
            Self::FloatOrder => cell!(UInt64Array, FloatOrder),
            Self::Unordered => AtomRef::Unordered,
            Self::Null => AtomRef::Null,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Layout(u16);

impl Layout {
    fn kinds(self) -> impl Iterator<Item = Kind> {
        KINDS
            .into_iter()
            .filter(move |kind| self.0 & kind.bit() != 0)
    }

    fn plain(self) -> Option<Kind> {
        let non_null = self.0 & !Kind::Null.bit();
        if non_null == 0 {
            return Some(Kind::Null);
        }
        if non_null == Kind::Unordered.bit() && self.0 != non_null {
            return None;
        }
        (non_null.count_ones() == 1).then(|| KINDS[non_null.trailing_zeros() as usize])
    }

    fn field(self, slot: usize) -> Result<Field> {
        let name = slot.to_string();
        if let Some(kind) = self.plain() {
            return Ok(Field::new(
                name,
                kind.data_type(),
                self.0 & Kind::Null.bit() != 0,
            ));
        }
        let union = UnionFields::try_new(
            self.kinds().map(|kind| kind as i8),
            self.kinds().map(|kind| {
                Field::new(
                    (kind as u8).to_string(),
                    kind.data_type(),
                    kind == Kind::Null,
                )
            }),
        )
        .map_err(format_error)?;
        Ok(Field::new(
            name,
            DataType::Union(union, UnionMode::Dense),
            false,
        ))
    }

    fn build(self, field: &Field, cells: Vec<AtomRef<'_>>, bytes: usize) -> Result<ArrayRef> {
        if let Some(kind) = self.plain() {
            return kind.build(cells.into_iter(), bytes);
        }
        let DataType::Union(fields, UnionMode::Dense) = field.data_type() else {
            return Err(invalid());
        };
        let mut legs: [Vec<_>; KINDS.len()] = std::array::from_fn(|_| Vec::new());
        let mut payloads = [0usize; KINDS.len()];
        let mut ids = Vec::with_capacity(cells.len());
        let mut offsets = Vec::with_capacity(cells.len());
        for cell in cells {
            let kind = Kind::of(cell) as usize;
            ids.push(kind as i8);
            offsets.push(i32::try_from(legs[kind].len()).map_err(format_error)?);
            payloads[kind] = payloads[kind].saturating_add(cell.retained_bytes());
            legs[kind].push(cell);
        }
        let children = self
            .kinds()
            .map(|kind| kind.build(legs[kind as usize].iter().copied(), payloads[kind as usize]))
            .collect::<Result<_>>()?;
        Ok(Arc::new(
            UnionArray::try_new(fields.clone(), ids.into(), Some(offsets.into()), children)
                .map_err(format_error)?,
        ))
    }

    fn interleave(
        sources: &[(Self, &dyn Array)],
        indices: &[(usize, usize)],
        slot: usize,
        share_slices: bool,
    ) -> Result<(Self, ArrayRef)> {
        let first = sources[0].0;
        if let Some(kind) = first.plain()
            && first.0 == kind.bit()
            && sources.iter().all(|(layout, _)| *layout == first)
        {
            let values = sources.iter().map(|&(_, array)| array).collect::<Vec<_>>();
            return Ok((first, select_array(&values, indices, share_slices)?));
        }

        let unions = sources
            .iter()
            .map(|(layout, array)| {
                if layout.plain().is_some() {
                    Ok(None)
                } else {
                    arrow::<UnionArray>(*array).map(Some)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let chosen = indices
            .iter()
            .map(|&(source, row)| {
                let (kind, array, row) = if let Some(union) = unions[source] {
                    let tag = union.type_id(row);
                    (
                        KINDS[tag as usize],
                        union.child(tag).as_ref(),
                        union.value_offset(row),
                    )
                } else {
                    let (layout, array) = sources[source];
                    (layout.plain().expect("plain source"), array, row)
                };
                let kind = if array.is_null(row) { Kind::Null } else { kind };
                (kind, source, row)
            })
            .collect::<Vec<_>>();
        let layout = Self(
            chosen
                .iter()
                .fold(0, |mask, (kind, _, _)| mask | kind.bit()),
        );

        let leg = |kind: Kind| {
            let mut arrays = Vec::new();
            let mut remap = vec![0; sources.len()];
            for (source, &(layout, array)) in sources.iter().enumerate() {
                if layout.0 & kind.bit() == 0 {
                    continue;
                }
                remap[source] = arrays.len();
                arrays.push(unions[source].map_or(array, |union| union.child(kind as i8).as_ref()));
            }
            (arrays, remap)
        };

        if let Some(kind) = layout.plain() {
            if matches!(kind, Kind::Null | Kind::Unordered) {
                return Ok((layout, Arc::new(NullArray::new(chosen.len()))));
            }
            let (mut arrays, remap) = leg(kind);
            let nullable = layout.0 & Kind::Null.bit() != 0;
            let null = nullable.then(|| arrow_array::new_null_array(&kind.data_type(), 1));
            let null_source = arrays.len();
            if let Some(null) = null.as_deref() {
                arrays.push(null);
            }
            let rows = chosen
                .iter()
                .map(|&(kind, source, row)| {
                    if kind == Kind::Null {
                        (null_source, 0)
                    } else {
                        (remap[source], row)
                    }
                })
                .collect::<Vec<_>>();
            return Ok((
                layout,
                select_array(&arrays, &rows, share_slices && !nullable)?,
            ));
        }

        let field = layout.field(slot)?;
        let DataType::Union(fields, UnionMode::Dense) = field.data_type() else { return Err(invalid()); };
        let mut selected: [Vec<_>; KINDS.len()] = std::array::from_fn(|_| Vec::new());
        let mut tags = Vec::with_capacity(chosen.len());
        let mut offsets = Vec::with_capacity(chosen.len());
        for (kind, source, row) in chosen {
            let rows = &mut selected[kind as usize];
            tags.push(kind as i8);
            offsets.push(i32::try_from(rows.len()).map_err(format_error)?);
            rows.push((source, row));
        }
        let mut payload = 0usize;
        let children = layout
            .kinds()
            .map(|kind| {
                let rows = &mut selected[kind as usize];
                if matches!(kind, Kind::Null | Kind::Unordered) {
                    return Ok(Arc::new(NullArray::new(rows.len())) as ArrayRef);
                }
                let (arrays, remap) = leg(kind);
                for (source, _) in rows.iter_mut() {
                    *source = remap[*source];
                }
                let child = select_array(&arrays, rows, share_slices)?;
                payload = payload.saturating_add(match kind {
                    Kind::Text => byte_span(arrow::<StringArray>(child.as_ref())?.value_offsets()),
                    Kind::Binary => {
                        byte_span(arrow::<BinaryArray>(child.as_ref())?.value_offsets())
                    }
                    _ => 0,
                });
                if payload > i32::MAX as usize {
                    return Err(invalid());
                }
                Ok(child)
            })
            .collect::<Result<Vec<_>>>()?;
        let union =
            UnionArray::try_new(fields.clone(), tags.into(), Some(offsets.into()), children)
                .map_err(format_error)?;
        Ok((layout, Arc::new(union)))
    }

    fn cell(self, array: &dyn Array, row: usize) -> AtomRef<'_> {
        if let Some(kind) = self.plain() {
            return kind.cell(array, row);
        }
        let union = arrow::<UnionArray>(array).expect("validated posting union");
        let tag = union.type_id(row);
        KINDS[tag as usize].cell(union.child(tag).as_ref(), union.value_offset(row))
    }
}

fn interleave_array(arrays: &[&dyn Array], indices: &[(usize, usize)]) -> Result<ArrayRef> {
    arrow_select::interleave::interleave(arrays, indices).map_err(|error| match error {
        arrow_schema::ArrowError::OffsetOverflowError(_) => invalid(),
        _ => format_error(error),
    })
}

fn byte_span(offsets: &[i32]) -> usize {
    (offsets[offsets.len() - 1] - offsets[0]) as usize
}

fn select_array(
    arrays: &[&dyn Array],
    indices: &[(usize, usize)],
    share_slices: bool,
) -> Result<ArrayRef> {
    if share_slices
        && let Some(&(source, start)) = indices.first()
        && indices
            .iter()
            .enumerate()
            .all(|(offset, &index)| index == (source, start + offset))
        && let Some(slice) = compact_slice(arrays[source], start, indices.len())?
    {
        return Ok(slice);
    }
    interleave_array(arrays, indices)
}

fn compact_slice(array: &dyn Array, start: usize, len: usize) -> Result<Option<ArrayRef>> {
    let data = array.to_data();
    // Callers have already established index-owned backing. Arrow may report only a custom view's extent.
    if data.buffers().iter().any(|buffer| buffer.capacity() == 0)
        || data.nulls().is_some_and(|nulls| nulls.buffer().capacity() == 0)
    {
        return Ok(None);
    }
    let slice = array.slice(start, len);
    // IPC can carry the rest of a byte through a byte-aligned bitmap slice.
    let compact_bytes = match slice.data_type() {
        DataType::Boolean => {
            let values = arrow::<BooleanArray>(slice.as_ref())?.values();
            (values.offset().is_multiple_of(8) && values.len().is_multiple_of(8))
                .then_some(values.len() / 8)
        }
        DataType::Utf8 => {
            let offsets = arrow::<StringArray>(slice.as_ref())?.value_offsets();
            Some(size_of_val(offsets).saturating_add(byte_span(offsets)))
        }
        DataType::Binary => {
            let offsets = arrow::<BinaryArray>(slice.as_ref())?.value_offsets();
            Some(size_of_val(offsets).saturating_add(byte_span(offsets)))
        }
        DataType::FixedSizeBinary(width) => Some(len.saturating_mul(*width as usize)),
        data_type => data_type
            .primitive_width()
            .map(|width| len.saturating_mul(width)),
    };
    // Allow for Arrow buffer rounding when comparing reported capacity with selected bytes.
    Ok((slice.null_count() == 0
        && compact_bytes.is_some_and(|bytes| {
            slice.get_buffer_memory_size() <= bytes.saturating_mul(2).saturating_add(64)
        }))
    .then_some(slice))
}

#[derive(Debug)]
pub(super) struct Postings(KeyBatch);

impl Postings {
    pub fn build(keys: &[(Bucket, Tuple<'_>, AddressRef<'_>)]) -> Result<Self> {
        let levels = UInt8Array::from(
            keys.iter()
                .map(|(bucket, _, _)| bucket.level as u8)
                .collect::<Vec<_>>(),
        );
        let starts = UInt64Array::from(
            keys.iter()
                .map(|(bucket, _, _)| bucket.start)
                .collect::<Vec<_>>(),
        );
        let iids = rtdb_columnar::build_iids(
            &keys
                .iter()
                .map(|(_, _, address)| address.iid)
                .collect::<Vec<_>>(),
            None,
        )?;
        let bytes = keys.iter().try_fold(0usize, |sum, (_, _, address)| {
            sum.checked_add(address.key.len()).ok_or_else(invalid)
        })?;
        i32::try_from(bytes).map_err(format_error)?;
        let mut identities = BinaryBuilder::with_capacity(keys.len(), bytes);
        for (_, _, address) in keys {
            identities.append_value(address.key);
        }
        let tuples = keys.iter().map(|(_, tuple, _)| *tuple).collect::<Vec<_>>();
        KeyBatch::gather(
            &tuples,
            BatchKind::Posting,
            vec![
                Arc::new(levels),
                Arc::new(starts),
                iids,
                Arc::new(identities.finish()),
            ],
        )
        .map(Self)
    }

    pub fn interleave(rows: &[(&Self, usize)]) -> Result<Self> {
        let selection: Vec<_> = rows.iter().map(|&(postings, row)| (&postings.0, row)).collect();
        let (layouts, arrays) = KeyBatch::interleave(&selection, true)?;
        let batch = RecordBatch::try_new(schema(BatchKind::Posting, &layouts)?, arrays)
            .map_err(format_error)?;
        Ok(Self(KeyBatch::new(
            layouts,
            batch,
            BatchKind::Posting.prefix(),
            true,
        )?))
    }

    pub fn len(&self) -> usize {
        self.0.batch.num_rows()
    }
    pub fn bytes(&self) -> usize {
        self.0.bytes
    }

    pub fn heap_bytes_excluding(&self, backing: &Buffer) -> Result<usize> {
        Ok(self.0.bytes_excluding(backing)? - size_of::<RecordBatch>())
    }

    pub fn at(&self, row: usize) -> (Bucket, Tuple<'_>, AddressRef<'_>) {
        let level =
            arrow::<UInt8Array>(self.0.batch.column(0).as_ref()).expect("validated bucket level");
        let start =
            arrow::<UInt64Array>(self.0.batch.column(1).as_ref()).expect("validated bucket start");
        (
            Bucket {
                level: level.value(row).into(),
                start: start.value(row),
            },
            Tuple::Arrow(&self.0, row),
            AddressRef {
                iid: Iid(fixed(self.0.batch.column(2).as_ref(), row)),
                key: arrow::<BinaryArray>(self.0.batch.column(3).as_ref())
                    .expect("validated entity key")
                    .value(row),
            },
        )
    }

    pub fn plain_column(&self, slot: usize) -> Option<&ArrayRef> {
        self.0.layouts.get(slot)?.plain()?;
        self.0.batch.columns().get(self.0.prefix + slot)
    }

    pub fn stage<'a>(&'a self, header: &'a [u8], suffix_len: usize) -> Result<KeyBatchEncoding<'a>> {
        self.0.stage(header, suffix_len)
    }

    #[cfg(test)]
    fn encode(&self, header: &[u8], suffix: &[u8]) -> Result<Vec<u8>> {
        let encoding = self.stage(header, suffix.len())?;
        let mut bytes = Vec::with_capacity(encoding.encoded_len());
        encoding.append_batch(&mut bytes);
        bytes.extend_from_slice(suffix);
        Ok(bytes)
    }

    pub fn decode(bytes: Buffer, max_rows: usize) -> Result<(Self, Buffer)> {
        let (batch, suffix) = KeyBatch::decode(bytes, max_rows, BatchKind::Posting)?;
        let postings = Self(batch);
        for row in 0..postings.len() {
            let (bucket, _, address) = postings.at(row);
            if (!address.key.is_empty()
                && rtdb_columnar::EntityIdRef::from_handle_bytes(address.key).is_err())
                || bucket.level > 64
                || super::domain::bucket_start(bucket.start.into(), bucket.level) != bucket.start
            {
                return Err(invalid());
            }
        }
        Ok((postings, suffix))
    }
}

#[derive(Clone, Copy)]
pub(super) enum BatchKind {
    Keys,
    Posting,
    Correction,
}

impl BatchKind {
    fn fields(self) -> Vec<Field> {
        match self {
            Self::Keys => Vec::new(),
            Self::Posting => vec![
                Field::new("bucket_level", DataType::UInt8, false),
                Field::new("bucket_start", DataType::UInt64, false),
                Field::new("iid", DataType::FixedSizeBinary(16), false),
                Field::new("entity_key", DataType::Binary, false),
            ],
            Self::Correction => vec![
                Field::new("valid_from", DataType::Int64, false),
                Field::new("valid_to", DataType::Int64, false),
            ],
        }
    }

    fn prefix(self) -> usize {
        match self {
            Self::Keys => 0,
            Self::Posting => 4,
            Self::Correction => 2,
        }
    }
}

pub(super) enum KeyColumn<'a> {
    Missing,
    Typed(Kind, &'a dyn Array),
    Cells(&'a Field, &'a dyn Array),
}

impl<'a> KeyColumn<'a> {
    pub fn new(column: Option<(&'a Field, &'a dyn Array)>) -> Self {
        let Some((field, array)) = column else { return Self::Missing; };
        let kind = if let Some(extension) = field.extension_type_name() {
            match (extension, field.data_type()) {
                ("uuid", DataType::FixedSizeBinary(16)) => Kind::Uuid,
                _ => return Self::Cells(field, array),
            }
        } else {
            match field.data_type() {
                DataType::Union(..) | DataType::RunEndEncoded(..) =>
                    return Self::Cells(field, array),
                DataType::Boolean => Kind::Bool,
                DataType::Decimal128(..) => Kind::Decimal128,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => Kind::Integer,
                DataType::Utf8 => Kind::Text,
                DataType::Binary => Kind::Binary,
                DataType::Date32 | DataType::Date64 => Kind::Day,
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(_)) => Kind::InstantMicros,
                DataType::Float32 | DataType::Float64 => Kind::FloatOrder,
                DataType::Null => Kind::Null,
                _ => Kind::Unordered,
            }
        };
        Self::Typed(kind, array)
    }

    pub fn select(&self, rows: &[u32]) -> Result<SelectedColumn<'a>> {
        let byte_count = |offsets: &[i32], array: &dyn Array| {
            rows.iter().copied().map(|row| row as usize)
                .filter(|&row| array.is_valid(row))
                .fold(0usize, |bytes, row| bytes.saturating_add((offsets[row + 1] - offsets[row]) as usize))
        };
        Ok(match *self {
            Self::Missing => SelectedColumn::Typed(Kind::Null, None, 0),
            Self::Typed(kind, array) => {
                let bytes = match kind {
                    Kind::Text => byte_count(arrow::<StringArray>(array)?.value_offsets(), array),
                    Kind::Binary => byte_count(arrow::<BinaryArray>(array)?.value_offsets(), array),
                    _ => 0,
                };
                SelectedColumn::Typed(kind, Some(array), bytes)
            }
            Self::Cells(field, array) => SelectedColumn::Cells(rows.iter().map(|&row| {
                AtomRef::from_arrow(field, array, row as usize)
            }).collect::<Result<_>>()?),
        })
    }
}

pub(super) enum SelectedColumn<'a> {
    Typed(Kind, Option<&'a dyn Array>, usize),
    Cells(Vec<AtomRef<'a>>),
}

impl SelectedColumn<'_> {
    pub fn payload(&self) -> usize {
        match self {
            Self::Typed(_, _, bytes) => *bytes,
            Self::Cells(cells) => cells.iter().map(AtomRef::retained_bytes).fold(0, usize::saturating_add),
        }
    }

    fn build(self, slot: usize, rows: &arrow_array::UInt32Array) -> Result<(Layout, ArrayRef)> {
        let bytes = self.payload();
        if bytes > i32::MAX as usize { return Err(invalid()); }
        match self {
            Self::Typed(Kind::Null, _, _) => Ok((Layout(Kind::Null.bit()), Arc::new(NullArray::new(rows.len())))),
            Self::Typed(kind, Some(array), _) => {
                let nulls = rows.values().iter().filter(|&&row| array.is_null(row as usize)).count();
                let layout = Layout(if nulls == rows.len() {
                    Kind::Null.bit()
                } else {
                    kind.bit() | if nulls == 0 { 0 } else { Kind::Null.bit() }
                });
                if layout.plain() == Some(Kind::Null) || kind == Kind::Unordered {
                    let values = if layout.plain().is_some() {
                        Arc::new(NullArray::new(rows.len())) as ArrayRef
                    } else {
                        let field = layout.field(slot)?;
                        let DataType::Union(fields, UnionMode::Dense) = field.data_type() else { return Err(invalid()); };
                        let mut counts = [0i32; 2];
                        let (tags, offsets): (Vec<_>, Vec<_>) = rows.values().iter().map(|&row| {
                            let tag = usize::from(array.is_null(row as usize));
                            let offset = counts[tag];
                            counts[tag] += 1;
                            ((Kind::Unordered as i8) + tag as i8, offset)
                        }).unzip();
                        let children = counts.map(|count| Arc::new(NullArray::new(count as usize)) as ArrayRef);
                        Arc::new(UnionArray::try_new(fields.clone(), tags.into(), Some(offsets.into()), children.into()).map_err(format_error)?)
                    };
                    return Ok((layout, values));
                }
                let selected = if matches!(
                    kind,
                    Kind::Integer
                        | Kind::Decimal128
                        | Kind::Day
                        | Kind::FloatOrder
                        | Kind::InstantMicros
                ) {
                    kind.gather_execution(array, rows.values())?
                } else if nulls != 0 {
                    // A source null can hide arbitrary bytes in its physical value buffer.
                    let null = arrow_array::new_null_array(array.data_type(), 1);
                    let indices = rows
                        .values()
                        .iter()
                        .map(|&row| {
                            let row = row as usize;
                            if array.is_null(row) { (1, 0) } else { (0, row) }
                        })
                        .collect::<Vec<_>>();
                    interleave_array(&[array, null.as_ref()], &indices)?
                } else {
                    arrow_select::take::take(array, rows, None).map_err(format_error)?
                };
                Ok((layout, selected))
            }
            Self::Typed(_, None, _) => unreachable!("a missing column has null type"),
            Self::Cells(cells) => {
                let layout = Layout(cells.iter().fold(0, |mask, &cell| mask | Kind::of(cell).bit()));
                let values = layout.build(&layout.field(slot)?, cells, bytes)?;
                Ok((layout, values))
            }
        }
    }
}

impl Kind {
    fn gather_execution(self, array: &dyn Array, rows: &[u32]) -> Result<ArrayRef> {
        use arrow_array::{
            Date32Array, Date64Array, Float32Array, Float64Array, Int8Array, Int16Array,
            Int32Array, UInt16Array, UInt32Array,
        };
        macro_rules! gather {
            ($array:ty, $variant:ident, $convert:expr) => {{
                let values = arrow::<$array>(array)?;
                let convert = $convert;
                self.build(
                    rows.iter().map(|&row| {
                        let row = row as usize;
                        if values.is_null(row) {
                            AtomRef::Null
                        } else {
                            AtomRef::$variant(convert(values.value(row)))
                        }
                    }),
                    0,
                )
            }};
        }
        match self {
            Self::Integer => match array.data_type() {
                DataType::Int8 => gather!(Int8Array, Integer, i128::from),
                DataType::Int16 => gather!(Int16Array, Integer, i128::from),
                DataType::Int32 => gather!(Int32Array, Integer, i128::from),
                DataType::Int64 => gather!(Int64Array, Integer, i128::from),
                DataType::UInt8 => gather!(UInt8Array, Integer, |value| i128::from(value as i8)),
                DataType::UInt16 => gather!(UInt16Array, Integer, |value| i128::from(value as i16)),
                DataType::UInt32 => gather!(UInt32Array, Integer, |value| i128::from(value as i32)),
                DataType::UInt64 => gather!(UInt64Array, Integer, |value| i128::from(value as i64)),
                _ => Err(invalid()),
            },
            Self::Decimal128 => {
                let values = arrow::<Decimal128Array>(array)?;
                let (precision, scale) = (values.precision(), values.scale());
                let mut builder = FixedSizeBinaryBuilder::with_capacity(rows.len(), 18);
                for &row in rows {
                    let row = row as usize;
                    if values.is_null(row) {
                        builder.append_null();
                        continue;
                    }
                    let coefficient = values.value(row);
                    if !super::key::decimal_coefficient_fits(precision, coefficient) {
                        return Err(format_error("Decimal128 coefficient exceeds its path declaration"));
                    }
                    let mut bytes = [precision; 18];
                    bytes[1] = scale as u8;
                    bytes[2..].copy_from_slice(&coefficient.to_be_bytes());
                    builder.append_value(bytes).map_err(format_error)?;
                }
                Ok(Arc::new(builder.finish()))
            }
            Self::Day if array.data_type() == &DataType::Date32 => {
                gather!(Date32Array, Day, i64::from)
            }
            Self::Day => gather!(Date64Array, Day, |millis: i64| millis
                .div_euclid(rtdb_types::java_date::MILLIS_PER_DAY)),
            Self::InstantMicros => gather!(
                TimestampMicrosecondArray,
                InstantMicros,
                std::convert::identity
            ),
            Self::FloatOrder if array.data_type() == &DataType::Float32 => {
                gather!(Float32Array, FloatOrder, |value| super::key::float_order(
                    f64::from(value)
                ))
            }
            Self::FloatOrder => gather!(Float64Array, FloatOrder, super::key::float_order),
            _ => Err(invalid()),
        }
    }
}

#[derive(Debug)]
pub(super) struct KeyBatch {
    layouts: Vec<Layout>,
    batch: RecordBatch,
    prefix: usize,
    bytes: usize,
    share_slices: bool,
}

pub(super) struct KeyBatchEncoding<'a> {
    header: &'a [u8],
    lengths: [u32; 3],
    layouts: &'a [Layout],
    data: EncodedData,
    body_start: usize,
    len: usize,
}

impl KeyBatchEncoding<'_> {
    pub fn encoded_len(&self) -> usize {
        self.len
    }

    pub fn append_batch(self, bytes: &mut Vec<u8>) {
        let start = bytes.len();
        bytes.extend_from_slice(self.header);
        for length in self.lengths {
            bytes.extend_from_slice(&length.to_le_bytes());
        }
        for layout in self.layouts {
            bytes.extend_from_slice(&layout.0.to_le_bytes());
        }
        bytes.extend_from_slice(&self.data.ipc_message);
        bytes.resize(start + self.body_start, 0);
        bytes.extend_from_slice(&self.data.arrow_data);
    }
}

impl KeyBatch {
    pub fn gather(tuples: &[Tuple<'_>], kind: BatchKind, mut prefix: Vec<ArrayRef>) -> Result<Self> {
        let selection = tuples.iter().map(|tuple| match tuple {
            Tuple::Arrow(keys, row) => Some((*keys, *row)),
            _ => None,
        }).collect::<Option<Vec<_>>>();
        if let Some(selection) = selection {
            let (layouts, keys) = Self::interleave(&selection, false)?;
            prefix.extend(keys);
            let batch = RecordBatch::try_new(schema(kind, &layouts)?, prefix).map_err(format_error)?;
            return Self::new(layouts, batch, kind.prefix(), true);
        }
        Self::build(tuples, kind, prefix)
    }

    fn interleave(
        selection: &[(&Self, usize)],
        prefix: bool,
    ) -> Result<(Vec<Layout>, Vec<ArrayRef>)> {
        let &(first, _) = selection.first().ok_or_else(invalid)?;
        if first.layouts.is_empty() {
            return Err(invalid());
        }
        let mut sources = Vec::new();
        let mut positions = HashMap::new();
        let mut indices = Vec::with_capacity(selection.len());
        for &(batch, row) in selection {
            if batch.layouts.len() != first.layouts.len() || row >= batch.batch.num_rows() {
                return Err(invalid());
            }
            let source = *positions.entry(std::ptr::from_ref(batch)).or_insert_with(|| {
                sources.push(batch);
                sources.len() - 1
            });
            indices.push((source, row));
        }
        let share_slices = sources.iter().all(|source| source.share_slices);
        let mut output = Vec::new();
        if prefix {
            for column in 0..first.prefix {
                let values = sources.iter().map(|source| source.batch.column(column).as_ref()).collect::<Vec<_>>();
                output.push(select_array(&values, &indices, share_slices)?);
            }
        }
        let mut layouts = Vec::with_capacity(first.layouts.len());
        for slot in 0..first.layouts.len() {
            let values = sources
                .iter()
                .map(|source| {
                    (
                        source.layouts[slot],
                        source.batch.column(slot + source.prefix).as_ref(),
                    )
                })
                .collect::<Vec<_>>();
            let (layout, array) = Layout::interleave(&values, &indices, slot, share_slices)?;
            layouts.push(layout);
            output.push(array);
        }
        Ok((layouts, output))
    }

    pub fn project(columns: Vec<SelectedColumn<'_>>, rows: Vec<u32>) -> Result<Self> {
        if columns.is_empty() || rows.is_empty() {
            return Err(invalid());
        }
        let rows = arrow_array::UInt32Array::from(rows);
        let mut layouts = Vec::with_capacity(columns.len());
        let mut arrays = Vec::with_capacity(columns.len());
        for (slot, column) in columns.into_iter().enumerate() {
            let (layout, array) = column.build(slot, &rows)?;
            layouts.push(layout);
            arrays.push(array);
        }
        let batch = RecordBatch::try_new(schema(BatchKind::Keys, &layouts)?, arrays).map_err(format_error)?;
        Self::new(layouts, batch, 0, true)
    }

    pub fn build(tuples: &[Tuple<'_>], kind: BatchKind, mut arrays: Vec<ArrayRef>) -> Result<Self> {
        let width = tuples.first().ok_or_else(invalid)?.len();
        if width == 0
            || tuples.iter().any(|tuple| tuple.len() != width)
            || arrays.len() != kind.prefix()
        {
            return Err(invalid());
        }
        let layouts = (0..width)
            .map(|slot| {
                Layout(
                    tuples
                        .iter()
                        .fold(0, |mask, tuple| mask | Kind::of(tuple.cell(slot)).bit()),
                )
            })
            .collect::<Vec<_>>();
        let schema = schema(kind, &layouts)?;
        for (slot, layout) in layouts.iter().enumerate() {
            let cells: Vec<_> = tuples.iter().map(|tuple| tuple.cell(slot)).collect();
            let bytes = cells
                .iter()
                .map(AtomRef::retained_bytes)
                .fold(0usize, usize::saturating_add);
            if bytes > i32::MAX as usize {
                return Err(invalid());
            }
            arrays.push(layout.build(schema.field(slot + kind.prefix()), cells, bytes)?);
        }
        let batch = RecordBatch::try_new(schema, arrays).map_err(format_error)?;
        Self::new(layouts, batch, kind.prefix(), true)
    }

    pub fn build_with_prefix(
        tuples: &[Tuple<'_>],
        mut fields: Vec<Field>,
        mut arrays: Vec<ArrayRef>,
    ) -> Result<Self> {
        let width = tuples.first().ok_or_else(invalid)?.len();
        if tuples.iter().any(|tuple| tuple.len() != width) || arrays.len() != fields.len() {
            return Err(invalid());
        }
        let prefix = fields.len();
        let mut layouts = Vec::with_capacity(width);
        for slot in 0..width {
            let cells = tuples
                .iter()
                .map(|tuple| tuple.cell(slot))
                .collect::<Vec<_>>();
            let layout = Layout(
                cells
                    .iter()
                    .fold(0, |mask, &cell| mask | Kind::of(cell).bit()),
            );
            let name = format!("{slot}_{:x}", layout.0);
            let field = layout.field(slot)?.with_name(name);
            let bytes = cells.iter().map(AtomRef::retained_bytes).fold(0, usize::saturating_add);
            arrays.push(layout.build(&field, cells, bytes)?);
            fields.push(field);
            layouts.push(layout);
        }
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(format_error)?;
        Self::new(layouts, batch, prefix, false)
    }

    pub fn from_batch(batch: RecordBatch, prefix: &[Field]) -> Result<Self> {
        let fields = batch.schema_ref().fields();
        if batch.num_rows() == 0
            || fields.len() < prefix.len()
            || fields
                .iter()
                .zip(prefix)
                .any(|(actual, expected)| actual.as_ref() != expected)
            || batch
                .columns()
                .iter()
                .zip(prefix)
                .any(|(column, field)| !field.is_nullable() && column.null_count() != 0)
        {
            return Err(invalid());
        }
        let layouts = fields[prefix.len()..]
            .iter()
            .enumerate()
            .map(|(slot, field)| {
                let (_, bits) = field.name().rsplit_once('_').ok_or_else(invalid)?;
                let mask = u16::from_str_radix(bits, 16).map_err(format_error)?;
                let layout = Layout(mask);
                if mask == 0
                    || mask & !VALID_LAYOUT_BITS != 0
                    || &layout.field(slot)?.with_name(format!("{slot}_{mask:x}")) != field.as_ref()
                {
                    return Err(invalid());
                }
                Ok(layout)
            })
            .collect::<Result<Vec<_>>>()?;
        Self::new(layouts, batch, prefix.len(), false)
    }

    fn new(
        layouts: Vec<Layout>,
        batch: RecordBatch,
        prefix: usize,
        share_slices: bool,
    ) -> Result<Self> {
        let bytes = rtdb_columnar::shared_allocation_size(&batch)?
            .saturating_add(layouts.len().saturating_mul(256));
        Ok(Self {
            layouts,
            batch,
            prefix,
            bytes,
            share_slices,
        })
    }

    pub fn retained_bytes(&self) -> usize {
        self.bytes
    }

    pub fn bytes_excluding(&self, backing: &Buffer) -> Result<usize> {
        Ok(rtdb_columnar::shared_allocation_size_excluding(&self.batch, backing)?
            .saturating_add(self.layouts.len().saturating_mul(256)))
    }

    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    // One schema-bound IPC record batch. The short layout prefix identifies its typed columns.
    pub fn stage<'a>(&'a self, header: &'a [u8], suffix_len: usize) -> Result<KeyBatchEncoding<'a>> {
        let options = IpcWriteOptions::try_new(ALIGNMENT, false, MetadataVersion::V5)
            .map_err(format_error)?;
        let (dictionaries, data) = IpcDataGenerator::default()
            .encode(
                &self.batch,
                &mut DictionaryTracker::new(false),
                &options,
                &mut CompressionContext::default(),
            )
            .map_err(format_error)?;
        if !dictionaries.is_empty() {
            return Err(invalid());
        }
        let lengths = [
            u32::try_from(self.layouts.len()).map_err(format_error)?,
            u32::try_from(data.ipc_message.len()).map_err(format_error)?,
            u32::try_from(suffix_len).map_err(format_error)?,
        ];
        let layouts_bytes = self.layouts.len()
            .checked_mul(size_of::<u16>())
            .ok_or_else(invalid)?;
        let body_start = header.len()
            .checked_add(size_of_val(&lengths))
            .and_then(|len| len.checked_add(layouts_bytes))
            .and_then(|len| len.checked_add(data.ipc_message.len()))
            .and_then(|len| len.checked_next_multiple_of(ALIGNMENT))
            .ok_or_else(invalid)?;
        let len = body_start
            .checked_add(data.arrow_data.len())
            .and_then(|len| len.checked_add(suffix_len))
            .filter(|&len| len <= isize::MAX as usize)
            .ok_or_else(invalid)?;
        Ok(KeyBatchEncoding {
            header,
            lengths,
            layouts: &self.layouts,
            data,
            body_start,
            len,
        })
    }

    pub fn decode(bytes: Buffer, max_rows: usize, kind: BatchKind) -> Result<(Self, Buffer)> {
        let length = |offset| -> Result<usize> {
            let raw = bytes.get(offset..offset + 4).ok_or_else(invalid)?;
            Ok(u32::from_le_bytes(raw.try_into().map_err(format_error)?) as usize)
        };
        let width = length(0)?;
        let layouts_bytes = width.checked_mul(size_of::<u16>()).ok_or_else(invalid)?;
        let layouts_end = 12usize.checked_add(layouts_bytes).ok_or_else(invalid)?;
        let message_end = layouts_end.checked_add(length(4)?).ok_or_else(invalid)?;
        let body_start = message_end
            .checked_next_multiple_of(ALIGNMENT)
            .ok_or_else(invalid)?;
        let body_end = bytes.len().checked_sub(length(8)?).ok_or_else(invalid)?;
        if body_start > body_end || width == 0 {
            return Err(invalid());
        }
        let layouts = bytes[12..layouts_end]
            .chunks_exact(size_of::<u16>())
            .map(|raw| {
                let mask = u16::from_le_bytes([raw[0], raw[1]]);
                if mask == 0 || mask & !VALID_LAYOUT_BITS != 0 {
                    Err(invalid())
                } else {
                    Ok(Layout(mask))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let message = root_as_message(&bytes[layouts_end..message_end]).map_err(format_error)?;
        let record = message.header_as_record_batch().ok_or_else(invalid)?;
        let rows = usize::try_from(record.length()).map_err(format_error)?;
        let body_len = body_end - body_start;
        if rows == 0
            || rows > max_rows
            || message.version() != MetadataVersion::V5
            || record.compression().is_some()
            || record
                .variadicBufferCounts()
                .is_some_and(|counts| !counts.is_empty())
            || usize::try_from(message.bodyLength()).ok() != Some(body_len)
        {
            return Err(invalid());
        }
        let nodes = record.nodes().ok_or_else(invalid)?;
        let buffers = record.buffers().ok_or_else(invalid)?;
        if nodes.iter().any(|node| {
            !(0..=record.length()).contains(&node.length())
                || !(0..=node.length()).contains(&node.null_count())
        }) || buffers.iter().any(|buffer| {
            let end = buffer
                .offset()
                .checked_add(buffer.length())
                .and_then(|end| usize::try_from(end).ok());
            buffer.offset() < 0 || buffer.length() < 0 || end.is_none_or(|end| end > body_len)
        }) {
            return Err(invalid());
        }
        let mut nodes = nodes.iter();
        let mut buffers = buffers.iter();
        for (index, layout) in std::iter::repeat_n(None, kind.prefix())
            .chain(layouts.iter().copied().map(Some))
            .enumerate()
        {
            let node = nodes.next().ok_or_else(invalid)?;
            if node.length() != record.length() {
                return Err(invalid());
            }
            let Some(layout) = layout else {
                let count = if matches!(kind, BatchKind::Posting) && index == 3 {
                    3
                } else {
                    2
                };
                for _ in 0..count {
                    buffers.next().ok_or_else(invalid)?;
                }
                continue;
            };
            if let Some(kind) = layout.plain() {
                for _ in 0..kind.buffer_count() {
                    buffers.next().ok_or_else(invalid)?;
                }
            } else {
                for min_len in [rows, rows * size_of::<i32>()] {
                    let buffer = buffers.next().ok_or_else(invalid)?;
                    if buffer.length() < min_len as i64 || buffer.offset() % ALIGNMENT as i64 != 0 {
                        return Err(invalid());
                    }
                }
                for kind in layout.kinds() {
                    nodes.next().ok_or_else(invalid)?;
                    for _ in 0..kind.buffer_count() {
                        buffers.next().ok_or_else(invalid)?;
                    }
                }
            }
        }
        if nodes.next().is_some() || buffers.next().is_some() {
            return Err(invalid());
        }
        let body = bytes.slice_with_length(body_start, body_len);
        let body = if body.as_ptr().align_offset(ALIGNMENT) == 0 {
            body
        } else {
            Buffer::from_slice_ref(body.as_ref())
        };
        let batch = read_record_batch(
            &body,
            record,
            schema(kind, &layouts)?,
            &HashMap::new(),
            None,
            &MetadataVersion::V5,
        )
        .map_err(format_error)?;
        if batch.num_rows() != rows
            || batch.columns()[..kind.prefix()]
                .iter()
                .any(|array| array.null_count() != 0)
        {
            return Err(invalid());
        }
        let keys = Self::new(layouts, batch, kind.prefix(), false)?;
        Ok((keys, bytes.slice(body_end)))
    }
}

fn schema(kind: BatchKind, layouts: &[Layout]) -> Result<Arc<Schema>> {
    let mut fields = kind.fields();
    for (slot, layout) in layouts.iter().enumerate() {
        fields.push(layout.field(slot)?);
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn fixed(array: &dyn Array, row: usize) -> [u8; 16] {
    arrow::<FixedSizeBinaryArray>(array)
        .expect("validated fixed-width column")
        .value(row)
        .try_into()
        .expect("validated sixteen-byte column")
}

fn invalid() -> rtdb_types::Anomaly {
    format_error("Invalid ordered index Arrow keys")
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Tuple<'a> {
    Owned(&'a [Atom]),
    #[cfg(test)]
    Cells(&'a [AtomRef<'a>]),
    Nulls(usize),
    Arrow(&'a KeyBatch, usize),
}

impl<'a> Tuple<'a> {
    pub fn len(self) -> usize {
        match self {
            Self::Owned(atoms) => atoms.len(),
            #[cfg(test)]
            Self::Cells(cells) => cells.len(),
            Self::Nulls(width) => width,
            Self::Arrow(postings, _) => postings.layouts.len(),
        }
    }

    pub fn get(self, slot: usize) -> Option<AtomRef<'a>> {
        match self {
            Self::Owned(atoms) => atoms.get(slot).map(Atom::borrowed),
            #[cfg(test)]
            Self::Cells(cells) => cells.get(slot).copied(),
            Self::Nulls(width) => (slot < width).then_some(AtomRef::Null),
            Self::Arrow(postings, row) => postings.layouts.get(slot).map(|layout| {
                layout.cell(postings.batch.column(slot + postings.prefix).as_ref(), row)
            }),
        }
    }

    pub fn iter(self) -> impl ExactSizeIterator<Item = AtomRef<'a>> {
        (0..self.len()).map(move |slot| self.cell(slot))
    }

    pub fn cell(self, slot: usize) -> AtomRef<'a> {
        self.get(slot).expect("slot is in the posting tuple")
    }

    pub fn plain_column(self, slot: usize, kind: Kind) -> Option<&'a dyn Array> {
        let Self::Arrow(postings, _) = self else { return None; };
        if postings.layouts.get(slot)?.plain()? != kind {
            return None;
        }
        Some(postings.batch.column(postings.prefix + slot).as_ref())
    }

    pub fn compare(self, other: Tuple<'_>) -> Ordering {
        self.iter()
            .zip(other.iter())
            .find_map(|(left, right)| {
                let order = left.cmp(&right);
                (order != Ordering::Equal).then_some(order)
            })
            .unwrap_or_else(|| self.len().cmp(&other.len()))
    }
}

#[derive(Debug)]
pub(super) struct ProjectedKeys {
    batch: KeyBatch,
    _memory: Arc<MemoryReservation>,
}

impl ProjectedKeys {
    pub fn allocation_bytes() -> usize {
        size_of::<Self>() - size_of::<RecordBatch>() + 2 * size_of::<usize>()
    }

    pub fn new(batch: KeyBatch, memory: Arc<MemoryReservation>) -> Arc<Self> {
        Arc::new(Self {
            batch,
            _memory: memory,
        })
    }

    pub fn row(self: &Arc<Self>, row: usize) -> KeyRow {
        debug_assert!(row < self.batch.batch.num_rows());
        KeyRow::Arrow(self.clone(), row)
    }
}

#[derive(Clone, Debug)]
pub(super) enum KeyRow {
    Fence(Arc<[Atom]>),
    Arrow(Arc<ProjectedKeys>, usize),
}

impl KeyRow {
    pub fn view(&self) -> Tuple<'_> {
        match self {
            Self::Fence(atoms) => Tuple::Owned(atoms),
            Self::Arrow(keys, row) => Tuple::Arrow(&keys.batch, *row),
        }
    }

    pub fn bytes(&self) -> usize {
        self.view()
            .iter()
            .map(|cell| size_of::<Atom>() + cell.retained_bytes())
            .sum()
    }
}

impl PartialEq for KeyRow {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for KeyRow {}
impl PartialOrd for KeyRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for KeyRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.view().compare(other.view())
    }
}

impl Serialize for KeyRow {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let tuple = self.view();
        let mut sequence = serializer.serialize_seq(Some(tuple.len()))?;
        for cell in tuple.iter() {
            sequence.serialize_element(&cell)?;
        }
        sequence.end()
    }
}
impl<'de> Deserialize<'de> for KeyRow {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Arc::deserialize(deserializer).map(Self::Fence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_key_projection_matches_the_scalar_codec_for_selected_rows() {
        use arrow_array::{Date64Array, Float64Array};
        let integers = UInt64Array::from(vec![Some(1), Some(u64::MAX), None, Some(1 << 63)]);
        let days = Date64Array::from(vec![Some(0), Some(-1), None, Some(-86_400_001)]);
        let floats = Float64Array::from(vec![Some(0.0), Some(-0.0), None, Some(f64::from_bits(0xfff8_0000_0000_0123))]);
        let text = StringArray::from(vec![Some("unused"), Some("abc"), None, Some("def")]);
        let arrays: [&dyn Array; 4] = [&integers, &days, &floats, &text];
        let fields = arrays.iter().map(|array| Field::new("test", array.data_type().clone(), true)).collect::<Vec<_>>();
        let selection = vec![3, 2, 1, 0, 3];
        let columns = fields.iter().zip(arrays).map(|(field, array)| {
            let column = KeyColumn::new(Some((field, array)));
            assert!(matches!(column, KeyColumn::Typed(..)));
            column.select(&selection).unwrap()
        }).collect();
        let scalar = selection.iter().map(|&row| {
            fields.iter().zip(arrays).map(|(field, array)| AtomRef::from_arrow(field, array, row as usize).unwrap())
                .collect::<Vec<_>>()
        }).collect::<Vec<_>>();
        let typed = KeyBatch::project(columns, selection).unwrap();
        let legacy = KeyBatch::build(&scalar.iter().map(|row| Tuple::Cells(row)).collect::<Vec<_>>(), BatchKind::Keys, Vec::new()).unwrap();
        for row in 0..typed.batch().num_rows() {
            assert_eq!(Tuple::Arrow(&typed, row).compare(Tuple::Arrow(&legacy, row)), Ordering::Equal);
        }
        assert_eq!(
            typed.encode(b"", b"").unwrap(),
            legacy.encode(b"", b"").unwrap()
        );
        let independently_gathered = KeyBatch::gather(
            &[Tuple::Arrow(&typed, 0), Tuple::Arrow(&typed, 2)],
            BatchKind::Correction, vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(Int64Array::from(vec![3, 4]))],
        ).unwrap();
        assert_eq!(Tuple::Arrow(&independently_gathered, 1).compare(Tuple::Arrow(&legacy, 2)), Ordering::Equal);
    }

    #[test]
    fn source_projection_copies_contiguous_scalars_and_preserves_wire_bytes() {
        let text = StringArray::from([["skip"; 8], ["word"; 8]].concat());
        let binary = BinaryArray::from([[&b"skip"[..]; 8], [&b"data"[..]; 8]].concat());
        let uuid = FixedSizeBinaryArray::try_from_iter(
            [[[0_u8; 16]; 8], [[1; 16]; 8]].into_iter().flatten(),
        )
        .unwrap();
        let bits = BooleanArray::from([[false; 8], [true; 8]].concat());
        let times =
            TimestampMicrosecondArray::from([[0; 8], [-7; 8]].concat()).with_timezone("+01:00");
        let arrays: [&dyn Array; 5] = [&text, &binary, &uuid, &bits, &times];
        let mut fields = arrays.map(|array| Field::new("key", array.data_type().clone(), false));
        fields[2] = fields[2].clone().with_metadata(HashMap::from([(
            rtdb_columnar::EXTENSION_NAME_KEY.into(),
            "uuid".into(),
        )]));
        let rows: Vec<u32> = (8..16).collect();
        let columns = fields
            .iter()
            .zip(arrays)
            .map(|(field, array)| KeyColumn::new(Some((field, array))).select(&rows).unwrap())
            .collect();
        let projected = KeyBatch::project(columns, rows).unwrap();
        for (slot, source) in arrays.into_iter().enumerate() {
            let source_data = source.to_data();
            let output_data = projected.batch().column(slot).to_data();
            assert!(
                source_data
                    .buffers()
                    .iter()
                    .zip(output_data.buffers())
                    .all(|(source, output)| { source.data_ptr() != output.data_ptr() })
            );
        }
        let canonical = [
            AtomRef::Text("word"),
            AtomRef::Binary(b"data"),
            AtomRef::Uuid([1; 16]),
            AtomRef::Bool(true),
            AtomRef::InstantMicros(-7),
        ];
        let copied =
            KeyBatch::build(&[Tuple::Cells(&canonical); 8], BatchKind::Keys, vec![]).unwrap();
        assert_eq!(
            projected.encode(b"", b"").unwrap(),
            copied.encode(b"", b"").unwrap()
        );
        let tuples = (0..8)
            .map(|row| Tuple::Arrow(&projected, row))
            .collect::<Vec<_>>();
        let rebuilt = KeyBatch::gather(&tuples, BatchKind::Keys, vec![]).unwrap();
        let text_data = rebuilt.batch().column(0).to_data();
        assert_ne!(text_data.buffers()[1].data_ptr(), text.values().data_ptr());
        assert_eq!(
            rebuilt.encode(b"", b"").unwrap(),
            copied.encode(b"", b"").unwrap()
        );
    }

    #[test]
    fn source_projection_releases_a_nonempty_custom_view_owner() -> Result<()> {
        use arrow_buffer::OffsetBuffer;
        use rtdb_columnar::MemoryBudget;

        struct Source {
            bytes: Vec<u8>,
            _allocation: Arc<MemoryReservation>,
        }

        impl AsRef<[u8]> for Source {
            fn as_ref(&self) -> &[u8] { &self.bytes[..3] }
        }

        let memory = MemoryBudget::new(4 << 20);
        let size = 1 << 20;
        let allocation = memory.reserve(size)?;
        let mut bytes = vec![0; size];
        bytes[..3].copy_from_slice(b"key");
        let source = Buffer::from(Bytes::from_owner(Source { bytes, _allocation: allocation }));
        assert_eq!(source.capacity(), 3);
        let strings = StringArray::new(OffsetBuffer::new(vec![0, 3].into()), source, None);
        let field = Field::new("key", DataType::Utf8, false);
        let rows = vec![0];
        let column = KeyColumn::new(Some((&field, &strings))).select(&rows)?;
        let projected = KeyBatch::project(vec![column], rows)?;
        assert_eq!(memory.used_bytes(), size);
        drop(strings);
        assert_eq!(memory.used_bytes(), 0);
        assert!(matches!(Tuple::Arrow(&projected, 0).cell(0), AtomRef::Text("key")));
        Ok(())
    }

    #[test]
    fn source_projection_copies_oversized_buffers_and_partial_boolean_bytes() {
        let text = StringArray::from(vec!["kept".to_owned(), "x".repeat(4096)]);
        let bits = BooleanArray::from(vec![true; 8]);
        let arrays: [&dyn Array; 2] = [&text, &bits];
        let rows = vec![0];
        let fields = arrays.map(|array| Field::new("key", array.data_type().clone(), false));
        let columns = fields
            .iter()
            .zip(arrays)
            .map(|(field, array)| KeyColumn::new(Some((field, array))).select(&rows).unwrap())
            .collect();
        let projected = KeyBatch::project(columns, rows).unwrap();
        let canonical = [AtomRef::Text("kept"), AtomRef::Bool(true)];
        let copied = KeyBatch::build(&[Tuple::Cells(&canonical)], BatchKind::Keys, vec![]).unwrap();
        assert_eq!(
            projected.encode(b"", b"").unwrap(),
            copied.encode(b"", b"").unwrap()
        );
        assert!(projected.share_slices);
        assert!(projected.batch().column(0).get_buffer_memory_size() < 4096);
    }

    #[test]
    fn projection_discards_bytes_under_nulls_before_retaining_selected_tuples() {
        use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};

        let nulls = NullBuffer::from(vec![true, false, true]);
        let text = StringArray::new(
            OffsetBuffer::new(vec![0, 3, 9, 12].into()),
            Buffer::from(b"onehiddentwo".to_vec()),
            Some(nulls.clone()),
        );
        let bools = BooleanArray::new(BooleanBuffer::new_set(3), Some(nulls.clone()));
        let floats = arrow_array::Float64Array::new(vec![-0.0, 8.0, 2.0].into(), Some(nulls));
        let arrays: [&dyn Array; 3] = [&text, &bools, &floats];
        let fields = arrays.map(|array| Field::new("key", array.data_type().clone(), true));
        let rows = vec![0, 1];
        let columns = fields
            .iter()
            .zip(arrays)
            .map(|(field, array)| KeyColumn::new(Some((field, array))).select(&rows).unwrap())
            .collect();
        let projected = KeyBatch::project(columns, rows).unwrap();
        let valid = [
            AtomRef::Text("one"),
            AtomRef::Bool(true),
            AtomRef::FloatOrder(super::super::key::float_order(-0.0)),
        ];
        let null = [AtomRef::Null; 3];
        let scalar = KeyBatch::build(
            &[Tuple::Cells(&valid), Tuple::Cells(&null)],
            BatchKind::Keys,
            vec![],
        )
        .unwrap();
        assert_eq!(
            projected.encode(b"", b"").unwrap(),
            scalar.encode(b"", b"").unwrap()
        );
    }

    #[test]
    fn interleaving_correction_keys_preserves_dense_tags_and_canonical_null_bytes() {
        let mixed = [
            [AtomRef::Text("one"), AtomRef::Integer(1)],
            [AtomRef::Integer(2), AtomRef::Null],
            [AtomRef::Null, AtomRef::Integer(3)],
        ];
        let batch = KeyBatch::build(&mixed.iter().map(|row| Tuple::Cells(row)).collect::<Vec<_>>(),
            BatchKind::Keys, Vec::new()).unwrap();
        for selection in [&[2, 1, 0][..], &[2, 0][..], &[1][..]] {
            let keys = selection.iter().map(|&row| Tuple::Arrow(&batch, row)).collect::<Vec<_>>();
            let scalar = selection.iter().map(|&row| Tuple::Cells(&mixed[row])).collect::<Vec<_>>();
            let prefix: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(0..selection.len() as i64)),
                Arc::new(Int64Array::from_iter_values(1..=selection.len() as i64))];
            let typed = KeyBatch::gather(&keys, BatchKind::Correction, prefix.clone()).unwrap();
            let canonical = KeyBatch::build(&scalar, BatchKind::Correction, prefix).unwrap();
            assert_eq!(typed.encode(b"", b"").unwrap(), canonical.encode(b"", b"").unwrap());
        }
    }

    #[test]
    fn correction_gather_reconciles_plain_and_union_legs_by_kind() {
        let mixed = [
            [
                AtomRef::Binary(b"other"),
                AtomRef::InstantMicros(5),
                AtomRef::Unordered,
            ],
            [AtomRef::Text("one"), AtomRef::Day(5), AtomRef::Null],
        ];
        let plain = [
            AtomRef::Text("two"),
            AtomRef::InstantMicros(6),
            AtomRef::Unordered,
        ];
        let mixed_keys = KeyBatch::build(
            &[Tuple::Cells(&mixed[0]), Tuple::Cells(&mixed[1])],
            BatchKind::Keys,
            vec![],
        )
        .unwrap();
        let plain_keys = KeyBatch::build(&[Tuple::Cells(&plain)], BatchKind::Keys, vec![]).unwrap();
        let prefix: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 1, 2])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ];
        let typed = KeyBatch::gather(
            &[
                Tuple::Arrow(&mixed_keys, 1),
                Tuple::Arrow(&plain_keys, 0),
                Tuple::Arrow(&mixed_keys, 1),
            ],
            BatchKind::Correction,
            prefix.clone(),
        )
        .unwrap();
        let canonical = KeyBatch::build(
            &[
                Tuple::Cells(&mixed[1]),
                Tuple::Cells(&plain),
                Tuple::Cells(&mixed[1]),
            ],
            BatchKind::Correction,
            prefix,
        )
        .unwrap();
        assert_eq!(
            typed.encode(b"", b"").unwrap(),
            canonical.encode(b"", b"").unwrap()
        );
    }

    #[test]
    fn contiguous_posting_selection_retains_values_and_writes_canonical_bytes() {
        let cells = [
            [AtomRef::Text("before"), AtomRef::Bool(true)],
            [AtomRef::Text("one"), AtomRef::Bool(true)],
            [AtomRef::Text("two"), AtomRef::Bool(false)],
            [AtomRef::Text("after"), AtomRef::Bool(true)],
        ];
        let keys = cells
            .iter()
            .enumerate()
            .map(|(row, tuple)| {
                (
                    Bucket {
                        level: 0,
                        start: row as u64,
                    },
                    Tuple::Cells(tuple),
                    AddressRef {
                        iid: Iid::MIN,
                        key: b"",
                    },
                )
            })
            .collect::<Vec<_>>();
        let source = Postings::build(&keys).unwrap();
        let selected = Postings::interleave(&[(&source, 1), (&source, 2)]).unwrap();
        assert_eq!(selected.at(0).1.cell(0), AtomRef::Text("one"));
        let AtomRef::Text(selected_text) = selected.at(0).1.cell(0) else {
            unreachable!()
        };
        let AtomRef::Text(source_text) = source.at(1).1.cell(0) else {
            unreachable!()
        };
        assert_eq!(selected_text.as_ptr(), source_text.as_ptr());
        assert!(selected.bytes() >= source.plain_column(0).unwrap().get_buffer_memory_size());
        drop(source);
        let canonical = Postings::build(&keys[1..3]).unwrap();
        assert_eq!(
            selected.encode(b"", b"").unwrap(),
            canonical.encode(b"", b"").unwrap()
        );
    }

    #[test]
    fn interleaving_posting_edits_preserves_the_exact_leaf_bytes() {
        let first = [
            (Bucket { level: 0, start: 1 }, [AtomRef::Text("one"), AtomRef::Integer(1)], Iid([1; 16])),
            (Bucket { level: 0, start: 3 }, [AtomRef::Text("three"), AtomRef::Integer(3)], Iid([3; 16])),
        ];
        let edit = [(
            Bucket { level: 0, start: 2 },
            [AtomRef::Binary(b"two"), AtomRef::Null],
            Iid([2; 16]),
        )];
        fn key<'a>(
            (bucket, tuple, iid): &'a (Bucket, [AtomRef<'static>; 2], Iid),
        ) -> (Bucket, Tuple<'a>, AddressRef<'a>) {
            (
                *bucket,
                Tuple::Cells(tuple),
                AddressRef {
                    iid: *iid,
                    key: b"\0hello",
                },
            )
        }
        let first_keys = first.iter().map(key).collect::<Vec<_>>();
        let edit_keys = edit.iter().map(key).collect::<Vec<_>>();
        let existing = Postings::build(&first_keys).unwrap();
        let edits = Postings::build(&edit_keys).unwrap();
        let result = Postings::interleave(&[(&existing, 0), (&edits, 0), (&existing, 1)]).unwrap();
        let scalar = Postings::build(&[first_keys[0], edit_keys[0], first_keys[1]]).unwrap();
        assert_eq!(result.encode(b"header", b"suffix").unwrap(), scalar.encode(b"header", b"suffix").unwrap());
    }

    #[test]
    fn a_schema_bound_batch_round_trips_ordered_mixed_cells_with_its_suffix() {
        let bucket = Bucket {
            level: 64,
            start: 0,
        };
        let rows = [
            [
                Atom::Bool(false),
                Atom::Text("prefix".into()),
                Atom::Unordered,
            ],
            [
                Atom::Integer(i64::MIN.into()),
                Atom::Text("prefix".into()),
                Atom::Null,
            ],
            [
                Atom::Integer(u64::MAX.into()),
                Atom::Text("prefix".into()),
                Atom::Unordered,
            ],
            [Atom::Text("a".into()), Atom::Null, Atom::Null],
            [
                Atom::Binary(vec![0, 255]),
                Atom::Text("x".into()),
                Atom::Unordered,
            ],
            [Atom::Uuid([255; 16]), Atom::Text("x".into()), Atom::Null],
            [Atom::Day(-1), Atom::Text("x".into()), Atom::Unordered],
            [Atom::InstantMicros(-1), Atom::Text("x".into()), Atom::Null],
            [
                Atom::FloatOrder(u64::MAX),
                Atom::Text("x".into()),
                Atom::Null,
            ],
            [Atom::Unordered, Atom::Text("x".into()), Atom::Null],
            [Atom::Null, Atom::Text("x".into()), Atom::Unordered],
        ];
        let keys: Vec<_> = rows
            .iter()
            .enumerate()
            .map(|(row, cells)| {
                (
                    bucket,
                    Tuple::Owned(cells),
                    AddressRef {
                        iid: Iid([row as u8; 16]),
                        key: &[],
                    },
                )
            })
            .collect();
        let encoded = Postings::build(&keys)
            .unwrap()
            .encode(&[], b"suffix")
            .unwrap();
        let mut unaligned = vec![0];
        unaligned.extend_from_slice(&encoded);
        let (postings, suffix) = Postings::decode(Buffer::from_vec(unaligned).slice(1), 64).unwrap();
        assert_eq!(suffix.as_ref(), b"suffix");
        assert_eq!(postings.len(), rows.len());
        for (row, &(bucket, tuple, iid)) in keys.iter().enumerate() {
            let (actual_bucket, actual, actual_iid) = postings.at(row);
            assert_eq!(actual_bucket, bucket);
            assert_eq!(actual_iid.iid, iid.iid);
            assert_eq!(actual.compare(tuple), Ordering::Equal);
            if row > 0 {
                assert_eq!(postings.at(row - 1).1.compare(actual), Ordering::Less);
            }
        }
        assert_eq!(postings.encode(&[], b"suffix").unwrap(), encoded);
    }

    #[test]
    fn an_aligned_leaf_borrows_its_text_from_the_encoded_buffer() {
        let cells = [Atom::Text("value".into())];
        let keys = [(
            Bucket {
                level: 64,
                start: 0,
            },
            Tuple::Owned(&cells),
            AddressRef {
                iid: Iid::MIN,
                key: &[],
            },
        )];
        let encoded = Postings::build(&keys).unwrap().encode(&[], &[]).unwrap();
        let aligned = Buffer::from_slice_ref(&encoded);
        struct Aligned(Buffer);
        impl AsRef<[u8]> for Aligned {
            fn as_ref(&self) -> &[u8] {
                self.0.as_slice()
            }
        }
        let bytes = Bytes::from_owner(Aligned(aligned));
        let (postings, _) = Postings::decode(Buffer::from(bytes.clone()), 64).unwrap();
        let AtomRef::Text(value) = postings.at(0).1.cell(0) else {
            panic!("expected text");
        };
        let start = (value.as_ptr() as usize)
            .checked_sub(bytes.as_ptr() as usize)
            .unwrap();
        assert_eq!(
            bytes.get(start..start + value.len()),
            Some(b"value".as_slice())
        );
    }
}
