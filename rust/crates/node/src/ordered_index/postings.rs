use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Int64Array,
    NullArray, RecordBatch, StringArray, UInt8Array, UInt64Array, UnionArray,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder, StringBuilder},
};
use arrow_buffer::Buffer;
use arrow_ipc::{
    MetadataVersion,
    reader::read_record_batch,
    root_as_message,
    writer::{CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
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
enum Kind {
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

    fn build<'a>(self, cells: impl ExactSizeIterator<Item = AtomRef<'a>>) -> Result<ArrayRef> {
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
                let mut builder = StringBuilder::with_capacity(cells.len(), 0);
                for cell in cells {
                    match cell {
                        AtomRef::Text(value) => builder.append_value(value),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            Self::Binary => {
                let mut builder = BinaryBuilder::with_capacity(cells.len(), 0);
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

#[derive(Clone, Copy, Debug)]
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

    fn build(self, field: &Field, cells: Vec<AtomRef<'_>>) -> Result<ArrayRef> {
        if let Some(kind) = self.plain() {
            return kind.build(cells.into_iter());
        }
        let DataType::Union(fields, UnionMode::Dense) = field.data_type() else {
            return Err(invalid());
        };
        let mut legs: [Vec<_>; KINDS.len()] = std::array::from_fn(|_| Vec::new());
        let mut ids = Vec::with_capacity(cells.len());
        let mut offsets = Vec::with_capacity(cells.len());
        for cell in cells {
            let kind = Kind::of(cell) as usize;
            ids.push(kind as i8);
            offsets.push(i32::try_from(legs[kind].len()).map_err(format_error)?);
            legs[kind].push(cell);
        }
        let children = self
            .kinds()
            .map(|kind| kind.build(legs[kind as usize].drain(..)))
            .collect::<Result<_>>()?;
        Ok(Arc::new(
            UnionArray::try_new(fields.clone(), ids.into(), Some(offsets.into()), children)
                .map_err(format_error)?,
        ))
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
        KeyBatch::build(
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

    pub fn len(&self) -> usize {
        self.0.batch.num_rows()
    }
    pub fn bytes(&self) -> usize {
        self.0.bytes
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

    pub fn encode(&self, header: &[u8], suffix: &[u8]) -> Result<Vec<u8>> {
        self.0.encode(header, suffix)
    }

    pub fn decode(bytes: Bytes, max_rows: usize) -> Result<(Self, Bytes)> {
        let (batch, suffix) = KeyBatch::decode(bytes, max_rows, BatchKind::Posting)?;
        let postings = Self(batch);
        for row in 0..postings.len() {
            let (bucket, _, address) = postings.at(row);
            if (!address.key.is_empty()
                && rtdb_types::EntityId::from_handle_bytes(address.key).is_err())
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

#[derive(Debug)]
pub(super) struct KeyBatch {
    layouts: Vec<Layout>,
    batch: RecordBatch,
    prefix: usize,
    bytes: usize,
}

impl KeyBatch {
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
            arrays.push(layout.build(schema.field(slot + kind.prefix()), cells)?);
        }
        let batch = RecordBatch::try_new(schema, arrays).map_err(format_error)?;
        Self::new(layouts, batch, kind.prefix())
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
            arrays.push(layout.build(&field, cells)?);
            fields.push(field);
            layouts.push(layout);
        }
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(format_error)?;
        Self::new(layouts, batch, prefix)
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
        Self::new(layouts, batch, prefix.len())
    }

    fn new(layouts: Vec<Layout>, batch: RecordBatch, prefix: usize) -> Result<Self> {
        let bytes = rtdb_columnar::shared_allocation_size(&batch)?
            .saturating_add(layouts.len().saturating_mul(256));
        Ok(Self {
            layouts,
            batch,
            prefix,
            bytes,
        })
    }

    pub fn retained_bytes(&self) -> usize {
        self.bytes
    }
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    // One schema-bound IPC record batch. The short layout prefix identifies its typed columns.
    pub fn encode(&self, header: &[u8], suffix: &[u8]) -> Result<Vec<u8>> {
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
        let mut bytes = header.to_vec();
        for length in [self.layouts.len(), data.ipc_message.len(), suffix.len()] {
            bytes.extend_from_slice(&u32::try_from(length).map_err(format_error)?.to_le_bytes());
        }
        for layout in &self.layouts {
            bytes.extend_from_slice(&layout.0.to_le_bytes());
        }
        bytes.extend_from_slice(&data.ipc_message);
        bytes.resize(bytes.len().next_multiple_of(ALIGNMENT), 0);
        bytes.extend_from_slice(&data.arrow_data);
        bytes.extend_from_slice(suffix);
        Ok(bytes)
    }

    pub fn decode(bytes: Bytes, max_rows: usize, kind: BatchKind) -> Result<(Self, Bytes)> {
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
        let body = bytes.slice(body_start..body_end);
        let body = if body.as_ptr().align_offset(ALIGNMENT) == 0 {
            Buffer::from(body)
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
        let mut keys = Self::new(layouts, batch, kind.prefix())?;
        keys.bytes = keys.bytes.saturating_add(body_start);
        Ok((keys, bytes.slice(body_end..)))
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
    Cells(&'a [AtomRef<'a>]),
    Nulls(usize),
    Arrow(&'a KeyBatch, usize),
}

impl<'a> Tuple<'a> {
    pub fn len(self) -> usize {
        match self {
            Self::Owned(atoms) => atoms.len(),
            Self::Cells(cells) => cells.len(),
            Self::Nulls(width) => width,
            Self::Arrow(postings, _) => postings.layouts.len(),
        }
    }

    pub fn get(self, slot: usize) -> Option<AtomRef<'a>> {
        match self {
            Self::Owned(atoms) => atoms.get(slot).map(Atom::borrowed),
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
        let (postings, suffix) = Postings::decode(Bytes::from(unaligned).slice(1..), 64).unwrap();
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
        let (postings, _) = Postings::decode(bytes.clone(), 64).unwrap();
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
