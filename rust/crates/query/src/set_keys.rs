use std::{borrow::Cow, collections::HashMap, convert::Infallible, mem::size_of, ops::Range, sync::Arc};

use arrow_array::{Array, FixedSizeListArray, ListArray, cast::AsArray, types::{TimestampSecondType, TimestampMillisecondType, TimestampMicrosecondType, TimestampNanosecondType}};
use arrow_buffer::BooleanBufferBuilder;
use arrow_schema::DataType;
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation};
use rtdb_types::{ListKind, LogicalType, Result, ScalarType, TimeUnit, Value};

use crate::{
    QueryContext,
    columnar::{Column, ColumnBatch, float_vector::{FloatVector, nonnull}},
    invalid,
    join_storage::combine_cell_hash,
    operators::{combine_equality_hash, equality_cell_hash, has_uri, local_micros_equality_hash},
    scalar_key::{ScalarColumn, ScalarKey},
};

#[derive(Clone, Copy)]
struct ArrowKey<'a> {
    columns: &'a [Column],
    row: usize,
    projected: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct KeyColumns<'a> {
    batch: &'a ColumnBatch,
    positions: Option<&'a [usize]>,
}

impl<'a> KeyColumns<'a> {
    pub fn projection(batch: &'a ColumnBatch, positions: &'a [usize]) -> Self {
        Self { batch, positions: Some(positions) }
    }

    pub(crate) fn bind_typed(self, context: &QueryContext) -> Result<Option<TypedKeys<'a>>> {
        if self.positions.is_some_and(|positions| positions.iter().any(|&p| p >= self.batch.columns.len())) {
            return Err(invalid("column-gather", "Row position is outside its source batch"));
        }
        let width = self.positions.map_or(self.batch.columns.len(), <[usize]>::len);
        let columns = (0..width).map(|index| {
            &self.batch.columns[self.positions.map_or(index, |positions| positions[index])]
        });
        if !columns.clone().all(|column| TypedKeyColumn::bind(column).is_some()) {
            return Ok(None);
        }
        let bytes = width.checked_mul(size_of::<TypedKeyColumn<'_>>()).ok_or_else(size_error)?;
        let reservation = context.memory.reserve(bytes)?;
        let readers = columns
            .map(|column| TypedKeyColumn::bind(column).expect("The key layout was bound"))
            .collect();
        Ok(Some(TypedKeys { source: self, readers, _reservation: reservation }))
    }
}

impl<'a> From<&'a ColumnBatch> for KeyColumns<'a> {
    fn from(batch: &'a ColumnBatch) -> Self {
        Self { batch, positions: None }
    }
}

enum TypedKeyColumn<'a> {
    Flat(ScalarColumn<'a>),
    Selected { column: &'a Column, reader: ScalarColumn<'a> },
}

impl<'a> TypedKeyColumn<'a> {
    fn bind(column: &'a Column) -> Option<Self> {
        Some(match column {
            Column::Arrow { array, field, logical } =>
                Self::Flat(ScalarColumn::bind(array.as_ref(), field, logical.as_ref())?),
            Column::Selected(selected) => {
                let mut layouts = selected.source_layouts();
                let Some((array, field, logical)) = layouts.next() else {
                    return Some(Self::Flat(ScalarColumn::Null));
                };
                if layouts.next().is_some() { return None; }
                Self::Selected { column, reader: ScalarColumn::bind(array, field, logical)? }
            }
            Column::Values(_) => return None,
        })
    }

    fn read(&self, row: usize) -> ScalarKey<'a> {
        match self {
            Self::Flat(reader) => reader.read(row),
            Self::Selected { column, reader } => Self::position(column, row)
                .map_or(ScalarKey::Null, |position| reader.read(position)),
        }
    }

    fn visit(&self, len: usize, visit: impl FnMut(ScalarKey<'a>) -> Result<()>) -> Result<()> {
        match self {
            Self::Flat(reader) => reader.visit((0..len).map(Some), visit),
            Self::Selected { column, reader } =>
                reader.visit((0..len).map(|row| Self::position(column, row)), visit),
        }
    }

    fn position(column: &Column, row: usize) -> Option<usize> {
        column.arrow_cell(row).expect("The selected key covers the batch")
            .map(|(_, _, _, offset)| offset)
    }
}

pub(crate) struct TypedKeys<'a> {
    source: KeyColumns<'a>,
    readers: Vec<TypedKeyColumn<'a>>,
    _reservation: Arc<MemoryReservation>,
}

impl TypedKeys<'_> {
    pub(crate) fn hash_vector_into(&self, hashes: &mut [u32], context: &QueryContext) -> Result<()> {
        self.hash_join_into(hashes, context, JoinHash::Vector)
    }

    pub(crate) fn hash_join_into(&self, hashes: &mut [u32], context: &QueryContext, hashing: JoinHash) -> Result<()> {
        self.hash_into(hashes, context, |key| hashing.scalar(key), |hash, cell| combine_cell_hash(hash, cell as i32))
    }

    fn hash_for_index(&self, hashes: &mut [u32], context: &QueryContext, hashing: KeyHash) -> Result<()> {
        match hashing {
            KeyHash::Set => self.hash_into(hashes, context, |key| key.hash(), combine_equality_hash),
            KeyHash::Vector => self.hash_vector_into(hashes, context),
        }
    }

    fn hash_into(
        &self, hashes: &mut [u32], context: &QueryContext, hash_cell: impl Fn(ScalarKey<'_>) -> u32,
        combine: impl Fn(u32, u32) -> u32,
    ) -> Result<()> {
        let len = self.source.batch.len;
        assert_eq!(len, hashes.len());
        hashes.fill(0);
        for column in &self.readers {
            let mut output = hashes.iter_mut().enumerate();
            column.visit(len, |key| {
                let (row, hash) = output.next().expect("The column emits one key per row");
                context.check_cancelled_at(row)?;
                *hash = combine(*hash, hash_cell(key));
                Ok(())
            })?;
        }
        Ok(())
    }

    pub(crate) fn key(&self, row: usize, projected: bool) -> Key<'_> {
        Key {
            source: ArrowKey { columns: &self.source.batch.columns, row, projected },
            positions: self.source.positions,
            typed: Some(&self.readers),
            values: Vec::new(),
            has_uri: false,
            index_timestamps: false,
            _reservation: None,
        }
    }

    fn equal_rows(&self, left: usize, right: usize) -> bool {
        self.readers.iter().all(|column| column.read(left).equal(column.read(right)) == Some(true))
    }
}

pub(crate) struct Key<'a> {
    source: ArrowKey<'a>,
    positions: Option<&'a [usize]>,
    typed: Option<&'a [TypedKeyColumn<'a>]>,
    values: Vec<(usize, Value)>,
    has_uri: bool,
    index_timestamps: bool,
    _reservation: Option<Arc<MemoryReservation>>,
}

#[derive(Clone, Copy)]
enum KeyHash {
    Set,
    Vector,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinHash {
    Vector,
    CanonicalZero,
}

impl JoinHash {
    pub(crate) fn scalar(self, key: ScalarKey<'_>) -> u32 {
        match key {
            ScalarKey::Float(value) if self == Self::CanonicalZero && value == 0.0 => ScalarKey::Float(0.0).hash(),
            _ => key.hash(),
        }
    }
}

#[derive(Clone)]
struct PlainListKey<'a> {
    fixed: bool,
    children: ScalarColumn<'a>,
    rows: Range<usize>,
}

impl<'a> PlainListKey<'a> {
    fn arrow(cell: ArrowCell<'a>, logical: &LogicalType) -> Option<Self> {
        let ArrowCell { array, field, row } = cell;
        if row >= array.len() || field.data_type() != array.data_type() || !field.metadata().is_empty() || array.is_null(row) {
            return None;
        }
        let LogicalType::List { kind, element } = nonnull(logical) else { return None; };
        let (children, rows) = match (kind, field.data_type()) {
            (ListKind::List, DataType::List(child)) => {
                let list = array.as_any().downcast_ref::<ListArray>()?;
                let offsets = list.value_offsets();
                let children = ScalarColumn::bind(list.values().as_ref(), child, Some(element))?;
                (children, offsets[row] as usize..offsets[row + 1] as usize)
            }
            (ListKind::FixedSizeList(width), DataType::FixedSizeList(child, physical)) if width == physical => {
                let list = array.as_any().downcast_ref::<FixedSizeListArray>()?;
                let start = list.value_offset(row) as usize;
                let children = ScalarColumn::bind(list.values().as_ref(), child, Some(element))?;
                (children, start..start + *physical as usize)
            }
            _ => return None,
        };
        if matches!(children, ScalarColumn::Null | ScalarColumn::F32(_) | ScalarColumn::F64(_)) {
            return None;
        }
        Some(Self { fixed: matches!(kind, ListKind::FixedSizeList(_)), children, rows })
    }

    fn hash(&self, hash_key: fn(ScalarKey<'a>) -> u32) -> i32 {
        let mut hash = 0i32;
        self.children.visit(self.rows.clone().map(Some), |key| {
            hash = hash.wrapping_mul(31).wrapping_add(hash_key(key) as i32);
            Ok::<_, Infallible>(())
        }).unwrap();
        hash
    }

    fn equal(&self, right: &Self) -> Option<Option<bool>> {
        self.equal_children(right.fixed, right.rows.clone().map(|row| right.children.read(row)))
    }

    fn equal_children(&self, fixed: bool, right: impl ExactSizeIterator<Item = ScalarKey<'a>>) -> Option<Option<bool>> {
        if self.fixed != fixed || self.rows.len() != right.len() {
            return Some(Some(false));
        }
        let mut unknown = false;
        for (left, right) in self.rows.clone().map(|row| self.children.read(row)).zip(right) {
            if matches!(left, ScalarKey::Null) || matches!(right, ScalarKey::Null) {
                unknown = true;
            } else if left.equal(right) != Some(true) {
                return Some(Some(false));
            }
        }
        Some((!unknown).then_some(true))
    }
}

#[derive(Clone)]
enum DirectKey<'a> {
    Scalar(ScalarKey<'a>),
    ScalarList(PlainListKey<'a>),
    FloatList(FloatVector<'a>),
    Timestamp(TimestampKey),
}

#[derive(Clone, Copy)]
struct TimestampKey {
    value: i64,
    unit: TimeUnit,
    zoned: bool,
}

impl TimestampKey {
    fn arrow(cell: ArrowCell<'_>, logical: &LogicalType) -> Option<DirectKey<'static>> {
        let LogicalType::Scalar(scalar @ ScalarType::Timestamp { unit, timezone }) = nonnull(logical) else {
            return None;
        };
        use arrow_schema::TimeUnit as ArrowUnit;
        let ArrowCell { array, field, row } = cell;
        if field.data_type() != array.data_type() || row >= array.len() || !cell.has_scalar_storage(scalar) {
            return None;
        }
        let DataType::Timestamp(physical_unit, _) = array.data_type() else { return None; };
        if array.is_null(row) { return Some(DirectKey::Scalar(ScalarKey::Null)); }
        let value = match (unit, physical_unit) {
            (TimeUnit::Second, ArrowUnit::Second) =>
                array.as_primitive::<TimestampSecondType>().value(row),
            (TimeUnit::Millisecond, ArrowUnit::Millisecond) =>
                array.as_primitive::<TimestampMillisecondType>().value(row),
            (TimeUnit::Microsecond, ArrowUnit::Microsecond) =>
                array.as_primitive::<TimestampMicrosecondType>().value(row),
            (TimeUnit::Nanosecond, ArrowUnit::Nanosecond) =>
                array.as_primitive::<TimestampNanosecondType>().value(row),
            _ => return None,
        };
        Some(DirectKey::Timestamp(Self { value, unit: *unit, zoned: timezone.is_some() }))
    }

    fn from_value(value: &Value) -> Option<Self> {
        let Value::Timestamp { value, unit, timezone } = value else { return None; };
        Some(Self { value: *value, unit: *unit, zoned: timezone.is_some() })
    }
}

impl<'a> DirectKey<'a> {
    fn read_with_timestamps(column: &'a Column, row: usize, index_timestamps: bool) -> Option<Self> {
        if let Column::Values(values) = column {
            let value = values.get(row)?;
            if index_timestamps && let Some(timestamp) = TimestampKey::from_value(value) {
                return Some(Self::Timestamp(timestamp));
            }
            return Self::from_value(value);
        }
        let Some((array, field, logical, row)) = column.arrow_cell(row)? else {
            return Some(Self::Scalar(ScalarKey::Null));
        };
        if let Some(scalar) = ScalarColumn::bind(array, field, logical) {
            return Some(Self::Scalar(scalar.read(row)));
        }
        let cell = ArrowCell { array, field, row };
        if let Some(logical) = logical {
            if index_timestamps && let Some(key) = TimestampKey::arrow(cell, logical) {
                return Some(key);
            }
            if let Some(list) = PlainListKey::arrow(cell, logical) {
                return Some(Self::ScalarList(list));
            }
        }
        let (cell, logical) = cell.logical_position(logical).ok()?;
        if logical == LogicalType::Null || cell.array.is_null(cell.row) {
            return Some(Self::Scalar(ScalarKey::Null));
        }
        if let Some(key) = ScalarKey::read_resolved(cell, &logical) { return Some(Self::Scalar(key)); }
        if index_timestamps && let Some(key) = TimestampKey::arrow(cell, &logical) {
            return Some(key);
        }
        if let Some(list) = PlainListKey::arrow(cell, &logical) {
            return Some(Self::ScalarList(list));
        }
        FloatVector::arrow(cell, &logical).map(Self::FloatList)
    }

    fn from_value(value: &'a Value) -> Option<Self> {
        if let Some(key) = ScalarKey::from_value(value) { return Some(Self::Scalar(key)); }
        FloatVector::value(value).map(Self::FloatList)
    }

    fn equal(self, right: Self, nulls_equal: bool) -> Option<Option<bool>> {
        match (self, right) {
            (Self::Scalar(ScalarKey::Null), Self::Scalar(ScalarKey::Null)) =>
                Some(nulls_equal.then_some(true)),
            (Self::Scalar(ScalarKey::Null), _) | (_, Self::Scalar(ScalarKey::Null)) =>
                Some(nulls_equal.then_some(false)),
            (Self::Scalar(left), Self::Scalar(right)) => left.equal(right).map(Some),
            (Self::ScalarList(left), Self::ScalarList(right)) => left.equal(&right),
            (Self::ScalarList(list), Self::FloatList(vector)) | (Self::FloatList(vector), Self::ScalarList(list)) =>
                list.equal_children(vector.width.is_some(), vector.range.clone().map(|row| {
                    vector.float_component(row).map_or(ScalarKey::Null, ScalarKey::Float)
                })),
            (Self::ScalarList(_), _) | (_, Self::ScalarList(_)) => None,
            (Self::Timestamp(left), Self::Timestamp(right)) if left.unit == right.unit && left.zoned == right.zoned =>
                Some(Some(left.value == right.value)),
            (Self::Timestamp(_), Self::FloatList(_)) | (Self::FloatList(_), Self::Timestamp(_)) => Some(Some(false)),
            (Self::Timestamp(_), _) | (_, Self::Timestamp(_)) => None,
            (Self::FloatList(left), Self::FloatList(right)) => {
                if left.width.is_some() != right.width.is_some() || left.range.len() != right.range.len() {
                    return Some(Some(false));
                }
                let mut unknown = false;
                for (l, r) in left.range.clone().zip(right.range.clone()) {
                    match (left.float_component(l), right.float_component(r)) {
                        (Some(l), Some(r)) if l == r => {}
                        (Some(_), Some(_)) => return Some(Some(false)),
                        _ => unknown = true,
                    }
                }
                Some((!unknown).then_some(true))
            }
            _ => Some(Some(false)),
        }
    }

    fn equal_value(&self, value: &Value, nulls_equal: bool) -> Option<Option<bool>> {
        let Self::FloatList(vector) = self else { return None; };
        let list = match value {
            Value::List(list) if vector.width.is_none() => list,
            Value::FixedSizeList(list) if vector.width.is_some() => list,
            Value::Null | Value::Undefined => return Some(nulls_equal.then_some(false)),
            Value::Transit(_) => return None,
            _ => return Some(Some(false)),
        };
        if vector.range.len() != list.len() { return Some(Some(false)); }
        let mut unknown = false;
        for (index, value) in vector.range.clone().zip(list.iter()) {
            let left = vector.float_component(index);
            if left.is_none() || matches!(value.as_ref(), Value::Null | Value::Undefined) {
                unknown = true;
            } else if matches!(value.as_ref(), Value::Transit(_)) {
                return None;
            } else if left != rtdb_expr::number(value.as_ref()) {
                return Some(Some(false));
            }
        }
        Some((!unknown).then_some(true))
    }

    fn hash(self) -> i32 {
        self.hash_with(ScalarKey::hash, rtdb_columnar::xxhash32)
    }

    fn set_hash(self, timezone: &str) -> u32 {
        match self {
            Self::Timestamp(key) => local_micros_equality_hash(
                rtdb_expr::local_timestamp_micros(key.value, key.unit, key.zoned, timezone),
            ),
            _ => self.hash() as u32,
        }
    }

    fn hash_with(self, hash_key: fn(ScalarKey<'a>) -> u32, hash_bytes: fn(&[u8], u32) -> u32) -> i32 {
        match self {
            Self::Scalar(key) => hash_key(key) as i32,
            Self::Timestamp(key) => hash_key(ScalarKey::Float(key.value as f64 / key.unit.units_per_second() as f64)) as i32,
            Self::ScalarList(list) => list.hash(hash_key),
            Self::FloatList(vector) => rtdb_columnar::hash_float_components(
                vector.range.clone().map(|index| vector.float_component(index)), 0, hash_bytes,
            ),
        }
    }
}

pub(crate) fn direct_key_equal(left: &Column, l: usize, right: &Column, r: usize, nulls_equal: bool) -> Option<Option<bool>> {
    DirectKey::read_with_timestamps(left, l, true)?.equal(DirectKey::read_with_timestamps(right, r, true)?, nulls_equal)
}

pub(crate) struct BloomKey<'a> {
    pub hashes: (i32, i32),
    pub scalar: Option<ScalarKey<'a>>,
    pub retain_iid_limit: bool,
}

pub(crate) fn direct_bloom_hashes(column: &Column, row: usize) -> Option<BloomKey<'_>> {
    let key = DirectKey::read_with_timestamps(column, row, true)?;
    let scalar = match key { DirectKey::Scalar(scalar) => Some(scalar), _ => None };
    let retain_iid_limit = matches!(key, DirectKey::ScalarList(_) | DirectKey::Timestamp(_));
    let hashes = (key.clone().hash(), key.hash_with(ScalarKey::murmur3, rtdb_columnar::murmur3_32));
    Some(BloomKey { hashes, scalar, retain_iid_limit })
}

impl<'a> Key<'a> {
    fn read(source: ArrowKey<'a>, positions: Option<&'a [usize]>, index_timestamps: bool, context: &QueryContext) -> Result<Self> {
        let mut key = Self { source, positions, typed: None, values: Vec::new(), has_uri: false, index_timestamps, _reservation: None };
        let mut bound = 256usize;
        for index in 0..key.len() {
            let column = key.column(index);
            if let Some(direct) = DirectKey::read_with_timestamps(column, source.row, index_timestamps) {
                key.has_uri |= matches!(direct, DirectKey::Scalar(ScalarKey::Uri(_)));
            } else {
                bound = bound.checked_add(column.bound(source.row)?)
                    .and_then(|bound| bound.checked_add(4 * size_of::<(usize, Value)>()))
                    .ok_or_else(size_error)?;
                match &mut key._reservation {
                    Some(reservation) => Arc::get_mut(reservation).expect("A key owns its scratch").grow_to(bound)?,
                    None => key._reservation = Some(context.memory.reserve(bound)?),
                }
                let value = key.read_value(index)?;
                key.has_uri |= has_uri(&value);
                key.values.push((index, value));
            }
        }
        Ok(key)
    }

    pub(crate) fn for_columns(batch: &'a ColumnBatch, row: usize, projected: bool, positions: &'a [usize],
        context: &QueryContext) -> Result<Self> {
        Self::read(ArrowKey { columns: &batch.columns, row, projected }, Some(positions), true, context)
    }

    fn len(&self) -> usize {
        self.positions.map_or(self.source.columns.len(), <[usize]>::len)
    }

    fn column(&self, index: usize) -> &'a Column {
        &self.source.columns[self.positions.map_or(index, |positions| positions[index])]
    }

    fn read_value(&self, index: usize) -> Result<Value> {
        let mut value = self.column(index).value(self.source.row)?;
        if self.source.projected { value.admit_expression_dates()?; }
        Ok(value)
    }

    pub(crate) fn cached(&self, index: usize) -> Option<&Value> {
        let position = self.values.binary_search_by_key(&index, |(column, _)| *column).ok()?;
        Some(&self.values[position].1)
    }

    fn direct(&self, index: usize) -> Option<DirectKey<'_>> {
        if let Some(typed) = self.typed {
            return Some(DirectKey::Scalar(typed[index].read(self.source.row)));
        }
        match self.cached(index) {
            Some(value) => DirectKey::from_value(value),
            None => DirectKey::read_with_timestamps(self.column(index), self.source.row, self.index_timestamps),
        }
    }

    fn bind(
        columns: impl Into<KeyColumns<'a>>, row: usize, projected: bool, context: &QueryContext,
        hashing: KeyHash, index_timestamps: bool,
    ) -> Result<(u32, Self)> {
        let KeyColumns { batch, positions } = columns.into();
        let key = Self::read(ArrowKey { columns: &batch.columns, row, projected }, positions, index_timestamps, context)?;
        let hash = match hashing {
            KeyHash::Set => (0..key.len()).fold(0, |hash, index| {
                let cell = match key.cached(index) {
                    Some(value) => equality_cell_hash(value, &context.timezone),
                    None => key.direct(index).expect("An unread key has a direct reader").set_hash(&context.timezone),
                };
                combine_equality_hash(hash, cell)
            }),
            KeyHash::Vector => key.vector_hash()?,
        };
        Ok((hash, key))
    }

    /// Whether every part of the key reads directly from its column, as the decoded comparison needs.
    fn decodable(&self) -> bool { !self.has_uri && self.values.is_empty() }

    fn direct_parts(&self) -> impl ExactSizeIterator<Item = Option<DirectKey<'_>>> {
        (0..self.len()).map(|index| self.direct(index))
    }

    pub(crate) fn vector_hash(&self) -> Result<u32> {
        self.join_hash(JoinHash::Vector)
    }

    pub(crate) fn join_hash(&self, hashing: JoinHash) -> Result<u32> {
        (0..self.len()).try_fold(0, |hash, index| {
            let cell = match self.cached(index) {
                Some(value) => {
                    if hashing == JoinHash::CanonicalZero
                        && let Some(key @ ScalarKey::Float(_)) = ScalarKey::from_value(value)
                    {
                        hashing.scalar(key) as i32
                    } else {
                        rtdb_columnar::hash_value(value, 0)?
                    }
                }
                None => match self.direct(index).expect("An unread key has a direct reader") {
                    DirectKey::Scalar(key) => hashing.scalar(key) as i32,
                    key => key.hash(),
                },
            };
            Ok(combine_cell_hash(hash, cell))
        })
    }

    fn equals(&self, other: ArrowKey<'_>, positions: Option<&[usize]>, context: &QueryContext) -> Result<bool> {
        if self.len() != positions.map_or(other.columns.len(), <[usize]>::len) { return Ok(false); }
        let right = Key::read(other, positions, self.index_timestamps, context)?;
        // URI preflight can raise a hash error in a later cell even if an earlier cell compares unequal.
        if self.has_uri || right.has_uri {
            let _scratch = context.memory.reserve(4096)?;
            if self.vector_hash()? != right.vector_hash()? { return Ok(false); }
        }
        for index in 0..self.len() {
            if self.equal_at(&right, index, true, context, None)? != Some(true) { return Ok(false); }
        }
        Ok(true)
    }

    pub(crate) fn equal_at(&self, right: &Key<'_>, index: usize, nulls_equal: bool, context: &QueryContext,
        scope: Option<&rtdb_expr::EvaluationScope>) -> Result<Option<bool>> {
        let direct = match (self.direct(index), right.direct(index)) {
            (Some(a), Some(b)) => a.equal(b, nulls_equal),
            (Some(a), None) => right.cached(index).and_then(|value| a.equal_value(value, nulls_equal)),
            (None, Some(b)) => self.cached(index).and_then(|value| b.equal_value(value, nulls_equal)),
            _ => None,
        };
        if let Some(equal) = direct { return Ok(equal); }
        let bound = |key: &Key<'_>| -> Result<usize> {
            if key.cached(index).is_some() { Ok(0) } else { key.column(index).bound(key.source.row) }
        };
        let _scratch = context.memory.reserve(bound(self)?.checked_add(bound(right)?).ok_or_else(size_error)?)?;
        let l = if let Some(value) = self.cached(index) { Cow::Borrowed(value) } else { Cow::Owned(self.read_value(index)?) };
        let r = if let Some(value) = right.cached(index) { Cow::Borrowed(value) } else { Cow::Owned(right.read_value(index)?) };
        let local;
        let scope = match scope {
            Some(scope) => scope,
            None => { local = rtdb_expr::EvaluationScope::new(&context.memory)?; &local }
        };
        if nulls_equal {
            if matches!(l.as_ref(), Value::Null | Value::Undefined) || matches!(r.as_ref(), Value::Null | Value::Undefined) {
                return Ok(Some(matches!(l.as_ref(), Value::Null | Value::Undefined) && matches!(r.as_ref(), Value::Null | Value::Undefined)));
            }
        }
        let equal = rtdb_expr::sql_eq_in_timezone(&l, &r, &context.timezone, scope)?;
        scope.reset()?;
        Ok(equal)
    }
}

#[derive(Clone, Copy)]
struct Position {
    batch: usize,
    row: usize,
    next: usize,
    consumed: bool,
}

#[derive(Default)]
struct Positions {
    rows: Vec<Position>,
    // Each chain's last position links back to its first.
    tails: HashMap<u64, usize>,
}

impl Positions {
    fn allocation_bound(rows: usize) -> Result<usize> {
        rows.checked_mul(4 * size_of::<Position>() + 4 * (size_of::<(u64, usize)>() + 1))
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(size_error)
    }

    fn push(&mut self, bucket: u64, batch: usize, row: usize) {
        let index = self.rows.len();
        self.rows.push(Position { batch, row, next: index, consumed: false });
        self.link(bucket, index);
    }

    fn link(&mut self, bucket: u64, index: usize) {
        use std::collections::hash_map::Entry;
        match self.tails.entry(bucket) {
            Entry::Occupied(mut entry) => {
                let tail = entry.get_mut();
                self.rows[index].next = self.rows[*tail].next;
                self.rows[*tail].next = index;
                *tail = index;
            }
            Entry::Vacant(entry) => {
                self.rows[index].next = index;
                entry.insert(index);
            }
        }
    }

    fn build_page(&mut self, page: usize, len: usize, mut hash: impl FnMut(usize) -> Result<u32>) -> Result<()> {
        let start = self.rows.len();
        let end = start.checked_add(len).ok_or_else(size_error)?;
        // During growth, keep the prior allocation so a failed hash can also roll back the capacity charge.
        let mut grown = (end > self.rows.capacity()).then(|| {
            let mut rows = Vec::with_capacity(end.max(self.rows.capacity().saturating_mul(2)));
            rows.extend_from_slice(&self.rows);
            rows
        });
        let rows = grown.as_mut().unwrap_or(&mut self.rows);
        // The unlinked suffix carries its hashes until every row in the page has validated.
        for row in 0..len {
            let hash = match hash(row) {
                Ok(hash) => hash,
                Err(error) => {
                    rows.truncate(start);
                    return Err(error);
                }
            };
            rows.push(Position { batch: page, row, next: hash as usize, consumed: false });
        }
        if let Some(grown) = grown {
            self.rows = grown;
        }
        for index in start..end {
            let hash = self.rows[index].next as u32;
            self.link(u64::from(hash), index);
        }
        Ok(())
    }

    fn find(
        &mut self,
        bucket: u64,
        all: bool,
        context: &QueryContext,
        mut equal: impl FnMut(usize, Position) -> Result<bool>,
    ) -> Result<Option<usize>> {
        let Some(&tail) = self.tails.get(&bucket) else { return Ok(None); };
        let mut index = self.rows[tail].next;
        let mut checked = 0usize;
        loop {
            context.check_cancelled_at(checked)?;
            checked += 1;
            let position = self.rows[index];
            if (!all || !position.consumed) && equal(index, position)? {
                if all {
                    self.rows[index].consumed = true;
                }
                return Ok(Some(index));
            }
            if index == tail {
                return Ok(None);
            }
            index = position.next;
        }
    }
}

pub(crate) struct BatchKeyIndex {
    batches: Vec<(ColumnBatch, bool)>,
    positions: Positions,
    reservation: Arc<MemoryReservation>,
    hashing: KeyHash,
    /// Stored keys already decoded for comparison, by position.
    decoded: DecodedKeys,
}

/// Owned copies of stored direct keys, decoded on their first comparison, so that a key met again is not read again.
#[derive(Default)]
struct DecodedKeys { keys: HashMap<usize, Decoded>, charge: Option<Arc<MemoryReservation>> }

enum Decoded {
    Scalars(Box<[OwnedScalar]>),
    /// A key with a part outside the owned scalars, compared through its columns.
    General,
}

enum OwnedScalar {
    Null, Bool(bool), Signed(i64), Float(f64), Text(Box<str>), Keyword(Box<str>),
    Bytes(Box<[u8]>), FixedBytes(Box<[u8]>), Uuid([u8; 16]), Reference(i32), Timestamp(TimestampKey),
}

impl OwnedScalar {
    fn of(key: DirectKey<'_>) -> Self {
        match key {
            DirectKey::Timestamp(key) => Self::Timestamp(key),
            DirectKey::Scalar(key) => match key {
                ScalarKey::Null => Self::Null,
                ScalarKey::Bool(value) => Self::Bool(value),
                ScalarKey::Signed(value) => Self::Signed(value),
                ScalarKey::Float(value) => Self::Float(value),
                ScalarKey::Text(value) => Self::Text(value.into()),
                ScalarKey::Keyword(value) => Self::Keyword(value.into()),
                ScalarKey::Bytes(value) => Self::Bytes(value.into()),
                ScalarKey::FixedBytes(value) => Self::FixedBytes(value.into()),
                ScalarKey::Uuid(value) => Self::Uuid(*value),
                ScalarKey::Reference(value) => Self::Reference(value),
                ScalarKey::Uri(_) => unreachable!("URI keys require preflight"),
            },
            DirectKey::ScalarList(_) | DirectKey::FloatList(_) => unreachable!("List keys retain their Arrow positions"),
        }
    }

    fn key(&self) -> DirectKey<'_> {
        DirectKey::Scalar(match self {
            Self::Null => ScalarKey::Null,
            Self::Bool(value) => ScalarKey::Bool(*value),
            Self::Signed(value) => ScalarKey::Signed(*value),
            Self::Float(value) => ScalarKey::Float(*value),
            Self::Text(value) => ScalarKey::Text(value),
            Self::Keyword(value) => ScalarKey::Keyword(value),
            Self::Bytes(value) => ScalarKey::Bytes(value),
            Self::FixedBytes(value) => ScalarKey::FixedBytes(value),
            Self::Uuid(value) => ScalarKey::Uuid(value),
            Self::Reference(value) => ScalarKey::Reference(*value),
            Self::Timestamp(value) => return DirectKey::Timestamp(*value),
        })
    }

    fn payload_bytes(key: DirectKey<'_>) -> Option<usize> {
        Some(match key {
            DirectKey::Scalar(ScalarKey::Text(value) | ScalarKey::Keyword(value)) => value.len(),
            DirectKey::Scalar(ScalarKey::Bytes(value) | ScalarKey::FixedBytes(value)) => value.len(),
            DirectKey::Scalar(ScalarKey::Uri(_)) | DirectKey::ScalarList(_) | DirectKey::FloatList(_) => return None,
            _ => 0,
        })
    }
}

impl DecodedKeys {
    fn get<'c>(
        &mut self, index: usize, columns: impl ExactSizeIterator<Item = &'c Column> + Clone, row: usize,
        index_timestamps: bool, memory: &Arc<MemoryBudget>,
    ) -> Result<Option<&Decoded>> {
        if !self.keys.contains_key(&index) {
            let read = |column| DirectKey::read_with_timestamps(column, row, index_timestamps);
            let mut payload = Some(columns.len().checked_mul(size_of::<OwnedScalar>()).ok_or_else(size_error)?);
            for column in columns.clone() {
                let Some(bytes) = read(column).and_then(OwnedScalar::payload_bytes) else {
                    payload = None;
                    break;
                };
                payload = Some(payload.unwrap().checked_add(bytes).ok_or_else(size_error)?);
            }
            let bytes = payload.unwrap_or(0).checked_add(4 * (size_of::<(usize, Decoded)>() + 1)).ok_or_else(size_error)?;
            if !self.charge(bytes, memory)? { return Ok(None); }
            let decoded = match payload {
                Some(_) => Decoded::Scalars(columns.map(|column| OwnedScalar::of(read(column).expect("The key was measured"))).collect()),
                None => Decoded::General,
            };
            self.keys.insert(index, decoded);
        }
        Ok(self.keys.get(&index))
    }

    fn charge(&mut self, additional: usize, memory: &Arc<MemoryBudget>) -> Result<bool> {
        let held = self.charge.as_ref().map_or(0, |charge| charge.bytes());
        let bytes = held.checked_add(additional).ok_or_else(size_error)?;
        let result = match &mut self.charge {
            Some(charge) => Arc::get_mut(charge).expect("The decoded keys own their charge").grow_to(bytes),
            None => memory.reserve(bytes).map(|charge| self.charge = Some(charge)),
        };
        match result {
            Ok(()) => Ok(true),
            Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

/// Whether a key equals a decoded one, as `Key::equals` decides it; `None` where only the general comparison can decide.
fn decoded_equal<'a>(probe: impl ExactSizeIterator<Item = Option<DirectKey<'a>>>, stored: &[OwnedScalar]) -> Option<bool> {
    if probe.len() != stored.len() { return None; }
    for (left, right) in probe.zip(stored) {
        let left = left?;
        if matches!(left, DirectKey::Scalar(ScalarKey::Uri(_)) | DirectKey::ScalarList(_) | DirectKey::FloatList(_)) { return None; }
        if left.equal(right.key(), true)? != Some(true) { return Some(false); }
    }
    Some(true)
}

impl BatchKeyIndex {
    pub fn new(context: &QueryContext) -> Result<Self> {
        Self::with_hash(context, KeyHash::Set)
    }

    pub fn vector_hashing(context: &QueryContext) -> Result<Self> {
        Self::with_hash(context, KeyHash::Vector)
    }

    fn with_hash(context: &QueryContext, hashing: KeyHash) -> Result<Self> {
        Ok(Self {
            batches: Vec::new(),
            positions: Positions::default(),
            reservation: context.memory.reserve(0)?,
            hashing,
            decoded: DecodedKeys::default(),
        })
    }

    pub fn len(&self) -> usize {
        self.positions.rows.len()
    }

    fn push_page(&mut self, batch: ColumnBatch, projected: bool) -> usize {
        self.batches.push((batch, projected));
        self.batches.len() - 1
    }

    pub fn into_group_keys(self, width: usize, context: &QueryContext) -> Result<Option<ColumnBatch>> {
        if self.len() == 0 {
            return Ok(None);
        }
        let Self { batches, positions: Positions { rows: positions, tails }, mut reservation, decoded, .. } = self;
        drop((tails, decoded));
        let batch_bytes = batches.capacity().checked_mul(size_of::<(ColumnBatch, bool)>()).ok_or_else(size_error)?;
        let retained = positions.capacity().checked_mul(size_of::<Position>())
            .and_then(|bytes| bytes.checked_add(batch_bytes))
            .ok_or_else(size_error)?;
        let lease = Arc::get_mut(&mut reservation).expect("The key index owns its reservation");
        lease.shrink_to(retained)?;

        let bytes = positions.len()
            .checked_mul(size_of::<(usize, usize)>())
            .and_then(|bytes| {
                batches.len()
                    .checked_mul(size_of::<&ColumnBatch>())
                    .and_then(|batches| bytes.checked_add(batches))
            })
            .and_then(|bytes| {
                width
                    .checked_mul(size_of::<usize>())
                    .and_then(|width| bytes.checked_add(width))
            })
            .ok_or_else(size_error)?;
        let _scratch = context.memory.reserve(bytes)?;
        let rows: Vec<_> = positions.iter().map(|position| (position.batch, position.row)).collect();
        drop(positions);
        lease.shrink_to(batch_bytes)?;
        let sources: Vec<_> = batches.iter().map(|(batch, _)| batch).collect();
        let columns: Vec<_> = (0..width).collect();
        ColumnBatch::take_rows_with_custody(&sources, &rows, &columns, true, &context.memory)
            .map(Some)
    }

    fn reserve_page(&mut self, len: usize) -> Result<()> {
        let rows = self
            .positions
            .rows
            .len()
            .checked_add(len)
            .ok_or_else(size_error)?;
        let batches = self
            .batches
            .len()
            .checked_add(1)
            .and_then(|len| len.checked_mul(4 * size_of::<(ColumnBatch, bool)>()))
            .ok_or_else(size_error)?;
        let bytes = Positions::allocation_bound(rows)?
            .checked_add(batches)
            .ok_or_else(size_error)?;
        Arc::get_mut(&mut self.reservation)
            .expect("The key index owns its reservation")
            .grow_to(bytes)
    }

    fn append(&mut self, batch: ColumnBatch, projected: bool, hashes: &[u32]) -> Result<()> {
        self.reserve_page(hashes.len())?;
        let page = self.push_page(batch, projected);
        for (row, &hash) in hashes.iter().enumerate() {
            self.positions.push(u64::from(hash), page, row);
        }
        Ok(())
    }

    pub fn build(
        &mut self,
        batch: ColumnBatch,
        projected: bool,
        context: &QueryContext,
    ) -> Result<()> {
        if batch.len == 0 {
            return Ok(());
        }
        context.check_cancelled()?;
        let previous_charge = self.reservation.bytes();
        self.reserve_page(batch.len)?;
        let page = self.batches.len();
        let hashing = self.hashing;
        let built = self.positions.build_page(page, batch.len, |row| {
            if row != 0 {
                context.check_cancelled_at(row)?;
            }
            Key::bind(&batch, row, projected, context, hashing, matches!(hashing, KeyHash::Set)).map(|(hash, _)| hash)
        });
        if let Err(error) = built {
            Arc::get_mut(&mut self.reservation)
                .expect("The key index owns its reservation")
                .shrink_to(previous_charge)
                .expect("The build reserved at least its previous charge");
            return Err(error);
        }
        self.push_page(batch, projected);
        Ok(())
    }

    pub fn matches(
        &mut self,
        batch: &ColumnBatch,
        row: usize,
        projected: bool,
        all: bool,
        context: &QueryContext,
    ) -> Result<bool> {
        let (hash, key) = Key::bind(batch, row, projected, context, self.hashing, matches!(self.hashing, KeyHash::Set))?;
        self.matches_key(hash, &key, all, context)
    }

    pub fn matches_key(&mut self, hash: u32, key: &Key<'_>, all: bool, context: &QueryContext) -> Result<bool> {
        self.find(hash, key, all, context).map(|found| found.is_some())
    }

    pub(crate) fn matches_column(
        &mut self,
        batch: &ColumnBatch,
        position: usize,
        row: usize,
        projected: bool,
        context: &QueryContext,
    ) -> Result<bool> {
        let positions = [position];
        let columns = KeyColumns::projection(batch, &positions);
        let (hash, key) = Key::bind(columns, row, projected, context, self.hashing, false)?;
        self.matches_key(hash, &key, false, context)
    }

    fn find(
        &mut self,
        hash: u32,
        key: &Key<'_>,
        all: bool,
        context: &QueryContext,
    ) -> Result<Option<usize>> {
        let batches = &self.batches;
        let decoded = &mut self.decoded;
        let direct = key.typed.is_none() && key.decodable();
        self.positions.find(u64::from(hash), all, context, |index, position| {
            let (batch, projected) = &batches[position.batch];
            if direct
                && let Some(Decoded::Scalars(stored)) = decoded.get(index, batch.columns.iter(), position.row, key.index_timestamps, &context.memory)?
                && let Some(equal) = decoded_equal(key.direct_parts(), stored)
            {
                return Ok(equal);
            }
            key.equals(ArrowKey { columns: &batch.columns, row: position.row, projected: *projected }, None, context)
        })
    }

    pub fn insert_distinct_argument(
        &mut self, batch: &ColumnBatch, column: usize, row: usize, group: usize, projected: bool,
        typed: Option<&TypedKeys<'_>>, retained_source: &mut Option<(usize, usize)>, context: &QueryContext,
    ) -> Result<bool> {
        let columns = std::slice::from_ref(&batch.columns[column]);
        let key = match typed {
            Some(typed) => typed.key(row, projected),
            None => Key::read(ArrowKey { columns, row, projected }, None, false, context)?,
        };
        let hash = key.vector_hash()?;
        let source = &self.batches;
        let group = u32::try_from(group).map_err(|_| size_error())?;
        let bucket = (u64::from(group) << 32) | u64::from(hash);
        if self.positions.find(bucket, false, context, |_, position| {
            let (batch, projected) = &source[position.batch];
            let other = Key::read(ArrowKey { columns: &batch.columns, row: position.row, projected: *projected }, None, false, context)?;
            Ok(key.equal_at(&other, 0, false, context, None)? == Some(true))
        })?.is_some() {
            return Ok(false);
        }
        let rows = self.positions.rows.len().checked_add(1).ok_or_else(size_error)?;
        let batches = self.batches.len().checked_add(usize::from(retained_source.is_none())).ok_or_else(size_error)?;
        let bytes = batches.checked_mul(4 * size_of::<(ColumnBatch, bool)>())
            .and_then(|batches| Positions::allocation_bound(rows).ok()?.checked_add(batches))
            .ok_or_else(size_error)?;
        Arc::get_mut(&mut self.reservation).expect("The key index owns its reservation").grow_to(bytes)?;
        let page = match *retained_source {
            Some((page, _)) => page,
            None => {
                let source = batch.key_projection(&[column], &context.memory)?;
                let page = self.push_page(source, projected);
                *retained_source = Some((page, self.positions.rows.len()));
                page
            }
        };
        self.positions.push(bucket, page, row);
        Ok(true)
    }

    pub fn finish_distinct_argument(&mut self, retained_source: Option<(usize, usize)>, context: &QueryContext) -> Result<()> {
        let Some((page, start)) = retained_source else { return Ok(()); };
        let source = &mut self.batches[page].0;
        let positions = &mut self.positions.rows[start..];
        if positions.len() > source.len / 2 || TypedKeyColumn::bind(&source.columns[0]).is_none() {
            return Ok(());
        }
        let bytes = positions.len().checked_mul(size_of::<(usize, usize)>()).ok_or_else(size_error)?;
        let _scratch = match context.memory.reserve(bytes) {
            Ok(scratch) => scratch,
            Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => return Ok(()),
            Err(error) => return Err(error),
        };
        let mut picks = Vec::with_capacity(positions.len());
        for (index, position) in positions.iter().enumerate() {
            context.check_cancelled_at(index)?;
            picks.push((0, position.row));
        }
        match source.select_keys(&picks, &context.memory) {
            Ok(keys) => {
                *source = keys;
                for (row, position) in positions.iter_mut().enumerate() {
                    position.row = row;
                }
                Ok(())
            }
            Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub fn distinct(
        &mut self,
        batch: &ColumnBatch,
        projected: bool,
        bits: &mut BooleanBufferBuilder,
        context: &QueryContext,
        schema: &rtdb_plan::Schema,
        mut select: impl FnMut(u32, &Key<'_>) -> Result<bool>,
    ) -> Result<()> {
        self.identify_keys(batch, projected, context, schema, |_, _, first, hash, key| {
            bits.append(first && select(hash, key)?);
            Ok(())
        })
    }

    pub fn identify<'a>(
        &mut self,
        columns: impl Into<KeyColumns<'a>>,
        projected: bool,
        context: &QueryContext,
        schema: &rtdb_plan::Schema,
        mut visit: impl FnMut(usize, usize, bool) -> Result<()>,
    ) -> Result<()> {
        self.identify_keys(columns, projected, context, schema, |row, index, first, _, _| visit(row, index, first))
    }

    fn identify_keys<'a>(
        &mut self,
        columns: impl Into<KeyColumns<'a>>,
        projected: bool,
        context: &QueryContext,
        schema: &rtdb_plan::Schema,
        mut visit: impl FnMut(usize, usize, bool, u32, &Key<'_>) -> Result<()>,
    ) -> Result<()> {
        let columns = columns.into();
        let KeyColumns { batch, positions } = columns;
        let typed = columns.bind_typed(context)?;
        let width = positions.map_or(batch.columns.len(), <[usize]>::len);
        let bytes = batch.len
            .checked_mul(size_of::<u32>())
            .and_then(|bytes| width.checked_mul(size_of::<usize>()).and_then(|columns| bytes.checked_add(columns)))
            .ok_or_else(size_error)?;
        let mut scratch = context.memory.reserve(bytes)?;
        let mut admitted = 0usize;
        let mut current = Positions::default();
        let mut current_decoded = DecodedKeys::default();
        let previous = self.positions.rows.len();
        let mut hashes = Vec::with_capacity(batch.len);
        if let Some(typed) = &typed && batch.len != 0 {
            context.check_cancelled()?;
            crate::operators::validate_distinct_types(schema)?;
            hashes.resize(batch.len, 0);
            typed.hash_for_index(&mut hashes, context, self.hashing)?;
        }
        for row in 0..batch.len {
            context.check_cancelled_at(row)?;
            let (hash, key) = match &typed {
                Some(typed) => (hashes[row], Key { index_timestamps: true, ..typed.key(row, projected) }),
                None => {
                    let pair = Key::bind(columns, row, projected, context, self.hashing, true)?;
                    if row == 0 { crate::operators::validate_distinct_types(schema)?; }
                    pair
                }
            };
            if let Some(index) = self.find(hash, &key, false, context)? {
                visit(row, index, false, hash, &key)?;
                continue;
            }
            let direct = key.decodable();
            let known = current.find(u64::from(hash), false, context, |index, position| {
                if let Some(typed) = &typed {
                    return Ok(typed.equal_rows(row, position.row));
                }
                if direct {
                    let columns = (0..key.len()).map(|index| key.column(index));
                    if let Some(Decoded::Scalars(stored)) = current_decoded.get(index, columns, position.row, true, &context.memory)?
                        && let Some(equal) = decoded_equal(key.direct_parts(), stored)
                    {
                        return Ok(equal);
                    }
                }
                key.equals(ArrowKey { columns: &batch.columns, row: position.row, projected }, positions, context)
            })?;
            let index = previous
                .checked_add(known.unwrap_or(current.rows.len()))
                .ok_or_else(size_error)?;
            visit(row, index, known.is_none(), hash, &key)?;
            if known.is_none() {
                let first_seen = current.rows.len();
                if first_seen == admitted {
                    let next = admitted.saturating_mul(2).max(1).min(batch.len);
                    let key_bytes = next.checked_mul(size_of::<(usize, usize)>())
                        .and_then(|keys| keys.checked_add(bytes))
                        .and_then(|base| base.checked_add(Positions::allocation_bound(next).ok()?))
                        .ok_or_else(size_error)?;
                    Arc::get_mut(&mut scratch).expect("The grouping page owns its scratch").grow_to(key_bytes)?;
                    admitted = next;
                }
                current.push(u64::from(hash), 0, row);
                if typed.is_some() {
                    // Compact first-seen hashes into the already visited prefix.
                    hashes[first_seen] = hash;
                } else {
                    hashes.push(hash);
                }
            }
        }
        if current.rows.is_empty() {
            return Ok(());
        }
        let picks: Vec<_> = current.rows.iter().map(|position| (0, position.row)).collect();
        drop((current, current_decoded));
        let remaining = picks.len().checked_mul(size_of::<(usize, usize)>())
            .and_then(|picks| picks.checked_add(bytes)).ok_or_else(size_error)?;
        Arc::get_mut(&mut scratch).expect("The grouping page owns its scratch").shrink_to(remaining)?;
        let compact = match (positions, self.hashing) {
            (None, KeyHash::Set) => batch.select_keys(&picks, &context.memory)?,
            _ => batch.compact_group_key_columns(&picks, positions, &context.memory)?,
        };
        hashes.truncate(picks.len());
        self.append(compact, projected, &hashes)
    }
}

fn size_error() -> rtdb_types::Anomaly {
    invalid("set-size", "Set key index exceeds addressable memory")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow_array::{Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use rtdb_columnar::{MemoryBudget, OwnedBatch};
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::Timestamp;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{CatalogSnapshot, DatabaseSnapshot, QueryOptions, columnar::{PredicateSelection, SelectedColumn}};

    fn context() -> Arc<QueryContext> {
        let database = DatabaseSnapshot {
            partitions: vec![],
            storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
        };
        let catalog =
            CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false)
                .unwrap();
        QueryContext::new(
            catalog,
            MemoryBudget::new(1 << 20),
            Timestamp(0),
            "UTC",
            QueryOptions::default(),
            &CancellationToken::new(),
        )
        .unwrap()
    }

    fn batch(numbers: ArrayRef, texts: Vec<Option<&str>>, context: &QueryContext) -> ColumnBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", numbers.data_type().clone(), true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![numbers, Arc::new(StringArray::from(texts))])
                .unwrap();
        ColumnBatch::from_owned(OwnedBatch::new(batch, &context.memory).unwrap()).unwrap()
    }

    #[test]
    fn typed_selected_key_hashes_match_value_hashes_in_projection_order() {
        let context = context();
        let nan = f64::from_bits(0x7ff8_0000_0000_1357);
        let input = batch(
            Arc::new(Float64Array::from(vec![Some(-0.0), Some(nan), None, Some(13.0)])),
            vec![Some("zero"), Some("nan"), Some("nil"), Some("number")],
            &context,
        );
        let positions = Arc::new(vec![(0, 3), (0, 1), (1, 0), (0, 2), (0, 0), (0, 3)]);
        let columns = input.columns.iter().enumerate().map(|(index, column)| {
            let Column::Arrow { array, field, logical } = column else { panic!("Expected an Arrow vector") };
            let source = (Arc::clone(array), Arc::clone(field), logical.clone(),
                Some(Arc::clone(input.column_reservation(index))));
            Column::Selected(SelectedColumn::new(vec![Some(source), None], Arc::clone(&positions)))
        }).collect();
        let selected = ColumnBatch::settled(columns, positions.len(), context.memory.reserve(4096).unwrap()).unwrap();
        let typed = KeyColumns::projection(&selected, &[1, 0, 1]).bind_typed(&context).unwrap().unwrap();
        let mut vector = vec![u32::MAX; selected.len];
        let mut set = vector.clone();
        typed.hash_vector_into(&mut vector, &context).unwrap();
        typed.hash_for_index(&mut set, &context, KeyHash::Set).unwrap();

        for (row, (number, text)) in [
            (Some(13.0), Some("number")), (Some(nan), Some("nan")), (None, None),
            (None, Some("nil")), (Some(-0.0), Some("zero")), (Some(13.0), Some("number")),
        ].into_iter().enumerate() {
            let number = number.map_or(Value::Null, Value::F64);
            let text = text.map_or(Value::Null, |value| Value::Utf8(value.into()));
            let values = [text.clone(), number, text];
            assert_eq!(vector[row], crate::join_storage::hash_values(&values).unwrap());
            assert_eq!(set[row], crate::operators::equality_hash(&values, &context.timezone));
            assert_eq!(typed.key(row, false).vector_hash().unwrap(), vector[row]);
        }

        let nulls = typed.key(2, false);
        let nan = typed.key(1, false);
        assert_eq!(nulls.equal_at(&nulls, 1, true, &context, None).unwrap(), Some(true));
        assert_eq!(nulls.equal_at(&nulls, 1, false, &context, None).unwrap(), None);
        assert_eq!(nan.equal_at(&nan, 1, true, &context, None).unwrap(), Some(false));
        assert!(typed.equal_rows(0, 5));
    }

    #[test]
    fn join_hashes_agree_with_numeric_equality_across_typed_and_row_readers() {
        let context = context();
        let large = 1_i64 << 53;
        let pages = [
            batch(Arc::new(Int64Array::from(vec![Some(0), Some(large), Some(large + 1), None])),
                vec![Some("key"); 4], &context),
            batch(Arc::new(Float32Array::from(vec![Some(-0.0), Some(1.5), Some(f32::NAN), None])),
                vec![Some("key"); 4], &context),
            batch(Arc::new(Float64Array::from(vec![Some(0.0), Some(1.5), Some(large as f64),
                Some(f64::from_bits(0x7ff8_0000_0000_0123)), None])), vec![Some("key"); 5], &context),
        ];
        let projection = [1, 0];
        let scope = rtdb_expr::EvaluationScope::new(&context.memory).unwrap();
        for left in &pages {
            let typed_left = KeyColumns::projection(left, &projection).bind_typed(&context).unwrap().unwrap();
            let mut left_hashes = vec![0; left.len];
            typed_left.hash_join_into(&mut left_hashes, &context, JoinHash::CanonicalZero).unwrap();
            for right in &pages {
                let typed_right = KeyColumns::projection(right, &projection).bind_typed(&context).unwrap().unwrap();
                let mut right_hashes = vec![0; right.len];
                typed_right.hash_join_into(&mut right_hashes, &context, JoinHash::CanonicalZero).unwrap();
                for (l, &hash) in left_hashes.iter().enumerate() {
                    let row = Key::for_columns(left, l, false, &projection, &context).unwrap();
                    assert_eq!(row.join_hash(JoinHash::CanonicalZero).unwrap(), hash);
                    for (r, &right_hash) in right_hashes.iter().enumerate() {
                        let lvalue = left.columns[0].value(l).unwrap();
                        let rvalue = right.columns[0].value(r).unwrap();
                        let expected = rtdb_expr::sql_eq(&lvalue, &rvalue, &scope).unwrap();
                        let left = typed_left.key(l, false);
                        let right = typed_right.key(r, false);
                        assert_eq!(left.equal_at(&right, 1, false, &context, None).unwrap(), expected);
                        let both_null = matches!((&lvalue, &rvalue), (Value::Null, Value::Null));
                        if both_null || expected == Some(true) {
                            assert_eq!(left.equal_at(&right, 1, true, &context, None).unwrap(), Some(true));
                            assert_eq!(hash, right_hash);
                        }
                    }
                }
            }
        }

        let positive = Key::for_columns(&pages[2], 0, false, &[0], &context).unwrap();
        let negative = Key::for_columns(&pages[1], 0, false, &[0], &context).unwrap();
        assert_ne!(positive.vector_hash().unwrap() & 3, negative.vector_hash().unwrap() & 3);
        assert_eq!(positive.join_hash(JoinHash::CanonicalZero).unwrap(), negative.join_hash(JoinHash::CanonicalZero).unwrap());
    }

    #[test]
    fn group_keys_keep_first_seen_ordinals_across_numeric_widths_and_nulls() {
        let context = context();
        let large = 1_i64 << 55;
        let batches = [
            batch(Arc::new(Int64Array::from(vec![Some(large), Some(large + 1), None, Some(0), Some(large)])),
                vec![Some("key"); 5], &context),
            batch(Arc::new(Float32Array::from(vec![Some(large as f32), None, Some(-0.0), Some(f32::NAN), Some(f32::NAN)])),
                vec![Some("key"); 5], &context),
            batch(Arc::new(Float64Array::from(vec![-0.0, 0.0, f64::NAN])),
                vec![Some("key"); 3], &context),
        ];
        let mut index = BatchKeyIndex::vector_hashing(&context).unwrap();
        let mut groups = Vec::new();
        for batch in &batches {
            index.identify(KeyColumns::projection(batch, &[1, 0]), false, &context,
                &rtdb_plan::Schema::default(), |_, ordinal, first| {
                    groups.push((ordinal, first));
                    Ok(())
                }).unwrap();
        }
        assert_eq!(groups, [
            (0, true), (1, true), (2, true), (3, true), (0, false),
            (0, false), (2, false), (4, true), (5, true), (6, true),
            (4, false), (3, false), (7, true),
        ]);
        let keys = index.into_group_keys(2, &context).unwrap().unwrap();
        assert_eq!(keys.len, 8);
        for (row, expected) in [Value::I64(large), Value::I64(large + 1), Value::Null, Value::I64(0), Value::F32(-0.0)]
            .into_iter().enumerate()
        {
            assert_eq!(keys.columns[1].value(row).unwrap(), expected);
        }
    }

    #[test]
    fn typed_keys_reuse_first_seen_text_and_nulls_across_reversed_selections() {
        let context = context();
        let count = 768;
        let texts = (0..count).map(|i| format!("clé {i}")).collect::<Vec<_>>();
        let input = batch(Arc::new(Int64Array::from_iter((0..count as i64).map(Some).chain([None]))),
            texts.iter().map(|text| Some(text.as_str())).chain([None]).collect(), &context);
        let picks = Arc::new(std::iter::once((1, 0)).chain((0..count).rev().map(|row| (0, row))).collect());
        let columns = input.columns.iter().enumerate().map(|(index, column)| {
            let Column::Arrow { array, field, logical } = column else { panic!("Expected an Arrow vector"); };
            let source = (Arc::clone(array), Arc::clone(field), logical.clone(),
                Some(Arc::clone(input.column_reservation(index))));
            Column::Selected(SelectedColumn::new(vec![Some(source), None], Arc::clone(&picks)))
        }).collect();
        let selected = ColumnBatch::settled(columns, count + 1, context.memory.reserve(16_384).unwrap()).unwrap();
        let schema = rtdb_plan::Schema::default();
        for vector in [false, true] {
            let mut index = if vector { BatchKeyIndex::vector_hashing(&context) } else { BatchKeyIndex::new(&context) }.unwrap();
            index.identify(&input, false, &context, &schema, |row, ordinal, first| {
                assert_eq!((ordinal, first), (row, true));
                Ok(())
            }).unwrap();
            let retained = context.memory.used_bytes();
            index.identify(&selected, false, &context, &schema, |row, ordinal, first| {
                assert_eq!((ordinal, first), (count - row, false));
                Ok(())
            }).unwrap();
            assert_eq!(context.memory.used_bytes(), retained);
            let keys = index.into_group_keys(2, &context).unwrap().unwrap();
            assert_eq!(keys.len, count + 1);
            for (row, text) in texts.iter().enumerate() {
                assert_eq!(keys.columns[1].value(row).unwrap(), Value::Utf8(text.clone()));
            }
            assert_eq!(keys.columns[1].value(count).unwrap(), Value::Null);
        }
    }

    #[test]
    fn grouping_and_distinct_keep_high_cardinality_keys_within_the_budget() {
        const PAGE_ROWS: usize = 256;
        const TOTAL_ROWS: usize = 8 * PAGE_ROWS;
        const FIRST_KEY: i64 = 1 << 55;

        let context = context();
        let memory = &context.memory;
        let schema = rtdb_plan::Schema::default();
        for grouped in [true, false] {
            let pressure = memory.reserve(memory.available_bytes() - (512 << 10)).unwrap();
            let mut index = if grouped {
                BatchKeyIndex::vector_hashing(&context).unwrap()
            } else {
                BatchKeyIndex::new(&context).unwrap()
            };
            for repeated in [false, true] {
                for start in (0..TOTAL_ROWS).step_by(PAGE_ROWS) {
                    let numbers = Int64Array::from_iter_values(
                        (start..start + PAGE_ROWS).map(|row| FIRST_KEY + row as i64),
                    );
                    let input = batch(Arc::new(numbers), vec![Some("key"); PAGE_ROWS], &context);
                    if grouped {
                        index.identify(&input, false, &context, &schema, |row, ordinal, first| {
                            assert_eq!((ordinal, first), (start + row, !repeated));
                            Ok(())
                        }).unwrap();
                    } else {
                        let selection = PredicateSelection::build(PAGE_ROWS, memory, |bits| {
                            index.distinct(&input, false, bits, &context, &schema, |_, _| Ok(true))
                        }).unwrap();
                        assert!((0..PAGE_ROWS).all(|row| selection.contains(row) != repeated));
                    }
                }
                assert_eq!(index.len(), TOTAL_ROWS);
            }
            if grouped {
                let _finish_pressure = memory.reserve(memory.available_bytes() - (16 << 10)).unwrap();
                let output = index.into_group_keys(2, &context).unwrap().unwrap();
                assert_eq!(output.len, TOTAL_ROWS);
                for row in 0..output.len {
                    assert_eq!(output.columns[0].value(row).unwrap(), Value::I64(FIRST_KEY + row as i64));
                    assert_eq!(output.columns[1].value(row).unwrap(), Value::Utf8("key".into()));
                }
            } else {
                drop(index);
            }
            drop(pressure);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn sparse_distinct_arguments_keep_group_membership_and_detach_text_backing() {
        let context = context();
        let oversized = "x".repeat(1 << 16);
        let input = batch(Arc::new(Int64Array::from(vec![0; 4])),
            vec![Some(&oversized), Some("é"), None, Some(&oversized)], &context);
        let mut index = BatchKeyIndex::vector_hashing(&context).unwrap();
        let mut source = None;
        for row in [1, 2] {
            assert!(index.insert_distinct_argument(&input, 1, row, 2, false, None, &mut source, &context).unwrap());
        }
        index.finish_distinct_argument(source, &context).unwrap();
        drop(input);
        assert!(context.memory.used_bytes() < oversized.len());

        let probe = batch(Arc::new(Int64Array::from(vec![0; 2])), vec![Some("é"), None], &context);
        let mut source = None;
        assert!(!index.insert_distinct_argument(&probe, 1, 0, 2, false, None, &mut source, &context).unwrap());
        assert!(index.insert_distinct_argument(&probe, 1, 0, 3, false, None, &mut source, &context).unwrap());
        assert!(index.insert_distinct_argument(&probe, 1, 1, 2, false, None, &mut source, &context).unwrap());
    }

    /// Each hashing mode reads a cell either directly from Arrow or as a decoded value; a cell must hash alike either way,
    /// and the join spill partitions and Bloom probes hash it as the vector mode does.
    #[test]
    fn every_key_hash_path_agrees_on_each_cell() {
        let context = context();
        let values = [
            Value::Null, Value::Bool(true), Value::I8(-3), Value::I16(300), Value::I32(-70_000), Value::I64(1 << 40),
            Value::F32(1.5), Value::F64(-2.25), Value::Utf8("text".into()), Value::Binary(vec![1, 2, 3]),
            Value::FixedSizeBinary(vec![4; 8]), Value::Uuid([7; 16]), Value::Oid(5), Value::Uri("http://example.com/a".into()),
            Value::list(vec![Value::F64(1.0), Value::Null, Value::F64(2.5)]), Value::list(vec![Value::I64(1), Value::I64(2)]),
            Value::DateDay(19_000), Value::Timestamp { value: 1_700_000_000_000_000, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) },
        ];
        for value in values {
            let logical = value.logical_type();
            let field = rtdb_columnar::to_field("k", &logical).unwrap();
            let array = rtdb_columnar::copy_array(&logical, std::slice::from_ref(&value)).unwrap();
            let record = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
            let batch = ColumnBatch::from_owned(OwnedBatch::new(record, &context.memory).unwrap()).unwrap();
            let cells = std::slice::from_ref(&value);

            let (set, _) = Key::bind(&batch, 0, false, &context, KeyHash::Set, true).unwrap();
            assert_eq!(set, crate::operators::equality_hash(cells, &context.timezone), "set hash of {value:?}");
            let (vector, key) = Key::bind(&batch, 0, false, &context, KeyHash::Vector, true).unwrap();
            assert_eq!(vector, crate::join_storage::hash_values(cells).unwrap(), "vector hash of {value:?}");
            assert_eq!(key.vector_hash().unwrap(), vector);
            if let Some(direct) = DirectKey::from_value(&value) {
                let cell = direct.clone().hash();
                assert_eq!(cell, rtdb_columnar::hash_value(&value, 0).unwrap(), "direct and vector cell hash of {value:?}");
                assert_eq!(cell as u32, crate::operators::equality_cell_hash(&value, &context.timezone), "direct and set cell hash of {value:?}");
                assert_eq!(direct_bloom_hashes(&batch.columns[0], 0).map(|key| key.hashes.0), Some(cell), "Bloom hash of {value:?}");
            }
        }
    }

    #[test]
    fn timestamp_keys_match_value_hashes_without_owned_values() {
        let context = context();
        let values = [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond]
            .into_iter().flat_map(|unit| [None, Some("America/New_York".into())].into_iter().map(move |timezone|
                Value::Timestamp { value: -7 * unit.units_per_second() - 1, unit, timezone }));
        for value in values {
            let logical = value.logical_type().maybe();
            let field = rtdb_columnar::to_field("t", &logical).unwrap();
            let array = rtdb_columnar::copy_array(&logical, &[value.clone(), Value::Null]).unwrap();
            let record = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
            let batch = ColumnBatch::from_owned(OwnedBatch::new(record, &context.memory).unwrap()).unwrap();
            let Column::Arrow { array, field, logical } = &batch.columns[0] else { panic!("Expected Arrow timestamps") };
            let source = (Arc::clone(array), Arc::clone(field), logical.clone(), Some(Arc::clone(batch.column_reservation(0))));
            let selected = ColumnBatch::settled(
                vec![Column::Selected(SelectedColumn::new(vec![Some(source), None], Arc::new(vec![(0, 1), (0, 0), (1, 0)])))],
                3, context.memory.reserve(4096).unwrap(),
            ).unwrap();
            for (row, value) in [Value::Null, value, Value::Null].iter().enumerate() {
                let (set, key) = Key::bind(&selected, row, true, &context, KeyHash::Set, true).unwrap();
                assert!(key.values.is_empty());
                assert_eq!(set, crate::operators::equality_hash(std::slice::from_ref(value), &context.timezone));
                assert_eq!(key.vector_hash().unwrap(), crate::join_storage::hash_values(std::slice::from_ref(value)).unwrap());
            }
        }
        assert_eq!(context.memory.used_bytes(), 0);
    }

    #[test]
    fn timestamp_comparison_cache_admits_sparse_positions_and_falls_back_under_pressure() {
        let context = context();
        let value = |value| Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
        let n = 1_i64 << 55;
        let column = Column::Values(vec![value(n), value(n + 1)]);
        let values = ColumnBatch::settled(vec![column], 2, context.memory.reserve(4096).unwrap()).unwrap();
        let mut decoded = DecodedKeys::default();
        let slot = 1 << 30;
        let before = context.memory.used_bytes();
        let pressure = context.memory.reserve(context.memory.available_bytes()).unwrap();
        assert!(decoded.get(slot, values.columns.iter(), 0, true, &context.memory).unwrap().is_none());
        assert!(decoded.keys.is_empty());
        assert!(decoded.charge.is_none());
        drop(pressure);

        let Some(Decoded::Scalars(stored)) = decoded.get(slot, values.columns.iter(), 0, true, &context.memory).unwrap() else {
            panic!("Expected a cached timestamp key");
        };
        for (row, equal) in [true, false].into_iter().enumerate() {
            let key = Key::bind(&values, row, false, &context, KeyHash::Set, true).unwrap().1;
            assert_eq!(decoded_equal(key.direct_parts(), stored), Some(equal));
        }
        assert_eq!(decoded.keys.len(), 1);
        drop(decoded);
        assert_eq!(context.memory.used_bytes(), before);
    }

    #[test]
    fn decoded_keys_compare_as_the_column_keys_they_copy() {
        let stored = [OwnedScalar::Signed(1), OwnedScalar::Text("a".into()), OwnedScalar::Null];
        let parts = |keys: &[ScalarKey<'static>]| keys.to_vec().into_iter().map(|key| Some(DirectKey::Scalar(key)));
        assert_eq!(decoded_equal(parts(&[ScalarKey::Float(1.0), ScalarKey::Keyword("a"), ScalarKey::Null]), &stored),
            Some(ScalarKey::Float(1.0).equal(ScalarKey::Signed(1)) == Some(true) && ScalarKey::Keyword("a").equal(ScalarKey::Text("a")) == Some(true)));
        assert_eq!(decoded_equal(parts(&[ScalarKey::Signed(2), ScalarKey::Text("a"), ScalarKey::Null]), &stored), Some(false));
        assert_eq!(decoded_equal(parts(&[ScalarKey::Signed(1), ScalarKey::Text("a"), ScalarKey::Signed(0)]), &stored), Some(false), "null differs from a value");
        assert_eq!(decoded_equal(parts(&[ScalarKey::Signed(1)]), &stored), None, "a different width takes the general comparison");
        assert_eq!(decoded_equal(parts(&[ScalarKey::Uri("https://example.com"), ScalarKey::Text("a"), ScalarKey::Null]), &stored), None);
        assert!(OwnedScalar::payload_bytes(DirectKey::Scalar(ScalarKey::Uri("https://example.com"))).is_none());
    }

    #[test]
    fn arrow_set_index_preserves_collision_order_and_all_multiplicities() {
        let context = context();
        let n = 1_i64 << 55;
        let right = batch(
            Arc::new(Int64Array::from(vec![
                Some(n),
                Some(n + 1),
                Some(n),
                Some(5),
                None,
            ])),
            vec![Some("same"), Some("same"), Some("same"), None, Some("null")],
            &context,
        );
        let (first, key) = Key::bind(&right, 0, false, &context, KeyHash::Set, true).unwrap();
        let (second, _) = Key::bind(&right, 1, false, &context, KeyHash::Set, true).unwrap();
        assert!(key.values.is_empty());
        assert_eq!(first, second);
        let mut index = BatchKeyIndex::new(&context).unwrap();
        index.build(right, false, &context).unwrap();
        let left = batch(
            Arc::new(Int64Array::from(vec![n, n, n, n + 1, n + 1])),
            vec![Some("same"); 5],
            &context,
        );
        let matched: Vec<_> = (0..left.len)
            .map(|row| index.matches(&left, row, false, true, &context).unwrap())
            .collect();
        assert_eq!(matched, [true, true, false, true, false]);
        let narrow = batch(
            Arc::new(Int32Array::from(vec![Some(5), None])),
            vec![None, Some("null")],
            &context,
        );
        for row in 0..narrow.len {
            assert!(index.matches(&narrow, row, false, false, &context).unwrap());
        }
        drop((index, left, narrow));
        assert_eq!(context.memory.used_bytes(), 0);
    }

    #[test]
    fn distinct_shares_the_buffers_of_an_all_unique_binary_batch() {
        use arrow_array::BinaryArray;

        let context = context();
        let values: Vec<_> = (0..16).map(|value| vec![value; 32_768]).collect();
        let array = BinaryArray::from_iter_values(values.iter().map(Vec::as_slice));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::Binary,
            false,
        )]));
        let input = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let input =
            ColumnBatch::from_owned(OwnedBatch::new(input, &context.memory).unwrap()).unwrap();
        let mut index = BatchKeyIndex::new(&context).unwrap();
        let selection = PredicateSelection::build(input.len, &context.memory, |bits| {
            index.distinct(
                &input,
                false,
                bits,
                &context,
                &rtdb_plan::Schema::default(),
                |_, _| Ok(true),
            )
        }).unwrap();
        assert!((0..input.len).all(|row| selection.contains(row)));
        let [Column::Arrow { array: retained, .. }] = index.batches[0].0.columns.as_slice() else {
            panic!("Expected an Arrow key page");
        };
        let [Column::Arrow { array: source, .. }] = input.columns.as_slice() else {
            panic!("Expected an Arrow input page");
        };
        assert!(Arc::ptr_eq(retained, source));
        drop((input, selection));
        assert!(context.memory.used_bytes() < 600 * 1024);
        drop(index);
        assert_eq!(context.memory.used_bytes(), 0);
    }

    #[test]
    fn distinct_key_span_shares_proportionate_backing() {
        let context = context();
        let input = batch(
            Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
            vec![None, Some("one"), Some("two"), None],
            &context,
        );
        let data = |array: &dyn Array| array.as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr();
        let (source, _, _, _) = input.columns[0].arrow_cell(0).unwrap().unwrap();
        let ptr = data(source).wrapping_add(1);
        let keys = input.select_keys(&[(0, 1), (0, 2)], &context.memory).unwrap();
        drop(input);
        let (retained, _, _, _) = keys.columns[0].arrow_cell(0).unwrap().unwrap();
        assert_eq!(data(retained), ptr);
        assert_eq!(keys.columns[0].value(0).unwrap(), Value::I64(1));
        assert_eq!(keys.columns[1].value(1).unwrap(), Value::Utf8("two".into()));
        drop(keys);
        assert_eq!(context.memory.used_bytes(), 0);
    }

    #[test]
    fn distinct_key_span_detaches_even_a_full_page_with_oversized_backing() {
        let context = context();
        let large = "x".repeat(1 << 16);
        let input = batch(
            Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
            vec![Some(large.as_str()), Some("a"), Some("b"), Some(large.as_str())],
            &context,
        ).slice(1, 2, &context.memory).unwrap();
        let keys = input.select_keys(&[(0, 0), (0, 1)], &context.memory).unwrap();
        drop(input);
        assert!(context.memory.used_bytes() < large.len());
        assert_eq!(keys.columns[0].value(0).unwrap(), Value::I64(1));
        assert_eq!(keys.columns[1].value(1).unwrap(), Value::Utf8("b".into()));
        drop(keys);
        assert_eq!(context.memory.used_bytes(), 0);
    }

    #[test]
    fn distinct_keeps_one_zero_column_row_across_pages() {
        let context = context();
        let mut index = BatchKeyIndex::new(&context).unwrap();
        let decide = |index: &mut BatchKeyIndex, input: &ColumnBatch| {
            PredicateSelection::build(input.len, &context.memory, |bits| {
                index.distinct(input, false, bits, &context, &rtdb_plan::Schema::default(), |_, _| Ok(true))
            }).unwrap()
        };
        let input = ColumnBatch::settled(vec![], 9, context.memory.reserve(256).unwrap()).unwrap();
        let selection = decide(&mut index, &input);
        assert!(selection.contains(0));
        assert!((1..input.len).all(|row| !selection.contains(row)));
        let output = input.select_predicate(&selection, &context.memory).unwrap();
        assert_eq!(output.len, 1);
        assert!(output.columns.is_empty());
        assert!(decide(&mut index, &output).is_empty());
        drop((index, output, selection));
        assert_eq!(context.memory.used_bytes(), 0);
    }

    #[test]
    fn a_failed_build_restores_positions_chains_and_admission() {
        let context = context();
        let page = |values: Vec<Value>| {
            let len = values.len();
            ColumnBatch::settled(vec![Column::Values(values)], len, context.memory.reserve(1024).unwrap()).unwrap()
        };
        let probe = page(vec![Value::I64(1), Value::I64(2)]);
        for spare in [false, true] {
            let mut index = BatchKeyIndex::vector_hashing(&context).unwrap();
            index.build(page(vec![Value::I64(1); 4]), false, &context).unwrap();
            if spare {
                index.build(page(vec![Value::I64(3)]), false, &context).unwrap();
            }
            let before = index.len();
            assert_eq!(index.positions.rows.capacity() - before >= 3, spare);
            assert!(index.matches(&probe, 0, false, true, &context).unwrap());
            let bytes = context.memory.used_bytes();
            let rejected = page(vec![Value::I64(1), Value::I64(2), Value::F16(0)]);
            assert_eq!(index.build(rejected, false, &context).unwrap_err().code, "rtdb/jvm-half-float");
            assert_eq!(index.len(), before);
            assert_eq!(context.memory.used_bytes(), bytes);

            assert!(!index.matches(&probe, 1, false, false, &context).unwrap());
            index.build(page(vec![Value::I64(1)]), false, &context).unwrap();
            for _ in 0..4 {
                assert!(index.matches(&probe, 0, false, true, &context).unwrap());
            }
            assert!(!index.matches(&probe, 0, false, true, &context).unwrap());
        }
        drop(probe);
        assert_eq!(context.memory.used_bytes(), 0);
    }

    #[test]
    fn direct_key_candidates_preserve_scalar_uri_preflight_on_hash_collisions() {
        use arrow_array::BinaryArray;
        use rtdb_types::Value;

        let context = context();
        let n = 1_i64 << 55;
        let binary = [37, 123, 18, 74];
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Binary, false),
        ]));
        let left = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![n])),
                Arc::new(StringArray::from(vec!["https://a.example/"])),
                Arc::new(BinaryArray::from_iter_values([binary])),
            ],
        )
        .unwrap();
        let left =
            ColumnBatch::from_owned(OwnedBatch::new(left, &context.memory).unwrap()).unwrap();
        let right = ColumnBatch::settled(
            vec![
                Column::Values(vec![Value::I64(n + 1)]),
                Column::Values(vec![Value::Uri("https://a.example/".into())]),
                Column::Values(vec![Value::F16(0)]),
            ],
            1,
            context.memory.reserve(1024).unwrap(),
        )
        .unwrap();
        let (left_hash, _) = Key::bind(&left, 0, false, &context, KeyHash::Set, true).unwrap();
        let (right_hash, _) = Key::bind(&right, 0, false, &context, KeyHash::Set, true).unwrap();
        assert_eq!(left_hash, right_hash);
        let mut index = BatchKeyIndex::new(&context).unwrap();
        index.build(right, false, &context).unwrap();
        assert_eq!(
            index
                .matches(&left, 0, false, true, &context)
                .unwrap_err()
                .code,
            "rtdb/jvm-half-float"
        );
    }

    #[test]
    fn composite_keys_decode_only_the_column_that_needs_scalar_semantics() {
        use rtdb_types::{LogicalType, ScalarType};

        let context = context();
        let vector = |value| Value::list(vec![value]);
        let left = [Value::DateDay(0), vector(Value::F32(-0.0))];
        let right = [Value::DateMilli(0), vector(Value::F64(-0.0))];
        let batch = |values: &[Value]| {
            let types = [values[0].logical_type(), LogicalType::list(LogicalType::from_legs([
                LogicalType::Scalar(ScalarType::F32), LogicalType::Null, LogicalType::Scalar(ScalarType::F64),
            ]))];
            let fields = types.iter().enumerate().map(|(i, ty)| rtdb_columnar::to_field(&format!("k{i}"), ty).unwrap()).collect::<Vec<_>>();
            let arrays = types.iter().zip(values).map(|(ty, value)| rtdb_columnar::copy_array(ty, std::slice::from_ref(value)).unwrap()).collect();
            ColumnBatch::from_owned(OwnedBatch::new(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap(), &context.memory).unwrap()).unwrap()
        };
        let left_batch = batch(&left);
        let right_batch = batch(&right);
        assert!(KeyColumns::from(&left_batch).bind_typed(&context).unwrap().is_none());
        let (hash, key) = Key::bind(&left_batch, 0, false, &context, KeyHash::Set, true).unwrap();
        assert_eq!(key.values, [(0, Value::DateDay(0))]);
        assert_eq!(hash, crate::operators::equality_hash(&left, &context.timezone));
        assert_eq!(hash, crate::operators::equality_hash(&right, &context.timezone));
        let mut index = BatchKeyIndex::new(&context).unwrap();
        index.build(left_batch, false, &context).unwrap();
        assert!(index.matches(&right_batch, 0, false, true, &context).unwrap());
        assert!(!index.matches(&right_batch, 0, false, true, &context).unwrap());

        let join = Key::for_columns(&right_batch, 0, false, &[0, 1], &context).unwrap();
        assert_eq!(join.values, [(0, Value::DateMilli(0))]);
        assert_eq!(join.vector_hash().unwrap(), crate::join_storage::hash_values(&right).unwrap());
        assert!(matches!(join.direct(1), Some(DirectKey::FloatList(_))));
    }

    #[test]
    fn selected_uri_alias_keys_keep_raw_hash_preflight_and_borrow_the_text() {
        use arrow_array::{Array, UnionArray};
        use arrow_schema::UnionFields;
        use rtdb_types::{LogicalType, ScalarType};

        let context = context();
        let uri = LogicalType::Scalar(ScalarType::Uri);
        let texts = ["http://EXAMPLE.com/a%2f", "http://example.com/a%2F"];
        let fields = UnionFields::try_new([0, 1], [
            Field::new("uri", DataType::Utf8, false),
            rtdb_columnar::to_field("unused", &uri).unwrap(),
        ]).unwrap();
        let array = Arc::new(UnionArray::try_new(fields, vec![0_i8, 0].into(), Some(vec![0_i32, 1].into()), vec![
            Arc::new(StringArray::from(Vec::from(texts))),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ]).unwrap());
        let field = Arc::new(Field::new("key", array.data_type().clone(), false));
        let logical = rtdb_columnar::from_field(&field).unwrap();
        let reservation = context.memory.reserve(array.get_array_memory_size() + 4096).unwrap();
        let batch = ColumnBatch::settled(vec![Column::Arrow { array, field, logical: Some(logical) }], 2, reservation).unwrap();
        let (hash, first) = Key::bind(&batch, 0, false, &context, KeyHash::Set, true).unwrap();
        let (_, second) = Key::bind(&batch, 1, false, &context, KeyHash::Set, true).unwrap();
        let expected = [Value::Uri(texts[0].into())];
        assert!(first.values.is_empty());
        assert!(second.values.is_empty());
        assert_eq!(hash, crate::operators::equality_hash(&expected, &context.timezone));
        assert_eq!(first.vector_hash().unwrap(), crate::join_storage::hash_values(&expected).unwrap());
        assert_eq!(first.equal_at(&second, 0, false, &context, None).unwrap(), Some(true));
        assert!(!first.equals(ArrowKey { columns: &batch.columns, row: 1, projected: false }, None, &context).unwrap());
        assert_eq!(direct_bloom_hashes(&batch.columns[0], 0).unwrap().hashes, (
            rtdb_columnar::hash_value(&expected[0], 0).unwrap(),
            rtdb_columnar::hash_value_murmur3(&expected[0], 0).unwrap(),
        ));
    }

}
