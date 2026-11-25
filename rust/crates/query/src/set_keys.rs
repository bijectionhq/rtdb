use std::{borrow::Cow, collections::HashMap, mem::size_of, sync::Arc};

use rtdb_columnar::MemoryReservation;
use rtdb_types::{Result, Value};

use crate::{
    QueryContext,
    columnar::{Column, ColumnBatch, float_vector::{FloatVector, read_position}},
    invalid,
    join_storage::combine_cell_hash,
    operators::{combine_equality_hash, equality_cell_hash, has_uri},
    scalar_key::ScalarKey,
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
}

impl<'a> From<&'a ColumnBatch> for KeyColumns<'a> {
    fn from(batch: &'a ColumnBatch) -> Self {
        Self { batch, positions: None }
    }
}

pub(crate) struct Key<'a> {
    source: ArrowKey<'a>,
    positions: Option<&'a [usize]>,
    values: Vec<(usize, Value)>,
    has_uri: bool,
    _reservation: Option<Arc<MemoryReservation>>,
}

#[derive(Clone, Copy)]
enum KeyHash {
    Set,
    Vector,
}

#[derive(Clone)]
enum DirectKey<'a> {
    Scalar(ScalarKey<'a>),
    FloatList(FloatVector<'a>),
}

impl<'a> DirectKey<'a> {
    fn read(column: &'a Column, row: usize) -> Option<Self> {
        if let Column::Values(values) = column { return values.get(row).and_then(Self::from_value); }
        if let Some(scalar) = ScalarKey::read(column, row) { return Some(Self::Scalar(scalar)); }
        let Some((cell, logical)) = read_position(column, row)? else {
            return Some(Self::Scalar(ScalarKey::Null));
        };
        if logical == rtdb_types::LogicalType::Null || cell.array.is_null(cell.row) {
            return Some(Self::Scalar(ScalarKey::Null));
        }
        if let Some(key) = ScalarKey::read_resolved(cell, &logical) { return Some(Self::Scalar(key)); }
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

    fn hash_with(self, hash_key: fn(ScalarKey<'a>) -> u32, hash_bytes: fn(&[u8], u32) -> u32) -> i32 {
        match self {
            Self::Scalar(key) => hash_key(key) as i32,
            Self::FloatList(vector) => rtdb_columnar::hash_float_components(
                vector.range.clone().map(|index| vector.float_component(index)), 0, hash_bytes,
            ),
        }
    }
}

pub(crate) fn direct_key_equal(left: &Column, l: usize, right: &Column, r: usize, nulls_equal: bool) -> Option<Option<bool>> {
    DirectKey::read(left, l)?.equal(DirectKey::read(right, r)?, nulls_equal)
}

pub(crate) fn direct_bloom_hashes(column: &Column, row: usize) -> Option<((i32, i32), Option<ScalarKey<'_>>)> {
    let key = DirectKey::read(column, row)?;
    let scalar = match key { DirectKey::Scalar(scalar) => Some(scalar), DirectKey::FloatList(_) => None };
    Some(((key.clone().hash(), key.hash_with(ScalarKey::murmur3, rtdb_columnar::murmur3_32)), scalar))
}

impl<'a> Key<'a> {
    fn read(source: ArrowKey<'a>, positions: Option<&'a [usize]>, context: &QueryContext) -> Result<Self> {
        let mut key = Self { source, positions, values: Vec::new(), has_uri: false, _reservation: None };
        let mut bound = 256usize;
        for index in 0..key.len() {
            let column = key.column(index);
            if let Some(direct) = DirectKey::read(column, source.row) {
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
        Self::read(ArrowKey { columns: &batch.columns, row, projected }, Some(positions), context)
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

    fn cached(&self, index: usize) -> Option<&Value> {
        let position = self.values.binary_search_by_key(&index, |(column, _)| *column).ok()?;
        Some(&self.values[position].1)
    }

    fn direct(&self, index: usize) -> Option<DirectKey<'_>> {
        match self.cached(index) {
            Some(value) => DirectKey::from_value(value),
            None => DirectKey::read(self.column(index), self.source.row),
        }
    }

    fn bind(columns: impl Into<KeyColumns<'a>>, row: usize, projected: bool, context: &QueryContext, hashing: KeyHash) -> Result<(u32, Self)> {
        let KeyColumns { batch, positions } = columns.into();
        let key = Self::read(ArrowKey { columns: &batch.columns, row, projected }, positions, context)?;
        let hash = match hashing {
            KeyHash::Set => (0..key.len()).fold(0, |hash, index| {
                let cell = match key.cached(index) {
                    Some(value) => equality_cell_hash(value, &context.timezone),
                    None => key.direct(index).expect("An unread key has a direct reader").hash() as u32,
                };
                combine_equality_hash(hash, cell)
            }),
            KeyHash::Vector => key.vector_hash()?,
        };
        Ok((hash, key))
    }

    pub(crate) fn vector_hash(&self) -> Result<u32> {
        (0..self.len()).try_fold(0, |hash, index| {
            let cell = match self.cached(index) {
                Some(value) => rtdb_columnar::hash_value(value, 0)?,
                None => self.direct(index).expect("An unread key has a direct reader").hash(),
            };
            Ok(combine_cell_hash(hash, cell))
        })
    }

    fn equals(&self, other: ArrowKey<'_>, positions: Option<&[usize]>, context: &QueryContext) -> Result<bool> {
        if self.len() != positions.map_or(other.columns.len(), <[usize]>::len) { return Ok(false); }
        let right = Key::read(other, positions, context)?;
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
    next: Option<usize>,
    consumed: bool,
}

#[derive(Default)]
struct Positions {
    rows: Vec<Position>,
    // The first and last position in each hash chain, in input order.
    chains: HashMap<u64, (usize, usize)>,
}

impl Positions {
    fn allocation_bound(rows: usize) -> Result<usize> {
        rows.checked_mul(4 * size_of::<Position>() + 4 * (size_of::<(u64, (usize, usize))>() + 1))
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(size_error)
    }

    fn push(&mut self, bucket: u64, batch: usize, row: usize) {
        use std::collections::hash_map::Entry;
        let index = self.rows.len();
        match self.chains.entry(bucket) {
            Entry::Occupied(mut entry) => {
                let (_, last) = entry.get_mut();
                self.rows[*last].next = Some(index);
                *last = index;
            }
            Entry::Vacant(entry) => {
                entry.insert((index, index));
            }
        }
        self.rows.push(Position {
            batch,
            row,
            next: None,
            consumed: false,
        });
    }

    fn find(
        &mut self,
        bucket: u64,
        all: bool,
        context: &QueryContext,
        equal: impl Fn(Position) -> Result<bool>,
    ) -> Result<Option<usize>> {
        let mut next = self.chains.get(&bucket).map(|&(first, _)| first);
        let mut checked = 0usize;
        while let Some(index) = next {
            context.check_cancelled_at(checked)?;
            checked += 1;
            let position = self.rows[index];
            if (!all || !position.consumed) && equal(position)? {
                if all {
                    self.rows[index].consumed = true;
                }
                return Ok(Some(index));
            }
            next = position.next;
        }
        Ok(None)
    }
}

pub(crate) struct BatchKeyIndex {
    batches: Vec<(ColumnBatch, bool)>,
    positions: Positions,
    reservation: Arc<MemoryReservation>,
    hashing: KeyHash,
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
        })
    }

    pub fn len(&self) -> usize {
        self.positions.rows.len()
    }

    pub fn group_keys(&self, width: usize, context: &QueryContext) -> Result<Option<ColumnBatch>> {
        if self.len() == 0 {
            return Ok(None);
        }
        let bytes = self
            .len()
            .checked_mul(size_of::<(usize, usize)>())
            .and_then(|bytes| {
                self.batches
                    .len()
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
        let batches: Vec<_> = self.batches.iter().map(|(batch, _)| batch).collect();
        let rows: Vec<_> = self
            .positions
            .rows
            .iter()
            .map(|position| (position.batch, position.row))
            .collect();
        let columns: Vec<_> = (0..width).collect();
        ColumnBatch::take_rows_with_custody(&batches, &rows, &columns, true, &context.memory)
            .map(Some)
    }

    fn append(&mut self, batch: ColumnBatch, projected: bool, hashes: &[u32]) -> Result<()> {
        let rows = self
            .positions
            .rows
            .len()
            .checked_add(hashes.len())
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
            .grow_to(bytes)?;
        let page = self.batches.len();
        self.batches.push((batch, projected));
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
        let _hashes = context.memory.reserve(
            batch
                .len
                .checked_mul(size_of::<u32>())
                .ok_or_else(size_error)?,
        )?;
        let mut hashes = Vec::with_capacity(batch.len);
        for row in 0..batch.len {
            context.check_cancelled_at(row)?;
            let (hash, _) = Key::bind(&batch, row, projected, context, self.hashing)?;
            hashes.push(hash);
        }
        self.append(batch, projected, &hashes)
    }

    pub fn matches(
        &mut self,
        batch: &ColumnBatch,
        row: usize,
        projected: bool,
        all: bool,
        context: &QueryContext,
    ) -> Result<bool> {
        let (hash, key) = Key::bind(batch, row, projected, context, self.hashing)?;
        self.find(hash, &key, all, context)
            .map(|found| found.is_some())
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
        let (hash, key) = Key::bind(columns, row, projected, context, self.hashing)?;
        self.find(hash, &key, false, context)
            .map(|found| found.is_some())
    }

    fn find(
        &mut self,
        hash: u32,
        key: &Key<'_>,
        all: bool,
        context: &QueryContext,
    ) -> Result<Option<usize>> {
        let batches = &self.batches;
        self.positions.find(u64::from(hash), all, context, |position| {
            let (batch, projected) = &batches[position.batch];
            key.equals(ArrowKey { columns: &batch.columns, row: position.row, projected: *projected }, None, context)
        })
    }

    pub fn insert_distinct_argument(
        &mut self, batch: &ColumnBatch, column: usize, row: usize, group: usize, projected: bool,
        retained_source: &mut Option<usize>, context: &QueryContext,
    ) -> Result<bool> {
        let columns = std::slice::from_ref(&batch.columns[column]);
        let key = Key::read(ArrowKey { columns, row, projected }, None, context)?;
        let hash = key.vector_hash()?;
        let source = &self.batches;
        let group = u32::try_from(group).map_err(|_| size_error())?;
        let bucket = (u64::from(group) << 32) | u64::from(hash);
        if self.positions.find(bucket, false, context, |position| {
            let (batch, projected) = &source[position.batch];
            let other = Key::read(ArrowKey { columns: &batch.columns, row: position.row, projected: *projected }, None, context)?;
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
            Some(page) => page,
            None => {
                let page = self.batches.len();
                self.batches.push((batch.key_projection(&[column], &context.memory)?, projected));
                *retained_source = Some(page);
                page
            }
        };
        self.positions.push(bucket, page, row);
        Ok(true)
    }

    pub fn distinct(
        &mut self,
        batch: &ColumnBatch,
        projected: bool,
        mask: &mut Vec<bool>,
        context: &QueryContext,
        schema: &rtdb_plan::Schema,
        mut select: impl FnMut(usize) -> Result<bool>,
    ) -> Result<()> {
        self.identify(batch, projected, context, schema, |row, _, first| {
            mask.push(first && select(row)?);
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
        let columns = columns.into();
        let KeyColumns { batch, positions } = columns;
        if positions.is_some_and(|positions| positions.iter().any(|&position| position >= batch.columns.len())) {
            return Err(invalid("column-gather", "Row position is outside its source batch"));
        }
        let width = positions.map_or(batch.columns.len(), <[usize]>::len);
        let bytes = batch
            .len
            .checked_mul(size_of::<(usize, usize)>() + size_of::<u32>())
            .and_then(|bytes| width.checked_mul(size_of::<usize>()).and_then(|columns| bytes.checked_add(columns)))
            .ok_or_else(size_error)?;
        let _scratch = context.memory.reserve(
            bytes
                .checked_add(Positions::allocation_bound(batch.len)?)
                .ok_or_else(size_error)?,
        )?;
        let mut current = Positions::default();
        let previous = self.positions.rows.len();
        let mut picks = Vec::with_capacity(batch.len);
        let mut hashes = Vec::with_capacity(batch.len);
        for row in 0..batch.len {
            context.check_cancelled_at(row)?;
            let (hash, key) = Key::bind(columns, row, projected, context, self.hashing)?;
            if row == 0 {
                crate::operators::validate_distinct_types(schema)?;
            }
            if let Some(index) = self.find(hash, &key, false, context)? {
                visit(row, index, false)?;
                continue;
            }
            let known = current.find(u64::from(hash), false, context, |position| {
                key.equals(ArrowKey { columns: &batch.columns, row: position.row, projected }, positions, context)
            })?;
            let index = previous
                .checked_add(known.unwrap_or(current.rows.len()))
                .ok_or_else(size_error)?;
            visit(row, index, known.is_none())?;
            if known.is_none() {
                current.push(u64::from(hash), 0, row);
                picks.push((0, row));
                hashes.push(hash);
            }
        }
        if picks.is_empty() {
            return Ok(());
        }
        let compact = match (positions, self.hashing) {
            (None, KeyHash::Set) => batch.select_keys(&picks, &context.memory)?,
            _ => batch.compact_group_key_columns(&picks, positions, &context.memory)?,
        };
        self.append(compact, projected, &hashes)
    }
}

fn size_error() -> rtdb_types::Anomaly {
    invalid("set-size", "Set key index exceeds addressable memory")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use rtdb_columnar::{MemoryBudget, OwnedBatch};
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::Timestamp;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{CatalogSnapshot, DatabaseSnapshot, QueryOptions, columnar::Column};

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

    fn batch(integers: ArrayRef, texts: Vec<Option<&str>>, context: &QueryContext) -> ColumnBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", integers.data_type().clone(), true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![integers, Arc::new(StringArray::from(texts))])
                .unwrap();
        ColumnBatch::from_owned(OwnedBatch::new(batch, &context.memory).unwrap()).unwrap()
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
        let (first, key) = Key::bind(&right, 0, false, &context, KeyHash::Set).unwrap();
        let (second, _) = Key::bind(&right, 1, false, &context, KeyHash::Set).unwrap();
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
        let mut mask = Vec::new();
        let mut index = BatchKeyIndex::new(&context).unwrap();
        index
            .distinct(
                &input,
                false,
                &mut mask,
                &context,
                &rtdb_plan::Schema::default(),
                |_| Ok(true),
            )
            .unwrap();
        assert_eq!(mask, vec![true; 16]);
        drop(input);
        assert!(context.memory.used_bytes() < 600 * 1024);
        assert!(matches!(index.batches[0].0.columns[0], Column::Selected(_)));
        drop(index);
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
        let (left_hash, _) = Key::bind(&left, 0, false, &context, KeyHash::Set).unwrap();
        let (right_hash, _) = Key::bind(&right, 0, false, &context, KeyHash::Set).unwrap();
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
        let (hash, key) = Key::bind(&left_batch, 0, false, &context, KeyHash::Set).unwrap();
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
        let (hash, first) = Key::bind(&batch, 0, false, &context, KeyHash::Set).unwrap();
        let (_, second) = Key::bind(&batch, 1, false, &context, KeyHash::Set).unwrap();
        let expected = [Value::Uri(texts[0].into())];
        assert!(first.values.is_empty());
        assert!(second.values.is_empty());
        assert_eq!(hash, crate::operators::equality_hash(&expected, &context.timezone));
        assert_eq!(first.vector_hash().unwrap(), crate::join_storage::hash_values(&expected).unwrap());
        assert_eq!(first.equal_at(&second, 0, false, &context, None).unwrap(), Some(true));
        assert!(!first.equals(ArrowKey { columns: &batch.columns, row: 1, projected: false }, None, &context).unwrap());
        assert_eq!(direct_bloom_hashes(&batch.columns[0], 0).unwrap().0, (
            rtdb_columnar::hash_value(&expected[0], 0).unwrap(),
            rtdb_columnar::hash_value_murmur3(&expected[0], 0).unwrap(),
        ));
    }

}
