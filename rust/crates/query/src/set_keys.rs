use std::{collections::HashMap, mem::size_of, sync::Arc};

use rtdb_columnar::MemoryReservation;
use rtdb_types::{LogicalType, Result};

use crate::{
    QueryContext,
    columnar::{Column, ColumnBatch},
    cursor::OwnedRow,
    invalid,
    operators::{combine_equality_hash, equality_hash, rows_equal},
    scalar_key::ScalarKey,
};

#[derive(Clone, Copy)]
struct ArrowKey<'a> {
    batch: &'a ColumnBatch,
    row: usize,
    projected: bool,
}

impl ArrowKey<'_> {
    fn read(self, context: &QueryContext) -> Result<OwnedRow> {
        let memory = &context.memory;
        let bound = self
            .batch
            .columns
            .iter()
            .try_fold(256usize, |bytes, column| {
                bytes
                    .checked_add(column.bound(self.row)?)
                    .ok_or_else(size_error)
            })?;
        let _scratch = memory.reserve(bound)?;
        let mut values = Vec::with_capacity(self.batch.columns.len());
        for column in &self.batch.columns {
            let mut value = column.value(self.row)?;
            if self.projected {
                value.admit_expression_dates()?;
            }
            values.push(value);
        }
        OwnedRow::new(values, memory)
    }
}

enum Key<'a> {
    Arrow(ArrowKey<'a>),
    Scalar(OwnedRow),
}

#[derive(Clone, Copy)]
enum KeyHash {
    Set,
    Vector,
}

pub(crate) fn vector_cell_hash(column: &Column, row: usize) -> Option<i32> {
    if let Some(key) = ScalarKey::read(column, row) { return Some(key.hash() as i32); }
    let (array, field, logical, offset) = column.arrow_cell(row)??;
    let logical = match logical? { LogicalType::Maybe(inner) => inner.as_ref(), logical => logical };
    if !matches!(logical, LogicalType::Scalar(_)) && column.expression_cell(row)?.float_list_span().is_none() {
        return None;
    }
    let physical = rtdb_columnar::from_field(field).ok()?;
    let physical = match &physical { LogicalType::Maybe(inner) => inner.as_ref(), physical => physical };
    if physical != logical { return None; }
    rtdb_columnar::hash_cell(field, array, offset, 0).ok()
}

impl<'a> Key<'a> {
    fn bind(
        batch: &'a ColumnBatch,
        row: usize,
        projected: bool,
        context: &QueryContext,
        hashing: KeyHash,
    ) -> Result<(u32, Self)> {
        let hash = batch.columns.iter().try_fold(0, |hash, column| {
            match hashing {
                KeyHash::Set => ScalarKey::read(column, row).map(|cell| cell.hash())
                    .or_else(|| {
                        let cell = column.expression_cell(row)?;
                        cell.float_list_span()?;
                        rtdb_columnar::hash_cell(cell.field, cell.array, cell.row, 0).ok().map(|value| value as u32)
                    }).map(|cell| combine_equality_hash(hash, cell)),
                KeyHash::Vector => vector_cell_hash(column, row).map(|cell| crate::join_storage::combine_cell_hash(hash, cell)),
            }
        });
        let key = ArrowKey {
            batch,
            row,
            projected,
        };
        match hash {
            Some(hash) => Ok((hash, Self::Arrow(key))),
            None => {
                let key = key.read(context)?;
                let hash = match hashing {
                    KeyHash::Set => equality_hash(&key.values, &context.timezone),
                    KeyHash::Vector => crate::join_storage::hash_values(&key.values)?,
                };
                Ok((hash, Self::Scalar(key)))
            }
        }
    }

    fn equals(&self, other: ArrowKey<'_>, context: &QueryContext) -> Result<bool> {
        if let Self::Arrow(key) = self
            && let Some(equal) = direct_equal(*key, other)
        {
            return Ok(equal);
        }
        let (left, keep);
        match self {
            Self::Arrow(key) => {
                keep = key.read(context)?;
                left = &keep.values;
            }
            Self::Scalar(key) => left = &key.values,
        }
        let right = other.read(context)?;
        rows_equal(left, &right.values, &context.timezone, &context.memory)
    }
}

fn direct_equal(left: ArrowKey<'_>, right: ArrowKey<'_>) -> Option<bool> {
    if left.batch.columns.len() != right.batch.columns.len() {
        return Some(false);
    }
    let mut equal = true;
    for (left_column, right_column) in left.batch.columns.iter().zip(&right.batch.columns) {
        let scalar = ScalarKey::read(left_column, left.row).zip(ScalarKey::read(right_column, right.row));
        if let Some((left, right)) = scalar {
            equal &= left.equal(right)?;
            continue;
        }
        let left = left_column.expression_cell(left.row)?;
        let right = right_column.expression_cell(right.row)?;
        if std::mem::discriminant(&left.sequence_kind()?) != std::mem::discriminant(&right.sequence_kind()?) {
            return None;
        }
        let (left, a) = left.float_list_span()?;
        let (right, b) = right.float_list_span()?;
        equal &= a.len() == b.len() && a.zip(b).all(|(a, b)| match (left.float_component(a), right.float_component(b)) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        });
    }
    Some(equal)
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
    chains: HashMap<u32, (usize, usize)>,
}

impl Positions {
    fn allocation_bound(rows: usize) -> Result<usize> {
        rows.checked_mul(4 * size_of::<Position>() + 4 * (size_of::<(u32, (usize, usize))>() + 1))
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(size_error)
    }

    fn push(&mut self, hash: u32, batch: usize, row: usize) {
        use std::collections::hash_map::Entry;
        let index = self.rows.len();
        match self.chains.entry(hash) {
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

    fn find<'a>(
        &mut self,
        hash: u32,
        key: &Key<'_>,
        all: bool,
        context: &QueryContext,
        resolve: impl Fn(Position) -> ArrowKey<'a>,
    ) -> Result<Option<usize>> {
        let mut next = self.chains.get(&hash).map(|&(first, _)| first);
        let mut checked = 0usize;
        while let Some(index) = next {
            context.check_cancelled_at(checked)?;
            checked += 1;
            let position = self.rows[index];
            if (!all || !position.consumed) && key.equals(resolve(position), context)? {
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
            self.positions.push(hash, page, row);
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

    fn find(
        &mut self,
        hash: u32,
        key: &Key<'_>,
        all: bool,
        context: &QueryContext,
    ) -> Result<Option<usize>> {
        let batches = &self.batches;
        self.positions.find(hash, key, all, context, |position| {
            let (batch, projected) = &batches[position.batch];
            ArrowKey {
                batch,
                row: position.row,
                projected: *projected,
            }
        })
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

    pub fn identify(
        &mut self,
        batch: &ColumnBatch,
        projected: bool,
        context: &QueryContext,
        schema: &rtdb_plan::Schema,
        mut visit: impl FnMut(usize, usize, bool) -> Result<()>,
    ) -> Result<()> {
        let bytes = batch
            .len
            .checked_mul(size_of::<(usize, usize)>() + size_of::<u32>())
            .and_then(|bytes| {
                batch
                    .columns
                    .len()
                    .checked_mul(size_of::<usize>())
                    .and_then(|columns| bytes.checked_add(columns))
            })
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
            let (hash, key) = Key::bind(batch, row, projected, context, self.hashing)?;
            if row == 0 {
                crate::operators::validate_distinct_types(schema)?;
            }
            if let Some(index) = self.find(hash, &key, false, context)? {
                visit(row, index, false)?;
                continue;
            }
            let known = current.find(hash, &key, false, context, |position| ArrowKey {
                batch,
                row: position.row,
                projected,
            })?;
            let index = previous
                .checked_add(known.unwrap_or(current.rows.len()))
                .ok_or_else(size_error)?;
            visit(row, index, known.is_none())?;
            if known.is_none() {
                current.push(hash, 0, row);
                picks.push((0, row));
                hashes.push(hash);
            }
        }
        if picks.is_empty() {
            return Ok(());
        }
        let compact = match self.hashing {
            KeyHash::Set => batch.select_keys(&picks, &context.memory)?,
            KeyHash::Vector => batch.compact_group_keys(&picks, &context.memory)?,
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
        assert!(matches!(key, Key::Arrow(_)));
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
}
